//! # Generic Stateful Actor (`StatefulActor`)
//!
//! A zero-cost, general-purpose stateful actor abstraction (modeled after Erlang/OTP `gen_server`)
//! that runs on the exact same [`super::service_supervisor::ServiceSupervisor`] and
//! [`super::root_supervisor::RootSupervisor`] hierarchy as [`super::generic_actor::GenericResourceActor`].
//!
//! ## Key Highlights
//! - **Zero-Cost Futures**: Uses native `impl Future` in [`StatefulActor::handle`], [`StatefulActor::on_start`],
//!   and [`StatefulActor::on_stop`] — zero `Box::pin` heap allocations per message.
//! - **1-Hop Data Plane with Self-Healing**: [`ActorClient`] communicates directly with the actor's
//!   bounded mailbox and automatically rebinds via the [`ServiceSupervisorHandle`] when an actor
//!   crashes and restarts.
//! - **Request/Response & Fire-and-Forget**: Supports both [`ActorClient::call`] (with response)
//!   and [`ActorClient::cast`] (asynchronous notification).
//! - **Full OTP Lifecycle & Supervision**: Supports [`RestartType`], [`ShutdownPolicy`], [`RestartPolicy`],
//!   and all [` super::model::SupervisionStrategy`] modes (`OneForOne`, `OneForAll`, `RestForOne`).

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};

use super::model::{RestartPolicy, RestartType, ServiceError, ShutdownPolicy, TaskResult};
use super::root_supervisor::RootSupervisorHandle;
use super::service_supervisor::{
    ChildSpec, ServiceSupervisorHandle, ShutdownSignalFn, UntypedActorFactory,
};

/// Control directive set by a [`StatefulActor`] through its [`ActorContext`] during message handling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorControl {
    /// Continue normal message processing.
    Continue,
    /// Stop the actor cleanly (`Ok(())`) after the current handler completes, invoking `on_stop`.
    Stop,
    /// Terminate the actor abnormally with an error reason, triggering supervision recovery.
    Fail(String),
}

/// Execution and lifecycle context provided to [`StatefulActor`] callbacks.
#[derive(Debug, Clone)]
pub struct ActorContext {
    name: String,
    instance_id: usize,
    control: ActorControl,
}

impl ActorContext {
    /// Creates a new actor context for the given actor name and supervisor instance ID.
    pub fn new(name: impl Into<String>, instance_id: usize) -> Self {
        Self {
            name: name.into(),
            instance_id,
            control: ActorControl::Continue,
        }
    }

    /// Returns the registered name of this actor.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the current actor instance ID (starts at 1 and increments on each supervisor restart).
    pub fn instance_id(&self) -> usize {
        self.instance_id
    }

    /// Instructs the actor to stop cleanly (`Ok(())`) after the current message handler finishes.
    pub fn stop(&mut self) {
        self.control = ActorControl::Stop;
    }

    /// Instructs the actor to exit abnormally with `reason`, escalating the failure to its supervisor.
    pub fn fail(&mut self, reason: impl Into<String>) {
        self.control = ActorControl::Fail(reason.into());
    }
}

/// A zero-cost, general-purpose stateful actor trait (modeled after Erlang/OTP `gen_server`).
///
/// Unlike `#[async_trait]`, this trait uses native `impl Future` return types so message handling
/// incurs zero `Box::pin` heap allocations.
pub trait StatefulActor: Send + 'static {
    /// The message or command type accepted by this actor.
    type Message: Send + 'static;
    /// The response payload returned to [`ActorClient::call`] callers.
    type Response: Send + 'static;
    /// The domain error type returned when message handling fails.
    type Error: Send + 'static;

    /// Handles an incoming message sequentially with exclusive `&mut self` access to actor state.
    fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    /// Optional lifecycle hook invoked when the actor starts (or restarts) before messages are processed.
    /// Returning `Err(reason)` aborts startup and triggers supervision recovery.
    fn on_start(
        &mut self,
        _ctx: &mut ActorContext,
    ) -> impl Future<Output = Result<(), String>> + Send {
        async { Ok(()) }
    }

    /// Optional lifecycle hook invoked during graceful shutdown after the mailbox has been drained.
    fn on_stop(&mut self, _ctx: &mut ActorContext) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Error wrapper returned by [`ActorClient`] operations.
#[derive(Debug, Error, Clone)]
pub enum ActorError<E> {
    #[error("Actor task terminated unexpectedly or is unavailable")]
    ActorUnavailable,
    #[error("Service supervisor error: {0}")]
    Service(#[from] ServiceError),
    #[error("Actor handler failed: {0}")]
    HandlerFailed(E),
    #[error("Actor operation timed out after {0:?}")]
    Timeout(Duration),
}

/// Message protocol envelope processed by a [`StatefulActorRunner`].
pub(crate) enum ActorEnvelope<A: StatefulActor> {
    /// Request-response message (`call`).
    Call {
        msg: A::Message,
        respond_to: oneshot::Sender<Result<A::Response, ActorError<A::Error>>>,
    },
    /// Fire-and-forget message (`cast`).
    Cast { msg: A::Message },
    /// Requests a graceful shutdown: closes mailbox, drains pending messages, calls `on_stop`, and exits `Ok(())`.
    Shutdown,
    /// Explicit crash injection command for chaos and resilience testing.
    SimulateCrash,
}

// ============================================================================
// Configuration & Builder for StatefulActor
// ============================================================================

/// Configuration parameters for a [`StatefulActor`].
#[derive(Debug, Clone)]
pub struct StatefulActorConfig {
    pub name: String,
    pub channel_capacity: usize,
    pub restart_policy: RestartPolicy,
    pub restart_type: RestartType,
    pub shutdown_policy: ShutdownPolicy,
    pub handler_timeout: Option<Duration>,
}

impl StatefulActorConfig {
    /// Initializes default configuration settings for a [`StatefulActor`].
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            channel_capacity: 256,
            restart_policy: RestartPolicy::default(),
            restart_type: RestartType::default(),
            shutdown_policy: ShutdownPolicy::default(),
            handler_timeout: None,
        }
    }
}

/// Fluent builder for configuring and spawning a [`StatefulActor`] under a [`super::service_supervisor::ServiceSupervisor`].
pub struct StatefulActorBuilder<A, F> {
    config: StatefulActorConfig,
    factory: Arc<F>,
    _marker: PhantomData<A>,
}

impl StatefulActorBuilder<(), ()> {
    /// Creates a builder using a factory closure `Fn(usize) -> A` that receives the `instance_id`
    /// on initial start and on every subsequent supervisor restart.
    pub fn new<A, F>(name: impl Into<String>, factory: F) -> StatefulActorBuilder<A, F>
    where
        A: StatefulActor,
        F: Fn(usize) -> A + Send + Sync + 'static,
    {
        StatefulActorBuilder {
            config: StatefulActorConfig::new(name),
            factory: Arc::new(factory),
            _marker: PhantomData,
        }
    }

    /// Creates a builder from a `Clone`-able initial actor instance.
    /// On each supervisor restart, a fresh clone of the initial state is spawned.
    pub fn from_instance<A>(
        name: impl Into<String>,
        initial: A,
    ) -> StatefulActorBuilder<A, impl Fn(usize) -> A + Send + Sync + 'static>
    where
        A: StatefulActor + Clone + Sync,
    {
        Self::new(name, move |_inst_id| initial.clone())
    }

    /// Creates a builder for an actor implementing [`Default`].
    pub fn from_default<A>(
        name: impl Into<String>,
    ) -> StatefulActorBuilder<A, impl Fn(usize) -> A + Send + Sync + 'static>
    where
        A: StatefulActor + Default,
    {
        Self::new(name, |_inst_id| A::default())
    }
}

impl<A, F> StatefulActorBuilder<A, F>
where
    A: StatefulActor,
    F: Fn(usize) -> A + Send + Sync + 'static,
{
    /// Sets the actor's bounded mailbox channel capacity (defaults to 256).
    #[must_use]
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.config.channel_capacity = capacity.max(1);
        self
    }

    /// Sets the rate-limiting and backoff restart policy for the actor.
    #[must_use]
    pub fn restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.config.restart_policy = policy;
        self
    }

    /// Sets the OTP restart type (`Transient`, `Permanent`, or `Temporary`).
    #[must_use]
    pub fn restart_type(mut self, restart_type: RestartType) -> Self {
        self.config.restart_type = restart_type;
        self
    }

    /// Sets the shutdown policy (`Timeout(Duration)`, `BrutalKill`, or `Infinity`).
    #[must_use]
    pub fn shutdown_policy(mut self, policy: ShutdownPolicy) -> Self {
        self.config.shutdown_policy = policy;
        self
    }

    /// Convenience method setting [`ShutdownPolicy::Timeout`] for graceful shutdown.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_policy = ShutdownPolicy::Timeout(timeout);
        self
    }

    /// Sets an optional timeout for individual `handle()` executions.
    #[must_use]
    pub fn handler_timeout(mut self, timeout: Duration) -> Self {
        self.config.handler_timeout = Some(timeout);
        self
    }

    /// Builds an OTP-style [`ChildSpec`] that can be registered with a [`super::service_supervisor::ServiceSupervisor`].
    pub fn build_child_spec(self) -> ChildSpec {
        let mut config = self.config;
        config.channel_capacity = config.channel_capacity.max(1);
        let actor_factory = self.factory;
        let channel_capacity = config.channel_capacity;
        let restart_policy = config.restart_policy;
        let restart_type = config.restart_type;
        let shutdown_policy = config.shutdown_policy;
        let name = config.name.clone();

        type ShutdownBinding<A> = (
            std::sync::Weak<Sender<ActorEnvelope<A>>>,
            watch::Sender<bool>,
        );
        let active_shutdown_txs: Arc<std::sync::Mutex<Vec<ShutdownBinding<A>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let shutdown_txs_for_factory = active_shutdown_txs.clone();

        let factory: UntypedActorFactory = Arc::new(move |new_inst| {
            let (tx, rx) = mpsc::channel::<ActorEnvelope<A>>(channel_capacity);
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let sender_arc = Arc::new(tx);
            {
                let mut guard = shutdown_txs_for_factory
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.retain(|(weak_tx, watch_tx)| {
                    weak_tx.strong_count() > 0 && !watch_tx.is_closed()
                });
                guard.push((Arc::downgrade(&sender_arc), shutdown_tx));
            }
            let factory = actor_factory.clone();
            let cfg = config.clone();
            let task_handle = tokio::spawn(async move {
                let actor = (factory)(new_inst);
                let runner = StatefulActorRunner::new(cfg, actor, rx, shutdown_rx, new_inst);
                runner.run().await
            });
            (sender_arc, task_handle)
        });

        let shutdown_signal: ShutdownSignalFn = Arc::new(move |untyped| {
            if let Some(cmd_tx) = untyped.downcast_ref::<Sender<ActorEnvelope<A>>>() {
                {
                    let mut guard = active_shutdown_txs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    guard.retain(|(weak_tx, watch_tx)| {
                        if let Some(arc_tx) = weak_tx.upgrade() {
                            if std::ptr::eq(arc_tx.as_ref(), cmd_tx) {
                                let _ = watch_tx.send(true);
                                return false;
                            }
                            !watch_tx.is_closed()
                        } else {
                            false
                        }
                    });
                }
                let _ = cmd_tx.try_send(ActorEnvelope::Shutdown);
            }
        });

        ChildSpec::new(name, factory, restart_policy)
            .restart_type(restart_type)
            .shutdown_policy(shutdown_policy)
            .with_shutdown_signal(shutdown_signal)
    }

    /// Spawns the actor under the given [`ServiceSupervisorHandle`] and returns a direct [`ActorClient`].
    pub async fn spawn(
        self,
        supervisor: &ServiceSupervisorHandle,
    ) -> Result<ActorClient<A>, ServiceError> {
        let name = self.config.name.clone();
        let spec = self.build_child_spec();
        supervisor.start_child(spec).await?;
        supervisor.get_actor_client::<A>(&name).await
    }

    /// Spawns the actor under the given [`ServiceSupervisorHandle`] and returns a direct [`ActorClient`].
    pub async fn spawn_and_register(
        self,
        supervisor: &ServiceSupervisorHandle,
        _root: &RootSupervisorHandle,
    ) -> Result<ActorClient<A>, ServiceError> {
        self.spawn(supervisor).await
    }
}

impl<A, F> From<StatefulActorBuilder<A, F>> for ChildSpec
where
    A: StatefulActor,
    F: Fn(usize) -> A + Send + Sync + 'static,
{
    fn from(builder: StatefulActorBuilder<A, F>) -> Self {
        builder.build_child_spec()
    }
}

// ============================================================================
// Runtime Loop for StatefulActor
// ============================================================================

/// Runtime loop driving a [`StatefulActor`] instance inside its dedicated Tokio task.
pub(crate) struct StatefulActorRunner<A: StatefulActor> {
    config: StatefulActorConfig,
    actor: A,
    receiver: Receiver<ActorEnvelope<A>>,
    shutdown_rx: watch::Receiver<bool>,
    instance_id: usize,
}

impl<A: StatefulActor> StatefulActorRunner<A> {
    pub(crate) fn new(
        config: StatefulActorConfig,
        actor: A,
        receiver: Receiver<ActorEnvelope<A>>,
        shutdown_rx: watch::Receiver<bool>,
        instance_id: usize,
    ) -> Self {
        Self {
            config,
            actor,
            receiver,
            shutdown_rx,
            instance_id,
        }
    }

    /// Executes a single message through `actor.handle` with optional timeout configuration.
    async fn execute_message(
        &mut self,
        msg: A::Message,
        ctx: &mut ActorContext,
    ) -> Result<A::Response, ActorError<A::Error>> {
        if let Some(timeout) = self.config.handler_timeout {
            match tokio::time::timeout(timeout, self.actor.handle(msg, ctx)).await {
                Ok(Ok(resp)) => Ok(resp),
                Ok(Err(err)) => Err(ActorError::HandlerFailed(err)),
                Err(_) => {
                    // Dropping `actor.handle` mid-flight can leave `&mut self` partially mutated;
                    // escalate to the supervisor so a fresh, consistent instance is spawned via `on_start`.
                    ctx.fail(format!(
                        "StatefulActor '{}' #{} handler timed out after {:?}",
                        self.config.name, self.instance_id, timeout
                    ));
                    Err(ActorError::Timeout(timeout))
                }
            }
        } else {
            self.actor
                .handle(msg, ctx)
                .await
                .map_err(ActorError::HandlerFailed)
        }
    }

    /// Drains remaining queued messages in the mailbox and runs `on_stop` before exiting cleanly.
    async fn finish_graceful_shutdown(&mut self, ctx: &mut ActorContext) -> TaskResult {
        tracing::info!(
            actor = %self.config.name,
            instance = self.instance_id,
            "🛑 StatefulActor graceful shutdown initiated. Draining mailbox and invoking on_stop..."
        );

        self.receiver.close();
        ctx.control = ActorControl::Continue;

        while let Ok(env) = self.receiver.try_recv() {
            match env {
                ActorEnvelope::Call { msg, respond_to } => {
                    let res = self.execute_message(msg, ctx).await;
                    let _ = respond_to.send(res);
                    if let ActorControl::Fail(reason) = &ctx.control {
                        return Err(reason.clone());
                    }
                }
                ActorEnvelope::Cast { msg } => {
                    let _ = self.execute_message(msg, ctx).await;
                    if let ActorControl::Fail(reason) = &ctx.control {
                        return Err(reason.clone());
                    }
                }
                ActorEnvelope::Shutdown | ActorEnvelope::SimulateCrash => break,
            }
        }

        self.actor.on_stop(ctx).await;

        tracing::info!(
            actor = %self.config.name,
            instance = self.instance_id,
            "✅ StatefulActor graceful shutdown complete."
        );
        Ok(())
    }

    /// Runs the actor lifecycle (`on_start` -> message loop -> `on_stop`).
    pub(crate) async fn run(mut self) -> TaskResult {
        let mut ctx = ActorContext::new(self.config.name.clone(), self.instance_id);

        if let Err(reason) = self.actor.on_start(&mut ctx).await {
            tracing::error!(
                actor = %self.config.name,
                instance = self.instance_id,
                reason = %reason,
                "❌ StatefulActor failed in on_start hook."
            );
            return Err(reason);
        }

        match &ctx.control {
            ActorControl::Continue => {}
            ActorControl::Stop => {
                self.actor.on_stop(&mut ctx).await;
                return Ok(());
            }
            ActorControl::Fail(reason) => {
                return Err(reason.clone());
            }
        }

        loop {
            if *self.shutdown_rx.borrow() {
                return self.finish_graceful_shutdown(&mut ctx).await;
            }

            tokio::select! {
                Ok(()) = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        return self.finish_graceful_shutdown(&mut ctx).await;
                    }
                }

                env_opt = self.receiver.recv() => {
                    let Some(envelope) = env_opt else {
                        self.actor.on_stop(&mut ctx).await;
                        return Ok(());
                    };

                    match envelope {
                        ActorEnvelope::Call { msg, respond_to } => {
                            let res = self.execute_message(msg, &mut ctx).await;
                            if !matches!(ctx.control, ActorControl::Continue) {
                                // Close the mailbox before waking the caller so any immediate
                                // follow-up call sees a closed channel and triggers rebinding
                                // rather than queuing into the dying actor's buffer.
                                self.receiver.close();
                            }
                            let _ = respond_to.send(res);

                            match &ctx.control {
                                ActorControl::Continue => {}
                                ActorControl::Stop => {
                                    return self.finish_graceful_shutdown(&mut ctx).await;
                                }
                                ActorControl::Fail(reason) => {
                                    let reason_owned = reason.clone();
                                    tracing::error!(
                                        actor = %self.config.name,
                                        instance = self.instance_id,
                                        reason = %reason_owned,
                                        "💥 StatefulActor escalated failure to supervisor."
                                    );
                                    return Err(reason_owned);
                                }
                            }
                        }

                        ActorEnvelope::Cast { msg } => {
                            let _ = self.execute_message(msg, &mut ctx).await;
                            if !matches!(ctx.control, ActorControl::Continue) {
                                self.receiver.close();
                            }

                            match &ctx.control {
                                ActorControl::Continue => {}
                                ActorControl::Stop => {
                                    return self.finish_graceful_shutdown(&mut ctx).await;
                                }
                                ActorControl::Fail(reason) => {
                                    let reason_owned = reason.clone();
                                    tracing::error!(
                                        actor = %self.config.name,
                                        instance = self.instance_id,
                                        reason = %reason_owned,
                                        "💥 StatefulActor escalated failure to supervisor."
                                    );
                                    return Err(reason_owned);
                                }
                            }
                        }

                        ActorEnvelope::Shutdown => {
                            return self.finish_graceful_shutdown(&mut ctx).await;
                        }

                        ActorEnvelope::SimulateCrash => {
                            tracing::warn!(
                                actor = %self.config.name,
                                instance = self.instance_id,
                                "💥 CRASH COMMAND RECEIVED: Terminating StatefulActor with crash error!"
                            );
                            return Err(format!(
                                "Intentional crash triggered via SimulateCrash on StatefulActor '{}' #{}",
                                self.config.name, self.instance_id
                            ));
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// Direct Data-Plane Client for StatefulActor
// ============================================================================

/// High-performance, self-healing 1-hop Data Plane client for a [`StatefulActor`].
pub struct ActorClient<A: StatefulActor> {
    name: String,
    sender: Arc<std::sync::RwLock<Sender<ActorEnvelope<A>>>>,
    supervisor: ServiceSupervisorHandle,
}

impl<A: StatefulActor> Clone for ActorClient<A> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            sender: self.sender.clone(),
            supervisor: self.supervisor.clone(),
        }
    }
}

impl<A: StatefulActor> ActorClient<A> {
    /// Creates a new direct client handle for a [`StatefulActor`].
    pub(crate) fn new(
        name: impl Into<String>,
        sender: Arc<std::sync::RwLock<Sender<ActorEnvelope<A>>>>,
        supervisor: ServiceSupervisorHandle,
    ) -> Self {
        Self {
            name: name.into(),
            sender,
            supervisor,
        }
    }

    /// Returns the name of the supervised actor.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the name of the [`ServiceSupervisorHandle`] managing this actor.
    pub fn service_name(&self) -> &str {
        self.supervisor.name()
    }

    /// Returns a clone of the currently cached channel sender without holding the lock across `.await`.
    fn current_sender(&self) -> Sender<ActorEnvelope<A>> {
        self.sender
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Re-resolves the actor's mailbox channel from the [`ServiceSupervisorHandle`] after a crash.
    async fn rebind_channel(&self) -> Result<(), ActorError<A::Error>> {
        match self
            .supervisor
            .resolve_actor_channel::<Sender<ActorEnvelope<A>>>(&self.name)
            .await
        {
            Ok(new_sender) if !new_sender.is_closed() => {
                *self
                    .sender
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = new_sender;
                tracing::info!(
                    actor = %self.name,
                    "🔄 ActorClient channel re-established with replacement actor!"
                );
                Ok(())
            }
            Ok(_) => Err(ActorError::ActorUnavailable),
            Err(err) => Err(ActorError::Service(err)),
        }
    }

    /// Waits briefly for supervision recovery and re-binds the direct channel, propagating permanent failures.
    async fn backoff_and_rebind(&self) -> Result<(), ActorError<A::Error>> {
        const REBIND_BACKOFF: Duration = Duration::from_millis(60);
        tokio::time::sleep(REBIND_BACKOFF).await;
        match self.rebind_channel().await {
            Ok(()) => Ok(()),
            Err(ActorError::Service(
                err @ (ServiceError::MaxRestartsExceeded(_, _, _)
                | ServiceError::ActorNotFound(_)
                | ServiceError::ActorTypeMismatch(_)),
            )) => Err(ActorError::Service(err)),
            Err(_) => Ok(()),
        }
    }

    /// Sends a request message (`call`) directly to the actor and awaits its response.
    ///
    /// If the actor's channel was closed due to a prior crash, `call` recovers the original
    /// message from the closed channel without requiring `A::Message: Clone`, rebinds to the
    /// replacement actor via the supervisor, and delivers the message transparently.
    pub async fn call(&self, mut msg: A::Message) -> Result<A::Response, ActorError<A::Error>> {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_sender = self.current_sender();

            let (tx, rx) = oneshot::channel();
            match current_sender
                .send(ActorEnvelope::Call {
                    msg,
                    respond_to: tx,
                })
                .await
            {
                Ok(()) => {
                    if let Ok(res) = rx.await {
                        return res;
                    }
                    // The actor crashed while processing the message (`msg` was consumed).
                    return Err(ActorError::ActorUnavailable);
                }
                Err(mpsc::error::SendError(ActorEnvelope::Call {
                    msg: returned_msg, ..
                })) => {
                    msg = returned_msg;
                    if attempt < MAX_ATTEMPTS {
                        self.backoff_and_rebind().await?;
                    }
                }
                Err(_) => unreachable!(),
            }
        }

        Err(ActorError::ActorUnavailable)
    }

    /// Sends a cloneable request message (`call_retry`) and retries across both channel disconnections
    /// and mid-flight handler crashes.
    pub async fn call_retry(&self, msg: A::Message) -> Result<A::Response, ActorError<A::Error>>
    where
        A::Message: Clone,
    {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_sender = self.current_sender();

            let (tx, rx) = oneshot::channel();
            let send_res = current_sender
                .send(ActorEnvelope::Call {
                    msg: msg.clone(),
                    respond_to: tx,
                })
                .await;

            if send_res.is_ok()
                && let Ok(res) = rx.await
            {
                return res;
            }

            if attempt < MAX_ATTEMPTS {
                self.backoff_and_rebind().await?;
            }
        }

        Err(ActorError::ActorUnavailable)
    }

    /// Sends a request message (`call`) with a client-side timeout deadline.
    pub async fn call_timeout(
        &self,
        msg: A::Message,
        timeout: Duration,
    ) -> Result<A::Response, ActorError<A::Error>> {
        tokio::time::timeout(timeout, self.call(msg))
            .await
            .map_err(|_| ActorError::Timeout(timeout))?
    }

    /// Sends an asynchronous fire-and-forget notification (`cast`) to the actor.
    /// Automatically rebinds via the supervisor if the channel was closed by a prior crash.
    pub async fn cast(&self, mut msg: A::Message) -> Result<(), ActorError<A::Error>> {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_sender = self.current_sender();

            match current_sender.send(ActorEnvelope::Cast { msg }).await {
                Ok(()) => return Ok(()),
                Err(mpsc::error::SendError(ActorEnvelope::Cast { msg: returned_msg })) => {
                    msg = returned_msg;
                    if attempt < MAX_ATTEMPTS {
                        self.backoff_and_rebind().await?;
                    }
                }
                Err(_) => unreachable!(),
            }
        }

        Err(ActorError::ActorUnavailable)
    }

    /// Sends a one-way control command to the actor, transparently rebinding if the channel was closed by a prior crash.
    async fn send_control(
        &self,
        make_env: impl Fn() -> ActorEnvelope<A>,
    ) -> Result<(), ActorError<A::Error>> {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_sender = self.current_sender();

            if current_sender.send(make_env()).await.is_ok() {
                return Ok(());
            }

            if attempt < MAX_ATTEMPTS {
                self.backoff_and_rebind().await?;
            }
        }

        Err(ActorError::ActorUnavailable)
    }

    /// Requests a graceful shutdown of the actor over its direct data-plane channel.
    pub async fn shutdown(&self) -> Result<(), ActorError<A::Error>> {
        self.send_control(|| ActorEnvelope::Shutdown).await
    }

    /// Injects an intentional crash command directly into the actor for resilience testing.
    pub async fn simulate_crash(&self) -> Result<(), ActorError<A::Error>> {
        self.send_control(|| ActorEnvelope::SimulateCrash).await
    }
}

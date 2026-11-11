//! # Generic Stateful Actor (`StatefulActor`)
//!
//! A zero-cost, general-purpose stateful actor abstraction (modeled after Erlang/OTP `gen_server`)
//! that runs on the unified [`super::supervisor::Supervisor`] hierarchy alongside
//! [`super::generic_actor::ResourceActor`].
//!
//! ## Key Highlights
//! - **Zero-Cost Futures**: Uses native `impl Future` in [`StatefulActor::handle`], [`StatefulActor::handle_cast`],
//!   [`StatefulActor::on_start`], and [`StatefulActor::on_stop`] — zero `Box::pin` heap allocations per message.
//! - **1-Hop Data Plane with Zero-Polling Self-Healing**: [`ActorClient`] communicates directly with the actor's
//!   bounded mailbox and automatically rebinds via a reactive [`watch::Receiver<SlotState>`] when an actor
//!   or its supervisor crashes and restarts.
//! - **Request/Response, Typed Sub-Messages & Fire-and-Forget**: Supports [`ActorClient::call`], [`ActorClient::ask`]
//!   (via [`CallMessage`]), [`ActorClient::cast`], [`ActorClient::try_cast`], and [`ActorClient::cast_timeout`].
//! - **Rich Execution Context**: [`ActorContext<A>`] provides `client()`, `supervisor()`, `send_after()`, `send_interval()`, `spawn_scoped()`, and `forward_receiver()`.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver, Sender, WeakSender},
    oneshot, watch,
};
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use super::model::{RestartPolicy, RestartType, ShutdownPolicy, SupervisorError, TaskResult};
use super::supervisor::{
    ChildSpec, ShutdownSignalFn, SlotState, SpawnContext, SupervisorHandle, UntypedActorFactory,
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

/// Execution and messaging scope passed to instance-bound background tasks spawned via
/// [`ActorContext::spawn_scoped`].
///
/// Unlike [`ActorClient`], an [`ActorScope`] is pinned to the single actor instance
/// (`instance_id`) that created it: once that instance stops, crashes, or panics,
/// [`Self::cancelled`] completes immediately and [`Self::cast`] / [`Self::try_cast`] return
/// [`ActorError::ActorUnavailable`] instead of rebinding onto a restarted replacement instance.
pub struct ActorScope<A: StatefulActor> {
    name: String,
    instance_id: usize,
    weak_sender: WeakSender<ActorEnvelope<A>>,
    cancel_rx: watch::Receiver<bool>,
}

impl<A: StatefulActor> Clone for ActorScope<A> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            instance_id: self.instance_id,
            weak_sender: self.weak_sender.clone(),
            cancel_rx: self.cancel_rx.clone(),
        }
    }
}

impl<A: StatefulActor> std::fmt::Debug for ActorScope<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorScope")
            .field("name", &self.name)
            .field("instance_id", &self.instance_id)
            .field("cancelled", &*self.cancel_rx.borrow())
            .finish()
    }
}

impl<A: StatefulActor> ActorScope<A> {
    /// Returns the registered name of the owning actor.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the instance ID (`1`, `2`, ...) to which this scope is pinned.
    pub fn instance_id(&self) -> usize {
        self.instance_id
    }

    /// Returns `true` if this actor instance has exited, panicked, or begun stopping its lifecycle tasks.
    pub fn is_cancelled(&self) -> bool {
        *self.cancel_rx.borrow()
    }

    /// Completes as soon as the owning actor instance exits, crashes, panics, or enters final stop hooks.
    ///
    /// This method is cancel-safe and designed for direct use in `tokio::select!`.
    pub async fn cancelled(&mut self) {
        let _ = self.cancel_rx.wait_for(|&cancelled| cancelled).await;
    }

    /// Sends `msg` via `cast` to the owning actor instance, respecting bounded mailbox backpressure.
    ///
    /// Aborts immediately with [`ActorError::ActorUnavailable`] if the actor instance exits or is
    /// cancelled before the message can be enqueued. Never rebinds onto a restarted replacement instance.
    pub async fn cast(&self, msg: A::Message) -> Result<(), ActorError<A::Error>> {
        if self.is_cancelled() {
            return Err(ActorError::ActorUnavailable);
        }
        let Some(tx) = self.weak_sender.upgrade() else {
            return Err(ActorError::ActorUnavailable);
        };
        let mut cancel_rx = self.cancel_rx.clone();
        tokio::select! {
            biased;
            _ = cancel_rx.wait_for(|&cancelled| cancelled) => Err(ActorError::ActorUnavailable),
            res = tx.send(ActorEnvelope::Cast { msg }) => {
                res.map_err(|_| ActorError::ActorUnavailable)
            }
        }
    }

    /// Attempts to enqueue `msg` via `cast` into the owning actor instance without waiting.
    ///
    /// Returns [`ActorError::MailboxFull`] if the mailbox is at capacity, or
    /// [`ActorError::ActorUnavailable`] if the instance has been cancelled or closed.
    pub fn try_cast(&self, msg: A::Message) -> Result<(), ActorError<A::Error>> {
        if self.is_cancelled() {
            return Err(ActorError::ActorUnavailable);
        }
        let Some(tx) = self.weak_sender.upgrade() else {
            return Err(ActorError::ActorUnavailable);
        };
        match tx.try_send(ActorEnvelope::Cast { msg }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(ActorError::MailboxFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(ActorError::ActorUnavailable),
        }
    }
}

/// Execution and lifecycle context provided to [`StatefulActor`] callbacks.
pub struct ActorContext<A: StatefulActor> {
    name: String,
    instance_id: usize,
    control: ActorControl,
    weak_sender: WeakSender<ActorEnvelope<A>>,
    cancel_rx: watch::Receiver<bool>,
    supervisor: SupervisorHandle,
}

impl<A: StatefulActor> Clone for ActorContext<A> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            instance_id: self.instance_id,
            control: self.control.clone(),
            weak_sender: self.weak_sender.clone(),
            cancel_rx: self.cancel_rx.clone(),
            supervisor: self.supervisor.clone(),
        }
    }
}

impl<A: StatefulActor> std::fmt::Debug for ActorContext<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorContext")
            .field("name", &self.name)
            .field("instance_id", &self.instance_id)
            .field("control", &self.control)
            .finish()
    }
}

impl<A: StatefulActor> ActorContext<A> {
    /// Creates a new actor context for the given actor name, instance ID, weak self-sender, lifecycle signal, and parent supervisor handle.
    pub(crate) fn new(
        name: impl Into<String>,
        instance_id: usize,
        weak_sender: WeakSender<ActorEnvelope<A>>,
        cancel_rx: watch::Receiver<bool>,
        supervisor: SupervisorHandle,
    ) -> Self {
        Self {
            name: name.into(),
            instance_id,
            control: ActorControl::Continue,
            weak_sender,
            cancel_rx,
            supervisor,
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

    /// Returns a handle to the parent [`SupervisorHandle`] managing this actor.
    ///
    /// # Deadlock Warning
    ///
    /// Do **not** `.await` [`SupervisorHandle::shutdown`], [`SupervisorHandle::stop_child`],
    /// or [`SupervisorHandle::delete_child`] (targeting this actor) directly from inside a
    /// message handler: the supervisor waits for the current handler to return before completing
    /// those operations, which will deadlock until the actor's [`super::model::ShutdownPolicy`]
    /// timeout expires. Use [`SupervisorHandle::request_shutdown`] or [`ActorContext::stop`]
    /// instead.
    pub fn supervisor(&self) -> &SupervisorHandle {
        &self.supervisor
    }

    /// Returns a self-healing [`ActorClient<A>`] pointing to this actor.
    pub fn client(&self) -> ActorClient<A> {
        let sender = self.weak_sender.upgrade().unwrap_or_else(|| {
            let (tx, _rx) = mpsc::channel(1);
            tx
        });
        ActorClient::new_unsubscribed(self.name.clone(), sender, self.supervisor.clone())
    }

    /// Schedules `msg` to be delivered to this actor via `cast` after `delay`.
    ///
    /// The timer task aborts automatically if this actor instance stops or panics before `delay` elapses.
    /// Returns a [`tokio::task::AbortHandle`] that can be used to cancel the scheduled timer early.
    pub fn send_after(&self, delay: Duration, msg: A::Message) -> tokio::task::AbortHandle {
        let weak_tx = self.weak_sender.clone();
        let mut cancel_rx = self.cancel_rx.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = cancel_rx.wait_for(|&cancelled| cancelled) => return,
                _ = tokio::time::sleep(delay) => {}
            }
            if let Some(tx) = weak_tx.upgrade() {
                tokio::select! {
                    biased;
                    _ = cancel_rx.wait_for(|&cancelled| cancelled) => {}
                    _ = tx.send(ActorEnvelope::Cast { msg }) => {}
                }
            }
        })
        .abort_handle()
    }

    /// Periodically delivers clones of `msg` to this actor via `cast` every `period`.
    ///
    /// The first message is scheduled after one full `period` (not immediately at call time).
    /// Missed ticks caused by slow handlers or full mailboxes are skipped rather than burst-queued
    /// (`MissedTickBehavior::Skip`), and the background task terminates immediately when this actor
    /// instance stops, crashes, or panics.
    ///
    /// If `period` is [`Duration::ZERO`], logs a warning and returns an already-completed
    /// [`tokio::task::AbortHandle`] instead of panicking.
    ///
    /// For non-`Clone` messages or dynamically generated payloads, use [`Self::send_interval_with`].
    pub fn send_interval(&self, period: Duration, msg: A::Message) -> tokio::task::AbortHandle
    where
        A::Message: Clone,
    {
        self.send_interval_with(period, move || msg.clone())
    }

    /// Periodically generates and delivers messages via `cast` every `period` using `make_msg`.
    ///
    /// Unlike [`Self::send_interval`], this works with non-`Clone` message types and lets each tick
    /// compute fresh payloads. `make_msg` is invoked only after a bounded mailbox permit is reserved,
    /// avoiding wasted allocations if the actor stops while backpressured.
    ///
    /// The first tick fires after `period`, missed ticks under backpressure are skipped
    /// (`MissedTickBehavior::Skip`), and the timer task stops immediately when the current actor
    /// instance exits or panics. If `period` is [`Duration::ZERO`], logs a warning and returns an
    /// already-completed [`tokio::task::AbortHandle`].
    pub fn send_interval_with<F>(
        &self,
        period: Duration,
        mut make_msg: F,
    ) -> tokio::task::AbortHandle
    where
        F: FnMut() -> A::Message + Send + 'static,
    {
        if period.is_zero() {
            tracing::warn!(
                actor = %self.name,
                instance = self.instance_id,
                "Ignoring send_interval_with called with zero duration."
            );
            return tokio::spawn(async {}).abort_handle();
        }

        let weak_tx = self.weak_sender.clone();
        let mut cancel_rx = self.cancel_rx.clone();
        tokio::spawn(async move {
            let Some(start) = Instant::now().checked_add(period) else {
                let _ = cancel_rx.wait_for(|&cancelled| cancelled).await;
                return;
            };

            let mut ticker = interval_at(start, period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    _ = cancel_rx.wait_for(|&cancelled| cancelled) => break,
                    _ = ticker.tick() => {}
                }

                let Some(tx) = weak_tx.upgrade() else {
                    break;
                };

                let permit = tokio::select! {
                    biased;
                    _ = cancel_rx.wait_for(|&cancelled| cancelled) => break,
                    res = tx.reserve() => match res {
                        Ok(permit) => permit,
                        Err(_) => break,
                    }
                };

                permit.send(ActorEnvelope::Cast { msg: make_msg() });
            }
        })
        .abort_handle()
    }

    /// Spawns a background task bound to this actor instance's lifecycle.
    ///
    /// The spawned future receives an [`ActorScope<A>`] pinned to the current `instance_id` for
    /// observing cancellation (`scope.cancelled()`) and sending messages via `scope.cast(...)` /
    /// `scope.try_cast(...)`. When this actor instance stops (right before `on_stop` after mailbox
    /// draining), fails, or panics, the scope cancels immediately so external stream/socket loops
    /// shut down cleanly without leaking tasks or forwarding stale events to a restarted instance.
    pub fn spawn_scoped<F, Fut>(&self, task: F) -> tokio::task::AbortHandle
    where
        F: FnOnce(ActorScope<A>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let scope = ActorScope {
            name: self.name.clone(),
            instance_id: self.instance_id,
            weak_sender: self.weak_sender.clone(),
            cancel_rx: self.cancel_rx.clone(),
        };
        tokio::spawn(task(scope)).abort_handle()
    }

    /// Forwards items from an [`mpsc::Receiver<T>`] into this actor as `cast` messages via `map_msg`.
    ///
    /// Items are pulled one at a time while waiting for mailbox space (`tx.reserve()`), so idle
    /// forwarders never hold mailbox permits or starve `call`/`cast` callers even with
    /// `channel_capacity(1)`. The task exits automatically when `rx` closes or when the current
    /// actor instance stops, crashes, or panics.
    pub fn forward_receiver<T, F>(
        &self,
        mut rx: mpsc::Receiver<T>,
        mut map_msg: F,
    ) -> tokio::task::AbortHandle
    where
        T: Send + 'static,
        F: FnMut(T) -> A::Message + Send + 'static,
    {
        let weak_tx = self.weak_sender.clone();
        let mut cancel_rx = self.cancel_rx.clone();
        tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    biased;
                    _ = cancel_rx.wait_for(|&cancelled| cancelled) => break,
                    maybe_item = rx.recv() => match maybe_item {
                        Some(item) => item,
                        None => break,
                    }
                };

                let Some(tx) = weak_tx.upgrade() else {
                    break;
                };

                let permit = tokio::select! {
                    biased;
                    _ = cancel_rx.wait_for(|&cancelled| cancelled) => break,
                    res = tx.reserve() => match res {
                        Ok(permit) => permit,
                        Err(_) => break,
                    }
                };

                permit.send(ActorEnvelope::Cast { msg: map_msg(item) });
            }
        })
        .abort_handle()
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
pub trait StatefulActor: Sized + Send + 'static {
    /// The message or command type accepted by this actor.
    type Message: Send + 'static;
    /// The response payload returned to [`ActorClient::call`] callers.
    type Response: Send + 'static;
    /// The domain error type returned when message handling fails.
    type Error: std::fmt::Debug + Send + 'static;

    /// Handles a synchronous request-response message (`call`) sequentially with exclusive `&mut self` access.
    fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext<Self>,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    /// Handles an asynchronous fire-and-forget message (`cast`) sequentially with exclusive `&mut self` access.
    ///
    /// By default, delegates to [`StatefulActor::handle`] and discards the response value.
    fn handle_cast(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext<Self>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let _ = self.handle(msg, ctx).await?;
            Ok(())
        }
    }

    /// Optional lifecycle hook invoked when the actor starts (or restarts) before messages are processed.
    /// Returning `Err(err)` aborts startup and triggers supervision recovery.
    fn on_start(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Optional lifecycle hook invoked during graceful shutdown after the mailbox has been drained.
    fn on_stop(&mut self, _ctx: &mut ActorContext<Self>) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Optional ergonomic trait for sub-message types that map to a specific reply type from [`StatefulActor::Response`].
///
/// Enables calling [`ActorClient::ask`] to receive `M::Reply` directly without manual enum unpacking at the call site.
pub trait CallMessage<A: StatefulActor>: Into<A::Message> {
    /// The unpacked response type returned by [`ActorClient::ask`].
    type Reply;

    /// Extracts the typed reply from the actor's unified [`StatefulActor::Response`].
    fn extract_reply(resp: A::Response) -> Result<Self::Reply, A::Error>;
}

/// Error wrapper returned by [`ActorClient`] operations.
#[non_exhaustive]
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ActorError<E> {
    #[error("Actor task terminated unexpectedly or is unavailable")]
    ActorUnavailable,
    #[error("Actor mailbox is full (backpressure limit reached)")]
    MailboxFull,
    #[error("Supervisor error: {0}")]
    Supervisor(#[from] SupervisorError),
    #[error("Actor handler failed: {0}")]
    HandlerFailed(E),
    #[error("Actor operation timed out after {0:?}")]
    Timeout(Duration),
}

/// Internal outcome delivered over the per-call oneshot channel.
pub(crate) enum CallOutcome<A: StatefulActor> {
    /// The actor processed the message and produced a result.
    Processed(Result<A::Response, ActorError<A::Error>>),
    /// The actor aborted in `on_start` before inspecting the message, returning ownership to the caller.
    Unprocessed(A::Message),
}

/// Message protocol envelope processed by a [`StatefulActorRunner`].
pub(crate) enum ActorEnvelope<A: StatefulActor> {
    /// Request-response message (`call`).
    Call {
        msg: A::Message,
        respond_to: oneshot::Sender<CallOutcome<A>>,
    },
    /// Fire-and-forget message (`cast`).
    Cast { msg: A::Message },
    /// Requests a graceful shutdown: closes mailbox, drains pending messages, calls `on_stop`, and exits `Ok(())`.
    Shutdown,
    /// Explicit crash injection command for chaos and resilience testing.
    #[cfg(any(test, feature = "chaos_test"))]
    SimulateCrash,
}

// ============================================================================
// Configuration & Builder for StatefulActor
// ============================================================================

/// Configuration parameters for a [`StatefulActor`].
#[non_exhaustive]
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

/// Fluent builder for configuring and spawning a [`StatefulActor`] under a [`super::supervisor::Supervisor`].
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
    /// Sets the actor's bounded mailbox channel capacity (defaults to 256; clamped to `>= 1`).
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

    /// Sets an optional timeout for individual `handle()` and `handle_cast()` executions.
    #[must_use]
    pub fn handler_timeout(mut self, timeout: Duration) -> Self {
        self.config.handler_timeout = Some(timeout);
        self
    }

    /// Builds an OTP-style [`ChildSpec`] that can be registered with a [`super::supervisor::Supervisor`].
    pub fn build_child_spec(self) -> ChildSpec {
        let mut config = self.config;
        config.channel_capacity = config.channel_capacity.max(1);
        let actor_factory = self.factory;
        let channel_capacity = config.channel_capacity;
        let restart_policy = config.restart_policy;
        let restart_type = config.restart_type;
        let shutdown_policy = config.shutdown_policy;
        let name = config.name.clone();

        let factory: UntypedActorFactory = Arc::new(move |spawn_ctx: SpawnContext| {
            let (tx, rx) = mpsc::channel::<ActorEnvelope<A>>(channel_capacity);
            let weak_tx = tx.downgrade();
            let sender_arc = Arc::new(tx);
            let factory = actor_factory.clone();
            let cfg = config.clone();
            let task_handle = tokio::spawn(async move {
                let actor = (factory)(spawn_ctx.instance_id);
                let runner = StatefulActorRunner::new(
                    cfg,
                    actor,
                    rx,
                    weak_tx,
                    spawn_ctx.shutdown_rx,
                    spawn_ctx.supervisor,
                    spawn_ctx.instance_id,
                );
                runner.run().await
            });
            (sender_arc, task_handle)
        });

        let shutdown_signal: ShutdownSignalFn = Arc::new(|untyped| {
            if let Some(cmd_tx) = untyped.downcast_ref::<Sender<ActorEnvelope<A>>>() {
                let _ = cmd_tx.try_send(ActorEnvelope::Shutdown);
            }
        });

        #[allow(unused_mut)]
        let mut spec = ChildSpec::new(name, factory, restart_policy)
            .restart_type(restart_type)
            .shutdown_policy(shutdown_policy)
            .with_shutdown_signal(shutdown_signal);

        #[cfg(any(test, feature = "chaos_test"))]
        {
            let crash_signal: ShutdownSignalFn = Arc::new(|untyped| {
                if let Some(cmd_tx) = untyped.downcast_ref::<Sender<ActorEnvelope<A>>>() {
                    let _ = cmd_tx.try_send(ActorEnvelope::SimulateCrash);
                }
            });
            spec = spec.with_crash_signal(crash_signal);
        }

        spec
    }

    /// Spawns the actor under the given [`SupervisorHandle`] and returns a direct [`ActorClient`].
    pub async fn spawn(
        self,
        supervisor: &SupervisorHandle,
    ) -> Result<ActorClient<A>, SupervisorError> {
        let name = self.config.name.clone();
        let spec = self.build_child_spec();
        supervisor.start_child(spec).await?;
        supervisor.get_actor_client::<A>(&name).await
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
    weak_sender: WeakSender<ActorEnvelope<A>>,
    shutdown_rx: watch::Receiver<bool>,
    supervisor: SupervisorHandle,
    instance_id: usize,
}

/// Drop guard broadcasting `true` on the instance lifecycle channel whenever the actor task returns or unwinds.
struct InstanceCancelGuard(watch::Sender<bool>);

impl InstanceCancelGuard {
    fn cancel(&self) {
        let _ = self.0.send(true);
    }
}

impl Drop for InstanceCancelGuard {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl<A: StatefulActor> StatefulActorRunner<A> {
    pub(crate) fn new(
        config: StatefulActorConfig,
        actor: A,
        receiver: Receiver<ActorEnvelope<A>>,
        weak_sender: WeakSender<ActorEnvelope<A>>,
        shutdown_rx: watch::Receiver<bool>,
        supervisor: SupervisorHandle,
        instance_id: usize,
    ) -> Self {
        Self {
            config,
            actor,
            receiver,
            weak_sender,
            shutdown_rx,
            supervisor,
            instance_id,
        }
    }

    /// Executes a synchronous request-response (`call`) message through `actor.handle` with optional timeout.
    async fn execute_call(
        &mut self,
        msg: A::Message,
        ctx: &mut ActorContext<A>,
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

    /// Executes a fire-and-forget (`cast`) message through `actor.handle_cast` with optional timeout.
    async fn execute_cast(
        &mut self,
        msg: A::Message,
        ctx: &mut ActorContext<A>,
    ) -> Result<(), ActorError<A::Error>> {
        if let Some(timeout) = self.config.handler_timeout {
            match tokio::time::timeout(timeout, self.actor.handle_cast(msg, ctx)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(ActorError::HandlerFailed(err)),
                Err(_) => {
                    ctx.fail(format!(
                        "StatefulActor '{}' #{} handle_cast timed out after {:?}",
                        self.config.name, self.instance_id, timeout
                    ));
                    Err(ActorError::Timeout(timeout))
                }
            }
        } else {
            self.actor
                .handle_cast(msg, ctx)
                .await
                .map_err(ActorError::HandlerFailed)
        }
    }

    /// Drains remaining queued messages in the mailbox and runs `on_stop` before exiting cleanly.
    async fn finish_graceful_shutdown(
        &mut self,
        ctx: &mut ActorContext<A>,
        cancel_guard: &InstanceCancelGuard,
    ) -> TaskResult {
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
                    let res = self.execute_call(msg, ctx).await;
                    let _ = respond_to.send(CallOutcome::Processed(res));
                    if let ActorControl::Fail(reason) = &ctx.control {
                        return Err(reason.clone());
                    }
                }
                ActorEnvelope::Cast { msg } => {
                    if let Err(err) = self.execute_cast(msg, ctx).await {
                        tracing::warn!(
                            actor = %self.config.name,
                            instance = self.instance_id,
                            error = ?err,
                            "StatefulActor cast handler returned error during shutdown drain."
                        );
                    }
                    if let ActorControl::Fail(reason) = &ctx.control {
                        return Err(reason.clone());
                    }
                }
                ActorEnvelope::Shutdown => continue,
                #[cfg(any(test, feature = "chaos_test"))]
                ActorEnvelope::SimulateCrash => break,
            }
        }

        cancel_guard.cancel();
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
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let cancel_guard = InstanceCancelGuard(cancel_tx);

        let mut ctx = ActorContext::new(
            self.config.name.clone(),
            self.instance_id,
            self.weak_sender.clone(),
            cancel_rx,
            self.supervisor.clone(),
        );

        if let Err(err) = self.actor.on_start(&mut ctx).await {
            let reason = format!("{err:?}");
            tracing::error!(
                actor = %self.config.name,
                instance = self.instance_id,
                reason = %reason,
                "❌ StatefulActor failed in on_start hook."
            );
            // Return any queued `Call` messages untouched so callers can rebind without losing non-Clone payloads
            self.receiver.close();
            while let Ok(env) = self.receiver.try_recv() {
                if let ActorEnvelope::Call { msg, respond_to } = env {
                    let _ = respond_to.send(CallOutcome::Unprocessed(msg));
                }
            }
            return Err(reason);
        }

        match &ctx.control {
            ActorControl::Continue => {}
            ActorControl::Stop => {
                self.receiver.close();
                cancel_guard.cancel();
                self.actor.on_stop(&mut ctx).await;
                return Ok(());
            }
            ActorControl::Fail(reason) => {
                let reason_owned = reason.clone();
                self.receiver.close();
                while let Ok(env) = self.receiver.try_recv() {
                    if let ActorEnvelope::Call { msg, respond_to } = env {
                        let _ = respond_to.send(CallOutcome::Unprocessed(msg));
                    }
                }
                return Err(reason_owned);
            }
        }

        loop {
            if *self.shutdown_rx.borrow() {
                return self.finish_graceful_shutdown(&mut ctx, &cancel_guard).await;
            }

            tokio::select! {
                biased;

                Ok(()) = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        return self.finish_graceful_shutdown(&mut ctx, &cancel_guard).await;
                    }
                }

                env_opt = self.receiver.recv() => {
                    let Some(envelope) = env_opt else {
                        cancel_guard.cancel();
                        self.actor.on_stop(&mut ctx).await;
                        return Ok(());
                    };

                    match envelope {
                        ActorEnvelope::Call { msg, respond_to } => {
                            let res = self.execute_call(msg, &mut ctx).await;
                            if !matches!(ctx.control, ActorControl::Continue) {
                                // Close the mailbox before waking the caller so any immediate
                                // follow-up call sees a closed channel and triggers rebinding
                                // rather than queuing into the dying actor's buffer.
                                self.receiver.close();
                            }
                            let _ = respond_to.send(CallOutcome::Processed(res));

                            match &ctx.control {
                                ActorControl::Continue => {}
                                ActorControl::Stop => {
                                    return self.finish_graceful_shutdown(&mut ctx, &cancel_guard).await;
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
                            if let Err(err) = self.execute_cast(msg, &mut ctx).await {
                                tracing::warn!(
                                    actor = %self.config.name,
                                    instance = self.instance_id,
                                    error = ?err,
                                    "StatefulActor cast handler returned error."
                                );
                            }
                            if !matches!(ctx.control, ActorControl::Continue) {
                                self.receiver.close();
                            }

                            match &ctx.control {
                                ActorControl::Continue => {}
                                ActorControl::Stop => {
                                    return self.finish_graceful_shutdown(&mut ctx, &cancel_guard).await;
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
                            return self.finish_graceful_shutdown(&mut ctx, &cancel_guard).await;
                        }

                        #[cfg(any(test, feature = "chaos_test"))]
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

/// Approximate point-in-time queue statistics for an [`ActorClient`]'s bounded mailbox channel.
///
/// Returned synchronously by [`ActorClient::queue_stats`] based on the currently cached channel
/// sender (`current_sender()`). Because values are read lock-free without contacting the supervisor
/// or triggering channel rebinding:
///
/// - `queued` and `available` are approximate point-in-time snapshots under concurrent sends/receives.
/// - Immediately after an actor crash or restart (and *before* the next `call` / `cast` rebinds the
///   client), `queue_stats()` still inspects the previous instance's dead mailbox, reporting
///   `is_closed == true`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueStats {
    /// Total bounded capacity configured for this mailbox (`StatefulActorBuilder::channel_capacity`).
    pub max_capacity: usize,
    /// Approximate remaining free slots currently available before backpressure.
    pub available: usize,
    /// Approximate number of queued messages or reserved permits (`max_capacity.saturating_sub(available)`).
    pub queued: usize,
    /// `true` if the currently cached mailbox channel is closed.
    pub is_closed: bool,
}

impl QueueStats {
    /// Returns `true` if no free slots are currently available in the cached mailbox (`available == 0`).
    pub fn is_full(&self) -> bool {
        self.available == 0
    }
}

/// High-performance, self-healing 1-hop Data Plane client for a [`StatefulActor`].
pub struct ActorClient<A: StatefulActor> {
    name: String,
    sender: Arc<RwLock<Sender<ActorEnvelope<A>>>>,
    slot_rx: Arc<RwLock<Option<watch::Receiver<SlotState>>>>,
    supervisor: SupervisorHandle,
}

impl<A: StatefulActor> Clone for ActorClient<A> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            sender: self.sender.clone(),
            slot_rx: self.slot_rx.clone(),
            supervisor: self.supervisor.clone(),
        }
    }
}

impl<A: StatefulActor> ActorClient<A> {
    /// Creates a new direct client handle subscribed to the child's [`SlotState`] watch channel.
    pub(crate) fn new(
        name: impl Into<String>,
        sender: Sender<ActorEnvelope<A>>,
        slot_rx: watch::Receiver<SlotState>,
        supervisor: SupervisorHandle,
    ) -> Self {
        Self {
            name: name.into(),
            sender: Arc::new(RwLock::new(sender)),
            slot_rx: Arc::new(RwLock::new(Some(slot_rx))),
            supervisor,
        }
    }

    /// Creates a direct client handle (used by [`ActorContext::client`]) that lazily subscribes to [`SlotState`] on first rebind.
    pub(crate) fn new_unsubscribed(
        name: impl Into<String>,
        sender: Sender<ActorEnvelope<A>>,
        supervisor: SupervisorHandle,
    ) -> Self {
        Self {
            name: name.into(),
            sender: Arc::new(RwLock::new(sender)),
            slot_rx: Arc::new(RwLock::new(None)),
            supervisor,
        }
    }

    /// Returns the name of the supervised actor.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the name of the [`SupervisorHandle`] managing this actor.
    pub fn service_name(&self) -> &str {
        self.supervisor.name()
    }

    /// Returns a reference to the [`SupervisorHandle`] managing this actor.
    pub fn supervisor(&self) -> &SupervisorHandle {
        &self.supervisor
    }

    /// Returns a clone of the currently cached channel sender without holding the lock across `.await`.
    fn current_sender(&self) -> Sender<ActorEnvelope<A>> {
        self.sender
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Returns an approximate point-in-time snapshot of this client's bounded mailbox queue.
    ///
    /// Reads only the locally cached sender (`current_sender()`) without contacting the supervisor
    /// or waiting on locks/channels. As a result, concurrent senders/receivers may shift counts
    /// immediately after return, and after an actor crash (before the next `call`/`cast` triggers
    /// rebinding) this reflects the closed mailbox of the exited instance (`is_closed == true`).
    pub fn queue_stats(&self) -> QueueStats {
        let tx = self.current_sender();
        let max_capacity = tx.max_capacity();
        let available = tx.capacity().min(max_capacity);
        QueueStats {
            max_capacity,
            available,
            queued: max_capacity.saturating_sub(available),
            is_closed: tx.is_closed(),
        }
    }

    /// Reactively waits on the [`SlotState`] watch channel until a replacement actor instance is running,
    /// or returns immediately if the actor was stopped or permanently failed.
    async fn rebind_channel(
        &self,
        failed_sender: &Sender<ActorEnvelope<A>>,
    ) -> Result<(), ActorError<A::Error>> {
        {
            let current = self.current_sender();
            if !current.is_closed() && !current.same_channel(failed_sender) {
                return Ok(());
            }
        }

        loop {
            let maybe_rx = self
                .slot_rx
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();

            let mut rx = match maybe_rx {
                Some(rx) => rx,
                None => {
                    let resolved = self
                        .supervisor
                        .resolve_child_slot(&self.name)
                        .await
                        .map_err(ActorError::Supervisor)?;
                    *self
                        .slot_rx
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(resolved.clone());
                    resolved
                }
            };

            let wait_state = rx
                .wait_for(|state| match state {
                    SlotState::Running { endpoint, .. } => endpoint
                        .downcast_ref::<Sender<ActorEnvelope<A>>>()
                        .is_some_and(|tx| !tx.is_closed() && !tx.same_channel(failed_sender)),
                    SlotState::Stopped | SlotState::Failed(_) => true,
                    SlotState::Restarting { .. } => false,
                })
                .await
                .ok()
                .map(|s| s.clone());

            if let Some(state_ref) = wait_state {
                match &state_ref {
                    SlotState::Running { endpoint, .. } => {
                        if let Some(new_tx) = endpoint.downcast_ref::<Sender<ActorEnvelope<A>>>() {
                            *self
                                .sender
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                new_tx.clone();
                            tracing::info!(
                                actor = %self.name,
                                "🔄 ActorClient channel re-established with replacement actor!"
                            );
                            return Ok(());
                        }
                        return Err(ActorError::Supervisor(SupervisorError::ChildTypeMismatch(
                            self.name.clone(),
                        )));
                    }
                    SlotState::Failed(err) => return Err(ActorError::Supervisor(err.clone())),
                    SlotState::Stopped => {
                        return Err(ActorError::Supervisor(SupervisorError::ChildNotFound(
                            self.name.clone(),
                        )));
                    }
                    SlotState::Restarting { .. } => unreachable!(),
                }
            }

            // The watch::Sender in the parent supervisor was dropped (e.g. parent supervisor restarted).
            // Re-resolve the slot from `self.supervisor`.
            let new_rx = self
                .supervisor
                .resolve_child_slot(&self.name)
                .await
                .map_err(ActorError::Supervisor)?;
            *self
                .slot_rx
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(new_rx);
        }
    }

    /// Sends a request message (`call`) directly to the actor and awaits its response.
    ///
    /// If the actor's channel was closed due to a prior crash (or aborted during `on_start`),
    /// `call` recovers the original message without requiring `A::Message: Clone`, reactively
    /// awaits the replacement actor via `watch`, and delivers the message transparently.
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
                Ok(()) => match rx.await {
                    Ok(CallOutcome::Processed(res)) => return res,
                    Ok(CallOutcome::Unprocessed(returned_msg)) => {
                        msg = returned_msg;
                        if attempt < MAX_ATTEMPTS {
                            self.rebind_channel(&current_sender).await?;
                        }
                    }
                    Err(_) => {
                        // The actor crashed mid-flight while processing the message (`msg` was consumed).
                        return Err(ActorError::ActorUnavailable);
                    }
                },
                Err(mpsc::error::SendError(ActorEnvelope::Call {
                    msg: returned_msg, ..
                })) => {
                    msg = returned_msg;
                    if attempt < MAX_ATTEMPTS {
                        self.rebind_channel(&current_sender).await?;
                    }
                }
                Err(_) => unreachable!(),
            }
        }

        Err(ActorError::ActorUnavailable)
    }

    /// Sends a typed [`CallMessage<A>`] and extracts its specific [`CallMessage::Reply`].
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, ActorError<A::Error>>
    where
        M: CallMessage<A>,
    {
        let resp = self.call(msg.into()).await?;
        M::extract_reply(resp).map_err(ActorError::HandlerFailed)
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
                && let Ok(CallOutcome::Processed(res)) = rx.await
            {
                return res;
            }

            if attempt < MAX_ATTEMPTS {
                self.rebind_channel(&current_sender).await?;
            }
        }

        Err(ActorError::ActorUnavailable)
    }

    /// Sends a cloneable request message (`call_retry`) with a client-side timeout deadline.
    pub async fn call_retry_timeout(
        &self,
        msg: A::Message,
        timeout: Duration,
    ) -> Result<A::Response, ActorError<A::Error>>
    where
        A::Message: Clone,
    {
        tokio::time::timeout(timeout, self.call_retry(msg))
            .await
            .map_err(|_| ActorError::Timeout(timeout))?
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
    /// Automatically rebinds via the reactive [`SlotState`] watch channel if the mailbox was closed by a prior crash.
    pub async fn cast(&self, mut msg: A::Message) -> Result<(), ActorError<A::Error>> {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_sender = self.current_sender();

            match current_sender.send(ActorEnvelope::Cast { msg }).await {
                Ok(()) => return Ok(()),
                Err(mpsc::error::SendError(ActorEnvelope::Cast { msg: returned_msg })) => {
                    msg = returned_msg;
                    if attempt < MAX_ATTEMPTS {
                        self.rebind_channel(&current_sender).await?;
                    }
                }
                Err(_) => unreachable!(),
            }
        }

        Err(ActorError::ActorUnavailable)
    }

    /// Attempts to enqueue a fire-and-forget notification (`try_cast`) without blocking.
    ///
    /// Returns [`ActorError::MailboxFull`] immediately if the actor's bounded mailbox has reached capacity,
    /// or [`ActorError::ActorUnavailable`] if the mailbox is currently closed.
    pub fn try_cast(&self, msg: A::Message) -> Result<(), ActorError<A::Error>> {
        let current_sender = self.current_sender();
        match current_sender.try_send(ActorEnvelope::Cast { msg }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(ActorError::MailboxFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(ActorError::ActorUnavailable),
        }
    }

    /// Sends an asynchronous fire-and-forget notification (`cast`) with a client-side timeout deadline.
    pub async fn cast_timeout(
        &self,
        msg: A::Message,
        timeout: Duration,
    ) -> Result<(), ActorError<A::Error>> {
        tokio::time::timeout(timeout, self.cast(msg))
            .await
            .map_err(|_| ActorError::Timeout(timeout))?
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
                self.rebind_channel(&current_sender).await?;
            }
        }

        Err(ActorError::ActorUnavailable)
    }

    /// Requests a graceful shutdown of the actor over its direct data-plane channel.
    pub async fn shutdown(&self) -> Result<(), ActorError<A::Error>> {
        self.send_control(|| ActorEnvelope::Shutdown).await
    }

    /// Injects an intentional crash command directly into the actor for resilience testing.
    #[cfg(any(test, feature = "chaos_test"))]
    pub async fn simulate_crash(&self) -> Result<(), ActorError<A::Error>> {
        self.send_control(|| ActorEnvelope::SimulateCrash).await
    }
}

//! # Service Supervisor Implementation
//!
//! A completely type-agnostic domain-level supervisor actor.
//! Responsible for receiving dynamic self-registrations from arbitrary worker actors,
//! monitoring them, and auto-healing them according to the configured [`SupervisionStrategy`]
//! (`OneForOne`, `OneForAll`, `RestForOne`).
//! Registers itself autonomously with the [`super::root_supervisor::RootSupervisor`].

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{
    RwLock,
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tokio::task::{JoinError, JoinHandle, JoinSet};

use super::generic_actor::{ResourceClient, ResourceCommand};
use super::model::{
    RestartPolicy, RestartTracker, RestartType, RootError, ServiceError, ShutdownPolicy,
    SupervisionStrategy, TaskResult,
};
use super::root_supervisor::{RootSupervisorHandle, ServiceFactory};
use super::stateful_actor::{ActorClient, ActorEnvelope, StatefulActor};

/// Type-erased sender handle stored by the supervisor for dynamic client resolution.
pub type UntypedSender = Arc<dyn Any + Send + Sync>;

/// Type-erased factory closure invoked by the supervisor to restart crashed actors.
pub type UntypedActorFactory =
    Arc<dyn Fn(usize) -> (UntypedSender, JoinHandle<TaskResult>) + Send + Sync + 'static>;

/// Optional callback invoked by the supervisor to signal a graceful shutdown to a child actor.
pub type ShutdownSignalFn = Arc<dyn Fn(&UntypedSender) + Send + Sync + 'static>;

/// Specification defining a child actor to be supervised by a [`ServiceSupervisor`].
#[derive(Clone)]
pub struct ChildSpec {
    pub name: String,
    pub factory: UntypedActorFactory,
    pub restart_policy: RestartPolicy,
    pub restart_type: RestartType,
    pub shutdown_policy: ShutdownPolicy,
    pub shutdown_signal: Option<ShutdownSignalFn>,
}

impl ChildSpec {
    /// Creates a new child specification with default [`RestartType::Transient`] and [`ShutdownPolicy`].
    pub fn new(
        name: impl Into<String>,
        factory: UntypedActorFactory,
        restart_policy: RestartPolicy,
    ) -> Self {
        Self {
            name: name.into(),
            factory,
            restart_policy,
            restart_type: RestartType::default(),
            shutdown_policy: ShutdownPolicy::default(),
            shutdown_signal: None,
        }
    }

    /// Sets the OTP restart type (`Transient`, `Permanent`, or `Temporary`).
    #[must_use]
    pub fn restart_type(mut self, restart_type: RestartType) -> Self {
        self.restart_type = restart_type;
        self
    }

    /// Sets the shutdown policy (`Timeout(Duration)`, `BrutalKill`, or `Infinity`).
    #[must_use]
    pub fn shutdown_policy(mut self, policy: ShutdownPolicy) -> Self {
        self.shutdown_policy = policy;
        self
    }

    /// Convenience method setting [`ShutdownPolicy::Timeout`] for graceful shutdown.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_policy = ShutdownPolicy::Timeout(timeout);
        self
    }

    /// Attaches a shutdown signal callback invoked when the supervisor initiates a graceful shutdown.
    #[must_use]
    pub fn with_shutdown_signal(mut self, signal: ShutdownSignalFn) -> Self {
        self.shutdown_signal = Some(signal);
        self
    }
}

/// Fluent builder for creating and configuring a [`ServiceSupervisor`] with [`ChildSpec`]s.
pub struct ServiceSupervisorBuilder {
    name: String,
    strategy: SupervisionStrategy,
    child_specs: Vec<ChildSpec>,
    restart_policy: RestartPolicy,
    shutdown_policy: ShutdownPolicy,
    escalate_on_failure: bool,
}

impl ServiceSupervisorBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            strategy: SupervisionStrategy::default(),
            child_specs: Vec::new(),
            restart_policy: RestartPolicy::default(),
            // Per Erlang/OTP convention, a supervisor child defaults to Infinity so it can
            // sequentially run each of its own children's bounded ShutdownPolicies without
            // being prematurely aborted by its parent.
            shutdown_policy: ShutdownPolicy::Infinity,
            escalate_on_failure: false,
        }
    }

    #[must_use]
    pub fn strategy(mut self, strategy: SupervisionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    #[must_use]
    pub fn restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.restart_policy = policy;
        self
    }

    /// Sets the shutdown policy governing how the [`super::root_supervisor::RootSupervisor`] terminates this service supervisor.
    #[must_use]
    pub fn shutdown_policy(mut self, policy: ShutdownPolicy) -> Self {
        self.shutdown_policy = policy;
        self
    }

    /// Convenience method setting [`ShutdownPolicy::Timeout`] on this service supervisor.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_policy = ShutdownPolicy::Timeout(timeout);
        self
    }

    /// Configures whether child crash-loops should escalate to the RootSupervisor.
    /// When enabled (`true`), exceeding `max_restarts` causes the ServiceSupervisor to terminate,
    /// triggering a supervisor-level restart by the RootSupervisor.
    /// Default: `false` (marks child as permanently failed).
    #[must_use]
    pub fn escalate_on_failure(mut self, escalate: bool) -> Self {
        self.escalate_on_failure = escalate;
        self
    }

    #[must_use]
    pub fn add_child(mut self, spec: impl Into<ChildSpec>) -> Self {
        let child = spec.into();
        if self.child_specs.iter().any(|s| s.name == child.name) {
            tracing::warn!(
                supervisor = %self.name,
                child = %child.name,
                "⚠️ Duplicate child name in ServiceSupervisorBuilder::add_child ignored."
            );
        } else {
            self.child_specs.push(child);
        }
        self
    }

    pub async fn spawn(
        self,
        root: &RootSupervisorHandle,
    ) -> Result<ServiceSupervisorHandle, RootError> {
        ServiceSupervisor::spawn_with_specs_and_policy_with_shutdown(
            self.name,
            self.strategy,
            self.child_specs,
            self.restart_policy,
            self.shutdown_policy,
            self.escalate_on_failure,
            root,
        )
        .await
    }
}

/// Commands handled by the [`ServiceSupervisor`] actor loop.
pub enum ServiceSupervisorCommand {
    /// Starts and supervises a child actor from a [`ChildSpec`].
    StartChild {
        spec: ChildSpec,
        respond_to: oneshot::Sender<Result<(), ServiceError>>,
    },
    /// Resolve the current active communication channel for a registered actor.
    ResolveActorChannel {
        actor_name: String,
        respond_to: oneshot::Sender<Result<UntypedSender, ServiceError>>,
    },
    /// Manually restarts or recovers a child actor that has permanently failed or needs a reset.
    RestartChild {
        actor_name: String,
        respond_to: oneshot::Sender<Result<(), ServiceError>>,
    },
    /// Gracefully stops a specific supervised child actor according to its [`ShutdownPolicy`].
    StopChild {
        actor_name: String,
        respond_to: oneshot::Sender<Result<(), ServiceError>>,
    },
    /// Gracefully stops (if running) and completely removes a child actor's specification and state.
    DeleteChild {
        actor_name: String,
        respond_to: oneshot::Sender<Result<(), ServiceError>>,
    },
    /// Gracefully shuts down all supervised child actors and terminates the service supervisor.
    Shutdown {
        respond_to: oneshot::Sender<()>,
    },
    /// Simulate an intentional crash in the service supervisor itself.
    SimulateCrash,
}

/// Metadata tracked for each actively supervised actor.
struct ActiveActor {
    sender: UntypedSender,
    abort_handle: tokio::task::AbortHandle,
    term_rx: watch::Receiver<bool>,
    factory: UntypedActorFactory,
    restart_type: RestartType,
    shutdown_policy: ShutdownPolicy,
    shutdown_signal: Option<ShutdownSignalFn>,
    instance_id: usize,
    restart_ticket: u64,
    restart_tracker: RestartTracker,
    is_failed: bool,
    is_stopped: bool,
    is_stopping: bool,
    is_restarting: bool,
    pending_resolutions: Vec<oneshot::Sender<Result<UntypedSender, ServiceError>>>,
}

impl Drop for ActiveActor {
    fn drop(&mut self) {
        // Automatically abort the child task when the supervisor drops or terminates
        self.abort_handle.abort();
    }
}

/// Drop guard ensuring `term_tx.send(true)` is always broadcast when a monitored task exits or is aborted.
struct TermSignalGuard(watch::Sender<bool>);

impl Drop for TermSignalGuard {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// The Service Supervisor actor managing a collection of dynamically registered actors.
pub struct ServiceSupervisor {
    name: String,
    instance_id: usize,
    strategy: SupervisionStrategy,
    child_specs: Arc<RwLock<Vec<ChildSpec>>>,
    escalate_on_failure: bool,
    boot_gate: Option<oneshot::Receiver<()>>,
    actors: HashMap<String, ActiveActor>,
    actor_order: Vec<String>,
    next_restart_ticket: u64,
    join_set: JoinSet<(String, usize, Result<TaskResult, JoinError>)>,
}

impl ServiceSupervisor {
    /// Returns a new fluent builder for constructing a [`ServiceSupervisor`] with child specifications.
    pub fn builder(name: impl Into<String>) -> ServiceSupervisorBuilder {
        ServiceSupervisorBuilder::new(name)
    }

    /// Initializes a service supervisor with a specified strategy, shared child specifications, and escalation setting.
    pub(crate) fn with_strategy_specs_and_escalate(
        name: impl Into<String>,
        instance_id: usize,
        strategy: SupervisionStrategy,
        child_specs: Arc<RwLock<Vec<ChildSpec>>>,
        escalate_on_failure: bool,
    ) -> Self {
        Self {
            name: name.into(),
            instance_id,
            strategy,
            child_specs,
            escalate_on_failure,
            boot_gate: None,
            actors: HashMap::new(),
            actor_order: Vec::new(),
            next_restart_ticket: 0,
            join_set: JoinSet::new(),
        }
    }

    /// Allocates a monotonically increasing ticket to uniquely identify a child spawn or restart wave.
    fn allocate_restart_ticket(&mut self) -> u64 {
        self.next_restart_ticket = self.next_restart_ticket.wrapping_add(1);
        self.next_restart_ticket
    }

    /// Boots a [`ServiceSupervisor`] with a collection of [`ChildSpec`]s and registers with the root supervisor.
    pub async fn spawn_with_specs(
        name: impl Into<String>,
        strategy: SupervisionStrategy,
        child_specs: Vec<ChildSpec>,
        root: &RootSupervisorHandle,
    ) -> Result<ServiceSupervisorHandle, RootError> {
        Self::spawn_with_specs_and_policy(
            name,
            strategy,
            child_specs,
            RestartPolicy::default(),
            root,
        )
        .await
    }

    /// Boots a [`ServiceSupervisor`] with ChildSpecs and a specific RestartPolicy for supervisor restarts.
    pub async fn spawn_with_specs_and_policy(
        name: impl Into<String>,
        strategy: SupervisionStrategy,
        child_specs: Vec<ChildSpec>,
        restart_policy: RestartPolicy,
        root: &RootSupervisorHandle,
    ) -> Result<ServiceSupervisorHandle, RootError> {
        Self::spawn_with_specs_and_policy_full(
            name,
            strategy,
            child_specs,
            restart_policy,
            false,
            root,
        )
        .await
    }

    /// Boots a [`ServiceSupervisor`] with ChildSpecs, RestartPolicy, and explicit escalation behavior.
    pub async fn spawn_with_specs_and_policy_full(
        name: impl Into<String>,
        strategy: SupervisionStrategy,
        child_specs: Vec<ChildSpec>,
        restart_policy: RestartPolicy,
        escalate_on_failure: bool,
        root: &RootSupervisorHandle,
    ) -> Result<ServiceSupervisorHandle, RootError> {
        Self::spawn_with_specs_and_policy_with_shutdown(
            name,
            strategy,
            child_specs,
            restart_policy,
            ShutdownPolicy::Infinity,
            escalate_on_failure,
            root,
        )
        .await
    }

    /// Boots a [`ServiceSupervisor`] with ChildSpecs, RestartPolicy, ShutdownPolicy, and explicit escalation behavior.
    pub async fn spawn_with_specs_and_policy_with_shutdown(
        name: impl Into<String>,
        strategy: SupervisionStrategy,
        child_specs: Vec<ChildSpec>,
        restart_policy: RestartPolicy,
        shutdown_policy: ShutdownPolicy,
        escalate_on_failure: bool,
        root: &RootSupervisorHandle,
    ) -> Result<ServiceSupervisorHandle, RootError> {
        let name = name.into();
        let (tx, rx) = mpsc::channel(256);
        let handle = ServiceSupervisorHandle::new(name.clone(), tx.clone(), Some(root.clone()));

        let shared_specs = Arc::new(RwLock::new(child_specs));
        let name_for_factory = name.clone();
        let factory_strategy = strategy;
        let factory_specs = shared_specs.clone();
        let factory_escalate = escalate_on_failure;

        let factory: ServiceFactory = Arc::new(move |instance_id| {
            let (new_tx, new_rx) = mpsc::channel(256);
            let supervisor = ServiceSupervisor::with_strategy_specs_and_escalate(
                name_for_factory.clone(),
                instance_id,
                factory_strategy,
                factory_specs.clone(),
                factory_escalate,
            );
            let task_handle = tokio::spawn(supervisor.run(new_rx));
            (new_tx, task_handle)
        });

        let (boot_tx, boot_rx) = oneshot::channel();
        let mut supervisor = ServiceSupervisor::with_strategy_specs_and_escalate(
            name.clone(),
            1,
            strategy,
            shared_specs,
            escalate_on_failure,
        );
        supervisor.boot_gate = Some(boot_rx);
        let task_handle = tokio::spawn(supervisor.run(rx));

        // The ServiceSupervisor registers ITSELF with the RootSupervisor before booting children!
        root.register_service_self_with_policies(
            name,
            tx,
            task_handle,
            factory,
            restart_policy,
            shutdown_policy,
        )
        .await?;

        let _ = boot_tx.send(());

        Ok(handle)
    }

    /// Autonomous bootstrap: Instantiates the ServiceSupervisor and registers ITSELF
    /// with the specified [`RootSupervisorHandle`]. Defaults to OneForOne strategy.
    pub async fn spawn_and_register(
        name: impl Into<String>,
        root: &RootSupervisorHandle,
    ) -> Result<ServiceSupervisorHandle, RootError> {
        Self::spawn_with_specs(name, SupervisionStrategy::default(), Vec::new(), root).await
    }

    /// Autonomous bootstrap with a specific supervision strategy (e.g. OneForAll, RestForOne).
    pub async fn spawn_and_register_with_strategy(
        name: impl Into<String>,
        strategy: SupervisionStrategy,
        root: &RootSupervisorHandle,
    ) -> Result<ServiceSupervisorHandle, RootError> {
        Self::spawn_with_specs(name, strategy, Vec::new(), root).await
    }

    /// Spawns a child actor from a ChildSpec, tracking it in join_set and actors registry.
    fn spawn_child_from_spec(&mut self, spec: ChildSpec) -> Result<(), ServiceError> {
        let name = spec.name;
        if let Some(existing) = self.actors.get(&name)
            && !existing.is_stopped
        {
            tracing::warn!(
                supervisor = %self.name,
                instance = self.instance_id,
                child = %name,
                "Child actor is already running; skipping duplicate spawn."
            );
            return Ok(());
        }

        let inst_id = self
            .actors
            .get(&name)
            .map_or(1, |entry| entry.instance_id + 1);

        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            child = %name,
            child_instance = inst_id,
            restart_type = ?spec.restart_type,
            shutdown_policy = ?spec.shutdown_policy,
            "🚀 Spawning child actor from ChildSpec..."
        );

        let spawn_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (spec.factory)(inst_id)
        }));
        let (sender, task_handle) = match spawn_res {
            Ok(pair) => pair,
            Err(panic_err) => {
                tracing::error!(
                    supervisor = %self.name,
                    instance = self.instance_id,
                    child = %name,
                    panic = ?panic_err,
                    "💥 Child actor factory panicked during spawn!"
                );
                return Err(ServiceError::ServiceUnavailable);
            }
        };
        let abort_handle = task_handle.abort_handle();
        let (term_tx, term_rx) = watch::channel(false);
        let name_for_task = name.clone();
        let ticket = self.allocate_restart_ticket();

        self.join_set.spawn(async move {
            let _guard = TermSignalGuard(term_tx);
            let res = task_handle.await;
            (name_for_task, inst_id, res)
        });

        if !self.actor_order.contains(&name) {
            self.actor_order.push(name.clone());
        }

        self.actors.insert(
            name,
            ActiveActor {
                sender,
                abort_handle,
                term_rx,
                factory: spec.factory,
                restart_type: spec.restart_type,
                shutdown_policy: spec.shutdown_policy,
                shutdown_signal: spec.shutdown_signal,
                instance_id: inst_id,
                restart_ticket: ticket,
                restart_tracker: RestartTracker::new(spec.restart_policy),
                is_failed: false,
                is_stopped: false,
                is_stopping: false,
                is_restarting: false,
                pending_resolutions: Vec::new(),
            },
        );
        Ok(())
    }

    /// Signals graceful shutdown (or brutal kill) to a running child actor according to its
    /// [`ShutdownPolicy`] and returns a watch receiver that resolves once the task terminates.
    fn initiate_actor_termination(
        supervisor_name: String,
        child_name: String,
        entry: &mut ActiveActor,
    ) -> watch::Receiver<bool> {
        let policy = entry.shutdown_policy;
        let abort_handle = entry.abort_handle.clone();
        let term_rx = entry.term_rx.clone();

        match policy {
            ShutdownPolicy::BrutalKill => {
                tracing::info!(
                    supervisor = %supervisor_name,
                    child = %child_name,
                    "⚡ BrutalKill policy: Immediately aborting child actor."
                );
                entry.sender = Arc::new(());
                abort_handle.abort();
            }
            ShutdownPolicy::Timeout(timeout) => {
                tracing::info!(
                    supervisor = %supervisor_name,
                    child = %child_name,
                    timeout = ?timeout,
                    "🛑 Signaling graceful shutdown to child actor with timeout..."
                );
                if let Some(sig) = &entry.shutdown_signal {
                    sig(&entry.sender);
                }
                entry.sender = Arc::new(());

                let mut rx_for_timeout = term_rx.clone();
                tokio::spawn(async move {
                    let timed_out = tokio::time::timeout(timeout, async {
                        let _ = rx_for_timeout.wait_for(|&done| done).await;
                    })
                    .await
                    .is_err();
                    if timed_out {
                        tracing::warn!(
                            supervisor = %supervisor_name,
                            child = %child_name,
                            timeout = ?timeout,
                            "⚠️ Child actor exceeded graceful shutdown timeout; forcefully aborting!"
                        );
                        abort_handle.abort();
                    }
                });
            }
            ShutdownPolicy::Infinity => {
                tracing::info!(
                    supervisor = %supervisor_name,
                    child = %child_name,
                    "🛑 Signaling graceful shutdown to child actor (Infinity policy)..."
                );
                if let Some(sig) = &entry.shutdown_signal {
                    sig(&entry.sender);
                }
                entry.sender = Arc::new(());
            }
        }

        term_rx
    }

    /// Initiates a non-blocking shutdown of a single child actor according to its [`ShutdownPolicy`].
    fn trigger_child_shutdown(
        supervisor_name: String,
        child_name: String,
        entry: &mut ActiveActor,
        restart_ticket: u64,
        respond_to: Option<oneshot::Sender<Result<(), ServiceError>>>,
    ) {
        entry.is_stopping = true;
        entry.is_stopped = true;
        entry.is_restarting = false;
        entry.restart_ticket = restart_ticket;
        for waiter in entry.pending_resolutions.drain(..) {
            let _ = waiter.send(Err(ServiceError::ActorNotFound(child_name.clone())));
        }

        let mut term_rx = Self::initiate_actor_termination(supervisor_name, child_name, entry);
        if let Some(tx) = respond_to {
            tokio::spawn(async move {
                let _ = term_rx.wait_for(|&done| done).await;
                let _ = tx.send(Ok(()));
            });
        }
    }

    /// Gracefully shuts down all supervised child actors sequentially in reverse registration order
    /// according to their configured [`ShutdownPolicy`].
    async fn graceful_shutdown_all(&mut self) {
        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            children = self.actors.len(),
            "🛑 ServiceSupervisor initiating sequential graceful shutdown of all children..."
        );

        let sup_name = self.name.clone();
        // Shut down children sequentially in reverse registration order (OTP standard)
        let ordered_children: Vec<String> = self.actor_order.iter().rev().cloned().collect();
        for child_name in ordered_children {
            let ticket = self.allocate_restart_ticket();
            let maybe_rx = if let Some(entry) = self.actors.get_mut(&child_name)
                && !entry.is_stopped
            {
                let (tx, rx) = oneshot::channel();
                Self::trigger_child_shutdown(sup_name.clone(), child_name, entry, ticket, Some(tx));
                Some(rx)
            } else {
                None
            };
            if let Some(rx) = maybe_rx {
                let _ = rx.await;
            }
        }

        // Reap all terminated child tasks from join_set
        while self.join_set.join_next().await.is_some() {}

        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            "✅ ServiceSupervisor graceful shutdown complete."
        );
    }

    /// Main event loop processing commands and monitoring child actor terminations.
    pub(crate) async fn run(
        mut self,
        mut receiver: Receiver<ServiceSupervisorCommand>,
    ) -> TaskResult {
        if let Some(gate) = self.boot_gate.take()
            && gate.await.is_err()
        {
            return Ok(());
        }

        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            strategy = ?self.strategy,
            "🛡️ Online. Booting children..."
        );

        // Boot initial child specifications
        let initial_specs = {
            let guard = self.child_specs.read().await;
            guard.clone()
        };
        for spec in initial_specs {
            if self.spawn_child_from_spec(spec.clone()).is_err() {
                let ticket = self.allocate_restart_ticket();
                let (term_tx, term_rx) = watch::channel(true);
                drop(term_tx);
                let dummy_abort = tokio::spawn(async {}).abort_handle();
                if !self.actor_order.contains(&spec.name) {
                    self.actor_order.push(spec.name.clone());
                }
                self.actors.insert(
                    spec.name,
                    ActiveActor {
                        sender: Arc::new(()),
                        abort_handle: dummy_abort,
                        term_rx,
                        factory: spec.factory,
                        restart_type: spec.restart_type,
                        shutdown_policy: spec.shutdown_policy,
                        shutdown_signal: spec.shutdown_signal,
                        instance_id: 1,
                        restart_ticket: ticket,
                        restart_tracker: RestartTracker::new(spec.restart_policy),
                        is_failed: true,
                        is_stopped: true,
                        is_stopping: false,
                        is_restarting: false,
                        pending_resolutions: Vec::new(),
                    },
                );
            }
        }

        let (scheduled_restart_tx, mut scheduled_restart_rx) =
            mpsc::channel::<(
                Vec<(String, u64)>,
                Option<oneshot::Sender<Result<(), ServiceError>>>,
            )>(64);

        loop {
            tokio::select! {
                Some((targets, maybe_respond_to)) = scheduled_restart_rx.recv() => {
                    let res = self.execute_restart(targets);
                    if let Some(respond_to) = maybe_respond_to {
                        let _ = respond_to.send(res);
                    }
                }

                cmd_opt = receiver.recv() => {
                    let Some(cmd) = cmd_opt else {
                        tracing::info!(
                            supervisor = %self.name,
                            instance = self.instance_id,
                            "All client handles dropped. Shutting down."
                        );
                        return Ok(());
                    };

                    match cmd {
                        ServiceSupervisorCommand::StartChild { spec, respond_to } => {
                            let already_running = self
                                .actors
                                .get(&spec.name)
                                .is_some_and(|a| !a.is_stopped);
                            if already_running {
                                let _ = respond_to.send(Err(ServiceError::ActorAlreadyExists(spec.name)));
                            } else {
                                match self.spawn_child_from_spec(spec.clone()) {
                                    Ok(()) => {
                                        {
                                            let mut guard = self.child_specs.write().await;
                                            if let Some(existing_spec) = guard.iter_mut().find(|s| s.name == spec.name) {
                                                *existing_spec = spec;
                                            } else {
                                                guard.push(spec);
                                            }
                                        }
                                        let _ = respond_to.send(Ok(()));
                                    }
                                    Err(err) => {
                                        let _ = respond_to.send(Err(err));
                                    }
                                }
                            }
                        }

                        ServiceSupervisorCommand::ResolveActorChannel { actor_name, respond_to } => {
                            if let Some(actor) = self.actors.get_mut(&actor_name) {
                                if actor.is_failed {
                                    let _ = respond_to.send(Err(ServiceError::MaxRestartsExceeded(
                                        actor_name,
                                        actor.restart_tracker.policy.max_restarts,
                                        actor.restart_tracker.policy.time_window,
                                    )));
                                } else if actor.is_stopped || actor.is_stopping {
                                    let _ = respond_to.send(Err(ServiceError::ActorNotFound(actor_name)));
                                } else if actor.is_restarting || *actor.term_rx.borrow() {
                                    actor.pending_resolutions.push(respond_to);
                                } else {
                                    let _ = respond_to.send(Ok(actor.sender.clone()));
                                }
                            } else {
                                let _ = respond_to.send(Err(ServiceError::ActorNotFound(actor_name)));
                            }
                        }

                        ServiceSupervisorCommand::RestartChild { actor_name, respond_to } => {
                            if !self.actors.contains_key(&actor_name) {
                                let _ = respond_to.send(Err(ServiceError::ActorNotFound(actor_name)));
                                continue;
                            }

                            tracing::info!(
                                supervisor = %self.name,
                                child = %actor_name,
                                strategy = ?self.strategy,
                                "🔄 Manual child restart triggered via ServiceSupervisor."
                            );

                            let actors_to_restart: Vec<String> = match self.strategy {
                                SupervisionStrategy::OneForOne => vec![actor_name.clone()],
                                SupervisionStrategy::OneForAll => self.actor_order.clone(),
                                SupervisionStrategy::RestForOne => {
                                    if let Some(pos) =
                                        self.actor_order.iter().position(|n| n == &actor_name)
                                    {
                                        self.actor_order[pos..].to_vec()
                                    } else {
                                        vec![actor_name.clone()]
                                    }
                                }
                            };

                            let mut term_receivers = Vec::new();
                            let mut temp_to_remove = Vec::new();
                            for target_name in actors_to_restart.iter().rev() {
                                let ticket = self.allocate_restart_ticket();
                                if let Some(entry) = self.actors.get_mut(target_name) {
                                    if target_name != &actor_name && entry.is_stopped {
                                        continue;
                                    }
                                    let is_running = !*entry.term_rx.borrow();
                                    if target_name != &actor_name
                                        && entry.restart_type == RestartType::Temporary
                                    {
                                        temp_to_remove.push((target_name.clone(), is_running));
                                        continue;
                                    }

                                    // Ensure manually restarted child is present in shared child_specs
                                    if target_name == &actor_name {
                                        let mut guard = self.child_specs.write().await;
                                        if !guard.iter().any(|s| s.name == *target_name) {
                                            guard.push(ChildSpec {
                                                name: target_name.clone(),
                                                factory: entry.factory.clone(),
                                                restart_policy: entry.restart_tracker.policy,
                                                restart_type: entry.restart_type,
                                                shutdown_policy: entry.shutdown_policy,
                                                shutdown_signal: entry.shutdown_signal.clone(),
                                            });
                                        }
                                    }

                                    entry.is_failed = false;
                                    entry.is_stopped = false;
                                    entry.is_stopping = false;
                                    entry.is_restarting = true;
                                    entry.restart_ticket = ticket;
                                    entry.restart_tracker =
                                        RestartTracker::new(entry.restart_tracker.policy);

                                    if is_running {
                                        let rx = Self::initiate_actor_termination(
                                            self.name.clone(),
                                            target_name.clone(),
                                            entry,
                                        );
                                        term_receivers.push(rx);
                                    }
                                }
                            }

                            for (temp_name, is_running) in temp_to_remove {
                                self.actor_order.retain(|n| n != &temp_name);
                                self.child_specs
                                    .write()
                                    .await
                                    .retain(|s| s.name != temp_name);
                                if let Some(mut entry) = self.actors.remove(&temp_name) {
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(ServiceError::ActorNotFound(
                                            temp_name.clone(),
                                        )));
                                    }
                                    if is_running {
                                        let mut rx = Self::initiate_actor_termination(
                                            self.name.clone(),
                                            temp_name,
                                            &mut entry,
                                        );
                                        tokio::spawn(async move {
                                            let _ = rx.wait_for(|&done| done).await;
                                            drop(entry);
                                        });
                                    }
                                }
                            }

                            let targets: Vec<(String, u64)> = actors_to_restart
                                .iter()
                                .filter_map(|name| {
                                    self.actors.get(name).and_then(|entry| {
                                        if name != &actor_name
                                            && (entry.is_stopped
                                                || entry.restart_type == RestartType::Temporary)
                                        {
                                            None
                                        } else {
                                            Some((name.clone(), entry.restart_ticket))
                                        }
                                    })
                                })
                                .collect();

                            if term_receivers.is_empty() {
                                let res = self.execute_restart(targets);
                                let _ = respond_to.send(res);
                            } else {
                                let tx = scheduled_restart_tx.clone();
                                tokio::spawn(async move {
                                    for mut rx in term_receivers {
                                        let _ = rx.wait_for(|&done| done).await;
                                    }
                                    let _ = tx.send((targets, Some(respond_to))).await;
                                });
                            }
                        }

                        ServiceSupervisorCommand::StopChild { actor_name, respond_to } => {
                            let ticket = self.allocate_restart_ticket();
                            if let Some(entry) = self.actors.get_mut(&actor_name) {
                                self.child_specs
                                    .write()
                                    .await
                                    .retain(|s| s.name != actor_name);
                                if entry.is_stopped {
                                    let _ = respond_to.send(Ok(()));
                                } else {
                                    Self::trigger_child_shutdown(
                                        self.name.clone(),
                                        actor_name,
                                        entry,
                                        ticket,
                                        Some(respond_to),
                                    );
                                }
                            } else {
                                let _ = respond_to.send(Err(ServiceError::ActorNotFound(actor_name)));
                            }
                        }

                        ServiceSupervisorCommand::DeleteChild { actor_name, respond_to } => {
                            if let Some(mut entry) = self.actors.remove(&actor_name) {
                                self.actor_order.retain(|n| n != &actor_name);
                                self.child_specs
                                    .write()
                                    .await
                                    .retain(|s| s.name != actor_name);
                                for waiter in entry.pending_resolutions.drain(..) {
                                    let _ = waiter.send(Err(ServiceError::ActorNotFound(
                                        actor_name.clone(),
                                    )));
                                }
                                let is_running = !entry.is_stopped && !*entry.term_rx.borrow();
                                if is_running {
                                    let mut term_rx = Self::initiate_actor_termination(
                                        self.name.clone(),
                                        actor_name,
                                        &mut entry,
                                    );
                                    // Move entry into the spawned task so its Drop (abort_handle.abort())
                                    // only runs after graceful shutdown completes or times out!
                                    tokio::spawn(async move {
                                        let _ = term_rx.wait_for(|&done| done).await;
                                        drop(entry);
                                        let _ = respond_to.send(Ok(()));
                                    });
                                } else {
                                    let _ = respond_to.send(Ok(()));
                                }
                            } else {
                                let _ = respond_to.send(Err(ServiceError::ActorNotFound(actor_name)));
                            }
                        }

                        ServiceSupervisorCommand::Shutdown { respond_to } => {
                            receiver.close();
                            self.graceful_shutdown_all().await;
                            let _ = respond_to.send(());
                            return Ok(());
                        }

                        ServiceSupervisorCommand::SimulateCrash => {
                            tracing::warn!(
                                supervisor = %self.name,
                                instance = self.instance_id,
                                "💥 CRASH COMMAND: Executing intentional supervisor crash!"
                            );
                            return Err(format!(
                                "Intentional crash in ServiceSupervisor '{}' #{}",
                                self.name, self.instance_id
                            ));
                        }
                    }
                }

                // Child actor monitoring: detect exits, cancellations, and crashes
                Some(join_res) = self.join_set.join_next() => {
                    if let Ok((actor_name, inst_id, task_result)) = join_res {
                        // 1. Ignore stale exit events from older superseded actor instances
                        let (restart_type, is_stopping, is_restarting, is_failed) =
                            match self.actors.get(&actor_name) {
                                Some(entry) if inst_id < entry.instance_id => {
                                    tracing::debug!(
                                        supervisor = %self.name,
                                        child = %actor_name,
                                        stale_instance = inst_id,
                                        current_instance = entry.instance_id,
                                        "ℹ️ Ignoring stale termination event for older actor instance."
                                    );
                                    continue;
                                }
                                Some(entry) => (
                                    entry.restart_type,
                                    entry.is_stopping,
                                    entry.is_restarting,
                                    entry.is_failed,
                                ),
                                None => continue,
                            };

                        // 2. If explicitly stopped or already restarting/failed via supervisor strategy, do not trigger another restart
                        if is_stopping || is_restarting || is_failed {
                            tracing::debug!(
                                supervisor = %self.name,
                                child = %actor_name,
                                instance = inst_id,
                                "ℹ️ Child actor stopped cleanly following supervisor stop/restart sequence."
                            );
                            continue;
                        }

                        // 3. Evaluate termination outcome against configured RestartType
                        let crash_reason: Option<String> = match (restart_type, task_result) {
                            (RestartType::Temporary, Ok(Ok(()))) => {
                                tracing::info!(
                                    supervisor = %self.name,
                                    child = %actor_name,
                                    instance = inst_id,
                                    "Temporary child actor completed cleanly (not restarting)."
                                );
                                self.child_specs
                                    .write()
                                    .await
                                    .retain(|s| s.name != actor_name);
                                if self.actors.get(&actor_name).is_some_and(|e| e.instance_id == inst_id)
                                    && let Some(mut entry) = self.actors.remove(&actor_name)
                                {
                                    self.actor_order.retain(|n| n != &actor_name);
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(ServiceError::ActorNotFound(actor_name.clone())));
                                    }
                                }
                                None
                            }
                            (RestartType::Temporary, Ok(Err(reason))) => {
                                tracing::warn!(
                                    supervisor = %self.name,
                                    child = %actor_name,
                                    instance = inst_id,
                                    reason = %reason,
                                    "Temporary child actor exited with error (not restarting per RestartType::Temporary)."
                                );
                                self.child_specs
                                    .write()
                                    .await
                                    .retain(|s| s.name != actor_name);
                                if self.actors.get(&actor_name).is_some_and(|e| e.instance_id == inst_id)
                                    && let Some(mut entry) = self.actors.remove(&actor_name)
                                {
                                    self.actor_order.retain(|n| n != &actor_name);
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(ServiceError::ActorNotFound(actor_name.clone())));
                                    }
                                }
                                None
                            }
                            (RestartType::Temporary, Err(panic_err)) => {
                                if !panic_err.is_cancelled() {
                                    tracing::warn!(
                                        supervisor = %self.name,
                                        child = %actor_name,
                                        instance = inst_id,
                                        error = ?panic_err,
                                        "Temporary child actor panicked (not restarting per RestartType::Temporary)."
                                    );
                                }
                                self.child_specs
                                    .write()
                                    .await
                                    .retain(|s| s.name != actor_name);
                                if self.actors.get(&actor_name).is_some_and(|e| e.instance_id == inst_id)
                                    && let Some(mut entry) = self.actors.remove(&actor_name)
                                {
                                    self.actor_order.retain(|n| n != &actor_name);
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(ServiceError::ActorNotFound(actor_name.clone())));
                                    }
                                }
                                None
                            }

                            (RestartType::Transient, Ok(Ok(()))) => {
                                tracing::info!(
                                    supervisor = %self.name,
                                    child = %actor_name,
                                    instance = inst_id,
                                    "Child actor terminated cleanly (Transient; not restarting)."
                                );
                                self.child_specs
                                    .write()
                                    .await
                                    .retain(|s| s.name != actor_name);
                                if let Some(entry) = self.actors.get_mut(&actor_name)
                                    && entry.instance_id == inst_id
                                {
                                    entry.is_stopped = true;
                                    entry.sender = Arc::new(());
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(ServiceError::ActorNotFound(actor_name.clone())));
                                    }
                                }
                                None
                            }

                            (RestartType::Permanent, Ok(Ok(()))) => {
                                tracing::warn!(
                                    supervisor = %self.name,
                                    child = %actor_name,
                                    instance = inst_id,
                                    "Permanent child actor exited normally with Ok(()); restarting per RestartType::Permanent."
                                );
                                Some(format!(
                                    "Permanent actor '{}' #{} exited normally",
                                    actor_name, inst_id
                                ))
                            }

                            (RestartType::Transient | RestartType::Permanent, Ok(Err(reason))) => {
                                Some(reason)
                            }
                            (RestartType::Transient | RestartType::Permanent, Err(panic_err)) => {
                                // If the task was cancelled by the supervisor as part of OneForAll or RestForOne,
                                // simply log and do not trigger cascading restarts!
                                if panic_err.is_cancelled() {
                                    tracing::debug!(
                                        supervisor = %self.name,
                                        child = %actor_name,
                                        instance = inst_id,
                                        "ℹ️ Child actor cancelled by supervisor restart strategy."
                                    );
                                    continue;
                                }
                                Some(format!("Task panicked: {:?}", panic_err))
                            }
                        };

                        let Some(crash_reason) = crash_reason else {
                            continue;
                        };

                        tracing::error!(
                            supervisor = %self.name,
                            child = %actor_name,
                            instance = inst_id,
                            reason = %crash_reason,
                            "🚨 ALARM: Child actor terminated and requires supervision recovery!"
                        );

                        // Determine which actors to restart based on strategy
                        let actors_to_restart: Vec<String> = match self.strategy {
                            SupervisionStrategy::OneForOne => vec![actor_name.clone()],
                            SupervisionStrategy::OneForAll => self.actor_order.clone(),
                            SupervisionStrategy::RestForOne => {
                                if let Some(pos) = self.actor_order.iter().position(|n| n == &actor_name) {
                                    self.actor_order[pos..].to_vec()
                                } else {
                                    vec![actor_name.clone()]
                                }
                            }
                        };

                        tracing::info!(
                            supervisor = %self.name,
                            strategy = ?self.strategy,
                            targets = ?actors_to_restart,
                            "📋 Initiating restart sequence for target group."
                        );

                        // Check crash limit and compute backoff on the actor that triggered the crash
                        let maybe_backoff = if let Some(actor_entry) = self.actors.get_mut(&actor_name) {
                            actor_entry.restart_tracker.record_crash()
                        } else {
                            Some(Duration::ZERO)
                        };

                        match maybe_backoff {
                            Some(backoff) => {
                                // Mark all target actors as restarting with a fresh restart ticket
                                // and gracefully terminate siblings in reverse order.
                                let mut term_receivers = Vec::new();
                                let mut temp_to_remove = Vec::new();
                                for target_name in actors_to_restart.iter().rev() {
                                    let ticket = self.allocate_restart_ticket();
                                    if let Some(entry) = self.actors.get_mut(target_name) {
                                        if target_name != &actor_name && entry.is_stopped {
                                            continue;
                                        }
                                        let is_running =
                                            target_name != &actor_name && !*entry.term_rx.borrow();
                                        if entry.restart_type == RestartType::Temporary {
                                            temp_to_remove.push((target_name.clone(), is_running));
                                        } else {
                                            entry.is_restarting = true;
                                            entry.restart_ticket = ticket;
                                            if is_running {
                                                let rx = Self::initiate_actor_termination(
                                                    self.name.clone(),
                                                    target_name.clone(),
                                                    entry,
                                                );
                                                term_receivers.push(rx);
                                            }
                                        }
                                    }
                                }

                                for (temp_name, is_running) in temp_to_remove {
                                    self.actor_order.retain(|n| n != &temp_name);
                                    self.child_specs
                                        .write()
                                        .await
                                        .retain(|s| s.name != temp_name);
                                    if let Some(mut entry) = self.actors.remove(&temp_name) {
                                        for waiter in entry.pending_resolutions.drain(..) {
                                            let _ = waiter.send(Err(ServiceError::ActorNotFound(
                                                temp_name.clone(),
                                            )));
                                        }
                                        if is_running {
                                            let mut rx = Self::initiate_actor_termination(
                                                self.name.clone(),
                                                temp_name,
                                                &mut entry,
                                            );
                                            tokio::spawn(async move {
                                                let _ = rx.wait_for(|&done| done).await;
                                                drop(entry);
                                            });
                                        }
                                    }
                                }

                                let targets: Vec<(String, u64)> = actors_to_restart
                                    .iter()
                                    .filter_map(|name| {
                                        self.actors.get(name).and_then(|entry| {
                                            if (name != &actor_name && entry.is_stopped)
                                                || entry.restart_type == RestartType::Temporary
                                            {
                                                None
                                            } else {
                                                Some((name.clone(), entry.restart_ticket))
                                            }
                                        })
                                    })
                                    .collect();

                                if backoff.is_zero() && term_receivers.is_empty() {
                                    let _ = self.execute_restart(targets);
                                } else {
                                    tracing::info!(
                                        supervisor = %self.name,
                                        targets = ?actors_to_restart,
                                        backoff = ?backoff,
                                        "⏳ Backoff delay / sibling shutdown: Scheduling non-blocking restart..."
                                    );
                                    let tx = scheduled_restart_tx.clone();
                                    tokio::spawn(async move {
                                        for mut rx in term_receivers {
                                            let _ = rx.wait_for(|&done| done).await;
                                        }
                                        if !backoff.is_zero() {
                                            tokio::time::sleep(backoff).await;
                                        }
                                        let _ = tx.send((targets, None)).await;
                                    });
                                }
                            }
                            None => {
                                if self.escalate_on_failure {
                                    tracing::error!(
                                        supervisor = %self.name,
                                        child = %actor_name,
                                        "🛑 CRASH-LOOP ESCALATION: Child exceeded max restarts. Escalating to RootSupervisor!"
                                    );
                                    return Err(format!(
                                        "ServiceSupervisor '{}' reached max restart threshold for child '{}'; escalating to RootSupervisor!",
                                        self.name, actor_name
                                    ));
                                }

                                tracing::error!(
                                    supervisor = %self.name,
                                    child = %actor_name,
                                    targets = ?actors_to_restart,
                                    "🛑 CRASH-LOOP DETECTED: Actor exceeded max restarts! Marking restart group as permanently FAILED."
                                );

                                for target_name in actors_to_restart.iter().rev() {
                                    let ticket = self.allocate_restart_ticket();
                                    if let Some(entry) = self.actors.get_mut(target_name) {
                                        let is_running =
                                            target_name != &actor_name && !*entry.term_rx.borrow();
                                        entry.is_failed = true;
                                        entry.is_restarting = false;
                                        entry.restart_ticket = ticket;
                                        let err = ServiceError::MaxRestartsExceeded(
                                            target_name.clone(),
                                            entry.restart_tracker.policy.max_restarts,
                                            entry.restart_tracker.policy.time_window,
                                        );
                                        for waiter in entry.pending_resolutions.drain(..) {
                                            let _ = waiter.send(Err(err.clone()));
                                        }
                                        if is_running {
                                            let _ = Self::initiate_actor_termination(
                                                self.name.clone(),
                                                target_name.clone(),
                                                entry,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Spawns fresh actor instances for the requested target names if their restart ticket matches.
    fn execute_restart(&mut self, targets: Vec<(String, u64)>) -> Result<(), ServiceError> {
        let mut result = Ok(());
        for (target_name, expected_ticket) in targets {
            if let Some(entry) = self.actors.get_mut(&target_name)
                && !entry.is_stopping
                && (!entry.is_failed
                    || matches!(
                        self.strategy,
                        SupervisionStrategy::OneForAll | SupervisionStrategy::RestForOne
                    ))
                && entry.restart_ticket == expected_ticket
            {
                if entry.is_failed {
                    entry.restart_tracker = RestartTracker::new(entry.restart_tracker.policy);
                }
                entry.is_failed = false;
                entry.is_stopped = false;
                entry.abort_handle.abort();
                entry.instance_id += 1;
                let new_inst = entry.instance_id;
                entry.is_restarting = false;

                tracing::info!(
                    supervisor = %self.name,
                    strategy = ?self.strategy,
                    child = %target_name,
                    instance = new_inst,
                    "🔄 Spawning replacement actor..."
                );

                let spawn_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    (entry.factory)(new_inst)
                }));
                let (new_sender, task_handle) = match spawn_res {
                    Ok(pair) => pair,
                    Err(panic_err) => {
                        tracing::error!(
                            supervisor = %self.name,
                            child = %target_name,
                            panic = ?panic_err,
                            "💥 Child actor factory panicked during restart!"
                        );
                        entry.is_failed = true;
                        result = Err(ServiceError::ServiceUnavailable);
                        continue;
                    }
                };
                let (term_tx, term_rx) = watch::channel(false);
                let name_for_task = target_name.clone();
                entry.abort_handle = task_handle.abort_handle();
                entry.term_rx = term_rx;
                entry.sender = new_sender.clone();

                for waiter in entry.pending_resolutions.drain(..) {
                    let _ = waiter.send(Ok(new_sender.clone()));
                }

                self.join_set.spawn(async move {
                    let _guard = TermSignalGuard(term_tx);
                    let res = task_handle.await;
                    (name_for_task, new_inst, res)
                });
            }
        }
        result
    }
}

// ============================================================================
// Service Supervisor Public Client Handle
// ============================================================================

/// Public thread-safe handle for communicating with a [`ServiceSupervisor`].
#[derive(Clone)]
pub struct ServiceSupervisorHandle {
    name: String,
    sender: Arc<RwLock<Sender<ServiceSupervisorCommand>>>,
    root: Option<RootSupervisorHandle>,
}

impl ServiceSupervisorHandle {
    /// Creates a handle wrapping the supervisor's command sender.
    pub(crate) fn new(
        name: impl Into<String>,
        sender: Sender<ServiceSupervisorCommand>,
        root: Option<RootSupervisorHandle>,
    ) -> Self {
        Self {
            name: name.into(),
            sender: Arc::new(RwLock::new(sender)),
            root,
        }
    }

    /// Returns the name of the service supervisor.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Obtains an active sender to the supervisor. If the supervisor crashed and was
    /// restarted by the RootSupervisor, automatically re-resolves the new channel!
    pub(crate) async fn get_sender(
        &self,
    ) -> Result<Sender<ServiceSupervisorCommand>, ServiceError> {
        {
            let guard = self.sender.read().await;
            if !guard.is_closed() {
                return Ok(guard.clone());
            }
        }

        if let Some(root) = &self.root {
            match root.resolve_service_channel(&self.name).await {
                Ok(new_sender) if !new_sender.is_closed() => {
                    let mut guard = self.sender.write().await;
                    *guard = new_sender.clone();
                    return Ok(new_sender);
                }
                _ => return Err(ServiceError::ServiceUnavailable),
            }
        }

        let guard = self.sender.read().await;
        if guard.is_closed() {
            Err(ServiceError::ServiceUnavailable)
        } else {
            Ok(guard.clone())
        }
    }

    /// Starts and supervises a new child actor from a [`ChildSpec`].
    pub async fn start_child(&self, spec: ChildSpec) -> Result<(), ServiceError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(ServiceSupervisorCommand::StartChild {
                spec,
                respond_to: tx,
            })
            .await
            .map_err(|_| ServiceError::ServiceUnavailable)?;

        rx.await.map_err(|_| ServiceError::ServiceUnavailable)?
    }

    /// Manually restarts or recovers a child actor, resetting its failed status and backoff counters.
    /// Under [`SupervisionStrategy::OneForAll`], restarts all sibling actors to restore consistent state.
    pub async fn restart_child(&self, actor_name: impl Into<String>) -> Result<(), ServiceError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(ServiceSupervisorCommand::RestartChild {
                actor_name: actor_name.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| ServiceError::ServiceUnavailable)?;

        rx.await.map_err(|_| ServiceError::ServiceUnavailable)?
    }

    /// Gracefully stops a specific supervised child actor according to its [`ShutdownPolicy`].
    /// Even if the child has [`RestartType::Permanent`], an explicit stop will not trigger a restart.
    pub async fn stop_child(&self, actor_name: impl Into<String>) -> Result<(), ServiceError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(ServiceSupervisorCommand::StopChild {
                actor_name: actor_name.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| ServiceError::ServiceUnavailable)?;

        rx.await.map_err(|_| ServiceError::ServiceUnavailable)?
    }

    /// Gracefully stops (if running) and completely removes a child actor's specification and state
    /// from the supervisor.
    pub async fn delete_child(&self, actor_name: impl Into<String>) -> Result<(), ServiceError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(ServiceSupervisorCommand::DeleteChild {
                actor_name: actor_name.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| ServiceError::ServiceUnavailable)?;

        rx.await.map_err(|_| ServiceError::ServiceUnavailable)?
    }

    /// Gracefully shuts down all supervised child actors and terminates this [`ServiceSupervisor`].
    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = {
            let guard = self.sender.read().await;
            if guard.is_closed() {
                return Ok(());
            }
            guard.clone()
        };
        if cmd_sender
            .send(ServiceSupervisorCommand::Shutdown { respond_to: tx })
            .await
            .is_err()
        {
            return Ok(());
        }

        let _ = rx.await;
        Ok(())
    }

    /// Retrieves a direct [`ResourceClient`] data-plane handle for a child resource actor.
    pub async fn get_client<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static>(
        &self,
        actor_name: impl Into<String>,
    ) -> Result<ResourceClient<T, E>, ServiceError> {
        let name = actor_name.into();
        let sender = self
            .resolve_actor_channel::<mpsc::Sender<ResourceCommand<T, E>>>(&name)
            .await?;
        let shared_sender = Arc::new(std::sync::RwLock::new(sender));
        Ok(ResourceClient::new(name, shared_sender, self.clone()))
    }

    /// Retrieves a direct [`ActorClient`] data-plane handle for a supervised [`StatefulActor`].
    pub async fn get_actor_client<A: StatefulActor>(
        &self,
        actor_name: impl Into<String>,
    ) -> Result<ActorClient<A>, ServiceError> {
        let name = actor_name.into();
        let sender = self
            .resolve_actor_channel::<mpsc::Sender<ActorEnvelope<A>>>(&name)
            .await?;
        let shared_sender = Arc::new(std::sync::RwLock::new(sender));
        Ok(ActorClient::new(name, shared_sender, self.clone()))
    }

    /// Queries the supervisor's registry to retrieve the untyped channel sender for an actor.
    pub async fn resolve_actor_channel_untyped(
        &self,
        actor_name: &str,
    ) -> Result<UntypedSender, ServiceError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(ServiceSupervisorCommand::ResolveActorChannel {
                actor_name: actor_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| ServiceError::ServiceUnavailable)?;

        rx.await.map_err(|_| ServiceError::ServiceUnavailable)?
    }

    /// Queries the supervisor's registry and downcasts the channel to the expected typed sender.
    pub async fn resolve_actor_channel<S: Clone + Send + Sync + 'static>(
        &self,
        actor_name: &str,
    ) -> Result<S, ServiceError> {
        let untyped = self.resolve_actor_channel_untyped(actor_name).await?;
        if let Some(typed) = untyped.downcast_ref::<S>() {
            Ok(typed.clone())
        } else {
            Err(ServiceError::ActorTypeMismatch(actor_name.to_string()))
        }
    }

    /// Injects an intentional crash into the service supervisor itself.
    pub async fn trigger_service_crash(&self) -> Result<(), ServiceError> {
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(ServiceSupervisorCommand::SimulateCrash)
            .await
            .map_err(|_| ServiceError::ServiceUnavailable)
    }
}

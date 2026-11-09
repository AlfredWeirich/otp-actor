//! # Unified Recursive OTP Supervisor (`Supervisor`)
//!
//! A single, composable, arbitrarily nestable supervisor actor modeled after Erlang/OTP's `supervisor` behavior.
//!
//! Every [`Supervisor`] can supervise leaf actors ([`super::stateful_actor::StatefulActor`],
//! [`super::generic_actor::ResourceActor`]) as well as child [`Supervisor`]s to arbitrary depth $N$.
//! Every level of the hierarchy supports:
//! - [`SupervisionStrategy::OneForOne`], [`SupervisionStrategy::OneForAll`], and [`SupervisionStrategy::RestForOne`]
//! - [`RestartType::Transient`], [`RestartType::Permanent`], and [`RestartType::Temporary`]
//! - [`ShutdownPolicy::Timeout`], [`ShutdownPolicy::BrutalKill`], and [`ShutdownPolicy::Infinity`]
//! - Rate-limited exponential backoff ([`RestartPolicy`], [`RestartTracker`]) and optional failure escalation (`escalate_on_failure`)
//! - Lock-free, zero-polling data-plane rebinding via per-child [`tokio::sync::watch`] channels ([`SlotState`]).

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{
    mpsc::{self, Receiver, Sender, WeakSender},
    oneshot, watch,
};
use tokio::task::{JoinError, JoinHandle, JoinSet};

use super::generic_actor::{ResourceClient, ResourceEndpoint};
use super::model::{
    RestartPolicy, RestartTracker, RestartType, ShutdownPolicy, SupervisionStrategy,
    SupervisorError, TaskResult,
};
use super::stateful_actor::{ActorClient, ActorEnvelope, StatefulActor};

/// Type-erased endpoint handle stored by the supervisor for dynamic client resolution.
pub type UntypedSender = Arc<dyn Any + Send + Sync>;

/// Context provided by a [`Supervisor`] to a [`ChildSpec`] factory upon initial spawn and every restart.
#[derive(Clone)]
pub struct SpawnContext {
    /// Monotonically increasing instance ID (starts at `1` and increments on each supervisor restart).
    pub instance_id: usize,
    /// Watch receiver that transitions to `true` when the supervisor initiates a graceful shutdown of this child.
    pub shutdown_rx: watch::Receiver<bool>,
    /// Weak-backed handle to the parent [`Supervisor`] managing this child (prevents reference cycles).
    pub supervisor: SupervisorHandle,
}

/// Type-erased factory closure invoked by the [`Supervisor`] to spawn or restart a supervised child.
pub type UntypedActorFactory =
    Arc<dyn Fn(SpawnContext) -> (UntypedSender, JoinHandle<TaskResult>) + Send + Sync + 'static>;

/// Optional callback invoked by the supervisor to signal a graceful shutdown or crash to a child.
pub type ShutdownSignalFn = Arc<dyn Fn(&UntypedSender) + Send + Sync + 'static>;

/// Reactive lifecycle state published by a [`Supervisor`] for each supervised child slot.
///
/// Data-plane clients ([`ActorClient`], [`ResourceClient`], and child [`SupervisorHandle`]s)
/// subscribe to a [`watch::Receiver<SlotState>`] to rebind in 0ms without polling loops.
#[derive(Clone)]
pub enum SlotState {
    /// The child is actively running with the given `instance_id` and type-erased endpoint.
    Running {
        instance_id: usize,
        endpoint: UntypedSender,
    },
    /// The child is currently shutting down or waiting in exponential backoff before restarting.
    Restarting { instance_id: usize },
    /// The child was explicitly stopped, deleted, or completed its lifecycle without restart.
    Stopped,
    /// The child exceeded its [`RestartPolicy`] limits and is permanently failed.
    Failed(SupervisorError),
}

/// Specification defining a child (leaf actor or child [`Supervisor`]) to be supervised by a [`Supervisor`].
#[derive(Clone)]
pub struct ChildSpec {
    pub name: String,
    pub factory: UntypedActorFactory,
    pub restart_policy: RestartPolicy,
    pub restart_type: RestartType,
    pub shutdown_policy: ShutdownPolicy,
    pub shutdown_signal: Option<ShutdownSignalFn>,
    #[cfg(any(test, feature = "chaos_test"))]
    pub(crate) crash_signal: Option<ShutdownSignalFn>,
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
            #[cfg(any(test, feature = "chaos_test"))]
            crash_signal: None,
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

    /// Attaches an intentional crash signal callback for chaos testing.
    #[cfg(any(test, feature = "chaos_test"))]
    #[must_use]
    pub(crate) fn with_crash_signal(mut self, signal: ShutdownSignalFn) -> Self {
        self.crash_signal = Some(signal);
        self
    }
}

/// Fluent builder for configuring and spawning a [`Supervisor`] (either as a root supervisor or as a child of another [`Supervisor`]).
pub struct SupervisorBuilder {
    name: String,
    strategy: SupervisionStrategy,
    child_specs: Vec<ChildSpec>,
    restart_policy: RestartPolicy,
    restart_type: RestartType,
    shutdown_policy: ShutdownPolicy,
    escalate_on_failure: bool,
}

impl SupervisorBuilder {
    /// Creates a new [`SupervisorBuilder`] with the given supervisor name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            strategy: SupervisionStrategy::default(),
            child_specs: Vec::new(),
            restart_policy: RestartPolicy::default(),
            restart_type: RestartType::Transient,
            // Per Erlang/OTP convention, a supervisor child defaults to Infinity so it can
            // sequentially run each of its own children's bounded ShutdownPolicies without
            // being prematurely aborted by its parent.
            shutdown_policy: ShutdownPolicy::Infinity,
            escalate_on_failure: false,
        }
    }

    /// Sets the [`SupervisionStrategy`] (`OneForOne`, `OneForAll`, or `RestForOne`).
    #[must_use]
    pub fn strategy(mut self, strategy: SupervisionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Sets the [`RestartPolicy`] governing how a parent supervisor rate-limits restarts of this supervisor.
    #[must_use]
    pub fn restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.restart_policy = policy;
        self
    }

    /// Sets the [`RestartType`] (`Transient`, `Permanent`, or `Temporary`) when supervised by a parent supervisor.
    #[must_use]
    pub fn restart_type(mut self, restart_type: RestartType) -> Self {
        self.restart_type = restart_type;
        self
    }

    /// Sets the [`ShutdownPolicy`] governing how a parent supervisor terminates this supervisor.
    #[must_use]
    pub fn shutdown_policy(mut self, policy: ShutdownPolicy) -> Self {
        self.shutdown_policy = policy;
        self
    }

    /// Convenience method setting [`ShutdownPolicy::Timeout`] on this supervisor.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_policy = ShutdownPolicy::Timeout(timeout);
        self
    }

    /// Configures whether child crash-loops (`MaxRestartsExceeded`) should escalate to this supervisor's parent.
    ///
    /// When enabled (`true`), exceeding `max_restarts` causes this [`Supervisor`] to terminate with `Err`,
    /// triggering a subtree restart by its parent [`Supervisor`].
    /// Default: `false` (marks the offending child/restart group as permanently failed).
    #[must_use]
    pub fn escalate_on_failure(mut self, escalate: bool) -> Self {
        self.escalate_on_failure = escalate;
        self
    }

    /// Adds a child specification (leaf actor or nested [`SupervisorBuilder`]) to be booted in registration order.
    #[must_use]
    pub fn add_child(mut self, spec: impl Into<ChildSpec>) -> Self {
        let child = spec.into();
        if self.child_specs.iter().any(|s| s.name == child.name) {
            tracing::warn!(
                supervisor = %self.name,
                child = %child.name,
                "⚠️ Duplicate child name in SupervisorBuilder::add_child ignored."
            );
        } else {
            self.child_specs.push(child);
        }
        self
    }

    /// Converts this [`SupervisorBuilder`] into a [`ChildSpec`] that can be supervised by any parent [`Supervisor`].
    pub fn build_child_spec(self) -> ChildSpec {
        let name = self.name;
        let strategy = self.strategy;
        let shared_specs = Arc::new(RwLock::new(self.child_specs));
        let restart_policy = self.restart_policy;
        let restart_type = self.restart_type;
        let shutdown_policy = self.shutdown_policy;
        let escalate_on_failure = self.escalate_on_failure;

        let name_for_factory = name.clone();
        let specs_for_factory = shared_specs.clone();

        let factory: UntypedActorFactory = Arc::new(move |ctx: SpawnContext| {
            let (tx, rx) = mpsc::channel(256);
            let sender_arc = Arc::new(tx.clone());
            let self_handle = SupervisorHandle::new_weak(
                name_for_factory.clone(),
                tx.downgrade(),
                Some(ctx.supervisor),
            );
            let supervisor = Supervisor::new_internal(
                name_for_factory.clone(),
                ctx.instance_id,
                strategy,
                specs_for_factory.clone(),
                escalate_on_failure,
                ctx.shutdown_rx,
                self_handle,
            );
            let task_handle = tokio::spawn(supervisor.run(rx));
            (sender_arc, task_handle)
        });

        let shutdown_signal: ShutdownSignalFn = Arc::new(|untyped| {
            if let Some(cmd_tx) = untyped.downcast_ref::<Sender<SupervisorCommand>>() {
                let (ack_tx, _ack_rx) = oneshot::channel();
                let _ = cmd_tx.try_send(SupervisorCommand::Shutdown { respond_to: ack_tx });
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
                if let Some(cmd_tx) = untyped.downcast_ref::<Sender<SupervisorCommand>>() {
                    let _ = cmd_tx.try_send(SupervisorCommand::SimulateCrash);
                }
            });
            spec = spec.with_crash_signal(crash_signal);
        }

        spec
    }

    /// Spawns this [`Supervisor`] as a top-level root supervisor on the current Tokio runtime.
    ///
    /// # Panics
    /// Panics if called outside the context of an active Tokio runtime.
    pub fn spawn_root(self) -> SupervisorHandle {
        let name = self.name;
        let strategy = self.strategy;
        let shared_specs = Arc::new(RwLock::new(self.child_specs));
        let escalate_on_failure = self.escalate_on_failure;

        let (tx, rx) = mpsc::channel(256);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let self_handle = SupervisorHandle::new_weak(name.clone(), tx.downgrade(), None);
        let supervisor = Supervisor::new_internal(
            name.clone(),
            1,
            strategy,
            shared_specs,
            escalate_on_failure,
            shutdown_rx,
            self_handle,
        );
        tokio::spawn(supervisor.run(rx));
        SupervisorHandle::new_root_handle(name, tx)
    }

    /// Spawns this [`Supervisor`] as a supervised child under `parent` and returns its [`SupervisorHandle`].
    pub async fn spawn(self, parent: &SupervisorHandle) -> Result<SupervisorHandle, SupervisorError> {
        let name = self.name.clone();
        let spec = self.build_child_spec();
        parent.start_child(spec).await?;
        parent.get_supervisor(name).await
    }
}

impl From<SupervisorBuilder> for ChildSpec {
    fn from(builder: SupervisorBuilder) -> Self {
        builder.build_child_spec()
    }
}

/// Internal commands processed by a [`Supervisor`].
pub(crate) enum SupervisorCommand {
    /// Starts and supervises a child from a [`ChildSpec`].
    StartChild {
        spec: ChildSpec,
        respond_to: oneshot::Sender<Result<(), SupervisorError>>,
    },
    /// Resolves the current active untyped endpoint for a registered child.
    ResolveChildChannel {
        child_name: String,
        respond_to: oneshot::Sender<Result<UntypedSender, SupervisorError>>,
    },
    /// Subscribes to the reactive [`SlotState`] watch channel for a registered child.
    ResolveChildSlot {
        child_name: String,
        respond_to: oneshot::Sender<Result<watch::Receiver<SlotState>, SupervisorError>>,
    },
    /// Manually restarts or recovers a child (and any strategy-coupled siblings), resetting backoff limits.
    RestartChild {
        child_name: String,
        respond_to: oneshot::Sender<Result<(), SupervisorError>>,
    },
    /// Gracefully stops a specific supervised child according to its [`ShutdownPolicy`].
    StopChild {
        child_name: String,
        respond_to: oneshot::Sender<Result<(), SupervisorError>>,
    },
    /// Gracefully stops (if running) and completely removes a child's specification and state.
    DeleteChild {
        child_name: String,
        respond_to: oneshot::Sender<Result<(), SupervisorError>>,
    },
    /// Gracefully shuts down all supervised children in reverse registration order and exits.
    Shutdown { respond_to: oneshot::Sender<()> },
    /// Triggers an intentional crash in a specific supervised child for chaos testing.
    #[cfg(any(test, feature = "chaos_test"))]
    TriggerChildCrash {
        child_name: String,
        respond_to: oneshot::Sender<Result<(), SupervisorError>>,
    },
    /// Simulates an intentional crash in this supervisor itself.
    #[cfg(any(test, feature = "chaos_test"))]
    SimulateCrash,
}

/// Metadata tracked for each actively supervised child (leaf actor or child [`Supervisor`]).
struct ActiveChild {
    sender: UntypedSender,
    slot_tx: watch::Sender<SlotState>,
    shutdown_tx: watch::Sender<bool>,
    abort_handle: tokio::task::AbortHandle,
    term_rx: watch::Receiver<bool>,
    factory: UntypedActorFactory,
    restart_type: RestartType,
    shutdown_policy: ShutdownPolicy,
    shutdown_signal: Option<ShutdownSignalFn>,
    #[cfg(any(test, feature = "chaos_test"))]
    crash_signal: Option<ShutdownSignalFn>,
    instance_id: usize,
    restart_ticket: u64,
    restart_tracker: RestartTracker,
    is_failed: bool,
    is_stopped: bool,
    is_stopping: bool,
    is_restarting: bool,
    pending_resolutions: Vec<oneshot::Sender<Result<UntypedSender, SupervisorError>>>,
}

impl Drop for ActiveChild {
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

/// Unified, recursive OTP Supervisor actor capable of supervising leaf actors and nested [`Supervisor`]s.
pub struct Supervisor {
    name: String,
    instance_id: usize,
    strategy: SupervisionStrategy,
    child_specs: Arc<RwLock<Vec<ChildSpec>>>,
    escalate_on_failure: bool,
    shutdown_rx: watch::Receiver<bool>,
    self_handle: SupervisorHandle,
    children: HashMap<String, ActiveChild>,
    child_order: Vec<String>,
    next_restart_ticket: u64,
    join_set: JoinSet<(String, usize, Result<TaskResult, JoinError>)>,
}

impl Supervisor {
    /// Spawns a new top-level [`Supervisor`] with the given `name` and default [`SupervisionStrategy::OneForOne`] onto the Tokio runtime.
    ///
    /// # Panics
    /// Panics if called outside the context of an active Tokio runtime.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(name: impl Into<String>) -> SupervisorHandle {
        SupervisorBuilder::new(name).spawn_root()
    }

    /// Spawns a new top-level [`Supervisor`] with the given `name` and [`SupervisionStrategy`] onto the Tokio runtime.
    ///
    /// # Panics
    /// Panics if called outside the context of an active Tokio runtime.
    pub fn with_strategy(name: impl Into<String>, strategy: SupervisionStrategy) -> SupervisorHandle {
        SupervisorBuilder::new(name).strategy(strategy).spawn_root()
    }

    /// Spawns a new top-level root [`Supervisor`] named `"root"` with default [`SupervisionStrategy::OneForOne`] onto the Tokio runtime.
    ///
    /// # Panics
    /// Panics if called outside the context of an active Tokio runtime.
    pub fn new_root() -> SupervisorHandle {
        SupervisorBuilder::new("root").spawn_root()
    }

    /// Returns a fluent [`SupervisorBuilder`] for configuring a root or child [`Supervisor`].
    pub fn builder(name: impl Into<String>) -> SupervisorBuilder {
        SupervisorBuilder::new(name)
    }

    /// Spawns a child [`Supervisor`] with default [`SupervisionStrategy::OneForOne`] under `parent`.
    pub async fn spawn_child(
        name: impl Into<String>,
        parent: &SupervisorHandle,
    ) -> Result<SupervisorHandle, SupervisorError> {
        SupervisorBuilder::new(name).spawn(parent).await
    }

    /// Spawns a child [`Supervisor`] with the given [`SupervisionStrategy`] under `parent`.
    pub async fn spawn_child_with_strategy(
        name: impl Into<String>,
        strategy: SupervisionStrategy,
        parent: &SupervisorHandle,
    ) -> Result<SupervisorHandle, SupervisorError> {
        SupervisorBuilder::new(name)
            .strategy(strategy)
            .spawn(parent)
            .await
    }

    pub(crate) fn new_internal(
        name: String,
        instance_id: usize,
        strategy: SupervisionStrategy,
        child_specs: Arc<RwLock<Vec<ChildSpec>>>,
        escalate_on_failure: bool,
        shutdown_rx: watch::Receiver<bool>,
        self_handle: SupervisorHandle,
    ) -> Self {
        Self {
            name,
            instance_id,
            strategy,
            child_specs,
            escalate_on_failure,
            shutdown_rx,
            self_handle,
            children: HashMap::new(),
            child_order: Vec::new(),
            next_restart_ticket: 0,
            join_set: JoinSet::new(),
        }
    }

    /// Allocates a monotonically increasing ticket to uniquely identify a child spawn or restart wave.
    fn allocate_restart_ticket(&mut self) -> u64 {
        self.next_restart_ticket = self.next_restart_ticket.wrapping_add(1);
        self.next_restart_ticket
    }

    /// Spawns a child from a [`ChildSpec`], tracking it in `join_set` and `children`.
    fn spawn_child_from_spec(&mut self, spec: ChildSpec) -> Result<(), SupervisorError> {
        let name = spec.name;
        if let Some(existing) = self.children.get(&name)
            && (!existing.is_stopped || !*existing.term_rx.borrow())
        {
            tracing::warn!(
                supervisor = %self.name,
                instance = self.instance_id,
                child = %name,
                "Child is already running; skipping duplicate spawn."
            );
            return Ok(());
        }

        let inst_id = self
            .children
            .get(&name)
            .map_or(1, |entry| entry.instance_id + 1);

        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            child = %name,
            child_instance = inst_id,
            restart_type = ?spec.restart_type,
            shutdown_policy = ?spec.shutdown_policy,
            "🚀 Spawning child from ChildSpec..."
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let spawn_ctx = SpawnContext {
            instance_id: inst_id,
            shutdown_rx,
            supervisor: self.self_handle.clone(),
        };

        let spawn_res =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (spec.factory)(spawn_ctx)));
        let (sender, task_handle) = match spawn_res {
            Ok(pair) => pair,
            Err(panic_err) => {
                tracing::error!(
                    supervisor = %self.name,
                    instance = self.instance_id,
                    child = %name,
                    panic = ?panic_err,
                    "💥 Child factory panicked during spawn!"
                );
                return Err(SupervisorError::SupervisorUnavailable);
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

        if !self.child_order.contains(&name) {
            self.child_order.push(name.clone());
        }

        let initial_state = SlotState::Running {
            instance_id: inst_id,
            endpoint: sender.clone(),
        };
        let slot_tx = if let Some(existing) = self.children.remove(&name) {
            existing.slot_tx.send_replace(initial_state);
            existing.slot_tx.clone()
        } else {
            let (tx, _rx) = watch::channel(initial_state);
            tx
        };

        self.children.insert(
            name,
            ActiveChild {
                sender,
                slot_tx,
                shutdown_tx,
                abort_handle,
                term_rx,
                factory: spec.factory,
                restart_type: spec.restart_type,
                shutdown_policy: spec.shutdown_policy,
                shutdown_signal: spec.shutdown_signal,
                #[cfg(any(test, feature = "chaos_test"))]
                crash_signal: spec.crash_signal,
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

    /// Signals graceful shutdown (or brutal kill) to a running child according to its
    /// [`ShutdownPolicy`] and returns a watch receiver that resolves once the task terminates.
    fn initiate_child_termination(
        supervisor_name: String,
        child_name: String,
        entry: &mut ActiveChild,
    ) -> watch::Receiver<bool> {
        let policy = entry.shutdown_policy;
        let abort_handle = entry.abort_handle.clone();
        let term_rx = entry.term_rx.clone();

        match policy {
            ShutdownPolicy::BrutalKill => {
                tracing::info!(
                    supervisor = %supervisor_name,
                    child = %child_name,
                    "⚡ BrutalKill policy: Immediately aborting child."
                );
                entry.sender = Arc::new(());
                abort_handle.abort();
            }
            ShutdownPolicy::Timeout(timeout) => {
                tracing::info!(
                    supervisor = %supervisor_name,
                    child = %child_name,
                    timeout = ?timeout,
                    "🛑 Signaling graceful shutdown to child with timeout..."
                );
                let _ = entry.shutdown_tx.send(true);
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
                            "⚠️ Child exceeded graceful shutdown timeout; forcefully aborting!"
                        );
                        abort_handle.abort();
                    }
                });
            }
            ShutdownPolicy::Infinity => {
                tracing::info!(
                    supervisor = %supervisor_name,
                    child = %child_name,
                    "🛑 Signaling graceful shutdown to child (Infinity policy)..."
                );
                let _ = entry.shutdown_tx.send(true);
                if let Some(sig) = &entry.shutdown_signal {
                    sig(&entry.sender);
                }
                entry.sender = Arc::new(());
            }
        }

        term_rx
    }

    /// Initiates a non-blocking shutdown of a single child according to its [`ShutdownPolicy`].
    fn trigger_child_shutdown(
        supervisor_name: String,
        child_name: String,
        entry: &mut ActiveChild,
        restart_ticket: u64,
        mark_slot_stopped: bool,
        respond_to: Option<oneshot::Sender<Result<(), SupervisorError>>>,
    ) {
        entry.is_stopping = true;
        entry.is_stopped = true;
        entry.is_restarting = false;
        entry.restart_ticket = restart_ticket;
        if mark_slot_stopped {
            entry.slot_tx.send_replace(SlotState::Stopped);
        } else {
            entry.slot_tx.send_replace(SlotState::Restarting {
                instance_id: entry.instance_id,
            });
        }
        for waiter in entry.pending_resolutions.drain(..) {
            let _ = waiter.send(Err(SupervisorError::ChildNotFound(child_name.clone())));
        }

        let mut term_rx = Self::initiate_child_termination(supervisor_name, child_name, entry);
        if let Some(tx) = respond_to {
            tokio::spawn(async move {
                let _ = term_rx.wait_for(|&done| done).await;
                let _ = tx.send(Ok(()));
            });
        }
    }

    /// Gracefully shuts down all supervised children sequentially in reverse registration order
    /// according to their configured [`ShutdownPolicy`].
    async fn graceful_shutdown_all(&mut self) {
        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            children = self.children.len(),
            "🛑 Supervisor initiating sequential graceful shutdown of all children..."
        );

        let sup_name = self.name.clone();
        // Shut down children sequentially in reverse registration order (OTP standard)
        let ordered_children: Vec<String> = self.child_order.iter().rev().cloned().collect();
        for child_name in ordered_children {
            let ticket = self.allocate_restart_ticket();
            let maybe_rx = if let Some(entry) = self.children.get_mut(&child_name)
                && !entry.is_stopped
            {
                let (tx, rx) = oneshot::channel();
                Self::trigger_child_shutdown(
                    sup_name.clone(),
                    child_name,
                    entry,
                    ticket,
                    false,
                    Some(tx),
                );
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
        self.children.clear();

        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            "✅ Supervisor graceful shutdown complete."
        );
    }

    /// Main event loop processing commands and monitoring child terminations.
    pub(crate) async fn run(mut self, mut receiver: Receiver<SupervisorCommand>) -> TaskResult {
        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            strategy = ?self.strategy,
            "🛡️ Supervisor online. Booting children..."
        );

        // Boot initial child specifications synchronously before accepting commands
        let initial_specs = self
            .child_specs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for spec in initial_specs {
            if self.spawn_child_from_spec(spec.clone()).is_err() {
                let ticket = self.allocate_restart_ticket();
                let (term_tx, term_rx) = watch::channel(true);
                drop(term_tx);
                let (shutdown_tx, _) = watch::channel(true);
                let (slot_tx, _) =
                    watch::channel(SlotState::Failed(SupervisorError::SupervisorUnavailable));
                let dummy_abort = tokio::spawn(async {}).abort_handle();
                if !self.child_order.contains(&spec.name) {
                    self.child_order.push(spec.name.clone());
                }
                self.children.insert(
                    spec.name,
                    ActiveChild {
                        sender: Arc::new(()),
                        slot_tx,
                        shutdown_tx,
                        abort_handle: dummy_abort,
                        term_rx,
                        factory: spec.factory,
                        restart_type: spec.restart_type,
                        shutdown_policy: spec.shutdown_policy,
                        shutdown_signal: spec.shutdown_signal,
                        #[cfg(any(test, feature = "chaos_test"))]
                        crash_signal: spec.crash_signal,
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

        let (scheduled_restart_tx, mut scheduled_restart_rx) = mpsc::channel::<(
            Vec<(String, u64)>,
            Option<oneshot::Sender<Result<(), SupervisorError>>>,
        )>(64);

        loop {
            if *self.shutdown_rx.borrow() {
                receiver.close();
                self.graceful_shutdown_all().await;
                return Ok(());
            }

            tokio::select! {
                Ok(()) = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        receiver.close();
                        self.graceful_shutdown_all().await;
                        return Ok(());
                    }
                }

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
                            "All supervisor handles dropped. Aborting supervised child tasks."
                        );
                        self.join_set.abort_all();
                        return Ok(());
                    };

                    match cmd {
                        SupervisorCommand::StartChild { spec, respond_to } => {
                            let already_running = self
                                .children
                                .get(&spec.name)
                                .is_some_and(|c| !c.is_stopped || !*c.term_rx.borrow());
                            if already_running {
                                let _ = respond_to.send(Err(SupervisorError::ChildAlreadyExists(spec.name)));
                            } else {
                                match self.spawn_child_from_spec(spec.clone()) {
                                    Ok(()) => {
                                        {
                                            let mut guard = self
                                                .child_specs
                                                .write()
                                                .unwrap_or_else(std::sync::PoisonError::into_inner);
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

                        SupervisorCommand::ResolveChildChannel { child_name, respond_to } => {
                            if let Some(child) = self.children.get_mut(&child_name) {
                                if child.is_failed {
                                    let _ = respond_to.send(Err(SupervisorError::MaxRestartsExceeded(
                                        child_name,
                                        child.restart_tracker.policy.max_restarts,
                                        child.restart_tracker.policy.time_window,
                                    )));
                                } else if child.is_stopped || child.is_stopping {
                                    let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                                } else if child.is_restarting || *child.term_rx.borrow() {
                                    child.pending_resolutions.push(respond_to);
                                } else {
                                    let _ = respond_to.send(Ok(child.sender.clone()));
                                }
                            } else {
                                let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                            }
                        }

                        SupervisorCommand::ResolveChildSlot { child_name, respond_to } => {
                            if let Some(child) = self.children.get(&child_name) {
                                if child.is_failed {
                                    let _ = respond_to.send(Err(SupervisorError::MaxRestartsExceeded(
                                        child_name,
                                        child.restart_tracker.policy.max_restarts,
                                        child.restart_tracker.policy.time_window,
                                    )));
                                } else if child.is_stopped || child.is_stopping {
                                    let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                                } else {
                                    let _ = respond_to.send(Ok(child.slot_tx.subscribe()));
                                }
                            } else {
                                let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                            }
                        }

                        SupervisorCommand::RestartChild { child_name, respond_to } => {
                            if !self.children.contains_key(&child_name) {
                                let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                                continue;
                            }

                            tracing::info!(
                                supervisor = %self.name,
                                child = %child_name,
                                strategy = ?self.strategy,
                                "🔄 Manual child restart triggered via Supervisor."
                            );

                            let children_to_restart: Vec<String> = match self.strategy {
                                SupervisionStrategy::OneForOne => vec![child_name.clone()],
                                SupervisionStrategy::OneForAll => self.child_order.clone(),
                                SupervisionStrategy::RestForOne => {
                                    if let Some(pos) =
                                        self.child_order.iter().position(|n| n == &child_name)
                                    {
                                        self.child_order[pos..].to_vec()
                                    } else {
                                        vec![child_name.clone()]
                                    }
                                }
                            };

                            let mut term_receivers = Vec::new();
                            let mut temp_to_remove = Vec::new();
                            for target_name in children_to_restart.iter().rev() {
                                let ticket = self.allocate_restart_ticket();
                                if let Some(entry) = self.children.get_mut(target_name) {
                                    if target_name != &child_name
                                        && (entry.is_stopped || entry.is_stopping)
                                    {
                                        continue;
                                    }
                                    let is_running = !*entry.term_rx.borrow();
                                    if target_name != &child_name
                                        && entry.restart_type == RestartType::Temporary
                                    {
                                        temp_to_remove.push((target_name.clone(), is_running));
                                        continue;
                                    }

                                    // Ensure manually restarted child is present in shared child_specs
                                    if target_name == &child_name {
                                        let mut guard = self
                                            .child_specs
                                            .write()
                                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                                        if !guard.iter().any(|s| s.name == *target_name) {
                                            guard.push(ChildSpec {
                                                name: target_name.clone(),
                                                factory: entry.factory.clone(),
                                                restart_policy: entry.restart_tracker.policy,
                                                restart_type: entry.restart_type,
                                                shutdown_policy: entry.shutdown_policy,
                                                shutdown_signal: entry.shutdown_signal.clone(),
                                                #[cfg(any(test, feature = "chaos_test"))]
                                                crash_signal: entry.crash_signal.clone(),
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
                                    entry.slot_tx.send_replace(SlotState::Restarting {
                                        instance_id: entry.instance_id,
                                    });

                                    if is_running {
                                        let rx = Self::initiate_child_termination(
                                            self.name.clone(),
                                            target_name.clone(),
                                            entry,
                                        );
                                        term_receivers.push(rx);
                                    }
                                }
                            }

                            for (temp_name, is_running) in temp_to_remove {
                                self.child_order.retain(|n| n != &temp_name);
                                self.child_specs
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .retain(|s| s.name != temp_name);
                                if let Some(mut entry) = self.children.remove(&temp_name) {
                                    entry.slot_tx.send_replace(SlotState::Stopped);
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(SupervisorError::ChildNotFound(
                                            temp_name.clone(),
                                        )));
                                    }
                                    if is_running {
                                        let mut rx = Self::initiate_child_termination(
                                            self.name.clone(),
                                            temp_name,
                                            &mut entry,
                                        );
                                        term_receivers.push(rx.clone());
                                        tokio::spawn(async move {
                                            let _ = rx.wait_for(|&done| done).await;
                                            drop(entry);
                                        });
                                    }
                                }
                            }

                            let targets: Vec<(String, u64)> = children_to_restart
                                .iter()
                                .filter_map(|name| {
                                    self.children.get(name).and_then(|entry| {
                                        if name != &child_name
                                            && (entry.is_stopped
                                                || entry.is_stopping
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

                        SupervisorCommand::StopChild { child_name, respond_to } => {
                            let ticket = self.allocate_restart_ticket();
                            if let Some(entry) = self.children.get_mut(&child_name) {
                                self.child_specs
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .retain(|s| s.name != child_name);
                                if entry.is_stopped && *entry.term_rx.borrow() {
                                    let _ = respond_to.send(Ok(()));
                                } else if entry.is_stopping {
                                    let mut term_rx = entry.term_rx.clone();
                                    tokio::spawn(async move {
                                        let _ = term_rx.wait_for(|&done| done).await;
                                        let _ = respond_to.send(Ok(()));
                                    });
                                } else {
                                    Self::trigger_child_shutdown(
                                        self.name.clone(),
                                        child_name,
                                        entry,
                                        ticket,
                                        true,
                                        Some(respond_to),
                                    );
                                }
                            } else {
                                let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                            }
                        }

                        SupervisorCommand::DeleteChild { child_name, respond_to } => {
                            if let Some(mut entry) = self.children.remove(&child_name) {
                                self.child_order.retain(|n| n != &child_name);
                                self.child_specs
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .retain(|s| s.name != child_name);
                                entry.slot_tx.send_replace(SlotState::Stopped);
                                for waiter in entry.pending_resolutions.drain(..) {
                                    let _ = waiter.send(Err(SupervisorError::ChildNotFound(
                                        child_name.clone(),
                                    )));
                                }
                                let is_running = !*entry.term_rx.borrow();
                                if is_running {
                                    let mut term_rx = Self::initiate_child_termination(
                                        self.name.clone(),
                                        child_name,
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
                                let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                            }
                        }

                        SupervisorCommand::Shutdown { respond_to } => {
                            receiver.close();
                            self.graceful_shutdown_all().await;
                            let _ = respond_to.send(());
                            return Ok(());
                        }

                        #[cfg(any(test, feature = "chaos_test"))]
                        SupervisorCommand::TriggerChildCrash { child_name, respond_to } => {
                            if let Some(child) = self.children.get(&child_name) {
                                if child.is_failed {
                                    let _ = respond_to.send(Err(SupervisorError::MaxRestartsExceeded(
                                        child_name,
                                        child.restart_tracker.policy.max_restarts,
                                        child.restart_tracker.policy.time_window,
                                    )));
                                } else if child.is_stopped || child.is_stopping {
                                    let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                                } else if let Some(crash_sig) = &child.crash_signal {
                                    crash_sig(&child.sender);
                                    let _ = respond_to.send(Ok(()));
                                } else {
                                    child.abort_handle.abort();
                                    let _ = respond_to.send(Ok(()));
                                }
                            } else {
                                let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                            }
                        }

                        #[cfg(any(test, feature = "chaos_test"))]
                        SupervisorCommand::SimulateCrash => {
                            tracing::warn!(
                                supervisor = %self.name,
                                instance = self.instance_id,
                                "💥 CRASH COMMAND: Executing intentional supervisor crash!"
                            );
                            return Err(format!(
                                "Intentional crash in Supervisor '{}' #{}",
                                self.name, self.instance_id
                            ));
                        }
                    }
                }

                // Child monitoring: detect exits, cancellations, and crashes
                Some(join_res) = self.join_set.join_next() => {
                    if let Ok((child_name, inst_id, task_result)) = join_res {
                        // 1. Ignore stale exit events from older superseded child instances
                        let (restart_type, is_stopping, is_restarting, is_failed) =
                            match self.children.get_mut(&child_name) {
                                Some(entry) if inst_id < entry.instance_id => {
                                    tracing::debug!(
                                        supervisor = %self.name,
                                        child = %child_name,
                                        stale_instance = inst_id,
                                        current_instance = entry.instance_id,
                                        "ℹ️ Ignoring stale termination event for older child instance."
                                    );
                                    continue;
                                }
                                Some(entry) => {
                                    let info = (
                                        entry.restart_type,
                                        entry.is_stopping,
                                        entry.is_restarting,
                                        entry.is_failed,
                                    );
                                    if entry.is_stopping {
                                        entry.is_stopping = false;
                                    }
                                    info
                                }
                                None => continue,
                            };

                        // 2. If explicitly stopped or already restarting/failed via supervisor strategy, do not trigger another restart
                        if is_stopping || is_restarting || is_failed {
                            tracing::debug!(
                                supervisor = %self.name,
                                child = %child_name,
                                instance = inst_id,
                                "ℹ️ Child stopped cleanly following supervisor stop/restart sequence."
                            );
                            continue;
                        }

                        // 3. Evaluate termination outcome against configured RestartType
                        let crash_reason: Option<String> = match (restart_type, task_result) {
                            (RestartType::Temporary, Ok(Ok(()))) => {
                                tracing::info!(
                                    supervisor = %self.name,
                                    child = %child_name,
                                    instance = inst_id,
                                    "Temporary child completed cleanly (not restarting)."
                                );
                                self.child_specs
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .retain(|s| s.name != child_name);
                                if self.children.get(&child_name).is_some_and(|e| e.instance_id == inst_id)
                                    && let Some(mut entry) = self.children.remove(&child_name)
                                {
                                    self.child_order.retain(|n| n != &child_name);
                                    entry.slot_tx.send_replace(SlotState::Stopped);
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(SupervisorError::ChildNotFound(child_name.clone())));
                                    }
                                }
                                None
                            }
                            (RestartType::Temporary, Ok(Err(reason))) => {
                                tracing::warn!(
                                    supervisor = %self.name,
                                    child = %child_name,
                                    instance = inst_id,
                                    reason = %reason,
                                    "Temporary child exited with error (not restarting per RestartType::Temporary)."
                                );
                                self.child_specs
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .retain(|s| s.name != child_name);
                                if self.children.get(&child_name).is_some_and(|e| e.instance_id == inst_id)
                                    && let Some(mut entry) = self.children.remove(&child_name)
                                {
                                    self.child_order.retain(|n| n != &child_name);
                                    entry.slot_tx.send_replace(SlotState::Stopped);
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(SupervisorError::ChildNotFound(child_name.clone())));
                                    }
                                }
                                None
                            }
                            (RestartType::Temporary, Err(panic_err)) => {
                                if !panic_err.is_cancelled() {
                                    tracing::warn!(
                                        supervisor = %self.name,
                                        child = %child_name,
                                        instance = inst_id,
                                        error = ?panic_err,
                                        "Temporary child panicked (not restarting per RestartType::Temporary)."
                                    );
                                }
                                self.child_specs
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .retain(|s| s.name != child_name);
                                if self.children.get(&child_name).is_some_and(|e| e.instance_id == inst_id)
                                    && let Some(mut entry) = self.children.remove(&child_name)
                                {
                                    self.child_order.retain(|n| n != &child_name);
                                    entry.slot_tx.send_replace(SlotState::Stopped);
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(SupervisorError::ChildNotFound(child_name.clone())));
                                    }
                                }
                                None
                            }

                            (RestartType::Transient, Ok(Ok(()))) => {
                                tracing::info!(
                                    supervisor = %self.name,
                                    child = %child_name,
                                    instance = inst_id,
                                    "Child terminated cleanly (Transient; not restarting)."
                                );
                                self.child_specs
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .retain(|s| s.name != child_name);
                                if let Some(entry) = self.children.get_mut(&child_name)
                                    && entry.instance_id == inst_id
                                {
                                    entry.is_stopped = true;
                                    entry.sender = Arc::new(());
                                    entry.slot_tx.send_replace(SlotState::Stopped);
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(SupervisorError::ChildNotFound(child_name.clone())));
                                    }
                                }
                                None
                            }

                            (RestartType::Permanent, Ok(Ok(()))) => {
                                tracing::warn!(
                                    supervisor = %self.name,
                                    child = %child_name,
                                    instance = inst_id,
                                    "Permanent child exited normally with Ok(()); restarting per RestartType::Permanent."
                                );
                                Some(format!(
                                    "Permanent child '{}' #{} exited normally",
                                    child_name, inst_id
                                ))
                            }

                            (RestartType::Transient | RestartType::Permanent, Ok(Err(reason))) => {
                                Some(reason)
                            }
                            (RestartType::Transient | RestartType::Permanent, Err(panic_err)) => {
                                if panic_err.is_cancelled() {
                                    tracing::debug!(
                                        supervisor = %self.name,
                                        child = %child_name,
                                        instance = inst_id,
                                        "ℹ️ Child cancelled by supervisor restart strategy."
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
                            child = %child_name,
                            instance = inst_id,
                            reason = %crash_reason,
                            "🚨 ALARM: Child terminated and requires supervision recovery!"
                        );

                        // Determine which children to restart based on strategy
                        let children_to_restart: Vec<String> = match self.strategy {
                            SupervisionStrategy::OneForOne => vec![child_name.clone()],
                            SupervisionStrategy::OneForAll => self.child_order.clone(),
                            SupervisionStrategy::RestForOne => {
                                if let Some(pos) = self.child_order.iter().position(|n| n == &child_name) {
                                    self.child_order[pos..].to_vec()
                                } else {
                                    vec![child_name.clone()]
                                }
                            }
                        };

                        tracing::info!(
                            supervisor = %self.name,
                            strategy = ?self.strategy,
                            targets = ?children_to_restart,
                            "📋 Initiating restart sequence for target group."
                        );

                        // Check crash limit and compute backoff on the child that triggered the crash
                        let maybe_backoff = if let Some(child_entry) = self.children.get_mut(&child_name) {
                            let res = child_entry.restart_tracker.record_crash();
                            if let Some(backoff) = res {
                                child_entry.restart_tracker.offset_for_backoff(backoff);
                            }
                            res
                        } else {
                            Some(Duration::ZERO)
                        };

                        match maybe_backoff {
                            Some(backoff) => {
                                // Mark all target children as restarting with a fresh restart ticket
                                // and gracefully terminate siblings in reverse order.
                                let mut term_receivers = Vec::new();
                                let mut temp_to_remove = Vec::new();
                                for target_name in children_to_restart.iter().rev() {
                                    let ticket = self.allocate_restart_ticket();
                                    if let Some(entry) = self.children.get_mut(target_name) {
                                        if target_name != &child_name
                                            && (entry.is_stopped || entry.is_stopping)
                                        {
                                            continue;
                                        }
                                        let is_running =
                                            target_name != &child_name && !*entry.term_rx.borrow();
                                        if entry.restart_type == RestartType::Temporary {
                                            temp_to_remove.push((target_name.clone(), is_running));
                                        } else {
                                            entry.is_restarting = true;
                                            entry.restart_ticket = ticket;
                                            entry.slot_tx.send_replace(SlotState::Restarting {
                                                instance_id: entry.instance_id,
                                            });
                                            if is_running {
                                                let rx = Self::initiate_child_termination(
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
                                    self.child_order.retain(|n| n != &temp_name);
                                    self.child_specs
                                        .write()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        .retain(|s| s.name != temp_name);
                                    if let Some(mut entry) = self.children.remove(&temp_name) {
                                        let _ = entry.slot_tx.send(SlotState::Stopped);
                                        for waiter in entry.pending_resolutions.drain(..) {
                                            let _ = waiter.send(Err(SupervisorError::ChildNotFound(
                                                temp_name.clone(),
                                            )));
                                        }
                                        if is_running {
                                            let mut rx = Self::initiate_child_termination(
                                                self.name.clone(),
                                                temp_name,
                                                &mut entry,
                                            );
                                            term_receivers.push(rx.clone());
                                            tokio::spawn(async move {
                                                let _ = rx.wait_for(|&done| done).await;
                                                drop(entry);
                                            });
                                        }
                                    }
                                }

                                let targets: Vec<(String, u64)> = children_to_restart
                                    .iter()
                                    .filter_map(|name| {
                                        self.children.get(name).and_then(|entry| {
                                            if (name != &child_name
                                                && (entry.is_stopped || entry.is_stopping))
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
                                        targets = ?children_to_restart,
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
                                        child = %child_name,
                                        "🛑 CRASH-LOOP ESCALATION: Child exceeded max restarts. Escalating to parent Supervisor!"
                                    );
                                    return Err(format!(
                                        "Supervisor '{}' reached max restart threshold for child '{}'; escalating to parent Supervisor!",
                                        self.name, child_name
                                    ));
                                }

                                tracing::error!(
                                    supervisor = %self.name,
                                    child = %child_name,
                                    targets = ?children_to_restart,
                                    "🛑 CRASH-LOOP DETECTED: Child exceeded max restarts! Marking restart group as permanently FAILED."
                                );

                                let mut temp_to_remove = Vec::new();
                                for target_name in children_to_restart.iter().rev() {
                                    let ticket = self.allocate_restart_ticket();
                                    if let Some(entry) = self.children.get_mut(target_name) {
                                        if target_name != &child_name
                                            && (entry.is_stopped || entry.is_stopping)
                                        {
                                            continue;
                                        }
                                        let is_running =
                                            target_name != &child_name && !*entry.term_rx.borrow();
                                        if entry.restart_type == RestartType::Temporary {
                                            temp_to_remove.push((target_name.clone(), is_running));
                                            continue;
                                        }
                                        entry.is_failed = true;
                                        entry.is_restarting = false;
                                        entry.restart_ticket = ticket;
                                        let err = SupervisorError::MaxRestartsExceeded(
                                            target_name.clone(),
                                            entry.restart_tracker.policy.max_restarts,
                                            entry.restart_tracker.policy.time_window,
                                        );
                                        entry.slot_tx.send_replace(SlotState::Failed(err.clone()));
                                        for waiter in entry.pending_resolutions.drain(..) {
                                            let _ = waiter.send(Err(err.clone()));
                                        }
                                        if is_running {
                                            let _ = Self::initiate_child_termination(
                                                self.name.clone(),
                                                target_name.clone(),
                                                entry,
                                            );
                                        }
                                    }
                                }

                                for (temp_name, is_running) in temp_to_remove {
                                    self.child_order.retain(|n| n != &temp_name);
                                    self.child_specs
                                        .write()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        .retain(|s| s.name != temp_name);
                                    if let Some(mut entry) = self.children.remove(&temp_name) {
                                        let _ = entry.slot_tx.send(SlotState::Stopped);
                                        for waiter in entry.pending_resolutions.drain(..) {
                                            let _ = waiter.send(Err(SupervisorError::ChildNotFound(
                                                temp_name.clone(),
                                            )));
                                        }
                                        if is_running {
                                            let mut rx = Self::initiate_child_termination(
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
                            }
                        }
                    }
                }
            }
        }
    }

    /// Spawns fresh child instances for the requested target names if their restart ticket matches.
    fn execute_restart(&mut self, targets: Vec<(String, u64)>) -> Result<(), SupervisorError> {
        let mut result = Ok(());
        for (target_name, expected_ticket) in targets {
            if let Some(entry) = self.children.get_mut(&target_name)
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
                    "🔄 Spawning replacement child..."
                );

                let (shutdown_tx, shutdown_rx) = watch::channel(false);
                let spawn_ctx = SpawnContext {
                    instance_id: new_inst,
                    shutdown_rx,
                    supervisor: self.self_handle.clone(),
                };

                let spawn_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    (entry.factory)(spawn_ctx)
                }));
                let (new_sender, task_handle) = match spawn_res {
                    Ok(pair) => pair,
                    Err(panic_err) => {
                        tracing::error!(
                            supervisor = %self.name,
                            child = %target_name,
                            panic = ?panic_err,
                            "💥 Child factory panicked during restart!"
                        );
                        entry.is_failed = true;
                        entry
                            .slot_tx
                            .send_replace(SlotState::Failed(SupervisorError::SupervisorUnavailable));
                        for waiter in entry.pending_resolutions.drain(..) {
                            let _ = waiter.send(Err(SupervisorError::SupervisorUnavailable));
                        }
                        result = Err(SupervisorError::SupervisorUnavailable);
                        continue;
                    }
                };
                let (term_tx, term_rx) = watch::channel(false);
                let name_for_task = target_name.clone();
                entry.abort_handle = task_handle.abort_handle();
                entry.term_rx = term_rx;
                entry.shutdown_tx = shutdown_tx;
                entry.sender = new_sender.clone();
                entry.slot_tx.send_replace(SlotState::Running {
                    instance_id: new_inst,
                    endpoint: new_sender.clone(),
                });

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
// Unified Supervisor Public Handle
// ============================================================================

#[derive(Clone)]
enum SupervisorChannel {
    /// External handle keeping the supervisor alive, with an optional reactive slot subscription from its parent.
    Strong {
        sender: Arc<RwLock<Sender<SupervisorCommand>>>,
        slot_rx: Option<Arc<RwLock<watch::Receiver<SlotState>>>>,
    },
    /// Internal handle passed via [`SpawnContext`] / [`super::stateful_actor::ActorContext`] that does not prevent drop shutdown.
    Weak {
        sender: Arc<RwLock<WeakSender<SupervisorCommand>>>,
    },
}

/// Public thread-safe client handle for interacting with any [`Supervisor`] in the supervision hierarchy.
#[derive(Clone)]
pub struct SupervisorHandle {
    name: String,
    channel: SupervisorChannel,
    parent: Option<Arc<SupervisorHandle>>,
}

impl std::fmt::Debug for SupervisorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisorHandle")
            .field("name", &self.name)
            .finish()
    }
}

impl SupervisorHandle {
    pub(crate) fn new_root_handle(
        name: impl Into<String>,
        sender: Sender<SupervisorCommand>,
    ) -> Self {
        Self {
            name: name.into(),
            channel: SupervisorChannel::Strong {
                sender: Arc::new(RwLock::new(sender)),
                slot_rx: None,
            },
            parent: None,
        }
    }

    pub(crate) fn new_child_handle(
        name: impl Into<String>,
        sender: Sender<SupervisorCommand>,
        slot_rx: watch::Receiver<SlotState>,
        parent: SupervisorHandle,
    ) -> Self {
        Self {
            name: name.into(),
            channel: SupervisorChannel::Strong {
                sender: Arc::new(RwLock::new(sender)),
                slot_rx: Some(Arc::new(RwLock::new(slot_rx))),
            },
            parent: Some(Arc::new(parent)),
        }
    }

    pub(crate) fn new_weak(
        name: impl Into<String>,
        sender: WeakSender<SupervisorCommand>,
        parent: Option<SupervisorHandle>,
    ) -> Self {
        Self {
            name: name.into(),
            channel: SupervisorChannel::Weak {
                sender: Arc::new(RwLock::new(sender.clone())),
            },
            parent: parent.map(|p| Arc::new(p.downgrade())),
        }
    }

    /// Creates a weak version of this [`SupervisorHandle`] that does not prevent supervisor termination on drop.
    pub(crate) fn downgrade(&self) -> Self {
        match &self.channel {
            SupervisorChannel::Strong { sender, .. } => {
                let weak = sender
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .downgrade();
                Self {
                    name: self.name.clone(),
                    channel: SupervisorChannel::Weak {
                        sender: Arc::new(RwLock::new(weak)),
                    },
                    parent: self.parent.as_ref().map(|p| Arc::new(p.downgrade())),
                }
            }
            SupervisorChannel::Weak { .. } => self.clone(),
        }
    }

    /// Returns the registered name of this [`Supervisor`].
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Obtains an active command sender to this supervisor.
    /// If this supervisor is a child of a parent supervisor and was restarted, transparently rebinds via its [`SlotState`] watch channel.
    pub(crate) async fn get_sender(&self) -> Result<Sender<SupervisorCommand>, SupervisorError> {
        match &self.channel {
            SupervisorChannel::Strong { sender, slot_rx } => {
                {
                    let guard = sender
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if !guard.is_closed() {
                        return Ok(guard.clone());
                    }
                }

                // Rebind via reactive watch slot if supervised by a parent
                if let Some(slot_lock) = slot_rx {
                    let mut rx = slot_lock
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    loop {
                        let wait_state = rx
                            .wait_for(|s| match s {
                                SlotState::Running { endpoint, .. } => endpoint
                                    .downcast_ref::<Sender<SupervisorCommand>>()
                                    .is_some_and(|tx| !tx.is_closed()),
                                SlotState::Stopped | SlotState::Failed(_) => true,
                                SlotState::Restarting { .. } => false,
                            })
                            .await
                            .ok()
                            .map(|s| s.clone());

                        if let Some(state) = wait_state {
                            match &state {
                                SlotState::Running { endpoint, .. } => {
                                    if let Some(new_tx) =
                                        endpoint.downcast_ref::<Sender<SupervisorCommand>>()
                                    {
                                        *sender
                                            .write()
                                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                            new_tx.clone();
                                        return Ok(new_tx.clone());
                                    }
                                }
                                SlotState::Failed(err) => return Err(err.clone()),
                                SlotState::Stopped => {
                                    return Err(SupervisorError::SupervisorUnavailable);
                                }
                                SlotState::Restarting { .. } => unreachable!(),
                            }
                        }

                        // Parent supervisor's watch sender was dropped (e.g. parent supervisor restarted)
                        let Some(parent) = &self.parent else {
                            return Err(SupervisorError::SupervisorUnavailable);
                        };
                        let new_rx = Box::pin(parent.resolve_child_slot(&self.name))
                            .await
                            .map_err(|_| SupervisorError::SupervisorUnavailable)?;
                        *slot_lock
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = new_rx.clone();
                        rx = new_rx;
                    }
                }

                Err(SupervisorError::SupervisorUnavailable)
            }

            SupervisorChannel::Weak { sender } => {
                {
                    let guard = sender
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(upgraded) = guard.upgrade()
                        && !upgraded.is_closed()
                    {
                        return Ok(upgraded);
                    }
                }

                if let Some(parent) = &self.parent {
                    let new_tx = Box::pin(
                        parent.resolve_child_channel::<Sender<SupervisorCommand>>(&self.name),
                    )
                    .await
                    .map_err(|_| SupervisorError::SupervisorUnavailable)?;
                    if !new_tx.is_closed() {
                        *sender
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            new_tx.downgrade();
                        return Ok(new_tx);
                    }
                }

                Err(SupervisorError::SupervisorUnavailable)
            }
        }
    }

    /// Starts and supervises a new child (leaf actor or nested [`SupervisorBuilder`]) from a [`ChildSpec`].
    pub async fn start_child(&self, spec: impl Into<ChildSpec>) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(SupervisorCommand::StartChild {
                spec: spec.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;

        rx.await.map_err(|_| SupervisorError::SupervisorUnavailable)?
    }

    /// Manually restarts or recovers a child, resetting its failed status and backoff counters.
    /// Under [`SupervisionStrategy::OneForAll`] or [`SupervisionStrategy::RestForOne`], also restarts coupled siblings.
    pub async fn restart_child(
        &self,
        child_name: impl Into<String>,
    ) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(SupervisorCommand::RestartChild {
                child_name: child_name.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;

        rx.await.map_err(|_| SupervisorError::SupervisorUnavailable)?
    }

    /// Gracefully stops a specific supervised child according to its [`ShutdownPolicy`].
    /// Even if the child has [`RestartType::Permanent`], an explicit stop will not trigger a restart.
    pub async fn stop_child(&self, child_name: impl Into<String>) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(SupervisorCommand::StopChild {
                child_name: child_name.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;

        rx.await.map_err(|_| SupervisorError::SupervisorUnavailable)?
    }

    /// Gracefully stops (if running) and completely removes a child's specification and state from this supervisor.
    pub async fn delete_child(&self, child_name: impl Into<String>) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(SupervisorCommand::DeleteChild {
                child_name: child_name.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;

        rx.await.map_err(|_| SupervisorError::SupervisorUnavailable)?
    }

    /// Gracefully shuts down all supervised children in reverse registration order and terminates this [`Supervisor`].
    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        let current_tx = match &self.channel {
            SupervisorChannel::Strong { sender, .. } => {
                let guard = sender
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if guard.is_closed() {
                    return Ok(());
                }
                guard.clone()
            }
            SupervisorChannel::Weak { sender } => {
                let guard = sender
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match guard.upgrade() {
                    Some(tx) if !tx.is_closed() => tx,
                    _ => return Ok(()),
                }
            }
        };

        let (tx, rx) = oneshot::channel();
        if current_tx
            .send(SupervisorCommand::Shutdown { respond_to: tx })
            .await
            .is_err()
        {
            return Ok(());
        }

        let _ = rx.await;
        Ok(())
    }

    /// Subscribes to the reactive [`SlotState`] watch channel for a supervised child.
    pub(crate) async fn resolve_child_slot(
        &self,
        child_name: &str,
    ) -> Result<watch::Receiver<SlotState>, SupervisorError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(SupervisorCommand::ResolveChildSlot {
                child_name: child_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;

        rx.await.map_err(|_| SupervisorError::SupervisorUnavailable)?
    }

    /// Retrieves a direct [`ResourceClient`] data-plane handle for a supervised [`super::generic_actor::ResourceActor`].
    pub async fn get_resource_client<
        T: Clone + Send + Sync + 'static,
        E: Clone + Send + Sync + 'static,
    >(
        &self,
        child_name: impl Into<String>,
    ) -> Result<ResourceClient<T, E>, SupervisorError> {
        let name = child_name.into();
        let mut slot_rx = self.resolve_child_slot(&name).await?;
        let state = slot_rx
            .wait_for(|s| !matches!(s, SlotState::Restarting { .. }))
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;
        let endpoint = match &*state {
            SlotState::Running { endpoint, .. } => {
                if let Some(ep) = endpoint.downcast_ref::<ResourceEndpoint<T, E>>() {
                    ep.clone()
                } else {
                    return Err(SupervisorError::ChildTypeMismatch(name));
                }
            }
            SlotState::Failed(err) => return Err(err.clone()),
            SlotState::Stopped => return Err(SupervisorError::ChildNotFound(name)),
            SlotState::Restarting { .. } => unreachable!(),
        };
        drop(state);

        Ok(ResourceClient::new(name, endpoint, slot_rx, self.clone()))
    }

    /// Retrieves a direct [`ActorClient`] data-plane handle for a supervised [`StatefulActor`].
    pub async fn get_actor_client<A: StatefulActor>(
        &self,
        child_name: impl Into<String>,
    ) -> Result<ActorClient<A>, SupervisorError> {
        let name = child_name.into();
        let mut slot_rx = self.resolve_child_slot(&name).await?;
        let state = slot_rx
            .wait_for(|s| !matches!(s, SlotState::Restarting { .. }))
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;
        let sender = match &*state {
            SlotState::Running { endpoint, .. } => {
                if let Some(tx) = endpoint.downcast_ref::<Sender<ActorEnvelope<A>>>() {
                    tx.clone()
                } else {
                    return Err(SupervisorError::ChildTypeMismatch(name));
                }
            }
            SlotState::Failed(err) => return Err(err.clone()),
            SlotState::Stopped => return Err(SupervisorError::ChildNotFound(name)),
            SlotState::Restarting { .. } => unreachable!(),
        };
        drop(state);

        Ok(ActorClient::new(name, sender, slot_rx, self.clone()))
    }

    /// Resolves a supervised child [`Supervisor`] by name and returns a resilient [`SupervisorHandle`].
    pub async fn get_supervisor(
        &self,
        child_name: impl Into<String>,
    ) -> Result<SupervisorHandle, SupervisorError> {
        let name = child_name.into();
        let mut slot_rx = self.resolve_child_slot(&name).await?;
        let state = slot_rx
            .wait_for(|s| !matches!(s, SlotState::Restarting { .. }))
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;
        let sender = match &*state {
            SlotState::Running { endpoint, .. } => {
                if let Some(tx) = endpoint.downcast_ref::<Sender<SupervisorCommand>>() {
                    tx.clone()
                } else {
                    return Err(SupervisorError::ChildTypeMismatch(name));
                }
            }
            SlotState::Failed(err) => return Err(err.clone()),
            SlotState::Stopped => return Err(SupervisorError::ChildNotFound(name)),
            SlotState::Restarting { .. } => unreachable!(),
        };
        drop(state);

        Ok(SupervisorHandle::new_child_handle(
            name,
            sender,
            slot_rx,
            self.clone(),
        ))
    }

    /// Queries the supervisor's registry to retrieve the untyped endpoint for a child.
    pub async fn resolve_child_channel_untyped(
        &self,
        child_name: &str,
    ) -> Result<UntypedSender, SupervisorError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(SupervisorCommand::ResolveChildChannel {
                child_name: child_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;

        rx.await.map_err(|_| SupervisorError::SupervisorUnavailable)?
    }

    /// Queries the supervisor's registry and downcasts the endpoint to the expected type `S`.
    pub async fn resolve_child_channel<S: Clone + Send + Sync + 'static>(
        &self,
        child_name: &str,
    ) -> Result<S, SupervisorError> {
        let untyped = self.resolve_child_channel_untyped(child_name).await?;
        if let Some(typed) = untyped.downcast_ref::<S>() {
            Ok(typed.clone())
        } else {
            Err(SupervisorError::ChildTypeMismatch(child_name.to_string()))
        }
    }

    /// Injects an intentional crash into a specific child managed by this supervisor.
    #[cfg(any(test, feature = "chaos_test"))]
    pub async fn trigger_child_crash(&self, child_name: &str) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(SupervisorCommand::TriggerChildCrash {
                child_name: child_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;

        rx.await.map_err(|_| SupervisorError::SupervisorUnavailable)?
    }

    /// Injects an intentional crash into this supervisor itself.
    #[cfg(any(test, feature = "chaos_test"))]
    pub async fn simulate_crash(&self) -> Result<(), SupervisorError> {
        let cmd_sender = self.get_sender().await?;
        cmd_sender
            .send(SupervisorCommand::SimulateCrash)
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)
    }
}

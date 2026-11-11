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
//! - Lock-free, zero-polling data-plane rebinding via per-child [`tokio::sync::watch`] channels ([`SlotState`])
//! - Runtime introspection snapshots with built-in timeouts ([`SupervisorHandle::snapshot`], [`SupervisorHandle::snapshot_timeout`]), whole-hierarchy snapshots under one shared deadline ([`SupervisorHandle::snapshot_tree`], [`SupervisorHandle::snapshot_tree_timeout`]), and read-only status observation ([`SupervisorHandle::watch_child_status`]).

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{
    mpsc::{self, Receiver, Sender, WeakSender},
    oneshot, watch,
};
use tokio::task::{JoinError, JoinHandle, JoinSet};

use super::generic_actor::{ResourceClient, ResourceEndpoint};
use super::model::{
    DEFAULT_SNAPSHOT_TIMEOUT, RestartPolicy, RestartTracker, RestartType, ShutdownPolicy,
    SupervisionStrategy, SupervisorError, TaskResult,
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

/// Public lifecycle status of a supervised child at the time of inspection.
///
/// Mirrors [`SlotState`] without exposing internal communication endpoints (`UntypedSender`).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildStatus {
    /// The child is currently running under `instance_id`.
    Running { instance_id: usize },
    /// The child is shutting down for restart or waiting in backoff before spawning the replacement for `instance_id`.
    Restarting { instance_id: usize },
    /// The child stopped cleanly and is not scheduled to restart.
    Stopped,
    /// The child failed permanently and requires manual intervention (`restart_child`) or parent recovery.
    Failed(SupervisorError),
}

impl ChildStatus {
    /// Returns `true` if the child is currently running (`ChildStatus::Running`).
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    /// Returns `true` if the child is restarting or waiting in backoff (`ChildStatus::Restarting`).
    pub fn is_restarting(&self) -> bool {
        matches!(self, Self::Restarting { .. })
    }

    /// Returns `true` if the child is stopped (`ChildStatus::Stopped`).
    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped)
    }

    /// Returns `true` if the child is permanently failed (`ChildStatus::Failed`).
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// Returns the active or restarting `instance_id`, or `None` if `Stopped` / `Failed`.
    pub fn instance_id(&self) -> Option<usize> {
        match self {
            Self::Running { instance_id } | Self::Restarting { instance_id } => Some(*instance_id),
            Self::Stopped | Self::Failed(_) => None,
        }
    }
}

impl From<&SlotState> for ChildStatus {
    fn from(slot: &SlotState) -> Self {
        match slot {
            SlotState::Running { instance_id, .. } => Self::Running {
                instance_id: *instance_id,
            },
            SlotState::Restarting { instance_id } => Self::Restarting {
                instance_id: *instance_id,
            },
            SlotState::Stopped => Self::Stopped,
            SlotState::Failed(err) => Self::Failed(err.clone()),
        }
    }
}

impl From<SlotState> for ChildStatus {
    fn from(slot: SlotState) -> Self {
        Self::from(&slot)
    }
}

/// Point-in-time snapshot of a single supervised child within a [`SupervisorSnapshot`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSnapshot {
    /// Registered name of the child inside its parent [`Supervisor`].
    pub name: String,
    /// Current lifecycle status of the child.
    pub status: ChildStatus,
    /// Current or most recently spawned instance ID (`1`, `2`, ...).
    pub instance_id: usize,
    /// Configured OTP restart type (`Transient`, `Permanent`, or `Temporary`).
    pub restart_type: RestartType,
    /// Number of recent restarts currently inside the child's [`RestartPolicy::time_window`].
    pub restarts_in_window: usize,
    /// `true` if this child is a nested [`Supervisor`] (registered via [`SupervisorBuilder`]),
    /// `false` for leaf actors. Supervisor children can be descended into with
    /// [`SupervisorHandle::snapshot_tree`].
    pub is_supervisor: bool,
}

/// Point-in-time introspection snapshot of a [`Supervisor`] and its registered children.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorSnapshot {
    /// Registered name of this supervisor.
    pub name: String,
    /// Current instance ID of this supervisor (`1` initially; increments on restart).
    pub instance_id: usize,
    /// Supervision restart strategy applied by this supervisor.
    pub strategy: SupervisionStrategy,
    /// Supervised children in startup / registration order.
    pub children: Vec<ChildSnapshot>,
}

impl SupervisorSnapshot {
    /// Looks up a child snapshot by registered name.
    pub fn child(&self, name: &str) -> Option<&ChildSnapshot> {
        self.children.iter().find(|c| c.name == name)
    }

    /// Returns `true` if every child in [`Self::children`] is [`ChildStatus::Running`].
    ///
    /// Note that this returns `true` when `children` is empty; check
    /// `!snapshot.children.is_empty()` as well if at least one active child is required.
    pub fn all_running(&self) -> bool {
        self.children.iter().all(|c| c.status.is_running())
    }

    /// Returns `true` if at least one child is in [`ChildStatus::Failed`].
    pub fn any_failed(&self) -> bool {
        self.children.iter().any(|c| c.status.is_failed())
    }

    /// Returns `true` if at least one child is in [`ChildStatus::Restarting`].
    pub fn any_restarting(&self) -> bool {
        self.children.iter().any(|c| c.status.is_restarting())
    }

    /// Returns the number of children in [`ChildStatus::Running`].
    pub fn running_count(&self) -> usize {
        self.children
            .iter()
            .filter(|c| c.status.is_running())
            .count()
    }

    /// Returns the number of children in [`ChildStatus::Restarting`].
    pub fn restarting_count(&self) -> usize {
        self.children
            .iter()
            .filter(|c| c.status.is_restarting())
            .count()
    }

    /// Returns the number of children in [`ChildStatus::Stopped`].
    pub fn stopped_count(&self) -> usize {
        self.children
            .iter()
            .filter(|c| c.status.is_stopped())
            .count()
    }

    /// Returns the number of children in [`ChildStatus::Failed`].
    pub fn failed_count(&self) -> usize {
        self.children
            .iter()
            .filter(|c| c.status.is_failed())
            .count()
    }
}

/// Point-in-time introspection snapshot of an entire supervision (sub)tree, returned by
/// [`SupervisorHandle::snapshot_tree`] and [`SupervisorHandle::snapshot_tree_timeout`].
///
/// Composes the flat per-level [`SupervisorSnapshot`] with one [`SubtreeSnapshot`] entry for every
/// child that is itself a [`Supervisor`] (`ChildSnapshot::is_supervisor`), recursively. Because the
/// tree is collected level by level from independent supervisors, it is **not** an atomic snapshot
/// across levels: a child may restart between the moment its parent and the child itself are queried.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorTreeSnapshot {
    /// Flat snapshot of this supervisor and its direct children.
    pub snapshot: SupervisorSnapshot,
    /// One entry per direct child supervisor, in registration order.
    pub subtrees: Vec<SubtreeSnapshot>,
}

/// Outcome of descending into one child [`Supervisor`] while collecting a [`SupervisorTreeSnapshot`].
///
/// A failing or slow subtree never discards the rest of the tree: its error is recorded in
/// [`Self::result`] while sibling subtrees and ancestors are still reported.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubtreeSnapshot {
    /// Registered name of the child supervisor inside its parent.
    pub child_name: String,
    /// The child's own tree, or the reason it could not be inspected:
    /// - [`SupervisorError::Timeout`] if it did not answer before the overall deadline,
    /// - [`SupervisorError::SupervisorUnavailable`] if it is currently restarting or terminated meanwhile,
    /// - [`SupervisorError::ChildNotFound`] if it was stopped, or the permanent failure reason if it failed.
    pub result: Result<SupervisorTreeSnapshot, SupervisorError>,
}

impl SupervisorTreeSnapshot {
    /// Looks up the [`SubtreeSnapshot`] entry for the direct child supervisor `child_name`.
    pub fn subtree(&self, child_name: &str) -> Option<&SubtreeSnapshot> {
        self.subtrees.iter().find(|s| s.child_name == child_name)
    }

    /// Navigates down a path of nested supervisor names and returns the reachable tree found there.
    ///
    /// An empty `path` returns `self`. Returns `None` if a name is unknown or its subtree could
    /// not be inspected (see [`SubtreeSnapshot::result`]).
    pub fn find(&self, path: &[&str]) -> Option<&SupervisorTreeSnapshot> {
        let mut current = self;
        for name in path {
            current = current.subtree(name)?.result.as_ref().ok()?;
        }
        Some(current)
    }

    /// Returns `true` if every child on every level is [`ChildStatus::Running`] **and** every
    /// child supervisor could be inspected.
    ///
    /// Like [`SupervisorSnapshot::all_running`], this is `true` for a tree without any children.
    pub fn all_running_deep(&self) -> bool {
        self.snapshot.all_running()
            && self
                .subtrees
                .iter()
                .all(|s| s.result.as_ref().is_ok_and(Self::all_running_deep))
    }

    /// Returns `true` if at least one inspected level contains a [`ChildStatus::Failed`] child.
    pub fn any_failed_deep(&self) -> bool {
        self.snapshot.any_failed()
            || self
                .subtrees
                .iter()
                .any(|s| s.result.as_ref().is_ok_and(Self::any_failed_deep))
    }

    /// Returns `true` if at least one inspected level contains a [`ChildStatus::Restarting`] child.
    pub fn any_restarting_deep(&self) -> bool {
        self.snapshot.any_restarting()
            || self
                .subtrees
                .iter()
                .any(|s| s.result.as_ref().is_ok_and(Self::any_restarting_deep))
    }

    /// Returns the number of child supervisors, on every inspected level, that could not be inspected.
    pub fn unavailable_count(&self) -> usize {
        self.subtrees
            .iter()
            .map(|s| match &s.result {
                Ok(tree) => tree.unavailable_count(),
                Err(_) => 1,
            })
            .sum()
    }
}

/// Read-only reactive observer for a supervised child's [`ChildStatus`].
///
/// Wraps the supervisor's existing per-child `watch` channel without exposing internal endpoints.
/// If the child is deleted (`delete_child` or completed `Temporary` child) or the owning
/// supervisor terminates/restarts, [`Self::changed`] and [`Self::wait_for`] return
/// [`SupervisorError::SupervisorUnavailable`].
#[derive(Clone)]
pub struct ChildStatusWatcher {
    child_name: String,
    rx: watch::Receiver<SlotState>,
}

impl std::fmt::Debug for ChildStatusWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildStatusWatcher")
            .field("child_name", &self.child_name)
            .field("status", &self.current())
            .finish()
    }
}

impl ChildStatusWatcher {
    /// Returns the registered name of the observed child.
    pub fn child_name(&self) -> &str {
        &self.child_name
    }

    /// Returns the child's latest observed [`ChildStatus`] without blocking.
    pub fn current(&self) -> ChildStatus {
        ChildStatus::from(&*self.rx.borrow())
    }

    /// Waits for the next status transition on the observed child slot.
    ///
    /// Returns [`SupervisorError::SupervisorUnavailable`] once the slot sender is closed (for
    /// instance after [`SupervisorHandle::delete_child`] or supervisor termination/restart).
    pub async fn changed(&mut self) -> Result<(), SupervisorError> {
        self.rx
            .changed()
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)
    }

    /// Waits until [`ChildStatus`] satisfies `predicate` (checking the current status first)
    /// and returns a clone of the matching status.
    pub async fn wait_for<F>(&mut self, mut predicate: F) -> Result<ChildStatus, SupervisorError>
    where
        F: FnMut(&ChildStatus) -> bool,
    {
        let status_ref = self
            .rx
            .wait_for(|slot| {
                let status = ChildStatus::from(slot);
                predicate(&status)
            })
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;
        Ok(ChildStatus::from(&*status_ref))
    }
}

/// Specification defining a child (leaf actor or child [`Supervisor`]) to be supervised by a [`Supervisor`].
#[derive(Clone)]
pub struct ChildSpec {
    pub(crate) name: String,
    pub(crate) factory: UntypedActorFactory,
    pub(crate) restart_policy: RestartPolicy,
    pub(crate) restart_type: RestartType,
    pub(crate) shutdown_policy: ShutdownPolicy,
    pub(crate) shutdown_signal: Option<ShutdownSignalFn>,
    #[cfg(any(test, feature = "chaos_test"))]
    pub(crate) crash_signal: Option<ShutdownSignalFn>,
    /// `true` if this spec describes a nested [`Supervisor`] rather than a leaf actor.
    pub(crate) is_supervisor: bool,
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
            is_supervisor: false,
        }
    }

    /// Marks this specification as describing a nested [`Supervisor`] (used by tree introspection).
    #[must_use]
    pub(crate) fn mark_supervisor(mut self) -> Self {
        self.is_supervisor = true;
        self
    }

    /// Returns the registered child name in this specification.
    pub fn name(&self) -> &str {
        &self.name
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
            .with_shutdown_signal(shutdown_signal)
            .mark_supervisor();

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
    /// Subscribes to the reactive [`SlotState`] watch channel for status observation,
    /// including children that are currently `Stopped` or `Failed`.
    ObserveChildSlot {
        child_name: String,
        respond_to: oneshot::Sender<Result<watch::Receiver<SlotState>, SupervisorError>>,
    },
    /// Captures a point-in-time [`SupervisorSnapshot`] of this supervisor and its children.
    Snapshot {
        respond_to: oneshot::Sender<SupervisorSnapshot>,
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

type ScheduledRestartBatch = (
    Vec<(String, u64)>,
    Option<oneshot::Sender<Result<(), SupervisorError>>>,
);

/// Extracts a human-readable message from a caught panic payload.
fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown non-string panic payload".to_string()
    }
}

/// Explicit lifecycle state machine for a supervised child slot.
#[derive(Debug, Clone)]
enum ChildPhase {
    /// Child task has been spawned and is running normally.
    Running,
    /// Child is currently undergoing a supervisor-initiated restart wave.
    Restarting,
    /// Child is terminating due to `StopChild` or supervisor shutdown;
    /// once its task exits, it transitions to [`ChildPhase::Stopped`].
    Stopping,
    /// Child has stopped cleanly (via `StopChild`, `Transient` exit, or `Shutdown`).
    Stopped,
    /// Child permanently failed (e.g. `MaxRestartsExceeded` or `FactoryPanicked`).
    Failed(SupervisorError),
}

impl ChildPhase {
    /// Returns `true` if the child is still active or its previous task has not finished exiting yet.
    fn is_active_or_terminating(&self, term_rx: &watch::Receiver<bool>) -> bool {
        !matches!(self, Self::Stopped | Self::Failed(_)) || !*term_rx.borrow()
    }
}

/// Metadata tracked for each actively supervised child (leaf actor or child [`Supervisor`]).
struct ActiveChild {
    sender: UntypedSender,
    slot_tx: watch::Sender<SlotState>,
    shutdown_tx: watch::Sender<bool>,
    abort_handle: Option<tokio::task::AbortHandle>,
    term_rx: watch::Receiver<bool>,
    factory: UntypedActorFactory,
    restart_type: RestartType,
    shutdown_policy: ShutdownPolicy,
    shutdown_signal: Option<ShutdownSignalFn>,
    #[cfg(any(test, feature = "chaos_test"))]
    crash_signal: Option<ShutdownSignalFn>,
    is_supervisor: bool,
    instance_id: usize,
    spawn_id: u64,
    restart_ticket: u64,
    restart_tracker: RestartTracker,
    phase: ChildPhase,
    pending_resolutions: Vec<oneshot::Sender<Result<UntypedSender, SupervisorError>>>,
}

impl Drop for ActiveChild {
    fn drop(&mut self) {
        // Automatically abort the child task when the supervisor drops or terminates
        if let Some(handle) = &self.abort_handle {
            handle.abort();
        }
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
    next_spawn_id: u64,
    join_set: JoinSet<(String, u64, Result<TaskResult, JoinError>)>,
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
            next_spawn_id: 0,
            join_set: JoinSet::new(),
        }
    }

    /// Allocates a monotonically increasing ticket to uniquely identify a child spawn or restart wave.
    fn allocate_restart_ticket(&mut self) -> u64 {
        self.next_restart_ticket = self.next_restart_ticket.wrapping_add(1);
        self.next_restart_ticket
    }

    /// Allocates a strictly monotonic spawn identifier across the lifetime of this supervisor
    /// so stale task completions can never collide even after `delete_child` + `start_child`.
    fn allocate_spawn_id(&mut self) -> u64 {
        self.next_spawn_id = self.next_spawn_id.wrapping_add(1);
        self.next_spawn_id
    }

    /// Spawns a child from a [`ChildSpec`], tracking it in `join_set` and `children`.
    fn spawn_child_from_spec(&mut self, spec: ChildSpec) -> Result<(), SupervisorError> {
        let name = spec.name;
        if let Some(existing) = self.children.get(&name)
            && existing.phase.is_active_or_terminating(&existing.term_rx)
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
                let panic_msg = panic_payload_message(&*panic_err);
                tracing::error!(
                    supervisor = %self.name,
                    instance = self.instance_id,
                    child = %name,
                    panic = %panic_msg,
                    "💥 Child factory panicked during spawn!"
                );
                return Err(SupervisorError::FactoryPanicked(name, panic_msg));
            }
        };

        let abort_handle = task_handle.abort_handle();
        let (term_tx, term_rx) = watch::channel(false);
        let name_for_task = name.clone();
        let ticket = self.allocate_restart_ticket();
        let spawn_id = self.allocate_spawn_id();

        self.join_set.spawn(async move {
            let _guard = TermSignalGuard(term_tx);
            let res = task_handle.await;
            (name_for_task, spawn_id, res)
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
                abort_handle: Some(abort_handle),
                term_rx,
                factory: spec.factory,
                restart_type: spec.restart_type,
                shutdown_policy: spec.shutdown_policy,
                shutdown_signal: spec.shutdown_signal,
                #[cfg(any(test, feature = "chaos_test"))]
                crash_signal: spec.crash_signal,
                is_supervisor: spec.is_supervisor,
                instance_id: inst_id,
                spawn_id,
                restart_ticket: ticket,
                restart_tracker: RestartTracker::new(spec.restart_policy),
                phase: ChildPhase::Running,
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
                if let Some(handle) = abort_handle {
                    handle.abort();
                }
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
                        if let Some(handle) = abort_handle {
                            handle.abort();
                        }
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
        entry.phase = ChildPhase::Stopping;
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

    /// Returns the ordered target group affected by a restart of `child_name` under `self.strategy`.
    fn restart_group_for(&self, child_name: &str) -> Vec<String> {
        match self.strategy {
            SupervisionStrategy::OneForOne => vec![child_name.to_string()],
            SupervisionStrategy::OneForAll => self.child_order.clone(),
            SupervisionStrategy::RestForOne => {
                if let Some(pos) = self.child_order.iter().position(|n| n == child_name) {
                    self.child_order[pos..].to_vec()
                } else {
                    vec![child_name.to_string()]
                }
            }
        }
    }

    /// Removes `child_name` from the shared `child_specs` list.
    fn remove_child_spec(&self, child_name: &str) {
        self.child_specs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|s| s.name != child_name);
    }

    /// Removes a child slot completely (`child_specs`, `child_order`, `children`), publishes
    /// `SlotState::Stopped`, notifies pending waiters, and optionally initiates graceful termination
    /// if the child task is still running.
    fn remove_child_slot(
        &mut self,
        child_name: &str,
        expected_spawn_id: Option<u64>,
        terminate_if_running: bool,
    ) -> Option<watch::Receiver<bool>> {
        self.remove_child_spec(child_name);
        if let Some(expected_id) = expected_spawn_id
            && !self
                .children
                .get(child_name)
                .is_some_and(|e| e.spawn_id == expected_id)
        {
            return None;
        }

        let mut entry = self.children.remove(child_name)?;
        self.child_order.retain(|n| n != child_name);
        entry.phase = ChildPhase::Stopped;
        entry.slot_tx.send_replace(SlotState::Stopped);
        for waiter in entry.pending_resolutions.drain(..) {
            let _ = waiter.send(Err(SupervisorError::ChildNotFound(
                child_name.to_string(),
            )));
        }

        if terminate_if_running && !*entry.term_rx.borrow() {
            let mut term_rx = Self::initiate_child_termination(
                self.name.clone(),
                child_name.to_string(),
                &mut entry,
            );
            let rx_clone = term_rx.clone();
            // Move entry into the spawned task so its Drop (abort_handle.abort())
            // only runs after graceful shutdown completes or times out.
            tokio::spawn(async move {
                let _ = term_rx.wait_for(|&done| done).await;
                drop(entry);
            });
            Some(rx_clone)
        } else {
            None
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
                && matches!(
                    entry.phase,
                    ChildPhase::Running | ChildPhase::Restarting
                )
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

    /// Boots all initially configured child specifications before accepting control commands.
    fn boot_initial_children(&mut self) {
        let initial_specs = self
            .child_specs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for spec in initial_specs {
            if let Err(err) = self.spawn_child_from_spec(spec.clone()) {
                let ticket = self.allocate_restart_ticket();
                let (term_tx, term_rx) = watch::channel(true);
                drop(term_tx);
                let (shutdown_tx, _) = watch::channel(true);
                let (slot_tx, _) = watch::channel(SlotState::Failed(err.clone()));
                if !self.child_order.contains(&spec.name) {
                    self.child_order.push(spec.name.clone());
                }
                self.children.insert(
                    spec.name,
                    ActiveChild {
                        sender: Arc::new(()),
                        slot_tx,
                        shutdown_tx,
                        abort_handle: None,
                        term_rx,
                        factory: spec.factory,
                        restart_type: spec.restart_type,
                        shutdown_policy: spec.shutdown_policy,
                        shutdown_signal: spec.shutdown_signal,
                        #[cfg(any(test, feature = "chaos_test"))]
                        crash_signal: spec.crash_signal,
                        is_supervisor: spec.is_supervisor,
                        instance_id: 1,
                        spawn_id: 0,
                        restart_ticket: ticket,
                        restart_tracker: RestartTracker::new(spec.restart_policy),
                        phase: ChildPhase::Failed(err),
                        pending_resolutions: Vec::new(),
                    },
                );
            }
        }
    }

    /// Main event loop processing commands and monitoring child terminations.
    pub(crate) async fn run(mut self, mut receiver: Receiver<SupervisorCommand>) -> TaskResult {
        tracing::info!(
            supervisor = %self.name,
            instance = self.instance_id,
            strategy = ?self.strategy,
            "🛡️ Supervisor online. Booting children..."
        );

        self.boot_initial_children();

        let (scheduled_restart_tx, mut scheduled_restart_rx) =
            mpsc::channel::<ScheduledRestartBatch>(64);

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
                    let retry_tx = if maybe_respond_to.is_some() {
                        None
                    } else {
                        Some(&scheduled_restart_tx)
                    };
                    match self.execute_restart(targets, retry_tx) {
                        RestartOutcome::Done(res) => {
                            if let Some(respond_to) = maybe_respond_to {
                                let _ = respond_to.send(res);
                            }
                        }
                        RestartOutcome::Escalate(msg) => {
                            return Err(msg);
                        }
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

                    if let Some(outcome) = self
                        .handle_command(cmd, &mut receiver, &scheduled_restart_tx)
                        .await
                    {
                        return outcome;
                    }
                }

                Some(join_res) = self.join_set.join_next() => {
                    if let Ok((child_name, spawn_id, task_result)) = join_res {
                        self.handle_child_exit(
                            child_name,
                            spawn_id,
                            task_result,
                            &scheduled_restart_tx,
                        )?;
                    }
                }
            }
        }
    }

    /// Builds a point-in-time [`SupervisorSnapshot`] ordered according to `self.child_order`.
    fn build_snapshot(&self) -> SupervisorSnapshot {
        let children = self
            .child_order
            .iter()
            .filter_map(|name| {
                self.children.get(name).map(|child| ChildSnapshot {
                    name: name.clone(),
                    status: ChildStatus::from(&*child.slot_tx.borrow()),
                    instance_id: child.instance_id,
                    restart_type: child.restart_type,
                    restarts_in_window: child.restart_tracker.restarts_in_window(),
                    is_supervisor: child.is_supervisor,
                })
            })
            .collect();

        SupervisorSnapshot {
            name: self.name.clone(),
            instance_id: self.instance_id,
            strategy: self.strategy,
            children,
        }
    }

    /// Processes a single incoming [`SupervisorCommand`], returning `Some(TaskResult)` when
    /// the supervisor event loop should terminate (e.g. `Shutdown`, `SimulateCrash`, or escalation).
    async fn handle_command(
        &mut self,
        cmd: SupervisorCommand,
        receiver: &mut Receiver<SupervisorCommand>,
        scheduled_restart_tx: &Sender<ScheduledRestartBatch>,
    ) -> Option<TaskResult> {
        match cmd {
            SupervisorCommand::StartChild { spec, respond_to } => {
                let already_running = self
                    .children
                    .get(&spec.name)
                    .is_some_and(|c| c.phase.is_active_or_terminating(&c.term_rx));
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
                                if let Some(existing_spec) =
                                    guard.iter_mut().find(|s| s.name == spec.name)
                                {
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

            SupervisorCommand::ResolveChildChannel {
                child_name,
                respond_to,
            } => {
                if let Some(child) = self.children.get_mut(&child_name) {
                    match &child.phase {
                        ChildPhase::Failed(err) => {
                            let _ = respond_to.send(Err(err.clone()));
                        }
                        ChildPhase::Stopping | ChildPhase::Stopped => {
                            let _ =
                                respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                        }
                        ChildPhase::Restarting => {
                            child.pending_resolutions.push(respond_to);
                        }
                        ChildPhase::Running if *child.term_rx.borrow() => {
                            child.pending_resolutions.push(respond_to);
                        }
                        ChildPhase::Running => {
                            let _ = respond_to.send(Ok(child.sender.clone()));
                        }
                    }
                } else {
                    let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                }
            }

            SupervisorCommand::ResolveChildSlot {
                child_name,
                respond_to,
            } => {
                if let Some(child) = self.children.get(&child_name) {
                    match &child.phase {
                        ChildPhase::Failed(err) => {
                            let _ = respond_to.send(Err(err.clone()));
                        }
                        ChildPhase::Stopping | ChildPhase::Stopped => {
                            let _ =
                                respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                        }
                        ChildPhase::Running | ChildPhase::Restarting => {
                            let _ = respond_to.send(Ok(child.slot_tx.subscribe()));
                        }
                    }
                } else {
                    let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                }
            }

            SupervisorCommand::ObserveChildSlot {
                child_name,
                respond_to,
            } => {
                if let Some(child) = self.children.get(&child_name) {
                    let _ = respond_to.send(Ok(child.slot_tx.subscribe()));
                } else {
                    let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                }
            }

            SupervisorCommand::Snapshot { respond_to } => {
                let _ = respond_to.send(self.build_snapshot());
            }

            SupervisorCommand::RestartChild {
                child_name,
                respond_to,
            } => {
                if let Err(msg) =
                    self.handle_restart_child(child_name, respond_to, scheduled_restart_tx)
                {
                    return Some(Err(msg));
                }
            }

            SupervisorCommand::StopChild {
                child_name,
                respond_to,
            } => {
                let ticket = self.allocate_restart_ticket();
                if self.children.contains_key(&child_name) {
                    self.remove_child_spec(&child_name);
                    let sup_name = self.name.clone();
                    if let Some(entry) = self.children.get_mut(&child_name) {
                        match entry.phase {
                            ChildPhase::Stopped => {
                                let _ = respond_to.send(Ok(()));
                            }
                            ChildPhase::Stopping if *entry.term_rx.borrow() => {
                                entry.phase = ChildPhase::Stopped;
                                let _ = respond_to.send(Ok(()));
                            }
                            ChildPhase::Stopping => {
                                let mut term_rx = entry.term_rx.clone();
                                tokio::spawn(async move {
                                    let _ = term_rx.wait_for(|&done| done).await;
                                    let _ = respond_to.send(Ok(()));
                                });
                            }
                            ChildPhase::Running | ChildPhase::Restarting | ChildPhase::Failed(_) => {
                                Self::trigger_child_shutdown(
                                    sup_name,
                                    child_name,
                                    entry,
                                    ticket,
                                    true,
                                    Some(respond_to),
                                );
                            }
                        }
                    }
                } else {
                    let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                }
            }

            SupervisorCommand::DeleteChild {
                child_name,
                respond_to,
            } => {
                if self.children.contains_key(&child_name) {
                    if let Some(mut term_rx) = self.remove_child_slot(&child_name, None, true) {
                        tokio::spawn(async move {
                            let _ = term_rx.wait_for(|&done| done).await;
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
                return Some(Ok(()));
            }

            #[cfg(any(test, feature = "chaos_test"))]
            SupervisorCommand::TriggerChildCrash {
                child_name,
                respond_to,
            } => {
                if let Some(child) = self.children.get(&child_name) {
                    match &child.phase {
                        ChildPhase::Failed(err) => {
                            let _ = respond_to.send(Err(err.clone()));
                        }
                        ChildPhase::Stopping | ChildPhase::Stopped => {
                            let _ =
                                respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
                        }
                        ChildPhase::Running | ChildPhase::Restarting => {
                            if let Some(crash_sig) = &child.crash_signal {
                                crash_sig(&child.sender);
                            } else if let Some(handle) = &child.abort_handle {
                                handle.abort();
                            }
                            let _ = respond_to.send(Ok(()));
                        }
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
                return Some(Err(format!(
                    "Intentional crash in Supervisor '{}' #{}",
                    self.name, self.instance_id
                )));
            }
        }
        None
    }

    /// Handles an explicit `SupervisorHandle::restart_child` command across the strategy target group.
    fn handle_restart_child(
        &mut self,
        child_name: String,
        respond_to: oneshot::Sender<Result<(), SupervisorError>>,
        scheduled_restart_tx: &Sender<ScheduledRestartBatch>,
    ) -> Result<(), String> {
        if !self.children.contains_key(&child_name) {
            let _ = respond_to.send(Err(SupervisorError::ChildNotFound(child_name)));
            return Ok(());
        }

        tracing::info!(
            supervisor = %self.name,
            child = %child_name,
            strategy = ?self.strategy,
            "🔄 Manual child restart triggered via Supervisor."
        );

        let children_to_restart = self.restart_group_for(&child_name);
        let mut term_receivers = Vec::new();
        let mut temp_to_remove = Vec::new();

        for target_name in children_to_restart.iter().rev() {
            let ticket = self.allocate_restart_ticket();
            if let Some(entry) = self.children.get_mut(target_name) {
                if target_name != &child_name
                    && matches!(entry.phase, ChildPhase::Stopping | ChildPhase::Stopped)
                {
                    continue;
                }
                if target_name != &child_name && entry.restart_type == RestartType::Temporary {
                    temp_to_remove.push(target_name.clone());
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
                            is_supervisor: entry.is_supervisor,
                        });
                    }
                }

                let is_running = !*entry.term_rx.borrow();
                entry.phase = ChildPhase::Restarting;
                entry.restart_ticket = ticket;
                entry.restart_tracker = RestartTracker::new(entry.restart_tracker.policy);
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

        for temp_name in temp_to_remove {
            if let Some(rx) = self.remove_child_slot(&temp_name, None, true) {
                term_receivers.push(rx);
            }
        }

        let targets: Vec<(String, u64)> = children_to_restart
            .iter()
            .filter_map(|name| {
                self.children.get(name).and_then(|entry| {
                    if name != &child_name
                        && (matches!(entry.phase, ChildPhase::Stopping | ChildPhase::Stopped)
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
            let res = match self.execute_restart(targets, None) {
                RestartOutcome::Done(r) => r,
                RestartOutcome::Escalate(msg) => return Err(msg),
            };
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
        Ok(())
    }

    /// Processes a completed child task from `self.join_set`, ignoring stale task exits and
    /// applying the child's [`RestartType`] semantics.
    fn handle_child_exit(
        &mut self,
        child_name: String,
        spawn_id: u64,
        task_result: Result<TaskResult, JoinError>,
        scheduled_restart_tx: &Sender<ScheduledRestartBatch>,
    ) -> Result<(), String> {
        let (inst_id, restart_type) = match self.children.get_mut(&child_name) {
            Some(entry) if spawn_id != entry.spawn_id => {
                tracing::debug!(
                    supervisor = %self.name,
                    child = %child_name,
                    stale_spawn_id = spawn_id,
                    current_spawn_id = entry.spawn_id,
                    "ℹ️ Ignoring stale termination event for superseded child task."
                );
                return Ok(());
            }
            Some(entry) => match &entry.phase {
                ChildPhase::Stopping => {
                    entry.phase = ChildPhase::Stopped;
                    tracing::debug!(
                        supervisor = %self.name,
                        child = %child_name,
                        instance = entry.instance_id,
                        "ℹ️ Child stopped cleanly following supervisor stop/restart sequence."
                    );
                    return Ok(());
                }
                ChildPhase::Stopped | ChildPhase::Restarting | ChildPhase::Failed(_) => {
                    tracing::debug!(
                        supervisor = %self.name,
                        child = %child_name,
                        instance = entry.instance_id,
                        "ℹ️ Child stopped cleanly following supervisor stop/restart sequence."
                    );
                    return Ok(());
                }
                ChildPhase::Running => (entry.instance_id, entry.restart_type),
            },
            None => return Ok(()),
        };

        let crash_reason: Option<String> = match (restart_type, task_result) {
            (RestartType::Temporary, Ok(Ok(()))) => {
                tracing::info!(
                    supervisor = %self.name,
                    child = %child_name,
                    instance = inst_id,
                    "Temporary child completed cleanly (not restarting)."
                );
                let _ = self.remove_child_slot(&child_name, Some(spawn_id), false);
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
                let _ = self.remove_child_slot(&child_name, Some(spawn_id), false);
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
                let _ = self.remove_child_slot(&child_name, Some(spawn_id), false);
                None
            }

            (RestartType::Transient, Ok(Ok(()))) => {
                tracing::info!(
                    supervisor = %self.name,
                    child = %child_name,
                    instance = inst_id,
                    "Child terminated cleanly (Transient; not restarting)."
                );
                self.remove_child_spec(&child_name);
                if let Some(entry) = self.children.get_mut(&child_name)
                    && entry.spawn_id == spawn_id
                {
                    entry.phase = ChildPhase::Stopped;
                    entry.abort_handle = None;
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

            (RestartType::Transient | RestartType::Permanent, Ok(Err(reason))) => Some(reason),
            (RestartType::Transient | RestartType::Permanent, Err(panic_err)) => {
                if panic_err.is_cancelled() {
                    tracing::debug!(
                        supervisor = %self.name,
                        child = %child_name,
                        instance = inst_id,
                        "ℹ️ Child cancelled by supervisor restart strategy."
                    );
                    return Ok(());
                }
                Some(format!("Task panicked: {:?}", panic_err))
            }
        };

        let Some(crash_reason) = crash_reason else {
            return Ok(());
        };

        tracing::error!(
            supervisor = %self.name,
            child = %child_name,
            instance = inst_id,
            reason = %crash_reason,
            "🚨 ALARM: Child terminated and requires supervision recovery!"
        );

        self.handle_child_failure(child_name, None, scheduled_restart_tx, false)
    }

    /// Evaluates restart budgets, schedules backoff waves, or marks/escalates permanent failure
    /// when a supervised child crashes or its factory panics during recovery.
    fn handle_child_failure(
        &mut self,
        child_name: String,
        factory_panic_err: Option<SupervisorError>,
        scheduled_restart_tx: &Sender<ScheduledRestartBatch>,
        defer_via_channel: bool,
    ) -> Result<(), String> {
        let children_to_restart = self.restart_group_for(&child_name);

        tracing::info!(
            supervisor = %self.name,
            strategy = ?self.strategy,
            targets = ?children_to_restart,
            "📋 Initiating restart sequence for target group."
        );

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
                let mut term_receivers = Vec::new();
                let mut temp_to_remove = Vec::new();
                for target_name in children_to_restart.iter().rev() {
                    let ticket = self.allocate_restart_ticket();
                    if let Some(entry) = self.children.get_mut(target_name) {
                        if target_name != &child_name
                            && matches!(entry.phase, ChildPhase::Stopping | ChildPhase::Stopped)
                        {
                            continue;
                        }
                        if entry.restart_type == RestartType::Temporary {
                            temp_to_remove.push(target_name.clone());
                        } else {
                            let is_running =
                                target_name != &child_name && !*entry.term_rx.borrow();
                            entry.phase = ChildPhase::Restarting;
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

                for temp_name in temp_to_remove {
                    let terminate = temp_name != child_name;
                    if let Some(rx) = self.remove_child_slot(&temp_name, None, terminate) {
                        term_receivers.push(rx);
                    }
                }

                let targets: Vec<(String, u64)> = children_to_restart
                    .iter()
                    .filter_map(|name| {
                        self.children.get(name).and_then(|entry| {
                            if (name != &child_name
                                && matches!(
                                    entry.phase,
                                    ChildPhase::Stopping | ChildPhase::Stopped
                                ))
                                || entry.restart_type == RestartType::Temporary
                            {
                                None
                            } else {
                                Some((name.clone(), entry.restart_ticket))
                            }
                        })
                    })
                    .collect();

                if !defer_via_channel && backoff.is_zero() && term_receivers.is_empty() {
                    if let RestartOutcome::Escalate(msg) =
                        self.execute_restart(targets, Some(scheduled_restart_tx))
                    {
                        return Err(msg);
                    }
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
                Ok(())
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
                            && matches!(entry.phase, ChildPhase::Stopping | ChildPhase::Stopped)
                        {
                            continue;
                        }
                        if entry.restart_type == RestartType::Temporary {
                            temp_to_remove.push(target_name.clone());
                            continue;
                        }
                        let is_running = target_name != &child_name && !*entry.term_rx.borrow();
                        entry.restart_ticket = ticket;
                        let err = if target_name == &child_name {
                            factory_panic_err.clone().unwrap_or_else(|| {
                                SupervisorError::MaxRestartsExceeded(
                                    target_name.clone(),
                                    entry.restart_tracker.policy.max_restarts,
                                    entry.restart_tracker.policy.time_window,
                                )
                            })
                        } else {
                            SupervisorError::MaxRestartsExceeded(
                                target_name.clone(),
                                entry.restart_tracker.policy.max_restarts,
                                entry.restart_tracker.policy.time_window,
                            )
                        };
                        entry.phase = ChildPhase::Failed(err.clone());
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

                for temp_name in temp_to_remove {
                    let terminate = temp_name != child_name;
                    let _ = self.remove_child_slot(&temp_name, None, terminate);
                }
                Ok(())
            }
        }
    }

    /// Spawns fresh child instances for the requested target names if their restart ticket matches.
    fn execute_restart(
        &mut self,
        targets: Vec<(String, u64)>,
        retry_tx: Option<&Sender<ScheduledRestartBatch>>,
    ) -> RestartOutcome {
        let mut result = Ok(());
        for (target_name, expected_ticket) in targets {
            let should_restart = self.children.get(&target_name).is_some_and(|entry| {
                let phase_allows_restart = match &entry.phase {
                    ChildPhase::Stopping | ChildPhase::Stopped => false,
                    ChildPhase::Running | ChildPhase::Restarting => true,
                    ChildPhase::Failed(_) => matches!(
                        self.strategy,
                        SupervisionStrategy::OneForAll | SupervisionStrategy::RestForOne
                    ),
                };
                phase_allows_restart && entry.restart_ticket == expected_ticket
            });
            if !should_restart {
                continue;
            }
            let new_spawn_id = self.allocate_spawn_id();
            if let Some(entry) = self.children.get_mut(&target_name) {
                if matches!(entry.phase, ChildPhase::Failed(_)) {
                    entry.restart_tracker = RestartTracker::new(entry.restart_tracker.policy);
                }
                if let Some(handle) = entry.abort_handle.take() {
                    handle.abort();
                }
                entry.instance_id += 1;
                let new_inst = entry.instance_id;
                entry.spawn_id = new_spawn_id;
                entry.phase = ChildPhase::Running;

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
                        let panic_msg = panic_payload_message(&*panic_err);
                        tracing::error!(
                            supervisor = %self.name,
                            child = %target_name,
                            panic = %panic_msg,
                            "💥 Child factory panicked during restart!"
                        );
                        let err = SupervisorError::FactoryPanicked(target_name.clone(), panic_msg);
                        if let Some(tx) = retry_tx {
                            entry.phase = ChildPhase::Restarting;
                            entry.abort_handle = None;
                            let (term_tx, term_rx) = watch::channel(true);
                            drop(term_tx);
                            entry.term_rx = term_rx;
                            return match self.handle_child_failure(
                                target_name,
                                Some(err),
                                tx,
                                true,
                            ) {
                                Ok(()) => RestartOutcome::Done(Ok(())),
                                Err(msg) => RestartOutcome::Escalate(msg),
                            };
                        }

                        entry.phase = ChildPhase::Failed(err.clone());
                        entry.abort_handle = None;
                        let (term_tx, term_rx) = watch::channel(true);
                        drop(term_tx);
                        entry.term_rx = term_rx;
                        entry.slot_tx.send_replace(SlotState::Failed(err.clone()));
                        for waiter in entry.pending_resolutions.drain(..) {
                            let _ = waiter.send(Err(err.clone()));
                        }
                        result = Err(err);
                        continue;
                    }
                };
                let (term_tx, term_rx) = watch::channel(false);
                let name_for_task = target_name.clone();
                entry.abort_handle = Some(task_handle.abort_handle());
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
                    (name_for_task, new_spawn_id, res)
                });
            }
        }
        RestartOutcome::Done(result)
    }
}

enum RestartOutcome {
    Done(Result<(), SupervisorError>),
    Escalate(String),
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

    /// Sends a typed [`SupervisorCommand`] created by `make_cmd` and awaits its `oneshot` response.
    async fn send_request<T>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<T>) -> SupervisorCommand,
    ) -> Result<T, SupervisorError> {
        let cmd_sender = self.get_sender().await?;
        let (tx, rx) = oneshot::channel();
        cmd_sender
            .send(make_cmd(tx))
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;

        rx.await.map_err(|_| SupervisorError::SupervisorUnavailable)
    }

    /// Sends a fallible [`SupervisorCommand`] and flattens the reply's [`SupervisorError`] into the result.
    async fn send_result<T>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<Result<T, SupervisorError>>) -> SupervisorCommand,
    ) -> Result<T, SupervisorError> {
        self.send_request(make_cmd).await?
    }

    /// Starts and supervises a new child (leaf actor or nested [`SupervisorBuilder`]) from a [`ChildSpec`].
    pub async fn start_child(&self, spec: impl Into<ChildSpec>) -> Result<(), SupervisorError> {
        self.send_result(|respond_to| SupervisorCommand::StartChild {
            spec: spec.into(),
            respond_to,
        })
        .await
    }

    /// Manually restarts or recovers a child, resetting its failed status and backoff counters.
    /// Under [`SupervisionStrategy::OneForAll`] or [`SupervisionStrategy::RestForOne`], also restarts coupled siblings.
    pub async fn restart_child(
        &self,
        child_name: impl Into<String>,
    ) -> Result<(), SupervisorError> {
        self.send_result(|respond_to| SupervisorCommand::RestartChild {
            child_name: child_name.into(),
            respond_to,
        })
        .await
    }

    /// Gracefully stops a specific supervised child according to its [`ShutdownPolicy`].
    /// Even if the child has [`RestartType::Permanent`], an explicit stop will not trigger a restart.
    ///
    /// # Deadlock Warning
    ///
    /// Do **not** `.await` `stop_child(self_name)` from inside that same child's message handler:
    /// the supervisor waits for the child to finish its current handler before acknowledging
    /// `stop_child`, which will deadlock until its [`ShutdownPolicy`] timeout expires.
    pub async fn stop_child(&self, child_name: impl Into<String>) -> Result<(), SupervisorError> {
        self.send_result(|respond_to| SupervisorCommand::StopChild {
            child_name: child_name.into(),
            respond_to,
        })
        .await
    }

    /// Gracefully stops (if running) and completely removes a child's specification and state from this supervisor.
    ///
    /// # Deadlock Warning
    ///
    /// Do **not** `.await` `delete_child(self_name)` from inside that same child's message handler:
    /// the supervisor waits for the child to finish its current handler before acknowledging
    /// `delete_child`, which will deadlock until its [`ShutdownPolicy`] timeout expires.
    pub async fn delete_child(&self, child_name: impl Into<String>) -> Result<(), SupervisorError> {
        self.send_result(|respond_to| SupervisorCommand::DeleteChild {
            child_name: child_name.into(),
            respond_to,
        })
        .await
    }

    fn current_command_sender(&self) -> Option<Sender<SupervisorCommand>> {
        match &self.channel {
            SupervisorChannel::Strong { sender, .. } => {
                let guard = sender
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if guard.is_closed() {
                    None
                } else {
                    Some(guard.clone())
                }
            }
            SupervisorChannel::Weak { sender } => {
                let guard = sender
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.upgrade().filter(|tx| !tx.is_closed())
            }
        }
    }

    /// Requests graceful shutdown of this [`Supervisor`] without waiting for completion.
    ///
    /// Unlike [`SupervisorHandle::shutdown`], this method enqueues the shutdown command and
    /// returns immediately once accepted, making it safe to invoke from inside a supervised child
    /// actor's message handler (e.g. `ctx.supervisor().request_shutdown().await`) without
    /// deadlocking the child's own graceful shutdown sequence.
    pub async fn request_shutdown(&self) -> Result<(), SupervisorError> {
        let Some(current_tx) = self.current_command_sender() else {
            return Ok(());
        };

        let (tx, _rx) = oneshot::channel();
        let _ = current_tx
            .send(SupervisorCommand::Shutdown { respond_to: tx })
            .await;
        Ok(())
    }

    /// Gracefully shuts down all supervised children in reverse registration order and terminates this [`Supervisor`].
    ///
    /// # Deadlock Warning
    ///
    /// Do **not** `.await` `shutdown()` from inside a supervised child actor's message handler
    /// (for example via `ctx.supervisor().shutdown().await`): the supervisor waits for all
    /// supervised children to finish processing their current messages before acknowledging
    /// `shutdown()`, which will deadlock until the child's [`ShutdownPolicy`] timeout expires.
    /// Use [`SupervisorHandle::request_shutdown`] when triggering shutdown from within a child actor.
    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        let Some(current_tx) = self.current_command_sender() else {
            return Ok(());
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
        self.send_result(|respond_to| SupervisorCommand::ResolveChildSlot {
            child_name: child_name.to_string(),
            respond_to,
        })
        .await
    }

    /// Captures a point-in-time [`SupervisorSnapshot`] with the default [`DEFAULT_SNAPSHOT_TIMEOUT`].
    ///
    /// Returns [`SupervisorError::Timeout`] if the supervisor is busy (for example during a heavy
    /// shutdown or restart wave) and does not answer within the built-in deadline. To specify a
    /// custom timeout, use [`Self::snapshot_timeout`].
    pub async fn snapshot(&self) -> Result<SupervisorSnapshot, SupervisorError> {
        self.snapshot_timeout(DEFAULT_SNAPSHOT_TIMEOUT).await
    }

    /// Captures a point-in-time [`SupervisorSnapshot`] bounded by an explicit `timeout`.
    ///
    /// Wraps handle rebinding, command dispatch, and snapshot collection in `timeout`, returning
    /// [`SupervisorError::Timeout`] rather than blocking a health probe if the supervisor is stalled
    /// inside shutdown or cascading restarts.
    pub async fn snapshot_timeout(
        &self,
        timeout: Duration,
    ) -> Result<SupervisorSnapshot, SupervisorError> {
        tokio::time::timeout(timeout, self.snapshot_untimed())
            .await
            .map_err(|_| SupervisorError::Timeout(timeout))?
    }

    /// Requests a [`SupervisorSnapshot`] from this supervisor without any deadline of its own.
    async fn snapshot_untimed(&self) -> Result<SupervisorSnapshot, SupervisorError> {
        self.send_request(|respond_to| SupervisorCommand::Snapshot { respond_to })
            .await
    }

    /// Captures a point-in-time [`SupervisorTreeSnapshot`] of this supervisor and all nested child
    /// supervisors with the default [`DEFAULT_SNAPSHOT_TIMEOUT`].
    ///
    /// See [`Self::snapshot_tree_timeout`] for semantics and failure behavior.
    pub async fn snapshot_tree(&self) -> Result<SupervisorTreeSnapshot, SupervisorError> {
        self.snapshot_tree_timeout(DEFAULT_SNAPSHOT_TIMEOUT).await
    }

    /// Captures a point-in-time [`SupervisorTreeSnapshot`] of this supervisor and all nested child
    /// supervisors, bounded by a single overall `timeout`.
    ///
    /// - The `timeout` is one shared deadline for the **whole** tree, not a per-level budget.
    /// - Child supervisors are queried **concurrently**, so one stalled subtree cannot starve its siblings.
    /// - Fails with [`SupervisorError::Timeout`] only if *this* supervisor cannot be inspected in time.
    ///   A child supervisor that is restarting, stopped, failed, or too slow is reported as an
    ///   `Err` in its [`SubtreeSnapshot::result`] while the rest of the tree is still returned.
    /// - Descends only through children registered as [`Supervisor`]s (`ChildSnapshot::is_supervisor`).
    /// - The recursion runs on the caller side, never inside a supervisor's event loop, so a
    ///   supervisor is never blocked waiting on its own children.
    pub async fn snapshot_tree_timeout(
        &self,
        timeout: Duration,
    ) -> Result<SupervisorTreeSnapshot, SupervisorError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86400 * 365));
        self.collect_tree(deadline, timeout).await
    }

    /// Recursively collects the tree below this supervisor, honoring the shared `deadline`.
    fn collect_tree(
        &self,
        deadline: tokio::time::Instant,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<SupervisorTreeSnapshot, SupervisorError>> + Send + '_>>
    {
        Box::pin(async move {
            let snapshot = tokio::time::timeout_at(deadline, self.snapshot_untimed())
                .await
                .map_err(|_| SupervisorError::Timeout(timeout))??;

            let supervisor_children: Vec<&ChildSnapshot> = snapshot
                .children
                .iter()
                .filter(|c| c.is_supervisor)
                .collect();
            let mut slots: Vec<Option<SubtreeSnapshot>> =
                (0..supervisor_children.len()).map(|_| None).collect();
            let mut pending: JoinSet<(usize, SubtreeSnapshot)> = JoinSet::new();

            for (idx, child) in supervisor_children.iter().enumerate() {
                let child_name = child.name.clone();
                match &child.status {
                    ChildStatus::Running { .. } => {
                        let parent = self.clone();
                        pending.spawn(async move {
                            let result = tokio::time::timeout_at(
                                deadline,
                                Self::collect_child_tree(&parent, &child_name, deadline, timeout),
                            )
                            .await
                            .unwrap_or(Err(SupervisorError::Timeout(timeout)));
                            (idx, SubtreeSnapshot { child_name, result })
                        });
                    }
                    other => {
                        let reason = match other {
                            ChildStatus::Failed(err) => err.clone(),
                            ChildStatus::Stopped => {
                                SupervisorError::ChildNotFound(child_name.clone())
                            }
                            _ => SupervisorError::SupervisorUnavailable,
                        };
                        if let Some(slot) = slots.get_mut(idx) {
                            *slot = Some(SubtreeSnapshot {
                                child_name,
                                result: Err(reason),
                            });
                        }
                    }
                }
            }

            while let Some(joined) = pending.join_next().await {
                if let Ok((idx, subtree)) = joined
                    && let Some(slot) = slots.get_mut(idx)
                {
                    *slot = Some(subtree);
                }
            }

            let subtrees = supervisor_children
                .iter()
                .zip(slots)
                .map(|(child, slot)| {
                    slot.unwrap_or_else(|| SubtreeSnapshot {
                        child_name: child.name.clone(),
                        result: Err(SupervisorError::SupervisorUnavailable),
                    })
                })
                .collect();

            Ok(SupervisorTreeSnapshot { snapshot, subtrees })
        })
    }

    /// Resolves the child supervisor `child_name` below `parent` and collects its subtree.
    async fn collect_child_tree(
        parent: &SupervisorHandle,
        child_name: &str,
        deadline: tokio::time::Instant,
        timeout: Duration,
    ) -> Result<SupervisorTreeSnapshot, SupervisorError> {
        let child = parent.get_supervisor(child_name).await?;
        child.collect_tree(deadline, timeout).await
    }

    /// Returns a read-only [`ChildStatusWatcher`] for `child_name` backed by the supervisor's
    /// existing per-child `watch` channel.
    ///
    /// Unlike internal data-plane channel resolution, `watch_child_status` succeeds even when the
    /// child is currently [`ChildStatus::Stopped`] or [`ChildStatus::Failed`], returning
    /// [`SupervisorError::ChildNotFound`] only if no slot with `child_name` is registered.
    pub async fn watch_child_status(
        &self,
        child_name: impl Into<String>,
    ) -> Result<ChildStatusWatcher, SupervisorError> {
        let child_name = child_name.into();
        let rx = self
            .send_result(|respond_to| SupervisorCommand::ObserveChildSlot {
                child_name: child_name.clone(),
                respond_to,
            })
            .await?;
        Ok(ChildStatusWatcher { child_name, rx })
    }

    /// Waits for a supervised child slot to settle out of [`SlotState::Restarting`]
    /// and downcasts its active endpoint to `E`.
    async fn resolve_running_endpoint<E: Clone + Send + Sync + 'static>(
        &self,
        child_name: String,
    ) -> Result<(String, E, watch::Receiver<SlotState>), SupervisorError> {
        let mut slot_rx = self.resolve_child_slot(&child_name).await?;
        let state = slot_rx
            .wait_for(|s| !matches!(s, SlotState::Restarting { .. }))
            .await
            .map_err(|_| SupervisorError::SupervisorUnavailable)?;
        let endpoint = match &*state {
            SlotState::Running { endpoint, .. } => {
                if let Some(ep) = endpoint.downcast_ref::<E>() {
                    ep.clone()
                } else {
                    return Err(SupervisorError::ChildTypeMismatch(child_name));
                }
            }
            SlotState::Failed(err) => return Err(err.clone()),
            SlotState::Stopped => return Err(SupervisorError::ChildNotFound(child_name)),
            SlotState::Restarting { .. } => unreachable!(),
        };
        drop(state);

        Ok((child_name, endpoint, slot_rx))
    }

    /// Retrieves a direct [`ResourceClient`] data-plane handle for a supervised [`super::generic_actor::ResourceActor`].
    pub async fn get_resource_client<
        T: Clone + Send + Sync + 'static,
        E: Clone + Send + Sync + 'static,
    >(
        &self,
        child_name: impl Into<String>,
    ) -> Result<ResourceClient<T, E>, SupervisorError> {
        let (name, endpoint, slot_rx) = self
            .resolve_running_endpoint::<ResourceEndpoint<T, E>>(child_name.into())
            .await?;
        Ok(ResourceClient::new(name, endpoint, slot_rx, self.clone()))
    }

    /// Retrieves a direct [`ActorClient`] data-plane handle for a supervised [`StatefulActor`].
    pub async fn get_actor_client<A: StatefulActor>(
        &self,
        child_name: impl Into<String>,
    ) -> Result<ActorClient<A>, SupervisorError> {
        let (name, sender, slot_rx) = self
            .resolve_running_endpoint::<Sender<ActorEnvelope<A>>>(child_name.into())
            .await?;
        Ok(ActorClient::new(name, sender, slot_rx, self.clone()))
    }

    /// Resolves a supervised child [`Supervisor`] by name and returns a resilient [`SupervisorHandle`].
    pub async fn get_supervisor(
        &self,
        child_name: impl Into<String>,
    ) -> Result<SupervisorHandle, SupervisorError> {
        let (name, sender, slot_rx) = self
            .resolve_running_endpoint::<Sender<SupervisorCommand>>(child_name.into())
            .await?;
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
        self.send_result(|respond_to| SupervisorCommand::ResolveChildChannel {
            child_name: child_name.to_string(),
            respond_to,
        })
        .await
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
        self.send_result(|respond_to| SupervisorCommand::TriggerChildCrash {
            child_name: child_name.to_string(),
            respond_to,
        })
        .await
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

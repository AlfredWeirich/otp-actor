//! # Local Runtime Telemetry, Operational Health & Prometheus Exposition
//!
//! Provides lock-free actor performance counters ([`ActorMetrics`]), aggregated node-level
//! health and status snapshots ([`NodeHealth`], [`NodeStatus`]), a local monitor facade
//! ([`LocalNodeMonitor`]), and a pure OpenMetrics/Prometheus text formatter
//! ([`format_prometheus_metrics`]) with zero HTTP/JSON dependencies.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::generic_actor::CacheState;
use super::model::{DEFAULT_SNAPSHOT_TIMEOUT, RestartType, SupervisionStrategy, SupervisorError};
use super::stateful_actor::QueueStats;
use super::supervisor::{
    ChildSnapshot, ChildStatus, SupervisorHandle, SupervisorTreeSnapshot,
};

// ============================================================================
// 1. Lock-Free Actor Metrics Collector
// ============================================================================

#[derive(Debug, Default)]
struct ActorMetricsInner {
    in_flight: AtomicUsize,
    calls_total: AtomicU64,
    casts_total: AtomicU64,
    handler_errors_total: AtomicU64,
    timeouts_total: AtomicU64,
    panics_total: AtomicU64,
    last_latency_us: AtomicU64,
    ema_latency_us: AtomicU64,
    has_latency_sample: AtomicBool,
}

/// Lock-free, thread-safe performance and error counter handle shared between a
/// [`super::stateful_actor::StatefulActor`] runner (across restarts) and its supervisor slot.
#[derive(Debug, Clone, Default)]
pub struct ActorMetrics {
    inner: Arc<ActorMetricsInner>,
}

/// Drop guard tracking active `in_flight` handler executions (decrements safely on exit,
/// timeout cancellation, or panic unwind).
pub(crate) struct InFlightGuard<'a> {
    inner: &'a ActorMetricsInner,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.inner.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Drop guard detecting task panics during an actor task lifecycle.
pub(crate) struct RunnerPanicGuard {
    metrics: ActorMetrics,
}

impl RunnerPanicGuard {
    pub(crate) fn new(metrics: ActorMetrics) -> Self {
        Self { metrics }
    }
}

impl Drop for RunnerPanicGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.metrics.record_panic();
        }
    }
}

impl ActorMetrics {
    /// Creates a new zero-initialized [`ActorMetrics`] instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the start of an in-flight `call` or `cast` execution.
    pub(crate) fn enter_in_flight(&self) -> InFlightGuard<'_> {
        self.inner.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlightGuard { inner: &self.inner }
    }

    /// Records a completed `call` execution with its latency and outcome flags.
    pub(crate) fn record_call(&self, duration: Duration, handler_error: bool, timed_out: bool) {
        self.inner.calls_total.fetch_add(1, Ordering::Relaxed);
        self.record_outcome(duration, handler_error, timed_out);
    }

    /// Records a completed `cast` execution with its latency and outcome flags.
    pub(crate) fn record_cast(&self, duration: Duration, handler_error: bool, timed_out: bool) {
        self.inner.casts_total.fetch_add(1, Ordering::Relaxed);
        self.record_outcome(duration, handler_error, timed_out);
    }

    /// Records an unwinding panic observed inside the actor task.
    pub(crate) fn record_panic(&self) {
        self.inner.panics_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_outcome(&self, duration: Duration, handler_error: bool, timed_out: bool) {
        if handler_error {
            self.inner
                .handler_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if timed_out {
            self.inner.timeouts_total.fetch_add(1, Ordering::Relaxed);
        }

        let sample_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.inner
            .last_latency_us
            .store(sample_us, Ordering::Relaxed);

        if !self.inner.has_latency_sample.swap(true, Ordering::Relaxed) {
            self.inner
                .ema_latency_us
                .store(sample_us, Ordering::Relaxed);
        } else {
            // Exponential moving average with alpha = 0.25 ((prev * 3 + sample) / 4).
            let mut prev = self.inner.ema_latency_us.load(Ordering::Relaxed);
            loop {
                let next = prev.saturating_mul(3).saturating_add(sample_us) / 4;
                match self.inner.ema_latency_us.compare_exchange_weak(
                    prev,
                    next,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => prev = actual,
                }
            }
        }
    }

    /// Captures a lock-free point-in-time snapshot of all atomic actor counters.
    #[must_use]
    pub fn snapshot(&self) -> ActorMetricsSnapshot {
        ActorMetricsSnapshot {
            in_flight: self.inner.in_flight.load(Ordering::Relaxed),
            calls_total: self.inner.calls_total.load(Ordering::Relaxed),
            casts_total: self.inner.casts_total.load(Ordering::Relaxed),
            handler_errors_total: self.inner.handler_errors_total.load(Ordering::Relaxed),
            timeouts_total: self.inner.timeouts_total.load(Ordering::Relaxed),
            panics_total: self.inner.panics_total.load(Ordering::Relaxed),
            last_latency_us: self.inner.last_latency_us.load(Ordering::Relaxed),
            ema_latency_us: self.inner.ema_latency_us.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of an actor's execution throughput, errors, and latencies.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorMetricsSnapshot {
    /// Number of `call`/`cast` handlers currently executing on this actor.
    pub in_flight: usize,
    /// Total synchronous `call` messages processed.
    pub calls_total: u64,
    /// Total fire-and-forget `cast` messages processed.
    pub casts_total: u64,
    /// Total domain errors (`Err(A::Error)`) returned by `handle` / `handle_cast`.
    pub handler_errors_total: u64,
    /// Total handler executions aborted due to configured `handler_timeout`.
    pub timeouts_total: u64,
    /// Total unwinding panics caught across actor incarnations.
    pub panics_total: u64,
    /// Duration of the most recently completed handler invocation in microseconds.
    pub last_latency_us: u64,
    /// Exponential moving average handler latency in microseconds (`alpha = 0.25`).
    pub ema_latency_us: u64,
}

// ============================================================================
// 2. Operational Health & Node Status Snapshots
// ============================================================================

/// High-level operational health classification of a monitored node or supervision tree.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub enum HealthState {
    /// Supervisor/node is reachable and no permanent failures or unreachable subtrees were observed.
    Healthy,
    /// Supervisor/node is alive, but at least one child is restarting, permanently failed, or unreachable.
    Degraded,
    /// Node is actively draining in-flight requests prior to shutdown.
    Draining,
    /// Supervisor or node is no longer available.
    Unavailable,
}

impl HealthState {
    /// Returns a stable lowercase string identifier (`"ok"`, `"degraded"`, `"draining"`, `"unavailable"`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "ok",
            Self::Degraded => "degraded",
            Self::Draining => "draining",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Fast liveness summary returned by [`LocalNodeMonitor::health`] and [`LocalNodeMonitor::health_checked`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeHealth {
    /// `true` when the monitored supervisor/node is alive and accepting work.
    pub alive: bool,
    /// High-level health state.
    pub state: HealthState,
    /// Elapsed uptime in whole seconds since the configured start timestamp (default: supervisor start).
    pub uptime_seconds: u64,
}

/// Query options controlling [`LocalNodeMonitor::status`] and [`LocalNodeMonitor::prometheus_metrics`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct StatusOptions {
    /// Overall deadline applied when querying the supervision tree.
    pub timeout: Duration,
    /// Whether to include the hierarchical [`SupervisorStatusNode`] in [`NodeStatus::supervision_tree`].
    pub include_tree: bool,
    /// Whether to include per-child queue, metrics, and cache details in [`SupervisorStatusNode`].
    pub include_child_details: bool,
}

impl Default for StatusOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_SNAPSHOT_TIMEOUT,
            include_tree: true,
            include_child_details: true,
        }
    }
}

impl StatusOptions {
    /// Creates default [`StatusOptions`] (`timeout = DEFAULT_SNAPSHOT_TIMEOUT`, full tree & details).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the deadline for collecting the supervision tree snapshot.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Controls whether the full [`SupervisorStatusNode`] tree is attached to [`NodeStatus`].
    ///
    /// Summary counters in [`NodeStatus::summary`] are always computed.
    #[must_use]
    pub fn include_tree(mut self, include: bool) -> Self {
        self.include_tree = include;
        self
    }

    /// Controls whether per-child queue, actor metrics, and cache snapshots are attached.
    #[must_use]
    pub fn include_child_details(mut self, include: bool) -> Self {
        self.include_child_details = include;
        self
    }
}

/// Distinguishes leaf actors from nested child supervisors in [`ChildStatusEntry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub enum ChildKind {
    Actor,
    Supervisor,
}

impl ChildKind {
    /// Returns `"actor"` or `"supervisor"`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Actor => "actor",
            Self::Supervisor => "supervisor",
        }
    }
}

/// Serializable lifecycle state of a supervised child in [`ChildStatusEntry`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub enum ChildLifecycle {
    Running,
    Restarting,
    Stopped,
    Failed(String),
}

impl ChildLifecycle {
    /// Returns a stable status label (`"running"`, `"restarting"`, `"stopped"`, `"failed"`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Restarting => "restarting",
            Self::Stopped => "stopped",
            Self::Failed(_) => "failed",
        }
    }

    fn metric_code(&self) -> u8 {
        match self {
            Self::Running => 0,
            Self::Restarting => 1,
            Self::Stopped => 2,
            Self::Failed(_) => 3,
        }
    }
}

impl From<&ChildStatus> for ChildLifecycle {
    fn from(status: &ChildStatus) -> Self {
        match status {
            ChildStatus::Running { .. } => Self::Running,
            ChildStatus::Restarting { .. } => Self::Restarting,
            ChildStatus::Stopped => Self::Stopped,
            ChildStatus::Failed(err) => Self::Failed(err.to_string()),
        }
    }
}

/// Aggregated operational counters across an entire supervision tree.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeSummary {
    /// Total inspected supervisors (including the root).
    pub total_supervisors: usize,
    /// Total registered leaf child slots across all inspected levels.
    pub total_actors: usize,
    /// Total child slots (actors and sub-supervisors) currently in `Running`.
    pub running_children: usize,
    /// Total child slots currently undergoing a restart/backoff wave.
    pub restarting_children: usize,
    /// Total child slots currently in `Stopped`.
    pub stopped_children: usize,
    /// Total child slots currently in `Failed`.
    pub failed_children: usize,
    /// Number of nested sub-supervisors that could not be inspected before the deadline.
    pub unavailable_subtrees: usize,
    /// Cumulative number of child restarts executed across all inspected levels.
    pub total_restarts: u64,
    /// Total messages currently queued across all inspected bounded mailboxes.
    pub total_queued_messages: usize,
    /// Number of inspected mailboxes currently at full capacity (`available == 0`).
    pub full_mailboxes: usize,
    /// Cumulative `call` messages handled across all inspected actors.
    pub total_calls: u64,
    /// Cumulative `cast` messages handled across all inspected actors.
    pub total_casts: u64,
    /// Cumulative domain handler errors returned across all inspected actors.
    pub total_handler_errors: u64,
    /// Cumulative handler timeouts observed across all inspected actors.
    pub total_timeouts: u64,
    /// Cumulative unwinding panics caught across all inspected actors.
    pub total_panics: u64,
    /// Total handlers currently executing concurrently across all inspected actors.
    pub in_flight_calls: usize,
}

/// Operational snapshot for a single supervised child slot inside [`SupervisorStatusNode`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct ChildStatusEntry {
    pub name: String,
    pub kind: ChildKind,
    pub status: ChildLifecycle,
    pub instance_id: usize,
    pub instance_uptime_seconds: u64,
    pub restart_type: RestartType,
    pub restarts_in_window: usize,
    pub consecutive_failures: usize,
    pub restarts_total: u64,
    pub queue: Option<QueueStats>,
    pub metrics: Option<ActorMetricsSnapshot>,
    pub cache_state: Option<CacheState>,
}

/// Operational snapshot of one supervisor level and its nested child supervisors.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct SupervisorStatusNode {
    pub name: String,
    pub instance_id: usize,
    pub uptime_seconds: u64,
    pub strategy: SupervisionStrategy,
    pub children: Vec<ChildStatusEntry>,
    pub subtrees: Vec<SubtreeStatusEntry>,
}

/// Outcome of inspecting a nested child supervisor inside [`SupervisorStatusNode`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct SubtreeStatusEntry {
    pub child_name: String,
    pub result: Result<SupervisorStatusNode, String>,
}

/// Operational snapshot of an endpoint registered for external/remote dispatch.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct RegisteredEndpointStatus {
    /// Service domain name registered in the node registry.
    pub service_name: String,
    /// Actor name registered within `service_name`.
    pub actor_name: String,
    /// Point-in-time bounded mailbox queue snapshot, if provided by the endpoint.
    pub queue: Option<QueueStats>,
}

/// Transport and dispatch counters collected by a remote `ActorNode` host.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct RemoteMetricsSnapshot {
    /// Number of `(service_name, actor_name)` endpoints currently registered on the node.
    pub registered_endpoints: usize,
    /// Number of gRPC `Call` / `Cast` requests currently executing on the server.
    pub grpc_in_flight: usize,
    /// Total gRPC `Call` requests received by the server.
    pub grpc_calls_total: u64,
    /// Total gRPC `Cast` requests received by the server.
    pub grpc_casts_total: u64,
    /// Total requests rejected by the configured `Authorizer`.
    pub auth_rejected_total: u64,
    /// Total requests targeting an unknown `service_name`.
    pub service_not_found_total: u64,
    /// Total requests targeting an unknown `actor_name` in a known service.
    pub actor_not_found_total: u64,
    /// Total requests rejected due to unsupported wire `protocol_version`.
    pub incompatible_protocol_total: u64,
    /// Total framework-level errors returned by remote dispatch.
    pub framework_errors_total: u64,
}

/// Complete point-in-time operational snapshot of a local or remote actor node.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeStatus {
    pub health: NodeHealth,
    pub timestamp_unix: u64,
    pub summary: NodeSummary,
    pub supervision_tree: Option<SupervisorStatusNode>,
    /// Remote gRPC transport counters (populated when queried from a remote `ActorNode`).
    pub remote: Option<RemoteMetricsSnapshot>,
    /// Endpoints registered in the node's `ActorNodeRegistry` (populated for remote nodes).
    pub registered_endpoints: Vec<RegisteredEndpointStatus>,
}

// ============================================================================
// 3. Local Node Monitor
// ============================================================================

/// Non-intrusive local operational monitor for an `otp-actor` supervision tree.
///
/// Separates fast lock-free liveness checks ([`Self::health`]) from bounded full-tree
/// status collection ([`Self::status`]) and Prometheus text rendering ([`Self::prometheus_metrics`]).
#[derive(Clone)]
pub struct LocalNodeMonitor {
    supervisor: SupervisorHandle,
    started_at: Instant,
}

impl std::fmt::Debug for LocalNodeMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalNodeMonitor")
            .field("supervisor", &self.supervisor.name())
            .field("uptime_seconds", &self.uptime().as_secs())
            .finish()
    }
}

impl LocalNodeMonitor {
    /// Creates a [`LocalNodeMonitor`] anchored to `supervisor`'s start timestamp.
    #[must_use]
    pub fn new(supervisor: SupervisorHandle) -> Self {
        let started_at = supervisor.started_at();
        Self {
            supervisor,
            started_at,
        }
    }

    /// Overrides the reference start timestamp used for `uptime_seconds`
    /// (for example with the host application's startup `Instant`).
    #[must_use]
    pub fn with_started_at(mut self, started_at: Instant) -> Self {
        self.started_at = started_at;
        self
    }

    /// Returns a reference to the monitored [`SupervisorHandle`].
    #[must_use]
    pub fn supervisor(&self) -> &SupervisorHandle {
        &self.supervisor
    }

    /// Elapsed duration since `started_at`.
    #[must_use]
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Performs a synchronous, zero-hop liveness check on the root supervisor handle.
    ///
    /// Checks whether the supervisor command channel is still open without sending a message
    /// or walking the supervision tree.
    #[must_use]
    pub fn health(&self) -> NodeHealth {
        let alive = self.supervisor.is_alive();
        NodeHealth {
            alive,
            state: if alive {
                HealthState::Healthy
            } else {
                HealthState::Unavailable
            },
            uptime_seconds: self.uptime().as_secs(),
        }
    }

    /// Performs a bounded readiness/health check across the supervision tree within `timeout`.
    ///
    /// Reports [`HealthState::Healthy`] when the root is reachable and all inspected levels have
    /// no failed, restarting, or unreachable children; [`HealthState::Degraded`] when the root is
    /// alive but any child/subtree is failed, restarting, or unreachable; and
    /// [`HealthState::Unavailable`] when the root supervisor cannot be reached.
    #[tracing::instrument(skip(self))]
    pub async fn health_checked(&self, timeout: Duration) -> NodeHealth {
        let uptime_seconds = self.uptime().as_secs();
        match self.supervisor.snapshot_tree_timeout(timeout).await {
            Ok(tree) => {
                let degraded = tree.any_failed_deep()
                    || tree.any_restarting_deep()
                    || tree.unavailable_count() > 0;
                NodeHealth {
                    alive: true,
                    state: if degraded {
                        HealthState::Degraded
                    } else {
                        HealthState::Healthy
                    },
                    uptime_seconds,
                }
            }
            Err(_) => NodeHealth {
                alive: false,
                state: HealthState::Unavailable,
                uptime_seconds,
            },
        }
    }

    /// Collects a full operational [`NodeStatus`] snapshot bounded by `opts.timeout`.
    #[tracing::instrument(skip(self), err)]
    pub async fn status(&self, opts: StatusOptions) -> Result<NodeStatus, SupervisorError> {
        let tree = self.supervisor.snapshot_tree_timeout(opts.timeout).await?;
        let timestamp_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut summary = NodeSummary::default();
        let tree_node = build_status_node(&tree, &opts, &mut summary);

        let degraded = summary.failed_children > 0
            || summary.restarting_children > 0
            || summary.unavailable_subtrees > 0;

        let health = NodeHealth {
            alive: true,
            state: if degraded {
                HealthState::Degraded
            } else {
                HealthState::Healthy
            },
            uptime_seconds: self.uptime().as_secs(),
        };

        Ok(NodeStatus {
            health,
            timestamp_unix,
            summary,
            supervision_tree: if opts.include_tree {
                Some(tree_node)
            } else {
                None
            },
            remote: None,
            registered_endpoints: Vec::new(),
        })
    }

    /// Collects [`NodeStatus`] using [`StatusOptions::default()`].
    pub async fn status_default(&self) -> Result<NodeStatus, SupervisorError> {
        self.status(StatusOptions::default()).await
    }

    /// Collects [`NodeStatus`] and formats it in Prometheus / OpenMetrics text exposition format.
    #[tracing::instrument(skip(self), err)]
    pub async fn prometheus_metrics(&self, opts: StatusOptions) -> Result<String, SupervisorError> {
        let status = self.status(opts).await?;
        Ok(format_prometheus_metrics(&status))
    }
}

fn build_status_node(
    tree: &SupervisorTreeSnapshot,
    opts: &StatusOptions,
    summary: &mut NodeSummary,
) -> SupervisorStatusNode {
    summary.total_supervisors += 1;

    let mut children = Vec::with_capacity(tree.snapshot.children.len());
    for child in &tree.snapshot.children {
        if !child.is_supervisor {
            summary.total_actors += 1;
        }

        match &child.status {
            ChildStatus::Running { .. } => summary.running_children += 1,
            ChildStatus::Restarting { .. } => summary.restarting_children += 1,
            ChildStatus::Stopped => summary.stopped_children += 1,
            ChildStatus::Failed(_) => summary.failed_children += 1,
        }

        summary.total_restarts = summary
            .total_restarts
            .saturating_add(child.restarts_total);

        if let Some(q) = &child.queue_stats {
            summary.total_queued_messages = summary.total_queued_messages.saturating_add(q.queued);
            if q.is_full() {
                summary.full_mailboxes += 1;
            }
        }

        if let Some(m) = &child.actor_metrics {
            summary.total_calls = summary.total_calls.saturating_add(m.calls_total);
            summary.total_casts = summary.total_casts.saturating_add(m.casts_total);
            summary.total_handler_errors = summary
                .total_handler_errors
                .saturating_add(m.handler_errors_total);
            summary.total_timeouts = summary.total_timeouts.saturating_add(m.timeouts_total);
            summary.total_panics = summary.total_panics.saturating_add(m.panics_total);
            summary.in_flight_calls = summary.in_flight_calls.saturating_add(m.in_flight);
        }

        if opts.include_tree {
            children.push(child_entry_from_snapshot(child, opts.include_child_details));
        }
    }

    let mut subtrees = Vec::with_capacity(tree.subtrees.len());
    for sub in &tree.subtrees {
        match &sub.result {
            Ok(sub_tree) => {
                let node = build_status_node(sub_tree, opts, summary);
                if opts.include_tree {
                    subtrees.push(SubtreeStatusEntry {
                        child_name: sub.child_name.clone(),
                        result: Ok(node),
                    });
                }
            }
            Err(err) => {
                summary.unavailable_subtrees += 1;
                if opts.include_tree {
                    subtrees.push(SubtreeStatusEntry {
                        child_name: sub.child_name.clone(),
                        result: Err(err.to_string()),
                    });
                }
            }
        }
    }

    SupervisorStatusNode {
        name: tree.snapshot.name.clone(),
        instance_id: tree.snapshot.instance_id,
        uptime_seconds: tree.snapshot.uptime.as_secs(),
        strategy: tree.snapshot.strategy,
        children,
        subtrees,
    }
}

fn child_entry_from_snapshot(child: &ChildSnapshot, include_details: bool) -> ChildStatusEntry {
    ChildStatusEntry {
        name: child.name.clone(),
        kind: if child.is_supervisor {
            ChildKind::Supervisor
        } else {
            ChildKind::Actor
        },
        status: ChildLifecycle::from(&child.status),
        instance_id: child.instance_id,
        instance_uptime_seconds: child.instance_uptime.as_secs(),
        restart_type: child.restart_type,
        restarts_in_window: child.restarts_in_window,
        consecutive_failures: child.consecutive_failures,
        restarts_total: child.restarts_total,
        queue: if include_details {
            child.queue_stats
        } else {
            None
        },
        metrics: if include_details {
            child.actor_metrics
        } else {
            None
        },
        cache_state: if include_details {
            child.cache_state
        } else {
            None
        },
    }
}

// ============================================================================
// 4. Prometheus / OpenMetrics Text Exposition Formatter
// ============================================================================

fn escape_prom_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c if c.is_control() => out.push('?'),
            c => out.push(c),
        }
    }
    out
}

fn cache_state_code(state: &CacheState) -> u8 {
    match state {
        CacheState::Empty => 0,
        CacheState::Fresh { .. } => 1,
        CacheState::RefreshDue { .. } => 2,
        CacheState::StaleUsable { .. } => 3,
        CacheState::Expired { .. } => 4,
        CacheState::Invalidated => 5,
    }
}

struct FlatChildMetric<'a> {
    escaped_path: Arc<str>,
    escaped_child: String,
    child: &'a ChildStatusEntry,
}

fn collect_flat_children<'a>(
    node: &'a SupervisorStatusNode,
    escaped_prefix: &str,
    out: &mut Vec<FlatChildMetric<'a>>,
) {
    let escaped_name = escape_prom_label(&node.name);
    let escaped_path: Arc<str> = if escaped_prefix.is_empty() {
        Arc::from(escaped_name)
    } else {
        Arc::from(format!("{escaped_prefix}/{escaped_name}"))
    };

    for child in &node.children {
        out.push(FlatChildMetric {
            escaped_path: Arc::clone(&escaped_path),
            escaped_child: escape_prom_label(&child.name),
            child,
        });
    }

    for sub in &node.subtrees {
        if let Ok(sub_node) = &sub.result {
            collect_flat_children(sub_node, &escaped_path, out);
        }
    }
}

/// Formats a [`NodeStatus`] snapshot into Prometheus / OpenMetrics text exposition format
/// (`text/plain; version=0.0.4; charset=utf-8`).
#[must_use]
pub fn format_prometheus_metrics(status: &NodeStatus) -> String {
    let mut out = String::with_capacity(4096);

    let _ = writeln!(
        out,
        "# HELP otp_actor_node_alive Whether the monitored supervision node is alive (1=alive, 0=unavailable)."
    );
    let _ = writeln!(out, "# TYPE otp_actor_node_alive gauge");
    let _ = writeln!(
        out,
        "otp_actor_node_alive {}",
        if status.health.alive { 1 } else { 0 }
    );
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_node_uptime_seconds Elapsed uptime of the monitored node or supervisor in seconds."
    );
    let _ = writeln!(out, "# TYPE otp_actor_node_uptime_seconds gauge");
    let _ = writeln!(
        out,
        "otp_actor_node_uptime_seconds {}",
        status.health.uptime_seconds
    );
    out.push('\n');

    let s = &status.summary;
    let _ = writeln!(
        out,
        "# HELP otp_actor_summary_supervisors Number of inspected supervisors in the tree."
    );
    let _ = writeln!(out, "# TYPE otp_actor_summary_supervisors gauge");
    let _ = writeln!(out, "otp_actor_summary_supervisors {}", s.total_supervisors);
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_summary_actors Number of registered leaf actor slots in the tree."
    );
    let _ = writeln!(out, "# TYPE otp_actor_summary_actors gauge");
    let _ = writeln!(out, "otp_actor_summary_actors {}", s.total_actors);
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_summary_children Child counts across the supervision tree by lifecycle state."
    );
    let _ = writeln!(out, "# TYPE otp_actor_summary_children gauge");
    let _ = writeln!(
        out,
        "otp_actor_summary_children{{state=\"running\"}} {}",
        s.running_children
    );
    let _ = writeln!(
        out,
        "otp_actor_summary_children{{state=\"restarting\"}} {}",
        s.restarting_children
    );
    let _ = writeln!(
        out,
        "otp_actor_summary_children{{state=\"stopped\"}} {}",
        s.stopped_children
    );
    let _ = writeln!(
        out,
        "otp_actor_summary_children{{state=\"failed\"}} {}",
        s.failed_children
    );
    let _ = writeln!(
        out,
        "otp_actor_summary_children{{state=\"unavailable_subtree\"}} {}",
        s.unavailable_subtrees
    );
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_summary_restarts_total Cumulative restarts executed across all inspected children."
    );
    let _ = writeln!(out, "# TYPE otp_actor_summary_restarts_total counter");
    let _ = writeln!(out, "otp_actor_summary_restarts_total {}", s.total_restarts);
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_summary_in_flight_calls Currently executing handlers across all inspected actors."
    );
    let _ = writeln!(out, "# TYPE otp_actor_summary_in_flight_calls gauge");
    let _ = writeln!(
        out,
        "otp_actor_summary_in_flight_calls {}",
        s.in_flight_calls
    );
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_summary_queued_messages Total messages currently queued across inspected bounded mailboxes."
    );
    let _ = writeln!(out, "# TYPE otp_actor_summary_queued_messages gauge");
    let _ = writeln!(
        out,
        "otp_actor_summary_queued_messages {}",
        s.total_queued_messages
    );
    out.push('\n');

    if let Some(remote) = &status.remote {
        let _ = writeln!(
            out,
            "# HELP otp_actor_remote_registered_endpoints Number of registered endpoints on the remote gRPC node."
        );
        let _ = writeln!(out, "# TYPE otp_actor_remote_registered_endpoints gauge");
        let _ = writeln!(
            out,
            "otp_actor_remote_registered_endpoints {}",
            remote.registered_endpoints
        );
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_remote_grpc_in_flight Number of gRPC Call/Cast requests currently in flight."
        );
        let _ = writeln!(out, "# TYPE otp_actor_remote_grpc_in_flight gauge");
        let _ = writeln!(
            out,
            "otp_actor_remote_grpc_in_flight {}",
            remote.grpc_in_flight
        );
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_remote_grpc_calls_total Total gRPC Call RPCs handled by the remote node."
        );
        let _ = writeln!(out, "# TYPE otp_actor_remote_grpc_calls_total counter");
        let _ = writeln!(
            out,
            "otp_actor_remote_grpc_calls_total {}",
            remote.grpc_calls_total
        );
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_remote_grpc_casts_total Total gRPC Cast RPCs handled by the remote node."
        );
        let _ = writeln!(out, "# TYPE otp_actor_remote_grpc_casts_total counter");
        let _ = writeln!(
            out,
            "otp_actor_remote_grpc_casts_total {}",
            remote.grpc_casts_total
        );
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_remote_auth_rejected_total Total gRPC requests rejected by the node authorizer."
        );
        let _ = writeln!(out, "# TYPE otp_actor_remote_auth_rejected_total counter");
        let _ = writeln!(
            out,
            "otp_actor_remote_auth_rejected_total {}",
            remote.auth_rejected_total
        );
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_remote_not_found_total Total gRPC requests targeting unknown services or actors."
        );
        let _ = writeln!(out, "# TYPE otp_actor_remote_not_found_total counter");
        let _ = writeln!(
            out,
            "otp_actor_remote_not_found_total{{kind=\"service\"}} {}",
            remote.service_not_found_total
        );
        let _ = writeln!(
            out,
            "otp_actor_remote_not_found_total{{kind=\"actor\"}} {}",
            remote.actor_not_found_total
        );
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_remote_framework_errors_total Total framework-level errors returned over gRPC."
        );
        let _ = writeln!(out, "# TYPE otp_actor_remote_framework_errors_total counter");
        let _ = writeln!(
            out,
            "otp_actor_remote_framework_errors_total {}",
            remote.framework_errors_total
        );
        out.push('\n');
    }

    let Some(tree) = &status.supervision_tree else {
        return out;
    };

    let mut flat = Vec::new();
    collect_flat_children(tree, "", &mut flat);
    if flat.is_empty() {
        return out;
    }

    let _ = writeln!(
        out,
        "# HELP otp_actor_child_status Lifecycle state per supervised child (0=Running, 1=Restarting, 2=Stopped, 3=Failed)."
    );
    let _ = writeln!(out, "# TYPE otp_actor_child_status gauge");
    for item in &flat {
        let _ = writeln!(
            out,
            "otp_actor_child_status{{supervisor=\"{}\",child=\"{}\",kind=\"{}\"}} {}",
            item.escaped_path,
            item.escaped_child,
            item.child.kind.as_str(),
            item.child.status.metric_code()
        );
    }
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_child_instance_id Current incarnation ID per supervised child."
    );
    let _ = writeln!(out, "# TYPE otp_actor_child_instance_id gauge");
    for item in &flat {
        let _ = writeln!(
            out,
            "otp_actor_child_instance_id{{supervisor=\"{}\",child=\"{}\"}} {}",
            item.escaped_path, item.escaped_child, item.child.instance_id
        );
    }
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_child_instance_uptime_seconds Elapsed uptime of the current child incarnation in seconds."
    );
    let _ = writeln!(out, "# TYPE otp_actor_child_instance_uptime_seconds gauge");
    for item in &flat {
        let _ = writeln!(
            out,
            "otp_actor_child_instance_uptime_seconds{{supervisor=\"{}\",child=\"{}\"}} {}",
            item.escaped_path, item.escaped_child, item.child.instance_uptime_seconds
        );
    }
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_child_restarts_in_window Recent restarts currently inside the child's restart time window."
    );
    let _ = writeln!(out, "# TYPE otp_actor_child_restarts_in_window gauge");
    for item in &flat {
        let _ = writeln!(
            out,
            "otp_actor_child_restarts_in_window{{supervisor=\"{}\",child=\"{}\"}} {}",
            item.escaped_path, item.escaped_child, item.child.restarts_in_window
        );
    }
    out.push('\n');

    let _ = writeln!(
        out,
        "# HELP otp_actor_child_restarts_total Cumulative supervisor restarts for each child slot."
    );
    let _ = writeln!(out, "# TYPE otp_actor_child_restarts_total counter");
    for item in &flat {
        let _ = writeln!(
            out,
            "otp_actor_child_restarts_total{{supervisor=\"{}\",child=\"{}\"}} {}",
            item.escaped_path, item.escaped_child, item.child.restarts_total
        );
    }
    out.push('\n');

    if flat.iter().any(|i| i.child.queue.is_some()) {
        let _ = writeln!(
            out,
            "# HELP otp_actor_mailbox_queued Approximate queued message count in each bounded mailbox."
        );
        let _ = writeln!(out, "# TYPE otp_actor_mailbox_queued gauge");
        for item in &flat {
            if let Some(q) = &item.child.queue {
                let _ = writeln!(
                    out,
                    "otp_actor_mailbox_queued{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, q.queued
                );
            }
        }
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_mailbox_capacity Configured bounded mailbox capacity per child."
        );
        let _ = writeln!(out, "# TYPE otp_actor_mailbox_capacity gauge");
        for item in &flat {
            if let Some(q) = &item.child.queue {
                let _ = writeln!(
                    out,
                    "otp_actor_mailbox_capacity{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, q.max_capacity
                );
            }
        }
        out.push('\n');
    }

    if flat.iter().any(|i| i.child.metrics.is_some()) {
        let _ = writeln!(
            out,
            "# HELP otp_actor_in_flight Number of handlers currently executing on each actor."
        );
        let _ = writeln!(out, "# TYPE otp_actor_in_flight gauge");
        for item in &flat {
            if let Some(m) = &item.child.metrics {
                let _ = writeln!(
                    out,
                    "otp_actor_in_flight{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, m.in_flight
                );
            }
        }
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_calls_total Total synchronous call invocations processed per actor."
        );
        let _ = writeln!(out, "# TYPE otp_actor_calls_total counter");
        for item in &flat {
            if let Some(m) = &item.child.metrics {
                let _ = writeln!(
                    out,
                    "otp_actor_calls_total{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, m.calls_total
                );
            }
        }
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_casts_total Total fire-and-forget cast invocations processed per actor."
        );
        let _ = writeln!(out, "# TYPE otp_actor_casts_total counter");
        for item in &flat {
            if let Some(m) = &item.child.metrics {
                let _ = writeln!(
                    out,
                    "otp_actor_casts_total{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, m.casts_total
                );
            }
        }
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_handler_errors_total Total domain errors returned by actor handlers."
        );
        let _ = writeln!(out, "# TYPE otp_actor_handler_errors_total counter");
        for item in &flat {
            if let Some(m) = &item.child.metrics {
                let _ = writeln!(
                    out,
                    "otp_actor_handler_errors_total{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, m.handler_errors_total
                );
            }
        }
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_timeouts_total Total handler executions that exceeded handler_timeout."
        );
        let _ = writeln!(out, "# TYPE otp_actor_timeouts_total counter");
        for item in &flat {
            if let Some(m) = &item.child.metrics {
                let _ = writeln!(
                    out,
                    "otp_actor_timeouts_total{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, m.timeouts_total
                );
            }
        }
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_panics_total Total unwinding panics caught in actor tasks."
        );
        let _ = writeln!(out, "# TYPE otp_actor_panics_total counter");
        for item in &flat {
            if let Some(m) = &item.child.metrics {
                let _ = writeln!(
                    out,
                    "otp_actor_panics_total{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, m.panics_total
                );
            }
        }
        out.push('\n');

        let _ = writeln!(
            out,
            "# HELP otp_actor_ema_latency_microseconds Exponential moving average handler latency in microseconds."
        );
        let _ = writeln!(out, "# TYPE otp_actor_ema_latency_microseconds gauge");
        for item in &flat {
            if let Some(m) = &item.child.metrics {
                let _ = writeln!(
                    out,
                    "otp_actor_ema_latency_microseconds{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path, item.escaped_child, m.ema_latency_us
                );
            }
        }
        out.push('\n');
    }

    if flat.iter().any(|i| i.child.cache_state.is_some()) {
        let _ = writeln!(
            out,
            "# HELP otp_actor_resource_cache_state Cache state per ResourceActor (0=Empty, 1=Fresh, 2=RefreshDue, 3=StaleUsable, 4=Expired, 5=Invalidated)."
        );
        let _ = writeln!(out, "# TYPE otp_actor_resource_cache_state gauge");
        for item in &flat {
            if let Some(cs) = &item.child.cache_state {
                let _ = writeln!(
                    out,
                    "otp_actor_resource_cache_state{{supervisor=\"{}\",child=\"{}\"}} {}",
                    item.escaped_path,
                    item.escaped_child,
                    cache_state_code(cs)
                );
            }
        }
    }

    out
}

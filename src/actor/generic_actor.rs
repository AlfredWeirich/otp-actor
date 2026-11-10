//! # Generic Resource & Worker Actor (`ResourceActor`)
//!
//! A completely reusable, fault-tolerant concurrent actor that executes any user-provided
//! [`Worker`] implementation or closure to fetch, cache, and proactively refresh arbitrary resources.
//!
//! ## Key Highlights
//! - **Zero-Hop Cache Reads**: Fresh cached resources are published to a [`tokio::sync::watch`] channel
//!   and served in 0 hops by [`ResourceClient::get`] without allocating a `oneshot` channel or traversing
//!   the actor mailbox.
//! - **Zero-Cost Worker Futures**: Accepts custom structs, standalone `async fn`s, or closures returning
//!   native `impl Future` without `Box::pin` heap allocations.
//! - **Single-Flight Coalescing & Backpressure**: Concurrent cache-miss callers coalesce onto a single
//!   in-flight worker execution bounded by `max_waiters`.
//! - **Proactive Refresh, Negative Caching & Stale-While-Error**: Automatically refreshes resources
//!   before TTL expiration with jitter, enforces failure cooldowns, and serves stale values during transient outages.

use std::future::Future;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::model::{
    EXPIRY_BUFFER, RestartPolicy, RestartType, ShutdownPolicy, SupervisorError, TaskResult,
};
use super::supervisor::{
    ChildSpec, ShutdownSignalFn, SlotState, SpawnContext, SupervisorHandle, UntypedActorFactory,
};

/// Default execution timeout on asynchronous worker tasks (30 seconds).
pub const DEFAULT_WORKER_TIMEOUT: Duration = Duration::from_secs(30);

/// Default cooldown window applied after a worker failure or timeout (1 second).
pub const DEFAULT_FAILURE_COOLDOWN: Duration = Duration::from_secs(1);

/// Error wrapper returned by [`ResourceClient`] operations.
#[non_exhaustive]
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResourceError<E> {
    #[error("Actor task terminated unexpectedly or is unavailable")]
    ActorUnavailable,
    #[error("Supervisor error: {0}")]
    Supervisor(#[from] SupervisorError),
    #[error("Worker operation failed: {0}")]
    WorkerFailed(E),
    #[error("Actor operation timed out after {0:?}")]
    Timeout(Duration),
    #[error("Actor is overloaded: waiters queue reached limit ({0} waiters)")]
    Overloaded(usize),
}

/// Execution metadata provided to [`Worker`] implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerContext {
    /// Identifier of the actor instance (increments upon supervisor auto-restart of this actor).
    pub instance_id: usize,
    /// Monotonically increasing generation number of the resource within the current actor instance.
    pub generation: usize,
}

/// A generic value bundled with optional TTL / expiration metadata.
///
/// `Debug` output intentionally redacts `value` (`"<redacted>"`) so cached tokens,
/// passwords, and secrets are not accidentally leaked into log files.
#[derive(Clone)]
pub struct TimedResource<T> {
    /// The actual cached payload.
    pub value: T,
    /// Optional expiration deadline.
    pub expires_at: Option<Instant>,
}

impl<T> std::fmt::Debug for TimedResource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimedResource")
            .field("value", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl<T> TimedResource<T> {
    /// Constructs a resource valid for a specific duration from now.
    ///
    /// If adding `ttl` overflows the monotonic clock (e.g. [`Duration::MAX`]),
    /// the resource is treated as perpetual without panicking.
    pub fn with_ttl(value: T, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now().checked_add(ttl),
        }
    }

    /// Constructs a permanent resource that never expires automatically.
    pub fn perpetual(value: T) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }

    /// Indicates whether the resource is still valid for immediate use.
    pub fn is_still_usable(&self) -> bool {
        match self.expires_at {
            Some(exp) => Instant::now() < exp,
            None => true,
        }
    }

    /// Indicates whether the resource is usable within a stale-while-error grace window.
    pub fn is_usable_stale(&self, stale_allowance: Duration) -> bool {
        if self.is_still_usable() {
            return true;
        }
        match self.expires_at {
            Some(exp) => exp
                .checked_add(stale_allowance)
                .is_none_or(|deadline| Instant::now() < deadline),
            None => true,
        }
    }

    /// Indicates whether the resource is fresh (outside the pre-expiration buffer).
    pub fn is_fresh(&self, buffer: Duration) -> bool {
        match self.expires_at {
            Some(exp) => exp.saturating_duration_since(Instant::now()) > buffer,
            None => true,
        }
    }
}

/// Snapshot published to the zero-hop [`watch`] channel on each successful worker refresh.
#[derive(Clone)]
pub(crate) struct ResourceSnapshot<T> {
    pub(crate) resource: TimedResource<T>,
    pub(crate) refresh_threshold: Option<Instant>,
}

/// Shared data-plane endpoint for a [`ResourceActor`] instance, combining the 0-hop [`watch`] cache
/// receiver and the 1-hop `mpsc` command sender.
pub(crate) struct ResourceEndpoint<T, E> {
    pub(crate) sender: Sender<ResourceCommand<T, E>>,
    pub(crate) cache_rx: watch::Receiver<Option<ResourceSnapshot<T>>>,
    pub(crate) invalidated: Arc<AtomicBool>,
}

impl<T, E> Clone for ResourceEndpoint<T, E> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            cache_rx: self.cache_rx.clone(),
            invalidated: self.invalidated.clone(),
        }
    }
}

// ============================================================================
// Worker Trait & Zero-Cost Abstraction
// ============================================================================

/// The fundamental Worker trait for producing, fetching, and refreshing resources.
/// Enables zero-cost abstractions by avoiding heap allocations (`Box::pin`) when returning
/// concrete or unboxed futures (similar to `tower::Service`).
pub trait Worker<T, E>: Send + Sync + 'static {
    /// Executes the worker logic to fetch or refresh the resource.
    ///
    /// # Cancel Safety
    /// When `worker_timeout` is configured or when `ForceRefresh` aborts a previous task,
    /// this future may be dropped before completion. Implementations should ensure cancel-safety
    /// (e.g. not leaving external locks or sockets in a corrupted state).
    fn execute(
        &self,
        ctx: WorkerContext,
    ) -> impl Future<Output = Result<TimedResource<T>, E>> + Send;
}

/// Blanket implementation of [`Worker`] for any async function or closure matching
/// `Fn(WorkerContext) -> Future<Output = Result<TimedResource<T>, E>>`.
impl<T, E, F, Fut> Worker<T, E> for F
where
    T: Send + 'static,
    E: Send + 'static,
    F: Fn(WorkerContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<TimedResource<T>, E>> + Send,
{
    fn execute(
        &self,
        ctx: WorkerContext,
    ) -> impl Future<Output = Result<TimedResource<T>, E>> + Send {
        (self)(ctx)
    }
}

/// Message protocol processed by a [`ResourceActor`].
pub(crate) enum ResourceCommand<T, E> {
    /// Client request seeking the managed resource.
    GetResource {
        respond_to: oneshot::Sender<Result<T, ResourceError<E>>>,
    },
    /// Proactively forces a cache refresh, superseding older in-flight fetches.
    ForceRefresh,
    /// Requests a graceful shutdown: closes the mailbox, completes any in-flight worker
    /// refresh for waiting callers, and exits cleanly with `Ok(())`.
    Shutdown,
    /// Explicit crash injection command for chaos and resilience testing.
    #[cfg(any(test, feature = "chaos_test"))]
    SimulateCrash,
}

// ============================================================================
// Configuration & Unified Builder for ResourceActor
// ============================================================================

/// Configuration parameters for a [`ResourceActor`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ResourceActorConfig {
    pub name: String,
    pub expiry_buffer: Duration,
    pub channel_capacity: usize,
    pub max_waiters: usize,
    pub eager_prewarm: bool,
    pub restart_policy: RestartPolicy,
    pub restart_type: RestartType,
    pub shutdown_policy: ShutdownPolicy,
    pub worker_timeout: Option<Duration>,
    pub failure_cooldown: Duration,
    pub stale_while_error: Duration,
    pub proactive_refresh: bool,
    pub proactive_jitter: Duration,
    pub max_consecutive_failures: Option<usize>,
}

impl ResourceActorConfig {
    /// Initializes default actor configuration settings with production-safe defaults.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expiry_buffer: EXPIRY_BUFFER,
            channel_capacity: 256,
            max_waiters: 1024,
            eager_prewarm: false,
            restart_policy: RestartPolicy::default(),
            restart_type: RestartType::default(),
            shutdown_policy: ShutdownPolicy::default(),
            worker_timeout: Some(DEFAULT_WORKER_TIMEOUT),
            failure_cooldown: DEFAULT_FAILURE_COOLDOWN,
            stale_while_error: Duration::ZERO,
            proactive_refresh: true,
            proactive_jitter: Duration::ZERO,
            max_consecutive_failures: None,
        }
    }
}

/// Unified fluent builder for configuring and spawning a [`ResourceActor`].
///
/// Configuration methods can be chained in any order, both before and after attaching a [`Worker`] via [`.worker(...)`](ResourceActorBuilder::worker).
pub struct ResourceActorBuilder<T = (), E = (), W = ()> {
    config: ResourceActorConfig,
    worker: W,
    _marker: PhantomData<(T, E)>,
}

impl ResourceActorBuilder<(), (), ()> {
    /// Creates a new [`ResourceActorBuilder`] initialized with production-safe defaults.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            config: ResourceActorConfig::new(name),
            worker: (),
            _marker: PhantomData,
        }
    }

    /// Attaches a [`Worker`] implementation (custom struct, `async fn`, or closure).
    /// Infers resource payload type `T` and error type `E` automatically.
    #[must_use]
    pub fn worker<T, E, W>(self, worker: W) -> ResourceActorBuilder<T, E, Arc<W>>
    where
        T: Clone + Send + Sync + 'static,
        E: Clone + Send + Sync + 'static,
        W: Worker<T, E>,
    {
        ResourceActorBuilder {
            config: self.config,
            worker: Arc::new(worker),
            _marker: PhantomData,
        }
    }
}

impl<T, E, W> ResourceActorBuilder<T, E, W> {
    /// Sets the expiration safety buffer window (defaults to [`EXPIRY_BUFFER`], 2 seconds).
    #[must_use]
    pub fn expiry_buffer(mut self, buffer: Duration) -> Self {
        self.config.expiry_buffer = buffer;
        self
    }

    /// Sets the actor's mailbox channel capacity (defaults to 256; clamped to `>= 1`).
    #[must_use]
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.config.channel_capacity = capacity.max(1);
        self
    }

    /// Sets the maximum capacity for concurrent pending callers waiting in queue (defaults to 1024; clamped to `>= 1`).
    #[must_use]
    pub fn max_waiters(mut self, max: usize) -> Self {
        self.config.max_waiters = max.max(1);
        self
    }

    /// Enables eager pre-warming: the actor initiates fetching the resource immediately
    /// on startup rather than waiting for the first client request.
    #[must_use]
    pub fn eager_prewarm(mut self, eager: bool) -> Self {
        self.config.eager_prewarm = eager;
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

    /// Sets an execution timeout on the asynchronous worker task.
    #[must_use]
    pub fn worker_timeout(mut self, timeout: Duration) -> Self {
        self.config.worker_timeout = Some(timeout);
        self
    }

    /// Disables execution timeout on the asynchronous worker task (allows workers to run indefinitely).
    #[must_use]
    pub fn no_worker_timeout(mut self) -> Self {
        self.config.worker_timeout = None;
        self
    }

    /// Sets the failure cooldown window (defaults to [`DEFAULT_FAILURE_COOLDOWN`], 1 second).
    /// During this window following a worker failure or timeout, further calls to `get()`
    /// will fast-fail immediately without spawning redundant worker tasks.
    #[must_use]
    pub fn failure_cooldown(mut self, cooldown: Duration) -> Self {
        self.config.failure_cooldown = cooldown;
        self
    }

    /// Sets the stale-while-error grace window (defaults to `Duration::ZERO`).
    /// If an expired resource is within this window, callers receive the stale value
    /// while an asynchronous background refresh attempts recovery.
    #[must_use]
    pub fn stale_while_error(mut self, duration: Duration) -> Self {
        self.config.stale_while_error = duration;
        self
    }

    /// Enables or disables proactive background timer refresh (defaults to `true`).
    #[must_use]
    pub fn proactive_refresh(mut self, enabled: bool) -> Self {
        self.config.proactive_refresh = enabled;
        self
    }

    /// Sets the maximum proactive refresh jitter (defaults to `Duration::ZERO`).
    #[must_use]
    pub fn proactive_jitter(mut self, jitter: Duration) -> Self {
        self.config.proactive_jitter = jitter;
        self
    }

    /// Configures a threshold of consecutive worker failures (or timeouts) after which
    /// the actor exits with an error to trigger a full supervisor-level restart
    /// (including `OneForAll` or `RestForOne` sibling recovery) without panicking.
    #[must_use]
    pub fn max_consecutive_failures(mut self, max: usize) -> Self {
        self.config.max_consecutive_failures = Some(max);
        self
    }
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static, W: Worker<T, E>>
    ResourceActorBuilder<T, E, Arc<W>>
{
    /// Builds an OTP-style [`ChildSpec`] that can be passed to a [`super::supervisor::Supervisor`].
    pub fn build_child_spec(self) -> ChildSpec {
        let mut config = self.config;
        config.channel_capacity = config.channel_capacity.max(1);
        config.max_waiters = config.max_waiters.max(1);
        let worker = self.worker;
        let channel_capacity = config.channel_capacity;
        let restart_policy = config.restart_policy;
        let restart_type = config.restart_type;
        let shutdown_policy = config.shutdown_policy;
        let name = config.name.clone();

        let factory: UntypedActorFactory = Arc::new(move |spawn_ctx: SpawnContext| {
            let (tx, rx) = mpsc::channel(channel_capacity);
            let (cache_tx, cache_rx) = watch::channel(None);
            let invalidated = Arc::new(AtomicBool::new(false));
            let endpoint = Arc::new(ResourceEndpoint {
                sender: tx,
                cache_rx,
                invalidated: invalidated.clone(),
            });
            let w = worker.clone();
            let cfg = config.clone();
            let task_handle = tokio::spawn(async move {
                let actor = ResourceActor::with_channels(
                    cfg,
                    rx,
                    cache_tx,
                    invalidated,
                    spawn_ctx.shutdown_rx,
                    spawn_ctx.instance_id,
                    w,
                );
                actor.run().await
            });
            (endpoint, task_handle)
        });

        let shutdown_signal: ShutdownSignalFn = Arc::new(|untyped| {
            if let Some(ep) = untyped.downcast_ref::<ResourceEndpoint<T, E>>() {
                ep.invalidated.store(true, Ordering::Release);
                let _ = ep.sender.try_send(ResourceCommand::Shutdown);
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
                if let Some(ep) = untyped.downcast_ref::<ResourceEndpoint<T, E>>() {
                    ep.invalidated.store(true, Ordering::Release);
                    let _ = ep.sender.try_send(ResourceCommand::SimulateCrash);
                }
            });
            spec = spec.with_crash_signal(crash_signal);
        }

        spec
    }

    /// Spawns the actor under the given [`SupervisorHandle`] and returns a direct [`ResourceClient`].
    pub async fn spawn(
        self,
        supervisor: &SupervisorHandle,
    ) -> Result<ResourceClient<T, E>, SupervisorError> {
        let name = self.config.name.clone();
        let spec = self.build_child_spec();
        supervisor.start_child(spec).await?;
        supervisor.get_resource_client::<T, E>(&name).await
    }
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static, W: Worker<T, E>>
    From<ResourceActorBuilder<T, E, Arc<W>>> for ChildSpec
{
    fn from(builder: ResourceActorBuilder<T, E, Arc<W>>) -> Self {
        builder.build_child_spec()
    }
}

// ============================================================================
// ResourceActor Implementation
// ============================================================================

enum WorkerOutcome<T, E> {
    Completed {
        generation: usize,
        result: Result<TimedResource<T>, E>,
    },
    TimedOut {
        generation: usize,
        timeout: Duration,
    },
}

#[derive(Debug, Clone)]
enum FailureKind<E> {
    Worker(E),
    Timeout(Duration),
}

impl<E: Clone> FailureKind<E> {
    fn to_resource_error(&self) -> ResourceError<E> {
        match self {
            Self::Worker(e) => ResourceError::WorkerFailed(e.clone()),
            Self::Timeout(timeout) => ResourceError::Timeout(*timeout),
        }
    }
}

/// Fault-tolerant actor managing cached resources, cold starts, single-flight coalescing, and background refreshes.
pub struct ResourceActor<T = (), E = (), W = ()> {
    name: String,
    receiver: Receiver<ResourceCommand<T, E>>,
    cache_tx: watch::Sender<Option<ResourceSnapshot<T>>>,
    invalidated: Arc<AtomicBool>,
    shutdown_rx: watch::Receiver<bool>,
    current_resource: Option<TimedResource<T>>,
    refresh_in_flight: bool,
    waiters: Vec<oneshot::Sender<Result<T, ResourceError<E>>>>,
    max_waiters: usize,
    generation_counter: usize,
    instance_id: usize,
    worker: Arc<W>,
    expiry_buffer: Duration,
    eager_prewarm: bool,
    worker_timeout: Option<Duration>,
    failure_cooldown: Duration,
    stale_while_error: Duration,
    proactive_refresh: bool,
    proactive_jitter: Duration,
    max_consecutive_failures: Option<usize>,
    last_failure: Option<(Instant, FailureKind<E>)>,
    failure_count: usize,
    zero_ttl_streak: usize,
    refresh_threshold: Option<Instant>,
    next_proactive_target: Option<Instant>,
    current_worker_abort: Option<tokio::task::AbortHandle>,
}

impl ResourceActor<(), (), ()> {
    /// Returns a new fluent [`ResourceActorBuilder`] to configure and spawn a [`ResourceActor`].
    pub fn builder(name: impl Into<String>) -> ResourceActorBuilder<(), (), ()> {
        ResourceActorBuilder::new(name)
    }

    /// Alias for [`ResourceActor::builder`].
    pub fn spec(name: impl Into<String>) -> ResourceActorBuilder<(), (), ()> {
        ResourceActorBuilder::new(name)
    }
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static, W: Worker<T, E>>
    ResourceActor<T, E, W>
{
    /// Initializes a [`ResourceActor`] instance with its mailbox, zero-hop watch sender, and shutdown receiver.
    pub(crate) fn with_channels(
        config: ResourceActorConfig,
        receiver: Receiver<ResourceCommand<T, E>>,
        cache_tx: watch::Sender<Option<ResourceSnapshot<T>>>,
        invalidated: Arc<AtomicBool>,
        shutdown_rx: watch::Receiver<bool>,
        instance_id: usize,
        worker: Arc<W>,
    ) -> Self {
        Self {
            name: config.name,
            receiver,
            cache_tx,
            invalidated,
            shutdown_rx,
            current_resource: None,
            refresh_in_flight: false,
            waiters: Vec::new(),
            max_waiters: config.max_waiters,
            generation_counter: 0,
            instance_id,
            worker,
            expiry_buffer: config.expiry_buffer,
            eager_prewarm: config.eager_prewarm,
            worker_timeout: config.worker_timeout,
            failure_cooldown: config.failure_cooldown,
            stale_while_error: config.stale_while_error,
            proactive_refresh: config.proactive_refresh,
            proactive_jitter: config.proactive_jitter,
            max_consecutive_failures: config.max_consecutive_failures,
            last_failure: None,
            failure_count: 0,
            zero_ttl_streak: 0,
            refresh_threshold: None,
            next_proactive_target: None,
            current_worker_abort: None,
        }
    }

    /// Indicates whether the actor is currently within a post-failure cooldown window.
    fn is_in_failure_cooldown(&self) -> bool {
        if self.failure_cooldown.is_zero() {
            return false;
        }
        if let Some((failed_at, _)) = &self.last_failure {
            failed_at.elapsed() < self.failure_cooldown
        } else {
            false
        }
    }

    /// Computes exponential retry backoff delay after worker failures (100ms..=30s).
    fn failure_backoff_delay(&self) -> Duration {
        let exp = self.failure_count.saturating_sub(1).min(9) as u32;
        Duration::from_millis(100)
            .saturating_mul(1 << exp)
            .min(Duration::from_secs(30))
    }

    /// Indicates whether a background refresh triggered while serving a cached or stale
    /// value should be throttled due to an active cooldown, failure backoff, or scheduled retry.
    fn is_background_refresh_throttled(&self) -> bool {
        if self.is_in_failure_cooldown() {
            return true;
        }
        let Some((failed_at, _)) = &self.last_failure else {
            return false;
        };
        if failed_at.elapsed() < self.failure_backoff_delay() {
            return true;
        }
        self.next_proactive_target
            .is_some_and(|target| Instant::now() < target)
    }

    /// Computes bounded pseudo-random jitter in milliseconds within `0..max_jitter`.
    fn compute_jitter_ms(&self, max_jitter: Duration) -> u64 {
        if max_jitter.is_zero() {
            return 0;
        }
        let max_ms = u64::try_from(max_jitter.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.instance_id.hash(&mut hasher);
        self.generation_counter.hash(&mut hasher);
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        now_nanos.hash(&mut hasher);
        let seed = hasher.finish();

        let mut x = seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(self.generation_counter as u64);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        x % max_ms
    }

    /// Computes and schedules the next proactive refresh target before resource expiration.
    fn schedule_proactive_refresh(&mut self, expires_at: Option<Instant>) {
        if !self.proactive_refresh {
            self.next_proactive_target = None;
            return;
        }

        let Some(exp) = expires_at else {
            self.zero_ttl_streak = 0;
            self.next_proactive_target = None;
            return;
        };

        let now = Instant::now();
        let min_refresh_target = now + Duration::from_millis(50);

        let target = if exp <= now {
            // Protect against zero-TTL or already-expired resources causing a 20 Hz refresh storm:
            // apply exponential backoff (500ms, 1s, 2s, ... capped at 30s).
            self.zero_ttl_streak = self.zero_ttl_streak.saturating_add(1);
            let exp_shift = self.zero_ttl_streak.saturating_sub(1).min(8) as u32;
            let backoff = Duration::from_millis(500)
                .saturating_mul(1 << exp_shift)
                .min(Duration::from_secs(30));
            tracing::warn!(
                actor = %self.name,
                instance = self.instance_id,
                zero_ttl_streak = self.zero_ttl_streak,
                backoff = ?backoff,
                "⚠️ Worker returned an already-expired resource (TTL <= 0); backing off proactive refresh."
            );
            now.checked_add(backoff).unwrap_or(min_refresh_target)
        } else {
            self.zero_ttl_streak = 0;
            let ttl = exp - now;
            // Clamp buffer to at most half of the TTL to avoid permanently stale state when ttl <= expiry_buffer
            let effective_buffer = self.expiry_buffer.min(ttl / 2);
            let mut t = exp.checked_sub(effective_buffer).unwrap_or(now);

            if self.proactive_jitter > Duration::ZERO {
                // Clamp jitter to at most half of the remaining lead window so large jitter
                // never collapses the refresh interval to min_refresh_target (50ms).
                let lead_window = t.saturating_duration_since(now);
                let max_jitter = self.proactive_jitter.min(lead_window / 2);
                let jitter_ms = self.compute_jitter_ms(max_jitter);
                t = t.checked_sub(Duration::from_millis(jitter_ms)).unwrap_or(t);
            }
            t.max(min_refresh_target)
        };

        tracing::debug!(
            actor = %self.name,
            instance = self.instance_id,
            delay = ?target.duration_since(now),
            "⏰ Proactive refresh scheduled."
        );
        self.next_proactive_target = Some(target);
    }

    /// Schedules a proactive retry in the background following a worker failure.
    fn schedule_proactive_retry(&mut self) {
        if !self.proactive_refresh {
            return;
        }

        let remaining_cooldown = if let Some((failed_at, _)) = &self.last_failure {
            self.failure_cooldown.saturating_sub(failed_at.elapsed())
        } else {
            Duration::ZERO
        };

        // Exponential backoff based on consecutive failures:
        // 1st: 100ms, 2nd: 200ms, 3rd: 400ms, 4th: 800ms, 5th: 1.6s ... capped at 30s
        let backoff = self.failure_backoff_delay();

        let retry_delay = remaining_cooldown.max(backoff);
        let now = Instant::now();
        let mut target = now.checked_add(retry_delay).unwrap_or(now);

        if self.proactive_jitter > Duration::ZERO {
            let max_jitter = self.proactive_jitter.min(retry_delay);
            let jitter_ms = self.compute_jitter_ms(max_jitter);
            target = target
                .checked_add(Duration::from_millis(jitter_ms))
                .unwrap_or(target);
        }

        tracing::info!(
            actor = %self.name,
            instance = self.instance_id,
            retry_delay = ?retry_delay,
            consecutive_failures = self.failure_count,
            "⏰ Proactive retry scheduled following worker failure."
        );
        self.next_proactive_target = Some(target);
    }

    /// Main message processing loop (supervised within actor task).
    pub(crate) async fn run(mut self) -> TaskResult {
        let mut worker_tasks: JoinSet<WorkerOutcome<T, E>> = JoinSet::new();

        // Proactive refresh sleep timer
        let proactive_sleep = tokio::time::sleep(Duration::from_secs(86400 * 365));
        tokio::pin!(proactive_sleep);
        let mut current_timer_target: Option<Instant> = None;

        // Eager Pre-Warming on actor boot
        if self.eager_prewarm {
            tracing::info!(
                actor = %self.name,
                instance = self.instance_id,
                "🔥 Eager Pre-Warming: Proactively fetching initial resource in background..."
            );
            self.trigger_background_worker(&mut worker_tasks);
        }

        loop {
            if *self.shutdown_rx.borrow() {
                return self.finish_graceful_shutdown(&mut worker_tasks).await;
            }

            // Synchronize proactive timer if scheduled target changed
            if self.next_proactive_target != current_timer_target {
                current_timer_target = self.next_proactive_target;
                if let Some(target) = current_timer_target {
                    proactive_sleep.as_mut().reset(target);
                } else {
                    proactive_sleep
                        .as_mut()
                        .reset(Instant::now() + Duration::from_secs(86400 * 365));
                }
            }

            tokio::select! {
                Ok(()) = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        return self.finish_graceful_shutdown(&mut worker_tasks).await;
                    }
                }

                _ = &mut proactive_sleep, if self.next_proactive_target.is_some() => {
                    self.next_proactive_target = None;
                    current_timer_target = None;

                    if self.refresh_in_flight {
                        tracing::debug!(
                            actor = %self.name,
                            instance = self.instance_id,
                            "⏰ Proactive Timer fired, but worker refresh is already in flight. Skipping."
                        );
                    } else if self.is_in_failure_cooldown() {
                        tracing::debug!(
                            actor = %self.name,
                            instance = self.instance_id,
                            "⏰ Proactive Timer fired during failure cooldown. Rescheduling retry..."
                        );
                        self.schedule_proactive_retry();
                    } else {
                        tracing::info!(
                            actor = %self.name,
                            instance = self.instance_id,
                            "⏰ Proactive Timer fired! Refreshing resource before expiration..."
                        );
                        self.trigger_background_worker(&mut worker_tasks);
                    }
                }

                Some(join_res) = worker_tasks.join_next(), if !worker_tasks.is_empty() => {
                    match join_res {
                        Ok(WorkerOutcome::Completed { generation, result }) => {
                            self.handle_refresh_complete(generation, result)?;
                        }
                        Ok(WorkerOutcome::TimedOut { generation, timeout }) => {
                            self.handle_worker_timeout(generation, timeout)?;
                        }
                        Err(join_err) => {
                            if join_err.is_panic() {
                                tracing::error!(
                                    actor = %self.name,
                                    instance = self.instance_id,
                                    panic = ?join_err,
                                    "💥 WORKER PANIC ESCALATED."
                                );
                                self.invalidated.store(true, Ordering::Release);
                                self.cache_tx.send_replace(None);
                                self.current_worker_abort = None;
                                self.refresh_in_flight = false;
                                self.waiters.clear();
                                return Err(format!("Worker panic escalated to supervisor: {:?}", join_err));
                            } else if join_err.is_cancelled() {
                                tracing::debug!(
                                    actor = %self.name,
                                    instance = self.instance_id,
                                    "ℹ️ In-flight worker task cancelled (superseded)."
                                );
                            }
                        }
                    }
                }

                cmd_opt = self.receiver.recv() => {
                    let Some(command) = cmd_opt else {
                        self.invalidated.store(true, Ordering::Release);
                        return Ok(());
                    };

                    match command {
                        ResourceCommand::GetResource { respond_to } => {
                            self.handle_get_resource(respond_to, &mut worker_tasks);
                        }
                        ResourceCommand::ForceRefresh => {
                            tracing::info!(
                                actor = %self.name,
                                instance = self.instance_id,
                                generation = self.generation_counter,
                                "🔄 Force refresh requested. Superseding previous generation..."
                            );
                            if let Some(prev_abort) = self.current_worker_abort.take() {
                                tracing::debug!(
                                    actor = %self.name,
                                    instance = self.instance_id,
                                    generation = self.generation_counter,
                                    "⏹️ Aborting previous in-flight worker due to ForceRefresh..."
                                );
                                prev_abort.abort();
                            }
                            self.refresh_in_flight = false;
                            self.last_failure = None;
                            self.failure_count = 0;
                            self.trigger_background_worker(&mut worker_tasks);
                        }
                        ResourceCommand::Shutdown => {
                            return self.finish_graceful_shutdown(&mut worker_tasks).await;
                        }
                        #[cfg(any(test, feature = "chaos_test"))]
                        ResourceCommand::SimulateCrash => {
                            tracing::warn!(
                                actor = %self.name,
                                instance = self.instance_id,
                                "💥 CRASH COMMAND RECEIVED: Terminating actor with crash error!"
                            );
                            self.invalidated.store(true, Ordering::Release);
                            self.cache_tx.send_replace(None);
                            worker_tasks.abort_all();
                            self.waiters.clear();
                            return Err(format!(
                                "Intentional crash triggered via SimulateCrash on ResourceActor '{}' #{}",
                                self.name, self.instance_id
                            ));
                        }
                    }
                }
            }
        }
    }

    /// Gracefully drains the actor mailbox and awaits any in-flight worker refresh before exiting cleanly.
    async fn finish_graceful_shutdown(
        &mut self,
        worker_tasks: &mut JoinSet<WorkerOutcome<T, E>>,
    ) -> TaskResult {
        tracing::info!(
            actor = %self.name,
            instance = self.instance_id,
            refresh_in_flight = self.refresh_in_flight,
            waiters = self.waiters.len(),
            "🛑 Graceful shutdown initiated. Closing mailbox and completing in-flight work..."
        );

        // 1. Mark zero-hop cache invalidated and close mailbox so no new requests can be queued
        self.invalidated.store(true, Ordering::Release);
        self.receiver.close();

        // 2. Drain any requests already buffered in the channel
        while let Ok(cmd) = self.receiver.try_recv() {
            if let ResourceCommand::GetResource { respond_to } = cmd {
                if let Some(res) = &self.current_resource
                    && (res.is_still_usable()
                        || (self.stale_while_error > Duration::ZERO
                            && res.is_usable_stale(self.stale_while_error)))
                {
                    let _ = respond_to.send(Ok(res.value.clone()));
                    continue;
                }

                if self.refresh_in_flight && self.waiters.len() < self.max_waiters {
                    self.waiters.push(respond_to);
                } else {
                    let _ = respond_to.send(Err(ResourceError::ActorUnavailable));
                }
            }
        }

        // 3. Wait for any in-flight worker to finish and deliver its result to waiting callers
        while self.refresh_in_flight && !worker_tasks.is_empty() {
            if let Some(join_res) = worker_tasks.join_next().await {
                match join_res {
                    Ok(WorkerOutcome::Completed { generation, result }) => {
                        let _ = self.handle_refresh_complete(generation, result);
                    }
                    Ok(WorkerOutcome::TimedOut {
                        generation,
                        timeout,
                    }) => {
                        let _ = self.handle_worker_timeout(generation, timeout);
                    }
                    Err(join_err) => {
                        if join_err.is_cancelled() {
                            continue;
                        }
                        self.refresh_in_flight = false;
                        break;
                    }
                }
            } else {
                break;
            }
        }

        // 4. Clean up any remaining waiters or background tasks
        self.cache_tx.send_replace(None);
        for waiter in self.waiters.drain(..) {
            let _ = waiter.send(Err(ResourceError::ActorUnavailable));
        }
        worker_tasks.abort_all();

        tracing::info!(
            actor = %self.name,
            instance = self.instance_id,
            "✅ ResourceActor graceful shutdown complete."
        );
        Ok(())
    }

    /// Handles client get requests: Serves valid caches immediately; triggers background refresh.
    fn handle_get_resource(
        &mut self,
        respond_to: oneshot::Sender<Result<T, ResourceError<E>>>,
        worker_tasks: &mut JoinSet<WorkerOutcome<T, E>>,
    ) {
        let in_cooldown = self.is_in_failure_cooldown();
        let bg_throttled = self.is_background_refresh_throttled();

        // 1. Fresh or still usable resource
        if let Some(res) = &self.current_resource {
            if res.is_still_usable() {
                let _ = respond_to.send(Ok(res.value.clone()));

                let should_refresh = match self.refresh_threshold {
                    Some(threshold) => Instant::now() >= threshold,
                    None => false,
                };

                if should_refresh && !self.refresh_in_flight {
                    if bg_throttled {
                        tracing::debug!(
                            actor = %self.name,
                            instance = self.instance_id,
                            "Resource approaching expiration, but failure cooldown/backoff is active; skipping background refresh."
                        );
                    } else {
                        tracing::debug!(
                            actor = %self.name,
                            instance = self.instance_id,
                            "Resource approaching expiration. Served caller and initiated background refresh..."
                        );
                        self.trigger_background_worker(worker_tasks);
                    }
                }
                return;
            }

            // 2. Expired, but within stale-while-error allowance
            if self.stale_while_error > Duration::ZERO
                && res.is_usable_stale(self.stale_while_error)
            {
                tracing::warn!(
                    actor = %self.name,
                    instance = self.instance_id,
                    allowance = ?self.stale_while_error,
                    "⚠️ Resource expired! Serving STALE cache within stale_while_error window."
                );
                let _ = respond_to.send(Ok(res.value.clone()));

                if !self.refresh_in_flight && !bg_throttled {
                    tracing::info!(
                        actor = %self.name,
                        instance = self.instance_id,
                        "Triggering background worker to recover stale resource..."
                    );
                    self.trigger_background_worker(worker_tasks);
                }
                return;
            }
        }

        // 3. No usable resource (cold start or expired beyond stale allowance)
        if in_cooldown && let Some((t, f)) = &self.last_failure {
            let failure = f.to_resource_error();
            let remaining = self.failure_cooldown.saturating_sub(t.elapsed());
            tracing::warn!(
                actor = %self.name,
                instance = self.instance_id,
                remaining = ?remaining,
                "🛑 Fast-failing caller during failure cooldown to prevent retry storm."
            );
            let _ = respond_to.send(Err(failure));
            return;
        }

        // Prune dead/dropped callers only when approaching or reaching capacity
        if self.waiters.len() >= self.max_waiters {
            self.waiters.retain(|w| !w.is_closed());
        }

        // Check waiters limit for backpressure
        if self.waiters.len() >= self.max_waiters {
            tracing::warn!(
                actor = %self.name,
                instance = self.instance_id,
                limit = self.max_waiters,
                "⚠️ Waiters queue overflow! Rejecting caller with Overloaded error."
            );
            let _ = respond_to.send(Err(ResourceError::Overloaded(self.max_waiters)));
            return;
        }

        let reason = if self.current_resource.is_none() {
            "Cold Start (no resource in cache)"
        } else {
            "Hard Expiry (cached resource expired!)"
        };
        tracing::debug!(
            actor = %self.name,
            instance = self.instance_id,
            reason,
            waiters = self.waiters.len() + 1,
            "⏳ Caller parked in waiters queue."
        );
        self.waiters.push(respond_to);
        self.trigger_background_worker(worker_tasks);
    }

    /// Executes the user-supplied asynchronous worker in the background via JoinSet.
    fn trigger_background_worker(&mut self, worker_tasks: &mut JoinSet<WorkerOutcome<T, E>>) {
        if self.refresh_in_flight {
            return;
        }

        self.refresh_in_flight = true;
        self.generation_counter += 1;
        let next_gen = self.generation_counter;
        let actor_name = self.name.clone();
        let actor_id = self.instance_id;
        let worker = self.worker.clone();
        let timeout_opt = self.worker_timeout;

        tracing::info!(
            actor = %actor_name,
            instance = actor_id,
            generation = next_gen,
            "🚀 Executing background worker..."
        );

        let ctx = WorkerContext {
            instance_id: actor_id,
            generation: next_gen,
        };

        let abort_handle = worker_tasks.spawn(async move {
            let fut = worker.execute(ctx);
            if let Some(timeout) = timeout_opt {
                match tokio::time::timeout(timeout, fut).await {
                    Ok(result) => WorkerOutcome::Completed {
                        generation: next_gen,
                        result,
                    },
                    Err(_) => WorkerOutcome::TimedOut {
                        generation: next_gen,
                        timeout,
                    },
                }
            } else {
                WorkerOutcome::Completed {
                    generation: next_gen,
                    result: fut.await,
                }
            }
        });
        self.current_worker_abort = Some(abort_handle);
    }

    /// Handles worker timeout for the given generation.
    fn handle_worker_timeout(&mut self, generation: usize, timeout: Duration) -> TaskResult {
        if generation < self.generation_counter {
            tracing::debug!(
                actor = %self.name,
                instance = self.instance_id,
                generation,
                current_generation = self.generation_counter,
                "ℹ️ Stale worker timeout ignored."
            );
            return Ok(());
        }

        self.current_worker_abort = None;
        self.refresh_in_flight = false;
        self.failure_count += 1;
        self.last_failure = Some((Instant::now(), FailureKind::Timeout(timeout)));

        if let Some(max_failures) = self.max_consecutive_failures
            && max_failures > 0
            && self.failure_count >= max_failures
        {
            tracing::error!(
                actor = %self.name,
                instance = self.instance_id,
                consecutive_failures = self.failure_count,
                max_failures,
                "💥 Worker exceeded max_consecutive_failures via timeout; escalating to supervisor!"
            );
            self.invalidated.store(true, Ordering::Release);
            self.cache_tx.send_replace(None);
            self.waiters.clear();
            return Err(format!(
                "ResourceActor '{}' #{} exceeded max_consecutive_failures ({}) after worker timeout",
                self.name, self.instance_id, max_failures
            ));
        }

        tracing::warn!(
            actor = %self.name,
            instance = self.instance_id,
            generation,
            consecutive_failures = self.failure_count,
            timeout = ?timeout,
            "⏱️ Worker timed out."
        );

        for waiter in self.waiters.drain(..) {
            let _ = waiter.send(Err(ResourceError::Timeout(timeout)));
        }

        if self.proactive_refresh {
            self.schedule_proactive_retry();
        }
        Ok(())
    }

    /// Updates cache with the worker output if generation matches, publishes to the 0-hop `watch` channel,
    /// and broadcasts to parked waiters.
    fn handle_refresh_complete(
        &mut self,
        generation: usize,
        result: Result<TimedResource<T>, E>,
    ) -> TaskResult {
        if generation < self.generation_counter {
            tracing::debug!(
                actor = %self.name,
                instance = self.instance_id,
                generation,
                current_generation = self.generation_counter,
                "ℹ️ Stale worker output ignored."
            );
            return Ok(());
        }

        self.current_worker_abort = None;
        self.refresh_in_flight = false;

        match result {
            Ok(new_resource) => {
                tracing::info!(
                    actor = %self.name,
                    instance = self.instance_id,
                    generation,
                    "✅ New resource acquired via worker closure!"
                );

                self.last_failure = None;
                self.failure_count = 0;

                let val = new_resource.value.clone();
                let expires_at = new_resource.expires_at;
                let now = Instant::now();
                let threshold = expires_at.map(|exp| {
                    if exp <= now {
                        now
                    } else {
                        let ttl = exp - now;
                        let effective_buffer = self.expiry_buffer.min(ttl / 2);
                        exp.checked_sub(effective_buffer).unwrap_or(now)
                    }
                });
                self.refresh_threshold = threshold;
                self.current_resource = Some(new_resource.clone());
                self.cache_tx.send_replace(Some(ResourceSnapshot {
                    resource: new_resource,
                    refresh_threshold: threshold,
                }));

                if self.proactive_refresh {
                    self.schedule_proactive_refresh(expires_at);
                }

                if !self.waiters.is_empty() {
                    tracing::debug!(
                        actor = %self.name,
                        instance = self.instance_id,
                        waiters = self.waiters.len(),
                        "📢 Broadcasting value to waiting tasks."
                    );
                    for waiter in self.waiters.drain(..) {
                        let _ = waiter.send(Ok(val.clone()));
                    }
                }
            }
            Err(err) => {
                self.failure_count += 1;
                self.last_failure = Some((Instant::now(), FailureKind::Worker(err.clone())));

                if let Some(max_failures) = self.max_consecutive_failures
                    && max_failures > 0
                    && self.failure_count >= max_failures
                {
                    tracing::error!(
                        actor = %self.name,
                        instance = self.instance_id,
                        consecutive_failures = self.failure_count,
                        max_failures,
                        "💥 Worker exceeded max_consecutive_failures; escalating to supervisor!"
                    );
                    self.invalidated.store(true, Ordering::Release);
                    self.cache_tx.send_replace(None);
                    self.waiters.clear();
                    return Err(format!(
                        "ResourceActor '{}' #{} exceeded max_consecutive_failures ({})",
                        self.name, self.instance_id, max_failures
                    ));
                }

                tracing::warn!(
                    actor = %self.name,
                    instance = self.instance_id,
                    generation,
                    consecutive_failures = self.failure_count,
                    "❌ Worker failed."
                );

                for waiter in self.waiters.drain(..) {
                    let _ = waiter.send(Err(ResourceError::WorkerFailed(err.clone())));
                }

                if self.proactive_refresh {
                    self.schedule_proactive_retry();
                }
            }
        }
        Ok(())
    }
}

// ============================================================================
// Direct Resource Client Handle (Zero-Hop Cache + 1-Hop Control/Miss Data Plane)
// ============================================================================

/// High-performance, self-healing Data Plane client for a [`ResourceActor`].
#[derive(Clone)]
pub struct ResourceClient<T, E> {
    name: String,
    endpoint: Arc<RwLock<ResourceEndpoint<T, E>>>,
    slot_rx: Arc<RwLock<watch::Receiver<SlotState>>>,
    supervisor: SupervisorHandle,
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static> ResourceClient<T, E> {
    /// Creates a new direct client handle subscribed to the child's [`SlotState`] watch channel.
    pub(crate) fn new(
        name: impl Into<String>,
        endpoint: ResourceEndpoint<T, E>,
        slot_rx: watch::Receiver<SlotState>,
        supervisor: SupervisorHandle,
    ) -> Self {
        Self {
            name: name.into(),
            endpoint: Arc::new(RwLock::new(endpoint)),
            slot_rx: Arc::new(RwLock::new(slot_rx)),
            supervisor,
        }
    }

    /// Returns the name of the monitored actor.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the [`SupervisorHandle`] managing this actor.
    pub fn supervisor(&self) -> &SupervisorHandle {
        &self.supervisor
    }

    fn current_endpoint(&self) -> ResourceEndpoint<T, E> {
        self.endpoint
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Reactively waits on the [`SlotState`] watch channel until a replacement [`ResourceActor`] instance is running,
    /// or returns immediately if the actor was stopped or permanently failed.
    async fn rebind_channel(
        &self,
        failed_sender: &Sender<ResourceCommand<T, E>>,
    ) -> Result<(), ResourceError<E>> {
        {
            let current = self.current_endpoint();
            if !current.invalidated.load(Ordering::Acquire)
                && !current.sender.is_closed()
                && !current.sender.same_channel(failed_sender)
            {
                return Ok(());
            }
        }

        loop {
            let mut rx = self
                .slot_rx
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();

            let wait_state = rx
                .wait_for(|state| match state {
                    SlotState::Running { endpoint, .. } => endpoint
                        .downcast_ref::<ResourceEndpoint<T, E>>()
                        .is_some_and(|ep| {
                            !ep.invalidated.load(Ordering::Acquire)
                                && !ep.sender.is_closed()
                                && !ep.sender.same_channel(failed_sender)
                        }),
                    SlotState::Stopped | SlotState::Failed(_) => true,
                    SlotState::Restarting { .. } => false,
                })
                .await
                .ok()
                .map(|s| s.clone());

            if let Some(state_ref) = wait_state {
                match &state_ref {
                    SlotState::Running { endpoint, .. } => {
                        if let Some(new_ep) = endpoint.downcast_ref::<ResourceEndpoint<T, E>>() {
                            *self
                                .endpoint
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                new_ep.clone();
                            tracing::info!(
                                actor = %self.name,
                                "🔄 ResourceClient channel re-established with replacement actor!"
                            );
                            return Ok(());
                        }
                        return Err(ResourceError::Supervisor(
                            SupervisorError::ChildTypeMismatch(self.name.clone()),
                        ));
                    }
                    SlotState::Failed(err) => return Err(ResourceError::Supervisor(err.clone())),
                    SlotState::Stopped => {
                        return Err(ResourceError::Supervisor(SupervisorError::ChildNotFound(
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
                .map_err(ResourceError::Supervisor)?;
            *self
                .slot_rx
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = new_rx;
        }
    }

    /// Sends a one-way command to the actor, transparently rebinding if the channel was closed by a prior crash.
    async fn send_control(
        &self,
        invalidate_cache: bool,
        make_cmd: impl Fn() -> ResourceCommand<T, E>,
    ) -> Result<(), ResourceError<E>> {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_ep = self.current_endpoint();
            if invalidate_cache {
                current_ep.invalidated.store(true, Ordering::Release);
            }

            if current_ep.sender.send(make_cmd()).await.is_ok() {
                return Ok(());
            }

            if attempt < MAX_ATTEMPTS {
                self.rebind_channel(&current_ep.sender).await?;
            }
        }

        Err(ResourceError::ActorUnavailable)
    }

    /// Retrieves the resource:
    /// - **0-Hop Fast Path**: If the actor holds a fresh cached resource outside its expiration buffer,
    ///   returns a clone directly from the [`tokio::sync::watch`] channel without any channel allocation or context switch.
    /// - **1-Hop Coalesced Path**: On cold start, pre-expiration refresh window, or stale-while-error recovery,
    ///   sends a direct request to the [`ResourceActor`] and transparently rebinds via [`SlotState`] on crash/restart.
    pub async fn get(&self) -> Result<T, ResourceError<E>> {
        // 0-hop fast path via watch::Receiver
        {
            let current_ep = self.current_endpoint();
            if !current_ep.invalidated.load(Ordering::Acquire)
                && !current_ep.sender.is_closed()
                && let Some(snapshot) = current_ep.cache_rx.borrow().as_ref()
                && snapshot.resource.is_still_usable()
            {
                let needs_refresh = snapshot
                    .refresh_threshold
                    .is_some_and(|threshold| Instant::now() >= threshold);
                if !needs_refresh {
                    return Ok(snapshot.resource.value.clone());
                }
            }
        }

        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_ep = self.current_endpoint();

            let (tx, rx) = oneshot::channel();
            let send_res = current_ep
                .sender
                .send(ResourceCommand::GetResource { respond_to: tx })
                .await;

            if send_res.is_ok()
                && let Ok(item_res) = rx.await
                && !matches!(item_res, Err(ResourceError::ActorUnavailable))
            {
                return item_res;
            }

            if attempt < MAX_ATTEMPTS {
                tracing::warn!(
                    actor = %self.name,
                    attempt,
                    max_attempts = MAX_ATTEMPTS,
                    "⚠️ Direct channel broken or actor restarting. Rebinding via reactive watch slot..."
                );

                self.rebind_channel(&current_ep.sender).await?;
            }
        }

        Err(ResourceError::ActorUnavailable)
    }

    /// Retrieves the resource with a client-side timeout deadline.
    pub async fn get_timeout(&self, timeout: Duration) -> Result<T, ResourceError<E>> {
        tokio::time::timeout(timeout, self.get())
            .await
            .map_err(|_| ResourceError::Timeout(timeout))?
    }

    /// Proactively forces a background refresh, superseding any currently in-flight refresh.
    pub async fn force_refresh(&self) -> Result<(), ResourceError<E>> {
        self.send_control(false, || ResourceCommand::ForceRefresh)
            .await
    }

    /// Requests a graceful shutdown of the actor over its direct data-plane channel.
    pub async fn shutdown(&self) -> Result<(), ResourceError<E>> {
        self.send_control(true, || ResourceCommand::Shutdown).await
    }

    /// Injects an intentional crash command directly into the actor for resilience testing.
    #[cfg(any(test, feature = "chaos_test"))]
    pub async fn simulate_crash(&self) -> Result<(), ResourceError<E>> {
        self.send_control(true, || ResourceCommand::SimulateCrash)
            .await
    }
}

//! # Generic Resource & Worker Actor
//!
//! A completely reusable, fault-tolerant concurrent actor that executes any user-provided
//! [`Worker`] implementation or closure to fetch, cache, and proactively refresh arbitrary resources.
//! Supports zero-cost execution (without `Box::pin`), fluent builder configuration, eager pre-warming,
//! and restart policies.

use std::future::Future;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::model::{
    EXPIRY_BUFFER, RestartPolicy, RestartType, ServiceError, ShutdownPolicy, TaskResult,
};
use super::root_supervisor::RootSupervisorHandle;
use super::service_supervisor::{
    ChildSpec, ServiceSupervisorHandle, ShutdownSignalFn, UntypedActorFactory,
};

/// Default execution timeout on asynchronous worker tasks (30 seconds).
pub const DEFAULT_WORKER_TIMEOUT: Duration = Duration::from_secs(30);

/// Error wrapper returned by generic resource clients.
#[derive(Debug, Error, Clone)]
pub enum ResourceError<E> {
    #[error("Actor task terminated unexpectedly or is unavailable")]
    ActorUnavailable,
    #[error("Service supervisor error: {0}")]
    Service(#[from] ServiceError),
    #[error("Worker operation failed: {0}")]
    WorkerFailed(E),
    #[error("Actor operation timed out after {0:?}")]
    Timeout(Duration),
    #[error("Actor is overloaded: waiters queue reached limit ({0} waiters)")]
    Overloaded(usize),
}

/// Execution metadata provided to worker implementations.
#[derive(Debug, Clone)]
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

/// Message protocol processed by a [`GenericResourceActor`].
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
    SimulateCrash,
}

// ============================================================================
// Configuration & Builders for GenericResourceActor
// ============================================================================

/// Configuration parameters for a [`GenericResourceActor`].
#[derive(Debug, Clone)]
pub struct ActorConfig {
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

impl ActorConfig {
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
            failure_cooldown: Duration::ZERO,
            stale_while_error: Duration::ZERO,
            proactive_refresh: true,
            proactive_jitter: Duration::ZERO,
            max_consecutive_failures: None,
        }
    }
}

macro_rules! impl_actor_config_setters {
    () => {
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

        /// Sets the failure cooldown window (defaults to `Duration::ZERO`).
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
        /// When enabled, the actor automatically refreshes expiring resources in the background
        /// before their expiration buffer window is reached.
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
    };
}

/// Initial builder for a generic actor before a [`Worker`] is attached.
pub struct ActorBuilder {
    pub config: ActorConfig,
}

impl ActorBuilder {
    /// Creates a new builder initialized with defaults.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            config: ActorConfig::new(name),
        }
    }

    impl_actor_config_setters!();

    /// Attaches a [`Worker`] implementation (custom struct, async function, or closure).
    /// Infers resource payload type `T` and error type `E` automatically without type annotations!
    #[must_use]
    pub fn worker<T, E, W>(self, worker: W) -> GenericResourceActorBuilder<T, E, W>
    where
        T: Clone + Send + Sync + 'static,
        E: Clone + Send + Sync + 'static,
        W: Worker<T, E>,
    {
        GenericResourceActorBuilder {
            config: self.config,
            worker: Arc::new(worker),
            _marker: PhantomData,
        }
    }
}

/// Fully configured fluent builder ready to spawn and register a [`GenericResourceActor`].
pub struct GenericResourceActorBuilder<T, E, W> {
    config: ActorConfig,
    worker: Arc<W>,
    _marker: PhantomData<(T, E)>,
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static, W: Worker<T, E>>
    GenericResourceActorBuilder<T, E, W>
{
    impl_actor_config_setters!();

    /// Builds an OTP-style [`ChildSpec`] that can be passed to a [`super::service_supervisor::ServiceSupervisor`].
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

        type ShutdownBinding<T, E> = (
            std::sync::Weak<Sender<ResourceCommand<T, E>>>,
            watch::Sender<bool>,
        );
        let active_shutdown_txs: Arc<std::sync::Mutex<Vec<ShutdownBinding<T, E>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let shutdown_txs_for_factory = active_shutdown_txs.clone();

        let factory: UntypedActorFactory = Arc::new(move |new_inst| {
            let (tx, rx) = mpsc::channel(channel_capacity);
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
            let w = worker.clone();
            let cfg = config.clone();
            let task_handle = tokio::spawn(async move {
                let actor =
                    GenericResourceActor::with_shutdown_rx(cfg, rx, shutdown_rx, new_inst, w);
                actor.run().await
            });
            (sender_arc, task_handle)
        });

        let shutdown_signal: ShutdownSignalFn = Arc::new(move |untyped| {
            if let Some(cmd_tx) = untyped.downcast_ref::<Sender<ResourceCommand<T, E>>>() {
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
                let _ = cmd_tx.try_send(ResourceCommand::Shutdown);
            }
        });

        ChildSpec::new(name, factory, restart_policy)
            .restart_type(restart_type)
            .shutdown_policy(shutdown_policy)
            .with_shutdown_signal(shutdown_signal)
    }

    /// Spawns the actor via ChildSpec on the given supervisor and returns the direct client handle.
    pub async fn spawn(
        self,
        supervisor: &ServiceSupervisorHandle,
    ) -> Result<ResourceClient<T, E>, ServiceError> {
        let name = self.config.name.clone();
        let spec = self.build_child_spec();
        supervisor.start_child(spec).await?;
        supervisor.get_client::<T, E>(&name).await
    }

    /// Spawns the actor via ChildSpec, registers it with the supervisor, and returns the direct client handle.
    pub async fn spawn_and_register(
        self,
        supervisor: &ServiceSupervisorHandle,
        _root: &RootSupervisorHandle,
    ) -> Result<ResourceClient<T, E>, ServiceError> {
        self.spawn(supervisor).await
    }
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static, W: Worker<T, E>>
    From<GenericResourceActorBuilder<T, E, W>> for ChildSpec
{
    fn from(builder: GenericResourceActorBuilder<T, E, W>) -> Self {
        builder.build_child_spec()
    }
}

// ============================================================================
// Generic Resource Actor Implementation
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

/// Generic, fault-tolerant actor managing cached resources, cold starts, and background refreshes.
pub struct GenericResourceActor<T = (), E = (), W = ()> {
    name: String,
    receiver: Receiver<ResourceCommand<T, E>>,
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

impl GenericResourceActor<(), (), ()> {
    /// Returns a new fluent builder to configure and spawn a generic resource actor.
    pub fn builder(name: impl Into<String>) -> ActorBuilder {
        ActorBuilder::new(name)
    }

    /// Alias for `builder`: Returns a new fluent builder to configure an actor ChildSpec.
    pub fn spec(name: impl Into<String>) -> ActorBuilder {
        ActorBuilder::new(name)
    }
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static, W: Worker<T, E>>
    GenericResourceActor<T, E, W>
{
    /// Initializes a generic resource actor instance with an explicit supervisor shutdown watch channel.
    pub(crate) fn with_shutdown_rx(
        config: ActorConfig,
        receiver: Receiver<ResourceCommand<T, E>>,
        shutdown_rx: watch::Receiver<bool>,
        instance_id: usize,
        worker: Arc<W>,
    ) -> Self {
        Self {
            name: config.name,
            receiver,
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

    /// Computes bounded pseudo-random jitter in milliseconds within `0..max_jitter`.
    fn compute_jitter_ms(&self, max_jitter: Duration) -> u64 {
        if max_jitter.is_zero() {
            return 0;
        }
        let max_ms = u64::try_from(max_jitter.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);

        // Hash name, instance_id, generation, and current wall-clock nanos to ensure
        // actors with different names or across time don't collide on the same jitter.
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
        let exp = self.failure_count.saturating_sub(1).min(9) as u32;
        let backoff = Duration::from_millis(100)
            .saturating_mul(1 << exp)
            .min(Duration::from_secs(30));

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

    /// Shorthand constructor spawning with default configuration.
    pub async fn spawn_and_register(
        name: impl Into<String>,
        worker: W,
        expiry_buffer: Duration,
        supervisor: &ServiceSupervisorHandle,
        root: &RootSupervisorHandle,
    ) -> Result<ResourceClient<T, E>, ServiceError> {
        ActorBuilder::new(name)
            .worker(worker)
            .expiry_buffer(expiry_buffer)
            .spawn_and_register(supervisor, root)
            .await
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
                        ResourceCommand::SimulateCrash => {
                            tracing::warn!(
                                actor = %self.name,
                                instance = self.instance_id,
                                "💥 CRASH COMMAND RECEIVED: Terminating actor with crash error!"
                            );
                            worker_tasks.abort_all();
                            self.waiters.clear();
                            return Err(format!(
                                "Intentional crash triggered via SimulateCrash on Actor '{}' #{}",
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

        // 1. Close mailbox so no new requests can be queued
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
        for waiter in self.waiters.drain(..) {
            let _ = waiter.send(Err(ResourceError::ActorUnavailable));
        }
        worker_tasks.abort_all();

        tracing::info!(
            actor = %self.name,
            instance = self.instance_id,
            "✅ Actor graceful shutdown complete."
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

        // 1. Fresh or still usable resource
        if let Some(res) = &self.current_resource {
            if res.is_still_usable() {
                let _ = respond_to.send(Ok(res.value.clone()));

                let should_refresh = match self.refresh_threshold {
                    Some(threshold) => Instant::now() >= threshold,
                    None => false,
                };

                if should_refresh && !self.refresh_in_flight {
                    if in_cooldown {
                        tracing::debug!(
                            actor = %self.name,
                            instance = self.instance_id,
                            "Resource approaching expiration, but cooldown is active; skipping background refresh."
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

                if !self.refresh_in_flight && !in_cooldown {
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
            self.waiters.clear();
            return Err(format!(
                "Actor '{}' #{} exceeded max_consecutive_failures ({}) after worker timeout",
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

    /// Updates cache with the worker output if generation matches, and broadcasts to parked waiters.
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
                self.refresh_threshold = expires_at.map(|exp| {
                    if exp <= now {
                        now
                    } else {
                        let ttl = exp - now;
                        let effective_buffer = self.expiry_buffer.min(ttl / 2);
                        exp.checked_sub(effective_buffer).unwrap_or(now)
                    }
                });
                self.current_resource = Some(new_resource);

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
                    self.waiters.clear();
                    return Err(format!(
                        "Actor '{}' #{} exceeded max_consecutive_failures ({})",
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
// Direct Generic Resource Client Handle (Data Plane)
// ============================================================================

/// High-performance, direct Data Plane client for any [`GenericResourceActor`].
#[derive(Clone)]
pub struct ResourceClient<T, E> {
    name: String,
    sender: Arc<std::sync::RwLock<Sender<ResourceCommand<T, E>>>>,
    supervisor: ServiceSupervisorHandle,
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static> ResourceClient<T, E> {
    /// Creates a new direct client handle.
    pub(crate) fn new(
        name: impl Into<String>,
        sender: Arc<std::sync::RwLock<Sender<ResourceCommand<T, E>>>>,
        supervisor: ServiceSupervisorHandle,
    ) -> Self {
        Self {
            name: name.into(),
            sender,
            supervisor,
        }
    }

    /// Returns the name of the monitored actor.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Resolves and updates the internal sender channel from the [`ServiceSupervisorHandle`].
    async fn rebind_channel(&self) -> Result<(), ResourceError<E>> {
        for retry in 0..4 {
            match self
                .supervisor
                .resolve_actor_channel::<Sender<ResourceCommand<T, E>>>(&self.name)
                .await
            {
                Ok(new_sender) if !new_sender.is_closed() => {
                    *self
                        .sender
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = new_sender;
                    tracing::info!(
                        actor = %self.name,
                        "🔄 Channel re-established with replacement actor! Retrying direct request..."
                    );
                    return Ok(());
                }
                Ok(_) => {
                    if retry == 0 {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                }
                Err(err) => {
                    tracing::error!(
                        actor = %self.name,
                        error = ?err,
                        "❌ Failed to rebind channel."
                    );
                    return Err(ResourceError::Service(err));
                }
            }
        }
        Err(ResourceError::ActorUnavailable)
    }

    /// Re-binds the direct channel immediately if the supervisor has already replaced the actor
    /// (or awaits an in-progress restart via `pending_resolutions`), falling back to a brief
    /// 60 ms delay only if the crash notification has not yet reached the supervisor.
    async fn backoff_and_rebind(&self) -> Result<(), ResourceError<E>> {
        const REBIND_BACKOFF: Duration = Duration::from_millis(60);
        for first_try in [true, false] {
            match self.rebind_channel().await {
                Ok(()) => return Ok(()),
                Err(ResourceError::Service(
                    err @ (ServiceError::MaxRestartsExceeded(_, _, _)
                    | ServiceError::ActorNotFound(_)
                    | ServiceError::ActorTypeMismatch(_)),
                )) => return Err(ResourceError::Service(err)),
                Err(_) if first_try => {
                    tokio::time::sleep(REBIND_BACKOFF).await;
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Sends a one-way command to the actor, transparently rebinding if the channel was closed by a prior crash.
    async fn send_control(
        &self,
        make_cmd: impl Fn() -> ResourceCommand<T, E>,
    ) -> Result<(), ResourceError<E>> {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_sender = self
                .sender
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();

            if current_sender.send(make_cmd()).await.is_ok() {
                return Ok(());
            }

            if attempt < MAX_ATTEMPTS {
                self.backoff_and_rebind().await?;
            }
        }

        Err(ResourceError::ActorUnavailable)
    }

    /// Retrieves the resource directly from the actor over its direct channel (1-hop).
    /// On channel disconnection (caused by an actor crash or restart), automatically rebinds via
    /// the [`ServiceSupervisorHandle`] and transparently retries.
    pub async fn get(&self) -> Result<T, ResourceError<E>> {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_sender = self
                .sender
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();

            let (tx, rx) = oneshot::channel();
            let send_res = current_sender
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
                    "⚠️ Direct channel broken or actor restarting. Rebinding via ServiceSupervisor..."
                );

                self.backoff_and_rebind().await?;
            }
        }

        Err(ResourceError::ActorUnavailable)
    }

    /// Retrieves the resource directly from the actor with a client-side timeout deadline.
    pub async fn get_timeout(&self, timeout: Duration) -> Result<T, ResourceError<E>> {
        tokio::time::timeout(timeout, self.get())
            .await
            .map_err(|_| ResourceError::Timeout(timeout))?
    }

    /// Proactively forces a background refresh, superseding any currently in-flight refresh.
    pub async fn force_refresh(&self) -> Result<(), ResourceError<E>> {
        self.send_control(|| ResourceCommand::ForceRefresh).await
    }

    /// Requests a graceful shutdown of the actor over its direct data-plane channel.
    pub async fn shutdown(&self) -> Result<(), ResourceError<E>> {
        self.send_control(|| ResourceCommand::Shutdown).await
    }

    /// Injects an intentional crash command directly into the actor.
    pub async fn simulate_crash(&self) -> Result<(), ResourceError<E>> {
        self.send_control(|| ResourceCommand::SimulateCrash).await
    }
}

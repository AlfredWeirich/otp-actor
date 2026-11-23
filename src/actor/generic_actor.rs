//! # Generic Resource & Worker Actor (`ResourceActor`)
//!
//! A completely reusable, fault-tolerant concurrent actor that executes any user-provided
//! [`Worker`] implementation or closure to fetch, cache, and proactively refresh arbitrary resources.
//!
//! ## Key Highlights
//! - **Zero-Hop Cache Reads**: Fresh cached resources are published to a [`tokio::sync::watch`] channel
//!   and served in 0 hops by [`ResourceClient::get`] without allocating a `oneshot` channel or traversing
//!   the actor mailbox.
//! - **Non-Intrusive Cache Health**: [`ResourceClient::cache_state`] classifies the same 0-hop cache
//!   ([`CacheState`]: `Fresh`, `RefreshDue`, `StaleUsable`, `Expired`, ...) without a mailbox hop and
//!   without ever exposing the cached value.
//! - **Zero-Cost Worker Futures**: Accepts custom structs, standalone `async fn`s, or closures returning
//!   native `impl Future` without `Box::pin` heap allocations.
//! - **Single-Flight Coalescing & Backpressure**: Concurrent cache-miss callers coalesce onto a single
//!   in-flight worker execution bounded by `max_waiters`.
//! - **Proactive Refresh, Negative Caching & Stale-While-Error**: Automatically refreshes resources
//!   before TTL expiration with jitter, enforces failure cooldowns, and serves stale values during transient outages.

use std::future::Future;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::Instrument;

use super::model::{
    EXPIRY_BUFFER, RestartPolicy, RestartType, ShutdownPolicy, SupervisorError, TaskResult,
};
use super::stateful_actor::{QueueStats, queue_stats_from_sender};
use super::supervisor::{
    CacheProbeFn, ChildSpec, QueueProbeFn, ShutdownSignalFn, SlotState, SpawnContext,
    SupervisorHandle, UntypedActorFactory,
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

/// Non-generic span constructor so all `ResourceActor` types share a single `tracing` callsite.
fn make_resource_actor_span(name: &str, instance_id: usize) -> tracing::Span {
    tracing::info_span!(
        "resource_actor",
        actor = %name,
        instance = instance_id,
    )
}

/// Non-generic span constructor so all `ResourceActor` workers share a single `tracing` callsite.
fn make_resource_worker_span(name: &str, instance_id: usize, generation: usize) -> tracing::Span {
    tracing::info_span!(
        "resource_worker",
        actor = %name,
        instance = instance_id,
        generation = generation,
    )
}

/// Execution metadata provided to [`Worker`] implementations.
#[non_exhaustive]
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

    /// Indicates whether the resource is still valid at timestamp `now`.
    #[inline]
    pub(crate) fn is_still_usable_at(&self, now: Instant) -> bool {
        self.expires_at.is_none_or(|exp| now < exp)
    }

    /// Indicates whether the resource is usable within a stale-while-error grace window at `now`.
    #[inline]
    pub(crate) fn is_usable_stale_at(&self, now: Instant, stale_allowance: Duration) -> bool {
        if self.is_still_usable_at(now) {
            return true;
        }
        self.expires_at.is_none_or(|exp| {
            exp.checked_add(stale_allowance)
                .is_none_or(|deadline| now < deadline)
        })
    }

    /// Indicates whether the resource is still valid for immediate use.
    pub fn is_still_usable(&self) -> bool {
        self.is_still_usable_at(Instant::now())
    }

    /// Indicates whether the resource is usable within a stale-while-error grace window.
    pub fn is_usable_stale(&self, stale_allowance: Duration) -> bool {
        self.is_usable_stale_at(Instant::now(), stale_allowance)
    }

    /// Indicates whether more than `buffer` remains before `expires_at` (or always `true` if perpetual).
    ///
    /// Note that `ResourceActor` internally clamps its refresh buffer to `min(expiry_buffer, ttl / 2)`;
    /// prefer [`ResourceClient::cache_state`] when inspecting the actor's effective cache state.
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
    /// `true` while a background refresh trigger is queued or a refresh worker is running.
    pub(crate) refresh_pending: Arc<AtomicBool>,
    /// Monotonic deadline (ns since `epoch`) until which background refreshes are throttled (`0` = none).
    pub(crate) throttle_until_ns: Arc<AtomicU64>,
    /// Monotonic reference instant captured when this endpoint was created.
    pub(crate) epoch: Instant,
    /// Configured stale-while-error grace window, used to classify expired cache entries.
    pub(crate) stale_while_error: Duration,
}

impl<T, E> Clone for ResourceEndpoint<T, E> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            cache_rx: self.cache_rx.clone(),
            invalidated: self.invalidated.clone(),
            refresh_pending: self.refresh_pending.clone(),
            throttle_until_ns: self.throttle_until_ns.clone(),
            epoch: self.epoch,
            stale_while_error: self.stale_while_error,
        }
    }
}

impl<T, E> ResourceEndpoint<T, E> {
    /// Returns `true` if background refreshes are currently throttled by cooldown, backoff, or retry delay.
    #[inline]
    pub(crate) fn is_refresh_throttled_at(&self, now: Instant) -> bool {
        let deadline_ns = self.throttle_until_ns.load(Ordering::Acquire);
        if deadline_ns == 0 {
            return false;
        }
        let elapsed_ns = u64::try_from(now.saturating_duration_since(self.epoch).as_nanos())
            .unwrap_or(u64::MAX);
        elapsed_ns < deadline_ns
    }

    /// Classifies the locally visible cache without any channel round-trip (see [`CacheState`]).
    pub(crate) fn cache_state(&self) -> CacheState {
        if self.invalidated.load(Ordering::Acquire) || self.sender.is_closed() {
            return CacheState::Invalidated;
        }

        let cached = self.cache_rx.borrow();
        let Some(snapshot) = cached.as_ref() else {
            return CacheState::Empty;
        };

        let resource = &snapshot.resource;
        if resource.expires_at.is_none() && snapshot.refresh_threshold.is_none() {
            return CacheState::Fresh { expires_in: None };
        }

        let now = Instant::now();

        if resource.is_still_usable_at(now) {
            let expires_in = resource
                .expires_at
                .map(|exp| exp.saturating_duration_since(now));
            let refresh_due = snapshot.refresh_threshold.is_some_and(|t| now >= t);
            return if refresh_due {
                CacheState::RefreshDue { expires_in }
            } else {
                CacheState::Fresh { expires_in }
            };
        }

        let expired_for = resource
            .expires_at
            .map_or(Duration::ZERO, |exp| now.saturating_duration_since(exp));
        if self.stale_while_error > Duration::ZERO
            && resource.is_usable_stale_at(now, self.stale_while_error)
        {
            CacheState::StaleUsable { expired_for }
        } else {
            CacheState::Expired { expired_for }
        }
    }
}

/// Point-in-time classification of a [`ResourceClient`]'s locally cached resource.
///
/// Returned synchronously by [`ResourceClient::cache_state`], which reads the same 0-hop `watch`
/// cache as [`ResourceClient::get`] without crossing the actor mailbox, contacting the supervisor,
/// or triggering a refresh. The cached value itself is never exposed, only its metadata, so the
/// state is always safe to log or export as a health metric.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "remote", derive(serde::Serialize, serde::Deserialize))]
pub enum CacheState {
    /// No resource has been cached yet (cold start, before the first successful worker run).
    Empty,
    /// A fresh resource is cached outside the pre-expiration buffer and is served in 0 hops.
    Fresh {
        /// Remaining validity, or `None` for a perpetual resource ([`TimedResource::perpetual`]).
        expires_in: Option<Duration>,
    },
    /// The resource is still valid inside its pre-expiration buffer: [`ResourceClient::get`] still
    /// serves it in 0 hops and triggers at most one non-blocking background refresh when none is pending or throttled.
    RefreshDue {
        /// Remaining validity, or `None` for a perpetual resource.
        expires_in: Option<Duration>,
    },
    /// The resource has expired but is still inside the configured stale-while-error window,
    /// so [`ResourceClient::get`] serves it (via the actor) while a background refresh recovers.
    StaleUsable {
        /// Time elapsed since the resource expired.
        expired_for: Duration,
    },
    /// The resource has expired beyond any stale-while-error allowance: the next
    /// [`ResourceClient::get`] must wait for a fresh worker result.
    Expired {
        /// Time elapsed since the resource expired.
        expired_for: Duration,
    },
    /// The cached endpoint is invalidated or its actor has exited (shutdown, crash, or restart
    /// pending). The next [`ResourceClient::get`] transparently rebinds to the replacement
    /// instance. As with `ActorClient::queue_stats`, this also reports the old instance after a
    /// crash until the client's next call rebinds it.
    Invalidated,
}

impl CacheState {
    /// Returns `true` if the cached resource is outside its pre-expiration buffer (`CacheState::Fresh`).
    #[must_use]
    pub fn is_fresh(&self) -> bool {
        matches!(self, Self::Fresh { .. })
    }

    /// Returns `true` if a cached value could currently be returned to callers
    /// (`Fresh`, `RefreshDue`, or `StaleUsable`).
    #[must_use]
    pub fn has_usable_value(&self) -> bool {
        matches!(
            self,
            Self::Fresh { .. } | Self::RefreshDue { .. } | Self::StaleUsable { .. }
        )
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
    /// Fire-and-forget trigger sent from the 0-hop `RefreshDue` fast path to start at most one
    /// background refresh without allocating a `oneshot` or stalling the caller.
    TriggerBackgroundRefresh,
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

impl<T, E, W> std::fmt::Debug for ResourceActorBuilder<T, E, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceActorBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ResourceActorBuilder<(), (), ()> {
    /// Creates a new [`ResourceActorBuilder`] initialized with production-safe defaults.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            config: ResourceActorConfig::new(name),
            worker: (),
            _marker: PhantomData,
        }
    }

    /// Creates a new [`ResourceActorBuilder`] initialized from an explicit [`ResourceActorConfig`].
    ///
    /// `channel_capacity` and `max_waiters` are clamped to `>= 1`.
    #[must_use]
    pub fn from_config(mut config: ResourceActorConfig) -> Self {
        config.channel_capacity = config.channel_capacity.max(1);
        config.max_waiters = config.max_waiters.max(1);
        Self {
            config,
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
    /// Replaces the builder's [`ResourceActorConfig`] while preserving any attached [`Worker`].
    ///
    /// `channel_capacity` and `max_waiters` are clamped to `>= 1`.
    #[must_use]
    pub fn with_config(mut self, mut config: ResourceActorConfig) -> Self {
        config.channel_capacity = config.channel_capacity.max(1);
        config.max_waiters = config.max_waiters.max(1);
        self.config = config;
        self
    }

    /// Returns a shared reference to the current [`ResourceActorConfig`].
    #[must_use]
    pub fn config(&self) -> &ResourceActorConfig {
        &self.config
    }

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
    #[must_use]
    pub fn build_child_spec(self) -> ChildSpec {
        let mut config = self.config;
        config.channel_capacity = config.channel_capacity.max(1);
        config.max_waiters = config.max_waiters.max(1);
        let worker = self.worker;
        let channel_capacity = config.channel_capacity;
        let restart_policy = config.restart_policy;
        let restart_type = config.restart_type;
        let shutdown_policy = config.shutdown_policy;
        let stale_while_error = config.stale_while_error;
        let name = config.name.clone();

        let factory: UntypedActorFactory = Arc::new(move |spawn_ctx: SpawnContext| {
            let (tx, rx) = mpsc::channel(channel_capacity);
            let (cache_tx, cache_rx) = watch::channel(None);
            let invalidated = Arc::new(AtomicBool::new(false));
            let refresh_pending = Arc::new(AtomicBool::new(false));
            let throttle_until_ns = Arc::new(AtomicU64::new(0));
            let epoch = Instant::now();
            let endpoint = Arc::new(ResourceEndpoint {
                sender: tx,
                cache_rx,
                invalidated: invalidated.clone(),
                refresh_pending: refresh_pending.clone(),
                throttle_until_ns: throttle_until_ns.clone(),
                epoch,
                stale_while_error,
            });
            let w = worker.clone();
            let cfg = config.clone();
            let actor_span = make_resource_actor_span(&cfg.name, spawn_ctx.instance_id);
            let fut = Box::pin(
                async move {
                    let actor = ResourceActor::with_channels(
                        cfg,
                        rx,
                        cache_tx,
                        invalidated,
                        spawn_ctx.shutdown_rx,
                        spawn_ctx.instance_id,
                        w,
                    )
                    .with_refresh_sync(refresh_pending, throttle_until_ns, epoch);
                    actor.run().await
                }
                .instrument(actor_span),
            );
            (endpoint, fut)
        });

        let shutdown_signal: ShutdownSignalFn = Arc::new(|untyped| {
            if let Some(ep) = untyped.downcast_ref::<ResourceEndpoint<T, E>>() {
                ep.invalidated.store(true, Ordering::Release);
                let _ = ep.sender.try_send(ResourceCommand::Shutdown);
            }
        });

        let queue_probe: QueueProbeFn = Arc::new(|untyped| {
            untyped
                .downcast_ref::<ResourceEndpoint<T, E>>()
                .map(|ep| queue_stats_from_sender(&ep.sender))
        });

        let cache_probe: CacheProbeFn = Arc::new(|untyped| {
            untyped
                .downcast_ref::<ResourceEndpoint<T, E>>()
                .map(ResourceEndpoint::cache_state)
        });

        #[allow(unused_mut)]
        let mut spec = ChildSpec::new(name, factory, restart_policy)
            .restart_type(restart_type)
            .shutdown_policy(shutdown_policy)
            .with_shutdown_signal(shutdown_signal)
            .with_queue_probe(queue_probe)
            .with_cache_probe(cache_probe);

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
    refresh_pending: Arc<AtomicBool>,
    throttle_until_ns: Arc<AtomicU64>,
    epoch: Instant,
    shutdown_rx: watch::Receiver<bool>,
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
            refresh_pending: Arc::new(AtomicBool::new(false)),
            throttle_until_ns: Arc::new(AtomicU64::new(0)),
            epoch: Instant::now(),
            shutdown_rx,
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
            next_proactive_target: None,
            current_worker_abort: None,
        }
    }

    /// Attaches the endpoint's shared 0-hop refresh synchronization atomics and monotonic epoch.
    pub(crate) fn with_refresh_sync(
        mut self,
        refresh_pending: Arc<AtomicBool>,
        throttle_until_ns: Arc<AtomicU64>,
        epoch: Instant,
    ) -> Self {
        self.refresh_pending = refresh_pending;
        self.throttle_until_ns = throttle_until_ns;
        self.epoch = epoch;
        self
    }

    /// Publishes the active failure cooldown, backoff, and retry target deadline for 0-hop checks.
    fn sync_throttle_deadline(&self) {
        let Some((failed_at, _)) = &self.last_failure else {
            self.throttle_until_ns.store(0, Ordering::Release);
            return;
        };

        let max_delay = self.failure_cooldown.max(self.failure_backoff_delay());
        let Some(mut deadline) = failed_at.checked_add(max_delay) else {
            self.throttle_until_ns.store(u64::MAX, Ordering::Release);
            return;
        };
        if let Some(target) = self.next_proactive_target {
            deadline = deadline.max(target);
        }

        let ns = u64::try_from(deadline.saturating_duration_since(self.epoch).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        self.throttle_until_ns.store(ns, Ordering::Release);
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
        self.sync_throttle_deadline();
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
                                self.refresh_pending.store(false, Ordering::Release);
                                self.waiters.clear();
                                return Err(format!("Worker panic escalated to supervisor: {:?}", join_err));
                            } else if join_err.is_cancelled() {
                                if !self.refresh_in_flight {
                                    self.refresh_pending.store(false, Ordering::Release);
                                }
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
                        self.refresh_pending.store(false, Ordering::Release);
                        return Ok(());
                    };

                    match command {
                        ResourceCommand::GetResource { respond_to } => {
                            self.handle_get_resource(respond_to, &mut worker_tasks);
                        }
                        ResourceCommand::TriggerBackgroundRefresh => {
                            self.handle_trigger_background_refresh(&mut worker_tasks);
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
                            self.throttle_until_ns.store(0, Ordering::Release);
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
                            self.refresh_pending.store(false, Ordering::Release);
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
                let cached_val = {
                    let cached = self.cache_tx.borrow();
                    cached.as_ref().and_then(|snap| {
                        let now = Instant::now();
                        let res = &snap.resource;
                        if res.is_still_usable_at(now)
                            || (self.stale_while_error > Duration::ZERO
                                && res.is_usable_stale_at(now, self.stale_while_error))
                        {
                            Some(res.value.clone())
                        } else {
                            None
                        }
                    })
                };
                if let Some(val) = cached_val {
                    let _ = respond_to.send(Ok(val));
                    continue;
                }

                self.waiters.retain(|w| !w.is_closed());
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
                        tracing::error!(
                            actor = %self.name,
                            instance = self.instance_id,
                            error = %join_err,
                            "💥 In-flight worker panicked during graceful shutdown!"
                        );
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
        self.refresh_pending.store(false, Ordering::Release);
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

    /// Handles non-blocking 0-hop background refresh notifications sent from [`ResourceClient::get`].
    fn handle_trigger_background_refresh(
        &mut self,
        worker_tasks: &mut JoinSet<WorkerOutcome<T, E>>,
    ) {
        if self.refresh_in_flight {
            return;
        }

        let needs_refresh = {
            let cached = self.cache_tx.borrow();
            match cached.as_ref() {
                Some(snap) => {
                    let now = Instant::now();
                    let res = &snap.resource;
                    if res.is_still_usable_at(now) {
                        snap.refresh_threshold.is_some_and(|t| now >= t)
                    } else {
                        self.stale_while_error > Duration::ZERO
                            && res.is_usable_stale_at(now, self.stale_while_error)
                    }
                }
                None => false,
            }
        };

        if !needs_refresh || self.is_background_refresh_throttled() {
            if self.last_failure.is_some() {
                self.sync_throttle_deadline();
            }
            self.refresh_pending.store(false, Ordering::Release);
            return;
        }

        self.trigger_background_worker(worker_tasks);
    }

    /// Handles client get requests: Serves valid caches immediately; triggers background refresh.
    fn handle_get_resource(
        &mut self,
        respond_to: oneshot::Sender<Result<T, ResourceError<E>>>,
        worker_tasks: &mut JoinSet<WorkerOutcome<T, E>>,
    ) {
        enum CacheLookup<T> {
            Usable { value: T, should_refresh: bool },
            Stale { value: T },
            Miss { had_cached: bool },
        }

        let in_cooldown = self.is_in_failure_cooldown();
        let bg_throttled = self.is_background_refresh_throttled();

        let lookup = {
            let cached = self.cache_tx.borrow();
            match cached.as_ref() {
                Some(snap) => {
                    let now = Instant::now();
                    let res = &snap.resource;
                    if res.is_still_usable_at(now) {
                        let should_refresh = snap.refresh_threshold.is_some_and(|t| now >= t);
                        CacheLookup::Usable {
                            value: res.value.clone(),
                            should_refresh,
                        }
                    } else if self.stale_while_error > Duration::ZERO
                        && res.is_usable_stale_at(now, self.stale_while_error)
                    {
                        CacheLookup::Stale {
                            value: res.value.clone(),
                        }
                    } else {
                        CacheLookup::Miss { had_cached: true }
                    }
                }
                None => CacheLookup::Miss { had_cached: false },
            }
        };

        let had_cached = match lookup {
            // 1. Fresh or still usable resource
            CacheLookup::Usable {
                value,
                should_refresh,
            } => {
                let _ = respond_to.send(Ok(value));

                if should_refresh && !self.refresh_in_flight {
                    if bg_throttled {
                        self.sync_throttle_deadline();
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
            CacheLookup::Stale { value } => {
                tracing::warn!(
                    actor = %self.name,
                    instance = self.instance_id,
                    allowance = ?self.stale_while_error,
                    "⚠️ Resource expired! Serving STALE cache within stale_while_error window."
                );
                let _ = respond_to.send(Ok(value));

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
            CacheLookup::Miss { had_cached } => had_cached,
        };

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

        let reason = if had_cached {
            "Hard Expiry (cached resource expired!)"
        } else {
            "Cold Start (no resource in cache)"
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
        self.refresh_pending.store(true, Ordering::Release);
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

        let worker_span = make_resource_worker_span(&actor_name, actor_id, next_gen);

        let abort_handle = worker_tasks.spawn(
            async move {
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
            }
            .instrument(worker_span),
        );
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
            self.refresh_pending.store(false, Ordering::Release);
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
        // Publish throttle deadline before clearing `refresh_pending` so concurrent 0-hop readers
        // see the updated backoff window immediately.
        self.sync_throttle_deadline();
        self.refresh_pending.store(false, Ordering::Release);
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
                self.throttle_until_ns.store(0, Ordering::Release);

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
                self.cache_tx.send_replace(Some(ResourceSnapshot {
                    resource: new_resource,
                    refresh_threshold: threshold,
                }));

                if self.proactive_refresh {
                    self.schedule_proactive_refresh(expires_at);
                }

                // Publish the refreshed watch snapshot first before clearing `refresh_pending`.
                self.refresh_pending.store(false, Ordering::Release);

                if !self.waiters.is_empty() {
                    tracing::debug!(
                        actor = %self.name,
                        instance = self.instance_id,
                        waiters = self.waiters.len(),
                        "📢 Broadcasting value to waiting tasks."
                    );
                    let cached = self.cache_tx.borrow();
                    if let Some(snapshot) = cached.as_ref() {
                        for waiter in self.waiters.drain(..) {
                            let _ = waiter.send(Ok(snapshot.resource.value.clone()));
                        }
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
                    self.refresh_pending.store(false, Ordering::Release);
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
                // Publish throttle deadline before clearing `refresh_pending`.
                self.sync_throttle_deadline();
                self.refresh_pending.store(false, Ordering::Release);
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
    name: Arc<str>,
    endpoint: Arc<RwLock<ResourceEndpoint<T, E>>>,
    slot_rx: Arc<RwLock<watch::Receiver<SlotState>>>,
    supervisor: SupervisorHandle,
}

impl<T, E> std::fmt::Debug for ResourceClient<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache_state = self
            .endpoint
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cache_state();
        f.debug_struct("ResourceClient")
            .field("name", &self.name)
            .field("supervisor", &self.supervisor.name())
            .field("cache_state", &cache_state)
            .finish()
    }
}

impl<T: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static> ResourceClient<T, E> {
    /// Creates a new direct client handle subscribed to the child's [`SlotState`] watch channel.
    pub(crate) fn new(
        name: impl Into<Arc<str>>,
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
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the [`SupervisorHandle`] managing this actor.
    #[must_use]
    pub fn supervisor(&self) -> &SupervisorHandle {
        &self.supervisor
    }

    /// Returns the name of the [`SupervisorHandle`] managing this actor.
    #[must_use]
    pub fn service_name(&self) -> &str {
        self.supervisor.name()
    }

    /// Returns an approximate point-in-time snapshot of this client's bounded mailbox queue.
    ///
    /// Reads only the locally cached sender without contacting the supervisor or locking across `.await`.
    #[must_use]
    pub fn queue_stats(&self) -> QueueStats {
        let ep = self
            .endpoint
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue_stats_from_sender(&ep.sender)
    }

    fn current_sender(&self) -> Sender<ResourceCommand<T, E>> {
        self.endpoint
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sender
            .clone()
    }

    /// Reactively waits on the [`SlotState`] watch channel until a replacement [`ResourceActor`] instance is running,
    /// or returns immediately if the actor was stopped or permanently failed.
    async fn rebind_channel(
        &self,
        failed_sender: &Sender<ResourceCommand<T, E>>,
    ) -> Result<(), ResourceError<E>> {
        {
            let current = self
                .endpoint
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
                            SupervisorError::ChildTypeMismatch(self.name.to_string()),
                        ));
                    }
                    SlotState::Failed(err) => return Err(ResourceError::Supervisor(err.clone())),
                    SlotState::Stopped => {
                        return Err(ResourceError::Supervisor(SupervisorError::ChildNotFound(
                            self.name.to_string(),
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

        for _attempt in 1..=MAX_ATTEMPTS {
            let current_sender = {
                let current_ep = self
                    .endpoint
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if invalidate_cache {
                    current_ep.invalidated.store(true, Ordering::Release);
                }
                current_ep.sender.clone()
            };

            if current_sender.send(make_cmd()).await.is_ok() {
                return Ok(());
            }

            self.rebind_channel(&current_sender).await?;
        }

        Err(ResourceError::ActorUnavailable)
    }

    /// Retrieves the resource:
    /// - **0-Hop Fast Path**: If the actor holds a fresh or still-usable pre-expiration (`RefreshDue`)
    ///   cached resource, returns a clone directly from the [`tokio::sync::watch`] channel without any
    ///   `oneshot` allocation or mailbox waiting (firing at most one non-blocking
    ///   `TriggerBackgroundRefresh` when in `RefreshDue` and not already pending or throttled).
    /// - **1-Hop Coalesced Path**: On cold start, hard expiry, or stale-while-error recovery,
    ///   sends a direct request to the [`ResourceActor`] and transparently rebinds via its internal `SlotState` watch channel on crash/restart.
    #[tracing::instrument(skip(self), fields(actor = %self.name))]
    pub async fn get(&self) -> Result<T, ResourceError<E>> {
        // 0-hop fast path via watch::Receiver (read directly under RwLockReadGuard with zero
        // ResourceEndpoint atomic refcount clones and at most one `Instant::now()` syscall).
        {
            let current_ep = self
                .endpoint
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !current_ep.invalidated.load(Ordering::Acquire)
                && !current_ep.sender.is_closed()
                && let Some(snapshot) = current_ep.cache_rx.borrow().as_ref()
            {
                let (usable, refresh_due_now) =
                    match (snapshot.resource.expires_at, snapshot.refresh_threshold) {
                        (None, None) => (true, None),
                        _ => {
                            let now = Instant::now();
                            if !snapshot.resource.is_still_usable_at(now) {
                                (false, None)
                            } else if snapshot
                                .refresh_threshold
                                .is_some_and(|threshold| now >= threshold)
                            {
                                (true, Some(now))
                            } else {
                                (true, None)
                            }
                        }
                    };
                if usable {
                    if let Some(now) = refresh_due_now
                        && !current_ep.refresh_pending.load(Ordering::Acquire)
                        && !current_ep.is_refresh_throttled_at(now)
                        && current_ep
                            .refresh_pending
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                    {
                        match current_ep
                            .sender
                            .try_send(ResourceCommand::TriggerBackgroundRefresh)
                        {
                            Ok(()) => return Ok(snapshot.resource.value.clone()),
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                current_ep.refresh_pending.store(false, Ordering::Release);
                                return Ok(snapshot.resource.value.clone());
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                current_ep.refresh_pending.store(false, Ordering::Release);
                                // Fall through to the 1-hop rebinding loop below.
                            }
                        }
                    } else {
                        return Ok(snapshot.resource.value.clone());
                    }
                }
            }
        }

        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let current_sender = self.current_sender();

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

            tracing::warn!(
                actor = %self.name,
                attempt,
                max_attempts = MAX_ATTEMPTS,
                "⚠️ Direct channel broken or actor restarting. Rebinding via reactive watch slot..."
            );

            self.rebind_channel(&current_sender).await?;
        }

        Err(ResourceError::ActorUnavailable)
    }

    /// Retrieves the resource with a client-side timeout deadline.
    #[tracing::instrument(skip(self), fields(actor = %self.name))]
    pub async fn get_timeout(&self, timeout: Duration) -> Result<T, ResourceError<E>> {
        tokio::time::timeout(timeout, self.get())
            .await
            .map_err(|_| ResourceError::Timeout(timeout))?
    }

    /// Returns a point-in-time [`CacheState`] classification of this client's cached resource.
    ///
    /// Synchronous and lock-free with respect to the actor: it reads the same 0-hop `watch` cache
    /// as [`Self::get`] and never sends a command, contacts the supervisor, waits for a worker, or
    /// triggers a refresh, so it is safe to call from health probes and metrics collectors at any
    /// rate. The cached value itself is never exposed.
    ///
    /// The result is an instantaneous observation and may change immediately after it returns.
    /// After an actor crash and *before* the client's next `get`/control call rebinds it, this
    /// still inspects the previous instance and reports [`CacheState::Invalidated`].
    pub fn cache_state(&self) -> CacheState {
        self.endpoint
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cache_state()
    }

    /// Proactively forces a background refresh, superseding any currently in-flight refresh.
    #[tracing::instrument(skip(self), fields(actor = %self.name))]
    pub async fn force_refresh(&self) -> Result<(), ResourceError<E>> {
        self.send_control(false, || ResourceCommand::ForceRefresh)
            .await
    }

    /// Requests a graceful shutdown of the actor over its direct data-plane channel.
    #[tracing::instrument(skip(self), fields(actor = %self.name))]
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

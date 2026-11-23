//! gRPC Server hosting the `ActorNode` service with 2-phase coordinated graceful shutdown.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::actor::{
    DEFAULT_SNAPSHOT_TIMEOUT, HealthState, LocalNodeMonitor, NodeHealth, NodeStatus, NodeSummary,
    RemoteMetricsSnapshot, StatusOptions, SupervisorError, SupervisorHandle,
};
use crate::remote::endpoint::{MAX_GRPC_FRAME_BYTES, REMOTE_PROTOCOL_VERSION, encode_payload};
use crate::remote::error::RemoteNodeError;
use crate::remote::proto::call_response::Result as CallResult;
use crate::remote::proto::remote_error::Kind;
use crate::remote::proto::status_response::Result as StatusResult;
use crate::remote::proto::{
    ActorNode, ActorNodeAdmin, ActorNodeAdminServer, ActorNodeServer, CallRequest, CallResponse,
    CastRequest, CastResponse, HealthRequest, HealthResponse, RemoteError, StatusRequest,
    StatusResponse,
};
use crate::remote::registry::{ActorNodeRegistry, sanitize_label};

/// Default server-side call timeout applied when a remote `CallRequest` omits `timeout_ms` (`30s`).
pub const DEFAULT_REMOTE_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum allowed per-request timeout on the server side (`300s`).
pub const MAX_REMOTE_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Request-level authorization callback invoked by [`ActorNodeService`] before routing to an actor.
///
/// Receives the incoming gRPC [`tonic::metadata::MetadataMap`], request [`tonic::Extensions`],
/// `service_name`, and `actor_name`. Return `Err(Status::unauthenticated(...))` or
/// `Err(Status::permission_denied(...))` to reject unauthorized callers.
pub type Authorizer = Arc<
    dyn Fn(&tonic::metadata::MetadataMap, &tonic::Extensions, &str, &str) -> Result<(), Status>
        + Send
        + Sync,
>;

#[derive(Debug, Default)]
struct RemoteNodeMetricsInner {
    grpc_in_flight: AtomicUsize,
    grpc_calls_total: AtomicU64,
    grpc_casts_total: AtomicU64,
    auth_rejected_total: AtomicU64,
    service_not_found_total: AtomicU64,
    actor_not_found_total: AtomicU64,
    incompatible_protocol_total: AtomicU64,
    framework_errors_total: AtomicU64,
}

#[derive(Clone, Default)]
pub(crate) struct RemoteNodeMetrics {
    inner: Arc<RemoteNodeMetricsInner>,
}

struct GrpcInFlightGuard<'a> {
    inner: &'a RemoteNodeMetricsInner,
}

impl Drop for GrpcInFlightGuard<'_> {
    fn drop(&mut self) {
        self.inner.grpc_in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl RemoteNodeMetrics {
    fn enter_in_flight(&self) -> GrpcInFlightGuard<'_> {
        self.inner.grpc_in_flight.fetch_add(1, Ordering::Relaxed);
        GrpcInFlightGuard { inner: &self.inner }
    }

    fn record_call(&self) {
        self.inner.grpc_calls_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_cast(&self) {
        self.inner.grpc_casts_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_auth_rejected(&self) {
        self.inner
            .auth_rejected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_remote_error(&self, err: &RemoteError) {
        self.inner
            .framework_errors_total
            .fetch_add(1, Ordering::Relaxed);
        match Kind::try_from(err.kind) {
            Ok(Kind::ServiceNotFound) => {
                self.inner
                    .service_not_found_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(Kind::ActorNotFound) => {
                self.inner
                    .actor_not_found_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(Kind::IncompatibleProtocol) => {
                self.inner
                    .incompatible_protocol_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub(crate) fn snapshot(&self, registered_endpoints: usize) -> RemoteMetricsSnapshot {
        RemoteMetricsSnapshot {
            registered_endpoints,
            grpc_in_flight: self.inner.grpc_in_flight.load(Ordering::Relaxed),
            grpc_calls_total: self.inner.grpc_calls_total.load(Ordering::Relaxed),
            grpc_casts_total: self.inner.grpc_casts_total.load(Ordering::Relaxed),
            auth_rejected_total: self.inner.auth_rejected_total.load(Ordering::Relaxed),
            service_not_found_total: self.inner.service_not_found_total.load(Ordering::Relaxed),
            actor_not_found_total: self.inner.actor_not_found_total.load(Ordering::Relaxed),
            incompatible_protocol_total: self
                .inner
                .incompatible_protocol_total
                .load(Ordering::Relaxed),
            framework_errors_total: self.inner.framework_errors_total.load(Ordering::Relaxed),
        }
    }
}

fn clamp_timeout(
    client_timeout_ms: Option<u64>,
    fallback: Option<Duration>,
    max_request_timeout: Option<Duration>,
) -> Option<Duration> {
    let raw = client_timeout_ms.map(Duration::from_millis).or(fallback);
    match (raw, max_request_timeout) {
        (Some(d), Some(max)) => Some(d.min(max)),
        (other, _) => other,
    }
}

/// Implementation of the generated `ActorNode` gRPC service.
#[derive(Clone)]
pub struct ActorNodeService {
    registry: ActorNodeRegistry,
    default_call_timeout: Option<Duration>,
    default_cast_timeout: Option<Duration>,
    max_request_timeout: Option<Duration>,
    authorizer: Option<Authorizer>,
    draining: Arc<AtomicBool>,
    metrics: RemoteNodeMetrics,
}

impl std::fmt::Debug for ActorNodeService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorNodeService")
            .field("registered_endpoints", &self.registry.len())
            .field("default_call_timeout", &self.default_call_timeout)
            .field("default_cast_timeout", &self.default_cast_timeout)
            .field("max_request_timeout", &self.max_request_timeout)
            .field("draining", &self.draining.load(Ordering::Acquire))
            .field("has_authorizer", &self.authorizer.is_some())
            .finish_non_exhaustive()
    }
}

impl ActorNodeService {
    /// Creates a new service wrapping the given registry with server-side timeout defaults
    /// ([`DEFAULT_REMOTE_CALL_TIMEOUT`] and [`MAX_REMOTE_REQUEST_TIMEOUT`]).
    pub fn new(registry: ActorNodeRegistry) -> Self {
        Self {
            registry,
            default_call_timeout: Some(DEFAULT_REMOTE_CALL_TIMEOUT),
            default_cast_timeout: None,
            max_request_timeout: Some(MAX_REMOTE_REQUEST_TIMEOUT),
            authorizer: None,
            draining: Arc::new(AtomicBool::new(false)),
            metrics: RemoteNodeMetrics::default(),
        }
    }

    /// Sets the fallback timeout for `call` requests that do not specify `timeout_ms`.
    #[must_use]
    pub fn with_default_call_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.default_call_timeout = timeout;
        self
    }

    /// Sets the fallback timeout for `cast` requests that do not specify `timeout_ms`.
    ///
    /// When set to `Some(Duration::ZERO)`, casts without an explicit timeout perform non-blocking
    /// `try_cast` load-shedding (`MailboxFull`) instead of waiting up to
    /// [`crate::remote::endpoint::DEFAULT_REMOTE_CAST_TIMEOUT`].
    #[must_use]
    pub fn with_default_cast_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.default_cast_timeout = timeout;
        self
    }

    /// Sets the upper bound on `timeout_ms` accepted from remote clients.
    #[must_use]
    pub fn with_max_request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.max_request_timeout = timeout;
        self
    }

    /// Attaches a request-level [`Authorizer`] hook checked before resolving endpoints.
    #[must_use]
    pub fn with_authorizer(mut self, authorizer: Authorizer) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    pub(crate) fn with_draining(mut self, draining: Arc<AtomicBool>) -> Self {
        self.draining = draining;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: RemoteNodeMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Returns a point-in-time snapshot of gRPC transport counters for this service.
    pub fn metrics(&self) -> RemoteMetricsSnapshot {
        self.metrics.snapshot(self.registry.len())
    }

    fn effective_timeout(
        &self,
        client_timeout_ms: Option<u64>,
        fallback: Option<Duration>,
    ) -> Option<Duration> {
        clamp_timeout(client_timeout_ms, fallback, self.max_request_timeout)
    }

    fn check_not_draining(&self) -> Result<(), RemoteError> {
        if self.draining.load(Ordering::Acquire) {
            let err = RemoteError {
                kind: Kind::ActorUnavailable as i32,
                message: "Node is draining for shutdown".into(),
            };
            self.metrics.record_remote_error(&err);
            return Err(err);
        }
        Ok(())
    }
}

/// Executes an optional [`Authorizer`] hook and records rejected attempts in [`RemoteNodeMetrics`].
fn authorize_or_record(
    authorizer: &Option<Authorizer>,
    metrics: &RemoteNodeMetrics,
    metadata: &tonic::metadata::MetadataMap,
    extensions: &tonic::Extensions,
    service_name: &str,
    actor_name: &str,
) -> Result<(), Status> {
    if let Some(auth) = authorizer
        && let Err(status) = auth(metadata, extensions, service_name, actor_name)
    {
        metrics.record_auth_rejected();
        return Err(status);
    }
    Ok(())
}

/// Validates the incoming wire protocol version and records mismatches in [`RemoteNodeMetrics`].
fn check_protocol_version(
    protocol_version: u32,
    metrics: &RemoteNodeMetrics,
) -> Result<(), RemoteError> {
    if protocol_version != REMOTE_PROTOCOL_VERSION {
        let err = RemoteError {
            kind: Kind::IncompatibleProtocol as i32,
            message: format!(
                "Unsupported protocol version {protocol_version} (expected {REMOTE_PROTOCOL_VERSION})"
            ),
        };
        metrics.record_remote_error(&err);
        return Err(err);
    }
    Ok(())
}

#[tonic::async_trait]
impl ActorNode for ActorNodeService {
    #[tracing::instrument(
        name = "grpc_call",
        skip(self, request),
        fields(
            service = tracing::field::Empty,
            actor = tracing::field::Empty,
        )
    )]
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        let _in_flight = self.metrics.enter_in_flight();
        self.metrics.record_call();

        let (metadata, extensions, req) = request.into_parts();
        let span = tracing::Span::current();
        if !span.is_disabled() {
            span.record("service", sanitize_label(&req.service_name).as_ref());
            span.record("actor", sanitize_label(&req.actor_name).as_ref());
        }

        authorize_or_record(
            &self.authorizer,
            &self.metrics,
            &metadata,
            &extensions,
            &req.service_name,
            &req.actor_name,
        )?;

        if let Err(remote_err) = check_protocol_version(req.protocol_version, &self.metrics) {
            return Ok(Response::new(CallResponse {
                result: Some(CallResult::FrameworkError(remote_err)),
            }));
        }

        if let Err(remote_err) = self.check_not_draining() {
            return Ok(Response::new(CallResponse {
                result: Some(CallResult::FrameworkError(remote_err)),
            }));
        }

        let timeout = self.effective_timeout(req.timeout_ms, self.default_call_timeout);

        let endpoint = match self.registry.resolve(&req.service_name, &req.actor_name) {
            Ok(ep) => ep,
            Err(remote_err) => {
                self.metrics.record_remote_error(&remote_err);
                return Ok(Response::new(CallResponse {
                    result: Some(CallResult::FrameworkError(remote_err)),
                }));
            }
        };

        let response = endpoint.call_bytes(&req.payload, timeout).await;
        if let Some(CallResult::FrameworkError(ref err)) = response.result {
            self.metrics.record_remote_error(err);
        }
        Ok(Response::new(response))
    }

    #[tracing::instrument(
        name = "grpc_cast",
        skip(self, request),
        fields(
            service = tracing::field::Empty,
            actor = tracing::field::Empty,
        )
    )]
    async fn cast(&self, request: Request<CastRequest>) -> Result<Response<CastResponse>, Status> {
        let _in_flight = self.metrics.enter_in_flight();
        self.metrics.record_cast();

        let (metadata, extensions, req) = request.into_parts();
        let span = tracing::Span::current();
        if !span.is_disabled() {
            span.record("service", sanitize_label(&req.service_name).as_ref());
            span.record("actor", sanitize_label(&req.actor_name).as_ref());
        }

        authorize_or_record(
            &self.authorizer,
            &self.metrics,
            &metadata,
            &extensions,
            &req.service_name,
            &req.actor_name,
        )?;

        if let Err(remote_err) = check_protocol_version(req.protocol_version, &self.metrics) {
            return Ok(Response::new(CastResponse {
                framework_error: Some(remote_err),
            }));
        }

        if let Err(remote_err) = self.check_not_draining() {
            return Ok(Response::new(CastResponse {
                framework_error: Some(remote_err),
            }));
        }

        let timeout = self.effective_timeout(req.timeout_ms, self.default_cast_timeout);

        let endpoint = match self.registry.resolve(&req.service_name, &req.actor_name) {
            Ok(ep) => ep,
            Err(remote_err) => {
                self.metrics.record_remote_error(&remote_err);
                return Ok(Response::new(CastResponse {
                    framework_error: Some(remote_err),
                }));
            }
        };

        let response = endpoint.cast_bytes(&req.payload, timeout).await;
        if let Some(ref err) = response.framework_error {
            self.metrics.record_remote_error(err);
        }
        Ok(Response::new(response))
    }
}

/// Implementation of the generated `ActorNodeAdmin` gRPC service for node health and operational status.
#[derive(Clone)]
pub struct ActorNodeAdminService {
    registry: ActorNodeRegistry,
    supervisor: Option<SupervisorHandle>,
    started_at: Instant,
    draining: Arc<AtomicBool>,
    metrics: RemoteNodeMetrics,
    max_request_timeout: Option<Duration>,
    authorizer: Option<Authorizer>,
}

impl std::fmt::Debug for ActorNodeAdminService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorNodeAdminService")
            .field("registered_endpoints", &self.registry.len())
            .field("supervisor", &self.supervisor.as_ref().map(SupervisorHandle::name))
            .field("draining", &self.draining.load(Ordering::Acquire))
            .field("max_request_timeout", &self.max_request_timeout)
            .field("has_authorizer", &self.authorizer.is_some())
            .finish_non_exhaustive()
    }
}

impl ActorNodeAdminService {
    /// Creates an admin service inspecting `registry` and an optional [`SupervisorHandle`].
    pub fn new(registry: ActorNodeRegistry, supervisor: Option<SupervisorHandle>) -> Self {
        Self {
            registry,
            supervisor,
            started_at: Instant::now(),
            draining: Arc::new(AtomicBool::new(false)),
            metrics: RemoteNodeMetrics::default(),
            max_request_timeout: Some(MAX_REMOTE_REQUEST_TIMEOUT),
            authorizer: None,
        }
    }

    /// Overrides the reference start timestamp reported as `uptime_seconds`.
    #[must_use]
    pub fn with_started_at(mut self, started_at: Instant) -> Self {
        self.started_at = started_at;
        self
    }

    /// Sets the upper bound on `timeout_ms` accepted for `Status` requests.
    #[must_use]
    pub fn with_max_request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.max_request_timeout = timeout;
        self
    }

    /// Attaches the request-level [`Authorizer`] invoked with `("__admin", "health" | "status")`.
    #[must_use]
    pub fn with_authorizer(mut self, authorizer: Authorizer) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    pub(crate) fn with_draining(mut self, draining: Arc<AtomicBool>) -> Self {
        self.draining = draining;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: RemoteNodeMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    fn compute_fast_health(&self) -> NodeHealth {
        let uptime_seconds = self.started_at.elapsed().as_secs();
        if self.draining.load(Ordering::Acquire) {
            return NodeHealth {
                alive: false,
                state: HealthState::Draining,
                uptime_seconds,
            };
        }
        if let Some(sup) = &self.supervisor
            && !sup.is_alive()
        {
            return NodeHealth {
                alive: false,
                state: HealthState::Unavailable,
                uptime_seconds,
            };
        }
        let state = if self.registry.has_closed_endpoint() {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };
        NodeHealth {
            alive: true,
            state,
            uptime_seconds,
        }
    }
}

#[tonic::async_trait]
impl ActorNodeAdmin for ActorNodeAdminService {
    #[tracing::instrument(name = "grpc_admin_health", skip(self, request))]
    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let (metadata, extensions, req) = request.into_parts();
        authorize_or_record(
            &self.authorizer,
            &self.metrics,
            &metadata,
            &extensions,
            "__admin",
            "health",
        )?;

        if let Err(err) = check_protocol_version(req.protocol_version, &self.metrics) {
            return Ok(Response::new(HealthResponse {
                alive: false,
                uptime_seconds: self.started_at.elapsed().as_secs(),
                state: HealthState::Unavailable.as_str().to_string(),
                framework_error: Some(err),
            }));
        }

        let health = self.compute_fast_health();
        Ok(Response::new(HealthResponse {
            alive: health.alive,
            uptime_seconds: health.uptime_seconds,
            state: health.state.as_str().to_string(),
            framework_error: None,
        }))
    }

    #[tracing::instrument(name = "grpc_admin_status", skip(self, request))]
    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let (metadata, extensions, req) = request.into_parts();
        authorize_or_record(
            &self.authorizer,
            &self.metrics,
            &metadata,
            &extensions,
            "__admin",
            "status",
        )?;

        if let Err(err) = check_protocol_version(req.protocol_version, &self.metrics) {
            return Ok(Response::new(StatusResponse {
                result: Some(StatusResult::FrameworkError(err)),
            }));
        }

        let timeout = clamp_timeout(
            req.timeout_ms,
            Some(DEFAULT_SNAPSHOT_TIMEOUT),
            self.max_request_timeout,
        )
        .unwrap_or(DEFAULT_SNAPSHOT_TIMEOUT);

        let opts = StatusOptions::new()
            .timeout(timeout)
            .include_tree(req.include_tree)
            .include_child_details(req.include_child_details);

        let need_endpoints = req.include_child_details || self.supervisor.is_none();
        let (registered_endpoints, endpoint_count) = if need_endpoints {
            let eps = self.registry.snapshot_endpoints();
            let len = eps.len();
            (eps, len)
        } else {
            (Vec::new(), self.registry.len())
        };
        let remote_snapshot = self.metrics.snapshot(endpoint_count);
        let is_draining = self.draining.load(Ordering::Acquire);

        let mut node_status = if let Some(sup) = &self.supervisor {
            let monitor = LocalNodeMonitor::new(sup.clone()).with_started_at(self.started_at);
            match monitor.status(opts).await {
                Ok(status) => status,
                Err(sup_err) => {
                    let (kind, message) = match sup_err {
                        SupervisorError::Timeout(_) => {
                            (Kind::Timeout as i32, sup_err.to_string())
                        }
                        other => (Kind::ActorUnavailable as i32, other.to_string()),
                    };
                    let err = RemoteError { kind, message };
                    self.metrics.record_remote_error(&err);
                    return Ok(Response::new(StatusResponse {
                        result: Some(StatusResult::FrameworkError(err)),
                    }));
                }
            }
        } else {
            let mut summary = NodeSummary {
                total_actors: registered_endpoints.len(),
                ..Default::default()
            };
            for ep in &registered_endpoints {
                if let Some(q) = &ep.queue {
                    if q.is_closed {
                        summary.stopped_children += 1;
                    } else {
                        summary.running_children += 1;
                    }
                    summary.total_queued_messages =
                        summary.total_queued_messages.saturating_add(q.queued);
                    if q.is_full() {
                        summary.full_mailboxes += 1;
                    }
                }
            }
            let mut health = self.compute_fast_health();
            if health.state == HealthState::Healthy && summary.stopped_children > 0 {
                health.state = HealthState::Degraded;
            }
            NodeStatus {
                health,
                timestamp_unix: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                summary,
                supervision_tree: None,
                remote: None,
                registered_endpoints: Vec::new(),
            }
        };

        if is_draining {
            node_status.health.alive = false;
            node_status.health.state = HealthState::Draining;
        }
        node_status.remote = Some(remote_snapshot);
        if req.include_child_details {
            node_status.registered_endpoints = registered_endpoints;
        }

        match encode_payload(&node_status) {
            Ok(bytes) => Ok(Response::new(StatusResponse {
                result: Some(StatusResult::StatusPayload(bytes)),
            })),
            Err(err) => {
                let remote_err = RemoteError {
                    kind: Kind::Serialization as i32,
                    message: format!("Failed to serialize node status payload: {err}"),
                };
                self.metrics.record_remote_error(&remote_err);
                Ok(Response::new(StatusResponse {
                    result: Some(StatusResult::FrameworkError(remote_err)),
                }))
            }
        }
    }
}

/// Default Phase-1 timeout for draining active in-flight gRPC calls during shutdown.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Builder for configuring and starting an [`ActorNodeHandle`] with custom Tonic [`tonic::transport::Server`]
/// settings (such as TLS, HTTP/2 limits, keepalive), request timeouts, or an [`Authorizer`].
pub struct ActorNodeBuilder {
    addr: SocketAddr,
    registry: ActorNodeRegistry,
    supervisor: Option<SupervisorHandle>,
    server: tonic::transport::Server,
    default_call_timeout: Option<Duration>,
    default_cast_timeout: Option<Duration>,
    max_request_timeout: Option<Duration>,
    authorizer: Option<Authorizer>,
    drain_timeout: Option<Duration>,
    started_at: Option<Instant>,
}

impl std::fmt::Debug for ActorNodeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorNodeBuilder")
            .field("addr", &self.addr)
            .field("registered_endpoints", &self.registry.len())
            .field(
                "supervisor",
                &self.supervisor.as_ref().map(SupervisorHandle::name),
            )
            .field("default_call_timeout", &self.default_call_timeout)
            .field("default_cast_timeout", &self.default_cast_timeout)
            .field("max_request_timeout", &self.max_request_timeout)
            .field("drain_timeout", &self.drain_timeout)
            .field("has_authorizer", &self.authorizer.is_some())
            .finish_non_exhaustive()
    }
}

impl ActorNodeBuilder {
    /// Creates a new builder bound to `addr` and backed by `registry`.
    pub fn new(addr: SocketAddr, registry: ActorNodeRegistry) -> Self {
        Self {
            addr,
            registry,
            supervisor: None,
            server: tonic::transport::Server::builder(),
            default_call_timeout: Some(DEFAULT_REMOTE_CALL_TIMEOUT),
            default_cast_timeout: None,
            max_request_timeout: Some(MAX_REMOTE_REQUEST_TIMEOUT),
            authorizer: None,
            drain_timeout: Some(DEFAULT_DRAIN_TIMEOUT),
            started_at: None,
        }
    }

    /// Overrides the reference start timestamp reported by `ActorNodeAdmin` (`uptime_seconds`).
    #[must_use]
    pub fn started_at(mut self, started_at: Instant) -> Self {
        self.started_at = Some(started_at);
        self
    }

    /// Attaches a [`SupervisorHandle`] to be shut down during Phase 2 of [`ActorNodeHandle::shutdown`].
    #[must_use]
    pub fn supervisor(mut self, supervisor: SupervisorHandle) -> Self {
        self.supervisor = Some(supervisor);
        self
    }

    /// Supplies a pre-configured [`tonic::transport::Server`] (e.g. with TLS, concurrency limits,
    /// or HTTP/2 settings).
    #[must_use]
    pub fn server(mut self, server: tonic::transport::Server) -> Self {
        self.server = server;
        self
    }

    /// Sets the fallback timeout for `call` requests omitting `timeout_ms`.
    #[must_use]
    pub fn default_call_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.default_call_timeout = timeout;
        self
    }

    /// Sets the fallback timeout for `cast` requests omitting `timeout_ms`.
    ///
    /// When set to `Some(Duration::ZERO)`, casts without an explicit timeout execute non-blocking
    /// `try_cast` load-shedding (`MailboxFull`) instead of waiting up to
    /// [`crate::remote::endpoint::DEFAULT_REMOTE_CAST_TIMEOUT`].
    #[must_use]
    pub fn default_cast_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.default_cast_timeout = timeout;
        self
    }

    /// Sets the maximum per-request timeout accepted from remote callers.
    #[must_use]
    pub fn max_request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.max_request_timeout = timeout;
        self
    }

    /// Attaches a request-level [`Authorizer`] hook checked prior to actor routing.
    #[must_use]
    pub fn authorizer(mut self, authorizer: Authorizer) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    /// Sets the Phase-1 graceful drain timeout (defaults to [`DEFAULT_DRAIN_TIMEOUT`]).
    #[must_use]
    pub fn drain_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.drain_timeout = timeout;
        self
    }

    fn build_services(
        &self,
        metrics: RemoteNodeMetrics,
        draining: Arc<AtomicBool>,
        started_at: Instant,
    ) -> (
        ActorNodeServer<ActorNodeService>,
        ActorNodeAdminServer<ActorNodeAdminService>,
    ) {
        let mut node_svc = ActorNodeService::new(self.registry.clone())
            .with_default_call_timeout(self.default_call_timeout)
            .with_default_cast_timeout(self.default_cast_timeout)
            .with_max_request_timeout(self.max_request_timeout)
            .with_draining(draining.clone())
            .with_metrics(metrics.clone());
        let mut admin_svc = ActorNodeAdminService::new(self.registry.clone(), self.supervisor.clone())
            .with_started_at(started_at)
            .with_draining(draining)
            .with_metrics(metrics)
            .with_max_request_timeout(self.max_request_timeout);

        if let Some(auth) = &self.authorizer {
            node_svc = node_svc.with_authorizer(auth.clone());
            admin_svc = admin_svc.with_authorizer(auth.clone());
        }

        (
            ActorNodeServer::new(node_svc)
                .max_decoding_message_size(MAX_GRPC_FRAME_BYTES)
                .max_encoding_message_size(MAX_GRPC_FRAME_BYTES),
            ActorNodeAdminServer::new(admin_svc)
                .max_decoding_message_size(MAX_GRPC_FRAME_BYTES)
                .max_encoding_message_size(MAX_GRPC_FRAME_BYTES),
        )
    }

    /// Binds the TCP listener and starts serving both `ActorNode` and `ActorNodeAdmin` gRPC services.
    pub async fn start(mut self) -> Result<ActorNodeHandle, RemoteNodeError> {
        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let started_at = self.started_at.unwrap_or_else(Instant::now);
        let draining = Arc::new(AtomicBool::new(false));
        let metrics = RemoteNodeMetrics::default();

        let (node_svc, admin_svc) =
            self.build_services(metrics.clone(), draining.clone(), started_at);
        let router = self.server.add_service(node_svc).add_service(admin_svc);

        let server_task = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            router
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        Ok(ActorNodeHandle {
            addr: local_addr,
            registry: self.registry,
            supervisor: self.supervisor,
            drain_timeout: self.drain_timeout,
            started_at,
            draining,
            metrics,
            shutdown_tx: Some(shutdown_tx),
            server_task: Some(server_task),
        })
    }

    /// Binds the TCP listener and starts serving the gRPC node wrapped with a Tonic [`tonic::service::Interceptor`].
    pub async fn start_with_interceptor<F>(
        mut self,
        interceptor: F,
    ) -> Result<ActorNodeHandle, RemoteNodeError>
    where
        F: tonic::service::Interceptor + Clone + Send + Sync + 'static,
    {
        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let started_at = self.started_at.unwrap_or_else(Instant::now);
        let draining = Arc::new(AtomicBool::new(false));
        let metrics = RemoteNodeMetrics::default();

        let (raw_node_svc, raw_admin_svc) =
            self.build_services(metrics.clone(), draining.clone(), started_at);
        let node_svc =
            tonic::service::interceptor::InterceptedService::new(raw_node_svc, interceptor.clone());
        let admin_svc =
            tonic::service::interceptor::InterceptedService::new(raw_admin_svc, interceptor);
        let router = self.server.add_service(node_svc).add_service(admin_svc);

        let server_task = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            router
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        Ok(ActorNodeHandle {
            addr: local_addr,
            registry: self.registry,
            supervisor: self.supervisor,
            drain_timeout: self.drain_timeout,
            started_at,
            draining,
            metrics,
            shutdown_tx: Some(shutdown_tx),
            server_task: Some(server_task),
        })
    }
}

/// Handle controlling an active gRPC actor node server, coordinating graceful 2-phase shutdown.
pub struct ActorNodeHandle {
    addr: SocketAddr,
    registry: ActorNodeRegistry,
    supervisor: Option<SupervisorHandle>,
    drain_timeout: Option<Duration>,
    started_at: Instant,
    draining: Arc<AtomicBool>,
    metrics: RemoteNodeMetrics,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_task: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl std::fmt::Debug for ActorNodeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorNodeHandle")
            .field("addr", &self.addr)
            .field("registered_endpoints", &self.registry.len())
            .field(
                "supervisor",
                &self.supervisor.as_ref().map(SupervisorHandle::name),
            )
            .field("draining", &self.draining.load(Ordering::Acquire))
            .field("drain_timeout", &self.drain_timeout)
            .finish_non_exhaustive()
    }
}

impl ActorNodeHandle {
    /// Creates an [`ActorNodeBuilder`] for configuring TLS, timeouts, interceptors, or authorization.
    pub fn builder(addr: SocketAddr, registry: ActorNodeRegistry) -> ActorNodeBuilder {
        ActorNodeBuilder::new(addr, registry)
    }

    /// Spawns a new `ActorNodeServer` bound to the given socket address using default server settings.
    pub async fn start(
        addr: SocketAddr,
        registry: ActorNodeRegistry,
        supervisor: Option<SupervisorHandle>,
    ) -> Result<Self, RemoteNodeError> {
        Self::start_with(addr, registry, supervisor, tonic::transport::Server::builder()).await
    }

    /// Spawns a new `ActorNodeServer` bound to `addr` using a pre-configured [`tonic::transport::Server`]
    /// (for example with TLS configuration, HTTP/2 settings, or concurrency limits).
    pub async fn start_with(
        addr: SocketAddr,
        registry: ActorNodeRegistry,
        supervisor: Option<SupervisorHandle>,
        server: tonic::transport::Server,
    ) -> Result<Self, RemoteNodeError> {
        let mut builder = Self::builder(addr, registry).server(server);
        if let Some(sup) = supervisor {
            builder = builder.supervisor(sup);
        }
        builder.start().await
    }

    /// Sets the maximum duration to wait in Phase 1 for active in-flight gRPC requests to drain
    /// before aborting remaining streams and proceeding to Phase 2 (`Supervisor` shutdown).
    ///
    /// Defaults to [`DEFAULT_DRAIN_TIMEOUT`] (`10s`).
    #[must_use]
    pub fn with_drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = Some(timeout);
        self
    }

    /// Disables the Phase-1 drain timeout, waiting indefinitely for all open HTTP/2 streams to close.
    #[must_use]
    pub fn without_drain_timeout(mut self) -> Self {
        self.drain_timeout = None;
        self
    }

    /// The bound local address of the running node.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Access the registry associated with this node.
    pub fn registry(&self) -> &ActorNodeRegistry {
        &self.registry
    }

    /// Returns the elapsed uptime of this running gRPC node.
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Captures a point-in-time snapshot of gRPC transport counters for this node.
    pub fn remote_metrics(&self) -> RemoteMetricsSnapshot {
        self.metrics.snapshot(self.registry.len())
    }

    /// Performs a coordinated two-phase graceful shutdown:
    /// 1. Closes the gRPC server (HTTP/2 GOAWAY) and drains active in-flight requests (bounded by `drain_timeout`).
    /// 2. If a `SupervisorHandle` was attached, initiates cascading actor subtree shutdown.
    pub async fn shutdown(mut self) -> Result<(), RemoteNodeError> {
        self.draining.store(true, Ordering::Release);
        tracing::info!(addr = %self.addr, "Initiating Phase 1: gRPC server shutdown");
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(mut handle) = self.server_task.take() {
            match self.drain_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, &mut handle).await {
                    Ok(join_res) => join_res??,
                    Err(_) => {
                        tracing::warn!(
                            addr = %self.addr,
                            timeout = ?timeout,
                            "Phase 1 gRPC drain timed out; aborting remaining streams and proceeding to Phase 2"
                        );
                        handle.abort();
                    }
                },
                None => {
                    handle.await??;
                }
            }
        }

        if let Some(supervisor) = self.supervisor.take() {
            tracing::info!("Initiating Phase 2: Supervisor cascading subtree shutdown");
            supervisor.shutdown().await?;
        }

        tracing::info!(addr = %self.addr, "Coordinated 2-phase shutdown complete");
        Ok(())
    }
}

impl Drop for ActorNodeHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.server_task.take() {
            handle.abort();
        }
    }
}

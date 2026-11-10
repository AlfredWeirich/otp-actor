//! gRPC Server hosting the `ActorNode` service with 2-phase coordinated graceful shutdown.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::actor::SupervisorHandle;
use crate::remote::endpoint::{MAX_GRPC_FRAME_BYTES, REMOTE_PROTOCOL_VERSION};
use crate::remote::error::RemoteNodeError;
use crate::remote::proto::call_response::Result as CallResult;
use crate::remote::proto::remote_error::Kind;
use crate::remote::proto::{
    ActorNode, ActorNodeServer, CallRequest, CallResponse, CastRequest, CastResponse, RemoteError,
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

/// Implementation of the generated `ActorNode` gRPC service.
#[derive(Clone)]
pub struct ActorNodeService {
    registry: ActorNodeRegistry,
    default_call_timeout: Option<Duration>,
    max_request_timeout: Option<Duration>,
    authorizer: Option<Authorizer>,
}

impl ActorNodeService {
    /// Creates a new service wrapping the given registry with server-side timeout defaults
    /// ([`DEFAULT_REMOTE_CALL_TIMEOUT`] and [`MAX_REMOTE_REQUEST_TIMEOUT`]).
    pub fn new(registry: ActorNodeRegistry) -> Self {
        Self {
            registry,
            default_call_timeout: Some(DEFAULT_REMOTE_CALL_TIMEOUT),
            max_request_timeout: Some(MAX_REMOTE_REQUEST_TIMEOUT),
            authorizer: None,
        }
    }

    /// Sets the fallback timeout for `call` requests that do not specify `timeout_ms`.
    #[must_use]
    pub fn with_default_call_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.default_call_timeout = timeout;
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

    fn effective_timeout(
        &self,
        client_timeout_ms: Option<u64>,
        fallback: Option<Duration>,
    ) -> Option<Duration> {
        let raw = client_timeout_ms.map(Duration::from_millis).or(fallback);
        match (raw, self.max_request_timeout) {
            (Some(d), Some(max)) => Some(d.min(max)),
            (other, _) => other,
        }
    }
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
        let (metadata, extensions, req) = request.into_parts();
        let span = tracing::Span::current();
        span.record("service", sanitize_label(&req.service_name).as_str());
        span.record("actor", sanitize_label(&req.actor_name).as_str());

        if let Some(auth) = &self.authorizer {
            auth(&metadata, &extensions, &req.service_name, &req.actor_name)?;
        }

        if req.protocol_version != REMOTE_PROTOCOL_VERSION {
            return Ok(Response::new(CallResponse {
                result: Some(CallResult::FrameworkError(RemoteError {
                    kind: Kind::IncompatibleProtocol as i32,
                    message: format!(
                        "Unsupported protocol version {} (expected {})",
                        req.protocol_version, REMOTE_PROTOCOL_VERSION
                    ),
                })),
            }));
        }

        let timeout = self.effective_timeout(req.timeout_ms, self.default_call_timeout);

        let endpoint = match self.registry.resolve(&req.service_name, &req.actor_name) {
            Ok(ep) => ep,
            Err(remote_err) => {
                return Ok(Response::new(CallResponse {
                    result: Some(CallResult::FrameworkError(remote_err)),
                }));
            }
        };

        let response = endpoint.call_bytes(&req.payload, timeout).await;
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
        let (metadata, extensions, req) = request.into_parts();
        let span = tracing::Span::current();
        span.record("service", sanitize_label(&req.service_name).as_str());
        span.record("actor", sanitize_label(&req.actor_name).as_str());

        if let Some(auth) = &self.authorizer {
            auth(&metadata, &extensions, &req.service_name, &req.actor_name)?;
        }

        if req.protocol_version != REMOTE_PROTOCOL_VERSION {
            return Ok(Response::new(CastResponse {
                framework_error: Some(RemoteError {
                    kind: Kind::IncompatibleProtocol as i32,
                    message: format!(
                        "Unsupported protocol version {} (expected {})",
                        req.protocol_version, REMOTE_PROTOCOL_VERSION
                    ),
                }),
            }));
        }

        let timeout = self.effective_timeout(req.timeout_ms, None);

        let endpoint = match self.registry.resolve(&req.service_name, &req.actor_name) {
            Ok(ep) => ep,
            Err(remote_err) => {
                return Ok(Response::new(CastResponse {
                    framework_error: Some(remote_err),
                }));
            }
        };

        let response = endpoint.cast_bytes(&req.payload, timeout).await;
        Ok(Response::new(response))
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
    max_request_timeout: Option<Duration>,
    authorizer: Option<Authorizer>,
    drain_timeout: Option<Duration>,
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
            max_request_timeout: Some(MAX_REMOTE_REQUEST_TIMEOUT),
            authorizer: None,
            drain_timeout: Some(DEFAULT_DRAIN_TIMEOUT),
        }
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

    fn build_service(&self) -> ActorNodeServer<ActorNodeService> {
        let mut service = ActorNodeService::new(self.registry.clone())
            .with_default_call_timeout(self.default_call_timeout)
            .with_max_request_timeout(self.max_request_timeout);
        if let Some(auth) = &self.authorizer {
            service = service.with_authorizer(auth.clone());
        }
        ActorNodeServer::new(service)
            .max_decoding_message_size(MAX_GRPC_FRAME_BYTES)
            .max_encoding_message_size(MAX_GRPC_FRAME_BYTES)
    }

    /// Binds the TCP listener and starts serving the gRPC node.
    pub async fn start(mut self) -> Result<ActorNodeHandle, RemoteNodeError> {
        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let svc = self.build_service();
        let router = self.server.add_service(svc);

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

        let svc =
            tonic::service::interceptor::InterceptedService::new(self.build_service(), interceptor);
        let router = self.server.add_service(svc);

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
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_task: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
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

    /// Performs a coordinated two-phase graceful shutdown:
    /// 1. Closes the gRPC server (HTTP/2 GOAWAY) and drains active in-flight requests (bounded by `drain_timeout`).
    /// 2. If a `SupervisorHandle` was attached, initiates cascading actor subtree shutdown.
    pub async fn shutdown(mut self) -> Result<(), RemoteNodeError> {
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

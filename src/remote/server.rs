//! gRPC Server hosting the `ActorNode` service with 2-phase coordinated graceful shutdown.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::actor::RootSupervisorHandle;
use crate::remote::endpoint::MAX_GRPC_FRAME_BYTES;
use crate::remote::proto::call_response::Result as CallResult;
use crate::remote::proto::{
    ActorNode, ActorNodeServer, CallRequest, CallResponse, CastRequest, CastResponse,
};
use crate::remote::registry::ActorNodeRegistry;

/// Implementation of the generated `ActorNode` gRPC service.
#[derive(Clone)]
pub struct ActorNodeService {
    registry: ActorNodeRegistry,
}

impl ActorNodeService {
    /// Creates a new service wrapping the given registry.
    pub fn new(registry: ActorNodeRegistry) -> Self {
        Self { registry }
    }
}

#[tonic::async_trait]
impl ActorNode for ActorNodeService {
    #[tracing::instrument(
        name = "grpc_call",
        skip(self, request),
        fields(
            service = %request.get_ref().service_name,
            actor = %request.get_ref().actor_name,
        )
    )]
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        let req = request.into_inner();
        let timeout = req.timeout_ms.map(Duration::from_millis);

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
            service = %request.get_ref().service_name,
            actor = %request.get_ref().actor_name,
        )
    )]
    async fn cast(&self, request: Request<CastRequest>) -> Result<Response<CastResponse>, Status> {
        let req = request.into_inner();

        let endpoint = match self.registry.resolve(&req.service_name, &req.actor_name) {
            Ok(ep) => ep,
            Err(remote_err) => {
                return Ok(Response::new(CastResponse {
                    framework_error: Some(remote_err),
                }));
            }
        };

        let response = endpoint.cast_bytes(&req.payload).await;
        Ok(Response::new(response))
    }
}

/// Default Phase-1 timeout for draining active in-flight gRPC calls during shutdown.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Handle controlling an active gRPC actor node server, coordinating graceful 2-phase shutdown.
pub struct ActorNodeHandle {
    addr: SocketAddr,
    registry: ActorNodeRegistry,
    root: Option<RootSupervisorHandle>,
    drain_timeout: Option<Duration>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_task: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl ActorNodeHandle {
    /// Spawns a new `ActorNodeServer` bound to the given socket address.
    pub async fn start(
        addr: SocketAddr,
        registry: ActorNodeRegistry,
        root: Option<RootSupervisorHandle>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let service = ActorNodeService::new(registry.clone());
        let svc = ActorNodeServer::new(service)
            .max_decoding_message_size(MAX_GRPC_FRAME_BYTES)
            .max_encoding_message_size(MAX_GRPC_FRAME_BYTES);
        let server = tonic::transport::Server::builder().add_service(svc);

        let server_task = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            server
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        Ok(Self {
            addr: local_addr,
            registry,
            root,
            drain_timeout: Some(DEFAULT_DRAIN_TIMEOUT),
            shutdown_tx: Some(shutdown_tx),
            server_task: Some(server_task),
        })
    }

    /// Sets the maximum duration to wait in Phase 1 for active in-flight gRPC requests to drain
    /// before aborting remaining streams and proceeding to Phase 2 (`RootSupervisor` shutdown).
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
    /// 2. If a `RootSupervisorHandle` was attached, initiates cascading actor subtree shutdown.
    pub async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

        if let Some(root) = self.root.take() {
            tracing::info!("Initiating Phase 2: RootSupervisor cascading subtree shutdown");
            root.shutdown().await?;
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

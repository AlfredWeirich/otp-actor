//! gRPC Server hosting the `ActorNode` service with 2-phase coordinated graceful shutdown.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::actor::RootSupervisorHandle;
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

/// Handle controlling an active gRPC actor node server, coordinating graceful 2-phase shutdown.
pub struct ActorNodeHandle {
    addr: SocketAddr,
    registry: ActorNodeRegistry,
    root: Option<RootSupervisorHandle>,
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
        let server = tonic::transport::Server::builder().add_service(ActorNodeServer::new(service));

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
            shutdown_tx: Some(shutdown_tx),
            server_task: Some(server_task),
        })
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
    /// 1. Closes the gRPC server (HTTP/2 GOAWAY) and drains all active in-flight requests.
    /// 2. If a `RootSupervisorHandle` was attached, initiates cascading actor subtree shutdown.
    pub async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!(addr = %self.addr, "Initiating Phase 1: gRPC server shutdown");
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.server_task.take() {
            handle.await??;
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
    }
}

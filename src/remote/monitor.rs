//! Location-transparent node health and operational telemetry monitors over gRPC.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

use crate::actor::{
    HealthState, LocalNodeMonitor, NodeHealth, NodeStatus, StatusOptions, SupervisorError,
    SupervisorHandle, format_prometheus_metrics,
};
use crate::remote::client::{duration_to_wire_ms, server_budget_for_deadline};
use crate::remote::endpoint::{MAX_GRPC_FRAME_BYTES, REMOTE_PROTOCOL_VERSION, decode_payload};
use crate::remote::error::RemoteActorError;
use crate::remote::proto::status_response::Result as StatusResult;
use crate::remote::proto::{ActorNodeAdminClient, HealthRequest, StatusRequest};

/// Error type returned by [`RemoteNodeMonitor`] and [`AnyNodeMonitor`] operations.
///
/// Uses [`Infallible`] as the actor-domain error parameter because node-level health and status
/// queries never carry per-actor domain errors (`RemoteActorError::Actor`).
pub type RemoteMonitorError = RemoteActorError<Infallible>;

type MetadataInjector = Arc<dyn Fn(&mut tonic::metadata::MetadataMap) + Send + Sync>;

/// Remote client handle querying operational health and status from an [`super::ActorNodeHandle`]
/// via the `ActorNodeAdmin` gRPC service.
#[derive(Clone)]
pub struct RemoteNodeMonitor {
    channel: Channel,
    client: ActorNodeAdminClient<Channel>,
    metadata_injector: Option<MetadataInjector>,
}

impl std::fmt::Debug for RemoteNodeMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteNodeMonitor").finish()
    }
}

impl RemoteNodeMonitor {
    /// Creates a [`RemoteNodeMonitor`] from an existing connected gRPC [`Channel`].
    pub fn new(channel: Channel) -> Self {
        let client = ActorNodeAdminClient::new(channel.clone())
            .max_decoding_message_size(MAX_GRPC_FRAME_BYTES)
            .max_encoding_message_size(MAX_GRPC_FRAME_BYTES);
        Self {
            channel,
            client,
            metadata_injector: None,
        }
    }

    /// Connects to a remote node endpoint URL (e.g. `"http://127.0.0.1:50051"`).
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, tonic::transport::Error> {
        let ep = Endpoint::from_shared(endpoint.into())?;
        Self::connect_with(ep).await
    }

    /// Connects using a pre-configured Tonic [`Endpoint`] (e.g. with TLS or keepalive settings).
    pub async fn connect_with(endpoint: Endpoint) -> Result<Self, tonic::transport::Error> {
        let channel = endpoint.connect().await?;
        Ok(Self::new(channel))
    }

    /// Attaches a per-request metadata hook invoked before every `health` and `status` RPC.
    #[must_use]
    pub fn with_request_metadata<F>(mut self, injector: F) -> Self
    where
        F: Fn(&mut tonic::metadata::MetadataMap) + Send + Sync + 'static,
    {
        self.metadata_injector = Some(Arc::new(injector));
        self
    }

    /// Accesses the underlying gRPC HTTP/2 channel.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    fn prepare_request<T>(&self, body: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(body);
        if let Some(injector) = &self.metadata_injector {
            injector(request.metadata_mut());
        }
        request
    }

    /// Queries fast liveness status (`Health` RPC) from the remote node.
    #[tracing::instrument(skip(self), err)]
    pub async fn health(&self) -> Result<NodeHealth, RemoteMonitorError> {
        self.health_internal().await
    }

    /// Queries fast liveness status (`Health` RPC) bounded by `timeout`.
    #[tracing::instrument(skip(self), err)]
    pub async fn health_timeout(&self, timeout: Duration) -> Result<NodeHealth, RemoteMonitorError> {
        tokio::time::timeout(timeout, self.health_internal())
            .await
            .map_err(|_| RemoteActorError::Timeout)?
    }

    async fn health_internal(&self) -> Result<NodeHealth, RemoteMonitorError> {
        let req = self.prepare_request(HealthRequest {
            protocol_version: REMOTE_PROTOCOL_VERSION,
        });
        let mut client = self.client.clone();
        let resp = client.health(req).await?.into_inner();
        if let Some(err) = resp.framework_error {
            return Err(RemoteActorError::from(err));
        }

        let state = match resp.state.as_str() {
            "ok" => HealthState::Healthy,
            "degraded" => HealthState::Degraded,
            "draining" => HealthState::Draining,
            _ => HealthState::Unavailable,
        };

        Ok(NodeHealth {
            alive: resp.alive,
            state,
            uptime_seconds: resp.uptime_seconds,
        })
    }

    /// Queries a bounded operational [`NodeStatus`] snapshot (`Status` RPC) from the remote node.
    #[tracing::instrument(skip(self), err)]
    pub async fn status(&self, opts: StatusOptions) -> Result<NodeStatus, RemoteMonitorError> {
        let timeout = opts.timeout;
        let server_tree_budget = server_budget_for_deadline(timeout);
        tokio::time::timeout(timeout, self.status_internal(opts, server_tree_budget))
            .await
            .map_err(|_| RemoteActorError::Timeout)?
    }

    /// Queries [`NodeStatus`] using [`StatusOptions::default()`].
    pub async fn status_default(&self) -> Result<NodeStatus, RemoteMonitorError> {
        self.status(StatusOptions::default()).await
    }

    async fn status_internal(
        &self,
        opts: StatusOptions,
        server_tree_budget: Duration,
    ) -> Result<NodeStatus, RemoteMonitorError> {
        let req = self.prepare_request(StatusRequest {
            protocol_version: REMOTE_PROTOCOL_VERSION,
            timeout_ms: Some(duration_to_wire_ms(server_tree_budget)),
            include_tree: opts.include_tree,
            include_child_details: opts.include_child_details,
        });

        let mut client = self.client.clone();
        let resp = client.status(req).await?.into_inner();
        match resp.result {
            Some(StatusResult::StatusPayload(bytes)) => {
                decode_payload(&bytes).map_err(|e| RemoteActorError::Deserialization(e.to_string()))
            }
            Some(StatusResult::FrameworkError(err)) => Err(RemoteActorError::from(err)),
            None => Err(RemoteActorError::ActorPanic(
                "Empty status response from remote node".into(),
            )),
        }
    }

    /// Queries [`NodeStatus`] from the remote node and formats it in Prometheus exposition format.
    #[tracing::instrument(skip(self), err)]
    pub async fn prometheus_metrics(
        &self,
        opts: StatusOptions,
    ) -> Result<String, RemoteMonitorError> {
        let status = self.status(opts).await?;
        Ok(format_prometheus_metrics(&status))
    }
}

/// Unified operational monitor providing Location Transparency across local and remote nodes.
#[derive(Clone)]
pub enum AnyNodeMonitor {
    /// Local monitor reading directly from an in-process [`SupervisorHandle`].
    Local(LocalNodeMonitor),
    /// Remote monitor querying an [`super::ActorNodeHandle`] over gRPC.
    Remote(RemoteNodeMonitor),
}

impl std::fmt::Debug for AnyNodeMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(local) => f.debug_tuple("AnyNodeMonitor::Local").field(local).finish(),
            Self::Remote(remote) => f
                .debug_tuple("AnyNodeMonitor::Remote")
                .field(remote)
                .finish(),
        }
    }
}

impl AnyNodeMonitor {
    /// Creates a local [`AnyNodeMonitor`] wrapping `supervisor`.
    pub fn local(supervisor: SupervisorHandle) -> Self {
        Self::Local(LocalNodeMonitor::new(supervisor))
    }

    /// Creates a local [`AnyNodeMonitor`] from an existing [`LocalNodeMonitor`].
    pub fn from_local(monitor: LocalNodeMonitor) -> Self {
        Self::Local(monitor)
    }

    /// Connects to a remote node endpoint URL (e.g. `"http://127.0.0.1:50051"`).
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, tonic::transport::Error> {
        Ok(Self::Remote(RemoteNodeMonitor::connect(endpoint).await?))
    }

    /// Creates a remote [`AnyNodeMonitor`] from an existing [`RemoteNodeMonitor`].
    pub fn from_remote(monitor: RemoteNodeMonitor) -> Self {
        Self::Remote(monitor)
    }

    fn map_supervisor_error(err: SupervisorError) -> RemoteMonitorError {
        match err {
            SupervisorError::Timeout(_) => RemoteActorError::Timeout,
            SupervisorError::ChildNotFound(name) => RemoteActorError::ActorNotFound(name),
            SupervisorError::MaxRestartsExceeded(name, max, window) => {
                RemoteActorError::MaxRestartsExceeded(format!(
                    "Child '{name}' exceeded max restarts ({max} in {window:?})"
                ))
            }
            _ => RemoteActorError::ActorUnavailable,
        }
    }

    /// Queries node health transparently (zero-hop locally, `Health` RPC remotely).
    #[tracing::instrument(skip(self), err)]
    pub async fn health(&self) -> Result<NodeHealth, RemoteMonitorError> {
        match self {
            Self::Local(local) => Ok(local.health()),
            Self::Remote(remote) => remote.health().await,
        }
    }

    /// Queries a bounded operational [`NodeStatus`] transparently across local or remote nodes.
    #[tracing::instrument(skip(self), err)]
    pub async fn status(&self, opts: StatusOptions) -> Result<NodeStatus, RemoteMonitorError> {
        match self {
            Self::Local(local) => local.status(opts).await.map_err(Self::map_supervisor_error),
            Self::Remote(remote) => remote.status(opts).await,
        }
    }

    /// Queries [`NodeStatus`] using [`StatusOptions::default()`].
    pub async fn status_default(&self) -> Result<NodeStatus, RemoteMonitorError> {
        self.status(StatusOptions::default()).await
    }

    /// Queries [`NodeStatus`] and renders it in Prometheus / OpenMetrics text exposition format.
    #[tracing::instrument(skip(self), err)]
    pub async fn prometheus_metrics(
        &self,
        opts: StatusOptions,
    ) -> Result<String, RemoteMonitorError> {
        match self {
            Self::Local(local) => local
                .prometheus_metrics(opts)
                .await
                .map_err(Self::map_supervisor_error),
            Self::Remote(remote) => remote.prometheus_metrics(opts).await,
        }
    }
}

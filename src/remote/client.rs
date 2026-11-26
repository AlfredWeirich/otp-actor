//! Strongly typed remote actor client communicating over gRPC with Location Transparency.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

use crate::actor::{ActorClient, StatefulActor};
use crate::remote::endpoint::{
    MAX_GRPC_FRAME_BYTES, REMOTE_PROTOCOL_VERSION, decode_payload, encode_payload,
};
use crate::remote::error::RemoteActorError;
use crate::remote::proto::call_response::Result as CallResult;
use crate::remote::proto::{ActorNodeClient, CallRequest, CastRequest};

pub(crate) type MetadataInjector = Arc<dyn Fn(&mut tonic::metadata::MetadataMap) + Send + Sync>;

/// Chains `next` after any existing [`MetadataInjector`] so multiple metadata hooks
/// (such as correlation IDs and bearer tokens) compose cleanly regardless of call order.
pub(crate) fn compose_metadata_injector<F>(
    existing: Option<MetadataInjector>,
    next: F,
) -> MetadataInjector
where
    F: Fn(&mut tonic::metadata::MetadataMap) + Send + Sync + 'static,
{
    match existing {
        None => Arc::new(next),
        Some(prev) => Arc::new(move |metadata| {
            prev(metadata);
            next(metadata);
        }),
    }
}

/// Validates a non-empty Bearer token and builds a `"Bearer <token>"` gRPC metadata value.
pub(crate) fn parse_bearer_metadata_value(
    token: &str,
) -> Result<
    tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    tonic::metadata::errors::InvalidMetadataValue,
> {
    if token.is_empty() {
        return tonic::metadata::MetadataValue::try_from("\n");
    }
    tonic::metadata::MetadataValue::try_from(format!("Bearer {token}"))
}

/// Maximum transport and serialization headroom reserved between the server execution budget
/// (`timeout_ms`) and the client's overall deadline so structured server responses (such as
/// degraded `NodeStatus` snapshots or `Kind::Timeout` framework errors) arrive before the client
/// cancels the HTTP/2 stream.
const MAX_TRANSPORT_HEADROOM: Duration = Duration::from_millis(100);

/// Safely converts a [`Duration`] into non-overflowing milliseconds, clamping sub-millisecond
/// non-zero durations to `1ms` so they do not truncate to `0ms` on the wire.
pub(crate) fn duration_to_wire_ms(d: Duration) -> u64 {
    if d.is_zero() {
        0
    } else {
        d.as_millis().clamp(1, u128::from(u64::MAX)) as u64
    }
}

/// Derives the server-side execution budget (`timeout_ms`) from a client-side `deadline`,
/// reserving up to `min(deadline / 4, 100ms)` of transport/serialization headroom so the server
/// can finish and return a structured response before the client's deadline elapses.
pub(crate) fn server_budget_for_deadline(deadline: Duration) -> Duration {
    let headroom = (deadline / 4).min(MAX_TRANSPORT_HEADROOM);
    if headroom.is_zero() {
        deadline
    } else {
        deadline.saturating_sub(headroom)
    }
}

/// Client handle for invoking a remote `StatefulActor` over a gRPC connection.
#[derive(Clone)]
pub struct RemoteActorClient<A: StatefulActor> {
    service_name: Arc<str>,
    actor_name: Arc<str>,
    channel: Channel,
    client: ActorNodeClient<Channel>,
    metadata_injector: Option<MetadataInjector>,
    _phantom: PhantomData<A>,
}

impl<A: StatefulActor> std::fmt::Debug for RemoteActorClient<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteActorClient")
            .field("service_name", &self.service_name)
            .field("actor_name", &self.actor_name)
            .field("has_metadata_injector", &self.metadata_injector.is_some())
            .finish_non_exhaustive()
    }
}

impl<A: StatefulActor> RemoteActorClient<A> {
    /// Creates a client from an existing connected gRPC `Channel`.
    pub fn new(
        channel: Channel,
        service_name: impl Into<String>,
        actor_name: impl Into<String>,
    ) -> Self {
        let client = ActorNodeClient::new(channel.clone())
            .max_decoding_message_size(MAX_GRPC_FRAME_BYTES)
            .max_encoding_message_size(MAX_GRPC_FRAME_BYTES);
        Self {
            service_name: Arc::from(service_name.into()),
            actor_name: Arc::from(actor_name.into()),
            channel,
            client,
            metadata_injector: None,
            _phantom: PhantomData,
        }
    }

    /// Connects to a remote node endpoint URL (e.g. `"http://127.0.0.1:50051"`).
    pub async fn connect(
        endpoint: impl Into<String>,
        service_name: impl Into<String>,
        actor_name: impl Into<String>,
    ) -> Result<Self, tonic::transport::Error> {
        let ep = Endpoint::from_shared(endpoint.into())?;
        Self::connect_with(ep, service_name, actor_name).await
    }

    /// Connects using a pre-configured Tonic [`Endpoint`] (e.g. with TLS, keepalive, or timeouts).
    pub async fn connect_with(
        endpoint: Endpoint,
        service_name: impl Into<String>,
        actor_name: impl Into<String>,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = endpoint.connect().await?;
        Ok(Self::new(channel, service_name, actor_name))
    }

    /// Attaches a per-request metadata hook invoked before every `call` and `cast`
    /// (for example to inject correlation headers or custom metadata).
    ///
    /// Multiple calls compose in registration order rather than overwriting previous hooks.
    #[must_use]
    pub fn with_request_metadata<F>(mut self, injector: F) -> Self
    where
        F: Fn(&mut tonic::metadata::MetadataMap) + Send + Sync + 'static,
    {
        self.metadata_injector = Some(compose_metadata_injector(
            self.metadata_injector.take(),
            injector,
        ));
        self
    }

    /// Attaches a static `authorization: Bearer <token>` header to every `call` and `cast`.
    ///
    /// Validates the token upfront so empty tokens or invalid gRPC metadata characters fail
    /// immediately without panicking. Composes cleanly with [`Self::with_request_metadata`].
    pub fn with_bearer_token(
        mut self,
        token: impl AsRef<str>,
    ) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        let header = parse_bearer_metadata_value(token.as_ref())?;
        self.metadata_injector = Some(compose_metadata_injector(
            self.metadata_injector.take(),
            move |metadata| {
                metadata.insert("authorization", header.clone());
            },
        ));
        Ok(self)
    }

    /// Attaches a dynamic Bearer token provider invoked before every `call` and `cast`.
    ///
    /// Useful for rotating OAuth2/JWT tokens (e.g., read from a local `ResourceActor` or cache).
    /// When the provider returns `None`, no `authorization` header is added by this hook.
    /// Composes cleanly with [`Self::with_request_metadata`].
    #[must_use]
    pub fn with_bearer_token_provider<F>(mut self, provider: F) -> Self
    where
        F: Fn() -> Option<String> + Send + Sync + 'static,
    {
        self.metadata_injector = Some(compose_metadata_injector(
            self.metadata_injector.take(),
            move |metadata| {
                if let Some(token) = provider() {
                    match parse_bearer_metadata_value(&token) {
                        Ok(header) => {
                            metadata.insert("authorization", header);
                        }
                        Err(_) => {
                            tracing::warn!(
                                "Ignoring dynamic bearer token with invalid gRPC metadata characters"
                            );
                        }
                    }
                }
            },
        ));
        self
    }

    /// Access the underlying gRPC HTTP/2 channel.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Name of the remote service domain.
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// Name of the remote actor.
    pub fn actor_name(&self) -> &str {
        &self.actor_name
    }

    /// Alias for [`Self::actor_name`], matching [`ActorClient::name`].
    #[must_use]
    pub fn name(&self) -> &str {
        self.actor_name()
    }

    /// Sends a request message (`call`) to the remote actor and awaits its typed response.
    #[tracing::instrument(skip(self, msg), fields(service = %self.service_name, actor = %self.actor_name), err(Debug))]
    pub async fn call(&self, msg: A::Message) -> Result<A::Response, RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        self.call_internal(msg, None).await
    }

    /// Sends a request message (`call`) with a client-side timeout deadline.
    #[tracing::instrument(skip(self, msg), fields(service = %self.service_name, actor = %self.actor_name), err(Debug))]
    pub async fn call_timeout(
        &self,
        msg: A::Message,
        timeout: Duration,
    ) -> Result<A::Response, RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        let server_budget = server_budget_for_deadline(timeout);
        tokio::time::timeout(timeout, self.call_internal(msg, Some(server_budget)))
            .await
            .map_err(|_| RemoteActorError::Timeout)?
    }

    /// Internal execution of gRPC Call RPC.
    async fn call_internal(
        &self,
        msg: A::Message,
        server_timeout: Option<Duration>,
    ) -> Result<A::Response, RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        let payload =
            encode_payload(&msg).map_err(|e| RemoteActorError::Serialization(e.to_string()))?;

        let mut request = tonic::Request::new(CallRequest {
            service_name: self.service_name.to_string(),
            actor_name: self.actor_name.to_string(),
            payload,
            timeout_ms: server_timeout.map(duration_to_wire_ms),
            protocol_version: REMOTE_PROTOCOL_VERSION,
        });
        if let Some(injector) = &self.metadata_injector {
            injector(request.metadata_mut());
        }

        let mut client = self.client.clone();
        let response = client.call(request).await?.into_inner();

        match response.result {
            Some(CallResult::OkPayload(bytes)) => {
                decode_payload(&bytes).map_err(|e| RemoteActorError::Deserialization(e.to_string()))
            }
            Some(CallResult::ActorErrorPayload(bytes)) => {
                let domain_err: A::Error = decode_payload(&bytes)
                    .map_err(|e| RemoteActorError::Deserialization(e.to_string()))?;
                Err(RemoteActorError::Actor(domain_err))
            }
            Some(CallResult::FrameworkError(err)) => Err(RemoteActorError::from(err)),
            None => Err(RemoteActorError::ActorPanic(
                "Empty response from remote node".into(),
            )),
        }
    }

    /// Sends an asynchronous fire-and-forget notification (`cast`) to the remote actor.
    #[tracing::instrument(skip(self, msg), fields(service = %self.service_name, actor = %self.actor_name), err(Debug))]
    pub async fn cast(&self, msg: A::Message) -> Result<(), RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
    {
        self.cast_internal(msg, None).await
    }

    /// Sends an asynchronous fire-and-forget notification (`cast`) with a client-side timeout deadline.
    ///
    /// Passing [`Duration::ZERO`] performs a non-blocking `try_cast` on the remote actor, returning
    /// [`RemoteActorError::MailboxFull`] immediately if its bounded mailbox is saturated.
    #[tracing::instrument(skip(self, msg), fields(service = %self.service_name, actor = %self.actor_name), err(Debug))]
    pub async fn cast_timeout(
        &self,
        msg: A::Message,
        timeout: Duration,
    ) -> Result<(), RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
    {
        if timeout.is_zero() {
            return self.cast_internal(msg, Some(Duration::ZERO)).await;
        }
        let server_budget = server_budget_for_deadline(timeout);
        tokio::time::timeout(timeout, self.cast_internal(msg, Some(server_budget)))
            .await
            .map_err(|_| RemoteActorError::Timeout)?
    }

    async fn cast_internal(
        &self,
        msg: A::Message,
        server_timeout: Option<Duration>,
    ) -> Result<(), RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
    {
        let payload =
            encode_payload(&msg).map_err(|e| RemoteActorError::Serialization(e.to_string()))?;

        let mut request = tonic::Request::new(CastRequest {
            service_name: self.service_name.to_string(),
            actor_name: self.actor_name.to_string(),
            payload,
            timeout_ms: server_timeout.map(duration_to_wire_ms),
            protocol_version: REMOTE_PROTOCOL_VERSION,
        });
        if let Some(injector) = &self.metadata_injector {
            injector(request.metadata_mut());
        }

        let mut client = self.client.clone();
        let response = client.cast(request).await?.into_inner();

        if let Some(err) = response.framework_error {
            return Err(RemoteActorError::from(err));
        }

        Ok(())
    }

    /// Sends a typed sub-message implementing [`crate::actor::CallMessage<A>`] to the remote actor
    /// and unpacks its strongly typed [`crate::actor::CallMessage::Reply`].
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, RemoteActorError<A::Error>>
    where
        M: crate::actor::CallMessage<A>,
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        let resp = self.call(msg.into()).await?;
        M::extract_reply(resp).map_err(RemoteActorError::Actor)
    }

    /// Sends a typed sub-message implementing [`crate::actor::CallMessage<A>`] with a client-side
    /// timeout deadline and unpacks its strongly typed [`crate::actor::CallMessage::Reply`].
    pub async fn ask_timeout<M>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> Result<M::Reply, RemoteActorError<A::Error>>
    where
        M: crate::actor::CallMessage<A>,
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        let resp = self.call_timeout(msg.into(), timeout).await?;
        M::extract_reply(resp).map_err(RemoteActorError::Actor)
    }
}

/// Unified client enum providing complete Location Transparency across local and remote actors.
#[derive(Clone)]
pub enum AnyActorClient<A: StatefulActor> {
    /// Local actor client communicating over in-memory bounded channels.
    Local(ActorClient<A>),
    /// Remote actor client communicating over gRPC network transport.
    Remote(RemoteActorClient<A>),
}

impl<A: StatefulActor> std::fmt::Debug for AnyActorClient<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(client) => f.debug_tuple("AnyActorClient::Local").field(client).finish(),
            Self::Remote(client) => f
                .debug_tuple("AnyActorClient::Remote")
                .field(client)
                .finish(),
        }
    }
}

impl<A: StatefulActor> From<ActorClient<A>> for AnyActorClient<A> {
    fn from(client: ActorClient<A>) -> Self {
        Self::Local(client)
    }
}

impl<A: StatefulActor> From<RemoteActorClient<A>> for AnyActorClient<A> {
    fn from(client: RemoteActorClient<A>) -> Self {
        Self::Remote(client)
    }
}

impl<A: StatefulActor> AnyActorClient<A> {
    /// Wraps a local [`ActorClient<A>`] into [`AnyActorClient::Local`].
    #[must_use]
    pub fn from_local(client: ActorClient<A>) -> Self {
        Self::Local(client)
    }

    /// Wraps a remote [`RemoteActorClient<A>`] into [`AnyActorClient::Remote`].
    #[must_use]
    pub fn from_remote(client: RemoteActorClient<A>) -> Self {
        Self::Remote(client)
    }

    /// Connects to a remote node endpoint URL and wraps the client in [`AnyActorClient::Remote`].
    pub async fn connect(
        endpoint: impl Into<String>,
        service_name: impl Into<String>,
        actor_name: impl Into<String>,
    ) -> Result<Self, tonic::transport::Error> {
        Ok(Self::Remote(
            RemoteActorClient::connect(endpoint, service_name, actor_name).await?,
        ))
    }

    /// Connects using a pre-configured Tonic [`Endpoint`] and wraps the client in [`AnyActorClient::Remote`].
    pub async fn connect_with(
        endpoint: Endpoint,
        service_name: impl Into<String>,
        actor_name: impl Into<String>,
    ) -> Result<Self, tonic::transport::Error> {
        Ok(Self::Remote(
            RemoteActorClient::connect_with(endpoint, service_name, actor_name).await?,
        ))
    }

    /// Attaches a per-request metadata hook for remote calls (no-op on [`AnyActorClient::Local`]).
    #[must_use]
    pub fn with_request_metadata<F>(self, injector: F) -> Self
    where
        F: Fn(&mut tonic::metadata::MetadataMap) + Send + Sync + 'static,
    {
        match self {
            Self::Local(client) => Self::Local(client),
            Self::Remote(client) => Self::Remote(client.with_request_metadata(injector)),
        }
    }

    /// Attaches a static `authorization: Bearer <token>` header on remote calls.
    ///
    /// Validates the token upfront even for [`AnyActorClient::Local`] so invalid tokens fail fast
    /// consistently across local tests and remote production deployments.
    pub fn with_bearer_token(
        self,
        token: impl AsRef<str>,
    ) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        match self {
            Self::Local(client) => {
                let _ = parse_bearer_metadata_value(token.as_ref())?;
                Ok(Self::Local(client))
            }
            Self::Remote(client) => Ok(Self::Remote(client.with_bearer_token(token)?)),
        }
    }

    /// Attaches a dynamic Bearer token provider on remote calls (no-op on [`AnyActorClient::Local`]).
    #[must_use]
    pub fn with_bearer_token_provider<F>(self, provider: F) -> Self
    where
        F: Fn() -> Option<String> + Send + Sync + 'static,
    {
        match self {
            Self::Local(client) => Self::Local(client),
            Self::Remote(client) => Self::Remote(client.with_bearer_token_provider(provider)),
        }
    }

    /// Returns the name of the target actor.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Local(client) => client.name(),
            Self::Remote(client) => client.name(),
        }
    }

    /// Returns the name of the target actor (`actor_name`).
    #[must_use]
    pub fn actor_name(&self) -> &str {
        match self {
            Self::Local(client) => client.name(),
            Self::Remote(client) => client.actor_name(),
        }
    }

    /// Returns the name of the service domain managing the target actor.
    #[must_use]
    pub fn service_name(&self) -> &str {
        match self {
            Self::Local(client) => client.service_name(),
            Self::Remote(client) => client.service_name(),
        }
    }

    fn map_local_error(err: crate::actor::ActorError<A::Error>) -> RemoteActorError<A::Error> {
        match err {
            crate::actor::ActorError::HandlerFailed(e) => RemoteActorError::Actor(e),
            crate::actor::ActorError::ActorUnavailable => RemoteActorError::ActorUnavailable,
            crate::actor::ActorError::MailboxFull => RemoteActorError::MailboxFull,
            crate::actor::ActorError::Timeout(_) => RemoteActorError::Timeout,
            crate::actor::ActorError::Supervisor(
                crate::actor::SupervisorError::ChildNotFound(n),
            ) => RemoteActorError::ActorNotFound(n),
            crate::actor::ActorError::Supervisor(
                crate::actor::SupervisorError::MaxRestartsExceeded(name, max, window),
            ) => RemoteActorError::MaxRestartsExceeded(format!(
                "Child '{name}' exceeded max restarts ({max} in {window:?})"
            )),
            crate::actor::ActorError::Supervisor(_) => RemoteActorError::ActorUnavailable,
        }
    }

    /// Sends a request message (`call`) transparently to either a local or remote actor.
    pub async fn call(&self, msg: A::Message) -> Result<A::Response, RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        match self {
            Self::Local(client) => client.call(msg).await.map_err(Self::map_local_error),
            Self::Remote(client) => client.call(msg).await,
        }
    }

    /// Sends a request message (`call`) with a timeout deadline transparently to either a local or remote actor.
    pub async fn call_timeout(
        &self,
        msg: A::Message,
        timeout: Duration,
    ) -> Result<A::Response, RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        match self {
            Self::Local(client) => client
                .call_timeout(msg, timeout)
                .await
                .map_err(Self::map_local_error),
            Self::Remote(client) => client.call_timeout(msg, timeout).await,
        }
    }

    /// Sends a typed sub-message implementing [`crate::actor::CallMessage<A>`] transparently to
    /// either a local or remote actor and unpacks its strongly typed reply.
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, RemoteActorError<A::Error>>
    where
        M: crate::actor::CallMessage<A>,
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        match self {
            Self::Local(client) => client.ask(msg).await.map_err(Self::map_local_error),
            Self::Remote(client) => client.ask(msg).await,
        }
    }

    /// Sends a typed sub-message implementing [`crate::actor::CallMessage<A>`] with a timeout
    /// deadline transparently to either a local or remote actor and unpacks its strongly typed reply.
    pub async fn ask_timeout<M>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> Result<M::Reply, RemoteActorError<A::Error>>
    where
        M: crate::actor::CallMessage<A>,
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        let resp = self.call_timeout(msg.into(), timeout).await?;
        M::extract_reply(resp).map_err(RemoteActorError::Actor)
    }

    /// Sends an asynchronous notification (`cast`) transparently to either a local or remote actor.
    pub async fn cast(&self, msg: A::Message) -> Result<(), RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
    {
        match self {
            Self::Local(client) => client.cast(msg).await.map_err(Self::map_local_error),
            Self::Remote(client) => client.cast(msg).await,
        }
    }

    /// Sends an asynchronous notification (`cast`) with a timeout deadline transparently to either a local or remote actor.
    ///
    /// Passing [`Duration::ZERO`] performs a non-blocking `try_cast` on both local and remote actors,
    /// returning [`RemoteActorError::MailboxFull`] immediately if the bounded mailbox is saturated.
    pub async fn cast_timeout(
        &self,
        msg: A::Message,
        timeout: Duration,
    ) -> Result<(), RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
    {
        match self {
            Self::Local(client) if timeout.is_zero() => {
                client.try_cast(msg).map_err(Self::map_local_error)
            }
            Self::Local(client) => client
                .cast_timeout(msg, timeout)
                .await
                .map_err(Self::map_local_error),
            Self::Remote(client) => client.cast_timeout(msg, timeout).await,
        }
    }
}

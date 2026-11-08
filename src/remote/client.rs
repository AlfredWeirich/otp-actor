//! Strongly typed remote actor client communicating over gRPC with Location Transparency.

use std::marker::PhantomData;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

use crate::actor::{ActorClient, StatefulActor};
use crate::remote::endpoint::{decode_payload, encode_payload};
use crate::remote::error::RemoteActorError;
use crate::remote::proto::call_response::Result as CallResult;
use crate::remote::proto::{ActorNodeClient, CallRequest, CastRequest};

/// Safely converts a [`Duration`] into non-overflowing milliseconds, clamping sub-millisecond
/// non-zero durations to `1ms` so they do not truncate to `0ms` on the wire.
fn duration_to_wire_ms(d: Duration) -> u64 {
    if d.is_zero() {
        0
    } else {
        d.as_millis().clamp(1, u128::from(u64::MAX)) as u64
    }
}

/// Client handle for invoking a remote `StatefulActor` over a gRPC connection.
#[derive(Clone)]
pub struct RemoteActorClient<A: StatefulActor> {
    service_name: String,
    actor_name: String,
    channel: Channel,
    _phantom: PhantomData<A>,
}

impl<A: StatefulActor> RemoteActorClient<A> {
    /// Creates a client from an existing connected gRPC `Channel`.
    pub fn new(
        channel: Channel,
        service_name: impl Into<String>,
        actor_name: impl Into<String>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            actor_name: actor_name.into(),
            channel,
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
        let channel = ep.connect().await?;
        Ok(Self::new(channel, service_name, actor_name))
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

    /// Sends a request message (`call`) to the remote actor and awaits its typed response.
    pub async fn call(&self, msg: A::Message) -> Result<A::Response, RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        self.call_internal(msg, None).await
    }

    /// Sends a request message (`call`) with a client-side timeout deadline.
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
        tokio::time::timeout(timeout, self.call_internal(msg, Some(timeout)))
            .await
            .map_err(|_| RemoteActorError::Timeout)?
    }

    /// Internal execution of gRPC Call RPC.
    async fn call_internal(
        &self,
        msg: A::Message,
        timeout: Option<Duration>,
    ) -> Result<A::Response, RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned,
    {
        let payload =
            encode_payload(&msg).map_err(|e| RemoteActorError::Serialization(e.to_string()))?;

        let mut request = tonic::Request::new(CallRequest {
            service_name: self.service_name.clone(),
            actor_name: self.actor_name.clone(),
            payload,
            timeout_ms: timeout.map(duration_to_wire_ms),
        });
        if let Some(dur) = timeout {
            request.set_timeout(dur);
        }

        let mut client = ActorNodeClient::new(self.channel.clone());
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
    pub async fn cast(&self, msg: A::Message) -> Result<(), RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
    {
        let payload =
            encode_payload(&msg).map_err(|e| RemoteActorError::Serialization(e.to_string()))?;

        let request = CastRequest {
            service_name: self.service_name.clone(),
            actor_name: self.actor_name.clone(),
            payload,
        };

        let mut client = ActorNodeClient::new(self.channel.clone());
        let response = client.cast(request).await?.into_inner();

        if let Some(err) = response.framework_error {
            return Err(RemoteActorError::from(err));
        }

        Ok(())
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

impl<A: StatefulActor> AnyActorClient<A> {
    fn map_local_error(err: crate::actor::ActorError<A::Error>) -> RemoteActorError<A::Error>
    where
        A::Error: ToString,
    {
        match err {
            crate::actor::ActorError::HandlerFailed(e) => RemoteActorError::Actor(e),
            crate::actor::ActorError::ActorUnavailable => RemoteActorError::ActorUnavailable,
            crate::actor::ActorError::Timeout(_) => RemoteActorError::Timeout,
            crate::actor::ActorError::Service(crate::actor::ServiceError::ActorNotFound(n)) => {
                RemoteActorError::ActorNotFound(n)
            }
            crate::actor::ActorError::Service(other) => {
                RemoteActorError::ActorPanic(other.to_string())
            }
        }
    }

    /// Sends a request message (`call`) transparently to either a local or remote actor.
    pub async fn call(&self, msg: A::Message) -> Result<A::Response, RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Response: serde::de::DeserializeOwned,
        A::Error: serde::de::DeserializeOwned + ToString,
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
        A::Error: serde::de::DeserializeOwned + ToString,
    {
        match self {
            Self::Local(client) => client
                .call_timeout(msg, timeout)
                .await
                .map_err(Self::map_local_error),
            Self::Remote(client) => client.call_timeout(msg, timeout).await,
        }
    }

    /// Sends an asynchronous notification (`cast`) transparently to either a local or remote actor.
    pub async fn cast(&self, msg: A::Message) -> Result<(), RemoteActorError<A::Error>>
    where
        A::Message: serde::Serialize,
        A::Error: ToString,
    {
        match self {
            Self::Local(client) => client.cast(msg).await.map_err(Self::map_local_error),
            Self::Remote(client) => client.cast(msg).await,
        }
    }
}

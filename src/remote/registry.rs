//! Thread-safe registry mapping `(service_name, actor_name)` to `ByteEndpoint`s.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use crate::actor::{ActorClient, StatefulActor};
use crate::remote::endpoint::ByteEndpoint;
use crate::remote::proto::RemoteError;
use crate::remote::proto::remote_error::Kind;

type EndpointMap = HashMap<(String, String), Arc<dyn ByteEndpoint>>;

/// Registry that routes incoming gRPC calls to local actors.
#[derive(Clone, Default)]
pub struct ActorNodeRegistry {
    endpoints: Arc<RwLock<EndpointMap>>,
}

impl ActorNodeRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a generic `ByteEndpoint` directly.
    pub fn register_endpoint(
        &self,
        service_name: impl Into<String>,
        actor_name: impl Into<String>,
        endpoint: Arc<dyn ByteEndpoint>,
    ) {
        let mut map = self
            .endpoints
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        map.insert((service_name.into(), actor_name.into()), endpoint);
    }

    /// Registers a strongly typed `ActorClient<A>` under the specified service and actor names.
    pub fn register<A>(
        &self,
        service_name: impl Into<String>,
        actor_name: impl Into<String>,
        client: ActorClient<A>,
    ) where
        A: StatefulActor + 'static,
        A::Message: serde::de::DeserializeOwned + Send + 'static,
        A::Response: serde::Serialize + Send + 'static,
        A::Error: serde::Serialize + ToString + Send + Sync + 'static,
    {
        self.register_endpoint(service_name, actor_name, Arc::new(client));
    }

    /// Registers a strongly typed `ActorClient<A>` using its intrinsic `service_name()` and `name()`.
    pub fn register_client<A>(&self, client: ActorClient<A>)
    where
        A: StatefulActor + 'static,
        A::Message: serde::de::DeserializeOwned + Send + 'static,
        A::Response: serde::Serialize + Send + 'static,
        A::Error: serde::Serialize + ToString + Send + Sync + 'static,
    {
        let service_name = client.service_name().to_string();
        let actor_name = client.name().to_string();
        self.register_endpoint(service_name, actor_name, Arc::new(client));
    }

    /// Unregisters an actor endpoint from the node registry.
    pub fn unregister(&self, service_name: &str, actor_name: &str) -> bool {
        let mut map = self
            .endpoints
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        map.remove(&(service_name.to_string(), actor_name.to_string()))
            .is_some()
    }

    /// Looks up the actor endpoint for the given service and actor name.
    pub fn get(&self, service_name: &str, actor_name: &str) -> Option<Arc<dyn ByteEndpoint>> {
        let map = self
            .endpoints
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        map.get(&(service_name.to_string(), actor_name.to_string()))
            .cloned()
    }

    /// Resolves the actor endpoint, distinguishing between `ServiceNotFound` and `ActorNotFound`.
    pub fn resolve(
        &self,
        service_name: &str,
        actor_name: &str,
    ) -> Result<Arc<dyn ByteEndpoint>, RemoteError> {
        let map = self
            .endpoints
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(ep) = map.get(&(service_name.to_string(), actor_name.to_string())) {
            return Ok(ep.clone());
        }

        let service_exists = map.keys().any(|(svc, _)| svc == service_name);
        if service_exists {
            Err(RemoteError {
                kind: Kind::ActorNotFound as i32,
                message: format!("{service_name}/{actor_name}"),
            })
        } else {
            Err(RemoteError {
                kind: Kind::ServiceNotFound as i32,
                message: service_name.to_string(),
            })
        }
    }
}

//! Thread-safe registry mapping `(service_name, actor_name)` to `ByteEndpoint`s.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use crate::actor::{ActorClient, StatefulActor};
use crate::remote::endpoint::ByteEndpoint;
use crate::remote::proto::RemoteError;
use crate::remote::proto::remote_error::Kind;

type EndpointMap = HashMap<String, HashMap<String, Arc<dyn ByteEndpoint>>>;

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
        map.entry(service_name.into())
            .or_default()
            .insert(actor_name.into(), endpoint);
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
        if let Some(actors) = map.get_mut(service_name) {
            let removed = actors.remove(actor_name).is_some();
            if actors.is_empty() {
                map.remove(service_name);
            }
            removed
        } else {
            false
        }
    }

    /// Looks up the actor endpoint for the given service and actor name without heap allocations.
    pub fn get(&self, service_name: &str, actor_name: &str) -> Option<Arc<dyn ByteEndpoint>> {
        let map = self
            .endpoints
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        map.get(service_name)
            .and_then(|actors| actors.get(actor_name))
            .cloned()
    }

    /// Resolves the actor endpoint without heap allocations on the hot path,
    /// distinguishing between `ServiceNotFound` and `ActorNotFound` in $O(1)$.
    pub fn resolve(
        &self,
        service_name: &str,
        actor_name: &str,
    ) -> Result<Arc<dyn ByteEndpoint>, RemoteError> {
        let map = self
            .endpoints
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        match map.get(service_name) {
            Some(actors) => match actors.get(actor_name) {
                Some(ep) => Ok(ep.clone()),
                None => Err(RemoteError {
                    kind: Kind::ActorNotFound as i32,
                    message: format!("{service_name}/{actor_name}"),
                }),
            },
            None => Err(RemoteError {
                kind: Kind::ServiceNotFound as i32,
                message: service_name.to_string(),
            }),
        }
    }
}

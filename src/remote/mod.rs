//! # Remote Actors over gRPC
//!
//! Provides network-transparent actor communication over gRPC using standard Protobuf
//! envelopes and serde payload serialization:
//! - **Location Transparency**: [`RemoteActorClient`] and [`AnyActorClient`] provide the same ergonomics
//!   as local [`crate::actor::ActorClient`].
//! - **Self-Healing Supervision**: Remote calls trigger transparent rebinding and supervisor recovery
//!   on the hosting node upon actor crashes.
//! - **Coordinated 2-Phase Graceful Shutdown**: [`ActorNodeHandle`] cleanly drains in-flight gRPC calls
//!   (`HTTP/2 GOAWAY`) before executing cascading subtree shutdowns.

pub mod client;
pub mod endpoint;
pub mod error;
pub mod proto;
pub mod registry;
pub mod server;

pub use client::{AnyActorClient, RemoteActorClient};
pub use endpoint::ByteEndpoint;
pub use error::RemoteActorError;
pub use proto::{ActorNodeClient, ActorNodeServer};
pub use registry::ActorNodeRegistry;
pub use server::{ActorNodeHandle, ActorNodeService, DEFAULT_DRAIN_TIMEOUT};

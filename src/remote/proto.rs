//! Protobuf definitions and generated gRPC types for `otp-actor`.

#[allow(clippy::all)]
pub mod v1 {
    tonic::include_proto!("otp_actor.remote.v1");
}

pub use prost::bytes::Bytes;
pub use v1::actor_node_admin_client::ActorNodeAdminClient;
pub use v1::actor_node_admin_server::{ActorNodeAdmin, ActorNodeAdminServer};
pub use v1::actor_node_client::ActorNodeClient;
pub use v1::actor_node_server::{ActorNode, ActorNodeServer};
pub use v1::{
    CallRequest, CallResponse, CastRequest, CastResponse, HealthRequest, HealthResponse,
    RemoteError, StatusRequest, StatusResponse, call_response, remote_error, status_response,
};

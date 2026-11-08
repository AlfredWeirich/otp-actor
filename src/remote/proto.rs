//! Protobuf definitions and generated gRPC types for `otp-actor`.

#[allow(clippy::all)]
pub mod v1 {
    tonic::include_proto!("otp_actor.remote.v1");
}

pub use v1::actor_node_client::ActorNodeClient;
pub use v1::actor_node_server::{ActorNode, ActorNodeServer};
pub use v1::{
    CallRequest, CallResponse, CastRequest, CastResponse, RemoteError, call_response, remote_error,
};

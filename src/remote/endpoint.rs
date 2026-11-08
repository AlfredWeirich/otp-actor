//! Type-erased `ByteEndpoint` interface bridging raw byte payloads and typed `ActorClient<A>`.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use bincode::Options;

use crate::actor::{ActorClient, StatefulActor};
use crate::remote::proto::call_response::Result as CallResult;
use crate::remote::proto::remote_error::Kind;
use crate::remote::proto::{CallResponse, CastResponse, RemoteError};

/// Maximum serialized payload size (4 MiB, aligned with default gRPC frame limit)
/// to guard against untrusted length-prefix allocation bombs.
pub const MAX_REMOTE_PAYLOAD_BYTES: u64 = 4 * 1024 * 1024;

/// Encodes a value using bounded bincode options compatible with standard fixed-int encoding.
pub(crate) fn encode_payload<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, bincode::Error> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .with_limit(MAX_REMOTE_PAYLOAD_BYTES)
        .serialize(val)
}

/// Decodes a value from a byte slice with an allocation cap of [`MAX_REMOTE_PAYLOAD_BYTES`].
pub(crate) fn decode_payload<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, bincode::Error> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .with_limit(MAX_REMOTE_PAYLOAD_BYTES)
        .deserialize(bytes)
}

/// Type-erased endpoint for receiving raw serialized byte messages and routing them
/// to a concrete actor client.
pub trait ByteEndpoint: Send + Sync + 'static {
    /// Dispatches a raw byte call message to the underlying actor.
    fn call_bytes<'a>(
        &'a self,
        payload: &'a [u8],
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn Future<Output = CallResponse> + Send + 'a>>;

    /// Dispatches a raw byte cast message to the underlying actor.
    fn cast_bytes<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = CastResponse> + Send + 'a>>;
}

impl<A> ByteEndpoint for ActorClient<A>
where
    A: StatefulActor + 'static,
    A::Message: serde::de::DeserializeOwned + Send + 'static,
    A::Response: serde::Serialize + Send + 'static,
    A::Error: serde::Serialize + ToString + Send + Sync + 'static,
{
    fn call_bytes<'a>(
        &'a self,
        payload: &'a [u8],
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn Future<Output = CallResponse> + Send + 'a>> {
        Box::pin(async move {
            let msg: A::Message = match decode_payload(payload) {
                Ok(m) => m,
                Err(err) => {
                    return CallResponse {
                        result: Some(CallResult::FrameworkError(RemoteError {
                            kind: Kind::Deserialization as i32,
                            message: format!("Failed to deserialize request payload: {err}"),
                        })),
                    };
                }
            };

            let call_res = match timeout {
                Some(dur) => self.call_timeout(msg, dur).await,
                None => self.call(msg).await,
            };

            match call_res {
                Ok(resp) => match encode_payload(&resp) {
                    Ok(bytes) => CallResponse {
                        result: Some(CallResult::OkPayload(bytes)),
                    },
                    Err(err) => CallResponse {
                        result: Some(CallResult::FrameworkError(RemoteError {
                            kind: Kind::Serialization as i32,
                            message: format!("Failed to serialize response payload: {err}"),
                        })),
                    },
                },
                Err(crate::actor::ActorError::HandlerFailed(domain_err)) => {
                    match encode_payload(&domain_err) {
                        Ok(bytes) => CallResponse {
                            result: Some(CallResult::ActorErrorPayload(bytes)),
                        },
                        Err(err) => CallResponse {
                            result: Some(CallResult::FrameworkError(RemoteError {
                                kind: Kind::Serialization as i32,
                                message: format!("Failed to serialize actor domain error: {err}"),
                            })),
                        },
                    }
                }
                Err(other_err) => CallResponse {
                    result: Some(CallResult::FrameworkError(other_err.into())),
                },
            }
        })
    }

    fn cast_bytes<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = CastResponse> + Send + 'a>> {
        Box::pin(async move {
            let msg: A::Message = match decode_payload(payload) {
                Ok(m) => m,
                Err(err) => {
                    return CastResponse {
                        framework_error: Some(RemoteError {
                            kind: Kind::Deserialization as i32,
                            message: format!("Failed to deserialize cast payload: {err}"),
                        }),
                    };
                }
            };

            match self.cast(msg).await {
                Ok(()) => CastResponse {
                    framework_error: None,
                },
                Err(err) => CastResponse {
                    framework_error: Some(err.into()),
                },
            }
        })
    }
}

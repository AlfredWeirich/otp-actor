//! Type-erased `ByteEndpoint` interface bridging raw byte payloads and typed `ActorClient<A>`.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::actor::{ActorClient, StatefulActor};
use crate::remote::proto::call_response::Result as CallResult;
use crate::remote::proto::remote_error::Kind;
use crate::remote::proto::{CallResponse, CastResponse, RemoteError};

/// Maximum serialized payload size (4 MiB, aligned with default gRPC frame limit)
/// to guard against oversized messages and allocation bombs.
pub const MAX_REMOTE_PAYLOAD_BYTES: u64 = 4 * 1024 * 1024;

/// Maximum gRPC Protobuf frame size (4 MiB payload + 64 KiB envelope headroom)
/// configured on Tonic server and client codecs so full 4 MiB payloads do not fail frame decoding.
pub const MAX_GRPC_FRAME_BYTES: usize = (MAX_REMOTE_PAYLOAD_BYTES as usize) + 64 * 1024;

/// Default backpressure timeout for remote `cast` operations before returning `MailboxFull`.
pub const DEFAULT_REMOTE_CAST_TIMEOUT: Duration = Duration::from_secs(5);

/// Current wire protocol version sent in [`crate::remote::proto::CallRequest`] and
/// [`crate::remote::proto::CastRequest`] envelopes.
pub const REMOTE_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PayloadCodecError {
    #[error("payload size ({size} bytes) exceeds maximum allowed ({max} bytes)")]
    SizeLimitExceeded { size: usize, max: usize },
    #[error("trailing {remaining} unconsumed byte(s) after payload deserialization")]
    TrailingBytes { remaining: usize },
    #[error(transparent)]
    Postcard(#[from] postcard::Error),
}

struct BoundedVec {
    buf: Vec<u8>,
    max_len: usize,
}

impl postcard::ser_flavors::Flavor for BoundedVec {
    type Output = Vec<u8>;

    fn try_extend(&mut self, data: &[u8]) -> postcard::Result<()> {
        let next_len = self
            .buf
            .len()
            .checked_add(data.len())
            .filter(|&len| len <= self.max_len)
            .ok_or(postcard::Error::SerializeBufferFull)?;
        self.buf.reserve(next_len - self.buf.len());
        self.buf.extend_from_slice(data);
        Ok(())
    }

    fn try_push(&mut self, data: u8) -> postcard::Result<()> {
        if self.buf.len() >= self.max_len {
            return Err(postcard::Error::SerializeBufferFull);
        }
        self.buf.push(data);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(self.buf)
    }
}

/// Encodes a value with `postcard`, aborting before allocating beyond [`MAX_REMOTE_PAYLOAD_BYTES`].
pub(crate) fn encode_payload<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, PayloadCodecError> {
    let max_len = MAX_REMOTE_PAYLOAD_BYTES as usize;
    let bytes = postcard::serialize_with_flavor(
        val,
        BoundedVec {
            buf: Vec::new(),
            max_len,
        },
    )?;
    Ok(bytes)
}

/// Decodes a `postcard`-encoded value from a byte slice capped at [`MAX_REMOTE_PAYLOAD_BYTES`],
/// rejecting any trailing unconsumed bytes.
pub(crate) fn decode_payload<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, PayloadCodecError> {
    let max = MAX_REMOTE_PAYLOAD_BYTES as usize;
    if bytes.len() > max {
        return Err(PayloadCodecError::SizeLimitExceeded {
            size: bytes.len(),
            max,
        });
    }
    let (value, remainder) = postcard::take_from_bytes(bytes)?;
    if !remainder.is_empty() {
        return Err(PayloadCodecError::TrailingBytes {
            remaining: remainder.len(),
        });
    }
    Ok(value)
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
        timeout: Option<Duration>,
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
        timeout: Option<Duration>,
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

            match timeout {
                Some(dur) => match self.cast_timeout(msg, dur).await {
                    Ok(()) => CastResponse {
                        framework_error: None,
                    },
                    Err(err) => CastResponse {
                        framework_error: Some(err.into()),
                    },
                },
                None => {
                    match tokio::time::timeout(DEFAULT_REMOTE_CAST_TIMEOUT, self.cast(msg)).await {
                        Ok(Ok(())) => CastResponse {
                            framework_error: None,
                        },
                        Ok(Err(err)) => CastResponse {
                            framework_error: Some(err.into()),
                        },
                        Err(_) => CastResponse {
                            framework_error: Some(RemoteError {
                                kind: Kind::MailboxFull as i32,
                                message: format!(
                                    "Remote actor mailbox remained full for {:?}",
                                    DEFAULT_REMOTE_CAST_TIMEOUT
                                ),
                            }),
                        },
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postcard_payload_roundtrip_and_bounds() {
        let input = ("hello".to_string(), 42_u64, vec![1_u8, 2, 3]);
        let encoded = encode_payload(&input).expect("encode should succeed");
        let decoded: (String, u64, Vec<u8>) =
            decode_payload(&encoded).expect("decode should succeed");
        assert_eq!(decoded, input);

        // Trailing bytes must be rejected
        let mut with_trailing = encoded.clone();
        with_trailing.push(0xFF);
        let trailing_err = decode_payload::<(String, u64, Vec<u8>)>(&with_trailing).unwrap_err();
        assert!(
            matches!(trailing_err, PayloadCodecError::TrailingBytes { remaining: 1 }),
            "Expected TrailingBytes, got {trailing_err:?}"
        );

        // Oversized encode must fail cleanly at MAX_REMOTE_PAYLOAD_BYTES
        let too_large_encode = vec![0_u8; (MAX_REMOTE_PAYLOAD_BYTES as usize) + 1];
        assert!(encode_payload(&too_large_encode).is_err());

        // Oversized decode buffer must fail cleanly before deserializing
        let too_large_decode = vec![0_u8; (MAX_REMOTE_PAYLOAD_BYTES as usize) + 1];
        let size_err = decode_payload::<Vec<u8>>(&too_large_decode).unwrap_err();
        assert!(
            matches!(size_err, PayloadCodecError::SizeLimitExceeded { .. }),
            "Expected SizeLimitExceeded, got {size_err:?}"
        );
    }
}

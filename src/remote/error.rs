//! Error types for remote actor interactions across network boundaries.

use crate::remote::proto::RemoteError;
use crate::remote::proto::remote_error::Kind;
use std::fmt::{self, Debug, Display};

/// Error encountered when communicating with a remote actor over gRPC.
#[derive(Debug, Clone)]
pub enum RemoteActorError<E> {
    /// Domain-specific error returned by the remote actor's `handle` method.
    Actor(E),

    /// Transport-level network or gRPC communication failure.
    Transport(tonic::Status),

    /// The target service supervisor was not registered on the remote node.
    ServiceNotFound(String),

    /// The target actor was not registered on the remote node.
    ActorNotFound(String),

    /// The remote actor's mailbox is currently saturated.
    MailboxFull,

    /// The remote actor is temporarily unavailable (e.g. recovering or restarting).
    ActorUnavailable,

    /// Operation timed out waiting for the remote node or actor.
    Timeout,

    /// Failure serializing the message payload before transmission.
    Serialization(String),

    /// Failure deserializing the received response payload.
    Deserialization(String),

    /// The remote actor panicked or crashed while handling the message.
    ActorPanic(String),
}

impl<E: Display> Display for RemoteActorError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(err) => write!(f, "Remote actor domain error: {err}"),
            Self::Transport(status) => write!(f, "gRPC transport error: {status}"),
            Self::ServiceNotFound(svc) => write!(f, "Service '{svc}' not found on remote node"),
            Self::ActorNotFound(act) => write!(f, "Actor '{act}' not found on remote node"),
            Self::MailboxFull => write!(f, "Remote actor mailbox is full"),
            Self::ActorUnavailable => write!(f, "Remote actor is temporarily unavailable"),
            Self::Timeout => write!(f, "Remote actor operation timed out"),
            Self::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            Self::Deserialization(msg) => write!(f, "Deserialization error: {msg}"),
            Self::ActorPanic(msg) => write!(f, "Remote actor panicked or crashed: {msg}"),
        }
    }
}

impl<E: Debug + Display> std::error::Error for RemoteActorError<E> {}

impl<E> From<tonic::Status> for RemoteActorError<E> {
    fn from(status: tonic::Status) -> Self {
        if status.code() == tonic::Code::DeadlineExceeded
            || (status.code() == tonic::Code::Cancelled
                && status.message().contains("Timeout expired"))
        {
            Self::Timeout
        } else {
            Self::Transport(status)
        }
    }
}

impl<E> From<RemoteError> for RemoteActorError<E> {
    fn from(err: RemoteError) -> Self {
        let kind = Kind::try_from(err.kind).unwrap_or(Kind::Unspecified);
        match kind {
            Kind::ServiceNotFound => RemoteActorError::ServiceNotFound(err.message),
            Kind::ActorNotFound => RemoteActorError::ActorNotFound(err.message),
            Kind::MailboxFull => RemoteActorError::MailboxFull,
            Kind::ActorUnavailable => RemoteActorError::ActorUnavailable,
            Kind::Timeout => RemoteActorError::Timeout,
            Kind::Serialization => RemoteActorError::Serialization(err.message),
            Kind::Deserialization => RemoteActorError::Deserialization(err.message),
            Kind::ActorPanic => RemoteActorError::ActorPanic(err.message),
            Kind::Unspecified => RemoteActorError::ActorPanic(err.message),
        }
    }
}

impl<E: ToString> From<crate::actor::ActorError<E>> for RemoteError {
    fn from(err: crate::actor::ActorError<E>) -> Self {
        match err {
            crate::actor::ActorError::ActorUnavailable => RemoteError {
                kind: Kind::ActorUnavailable as i32,
                message: "Actor task terminated unexpectedly or is unavailable".into(),
            },
            crate::actor::ActorError::Service(crate::actor::ServiceError::ActorNotFound(name)) => {
                RemoteError {
                    kind: Kind::ActorNotFound as i32,
                    message: name,
                }
            }
            crate::actor::ActorError::Service(other_svc_err) => RemoteError {
                kind: Kind::ActorUnavailable as i32,
                message: other_svc_err.to_string(),
            },
            crate::actor::ActorError::Timeout(duration) => RemoteError {
                kind: Kind::Timeout as i32,
                message: format!("Actor call timed out after {duration:?}"),
            },
            crate::actor::ActorError::HandlerFailed(domain_err) => RemoteError {
                kind: Kind::Unspecified as i32,
                message: format!("Unhandled actor domain error: {}", domain_err.to_string()),
            },
        }
    }
}

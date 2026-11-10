//! Error types for remote actor interactions across network boundaries.

use crate::remote::proto::RemoteError;
use crate::remote::proto::remote_error::Kind;
use std::fmt::{self, Debug, Display};

/// Error encountered when starting or shutting down a remote [`crate::remote::ActorNodeHandle`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum RemoteNodeError {
    /// Failed to bind TCP listener socket.
    #[error("Failed to bind TCP listener: {0}")]
    Bind(#[from] std::io::Error),

    /// Tonic gRPC server transport error.
    #[error("gRPC server transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// Server background task panicked or was cancelled.
    #[error("Server task failed to join: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// Attached supervisor subtree failed to shut down cleanly.
    #[error("Supervisor shutdown error: {0}")]
    Supervisor(#[from] crate::actor::SupervisorError),
}

/// Error encountered when communicating with a remote actor over gRPC.
#[non_exhaustive]
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

    /// The remote actor exceeded its restart intensity budget and was permanently terminated.
    MaxRestartsExceeded(String),

    /// Operation timed out waiting for the remote node or actor.
    Timeout,

    /// Failure serializing the message payload before transmission.
    Serialization(String),

    /// Failure deserializing the received response payload.
    Deserialization(String),

    /// The remote actor panicked or crashed while handling the message.
    ActorPanic(String),

    /// The client and remote server speak incompatible wire protocol versions.
    IncompatibleProtocol(String),

    /// Unrecognized framework error code received from a newer or incompatible node.
    Unknown { kind: i32, message: String },
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
            Self::MaxRestartsExceeded(msg) => {
                write!(f, "Remote actor exceeded max restarts: {msg}")
            }
            Self::Timeout => write!(f, "Remote actor operation timed out"),
            Self::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            Self::Deserialization(msg) => write!(f, "Deserialization error: {msg}"),
            Self::ActorPanic(msg) => write!(f, "Remote actor panicked or crashed: {msg}"),
            Self::IncompatibleProtocol(msg) => write!(f, "Incompatible remote protocol: {msg}"),
            Self::Unknown { kind, message } => {
                write!(f, "Unknown remote framework error (kind={kind}): {message}")
            }
        }
    }
}

impl<E: Debug + Display> std::error::Error for RemoteActorError<E> {}

fn status_is_timeout(status: &tonic::Status) -> bool {
    if status.code() == tonic::Code::DeadlineExceeded {
        return true;
    }
    let mut source = std::error::Error::source(status);
    while let Some(err) = source {
        if err.is::<tonic::TimeoutExpired>() {
            return true;
        }
        source = err.source();
    }
    status.code() == tonic::Code::Cancelled && status.message() == "Timeout expired"
}

impl<E> From<tonic::Status> for RemoteActorError<E> {
    fn from(status: tonic::Status) -> Self {
        if status_is_timeout(&status) {
            Self::Timeout
        } else {
            Self::Transport(status)
        }
    }
}

impl<E> From<RemoteError> for RemoteActorError<E> {
    fn from(err: RemoteError) -> Self {
        match Kind::try_from(err.kind) {
            Ok(Kind::ServiceNotFound) => RemoteActorError::ServiceNotFound(err.message),
            Ok(Kind::ActorNotFound) => RemoteActorError::ActorNotFound(err.message),
            Ok(Kind::MailboxFull) => RemoteActorError::MailboxFull,
            Ok(Kind::ActorUnavailable) => RemoteActorError::ActorUnavailable,
            Ok(Kind::MaxRestartsExceeded) => RemoteActorError::MaxRestartsExceeded(err.message),
            Ok(Kind::Timeout) => RemoteActorError::Timeout,
            Ok(Kind::Serialization) => RemoteActorError::Serialization(err.message),
            Ok(Kind::Deserialization) => RemoteActorError::Deserialization(err.message),
            Ok(Kind::ActorPanic) => RemoteActorError::ActorPanic(err.message),
            Ok(Kind::IncompatibleProtocol) => RemoteActorError::IncompatibleProtocol(err.message),
            Ok(Kind::Unspecified) => RemoteActorError::ActorPanic(err.message),
            Err(_) => RemoteActorError::Unknown {
                kind: err.kind,
                message: err.message,
            },
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
            crate::actor::ActorError::MailboxFull => RemoteError {
                kind: Kind::MailboxFull as i32,
                message: "Actor mailbox is full".into(),
            },
            crate::actor::ActorError::Supervisor(
                crate::actor::SupervisorError::ChildNotFound(name),
            ) => RemoteError {
                kind: Kind::ActorNotFound as i32,
                message: name,
            },
            crate::actor::ActorError::Supervisor(
                crate::actor::SupervisorError::MaxRestartsExceeded(name, max, window),
            ) => RemoteError {
                kind: Kind::MaxRestartsExceeded as i32,
                message: format!("Child '{name}' exceeded max restarts ({max} in {window:?})"),
            },
            crate::actor::ActorError::Supervisor(other_sup_err) => RemoteError {
                kind: Kind::ActorUnavailable as i32,
                message: other_sup_err.to_string(),
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

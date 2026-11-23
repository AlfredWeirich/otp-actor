//! Error types for remote actor interactions across network boundaries.

use crate::remote::proto::RemoteError;
use crate::remote::proto::remote_error::Kind;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

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
    Transport(Arc<tonic::Status>),

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

impl<E> RemoteActorError<E> {
    /// Wraps a [`tonic::Status`] into [`RemoteActorError::Timeout`] if it represents a deadline
    /// expiration, or into [`RemoteActorError::Transport`] otherwise.
    #[must_use]
    pub fn transport(status: tonic::Status) -> Self {
        Self::from(status)
    }

    /// Returns a reference to the underlying [`tonic::Status`] when this error is
    /// [`RemoteActorError::Transport`].
    #[must_use]
    pub fn transport_status(&self) -> Option<&tonic::Status> {
        match self {
            Self::Transport(status) => Some(status.as_ref()),
            _ => None,
        }
    }

    /// Returns a reference to the actor's domain error when this error is
    /// [`RemoteActorError::Actor`].
    #[must_use]
    pub fn actor_error(&self) -> Option<&E> {
        match self {
            Self::Actor(err) => Some(err),
            _ => None,
        }
    }
}

impl<E: PartialEq> PartialEq for RemoteActorError<E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Actor(a), Self::Actor(b)) => a == b,
            (Self::Transport(a), Self::Transport(b)) => {
                Arc::ptr_eq(a, b)
                    || (a.code() == b.code()
                        && a.message() == b.message()
                        && a.details() == b.details())
            }
            (Self::ServiceNotFound(a), Self::ServiceNotFound(b)) => a == b,
            (Self::ActorNotFound(a), Self::ActorNotFound(b)) => a == b,
            (Self::MailboxFull, Self::MailboxFull) => true,
            (Self::ActorUnavailable, Self::ActorUnavailable) => true,
            (Self::MaxRestartsExceeded(a), Self::MaxRestartsExceeded(b)) => a == b,
            (Self::Timeout, Self::Timeout) => true,
            (Self::Serialization(a), Self::Serialization(b)) => a == b,
            (Self::Deserialization(a), Self::Deserialization(b)) => a == b,
            (Self::ActorPanic(a), Self::ActorPanic(b)) => a == b,
            (Self::IncompatibleProtocol(a), Self::IncompatibleProtocol(b)) => a == b,
            (
                Self::Unknown {
                    kind: k1,
                    message: m1,
                },
                Self::Unknown {
                    kind: k2,
                    message: m2,
                },
            ) => k1 == k2 && m1 == m2,
            _ => false,
        }
    }
}

impl<E: Eq> Eq for RemoteActorError<E> {}

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

impl<E: Debug + Display> std::error::Error for RemoteActorError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(status) => Some(status.as_ref()),
            _ => None,
        }
    }
}

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
            Self::Transport(Arc::new(status))
        }
    }
}

impl<E> From<Arc<tonic::Status>> for RemoteActorError<E> {
    fn from(status: Arc<tonic::Status>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn test_remote_actor_error_preserves_transport_source_chain() {
        let status = tonic::Status::permission_denied("missing bearer token");
        let err: RemoteActorError<String> = RemoteActorError::transport(status);
        let source = err
            .source()
            .expect("Transport variant must expose tonic::Status via Error::source()");
        let downcast = source
            .downcast_ref::<tonic::Status>()
            .expect("source must downcast to tonic::Status");
        assert_eq!(downcast.code(), tonic::Code::PermissionDenied);
        assert_eq!(downcast.message(), "missing bearer token");
        assert_eq!(
            err.transport_status().map(tonic::Status::code),
            Some(tonic::Code::PermissionDenied)
        );
        assert!(err.actor_error().is_none());

        let same_err: RemoteActorError<String> =
            RemoteActorError::from(tonic::Status::permission_denied("missing bearer token"));
        let diff_err: RemoteActorError<String> =
            RemoteActorError::from(tonic::Status::unauthenticated("missing bearer token"));
        assert_eq!(err, err.clone());
        assert_eq!(err, same_err);
        assert_ne!(err, diff_err);
        assert!(
            std::mem::size_of::<RemoteActorError<String>>() <= 40,
            "Expected Arc-boxed Transport variant to keep RemoteActorError compact, got {} bytes",
            std::mem::size_of::<RemoteActorError<String>>()
        );

        let domain_err: RemoteActorError<String> = RemoteActorError::Actor("boom".to_string());
        assert_eq!(domain_err.actor_error().map(String::as_str), Some("boom"));
        assert!(domain_err.transport_status().is_none());
        assert!(domain_err.source().is_none());
        assert_eq!(domain_err, RemoteActorError::Actor("boom".to_string()));

        let timeout_err: RemoteActorError<String> = RemoteActorError::Timeout;
        assert!(timeout_err.source().is_none());
        assert_eq!(timeout_err, RemoteActorError::Timeout);
    }
}

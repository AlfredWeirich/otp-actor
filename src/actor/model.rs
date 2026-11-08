//! # Domain Models and Supervision Tree Protocol Definitions
//!
//! Defines the core data structures, error types, commands, and restart policies
//! utilized across the multi-level supervision hierarchy (Root -> Service -> Actors).

use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

/// Safety margin threshold: Asynchronous refresh requests are triggered before actual expiration.
/// Set to 2 seconds for this demonstration (typically 5 minutes / 300s in production environments).
pub const EXPIRY_BUFFER: Duration = Duration::from_secs(2);

/// Default grace period granted to an actor or service during graceful shutdown (5 seconds).
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Result returned by supervised actor and supervisor tasks upon termination.
/// - `Ok(())` indicates a clean, normal shutdown.
/// - `Err(reason)` signals an abnormal crash or failure escalation that triggers supervision recovery.
pub type TaskResult = Result<(), String>;

// ============================================================================
// 1. Restart Policy, Restart Type, Shutdown Policy, Strategies & Crash Tracking
// ============================================================================

/// Defines when a supervised child actor should be restarted by its supervisor upon termination
/// (modeled after Erlang/OTP child specification restart rules).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestartType {
    /// The child actor is restarted only if it terminates abnormally (`Err` or panic).
    /// If it terminates normally (`Ok(())`), it is not restarted.
    #[default]
    Transient,

    /// The child actor is always restarted, regardless of whether it terminates normally (`Ok(())`)
    /// or abnormally (`Err` / panic), unless explicitly stopped via the supervisor.
    /// Best for critical long-lived daemons and resource actors.
    Permanent,

    /// The child actor is never restarted (neither on normal exit nor on crash/panic, nor when
    /// siblings restart under `OneForAll` / `RestForOne`). Best for one-shot tasks.
    Temporary,
}

/// Defines how a supervisor terminates a child actor or service during shutdown or stop operations
/// (modeled after Erlang/OTP shutdown specifications).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownPolicy {
    /// Signals graceful shutdown and waits up to the specified timeout for the task to complete
    /// in-flight work before forcefully aborting it.
    Timeout(Duration),

    /// Immediately aborts the task (`AbortHandle::abort`) without waiting for graceful cleanup.
    BrutalKill,

    /// Signals graceful shutdown and waits indefinitely for the task to terminate cleanly.
    Infinity,
}

impl Default for ShutdownPolicy {
    fn default() -> Self {
        Self::Timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }
}

/// Supervision restart strategy governing how sibling actors are treated when one actor crashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SupervisionStrategy {
    /// Only the crashed actor is restarted. Sibling actors remain completely unaffected.
    /// Best for independent, shared-nothing actors.
    #[default]
    OneForOne,

    /// If any actor crashes, ALL actors supervised by this supervisor are terminated
    /// and restarted in order of registration.
    /// Best for tightly coupled actors sharing in-memory session or dependent state.
    OneForAll,

    /// If an actor crashes, that actor and all subsequent actors registered AFTER it
    /// are terminated and restarted in order of registration. Earlier actors remain untouched.
    /// Best for linear dependency pipelines (A -> B -> C).
    RestForOne,
}

/// Configuration defining restart rate-limiting and backoff behavior for supervised actors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RestartPolicy {
    /// Maximum number of restarts allowed within the time window.
    pub max_restarts: usize,
    /// Time window over which restart frequencies are tracked.
    pub time_window: Duration,
    /// Base delay applied before spawning the replacement actor on first restart.
    pub initial_backoff: Duration,
    /// Maximum backoff ceiling across consecutive failures.
    pub max_backoff: Duration,
    /// Factor by which backoff increases after each consecutive crash.
    pub backoff_multiplier: f64,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            time_window: Duration::from_secs(10),
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(2),
            backoff_multiplier: 2.0,
        }
    }
}

/// Helper tracking restart timestamps and computing exponential backoff.
#[derive(Debug, Clone)]
pub struct RestartTracker {
    pub policy: RestartPolicy,
    pub restarts: Vec<Instant>,
    pub consecutive_failures: usize,
}

impl RestartTracker {
    pub fn new(policy: RestartPolicy) -> Self {
        Self {
            policy,
            restarts: Vec::new(),
            consecutive_failures: 0,
        }
    }

    /// Records a crash.
    /// Returns `Some(backoff)` if a restart is allowed within the configured rate limit,
    /// or `None` if the crash threshold has been exceeded.
    pub fn record_crash(&mut self) -> Option<Duration> {
        let now = Instant::now();
        self.restarts
            .retain(|&t| now.duration_since(t) <= self.policy.time_window);

        // If all past crashes are outside the time window, the component has stabilized:
        // reset consecutive failures so backoff does not stay permanently saturated!
        if self.restarts.is_empty() {
            self.consecutive_failures = 0;
        }

        if self.restarts.len() >= self.policy.max_restarts {
            return None;
        }

        self.restarts.push(now);
        self.consecutive_failures += 1;

        // Cap exponent at 30 to prevent f64 overflow / NaN / Infinity panics
        let exp = (self.consecutive_failures.saturating_sub(1)).min(30) as i32;
        let multiplier = self.policy.backoff_multiplier.powi(exp);
        let backoff_secs = self.policy.initial_backoff.as_secs_f64() * multiplier;

        let backoff = if backoff_secs.is_finite() && backoff_secs >= 0.0 {
            Duration::try_from_secs_f64(backoff_secs)
                .unwrap_or(self.policy.max_backoff)
                .min(self.policy.max_backoff)
        } else {
            self.policy.max_backoff
        };

        Some(backoff)
    }
}

// ============================================================================
// 2. Supervisor Error Definitions
// ============================================================================

/// Error conditions encountered at the service supervisor level.
#[derive(Debug, Error, Clone)]
pub enum ServiceError {
    #[error("Actor '{0}' is already registered with this service supervisor")]
    ActorAlreadyExists(String),
    #[error("Actor '{0}' was not found")]
    ActorNotFound(String),
    #[error("Actor '{0}' channel type mismatch during downcast")]
    ActorTypeMismatch(String),
    #[error("Service supervisor is unavailable or terminated")]
    ServiceUnavailable,
    #[error(
        "Actor '{0}' exceeded maximum restart threshold ({1} restarts within {2:?}) and is permanently failed"
    )]
    MaxRestartsExceeded(String, usize, Duration),
}

/// Error conditions encountered at the root supervisor level.
#[derive(Debug, Error, Clone)]
pub enum RootError {
    #[error("Service '{0}' is already registered with root supervisor")]
    ServiceAlreadyExists(String),
    #[error("Service '{0}' was not found")]
    ServiceNotFound(String),
    #[error("Root supervisor is unavailable or terminated")]
    RootUnavailable,
    #[error(
        "Service '{0}' exceeded maximum restart threshold ({1} restarts within {2:?}) and is permanently failed"
    )]
    MaxRestartsExceeded(String, usize, Duration),
}

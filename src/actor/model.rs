//! # Domain Models and Supervision Tree Protocol Definitions
//!
//! Defines the core data structures, error types, and restart policies
//! utilized across the recursive supervision hierarchy (`Supervisor` -> child `Supervisor`s / actors).

use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

/// Default safety margin before resource expiration when proactive background refresh is triggered (2 seconds).
///
/// Override per actor via [`super::generic_actor::ResourceActorBuilder::expiry_buffer`] for
/// longer-lived credentials (e.g., 60–300 seconds for OAuth2 tokens).
pub const EXPIRY_BUFFER: Duration = Duration::from_secs(2);

/// Default grace period granted to an actor or child supervisor during graceful shutdown (5 seconds).
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Result returned by supervised actor and supervisor tasks upon termination.
/// - `Ok(())` indicates a clean, normal shutdown.
/// - `Err(reason)` signals an abnormal crash or failure escalation that triggers supervision recovery.
pub type TaskResult = Result<(), String>;

// ============================================================================
// 1. Restart Policy, Restart Type, Shutdown Policy, Strategies & Crash Tracking
// ============================================================================

/// Defines when a supervised child (actor or supervisor) should be restarted upon termination
/// (modeled after Erlang/OTP child specification restart rules).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestartType {
    /// The child is restarted only if it terminates abnormally (`Err` or panic).
    /// If it terminates normally (`Ok(())`), it is not restarted.
    #[default]
    Transient,

    /// The child is always restarted, regardless of whether it terminates normally (`Ok(())`)
    /// or abnormally (`Err` / panic), unless explicitly stopped via the supervisor.
    /// Best for critical long-lived daemons, sub-supervisors, and resource actors.
    Permanent,

    /// The child is never restarted (neither on normal exit nor on crash/panic, nor when
    /// siblings restart under `OneForAll` / `RestForOne`). Best for one-shot tasks.
    Temporary,
}

/// Defines how a supervisor terminates a child actor or child supervisor during shutdown or stop operations
/// (modeled after Erlang/OTP shutdown specifications).
#[non_exhaustive]
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

/// Supervision restart strategy governing how sibling children are treated when one child crashes.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SupervisionStrategy {
    /// Only the crashed child is restarted. Sibling children remain completely unaffected.
    /// Best for independent, shared-nothing actors or sub-supervisors.
    #[default]
    OneForOne,

    /// If any child crashes, ALL children supervised by this supervisor are terminated
    /// in reverse registration order and restarted in order of registration.
    /// Best for tightly coupled actors or services sharing dependent state.
    OneForAll,

    /// If a child crashes, that child and all subsequent children registered AFTER it
    /// are terminated in reverse order and restarted in order of registration. Earlier children remain untouched.
    /// Best for linear dependency pipelines (`A -> B -> C`).
    RestForOne,
}

/// Configuration defining restart rate-limiting and exponential backoff behavior for supervised children.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RestartPolicy {
    /// Maximum number of restarts allowed within the time window.
    pub max_restarts: usize,
    /// Time window over which restart frequencies are tracked.
    pub time_window: Duration,
    /// Base delay applied before spawning the replacement child on first restart.
    pub initial_backoff: Duration,
    /// Maximum backoff ceiling across consecutive failures.
    pub max_backoff: Duration,
    /// Factor by which backoff increases after each consecutive crash.
    pub backoff_multiplier: f64,
}

impl RestartPolicy {
    /// Creates a new [`RestartPolicy`] with the given `max_restarts` and `time_window`,
    /// using default backoff settings (`50ms` initial, `2s` max, `2.0x` multiplier).
    #[must_use]
    pub fn new(max_restarts: usize, time_window: Duration) -> Self {
        Self {
            max_restarts,
            time_window,
            ..Self::default()
        }
    }

    /// Sets the maximum number of restarts allowed within `time_window`.
    #[must_use]
    pub fn max_restarts(mut self, max_restarts: usize) -> Self {
        self.max_restarts = max_restarts;
        self
    }

    /// Sets the sliding time window over which restarts are counted.
    #[must_use]
    pub fn time_window(mut self, time_window: Duration) -> Self {
        self.time_window = time_window;
        self
    }

    /// Sets the initial backoff delay before the first restart.
    #[must_use]
    pub fn initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    /// Sets the maximum backoff ceiling across consecutive restarts.
    #[must_use]
    pub fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Sets the exponential backoff multiplier applied on each consecutive restart.
    #[must_use]
    pub fn backoff_multiplier(mut self, backoff_multiplier: f64) -> Self {
        self.backoff_multiplier = backoff_multiplier;
        self
    }
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
    pub(crate) policy: RestartPolicy,
    pub(crate) restarts: Vec<Instant>,
    pub(crate) consecutive_failures: usize,
}

impl RestartTracker {
    /// Creates a new [`RestartTracker`] governed by the given [`RestartPolicy`].
    pub fn new(policy: RestartPolicy) -> Self {
        Self {
            policy,
            restarts: Vec::new(),
            consecutive_failures: 0,
        }
    }

    /// Returns the configured [`RestartPolicy`].
    pub fn policy(&self) -> &RestartPolicy {
        &self.policy
    }

    /// Returns the number of consecutive failures within the active window.
    pub fn consecutive_failures(&self) -> usize {
        self.consecutive_failures
    }

    /// Records a crash.
    /// Returns `Some(backoff)` if a restart is allowed within the configured rate limit,
    /// or `None` if the crash threshold has been exceeded.
    pub fn record_crash(&mut self) -> Option<Duration> {
        let now = Instant::now();
        self.restarts
            .retain(|&t| now.saturating_duration_since(t) <= self.policy.time_window);

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

    /// Offsets recorded crash timestamps by a scheduled supervisor backoff sleep duration so that
    /// `time_window` tracks active runtime rather than expiring while waiting in backoff.
    pub(crate) fn offset_for_backoff(&mut self, backoff: Duration) {
        if !backoff.is_zero() {
            for t in &mut self.restarts {
                *t += backoff;
            }
        }
    }
}

// ============================================================================
// 2. Unified Supervisor Error Definition
// ============================================================================

/// Error conditions encountered at any level of the [`super::supervisor::Supervisor`] hierarchy.
#[non_exhaustive]
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SupervisorError {
    #[error("Child '{0}' is already registered and running under this supervisor")]
    ChildAlreadyExists(String),
    #[error("Child '{0}' was not found under this supervisor")]
    ChildNotFound(String),
    #[error("Child '{0}' channel type mismatch during downcast")]
    ChildTypeMismatch(String),
    #[error("Supervisor is unavailable or has terminated")]
    SupervisorUnavailable,
    #[error(
        "Child '{0}' exceeded maximum restart threshold ({1} restarts within {2:?}) and is permanently failed"
    )]
    MaxRestartsExceeded(String, usize, Duration),
    #[error("Child '{0}' factory panicked during spawn: {1}")]
    FactoryPanicked(String, String),
}

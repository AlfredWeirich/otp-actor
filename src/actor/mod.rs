//! # Hierarchical OTP Supervision Tree & Generic Resource Actor System
//!
//! Provides a resilient, multi-level actor framework modeled after Erlang/OTP principles:
//! - **Control Plane**: [`RootSupervisor`] and [`ServiceSupervisor`] managing lifecycle,
//!   supervision strategies ([`SupervisionStrategy::OneForOne`], [`SupervisionStrategy::OneForAll`],
//!   [`SupervisionStrategy::RestForOne`]), rate-limited backoff, and hierarchical subtree recovery.
//! - **Data Plane**: [`ResourceClient`] communicating directly with [`GenericResourceActor`]
//!   (1-hop channel) with zero supervisor overhead, transparent channel rebinding across
//!   actor and supervisor restarts, proactive refreshes, and zero-cost worker execution
//!   via the [`Worker`] trait.
//!
//! ## Example
//! ```rust
//! use std::time::Duration;
//! use otp_actor::actor::{
//!     GenericResourceActor, RootSupervisor, ServiceSupervisor, TimedResource, WorkerContext,
//! };
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let root = RootSupervisor::new();
//! let service = ServiceSupervisor::spawn_and_register("auth-service", &root)
//!     .await
//!     .unwrap();
//!
//! let client = GenericResourceActor::builder("api-token")
//!     .worker(|ctx: WorkerContext| async move {
//!         Ok::<_, String>(TimedResource::with_ttl(
//!             format!("token-gen-{}", ctx.generation),
//!             Duration::from_secs(60),
//!         ))
//!     })
//!     .spawn(&service)
//!     .await
//!     .unwrap();
//!
//! assert_eq!(client.get().await.unwrap(), "token-gen-1");
//! root.shutdown().await.unwrap();
//! # }
//! ```

pub mod generic_actor;
pub mod model;
pub mod root_supervisor;
pub mod service_supervisor;
pub mod stateful_actor;

// Re-export primary types for a convenient public API
pub use generic_actor::{
    ActorBuilder, ActorConfig, DEFAULT_WORKER_TIMEOUT, GenericResourceActor,
    GenericResourceActorBuilder, ResourceClient, ResourceError, TimedResource, Worker,
    WorkerContext,
};
pub use model::{
    DEFAULT_SHUTDOWN_TIMEOUT, EXPIRY_BUFFER, RestartPolicy, RestartTracker, RestartType, RootError,
    ServiceError, ShutdownPolicy, SupervisionStrategy, TaskResult,
};
pub use root_supervisor::{RootSupervisor, RootSupervisorHandle};
pub use service_supervisor::{
    ChildSpec, ServiceSupervisor, ServiceSupervisorBuilder, ServiceSupervisorHandle,
    ShutdownSignalFn, UntypedActorFactory, UntypedSender,
};
pub use stateful_actor::{
    ActorClient, ActorContext, ActorControl, ActorError, StatefulActor, StatefulActorBuilder,
    StatefulActorConfig,
};

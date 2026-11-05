//! # Hierarchical OTP Supervision Tree & Actor System
//!
//! Provides a resilient, multi-level actor framework modeled after Erlang/OTP principles:
//! - **Control Plane**: [`RootSupervisor`] and [`ServiceSupervisor`] managing lifecycle,
//!   supervision strategies ([`SupervisionStrategy::OneForOne`], [`SupervisionStrategy::OneForAll`],
//!   [`SupervisionStrategy::RestForOne`]), restart rules ([`RestartType`]), bounded shutdown policies
//!   ([`ShutdownPolicy`]), rate-limited exponential backoff ([`RestartPolicy`], [`RestartTracker`]),
//!   and hierarchical subtree recovery.
//! - **Data Plane**: [`ResourceClient`] and [`ActorClient`] communicating directly with
//!   [`GenericResourceActor`] and [`StatefulActor`] over 1-hop bounded channels with zero
//!   supervisor overhead in the hot path, transparent channel rebinding across actor and supervisor
//!   restarts, proactive cache refreshes, and zero-cost worker execution via native `impl Future`.
//!
//! ## Example: Generic Resource Actor
//! ```rust
//! use std::time::Duration;
//! use otp_actor::actor::{
//!     GenericResourceActor, RootSupervisor, ServiceSupervisor, TimedResource, WorkerContext,
//! };
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let root = RootSupervisor::new();
//! let service = ServiceSupervisor::spawn_and_register("auth-service", &root).await?;
//!
//! let client = GenericResourceActor::builder("api-token")
//!     .worker(|ctx: WorkerContext| async move {
//!         Ok::<_, String>(TimedResource::with_ttl(
//!             format!("token-gen-{}", ctx.generation),
//!             Duration::from_secs(60),
//!         ))
//!     })
//!     .spawn(&service)
//!     .await?;
//!
//! assert_eq!(client.get().await?, "token-gen-1");
//! root.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Example: Stateful Actor (`gen_server` style)
//! ```rust
//! use otp_actor::actor::{
//!     ActorContext, RestartType, RootSupervisor, ServiceSupervisor,
//!     StatefulActor, StatefulActorBuilder,
//! };
//!
//! #[derive(Clone, Default)]
//! struct RegistryActor {
//!     items: Vec<String>,
//! }
//!
//! enum RegistryMsg {
//!     Insert(String),
//!     Count,
//! }
//!
//! impl StatefulActor for RegistryActor {
//!     type Message = RegistryMsg;
//!     type Response = usize;
//!     type Error = String;
//!
//!     async fn handle(
//!         &mut self,
//!         msg: Self::Message,
//!         _ctx: &mut ActorContext,
//!     ) -> Result<Self::Response, Self::Error> {
//!         match msg {
//!             RegistryMsg::Insert(item) => {
//!                 self.items.push(item);
//!                 Ok(self.items.len())
//!             }
//!             RegistryMsg::Count => Ok(self.items.len()),
//!         }
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let root = RootSupervisor::new();
//! let service = ServiceSupervisor::spawn_and_register("registry-service", &root).await?;
//!
//! let client = StatefulActorBuilder::from_default::<RegistryActor>("registry")
//!     .restart_type(RestartType::Permanent)
//!     .spawn(&service)
//!     .await?;
//!
//! client.cast(RegistryMsg::Insert("node-1".into())).await?;
//! let count = client.call(RegistryMsg::Count).await?;
//! assert_eq!(count, 1);
//!
//! root.shutdown().await?;
//! # Ok(())
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

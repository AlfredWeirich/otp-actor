//! # otp-actor
//!
//! A high-performance, fault-tolerant concurrent actor framework for [Tokio](https://tokio.rs),
//! modeled after Erlang/OTP principles.
//!
//! ## Architectural Pillars
//!
//! `otp-actor` combines three core capabilities:
//!
//! 1. **Hierarchical Supervision Tree** ([`RootSupervisor`] &rarr; [`ServiceSupervisor`] &rarr; child actors):
//!    - Detects task exits and unhandled panics, isolating failures within subsystems.
//!    - Applies configurable supervision strategies:
//!      - [`SupervisionStrategy::OneForOne`]: Only the failed child is restarted.
//!      - [`SupervisionStrategy::OneForAll`]: All supervised siblings are gracefully terminated in reverse registration order and restarted.
//!      - [`SupervisionStrategy::RestForOne`]: The failed child and all siblings registered after it are terminated and restarted in order.
//!    - Honors OTP-grade child restart specifications ([`RestartType::Transient`], [`RestartType::Permanent`], [`RestartType::Temporary`]).
//!    - Enforces bounded graceful shutdown policies ([`ShutdownPolicy::Timeout`], [`ShutdownPolicy::BrutalKill`], [`ShutdownPolicy::Infinity`]).
//!    - Defends against crash loops using sliding-window rate limiting and exponential backoff ([`RestartPolicy`], [`RestartTracker`]).
//!
//! 2. **Generic Resource Actor** ([`GenericResourceActor`]):
//!    - Proactively fetches, caches, and refreshes expiring resources (OAuth2 tokens, dynamic secrets, database credentials, configuration snapshots).
//!    - Zero-cost worker execution via the [`Worker`] trait: accepts custom structs, standalone `async fn`s, or closures returning native `impl Future` without `Box::pin` heap allocations.
//!    - Single-flight request coalescing (multiple concurrent callers await a single backend fetch).
//!    - Proactive background timer refresh before hard TTL expiration with entropy-hashed jitter.
//!    - Negative caching / failure cooldown to prevent retry storms against struggling backends.
//!    - Stale-while-error resilience to serve cached data during transient upstream outages.
//!
//! 3. **General-Purpose Stateful Actor** ([`StatefulActor`]):
//!    - Modeled after Erlang/OTP `gen_server` for building robust state machines.
//!    - Native `impl Future` methods (`handle`, `on_start`, `on_stop`) with zero heap allocations per message.
//!    - Direct 1-hop data plane with typed request/response ([`ActorClient::call`]) and asynchronous notification ([`ActorClient::cast`]).
//!    - Controlled failure escalation via [`ActorContext::fail`], triggering supervisor recovery while returning an immediate error to the caller.
//!    - Supervised alongside [`GenericResourceActor`] under the same supervision tree.
//!
//! ## Control Plane vs. Data Plane
//!
//! A core design principle is the strict decoupling of the control plane and the data plane:
//!
//! - **Data Plane (Direct 1-Hop)**: Clients ([`ResourceClient`], [`ActorClient`]) hold direct bounded `mpsc`
//!   channels to the actor mailboxes. Requests never traverse supervisors, eliminating bottlenecks.
//! - **Control Plane (Supervisors)**: Supervisors own task handles, monitor exits/panics, manage lifecycle transitions,
//!   and maintain active channel registries.
//! - **Transparent Self-Healing**: If an actor crashes, its direct channel closes. The client intercepts the
//!   disconnection, queries the supervisor for the replacement channel, and retries transparently.
//!
//! ## Quick Start: Generic Resource Actor
//!
//! ```rust
//! use std::time::Duration;
//! use otp_actor::{
//!     GenericResourceActor, RestartType, RootSupervisor, ServiceSupervisor,
//!     TimedResource, WorkerContext,
//! };
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // 1. Start the top-level root supervisor
//! let root = RootSupervisor::new();
//!
//! // 2. Start a domain service supervisor (autonomously self-registers with root)
//! let service = ServiceSupervisor::spawn_and_register("auth-service", &root).await?;
//!
//! // 3. Define a worker closure to acquire an expiring resource
//! let token_worker = |ctx: WorkerContext| async move {
//!     Ok::<_, String>(TimedResource::with_ttl(
//!         format!("token-gen-{}", ctx.generation),
//!         Duration::from_secs(60),
//!     ))
//! };
//!
//! // 4. Build, spawn, and obtain a direct client handle
//! let client = GenericResourceActor::builder("api-token")
//!     .worker(token_worker)
//!     .eager_prewarm(true)
//!     .expiry_buffer(Duration::from_secs(5))
//!     .spawn(&service)
//!     .await?;
//!
//! // 5. Retrieve resource via direct 1-hop channel (served from cache)
//! let token = client.get().await?;
//! assert_eq!(token, "token-gen-1");
//!
//! // 6. Graceful cascading shutdown
//! root.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Quick Start: Stateful Actor
//!
//! ```rust
//! use otp_actor::{
//!     ActorContext, RestartType, RootSupervisor, ServiceSupervisor,
//!     StatefulActor, StatefulActorBuilder,
//! };
//!
//! #[derive(Clone, Default)]
//! struct CounterActor {
//!     count: i64,
//! }
//!
//! enum CounterMsg {
//!     Add(i64),
//!     Get,
//! }
//!
//! impl StatefulActor for CounterActor {
//!     type Message = CounterMsg;
//!     type Response = i64;
//!     type Error = String;
//!
//!     async fn handle(
//!         &mut self,
//!         msg: Self::Message,
//!         _ctx: &mut ActorContext,
//!     ) -> Result<Self::Response, Self::Error> {
//!         match msg {
//!             CounterMsg::Add(n) => {
//!                 self.count += n;
//!                 Ok(self.count)
//!             }
//!             CounterMsg::Get => Ok(self.count),
//!         }
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let root = RootSupervisor::new();
//! let service = ServiceSupervisor::spawn_and_register("counter-service", &root).await?;
//!
//! let client = StatefulActorBuilder::from_default::<CounterActor>("counter")
//!     .restart_type(RestartType::Permanent)
//!     .spawn(&service)
//!     .await?;
//!
//! client.cast(CounterMsg::Add(10)).await?;
//! let total = client.call(CounterMsg::Get).await?;
//! assert_eq!(total, 10);
//!
//! root.shutdown().await?;
//! # Ok(())
//! # }
//! ```

pub mod actor;
pub use actor::*;

//! # otp-actor
//!
//! A high-performance, fault-tolerant concurrent actor framework for [Tokio](https://tokio.rs),
//! modeled after Erlang/OTP principles.
//!
//! ## Architectural Pillars
//!
//! `otp-actor` combines four core capabilities:
//!
//! 1. **Unified Recursive Supervision Tree** ([`Supervisor`] &rarr; child [`Supervisor`]s / leaf actors):
//!    - Arbitrarily nestable `Supervisor` hierarchy ($N$ levels deep).
//!    - Detects task exits and unhandled panics, isolating failures within subsystems.
//!    - Applies configurable supervision strategies at every level:
//!      - [`SupervisionStrategy::OneForOne`]: Only the failed child is restarted.
//!      - [`SupervisionStrategy::OneForAll`]: All supervised siblings are gracefully terminated in reverse registration order and restarted.
//!      - [`SupervisionStrategy::RestForOne`]: The failed child and all siblings registered after it are terminated and restarted in order.
//!    - Honors OTP-grade child restart specifications ([`RestartType::Transient`], [`RestartType::Permanent`], [`RestartType::Temporary`]).
//!    - Enforces bounded graceful shutdown policies ([`ShutdownPolicy::Timeout`], [`ShutdownPolicy::BrutalKill`], [`ShutdownPolicy::Infinity`]).
//!    - Defends against crash loops using sliding-window rate limiting and exponential backoff ([`RestartPolicy`], [`RestartTracker`]).
//!
//! 2. **Resource Actor** ([`ResourceActor`]):
//!    - Proactively fetches, caches, and refreshes expiring resources (OAuth2 tokens, dynamic secrets, database credentials, configuration snapshots).
//!    - Zero-hop cache reads via [`tokio::sync::watch`] in [`ResourceClient::get`].
//!    - Zero-cost worker execution via the [`Worker`] trait: accepts custom structs, standalone `async fn`s, or closures returning native `impl Future` without `Box::pin` heap allocations.
//!    - Single-flight request coalescing (multiple concurrent callers await a single backend fetch).
//!    - Proactive background timer refresh before hard TTL expiration with entropy-hashed jitter.
//!    - Negative caching / failure cooldown to prevent retry storms against struggling backends.
//!    - Stale-while-error resilience to serve cached data during transient upstream outages.
//!
//! 3. **General-Purpose Stateful Actor** ([`StatefulActor`]):
//!    - Modeled after Erlang/OTP `gen_server` for building robust state machines.
//!    - Native `impl Future` methods (`handle`, `handle_cast`, `on_start`, `on_stop`) with zero heap allocations per message.
//!    - Direct 1-hop data plane with typed request/response ([`ActorClient::call`], [`ActorClient::ask`] via [`CallMessage`]) and asynchronous notification ([`ActorClient::cast`], [`ActorClient::try_cast`], [`ActorClient::cast_timeout`]).
//!    - Rich execution context ([`ActorContext`]) with `client()`, `supervisor()`, `send_after()`, and controlled failure escalation via [`ActorContext::fail`].
//!    - Supervised alongside [`ResourceActor`] and child [`Supervisor`]s under the same supervision tree.
//!
//! 4. **Remote Actors over gRPC** (`--features remote`, see `otp_actor::remote`):
//!    - Extends [`StatefulActor`] messaging across processes and hosts over HTTP/2 gRPC (`proto/actor_node.proto`) with bounded `postcard` payloads (&le; 4 MiB).
//!    - Location transparency via `RemoteActorClient` and `AnyActorClient` (`Local` vs. `Remote`) with host-side re-binding via `ActorClient` / `ByteEndpoint`.
//!    - Coordinated 2-phase graceful shutdown via `ActorNodeHandle::shutdown` (HTTP/2 `GOAWAY` request drainage followed by [`SupervisorHandle::shutdown`]).
//!
//! ## Control Plane vs. Data Plane
//!
//! A core design principle is the strict decoupling of the control plane and the data plane:
//!
//! - **Data Plane (0-Hop Cache / Direct 1-Hop / gRPC Bridge)**: In-process clients ([`ResourceClient`], [`ActorClient`]) hold direct `watch` cache receivers and bounded `mpsc`
//!   channels to the actor mailboxes, while remote clients traverse gRPC into a registered `ActorClient` (`ByteEndpoint`). Requests never traverse supervisors, eliminating bottlenecks.
//! - **Control Plane (Supervisors)**: [`Supervisor`]s own task handles, monitor exits/panics, manage lifecycle transitions,
//!   and publish reactive [`SlotState`] updates over per-child `watch` channels.
//! - **Zero-Polling Self-Healing**: If an actor crashes, its direct channel closes. The client awaits the child's
//!   reactive [`SlotState`] `watch` channel and rebinds in 0ms as soon as the replacement actor spawns.
//!
//! ## Quick Start: Resource Actor
//!
//! ```rust
//! use std::time::Duration;
//! use otp_actor::{
//!     ResourceActor, Supervisor, TimedResource, WorkerContext,
//! };
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // 1. Start the top-level root supervisor
//! let root = Supervisor::new_root();
//!
//! // 2. Start a domain child supervisor under root
//! let service = Supervisor::spawn_child("auth-service", &root).await?;
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
//! let client = ResourceActor::builder("api-token")
//!     .worker(token_worker)
//!     .eager_prewarm(true)
//!     .expiry_buffer(Duration::from_secs(5))
//!     .spawn(&service)
//!     .await?;
//!
//! // 5. Retrieve resource (served in 0 hops from watch cache when fresh)
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
//!     ActorContext, RestartType, StatefulActor, StatefulActorBuilder, Supervisor,
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
//!         _ctx: &mut ActorContext<Self>,
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
//! let root = Supervisor::new_root();
//! let service = Supervisor::spawn_child("counter-service", &root).await?;
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
//!
//! ## Feature Flags
//!
//! All optional features are disabled by default (`default = []`), keeping the core framework
//! free of networking or test-only dependencies:
//!
//! - **`remote`**: Enables network-transparent remote actors over HTTP/2 gRPC via the
//!   `otp_actor::remote` module (`RemoteActorClient`, `AnyActorClient`, `ActorNodeBuilder`,
//!   `ActorNodeHandle`, `ByteEndpoint`, and `ActorNodeRegistry`). Pulls in `tonic`,
//!   `tonic-prost`, `prost`, `serde`, `postcard`, `tokio-stream`, `tonic-prost-build`, and
//!   `tokio/net`.
//! - **`chaos_test`**: Exposes intentional crash-injection helpers (`SupervisorHandle::trigger_child_crash`,
//!   `SupervisorHandle::simulate_crash`, `ActorClient::simulate_crash`, and
//!   `ResourceClient::simulate_crash`) for testing supervision recovery and crash-loop policies.
//!   Enabled automatically in `[dev-dependencies]` for integration tests and examples.

pub mod actor;
pub use actor::*;

#[cfg(feature = "remote")]
pub mod remote;

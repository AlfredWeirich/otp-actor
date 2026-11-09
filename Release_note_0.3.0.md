# Release Notes — `otp-actor` v0.3.0

**Release Date:** 2026-11-09  
**Minimum Supported Rust Version (MSRV):** Rust 1.88 (`edition = "2024"`)

`otp-actor` **0.3.0** is a major architectural release that unifies the supervision tree into a single recursive `Supervisor`, replaces `mpsc` channel polling with a reactive `tokio::sync::watch` data plane (including 0-hop cache reads for `ResourceActor`), expands `StatefulActor` ergonomics with `handle_cast`, `CallMessage::ask()`, `send_after()`, and non-blocking backpressure (`try_cast()`), and gates chaos-testing hooks behind a dedicated `chaos_test` Cargo feature.

---

## Highlights

- **Unified Recursive `Supervisor` (`N`-level trees):** Replaced the fixed two-tier `RootSupervisor` + `ServiceSupervisor` split with a single composable `Supervisor` that can manage leaf actors and child `Supervisor`s to arbitrary nesting depth, supporting `OneForOne`, `OneForAll`, and `RestForOne` at every level.
- **Zero-Polling Reactive Rebinding (`watch<SlotState>`):** Eliminated resolution polling and sleep loops. Every supervised child slot publishes lifecycle transitions via `tokio::sync::watch::Receiver<SlotState>` (`Running`, `Restarting`, `Stopped`, `Failed`), allowing `ResourceClient`, `ActorClient`, and nested `SupervisorHandle`s to rebind in 0 ms upon replacement spawn.
- **0-Hop Resource Cache Reads (`ResourceActor` / `ResourceClient::get()`):** Fresh resources are published to a `tokio::sync::watch` cache snapshot and served lock-free in 0 hops directly on the caller task without allocating a `oneshot` channel or traversing the actor mailbox.
- **Richer `StatefulActor` Ergonomics & Backpressure:** Added `StatefulActor::handle_cast`, typed sub-message responses via `CallMessage<A>` + `ActorClient::ask()`, `ActorContext<Self>` (`client()`, `supervisor()`, `send_after()`), non-blocking `ActorClient::try_cast()` (`ActorError::MailboxFull`), `cast_timeout()`, and `call_retry_timeout()`.
- **`chaos_test` Feature Gate:** Moved `simulate_crash()` and `trigger_child_crash()` behind `#[cfg(any(test, feature = "chaos_test"))]` so production binaries remain clean by default while tests, doc-tests, and examples work seamlessly via `[dev-dependencies]`.
- **Enhanced Remote gRPC Contract (`--features remote`):** Extended `actor_node.proto` with `CastRequest.timeout_ms` and `RemoteError::Kind::MAX_RESTARTS_EXCEEDED`, added `cast_timeout()` to `RemoteActorClient` and `AnyActorClient`, introduced `RemoteNodeError`, and unified server teardown around `SupervisorHandle`.

---

## What's New

### 1. Unified Recursive Supervision (`src/actor/supervisor.rs`, `src/actor/model.rs`)
- **Single Supervisor Type:** Use `Supervisor::new(name)`, `Supervisor::new_root()`, `Supervisor::with_strategy(name, strategy)`, `Supervisor::builder(name)`, `Supervisor::spawn_child(name, &parent)`, and `Supervisor::spawn_child_with_strategy(name, strategy, &parent)`.
- **Arbitrary Subtree Nesting:** `SupervisorBuilder::add_child(...)` accepts any `impl Into<ChildSpec>`, including `ResourceActorBuilder`, `StatefulActorBuilder`, or another nested `SupervisorBuilder`.
- **Default Child Supervisor Shutdown:** `SupervisorBuilder` defaults to `ShutdownPolicy::Infinity` for nested supervisors so parent supervisors wait for sequential reverse-order teardown of child subtrees without prematurely aborting in-flight child shutdowns.
- **Typed Child Lookup on `SupervisorHandle`:**
  - `get_resource_client::<T, E>(name) -> Result<ResourceClient<T, E>, SupervisorError>`
  - `get_actor_client::<A>(name) -> Result<ActorClient<A>, SupervisorError>`
  - `get_supervisor(name) -> Result<SupervisorHandle, SupervisorError>`
- **Hardened Child Watch State Updates:** `Supervisor` uses `watch::Sender::send_replace(...)` on every slot state transition (`Running`, `Restarting`, `Stopped`, `Failed`) and `ResourceActor` uses `send_replace(...)` on its cache channel so state updates are never dropped even when `receiver_count() == 0`.
- **Future-Proof Policies & Trackers:**
  - Marked `SupervisionStrategy`, `RestartType`, `ShutdownPolicy`, `RestartPolicy`, `SupervisorError`, `ResourceActorConfig`, `StatefulActorConfig`, `ResourceError`, `ActorError`, `RemoteActorError`, and `RemoteNodeError` as `#[non_exhaustive]`.
  - Added `RestartPolicy::new(max_restarts, time_window)` and fluent setters (`max_restarts`, `time_window`, `initial_backoff`, `max_backoff`, `backoff_multiplier`).
  - Encapsulated `RestartTracker` fields behind `policy()` and `consecutive_failures()`.

### 2. Zero-Hop Cache & Unified Builder for `ResourceActor` (`src/actor/generic_actor.rs`)
- Renamed `GenericResourceActor` to `ResourceActor`, `ActorConfig` to `ResourceActorConfig`, and merged `ActorBuilder` + `GenericResourceActorBuilder` into a single `ResourceActorBuilder<T, E, W>`.
- Builder options (`expiry_buffer`, `worker_timeout`, `eager_prewarm`, etc.) can be chained in any order before or after `.worker(...)`.
- `ResourceClient::get()` checks the 0-hop `watch::Receiver<Option<ResourceSnapshot<T>>>` fast path first (checking invalidation, open sender channel, expiration, and pre-refresh threshold), falling back to the 1-hop mailbox coalesced path only for cold starts, refreshes, or expired/stale recovery.

### 3. `StatefulActor` Ergonomics, Timers & Backpressure (`src/actor/stateful_actor.rs`)
- **`handle_cast` Hook:** Optional method on `StatefulActor` for lightweight fire-and-forget handling (defaults to calling `handle` and discarding the response).
- **Typed `ask` Sub-Messages:** Implement `CallMessage<A>` to unpack per-command reply types directly via `client.ask(msg).await?`.
- **Richer `ActorContext<Self>`:**
  - `ctx.client()` — creates a self-healing `ActorClient<Self>` handle to the current actor.
  - `ctx.supervisor()` — accesses the parent `&SupervisorHandle` for dynamic child spawning.
  - `ctx.send_after(delay, msg)` — schedules a delayed self-`cast`, returning a cancellable `tokio::task::AbortHandle`.
- **Backpressure & Timeouts:**
  - `client.try_cast(msg)` — non-blocking send returning `Err(ActorError::MailboxFull)` when capacity is reached.
  - `client.cast_timeout(msg, dur)` and `client.call_retry_timeout(msg, dur)`.
- **Watch Rebinding:** `ActorClient` holds a reactive `watch::Receiver<SlotState>` (and re-resolves from `SupervisorHandle` if the managing supervisor itself restarts).

### 4. Remote gRPC Improvements (`--features remote`, `proto/actor_node.proto`)
- Added `optional uint64 timeout_ms = 4` to `CastRequest`, propagated end-to-end by `RemoteActorClient::cast_timeout` and `AnyActorClient::cast_timeout` into `ByteEndpoint::cast_bytes`.
- Added `MAX_RESTARTS_EXCEEDED = 9` to `RemoteError::Kind` and `RemoteActorError::MaxRestartsExceeded(String)` for symmetric reporting when a supervised remote actor exhausts its restart budget.
- Added `RemoteNodeError` for structured error reporting from `ActorNodeHandle::start` and `ActorNodeHandle::shutdown`.
- `ActorNodeHandle::start` now accepts `Option<SupervisorHandle>` to orchestrate Phase-2 cascading subtree teardown after HTTP/2 request drainage.

### 5. Feature-Gated Chaos Engineering (`chaos_test`)
- `ResourceClient::simulate_crash()`, `ActorClient::simulate_crash()`, `SupervisorHandle::simulate_crash()`, and `SupervisorHandle::trigger_child_crash()` are gated behind `#[cfg(any(test, feature = "chaos_test"))]`.
- Enabled automatically for workspace tests, doc-tests, and examples via `[dev-dependencies] otp-actor = { path = ".", features = ["chaos_test"] }`.

---

## Breaking Changes & Migration Guide (from `0.2.x` to `0.3.0`)

### Summary of Renamed / Removed Symbols

| `0.2.x` | `0.3.0` |
| --- | --- |
| `RootSupervisor` / `RootSupervisorHandle` | `Supervisor` (`Supervisor::new_root()` / `Supervisor::new(...)`) / `SupervisorHandle` |
| `ServiceSupervisor` / `ServiceSupervisorBuilder` / `ServiceSupervisorHandle` | `Supervisor` / `SupervisorBuilder` / `SupervisorHandle` |
| `ServiceSupervisor::spawn_and_register(name, &root)` | `Supervisor::spawn_child(name, &root)` or `Supervisor::builder(name).spawn(&root)` |
| `RootError` / `ServiceError` | `SupervisorError` (`ChildAlreadyExists`, `ChildNotFound`, `ChildTypeMismatch`, `SupervisorUnavailable`, `MaxRestartsExceeded`) |
| `ResourceError::Service(ServiceError)` | `ResourceError::Supervisor(SupervisorError)` |
| `ActorError::Service(ServiceError)` | `ActorError::Supervisor(SupervisorError)` |
| `service.get_client::<T, E>(name)` | `supervisor.get_resource_client::<T, E>(name)` |
| `root.resolve_service_handle(name)` | `supervisor.get_supervisor(name)` |
| `service.resolve_actor_channel*` / `root.resolve_service_channel` | `supervisor.resolve_child_channel::<S>(name)` / `resolve_child_channel_untyped(name)` |
| `GenericResourceActor` / `ActorConfig` / `ActorBuilder` / `GenericResourceActorBuilder` | `ResourceActor` / `ResourceActorConfig` / `ResourceActorBuilder` |
| `ResourceActor::builder(name).spawn_and_register(&svc, &root)` | `ResourceActor::builder(name).spawn(&supervisor)` |
| `ActorContext` (non-generic) | `ActorContext<Self>` |
| `root.stop_service(name)` / `root.restart_service(name)` / `root.delete_service(name)` | `parent.stop_child(name)` / `parent.restart_child(name)` / `parent.delete_child(name)` |
| `trigger_service_crash()` | `supervisor.simulate_crash()` / `parent.trigger_child_crash(name)` *(requires `chaos_test`)* |
| `ActorNodeHandle::start(addr, reg, Option<RootSupervisorHandle>)` | `ActorNodeHandle::start(addr, reg, Option<SupervisorHandle>)` |

### Before / After Migration Example

**Before (`0.2.x`):**
```rust
use otp_actor::actor::{
    ActorContext, GenericResourceActor, RootSupervisor, ServiceSupervisor,
    StatefulActor, TimedResource, WorkerContext,
};

let root = RootSupervisor::new();
let service = ServiceSupervisor::spawn_and_register("auth-service", &root).await?;

let tokens = GenericResourceActor::builder("api-token")
    .worker(token_worker)
    .spawn_and_register(&service, &root)
    .await?;

// Inside StatefulActor:
async fn handle(&mut self, msg: Msg, ctx: &mut ActorContext) -> Result<Resp, Err> { ... }
```

**After (`0.3.0`):**
```rust
use otp_actor::actor::{
    ActorContext, ResourceActor, StatefulActor, Supervisor, TimedResource, WorkerContext,
};

let root = Supervisor::new_root();
let service = Supervisor::spawn_child("auth-service", &root).await?;

let tokens = ResourceActor::builder("api-token")
    .worker(token_worker)
    .spawn(&service)
    .await?;

// Inside StatefulActor:
async fn handle(&mut self, msg: Msg, ctx: &mut ActorContext<Self>) -> Result<Resp, Err> { ... }
```

### Constructing `#[non_exhaustive]` Types
If external code constructed `RestartPolicy { ... }` using struct literal syntax, switch to the constructor and builder methods:
```rust
let policy = RestartPolicy::new(3, Duration::from_secs(10))
    .initial_backoff(Duration::from_millis(25))
    .max_backoff(Duration::from_secs(1));
```
Also include a wildcard `_ => ...` arm when matching on `ResourceError`, `ActorError`, `SupervisorError`, `RestartType`, `ShutdownPolicy`, `SupervisionStrategy`, `RemoteActorError`, or `RemoteNodeError`.

### Using Chaos Helpers Downstream
If downstream integration tests outside this crate invoke `simulate_crash()` or `trigger_child_crash()`, enable the `chaos_test` feature under `[dev-dependencies]`:
```toml
[dev-dependencies]
otp-actor = { version = "0.3.0", features = ["chaos_test"] }
```

---

## Verification

All checks were run and passed cleanly for `0.3.0`:
- `cargo test --test actor_supervision_test --all-features`: **67 / 67 passed**
- `cargo test --test remote_grpc_test --features remote`: **7 / 7 passed**
- `cargo test --doc --all-features`: **4 / 4 passed**
- `cargo clippy --all-targets --all-features -- -D warnings`: **0 warnings**
- `cargo clippy --lib --no-default-features -- -D warnings`: **0 warnings**
- `cargo doc --no-deps --all-features`: **0 warnings**
- All examples compile (`cargo check --examples --all-features`) and non-interactive demos (`simple`, `dynamic_child`, `dynamic_supervisor`, `remote_all_in_one`) were executed and passed.

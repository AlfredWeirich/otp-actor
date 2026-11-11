# Release Notes — `otp-actor` v0.3.1

**Release Date:** 2026-11-11  
**Minimum Supported Rust Version (MSRV):** Rust 1.88 (`edition = "2024"`)

`otp-actor` **0.3.1** is a non-breaking feature, observability, and documentation release on top of `0.3.0`. It adds lifecycle-bound periodic timers, instance-pinned scoped background tasks and stream forwarding for `StatefulActor`, a bounded runtime introspection, recursive tree snapshot, and non-intrusive health observation API across `SupervisorHandle`, `ActorClient`, and `ResourceClient`, self-contained quickstart/pipeline examples, updated architectural documentation, and full `--all-features` docs.rs support.

---

## Highlights

- **Lifecycle-Bound Periodic Timers (`send_interval`, `send_interval_with`):** Schedule periodic ticks from `ActorContext<Self>` with `MissedTickBehavior::Skip`, bounded mailbox permit reservation before payload construction, and automatic cancellation when the spawning actor instance exits, stops, or panics (now also protecting `send_after`).
- **Instance-Pinned Scoped Background Tasks & Stream Forwarding (`spawn_scoped`, `ActorScope`, `forward_receiver`):** Attach external async loops (streams, sockets, background workers) directly to a single actor instance so they exit automatically on shutdown or panic and never leak events onto a restarted replacement instance.
- **Bounded Runtime Introspection, Recursive Tree Snapshots & Cache Health (`snapshot`, `snapshot_tree`, `watch_child_status`, `queue_stats`, `cache_state`):** Query ordered per-level snapshots (`SupervisorSnapshot`, `ChildSnapshot`, `ChildStatus`, `RestartTracker::restarts_in_window`), collect whole nested supervision trees under one shared deadline with per-subtree failure isolation (`SupervisorTreeSnapshot`, `SubtreeSnapshot`, `ChildSnapshot::is_supervisor`), subscribe to read-only child transitions (`ChildStatusWatcher`) even while `Stopped` or `Failed`, inspect approximate client mailbox utilization (`QueueStats`), and classify 0-hop cache health without mailbox hops or exposing cached values (`ResourceClient::cache_state` → `CacheState`).
- **New Runnable Examples, Architectural Guide Updates & docs.rs Coverage:** Added `minimum`, `quickstart`, and `rest_for_one_pipeline` examples, extended `microservice_pipeline` (including `cache_state()` and `snapshot_tree()`), added Sections `3.7` and `4.6` to [`docs/actor_model_guide.md`](docs/actor_model_guide.md), updated [`README.md`](README.md), and configured `[package.metadata.docs.rs] all-features = true` in [`Cargo.toml`](Cargo.toml).

---

## What's New

### 1. Lifecycle-Bound Timers & Cancellation Guards (`src/actor/stateful_actor.rs`)
- **`ActorContext::send_interval(period, msg)`** & **`ActorContext::send_interval_with(period, make_msg)`:**
  - Fires the first tick after one full `period` and delivers messages via bounded `cast`.
  - Uses `tokio::time::MissedTickBehavior::Skip` under load so slow handlers never build up a burst backlog.
  - `send_interval_with` works with non-`Clone` message types and invokes `make_msg()` only *after* acquiring a mailbox permit.
  - Guards `Duration::ZERO` and overflow-prone durations cleanly by logging a warning and returning an already-completed `tokio::task::AbortHandle` without panicking.
- **Automatic Instance Cancellation (`send_after`, `send_interval`, `send_interval_with`):**
  - `StatefulActorRunner` now manages an internal per-instance `watch::Sender<bool>` drop/shutdown guard (`InstanceCancelGuard`), cancelling all instance timers immediately when the actor stops gracefully (right before `on_stop`), fails, or panics.

### 2. Instance-Pinned Scoped Tasks & Stream Forwarding (`src/actor/stateful_actor.rs`)
- **`ActorContext::spawn_scoped(|mut scope: ActorScope<Self>| async move { ... }) -> AbortHandle`:**
  - Passes an [`ActorScope<A>`](src/actor/stateful_actor.rs) pinned to the spawning instance's `(name, instance_id)` with `is_cancelled()`, cancel-safe `cancelled()`, `cast(msg)`, and `try_cast(msg)`.
  - Once that actor instance exits, begins final stop hooks, or panics, `scope.cancelled()` completes immediately and `scope.cast` / `scope.try_cast` return `Err(ActorError::ActorUnavailable)` instead of silently rebinding onto a restarted replacement instance.
- **`ActorContext::forward_receiver(rx, map_msg) -> AbortHandle`:**
  - Pulls items from an external `tokio::sync::mpsc::Receiver<T>` into the actor via `cast`, awaiting `rx.recv()` *before* reserving a bounded mailbox permit so idle stream forwarders never starve callers on `channel_capacity(1)` actors.
  - Terminates cleanly when the upstream sender drops or when the actor instance stops or panics.

### 3. Runtime Introspection, Tree Snapshots & Health Observation (`src/actor/model.rs`, `src/actor/supervisor.rs`, `src/actor/stateful_actor.rs`, `src/actor/generic_actor.rs`)
- **Sliding-Window Restart Count (`RestartTracker::restarts_in_window`):**
  - Dynamically filters recorded crash timestamps against `RestartPolicy::time_window` (sharing window logic with `record_crash()`), allowing expired crashes to age out even when no new crash occurs.
- **Bounded Supervisor Snapshots (`SupervisorHandle::snapshot`, `SupervisorHandle::snapshot_timeout`):**
  - Captures an ordered [`SupervisorSnapshot`](src/actor/supervisor.rs) (`name`, `instance_id`, `strategy`, `children: Vec<ChildSnapshot>`) wrapped in `DEFAULT_SNAPSHOT_TIMEOUT` (2 seconds) or an explicit `Duration`, returning `SupervisorError::Timeout(Duration)` if the supervisor is stalled inside heavy teardown or backoff recovery.
  - Includes [`SupervisorSnapshot`](src/actor/supervisor.rs) inspection helpers: `child(name)`, `all_running()`, `any_failed()`, `any_restarting()`, `running_count()`, `restarting_count()`, `stopped_count()`, and `failed_count()`.
  - Each [`ChildSnapshot`](src/actor/supervisor.rs) exposes `name`, `status: ChildStatus`, `instance_id`, `restart_type`, `restarts_in_window`, and `is_supervisor` (recorded from `ChildSpec::is_supervisor` so stopped or failed child supervisors remain identifiable).
  - [`ChildStatus`](src/actor/supervisor.rs) (`Running`, `Restarting`, `Stopped`, `Failed`) provides `is_running()`, `is_restarting()`, `is_stopped()`, `is_failed()`, and `instance_id()` without exposing internal channel endpoints.
- **Recursive Supervision Tree Snapshots (`SupervisorHandle::snapshot_tree`, `SupervisorHandle::snapshot_tree_timeout`):**
  - Collects a [`SupervisorTreeSnapshot`](src/actor/supervisor.rs) (`snapshot`, `subtrees: Vec<SubtreeSnapshot>`) across nested supervisor hierarchies with helpers `subtree(child_name)`, `find(path)`, `all_running_deep()`, `any_failed_deep()`, `any_restarting_deep()`, and `unavailable_count()`.
  - Runs recursion concurrently on the caller task via `JoinSet` under a **single overall deadline** (`DEFAULT_SNAPSHOT_TIMEOUT` or explicit `Duration`), avoiding supervisor-blocks-on-child deadlocks and isolating slow, restarting, stopped (`ChildNotFound`), or failed child subtrees inside `SubtreeSnapshot::result` without discarding healthy siblings.
- **Read-Only Status Subscription (`SupervisorHandle::watch_child_status`):**
  - Returns a [`ChildStatusWatcher`](src/actor/supervisor.rs) wrapping the supervisor's existing per-child `watch<SlotState>` channel (`child_name()`, `current()`, `changed()`, `wait_for(...)`).
  - Intentionally allows observing children that are currently `ChildStatus::Stopped` or `ChildStatus::Failed`, returning `SupervisorError::SupervisorUnavailable` if the child slot is deleted or the owning supervisor stops/restarts.
- **Lock-Free Client Queue Inspection (`ActorClient::queue_stats`):**
  - Returns [`QueueStats`](src/actor/stateful_actor.rs) (`max_capacity`, `available`, `queued`, `is_closed`, `is_full()`) read lock-free from the locally cached sender (`current_sender()`) without touching the supervisor.
  - Documents point-in-time approximation and the post-crash/pre-rebind behavior (`is_closed == true` until the next `call`/`cast` triggers reactive rebinding).
- **Non-Intrusive Resource Cache Health (`ResourceClient::cache_state`):**
  - Synchronously classifies the client's 0-hop `watch` cache without crossing the actor mailbox, contacting the supervisor, triggering refreshes, or exposing the cached value.
  - Returns [`CacheState`](src/actor/generic_actor.rs) (`Empty`, `Fresh { expires_in }`, `RefreshDue { expires_in }`, `StaleUsable { expired_for }`, `Expired { expired_for }`, `Invalidated`) with `is_fresh()` and `has_usable_value()`, documenting the same post-crash/pre-rebind `Invalidated` caveat as `ActorClient::queue_stats`.
- All 10 new public symbols (`DEFAULT_SNAPSHOT_TIMEOUT`, `ActorScope`, `QueueStats`, `CacheState`, `ChildStatus`, `ChildSnapshot`, `SupervisorSnapshot`, `SupervisorTreeSnapshot`, `SubtreeSnapshot`, `ChildStatusWatcher`) are re-exported from both `otp_actor::actor` and the crate root `otp_actor`.

### 4. Examples, Documentation & docs.rs Builds
- **New & Extended Examples:**
  - [`examples/minimum.rs`](examples/minimum.rs): Minimal, output-free showcase of `ResourceActor` and `StatefulActor` under `Supervisor::new_root()`.
  - [`examples/quickstart.rs`](examples/quickstart.rs): ~35-line real task-panic isolation and 0-ms reactive `ActorClient` rebind demo.
  - [`examples/rest_for_one_pipeline.rs`](examples/rest_for_one_pipeline.rs): 3-stage `RestForOne` pipeline panic-recovery showcase with automated and `--interactive` modes.
  - [`examples/microservice_pipeline.rs`](examples/microservice_pipeline.rs): Extended with `send_interval_with` heartbeat flush ticks, `forward_receiver` stream ingestion, `queue_stats()`, `cache_state()` (`Fresh -> StaleUsable -> Fresh`), `watch_child_status()`, and `snapshot()` / `snapshot_tree()` health checks alongside `ResourceActor` auth caching and `try_cast` backpressure.
- **Documentation & docs.rs:**
  - Updated [`README.md`](README.md) with `Why otp-actor?` comparison matrix, runnable quickstart, timers/streams/scoped-task patterns, runtime introspection walkthroughs (`snapshot`, `snapshot_tree`, `watch_child_status`, `queue_stats`, `cache_state`), updated 86-test integration inventory, and project layout.
  - Updated [`docs/actor_model_guide.md`](docs/actor_model_guide.md) with Sections `3.7` (*Lifecycle-Scoped Background Tasks, Timers, and Stream Forwarding*) and `4.6` (*Runtime Introspection & Non-Intrusive Health Observation*, all 6 architectural trade-offs), updated Section `4.5` architecture diagram (`watch<SlotState>`), and Section `5.2` checklist.
  - Added `[package.metadata.docs.rs] all-features = true` in [`Cargo.toml`](Cargo.toml) and a `## Feature Flags` overview in [`src/lib.rs`](src/lib.rs) so `otp_actor::remote` is documented on docs.rs.

---

## Upgrade & Compatibility (from `0.3.0` to `0.3.1`)

- **100% Backwards-Compatible:** `0.3.1` introduces no breaking API or behavioral changes relative to `0.3.0`.
- Updating `otp-actor = "0.3.1"` in `Cargo.toml` is a drop-in upgrade. Because `SupervisorError` (and every newly introduced snapshot/health enum and struct) carries `#[non_exhaustive]`, adding `SupervisorError::Timeout(Duration)` preserves downstream exhaustive match compatibility.

---

## Verification

All non-network workspace checks were executed and passed for `0.3.1`:
- `cargo test --lib --all-features`: **1 / 1 passed**
- `cargo test --test actor_supervision_test --all-features`: **86 / 86 passed**
- `cargo test --doc --all-features`: **4 / 4 passed**
- `cargo clippy --all-targets --all-features -- -D warnings`: **0 warnings**
- `cargo clippy --lib --no-default-features -- -D warnings`: **0 warnings**
- `cargo doc --no-deps --all-features`: **0 warnings**
- `cargo check --examples --all-features` & runnable demos (`minimum`, `quickstart`, `rest_for_one_pipeline`, `microservice_pipeline`): **passed**
- `cargo package --offline --allow-dirty`: **40 files packaged and verified**
- `cargo test --test remote_grpc_test --features remote --no-run`: compiled cleanly (`src/remote/` and `tests/remote_grpc_test.rs` are unchanged since `0.3.0`; run `cargo test --test remote_grpc_test --features remote` locally with TCP loopback enabled prior to publishing)

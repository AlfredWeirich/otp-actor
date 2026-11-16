# Release Notes — `otp-actor` v0.3.3

**Release Date:** 2026-11-16  
**Minimum Supported Rust Version (MSRV):** Rust 1.88 (`edition = "2024"`)

`otp-actor` **0.3.3** is a non-breaking observability and remote-telemetry release on top of `0.3.2`. It introduces lock-free actor execution counters (`ActorMetrics`), structured operational health and node status snapshots (`NodeHealth`, `NodeStatus`, `LocalNodeMonitor`), a pure OpenMetrics/Prometheus text exposition formatter (`format_prometheus_metrics`) with zero HTTP or JSON dependencies, and a backwards-compatible gRPC control-plane service (`ActorNodeAdmin`) paired with location-transparent monitoring (`RemoteNodeMonitor`, `AnyNodeMonitor`).

---

## Highlights

- **Lock-Free Actor Metrics & Lifecycle Uptimes (`ActorMetrics`, `SupervisorHandle::uptime`, `ChildSnapshot`):** Every `StatefulActor` automatically tracks `in_flight`, `calls_total`, `casts_total`, `handler_errors_total`, `timeouts_total`, `panics_total`, `last_latency_us`, and `ema_latency_us` via relaxed atomics across restarts—with zero locks on the hot path—while `Supervisor`, `SupervisorHandle`, and `ChildSnapshot` track supervisor/incarnation uptimes, cumulative `restarts_total`, `consecutive_failures`, mailbox `QueueStats`, and `ResourceActor` `CacheState`.
- **Local Operational Monitoring & Prometheus Exposition (`LocalNodeMonitor`, `format_prometheus_metrics`):** Exposes fast zero-hop liveness checks (`health()`), bounded tree readiness checks (`health_checked(timeout)` distinguishing `Healthy`, `Degraded`, and `Unavailable`), full `NodeStatus` snapshots (`status(opts)`), and pure Prometheus/OpenMetrics text formatting without pulling in any HTTP server or JSON crate.
- **Non-Breaking Remote gRPC Control Plane (`ActorNodeAdmin`, `RemoteNodeMonitor`, `AnyNodeMonitor`):** Adds a separate `ActorNodeAdmin` gRPC service (`Health` and `Status` RPCs) alongside `ActorNode` in [`proto/actor_node.proto`](proto/actor_node.proto), tracks remote gRPC dispatch counters (`RemoteMetricsSnapshot`) and `ActorNodeRegistry::snapshot_endpoints()`, honors `Authorizer` and `ActorNodeHandle::shutdown` drain state (`HealthState::Draining`), and provides location transparency via `AnyNodeMonitor` (`Local` vs. `Remote`).
- **Two New Runnable Examples & Extended Test Coverage:** Adds [`examples/telemetry.rs`](examples/telemetry.rs) (core, no extra features) and [`examples/remote/monitor.rs`](examples/remote/monitor.rs) (`remote_monitor`, `--features remote`), plus 5 new integration tests across [`tests/actor_supervision_test.rs`](tests/actor_supervision_test.rs) and [`tests/remote_grpc_test.rs`](tests/remote_grpc_test.rs).

---

## What's New

### 1. Lock-Free Actor Metrics, Supervisor Uptimes & Enriched Snapshots (`src/actor/telemetry.rs`, `src/actor/supervisor.rs`, `src/actor/stateful_actor.rs`, `src/actor/generic_actor.rs`)
- **`ActorMetrics` & `ActorMetricsSnapshot` ([`src/actor/telemetry.rs`](src/actor/telemetry.rs)):**
  - Automatically instantiated per `StatefulActorBuilder::build_child_spec` and shared across supervisor restarts so counters persist across actor incarnations.
  - Uses `InFlightGuard` (RAII drop guard for accurate `in_flight` tracking across timeouts and unwinds) and `RunnerPanicGuard` (records `panics_total` on unwinding panics).
  - Computes `ema_latency_us` ($\alpha = 0.25$) lock-free via a `compare_exchange_weak` loop (`Ordering::Relaxed`).
- **Supervisor & Child Telemetry ([`src/actor/supervisor.rs`](src/actor/supervisor.rs)):**
  - Added `SupervisorHandle::started_at()`, `SupervisorHandle::uptime()`, and `SupervisorHandle::is_alive()`.
  - Extended `SupervisorSnapshot` with `uptime: Duration` and `ChildSnapshot` with `instance_uptime`, `restarts_total`, `consecutive_failures`, `queue_stats`, `actor_metrics`, and `cache_state`.
  - Wired `QueueProbeFn` on both `StatefulActorBuilder` and `ResourceActorBuilder`, and `CacheProbeFn` on `ResourceActorBuilder`, so bounded mailbox saturation and `CacheState` appear directly in snapshots without exposing cached resource values.

### 2. Local Node Monitor & Prometheus Exposition (`src/actor/telemetry.rs`, `src/actor/mod.rs`)
- **`LocalNodeMonitor`:**
  - `LocalNodeMonitor::new(supervisor).with_started_at(app_start)` allows host applications to anchor `uptime_seconds` to their own process start time while defaulting to the supervisor's start time.
  - `health()` performs a synchronous zero-hop check on `SupervisorHandle::is_alive()`.
  - `health_checked(timeout)` inspects the supervision tree under a bounded deadline (`Healthy`, `Degraded`, `Unavailable`).
  - `status(StatusOptions)` / `status_default()` builds `NodeStatus` (`health`, `timestamp_unix`, `summary: NodeSummary`, optional `supervision_tree: SupervisorStatusNode`).
  - `prometheus_metrics(StatusOptions)` and `format_prometheus_metrics(&NodeStatus) -> String` emit OpenMetrics/Prometheus text with escaped label values (`\`, `"`, `\n`, `\r`, control characters).
- **Zero-JSON Core:** All new DTOs work under `default = []` without `serde` or `serde_json`, and derive `serde::Serialize` / `serde::Deserialize` via `#[cfg_attr(feature = "remote", ...)]` (alongside `RestartType`, `SupervisionStrategy`, `QueueStats`, and `CacheState`) for `postcard` wire transport.

### 3. Non-Breaking Remote Telemetry over gRPC (`proto/actor_node.proto`, `src/remote/`)
- **Separate `ActorNodeAdmin` gRPC Service ([`proto/actor_node.proto`](proto/actor_node.proto), [`src/remote/proto.rs`](src/remote/proto.rs)):**
  - Defines `rpc Health (HealthRequest) returns (HealthResponse)` and `rpc Status (StatusRequest) returns (StatusResponse)` on `ActorNodeAdmin`, leaving `service ActorNode` (`Call`, `Cast`) completely untouched.
  - Both `ActorNodeServer` and `ActorNodeAdminServer` are hosted on the same listener in [`ActorNodeBuilder::start`](src/remote/server.rs) and [`ActorNodeBuilder::start_with_interceptor`](src/remote/server.rs).
- **Server-Side Counters, Drain State & Registry Snapshots ([`src/remote/server.rs`](src/remote/server.rs), [`src/remote/registry.rs`](src/remote/registry.rs), [`src/remote/endpoint.rs`](src/remote/endpoint.rs)):**
  - `ByteEndpoint` gains a defaulted `fn queue_stats(&self) -> Option<QueueStats> { None }` implemented for `ActorClient<A>`.
  - `ActorNodeRegistry` adds `len()`, `is_empty()`, and deterministic `snapshot_endpoints()`.
  - `ActorNodeService` records `RemoteMetricsSnapshot` (`registered_endpoints`, `grpc_in_flight`, `grpc_calls_total`, `grpc_casts_total`, `auth_rejected_total`, `service_not_found_total`, `actor_not_found_total`, `incompatible_protocol_total`, `framework_errors_total`).
  - `ActorNodeAdminService` enforces the configured `Authorizer` (with `("__admin", "health" | "status")`), reports `HealthState::Draining` immediately once `ActorNodeHandle::shutdown()` begins Phase 1, and supports nodes with or without an attached `SupervisorHandle`.
  - `ActorNodeBuilder::started_at(Instant)` and `ActorNodeHandle::uptime()` / `ActorNodeHandle::remote_metrics()` expose node-level timing and transport counters locally.
- **Location-Transparent Monitor Client ([`src/remote/monitor.rs`](src/remote/monitor.rs), [`src/remote/mod.rs`](src/remote/mod.rs)):**
  - Added `RemoteNodeMonitor` (`connect`, `connect_with`, `with_request_metadata`, `health`, `health_timeout`, `status`, `status_default`, `prometheus_metrics`) and `RemoteMonitorError` (`RemoteActorError<Infallible>`).
  - Added `AnyNodeMonitor::Local(LocalNodeMonitor) | Remote(RemoteNodeMonitor)` for uniform local and remote `health()`, `status()`, and `prometheus_metrics()` calls.

### 4. Runnable Examples (`examples/telemetry.rs`, `examples/remote/monitor.rs`)
- **[`examples/telemetry.rs`](examples/telemetry.rs):** Core example (`cargo run --example telemetry`) showing `LocalNodeMonitor`, `health()`, `health_checked()` (`Healthy` → `Degraded` → `Healthy` → `Unavailable`), `NodeSummary`, `CacheState`, and Prometheus output.
- **[`examples/remote/monitor.rs`](examples/remote/monitor.rs):** Remote example (`cargo run --example remote_monitor --features remote`, registered as `remote_monitor` in [`Cargo.toml`](Cargo.toml)) showing `ActorNodeHandle::builder` with `started_at` and Bearer-token `Authorizer`, remote `Call`/`Cast`/`ActorNotFound` traffic, `AnyNodeMonitor` (`Local` vs. `Remote`), `RemoteMetricsSnapshot`, and remote Prometheus output.

---

## Upgrade & Compatibility (from `0.3.2` to `0.3.3`)

- **100% Backwards-Compatible (Wire & Rust API):**
  - Existing `ActorNode` (`Call` / `Cast`) wire messages (`protocol_version = 1`) and the generated `ActorNode` trait are completely unchanged; adding `ActorNodeAdmin` as a separate gRPC service avoids breaking custom `impl ActorNode` implementations.
  - `ByteEndpoint::queue_stats` provides a default `{ None }` implementation so custom `impl ByteEndpoint` types continue compiling unchanged.
  - `SupervisorSnapshot`, `ChildSnapshot`, `NodeHealth`, `NodeStatus`, `StatusOptions`, `NodeSummary`, and `ActorMetricsSnapshot` are marked `#[non_exhaustive]`.

---

## Verification

All workspace checks, tests, clippy lints, documentation builds, and runnable examples passed:
- `cargo test --test actor_supervision_test --all-features`: **89 / 89 passed** (including 3 new telemetry & Prometheus tests)
- `cargo test --test remote_grpc_test --features remote`: **11 / 11 passed** (including 2 new `ActorNodeAdmin` / `AnyNodeMonitor` E2E & auth tests)
- `cargo test --lib --all-features`: **1 / 1 passed**
- `cargo test --doc --all-features`: **4 / 4 passed**
- `cargo clippy --all-targets --all-features -- -D warnings`: **0 warnings**
- `cargo clippy --lib --no-default-features -- -D warnings`: **0 warnings**
- `cargo doc --no-deps --all-features`: **0 warnings**
- `cargo run --example telemetry`: **passed**
- `cargo run --example remote_monitor --features remote`: **passed**

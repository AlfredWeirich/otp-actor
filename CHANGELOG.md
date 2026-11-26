# Changelog

All notable changes to `otp-actor` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Minimum Supported Rust Version (MSRV):** Rust 1.88 (`edition = "2024"`)

---

## [Unreleased]

---

## [0.4.2] - 2026-11-25

`otp-actor` **0.4.2** is a documentation-only patch release on top of `0.4.1`.

### Changed
- **Documentation & Package Metadata ([`README.md`](README.md), [`Cargo.toml`](Cargo.toml)):**
  - Overhauled the `README.md` structure, overview tables, and examples guide.
  - Bumped crate version to `0.4.2` for crates.io publication.

### Compatibility
- 100% backwards-compatible with `0.4.1` (no changes to `src/`).

---

## [0.4.1] - 2026-11-24

`otp-actor` **0.4.1** is a documentation-only patch release on top of `0.4.0`.

### Changed
- **Documentation & Package Metadata ([`README.md`](README.md), [`Cargo.toml`](Cargo.toml)):**
  - Updated `README.md` version snippets to `0.4.0`, refreshed test counts and project layout references, and cleaned up header notes.
  - Bumped crate version to `0.4.1` for crates.io publication.

### Compatibility
- 100% backwards-compatible with `0.4.0` (no changes to `src/`).

---

## [0.4.0] - 2026-11-23

`otp-actor` **0.4.0** is a major hardening, performance, and API-refinement release on top of `0.3.3` introducing zero-copy Protobuf payloads, Tower middleware support, single-task `JoinSet` supervision, and broad concurrency/reliability fixes.

### Breaking Changes
- **`Arc<tonic::Status>` Boxing on `RemoteActorError` & Generic Tower Layer Support on `ActorNodeBuilder` (`src/remote/{error,server}.rs`, `Cargo.toml`):**
  - Boxed `RemoteActorError::Transport(Arc<tonic::Status>)` (reducing `sizeof(RemoteActorError<E>)` to a single pointer) and implemented `PartialEq` / `Eq` (comparing `code()`, `message()`, and `details()` on `Transport`), alongside `From<Arc<tonic::Status>>` and inherent helpers `RemoteActorError::{transport, transport_status, actor_error}`.
  - Added optional `tower-layer` (`"0.3"`) under the `remote` feature and parameterized `ActorNodeBuilder<L = tower_layer::Identity>` with `.layer(new_layer)`, `.start_with_layer(new_layer)`, generalized `.server<NewL>(Server<NewL>)`, and `ActorNodeHandle::start_with<L, ResBody>(..., Server<L>)` so `Tower` middleware layers (rate limiting, mTLS/OID verification, error recovery) compose seamlessly with gRPC nodes.
  - *Migration:* Callers destructuring `RemoteActorError::Transport(status)` receive `&Arc<tonic::Status>` (dereferencing cleanly to `&tonic::Status` or via `.transport_status()`); callers constructing `RemoteActorError::from(status)` or `RemoteActorError::transport(status)` are unaffected.
- **Zero-Copy `prost::bytes::Bytes` Payloads in `proto::v1` (`build.rs`, `src/remote/{proto,endpoint}.rs`):**
  - Enabled `.bytes(".")` on `tonic_prost_build::configure()` so Prost generates zero-copy `prost::bytes::Bytes` instead of `Vec<u8>` for all `proto::v1` payload fields (`CallRequest.payload`, `CastRequest.payload`, `CallResponse.{ok_payload, actor_error_payload}`, `StatusResponse.status_payload`).
  - Re-exported `otp_actor::remote::proto::Bytes` and updated internal `BoundedVec` / `encode_payload` to return `prost::bytes::Bytes` without extra copies.
  - *Migration:* Callers constructing `proto::v1::{CallRequest, CastRequest, CallResponse, StatusResponse}` directly should pass `otp_actor::remote::proto::Bytes` (e.g., `.into()`, `Bytes::from(...)`, or `Default::default()`). High-level `RemoteActorClient` and `AnyActorClient` callers are unaffected.
- **Single-Task `Supervisor` `JoinSet` Architecture (`src/actor/{supervisor,generic_actor,stateful_actor,mod}.rs`):**
  - Added public type alias `ChildFuture = Pin<Box<dyn Future<Output = TaskResult> + Send + 'static>>` and updated `UntypedActorFactory` to return `(UntypedSender, ChildFuture)` instead of `(UntypedSender, JoinHandle<TaskResult>)`.
  - `Supervisor` now spawns each child future directly into its `JoinSet<TaskResult>` and maps `tokio::task::Id` via `join_next_with_id()`, eliminating the second monitor wrapper task per supervised child and letting `JoinSet::abort_all()` abort child tasks directly.
  - *Migration:* Custom `ChildSpec::new(...)` factories should replace `tokio::spawn(async move { ... })` with `Box::pin(async move { ... })`. High-level builders (`StatefulActorBuilder`, `ResourceActorBuilder`, `SupervisorBuilder`) are unaffected.
- **API Encapsulation & `#[non_exhaustive]` Future-Proofing (`src/actor/{generic_actor,stateful_actor,supervisor,telemetry,mod}.rs`):**
  - Added `#[non_exhaustive]` to `WorkerContext`, `SpawnContext`, `ChildKind`, and `ChildLifecycle`.
  - Restricted internal control/channel enums `SlotState` and `ActorControl` to `pub(crate)` and removed them from public crate re-exports (external observers continue using `ChildStatus` / `ChildStatusWatcher` and `ActorContext::{stop, fail}`).

### Added
- **`RestartPolicy: Eq`, `SupervisorError`/`ChildStatus` Serde & `ChildStatus`/`ChildLifecycle` Bridge (`src/actor/{model,supervisor,telemetry}.rs`):**
  - Normalized invalid (`< 1.0` or non-finite) `backoff_multiplier` values directly in `RestartPolicy::backoff_multiplier()` and implemented `Eq` (with canonical bit-level `PartialEq` semantics) on `RestartPolicy`.
  - Derived `serde::{Serialize, Deserialize}` under `#[cfg(feature = "remote")]` for `SupervisorError` and `ChildStatus`, added `ChildStatus::{as_str, failure_error, to_lifecycle}`, `ChildLifecycle::{is_running, is_restarting, is_stopped, is_failed, failure_reason}`, `From<ChildStatus> for ChildLifecycle`, and symmetric cross-type `PartialEq` between `ChildStatus` and `ChildLifecycle`.
  - Verified with `test_f15_restart_policy_eq_and_child_status_lifecycle_bridge` and `test_actor_node_builder_tower_layer_and_remote_error_eq`.
- **Client API Symmetry (`ask_timeout`, `ResourceClient` Queue Telemetry & `AnyActorClient` Helpers) (`src/actor/{stateful_actor,generic_actor}.rs`, `src/remote/client.rs`):**
  - Added `ActorClient::ask_timeout` for typed `CallMessage<A>` deadlines matching `RemoteActorClient::ask_timeout` and `AnyActorClient::ask_timeout`.
  - Added `ResourceClient::service_name()` and `ResourceClient::queue_stats()`, symmetric with `ActorClient`.
  - Added `RemoteActorClient::name()` (alias for `actor_name()`), `AnyActorClient::{from_local, from_remote, connect, connect_with, name, actor_name, service_name}`, and `From<ActorClient<A>>` / `From<RemoteActorClient<A>>` without introducing extra `serde` bounds.
- **Location-Transparent `ask`/`ask_timeout` & `Debug` Implementations for Remote Types (`src/remote/{client,registry,server}.rs`):**
  - Added `ask` and `ask_timeout` (`CallMessage<A>`) on both `RemoteActorClient<A>` and `AnyActorClient<A>`, matching local `ActorClient::ask` ergonomics.
  - Added `std::fmt::Debug` implementations for `RemoteActorClient<A>`, `AnyActorClient<A>`, `ActorNodeRegistry`, `ActorNodeService`, `ActorNodeAdminService`, `ActorNodeBuilder`, and `ActorNodeHandle`.
- **Config-Driven Builders & `Debug` Implementations for Public Core Types (`src/actor/{generic_actor,stateful_actor,supervisor}.rs`):**
  - Connected `ResourceActorConfig` and `StatefulActorConfig` to their builders via `ResourceActorBuilder::{from_config, with_config, config}` and `StatefulActorBuilder::{from_config, with_config, config}` (with automatic `>= 1` clamping on `channel_capacity` and `max_waiters`).
  - Added manual `std::fmt::Debug` implementations without tightening generic trait bounds for `ActorClient<A>`, `ResourceClient<T, E>`, `StatefulActorBuilder<A, F>`, `ResourceActorBuilder<T, E, W>`, `ChildSpec`, `SupervisorBuilder`, and `SpawnContext`.

### Changed
- **Zero-Allocation `Arc<str>` Handle Cloning, Lazy Admin Endpoint Snapshots & SemVer-Safe Client Tracing (`src/actor/{generic_actor,stateful_actor,supervisor}.rs`, `src/remote/{client,endpoint,registry,server}.rs`):**
  - Updated `ActorNodeAdminService::status` to skip calling `ActorNodeRegistry::snapshot_endpoints()` (using `len()`) when `include_child_details == false` and a `SupervisorHandle` is attached, and eliminated the intermediate `Vec` and `Arc<dyn ByteEndpoint>` clones inside `ActorNodeRegistry::snapshot_endpoints` by reading `queue_stats()` directly into a pre-sized `Vec::with_capacity(total)`.
  - Switched immutable handle name fields on `ActorScope`, `ActorContext`, `ActorClient`, `ResourceClient`, `SupervisorHandle`, and `RemoteActorClient` from `String` to `Arc<str>` for zero-allocation cloning, and updated `ResourceClient::{send_control, get}` 1-hop paths to clone only `ep.sender` instead of the full `ResourceEndpoint`.
  - Added `#[inline]` to `BoundedVec::{try_extend, try_push, finalize}` and added SemVer-compatible `#[tracing::instrument]` spans to `ActorClient::{call, call_timeout, call_retry, call_retry_timeout, cast, cast_timeout, shutdown}` (`err(Debug)`), `RemoteActorClient::{call, call_timeout, cast, cast_timeout}` (`err(Debug)`), and `ResourceClient::{get, get_timeout, force_refresh, shutdown}` (without `err`, preserving `E: Clone` bounds).
  - Verified with `test_client_handles_arc_str_zero_alloc_clone_and_tracing_instrument_preserves_trait_bounds` and `test_admin_status_skips_endpoint_snapshot_without_details_and_remote_client_arc_str`.
- **Deterministic Assertions & Zero-Sleep Termination in Remote Examples (`examples/remote/{all_in_one,client}.rs`):**
  - Replaced `let _ = ...` on `AccountMsg::SimulateCrash` with explicit `assert!(matches!(..., Err(RemoteActorError::Actor(_))))` checks and removed the unnecessary `sleep(50ms)` in `all_in_one.rs`, documenting the deterministic `receiver.close()`-before-reply guarantee.
- **Zero-Allocation Label Sanitization & Pre-Sized `BoundedVec` in Remote gRPC Hot Path (`src/remote/{registry,server,endpoint}.rs`):**
  - Switched `sanitize_label` to return `Cow<'_, str>` with a zero-allocation `Cow::Borrowed` fast path for clean labels (`<= MAX_LABEL_CHARS` chars, no control chars), and guarded `span.record(...)` in `ActorNodeService::{call, cast}` with `if !span.is_disabled()`.
  - Pre-allocated `BoundedVec` with `Vec::with_capacity(64)` in `encode_payload` and removed the redundant exact-size `self.buf.reserve(...)` call in `BoundedVec::try_extend` so `Vec::extend_from_slice` uses amortized growth within the 4 MiB cap.
- **`#[must_use]` Annotations & Allocation-Free Secret Masking (`src/actor/*`, `examples/{simple,otp_actor}.rs`):**
  - Added `#[must_use]` to pure getters, predicates, snapshots, and builder/constructor methods across `src/actor/*` (0 `clippy::must_use_candidate` warnings on `--lib`).
  - Rewrote `mask_secret` in `examples/simple.rs` and `examples/otp_actor.rs` using `str::char_indices()` without intermediate `Vec<char>` or temporary prefix/suffix `String` allocations.
- **Avoid `O(N)` `child_order` Scans & Upfront `ChildSpec` Clones (`src/actor/supervisor.rs`):**
  - Updated `Supervisor::spawn_child_from_spec` to borrow `&ChildSpec`, eliminating upfront `ChildSpec::clone()` calls in `boot_initial_children` and `SupervisorCommand::StartChild` (especially on failure paths).
  - Replaced `O(N)` `self.child_order.contains(...)` scans with `O(1)` map checks (`existing.is_none()` / `!self.children.contains_key(...)`).
- **Structured Tracing Instrumentation & Span Propagation (`src/actor/{supervisor,stateful_actor,generic_actor}.rs`):**
  - Added `#[tracing::instrument(..., err)]` on `SupervisorHandle` control-plane methods (`start_child`, `restart_child`, `stop_child`, `delete_child`, `request_shutdown`, `shutdown`, `snapshot_timeout`, `snapshot_tree_timeout`, `watch_child_status`) without tightening generic trait bounds.
  - Bound spawned `Supervisor`, `StatefulActor`, `ResourceActor`, background `Worker`, and `ActorContext` lifecycle tasks (`send_after`, `send_interval_with`, `spawn_scoped`, `forward_receiver`) to their respective actor/supervisor spans via `tracing::Instrument`.
- **Pre-Escaped Prometheus Labels & Zero Intermediate Filter Vecs (`src/actor/telemetry.rs`):**
  - Pre-escape supervisor paths once per supervisor level (`Arc<str>`) and child names once per child in `collect_flat_children`, eliminating up to 28 redundant heap allocations per child per scrape and removing the 3 intermediate filter `Vec`s in `format_prometheus_metrics` with byte-identical output.
- **Eliminate Redundant Resource Clones & Duplicate Cache Storage (`src/actor/generic_actor.rs`):**
  - Removed redundant `current_resource` and `refresh_threshold` fields from `ResourceActor`, reading directly from the single `cache_tx` (`watch::Sender<Option<ResourceSnapshot<T>>>`) source of truth.
  - `ResourceActor::handle_refresh_complete` now performs zero clones of `T` when `waiters` is empty (e.g. `eager_prewarm` and proactive background refreshes) and exactly `N` clones for `N` parked waiters (down from `N + 2`).
- **Zero-Clone 0-Hop Hot Path & Single-Timestamp Cache Checks (`src/actor/generic_actor.rs`, `src/actor/stateful_actor.rs`):**
  - Eliminated `ResourceEndpoint::clone()` (3 atomic increments + 3 decrements) on the 0-hop fast path of `ResourceClient::get()` and in `ResourceClient::rebind_channel()`, and eliminated `Sender::clone()` in `ActorClient::queue_stats()` and `ActorClient::rebind_channel()`.
  - Added `TimedResource::{is_still_usable_at, is_usable_stale_at}` so `ResourceClient::get()` and `ResourceEndpoint::cache_state()` perform at most one `Instant::now()` call (and zero clock calls for perpetual resources with no refresh threshold).

### Fixed
- **Prometheus Summary & Registry-Only Metrics, Atomic EMA Sentinel & Edge-Case Hardening (`src/actor/{telemetry,generic_actor,model}.rs`):**
  - Exported the 6 missing `NodeSummary` Prometheus metrics (`otp_actor_summary_full_mailboxes`, `otp_actor_summary_calls_total`, `otp_actor_summary_casts_total`, `otp_actor_summary_handler_errors_total`, `otp_actor_summary_timeouts_total`, `otp_actor_summary_panics_total`) and `registered_endpoints` queue gauges (`otp_actor_registered_endpoint_mailbox_queued`, `otp_actor_registered_endpoint_mailbox_capacity`), fixing exports for `include_tree(false)` and registry-only remote nodes (`supervisor: None`).
  - Eliminated the latency initialization race in `ActorMetrics` by replacing `has_latency_sample: AtomicBool` with an `UNINITIALIZED_EMA_US = u64::MAX` sentinel updated atomically via `compare_exchange_weak`.
  - Pruned closed waiter channels before enforcing `max_waiters` and logged non-cancelled worker panics in `ResourceActor::finish_graceful_shutdown`, clarified `TimedResource::is_fresh` buffer clamping in docs, and hardened `RestartTracker::record_crash` (`max_backoff` floored at `initial_backoff`; non-finite or `< 1.0` `backoff_multiplier` values clamped to `1.0`).
  - Verified with `test_format_prometheus_metrics_exposition_and_label_escaping`, `test_f11_prometheus_registered_endpoints_and_f14_api_symmetry_and_clamping`, and extended assertions in `tests/remote_grpc_test.rs`.
- **Dynamic Aging of `RestartTracker::consecutive_failures()`, Single-Roundtrip `SnapshotLevel` Tree Snapshots & Accurate Health/Uptime Reporting (`src/actor/{model,supervisor}.rs`, `src/remote/{registry,server}.rs`):**
  - Updated `RestartTracker::consecutive_failures()` to verify the most recent crash timestamp against `RestartPolicy::time_window`, returning `0` once a full stability window elapses without another crash (`restarts_in_window() == 0`).
  - Added internal `SupervisorCommand::SnapshotLevel` returning both the flat `SupervisorSnapshot` and direct `Sender<SupervisorCommand>` endpoints for immediate child supervisors, eliminating `K` redundant `ResolveChildSlot` roundtrips per level in `snapshot_tree_timeout` and avoiding blocking on `SlotState::Restarting` backoff timers.
  - Added `started_at: Instant` to `SlotState::Running` and updated `SupervisorHandle::{started_at, uptime}` so handles returned by `SupervisorHandle::get_supervisor()` preserve the child supervisor's actual creation/restart timestamp (and keep updating across child supervisor restarts).
  - Added `ActorNodeRegistry::has_closed_endpoint` and updated `ActorNodeAdminService::{compute_fast_health, status}` to report `HealthState::Degraded` when registered endpoints report closed mailboxes and populate `running_children` / `stopped_children` in registry-only mode.
  - Verified with `test_restart_tracker_restarts_in_window_ages_out_without_new_crash`, `test_consecutive_failures_ages_out_and_child_supervisor_started_at_and_snapshot_tree_no_backoff_stall`, and `test_admin_service_health_status_authorizer_and_registry_only_mode`.
- **Preserve Sibling `sender` During Sequential Restarts, Wait on `Stopping` Children & Graceful Shutdown on Escalation (`src/actor/supervisor.rs`):**
  - Updated `TerminationPlan::take` to clone `entry.sender` instead of replacing it prematurely with `Arc::new(())` (clearing `entry.sender` and `entry.abort_handle` only once the exited task is reaped in `handle_child_exit` / `handle_child_failure`), keeping `queue_probe` / `cache_probe` telemetry and shutdown signals intact for siblings still running during `OneForAll` / `RestForOne` sequential drains.
  - Updated `Supervisor::graceful_shutdown_all` to wait on children already in `ChildPhase::Stopping` (`term_rx.wait_for(|&done| done)`) so reverse-registration shutdown ordering holds even when racing with `stop_child`.
  - Updated `Supervisor::run` and `Supervisor::handle_command` so `.escalate_on_failure(true)` runs `graceful_shutdown_all()` before returning `Err(msg)` rather than hard-aborting remaining children.
  - Verified with new regression test `test_one_for_all_termination_preserves_sibling_telemetry_and_escalate_on_failure_runs_graceful_shutdown`.
- **Pre-Subscribed `SlotState` Channel for `ActorContext::client()` (`src/actor/supervisor.rs`, `src/actor/stateful_actor.rs`):**
  - Propagated `slot_rx: watch::Receiver<SlotState>` through `SpawnContext`, `StatefulActorRunner`, and `ActorContext` (reusing existing slot channels on restarts without mutating state if the factory panics).
  - Removed `ActorClient::new_unsubscribed` and simplified `ActorClient.slot_rx` to a non-optional `Arc<RwLock<watch::Receiver<SlotState>>>`, enabling clients obtained via `ctx.client()` to rebind synchronously on `try_cast()` after an actor restart.
  - Verified with new regression test `test_actor_context_client_try_cast_rebinds_synchronously_after_actor_restart`.
- **Zero-Hop `ResourceClient::get()` & Single-Flight Trigger During `RefreshDue` (`src/actor/generic_actor.rs`):**
  - Kept `ResourceClient::get()` on the 0-hop `watch` cache fast path for usable `CacheState::RefreshDue` entries instead of falling back to 1-hop `oneshot` + mailbox roundtrips for the entire `expiry_buffer` window.
  - Added lock-free `refresh_pending` (`AtomicBool`) and `throttle_until_ns` (`AtomicU64`) endpoint state with a fire-and-forget `ResourceCommand::TriggerBackgroundRefresh`, eliminating mailbox stampedes while a background refresh is in flight or throttled by `failure_cooldown`, `failure_backoff_delay`, or retry delay.
  - Verified with new regression test `test_resource_client_refresh_due_serves_zero_hop_without_mailbox_stampede`.
- **Immediate Slot Reuse After `stop_child` on Exited/Stopped Tasks (`src/actor/supervisor.rs`):**
  - Included `ChildPhase::Stopping` in `ChildPhase::is_active_or_terminating` alongside `Stopped` and `Failed(_)` so `start_child` never falsely rejects with `SupervisorError::ChildAlreadyExists` when an exited task's `Stopping -> Stopped` transition is still queued behind `StartChild`.
  - Updated `Supervisor::trigger_child_shutdown` to inspect `*entry.term_rx.borrow()` immediately: children with no active task (`Failed(_)`, backoff-sleeping `Restarting`, or already exited tasks) transition directly to `ChildPhase::Stopped` (clearing `abort_handle` and `sender`) rather than waiting indefinitely in `ChildPhase::Stopping` for a non-existent `JoinSet` exit.
  - Verified by `test_stop_child_on_failed_and_backoff_restarting_children_allows_immediate_start_child`.
- **Canonical `child_order` Preservation in `child_specs` Across Stop/Restart (`src/actor/supervisor.rs`):**
  - Replaced append-only `child_specs.push(...)` calls in `StartChild` and `handle_restart_child` with `Supervisor::upsert_child_spec`, inserting restored or re-registered `ChildSpec` entries at their canonical `child_order` index.
  - Extended `handle_restart_child` spec restoration to all strategy-coupled targets (`OneForAll` and `RestForOne` siblings as well as the directly restarted child).
  - Preserves `RestForOne` dependency ordering and reverse-registration graceful shutdown order across parent supervisor reboots, verified by `test_child_specs_preserve_registration_order_after_stop_restart_and_supervisor_reboot`.
- **Non-Blocking Remote `try_cast`, Configurable `default_cast_timeout` & Phase-1 Drain Rejection (`src/remote/{endpoint,client,server}.rs`):**
  - Updated `ByteEndpoint::cast_bytes` so `timeout == Some(Duration::ZERO)` invokes `ActorClient::try_cast` directly (returning `Kind::MailboxFull` immediately without blocking), and updated `RemoteActorClient::cast_timeout` / `AnyActorClient::cast_timeout` for symmetric `Duration::ZERO` semantics.
  - Added `ActorNodeService::with_default_cast_timeout` and `ActorNodeBuilder::default_cast_timeout` so operators can configure server-side fallback cast backpressure (including `Some(Duration::ZERO)` for non-blocking load-shedding).
  - Wired the node's Phase-1 `draining` flag into `ActorNodeService` so new `Call`/`Cast` frames arriving after shutdown begins are rejected cleanly with `Kind::ActorUnavailable` while already in-flight requests finish draining.
- **Preserve `Error::source()` Chain on `RemoteActorError` & Deduplicate gRPC Auth/Protocol Checks (`src/remote/{error,server}.rs`):**
  - Implemented `std::error::Error::source()` on `RemoteActorError<E>` to expose the underlying `tonic::Status` for `RemoteActorError::Transport` without tightening generic bounds on `E`.
  - Extracted shared `authorize_or_record` and `check_protocol_version` helpers in `src/remote/server.rs` across `ActorNodeService::{call, cast}` and `ActorNodeAdminService::{health, status}`.
- **Reserve Transport Headroom Between Server Timeout Budget & Client Deadline (`src/remote/{client,monitor}.rs`):**
  - Added `server_budget_for_deadline` (reserving up to `min(deadline / 4, 100ms)` of transport/serialization headroom) for `RemoteActorClient::{call_timeout, cast_timeout}` and `RemoteNodeMonitor::status`, and removed redundant per-request `tonic::Request::set_timeout` timers already enforced by the outer `tokio::time::timeout`.
  - Prevents premature HTTP/2 `RST_STREAM` cancellations so degraded `NodeStatus` snapshots (when a subtree hangs) and structured server `Kind::Timeout` responses (and server `framework_errors_total` accounting) arrive cleanly within the client's deadline.
- **Export Missing `otp_actor_remote_incompatible_protocol_total` Prometheus Metric (`src/actor/telemetry.rs`):**
  - Exported `RemoteMetricsSnapshot::incompatible_protocol_total` as `otp_actor_remote_incompatible_protocol_total` (`counter`) in `format_prometheus_metrics`, aligning Prometheus text exposition with all `RemoteNodeMetrics` counters.
- **Abort Dependent Restarts on Factory Panic & Cancel Backoff Tasks on Shutdown (`src/actor/supervisor.rs`):**
  - Under `SupervisionStrategy::OneForAll` / `RestForOne`, when a predecessor's child factory panics during a manual `restart_child`, `Supervisor::execute_restart` now aborts spawning subsequent dependent siblings and transitions their slots to `Failed(FactoryPanicked(...))` so clients fail fast and a follow-up `restart_child` can recover the entire group.
  - Coupled backoff sleep tasks in `handle_child_failure` to both `shutdown_rx` and `scheduled_restart_tx.closed()` (and closed `scheduled_restart_rx` at shutdown entry) so pending backoff timers terminate immediately on supervisor shutdown.
- **Overflow-Safe Backoff Offsetting & Virtual-Time Handler Latency (`src/actor/model.rs`, `src/actor/stateful_actor.rs`):**
  - Switched `RestartTracker::offset_for_backoff` to `Instant::checked_add(backoff)` to prevent panics when large backoffs (up to `Duration::MAX`) are configured.
  - Switched internal handler latency measurements in `StatefulActorRunner::{execute_call, execute_cast}` to `tokio::time::Instant::now()` so `ActorMetrics` honors Tokio virtual time (`start_paused = true`), while preserving `std::time::Instant` on public `SupervisorHandle::started_at` and `LocalNodeMonitor::with_started_at` APIs.
- **Preserve `SupervisorError` on Final Client Retry Attempt (`src/actor/stateful_actor.rs`, `src/actor/generic_actor.rs`):**
  - Updated all retry loops in `ActorClient` (`call`, `call_retry`, `cast`, `send_control`) and `ResourceClient` (`get`, `send_control`) to also invoke `rebind_channel(...).await?` after `attempt == MAX_ATTEMPTS` (`3`).
  - Prevents terminal `SupervisorError::MaxRestartsExceeded` / `ChildNotFound` states reached on the 3rd crash from being masked as `ActorUnavailable` (both locally and via gRPC `RemoteError`).
- **Idempotent Rebinding in `SupervisorHandle::shutdown` & `request_shutdown` (`src/actor/supervisor.rs`):**
  - Added `SupervisorHandle::resolve_sender_for_shutdown` so both `shutdown()` and `request_shutdown()` transparently rebind via `get_sender().await` when a child supervisor has restarted since the last command, instead of silently returning `Ok(())` against a closed instance `#1` sender.
  - Preserves idempotent `Ok(())` returns when the supervisor (or its parent) is already `Stopped` or `Failed`.
- **Rescue Buffered Non-`Clone` `Call` Messages on `StatefulActor` Failure Exits (`src/actor/stateful_actor.rs`):**
  - Added `StatefulActorRunner::return_unprocessed_calls` and `escalate_failure` so all buffered `ActorEnvelope::Call` messages remaining in the mailbox are returned via `CallOutcome::Unprocessed(msg)` across `on_start` failure, `ActorControl::Fail` (from `handle`, `handle_cast`, or `finish_graceful_shutdown`), `handler_timeout`, and `ActorEnvelope::SimulateCrash`, letting `ActorClient::call` transparently rebind and deliver them to the replacement instance without losing non-`Clone` payloads.
- **Sequential Reverse-Order Sibling Termination for `OneForAll` & `RestForOne` (`src/actor/supervisor.rs`):**
  - Updated `handle_restart_child` and both branches of `handle_child_failure` (recovery wave and `MaxRestartsExceeded`) to capture owned `TerminationPlan`s (including removed `RestartType::Temporary` siblings via `remove_child_slot_plan`) after synchronously transitioning slots to `ChildPhase::Restarting` / `Failed`.
  - Sibling shutdown signals and `ShutdownPolicy` deadlines now execute strictly sequentially in reverse registration order (`TerminationPlan::execute_sequence`), ensuring downstream `on_stop` hooks complete and can flush final messages to upstream siblings before upstream siblings begin shutting down.
- **Synchronous `ActorClient::try_cast` Rebinding (`src/actor/stateful_actor.rs`):**
  - When `try_cast` encounters a closed cached mailbox (`TrySendError::Closed`), it now synchronously inspects the local `SlotState` watch channel (`try_rebind_sync`) without blocking or contacting the supervisor.
  - If a replacement instance is already `SlotState::Running`, `ActorClient` immediately updates its cached sender (so `queue_stats()` also reflects the live channel) and retries `try_send` on the new instance.
  - Returns `Err(ActorError::ActorUnavailable)` while the slot is `Restarting`, and propagates `Err(ActorError::Supervisor(SupervisorError::MaxRestartsExceeded | ChildNotFound | ChildTypeMismatch))` when the slot has entered a terminal `Failed` or `Stopped` state.
  - Happy-path `try_cast` also checks `sender.read()` directly without cloning the `mpsc::Sender`.

---

## [0.3.3] - 2026-11-16

`otp-actor` **0.3.3** is a non-breaking observability and remote-telemetry release on top of `0.3.2`.

### Added
- **Lock-Free Actor Metrics (`src/actor/telemetry.rs`, `src/actor/stateful_actor.rs`, `src/actor/generic_actor.rs`):**
  - `ActorMetrics` and `ActorMetricsSnapshot` automatically instantiated per `StatefulActorBuilder::build_child_spec` and shared across supervisor restarts.
  - Tracks `in_flight`, `calls_total`, `casts_total`, `handler_errors_total`, `timeouts_total`, `panics_total`, `last_latency_us`, and lock-free `ema_latency_us` (α = 0.25) via `Ordering::Relaxed` atomics, `InFlightGuard`, and `RunnerPanicGuard`.
  - Wired `QueueProbeFn` on `StatefulActorBuilder` and `ResourceActorBuilder`, plus `CacheProbeFn` on `ResourceActorBuilder`.
- **Supervisor & Child Uptimes (`src/actor/supervisor.rs`):**
  - `SupervisorHandle::started_at()`, `SupervisorHandle::uptime()`, and `SupervisorHandle::is_alive()`.
  - Extended `SupervisorSnapshot` with `uptime: Duration` and `ChildSnapshot` with `instance_uptime`, `restarts_total`, `consecutive_failures`, `queue_stats`, `actor_metrics`, and `cache_state`.
- **Local Operational Monitoring & Prometheus Exposition (`src/actor/telemetry.rs`, `src/actor/mod.rs`):**
  - `LocalNodeMonitor` (`new`, `with_started_at`, `health()`, `health_checked(timeout)`, `status(StatusOptions)`, `status_default()`, `prometheus_metrics(StatusOptions)`).
  - Pure OpenMetrics/Prometheus text exposition via `format_prometheus_metrics(&NodeStatus) -> String` with proper label escaping and zero HTTP or JSON dependencies.
  - `NodeHealth`, `NodeStatus`, `NodeSummary`, `StatusOptions`, and `SupervisorStatusNode` (supporting `default = []` without `serde`/`serde_json`, and `#[cfg_attr(feature = "remote", ...)]` `Serialize`/`Deserialize` for `postcard` transport).
- **Non-Breaking Remote gRPC Control Plane (`proto/actor_node.proto`, `src/remote/`):**
  - Added separate `ActorNodeAdmin` gRPC service (`Health` and `Status` RPCs) hosted alongside `ActorNode` in `ActorNodeBuilder::start` and `start_with_interceptor`.
  - `ActorNodeAdminService` enforces the configured `Authorizer` (`("__admin", "health" | "status")`), reports `HealthState::Draining` once `ActorNodeHandle::shutdown()` begins Phase 1, and works with or without an attached `SupervisorHandle`.
  - `RemoteMetricsSnapshot` and `ActorNodeRegistry::snapshot_endpoints()`, `len()`, `is_empty()`, plus `ByteEndpoint::queue_stats()` (defaulted to `{ None }`, implemented for `ActorClient<A>`).
  - `ActorNodeBuilder::started_at(Instant)`, `ActorNodeHandle::uptime()`, and `ActorNodeHandle::remote_metrics()`.
  - Location-transparent monitoring via `RemoteNodeMonitor` (`connect`, `connect_with`, `with_request_metadata`, `health`, `health_timeout`, `status`, `status_default`, `prometheus_metrics`), `RemoteMonitorError`, and `AnyNodeMonitor::Local` / `AnyNodeMonitor::Remote`.
- **Examples & Tests:**
  - Added [`examples/telemetry.rs`](examples/telemetry.rs) (core) and [`examples/remote/monitor.rs`](examples/remote/monitor.rs) (`remote_monitor`, `--features remote`).
  - Added 5 new integration tests across [`tests/actor_supervision_test.rs`](tests/actor_supervision_test.rs) (89/89 passing) and [`tests/remote_grpc_test.rs`](tests/remote_grpc_test.rs) (11/11 passing).

### Compatibility
- 100% backwards-compatible with `0.3.2` (wire protocol `protocol_version = 1` and `ActorNode` trait unchanged; new snapshot/health types marked `#[non_exhaustive]`).

---

## [0.3.2] - 2026-11-11

`otp-actor` **0.3.2** is a non-breaking patch release on top of `0.3.1` focused on examples, documentation, and crates.io publication alignment.

### Changed
- **Runnable `ctx.spawn_scoped` (`ActorScope`) Showcase ([`examples/microservice_pipeline.rs`](examples/microservice_pipeline.rs)):**
  - Extended `IngestionWorker::on_start` to run `ctx.send_interval_with`, `ctx.forward_receiver`, and `ctx.spawn_scoped` side by side.
  - Emits `IngestionMsg::ScopedProbe { from_instance }` asserting `from_instance == ctx.instance_id()` and deterministically verifies via `tokio::sync::watch` exit counts that scoped tasks cancel immediately on instance crash (Phase 4) and shutdown (Phase 5) without leaking onto a restarted replacement instance.
- **Documentation & Package Metadata ([`Cargo.toml`](Cargo.toml), [`README.md`](README.md)):**
  - Bumped crate version to `0.3.2` for crates.io publication.
  - Cross-referenced `examples/microservice_pipeline.rs` under *Timers, streams & scoped background tasks* in `README.md` and updated dependency snippets, component descriptions, and project layout.

### Compatibility
- 100% backwards-compatible with `0.3.1` (no changes to `src/`).
- *Note for crates.io users upgrading from `0.3.0`:* Because `0.3.1` was tagged on GitHub without a separate crates.io publish, upgrading from `0.3.0` to `0.3.2` also includes all `0.3.1` changes below.

---

## [0.3.1] - 2026-11-11

`otp-actor` **0.3.1** is a non-breaking feature, observability, and documentation release on top of `0.3.0`.

### Added
- **Lifecycle-Bound Timers & Cancellation Guards (`src/actor/stateful_actor.rs`):**
  - `ActorContext::send_interval(period, msg)` and `ActorContext::send_interval_with(period, make_msg)` with `MissedTickBehavior::Skip`, bounded mailbox permit reservation before payload construction, and safe handling of `Duration::ZERO` / overflow-prone durations.
  - Per-instance `InstanceCancelGuard` (`watch::Sender<bool>`) in `StatefulActorRunner` that automatically cancels `send_after`, `send_interval`, and `send_interval_with` tasks on graceful stop, failure, or panic.
- **Instance-Pinned Scoped Tasks & Stream Forwarding (`src/actor/stateful_actor.rs`):**
  - `ActorContext::spawn_scoped(|mut scope: ActorScope<Self>| async move { ... })` providing `is_cancelled()`, `cancelled()`, `cast(msg)`, and `try_cast(msg)` pinned to `(name, instance_id)` so tasks terminate on exit/panic and never leak onto a restarted replacement instance.
  - `ActorContext::forward_receiver(rx, map_msg)` forwarding external `mpsc::Receiver<T>` items via `cast`, awaiting `rx.recv()` before reserving a mailbox permit.
- **Runtime Introspection, Tree Snapshots & Health Observation (`src/actor/model.rs`, `src/actor/supervisor.rs`, `src/actor/stateful_actor.rs`, `src/actor/generic_actor.rs`):**
  - `RestartTracker::restarts_in_window()` with dynamic sliding-window filtering against `RestartPolicy::time_window`.
  - Bounded `SupervisorHandle::snapshot()` and `snapshot_timeout(Duration)` (`DEFAULT_SNAPSHOT_TIMEOUT = 2s`, returning `SupervisorError::Timeout(Duration)`), plus `SupervisorSnapshot`, `ChildSnapshot` (with `is_supervisor`), and `ChildStatus` (`Running`, `Restarting`, `Stopped`, `Failed`).
  - Concurrent recursive `SupervisorHandle::snapshot_tree()` and `snapshot_tree_timeout(Duration)` returning `SupervisorTreeSnapshot` and `SubtreeSnapshot` under a single shared deadline with per-subtree failure isolation.
  - Read-only `SupervisorHandle::watch_child_status(name)` returning `ChildStatusWatcher`.
  - Lock-free `ActorClient::queue_stats()` returning `QueueStats` (`max_capacity`, `available`, `queued`, `is_closed`, `is_full()`).
  - Synchronous 0-hop `ResourceClient::cache_state()` returning `CacheState` (`Empty`, `Fresh`, `RefreshDue`, `StaleUsable`, `Expired`, `Invalidated`) without touching the mailbox or exposing cached values.
  - Re-exported all 10 new public symbols (`DEFAULT_SNAPSHOT_TIMEOUT`, `ActorScope`, `QueueStats`, `CacheState`, `ChildStatus`, `ChildSnapshot`, `SupervisorSnapshot`, `SupervisorTreeSnapshot`, `SubtreeSnapshot`, `ChildStatusWatcher`) from both `otp_actor::actor` and `otp_actor`.
- **Examples & Documentation:**
  - Added [`examples/minimum.rs`](examples/minimum.rs), [`examples/quickstart.rs`](examples/quickstart.rs), and [`examples/rest_for_one_pipeline.rs`](examples/rest_for_one_pipeline.rs); extended [`examples/microservice_pipeline.rs`](examples/microservice_pipeline.rs).
  - Added Sections `3.7` and `4.6` and updated Sections `4.5` and `5.2` in [`docs/actor_model_guide.md`](docs/actor_model_guide.md); updated [`README.md`](README.md).
  - Configured `[package.metadata.docs.rs] all-features = true` in [`Cargo.toml`](Cargo.toml) and added a `Feature Flags` overview in [`src/lib.rs`](src/lib.rs).

### Compatibility
- 100% backwards-compatible with `0.3.0` (`SupervisorError` and all new enums/structs carry `#[non_exhaustive]`).

---

## [0.3.0] - 2026-11-10

`otp-actor` **0.3.0** is a major architectural release unifying the supervision tree, introducing a reactive `watch` data plane with 0-hop resource cache reads, expanding `StatefulActor` ergonomics, and gating chaos-testing helpers.

### Added
- **Unified Recursive `Supervisor` (`src/actor/supervisor.rs`, `src/actor/model.rs`):**
  - Single composable `Supervisor` supporting arbitrary `N`-level nesting (`OneForOne`, `OneForAll`, `RestForOne`) via `Supervisor::new`, `new_root`, `with_strategy`, `builder`, `spawn_child`, `spawn_child_with_strategy`, and `SupervisorBuilder::add_child(impl Into<ChildSpec>)`.
  - Nested supervisors default to `ShutdownPolicy::Infinity` for reverse-order subtree teardown.
  - Typed lookups on `SupervisorHandle`: `get_resource_client::<T, E>`, `get_actor_client::<A>`, `get_supervisor`, and `request_shutdown()`.
  - Child factory panic isolation (`SupervisorError::FactoryPanicked`) and monotonic supervisor-wide `spawn_id` tracking to prevent stale-exit ABA races.
  - `RestartPolicy::new(max_restarts, time_window)` with fluent setters; encapsulated `RestartTracker` behind `policy()` and `consecutive_failures()`.
- **Reactive `watch` Rebinding & 0-Hop Resource Cache (`src/actor/generic_actor.rs`):**
  - `ResourceClient::get()` serves fresh values lock-free in 0 hops from `watch::Receiver<Option<ResourceSnapshot<T>>>`, falling back to the coalesced mailbox path only for cold starts, refreshes, or stale/expired recovery.
  - Zero-polling lifecycle rebinding across `ResourceClient`, `ActorClient`, and `SupervisorHandle` via `watch::Receiver<SlotState>`.
- **`StatefulActor` Ergonomics & Backpressure (`src/actor/stateful_actor.rs`):**
  - Optional `StatefulActor::handle_cast` hook and typed sub-message responses via `CallMessage<A>` + `ActorClient::ask()`.
  - `ActorContext<Self>` with `client()`, `supervisor()`, and `send_after(delay, msg)`.
  - Non-blocking `ActorClient::try_cast(msg)` (`ActorError::MailboxFull`), `cast_timeout()`, and `call_retry_timeout()`.
- **Remote gRPC Enhancements (`--features remote`, `proto/actor_node.proto`):**
  - Bounded `postcard` 1.1.3 wire serialization (4 MiB cap) with trailing-byte rejection, replacing `bincode`.
  - Protocol versioning (`protocol_version = 5`, `REMOTE_PROTOCOL_VERSION = 1`, `INCOMPATIBLE_PROTOCOL = 10`), server-side timeout caps (`DEFAULT_REMOTE_CALL_TIMEOUT = 30s`, `MAX_REMOTE_REQUEST_TIMEOUT = 300s`), sanitized tracing/errors, and `ActorNodeBuilder` (`server`, `authorizer`, `start_with_interceptor`) alongside `RemoteActorClient::connect_with` and `with_request_metadata`.
  - `CastRequest.timeout_ms` propagated by `RemoteActorClient::cast_timeout` and `AnyActorClient::cast_timeout`.
  - `RemoteError::Kind::MAX_RESTARTS_EXCEEDED = 9` (`RemoteActorError::MaxRestartsExceeded`), `RemoteNodeError`, and `ActorNodeHandle::start` accepting `Option<SupervisorHandle>` for Phase-2 subtree teardown.
- **Feature-Gated Chaos Engineering (`chaos_test`):**
  - `ResourceClient::simulate_crash()`, `ActorClient::simulate_crash()`, `SupervisorHandle::simulate_crash()`, and `SupervisorHandle::trigger_child_crash()` gated behind `#[cfg(any(test, feature = "chaos_test"))]` (enabled in `[dev-dependencies]`).

### Changed & Breaking Changes (from `0.2.x` to `0.3.0`)
- Marked core config, policy, and error enums/structs as `#[non_exhaustive]` (`SupervisionStrategy`, `RestartType`, `ShutdownPolicy`, `RestartPolicy`, `SupervisorError`, `ResourceActorConfig`, `StatefulActorConfig`, `ResourceError`, `ActorError`, `RemoteActorError`, `RemoteNodeError`).
- Renamed/unified symbols:
  - `RootSupervisor` / `ServiceSupervisor` (`*Handle`, `*Builder`) → `Supervisor` / `SupervisorHandle` / `SupervisorBuilder`
  - `ServiceSupervisor::spawn_and_register(name, &root)` → `Supervisor::spawn_child(name, &root)` / `Supervisor::builder(name).spawn(&root)`
  - `RootError` / `ServiceError` → `SupervisorError` (`ResourceError::Supervisor`, `ActorError::Supervisor`)
  - `service.get_client::<T, E>(name)` → `supervisor.get_resource_client::<T, E>(name)`
  - `root.resolve_service_handle(name)` → `supervisor.get_supervisor(name)`
  - `resolve_actor_channel*` / `resolve_service_channel` → `resolve_child_channel::<S>(name)` / `resolve_child_channel_untyped(name)`
  - `GenericResourceActor` / `ActorConfig` / `ActorBuilder` / `GenericResourceActorBuilder` → `ResourceActor` / `ResourceActorConfig` / `ResourceActorBuilder`
  - `ResourceActor::builder(name).spawn_and_register(&svc, &root)` → `ResourceActor::builder(name).spawn(&supervisor)`
  - `ActorContext` → `ActorContext<Self>`
  - `stop_service` / `restart_service` / `delete_service` → `stop_child` / `restart_child` / `delete_child`
  - `trigger_service_crash()` → `supervisor.simulate_crash()` / `parent.trigger_child_crash(name)` *(requires `chaos_test`)*
  - `ActorNodeHandle::start(addr, reg, Option<RootSupervisorHandle>)` → `ActorNodeHandle::start(addr, reg, Option<SupervisorHandle>)`

[Unreleased]: https://github.com/AlfredWeirich/otp-actor/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/AlfredWeirich/otp-actor/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/AlfredWeirich/otp-actor/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/AlfredWeirich/otp-actor/compare/v0.3.3...v0.4.0
[0.3.3]: https://github.com/AlfredWeirich/otp-actor/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/AlfredWeirich/otp-actor/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/AlfredWeirich/otp-actor/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/AlfredWeirich/otp-actor/releases/tag/v0.3.0

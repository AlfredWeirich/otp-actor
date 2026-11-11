# Release Notes — `otp-actor` v0.3.2

**Release Date:** 2026-11-11  
**Minimum Supported Rust Version (MSRV):** Rust 1.88 (`edition = "2024"`)

`otp-actor` **0.3.2** is a non-breaking patch release on top of `0.3.1`. It extends the runnable [`examples/microservice_pipeline.rs`](examples/microservice_pipeline.rs) blueprint with a deterministic `ctx.spawn_scoped` (`ActorScope`) background-loop showcase, links to it from the `spawn_scoped` documentation in [`README.md`](README.md), and bumps the crate version to `0.3.2` for crates.io publication.

---

## Highlights

- **Runnable `ctx.spawn_scoped` (`ActorScope`) Crash-Isolation Showcase:** Extended `IngestionWorker` in [`examples/microservice_pipeline.rs`](examples/microservice_pipeline.rs) with an instance-pinned `ctx.spawn_scoped` background loop emitting `IngestionMsg::ScopedProbe { from_instance }`, asserting `from_instance == ctx.instance_id()` on delivery and deterministically verifying via `watch` notifications that the scoped task cancels immediately on instance crash (Phase 4) and shutdown (Phase 5) without ever leaking onto a restarted replacement instance.
- **Documentation & Version Alignment (`0.3.2`):** Updated [`README.md`](README.md) dependency snippets, component descriptions, and the `Timers, streams & scoped background tasks` section to cross-reference the runnable `spawn_scoped` blueprint, and bumped [`Cargo.toml`](Cargo.toml) to `0.3.2`.

---

## What's New

### 1. Runnable `spawn_scoped` / `ActorScope` Showcase (`examples/microservice_pipeline.rs`)
- **`IngestionWorker::on_start`:** Now launches all three lifecycle-bound `ActorContext` primitives side by side—`ctx.send_interval_with`, `ctx.forward_receiver` (on `AttachFeed`), and `ctx.spawn_scoped`.
- **No Cross-Incarnation Leakage Proof:** The `spawn_scoped` task sends `IngestionMsg::ScopedProbe { from_instance: scope.instance_id() }` into the actor mailbox; `IngestionWorker::handle` asserts `from_instance == ctx.instance_id()`.
- **Deterministic Cancellation Verification:** Both Phase 4 (after `ingestion-worker` instance `#1` fails via `ctx.fail` and restarts as `#2`) and Phase 5 (`root.shutdown().await?`) await deterministic `tokio::sync::watch` exit counts confirming that each instance's `scope.cancelled()` branch terminated cleanly (`1` exit after crash, `2` total exits after shutdown).

### 2. Documentation & Package Metadata (`Cargo.toml`, `README.md`)
- Bumped package version in [`Cargo.toml`](Cargo.toml) (and dependency examples in [`README.md`](README.md)) from `0.3.1` to `0.3.2`.
- Added a direct cross-reference to [`examples/microservice_pipeline.rs`](examples/microservice_pipeline.rs) under *Timers, streams & scoped background tasks* in [`README.md`](README.md), and updated the component table and project layout descriptions.

---

## Upgrade & Compatibility

- **100% Backwards-Compatible with `0.3.1`:** No library code (`src/`), public APIs, or runtime semantics changed between `0.3.1` and `0.3.2`.
- **Upgrading Directly from `0.3.0` on crates.io:** Because `0.3.1` was tagged and released on GitHub without a separate crates.io publish, upgrading from `0.3.0` to `0.3.2` also includes everything introduced in `0.3.1` (`send_interval` / `send_interval_with`, `ActorScope` / `spawn_scoped` / `forward_receiver`, `SupervisorHandle::snapshot` / `snapshot_tree`, `watch_child_status`, `ActorClient::queue_stats`, `ResourceClient::cache_state`, `minimum` / `quickstart` / `rest_for_one_pipeline` examples, and `--all-features` docs.rs metadata). See [`Release_note_0.3.1.md`](Release_note_0.3.1.md) for the complete `0.3.1` changelog.

---

## Verification

All non-network workspace checks were executed and passed for `0.3.2`:
- `cargo test --lib --all-features`: **1 / 1 passed**
- `cargo test --test actor_supervision_test --all-features`: **86 / 86 passed**
- `cargo test --doc --all-features`: **4 / 4 passed**
- `cargo clippy --all-targets --all-features -- -D warnings`: **0 warnings**
- `cargo clippy --lib --no-default-features -- -D warnings`: **0 warnings**
- `cargo doc --no-deps --all-features`: **0 warnings**
- `rustfmt --edition 2024 --check examples/microservice_pipeline.rs`: **passed**
- `cargo check --examples --all-features` & runnable demos (`minimum`, `quickstart`, `rest_for_one_pipeline`, `microservice_pipeline` including 10x consecutive runs): **passed**
- `cargo package --offline --allow-dirty`: **41 files packaged and verified**
- `cargo test --test remote_grpc_test --features remote --no-run`: compiled cleanly (`src/remote/` and `tests/remote_grpc_test.rs` are unchanged since `0.3.0`; run `cargo test --all-features` locally with TCP loopback enabled prior to publishing)


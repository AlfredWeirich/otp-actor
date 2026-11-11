//! # Blueprint: Resilient Microservice Ingestion Pipeline
//!
//! Combines `otp-actor`'s core production building blocks in a realistic service topology:
//! - **`ResourceActor` (`db-auth-token`)**: 0-hop `watch` cache reads, proactive background refresh,
//!   and `stale_while_error` resilience during simulated upstream auth-provider outages, observed
//!   non-intrusively via [`CacheState`] (`auth_client.cache_state()`).
//! - **Bounded Mailbox Backpressure & Queue Metrics (`storage-sink`)**: `StatefulActor` with explicit
//!   `channel_capacity`, deterministic load shedding (`ActorError::MailboxFull` via `try_cast`), and
//!   [`QueueStats`] observation (`sink_client.queue_stats()`).
//! - **Ingestion Worker (`ingestion-worker`)**: `StatefulActor` using `ctx.forward_receiver(...)`
//!   for external channel ingestion, `ctx.send_interval_with(...)` for lifecycle-bound heartbeats
//!   on a non-`Clone` message enum, and automatic 0-ms `watch<SlotState>` rebinding after a crash.
//! - **Runtime Introspection (`pipeline`)**: Non-invasive [`ChildStatusWatcher`] observation
//!   (`watch_child_status`), bounded supervisor health snapshots (`pipeline.snapshot()`), and a
//!   whole-hierarchy view (`root.snapshot_tree()`), including flapping detection
//!   (`restarts_in_window`).
//!
//! Run with:
//! ```bash
//! cargo run --example microservice_pipeline
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use otp_actor::{
    ActorClient, ActorContext, ActorError, CacheState, ChildStatus, QueueStats, ResourceActor,
    ResourceClient, RestartPolicy, RestartType, StatefulActor, StatefulActorBuilder,
    SupervisionStrategy, Supervisor, TimedResource, WorkerContext,
};
use tokio::sync::{Notify, mpsc, oneshot};

// ============================================================================
// Stage 2: Bounded Storage Sink (`StatefulActor`)
// ============================================================================

#[derive(Default)]
struct StorageSink {
    persisted: Vec<String>,
}

enum SinkMsg {
    Write {
        record: String,
        auth_token: String,
    },
    /// Deterministic test/demo gate to pause the sink handler and exercise mailbox backpressure.
    PauseUntil(oneshot::Sender<()>, oneshot::Receiver<()>),
    Stats,
}

impl StatefulActor for StorageSink {
    type Message = SinkMsg;
    type Response = (usize, usize); // (persisted_count, instance_id)
    type Error = String;

    async fn handle(
        &mut self,
        msg: SinkMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<Self::Response, String> {
        match msg {
            SinkMsg::Write { record, auth_token } => {
                self.persisted.push(format!("{record} [auth={auth_token}]"));
                Ok((self.persisted.len(), ctx.instance_id()))
            }
            SinkMsg::PauseUntil(entered_tx, release_rx) => {
                let _ = entered_tx.send(());
                let _ = release_rx.await;
                Ok((self.persisted.len(), ctx.instance_id()))
            }
            SinkMsg::Stats => Ok((self.persisted.len(), ctx.instance_id())),
        }
    }

    async fn on_stop(&mut self, ctx: &mut ActorContext<Self>) {
        println!(
            "  [storage-sink #{}] on_stop: flushed {} total records",
            ctx.instance_id(),
            self.persisted.len()
        );
    }
}

// ============================================================================
// Stage 1: Ingestion & Validation Worker (`StatefulActor`)
// ============================================================================

#[derive(Clone)]
struct IngestionEvent {
    id: u64,
    payload: String,
    valid: bool,
}

enum IngestionMsg {
    Ingest(IngestionEvent),
    AttachFeed(mpsc::Receiver<IngestionEvent>),
    Heartbeat,
    Status,
    SimulateBug,
}

struct IngestionWorker {
    auth_client: ResourceClient<String, String>,
    sink_client: ActorClient<StorageSink>,
    processed_notify: Arc<Notify>,
    accepted: u64,
    rejected: u64,
    shed: u64,
    heartbeats: u64,
}

impl IngestionWorker {
    fn new(
        auth_client: ResourceClient<String, String>,
        sink_client: ActorClient<StorageSink>,
        processed_notify: Arc<Notify>,
    ) -> Self {
        Self {
            auth_client,
            sink_client,
            processed_notify,
            accepted: 0,
            rejected: 0,
            shed: 0,
            heartbeats: 0,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct IngestionStats {
    instance_id: usize,
    accepted: u64,
    rejected: u64,
    shed: u64,
    heartbeats: u64,
}

enum IngestionReply {
    Forwarded,
    DroppedInvalid,
    BackpressureShed,
    Ack,
    Stats(IngestionStats),
}

impl StatefulActor for IngestionWorker {
    type Message = IngestionMsg;
    type Response = IngestionReply;
    type Error = String;

    async fn on_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), String> {
        // Instance-bound periodic heartbeat (`send_interval_with` works without `Clone` on `IngestionMsg`).
        // Cancels automatically if this actor instance crashes or stops.
        ctx.send_interval_with(Duration::from_millis(80), || IngestionMsg::Heartbeat);
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: IngestionMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<IngestionReply, String> {
        match msg {
            IngestionMsg::Ingest(ev) => {
                if !ev.valid {
                    self.rejected += 1;
                    self.processed_notify.notify_waiters();
                    return Ok(IngestionReply::DroppedInvalid);
                }

                // 0-hop watch read when fresh; seamless stale-while-error during transient upstream outage.
                let token = self
                    .auth_client
                    .get()
                    .await
                    .map_err(|e| format!("auth unavailable: {e}"))?;

                let record = format!("event#{}:{}", ev.id, ev.payload);
                match self.sink_client.try_cast(SinkMsg::Write {
                    record,
                    auth_token: token,
                }) {
                    Ok(()) => {
                        self.accepted += 1;
                        self.processed_notify.notify_waiters();
                        Ok(IngestionReply::Forwarded)
                    }
                    Err(ActorError::MailboxFull) => {
                        self.shed += 1;
                        self.processed_notify.notify_waiters();
                        Ok(IngestionReply::BackpressureShed)
                    }
                    Err(e) => Err(format!("sink error: {e}")),
                }
            }
            IngestionMsg::AttachFeed(rx) => {
                // Pulls items from external mpsc receiver before reserving mailbox permits (no starvation).
                ctx.forward_receiver(rx, IngestionMsg::Ingest);
                Ok(IngestionReply::Ack)
            }
            IngestionMsg::Heartbeat => {
                self.heartbeats += 1;
                Ok(IngestionReply::Ack)
            }
            IngestionMsg::Status => Ok(IngestionReply::Stats(IngestionStats {
                instance_id: ctx.instance_id(),
                accepted: self.accepted,
                rejected: self.rejected,
                shed: self.shed,
                heartbeats: self.heartbeats,
            })),
            IngestionMsg::SimulateBug => {
                ctx.fail("simulated parser invariant violation");
                Err("ingestion worker failed".to_string())
            }
        }
    }

    async fn on_stop(&mut self, ctx: &mut ActorContext<Self>) {
        println!(
            "  [ingestion-worker #{}] on_stop: accepted={}, rejected={}, shed={}, heartbeats={}",
            ctx.instance_id(),
            self.accepted,
            self.rejected,
            self.shed,
            self.heartbeats
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Microservice Ingestion Pipeline Blueprint ===\n");

    let root = Supervisor::new_root();
    let pipeline = Supervisor::builder("ingestion-pipeline")
        .strategy(SupervisionStrategy::OneForOne)
        .spawn(&root)
        .await?;

    // 1. ResourceActor for short-lived DB/API auth tokens with stale-while-error resilience
    let upstream_healthy = Arc::new(AtomicBool::new(true));
    let upstream_flag = upstream_healthy.clone();

    let auth_client = ResourceActor::builder("db-auth-token")
        .worker(move |ctx: WorkerContext| {
            let healthy = upstream_flag.clone();
            async move {
                if !healthy.load(Ordering::SeqCst) {
                    return Err("upstream identity provider 503".to_string());
                }
                Ok::<_, String>(TimedResource::with_ttl(
                    format!("tok-gen-{}", ctx.generation),
                    Duration::from_millis(140),
                ))
            }
        })
        .eager_prewarm(true)
        .expiry_buffer(Duration::from_millis(40))
        .failure_cooldown(Duration::from_millis(40))
        .stale_while_error(Duration::from_secs(2))
        .spawn(&pipeline)
        .await?;

    // Wait briefly so eager_prewarm populates the 0-hop watch snapshot before traffic arrives.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 2. StorageSink with a small bounded mailbox (capacity = 2) to demonstrate explicit backpressure
    let sink_client = StatefulActorBuilder::from_default::<StorageSink>("storage-sink")
        .channel_capacity(2)
        .restart_type(RestartType::Permanent)
        .spawn(&pipeline)
        .await?;

    // 3. IngestionWorker wired with both ResourceClient and ActorClient<StorageSink>
    let auth_for_worker = auth_client.clone();
    let sink_for_worker = sink_client.clone();
    let processed_notify = Arc::new(Notify::new());
    let notify_for_worker = processed_notify.clone();
    let fast_restart =
        RestartPolicy::new(5, Duration::from_secs(10)).initial_backoff(Duration::from_millis(20));

    let ingestion_client = StatefulActorBuilder::new("ingestion-worker", move |_| {
        IngestionWorker::new(
            auth_for_worker.clone(),
            sink_for_worker.clone(),
            notify_for_worker.clone(),
        )
    })
    .restart_type(RestartType::Permanent)
    .restart_policy(fast_restart)
    .spawn(&pipeline)
    .await?;

    // ------------------------------------------------------------------------
    // Phase 1: Normal ingestion, 0-hop auth reads & forward_receiver stream feed
    // ------------------------------------------------------------------------
    println!("--- Phase 1: Filtering, External Stream Forwarding & 0-hop Auth Token Reads ---");
    for (id, valid) in [(1, true), (2, false), (3, true)] {
        let reply = ingestion_client
            .call(IngestionMsg::Ingest(IngestionEvent {
                id,
                payload: format!("order-{id}"),
                valid,
            }))
            .await?;
        match reply {
            IngestionReply::Forwarded => println!("  Event #{id}: forwarded to sink"),
            IngestionReply::DroppedInvalid => println!("  Event #{id}: filtered out (invalid)"),
            IngestionReply::BackpressureShed | IngestionReply::Ack | IngestionReply::Stats(_) => {}
        }
    }

    // Attach an external mpsc event feed via `ctx.forward_receiver(...)`
    let (feed_tx, feed_rx) = mpsc::channel(4);
    ingestion_client
        .call(IngestionMsg::AttachFeed(feed_rx))
        .await?;
    for id in [4, 5] {
        let processed = processed_notify.notified();
        feed_tx
            .send(IngestionEvent {
                id,
                payload: format!("stream-{id}"),
                valid: true,
            })
            .await?;
        processed.await;
        println!("  Stream event #{id}: forwarded via ctx.forward_receiver");
    }
    drop(feed_tx);

    let (persisted, _) = sink_client.call(SinkMsg::Stats).await?;
    println!("  Sink persisted count: {persisted}\n");
    assert_eq!(persisted, 4);

    // ------------------------------------------------------------------------
    // Phase 2: Bounded mailbox backpressure + queue_stats() observation
    // ------------------------------------------------------------------------
    println!("--- Phase 2: Bounded Mailbox Backpressure (try_cast & queue_stats) ---");
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    sink_client
        .cast(SinkMsg::PauseUntil(entered_tx, release_rx))
        .await?;
    entered_rx.await?; // Sink is now deterministically busy inside handle()

    let mut shed_count = 0;
    for id in 10..15 {
        match ingestion_client
            .call(IngestionMsg::Ingest(IngestionEvent {
                id,
                payload: format!("burst-{id}"),
                valid: true,
            }))
            .await?
        {
            IngestionReply::Forwarded => {
                println!("  Burst event #{id}: queued in bounded sink mailbox")
            }
            IngestionReply::BackpressureShed => {
                shed_count += 1;
                println!("  Burst event #{id}: MailboxFull -> shed cleanly via backpressure!");
            }
            IngestionReply::DroppedInvalid | IngestionReply::Ack | IngestionReply::Stats(_) => {}
        }
    }
    assert!(
        shed_count >= 1,
        "bounded mailbox must shed excess burst load"
    );

    let busy_stats: QueueStats = sink_client.queue_stats();
    println!(
        "  Sink queue_stats under backpressure: queued={}/{}, available={}, is_full={}",
        busy_stats.queued,
        busy_stats.max_capacity,
        busy_stats.available,
        busy_stats.is_full()
    );
    assert!(busy_stats.is_full());
    assert_eq!(busy_stats.queued, 2);

    // Release sink gate and confirm queued messages drain cleanly
    let _ = release_tx.send(());
    let (persisted_after_burst, _) = sink_client.call(SinkMsg::Stats).await?;
    let drained_stats = sink_client.queue_stats();
    println!(
        "  Sink drained queued burst -> persisted={persisted_after_burst} (queued={}/{})",
        drained_stats.queued, drained_stats.max_capacity
    );
    assert_eq!(persisted_after_burst, 6);
    assert_eq!(drained_stats.queued, 0);

    if let IngestionReply::Stats(before_crash) = ingestion_client.call(IngestionMsg::Status).await?
    {
        println!(
            "  Ingestion totals after burst (instance #{}): accepted={}, rejected={}, shed={}\n",
            before_crash.instance_id,
            before_crash.accepted,
            before_crash.rejected,
            before_crash.shed
        );
        assert_eq!(
            (
                before_crash.accepted,
                before_crash.rejected,
                before_crash.shed
            ),
            (6, 1, 3)
        );
    }

    // ------------------------------------------------------------------------
    // Phase 3: Stale-while-error during transient upstream outage
    // ------------------------------------------------------------------------
    println!("--- Phase 3: Upstream Auth Outage + Stale-While-Error ---");
    println!(
        "  Auth cache before outage (no mailbox hop): {:?}",
        auth_client.cache_state()
    );
    upstream_healthy.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(160)).await;

    // The cached token has expired, but is still inside the 2s stale-while-error window.
    // `cache_state()` reports this without contacting the actor and never exposes the token value.
    let outage_state = auth_client.cache_state();
    println!("  Auth cache during outage: {outage_state:?}");
    assert!(matches!(outage_state, CacheState::StaleUsable { .. }));

    let stale_token = auth_client.get().await?;
    println!("  Upstream is DOWN, yet ResourceActor served stale token: {stale_token}");

    upstream_healthy.store(true, Ordering::SeqCst);
    auth_client.force_refresh().await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let recovered_token = auth_client.get().await?;
    println!("  Upstream is UP again -> fresh token acquired: {recovered_token}");
    println!(
        "  Auth cache after recovery: {:?}\n",
        auth_client.cache_state()
    );
    assert!(auth_client.cache_state().has_usable_value());
    assert_ne!(stale_token, recovered_token);

    // ------------------------------------------------------------------------
    // Phase 4: Worker crash, ChildStatusWatcher & SupervisorSnapshot
    // ------------------------------------------------------------------------
    println!("--- Phase 4: Worker Crash, Status Watcher & Supervisor Snapshot ---");
    if let IngestionReply::Stats(pre_crash) = ingestion_client.call(IngestionMsg::Status).await? {
        println!(
            "  Worker #{} state before crash: {pre_crash:?}",
            pre_crash.instance_id
        );
    }
    let mut worker_status = pipeline.watch_child_status("ingestion-worker").await?;
    let _ = ingestion_client.call(IngestionMsg::SimulateBug).await;

    // Wait non-invasively for replacement instance #2 via existing watch slot:
    let new_status = worker_status
        .wait_for(|s| matches!(s, ChildStatus::Running { instance_id: 2 }))
        .await?;
    println!("  ChildStatusWatcher observed recovery: {new_status:?}");

    // Immediately reuse `ingestion_client` with zero reconnect logic:
    match ingestion_client.call(IngestionMsg::Status).await? {
        IngestionReply::Stats(stats) => {
            println!("  Rebound transparently to fresh worker: {stats:?}");
            assert_eq!(stats.instance_id, 2);
        }
        _ => unreachable!(),
    }

    // Inspect ordered children, statuses, and flapping metrics (`restarts_in_window`) via bounded snapshot:
    let snap = pipeline.snapshot().await?;
    println!(
        "  Supervisor '{}' snapshot: all_running={}, any_failed={}",
        snap.name,
        snap.all_running(),
        snap.any_failed()
    );
    for child in &snap.children {
        println!(
            "    - {}: status={:?}, instance_id={}, restart_type={:?}, restarts_in_window={}",
            child.name,
            child.status,
            child.instance_id,
            child.restart_type,
            child.restarts_in_window
        );
    }
    assert!(snap.all_running());
    assert!(!snap.any_failed());
    assert_eq!(
        snap.child("ingestion-worker").map(|c| c.restarts_in_window),
        Some(1)
    );

    // Whole-hierarchy view from the root: descends into every child supervisor under one shared
    // deadline, and degrades per subtree instead of failing the whole probe.
    let tree = root.snapshot_tree().await?;
    println!(
        "  Tree from '{}': all_running_deep={}, any_failed_deep={}, unavailable_subtrees={}",
        tree.snapshot.name,
        tree.all_running_deep(),
        tree.any_failed_deep(),
        tree.unavailable_count()
    );
    if let Some(pipeline_tree) = tree.find(&["ingestion-pipeline"]) {
        println!(
            "    - subtree '{}': {} children running",
            pipeline_tree.snapshot.name,
            pipeline_tree.snapshot.running_count()
        );
    }
    assert!(tree.all_running_deep());
    assert_eq!(
        tree.find(&["ingestion-pipeline"])
            .map(|t| t.snapshot.children.len()),
        Some(3)
    );
    println!();

    // ------------------------------------------------------------------------
    // Phase 5: Coordinated reverse-order graceful shutdown
    // ------------------------------------------------------------------------
    println!("--- Phase 5: Graceful Shutdown ---");
    root.shutdown().await?;
    println!("Pipeline shutdown complete.");
    Ok(())
}

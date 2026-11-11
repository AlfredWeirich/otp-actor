//! # Blueprint: Resilient Microservice Ingestion Pipeline
//!
//! Combines `otp-actor`'s core production building blocks in a realistic service topology:
//! - **`ResourceActor` (`db-auth-token`)**: 0-hop `watch` cache reads, proactive background refresh,
//!   and `stale_while_error` resilience during simulated upstream auth-provider outages.
//! - **Bounded Mailbox Backpressure (`storage-sink`)**: `StatefulActor` with explicit `channel_capacity`
//!   demonstrating deterministic load shedding (`ActorError::MailboxFull` via `try_cast`).
//! - **Ingestion Worker (`ingestion-worker`)**: `StatefulActor` with `handle_cast` and `handle`,
//!   reading auth tokens in 0 hops and forwarding valid events downstream, plus automatic
//!   0-ms `watch<SlotState>` rebinding after a worker crash.
//!
//! Run with:
//! ```bash
//! cargo run --example microservice_pipeline
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use otp_actor::{
    ActorClient, ActorContext, ActorError, ResourceActor, ResourceClient, RestartPolicy,
    RestartType, StatefulActor, StatefulActorBuilder, SupervisionStrategy, Supervisor,
    TimedResource, WorkerContext,
};
use tokio::sync::oneshot;

// ============================================================================
// Stage 2: Bounded Storage Sink (`StatefulActor`)
// ============================================================================

#[derive(Default)]
struct StorageSink {
    persisted: Vec<String>,
}

enum SinkMsg {
    Write { record: String, auth_token: String },
    /// Deterministic test/demo gate to pause the sink handler and exercise mailbox backpressure.
    PauseUntil(oneshot::Sender<()>, oneshot::Receiver<()>),
    Stats,
}

impl StatefulActor for StorageSink {
    type Message = SinkMsg;
    type Response = (usize, usize); // (persisted_count, instance_id)
    type Error = String;

    async fn handle(&mut self, msg: SinkMsg, ctx: &mut ActorContext<Self>) -> Result<Self::Response, String> {
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
    Status,
    SimulateBug,
}

struct IngestionWorker {
    auth_client: ResourceClient<String, String>,
    sink_client: ActorClient<StorageSink>,
    accepted: u64,
    rejected: u64,
    shed: u64,
}

impl IngestionWorker {
    fn new(auth_client: ResourceClient<String, String>, sink_client: ActorClient<StorageSink>) -> Self {
        Self {
            auth_client,
            sink_client,
            accepted: 0,
            rejected: 0,
            shed: 0,
        }
    }
}

#[derive(Debug)]
struct IngestionStats {
    instance_id: usize,
    accepted: u64,
    rejected: u64,
    shed: u64,
}

enum IngestionReply {
    Forwarded,
    DroppedInvalid,
    BackpressureShed,
    Stats(IngestionStats),
}

impl StatefulActor for IngestionWorker {
    type Message = IngestionMsg;
    type Response = IngestionReply;
    type Error = String;

    async fn handle(
        &mut self,
        msg: IngestionMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<IngestionReply, String> {
        match msg {
            IngestionMsg::Ingest(ev) => {
                if !ev.valid {
                    self.rejected += 1;
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
                        Ok(IngestionReply::Forwarded)
                    }
                    Err(ActorError::MailboxFull) => {
                        self.shed += 1;
                        Ok(IngestionReply::BackpressureShed)
                    }
                    Err(e) => Err(format!("sink error: {e}")),
                }
            }
            IngestionMsg::Status => Ok(IngestionReply::Stats(IngestionStats {
                instance_id: ctx.instance_id(),
                accepted: self.accepted,
                rejected: self.rejected,
                shed: self.shed,
            })),
            IngestionMsg::SimulateBug => {
                ctx.fail("simulated parser invariant violation");
                Err("ingestion worker failed".to_string())
            }
        }
    }

    async fn on_stop(&mut self, ctx: &mut ActorContext<Self>) {
        println!(
            "  [ingestion-worker #{}] on_stop: accepted={}, rejected={}, shed={}",
            ctx.instance_id(),
            self.accepted,
            self.rejected,
            self.shed
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
    let fast_restart = RestartPolicy::new(5, Duration::from_secs(10))
        .initial_backoff(Duration::from_millis(20));

    let ingestion_client = StatefulActorBuilder::new("ingestion-worker", move |_| {
        IngestionWorker::new(auth_for_worker.clone(), sink_for_worker.clone())
    })
    .restart_type(RestartType::Permanent)
    .restart_policy(fast_restart)
    .spawn(&pipeline)
    .await?;

    // ------------------------------------------------------------------------
    // Phase 1: Normal ingestion with 0-hop auth token reads
    // ------------------------------------------------------------------------
    println!("--- Phase 1: Filtering & 0-hop Auth Token Reads ---");
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
            IngestionReply::BackpressureShed | IngestionReply::Stats(_) => {}
        }
    }
    let (persisted, _) = sink_client.call(SinkMsg::Stats).await?;
    println!("  Sink persisted count: {persisted}\n");
    assert_eq!(persisted, 2);

    // ------------------------------------------------------------------------
    // Phase 2: Bounded mailbox backpressure (no OOM under burst load)
    // ------------------------------------------------------------------------
    println!("--- Phase 2: Bounded Mailbox Backpressure (try_cast -> MailboxFull) ---");
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
            IngestionReply::Forwarded => println!("  Burst event #{id}: queued in bounded sink mailbox"),
            IngestionReply::BackpressureShed => {
                shed_count += 1;
                println!("  Burst event #{id}: MailboxFull -> shed cleanly via backpressure!");
            }
            IngestionReply::DroppedInvalid | IngestionReply::Stats(_) => {}
        }
    }
    assert!(shed_count >= 1, "bounded mailbox must shed excess burst load");

    // Release sink gate and confirm queued messages drain cleanly
    let _ = release_tx.send(());
    let (persisted_after_burst, _) = sink_client.call(SinkMsg::Stats).await?;
    println!("  Sink drained queued burst -> persisted count: {persisted_after_burst}");
    if let IngestionReply::Stats(before_crash) = ingestion_client.call(IngestionMsg::Status).await? {
        println!(
            "  Ingestion totals before crash (instance #{}): accepted={}, rejected={}, shed={}\n",
            before_crash.instance_id, before_crash.accepted, before_crash.rejected, before_crash.shed
        );
        assert_eq!((before_crash.accepted, before_crash.rejected, before_crash.shed), (4, 1, 3));
    }

    // ------------------------------------------------------------------------
    // Phase 3: Stale-while-error during transient upstream outage
    // ------------------------------------------------------------------------
    println!("--- Phase 3: Upstream Auth Outage + Stale-While-Error ---");
    upstream_healthy.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(160)).await;

    let stale_token = auth_client.get().await?;
    println!("  Upstream is DOWN, yet ResourceActor served stale token: {stale_token}");

    upstream_healthy.store(true, Ordering::SeqCst);
    auth_client.force_refresh().await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let recovered_token = auth_client.get().await?;
    println!("  Upstream is UP again -> fresh token acquired: {recovered_token}\n");
    assert_ne!(stale_token, recovered_token);

    // ------------------------------------------------------------------------
    // Phase 4: Worker crash -> 0-ms reactive rebind on the same client handle
    // ------------------------------------------------------------------------
    println!("--- Phase 4: Worker Crash & Transparent Rebinding ---");
    let _ = ingestion_client.call(IngestionMsg::SimulateBug).await;

    // Immediately reuse `ingestion_client` with zero reconnect logic:
    match ingestion_client.call(IngestionMsg::Status).await? {
        IngestionReply::Stats(stats) => {
            println!("  Rebound transparently to fresh worker: {stats:?}\n");
            assert_eq!(stats.instance_id, 2);
        }
        _ => unreachable!(),
    }

    // ------------------------------------------------------------------------
    // Phase 5: Coordinated reverse-order graceful shutdown
    // ------------------------------------------------------------------------
    println!("--- Phase 5: Graceful Shutdown ---");
    root.shutdown().await?;
    println!("Pipeline shutdown complete.");
    Ok(())
}

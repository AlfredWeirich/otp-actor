//! # Local Operational Telemetry & Prometheus Exposition
//!
//! Demonstrates non-intrusive operational monitoring for an in-process `otp-actor` supervision
//! tree without any HTTP server or JSON dependencies:
//! - [`LocalNodeMonitor::health`]: synchronous zero-hop liveness check with host-application
//!   `started_at` uptime override.
//! - [`LocalNodeMonitor::health_checked`]: bounded tree readiness probe (`Healthy` -> `Degraded`
//!   during crash-loop -> `Healthy` after `restart_child`).
//! - [`LocalNodeMonitor::status`]: aggregated [`NodeSummary`], per-child lifecycle & restart
//!   counters, bounded mailbox [`QueueStats`], lock-free [`ActorMetricsSnapshot`], and
//!   [`ResourceActor`] [`CacheState`].
//! - [`format_prometheus_metrics`]: OpenMetrics / Prometheus text exposition format.
//!
//! ## Run this example:
//! ```bash
//! cargo run --example telemetry
//! ```

use std::time::{Duration, Instant};

use otp_actor::{
    ActorContext, ActorError, ChildStatus, HealthState, LocalNodeMonitor, ResourceActor,
    RestartPolicy, RestartType, StatefulActor, StatefulActorBuilder, StatusOptions, Supervisor,
    TimedResource, WorkerContext, format_prometheus_metrics,
};

#[derive(Clone, Default)]
struct LedgerWorker {
    processed: u64,
}

enum LedgerMsg {
    Record(u64),
    FailDomain,
    PanicNow,
}

impl StatefulActor for LedgerWorker {
    type Message = LedgerMsg;
    type Response = u64;
    type Error = &'static str;

    async fn handle(
        &mut self,
        msg: Self::Message,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            LedgerMsg::Record(amount) => {
                self.processed += amount;
                Ok(self.processed)
            }
            LedgerMsg::FailDomain => Err("validation rejected"),
            LedgerMsg::PanicNow => panic!("simulated worker panic"),
        }
    }

    async fn handle_cast(
        &mut self,
        msg: Self::Message,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Self::Error> {
        if let LedgerMsg::Record(amount) = msg {
            self.processed += amount;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Host application tracks its own process start timestamp and passes it to LocalNodeMonitor.
    let app_started_at = Instant::now();

    let root = Supervisor::new("telemetry-root");
    let payments = Supervisor::builder("payments").spawn(&root).await?;

    let worker = StatefulActorBuilder::from_default::<LedgerWorker>("ledger-worker")
        .channel_capacity(32)
        .restart_type(RestartType::Permanent)
        .restart_policy(
            RestartPolicy::new(1, Duration::from_secs(10))
                .initial_backoff(Duration::from_millis(5))
                .max_backoff(Duration::from_millis(5)),
        )
        .spawn(&payments)
        .await?;

    let signing_key = ResourceActor::builder("signing-key")
        .worker(|ctx: WorkerContext| async move {
            Ok::<_, String>(TimedResource::with_ttl(
                format!("key-gen-{}", ctx.generation),
                Duration::from_secs(60),
            ))
        })
        .eager_prewarm(true)
        .spawn(&payments)
        .await?;
    let _ = signing_key.get().await?;

    let monitor = LocalNodeMonitor::new(root.clone()).with_started_at(app_started_at);

    // 1. Fast zero-hop liveness check
    let health = monitor.health();
    println!(
        "[1] Fast health: alive={}, state={}, uptime={}s",
        health.alive,
        health.state.as_str(),
        health.uptime_seconds
    );
    assert!(health.alive);
    assert_eq!(health.state, HealthState::Healthy);

    // 2. Drive normal calls, casts, and a domain error
    assert_eq!(worker.call(LedgerMsg::Record(100)).await?, 100);
    worker.cast(LedgerMsg::Record(50)).await?;
    assert!(matches!(
        worker.call(LedgerMsg::FailDomain).await,
        Err(ActorError::HandlerFailed("validation rejected"))
    ));
    assert_eq!(worker.call(LedgerMsg::Record(25)).await?, 175);

    // 3. Trigger a crash-loop (budget = 1 restart) -> health_checked reports Degraded
    let mut watcher = payments.watch_child_status("ledger-worker").await?;
    let _ = worker.call(LedgerMsg::PanicNow).await;
    watcher
        .wait_for(|s| matches!(s, ChildStatus::Running { instance_id: 2 }))
        .await?;
    let _ = worker.call(LedgerMsg::PanicNow).await;
    watcher.wait_for(|s| s.is_failed()).await?;

    let degraded = monitor.health_checked(Duration::from_secs(1)).await;
    println!(
        "[2] After crash-loop: alive={}, state={}",
        degraded.alive,
        degraded.state.as_str()
    );
    assert_eq!(degraded.state, HealthState::Degraded);

    // 4. Recover child manually -> returns to Healthy
    payments.restart_child("ledger-worker").await?;
    assert_eq!(worker.call(LedgerMsg::Record(10)).await?, 10);

    let recovered = monitor.health_checked(Duration::from_secs(1)).await;
    println!(
        "[3] After manual recovery: alive={}, state={}",
        recovered.alive,
        recovered.state.as_str()
    );
    assert_eq!(recovered.state, HealthState::Healthy);

    // 5. Inspect structured NodeStatus and Prometheus exposition output
    let status = monitor.status_default().await?;
    println!(
        "[4] Summary: supervisors={}, actors={}, calls={}, casts={}, errors={}, panics={}, restarts={}",
        status.summary.total_supervisors,
        status.summary.total_actors,
        status.summary.total_calls,
        status.summary.total_casts,
        status.summary.total_handler_errors,
        status.summary.total_panics,
        status.summary.total_restarts
    );

    let prom_text = format_prometheus_metrics(&status);
    println!("\n[5] Prometheus exposition:\n{prom_text}");
    assert!(prom_text.contains("otp_actor_calls_total{supervisor=\"telemetry-root/payments\",child=\"ledger-worker\"} 4\n"));
    assert!(prom_text.contains("otp_actor_resource_cache_state{supervisor=\"telemetry-root/payments\",child=\"signing-key\"} 1\n"));

    // 6. Compact status omits the full tree while preserving NodeSummary
    let compact = monitor
        .status(StatusOptions::new().include_tree(false))
        .await?;
    assert!(compact.supervision_tree.is_none());
    assert_eq!(compact.summary, status.summary);

    // 7. Graceful shutdown transitions local health to Unavailable
    root.shutdown().await?;
    let after_shutdown = monitor.health();
    println!(
        "[6] After shutdown: alive={}, state={}",
        after_shutdown.alive,
        after_shutdown.state.as_str()
    );
    assert!(!after_shutdown.alive);
    assert_eq!(after_shutdown.state, HealthState::Unavailable);

    Ok(())
}

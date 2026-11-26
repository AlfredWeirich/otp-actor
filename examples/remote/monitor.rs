//! # Remote Operational Telemetry & Location-Transparent Monitoring
//!
//! Demonstrates the `ActorNodeAdmin` gRPC control plane (`Health` & `Status` RPCs) alongside
//! [`AnyNodeMonitor`] for uniform local and remote operational monitoring:
//! - Host node configured with [`ActorNodeHandle::builder`], `.started_at(...)`, and a Bearer
//!   token [`Authorizer`] protecting both `ActorNode` and `ActorNodeAdmin`.
//! - Live `Call`, `Cast`, and `ActorNotFound` traffic driving remote gRPC counters.
//! - Identical `health()`, `status_default()`, and `prometheus_metrics()` calls across
//!   [`AnyNodeMonitor::Local`] and [`AnyNodeMonitor::Remote`].
//!
//! ## Run this example:
//! ```bash
//! cargo run --example remote_monitor --features remote
//! ```

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::{AccountMsg, BankAccountActor};
use otp_actor::actor::{HealthState, RestartType, StatefulActorBuilder, StatusOptions, Supervisor};
use otp_actor::remote::{
    ActorNodeHandle, ActorNodeRegistry, AnyNodeMonitor, RemoteActorClient, RemoteActorError,
    RemoteNodeMonitor,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,otp_actor=info".into()),
        )
        .init();

    let app_started_at = Instant::now();

    // 1. Start supervision tree & actor on the server node
    let root = Supervisor::new("node-root");
    let service = Supervisor::builder("banking-service").spawn(&root).await?;

    let initial_account = BankAccountActor {
        account_id: "ACC-42".into(),
        balance: 500,
    };
    let local_client = StatefulActorBuilder::from_instance("account-42", initial_account)
        .channel_capacity(16)
        .restart_type(RestartType::Permanent)
        .spawn(&service)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(local_client);

    // 2. Start gRPC node (hosts both ActorNode and ActorNodeAdmin on the same port)
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_node = ActorNodeHandle::builder(bind_addr, registry)
        .supervisor(root.clone())
        .started_at(app_started_at)
        .bearer_token("demo-token")
        .start()
        .await?;
    let endpoint_url = format!("http://{}", server_node.local_addr());
    println!("[Node] Serving ActorNode + ActorNodeAdmin on {endpoint_url}");

    // 3. Drive some remote Call, Cast, and ActorNotFound traffic
    let remote_actor =
        RemoteActorClient::<BankAccountActor>::connect(&endpoint_url, "banking-service", "account-42")
            .await?
            .with_bearer_token("demo-token")?;
    assert_eq!(remote_actor.call(AccountMsg::Deposit(250)).await?, 750);
    remote_actor.cast(AccountMsg::Deposit(50)).await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(remote_actor.call(AccountMsg::GetBalance).await?, 800);

    let missing_actor =
        RemoteActorClient::<BankAccountActor>::connect(&endpoint_url, "banking-service", "unknown")
            .await?
            .with_bearer_token("demo-token")?;
    assert!(matches!(
        missing_actor.call(AccountMsg::GetBalance).await,
        Err(RemoteActorError::ActorNotFound(_))
    ));

    // 4. Query telemetry transparently via AnyNodeMonitor (Local vs Remote)
    let local_monitor = AnyNodeMonitor::local(root);
    let remote_monitor = AnyNodeMonitor::from_remote(
        RemoteNodeMonitor::connect(&endpoint_url)
            .await?
            .with_bearer_token("demo-token")?,
    );

    let local_health = local_monitor.health().await?;
    let remote_health = remote_monitor.health().await?;
    println!(
        "[Monitor] Local health={:?}, Remote health={:?} (uptime={}s)",
        local_health.state, remote_health.state, remote_health.uptime_seconds
    );
    assert_eq!(remote_health.state, HealthState::Healthy);

    let status = remote_monitor.status_default().await?;
    let remote_counters = status.remote.expect("remote counters present");
    println!(
        "[Monitor] Remote status: actors={}, actor_calls={}, grpc_calls={}, grpc_casts={}, actor_not_found={}",
        status.summary.total_actors,
        status.summary.total_calls,
        remote_counters.grpc_calls_total,
        remote_counters.grpc_casts_total,
        remote_counters.actor_not_found_total
    );

    let prom = remote_monitor
        .prometheus_metrics(StatusOptions::default())
        .await?;
    println!("\n[Monitor] Remote Prometheus exposition:\n{prom}");
    assert!(prom.contains("otp_actor_remote_grpc_calls_total 3\n"));
    assert!(prom.contains("otp_actor_remote_not_found_total{kind=\"actor\"} 1\n"));

    server_node.shutdown().await?;
    Ok(())
}

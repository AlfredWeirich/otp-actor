//! # Remote Actor Node (Server)
//!
//! Hosts the supervised `BankAccountActor` inside an OTP supervision tree and serves
//! it over high-performance gRPC (HTTP/2).
//!
//! ## How to Run:
//! ```bash
//! # Start server (Terminal 1)
//! cargo run --example remote_server --features remote
//! ```
//!
//! Optional environment variables:
//! - `REMOTE_ADDR`: Override bind address (default: `127.0.0.1:50051`). Set to `0.0.0.0:50051` to allow remote LAN access.

mod common;

use common::BankAccountActor;
use otp_actor::actor::{RestartType, RootSupervisor, ServiceSupervisor, StatefulActorBuilder};
use otp_actor::remote::{ActorNodeHandle, ActorNodeRegistry};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,otp_actor=debug".into()),
        )
        .init();

    println!("============================================================");
    println!("  [Rechner B / Server Node] OTP Actor Host via gRPC");
    println!("============================================================\n");

    // 1. Initialize Supervision Tree
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("banking-service", &root).await?;

    // 2. Start initial actor instance under OTP supervision
    let initial_account = BankAccountActor {
        account_id: "ACC-9876".into(),
        balance: 1000,
    };

    // Use RestartType::Transient:
    // - Recovers and re-spawns on unexpected crash (SimulateCrash -> ctx.fail)
    // - Stays gracefully terminated when closed normally by client (CloseAccount -> ctx.stop)
    let local_client = StatefulActorBuilder::from_instance("account-9876", initial_account)
        .restart_type(RestartType::Transient)
        .spawn(&service)
        .await?;

    // 3. Register with gRPC Node Registry
    let registry = ActorNodeRegistry::new();
    registry.register_client(local_client);

    // 4. Bind address (configurable via REMOTE_ADDR)
    let bind_str = std::env::var("REMOTE_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".into());
    let bind_addr: SocketAddr = bind_str.parse()?;

    let node = ActorNodeHandle::start(bind_addr, registry, Some(root)).await?;
    let local_addr = node.local_addr();

    println!("🚀 Server online and ready!");
    println!("   Hosting actor : 'banking-service/account-9876'");
    println!("   Listening on  : http://{local_addr}");
    println!("\n👉 Now open Terminal 2 and run:");
    println!("   cargo run --example remote_client --features remote\n");
    println!("(Press Ctrl+C to initiate coordinated 2-phase graceful shutdown)\n");

    // 5. Keep server running until Ctrl+C
    tokio::signal::ctrl_c().await?;

    println!("\n🛑 Shutdown signal received. Starting 2-phase graceful shutdown...");
    node.shutdown().await?;
    println!("✅ Server Node shutdown complete. All actors and connections drained.\n");

    Ok(())
}

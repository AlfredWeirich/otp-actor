//! # Remote Actor over gRPC (All-in-One Showcase)
//!
//! Demonstrates the full lifecycle (server hosting + client calls + crash self-healing + shutdown)
//! within a single executable process.
//!
//! ## Run this example:
//! ```bash
//! cargo run --example remote_all_in_one --features remote
//! ```

mod common;

use common::{AccountMsg, BankAccountActor};
use otp_actor::actor::{RestartType, RootSupervisor, ServiceSupervisor, StatefulActorBuilder};
use otp_actor::remote::{ActorNodeHandle, ActorNodeRegistry, RemoteActorClient, RemoteActorError};
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
    println!("  otp-actor: Distributed Remote Actor over gRPC (HTTP/2)");
    println!("============================================================\n");

    // ------------------------------------------------------------------------
    // Step 1: Initialize Supervision Tree on Server Node
    // ------------------------------------------------------------------------
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("banking-service", &root).await?;

    let initial_account = BankAccountActor {
        account_id: "ACC-9876".into(),
        balance: 1000,
    };

    let local_client = StatefulActorBuilder::from_instance("account-9876", initial_account)
        .restart_type(RestartType::Permanent)
        .spawn(&service)
        .await?;

    // ------------------------------------------------------------------------
    // Step 2: Register Actor with Remote Node Registry & Start gRPC Server
    // ------------------------------------------------------------------------
    let registry = ActorNodeRegistry::new();
    registry.register_client(local_client);

    // Bind to an ephemeral OS port on localhost
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_node = ActorNodeHandle::start(bind_addr, registry, Some(root)).await?;
    let server_addr = server_node.local_addr();

    println!("[Server Node] Hosting 'banking-service/account-9876' on gRPC http://{server_addr}\n");

    // ------------------------------------------------------------------------
    // Step 3: Connect Remote Client from Client Process / Context
    // ------------------------------------------------------------------------
    let remote_client = RemoteActorClient::<BankAccountActor>::connect(
        format!("http://{server_addr}"),
        "banking-service",
        "account-9876",
    )
    .await?;

    println!("[Client] Successfully connected to remote actor!");

    // 1. Check Initial Balance
    let balance = remote_client.call(AccountMsg::GetBalance).await?;
    println!("[Client] Query balance: {balance} EUR (Expected: 1000)");
    assert_eq!(balance, 1000);

    // 2. Perform Deposit
    let balance = remote_client.call(AccountMsg::Deposit(500)).await?;
    println!("[Client] Deposited 500 EUR -> New balance: {balance} EUR");
    assert_eq!(balance, 1500);

    // 3. Perform Withdrawal
    let balance = remote_client.call(AccountMsg::Withdraw(300)).await?;
    println!("[Client] Withdrew 300 EUR  -> New balance: {balance} EUR");
    assert_eq!(balance, 1200);

    // 4. Test Domain Error Propagation
    println!("\n[Client] Testing business error propagation (Overdraft attempt)...");
    match remote_client.call(AccountMsg::Withdraw(5000)).await {
        Err(RemoteActorError::Actor(domain_err)) => {
            println!("[Client] Received expected typed business error: '{domain_err}'");
        }
        other => panic!("Expected domain error, got {other:?}"),
    }

    // ------------------------------------------------------------------------
    // Step 4: Resilience Showcase - Remote Actor Crash & Transparent Healing
    // ------------------------------------------------------------------------
    println!("\n------------------------------------------------------------");
    println!("  Resilience Showcase: Triggering crash on remote node");
    println!("------------------------------------------------------------");

    println!("[Client] Injecting fatal crash command into remote actor...");
    let _ = remote_client.call(AccountMsg::SimulateCrash).await;

    println!("[Client] Immediately dispatching new request over the existing gRPC channel...");
    // The server-side ActorClient automatically backs off, waits for ServiceSupervisor
    // to re-spawn the actor with initial state (1000), rebinds, and processes the deposit!
    let resurrected_balance = remote_client.call(AccountMsg::Deposit(250)).await?;
    println!(
        "[Client] Self-healing successful! Replacement actor responded: {resurrected_balance} EUR (Expected: 1250)\n"
    );
    assert_eq!(resurrected_balance, 1250);

    // ------------------------------------------------------------------------
    // Step 5: Coordinated 2-Phase Graceful Shutdown
    // ------------------------------------------------------------------------
    println!("------------------------------------------------------------");
    println!("  Graceful Shutdown: Coordinating gRPC drainage & actor tree");
    println!("------------------------------------------------------------");

    server_node.shutdown().await?;
    println!("[Server Node] Graceful 2-phase shutdown complete. All resources cleaned up.");

    Ok(())
}

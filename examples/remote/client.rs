//! # Remote Actor Client
//!
//! Connects over network gRPC (HTTP/2) to an actor running on a remote node (Rechner B).
//! Requires no local supervisors or local actors.
//!
//! ## How to Run:
//! ```bash
//! # Run client (Terminal 2)
//! cargo run --example remote_client --features remote
//! ```
//!
//! Optional environment variables:
//! - `REMOTE_ENDPOINT`: Target node gRPC URL (default: `http://127.0.0.1:50051`).

mod common;

use common::{AccountMsg, BankAccountActor};
use otp_actor::remote::{RemoteActorClient, RemoteActorError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,otp_actor=warn".into()),
        )
        .init();

    println!("============================================================");
    println!("  [Rechner A / Client] Connecting to Remote Actor over gRPC");
    println!("============================================================\n");

    let endpoint_url =
        std::env::var("REMOTE_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".into());

    println!("Connecting to remote node at {endpoint_url}...");
    let client = RemoteActorClient::<BankAccountActor>::connect(
        &endpoint_url,
        "banking-service",
        "account-9876",
    )
    .await?;

    println!("✅ Connected to 'banking-service/account-9876'!\n");

    // 1. Initial Balance
    let balance = client.call(AccountMsg::GetBalance).await?;
    println!("1️⃣ [Query] Current balance on remote node: {balance} EUR");

    // 2. Deposit
    let balance = client.call(AccountMsg::Deposit(500)).await?;
    println!("2️⃣ [Deposit] Deposited 500 EUR -> New balance: {balance} EUR");

    // 3. Withdrawal
    let balance = client.call(AccountMsg::Withdraw(300)).await?;
    println!("3️⃣ [Withdraw] Withdrew 300 EUR -> New balance: {balance} EUR");

    // 4. Domain Error Handling
    println!("\n4️⃣ [Business Error Test] Attempting overdraft of 50,000 EUR...");
    match client.call(AccountMsg::Withdraw(50000)).await {
        Err(RemoteActorError::Actor(domain_err)) => {
            println!("   ↳ Received expected typed business error: '{domain_err}'");
        }
        other => panic!("Expected domain error, got {other:?}"),
    }

    // 5. Crash & Self-Healing Showcase
    println!("\n5️⃣ [Resilience Test] Sending crash injection to remote actor...");
    let _ = client.call(AccountMsg::SimulateCrash).await;
    println!("   ↳ Crash command delivered! Remote supervisor is restarting actor...");

    println!("   ↳ Immediately dispatching follow-up deposit over existing gRPC connection...");
    let balance = client.call(AccountMsg::Deposit(250)).await?;
    println!("   ↳ ✅ Transparent recovery successful! Replacement actor balance: {balance} EUR");

    // 6. Controlled Domain Termination via ctx.stop()
    println!(
        "\n6️⃣ [Controlled Termination] Client closing account via AccountMsg::CloseAccount..."
    );
    let final_balance = client.call(AccountMsg::CloseAccount).await?;
    println!("   ↳ Account closed with final payout balance: {final_balance} EUR.");
    println!("   ↳ Remote actor drained its queue and stopped gracefully (on_stop executed).");
    println!(
        "   ↳ Supervisor (RestartType::Transient) honors clean shutdown and leaves actor dead."
    );

    // Verify actor is truly stopped and no longer available
    match client.call(AccountMsg::GetBalance).await {
        Err(RemoteActorError::ActorNotFound(_) | RemoteActorError::ActorUnavailable) => {
            println!("   ↳ ✅ Verified: Follow-up call correctly reports actor no longer active!");
        }
        other => panic!("Expected ActorNotFound or ActorUnavailable after shutdown, got {other:?}"),
    }

    println!("\n🎉 All remote actor calls completed successfully!\n");
    Ok(())
}

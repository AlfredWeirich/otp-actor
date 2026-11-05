//! # Hierarchical Supervision Tree Demo Application
//!
//! Demonstrates the OTP-modeled supervision architecture:
//! - **Autonomous Service Self-Registration**: Services register with the RootSupervisor;
//!   child actors are spawned and supervised via ChildSpecs under the ServiceSupervisor.
//! - **Generic Worker Trait**: Pure decoupled actors powered by custom structs or async closures (Zero `Box::pin`!).
//! - **Control Plane & Data Plane Decoupling**: Clients talk directly to actors (1-hop channel).
//! - **Transparent Client Self-Healing**: Transparently recovers across worker crashes and supervisor restarts.

use std::time::Duration;
use tokio::time::Instant;

use aixtor::actor::{
    GenericResourceActor, RestartPolicy, RootSupervisor, ServiceSupervisor, SupervisionStrategy,
    TimedResource, Worker, WorkerContext,
};

// ============================================================================
// Domain Types & Worker Implementations (Defined in Application / Domain Layer)
// ============================================================================

/// Represents an OAuth2 / IAM Access Token and its lifecycle metadata.
#[derive(Debug, Clone)]
pub struct GcpToken {
    pub access_token: String,
    pub generation: usize,
    pub expires_at: Instant,
}

/// Domain errors for Google Cloud token fetching.
#[derive(Debug, thiserror::Error, Clone)]
pub enum TokenError {
    #[error("Network error: {0}")]
    Network(String),
}

/// Simulates a Google Cloud Metadata Server HTTP request to acquire an OAuth2 access token.
pub async fn fetch_token_from_gcp(
    service_scope: &str,
    generation: usize,
) -> Result<GcpToken, TokenError> {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let access_token = format!(
        "ya29.c.MOCK_GCP_{}_GEN_{:02}_{:?}",
        service_scope.to_uppercase().replace('-', "_"),
        generation,
        Instant::now()
    );

    Ok(GcpToken {
        access_token,
        generation,
        expires_at: Instant::now() + Duration::from_secs(4),
    })
}

/// 1. Showcase: Custom Struct implementing the `Worker` Trait directly!
pub struct GcpTokenWorker {
    pub scope: String,
}

impl Worker<String, TokenError> for GcpTokenWorker {
    async fn execute(&self, ctx: WorkerContext) -> Result<TimedResource<String>, TokenError> {
        let token = fetch_token_from_gcp(&self.scope, ctx.generation).await?;
        tracing::debug!(
            scope = %self.scope,
            generation = token.generation,
            "Fetched GCP token from metadata server"
        );
        Ok(TimedResource {
            value: token.access_token,
            expires_at: Some(token.expires_at),
        })
    }
}

/// 2. Showcase: Standalone async function implementing `Worker` directly (Zero `Box::pin`!).
pub async fn gcp_bigquery_worker(
    ctx: WorkerContext,
) -> Result<TimedResource<String>, TokenError> {
    let token = fetch_token_from_gcp("gcp-bigquery", ctx.generation).await?;
    tracing::debug!(
        scope = "gcp-bigquery",
        generation = token.generation,
        "Fetched BigQuery token from metadata server"
    );
    Ok(TimedResource {
        value: token.access_token,
        expires_at: Some(token.expires_at),
    })
}

/// Helper to safely mask and format secrets/tokens in output without raw slicing panics.
fn mask_secret(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= 12 {
        "***".to_string()
    } else {
        let prefix: String = chars[..8.min(chars.len())].iter().collect();
        let suffix: String = chars[chars.len().saturating_sub(4)..].iter().collect();
        format!("{prefix}…{suffix} (len: {})", chars.len())
    }
}

// ============================================================================
// Main Application Demonstration
// ============================================================================

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_target(false)
        .init();

    println!("========================================================");
    println!("Supervision Tree with Autonomous Self-Registration");
    println!("  RootSupervisor <- ServiceSupervisor <- Dynamic Actors");
    println!("========================================================\n");

    // ------------------------------------------------------------------
    // Phase 1: Bootstrapping Root Supervisor & Service Self-Registration
    // ------------------------------------------------------------------
    println!("-------------------------------------------------------------------------");
    println!("--- Phase 1: Root Supervisor & Service Self-Registration ---");
    println!("-------------------------------------------------------------------------");

    let root_supervisor = RootSupervisor::new();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The ServiceSupervisor starts itself and registers autonomously with the RootSupervisor!
    println!(
        "-> ServiceSupervisor 'gcp-auth-service' boots and registers ITSELF with RootSupervisor..."
    );
    let auth_service_supervisor =
        ServiceSupervisor::spawn_and_register("gcp-auth-service", &root_supervisor)
            .await
            .expect("Failed to self-register service supervisor");

    tokio::time::sleep(Duration::from_millis(50)).await;
    println!("-> ServiceSupervisor successfully connected to RootSupervisor.\n");

    // ------------------------------------------------------------------
    // Phase 2: Autonomous Actor Registration with Injected Workers
    // ------------------------------------------------------------------
    println!("-------------------------------------------------------------------------");
    println!("--- Phase 2: Autonomous Actor Registration (Injected Worker Trait) ---");
    println!("-------------------------------------------------------------------------");

    // Actor 1: Injected custom Worker struct + Eager Pre-Warming ENABLED!
    println!(
        "-> Actor 'gcp-storage' boots with custom GcpTokenWorker struct (Pre-Warm: ENABLED)..."
    );
    let storage_worker = GcpTokenWorker {
        scope: "gcp-storage".to_string(),
    };
    let storage_client = GenericResourceActor::builder("gcp-storage")
        .worker(storage_worker) // <-- Worker Struct injected from the outside!
        .eager_prewarm(true)
        .spawn_and_register(&auth_service_supervisor, &root_supervisor)
        .await
        .expect("Failed to self-register gcp-storage actor");

    // Actor 2: Injected Worker async function (Lazy / Cold Start)
    println!("-> Actor 'gcp-bigquery' boots with worker async function (Pre-Warm: DISABLED)...");
    let bigquery_client = GenericResourceActor::builder("gcp-bigquery")
        .worker(gcp_bigquery_worker) // <-- Zero-allocation standalone async function!
        .eager_prewarm(false)
        .spawn_and_register(&auth_service_supervisor, &root_supervisor)
        .await
        .expect("Failed to self-register gcp-bigquery actor");

    // Actor 3: Completely custom generic worker closure (e.g. dynamic Database Secret rotator)
    println!("-> Actor 'billing-db' boots with custom DB rotator closure...");
    let db_worker = |ctx: WorkerContext| async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok::<_, String>(TimedResource::with_ttl(
            format!(
                "postgres://admin:mock_token_gen_{:02}@db.internal:5432/billing",
                ctx.generation
            ),
            Duration::from_secs(5),
        ))
    };

    let db_client = GenericResourceActor::builder("billing-db")
        .worker(db_worker) // <-- Zero-allocation inline async closure!
        .expiry_buffer(Duration::from_secs(2))
        .channel_capacity(128)
        .spawn_and_register(&auth_service_supervisor, &root_supervisor)
        .await
        .expect("Failed to self-register billing-db actor");

    // Allow 350ms so that the eager pre-warming background fetch for 'gcp-storage' completes!
    tokio::time::sleep(Duration::from_millis(350)).await;
    println!("-> All 3 actors active. Pre-warm background task for 'gcp-storage' completed.\n");

    // ------------------------------------------------------------------
    // Phase 3: Direct Data Plane Serving & Eager Cache Verification
    // ------------------------------------------------------------------
    println!("-------------------------------------------------------------------------");
    println!("--- Phase 3: Direct Data Plane Serving & Eager Cache Verification ---");
    println!("-------------------------------------------------------------------------");

    let t_start = Instant::now();

    // Query Storage Token directly through storage_client (Instant Cache-Hit due to Eager Pre-Warming!)
    let storage_token = storage_client.get().await.unwrap();
    println!(
        "  [Client -> StorageActor] 🔥 EAGER CACHE-HIT on 1st request! Elapsed: {:?}: {}",
        t_start.elapsed(),
        mask_secret(&storage_token)
    );

    // Query BigQuery Token directly through bigquery_client (Cold Start: took ~300ms)
    let bq_start = Instant::now();
    let bq_token = bigquery_client.get().await.unwrap();
    println!(
        "  [Client -> BigQueryActor] ❄️  COLD START on 1st request! Elapsed: {:?}: {}",
        bq_start.elapsed(),
        mask_secret(&bq_token)
    );

    // Query Custom Database Resource directly through db_client (Cold Start: took ~200ms)
    let db_start = Instant::now();
    let db_conn = db_client.get().await.unwrap();
    println!(
        "  [Client -> DbActor] ❄️  COLD START on 1st request! Elapsed: {:?}: {}",
        db_start.elapsed(),
        mask_secret(&db_conn)
    );

    // Subsequent direct query to BigQuery: Instant Cache Hit (bypasses supervisor completely)
    let bq_cache_start = Instant::now();
    let cached_bq = bigquery_client.get().await.unwrap();
    println!(
        "  [Client -> BigQueryActor] ⚡ Subsequent Instant Direct Cache-Hit in {:?}: {}\n",
        bq_cache_start.elapsed(),
        mask_secret(&cached_bq)
    );

    // ------------------------------------------------------------------
    // Phase 4: Actor-Level Crash & Transparent Client Channel Rebinding
    // We intentionally crash 'gcp-storage'.
    // Sibling 'gcp-bigquery' MUST remain completely unaffected!
    // ------------------------------------------------------------------
    println!("-------------------------------------------------------------------------");
    println!("--- Phase 4: Isolated Actor Crash & Transparent Client Rebind ---");
    println!("-> Injecting crash command directly into 'gcp-storage' via client...");
    println!("-------------------------------------------------------------------------");
    let _ = storage_client.simulate_crash().await;

    // Allow 80ms for ServiceSupervisor to catch panic and respawn 'gcp-storage' #2
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Verify Sibling Resilience: 'gcp-bigquery' was NEVER interrupted!
    println!("\n-> Verifying Sibling Resilience: Requesting token directly from 'gcp-bigquery'...");
    let bq_check_start = Instant::now();
    let bq_check_token = bigquery_client.get().await.unwrap();
    println!(
        "  [Client -> BigQueryActor] ✅ BigQuery unaffected! Instant response in {:?}: {}",
        bq_check_start.elapsed(),
        mask_secret(&bq_check_token)
    );

    // Verify Self-Healing: 'storage_client' detects broken channel, rebinds to #2, and succeeds!
    println!("\n-> Verifying Self-Healing: Calling storage_client.get()...");
    let storage_check_start = Instant::now();
    let storage_check_token = storage_client.get().await.unwrap();
    println!(
        "  [Client -> StorageActor] ✅ Restored Storage Actor responded in {:?}: {}\n",
        storage_check_start.elapsed(),
        mask_secret(&storage_check_token)
    );

    // ------------------------------------------------------------------
    // Phase 5: Service-Supervisor Crash & Root-Supervisor Auto-Restart
    // ------------------------------------------------------------------
    println!("-------------------------------------------------------------------------");
    println!("--- Phase 5: Service-Supervisor Crash & Hierarchical Subtree Restart ---");
    println!("-> Injecting crash command into ServiceSupervisor 'gcp-auth-service' via Root...");
    println!("-------------------------------------------------------------------------");

    let _ = root_supervisor
        .trigger_service_crash("gcp-auth-service")
        .await;

    // Allow 80ms for RootSupervisor to intercept panic and spawn ServiceSupervisor #2
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Verify OTP Subtree Recovery & Transparent Client Rebinding:
    // In OTP, dropping the supervisor terminates its child actors cleanly. ServiceSupervisor #2
    // boots replacement child actors from ChildSpecs. Clients detect the closed channel,
    // automatically rebind to the replacement actors, and complete queries transparently!
    println!(
        "\n-> Verifying Transparent Rebind: Clients query actors across supervisor restart..."
    );
    let bq_p5_start = Instant::now();
    let bq_p5_token = bigquery_client
        .get()
        .await
        .expect("BigQuery client should transparently recover");
    println!(
        "  [Client -> BigQueryActor] ✅ Transparent rebind & served in {:?}: {}",
        bq_p5_start.elapsed(),
        mask_secret(&bq_p5_token)
    );

    let storage_p5_start = Instant::now();
    let storage_p5_token = storage_client
        .get()
        .await
        .expect("Storage client should transparently recover");
    println!(
        "  [Client -> StorageActor] ✅ Transparent rebind & served in {:?}: {}",
        storage_p5_start.elapsed(),
        mask_secret(&storage_p5_token)
    );

    let db_p5_start = Instant::now();
    let db_p5_conn = db_client
        .get()
        .await
        .expect("Database client should transparently recover");
    println!(
        "  [Client -> DbActor] ✅ Transparent rebind & served in {:?}: {}\n",
        db_p5_start.elapsed(),
        mask_secret(&db_p5_conn)
    );

    // ------------------------------------------------------------------
    // Phase 6: Restored Subtree Active & Self-Healing Operational
    // ------------------------------------------------------------------
    println!("-------------------------------------------------------------------------");
    println!("--- Phase 6: Restored Subtree Active & Self-Healing Operational ---");
    println!("-> Verifying that ServiceSupervisor #2 supervises its replacement actors...");
    println!("-------------------------------------------------------------------------");
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!(
        "\n-> Verifying Restored Supervision: Injecting crash into 'gcp-storage' under ServiceSupervisor #2..."
    );
    let _ = storage_client.simulate_crash().await;

    // Allow 80ms for restored ServiceSupervisor #2 to detect the crash and restart 'gcp-storage' #3!
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Client requests token: transparent rebind to 'gcp-storage' #3 under ServiceSupervisor #2!
    println!(
        "\n-> Verifying Self-Healing under restored ServiceSupervisor #2: Calling storage_client.get()..."
    );
    let storage_p6_start = Instant::now();
    let storage_p6_token = storage_client.get().await.unwrap();
    println!(
        "  [Client -> StorageActor] ✅ Successfully served by restored 'gcp-storage' #3 in {:?}: {}",
        storage_p6_start.elapsed(),
        mask_secret(&storage_p6_token)
    );

    // Verify Sibling Resilience: BigQuery is still healthy!
    let bq_p6_start = Instant::now();
    let bq_p6_token = bigquery_client.get().await.unwrap();
    println!(
        "  [Client -> BigQueryActor] ✅ BigQuery still healthy and served in {:?}: {}\n",
        bq_p6_start.elapsed(),
        mask_secret(&bq_p6_token)
    );

    // ------------------------------------------------------------------
    // Phase 7: Crash-Loop Protection & Exponential Backoff
    // ------------------------------------------------------------------
    println!("-------------------------------------------------------------------------");
    println!("--- Phase 7: Crash-Loop Protection & Exponential Backoff ---");
    println!("-------------------------------------------------------------------------");
    println!("-> Booting a volatile actor 'unstable-worker' with strict RestartPolicy:");
    println!("   - max_restarts: 2");
    println!("   - time_window: 5s");
    println!("   - initial_backoff: 50ms, multiplier: 2.0 (50ms -> 100ms -> permanent failure)\n");

    let unstable_worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::with_ttl(
            format!("volatile-payload-gen-{:02}", ctx.generation),
            Duration::from_secs(10),
        ))
    };

    let strict_policy = RestartPolicy {
        max_restarts: 2,
        time_window: Duration::from_secs(5),
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(500),
        backoff_multiplier: 2.0,
    };

    let unstable_client = GenericResourceActor::builder("unstable-worker")
        .worker(unstable_worker)
        .restart_policy(strict_policy)
        .spawn_and_register(&auth_service_supervisor, &root_supervisor)
        .await
        .expect("Failed to spawn unstable-worker");

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Initial successful read
    let initial_val = unstable_client
        .get()
        .await
        .expect("Initial fetch must succeed");
    println!(
        "  [Client -> unstable-worker] Initial read succeeded: {}",
        initial_val
    );

    println!("\n-> Triggering Crash #1 (Allowed, backoff: 50ms)...");
    let _ = unstable_client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify it recovered from Crash #1 (Instance #2)
    let val_after_crash1 = unstable_client
        .get()
        .await
        .expect("Should recover after crash #1");
    println!(
        "  [Client -> unstable-worker] ✅ Successfully recovered from crash #1: {}",
        val_after_crash1
    );

    println!("\n-> Triggering Crash #2 (Allowed, exponential backoff: 100ms)...");
    let _ = unstable_client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(180)).await;

    // Verify it recovered from Crash #2 (Instance #3)
    let val_after_crash2 = unstable_client
        .get()
        .await
        .expect("Should recover after crash #2");
    println!(
        "  [Client -> unstable-worker] ✅ Successfully recovered from crash #2: {}",
        val_after_crash2
    );

    println!("\n-> Triggering Crash #3 (Exceeds max_restarts = 2 within 5s window)...");
    let _ = unstable_client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("-> Attempting to access permanently failed actor via client...");
    let result = unstable_client.get().await;
    match result {
        Err(err) => {
            println!(
                "  [Client -> unstable-worker] 🛑 Fast-fail confirmed! Expected failure intercepted:\n  --> {:?}\n",
                err
            );
        }
        Ok(unexpected) => panic!(
            "Actor should have been halted by crash-loop protection! Got: {}",
            unexpected
        ),
    }

    println!("========================================================");
    println!("--- Phase 8: Dependent Actors & One-for-All Supervision ---");
    println!("--------------------------------------------------------");
    println!(
        "-> Booting a dedicated ServiceSupervisor 'order-pipeline' with SupervisionStrategy::OneForAll..."
    );
    let pipeline_supervisor = ServiceSupervisor::spawn_and_register_with_strategy(
        "order-pipeline",
        SupervisionStrategy::OneForAll,
        &root_supervisor,
    )
    .await
    .expect("Failed to register order-pipeline supervisor");

    println!("-> Registering 2 dependent actors under 'order-pipeline':");
    println!("   1. 'session-mgr' (manages stateful session)");
    println!("   2. 'order-cache' (caches data coupled to session)");

    let session_worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "session_token_inst_{}",
            ctx.instance_id
        )))
    };
    let cache_worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "cached_state_inst_{}",
            ctx.instance_id
        )))
    };

    let session_client = GenericResourceActor::builder("session-mgr")
        .worker(session_worker)
        .spawn_and_register(&pipeline_supervisor, &root_supervisor)
        .await
        .expect("Failed to spawn session-mgr");

    let cache_client = GenericResourceActor::builder("order-cache")
        .worker(cache_worker)
        .spawn_and_register(&pipeline_supervisor, &root_supervisor)
        .await
        .expect("Failed to spawn order-cache");

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Both actors are at instance 1
    let s1 = session_client.get().await.unwrap();
    let c1 = cache_client.get().await.unwrap();
    println!(
        "  [Initial State] session-mgr: {} | order-cache: {}",
        s1, c1
    );

    println!("\n-> Injecting crash into 'session-mgr'...");
    println!(
        "   Under 'OneForAll', 'order-cache' MUST be automatically aborted and restarted as well!"
    );
    let _ = session_client.simulate_crash().await;

    // Allow 120ms for supervisor to execute OneForAll restart sequence
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Verify both actors were recreated to instance 2!
    let s2 = session_client.get().await.unwrap();
    let c2 = cache_client.get().await.unwrap();
    println!(
        "  [After OneForAll Restart] session-mgr: {} | order-cache: {}",
        s2, c2
    );
    assert_eq!(s2, "session_token_inst_2");
    assert_eq!(c2, "cached_state_inst_2");
    println!(
        "  ✅ One-for-All verified: Both dependent actors cleanly re-initialized to instance #2!\n"
    );

    println!("========================================================");
    println!("Supervision Tree demonstration concluded successfully.");
    println!("========================================================");
}

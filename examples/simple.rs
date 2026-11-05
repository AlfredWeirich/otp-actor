//! # Educational Walkthrough: Resilient Resource Actor, StatefulActor & Supervision Tree
//!
//! This example demonstrates the foundational architectural patterns and core capabilities
//! of the framework:
//!
//! 1. **Supervision Tree Hierarchy (Control Plane)**:
//!    Multi-level lifecycle management via `RootSupervisor` -> `ServiceSupervisor` -> actors.
//! 2. **Worker Definition (Zero-Cost Abstraction)**:
//!    A lightweight asynchronous closure or struct returning `impl Future` to fetch and refresh
//!    expiring resources (`TimedResource`) without `Box::pin` heap allocations.
//! 3. **Declarative Actor Configuration & Registration**:
//!    Fluent builder ergonomics with eager pre-warming, expiry safety margins, and OTP restart rules.
//! 4. **Direct Data-Plane Channel (Zero Supervisor Overhead)**:
//!    Direct 1-hop client-to-actor messaging for sub-microsecond cache hits and maximum throughput.
//! 5. **Transparent Client Self-Healing**:
//!    Automatic supervisor-driven crash recovery with transparent client channel rebinding and retry.
//! 6. **General-Purpose Stateful Actor (`StatefulActor`)**:
//!    Erlang/OTP `gen_server`-style state machines (`call` request/response, `cast` fire-and-forget,
//!    `on_start` and `on_stop` lifecycle hooks) supervised alongside resource actors in the same tree.
//! 7. **Graceful Cascading Shutdown**:
//!    Clean termination in reverse registration order honoring bounded `ShutdownPolicy` deadlines
//!    and mailbox draining.

use std::time::Duration;
use tokio::time::Instant;

use otp_actor::actor::{
    ActorContext, GenericResourceActor, RestartType, RootSupervisor, ServiceSupervisor,
    StatefulActor, StatefulActorBuilder, TimedResource, WorkerContext,
};

// ============================================================================
// Example: General-Purpose Stateful Actor (StatefulActor)
// ============================================================================

/// In-memory state container for our audit counter stateful actor.
#[derive(Clone, Default)]
struct AuditCounterActor {
    requests_logged: u64,
}

/// Typed message protocol for `AuditCounterActor`.
#[derive(Clone, Debug)]
enum AuditMsg {
    /// Increments the request counter by `n` and returns `(current_total, instance_id)`.
    RecordEvents(u64),
    /// Queries the current counter total alongside the supervisor `instance_id`.
    GetStats,
    /// Simulates an unrecoverable internal state corruption via `ctx.fail(...)`.
    TriggerFatalError,
}

impl StatefulActor for AuditCounterActor {
    type Message = AuditMsg;
    type Response = (u64, usize);
    type Error = String;

    async fn on_start(&mut self, ctx: &mut ActorContext) -> Result<(), String> {
        println!(
            "  🟢 [StatefulActor '{}'] on_start hook executed (Instance #{})",
            ctx.name(),
            ctx.instance_id()
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            AuditMsg::RecordEvents(n) => {
                self.requests_logged += n;
                Ok((self.requests_logged, ctx.instance_id()))
            }
            AuditMsg::GetStats => Ok((self.requests_logged, ctx.instance_id())),
            AuditMsg::TriggerFatalError => {
                // `ctx.fail(...)` escalates the fatal failure to the ServiceSupervisor (triggering restart recovery),
                // while `Err(...)` immediately returns the error message to the waiting caller (`call`).
                ctx.fail("Simulated critical state corruption in AuditCounterActor");
                Err("Internal state corrupted".to_string())
            }
        }
    }

    async fn on_stop(&mut self, ctx: &mut ActorContext) {
        println!(
            "  🔵 [StatefulActor '{}'] on_stop hook executed during graceful shutdown (Instance #{}, logged events: {})",
            ctx.name(),
            ctx.instance_id(),
            self.requests_logged
        );
    }
}

/// Masks sensitive credentials/tokens for secure console display.
fn mask_secret(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= 12 {
        "***".to_string()
    } else {
        let prefix: String = chars[..6.min(chars.len())].iter().collect();
        let suffix: String = chars[chars.len().saturating_sub(4)..].iter().collect();
        format!("{prefix}…{suffix} (len: {})", chars.len())
    }
}

#[tokio::main]
async fn main() {
    // Optional: Initialize structured tracing subscriber (set to WARN to keep console output clean)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::WARN.into()),
        )
        .with_target(false)
        .init();

    println!("========================================================");
    println!("  Educational Demo: Generic Resource Actor & StatefulActor");
    println!("========================================================\n");

    // ------------------------------------------------------------------
    // Step 1: Initialize Supervision Hierarchy (Control Plane)
    // ------------------------------------------------------------------
    println!("--- Step 1: Initialize Supervision Hierarchy ---");

    // The RootSupervisor oversees domain ServiceSupervisors at the root level.
    let root = RootSupervisor::new();

    // The ServiceSupervisor manages the concrete worker and stateful actors within a domain.
    // It autonomously self-registers with the RootSupervisor.
    let auth_service = ServiceSupervisor::spawn_and_register("auth-service", &root)
        .await
        .expect("Failed to register ServiceSupervisor with RootSupervisor");

    println!("  ✅ RootSupervisor and ServiceSupervisor ('auth-service') active.\n");

    // ------------------------------------------------------------------
    // Step 2: Define Worker Logic (Resource Acquisition)
    // ------------------------------------------------------------------
    println!("--- Step 2: Define Worker Logic (Resource Acquisition) ---");

    // A Worker is any async function or closure matching the signature:
    //   Fn(WorkerContext) -> Future<Output = Result<TimedResource<T>, E>>
    // Thanks to native `impl Future`, there are zero `Box::pin` heap allocations.
    let token_worker = |ctx: WorkerContext| async move {
        println!(
            "  ⚙️  [Worker Invocation] Fetching fresh token from identity provider (Generation: {}, Instance: {})...",
            ctx.generation, ctx.instance_id
        );

        // Simulate network latency of an external API call (e.g., OAuth2 token endpoint)
        tokio::time::sleep(Duration::from_millis(50)).await;

        let token_value = format!("TOKEN_SECRET_GEN_{:02}", ctx.generation);

        // Bundle the acquired value with an explicit Time-to-Live (TTL):
        Ok::<_, String>(TimedResource::with_ttl(
            token_value,
            Duration::from_secs(5), // Valid for 5 seconds
        ))
    };

    println!("  ✅ Worker closure configured for token generation.\n");

    // ------------------------------------------------------------------
    // Step 3: Configure & Launch Actor via Fluent Builder
    // ------------------------------------------------------------------
    println!("--- Step 3: Configure & Launch Actor ---");

    let client = GenericResourceActor::builder("auth-token")
        .worker(token_worker)
        .restart_type(RestartType::Permanent) // Critical daemon: always restarted on termination
        .shutdown_timeout(Duration::from_secs(2)) // Graceful shutdown with 2s grace period for in-flight workers
        .eager_prewarm(true) // Pre-emptively fetch the resource in the background at boot time
        .expiry_buffer(Duration::from_secs(1)) // Proactively refresh 1 second before expiry
        .spawn_and_register(&auth_service, &root)
        .await
        .expect("Failed to spawn generic resource actor");

    // Short sleep (100ms) to allow the asynchronous eager pre-warm background task to complete
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("  ✅ Actor 'auth-token' online. Client handle acquired.\n");

    // ------------------------------------------------------------------
    // Step 4: Data-Plane Serving & Caching (Direct 1-Hop)
    // ------------------------------------------------------------------
    println!("--- Step 4: Test Data-Plane Serving & Caching ---");

    // 1st request: Instant cache hit due to eager pre-warming!
    let start = Instant::now();
    let token1 = client.get().await.expect("Failed to retrieve token");
    let elapsed1 = start.elapsed();
    println!(
        "  👉 1st fetch: \"{}\" (Latency: {:?}) -> Instant cache hit!",
        mask_secret(&token1),
        elapsed1
    );

    // 2nd request: Subsequent cache hit (worker is NOT re-invoked)
    let start = Instant::now();
    let token2 = client.get().await.expect("Failed to retrieve token");
    let elapsed2 = start.elapsed();
    println!(
        "  👉 2nd fetch: \"{}\" (Latency: {:?}) -> Served directly from in-memory cache!",
        mask_secret(&token2),
        elapsed2
    );

    println!("  ℹ️  Notice: Client queries travel directly to the actor mailbox; supervisors incur 0% overhead.\n");

    // ------------------------------------------------------------------
    // Step 5: Fault Resilience & Transparent Self-Healing
    // ------------------------------------------------------------------
    println!("--- Step 5: Resilience Testing (Crash & Transparent Self-Healing) ---");
    println!("  💥 Triggering controlled actor crash via simulate_crash()...");

    // Simulate an abnormal exit/panic of the actor task
    let _ = client.simulate_crash().await;

    // Brief delay: ServiceSupervisor intercepts the termination and boots replacement instance #2
    tokio::time::sleep(Duration::from_millis(80)).await;

    println!("  🔄 Client executes get() (detects disconnected channel & transparently rebinds)...");
    let start = Instant::now();
    let token_recovered = client
        .get()
        .await
        .expect("Transparent client self-healing failed");
    let elapsed_rec = start.elapsed();

    println!(
        "  ✅ Post-crash recovery & rebind: \"{}\" (Latency: {:?})",
        mask_secret(&token_recovered),
        elapsed_rec
    );
    println!("  ℹ️  The caller requires zero manual reconnect logic — recovery occurs completely transparently!\n");

    // ------------------------------------------------------------------
    // Step 6: Manual Forced Refresh (force_refresh)
    // ------------------------------------------------------------------
    println!("--- Step 6: Manual Refresh (force_refresh) ---");
    println!("  🔄 Forcing out-of-band cache invalidation & background refresh...");

    client
        .force_refresh()
        .await
        .expect("Force refresh failed");

    // The refresh executes asynchronously in the background (non-blocking) so in-flight callers
    // are not stalled. Wait briefly (70ms) for the worker task to complete:
    tokio::time::sleep(Duration::from_millis(70)).await;

    let fresh_token = client.get().await.expect("Failed to retrieve token");
    println!(
        "  ✅ Fresh token following forced refresh: \"{}\"\n",
        mask_secret(&fresh_token)
    );

    // ------------------------------------------------------------------
    // Step 7: General-Purpose StatefulActor under the Same Supervisor
    // ------------------------------------------------------------------
    println!("--- Step 7: General-Purpose StatefulActor (call, cast & crash recovery) ---");

    let audit_client = StatefulActorBuilder::from_default::<AuditCounterActor>("audit-counter")
        .restart_type(RestartType::Permanent)
        .shutdown_timeout(Duration::from_secs(2))
        .spawn(&auth_service)
        .await
        .expect("Failed to spawn StatefulActor");

    // 1. Send asynchronous fire-and-forget notifications (cast)
    audit_client
        .cast(AuditMsg::RecordEvents(3))
        .await
        .expect("cast failed");
    audit_client
        .cast(AuditMsg::RecordEvents(2))
        .await
        .expect("cast failed");

    // 2. Synchronous request-response queries (call)
    let _ = audit_client
        .call(AuditMsg::RecordEvents(5))
        .await
        .expect("call failed");
    let (count, inst) = audit_client
        .call(AuditMsg::GetStats)
        .await
        .expect("call failed");
    println!(
        "  👉 [StatefulActor] Counter state after 2x cast (3+2) + 2x call (RecordEvents(5) + GetStats): {} events (Instance #{})",
        count, inst
    );

    // 3. Trigger fatal handler escalation via ctx.fail(...) -> Supervisor restarts to Instance #2
    println!("  💥 Triggering fatal error inside StatefulActor via ctx.fail(...)...");
    let _ = audit_client.call(AuditMsg::TriggerFatalError).await;

    // ActorClient automatically rebinds to replacement Instance #2 on subsequent call!
    let (new_count, new_inst) = audit_client
        .call(AuditMsg::RecordEvents(1))
        .await
        .expect("Automatic ActorClient channel rebind failed");
    println!(
        "  ✅ [StatefulActor] After self-healing: Counter = {} event (Fresh clean instance #{})\n",
        new_count, new_inst
    );

    // ------------------------------------------------------------------
    // Step 8: Orderly Cascading Shutdown (Graceful Shutdown)
    // ------------------------------------------------------------------
    println!("--- Step 8: Graceful Shutdown of the Entire Supervision Tree ---");
    println!("  🛑 Cascading graceful shutdown across RootSupervisor, ServiceSupervisors, and all child actors...");

    root.shutdown()
        .await
        .expect("Graceful shutdown failed");

    println!("  ✅ All active worker and stateful actors (including on_stop hooks) terminated cleanly.\n");

    println!("========================================================");
    println!("  Summary:");
    println!("  - GenericResourceActor (TTL cache) + StatefulActor unified in one tree.");
    println!("  - Clean architectural separation: Control Plane (supervisors) vs. Data Plane (clients).");
    println!("  - OTP RestartType (Permanent/Transient/Temporary) & bounded ShutdownPolicy.");
    println!("  - Automatic, transparent self-healing with zero client-side reconnect boilerplates.");
    println!("========================================================");
}

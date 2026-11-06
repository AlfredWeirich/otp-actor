//! # Example: Dynamic Subsystem Supervision (Dynamic Subtree)
//!
//! Demonstrates how an actor (`TenantManager`) dynamically spawns a **new dedicated**
//! `ServiceSupervisor` at runtime and registers two tightly-coupled child actors
//! (`DatabaseWorker` and `CacheWorker`) under it using the `OneForAll` supervision strategy.
//!
//! ### Key Architectural Takeaways:
//! 1. **Dynamic Subtree Creation**: The manager actor holds a `RootSupervisorHandle`
//!    and provisions isolated domain supervisors on demand (e.g., per-tenant, per-session).
//! 2. **Isolated Failure Domain (`OneForAll`)**: If `DatabaseWorker` crashes, the dedicated
//!    tenant supervisor restarts *both* `DatabaseWorker` and `CacheWorker` to guarantee
//!    in-memory cache-consistency without affecting any other services or tenants.
//! 3. **Hierarchical Clean Teardown**: Shutting down the root supervisor gracefully
//!    terminates the entire multi-level tree.

use std::time::Duration;
use otp_actor::actor::{
    ActorClient, ActorContext, RestartType, RootSupervisor, RootSupervisorHandle,
    ServiceSupervisor, StatefulActor, StatefulActorBuilder, SupervisionStrategy,
};

// ============================================================================
// Child Actor B: Database Worker
// ============================================================================

#[derive(Clone)]
struct DatabaseWorker {
    tenant_id: String,
}

#[derive(Clone, Debug)]
enum DbMsg {
    Query(String),
    SimulateCrash,
}

impl StatefulActor for DatabaseWorker {
    type Message = DbMsg;
    type Response = (String, usize);
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            DbMsg::Query(sql) => {
                let response = format!("[{}] Executed: '{sql}'", self.tenant_id);
                Ok((response, ctx.instance_id()))
            }
            DbMsg::SimulateCrash => {
                println!(
                    "  💥 [DatabaseWorker '{}'] Triggering fatal panic via ctx.fail()...",
                    ctx.name()
                );
                ctx.fail("Simulated database connection loss");
                Err("Database crashed".to_string())
            }
        }
    }
}

// ============================================================================
// Child Actor C: Cache Worker
// ============================================================================

#[derive(Clone)]
struct CacheWorker {
    tenant_id: String,
}

#[derive(Clone, Debug)]
enum CacheMsg {
    Get(String),
}

impl StatefulActor for CacheWorker {
    type Message = CacheMsg;
    type Response = (String, usize);
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            CacheMsg::Get(key) => {
                let response = format!("[{}] Cache hit for: '{key}'", self.tenant_id);
                Ok((response, ctx.instance_id()))
            }
        }
    }
}

// ============================================================================
// Client Handles Bundle returned by the Manager
// ============================================================================

#[derive(Clone)]
struct TenantClients {
    db: ActorClient<DatabaseWorker>,
    cache: ActorClient<CacheWorker>,
}

// ============================================================================
// Manager Actor (Actor A): Holds RootSupervisorHandle via DI
// ============================================================================

#[derive(Clone)]
struct TenantManager {
    root: RootSupervisorHandle,
}

enum ManagerMsg {
    ProvisionTenant(String),
}

impl StatefulActor for TenantManager {
    type Message = ManagerMsg;
    type Response = TenantClients;
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        _ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            ManagerMsg::ProvisionTenant(tenant_id) => {
                let supervisor_name = format!("tenant-{tenant_id}-service");
                println!(
                    "  [TenantManager] 1. Spawning dedicated ServiceSupervisor '{}' (Strategy: OneForAll)...",
                    supervisor_name
                );

                // 1. Dynamically spawn a brand new ServiceSupervisor under RootSupervisor
                let tenant_supervisor = ServiceSupervisor::spawn_and_register_with_strategy(
                    supervisor_name,
                    SupervisionStrategy::OneForAll,
                    &self.root,
                )
                .await
                .map_err(|e| e.to_string())?;

                // 2. Register DatabaseWorker (Child B) under the new supervisor
                let db_name = format!("db-{tenant_id}");
                println!("  [TenantManager] 2. Spawning child '{db_name}' under tenant supervisor...");
                let db_client = StatefulActorBuilder::from_instance(
                    db_name,
                    DatabaseWorker {
                        tenant_id: tenant_id.clone(),
                    },
                )
                .restart_type(RestartType::Permanent)
                .spawn(&tenant_supervisor)
                .await
                .map_err(|e| e.to_string())?;

                // 3. Register CacheWorker (Child C) under the same tenant supervisor
                let cache_name = format!("cache-{tenant_id}");
                println!("  [TenantManager] 3. Spawning child '{cache_name}' under tenant supervisor...");
                let cache_client = StatefulActorBuilder::from_instance(
                    cache_name,
                    CacheWorker {
                        tenant_id: tenant_id.clone(),
                    },
                )
                .restart_type(RestartType::Permanent)
                .spawn(&tenant_supervisor)
                .await
                .map_err(|e| e.to_string())?;

                Ok(TenantClients {
                    db: db_client,
                    cache: cache_client,
                })
            }
        }
    }
}

// ============================================================================
// Main Execution
// ============================================================================

#[tokio::main]
async fn main() {
    println!("=== Dynamic Subtree Supervision with otp-actor ===\n");

    // Step 1: Initialize the RootSupervisor
    let root = RootSupervisor::new();

    // Step 2: Start an Admin ServiceSupervisor to host our TenantManager
    let admin_service = ServiceSupervisor::spawn_and_register("admin-service", &root)
        .await
        .expect("Failed to spawn admin-service");

    let manager = TenantManager { root: root.clone() };

    let manager_client = StatefulActorBuilder::from_instance("tenant-manager", manager)
        .spawn(&admin_service)
        .await
        .expect("Failed to spawn tenant-manager");

    println!("✅ TenantManager online.\n");

    // Step 3: Ask TenantManager to provision a dynamic subtree for 'tenant-alpha'
    println!("--- Step 1: Dynamically Provision Tenant Subtree ---");
    let tenant_clients = manager_client
        .call(ManagerMsg::ProvisionTenant("alpha".to_string()))
        .await
        .expect("Failed to provision tenant");

    println!("✅ Subtree provisioned successfully.\n");

    // Step 4: Interact with both dynamically spawned sub-actors
    println!("--- Step 2: Test Direct Data-Plane Communication ---");
    let (db_res, db_inst) = tenant_clients
        .db
        .call(DbMsg::Query("SELECT * FROM users".to_string()))
        .await
        .expect("Database query failed");
    println!("  👉 Database Response: \"{db_res}\" (Instance #{db_inst})");

    let (cache_res, cache_inst) = tenant_clients
        .cache
        .call(CacheMsg::Get("user:42".to_string()))
        .await
        .expect("Cache query failed");
    println!("  👉 Cache Response:    \"{cache_res}\" (Instance #{cache_inst})\n");

    // Step 5: Demonstrate the power of OneForAll in the dynamic subtree
    println!("--- Step 3: Demonstrate OneForAll Isolation in Dynamic Subtree ---");
    println!("  Simulating crash in DatabaseWorker...");
    let _ = tenant_clients.db.call(DbMsg::SimulateCrash).await;

    // Small delay to allow the supervisor's restart wave to complete
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Both DatabaseWorker AND CacheWorker restarted because of OneForAll!
    let (db_rec, db_new_inst) = tenant_clients
        .db
        .call(DbMsg::Query("SELECT 1".to_string()))
        .await
        .expect("Database recovery query failed");

    let (cache_rec, cache_new_inst) = tenant_clients
        .cache
        .call(CacheMsg::Get("health".to_string()))
        .await
        .expect("Cache recovery query failed");

    println!(
        "  ✅ DatabaseWorker recovered: \"{db_rec}\" (Now on Instance #{db_new_inst})"
    );
    println!(
        "  ✅ CacheWorker co-restarted: \"{cache_rec}\" (Now on Instance #{cache_new_inst})"
    );
    println!("  💡 Both actors were co-restarted together under their dedicated supervisor to preserve consistency!\n");

    // Step 6: Hierarchical Graceful Shutdown
    println!("--- Step 4: Hierarchical Graceful Shutdown ---");
    root.shutdown().await.expect("Shutdown failed");
    println!("✅ RootSupervisor, all ServiceSupervisors, and all dynamic sub-actors shut down cleanly.");
}


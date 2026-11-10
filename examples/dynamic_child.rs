//! # Example: Dynamic Child Actor Spawning via `ctx.supervisor()`
//!
//! Demonstrates how a `Manager` actor uses `ctx.supervisor()` from its [`ActorContext`]
//! (or an injected [`SupervisorHandle`]) to dynamically spawn and register new `Worker`
//! actors under the supervision tree at runtime.

use otp_actor::actor::{
    ActorClient, ActorContext, RestartType, StatefulActor, StatefulActorBuilder, Supervisor,
};

// ============================================================================
// The Worker Actor (Spawned dynamically at runtime)
// ============================================================================

#[derive(Clone)]
struct Worker {
    id: u32,
}

impl StatefulActor for Worker {
    type Message = String;
    type Response = String;
    type Error = String;

    async fn handle(
        &mut self,
        task: Self::Message,
        ctx: &mut ActorContext<Self>,
    ) -> Result<Self::Response, Self::Error> {
        Ok(format!(
            "Worker #{} (instance #{}) finished task: '{task}'",
            self.id,
            ctx.instance_id()
        ))
    }
}

// ============================================================================
// The Manager Actor (Spawns siblings using ctx.supervisor())
// ============================================================================

#[derive(Clone, Default)]
struct Manager;

enum ManagerMsg {
    SpawnWorker(u32),
}

impl StatefulActor for Manager {
    type Message = ManagerMsg;
    type Response = ActorClient<Worker>;
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext<Self>,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            ManagerMsg::SpawnWorker(worker_id) => {
                let worker_name = format!("worker-{worker_id}");
                println!("  [Manager] Dynamically registering '{worker_name}' under supervisor...");

                // The Manager uses ctx.supervisor() directly to spawn a sibling Worker:
                let worker_client =
                    StatefulActorBuilder::from_instance(worker_name, Worker { id: worker_id })
                        .restart_type(RestartType::Transient)
                        .spawn(ctx.supervisor())
                        .await
                        .map_err(|e| e.to_string())?;

                Ok(worker_client)
            }
        }
    }
}

// ============================================================================
// Main Execution
// ============================================================================

#[tokio::main]
async fn main() {
    println!("=== Dynamic Child Spawning with otp-actor ===\n");

    // 1. Initialize the Supervision Tree (root -> worker-service)
    let root = Supervisor::new("root");
    let service = Supervisor::builder("worker-service")
        .spawn(&root)
        .await
        .expect("Failed to spawn service supervisor");

    // 2. Start Manager under worker-service
    let manager_client = StatefulActorBuilder::from_default::<Manager>("manager")
        .spawn(&service)
        .await
        .expect("Failed to spawn Manager");

    println!("✅ Manager online.\n");

    // 3. Ask Manager to dynamically spawn worker-1
    println!("--- 1. Ask Manager to spawn worker-1 ---");
    let worker1_client = manager_client
        .call(ManagerMsg::SpawnWorker(1))
        .await
        .expect("Manager failed to spawn worker-1");

    // Interact directly with worker-1 over its data-plane channel
    let res1 = worker1_client
        .call("Process order #1001".to_string())
        .await
        .expect("worker-1 failed");
    println!("  👉 Response from worker-1: \"{res1}\"\n");

    // 4. Ask Manager to dynamically spawn worker-2
    println!("--- 2. Ask Manager to spawn worker-2 ---");
    let worker2_client = manager_client
        .call(ManagerMsg::SpawnWorker(2))
        .await
        .expect("Manager failed to spawn worker-2");

    let res2 = worker2_client
        .call("Process order #1002".to_string())
        .await
        .expect("worker-2 failed");
    println!("  👉 Response from worker-2: \"{res2}\"\n");

    // 5. Query the supervisor directly to prove worker-1 is actively tracked
    println!("--- 3. Query Supervisor directly for worker-1 ---");
    let resolved_client = service
        .get_actor_client::<Worker>("worker-1")
        .await
        .expect("worker-1 not found in supervisor registry");

    let res_lookup = resolved_client
        .call("Follow-up task for worker-1".to_string())
        .await
        .expect("Failed calling resolved worker-1");
    println!("  👉 Lookup verified: \"{res_lookup}\"\n");

    // 6. Clean graceful shutdown of the entire tree
    println!("--- 4. Graceful Shutdown ---");
    root.shutdown().await.expect("Shutdown failed");
    println!("✅ All supervisors, Manager, and Workers terminated cleanly.");
}

//! # Example: Dynamic Child Actor Spawning via Injected Supervisor Handle
//!
//! Demonstrates how an actor (Actor A / `ManagerActor`) receives a `ServiceSupervisorHandle`
//! via dependency injection, allowing it to dynamically spawn and register new child actors
//! (Actor B / `WorkerActor`) under the supervision tree at runtime.

use otp_actor::actor::{
    ActorClient, ActorContext, RestartType, RootSupervisor, ServiceSupervisor,
    ServiceSupervisorHandle, StatefulActor, StatefulActorBuilder,
};

// ============================================================================
// Actor B: The Worker (Spawned dynamically at runtime)
// ============================================================================

#[derive(Clone)]
struct WorkerActor {
    id: u32,
}

impl StatefulActor for WorkerActor {
    type Message = String;
    type Response = String;
    type Error = String;

    async fn handle(
        &mut self,
        task: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        Ok(format!(
            "Worker #{} (instance #{}) finished task: '{task}'",
            self.id,
            ctx.instance_id()
        ))
    }
}

// ============================================================================
// Actor A: The Manager (Holds ServiceSupervisorHandle via Dependency Injection)
// ============================================================================

#[derive(Clone)]
struct ManagerActor {
    supervisor: ServiceSupervisorHandle,
}

enum ManagerMsg {
    SpawnWorker(u32),
}

impl StatefulActor for ManagerActor {
    type Message = ManagerMsg;
    type Response = ActorClient<WorkerActor>;
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        _ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            ManagerMsg::SpawnWorker(worker_id) => {
                let worker_name = format!("worker-{worker_id}");
                println!("  [Manager] Dynamically registering '{worker_name}' under supervisor...");

                // Actor A uses its injected supervisor handle to spawn Actor B:
                let worker_client = StatefulActorBuilder::from_instance(
                    worker_name,
                    WorkerActor { id: worker_id },
                )
                .restart_type(RestartType::Transient)
                .spawn(&self.supervisor)
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

    // 1. Initialize the Supervision Tree (Root -> Service)
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("worker-service", &root)
        .await
        .expect("Failed to spawn service supervisor");

    // 2. Start Actor A (Manager), injecting the ServiceSupervisorHandle
    let manager = ManagerActor {
        supervisor: service.clone(),
    };

    let manager_client = StatefulActorBuilder::from_instance("manager", manager)
        .spawn(&service)
        .await
        .expect("Failed to spawn manager actor");

    println!("✅ Manager actor online.\n");

    // 3. Ask Actor A to dynamically spawn Worker #1 (Actor B)
    println!("--- 1. Ask Manager to spawn Worker #1 ---");
    let worker1_client = manager_client
        .call(ManagerMsg::SpawnWorker(1))
        .await
        .expect("Manager failed to spawn worker 1");

    // Interact directly with Worker #1 over its data-plane channel
    let res1 = worker1_client
        .call("Process order #1001".to_string())
        .await
        .expect("Worker 1 failed");
    println!("  👉 Direct response from Worker #1: \"{res1}\"\n");

    // 4. Ask Actor A to dynamically spawn Worker #2 (another Actor B)
    println!("--- 2. Ask Manager to spawn Worker #2 ---");
    let worker2_client = manager_client
        .call(ManagerMsg::SpawnWorker(2))
        .await
        .expect("Manager failed to spawn worker 2");

    let res2 = worker2_client
        .call("Process order #1002".to_string())
        .await
        .expect("Worker 2 failed");
    println!("  👉 Direct response from Worker #2: \"{res2}\"\n");

    // 5. Query the supervisor directly to prove Worker #1 is actively tracked
    println!("--- 3. Query ServiceSupervisor for dynamically registered Worker #1 ---");
    let resolved_client = service
        .get_actor_client::<WorkerActor>("worker-1")
        .await
        .expect("Worker 1 not found in supervisor registry");

    let res_lookup = resolved_client
        .call("Follow-up task for worker 1".to_string())
        .await
        .expect("Failed calling resolved worker 1");
    println!("  👉 Lookup verified: \"{res_lookup}\"\n");

    // 6. Clean graceful shutdown of the entire tree
    println!("--- 4. Graceful Shutdown ---");
    root.shutdown().await.expect("Shutdown failed");
    println!("✅ All supervisors and dynamic actors terminated cleanly.");
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use aixtor::actor::{
    ActorContext, ChildSpec, GenericResourceActor, ResourceError, RestartPolicy, RestartTracker,
    RestartType, RootError, RootSupervisor, ServiceError, ServiceSupervisor, ShutdownPolicy,
    StatefulActor, StatefulActorBuilder, SupervisionStrategy, TimedResource, Worker, WorkerContext,
};

#[tokio::test]
async fn test_builder_and_eager_prewarm() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("test-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Zero-allocation async closure implementing Worker<String, String> directly!
    let worker = move |ctx: WorkerContext| {
        let c = counter_clone.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<_, String>(TimedResource::with_ttl(
                format!("val_gen_{}", ctx.generation),
                Duration::from_secs(5),
            ))
        }
    };

    let client = GenericResourceActor::builder("prewarmed-actor")
        .worker(worker)
        .eager_prewarm(true)
        .expiry_buffer(Duration::from_secs(1))
        .channel_capacity(64)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    // Wait for the eager pre-warm background task to finish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // First request should be an immediate cache hit (< 5ms)
    let start = Instant::now();
    let val = client.get().await.expect("Failed to get resource");
    let elapsed = start.elapsed();

    assert_eq!(val, "val_gen_1");
    assert!(
        elapsed < Duration::from_millis(15),
        "Expected cache hit to be fast, took {:?}",
        elapsed
    );
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_crash_loop_protection() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("crash-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let policy = RestartPolicy {
        max_restarts: 2,
        time_window: Duration::from_secs(2),
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(100),
        backoff_multiplier: 2.0,
    };

    let worker = |_ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual("test-data".to_string()))
    };

    let client = GenericResourceActor::builder("crashing-actor")
        .worker(worker)
        .restart_policy(policy)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(client.get().await.unwrap(), "test-data");

    // Crash 1: Allowed (restart 1 of 2)
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(client.get().await.unwrap(), "test-data");

    // Crash 2: Allowed (restart 2 of 2)
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(client.get().await.unwrap(), "test-data");

    // Crash 3: Exceeds threshold!
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let res = client.get().await;
    match res {
        Err(ResourceError::Service(err)) => {
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("exceeded maximum restart threshold"),
                "Unexpected error message: {}",
                err_msg
            );
        }
        other => panic!(
            "Expected ServiceError::MaxRestartsExceeded, got: {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_sibling_isolation_during_crash() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("iso-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let worker_a = |_ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual("data-a".to_string()))
    };
    let worker_b = |_ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual("data-b".to_string()))
    };

    let client_a = GenericResourceActor::builder("actor-a")
        .worker(worker_a)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor A spawn failed");

    let client_b = GenericResourceActor::builder("actor-b")
        .worker(worker_b)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor B spawn failed");

    // Warm both actors
    assert_eq!(client_a.get().await.unwrap(), "data-a");
    assert_eq!(client_b.get().await.unwrap(), "data-b");

    // Crash Actor A
    let _ = client_a.simulate_crash().await;

    // Immediately verify Actor B is completely healthy and responsive
    let val_b = client_b.get().await.expect("Actor B should be unaffected");
    assert_eq!(val_b, "data-b");

    // Wait for Actor A to recover
    tokio::time::sleep(Duration::from_millis(100)).await;
    let val_a = client_a.get().await.expect("Actor A should recover");
    assert_eq!(val_a, "data-a");
}

#[tokio::test]
async fn test_strategy_one_for_all() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register_with_strategy(
        "all-service",
        SupervisionStrategy::OneForAll,
        &root,
    )
    .await
    .expect("ServiceSupervisor registration failed");

    let worker_a = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "a_inst_{}",
            ctx.instance_id
        )))
    };
    let worker_b = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "b_inst_{}",
            ctx.instance_id
        )))
    };
    let worker_c = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "c_inst_{}",
            ctx.instance_id
        )))
    };

    let client_a = GenericResourceActor::builder("actor-a")
        .worker(worker_a)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor A spawn failed");

    let client_b = GenericResourceActor::builder("actor-b")
        .worker(worker_b)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor B spawn failed");

    let client_c = GenericResourceActor::builder("actor-c")
        .worker(worker_c)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor C spawn failed");

    // All actors start at instance 1
    assert_eq!(client_a.get().await.unwrap(), "a_inst_1");
    assert_eq!(client_b.get().await.unwrap(), "b_inst_1");
    assert_eq!(client_c.get().await.unwrap(), "c_inst_1");

    // Intentionally crash Actor B
    let _ = client_b.simulate_crash().await;

    // Wait for supervisor to execute OneForAll restart sequence
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Under OneForAll, ALL actors must have been restarted and bumped to instance 2!
    assert_eq!(client_a.get().await.unwrap(), "a_inst_2");
    assert_eq!(client_b.get().await.unwrap(), "b_inst_2");
    assert_eq!(client_c.get().await.unwrap(), "c_inst_2");
}

#[tokio::test]
async fn test_strategy_rest_for_one() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register_with_strategy(
        "rest-service",
        SupervisionStrategy::RestForOne,
        &root,
    )
    .await
    .expect("ServiceSupervisor registration failed");

    // Pipeline: 1 -> 2 -> 3
    let worker_1 = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "pipe1_inst_{}",
            ctx.instance_id
        )))
    };
    let worker_2 = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "pipe2_inst_{}",
            ctx.instance_id
        )))
    };
    let worker_3 = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "pipe3_inst_{}",
            ctx.instance_id
        )))
    };

    let client_1 = GenericResourceActor::builder("pipe-1")
        .worker(worker_1)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Pipe 1 spawn failed");

    let client_2 = GenericResourceActor::builder("pipe-2")
        .worker(worker_2)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Pipe 2 spawn failed");

    let client_3 = GenericResourceActor::builder("pipe-3")
        .worker(worker_3)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Pipe 3 spawn failed");

    // All start at instance 1
    assert_eq!(client_1.get().await.unwrap(), "pipe1_inst_1");
    assert_eq!(client_2.get().await.unwrap(), "pipe2_inst_1");
    assert_eq!(client_3.get().await.unwrap(), "pipe3_inst_1");

    // 1. Crash pipe-2 (middle of the pipeline):
    // pipe-1 (upstream) must NOT be restarted!
    // pipe-2 and pipe-3 (downstream) MUST be restarted!
    let _ = client_2.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(client_1.get().await.unwrap(), "pipe1_inst_1"); // Still inst 1!
    assert_eq!(client_2.get().await.unwrap(), "pipe2_inst_2"); // Restarted to inst 2!
    assert_eq!(client_3.get().await.unwrap(), "pipe3_inst_2"); // Restarted to inst 2!

    // 2. Crash pipe-3 (end of the pipeline):
    // Only pipe-3 should restart! pipe-1 and pipe-2 remain untouched.
    let _ = client_3.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(client_1.get().await.unwrap(), "pipe1_inst_1"); // Still inst 1!
    assert_eq!(client_2.get().await.unwrap(), "pipe2_inst_2"); // Still inst 2!
    assert_eq!(client_3.get().await.unwrap(), "pipe3_inst_3"); // Restarted to inst 3!

    // 3. Crash pipe-1 (root of pipeline):
    // ALL actors (1, 2, 3) must be restarted!
    let _ = client_1.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(client_1.get().await.unwrap(), "pipe1_inst_2"); // Restarted to inst 2!
    assert_eq!(client_2.get().await.unwrap(), "pipe2_inst_3"); // Restarted to inst 3!
    assert_eq!(client_3.get().await.unwrap(), "pipe3_inst_4"); // Restarted to inst 4!
}

#[tokio::test]
async fn test_worker_panic_auto_recovery() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("panic-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    // Worker panics on instance 1, but succeeds on instance 2
    let worker = |ctx: WorkerContext| async move {
        if ctx.instance_id == 1 {
            panic!("Intentional worker crash during instance 1!");
        }
        Ok::<_, String>(TimedResource::perpetual(format!("recovered_inst_{}", ctx.instance_id)))
    };

    let client = GenericResourceActor::builder("panicky-actor")
        .worker(worker)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    // Client request must NOT hang!
    // The panic should be intercepted, escalated to ServiceSupervisor,
    // the actor auto-restarted to instance #2, and client transparently rebinds.
    let res = tokio::time::timeout(Duration::from_secs(3), client.get()).await;
    let val = res
        .expect("Client request timed out / hung!")
        .expect("Failed to get resource after recovery");

    assert_eq!(val, "recovered_inst_2");
}

#[test]
fn test_backoff_overflow_prevention() {
    let policy = RestartPolicy {
        max_restarts: usize::MAX,
        time_window: Duration::from_secs(u64::MAX / 4),
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(2),
        backoff_multiplier: 2.0,
    };

    let mut tracker = RestartTracker::new(policy);

    // Call 1200 times (exceeding f64 exponent range of 1024)
    for _ in 0..1200 {
        let backoff = tracker.record_crash().expect("Expected backoff to succeed");
        assert!(backoff <= Duration::from_secs(2));
        assert!(backoff >= Duration::from_millis(50));
    }
}

#[tokio::test(start_paused = true)]
async fn test_backoff_decays_after_stability() {
    let policy = RestartPolicy {
        max_restarts: 5,
        time_window: Duration::from_millis(10),
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(2),
        backoff_multiplier: 2.0,
    };

    let mut tracker = RestartTracker::new(policy);

    // Crash 1: backoff is 50ms
    let b1 = tracker.record_crash().unwrap();
    assert_eq!(b1, Duration::from_millis(50));

    // Crash 2 immediate: backoff is 100ms
    let b2 = tracker.record_crash().unwrap();
    assert_eq!(b2, Duration::from_millis(100));

    // Wait until time_window expires (component stabilized) in paused virtual time
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Crash 3 after stabilization: consecutive_failures reset, backoff drops back to initial 50ms!
    let b3 = tracker.record_crash().unwrap();
    assert_eq!(
        b3,
        Duration::from_millis(50),
        "Expected backoff to decay back to initial after stability"
    );
}

#[tokio::test]
async fn test_supervisor_nonblocking_backoff() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("nonblocking-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let slow_policy = RestartPolicy {
        max_restarts: 5,
        time_window: Duration::from_secs(10),
        initial_backoff: Duration::from_millis(500), // 500ms sleep on restart!
        max_backoff: Duration::from_secs(2),
        backoff_multiplier: 2.0,
    };

    let worker_a = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("actor_a_{}", ctx.instance_id)))
    };
    let worker_b = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("actor_b_{}", ctx.instance_id)))
    };

    let client_a = GenericResourceActor::builder("actor-a")
        .worker(worker_a)
        .restart_policy(slow_policy)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor A spawn failed");

    let client_b = GenericResourceActor::builder("actor-b")
        .worker(worker_b)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor B spawn failed");

    // Initialize both
    assert_eq!(client_a.get().await.unwrap(), "actor_a_1");
    assert_eq!(client_b.get().await.unwrap(), "actor_b_1");

    // Crash Actor A (which triggers 500ms backoff in supervisor)
    let _ = client_a.simulate_crash().await;

    // Immediately query Actor B while Actor A is in its 500ms backoff!
    let start = Instant::now();
    let val_b = client_b.get().await.expect("Actor B query failed");
    let elapsed = start.elapsed();

    assert_eq!(val_b, "actor_b_1");
    assert!(
        elapsed < Duration::from_millis(50),
        "Supervisor was blocked by Actor A's backoff! Elapsed: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_client_get_timeout() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("timeout-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    // Worker that takes 2 seconds to respond
    let worker = |_ctx: WorkerContext| async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok::<_, String>(TimedResource::perpetual("slow_data".to_string()))
    };

    let client = GenericResourceActor::builder("slow-actor")
        .worker(worker)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    // Request with 50ms timeout must fail fast with ResourceError::Timeout
    let res = client.get_timeout(Duration::from_millis(50)).await;
    match res {
        Err(ResourceError::Timeout(d)) => {
            assert_eq!(d, Duration::from_millis(50));
        }
        other => panic!("Expected ResourceError::Timeout, got: {:?}", other),
    }
}

// ============================================================================
// Option B: OTP Child-Spec Model Integration Tests
// ============================================================================

#[tokio::test]
async fn test_child_spec_builder_pattern() {
    let root = RootSupervisor::new();

    // 1. Closures defined externally and passed into ChildSpecs via builder
    let worker_a = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("val_a_{}", ctx.instance_id)))
    };
    let worker_b = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("val_b_{}", ctx.instance_id)))
    };

    // 2. Build ServiceSupervisor declaring ChildSpecs up-front (OTP Style!)
    let sup = ServiceSupervisor::builder("auth-service")
        .strategy(SupervisionStrategy::OneForOne)
        .add_child(
            GenericResourceActor::builder("actor-a")
                .worker(worker_a)
                .eager_prewarm(true),
        )
        .add_child(
            GenericResourceActor::builder("actor-b")
                .worker(worker_b)
                .eager_prewarm(false),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn ServiceSupervisor with ChildSpecs");

    // 3. Acquire typed ResourceClients directly from the supervisor
    let client_a = sup
        .get_client::<String, String>("actor-a")
        .await
        .expect("Failed to acquire client A");
    let client_b = sup
        .get_client::<String, String>("actor-b")
        .await
        .expect("Failed to acquire client B");

    assert_eq!(client_a.get().await.unwrap(), "val_a_1");
    assert_eq!(client_b.get().await.unwrap(), "val_b_1");
}

#[tokio::test]
async fn test_dynamic_start_child() {
    let root = RootSupervisor::new();

    let sup = ServiceSupervisor::builder("dyn-service")
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("dyn_data_{}", ctx.instance_id)))
    };

    // Dynamically start a child actor at runtime from a ChildSpec
    let spec: ChildSpec = GenericResourceActor::builder("dyn-child")
        .worker(worker)
        .build_child_spec();

    sup.start_child(spec).await.expect("Failed to start child dynamically");

    let client = sup
        .get_client::<String, String>("dyn-child")
        .await
        .expect("Failed to acquire dynamic client");

    assert_eq!(client.get().await.unwrap(), "dyn_data_1");
}

#[tokio::test]
async fn test_one_for_all_survives_and_heals_after_supervisor_crash() {
    // Proves that Option B completely resolves the flaw discovered in Probe C!
    let root = RootSupervisor::new();

    let worker_a = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("a{}", ctx.instance_id)))
    };
    let worker_b = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("b{}", ctx.instance_id)))
    };

    let sup = ServiceSupervisor::builder("coupled-service")
        .strategy(SupervisionStrategy::OneForAll)
        .add_child(GenericResourceActor::builder("a").worker(worker_a))
        .add_child(GenericResourceActor::builder("b").worker(worker_b))
        .spawn(&root)
        .await
        .expect("Failed to spawn coupled supervisor");

    let client_a = sup.get_client::<String, String>("a").await.unwrap();
    let client_b = sup.get_client::<String, String>("b").await.unwrap();

    assert_eq!(client_a.get().await.unwrap(), "a1");
    assert_eq!(client_b.get().await.unwrap(), "b1");

    // 1. Crash the ServiceSupervisor itself!
    // In Option B, RootSupervisor restarts the ServiceSupervisor fresh,
    // which automatically boots its child specifications fresh!
    let _ = root.trigger_service_crash("coupled-service").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify both clients transparently reconnect across the supervisor reboot
    assert_eq!(client_a.get().await.unwrap(), "a1");
    assert_eq!(client_b.get().await.unwrap(), "b1");

    // 2. Now simulate a crash in actor 'a' under the restored OneForAll supervisor!
    let _ = client_a.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Under OneForAll, actor 'b' MUST have been aborted and restarted to 'b2' as well!
    let val_a = client_a.get().await.unwrap();
    let val_b = client_b.get().await.unwrap();

    assert_eq!(val_a, "a2", "Actor A must restart to instance 2");
    assert_eq!(
        val_b, "b2",
        "Actor B MUST be aborted and restarted to instance 2 by OneForAll!"
    );
}

// Custom Struct Worker demonstrating that custom structs are fully supported
struct CustomStructWorker {
    prefix: String,
}

impl Worker<String, String> for CustomStructWorker {
    async fn execute(&self, ctx: WorkerContext) -> Result<TimedResource<String>, String> {
        Ok(TimedResource::perpetual(format!("{}_inst_{}", self.prefix, ctx.instance_id)))
    }
}

#[tokio::test]
async fn test_child_spec_with_custom_struct_worker() {
    let root = RootSupervisor::new();

    let custom_worker = CustomStructWorker {
        prefix: "db_token".to_string(),
    };

    let sup = ServiceSupervisor::builder("struct-service")
        .add_child(GenericResourceActor::builder("token-worker").worker(custom_worker))
        .spawn(&root)
        .await
        .expect("Failed to spawn struct supervisor");

    let client = sup
        .get_client::<String, String>("token-worker")
        .await
        .expect("Failed to get client");

    assert_eq!(client.get().await.unwrap(), "db_token_inst_1");

    // Simulate crash and verify struct worker generates inst 2
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(client.get().await.unwrap(), "db_token_inst_2");
}

#[tokio::test]
async fn test_child_spec_with_closure_capturing_env() {
    let root = RootSupervisor::new();

    // Environment state outside the actor
    let invocation_history = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let history_clone = invocation_history.clone();

    // Async closure capturing local environment from outside!
    let capturing_worker = move |ctx: WorkerContext| {
        let hist = history_clone.clone();
        async move {
            let mut guard = hist.lock().await;
            guard.push(ctx.instance_id);
            Ok::<_, String>(TimedResource::perpetual(format!("history_len_{}", guard.len())))
        }
    };

    let sup = ServiceSupervisor::builder("env-service")
        .add_child(GenericResourceActor::builder("env-worker").worker(capturing_worker))
        .spawn(&root)
        .await
        .expect("Failed to spawn env supervisor");

    let client = sup
        .get_client::<String, String>("env-worker")
        .await
        .expect("Failed to get client");

    assert_eq!(client.get().await.unwrap(), "history_len_1");

    // Crash and verify closure still captures the shared environment
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(client.get().await.unwrap(), "history_len_2");

    let history = invocation_history.lock().await.clone();
    assert_eq!(history, vec![1, 2]);
}

#[tokio::test]
async fn test_generation_guard_ignores_stale_refresh() {
    let root = RootSupervisor::new();

    // Worker where gen 1 is artificially slow (200ms) and gen 2 is fast (15ms)
    let worker = |ctx: WorkerContext| async move {
        if ctx.generation == 1 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, String>(TimedResource::perpetual("stale_gen_1".to_string()))
        } else {
            tokio::time::sleep(Duration::from_millis(15)).await;
            Ok::<_, String>(TimedResource::perpetual("fresh_gen_2".to_string()))
        }
    };

    let sup = ServiceSupervisor::builder("gen-service")
        .add_child(
            GenericResourceActor::builder("race-actor")
                .worker(worker)
                .eager_prewarm(true), // Starts generation 1 (slow, 200ms) immediately
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("race-actor")
        .await
        .expect("Failed to get client");

    // Wait 25ms: Gen 1 is currently in-flight
    tokio::time::sleep(Duration::from_millis(25)).await;

    // Trigger force_refresh -> starts generation 2 (fast, 15ms)
    client.force_refresh().await.expect("Force refresh failed");

    // Wait 60ms: Gen 2 finishes (at ~40ms total), cache becomes "fresh_gen_2"
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        client.get().await.unwrap(),
        "fresh_gen_2",
        "Cache should hold generation 2"
    );

    // Wait until 250ms total: Gen 1's 200ms sleep finishes and delivers "stale_gen_1"
    tokio::time::sleep(Duration::from_millis(180)).await;

    // The generation guard MUST have discarded Gen 1's result!
    // Cache must still hold "fresh_gen_2", NOT "stale_gen_1"!
    assert_eq!(
        client.get().await.unwrap(),
        "fresh_gen_2",
        "Generation guard failed: stale generation 1 overwrote generation 2!"
    );
}

#[tokio::test]
async fn test_actor_worker_timeout() {
    let root = RootSupervisor::new();

    let worker = |ctx: WorkerContext| async move {
        if ctx.generation == 1 {
            // First generation hangs / is too slow
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok::<_, String>(TimedResource::perpetual("slow_gen_1".to_string()))
        } else {
            // Second generation responds quickly
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<_, String>(TimedResource::perpetual("fast_gen_2".to_string()))
        }
    };

    let sup = ServiceSupervisor::builder("worker-timeout-service")
        .add_child(
            GenericResourceActor::builder("timeout-actor")
                .worker(worker)
                .worker_timeout(Duration::from_millis(60)),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("timeout-actor")
        .await
        .expect("Failed to get client");

    // Call 1 triggers generation 1 which should time out after 60ms
    let start = Instant::now();
    let res = client.get().await;
    let elapsed = start.elapsed();

    match res {
        Err(ResourceError::Timeout(d)) => {
            assert_eq!(d, Duration::from_millis(60));
            assert!(
                elapsed < Duration::from_millis(200),
                "Should have failed around 60ms, took {:?}",
                elapsed
            );
        }
        other => panic!("Expected ResourceError::Timeout, got: {:?}", other),
    }

    // Call 2 triggers generation 2 which succeeds quickly
    let val2 = client.get().await.expect("Call 2 should succeed");
    assert_eq!(val2, "fast_gen_2");
}

#[tokio::test]
async fn test_actor_overloaded_backpressure() {
    let root = RootSupervisor::new();

    // Slow worker so callers pile up in waiters queue
    let worker = |_ctx: WorkerContext| async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok::<_, String>(TimedResource::perpetual("ok".to_string()))
    };

    let sup = ServiceSupervisor::builder("overload-service")
        .add_child(
            GenericResourceActor::builder("overloaded-actor")
                .worker(worker)
                .max_waiters(2), // Only allow 2 callers waiting in queue!
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("overloaded-actor")
        .await
        .expect("Failed to get client");

    let c1 = client.clone();
    let c2 = client.clone();
    let c3 = client.clone();

    // Spawn 2 callers that will occupy the 2 slots in waiters queue
    let h1 = tokio::spawn(async move { c1.get().await });
    let h2 = tokio::spawn(async move { c2.get().await });

    // Allow 20ms for c1 and c2 to reach the actor's waiters queue
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Call 3 should be rejected immediately with Overloaded without waiting 200ms
    let start3 = Instant::now();
    let res3 = c3.get().await;
    let elapsed3 = start3.elapsed();

    assert!(
        elapsed3 < Duration::from_millis(50),
        "Overload should fast-fail without waiting, took {:?}",
        elapsed3
    );

    match res3 {
        Err(ResourceError::Overloaded(limit)) => {
            assert_eq!(limit, 2);
        }
        other => panic!("Expected ResourceError::Overloaded, got: {:?}", other),
    }

    // Wait for the first two to finish successfully
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    assert_eq!(r1.unwrap(), "ok");
    assert_eq!(r2.unwrap(), "ok");
}

#[tokio::test]
async fn test_dead_waiters_pruned() {
    let root = RootSupervisor::new();

    let worker = |_ctx: WorkerContext| async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok::<_, String>(TimedResource::perpetual("slow_val".to_string()))
    };

    let sup = ServiceSupervisor::builder("prune-service")
        .add_child(
            GenericResourceActor::builder("prune-actor")
                .worker(worker)
                .max_waiters(1), // Max 1 waiter!
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("prune-actor")
        .await
        .expect("Failed to get client");

    // Caller 1 uses client-side timeout of 20ms (drops its receiver when it times out)
    let _ = client.get_timeout(Duration::from_millis(20)).await;

    // Wait 30ms so caller 1's oneshot channel is definitely closed
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Now send another request with longer timeout.
    // If dead waiters were NOT pruned, queue length would be 1 and this call would be rejected with Overloaded!
    // Because dead waiters ARE pruned, it can wait and receive the value!
    let val = client
        .get_timeout(Duration::from_millis(400))
        .await
        .expect("Dead waiter should be pruned allowing new waiter slot");

    assert_eq!(val, "slow_val");
}

#[tokio::test]
async fn test_client_channel_rebind_during_slow_backoff() {
    let root = RootSupervisor::new();

    // Configure a slow initial backoff of 350ms on restart!
    let slow_policy = RestartPolicy {
        max_restarts: 3,
        time_window: Duration::from_secs(5),
        initial_backoff: Duration::from_millis(350),
        max_backoff: Duration::from_secs(1),
        backoff_multiplier: 2.0,
    };

    let worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("val_inst_{}", ctx.instance_id)))
    };

    let sup = ServiceSupervisor::builder("rebind-service")
        .add_child(
            GenericResourceActor::builder("rebind-actor")
                .worker(worker)
                .restart_policy(slow_policy),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("rebind-actor")
        .await
        .expect("Failed to get client");

    assert_eq!(client.get().await.unwrap(), "val_inst_1");

    // Crash the actor
    let _ = client.simulate_crash().await;

    // Immediately request the resource WITHOUT sleeping.
    // The actor is in its 350ms backoff!
    // Previously, client would retry 3x in ~120ms and fail with ActorUnavailable.
    // With parked resolutions, it cleanly waits for the supervisor to complete backoff and respawn!
    let start = Instant::now();
    let val = client.get().await.expect("Client should successfully rebind even during slow backoff");
    let elapsed = start.elapsed();

    assert_eq!(val, "val_inst_2");
    assert!(
        elapsed >= Duration::from_millis(300),
        "Should have waited for the 350ms backoff, elapsed: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_root_supervisor_crash_loop_protection() {
    let root = RootSupervisor::new();

    let strict_policy = RestartPolicy {
        max_restarts: 2,
        time_window: Duration::from_secs(3),
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(100),
        backoff_multiplier: 2.0,
    };

    let sup = ServiceSupervisor::builder("fragile-service")
        .restart_policy(strict_policy)
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    // Crash 1: Allowed (restart 1 of 2)
    sup.trigger_service_crash().await.expect("Crash 1 trigger failed");
    tokio::time::sleep(Duration::from_millis(80)).await;
    let sender1 = root.resolve_service_channel("fragile-service").await;
    assert!(sender1.is_ok(), "Service should recover from crash 1");

    // Crash 2: Allowed (restart 2 of 2)
    sup.trigger_service_crash().await.expect("Crash 2 trigger failed");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let sender2 = root.resolve_service_channel("fragile-service").await;
    assert!(sender2.is_ok(), "Service should recover from crash 2");

    // Crash 3: Exceeds threshold!
    sup.trigger_service_crash().await.expect("Crash 3 trigger failed");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let res = root.resolve_service_channel("fragile-service").await;
    match res {
        Err(RootError::MaxRestartsExceeded(name, max, _)) => {
            assert_eq!(name, "fragile-service");
            assert_eq!(max, 2);
        }
        other => panic!("Expected RootError::MaxRestartsExceeded, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_proactive_timer_refresh() {
    let root = RootSupervisor::new();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Worker returns resources with 100ms TTL.
    let worker = move |ctx: WorkerContext| {
        let c = counter_clone.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(TimedResource::with_ttl(
                format!("val_gen_{}", ctx.generation),
                Duration::from_millis(100),
            ))
        }
    };

    let sup = ServiceSupervisor::builder("proactive-service")
        .add_child(
            GenericResourceActor::builder("proactive-actor")
                .worker(worker)
                .eager_prewarm(true)
                .expiry_buffer(Duration::from_millis(40))
                .proactive_refresh(true),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("proactive-actor")
        .await
        .expect("Failed to get client");

    // Immediately after boot (eager prewarm done): should be gen 1
    tokio::time::sleep(Duration::from_millis(20)).await;
    let val1 = client.get().await.expect("Gen 1 fetch should succeed");
    assert_eq!(val1, "val_gen_1");
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // With 100ms TTL and 40ms buffer, target is at 60ms.
    // Wait until ~75ms: proactive timer should fire and fetch gen 2 without any caller request!
    tokio::time::sleep(Duration::from_millis(65)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "Proactive timer should have triggered generation 2 fetch in background"
    );

    // Call get() at 85ms: Caller receives gen 2 immediately without cold start or wait!
    let val2 = client.get().await.expect("Gen 2 fetch should succeed");
    assert_eq!(val2, "val_gen_2");
}

#[tokio::test]
async fn test_retry_storm_protection_and_negative_caching() {
    let root = RootSupervisor::new();

    let executions = Arc::new(AtomicUsize::new(0));
    let exec_clone = executions.clone();

    let failing_worker = move |_ctx: WorkerContext| {
        let e = exec_clone.clone();
        async move {
            e.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err::<TimedResource<String>, _>("upstream unavailable".to_string())
        }
    };

    let sup = ServiceSupervisor::builder("storm-protection-service")
        .add_child(
            GenericResourceActor::builder("storm-actor")
                .worker(failing_worker)
                .failure_cooldown(Duration::from_millis(150)),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("storm-actor")
        .await
        .expect("Failed to get client");

    // Call 1 triggers worker execution which fails
    let res1 = client.get().await;
    match res1 {
        Err(ResourceError::WorkerFailed(err)) => {
            assert_eq!(err, "upstream unavailable");
        }
        other => panic!("Expected WorkerFailed, got {:?}", other),
    }
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    // Now fire 20 concurrent get() requests during failure cooldown!
    let mut handles = Vec::new();
    for _ in 0..20 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.get().await
        }));
    }

    for h in handles {
        let res = h.await.unwrap();
        match res {
            Err(ResourceError::WorkerFailed(err)) => {
                assert_eq!(err, "upstream unavailable");
            }
            other => panic!("Expected fast-fail WorkerFailed during cooldown, got {:?}", other),
        }
    }

    // Crucial assertion: Executions count must STILL be 1! (Zero upstream retry storm!)
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "Worker must not be re-invoked during failure cooldown window"
    );

    // Wait until cooldown expires (150ms)
    tokio::time::sleep(Duration::from_millis(160)).await;

    // Next call after cooldown should re-attempt the worker
    let res2 = client.get().await;
    assert!(matches!(res2, Err(ResourceError::WorkerFailed(_))));
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "Worker should be invoked again after failure cooldown expires"
    );
}

#[tokio::test]
async fn test_stale_while_error_serves_expired_cache() {
    let root = RootSupervisor::new();

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    let worker = move |ctx: WorkerContext| {
        let a = attempts_clone.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // Initial fetch succeeds with a 40ms TTL
                Ok::<_, String>(TimedResource::with_ttl(
                    format!("initial_data_gen_{}", ctx.generation),
                    Duration::from_millis(40),
                ))
            } else {
                // Subsequent refreshes fail (outage)
                Err::<TimedResource<String>, _>("upstream outage".to_string())
            }
        }
    };

    let sup = ServiceSupervisor::builder("stale-cache-service")
        .add_child(
            GenericResourceActor::builder("stale-actor")
                .worker(worker)
                .eager_prewarm(true)
                .expiry_buffer(Duration::from_millis(10))
                .failure_cooldown(Duration::from_millis(80))
                .stale_while_error(Duration::from_millis(150))
                .proactive_refresh(false),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("stale-actor")
        .await
        .expect("Failed to get client");

    // Eager pre-warm completed
    tokio::time::sleep(Duration::from_millis(15)).await;
    let val1 = client.get().await.expect("Initial fetch must succeed");
    assert_eq!(val1, "initial_data_gen_1");

    // Wait until TTL expires (40ms elapsed => resource is expired)
    tokio::time::sleep(Duration::from_millis(35)).await;

    // Call get(): Resource is expired, but within stale_while_error window (150ms).
    // Client should receive the stale value while background worker fails!
    let val2 = client.get().await.expect("Stale value should be served during outage");
    assert_eq!(val2, "initial_data_gen_1");

    // Wait for the background worker to fail and set failure cooldown
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Subsequent call during outage: Still within stale_while_error window!
    let val3 = client.get().await.expect("Stale value should still be served");
    assert_eq!(val3, "initial_data_gen_1");

    // Wait until stale_while_error window expires (total elapsed > 40ms + 150ms = 190ms)
    tokio::time::sleep(Duration::from_millis(160)).await;

    // Now resource is completely expired past stale_while_error: Should fail!
    let res_final = client.get().await;
    assert!(
        matches!(res_final, Err(ResourceError::WorkerFailed(_))),
        "Expected WorkerFailed after stale_while_error grace window expired, got: {:?}",
        res_final
    );
}

#[tokio::test]
async fn test_force_refresh_bypasses_failure_cooldown() {
    let root = RootSupervisor::new();

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    let worker = move |ctx: WorkerContext| {
        let a = attempts_clone.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Err::<TimedResource<String>, _>("initial failure".to_string())
            } else {
                Ok::<_, String>(TimedResource::perpetual(format!("recovered_gen_{}", ctx.generation)))
            }
        }
    };

    let sup = ServiceSupervisor::builder("force-refresh-service")
        .add_child(
            GenericResourceActor::builder("force-actor")
                .worker(worker)
                .failure_cooldown(Duration::from_secs(10)), // Long cooldown
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("force-actor")
        .await
        .expect("Failed to get client");

    // Call 1 fails and enters 10-second failure cooldown
    let res1 = client.get().await;
    assert!(matches!(res1, Err(ResourceError::WorkerFailed(_))));

    // Normal get() fast-fails immediately
    let res2 = client.get().await;
    assert!(matches!(res2, Err(ResourceError::WorkerFailed(_))));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // Administrative ForceRefresh: Bypasses the 10-second cooldown immediately!
    client.force_refresh().await.expect("force_refresh failed");
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Next get() retrieves the successfully recovered resource
    let val = client.get().await.expect("Should succeed after force_refresh");
    assert_eq!(val, "recovered_gen_2");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn test_ttl_duration_max_no_panic() {
    let res = TimedResource::with_ttl("super_secret_token".to_string(), Duration::MAX);
    assert!(res.expires_at.is_none());
    assert!(res.is_still_usable());
    assert!(res.is_fresh(Duration::from_secs(3600)));

    let dbg_str = format!("{res:?}");
    assert!(
        dbg_str.contains("<redacted>") && !dbg_str.contains("super_secret_token"),
        "TimedResource Debug output must redact secret values: {dbg_str}"
    );
}

#[tokio::test]
async fn test_force_refresh_aborts_in_flight_worker() {
    let root = RootSupervisor::new();
    let was_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let was_cancelled_clone = was_cancelled.clone();

    struct DropGuard(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for DropGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let worker = move |ctx: WorkerContext| {
        let c = was_cancelled_clone.clone();
        async move {
            if ctx.generation == 1 {
                let _guard = DropGuard(c);
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok::<_, String>(TimedResource::perpetual("gen_1".to_string()))
            } else {
                Ok::<_, String>(TimedResource::perpetual("gen_2".to_string()))
            }
        }
    };

    let sup = ServiceSupervisor::builder("abort-service")
        .add_child(
            GenericResourceActor::builder("abort-actor")
                .worker(worker)
                .eager_prewarm(true),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("abort-actor")
        .await
        .expect("Failed to get client");

    // Gen 1 is currently in-flight
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!was_cancelled.load(Ordering::SeqCst));

    // Calling force_refresh must abort Gen 1!
    client.force_refresh().await.expect("Force refresh failed");
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert!(
        was_cancelled.load(Ordering::SeqCst),
        "Superseded worker task should have been aborted when force_refresh was triggered"
    );

    let val = client.get().await.expect("Fetch gen 2 should succeed");
    assert_eq!(val, "gen_2");
}

#[tokio::test]
async fn test_retry_storm_exponential_backoff() {
    let root = RootSupervisor::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    let failing_worker = move |_ctx: WorkerContext| {
        let a = attempts_clone.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            Err::<TimedResource<String>, _>("service unavailable".to_string())
        }
    };

    let sup = ServiceSupervisor::builder("backoff-service")
        .add_child(
            GenericResourceActor::builder("backoff-actor")
                .worker(failing_worker)
                .proactive_refresh(true),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    let client = sup
        .get_client::<String, String>("backoff-actor")
        .await
        .expect("Failed to get client");

    // First attempt fails immediately
    let _ = client.get().await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // With exponential backoff (100ms, then 200ms):
    // In 150ms, at most 1 retry (total <= 2 attempts)
    tokio::time::sleep(Duration::from_millis(150)).await;
    let count = attempts.load(Ordering::SeqCst);
    assert!(
        count <= 2,
        "Exponential backoff should rate-limit retries; expected <= 2 attempts, got {}",
        count
    );
}

#[tokio::test]
async fn test_root_supervisor_shutdown_aborts_entire_tree() {
    let root = RootSupervisor::new();

    let was_dropped = Arc::new(AtomicBool::new(false));
    let was_dropped_clone = was_dropped.clone();

    struct DropGuard(Arc<AtomicBool>);
    impl Drop for DropGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let guard = DropGuard(was_dropped_clone);

    let worker = move |_ctx: WorkerContext| {
        let _g = &guard;
        async move {
            Ok::<_, String>(TimedResource::with_ttl(
                "val".to_string(),
                Duration::from_secs(60),
            ))
        }
    };

    let sup = ServiceSupervisor::builder("shutdown-service")
        .add_child(GenericResourceActor::builder("shutdown-actor").worker(worker))
        .spawn(&root)
        .await
        .expect("Failed to spawn supervisor");

    // Supervisor and actor are running. DropGuard has not dropped.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!was_dropped.load(Ordering::SeqCst));

    // When all external handles to RootSupervisor and ServiceSupervisor are dropped,
    // RootSupervisor detects channel closure, exits its loop, and calls self.join_set.abort_all().
    // This aborts the ServiceSupervisor, whose ActiveActor drops and aborts the GenericResourceActor,
    // which in turn drops the worker closure and triggers the DropGuard!
    drop(sup);
    drop(root);

    // Give Tokio runtime a moment to abort the tasks and run Drop destructors
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        was_dropped.load(Ordering::SeqCst),
        "Dropping all root handles must abort the supervision hierarchy and drop all actors"
    );
}

#[tokio::test]
async fn test_restart_child_recovers_failed_actor() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("recover-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let policy = RestartPolicy {
        max_restarts: 2,
        time_window: Duration::from_secs(2),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(50),
        backoff_multiplier: 2.0,
    };

    let worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("val_inst_{}", ctx.instance_id)))
    };

    let client = GenericResourceActor::builder("crashing-actor")
        .worker(worker)
        .restart_policy(policy)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(client.get().await.unwrap(), "val_inst_1");

    // Crash 1: Allowed (restart 1)
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(client.get().await.unwrap(), "val_inst_2");

    // Crash 2: Allowed (restart 2)
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.get().await.unwrap(), "val_inst_3");

    // Crash 3: Exceeds threshold -> permanently failed
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Direct access must fail
    assert!(client.get().await.is_err());

    // Operator intervention: Manual restart via ServiceSupervisor
    service_sup
        .restart_child("crashing-actor")
        .await
        .expect("Manual restart_child should succeed");

    // Client transparently rebinds and gets fresh instance #4!
    let recovered = client.get().await.expect("Recovered actor should answer requests");
    assert_eq!(recovered, "val_inst_4");
}

#[tokio::test]
async fn test_escalate_on_failure_triggers_root_supervisor_restart() {
    let root = RootSupervisor::new();

    let child_policy = RestartPolicy {
        max_restarts: 1, // Only 1 restart before escalating
        time_window: Duration::from_secs(2),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(20),
        backoff_multiplier: 2.0,
    };

    let worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!("inst_{}", ctx.instance_id)))
    };

    let service_sup = ServiceSupervisor::builder("escalating-svc")
        .escalate_on_failure(true)
        .add_child(
            GenericResourceActor::builder("child-actor")
                .worker(worker)
                .restart_policy(child_policy),
        )
        .spawn(&root)
        .await
        .expect("Spawn failed");

    let client = service_sup
        .get_client::<String, String>("child-actor")
        .await
        .expect("Get client failed");

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(client.get().await.unwrap(), "inst_1");

    // Crash 1: restarts child under instance 2
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(client.get().await.unwrap(), "inst_2");

    // Crash 2: Exceeds max_restarts(1). Because escalate_on_failure=true,
    // the ServiceSupervisor panics and escalates to RootSupervisor!
    let _ = client.simulate_crash().await;

    // RootSupervisor catches the crash and restarts the ServiceSupervisor (new supervisor instance #2)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client should transparently rebind via RootSupervisor to the rebooted ServiceSupervisor
    let rebooted_val = client.get().await.expect("Rebound request after escalation must succeed");
    assert_eq!(rebooted_val, "inst_1"); // Reset back to inst 1 under the new supervisor instance!
}

#[tokio::test]
async fn test_restart_service_recovers_failed_supervisor() {
    let root = RootSupervisor::new();

    let service_policy = RestartPolicy {
        max_restarts: 1,
        time_window: Duration::from_secs(2),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(20),
        backoff_multiplier: 2.0,
    };

    let _sup = ServiceSupervisor::builder("fragile-root-svc")
        .restart_policy(service_policy)
        .spawn(&root)
        .await
        .expect("Spawn failed");

    // Crash 1: Allowed
    root.trigger_service_crash("fragile-root-svc").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(root.resolve_service_channel("fragile-root-svc").await.is_ok());

    // Crash 2: Exceeds limit -> root marks service permanently failed
    root.trigger_service_crash("fragile-root-svc").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    match root.resolve_service_channel("fragile-root-svc").await {
        Err(RootError::MaxRestartsExceeded(name, max, _)) => {
            assert_eq!(name, "fragile-root-svc");
            assert_eq!(max, 1);
        }
        other => panic!("Expected MaxRestartsExceeded, got: {:?}", other),
    }

    // Manual operator recovery: restart_service
    root.restart_service("fragile-root-svc")
        .await
        .expect("Manual restart_service failed");

    // Service is restored and channel can be resolved again!
    let channel_res = root.resolve_service_channel("fragile-root-svc").await;
    assert!(
        channel_res.is_ok(),
        "Resolved channel must be Ok after manual restart_service"
    );
}

#[tokio::test]
async fn test_max_consecutive_failures_escalates_without_panic() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register_with_strategy(
        "result-crash-service",
        SupervisionStrategy::OneForAll,
        &root,
    )
    .await
    .expect("ServiceSupervisor registration failed");

    // Worker fails via Result::Err on instance 1, but succeeds on instance 2 (zero panics!)
    let worker_a = |ctx: WorkerContext| async move {
        if ctx.instance_id == 1 {
            return Err("Fatal backend error on instance 1".to_string());
        }
        Ok(TimedResource::perpetual(format!(
            "recovered_a_inst_{}",
            ctx.instance_id
        )))
    };

    let worker_b = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "sibling_b_inst_{}",
            ctx.instance_id
        )))
    };

    let client_a = GenericResourceActor::builder("actor-a")
        .worker(worker_a)
        .max_consecutive_failures(1) // Escalate to supervisor after 1 worker error!
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor A spawn failed");

    let client_b = GenericResourceActor::builder("actor-b")
        .worker(worker_b)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor B spawn failed");

    // Sibling B starts at instance 1
    assert_eq!(client_b.get().await.unwrap(), "sibling_b_inst_1");

    // Calling client_a.get() triggers worker_a on instance 1 -> returns Err ->
    // actor-a exits with Err(TaskResult) -> OneForAll restarts both actor-a and actor-b to instance 2 ->
    // client_a transparently rebinds and returns instance 2's value!
    let val_a = client_a
        .get()
        .await
        .expect("Client A should transparently recover via Result-based supervision");
    assert_eq!(val_a, "recovered_a_inst_2");

    // Wait briefly for OneForAll sibling restart to complete if needed
    tokio::time::sleep(Duration::from_millis(50)).await;
    let val_b = client_b.get().await.expect("Client B should recover");
    assert_eq!(val_b, "sibling_b_inst_2");
}

// ============================================================================
// RestartType (Permanent, Transient, Temporary) & Graceful Shutdown Tests
// ============================================================================

#[tokio::test]
async fn test_restart_type_permanent_restarts_on_normal_exit() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("perm-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "perm_inst_{}",
            ctx.instance_id
        )))
    };

    let client = GenericResourceActor::builder("perm-actor")
        .worker(worker)
        .restart_type(RestartType::Permanent)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    assert_eq!(client.get().await.unwrap(), "perm_inst_1");

    // Request a clean shutdown directly on the actor (causes actor.run() to exit with Ok(())).
    // Because RestartType is Permanent, the supervisor MUST restart it to instance 2!
    client.shutdown().await.expect("Direct shutdown command failed");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let val2 = client
        .get()
        .await
        .expect("Permanent actor must be restarted after normal exit");
    assert_eq!(val2, "perm_inst_2");
}

#[tokio::test]
async fn test_restart_type_transient_stops_on_normal_exit_but_restarts_on_crash() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("trans-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "trans_inst_{}",
            ctx.instance_id
        )))
    };

    let client = GenericResourceActor::builder("trans-actor")
        .worker(worker)
        .restart_type(RestartType::Transient)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    assert_eq!(client.get().await.unwrap(), "trans_inst_1");

    // 1. Abnormal crash: Transient actor MUST be restarted to instance 2
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(client.get().await.unwrap(), "trans_inst_2");

    // 2. Clean exit via shutdown(): Transient actor must NOT be restarted
    client.shutdown().await.expect("Direct shutdown command failed");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let res = service_sup
        .resolve_actor_channel_untyped("trans-actor")
        .await;
    assert!(
        matches!(res, Err(ServiceError::ActorNotFound(_))),
        "Expected ActorNotFound after clean exit of Transient actor"
    );
}

#[tokio::test]
async fn test_restart_type_temporary_never_restarts_even_on_crash() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("temp-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "temp_inst_{}",
            ctx.instance_id
        )))
    };

    let client = GenericResourceActor::builder("temp-actor")
        .worker(worker)
        .restart_type(RestartType::Temporary)
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    assert_eq!(client.get().await.unwrap(), "temp_inst_1");

    // Crash the Temporary actor: It must NEVER be restarted
    let _ = client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(80)).await;

    let res = service_sup
        .resolve_actor_channel_untyped("temp-actor")
        .await;
    assert!(
        matches!(res, Err(ServiceError::ActorNotFound(_))),
        "Expected ActorNotFound after crash of Temporary actor"
    );
}

#[tokio::test]
async fn test_stop_child_completes_in_flight_worker_gracefully() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("graceful-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    // Worker takes 120ms to produce resource
    let worker = |_ctx: WorkerContext| async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        Ok::<_, String>(TimedResource::perpetual("in_flight_completed".to_string()))
    };

    // Configure actor as Permanent with a 500ms graceful shutdown timeout
    let client = GenericResourceActor::builder("graceful-actor")
        .worker(worker)
        .restart_type(RestartType::Permanent)
        .shutdown_timeout(Duration::from_millis(500))
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    // Start an in-flight get() request
    let client_for_call = client.clone();
    let get_handle = tokio::spawn(async move { client_for_call.get().await });

    // Wait 25ms so the request is inside the actor and the 120ms worker is in-flight
    tokio::time::sleep(Duration::from_millis(25)).await;

    // Explicitly stop the child via supervisor (even though it is Permanent, explicit stop must not restart!)
    service_sup
        .stop_child("graceful-actor")
        .await
        .expect("stop_child should succeed");

    // The waiting caller MUST have received the completed result before the actor exited!
    let caller_res = get_handle.await.unwrap();
    assert_eq!(
        caller_res.expect("In-flight request should complete during graceful shutdown"),
        "in_flight_completed"
    );

    // And the Permanent actor must remain stopped (not restarted) after explicit stop_child
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        matches!(
            service_sup.resolve_actor_channel_untyped("graceful-actor").await,
            Err(ServiceError::ActorNotFound(_))
        ),
        "Explicitly stopped Permanent actor must not auto-restart"
    );
}

#[tokio::test]
async fn test_shutdown_policy_timeout_aborts_hung_actor() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("hung-shutdown-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    // Worker hangs for 10 seconds
    let worker = |_ctx: WorkerContext| async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok::<_, String>(TimedResource::perpetual("never".to_string()))
    };

    let _client = GenericResourceActor::builder("hung-actor")
        .worker(worker)
        .eager_prewarm(true)
        .shutdown_policy(ShutdownPolicy::Timeout(Duration::from_millis(80)))
        .spawn_and_register(&service_sup, &root)
        .await
        .expect("Actor spawn failed");

    // Allow eager prewarm to start the 10-second worker
    tokio::time::sleep(Duration::from_millis(20)).await;

    // stop_child should wait ~80ms for graceful shutdown, then forcefully abort the hung actor
    let start = Instant::now();
    service_sup
        .stop_child("hung-actor")
        .await
        .expect("stop_child should succeed after timeout abort");
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(65) && elapsed < Duration::from_millis(400),
        "Expected shutdown timeout around 80ms before forceful abort, took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_root_supervisor_graceful_shutdown_cascade() {
    let root = RootSupervisor::new();

    let completed = Arc::new(AtomicBool::new(false));
    let completed_clone = completed.clone();

    let worker = move |_ctx: WorkerContext| {
        let c = completed_clone.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c.store(true, Ordering::SeqCst);
            Ok::<_, String>(TimedResource::perpetual("done".to_string()))
        }
    };

    let sup = ServiceSupervisor::builder("cascade-service")
        .shutdown_timeout(Duration::from_secs(2))
        .add_child(
            GenericResourceActor::builder("cascade-actor")
                .worker(worker)
                .eager_prewarm(true)
                .shutdown_timeout(Duration::from_secs(1)),
        )
        .spawn(&root)
        .await
        .expect("Failed to spawn service");

    // Wait 20ms so the 100ms pre-warm worker is actively in-flight
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!completed.load(Ordering::SeqCst));

    // Trigger full tree graceful shutdown on RootSupervisor
    root.shutdown()
        .await
        .expect("Root graceful shutdown should succeed");

    // The in-flight worker inside cascade-actor must have been allowed to complete before exit!
    assert!(
        completed.load(Ordering::SeqCst),
        "Graceful root shutdown must allow in-flight child worker to complete within timeout"
    );

    // Subsequent operations on the shut-down supervisor return unavailable
    assert!(sup.resolve_actor_channel_untyped("cascade-actor").await.is_err());
}

// ============================================================================
// StatefulActor Integration Tests
// ============================================================================

#[derive(Clone, Default)]
struct CounterActor {
    total: i64,
}

#[derive(Clone, Debug)]
enum CounterMsg {
    Add(i64),
    GetWithInstance,
    TriggerFatalError,
}

impl StatefulActor for CounterActor {
    type Message = CounterMsg;
    type Response = (i64, usize);
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            CounterMsg::Add(delta) => {
                self.total += delta;
                Ok((self.total, ctx.instance_id()))
            }
            CounterMsg::GetWithInstance => Ok((self.total, ctx.instance_id())),
            CounterMsg::TriggerFatalError => {
                ctx.fail("Fatal state corruption in CounterActor");
                Err("corrupted".to_string())
            }
        }
    }
}

#[tokio::test]
async fn test_stateful_actor_call_and_cast_with_crash_recovery() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("stateful-actor-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let client = StatefulActorBuilder::from_default::<CounterActor>("session-counter")
        .restart_type(RestartType::Permanent)
        .spawn(&service_sup)
        .await
        .expect("Failed to spawn StatefulActor");

    // 1. Fire-and-forget cast()
    client.cast(CounterMsg::Add(10)).await.unwrap();
    client.cast(CounterMsg::Add(5)).await.unwrap();

    // 2. Request-response call()
    let (total, inst) = client.call(CounterMsg::Add(7)).await.unwrap();
    assert_eq!(total, 22);
    assert_eq!(inst, 1);

    // 3. Trigger fatal failure inside handler via ctx.fail(...)
    let _ = client.call(CounterMsg::TriggerFatalError).await;

    // Wait briefly for supervisor restart; ActorClient must transparently rebind to instance #2!
    let (new_total, new_inst) = client.call(CounterMsg::Add(100)).await.unwrap();
    assert_eq!(new_total, 100, "Fresh instance starts with total = 0 + 100");
    assert_eq!(new_inst, 2, "Supervisor must have bumped instance_id to 2");
}

struct LifecycleActor {
    started_instances: Arc<tokio::sync::Mutex<Vec<usize>>>,
    stopped_count: Arc<AtomicUsize>,
    processed: usize,
}

impl StatefulActor for LifecycleActor {
    type Message = usize;
    type Response = usize;
    type Error = String;

    async fn on_start(&mut self, ctx: &mut ActorContext) -> Result<(), String> {
        self.started_instances.lock().await.push(ctx.instance_id());
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: Self::Message,
        _ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        self.processed += msg;
        Ok(self.processed)
    }

    async fn on_stop(&mut self, _ctx: &mut ActorContext) {
        self.stopped_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn test_stateful_actor_lifecycle_hooks_on_start_and_on_stop() {
    let root = RootSupervisor::new();
    let service_sup = ServiceSupervisor::spawn_and_register("lifecycle-service", &root)
        .await
        .expect("ServiceSupervisor registration failed");

    let started = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let stopped = Arc::new(AtomicUsize::new(0));

    let started_clone = started.clone();
    let stopped_clone = stopped.clone();

    let client = StatefulActorBuilder::new("lifecycle-actor", move |_inst| LifecycleActor {
        started_instances: started_clone.clone(),
        stopped_count: stopped_clone.clone(),
        processed: 0,
    })
    .spawn(&service_sup)
    .await
    .expect("Failed to spawn LifecycleActor");

    assert_eq!(client.call(42).await.unwrap(), 42);
    assert_eq!(*started.lock().await, vec![1]);
    assert_eq!(stopped.load(Ordering::SeqCst), 0);

    // Gracefully stop the child via ServiceSupervisor -> must invoke on_stop()!
    service_sup
        .stop_child("lifecycle-actor")
        .await
        .expect("stop_child failed");

    assert_eq!(
        stopped.load(Ordering::SeqCst),
        1,
        "on_stop hook must be called during graceful shutdown"
    );
}

#[tokio::test]
async fn test_mixed_supervision_resource_actor_and_stateful_actor_one_for_all() {
    let root = RootSupervisor::new();

    let token_worker = |ctx: WorkerContext| async move {
        Ok::<_, String>(TimedResource::perpetual(format!(
            "token_inst_{}",
            ctx.instance_id
        )))
    };

    // Register BOTH a GenericResourceActor and a StatefulActor in the SAME OneForAll ServiceSupervisor!
    let sup = ServiceSupervisor::builder("mixed-pipeline-service")
        .strategy(SupervisionStrategy::OneForAll)
        .add_child(GenericResourceActor::builder("auth-token").worker(token_worker))
        .add_child(StatefulActorBuilder::from_default::<CounterActor>("order-counter"))
        .spawn(&root)
        .await
        .expect("Failed to spawn mixed supervisor");

    let token_client = sup
        .get_client::<String, String>("auth-token")
        .await
        .expect("Failed to get ResourceClient");
    let counter_client = sup
        .get_actor_client::<CounterActor>("order-counter")
        .await
        .expect("Failed to get ActorClient");

    assert_eq!(token_client.get().await.unwrap(), "token_inst_1");
    assert_eq!(
        counter_client.call(CounterMsg::Add(5)).await.unwrap(),
        (5, 1)
    );

    // Crash the StatefulActor -> OneForAll must restart BOTH the StatefulActor AND the GenericResourceActor!
    let _ = counter_client.simulate_crash().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        token_client.get().await.unwrap(),
        "token_inst_2",
        "GenericResourceActor must be restarted to inst 2 when sibling StatefulActor crashes under OneForAll"
    );
    assert_eq!(
        counter_client.call(CounterMsg::GetWithInstance).await.unwrap(),
        (0, 2),
        "StatefulActor must be restarted to inst 2 with fresh state"
    );
}

#[tokio::test]
async fn test_ttl_shorter_than_expiry_buffer_does_not_storm_refresh_on_get() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("short-ttl-service", &root)
        .await
        .expect("Failed to spawn service supervisor");

    let call_count = Arc::new(AtomicUsize::new(0));
    let c = call_count.clone();

    let client = GenericResourceActor::builder("short-ttl-actor")
        .worker(move |_ctx: WorkerContext| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(TimedResource::with_ttl(
                    "token-val".to_string(),
                    Duration::from_millis(800),
                ))
            }
        })
        .expiry_buffer(Duration::from_secs(2)) // buffer > ttl!
        .proactive_refresh(false)
        .spawn(&sup)
        .await
        .expect("Failed to spawn actor");

    // Initial fetch (cold start) -> 1 execution
    let val = client.get().await.unwrap();
    assert_eq!(val, "token-val");
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Call get() 30 times over 150ms (well within the 400ms half-TTL fresh window)
    for _ in 0..30 {
        let v = client.get().await.unwrap();
        assert_eq!(v, "token-val");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Must still be exactly 1 call (cache hit, no refresh storm on every get)
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        1,
        "get() must serve fresh cache without triggering a refresh storm when TTL < expiry_buffer"
    );

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_zero_channel_capacity_and_spec_poisoning_protection() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("cap-service", &root)
        .await
        .expect("Failed to spawn service supervisor");

    // 1. Clamping: channel_capacity(0) must not panic and must spawn successfully
    let client = GenericResourceActor::builder("cap-zero-actor")
        .worker(|_ctx: WorkerContext| async {
            Ok::<_, String>(TimedResource::perpetual("ok".to_string()))
        })
        .channel_capacity(0)
        .spawn(&sup)
        .await
        .expect("channel_capacity(0) must be clamped to at least 1 and not panic");

    assert_eq!(client.get().await.unwrap(), "ok");

    // 2. Custom spec whose factory panics synchronously must not crash the ServiceSupervisor
    let panicking_spec = ChildSpec::new(
        "panicking-spec",
        Arc::new(|_inst| panic!("Synchronous factory failure")),
        RestartPolicy::default(),
    );

    let start_res = sup.start_child(panicking_spec).await;
    assert!(
        start_res.is_err(),
        "Panicking factory must return error during start_child"
    );

    // Supervisor must remain alive and able to spawn healthy children
    let healthy_client = GenericResourceActor::builder("healthy-after-panic")
        .worker(|_ctx: WorkerContext| async {
            Ok::<_, String>(TimedResource::perpetual("healthy".to_string()))
        })
        .spawn(&sup)
        .await
        .expect("ServiceSupervisor must remain healthy after catching child factory panic");

    assert_eq!(healthy_client.get().await.unwrap(), "healthy");

    let _ = root.shutdown().await;
}

#[derive(Clone, Default)]
struct StopDrainActor {
    handled_count: Arc<AtomicUsize>,
}

impl StatefulActor for StopDrainActor {
    type Message = usize;
    type Response = usize;
    type Error = String;

    async fn handle(&mut self, msg: Self::Message, ctx: &mut ActorContext) -> Result<Self::Response, Self::Error> {
        self.handled_count.fetch_add(1, Ordering::SeqCst);
        if msg == 0 {
            // Give time for concurrent callers to buffer in the mailbox before initiating stop
            tokio::time::sleep(Duration::from_millis(60)).await;
            ctx.stop();
        }
        Ok(msg)
    }
}

#[tokio::test]
async fn test_stateful_actor_ctx_stop_drains_all_queued_messages() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("drain-service", &root)
        .await
        .expect("Failed to spawn service supervisor");

    let handled_count = Arc::new(AtomicUsize::new(0));
    let h = handled_count.clone();

    let client = StatefulActorBuilder::new("stop-drain-actor", move |_inst| StopDrainActor {
        handled_count: h.clone(),
    })
    .channel_capacity(64)
    .spawn(&sup)
    .await
    .expect("Failed to spawn StatefulActor");

    // Start first call which will invoke ctx.stop() after 60ms
    let c0 = client.clone();
    let first = tokio::spawn(async move { c0.call(0).await });

    // Allow the first call to enter handle()
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Queue 5 more calls in the mailbox while msg 0 is still executing
    let mut tasks = Vec::new();
    for i in 1..=5 {
        let c = client.clone();
        tasks.push(tokio::spawn(async move { c.call(i).await }));
    }

    assert_eq!(first.await.unwrap().unwrap(), 0);

    // All 5 queued calls must complete cleanly during graceful drain
    for (idx, task) in tasks.into_iter().enumerate() {
        let res = task.await.unwrap();
        assert_eq!(
            res.unwrap(),
            idx + 1,
            "Queued message must not be dropped when actor shuts down via ctx.stop()"
        );
    }

    assert_eq!(
        handled_count.load(Ordering::SeqCst),
        6,
        "All 6 messages (initial + 5 queued) must be handled during drain"
    );

    let _ = root.shutdown().await;
}

struct ExclusiveResourceSiblingActor {
    instance_id: usize,
    active_instances: Arc<AtomicUsize>,
    overlap_detected: Arc<AtomicBool>,
    events: Arc<std::sync::Mutex<Vec<String>>>,
}

impl StatefulActor for ExclusiveResourceSiblingActor {
    type Message = bool; // true = panic, false = ping
    type Response = usize;
    type Error = String;

    async fn on_start(&mut self, _ctx: &mut ActorContext) -> Result<(), Self::Error> {
        let prev = self.active_instances.fetch_add(1, Ordering::SeqCst);
        if prev > 0 {
            self.overlap_detected.store(true, Ordering::SeqCst);
        }
        self.events
            .lock()
            .unwrap()
            .push(format!("start_{}", self.instance_id));
        Ok(())
    }

    async fn handle(&mut self, should_panic: Self::Message, _ctx: &mut ActorContext) -> Result<Self::Response, Self::Error> {
        if should_panic {
            panic!("intentional crash");
        }
        Ok(self.instance_id)
    }

    async fn on_stop(&mut self, _ctx: &mut ActorContext) {
        // Simulate releasing an exclusive resource (e.g. flushing WAL / closing lock file)
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.events
            .lock()
            .unwrap()
            .push(format!("stop_{}", self.instance_id));
        self.active_instances.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn test_one_for_all_and_restart_child_graceful_on_stop_without_overlap() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::builder("ofa-graceful-sup")
        .strategy(SupervisionStrategy::OneForAll)
        .spawn(&root)
        .await
        .expect("Failed to spawn service supervisor");

    let active_b = Arc::new(AtomicUsize::new(0));
    let overlap_b = Arc::new(AtomicBool::new(false));
    let events_b = Arc::new(std::sync::Mutex::new(Vec::new()));

    let client_a = StatefulActorBuilder::new("actor-a", move |inst| ExclusiveResourceSiblingActor {
        instance_id: inst,
        active_instances: Arc::new(AtomicUsize::new(0)),
        overlap_detected: Arc::new(AtomicBool::new(false)),
        events: Arc::new(std::sync::Mutex::new(Vec::new())),
    })
    .restart_policy(RestartPolicy {
        initial_backoff: Duration::ZERO,
        ..Default::default()
    })
    .spawn(&sup)
    .await
    .expect("Failed to spawn actor-a");

    let ab = active_b.clone();
    let ob = overlap_b.clone();
    let eb = events_b.clone();
    let client_b = StatefulActorBuilder::new("actor-b", move |inst| ExclusiveResourceSiblingActor {
        instance_id: inst,
        active_instances: ab.clone(),
        overlap_detected: ob.clone(),
        events: eb.clone(),
    })
    .restart_policy(RestartPolicy {
        initial_backoff: Duration::ZERO,
        ..Default::default()
    })
    .shutdown_policy(ShutdownPolicy::Timeout(Duration::from_secs(2)))
    .spawn(&sup)
    .await
    .expect("Failed to spawn actor-b");

    assert_eq!(client_a.call(false).await.unwrap(), 1);
    assert_eq!(client_b.call(false).await.unwrap(), 1);

    // 1. Crash actor-a; under OneForAll, sibling actor-b must gracefully run on_stop()
    //    BEFORE instance 2 of actor-b starts, even with initial_backoff = 0.
    let _ = client_a.call(true).await;

    // Wait for actor-b instance 2 to become responsive
    let mut inst_b = 1;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        if let Ok(id) = client_b.call(false).await
            && id == 2
        {
            inst_b = id;
            break;
        }
    }
    assert_eq!(inst_b, 2, "Sibling actor-b should have restarted to instance 2");
    assert!(
        !overlap_b.load(Ordering::SeqCst),
        "Old and new instances of actor-b must never overlap concurrently"
    );

    // 2. Also verify manual restart_child honors ShutdownPolicy and waits for on_stop()
    sup.restart_child("actor-b")
        .await
        .expect("Failed to manually restart actor-b");

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        if let Ok(id) = client_b.call(false).await
            && id == 3
        {
            inst_b = id;
            break;
        }
    }
    assert_eq!(inst_b, 3, "Actor-b should have restarted to instance 3");
    assert!(
        !overlap_b.load(Ordering::SeqCst),
        "Old and new instances of actor-b must not overlap during manual restart_child"
    );

    let recorded = events_b.lock().unwrap().clone();
    assert_eq!(
        recorded,
        vec!["start_1", "stop_1", "start_2", "stop_2", "start_3"],
        "Lifecycle hooks must strictly serialize stop of old instance before start of new instance"
    );

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_duplicate_service_supervisor_does_not_boot_children() {
    let root = RootSupervisor::new();
    let _sup1 = ServiceSupervisor::spawn_and_register("dup-svc", &root)
        .await
        .expect("First registration should succeed");

    let child_booted = Arc::new(AtomicUsize::new(0));
    let cb = child_booted.clone();

    let spec = GenericResourceActor::builder("side-effect-child")
        .worker(move |_ctx: WorkerContext| {
            let cb = cb.clone();
            async move {
                cb.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(TimedResource::perpetual("ok".to_string()))
            }
        })
        .eager_prewarm(true)
        .build_child_spec();

    let dup_res = ServiceSupervisor::builder("dup-svc")
        .add_child(spec)
        .spawn(&root)
        .await;
    assert!(
        matches!(dup_res, Err(RootError::ServiceAlreadyExists(_))),
        "Duplicate service name must be rejected by RootSupervisor"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        child_booted.load(Ordering::SeqCst),
        0,
        "Rejected duplicate ServiceSupervisor must never boot its children or run eager_prewarm"
    );

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_stopped_and_deleted_children_do_not_resurrect_on_supervisor_restart() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::builder("lifecycle-svc")
        .restart_policy(RestartPolicy {
            initial_backoff: Duration::from_millis(10),
            ..Default::default()
        })
        .spawn(&root)
        .await
        .expect("Failed to spawn service supervisor");

    let _c1 = GenericResourceActor::builder("stopped-child")
        .worker(|ctx: WorkerContext| async move {
            Ok::<_, String>(TimedResource::perpetual(format!("inst_{}", ctx.instance_id)))
        })
        .spawn_and_register(&sup, &root)
        .await
        .expect("Failed to spawn stopped-child");

    let _c2 = GenericResourceActor::builder("deleted-child")
        .worker(|ctx: WorkerContext| async move {
            Ok::<_, String>(TimedResource::perpetual(format!("inst_{}", ctx.instance_id)))
        })
        .spawn_and_register(&sup, &root)
        .await
        .expect("Failed to spawn deleted-child");

    let c3 = GenericResourceActor::builder("active-child")
        .worker(|ctx: WorkerContext| async move {
            Ok::<_, String>(TimedResource::perpetual(format!("inst_{}", ctx.instance_id)))
        })
        .spawn_and_register(&sup, &root)
        .await
        .expect("Failed to spawn active-child");

    // Explicitly stop "stopped-child" and delete "deleted-child"
    sup.stop_child("stopped-child").await.unwrap();
    sup.delete_child("deleted-child").await.unwrap();

    // Crash the ServiceSupervisor so RootSupervisor respawns it from shared child_specs
    sup.trigger_service_crash().await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Active child must be running on the respawned supervisor
    assert_eq!(c3.get().await.unwrap(), "inst_1");

    // Neither the stopped child nor the deleted child may resurrect!
    assert!(
        matches!(
            sup.resolve_actor_channel_untyped("stopped-child").await,
            Err(ServiceError::ActorNotFound(_))
        ),
        "Stopped child must not resurrect after ServiceSupervisor restart"
    );
    assert!(
        matches!(
            sup.resolve_actor_channel_untyped("deleted-child").await,
            Err(ServiceError::ActorNotFound(_))
        ),
        "Deleted child must not resurrect after ServiceSupervisor restart"
    );

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_client_control_commands_rebind_after_crash() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("rebind-ctrl-svc", &root)
        .await
        .expect("Failed to spawn service supervisor");

    let gen_counter = Arc::new(AtomicUsize::new(0));
    let gc = gen_counter.clone();
    let res_client = GenericResourceActor::builder("res-ctrl-actor")
        .worker(move |_ctx: WorkerContext| {
            let gc = gc.clone();
            async move {
                let n = gc.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, String>(TimedResource::perpetual(n))
            }
        })
        .restart_policy(RestartPolicy {
            initial_backoff: Duration::from_millis(10),
            ..Default::default()
        })
        .spawn_and_register(&sup, &root)
        .await
        .expect("Failed to spawn resource actor");

    assert_eq!(res_client.get().await.unwrap(), 1);

    // Crash instance 1; do NOT call get() first, instead call force_refresh() directly!
    res_client.simulate_crash().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    res_client
        .force_refresh()
        .await
        .expect("force_refresh must transparently rebind after crash");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(res_client.get().await.unwrap(), 2);

    // Crash instance 2; then call shutdown() directly without calling get() first!
    res_client.simulate_crash().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    res_client
        .shutdown()
        .await
        .expect("shutdown must transparently rebind after crash");

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_large_jitter_and_zero_ttl_do_not_cause_refresh_storm() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("h2-storm-svc", &root)
        .await
        .expect("Failed to spawn service supervisor");

    // 1. Large jitter (1 hour) vs 2s TTL: must clamp jitter to at most half the lead window
    let jitter_calls = Arc::new(AtomicUsize::new(0));
    let jc = jitter_calls.clone();
    let _c_jitter = GenericResourceActor::builder("large-jitter-actor")
        .worker(move |_ctx: WorkerContext| {
            let jc = jc.clone();
            async move {
                jc.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(TimedResource::with_ttl("val", Duration::from_secs(2)))
            }
        })
        .expiry_buffer(Duration::from_millis(500))
        .proactive_refresh(true)
        .proactive_jitter(Duration::from_secs(3600))
        .eager_prewarm(true)
        .spawn_and_register(&sup, &root)
        .await
        .expect("Failed to spawn large-jitter-actor");

    // 2. Zero-TTL resource: must exponentially back off instead of polling at 20 Hz
    let zero_calls = Arc::new(AtomicUsize::new(0));
    let zc = zero_calls.clone();
    let _c_zero = GenericResourceActor::builder("zero-ttl-actor")
        .worker(move |_ctx: WorkerContext| {
            let zc = zc.clone();
            async move {
                zc.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(TimedResource::with_ttl("val", Duration::ZERO))
            }
        })
        .proactive_refresh(true)
        .eager_prewarm(true)
        .spawn_and_register(&sup, &root)
        .await
        .expect("Failed to spawn zero-ttl-actor");

    tokio::time::sleep(Duration::from_millis(350)).await;

    assert_eq!(
        jitter_calls.load(Ordering::SeqCst),
        1,
        "Large jitter must not collapse proactive refresh interval into a 50ms tight loop"
    );
    assert_eq!(
        zero_calls.load(Ordering::SeqCst),
        1,
        "Zero-TTL resource must back off >= 500ms instead of hammering every 50ms"
    );

    let _ = root.shutdown().await;
}

struct HalfMutatedTimeoutActor {
    instance_id: usize,
    corrupted: bool,
}

impl StatefulActor for HalfMutatedTimeoutActor {
    type Message = Duration; // sleep duration inside handle
    type Response = (usize, bool);
    type Error = String;

    async fn handle(
        &mut self,
        sleep_dur: Self::Message,
        _ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        if !sleep_dur.is_zero() {
            // Mutate state BEFORE awaiting a slow operation that will time out!
            self.corrupted = true;
            tokio::time::sleep(sleep_dur).await;
            self.corrupted = false;
        }
        Ok((self.instance_id, self.corrupted))
    }
}

#[tokio::test]
async fn test_stateful_actor_handler_timeout_fails_fast_and_restarts_cleanly() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("m2-timeout-svc", &root)
        .await
        .expect("Failed to spawn service supervisor");

    let client = StatefulActorBuilder::new("timeout-actor", |inst| HalfMutatedTimeoutActor {
        instance_id: inst,
        corrupted: false,
    })
    .handler_timeout(Duration::from_millis(40))
    .restart_policy(RestartPolicy {
        initial_backoff: Duration::from_millis(10),
        ..Default::default()
    })
    .spawn(&sup)
    .await
    .expect("Failed to spawn timeout-actor");

    assert_eq!(client.call(Duration::ZERO).await.unwrap(), (1, false));

    // Trigger a 200ms sleep that exceeds the 40ms handler_timeout:
    // 1. Caller receives Err(ActorError::Timeout)
    // 2. Actor fails fast so half-mutated `corrupted = true` state is discarded
    // 3. Supervisor spawns fresh instance #2 with `corrupted = false`!
    let timeout_res = client.call(Duration::from_millis(200)).await;
    assert!(
        matches!(timeout_res, Err(aixtor::actor::ActorError::Timeout(_))),
        "Expected ActorError::Timeout when handler_timeout expires, got {:?}",
        timeout_res
    );

    // Follow-up call_retry must transparently reach fresh instance #2 with clean state!
    let (inst, corrupted) = client
        .call_retry(Duration::ZERO)
        .await
        .expect("Actor should recover to fresh instance #2");
    assert_eq!(inst, 2, "Supervisor must restart actor after handler_timeout");
    assert!(
        !corrupted,
        "Restarted instance #2 must not carry half-mutated state from timed-out handler"
    );

    // Also verify client-side call_timeout
    let client_timeout_res = client
        .call_timeout(Duration::from_millis(30), Duration::from_millis(5))
        .await;
    assert!(matches!(
        client_timeout_res,
        Err(aixtor::actor::ActorError::Timeout(_))
    ));

    let _ = root.shutdown().await;
}

struct ShutdownOrderActor {
    name: &'static str,
    log: Arc<std::sync::Mutex<Vec<String>>>,
}

impl StatefulActor for ShutdownOrderActor {
    type Message = ();
    type Response = ();
    type Error = String;

    async fn handle(
        &mut self,
        _msg: Self::Message,
        _ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut ActorContext) {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}_begin", self.name));
        tokio::time::sleep(Duration::from_millis(30)).await;
        self.log.lock().unwrap().push(format!("{}_end", self.name));
    }
}

#[tokio::test]
async fn test_sequential_graceful_shutdown_in_reverse_registration_order() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("seq-shutdown-svc", &root)
        .await
        .expect("Failed to spawn service supervisor");

    let log = Arc::new(std::sync::Mutex::new(Vec::new()));

    for name in ["child_1", "child_2", "child_3"] {
        let l = log.clone();
        StatefulActorBuilder::new(name, move |_inst| ShutdownOrderActor {
            name,
            log: l.clone(),
        })
        .spawn(&sup)
        .await
        .expect("Failed to spawn child");
    }

    // Stop the service via RootSupervisor: children must shut down strictly sequentially
    // in reverse registration order (child_3 completely stops before child_2 begins, etc.)
    root.stop_service("seq-shutdown-svc")
        .await
        .expect("stop_service failed");

    let recorded = log.lock().unwrap().clone();
    assert_eq!(
        recorded,
        vec![
            "child_3_begin",
            "child_3_end",
            "child_2_begin",
            "child_2_end",
            "child_1_begin",
            "child_1_end",
        ],
        "Children must shut down strictly sequentially in reverse registration order"
    );

    let _ = root.shutdown().await;
}

/// Intentionally does NOT implement `Clone`.
struct NonCloneMsg(usize);

#[derive(Default)]
struct NonCloneMessageActor {
    sum: usize,
}

impl StatefulActor for NonCloneMessageActor {
    type Message = NonCloneMsg;
    type Response = (usize, usize);
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        self.sum += msg.0;
        Ok((self.sum, ctx.instance_id()))
    }
}

struct MidHandlerPanicActor;

impl StatefulActor for MidHandlerPanicActor {
    type Message = usize;
    type Response = (usize, usize);
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        if ctx.instance_id() == 1 {
            panic!("Simulated mid-handler crash on instance 1");
        }
        Ok((msg * 10, ctx.instance_id()))
    }
}

#[tokio::test]
async fn test_stateful_actor_non_clone_message_rebind_and_call_retry_mid_handler() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("non-clone-svc", &root)
        .await
        .expect("Failed to spawn service supervisor");

    // Part A: Non-Clone Message recovery across closed channel on both call() and cast()
    let non_clone_client = StatefulActorBuilder::from_default::<NonCloneMessageActor>("non-clone-actor")
        .restart_policy(RestartPolicy {
            initial_backoff: Duration::from_millis(10),
            ..Default::default()
        })
        .spawn(&sup)
        .await
        .expect("Failed to spawn NonCloneMessageActor");

    assert_eq!(non_clone_client.call(NonCloneMsg(10)).await.unwrap(), (10, 1));

    // Crash instance 1; next cast(NonCloneMsg) and call(NonCloneMsg) must recover the uncloned
    // message out of SendError and deliver it to instance 2!
    non_clone_client.simulate_crash().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    non_clone_client
        .cast(NonCloneMsg(7))
        .await
        .expect("cast with non-Clone message must rebind and deliver to instance 2");
    let (sum, inst) = non_clone_client
        .call(NonCloneMsg(5))
        .await
        .expect("call with non-Clone message must rebind and deliver to instance 2");
    assert_eq!((sum, inst), (12, 2));

    // Part B: Mid-handler crash recovery via call_retry()
    let retry_client = StatefulActorBuilder::new("mid-panic-actor", |_inst| MidHandlerPanicActor)
        .restart_policy(RestartPolicy {
            initial_backoff: Duration::from_millis(10),
            ..Default::default()
        })
        .spawn(&sup)
        .await
        .expect("Failed to spawn MidHandlerPanicActor");

    let (val, retry_inst) = retry_client
        .call_retry(4)
        .await
        .expect("call_retry must transparently recover across mid-handler panic");
    assert_eq!((val, retry_inst), (40, 2));

    let _ = root.shutdown().await;
}

#[derive(Default)]
struct FlakyStartActor {
    rehydrated_value: usize,
}

impl StatefulActor for FlakyStartActor {
    type Message = ();
    type Response = (usize, usize);
    type Error = String;

    async fn on_start(&mut self, ctx: &mut ActorContext) -> Result<(), String> {
        if ctx.instance_id() == 1 {
            return Err("Transient DB connection failure during on_start".to_string());
        }
        self.rehydrated_value = 99;
        Ok(())
    }

    async fn handle(
        &mut self,
        _msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        Ok((ctx.instance_id(), self.rehydrated_value))
    }
}

#[tokio::test]
async fn test_stateful_actor_on_start_failure_triggers_supervisor_recovery() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("flaky-start-svc", &root)
        .await
        .expect("Failed to spawn service supervisor");

    let client = StatefulActorBuilder::from_default::<FlakyStartActor>("flaky-start-actor")
        .restart_policy(RestartPolicy {
            initial_backoff: Duration::from_millis(10),
            ..Default::default()
        })
        .spawn(&sup)
        .await
        .expect("Failed to spawn FlakyStartActor");

    // Instance 1 fails immediately in `on_start`; supervisor restarts it as instance 2,
    // which succeeds in `on_start` and serves the client request!
    let (inst, rehydrated) = client
        .call(())
        .await
        .expect("Actor must recover automatically when instance 1 fails in on_start");
    assert_eq!(inst, 2, "Supervisor must have restarted actor after on_start failure");
    assert_eq!(rehydrated, 99, "Instance 2 must have completed on_start rehydration");

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_rest_for_one_overlapping_backoffs_and_failed_downstream_recovery() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::builder("rfo-overlap-svc")
        .strategy(SupervisionStrategy::RestForOne)
        .spawn(&root)
        .await
        .expect("Failed to spawn RestForOne supervisor");

    let events = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let ev_b = events.clone();
    let client_b = StatefulActorBuilder::new("actor-b", move |inst| {
        let ev = ev_b.clone();
        ev.lock().unwrap().push(format!("b_start_{}", inst));
        CounterActor::default()
    })
    .restart_policy(RestartPolicy {
        max_restarts: 5,
        time_window: Duration::from_secs(5),
        initial_backoff: Duration::from_millis(90),
        max_backoff: Duration::from_millis(200),
        backoff_multiplier: 1.0,
    })
    .spawn(&sup)
    .await
    .expect("Failed to spawn actor-b");

    let ev_c = events.clone();
    let client_c = StatefulActorBuilder::new("actor-c", move |inst| {
        let ev = ev_c.clone();
        ev.lock().unwrap().push(format!("c_start_{}", inst));
        CounterActor::default()
    })
    .restart_policy(RestartPolicy {
        max_restarts: 1,
        time_window: Duration::from_secs(5),
        initial_backoff: Duration::from_millis(35),
        max_backoff: Duration::from_millis(200),
        backoff_multiplier: 1.0,
    })
    .spawn(&sup)
    .await
    .expect("Failed to spawn actor-c");

    events.lock().unwrap().clear();

    // 1. Crash downstream C (schedules 35ms backoff timer), then 10ms later crash upstream B
    //    (schedules 90ms backoff timer for [B, C]).
    //    C's earlier 35ms timer must be invalidated by B's restart wave so C does not start
    //    before B or get left stuck in `is_restarting`.
    client_c.simulate_crash().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    client_b.simulate_crash().await.unwrap();

    let (_, inst_b) = client_b
        .call_retry(CounterMsg::GetWithInstance)
        .await
        .expect("actor-b must recover");
    let (_, inst_c) = client_c
        .call_retry(CounterMsg::GetWithInstance)
        .await
        .expect("actor-c must recover after B's RestForOne wave");
    assert_eq!(inst_b, 2);
    assert_eq!(inst_c, 2);
    assert_eq!(
        *events.lock().unwrap(),
        vec!["b_start_2".to_string(), "c_start_2".to_string()],
        "C must not start before B when B crashes during C's backoff"
    );

    // 2. Drive downstream C into permanent failure (`max_restarts: 1`), then crash upstream B:
    //    RestForOne must also recover the failed downstream C!
    client_c.simulate_crash().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
        client_c.call_retry(CounterMsg::GetWithInstance).await,
        Err(aixtor::ActorError::Service(ServiceError::MaxRestartsExceeded(_, _, _)))
    ));

    client_b.simulate_crash().await.unwrap();
    let (_, inst_b_after) = client_b
        .call_retry(CounterMsg::GetWithInstance)
        .await
        .expect("actor-b must recover to inst 3");
    let (_, inst_c_after) = client_c
        .call_retry(CounterMsg::GetWithInstance)
        .await
        .expect("Failed downstream actor-c must be recovered when upstream actor-b restarts under RestForOne");
    assert_eq!(inst_b_after, 3);
    assert_eq!(inst_c_after, 3);

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_delete_and_start_child_during_backoff_ignores_stale_timer() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("del-backoff-svc", &root)
        .await
        .expect("Failed to spawn service supervisor");

    let spawn_count = Arc::new(AtomicUsize::new(0));
    let sc1 = spawn_count.clone();

    let client1 = GenericResourceActor::builder("recreated-child")
        .worker(move |ctx: WorkerContext| {
            let sc = sc1.clone();
            async move {
                sc.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(TimedResource::perpetual(ctx.instance_id))
            }
        })
        .eager_prewarm(true)
        .restart_policy(RestartPolicy {
            initial_backoff: Duration::from_millis(80),
            ..Default::default()
        })
        .spawn(&sup)
        .await
        .expect("Failed to spawn initial child");

    assert_eq!(client1.get().await.unwrap(), 1);
    assert_eq!(spawn_count.load(Ordering::SeqCst), 1);

    // Crash child so an 80ms backoff timer is scheduled
    client1.simulate_crash().await.unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Delete child and immediately start a fresh child with the same name while the 80ms timer is still in flight
    sup.delete_child("recreated-child").await.unwrap();

    let sc2 = spawn_count.clone();
    let client2 = GenericResourceActor::builder("recreated-child")
        .worker(move |ctx: WorkerContext| {
            let sc = sc2.clone();
            async move {
                sc.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(TimedResource::perpetual(ctx.instance_id))
            }
        })
        .eager_prewarm(true)
        .spawn(&sup)
        .await
        .expect("Failed to start replacement child");

    assert_eq!(client2.get().await.unwrap(), 1);
    assert_eq!(spawn_count.load(Ordering::SeqCst), 2);

    // Wait past the original 80ms backoff timer; it must NOT fire and spawn an extra instance!
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(
        client2.get().await.unwrap(),
        1,
        "Stale backoff timer from deleted child must not restart the newly added child"
    );
    assert_eq!(spawn_count.load(Ordering::SeqCst), 2);

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_root_restart_service_honors_shutdown_policy_without_overlap() {
    let root = RootSupervisor::new();

    let active_instances = Arc::new(AtomicUsize::new(0));
    let overlap_detected = Arc::new(AtomicBool::new(false));
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));

    let ai = active_instances.clone();
    let od = overlap_detected.clone();
    let ev = events.clone();

    let child_spec = StatefulActorBuilder::new("exclusive-child", move |inst| {
        ExclusiveResourceSiblingActor {
            instance_id: inst,
            active_instances: ai.clone(),
            overlap_detected: od.clone(),
            events: ev.clone(),
        }
    })
    .shutdown_policy(ShutdownPolicy::Timeout(Duration::from_secs(2)))
    .build_child_spec();

    let sup = ServiceSupervisor::builder("restartable-svc")
        .shutdown_policy(ShutdownPolicy::Timeout(Duration::from_secs(2)))
        .add_child(child_spec)
        .spawn(&root)
        .await
        .expect("Failed to spawn service supervisor");

    let client = sup
        .get_actor_client::<ExclusiveResourceSiblingActor>("exclusive-child")
        .await
        .expect("Failed to get actor client");
    assert_eq!(client.call(false).await.unwrap(), 1);

    // Manually restart the entire service via RootSupervisor:
    // Must gracefully shut down the old ServiceSupervisor and await its child's 50ms `on_stop`
    // BEFORE spawning the replacement ServiceSupervisor and its child's `on_start`!
    root.restart_service("restartable-svc")
        .await
        .expect("restart_service should succeed");

    assert_eq!(client.call(false).await.unwrap(), 1);
    assert!(
        !overlap_detected.load(Ordering::SeqCst),
        "Old and new service children must never overlap during root.restart_service"
    );
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            "start_1".to_string(),
            "stop_1".to_string(),
            "start_1".to_string()
        ],
        "root.restart_service must complete old child's on_stop before new child's on_start"
    );

    let _ = root.shutdown().await;
}

#[tokio::test]
async fn test_resource_client_rebinds_on_actor_unavailable_and_propagates_actor_not_found() {
    let root = RootSupervisor::new();
    let sup = ServiceSupervisor::spawn_and_register("rebind-err-svc", &root)
        .await
        .expect("Failed to spawn service supervisor");

    // 1. Verify ResourceClient::get() and ActorClient::call() propagate ServiceError::ActorNotFound
    //    when an actor has been explicitly stopped.
    let res_client = GenericResourceActor::builder("stopped-res-actor")
        .worker(|_ctx: WorkerContext| async {
            Ok::<_, String>(TimedResource::perpetual("ok".to_string()))
        })
        .spawn(&sup)
        .await
        .expect("Failed to spawn resource actor");

    let state_client = StatefulActorBuilder::from_default::<CounterActor>("stopped-state-actor")
        .spawn(&sup)
        .await
        .expect("Failed to spawn stateful actor");

    sup.stop_child("stopped-res-actor").await.unwrap();
    sup.stop_child("stopped-state-actor").await.unwrap();

    let res_err = res_client.get().await;
    assert!(
        matches!(
            res_err,
            Err(ResourceError::Service(ServiceError::ActorNotFound(_)))
        ),
        "Expected ResourceError::Service(ServiceError::ActorNotFound), got {:?}",
        res_err
    );

    let state_err = state_client.call(CounterMsg::GetWithInstance).await;
    assert!(
        matches!(
            state_err,
            Err(aixtor::ActorError::Service(ServiceError::ActorNotFound(_)))
        ),
        "Expected ActorError::Service(ServiceError::ActorNotFound), got {:?}",
        state_err
    );

    let _ = root.shutdown().await;
}


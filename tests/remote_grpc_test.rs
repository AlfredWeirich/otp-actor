//! Integration tests verifying remote gRPC actors, self-healing supervision,
//! and coordinated 2-phase graceful shutdown.

#![cfg(feature = "remote")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use otp_actor::actor::{
    ActorContext, RestartType, RootSupervisor, ServiceSupervisor, StatefulActor,
    StatefulActorBuilder,
};
use otp_actor::remote::{
    ActorNodeHandle, ActorNodeRegistry, AnyActorClient, RemoteActorClient, RemoteActorError,
};
use serde::{Deserialize, Serialize};

// ----------------------------------------------------------------------------
// Test Actor Definition
// ----------------------------------------------------------------------------

#[derive(Clone, Default)]
struct CalculatorActor {
    value: i64,
    stop_counter: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CalcMsg {
    Add(i64),
    Sub(i64),
    Divide(i64),
    Delay(Duration),
    Get,
    Reset,
    Crash,
    Stop,
}

impl StatefulActor for CalculatorActor {
    type Message = CalcMsg;
    type Response = i64;
    type Error = String;

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            CalcMsg::Add(n) => {
                self.value += n;
                Ok(self.value)
            }
            CalcMsg::Sub(n) => {
                self.value -= n;
                Ok(self.value)
            }
            CalcMsg::Divide(n) => {
                if n == 0 {
                    return Err("Division by zero".into());
                }
                self.value /= n;
                Ok(self.value)
            }
            CalcMsg::Delay(duration) => {
                tokio::time::sleep(duration).await;
                Ok(self.value)
            }
            CalcMsg::Get => Ok(self.value),
            CalcMsg::Reset => {
                self.value = 0;
                Ok(self.value)
            }
            CalcMsg::Crash => {
                ctx.fail("Intentional crash for resilience test");
                Err("Crashing".into())
            }
            CalcMsg::Stop => {
                ctx.stop();
                Ok(self.value)
            }
        }
    }

    async fn on_stop(&mut self, _ctx: &mut ActorContext) {
        self.stop_counter.fetch_add(1, Ordering::SeqCst);
    }
}

// ----------------------------------------------------------------------------
// Test 1: Basic Remote Call, Cast, ServiceNotFound & ActorNotFound
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_grpc_call_cast_and_errors()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("calc-service", &root).await?;

    let stop_counter = Arc::new(AtomicUsize::new(0));
    let actor_instance = CalculatorActor {
        value: 100,
        stop_counter: stop_counter.clone(),
    };

    let client = StatefulActorBuilder::from_instance("calc-1", actor_instance)
        .restart_type(RestartType::Permanent)
        .spawn(&service)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(client);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeHandle::start(bind_addr, registry, Some(root)).await?;
    let node_addr = node.local_addr();

    let endpoint_url = format!("http://{node_addr}");
    let remote_client =
        RemoteActorClient::<CalculatorActor>::connect(&endpoint_url, "calc-service", "calc-1")
            .await?;

    // 1. Remote call
    let res = remote_client.call(CalcMsg::Add(25)).await?;
    assert_eq!(res, 125);

    let res = remote_client.call(CalcMsg::Sub(15)).await?;
    assert_eq!(res, 110);

    // 2. Remote cast
    remote_client.cast(CalcMsg::Reset).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let res = remote_client.call(CalcMsg::Get).await?;
    assert_eq!(res, 0);

    // 3. Domain error propagation
    let err = remote_client.call(CalcMsg::Divide(0)).await;
    match err {
        Err(RemoteActorError::Actor(msg)) => assert_eq!(msg, "Division by zero"),
        other => panic!("Expected domain error, got {other:?}"),
    }

    // 4. Actor not found in existing service
    let unknown_actor = RemoteActorClient::<CalculatorActor>::connect(
        &endpoint_url,
        "calc-service",
        "non-existent",
    )
    .await?;
    let err = unknown_actor.call(CalcMsg::Get).await;
    match err {
        Err(RemoteActorError::ActorNotFound(name)) => {
            assert_eq!(name, "calc-service/non-existent");
        }
        other => panic!("Expected ActorNotFound, got {other:?}"),
    }

    // 5. Service not found on node
    let unknown_service =
        RemoteActorClient::<CalculatorActor>::connect(&endpoint_url, "unknown-service", "calc-1")
            .await?;
    let err = unknown_service.call(CalcMsg::Get).await;
    match err {
        Err(RemoteActorError::ServiceNotFound(svc)) => {
            assert_eq!(svc, "unknown-service");
        }
        other => panic!("Expected ServiceNotFound, got {other:?}"),
    }

    node.shutdown().await?;
    assert_eq!(stop_counter.load(Ordering::SeqCst), 1);
    Ok(())
}

// ----------------------------------------------------------------------------
// Test 2: Crash & Self-Healing over gRPC (Immediate Call Without Manual Sleep)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_grpc_actor_crash_and_self_healing()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("healing-service", &root).await?;

    let stop_counter = Arc::new(AtomicUsize::new(0));
    let actor_instance = CalculatorActor {
        value: 10,
        stop_counter: stop_counter.clone(),
    };

    let client = StatefulActorBuilder::from_instance("calc-resilient", actor_instance)
        .restart_type(RestartType::Permanent)
        .spawn(&service)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(client);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeHandle::start(bind_addr, registry, Some(root)).await?;
    let node_addr = node.local_addr();

    let remote_client = RemoteActorClient::<CalculatorActor>::connect(
        format!("http://{node_addr}"),
        "healing-service",
        "calc-resilient",
    )
    .await?;

    assert_eq!(remote_client.call(CalcMsg::Add(5)).await?, 15);

    // Trigger intentional actor crash
    let _ = remote_client.call(CalcMsg::Crash).await;

    // Immediately call without manual sleep: the server-side ActorClient transparently
    // backs off, waits for ServiceSupervisor recovery, rebinds, and processes the message!
    let res = remote_client.call(CalcMsg::Add(20)).await?;
    assert_eq!(res, 30);

    node.shutdown().await?;
    Ok(())
}

// ----------------------------------------------------------------------------
// Test 3: Location Transparency with AnyActorClient
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_location_transparency_any_client()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("transparency-service", &root).await?;

    let stop_counter = Arc::new(AtomicUsize::new(0));
    let local_actor = CalculatorActor {
        value: 50,
        stop_counter: stop_counter.clone(),
    };

    let local_client = StatefulActorBuilder::from_instance("calc-local", local_actor)
        .restart_type(RestartType::Permanent)
        .spawn(&service)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(local_client.clone());

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeHandle::start(bind_addr, registry, Some(root)).await?;
    let node_addr = node.local_addr();

    let remote_client = RemoteActorClient::<CalculatorActor>::connect(
        format!("http://{node_addr}"),
        "transparency-service",
        "calc-local",
    )
    .await?;

    let client_a = AnyActorClient::Local(local_client);
    let client_b = AnyActorClient::Remote(remote_client);

    // Call locally
    assert_eq!(client_a.call(CalcMsg::Add(10)).await?, 60);

    // Call remotely with timeout (both target the same underlying state on this node)
    assert_eq!(
        client_b
            .call_timeout(CalcMsg::Add(10), Duration::from_secs(2))
            .await?,
        70
    );

    node.shutdown().await?;
    Ok(())
}

// ----------------------------------------------------------------------------
// Test 4: Call Timeout Propagation
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_grpc_call_timeout() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("timeout-service", &root).await?;

    let stop_counter = Arc::new(AtomicUsize::new(0));
    let local_actor = CalculatorActor {
        value: 0,
        stop_counter: stop_counter.clone(),
    };

    let local_client = StatefulActorBuilder::from_instance("calc-timeout", local_actor)
        .restart_type(RestartType::Permanent)
        .spawn(&service)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(local_client);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeHandle::start(bind_addr, registry, Some(root)).await?;
    let node_addr = node.local_addr();

    let remote_client = RemoteActorClient::<CalculatorActor>::connect(
        format!("http://{node_addr}"),
        "timeout-service",
        "calc-timeout",
    )
    .await?;

    // Request takes 200ms, but timeout deadline is 50ms
    let err = remote_client
        .call_timeout(
            CalcMsg::Delay(Duration::from_millis(200)),
            Duration::from_millis(50),
        )
        .await;

    match err {
        Err(RemoteActorError::Timeout) => (),
        other => panic!("Expected Timeout, got {other:?}"),
    }

    node.shutdown().await?;
    Ok(())
}

// ----------------------------------------------------------------------------
// Test 5: 2-Phase Graceful Shutdown Drains In-Flight gRPC Calls Before Actor Stop
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_grpc_2phase_graceful_shutdown_drains_inflight_call()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("drain-service", &root).await?;

    let stop_counter = Arc::new(AtomicUsize::new(0));
    let local_actor = CalculatorActor {
        value: 42,
        stop_counter: stop_counter.clone(),
    };

    let local_client = StatefulActorBuilder::from_instance("calc-drain", local_actor)
        .restart_type(RestartType::Permanent)
        .spawn(&service)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(local_client);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeHandle::start(bind_addr, registry, Some(root)).await?;
    let node_addr = node.local_addr();

    let remote_client = RemoteActorClient::<CalculatorActor>::connect(
        format!("http://{node_addr}"),
        "drain-service",
        "calc-drain",
    )
    .await?;

    let client_for_inflight = remote_client.clone();
    let stop_counter_for_inflight = stop_counter.clone();

    // Start an in-flight call taking 80ms
    let inflight_task = tokio::spawn(async move {
        let res = client_for_inflight
            .call(CalcMsg::Delay(Duration::from_millis(80)))
            .await?;
        // At the moment the gRPC response arrives, Phase 2 (actor on_stop) must not have run yet!
        let stops_at_completion = stop_counter_for_inflight.load(Ordering::SeqCst);
        Ok::<(i64, usize), RemoteActorError<String>>((res, stops_at_completion))
    });

    // Give the RPC 20ms to reach the server handler, then trigger 2-phase shutdown
    tokio::time::sleep(Duration::from_millis(20)).await;
    node.shutdown().await?;

    let (val, stops_during_inflight) = inflight_task.await??;
    assert_eq!(val, 42);
    assert_eq!(
        stops_during_inflight, 0,
        "Actor must not be stopped before in-flight gRPC call finishes"
    );
    assert_eq!(
        stop_counter.load(Ordering::SeqCst),
        1,
        "Actor on_stop must be invoked during Phase 2 after gRPC drainage"
    );

    Ok(())
}

// ----------------------------------------------------------------------------
// Test 6: Client-Initiated Graceful Termination (ctx.stop with RestartType::Transient)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_grpc_client_initiated_graceful_stop_transient()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = RootSupervisor::new();
    let service = ServiceSupervisor::spawn_and_register("transient-service", &root).await?;

    let stop_counter = Arc::new(AtomicUsize::new(0));
    let local_actor = CalculatorActor {
        value: 100,
        stop_counter: stop_counter.clone(),
    };

    let local_client = StatefulActorBuilder::from_instance("calc-transient", local_actor)
        .restart_type(RestartType::Transient)
        .spawn(&service)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(local_client);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeHandle::start(bind_addr, registry, Some(root)).await?;
    let node_addr = node.local_addr();

    let remote_client = RemoteActorClient::<CalculatorActor>::connect(
        format!("http://{node_addr}"),
        "transient-service",
        "calc-transient",
    )
    .await?;

    // 1. Regular call works
    assert_eq!(remote_client.call(CalcMsg::Add(50)).await?, 150);

    // 2. Client initiates graceful stop
    assert_eq!(remote_client.call(CalcMsg::Stop).await?, 150);

    // Give supervisor brief moment to observe clean termination
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify on_stop was invoked
    assert_eq!(stop_counter.load(Ordering::SeqCst), 1);

    // Verify actor is not restarted (Transient policy)
    let err = remote_client.call(CalcMsg::Get).await;
    match err {
        Err(RemoteActorError::ActorNotFound(_) | RemoteActorError::ActorUnavailable) => (),
        other => panic!("Expected ActorNotFound or ActorUnavailable, got {other:?}"),
    }

    node.shutdown().await?;
    Ok(())
}

//! Integration tests verifying remote gRPC actors, self-healing supervision,
//! and coordinated 2-phase graceful shutdown.

#![cfg(feature = "remote")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use otp_actor::actor::{
    ActorContext, RestartType, StatefulActor, StatefulActorBuilder, Supervisor,
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
        ctx: &mut ActorContext<Self>,
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

    async fn on_stop(&mut self, _ctx: &mut ActorContext<Self>) {
        self.stop_counter.fetch_add(1, Ordering::SeqCst);
    }
}

// ----------------------------------------------------------------------------
// Test 1: Basic Remote Call, Cast, ServiceNotFound & ActorNotFound
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_grpc_call_cast_and_errors()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = Supervisor::new("root");
    let service = Supervisor::builder("calc-service").spawn(&root).await?;

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

    // 2. Remote cast & cast_timeout
    remote_client.cast(CalcMsg::Reset).await?;
    remote_client
        .cast_timeout(CalcMsg::Add(5), Duration::from_secs(2))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let res = remote_client.call(CalcMsg::Get).await?;
    assert_eq!(res, 5);

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
    let root = Supervisor::new("root");
    let service = Supervisor::builder("healing-service").spawn(&root).await?;

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
    // waits on its watch slot for Supervisor recovery, rebinds, and processes the message!
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
    let root = Supervisor::new("root");
    let service = Supervisor::builder("transparency-service")
        .spawn(&root)
        .await?;

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

    // Typed CallMessage (`ask` / `ask_timeout`) location transparency (F-20)
    struct AddTyped(i64);
    impl From<AddTyped> for CalcMsg {
        fn from(m: AddTyped) -> Self {
            CalcMsg::Add(m.0)
        }
    }
    impl otp_actor::CallMessage<CalculatorActor> for AddTyped {
        type Reply = i64;
        fn extract_reply(resp: i64) -> Result<Self::Reply, String> {
            Ok(resp)
        }
    }

    assert_eq!(client_a.ask(AddTyped(5)).await?, 75);
    assert_eq!(client_b.ask(AddTyped(5)).await?, 80);
    assert_eq!(
        client_a
            .ask_timeout(AddTyped(2), Duration::from_secs(2))
            .await?,
        82
    );
    assert_eq!(
        client_b
            .ask_timeout(AddTyped(3), Duration::from_secs(2))
            .await?,
        85
    );

    // Debug formatting on public remote types (F-20)
    assert!(format!("{client_a:?}").contains("AnyActorClient::Local"));
    assert!(format!("{client_b:?}").contains("AnyActorClient::Remote"));
    assert!(format!("{client_b:?}").contains("calc-local"));
    assert!(format!("{node:?}").contains("ActorNodeHandle"));
    let dbg_registry = ActorNodeRegistry::new();
    assert!(format!("{dbg_registry:?}").contains("ActorNodeRegistry"));
    let dbg_svc = otp_actor::remote::ActorNodeService::new(dbg_registry.clone());
    assert!(format!("{dbg_svc:?}").contains("ActorNodeService"));
    let dbg_admin = otp_actor::remote::ActorNodeAdminService::new(dbg_registry.clone(), None);
    assert!(format!("{dbg_admin:?}").contains("ActorNodeAdminService"));
    let dbg_builder = otp_actor::remote::ActorNodeBuilder::new(bind_addr, dbg_registry);
    assert!(format!("{dbg_builder:?}").contains("ActorNodeBuilder"));

    node.shutdown().await?;
    Ok(())
}

// ----------------------------------------------------------------------------
// Test 4: Call Timeout Propagation
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_grpc_call_timeout() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = Supervisor::new("root");
    let service = Supervisor::builder("timeout-service").spawn(&root).await?;

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

    // Request takes 200ms, but client deadline is 50ms (server budget reserves headroom, so
    // the server records and returns a structured Kind::Timeout before the client cancels HTTP/2)
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
    assert_eq!(
        node.remote_metrics().framework_errors_total,
        1,
        "server should return structured Kind::Timeout before client cancels the HTTP/2 stream"
    );

    node.shutdown().await?;
    Ok(())
}

// ----------------------------------------------------------------------------
// Test 5: 2-Phase Graceful Shutdown Drains In-Flight gRPC Calls Before Actor Stop
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_grpc_2phase_graceful_shutdown_drains_inflight_call()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = Supervisor::new("root");
    let service = Supervisor::builder("drain-service").spawn(&root).await?;

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
    let root = Supervisor::new("root");
    let service = Supervisor::builder("transient-service")
        .spawn(&root)
        .await?;

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

// ----------------------------------------------------------------------------
// Test 7: Trailing Bytes Rejection, Error Symmetry & Phase-1 Drain Timeout
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_trailing_bytes_and_drain_timeout_and_error_symmetry()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use otp_actor::actor::{RestartPolicy, ShutdownPolicy};
    use otp_actor::remote::ActorNodeClient;
    use otp_actor::remote::proto::CallRequest;
    use otp_actor::remote::proto::call_response::Result as CallResult;
    use otp_actor::remote::proto::remote_error::Kind;

    let root = Supervisor::new("root");
    let service = Supervisor::builder("hardening-service")
        .spawn(&root)
        .await?;

    let stop_counter = Arc::new(AtomicUsize::new(0));
    let actor_instance = CalculatorActor {
        value: 10,
        stop_counter: stop_counter.clone(),
    };

    let local_client = StatefulActorBuilder::from_instance("calc-hardened", actor_instance)
        .restart_policy(
            RestartPolicy::new(0, Duration::from_secs(10))
                .initial_backoff(Duration::from_millis(10))
                .max_backoff(Duration::from_millis(50)),
        )
        .restart_type(RestartType::Permanent)
        .shutdown_policy(ShutdownPolicy::Timeout(Duration::from_millis(50)))
        .spawn(&service)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(local_client.clone());

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeHandle::start(bind_addr, registry, Some(root))
        .await?
        .with_drain_timeout(Duration::from_millis(50));
    let node_addr = node.local_addr();
    let endpoint_url = format!("http://{node_addr}");

    // 1. Verify trailing garbage bytes are rejected with Deserialization error ([L-1])
    let mut payload = postcard::to_stdvec(&CalcMsg::Add(5))?;
    payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

    let mut raw_client = ActorNodeClient::connect(endpoint_url.clone()).await?;
    let raw_resp = raw_client
        .call(CallRequest {
            service_name: "hardening-service".into(),
            actor_name: "calc-hardened".into(),
            payload: payload.into(),
            timeout_ms: None,
            protocol_version: otp_actor::remote::REMOTE_PROTOCOL_VERSION,
        })
        .await?
        .into_inner();

    match raw_resp.result {
        Some(CallResult::FrameworkError(err)) => {
            assert_eq!(err.kind, Kind::Deserialization as i32);
        }
        other => {
            panic!("Expected Deserialization framework error for trailing bytes, got {other:?}")
        }
    }

    // 2. Verify AnyActorClient error symmetry on MaxRestartsExceeded ([M-2])
    let remote_client = RemoteActorClient::<CalculatorActor>::connect(
        &endpoint_url,
        "hardening-service",
        "calc-hardened",
    )
    .await?;

    let any_local = AnyActorClient::Local(local_client.clone());
    let any_remote = AnyActorClient::Remote(remote_client.clone());

    // Crash the actor; with max_restarts = 0 it transitions immediately to MaxRestartsExceeded
    let _ = remote_client.call(CalcMsg::Crash).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    let local_err = any_local.call(CalcMsg::Get).await;
    let remote_err = any_remote.call(CalcMsg::Get).await;
    assert!(
        matches!(local_err, Err(RemoteActorError::MaxRestartsExceeded(_))),
        "Expected Local AnyActorClient to map MaxRestartsExceeded to MaxRestartsExceeded, got {local_err:?}"
    );
    assert!(
        matches!(remote_err, Err(RemoteActorError::MaxRestartsExceeded(_))),
        "Expected Remote AnyActorClient to map MaxRestartsExceeded to MaxRestartsExceeded, got {remote_err:?}"
    );

    // 3. Recover actor and verify Phase-1 drain timeout bounds hung RPCs ([M-1])
    service.restart_child("calc-hardened").await?;
    let slow_client = remote_client.clone();
    let _hung_rpc = tokio::spawn(async move {
        let _ = slow_client
            .call(CalcMsg::Delay(Duration::from_secs(10)))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Shutdown must complete rapidly (within 500ms) despite the 10s RPC because drain_timeout = 50ms
    // and the actor's ShutdownPolicy::Timeout is also 50ms.
    tokio::time::timeout(Duration::from_millis(500), node.shutdown())
        .await
        .expect("Shutdown timed out despite drain_timeout")?;

    Ok(())
}

// ============================================================================
// In-Process Unit Tests for gRPC Service Hardening (No TCP Sockets Required)
// ============================================================================

#[tokio::test]
async fn test_service_protocol_version_and_unknown_error_mapping() {
    use otp_actor::remote::proto::call_response::Result as CallResult;
    use otp_actor::remote::proto::remote_error::Kind;
    use otp_actor::remote::proto::{ActorNode, CallRequest, CastRequest, RemoteError};
    use otp_actor::remote::{ActorNodeService, REMOTE_PROTOCOL_VERSION};

    let registry = ActorNodeRegistry::new();
    let service = ActorNodeService::new(registry);

    // 1. Incompatible protocol version on Call returns Kind::IncompatibleProtocol
    let resp = service
        .call(tonic::Request::new(CallRequest {
            service_name: "svc".into(),
            actor_name: "act".into(),
            payload: Default::default(),
            timeout_ms: None,
            protocol_version: REMOTE_PROTOCOL_VERSION + 42,
        }))
        .await
        .unwrap()
        .into_inner();

    match resp.result {
        Some(CallResult::FrameworkError(err)) => {
            assert_eq!(err.kind, Kind::IncompatibleProtocol as i32);
            let mapped = RemoteActorError::<String>::from(err);
            assert!(
                matches!(mapped, RemoteActorError::IncompatibleProtocol(ref m) if m.contains("Unsupported protocol version")),
                "Expected IncompatibleProtocol, got {mapped:?}"
            );
        }
        other => panic!("Expected IncompatibleProtocol framework error, got {other:?}"),
    }

    // 2. Incompatible protocol version on Cast returns Kind::IncompatibleProtocol
    let cast_resp = service
        .cast(tonic::Request::new(CastRequest {
            service_name: "svc".into(),
            actor_name: "act".into(),
            payload: Default::default(),
            timeout_ms: None,
            protocol_version: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    let err = cast_resp
        .framework_error
        .expect("Expected framework_error on version mismatch");
    assert_eq!(err.kind, Kind::IncompatibleProtocol as i32);

    // 3. Unrecognized future RemoteError.kind maps to RemoteActorError::Unknown (not ActorPanic)
    let unknown_mapped = RemoteActorError::<String>::from(RemoteError {
        kind: 9999,
        message: "future error variant".into(),
    });
    assert!(
        matches!(
            unknown_mapped,
            RemoteActorError::Unknown { kind: 9999, ref message } if message == "future error variant"
        ),
        "Expected Unknown error variant, got {unknown_mapped:?}"
    );
}

#[tokio::test]
async fn test_service_authorizer_label_sanitization_and_timeout_clamping() {
    use otp_actor::remote::proto::call_response::Result as CallResult;
    use otp_actor::remote::proto::remote_error::Kind;
    use otp_actor::remote::proto::{ActorNode, CallRequest};
    use otp_actor::remote::{ActorNodeService, REMOTE_PROTOCOL_VERSION};

    let root = Supervisor::new("root");
    let svc = Supervisor::builder("auth-svc").spawn(&root).await.unwrap();
    let calc = StatefulActorBuilder::from_default::<CalculatorActor>("calc")
        .spawn(&svc)
        .await
        .unwrap();

    let registry = ActorNodeRegistry::new();
    registry.register("auth-svc", "calc", calc);

    let service = ActorNodeService::new(registry)
        .with_max_request_timeout(Some(Duration::from_millis(40)))
        .with_authorizer(Arc::new(|metadata, _ext, _svc, _actor| {
            match metadata.get("authorization") {
                Some(val) if val == "Bearer secret-token" => Ok(()),
                _ => Err(tonic::Status::unauthenticated("missing or invalid token")),
            }
        }));

    let get_payload =
        otp_actor::remote::proto::Bytes::from(postcard::to_stdvec(&CalcMsg::Get).unwrap());

    // 1. Unauthenticated request is rejected before routing
    let unauth_err = service
        .call(tonic::Request::new(CallRequest {
            service_name: "auth-svc".into(),
            actor_name: "calc".into(),
            payload: get_payload.clone(),
            timeout_ms: None,
            protocol_version: REMOTE_PROTOCOL_VERSION,
        }))
        .await
        .unwrap_err();
    assert_eq!(unauth_err.code(), tonic::Code::Unauthenticated);

    // 2. Authenticated request succeeds
    let mut auth_req = tonic::Request::new(CallRequest {
        service_name: "auth-svc".into(),
        actor_name: "calc".into(),
        payload: get_payload,
        timeout_ms: None,
        protocol_version: REMOTE_PROTOCOL_VERSION,
    });
    auth_req
        .metadata_mut()
        .insert("authorization", "Bearer secret-token".parse().unwrap());
    let ok_resp = service.call(auth_req).await.unwrap().into_inner();
    let ok_bytes: otp_actor::remote::proto::Bytes = match ok_resp.result {
        Some(CallResult::OkPayload(b)) => b,
        other => panic!("Expected OkPayload, got {other:?}"),
    };
    assert_eq!(postcard::from_bytes::<i64>(&ok_bytes).unwrap(), 0);

    // 3. Control characters & long untrusted names are sanitized in ServiceNotFound/ActorNotFound
    let long_evil_service = format!("evil\n\r\x1b[31m{}", "x".repeat(200));
    let mut evil_req = tonic::Request::new(CallRequest {
        service_name: long_evil_service,
        actor_name: "calc".into(),
        payload: Default::default(),
        timeout_ms: None,
        protocol_version: REMOTE_PROTOCOL_VERSION,
    });
    evil_req
        .metadata_mut()
        .insert("authorization", "Bearer secret-token".parse().unwrap());
    let evil_resp = service.call(evil_req).await.unwrap().into_inner();
    match evil_resp.result {
        Some(CallResult::FrameworkError(err)) => {
            assert_eq!(err.kind, Kind::ServiceNotFound as i32);
            assert!(!err.message.contains('\n'));
            assert!(!err.message.contains('\r'));
            assert!(!err.message.contains('\x1b'));
            assert!(err.message.ends_with('…'));
        }
        other => panic!("Expected sanitized ServiceNotFound error, got {other:?}"),
    }

    // 4. Server clamps excessive client timeout_ms via max_request_timeout (40ms)
    let delay_payload = postcard::to_stdvec(&CalcMsg::Delay(Duration::from_secs(5))).unwrap();
    let mut slow_req = tonic::Request::new(CallRequest {
        service_name: "auth-svc".into(),
        actor_name: "calc".into(),
        payload: delay_payload.into(),
        timeout_ms: Some(60_000),
        protocol_version: REMOTE_PROTOCOL_VERSION,
    });
    slow_req
        .metadata_mut()
        .insert("authorization", "Bearer secret-token".parse().unwrap());

    let start = std::time::Instant::now();
    let timeout_resp = service.call(slow_req).await.unwrap().into_inner();
    assert!(
        start.elapsed() < Duration::from_millis(300),
        "Server max_request_timeout should have clamped 60s request to 40ms"
    );
    match timeout_resp.result {
        Some(CallResult::FrameworkError(err)) => {
            assert_eq!(err.kind, Kind::Timeout as i32);
        }
        other => panic!("Expected Timeout framework error, got {other:?}"),
    }

    // 5. Tonic Status timeout mapping distinguishes timeout vs plain cancellation
    assert!(matches!(
        RemoteActorError::<String>::from(tonic::Status::deadline_exceeded("deadline")),
        RemoteActorError::Timeout
    ));
    assert!(matches!(
        RemoteActorError::<String>::from(tonic::Status::cancelled("Timeout expired")),
        RemoteActorError::Timeout
    ));
    assert!(matches!(
        RemoteActorError::<String>::from(tonic::Status::cancelled("user cancelled")),
        RemoteActorError::Transport(_)
    ));

    root.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_admin_service_health_status_authorizer_and_registry_only_mode() {
    use otp_actor::actor::{HealthState, NodeStatus, StatusOptions};
    use otp_actor::remote::proto::remote_error::Kind;
    use otp_actor::remote::proto::status_response::Result as StatusResult;
    use otp_actor::remote::proto::{ActorNodeAdmin, HealthRequest, StatusRequest};
    use otp_actor::remote::{ActorNodeAdminService, REMOTE_PROTOCOL_VERSION};

    let root = Supervisor::new("admin-unit-root");
    let svc = Supervisor::builder("admin-svc").spawn(&root).await.unwrap();
    let calc = StatefulActorBuilder::from_default::<CalculatorActor>("calc")
        .channel_capacity(12)
        .spawn(&svc)
        .await
        .unwrap();

    let registry = ActorNodeRegistry::new();
    assert!(registry.is_empty());
    registry.register("admin-svc", "calc", calc);
    assert_eq!(registry.len(), 1);

    let admin_svc = ActorNodeAdminService::new(registry.clone(), None)
        .with_started_at(std::time::Instant::now() - Duration::from_secs(30))
        .with_authorizer(Arc::new(|metadata, _ext, svc_name, actor_name| {
            assert_eq!(svc_name, "__admin");
            assert!(actor_name == "health" || actor_name == "status");
            match metadata.get("authorization") {
                Some(v) if v == "Bearer admin-token" => Ok(()),
                _ => Err(tonic::Status::permission_denied("admin auth required")),
            }
        }));

    // 1. Unauthorized Health and Status are rejected
    let unauth_health = admin_svc
        .health(tonic::Request::new(HealthRequest {
            protocol_version: REMOTE_PROTOCOL_VERSION,
        }))
        .await
        .unwrap_err();
    assert_eq!(unauth_health.code(), tonic::Code::PermissionDenied);

    let unauth_status = admin_svc
        .status(tonic::Request::new(StatusRequest {
            protocol_version: REMOTE_PROTOCOL_VERSION,
            timeout_ms: None,
            include_tree: true,
            include_child_details: true,
        }))
        .await
        .unwrap_err();
    assert_eq!(unauth_status.code(), tonic::Code::PermissionDenied);

    // 2. Incompatible protocol version returns IncompatibleProtocol
    let mut bad_ver_req = tonic::Request::new(StatusRequest {
        protocol_version: REMOTE_PROTOCOL_VERSION + 1,
        timeout_ms: None,
        include_tree: true,
        include_child_details: true,
    });
    bad_ver_req
        .metadata_mut()
        .insert("authorization", "Bearer admin-token".parse().unwrap());
    let bad_ver_resp = admin_svc.status(bad_ver_req).await.unwrap().into_inner();
    match bad_ver_resp.result {
        Some(StatusResult::FrameworkError(err)) => {
            assert_eq!(err.kind, Kind::IncompatibleProtocol as i32);
        }
        other => panic!("Expected IncompatibleProtocol, got {other:?}"),
    }

    // 3. Authorized Health and Status without attached SupervisorHandle still report endpoints & uptime
    let mut health_req = tonic::Request::new(HealthRequest {
        protocol_version: REMOTE_PROTOCOL_VERSION,
    });
    health_req
        .metadata_mut()
        .insert("authorization", "Bearer admin-token".parse().unwrap());
    let health_resp = admin_svc.health(health_req).await.unwrap().into_inner();
    assert!(health_resp.alive);
    assert_eq!(health_resp.state, "ok");
    assert!(health_resp.uptime_seconds >= 30);

    let mut status_req = tonic::Request::new(StatusRequest {
        protocol_version: REMOTE_PROTOCOL_VERSION,
        timeout_ms: Some(1000),
        include_tree: true,
        include_child_details: true,
    });
    status_req
        .metadata_mut()
        .insert("authorization", "Bearer admin-token".parse().unwrap());
    let status_resp = admin_svc.status(status_req).await.unwrap().into_inner();
    let StatusResult::StatusPayload(bytes) = status_resp.result.unwrap() else {
        panic!("Expected StatusPayload");
    };
    let decoded: NodeStatus = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.health.state, HealthState::Healthy);
    assert_eq!(decoded.summary.running_children, 1);
    assert_eq!(decoded.summary.stopped_children, 0);
    assert!(decoded.supervision_tree.is_none());
    assert_eq!(decoded.registered_endpoints.len(), 1);
    assert_eq!(decoded.registered_endpoints[0].service_name, "admin-svc");
    assert_eq!(decoded.registered_endpoints[0].actor_name, "calc");
    assert_eq!(
        decoded.registered_endpoints[0].queue.unwrap().max_capacity,
        12
    );
    assert_eq!(
        decoded.remote.as_ref().unwrap().incompatible_protocol_total,
        1
    );
    let prom = otp_actor::format_prometheus_metrics(&decoded);
    assert!(prom.contains("# HELP otp_actor_remote_incompatible_protocol_total "));
    assert!(prom.contains("# TYPE otp_actor_remote_incompatible_protocol_total counter\n"));
    assert!(prom.contains("otp_actor_remote_incompatible_protocol_total 1\n"));

    // 4. Stopping the sole registered actor degrades registry-only health and updates summary counts (F-12)
    svc.stop_child("calc").await.unwrap();

    let mut degraded_health_req = tonic::Request::new(HealthRequest {
        protocol_version: REMOTE_PROTOCOL_VERSION,
    });
    degraded_health_req
        .metadata_mut()
        .insert("authorization", "Bearer admin-token".parse().unwrap());
    let degraded_health = admin_svc
        .health(degraded_health_req)
        .await
        .unwrap()
        .into_inner();
    assert!(degraded_health.alive);
    assert_eq!(degraded_health.state, "degraded");

    let mut degraded_status_req = tonic::Request::new(StatusRequest {
        protocol_version: REMOTE_PROTOCOL_VERSION,
        timeout_ms: Some(1000),
        include_tree: true,
        include_child_details: false,
    });
    degraded_status_req
        .metadata_mut()
        .insert("authorization", "Bearer admin-token".parse().unwrap());
    let degraded_status_resp = admin_svc.status(degraded_status_req).await.unwrap().into_inner();
    let StatusResult::StatusPayload(degraded_bytes) = degraded_status_resp.result.unwrap() else {
        panic!("Expected StatusPayload");
    };
    let degraded_decoded: NodeStatus = postcard::from_bytes(&degraded_bytes).unwrap();
    assert!(degraded_decoded.health.alive);
    assert_eq!(degraded_decoded.health.state, HealthState::Degraded);
    assert_eq!(degraded_decoded.summary.total_actors, 1);
    assert_eq!(degraded_decoded.summary.running_children, 0);
    assert_eq!(degraded_decoded.summary.stopped_children, 1);

    // Ensure StatusOptions::default() stays aligned
    assert!(StatusOptions::default().include_tree);

    root.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_remote_node_monitor_and_any_node_monitor_end_to_end()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use otp_actor::actor::{HealthState, StatusOptions};
    use otp_actor::remote::{AnyNodeMonitor, RemoteNodeMonitor};

    let root = Supervisor::new("e2e-admin-root");
    let svc = Supervisor::builder("calc-svc").spawn(&root).await?;
    let calc = StatefulActorBuilder::from_default::<CalculatorActor>("calc")
        .channel_capacity(16)
        .spawn(&svc)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register_client(calc);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeHandle::builder(bind_addr, registry)
        .supervisor(root.clone())
        .started_at(std::time::Instant::now() - Duration::from_secs(15))
        .authorizer(Arc::new(|metadata, _ext, _svc, _actor| {
            match metadata.get("authorization") {
                Some(val) if val == "Bearer valid" => Ok(()),
                _ => Err(tonic::Status::unauthenticated("missing token")),
            }
        }))
        .start()
        .await?;
    let endpoint_url = format!("http://{}", node.local_addr());

    // 1. Drive some remote Call, Cast, and ActorNotFound traffic
    let remote_client =
        RemoteActorClient::<CalculatorActor>::connect(&endpoint_url, "calc-svc", "calc")
            .await?
            .with_request_metadata(|m| {
                m.insert("authorization", "Bearer valid".parse().unwrap());
            });
    assert_eq!(remote_client.call(CalcMsg::Add(7)).await?, 7);
    remote_client.cast(CalcMsg::Add(3)).await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(remote_client.call(CalcMsg::Get).await?, 10);

    let missing_client =
        RemoteActorClient::<CalculatorActor>::connect(&endpoint_url, "calc-svc", "missing")
            .await?
            .with_request_metadata(|m| {
                m.insert("authorization", "Bearer valid".parse().unwrap());
            });
    assert!(matches!(
        missing_client.call(CalcMsg::Get).await,
        Err(RemoteActorError::ActorNotFound(_))
    ));

    // 2. Query remote monitor via AnyNodeMonitor::Remote
    let remote_monitor = RemoteNodeMonitor::connect(&endpoint_url)
        .await?
        .with_request_metadata(|m| {
            m.insert("authorization", "Bearer valid".parse().unwrap());
        });
    let any_remote = AnyNodeMonitor::from_remote(remote_monitor);
    let any_local = AnyNodeMonitor::local(root.clone());

    let remote_health = any_remote.health().await?;
    let local_health = any_local.health().await?;
    assert!(remote_health.alive);
    assert!(local_health.alive);
    assert_eq!(remote_health.state, HealthState::Healthy);
    assert!(remote_health.uptime_seconds >= 15);

    let remote_status = any_remote.status_default().await?;
    let local_status = any_local.status_default().await?;

    assert_eq!(remote_status.summary.total_supervisors, 2);
    assert_eq!(remote_status.summary.total_actors, 1);
    assert_eq!(remote_status.summary.total_calls, 2);
    assert_eq!(remote_status.summary.total_casts, 1);
    assert_eq!(
        remote_status.summary.total_calls,
        local_status.summary.total_calls
    );

    let remote_counters = remote_status.remote.expect("remote counters must be present");
    assert_eq!(remote_counters.registered_endpoints, 1);
    assert_eq!(remote_counters.grpc_calls_total, 3); // 2 valid + 1 missing
    assert_eq!(remote_counters.grpc_casts_total, 1);
    assert_eq!(remote_counters.actor_not_found_total, 1);
    assert_eq!(remote_counters.grpc_in_flight, 0);
    assert_eq!(remote_status.registered_endpoints.len(), 1);
    assert_eq!(
        remote_status.registered_endpoints[0].queue.unwrap().max_capacity,
        16
    );

    // 3. Prometheus output includes both actor-level and remote transport metrics
    let prom = any_remote
        .prometheus_metrics(StatusOptions::default())
        .await?;
    assert!(prom.contains("otp_actor_remote_grpc_calls_total 3\n"));
    assert!(prom.contains("otp_actor_remote_not_found_total{kind=\"actor\"} 1\n"));
    assert!(prom.contains("otp_actor_remote_incompatible_protocol_total 0\n"));
    assert!(prom.contains("otp_actor_calls_total{supervisor=\"e2e-admin-root/calc-svc\",child=\"calc\"} 2\n"));

    // 4. Local handle counters match
    assert_eq!(node.remote_metrics().grpc_calls_total, 3);

    node.shutdown().await?;
    Ok(())
}

// ----------------------------------------------------------------------------
// Test 12: RemoteNodeMonitor::status Receives Degraded Snapshot When Subtree Hangs (F-17)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_monitor_status_receives_degraded_snapshot_when_subtree_hangs()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use otp_actor::actor::{HealthState, StatusOptions};
    use otp_actor::remote::{ActorNodeBuilder, RemoteNodeMonitor};

    let root = Supervisor::new("f17-degraded-root");
    let healthy_svc = Supervisor::builder("healthy-svc").spawn(&root).await?;
    let healthy_calc = StatefulActorBuilder::from_default::<CalculatorActor>("calc-ok")
        .spawn(&healthy_svc)
        .await?;

    let stuck_svc = Supervisor::builder("stuck-svc").spawn(&root).await?;
    let stuck_calc = StatefulActorBuilder::from_default::<CalculatorActor>("calc-stuck")
        .shutdown_timeout(Duration::from_millis(700))
        .spawn(&stuck_svc)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register("healthy-svc", "calc-ok", healthy_calc);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeBuilder::new(bind_addr, registry)
        .supervisor(root)
        .start()
        .await?;
    let endpoint_url = format!("http://{}", node.local_addr());

    // Block `stuck-svc`'s command loop while waiting for its child's in-flight delay during shutdown
    stuck_calc
        .cast(CalcMsg::Delay(Duration::from_millis(500)))
        .await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let stuck_for_shutdown = stuck_svc.clone();
    let shutdown_task = tokio::spawn(async move { stuck_for_shutdown.shutdown().await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let remote_monitor = RemoteNodeMonitor::connect(&endpoint_url).await?;
    let status = remote_monitor
        .status(StatusOptions::new().timeout(Duration::from_millis(160)))
        .await?;

    assert!(status.health.alive);
    assert_eq!(status.health.state, HealthState::Degraded);
    assert_eq!(status.summary.unavailable_subtrees, 1);

    let tree = status
        .supervision_tree
        .expect("supervision tree should be attached");
    let healthy_entry = tree
        .subtrees
        .iter()
        .find(|s| s.child_name == "healthy-svc")
        .expect("healthy-svc subtree entry missing");
    assert!(healthy_entry.result.is_ok());

    let stuck_entry = tree
        .subtrees
        .iter()
        .find(|s| s.child_name == "stuck-svc")
        .expect("stuck-svc subtree entry missing");
    assert!(stuck_entry.result.is_err());

    let _ = shutdown_task.await;
    node.shutdown().await?;
    Ok(())
}

// ----------------------------------------------------------------------------
// Test 13: Non-Blocking `cast_timeout(Duration::ZERO)`, `default_cast_timeout` & Draining (F-21)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_remote_cast_zero_timeout_try_cast_and_draining_rejects_new_requests()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use otp_actor::remote::ActorNodeBuilder;

    let root = Supervisor::new("f21-root");
    let svc = Supervisor::builder("f21-svc").spawn(&root).await?;
    let local_calc = StatefulActorBuilder::from_default::<CalculatorActor>("bounded-calc")
        .channel_capacity(1)
        .spawn(&svc)
        .await?;

    let registry = ActorNodeRegistry::new();
    registry.register("f21-svc", "bounded-calc", local_calc.clone());

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let node = ActorNodeBuilder::new(bind_addr, registry)
        .supervisor(root)
        .default_cast_timeout(Some(Duration::ZERO))
        .start()
        .await?;
    let endpoint_url = format!("http://{}", node.local_addr());

    let remote_calc = RemoteActorClient::<CalculatorActor>::connect(
        &endpoint_url,
        "f21-svc",
        "bounded-calc",
    )
    .await?;
    let any_local = AnyActorClient::Local(local_calc.clone());

    // 1. Occupy handler with a 250ms delay and fill the single bounded mailbox slot
    local_calc
        .cast(CalcMsg::Delay(Duration::from_millis(250)))
        .await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    local_calc.try_cast(CalcMsg::Add(1))?;

    // 2. Both `cast_timeout(..., Duration::ZERO)` (local & remote) and `cast(...)` under
    //    `default_cast_timeout(Some(Duration::ZERO))` fail immediately with MailboxFull.
    let started = std::time::Instant::now();
    assert!(matches!(
        remote_calc.cast_timeout(CalcMsg::Add(1), Duration::ZERO).await,
        Err(RemoteActorError::MailboxFull)
    ));
    assert!(matches!(
        any_local.cast_timeout(CalcMsg::Add(1), Duration::ZERO).await,
        Err(RemoteActorError::MailboxFull)
    ));
    assert!(matches!(
        remote_calc.cast(CalcMsg::Add(1)).await,
        Err(RemoteActorError::MailboxFull)
    ));
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "non-blocking cast load-shedding should return immediately, took {:?}",
        started.elapsed()
    );

    // Wait for the delay + queued Add(1) to finish
    assert_eq!(remote_calc.call(CalcMsg::Get).await?, 1);

    // 3. Start an in-flight call, trigger Phase-1 draining via `node.shutdown()`, and verify
    //    that new calls arriving while draining are rejected while the in-flight call finishes.
    let inflight_client = remote_calc.clone();
    let inflight = tokio::spawn(async move {
        inflight_client
            .call(CalcMsg::Delay(Duration::from_millis(250)))
            .await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;

    let shutdown_task = tokio::spawn(async move { node.shutdown().await });
    tokio::time::sleep(Duration::from_millis(25)).await;

    let during_drain = remote_calc.call(CalcMsg::Get).await;
    assert!(
        matches!(
            during_drain,
            Err(RemoteActorError::ActorUnavailable | RemoteActorError::Transport(_))
        ),
        "Expected ActorUnavailable or Transport during Phase-1 drain, got {during_drain:?}"
    );

    assert_eq!(inflight.await??, 1);
    shutdown_task.await??;
    Ok(())
}

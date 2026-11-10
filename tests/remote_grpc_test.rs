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
            payload,
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
            payload: Vec::new(),
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
            payload: Vec::new(),
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

    let get_payload = postcard::to_stdvec(&CalcMsg::Get).unwrap();

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
    assert!(matches!(ok_resp.result, Some(CallResult::OkPayload(_))));

    // 3. Control characters & long untrusted names are sanitized in ServiceNotFound/ActorNotFound
    let long_evil_service = format!("evil\n\r\x1b[31m{}", "x".repeat(200));
    let mut evil_req = tonic::Request::new(CallRequest {
        service_name: long_evil_service,
        actor_name: "calc".into(),
        payload: Vec::new(),
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
        payload: delay_payload,
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

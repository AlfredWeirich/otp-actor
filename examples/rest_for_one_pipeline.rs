//! # Blueprint: `RestForOne` Pipeline & Panic Recovery
//!
//! Demonstrates Erlang/OTP's `RestForOne` supervision strategy on a 3-stage pipeline:
//! `stage-1-ingress` -> `stage-2-transform` -> `stage-3-sink`
//!
//! - Crashing `stage-3-sink` restarts **only** `stage-3-sink` (`stage-1` and `stage-2` stay alive).
//! - Panicking `stage-2-transform` leaves `stage-1-ingress` untouched, gracefully stops `stage-3-sink`
//!   (`on_stop` executes in reverse order), and restarts `stage-2` + `stage-3` in registration order.
//! - Crashing `stage-1-ingress` cascades cleanly through all three stages.
//!
//! ## Modes
//! - **Automated showcase (default):**
//!   ```bash
//!   cargo run --example rest_for_one_pipeline
//!   ```
//! - **Interactive terminal mode (inject panics with Enter):**
//!   ```bash
//!   cargo run --example rest_for_one_pipeline -- --interactive
//!   ```

use std::time::Duration;

use otp_actor::{
    ActorClient, ActorContext, RestartPolicy, RestartType, StatefulActor, StatefulActorBuilder,
    SupervisionStrategy, Supervisor,
};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Clone)]
struct PipelineStage {
    stage_name: &'static str,
    processed: u64,
}

enum StageMsg {
    Process(String),
    Inspect,
    InjectPanic,
}

#[derive(Debug)]
struct StageState {
    instance_id: usize,
    processed: u64,
}

impl StatefulActor for PipelineStage {
    type Message = StageMsg;
    type Response = StageState;
    type Error = String;

    async fn on_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), String> {
        println!(
            "  [+] {} started (instance #{})",
            self.stage_name,
            ctx.instance_id()
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: StageMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<StageState, String> {
        match msg {
            StageMsg::Process(_item) => {
                self.processed += 1;
                Ok(StageState {
                    instance_id: ctx.instance_id(),
                    processed: self.processed,
                })
            }
            StageMsg::Inspect => Ok(StageState {
                instance_id: ctx.instance_id(),
                processed: self.processed,
            }),
            StageMsg::InjectPanic => {
                panic!(
                    "intentional panic in {} (instance #{})",
                    self.stage_name,
                    ctx.instance_id()
                );
            }
        }
    }

    async fn on_stop(&mut self, ctx: &mut ActorContext<Self>) {
        println!(
            "  [-] {} graceful on_stop (instance #{}, flushed {} items)",
            self.stage_name,
            ctx.instance_id(),
            self.processed
        );
    }
}

async fn print_topology(
    s1: &ActorClient<PipelineStage>,
    s2: &ActorClient<PipelineStage>,
    s3: &ActorClient<PipelineStage>,
) -> Result<(StageState, StageState, StageState), Box<dyn std::error::Error>> {
    let st1 = s1.call(StageMsg::Inspect).await?;
    let st2 = s2.call(StageMsg::Inspect).await?;
    let st3 = s3.call(StageMsg::Inspect).await?;
    println!(
        "  Topology: [stage-1 #{} ({} items)] -> [stage-2 #{} ({} items)] -> [stage-3 #{} ({} items)]",
        st1.instance_id,
        st1.processed,
        st2.instance_id,
        st2.processed,
        st3.instance_id,
        st3.processed
    );
    Ok((st1, st2, st3))
}

async fn run_automated(
    s1: &ActorClient<PipelineStage>,
    s2: &ActorClient<PipelineStage>,
    s3: &ActorClient<PipelineStage>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Step 1: Process initial items through all 3 stages ---");
    for n in 1..=3 {
        let payload = format!("record-{n}");
        s1.call(StageMsg::Process(payload.clone())).await?;
        s2.call(StageMsg::Process(payload.clone())).await?;
        s3.call(StageMsg::Process(payload)).await?;
    }
    let (a1, a2, a3) = print_topology(s1, s2, s3).await?;
    assert_eq!((a1.instance_id, a2.instance_id, a3.instance_id), (1, 1, 1));

    println!("\n--- Step 2: Inject panic into stage-2-transform (expected stderr panic output follows; stage-2 & stage-3 will restart, stage-1 stays untouched) ---");
    let _ = s2.call(StageMsg::InjectPanic).await;

    // Immediately query all three existing clients — zero manual reconnect logic!
    let (b1, b2, b3) = print_topology(s1, s2, s3).await?;
    assert_eq!(b1.instance_id, 1, "stage-1 must remain untouched");
    assert_eq!(b1.processed, 3, "stage-1 state must stay intact");
    assert_eq!(b2.instance_id, 2, "stage-2 must restart");
    assert_eq!(b3.instance_id, 2, "stage-3 must restart because it depends on stage-2");

    println!("\n--- Step 3: Inject panic into stage-3-sink (expected stderr panic output follows; ONLY stage-3 will restart) ---");
    let _ = s3.call(StageMsg::InjectPanic).await;
    let (c1, c2, c3) = print_topology(s1, s2, s3).await?;
    assert_eq!(c1.instance_id, 1);
    assert_eq!(c2.instance_id, 2);
    assert_eq!(c3.instance_id, 3);

    Ok(())
}

async fn run_interactive(
    s1: &ActorClient<PipelineStage>,
    s2: &ActorClient<PipelineStage>,
    s3: &ActorClient<PipelineStage>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    loop {
        print_topology(s1, s2, s3).await?;
        println!("\nChoose action: [1] panic stage-1 | [2] panic stage-2 | [3] panic stage-3 | [p] process item | [q] quit");
        let Some(input) = lines.next_line().await? else {
            break;
        };
        match input.trim() {
            "1" => {
                println!("Injecting panic in stage-1 (expected stderr panic output follows)...");
                let _ = s1.call(StageMsg::InjectPanic).await;
            }
            "2" => {
                println!("Injecting panic in stage-2 (expected stderr panic output follows)...");
                let _ = s2.call(StageMsg::InjectPanic).await;
            }
            "3" => {
                println!("Injecting panic in stage-3 (expected stderr panic output follows)...");
                let _ = s3.call(StageMsg::InjectPanic).await;
            }
            "p" => {
                s1.call(StageMsg::Process("manual".into())).await?;
                s2.call(StageMsg::Process("manual".into())).await?;
                s3.call(StageMsg::Process("manual".into())).await?;
            }
            "q" => break,
            other => println!("Unknown command '{other}'"),
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RestForOne Pipeline & Panic Recovery ===");

    let policy = RestartPolicy::new(10, Duration::from_secs(30))
        .initial_backoff(Duration::from_millis(20));

    let root = Supervisor::new_root();
    let pipeline = Supervisor::builder("pipeline")
        .strategy(SupervisionStrategy::RestForOne)
        .spawn(&root)
        .await?;

    let s1 = StatefulActorBuilder::from_instance(
        "stage-1-ingress",
        PipelineStage {
            stage_name: "stage-1-ingress",
            processed: 0,
        },
    )
    .restart_type(RestartType::Permanent)
    .restart_policy(policy)
    .spawn(&pipeline)
    .await?;

    let s2 = StatefulActorBuilder::from_instance(
        "stage-2-transform",
        PipelineStage {
            stage_name: "stage-2-transform",
            processed: 0,
        },
    )
    .restart_type(RestartType::Permanent)
    .restart_policy(policy)
    .spawn(&pipeline)
    .await?;

    let s3 = StatefulActorBuilder::from_instance(
        "stage-3-sink",
        PipelineStage {
            stage_name: "stage-3-sink",
            processed: 0,
        },
    )
    .restart_type(RestartType::Permanent)
    .restart_policy(policy)
    .spawn(&pipeline)
    .await?;

    let interactive = std::env::args().any(|arg| arg == "--interactive");
    if interactive {
        run_interactive(&s1, &s2, &s3).await?;
    } else {
        run_automated(&s1, &s2, &s3).await?;
    }

    println!("\n--- Graceful Shutdown (reverse registration order) ---");
    root.shutdown().await?;
    Ok(())
}

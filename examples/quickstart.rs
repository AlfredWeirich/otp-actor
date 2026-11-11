//! # Quickstart: Zero-Polling Self-Healing on Actor Panic
//!
//! Demonstrates `otp-actor`'s core differentiator in ~35 lines of code:
//! 1. Zero-macro `StatefulActor` using native `impl Future` (no `#[async_trait]`, no `Box::pin`).
//! 2. Real task panic isolation inside a Tokio `Supervisor`.
//! 3. Automatic 0-ms client rebinding via `watch<SlotState>` with no polling or reconnect logic.
//!
//! Run with:
//! ```bash
//! cargo run --example quickstart
//! ```

use otp_actor::{
    ActorContext, ActorError, StatefulActor, StatefulActorBuilder, Supervisor,
};

#[derive(Default)]
struct Counter(i64);

enum Msg {
    Add(i64),
    Get,
    PanicNow,
}

impl StatefulActor for Counter {
    type Message = Msg;
    type Response = i64;
    type Error = String;

    async fn handle(&mut self, msg: Msg, ctx: &mut ActorContext<Self>) -> Result<i64, String> {
        match msg {
            Msg::Add(n) => {
                self.0 += n;
                Ok(self.0)
            }
            Msg::Get => Ok(self.0),
            Msg::PanicNow => panic!("boom inside instance #{}!", ctx.instance_id()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Supervisor::new_root();
    let client = StatefulActorBuilder::from_default::<Counter>("counter")
        .spawn(&root)
        .await?;

    assert_eq!(client.call(Msg::Add(42)).await?, 42);
    println!("Before panic: counter = {}", client.call(Msg::Get).await?);

    // Inject a real Rust panic inside the actor (stderr panic output is expected).
    println!(
        "Injecting real panic into actor (the 'panicked at ...' stderr output below is expected)..."
    );
    let crash = client.call(Msg::PanicNow).await;
    assert!(matches!(crash, Err(ActorError::ActorUnavailable)));

    // Immediately reuse the SAME client handle — zero sleep, zero polling, zero reconnect code.
    // `ActorClient` waits on `watch<SlotState>`, rebinds as soon as instance #2 spawns,
    // and delivers the recovered non-Clone `Msg::Get` to a fresh state.
    let after = client.call(Msg::Get).await?;
    assert_eq!(after, 0);
    println!("After panic (same ActorClient, fresh instance #2): counter = {after}");

    root.shutdown().await?;
    Ok(())
}

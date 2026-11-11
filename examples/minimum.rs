//! Minimal, output-free example demonstrating both actor types (`ResourceActor` and `StatefulActor`)
//! supervised under a single root `Supervisor`.

use otp_actor::{
    ActorContext, ResourceActor, StatefulActor, StatefulActorBuilder, Supervisor, TimedResource,
};

/// Internal state of the `StatefulActor`.
/// `Default` allows spawning via `StatefulActorBuilder::from_default` on start and restart.
#[derive(Default)]
struct Counter(i64);

/// Message protocol accepted by `Counter`.
enum Msg {
    /// Increments the internal counter by the given delta.
    Add(i64),
    /// Queries the current counter value.
    Get,
}

impl StatefulActor for Counter {
    type Message = Msg;
    type Response = i64;
    type Error = String;

    // Processes incoming `call` (and, by default, `cast`) messages sequentially with `&mut self`.
    async fn handle(&mut self, msg: Msg, _ctx: &mut ActorContext<Self>) -> Result<i64, String> {
        match msg {
            Msg::Add(n) => {
                self.0 += n;
                Ok(self.0)
            }
            Msg::Get => Ok(self.0),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start the top-level root supervisor that manages the lifecycle of all child actors.
    let root = Supervisor::new_root();

    // 1. ResourceActor: fetches, caches, and serves a resource via a 0-hop watch snapshot.
    // Here, the worker closure produces a non-expiring (`perpetual`) value on first request.
    let resource = ResourceActor::builder("resource")
        .worker(|_| async { Ok::<_, String>(TimedResource::perpetual("value")) })
        .spawn(&root)
        .await?;

    // Triggers the worker on cold start and returns the cached value (subsequent reads are 0-hop).
    assert_eq!(resource.get().await?, "value");

    // 2. StatefulActor: general-purpose `gen_server`-style actor encapsulating mutable state.
    let counter = StatefulActorBuilder::from_default::<Counter>("counter")
        .spawn(&root)
        .await?;

    // Send a fire-and-forget notification (`cast`) to mutate state asynchronously.
    counter.cast(Msg::Add(1)).await?;
    counter.cast(Msg::Add(1)).await?;
    counter.cast(Msg::Add(1)).await?;

    // Send a request-response query (`call`) and await the actor's reply.
    assert_eq!(counter.call(Msg::Get).await?, 3);

    // Gracefully stop all supervised actors in reverse registration order before exiting.
    root.shutdown().await?;
    Ok(())
}

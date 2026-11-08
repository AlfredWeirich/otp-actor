//! build.rs
//!
//! Build script for `otp-actor`.
//! When the `remote` feature is enabled, compiles the universal actor gRPC proto definition
//! into Rust stubs using `tonic-prost-build`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "remote")]
    {
        println!("cargo:rerun-if-changed=proto/actor_node.proto");
        tonic_prost_build::configure().compile_protos(&["proto/actor_node.proto"], &["proto"])?;
    }
    Ok(())
}

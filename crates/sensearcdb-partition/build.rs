//! prost + tonic build script for sensearcdb-partition proto definitions.
//!
//! Compiles `partition.proto` into Rust code,
//! generating both message types (prost) and gRPC service stubs (tonic).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(&["src/proto/partition.proto"], &["src/proto"])?;
    Ok(())
}

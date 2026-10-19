//! prost + tonic build script for sensearcdb-ingest proto definitions.
//!
//! Compiles `ingest.proto` and `point_batch.proto` into Rust code,
//! generating both message types (prost) and gRPC service stubs (tonic).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &["src/proto/point_batch.proto", "src/proto/ingest.proto"],
        &["src/proto"],
    )?;
    Ok(())
}

//! Build script for sensearcdb-schema.
//!
//! Compiles the schema.proto file into Rust code using tonic-build / prost.
//! Uses `protoc-bin-vendored` to provide a self-contained protoc binary.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use vendored protoc binary (no system protoc needed)
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&["src/proto/schema.proto"], &["src/proto/"])?;
    Ok(())
}

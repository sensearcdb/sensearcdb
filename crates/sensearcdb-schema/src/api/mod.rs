//! Schema REST 与 gRPC API。
//!
//! - `rest`: axum-based REST 端点（`/api/v1/schema/*`）
//! - `grpc`: tonic-based gRPC SchemaService

pub mod grpc;
pub mod rest;

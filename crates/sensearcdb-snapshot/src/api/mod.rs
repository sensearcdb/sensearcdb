//! Snapshot API 模块。
//!
//! - `grpc` — 纯 gRPC 数据面（GetSnapshot / BatchGetSnapshot / ListSnapshots /
//!   SubscribeSnapshots），取代原 REST/SSE 端点。

pub mod grpc;

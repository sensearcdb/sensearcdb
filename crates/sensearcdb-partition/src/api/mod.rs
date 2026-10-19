//! Partition admin API module.
//!
//! Provides both REST and gRPC interfaces for partition management:
//! - REST: `GET/POST /api/partitions/*`
//! - gRPC: `PartitionAdmin` service (List/Get/Freeze/Advance)

pub mod grpc;
pub mod rest;

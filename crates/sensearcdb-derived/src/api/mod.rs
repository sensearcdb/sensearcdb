//! API module for the derived tag engine.
//!
//! Provides REST and gRPC management endpoints for:
//! - Registering/unregistering derived tags
//! - Validating expressions
//! - Querying tag status and dependency graph

pub mod grpc;
pub mod rest;

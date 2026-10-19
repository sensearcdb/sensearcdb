//! # sensearcdb-cq
//!
//! SenseArcDB CQ (Continuous Query) engine.
//!
//! Timer-driven 4-layer tumbling window pre-aggregation (CQ_10s / CQ_1m / CQ_1h / CQ_1d)
//! with hierarchical sum+count passing and AutoRouter for transparent query acceleration.
//!
//! ## Architecture
//!
//! - `CqEngine`: main entry point, owns definitions, window states, scheduler
//! - `CqScheduler`: 1s tick loop, window alignment, concurrency control
//! - `CqDefinition` / `CqGranularity`: persisted CQ configuration
//! - `CqWindowKey` / `CqWindowState`: window lifecycle management
//! - Aggregation executor: reads source data, computes aggregates, writes results
//! - Recalculation queue: handles late-arriving data via dirty marking
//! - AutoRouter: maps query windows to optimal CQ granularity
//!
//! ## Crate Boundaries
//!
//! CQ uses `AggregateState` and `AggFunction` from `sensearcdb-common`.
//! CQ is called by the engine's write path (`on_data_written`) and schema events.
//! The engine's `AutoRouter` (query module) consumes CQ window information
//! via a registration interface.

pub mod aggregation;
pub mod auto_router;
pub mod column_resolver;
pub mod definition;
pub mod definition_store;
pub mod engine;
pub mod layout;
pub mod metrics;
pub mod recalculation;
pub mod scheduler;
pub mod window;

pub mod api {
    //! CQ management API (REST + gRPC).
    pub mod grpc;
    pub mod rest;
}

// Re-exports for convenient crate-level access.
pub use aggregation::{CqAggregationExecutor, CqWindowResult};
pub use auto_router::AutoRouter as CqAutoRouter;
pub use column_resolver::{CqColumnResolver, FormulaColumnResolver, SchemaColumnResolver};
pub use definition::{CqDefinition, CqGranularity, CqStatus};
pub use definition_store::CqDefinitionStore;
pub use engine::CqEngine;
pub use layout::{aux_column_id, CqAuxColumn, AUX_COLUMNS_PER_SOURCE};
pub use metrics::CqMetrics;
pub use recalculation::{is_window_stale, mark_window_dirty, RecalculationQueue};
pub use scheduler::CqScheduler;
pub use window::{CqWindowKey, CqWindowState, CqWindowStatus};

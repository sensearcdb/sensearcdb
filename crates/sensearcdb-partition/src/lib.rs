//! sensearcdb-partition: Time-based partition management for SenseArcDB.
//!
//! Manages the partition state machine (Active → Frozen → Compacting → Archived → Expired),
//! write routing, disk recovery, and partition admin APIs.
//!
//! # Architecture
//!
//! ```text
//! PartitionManager
//! ├── partitions: DashMap<table_id → BTreeMap<partition_ns → PartitionInfo>>
//! ├── metadata: PartitionMetadataStore (config.db partitions table)
//! ├── event_bus: PartitionEventBus (tokio::broadcast, capacity=1024)
//! ├── scheduler: PartitionScheduler (time-driven background task)
//! └── metrics: PartitionMetrics (Prometheus)
//! ```
//!
//! # Key APIs
//!
//! - `PartitionManager::route_to_partition(table_id, ts_ns)` — write routing
//! - `PartitionManager::get_partitions_for_range(table_id, start, end)` — query routing
//! - `PartitionManager::advance_partition(table_id)` — manual advance
//! - `PartitionManager::event_bus().subscribe()` — event subscription
//!
//! # State Machine
//!
//! ```text
//! Active (writable)
//!   │ time boundary or manual force_freeze
//!   ▼
//! Frozen (no writes, waiting for L1→L2 compaction)
//!   │ compaction scheduled
//!   ▼
//! Compacting (L1→L2 in progress)
//!   │ compaction success         │ compaction retry exhausted
//!   ▼                            ▼
//! Archived (read-only L2)       Frozen (rollback)
//!   │ TTL expired
//!   ▼
//! Expired (awaiting deletion)
//! ```

pub mod advance;
pub mod api;
pub mod event;
pub mod manager;
pub mod metadata;
pub mod metrics;
pub mod recovery;
pub mod route;
pub mod scheduler;
pub mod state;

// ─── Proto-generated code ───

/// Proto-generated code for partition admin services and messages.
///
/// Generated from `src/proto/partition.proto` via `tonic-build` in `build.rs`.
pub mod proto {
    tonic::include_proto!("sensearcdb.partition");
}

// ─── Re-exports ───

pub use advance::AdvanceResult;
pub use event::{PartitionEvent, PartitionEventBus};
pub use manager::PartitionManager;
pub use route::RouteError;
pub use state::{PartitionInfo, PartitionState};

//! # sensearcdb-stream
//!
//! SenseArcDB Stream Compute engine.
//!
//! Data-driven streaming aggregation with Tumbling/Hopping/Session windows
//! defined via `CREATE STREAM COMPUTE` SQL statements.
//! Results are written to user-specified target tables through the standard write path.
//!
//! ## Architecture
//!
//! - `StreamComputeEngine`: main entry point, owns task registry, data event bus, consume loops
//! - `TaskRegistry` / `StreamTask`: persisted stream task configuration
//! - `WindowManager`: two-level locking, per-window bucket aggregation
//! - `DataEventBus`: write-path callback routing to mpsc channels
//! - `ConsumeLoop`: tokio task, channel → WindowManager → write results
//! - `SessionTimer`: periodic session timeout checker
//! - `StreamSqlParser`: CREATE/DROP/SHOW STREAM COMPUTE SQL parsing
//!
//! ## Crate Boundaries
//!
//! Stream uses `AggregateState` and `AggFunction` from `sensearcdb-common`.
//! Stream receives data events through the engine's write path (`on_write` callback).
//! Results are written to user-specified target tables via `StreamSink` trait.
//! Table/column name resolution is handled by `StreamCatalog` trait,
//! avoiding compile-time dependency on `sensearcdb-schema`.

pub mod consume_loop;
pub mod data_event_bus;
pub mod engine;
pub mod metrics;
pub mod session_timer;
pub mod sql_parser;
pub mod task;
pub mod task_store;
pub mod window_manager;

pub mod api {
    //! Stream compute management API (REST + gRPC).
    pub mod grpc;
    pub mod rest;
}

use sensearcdb_common::{DataType, TenantId, TypedValue};

// ─── Traits for cross-crate decoupling ───

/// Write sink for streaming window results.
///
/// Implemented by the main binary using `Engine::write_batch()`.
/// Stream module uses this trait to write aggregated results to target tables
/// without depending on `sensearcdb-engine` directly.
pub trait StreamSink: Send + Sync {
    /// Write a batch of window results to the target table.
    ///
    /// # Arguments
    /// * `target_table_id` - Target table ID.
    /// * `rows` - Vector of (timestamp_millis, column_values).
    fn write_batch(
        &self,
        target_table_id: u32,
        rows: &[(i64, Vec<TypedValue>)],
    ) -> Result<(), String>;
}

/// Catalog resolver for stream task creation.
///
/// Implemented by the main binary using `sensearcdb_schema::Catalog`.
/// Stream module uses this trait to resolve SQL-level table/column names to IDs
/// without depending on `sensearcdb-schema` directly.
pub trait StreamCatalog: Send + Sync {
    /// Resolve a table name to its table_id（租户作用域：name→id 解析需 tenant）。
    /// Returns `None` if the table does not exist.
    fn resolve_table(&self, tenant_id: TenantId, table_name: &str) -> Option<u32>;

    /// Resolve a column name within a table to its column_id.
    /// Returns `None` if the table or column does not exist.
    fn resolve_column(&self, table_id: u32, column_name: &str) -> Option<u16>;

    /// Resolve a column name within a table to its physical [`DataType`].
    /// Returns `None` if the table or column does not exist.
    fn resolve_column_type(&self, table_id: u32, column_name: &str) -> Option<DataType>;

    /// Auto-create a target table for stream results（租户作用域：建表 tenant 用入参）。
    ///
    /// `columns` carries the target column name and physical type, so the target
    /// table schema reflects the real aggregation result type (not a hard-coded F64).
    /// Returns the newly created table's ID, or an error message.
    fn auto_create_target_table(
        &self,
        tenant_id: TenantId,
        table_name: &str,
        columns: &[(String, DataType)],
    ) -> Result<u32, String>;

    /// Verify that a table exists.
    fn table_exists(&self, table_id: u32) -> bool;

    /// 反查 table_id → 表名（供前端 `StreamTask.source/sink` 形状适配使用）。
    ///
    /// 默认返回 `None`（未实现反查的 catalog 可继承默认实现，不破坏既有 impl）。
    fn table_name(&self, table_id: u32) -> Option<String> {
        let _ = table_id;
        None
    }
}

/// 计算流聚合结果列的物理数据类型（P1-12 次级缺口：不硬编码 F64）。
///
/// 已下沉到 `sensearcdb-common` 的 [`agg_result_type`]，此处以别名 re-export，
/// 保证 CQ 与 Stream 共用同一张映射表、杜绝两处漂移（Phase7-B5 P4）。
pub use sensearcdb_common::agg_result_type as stream_result_type;

// Re-exports for convenient crate-level access.
pub use consume_loop::ConsumeLoop;
pub use data_event_bus::{DataEvent, DataEventBus};
pub use engine::StreamComputeEngine;
pub use session_timer::SessionTimer;
pub use sql_parser::{
    parse_interval_ms, parse_stream_sql, AggregationDef, ParseError, StreamSqlStatement,
};
pub use task::{
    PartitionBySpec, ProjectionColumn, ProjectionSpec, StreamTask, TargetColumnDef, TaskId,
    TaskRegistry, TaskStatus, WindowSpec,
};
pub use task_store::StreamTaskStore;
pub use window_manager::{PartitionBucket, PartitionKey, WindowManager, WindowResult};

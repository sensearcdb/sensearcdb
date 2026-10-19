//! # sensearcdb-common
//!
//! SenseArcDB 共享类型、错误、常量与配置管理。
//!
//! 本 crate 是 sensearcdb-engine、sensearcdb-schema、sensearcdb-snapshot
//! 之间的唯一编译期依赖桥梁。三个模块间无直接的编译期交叉依赖，
//! 所有共享类型均通过本 crate 导出。

pub mod aggregate;
pub mod api_response;
pub mod config;
pub mod config_whitelist;
pub mod constants;
pub mod data_source;
pub mod error;
pub mod node_id;
pub mod route;
pub mod route_registry;
pub mod system_config_store;
pub mod time;
pub mod types;
pub mod write_attr;

// ─── Re-exports: 最常用类型提升到 crate 根 ───

pub use types::{
    AnomalyScore, BackoffConfig, ColumnRef, ConditionId, DataType, Durability, ForecastHorizon,
    PointFlags, Quality, TenantId, TimestampNanos, TypedValue,
};

pub use error::SenseArcError;

pub use config::{
    AiOpsConfig, AnomalyConfig, AppConfig, AssetModelConfig, ConditionConfig, ConfigHandle,
    EngineConfig, ForecastConfig, PathsConfig, ReplayConfig, SnapshotConfig, TenantConfig,
};

pub use config_whitelist::{
    apply_whitelist_value, whitelist_value, whitelist_values, WHITELIST_KEYS,
};

pub use system_config_store::SystemConfigStore;

pub use aggregate::{agg_result_type, AggFunction, AggregateState, Numeric};

pub use route::{CqRouteHint, CqRouteProvider};

pub use time::align_to_partition;

pub use node_id::parse_node_id;

pub use data_source::{
    AnomalyReader, AnomalyRecord, ConditionReader, ConditionSnapshot, DegradationReader,
    HistoryReader, NoopAnomalyReader, NoopConditionReader, NoopDegradationReader,
    NoopHistoryReader, NoopSnapshotReader, NoopTagEnumerator, NoopTopologyProvider,
    SnapshotReader, TagEnumerator, TopologyProvider, TopologyRelation,
};

pub use api_response::ApiResponse;

//! 查询引擎模块。
//!
//! SenseArcDB 的 SQL 查询引擎，基于 sqlparser-rs 构建。
//! 支持时间范围查询、列投影、LIMIT 下推和 k-way merge。
//!
//! ## 模块
//!
//! - `dialect`: SenseArcDialect（反引号标识符、time_bucket、SAMPLE BY 扩展）
//! - `planner`: QueryPlanner（SQL 解析 → LogicalPlan + 列投影 + LIMIT 下推）
//! - `executor`: QueryExecutor（k-way merge 小顶堆 → Arrow RecordBatch）
//! - `autorouter`: AutoRouter — 通过 `CqRouteProvider` trait 将查询路由到 Raw 或 CQ 路径

pub mod autorouter;
pub mod dialect;
pub mod executor;
pub mod planner;

pub use autorouter::AutoRouter;
pub use dialect::{
    extract_sample_by, parse_sample_interval_ms, parse_sensearc_sql, FillStrategy, SampleByClause,
    SenseArcDialect, SenseArcStatement,
};
pub use executor::QueryExecutor;
pub use planner::QueryPlanner;

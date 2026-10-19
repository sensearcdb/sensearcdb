//! SenseArcDB 可观测性模块。
//!
//! 提供 Prometheus /metrics 端点 (5101)、慢查询日志异步写入、
//! tracing JSON STDOUT 输出、Grafana Dashboard JSON 和 PrometheusRule YAML。
//!
//! > 阶段 5 收尾后，readiness 职责已收敛到 ingest 的 `/health/ready`（5100），
//! > 本 crate 仅保留 /metrics（5101）这一已接线的 metrics 提供者身份。
//!
//! ## 硬约束
//!
//! - Counter/Gauge = prometheus crate 内部 AtomicU64 + Relaxed
//! - Histogram = prometheus crate 内部 AtomicU64 bucket 数组（绝不使用 std::sync::Mutex）
//! - 慢查询 Channel 满则丢弃 + inc counter，绝不 retry/sleep/yield/spawn_blocking
//! - TRACE 采样在 Layer 层 enabled() 阶段完成，禁止业务代码 rand()
//!
//! ## 模块结构
//!
//! - `engine` — ObserveEngine: 指标/慢查询生命周期
//! - `metrics` — MetricsRegistry: once_cell Lazy 全局注册
//! - `slow_query` — SlowQueryWriter: 有界 Channel + BufWriter
//! - `tracing_setup` — tracing-subscriber JSON layer + Layer 层 TRACE 采样
//! - `trace_context` — W3C trace_id/span_id
//! - `api` — REST 端点: GET /metrics
//! - `serve` — 独立 axum server (5101)

pub mod api;
pub mod engine;
pub mod metrics;
pub mod serve;
pub mod slow_query;
pub mod trace_context;
pub mod tracing_setup;

// 顶层 re-export — 方便外部 crate 使用
pub use engine::ObserveEngine;
pub use metrics::MetricsRegistry;
pub use slow_query::fingerprint::fingerprint_sql;
pub use slow_query::log::SlowQueryLog;
pub use slow_query::SlowQueryWriter;
pub use trace_context::TraceContext;
pub use tracing_setup::init_tracing;

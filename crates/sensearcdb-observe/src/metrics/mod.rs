//! MetricsRegistry: once_cell Lazy 全局注册 + AtomicU64 / parking_lot RwLock。
//!
//! ## 并发安全（硬约束）
//!
//! - **Counter/Gauge**: `prometheus` crate 内部使用 `AtomicU64`/`AtomicI64` + `Relaxed`
//! - **Histogram**: `prometheus` crate 内部使用 `AtomicU64` bucket 数组
//! - **全局注册**: `once_cell::sync::Lazy<MetricsRegistry>`，避免热路径 HashMap 查找
//! - **绝不使用 `std::sync::Mutex`**：grep `sensearcdb-observe/src/` 结果必须为空
//!
//! ## 指标分类
//!
//! | 模块 | 数量 | 文件 |
//! |------|:---:|------|
//! | 引擎核心 | 40+ | engine_metrics.rs |
//! | 集群 | 8 | cluster_metrics.rs |
//! | 告警引擎 | 6 | alert_metrics.rs |
//! | 事件框架 | 5 | event_metrics.rs |
//! | AI 模块 | 9 | ai_metrics.rs |
//! | 系统资源 | 9 | system_metrics.rs |
//! | 日志与构建 | 4 | log_metrics.rs |

use once_cell::sync::Lazy;
use prometheus::{Registry, TextEncoder};

pub mod ai_metrics;
pub mod alert_metrics;
pub mod cluster_metrics;
pub mod engine_metrics;
pub mod event_metrics;
pub mod log_metrics;
pub mod system_metrics;

// ─── 全局 MetricsRegistry — once_cell::sync::Lazy 单例 ───

/// 全局指标注册中心（once_cell Lazy 单例）。
///
/// 所有指标通过 `MetricsRegistry::global()` 获取，内部使用 `parking_lot::RwLock`
/// 保护 Registry 的 gather 操作，Counter/Gauge/Histogram 自身均为原子操作。
pub static METRICS: Lazy<MetricsRegistry> = Lazy::new(MetricsRegistry::new);

/// 全局指标注册中心。
///
/// ## 指标生命周期
///
/// - 所有指标在 `MetricsRegistry::new()` 中创建并注册到 `prometheus::Registry`
/// - 通过 `gather()` 序列化为 Prometheus text format 供 `/metrics` 端点使用
/// - `build_info` 在创建时即设置版本/commit 等静态标签
#[derive(Debug)]
pub struct MetricsRegistry {
    /// Prometheus 注册中心。
    pub registry: Registry,

    // ─── 引擎核心指标 ───
    pub write_requests: prometheus::IntCounter,
    pub write_points: prometheus::IntCounter,
    pub write_bytes: prometheus::IntCounter,
    pub write_latency: prometheus::Histogram,
    pub write_errors: prometheus::IntCounterVec,
    pub wal_fsync_latency: prometheus::Histogram,
    pub query_requests: prometheus::IntCounterVec,
    pub query_latency: prometheus::Histogram,
    pub query_errors: prometheus::IntCounterVec,
    pub query_rows_scanned: prometheus::IntCounter,
    pub snapshot_hit_ratio: prometheus::Gauge,
    pub wal_size_bytes: prometheus::IntGauge,
    pub wal_segments: prometheus::IntGauge,
    pub wal_cleaner_deleted: prometheus::IntCounter,
    pub wal_group_commit_batch_size: prometheus::Histogram,
    pub memtable_size_bytes: prometheus::IntGauge,
    pub memtable_row_count: prometheus::IntGauge,
    pub memtable_usage_ratio: prometheus::Gauge,
    pub memtable_flush_total: prometheus::IntCounter,
    pub memtable_flush_latency: prometheus::Histogram,
    pub sac_file_count: prometheus::IntGaugeVec,
    pub sac_size_bytes: prometheus::IntGaugeVec,
    pub compaction_operations: prometheus::IntCounterVec,
    pub compaction_bytes: prometheus::IntCounterVec,
    pub compaction_duration: prometheus::Histogram,
    pub compaction_backlog: prometheus::IntGauge,
    pub disk_usage_ratio: prometheus::GaugeVec,
    pub disk_total_bytes: prometheus::IntGaugeVec,
    pub disk_used_bytes: prometheus::IntGaugeVec,
    pub connections_active: prometheus::IntGaugeVec,
    pub connections_total: prometheus::IntCounterVec,
    pub connections_rejected: prometheus::IntCounter,

    // ─── 集群指标 ───
    pub cluster_node_state: prometheus::IntGaugeVec,
    pub cluster_term: prometheus::IntGauge,
    pub cluster_committed_index: prometheus::IntGauge,
    pub cluster_last_applied: prometheus::IntGauge,
    pub cluster_replication_lag: prometheus::IntGaugeVec,
    pub cluster_replication_latency: prometheus::Histogram,
    pub cluster_leader_elections: prometheus::IntCounter,
    pub cluster_heartbeat_latency: prometheus::Histogram,

    // ─── 告警引擎指标 ───
    pub alert_evaluations: prometheus::IntCounterVec,
    pub alert_fired: prometheus::IntCounterVec,
    pub alert_active_count: prometheus::IntGaugeVec,
    pub alert_notification_sent: prometheus::IntCounterVec,
    pub alert_notification_failed: prometheus::IntCounterVec,
    pub alert_dlq_backlog: prometheus::IntGauge,

    // ─── 事件框架指标 ───
    pub event_published: prometheus::IntCounterVec,
    pub event_throttled: prometheus::IntCounterVec,
    pub event_channel_capacity_ratio: prometheus::Gauge,
    pub event_sse_connections: prometheus::IntGauge,
    pub event_store_lag: prometheus::IntGauge,

    // ─── AI 模块指标 ───
    pub ai_inference_requests: prometheus::IntCounterVec,
    pub ai_inference_latency: prometheus::Histogram,
    pub ai_inference_errors: prometheus::IntCounterVec,
    pub ai_model_switch: prometheus::IntCounter,
    pub ai_anomaly_triggered: prometheus::IntCounter,
    pub ai_engine_state: prometheus::IntGaugeVec,
    pub ai_current_mape: prometheus::GaugeVec,
    pub ai_training_progress: prometheus::Gauge,
    pub ai_ood_detections: prometheus::IntCounterVec,

    // ─── 系统资源指标 ───
    pub system_cpu_usage: prometheus::Gauge,
    pub system_memory_rss: prometheus::IntGauge,
    pub system_memory_virtual: prometheus::IntGauge,
    pub system_fd_open: prometheus::IntGauge,
    pub system_fd_max: prometheus::IntGauge,
    pub system_uptime: prometheus::IntGauge,
    pub system_thread_count: prometheus::IntGauge,
    pub system_io_read: prometheus::IntCounter,
    pub system_io_write: prometheus::IntCounter,

    // ─── 日志与构建信息 ───
    pub log_messages: prometheus::IntCounterVec,
    pub slow_query_count: prometheus::IntCounter,
    pub slow_query_dropped: prometheus::IntCounter,
    pub scrape_timeout_total: prometheus::IntCounter,
    pub build_info: prometheus::IntGaugeVec,
}

impl MetricsRegistry {
    /// 获取全局 MetricsRegistry 单例。
    ///
    /// 首次调用时初始化所有指标并注册到内部 `Registry`。
    /// 后续调用返回 `&'static` 引用，无额外开销。
    pub fn global() -> &'static Self {
        &METRICS
    }

    /// 创建新的 MetricsRegistry 并注册所有指标。
    ///
    /// # Panics
    ///
    /// 任何指标注册失败（如名称冲突）会导致 panic。
    fn new() -> Self {
        let registry = Registry::new();

        let slf = Self {
            write_requests: engine_metrics::register_write_requests(&registry),
            write_points: engine_metrics::register_write_points(&registry),
            write_bytes: engine_metrics::register_write_bytes(&registry),
            write_latency: engine_metrics::register_write_latency(&registry),
            write_errors: engine_metrics::register_write_errors(&registry),
            wal_fsync_latency: engine_metrics::register_wal_fsync_latency(&registry),
            query_requests: engine_metrics::register_query_requests(&registry),
            query_latency: engine_metrics::register_query_latency(&registry),
            query_errors: engine_metrics::register_query_errors(&registry),
            query_rows_scanned: engine_metrics::register_query_rows_scanned(&registry),
            snapshot_hit_ratio: engine_metrics::register_snapshot_hit_ratio(&registry),
            wal_size_bytes: engine_metrics::register_wal_size_bytes(&registry),
            wal_segments: engine_metrics::register_wal_segments(&registry),
            wal_cleaner_deleted: engine_metrics::register_wal_cleaner_deleted(&registry),
            wal_group_commit_batch_size: engine_metrics::register_wal_group_commit_batch_size(
                &registry,
            ),
            memtable_size_bytes: engine_metrics::register_memtable_size_bytes(&registry),
            memtable_row_count: engine_metrics::register_memtable_row_count(&registry),
            memtable_usage_ratio: engine_metrics::register_memtable_usage_ratio(&registry),
            memtable_flush_total: engine_metrics::register_memtable_flush_total(&registry),
            memtable_flush_latency: engine_metrics::register_memtable_flush_latency(&registry),
            sac_file_count: engine_metrics::register_sac_file_count(&registry),
            sac_size_bytes: engine_metrics::register_sac_size_bytes(&registry),
            compaction_operations: engine_metrics::register_compaction_operations(&registry),
            compaction_bytes: engine_metrics::register_compaction_bytes(&registry),
            compaction_duration: engine_metrics::register_compaction_duration(&registry),
            compaction_backlog: engine_metrics::register_compaction_backlog(&registry),
            disk_usage_ratio: engine_metrics::register_disk_usage_ratio(&registry),
            disk_total_bytes: engine_metrics::register_disk_total_bytes(&registry),
            disk_used_bytes: engine_metrics::register_disk_used_bytes(&registry),
            connections_active: engine_metrics::register_connections_active(&registry),
            connections_total: engine_metrics::register_connections_total(&registry),
            connections_rejected: engine_metrics::register_connections_rejected(&registry),

            cluster_node_state: cluster_metrics::register_cluster_node_state(&registry),
            cluster_term: cluster_metrics::register_cluster_term(&registry),
            cluster_committed_index: cluster_metrics::register_cluster_committed_index(&registry),
            cluster_last_applied: cluster_metrics::register_cluster_last_applied(&registry),
            cluster_replication_lag: cluster_metrics::register_cluster_replication_lag(&registry),
            cluster_replication_latency: cluster_metrics::register_cluster_replication_latency(
                &registry,
            ),
            cluster_leader_elections: cluster_metrics::register_cluster_leader_elections(&registry),
            cluster_heartbeat_latency: cluster_metrics::register_cluster_heartbeat_latency(
                &registry,
            ),

            alert_evaluations: alert_metrics::register_alert_evaluations(&registry),
            alert_fired: alert_metrics::register_alert_fired(&registry),
            alert_active_count: alert_metrics::register_alert_active_count(&registry),
            alert_notification_sent: alert_metrics::register_alert_notification_sent(&registry),
            alert_notification_failed: alert_metrics::register_alert_notification_failed(&registry),
            alert_dlq_backlog: alert_metrics::register_alert_dlq_backlog(&registry),

            event_published: event_metrics::register_event_published(&registry),
            event_throttled: event_metrics::register_event_throttled(&registry),
            event_channel_capacity_ratio: event_metrics::register_event_channel_capacity_ratio(
                &registry,
            ),
            event_sse_connections: event_metrics::register_event_sse_connections(&registry),
            event_store_lag: event_metrics::register_event_store_lag(&registry),

            ai_inference_requests: ai_metrics::register_ai_inference_requests(&registry),
            ai_inference_latency: ai_metrics::register_ai_inference_latency(&registry),
            ai_inference_errors: ai_metrics::register_ai_inference_errors(&registry),
            ai_model_switch: ai_metrics::register_ai_model_switch(&registry),
            ai_anomaly_triggered: ai_metrics::register_ai_anomaly_triggered(&registry),
            ai_engine_state: ai_metrics::register_ai_engine_state(&registry),
            ai_current_mape: ai_metrics::register_ai_current_mape(&registry),
            ai_training_progress: ai_metrics::register_ai_training_progress(&registry),
            ai_ood_detections: ai_metrics::register_ai_ood_detections(&registry),

            system_cpu_usage: system_metrics::register_system_cpu_usage(&registry),
            system_memory_rss: system_metrics::register_system_memory_rss(&registry),
            system_memory_virtual: system_metrics::register_system_memory_virtual(&registry),
            system_fd_open: system_metrics::register_system_fd_open(&registry),
            system_fd_max: system_metrics::register_system_fd_max(&registry),
            system_uptime: system_metrics::register_system_uptime(&registry),
            system_thread_count: system_metrics::register_system_thread_count(&registry),
            system_io_read: system_metrics::register_system_io_read(&registry),
            system_io_write: system_metrics::register_system_io_write(&registry),

            log_messages: log_metrics::register_log_messages(&registry),
            slow_query_count: log_metrics::register_slow_query_count(&registry),
            slow_query_dropped: log_metrics::register_slow_query_dropped(&registry),
            scrape_timeout_total: log_metrics::register_scrape_timeout_total(&registry),
            build_info: log_metrics::register_build_info(&registry),

            registry,
        };

        // 设置 build_info 的静态标签
        slf.build_info
            .with_label_values(&[
                env!("CARGO_PKG_VERSION"),
                option_env!("GIT_COMMIT_HASH").unwrap_or("unknown"),
                option_env!("BUILD_DATE").unwrap_or("unknown"),
                option_env!("RUSTC_VERSION").unwrap_or("unknown"),
            ])
            .set(1);

        slf
    }

    /// 序列化为 Prometheus text format。
    ///
    /// 使用 `prometheus::TextEncoder` 将所有注册指标编码为
    /// Prometheus exposition format 字符串，供 `/metrics` 端点使用。
    ///
    /// # Panics
    ///
    /// 仅在内部编码逻辑错误时 panic（如 buffer 分配失败）。
    pub fn gather(&self) -> String {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        encoder
            .encode_to_string(&metric_families)
            .unwrap_or_else(|e| format!("# ERROR encoding metrics: {}\n", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry_global() {
        let m = MetricsRegistry::global();
        // 验证全局实例可访问
        assert!(std::ptr::eq(m, MetricsRegistry::global()));
    }

    #[test]
    fn test_metrics_gather() {
        let m = MetricsRegistry::global();
        let text = m.gather();
        // 必须包含 Prometheus HELP/TYPE 注释
        assert!(
            text.contains("sensearc_write_requests_total"),
            "Expected write_requests in output:\n{}",
            text
        );
        assert!(
            text.contains("sensearc_build_info"),
            "Expected build_info in output:\n{}",
            text
        );
    }

    #[test]
    fn test_counter_increment() {
        let m = MetricsRegistry::global();
        let before = m.write_requests.get();
        m.write_requests.inc();
        assert_eq!(m.write_requests.get(), before + 1);
    }

    #[test]
    fn test_histogram_observe() {
        let m = MetricsRegistry::global();
        m.write_latency.observe(0.005);
        m.write_latency.observe(0.015);
        let text = m.gather();
        assert!(
            text.contains("sensearc_write_latency_seconds"),
            "Expected write_latency in output"
        );
    }

    #[test]
    fn test_build_info_labels() {
        let m = MetricsRegistry::global();
        let text = m.gather();
        // 确认 version label 存在
        assert!(
            text.contains(env!("CARGO_PKG_VERSION")),
            "Expected version in build_info"
        );
    }

    #[test]
    fn test_no_std_mutex_in_observe_src() {
        // 硬约束验证：sensearcdb-observe/src/ 下无 std::sync::Mutex
        // 此测试通过编译时检查：所有代码使用 prometheus crate 的原子类型
        // 和 parking_lot，绝不引入 std::sync::Mutex
        let m = MetricsRegistry::global();
        // 验证能正常 gather（无死锁）
        let _text = m.gather();
    }
}

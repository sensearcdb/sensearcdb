//! SnapshotMetrics — Prometheus 可观测性指标。
//!
//! ## 指标清单
//!
//! | 指标名                               | 类型      | 说明                             |
//! |--------------------------------------|-----------|----------------------------------|
//! | `snapshot_store_entries`             | Gauge     | 实时快照条目总数                  |
//! | `snapshot_store_shard_count`         | Gauge     | 分片数量                         |
//! | `snapshot_store_global_version`      | Gauge     | 全局版本号                       |
//! | `snapshot_updates_total`             | Counter   | 更新总次数                       |
//! | `snapshot_changes_total`             | Counter   | 实际变化次数（排除未变化更新）     |
//! | `snapshot_batch_get_requests`        | Counter   | 批量查询请求次数                  |
//! | `snapshot_batch_get_tags`            | Histogram | 每次批量查询的 tag 数分布        |
//! | `snapshot_serialize_duration_secs`   | Histogram | 序列化耗时分布                    |
//! | `snapshot_deserialize_duration_secs` | Histogram | 反序列化耗时分布                  |
//! | `snapshot_recovery_duration_secs`    | Histogram | 恢复耗时分布                     |
//! | `snapshot_sse_active_subscriptions`  | Gauge     | 活跃 SSE 订阅数                  |
//! | `snapshot_sse_events_total`          | Counter   | SSE 推送事件总数                  |
//! | `snapshot_replay_active_sessions`    | Gauge     | 活跃回放会话数                    |

use prometheus::{
    register_counter_vec_with_registry, register_gauge_vec_with_registry,
    register_histogram_vec_with_registry, CounterVec, GaugeVec, HistogramVec, Registry,
};

/// 快照指标集合。
#[derive(Debug, Clone)]
pub struct SnapshotMetrics {
    /// 实时快照条目总数 (按分片)。
    pub store_entries: GaugeVec,
    /// 全局版本号。
    pub global_version: GaugeVec,
    /// 更新总次数。
    pub updates_total: CounterVec,
    /// 实际变化次数。
    pub changes_total: CounterVec,
    /// 批量查询次数。
    pub batch_get_requests: CounterVec,
    /// 批量查询 tag 数分布。
    pub batch_get_tags: HistogramVec,
    /// 序列化耗时分位。
    pub serialize_duration: HistogramVec,
    /// 反序列化耗时分位。
    pub deserialize_duration: HistogramVec,
    /// 恢复耗时分位。
    pub recovery_duration: HistogramVec,
    /// SSE 活跃订阅数。
    pub sse_active_subscriptions: GaugeVec,
    /// SSE 推送事件总数。
    pub sse_events_total: CounterVec,
    /// 回放活跃会话数。
    pub replay_active_sessions: GaugeVec,
}

impl SnapshotMetrics {
    /// 创建指标集并注册到给定的 Registry。
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let store_entries = register_gauge_vec_with_registry!(
            "snapshot_store_entries",
            "Total number of entries in the snapshot store",
            &["shard"],
            registry
        )?;

        let global_version = register_gauge_vec_with_registry!(
            "snapshot_store_global_version",
            "Global monotonic version counter",
            &[],
            registry
        )?;

        let updates_total = register_counter_vec_with_registry!(
            "snapshot_updates_total",
            "Total number of update calls",
            &[],
            registry
        )?;

        let changes_total = register_counter_vec_with_registry!(
            "snapshot_changes_total",
            "Total number of actual value changes",
            &[],
            registry
        )?;

        let batch_get_requests = register_counter_vec_with_registry!(
            "snapshot_batch_get_requests",
            "Total number of batch_get requests",
            &[],
            registry
        )?;

        let batch_get_tags = register_histogram_vec_with_registry!(
            "snapshot_batch_get_tags",
            "Distribution of tag count per batch_get request",
            &[],
            vec![10.0, 100.0, 1000.0, 10_000.0, 100_000.0, 200_000.0],
            registry
        )?;

        let serialize_duration = register_histogram_vec_with_registry!(
            "snapshot_serialize_duration_secs",
            "Serialization duration histogram",
            &[],
            vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0],
            registry
        )?;

        let deserialize_duration = register_histogram_vec_with_registry!(
            "snapshot_deserialize_duration_secs",
            "Deserialization duration histogram",
            &[],
            vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0],
            registry
        )?;

        let recovery_duration = register_histogram_vec_with_registry!(
            "snapshot_recovery_duration_secs",
            "Recovery duration histogram",
            &[],
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 60.0],
            registry
        )?;

        let sse_active_subscriptions = register_gauge_vec_with_registry!(
            "snapshot_sse_active_subscriptions",
            "Number of active SSE subscriptions",
            &[],
            registry
        )?;

        let sse_events_total = register_counter_vec_with_registry!(
            "snapshot_sse_events_total",
            "Total number of SSE events pushed",
            &["event_type"],
            registry
        )?;

        let replay_active_sessions = register_gauge_vec_with_registry!(
            "snapshot_replay_active_sessions",
            "Number of active replay sessions",
            &[],
            registry
        )?;

        Ok(Self {
            store_entries,
            global_version,
            updates_total,
            changes_total,
            batch_get_requests,
            batch_get_tags,
            serialize_duration,
            deserialize_duration,
            recovery_duration,
            sse_active_subscriptions,
            sse_events_total,
            replay_active_sessions,
        })
    }

    /// 创建不注册任何指标的 no-op 版本（用于测试）。
    pub fn noop() -> Self {
        let registry = Registry::new();
        Self::new(&registry).expect("noop metrics creation should never fail")
    }
}

impl Default for SnapshotMetrics {
    fn default() -> Self {
        Self::noop()
    }
}

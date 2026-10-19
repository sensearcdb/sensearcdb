//! Prometheus metrics for the stream compute module.
//!
//! ## Exported metrics
//!
//! | Metric | Type | Description |
//! |--------|------|-------------|
//! | `sensearcdb_stream_dropped_events` | CounterVec | Data events dropped per task (label `task_id`) because the task channel was full |
//! | `sensearcdb_stream_dropped_events_total` | Counter | Total data events dropped because a task channel was full |
//!
//! ## 背压语义（P2-04）
//!
//! 写入路径保持 `try_send` 非阻塞：channel 满则丢弃新点，绝不阻塞主写入热路径。
//! 每条丢弃都真实递增上面两个 counter；日志告警的阈值聚合逻辑见
//! [`crate::data_event_bus`]，避免高吞吐下逐条 `warn!` 刷屏。

use prometheus::{register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec};
use std::sync::OnceLock;

// ══════════════════════════════════════════════════════════════════════════════
// Metric definitions
// ══════════════════════════════════════════════════════════════════════════════

/// 累计丢弃的数据事件总数（channel 满丢弃，无 label）。
///
/// `status_summary()` / REST / gRPC 状态端点直接读取此值暴露「累计丢点数」。
pub fn stream_dropped_events_total() -> &'static IntCounter {
    static METRIC: OnceLock<IntCounter> = OnceLock::new();
    METRIC.get_or_init(|| {
        register_int_counter!(
            "sensearcdb_stream_dropped_events_total",
            "Total number of data events dropped because the task channel was full"
        )
        .expect("failed to register sensearcdb_stream_dropped_events_total")
    })
}

/// 按 `task_id` 维度统计的丢弃数据事件数（channel 满丢弃）。
///
/// label `task_id` 便于在多个流任务共享同一源表时定位具体是哪个任务的
/// channel 背压导致丢弃。
pub fn stream_dropped_events() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        register_int_counter_vec!(
            "sensearcdb_stream_dropped_events",
            "Number of data events dropped per task because the task channel was full",
            &["task_id"]
        )
        .expect("failed to register sensearcdb_stream_dropped_events")
    })
}

// ══════════════════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registration() {
        // 验证两个 metric 均可访问且不 panic。
        let _ = stream_dropped_events_total();
        let _ = stream_dropped_events();
    }
}

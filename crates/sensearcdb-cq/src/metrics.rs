//! CQ 引擎 Prometheus 指标。
//!
//! 注册并导出 CQ 引擎的运行时指标，用于监控和告警。

use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts, Registry};

// ─── 指标结构 ───

/// CQ 引擎 Prometheus 指标集合。
///
/// 所有指标在构造时注册到给定的 `Registry`。
pub struct CqMetrics {
    /// CQ 窗口执行总数（按粒度和状态）。
    pub windows_executed: IntCounterVec,
    /// CQ 窗口执行失败数。
    pub windows_failed: IntCounterVec,
    /// CQ 重算次数。
    pub recalculations: IntCounterVec,
    /// 当前活跃的 CQ 定义数。
    pub definitions_active: IntGauge,
    /// 重算队列长度。
    pub recalculation_queue_len: IntGauge,
    /// 窗口状态缓存条目数。
    pub window_states_count: IntGauge,
    /// 窗口聚合处理行数。
    pub rows_processed: IntCounterVec,
    /// CQ 执行耗时分布（总计纳秒）。
    pub execution_time_ns_total: IntCounterVec,
    /// 迟到数据触发次数。
    pub late_data_triggers: IntCounter,
}

impl CqMetrics {
    /// 创建并注册所有 CQ 指标。
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let windows_executed = IntCounterVec::new(
            Opts::new(
                "sensearcdb_cq_windows_executed_total",
                "Total number of CQ windows executed",
            ),
            &["granularity", "status"],
        )?;

        let windows_failed = IntCounterVec::new(
            Opts::new(
                "sensearcdb_cq_windows_failed_total",
                "Total number of CQ window execution failures",
            ),
            &["granularity", "reason"],
        )?;

        let recalculations = IntCounterVec::new(
            Opts::new(
                "sensearcdb_cq_recalculations_total",
                "Total number of CQ window recalculations due to late data",
            ),
            &["granularity"],
        )?;

        let definitions_active = IntGauge::new(
            "sensearcdb_cq_definitions_active",
            "Number of active CQ definitions",
        )?;

        let recalculation_queue_len = IntGauge::new(
            "sensearcdb_cq_recalculation_queue_length",
            "Current length of the recalculation queue",
        )?;

        let window_states_count = IntGauge::new(
            "sensearcdb_cq_window_states_count",
            "Current number of window state entries",
        )?;

        let rows_processed = IntCounterVec::new(
            Opts::new(
                "sensearcdb_cq_rows_processed_total",
                "Total number of rows processed by CQ aggregation",
            ),
            &["granularity"],
        )?;

        let execution_time_ns_total = IntCounterVec::new(
            Opts::new(
                "sensearcdb_cq_execution_time_ns_total",
                "Total execution time in nanoseconds",
            ),
            &["granularity"],
        )?;

        let late_data_triggers = IntCounter::new(
            "sensearcdb_cq_late_data_triggers_total",
            "Total number of late data events that triggered dirty marking",
        )?;

        registry.register(Box::new(windows_executed.clone()))?;
        registry.register(Box::new(windows_failed.clone()))?;
        registry.register(Box::new(recalculations.clone()))?;
        registry.register(Box::new(definitions_active.clone()))?;
        registry.register(Box::new(recalculation_queue_len.clone()))?;
        registry.register(Box::new(window_states_count.clone()))?;
        registry.register(Box::new(rows_processed.clone()))?;
        registry.register(Box::new(execution_time_ns_total.clone()))?;
        registry.register(Box::new(late_data_triggers.clone()))?;

        Ok(Self {
            windows_executed,
            windows_failed,
            recalculations,
            definitions_active,
            recalculation_queue_len,
            window_states_count,
            rows_processed,
            execution_time_ns_total,
            late_data_triggers,
        })
    }

    /// 记录一次成功的窗口执行。
    pub fn record_success(&self, granularity: &str) {
        self.windows_executed
            .with_label_values(&[granularity, "success"])
            .inc();
    }

    /// 记录一次失败的窗口执行。
    pub fn record_failure(&self, granularity: &str, reason: &str) {
        self.windows_failed
            .with_label_values(&[granularity, reason])
            .inc();
    }

    /// 记录一次重算。
    pub fn record_recalculation(&self, granularity: &str) {
        self.recalculations.with_label_values(&[granularity]).inc();
    }

    /// 记录处理的行数。
    pub fn record_rows(&self, granularity: &str, rows: u64) {
        self.rows_processed
            .with_label_values(&[granularity])
            .inc_by(rows);
    }

    /// 记录执行耗时。
    pub fn record_execution_time(&self, granularity: &str, duration_ns: u64) {
        self.execution_time_ns_total
            .with_label_values(&[granularity])
            .inc_by(duration_ns);
    }

    /// 记录迟到数据触发。
    pub fn record_late_data(&self) {
        self.late_data_triggers.inc();
    }

    /// 更新活跃定义数。
    pub fn set_definitions_active(&self, count: usize) {
        self.definitions_active.set(count as i64);
    }

    /// 更新重算队列长度。
    pub fn set_queue_len(&self, len: usize) {
        self.recalculation_queue_len.set(len as i64);
    }

    /// 更新窗口状态缓存条目数。
    pub fn set_window_states_count(&self, count: usize) {
        self.window_states_count.set(count as i64);
    }
}

impl Default for CqMetrics {
    fn default() -> Self {
        // 返回一个未注册的指标集合（用于测试或禁用场景）
        let registry = Registry::new();
        Self::new(&registry).expect("Failed to create default CqMetrics")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let registry = Registry::new();
        let metrics = CqMetrics::new(&registry).unwrap();

        metrics.record_success("CQ_10s");
        metrics.record_failure("CQ_1m", "timeout");
        metrics.record_recalculation("CQ_10s");
        metrics.record_rows("CQ_10s", 100);
        metrics.record_execution_time("CQ_10s", 50_000_000);
        metrics.record_late_data();
        metrics.set_definitions_active(4);
        metrics.set_queue_len(2);
        metrics.set_window_states_count(1000);

        // 验证指标已注册
        let metric_families = registry.gather();
        assert!(!metric_families.is_empty());
    }
}

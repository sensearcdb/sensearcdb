//! EngineMetrics：Prometheus 指标注册。
//!
//! 提供存储引擎核心指标的 Prometheus 注册，涵盖：
//! - 写入吞吐量
//! - MemTable 状态
//! - Compaction 统计
//! - SAC 文件 I/O
//! - WAL 状态

use prometheus::proto::MetricFamily;
use prometheus::{IntCounter, IntGauge, Opts, Registry};

/// 引擎指标集合。
///
/// 所有指标通过 Prometheus `Registry` 注册，可通过 `/metrics` 端点导出。
#[derive(Debug, Clone)]
pub struct EngineMetrics {
    /// 注册中心。
    registry: Registry,
    /// 写入数据点总数。
    pub points_written: IntCounter,
    /// MemTable 活跃数据点数。
    pub memtable_active_points: IntGauge,
    /// MemTable 冻结次数。
    pub memtable_freezes: IntCounter,
    /// SAC 文件写入次数。
    pub sac_files_written: IntCounter,
    /// SAC 文件字节数。
    pub sac_bytes_written: IntCounter,
    /// Compaction 执行次数。
    pub compaction_runs: IntCounter,
    /// WAL 写入字节数。
    pub wal_bytes_written: IntCounter,
    /// WAL 段切换次数。
    pub wal_segment_rotations: IntCounter,
    /// 查询执行次数。
    pub queries_executed: IntCounter,
}

impl EngineMetrics {
    /// 创建新的 EngineMetrics 实例并注册所有指标。
    pub fn new() -> Self {
        let registry = Registry::new();

        let points_written = IntCounter::with_opts(Opts::new(
            "sensearcdb_engine_points_written",
            "Total data points written",
        ))
        .unwrap();

        let memtable_active_points = IntGauge::with_opts(Opts::new(
            "sensearcdb_memtable_active_points",
            "Active MemTable point count",
        ))
        .unwrap();

        let memtable_freezes = IntCounter::with_opts(Opts::new(
            "sensearcdb_memtable_freezes",
            "MemTable freeze count",
        ))
        .unwrap();

        let sac_files_written = IntCounter::with_opts(Opts::new(
            "sensearcdb_sac_files_written",
            "SAC files written",
        ))
        .unwrap();

        let sac_bytes_written = IntCounter::with_opts(Opts::new(
            "sensearcdb_sac_bytes_written",
            "SAC bytes written",
        ))
        .unwrap();

        let compaction_runs = IntCounter::with_opts(Opts::new(
            "sensearcdb_compaction_runs",
            "Compaction executions",
        ))
        .unwrap();

        let wal_bytes_written = IntCounter::with_opts(Opts::new(
            "sensearcdb_wal_bytes_written",
            "WAL bytes written",
        ))
        .unwrap();

        let wal_segment_rotations = IntCounter::with_opts(Opts::new(
            "sensearcdb_wal_segment_rotations",
            "WAL segment rotations",
        ))
        .unwrap();

        let queries_executed =
            IntCounter::with_opts(Opts::new("sensearcdb_queries_executed", "Query executions"))
                .unwrap();

        // 注册所有指标
        let _ = registry.register(Box::new(points_written.clone()));
        let _ = registry.register(Box::new(memtable_active_points.clone()));
        let _ = registry.register(Box::new(memtable_freezes.clone()));
        let _ = registry.register(Box::new(sac_files_written.clone()));
        let _ = registry.register(Box::new(sac_bytes_written.clone()));
        let _ = registry.register(Box::new(compaction_runs.clone()));
        let _ = registry.register(Box::new(wal_bytes_written.clone()));
        let _ = registry.register(Box::new(wal_segment_rotations.clone()));
        let _ = registry.register(Box::new(queries_executed.clone()));

        Self {
            registry,
            points_written,
            memtable_active_points,
            memtable_freezes,
            sac_files_written,
            sac_bytes_written,
            compaction_runs,
            wal_bytes_written,
            wal_segment_rotations,
            queries_executed,
        }
    }

    /// 记录写入事件。
    ///
    /// # 参数
    /// - `count`: 写入的数据点数。
    pub fn record_write(&self, count: u64) {
        self.points_written.inc_by(count);
    }

    /// 记录 MemTable 冻结。
    pub fn record_freeze(&self) {
        self.memtable_freezes.inc();
    }

    /// 记录 SAC 文件写入。
    ///
    /// # 参数
    /// - `bytes`: 写入的字节数。
    pub fn record_sac_write(&self, bytes: u64) {
        self.sac_files_written.inc();
        self.sac_bytes_written.inc_by(bytes);
    }

    /// 记录 Compaction 执行。
    pub fn record_compaction(&self) {
        self.compaction_runs.inc();
    }

    /// 记录 WAL 写入。
    ///
    /// # 参数
    /// - `bytes`: 写入的字节数。
    pub fn record_wal_write(&self, bytes: u64) {
        self.wal_bytes_written.inc_by(bytes);
    }

    /// 记录 WAL 段切换。
    pub fn record_wal_rotation(&self) {
        self.wal_segment_rotations.inc();
    }

    /// 记录查询执行。
    pub fn record_query(&self) {
        self.queries_executed.inc();
    }

    /// 更新 MemTable 活跃数据点数。
    ///
    /// # 参数
    /// - `count`: 当前数据点数。
    pub fn set_memtable_points(&self, count: i64) {
        self.memtable_active_points.set(count);
    }

    /// 获取 Prometheus Registry 引用。
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// 收集所有指标。
    pub fn gather(&self) -> Vec<MetricFamily> {
        self.registry.gather()
    }
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registration() {
        let metrics = EngineMetrics::new();
        // 验证 gather 不 panic
        let families = metrics.gather();
        assert!(!families.is_empty(), "should have registered metrics");
    }

    #[test]
    fn test_record_write() {
        let metrics = EngineMetrics::new();
        metrics.record_write(100);
        assert_eq!(metrics.points_written.get(), 100);
    }

    #[test]
    fn test_record_compaction() {
        let metrics = EngineMetrics::new();
        metrics.record_compaction();
        metrics.record_compaction();
        assert_eq!(metrics.compaction_runs.get(), 2);
    }

    #[test]
    fn test_set_memtable_points() {
        let metrics = EngineMetrics::new();
        metrics.set_memtable_points(500_000);
        assert_eq!(metrics.memtable_active_points.get(), 500_000);
    }
}

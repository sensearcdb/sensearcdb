//! 引擎核心指标：写入/查询/WAL/MemTable/SAC/Compaction/存储/连接 (40+)。
//!
//! 所有指标命名前缀 `sensearc_`，使用 Prometheus 标准命名约定：
//! - Counter 后缀 `_total`
//! - Histogram 后缀 `_seconds` / `_bytes`
//! - Gauge 无后缀

use prometheus::{HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

// ═══════════════════════════════════════════════════
// 写入指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_write_requests(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_write_requests_total",
        "Total number of write requests received",
    ))
    .expect("register sensearc_write_requests_total");
    r.register(Box::new(c.clone()))
        .expect("register write_requests");
    c
}

pub(crate) fn register_write_points(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_write_points_total",
        "Total number of data points written",
    ))
    .expect("register sensearc_write_points_total");
    r.register(Box::new(c.clone()))
        .expect("register write_points");
    c
}

pub(crate) fn register_write_bytes(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_write_bytes_total",
        "Total bytes written to WAL",
    ))
    .expect("register sensearc_write_bytes_total");
    r.register(Box::new(c.clone()))
        .expect("register write_bytes");
    c
}

pub(crate) fn register_write_latency(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_write_latency_seconds",
            "Write request latency in seconds",
        )
        .buckets(vec![
            0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.5, 1.0,
        ]),
    )
    .expect("register sensearc_write_latency_seconds");
    r.register(Box::new(h.clone()))
        .expect("register write_latency");
    h
}

pub(crate) fn register_write_errors(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_write_errors_total",
            "Total write errors by error type",
        ),
        &["error_type"],
    )
    .expect("register sensearc_write_errors_total");
    r.register(Box::new(v.clone()))
        .expect("register write_errors");
    v
}

// ═══════════════════════════════════════════════════
// WAL 指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_wal_fsync_latency(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_wal_fsync_latency_seconds",
            "WAL fsync latency in seconds",
        )
        .buckets(vec![
            0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1,
        ]),
    )
    .expect("register sensearc_wal_fsync_latency_seconds");
    r.register(Box::new(h.clone()))
        .expect("register wal_fsync_latency");
    h
}

pub(crate) fn register_wal_size_bytes(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_wal_size_bytes",
        "Current total WAL size in bytes",
    ))
    .expect("register sensearc_wal_size_bytes");
    r.register(Box::new(g.clone()))
        .expect("register wal_size_bytes");
    g
}

pub(crate) fn register_wal_segments(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_wal_segments",
        "Current number of WAL segments",
    ))
    .expect("register sensearc_wal_segments");
    r.register(Box::new(g.clone()))
        .expect("register wal_segments");
    g
}

pub(crate) fn register_wal_cleaner_deleted(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_wal_cleaner_deleted_total",
        "Total WAL segments deleted by cleaner",
    ))
    .expect("register sensearc_wal_cleaner_deleted_total");
    r.register(Box::new(c.clone()))
        .expect("register wal_cleaner_deleted");
    c
}

pub(crate) fn register_wal_group_commit_batch_size(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_wal_group_commit_batch_size",
            "WAL group commit batch size distribution",
        )
        .buckets(vec![
            1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0,
        ]),
    )
    .expect("register sensearc_wal_group_commit_batch_size");
    r.register(Box::new(h.clone()))
        .expect("register wal_group_commit_batch_size");
    h
}

// ═══════════════════════════════════════════════════
// 查询指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_query_requests(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_query_requests_total",
            "Total query requests by query type",
        ),
        &["query_type"],
    )
    .expect("register sensearc_query_requests_total");
    r.register(Box::new(v.clone()))
        .expect("register query_requests");
    v
}

pub(crate) fn register_query_latency(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_query_latency_seconds",
            "Query execution latency in seconds",
        )
        .buckets(vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ]),
    )
    .expect("register sensearc_query_latency_seconds");
    r.register(Box::new(h.clone()))
        .expect("register query_latency");
    h
}

pub(crate) fn register_query_errors(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_query_errors_total",
            "Total query errors by error type",
        ),
        &["error_type"],
    )
    .expect("register sensearc_query_errors_total");
    r.register(Box::new(v.clone()))
        .expect("register query_errors");
    v
}

pub(crate) fn register_query_rows_scanned(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_query_rows_scanned_total",
        "Total rows scanned during queries",
    ))
    .expect("register sensearc_query_rows_scanned_total");
    r.register(Box::new(c.clone()))
        .expect("register query_rows_scanned");
    c
}

// ═══════════════════════════════════════════════════
// Snapshot 指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_snapshot_hit_ratio(r: &Registry) -> prometheus::Gauge {
    let g = prometheus::Gauge::with_opts(Opts::new(
        "sensearc_snapshot_hit_ratio",
        "Snapshot store cache hit ratio (0.0-1.0)",
    ))
    .expect("register sensearc_snapshot_hit_ratio");
    r.register(Box::new(g.clone()))
        .expect("register snapshot_hit_ratio");
    g
}

// ═══════════════════════════════════════════════════
// MemTable 指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_memtable_size_bytes(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_memtable_size_bytes",
        "Current MemTable size in bytes",
    ))
    .expect("register sensearc_memtable_size_bytes");
    r.register(Box::new(g.clone()))
        .expect("register memtable_size_bytes");
    g
}

pub(crate) fn register_memtable_row_count(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_memtable_row_count",
        "Current number of rows in MemTable",
    ))
    .expect("register sensearc_memtable_row_count");
    r.register(Box::new(g.clone()))
        .expect("register memtable_row_count");
    g
}

pub(crate) fn register_memtable_usage_ratio(r: &Registry) -> prometheus::Gauge {
    let g = prometheus::Gauge::with_opts(Opts::new(
        "sensearc_memtable_usage_ratio",
        "MemTable usage ratio (0.0-1.0)",
    ))
    .expect("register sensearc_memtable_usage_ratio");
    r.register(Box::new(g.clone()))
        .expect("register memtable_usage_ratio");
    g
}

pub(crate) fn register_memtable_flush_total(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_memtable_flush_total",
        "Total number of MemTable flushes",
    ))
    .expect("register sensearc_memtable_flush_total");
    r.register(Box::new(c.clone()))
        .expect("register memtable_flush_total");
    c
}

pub(crate) fn register_memtable_flush_latency(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_memtable_flush_latency_seconds",
            "MemTable flush latency in seconds",
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
    )
    .expect("register sensearc_memtable_flush_latency_seconds");
    r.register(Box::new(h.clone()))
        .expect("register memtable_flush_latency");
    h
}

// ═══════════════════════════════════════════════════
// SAC 指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_sac_file_count(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new("sensearc_sac_file_count", "SAC file count by level"),
        &["level"],
    )
    .expect("register sensearc_sac_file_count");
    r.register(Box::new(v.clone()))
        .expect("register sac_file_count");
    v
}

pub(crate) fn register_sac_size_bytes(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new(
            "sensearc_sac_size_bytes",
            "SAC total size in bytes by level",
        ),
        &["level"],
    )
    .expect("register sensearc_sac_size_bytes");
    r.register(Box::new(v.clone()))
        .expect("register sac_size_bytes");
    v
}

// ═══════════════════════════════════════════════════
// Compaction 指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_compaction_operations(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_compaction_operations_total",
            "Total compaction operations by type",
        ),
        &["type"],
    )
    .expect("register sensearc_compaction_operations_total");
    r.register(Box::new(v.clone()))
        .expect("register compaction_operations");
    v
}

pub(crate) fn register_compaction_bytes(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_compaction_bytes_total",
            "Total bytes processed by compaction",
        ),
        &["type"],
    )
    .expect("register sensearc_compaction_bytes_total");
    r.register(Box::new(v.clone()))
        .expect("register compaction_bytes");
    v
}

pub(crate) fn register_compaction_duration(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_compaction_duration_seconds",
            "Compaction operation duration in seconds",
        )
        .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0]),
    )
    .expect("register sensearc_compaction_duration_seconds");
    r.register(Box::new(h.clone()))
        .expect("register compaction_duration");
    h
}

pub(crate) fn register_compaction_backlog(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_compaction_backlog",
        "Number of files pending compaction",
    ))
    .expect("register sensearc_compaction_backlog");
    r.register(Box::new(g.clone()))
        .expect("register compaction_backlog");
    g
}

// ═══════════════════════════════════════════════════
// 磁盘指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_disk_usage_ratio(r: &Registry) -> prometheus::GaugeVec {
    let v = prometheus::GaugeVec::new(
        Opts::new(
            "sensearc_disk_usage_ratio",
            "Disk usage ratio by path (0.0-1.0)",
        ),
        &["path"],
    )
    .expect("register sensearc_disk_usage_ratio");
    r.register(Box::new(v.clone()))
        .expect("register disk_usage_ratio");
    v
}

pub(crate) fn register_disk_total_bytes(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new(
            "sensearc_disk_total_bytes",
            "Total disk capacity in bytes by path",
        ),
        &["path"],
    )
    .expect("register sensearc_disk_total_bytes");
    r.register(Box::new(v.clone()))
        .expect("register disk_total_bytes");
    v
}

pub(crate) fn register_disk_used_bytes(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new(
            "sensearc_disk_used_bytes",
            "Used disk space in bytes by path",
        ),
        &["path"],
    )
    .expect("register sensearc_disk_used_bytes");
    r.register(Box::new(v.clone()))
        .expect("register disk_used_bytes");
    v
}

// ═══════════════════════════════════════════════════
// 连接指标
// ═══════════════════════════════════════════════════

pub(crate) fn register_connections_active(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new(
            "sensearc_connections_active",
            "Currently active connections by protocol",
        ),
        &["protocol"],
    )
    .expect("register sensearc_connections_active");
    r.register(Box::new(v.clone()))
        .expect("register connections_active");
    v
}

pub(crate) fn register_connections_total(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_connections_total",
            "Total connections accepted by protocol",
        ),
        &["protocol"],
    )
    .expect("register sensearc_connections_total");
    r.register(Box::new(v.clone()))
        .expect("register connections_total");
    v
}

pub(crate) fn register_connections_rejected(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_connections_rejected_total",
        "Total connections rejected",
    ))
    .expect("register sensearc_connections_rejected_total");
    r.register(Box::new(c.clone()))
        .expect("register connections_rejected");
    c
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_all_engine_metrics_register() {
        let r = Registry::new();
        register_write_requests(&r);
        register_write_points(&r);
        register_write_bytes(&r);
        register_write_latency(&r);
        register_write_errors(&r);
        register_wal_fsync_latency(&r);
        register_wal_size_bytes(&r);
        register_wal_segments(&r);
        register_wal_cleaner_deleted(&r);
        register_wal_group_commit_batch_size(&r);
        register_query_requests(&r);
        register_query_latency(&r);
        register_query_errors(&r);
        register_query_rows_scanned(&r);
        register_snapshot_hit_ratio(&r);
        register_memtable_size_bytes(&r);
        register_memtable_row_count(&r);
        register_memtable_usage_ratio(&r);
        register_memtable_flush_total(&r);
        register_memtable_flush_latency(&r);
        register_sac_file_count(&r);
        register_sac_size_bytes(&r);
        register_compaction_operations(&r);
        register_compaction_bytes(&r);
        register_compaction_duration(&r);
        register_compaction_backlog(&r);
        register_disk_usage_ratio(&r);
        register_disk_total_bytes(&r);
        register_disk_used_bytes(&r);
        register_connections_active(&r);
        register_connections_total(&r);
        register_connections_rejected(&r);
        // All registered without panic
    }
}

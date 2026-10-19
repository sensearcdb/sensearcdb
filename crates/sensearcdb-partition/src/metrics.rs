//! Prometheus metrics for partition management.
//!
//! Exposes gauges for per-state partition counts, a counter for
//! partition advances, and a latency histogram for advance operations.

use prometheus::{Histogram, IntCounter, IntCounterVec, IntGauge};

/// Collection of Prometheus metrics for partition lifecycle monitoring.
#[derive(Debug, Clone)]
pub struct PartitionMetrics {
    /// Number of partitions currently in the Active state.
    pub active_count: IntGauge,

    /// Number of partitions currently in the Frozen state.
    pub frozen_count: IntGauge,

    /// Number of partitions currently in the Compacting state.
    pub compacting_count: IntGauge,

    /// Number of partitions currently in the Archived state.
    pub archived_count: IntGauge,

    /// Number of partitions currently in the Expired state.
    pub expired_count: IntGauge,

    /// Total number of partitions across all states.
    pub total_partitions: IntGauge,

    /// Total number of partition advance operations performed.
    pub advance_total: IntCounter,

    /// Latency histogram for partition advance operations (seconds).
    pub advance_latency_seconds: Histogram,

    /// Counter of write-route operations, labeled by result.
    /// Labels: `result=["accepted", "rejected_frozen", "rejected_compacting",
    ///          "rejected_archived", "rejected_expired", "auto_created"]`.
    pub route_total: IntCounterVec,
}

impl PartitionMetrics {
    /// Create all partition metrics.
    ///
    /// Uses unregistered constructors so multiple instances can coexist
    /// (e.g., in tests). For production use, register these metrics with
    /// the default prometheus registry via `prometheus::register!`.
    pub fn new() -> Self {
        let active_count = IntGauge::new(
            "sensearcdb_partition_active_count",
            "Number of Active partitions",
        )
        .unwrap();

        let frozen_count = IntGauge::new(
            "sensearcdb_partition_frozen_count",
            "Number of Frozen partitions",
        )
        .unwrap();

        let compacting_count = IntGauge::new(
            "sensearcdb_partition_compacting_count",
            "Number of Compacting partitions",
        )
        .unwrap();

        let archived_count = IntGauge::new(
            "sensearcdb_partition_archived_count",
            "Number of Archived partitions",
        )
        .unwrap();

        let expired_count = IntGauge::new(
            "sensearcdb_partition_expired_count",
            "Number of Expired partitions",
        )
        .unwrap();

        let total_partitions =
            IntGauge::new("sensearcdb_partition_total", "Total number of partitions").unwrap();

        let advance_total = IntCounter::new(
            "sensearcdb_partition_advance_total",
            "Total number of partition advance operations",
        )
        .unwrap();

        let advance_latency_seconds = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sensearcdb_partition_advance_latency_seconds",
                "Latency of partition advance operations in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0,
            ]),
        )
        .unwrap();

        let route_total = IntCounterVec::new(
            prometheus::Opts::new(
                "sensearcdb_partition_route_total",
                "Total write-route operations by result",
            ),
            &["result"],
        )
        .unwrap();

        Self {
            active_count,
            frozen_count,
            compacting_count,
            archived_count,
            expired_count,
            total_partitions,
            advance_total,
            advance_latency_seconds,
            route_total,
        }
    }

    /// Update all state-count gauges from a list of partition infos.
    pub fn update_state_counts<I>(&self, partitions: I)
    where
        I: IntoIterator<Item = crate::state::PartitionState>,
    {
        let mut active: i64 = 0;
        let mut frozen: i64 = 0;
        let mut compacting: i64 = 0;
        let mut archived: i64 = 0;
        let mut expired: i64 = 0;

        for s in partitions {
            match s {
                crate::state::PartitionState::Active => active += 1,
                crate::state::PartitionState::Frozen => frozen += 1,
                crate::state::PartitionState::Compacting => compacting += 1,
                crate::state::PartitionState::Archived => archived += 1,
                crate::state::PartitionState::Expired => expired += 1,
            }
        }

        let total = active + frozen + compacting + archived + expired;

        self.active_count.set(active);
        self.frozen_count.set(frozen);
        self.compacting_count.set(compacting);
        self.archived_count.set(archived);
        self.expired_count.set(expired);
        self.total_partitions.set(total);
    }
}

impl Default for PartitionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::PartitionState;

    #[test]
    fn test_metrics_creation() {
        let metrics = PartitionMetrics::new();
        assert_eq!(metrics.active_count.get(), 0);
        assert_eq!(metrics.frozen_count.get(), 0);
        assert_eq!(metrics.compacting_count.get(), 0);
        assert_eq!(metrics.archived_count.get(), 0);
        assert_eq!(metrics.expired_count.get(), 0);
        assert_eq!(metrics.total_partitions.get(), 0);
        assert_eq!(metrics.advance_total.get(), 0);
    }

    #[test]
    fn test_update_state_counts() {
        let metrics = PartitionMetrics::new();
        let states = vec![
            PartitionState::Active,
            PartitionState::Active,
            PartitionState::Frozen,
            PartitionState::Compacting,
            PartitionState::Archived,
            PartitionState::Archived,
            PartitionState::Archived,
            PartitionState::Expired,
        ];

        metrics.update_state_counts(states);

        assert_eq!(metrics.active_count.get(), 2);
        assert_eq!(metrics.frozen_count.get(), 1);
        assert_eq!(metrics.compacting_count.get(), 1);
        assert_eq!(metrics.archived_count.get(), 3);
        assert_eq!(metrics.expired_count.get(), 1);
        assert_eq!(metrics.total_partitions.get(), 8);
    }

    #[test]
    fn test_update_state_counts_empty() {
        let metrics = PartitionMetrics::new();
        metrics.update_state_counts(Vec::<PartitionState>::new());

        assert_eq!(metrics.active_count.get(), 0);
        assert_eq!(metrics.total_partitions.get(), 0);
    }

    #[test]
    fn test_route_counter() {
        let metrics = PartitionMetrics::new();
        metrics.route_total.with_label_values(&["accepted"]).inc();
        metrics
            .route_total
            .with_label_values(&["rejected_frozen"])
            .inc();

        assert_eq!(
            metrics.route_total.with_label_values(&["accepted"]).get(),
            1
        );
        assert_eq!(
            metrics
                .route_total
                .with_label_values(&["rejected_frozen"])
                .get(),
            1
        );
    }

    #[test]
    fn test_advance_latency() {
        let metrics = PartitionMetrics::new();
        metrics.advance_latency_seconds.observe(0.5);
        metrics.advance_latency_seconds.observe(1.2);
        metrics.advance_latency_seconds.observe(30.0);

        let sample_count = metrics.advance_latency_seconds.get_sample_count();
        assert!(sample_count >= 3);
    }

    #[test]
    fn test_multiple_instances() {
        // Multiple PartitionMetrics instances should coexist
        let m1 = PartitionMetrics::new();
        let m2 = PartitionMetrics::new();
        m1.active_count.set(5);
        m2.active_count.set(10);
        assert_eq!(m1.active_count.get(), 5);
        assert_eq!(m2.active_count.get(), 10);
    }
}

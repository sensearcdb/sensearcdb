//! Time-driven partition scheduler.
//!
//! The `PartitionScheduler` runs a periodic loop that checks whether
//! any Active partitions have reached their time boundary and need to
//! be advanced (Active → Frozen → new Active).
//!
//! # Engine Integration (Wave 2)
//!
//! Before advancing a partition, the scheduler calls `Engine::force_freeze()`
//! via the `advance_partition` function to flush all in-flight MemTable data.

use std::sync::Arc;

use sensearcdb_engine::engine::Engine;

use crate::advance;
use crate::event::PartitionEventBus;
use crate::metadata::PartitionMetadataStore;
use crate::metrics::PartitionMetrics;
use crate::state::PartitionState;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::time::Duration;

/// Configuration for the partition scheduler loop.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Interval between partition boundary checks (milliseconds).
    pub check_interval_ms: u64,
    /// Maximum time to wait for inflight flushes during advance (milliseconds).
    pub closing_timeout_ms: u64,
    /// Maximum number of force_freeze operations per partition.
    pub max_freezes_per_partition: u32,
    /// Partition window duration (milliseconds).
    pub duration_ms: u64,
    /// Timezone offset for boundary alignment (seconds).
    pub timezone_offset_secs: i64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 60_000,
            closing_timeout_ms: 30_000,
            max_freezes_per_partition: 1_000,
            duration_ms: 86_400_000,
            timezone_offset_secs: 28_800,
        }
    }
}

/// Time-driven partition scheduler.
///
/// Runs a background task that periodically scans all Active partitions,
/// checks if they have reached their time boundary, and triggers partition
/// advancement.
pub struct PartitionScheduler;

impl PartitionScheduler {
    /// Start the scheduler background loop.
    ///
    /// # Arguments
    ///
    /// * `partitions` - The in-memory partition map (shared with PartitionManager).
    /// * `metadata` - The metadata persistence store.
    /// * `event_bus` - The event bus for broadcasting advance events.
    /// * `metrics` - Prometheus metrics for observability.
    /// * `config` - Scheduler configuration parameters.
    /// * `engine` - Optional engine reference for force_freeze() on advance.
    /// * `shutdown_rx` - Oneshot receiver for graceful shutdown signal.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` for the spawned background task.
    pub fn start(
        partitions: Arc<DashMap<u32, BTreeMap<i64, Arc<crate::state::PartitionInfo>>>>,
        metadata: Arc<PartitionMetadataStore>,
        event_bus: Arc<PartitionEventBus>,
        metrics: PartitionMetrics,
        config: SchedulerConfig,
        engine: Option<Arc<Engine>>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let check_interval = Duration::from_millis(config.check_interval_ms);

            tracing::info!(
                check_interval_ms = config.check_interval_ms,
                closing_timeout_ms = config.closing_timeout_ms,
                has_engine = engine.is_some(),
                "PartitionScheduler: started"
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(check_interval) => {
                        Self::check_and_advance(
                            &partitions,
                            &metadata,
                            &event_bus,
                            &metrics,
                            &config,
                            engine.as_ref(),
                        ).await;
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("PartitionScheduler: received shutdown signal");
                        break;
                    }
                }
            }

            tracing::info!("PartitionScheduler: stopped");
        })
    }

    /// Perform a single check-and-advance cycle.
    ///
    /// Scans all Active partitions, checks if they have reached their
    /// time boundary, and triggers advancement for each that has.
    async fn check_and_advance(
        partitions: &DashMap<u32, BTreeMap<i64, Arc<crate::state::PartitionInfo>>>,
        metadata: &PartitionMetadataStore,
        event_bus: &PartitionEventBus,
        metrics: &PartitionMetrics,
        config: &SchedulerConfig,
        engine: Option<&Arc<Engine>>,
    ) {
        let now_ns = match chrono::Utc::now().timestamp_nanos_opt() {
            Some(ns) => ns,
            None => {
                tracing::warn!("PartitionScheduler: failed to get current time, skipping cycle");
                return;
            }
        };

        // Collect tables and partition_ns that need advancement
        let mut to_advance: Vec<(u32, i64)> = Vec::new();

        for table_entry in partitions.iter() {
            let table_id = *table_entry.key();
            let table_partitions = table_entry.value();

            for (partition_ns, info) in table_partitions.iter() {
                if info.state != PartitionState::Active {
                    continue;
                }

                let end_ns = partition_ns + (config.duration_ms as i64) * 1_000_000;

                if now_ns >= end_ns {
                    to_advance.push((table_id, *partition_ns));
                }
            }
        }

        // Advance each partition
        for (table_id, _partition_ns) in &to_advance {
            tracing::info!(
                table_id = *table_id,
                "PartitionScheduler: time boundary reached, advancing partition"
            );

            match advance::advance_partition(
                partitions,
                metadata,
                event_bus,
                *table_id,
                config.duration_ms,
                config.timezone_offset_secs,
                now_ns,
                engine,
            ) {
                Ok(result) => {
                    metrics.advance_total.inc();
                    metrics
                        .advance_latency_seconds
                        .observe((now_ns - result.previous_partition_ns) as f64 / 1_000_000_000.0);
                    tracing::info!(
                        table_id = *table_id,
                        old_partition_ns = result.previous_partition_ns,
                        new_partition_ns = result.new_partition_ns,
                        "PartitionScheduler: advance completed"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        table_id = *table_id,
                        error = %e,
                        "PartitionScheduler: advance failed"
                    );
                }
            }
        }

        // Refresh metrics after the cycle
        let all_states: Vec<PartitionState> = partitions
            .iter()
            .flat_map(|entry| {
                let states: Vec<PartitionState> =
                    entry.value().values().map(|info| info.state).collect();
                states
            })
            .collect();
        metrics.update_state_counts(all_states);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::PartitionEventBus;
    use crate::metadata::PartitionMetadataStore;
    use crate::metrics::PartitionMetrics;
    use crate::state::PartitionInfo;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::time::timeout;

    fn make_active_info(table_id: u32, partition_ns: i64) -> Arc<PartitionInfo> {
        Arc::new(PartitionInfo::new_active(
            table_id,
            partition_ns,
            partition_ns,
        ))
    }

    #[tokio::test]
    async fn test_scheduler_no_active_partitions() {
        let partitions = Arc::new(DashMap::new());
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("config.db");
        let metadata = Arc::new(PartitionMetadataStore::open(&db_path).unwrap());
        let event_bus = Arc::new(PartitionEventBus::new());
        let metrics = PartitionMetrics::new();
        let config = SchedulerConfig::default();

        // This should not panic with empty partitions
        PartitionScheduler::check_and_advance(
            &partitions,
            &metadata,
            &event_bus,
            &metrics,
            &config,
            None,
        )
        .await;

        assert_eq!(metrics.advance_total.get(), 0);
    }

    #[tokio::test]
    async fn test_scheduler_advances_expired_partition() {
        let partitions = Arc::new(DashMap::new());
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("config.db");
        let metadata = Arc::new(PartitionMetadataStore::open(&db_path).unwrap());
        let event_bus = Arc::new(PartitionEventBus::new());
        let metrics = PartitionMetrics::new();

        // Use a partition that started far in the past so it's definitely expired
        let mut config = SchedulerConfig::default();
        config.duration_ms = 1; // 1ms partition — anything is expired

        let partition_ns = 0i64;
        let info = make_active_info(1, partition_ns);
        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);

        // Persist it first
        metadata
            .insert_partition(&PartitionInfo::new_active(1, partition_ns, 0))
            .unwrap();

        PartitionScheduler::check_and_advance(
            &partitions,
            &metadata,
            &event_bus,
            &metrics,
            &config,
            None,
        )
        .await;

        // After advance, the old partition should be Frozen
        let updated = partitions.get(&1).unwrap();
        let old = updated.get(&partition_ns).unwrap();
        assert_eq!(old.state, PartitionState::Frozen);
        assert_eq!(metrics.advance_total.get(), 1);
    }

    #[tokio::test]
    async fn test_scheduler_shutdown() {
        let partitions = Arc::new(DashMap::new());
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("config.db");
        let metadata = Arc::new(PartitionMetadataStore::open(&db_path).unwrap());
        let event_bus = Arc::new(PartitionEventBus::new());
        let metrics = PartitionMetrics::new();
        let config = SchedulerConfig::default();

        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = PartitionScheduler::start(
            Arc::clone(&partitions),
            metadata,
            event_bus,
            metrics,
            config,
            None,
            rx,
        );

        // Send shutdown immediately
        let _ = tx.send(());

        // Wait for graceful shutdown with timeout
        let result = timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok());
    }
}

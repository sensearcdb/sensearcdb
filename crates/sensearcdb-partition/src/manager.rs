//! PartitionManager: the central coordinator for partition lifecycle.
//!
//! The `PartitionManager` owns the in-memory partition map (`DashMap`),
//! the metadata persistence store, the event bus, and the scheduler.
//! It provides the primary APIs for write routing, cross-partition
//! queries, partition advancement, and recovery.
//!
//! # Engine Integration (Wave 2)
//!
//! The engine reference is optional — when present, `advance_partition()` will
//! call `Engine::force_freeze()` before transitioning the partition to `Frozen`,
//! ensuring all in-flight MemTable data is flushed before the partition closes.

use std::sync::Arc;

use sensearcdb_common::config::{ConfigHandle, PartitionConfig};
use sensearcdb_engine::engine::Engine;

use crate::advance::AdvanceResult;
use crate::event::{PartitionEventBus, SharedEventBus};
use crate::metadata::PartitionMetadataStore;
use crate::metrics::PartitionMetrics;
use crate::recovery;
use crate::route::{self, get_partitions_for_range, RouteError};
use crate::state::{PartitionInfo, PartitionState};
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Central partition management component.
///
/// # Architecture
///
/// The `PartitionManager` holds:
/// - An in-memory `DashMap<table_id → BTreeMap<partition_ns → Arc<PartitionInfo>>>`
///   for O(log n) partition lookup.
/// - A `PartitionMetadataStore` backed by `config.db` for persistence.
/// - A `PartitionEventBus` for asynchronous lifecycle notifications.
/// - A `PartitionScheduler` for time-driven partition advancement.
/// - `PartitionMetrics` for Prometheus observability.
/// - An optional `Arc<Engine>` for `force_freeze()` integration on advance.
pub struct PartitionManager {
    /// In-memory partition map: `table_id → (partition_ns → PartitionInfo)`.
    ///
    /// Wrapped in `Arc` so the background `PartitionScheduler` can share
    /// the same map without a lifetime dependency on the manager.
    partitions: Arc<DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>>>,

    /// Persisted metadata in `config.db`.
    metadata: Arc<PartitionMetadataStore>,

    /// Broadcast event bus for lifecycle notifications.
    event_bus: SharedEventBus,

    /// Hot-reloadable configuration handle.
    config: ConfigHandle,

    /// Prometheus metrics.
    metrics: PartitionMetrics,

    /// Path to the SAC data directory, used for disk recovery.
    sac_dir: PathBuf,

    /// Optional engine reference for force_freeze on partition advance.
    engine: Option<Arc<Engine>>,
}

impl PartitionManager {
    /// Create a new `PartitionManager` and perform disk recovery.
    ///
    /// On startup, this will:
    /// 1. Open (or create) the `config.db` metadata store.
    /// 2. Scan the SAC directory on disk to discover partitions.
    /// 3. Reconcile disk state with metadata (disk is ground truth).
    /// 4. Populate the in-memory partition map.
    /// 5. Start the background scheduler.
    ///
    /// # Arguments
    ///
    /// * `config` - Hot-reloadable configuration handle.
    /// * `engine` - Optional engine reference for force_freeze() on advance.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store cannot be opened or
    /// disk recovery fails catastrophically.
    pub async fn new(
        config: ConfigHandle,
        engine: Arc<Engine>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cfg = config.get();
        let _partition_cfg = cfg.partition.clone();
        let config_db_path = cfg.paths.config_db_path();
        let sac_dir = cfg.paths.sac_dir();

        // Open metadata store
        let metadata = Arc::new(PartitionMetadataStore::open(&config_db_path)?);

        // Create the event bus
        let event_bus = Arc::new(PartitionEventBus::new());

        // Create metrics
        let metrics = PartitionMetrics::new();

        let manager = Self {
            partitions: Arc::new(DashMap::new()),
            metadata,
            event_bus,
            config,
            metrics,
            sac_dir,
            engine: Some(engine),
        };

        // Perform disk recovery
        manager.discover_from_disk().await?;

        tracing::info!(
            table_count = manager.partitions.len(),
            "PartitionManager: initialized with disk recovery complete"
        );

        Ok(manager)
    }

    /// Route a write timestamp to the correct partition.
    ///
    /// Returns the partition info if the write is accepted, or a `RouteError`
    /// if the target partition is not writable. Automatically creates a new
    /// Active partition if none exists for the timestamp's window.
    pub fn route_to_partition(
        &self,
        table_id: u32,
        ts_ns: i64,
    ) -> Result<Arc<PartitionInfo>, RouteError> {
        let cfg = self.config.get();
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let result = route::route_to_partition(
            &self.partitions,
            &self.metadata,
            table_id,
            ts_ns,
            cfg.partition.duration_ms,
            cfg.partition.timezone_offset_secs,
            now_ns,
        );

        // Update route metrics
        match &result {
            Ok(_) => {
                self.metrics
                    .route_total
                    .with_label_values(&["accepted"])
                    .inc();
            }
            Err(e) => {
                let label = match e {
                    RouteError::PartitionFrozen(..) => "rejected_frozen",
                    RouteError::PartitionCompacting(..) => "rejected_compacting",
                    RouteError::PartitionArchived(..) => "rejected_archived",
                    RouteError::PartitionExpired(..) => "rejected_expired",
                    RouteError::PartitionNotFound(..) | RouteError::TableDeleted(..) => {
                        "rejected_other"
                    }
                };
                self.metrics.route_total.with_label_values(&[label]).inc();
            }
        }

        result
    }

    /// Get all queryable partitions overlapping the time range `[start_ns, end_ns)`.
    pub fn get_partitions_for_range(
        &self,
        table_id: u32,
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<Arc<PartitionInfo>> {
        let cfg = self.config.get();
        get_partitions_for_range(
            &self.partitions,
            table_id,
            start_ns,
            end_ns,
            cfg.partition.duration_ms,
        )
    }

    /// Manually trigger a partition advance for the given table.
    ///
    /// This freezes the current Active partition and creates a new one.
    /// If an engine reference is available, calls `Engine::force_freeze()`
    /// before the state transition to flush all in-flight MemTable data.
    pub async fn advance_partition(
        &self,
        table_id: u32,
    ) -> Result<AdvanceResult, Box<dyn std::error::Error + Send + Sync>> {
        let cfg = self.config.get();
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        crate::advance::advance_partition(
            &self.partitions,
            &self.metadata,
            &self.event_bus,
            table_id,
            cfg.partition.duration_ms,
            cfg.partition.timezone_offset_secs,
            now_ns,
            self.engine.as_ref(),
        )
    }

    /// Align a timestamp to its partition boundary.
    pub fn align_to_partition(&self, ts_ns: i64) -> i64 {
        let cfg = self.config.get();
        route::align_to_partition(
            ts_ns,
            cfg.partition.duration_ms,
            cfg.partition.timezone_offset_secs,
        )
    }

    /// Return a reference to the shared event bus.
    pub fn event_bus(&self) -> &SharedEventBus {
        &self.event_bus
    }

    /// Return a clone of the shared event bus.
    pub fn event_bus_clone(&self) -> SharedEventBus {
        Arc::clone(&self.event_bus)
    }

    /// List all partitions for a given table (by table_id).
    pub fn list_partitions_by_table(&self, table_id: u32) -> Vec<Arc<PartitionInfo>> {
        match self.partitions.get(&table_id) {
            Some(map) => map.values().cloned().collect(),
            None => Vec::new(),
        }
    }

    /// Get a specific partition by `(table_id, partition_ns)`.
    pub fn get_partition(&self, table_id: u32, partition_ns: i64) -> Option<Arc<PartitionInfo>> {
        self.partitions
            .get(&table_id)
            .and_then(|map| map.get(&partition_ns).cloned())
    }

    /// List all partitions across all tables.
    pub fn list_all_partitions(&self) -> Vec<Arc<PartitionInfo>> {
        let mut result = Vec::new();
        for entry in self.partitions.iter() {
            for info in entry.value().values() {
                result.push(Arc::clone(info));
            }
        }
        result
    }

    /// Return a reference to the metadata store.
    pub fn metadata_store(&self) -> &Arc<PartitionMetadataStore> {
        &self.metadata
    }

    /// Return a reference to the metrics.
    pub fn metrics(&self) -> &PartitionMetrics {
        &self.metrics
    }

    /// Return partition config.
    pub fn partition_config(&self) -> PartitionConfig {
        self.config.get().partition.clone()
    }

    /// Return reference to the optional engine.
    pub fn engine(&self) -> Option<&Arc<Engine>> {
        self.engine.as_ref()
    }

    /// Start the time-driven `PartitionScheduler` background loop.
    ///
    /// This is the production wiring point for `GAP2-PAR-02`: the scheduler
    /// periodically scans `Active` partitions and triggers `Active → Frozen`
    /// advancement when a time boundary is reached.
    ///
    /// # Arguments
    ///
    /// * `shutdown_rx` — oneshot receiver used to gracefully stop the loop.
    ///
    /// # Returns
    ///
    /// The spawned task's `JoinHandle`.
    pub fn start_scheduler(
        &self,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        let cfg = self.config.get();
        let scheduler_config = crate::scheduler::SchedulerConfig {
            check_interval_ms: cfg.partition.check_interval_ms,
            closing_timeout_ms: cfg.partition.closing_timeout_ms,
            max_freezes_per_partition: cfg.partition.max_freezes_per_partition,
            duration_ms: cfg.partition.duration_ms,
            timezone_offset_secs: cfg.partition.timezone_offset_secs,
        };

        crate::scheduler::PartitionScheduler::start(
            Arc::clone(&self.partitions),
            Arc::clone(&self.metadata),
            self.event_bus_clone(),
            self.metrics.clone(),
            scheduler_config,
            self.engine.clone(),
            shutdown_rx,
        )
    }

    /// Transition an `Archived` partition to `Expired` and persist the change.
    ///
    /// Used by the lifecycle TTL cleaner (`GAP2-LCY-01`) so that TTL expiry
    /// actually mutates the partition state machine (previously it only
    /// broadcast an event and left the in-memory state unchanged).
    pub fn mark_partition_expired(
        &self,
        table_id: u32,
        partition_ns: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Persist the state transition first (metadata is the authoritative record).
        self.metadata
            .update_state(table_id, partition_ns, PartitionState::Expired)?;

        // Update the in-memory map.
        if let Some(mut table_map) = self.partitions.get_mut(&table_id) {
            if let Some(info) = table_map.get(&partition_ns) {
                let mut updated = (**info).clone();
                updated.state = PartitionState::Expired;
                table_map.insert(partition_ns, Arc::new(updated));
            }
        }

        tracing::info!(
            table_id = table_id,
            partition_ns = partition_ns,
            "PartitionManager: partition marked Expired"
        );
        Ok(())
    }

    /// Remove a partition from the in-memory map and metadata store.
    ///
    /// Used after physical deletion of an `Expired` partition's SAC files so
    /// that config.db does not accumulate stale records.
    pub fn remove_partition(
        &self,
        table_id: u32,
        partition_ns: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.metadata.delete_partition(table_id, partition_ns)?;
        if let Some(mut table_map) = self.partitions.get_mut(&table_id) {
            table_map.remove(&partition_ns);
        }
        Ok(())
    }

    // ─── Internal helpers ───

    /// Perform disk recovery: scan SAC directory → reconcile with metadata.
    async fn discover_from_disk(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        recovery::discover_partitions_from_disk(
            &self.partitions,
            &self.metadata,
            &self.sac_dir,
            self.config.get().partition.duration_ms,
        )
        .await
    }

    /// Update the state-count metrics from the in-memory map.
    pub fn refresh_metrics(&self) {
        let states: Vec<PartitionState> = self
            .partitions
            .iter()
            .flat_map(|entry| {
                let s: Vec<PartitionState> =
                    entry.value().values().map(|info| info.state).collect();
                s
            })
            .collect();
        self.metrics.update_state_counts(states);
    }
}

impl std::fmt::Debug for PartitionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_partitions: usize = self.partitions.iter().map(|e| e.value().len()).sum();
        f.debug_struct("PartitionManager")
            .field("table_count", &self.partitions.len())
            .field("total_partitions", &total_partitions)
            .field("sac_dir", &self.sac_dir)
            .field("has_engine", &self.engine.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::PartitionState;
    use tempfile::tempdir;

    /// Create a minimal test engine for integration tests.
    /// We create a ConfigHandle and build a real Engine, but since
    /// Engine::new is async and requires real WAL setup, we use a
    /// simpler approach: test without engine for pure-partition tests.
    async fn make_test_partition_manager(data_dir: &std::path::Path) -> PartitionManager {
        let mut cfg = sensearcdb_common::config::AppConfig::default();
        cfg.paths.data_dir = data_dir.to_string_lossy().to_string();

        // We need an engine. For tests, create a minimal one.
        let config_handle = ConfigHandle::new(cfg);
        let engine = Arc::new(
            Engine::new(&config_handle, None)
                .await
                .expect("failed to create test engine"),
        );
        PartitionManager::new(config_handle, engine)
            .await
            .expect("failed to create test partition manager")
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");
        std::fs::create_dir_all(&sac_dir).unwrap();

        let manager = make_test_partition_manager(dir.path()).await;
        assert!(manager.engine().is_some());
    }

    #[tokio::test]
    async fn test_route_and_list_roundtrip() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");
        std::fs::create_dir_all(&sac_dir).unwrap();

        let manager = make_test_partition_manager(dir.path()).await;

        // Route a write — this should auto-create a partition
        let ts = 1_703_980_800_000_000_000_i64;
        let result = manager.route_to_partition(1, ts);
        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.state, PartitionState::Active);

        // List by table
        let list = manager.list_partitions_by_table(1);
        assert_eq!(list.len(), 1);

        // Get by key
        let retrieved = manager.get_partition(1, info.partition_ns);
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_advance_partition() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");
        std::fs::create_dir_all(&sac_dir).unwrap();

        let manager = make_test_partition_manager(dir.path()).await;

        // First, auto-create a partition
        let ts = 1_703_980_800_000_000_000_i64;
        let _ = manager.route_to_partition(1, ts).unwrap();

        // Advance the partition
        let result = manager.advance_partition(1).await;
        assert!(result.is_ok());
        let adv = result.unwrap();
        assert!(adv.new_partition_ns > adv.previous_partition_ns);
        assert_eq!(adv.previous_state, PartitionState::Frozen);
        assert_eq!(adv.new_state, PartitionState::Active);
    }

    #[tokio::test]
    async fn test_get_partitions_for_range() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");
        std::fs::create_dir_all(&sac_dir).unwrap();

        let manager = make_test_partition_manager(dir.path()).await;

        // Create a partition
        let ts = 1_703_980_800_000_000_000_i64;
        let info = manager.route_to_partition(1, ts).unwrap();

        let range = manager.get_partitions_for_range(
            1,
            info.partition_ns - 1,
            info.end_ns(manager.config.get().partition.duration_ms) + 1,
        );
        assert!(!range.is_empty());
    }

    #[tokio::test]
    async fn test_align_to_partition() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");
        std::fs::create_dir_all(&sac_dir).unwrap();

        let manager = make_test_partition_manager(dir.path()).await;

        // 2024-01-01 00:00:00 UTC+8 = 2023-12-31 16:00:00 UTC
        // epoch seconds: 1704038400
        let jan1_utc8 = 1_704_038_400_000_000_000_i64;
        let aligned = manager.align_to_partition(jan1_utc8);
        assert_eq!(aligned, jan1_utc8);

        // Noon same day should align to midnight
        let noon = jan1_utc8 + 12 * 3_600_000_000_000_i64;
        let aligned_noon = manager.align_to_partition(noon);
        assert_eq!(aligned_noon, jan1_utc8);
    }
}

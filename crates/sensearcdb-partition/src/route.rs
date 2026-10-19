//! Write routing: timestamp → partition lookup with auto-creation.
//!
//! # Routing Rules
//!
//! 1. Find the partition whose time window contains `ts_ns`.
//! 2. If the partition is `Active` → accept the write.
//! 3. If the partition is `Frozen`, `Compacting`, `Archived`, or `Expired` →
//!    reject the write with the appropriate error.
//! 4. If no partition exists for the time window → automatically create
//!    a new `Active` partition aligned to the current partition boundary.
//!
//! # Boundary Alignment
//!
//! Partition boundaries are aligned to UTC+8 00:00 by default. The alignment
//! function `align_to_partition` computes the start of the partition window
//! that contains a given timestamp.

use crate::metadata::PartitionMetadataStore;
use crate::state::{PartitionInfo, PartitionState};
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

use sensearcdb_common::SenseArcError;

/// Errors that can occur when routing a write to a partition.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RouteError {
    /// No partition exists and auto-creation failed.
    #[error("partition not found for table_id={0} at ts={1}")]
    PartitionNotFound(u32, i64),

    /// The target partition is Frozen and cannot accept writes.
    #[error("partition {0}:{1} is frozen")]
    PartitionFrozen(u32, i64),

    /// The target partition is Compacting and cannot accept writes.
    #[error("partition {0}:{1} is compacting")]
    PartitionCompacting(u32, i64),

    /// The target partition is Archived and cannot accept writes.
    #[error("partition {0}:{1} is archived")]
    PartitionArchived(u32, i64),

    /// The target partition is Expired and cannot accept writes.
    #[error("partition {0}:{1} is expired")]
    PartitionExpired(u32, i64),

    /// The table has been deleted.
    #[error("table {0} has been deleted")]
    TableDeleted(u32),
}

impl RouteError {
    /// Returns `true` if the caller should retry with a newer timestamp.
    ///
    /// This is `true` for `PartitionFrozen` and `PartitionCompacting` because
    /// the data may belong to a newly created Active partition after an advance.
    #[inline]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::PartitionFrozen(..) | Self::PartitionCompacting(..)
        )
    }
}

/// 分区路由错误 → 统一错误收敛（写入被拒语义）。
impl From<RouteError> for SenseArcError {
    fn from(e: RouteError) -> Self {
        SenseArcError::WriteRejected(e.to_string())
    }
}

/// Align a timestamp (epoch nanoseconds, UTC) to its partition boundary.
///
/// Partition boundaries are aligned to the timezone's day boundary (e.g.,
/// UTC+8 00:00). The formula:
///
/// ```text
/// // Partition boundaries in UTC epoch: k * duration_ns - tz_offset_ns
/// k = floor((ts_ns + tz_offset_ns) / duration_ns)
/// partition_ns = k * duration_ns - tz_offset_ns
/// ```
///
/// # Arguments
///
/// * `ts_ns` - The timestamp in epoch nanoseconds (UTC).
/// * `duration_ms` - The partition window duration in milliseconds.
/// * `tz_offset_secs` - The timezone offset in seconds (e.g., 28800 for UTC+8).
///
/// # Returns
///
/// The epoch nanosecond value at the start of the partition window.
///
/// 单一真相源：委托给 `sensearcdb-common::align_to_partition`（引擎 flush / compaction /
/// query 三条主路径共用同一实现），避免本 crate 与引擎各写一份公式导致漂移。
pub fn align_to_partition(ts_ns: i64, duration_ms: u64, tz_offset_secs: i64) -> i64 {
    sensearcdb_common::align_to_partition(ts_ns, duration_ms, tz_offset_secs)
}

/// Find the partition containing `ts_ns` from the in-memory map.
///
/// Returns the partition info if found, `None` otherwise.
fn find_partition(
    partitions: &DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>>,
    table_id: u32,
    ts_ns: i64,
    duration_ms: u64,
) -> Option<Arc<PartitionInfo>> {
    let table_partitions = partitions.get(&table_id)?;
    // Scan backwards from the largest partition_ns <= ts_ns
    for (_, info) in table_partitions.iter().rev() {
        if info.contains_ts(ts_ns, duration_ms) {
            return Some(Arc::clone(info));
        }
        // Since BTreeMap is ordered, if we've passed below the range,
        // we can stop early.
        if info.partition_ns < ts_ns
            && !info.contains_ts(ts_ns, duration_ms)
            && info.partition_ns + (duration_ms as i64) * 1_000_000 <= ts_ns
        {
            // Still scanning... continue.
        }
    }
    None
}

/// Route a timestamp to the correct partition.
///
/// This is the core write-routing function. It looks up the partition
/// for the given `(table_id, ts_ns)`, checks its state, and either:
/// - Returns `Ok(partition)` for writable (Active) partitions
/// - Returns `Err(RouteError)` for non-writable or missing partitions
/// - Auto-creates a new Active partition if none exists for the window
///
/// # Arguments
///
/// * `partitions` - The in-memory partition map.
/// * `metadata` - The metadata store for persisting auto-created partitions.
/// * `table_id` - The table to route to.
/// * `ts_ns` - The write timestamp in epoch nanoseconds (UTC).
/// * `duration_ms` - The partition window duration in milliseconds.
/// * `tz_offset_secs` - Timezone offset for boundary alignment.
/// * `now_ns` - Current time in epoch nanoseconds (for `created_at`).
///
/// # Returns
///
/// `Ok(PartitionInfo)` if the write can proceed; `Err(RouteError)` otherwise.
pub fn route_to_partition(
    partitions: &DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>>,
    metadata: &PartitionMetadataStore,
    table_id: u32,
    ts_ns: i64,
    duration_ms: u64,
    tz_offset_secs: i64,
    now_ns: i64,
) -> Result<Arc<PartitionInfo>, RouteError> {
    // Try to find an existing partition
    if let Some(info) = find_partition(partitions, table_id, ts_ns, duration_ms) {
        return check_state(info, table_id, ts_ns);
    }

    // No partition found → auto-create at the current window boundary
    let partition_ns = align_to_partition(ts_ns, duration_ms, tz_offset_secs);
    let new_info = Arc::new(PartitionInfo::new_active(table_id, partition_ns, now_ns));

    // Insert into metadata store
    if let Err(e) = metadata.insert_partition(&new_info) {
        tracing::error!(
            table_id = table_id,
            partition_ns = partition_ns,
            error = %e,
            "route_to_partition: failed to persist auto-created partition"
        );
        return Err(RouteError::PartitionNotFound(table_id, ts_ns));
    }

    // Insert into in-memory map
    partitions
        .entry(table_id)
        .or_insert_with(BTreeMap::new)
        .insert(partition_ns, Arc::clone(&new_info));

    tracing::info!(
        table_id = table_id,
        partition_ns = partition_ns,
        "route_to_partition: auto-created new Active partition"
    );

    Ok(new_info)
}

/// Check if a partition's state allows writes.
fn check_state(
    info: Arc<PartitionInfo>,
    table_id: u32,
    ts_ns: i64,
) -> Result<Arc<PartitionInfo>, RouteError> {
    match info.state {
        PartitionState::Active => Ok(info),
        PartitionState::Frozen => Err(RouteError::PartitionFrozen(table_id, ts_ns)),
        PartitionState::Compacting => Err(RouteError::PartitionCompacting(table_id, ts_ns)),
        PartitionState::Archived => Err(RouteError::PartitionArchived(table_id, ts_ns)),
        PartitionState::Expired => Err(RouteError::PartitionExpired(table_id, ts_ns)),
    }
}

/// Get all partitions that overlap with the time range `[start_ns, end_ns)`.
///
/// This is used for cross-partition queries. It returns partitions from
/// all states *except* `Expired`.
pub fn get_partitions_for_range(
    partitions: &DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>>,
    table_id: u32,
    start_ns: i64,
    end_ns: i64,
    duration_ms: u64,
) -> Vec<Arc<PartitionInfo>> {
    let table_partitions = match partitions.get(&table_id) {
        Some(tp) => tp,
        None => return Vec::new(),
    };

    let mut result = Vec::new();
    for (_, info) in table_partitions.iter() {
        // Check overlap: partition window [p_start, p_end) vs query [start, end)
        let p_end = info.end_ns(duration_ms);
        if info.partition_ns < end_ns && p_end > start_ns {
            if info.state.is_queryable() {
                result.push(Arc::clone(info));
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::PartitionState;
    use dashmap::DashMap;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tempfile::tempdir;

    // Default: 24h partitions, UTC+8
    const DURATION_MS: u64 = 86_400_000; // 24h
    const TZ_OFFSET_SECS: i64 = 28_800; // UTC+8

    fn setup_test_store() -> (PartitionMetadataStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();
        (store, dir)
    }

    // ─── align_to_partition tests ───

    #[test]
    fn test_align_to_partition_utc8_midnight() {
        // 2024-01-01 00:00:00 UTC+8 = 2023-12-31 16:00:00 UTC
        // epoch seconds: 1704038400, epoch ns: 1_704_038_400_000_000_000
        let jan1_utc8 = 1_704_038_400_000_000_000_i64;
        let aligned = align_to_partition(jan1_utc8, DURATION_MS, TZ_OFFSET_SECS);
        assert_eq!(aligned, jan1_utc8);

        // 2024-01-01 12:00:00 UTC+8 should align to 00:00
        let noon_utc8 = jan1_utc8 + 12 * 3_600_000_000_000_i64;
        let aligned_noon = align_to_partition(noon_utc8, DURATION_MS, TZ_OFFSET_SECS);
        assert_eq!(aligned_noon, jan1_utc8);
    }

    #[test]
    fn test_align_to_partition_utc_midnight() {
        // UTC+0: epoch boundaries are at multiples of 86400 seconds
        let jan1_utc = 1_704_067_200_000_000_000_i64; // 2024-01-01 00:00:00 UTC
        let aligned = align_to_partition(jan1_utc, DURATION_MS, 0);
        assert_eq!(aligned, jan1_utc);
    }

    #[test]
    fn test_align_to_partition_sub_day() {
        // 1-hour partitions
        let ts = 1_000_000_000_000_000_000_i64;
        let aligned = align_to_partition(ts, 3_600_000, 0);
        assert_eq!(aligned % (3_600_000 * 1_000_000), 0);
        assert!(aligned <= ts);
        assert!(aligned + 3_600_000_000_000 > ts);
    }

    // ─── route_to_partition tests ───

    #[test]
    fn test_route_active_accepts_write() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();
        let (store, _dir) = setup_test_store();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        let info = Arc::new(PartitionInfo {
            table_id: 1,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            partition_ns,
            state: PartitionState::Active,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: partition_ns,
            frozen_at: None,
            compacting_at: None,
            archived_at: None,
            freeze_count: 0,
        });

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);

        let now = partition_ns + 3_600_000_000_000; // 1 hour later
        let result = route_to_partition(
            &partitions,
            &store,
            1,
            partition_ns + 1_000_000_000, // 1 second into partition
            DURATION_MS,
            TZ_OFFSET_SECS,
            now,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_route_frozen_rejects_write() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();
        let (store, _dir) = setup_test_store();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        let info = Arc::new(PartitionInfo {
            table_id: 1,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            partition_ns,
            state: PartitionState::Frozen,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: partition_ns,
            frozen_at: Some(partition_ns + DURATION_MS as i64 * 1_000_000),
            compacting_at: None,
            archived_at: None,
            freeze_count: 0,
        });

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);

        let now = partition_ns + DURATION_MS as i64 * 2 * 1_000_000;
        let result = route_to_partition(
            &partitions,
            &store,
            1,
            partition_ns + 1,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now,
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RouteError::PartitionFrozen(1, _)
        ));
    }

    #[test]
    fn test_route_auto_create_partition() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();
        let (store, _dir) = setup_test_store();

        let ts_ns = 1_703_980_800_000_000_000_i64;
        let now = ts_ns;

        let result = route_to_partition(
            &partitions,
            &store,
            1,
            ts_ns,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now,
        );
        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.state, PartitionState::Active);
        assert_eq!(info.table_id, 1);

        // Verify it was persisted
        let persisted = store.get_partition(1, info.partition_ns).unwrap();
        assert!(persisted.is_some());
        assert_eq!(persisted.unwrap().state, PartitionState::Active);
    }

    #[test]
    fn test_route_archived_rejects_write() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();
        let (store, _dir) = setup_test_store();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        let info = Arc::new(PartitionInfo {
            table_id: 1,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            partition_ns,
            state: PartitionState::Archived,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: partition_ns,
            frozen_at: Some(partition_ns),
            compacting_at: None,
            archived_at: Some(partition_ns + DURATION_MS as i64 * 1_000_000),
            freeze_count: 0,
        });

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);

        let now = partition_ns + DURATION_MS as i64 * 3 * 1_000_000;
        let result = route_to_partition(
            &partitions,
            &store,
            1,
            partition_ns + 1,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now,
        );
        assert!(matches!(
            result.unwrap_err(),
            RouteError::PartitionArchived(1, _)
        ));
    }

    #[test]
    fn test_route_expired_rejects_write() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();
        let (store, _dir) = setup_test_store();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        let info = Arc::new(PartitionInfo {
            table_id: 1,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            partition_ns,
            state: PartitionState::Expired,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: partition_ns,
            frozen_at: Some(partition_ns),
            compacting_at: None,
            archived_at: Some(partition_ns),
            freeze_count: 0,
        });

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);

        let now = partition_ns + DURATION_MS as i64 * 10 * 1_000_000;
        let result = route_to_partition(
            &partitions,
            &store,
            1,
            partition_ns + 1,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now,
        );
        assert!(matches!(
            result.unwrap_err(),
            RouteError::PartitionExpired(1, _)
        ));
    }

    // ─── get_partitions_for_range tests ───

    #[test]
    fn test_get_partitions_for_range() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();

        // 24-hour partitions: [0, 24h) and [24h, 48h)
        let twenty_four_h_ns = 86_400_000_000_000_i64;
        let p1 = Arc::new(PartitionInfo::new_active(1, 0, 0));
        let p2 = Arc::new(PartitionInfo::new_active(1, twenty_four_h_ns, 0));

        let mut map = BTreeMap::new();
        map.insert(0, p1.clone());
        map.insert(twenty_four_h_ns, p2.clone());
        partitions.insert(1, map);

        // Query [12h, 36h) overlapping both partitions
        let result = get_partitions_for_range(
            &partitions,
            1,
            43_200_000_000_000,  // 12h in ns
            129_600_000_000_000, // 36h in ns
            DURATION_MS,
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_get_partitions_for_range_empty_table() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();
        let result = get_partitions_for_range(&partitions, 999, 0, 1_000_000, DURATION_MS);
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_partitions_for_range_excludes_expired() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();

        let mut p = PartitionInfo::new_active(1, 0, 0);
        p.state = PartitionState::Expired;
        let p_expired = Arc::new(p);

        let p_active = Arc::new(PartitionInfo::new_active(1, 86_400_000_000_000_000, 0));

        let mut map = BTreeMap::new();
        map.insert(0, p_expired);
        map.insert(86_400_000_000_000_000, p_active.clone());
        partitions.insert(1, map);

        let result =
            get_partitions_for_range(&partitions, 1, 0, 172_800_000_000_000_000, DURATION_MS);
        // Only the Active partition should be included
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].partition_ns, 86_400_000_000_000_000);
    }

    #[test]
    fn test_route_compacting_rejects_write() {
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();
        let (store, _dir) = setup_test_store();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        let info = Arc::new(PartitionInfo {
            table_id: 1,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            partition_ns,
            state: PartitionState::Compacting,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: partition_ns,
            frozen_at: Some(partition_ns),
            compacting_at: Some(partition_ns + 1),
            archived_at: None,
            freeze_count: 1,
        });

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);

        let now = partition_ns + DURATION_MS as i64 * 2 * 1_000_000;
        let result = route_to_partition(
            &partitions,
            &store,
            1,
            partition_ns + 1,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now,
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RouteError::PartitionCompacting(1, _)
        ));
    }

    #[test]
    fn test_route_error_is_retryable() {
        assert!(RouteError::PartitionFrozen(1, 0).is_retryable());
        assert!(RouteError::PartitionCompacting(1, 0).is_retryable());
        assert!(!RouteError::PartitionArchived(1, 0).is_retryable());
        assert!(!RouteError::PartitionExpired(1, 0).is_retryable());
        assert!(!RouteError::PartitionNotFound(1, 0).is_retryable());
        assert!(!RouteError::TableDeleted(1).is_retryable());
    }

    #[test]
    fn test_route_late_data_rejected_when_frozen() {
        // Simulate late-arriving data: partition is already Frozen
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();
        let (store, _dir) = setup_test_store();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        let info = Arc::new(PartitionInfo {
            table_id: 1,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            partition_ns,
            state: PartitionState::Frozen,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: partition_ns,
            frozen_at: Some(partition_ns + DURATION_MS as i64 * 1_000_000),
            compacting_at: None,
            archived_at: None,
            freeze_count: 1,
        });

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);

        // Data timestamp is within the partition window but partition is Frozen
        // This represents late data arriving after the window has been frozen
        let now = partition_ns + DURATION_MS as i64 * 2 * 1_000_000;
        let result = route_to_partition(
            &partitions,
            &store,
            1,
            partition_ns + 3_600_000_000_000, // 1 hour into window
            DURATION_MS,
            TZ_OFFSET_SECS,
            now,
        );
        assert!(matches!(
            result.unwrap_err(),
            RouteError::PartitionFrozen(..)
        ));
    }
}

//! Partition advance flow: freeze current Active → create new Active.
//!
//! The advance flow is triggered either by the time-driven scheduler
//! (when `now >= partition.start + duration`) or manually via the
//! admin API. The flow ensures:
//!
//! 1. `Engine::force_freeze()` is called to flush in-flight MemTable data.
//! 2. The current Active partition is frozen.
//! 3. A new Active partition is created at the next aligned boundary.
//! 4. Metadata is persisted to `config.db`.
//! 5. A `PartitionEvent::Advanced` is broadcast to all subscribers.

use std::sync::Arc;

use sensearcdb_engine::engine::Engine;

use crate::event::PartitionEventBus;
use crate::metadata::PartitionMetadataStore;
use crate::route;
use crate::state::{PartitionInfo, PartitionState};
use dashmap::DashMap;
use std::collections::BTreeMap;

/// Result of a partition advance operation.
#[derive(Debug, Clone)]
pub struct AdvanceResult {
    /// The partition_ns of the partition that was frozen.
    pub previous_partition_ns: i64,

    /// The partition_ns of the newly created Active partition.
    pub new_partition_ns: i64,

    /// The state of the previous partition after the operation.
    pub previous_state: PartitionState,

    /// The state of the new partition (always Active).
    pub new_state: PartitionState,
}

/// Advance the partition for a given table.
///
/// This function:
/// 1. Finds the current Active partition for the table.
/// 2. Calls `Engine::force_freeze()` to flush all in-flight MemTable data
///    before closing the partition (Wave 2 integration).
/// 3. Transitions it to Frozen.
/// 4. Creates a new Active partition at the next time boundary.
/// 5. Persists changes to metadata.
/// 6. Broadcasts a `PartitionEvent::Advanced`.
///
/// # Arguments
///
/// * `partitions` - The in-memory partition map.
/// * `metadata` - The metadata store for persisting changes.
/// * `event_bus` - The event bus for broadcasting the advance event.
/// * `table_id` - The table to advance.
/// * `duration_ms` - Partition window duration in milliseconds.
/// * `tz_offset_secs` - Timezone offset for boundary alignment.
/// * `now_ns` - Current time in epoch nanoseconds.
/// * `engine` - Optional engine reference for force_freeze() call.
///
/// # Errors
///
/// Returns an error if:
/// - No Active partition exists for the table (nothing to advance).
/// - State transition validation fails.
/// - Metadata persistence fails.
pub fn advance_partition(
    partitions: &DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>>,
    metadata: &PartitionMetadataStore,
    event_bus: &PartitionEventBus,
    table_id: u32,
    duration_ms: u64,
    tz_offset_secs: i64,
    now_ns: i64,
    engine: Option<&Arc<Engine>>,
) -> Result<AdvanceResult, Box<dyn std::error::Error + Send + Sync>> {
    // ─── Step 0: force_freeze the engine before partition switch ───
    //
    // This ensures that all in-flight MemTable data is flushed to WAL
    // and the MemTable is frozen before we mark the partition as Frozen.
    // This prevents a race condition where writes to the old partition
    // could be accepted after the partition is marked Frozen.
    if let Some(engine) = engine {
        let freeze_result = engine.force_freeze();
        tracing::info!(
            table_id = table_id,
            frozen = freeze_result.frozen,
            total_points = freeze_result.total_points,
            "advance_partition: engine force_freeze completed"
        );
    }

    // Find the current Active partition
    // NOTE: We must NOT hold a DashMap Ref while calling get_mut() below,
    // as that would deadlock the per-shard RwLock (read→write on same shard).
    let active_key = {
        let table_partitions = partitions
            .get(&table_id)
            .ok_or_else(|| format!("table {} not found in partition map", table_id))?;

        table_partitions
            .iter()
            .rev()
            .find(|(_, info)| info.state == PartitionState::Active)
            .map(|(k, _)| *k)
            .ok_or_else(|| format!("no Active partition found for table {}", table_id))?
    };
    // Ref dropped here — read lock released

    // Re-acquire a read lock to clone the partition info
    let active_info = {
        let table_partitions = partitions
            .get(&table_id)
            .ok_or_else(|| "table disappeared during advance".to_string())?;
        let info = table_partitions
            .get(&active_key)
            .ok_or_else(|| "partition disappeared during advance".to_string())?;
        (**info).clone()
    };
    // Ref dropped

    let previous_partition_ns = active_info.partition_ns;

    // Validate state transition
    if !active_info.state.can_transition_to(PartitionState::Frozen) {
        return Err(format!(
            "invalid state transition: {} → Frozen",
            active_info.state.as_str()
        )
        .into());
    }

    // Step 1: Freeze the current Active partition
    let mut frozen_info = active_info;
    frozen_info.state = PartitionState::Frozen;
    frozen_info.frozen_at = Some(now_ns);
    frozen_info.freeze_count = frozen_info.freeze_count.saturating_add(1);

    // Persist the state change
    metadata.update_state_frozen(table_id, previous_partition_ns, now_ns)?;

    // Update in-memory
    let frozen_arc = Arc::new(frozen_info);
    let mut table_map = partitions.get_mut(&table_id).unwrap();
    table_map.insert(previous_partition_ns, frozen_arc);

    // Step 2: Create a new Active partition at the next boundary
    let new_partition_ns = route::align_to_partition(
        previous_partition_ns + (duration_ms as i64) * 1_000_000,
        duration_ms,
        tz_offset_secs,
    );

    // Ensure the new partition start is strictly after the previous
    let new_partition_ns = if new_partition_ns <= previous_partition_ns {
        previous_partition_ns + (duration_ms as i64) * 1_000_000
    } else {
        new_partition_ns
    };

    let new_info = Arc::new(PartitionInfo::new_active(
        table_id,
        new_partition_ns,
        now_ns,
    ));

    // Persist the new partition
    metadata.insert_partition(&new_info)?;

    // Insert in-memory
    table_map.insert(new_partition_ns, Arc::clone(&new_info));

    // Step 3: Broadcast the event
    let _ = event_bus.send(crate::event::PartitionEvent::Advanced {
        table_id,
        partition_ns: previous_partition_ns,
    });

    // Step 4: Wake the engine compaction scheduler so the newly frozen
    // partition's L1 files can be picked up for L1→L2 compaction
    // (GAP2-PAR-03). The engine already runs its own periodic scan, so this
    // is a best-effort latency optimization.
    if let Some(engine) = engine {
        engine.compaction.notify_new_files();
    }

    tracing::info!(
        table_id = table_id,
        previous_partition_ns = previous_partition_ns,
        new_partition_ns = new_partition_ns,
        "advance_partition: completed"
    );

    Ok(AdvanceResult {
        previous_partition_ns,
        new_partition_ns,
        previous_state: PartitionState::Frozen,
        new_state: PartitionState::Active,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::PartitionEventBus;
    use crate::metadata::PartitionMetadataStore;
    use crate::state::PartitionInfo;
    use tempfile::tempdir;

    const DURATION_MS: u64 = 86_400_000;
    const TZ_OFFSET_SECS: i64 = 28_800;

    fn setup() -> (
        DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>>,
        PartitionMetadataStore,
        PartitionEventBus,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let metadata = PartitionMetadataStore::open(&db_path).unwrap();
        let partitions = DashMap::new();
        let event_bus = PartitionEventBus::new();
        (partitions, metadata, event_bus, dir)
    }

    #[test]
    fn test_advance_single_partition() {
        let (partitions, metadata, event_bus, _dir) = setup();
        let mut rx = event_bus.subscribe();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        let info = Arc::new(PartitionInfo::new_active(1, partition_ns, partition_ns));

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);
        metadata
            .insert_partition(&PartitionInfo::new_active(1, partition_ns, partition_ns))
            .unwrap();

        let now_ns = partition_ns + DURATION_MS as i64 * 1_000_000 + 1;
        let result = advance_partition(
            &partitions,
            &metadata,
            &event_bus,
            1,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now_ns,
            None, // no engine in unit test
        );
        assert!(result.is_ok());
        let adv = result.unwrap();

        // Old partition should be Frozen
        let table = partitions.get(&1).unwrap();
        let old = table.get(&partition_ns).unwrap();
        assert_eq!(old.state, PartitionState::Frozen);
        assert!(old.frozen_at.is_some());
        assert_eq!(old.freeze_count, 1);

        // New partition should exist and be Active
        let new = table.get(&adv.new_partition_ns).unwrap();
        assert_eq!(new.state, PartitionState::Active);
        assert!(adv.new_partition_ns > adv.previous_partition_ns);

        // Event should be broadcast
        let event = rx.try_recv().unwrap();
        match event {
            crate::event::PartitionEvent::Advanced {
                table_id,
                partition_ns: pn,
            } => {
                assert_eq!(table_id, 1);
                assert_eq!(pn, partition_ns);
            }
            _ => panic!("expected Advanced event"),
        }
    }

    #[test]
    fn test_advance_no_active_partition() {
        let (partitions, metadata, event_bus, _dir) = setup();

        let result = advance_partition(
            &partitions,
            &metadata,
            &event_bus,
            999,
            DURATION_MS,
            TZ_OFFSET_SECS,
            0,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_advance_multiple_partitions_advances_latest() {
        let (partitions, metadata, event_bus, _dir) = setup();

        // Insert an older Frozen partition and a newer Active partition
        let old_ns = 1_703_894_400_000_000_000_i64;
        let new_ns = 1_703_980_800_000_000_000_i64;

        let old = Arc::new(PartitionInfo {
            table_id: 1,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            partition_ns: old_ns,
            state: PartitionState::Frozen,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: old_ns,
            frozen_at: Some(old_ns + 1),
            compacting_at: None,
            archived_at: None,
            freeze_count: 1,
        });
        let new = Arc::new(PartitionInfo::new_active(1, new_ns, new_ns));

        let mut map = BTreeMap::new();
        map.insert(old_ns, old);
        map.insert(new_ns, new);
        partitions.insert(1, map);

        metadata
            .insert_partition(&PartitionInfo::new_active(1, new_ns, new_ns))
            .unwrap();

        let now_ns = new_ns + DURATION_MS as i64 * 1_000_000 + 1;
        let result = advance_partition(
            &partitions,
            &metadata,
            &event_bus,
            1,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now_ns,
            None,
        );
        assert!(result.is_ok());
        let adv = result.unwrap();

        // The newer (larger partition_ns) Active should have been advanced
        assert_eq!(adv.previous_partition_ns, new_ns);
    }

    #[test]
    fn test_advance_frozen_state_transition() {
        let (partitions, metadata, event_bus, _dir) = setup();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        // Create a partition that is already Frozen (no Active partition exists)
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
            frozen_at: Some(partition_ns),
            compacting_at: None,
            archived_at: None,
            freeze_count: 1,
        });

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);

        let now_ns = partition_ns + DURATION_MS as i64 * 2 * 1_000_000;
        let result = advance_partition(
            &partitions,
            &metadata,
            &event_bus,
            1,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now_ns,
            None,
        );
        // Should fail because there is no Active partition to advance
        assert!(result.is_err());
    }

    #[test]
    fn test_freeze_count_increment() {
        let (partitions, metadata, event_bus, _dir) = setup();

        let partition_ns = 1_703_980_800_000_000_000_i64;
        let info = Arc::new(PartitionInfo::new_active(1, partition_ns, partition_ns));

        partitions
            .entry(1)
            .or_insert_with(BTreeMap::new)
            .insert(partition_ns, info);
        metadata
            .insert_partition(&PartitionInfo::new_active(1, partition_ns, partition_ns))
            .unwrap();

        let now_ns = partition_ns + DURATION_MS as i64 * 1_000_000 + 1;
        let _ = advance_partition(
            &partitions,
            &metadata,
            &event_bus,
            1,
            DURATION_MS,
            TZ_OFFSET_SECS,
            now_ns,
            None,
        )
        .unwrap();

        let table = partitions.get(&1).unwrap();
        let frozen = table.get(&partition_ns).unwrap();
        assert_eq!(frozen.freeze_count, 1);
    }
}

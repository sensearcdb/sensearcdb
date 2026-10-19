//! Partition state machine: five-state lifecycle and partition metadata.
//!
//! # State machine
//!
//! ```text
//! Active → Frozen → Compacting → Archived → Expired
//!            ↑            ↑              │
//!            └── retry ───┘              │
//!                 └────── late write ────┘
//! ```
//!
//! - **Active**: currently accepting writes. L0→L1 compaction proceeds internally.
//! - **Frozen**: no longer accepting writes, waiting for L1→L2 compaction.
//! - **Compacting**: L1→L2 compaction in progress.
//! - **Archived**: read-only, single L2 file. Entry point for TTL evaluation.
//! - **Expired**: TTL expired, awaiting physical deletion.

use sensearcdb_common::TenantId;
use serde::{Deserialize, Serialize};

/// The five states of a time-based data partition.
///
/// The state machine guarantees that once a partition leaves `Active`,
/// it can never return to `Active`. The only backward transition allowed
/// is `Compacting → Frozen` on compaction retry exhaustion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum PartitionState {
    /// Currently accepting writes. L0→L1 compaction proceeds internally.
    Active = 0,
    /// No longer accepting writes. Awaiting L1→L2 compaction scheduling.
    Frozen = 1,
    /// L1→L2 compaction in progress.
    Compacting = 2,
    /// Read-only, single L2 file. TTL evaluation entry point.
    Archived = 3,
    /// TTL expired, awaiting physical file deletion.
    Expired = 4,
}

impl PartitionState {
    /// Returns `true` if this partition can accept new writes.
    ///
    /// Only `Active` partitions are writable. All other states
    /// reject incoming data points.
    #[inline]
    pub fn is_writable(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Returns `true` if data within this partition can be queried.
    ///
    /// All states except `Expired` are queryable.
    #[inline]
    pub fn is_queryable(self) -> bool {
        !matches!(self, Self::Expired)
    }

    /// Returns `true` if the transition from `self` to `target` is valid.
    ///
    /// Valid transitions:
    /// - `Active → Frozen` (time boundary reached, or manual force_freeze)
    /// - `Frozen → Compacting` (L1→L2 compaction scheduled)
    /// - `Frozen → Archived` (skip Compacting, direct archive)
    /// - `Compacting → Archived` (L2 compaction successful)
    /// - `Compacting → Frozen` (compaction retry exhausted, rollback)
    /// - `Archived → Frozen` (late-arriving data pulls the partition back to Frozen)
    /// - `Archived → Expired` (TTL expired)
    #[inline]
    pub fn can_transition_to(self, target: Self) -> bool {
        matches!(
            (self, target),
            (Self::Active, Self::Frozen)
                | (Self::Frozen, Self::Compacting)
                | (Self::Frozen, Self::Archived)
                | (Self::Compacting, Self::Archived)
                | (Self::Compacting, Self::Frozen)
                | (Self::Archived, Self::Frozen)
                | (Self::Archived, Self::Expired)
        )
    }

    /// Convert to a static string representation.
    ///
    /// These strings match the protobuf `PartitionInfo.state` field values.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Frozen => "Frozen",
            Self::Compacting => "Compacting",
            Self::Archived => "Archived",
            Self::Expired => "Expired",
        }
    }

    /// Parse from a string (case-sensitive).
    ///
    /// Returns `None` if the string does not match any known state.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Active" => Some(Self::Active),
            "Frozen" => Some(Self::Frozen),
            "Compacting" => Some(Self::Compacting),
            "Archived" => Some(Self::Archived),
            "Expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

/// Metadata for a single time-based partition.
///
/// Each partition covers a fixed time window aligned to a configurable
/// timezone boundary (default: UTC+8 00:00). The partition is uniquely
/// identified by `(table_id, partition_ns)` where `partition_ns` is
/// the epoch-nanosecond start of the partition window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// Owning table identifier.
    pub table_id: u32,

    /// 租户 ID（0 = 默认租户；`1..` = 真实租户，S5 起落 `partitions.tenant_id`）。
    pub tenant_id: TenantId,

    /// Partition start time in epoch nanoseconds (UTC).
    pub partition_ns: i64,

    /// Current lifecycle state.
    pub state: PartitionState,

    /// Number of SAC files in this partition.
    pub file_count: u32,

    /// Total on-disk bytes consumed by this partition's SAC files.
    pub total_bytes: u64,

    /// Minimum timestamp observed in this partition, if any data exists.
    pub min_ts: Option<i64>,

    /// Maximum timestamp observed in this partition, if any data exists.
    pub max_ts: Option<i64>,

    /// Epoch nanoseconds when this partition was created.
    pub created_at: i64,

    /// Epoch nanoseconds when this partition was frozen, if applicable.
    pub frozen_at: Option<i64>,

    /// Epoch nanoseconds when compaction began, if applicable.
    pub compacting_at: Option<i64>,

    /// Epoch nanoseconds when compaction completed (archived), if applicable.
    pub archived_at: Option<i64>,

    /// Number of force_freeze operations performed on this partition.
    pub freeze_count: u32,
}

impl PartitionInfo {
    /// Create a new `Active` partition starting at `partition_ns`.
    ///
    /// `now_ns` is the current time in epoch nanoseconds, used for `created_at`。
    /// 默认租户 0（向后兼容；真实租户用 [`new_active_for_tenant`](Self::new_active_for_tenant)）。
    pub fn new_active(table_id: u32, partition_ns: i64, now_ns: i64) -> Self {
        Self::new_active_for_tenant(table_id, TenantId::DEFAULT, partition_ns, now_ns)
    }

    /// 创建指定租户的新 `Active` 分区。
    pub fn new_active_for_tenant(
        table_id: u32,
        tenant_id: TenantId,
        partition_ns: i64,
        now_ns: i64,
    ) -> Self {
        Self {
            table_id,
            tenant_id,
            partition_ns,
            state: PartitionState::Active,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: now_ns,
            frozen_at: None,
            compacting_at: None,
            archived_at: None,
            freeze_count: 0,
        }
    }

    /// Return the end of this partition's time window (exclusive).
    ///
    /// `duration_ms` is the partition window length in milliseconds.
    #[inline]
    pub fn end_ns(&self, duration_ms: u64) -> i64 {
        self.partition_ns + (duration_ms as i64) * 1_000_000
    }

    /// Return `true` if `ts_ns` falls within this partition's time window.
    #[inline]
    pub fn contains_ts(&self, ts_ns: i64, duration_ms: u64) -> bool {
        ts_ns >= self.partition_ns && ts_ns < self.end_ns(duration_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── PartitionState tests ───

    #[test]
    fn test_state_is_writable() {
        assert!(PartitionState::Active.is_writable());
        assert!(!PartitionState::Frozen.is_writable());
        assert!(!PartitionState::Compacting.is_writable());
        assert!(!PartitionState::Archived.is_writable());
        assert!(!PartitionState::Expired.is_writable());
    }

    #[test]
    fn test_state_is_queryable() {
        assert!(PartitionState::Active.is_queryable());
        assert!(PartitionState::Frozen.is_queryable());
        assert!(PartitionState::Compacting.is_queryable());
        assert!(PartitionState::Archived.is_queryable());
        assert!(!PartitionState::Expired.is_queryable());
    }

    #[test]
    fn test_state_transitions_valid() {
        // Forward transitions
        assert!(PartitionState::Active.can_transition_to(PartitionState::Frozen));
        assert!(PartitionState::Frozen.can_transition_to(PartitionState::Compacting));
        assert!(PartitionState::Frozen.can_transition_to(PartitionState::Archived));
        assert!(PartitionState::Compacting.can_transition_to(PartitionState::Archived));
        assert!(PartitionState::Compacting.can_transition_to(PartitionState::Frozen)); // retry rollback
        assert!(PartitionState::Archived.can_transition_to(PartitionState::Expired));
        // Late-arriving data pulls an Archived partition back to Frozen (旁路 L0 + re-final).
        assert!(PartitionState::Archived.can_transition_to(PartitionState::Frozen));
    }

    #[test]
    fn test_state_transitions_invalid() {
        // Cannot go backwards (except Compacting→Frozen retry)
        assert!(!PartitionState::Frozen.can_transition_to(PartitionState::Active));
        assert!(!PartitionState::Compacting.can_transition_to(PartitionState::Active));
        assert!(!PartitionState::Archived.can_transition_to(PartitionState::Active));
        assert!(!PartitionState::Expired.can_transition_to(PartitionState::Active));

        // Cannot skip states in forward direction
        assert!(!PartitionState::Active.can_transition_to(PartitionState::Compacting));
        assert!(!PartitionState::Active.can_transition_to(PartitionState::Archived));
        assert!(!PartitionState::Active.can_transition_to(PartitionState::Expired));
        assert!(!PartitionState::Frozen.can_transition_to(PartitionState::Expired));
    }

    #[test]
    fn test_state_as_str_roundtrip() {
        for state in &[
            PartitionState::Active,
            PartitionState::Frozen,
            PartitionState::Compacting,
            PartitionState::Archived,
            PartitionState::Expired,
        ] {
            let s = state.as_str();
            let parsed = PartitionState::from_str(s);
            assert_eq!(parsed, Some(*state));
        }
    }

    #[test]
    fn test_state_from_str_invalid() {
        assert_eq!(PartitionState::from_str(""), None);
        assert_eq!(PartitionState::from_str("active"), None); // case-sensitive
        assert_eq!(PartitionState::from_str("Unknown"), None);
    }

    #[test]
    fn test_state_serde_roundtrip() {
        let original = PartitionState::Archived;
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: PartitionState = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    // ─── PartitionInfo tests ───

    #[test]
    fn test_partition_info_new_active() {
        let info =
            PartitionInfo::new_active(42, 1_700_000_000_000_000_000, 1_700_000_000_000_000_100);
        assert_eq!(info.table_id, 42);
        assert_eq!(info.partition_ns, 1_700_000_000_000_000_000);
        assert_eq!(info.state, PartitionState::Active);
        assert_eq!(info.file_count, 0);
        assert_eq!(info.total_bytes, 0);
        assert!(info.min_ts.is_none());
        assert!(info.max_ts.is_none());
        assert_eq!(info.created_at, 1_700_000_000_000_000_100);
        assert!(info.frozen_at.is_none());
        assert!(info.compacting_at.is_none());
        assert!(info.archived_at.is_none());
        assert_eq!(info.freeze_count, 0);
    }

    #[test]
    fn test_partition_info_end_ns() {
        // 24h duration in ms = 86_400_000
        let info = PartitionInfo::new_active(1, 0, 0);
        let end = info.end_ns(86_400_000);
        assert_eq!(end, 86_400_000_000_000); // 86_400_000 ms * 1_000_000 = 86_400_000_000_000 ns
    }

    #[test]
    fn test_partition_info_contains_ts() {
        let info = PartitionInfo::new_active(1, 86_400_000_000_000_000, 0);
        let duration_ms = 86_400_000; // 24h

        // Inside window
        assert!(info.contains_ts(86_400_000_000_000_000, duration_ms));
        assert!(info.contains_ts(86_400_000_000_000_500, duration_ms));

        // Equal to start → inside (inclusive)
        assert!(info.contains_ts(86_400_000_000_000_000, duration_ms));

        // Equal to end → outside (exclusive)
        let end = info.end_ns(duration_ms);
        assert!(!info.contains_ts(end, duration_ms));

        // Before start
        assert!(!info.contains_ts(0, duration_ms));

        // After end
        assert!(!info.contains_ts(end + 1, duration_ms));
    }

    // ─── Full five-state transition sequence test ───

    #[test]
    fn test_full_state_machine_sequence() {
        // Active → Frozen → Compacting → Archived → Expired
        let states = vec![
            PartitionState::Active,
            PartitionState::Frozen,
            PartitionState::Compacting,
            PartitionState::Archived,
            PartitionState::Expired,
        ];

        for window in states.windows(2) {
            let from = window[0];
            let to = window[1];
            assert!(
                from.can_transition_to(to),
                "transition {:?} → {:?} should be valid",
                from,
                to
            );
        }
    }

    #[test]
    fn test_compacting_retry_rollback() {
        // Compacting → Frozen (retry exhausted) is valid
        assert!(PartitionState::Compacting.can_transition_to(PartitionState::Frozen));

        // Frozen → Compacting again (retry) is valid
        assert!(PartitionState::Frozen.can_transition_to(PartitionState::Compacting));
    }

    #[test]
    fn test_direct_frozen_to_archived() {
        // Frozen can go directly to Archived (no compaction needed)
        assert!(PartitionState::Frozen.can_transition_to(PartitionState::Archived));
    }
}

//! # sensearcdb-snapshot
//!
//! SenseArcDB 实时快照数据库：SnapshotStore (32-shard)、值变化检测、
//! SSE 推送、崩溃恢复、ReplayEngine 历史回放。
//!
//! ## 模块概览
//!
//! - `store` — SnapshotStore 核心（32-shard + batch_get）。
//! - `entry` — SnapshotEntry 条目定义。
//! - `columnar` — ColumnarShard 列式分片（21 列 arrow builder + 索引）。
//! - `version` — AtomicU64 全局版本号管理。
//! - `change_detect` — 双路径变化检测 + F64_EPSILON。
//! - `tag_metadata` — TagMetadataStore 标签元数据缓存。
//! - `subscription` — SnapshotSubscriptionManager Fire-and-forget 订阅。
//! - `merge` — 列级 merge 纯函数（G2）。
//! - `serialize` — snapshot.bin bincode 序列化（确定性）。
//! - `snapshot_checkpointer` — 后台快照检查点（定期落盘 + 脏标记跳过）。
//! - `recovery` — 三层崩溃恢复。
//! - `replay` — ReplayEngine 历史回放子系统。
//! - `api` — 纯 gRPC 数据面端点。
//! - `metrics` — Prometheus 可观测性指标。

pub mod api;
pub mod change_detect;
pub mod columnar;
pub mod encryption;
pub mod entry;
pub mod merge;
pub mod metrics;
pub mod pending;
pub mod proto;
pub mod recovery;
pub mod replay;
pub mod serialize;
pub mod snapshot_checkpointer;
pub mod store;
pub mod subscription;
pub mod tag_metadata;
pub mod typed;
pub mod version;

// ─── Generated protobuf code ───
pub mod snapshot_proto {
    tonic::include_proto!("sensearcdb.snapshot.v1");
}

// ─── Re-exports ───

pub use change_detect::{values_equal, ChangeResult, F64_EPSILON};
pub use encryption::{SharedSnapshotEncryptor, SnapshotEncryptError, SnapshotEncryptor};
pub use entry::SnapshotEntry;
pub use metrics::SnapshotMetrics;
pub use recovery::{
    recover, NoopSacRecoveryReader, NoopWalReplayReader, RecoveryConfig, RecoveryError,
    RecoveryResult, RecoverySource, SacRecoveryReader, SacRecoveryReaderRef, SacScanRow,
    WalReplayReader, WalReplayReaderRef, WalReplayRecord,
};
pub use replay::{
    NoopSacRangeReader, ReplayAlertEvent, ReplayAlertOutput, ReplayAlerts, ReplayClock,
    ReplayDerived, ReplayDerivedOutput, ReplayDerivedPoint, ReplayEngine, ReplayEvent,
    ReplayParams, ReplaySessionHandle, ReplaySnapshotStore, ReplaySpeed, ReplayState, ReplayStats,
    SacDataRow, SacRangeReader, SacReaderError, SacReaderRef,
};
pub use serialize::{
    deserialize_from_file, read_snapshot_watermark, serialize_to_file, SerializeError,
};
pub use columnar::ColumnarShard;
pub use pending::{OwnedPendingPoint, SnapshotFlusher, SnapshotPendingBuffer};
pub use snapshot_checkpointer::{SnapshotCheckpointer, WalWatermarkProvider};
pub use store::{SnapshotCursor, SnapshotStore, TypedSnapshotValue};
pub use subscription::{DataChangeNotification, SnapshotSubscription, SnapshotSubscriptionManager};
pub use tag_metadata::{SchemaMirrorRef, TagMetadata, TagMetadataStore};
pub use typed::{typed_columns_json, typed_value_json};
pub use version::GlobalVersion;

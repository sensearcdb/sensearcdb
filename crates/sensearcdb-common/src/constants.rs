//! SenseArcDB 不可变文件格式常量。
//!
//! ⚠️ **本文件仅保留不可变的文件格式常量。**
//!
//! 修改以下任何常量都会破坏文件格式兼容性。运行时可调参数
//! 全部移至 [`config`](crate::config) 模块 → `AppConfig`。
//!
//! 旧默认值函数的迁移路径：
//! - `default_freeze_threshold()`     → `AppConfig.engine.freeze_threshold`
//! - `default_compaction_*()`         → `AppConfig.engine.compaction_*`
//! - `default_wal_*()`                → `AppConfig.engine.wal_*`
//! - `default_shard_multiplier()`     → `AppConfig.snapshot.shard_multiplier`
//! - `default_replay_*()`             → `AppConfig.snapshot.replay.*`

// ─── SAC 文件格式常量 ───
// 修改以下任何值都会破坏 SAC 文件兼容性，导致历史数据不可读。

/// SAC 文件 Magic Number: "RTM8"
pub const SAC_MAGIC: &[u8; 4] = b"RTM8";

/// SAC 文件格式版本号。
pub const SAC_VERSION: u16 = 9;

/// SAC Header 固定大小: 32 字节。
///
/// v8 布局：magic(4) + version(2) + partition_key(6) + flags(1) + level(1)
/// + seq(2) + dek_version(4) + tenant_id(4) + write_epoch(8)。
pub const SAC_HEADER_SIZE: usize = 32;

/// SAC 默认租户 ID（= 默认租户 0，与 `TenantId::DEFAULT` 一致）。
pub const DEFAULT_TENANT_ID: u32 = 0;

/// SAC Footer 固定大小: 64 字节。
///
/// v9 布局：min_ts(8) + max_ts(8) + block_count(4) + table_count(4) + l2_offset(8)
/// + table_index_offset(8) + block_count_mirror(4) + table_count_mirror(4)
/// + magic(4) + crc32c(4) + reserved(8)。
pub const SAC_FOOTER_SIZE: usize = 64;

/// SAC TableIndex 条目大小: 48 字节（定长，按 table_id 升序）。
pub const SAC_TABLE_INDEX_ENTRY_SIZE: usize = 48;

/// SAC L2 描述符条目大小: 56 字节。
pub const SAC_L2_ENTRY_SIZE: usize = 56;

// ─── WAL 文件格式常量 ───
// 修改以下任何值都会破坏 WAL 文件兼容性。

/// WAL Magic Number: "WAL2"
///
/// 全模式（单机 + 集群）统一使用 WAL2 帧格式；单机以 `raft_term=0`、
/// `raft_index=自增 seq` 复用，无 V1 兼容分支。
pub const WAL_MAGIC_V2: &[u8; 4] = b"WAL2";

/// WAL Frame Header 大小 (WAL2): 37 字节。
///
/// 字段布局: magic(4B) + flag(1B) + schema_version(4B) + raft_term(8B) + raft_index(8B) + data_len(4B) + point_count(4B) + crc32c(4B)
pub const WAL_FRAME_HEADER_SIZE_V2: usize = 37;

/// WAL Segment 文件大小: 64MB。
///
/// 修改此值会破坏文件布局和 flush_watermark 语义。
pub const WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

// ═══════════════════════════════════════════════════════════
// Wave 6: FORMAT_VERSION + 默认端口
// ═══════════════════════════════════════════════════════════

/// 数据存储格式版本 — WAL。
///
/// WAL=2: 支持 Variable-Length Frame（CRC32 校验）。
/// 启动时检查，不兼容则拒绝启动。
pub const FORMAT_VERSION_WAL: u32 = 2;

/// 数据存储格式版本 — SAC。
///
/// SAC=2: 多列宽表（column_id 显式落盘）+ TDE 加密（AES-256-GCM）。
/// 启动时检查，不兼容则拒绝启动。
pub const FORMAT_VERSION_SAC: u32 = 2;

/// 默认 REST API 端口（管理 + 查询 + health 端点）。
pub const DEFAULT_REST_PORT: u16 = 5100;

/// 默认 Prometheus /metrics 端点端口。
pub const DEFAULT_METRICS_PORT: u16 = 5101;

/// 默认 gRPC 写入端口（WriteService，内部服务间写入）。
pub const DEFAULT_GRPC_WRITE_PORT: u16 = 5201;

/// 默认 gRPC 查询端口（Query + Flight）。
pub const DEFAULT_GRPC_QUERY_PORT: u16 = 5202;

/// 默认 gRPC 管理端口（CollectorAdmin）。
pub const DEFAULT_GRPC_ADMIN_PORT: u16 = 5203;

/// 默认 gRPC 边缘同步端口（EdgeSyncService，树形联邦数据同步）。
pub const DEFAULT_GRPC_EDGE_PORT: u16 = 5204;

/// 默认 gRPC AI 推理聚合端口（forecast/anomaly/ai-ops/condition/asset）。
pub const DEFAULT_GRPC_AI_PORT: u16 = 5205;

/// 默认 gRPC 快照数据面端口（SnapshotService：Get/Batch/List/Subscribe）。
pub const DEFAULT_GRPC_SNAPSHOT_PORT: u16 = 5206;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sac_header_size() {
        // SacHeader v8: magic(4) + version(2) + partition_key(6) + flags(1) + level(1) + seq(2) + dek_version(4) + tenant_id(4) + write_epoch(8) = 32
        assert_eq!(SAC_HEADER_SIZE, 32);
    }

    #[test]
    fn test_sac_footer_size() {
        // SacFooter (SAC v9): min_ts(8) + max_ts(8) + block_count(4) + table_count(4)
        // + l2_offset(8) + table_index_offset(8) + block_count_mirror(4)
        // + table_count_mirror(4) + magic(4) + crc32c(4) + reserved(8) = 64
        assert_eq!(SAC_FOOTER_SIZE, 64);
    }

    #[test]
    fn test_sac_table_index_entry_size() {
        // TableIndexEntry (SAC v9): table_id(4) + block_count(4) + min_ts(8) + max_ts(8)
        // + first_l2_idx(4) + row_count(4) + total_block_bytes(8) + column_count(2)
        // + flags(1) + reserved(5) = 48
        assert_eq!(SAC_TABLE_INDEX_ENTRY_SIZE, 48);
    }

    #[test]
    fn test_wal_frame_header_size_v2() {
        // WalFrameHeaderV2: magic(4) + flag(1) + schema_version(4) + raft_term(8) + raft_index(8) + data_len(4) + point_count(4) + crc32c(4) = 37
        assert_eq!(WAL_FRAME_HEADER_SIZE_V2, 37);
    }

    #[test]
    fn test_wal_segment_size() {
        assert_eq!(WAL_SEGMENT_SIZE, 64 * 1024 * 1024);
    }

    #[test]
    fn test_magic_values() {
        assert_eq!(SAC_MAGIC, b"RTM8");
        assert_eq!(WAL_MAGIC_V2, b"WAL2");
    }
}

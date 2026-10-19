//! L2 描述符：56 字节/条，per-block，mmap 映射，按 (table_id, min_ts) 排序。
//!
//! ## 二进制布局 (56B, SAC v5 定稿 S3)
//!
//! | 偏移 | 大小 | 字段 |
//! |------|------|------|
//! | 0    | 4    | table_id: u32 LE |
//! | 4    | 8    | block_offset: u64 LE (Block 在文件内起始偏移) |
//! | 12   | 4    | block_size: u32 LE (压缩后 Block 总大小, 含 mini-header/目录/流/CRC) |
//! | 16   | 8    | min_ts: i64 LE |
//! | 24   | 8    | max_ts: i64 LE |
//! | 32   | 2    | column_count: u16 LE |
//! | 34   | 22   | reserved: 全零 |
//!
//! L2 为 **per-block 描述符**：字节偏移只记 `block_offset + block_size`；
//! 列级流偏移下沉到 Block 内 Stream Directory，不进 L2。

use sensearcdb_common::constants::SAC_L2_ENTRY_SIZE;

/// L2 描述符 (56 字节)。
///
/// L2 描述符区位于 SAC 文件末尾（Footer 之前），每个 Block 一条，记录该 Block 的
/// 字节日录与时间/列元数据。Reader 通过 mmap 映射 L2 描述符区，按 (table_id, min_ts)
/// 过滤后用 `block_offset` 定位、用 `block_size` 跳过损坏 Block。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Descriptor {
    /// 表 ID。
    pub table_id: u32,
    /// Block 在 SAC 文件内的起始字节偏移。
    pub block_offset: u64,
    /// 压缩后 Block 总大小（字节），含 mini-header + Stream Directory + 流 + CRC。
    pub block_size: u32,
    /// Block 内最小时间戳（epoch 纳秒）。
    pub min_ts: i64,
    /// Block 内最大时间戳（epoch 纳秒）。
    pub max_ts: i64,
    /// Block 内列数（投影域，用于不解压即可判断能否跳过该 Block）。
    pub column_count: u16,
    /// 保留字段 (全零)。
    pub reserved: [u8; 22],
}

impl L2Descriptor {
    /// 创建新的 L2 描述符。
    ///
    /// # 参数
    /// - `table_id`: 表 ID。
    /// - `block_offset`: Block 在文件内的起始字节偏移。
    /// - `block_size`: 压缩后 Block 总大小。
    /// - `min_ts`: Block 内最小时间戳。
    /// - `max_ts`: Block 内最大时间戳。
    /// - `column_count`: Block 内列数。
    pub fn new(
        table_id: u32,
        block_offset: u64,
        block_size: u32,
        min_ts: i64,
        max_ts: i64,
        column_count: u16,
    ) -> Self {
        Self {
            table_id,
            block_offset,
            block_size,
            min_ts,
            max_ts,
            column_count,
            reserved: [0u8; 22],
        }
    }

    /// 序列化为 56 字节数组。
    pub fn to_bytes(&self) -> [u8; SAC_L2_ENTRY_SIZE] {
        let mut buf = [0u8; SAC_L2_ENTRY_SIZE];

        buf[0..4].copy_from_slice(&self.table_id.to_le_bytes());
        buf[4..12].copy_from_slice(&self.block_offset.to_le_bytes());
        buf[12..16].copy_from_slice(&self.block_size.to_le_bytes());
        buf[16..24].copy_from_slice(&self.min_ts.to_le_bytes());
        buf[24..32].copy_from_slice(&self.max_ts.to_le_bytes());
        buf[32..34].copy_from_slice(&self.column_count.to_le_bytes());
        buf[34..56].copy_from_slice(&self.reserved);

        buf
    }

    /// 从 56 字节数组反序列化。
    pub fn from_bytes(bytes: &[u8; SAC_L2_ENTRY_SIZE]) -> Option<Self> {
        let table_id = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let block_offset = u64::from_le_bytes(bytes[4..12].try_into().ok()?);
        let block_size = u32::from_le_bytes(bytes[12..16].try_into().ok()?);
        let min_ts = i64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let max_ts = i64::from_le_bytes(bytes[24..32].try_into().ok()?);
        let column_count = u16::from_le_bytes(bytes[32..34].try_into().ok()?);
        let reserved: [u8; 22] = bytes[34..56].try_into().ok()?;

        Some(Self {
            table_id,
            block_offset,
            block_size,
            min_ts,
            max_ts,
            column_count,
            reserved,
        })
    }

    /// 检查此描述符是否与给定时间范围重叠。
    #[inline]
    pub fn overlaps(&self, ts_start: i64, ts_end: i64) -> bool {
        self.min_ts <= ts_end && self.max_ts >= ts_start
    }
}

impl Default for L2Descriptor {
    fn default() -> Self {
        Self::new(0, 0, 0, 0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_descriptor_round_trip() {
        let desc = L2Descriptor::new(42, 4096, 1024, 1000, 2000, 8);
        let bytes = desc.to_bytes();
        assert_eq!(bytes.len(), SAC_L2_ENTRY_SIZE);

        let decoded = L2Descriptor::from_bytes(&bytes).expect("decoding failed");
        assert_eq!(decoded, desc);
        assert_eq!(decoded.table_id, 42);
        assert_eq!(decoded.block_offset, 4096);
        assert_eq!(decoded.block_size, 1024);
        assert_eq!(decoded.min_ts, 1000);
        assert_eq!(decoded.max_ts, 2000);
        assert_eq!(decoded.column_count, 8);
    }

    #[test]
    fn test_l2_overlaps() {
        let desc = L2Descriptor::new(1, 0, 0, 1000, 2000, 1);
        assert!(desc.overlaps(500, 1500));
        assert!(!desc.overlaps(3000, 4000));
        assert!(desc.overlaps(2000, 2000));
    }

    #[test]
    fn test_l2_entry_size() {
        assert_eq!(SAC_L2_ENTRY_SIZE, 56);
    }
}

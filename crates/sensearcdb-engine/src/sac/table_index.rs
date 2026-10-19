//! TableIndex：48 字节/条的表级索引，按 table_id 升序，mmap on-demand 驻留。
//!
//! ## 二进制布局 (48B, SAC v9 定稿 §3)
//!
//! | 偏移 | 大小 | 类型 | 字段 |
//! |------|------|------|------|
//! | 0    | 4    | u32 LE | table_id（主键，升序） |
//! | 4    | 4    | u32 LE | block_count（该表在本文件的块数） |
//! | 8    | 8    | i64 LE | min_ts（该表跨块最小时间戳） |
//! | 16   | 8    | i64 LE | max_ts（该表跨块最大时间戳） |
//! | 24   | 4    | u32 LE | first_l2_idx（该表首个块在 L2 数组中的下标） |
//! | 28   | 4    | u32 LE | row_count（该表在本文件的总行数） |
//! | 32   | 8    | u64 LE | total_block_bytes（该表各块 block_size 之和） |
//! | 40   | 2    | u16 LE | column_count（该表列数，schema 投影提示） |
//! | 42   | 1    | u8     | flags（保留，当前恒 0） |
//! | 43   | 5    | u8[5]  | reserved（全零，对齐到 48B） |
//!
//! 定长 48B 使二分查找 = 纯下标计算 `base + idx * 48`，无需扫描，mmap 友好。

use sensearcdb_common::constants::SAC_TABLE_INDEX_ENTRY_SIZE;

/// 单条表索引 (48 字节，定长)。
///
/// 同一 SAC 文件内 `table_id` 唯一，条目按 `table_id` 严格升序。因为块按
/// `(table_id, min_ts)` 升序排布、L2 描述符也按同序排布，故 `first_l2_idx` 天然
/// 单调，表内块在 L2 数组中是连续区间 `[first_l2_idx, first_l2_idx + block_count)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIndexEntry {
    /// 表 ID（主键，升序）。
    pub table_id: u32,
    /// 该表在本文件的块数。
    pub block_count: u32,
    /// 该表在本文件的最小时间戳（跨块，epoch 纳秒）。
    pub min_ts: i64,
    /// 该表在本文件的最大时间戳（跨块，epoch 纳秒）。
    pub max_ts: i64,
    /// 该表首个块在 L2 描述符数组中的下标。
    pub first_l2_idx: u32,
    /// 该表在本文件的总行数。
    pub row_count: u32,
    /// 该表各块 `block_size` 之和（IO 预算/预取）。
    pub total_block_bytes: u64,
    /// 该表列数（schema 投影提示）。
    pub column_count: u16,
    /// 保留标志位（当前恒 0）。
    pub flags: u8,
    /// 保留字段（全零）。
    pub reserved: [u8; 5],
}

impl TableIndexEntry {
    /// 创建新的表索引条目。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: u32,
        block_count: u32,
        min_ts: i64,
        max_ts: i64,
        first_l2_idx: u32,
        row_count: u32,
        total_block_bytes: u64,
        column_count: u16,
        flags: u8,
    ) -> Self {
        Self {
            table_id,
            block_count,
            min_ts,
            max_ts,
            first_l2_idx,
            row_count,
            total_block_bytes,
            column_count,
            flags,
            reserved: [0u8; 5],
        }
    }

    /// 该表覆盖的 L2 下标区间（`[first_l2_idx, first_l2_idx + block_count)`）。
    #[inline]
    pub fn l2_range(&self) -> std::ops::Range<usize> {
        self.first_l2_idx as usize..(self.first_l2_idx as usize + self.block_count as usize)
    }

    /// 检查该表索引是否与给定时间范围重叠。
    #[inline]
    pub fn overlaps(&self, ts_start: i64, ts_end: i64) -> bool {
        self.min_ts <= ts_end && self.max_ts >= ts_start
    }

    /// 序列化为 48 字节数组。
    pub fn to_bytes(&self) -> [u8; SAC_TABLE_INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; SAC_TABLE_INDEX_ENTRY_SIZE];

        buf[0..4].copy_from_slice(&self.table_id.to_le_bytes());
        buf[4..8].copy_from_slice(&self.block_count.to_le_bytes());
        buf[8..16].copy_from_slice(&self.min_ts.to_le_bytes());
        buf[16..24].copy_from_slice(&self.max_ts.to_le_bytes());
        buf[24..28].copy_from_slice(&self.first_l2_idx.to_le_bytes());
        buf[28..32].copy_from_slice(&self.row_count.to_le_bytes());
        buf[32..40].copy_from_slice(&self.total_block_bytes.to_le_bytes());
        buf[40..42].copy_from_slice(&self.column_count.to_le_bytes());
        buf[42] = self.flags;
        buf[43..48].copy_from_slice(&self.reserved);

        buf
    }

    /// 从 48 字节数组反序列化。
    pub fn from_bytes(bytes: &[u8; SAC_TABLE_INDEX_ENTRY_SIZE]) -> Option<Self> {
        let table_id = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let block_count = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let min_ts = i64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let max_ts = i64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let first_l2_idx = u32::from_le_bytes(bytes[24..28].try_into().ok()?);
        let row_count = u32::from_le_bytes(bytes[28..32].try_into().ok()?);
        let total_block_bytes = u64::from_le_bytes(bytes[32..40].try_into().ok()?);
        let column_count = u16::from_le_bytes(bytes[40..42].try_into().ok()?);
        let flags = bytes[42];
        let reserved: [u8; 5] = bytes[43..48].try_into().ok()?;

        Some(Self {
            table_id,
            block_count,
            min_ts,
            max_ts,
            first_l2_idx,
            row_count,
            total_block_bytes,
            column_count,
            flags,
            reserved,
        })
    }
}

impl Default for TableIndexEntry {
    fn default() -> Self {
        Self::new(0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
}

/// 在按 `table_id` 升序的 TableIndex 字节数组中二分查找目标表。
///
/// # 参数
/// - `bytes`: TableIndex 区原始字节（长度须为 48 的整数倍）。
/// - `table_id`: 目标表 ID。
///
/// # 返回值
/// 命中返回条目下标（0-based）；未命中返回 `None`。
pub fn binary_search_table_index(bytes: &[u8], table_id: u32) -> Option<usize> {
    let count = bytes.len() / SAC_TABLE_INDEX_ENTRY_SIZE;
    if count == 0 {
        return None;
    }

    let mut lo = 0usize;
    let mut hi = count - 1;
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let start = mid * SAC_TABLE_INDEX_ENTRY_SIZE;
        let end = start + SAC_TABLE_INDEX_ENTRY_SIZE;
        let entry_bytes: &[u8; SAC_TABLE_INDEX_ENTRY_SIZE] = bytes[start..end].try_into().ok()?;
        let mid_id = u32::from_le_bytes(entry_bytes[0..4].try_into().ok()?);
        match mid_id.cmp(&table_id) {
            std::cmp::Ordering::Equal => return Some(mid),
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => {
                if mid == 0 {
                    break;
                }
                hi = mid - 1;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(table_id: u32) -> TableIndexEntry {
        TableIndexEntry::new(
            table_id,
            2,
            table_id as i64 * 100,
            table_id as i64 * 100 + 50,
            table_id,
            100,
            4096,
            4,
            0,
        )
    }

    #[test]
    fn test_table_index_round_trip() {
        let e = TableIndexEntry::new(42, 7, 1000, 9000, 128, 28672, 123456, 8, 0);
        let bytes = e.to_bytes();
        assert_eq!(bytes.len(), SAC_TABLE_INDEX_ENTRY_SIZE);
        assert_eq!(bytes[0..4], 42u32.to_le_bytes());
        assert_eq!(bytes[40..42], 8u16.to_le_bytes());
        assert_eq!(bytes[42], 0);
        assert_eq!(bytes[43..48], [0u8; 5]);

        let decoded = TableIndexEntry::from_bytes(&bytes).expect("decode failed");
        assert_eq!(decoded, e);
        assert_eq!(decoded.l2_range(), 128..135);
    }

    #[test]
    fn test_binary_search_table_index() {
        let mut bytes: Vec<u8> = Vec::new();
        for id in [3u32, 10, 42, 99, 1000] {
            bytes.extend_from_slice(&entry(id).to_bytes());
        }

        assert_eq!(binary_search_table_index(&bytes, 3), Some(0));
        assert_eq!(binary_search_table_index(&bytes, 10), Some(1));
        assert_eq!(binary_search_table_index(&bytes, 42), Some(2));
        assert_eq!(binary_search_table_index(&bytes, 99), Some(3));
        assert_eq!(binary_search_table_index(&bytes, 1000), Some(4));

        // 未命中：小于最小、中间缺失、大于最大。
        assert_eq!(binary_search_table_index(&bytes, 0), None);
        assert_eq!(binary_search_table_index(&bytes, 50), None);
        assert_eq!(binary_search_table_index(&bytes, 9999), None);
    }

    #[test]
    fn test_binary_search_empty() {
        assert_eq!(binary_search_table_index(&[], 1), None);
        assert_eq!(binary_search_table_index(&[0u8; 48], 0), Some(0));
    }

    #[test]
    fn test_entry_overlaps() {
        let e = TableIndexEntry::new(1, 1, 1000, 2000, 0, 10, 100, 1, 0);
        assert!(e.overlaps(1500, 1500));
        assert!(!e.overlaps(3000, 4000));
        assert!(e.overlaps(0, 1000));
    }
}

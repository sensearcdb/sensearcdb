//! SAC 文件尾 (SacFooter): 64 字节（SAC v9）。
//!
//! ## 二进制布局（SAC v9 定稿 §2.7）
//!
//! | 偏移 | 大小 | 字段 |
//! |------|------|------|
//! | 0    | 8    | min_ts: i64 LE |
//! | 8    | 8    | max_ts: i64 LE |
//! | 16   | 4    | block_count: u32 LE |
//! | 20   | 4    | table_count: u32 LE（总表数 T） |
//! | 24   | 8    | l2_offset: u64 LE (L2 描述符区在文件中的偏移) |
//! | 32   | 8    | table_index_offset: u64 LE (TableIndex 区在文件中的偏移) |
//! | 40   | 4    | block_count_mirror: u32 LE（镜像校验，须 == block_count） |
//! | 44   | 4    | table_count_mirror: u32 LE（镜像校验，须 == table_count） |
//! | 48   | 4    | magic: "RTM8" |
//! | 52   | 4    | crc32c: u32 LE |
//! | 56   | 8    | reserved: 全零（对齐到 64B） |

use sensearcdb_common::constants::{SAC_FOOTER_SIZE, SAC_MAGIC};

/// SAC 文件尾 (64 字节)。
///
/// 每个 SAC 文件以 `SacFooter` 结尾，包含全局时间范围、Block/Table 计数（含镜像校验）、
/// L2/TableIndex 偏移和 CRC32C 校验值。字节偏移一律使用 u64（无 4GB 硬上限，O4=A）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SacFooter {
    /// 文件中所有 Block 的最小时间戳（epoch 纳秒）。
    pub min_ts: i64,
    /// 文件中所有 Block 的最大时间戳（epoch 纳秒）。
    pub max_ts: i64,
    /// Block 总数。
    pub block_count: u32,
    /// 表总数。
    pub table_count: u32,
    /// L2 描述符区在文件中的字节偏移。
    pub l2_offset: u64,
    /// TableIndex 区在文件中的字节偏移。
    pub table_index_offset: u64,
    /// Block 总数镜像（撕裂写防护，须 == `block_count`）。
    pub block_count_mirror: u32,
    /// 表总数镜像（撕裂写防护，须 == `table_count`）。
    pub table_count_mirror: u32,
    /// Magic number 副本: "RTM8"。
    pub magic: [u8; 4],
    /// CRC32C 校验值（覆盖 Header + 所有 Block + L2 + TableIndex）。
    pub crc32c: u32,
    /// 保留字段（全零，对齐到 64B）。
    pub reserved: [u8; 8],
}

impl SacFooter {
    /// 创建新的 SAC 文件尾。
    ///
    /// `block_count_mirror` 与 `table_count_mirror` 自动与对应计数保持一致。
    ///
    /// # 参数
    /// - `min_ts`: 全局最小时间戳。
    /// - `max_ts`: 全局最大时间戳。
    /// - `block_count`: Block 总数。
    /// - `table_count`: 表总数。
    /// - `l2_offset`: L2 描述符区偏移。
    /// - `table_index_offset`: TableIndex 区偏移。
    /// - `crc32c`: CRC32C 校验值。
    pub fn new(
        min_ts: i64,
        max_ts: i64,
        block_count: u32,
        table_count: u32,
        l2_offset: u64,
        table_index_offset: u64,
        crc32c: u32,
    ) -> Self {
        Self {
            min_ts,
            max_ts,
            block_count,
            table_count,
            l2_offset,
            table_index_offset,
            block_count_mirror: block_count,
            table_count_mirror: table_count,
            magic: *SAC_MAGIC,
            crc32c,
            reserved: [0u8; 8],
        }
    }

    /// 校验 `block_count == block_count_mirror` 且 `table_count == table_count_mirror`。
    ///
    /// 用于 `from_bytes` 反序列化时检测 Footer 尾部字段撕裂写。
    #[inline]
    pub fn is_consistent(&self) -> bool {
        self.block_count == self.block_count_mirror && self.table_count == self.table_count_mirror
    }

    /// 序列化为 64 字节数组。
    pub fn to_bytes(&self) -> [u8; SAC_FOOTER_SIZE] {
        let mut buf = [0u8; SAC_FOOTER_SIZE];

        buf[0..8].copy_from_slice(&self.min_ts.to_le_bytes());
        buf[8..16].copy_from_slice(&self.max_ts.to_le_bytes());
        buf[16..20].copy_from_slice(&self.block_count.to_le_bytes());
        buf[20..24].copy_from_slice(&self.table_count.to_le_bytes());
        buf[24..32].copy_from_slice(&self.l2_offset.to_le_bytes());
        buf[32..40].copy_from_slice(&self.table_index_offset.to_le_bytes());
        buf[40..44].copy_from_slice(&self.block_count_mirror.to_le_bytes());
        buf[44..48].copy_from_slice(&self.table_count_mirror.to_le_bytes());
        buf[48..52].copy_from_slice(&self.magic);
        buf[52..56].copy_from_slice(&self.crc32c.to_le_bytes());
        buf[56..64].copy_from_slice(&self.reserved);

        buf
    }

    /// 从 64 字节数组反序列化。
    ///
    /// # 返回值
    /// 解析后的 `SacFooter`；若 magic 不匹配或 `block_count != block_count_mirror`
    /// 或 `table_count != table_count_mirror` 则返回 `None`。
    pub fn from_bytes(bytes: &[u8; SAC_FOOTER_SIZE]) -> Option<Self> {
        let min_ts = i64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let max_ts = i64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let block_count = u32::from_le_bytes(bytes[16..20].try_into().ok()?);
        let table_count = u32::from_le_bytes(bytes[20..24].try_into().ok()?);
        let l2_offset = u64::from_le_bytes(bytes[24..32].try_into().ok()?);
        let table_index_offset = u64::from_le_bytes(bytes[32..40].try_into().ok()?);
        let block_count_mirror = u32::from_le_bytes(bytes[40..44].try_into().ok()?);
        let table_count_mirror = u32::from_le_bytes(bytes[44..48].try_into().ok()?);
        let magic: [u8; 4] = bytes[48..52].try_into().ok()?;

        if &magic != SAC_MAGIC {
            return None;
        }

        let crc32c = u32::from_le_bytes(bytes[52..56].try_into().ok()?);
        let reserved: [u8; 8] = bytes[56..64].try_into().ok()?;

        let footer = Self {
            min_ts,
            max_ts,
            block_count,
            table_count,
            l2_offset,
            table_index_offset,
            block_count_mirror,
            table_count_mirror,
            magic,
            crc32c,
            reserved,
        };

        // S1：镜像校验，撕裂写防护。
        if !footer.is_consistent() {
            return None;
        }

        Some(footer)
    }
}

impl Default for SacFooter {
    fn default() -> Self {
        Self::new(0, 0, 0, 0, 0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_round_trip() {
        let ftr = SacFooter::new(100, 200, 10, 3, 4096, 8192, 0xDEAD_BEEF);
        let bytes = ftr.to_bytes();
        assert_eq!(bytes.len(), SAC_FOOTER_SIZE);
        assert_eq!(&bytes[48..52], SAC_MAGIC);

        let decoded = SacFooter::from_bytes(&bytes).expect("decoding failed");
        assert_eq!(decoded, ftr);
        assert_eq!(decoded.block_count, 10);
        assert_eq!(decoded.table_count, 3);
        assert_eq!(decoded.block_count_mirror, 10);
        assert_eq!(decoded.table_count_mirror, 3);
        assert_eq!(decoded.crc32c, 0xDEAD_BEEF);
    }

    #[test]
    fn test_footer_mirror_consistent() {
        let ftr = SacFooter::new(0, 0, 5, 2, 0, 0, 0);
        assert!(ftr.is_consistent());
        assert_eq!(ftr.block_count, ftr.block_count_mirror);
        assert_eq!(ftr.table_count, ftr.table_count_mirror);
    }

    #[test]
    fn test_footer_bad_magic() {
        let mut bytes = [0u8; SAC_FOOTER_SIZE];
        bytes[48..52].copy_from_slice(b"BAD!");
        assert!(SacFooter::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_footer_mirror_mismatch() {
        let ftr = SacFooter::new(0, 0, 5, 2, 0, 0, 0);
        let mut bytes = ftr.to_bytes();
        // 破坏 block_count mirror 字段
        bytes[40..44].copy_from_slice(&999u32.to_le_bytes());
        assert!(SacFooter::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_footer_table_count_mirror_mismatch() {
        let ftr = SacFooter::new(0, 0, 5, 2, 0, 0, 0);
        let mut bytes = ftr.to_bytes();
        // 破坏 table_count mirror 字段
        bytes[44..48].copy_from_slice(&999u32.to_le_bytes());
        assert!(SacFooter::from_bytes(&bytes).is_none());
    }
}

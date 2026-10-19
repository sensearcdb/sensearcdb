//! SAC 文件头 (SacHeader): 32 字节（SAC v8）。
//!
//! ## 二进制布局
//!
//! | 偏移 | 大小 | 字段 |
//! |------|------|------|
//! | 0    | 4    | magic: "RTM8" |
//! | 4    | 2    | version: u16 LE |
//! | 6    | 6    | partition_key: epoch ns LE (前 6 字节) |
//! | 12   | 1    | format_flags: u8（bit0 = TDE_ENCRYPTED） |
//! | 13   | 1    | level: u8 (0x00=L0, 0x01=L1, 0x02=L2, 0x03=L3, 0x04=L4) |
//! | 14   | 2    | seq: u16 LE |
//! | 16   | 4    | dek_version: u32 LE（0 = 明文/未加密） |
//! | 20   | 4    | tenant_id: u32 LE（默认租户 = 0） |
//! | 24   | 8    | write_epoch: u64 LE（文件写入时间 epoch ns，跨文件 LWW 排序键） |

use sensearcdb_common::constants::{SAC_HEADER_SIZE, SAC_MAGIC, SAC_VERSION};

use crate::compression::CompressionType;

/// `format_flags` bit0：TDE 加密标志（1 = 已加密）。
pub const FORMAT_FLAG_TDE_ENCRYPTED: u8 = 0x01;

/// SAC 文件层级。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SacLevel {
    /// L0: MemTable 直接刷出的文件，未经 Compaction。
    L0 = 0x00,
    /// L1: 经过 L0→L1 Compaction 的文件。
    L1 = 0x01,
    /// L2: 经过 L1→L2 Compaction 的文件（ZSTD9 压缩）。
    L2 = 0x02,
    /// L3: 经过 L2→L3 Compaction 的文件（ZSTD9 压缩，分代合并封顶层）。
    L3 = 0x03,
    /// L4: 分区 final 的最终态文件（一天一文件，ZSTD9 压缩）。
    L4 = 0x04,
}

impl SacLevel {
    /// 从 u8 解析层级。
    ///
    /// # 返回值
    /// 对应的 `SacLevel`；若输入值无效则返回 `None`。
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x00 => Some(SacLevel::L0),
            0x01 => Some(SacLevel::L1),
            0x02 => Some(SacLevel::L2),
            0x03 => Some(SacLevel::L3),
            0x04 => Some(SacLevel::L4),
            _ => None,
        }
    }

    /// 该层级 SAC 数据流的块级压缩算法（刀2：L0=LZ4，L1/L2/L3/L4=ZSTD9）。
    ///
    /// 与 `strategy.rs::compression_for` 的口径一致：L0 追求速度用 LZ4，
    /// L1 起（含 final 的 L4）追求压缩率用 ZSTD9。Reader 据此对 Block 内
    /// ts/val/qual 三流解压，避免「写入用 ZSTD、读取仍按 LZ4」导致解压失败。
    pub fn compression(self) -> CompressionType {
        match self {
            SacLevel::L0 => CompressionType::Lz4,
            SacLevel::L1 | SacLevel::L2 | SacLevel::L3 | SacLevel::L4 => CompressionType::Zstd9,
        }
    }
}

/// SAC 文件头 (32 字节，v8)。
///
/// 每个 SAC 文件以 `SacHeader` 开头，用于识别文件类型、格式版本、
/// 分区键、层级、序列号、租户与写入时间。Reader 通过 `magic` 字段验证文件格式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SacHeader {
    /// Magic number: "RTM8"。
    pub magic: [u8; 4],
    /// 格式版本号 (当前: 8)。
    pub version: u16,
    /// 分区键 (epoch 纳秒，低 6 字节 LE)。
    pub partition_key: [u8; 6],
    /// 格式标志位（bit0 = TDE 加密标志，其余保留）。
    pub format_flags: u8,
    /// SAC 层级。
    pub level: SacLevel,
    /// 分区内序列号 (LE)。
    pub seq: u16,
    /// TDE 加密标志（序列化到 `format_flags bit0`）。
    pub tde_encrypted: bool,
    /// DEK 版本（序列化到 offset 16..20 LE；0 = 明文/未加密）。
    pub dek_version: u32,
    /// 租户 ID（offset 20..24 LE；本批恒为默认租户 0）。
    pub tenant_id: u32,
    /// 文件写入时间（offset 24..32 LE，epoch ns）——跨文件 LWW 排序键。
    pub write_epoch: u64,
}

impl SacHeader {
    /// 创建新的 SAC 文件头。
    ///
    /// # 参数
    /// - `partition_key_ns`: 分区键（epoch 纳秒）。
    /// - `level`: SAC 层级。
    /// - `seq`: 分区内序列号。
    pub fn new(partition_key_ns: i64, level: SacLevel, seq: u16) -> Self {
        let mut pk_bytes = [0u8; 6];
        let pk = partition_key_ns as u64;
        pk_bytes[0] = (pk & 0xFF) as u8;
        pk_bytes[1] = ((pk >> 8) & 0xFF) as u8;
        pk_bytes[2] = ((pk >> 16) & 0xFF) as u8;
        pk_bytes[3] = ((pk >> 24) & 0xFF) as u8;
        pk_bytes[4] = ((pk >> 32) & 0xFF) as u8;
        pk_bytes[5] = ((pk >> 40) & 0xFF) as u8;

        Self {
            magic: *SAC_MAGIC,
            version: SAC_VERSION,
            partition_key: pk_bytes,
            format_flags: 0,
            level,
            seq,
            tde_encrypted: false,
            dek_version: 0,
            tenant_id: sensearcdb_common::constants::DEFAULT_TENANT_ID,
            write_epoch: 0,
        }
    }

    /// 设置 TDE 加密标志与 DEK 版本（链式）。
    ///
    /// # 参数
    /// - `dek_version`: 活跃 DEK 版本（非 0）。
    pub fn with_tde(mut self, dek_version: u32) -> Self {
        self.tde_encrypted = true;
        self.dek_version = dek_version;
        self
    }

    /// 该文件是否已加密。
    pub fn is_encrypted(&self) -> bool {
        self.tde_encrypted
    }

    /// 序列化为 32 字节数组。
    ///
    /// # 返回值
    /// `[u8; 32]` 的字节表示。
    pub fn to_bytes(&self) -> [u8; SAC_HEADER_SIZE] {
        let mut buf = [0u8; SAC_HEADER_SIZE];

        // magic (4B)
        buf[0..4].copy_from_slice(&self.magic);

        // version (2B LE)
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());

        // partition_key (6B LE)
        buf[6..12].copy_from_slice(&self.partition_key);

        // format_flags (1B)：bit0 = TDE_ENCRYPTED
        let flags = (self.format_flags & !FORMAT_FLAG_TDE_ENCRYPTED)
            | if self.tde_encrypted {
                FORMAT_FLAG_TDE_ENCRYPTED
            } else {
                0
            };
        buf[12] = flags;

        // level (1B)
        buf[13] = self.level as u8;

        // seq (2B LE)
        buf[14..16].copy_from_slice(&self.seq.to_le_bytes());

        // dek_version (u32 LE)
        buf[16..20].copy_from_slice(&self.dek_version.to_le_bytes());

        // tenant_id (u32 LE)
        buf[20..24].copy_from_slice(&self.tenant_id.to_le_bytes());

        // write_epoch (u64 LE)
        buf[24..32].copy_from_slice(&self.write_epoch.to_le_bytes());

        buf
    }

    /// 从 32 字节数组反序列化。
    ///
    /// # 参数
    /// - `bytes`: 32 字节的原始数据。
    ///
    /// # 返回值
    /// 解析后的 `SacHeader`；若 magic 不匹配或 level 无效则返回 `None`。
    pub fn from_bytes(bytes: &[u8; SAC_HEADER_SIZE]) -> Option<Self> {
        // 检查 magic
        let magic: [u8; 4] = bytes[0..4].try_into().ok()?;
        if &magic != SAC_MAGIC {
            return None;
        }

        // version
        let version = u16::from_le_bytes(bytes[4..6].try_into().ok()?);

        // partition_key
        let partition_key: [u8; 6] = bytes[6..12].try_into().ok()?;

        // format_flags
        let format_flags = bytes[12];

        // level
        let level = SacLevel::from_u8(bytes[13])?;

        // seq
        let seq = u16::from_le_bytes(bytes[14..16].try_into().ok()?);

        // dek_version (u32 LE)
        let dek_version = u32::from_le_bytes(bytes[16..20].try_into().ok()?);

        // tenant_id (u32 LE)
        let tenant_id = u32::from_le_bytes(bytes[20..24].try_into().ok()?);

        // write_epoch (u64 LE)
        let write_epoch = u64::from_le_bytes(bytes[24..32].try_into().ok()?);

        Some(Self {
            magic,
            version,
            partition_key,
            format_flags,
            level,
            seq,
            tde_encrypted: (format_flags & FORMAT_FLAG_TDE_ENCRYPTED) != 0,
            dek_version,
            tenant_id,
            write_epoch,
        })
    }
}

impl Default for SacHeader {
    fn default() -> Self {
        Self::new(0, SacLevel::L0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_round_trip() {
        let hdr = SacHeader::new(1_692_180_000_000_000_000, SacLevel::L1, 42);
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), SAC_HEADER_SIZE);
        assert_eq!(&bytes[0..4], SAC_MAGIC);
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            SAC_VERSION
        );
        assert_eq!(u32::from_le_bytes(bytes[20..24].try_into().unwrap()), 0); // tenant_id 默认 0

        let decoded = SacHeader::from_bytes(&bytes).expect("decoding failed");
        assert_eq!(decoded, hdr);
    }

    #[test]
    fn test_header_round_trip_with_write_epoch() {
        let mut hdr = SacHeader::new(0, SacLevel::L0, 7);
        hdr.write_epoch = 1_700_000_000_123_456_789;
        hdr.tenant_id = 1;

        let bytes = hdr.to_bytes();
        assert_eq!(
            u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            1_700_000_000_123_456_789
        );

        let decoded = SacHeader::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.write_epoch, 1_700_000_000_123_456_789);
        assert_eq!(decoded.tenant_id, 1);
    }

    #[test]
    fn test_header_bad_magic() {
        let mut bytes = [0u8; SAC_HEADER_SIZE];
        bytes[0..4].copy_from_slice(b"BAD!");
        assert!(SacHeader::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_header_bad_level() {
        let hdr = SacHeader::new(0, SacLevel::L0, 0);
        let mut bytes = hdr.to_bytes();
        bytes[13] = 0xFF; // invalid level
        assert!(SacHeader::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_sac_level_discriminants() {
        assert_eq!(SacLevel::L0 as u8, 0x00);
        assert_eq!(SacLevel::L1 as u8, 0x01);
        assert_eq!(SacLevel::L2 as u8, 0x02);
        assert_eq!(SacLevel::L3 as u8, 0x03);
        assert_eq!(SacLevel::L4 as u8, 0x04);
        assert_eq!(SacLevel::from_u8(0x03), Some(SacLevel::L3));
        assert_eq!(SacLevel::from_u8(0x04), Some(SacLevel::L4));
        assert_eq!(SacLevel::L3.compression(), CompressionType::Zstd9);
        assert_eq!(SacLevel::L4.compression(), CompressionType::Zstd9);
    }
}

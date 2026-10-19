//! WAL 帧格式：`WalFrameHeaderV2`（37 字节，全模式统一 "WAL2"）。
//!
//! SenseArcDB 全模式（单机 + 集群）统一使用 WAL2 帧，无 V1 兼容分支。
//! 单机写路径以 `raft_term=0`、`raft_index=自增 seq` 复用同一帧格式。
//!
//! ## 二进制布局 (37B)
//!
//! | 偏移 | 大小 | 字段 |
//! |------|------|------|
//! | 0    | 4    | magic: "WAL2" |
//! | 4    | 1    | flag: u8 (帧类型: 0=data, 1=control, 2=checkpoint) |
//! | 5    | 4    | schema_version: u32 LE |
//! | 9    | 8    | raft_term: u64 LE |
//! | 17   | 8    | raft_index: u64 LE |
//! | 25   | 4    | data_len: u32 LE (payload 长度) |
//! | 29   | 4    | point_count: u32 LE (帧内数据点数) |
//! | 33   | 4    | crc32c: u32 LE (覆盖 header+payload) |

use std::hash::Hasher;

use sensearcdb_common::constants::{WAL_FRAME_HEADER_SIZE_V2, WAL_MAGIC_V2};

/// 加密 WAL 帧 payload 的自描述前缀：`"WALE"`。
///
/// 加密后的 payload 线上格式（S4 Step 2：前缀 8B → 12B，加 tenant）：
/// `"WALE"(4B) || tenant_id(u32 LE 4B) || dek_version(u32 LE 4B) || nonce(12B) || tag(16B) || ciphertext`。
///
/// 沿用 WAL2 帧头 + 扩展加密 payload 前缀；「永不向前兼容」下旧 WAL 直接失效，
/// `FORMAT_VERSION_WAL` 保持 2 不变，仅 WALE 前缀长度变。
pub const WAL_ENC_MAGIC: &[u8; 4] = b"WALE";

/// `WALE` 前缀 + `tenant_id` + `dek_version` 的固定长度（12 字节）。
pub const WAL_ENC_PREFIX_LEN: usize = 12;

/// WAL 帧类型标志位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalFrameFlag {
    /// 普通数据帧。
    Data = 0,
    /// 控制帧（如 Raft 配置变更）。
    Control = 1,
    /// Checkpoint 帧（提示 Reader 可截断日志）。
    Checkpoint = 2,
}

impl WalFrameFlag {
    /// 从 u8 解码，未知值回退到 Data。
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => WalFrameFlag::Data,
            1 => WalFrameFlag::Control,
            2 => WalFrameFlag::Checkpoint,
            _ => WalFrameFlag::Data,
        }
    }
}

/// WAL 帧头 (37 字节，全模式统一 WAL2)。
///
/// 包含 Raft 日志复制所需的 `raft_term` 和 `raft_index` 字段，
/// 以及用于区分帧类型的 `flag` 字段。单机写路径以
/// `raft_term=0`、`raft_index=自增 seq` 复用本结构。
///
/// `point_count` 记录本帧包含的数据点数，用于跨重启 seq 续号时计算
/// **点级上界**（`raft_index + point_count - 1`），避免多点帧只读帧首
/// `raft_index` 而低估已分配 seq 上界（🟥 隐患）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrameHeaderV2 {
    /// Magic number: "WAL2"。
    pub magic: [u8; 4],
    /// 帧类型标志: 0=data, 1=control, 2=checkpoint。
    pub flag: u8,
    /// 写入时的 Schema 版本号。
    pub schema_version: u32,
    /// Raft term（单机为 0）。
    pub raft_term: u64,
    /// Raft log index（单机复用为自增 seq）。
    pub raft_index: u64,
    /// Payload 长度（字节）。
    pub data_len: u32,
    /// 帧内数据点数（Data 帧为 `points.len()`；Control/Checkpoint 帧为 0）。
    pub point_count: u32,
    /// CRC32C 校验值（覆盖 header + payload）。
    pub crc32c: u32,
}

impl WalFrameHeaderV2 {
    /// 创建新的 WAL 帧头。
    ///
    /// # 参数
    /// - `flag`: 帧类型。
    /// - `schema_version`: 写入时的 Schema 版本。
    /// - `raft_term`: Raft term。
    /// - `raft_index`: Raft log index。
    /// - `payload_len`: payload 字节数。
    /// - `point_count`: 帧内数据点数（Control/Checkpoint 帧传 0）。
    /// - `crc32c`: CRC32C 校验值。
    pub fn new(
        flag: WalFrameFlag,
        schema_version: u32,
        raft_term: u64,
        raft_index: u64,
        payload_len: u32,
        point_count: u32,
        crc32c: u32,
    ) -> Self {
        Self {
            magic: *WAL_MAGIC_V2,
            flag: flag as u8,
            schema_version,
            raft_term,
            raft_index,
            data_len: payload_len,
            point_count,
            crc32c,
        }
    }

    /// 序列化为 37 字节数组。
    pub fn to_bytes(&self) -> [u8; WAL_FRAME_HEADER_SIZE_V2] {
        let mut buf = [0u8; WAL_FRAME_HEADER_SIZE_V2];

        // offset 0: magic (4B)
        buf[0..4].copy_from_slice(&self.magic);
        // offset 4: flag (1B)
        buf[4] = self.flag;
        // offset 5: schema_version (4B) LE
        buf[5..9].copy_from_slice(&self.schema_version.to_le_bytes());
        // offset 9: raft_term (8B) LE
        buf[9..17].copy_from_slice(&self.raft_term.to_le_bytes());
        // offset 17: raft_index (8B) LE
        buf[17..25].copy_from_slice(&self.raft_index.to_le_bytes());
        // offset 25: data_len (4B) LE
        buf[25..29].copy_from_slice(&self.data_len.to_le_bytes());
        // offset 29: point_count (4B) LE
        buf[29..33].copy_from_slice(&self.point_count.to_le_bytes());
        // offset 33: crc32c (4B) LE
        buf[33..37].copy_from_slice(&self.crc32c.to_le_bytes());

        buf
    }

    /// 从 37 字节数组反序列化。
    ///
    /// # 返回值
    /// 解析后的 `WalFrameHeaderV2`；若 magic 不匹配则返回 `None`。
    pub fn from_bytes(bytes: &[u8; WAL_FRAME_HEADER_SIZE_V2]) -> Option<Self> {
        let magic: [u8; 4] = bytes[0..4].try_into().ok()?;

        if &magic != WAL_MAGIC_V2 {
            return None;
        }

        let flag = bytes[4];
        let schema_version = u32::from_le_bytes(bytes[5..9].try_into().ok()?);
        let raft_term = u64::from_le_bytes(bytes[9..17].try_into().ok()?);
        let raft_index = u64::from_le_bytes(bytes[17..25].try_into().ok()?);
        let data_len = u32::from_le_bytes(bytes[25..29].try_into().ok()?);
        let point_count = u32::from_le_bytes(bytes[29..33].try_into().ok()?);
        let crc32c = u32::from_le_bytes(bytes[33..37].try_into().ok()?);

        Some(Self {
            magic,
            flag,
            schema_version,
            raft_term,
            raft_index,
            data_len,
            point_count,
            crc32c,
        })
    }

    /// 验证 CRC32C 校验值。
    ///
    /// # 参数
    /// - `payload`: 帧的 payload 数据。
    ///
    /// # 返回值
    /// `true` 如果 CRC32C 校验通过。
    pub fn verify_crc(&self, payload: &[u8]) -> bool {
        let mut hasher = crc32c::Crc32cHasher::new(0);
        // CRC covers first 33 bytes of header (excluding crc32c field at offset 33)
        hasher.write(&self.to_bytes()[..33]);
        hasher.write(payload);
        hasher.finish() as u32 == self.crc32c
    }

    /// 返回帧类型标志。
    pub fn frame_flag(&self) -> WalFrameFlag {
        WalFrameFlag::from_u8(self.flag)
    }
}

impl Default for WalFrameHeaderV2 {
    fn default() -> Self {
        Self::new(WalFrameFlag::Data, 0, 0, 0, 0, 0, 0)
    }
}

/// 校验数据并计算 CRC32C。
///
/// # 参数
/// - `header_bytes`: header 的前 33 字节（不含 crc32c 字段）。
/// - `payload`: payload 数据。
///
/// # 返回值
/// CRC32C 值。
pub fn compute_wal_crc(header_bytes: &[u8; 33], payload: &[u8]) -> u32 {
    let mut hasher = crc32c::Crc32cHasher::new(0);
    hasher.write(header_bytes);
    hasher.write(payload);
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_header_round_trip() {
        let hdr = WalFrameHeaderV2::new(
            WalFrameFlag::Data,
            1,
            5,   // raft_term
            100, // raft_index
            200, // payload_len
            7,   // point_count
            0xDEAD_BEEF,
        );
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), WAL_FRAME_HEADER_SIZE_V2);
        assert_eq!(&bytes[0..4], WAL_MAGIC_V2);

        let decoded = WalFrameHeaderV2::from_bytes(&bytes).expect("decoding failed");
        assert_eq!(decoded, hdr);
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.raft_term, 5);
        assert_eq!(decoded.raft_index, 100);
        assert_eq!(decoded.data_len, 200);
        assert_eq!(decoded.point_count, 7);
    }

    #[test]
    fn test_wal_header_flag_types() {
        let data_hdr = WalFrameHeaderV2::new(WalFrameFlag::Data, 0, 0, 0, 0, 0, 0);
        let ctrl_hdr = WalFrameHeaderV2::new(WalFrameFlag::Control, 0, 0, 0, 0, 0, 0);
        let ckpt_hdr = WalFrameHeaderV2::new(WalFrameFlag::Checkpoint, 0, 0, 0, 0, 0, 0);

        assert_eq!(data_hdr.frame_flag(), WalFrameFlag::Data);
        assert_eq!(ctrl_hdr.frame_flag(), WalFrameFlag::Control);
        assert_eq!(ckpt_hdr.frame_flag(), WalFrameFlag::Checkpoint);

        assert_eq!(data_hdr.flag, 0);
        assert_eq!(ctrl_hdr.flag, 1);
        assert_eq!(ckpt_hdr.flag, 2);
    }

    #[test]
    fn test_wal_header_crc_verification() {
        let payload = b"test_payload";
        let mut hdr =
            WalFrameHeaderV2::new(WalFrameFlag::Data, 1, 3, 42, payload.len() as u32, 4, 0);

        // Build header prefix (first 33 bytes) and compute CRC
        let hdr_bytes = hdr.to_bytes();
        let mut hdr_prefix = [0u8; 33];
        hdr_prefix.copy_from_slice(&hdr_bytes[..33]);

        let crc = compute_wal_crc(&hdr_prefix, payload);
        hdr.crc32c = crc;

        assert!(hdr.verify_crc(payload));
        assert!(!hdr.verify_crc(b"corrupted_data"));
    }

    #[test]
    fn test_wal_header_size() {
        assert_eq!(WAL_FRAME_HEADER_SIZE_V2, 37);
    }

    #[test]
    fn test_wal_header_bad_magic() {
        let mut bytes = [0u8; WAL_FRAME_HEADER_SIZE_V2];
        bytes[0..4].copy_from_slice(b"BAD!");
        assert!(WalFrameHeaderV2::from_bytes(&bytes).is_none());
    }
}

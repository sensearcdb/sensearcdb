//! WAL 重放读取器（全模式统一 WAL2）。
//!
//! WalReader 按顺序读取 WAL 段文件，解析 WAL2（37B header）帧，
//! 支持按 seq 过滤重放。用于崩溃恢复和 Replay 场景。
//!
//! ## 硬规则：schema_version 不匹配只 skip + log，绝不 return Err
//!
//! WAL 重放是"尽力恢复"路径。即使 `schema_version` 完全无法识别，
//! 也只记录日志并跳过该帧，继续处理后续帧。不能因为单个帧的 schema 问题
//! 就中断整个恢复流程。

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use sensearcdb_common::{constants::WAL_FRAME_HEADER_SIZE_V2, SenseArcError, TenantId};
use tracing::{debug, warn};

use crate::compression::decompress_lz4;
use crate::encryption::{EncryptContext, EncryptError, SharedEncryptor};
use crate::engine::DataPoint;
use crate::wal::format::{WalFrameHeaderV2, WAL_ENC_MAGIC, WAL_ENC_PREFIX_LEN};
use crate::wal::writer::{deserialize_points, DecodedWalFrame};

/// WAL 读取器。
///
/// 按顺序遍历 WAL 段文件，解码每个帧并返回数据点。
pub struct WalReader {
    /// WAL 文件目录。
    wal_dir: PathBuf,
    /// 可选加密器（TDE 关闭时为 `None`）。
    encryptor: SharedEncryptor,
}

/// 从 WAL 帧解码的单条记录。
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// 帧数据点列表。
    pub points: Vec<DataPoint>,
    /// WAL 段序号。
    pub segment_seq: u64,
    /// 帧在段内的偏移。
    pub frame_offset: u64,
    /// Schema 版本号。
    pub schema_version: u32,
    /// Raft term（单机为 0）。
    pub raft_term: u64,
    /// Raft log index（单机复用为自增 seq）。
    pub raft_index: u64,
}

/// 段内 WAL 读取器：处理 schema_version 不匹配时的日志降级。
struct WalSegmentReader {
    /// 当前 segment 内 schema_version 不匹配的 warn 计数。
    /// 首次不匹配 → warn!，后续 → debug!，防止 Schema 回滚测试时日志爆炸。
    schema_version_warn_count: u32,
}

impl WalSegmentReader {
    fn new() -> Self {
        Self {
            schema_version_warn_count: 0,
        }
    }

    /// 处理 schema_version 不匹配的 WAL 帧。
    ///
    /// ## 硬规则：只 skip + log，绝不 return Err
    fn handle_schema_mismatch(&mut self, frame_version: u32, current_version: u32) {
        if frame_version > current_version {
            if self.schema_version_warn_count == 0 {
                warn!(
                    schema_version.frame = frame_version,
                    schema_version.current = current_version,
                    "WAL frame schema_version > current, skipping (first in segment)"
                );
            } else {
                debug!(
                    schema_version.frame = frame_version,
                    schema_version.current = current_version,
                    count = self.schema_version_warn_count,
                    "WAL frame schema_version > current, skipping (suppressed)"
                );
            }
            self.schema_version_warn_count += 1;
        }
    }

    /// 切换 segment 时重置计数器。
    fn reset(&mut self) {
        self.schema_version_warn_count = 0;
    }
}

impl WalReader {
    /// 创建新的 WAL 读取器（TDE 关闭）。
    ///
    /// # 参数
    /// - `wal_dir`: WAL 文件目录路径。
    pub fn new(wal_dir: impl Into<PathBuf>) -> Self {
        Self {
            wal_dir: wal_dir.into(),
            encryptor: None,
        }
    }

    /// 创建带 TDE 加密器的 WAL 读取器。
    ///
    /// # 参数
    /// - `wal_dir`: WAL 文件目录路径。
    /// - `encryptor`: 可选加密器（`None` = TDE 关）。
    pub fn new_with_encryptor(wal_dir: impl Into<PathBuf>, encryptor: SharedEncryptor) -> Self {
        Self {
            wal_dir: wal_dir.into(),
            encryptor,
        }
    }

    /// 列出所有 WAL 段文件，按序号排序。
    pub fn list_segments(&self) -> Result<Vec<PathBuf>, SenseArcError> {
        let mut segments = Vec::new();

        let entries = fs::read_dir(&self.wal_dir)
            .map_err(|e| SenseArcError::WalIoError(format!("read dir: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| SenseArcError::WalIoError(format!("entry: {}", e)))?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "wal") {
                segments.push(path);
            }
        }

        // 按文件名排序（段序号递增）
        segments.sort();

        Ok(segments)
    }

    /// 读取并重放所有 WAL 段文件。
    ///
    /// # 参数
    /// - `current_schema_version`: 当前 Schema 版本号（用于 schema_version 校验）。
    ///
    /// # 返回值
    /// 所有 WAL 帧的记录列表。
    pub fn replay_all(&self, current_schema_version: u32) -> Result<Vec<WalRecord>, SenseArcError> {
        let mut records = Vec::new();
        let segments = self.list_segments()?;

        for segment_path in &segments {
            let seg_records = self.read_segment(segment_path, current_schema_version, None)?;
            records.extend(seg_records);
        }

        Ok(records)
    }

    /// 读取并重放 `raft_index > after_seq` 的 WAL 帧（解码版剪枝）。
    ///
    /// 与 [`WalReader::replay_all`] 相同，但**在读取 payload / CRC / 解密 / 解压 /
    /// 反序列化之前**，依据 37B 帧头内的 `raft_index` 判定：`raft_index <= after_seq`
    /// 的帧数据已完整持久化在 SAC/snapshot，直接跳过（只读帧头 + 前进 offset），
    /// 省去已持久化帧的解码开销。产出的 `Vec<WalRecord>` 与「`replay_all` + 事后按
    /// `rec.raft_index <= after_seq` 过滤」的结果**完全一致**（同样的帧、同样的顺序、
    /// 同样的解码结果）。
    ///
    /// # 参数
    /// - `after_seq`: 已落盘最大数据 seq（SAC 水位），`raft_index <= after_seq` 的帧被跳过。
    /// - `current_schema_version`: 当前 Schema 版本号（用于 schema_version 校验）。
    pub fn replay_after(
        &self,
        after_seq: u64,
        current_schema_version: u32,
    ) -> Result<Vec<WalRecord>, SenseArcError> {
        let mut records = Vec::new();
        let segments = self.list_segments()?;

        for segment_path in &segments {
            let seg_records =
                self.read_segment(segment_path, current_schema_version, Some(after_seq))?;
            records.extend(seg_records);
        }

        Ok(records)
    }

    /// 读取单个 WAL 段文件。
    ///
    /// # 参数
    /// - `path`: 段文件路径。
    /// - `current_schema_version`: 当前 Schema 版本号。
    /// - `after_seq`: 剪枝水位（`Some(w)` 时跳过 `raft_index <= w` 的帧；`None` 表示不剪枝）。
    fn read_segment(
        &self,
        path: &Path,
        current_schema_version: u32,
        after_seq: Option<u64>,
    ) -> Result<Vec<WalRecord>, SenseArcError> {
        let mut records = Vec::new();
        let mut seg_reader = WalSegmentReader::new();

        let mut file =
            File::open(path).map_err(|e| SenseArcError::WalIoError(format!("open: {}", e)))?;

        let file_len = file
            .metadata()
            .map_err(|e| SenseArcError::WalIoError(format!("metadata: {}", e)))?
            .len();

        let segment_seq = parse_segment_seq(path);

        let mut offset: u64 = 0;

        while offset + WAL_FRAME_HEADER_SIZE_V2 as u64 <= file_len {
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| SenseArcError::WalIoError(format!("seek: {}", e)))?;

            // 读取帧头
            let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE_V2];
            match file.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(_) => break,
            }

            let header = match WalFrameHeaderV2::from_bytes(&header_buf) {
                Some(h) => h,
                None => {
                    // 非 WAL2 数据或损坏：停止读取
                    break;
                }
            };

            let payload_len = header.data_len as u64;

            if offset + WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len > file_len {
                // 帧不完整
                break;
            }

            // 剪枝：`raft_index <= after_seq` 的帧数据已完整落在 SAC/snapshot，跳过
            // CRC/解密/解压/反序列化（只读帧头 + 前进 offset）。判定粒度与现状
            // 「解码后按 rec.raft_index <= after_seq 整帧丢弃」一致（多 point 帧共享
            // 帧头 raft_index），语义零变化。
            if let Some(after_seq) = after_seq {
                if header.raft_index <= after_seq {
                    offset += WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len;
                    continue;
                }
            }

            // 读取 payload
            let mut payload = vec![0u8; payload_len as usize];
            file.read_exact(&mut payload)
                .map_err(|e| SenseArcError::WalIoError(format!("read payload: {}", e)))?;

            // CRC 校验
            if !header.verify_crc(&payload) {
                warn!(
                    segment = segment_seq,
                    offset = offset,
                    "WAL frame CRC mismatch, skipping"
                );
                offset += WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len;
                continue;
            }

            // Schema version 校验：只 skip + log，不 return Err
            if header.schema_version != current_schema_version {
                seg_reader.handle_schema_mismatch(header.schema_version, current_schema_version);
                // 仍然解码数据（尽力恢复）
            }

            // 解密（WALE 前缀自描述）或明文帧
            let decompressed = if payload.len() >= WAL_ENC_PREFIX_LEN
                && payload[0..4] == *WAL_ENC_MAGIC
            {
                // 密文帧：无密钥必须硬报错，拒绝把密文当明文返回（O-1 硬约束）。
                let encryptor = match &self.encryptor {
                    Some(e) => e,
                    None => return Err(EncryptError::NoEncryptor.into()),
                };
                let tenant_id = TenantId(u32::from_le_bytes(
                    payload[4..8].try_into().unwrap_or([0u8; 4]),
                ));
                let dek_version = u32::from_le_bytes(payload[8..12].try_into().unwrap_or([0u8; 4]));
                let ctx = EncryptContext::wal_frame(tenant_id, segment_seq, offset, dek_version);
                let blob = &payload[WAL_ENC_PREFIX_LEN..];
                match encryptor.decrypt(blob, &ctx) {
                    Ok(plain) => decompress_lz4(&plain),
                    Err(e) => {
                        // 解密失败（GCM 认证失败）按尽力恢复：warn + 跳过该帧。
                        warn!(
                            segment = segment_seq,
                            offset = offset,
                            error = %e,
                            "WAL frame decrypt failed, skipping"
                        );
                        offset += WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len;
                        continue;
                    }
                }
            } else {
                decompress_lz4(&payload)
            };
            let points = deserialize_points(&decompressed);

            records.push(WalRecord {
                points,
                segment_seq,
                frame_offset: offset,
                schema_version: header.schema_version,
                raft_term: header.raft_term,
                raft_index: header.raft_index,
            });

            offset += WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len;
        }

        // 重置计数器（段内状态，切换段时复位）
        seg_reader.reset();

        Ok(records)
    }

    /// 读取 `raft_index > after_lsn` 的原始 WAL 帧（不解码 DataPoint）。
    ///
    /// 与 [`WalReader::replay_all`] 不同，本方法返回**原始明文 payload**（LZ4 解压 +
    /// TDE 解密后、但仍为 DataPoint 序列化字节），供边缘同步把 WAL 增量打包为
    /// `SyncBatch` 直接转发。`after_lsn` 语义与单机 WAL 的 `raft_index`（自增 seq）
    /// 对齐，用于「从 checkpoint 水位之后」的增量读取边界。
    ///
    /// # 参数
    /// - `after_lsn`: 只返回 `raft_index > after_lsn` 的帧（0 表示全部）。
    pub fn read_raw_frames_after(
        &self,
        after_lsn: u64,
    ) -> Result<Vec<DecodedWalFrame>, SenseArcError> {
        let mut frames = Vec::new();
        let segments = self.list_segments()?;

        for segment_path in &segments {
            let seg_frames = self.read_segment_raw(segment_path, after_lsn)?;
            frames.extend(seg_frames);
        }

        Ok(frames)
    }

    /// 读取单个 WAL 段文件的原始帧，跳过 `raft_index <= after_lsn` 的帧。
    fn read_segment_raw(
        &self,
        path: &Path,
        after_lsn: u64,
    ) -> Result<Vec<DecodedWalFrame>, SenseArcError> {
        let mut frames = Vec::new();

        let mut file =
            File::open(path).map_err(|e| SenseArcError::WalIoError(format!("open: {}", e)))?;

        let file_len = file
            .metadata()
            .map_err(|e| SenseArcError::WalIoError(format!("metadata: {}", e)))?
            .len();

        let segment_seq = parse_segment_seq(path);

        let mut offset: u64 = 0;

        while offset + WAL_FRAME_HEADER_SIZE_V2 as u64 <= file_len {
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| SenseArcError::WalIoError(format!("seek: {}", e)))?;

            // 读取帧头
            let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE_V2];
            match file.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(_) => break,
            }

            let header = match WalFrameHeaderV2::from_bytes(&header_buf) {
                Some(h) => h,
                None => break,
            };

            let payload_len = header.data_len as u64;

            if offset + WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len > file_len {
                break;
            }

            // 读取 payload
            let mut payload = vec![0u8; payload_len as usize];
            file.read_exact(&mut payload)
                .map_err(|e| SenseArcError::WalIoError(format!("read payload: {}", e)))?;

            // CRC 校验
            if !header.verify_crc(&payload) {
                warn!(
                    segment = segment_seq,
                    offset = offset,
                    "WAL frame CRC mismatch, skipping"
                );
                offset += WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len;
                continue;
            }

            // 解密（WALE 前缀自描述）或明文帧
            let decompressed = if payload.len() >= WAL_ENC_PREFIX_LEN
                && payload[0..4] == *WAL_ENC_MAGIC
            {
                let encryptor = match &self.encryptor {
                    Some(e) => e,
                    None => return Err(EncryptError::NoEncryptor.into()),
                };
                let tenant_id = TenantId(u32::from_le_bytes(
                    payload[4..8].try_into().unwrap_or([0u8; 4]),
                ));
                let dek_version = u32::from_le_bytes(payload[8..12].try_into().unwrap_or([0u8; 4]));
                let ctx = EncryptContext::wal_frame(tenant_id, segment_seq, offset, dek_version);
                let blob = &payload[WAL_ENC_PREFIX_LEN..];
                match encryptor.decrypt(blob, &ctx) {
                    Ok(plain) => decompress_lz4(&plain),
                    Err(e) => {
                        warn!(
                            segment = segment_seq,
                            offset = offset,
                            error = %e,
                            "WAL frame decrypt failed, skipping"
                        );
                        offset += WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len;
                        continue;
                    }
                }
            } else {
                decompress_lz4(&payload)
            };

            // 增量边界：跳过 checkpoint 水位（raft_index）之前的帧
            if header.raft_index > after_lsn {
                frames.push(DecodedWalFrame {
                    segment_seq,
                    frame_offset: offset,
                    raft_term: header.raft_term,
                    raft_index: header.raft_index,
                    schema_version: header.schema_version,
                    payload: decompressed,
                });
            }

            offset += WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len;
        }

        Ok(frames)
    }
}

/// 从文件路径解析段序号。
fn parse_segment_seq(path: &Path) -> u64 {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::compress_lz4;
    use crate::wal::format::WalFrameFlag;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_read_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let reader = WalReader::new(tmp.path());
        let segments = reader.list_segments().unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn test_read_wal_segment() {
        let tmp = TempDir::new().unwrap();

        // 创建测试 WAL 段文件（WAL2 帧）
        let seg_path = tmp.path().join("00000001.wal");
        let payload = serialize_test_point(42, 100);
        let compressed = compress_lz4(&payload);

        let mut header = WalFrameHeaderV2::new(
            WalFrameFlag::Data,
            1,
            0, // raft_term（单机 0）
            1, // raft_index（seq）
            compressed.len() as u32,
            1, // point_count（单点帧）
            0,
        );
        let hdr_bytes = header.to_bytes();
        let mut prefix = [0u8; 33];
        prefix.copy_from_slice(&hdr_bytes[..33]);
        let crc = crate::wal::format::compute_wal_crc(&prefix, &compressed);
        header.crc32c = crc;

        let mut file = File::create(&seg_path).unwrap();
        file.write_all(&header.to_bytes()).unwrap();
        file.write_all(&compressed).unwrap();
        file.sync_all().unwrap();

        let reader = WalReader::new(tmp.path());
        let records = reader.replay_all(1).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].points.len(), 1);
        assert_eq!(records[0].points[0].table_id, 42);
        assert_eq!(records[0].raft_index, 1);
    }

    /// 写一个单点 WAL2 帧（正确 CRC），供 `replay_after` 剪枝回归测试使用。
    fn write_test_frame(dir: &std::path::Path, segment_seq: u64, raft_index: u64, table_id: u32) {
        let seg_path = dir.join(format!("{segment_seq:08}.wal"));
        let payload = serialize_test_point(table_id, raft_index as i64);
        let compressed = compress_lz4(&payload);

        let mut header = WalFrameHeaderV2::new(
            WalFrameFlag::Data,
            1,
            0, // raft_term（单机 0）
            raft_index,
            compressed.len() as u32,
            1, // point_count（单点帧）
            0,
        );
        let hdr_bytes = header.to_bytes();
        let mut prefix = [0u8; 33];
        prefix.copy_from_slice(&hdr_bytes[..33]);
        header.crc32c = crate::wal::format::compute_wal_crc(&prefix, &compressed);

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&seg_path)
            .unwrap();
        file.write_all(&header.to_bytes()).unwrap();
        file.write_all(&compressed).unwrap();
        file.flush().unwrap();
    }

    /// `replay_after(after_seq)` 与「`replay_all` + 事后按 `raft_index > after_seq` 过滤」
    /// 的结果**逐条一致**（帧顺序、seq、offset、points 全一致），验证剪枝语义零变化。
    #[test]
    fn test_replay_after_matches_replay_all_filter() {
        let tmp = TempDir::new().unwrap();

        // 段 0：raft_index 1 / 5 / 10；段 1：raft_index 20 / 30（跨段验证）。
        write_test_frame(tmp.path(), 0, 1, 101);
        write_test_frame(tmp.path(), 0, 5, 105);
        write_test_frame(tmp.path(), 0, 10, 110);
        write_test_frame(tmp.path(), 1, 20, 120);
        write_test_frame(tmp.path(), 1, 30, 130);

        let reader = WalReader::new(tmp.path());
        let all = reader.replay_all(1).unwrap();
        assert_eq!(all.len(), 5);

        for after in [0u64, 1, 5, 10, 20, 30, 40] {
            let filtered: Vec<WalRecord> = all
                .iter()
                .filter(|r| r.raft_index > after)
                .cloned()
                .collect();
            let pruned = reader.replay_after(after, 1).unwrap();
            assert_eq!(
                pruned.len(),
                filtered.len(),
                "after={after} 帧数不一致"
            );
            for (a, b) in pruned.iter().zip(filtered.iter()) {
                assert_eq!(a.raft_index, b.raft_index, "after={after} raft_index 不一致");
                assert_eq!(a.segment_seq, b.segment_seq, "after={after} segment 不一致");
                assert_eq!(a.frame_offset, b.frame_offset, "after={after} offset 不一致");
                assert_eq!(a.points.len(), b.points.len(), "after={after} 点数不一致");
                for (pa, pb) in a.points.iter().zip(b.points.iter()) {
                    assert_eq!(pa.table_id, pb.table_id);
                    assert_eq!(pa.ts, pb.ts);
                }
            }
        }
    }
}

/// 辅助：创建测试用的序列化点数据。
fn serialize_test_point(table_id: u32, ts: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u32.to_le_bytes()); // count = 1
    buf.extend_from_slice(&table_id.to_le_bytes());
    buf.extend_from_slice(&ts.to_le_bytes());
    buf.extend_from_slice(&ts.to_le_bytes()); // server_ts = ts
    buf.extend_from_slice(&1u32.to_le_bytes()); // schema_version = 1
    buf.push(0u8); // flags = 0
    buf.extend_from_slice(&1u16.to_le_bytes()); // 1 value
    buf.extend_from_slice(&0u16.to_le_bytes()); // col_id = 0
    buf.push(0u8); // tag = F64 (0)
    buf.extend_from_slice(&42.0f64.to_le_bytes()); // val = 42.0
    buf.extend_from_slice(&1u16.to_le_bytes()); // 1 quality
    buf.extend_from_slice(&0u16.to_le_bytes()); // col_id = 0
    buf.push(0xC0u8); // GOOD
    buf
}

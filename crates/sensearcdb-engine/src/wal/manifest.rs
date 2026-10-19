//! WAL 段 → 数据 seq 索引 manifest（C1）。
//!
//! 段文件名 seq 与帧内数据 seq（`raft_index`）是两套独立序号（见
//! `docs/wal-segment-recycling-research.md` §F5）。删除旧段必须按「段内最大
//! 数据 seq ≤ 回收水位」判断，因此需要一份持久化的「段 → 数据 seq」映射。
//!
//! 本模块在段 rollover 时记录 `{segment_seq, first_data_seq, last_data_seq}`，
//! 并原子写盘（`.tmp` → `fsync` → `rename` → 目录 `fsync`），是回收器（C3）
//! 判断「哪些段可安全删除」的唯一依据。
//!
//! ## 不变式 I4（两套 seq 不混淆）
//!
//! manifest 里的 `first_data_seq` / `last_data_seq` 恒为**数据 seq**
//! （帧内 `raft_index`），绝不混用段文件名 seq。回收器只读 `last_data_seq`。

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use sensearcdb_common::SenseArcError;

/// manifest 文件名。
pub const WAL_MANIFEST_FILE: &str = "manifest.bin";

/// manifest 文件 magic: "WALM"。
const MANIFEST_MAGIC: &[u8; 4] = b"WALM";

/// manifest 文件格式版本号。
const MANIFEST_VERSION: u32 = 1;

/// 单条记录字节数：segment_seq(u64) + first_data_seq(u64) + last_data_seq(u64)。
const RECORD_SIZE: usize = 24;

/// 文件头字节数：magic(4) + version(4) + count(4)。
const HEADER_SIZE: usize = 12;

/// 单个 WAL 段的数据 seq 映射。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentMeta {
    /// 段文件名 seq（`{seq:08}.wal` 中的 seq）。
    pub segment_seq: u64,
    /// 段内最小数据 seq（帧 `raft_index`）。
    pub first_data_seq: u64,
    /// 段内最大数据 seq（帧 `raft_index`）。
    pub last_data_seq: u64,
}

/// WAL 段 → 数据 seq 索引（持久化到 `wal/manifest.bin`）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalManifest {
    /// 按段 seq 升序排列的记录。
    pub entries: Vec<SegmentMeta>,
}

impl WalManifest {
    /// 从 `wal_dir` 加载 manifest。
    ///
    /// 文件不存在、magic/版本不匹配或结构损坏时，**保守返回空 manifest**
    /// （等价于「没有任何段可删」，回收器绝不基于损坏的映射删段）。
    pub fn load(wal_dir: &Path) -> Result<Self, SenseArcError> {
        let path = manifest_path(wal_dir);
        if !path.exists() {
            return Ok(Self::default());
        }

        let data = fs::read(&path)
            .map_err(|e| SenseArcError::WalIoError(format!("read manifest: {e}")))?;

        if data.len() < HEADER_SIZE || &data[0..4] != MANIFEST_MAGIC {
            tracing::warn!(path = %path.display(), "WAL manifest corrupt (magic); treating as empty");
            return Ok(Self::default());
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap_or([0u8; 4]));
        if version != MANIFEST_VERSION {
            tracing::warn!(
                path = %path.display(),
                version,
                "WAL manifest version mismatch; treating as empty"
            );
            return Ok(Self::default());
        }

        let count = u32::from_le_bytes(data[8..12].try_into().unwrap_or([0u8; 4])) as usize;
        if data.len() < HEADER_SIZE + count * RECORD_SIZE {
            tracing::warn!(path = %path.display(), "WAL manifest truncated; treating as empty");
            return Ok(Self::default());
        }

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let off = HEADER_SIZE + i * RECORD_SIZE;
            let segment_seq = read_u64(&data, off);
            let first_data_seq = read_u64(&data, off + 8);
            let last_data_seq = read_u64(&data, off + 16);
            entries.push(SegmentMeta {
                segment_seq,
                first_data_seq,
                last_data_seq,
            });
        }
        // 防御性排序：保证段 seq 升序（回收器按此取「最近 N 段」）。
        entries.sort_by_key(|e| e.segment_seq);

        Ok(Self { entries })
    }

    /// 原子写盘：写 `.tmp` → `fsync` → `rename` → 父目录 `fsync`。
    ///
    /// 崩溃半写只会留下 `.tmp`，最终路径永远是完整文件。
    pub fn write_atomic(&self, wal_dir: &Path) -> Result<(), SenseArcError> {
        let path = manifest_path(wal_dir);
        let tmp_path = wal_dir.join(format!("{WAL_MANIFEST_FILE}.tmp"));

        let mut buf = Vec::with_capacity(HEADER_SIZE + self.entries.len() * RECORD_SIZE);
        buf.extend_from_slice(MANIFEST_MAGIC);
        buf.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            buf.extend_from_slice(&e.segment_seq.to_le_bytes());
            buf.extend_from_slice(&e.first_data_seq.to_le_bytes());
            buf.extend_from_slice(&e.last_data_seq.to_le_bytes());
        }

        let write_result: Result<(), SenseArcError> = (|| {
            let mut file = fs::File::create(&tmp_path)
                .map_err(|e| SenseArcError::WalIoError(format!("create manifest tmp: {e}")))?;
            file.write_all(&buf)
                .map_err(|e| SenseArcError::WalIoError(format!("write manifest: {e}")))?;
            file.sync_all()
                .map_err(|e| SenseArcError::WalIoError(format!("fsync manifest: {e}")))?;
            drop(file);

            fs::rename(&tmp_path, &path)
                .map_err(|e| SenseArcError::WalIoError(format!("rename manifest: {e}")))?;
            fsync_dir(wal_dir)?;
            Ok(())
        })();

        if write_result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        write_result
    }

    /// 追加一条段记录（调用方负责随后 `write_atomic`）。
    pub fn push(&mut self, meta: SegmentMeta) {
        // 覆盖重复段（防御：同一段只保留最新一条）。
        if let Some(existing) = self
            .entries
            .iter_mut()
            .find(|e| e.segment_seq == meta.segment_seq)
        {
            *existing = meta;
        } else {
            self.entries.push(meta);
        }
        self.entries.sort_by_key(|e| e.segment_seq);
    }
}

/// 计算 manifest 文件路径。
fn manifest_path(wal_dir: &Path) -> PathBuf {
    wal_dir.join(WAL_MANIFEST_FILE)
}

/// 从字节切片偏移处读取小端 u64（越界返回 0，仅用于已校验长度的内部数据）。
fn read_u64(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap_or([0u8; 8]))
}

/// 对目录做一次 `sync_all`（best-effort；Linux rename 持久化要求）。
fn fsync_dir(dir: &Path) -> Result<(), SenseArcError> {
    if let Ok(f) = fs::File::open(dir) {
        let _ = f.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn meta(seq: u64, first: u64, last: u64) -> SegmentMeta {
        SegmentMeta {
            segment_seq: seq,
            first_data_seq: first,
            last_data_seq: last,
        }
    }

    #[test]
    fn test_manifest_round_trip() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let mut m = WalManifest::default();
        m.push(meta(0, 1, 100));
        m.push(meta(1, 101, 200));
        m.write_atomic(&wal_dir).unwrap();

        let loaded = WalManifest::load(&wal_dir).unwrap();
        assert_eq!(loaded.entries.len(), 2);
        assert_eq!(loaded.entries[0], meta(0, 1, 100));
        assert_eq!(loaded.entries[1], meta(1, 101, 200));
    }

    #[test]
    fn test_manifest_missing_is_empty() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        let m = WalManifest::load(&wal_dir).unwrap();
        assert!(m.entries.is_empty());
    }

    #[test]
    fn test_manifest_corrupt_is_empty() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        fs::write(wal_dir.join(WAL_MANIFEST_FILE), b"garbage-not-a-manifest").unwrap();

        let m = WalManifest::load(&wal_dir).unwrap();
        assert!(
            m.entries.is_empty(),
            "corrupt manifest must be treated as empty"
        );
    }

    #[test]
    fn test_push_dedups_same_segment() {
        let mut m = WalManifest::default();
        m.push(meta(3, 10, 20));
        m.push(meta(3, 15, 25));
        assert_eq!(m.entries.len(), 1, "同段重复 push 应覆盖而非重复");
        assert_eq!(m.entries[0], meta(3, 15, 25));
    }

    #[test]
    fn test_atomic_write_no_tmp_residue() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        let m = WalManifest::default();
        m.write_atomic(&wal_dir).unwrap();
        assert!(wal_dir.join(WAL_MANIFEST_FILE).exists());
        assert!(!wal_dir.join(format!("{WAL_MANIFEST_FILE}.tmp")).exists());
    }
}

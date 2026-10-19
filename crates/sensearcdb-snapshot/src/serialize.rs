//! snapshot.bin 二进制序列化/反序列化。
//!
//! ## 硬约束
//!
//! ⚠️ **禁止序列化 `HashMap` / `HashSet`** — 序列化格式中必须使用 `Vec`（确定性顺序）。
//!
//! `HashMap` 的迭代顺序由 `RandomState`（随机 SipHash seed）决定，
//! 同一份数据在不同进程、不同 Rust 版本下可能产生不同的字节序列。
//! `snapshot.bin` 作为崩溃恢复的确定性输入，必须保证同一逻辑状态产生完全相同的字节序列。
//!
//! ## 格式（v2）
//!
//! ```text
//! SnapshotFileHeader (固定 40B)
//!   ├── magic:     [u8; 4]   = b"SNAP"
//!   ├── version:   u32       = 2
//!   ├── shard_count: u32
//!   ├── global_version: u64
//!   ├── tde_encrypted: u8    (0=明文, 1=加密)
//!   ├── dek_version: u32     (0=明文/未加密)
//!   ├── reserved:  [u8; 7]
//!   └── wal_watermark: i64   (WAL 水位，server_ts epoch ns；0 = 无水位)
//!
//! Per-shard payload × shard_count
//!   u64 LE length prefix（明文，边界识别）
//!   + Arrow IPC File bytes（每个 shard 一个完整 IPC File，行按 (table_id, column_id) 升序）
//!     （TDE 开 → 整段 IPC 字节 SM4-GCM 加密 = nonce(12)||tag(16)||ct）
//! ```
//!
//! ## TDE（O-3）
//!
//! 文件头保持明文（magic 可见 + 自描述加密标志），**只加密每个 shard 的 payload**
//! （Arrow IPC File 字节），长度前缀保持明文以支持边界识别——与 WAL 的
//! payload-only 加密一致。

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use bincode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;
use std::time::Instant;
use tracing;

use crate::columnar::ColumnarShard;
use crate::encryption::{SnapshotEncryptError, SnapshotEncryptor};
use crate::entry::SnapshotEntry;
use crate::metrics::SnapshotMetrics;
use sensearcdb_common::SenseArcError;

/// snapshot.bin 文件 magic number: "SNAP"
pub const SNAPSHOT_MAGIC: &[u8; 4] = b"SNAP";

/// 当前文件格式版本号（v2 = Arrow IPC File payload）。
pub const SNAPSHOT_FORMAT_VERSION: u32 = 2;

/// 文件头固定大小：40 字节。
///
/// 布局: magic(4) + version(4) + shard_count(4) + global_version(8)
///        + tde_encrypted(1) + dek_version(4) + reserved(7) + wal_watermark(8) = 40
pub const SNAPSHOT_HEADER_SIZE: usize = 40;

/// snapshot.bin 文件头。
///
/// 布局: magic(4) + version(4) + shard_count(4) + global_version(8)
///        + tde_encrypted(1) + dek_version(4) + reserved(7) + wal_watermark(8) = 40
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotFileHeader {
    magic: [u8; 4],
    version: u32,
    shard_count: u32,
    global_version: u64,
    /// TDE 加密标志：0 = 明文，1 = 加密（自描述，文件头保持明文）。
    tde_encrypted: u8,
    /// DEK 版本：0 = 明文/未加密。
    dek_version: u32,
    reserved: [u8; 7],
    /// WAL 水位（server_ts，epoch 纳秒）。0 = 无水位。
    wal_watermark: i64,
}

impl SnapshotFileHeader {
    fn new(shard_count: u32, global_version: u64) -> Self {
        Self {
            magic: *SNAPSHOT_MAGIC,
            version: SNAPSHOT_FORMAT_VERSION,
            shard_count,
            global_version,
            tde_encrypted: 0,
            dek_version: 0,
            reserved: [0u8; 7],
            wal_watermark: 0,
        }
    }

    /// 设置 TDE 加密标志与 DEK 版本（链式）。
    fn with_tde(mut self, dek_version: u32) -> Self {
        self.tde_encrypted = 1;
        self.dek_version = dek_version;
        self
    }

    /// 设置 WAL 水位（server_ts，epoch 纳秒）。
    fn with_watermark(mut self, wal_watermark: i64) -> Self {
        self.wal_watermark = wal_watermark;
        self
    }

    /// 该快照是否已加密。
    fn is_encrypted(&self) -> bool {
        self.tde_encrypted != 0
    }

    fn validate(&self) -> Result<(), SerializeError> {
        if self.magic != *SNAPSHOT_MAGIC {
            return Err(SerializeError::InvalidMagic);
        }
        if self.version != SNAPSHOT_FORMAT_VERSION {
            return Err(SerializeError::UnsupportedVersion(self.version));
        }
        Ok(())
    }
}

/// 序列化/反序列化错误。
#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    #[error("invalid magic number")]
    InvalidMagic,
    #[error("unsupported snapshot file version: {0}")]
    UnsupportedVersion(u32),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("encryption error: {0}")]
    Encrypt(#[from] SnapshotEncryptError),
    #[error("checkpoint task failed: {0}")]
    CheckpointJoin(String),
}

impl SerializeError {
    /// 是否为「密文快照但无密钥」错误（用于恢复路径硬报错、禁止回退）。
    pub fn is_no_encryptor(&self) -> bool {
        matches!(
            self,
            SerializeError::Encrypt(SnapshotEncryptError::NoEncryptor)
        )
    }
}

/// 序列化错误 → 统一错误收敛（内部基础设施错误）。
impl From<SerializeError> for SenseArcError {
    fn from(e: SerializeError) -> Self {
        SenseArcError::Internal(e.to_string())
    }
}

/// 将 SnapshotStore 的所有分片序列化到 `snapshot.bin` 文件。
///
/// ## 确定性保证
///
/// 每个分片的条目在写入前按 `table_id` 升序排序，确保不同进程/版本
/// 对相同数据产生完全相同的字节序列。
///
/// # Arguments
///
/// - `path`: 目标文件路径。
/// - `shards`: 所有分片的只读引用（需已获取读锁）。
/// - `global_version`: 当前全局版本号。
/// - `metrics`: 指标收集器。
/// - `encryptor`: 可选加密器（`None` = TDE 关闭，明文快照）。
/// - `wal_watermark`: WAL 水位（server_ts，epoch ns），0 = 无水位。
pub fn serialize_to_file(
    path: &Path,
    shards: &[parking_lot::RwLockReadGuard<'_, ColumnarShard>],
    global_version: u64,
    metrics: &SnapshotMetrics,
    encryptor: Option<&dyn SnapshotEncryptor>,
    wal_watermark: i64,
) -> Result<(), SerializeError> {
    let start = Instant::now();

    // 原子写（PRD SNAP-08）：写 `snapshot.bin.tmp` → sync_all → rename → 父目录 fsync。
    // 崩溃半写只会留下 .tmp，最终文件路径永远完整。
    let tmp_path = tmp_sibling_path(path);

    // 确保 `snapshot.bin.tmp` 与最终 `snapshot.bin` 的父目录存在（幂等）。
    // 生产环境若 `data/snapshot/` 被误删，`File::create` 会因目录缺失报
    // `os error 3`（系统找不到指定的路径），导致 snapshot 持续静默落盘失败。
    // 这里提前 `create_dir_all` 自动重建，失败则上抛为 `SerializeError`，绝不吞错。
    ensure_parent_dir(path)?;
    ensure_parent_dir(&tmp_path)?;

    let write_result: Result<(), SerializeError> = (|| {
        let file = fs::File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);

        let (tde_enabled, dek_version) = match encryptor {
            Some(e) if e.is_enabled() => (true, e.active_dek_version()),
            _ => (false, 0),
        };
        let mut header = SnapshotFileHeader::new(shards.len() as u32, global_version)
            .with_watermark(wal_watermark);
        if tde_enabled {
            header = header.with_tde(dek_version);
        }
        let header_bytes = bincode::serialize(&header)?;
        debug_assert_eq!(header_bytes.len(), SNAPSHOT_HEADER_SIZE);
        writer.write_all(&header_bytes)?;

        for shard_guard in shards {
            // 列式 → Arrow IPC File（每 shard 一个完整 IPC File）。
            // to_record_batch 内部已按 (table_id, column_id) 升序确定性排序。
            let batch = shard_guard.to_record_batch();
            let mut cursor = Cursor::new(Vec::new());
            {
                let mut ipc = FileWriter::try_new(&mut cursor, batch.schema().as_ref())?;
                ipc.write(&batch)?;
                ipc.finish()?;
            }
            let payload_bytes = cursor.into_inner();

            // TDE：只加密 payload，长度前缀保持明文以支持边界识别（O-3）。
            let stored: Vec<u8> = if tde_enabled {
                encryptor
                    .expect("encryptor checked above")
                    .encrypt(&payload_bytes)?
            } else {
                payload_bytes
            };

            // 写入长度前缀（u64 LE）以支持反序列化时边界识别
            writer.write_all(&(stored.len() as u64).to_le_bytes())?;
            writer.write_all(&stored)?;
        }

        writer.flush()?;
        writer.get_ref().sync_all()?;

        // 原子 rename 到最终路径（覆盖已存在的旧文件）。
        fs::rename(&tmp_path, path)?;

        // 父目录 fsync：Linux rename 持久化要求（幂等，失败不致命）。
        if let Some(parent) = path.parent() {
            if let Ok(dir) = fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    })();

    // 失败时清理残留的 .tmp（成功时已被 rename 移走，无残留）。
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result?;

    let elapsed = start.elapsed().as_secs_f64();
    metrics
        .serialize_duration
        .with_label_values(&[])
        .observe(elapsed);

    tracing::info!(
        path = %path.display(),
        shard_count = shards.len(),
        global_version = global_version,
        wal_watermark = wal_watermark,
        tde_encrypted = tde_enabled_or_false(encryptor),
        duration_secs = elapsed,
        "snapshot.bin serialized"
    );

    Ok(())
}

/// 是否为 TDE 加密写（仅用于日志，避免在 closure 外保留 encryptor 借用）。
fn tde_enabled_or_false(encryptor: Option<&dyn SnapshotEncryptor>) -> bool {
    encryptor.map(|e| e.is_enabled()).unwrap_or(false)
}

/// 计算 `.tmp` 同目录临时文件路径（`snapshot.bin` → `snapshot.bin.tmp`）。
fn tmp_sibling_path(path: &Path) -> std::path::PathBuf {
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "snapshot.bin".to_string());
    path.with_file_name(format!("{}.tmp", file_name))
}

/// 确保 `file_path` 的父目录存在（幂等）。
///
/// - 父目录缺失时用 `std::fs::create_dir_all` 递归重建；
/// - 父目录已存在时 `create_dir_all` 幂等返回 `Ok`；
/// - 裸文件名（无父目录）跳过，避免对空路径调用 `create_dir_all`；
/// - 创建失败时上抛为 `SerializeError::Io`，绝不吞错。
fn ensure_parent_dir(file_path: &Path) -> Result<(), SerializeError> {
    let parent = match file_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => return Ok(()),
    };
    fs::create_dir_all(parent)?;
    Ok(())
}

/// 从 `snapshot.bin` 反序列化到内存分片数组。
///
/// 返回 `(shards, global_version, wal_watermark)`：
/// - `shards`: 重建的分片数组。
/// - `global_version`: 文件中的全局版本号（用于确定 WAL 重放起点）。
/// - `wal_watermark`: 文件中的 WAL 水位（server_ts，epoch ns）。
///
/// # Arguments
///
/// - `path`: snapshot.bin 文件路径。
/// - `shard_count`: 期望的分片数量（从配置读取，必须与文件中的一致）。
/// - `metrics`: 指标收集器。
/// - `encryptor`: 可选加密器（`None` = TDE 关闭；密文快照无密钥时硬报错）。
pub fn deserialize_from_file(
    path: &Path,
    shard_count: usize,
    metrics: &SnapshotMetrics,
    encryptor: Option<&dyn SnapshotEncryptor>,
) -> Result<(Vec<ColumnarShard>, u64, i64), SerializeError> {
    let start = Instant::now();

    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    // 读取固定 40B 文件头。
    let mut header_buf = [0u8; SNAPSHOT_HEADER_SIZE];
    reader.read_exact(&mut header_buf)?;
    let header: SnapshotFileHeader = bincode::deserialize(&header_buf)?;
    header.validate()?;

    // TDE：密文快照无密钥必须硬报错，绝不把密文当明文解析（O-3 硬约束）。
    if header.is_encrypted() && encryptor.is_none() {
        return Err(SerializeError::Encrypt(SnapshotEncryptError::NoEncryptor));
    }

    if header.shard_count as usize != shard_count {
        tracing::info!(
            file_shard_count = header.shard_count,
            config_shard_count = shard_count,
            "snapshot.bin shard_count mismatch; re-hashing entries into configured shard_count"
        );
    }

    let mut file_shards: Vec<ColumnarShard> = Vec::with_capacity(header.shard_count as usize);

    for _ in 0..header.shard_count as usize {
        // 读取长度前缀
        let mut len_buf = [0u8; 8];
        reader.read_exact(&mut len_buf)?;
        let payload_len = u64::from_le_bytes(len_buf) as usize;

        // 读取载荷（密文或明文）
        let mut payload_buf = vec![0u8; payload_len];
        reader.read_exact(&mut payload_buf)?;

        // TDE：解密 payload 后再 Arrow IPC 反序列化
        let plain: Vec<u8> = if header.is_encrypted() {
            encryptor
                .expect("encryptor checked above")
                .decrypt(&payload_buf, header.dek_version)?
        } else {
            payload_buf
        };
        let reader = FileReader::try_new(Cursor::new(plain), None)?;
        let mut shard = ColumnarShard::new();
        if let Some(Ok(batch)) = reader.into_iter().next() {
            shard = ColumnarShard::from_record_batch(&batch);
        }
        file_shards.push(shard);
    }

    // 重新按进程配置的 shard_count 分桶（shard_multiplier 变更后快照可移植）：
    // snapshot.bin 的 shard_count 可能与本进程配置不同（升级/降档桶数），必须逐条
    // 按 `hash(table_id) % shard_count` 重放条目，否则条目会滞留在旧桶索引，导致
    // `get()` 用新桶索引查不到（数据丢失）。红绳不变式：同一 table_id 永远映射到同一桶。
    let mut shards: Vec<ColumnarShard> = (0..shard_count).map(|_| ColumnarShard::new()).collect();
    for file_shard in &file_shards {
        for tid in file_shard.iter_table_ids() {
            if let Some(entry) = file_shard.project_entry(tid) {
                let idx = fxhash::hash(&tid) as usize % shard_count;
                shards[idx].upsert_from_entry(tid, entry);
            }
        }
    }
    for bucket in &mut shards {
        // shard_version 仅作持久化信息；重分桶后取桶内最大 entry.version 作为近似。
        bucket.shard_version = bucket
            .iter_table_ids()
            .into_iter()
            .filter_map(|tid| bucket.project_entry(tid))
            .map(|e| e.version)
            .max()
            .unwrap_or(0);
    }

    let elapsed = start.elapsed().as_secs_f64();
    metrics
        .deserialize_duration
        .with_label_values(&[])
        .observe(elapsed);

    let total_entries: usize = shards.iter().map(|s| s.entry_count()).sum();
    tracing::info!(
        path = %path.display(),
        shard_count = shards.len(),
        total_entries = total_entries,
        global_version = header.global_version,
        wal_watermark = header.wal_watermark,
        tde_encrypted = header.is_encrypted(),
        duration_secs = elapsed,
        "snapshot.bin deserialized"
    );

    Ok((shards, header.global_version, header.wal_watermark))
}

/// 只读 `snapshot.bin` 文件头的 `wal_watermark`（数据 seq），不反序列化分片。
///
/// 供 WAL 旧段回收器（C3）计算回收水位 `R = min(snapshot.bin wal_watermark, S)`。
/// 文件不存在、头损坏、magic/version 不匹配或读取失败时，**保守返回 `0`**
/// （等价「无水位」→ `R=0` → 回收器不删任何段），绝不基于不可信水位删段（I3）。
pub fn read_snapshot_watermark(path: &Path) -> i64 {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return 0,
    };

    let mut reader = BufReader::new(file);
    let mut header_buf = [0u8; SNAPSHOT_HEADER_SIZE];
    if reader.read_exact(&mut header_buf).is_err() {
        return 0;
    }

    match bincode::deserialize::<SnapshotFileHeader>(&header_buf) {
        Ok(header) if header.validate().is_ok() => header.wal_watermark,
        _ => 0,
    }
}

/// 从内存分片构建序列化字节（用于测试和 round-trip 验证）。
pub fn serialize_to_bytes(
    shards: &HashMap<u32, SnapshotEntry>,
    global_version: u64,
) -> Result<Vec<u8>, SerializeError> {
    let header = SnapshotFileHeader::new(1, global_version);
    let header_bytes = bincode::serialize(&header)?;

    // 构造临时单 shard（shard_count=1），按 table_id 升序 upsert 后导 IPC。
    let mut shard = ColumnarShard::new();
    let mut sorted_entries: Vec<(u32, SnapshotEntry)> =
        shards.iter().map(|(k, v)| (*k, v.clone())).collect();
    sorted_entries.sort_by_key(|(tid, _)| *tid);
    for (tid, entry) in sorted_entries {
        shard.upsert_from_entry(tid, entry);
    }
    shard.shard_version = global_version;

    let batch = shard.to_record_batch();
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut ipc = FileWriter::try_new(&mut cursor, batch.schema().as_ref())?;
        ipc.write(&batch)?;
        ipc.finish()?;
    }
    let payload_bytes = cursor.into_inner();

    let mut result = Vec::with_capacity(header_bytes.len() + 8 + payload_bytes.len());
    result.extend_from_slice(&header_bytes);
    result.extend_from_slice(&(payload_bytes.len() as u64).to_le_bytes());
    result.extend_from_slice(&payload_bytes);

    Ok(result)
}

/// 从字节反序列化为内存条目映射。
pub fn deserialize_from_bytes(
    data: &[u8],
) -> Result<(HashMap<u32, SnapshotEntry>, u64), SerializeError> {
    let header: SnapshotFileHeader = bincode::deserialize(&data[..SNAPSHOT_HEADER_SIZE])?;
    header.validate()?;

    let len_start = SNAPSHOT_HEADER_SIZE;
    let payload_len =
        u64::from_le_bytes(data[len_start..len_start + 8].try_into().unwrap()) as usize;

    let payload_start = len_start + 8;
    let reader = FileReader::try_new(
        Cursor::new(&data[payload_start..payload_start + payload_len]),
        None,
    )?;
    let mut shard = ColumnarShard::new();
    if let Some(Ok(batch)) = reader.into_iter().next() {
        shard = ColumnarShard::from_record_batch(&batch);
    }

    let mut entries = HashMap::new();
    for tid in shard.iter_table_ids() {
        if let Some(entry) = shard.project_entry(tid) {
            entries.insert(tid, entry);
        }
    }

    Ok((entries, header.global_version))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::{SnapshotEncryptError, SnapshotEncryptor};
    use crate::entry::SnapshotEntry;
    use sensearcdb_common::TypedValue;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn make_test_entry(value: f64) -> SnapshotEntry {
        SnapshotEntry::new_typed(
            sensearcdb_common::TenantId::DEFAULT,
            vec![(0, TypedValue::F64(value))],
            vec![(0, 0xC0)],
            1000,
            1001,
            1,
            false,
            None,
        )
    }

    #[test]
    fn test_round_trip_bytes() {
        let mut entries = HashMap::new();
        entries.insert(1u32, make_test_entry(42.5));
        entries.insert(2u32, make_test_entry(-3.14));
        entries.insert(3u32, make_test_entry(0.0));

        let data = serialize_to_bytes(&entries, 7).unwrap();
        let (restored, version) = deserialize_from_bytes(&data).unwrap();

        assert_eq!(version, 7);
        assert_eq!(restored.len(), 3);
        assert!((restored.get(&1).unwrap().value() - 42.5).abs() < 1e-12);
        assert!((restored.get(&2).unwrap().value() - (-3.14)).abs() < 1e-12);
        assert_eq!(restored.get(&3).unwrap().value(), 0.0);
    }

    #[test]
    fn test_round_trip_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let mut shard = ColumnarShard::new();
        shard.upsert_from_entry(1, make_test_entry(100.0));
        shard.upsert_from_entry(2, make_test_entry(200.0));

        let shard_arc = parking_lot::RwLock::new(shard);
        let guard = shard_arc.read();

        let metrics = SnapshotMetrics::noop();
        serialize_to_file(&path, &[guard], 5, &metrics, None, 0).unwrap();

        let (restored_shards, version, watermark) =
            deserialize_from_file(&path, 1, &metrics, None).unwrap();

        assert_eq!(version, 5);
        assert_eq!(watermark, 0);
        assert_eq!(restored_shards.len(), 1);
        let restored = &restored_shards[0];
        assert_eq!(restored.entry_count(), 2);
        assert!((restored.get(1).unwrap().value() - 100.0).abs() < 1e-12);
        assert!((restored.get(2).unwrap().value() - 200.0).abs() < 1e-12);
    }

    /// 升级方向 re-hash：旧 snapshot.bin（4 桶，hash % 4）用更大的 shard_count（16 桶）恢复，
    /// 逐条必须重放到 `hash % 16` 桶，否则 `get()` 用新桶索引查不到 → 数据丢失。
    #[test]
    fn test_deserialize_rehashes_entries_on_shard_count_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let metrics = SnapshotMetrics::noop();

        // 旧文件：4 桶，条目按 hash % 4 放置。
        let old_locks: Vec<parking_lot::RwLock<ColumnarShard>> = (0..4)
            .map(|_| parking_lot::RwLock::new(ColumnarShard::new()))
            .collect();
        for i in 1u32..=100u32 {
            let idx = fxhash::hash(&i) as usize % 4;
            old_locks[idx]
                .write()
                .upsert_from_entry(i, make_test_entry(i as f64));
        }
        let guards: Vec<_> = old_locks.iter().map(|l| l.read()).collect();
        serialize_to_file(&path, &guards, 7, &metrics, None, 0).unwrap();
        drop(guards);

        // 升级：用 16 桶恢复，必须 re-hash 且总数不丢。
        let (restored, version, _wm) = deserialize_from_file(&path, 16, &metrics, None).unwrap();
        assert_eq!(version, 7);
        assert_eq!(restored.len(), 16);

        let mut total = 0usize;
        for (bucket_idx, bucket) in restored.iter().enumerate() {
            for tid in bucket.iter_table_ids() {
                assert_eq!(
                    fxhash::hash(&tid) as usize % 16,
                    bucket_idx,
                    "table_id {} 未重放到正确的桶（数据错位）",
                    tid
                );
            }
            total += bucket.entry_count();
        }
        assert_eq!(total, 100, "re-hash 后不得丢失条目");
    }

    #[test]
    fn test_deterministic_output() {
        let mut entries = HashMap::new();
        entries.insert(10u32, make_test_entry(1.0));
        entries.insert(5u32, make_test_entry(2.0));
        entries.insert(7u32, make_test_entry(3.0));

        let data1 = serialize_to_bytes(&entries, 1).unwrap();
        let data2 = serialize_to_bytes(&entries, 1).unwrap();

        // Deterministic: same input → same bytes
        assert_eq!(data1, data2);
    }

    #[test]
    fn test_header_magic_validation() {
        let header = SnapshotFileHeader::new(1, 0);
        assert!(header.validate().is_ok());

        let mut bad_header = header;
        bad_header.magic = *b"XXXX";
        assert!(bad_header.validate().is_err());
    }

    // ─── TDE（O-3）round-trip ───

    /// XOR 型加密桩：产出 `nonce(12)||tag(16)||ct` 自包含密文，满足 28B 开销约定。
    struct TestEncryptor {
        version: u32,
    }

    impl SnapshotEncryptor for TestEncryptor {
        fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, SnapshotEncryptError> {
            let mut out = Vec::with_capacity(28 + plaintext.len());
            out.extend_from_slice(&[0xA1u8; 12]); // nonce
            out.extend_from_slice(&[0xB2u8; 16]); // tag
            for b in plaintext {
                out.push(b ^ 0x5A);
            }
            Ok(out)
        }

        fn decrypt(&self, blob: &[u8], _dek_version: u32) -> Result<Vec<u8>, SnapshotEncryptError> {
            if blob.len() < 28 {
                return Err(SnapshotEncryptError::TooShort(blob.len(), 28));
            }
            Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
        }

        fn active_dek_version(&self) -> u32 {
            self.version
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    fn make_encryptor() -> TestEncryptor {
        TestEncryptor { version: 1 }
    }

    #[test]
    fn test_tde_encrypted_roundtrip_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let mut shard = ColumnarShard::new();
        shard.upsert_from_entry(1, make_test_entry(100.0));
        shard.upsert_from_entry(2, make_test_entry(200.0));

        let shard_arc = parking_lot::RwLock::new(shard);
        let guard = shard_arc.read();
        let metrics = SnapshotMetrics::noop();

        // 写加密快照。
        let enc = make_encryptor();
        serialize_to_file(&path, &[guard], 5, &metrics, Some(&enc), 0).unwrap();

        // 文件头 bit 语义化：magic 仍明文 "SNAP"，tde_encrypted=1 + dek_version=1。
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(&raw[0..4], b"SNAP");
        assert_eq!(raw[20], 1, "header.tde_encrypted 应为 1");
        let dek = u32::from_le_bytes(raw[21..25].try_into().unwrap());
        assert_eq!(dek, 1, "header.dek_version 应为 1");

        // 读解密。
        let (restored_shards, version, _watermark) =
            deserialize_from_file(&path, 1, &metrics, Some(&make_encryptor())).unwrap();
        assert_eq!(version, 5);
        assert_eq!(restored_shards.len(), 1);
        let restored = &restored_shards[0];
        assert_eq!(restored.entry_count(), 2);
        assert!((restored.get(1).unwrap().value() - 100.0).abs() < 1e-12);
        assert!((restored.get(2).unwrap().value() - 200.0).abs() < 1e-12);
    }

    #[test]
    fn test_tde_encrypted_no_key_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let mut shard = ColumnarShard::new();
        shard.upsert_from_entry(1, make_test_entry(42.0));
        let shard_arc = parking_lot::RwLock::new(shard);
        let guard = shard_arc.read();
        let metrics = SnapshotMetrics::noop();

        serialize_to_file(&path, &[guard], 1, &metrics, Some(&make_encryptor()), 0).unwrap();

        // 密文快照 + 无加密器 → 必须硬报错 NoEncryptor。
        let result = deserialize_from_file(&path, 1, &metrics, None);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.is_no_encryptor(), "expected NoEncryptor, got: {}", err);
    }

    #[test]
    fn test_tde_plaintext_legacy_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let mut shard = ColumnarShard::new();
        shard.upsert_from_entry(1, make_test_entry(7.5));
        let shard_arc = parking_lot::RwLock::new(shard);
        let guard = shard_arc.read();
        let metrics = SnapshotMetrics::noop();

        // 明文 legacy 快照（encryptor=None）照写照读。
        serialize_to_file(&path, &[guard], 1, &metrics, None, 0).unwrap();
        let (restored_shards, version, _watermark) =
            deserialize_from_file(&path, 1, &metrics, None).unwrap();
        assert_eq!(version, 1);
        assert!((restored_shards[0].get(1).unwrap().value() - 7.5).abs() < 1e-12);
    }

    // ─── 写路径接线新增测试 ───

    /// 写→读 round-trip 必须保留 wal_watermark。
    #[test]
    fn test_round_trip_file_with_watermark() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let mut shard = ColumnarShard::new();
        shard.upsert_from_entry(1, make_test_entry(100.0));
        let shard_arc = parking_lot::RwLock::new(shard);
        let guard = shard_arc.read();
        let metrics = SnapshotMetrics::noop();

        let watermark: i64 = 1_700_000_000_123_456_789;
        serialize_to_file(&path, &[guard], 5, &metrics, None, watermark).unwrap();

        let (restored_shards, version, restored_watermark) =
            deserialize_from_file(&path, 1, &metrics, None).unwrap();
        assert_eq!(version, 5);
        assert_eq!(restored_watermark, watermark);
        assert_eq!(restored_shards.len(), 1);
    }

    /// 原子写：写后最终路径存在、无 .tmp 残留。
    #[test]
    fn test_atomic_write_no_tmp_residue() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let mut shard = ColumnarShard::new();
        shard.upsert_from_entry(1, make_test_entry(42.0));
        let shard_arc = parking_lot::RwLock::new(shard);
        let guard = shard_arc.read();
        let metrics = SnapshotMetrics::noop();

        serialize_to_file(&path, &[guard], 1, &metrics, None, 0).unwrap();

        assert!(path.exists(), "最终文件路径必须存在");
        assert!(
            !dir.path().join("snapshot.bin.tmp").exists(),
            "原子写成功后不得残留 .tmp"
        );
    }

    /// 父目录被误删后，serialize_to_file 必须自动重建目录并成功落盘（幂等）。
    ///
    /// 覆盖生产回归：`data/snapshot/` 被清理后，后端启动（非 first_run）不再重建该目录，
    /// `snapshot.bin` 写入 `File::create` 报 `os error 3` 并静默失败。
    #[test]
    fn test_serialize_to_file_recreates_missing_parent_dir() {
        let dir = tempdir().unwrap();
        let snapshot_dir = dir.path().join("data").join("snapshot");
        let path = snapshot_dir.join("snapshot.bin");

        // 先创建再删除，模拟「目录被误删」的起点。
        std::fs::create_dir_all(&snapshot_dir).unwrap();
        std::fs::remove_dir_all(&snapshot_dir).unwrap();
        assert!(!snapshot_dir.exists(), "前置：目标目录应已删除");

        let mut shard = ColumnarShard::new();
        shard.upsert_from_entry(1, make_test_entry(100.0));
        let shard_arc = parking_lot::RwLock::new(shard);
        let metrics = SnapshotMetrics::noop();

        // 第一次：目录缺失，应自动重建并成功落盘。
        let guard = shard_arc.read();
        serialize_to_file(&path, &[guard], 5, &metrics, None, 0).unwrap();
        assert!(snapshot_dir.exists(), "第一次写入后目录应被自动重建");
        assert!(path.exists(), "第一次写入后 snapshot.bin 应落盘");
        assert!(
            !snapshot_dir.join("snapshot.bin.tmp").exists(),
            "原子写成功后不得残留 .tmp"
        );

        // 第二次：再次删除目录后写入，验证幂等（重建 + 覆盖写均不报错）。
        std::fs::remove_dir_all(&snapshot_dir).unwrap();
        assert!(!snapshot_dir.exists(), "前置：第二次写入前目录应已删除");
        let guard = shard_arc.read();
        serialize_to_file(&path, &[guard], 5, &metrics, None, 0).unwrap();
        assert!(snapshot_dir.exists(), "第二次写入后目录应再次被重建");
        assert!(path.exists(), "第二次（幂等）写入后 snapshot.bin 应存在");
    }

    /// 多列多类型 round-trip：SnapshotEntry 序列化后反序列化保真
    /// （F64/I64/String/Bool/Enum16 混合列）。
    #[test]
    fn test_round_trip_multitype_columns() {
        use sensearcdb_common::TypedValue;

        let entry = SnapshotEntry::new_typed(
            sensearcdb_common::TenantId::DEFAULT,
            vec![
                (0, TypedValue::F64(42.5)),
                (1, TypedValue::I64(-7)),
                (2, TypedValue::String("open".to_string())),
                (3, TypedValue::Bool(true)),
                (4, TypedValue::Enum16(65535)),
            ],
            vec![(0, 0xC0), (1, 0xC0), (2, 0x40), (3, 0xC0), (4, 0x00)],
            1000,
            1001,
            3,
            false,
            None,
        );

        let mut entries = HashMap::new();
        entries.insert(1u32, entry);

        let data = serialize_to_bytes(&entries, 3).unwrap();
        let (restored, _version) = deserialize_from_bytes(&data).unwrap();

        let got = restored.get(&1).unwrap();
        assert_eq!(got.values.len(), 5);
        assert_eq!(got.values[0], (0, TypedValue::F64(42.5)));
        assert_eq!(got.values[1], (1, TypedValue::I64(-7)));
        assert_eq!(got.values[2], (2, TypedValue::String("open".to_string())));
        assert_eq!(got.values[3], (3, TypedValue::Bool(true)));
        assert_eq!(got.values[4], (4, TypedValue::Enum16(65535)));
        assert_eq!(got.qualities[4], (4, 0x00));
        assert_eq!(got.value(), 42.5);
    }

    /// S4 IPC roundtrip：write_seq null bitmap 保真——`None` 经 IPC 读回仍为 `None`
    /// （非 `Some(0)`），`Some` 读回原值。这是 LWW 决胜键不断裂的红线。
    #[test]
    fn test_ipc_roundtrip_write_seq_null_preserved() {
        // 构造带逐列 write_seq 的 SnapshotEntry（struct literal，new_typed 不接收 seq）。
        let entry = SnapshotEntry {
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            values: vec![(0, TypedValue::F64(42.0)), (1, TypedValue::I64(7))],
            qualities: vec![(0, 0xC0), (1, 0xC0)],
            ts: 1000,
            server_ts: 1001,
            version: 7,
            prev_value: None,
            prev_ts: None,
            is_derived: false,
            writer_session_id: None,
            ttl_expired: false,
            ts_values: vec![(0, 1000), (1, 1000)],
            server_ts_values: vec![(0, 1001), (1, 1001)],
            write_seqs: vec![(0, None), (1, Some(7))],
        };

        let mut entries = HashMap::new();
        entries.insert(1u32, entry);

        let data = serialize_to_bytes(&entries, 7).unwrap();
        let (restored, _version) = deserialize_from_bytes(&data).unwrap();

        let got = restored.get(&1).unwrap();
        // 红线：None 不得变 Some(0)。
        assert_eq!(got.write_seqs, vec![(0, None), (1, Some(7))]);
        assert_eq!(
            got.values,
            vec![(0, TypedValue::F64(42.0)), (1, TypedValue::I64(7))]
        );
    }

    /// S4：ColumnarShard → RecordBatch → ColumnarShard 的 write_seq null bitmap 保真
    /// 与 shard_version 经 schema metadata 回读。
    #[test]
    fn test_to_from_record_batch_write_seq_null_preserved() {
        let mut shard = ColumnarShard::new();
        let entry = SnapshotEntry {
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            values: vec![(0, TypedValue::F64(42.0))],
            qualities: vec![(0, 0xC0)],
            ts: 1000,
            server_ts: 1001,
            version: 7,
            prev_value: None,
            prev_ts: None,
            is_derived: false,
            writer_session_id: Some(99),
            ttl_expired: false,
            ts_values: vec![(0, 1000)],
            server_ts_values: vec![(0, 1001)],
            write_seqs: vec![(0, None)],
        };
        shard.upsert_from_entry(1, entry);
        shard.shard_version = 5;

        let batch = shard.to_record_batch();
        let restored = ColumnarShard::from_record_batch(&batch);

        let got = restored.get(1).unwrap();
        assert_eq!(
            got.write_seqs,
            vec![(0, None)],
            "write_seq None 不得变 Some(0)"
        );
        assert_eq!(got.writer_session_id, Some(99));
        assert_eq!(restored.shard_version, 5, "shard_version 应从 schema metadata 读回");
    }

    /// F32/U64/I32 三类型 IPC roundtrip：`to_record_batch → IPC → from_record_batch`
    /// 值保真、类型不变（补齐多类型 8/8 覆盖）。
    #[test]
    fn test_ipc_roundtrip_f32_u64_i32() {
        let mut shard = ColumnarShard::new();
        let entry = SnapshotEntry::new_typed(
            sensearcdb_common::TenantId::DEFAULT,
            vec![
                (0, TypedValue::F32(1.5)),
                (1, TypedValue::U64(u64::MAX)),
                (2, TypedValue::I32(i32::MIN)),
            ],
            vec![(0, 0xC0), (1, 0xC0), (2, 0xC0)],
            1000,
            1001,
            9,
            false,
            None,
        );
        shard.upsert_from_entry(1, entry);

        // 列式 → RecordBatch → IPC File 字节。
        let batch = shard.to_record_batch();
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut ipc = FileWriter::try_new(&mut cursor, batch.schema().as_ref()).unwrap();
            ipc.write(&batch).unwrap();
            ipc.finish().unwrap();
        }
        let bytes = cursor.into_inner();

        // IPC File 字节 → RecordBatch → 列式。
        let reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
        let restored_batch = reader.into_iter().next().unwrap().unwrap();
        let restored = ColumnarShard::from_record_batch(&restored_batch);

        let got = restored.get(1).unwrap();
        assert_eq!(got.values.len(), 3);
        assert_eq!(got.values[0], (0, TypedValue::F32(1.5)));
        assert_eq!(got.values[1], (1, TypedValue::U64(u64::MAX)));
        assert_eq!(got.values[2], (2, TypedValue::I32(i32::MIN)));
    }

    /// C2 独立边界（fresh eyes）：列级权限 + 序列化恢复组合——
    /// 加密落盘（IPC File + SM4 编解码桩）→ 恢复 → 读路径列级过滤，
    /// 验证「恢复后数据保真」与「权限过滤不越权（防存在性泄露）」两层语义。
    #[test]
    fn qa_regress_serialize_recover_column_permission_filter() {
        use crate::store::filter_entry_columns;

        // 3 列表 7：{0,1,2} 多类型。
        let mut shard = ColumnarShard::new();
        shard.upsert_from_entry(
            7,
            SnapshotEntry::new_typed(
                sensearcdb_common::TenantId::DEFAULT,
                vec![
                    (0, TypedValue::F64(1.0)),
                    (1, TypedValue::I64(2)),
                    (2, TypedValue::String("x".into())),
                ],
                vec![(0, 0xC0), (1, 0xC0), (2, 0xC0)],
                1000,
                2000,
                5,
                false,
                None,
            ),
        );

        let shard_arc = parking_lot::RwLock::new(shard);
        let guard = shard_arc.read();
        let metrics = SnapshotMetrics::noop();
        let dir = tempdir().unwrap();
        let path = dir.path().join("snap_enc.bin");

        // 加密落盘（XOR 桩 = SM4 编解码接线等价物）。
        let enc = make_encryptor();
        serialize_to_file(&path, &[guard], 5, &metrics, Some(&enc), 0).unwrap();

        // 恢复。
        let (restored_shards, version, _wm) =
            deserialize_from_file(&path, 1, &metrics, Some(&make_encryptor())).unwrap();
        assert_eq!(version, 5);
        assert_eq!(restored_shards.len(), 1);

        // 恢复后数据保真：3 列完整还原。
        let entry = restored_shards[0].get(7).expect("恢复后表 7 应有值");
        assert_eq!(
            entry.values,
            vec![
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::I64(2)),
                (2, TypedValue::String("x".into())),
            ],
            "加密落盘→恢复后多类型列必须逐位保真"
        );

        // 读路径列级过滤：只授权 (7,1) → 其余 2 列不得泄露。
        let filtered =
            filter_entry_columns(&entry, 7, &|_, cid| cid == 1).expect("授权列存在应返回 Some");
        assert_eq!(filtered.values.len(), 1, "过滤后只保留授权列");
        assert_eq!(filtered.values[0], (1, TypedValue::I64(2)));
        assert_eq!(filtered.qualities.len(), 1, "qualities 同步收缩");
        assert_eq!(filtered.ts_values.len(), 1, "ts_values 同步收缩");
        assert_eq!(filtered.server_ts_values.len(), 1, "server_ts_values 同步收缩");

        // 授权列全无 → None（防存在性泄露）。
        assert!(
            filter_entry_columns(&entry, 7, &|_, _cid| false).is_none(),
            "整表列全被过滤必须返回 None，不得泄露存在性"
        );
    }
}

//! QA 独立验证测试（严过关 Yan）。
//!
//! 以全新视角独立验证 snapshot.bin「写路径接线」的正确性，不依赖工程师自测。
//! 覆盖点：
//! - A. snapshot round-trip（含 wal_watermark，三者：shards/global_version/watermark）
//! - B. 原子写（成功无 .tmp 残留；失败清理 .tmp 且不破坏原文件）
//! - D. 恢复边界对齐（server_ts 过滤语义 + 水位 ±5s 边界 + 源/服务器时间戳区分）
//! - E. 格式定稿（40B header、magic、版本 1、水位字节定位）
//! - F. TDE 无回归（加密 + 水位共存 round-trip）
//! - C. 脏标记门控（is_dirty / last_written_version 正确翻转）

use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TypedValue};
use sensearcdb_snapshot::{
    deserialize_from_file, recover, serialize_to_file, RecoveryConfig, RecoveryError,
    RecoverySource, SchemaMirrorRef, SnapshotCheckpointer, SnapshotEncryptError, SnapshotEncryptor,
    SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore, WalReplayReader,
    WalReplayReaderRef, WalReplayRecord, WalWatermarkProvider,
};

// ─── 测试桩 ────────────────────────────────────────────────────────────────

struct TestSchemaMirror;

impl SchemaMirrorRef for TestSchemaMirror {
    fn get_table_name(&self, _: u32) -> Option<String> {
        Some("tag".into())
    }
    fn get_table_description(&self, _: u32) -> Option<String> {
        None
    }
    fn get_range_low(&self, _: u32, _: u16) -> f64 {
        0.0
    }
    fn get_range_high(&self, _: u32, _: u16) -> f64 {
        100.0
    }
    fn get_instrument_range_low(&self, _: u32, _: u16) -> Option<f64> {
        None
    }
    fn get_instrument_range_high(&self, _: u32, _: u16) -> Option<f64> {
        None
    }
    fn get_unit(&self, _: u32, _: u16) -> Option<String> {
        None
    }
    fn get_display_digits(&self, _: u32, _: u16) -> u8 {
        2
    }
    fn get_data_type(&self, _: u32, _: u16) -> DataType {
        DataType::F64
    }
    fn is_enabled(&self, _: u32) -> bool {
        true
    }
}

fn make_sub_mgr() -> Arc<SnapshotSubscriptionManager> {
    Arc::new(SnapshotSubscriptionManager::new())
}

fn make_metadata() -> Arc<TagMetadataStore> {
    Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)))
}

fn make_store() -> Arc<SnapshotStore> {
    let cfg = ConfigHandle::new(AppConfig::default());
    Arc::new(SnapshotStore::new(
        &cfg,
        make_sub_mgr(),
        make_metadata(),
        SnapshotMetrics::noop(),
    ))
}

/// 将 store 的当前状态序列化到 path，返回写盘时的 global_version。
fn serialize_store(path: &std::path::Path, store: &SnapshotStore, watermark: i64) -> u64 {
    let (guards, version) = store.snapshot_all_buckets();
    serialize_to_file(
        path,
        &guards,
        version,
        &SnapshotMetrics::noop(),
        None,
        watermark,
    )
    .expect("serialize should succeed");
    version
}

// ─── A + E：round-trip 含水位 + 40B header 格式定稿 ───────────────────────

#[test]
fn test_round_trip_with_watermark_and_header_layout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    let watermark: i64 = 1_700_000_000_123_456_789;

    let store = make_store();
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(100.5))],
        &[(0, 0xC0)],
        1_000,
        1_500,
        None,
        false,
        None,
    );
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        2,
        &[(0, TypedValue::F64(-3.25))],
        &[(0, 0xC0)],
        2_000,
        2_500,
        None,
        false,
        None,
    );
    let version = serialize_store(&path, &store, watermark);

    // ── 格式定稿断言（E）──
    let raw = std::fs::read(&path).unwrap();
    assert!(raw.len() >= 40, "文件至少包含 40B header");
    assert_eq!(&raw[0..4], b"SNAP", "magic 应为 SNAP");
    assert_eq!(
        u32::from_le_bytes(raw[4..8].try_into().unwrap()),
        2,
        "version 应为 2（S4 Arrow IPC File）"
    );
    assert_eq!(
        u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize,
        store.bucket_count(),
        "shard_count 应与配置一致"
    );
    assert_eq!(
        u64::from_le_bytes(raw[12..20].try_into().unwrap()),
        version,
        "global_version 应写入 offset 12..20"
    );
    assert_eq!(raw[20], 0, "明文快照 tde_encrypted 应为 0");
    assert_eq!(
        i64::from_le_bytes(raw[32..40].try_into().unwrap()),
        watermark,
        "wal_watermark 应位于 header 末 8 字节 offset 32..40"
    );

    // ── round-trip 断言（A）──
    let (shards, version_back, watermark_back) =
        deserialize_from_file(&path, store.bucket_count(), &SnapshotMetrics::noop(), None).unwrap();
    assert_eq!(version_back, version, "global_version 应往返一致");
    assert_eq!(watermark_back, watermark, "wal_watermark 应往返一致");
    assert_eq!(shards.len(), store.bucket_count());

    let total: usize = shards.iter().map(|s| s.entry_count()).sum();
    assert_eq!(total, 2, "两个 tag 应完整往返");
}

// ─── B：原子写 ────────────────────────────────────────────────────────────

#[test]
fn test_atomic_write_no_tmp_residue() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");

    let store = make_store();
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(42.0))],
        &[(0, 0xC0)],
        1,
        2,
        None,
        false,
        None,
    );
    serialize_store(&path, &store, 0);

    assert!(path.exists(), "最终文件路径必须存在");
    assert!(
        !dir.path().join("snapshot.bin.tmp").exists(),
        "原子写成功后不得残留 .tmp"
    );
}

/// 加密运算失败桩：模拟写盘中途失败（磁盘满/加密错误）。
struct FailingEncryptor;

impl SnapshotEncryptor for FailingEncryptor {
    fn encrypt(&self, _plaintext: &[u8]) -> Result<Vec<u8>, SnapshotEncryptError> {
        Err(SnapshotEncryptError::Crypto("simulated failure".into()))
    }
    fn decrypt(&self, _blob: &[u8], _dek_version: u32) -> Result<Vec<u8>, SnapshotEncryptError> {
        Ok(Vec::new())
    }
    fn active_dek_version(&self) -> u32 {
        1
    }
    fn is_enabled(&self) -> bool {
        true
    }
}

#[test]
fn test_atomic_write_failure_cleans_tmp_and_preserves_original() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");

    // 1) 先写一份有效明文快照。
    let store = make_store();
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(100.0))],
        &[(0, 0xC0)],
        1,
        2,
        None,
        false,
        None,
    );
    serialize_store(&path, &store, 0);
    let original = std::fs::read(&path).unwrap();

    // 2) 用会失败的加密器覆盖写 → 必须返回 Err（写盘中途失败）。
    let (guards, version) = store.snapshot_all_buckets();
    let res = serialize_to_file(
        &path,
        &guards,
        version,
        &SnapshotMetrics::noop(),
        Some(&FailingEncryptor),
        0,
    );
    assert!(res.is_err(), "写盘失败必须返回 Err");

    // 3) 原文件字节不变（未被破坏），且 .tmp 被清理。
    assert_eq!(
        std::fs::read(&path).unwrap(),
        original,
        "写盘失败不得破坏原文件"
    );
    assert!(
        !dir.path().join("snapshot.bin.tmp").exists(),
        "失败后 .tmp 必须被清理"
    );
}

// ─── D：恢复边界对齐（3.3 档 A：精确 seq，本轮核心风险点） ─────────────────

/// 按 WAL2 seq（raft_index）过滤并记录 after_seq 的 WAL 读取器，语义与
/// 生产实现 `EngineWalReplayReader` 一致（filter + 按 seq 升序排序）。
struct CapturingSeqWalReader {
    records: Vec<WalReplayRecord>,
    calls: Mutex<Vec<u64>>,
}

impl WalReplayReader for CapturingSeqWalReader {
    fn read_after(&self, after_seq: u64) -> Result<Vec<WalReplayRecord>, RecoveryError> {
        self.calls.lock().unwrap().push(after_seq);
        let mut out: Vec<WalReplayRecord> = self
            .records
            .iter()
            .filter(|r| r.seq > after_seq)
            .cloned()
            .collect();
        out.sort_by_key(|r| r.seq);
        Ok(out)
    }
}

fn wal_rec(table_id: u32, ts: i64, server_ts: i64, seq: u64, value: f64) -> WalReplayRecord {
    WalReplayRecord {
        tenant_id: sensearcdb_common::TenantId::DEFAULT,
        table_id,
        ts,
        server_ts,
        seq,
        values: vec![(0, TypedValue::F64(value))],
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
    }
}

#[test]
fn test_recovery_boundary_seq_semantics() {
    // 水位 = seq 100：snapshot.bin 已包含 seq<=100 的所有写入。
    const W: u64 = 100;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");

    // 快照含 tag1=100、tag2=200，水位 seq=W。
    let store = make_store();
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(100.0))],
        &[(0, 0xC0)],
        1_000,
        1_000,
        None,
        false,
        None,
    );
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        2,
        &[(0, TypedValue::F64(200.0))],
        &[(0, 0xC0)],
        2_000,
        2_000,
        None,
        false,
        None,
    );
    serialize_store(&path, &store, W as i64);

    let reader = Arc::new(CapturingSeqWalReader {
        records: vec![
            // seq < 水位 → 不得回放（不重复旧数据）
            wal_rec(3, 0, 9_999, 99, 333.0),
            // seq == 水位 → 不得回放（严格 >）
            wal_rec(4, 0, 9_999, 100, 444.0),
            // seq > 水位 → 必须回放（不漏新数据，覆盖 tag1 快照值；source ts 递增）
            wal_rec(1, 1_001, 9_999, 101, 111.0),
            // 相同 server_ts 多 batch：seq 更高 → 仍必须回放（列级 ts max）
            wal_rec(2, 2_001, 9_999, 102, 222.0),
            wal_rec(5, 3_000, 9_999, 103, 555.0),
        ],
        calls: Mutex::new(Vec::new()),
    });

    let config = RecoveryConfig {
        snapshot_path: path,
        shard_count: store.bucket_count(),
        sac_watermark: 0,
    };
    let rr = recover(
        &config,
        make_sub_mgr(),
        make_metadata(),
        SnapshotMetrics::noop(),
        None,
        Some(reader.clone() as WalReplayReaderRef),
        None,
    )
    .expect("recover should succeed");

    assert_eq!(rr.source, RecoverySource::SnapshotBin);

    // 边界对齐：read_after 只调用一次，且参数 = 精确 seq（不再减时间安全余量）。
    let calls = reader.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "snapshot 恢复只应调用一次 read_after");
    assert_eq!(calls[0], W, "应使用精确 seq 水位（不减安全余量）而非 0");

    // 必须回放的数据（不漏）。
    assert_eq!(
        rr.store.get(1).unwrap().value(),
        111.0,
        "seq 水位之上新数据必须回放"
    );
    assert_eq!(
        rr.store.get(2).unwrap().value(),
        222.0,
        "相同 server_ts 多 batch 按 seq 回放"
    );
    assert_eq!(
        rr.store.get(5).unwrap().value(),
        555.0,
        "seq 更高也必须回放"
    );

    // 不得回放的数据（不重复、不误入）。
    assert!(rr.store.get(3).is_none(), "seq < 水位不得回放");
    assert!(rr.store.get(4).is_none(), "seq == 水位不得回放（严格 >）");
}

#[test]
fn test_recovery_zero_watermark_replays_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");

    let store = make_store();
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(100.0))],
        &[(0, 0xC0)],
        1,
        2,
        None,
        false,
        None,
    );
    serialize_store(&path, &store, 0); // watermark = 0（旧文件/无水位）

    let reader = Arc::new(CapturingSeqWalReader {
        records: vec![wal_rec(1, 100, 100, 1, 200.0)],
        calls: Mutex::new(Vec::new()),
    });

    let config = RecoveryConfig {
        snapshot_path: path,
        shard_count: store.bucket_count(),
        sac_watermark: 0,
    };
    let rr = recover(
        &config,
        make_sub_mgr(),
        make_metadata(),
        SnapshotMetrics::noop(),
        None,
        Some(reader.clone() as WalReplayReaderRef),
        None,
    )
    .unwrap();

    assert_eq!(
        rr.store.get(1).unwrap().value(),
        200.0,
        "无水位时应全量回放"
    );
    let calls = reader.calls.lock().unwrap();
    assert_eq!(calls[0], 0, "无水位时 after_seq 应为 0（全量回放）");
}

// ─── F：TDE 无回归（加密 + 水位共存 round-trip） ─────────────────────────

struct XorEncryptor;

impl SnapshotEncryptor for XorEncryptor {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, SnapshotEncryptError> {
        let mut out = vec![0xAAu8; 28]; // nonce(12)||tag(16)
        out.extend(plaintext.iter().map(|b| b ^ 0x5A));
        Ok(out)
    }
    fn decrypt(&self, blob: &[u8], _dek_version: u32) -> Result<Vec<u8>, SnapshotEncryptError> {
        Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
    }
    fn active_dek_version(&self) -> u32 {
        3
    }
    fn is_enabled(&self) -> bool {
        true
    }
}

#[test]
fn test_tde_roundtrip_with_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    let watermark: i64 = 9_000_000_000_000;

    let store = make_store();
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(10.0))],
        &[(0, 0xC0)],
        1,
        2,
        None,
        false,
        None,
    );
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        2,
        &[(0, TypedValue::F64(20.0))],
        &[(0, 0xC0)],
        3,
        4,
        None,
        false,
        None,
    );

    let (guards, version) = store.snapshot_all_buckets();
    serialize_to_file(
        &path,
        &guards,
        version,
        &SnapshotMetrics::noop(),
        Some(&XorEncryptor),
        watermark,
    )
    .unwrap();

    // 文件头明文（magic 可见 + 自描述加密标志）。
    let raw = std::fs::read(&path).unwrap();
    assert_eq!(&raw[0..4], b"SNAP");
    assert_eq!(raw[20], 1, "tde_encrypted 应为 1");

    let (shards, version_back, watermark_back) = deserialize_from_file(
        &path,
        store.bucket_count(),
        &SnapshotMetrics::noop(),
        Some(&XorEncryptor),
    )
    .unwrap();
    assert_eq!(version_back, version);
    assert_eq!(watermark_back, watermark, "加密快照的水位也应往返一致");
    let total: usize = shards.iter().map(|s| s.entry_count()).sum();
    assert_eq!(total, 2);
}

// ─── C：脏标记门控 ────────────────────────────────────────────────────────

struct MockWatermark(u64);

impl WalWatermarkProvider for MockWatermark {
    fn current_watermark(&self) -> u64 {
        self.0
    }
}

#[test]
fn test_dirty_marker_gate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    let store = make_store();

    let cp = SnapshotCheckpointer::new(
        Arc::new(ArcSwap::new(store.clone())),
        path.clone(),
        ConfigHandle::new(AppConfig::default()),
        SnapshotMetrics::noop(),
        None,
        Some(Arc::new(MockWatermark(123_456))),
    );

    // 初始：空 store，version=0 == last_written=0 → 不脏。
    assert!(!cp.is_dirty());
    assert_eq!(cp.last_written_version(), 0);

    // flush 后推进 last_written_version。
    cp.flush_once().unwrap();
    assert!(path.exists());
    assert_eq!(cp.last_written_version(), store.current_version());

    // 写入 → 变脏。
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(42.0))],
        &[(0, 0xC0)],
        1000,
        2000,
        None,
        false,
        None,
    );
    assert!(cp.is_dirty(), "新写入后应标记为脏");

    // 再 flush → 干净。
    cp.flush_once().unwrap();
    assert!(!cp.is_dirty());
    assert_eq!(cp.last_written_version(), store.current_version());
}

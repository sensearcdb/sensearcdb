//! QA 独立验证（严过关 Yan）—— snapshot.bin TDE 加密正确性。
//!
//! 独立覆盖（不信任 `qa_independent_verify.rs` 里的 XOR 桩，其无法检测篡改）：
//!   1. 加密 round-trip（多 shard、shard_version/global_version 无损）
//!   2. 无密钥遇密文硬报错（NoEncryptor）
//!   3. 恢复路径禁止回退到 SAC 扫描（防止绕过加密）
//!   4. 篡改密文 payload → 解密失败报错（GCM 完整性真实生效）
//!   5. tde_encrypted / dek_version 默认值恒 0

use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TypedValue};
use sensearcdb_snapshot::{
    deserialize_from_file, recover, serialize_to_file, RecoveryConfig, SchemaMirrorRef,
    SerializeError, SnapshotEncryptError, SnapshotEncryptor, SnapshotMetrics, SnapshotStore,
    SnapshotSubscriptionManager, TagMetadataStore,
};

// ─── 认证型加密桩（模拟 GCM 语义）────────────────────────────────────────

fn keyed_tag(data: &[u8], key: u8) -> [u8; 16] {
    let mut h: u32 = 0x811c_9dc5;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h ^= key as u32;
    let mut tag = [0u8; 16];
    tag[..4].copy_from_slice(&h.to_le_bytes());
    tag
}

struct GcmLikeSnapshotEncryptor {
    key: u8,
    version: u32,
}

impl SnapshotEncryptor for GcmLikeSnapshotEncryptor {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, SnapshotEncryptError> {
        let mut out = Vec::with_capacity(28 + plaintext.len());
        out.extend_from_slice(&[0xA5u8; 12]); // nonce
        out.extend_from_slice(&keyed_tag(plaintext, self.key)); // tag
        out.extend(plaintext.iter().map(|b| b ^ self.key)); // ct
        Ok(out)
    }

    fn decrypt(&self, blob: &[u8], _dek_version: u32) -> Result<Vec<u8>, SnapshotEncryptError> {
        if blob.len() < 28 {
            return Err(SnapshotEncryptError::TooShort(blob.len(), 28));
        }
        let tag = &blob[12..28];
        let ct = &blob[28..];
        let plain: Vec<u8> = ct.iter().map(|b| b ^ self.key).collect();
        if keyed_tag(&plain, self.key) != tag {
            return Err(SnapshotEncryptError::AuthFailed);
        }
        Ok(plain)
    }

    fn active_dek_version(&self) -> u32 {
        self.version
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

fn enc(key: u8) -> GcmLikeSnapshotEncryptor {
    GcmLikeSnapshotEncryptor { key, version: 1 }
}

// ─── 测试桩 ──────────────────────────────────────────────────────────────

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

fn make_store() -> Arc<SnapshotStore> {
    let cfg = ConfigHandle::new(AppConfig::default());
    Arc::new(SnapshotStore::new(
        &cfg,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
        SnapshotMetrics::noop(),
    ))
}

// ─── 1. 加密 round-trip（多 shard）───────────────────────────────────────

#[test]
fn qa_snapshot_multishard_encrypted_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");

    let store = make_store();
    // 100 个 tag 散落到多个 shard。
    for i in 1u32..=100u32 {
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            i,
            &[(0, TypedValue::F64(i as f64 * 0.5))],
            &[(0, 0xC0)],
            1000 + i as i64,
            2000 + i as i64,
            None,
            false,
            None,
        );
    }

    let (guards, version) = store.snapshot_all_buckets();
    let watermark: i64 = 1_700_000_000_123_456_789;
    serialize_to_file(
        &path,
        &guards,
        version,
        &SnapshotMetrics::noop(),
        Some(&enc(0x5A)),
        watermark,
    )
    .unwrap();

    let raw = std::fs::read(&path).unwrap();
    assert_eq!(&raw[0..4], b"SNAP", "magic 必须明文");
    assert_eq!(raw[20], 1, "tde_encrypted 应为 1");

    let (shards, version_back, watermark_back) = deserialize_from_file(
        &path,
        store.bucket_count(),
        &SnapshotMetrics::noop(),
        Some(&enc(0x5A)),
    )
    .unwrap();

    assert_eq!(version_back, version, "global_version 无损");
    assert_eq!(watermark_back, watermark, "wal_watermark 无损");
    assert_eq!(shards.len(), store.bucket_count());
    let total: usize = shards.iter().map(|s| s.entry_count()).sum();
    assert_eq!(total, 100, "100 个 tag 应完整往返");

    // 抽查几个 tag 值无损。
    let mut found = 0usize;
    for shard in &shards {
        if let Some(e) = shard.get(1) {
            assert!((e.value() - 0.5).abs() < 1e-12);
            found += 1;
        }
        if let Some(e) = shard.get(100) {
            assert!((e.value() - 50.0).abs() < 1e-12);
            found += 1;
        }
    }
    assert_eq!(found, 2);
}

// ─── 2. 无密钥硬报错 ────────────────────────────────────────────────────

#[test]
fn qa_snapshot_no_key_hard_error() {
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
    let (guards, version) = store.snapshot_all_buckets();
    serialize_to_file(
        &path,
        &guards,
        version,
        &SnapshotMetrics::noop(),
        Some(&enc(0x5A)),
        0,
    )
    .unwrap();

    let result = deserialize_from_file(&path, store.bucket_count(), &SnapshotMetrics::noop(), None);
    assert!(result.is_err());
    match result.err().unwrap() {
        SerializeError::Encrypt(SnapshotEncryptError::NoEncryptor) => {}
        other => panic!("expected NoEncryptor, got: {}", other),
    }
}

// ─── 3. 恢复路径禁止回退 SAC ────────────────────────────────────────────

#[test]
fn qa_recover_encrypted_snapshot_no_key_refuses_sac_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");

    let store = make_store();
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(1.0))],
        &[(0, 0xC0)],
        1,
        2,
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
        Some(&enc(0x5A)),
        0,
    )
    .unwrap();

    let config = RecoveryConfig {
        snapshot_path: path,
        shard_count: store.bucket_count(),
        sac_watermark: 0,
    };

    // 即使提供了 SAC reader，密文快照无密钥也必须硬报错、绝不回退 SAC。
    let sac_reader = Arc::new(NoopSac);
    let result = recover(
        &config,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
        SnapshotMetrics::noop(),
        Some(sac_reader),
        None,
        None,
    );
    assert!(
        result.is_err(),
        "encrypted snapshot without key must hard-error"
    );
    assert!(result.err().unwrap().is_no_encryptor());
}

struct NoopSac;
impl sensearcdb_snapshot::SacRecoveryReader for NoopSac {
    fn scan_all(
        &self,
    ) -> Result<Vec<sensearcdb_snapshot::SacScanRow>, sensearcdb_snapshot::RecoveryError> {
        // 如果回退被触发，这里会返回数据；正确行为是根本不会走到这里。
        Ok(vec![sensearcdb_snapshot::SacScanRow {
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            table_id: 999,
            ts: 1,
            server_ts: 2,
            values: vec![(0, TypedValue::F64(999.0))],
            qualities: vec![(0, 0xC0)],
        }])
    }
}

// ─── 4. 篡改密文 payload → 解密失败报错 ─────────────────────────────────

#[test]
fn qa_snapshot_ciphertext_tamper_auth_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");

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
    let (guards, version) = store.snapshot_all_buckets();
    serialize_to_file(
        &path,
        &guards,
        version,
        &SnapshotMetrics::noop(),
        Some(&enc(0x5A)),
        0,
    )
    .unwrap();

    // 布局：[40B header][8B len][shard0 payload = nonce12|tag16|ct...]
    // 篡改 shard0 的 ct 首字节（offset 40 + 8 + 28 = 76）。
    let mut bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() > 76, "snapshot too small");
    bytes[76] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    // 有正确密钥，但密文被篡改 → GCM 认证失败报错（不是「解出错误数据仍成功」）。
    let result = deserialize_from_file(
        &path,
        store.bucket_count(),
        &SnapshotMetrics::noop(),
        Some(&enc(0x5A)),
    );
    assert!(result.is_err(), "篡改密文必须解密失败");
    match result.err().unwrap() {
        SerializeError::Encrypt(SnapshotEncryptError::AuthFailed) => {}
        other => panic!("expected AuthFailed, got: {}", other),
    }
}

// ─── 5. 默认值恒 0 ──────────────────────────────────────────────────────

#[test]
fn qa_snapshot_plaintext_defaults_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");

    let store = make_store();
    store.update_typed(
        sensearcdb_common::TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(7.5))],
        &[(0, 0xC0)],
        1,
        2,
        None,
        false,
        None,
    );
    let (guards, version) = store.snapshot_all_buckets();
    serialize_to_file(&path, &guards, version, &SnapshotMetrics::noop(), None, 0).unwrap();

    let raw = std::fs::read(&path).unwrap();
    assert_eq!(raw[20], 0, "明文快照 tde_encrypted 必须 0");
    assert_eq!(
        u32::from_le_bytes(raw[21..25].try_into().unwrap()),
        0,
        "明文快照 dek_version 必须 0"
    );
    // reserved[7] 恒 0。
    assert_eq!(&raw[25..32], &[0u8; 7], "reserved 必须恒 0");
}

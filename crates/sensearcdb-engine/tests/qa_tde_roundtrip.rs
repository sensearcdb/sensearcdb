//! QA 独立回归验证：TDE 注入 engine 的加密 round-trip。
//!
//! 覆盖完成标准：
//! - SAC 写加密 → 读解密 round-trip
//! - WAL 写加密 → 读解密 round-trip
//! - 无密钥遇密文硬报错（`NoEncryptor`）
//! - 明文 legacy 文件照读（TDE 关闭行为不回归）
//! - SAC Header `format_flags bit0` + `dek_version` 语义化（O-1=A）
//!
//! 本文件使用一个 XOR 型 `Encryptor` 桩（engine 不依赖 security crate），
//! 只验证「加密落点接线 + 密文布局 + reader 解密切片」的正确性；真实 AES/SM4
//! 算法 round-trip 由 `sensearcdb-security` 的 `tde.rs` 测试覆盖。

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::compression::CompressionType;
use sensearcdb_engine::encryption::{EncryptContext, EncryptError, Encryptor, SharedEncryptor};
use sensearcdb_engine::engine::{DataPoint, PointBatch};
use sensearcdb_engine::memtable::{ColVec, ColumnarChunk, TypedValue};
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::{SacReader, SacWriter};
use sensearcdb_engine::wal::{WalReader, WalWriter};

/// XOR 型加密桩：产出 `nonce(12B)||tag(16B)||ct` 自包含密文，满足 28B 开销约定。
struct TestEncryptor {
    version: u32,
    enabled: bool,
}

impl Encryptor for TestEncryptor {
    fn encrypt(&self, plaintext: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        let mut out = Vec::with_capacity(28 + plaintext.len());
        out.extend_from_slice(&[0xA1u8; 12]); // nonce
        out.extend_from_slice(&[0xB2u8; 16]); // tag
        for b in plaintext {
            out.push(b ^ 0x5A);
        }
        Ok(out)
    }

    fn decrypt(&self, blob: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        if blob.len() < 28 {
            return Err(EncryptError::TooShort(blob.len(), 28));
        }
        Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
    }

    fn active_dek_version(&self, _tenant_id: sensearcdb_engine::TenantId) -> u32 {
        self.version
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// 构造一个启用的测试加密器（dek_version = 1）。
fn enc() -> SharedEncryptor {
    Some(Arc::new(TestEncryptor {
        version: 1,
        enabled: true,
    }))
}

/// 构造一个使用临时 wal_dir 的 ConfigHandle。
fn temp_wal_config() -> (ConfigHandle, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    (ConfigHandle::new(cfg), tmp)
}

/// 构造一个测试 ColumnarChunk（两列：F64 + I64）。
fn test_chunk() -> ColumnarChunk {
    let mut columns = HashMap::new();
    columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0, 4.0]));
    columns.insert(1, ColVec::I64(vec![10, -20, 30, 40]));
    let ts_index: BTreeMap<i64, usize> = [100i64, 200, 300, 400]
        .into_iter()
        .enumerate()
        .map(|(i, ts)| (ts, i))
        .collect();
    // 两列均密集（各 4 值），presence 全 true（生产 append 语义）。
    let presence: HashMap<u16, Vec<bool>> = columns
        .iter()
        .map(|(cid, _)| (*cid, vec![true; 4]))
        .collect();
    ColumnarChunk {
        table_id: 42,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        timestamps: vec![100, 200, 300, 400],
        columns,
        presence,
        qualities: vec![0xC0; 4],
        ts_index,
        point_count: 4,
        out_of_order: false,
        max_wal_seq: Some(7),
        min_wal_seq: None,
    }
}

/// 写入一个 SAC 文件，返回文件路径。
fn write_sac(dir: &std::path::Path, encryptor: SharedEncryptor) -> PathBuf {
    let writer = SacWriter::new_with_encryptor(dir, CompressionType::Lz4, encryptor);
    let chunk = test_chunk();
    writer
        .write_chunk(&chunk, 0, SacLevel::L0, 1, Some(7))
        .expect("write failed")
}

// ─── SAC 加密 round-trip ───────────────────────────────────────────────────

#[test]
fn qa_tde_sac_encrypted_roundtrip() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_sac(&tmp.path().join("sac"), enc());

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        42,
        0,
        1000,
        &[],
        enc(),
    )
    .expect("read failed");
    assert_eq!(rows.len(), 4, "应读回 4 行");
    assert_eq!(rows[0].ts, 100);

    let mut v0 = rows[0].values.clone();
    v0.sort_by_key(|(c, _)| *c);
    assert_eq!(v0[0], (0, TypedValue::F64(1.0)));
    assert_eq!(v0[1], (1, TypedValue::I64(10)));

    // 负值 I64 也应无损还原。
    let mut v1 = rows[1].values.clone();
    v1.sort_by_key(|(c, _)| *c);
    assert_eq!(v1[1], (1, TypedValue::I64(-20)));
}

#[test]
fn qa_tde_sac_encrypted_no_key_errors() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_sac(&tmp.path().join("sac"), enc());

    // 密文文件 + 无加密器 → 必须硬报错，拒绝把密文当明文返回。
    let result = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        42,
        0,
        1000,
        &[],
        None,
    );
    assert!(result.is_err(), "encrypted SAC without key must hard-error");
}

#[test]
fn qa_tde_sac_plaintext_legacy_read() {
    let tmp = tempfile::TempDir::new().unwrap();
    // TDE 关闭（None）写出的明文 legacy 文件，无密钥照读。
    let path = write_sac(&tmp.path().join("sac"), None);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        42,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].ts, 100);
}

#[test]
fn qa_tde_sac_header_encrypted_flag_and_dek_version() {
    let tmp = tempfile::TempDir::new().unwrap();

    // 加密文件：bit0=1 + dek_version=1。
    let enc_path = write_sac(&tmp.path().join("sac"), enc());
    let enc_bytes = std::fs::read(&enc_path).unwrap();
    assert_eq!(&enc_bytes[0..4], b"RTM8", "magic 必须保持明文");
    assert_eq!(enc_bytes[12] & 0x01, 0x01, "format_flags bit0 应为 1");
    let dek_version = u32::from_le_bytes(enc_bytes[16..20].try_into().unwrap());
    assert_eq!(dek_version, 1, "dek_version 应写入 padding[0..4]");

    // 明文文件：bit0=0 + dek_version=0（默认值须与恒 0 一致）。
    let plain_path = write_sac(&tmp.path().join("sac_plain"), None);
    let plain_bytes = std::fs::read(&plain_path).unwrap();
    assert_eq!(plain_bytes[12] & 0x01, 0x00, "明文文件 bit0 应为 0");
    let plain_dek = u32::from_le_bytes(plain_bytes[16..20].try_into().unwrap());
    assert_eq!(plain_dek, 0, "明文文件 dek_version 应为 0");
}

// ─── WAL 加密 round-trip ───────────────────────────────────────────────────

#[tokio::test]
async fn qa_tde_wal_encrypted_roundtrip() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new_with_encryptor(&cfg, enc()).expect("create WalWriter");

    let point = DataPoint {
        table_id: 7,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        ts: 123_456_789,
        server_ts: 123_456_790,
        values: vec![(0, TypedValue::F64(42.5))],
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    };
    wal.write_batch(PointBatch {
        points: vec![point],
    })
    .await
    .expect("WAL write failed");

    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new_with_encryptor(wal_dir, enc());
    let records = reader.replay_all(1).expect("WAL replay failed");

    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].table_id, 7);
    assert_eq!(recovered[0].values[0], (0, TypedValue::F64(42.5)));
}

#[tokio::test]
async fn qa_tde_wal_encrypted_no_key_errors() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new_with_encryptor(&cfg, enc()).expect("create WalWriter");

    let point = DataPoint {
        table_id: 8,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        ts: 1,
        server_ts: 2,
        values: vec![(0, TypedValue::F64(9.9))],
        qualities: vec![],
        schema_version: 1,
        flags: 0,
    };
    wal.write_batch(PointBatch {
        points: vec![point],
    })
    .await
    .expect("WAL write failed");

    // 无加密器重放密文 WAL → 必须硬报错。
    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new(wal_dir);
    let result = reader.replay_all(1);
    assert!(result.is_err(), "encrypted WAL without key must hard-error");
}

#[tokio::test]
async fn qa_tde_wal_plaintext_legacy_read() {
    let (cfg, _tmp) = temp_wal_config();
    // TDE 关闭（new）写出的明文 legacy 帧，无密钥照读。
    let wal = WalWriter::new(&cfg).expect("create WalWriter");
    let point = DataPoint {
        table_id: 9,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        ts: 100,
        server_ts: 101,
        values: vec![(0, TypedValue::F64(1.0))],
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    };
    wal.write_batch(PointBatch {
        points: vec![point],
    })
    .await
    .expect("WAL write failed");

    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new(wal_dir);
    let records = reader.replay_all(1).expect("WAL replay failed");
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].values[0], (0, TypedValue::F64(1.0)));
}

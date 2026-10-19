//! QA 独立验证（严过关 Yan）—— TDE 三层加密正确性与格式兼容。
//!
//! 不信任工程师自测（`qa_tde_roundtrip.rs`），以全新视角独立覆盖：
//!   1. 三层加密 round-trip（SAC/WAL/snapshot 多类型值无损）
//!   2. 无密钥遇密文硬报错（NoEncryptor，绝不静默返回明文）
//!   3. 明文 legacy 兼容 + 明文/密文混存
//!   4. 篡改密文检测（CRC 粗筛跳过 + GCM 认证失败路径）
//!
//! 本文件使用一个「带认证 tag 的 XOR 桩」模拟 GCM 语义（keyed-FNV tag），
//! 能真实区分「篡改密文 → AuthFailed」与「正确密钥 → 解出原文」，
//! 避免工程师自测里 XOR 桩「篡改后仍解出错误数据却报成功」的盲区。

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle, SenseArcError};
use sensearcdb_engine::compression::CompressionType;
use sensearcdb_engine::encryption::{EncryptContext, EncryptError, Encryptor, SharedEncryptor};
use sensearcdb_engine::engine::{DataPoint, PointBatch};
use sensearcdb_engine::memtable::{ColVec, ColumnarChunk, TypedValue};
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::{SacReader, SacWriter};
use sensearcdb_engine::wal::{WalReader, WalWriter};
use sensearcdb_engine::TenantId;

// ─── 认证型加密桩（模拟 GCM 语义）────────────────────────────────────────

/// keyed-FNV 认证 tag：`encrypt` 写入 `nonce(12)||tag(16)||ct`，
/// `decrypt` 校验 tag，篡改密文或密钥错误都会返回 `AuthFailed`。
/// 这比工程师自测的纯 XOR 桩（无法检测篡改）更接近真实 GCM 行为。
struct GcmLikeEncryptor {
    key: u8,
    version: u32,
    enabled: bool,
    /// 置 true 时 `decrypt` 恒返回 AuthFailed（模拟错误密钥/损坏）。
    fail_decrypt: bool,
}

fn keyed_tag(data: &[u8], key: u8) -> [u8; 16] {
    let mut h: u32 = 0x811c_9dc5; // FNV-1a
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h ^= key as u32;
    let mut tag = [0u8; 16];
    tag[..4].copy_from_slice(&h.to_le_bytes());
    tag
}

impl Encryptor for GcmLikeEncryptor {
    fn encrypt(&self, plaintext: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        let mut out = Vec::with_capacity(28 + plaintext.len());
        out.extend_from_slice(&[0xA5u8; 12]); // nonce
        out.extend_from_slice(&keyed_tag(plaintext, self.key)); // tag
        out.extend(plaintext.iter().map(|b| b ^ self.key)); // ct
        Ok(out)
    }

    fn decrypt(&self, blob: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        if self.fail_decrypt {
            return Err(EncryptError::AuthFailed);
        }
        if blob.len() < 28 {
            return Err(EncryptError::TooShort(blob.len(), 28));
        }
        let tag = &blob[12..28];
        let ct = &blob[28..];
        let plain: Vec<u8> = ct.iter().map(|b| b ^ self.key).collect();
        if keyed_tag(&plain, self.key) != tag {
            return Err(EncryptError::AuthFailed);
        }
        Ok(plain)
    }

    fn active_dek_version(&self, _tenant_id: TenantId) -> u32 {
        self.version
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

fn enc() -> SharedEncryptor {
    Some(Arc::new(GcmLikeEncryptor {
        key: 0x5A,
        version: 1,
        enabled: true,
        fail_decrypt: false,
    }))
}

fn enc_wrong_key() -> SharedEncryptor {
    Some(Arc::new(GcmLikeEncryptor {
        key: 0x5B, // 错误密钥
        version: 1,
        enabled: true,
        fail_decrypt: false,
    }))
}

fn is_no_encryptor(e: &SenseArcError) -> bool {
    matches!(e, SenseArcError::TdeError(s) if s.contains("no Encryptor provided"))
}

fn temp_wal_config() -> (ConfigHandle, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    (ConfigHandle::new(cfg), tmp)
}

/// 8 种物理类型全覆盖的 ColumnarChunk（F64/F32/I64/U64/I32/Bool/String/Enum16）。
fn multitype_chunk() -> ColumnarChunk {
    let mut columns = HashMap::new();
    columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0]));
    columns.insert(1, ColVec::F32(vec![1.5, -2.5, 3.5]));
    columns.insert(2, ColVec::I64(vec![10, -20, 30]));
    columns.insert(3, ColVec::U64(vec![1, 2, 18_446_744_073_709_551_615]));
    columns.insert(4, ColVec::I32(vec![-7, 8, -9]));
    columns.insert(5, ColVec::Bool(vec![true, false, true]));
    columns.insert(
        6,
        ColVec::String(vec![
            "open".to_string(),
            "close".to_string(),
            "open".to_string(),
        ]),
    );
    columns.insert(7, ColVec::Enum16(vec![0, 1, 65535]));
    let ts_index: BTreeMap<i64, usize> = [100i64, 200, 300]
        .into_iter()
        .enumerate()
        .map(|(i, ts)| (ts, i))
        .collect();
    // 8 列均密集（各 3 值），presence 全 true（生产 append 语义）。
    let presence: HashMap<u16, Vec<bool>> = columns
        .iter()
        .map(|(cid, _)| (*cid, vec![true; 3]))
        .collect();
    ColumnarChunk {
        table_id: 77,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        timestamps: vec![100, 200, 300],
        columns,
        presence,
        qualities: vec![0xC0, 0x80, 0xC0],
        ts_index,
        point_count: 3,
        out_of_order: false,
        max_wal_seq: Some(9),
        min_wal_seq: None,
    }
}

fn write_sac(dir: &std::path::Path, encryptor: SharedEncryptor) -> PathBuf {
    let writer = SacWriter::new_with_encryptor(dir, CompressionType::Lz4, encryptor);
    writer
        .write_chunk(&multitype_chunk(), 0, SacLevel::L0, 1, Some(9))
        .expect("write failed")
}

/// ts 密文流在文件内的起始偏移（header 24B + mini-header 40B + dir (N+2)*12B）。
fn ts_stream_offset(column_count: usize) -> usize {
    24 + 40 + (column_count + 2) * 12
}

// ─── 1. 三层加密 round-trip ──────────────────────────────────────────────

#[test]
fn qa_sac_multitype_encrypted_roundtrip() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_sac(&tmp.path().join("sac"), enc());

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        77,
        0,
        1000,
        &[],
        enc(),
    )
    .expect("read failed");
    assert_eq!(rows.len(), 3);

    // 逐行、逐类型校验无损。
    let r0 = &rows[0];
    assert_eq!(r0.ts, 100);
    let mut v0 = r0.values.clone();
    v0.sort_by_key(|(c, _)| *c);
    assert_eq!(v0[0], (0, TypedValue::F64(1.0)));
    assert_eq!(v0[1], (1, TypedValue::F32(1.5)));
    assert_eq!(v0[2], (2, TypedValue::I64(10)));
    assert_eq!(v0[3], (3, TypedValue::U64(1)));
    assert_eq!(v0[4], (4, TypedValue::I32(-7)));
    assert_eq!(v0[5], (5, TypedValue::Bool(true)));
    assert_eq!(v0[6], (6, TypedValue::String("open".to_string())));
    assert_eq!(v0[7], (7, TypedValue::Enum16(0)));

    // U64 上限与 Enum16 上限（边界值）无损。
    let r1 = &rows[1];
    let mut v1 = r1.values.clone();
    v1.sort_by_key(|(c, _)| *c);
    assert_eq!(v1[3], (3, TypedValue::U64(2)));
    assert_eq!(v1[7], (7, TypedValue::Enum16(1)));

    let r2 = &rows[2];
    let mut v2 = r2.values.clone();
    v2.sort_by_key(|(c, _)| *c);
    assert_eq!(v2[3], (3, TypedValue::U64(18_446_744_073_709_551_615)));
    assert_eq!(v2[7], (7, TypedValue::Enum16(65535)));
    assert_eq!(v2[6], (6, TypedValue::String("open".to_string())));
    // 负值 F32/I64/I32 无损。
    assert_eq!(v2[1], (1, TypedValue::F32(3.5)));
    assert_eq!(v2[2], (2, TypedValue::I64(30)));
    assert_eq!(v2[4], (4, TypedValue::I32(-9)));
}

#[test]
fn qa_sac_ciphertext_really_encrypted() {
    let tmp = tempfile::TempDir::new().unwrap();
    let enc_path = write_sac(&tmp.path().join("sac_enc"), enc());
    let plain_path = write_sac(&tmp.path().join("sac_plain"), None);

    let enc_bytes = std::fs::read(&enc_path).unwrap();
    let plain_bytes = std::fs::read(&plain_path).unwrap();

    // 密文文件必须比明文文件大（每流 nonce12+tag16 开销；8 列 = 10 流）。
    assert!(
        enc_bytes.len() > plain_bytes.len() + 28 * 9,
        "encrypted {} vs plain {}, missing per-stream overhead",
        enc_bytes.len(),
        plain_bytes.len()
    );
    // 密文字节 ≠ 明文（不只是 header 差异，流内容也应不同）。
    assert_ne!(&enc_bytes[..], &plain_bytes[..]);
    // magic 保持明文。
    assert_eq!(&enc_bytes[0..4], b"RTM8");
}

#[tokio::test]
async fn qa_wal_multitype_encrypted_roundtrip() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new_with_encryptor(&cfg, enc()).expect("create WalWriter");

    let point = DataPoint {
        table_id: 7,
        tenant_id: TenantId::DEFAULT,
        ts: 123_456_789,
        server_ts: 123_456_790,
        values: vec![
            (0, TypedValue::F64(42.5)),
            (1, TypedValue::F32(-1.25)),
            (2, TypedValue::I64(-9_223_372_036_854_775_808)),
            (3, TypedValue::U64(18_446_744_073_709_551_615)),
            (4, TypedValue::I32(-2_147_483_648)),
            (5, TypedValue::Bool(true)),
            (6, TypedValue::String("phase-b".to_string())),
            (7, TypedValue::Enum16(65535)),
        ],
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
    let vals = &recovered[0].values;
    assert_eq!(vals[0], (0, TypedValue::F64(42.5)));
    assert_eq!(vals[1], (1, TypedValue::F32(-1.25)));
    assert_eq!(vals[2], (2, TypedValue::I64(-9_223_372_036_854_775_808)));
    assert_eq!(vals[3], (3, TypedValue::U64(18_446_744_073_709_551_615)));
    assert_eq!(vals[4], (4, TypedValue::I32(-2_147_483_648)));
    assert_eq!(vals[5], (5, TypedValue::Bool(true)));
    assert_eq!(vals[6], (6, TypedValue::String("phase-b".to_string())));
    assert_eq!(vals[7], (7, TypedValue::Enum16(65535)));
}

// ─── 2. 无密钥遇密文硬报错 ──────────────────────────────────────────────

#[test]
fn qa_sac_no_key_hard_error_is_noencryptor() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_sac(&tmp.path().join("sac"), enc());

    let result = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        77,
        0,
        1000,
        &[],
        None,
    );
    assert!(result.is_err(), "encrypted SAC without key must hard-error");
    assert!(
        is_no_encryptor(result.as_ref().err().unwrap()),
        "expected NoEncryptor, got: {:?}",
        result
    );
}

#[tokio::test]
async fn qa_wal_no_key_hard_error_is_noencryptor() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new_with_encryptor(&cfg, enc()).expect("create WalWriter");
    let point = DataPoint {
        table_id: 8,
        tenant_id: TenantId::DEFAULT,
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

    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new(wal_dir);
    let result = reader.replay_all(1);
    assert!(result.is_err(), "encrypted WAL without key must hard-error");
    assert!(
        is_no_encryptor(result.as_ref().err().unwrap()),
        "expected NoEncryptor, got: {:?}",
        result
    );
}

// ─── 3. 明文 legacy 兼容 + 混存 ──────────────────────────────────────────

#[test]
fn qa_sac_mixed_plaintext_and_encrypted() {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().join("sac");

    // 同一目录写一个明文 + 一个加密文件。
    let plain_path = {
        let writer = SacWriter::new(&dir, CompressionType::Lz4);
        writer
            .write_chunk(&multitype_chunk(), 0, SacLevel::L0, 1, Some(9))
            .unwrap()
    };
    let enc_path = {
        let writer = SacWriter::new_with_encryptor(&dir, CompressionType::Lz4, enc());
        writer
            .write_chunk(&multitype_chunk(), 0, SacLevel::L0, 2, Some(9))
            .unwrap()
    };

    // 明文文件：无密钥照读。
    let plain_rows = SacReader::read_range(
        &plain_path,
        sensearcdb_engine::TenantId::DEFAULT,
        77,
        0,
        1000,
        &[],
        None,
    )
    .unwrap();
    assert_eq!(plain_rows.len(), 3);

    // 密文文件：有密钥正确解密；无密钥硬报错。
    let enc_rows = SacReader::read_range(
        &enc_path,
        sensearcdb_engine::TenantId::DEFAULT,
        77,
        0,
        1000,
        &[],
        enc(),
    )
    .unwrap();
    assert_eq!(enc_rows.len(), 3);
    assert!(SacReader::read_range(
        &enc_path,
        sensearcdb_engine::TenantId::DEFAULT,
        77,
        0,
        1000,
        &[],
        None
    )
    .is_err());

    // 明文文件 header bit0=0、dek_version=0（不被误判为密文）。
    let plain_bytes = std::fs::read(&plain_path).unwrap();
    assert_eq!(plain_bytes[12] & 0x01, 0, "明文文件 bit0 必须恒 0");
    assert_eq!(
        u32::from_le_bytes(plain_bytes[16..20].try_into().unwrap()),
        0,
        "明文文件 dek_version 必须恒 0"
    );
}

#[test]
fn qa_sac_header_tenant_id_default() {
    // SAC v8：offset 20..24 为 tenant_id，默认租户 = 0。
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_sac(&tmp.path().join("sac"), enc());
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
        0,
        "tenant_id（offset 20..24）应恒为默认租户 0"
    );
}

#[tokio::test]
async fn qa_wal_mixed_plaintext_and_encrypted_segments() {
    // 明文段（legacy）与密文段共存于同一目录，重放时分别走明文/解密路径。
    let (cfg_plain, _t1) = temp_wal_config();
    let (cfg_enc, _t2) = temp_wal_config();

    // 明文帧。
    let w1 = WalWriter::new(&cfg_plain).unwrap();
    w1.write_batch(PointBatch {
        points: vec![DataPoint {
            table_id: 1,
            tenant_id: TenantId::DEFAULT,
            ts: 100,
            server_ts: 101,
            values: vec![(0, TypedValue::F64(1.0))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }],
    })
    .await
    .unwrap();

    // 密文帧。
    let w2 = WalWriter::new_with_encryptor(&cfg_enc, enc()).unwrap();
    w2.write_batch(PointBatch {
        points: vec![DataPoint {
            table_id: 2,
            tenant_id: TenantId::DEFAULT,
            ts: 200,
            server_ts: 201,
            values: vec![(0, TypedValue::F64(2.0))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }],
    })
    .await
    .unwrap();

    // 合并到同一目录。
    let mixed_dir = tempfile::TempDir::new().unwrap();
    let plain_seg = std::fs::read_dir(cfg_plain.get().paths.wal_dir())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let enc_seg = std::fs::read_dir(cfg_enc.get().paths.wal_dir())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::copy(&plain_seg, mixed_dir.path().join("00000001.wal")).unwrap();
    std::fs::copy(&enc_seg, mixed_dir.path().join("00000002.wal")).unwrap();

    // 有密钥重放：明文 + 密文帧都正确恢复。
    let reader = WalReader::new_with_encryptor(mixed_dir.path(), enc());
    let records = reader.replay_all(1).unwrap();
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 2, "明文 + 密文帧都应恢复");
    let mut ids: Vec<u32> = recovered.iter().map(|p| p.table_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);

    // 无密钥重放：明文帧照读，密文帧硬报错（绝不把密文当明文）。
    let reader_no_key = WalReader::new(mixed_dir.path());
    let result = reader_no_key.replay_all(1);
    assert!(result.is_err());
    assert!(is_no_encryptor(&result.err().unwrap()));
}

// ─── 4. 篡改密文检测 ────────────────────────────────────────────────────

#[test]
fn qa_sac_ciphertext_tamper_crc_skips_block_no_fatal() {
    // 篡改密文字节 → per-block CRC（覆盖密文）失败 → 跳过该 Block，不返回全文件错误、不 panic。
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_sac(&tmp.path().join("sac"), enc());

    let mut bytes = std::fs::read(&path).unwrap();
    // 篡改 ts 密文流的 ct 首字节（8 列 → dir = 10*12 = 120）。
    let ts_off = ts_stream_offset(8);
    let tamper_at = ts_off + 28;
    assert!(tamper_at < bytes.len(), "file too small for tamper test");
    bytes[tamper_at] ^= 0xFF;

    std::fs::write(&path, &bytes).unwrap();

    let result = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        77,
        0,
        1000,
        &[],
        enc(),
    );
    // 关键断言：CRC 失败跳过 Block，返回 Ok（空结果），而非 Err 或 panic。
    assert!(
        result.is_ok(),
        "tamper 应走 CRC 跳过而非全文件 Err，got {:?}",
        result
    );
    assert!(
        result.unwrap().is_empty(),
        "被篡改的 Block 应被跳过，返回 0 行"
    );
}

#[test]
fn qa_sac_wrong_key_decrypt_auth_failure_should_skip_block() {
    // 错误密钥：CRC（覆盖密文、与密钥无关）通过，但 GCM 认证失败。
    // 按任务要求应「按 per-block 策略跳过该 Block + 告警」，而非全文件 Err。
    // 当前实现（reader.rs read_block 对 decrypt_with 用 `?`）会返回 Err(AuthFailed)，
    // 与 WAL 的「解密失败 → warn + 跳过帧」不一致，详见 QA 报告。
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_sac(&tmp.path().join("sac"), enc());

    let result = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        77,
        0,
        1000,
        &[],
        enc_wrong_key(),
    );
    // 期望（任务硬性要求）：跳过 Block、返回 Ok(空)，不返回全文件错误、不 panic。
    assert!(
        result.is_ok(),
        "GCM 认证失败应按 per-block 跳过（Ok 空结果），实际返回 Err: {:?}",
        result
    );
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
async fn qa_wal_ciphertext_tamper_skips_frame_best_effort() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new_with_encryptor(&cfg, enc()).unwrap();
    // 两次独立 write_batch → 两个独立帧（帧 0 正常，帧 1 将被篡改）。
    wal.write_batch(PointBatch {
        points: vec![DataPoint {
            table_id: 1,
            tenant_id: TenantId::DEFAULT,
            ts: 100,
            server_ts: 101,
            values: vec![(0, TypedValue::F64(1.0))],
            qualities: vec![],
            schema_version: 1,
            flags: 0,
        }],
    })
    .await
    .unwrap();
    wal.write_batch(PointBatch {
        points: vec![DataPoint {
            table_id: 2,
            tenant_id: TenantId::DEFAULT,
            ts: 200,
            server_ts: 201,
            values: vec![(0, TypedValue::F64(2.0))],
            qualities: vec![],
            schema_version: 1,
            flags: 0,
        }],
    })
    .await
    .unwrap();

    let wal_dir = cfg.get().paths.wal_dir();
    let seg = std::fs::read_dir(&wal_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut bytes = std::fs::read(&seg).unwrap();

    // 帧 0 = 37B WAL2 header + payload0；篡改帧 0 的 ct 首字节（37 + 8 + 28 = 73）。
    let tamper_at = 37 + 8 + 28;
    assert!(tamper_at < bytes.len());
    bytes[tamper_at] ^= 0xFF;
    std::fs::write(&seg, &bytes).unwrap();

    // CRC 失败 → 跳过帧 0，尽力恢复帧 1。
    let reader = WalReader::new_with_encryptor(wal_dir, enc());
    let records = reader.replay_all(1).unwrap();
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 1, "篡改帧应被跳过，仅恢复完好帧");
    assert_eq!(recovered[0].table_id, 2);
}

#[tokio::test]
async fn qa_wal_wrong_key_decrypt_failure_skips_frame_best_effort() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new_with_encryptor(&cfg, enc()).unwrap();
    wal.write_batch(PointBatch {
        points: vec![DataPoint {
            table_id: 9,
            tenant_id: TenantId::DEFAULT,
            ts: 1,
            server_ts: 2,
            values: vec![(0, TypedValue::F64(3.14))],
            qualities: vec![],
            schema_version: 1,
            flags: 0,
        }],
    })
    .await
    .unwrap();

    let wal_dir = cfg.get().paths.wal_dir();
    // 错误密钥：CRC 通过，GCM 认证失败 → warn + 跳过帧，不中断重放。
    let reader = WalReader::new_with_encryptor(wal_dir, enc_wrong_key());
    let records = reader.replay_all(1);
    assert!(records.is_ok(), "解密失败应尽力恢复（Ok），而非全文件 Err");
    assert!(records.unwrap().is_empty(), "无法解密的帧应被跳过");
}

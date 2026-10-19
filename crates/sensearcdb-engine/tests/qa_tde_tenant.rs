//! QA 独立回归验证：分租户密钥在 WAL 帧前缀上的落点（S4 Step 2）。
//!
//! 覆盖：
//! - WAL `WALE` 前缀携带 tenant_id 的 round-trip（写 tenant=1 → 读回 tenant=1）。
//! - 跨租户解密失败：篡改帧前缀 tenant 段后，用「错误租户密钥」解密必须失败
//!   （GCM tag 认证失败 → 尽力恢复跳过该帧，不返回错误明文）。

use std::path::PathBuf;
use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::encryption::{EncryptContext, EncryptError, Encryptor, SharedEncryptor};
use sensearcdb_engine::engine::{DataPoint, PointBatch};
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_engine::wal::{WalReader, WalWriter};
use sensearcdb_engine::TenantId;

/// 租户感知的 keyed-tag 加密桩：密钥派生自 `ctx.tenant_id`，
/// 使租户 1 与租户 2 的密文互不可解密（模拟 GCM 认证 tag 语义）。
struct TenantKeyedEncryptor;

fn key_of(tenant_id: TenantId) -> u8 {
    (tenant_id.as_u32() as u8).wrapping_add(1)
}

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

impl Encryptor for TenantKeyedEncryptor {
    fn encrypt(&self, plaintext: &[u8], ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        let key = key_of(ctx.tenant_id);
        let mut out = Vec::with_capacity(28 + plaintext.len());
        out.extend_from_slice(&[0xA7u8; 12]); // nonce
        out.extend_from_slice(&keyed_tag(plaintext, key));
        out.extend(plaintext.iter().map(|b| b ^ key));
        Ok(out)
    }

    fn decrypt(&self, blob: &[u8], ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        let key = key_of(ctx.tenant_id);
        if blob.len() < 28 {
            return Err(EncryptError::TooShort(blob.len(), 28));
        }
        let tag = &blob[12..28];
        let ct = &blob[28..];
        let plain: Vec<u8> = ct.iter().map(|b| b ^ key).collect();
        if keyed_tag(&plain, key) != tag {
            return Err(EncryptError::AuthFailed);
        }
        Ok(plain)
    }

    fn active_dek_version(&self, _tenant_id: TenantId) -> u32 {
        1
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

fn tenant_enc() -> SharedEncryptor {
    Some(Arc::new(TenantKeyedEncryptor))
}

fn temp_wal_config() -> (ConfigHandle, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    (ConfigHandle::new(cfg), tmp)
}

fn point(tenant_id: TenantId) -> DataPoint {
    DataPoint {
        table_id: 7,
        tenant_id,
        ts: 123_456_789,
        server_ts: 123_456_790,
        values: vec![(0, TypedValue::F64(42.5))],
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    }
}

#[tokio::test]
async fn qa_wal_tenant_prefix_roundtrip() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new_with_encryptor(&cfg, tenant_enc()).expect("create WalWriter");
    wal.write_batch(PointBatch {
        points: vec![point(TenantId(1))],
    })
    .await
    .expect("WAL write failed");

    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new_with_encryptor(wal_dir, tenant_enc());
    let records = reader.replay_all(1).expect("replay failed");

    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].tenant_id, TenantId(1));
    assert_eq!(recovered[0].values[0], (0, TypedValue::F64(42.5)));
}

#[tokio::test]
async fn qa_wal_cross_tenant_decrypt_fails() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new_with_encryptor(&cfg, tenant_enc()).expect("create WalWriter");
    wal.write_batch(PointBatch {
        points: vec![point(TenantId(1))],
    })
    .await
    .expect("WAL write failed");

    let wal_dir = cfg.get().paths.wal_dir();

    // 篡改帧前缀的 tenant 段（payload 起始于 37B 帧头之后；tenant 位于 magic(4B) 之后）。
    let mut tampered = false;
    for entry in std::fs::read_dir(&wal_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().map_or(false, |e| e == "wal") {
            let mut bytes = std::fs::read(&path).unwrap();
            if let Some(pos) = bytes.windows(4).position(|w| w == b"WALE") {
                // tenant 段 = magic 之后 4 字节，改写为 tenant 2。
                let tenant_off = pos + 4;
                bytes[tenant_off..tenant_off + 4].copy_from_slice(&2u32.to_le_bytes());
                std::fs::write(&path, &bytes).unwrap();
                tampered = true;
            }
        }
    }
    assert!(tampered, "WALE prefix not found");

    // 用租户感知加密器重放：帧前缀被改为 tenant 2，但密文是 tenant 1 密钥加密 →
    // GCM 认证失败，帧被尽力恢复跳过，得到 0 条数据点。
    let reader = WalReader::new_with_encryptor(wal_dir, tenant_enc());
    let records = reader.replay_all(1).expect("replay failed");
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(
        recovered.len(),
        0,
        "cross-tenant decrypt must not yield plaintext"
    );
}

//! QA 独立回归验证（阶段 2 · 改动 B）：WAL 多类型往返。
//!
//! 独立于工程师在 `wal/writer.rs` 内的 `serialize_points`/`deserialize_points`
//! 单元测试，本文件走**真实文件 I/O 路径**：
//! `WalWriter`（serialize → LZ4 压缩 → 段文件写入 + fsync）
//!   → `WalReader`（帧头解析 → CRC 校验 → LZ4 解压 → deserialize）。
//!
//! 验证 F32 / I64 / U64 / I32 / Bool / String / Enum16 各类型写盘后读回类型一致，
//! 且 U64 大值（> i64::MAX）不再 `as f64` 折叠溢出。

use std::path::PathBuf;

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::engine::{DataPoint, PointBatch};
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_engine::wal::{WalReader, WalWriter};

/// 构造一个使用临时 wal_dir 的 ConfigHandle；TempDir 保证测试结束自动清理。
fn temp_wal_config() -> (ConfigHandle, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    (ConfigHandle::new(cfg), tmp)
}

/// 全类型（F32/I64/U64/I32/Bool/String/Enum16）经 WAL 文件往返后类型与值均保真。
#[tokio::test]
async fn qa_wal_multitype_roundtrip_via_file() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new(&cfg).expect("create WalWriter");

    let values = vec![
        (0u16, TypedValue::F32(1.5)),
        (1u16, TypedValue::I64(-42)),
        // u64::MAX：若被折叠为 f64 会丢失精度，此处必须原样保真。
        (2u16, TypedValue::U64(18_446_744_073_709_551_615)),
        (3u16, TypedValue::I32(-7)),
        (4u16, TypedValue::Bool(true)),
        (5u16, TypedValue::String("open".to_string())),
        (6u16, TypedValue::Enum16(65_535)),
    ];
    let point = DataPoint {
        table_id: 7,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        ts: 123_456_789,
        server_ts: 123_456_790,
        values,
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    };
    let batch = PointBatch {
        points: vec![point],
    };

    wal.write_batch(batch).await.expect("WAL write failed");

    // 从真实段文件读回（CRC + 解压 + 反序列化）。
    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new(wal_dir);
    let records = reader.replay_all(1).expect("WAL replay failed");

    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 1, "应恰好重放一个数据点");
    let dp = recovered[0];

    assert_eq!(dp.table_id, 7);
    assert_eq!(dp.ts, 123_456_789);
    assert_eq!(dp.values.len(), 7, "7 个列应全部保留");
    assert_eq!(dp.values[0], (0, TypedValue::F32(1.5)));
    assert_eq!(dp.values[1], (1, TypedValue::I64(-42)));
    assert_eq!(
        dp.values[2],
        (2, TypedValue::U64(18_446_744_073_709_551_615)),
        "U64 大值必须无损往返，不得溢出/折叠为 f64"
    );
    assert_eq!(dp.values[3], (3, TypedValue::I32(-7)));
    assert_eq!(dp.values[4], (4, TypedValue::Bool(true)));
    assert_eq!(dp.values[5], (5, TypedValue::String("open".to_string())));
    assert_eq!(dp.values[6], (6, TypedValue::Enum16(65_535)));
}

/// 边界：Bool=false 与空串/Unicode 字符串往返保真（易被 0 值/长度 0 特例破坏）。
#[tokio::test]
async fn qa_wal_bool_false_and_string_edges_roundtrip() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new(&cfg).expect("create WalWriter");

    let values = vec![
        (0u16, TypedValue::Bool(false)),
        (1u16, TypedValue::Bool(true)),
        (2u16, TypedValue::String(String::new())), // 空串
        (3u16, TypedValue::String("温控-单元①".to_string())), // 多字节 UTF-8
    ];
    let point = DataPoint {
        table_id: 9,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        ts: 1,
        server_ts: 2,
        values,
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
    let records = WalReader::new(wal_dir).replay_all(1).unwrap();
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 1);
    let dp = recovered[0];
    assert_eq!(dp.values[0], (0, TypedValue::Bool(false)));
    assert_eq!(dp.values[1], (1, TypedValue::Bool(true)));
    assert_eq!(dp.values[2], (2, TypedValue::String(String::new())));
    assert_eq!(
        dp.values[3],
        (3, TypedValue::String("温控-单元①".to_string())),
        "多字节 UTF-8 字符串必须原样往返"
    );
}

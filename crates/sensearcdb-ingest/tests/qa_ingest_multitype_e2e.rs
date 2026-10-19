//! QA 独立回归验证（阶段 2 · 改动 B）：ingest → engine 多类型直通（端到端）。
//!
//! 验证链路：ingest `PointBatch`（列式）→ `EngineDirectWriter::write_batch`
//!   → `convert_inline`（不再 `as f64` 折叠）→ engine `DataPoint.values: Vec<(u16, TypedValue)>`
//!   → `Engine::write_batch`（WAL + MemTable）→ `MemTable::get_typed_value` 读回。
//!
//! 重点：I64 负值不丢、U64 大值不溢出、String 返回原串、Bool 不退化。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::engine::Engine;
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_ingest::batch::ColVec;
use sensearcdb_ingest::{EngineDirectWriter, EngineWriter, PointBatch};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 构造使用独立临时目录的 Engine（避免并行测试互相污染）。
async fn temp_engine() -> (Arc<Engine>, PathBuf) {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "sensearcdb_qa_ingest_e2e_{}_{}",
        std::process::id(),
        n
    ));
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = dir.to_string_lossy().into_owned();

    std::fs::create_dir_all(cfg.paths.data_dir.clone()).unwrap();
    std::fs::create_dir_all(cfg.paths.wal_dir()).unwrap();
    std::fs::create_dir_all(cfg.paths.sac_dir()).unwrap();

    let handle = ConfigHandle::new(cfg);
    let engine = Arc::new(Engine::new(&handle, None).await.expect("create engine"));
    (engine, dir)
}

/// 多类型全链路直通：I64 / U64 / I32 / F32 / Bool / String / F64，两行数据逐一保真。
#[tokio::test]
async fn qa_ingest_multitype_e2e_type_fidelity() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(11);
    batch.timestamps = vec![1_000, 2_000];
    batch.columns = vec![
        ColVec::I64(vec![-123_456_789, 9_876_543_210]),
        ColVec::U64(vec![18_446_744_073_709_551_615, 1]), // u64::MAX 与 1
        ColVec::I32(vec![-7, 7]),
        ColVec::F32(vec![1.5, -2.5]),
        ColVec::Bool(vec![0, 1]),
        ColVec::String(vec!["running".to_string(), "stopped".to_string()]),
        ColVec::F64(vec![3.14159, -2.71828]),
    ];
    batch.column_names = (0..7).map(|i| format!("c{i}")).collect();
    batch.column_ids = (0..7).collect();
    batch.qualities = vec![0xC0; 2];
    batch.flags = vec![0; 2];

    let receipt = writer.write_batch(batch).await.expect("write failed");
    assert_eq!(receipt.points_written, 2);

    let chunk = engine
        .memtable
        .active_snapshot()
        .get_chunk(11)
        .expect("chunk should exist");

    // 行 0
    assert_eq!(
        chunk.get_typed_value(0, 0),
        Some(TypedValue::I64(-123_456_789))
    );
    assert_eq!(
        chunk.get_typed_value(1, 0),
        Some(TypedValue::U64(18_446_744_073_709_551_615)),
        "U64 大值必须保真，不得折叠为 f64 溢出"
    );
    assert_eq!(chunk.get_typed_value(2, 0), Some(TypedValue::I32(-7)));
    assert_eq!(chunk.get_typed_value(3, 0), Some(TypedValue::F32(1.5)));
    assert_eq!(chunk.get_typed_value(4, 0), Some(TypedValue::Bool(false)));
    assert_eq!(
        chunk.get_typed_value(5, 0),
        Some(TypedValue::String("running".to_string()))
    );
    assert_eq!(chunk.get_typed_value(6, 0), Some(TypedValue::F64(3.14159)));

    // 行 1
    assert_eq!(
        chunk.get_typed_value(0, 1),
        Some(TypedValue::I64(9_876_543_210))
    );
    assert_eq!(chunk.get_typed_value(1, 1), Some(TypedValue::U64(1)));
    assert_eq!(chunk.get_typed_value(2, 1), Some(TypedValue::I32(7)));
    assert_eq!(chunk.get_typed_value(3, 1), Some(TypedValue::F32(-2.5)));
    assert_eq!(chunk.get_typed_value(4, 1), Some(TypedValue::Bool(true)));
    assert_eq!(
        chunk.get_typed_value(5, 1),
        Some(TypedValue::String("stopped".to_string()))
    );
    assert_eq!(chunk.get_typed_value(6, 1), Some(TypedValue::F64(-2.71828)));

    let _ = std::fs::remove_dir_all(&dir);
}

/// 边界：U64 > i64::MAX 的值在直通层不得被 `as f64` 折叠（Simple8b 大值 bug 的直通层验证）。
#[tokio::test]
async fn qa_ingest_u64_beyond_i64_max_three_rows() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let boundary: u64 = 1 << 63;
    let vals = vec![boundary, boundary + 1, u64::MAX];
    let mut batch = PointBatch::new(22);
    batch.timestamps = vec![1, 2, 3];
    batch.columns = vec![ColVec::U64(vals.clone())];
    batch.column_names = vec!["u64val".to_string()];
    batch.column_ids = vec![0];
    batch.qualities = vec![0xC0; 3];
    batch.flags = vec![0; 3];

    writer.write_batch(batch).await.expect("write failed");

    let chunk = engine
        .memtable
        .active_snapshot()
        .get_chunk(22)
        .expect("chunk should exist");

    for (i, expect) in vals.iter().enumerate() {
        assert_eq!(
            chunk.get_typed_value(0, i),
            Some(TypedValue::U64(*expect)),
            "row {i}: U64 值 {expect} 未无损还原"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

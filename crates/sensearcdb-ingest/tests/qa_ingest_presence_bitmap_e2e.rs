//! QA 独立回归验证（fresh eyes）：治碎片化 presence bitmap 语义。
//!
//! 验证链路：Go adapter 整桶单批 + presence 位图 → gRPC proto → 服务端
//! proto_to_point_batch（读 presence）→ convert_to_engine_batch（presence=false
//! 跳过不 emit）→ engine memtable push_absent（presence=false）→ get_typed_value
//! 读回 None。
//!
//! 本文件独立设计边界用例，不复用工程师自测的断言形状，重点验证：
//!   ① 缺 1 列稀疏行读回 None（不是 0）
//!   ② 全密集行（presence 空）读回全部值
//!   ③ 混合稀疏/密集行在同一批不错位
//!   ④ presence 长度 ≠ row_count 的防御（不 panic、不越界、不错位）
//!   ⑤ 旧客户端无 presence 字段的兼容（空位图 = 全存在）

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::engine::Engine;
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_ingest::batch::ColVec;
use sensearcdb_ingest::{EngineDirectWriter, EngineWriter, PointBatch};

static COUNTER: AtomicU64 = AtomicU64::new(0);

async fn temp_engine() -> (Arc<Engine>, PathBuf) {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "sensearcdb_qa_ingest_presence_{}_{}",
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

fn f64_col(values: Vec<f64>) -> ColVec {
    ColVec::F64(values)
}

/// ① 缺 1 列稀疏行：col_0 有值、col_1 缺失（presence=false），读回 col_1 必须 None。
#[tokio::test]
async fn qa_presence_sparse_row_reads_back_none() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(1);
    batch.timestamps = vec![100];
    batch.columns = vec![f64_col(vec![5.0]), f64_col(vec![0.0])]; // col_1 占位 0.0
    batch.column_names = vec!["table_1.col_0".into(), "table_1.col_1".into()];
    batch.column_ids = vec![0, 1];
    batch.column_presence = vec![vec![true], vec![false]]; // 第 2 列缺失
    batch.qualities = vec![0xC0; 1];
    batch.flags = vec![0; 1];

    let receipt = writer.write_batch(batch).await.expect("write failed");
    assert_eq!(receipt.points_written, 1);

    let chunk = engine.memtable.active_snapshot().get_chunk(1).expect("chunk");
    assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(5.0)));
    // 关键：缺失列读回 None，绝不能是 Some(F64(0.0))。
    assert_eq!(
        chunk.get_typed_value(1, 0),
        None,
        "presence=false 的缺失列必须读回 None，不得误写成 0"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// ② 全密集行（presence 空 = 全存在）：所有列所有行都必须读出正确值。
#[tokio::test]
async fn qa_presence_dense_empty_bitmap_reads_all_values() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(2);
    batch.timestamps = vec![100, 200];
    batch.columns = vec![f64_col(vec![1.0, 2.0]), f64_col(vec![10.0, 20.0])];
    batch.column_names = vec!["table_2.col_0".into(), "table_2.col_1".into()];
    batch.column_ids = vec![0, 1];
    batch.column_presence = Vec::new(); // 空 = 全存在（旧客户端 / 密集列）
    batch.qualities = vec![0xC0; 2];
    batch.flags = vec![0; 2];

    let receipt = writer.write_batch(batch).await.expect("write failed");
    assert_eq!(receipt.points_written, 2);

    let chunk = engine.memtable.active_snapshot().get_chunk(2).expect("chunk");
    assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
    assert_eq!(chunk.get_typed_value(0, 1), Some(TypedValue::F64(2.0)));
    assert_eq!(chunk.get_typed_value(1, 0), Some(TypedValue::F64(10.0)));
    assert_eq!(chunk.get_typed_value(1, 1), Some(TypedValue::F64(20.0)));

    let _ = std::fs::remove_dir_all(&dir);
}

/// ③ 混合稀疏/密集行在同一批：值必须按行对齐，不错位。
///
/// 行 0 密集（col_0=1.0, col_1=10.0）；行 1 稀疏（col_0=2.0, col_1 缺失）。
/// 若 presence 位图错位（例如把「行 0 的 col_1 缺失」误判到行 1），会读到错误值。
#[tokio::test]
async fn qa_presence_mixed_sparse_dense_rows_do_not_misalign() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(3);
    batch.timestamps = vec![100, 200];
    batch.columns = vec![
        f64_col(vec![1.0, 2.0]),   // col_0 两行都有值
        f64_col(vec![10.0, 0.0]),  // col_1 行 1 占位 0.0
    ];
    batch.column_names = vec!["table_3.col_0".into(), "table_3.col_1".into()];
    batch.column_ids = vec![0, 1];
    batch.column_presence = vec![
        vec![true, true],  // col_0 全存在（等价于可省略）
        vec![true, false], // col_1 行 0 存在、行 1 缺失
    ];
    batch.qualities = vec![0xC0; 2];
    batch.flags = vec![0; 2];

    let receipt = writer.write_batch(batch).await.expect("write failed");
    assert_eq!(receipt.points_written, 2);

    let chunk = engine.memtable.active_snapshot().get_chunk(3).expect("chunk");
    // 行 0：col_0=1.0、col_1=10.0（都密集）。
    assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
    assert_eq!(chunk.get_typed_value(1, 0), Some(TypedValue::F64(10.0)));
    // 行 1：col_0=2.0（有值）、col_1=None（缺失，且不得错位成行 0 的 10.0）。
    assert_eq!(chunk.get_typed_value(0, 1), Some(TypedValue::F64(2.0)));
    assert_eq!(
        chunk.get_typed_value(1, 1),
        None,
        "混合批次中缺失列必须精确落到正确的行，不得错位"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// ④ presence 长度 ≠ row_count 的防御：不 panic、不越界。
///
/// - 位图短于 row_count：越界位按「存在」兜底（fail-open，缺失位不丢数据）。
/// - 位图长于 row_count：多余位忽略。
/// 两种情况下都不允许 panic 或下标越界导致的错位。
#[tokio::test]
async fn qa_presence_bitmap_length_mismatch_is_defensive() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    // 行数 3，但 col_1 位图只有 2 位（短 1）。
    let mut batch = PointBatch::new(4);
    batch.timestamps = vec![100, 200, 300];
    batch.columns = vec![
        f64_col(vec![1.0, 2.0, 3.0]),
        f64_col(vec![10.0, 0.0, 30.0]), // 行 1 占位 0.0
    ];
    batch.column_names = vec!["table_4.col_0".into(), "table_4.col_1".into()];
    batch.column_ids = vec![0, 1];
    batch.column_presence = vec![
        Vec::new(),              // col_0 空位图 = 全存在
        vec![true, false],       // col_1 位图短于 3 行（缺第 3 位）
    ];
    batch.qualities = vec![0xC0; 3];
    batch.flags = vec![0; 3];

    // 不 panic、写入成功。
    let receipt = writer.write_batch(batch).await.expect("写路径不得因位图长度不一致而 panic");
    assert_eq!(receipt.points_written, 3);

    let chunk = engine.memtable.active_snapshot().get_chunk(4).expect("chunk");
    // col_1 行 1 位图 false → None；行 2 位图越界 → 兜底按存在（30.0 读出）。
    assert_eq!(chunk.get_typed_value(1, 0), Some(TypedValue::F64(10.0)));
    assert_eq!(chunk.get_typed_value(1, 1), None);
    assert_eq!(
        chunk.get_typed_value(1, 2),
        Some(TypedValue::F64(30.0)),
        "位图越界位按 fail-open 兜底为存在，值 30.0 不得丢失"
    );
    // col_0 全存在，逐行正确。
    assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
    assert_eq!(chunk.get_typed_value(0, 1), Some(TypedValue::F64(2.0)));
    assert_eq!(chunk.get_typed_value(0, 2), Some(TypedValue::F64(3.0)));

    let _ = std::fs::remove_dir_all(&dir);
}

/// ⑤ 旧客户端无 presence 字段（column_presence 空 outer vec）：全列全行按存在处理。
/// 等价于「全密集」路径，任何值都不得被当作缺失而丢失。
#[tokio::test]
async fn qa_presence_legacy_client_no_field_treats_all_present() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(5);
    batch.timestamps = vec![100, 200];
    batch.columns = vec![f64_col(vec![7.0, 8.0]), f64_col(vec![70.0, 80.0])];
    batch.column_names = vec!["table_5.col_0".into(), "table_5.col_1".into()];
    batch.column_ids = vec![0, 1];
    batch.column_presence = Vec::new(); // 旧客户端完全不发 presence
    batch.qualities = vec![0xC0; 2];
    batch.flags = vec![0; 2];

    let receipt = writer.write_batch(batch).await.expect("write failed");
    assert_eq!(receipt.points_written, 2);

    let chunk = engine.memtable.active_snapshot().get_chunk(5).expect("chunk");
    assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(7.0)));
    assert_eq!(chunk.get_typed_value(0, 1), Some(TypedValue::F64(8.0)));
    assert_eq!(chunk.get_typed_value(1, 0), Some(TypedValue::F64(70.0)));
    assert_eq!(chunk.get_typed_value(1, 1), Some(TypedValue::F64(80.0)));

    let _ = std::fs::remove_dir_all(&dir);
}

/// ⑤-bis 旧客户端「无 presence」与「全 true presence」两种等价表示结果一致。
#[tokio::test]
async fn qa_presence_empty_vs_all_true_equivalent() {
    let (engine, dir) = temp_engine().await;

    let write = |table: u32, presence: Vec<Vec<bool>>| {
        let engine = engine.clone();
        let writer = EngineDirectWriter::new(engine);
        async move {
            let mut batch = PointBatch::new(table);
            batch.timestamps = vec![100];
            batch.columns = vec![f64_col(vec![42.0])];
            batch.column_names = vec![format!("table_{table}.col_0")];
            batch.column_ids = vec![0];
            batch.column_presence = presence;
            batch.qualities = vec![0xC0; 1];
            batch.flags = vec![0; 1];
            writer.write_batch(batch).await.expect("write")
        }
    };

    // 表 6：空位图；表 7：全 true 位图。
    write(6, Vec::new()).await;
    write(7, vec![vec![true]]).await;

    let snap = engine.memtable.active_snapshot();
    let c6 = snap.get_chunk(6).expect("chunk 6");
    let c7 = snap.get_chunk(7).expect("chunk 7");
    assert_eq!(c6.get_typed_value(0, 0), Some(TypedValue::F64(42.0)));
    assert_eq!(c7.get_typed_value(0, 0), Some(TypedValue::F64(42.0)));

    let _ = std::fs::remove_dir_all(&dir);
}

//! QA gRPC 端到端 typed 往返回归（P7）。
//!
//! 验证「类型保真」穿透 proto 序列化 + Arrow IPC 编解码：
//! 写数据（ingest `EngineDirectWriter` 或 engine `MemTable` 直接插入）→
//! `GrpcQueryService::query`（SQL → 引擎 typed 路径）→ 解码
//! `QueryResponse.arrow_batch`（Arrow IPC StreamReader）→ 断言 field 类型与值。
//!
//! 核心判别器：TSBS `status(BIGINT)` 大值（2^53+1）经 gRPC 后必须是 `Int64` 且
//! 值精确不折 f64；U64 大值 → `UInt64`；F32 → `Float32`；`None` → null bitmap。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt16Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use tokio_stream::StreamExt;
use tonic::Request;

use sensearcdb_common::{AppConfig, ConfigHandle, TenantId};
use sensearcdb_engine::engine::Engine;
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_ingest::batch::ColVec;
use sensearcdb_ingest::grpc::auth::GrpcTenantResolver;
use sensearcdb_ingest::grpc::query_service::GrpcQueryService;
use sensearcdb_ingest::proto::query_service_server::QueryService;
use sensearcdb_ingest::proto::QueryRequest;
use sensearcdb_ingest::{EngineDirectWriter, EngineWriter, PointBatch};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 构造使用独立临时目录的 Engine（避免并行测试互相污染）。
async fn temp_engine() -> (Arc<Engine>, PathBuf) {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "sensearcdb_qa_grpc_typed_e2e_{}_{}",
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

/// 解码单个 `QueryResponse.arrow_batch`（Arrow IPC stream：schema + 一个 batch）。
fn decode_batch(bytes: &[u8]) -> RecordBatch {
    use arrow::ipc::reader::StreamReader;

    let cursor = std::io::Cursor::new(bytes.to_vec());
    let mut reader = StreamReader::try_new(cursor, None).expect("decode arrow ipc stream");
    reader
        .next()
        .expect("stream contains one batch")
        .expect("decode record batch")
}

/// 直接调用 `GrpcQueryService::query`（trait 方法，无需起端口），收集并解码所有 batch。
async fn run_query(engine: &Arc<Engine>, sql: &str) -> Vec<RecordBatch> {
    let resolver: GrpcTenantResolver = Arc::new(|_m| Some(TenantId::DEFAULT));
    let svc = GrpcQueryService::new(engine.clone()).with_tenant_resolver(resolver);

    let req = Request::new(QueryRequest {
        query: sql.to_string(),
        params: Default::default(),
    });
    let resp = svc.query(req).await.expect("query rpc");
    let mut stream = resp.into_inner();

    let mut batches = Vec::new();
    while let Some(item) = stream.next().await {
        let resp = item.expect("stream item");
        let batch = decode_batch(&resp.arrow_batch);
        assert_eq!(
            batch.num_rows() as u32,
            resp.row_count,
            "row_count 必须与 batch 行数一致"
        );
        batches.push(batch);
    }
    batches
}

/// T1：status(I64) 大值（2^53+1）经 gRPC → `Int64` 且值精确不折 f64。
#[tokio::test]
async fn qa_grpc_query_i64_status_large_value_not_folded() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(42);
    batch.timestamps = vec![1_000];
    batch.columns = vec![ColVec::I64(vec![9_007_199_254_740_993])]; // 2^53 + 1
    batch.column_names = vec!["col_0".to_string()];
    batch.column_ids = vec![0];
    batch.qualities = vec![0xC0; 1];
    batch.flags = vec![0; 1];
    writer.write_batch(batch).await.expect("write failed");

    let batches = run_query(&engine, "SELECT ts, col_0 FROM table_42").await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // field(0)=ts → Int64；field(1)=status → Int64（非 Float64）。
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
    let col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        col.value(0),
        9_007_199_254_740_993i64,
        "status(2^53+1) 必须精确，f64 折叠会变 2^53"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// T2：U64 大值（u64::MAX）经 gRPC → `UInt64` 且值精确无损。
#[tokio::test]
async fn qa_grpc_query_u64_large_value_not_folded() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(43);
    batch.timestamps = vec![1_000];
    batch.columns = vec![ColVec::U64(vec![u64::MAX])];
    batch.column_names = vec!["col_0".to_string()];
    batch.column_ids = vec![0];
    batch.qualities = vec![0xC0; 1];
    batch.flags = vec![0; 1];
    writer.write_batch(batch).await.expect("write failed");

    let batches = run_query(&engine, "SELECT col_0 FROM table_43").await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(col.value(0), u64::MAX, "U64 大值必须无损");

    let _ = std::fs::remove_dir_all(&dir);
}

/// T3：F32 经 gRPC → `Float32`（不升 f64）。
#[tokio::test]
async fn qa_grpc_query_f32_not_promoted() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(44);
    batch.timestamps = vec![1_000];
    batch.columns = vec![ColVec::F32(vec![1.5])];
    batch.column_names = vec!["col_0".to_string()];
    batch.column_ids = vec![0];
    batch.qualities = vec![0xC0; 1];
    batch.flags = vec![0; 1];
    writer.write_batch(batch).await.expect("write failed");

    let batches = run_query(&engine, "SELECT col_0 FROM table_44").await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    assert_eq!(batch.schema().field(0).data_type(), &DataType::Float32);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(col.value(0), 1.5f32);

    let _ = std::fs::remove_dir_all(&dir);
}

/// T4：多类型混查（一表 8 列不同类型）——逐列 field 类型与映射表对应。
///
/// Enum16 无法经 ingest `PointBatch`（其 `ColVec` 无 Enum16 变体），此处直接
/// 插入 engine MemTable 以覆盖完整 8 类型映射（含 `Enum16 → UInt16`）。
#[tokio::test]
async fn qa_grpc_query_multi_type_projection() {
    let (engine, dir) = temp_engine().await;

    engine.memtable.insert(
        45,
        1_000,
        &[
            (0, TypedValue::F64(3.14)),
            (1, TypedValue::F32(1.5)),
            (2, TypedValue::I64(-7)),
            (3, TypedValue::U64(42)),
            (4, TypedValue::I32(-2_000_000_000)),
            (5, TypedValue::Bool(true)),
            (6, TypedValue::String("on".to_string())),
            (7, TypedValue::Enum16(65535)), // 0xFFFF：验证 UInt16 直通，不折 Int16 负数
        ],
        &[(0, 0xC0)],
        0,
    );

    let batches = run_query(
        &engine,
        "SELECT ts, col_0, col_1, col_2, col_3, col_4, col_5, col_6, col_7 FROM table_45",
    )
    .await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 9);

    let schema = batch.schema();
    let fields = schema.fields();
    assert_eq!(fields[0].data_type(), &DataType::Int64, "ts");
    assert_eq!(fields[1].data_type(), &DataType::Float64);
    assert_eq!(fields[2].data_type(), &DataType::Float32);
    assert_eq!(fields[3].data_type(), &DataType::Int64);
    assert_eq!(fields[4].data_type(), &DataType::UInt64);
    assert_eq!(fields[5].data_type(), &DataType::Int32);
    assert_eq!(fields[6].data_type(), &DataType::Boolean);
    assert_eq!(fields[7].data_type(), &DataType::Utf8);
    assert_eq!(fields[8].data_type(), &DataType::UInt16, "Enum16 → UInt16");

    // 抽查几个值与类型保真。
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        3.14
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        -2_000_000_000
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0),
        true
    );
    assert_eq!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "on"
    );
    assert_eq!(
        batch
            .column(8)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(0),
        65535
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// T5：NULL → null bitmap——稀疏列缺失行 `is_null == true`，且非 0/空串/占位值。
#[tokio::test]
async fn qa_grpc_query_null_bitmap() {
    let (engine, dir) = temp_engine().await;

    // col_0 三行全 present；col_1 仅第 1、3 行 present，第 2 行稀疏缺失 → null。
    engine.memtable.insert(
        46,
        100,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(10.0))],
        &[(0, 0xC0)],
        0,
    );
    engine
        .memtable
        .insert(46, 200, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);
    engine.memtable.insert(
        46,
        300,
        &[(0, TypedValue::F64(3.0)), (1, TypedValue::F64(30.0))],
        &[(0, 0xC0)],
        0,
    );

    let batches = run_query(&engine, "SELECT ts, col_0, col_1 FROM table_46").await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);

    let col1 = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(!col1.is_null(0), "第 1 行 col_1 present");
    assert!(col1.is_null(1), "第 2 行 col_1 稀疏缺失 → null bitmap");
    assert!(!col1.is_null(2), "第 3 行 col_1 present");
    assert_eq!(col1.value(0), 10.0);
    assert_eq!(col1.value(2), 30.0);
    assert_eq!(col1.null_count(), 1, "稀疏列 null 数量应为 1（第 2 行）");

    let _ = std::fs::remove_dir_all(&dir);
}

/// T6：聚合保真——`count(*) → UInt64`、`sum(U64 大值) → UInt64` 且 2^53+1 精确。
#[tokio::test]
async fn qa_grpc_query_aggregate_count_and_sum_u64() {
    let (engine, dir) = temp_engine().await;
    let writer = EngineDirectWriter::new(engine.clone());

    let mut batch = PointBatch::new(48);
    batch.timestamps = vec![1_000];
    batch.columns = vec![ColVec::U64(vec![9_007_199_254_740_993])]; // 2^53 + 1
    batch.column_names = vec!["col_0".to_string()];
    batch.column_ids = vec![0];
    batch.qualities = vec![0xC0; 1];
    batch.flags = vec![0; 1];
    writer.write_batch(batch).await.expect("write failed");

    let batches = run_query(&engine, "SELECT count(*), sum(col_0) FROM table_48").await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::UInt64,
        "count → UInt64"
    );
    assert_eq!(
        batch.schema().field(1).data_type(),
        &DataType::UInt64,
        "sum(U64) → UInt64"
    );

    let cnt = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(cnt.value(0), 1);

    let sum = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(
        sum.value(0),
        9_007_199_254_740_993u64,
        "sum(U64 大值) 必须精确，f64 折叠会变 2^53"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// T7（边界固化）：混型 CASE（I64/F64 分支）显式降级为 `Float64`——设计内行为，非 bug。
#[tokio::test]
async fn qa_grpc_query_case_mixed_type_degrades_to_f64() {
    let (engine, dir) = temp_engine().await;

    engine
        .memtable
        .insert(49, 100, &[(0, TypedValue::I64(0))], &[(0, 0xC0)], 0);
    engine
        .memtable
        .insert(49, 200, &[(0, TypedValue::I64(1))], &[(0, 0xC0)], 0);

    let batches = run_query(
        &engine,
        "SELECT CASE WHEN col_0 = 0 THEN 1 ELSE 1.5 END FROM table_49",
    )
    .await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Float64,
        "混型 CASE 显式降级 Float64（文档化预期）"
    );
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(col.value(0), 1.0, "THEN 1(I64) 应提升为 1.0，非 NULL");
    assert_eq!(col.value(1), 1.5);

    let _ = std::fs::remove_dir_all(&dir);
}

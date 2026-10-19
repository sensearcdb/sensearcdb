//! qa-backpressure 独立验证：引擎 write_stalled 背压拒绝的语义正确性。
//!
//! 不信任工程师自测，fresh eyes 独立构造边界用例：
//! - A. write_stalled 时必须返回 `SenseArcError::MemTableFull`（精确变体，非 Internal 字符串）。
//! - B. 背压拒绝必须幂等：拒绝发生在 WAL 落盘 / MemTable 落地之前，被拒 batch
//!      无任何副作用（未写 WAL、未进 memtable），重试同一批不重复。
//! - C. 非背压错误（配额拒绝）不得误判为 MemTableFull。

use sensearcdb_common::{AppConfig, ConfigHandle, SenseArcError, TenantId};
use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch};
use sensearcdb_engine::memtable::TypedValue;

fn point(table_id: u32, ts: i64, v: f64) -> DataPoint {
    DataPoint {
        table_id,
        tenant_id: TenantId::DEFAULT,
        ts,
        server_ts: ts + 1,
        values: vec![(0, TypedValue::F64(v))],
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    }
}

/// 构造一个 immutable 队列深度超过硬阈值（64）的 stalled 引擎。
///
/// 通过写 1 点 + force_freeze 重复 65 次制造 65 个 immutable（> hard_limit=64），
/// 触发 `write_stalled() == true`。返回 `(engine, _tmpdir)` 保持数据目录存活。
async fn stalled_engine() -> (Engine, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    let handle = ConfigHandle::new(cfg);
    let engine = Engine::new(&handle, None).await.expect("engine");
    for i in 0..65 {
        engine
            .write_batch(PointBatch {
                points: vec![point(7, i * 100, i as f64)],
            })
            .await
            .expect("seed write");
        engine.force_freeze();
    }
    (engine, tmp)
}

/// A. write_stalled → 精确返回 `MemTableFull`（不是 `Internal`）。
#[tokio::test]
async fn qa_backpressure_write_stalled_returns_memtable_full() {
    let (engine, _tmp) = stalled_engine().await;
    let err = engine
        .write_batch(PointBatch {
            points: vec![point(7, 9999, 42.0)],
        })
        .await
        .expect_err("stalled write must reject");
    assert!(
        matches!(err, SenseArcError::MemTableFull),
        "expected SenseArcError::MemTableFull, got {err:?}"
    );
}

/// B. 背压拒绝幂等：被拒 batch 无副作用，重试同一批不重复。
#[tokio::test]
async fn qa_backpressure_rejection_is_idempotent() {
    let (engine, _tmp) = stalled_engine().await;

    // 1. stalled 时拒绝 ts=9999。
    let err = engine
        .write_batch(PointBatch {
            points: vec![point(7, 9999, 42.0)],
        })
        .await
        .expect_err("stalled write must reject");
    assert!(matches!(err, SenseArcError::MemTableFull));

    // 2. 排空 immutable（解除 stall）。
    engine.flush_immutable_sync().await.expect("drain immutable");

    // 3. 被拒的 ts=9999 不得可见（证明拒绝发生在 WAL/MemTable 之前，无副作用）。
    let (_cols, tss) = engine
        .query_table_columns(TenantId::DEFAULT, 7, &[0], 9999, 9999)
        .await
        .expect("query rejected ts");
    assert!(
        tss.is_empty(),
        "被拒 batch 不得落 WAL/MemTable；排空后查询 ts=9999 应为空，实际 {tss:?}"
    );

    // 4. 重试同一批（解除 stall 后）→ 恰好写 1 点，不重复。
    engine
        .write_batch(PointBatch {
            points: vec![point(7, 9999, 42.0)],
        })
        .await
        .expect("retry after un-stall must succeed");
    let (_cols, tss) = engine
        .query_table_columns(TenantId::DEFAULT, 7, &[0], 9999, 9999)
        .await
        .expect("query retried ts");
    assert_eq!(tss, vec![9999], "重试后应恰好 1 点，不得重复");
}

/// C. 非背压错误（配额拒绝）不得返回 MemTableFull。
///
/// 通过注入一个恒拒绝的 quota gate 验证：`check_quota` 在 `write_stalled` 之后、
/// 但仍是 WAL 之前的独立闸口，返回 `QuotaExceeded` 而非 `MemTableFull`。
#[tokio::test]
async fn qa_backpressure_quota_reject_is_not_memtable_full() {
    use sensearcdb_engine::quota::{QuotaSnapshot, QuotaSource, TenantQuotaGate};

    struct RejectAll;
    impl QuotaSource for RejectAll {
        fn snapshot(&self, _tenant: TenantId) -> Option<QuotaSnapshot> {
            Some(QuotaSnapshot {
                points_limit: Some(0),
                storage_limit_bytes: None,
                write_rate_limit: None,
                warn_ratio: 0.0,
            })
        }
    }

    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    let handle = ConfigHandle::new(cfg);
    let engine = Engine::new(&handle, None).await.expect("engine");
    engine.set_quota_gate(std::sync::Arc::new(TenantQuotaGate::new(std::sync::Arc::new(
        RejectAll,
    ))));

    let err = engine
        .write_batch(PointBatch {
            points: vec![point(7, 1, 1.0)],
        })
        .await
        .expect_err("quota reject must fail");
    assert!(
        !matches!(err, SenseArcError::MemTableFull),
        "配额拒绝不得误判为 MemTableFull，实际 {err:?}"
    );
    assert!(
        matches!(err, SenseArcError::QuotaExceeded(_)),
        "配额拒绝应为 QuotaExceeded，实际 {err:?}"
    );
}

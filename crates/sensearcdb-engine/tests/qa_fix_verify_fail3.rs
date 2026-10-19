//! 独立 QA 验证（FAIL-3）：`write_batch` 的 `wal_seq_start` 不被空子批的 0 哨兵污染。
//!
//! 覆盖边界：三子批全空 / 纯 group / 纯 durable / 纯 async / 混档，均独立构造
//! 数据并经真实 `Engine::write_batch` 跑通，不依赖工程师的回归测试。

use sensearcdb_common::{AppConfig, ConfigHandle, Durability};
use sensearcdb_engine::engine::{DataPoint, DurabilityResolver, Engine, PointBatch};
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_engine::TenantId;
use std::sync::Arc;

struct StaticResolver;
impl DurabilityResolver for StaticResolver {
    fn resolve(&self, table_id: u32) -> Durability {
        match table_id {
            1 => Durability::Durable,
            2 => Durability::Async,
            _ => Durability::Group,
        }
    }
}

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

async fn test_engine() -> Engine {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    let handle = ConfigHandle::new(cfg);
    Engine::new(&handle, None).await.expect("engine")
}

/// ① 三子批全空 → written=0、wal_seq_start=0（unwrap_or(0) 兜底）。
#[tokio::test]
async fn all_empty_returns_zero() {
    let engine = test_engine().await;
    let result = engine
        .write_batch(PointBatch { points: vec![] })
        .await
        .expect("empty batch must succeed");
    assert_eq!(result.written, 0);
    assert_eq!(result.wal_seq_start, 0, "全空批应返回 0");
}

/// ② 纯 group 批（durable/async 子批空）→ 返回真实分配的非 0 seq_start。
#[tokio::test]
async fn pure_group_nonzero() {
    let engine = test_engine().await;
    let result = engine
        .write_batch(PointBatch {
            points: vec![point(7, 100, 1.0), point(7, 200, 2.0)],
        })
        .await
        .expect("pure group write must succeed");
    assert_eq!(result.written, 2);
    assert!(
        result.wal_seq_start > 0,
        "纯 group 批 wal_seq_start 应为非 0，实际 {}",
        result.wal_seq_start
    );
}

/// ③ 纯 durable 批（group/async 子批空）→ 非 0。
#[tokio::test]
async fn pure_durable_nonzero() {
    let engine = test_engine().await;
    engine.set_durability_resolver(Arc::new(StaticResolver));
    let result = engine
        .write_batch(PointBatch {
            points: vec![point(1, 100, 1.0)],
        })
        .await
        .expect("pure durable write must succeed");
    assert_eq!(result.written, 1);
    assert!(
        result.wal_seq_start > 0,
        "纯 durable 批 wal_seq_start 应为非 0，实际 {}",
        result.wal_seq_start
    );
}

/// ④ 纯 async 批（group/durable 子批空）→ 非 0。
#[tokio::test]
async fn pure_async_nonzero() {
    let engine = test_engine().await;
    engine.set_durability_resolver(Arc::new(StaticResolver));
    let result = engine
        .write_batch(PointBatch {
            points: vec![point(2, 100, 1.0)],
        })
        .await
        .expect("pure async write must succeed");
    assert_eq!(result.written, 1);
    assert!(
        result.wal_seq_start > 0,
        "纯 async 批 wal_seq_start 应为非 0，实际 {}",
        result.wal_seq_start
    );
}

/// ⑤ 混档批（三子批均非空）→ written=3 且 wal_seq_start 非 0（min 语义不被 0 污染）。
#[tokio::test]
async fn mixed_nonzero() {
    let engine = test_engine().await;
    engine.set_durability_resolver(Arc::new(StaticResolver));
    let result = engine
        .write_batch(PointBatch {
            points: vec![
                point(1, 100, 1.0), // durable
                point(2, 200, 2.0), // async
                point(3, 300, 3.0), // group
            ],
        })
        .await
        .expect("mixed durability write must succeed");
    assert_eq!(result.written, 3);
    assert!(
        result.wal_seq_start > 0,
        "混档批 wal_seq_start 应为非 0，实际 {}",
        result.wal_seq_start
    );
}

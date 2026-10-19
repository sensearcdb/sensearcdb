//! QA 独立复核测试（qa-p1-review）：lastpoint 快照 G2 + G1 + G3。
//!
//! 以全新视角独立构造稀疏数据，不采信工程师自测。覆盖：
//! - G2 P0：列级 merge 不丢列（含并发同 tag 不同列）+ 列级 ts 独立。
//! - G3：SubscribeSnapshots 首帧 INITIAL_SNAPSHOT + 后续 DELTA。
//!
//! 本文件由 QA 独立编写，聚焦工程师用例未覆盖的并发场景与订阅流。

use std::sync::Arc;

use arc_swap::ArcSwap;
use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId, TypedValue};
use sensearcdb_snapshot::{
    SchemaMirrorRef, SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
};
use tokio_stream::StreamExt;
use tonic::Request;

// ─── 测试桩 ────────────────────────────────────────────────────────────────

struct TestMirror;

impl SchemaMirrorRef for TestMirror {
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

fn make_store(shard_multiplier: usize) -> Arc<SnapshotStore> {
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = shard_multiplier;
    let ch = ConfigHandle::new(cfg);
    Arc::new(SnapshotStore::new(
        &ch,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestMirror))),
        SnapshotMetrics::noop(),
    ))
}

// ─── G2 P0：列级 merge 不丢列 ──────────────────────────────────────────────

/// 独立验证：先写 {col0} 再写 {col1}，读回必须含 {col0,col1} 且各自值正确。
#[test]
fn qa_sparse_write_two_columns_both_present() {
    let store = make_store(1);
    store.update_typed(
        TenantId::DEFAULT,
        1,
        &[(0, TypedValue::F64(10.0))],
        &[(0, 0xC0)],
        1000,
        2000,
        None,
        false,
        None,
    );
    store.update_typed(
        TenantId::DEFAULT,
        1,
        &[(1, TypedValue::I64(42))],
        &[(1, 0x00)],
        3000,
        4000,
        None,
        false,
        None,
    );

    let entry = store.get(1).expect("entry must exist");
    assert_eq!(
        entry.values,
        vec![(0, TypedValue::F64(10.0)), (1, TypedValue::I64(42))],
        "稀疏写不得丢列"
    );
    assert_eq!(entry.qualities, vec![(0, 0xC0), (1, 0x00)]);
}

/// 独立验证：混合覆盖 (T,{0,1}) → (T,{1,2}) 读回 {0旧,1新,2新}。
#[test]
fn qa_sparse_mixed_cover_and_keep() {
    let store = make_store(1);
    store.update_typed(
        TenantId::DEFAULT,
        2,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
        &[(0, 0xC0), (1, 0xC0)],
        100,
        200,
        None,
        false,
        None,
    );
    store.update_typed(
        TenantId::DEFAULT,
        2,
        &[(1, TypedValue::F64(20.0)), (2, TypedValue::F64(3.0))],
        &[(1, 0x00), (2, 0xC0)],
        300,
        400,
        None,
        false,
        None,
    );

    let entry = store.get(2).unwrap();
    assert_eq!(
        entry.values,
        vec![
            (0, TypedValue::F64(1.0)),
            (1, TypedValue::F64(20.0)),
            (2, TypedValue::F64(3.0)),
        ],
        "col0 保留旧值，col1 覆盖，col2 新增"
    );
}

/// 独立验证：并发同 tag 不同列不丢（工程师用例未覆盖的关键并发场景）。
#[test]
fn qa_concurrent_same_tag_different_columns_no_loss() {
    let store = make_store(1);

    // 双线程反复写同一 tag 的不同列，写入锁串行 + 列级 merge 保证两者都保留。
    let s0 = store.clone();
    let s1 = store.clone();
    let h0 = std::thread::spawn(move || {
        for i in 0..500u32 {
            s0.update_typed(
                TenantId::DEFAULT,
                3,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                i as i64,
                i as i64,
                None,
                false,
                None,
            );
        }
    });
    let h1 = std::thread::spawn(move || {
        for i in 0..500u32 {
            s1.update_typed(
                TenantId::DEFAULT,
                3,
                &[(1, TypedValue::I64(i as i64))],
                &[(1, 0x00)],
                i as i64,
                i as i64,
                None,
                false,
                None,
            );
        }
    });
    h0.join().unwrap();
    h1.join().unwrap();

    let entry = store.get(3).unwrap();
    assert_eq!(
        entry.values.len(),
        2,
        "并发写不同列后必须保留两列，实际 {:?}",
        entry.values
    );
    let ids: Vec<u16> = entry.values.iter().map(|(c, _)| *c).collect();
    assert_eq!(ids, vec![0, 1], "列 0 和列 1 都应存在");
}

/// 独立验证：列级 ts 独立 —— 写 {0}@100 再写 {1}@200，ts_values 分别为 100/200。
#[test]
fn qa_column_level_ts_independent() {
    let store = make_store(1);
    store.update_typed(
        TenantId::DEFAULT,
        4,
        &[(0, TypedValue::F64(1.0))],
        &[(0, 0xC0)],
        100,
        1000,
        None,
        false,
        None,
    );
    store.update_typed(
        TenantId::DEFAULT,
        4,
        &[(1, TypedValue::F64(2.0))],
        &[(1, 0x00)],
        200,
        2000,
        None,
        false,
        None,
    );

    let entry = store.get(4).unwrap();
    assert_eq!(
        entry.ts_values,
        vec![(0, 100), (1, 200)],
        "列级 ts 必须独立"
    );
    assert_eq!(entry.server_ts_values, vec![(0, 1000), (1, 2000)]);
    // 兼容字段 = max
    assert_eq!(entry.ts, 200);
    assert_eq!(entry.server_ts, 2000);
}

// ─── G3：SubscribeSnapshots 首帧 INITIAL + 后续 DELTA ──────────────────────

#[tokio::test]
async fn qa_subscribe_initial_then_delta() {
    use sensearcdb_snapshot::api::grpc::SnapshotGrpcService;
    use sensearcdb_snapshot::snapshot_proto::snapshot_event::EventType;
    use sensearcdb_snapshot::snapshot_proto::snapshot_service_server::SnapshotService;
    use sensearcdb_snapshot::snapshot_proto::SubscribeSnapshotsRequest;

    // 关键：store 与 service 必须共享同一个 subscription_manager。
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = 1;
    let ch = ConfigHandle::new(cfg);
    let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
    let store = Arc::new(SnapshotStore::new(
        &ch,
        sub_mgr.clone(),
        Arc::new(TagMetadataStore::new(Arc::new(TestMirror))),
        SnapshotMetrics::noop(),
    ));

    // 先写一条，作为 INITIAL 快照。
    store.update_typed(
        TenantId::DEFAULT,
        5,
        &[(0, TypedValue::F64(7.5))],
        &[(0, 0xC0)],
        100,
        200,
        None,
        false,
        None,
    );

    let service = SnapshotGrpcService::new(Arc::new(ArcSwap::new(store.clone())), sub_mgr.clone())
        .with_tenant_resolver(Arc::new(|_| Some(TenantId::DEFAULT)));

    let mut stream = service
        .subscribe_snapshots(Request::new(SubscribeSnapshotsRequest {
            tag_ids: vec![5],
            subscription_id: "qa-review".into(),
        }))
        .await
        .expect("subscribe rpc")
        .into_inner();

    // 首帧 = INITIAL_SNAPSHOT。
    let first = stream.next().await.expect("first frame").expect("ok");
    assert_eq!(first.event_type, EventType::InitialSnapshot as i32);
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].tag_id, 5);

    // 触发一次写 → 应收到 DELTA。
    store.update_typed(
        TenantId::DEFAULT,
        5,
        &[(0, TypedValue::F64(9.0))],
        &[(0, 0xC0)],
        300,
        400,
        None,
        false,
        None,
    );

    let second = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("delta within timeout")
        .expect("delta frame")
        .expect("ok");
    assert_eq!(second.event_type, EventType::Delta as i32);
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].tag_id, 5);
}

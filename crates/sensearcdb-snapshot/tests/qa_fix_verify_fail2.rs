//! 独立 QA 验证（FAIL-2 → LWW 统一）：snapshot 同 ts 冲突语义统一为 write_seq 决胜。
//!
//! 不依赖工程师的单元测试，直接通过公开 API 独立设计边界用例：
//! 1) 在线同 ts write_seq 大者胜（后到者胜，取代旧「先到者胜」）；
//! 2) 同 ts write_seq 小者晚到不覆盖（乱序保护）；
//! 3) 跨 ts 乱序（new_ts < old_ts）仍拒绝，不得无条件覆盖；
//! 4) 列级隔离：同一次写入不同列独立取舍。

use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId, TypedValue};
use sensearcdb_snapshot::{
    SchemaMirrorRef, SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
};
use std::sync::Arc;

struct TestMirror;
impl SchemaMirrorRef for TestMirror {
    fn get_table_name(&self, _: u32) -> Option<String> {
        Some("t".into())
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

fn make_store() -> SnapshotStore {
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = 1;
    let handle = ConfigHandle::new(cfg);
    SnapshotStore::new(
        &handle,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestMirror))),
        SnapshotMetrics::noop(),
    )
}

fn v(id: u16, x: f64) -> (u16, TypedValue) {
    (id, TypedValue::F64(x))
}

/// ① 在线路径统一 LWW：同 ts write_seq 大者覆盖小者。
#[test]
fn online_same_ts_higher_seq_wins() {
    let store = make_store();
    let r1 = store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 1.0)],
        &[(0, 0xC0)],
        100,
        200,
        Some(5),
        false,
        None,
    );
    assert!(r1.changed);

    let r2 = store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 9.0)],
        &[(0, 0x00)],
        100,
        999,
        Some(10),
        false,
        None,
    );
    assert!(r2.changed, "在线同 ts 高 write_seq 应覆盖");
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![v(0, 9.0)]);
    assert_eq!(
        e.server_ts_values,
        vec![(0, 999)],
        "覆盖时 server_ts 同步更新"
    );
    assert_eq!(e.write_seqs, vec![(0, Some(10))]);
}

/// ② 同 ts write_seq 小者晚到不覆盖（乱序保护）。
#[test]
fn online_same_ts_lower_seq_rejected() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 9.0)],
        &[(0, 0xC0)],
        100,
        999,
        Some(10),
        false,
        None,
    );

    let r = store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 1.0)],
        &[(0, 0x00)],
        100,
        200,
        Some(5),
        false,
        None,
    );
    assert!(!r.changed, "同 ts 低 write_seq 不得回滚");
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![v(0, 9.0)]);
    assert_eq!(e.write_seqs, vec![(0, Some(10))]);
}

/// ③ 跨 ts 乱序：new_ts < old_ts 仍拒绝（write_seq 不参与跨 ts 比较）。
#[test]
fn cross_ts_out_of_order_rejected() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 2.0)],
        &[(0, 0xC0)],
        200,
        2000,
        Some(1),
        false,
        None,
    );

    let r = store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 1.0)],
        &[(0, 0x00)],
        100,
        1000,
        Some(2),
        false,
        None,
    );
    assert!(!r.changed, "跨 ts 乱序仍不得覆盖");
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![v(0, 2.0)]);
    assert_eq!(e.server_ts_values, vec![(0, 2000)]);
}

/// ④ 列级隔离：col0 乱序拒、col1 同 ts 高 seq 覆盖、col2 新 ts 覆盖。
#[test]
fn column_level_isolation() {
    let store = make_store();
    // 现有：col0@200, col1@100, col2@100
    store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 1.0), v(1, 2.0), v(2, 3.0)],
        &[(0, 0xC0), (1, 0xC0), (2, 0xC0)],
        100,
        100,
        Some(1),
        false,
        None,
    );
    store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 10.0)],
        &[(0, 0xC0)],
        200,
        200,
        Some(2),
        false,
        None,
    );

    // 写入：col0@100(乱序拒)、col1@100(同 ts 高 seq 覆盖)、col2@300(新 ts 覆盖)
    let r = store.update_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 99.0), v(1, 20.0), v(2, 30.0)],
        &[(0, 0x00), (1, 0x00), (2, 0x00)],
        100,
        999,
        Some(3),
        false,
        None,
    );
    assert!(r.changed);
    let e = store.get(1).unwrap();
    assert_eq!(
        e.values,
        vec![v(0, 10.0), v(1, 20.0), v(2, 30.0)],
        "col0 乱序拒、col1/col2 同 ts 高 seq 覆盖"
    );
    assert_eq!(e.ts_values, vec![(0, 200), (1, 100), (2, 100)]);
    // col1/col2 同 ts 高 seq 覆盖 → server_ts 同步 999。
    assert_eq!(e.server_ts_values, vec![(0, 200), (1, 999), (2, 999)]);
}


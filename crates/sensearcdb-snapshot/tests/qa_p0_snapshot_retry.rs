//! QA 独立验证（qa-p0-snapshot-retry）—— P0 snapshot 轻量化（1a 整行覆盖
//! 快速路径 + 1c 通知去深拷贝）的正确性边界。fresh eyes，不依赖工程师自测。
//!
//! 覆盖（均为工程师自测未覆盖或覆盖不足的边界）：
//! - F1. 列集合一致但写入顺序不同（{0,1} vs {1,0}）：归一化后应正确识别为
//!   「集合一致」并走快速路径，两列均更新为新 ts 的新值。
//! - F2. 列数相等但 column_id 不同（{0,1} vs {0,2}）：不得误走快速路径，
//!   必须回退 merge 做列级逐点裁决（覆盖 0 + 保留/新增）。
//! - F3. 快速路径正确继承 write_seq / server_ts / prev_value / prev_ts（不丢字段）。
//! - F4. 同 ts 且 write_seq 相等 → 回退 merge 平局保留 old，不得覆盖成旧值。
//! - F5. 首次写入（existing 为空）不得走快速路径（NewEntry 语义），且后续整行
//!   严格更新才走快速路径。
//! - F6. 快速路径下 prev_value / prev_ts 首列前值追踪与 merge 路径一致。

use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId, TypedValue};
use sensearcdb_snapshot::{
    SchemaMirrorRef, SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
};

struct TestSchemaMirror;

impl SchemaMirrorRef for TestSchemaMirror {
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

fn make_store() -> Arc<SnapshotStore> {
    let cfg = ConfigHandle::new(AppConfig::default());
    Arc::new(SnapshotStore::new(
        &cfg,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
        SnapshotMetrics::noop(),
    ))
}

// ─── F1：列集合一致但顺序不同 ─────────────────────────────────────────────

/// 写入 {0,1}@ts100，再以 {1,0}（反序）@ts200 整行严格更新。
/// 归一化后应识别为「集合一致」走快速路径，两列均更新为 ts=200 的新值。
#[test]
fn f1_same_set_different_order_fast_path() {
    let store = make_store();
    let r1 = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
        &[(0, 0xC0), (1, 0xC0)],
        100, 1001, Some(5), false, None,
    );
    assert!(r1.changed);

    // 反序写入（{1,0}），列集合一致，每列 ts 严格 > 旧 ts。
    let r2 = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(1, TypedValue::F64(20.0)), (0, TypedValue::F64(10.0))],
        &[(1, 0x00), (0, 0x00)],
        200, 2001, Some(6), false, None,
    );
    assert!(r2.changed, "集合一致 + 严格更新必须覆盖");

    let e = store.get(1).unwrap();
    // 归一化后升序存储。
    assert_eq!(e.values, vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))]);
    assert_eq!(e.qualities, vec![(0, 0x00), (1, 0x00)]);
    assert_eq!(e.ts_values, vec![(0, 200), (1, 200)]);
    assert_eq!(e.server_ts_values, vec![(0, 2001), (1, 2001)]);
    assert_eq!(e.ts, 200);
    assert_eq!(e.server_ts, 2001);
    assert_eq!(e.write_seqs, vec![(0, Some(6)), (1, Some(6))]);
}

// ─── F2：列数相等但 column_id 不同 ────────────────────────────────────────

/// 写入 {0,1}@ts100，再写 {0,2}@ts200。列数相等（都是 2）但集合不同（1≠2），
/// 不得误走快速路径整行覆盖，必须回退 merge：col0 覆盖、col2 新增、col1 保留旧值。
#[test]
fn f2_equal_len_different_column_ids_falls_back() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
        &[(0, 0xC0), (1, 0xC0)],
        100, 1001, Some(5), false, None,
    );

    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (2, TypedValue::F64(30.0))],
        &[(0, 0x00), (2, 0xC0)],
        200, 2001, Some(6), false, None,
    );
    assert!(r.changed);

    let e = store.get(1).unwrap();
    // col1 必须保留旧值（未在本次写入携带）。
    assert_eq!(
        e.values,
        vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(2.0)), (2, TypedValue::F64(30.0))]
    );
    assert_eq!(e.qualities, vec![(0, 0x00), (1, 0xC0), (2, 0xC0)]);
    assert_eq!(e.ts_values, vec![(0, 200), (1, 100), (2, 200)]);
    assert_eq!(e.write_seqs, vec![(0, Some(6)), (1, Some(5)), (2, Some(6))]);
}

// ─── F3：快速路径字段继承（write_seq / server_ts / prev_*） ───────────────

/// 快速路径整行覆盖后，write_seq / server_ts / prev_value / prev_ts / ts_values
/// 必须正确继承新值 + 追踪前值，不丢字段、不残留旧值。
#[test]
fn f3_fast_path_field_inheritance() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
        &[(0, 0xC0), (1, 0xC0)],
        100, 1001, Some(5), false, None,
    );

    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))],
        &[(0, 0x00), (1, 0x00)],
        200, 2001, Some(6), false, None,
    );
    assert!(r.changed);

    let e = store.get(1).unwrap();
    assert_eq!(e.ts, 200, "ts 更新为新值");
    assert_eq!(e.server_ts, 2001, "server_ts 更新为新值");
    assert_eq!(e.ts_values, vec![(0, 200), (1, 200)]);
    assert_eq!(e.server_ts_values, vec![(0, 2001), (1, 2001)]);
    assert_eq!(e.write_seqs, vec![(0, Some(6)), (1, Some(6))], "write_seq 更新为新值");
    // S1 列式：prev_value/prev_ts 不再存储，投影恒 None。
    assert_eq!(e.prev_value, None, "S1 不再存储前值");
    assert_eq!(e.prev_ts, None, "S1 不再存储前 ts");
    assert!(!e.ttl_expired);
}

// ─── F4：同 ts 且 write_seq 相等 → 平局保留 old ───────────────────────────

/// 同 ts 且 seq 相等（都 Some(5)），回退 merge，new_wins_tie 平局保留 old，
/// 不得覆盖成新值。
#[test]
fn f4_same_ts_equal_seq_keeps_old() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0))],
        &[(0, 0xC0)],
        100, 200, Some(5), false, None,
    );

    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(9.0))],
        &[(0, 0x00)],
        100, 200, Some(5), false, None,
    );
    // 完全平局（ts 相等 + seq 相等 + server_ts 相等）→ 保留 old → unchanged。
    assert!(!r.changed, "同 ts 同 seq 同 server_ts 必须保留 old");

    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(1.0))], "不得覆盖成旧值 9.0");
    assert_eq!(e.quality(), 0xC0);
    assert_eq!(e.write_seqs, vec![(0, Some(5))]);
}

// ─── F5：首次写入不走快速路径 ─────────────────────────────────────────────

/// existing 为空时 fast_overwrite=false，走 NewEntry 分支；后续整行严格更新才走快速路径。
#[test]
fn f5_first_write_not_fast_path_then_fast_path() {
    let store = make_store();
    // 首次写入（existing=None）。
    let r1 = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0))],
        &[(0, 0xC0)],
        100, 1001, Some(5), false, None,
    );
    assert!(r1.changed);
    let e1 = store.get(1).unwrap();
    assert_eq!(e1.values, vec![(0, TypedValue::F64(1.0))]);
    assert_eq!(e1.prev_value, None, "首次写入无前值");

    // 第二次整行严格更新（existing=Some）。
    let r2 = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(2.0))],
        &[(0, 0x00)],
        200, 2001, Some(6), false, None,
    );
    assert!(r2.changed);
    let e2 = store.get(1).unwrap();
    assert_eq!(e2.values, vec![(0, TypedValue::F64(2.0))]);
    assert_eq!(e2.prev_value, None, "S1 不再存储前值");
    assert_eq!(e2.prev_ts, None, "S1 不再存储前 ts");
}

// ─── F6：1c 通知 Arc 内容与 store 内最终条目一致 ──────────────────────────

/// 验证 1c 通知去深拷贝后，订阅者收到的 Arc<SnapshotEntry> 内容与 store 内
/// 最终存储的条目完全一致（值 / ts / seq / 前值），无悬垂、无旧值残留。
#[test]
fn f6_notify_arc_matches_stored_entry() {
    let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
    let cfg = ConfigHandle::new(AppConfig::default());
    let store = Arc::new(SnapshotStore::new(
        &cfg,
        sub_mgr.clone(),
        Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
        SnapshotMetrics::noop(),
    ));

    let (tx, mut rx) = tokio::sync::mpsc::channel::<
        sensearcdb_snapshot::DataChangeNotification,
    >(16);
    sub_mgr.subscribe(vec![1], tx, false);

    store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
        &[(0, 0xC0), (1, 0xC0)],
        100, 1001, Some(5), false, None,
    );
    // 触发快速路径整行覆盖。
    store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))],
        &[(0, 0x00), (1, 0x00)],
        200, 2001, Some(6), false, None,
    );

    let stored = store.get(1).unwrap();

    // 消费两条通知，取最后一条（快速路径写入）。
    let mut last: Option<sensearcdb_snapshot::DataChangeNotification> = None;
    while let Ok(n) = rx.try_recv() {
        last = Some(n);
    }
    let notif = last.expect("至少收到一条通知");

    assert_eq!(notif.table_id, 1);
    // 通知携带的 entry 必须与 store 内最终条目一致。
    assert_eq!(notif.entry.values, stored.values);
    assert_eq!(notif.entry.qualities, stored.qualities);
    assert_eq!(notif.entry.ts, stored.ts);
    assert_eq!(notif.entry.server_ts, stored.server_ts);
    assert_eq!(notif.entry.ts_values, stored.ts_values);
    assert_eq!(notif.entry.server_ts_values, stored.server_ts_values);
    assert_eq!(notif.entry.write_seqs, stored.write_seqs);
    assert_eq!(notif.entry.version, stored.version);
    assert_eq!(notif.entry.prev_value, stored.prev_value);
}

//! QA 独立验证（qa-p0-snapshot-retry3）—— P0 snapshot 轻量化（1a 整行覆盖
//! 快速路径 + 1c 通知去深拷贝）的正确性边界。fresh eyes，不依赖工程师自测，
//! 也不照抄 qa-p0-snapshot-retry 的 F1~F6。
//!
//! 聚焦本次验证最关心的「逐列 ts 检查是否严密」与「LWW 同 ts 是否被快速路径
//! 挡在门外」两个高风险点：
//!
//! - N1. 首列推进、中列乱序（满列集写入）→ 快速路径必须拒绝，仅乱序列保留旧值，
//!   其余列更新。若实现只检查首列/行级 ts，中列会被错误覆盖。
//! - N2. 三列列序不同但集合相同 → 快速路径覆盖 + write_seq/quality 完整继承。
//! - N3. 快速路径下 write_seq=None 覆盖 None，不得凭空变成 Some。
//! - N4. 同 ts 满列 write_seq 大者胜（快速路径被严格 > 挡回 merge）。
//! - N5. 同 ts 满列 Some > None。
//! - N6. 同 ts 满列新 seq 更小不覆盖。
//! - N7. 同 ts 平局（None vs None 且 server_ts 相等）保留 old。
//! - N8. 稀疏列 3 列 → 写 2 列，未写列保留。
//! - N9. import_columns_typed 满列严格更新走快速路径，逐列 ts / write_seq 保真。
//! - N10. 1c 通知 Arc 稳定：拒绝写不通知、通知内容与 store 一致。

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

// ─── N1：首列推进、中列乱序（满列集写入）─────────────────────────────────

/// 关键高风险点：满列集写入中，col0 推进、col1 乱序、col2 推进。
/// 若快速路径只检查首列或行级 ts，会误覆盖 col1；正确实现必须逐列检查并回退 merge。
#[test]
fn n1_first_col_advances_mid_col_out_of_order() {
    let store = make_store();
    // 构造逐列不同旧 ts：col0@100, col1@500, col2@200。
    store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 100, 1001, Some(1), false, None);
    store.update_typed(TenantId::DEFAULT, 1, &[(1, TypedValue::F64(2.0))], &[(1, 0xC0)], 500, 5001, Some(2), false, None);
    store.update_typed(TenantId::DEFAULT, 1, &[(2, TypedValue::F64(3.0))], &[(2, 0xC0)], 200, 2001, Some(3), false, None);

    // 满列集写入，行级 ts=300：col0 推进(300>100)、col1 乱序(300<500)、col2 推进(300>200)。
    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0)), (2, TypedValue::F64(30.0))],
        &[(0, 0x00), (1, 0x00), (2, 0x00)],
        300, 3001, Some(9), false, None,
    );
    assert!(r.changed, "col0/col2 接受 → changed=true");

    let e = store.get(1).unwrap();
    // col1 必须保留旧值 2.0@500，不被 300 覆盖。
    assert_eq!(
        e.values,
        vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(2.0)), (2, TypedValue::F64(30.0))],
        "中列乱序必须回退 merge，保留旧值"
    );
    assert_eq!(e.qualities, vec![(0, 0x00), (1, 0xC0), (2, 0x00)]);
    assert_eq!(e.ts_values, vec![(0, 300), (1, 500), (2, 300)]);
    assert_eq!(e.write_seqs, vec![(0, Some(9)), (1, Some(2)), (2, Some(9))]);
}

// ─── N2：三列列序不同但集合相同 → 快速路径覆盖 ──────────────────────────

#[test]
fn n2_three_col_same_set_different_order_overwrites() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::I64(2)), (2, TypedValue::String("a".into()))],
        &[(0, 0xC0), (1, 0xC0), (2, 0x40)],
        100, 1001, Some(5), false, None,
    );
    // 以 {2,0,1} 反序写入（集合一致），每列 ts 严格 > 旧 ts → 快速路径。
    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(2, TypedValue::String("b".into())), (0, TypedValue::F64(10.0)), (1, TypedValue::I64(20))],
        &[(2, 0x00), (0, 0x00), (1, 0x00)],
        200, 2001, Some(6), false, None,
    );
    assert!(r.changed);

    let e = store.get(1).unwrap();
    assert_eq!(
        e.values,
        vec![(0, TypedValue::F64(10.0)), (1, TypedValue::I64(20)), (2, TypedValue::String("b".into()))]
    );
    assert_eq!(e.qualities, vec![(0, 0x00), (1, 0x00), (2, 0x00)]);
    assert_eq!(e.ts_values, vec![(0, 200), (1, 200), (2, 200)]);
    assert_eq!(e.write_seqs, vec![(0, Some(6)), (1, Some(6)), (2, Some(6))]);
}

// ─── N3：快速路径下 write_seq=None 覆盖 None ──────────────────────────────

#[test]
fn n3_fast_path_none_seq_stays_none() {
    let store = make_store();
    store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))], &[(0, 0xC0), (1, 0xC0)], 100, 1001, None, false, None);

    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))],
        &[(0, 0x00), (1, 0x00)],
        200, 2001, None, false, None,
    );
    assert!(r.changed);

    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))]);
    assert_eq!(e.write_seqs, vec![(0, None), (1, None)], "快速路径不得凭空造出 Some(seq)");
}

// ─── N4~N7：同 ts 满列（快速路径被严格 > 挡回 merge 的正确裁决）──────────

#[test]
fn n4_same_ts_full_row_higher_seq_wins() {
    let store = make_store();
    store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))], &[(0, 0xC0), (1, 0xC0)], 100, 1001, Some(5), false, None);

    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))],
        &[(0, 0x00), (1, 0x00)],
        100, 999, Some(10), false, None,
    );
    assert!(r.changed, "同 ts 高 write_seq 必须覆盖");
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))]);
    assert_eq!(e.write_seqs, vec![(0, Some(10)), (1, Some(10))]);
    assert_eq!(e.server_ts, 999);
}

#[test]
fn n5_same_ts_full_row_some_beats_none() {
    let store = make_store();
    store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))], &[(0, 0xC0), (1, 0xC0)], 100, 1001, None, false, None);

    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))],
        &[(0, 0x00), (1, 0x00)],
        100, 999, Some(1), false, None,
    );
    assert!(r.changed, "Some 必须覆盖 None");
    let e = store.get(1).unwrap();
    assert_eq!(e.write_seqs, vec![(0, Some(1)), (1, Some(1))]);
}

#[test]
fn n6_same_ts_full_row_lower_seq_rejected() {
    let store = make_store();
    store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))], &[(0, 0x00), (1, 0x00)], 100, 999, Some(10), false, None);

    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
        &[(0, 0xC0), (1, 0xC0)],
        100, 1000, Some(3), false, None,
    );
    assert!(!r.changed, "同 ts 低 write_seq 不得回滚");
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))]);
    assert_eq!(e.write_seqs, vec![(0, Some(10)), (1, Some(10))]);
}

#[test]
fn n7_same_ts_full_row_tie_keeps_old() {
    let store = make_store();
    store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))], &[(0, 0xC0), (1, 0xC0)], 100, 200, None, false, None);

    // None vs None 且 server_ts 相等 → 完全平局 → 保留 old。
    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(9.0)), (1, TypedValue::F64(8.0))],
        &[(0, 0x00), (1, 0x00)],
        100, 200, None, false, None,
    );
    assert!(!r.changed, "平局必须保留 old");
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))]);
    assert_eq!(e.write_seqs, vec![(0, None), (1, None)]);
}

// ─── N8：稀疏列 3 列 → 写 2 列，未写列保留 ────────────────────────────────

#[test]
fn n8_sparse_three_to_two_keeps_unwritten() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0)), (2, TypedValue::F64(3.0))],
        &[(0, 0xC0), (1, 0xC0), (2, 0xC0)],
        100, 1001, Some(5), false, None,
    );
    // 只写 {0,1}，列集合不一致（2≠3）→ 必须回退 merge，保留 col2。
    let r = store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))],
        &[(0, 0x00), (1, 0x00)],
        200, 2001, Some(6), false, None,
    );
    assert!(r.changed);
    let e = store.get(1).unwrap();
    assert_eq!(
        e.values,
        vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0)), (2, TypedValue::F64(3.0))],
        "col2 必须保留旧值"
    );
    assert_eq!(e.qualities, vec![(0, 0x00), (1, 0x00), (2, 0xC0)]);
    assert_eq!(e.ts_values, vec![(0, 200), (1, 200), (2, 100)]);
    assert_eq!(e.write_seqs, vec![(0, Some(6)), (1, Some(6)), (2, Some(5))]);
}

// ─── N9：import_columns_typed 满列严格更新走快速路径，逐列字段保真 ───────

#[test]
fn n9_import_full_row_fast_path_preserves_per_column() {
    let store = make_store();
    // 首次导入：col0 seq Some(5)、col1 seq None。
    store.import_columns_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
        &[(0, 0xC0), (1, 0xC0)],
        &[(0, 100), (1, 100)],
        &[(0, 1001), (1, 1001)],
        &[(0, Some(5)), (1, None)],
        false, None,
    );
    // 满列严格更新（逐列 ts 均 > 旧 ts）→ 快速路径。
    let r = store.import_columns_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))],
        &[(0, 0x00), (1, 0x00)],
        &[(0, 200), (1, 250)],
        &[(0, 2001), (1, 2501)],
        &[(0, Some(7)), (1, Some(9))],
        false, None,
    );
    assert!(r.changed);
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))]);
    assert_eq!(e.ts_values, vec![(0, 200), (1, 250)], "逐列 ts 保真");
    assert_eq!(e.server_ts_values, vec![(0, 2001), (1, 2501)]);
    assert_eq!(e.write_seqs, vec![(0, Some(7)), (1, Some(9))], "逐列 write_seq 保真");
    assert_eq!(e.ts, 250, "兼容字段 = max(逐列 ts)");
}

// ─── N10：1c 通知 Arc 稳定：拒绝写不通知、通知内容与 store 一致 ─────────

#[test]
fn n10_notify_arc_stable_and_rejected_write_no_notify() {
    let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
    let cfg = ConfigHandle::new(AppConfig::default());
    let store = Arc::new(SnapshotStore::new(
        &cfg,
        sub_mgr.clone(),
        Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
        SnapshotMetrics::noop(),
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<sensearcdb_snapshot::DataChangeNotification>(16);
    sub_mgr.subscribe(vec![1], tx, false);

    // 首写（无前值）。
    store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 100, 1001, Some(5), false, None);
    // 快速路径写入。
    store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(10.0))], &[(0, 0x00)], 200, 2001, Some(6), false, None);
    // 乱序写入（拒绝，changed=false，不通知）。
    let r_reject = store.update_typed(TenantId::DEFAULT, 1, &[(0, TypedValue::F64(0.5))], &[(0, 0xC0)], 50, 5001, Some(7), false, None);
    assert!(!r_reject.changed);

    let stored = store.get(1).unwrap();
    assert_eq!(stored.values, vec![(0, TypedValue::F64(10.0))]);

    // 应恰好收到 2 条通知（首写 + 快速路径），拒绝写无通知。
    let mut notifs = Vec::new();
    while let Ok(n) = rx.try_recv() {
        notifs.push(n);
    }
    assert_eq!(notifs.len(), 2, "拒绝写不得产生通知");
    let last = notifs.last().unwrap();
    assert_eq!(last.entry.values, stored.values);
    assert_eq!(last.entry.write_seqs, stored.write_seqs);
    assert_eq!(last.entry.version, stored.version);
}

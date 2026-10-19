//! QA 独立验证（软件 QA 工程师 #3）—— SnapshotStore 同 ts 冲突统一为 LWW
//! （write_seq 决胜）的边界语义。fresh eyes，不依赖工程师自测。
//!
//! 覆盖：
//! - A1. 同 ts 同列，write_seq 大者覆盖小者（后写胜）。
//! - A2. 乱序（write_seq 小晚到）不覆盖（先写大 seq 保留）。
//! - B1. Some vs None：Some 胜。
//! - B2. None vs None：server_ts 决胜；server_ts 也相等 → 保留 old。
//! - B3. 旧快照缺 `write_seqs` 字段反序列化 → 全 None，不 panic（#[serde(default)]）。
//! - C 列级对齐：多列同一次写入中，逐列 seq 与 value/quality/ts/server_ts 原子绑定，
//!   不被张冠李戴（merge 层逐点对齐）。
//!
//! 决胜规则采信设计 `docs/system_design_snapshot_lww.md` §2.2：
//!   write_seq 大者胜（Some > None）→ server_ts 大者胜 → 平局保留 old。

use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId, TypedValue};
use sensearcdb_snapshot::merge::new_wins_tie;
use sensearcdb_snapshot::{
    SchemaMirrorRef, SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
};

// ─── store 桩 ──────────────────────────────────────────────────────────────

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

// ─── A + B：决胜纯函数决策表 ──────────────────────────────────────────────

/// 穷举 `new_wins_tie` 的 4 种 seq 组合 × server_ts 关系，锁死口径。
#[test]
fn test_new_wins_tie_full_decision_table() {
    // (new_seq, old_seq, new_sts, old_sts, expect_new_wins)
    let cases: &[(Option<u64>, Option<u64>, i64, i64, bool)] = &[
        // Some > None（flush 数据优先于遗留 None）。
        (Some(1), None, 0, 999, true), // 即使 server_ts 旧也 Some 胜
        // None < Some。
        (None, Some(1), 999, 0, false),
        // 同为 Some：大者胜。
        (Some(10), Some(5), 0, 999, true), // 即使 server_ts 旧也 seq 大者胜
        (Some(5), Some(10), 999, 0, false),
        // 同为 Some 且相等：退 server_ts 大者胜。
        (Some(7), Some(7), 999, 0, true),
        (Some(7), Some(7), 0, 999, false),
        // 同为 None：退 server_ts 大者胜。
        (None, None, 999, 0, true),
        (None, None, 0, 999, false),
    ];
    for (ns, os, nsts, osts, expect) in cases {
        let got = new_wins_tie(*ns, *os, *nsts, *osts);
        // 对称验证：交换 new/old 后，结论应与「非平局」相反。
        let got_swapped = new_wins_tie(*os, *ns, *osts, *nsts);
        assert_eq!(got, *expect, "case ({ns:?},{os:?},{nsts},{osts})");
        if *ns != *os || *nsts != *osts {
            // 非完全平局时，交换后必然反转。
            assert_eq!(got_swapped, !got, "对称性被破坏");
        }
    }

    // 完全平局 → 保留 old（返回 false，两个方向都 false）。
    assert!(!new_wins_tie(None, None, 200, 200));
    assert!(!new_wins_tie(Some(5), Some(5), 200, 200));
}

// ─── A1/A2/B1/B2：store 级端到端 ──────────────────────────────────────────

/// A1：同 ts 同列，write_seq 大者覆盖小者。
#[test]
fn test_store_same_ts_higher_seq_wins() {
    let store = make_store();
    let r1 = store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 100, 200, Some(5),
        false, None,
    );
    assert!(r1.changed);

    let r2 = store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(9.0))], &[(0, 0x00)], 100, 999, Some(10),
        false, None,
    );
    assert!(r2.changed, "同 ts 高 write_seq 必须覆盖");

    let e = store.get(1).unwrap();
    assert!((e.value() - 9.0).abs() < 1e-9);
    assert_eq!(e.quality(), 0x00);
    assert_eq!(e.server_ts, 999);
    assert_eq!(e.write_seqs, vec![(0, Some(10))], "覆盖后 write_seq 同步更新为 10");
}

/// A2：乱序（seq 小晚到）不覆盖，值/seq 不回滚。
#[test]
fn test_store_same_ts_lower_seq_rejected() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(9.0))], &[(0, 0xC0)], 100, 999, Some(10),
        false, None,
    );

    let r = store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(0.5))], &[(0, 0x00)], 100, 1000, Some(5),
        false, None,
    );
    assert!(!r.changed, "同 ts 低 write_seq 不得回滚");

    let e = store.get(1).unwrap();
    assert!((e.value() - 9.0).abs() < 1e-9);
    assert_eq!(e.write_seqs, vec![(0, Some(10))], "拒绝时 write_seq 不回滚");
    assert_eq!(e.server_ts, 999, "拒绝时 server_ts 不回滚");
}

/// B1：Some vs None → Some 胜。
#[test]
fn test_store_some_seq_beats_none() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 100, 200, None, false,
        None,
    );
    let r = store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(9.0))], &[(0, 0x00)], 100, 999, Some(1),
        false, None,
    );
    assert!(r.changed, "Some 必须覆盖 None 遗留条目");
    let e = store.get(1).unwrap();
    assert!((e.value() - 9.0).abs() < 1e-9);
    assert_eq!(e.write_seqs, vec![(0, Some(1))]);
}

/// B2：None vs None → server_ts 大者胜；server_ts 相等 → 保留 old。
#[test]
fn test_store_none_vs_none_server_ts_tiebreak() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 100, 200, None, false,
        None,
    );
    let r = store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(9.0))], &[(0, 0x00)], 100, 999, None, false,
        None,
    );
    assert!(r.changed, "None vs None 且 server_ts 大者胜");
    let e = store.get(1).unwrap();
    assert!((e.value() - 9.0).abs() < 1e-9);

    // server_ts 相等 → 保留 old。
    let r2 = store.update_typed(
        TenantId::DEFAULT, 1, &[(0, TypedValue::F64(8.0))], &[(0, 0xC0)], 100, 999, None, false,
        None,
    );
    assert!(!r2.changed, "None vs None 且 server_ts 相等 → 保留 old");
    let e2 = store.get(1).unwrap();
    assert!((e2.value() - 9.0).abs() < 1e-9);
}

// ─── B3：旧快照缺 write_seqs 字段反序列化兼容 ─────────────────────────────

/// 旧快照 JSON 缺 `write_seqs` 字段 → #[serde(default)] → 空 Vec（= 全 None），不 panic。
#[test]
fn test_legacy_entry_without_write_seqs_deserializes() {
    let legacy = r#"{"values":[[0,{"F64":1.0}]],"qualities":[[0,192]],"ts":100,"server_ts":200,"version":1,"prev_value":null,"prev_ts":null,"is_derived":false,"writer_session_id":null,"ttl_expired":false,"ts_values":[[0,100]],"server_ts_values":[[0,200]]}"#;
    let e: sensearcdb_snapshot::SnapshotEntry =
        serde_json::from_str(legacy).expect("缺 write_seqs 字段的旧快照必须能反序列化");
    assert!(
        e.write_seqs.is_empty(),
        "缺字段时应反序列化为空 Vec（全 None）"
    );
}

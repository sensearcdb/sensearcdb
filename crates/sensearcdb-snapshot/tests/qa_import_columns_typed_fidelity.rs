//! QA 独立验证（软件 QA 工程师 #4）—— 跨节点快照迁移逐列保真度收尾：
//! `import_columns_typed` 逐列导入 API 的正确性。fresh eyes，不采信工程师自测。
//!
//! 覆盖（对应验收项 A~E）：
//! - A1. 逐列对齐（write_seqs 列子集不齐 + ts/server_ts 全量）→ 五组按 column_id 对齐。
//! - A2. 逐列对齐（ts_values/server_ts_values 列子集不齐）→ 是否正确按 column_id
//!        对齐 + 缺列兜底，而非张冠李戴（import_columns_typed 最易错处）。
//! - B.  迁移 roundtrip 保真：serialize → deserialize → import → 读回，逐列
//!        write_seqs / ts_values / server_ts_values 与迁移前完全一致（不压平、不丢 None）。
//! - C.  遗留空 write_seqs 兜底：不 panic，全列 None。
//! - D.  LWW 决胜在 import 场景：迁移导入 Some(seq) 后，本地写同 ts 同列更大 seq 覆盖。
//!
//! 决胜规则采信设计 `docs/system_design_snapshot_lww.md` §2.2：
//!   write_seq 大者胜（Some > None）→ server_ts 大者胜 → 平局保留 old。

use std::collections::HashMap;
use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId, TypedValue};
use sensearcdb_snapshot::serialize::{deserialize_from_bytes, serialize_to_bytes};
use sensearcdb_snapshot::{
    SchemaMirrorRef, SnapshotEntry, SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager,
    TagMetadataStore,
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

fn make_store() -> SnapshotStore {
    let cfg = ConfigHandle::new(AppConfig::default());
    SnapshotStore::new(
        &cfg,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
        SnapshotMetrics::noop(),
    )
}

fn v(id: u16, x: f64) -> (u16, TypedValue) {
    (id, TypedValue::F64(x))
}

// ─── A1：write_seqs 列子集不齐（现实场景：遗留条目 write_seqs 部分缺失）───

/// values 含 col 1/3/5，write_seqs 只含 col 1（部分缺失），
/// ts_values / server_ts_values 全量。验证 write_seqs 经 HashMap 按 column_id 对齐
/// （缺列补 None），且 ts/server_ts 不因 write_seqs 缺列而错位。
#[test]
fn qa_import_partial_write_seqs_align_by_id() {
    let store = make_store();

    // 故意乱序 values。
    let values = vec![v(5, 50.0), v(1, 10.0), v(3, 30.0)];
    let qualities = vec![(5u16, 0x40u8), (1u16, 0xC0u8), (3u16, 0x00u8)];
    let ts_values = vec![(5u16, 500i64), (1u16, 100i64), (3u16, 300i64)];
    let server_ts_values = vec![(5u16, 5000i64), (1u16, 1000i64), (3u16, 3000i64)];
    // write_seqs 只给 col 1（部分缺失），且乱序。
    let write_seqs = vec![(1u16, Some(7u64))];

    let r = store.import_columns_typed(
        TenantId::DEFAULT,
        1,
        &values,
        &qualities,
        &ts_values,
        &server_ts_values,
        &write_seqs,
        false,
        None,
    );
    assert!(r.changed);

    let e = store.get(1).unwrap();
    // 五组按 column_id 升序，同下标对齐。
    assert_eq!(e.values, vec![v(1, 10.0), v(3, 30.0), v(5, 50.0)]);
    assert_eq!(e.qualities, vec![(1, 0xC0), (3, 0x00), (5, 0x40)]);
    assert_eq!(e.ts_values, vec![(1, 100), (3, 300), (5, 500)]);
    assert_eq!(e.server_ts_values, vec![(1, 1000), (3, 3000), (5, 5000)]);
    // write_seqs 缺列补 None：col 1 保留 Some(7)，col 3/5 补 None。
    assert_eq!(
        e.write_seqs,
        vec![(1, Some(7)), (3, None), (5, None)],
        "write_seqs 必须按 column_id 对齐（缺列补 None），不得张冠李戴"
    );
    // 兼容字段 = max(逐列)。
    assert_eq!(e.ts, 500);
    assert_eq!(e.server_ts, 5000);
}

// ─── A2：ts_values / server_ts_values 列子集不齐（防御性边界，最易错处）───

/// values 含 col 1/3/5，但 ts_values / server_ts_values 只含 col 3/5（子集）。
/// 期望：import_columns_typed 仍按 column_id 对齐五组 + 缺列兜底，而非张冠李戴。
///
/// 修复后：`import_columns_typed` 对 values/qualities/ts_values/server_ts_values/write_seqs
/// 均按 values 的 column_id 做 HashMap 对齐（缺列补语义安全默认值），不依赖
/// 「五组同列集」契约；本测试回归护栏已从 `#[ignore]` 转为真实断言。
#[test]
fn qa_import_subset_ts_values_align_by_id() {
    let store = make_store();

    let values = vec![v(1, 10.0), v(3, 30.0), v(5, 50.0)];
    let qualities = vec![(1u16, 0xC0u8), (3u16, 0x00u8), (5u16, 0x40u8)];
    // ts_values / server_ts_values 只含 col 3/5（子集，缺失 col 1）。
    let ts_values = vec![(5u16, 500i64), (3u16, 300i64)];
    let server_ts_values = vec![(5u16, 5000i64), (3u16, 3000i64)];
    let write_seqs: Vec<(u16, Option<u64>)> = vec![];

    let r = store.import_columns_typed(
        TenantId::DEFAULT,
        2,
        &values,
        &qualities,
        &ts_values,
        &server_ts_values,
        &write_seqs,
        false,
        None,
    );
    assert!(r.changed);

    let e = store.get(2).unwrap();
    // values 仍应完整 3 列。
    assert_eq!(e.values, vec![v(1, 10.0), v(3, 30.0), v(5, 50.0)]);

    // 关键断言：五组同长且同 column_id 对齐。
    // 缺列兜底后 ts_values 应为 3 项，col 3/5 的 ts 不张冠李戴到 col 1。
    assert_eq!(
        e.ts_values.len(),
        3,
        "ts_values 缺列时仍应与 values 同长（缺列兜底），不得静默丢列"
    );
    // col 3/5 的 ts 必须仍绑定各自列。
    let ts_by_col: HashMap<u16, i64> = e.ts_values.iter().copied().collect();
    assert_eq!(ts_by_col.get(&3), Some(&300), "col 3 的 ts 不得错位");
    assert_eq!(ts_by_col.get(&5), Some(&500), "col 5 的 ts 不得错位");

    // server_ts_values 同理。
    assert_eq!(e.server_ts_values.len(), 3, "server_ts_values 缺列时应补齐");
    let sts_by_col: HashMap<u16, i64> = e.server_ts_values.iter().copied().collect();
    assert_eq!(sts_by_col.get(&3), Some(&3000), "col 3 的 server_ts 不得错位");
    assert_eq!(sts_by_col.get(&5), Some(&5000), "col 5 的 server_ts 不得错位");
}

// ─── B：迁移 roundtrip 保真 ────────────────────────────────────────────────

/// 独立构造 SnapshotEntry（逐列 write_seqs 混合 Some/None + 逐列 ts/server_ts 不同），
/// 走 serialize_to_bytes → deserialize_from_bytes → import_columns_typed → 读回，
/// 断言逐列字段与迁移前完全一致（不压平、不丢 None）。
#[test]
fn qa_import_roundtrip_preserves_per_column_fields() {
    let store = make_store();

    // 混合 Some/None + 逐列 ts/server_ts 各不同。
    let entry = SnapshotEntry {
        tenant_id: TenantId(3),
        values: vec![v(0, 10.5), (2, TypedValue::I64(-7)), (7, TypedValue::Bool(true))],
        qualities: vec![(0, 0xC0), (2, 0x40), (7, 0x00)],
        ts: 3000,
        server_ts: 9000,
        version: 9,
        prev_value: Some(TypedValue::F64(10.0)),
        prev_ts: Some(1000),
        is_derived: false,
        writer_session_id: Some(42),
        ttl_expired: false,
        ts_values: vec![(0, 1000), (2, 2000), (7, 3000)],
        server_ts_values: vec![(0, 5000), (2, 4000), (7, 9000)],
        write_seqs: vec![(0, Some(7)), (2, None), (7, Some(99))],
    };

    let mut export: HashMap<u32, SnapshotEntry> = HashMap::new();
    export.insert(88, entry.clone());

    // 真 roundtrip：bincode 序列化 → 反序列化。
    let bytes = serialize_to_bytes(&export, 9).expect("serialize");
    let (restored, _version) = deserialize_from_bytes(&bytes).expect("deserialize");
    let restored_entry = restored.get(&88).expect("entry restored");

    // 逐列导入。
    let r = store.import_columns_typed(
        restored_entry.tenant_id,
        88,
        &restored_entry.values,
        &restored_entry.qualities,
        &restored_entry.ts_values,
        &restored_entry.server_ts_values,
        &restored_entry.write_seqs,
        restored_entry.is_derived,
        restored_entry.writer_session_id,
    );
    assert!(r.changed);

    let got = store.get(88).expect("imported entry");
    assert_eq!(got.values, entry.values);
    assert_eq!(got.qualities, entry.qualities);
    assert_eq!(
        got.ts_values, entry.ts_values,
        "逐列 ts_values 必须保真（不压平、不丢）"
    );
    assert_eq!(
        got.server_ts_values, entry.server_ts_values,
        "逐列 server_ts_values 必须保真（不压平、不丢）"
    );
    assert_eq!(
        got.write_seqs, entry.write_seqs,
        "逐列 write_seqs 必须保真（不压平、不丢 None）"
    );
    assert_eq!(got.ts, 3000);
    assert_eq!(got.server_ts, 9000);
    assert_eq!(got.tenant_id, TenantId(3));
    assert_eq!(got.writer_session_id, Some(42));
}

// ─── C：遗留空 write_seqs 兜底 ─────────────────────────────────────────────

/// write_seqs 为空（#[serde(default)] 旧快照）→ import 后不 panic，write_seqs 全 None。
#[test]
fn qa_import_legacy_empty_write_seqs_no_panic() {
    let store = make_store();
    let r = store.import_columns_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 1.0), v(1, 2.0)],
        &[(0, 0xC0), (1, 0xC0)],
        &[(0, 1000), (1, 2000)],
        &[(0, 1001), (1, 2001)],
        &[],
        false,
        None,
    );
    assert!(r.changed);
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![v(0, 1.0), v(1, 2.0)]);
    assert_eq!(e.write_seqs, vec![(0, None), (1, None)], "空 write_seqs 应全 None");
}

// ─── D：LWW 决胜在 import 场景 ─────────────────────────────────────────────

/// 迁移导入 entry（含 Some(seq)），再本地写同 ts 同列更大 seq → 新写覆盖迁移数据。
#[test]
fn qa_import_then_local_write_lww_higher_seq_wins() {
    let store = make_store();

    // 迁移导入：col 0，ts=100，seq=Some(5)。
    let r1 = store.import_columns_typed(
        TenantId::DEFAULT,
        1,
        &[v(0, 1.0)],
        &[(0, 0xC0)],
        &[(0, 100)],
        &[(0, 200)],
        &[(0, Some(5))],
        false,
        None,
    );
    assert!(r1.changed);

    // 本地写：同 ts=100，seq=Some(10)（更大）→ 必须覆盖迁移数据。
    let r2 = store.update_typed(
        TenantId::DEFAULT, 1, &[v(0, 9.0)], &[(0, 0x00)], 100, 999, Some(10), false, None,
    );
    assert!(r2.changed, "本地更大 write_seq 必须覆盖迁移导入的数据");

    let e = store.get(1).unwrap();
    assert!((e.value() - 9.0).abs() < 1e-9, "迁移值 1.0 应被本地 9.0 覆盖");
    assert_eq!(e.quality(), 0x00);
    assert_eq!(e.write_seqs, vec![(0, Some(10))], "覆盖后 write_seq 同步更新为 10");
    assert_eq!(e.server_ts, 999, "覆盖时 server_ts 同步更新");
}

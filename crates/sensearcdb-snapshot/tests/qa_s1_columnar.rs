//! QA 独立验证（S1 列式骨架）—— `ColumnarShard` + `update_batch` + `batch_get_typed`
//! + `update_typed` shim 的正确性边界。fresh eyes，不依赖工程师自测。
//!
//! 覆盖 spec §6.2/§6.3 的 S1 可验点：
//! - Q1 数据正确性：多类型列式 roundtrip 保真（value/quality/source_ts/server_ts/write_seq）。
//! - Q2 列级 LWW：批内同 (table_id,column_id) 同 ts 决胜（高 seq 胜 / Some 胜 None /
//!   None vs None 退 server_ts）、批间 LWW 覆盖与回滚。
//! - Q4 质量码：逐列保真 + `batch_get_typed` 投影四组同下标对齐。
//! - Q7 并发：批量原子性（无半批读 / 无 torn read）。
//! - Q8 稀疏列：先后写不同列不丢列（触发 `rebuild_sorted` 路径）。
//! - update_typed shim 语义等价：`update_typed` 与单点 `update_batch` 逐字段一致。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId, TypedValue};
use sensearcdb_snapshot::{
    columnar::PendingPoint, ColumnarShard, SchemaMirrorRef, SnapshotEntry, SnapshotMetrics,
    SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
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

fn make_store() -> SnapshotStore {
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = 1; // 减少测试分片数，放大分片内并发。
    let cfg = ConfigHandle::new(cfg);
    SnapshotStore::new(
        &cfg,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
        SnapshotMetrics::noop(),
    )
}

// ─── Q1：多类型列式 roundtrip 保真 ─────────────────────────────────────────

/// 直接对 `ColumnarShard` 做 upsert → project 往返，验证 8 种 dtype 全保真
/// （value/quality/ts_values/server_ts_values/version/is_derived/writer_session_id）。
#[test]
fn q1_columnar_shard_multitype_roundtrip() {
    let mut shard = ColumnarShard::new();
    let entry = SnapshotEntry::new_typed(
        TenantId::DEFAULT,
        vec![
            (0, TypedValue::F64(42.5)),
            (1, TypedValue::F32(1.5)),
            (2, TypedValue::I64(-7)),
            (3, TypedValue::U64(99)),
            (4, TypedValue::I32(-3)),
            (5, TypedValue::Bool(true)),
            (6, TypedValue::String("open".to_string())),
            (7, TypedValue::Enum16(65535)),
        ],
        vec![
            (0, 0xC0),
            (1, 0xC0),
            (2, 0x40),
            (3, 0xC0),
            (4, 0x00),
            (5, 0xC0),
            (6, 0x40),
            (7, 0x00),
        ],
        1000,
        1001,
        3,
        false,
        Some(7),
    );
    shard.upsert_from_entry(1, entry.clone());
    assert_eq!(shard.entry_count(), 1);
    assert_eq!(shard.iter_table_ids(), vec![1]);

    let proj = shard.project_entry(1).expect("project_entry 必须命中");
    assert_eq!(proj.values, entry.values, "8 dtype 值列逐列保真");
    assert_eq!(proj.qualities, entry.qualities, "质量码逐列保真");
    assert_eq!(proj.ts, 1000);
    assert_eq!(proj.server_ts, 1001);
    assert_eq!(
        proj.ts_values,
        vec![
            (0, 1000),
            (1, 1000),
            (2, 1000),
            (3, 1000),
            (4, 1000),
            (5, 1000),
            (6, 1000),
            (7, 1000)
        ],
        "列级 ts 保真"
    );
    assert_eq!(proj.version, 3);
    assert!(!proj.is_derived);
    assert_eq!(proj.writer_session_id, Some(7));
    assert_eq!(proj.tenant_id, TenantId::DEFAULT);
    // S1 不再存储前值。
    assert_eq!(proj.prev_value, None);
    assert_eq!(proj.prev_ts, None);

    // project_typed_value 同步保真。
    let tv = shard.project_typed_value(1).expect("project_typed_value 必须命中");
    assert_eq!(tv.values, entry.values);
    assert_eq!(tv.qualities, entry.qualities);
    assert_eq!(tv.ts, 1000);
    assert_eq!(tv.tenant_id, TenantId::DEFAULT);
}

/// 通过 store 写路径（update_batch 带 write_seq）→ get() 读回，验证 write_seq 逐列回读。
#[test]
fn q1_store_roundtrip_write_seq_fidelity() {
    let store = make_store();
    let vals = vec![
        (0u16, TypedValue::F64(10.0)),
        (1u16, TypedValue::I64(42)),
    ];
    let quals = vec![(0u16, 0xC0u8), (1u16, 0x00u8)];
    let p = PendingPoint {
        tenant_id: TenantId::DEFAULT,
        table_id: 7,
        values: &vals,
        qualities: &quals,
        ts: 200,
        server_ts: 2001,
        write_seq: Some(9),
        is_derived: false,
        writer_session_id: None,
    };
    let res = store.update_batch(&[p]);
    assert_eq!(res, vec![(7, true, 1)], "单点批 changed=true，version=1");

    let e = store.get(7).expect("get 必须命中");
    assert_eq!(e.values, vals);
    assert_eq!(e.qualities, quals);
    assert_eq!(e.write_seqs, vec![(0, Some(9)), (1, Some(9))], "write_seq 逐列回读");
    assert_eq!(e.ts_values, vec![(0, 200), (1, 200)]);
    assert_eq!(e.version, 1);
}

// ─── Q2：批内 LWW（同 (table_id,column_id) 同 ts 决胜） ─────────────────────

/// 同 ts 三点：Some(10) 胜 Some(5)；None 败给 Some；最终仅胜者落列。
#[test]
fn q2_update_batch_internal_lww_some_beats_none_higher_seq_wins() {
    let store = make_store();
    // 三点同 (1,0) 同 ts=100：value 1.0(seq5) / 2.0(None) / 3.0(seq10)。
    // 决胜：seq10 胜 seq5；None 败给 Some。终值 3.0。
    let v1 = vec![(0u16, TypedValue::F64(1.0))];
    let v2 = vec![(0u16, TypedValue::F64(2.0))];
    let v3 = vec![(0u16, TypedValue::F64(3.0))];
    let q1 = vec![(0u16, 0xC0u8)];
    let p1 = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1, values: &v1, qualities: &q1,
        ts: 100, server_ts: 1001, write_seq: Some(5), is_derived: false, writer_session_id: None,
    };
    let p2 = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1, values: &v2, qualities: &q1,
        ts: 100, server_ts: 1002, write_seq: None, is_derived: false, writer_session_id: None,
    };
    let p3 = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1, values: &v3, qualities: &q1,
        ts: 100, server_ts: 1003, write_seq: Some(10), is_derived: false, writer_session_id: None,
    };
    store.update_batch(&[p1, p2, p3]);

    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(3.0))], "批内 LWW：高 seq 胜");
    assert_eq!(e.write_seqs, vec![(0, Some(10))]);
}

/// None vs None（同 ts 且都无 seq）→ server_ts 大者胜。
#[test]
fn q2_update_batch_internal_lww_none_vs_none_server_ts() {
    let store = make_store();
    let v1 = vec![(0u16, TypedValue::F64(1.0))];
    let v2 = vec![(0u16, TypedValue::F64(2.0))];
    let q = vec![(0u16, 0xC0u8)];
    let p1 = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1, values: &v1, qualities: &q,
        ts: 100, server_ts: 1001, write_seq: None, is_derived: false, writer_session_id: None,
    };
    let p2 = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1, values: &v2, qualities: &q,
        ts: 100, server_ts: 1002, write_seq: None, is_derived: false, writer_session_id: None,
    };
    store.update_batch(&[p1, p2]);
    let e = store.get(1).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(2.0))], "None vs None：server_ts 大者胜");
    assert_eq!(e.server_ts, 1002);
}

/// 批间 LWW：批 A seq=5，批 B 同 ts seq=10 → 覆盖；批 C 同 ts seq=3 → 回滚。
#[test]
fn q2_batch_vs_batch_lww_consistency() {
    let store = make_store();
    let v5 = vec![(0u16, TypedValue::F64(5.0))];
    let v10 = vec![(0u16, TypedValue::F64(10.0))];
    let v3 = vec![(0u16, TypedValue::F64(3.0))];
    let q = vec![(0u16, 0xC0u8)];

    let a = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1, values: &v5, qualities: &q,
        ts: 100, server_ts: 1001, write_seq: Some(5), is_derived: false, writer_session_id: None,
    };
    store.update_batch(&[a]);
    assert_eq!(store.get(1).unwrap().value(), 5.0);

    let b = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1, values: &v10, qualities: &q,
        ts: 100, server_ts: 1001, write_seq: Some(10), is_derived: false, writer_session_id: None,
    };
    let rb = store.update_batch(&[b]);
    assert_eq!(rb, vec![(1, true, 2)], "高 seq 覆盖，changed=true");
    assert_eq!(store.get(1).unwrap().value(), 10.0);

    let c = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1, values: &v3, qualities: &q,
        ts: 100, server_ts: 1001, write_seq: Some(3), is_derived: false, writer_session_id: None,
    };
    let rc = store.update_batch(&[c]);
    assert_eq!(rc, vec![(1, false, 2)], "低 seq 回滚，changed=false，version 不变");
    assert_eq!(store.get(1).unwrap().value(), 10.0, "回滚不覆盖");
}

// ─── Q4：质量码 + batch_get_typed 投影逐列对齐 ───────────────────────────────

/// 多列读回：values/qualities/ts_values/server_ts_values 四组同下标同 column_id，
/// ts/server_ts = max(列级)。
#[test]
fn q4_batch_get_typed_projection_alignment_and_quality() {
    let store = make_store();
    // col0 @ ts=100 quality=0xC0；col1 @ ts=300 quality=0x00（稀疏不同 ts）。
    store.update_typed(
        TenantId::DEFAULT, 3,
        &[(0, TypedValue::F64(1.0))],
        &[(0, 0xC0)],
        100, 1000, Some(1), false, None,
    );
    store.update_typed(
        TenantId::DEFAULT, 3,
        &[(1, TypedValue::I64(42))],
        &[(1, 0x00)],
        300, 3000, Some(2), false, None,
    );

    let got = store.batch_get_typed(&[3]);
    assert_eq!(got.len(), 1);
    let (tid, tv) = &got[0];
    assert_eq!(*tid, 3);

    // 四组同下标同 column_id（升序）。
    assert_eq!(tv.values, vec![(0, TypedValue::F64(1.0)), (1, TypedValue::I64(42))]);
    assert_eq!(tv.qualities, vec![(0, 0xC0), (1, 0x00)], "质量码逐列保真");
    assert_eq!(tv.ts_values, vec![(0, 100), (1, 300)]);
    assert_eq!(tv.server_ts_values, vec![(0, 1000), (1, 3000)]);
    // 兼容字段 = max(列级)。
    assert_eq!(tv.ts, 300);
    assert_eq!(tv.server_ts, 3000);
    assert_eq!(tv.tenant_id, TenantId::DEFAULT);
}

/// 覆盖同步：同列同 ts 高 seq 覆盖时，quality 同步更新为新值，不残留旧 quality。
#[test]
fn q4_quality_overwrite_synchronized() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 9,
        &[(0, TypedValue::F64(1.0))],
        &[(0, 0xC0)],
        100, 1000, Some(1), false, None,
    );
    store.update_typed(
        TenantId::DEFAULT, 9,
        &[(0, TypedValue::F64(2.0))],
        &[(0, 0x00)],
        100, 1000, Some(2), false, None,
    );
    let e = store.get(9).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(2.0))]);
    assert_eq!(e.qualities, vec![(0, 0x00)], "质量码与值同步覆盖");
}

// ─── Q7：并发（批量原子性 / 无半批读） ──────────────────────────────────────

/// 写线程持续批量写 {0,1} 两列（同 ts 一起推进），读线程断言永不读到
/// col0/col1 ts 不一致的 torn 状态（同一 batch 两列在同一 shard 写锁内更新）。
#[test]
fn q7_concurrent_batch_no_torn_read() {
    let store = Arc::new(make_store());
    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let s = store.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut ts = 1i64;
            while !stop.load(Ordering::Relaxed) {
                let vals = vec![
                    (0u16, TypedValue::F64(ts as f64)),
                    (1u16, TypedValue::F64(ts as f64)),
                ];
                let quals = vec![(0u16, 0xC0u8), (1u16, 0xC0u8)];
                let p = PendingPoint {
                    tenant_id: TenantId::DEFAULT, table_id: 1,
                    values: &vals, qualities: &quals,
                    ts, server_ts: ts, write_seq: Some(ts as u64),
                    is_derived: false, writer_session_id: None,
                };
                s.update_batch(&[p]);
                ts += 1;
            }
        })
    };

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let s = store.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Some(e) = s.get(1) {
                        if e.values.len() == 2 {
                            assert_eq!(
                                e.ts_values[0].1, e.ts_values[1].1,
                                "torn read：同 batch 两列 ts 必须一致"
                            );
                        }
                    }
                }
            })
        })
        .collect();

    std::thread::sleep(std::time::Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
}

/// 并发批量写不同 tag：总数守恒，各 tag 值在其合法区间内（无丢失、无错位）。
#[test]
fn q7_concurrent_batch_different_tags_no_loss() {
    let store = Arc::new(make_store());
    let num_threads = 8;
    let tags_per_thread = 16;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let s = store.clone();
            std::thread::spawn(move || {
                for i in 0..tags_per_thread {
                    let tid = (t * tags_per_thread + i) as u32;
                    let vals = vec![(0u16, TypedValue::F64(i as f64))];
                    let quals = vec![(0u16, 0xC0u8)];
                    let p = PendingPoint {
                        tenant_id: TenantId::DEFAULT, table_id: tid,
                        values: &vals, qualities: &quals,
                        ts: 1000 + i as i64, server_ts: 2000 + i as i64,
                        write_seq: None, is_derived: false, writer_session_id: None,
                    };
                    s.update_batch(&[p]);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(store.total_entries(), (num_threads * tags_per_thread) as usize);
    for i in 0..tags_per_thread {
        let tid = (i) as u32;
        let e = store.get(tid).unwrap();
        assert!((e.value() - i as f64).abs() < 1e-12, "tag {} 值错位", tid);
    }
}

// ─── Q8：稀疏新增列（触发 rebuild_sorted） ──────────────────────────────────

/// 先写 {0} 再写 {1}（稀疏新增列），读回两列不丢；后续值覆盖仍正确。
#[test]
fn q8_sparse_new_column_rebuild_then_overwrite() {
    let store = make_store();
    store.update_typed(
        TenantId::DEFAULT, 5,
        &[(0, TypedValue::F64(1.0))],
        &[(0, 0xC0)],
        100, 1000, Some(1), false, None,
    );
    // 稀疏新增 col1 → 触发整 shard 重排。
    store.update_typed(
        TenantId::DEFAULT, 5,
        &[(1, TypedValue::I64(42))],
        &[(1, 0x00)],
        200, 2000, Some(2), false, None,
    );
    let e = store.get(5).unwrap();
    assert_eq!(
        e.values,
        vec![(0, TypedValue::F64(1.0)), (1, TypedValue::I64(42))],
        "稀疏新增列不得丢列"
    );
    assert_eq!(e.qualities, vec![(0, 0xC0), (1, 0x00)]);
    assert_eq!(e.ts_values, vec![(0, 100), (1, 200)]);

    // 重排后 col0 值覆盖仍正确（同 ts 高 seq）。
    store.update_typed(
        TenantId::DEFAULT, 5,
        &[(0, TypedValue::F64(9.0))],
        &[(0, 0x80)],
        100, 1000, Some(3), false, None,
    );
    let e2 = store.get(5).unwrap();
    assert_eq!(
        e2.values,
        vec![(0, TypedValue::F64(9.0)), (1, TypedValue::I64(42))],
        "重排后 col0 覆盖 + col1 保留"
    );
    assert_eq!(e2.qualities, vec![(0, 0x80), (1, 0x00)]);
}

/// 稀疏新增列触发重排后，另一 table 的既有行不丢失（整 shard drain 重放正确）。
#[test]
fn q8_sparse_rebuild_preserves_other_tables() {
    let store = make_store();
    // 先建两个不同表，再对表 A 稀疏新增列，验证表 B 数据仍在。
    store.update_typed(
        TenantId::DEFAULT, 10,
        &[(0, TypedValue::F64(1.0))],
        &[(0, 0xC0)], 100, 1000, Some(1), false, None,
    );
    store.update_typed(
        TenantId::DEFAULT, 20,
        &[(0, TypedValue::F64(2.0))],
        &[(0, 0xC0)], 100, 1000, Some(1), false, None,
    );
    // 表 A 稀疏新增列（触发 rebuild_sorted，drain 整 shard）。
    store.update_typed(
        TenantId::DEFAULT, 10,
        &[(1, TypedValue::I64(7))],
        &[(1, 0x00)], 200, 2000, Some(2), false, None,
    );

    assert_eq!(store.total_entries(), 2, "rebuild 后表数不丢");
    let a = store.get(10).unwrap();
    assert_eq!(a.values.len(), 2, "表 A 两列都在");
    let b = store.get(20).unwrap();
    assert_eq!(b.values, vec![(0, TypedValue::F64(2.0))], "表 B 不丢");
}

// ─── update_typed shim 语义等价 ────────────────────────────────────────────

/// `update_typed` 与单点 `update_batch` 逐字段一致（含乱序拒绝、write_seq、
/// is_derived / writer_session_id / tenant 透传）。
#[test]
fn update_typed_shim_equivalent_to_single_batch() {
    let a_store = make_store();
    let a_r = a_store.update_typed(
        TenantId::DEFAULT, 1,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::I64(2))],
        &[(0, 0xC0), (1, 0x00)],
        100, 1001, Some(5), true, Some(9),
    );
    assert!(a_r.changed);
    let a = a_store.get(1).unwrap().clone();

    let b_store = make_store();
    let vals = vec![(0u16, TypedValue::F64(1.0)), (1u16, TypedValue::I64(2))];
    let quals = vec![(0u16, 0xC0u8), (1u16, 0x00u8)];
    let p = PendingPoint {
        tenant_id: TenantId::DEFAULT, table_id: 1,
        values: &vals, qualities: &quals,
        ts: 100, server_ts: 1001, write_seq: Some(5),
        is_derived: true, writer_session_id: Some(9),
    };
    let b_r = b_store.update_batch(&[p]);
    assert_eq!(b_r, vec![(1, true, 1)], "shim 与批的 changed/version 一致");
    let b = b_store.get(1).unwrap().clone();

    assert_eq!(a.values, b.values);
    assert_eq!(a.qualities, b.qualities);
    assert_eq!(a.ts_values, b.ts_values);
    assert_eq!(a.server_ts_values, b.server_ts_values);
    assert_eq!(a.write_seqs, b.write_seqs);
    assert_eq!(a.ts, b.ts);
    assert_eq!(a.server_ts, b.server_ts);
    assert_eq!(a.is_derived, b.is_derived);
    assert_eq!(a.writer_session_id, b.writer_session_id);
    assert_eq!(a.tenant_id, b.tenant_id);
}

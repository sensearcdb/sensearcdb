//! QA 独立边界测试（S1 列式骨架，fresh eyes）。
//!
//! 针对工程师可能未覆盖的边界（spec 未逐条列出的组合）：
//! - D1：可空/bool/string 列「整列重建」覆盖中间一行时，其余行值/validity 不丢（B1 风险点）。
//! - D2：同 ts 低 seq 回滚 + 列级隔离组合（col0 拒、col1 收，互不串）。
//! - D3：序列化 roundtrip 后 LWW 继续正确（决胜键 write_seq 不丢）。
//! - D4：序列化 roundtrip 后逐 tag version 保真（探查版本是否被 shard_version 覆盖）。

use std::sync::Arc;

use sensearcdb_common::{DataType, TenantId, TypedValue};
use sensearcdb_snapshot::{
    deserialize_from_file, serialize_to_file, ColumnarShard, SchemaMirrorRef, SnapshotMetrics,
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

fn make_sub_mgr() -> Arc<SnapshotSubscriptionManager> {
    Arc::new(SnapshotSubscriptionManager::new())
}

fn make_metadata() -> Arc<TagMetadataStore> {
    Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)))
}

/// 构造固定桶数的 store（分桶确定性，便于 serialize/deserialize 同桶数往返）。
fn make_store_with_bucket_count(count: usize) -> SnapshotStore {
    let buckets: Vec<ColumnarShard> = (0..count).map(|_| ColumnarShard::new()).collect();
    SnapshotStore::from_buckets(
        buckets,
        0,
        make_sub_mgr(),
        make_metadata(),
        SnapshotMetrics::noop(),
    )
}

// ─── D1：可空/bool/string 列「整列重建」覆盖中间一行不丢其他行 ──────────────

/// 三列 String 表：覆盖中间列（触发 `rebuild_string` 整列重建），
/// 断言其余行值不丢、不串、不误改 validity。
#[test]
fn d1_string_mid_row_overwrite_preserves_others() {
    let store = make_store_with_bucket_count(8);
    // 一次性写入 col0/col1/col2 三列 String。
    store.update_typed(
        TenantId::DEFAULT,
        102,
        &[
            (0, TypedValue::String("aaa".into())),
            (1, TypedValue::String("bbb".into())),
            (2, TypedValue::String("ccc".into())),
        ],
        &[(0, 0xC0), (1, 0xC0), (2, 0xC0)],
        100,
        1000,
        Some(1),
        false,
        None,
    );
    // 稀疏覆盖中间列 col1（新 ts 更高 → LWW 胜 → rebuild_string）。
    store.update_typed(
        TenantId::DEFAULT,
        102,
        &[(1, TypedValue::String("BBB".into()))],
        &[(1, 0x00)],
        200,
        2000,
        Some(2),
        false,
        None,
    );

    let e = store.get(102).unwrap();
    assert_eq!(
        e.values,
        vec![
            (0, TypedValue::String("aaa".into())),
            (1, TypedValue::String("BBB".into())),
            (2, TypedValue::String("ccc".into())),
        ],
        "整列重建后：col0/col2 不丢，仅 col1 变"
    );
    assert_eq!(e.qualities, vec![(0, 0xC0), (1, 0x00), (2, 0xC0)]);
}

/// 三列 Bool 表：覆盖中间列（触发 `rebuild_bool`），其余行 validity 不误改。
#[test]
fn d1_bool_mid_row_overwrite_preserves_others() {
    let store = make_store_with_bucket_count(8);
    store.update_typed(
        TenantId::DEFAULT,
        103,
        &[
            (0, TypedValue::Bool(true)),
            (1, TypedValue::Bool(true)),
            (2, TypedValue::Bool(true)),
        ],
        &[(0, 0xC0), (1, 0xC0), (2, 0xC0)],
        100,
        1000,
        Some(1),
        false,
        None,
    );
    store.update_typed(
        TenantId::DEFAULT,
        103,
        &[(1, TypedValue::Bool(false))],
        &[(1, 0x00)],
        200,
        2000,
        Some(2),
        false,
        None,
    );

    let e = store.get(103).unwrap();
    assert_eq!(
        e.values,
        vec![
            (0, TypedValue::Bool(true)),
            (1, TypedValue::Bool(false)),
            (2, TypedValue::Bool(true)),
        ],
        "bool 整列重建后其余行不变"
    );
}

/// 可空 write_seq 列：覆盖中间一行的 write_seq None→Some（触发 `rebuild_nullable`），
/// 其余行的 write_seq 仍为 None（validity 不误改）。
#[test]
fn d1_write_seq_null_state_mid_row_preserves_others() {
    let store = make_store_with_bucket_count(8);
    // 三列 F64，初始 write_seq 全 None。
    store.update_typed(
        TenantId::DEFAULT,
        101,
        &[
            (0, TypedValue::F64(1.0)),
            (1, TypedValue::F64(2.0)),
            (2, TypedValue::F64(3.0)),
        ],
        &[(0, 0xC0), (1, 0xC0), (2, 0xC0)],
        100,
        1000,
        None,
        false,
        None,
    );
    // 同 ts 覆盖中间列 col1，write_seq Some(5)（Some > None 胜），且值变化 → overwrite。
    // 此时 write_seq 列 None→Some，走 rebuild_nullable。
    store.update_typed(
        TenantId::DEFAULT,
        101,
        &[(1, TypedValue::F64(20.0))],
        &[(1, 0x00)],
        100,
        1000,
        Some(5),
        false,
        None,
    );

    let e = store.get(101).unwrap();
    assert_eq!(
        e.values,
        vec![
            (0, TypedValue::F64(1.0)),
            (1, TypedValue::F64(20.0)),
            (2, TypedValue::F64(3.0)),
        ],
        "值仅 col1 变"
    );
    assert_eq!(
        e.write_seqs,
        vec![(0, None), (1, Some(5)), (2, None)],
        "write_seq 仅 col1 变 Some(5)，col0/col2 保持 None（validity 不误改）"
    );
}

// ─── D2：同 ts 低 seq 回滚 + 列级隔离组合 ────────────────────────────────────

/// 同一次写入带 col0（低 seq，应拒）与 col1（高 seq，应收），
/// 确认列级独立决胜、互不串、拒列四元组完全不变。
///
/// 注：`update_typed`/`PendingPoint` 只承载「单点共享 write_seq」，无法表达
/// 逐列不同 seq；逐列独立 write_seq 必须走 `import_columns_typed`。
#[test]
fn d2_same_ts_low_seq_rollback_with_column_isolation() {
    let store = make_store_with_bucket_count(8);
    // 初始 col0/col1 均 ts=100、seq=Some(10)。
    store.import_columns_typed(
        TenantId::DEFAULT,
        200,
        &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
        &[(0, 0xC0), (1, 0xC0)],
        &[(0, 100), (1, 100)],
        &[(0, 1000), (1, 1000)],
        &[(0, Some(10)), (1, Some(10))],
        false,
        None,
    );
    // 同 ts：col0 seq=Some(5)（低，拒）；col1 seq=Some(20)（高，收）。
    store.import_columns_typed(
        TenantId::DEFAULT,
        200,
        &[(0, TypedValue::F64(11.0)), (1, TypedValue::F64(22.0))],
        &[(0, 0x00), (1, 0x00)],
        &[(0, 100), (1, 100)],
        &[(0, 1000), (1, 1000)],
        &[(0, Some(5)), (1, Some(20))],
        false,
        None,
    );

    let e = store.get(200).unwrap();
    // col0 保持旧值（低 seq 回滚）；col1 更新为新值（高 seq 覆盖）。
    assert_eq!(
        e.values,
        vec![(0, TypedValue::F64(1.0)), (1, TypedValue::F64(22.0))],
        "col0 拒（回滚）、col1 收"
    );
    assert_eq!(e.qualities, vec![(0, 0xC0), (1, 0x00)], "col0 质量码不变，col1 更新");
    // 列级隔离：col0 的 write_seq 保持 Some(10)（回滚不覆盖）。
    assert_eq!(e.write_seqs, vec![(0, Some(10)), (1, Some(20))]);
    assert_eq!(e.ts_values, vec![(0, 100), (1, 100)]);
}

// ─── D3：序列化 roundtrip 后 LWW 继续正确 ───────────────────────────────────

/// 落盘→恢复后，write_seq 决胜键不丢，同 ts 高 seq 覆盖、低 seq 回滚仍正确。
#[test]
fn d3_serialize_roundtrip_then_lww_continues() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    const BUCKETS: usize = 8;

    let store = make_store_with_bucket_count(BUCKETS);
    store.update_typed(
        TenantId::DEFAULT,
        300,
        &[(0, TypedValue::F64(5.0))],
        &[(0, 0xC0)],
        100,
        1000,
        Some(5),
        false,
        None,
    );

    // 落盘。
    let (guards, gv) = store.snapshot_all_buckets();
    serialize_to_file(&path, &guards, gv, &SnapshotMetrics::noop(), None, 0).unwrap();
    drop(guards);

    // 恢复。
    let metrics = SnapshotMetrics::noop();
    let (shards, version, _wm) =
        deserialize_from_file(&path, BUCKETS, &metrics, None).unwrap();
    let store2 = SnapshotStore::from_buckets(
        shards,
        version,
        make_sub_mgr(),
        make_metadata(),
        SnapshotMetrics::noop(),
    );

    // 恢复后先确认原值 + write_seq 都在。
    let e = store2.get(300).unwrap();
    assert_eq!(e.values, vec![(0, TypedValue::F64(5.0))]);
    assert_eq!(e.write_seqs, vec![(0, Some(5))], "roundtrip 后 write_seq 不丢");

    // 同 ts 高 seq → 覆盖。
    store2.update_typed(
        TenantId::DEFAULT,
        300,
        &[(0, TypedValue::F64(9.0))],
        &[(0, 0x00)],
        100,
        1000,
        Some(10),
        false,
        None,
    );
    assert_eq!(store2.get(300).unwrap().value(), 9.0, "高 seq 覆盖仍正确");

    // 同 ts 低 seq → 回滚。
    store2.update_typed(
        TenantId::DEFAULT,
        300,
        &[(0, TypedValue::F64(3.0))],
        &[(0, 0x80)],
        100,
        1000,
        Some(3),
        false,
        None,
    );
    let e2 = store2.get(300).unwrap();
    assert_eq!(e2.value(), 9.0, "低 seq 回滚仍正确");
    assert_eq!(e2.write_seqs, vec![(0, Some(10))], "决胜键保持为高 seq");
}

// ─── D4：序列化 roundtrip 后逐 tag version 保真（探查） ─────────────────────

/// 期望：每个 tag 的 version 是它最后一次变化的全局版本号，roundtrip 后应保真。
/// （若失败：说明 deserialize 把 entry.version 覆盖为 shard_version / 0。）
#[test]
fn d4_serialize_roundtrip_preserves_entry_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    const BUCKETS: usize = 8;

    let store = make_store_with_bucket_count(BUCKETS);
    // tag 400 version=1；tag 401 version=2（版本应各不相同）。
    store.update_typed(
        TenantId::DEFAULT,
        400,
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
        401,
        &[(0, TypedValue::F64(2.0))],
        &[(0, 0xC0)],
        100,
        1000,
        None,
        false,
        None,
    );

    let (guards, gv) = store.snapshot_all_buckets();
    serialize_to_file(&path, &guards, gv, &SnapshotMetrics::noop(), None, 0).unwrap();
    drop(guards);

    let metrics = SnapshotMetrics::noop();
    let (shards, version, _wm) =
        deserialize_from_file(&path, BUCKETS, &metrics, None).unwrap();
    let store2 = SnapshotStore::from_buckets(
        shards,
        version,
        make_sub_mgr(),
        make_metadata(),
        SnapshotMetrics::noop(),
    );

    assert_eq!(
        store2.get(400).unwrap().version,
        1,
        "tag 400 的逐 tag version 应在 roundtrip 后保真"
    );
    assert_eq!(
        store2.get(401).unwrap().version,
        2,
        "tag 401 的逐 tag version 应在 roundtrip 后保真"
    );
}

// ─── D5：rehash 重分区后逐 tag version 保真（跨 shard 迁移不丢版本） ───────

/// 序列化用 4 桶、反序列化用 8 桶（触发 rehash 分支 `serialize.rs:474`），
/// 确认重分区后每个 tag 的 version 仍为其自身 entry.version，不被 0 冲刷。
#[test]
fn d5_rehash_roundtrip_preserves_entry_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    const SRC_BUCKETS: usize = 4;
    const DST_BUCKETS: usize = 8;

    let store = make_store_with_bucket_count(SRC_BUCKETS);
    // tag 400 version=1；tag 401 version=2。
    store.update_typed(
        TenantId::DEFAULT,
        400,
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
        401,
        &[(0, TypedValue::F64(2.0))],
        &[(0, 0xC0)],
        100,
        1000,
        None,
        false,
        None,
    );

    let (guards, gv) = store.snapshot_all_buckets();
    serialize_to_file(&path, &guards, gv, &SnapshotMetrics::noop(), None, 0).unwrap();
    drop(guards);

    // 用不同的桶数反序列化 → 触发 rehash 重分区。
    let metrics = SnapshotMetrics::noop();
    let (shards, version, _wm) =
        deserialize_from_file(&path, DST_BUCKETS, &metrics, None).unwrap();
    let store2 = SnapshotStore::from_buckets(
        shards,
        version,
        make_sub_mgr(),
        make_metadata(),
        SnapshotMetrics::noop(),
    );

    assert_eq!(
        store2.get(400).unwrap().version,
        1,
        "rehash 后 tag 400 的逐 tag version 应保真"
    );
    assert_eq!(
        store2.get(401).unwrap().version,
        2,
        "rehash 后 tag 401 的逐 tag version 应保真"
    );
}

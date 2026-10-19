//! QA 独立边界验证：snapshot 分桶数变更后的 re-hash 重放正确性。
//!
//! 独立于工程师自测（fresh eyes）。覆盖：
//! - 升级方向 64→256（逐条落桶 + 总数守恒 + 黑盒 get + 全量枚举不重不漏）
//! - 降档方向 256→4、256→16
//! - 空 snapshot、单条目、跨桶碰撞 table_id、shard_count=1、极大 shard_count
//! - 反序列化中途失败（截断文件必须 Err，不得 panic）
//!
//! 判定基准：红线不变式「同 table_id 恒落同桶」，桶索引 = fxhash::hash(tid) % bucket_count。

use std::collections::HashMap;
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

/// 构造指定桶数的 store（from_buckets 的 bucket_count = buckets.len()，可控）。
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

/// 写入单列 F64 条目（value = tid as f64，用于值正确性校验）。
fn write_entry(store: &SnapshotStore, tid: u32) {
    store.update_typed(
        TenantId::DEFAULT,
        tid,
        &[(0, TypedValue::F64(tid as f64))],
        &[(0, 0xC0)],
        1000 + tid as i64,
        2000 + tid as i64,
        None,
        false,
        None,
    );
}

/// 构造「旧桶数 = old_count」的 snapshot.bin，写入 tids 集合，返回文件路径。
fn build_snapshot_with_old_bucket_count(
    dir: &std::path::Path,
    old_count: usize,
    tids: &[u32],
) -> std::path::PathBuf {
    let path = dir.join("snapshot.bin");
    let store = make_store_with_bucket_count(old_count);
    for &tid in tids {
        write_entry(&store, tid);
    }
    let (guards, version) = store.snapshot_all_buckets();
    serialize_to_file(&path, &guards, version, &SnapshotMetrics::noop(), None, 0).unwrap();
    drop(guards);
    path
}

/// 核心校验：deserialize 到 new_count 桶后，逐条落桶 + 总数守恒 + 黑盒 get + 全量枚举不重不漏。
fn assert_rehash_correct(shards: Vec<ColumnarShard>, new_count: usize, tids: &[u32]) {
    // ① 逐条断言：每条 tid 必须落在 fxhash::hash(tid) % new_count 桶。
    for (bucket_idx, bucket) in shards.iter().enumerate() {
        for tid in bucket.iter_table_ids() {
            assert_eq!(
                fxhash::hash(&tid) as usize % new_count,
                bucket_idx,
                "table_id {} 未落在正确桶（数据错位）",
                tid
            );
        }
    }

    // ② 总数守恒：re-hash 后不得丢失、不得重复。
    let total: usize = shards.iter().map(|s| s.entry_count()).sum();
    assert_eq!(total, tids.len(), "re-hash 后条目总数不守恒");

    // ③ 黑盒 get：用新 store 逐条查询，必须查到且值正确（同时证明 get() 与 re-hash
    //    用同一 hash 函数 + 同一取模——若不一致则 get() 会查不到）。
    let store = SnapshotStore::from_buckets(
        shards,
        0,
        make_sub_mgr(),
        make_metadata(),
        SnapshotMetrics::noop(),
    );
    for &tid in tids {
        let entry = store.get(tid).expect("re-hash 后 get() 查不到条目（数据丢失）");
        assert!(
            (entry.value() - tid as f64).abs() < 1e-12,
            "table_id {} 值不匹配",
            tid
        );
    }

    // ④ 全量枚举不重不漏：list_all 返回 tids 且唯一。
    let (all, next) = store.list_all(None, usize::MAX, TenantId::DEFAULT);
    assert!(next.is_none(), "list_all 应一次扫完");
    let mut got: Vec<u32> = all.iter().map(|(t, _)| *t).collect();
    got.sort_unstable();
    let mut expect: Vec<u32> = tids.to_vec();
    expect.sort_unstable();
    assert_eq!(got, expect, "全量枚举不重不漏");
}

#[test]
fn test_upgrade_64_to_256_rehash() {
    let dir = tempfile::tempdir().unwrap();
    let tids: Vec<u32> = (1u32..=2000).collect();
    let path = build_snapshot_with_old_bucket_count(dir.path(), 64, &tids);

    let metrics = SnapshotMetrics::noop();
    let (shards, version, _wm) = deserialize_from_file(&path, 256, &metrics, None).unwrap();
    assert_eq!(shards.len(), 256);
    assert_eq!(version, tids.len() as u64, "global_version 应守恒");
    assert_rehash_correct(shards, 256, &tids);
}

#[test]
fn test_downgrade_256_to_4_rehash() {
    let dir = tempfile::tempdir().unwrap();
    let tids: Vec<u32> = (1u32..=2000).collect();
    let path = build_snapshot_with_old_bucket_count(dir.path(), 256, &tids);

    let metrics = SnapshotMetrics::noop();
    let (shards, version, _wm) = deserialize_from_file(&path, 4, &metrics, None).unwrap();
    assert_eq!(shards.len(), 4);
    assert_eq!(version, tids.len() as u64);
    assert_rehash_correct(shards, 4, &tids);
}

#[test]
fn test_downgrade_256_to_16_rehash() {
    let dir = tempfile::tempdir().unwrap();
    let tids: Vec<u32> = (1u32..=2000).collect();
    let path = build_snapshot_with_old_bucket_count(dir.path(), 256, &tids);

    let metrics = SnapshotMetrics::noop();
    let (shards, version, _wm) = deserialize_from_file(&path, 16, &metrics, None).unwrap();
    assert_eq!(shards.len(), 16);
    assert_eq!(version, tids.len() as u64);
    assert_rehash_correct(shards, 16, &tids);
}

#[test]
fn test_empty_snapshot_rehash() {
    let dir = tempfile::tempdir().unwrap();
    let tids: Vec<u32> = Vec::new();
    let path = build_snapshot_with_old_bucket_count(dir.path(), 64, &tids);

    let metrics = SnapshotMetrics::noop();
    let (shards, version, _wm) = deserialize_from_file(&path, 256, &metrics, None).unwrap();
    assert_eq!(shards.len(), 256);
    assert_eq!(version, 0, "空 snapshot 的 global_version 应为 0");
    assert_rehash_correct(shards, 256, &tids);
}

#[test]
fn test_single_entry_rehash() {
    let dir = tempfile::tempdir().unwrap();
    let tids = vec![42u32];
    let path = build_snapshot_with_old_bucket_count(dir.path(), 64, &tids);

    let metrics = SnapshotMetrics::noop();
    let (shards, _version, _wm) = deserialize_from_file(&path, 256, &metrics, None).unwrap();
    assert_rehash_correct(shards, 256, &tids);
}

/// 找「旧桶同→新桶异」碰撞对（升级拆分方向）。
fn find_same_old_diff_new(old: usize, new: usize) -> (u32, u32) {
    let mut by_old: HashMap<usize, HashMap<usize, u32>> = HashMap::new();
    for tid in 1u32..=1_000_000u32 {
        let o = fxhash::hash(&tid) as usize % old;
        let n = fxhash::hash(&tid) as usize % new;
        let m = by_old.entry(o).or_default();
        if let Some((&other_n, &other_tid)) = m.iter().find(|(&k, _)| k != n) {
            let _ = other_n;
            return (other_tid, tid);
        }
        m.insert(n, tid);
    }
    panic!("未找到「旧桶同→新桶异」碰撞对");
}

/// 找「旧桶异→新桶同」碰撞对（降档合并方向）。
fn find_diff_old_same_new(old: usize, new: usize) -> (u32, u32) {
    let mut by_new: HashMap<usize, HashMap<usize, u32>> = HashMap::new();
    for tid in 1u32..=1_000_000u32 {
        let o = fxhash::hash(&tid) as usize % old;
        let n = fxhash::hash(&tid) as usize % new;
        let m = by_new.entry(n).or_default();
        if let Some((&other_o, &other_tid)) = m.iter().find(|(&k, _)| k != o) {
            let _ = other_o;
            return (other_tid, tid);
        }
        m.insert(o, tid);
    }
    panic!("未找到「旧桶异→新桶同」碰撞对");
}

/// 跨桶碰撞：升级方向「旧桶同→新桶异」（拆分）、降档方向「旧桶异→新桶同」（合并），
/// 验证 re-hash 后两条都不丢、都落在各自正确的新桶。
#[test]
fn test_cross_bucket_collision_rehash() {
    // 升级 64→256：旧桶同→新桶异。
    let (a, b) = find_same_old_diff_new(64, 256);
    assert_eq!(fxhash::hash(&a) as usize % 64, fxhash::hash(&b) as usize % 64);
    assert_ne!(fxhash::hash(&a) as usize % 256, fxhash::hash(&b) as usize % 256);

    let dir_up = tempfile::tempdir().unwrap();
    let tids_up = vec![a, b];
    let path = build_snapshot_with_old_bucket_count(dir_up.path(), 64, &tids_up);
    let metrics = SnapshotMetrics::noop();
    let (shards, _version, _wm) = deserialize_from_file(&path, 256, &metrics, None).unwrap();
    assert_rehash_correct(shards, 256, &tids_up);

    // 降档 256→64：旧桶异→新桶同。
    let (c, d) = find_diff_old_same_new(256, 64);
    assert_ne!(fxhash::hash(&c) as usize % 256, fxhash::hash(&d) as usize % 256);
    assert_eq!(fxhash::hash(&c) as usize % 64, fxhash::hash(&d) as usize % 64);

    let dir_down = tempfile::tempdir().unwrap();
    let tids_down = vec![c, d];
    let path2 = build_snapshot_with_old_bucket_count(dir_down.path(), 256, &tids_down);
    let (shards2, _v2, _w2) = deserialize_from_file(&path2, 64, &metrics, None).unwrap();
    assert_rehash_correct(shards2, 64, &tids_down);
}

#[test]
fn test_shard_count_1_rehash() {
    let dir = tempfile::tempdir().unwrap();
    let tids: Vec<u32> = (1u32..=500).collect();
    let path = build_snapshot_with_old_bucket_count(dir.path(), 64, &tids);

    let metrics = SnapshotMetrics::noop();
    let (shards, _version, _wm) = deserialize_from_file(&path, 1, &metrics, None).unwrap();
    assert_eq!(shards.len(), 1, "降到 1 桶");
    assert_rehash_correct(shards, 1, &tids);
}

#[test]
fn test_shard_count_huge_rehash() {
    let dir = tempfile::tempdir().unwrap();
    let tids: Vec<u32> = (1u32..=1000).collect();
    let path = build_snapshot_with_old_bucket_count(dir.path(), 64, &tids);

    let metrics = SnapshotMetrics::noop();
    let (shards, _version, _wm) = deserialize_from_file(&path, 4096, &metrics, None).unwrap();
    assert_eq!(shards.len(), 4096);
    assert_rehash_correct(shards, 4096, &tids);
}

#[test]
fn test_deserialize_truncated_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let tids: Vec<u32> = (1u32..=100).collect();
    let path = build_snapshot_with_old_bucket_count(dir.path(), 64, &tids);

    let raw = std::fs::read(&path).unwrap();
    // 截断到 header(40B) + 部分长度前缀(4B)，读长度前缀时 read_exact 必失败。
    let truncated = &raw[..44];
    let trunc_path = dir.path().join("snapshot_truncated.bin");
    std::fs::write(&trunc_path, truncated).unwrap();

    let metrics = SnapshotMetrics::noop();
    let result = deserialize_from_file(&trunc_path, 256, &metrics, None);
    assert!(result.is_err(), "截断文件必须返回 Err，不得 panic 或静默成功");
}

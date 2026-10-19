//! QA 独立验证（qa-dir1-memtable-batch）：方向1「memtable 按列批量写入」。
//!
//! fresh eyes，不照抄工程师测试。核心判定：`ColumnarChunk::append_batch` 与逐点
//! `append` 是否逐字节等价（timestamps / columns / presence / qualities / ts_index /
//! point_count / out_of_order / min-max_wal_seq + 全列全行 get_typed_value /
//! get_range / sorted_positions）。
//!
//! 覆盖边界（与 team-lead 指定的 7 类对齐）：
//!   ① 批内列集合突变（首点全列、后续缺列/多列/列序不同/重复列）
//!   ② 新鲜表首行批量（回退）
//!   ③ 批内乱序 + 同 ts LWW
//!   ④ 类型不匹配触发 presence 物化（首个/中间/已有 entry 三种）
//!   ⑤ 稀疏回退 + 密集混合
//!   ⑥ col_id 边界（127 快路径 / 128 回退）
//!   ⑦ 空批/单点
//!   外加：多类型列、insert_batch 统计与 seq 收口、table 分桶保序。

use sensearcdb_engine::engine::DataPoint;
use sensearcdb_engine::memtable::{ColumnarChunk, ColumnarMemTable, TypedValue};
use sensearcdb_engine::TenantId;

/// 构造 f64 列值列表。
fn f64s(v: &[(u16, f64)]) -> Vec<(u16, TypedValue)> {
    v.iter().map(|(id, x)| (*id, TypedValue::F64(*x))).collect()
}

/// 构造一个 DataPoint（默认租户、单质量流 0xC0）。
fn dp(table_id: u32, ts: i64, values: Vec<(u16, TypedValue)>) -> DataPoint {
    DataPoint {
        table_id,
        tenant_id: TenantId::DEFAULT,
        ts,
        server_ts: ts,
        values,
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    }
}

/// 逐字节断言两个 chunk 完全等价（结构体全字段 + 查询语义）。
fn assert_equiv(a: &ColumnarChunk, b: &ColumnarChunk, label: &str) {
    assert_eq!(a.table_id, b.table_id, "{label}: table_id");
    assert_eq!(a.tenant_id, b.tenant_id, "{label}: tenant_id");
    assert_eq!(a.timestamps, b.timestamps, "{label}: timestamps");
    assert_eq!(a.columns, b.columns, "{label}: columns");
    assert_eq!(a.presence, b.presence, "{label}: presence");
    assert_eq!(a.qualities, b.qualities, "{label}: qualities");
    assert_eq!(a.ts_index, b.ts_index, "{label}: ts_index");
    assert_eq!(a.point_count, b.point_count, "{label}: point_count");
    assert_eq!(a.out_of_order, b.out_of_order, "{label}: out_of_order");
    assert_eq!(a.max_wal_seq, b.max_wal_seq, "{label}: max_wal_seq");
    assert_eq!(a.min_wal_seq, b.min_wal_seq, "{label}: min_wal_seq");

    // 排序语义等价。
    assert_eq!(a.sorted_positions(), b.sorted_positions(), "{label}: sorted_positions");
    // 全范围 get_range 等价。
    if let (Some(&amin), Some(&amax)) = (a.timestamps.iter().min(), a.timestamps.iter().max()) {
        assert_eq!(a.get_range(amin, amax), b.get_range(amin, amax), "{label}: get_range");
    }
    // 全列全行 typed 读取逐字节一致。
    let mut col_ids: Vec<u16> = a.columns.keys().copied().collect();
    col_ids.sort_unstable();
    for col in &col_ids {
        for r in 0..a.point_count {
            assert_eq!(
                a.get_typed_value(*col, r),
                b.get_typed_value(*col, r),
                "{label}: col {col} row {r}"
            );
        }
    }
}

/// 统一等价性检查：`points[..seed_len]` 逐点 append 建种子，其余走 append_batch；
/// 与「全量逐点 append」比对。seed_len=0 即全量走 append_batch（新鲜表/回退路径）。
fn check_equiv(points: &[DataPoint], seed_len: usize, label: &str) {
    let table_id = points.first().map(|p| p.table_id).unwrap_or(1);

    let mut r = ColumnarChunk::new(TenantId::DEFAULT, table_id);
    for p in points {
        r.append(p.ts, &p.values, &p.qualities, p.flags);
    }

    let mut b = ColumnarChunk::new(TenantId::DEFAULT, table_id);
    for p in &points[..seed_len] {
        b.append(p.ts, &p.values, &p.qualities, p.flags);
    }
    let refs: Vec<&DataPoint> = points[seed_len..].iter().collect();
    b.append_batch(&refs);

    assert_equiv(&r, &b, label);
}

// ---------------------------------------------------------------------------
// ① 批内列集合突变（首点全列、后续缺列 / 多列 / 列序不同 / 重复列）
// ---------------------------------------------------------------------------

#[test]
fn qa_batch_col_set_mutation_missing_col() {
    // 首点覆盖 col0/col1，后续缺 col1 → 应回退逐点。
    let pts = vec![
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)])),
        dp(1, 200, f64s(&[(0, 2.0)])),
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0)])),
    ];
    check_equiv(&pts, 1, "col_set_mutation_missing_col");
}

#[test]
fn qa_batch_col_set_mutation_extra_col() {
    // 后续多出 col2（新列）→ 回退。
    let pts = vec![
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)])),
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0), (2, 200.0)])),
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0)])),
    ];
    check_equiv(&pts, 1, "col_set_mutation_extra_col");
}

#[test]
fn qa_batch_col_order_change() {
    // 后续列序与首点不同 → 回退（避免列错位）。
    let pts = vec![
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0), (2, 100.0)])),
        dp(1, 200, f64s(&[(0, 2.0), (2, 200.0), (1, 20.0)])),
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0), (2, 300.0)])),
    ];
    check_equiv(&pts, 1, "col_order_change");
}

#[test]
fn qa_batch_duplicate_col_first_point() {
    // 首点含重复 col → is_fully_dense false → 回退（去重保留最后值）。
    let pts = vec![
        dp(
            1,
            100,
            vec![(0, TypedValue::F64(1.0)), (0, TypedValue::F64(9.0)), (1, TypedValue::F64(10.0))],
        ),
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0)])),
    ];
    check_equiv(&pts, 0, "duplicate_col_first_point");
}

// ---------------------------------------------------------------------------
// ② 新鲜表首行批量（columns 空 → is_fully_dense 恒 false → 回退）
// ---------------------------------------------------------------------------

#[test]
fn qa_batch_fresh_table_first_batch() {
    let pts = vec![
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)])),
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0)])),
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0)])),
    ];
    check_equiv(&pts, 0, "fresh_table_first_batch");
}

// ---------------------------------------------------------------------------
// ③ 批内乱序 + 同 ts LWW（密集快路径）
// ---------------------------------------------------------------------------

#[test]
fn qa_batch_dense_out_of_order_lww() {
    let seed = vec![dp(1, 500, f64s(&[(0, 5.0), (1, 50.0)]))];
    let batch = vec![
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0)])),
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)])), // 乱序
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0)])),
        dp(1, 100, f64s(&[(0, 9.9), (1, 99.0)])), // 同 ts LWW 覆盖
    ];
    let mut pts = seed.clone();
    pts.extend(batch);
    check_equiv(&pts, 1, "dense_out_of_order_lww");
}

#[test]
fn qa_batch_dense_ordered() {
    let pts = vec![
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0), (2, 100.0)])),
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0), (2, 200.0)])),
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0), (2, 300.0)])),
        dp(1, 400, f64s(&[(0, 4.0), (1, 40.0), (2, 400.0)])),
    ];
    check_equiv(&pts, 1, "dense_ordered");
}

// ---------------------------------------------------------------------------
// ④ 类型不匹配触发 presence 物化
// ---------------------------------------------------------------------------

#[test]
fn qa_batch_dense_type_mismatch_first_point() {
    // 批内首个点即类型不匹配（col0 F64 列收到 I64）。
    let seed = vec![dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)]))];
    let batch = vec![
        dp(1, 200, vec![(0, TypedValue::I64(7)), (1, TypedValue::F64(20.0))]),
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0)])),
    ];
    let mut pts = seed.clone();
    pts.extend(batch);
    check_equiv(&pts, 1, "dense_type_mismatch_first_point");
}

#[test]
fn qa_batch_dense_type_mismatch_middle_point() {
    // 批内第 2 点类型不匹配（物化 base 应是正确 row_idx）。
    let seed = vec![dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)]))];
    let batch = vec![
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0)])),
        dp(1, 300, vec![(0, TypedValue::String("x".into())), (1, TypedValue::F64(30.0))]),
        dp(1, 400, f64s(&[(0, 4.0), (1, 40.0)])),
    ];
    let mut pts = seed.clone();
    pts.extend(batch);
    check_equiv(&pts, 1, "dense_type_mismatch_middle_point");
}

#[test]
fn qa_batch_dense_type_mismatch_with_existing_presence() {
    // 列已有 presence entry（历史稀疏），再走密集批量 + 类型不匹配（true, Some 分支）。
    let seed = vec![
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)])),
        dp(1, 150, f64s(&[(0, 1.5)])), // 稀疏写 → col1 物化 presence
    ];
    let batch = vec![
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0)])),
        dp(1, 300, vec![(0, TypedValue::I64(3)), (1, TypedValue::F64(30.0))]),
        dp(1, 400, f64s(&[(0, 4.0), (1, 40.0)])),
    ];
    let mut pts = seed.clone();
    pts.extend(batch);
    check_equiv(&pts, 2, "dense_type_mismatch_with_existing_presence");
}

// ---------------------------------------------------------------------------
// ⑤ 稀疏回退 + 密集混合
// ---------------------------------------------------------------------------

#[test]
fn qa_batch_sparse_fallback_mixed() {
    // 首点稀疏（缺列）→ 回退；后续密集。整批 vs 逐点等价。
    let seed = vec![dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)]))];
    let batch = vec![
        dp(1, 200, f64s(&[(0, 2.0)])),
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0)])),
    ];
    let mut pts = seed.clone();
    pts.extend(batch);
    check_equiv(&pts, 1, "sparse_fallback_mixed");
}

#[test]
fn qa_batch_dense_then_sparse_then_dense() {
    // 密集批量 → 稀疏批量（回退）→ 再密集批量，全程等价。
    let pts = vec![
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)])),
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0)])),
        dp(1, 300, f64s(&[(0, 3.0), (1, 30.0)])),
        dp(1, 400, f64s(&[(0, 4.0)])), // 稀疏
        dp(1, 500, f64s(&[(0, 5.0), (1, 50.0)])),
        dp(1, 600, f64s(&[(0, 6.0), (1, 60.0)])),
    ];
    check_equiv(&pts, 1, "dense_then_sparse_then_dense");
}

// ---------------------------------------------------------------------------
// ⑥ col_id 边界（127 快路径 / 128 回退）
// ---------------------------------------------------------------------------

#[test]
fn qa_batch_col_id_127_fast_path() {
    // col 127 < 128，is_fully_dense 接受 → 快路径，应逐字节等价。
    let pts = vec![
        dp(1, 100, vec![(127, TypedValue::F64(1.0))]),
        dp(1, 200, vec![(127, TypedValue::F64(2.0))]),
        dp(1, 300, vec![(127, TypedValue::F64(3.0))]),
    ];
    check_equiv(&pts, 1, "col_id_127_fast_path");
}

#[test]
fn qa_batch_col_id_128_fallback() {
    // col 128 >= 128，is_fully_dense 回退稀疏 → 应逐字节等价（语义不变仅失快路径）。
    let pts = vec![
        dp(1, 100, vec![(128, TypedValue::F64(1.0))]),
        dp(1, 200, vec![(128, TypedValue::F64(2.0))]),
        dp(1, 300, vec![(128, TypedValue::F64(3.0))]),
    ];
    check_equiv(&pts, 1, "col_id_128_fallback");
}

#[test]
fn qa_batch_col_id_mixed_high_low() {
    // 混合高低 col_id，低 id 覆盖全部既有列时高 id 是新列 → 回退。
    let pts = vec![
        dp(1, 100, vec![(0, TypedValue::F64(1.0)), (200, TypedValue::F64(10.0))]),
        dp(1, 200, vec![(0, TypedValue::F64(2.0)), (200, TypedValue::F64(20.0))]),
    ];
    check_equiv(&pts, 1, "col_id_mixed_high_low");
}

// ---------------------------------------------------------------------------
// ⑦ 空批 / 单点
// ---------------------------------------------------------------------------

#[test]
fn qa_batch_empty_noop() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    let empty: Vec<&DataPoint> = vec![];
    c.append_batch(&empty);
    assert_eq!(c.point_count, 0);
    assert_eq!(c.timestamps.len(), 0);
    assert!(c.columns.is_empty());
    assert!(c.presence.is_empty());
    assert!(c.ts_index.is_empty());
    assert!(!c.out_of_order);
}

#[test]
fn qa_batch_single_point_dense_fast_path() {
    // 种子建列后，单点密集批走快路径（n=1），与逐点等价。
    let pts = vec![
        dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)])),
        dp(1, 200, f64s(&[(0, 2.0), (1, 20.0)])),
    ];
    check_equiv(&pts, 1, "single_point_dense_fast_path");
}

#[test]
fn qa_batch_single_point_fresh_fallback() {
    // 新鲜表单点批：columns 空 → 回退（n=1）。
    let pts = vec![dp(1, 100, f64s(&[(0, 1.0), (1, 10.0)]))];
    check_equiv(&pts, 0, "single_point_fresh_fallback");
}

// ---------------------------------------------------------------------------
// 多类型列
// ---------------------------------------------------------------------------

#[test]
fn qa_batch_multitype_columns() {
    let v = |ts: i64, i: i64| {
        dp(
            1,
            ts,
            vec![
                (0, TypedValue::F64(i as f64)),
                (1, TypedValue::I64(i)),
                (2, TypedValue::Bool(i % 2 == 0)),
                (3, TypedValue::String(format!("s{i}"))),
                (4, TypedValue::Enum16((i % 100) as u16)),
            ],
        )
    };
    let pts = vec![v(100, 1), v(200, 2), v(300, 3), v(400, 4)];
    check_equiv(&pts, 1, "multitype_columns");
}

// ---------------------------------------------------------------------------
// 表级 insert_batch：统计收口 / seq 对齐 / 分桶保序（C7）
// ---------------------------------------------------------------------------

/// 计算与 point_size 相同的字节口径（8 + values.len()*10 + qualities.len()*3）。
fn point_size(p: &DataPoint) -> usize {
    8 + p.values.len() * 10 + p.qualities.len() * 3
}

#[test]
fn qa_insert_batch_stats_and_seq() {
    let mt = ColumnarMemTable::new(460_000);

    // 3 表、跨分片、带 seq，且同一表出现两次（保序 + min/max）。
    let points = vec![
        dp(1, 100, f64s(&[(0, 1.0)])),
        dp(2, 200, f64s(&[(0, 2.0)])),
        dp(1, 300, f64s(&[(0, 3.0)])),
        dp(17, 400, f64s(&[(0, 4.0)])),
        dp(1, 500, f64s(&[(0, 5.0)])),
    ];
    let seqs = vec![10u64, 20, 30, 40, 50];

    mt.insert_batch(&points, &seqs);

    // 统计收口：不漏点、不重复计。
    assert_eq!(mt.total_points(), 5);
    let expected_bytes: usize = points.iter().map(point_size).sum();
    assert_eq!(mt.size_bytes(), expected_bytes, "size_bytes 批量收口不一致");
    assert_eq!(mt.table_count(), 3);

    // seq 对齐：每表 min/max。
    let c1 = mt.get_chunk(1).unwrap();
    assert_eq!(c1.point_count, 3);
    assert_eq!(c1.max_wal_seq, Some(50));
    assert_eq!(c1.min_wal_seq, Some(10));
    // 保序：timestamps 按输入顺序 100,300,500。
    assert_eq!(c1.timestamps, vec![100, 300, 500]);

    let c2 = mt.get_chunk(2).unwrap();
    assert_eq!(c2.point_count, 1);
    assert_eq!(c2.max_wal_seq, Some(20));
    assert_eq!(c2.min_wal_seq, Some(20));

    let c17 = mt.get_chunk(17).unwrap();
    assert_eq!(c17.point_count, 1);
    assert_eq!(c17.max_wal_seq, Some(40));
}

#[test]
fn qa_insert_batch_empty_seqs() {
    let mt = ColumnarMemTable::new(460_000);
    let points = vec![
        dp(1, 100, f64s(&[(0, 1.0)])),
        dp(1, 200, f64s(&[(0, 2.0)])),
    ];
    mt.insert_batch(&points, &[]); // 空 seqs
    assert_eq!(mt.total_points(), 2);
    let c1 = mt.get_chunk(1).unwrap();
    assert_eq!(c1.point_count, 2);
    assert_eq!(c1.max_wal_seq, None);
    assert_eq!(c1.min_wal_seq, None);
}

#[test]
fn qa_insert_batch_lww_same_ts_preserved_order() {
    let mt = ColumnarMemTable::new(460_000);
    // 同表同 ts 后写覆盖（LWW）+ 跨表交错，验证分桶保序影响 sorted_positions。
    let points = vec![
        dp(1, 100, f64s(&[(0, 1.0)])),
        dp(2, 999, f64s(&[(0, 2.0)])),
        dp(1, 200, f64s(&[(0, 3.0)])),
        dp(1, 200, f64s(&[(0, 9.0)])), // 同 ts LWW 覆盖
        dp(2, 888, f64s(&[(0, 8.0)])),
    ];
    mt.insert_batch(&points, &[]);

    let c1 = mt.get_chunk(1).unwrap();
    assert_eq!(c1.timestamps, vec![100, 200, 200]);
    // 读最新值（LWW）：row2 = 9.0。
    assert_eq!(c1.sorted_positions(), vec![0, 2]);
    assert_eq!(c1.get_typed_value(0, 2), Some(TypedValue::F64(9.0)));
    assert_eq!(c1.get_range(200, 200), vec![(200, 2)]);
}

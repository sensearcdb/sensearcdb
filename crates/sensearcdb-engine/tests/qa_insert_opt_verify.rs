//! QA 独立验证（qa-engine-insert）：引擎 insert 路径两项优化的正确性。
//!
//! 验证对象：
//! 1. `ColumnarChunk::append` 的 ts_index 惰性化（有序路径不维护 BTreeMap，
//!    首次乱序 `rebuild_ts_index` 物化历史）。
//! 2. `append` 的全列 present 快路径（`is_fully_dense` + `write_existing_column`）。
//!
//! 核心方法：用「旧 eager 实现」的语义构造独立参考实现（BTreeMap 逐点 insert），
//! 对任意时间戳序列（有序 / 乱序 / 连续同 ts / 交错同 ts）逐字节比对
//! `sorted_positions` / `get_range` / `ts_index`，不信任工程师自测。

use std::collections::BTreeMap;

use sensearcdb_engine::memtable::{ColumnarChunk, TypedValue};
use sensearcdb_engine::TenantId;

/// 便捷构造 f64 单列值列表。
fn f64s(v: &[(u16, f64)]) -> Vec<(u16, TypedValue)> {
    v.iter().map(|(id, x)| (*id, TypedValue::F64(*x))).collect()
}

/// 参考实现：模拟「旧每点 eager insert BTreeMap」的 LWW 语义。
/// 键为 ts，值为「最后一次出现该 ts 的 row_idx」。
fn reference_ts_index(timestamps: &[i64]) -> BTreeMap<i64, usize> {
    let mut m = BTreeMap::new();
    for (i, &ts) in timestamps.iter().enumerate() {
        m.insert(ts, i);
    }
    m
}

/// 参考 `sorted_positions`：按 ts 升序返回 LWW 去重后的 row_idx。
fn reference_sorted_positions(timestamps: &[i64]) -> Vec<usize> {
    reference_ts_index(timestamps).values().copied().collect()
}

/// 参考 `get_range`：范围 [start, end] 内按 ts 升序的 (ts, row_idx)。
/// 反向范围（start > end）语义为空集（不 panic）——这是有序分支已实现的正确语义。
fn reference_get_range(timestamps: &[i64], start: i64, end: i64) -> Vec<(i64, usize)> {
    if start > end {
        return Vec::new();
    }
    reference_ts_index(timestamps)
        .range(start..=end)
        .map(|(ts, idx)| (*ts, *idx))
        .collect()
}

/// 用「单列 + 值 = 行号」构造 chunk，使每个 row_idx 唯一可辨识。
fn build_chunk(ts_seq: &[i64]) -> ColumnarChunk {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
    for (i, &ts) in ts_seq.iter().enumerate() {
        chunk.append(
            ts,
            &[(0, TypedValue::F64(i as f64))],
            &[(0, 0xC0)],
            0,
        );
    }
    chunk
}

/// 全量断言：对同一 chunk，`sorted_positions` 与 `get_range`（多窗口）必须与
/// 「旧 eager BTreeMap」参考实现逐字节一致。
fn assert_equivalent_to_eager_reference(chunk: &ColumnarChunk, ts_seq: &[i64]) {
    assert_eq!(
        chunk.sorted_positions(),
        reference_sorted_positions(ts_seq),
        "sorted_positions 与 eager 参考实现不一致，ts_seq={ts_seq:?}"
    );

    // 覆盖多个范围窗口（含空窗口、越界、单点、跨段、含重复 ts 的窗口）。
    // 反向范围（start > end）单独在专门用例中验证，避免 BTreeMap 反向 range 的
    // panic 掩盖正常等价性结论。
    let probes: [(i64, i64); 9] = [
        (i64::MIN, i64::MAX),
        (0, 0),
        (100, 100),
        (100, 200),
        (150, 300),
        (200, 400),
        (250, 250),
        (99, 101),
        (i64::MIN, 150),
    ];
    for (s, e) in probes {
        assert_eq!(
            chunk.get_range(s, e),
            reference_get_range(ts_seq, s, e),
            "get_range({s},{e}) 与 eager 参考实现不一致，ts_seq={ts_seq:?}"
        );
    }
}

#[test]
fn qa_insert_sorted_positions_equivalent_to_eager_reference() {
    let sequences: Vec<Vec<i64>> = vec![
        vec![],
        vec![100],
        vec![100, 200, 300],
        // 连续同 ts（有序路径 LWW）
        vec![100, 100, 200, 200, 300],
        vec![100, 100, 100, 200],
        // 乱序发生在末尾
        vec![100, 200, 300, 250],
        // 乱序 + 同 ts LWW（真实 flush 场景）
        vec![300, 100, 200, 100],
        // 乱序且与历史同 ts（覆盖既有 ts）
        vec![100, 200, 100],
        // 乱序 + 连续同 ts 交错
        vec![100, 200, 200, 150, 150, 250],
        // 严格递减
        vec![5, 4, 3, 2, 1],
        // 乱序点与有序前缀同 ts
        vec![100, 100, 300, 200, 100],
    ];

    for ts_seq in &sequences {
        let chunk = build_chunk(ts_seq);
        assert_eq!(chunk.timestamps, *ts_seq, "timestamps 必须为原始物理顺序");
        assert_equivalent_to_eager_reference(&chunk, ts_seq);
    }
}

/// 红线 1：乱序发生在第 N 个点（前 N-1 有序）时，rebuild 必须物化前 N-1 个点。
#[test]
fn qa_insert_first_out_of_order_rebuild_captures_all_prior_points() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);

    // 前 4 个点有序。
    chunk.append(100, &f64s(&[(0, 0.0)]), &[(0, 0xC0)], 0);
    chunk.append(200, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    chunk.append(300, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    chunk.append(400, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);

    // 乱序前必须保持惰性：无 ts_index、无 out_of_order。
    assert!(chunk.ts_index.is_empty(), "乱序前 ts_index 应惰性为空");
    assert!(!chunk.out_of_order);

    // 第 5 个点乱序。
    chunk.append(250, &f64s(&[(0, 4.0)]), &[(0, 0xC0)], 0);

    assert!(chunk.out_of_order);
    let expected = reference_ts_index(&[100, 200, 300, 400, 250]);
    assert_eq!(
        chunk.ts_index, expected,
        "首次乱序 rebuild 后 ts_index 必须包含全部 N 个点（含前 N-1 有序点）"
    );
    assert_eq!(chunk.sorted_positions(), vec![0, 1, 4, 2, 3]);
    assert_eq!(
        chunk.get_range(100, 400),
        vec![(100, 0), (200, 1), (250, 4), (300, 2), (400, 3)]
    );
}

/// 红线 1 强化：有序前缀内存在连续同 ts LWW 时，rebuild 必须保留最新 row_idx，
/// 而不是把同 ts 的两个点都塞进 ts_index。
#[test]
fn qa_insert_rebuild_preserves_lww_in_ordered_prefix() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 有序前缀含同 ts 重写：ts=100 出现两次（LWW 保留 idx=1）。
    chunk.append(100, &f64s(&[(0, 0.0)]), &[(0, 0xC0)], 0);
    chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    chunk.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
    assert!(chunk.ts_index.is_empty());

    // 首次乱序（ts=250）。
    chunk.append(250, &f64s(&[(0, 4.0)]), &[(0, 0xC0)], 0);

    let expected = reference_ts_index(&[100, 100, 200, 300, 250]);
    assert_eq!(chunk.ts_index, expected);
    assert_eq!(chunk.sorted_positions(), vec![1, 2, 4, 3]);
    // ts=100 读到的必须是最新值（row_idx=1 → 1.0），不是 idx=0 → 0.0。
    assert_eq!(chunk.get_range(100, 100), vec![(100, 1)]);
    assert_eq!(chunk.get_value(0, 1), Some(1.0));
}

/// 红线 2：连续同 ts（有序模式，惰性 ts_index）的 LWW 去重，只保留最后一个 row_idx。
#[test]
fn qa_insert_consecutive_same_ts_ordered_lww() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
    chunk.append(100, &f64s(&[(0, 0.0)]), &[(0, 0xC0)], 0);
    chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    chunk.append(100, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    chunk.append(200, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
    chunk.append(200, &f64s(&[(0, 4.0)]), &[(0, 0xC0)], 0);

    assert!(chunk.ts_index.is_empty(), "有序路径必须保持 ts_index 惰性");
    assert!(!chunk.out_of_order);
    assert_eq!(chunk.sorted_positions(), vec![2, 4]);
    assert_eq!(chunk.get_range(100, 100), vec![(100, 2)]);
    assert_eq!(chunk.get_range(200, 200), vec![(200, 4)]);
    assert_eq!(chunk.get_range(100, 200), vec![(100, 2), (200, 4)]);
    assert_eq!(chunk.get_range(150, 200), vec![(200, 4)]);
    // LWW 最新值可读。
    assert_eq!(chunk.get_value(0, 2), Some(2.0));
    assert_eq!(chunk.get_value(0, 4), Some(4.0));
}

/// 红线 3：is_fully_dense 边界——「值数量 == 列数量」但含新列时，必须回退稀疏路径
/// （新列回填占位），列向量长度/位图/读取与期望一致。
#[test]
fn qa_insert_new_column_in_dense_shaped_row_falls_back_to_sparse() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
    chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);

    // 第二个点：col 0（已存在）+ col 1（新列）。此时 values.len()==2 != columns.len()==1，
    // is_fully_dense 返回 false → 走稀疏路径，col 1 回填 row0 占位。
    chunk.append(
        200,
        &[(0, TypedValue::F64(2.0)), (1, TypedValue::F64(9.0))],
        &[(0, 0xC0)],
        0,
    );

    assert_eq!(chunk.point_count, 2);
    assert_eq!(chunk.columns.get(&0).map(|c| c.len()), Some(2));
    assert_eq!(chunk.columns.get(&1).map(|c| c.len()), Some(2));
    assert_eq!(chunk.get_typed_value(0, 1), Some(TypedValue::F64(2.0)));
    assert_eq!(chunk.get_typed_value(1, 1), Some(TypedValue::F64(9.0)));
    // 新列 row0 必须为稀疏缺失，不能错位。
    assert_eq!(chunk.get_typed_value(1, 0), None);
}

/// 重复 col_id 防御性去重（修复前：稀疏路径对同一列连续 push 两次 → 列向量长度 > 行数）。
///
/// 修复后：单点内重复 col_id 按「保留最后出现值」去重，列向量长度恒 == 行数，
/// 不产生 double push（数据不错位）。`is_fully_dense` 仍正确拒绝重复 col_id（返回
/// false 走稀疏路径），稀疏路径内完成去重。
#[test]
fn qa_insert_duplicate_col_id_deduped_keep_last() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 单点内重复 col 0：最后出现值 2.0 应胜出。
    chunk.append(
        100,
        &[(0, TypedValue::F64(1.0)), (0, TypedValue::F64(2.0))],
        &[(0, 0xC0)],
        0,
    );

    // 去重后：1 个点，col0 向量长度 == 行数 == 1（无 double push）。
    // 2a 优化：首行密集写入不创建 presence entry（无 entry 即纯密集态，is_present 走密集回退）。
    assert_eq!(chunk.point_count, 1);
    assert_eq!(chunk.timestamps.len(), 1);
    assert_eq!(chunk.columns.get(&0).map(|c| c.len()), Some(1));
    assert!(
        chunk.presence.get(&0).is_none(),
        "首行密集写入不应创建 presence entry"
    );
    // 保留最后出现值。
    assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(2.0)));
}

/// 红线 3：is_fully_dense 快路径与稀疏路径对 presence 位图的最终状态一致。
///
/// 通过「密集 ↔ 稀疏交替」的写入序列，验证最终 presence 位图、列长度、逐列取值
/// 与独立计算的期望一致（覆盖：新列出现、部分列缺失、密集行）。
#[test]
fn qa_insert_dense_sparse_presence_bitmap_consistency() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);

    // row0：只有 col0（稀疏，col1 尚未出现）。
    chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    // row1：col0 + col1（col1 新列 → 稀疏路径回填）。
    chunk.append(
        200,
        &[(0, TypedValue::F64(2.0)), (1, TypedValue::F64(20.0))],
        &[(0, 0xC0)],
        0,
    );
    // row2：只有 col0（col1 稀疏缺失 → 稀疏路径）。
    chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
    // row3：col0 + col1 全 present（密集快路径）。
    chunk.append(
        400,
        &[(0, TypedValue::F64(4.0)), (1, TypedValue::F64(40.0))],
        &[(0, 0xC0)],
        0,
    );

    assert_eq!(chunk.point_count, 4);
    // 两列长度都必须 == 行数 4。
    assert_eq!(chunk.columns.get(&0).map(|c| c.len()), Some(4));
    assert_eq!(chunk.columns.get(&1).map(|c| c.len()), Some(4));

    // col0 全 present。
    assert_eq!(chunk.presence.get(&0), Some(&vec![true, true, true, true]));
    // col1 只有 row1/row3 present。
    assert_eq!(chunk.presence.get(&1), Some(&vec![false, true, false, true]));

    // 逐列取值：稀疏行必须返回 None（不能错位读到相邻行）。
    assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
    assert_eq!(chunk.get_typed_value(0, 1), Some(TypedValue::F64(2.0)));
    assert_eq!(chunk.get_typed_value(0, 2), Some(TypedValue::F64(3.0)));
    assert_eq!(chunk.get_typed_value(0, 3), Some(TypedValue::F64(4.0)));
    assert_eq!(chunk.get_typed_value(1, 0), None);
    assert_eq!(chunk.get_typed_value(1, 1), Some(TypedValue::F64(20.0)));
    assert_eq!(chunk.get_typed_value(1, 2), None);
    assert_eq!(chunk.get_typed_value(1, 3), Some(TypedValue::F64(40.0)));
}

/// 红线 3：col_id >= 128 时 is_fully_dense 保守回退稀疏路径，语义不变。
#[test]
fn qa_insert_col_id_ge_128_falls_back_semantically_identical() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);

    // col 200（>= 128）与 col 0 都 present，但 is_fully_dense 对 200 保守回退。
    chunk.append(
        100,
        &[(0, TypedValue::F64(1.0)), (200, TypedValue::F64(2.0))],
        &[(0, 0xC0)],
        0,
    );
    chunk.append(
        200,
        &[(0, TypedValue::F64(3.0)), (200, TypedValue::F64(4.0))],
        &[(0, 0xC0)],
        0,
    );

    assert_eq!(chunk.point_count, 2);
    assert_eq!(chunk.columns.get(&200).map(|c| c.len()), Some(2));
    assert_eq!(chunk.get_typed_value(200, 0), Some(TypedValue::F64(2.0)));
    assert_eq!(chunk.get_typed_value(200, 1), Some(TypedValue::F64(4.0)));
    assert_eq!(chunk.sorted_positions(), vec![0, 1]);
}

/// 红线 4：flush/SAC 排序顺序不变式——`sorted_positions()` 必须是「ts 升序 + LWW」，
/// 与旧 `ts_index.values()` 逐字节一致（含乱序 chunk 与已物化 chunk）。
#[test]
fn qa_insert_sorted_positions_is_ts_ascending_lww() {
    // 乱序 chunk：物理 append 顺序与 ts 顺序不同，但 sorted_positions 必须 ts 升序。
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
    chunk.append(300, &f64s(&[(0, 30.0)]), &[(0, 0xC0)], 0);
    chunk.append(100, &f64s(&[(0, 10.0)]), &[(0, 0xC0)], 0);
    chunk.append(200, &f64s(&[(0, 20.0)]), &[(0, 0xC0)], 0);
    chunk.append(100, &f64s(&[(0, 99.0)]), &[(0, 0xC0)], 0); // 同 ts LWW

    assert_eq!(chunk.sorted_positions(), vec![3, 2, 0]);
    // 校验 sorted_positions 返回的 row_idx 对应的 ts 单调不减。
    let ts_order: Vec<i64> = chunk
        .sorted_positions()
        .iter()
        .map(|&p| chunk.timestamps[p])
        .collect();
    assert_eq!(ts_order, vec![100, 200, 300]);

    // 直接构造「已物化 ts_index」的 chunk（模拟 build_sub_chunk / SAC 读回），
    // 必须恒走 ts_index 路径（逐字节保持旧行为）。
    let mut materialized = ColumnarChunk::new(TenantId::DEFAULT, 7);
    materialized.timestamps = vec![300, 100, 200];
    materialized.ts_index.insert(100, 1);
    materialized.ts_index.insert(200, 2);
    materialized.ts_index.insert(300, 0);
    assert_eq!(materialized.sorted_positions(), vec![1, 2, 0]);
    assert_eq!(
        materialized.get_range(100, 300),
        vec![(100, 1), (200, 2), (300, 0)]
    );
}

/// 红线 5：get_range 有序 / 乱序分派结果一致——对同一逻辑数据（LWW 结果集相同），
/// 两个分支必须返回相同的 ts 升序序列。
#[test]
fn qa_insert_get_range_ordered_and_out_of_order_consistent() {
    // 有序版本：ts 直接按升序写入。
    let mut ordered = ColumnarChunk::new(TenantId::DEFAULT, 1);
    ordered.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    ordered.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    ordered.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);

    // 乱序版本：同样的三个 ts，物理顺序打乱。
    let mut ooo = ColumnarChunk::new(TenantId::DEFAULT, 1);
    ooo.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    ooo.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
    ooo.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);

    // 两者 get_range 的 (ts) 序列必须一致；row_idx 各自映射到正确的值。
    assert_eq!(
        ordered.get_range(100, 300).iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
        ooo.get_range(100, 300).iter().map(|(ts, _)| *ts).collect::<Vec<_>>()
    );
    assert_eq!(ordered.get_range(100, 300), vec![(100, 0), (200, 1), (300, 2)]);
    assert_eq!(ooo.get_range(100, 300), vec![(100, 2), (200, 0), (300, 1)]);
}

/// 红线 6（回归）：空 chunk 与越界/反向范围查询不 panic 且返回空。
#[test]
fn qa_insert_empty_and_out_of_bounds_range() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
    assert_eq!(chunk.sorted_positions(), Vec::<usize>::new());
    assert_eq!(chunk.get_range(0, 100), Vec::<(i64, usize)>::new());

    chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    chunk.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    // 反向范围（start > end）与完全越界。
    assert_eq!(chunk.get_range(500, 100), Vec::<(i64, usize)>::new());
    assert_eq!(chunk.get_range(300, 400), Vec::<(i64, usize)>::new());
    assert_eq!(chunk.get_range(0, 50), Vec::<(i64, usize)>::new());
    assert_eq!(chunk.get_range(150, 150), Vec::<(i64, usize)>::new());
}

/// 反向范围（start > end）防御守卫验证：有序 / 乱序两个分支必须一致返回空集。
///
/// - 有序分支 `ordered_range` 对反向范围返回空集（不 panic）。
/// - 乱序分支走 `ts_index.range(start..=end)`，`BTreeMap` 反向 range 会在迭代时 panic。
///
/// 修复：`get_range` 入口已加 `if ts_start > ts_end { return vec![] }` 防御守卫，
/// 两个分支统一返回空。本测试断言两分支均不 panic 且返回空集。
#[test]
fn qa_insert_get_range_inverted_range_branch_inconsistency() {
    let mut ordered = ColumnarChunk::new(TenantId::DEFAULT, 1);
    ordered.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    ordered.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);

    let mut ooo = ColumnarChunk::new(TenantId::DEFAULT, 1);
    ooo.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    ooo.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0); // 乱序

    // 反向范围：两个分支统一返回空集（不 panic）。
    assert_eq!(ordered.get_range(500, 100), Vec::<(i64, usize)>::new());
    assert_eq!(ooo.get_range(500, 100), Vec::<(i64, usize)>::new());
}

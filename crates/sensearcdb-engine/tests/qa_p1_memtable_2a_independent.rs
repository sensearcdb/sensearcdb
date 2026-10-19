//! QA 独立验证（qa-p1-memtable-2a）：P1 2a「密集宽表跳过 presence 位图维护」。
//!
//! fresh eyes，不照抄工程师测试。逐条验证核心不变式：
//!   presence 无 entry ⟺ 该列至今所有行都有值（纯密集）。
//!   presence 有 entry ⟺ 该列至少出现过一次稀疏缺失或类型不匹配，此后每行位图对齐。
//!
//! 覆盖边界：纯密集空 presence、密集→稀疏物化 [true×N,false]、稀疏→密集对齐、
//! 类型不匹配物化、首行抑制后连续密集、非首行新增列补 false、重复 col_id 去重
//! （列长度/值/presence 三方面）、密集期后新增列、密集列+稀疏列混合再密集。

use sensearcdb_engine::memtable::{ColumnarChunk, TypedValue};
use sensearcdb_engine::TenantId;

fn f64s(v: &[(u16, f64)]) -> Vec<(u16, TypedValue)> {
    v.iter().map(|(id, x)| (*id, TypedValue::F64(*x))).collect()
}

/// 断言某列长度 == 行数（核心不变式：列向量长度恒 == 行数）。
fn assert_col_len_eq_rows(chunk: &ColumnarChunk, col: u16) {
    let rows = chunk.timestamps.len();
    let len = chunk.columns.get(&col).map(|c| c.len());
    assert_eq!(len, Some(rows), "col {col} 长度应 == 行数 {rows}");
}

#[test]
fn ind_pure_dense_presence_stays_empty() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 首行建 3 列。
    c.append(100, &f64s(&[(0, 1.0), (1, 10.0), (2, 100.0)]), &[(0, 0xC0)], 0);
    assert!(c.presence.is_empty(), "首行全列密集应空 presence");
    // 连续密集 append。
    for i in 1..5 {
        c.append(
            100 * (i + 1),
            &f64s(&[(0, 1.0 + i as f64), (1, 10.0 + i as f64), (2, 100.0 + i as f64)]),
            &[(0, 0xC0)],
            0,
        );
    }
    assert!(c.presence.is_empty(), "连续密集 append 后 presence 应仍空");
    assert_eq!(c.point_count, 5);
    // 读值正确（靠 is_present 密集回退）。
    assert_eq!(c.get_typed_value(0, 4), Some(TypedValue::F64(5.0)));
    assert_eq!(c.get_typed_value(1, 4), Some(TypedValue::F64(14.0)));
    assert_eq!(c.get_typed_value(2, 4), Some(TypedValue::F64(104.0)));
    // 越界行返回 None。
    assert_eq!(c.get_typed_value(0, 5), None);
    assert_col_len_eq_rows(&c, 0);
    assert_col_len_eq_rows(&c, 1);
    assert_col_len_eq_rows(&c, 2);
}

#[test]
fn ind_dense_then_sparse_materializes_true_then_false() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 密集 3 行（col0/col1 全覆盖）。
    for i in 0..3 {
        c.append(100 * (i + 1), &f64s(&[(0, i as f64), (1, (i * 10) as f64)]), &[(0, 0xC0)], 0);
    }
    assert!(c.presence.is_empty());
    // 稀疏写只写 col0，col1 缺失。
    c.append(400, &f64s(&[(0, 99.0)]), &[(0, 0xC0)], 0);

    assert_eq!(c.presence.get(&0), Some(&vec![true, true, true, true]));
    assert_eq!(c.presence.get(&1), Some(&vec![true, true, true, false]));

    // 读：密集行正确，缺失行 None。
    assert_eq!(c.get_typed_value(1, 2), Some(TypedValue::F64(20.0)));
    assert_eq!(c.get_typed_value(1, 3), None);
    assert_eq!(c.get_typed_value(0, 3), Some(TypedValue::F64(99.0)));
    assert_col_len_eq_rows(&c, 0);
    assert_col_len_eq_rows(&c, 1);
}

#[test]
fn ind_sparse_then_dense_keeps_aligned() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 首行 col0/col1 密集。
    c.append(100, &f64s(&[(0, 0.0), (1, 10.0)]), &[(0, 0xC0)], 0);
    // 稀疏行只写 col0。
    c.append(200, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    // 再密集写多行。
    for i in 0..3 {
        c.append(
            300 + 100 * i,
            &f64s(&[(0, 10.0 + i as f64), (1, 20.0 + i as f64)]),
            &[(0, 0xC0)],
            0,
        );
    }
    // col1 在 row1 缺失后进入 presence 模式，后续密集写继续对齐增长。
    assert_eq!(
        c.presence.get(&1),
        Some(&vec![true, false, true, true, true])
    );
    // col0 从 row1 起也有 entry（稀疏写触发物化）。
    assert_eq!(
        c.presence.get(&0),
        Some(&vec![true, true, true, true, true])
    );
    // 读取：缺失行 None，密集行正确。
    assert_eq!(c.get_typed_value(1, 1), None);
    assert_eq!(c.get_typed_value(1, 2), Some(TypedValue::F64(20.0)));
    assert_eq!(c.get_typed_value(1, 4), Some(TypedValue::F64(22.0)));
    assert_col_len_eq_rows(&c, 0);
    assert_col_len_eq_rows(&c, 1);
}

#[test]
fn ind_dense_type_mismatch_materializes_and_others_unaffected() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 密集 2 行，col0(F64)/col1(F64)/col2(F64)。
    c.append(100, &f64s(&[(0, 1.0), (1, 10.0), (2, 100.0)]), &[(0, 0xC0)], 0);
    c.append(200, &f64s(&[(0, 2.0), (1, 20.0), (2, 200.0)]), &[(0, 0xC0)], 0);
    assert!(c.presence.is_empty());

    // 密集写 row2：col0 类型不匹配（I64 进 F64 列）。
    c.append(
        300,
        &[
            (0, TypedValue::I64(9)),
            (1, TypedValue::F64(30.0)),
            (2, TypedValue::F64(300.0)),
        ],
        &[(0, 0xC0)],
        0,
    );

    // col0 物化 [true,true,false]；col1/col2 不受牵连保持纯密集。
    assert_eq!(c.presence.get(&0), Some(&vec![true, true, false]));
    assert!(c.presence.get(&1).is_none(), "col1 不应受牵连");
    assert!(c.presence.get(&2).is_none(), "col2 不应受牵连");
    // 读 col0 row2 None（不误读占位），row0/row1 仍正确。
    assert_eq!(c.get_typed_value(0, 2), None);
    assert_eq!(c.get_typed_value(0, 1), Some(TypedValue::F64(2.0)));
    assert_eq!(c.get_typed_value(1, 2), Some(TypedValue::F64(30.0)));
    assert_col_len_eq_rows(&c, 0);
}

#[test]
fn ind_fresh_table_first_row_suppresses_presence() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 首行全列密集。
    c.append(100, &f64s(&[(0, 1.0), (1, 10.0)]), &[(0, 0xC0)], 0);
    assert!(c.presence.is_empty(), "首行不应建 presence");
    // 连续密集。
    c.append(200, &f64s(&[(0, 2.0), (1, 20.0)]), &[(0, 0xC0)], 0);
    c.append(300, &f64s(&[(0, 3.0), (1, 30.0)]), &[(0, 0xC0)], 0);
    assert!(c.presence.is_empty(), "连续密集仍应空");
    // 读值正确。
    assert_eq!(c.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
    assert_eq!(c.get_typed_value(1, 2), Some(TypedValue::F64(30.0)));
    // 首次稀疏缺失才物化。
    c.append(400, &f64s(&[(0, 4.0)]), &[(0, 0xC0)], 0);
    assert_eq!(c.presence.get(&0), Some(&vec![true, true, true, true]));
    assert_eq!(c.presence.get(&1), Some(&vec![true, true, true, false]));
    assert_eq!(c.get_typed_value(1, 3), None);
}

#[test]
fn ind_non_first_row_new_column_backfills_false() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 前 2 行只有 col0（密集）。
    c.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    c.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    assert!(c.presence.is_empty());

    // row2 新增 col1（非首行）→ 回填 2 个占位，presence [false,false,true]。
    c.append(300, &f64s(&[(0, 3.0), (1, 30.0)]), &[(0, 0xC0)], 0);

    assert_col_len_eq_rows(&c, 0);
    assert_col_len_eq_rows(&c, 1);
    assert_eq!(c.presence.get(&1), Some(&vec![false, false, true]));
    // 新列前两行必须稀疏缺失。
    assert_eq!(c.get_typed_value(1, 0), None);
    assert_eq!(c.get_typed_value(1, 1), None);
    assert_eq!(c.get_typed_value(1, 2), Some(TypedValue::F64(30.0)));
    // col0 在本次稀疏写中被物化 [true,true,true]。
    assert_eq!(c.presence.get(&0), Some(&vec![true, true, true]));
}

#[test]
fn ind_duplicate_col_id_dedup_three_aspects() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 单点内重复 col0，最后值 2.0 胜出。
    c.append(
        100,
        &[(0, TypedValue::F64(1.0)), (0, TypedValue::F64(2.0))],
        &[(0, 0xC0)],
        0,
    );
    // 三方面：列长度 == 行数；值保留最后；presence 为空（首行密集语义）。
    assert_eq!(c.point_count, 1);
    assert_eq!(c.timestamps.len(), 1);
    assert_eq!(c.columns.get(&0).map(|c| c.len()), Some(1), "无 double push");
    assert_eq!(c.get_typed_value(0, 0), Some(TypedValue::F64(2.0)), "保留最后值");
    assert!(c.presence.get(&0).is_none(), "首行密集不建 presence");

    // 追加一行（col0 单列密集）后仍是纯密集，长度==2，值正确。
    c.append(200, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
    assert!(c.presence.get(&0).is_none());
    assert_eq!(c.columns.get(&0).map(|c| c.len()), Some(2));
    assert_eq!(c.get_typed_value(0, 1), Some(TypedValue::F64(3.0)));
}

#[test]
fn ind_dense_then_new_column_not_just_missing() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 密集 2 行 col0。
    c.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    c.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
    assert!(c.presence.is_empty());

    // 稀疏写新增 col1（不是缺失既有列，而是新列）。
    c.append(300, &f64s(&[(0, 3.0), (1, 30.0)]), &[(0, 0xC0)], 0);

    // col0 物化 [true,true,true]；col1 回填 [false,false,true]。
    assert_eq!(c.presence.get(&0), Some(&vec![true, true, true]));
    assert_eq!(c.presence.get(&1), Some(&vec![false, false, true]));
    assert_eq!(c.get_typed_value(1, 0), None);
    assert_eq!(c.get_typed_value(1, 2), Some(TypedValue::F64(30.0)));
    assert_col_len_eq_rows(&c, 0);
    assert_col_len_eq_rows(&c, 1);
}

#[test]
fn ind_mixed_dense_and_sparse_columns_then_dense_write() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 首行：col0 稀疏（只有 col0），col1 未出现。
    c.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
    // row1：col0+col1 密集（col1 新列回填）。
    c.append(200, &f64s(&[(0, 2.0), (1, 20.0)]), &[(0, 0xC0)], 0);
    // row2：只 col0（col1 稀疏缺失）。
    c.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
    // row3：col0+col1 密集（两列都已存在且全覆盖 → is_fully_dense true）。
    c.append(400, &f64s(&[(0, 4.0), (1, 40.0)]), &[(0, 0xC0)], 0);

    // 两列都已有 presence entry，密集写走 Some 分支继续对齐。
    assert_eq!(c.presence.get(&0), Some(&vec![true, true, true, true]));
    assert_eq!(c.presence.get(&1), Some(&vec![false, true, false, true]));
    assert_eq!(c.get_typed_value(1, 2), None);
    assert_eq!(c.get_typed_value(1, 3), Some(TypedValue::F64(40.0)));
    assert_col_len_eq_rows(&c, 0);
    assert_col_len_eq_rows(&c, 1);
}

#[test]
fn ind_dense_many_rows_sparse_once_materialize_correct_true_prefix() {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 密集 5 行 3 列。
    for i in 0..5 {
        c.append(
            100 * (i + 1),
            &f64s(&[(0, i as f64), (1, (i * 10) as f64), (2, (i * 100) as f64)]),
            &[(0, 0xC0)],
            0,
        );
    }
    assert!(c.presence.is_empty());
    // 稀疏写：只 col1。
    c.append(600, &f64s(&[(1, 99.0)]), &[(0, 0xC0)], 0);

    // 物化：col1 前 5 行全 true + 本行 true；col0/col2 前 5 行 true + 本行 false。
    assert_eq!(c.presence.get(&1), Some(&vec![true, true, true, true, true, true]));
    assert_eq!(c.presence.get(&0), Some(&vec![true, true, true, true, true, false]));
    assert_eq!(c.presence.get(&2), Some(&vec![true, true, true, true, true, false]));
    assert_eq!(c.get_typed_value(1, 5), Some(TypedValue::F64(99.0)));
    assert_eq!(c.get_typed_value(0, 5), None);
    assert_eq!(c.get_typed_value(2, 5), None);
    assert_eq!(c.get_typed_value(0, 4), Some(TypedValue::F64(4.0)));
    assert_col_len_eq_rows(&c, 0);
    assert_col_len_eq_rows(&c, 1);
    assert_col_len_eq_rows(&c, 2);
}

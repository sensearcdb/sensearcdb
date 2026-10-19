//! QA 独立回归验证（阶段 1）：MemTable 多类型写入（GAP-ENG-14）。
//!
//! 验证：
//! 1. 同一列先插 F64 再插 I64：类型不匹配 → 告警跳过（不报错、不静默 upcast）。
//! 2. String 列读回应为原字符串，而非 f64。
//! 3. 每列按首次插入值的物理类型建对应 ColVec 变体（非恒 F64）。

use sensearcdb_engine::memtable::col_vec::{ColVec, TypedValue};
use sensearcdb_engine::memtable::ColumnarChunk;
use sensearcdb_engine::TenantId;

#[test]
fn qa_memtable_type_mismatch_skips_not_errors() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);

    // 第一行：col 0 = F64
    chunk.append(100, &[(0, TypedValue::F64(1.5))], &[(0, 0xC0)], 0);
    // 第二行：col 0 = I64（类型不匹配）
    chunk.append(200, &[(0, TypedValue::I64(42))], &[(0, 0xC0)], 0);

    // 列仍为 F64 变体，且只保留了第一行的 F64 值；I64 被跳过。
    assert!(matches!(chunk.columns.get(&0), Some(ColVec::F64(_))));
    // 稀疏对齐（缺口 2 SAC null 位图）：列向量长度恒 == 行数（2 行），
    // 被跳过的 I64 行以 `push_absent` 占位（presence=false），读回 None。
    assert_eq!(chunk.columns.get(&0).unwrap().len(), 2);
    assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(1.5)));
    assert_eq!(chunk.get_typed_value(0, 1), None);

    // 时间轴仍记录了两行（跳过的是「值」，不是「点」）。
    assert_eq!(chunk.point_count, 2);
    assert_eq!(chunk.timestamps, vec![100, 200]);
}

#[test]
fn qa_memtable_string_readback_is_string() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 7);
    chunk.append(
        100,
        &[(0, TypedValue::String("running".to_string()))],
        &[(0, 0xC0)],
        0,
    );
    chunk.append(
        200,
        &[(0, TypedValue::String("stopped".to_string()))],
        &[(0, 0xC0)],
        0,
    );

    assert!(matches!(chunk.columns.get(&0), Some(ColVec::String(_))));
    assert_eq!(
        chunk.get_typed_value(0, 0),
        Some(TypedValue::String("running".to_string()))
    );
    assert_eq!(
        chunk.get_typed_value(0, 1),
        Some(TypedValue::String("stopped".to_string()))
    );
    // 不应退化为 f64：get_value 对 String 列返回 None。
    assert_eq!(chunk.get_value(0, 0), None);
}

#[test]
fn qa_memtable_each_column_typed_by_first_value() {
    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 9);
    chunk.append(
        100,
        &[
            (0, TypedValue::F64(1.0)),
            (1, TypedValue::I64(1)),
            (2, TypedValue::U64(1)),
            (3, TypedValue::I32(1)),
            (4, TypedValue::Bool(true)),
            (5, TypedValue::String("x".to_string())),
            (6, TypedValue::Enum16(1)),
            (7, TypedValue::F32(1.0)),
        ],
        &[(0, 0xC0)],
        0,
    );

    assert!(matches!(chunk.columns.get(&0), Some(ColVec::F64(_))));
    assert!(matches!(chunk.columns.get(&1), Some(ColVec::I64(_))));
    assert!(matches!(chunk.columns.get(&2), Some(ColVec::U64(_))));
    assert!(matches!(chunk.columns.get(&3), Some(ColVec::I32(_))));
    assert!(matches!(chunk.columns.get(&4), Some(ColVec::Bool(_))));
    assert!(matches!(chunk.columns.get(&5), Some(ColVec::String(_))));
    assert!(matches!(chunk.columns.get(&6), Some(ColVec::Enum16(_))));
    assert!(matches!(chunk.columns.get(&7), Some(ColVec::F32(_))));
}

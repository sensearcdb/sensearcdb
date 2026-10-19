//! 多类型值 → JSON 表示辅助函数。
//!
//! REST / SSE / console 输出多列 typed 字段 `values: [{column_id, data_type, value}]`
//! 时复用此处的转换，保证三个出口的字段形状一致，同时保留 `value: f64` 字段向后兼容。

use sensearcdb_common::TypedValue;
use serde_json::{json, Value};

/// 将单个 [`TypedValue`] 转为原始标量 JSON 值。
pub fn typed_value_json(v: &TypedValue) -> Value {
    match v {
        TypedValue::F64(x) => json!(x),
        TypedValue::F32(x) => json!(x),
        TypedValue::I64(x) => json!(x),
        TypedValue::U64(x) => json!(x),
        TypedValue::I32(x) => json!(x),
        TypedValue::Bool(x) => json!(x),
        TypedValue::String(x) => json!(x),
        TypedValue::Enum16(x) => json!(x),
    }
}

/// 将多列 values 转为 typed JSON 数组 `[{column_id, data_type, value}]`。
pub fn typed_columns_json(values: &[(u16, TypedValue)]) -> Vec<Value> {
    values
        .iter()
        .map(|(cid, v)| {
            json!({
                "column_id": cid,
                "data_type": v.data_type(),
                "value": typed_value_json(v),
            })
        })
        .collect()
}

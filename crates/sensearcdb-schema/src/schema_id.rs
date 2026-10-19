//! FNV-1a schema_id 计算。
//!
//! Schema ID 由所有列定义的 (列名 + 数据类型 + 精度) 串联后通过
//! 32-bit FNV-1a 哈希计算得到。相同结构的表（同名列、同类型、同精度）
//! 会产生相同的 schema_id，用于同构表聚类。

use crate::mirror::ColumnDef;
use sensearcdb_common::DataType;

/// FNV-1a 32-bit 偏移基值。
const FNV_OFFSET_BASIS: u32 = 0x811c9dc5;

/// FNV-1a 32-bit 质数乘数。
const FNV_PRIME: u32 = 0x01000193;

/// 计算给定列定义的 FNV-1a schema_id。
///
/// 算法：对每列依次哈希 `column_name.as_bytes()` + `data_type` discriminant (u8)
///       + `precision` 的 little-endian 字节表示。
///
/// # 重要属性
/// - 列顺序敏感：不同的列顺序产生不同的 schema_id
/// - 类型敏感：`DataType::F64` 和 `DataType::F32` 产生不同 hash
/// - 精度敏感：`precision=0.0` 和 `precision=0.001` 产生不同 hash
///
/// # Examples
/// ```ignore
/// let cols1 = vec![ColumnDef { name: "value".into(), data_type: DataType::F64, precision: 0.001, .. }];
/// let cols2 = vec![ColumnDef { name: "value".into(), data_type: DataType::F64, precision: 0.001, .. }];
/// assert_eq!(compute_schema_id(&cols1), compute_schema_id(&cols2));
/// ```
pub fn compute_schema_id(columns: &[ColumnDef]) -> u32 {
    let mut hash: u32 = FNV_OFFSET_BASIS;

    for col in columns {
        // 哈希列名（逐字节）
        for &byte in col.name.as_bytes() {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(FNV_PRIME);
        }

        // 哈希 data_type discriminant
        let dt_discriminant = data_type_to_discriminant(col.data_type);
        hash ^= dt_discriminant as u32;
        hash = hash.wrapping_mul(FNV_PRIME);

        // 哈希精度（f64 → u64 → LE bytes）
        let precision_bits: u64 = col.precision.to_bits();
        for &byte in &precision_bits.to_le_bytes() {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    hash
}

/// 将 `DataType` 映射为 1-based discriminant（proto 兼容）。
///
/// 返回值为 1–8，对应 proto `DataType` enum。空输入（无列）时返回 0。
#[inline]
fn data_type_to_discriminant(dt: DataType) -> u8 {
    match dt {
        DataType::F64 => 1,
        DataType::F32 => 2,
        DataType::I64 => 3,
        DataType::U64 => 4,
        DataType::I32 => 5,
        DataType::Bool => 6,
        DataType::String => 7,
        DataType::Enum16 => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mirror::ColumnDef;
    use sensearcdb_common::DataType;

    fn make_col(name: &str, dt: DataType, precision: f64) -> ColumnDef {
        ColumnDef {
            column_id: 0,
            name: name.to_string(),
            data_type: dt,
            precision,
            nullable: false,
            unit: None,
            description: None,
            deadband: None,
            enum_values: None,
            deleted_at: None,
            range_low: 0.0,
            range_high: 100.0,
            instrument_range_low: None,
            instrument_range_high: None,
            display_digits: 2,
        }
    }

    #[test]
    fn test_empty_columns_returns_offset_basis() {
        // FNV-1a of empty input = offset basis (no bytes hashed)
        assert_eq!(compute_schema_id(&[]), 0x811c9dc5);
    }

    #[test]
    fn test_idempotent() {
        let cols = vec![make_col("temperature", DataType::F64, 0.001)];
        let id1 = compute_schema_id(&cols);
        let id2 = compute_schema_id(&cols);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_order_sensitive() {
        let a = vec![
            make_col("a", DataType::F64, 0.0),
            make_col("b", DataType::F64, 0.0),
        ];
        let b = vec![
            make_col("b", DataType::F64, 0.0),
            make_col("a", DataType::F64, 0.0),
        ];
        assert_ne!(compute_schema_id(&a), compute_schema_id(&b));
    }

    #[test]
    fn test_type_sensitive() {
        let a = vec![make_col("v", DataType::F64, 0.0)];
        let b = vec![make_col("v", DataType::F32, 0.0)];
        assert_ne!(compute_schema_id(&a), compute_schema_id(&b));
    }

    #[test]
    fn test_precision_sensitive() {
        let a = vec![make_col("v", DataType::F64, 0.0)];
        let b = vec![make_col("v", DataType::F64, 0.001)];
        assert_ne!(compute_schema_id(&a), compute_schema_id(&b));
    }

    #[test]
    fn test_same_structure_same_hash() {
        let a = vec![make_col("value", DataType::F64, 0.001)];
        let b = vec![make_col("value", DataType::F64, 0.001)];
        assert_eq!(compute_schema_id(&a), compute_schema_id(&b));
    }

    #[test]
    fn test_multi_column() {
        let cols = vec![
            make_col("ts", DataType::I64, 0.0),
            make_col("value", DataType::F64, 0.001),
            make_col("quality", DataType::U64, 0.0),
        ];
        let id = compute_schema_id(&cols);
        // 只验证稳定性和非零
        assert_ne!(id, 0);
        assert_eq!(id, compute_schema_id(&cols));
    }
}

//! ColVec 枚举：列值存储类型。
//!
//! 每种物理数据类型对应一个变体，存储为列式数组。
//! 提供统一的 `push`、`get`、`len` 方法。

use sensearcdb_common::DataType;

/// 兼容 re-export：`TypedValue` 已下沉到 `sensearcdb-common`（与 `DataType` 同 crate），
/// 此处保留原路径以降低 engine 内既有引用替换风险。
pub use sensearcdb_common::TypedValue;

/// 列值枚举——对应 `DataType` 的每种物理类型。
///
/// SAC Block 编码时根据此枚举选择相应的编码器：
/// - `F64` → Gorilla 编码
/// - `I64`/`U64`/`I32` → Simple8b 编码
/// - `String` → Dictionary+LZ4 编码
/// - `Bool` → RLE 编码
#[derive(Debug, Clone, PartialEq)]
pub enum ColVec {
    /// 64 位浮点值。
    F64(Vec<f64>),
    /// 32 位浮点值。
    F32(Vec<f32>),
    /// 64 位有符号整数。
    I64(Vec<i64>),
    /// 64 位无符号整数。
    U64(Vec<u64>),
    /// 32 位有符号整数。
    I32(Vec<i32>),
    /// 布尔值。
    Bool(Vec<bool>),
    /// UTF-8 字符串。
    String(Vec<String>),
    /// 16 位枚举值。
    Enum16(Vec<u16>),
}

impl ColVec {
    /// 获取列中的元素数量。
    pub fn len(&self) -> usize {
        match self {
            ColVec::F64(v) => v.len(),
            ColVec::F32(v) => v.len(),
            ColVec::I64(v) => v.len(),
            ColVec::U64(v) => v.len(),
            ColVec::I32(v) => v.len(),
            ColVec::Bool(v) => v.len(),
            ColVec::String(v) => v.len(),
            ColVec::Enum16(v) => v.len(),
        }
    }

    /// 检查列是否为空。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 获取所有值作为 f64（数值类型转换）。
    ///
    /// # 注意
    /// `String` 和 `Bool` 类型返回空数组。
    pub fn iter_f64(&self) -> Box<dyn Iterator<Item = f64> + '_> {
        match self {
            ColVec::F64(v) => Box::new(v.iter().copied()),
            ColVec::F32(v) => Box::new(v.iter().map(|&x| x as f64)),
            ColVec::I64(v) => Box::new(v.iter().map(|&x| x as f64)),
            ColVec::U64(v) => Box::new(v.iter().map(|&x| x as f64)),
            ColVec::I32(v) => Box::new(v.iter().map(|&x| x as f64)),
            _ => Box::new(std::iter::empty()),
        }
    }

    /// 创建一个与给定 DataType 对应的空 ColVec。
    ///
    /// # 参数
    /// - `data_type`: 物理数据类型。
    pub fn new_for_type(data_type: DataType) -> Self {
        match data_type {
            DataType::F64 => ColVec::F64(Vec::new()),
            DataType::F32 => ColVec::F32(Vec::new()),
            DataType::I64 => ColVec::I64(Vec::new()),
            DataType::U64 => ColVec::U64(Vec::new()),
            DataType::I32 => ColVec::I32(Vec::new()),
            DataType::Bool => ColVec::Bool(Vec::new()),
            DataType::String => ColVec::String(Vec::new()),
            DataType::Enum16 => ColVec::Enum16(Vec::new()),
        }
    }

    /// 返回此列对应的物理数据类型。
    #[inline]
    pub fn data_type(&self) -> DataType {
        match self {
            ColVec::F64(_) => DataType::F64,
            ColVec::F32(_) => DataType::F32,
            ColVec::I64(_) => DataType::I64,
            ColVec::U64(_) => DataType::U64,
            ColVec::I32(_) => DataType::I32,
            ColVec::Bool(_) => DataType::Bool,
            ColVec::String(_) => DataType::String,
            ColVec::Enum16(_) => DataType::Enum16,
        }
    }

    /// 尝试将带类型值追加到本列。
    ///
    /// 仅当 `value` 的物理类型与本列变体一致时才追加并返回 `true`；
    /// 类型不匹配时返回 `false`（调用方据此跳过或告警，不再静默丢弃）。
    #[inline]
    pub fn push_typed(&mut self, value: &TypedValue) -> bool {
        match (self, value) {
            (ColVec::F64(v), TypedValue::F64(x)) => {
                v.push(*x);
                true
            }
            (ColVec::F32(v), TypedValue::F32(x)) => {
                v.push(*x);
                true
            }
            (ColVec::I64(v), TypedValue::I64(x)) => {
                v.push(*x);
                true
            }
            (ColVec::U64(v), TypedValue::U64(x)) => {
                v.push(*x);
                true
            }
            (ColVec::I32(v), TypedValue::I32(x)) => {
                v.push(*x);
                true
            }
            (ColVec::Bool(v), TypedValue::Bool(x)) => {
                v.push(*x);
                true
            }
            (ColVec::String(v), TypedValue::String(x)) => {
                v.push(x.clone());
                true
            }
            (ColVec::Enum16(v), TypedValue::Enum16(x)) => {
                v.push(*x);
                true
            }
            _ => false,
        }
    }

    /// 追加一个「缺失」占位值，保持列向量长度与行数对齐。
    ///
    /// 占位值本身无意义：读取路径会先查 `ColumnarChunk.presence` 位图，
    /// 稀疏行（presence=false）不会把占位值暴露给上层。这里仅保证
    /// `ColVec` 为密集布局，使 `row_idx` 可作为 O(1) 直接下标。
    pub fn push_absent(&mut self) {
        match self {
            ColVec::F64(v) => v.push(0.0),
            ColVec::F32(v) => v.push(0.0),
            ColVec::I64(v) => v.push(0),
            ColVec::U64(v) => v.push(0),
            ColVec::I32(v) => v.push(0),
            ColVec::Bool(v) => v.push(false),
            ColVec::String(v) => v.push(String::new()),
            ColVec::Enum16(v) => v.push(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_col_vec_len() {
        let v = ColVec::F64(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.len(), 3);
        assert!(!v.is_empty());
    }

    #[test]
    fn test_col_vec_empty() {
        let v = ColVec::I64(Vec::new());
        assert_eq!(v.len(), 0);
        assert!(v.is_empty());
    }

    #[test]
    fn test_iter_f64() {
        let v = ColVec::F64(vec![1.0, 2.0, 3.0]);
        let result: Vec<f64> = v.iter_f64().collect();
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_iter_f64_from_i64() {
        let v = ColVec::I64(vec![10, 20, 30]);
        let result: Vec<f64> = v.iter_f64().collect();
        assert_eq!(result, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn test_new_for_type_creates_correct_variant() {
        assert_eq!(ColVec::new_for_type(DataType::F64), ColVec::F64(Vec::new()));
        assert_eq!(ColVec::new_for_type(DataType::I64), ColVec::I64(Vec::new()));
        assert_eq!(
            ColVec::new_for_type(DataType::String),
            ColVec::String(Vec::new())
        );
        assert_eq!(
            ColVec::new_for_type(DataType::Enum16),
            ColVec::Enum16(Vec::new())
        );
    }

    #[test]
    fn test_typed_value_data_type() {
        assert_eq!(TypedValue::F64(1.0).data_type(), DataType::F64);
        assert_eq!(TypedValue::F32(1.0).data_type(), DataType::F32);
        assert_eq!(TypedValue::I64(1).data_type(), DataType::I64);
        assert_eq!(TypedValue::Bool(true).data_type(), DataType::Bool);
        assert_eq!(TypedValue::String("x".into()).data_type(), DataType::String);
    }

    #[test]
    fn test_push_typed_matching() {
        let mut col = ColVec::new_for_type(DataType::I64);
        assert!(col.push_typed(&TypedValue::I64(10)));
        assert!(col.push_typed(&TypedValue::I64(20)));
        assert_eq!(col, ColVec::I64(vec![10, 20]));
    }

    #[test]
    fn test_push_typed_mismatch_rejected() {
        let mut col = ColVec::new_for_type(DataType::I64);
        assert!(!col.push_typed(&TypedValue::F64(1.0)));
        assert!(!col.push_typed(&TypedValue::String("x".into())));
        assert_eq!(col, ColVec::I64(vec![]));
    }
}

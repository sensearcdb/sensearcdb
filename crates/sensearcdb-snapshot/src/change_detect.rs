//! 值变化检测——双路径比较策略。
//!
//! ## 双路径策略
//!
//! 1. **版本号路径**（快速路径）：版本号不同则视为变化，跳过值比较。
//! 2. **值比较路径**（精确路径）：带 F64_EPSILON 的浮点比较，
//!    处理低振幅波动场景（噪声 < epsilon 的变化不触发推送）。
//!
//! ## F64_EPSILON 比较算法
//!
//! 对于接近零的值使用绝对容差，对于大值使用相对容差：
//! - `|a| < 1e-10 && |b| < 1e-10` → 绝对容差 `F64_EPSILON`
//! - 否则 → 相对容差 `F64_EPSILON * max(|a|, |b|, 1.0)`
//!
//! NaN 值处理：任何 NaN 与任何值（包括自身）比较均返回 `false`（不相等）。

use sensearcdb_common::TypedValue;

/// 浮点相等比较的默认 epsilon。
pub const F64_EPSILON: f64 = 1e-12;

/// 小值阈值：当两个值均小于此阈值时使用绝对容差。
const SMALL_VALUE_THRESHOLD: f64 = 1e-10;

/// 判断两个 f64 值是否在 F64_EPSILON 容差范围内相等。
///
/// ## 特殊值处理
///
/// | 输入             | 结果    | 说明                              |
/// |------------------|---------|-----------------------------------|
/// | NaN, 任意值       | `false` | NaN 与任何值不相等                 |
/// | 任意值, NaN       | `false` | 同上                              |
/// | +∞, +∞           | `true`  | 无穷大视为自身相等                 |
/// | -∞, -∞           | `true`  | 同上                              |
/// | +∞, -∞           | `false` | 正负无穷不相等                     |
/// | 0.0, 1e-13       | `false` | 超出绝对容差                       |
/// | 0.0, 1e-15       | `true`  | 在绝对容差内                       |
/// | 1.0, 1.0+1e-13   | `true`  | 在相对容差内 (1e-12 * 1.0)         |
/// | 1.0, 1.0+1e-11   | `false` | 超出相对容差                       |
///
/// # Examples
///
/// ```
/// use sensearcdb_snapshot::change_detect::values_equal;
///
/// assert!(values_equal(1.0, 1.0));
/// assert!(values_equal(1.0, 1.0 + 1e-13));
/// assert!(!values_equal(1.0, 1.0 + 1e-11));
/// assert!(!values_equal(f64::NAN, f64::NAN));
/// assert!(!values_equal(1.0, f64::NAN));
/// ```
#[inline]
pub fn values_equal(a: f64, b: f64) -> bool {
    // NaN 与任何值不相等（包括 NaN 自身）
    if a.is_nan() || b.is_nan() {
        return false;
    }

    // 无穷大处理：INFINITY - INFINITY = NaN，需要特殊处理
    if a.is_infinite() || b.is_infinite() {
        // 同号无穷相等，异号不相等
        return a == b;
    }

    let diff = (a - b).abs();

    // 接近零的值使用绝对容差
    if a.abs() < SMALL_VALUE_THRESHOLD && b.abs() < SMALL_VALUE_THRESHOLD {
        return diff < F64_EPSILON;
    }

    // 大值使用相对容差
    let scale = a.abs().max(b.abs());
    diff < F64_EPSILON * scale.max(1.0)
}

/// 双路径变化检测结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeResult {
    /// 值已变化（版本号路径或值比较路径确认）。
    Changed,
    /// 值未变化。
    Unchanged,
    /// 新条目（之前不存在）。
    NewEntry,
}

/// 双路径变化检测：先比较版本号，再比较值。
///
/// 如果 `existing_entry` 为 `None`，则视为新条目（返回 `NewEntry`）。
/// 如果 `existing_entry.version != new_version`，快速返回 `Changed`。
/// 否则通过 `values_equal` 进行精确的值比较。
///
/// # Arguments
///
/// - `existing_value`: 当前存储的值。
/// - `existing_quality`: 当前存储的质量码。
/// - `new_value`: 新写入的值。
/// - `new_quality`: 新写入的质量码。
///
/// # Returns
///
/// - `ChangeResult::NewEntry` — tag 之前不存在。
/// - `ChangeResult::Changed` — 值或质量码有变化。
/// - `ChangeResult::Unchanged` — 无变化。
#[inline]
pub fn detect_change(
    existing_value: Option<f64>,
    existing_quality: Option<u8>,
    new_value: f64,
    new_quality: u8,
) -> ChangeResult {
    match (existing_value, existing_quality) {
        (None, _) => ChangeResult::NewEntry,
        (Some(old_val), Some(old_qual)) => {
            if !values_equal(old_val, new_value) || old_qual != new_quality {
                ChangeResult::Changed
            } else {
                ChangeResult::Unchanged
            }
        }
        _ => ChangeResult::NewEntry,
    }
}

/// 按类型分派判断两个 [`TypedValue`] 是否相等。
///
/// - 数值型（F64/F32）走 epsilon 相对/绝对容差。
/// - 整型（I64/U64/I32）、Bool、Enum16 精确相等。
/// - String 字符串相等。
/// - 类型不匹配视为不相等（Changed）。
#[inline]
pub fn typed_values_equal(a: &TypedValue, b: &TypedValue) -> bool {
    match (a, b) {
        (TypedValue::F64(x), TypedValue::F64(y)) => values_equal(*x, *y),
        (TypedValue::F32(x), TypedValue::F32(y)) => values_equal(*x as f64, *y as f64),
        (TypedValue::I64(x), TypedValue::I64(y)) => x == y,
        (TypedValue::U64(x), TypedValue::U64(y)) => x == y,
        (TypedValue::I32(x), TypedValue::I32(y)) => x == y,
        (TypedValue::Bool(x), TypedValue::Bool(y)) => x == y,
        (TypedValue::String(x), TypedValue::String(y)) => x == y,
        (TypedValue::Enum16(x), TypedValue::Enum16(y)) => x == y,
        _ => false,
    }
}

/// 判断两个多列值切片是否相等（column_id 升序）。
fn typed_slices_equal(a: &[(u16, TypedValue)], b: &[(u16, TypedValue)]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|((ca, va), (cb, vb))| ca == cb && typed_values_equal(va, vb))
}

/// 判断两个多列质量码切片是否相等（column_id 升序）。
fn quality_slices_equal(a: &[(u16, u8)], b: &[(u16, u8)]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|((ca, qa), (cb, qb))| ca == cb && qa == qb)
}

/// 判断两个列级时间戳切片是否相等（column_id 升序，时间戳精确相等）。
fn ts_slices_equal(a: &[(u16, i64)], b: &[(u16, i64)]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|((ca, ta), (cb, tb))| ca == cb && ta == tb)
}

/// 多类型化值变化检测：比较完整多列 values/qualities。
///
/// - 之前不存在 → `NewEntry`。
/// - 任一列值（按类型分派）或质量码变化 → `Changed`。
/// - 否则 → `Unchanged`。
///
/// 入参 `existing_values` / `existing_qualities` 与 `new_values` / `new_qualities`
/// 均需按 column_id 升序排列（由 `SnapshotStore::update_typed` 归一化保证）。
#[inline]
pub fn detect_change_typed(
    existing_values: Option<&[(u16, TypedValue)]>,
    existing_qualities: Option<&[(u16, u8)]>,
    new_values: &[(u16, TypedValue)],
    new_qualities: &[(u16, u8)],
) -> ChangeResult {
    detect_change_typed_full(
        existing_values,
        existing_qualities,
        None,
        None,
        new_values,
        new_qualities,
        &[],
        &[],
    )
}

/// 多类型化值变化检测（含列级时间戳）：比较完整多列 values/qualities/ts_values/server_ts_values。
///
/// - 之前不存在 → `NewEntry`。
/// - 任一列值（按类型分派）、质量码、列级 source ts 或列级 server ts 变化 → `Changed`。
/// - 否则 → `Unchanged`。
///
/// 入参各切片均需按 column_id 升序排列。`existing_ts_values` / `existing_server_ts_values`
/// 为 `None` 时按空切片处理（兼容未列级化的调用方）。
#[inline]
pub fn detect_change_typed_full(
    existing_values: Option<&[(u16, TypedValue)]>,
    existing_qualities: Option<&[(u16, u8)]>,
    existing_ts_values: Option<&[(u16, i64)]>,
    existing_server_ts_values: Option<&[(u16, i64)]>,
    new_values: &[(u16, TypedValue)],
    new_qualities: &[(u16, u8)],
    new_ts_values: &[(u16, i64)],
    new_server_ts_values: &[(u16, i64)],
) -> ChangeResult {
    match existing_values {
        None => ChangeResult::NewEntry,
        Some(old_vals) => {
            let old_quals = existing_qualities.unwrap_or(&[]);
            let old_ts = existing_ts_values.unwrap_or(&[]);
            let old_sts = existing_server_ts_values.unwrap_or(&[]);
            if !typed_slices_equal(old_vals, new_values)
                || !quality_slices_equal(old_quals, new_qualities)
                || !ts_slices_equal(old_ts, new_ts_values)
                || !ts_slices_equal(old_sts, new_server_ts_values)
            {
                ChangeResult::Changed
            } else {
                ChangeResult::Unchanged
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── values_equal ───

    #[test]
    fn test_values_equal_exact() {
        assert!(values_equal(0.0, 0.0));
        assert!(values_equal(1.0, 1.0));
        assert!(values_equal(-5.0, -5.0));
    }

    #[test]
    fn test_values_equal_within_epsilon() {
        assert!(values_equal(1.0, 1.0 + 1e-13));
        // 1e-12 * 100.0 = 1e-10 boundary; use 1e-11 to stay strictly within
        assert!(values_equal(100.0, 100.0 + 1e-11));
    }

    #[test]
    fn test_values_equal_beyond_epsilon() {
        assert!(!values_equal(1.0, 1.0 + 1e-11));
        assert!(!values_equal(0.0, 1e-12 + 1e-12));
    }

    #[test]
    fn test_values_equal_nan() {
        assert!(!values_equal(f64::NAN, f64::NAN));
        assert!(!values_equal(1.0, f64::NAN));
        assert!(!values_equal(f64::NAN, 0.0));
    }

    #[test]
    fn test_values_equal_infinity() {
        assert!(values_equal(f64::INFINITY, f64::INFINITY));
        assert!(values_equal(f64::NEG_INFINITY, f64::NEG_INFINITY));
        assert!(!values_equal(f64::INFINITY, f64::NEG_INFINITY));
    }

    #[test]
    fn test_values_equal_zero_boundary() {
        // 在绝对容差内
        assert!(values_equal(0.0, 1e-15));
        assert!(values_equal(0.0, 0.0));
        // 超出绝对容差
        assert!(!values_equal(0.0, 1e-11));
    }

    #[test]
    fn test_values_equal_negative() {
        assert!(values_equal(-1.0, -1.0));
        assert!(values_equal(-1.0, -1.0 + 1e-13));
    }

    // ─── detect_change ───

    #[test]
    fn test_detect_new_entry() {
        assert_eq!(
            detect_change(None, None, 42.0, 0xC0),
            ChangeResult::NewEntry
        );
    }

    #[test]
    fn test_detect_changed_value() {
        assert_eq!(
            detect_change(Some(1.0), Some(0xC0), 2.0, 0xC0),
            ChangeResult::Changed
        );
    }

    #[test]
    fn test_detect_changed_quality() {
        assert_eq!(
            detect_change(Some(1.0), Some(0xC0), 1.0, 0x00),
            ChangeResult::Changed
        );
    }

    #[test]
    fn test_detect_unchanged() {
        assert_eq!(
            detect_change(Some(1.0), Some(0xC0), 1.0, 0xC0),
            ChangeResult::Unchanged
        );
    }

    #[test]
    fn test_detect_epsilon_unchanged() {
        assert_eq!(
            detect_change(Some(1.0), Some(0xC0), 1.0 + 1e-13, 0xC0),
            ChangeResult::Unchanged
        );
    }

    #[test]
    fn test_detect_nan_change() {
        assert_eq!(
            detect_change(Some(1.0), Some(0xC0), f64::NAN, 0xC0),
            ChangeResult::Changed
        );
    }

    // ─── typed_values_equal（按类型分派） ───

    #[test]
    fn test_typed_values_equal_numeric_epsilon() {
        assert!(typed_values_equal(
            &TypedValue::F64(1.0),
            &TypedValue::F64(1.0 + 1e-13)
        ));
        assert!(!typed_values_equal(
            &TypedValue::F64(1.0),
            &TypedValue::F64(1.0 + 1e-11)
        ));
        assert!(typed_values_equal(
            &TypedValue::F32(1.0),
            &TypedValue::F32(1.0)
        ));
    }

    #[test]
    fn test_typed_values_equal_integer_exact() {
        assert!(typed_values_equal(
            &TypedValue::I64(42),
            &TypedValue::I64(42)
        ));
        assert!(!typed_values_equal(
            &TypedValue::I64(42),
            &TypedValue::I64(43)
        ));
        assert!(typed_values_equal(
            &TypedValue::U64(42),
            &TypedValue::U64(42)
        ));
        assert!(typed_values_equal(&TypedValue::I32(7), &TypedValue::I32(7)));
        // 整型不走 epsilon：1e-12 级别的差异不可能出现，精确相等
        assert!(!typed_values_equal(
            &TypedValue::I64(1),
            &TypedValue::I64(2)
        ));
    }

    #[test]
    fn test_typed_values_equal_bool_enum() {
        assert!(typed_values_equal(
            &TypedValue::Bool(true),
            &TypedValue::Bool(true)
        ));
        assert!(!typed_values_equal(
            &TypedValue::Bool(true),
            &TypedValue::Bool(false)
        ));
        assert!(typed_values_equal(
            &TypedValue::Enum16(3),
            &TypedValue::Enum16(3)
        ));
        assert!(!typed_values_equal(
            &TypedValue::Enum16(3),
            &TypedValue::Enum16(4)
        ));
    }

    #[test]
    fn test_typed_values_equal_string() {
        assert!(typed_values_equal(
            &TypedValue::String("open".into()),
            &TypedValue::String("open".into())
        ));
        assert!(!typed_values_equal(
            &TypedValue::String("open".into()),
            &TypedValue::String("close".into())
        ));
    }

    #[test]
    fn test_typed_values_equal_type_mismatch_changed() {
        // 类型不匹配视为不相等（Changed），即使 as_f64 后数值相同
        assert!(!typed_values_equal(
            &TypedValue::F64(1.0),
            &TypedValue::I64(1)
        ));
        assert!(!typed_values_equal(
            &TypedValue::String("1".into()),
            &TypedValue::F64(1.0)
        ));
    }

    // ─── detect_change_typed ───

    #[test]
    fn test_detect_change_typed_new_entry() {
        assert_eq!(
            detect_change_typed(None, None, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)]),
            ChangeResult::NewEntry
        );
    }

    #[test]
    fn test_detect_change_typed_unchanged() {
        let vals = [(0, TypedValue::F64(1.0))];
        let quals = [(0, 0xC0)];
        assert_eq!(
            detect_change_typed(Some(&vals), Some(&quals), &vals, &quals),
            ChangeResult::Unchanged
        );
    }

    #[test]
    fn test_detect_change_typed_value_changed() {
        let old = [(0, TypedValue::I64(1))];
        let new = [(0, TypedValue::I64(2))];
        assert_eq!(
            detect_change_typed(Some(&old), Some(&[(0, 0xC0)]), &new, &[(0, 0xC0)]),
            ChangeResult::Changed
        );
    }

    #[test]
    fn test_detect_change_typed_quality_changed() {
        let vals = [(0, TypedValue::F64(1.0))];
        assert_eq!(
            detect_change_typed(Some(&vals), Some(&[(0, 0xC0)]), &vals, &[(0, 0x00)]),
            ChangeResult::Changed
        );
    }

    #[test]
    fn test_detect_change_typed_string_preserved() {
        // String 值不能被 as_f64 折叠（否则 NaN==NaN 误判 unchanged）
        let old = [(0, TypedValue::String("open".into()))];
        let new = [(0, TypedValue::String("close".into()))];
        assert_eq!(
            detect_change_typed(Some(&old), Some(&[(0, 0xC0)]), &new, &[(0, 0xC0)]),
            ChangeResult::Changed
        );
    }

    // ─── detect_change_typed_full（列级时间戳纳入变化判定） ───

    #[test]
    fn test_detect_change_typed_full_ts_change_is_changed() {
        let vals = [(0, TypedValue::F64(1.0))];
        let quals = [(0, 0xC0)];
        // 值、质量不变，仅列级 source ts 变化 → Changed（OPC UA STATUS_VALUE_TIMESTAMP）。
        assert_eq!(
            detect_change_typed_full(
                Some(&vals),
                Some(&quals),
                Some(&[(0, 100)]),
                Some(&[(0, 200)]),
                &vals,
                &quals,
                &[(0, 101)],
                &[(0, 200)],
            ),
            ChangeResult::Changed
        );
    }

    #[test]
    fn test_detect_change_typed_full_server_ts_change_is_changed() {
        let vals = [(0, TypedValue::F64(1.0))];
        let quals = [(0, 0xC0)];
        assert_eq!(
            detect_change_typed_full(
                Some(&vals),
                Some(&quals),
                Some(&[(0, 100)]),
                Some(&[(0, 200)]),
                &vals,
                &quals,
                &[(0, 100)],
                &[(0, 201)],
            ),
            ChangeResult::Changed
        );
    }

    #[test]
    fn test_detect_change_typed_full_identical_is_unchanged() {
        let vals = [(0, TypedValue::F64(1.0))];
        let quals = [(0, 0xC0)];
        assert_eq!(
            detect_change_typed_full(
                Some(&vals),
                Some(&quals),
                Some(&[(0, 100)]),
                Some(&[(0, 200)]),
                &vals,
                &quals,
                &[(0, 100)],
                &[(0, 200)],
            ),
            ChangeResult::Unchanged
        );
    }

    #[test]
    fn test_detect_change_typed_full_new_column_is_changed() {
        let old_vals = [(0, TypedValue::F64(1.0))];
        let new_vals = [(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))];
        assert_eq!(
            detect_change_typed_full(
                Some(&old_vals),
                Some(&[(0, 0xC0)]),
                Some(&[(0, 100)]),
                Some(&[(0, 200)]),
                &new_vals,
                &[(0, 0xC0), (1, 0xC0)],
                &[(0, 100), (1, 100)],
                &[(0, 200), (1, 200)],
            ),
            ChangeResult::Changed
        );
    }
}

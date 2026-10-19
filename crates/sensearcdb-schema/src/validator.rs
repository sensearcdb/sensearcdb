//! table_name / column_name 格式校验。
//!
//! - `table_name`：允许 `[a-z0-9._-]`，最大 256 字符，统一小写（推荐点号路径式）。
//! - `column_name`：`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`，最大 128 字符。

use regex::Regex;
use sensearcdb_common::SenseArcError;
use std::sync::LazyLock;

/// 列名正则：以字母或下划线开头，后跟最多 127 个字母/数字/下划线。
static NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]{0,127}$").expect("static regex must compile")
});

/// 表名正则：小写字母/数字/点号/下划线/短横，最大 256 字符。
static TABLE_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z0-9._-]{1,256}$").expect("static regex must compile"));

/// SQL 保留字列表——这些词不能用作 table_name 或 column_name。
///
/// 仅包含 SQLite 中会产生歧义的关键词。
static SQL_RESERVED: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "ASC",
    "BEGIN",
    "BETWEEN",
    "BY",
    "CASE",
    "CHECK",
    "COLUMN",
    "COMMIT",
    "CREATE",
    "CROSS",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXISTS",
    "FALSE",
    "FROM",
    "FULL",
    "GROUP",
    "HAVING",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "KEY",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NATURAL",
    "NOT",
    "NULL",
    "OF",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PRIMARY",
    "REFERENCES",
    "RIGHT",
    "ROLLBACK",
    "SELECT",
    "SET",
    "TABLE",
    "THEN",
    "TO",
    "TRUE",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    "WITH",
];

/// 规范化表名：统一小写。
///
/// 在持久化前调用，保证同构表聚类与名称索引的一致性。
pub fn normalize_table_name(name: &str) -> String {
    name.to_lowercase()
}

/// 验证表名格式。
///
/// # 规则
/// - 长度 1–256 字符
/// - 匹配 `^[a-z0-9._-]{1,256}$`（允许点号/短横，推荐路径式 `PlantA.Line1.TT101`）
/// - 不能是 SQL 保留字（大小写不敏感）
///
/// # Errors
/// 返回 [`SenseArcError::InvalidTableName`] 如果校验失败。
pub fn validate_table_name(name: &str) -> Result<(), SenseArcError> {
    // 空名检查
    if name.is_empty() {
        return Err(SenseArcError::InvalidTableName(
            "table name must not be empty".to_string(),
        ));
    }

    // 长度检查
    if name.len() > 256 {
        return Err(SenseArcError::InvalidTableName(format!(
            "table name '{}' exceeds 256 characters (got {})",
            name,
            name.len()
        )));
    }

    // 格式校验（小写化后校验，支持入口统一 lowercase）
    let lower = name.to_lowercase();
    if !TABLE_NAME_RE.is_match(&lower) {
        return Err(SenseArcError::InvalidTableName(format!(
            "table name '{}' must match ^[a-z0-9._-]{{1,256}}$ (lowercase letters, digits, dot, underscore, hyphen)",
            name
        )));
    }

    // 保留字检查（大小写不敏感）
    let upper = lower.to_uppercase();
    if SQL_RESERVED.contains(&upper.as_str()) {
        return Err(SenseArcError::InvalidTableName(format!(
            "table name '{}' is a reserved SQL keyword",
            name
        )));
    }

    Ok(())
}

/// 验证列名格式。
///
/// # 规则
/// - 长度 1–128 字符
/// - 匹配 `^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`
/// - 不能是 SQL 保留字
///
/// # Errors
/// 返回 [`SenseArcError::InvalidColumnName`] 如果校验失败。
pub fn validate_column_name(name: &str) -> Result<(), SenseArcError> {
    validate_name(name, "column")
}

/// 通用列名校验。
fn validate_name(name: &str, kind: &str) -> Result<(), SenseArcError> {
    // 空名检查
    if name.is_empty() {
        let msg = format!("{} name must not be empty", kind);
        return Err(SenseArcError::InvalidColumnName(msg));
    }

    // 长度检查
    if name.len() > 128 {
        let msg = format!(
            "{} name '{}' exceeds 128 characters (got {})",
            kind,
            name,
            name.len()
        );
        return Err(SenseArcError::InvalidColumnName(msg));
    }

    // 格式校验
    if !NAME_RE.is_match(name) {
        let msg = format!(
            "{} name '{}' must match pattern: start with letter/underscore, followed by letters/digits/underscores",
            kind, name
        );
        return Err(SenseArcError::InvalidColumnName(msg));
    }

    // 保留字检查（大小写不敏感）
    let upper = name.to_uppercase();
    if SQL_RESERVED.contains(&upper.as_str()) {
        let msg = format!("{} name '{}' is a reserved SQL keyword", kind, name);
        return Err(SenseArcError::InvalidColumnName(msg));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── 合法表名 ───

    #[test]
    fn test_valid_simple_table() {
        assert!(validate_table_name("temperature").is_ok());
    }

    #[test]
    fn test_valid_path_style_table() {
        // PRD 推荐路径式表名（点号分隔）
        assert!(validate_table_name("planta.line1.tt101").is_ok());
    }

    #[test]
    fn test_valid_table_with_hyphen() {
        assert!(validate_table_name("table-name").is_ok());
    }

    #[test]
    fn test_valid_table_with_digits() {
        assert!(validate_table_name("table_42").is_ok());
    }

    #[test]
    fn test_valid_table_uppercase_normalized() {
        // 大写会在校验时被小写化
        assert!(validate_table_name("PlantA.Line1").is_ok());
    }

    #[test]
    fn test_valid_256_chars_table() {
        let name = "a".repeat(256);
        assert!(validate_table_name(&name).is_ok());
    }

    #[test]
    fn test_normalize_table_name() {
        assert_eq!(normalize_table_name("PlantA.Line1"), "planta.line1");
    }

    // ─── 非法表名 ───

    #[test]
    fn test_invalid_empty_table() {
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn test_invalid_table_too_long() {
        let name = "a".repeat(257);
        assert!(validate_table_name(&name).is_err());
    }

    #[test]
    fn test_invalid_table_space() {
        assert!(validate_table_name("table name").is_err());
    }

    #[test]
    fn test_invalid_table_slash() {
        assert!(validate_table_name("table/name").is_err());
    }

    #[test]
    fn test_invalid_table_reserved_word() {
        assert!(validate_table_name("select").is_err());
        assert!(validate_table_name("TABLE").is_err());
    }

    // ─── 列名 ───

    #[test]
    fn test_valid_column() {
        assert!(validate_column_name("value").is_ok());
        assert!(validate_column_name("sensor_1_temp").is_ok());
        assert!(validate_column_name("Boiler_1_Temp").is_ok());
    }

    #[test]
    fn test_invalid_column_special() {
        assert!(validate_column_name("column-name").is_err());
        assert!(validate_column_name("column.name").is_err());
    }

    #[test]
    fn test_invalid_column_reserved() {
        assert!(validate_column_name("FROM").is_err());
    }

    #[test]
    fn test_invalid_column_chinese() {
        assert!(validate_column_name("温度").is_err());
    }
}

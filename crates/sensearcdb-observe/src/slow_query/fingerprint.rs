//! SQL Fingerprint 算法——常量和字面量替换为 `?`。
//!
//! ## 算法
//!
//! 将 SQL 语句中的字面量（字符串、数字、布尔值、NULL）替换为 `?`，
//! 生成参数化指纹。用于慢查询日志中的 sql_fingerprint 字段，
//! 方便按查询模式聚合分析。
//!
//! ## 示例
//!
//! ```text
//! SELECT * FROM sensors WHERE id = 42 AND name = 'temp'
//! -> SELECT * FROM sensors WHERE id = ? AND name = ?
//! ```

/// 对 SQL 语句做参数化指纹提取——将所有字面量替换为 `?`。
///
/// # 替换规则
///
/// | 类型 | 示例 | 替换 |
/// |------|------|------|
/// | 单引号字符串 | `'hello'` | `?` |
/// | 整数/浮点数 | `42`, `3.14` | `?` |
/// | 布尔值 | `TRUE`, `false` | `?` |
/// | NULL | `NULL` | `?` |
///
/// # 注意事项
///
/// - 不解析完整 SQL AST（避免引入 sqlparser 依赖与性能开销）
/// - 按字符扫描，状态机处理引号内转义
/// - 字符串内的数字不替换（如 `'v1.0'` 保持原样）
/// - 非 SQL 文本直接返回原值
pub fn fingerprint_sql(sql: &str) -> String {
    if sql.is_empty() {
        return String::new();
    }

    let bytes = sql.as_bytes();
    let mut result = String::with_capacity(sql.len());
    let mut i = 0;
    let len = bytes.len();

    while i < len {
        let ch = bytes[i];

        match ch {
            // 单引号字符串
            b'\'' => {
                result.push('?');
                // 跳过整个字符串字面量
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        // 检查是否为转义引号 ''
                        if i + 1 < len && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            // 双引号标识符(保留)
            b'"' => {
                result.push('"');
                i += 1;
                while i < len {
                    if bytes[i] == b'"' {
                        if i + 1 < len && bytes[i + 1] == b'"' {
                            result.push('"');
                            result.push('"');
                            i += 2;
                            continue;
                        }
                        result.push('"');
                        i += 1;
                        break;
                    }
                    result.push(bytes[i] as char);
                    i += 1;
                }
            }
            // 数字开头
            b'0'..=b'9' => {
                // 仅在非标识符上下文替换数字
                if is_number_context(bytes, i) {
                    result.push('?');
                    // 跳过整个数字
                    while i < len
                        && (bytes[i].is_ascii_digit()
                            || bytes[i] == b'.'
                            || bytes[i] == b'e'
                            || bytes[i] == b'E'
                            || bytes[i] == b'-'
                            || bytes[i] == b'+')
                    {
                        i += 1;
                    }
                } else {
                    result.push(ch as char);
                    i += 1;
                }
            }
            // 负号开头的数字（仅在数字上下文）
            b'-' if i + 1 < len && bytes[i + 1].is_ascii_digit() => {
                if is_negative_number_context(bytes, i) {
                    result.push('?');
                    i += 1;
                    while i < len
                        && (bytes[i].is_ascii_digit()
                            || bytes[i] == b'.'
                            || bytes[i] == b'e'
                            || bytes[i] == b'E')
                    {
                        i += 1;
                    }
                } else {
                    result.push(ch as char);
                    i += 1;
                }
            }
            // 关键字 NULL / TRUE / FALSE
            _ if should_replace_keyword(bytes, i) => {
                result.push('?');
                // 跳过关键字
                while i < len && bytes[i].is_ascii_alphabetic() {
                    i += 1;
                }
            }
            _ => {
                result.push(ch as char);
                i += 1;
            }
        }
    }

    // 规范化多余空格
    normalize_whitespace(&result)
}

/// 判断当前字符位置是否在数字字面量上下文（前导空白或运算符）。
fn is_number_context(bytes: &[u8], pos: usize) -> bool {
    if pos == 0 {
        return true;
    }
    let prev = bytes[pos - 1];
    matches!(
        prev,
        b' ' | b'\t' | b'\n' | b'\r' | b'(' | b',' | b'=' | b'<' | b'>' | b'+' | b'-' | b'*' | b'/'
    )
}

/// 判断负号是否属于负数上下文（如 `WHERE x = -5`）。
fn is_negative_number_context(bytes: &[u8], pos: usize) -> bool {
    if pos == 0 {
        return false;
    }
    let prev = bytes[pos - 1];
    matches!(
        prev,
        b' ' | b'\t' | b'\n' | b'\r' | b'(' | b',' | b'=' | b'<' | b'>'
    )
}

/// 检查是否为关键字 NULL/TRUE/FALSE（大小写不敏感）。
fn should_replace_keyword(bytes: &[u8], pos: usize) -> bool {
    let remaining = &bytes[pos..];
    let upper: Vec<u8> = remaining
        .iter()
        .take_while(|b| b.is_ascii_alphabetic())
        .map(|b| b.to_ascii_uppercase())
        .collect();

    if upper.is_empty() {
        return false;
    }

    let keyword = std::str::from_utf8(&upper).unwrap_or("");
    let is_keyword = matches!(keyword, "NULL" | "TRUE" | "FALSE");

    if !is_keyword {
        return false;
    }

    // 确认为关键字边界：前导或后随非标识符字符
    let before_ok = pos == 0 || !bytes[pos - 1].is_ascii_alphanumeric() && bytes[pos - 1] != b'_';
    let after_pos = pos + keyword.len();
    let after_ok = after_pos >= bytes.len()
        || !bytes[after_pos].is_ascii_alphanumeric() && bytes[after_pos] != b'_';

    before_ok && after_ok
}

/// 规范化空白字符：多个连续空白 → 单个空格。
fn normalize_whitespace(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut in_whitespace = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !in_whitespace {
                result.push(' ');
                in_whitespace = true;
            }
        } else {
            result.push(ch);
            in_whitespace = false;
        }
    }
    result.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_string_literal() {
        let sql = "SELECT * FROM t WHERE name = 'hello'";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE name = ?");
    }

    #[test]
    fn test_fingerprint_integer() {
        let sql = "SELECT * FROM t WHERE id = 42";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE id = ?");
    }

    #[test]
    fn test_fingerprint_float() {
        let sql = "SELECT * FROM t WHERE val > 3.14";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE val > ?");
    }

    #[test]
    fn test_fingerprint_multiple_literals() {
        let sql = "INSERT INTO t (a, b, c) VALUES (1, 'x', 2.5)";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "INSERT INTO t (a, b, c) VALUES (?, ?, ?)");
    }

    #[test]
    fn test_fingerprint_null() {
        let sql = "SELECT * FROM t WHERE col IS NULL";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE col IS ?");
    }

    #[test]
    fn test_fingerprint_bool() {
        let sql = "SELECT * FROM t WHERE active = TRUE";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE active = ?");
    }

    #[test]
    fn test_fingerprint_negative_number() {
        let sql = "SELECT * FROM t WHERE x = -5";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE x = ?");
    }

    #[test]
    fn test_fingerprint_identifier_with_number() {
        let sql = "SELECT col1, col2 FROM table3";
        let fp = fingerprint_sql(sql);
        // Identifiers containing numbers should NOT be replaced
        assert_eq!(fp, "SELECT col1, col2 FROM table3");
    }

    #[test]
    fn test_fingerprint_in_clause() {
        let sql = "SELECT * FROM t WHERE id IN (1, 2, 3)";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE id IN (?, ?, ?)");
    }

    #[test]
    fn test_fingerprint_escaped_string() {
        let sql = "SELECT * FROM t WHERE name = 'it''s'";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE name = ?");
    }

    #[test]
    fn test_fingerprint_empty() {
        assert_eq!(fingerprint_sql(""), "");
    }

    #[test]
    fn test_fingerprint_double_quoted_identifier() {
        let sql = r#"SELECT "col name" FROM t"#;
        let fp = fingerprint_sql(sql);
        assert!(
            fp.contains("col name"),
            "Double-quoted identifiers must be preserved"
        );
    }

    #[test]
    fn test_fingerprint_between() {
        let sql = "SELECT * FROM t WHERE ts BETWEEN '2024-01-01' AND '2024-12-31'";
        let fp = fingerprint_sql(sql);
        assert_eq!(fp, "SELECT * FROM t WHERE ts BETWEEN ? AND ?");
    }
}

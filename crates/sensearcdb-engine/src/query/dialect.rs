//! SenseArcDialect：sqlparser-rs 扩展方言 + SAMPLE BY 子句解析。
//!
//! 支持 SenseArcDB 特有的 SQL 语法扩展：
//! - 反引号标识符（MySQL 风格）
//! - `time_bucket` 函数
//! - `SAMPLE BY` 子句：时序数据降采样语法
//!
//! ## SAMPLE BY 语法
//!
//! ```sql
//! SELECT ... FROM table WHERE ts BETWEEN start AND end SAMPLE BY 10s [FILL(prev)]
//! ```
//!
//! Interval 单位：`s` (秒), `m` (分), `h` (时), `d` (天)
//!
//! ## 实现策略
//!
//! sqlparser-rs v0.44 不支持自定义子句注入。本实现采用"预提取 + 代理解析"策略：
//! 1. 预处理 SQL 提取 SAMPLE BY 子句及其后的内容
//! 2. 将去除 SAMPLE BY 的 SQL 交给 sqlparser 解析
//! 3. 返回包含可选 SAMPLE BY 信息的扩展 AST

use sqlparser::ast::Statement as SqlStatement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

// ─── SenseArcDialect ───

/// SenseArcDB SQL 方言。
///
/// 基于 sqlparser-rs 的 `GenericDialect`，添加以下扩展：
/// - 支持反引号（`` ` ``）作为标识符引用符
/// - 支持 `#` 和 `@` 作为标识符起始字符
/// - 支持 `SAMPLE` 作为关键字
/// - 支持 `FILL` 作为关键字
#[derive(Debug, Clone, Default)]
pub struct SenseArcDialect;

impl Dialect for SenseArcDialect {
    /// 是否为标识符的合法起始字符。
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic() || ch == '_' || ch == '#' || ch == '@'
    }

    /// 是否为标识符的合法后续字符。
    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_' || ch == '#' || ch == '@' || ch == '$'
    }

    /// 标识符是否需要引号（双引号或反引号）。
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    /// 是否支持聚合中的 FILTER 子句。
    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }
}

// ─── SAMPLE BY 相关类型 ───

/// SAMPLE BY 子句定义。
///
/// 描述时序数据的降采样间隔和填充策略。
#[derive(Debug, Clone, PartialEq)]
pub struct SampleByClause {
    /// 采样间隔（毫秒）
    pub interval_ms: u64,
    /// 原始 interval 字符串（如 "10s"）
    pub interval_raw: String,
    /// 填充策略：None = 不填充, Some(prev) = 用前值填充
    pub fill: Option<FillStrategy>,
}

/// FILL 填充策略。
#[derive(Debug, Clone, PartialEq)]
pub enum FillStrategy {
    /// 用前一个非空值填充
    Prev,
    /// 用指定常量填充
    Value(f64),
    /// 不填充（默认，间隙为 NULL）
    None,
}

// ─── 扩展 SQL 语句 ───

/// SenseArcDB 扩展 SQL 语句。
///
/// 包装 sqlparser-rs 的标准 Statement，附加 SAMPLE BY 信息。
#[derive(Debug, Clone)]
pub struct SenseArcStatement {
    /// sqlparser-rs 解析的标准 AST
    pub statement: SqlStatement,
    /// SAMPLE BY 子句（如果存在）
    pub sample_by: Option<SampleByClause>,
}

// ─── SAMPLE BY 解析器 ───

/// 解析 SAMPLE BY 子句。
///
/// 从 SQL 字符串中提取 `SAMPLE BY <interval> [FILL(...)]` 子句。
///
/// # 参数
/// * `sql` - 完整 SQL 字符串
///
/// # 返回
/// * `Ok((cleaned_sql, sample_by_clause))` — 去除 SAMPLE BY 的 SQL + SAMPLE BY 信息；
///   若 interval + 可选 FILL 之后仍有残留子句，返回 `Err`（不静默丢弃用户子句）。
pub fn extract_sample_by(sql: &str) -> Result<(String, Option<SampleByClause>), String> {
    // 查找 SAMPLE BY 关键字（大小写不敏感）
    let upper = sql.to_uppercase();
    let sample_by_pos = match upper.find("SAMPLE BY") {
        Some(pos) => pos,
        None => return Ok((sql.to_string(), None)),
    };

    // 分割 SQL：SAMPLE BY 之前的部分 + SAMPLE BY 之后的子句
    let before_sample = &sql[..sample_by_pos].trim_end();
    let after_sample = &sql[sample_by_pos + "SAMPLE BY".len()..].trim_start();

    // 解析 interval
    let (interval_ms, interval_raw, remaining) = match parse_interval_from_str(after_sample) {
        Ok((ms, raw, rest)) => (ms, raw, rest),
        Err(_) => {
            // 无法解析 interval，交给 sqlparser 对含 `SAMPLE BY` 的原始 SQL 显式报错。
            return Ok((sql.to_string(), None));
        }
    };

    // 检查 FILL 子句，并取回 FILL 之后的残留内容。
    let (fill, rest_after_fill) = parse_fill_clause(remaining);

    // 收口：interval + 可选 FILL 解析完后若仍有残留子句，显式报错，绝不静默丢弃。
    let residual = rest_after_fill.trim();
    if !residual.is_empty() {
        return Err(format!(
            "SAMPLE BY 后不支持其它子句：`{residual}`（请改用 GROUP BY time(...) 或移除该子句）"
        ));
    }

    let clause = SampleByClause {
        interval_ms,
        interval_raw,
        fill,
    };

    Ok((before_sample.to_string(), Some(clause)))
}

/// 解析 interval 字符串，返回 (毫秒值, 原始字符串, 剩余部分)。
fn parse_interval_from_str(s: &str) -> Result<(u64, String, &str), String> {
    let s = s.trim_start();

    // 提取数字部分
    let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());

    if num_end == 0 {
        return Err("no numeric value in interval".into());
    }

    let num_str = &s[..num_end];
    let num: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid interval number: {}", num_str))?;

    let remaining = &s[num_end..];

    // 提取单位（s/m/h/d/w/mon）；`mon` 为多字符单位，需优先于单字符 `m` 匹配。
    let (multiplier, unit_str, rest) =
        if remaining.len() >= 3 && remaining[..3].eq_ignore_ascii_case("mon") {
            // 月长可变，interval_ms 仅用 30 天名义值；日历桶对齐在 planner 侧用 raw 判定。
            (2_592_000_000u64, "mon".to_string(), &remaining[3..])
        } else {
            let unit_char = remaining.chars().next().unwrap_or('s');
            let multiplier: u64 = match unit_char {
                's' | 'S' => 1_000,
                'm' | 'M' => 60_000,
                'h' | 'H' => 3_600_000,
                'd' | 'D' => 86_400_000,
                'w' | 'W' => 604_800_000,
                other => {
                    return Err(format!(
                        "unknown interval unit '{}', expected s/m/h/d/w/mon",
                        other
                    ));
                }
            };
            (
                multiplier,
                unit_char.to_lowercase().to_string(),
                &remaining[1..],
            )
        };

    let raw = format!("{}{}", num_str, unit_str);

    Ok((num * multiplier, raw, rest))
}

/// 解析 FILL 子句。
///
/// 语法：`FILL(prev)` | `FILL(linear)` | `FILL(value)` | `FILL(null)`
///
/// 返回 `(填充策略, FILL 之后的残留内容)`；若输入不以 FILL 开头，返回
/// `(None, s)`（残留 = 原输入），便于上层统一判定「interval + FILL 后是否还有剩余」。
fn parse_fill_clause(s: &str) -> (Option<FillStrategy>, &str) {
    let s = s.trim_start();
    let upper = s.to_uppercase();

    if !upper.starts_with("FILL") {
        return (None, s);
    }

    let after_fill = &s[4..].trim_start(); // 跳过 "FILL"

    // 期望 '('
    if !after_fill.starts_with('(') {
        return (None, s);
    }

    let inner_start = 1;
    let inner_end = after_fill.find(')').unwrap_or(after_fill.len());
    let inner = after_fill[inner_start..inner_end].trim().to_lowercase();
    // 跳过 ')'（若存在），取回 FILL 之后的残留内容。
    let rest_start = if inner_end < after_fill.len() {
        inner_end + 1
    } else {
        inner_end
    };
    let rest = &after_fill[rest_start..];

    let strategy = match inner.as_str() {
        "prev" => FillStrategy::Prev,
        "null" | "none" => FillStrategy::None,
        other => {
            // 尝试解析为数值常量
            if let Ok(val) = other.parse::<f64>() {
                FillStrategy::Value(val)
            } else {
                // 不认识的策略，默认 Prev
                FillStrategy::Prev
            }
        }
    };

    (Some(strategy), rest)
}

// ─── 公共 API ───

/// 解析 SenseArcDB SQL 语句（含 SAMPLE BY 扩展）。
///
/// 先提取 SAMPLE BY 子句，再将剩余 SQL 交给 sqlparser-rs 解析。
///
/// # Arguments
/// * `sql` - 完整 SQL 语句
///
/// # Returns
/// 包含标准 AST 和可选 SAMPLE BY 的扩展语句
pub fn parse_sensearc_sql(sql: &str) -> Result<SenseArcStatement, String> {
    let (cleaned_sql, sample_by) = extract_sample_by(sql)?;

    let dialect = SenseArcDialect;
    let statements =
        Parser::parse_sql(&dialect, &cleaned_sql).map_err(|e| format!("SQL parse error: {}", e))?;

    if statements.is_empty() {
        return Err("no statement parsed".into());
    }

    Ok(SenseArcStatement {
        statement: statements.into_iter().next().unwrap(),
        sample_by,
    })
}

/// 解析 SAMPLE BY interval 字符串为毫秒值（便捷函数）。
///
/// # Examples
/// ```
/// # use sensearcdb_engine::query::dialect::parse_sample_interval_ms;
/// assert_eq!(parse_sample_interval_ms("10s"), Ok(10_000));
/// assert_eq!(parse_sample_interval_ms("5m"), Ok(300_000));
/// assert_eq!(parse_sample_interval_ms("1h"), Ok(3_600_000));
/// assert_eq!(parse_sample_interval_ms("1d"), Ok(86_400_000));
/// ```
pub fn parse_sample_interval_ms(interval: &str) -> Result<u64, String> {
    parse_interval_from_str(interval).map(|(ms, _, _)| ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::parser::Parser;

    // ─── 基础方言测试 ───

    #[test]
    fn test_parse_simple_select() {
        let dialect = SenseArcDialect;
        let sql = "SELECT * FROM my_table WHERE ts BETWEEN 0 AND 1000";
        let result = Parser::parse_sql(&dialect, sql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_with_backtick() {
        let dialect = SenseArcDialect;
        let sql = "SELECT `value` FROM `table_42`";
        let result = Parser::parse_sql(&dialect, sql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_with_limit() {
        let dialect = SenseArcDialect;
        let sql = "SELECT * FROM t WHERE ts BETWEEN 0 AND 1000 LIMIT 100";
        let result = Parser::parse_sql(&dialect, sql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_identifier_start() {
        let dialect = SenseArcDialect;
        assert!(dialect.is_identifier_start('a'));
        assert!(dialect.is_identifier_start('_'));
        assert!(dialect.is_identifier_start('#'));
        assert!(dialect.is_identifier_start('@'));
        assert!(!dialect.is_identifier_start('1'));
    }

    // ─── SAMPLE BY 测试 ───

    #[test]
    fn test_parse_sample_interval_ms() {
        assert_eq!(parse_sample_interval_ms("10s").unwrap(), 10_000);
        assert_eq!(parse_sample_interval_ms("5m").unwrap(), 300_000);
        assert_eq!(parse_sample_interval_ms("1h").unwrap(), 3_600_000);
        assert_eq!(parse_sample_interval_ms("1d").unwrap(), 86_400_000);
        assert_eq!(parse_sample_interval_ms("30s").unwrap(), 30_000);
    }

    #[test]
    fn test_extract_sample_by_basic() {
        let sql = "SELECT * FROM t WHERE ts BETWEEN 0 AND 1000 SAMPLE BY 10s";
        let (cleaned, sample_by) = extract_sample_by(sql).unwrap();

        assert!(!cleaned.contains("SAMPLE BY"));
        assert!(cleaned.contains("BETWEEN 0 AND 1000"));

        let clause = sample_by.unwrap();
        assert_eq!(clause.interval_ms, 10_000);
        assert_eq!(clause.interval_raw, "10s");
        assert!(clause.fill.is_none());
    }

    #[test]
    fn test_extract_sample_by_with_fill() {
        let sql =
            "SELECT AVG(value) FROM sensors WHERE ts BETWEEN 0 AND 3600000 SAMPLE BY 1m FILL(prev)";
        let (cleaned, sample_by) = extract_sample_by(sql).unwrap();

        assert!(!cleaned.contains("SAMPLE BY"));
        let clause = sample_by.unwrap();
        assert_eq!(clause.interval_ms, 60_000);
        assert_eq!(clause.interval_raw, "1m");
        assert_eq!(clause.fill, Some(FillStrategy::Prev));
    }

    #[test]
    fn test_extract_sample_by_with_fill_null() {
        let sql = "SELECT * FROM t SAMPLE BY 5m FILL(null)";
        let (_cleaned, sample_by) = extract_sample_by(sql).unwrap();

        let clause = sample_by.unwrap();
        assert_eq!(clause.interval_ms, 300_000);
        assert_eq!(clause.fill, Some(FillStrategy::None));
    }

    #[test]
    fn test_extract_sample_by_hour() {
        let sql = "SELECT * FROM t SAMPLE BY 1h";
        let (_cleaned, sample_by) = extract_sample_by(sql).unwrap();

        let clause = sample_by.unwrap();
        assert_eq!(clause.interval_ms, 3_600_000);
        assert_eq!(clause.interval_raw, "1h");
    }

    #[test]
    fn test_extract_sample_by_day() {
        let sql = "SELECT * FROM t SAMPLE BY 1d";
        let (_cleaned, sample_by) = extract_sample_by(sql).unwrap();

        let clause = sample_by.unwrap();
        assert_eq!(clause.interval_ms, 86_400_000);
        assert_eq!(clause.interval_raw, "1d");
    }

    #[test]
    fn test_no_sample_by() {
        let sql = "SELECT * FROM t WHERE ts BETWEEN 0 AND 1000";
        let (cleaned, sample_by) = extract_sample_by(sql).unwrap();

        assert_eq!(cleaned, sql);
        assert!(sample_by.is_none());
    }

    #[test]
    fn test_extract_sample_by_week_and_month_units() {
        let (_, week) = extract_sample_by("SELECT * FROM t SAMPLE BY 1w").unwrap();
        assert_eq!(week.unwrap().interval_raw, "1w");

        let (_, mon) = extract_sample_by("SELECT * FROM t SAMPLE BY 1mon").unwrap();
        assert_eq!(mon.unwrap().interval_raw, "1mon");
    }

    #[test]
    fn test_parse_sensearc_sql_with_sample_by() {
        let sql = "SELECT * FROM t WHERE ts BETWEEN 0 AND 1000 SAMPLE BY 10s";
        let result = parse_sensearc_sql(sql).unwrap();

        assert!(result.sample_by.is_some());
        assert_eq!(result.sample_by.unwrap().interval_ms, 10_000);
    }

    #[test]
    fn test_parse_sensearc_sql_without_sample_by() {
        let sql = "SELECT * FROM t WHERE ts BETWEEN 0 AND 1000 LIMIT 100";
        let result = parse_sensearc_sql(sql).unwrap();

        assert!(result.sample_by.is_none());
    }

    #[test]
    fn test_fill_default_prev_for_unknown() {
        // 不认识的 FILL 参数默认回退到 Prev
        let sql = "SELECT * FROM t SAMPLE BY 10s FILL(unknown_strategy)";
        let (_cleaned, sample_by) = extract_sample_by(sql).unwrap();

        let clause = sample_by.unwrap();
        assert_eq!(clause.fill, Some(FillStrategy::Prev));
    }

    #[test]
    fn test_sample_by_case_insensitive() {
        let sql = "SELECT * FROM t sample by 10s fill(prev)";
        let (_cleaned, sample_by) = extract_sample_by(sql).unwrap();

        let clause = sample_by.unwrap();
        assert_eq!(clause.interval_ms, 10_000);
        assert_eq!(clause.fill, Some(FillStrategy::Prev));
    }

    /// 收口：SAMPLE BY 后残留 GROUP BY 子句须显式报错，不静默丢弃。
    #[test]
    fn test_extract_sample_by_residual_group_by_rejected() {
        let sql = "SELECT count(*) FROM t SAMPLE BY 1d GROUP BY time('1h')";
        let err = extract_sample_by(sql).unwrap_err();
        assert!(err.contains("SAMPLE BY 后不支持其它子句"), "err: {err}");
    }

    /// 收口：SAMPLE BY 后残留 LIMIT 子句须显式报错，不静默丢弃。
    #[test]
    fn test_extract_sample_by_residual_limit_rejected() {
        let sql = "SELECT count(*) FROM t SAMPLE BY 10s LIMIT 100";
        let err = extract_sample_by(sql).unwrap_err();
        assert!(err.contains("SAMPLE BY 后不支持其它子句"), "err: {err}");
    }
}

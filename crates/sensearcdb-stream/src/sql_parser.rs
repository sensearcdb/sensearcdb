//! 流计算 SQL 解析器。
//!
//! 解析三类流计算管理 SQL 语句：
//!
//! - `CREATE STREAM COMPUTE <name> ON <source_table>
//!    AS SELECT <aggregations> FROM <source_table>
//!    [PARTITION BY <column>]
//!    WINDOW TYPE <TUMBLING|HOPPING|SESSION>
//!    SIZE <interval> [STEP <interval>]`
//!
//! - `DROP STREAM COMPUTE <name>`
//!
//! - `SHOW STREAM COMPUTES`
//!
//! ## 窗口类型语法
//!
//! - Tumbling: `WINDOW TYPE TUMBLING SIZE 10s`
//! - Hopping:  `WINDOW TYPE HOPPING SIZE 1m STEP 30s`
//! - Session:  `WINDOW TYPE SESSION GAP 5m`
//!
//! ## Interval 格式
//!
//! `<number><unit>`：`10s` (秒), `5m` (分), `1h` (时), `1d` (天)

use sensearcdb_common::AggFunction;

use crate::task::WindowSpec;

// ─── SQL 语句类型 ───

/// 流计算 SQL 语句枚举。
#[derive(Debug, Clone, PartialEq)]
pub enum StreamSqlStatement {
    /// CREATE STREAM COMPUTE ...
    CreateStream {
        /// 任务名称（全局唯一）
        name: String,
        /// 源表名称
        source_table: String,
        /// 目标表名称（如果不指定则自动生成）
        target_table: Option<String>,
        /// 窗口规格
        window_spec: WindowSpec,
        /// 聚合列定义
        aggregations: Vec<AggregationDef>,
        /// 可选的分区键
        partition_by: Option<String>,
    },
    /// DROP STREAM COMPUTE <name>
    DropStream {
        /// 任务名称
        name: String,
    },
    /// SHOW STREAM COMPUTES
    ShowStreams,
}

/// 聚合列定义。
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationDef {
    /// 聚合函数
    pub function: AggFunction,
    /// 源列名称
    pub source_column_name: String,
    /// 输出别名
    pub alias: String,
}

// ─── 解析错误 ───

/// SQL 解析错误类型。
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// 语法错误
    SyntaxError(String),
    /// 不支持的窗口类型
    UnsupportedWindowType(String),
    /// 无效的 interval 格式
    InvalidInterval(String),
    /// 不支持的聚合函数
    UnsupportedAggFunction(String),
    /// 缺少必需子句
    MissingClause(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::SyntaxError(msg) => write!(f, "syntax error: {}", msg),
            ParseError::UnsupportedWindowType(msg) => {
                write!(f, "unsupported window type: {}", msg)
            }
            ParseError::InvalidInterval(msg) => write!(f, "invalid interval: {}", msg),
            ParseError::UnsupportedAggFunction(msg) => {
                write!(f, "unsupported aggregate function: {}", msg)
            }
            ParseError::MissingClause(msg) => write!(f, "missing clause: {}", msg),
        }
    }
}

// ─── 解析器 ───

/// 流计算 SQL 解析器。
///
/// 使用简单的递归下降解析，不依赖 sqlparser-rs（流计算 DDL 语法为其特有）。
pub struct StreamSqlParser {
    /// 输入 token 列表
    tokens: Vec<String>,
    /// 当前解析位置
    pos: usize,
}

impl StreamSqlParser {
    /// 创建新的解析器。
    pub fn new(sql: &str) -> Self {
        let tokens = tokenize(sql);
        Self { tokens, pos: 0 }
    }

    /// 解析一条 SQL 语句。
    pub fn parse(&mut self) -> Result<StreamSqlStatement, ParseError> {
        self.skip_whitespace_tokens();

        if self.pos >= self.tokens.len() {
            return Err(ParseError::SyntaxError("empty SQL statement".into()));
        }

        let first = self.current().to_uppercase();

        match first.as_str() {
            "CREATE" => self.parse_create_stream(),
            "DROP" => self.parse_drop_stream(),
            "SHOW" => self.parse_show_streams(),
            _ => Err(ParseError::SyntaxError(format!(
                "unexpected keyword: {}",
                self.current()
            ))),
        }
    }

    // ─── 内部方法 ───

    /// 解析 CREATE STREAM COMPUTE 语句。
    fn parse_create_stream(&mut self) -> Result<StreamSqlStatement, ParseError> {
        self.expect_keyword("CREATE")?;
        self.expect_keyword("STREAM")?;
        self.expect_keyword("COMPUTE")?;

        // 任务名称
        let name = self.expect_identifier()?;

        // ON <source_table>
        self.expect_keyword("ON")?;
        let source_table = self.expect_identifier()?;

        // AS SELECT ...
        self.expect_keyword("AS")?;
        self.expect_keyword("SELECT")?;

        // 聚合列列表
        let aggregations = self.parse_aggregation_list()?;

        // FROM <source_table> (可选的重复)
        if self.peek_keyword("FROM") {
            self.advance();
            let _from_table = self.expect_identifier()?;
        }

        // [PARTITION BY <column>]
        let partition_by = if self.peek_keyword("PARTITION") {
            self.advance();
            self.expect_keyword("BY")?;
            Some(self.expect_identifier()?)
        } else {
            None
        };

        // WINDOW TYPE <type> SIZE <interval> [STEP <interval>]
        let window_spec = self.parse_window_spec()?;

        // 可选的 INTO <target_table>
        let target_table = if self.peek_keyword("INTO") {
            self.advance();
            Some(self.expect_identifier()?)
        } else {
            None
        };

        Ok(StreamSqlStatement::CreateStream {
            name,
            source_table,
            target_table,
            window_spec,
            aggregations,
            partition_by,
        })
    }

    /// 解析 DROP STREAM COMPUTE 语句。
    fn parse_drop_stream(&mut self) -> Result<StreamSqlStatement, ParseError> {
        self.expect_keyword("DROP")?;
        self.expect_keyword("STREAM")?;
        self.expect_keyword("COMPUTE")?;

        let name = self.expect_identifier()?;

        Ok(StreamSqlStatement::DropStream { name })
    }

    /// 解析 SHOW STREAM COMPUTES 语句。
    fn parse_show_streams(&mut self) -> Result<StreamSqlStatement, ParseError> {
        self.expect_keyword("SHOW")?;
        self.expect_keyword("STREAM")?;
        self.expect_keyword("COMPUTES")?;

        Ok(StreamSqlStatement::ShowStreams)
    }

    /// 解析聚合列列表：`func(col) [AS alias], ...`
    fn parse_aggregation_list(&mut self) -> Result<Vec<AggregationDef>, ParseError> {
        let mut aggs = Vec::new();

        loop {
            let func_name = self.expect_identifier()?.to_uppercase();

            self.expect_token("(")?;
            let col_name = self.expect_identifier()?;
            self.expect_token(")")?;

            // 可选 AS alias
            let alias = if self.peek_keyword("AS") {
                self.advance();
                self.expect_identifier()?
            } else {
                format!("{}_{}", func_name.to_lowercase(), col_name)
            };

            let function = match func_name.as_str() {
                "AVG" => AggFunction::Avg,
                "SUM" => AggFunction::Sum,
                "COUNT" => AggFunction::Count,
                "MIN" => AggFunction::Min,
                "MAX" => AggFunction::Max,
                "FIRST" => AggFunction::First,
                "LAST" => AggFunction::Last,
                "STDDEV" => AggFunction::StdDev,
                other => {
                    return Err(ParseError::UnsupportedAggFunction(other.to_string()));
                }
            };

            aggs.push(AggregationDef {
                function,
                source_column_name: col_name,
                alias,
            });

            // 检查是否有更多列
            if self.peek_token(",") {
                self.advance();
            } else {
                break;
            }
        }

        Ok(aggs)
    }

    /// 解析窗口规格：`WINDOW TYPE <type> SIZE <interval> [STEP <interval> | GAP <interval>]`
    fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
        self.expect_keyword("WINDOW")?;
        self.expect_keyword("TYPE")?;

        let window_type = self.expect_identifier()?.to_uppercase();

        match window_type.as_str() {
            "TUMBLING" => {
                self.expect_keyword("SIZE")?;
                let interval_ms = self.parse_interval()?;
                Ok(WindowSpec::Tumbling { interval_ms })
            }
            "HOPPING" => {
                self.expect_keyword("SIZE")?;
                let interval_ms = self.parse_interval()?;
                self.expect_keyword("STEP")?;
                let step_ms = self.parse_interval()?;
                Ok(WindowSpec::Hopping {
                    interval_ms,
                    step_ms,
                })
            }
            "SESSION" => {
                self.expect_keyword("GAP")?;
                let gap_ms = self.parse_interval()?;
                Ok(WindowSpec::Session { gap_ms })
            }
            other => Err(ParseError::UnsupportedWindowType(other.to_string())),
        }
    }

    /// 解析 interval 字符串为毫秒。
    ///
    /// 格式：`<number><unit>`，其中 unit = s|m|h|d
    fn parse_interval(&mut self) -> Result<u64, ParseError> {
        let token = self.expect_identifier()?;
        parse_interval_ms(&token)
    }

    // ─── Token 辅助方法 ───

    fn current(&self) -> &str {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    fn peek_token(&self, expected: &str) -> bool {
        if self.pos >= self.tokens.len() {
            return false;
        }
        self.tokens[self.pos].eq_ignore_ascii_case(expected)
    }

    fn peek_keyword(&self, expected: &str) -> bool {
        self.peek_token(expected)
    }

    fn expect_token(&mut self, expected: &str) -> Result<(), ParseError> {
        if self.pos >= self.tokens.len() {
            return Err(ParseError::SyntaxError(format!(
                "expected '{}' but reached end of input",
                expected
            )));
        }
        let token = &self.tokens[self.pos];
        if token == expected {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::SyntaxError(format!(
                "expected '{}' but got '{}'",
                expected, token
            )))
        }
    }

    fn expect_keyword(&mut self, expected: &str) -> Result<(), ParseError> {
        if self.pos >= self.tokens.len() {
            return Err(ParseError::MissingClause(format!(
                "expected keyword '{}' but reached end of input",
                expected
            )));
        }
        let token = &self.tokens[self.pos];
        if token.eq_ignore_ascii_case(expected) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::SyntaxError(format!(
                "expected keyword '{}' but got '{}'",
                expected, token
            )))
        }
    }

    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        if self.pos >= self.tokens.len() {
            return Err(ParseError::SyntaxError(
                "expected identifier but reached end of input".into(),
            ));
        }
        let token = self.tokens[self.pos].clone();
        self.advance();
        Ok(token)
    }

    /// 跳过空白/注释 token（当前 tokenizer 不产生这类 token）
    fn skip_whitespace_tokens(&self) {
        // no-op: tokenizer 已经过滤了空白
    }
}

// ─── Tokenizer ───

/// 将 SQL 字符串分解为 token 列表。
///
/// 识别：
/// - 关键字 / 标识符（字母开头 + 字母数字下划线）
/// - 数字
/// - 标点符号（括号、逗号等）
/// - 忽略空白和注释
fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let len = chars.len();

    while i < len {
        let ch = chars[i];

        // 跳过空白
        if ch.is_whitespace() {
            i += 1;
            continue;
        }

        // 行注释 --
        if ch == '-' && i + 1 < len && chars[i + 1] == '-' {
            i += 2;
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        // 块注释 /* */
        if ch == '/' && i + 1 < len && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2; // 跳过 */
            continue;
        }

        // 标识符 / 关键字（含 interval 如 10s）
        if ch.is_ascii_alphabetic() || ch == '_' || ch == '`' {
            if ch == '`' {
                // 反引号标识符
                let start = i;
                i += 1;
                while i < len && chars[i] != '`' {
                    i += 1;
                }
                let ident: String = chars[start + 1..i].iter().collect();
                i += 1; // 跳过关闭反引号
                tokens.push(ident);
            } else {
                let start = i;
                while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                tokens.push(ident);
            }
            continue;
        }

        // 数字（含 interval: 10s, 5m, 1h, 1d）
        if ch.is_ascii_digit() {
            let start = i;
            while i < len && chars[i].is_ascii_digit() {
                i += 1;
            }
            // 检查是否紧跟 interval 单位
            if i < len
                && matches!(chars[i], 's' | 'm' | 'h' | 'd')
                && (i + 1 >= len || !chars[i + 1].is_ascii_alphanumeric())
            {
                let num: String = chars[start..i].iter().collect();
                let unit = chars[i];
                i += 1;
                tokens.push(format!("{}{}", num, unit));
            } else {
                let num: String = chars[start..i].iter().collect();
                tokens.push(num);
            }
            continue;
        }

        // 标点符号
        match ch {
            '(' | ')' | ',' | '*' | '+' | '-' | '/' | '=' => {
                tokens.push(ch.to_string());
                i += 1;
            }
            _ => {
                // 跳过无法识别的字符
                i += 1;
            }
        }
    }

    tokens
}

// ─── 公共函数 ───

/// 解析 interval 字符串为毫秒值。
///
/// # Examples
/// ```
/// # use sensearcdb_stream::sql_parser::parse_interval_ms;
/// assert_eq!(parse_interval_ms("10s").unwrap(), 10_000);
/// assert_eq!(parse_interval_ms("5m").unwrap(), 300_000);
/// assert_eq!(parse_interval_ms("1h").unwrap(), 3_600_000);
/// assert_eq!(parse_interval_ms("1d").unwrap(), 86_400_000);
/// ```
pub fn parse_interval_ms(interval: &str) -> Result<u64, ParseError> {
    if interval.is_empty() {
        return Err(ParseError::InvalidInterval("empty interval".into()));
    }

    let (num_str, unit) = interval.split_at(interval.len() - 1);

    let num: u64 = num_str
        .parse()
        .map_err(|_| ParseError::InvalidInterval(format!("invalid number: {}", num_str)))?;

    let multiplier: u64 = match unit {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        other => {
            return Err(ParseError::InvalidInterval(format!(
                "unknown unit '{}', expected s/m/h/d",
                other
            )));
        }
    };

    Ok(num * multiplier)
}

/// 便捷函数：解析一条流计算 SQL 语句。
pub fn parse_stream_sql(sql: &str) -> Result<StreamSqlStatement, ParseError> {
    let mut parser = StreamSqlParser::new(sql);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval_ms() {
        assert_eq!(parse_interval_ms("10s").unwrap(), 10_000);
        assert_eq!(parse_interval_ms("5m").unwrap(), 300_000);
        assert_eq!(parse_interval_ms("1h").unwrap(), 3_600_000);
        assert_eq!(parse_interval_ms("1d").unwrap(), 86_400_000);
        assert_eq!(parse_interval_ms("30s").unwrap(), 30_000);
        assert_eq!(parse_interval_ms("120m").unwrap(), 7_200_000);
    }

    #[test]
    fn test_parse_interval_ms_invalid() {
        assert!(parse_interval_ms("").is_err());
        assert!(parse_interval_ms("abc").is_err());
        assert!(parse_interval_ms("10x").is_err());
    }

    #[test]
    fn test_parse_create_tumbling() {
        let sql = "CREATE STREAM COMPUTE my_stream ON t_sensors AS SELECT AVG(value) FROM t_sensors WINDOW TYPE TUMBLING SIZE 10s";
        let result = parse_stream_sql(sql).unwrap();

        match result {
            StreamSqlStatement::CreateStream {
                name,
                source_table,
                window_spec,
                aggregations,
                ..
            } => {
                assert_eq!(name, "my_stream");
                assert_eq!(source_table, "t_sensors");
                assert_eq!(
                    window_spec,
                    WindowSpec::Tumbling {
                        interval_ms: 10_000
                    }
                );
                assert_eq!(aggregations.len(), 1);
                assert_eq!(aggregations[0].function, AggFunction::Avg);
                assert_eq!(aggregations[0].source_column_name, "value");
            }
            _ => panic!("expected CreateStream"),
        }
    }

    #[test]
    fn test_parse_create_hopping() {
        let sql = "CREATE STREAM COMPUTE hop1 ON t1 AS SELECT SUM(val) FROM t1 WINDOW TYPE HOPPING SIZE 1m STEP 30s";
        let result = parse_stream_sql(sql).unwrap();

        match result {
            StreamSqlStatement::CreateStream { window_spec, .. } => {
                assert_eq!(
                    window_spec,
                    WindowSpec::Hopping {
                        interval_ms: 60_000,
                        step_ms: 30_000,
                    }
                );
            }
            _ => panic!("expected CreateStream"),
        }
    }

    #[test]
    fn test_parse_create_session() {
        let sql = "CREATE STREAM COMPUTE sess1 ON t1 AS SELECT COUNT(val) FROM t1 WINDOW TYPE SESSION GAP 5m";
        let result = parse_stream_sql(sql).unwrap();

        match result {
            StreamSqlStatement::CreateStream { window_spec, .. } => {
                assert_eq!(window_spec, WindowSpec::Session { gap_ms: 300_000 });
            }
            _ => panic!("expected CreateStream"),
        }
    }

    #[test]
    fn test_parse_create_with_partition() {
        let sql = "CREATE STREAM COMPUTE p1 ON t_sensors AS SELECT AVG(temp) FROM t_sensors PARTITION BY device_id WINDOW TYPE TUMBLING SIZE 1m";
        let result = parse_stream_sql(sql).unwrap();

        match result {
            StreamSqlStatement::CreateStream { partition_by, .. } => {
                assert_eq!(partition_by, Some("device_id".to_string()));
            }
            _ => panic!("expected CreateStream"),
        }
    }

    #[test]
    fn test_parse_drop_stream() {
        let sql = "DROP STREAM COMPUTE my_stream";
        let result = parse_stream_sql(sql).unwrap();
        assert_eq!(
            result,
            StreamSqlStatement::DropStream {
                name: "my_stream".into()
            }
        );
    }

    #[test]
    fn test_parse_show_streams() {
        let sql = "SHOW STREAM COMPUTES";
        let result = parse_stream_sql(sql).unwrap();
        assert_eq!(result, StreamSqlStatement::ShowStreams);
    }

    #[test]
    fn test_parse_multiple_aggregations() {
        let sql = "CREATE STREAM COMPUTE multi ON t1 AS SELECT AVG(a), SUM(b), MAX(c) FROM t1 WINDOW TYPE TUMBLING SIZE 10s";
        let result = parse_stream_sql(sql).unwrap();

        match result {
            StreamSqlStatement::CreateStream { aggregations, .. } => {
                assert_eq!(aggregations.len(), 3);
                assert_eq!(aggregations[0].function, AggFunction::Avg);
                assert_eq!(aggregations[1].function, AggFunction::Sum);
                assert_eq!(aggregations[2].function, AggFunction::Max);
            }
            _ => panic!("expected CreateStream"),
        }
    }
}

//! SlowQueryLog 结构体定义与 JSON 序列化。

use serde::Serialize;

/// 慢查询日志条目。
///
/// 记录单次慢查询的完整上下文——SQL、指纹、执行耗时、扫描行数、来源、W3C trace_id。
/// 通过 `serde::Serialize` 序列化为 JSON Lines 格式写入 `slow_query.log`。
#[derive(Debug, Clone, Serialize)]
pub struct SlowQueryLog {
    /// 日志时间戳 (RFC 3339)。
    pub ts: String,
    /// 日志级别，固定 "SLOW_QUERY"。
    pub level: String,
    /// 查询唯一 ID（UUID short）。
    pub query_id: String,
    /// 查询类型：single / batch / range / aggregate。
    pub query_type: String,
    /// 原始 SQL 语句。
    pub sql: String,
    /// SQL 参数化指纹（常量和字面量替换为 ?）。
    pub sql_fingerprint: String,
    /// 查询执行耗时（毫秒）。
    pub duration_ms: f64,
    /// 扫描的数据行数。
    pub rows_scanned: u64,
    /// 返回的数据行数。
    pub rows_returned: u64,
    /// 涉及的表 ID 列表。
    pub table_ids: Vec<u32>,
    /// 查询来源：rest_api / grpc / sql_console。
    pub source: String,
    /// 客户端 IP 地址。
    pub client_ip: String,
    /// W3C trace ID (32 hex)。
    pub trace_id: String,
}

impl SlowQueryLog {
    /// 判断此查询是否为慢查询（超过阈值）。
    ///
    /// # 参数
    /// - `threshold_ms`: 慢查询阈值（毫秒）
    pub fn is_slow(&self, threshold_ms: u64) -> bool {
        self.duration_ms >= threshold_ms as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slow_query_log_serialization() {
        let log = SlowQueryLog {
            ts: "2024-01-01T00:00:00Z".into(),
            level: "SLOW_QUERY".into(),
            query_id: "abc123".into(),
            query_type: "range".into(),
            sql: "SELECT * FROM sensors WHERE ts > '2024-01-01'".into(),
            sql_fingerprint: "SELECT * FROM sensors WHERE ts > ?".into(),
            duration_ms: 2500.0,
            rows_scanned: 500_000,
            rows_returned: 1_000,
            table_ids: vec![1, 2],
            source: "rest_api".into(),
            client_ip: "192.168.1.100".into(),
            trace_id: "abcd1234abcd1234abcd1234abcd1234".into(),
        };

        let json = serde_json::to_string(&log).unwrap();
        assert!(json.contains("SLOW_QUERY"));
        assert!(json.contains("abc123"));
        assert!(json.contains("2500.0"));
        assert!(json.contains("abcd1234"));
    }

    #[test]
    fn test_is_slow() {
        let log = SlowQueryLog {
            ts: String::new(),
            level: String::new(),
            query_id: String::new(),
            query_type: String::new(),
            sql: String::new(),
            sql_fingerprint: String::new(),
            duration_ms: 1500.0,
            rows_scanned: 0,
            rows_returned: 0,
            table_ids: vec![],
            source: String::new(),
            client_ip: String::new(),
            trace_id: String::new(),
        };

        assert!(log.is_slow(1000));
        assert!(!log.is_slow(2000));
    }
}

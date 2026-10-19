//! W3C trace_id / span_id 生成与传播。
//!
//! 按照 W3C Trace Context 规范生成 32 位 hex 的 `trace_id` 和 16 位 hex 的 `span_id`。
//! trace_id 在请求入口处生成，通过 tracing span 传播到所有下游日志和慢查询记录。

use std::fmt;

/// W3C trace context——trace_id (32 hex) + span_id (16 hex)。
///
/// trace_id 和 span_id 均在构造时通过 fastrand 生成，不依赖全局 RNG 状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// W3C trace ID，32 位十六进制字符。
    pub trace_id: String,
    /// W3C span ID，16 位十六进制字符。
    pub span_id: String,
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

impl TraceContext {
    /// 生成新的 W3C trace context（新的 trace_id + span_id）。
    ///
    /// 使用 `fastrand` thread-local RNG，无锁、无全局状态。
    pub fn new() -> Self {
        Self {
            trace_id: gen_hex(32),
            span_id: gen_hex(16),
        }
    }

    /// 从已有 trace_id 派生新的 span（保留 trace_id，生成新 span_id）。
    ///
    /// 用于下游 span 传播：同一 trace 内的子操作共享 trace_id，
    /// 但拥有独立的 span_id。
    pub fn child_of(parent_trace_id: &str) -> Self {
        Self {
            trace_id: parent_trace_id.to_string(),
            span_id: gen_hex(16),
        }
    }

    /// 从已有的 trace_id 和 span_id 构造。
    ///
    /// 用于从 HTTP header（如 `traceparent`）反序列化。
    pub fn from_parts(trace_id: String, span_id: String) -> Self {
        Self { trace_id, span_id }
    }
}

impl fmt::Display for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.trace_id, self.span_id)
    }
}

/// 生成 `len` 位十六进制随机字符串。
///
/// 每字节生成 2 位 hex 字符，`fastrand::u8(..)` 提供随机字节。
fn gen_hex(len: usize) -> String {
    (0..len)
        .map(|_| {
            let byte = fastrand::u8(..);
            HEX_ALPHABET[(byte & 0x0F) as usize] as char
        })
        .collect()
}

/// 十六进制字母表。
const HEX_ALPHABET: &[u8] = b"0123456789abcdef";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_context_new() {
        let ctx = TraceContext::new();
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
        // 所有字符应为十六进制
        assert!(ctx.trace_id.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(ctx.span_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_trace_context_uniqueness() {
        let ctx1 = TraceContext::new();
        let ctx2 = TraceContext::new();
        // 概率极低碰撞（32 hex → 16^32 空间）
        assert_ne!(ctx1.trace_id, ctx2.trace_id);
    }

    #[test]
    fn test_child_of_preserves_trace_id() {
        let parent = TraceContext::new();
        let child = TraceContext::child_of(&parent.trace_id);
        assert_eq!(child.trace_id, parent.trace_id);
        assert_ne!(child.span_id, parent.span_id);
    }

    #[test]
    fn test_from_parts() {
        let ctx = TraceContext::from_parts(
            "abcd1234abcd1234abcd1234abcd1234".into(),
            "ef01ef01ef01ef01".into(),
        );
        assert_eq!(ctx.trace_id, "abcd1234abcd1234abcd1234abcd1234");
        assert_eq!(ctx.span_id, "ef01ef01ef01ef01");
    }

    #[test]
    fn test_display() {
        let ctx = TraceContext::from_parts("aaaa".repeat(8), "bbbb".repeat(4));
        let s = format!("{}", ctx);
        assert!(s.contains('/'));
        assert!(s.starts_with("aaaa"));
    }
}

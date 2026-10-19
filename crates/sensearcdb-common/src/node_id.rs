//! 集群节点 ID 严格解析（`node-<number>` → `u64`）。
//!
//! openraft 的 `NodeId = u64`，而配置里 `cluster.node_id` 是 `"node-XX"` 形式的
//! 字符串。任何「宽松解析 + 静默兜底」都会在非规范命名下让多节点静默撞到同一个
//! id（历史上 shard 分支的 `unwrap_or(1)` 即属此类 bug），因此统一收敛为这里的
//! 严格解析：必须匹配 `node-(\d+)`，非空 + 全 ASCII 数字，否则报错。

/// 严格解析 `node-<number>` 形式的节点 ID 为 `u64`。
///
/// # 规则
/// - 必须带 `node-` 前缀；
/// - 前缀后必须为非空的全 ASCII 数字序列；
/// - 数字必须能解析为 `u64`。
///
/// 任何不满足项均返回携带原始输入的 `Err`，绝不静默降级。
pub fn parse_node_id(node_id: &str) -> Result<u64, String> {
    let digits = node_id.strip_prefix("node-").ok_or_else(|| {
        format!(
            "invalid node_id '{}': must match `node-<number>` (e.g. node-01)",
            node_id
        )
    })?;

    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "invalid node_id '{}': must match `node-<number>` (e.g. node-01)",
            node_id
        ));
    }

    digits
        .parse::<u64>()
        .map_err(|e| format!("invalid node_id '{}': {}", node_id, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_node_ids() {
        assert_eq!(parse_node_id("node-01").unwrap(), 1);
        assert_eq!(parse_node_id("node-1").unwrap(), 1);
        assert_eq!(parse_node_id("node-42").unwrap(), 42);
        assert_eq!(parse_node_id("node-00123").unwrap(), 123);
    }

    #[test]
    fn rejects_invalid_node_ids() {
        assert!(parse_node_id("").is_err());
        assert!(parse_node_id("node-").is_err());
        assert!(parse_node_id("node-abc").is_err());
        assert!(parse_node_id("node-01-extra").is_err());
        assert!(parse_node_id("01").is_err());
        assert!(parse_node_id("node-0x1").is_err());
        assert!(parse_node_id("node- 1").is_err());
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse_node_id("node-999999999999999999999999").is_err());
    }
}

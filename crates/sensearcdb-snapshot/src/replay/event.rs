//! ReplayEvent — 回放引擎事件类型。
//!
//! 回放过程中产生的事件通过 `ReplaySessionHandle` 的环形缓冲区
//! 暴露给 API 消费者（REST/SSE），用于回放进度监控和结果展示。

use serde::{Deserialize, Serialize};

/// 回放事件类型。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplayEvent {
    /// 下一个数据点已就绪（可从 ReplaySnapshotStore 读取）。
    #[serde(rename = "next_value")]
    NextValue {
        /// 事件时间戳 (epoch ns)。
        event_ts: i64,
        /// 已处理的数据点数。
        processed_count: u64,
        /// 回放进度 (0.0 ~ 1.0)。
        progress: f64,
    },

    /// 派生标签计算完成。
    #[serde(rename = "derived")]
    Derived {
        /// 派生标签 table_id。
        table_id: u32,
        /// 计算结果值。
        value: f64,
        /// 计算结果质量码。
        quality: u8,
        /// 事件时间戳 (epoch ns)。
        event_ts: i64,
    },

    /// 告警事件。
    #[serde(rename = "alert")]
    Alert {
        /// 告警规则 ID。
        rule_id: String,
        /// 告警级别 (0=Info, 1=Warning, 2=Critical)。
        severity: u8,
        /// 告警消息。
        message: String,
        /// 事件时间戳 (epoch ns)。
        event_ts: i64,
    },

    /// 回放状态变更。
    #[serde(rename = "state_change")]
    StateChange {
        /// 旧状态。
        from: String,
        /// 新状态。
        to: String,
        /// 事件时间戳 (epoch ns)。
        event_ts: i64,
    },

    /// 回放心跳保活。
    #[serde(rename = "heartbeat")]
    Heartbeat {
        /// 事件时间戳 (epoch ns)。
        event_ts: i64,
    },

    /// 回放完成。
    #[serde(rename = "completed")]
    Completed {
        /// 总处理数据点数。
        total_processed: u64,
        /// 总耗时 (ns)。
        duration_ns: u64,
        /// 事件时间戳 (epoch ns)。
        event_ts: i64,
    },

    /// 回放错误。
    #[serde(rename = "error")]
    Error {
        /// 错误消息。
        message: String,
        /// 事件时间戳 (epoch ns)。
        event_ts: i64,
    },

    /// 显式降级提示（非错误，非数据点）。
    ///
    /// 用于 `include_derived=true` 但 `derived_definitions=0`、或
    /// `include_alerts=true` 但 `enabled_alert_rules=0` 等「开关打开却无可算」
    /// 场景，确保不静默（铁律「降级可见」）。
    #[serde(rename = "notice")]
    Notice {
        /// 提示消息。
        message: String,
        /// 事件时间戳 (epoch ns)。
        event_ts: i64,
    },
}

impl ReplayEvent {
    /// 创建 NextValue 事件。
    pub fn next_value(event_ts: i64, processed_count: u64, progress: f64) -> Self {
        Self::NextValue {
            event_ts,
            processed_count,
            progress,
        }
    }

    /// 创建 StateChange 事件。
    pub fn state_change(from: &str, to: &str, event_ts: i64) -> Self {
        Self::StateChange {
            from: from.to_string(),
            to: to.to_string(),
            event_ts,
        }
    }

    /// 创建 Heartbeat 事件。
    pub fn heartbeat(event_ts: i64) -> Self {
        Self::Heartbeat { event_ts }
    }

    /// 创建 Completed 事件。
    pub fn completed(total_processed: u64, duration_ns: u64, event_ts: i64) -> Self {
        Self::Completed {
            total_processed,
            duration_ns,
            event_ts,
        }
    }

    /// 创建 Error 事件。
    pub fn error(message: impl Into<String>, event_ts: i64) -> Self {
        Self::Error {
            message: message.into(),
            event_ts,
        }
    }

    /// 创建 Notice（降级提示）事件。
    pub fn notice(message: impl Into<String>, event_ts: i64) -> Self {
        Self::Notice {
            message: message.into(),
            event_ts,
        }
    }

    /// 创建 Derived（派生标签结果）事件。
    pub fn derived(table_id: u32, value: f64, quality: u8, event_ts: i64) -> Self {
        Self::Derived {
            table_id,
            value,
            quality,
            event_ts,
        }
    }

    /// 创建 Alert（告警）事件。
    pub fn alert(
        rule_id: impl Into<String>,
        severity: u8,
        message: impl Into<String>,
        event_ts: i64,
    ) -> Self {
        Self::Alert {
            rule_id: rule_id.into(),
            severity,
            message: message.into(),
            event_ts,
        }
    }

    /// 返回事件时间戳。
    pub fn event_ts(&self) -> i64 {
        match self {
            Self::NextValue { event_ts, .. }
            | Self::Derived { event_ts, .. }
            | Self::Alert { event_ts, .. }
            | Self::StateChange { event_ts, .. }
            | Self::Heartbeat { event_ts }
            | Self::Completed { event_ts, .. }
            | Self::Notice { event_ts, .. }
            | Self::Error { event_ts, .. } => *event_ts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_event_serialization() {
        let event = ReplayEvent::next_value(1_000_000, 100, 0.5);
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("next_value"));
        assert!(json.contains("100"));
        assert!(json.contains("0.5"));
    }

    #[test]
    fn test_replay_event_deserialization() {
        let json = r#"{"type":"heartbeat","event_ts":2000000}"#;
        let event: ReplayEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_ts(), 2_000_000);
    }

    #[test]
    fn test_state_change() {
        let event = ReplayEvent::state_change("Playing", "Paused", 1000);
        match event {
            ReplayEvent::StateChange { from, to, .. } => {
                assert_eq!(from, "Playing");
                assert_eq!(to, "Paused");
            }
            _ => panic!("expected StateChange"),
        }
    }

    #[test]
    fn test_completed() {
        let event = ReplayEvent::completed(50000, 3_000_000_000, 1000);
        match event {
            ReplayEvent::Completed {
                total_processed,
                duration_ns,
                ..
            } => {
                assert_eq!(total_processed, 50000);
                assert_eq!(duration_ns, 3_000_000_000);
            }
            _ => panic!("expected Completed"),
        }
    }
}

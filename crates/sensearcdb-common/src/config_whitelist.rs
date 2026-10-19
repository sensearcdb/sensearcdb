//! systemConfig 白名单：扁平 kv → 强类型 `AppConfig` 字段的显式映射。
//!
//! 仅暴露「安全、可热加载、非路径、非密钥/证书、非连接串」的运行时调优参数。
//! 每个白名单字段在此**显式**写一个 getter + setter 映射项（禁止对整棵
//! `AppConfig` 做 serde 全量序列化），确保绝不泄露 `paths.*` / 密钥 / 证书 /
//! 连接串等敏感字段。
//!
//! 值统一以 `i64` 表示（前端 `SystemConfig = Record<string, string|number|boolean>`
//! 的 number 子集），`u64`/`usize` 字段在读写时显式转换。

use crate::config::AppConfig;

/// 白名单字段完整清单（点路径，前端契约 key）。
///
/// 全部字段均带 `🔄 热加载` 标注，运行时 PUT 后经 `ConfigHandle::swap` 原子生效。
pub const WHITELIST_KEYS: &[&str] = &[
    "alert.evaluation_interval_secs",
    "alert.max_concurrent_evaluations",
    "alert.max_rules",
    "alert.max_active_alerts",
    "alert.auto_resolve_timeout_secs",
    "alert.notification_cooldown_secs",
    "stream.max_concurrency",
    "stream.max_active_buckets",
    "stream.max_window_interval_ms",
    "collector.points_per_sec",
    "collector.bytes_per_point",
    "cq.tick_interval_ms",
    "cq.max_concurrency",
    "engine.freeze_threshold",
];

/// 读取单个白名单字段的当前值（`Some(i64)` = 合法白名单字段）。
pub fn whitelist_value(config: &AppConfig, key: &str) -> Option<i64> {
    match key {
        "alert.evaluation_interval_secs" => Some(config.alert.evaluation_interval_secs as i64),
        "alert.max_concurrent_evaluations" => Some(config.alert.max_concurrent_evaluations as i64),
        "alert.max_rules" => Some(config.alert.max_rules as i64),
        "alert.max_active_alerts" => Some(config.alert.max_active_alerts as i64),
        "alert.auto_resolve_timeout_secs" => Some(config.alert.auto_resolve_timeout_secs as i64),
        "alert.notification_cooldown_secs" => Some(config.alert.notification_cooldown_secs as i64),
        "stream.max_concurrency" => Some(config.stream.max_concurrency as i64),
        "stream.max_active_buckets" => Some(config.stream.max_active_buckets as i64),
        "stream.max_window_interval_ms" => Some(config.stream.max_window_interval_ms as i64),
        "collector.points_per_sec" => Some(config.collector.points_per_sec as i64),
        "collector.bytes_per_point" => Some(config.collector.bytes_per_point as i64),
        "cq.tick_interval_ms" => Some(config.cq.tick_interval_ms as i64),
        "cq.max_concurrency" => Some(config.cq.max_concurrency as i64),
        "engine.freeze_threshold" => Some(config.engine.freeze_threshold as i64),
        _ => None,
    }
}

/// 读取全部白名单字段，返回有序 `(key, value)` 列表（供 GET 序列化为扁平 map）。
pub fn whitelist_values(config: &AppConfig) -> Vec<(String, i64)> {
    WHITELIST_KEYS
        .iter()
        .filter_map(|key| whitelist_value(config, key).map(|v| ((*key).to_string(), v)))
        .collect()
}

/// 将单个白名单字段的 `i64` 值写回 `AppConfig`（类型校验后显式赋值）。
///
/// - `key` 不在白名单 → `Err`（非法 key）。
/// - `value < 0` → `Err`（这些参数均为非负计数/时长，负值无意义）。
pub fn apply_whitelist_value(config: &mut AppConfig, key: &str, value: i64) -> Result<(), String> {
    if value < 0 {
        return Err(format!(
            "whitelist value must be non-negative: {} = {}",
            key, value
        ));
    }

    match key {
        "alert.evaluation_interval_secs" => {
            config.alert.evaluation_interval_secs = value as u64;
        }
        "alert.max_concurrent_evaluations" => {
            config.alert.max_concurrent_evaluations = value as usize;
        }
        "alert.max_rules" => {
            config.alert.max_rules = value as usize;
        }
        "alert.max_active_alerts" => {
            config.alert.max_active_alerts = value as usize;
        }
        "alert.auto_resolve_timeout_secs" => {
            config.alert.auto_resolve_timeout_secs = value as u64;
        }
        "alert.notification_cooldown_secs" => {
            config.alert.notification_cooldown_secs = value as u64;
        }
        "stream.max_concurrency" => {
            config.stream.max_concurrency = value as usize;
        }
        "stream.max_active_buckets" => {
            config.stream.max_active_buckets = value as usize;
        }
        "stream.max_window_interval_ms" => {
            config.stream.max_window_interval_ms = value as u64;
        }
        "collector.points_per_sec" => {
            config.collector.points_per_sec = value as u64;
        }
        "collector.bytes_per_point" => {
            config.collector.bytes_per_point = value as u64;
        }
        "cq.tick_interval_ms" => {
            config.cq.tick_interval_ms = value as u64;
        }
        "cq.max_concurrency" => {
            config.cq.max_concurrency = value as usize;
        }
        "engine.freeze_threshold" => {
            config.engine.freeze_threshold = value as usize;
        }
        _ => return Err(format!("unknown system config key: {}", key)),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_whitelist_roundtrip() {
        let mut cfg = AppConfig::default();
        let before = cfg.alert.evaluation_interval_secs;

        apply_whitelist_value(&mut cfg, "alert.evaluation_interval_secs", 77).unwrap();
        assert_eq!(cfg.alert.evaluation_interval_secs, 77);
        assert_ne!(cfg.alert.evaluation_interval_secs, before);
        assert_eq!(
            whitelist_value(&cfg, "alert.evaluation_interval_secs"),
            Some(77)
        );
    }

    #[test]
    fn test_whitelist_rejects_unknown_key() {
        let mut cfg = AppConfig::default();
        assert!(apply_whitelist_value(&mut cfg, "paths.data_dir", 1).is_err());
        assert!(apply_whitelist_value(&mut cfg, "security.auth.jwt.signing_key_path", 1).is_err());
        assert!(whitelist_value(&cfg, "paths.data_dir").is_none());
    }

    #[test]
    fn test_whitelist_rejects_negative() {
        let mut cfg = AppConfig::default();
        assert!(apply_whitelist_value(&mut cfg, "alert.evaluation_interval_secs", -1).is_err());
    }

    #[test]
    fn test_whitelist_values_covers_all_keys() {
        let cfg = AppConfig::default();
        let values = whitelist_values(&cfg);
        assert_eq!(values.len(), WHITELIST_KEYS.len());
        for (key, _) in &values {
            assert!(WHITELIST_KEYS.contains(&key.as_str()));
        }
    }
}

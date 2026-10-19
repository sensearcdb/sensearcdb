//! 部署模块 REST API 端点。
//!
//! 提供部署状态查询、配置热加载、优雅关闭触发等管理接口。

use serde::{Deserialize, Serialize};

/// 部署状态响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployStatus {
    /// 服务版本
    pub version: String,
    /// 进程 PID
    pub pid: u32,
    /// 启动时间 (ISO 8601)
    pub start_time: String,
    /// 运行时长（秒）
    pub uptime_secs: u64,
    /// FORMAT_VERSION 状态
    pub format_version: FormatVersionStatus,
    /// 进程锁状态
    pub lock_held: bool,
    /// 是否正在关闭
    pub shutting_down: bool,
}

/// FORMAT_VERSION 状态。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatVersionStatus {
    /// WAL 格式版本
    pub wal_version: u32,
    /// SAC 格式版本
    pub sac_version: u32,
    /// 是否兼容
    pub compatible: bool,
}

/// 构建部署状态。
///
/// # 参数
///
/// - `pid`: 进程 PID
/// - `start_time`: 启动时间戳
/// - `uptime_secs`: 运行秒数
pub fn build_deploy_status(pid: u32, start_time: &str, uptime_secs: u64) -> DeployStatus {
    use sensearcdb_common::constants::{FORMAT_VERSION_SAC, FORMAT_VERSION_WAL};

    DeployStatus {
        version: env!("CARGO_PKG_VERSION").to_string(),
        pid,
        start_time: start_time.to_string(),
        uptime_secs,
        format_version: FormatVersionStatus {
            wal_version: FORMAT_VERSION_WAL,
            sac_version: FORMAT_VERSION_SAC,
            compatible: true,
        },
        lock_held: true,
        shutting_down: crate::graceful_shutdown::GracefulShutdown::is_shutting_down(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_deploy_status() {
        let status = build_deploy_status(12345, "2026-01-01T00:00:00Z", 3600);
        assert_eq!(status.pid, 12345);
        assert_eq!(status.uptime_secs, 3600);
        assert!(status.lock_held);
        assert!(!status.shutting_down);
    }

    #[test]
    fn test_deploy_status_serialization() {
        let status = build_deploy_status(1, "t0", 0);
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"pid\":1"));
    }
}

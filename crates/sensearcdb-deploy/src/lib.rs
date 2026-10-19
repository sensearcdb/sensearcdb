//! SenseArcDB 部署运维模块（二进制内嵌）。
//!
//! 提供启动安全检查链、进程锁、配置 6 级优先级加载与校验、
//! 首次运行初始化、优雅关闭序列、systemd/Windows Service 集成。
//!
//! ## 启动安全检查链（严格顺序）
//!
//! ① FORMAT_VERSION 检查（最高优先级——校验通过前禁止任何写入/删除/恢复）
//! ② 进程锁检查（跨Major FATAL / 跨Minor WARNING）
//! ③ 配置校验（端口/目录权限/集群一致性）
//! ④ 端口绑定
//!
//! ## 子模块
//!
//! - `bootstrap` — 启动安全检查链编排
//! - `format_version` — FORMAT_VERSION 文件读写 + 版本比对
//! - `lock` — 进程锁: flock/LockFileEx + 僵尸锁检测
//! - `config_loader` — 配置 6 级优先级加载
//! - `config_validator` — 配置校验
//! - `first_run` — 首次运行初始化
//! - `graceful_shutdown` — 优雅关闭序列
//! - `systemd_notify` — sd_notify READY=1 / STOPPING=1
//! - `cert_reload` — SIGUSR1 证书热加载
//! - `service` — systemd unit 模板 + Windows Service 控制
//! - `package` — 安装包目录结构定义

pub mod api;
pub mod bootstrap;
pub mod cert_reload;
pub mod config_loader;
pub mod config_validator;
pub mod first_run;
pub mod format_version;
pub mod graceful_shutdown;
pub mod lock;
pub mod package;
pub mod service;
pub mod systemd_notify;

pub use bootstrap::Bootstrap;
pub use config_loader::ConfigLoader;
pub use config_validator::ConfigValidator;
pub use first_run::FirstRun;
pub use format_version::{check_format_version, FormatCheckResult, FormatVersion};
pub use graceful_shutdown::GracefulShutdown;
pub use lock::ProcessLock;

use sensearcdb_common::config::AppConfig;

/// 部署引导器——承载启动安全检查链的产物（配置 + 首次运行 admin 密码）。
///
/// ## 使用示例
///
/// ```ignore
/// let (lock, deploy) = Bootstrap::run(&cli_args).await?;
/// // ... 使用 deploy.config 与 deploy.admin_password 启动 Engine, ObserveEngine ...
/// GracefulShutdown::execute(engine, observe, lock).await?;
/// ```
#[derive(Debug)]
pub struct DeployBootstrap {
    /// 已加载的应用配置
    pub config: AppConfig,
    /// 配置来源路径
    pub config_path: Option<std::path::PathBuf>,
    /// 首次运行解析/生成的 admin 初始密码（仅首次运行非 `None`，供 startup 幂等 seed）。
    pub admin_password: Option<String>,
}

impl DeployBootstrap {
    /// 创建新的 DeployBootstrap 实例。
    pub fn new(config: AppConfig, config_path: Option<std::path::PathBuf>) -> Self {
        Self {
            config,
            config_path,
            admin_password: None,
        }
    }

    /// 附带首次运行解析出的 admin 密码（由 `Bootstrap::run` 调用）。
    pub fn with_admin_password(mut self, admin_password: Option<String>) -> Self {
        self.admin_password = admin_password;
        self
    }
}

impl Default for DeployBootstrap {
    fn default() -> Self {
        Self::new(AppConfig::default(), None)
    }
}

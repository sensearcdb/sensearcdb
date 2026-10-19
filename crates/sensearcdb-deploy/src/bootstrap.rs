//! 启动安全检查链编排。
//!
//! ## 执行顺序（严格）
//!
//! ① FORMAT_VERSION 检查
//! ② 进程锁
//! ③ 配置校验
//! ④ 端口绑定（由 Engine 完成）
//!
//! ⚠️ 步骤①校验通过前，禁止对 WAL/SAC/config.db 执行任何写入/删除/恢复操作。

use crate::config_loader::ConfigLoader;
use crate::config_validator::ConfigValidator;
use crate::first_run::FirstRun;
use crate::format_version::check_format_version;
use crate::lock::ProcessLock;
use crate::DeployBootstrap;
use sensearcdb_common::config::AppConfig;
use sensearcdb_common::SenseArcError;
use std::path::{Path, PathBuf};

/// Bootstrap 类型别名，兼容旧模块结构。
#[derive(Debug)]
pub struct Bootstrap;

impl Bootstrap {
    /// 仅加载配置（6 级优先级），不执行任何写盘/锁/校验动作。
    ///
    /// 拆分此步骤是为了让调用方能在「配置加载之后、FORMAT_VERSION 检查 / 进程锁 /
    /// 首次运行初始化之前」正确初始化日志系统（D3：`observe.log_output/log_file` 生效），
    /// 从而保证首次运行打印的 admin 初始密码等关键日志不丢失。
    pub fn load_config<A>(args: &A) -> Result<(AppConfig, Option<PathBuf>), SenseArcError>
    where
        A: BootstrapArgs,
    {
        ConfigLoader::load(args)
    }

    /// 执行启动安全检查链。
    ///
    /// 返回 `(ProcessLock, DeployBootstrap)`。
    pub async fn run<A>(args: &A) -> Result<(ProcessLock, DeployBootstrap), SenseArcError>
    where
        A: BootstrapArgs,
    {
        // 加载配置（6 级优先级）
        let (config, config_path) = ConfigLoader::load(args)?;
        Self::run_with_config(config, config_path).await
    }

    /// 基于已加载的配置执行安全检查链（FORMAT_VERSION → 进程锁 → 校验 → 首次运行初始化）。
    pub async fn run_with_config(
        config: AppConfig,
        config_path: Option<PathBuf>,
    ) -> Result<(ProcessLock, DeployBootstrap), SenseArcError> {
        // ① FORMAT_VERSION 检查
        let data_dir = Path::new(&config.paths.data_dir);
        let format_result = check_format_version(data_dir)?;

        let is_first_run = matches!(
            format_result,
            crate::format_version::FormatCheckResult::FirstRun
        );

        // ② 进程锁
        let lock = ProcessLock::acquire(data_dir)?;

        // ③ 配置校验
        ConfigValidator::validate(&config)?;

        // 首次运行初始化（返回解析/生成的 admin 初始密码，供 startup 幂等 seed）。
        let admin_password = if is_first_run {
            Some(FirstRun::initialize(&config).await?)
        } else {
            None
        };

        tracing::info!("启动安全检查链全部通过 ✓");
        Ok((
            lock,
            DeployBootstrap::new(config, config_path).with_admin_password(admin_password),
        ))
    }
}

/// 启动参数 trait。
///
/// 实现此 trait 的类型可用于 `Bootstrap::run()`。
pub trait BootstrapArgs {
    /// 返回 --config 指定的配置文件路径。
    fn config_path(&self) -> Option<&str>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use sensearcdb_common::config::AppConfig;

    struct TestArgs;
    impl BootstrapArgs for TestArgs {
        fn config_path(&self) -> Option<&str> {
            None
        }
    }

    #[tokio::test]
    async fn test_bootstrap_basic() {
        // 仅验证 Bootstrap 结构体能创建
        let _b = Bootstrap;
    }
}

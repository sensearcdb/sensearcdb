//! 配置 6 级优先级加载 + 环境变量覆盖。
//!
//! ## 优先级（高 → 低）
//!
//! 1. `--config /path/to/custom.toml` (CLI 参数)
//! 2. `SENSEARCDB__<SECTION>__<KEY>=<VALUE>` 环境变量（字段级覆盖）
//! 3. `SENSEARCDB_CONFIG` 环境变量指向的配置文件
//! 4. `./sensearcdb.toml` (当前工作目录)
//! 5. `~/.sensearcdb/config.toml` (用户目录)
//! 6. `/etc/sensearcdb/config.toml` (系统级, Linux) / `C:\ProgramData\SenseArcDB\config.toml` (Windows)
//! 7. 内置默认值 (`AppConfig::default()`)
//!
//! ## 字段级深度合并（GAP6-DEP-03）
//!
//! 通过 `config` crate 的递归 Map 合并实现：低优先级层级的未指定字段
//! 保留原值，只有高优先级层级**显式出现**的字段才覆盖。不再使用
//! 「section 级整体替换」的旧逻辑。

use crate::bootstrap::BootstrapArgs;
use sensearcdb_common::config::AppConfig;
use sensearcdb_common::SenseArcError;
use std::path::PathBuf;

/// 配置加载器——按优先级加载配置（字段级深度合并）。
#[derive(Debug)]
pub struct ConfigLoader;

impl ConfigLoader {
    /// 按优先级加载配置。
    ///
    /// 返回 `(AppConfig, 配置来源路径)`。
    /// 配置来源路径为 `Some` 表示从文件加载，`None` 表示仅使用默认值。
    pub fn load<A: BootstrapArgs>(args: &A) -> Result<(AppConfig, Option<PathBuf>), SenseArcError> {
        let mut source_path: Option<PathBuf> = None;

        // 第 7 层（最低）：内置默认值
        let mut builder = config::Config::builder().add_source(
            config::Config::try_from(&AppConfig::default())
                .map_err(|e| SenseArcError::Internal(format!("serialize default config: {}", e)))?,
        );

        // 第 6 层：系统级配置
        let system_path = Self::system_config_path();
        if system_path.exists() {
            builder = builder.add_source(config::File::from(system_path.clone()).required(false));
            source_path = Some(system_path.clone());
            tracing::debug!("加载系统配置: {}", system_path.display());
        }

        // 第 5 层：用户级配置
        if let Some(user_path) = Self::user_config_path() {
            if user_path.exists() {
                builder = builder.add_source(config::File::from(user_path.clone()).required(false));
                source_path = Some(user_path.clone());
                tracing::debug!("加载用户配置: {}", user_path.display());
            }
        }

        // 第 4 层：当前目录配置
        let cwd_path = PathBuf::from("./sensearcdb.toml");
        if cwd_path.exists() {
            builder = builder.add_source(config::File::from(cwd_path.clone()).required(false));
            source_path = Some(cwd_path.clone());
            tracing::debug!("加载当前目录配置: {}", cwd_path.display());
        }

        // 第 3 层：SENSEARCDB_CONFIG 指定的配置文件
        if let Ok(env_path) = std::env::var("SENSEARCDB_CONFIG") {
            let env_config_path = PathBuf::from(&env_path);
            if env_config_path.exists() {
                builder =
                    builder.add_source(config::File::from(env_config_path.clone()).required(false));
                source_path = Some(env_config_path.clone());
                tracing::debug!(
                    "加载 SENSEARCDB_CONFIG 指定配置: {}",
                    env_config_path.display()
                );
            }
        }

        // 第 2 层：--config CLI 参数
        if let Some(cli_path) = args.config_path() {
            let cli_config_path = PathBuf::from(cli_path);
            if cli_config_path.exists() {
                builder =
                    builder.add_source(config::File::from(cli_config_path.clone()).required(false));
                source_path = Some(cli_config_path.clone());
                tracing::debug!("加载 --config 指定配置: {}", cli_config_path.display());
            }
        }

        // 第 1 层（最高）：环境变量字段级覆盖
        // 格式: `SENSEARCDB__ENGINE__FREEZE_THRESHOLD=500000`
        // （prefix "SENSEARCDB" + 字段分隔 "__"，与前缀分隔符保持兼容）
        builder = builder.add_source(
            config::Environment::with_prefix("SENSEARCDB")
                .separator("__")
                .try_parsing(true),
        );

        let built = builder
            .build()
            .map_err(|e| SenseArcError::Internal(format!("配置构建失败: {}", e)))?;

        let app_config: AppConfig = built
            .try_deserialize()
            .map_err(|e| SenseArcError::Internal(format!("配置反序列化失败: {}", e)))?;

        Ok((app_config, source_path))
    }

    /// 返回系统级配置路径。
    #[cfg(target_os = "linux")]
    fn system_config_path() -> PathBuf {
        PathBuf::from("/etc/sensearcdb/config.toml")
    }

    #[cfg(target_os = "macos")]
    fn system_config_path() -> PathBuf {
        PathBuf::from("/usr/local/etc/sensearcdb/config.toml")
    }

    #[cfg(windows)]
    fn system_config_path() -> PathBuf {
        PathBuf::from(r"C:\ProgramData\SenseArcDB\config.toml")
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    fn system_config_path() -> PathBuf {
        PathBuf::from("/etc/sensearcdb/config.toml")
    }

    /// 返回用户级配置路径。
    fn user_config_path() -> Option<PathBuf> {
        dirs_next::home_dir().map(|home| home.join(".sensearcdb").join("config.toml"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct TestArgs {
        config: Option<String>,
    }
    impl BootstrapArgs for TestArgs {
        fn config_path(&self) -> Option<&str> {
            self.config.as_deref()
        }
    }

    #[test]
    fn test_load_defaults_only() {
        let args = TestArgs { config: None };
        // 系统/用户配置文件若存在会额外加载，但不应 panic
        let result = ConfigLoader::load(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_with_cli_config() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("custom.toml");
        std::fs::write(&config_path, "[paths]\ndata_dir = \"/custom/data\"\n").unwrap();

        let args = TestArgs {
            config: Some(config_path.to_string_lossy().to_string()),
        };
        let (config, source) = ConfigLoader::load(&args).unwrap();
        assert!(source.is_some());
        // 验证自定义路径被加载
        assert_eq!(config.paths.data_dir, "/custom/data");
    }

    #[test]
    fn test_field_level_deep_merge() {
        // 字段级合并：CLI 文件只覆盖 data_dir，不应把其他 paths 字段重置为默认。
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("custom.toml");
        std::fs::write(&config_path, "[paths]\ndata_dir = \"/custom/data\"\n").unwrap();

        let args = TestArgs {
            config: Some(config_path.to_string_lossy().to_string()),
        };
        let (config, _) = ConfigLoader::load(&args).unwrap();

        // data_dir 被覆盖
        assert_eq!(config.paths.data_dir, "/custom/data");
        // 其他字段保留默认值（非 section 级整体替换）
        assert!(!config.paths.sac_dir_name.is_empty());
        assert!(!config.paths.wal_dir_name.is_empty());
    }

    #[test]
    fn test_priority_cli_over_cwd() {
        let dir = TempDir::new().unwrap();
        let cli_config = dir.path().join("cli.toml");
        std::fs::write(&cli_config, "[paths]\ndata_dir = \"/cli/data\"\n").unwrap();

        let args = TestArgs {
            config: Some(cli_config.to_string_lossy().to_string()),
        };
        let (config, _) = ConfigLoader::load(&args).unwrap();
        assert_eq!(config.paths.data_dir, "/cli/data");
    }
}

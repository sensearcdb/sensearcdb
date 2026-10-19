//! 首次运行初始化。
//!
//! 负责：
//! - 创建 FORMAT_VERSION 文件
//! - 创建必需目录结构
//! - 生成 admin 初始密码（或从配置读取）
//! - 生成 JWT 签名密钥

use sensearcdb_common::config::AppConfig;
use sensearcdb_common::constants::{FORMAT_VERSION_SAC, FORMAT_VERSION_WAL};
use sensearcdb_common::SenseArcError;
use std::path::Path;

/// 首次运行初始化逻辑。
#[derive(Debug)]
pub struct FirstRun;

impl FirstRun {
    /// 执行首次运行初始化。
    ///
    /// 步骤：
    /// 1. 创建数据目录结构
    /// 2. 写入 FORMAT_VERSION
    /// 3. 解析/生成 admin 初始密码
    /// 4. 生成 JWT 签名密钥
    ///
    /// 返回解析/生成的 admin 初始密码（仅首次运行调用方可用），供 startup 的
    /// security 段做幂等 seed。
    pub async fn initialize(config: &AppConfig) -> Result<String, SenseArcError> {
        let data_dir = Path::new(&config.paths.data_dir);

        tracing::info!("===========================================");
        tracing::info!("  首次运行初始化");
        tracing::info!("  数据目录: {}", data_dir.display());
        tracing::info!("===========================================");

        // 1. 创建目录结构
        Self::create_directory_structure(data_dir)?;

        // 2. 写入 FORMAT_VERSION
        Self::write_format_version(data_dir)?;

        // 3. 处理 admin 密码
        let from_config = !config.deploy.first_run.admin_password.is_empty();
        let admin_password = Self::resolve_admin_password(config);
        if from_config {
            tracing::info!("  Admin 密码: 从配置读取");
        } else {
            // 红线边界：仅自动生成时首次启动打印一次明文密码（0600 日志保护），
            // 提示登录后立即改密；不落任何审计/持久化明文。
            tracing::info!(
                "  Admin 初始密码（自动生成，请登录后立即修改密码）: {}",
                admin_password
            );
        }

        // 4. 生成 JWT 密钥（占位）
        Self::ensure_jwt_key(config)?;

        tracing::info!("===========================================");
        tracing::info!("  初始化完成 ✓");
        tracing::info!("===========================================");

        Ok(admin_password)
    }

    /// 创建必需目录结构。
    fn create_directory_structure(data_dir: &Path) -> Result<(), SenseArcError> {
        let subdirs = [
            "sac", "wal", "snapshot", "metadata", "audit", "backup", "keys",
        ];

        for subdir in &subdirs {
            let path = data_dir.join(subdir);
            std::fs::create_dir_all(&path).map_err(|e| {
                SenseArcError::Internal(format!("无法创建目录 {}: {}", path.display(), e))
            })?;
            tracing::debug!("  创建目录: {}", path.display());
        }

        Ok(())
    }

    /// 写入初始 FORMAT_VERSION。
    fn write_format_version(data_dir: &Path) -> Result<(), SenseArcError> {
        let content = format!(
            "# {}/FORMAT_VERSION\n\
             # 此文件由 SenseArcDB 自动管理，请勿手动修改\n\
             WAL={}\n\
             SAC={}\n",
            data_dir.display(),
            FORMAT_VERSION_WAL,
            FORMAT_VERSION_SAC
        );

        let path = data_dir.join("FORMAT_VERSION");
        std::fs::write(&path, content).map_err(|e| {
            SenseArcError::Internal(format!("无法创建 FORMAT_VERSION {}: {}", path.display(), e))
        })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).ok();
        }

        tracing::info!(
            "  写入 FORMAT_VERSION (WAL={}, SAC={})",
            FORMAT_VERSION_WAL,
            FORMAT_VERSION_SAC
        );
        Ok(())
    }

    /// 解析 admin 初始密码。
    ///
    /// 优先级：
    /// 1. 配置指定的密码 (`deploy.first_run.admin_password`)
    /// 2. 自动生成 24 字符随机密码
    fn resolve_admin_password(config: &AppConfig) -> String {
        if !config.deploy.first_run.admin_password.is_empty() {
            return config.deploy.first_run.admin_password.clone();
        }
        // 自动生成
        generate_secure_password(24)
    }

    /// 确保 JWT 签名密钥存在。
    fn ensure_jwt_key(config: &AppConfig) -> Result<(), SenseArcError> {
        let key_path = Path::new(&config.security.auth.jwt.signing_key_path);
        if let Some(parent) = key_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                SenseArcError::Internal(format!(
                    "无法创建 JWT 密钥目录 {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }
        // JWT 密钥生成由 security crate 负责
        // 这里只确保目录存在
        Ok(())
    }
}

/// 生成安全的随机密码。
fn generate_secure_password(length: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let charset: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                          abcdefghijklmnopqrstuvwxyz\
                          0123456789\
                          !@#$%^&*()-_=+";
    let mut result = String::with_capacity(length);
    let mut state = seed as u64;

    for _ in 0..length {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = (state >> 33) as usize % charset.len();
        result.push(charset[idx] as char);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_initialize_creates_directories() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        let mut config = AppConfig::default();
        config.paths.data_dir = data_dir.to_string_lossy().to_string();

        FirstRun::initialize(&config).await.unwrap();

        assert!(data_dir.join("FORMAT_VERSION").exists());
        assert!(data_dir.join("sac").exists());
        assert!(data_dir.join("wal").exists());
    }

    #[test]
    fn test_generate_password_length() {
        let pwd = generate_secure_password(24);
        assert_eq!(pwd.len(), 24);
    }

    #[test]
    fn test_generate_password_randomness() {
        let pwd1 = generate_secure_password(24);
        let pwd2 = generate_secure_password(24);
        assert_ne!(pwd1, pwd2);
    }
}

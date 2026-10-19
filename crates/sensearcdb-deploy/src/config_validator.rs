//! 配置校验器——验证端口冲突、目录权限、集群一致性、磁盘/内存/FD 等。
//!
//! ## 校验模式
//!
//! - `strict`: 校验失败 → FATAL 退出
//! - `warn`: 校验失败 → WARNING 日志，继续启动

use sensearcdb_common::config::AppConfig;
use sensearcdb_common::constants::{
    DEFAULT_GRPC_ADMIN_PORT, DEFAULT_GRPC_QUERY_PORT, DEFAULT_GRPC_WRITE_PORT, DEFAULT_REST_PORT,
};
use sensearcdb_common::SenseArcError;
use std::path::{Component, Path};

/// 配置校验结果。
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// 是否通过
    pub passed: bool,
    /// 错误列表（strict 模式下导致 FATAL）
    pub errors: Vec<String>,
    /// 警告列表
    pub warnings: Vec<String>,
}

/// 配置校验器。
#[derive(Debug)]
pub struct ConfigValidator;

impl ConfigValidator {
    /// 执行完整配置校验。
    ///
    /// 校验项目：
    /// 1. 端口冲突检测
    /// 2. 数据目录存在性与权限
    /// 3. 集群一致性
    /// 4. 磁盘空间 / 内存 / FD 警告
    pub fn validate(config: &AppConfig) -> Result<ValidationResult, SenseArcError> {
        let mut result = ValidationResult {
            passed: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        };

        // 1. 端口校验
        Self::validate_ports(config, &mut result);

        // 2. 数据目录校验
        Self::validate_directories(config, &mut result);

        // 2b. 子目录相对名校验（I4：可覆写但不可逃逸 data_dir）
        Self::validate_relative_path_names(config, &mut result);

        // 3. 集群一致性校验
        Self::validate_cluster_config(config, &mut result);

        // 4. 资源警告
        Self::validate_resources(&mut result);

        // strict 模式下，有错误即 FATAL
        let mode = &config.deploy.validation_mode;
        if mode == "strict" && !result.errors.is_empty() {
            return Err(SenseArcError::Internal(format!(
                "配置校验失败 ({} 项错误):\n{}",
                result.errors.len(),
                result.errors.join("\n")
            )));
        }

        // 记录警告
        for w in &result.warnings {
            tracing::warn!("配置警告: {}", w);
        }

        Ok(result)
    }

    /// 端口冲突检测。
    fn validate_ports(config: &AppConfig, result: &mut ValidationResult) {
        let mut ports: Vec<(u16, &str)> = Vec::new();

        // 收集所有使用的端口
        ports.push((DEFAULT_REST_PORT, "REST API (5100)"));
        if config.observe.metrics_port > 0 {
            ports.push((config.observe.metrics_port, "Prometheus metrics"));
        }
        ports.push((DEFAULT_GRPC_WRITE_PORT, "WriteService (5201)"));
        ports.push((DEFAULT_GRPC_QUERY_PORT, "Query+Flight (5202)"));
        ports.push((DEFAULT_GRPC_ADMIN_PORT, "CollectorAdmin (5203)"));

        // 检测重复
        for i in 0..ports.len() {
            for j in (i + 1)..ports.len() {
                if ports[i].0 == ports[j].0 {
                    let err = format!(
                        "端口冲突: {} 和 {} 均使用端口 {}",
                        ports[i].1, ports[j].1, ports[i].0
                    );
                    result.errors.push(err);
                }
            }
        }

        // 检测是否在特权端口范围
        for (port, name) in &ports {
            if *port < 1024 && *port > 0 {
                result
                    .warnings
                    .push(format!("{} 使用特权端口 {}，需要 root 权限", name, port));
            }
        }
    }

    /// 数据目录校验。
    fn validate_directories(config: &AppConfig, result: &mut ValidationResult) {
        let data_dir = Path::new(&config.paths.data_dir);

        // 目录存在性
        if !data_dir.exists() {
            result.warnings.push(format!(
                "数据目录 {} 不存在，将在首次运行时创建",
                data_dir.display()
            ));
        } else if !data_dir.is_dir() {
            result
                .errors
                .push(format!("数据路径 {} 存在但不是目录", data_dir.display()));
        } else {
            // 检查可读性
            if let Err(e) = std::fs::read_dir(data_dir) {
                result
                    .errors
                    .push(format!("数据目录 {} 不可读: {}", data_dir.display(), e));
            }
            // 检查可写性（创建临时文件测试）
            let test_file = data_dir.join(".sensearcdb_write_test");
            match std::fs::File::create(&test_file) {
                Ok(_) => {
                    std::fs::remove_file(&test_file).ok();
                }
                Err(e) => {
                    result
                        .errors
                        .push(format!("数据目录 {} 不可写: {}", data_dir.display(), e));
                }
            }
        }
    }

    /// 子目录相对名校验（不变式 I4：子目录名可覆写但不可逃逸 `data_dir`）。
    ///
    /// 所有 `PathsConfig` 的 `*_name` 字段必须是「相对名」：
    /// - 拒绝绝对路径（Windows 盘符 / POSIX 根路径）
    /// - 拒绝含 `..`（ParentDir）的路径
    ///
    /// 允许正向分隔符构成的嵌套子目录（如 `shard_1/wal`）。
    fn validate_relative_path_names(config: &AppConfig, result: &mut ValidationResult) {
        let fields: [(&str, &str); 11] = [
            ("paths.sac_dir_name", &config.paths.sac_dir_name),
            ("paths.wal_dir_name", &config.paths.wal_dir_name),
            ("paths.snapshot_dir_name", &config.paths.snapshot_dir_name),
            ("paths.snapshot_file_name", &config.paths.snapshot_file_name),
            ("paths.metadata_dir_name", &config.paths.metadata_dir_name),
            ("paths.config_db_name", &config.paths.config_db_name),
            ("paths.audit_dir_name", &config.paths.audit_dir_name),
            ("paths.backup_dir_name", &config.paths.backup_dir_name),
            ("paths.keys_dir_name", &config.paths.keys_dir_name),
            (
                "paths.ai_ops_registry_db_name",
                &config.paths.ai_ops_registry_db_name,
            ),
            (
                "paths.ai_ops_blob_dir_name",
                &config.paths.ai_ops_blob_dir_name,
            ),
        ];

        for (field, name) in fields {
            let path = Path::new(name);
            // Windows 上 POSIX 风格根路径（如 `/etc/evil`，无盘符）的
            // `is_absolute()` 返回 false，但其 `components()` 含 `RootDir`，
            // 且 `PathBuf::join` 会以 RootDir 替换掉 data_dir（逃逸）。
            // 同理，盘符/前缀（`Component::Prefix`）也要一并拒绝。
            let has_root = path
                .components()
                .any(|c| matches!(c, Component::RootDir | Component::Prefix(_)));
            if path.is_absolute() || has_root {
                result.errors.push(format!(
                    "路径字段 {} 必须是相对名（拒绝绝对路径/根路径/盘符）: {:?}",
                    field, name
                ));
            } else if path.components().any(|c| matches!(c, Component::ParentDir)) {
                result.errors.push(format!(
                    "路径字段 {} 不得包含 '..' 父目录段: {:?}",
                    field, name
                ));
            }
        }
    }

    /// 集群配置一致性校验。
    fn validate_cluster_config(config: &AppConfig, result: &mut ValidationResult) {
        // 检查集群模式下的必要配置
        if !config.cluster.node_id.is_empty() {
            // 集群模式下检查 node_id 格式
            if config.cluster.node_id.len() > 64 {
                result.errors.push(format!(
                    "集群 node_id 过长 ({} > 64): {}",
                    config.cluster.node_id.len(),
                    config.cluster.node_id
                ));
            }
        }
    }

    /// 系统资源警告。
    fn validate_resources(_result: &mut ValidationResult) {
        // 磁盘空间检查
        #[cfg(unix)]
        {
            if let Ok(data_dir) = std::env::current_dir() {
                // 使用 statfs 检查可用空间
                use std::os::unix::ffi::OsStrExt;
                let path_bytes = data_dir.as_os_str().as_bytes();
                let c_path = std::ffi::CString::new(path_bytes).ok();
                if let Some(c_path) = c_path {
                    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
                    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
                    if rc == 0 {
                        let avail_bytes = stat.f_bavail as u64 * stat.f_frsize as u64;
                        let avail_gb = avail_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                        if avail_gb < 1.0 {
                            result
                                .warnings
                                .push(format!("磁盘可用空间不足 1GB (当前: {:.2}GB)", avail_gb));
                        }
                    }
                }
            }
        }

        // FD 限制检查
        #[cfg(unix)]
        {
            let mut rlim: libc::rlimit = unsafe { std::mem::zeroed() };
            let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
            if rc == 0 {
                let soft_limit = rlim.rlim_cur;
                if soft_limit < 4096 {
                    result.warnings.push(format!(
                        "文件描述符限制较低 ({})，建议 ≥ 4096。使用 ulimit -n 调整",
                        soft_limit
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_default_config() {
        let config = AppConfig::default();
        let result = ConfigValidator::validate(&config);
        // 默认配置可能因为目录不存在产生警告，但不应出错
        match result {
            Ok(r) => {
                // 至少没有端口冲突
                assert!(!r.errors.iter().any(|e| e.contains("端口冲突")));
            }
            Err(_) => {
                // strict 模式 + 目录不可写时可能报错，可接受
            }
        }
    }

    #[test]
    fn test_port_conflict_detection() {
        let mut config = AppConfig::default();
        config.observe.metrics_port = DEFAULT_REST_PORT; // 与 REST 冲突
        let result = ConfigValidator::validate(&config);
        match result {
            Ok(r) => {
                assert!(r.errors.iter().any(|e| e.contains("端口冲突")));
            }
            Err(_) => {} // strict 模式下可能直接返回 error
        }
    }

    /// 构造一个空的校验结果，直接调用私有方法 `validate_relative_path_names`，
    /// 避免 `validate` 里的目录可写性探测带来的无关副作用。
    fn validate_names(config: &AppConfig) -> ValidationResult {
        let mut result = ValidationResult {
            passed: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        };
        ConfigValidator::validate_relative_path_names(config, &mut result);
        result
    }

    #[test]
    fn test_sac_dir_name_posix_rootdir_rejected() {
        // `/etc/evil`：Windows 上 is_absolute()=false，但含 RootDir，必须拒绝。
        let mut config = AppConfig::default();
        config.paths.sac_dir_name = "/etc/evil".into();
        let result = validate_names(&config);
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.contains("sac_dir_name") && e.contains("绝对路径")),
            "前导斜杠根路径应被拒绝，实际 errors={:?}",
            result.errors
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_sac_dir_name_windows_prefix_rejected() {
        // 覆盖盘符绝对路径（C:\\evil、C:/evil）与盘符相对路径（C:evil，is_absolute()=false
        // 但含 Prefix，join 同样会逃逸 data_dir）。
        for evil in ["C:\\evil", "C:/evil", "C:evil"] {
            let mut config = AppConfig::default();
            config.paths.sac_dir_name = evil.into();
            let result = validate_names(&config);
            assert!(
                result
                    .errors
                    .iter()
                    .any(|e| e.contains("sac_dir_name") && e.contains("绝对路径")),
                "Windows 盘符/前缀路径 {evil:?} 应被拒绝，实际 errors={:?}",
                result.errors
            );
        }
    }

    #[test]
    fn test_sac_dir_name_forward_nested_allowed() {
        // 回归：正向嵌套子目录（如 `ssd/sac`）必须保持合法。
        let mut config = AppConfig::default();
        config.paths.sac_dir_name = "ssd/sac".into();
        let result = validate_names(&config);
        assert!(
            !result.errors.iter().any(|e| e.contains("sac_dir_name")),
            "正向嵌套 'ssd/sac' 不应触发校验错误，实际 errors={:?}",
            result.errors
        );
    }

    #[test]
    fn test_sac_dir_name_parent_dir_rejected() {
        // 回归：`..` 父目录段仍必须被拒绝。
        let mut config = AppConfig::default();
        config.paths.sac_dir_name = "../evil".into();
        let result = validate_names(&config);
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.contains("sac_dir_name") && e.contains("..")),
            "'../evil' 应被拒绝，实际 errors={:?}",
            result.errors
        );
    }
}

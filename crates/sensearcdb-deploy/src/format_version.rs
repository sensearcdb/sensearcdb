//! FORMAT_VERSION 文件读写 + 版本比对。
//!
//! ## 文件格式
//!
//! ```text
//! # {data_dir}/FORMAT_VERSION
//! # 此文件由 SenseArcDB 自动管理，请勿手动修改
//! WAL=2
//! SAC=2
//! ```
//!
//! ## 安全约束
//!
//! - FORMAT_VERSION 检查是启动安全检查链的**第一步**（最高优先级）
//! - 校验通过前，**禁止**对 WAL/SAC/config.db 执行任何写入/删除/恢复操作
//! - 仅允许只读 open + read + close

use sensearcdb_common::constants::{FORMAT_VERSION_SAC, FORMAT_VERSION_WAL};
use sensearcdb_common::SenseArcError;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// FORMAT_VERSION 文件中的版本信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatVersion {
    /// WAL 格式版本号
    pub wal_version: u32,
    /// SAC 格式版本号
    pub sac_version: u32,
}

/// 启动时 FORMAT_VERSION 检查结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatCheckResult {
    /// 首次启动——FORMAT_VERSION 文件不存在
    FirstRun,
    /// 版本匹配——正常启动
    Compatible,
    /// 升级场景——WAL/SAC 版本 < 内置版本
    UpgradeNeeded {
        /// 旧的 WAL 版本号
        old_wal: u32,
        /// 旧的 SAC 版本号
        old_sac: u32,
    },
    /// 不兼容——WAL/SAC 版本 > 内置版本
    Incompatible {
        /// 不兼容原因描述
        reason: String,
    },
}

impl FormatVersion {
    /// 创建当前内置版本对应的 FormatVersion。
    pub fn current() -> Self {
        Self {
            wal_version: FORMAT_VERSION_WAL,
            sac_version: FORMAT_VERSION_SAC,
        }
    }

    /// 读取 `{data_dir}/FORMAT_VERSION`。
    ///
    /// ⚠️ 仅允许只读 open + read + close——禁止任何写入。
    /// 返回 `Ok(None)` 表示文件不存在（首次启动）。
    pub fn read(data_dir: &Path) -> Result<Option<Self>, SenseArcError> {
        let path = format_version_path(data_dir);

        if !path.exists() {
            return Ok(None);
        }

        let file = fs::File::open(&path).map_err(|e| {
            SenseArcError::Internal(format!(
                "无法打开 FORMAT_VERSION 文件 {}: {}",
                path.display(),
                e
            ))
        })?;

        let reader = BufReader::new(file);
        let mut wal_version: Option<u32> = None;
        let mut sac_version: Option<u32> = None;

        for line in reader.lines() {
            let line = line
                .map_err(|e| SenseArcError::Internal(format!("读取 FORMAT_VERSION 失败: {}", e)))?;
            let trimmed = line.trim();

            // 跳过注释和空行
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            if let Some(value) = trimmed.strip_prefix("WAL=") {
                wal_version = Some(value.trim().parse::<u32>().map_err(|_| {
                    SenseArcError::Internal(format!("FORMAT_VERSION 中 WAL 版本无效: {}", value))
                })?);
            } else if let Some(value) = trimmed.strip_prefix("SAC=") {
                sac_version = Some(value.trim().parse::<u32>().map_err(|_| {
                    SenseArcError::Internal(format!("FORMAT_VERSION 中 SAC 版本无效: {}", value))
                })?);
            }
        }

        match (wal_version, sac_version) {
            (Some(w), Some(s)) => Ok(Some(Self {
                wal_version: w,
                sac_version: s,
            })),
            _ => Err(SenseArcError::Internal(
                "FORMAT_VERSION 文件格式无效：缺少 WAL= 或 SAC= 行".into(),
            )),
        }
    }

    /// 原子写入 FORMAT_VERSION 文件。
    ///
    /// 先写入临时文件，fsync 后原子 rename，确保不会出现损坏的 FORMAT_VERSION。
    pub fn atomic_write(&self, data_dir: &Path) -> Result<(), SenseArcError> {
        let path = format_version_path(data_dir);
        let tmp_path = path.with_extension("tmp");

        // 确保父目录存在
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                SenseArcError::Internal(format!("无法创建目录 {}: {}", parent.display(), e))
            })?;
        }

        let content = format!(
            "# {}\n\
             # 此文件由 SenseArcDB 自动管理，请勿手动修改\n\
             WAL={}\n\
             SAC={}\n",
            path.display(),
            self.wal_version,
            self.sac_version
        );

        // 写入临时文件
        {
            let mut file = fs::File::create(&tmp_path).map_err(|e| {
                SenseArcError::Internal(format!(
                    "无法创建临时 FORMAT_VERSION {}: {}",
                    tmp_path.display(),
                    e
                ))
            })?;
            file.write_all(content.as_bytes())
                .map_err(|e| SenseArcError::Internal(format!("写入 FORMAT_VERSION 失败: {}", e)))?;
            file.flush().map_err(|e| {
                SenseArcError::Internal(format!("flush FORMAT_VERSION 失败: {}", e))
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(fs::Permissions::from_mode(0o644)).ok();
            }
        }

        // 原子 rename
        fs::rename(&tmp_path, &path).map_err(|e| {
            SenseArcError::Internal(format!(
                "rename FORMAT_VERSION {} -> {} 失败: {}",
                tmp_path.display(),
                path.display(),
                e
            ))
        })?;

        tracing::info!(
            "FORMAT_VERSION 写入完成: WAL={}, SAC={}",
            self.wal_version,
            self.sac_version
        );
        Ok(())
    }
}

/// 返回 FORMAT_VERSION 文件路径。
pub fn format_version_path(data_dir: &Path) -> PathBuf {
    data_dir.join("FORMAT_VERSION")
}

/// 启动时检查 FORMAT_VERSION 兼容性。
///
/// # 检查逻辑
///
/// - 文件不存在 → `FirstRun`
/// - WAL_version == 内置 && SAC_version == 内置 → `Compatible`
/// - WAL_version < 内置 || SAC_version < 内置 → `UpgradeNeeded`
/// - WAL_version > 内置 || SAC_version > 内置 → `Incompatible`
///
/// ⚠️ 此函数在校验通过前不执行任何写入操作。
pub fn check_format_version(data_dir: &Path) -> Result<FormatCheckResult, SenseArcError> {
    let _builtin = FormatVersion::current();

    match FormatVersion::read(data_dir)? {
        None => Ok(FormatCheckResult::FirstRun),
        Some(fv) => {
            let wal_ok = fv.wal_version == FORMAT_VERSION_WAL;
            let sac_ok = fv.sac_version == FORMAT_VERSION_SAC;

            if wal_ok && sac_ok {
                Ok(FormatCheckResult::Compatible)
            } else if fv.wal_version > FORMAT_VERSION_WAL || fv.sac_version > FORMAT_VERSION_SAC {
                let reasons: Vec<String> = {
                    let mut v = Vec::new();
                    if fv.wal_version > FORMAT_VERSION_WAL {
                        v.push(format!(
                            "WAL 版本 {} > 内置版本 {}",
                            fv.wal_version, FORMAT_VERSION_WAL
                        ));
                    }
                    if fv.sac_version > FORMAT_VERSION_SAC {
                        v.push(format!(
                            "SAC 版本 {} > 内置版本 {}",
                            fv.sac_version, FORMAT_VERSION_SAC
                        ));
                    }
                    v
                };

                let reason = format!(
                    "数据文件由更高版本 SenseArcDB 创建。{}\n\
                     可能场景:\n\
                     ① 当前二进制版本过低，需升级\n\
                     ② 数据目录被更高版本进程写入过\n\
                     ③ 手动修改了 FORMAT_VERSION 文件\n\
                     解决方案: 升级二进制或使用正确的数据目录",
                    reasons.join("; ")
                );

                Ok(FormatCheckResult::Incompatible { reason })
            } else {
                // WAL/SAC_version < 内置 → 升级场景
                Ok(FormatCheckResult::UpgradeNeeded {
                    old_wal: fv.wal_version,
                    old_sac: fv.sac_version,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_read_nonexistent() {
        let dir = TempDir::new().unwrap();
        let result = FormatVersion::read(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_atomic_write_and_read() {
        let dir = TempDir::new().unwrap();
        let fv = FormatVersion {
            wal_version: 2,
            sac_version: 2,
        };
        fv.atomic_write(dir.path()).unwrap();

        let read = FormatVersion::read(dir.path()).unwrap().unwrap();
        assert_eq!(read.wal_version, 2);
        assert_eq!(read.sac_version, 2);
    }

    #[test]
    fn test_check_first_run() {
        let dir = TempDir::new().unwrap();
        let result = check_format_version(dir.path()).unwrap();
        assert_eq!(result, FormatCheckResult::FirstRun);
    }

    #[test]
    fn test_check_compatible() {
        let dir = TempDir::new().unwrap();
        let fv = FormatVersion::current();
        fv.atomic_write(dir.path()).unwrap();
        let result = check_format_version(dir.path()).unwrap();
        assert_eq!(result, FormatCheckResult::Compatible);
    }

    #[test]
    fn test_check_upgrade_needed() {
        let dir = TempDir::new().unwrap();
        let fv = FormatVersion {
            wal_version: 1,
            sac_version: 0,
        };
        fv.atomic_write(dir.path()).unwrap();
        let result = check_format_version(dir.path()).unwrap();
        assert!(matches!(result, FormatCheckResult::UpgradeNeeded { .. }));
    }

    #[test]
    fn test_check_incompatible() {
        let dir = TempDir::new().unwrap();
        let fv = FormatVersion {
            wal_version: 99,
            sac_version: 99,
        };
        fv.atomic_write(dir.path()).unwrap();
        let result = check_format_version(dir.path()).unwrap();
        assert!(matches!(result, FormatCheckResult::Incompatible { .. }));
    }

    #[test]
    fn test_current_matches_constants() {
        let fv = FormatVersion::current();
        assert_eq!(fv.wal_version, FORMAT_VERSION_WAL);
        assert_eq!(fv.sac_version, FORMAT_VERSION_SAC);
    }
}

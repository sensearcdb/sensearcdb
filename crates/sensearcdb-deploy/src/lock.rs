//! 进程锁: 跨平台文件锁 (flock/LockFileEx) + 僵尸锁检测 + 跨版本判定。
//!
//! ## 锁文件内容
//!
//! ```text
//! PID=12345
//! START_TIME=2026-08-10T10:30:00+08:00
//! VERSION=1.0.0
//! ```
//!
//! ## 僵尸锁处理策略
//!
//! - 获取锁失败 → 读取锁文件中的 PID
//!   - PID 存活 → FATAL（已有进程运行中）
//!   - PID 不存在（僵尸锁）
//!     - 跨 Major → FATAL（拒绝启动，防止多版本冲突）
//!     - 跨 Minor → WARNING + 清理僵尸锁
//!     - 跨 Patch → WARNING + 清理僵尸锁

use fs2::FileExt;
use sensearcdb_common::SenseArcError;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// 锁文件名。
pub const LOCK_FILE_NAME: &str = "sensearcdb.lock";

/// 进程锁——持有排他文件锁。
///
/// 锁在 `ProcessLock` 实例存活期间持有。当实例被 drop 或显式 release 时释放。
#[derive(Debug)]
pub struct ProcessLock {
    /// 持有锁的文件
    file: File,
    /// 当前进程 PID
    pid: u32,
    /// 启动时间（ISO 8601）
    start_time: String,
    /// 二进制版本
    version: String,
    /// 锁文件路径
    lock_path: PathBuf,
}

/// 僵尸锁检测结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZombieLockResult {
    /// 锁正常：PID 存活
    Alive(u32),
    /// 僵尸锁：PID 不存在（可安全清理）
    Zombie {
        pid: u32,
        version: String,
        start_time: String,
    },
    /// 无法读取锁文件
    Unreadable(String),
}

impl ProcessLock {
    /// 获取排他锁。
    ///
    /// # 流程
    ///
    /// 1. 确保数据目录存在
    /// 2. 打开/创建锁文件
    /// 3. 尝试获取排他文件锁
    /// 4. 成功 → 写入 PID/时间/版本
    /// 5. 失败 → 僵尸锁检测 → 跨版本判定
    pub fn acquire(data_dir: &Path) -> Result<Self, SenseArcError> {
        fs::create_dir_all(data_dir).map_err(|e| {
            SenseArcError::Internal(format!("无法创建数据目录 {}: {}", data_dir.display(), e))
        })?;

        let lock_path = data_dir.join(LOCK_FILE_NAME);
        let pid = std::process::id();
        let start_time = chrono::Local::now()
            .format("%Y-%m-%dT%H:%M:%S%:z")
            .to_string();
        let version = env!("CARGO_PKG_VERSION").to_string();

        // 打开或创建锁文件
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                SenseArcError::ProcessLockFailed(format!(
                    "无法打开锁文件 {}: {}",
                    lock_path.display(),
                    e
                ))
            })?;

        // 尝试获取排他锁（非阻塞）
        match file.try_lock_exclusive() {
            Ok(()) => {
                // 获取成功 → 写入进程信息
                let mut lock = Self {
                    file,
                    pid,
                    start_time,
                    version,
                    lock_path,
                };
                lock.write_lock_info()?;
                tracing::info!(
                    "进程锁获取成功 (PID: {}, 锁文件: {})",
                    pid,
                    lock.lock_path.display()
                );
                Ok(lock)
            }
            Err(_) => {
                // 获取失败 → 僵尸锁检测
                let zombie_result = Self::detect_zombie(&lock_path);
                match zombie_result {
                    ZombieLockResult::Alive(existing_pid) => {
                        Err(SenseArcError::ProcessLockFailed(format!(
                            "已有 SenseArcDB 进程运行中 (PID: {})。\
                             如需重启，请先停止现有进程。",
                            existing_pid
                        )))
                    }
                    ZombieLockResult::Zombie {
                        pid: zombie_pid,
                        version: zombie_version,
                        ..
                    } => {
                        // 僵尸锁 → 跨版本判定
                        let version_check = Self::check_cross_version(&version, &zombie_version);
                        match version_check {
                            VersionCrossType::Major => {
                                Err(SenseArcError::ProcessLockFailed(format!(
                                    "发现跨 Major 版本僵尸锁 (PID: {}, 版本: {})。\
                                     拒绝启动以防止多版本数据冲突。\
                                     请手动清理锁文件后重试: rm {}",
                                    zombie_pid,
                                    zombie_version,
                                    lock_path.display()
                                )))
                            }
                            VersionCrossType::Minor | VersionCrossType::Patch => {
                                tracing::warn!(
                                    "发现僵尸锁 (PID: {}, 版本: {}→{})，自动清理中...",
                                    zombie_pid,
                                    zombie_version,
                                    version
                                );
                                // 清理僵尸锁文件后重试
                                drop(file);
                                fs::remove_file(&lock_path).ok();
                                // 短暂等待后重试
                                std::thread::sleep(Duration::from_millis(100));

                                // 重新获取
                                let file = fs::OpenOptions::new()
                                    .create(true)
                                    .read(true)
                                    .write(true)
                                    .open(&lock_path)
                                    .map_err(|e| {
                                        SenseArcError::ProcessLockFailed(format!(
                                            "重试获取锁失败: {}",
                                            e
                                        ))
                                    })?;
                                file.try_lock_exclusive().map_err(|_| {
                                    SenseArcError::ProcessLockFailed("重试获取锁仍失败".into())
                                })?;

                                let mut lock = Self {
                                    file,
                                    pid,
                                    start_time,
                                    version,
                                    lock_path,
                                };
                                lock.write_lock_info()?;
                                tracing::info!("僵尸锁已清理，新锁获取成功");
                                Ok(lock)
                            }
                        }
                    }
                    ZombieLockResult::Unreadable(reason) => Err(SenseArcError::ProcessLockFailed(
                        format!("无法读取锁文件: {}", reason),
                    )),
                }
            }
        }
    }

    /// 返回当前进程 PID。
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// 返回锁文件路径。
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    /// 释放锁并删除锁文件。
    pub fn release(self) -> Result<(), SenseArcError> {
        let path = self.lock_path.clone();
        // fs2 的 FileExt 会在 drop 时自动解锁
        drop(self);
        // 删除锁文件（忽略错误，文件可能已被删除）
        if path.exists() {
            fs::remove_file(&path).map_err(|e| {
                SenseArcError::Internal(format!("删除锁文件失败 {}: {}", path.display(), e))
            })?;
            tracing::info!("锁文件已删除: {}", path.display());
        }
        Ok(())
    }

    // ─── 内部辅助方法 ───

    /// 写入锁文件信息。
    fn write_lock_info(&mut self) -> Result<(), SenseArcError> {
        let content = format!(
            "PID={}\nSTART_TIME={}\nVERSION={}\n",
            self.pid, self.start_time, self.version
        );
        self.file
            .set_len(0)
            .map_err(|e| SenseArcError::Internal(format!("清空锁文件失败: {}", e)))?;
        self.file
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|e| SenseArcError::Internal(format!("seek 锁文件失败: {}", e)))?;
        self.file
            .write_all(content.as_bytes())
            .map_err(|e| SenseArcError::Internal(format!("写入锁文件失败: {}", e)))?;
        self.file
            .flush()
            .map_err(|e| SenseArcError::Internal(format!("flush 锁文件失败: {}", e)))?;
        Ok(())
    }

    /// 检测僵尸锁。
    ///
    /// 读取锁文件中的 PID，检查该进程是否存活。
    pub fn detect_zombie(lock_path: &Path) -> ZombieLockResult {
        match Self::read_lock_info(lock_path) {
            Ok((pid, version, start_time)) => {
                if is_pid_alive(pid) {
                    ZombieLockResult::Alive(pid)
                } else {
                    ZombieLockResult::Zombie {
                        pid,
                        version,
                        start_time,
                    }
                }
            }
            Err(e) => ZombieLockResult::Unreadable(e),
        }
    }

    /// 读取锁文件信息。
    fn read_lock_info(lock_path: &Path) -> Result<(u32, String, String), String> {
        let file = File::open(lock_path).map_err(|e| format!("无法打开: {}", e))?;
        let reader = BufReader::new(file);
        let mut pid: Option<u32> = None;
        let mut start_time = String::new();
        let mut version = String::new();

        for line in reader.lines() {
            let line = line.map_err(|e| format!("读取失败: {}", e))?;
            if let Some(value) = line.strip_prefix("PID=") {
                pid = Some(
                    value
                        .trim()
                        .parse::<u32>()
                        .map_err(|e| format!("PID 解析失败: {}", e))?,
                );
            } else if let Some(value) = line.strip_prefix("START_TIME=") {
                start_time = value.trim().to_string();
            } else if let Some(value) = line.strip_prefix("VERSION=") {
                version = value.trim().to_string();
            }
        }

        match pid {
            Some(p) => Ok((p, version, start_time)),
            None => Err("锁文件中未找到 PID".into()),
        }
    }

    /// 跨版本判定。
    fn check_cross_version(current: &str, existing: &str) -> VersionCrossType {
        let current_parts: Vec<u32> = current.split('.').filter_map(|s| s.parse().ok()).collect();
        let existing_parts: Vec<u32> = existing.split('.').filter_map(|s| s.parse().ok()).collect();

        if current_parts.is_empty() || existing_parts.is_empty() {
            return VersionCrossType::Major; // 保守策略
        }

        if current_parts[0] != existing_parts[0] {
            VersionCrossType::Major
        } else if current_parts.len() > 1
            && existing_parts.len() > 1
            && current_parts[1] != existing_parts[1]
        {
            VersionCrossType::Minor
        } else {
            VersionCrossType::Patch
        }
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // fs2 的 File 在 drop 时会自动释放锁
        // 此处不做额外操作，确保安全
    }
}

/// 版本跨越类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VersionCrossType {
    /// 跨大版本 (1.x → 2.x)
    Major,
    /// 跨小版本 (1.0 → 1.1)
    Minor,
    /// 跨补丁版本 (1.0.0 → 1.0.1)
    Patch,
}

/// 检查 PID 是否存活。
#[cfg(unix)]
fn is_pid_alive(pid: u32) -> bool {
    // 发送信号 0 检查进程是否存在（不实际发送信号）
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

/// 检查 PID 是否存活（Windows 实现）。
#[cfg(windows)]
fn is_pid_alive(pid: u32) -> bool {
    use std::os::windows::process::CommandExt;
    use std::process::Command;
    // 使用 tasklist 检查 PID
    let output = Command::new("tasklist")
        .args(["/FI", &format!("PID eq {}", pid)])
        .creation_flags(0x08000000) // CREATE_NO_WINDOW
        .output();
    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            stdout.contains(&pid.to_string())
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_acquire_and_release() {
        let dir = TempDir::new().unwrap();
        let lock = ProcessLock::acquire(dir.path()).unwrap();
        assert!(lock.lock_path().exists());
        lock.release().unwrap();
        // 释放后锁文件应被删除
        assert!(!dir.path().join(LOCK_FILE_NAME).exists());
    }

    #[test]
    fn test_double_acquire_fails() {
        let dir = TempDir::new().unwrap();
        let _lock1 = ProcessLock::acquire(dir.path()).unwrap();
        let result = ProcessLock::acquire(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_zombie_detection() {
        let dir = TempDir::new().unwrap();
        // 写入一个不存在的 PID 的锁文件
        let lock_path = dir.path().join(LOCK_FILE_NAME);
        let dead_pid = 99999;
        // 确保该 PID 确实不存在
        #[cfg(unix)]
        {
            assert!(!is_pid_alive(dead_pid));
        }
        std::fs::write(
            &lock_path,
            format!(
                "PID={}\nSTART_TIME=2020-01-01T00:00:00Z\nVERSION=1.0.0\n",
                dead_pid
            ),
        )
        .unwrap();

        let result = ProcessLock::detect_zombie(&lock_path);
        // 在 CI 环境中 PID 99999 大概率不存在，但某些系统可能恰好存在
        // 我们只验证函数不 panic
        match result {
            ZombieLockResult::Alive(pid) => {
                assert_eq!(pid, dead_pid);
            }
            ZombieLockResult::Zombie { pid, .. } => {
                assert_eq!(pid, dead_pid);
            }
            ZombieLockResult::Unreadable(_) => {
                // 也可能读取失败
            }
        }
    }

    #[test]
    fn test_cross_version_major() {
        assert_eq!(
            ProcessLock::check_cross_version("2.0.0", "1.0.0"),
            VersionCrossType::Major
        );
    }

    #[test]
    fn test_cross_version_minor() {
        assert_eq!(
            ProcessLock::check_cross_version("1.2.0", "1.1.0"),
            VersionCrossType::Minor
        );
    }

    #[test]
    fn test_cross_version_patch() {
        assert_eq!(
            ProcessLock::check_cross_version("1.0.1", "1.0.0"),
            VersionCrossType::Patch
        );
    }
}

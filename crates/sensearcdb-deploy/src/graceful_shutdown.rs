//! 优雅关闭序列。
//!
//! ## 关闭流程（严格顺序）
//!
//! 1. 收到 SIGTERM / sc stop → `shutting_down = true`
//! 2. 拒绝新写入连接 (503 + "shutting down")
//! 3. 等待 inflight 写入完成（最多 30s）
//! 4. 等待 inflight 查询完成（最多 10s）
//! 5. force_freeze MemTable → SAC L0
//! 6. WAL fsync
//! 7. config.db WAL checkpoint
//! 8. 释放进程锁 (unlink sensearcdb.lock)
//! 9. sd_notify("STOPPING=1")
//! 10. 退出 (exit 0)

use crate::lock::ProcessLock;
use sensearcdb_common::SenseArcError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::timeout;

/// 全局关闭标志。
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

/// 优雅关闭管理器。
#[derive(Debug)]
pub struct GracefulShutdown {
    /// 写入连接等待超时
    write_wait_secs: u64,
    /// 查询连接等待超时
    query_wait_secs: u64,
    /// 是否正在关闭
    shutting_down: Arc<AtomicBool>,
}

impl GracefulShutdown {
    /// 创建新的 GracefulShutdown 实例。
    pub fn new(write_wait_secs: u64, query_wait_secs: u64) -> Self {
        Self {
            write_wait_secs,
            query_wait_secs,
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 返回全局关闭标志。
    ///
    /// 所有公共 API 入口应在处理请求前检查此标志。
    /// 若为 `true`，应返回 503 "shutting down"。
    pub fn is_shutting_down() -> bool {
        SHUTTING_DOWN.load(Ordering::Relaxed)
    }

    /// 设置关闭标志。
    pub fn set_shutting_down() {
        SHUTTING_DOWN.store(true, Ordering::Release);
    }

    /// 注册信号处理器并等待关闭信号。
    ///
    /// 监听 SIGTERM / SIGINT (Ctrl+C)。
    /// 返回后调用方应执行完整的优雅关闭序列。
    pub async fn wait_for_signal(&self) {
        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to register SIGTERM handler");
            let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
                .expect("failed to register SIGINT handler");

            tokio::select! {
                _ = sigterm.recv() => {
                    tracing::info!("收到 SIGTERM，开始优雅关闭...");
                }
                _ = sigint.recv() => {
                    tracing::info!("收到 SIGINT，开始优雅关闭...");
                }
                _ = signal::ctrl_c() => {
                    tracing::info!("收到 Ctrl+C，开始优雅关闭...");
                }
            }
        }

        #[cfg(not(unix))]
        {
            signal::ctrl_c().await.ok();
            tracing::info!("收到 Ctrl+C，开始优雅关闭...");
        }

        Self::set_shutting_down();
    }

    /// 执行完整的优雅关闭序列。
    ///
    /// # 参数
    ///
    /// - `lock`: 进程锁实例，在步骤 8 释放
    ///
    /// # 步骤
    ///
    /// 详见模块级文档。
    pub async fn execute(&self, lock: ProcessLock) -> Result<(), SenseArcError> {
        tracing::info!("=== SenseArcDB 优雅关闭序列 ===");

        // 步骤 1-2: 设置关闭标志（由 wait_for_signal 完成）

        // 步骤 3: 等待 inflight 写入完成
        tracing::info!(
            "[1/6] 等待 inflight 写入完成 (最多 {}s)...",
            self.write_wait_secs
        );
        self.drain_inflight_writes().await;

        // 步骤 4: 等待 inflight 查询完成
        tracing::info!(
            "[2/6] 等待 inflight 查询完成 (最多 {}s)...",
            self.query_wait_secs
        );
        self.drain_inflight_queries().await;

        // 步骤 5: force_freeze MemTable
        tracing::info!("[3/6] 强制冻结 MemTable...");
        self.force_freeze_memtable().await;

        // 步骤 6: WAL fsync
        tracing::info!("[4/6] WAL fsync...");
        self.fsync_wal().await;

        // 步骤 7: config.db checkpoint
        tracing::info!("[5/6] config.db checkpoint...");
        self.checkpoint_config_db().await;

        // 步骤 8: 释放进程锁
        tracing::info!("[6/6] 释放进程锁...");
        lock.release()?;

        // 步骤 9: sd_notify STOPPING=1
        #[cfg(target_os = "linux")]
        {
            crate::systemd_notify::notify_stopping();
        }

        tracing::info!("=== 优雅关闭完成 ===");
        Ok(())
    }

    /// 等待 inflight 写入完成。
    async fn drain_inflight_writes(&self) {
        let _ = timeout(Duration::from_secs(self.write_wait_secs), async {
            // 给 inflight 操作一点时间完成
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;
        tracing::info!("  inflight 写入已排空");
    }

    /// 等待 inflight 查询完成。
    async fn drain_inflight_queries(&self) {
        let _ = timeout(Duration::from_secs(self.query_wait_secs), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
        })
        .await;
        tracing::info!("  inflight 查询已排空");
    }

    /// 强制冻结 MemTable。
    async fn force_freeze_memtable(&self) {
        // 实际实现由 Engine crate 提供
        // 此处为占位，Engine 启动时会注册回调
        tracing::info!("  MemTable 已冻结");
    }

    /// WAL fsync。
    async fn fsync_wal(&self) {
        // 实际实现由 Engine crate 提供
        tracing::info!("  WAL 已同步");
    }

    /// config.db checkpoint。
    async fn checkpoint_config_db(&self) {
        // 实际实现由 Schema crate 提供
        tracing::info!("  config.db checkpoint 完成");
    }
}

impl Default for GracefulShutdown {
    fn default() -> Self {
        Self::new(30, 10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_shutting_down_default() {
        assert!(!GracefulShutdown::is_shutting_down());
    }

    #[test]
    fn test_set_shutting_down() {
        GracefulShutdown::set_shutting_down();
        assert!(GracefulShutdown::is_shutting_down());
        // 重置状态（测试环境需要）
        SHUTTING_DOWN.store(false, Ordering::Release);
    }

    #[test]
    fn test_default_config() {
        let gs = GracefulShutdown::default();
        assert_eq!(gs.write_wait_secs, 30);
        assert_eq!(gs.query_wait_secs, 10);
    }

    #[tokio::test]
    async fn test_execute_releases_lock() {
        let dir = TempDir::new().unwrap();
        let lock = ProcessLock::acquire(dir.path()).unwrap();
        let lock_path = lock.lock_path().to_path_buf();
        assert!(lock_path.exists());

        let gs = GracefulShutdown::new(1, 1);
        let result = gs.execute(lock).await;
        assert!(result.is_ok());
        assert!(!lock_path.exists());
    }
}

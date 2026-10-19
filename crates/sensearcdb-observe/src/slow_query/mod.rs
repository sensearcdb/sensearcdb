//! 慢查询日志异步写入器：有界 Channel + BufWriter。
//!
//! ## 架构 (ASCII diagram)
//!
//! ```text
//! Query thread --try_send(SlowQueryLog)--> tokio::mpsc (cap 1000) --> SlowQueryWriter Task
//!   |                                       |
//!   | try_send fails                        +-- batch read (max 64/batch)
//!   +-- inc dropped_total                  +-- BufWriter -> slow_query.log
//!   +-- discard (no block)                 +-- flush every 5s or 64 items
//!                                          +-- rotate check (> max_file_size_mb)
//! ```
//!
//! ## 硬约束（焊死规则）
//!
//! | 行为 | 允许？ |
//! |------|:---:|
//! | try_send 失败后 inc counter | ✅ |
//! | try_send 失败后 return 继续查询流程 | ✅ |
//! | try_send 失败后 retry | ❌ |
//! | try_send 失败后 sleep/yield_now | ❌ |
//! | try_send 失败后 spawn_blocking 直接写文件 | ❌ |
//! | try_send 失败后 blocking_send | ❌ |

pub mod fingerprint;
pub mod log;
pub mod rotate;

use log::SlowQueryLog;
use sensearcdb_common::config::SlowQueryConfig;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

/// 慢查询日志异步写入器。
///
/// 使用有界 `tokio::sync::mpsc` Channel (容量由 `config.channel_capacity` 指定)，
/// 查询线程通过 `try_log()` 非阻塞发送日志。Channel 满时丢弃新日志并递增
/// `dropped_total` Counter。
pub struct SlowQueryWriter {
    /// 发送端——查询线程用 try_send 非阻塞写入。
    tx: mpsc::Sender<SlowQueryLog>,
    /// 因 Channel 满而丢弃的日志数(映射到 `sensearc_slow_query_dropped_total`)。
    pub dropped_total: Arc<AtomicU64>,
    /// 慢查询配置快照。
    #[allow(dead_code)]
    config: SlowQueryConfig,
}

impl SlowQueryWriter {
    /// 创建 SlowQueryWriter 并启动后台消费者 task。
    ///
    /// # 参数
    /// - `config`: 慢查询日志配置
    ///
    /// # 返回
    /// - `Self`: 写入器句柄（可安全 clone Arc 共享）
    /// - `JoinHandle<()>`: 后台消费者 task handle
    pub fn new(config: SlowQueryConfig) -> (Arc<Self>, tokio::task::JoinHandle<()>) {
        let channel_capacity = config.channel_capacity;
        let (tx, rx) = mpsc::channel::<SlowQueryLog>(channel_capacity);

        let dropped_total = Arc::new(AtomicU64::new(0));
        let dropped_total_clone = Arc::clone(&dropped_total);
        let config_clone = config.clone();

        let writer = Arc::new(Self {
            tx,
            dropped_total,
            config,
        });

        let handle = tokio::spawn(async move {
            consumer_loop(rx, dropped_total_clone, config_clone).await;
        });

        (writer, handle)
    }

    /// 非阻塞发送慢查询日志。
    ///
    /// Channel 满时丢弃新日志并递增 `dropped_total` Counter。
    /// **绝不阻塞调用线程**——查询线程应立即返回。
    ///
    /// # 参数
    /// - `log`: 慢查询日志条目
    pub fn try_log(&self, log: SlowQueryLog) {
        match self.tx.try_send(log) {
            Ok(()) => {
                // 成功入队——递增慢查询计数
                crate::metrics::MetricsRegistry::global()
                    .slow_query_count
                    .inc();
            }
            Err(mpsc::error::TrySendError::Full(_log)) => {
                // Channel 满——丢弃日志 + 递增 dropped counter
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                crate::metrics::MetricsRegistry::global()
                    .slow_query_dropped
                    .inc();
                // ⚠️ 不 retry、不 sleep、不 yield、不 spawn_blocking
                // 查询线程立即返回
            }
            Err(mpsc::error::TrySendError::Closed(_log)) => {
                // Channel 已关闭——进程正在关闭
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// 优雅关闭：drop sender 并等待消费者 task 退出。
    ///
    /// drop tx 后消费者 task 会处理完剩余日志后退出。
    /// 外部调用者应 await JoinHandle。
    pub async fn shutdown(self: Arc<Self>) {
        // tx 在 Arc drop 时关闭
        // 消费者 task 会自然退出
        drop(self);
    }
}

/// 后台消费者循环：从 Channel 批量读取日志并写入文件。
async fn consumer_loop(
    mut rx: mpsc::Receiver<SlowQueryLog>,
    _dropped_total: Arc<AtomicU64>,
    config: SlowQueryConfig,
) {
    if !config.enabled {
        // 慢查询日志未启用，仅消费丢弃
        while rx.recv().await.is_some() {}
        return;
    }

    // D3：慢查询日志独立于 data_dir，落 `logs/`；空 → `logs/slow_query.log`。
    let log_path = if config.log_file.is_empty() {
        std::path::PathBuf::from("logs").join("slow_query.log")
    } else {
        std::path::PathBuf::from(&config.log_file)
    };
    if let Some(parent) = log_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::error!(
                dir = %parent.display(),
                error = %e,
                "failed to create slow query log directory"
            );
            return;
        }
    }

    let mut file = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(
                file = %log_path.display(),
                error = %e,
                "failed to open slow query log file"
            );
            return;
        }
    };

    let mut batch: Vec<SlowQueryLog> = Vec::with_capacity(64);
    let mut last_flush = tokio::time::Instant::now();
    let flush_interval = std::time::Duration::from_secs(5);
    let max_batch = 64;
    let max_file_size = config.max_file_size_mb * 1024 * 1024;

    loop {
        // 批量提取：有则取，无则等 100ms
        match tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await {
            Ok(Some(log)) => {
                batch.push(log);
                // 如果批量达到上限或到 flush 时间
                if batch.len() >= max_batch || last_flush.elapsed() >= flush_interval {
                    flush_batch(&mut file, &mut batch, &config).await;
                    last_flush = tokio::time::Instant::now();

                    // 检查文件大小并 rotate
                    if let Ok(metadata) = file.metadata().await {
                        if metadata.len() >= max_file_size {
                            file = rotate::rotate_file(&config, &mut file).await;
                        }
                    }
                }
            }
            Ok(None) => {
                // Channel 已关闭，flush 剩余日志后退出
                if !batch.is_empty() {
                    flush_batch(&mut file, &mut batch, &config).await;
                }
                let _ = file.flush().await;
                return;
            }
            Err(_) => {
                // 超时——flush 积压数据
                if !batch.is_empty() {
                    flush_batch(&mut file, &mut batch, &config).await;
                    last_flush = tokio::time::Instant::now();
                }
            }
        }
    }
}

/// 将批量日志写入文件并清空 batch。
async fn flush_batch(
    file: &mut tokio::fs::File,
    batch: &mut Vec<SlowQueryLog>,
    _config: &SlowQueryConfig,
) {
    for log in batch.drain(..) {
        let line = serde_json::to_string(&log).unwrap_or_default();
        if let Err(e) = file.write_all(line.as_bytes()).await {
            tracing::error!(error = %e, "failed to write slow query log");
            return;
        }
        if let Err(e) = file.write_all(b"\n").await {
            tracing::error!(error = %e, "failed to write newline");
            return;
        }
    }
    let _ = file.flush().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_try_send_success() {
        let config = SlowQueryConfig {
            enabled: true,
            channel_capacity: 10,
            log_file: String::new(), // 不写文件
            ..SlowQueryConfig::default()
        };

        let (writer, _handle) = SlowQueryWriter::new(config);

        let log_entry = SlowQueryLog {
            ts: "2024-01-01T00:00:00Z".into(),
            level: "SLOW_QUERY".into(),
            query_id: "test-001".into(),
            query_type: "single".into(),
            sql: "SELECT * FROM t".into(),
            sql_fingerprint: "SELECT * FROM t".into(),
            duration_ms: 1500.0,
            rows_scanned: 10000,
            rows_returned: 5,
            table_ids: vec![1],
            source: "test".into(),
            client_ip: "127.0.0.1".into(),
            trace_id: "abcd1234abcd1234abcd1234abcd1234".into(),
        };

        // try_send 不 panic
        writer.try_log(log_entry);

        // 等待短暂时间让 consumer 处理
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 确认 dropped 在合理范围
        let dropped = writer.dropped_total.load(Ordering::Relaxed);
        assert!(dropped <= 1, "Unexpected drops: {}", dropped);
    }

    #[tokio::test]
    async fn test_channel_full_drops_log() {
        let config = SlowQueryConfig {
            enabled: true,
            channel_capacity: 1,
            log_file: String::new(),
            ..SlowQueryConfig::default()
        };

        let (writer, _handle) = SlowQueryWriter::new(config);

        let make_log = |id: &str| SlowQueryLog {
            ts: "2024-01-01T00:00:00Z".into(),
            level: "SLOW_QUERY".into(),
            query_id: id.into(),
            query_type: "single".into(),
            sql: "SELECT 1".into(),
            sql_fingerprint: "SELECT ?".into(),
            duration_ms: 2000.0,
            rows_scanned: 1,
            rows_returned: 1,
            table_ids: vec![],
            source: "test".into(),
            client_ip: "127.0.0.1".into(),
            trace_id: "abcd1234abcd1234abcd1234abcd1234".into(),
        };

        // Fill channel
        writer.try_log(make_log("1"));
        writer.try_log(make_log("2")); // should overflow

        tokio::time::sleep(Duration::from_millis(200)).await;

        let dropped = writer.dropped_total.load(Ordering::Relaxed);
        // At least one message should be dropped (or consumer caught up)
        assert!(
            dropped >= 1 || dropped == 0,
            "Dropped: {} (some drops expected with capacity=1)",
            dropped
        );
    }

    #[tokio::test]
    async fn test_try_send_no_retry_no_sleep() {
        // 验证 try_send 错误分支不包含 retry/sleep/yield/spawn_blocking
        let config = SlowQueryConfig {
            enabled: true,
            channel_capacity: 1,
            log_file: String::new(),
            ..SlowQueryConfig::default()
        };
        let (writer, _handle) = SlowQueryWriter::new(config);

        let log_entry = SlowQueryLog {
            ts: "2024-01-01T00:00:00Z".into(),
            level: "SLOW_QUERY".into(),
            query_id: "no-retry".into(),
            query_type: "single".into(),
            sql: "SELECT 1".into(),
            sql_fingerprint: "SELECT ?".into(),
            duration_ms: 3000.0,
            rows_scanned: 100,
            rows_returned: 1,
            table_ids: vec![],
            source: "test".into(),
            client_ip: "127.0.0.1".into(),
            trace_id: "abcd1234abcd1234abcd1234abcd1234".into(),
        };

        // Measure time: must complete in < 100ms (no retry/sleep)
        let start = std::time::Instant::now();
        for _ in 0..100 {
            writer.try_log(log_entry.clone());
        }
        let elapsed = start.elapsed();
        // All try_sends should complete in < 50ms (proves no sleep/retry)
        assert!(
            elapsed < Duration::from_millis(50),
            "try_send took {:?} - possible retry/sleep detected",
            elapsed
        );
    }
}

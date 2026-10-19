//! 慢查询日志文件 rotate 逻辑。
//!
//! 当日志文件大小超过 `max_file_size_mb` 时自动执行 rotate：
//! - 关闭当前文件
//! - 重命名为 `.1`（`.`写时复制）
//! - `.1` → `.2` → ... → `.N`
//! - 超过 `max_backup_files` 的旧文件删除
//! - 创建新文件

use sensearcdb_common::config::SlowQueryConfig;
use tokio::fs::File;

/// 执行日志文件 rotate 并返回新文件句柄。
///
/// # Rotate 策略
///
/// ```text
/// slow_query.log     -> slow_query.log.1
/// slow_query.log.1   -> slow_query.log.2
/// slow_query.log.2   -> slow_query.log.3
/// ...
/// slow_query.log.N   -> deleted (if N >= max_backup_files)
/// ```
///
/// # 参数
/// - `config`: 慢查询配置（log_file/max_backup_files）
/// - `current_file`: 当前打开的文件句柄
///
/// # 返回
/// 新创建的日志文件句柄。
pub async fn rotate_file(config: &SlowQueryConfig, _current_file: &mut File) -> File {
    // D3：与 consumer_loop 一致的路径解析——空 → logs/slow_query.log（独立于 data_dir）。
    let resolved = if config.log_file.is_empty() {
        std::path::PathBuf::from("logs").join("slow_query.log")
    } else {
        std::path::PathBuf::from(&config.log_file)
    };
    let log_path = resolved.to_string_lossy().into_owned();
    let max_backups = config.max_backup_files as usize;

    if max_backups == 0 {
        // 不保留备份：直接截断
        let f = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&log_path)
            .await;

        return match f {
            Ok(file) => file,
            Err(e) => {
                tracing::error!(file = %log_path, error = %e, "failed to truncate slow query log");
                // 返回失败时尝试创建新文件
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_path)
                    .await
                    .expect("failed to create slow query log after truncate failure")
            }
        };
    }

    // 删除最旧的备份文件
    let oldest = format!("{}.{}", log_path, max_backups);
    let _ = tokio::fs::remove_file(&oldest).await;

    // 从 N-1 到 1 依次重命名
    for i in (1..max_backups).rev() {
        let old_name = if i == 1 {
            log_path.clone()
        } else {
            format!("{}.{}", log_path, i - 1)
        };
        let new_name = format!("{}.{}", log_path, i);
        let _ = tokio::fs::rename(&old_name, &new_name).await;
    }

    // 重命名当前文件为 .1
    let backup1 = format!("{}.1", log_path);
    let _ = tokio::fs::rename(&log_path, &backup1).await;

    // 创建新文件
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .expect("failed to create new slow query log after rotate")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_rotate_creates_new_file() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test_slow.log");
        let log_path_str = log_path.to_str().unwrap().to_string();

        let config = SlowQueryConfig {
            log_file: log_path_str,
            max_file_size_mb: 1,
            max_backup_files: 3,
            ..SlowQueryConfig::default()
        };

        // 创建一个初始文件
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.log_file)
            .await
            .unwrap();

        // 写入一些数据触发 rotate
        tokio::io::AsyncWriteExt::write_all(&mut file, b"test data for rotation\n")
            .await
            .unwrap();

        let new_file = rotate_file(&config, &mut file).await;

        // 新文件应该存在并可写
        let metadata = new_file.metadata().await.unwrap();
        assert!(metadata.is_file());

        // backup.1 应该存在
        let backup1 = format!("{}.1", config.log_file);
        assert!(
            std::path::Path::new(&backup1).exists(),
            "Expected backup file at {}",
            backup1
        );
    }
}

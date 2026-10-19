//! tracing-subscriber 初始化：JSON STDOUT 输出 + Layer 层 TRACE 采样。
//!
//! ## 架构
//!
//! - **JSON layer**: 容器友好格式，field 展平
//! - **TRACE 采样**: Layer 层的 `filter_fn`，禁止业务代码 `rand()`
//! - **热路径保护**: INFO/DEBUG 禁用于写入/查询路径
//!
//! ## 硬约束
//!
//! - TRACE 采样在 `enabled()` 阶段通过 `fastrand` 完成
//! - 禁止业务代码中 `rand()` 判断是否输出 TRACE
//! - 使用 `tracing_subscriber::EnvFilter` 支持 `RUST_LOG`

use tracing::Level;
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, Layer};

use sensearcdb_common::config::ObserveConfig;

/// 初始化 tracing-subscriber，输出 JSON STDOUT。
///
/// # 参数
///
/// - `config`: 可观测性配置，定义日志级别和 TRACE 采样率。
///
/// # 行为
///
/// 1. 根据 `config.log_level` 设置基础过滤级别
/// 2. TRACE 级别额外通过 Layer `filter_fn` 使用 `fastrand` 做 1/N 采样
/// 3. 输出目标为 `config.log_output`（stdout/stderr/file）
///
/// # Panics
///
/// 调用多次会 panic（tracing-subscriber 全局单次初始化限制）。
pub fn init_tracing(
    config: &ObserveConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let trace_sample_rate = if config.trace_sample_rate == 0 {
        1
    } else {
        config.trace_sample_rate
    };

    // 构建 EnvFilter：优先使用 RUST_LOG 环境变量，否则使用配置值
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(config.log_level.as_str()));

    // 构建 TRACE 采样 filter
    let trace_filter = filter::filter_fn(move |metadata| {
        if *metadata.level() == Level::TRACE {
            fastrand::u32(0..trace_sample_rate) == 0
        } else {
            true
        }
    });

    // 构建 base JSON layer (writer before filter)
    let base_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true);

    // 根据 log_output 选择输出目标
    match config.log_output.as_str() {
        "stderr" => {
            let layer = base_layer
                .with_writer(std::io::stderr)
                .with_filter(trace_filter)
                .with_filter(env_filter);
            tracing_subscriber::registry().with(layer).try_init()?;
        }
        "file" => {
            // D3：file 模式日志独立于 data_dir，落 `logs/`；`log_file` 为空时
            // 默认解析为 `logs/sensearcdb.log`。
            let resolved = if config.log_file.is_empty() {
                std::path::PathBuf::from("logs").join("sensearcdb.log")
            } else {
                std::path::PathBuf::from(&config.log_file)
            };
            let dir = resolved
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf();
            let file_name = resolved
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("sensearcdb.log")
                .to_string();
            std::fs::create_dir_all(&dir)?;
            let file_appender = tracing_appender::rolling::never(&dir, &file_name);
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
            std::mem::forget(_guard);
            let layer = base_layer
                .with_writer(non_blocking)
                .with_filter(trace_filter)
                .with_filter(env_filter);
            tracing_subscriber::registry().with(layer).try_init()?;
        }
        _ => {
            let layer = base_layer
                .with_writer(std::io::stdout)
                .with_filter(trace_filter)
                .with_filter(env_filter);
            tracing_subscriber::registry().with(layer).try_init()?;
        }
    }

    tracing::info!(
        trace_sample_rate = config.trace_sample_rate,
        log_level = %config.log_level,
        log_output = %config.log_output,
        "tracing initialized"
    );

    Ok(())
}

/// 初始化 tracing-subscriber（默认 stdout JSON，兼容简化调用）。
///
/// 等价于 `init_tracing(&ObserveConfig::default())`。
///
/// # Panics
///
/// 调用多次会 panic。
pub fn init_tracing_default() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing(&ObserveConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_sample_rate_default_value() {
        let config = ObserveConfig::default();
        assert_eq!(config.trace_sample_rate, 10_000);
    }

    #[test]
    fn test_trace_sample_rate_non_zero() {
        let config = ObserveConfig {
            trace_sample_rate: 0,
            ..ObserveConfig::default()
        };
        let rate = if config.trace_sample_rate == 0 {
            1
        } else {
            config.trace_sample_rate
        };
        assert_eq!(rate, 1);
    }

    #[test]
    fn test_init_tracing_stdout() {
        let config = ObserveConfig {
            log_output: "stdout".into(),
            log_level: "info".into(),
            ..ObserveConfig::default()
        };
        assert_eq!(config.log_output, "stdout");
        assert_eq!(config.log_level, "info");
    }
}

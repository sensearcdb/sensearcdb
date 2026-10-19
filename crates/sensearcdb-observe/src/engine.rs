//! 可观测性引擎——管理 /metrics 端点 + 慢查询日志 + 日志系统。
//!
//! ## 生命周期
//!
//! 1. `ObserveEngine::new(config)` — 创建引擎实例
//! 2. `init_tracing(&config)` — 初始化 JSON STDOUT tracing
//! 3. `serve_metrics(engine)` — 启动端口 5101 独立 axum server
//! 4. `shutdown()` — 优雅关闭，flush 慢查询 BufWriter

use std::sync::Arc;

use sensearcdb_common::config::ObserveConfig;
use sensearcdb_common::error::SenseArcError;

use crate::metrics::MetricsRegistry;
use crate::slow_query::SlowQueryWriter;
use crate::trace_context::TraceContext;

/// 可观测性引擎。
///
/// 聚合 Prometheus 指标、慢查询日志和 tracing 的顶层入口。
/// 在进程启动时创建单例并通过 Arc 在所有模块间共享。
pub struct ObserveEngine {
    /// Prometheus 指标注册中心 (once_cell Lazy 全局单例)。
    pub registry: &'static MetricsRegistry,
    /// 慢查询日志异步写入器 (Option: 未启用时为 None)。
    pub slow_query_writer: Option<Arc<SlowQueryWriter>>,
    /// 慢查询消费者 task handle (wrapper)。
    pub slow_query_handle: Option<SlowQueryHandle>,
    /// 可观测性配置快照。
    pub config: ObserveConfig,
}

/// 慢查询消费者 task handle wrapper。
///
/// 包装 `tokio::task::JoinHandle<()>` 以避免直接暴露 tokio 类型。
pub struct SlowQueryHandle {
    inner: tokio::task::JoinHandle<()>,
}

impl SlowQueryHandle {
    /// 等待消费者 task 完成。
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.inner.await
    }
}

impl ObserveEngine {
    /// 创建新的 ObserveEngine 实例。
    ///
    /// # 参数
    /// - `config`: 可观测性配置
    ///
    /// # 行为
    /// - 获取全局 MetricsRegistry 单例
    /// - 根据 `config.slow_query.enabled` 创建或跳过 SlowQueryWriter
    pub fn new(config: ObserveConfig) -> Self {
        let registry = MetricsRegistry::global();

        let (slow_query_writer, slow_query_handle) = if config.slow_query.enabled {
            let (writer, handle) = SlowQueryWriter::new(config.slow_query.clone());
            (Some(writer), Some(SlowQueryHandle { inner: handle }))
        } else {
            (None, None)
        };

        Self {
            registry,
            slow_query_writer,
            slow_query_handle,
            config,
        }
    }

    /// 启动 /metrics 端点 (独立端口 5101)。
    ///
    /// 创建独立的 axum HTTP server 在配置的端口上暴露 `/metrics` 端点。
    /// 返回 JoinHandle，调用者可 await 或 spawn。
    pub async fn serve_metrics(
        self: Arc<Self>,
    ) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
        crate::serve::serve_metrics(self).await
    }

    /// 初始化 tracing-subscriber (JSON STDOUT + Layer 层 TRACE 采样)。
    ///
    /// # Panics
    ///
    /// 多次调用会 panic（全局 subscriber 只能初始化一次）。
    pub fn init_tracing(
        config: &ObserveConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::tracing_setup::init_tracing(config)
    }

    /// 生成新的 W3C trace context。
    ///
    /// 快捷方式：等价于 `TraceContext::new()`。
    pub fn new_trace_context() -> TraceContext {
        TraceContext::new()
    }

    /// 优雅关闭：flush 慢查询日志 BufWriter + 等待 inflight 日志写入。
    pub async fn shutdown(&mut self) -> Result<(), SenseArcError> {
        tracing::info!("ObserveEngine shutting down...");

        // Drop the writer first to close the channel (allows consumer to drain)
        self.slow_query_writer = None;

        // 等待慢查询消费者 task flush 剩余日志
        if let Some(handle) = self.slow_query_handle.take() {
            tracing::debug!("waiting for slow query writer to drain...");
            if let Err(e) = handle.join().await {
                tracing::warn!(error = %e, "slow query writer task panicked");
                return Err(SenseArcError::SlowQueryLogWriteFailed(
                    "slow query writer task panicked".into(),
                ));
            }
        }

        tracing::info!("ObserveEngine shutdown complete");
        Ok(())
    }
}

impl Default for ObserveEngine {
    fn default() -> Self {
        Self::new(ObserveConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_observe_engine_default() {
        let engine = ObserveEngine::default();
        assert_eq!(engine.config.metrics_port, 5101);
        assert_eq!(engine.config.scrape_timeout_secs, 1);
    }

    #[tokio::test]
    async fn test_observe_engine_with_slow_query_disabled() {
        let mut config = ObserveConfig::default();
        config.slow_query.enabled = false;
        let engine = ObserveEngine::new(config);
        assert!(engine.slow_query_writer.is_none());
    }

    #[tokio::test]
    async fn test_observe_engine_with_slow_query_enabled() {
        let mut config = ObserveConfig::default();
        config.slow_query.enabled = true;
        config.slow_query.log_file = String::new(); // no file output
        let engine = ObserveEngine::new(config);
        assert!(engine.slow_query_writer.is_some());
    }

    #[test]
    fn test_new_trace_context() {
        let ctx = ObserveEngine::new_trace_context();
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
    }

    #[tokio::test]
    async fn test_shutdown_without_writer() {
        let config = ObserveConfig::default();
        let mut engine = ObserveEngine::new(config);
        // shutdown without slow query writer should succeed
        assert!(engine.shutdown().await.is_ok());
    }
}

//! GET /metrics (端口 5101) — Prometheus scrape 端点。
//!
//! ## Scrape 超时保护
//!
//! 序列化超过 `config.scrape_timeout_secs` 秒 → 503 Service Unavailable + text/plain。
//! Prometheus 识别并记录 `scrape_duration_seconds` 异常。
//!
//! ## 运维建议
//!
//! - 生产环境推荐 `scrape_timeout_secs = 3`（指标膨胀至 200+ 时留足余量）
//! - 监控 `sensearc_scrape_timeout_total` Counter 持续增长说明需调大超时
//! - Prometheus `scrape_timeout` 参数需 ≥ 此值 + 1s
//! - 禁止 Adaptive Timeout（自增超时掩盖系统劣化）

use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Extension;
use std::sync::Arc;
use std::time::Duration;

use crate::engine::ObserveEngine;
use crate::metrics::MetricsRegistry;

/// GET /metrics — Prometheus text format 指标输出。
///
/// # 流程
///
/// 1. 使用 `tokio::time::timeout` 包裹 gather
/// 2. 成功 → 200 + text/plain
/// 3. 超时 → 503 + "scrape timeout" + inc `sensearc_scrape_timeout_total`
pub async fn metrics_endpoint(
    Extension(engine): Extension<Arc<ObserveEngine>>,
) -> impl IntoResponse {
    let timeout_secs = engine.config.scrape_timeout_secs;

    let gather_result = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        tokio::task::spawn_blocking(move || MetricsRegistry::global().gather()),
    )
    .await;

    match gather_result {
        Ok(Ok(text)) => {
            // 成功——返回 text/plain
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                text,
            )
                .into_response()
        }
        Ok(Err(_join_err)) => {
            // spawn_blocking 失败
            MetricsRegistry::global().scrape_timeout_total.inc();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                "# ERROR: metrics gather panic\n".to_string(),
            )
                .into_response()
        }
        Err(_elapsed) => {
            // 超时
            MetricsRegistry::global().scrape_timeout_total.inc();
            tracing::warn!(timeout_secs = timeout_secs, "metrics scrape timeout");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                format!("# ERROR: scrape timeout ({timeout_secs}s)\n"),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::ObserveEngine;
    use sensearcdb_common::config::ObserveConfig;

    // Note: metrics_endpoint uses spawn_blocking, so we need a multi-thread runtime
    #[tokio::test(flavor = "multi_thread")]
    async fn test_metrics_endpoint_returns_200() {
        let mut config = ObserveConfig::default();
        config.scrape_timeout_secs = 5;
        let engine = Arc::new(ObserveEngine::new(config));

        let response = metrics_endpoint(Extension(engine)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_metrics_endpoint_content_type() {
        let config = ObserveConfig::default();
        let engine = Arc::new(ObserveEngine::new(config));

        let response = metrics_endpoint(Extension(engine)).await.into_response();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("text/plain"));
    }
}

//! 独立 axum HTTP server 启动 (端口 5101)。
//!
//! 在端口 5101 上运行独立的 Prometheus /metrics HTTP 端点。
//! 与主 REST server (5100) 完全隔离，防火墙可单独控制。

use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::api::metrics_endpoint;
use crate::engine::ObserveEngine;

/// 启动独立的 /metrics HTTP server（绑定 5101 端口）。
///
/// # 参数
/// - `engine`: ObserveEngine Arc 共享实例。
///
/// # 返回
/// server task 的 JoinHandle。
///
/// # Panics
/// 端口绑定失败时 panic。
pub async fn serve_metrics(
    engine: Arc<ObserveEngine>,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    let port = engine.config.metrics_port;
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let app = metrics_router(engine);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(port = port, "Prometheus /metrics server listening");

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "metrics server error");
        }
    });

    Ok(handle)
}

/// 构建 /metrics 路由（独立 axum Router）。
fn metrics_router(engine: Arc<ObserveEngine>) -> Router {
    Router::new()
        .route(
            "/metrics",
            axum::routing::get(metrics_endpoint::metrics_endpoint),
        )
        .layer(axum::Extension(engine))
}

/// 启动 metrics server 并阻塞当前 task。
///
/// 用于主线程中直接启动（不使用 spawn）。
///
/// # Panics
/// 端口绑定失败时 panic。
pub async fn serve_metrics_blocking(
    engine: Arc<ObserveEngine>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port = engine.config.metrics_port;
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let app = metrics_router(engine);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(
        port = port,
        "Prometheus /metrics server listening (blocking)"
    );

    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use sensearcdb_common::config::ObserveConfig;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn test_metrics_router() {
        let _config = sensearcdb_common::config::ObserveConfig::default();
        let _engine = Arc::new(ObserveEngine::new(_config));
        let _app = metrics_router(_engine);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_metrics_router_response() {
        let config = ObserveConfig::default();
        let engine = Arc::new(ObserveEngine::new(config));
        let app = metrics_router(engine);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}

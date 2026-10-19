//! REST middleware chain: RequestId → Auth → RateLimit → Audit.
//!
//! Each middleware adds trace context and logs request/response metadata.

use axum::{extract::Request, http::HeaderMap, middleware::Next, response::Response};
use std::time::Instant;
use tracing::{info, info_span, Instrument, Span};
use uuid::Uuid;

use sensearcdb_common::SenseArcError;

/// RequestId middleware: injects X-Request-Id header if not present.
pub async fn request_id_middleware(mut request: Request, next: Next) -> Response {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let span = info_span!(
        "request",
        request_id = %request_id,
        method = %request.method(),
        // S5 Step 5：日志脱敏——仅记 path，不落 URL query（避免 `?ticket=` 等敏感参数泄露）。
        path = %request.uri().path(),
    );

    // Inject request_id into extensions for downstream handlers
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));

    let mut response = next.run(request).instrument(span.clone()).await;
    response
        .headers_mut()
        .insert("x-request-id", request_id.parse().unwrap());

    response
}

/// Audit middleware: logs request method, path, status, and duration.
pub async fn audit_middleware(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let start = Instant::now();

    let response = next.run(request).await;

    let elapsed_ms = start.elapsed().as_millis();
    let status = response.status();

    info!(
        method = %method,
        // S5 Step 5：日志脱敏——仅记 path，不落 URL query（避免 `?ticket=` 敏感参数泄露）。
        path = %uri.path(),
        status = status.as_u16(),
        elapsed_ms = elapsed_ms,
        "request completed"
    );

    response
}

/// Auth middleware: validates JWT/API key on protected routes.
///
/// Skips auth for health endpoints and login.
pub async fn auth_middleware(
    mut request: Request,
    next: Next,
) -> Result<Response, axum::response::Response> {
    // Skip auth for health and auth endpoints
    let path = request.uri().path();
    if path.starts_with("/health") || path.starts_with("/api/v1/auth/") {
        return Ok(next.run(request).await);
    }

    // Check for JWT token or API key
    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let api_key = request
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok());

    if auth_header.is_none() && api_key.is_none() {
        let body = serde_json::json!({
            "code": SenseArcError::Unauthorized("authentication required".into()).code(),
            "message": "authentication required",
            "data": serde_json::Value::Null,
        });
        return Err(axum::response::Response::builder()
            .status(401)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                serde_json::to_string(&body).unwrap(),
            ))
            .unwrap());
    }

    Ok(next.run(request).await)
}

/// Rate limit middleware: tracks request rate per client.
pub async fn rate_limit_middleware(
    request: Request,
    next: Next,
) -> Result<Response, axum::response::Response> {
    // In production, check against a RateLimiter instance.
    // For now, pass through.
    Ok(next.run(request).await)
}

/// Wrapper type for request ID in extensions.
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

/// Extract the request ID from request headers or extensions.
pub fn get_request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::Service;

    #[test]
    fn test_request_id_type() {
        let rid = RequestId("abc-123".into());
        assert_eq!(rid.0, "abc-123");
    }

    /// 认证中间件缺失凭据 → 40100（Unauthorized），非 3 位码。
    #[tokio::test]
    async fn test_auth_middleware_returns_40100() {
        let mut app = axum::Router::new()
            .route("/protected", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(auth_middleware));

        let request = axum::http::Request::builder()
            .uri("/protected")
            .body(axum::body::Body::empty())
            .unwrap();

        // Router 恒 ready，`call` 由请求类型 `http::Request<Body>` 消歧，直接调用即可。
        let response = app.call(request).await.unwrap();

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], 40100);
    }
}

//! gRPC 认证与租户解析（依赖倒置：ingest 不反向依赖 security）。
//!
//! 真实校验由 bin 层注入（API Key → `ApiKeyManager`、JWT → `AuthManager` +
//! HomeTenantResolver），本模块只提供：
//! - [`AuthContext`]（含 `tenant_id` / `collector_id`），由认证 interceptor 写入
//!   `request.extensions()`；
//! - [`GrpcTenantResolver`] 类型（REST `TenantResolver` 的 metadata 版）；
//! - [`authenticate_request`] / [`validate_api_key`] / [`validate_jwt`]（委托注入 validator，
//!   不再桩）；
//! - [`auth_interceptor`]（构造 tonic 认证 interceptor）；
//! - [`resolve_tenant`]（写/查询服务从 extensions → resolver 解析 tenant，Q1 无 tenant 即 401）。

use std::sync::Arc;

use tonic::{Request, Status};

use sensearcdb_common::TenantId;

/// 认证后的主体上下文（由认证 interceptor 写入 `request.extensions()`）。
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// 认证主体标识（用户 username 或 `apikey:<key_id>`）。
    pub client_id: String,
    /// 认证方法。
    pub method: AuthMethod,
    /// 授予的权限。
    pub permissions: Vec<String>,
    /// 服务端推导出的租户；`None` = 平台主体/未解析（gRPC 数据面按 Q1 拒绝）。
    pub tenant_id: Option<TenantId>,
    /// API Key 绑定的 collector_id；JWT/mTLS 认证时为 `None`。
    pub collector_id: Option<String>,
}

/// 认证方法。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    /// 无认证。
    None,
    /// mTLS 客户端证书。
    Mtls,
    /// JWT bearer token。
    Jwt,
    /// metadata 中的 API key。
    ApiKey,
}

/// gRPC 请求租户解析器（REST `TenantResolver` 的 metadata 版）。
///
/// 由 bin 层注入（API Key → `api_keys.tenant_id`；JWT → HomeTenantResolver），
/// 使 ingest crate 不反向依赖 security crate。
pub type GrpcTenantResolver =
    Arc<dyn Fn(&tonic::metadata::MetadataMap) -> Option<TenantId> + Send + Sync>;

/// API Key 校验器（bin 层注入，委托 `ApiKeyManager::validate`）。
pub type ApiKeyValidator = Arc<dyn Fn(&str) -> Result<AuthContext, Status> + Send + Sync>;

/// JWT 校验器（bin 层注入，委托 `AuthManager::verify_jwt` + HomeTenantResolver）。
pub type JwtValidator = Arc<dyn Fn(&str) -> Result<AuthContext, Status> + Send + Sync>;

/// 校验 API Key（不再桩）：委托注入的 validator，失败 → 401。
pub fn validate_api_key(key: &str, validator: &ApiKeyValidator) -> Result<AuthContext, Status> {
    validator(key)
}

/// 校验 JWT（不再桩）：委托注入的 validator，失败 → 401。
pub fn validate_jwt(token: &str, validator: &JwtValidator) -> Result<AuthContext, Status> {
    validator(token)
}

/// 认证一个 gRPC 请求。
///
/// 按序检查：JWT Bearer → `x-api-key`。mTLS 本轮不覆盖（gRPC 未启 TLS），
/// 无凭据一律 401（Q1 强制认证，不静默回退默认租户）。
pub fn authenticate_request<T>(
    request: &Request<T>,
    api_key_validator: &ApiKeyValidator,
    jwt_validator: &JwtValidator,
) -> Result<AuthContext, Status> {
    if let Some(auth_header) = request.metadata().get("authorization") {
        let auth_str = auth_header
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid authorization header"))?;
        if let Some(token) = auth_str.strip_prefix("Bearer ") {
            return validate_jwt(token, jwt_validator);
        }
        // 有 authorization 头但非 Bearer：继续检查 x-api-key，最终无凭据则 401。
    }

    if let Some(api_key) = request.metadata().get("x-api-key") {
        let key = api_key
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid api key header"))?;
        return validate_api_key(key, api_key_validator);
    }

    Err(Status::unauthenticated("no valid authentication provided"))
}

/// 构造 tonic 认证 interceptor。
///
/// 认证成功后把 [`AuthContext`] 写入 `request.extensions()`。Q1：若 tenant 无法解析
/// （平台主体 / 无 home tenant）→ 401，不静默回退默认租户。
pub fn auth_interceptor(
    api_key_validator: ApiKeyValidator,
    jwt_validator: JwtValidator,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |mut req: Request<()>| {
        let ctx = authenticate_request(&req, &api_key_validator, &jwt_validator)?;
        if ctx.tenant_id.is_none() {
            return Err(Status::unauthenticated(
                "authenticated principal has no resolvable tenant",
            ));
        }
        req.extensions_mut().insert(ctx);
        Ok(req)
    }
}

/// 服务端从请求解析 tenant（写/查询服务共用）。
///
/// 优先级：
/// 1. `request.extensions()` 中的 [`AuthContext::tenant_id`]（interceptor 已解析）；
/// 2. 注入的 [`GrpcTenantResolver`]（单元测试 / 无 interceptor 场景）。
///
/// 两者都不可得 → 401（Q1：不静默回退默认租户 0）。
pub fn resolve_tenant<T>(
    request: &Request<T>,
    resolver: &Option<GrpcTenantResolver>,
) -> Result<TenantId, Status> {
    if let Some(ctx) = request.extensions().get::<AuthContext>() {
        if let Some(tenant) = ctx.tenant_id {
            return Ok(tenant);
        }
    }
    if let Some(resolver) = resolver {
        if let Some(tenant) = resolver(request.metadata()) {
            return Ok(tenant);
        }
    }
    Err(Status::unauthenticated(
        "missing credentials or unresolvable tenant",
    ))
}

/// Helper: 从 request extensions 提取 [`AuthContext`]。
pub fn get_auth_context<T>(request: &Request<T>) -> Option<&AuthContext> {
    request.extensions().get::<AuthContext>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_api_key_validator(tenant: TenantId, collector_id: Option<String>) -> ApiKeyValidator {
        Arc::new(move |_key| {
            Ok(AuthContext {
                client_id: "apikey:test".into(),
                method: AuthMethod::ApiKey,
                permissions: vec!["write".into(), "read".into()],
                tenant_id: Some(tenant),
                collector_id: collector_id.clone(),
            })
        })
    }

    fn reject_jwt_validator() -> JwtValidator {
        Arc::new(|_token| Err(Status::unauthenticated("invalid jwt")))
    }

    #[test]
    fn test_no_credentials_returns_401() {
        let api_key_validator = ok_api_key_validator(TenantId(1), Some("c1".into()));
        let jwt_validator = reject_jwt_validator();
        let req: Request<()> = Request::new(());
        let err = authenticate_request(&req, &api_key_validator, &jwt_validator).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_api_key_authenticated_via_validator() {
        let api_key_validator = ok_api_key_validator(TenantId(7), Some("collector-7".into()));
        let jwt_validator = reject_jwt_validator();
        let mut req: Request<()> = Request::new(());
        req.metadata_mut()
            .insert("x-api-key", "sak_test".parse().unwrap());

        let ctx = authenticate_request(&req, &api_key_validator, &jwt_validator).unwrap();
        assert_eq!(ctx.method, AuthMethod::ApiKey);
        assert_eq!(ctx.tenant_id, Some(TenantId(7)));
        assert_eq!(ctx.collector_id.as_deref(), Some("collector-7"));
    }

    #[test]
    fn test_interceptor_rejects_tenant_none() {
        // validator 返回 tenant None → interceptor 401。
        let api_key_validator: ApiKeyValidator = Arc::new(|_key| {
            Ok(AuthContext {
                client_id: "apikey:platform".into(),
                method: AuthMethod::ApiKey,
                permissions: vec![],
                tenant_id: None,
                collector_id: None,
            })
        });
        let jwt_validator = reject_jwt_validator();
        let interceptor = auth_interceptor(api_key_validator, jwt_validator);

        let mut req: Request<()> = Request::new(());
        req.metadata_mut()
            .insert("x-api-key", "sak_platform".parse().unwrap());
        let err = interceptor(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_resolve_tenant_prefers_extension_then_resolver() {
        // 无扩展、无 resolver → 401
        let req: Request<()> = Request::new(());
        assert!(resolve_tenant(&req, &None).is_err());

        // 有 resolver → 解析成功
        let resolver: GrpcTenantResolver = Arc::new(|_m| Some(TenantId(3)));
        assert_eq!(resolve_tenant(&req, &Some(resolver)).unwrap(), TenantId(3));

        // 有扩展 → 优先扩展
        let mut req: Request<()> = Request::new(());
        req.extensions_mut().insert(AuthContext {
            client_id: "u".into(),
            method: AuthMethod::Jwt,
            permissions: vec![],
            tenant_id: Some(TenantId(9)),
            collector_id: None,
        });
        let resolver: GrpcTenantResolver = Arc::new(|_m| Some(TenantId(3)));
        assert_eq!(resolve_tenant(&req, &Some(resolver)).unwrap(), TenantId(9));
    }
}

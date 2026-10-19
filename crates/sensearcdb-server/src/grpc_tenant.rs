//! 单机版 gRPC 认证降级（Open Core）。
//!
//! 完整版经 `build_grpc_auth_interceptor`（JWT/API Key 真实校验，无凭据 401）。
//! 单机版对标 InfluxDB Core：**无认证边界**，把 `x-tenant-id` metadata 解析为
//! 租户上下文注入 `request.extensions()`，永不 401。
//!
//! 复用 `sensearcdb_ingest::grpc::auth` 的 [`AuthContext`] / [`AuthMethod`] /
//! [`GrpcTenantResolver`]（依赖倒置，零 security 依赖）。

use std::sync::Arc;

use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use sensearcdb_common::TenantId;
use sensearcdb_ingest::grpc::auth::{AuthContext, AuthMethod, GrpcTenantResolver};

/// 从 gRPC metadata 解析 `x-tenant-id`（可选；缺省/非法回退 [`TenantId::DEFAULT`]）。
fn resolve_single_node_tenant(metadata: &MetadataMap) -> TenantId {
    metadata
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        .map(TenantId)
        .unwrap_or(TenantId::DEFAULT)
}

/// 单机版认证 interceptor：读 `x-tenant-id` → 构造 [`AuthContext`] 写入 extensions，
/// 永不 401。挂到 5201 WriteService / 5202 QueryService / 5206 SnapshotService。
pub fn build_single_node_interceptor(
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |mut req: Request<()>| {
        let tenant = resolve_single_node_tenant(req.metadata());
        let ctx = AuthContext {
            client_id: "single-node".to_string(),
            method: AuthMethod::None,
            permissions: vec!["write".into(), "read".into()],
            tenant_id: Some(tenant),
            collector_id: None,
        };
        req.extensions_mut().insert(ctx);
        Ok(req)
    }
}

/// 单机版租户解析器（注入 gRPC 服务兜底闭包，与 5201/5202/5206 共用）。
///
/// 返回类型 [`GrpcTenantResolver`] 与 snapshot 的 `SnapshotTenantResolver`
/// 结构同型（都是 `Arc<dyn Fn(&MetadataMap) -> Option<TenantId> + Send + Sync>`），
/// 可直接传入 `SnapshotGrpcService::with_tenant_resolver`。
pub fn build_single_node_tenant_resolver() -> GrpcTenantResolver {
    Arc::new(|metadata| Some(resolve_single_node_tenant(metadata)))
}

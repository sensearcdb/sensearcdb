//! gRPC management service for derived tags.
//!
//! Provides a gRPC interface for managing derived tags. This module
//! integrates with the tonic/prost gRPC framework.
//!
//! Note: The actual proto-generated types are expected to be defined
//! in a separate proto file. This module provides the service implementation
//! that would be invoked by the generated server trait.
//!
//! In a full deployment, the proto file would be:
//! ```protobuf
//! service DerivedTagAdmin {
//!   rpc RegisterTag(RegisterTagRequest) returns (RegisterTagResponse);
//!   rpc UnregisterTag(UnregisterTagRequest) returns (UnregisterTagResponse);
//!   rpc GetTag(GetTagRequest) returns (GetTagResponse);
//!   rpc ListTags(ListTagsRequest) returns (ListTagsResponse);
//!   rpc ValidateExpression(ValidateExpressionRequest) returns (ValidateExpressionResponse);
//! }
//! ```

use crate::deadband::DeadbandConfig;
use crate::engine::DerivedTagEngine;
use crate::fallback::FallbackPolicy;
use sensearcdb_common::TenantId;
use std::collections::HashMap;
use std::sync::Arc;

// ─── gRPC Service Implementation ───

/// gRPC service for derived tag management.
///
/// Wraps the [`DerivedTagEngine`] to provide remote management capabilities.
pub struct DerivedTagGrpcService {
    engine: Arc<DerivedTagEngine>,
}

impl DerivedTagGrpcService {
    /// Create a new gRPC service.
    pub fn new(engine: Arc<DerivedTagEngine>) -> Self {
        Self { engine }
    }
}

// ─── Request / Response structs (mirrors proto definitions) ───

/// gRPC request: register a derived tag.
#[derive(Debug, Clone)]
pub struct RegisterTagRequest {
    pub table_id: u32,
    pub tag_name: String,
    pub expression: String,
    pub fallback_policy: GrpcFallbackPolicy,
    pub deadband_abs: f64,
    pub deadband_rel: f64,
    pub deadband_max_time_secs: u32,
}

/// gRPC fallback policy enum.
#[derive(Debug, Clone)]
pub enum GrpcFallbackPolicy {
    Strict,
    Lenient { last_good_max_age_secs: u32 },
    Coalesce { defaults: HashMap<String, f64> },
}

impl From<GrpcFallbackPolicy> for FallbackPolicy {
    fn from(p: GrpcFallbackPolicy) -> Self {
        match p {
            GrpcFallbackPolicy::Strict => FallbackPolicy::Strict,
            GrpcFallbackPolicy::Lenient {
                last_good_max_age_secs,
            } => FallbackPolicy::Lenient {
                last_good_max_age_secs,
            },
            GrpcFallbackPolicy::Coalesce { defaults } => FallbackPolicy::Coalesce { defaults },
        }
    }
}

/// gRPC response: register result.
#[derive(Debug, Clone)]
pub struct RegisterTagResponse {
    pub success: bool,
    pub error_message: String,
}

/// gRPC request: unregister a derived tag.
#[derive(Debug, Clone)]
pub struct UnregisterTagRequest {
    pub tag_name: String,
}

/// gRPC response: unregister result.
#[derive(Debug, Clone)]
pub struct UnregisterTagResponse {
    pub success: bool,
    pub cascade_disabled: Vec<String>,
    pub error_message: String,
}

/// gRPC request: get a derived tag.
#[derive(Debug, Clone)]
pub struct GetTagRequest {
    pub tag_name: String,
}

/// gRPC response: tag info.
#[derive(Debug, Clone)]
pub struct GetTagResponse {
    pub found: bool,
    pub tag_name: String,
    pub table_id: u32,
    pub source_tags: Vec<String>,
    pub expression_hash: u64,
    pub state: String,
}

/// gRPC request: list tags.
#[derive(Debug, Clone)]
pub struct ListTagsRequest {
    pub offset: u32,
    pub limit: u32,
}

/// gRPC response: list tags.
#[derive(Debug, Clone)]
pub struct ListTagsResponse {
    pub tags: Vec<GetTagResponse>,
    pub total: u32,
}

/// gRPC request: validate expression.
#[derive(Debug, Clone)]
pub struct ValidateExpressionRequest {
    pub expression: String,
    pub max_depth: u32,
}

/// gRPC response: validation result.
#[derive(Debug, Clone)]
pub struct ValidateExpressionResponse {
    pub valid: bool,
    pub source_tags: Vec<String>,
    pub ast_display: String,
    pub error: String,
}

// ─── Service Methods ───

impl DerivedTagGrpcService {
    /// Handle a RegisterTag gRPC call.
    pub fn register_tag(&self, req: RegisterTagRequest) -> RegisterTagResponse {
        let deadband = DeadbandConfig {
            abs: req.deadband_abs,
            rel: req.deadband_rel,
            max_time_secs: req.deadband_max_time_secs,
        };

        match self.engine.register_tag(
            req.table_id,
            &req.tag_name,
            &req.expression,
            req.fallback_policy.into(),
            deadband,
        ) {
            Ok(()) => RegisterTagResponse {
                success: true,
                error_message: String::new(),
            },
            Err(e) => RegisterTagResponse {
                success: false,
                error_message: format!("{}", e),
            },
        }
    }

    /// Handle an UnregisterTag gRPC call.
    pub fn unregister_tag(&self, req: UnregisterTagRequest) -> UnregisterTagResponse {
        match self.engine.unregister_tag(TenantId::DEFAULT, &req.tag_name) {
            Ok(cascade) => UnregisterTagResponse {
                success: true,
                cascade_disabled: cascade,
                error_message: String::new(),
            },
            Err(e) => UnregisterTagResponse {
                success: false,
                cascade_disabled: Vec::new(),
                error_message: format!("{}", e),
            },
        }
    }

    /// Handle a GetTag gRPC call.
    pub fn get_tag(&self, req: GetTagRequest) -> GetTagResponse {
        let graph = self.engine.dep_graph().read();
        match graph.get(TenantId::DEFAULT, &req.tag_name) {
            Some(node) => GetTagResponse {
                found: true,
                tag_name: node.name.clone(),
                table_id: node.table_id,
                source_tags: node.source_tags.clone(),
                expression_hash: node.expression_hash,
                state: format!("{:?}", node.state),
            },
            None => GetTagResponse {
                found: false,
                tag_name: req.tag_name,
                table_id: 0,
                source_tags: vec![],
                expression_hash: 0,
                state: "NotFound".into(),
            },
        }
    }

    /// Handle a ListTags gRPC call.
    pub fn list_tags(&self, req: ListTagsRequest) -> ListTagsResponse {
        let graph = self.engine.dep_graph().read();
        let all_names = graph.all_node_names();
        let total = all_names.len() as u32;

        let tags: Vec<GetTagResponse> = all_names
            .into_iter()
            .skip(req.offset as usize)
            .take(req.limit as usize)
            .filter_map(|name| {
                graph
                    .get(TenantId::DEFAULT, name)
                    .map(|node| GetTagResponse {
                        found: true,
                        tag_name: node.name.clone(),
                        table_id: node.table_id,
                        source_tags: node.source_tags.clone(),
                        expression_hash: node.expression_hash,
                        state: format!("{:?}", node.state),
                    })
            })
            .collect();

        ListTagsResponse { tags, total }
    }

    /// Handle a ValidateExpression gRPC call.
    pub fn validate_expression(
        &self,
        req: ValidateExpressionRequest,
    ) -> ValidateExpressionResponse {
        match DerivedTagEngine::validate_expression(&req.expression, req.max_depth as usize) {
            Ok((ast, source_tags)) => ValidateExpressionResponse {
                valid: true,
                source_tags,
                ast_display: format!("{}", ast),
                error: String::new(),
            },
            Err(e) => ValidateExpressionResponse {
                valid: false,
                source_tags: vec![],
                ast_display: String::new(),
                error: format!("{}", e),
            },
        }
    }
}

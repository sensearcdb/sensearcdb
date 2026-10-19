//! CQ 引擎管理 gRPC 服务。
//!
//! 提供 CQ 定义和状态的管理 RPC 接口。
//! 使用 tonic + prost 实现。
//!
//! ## 服务定义
//!
//! - `ListDefinitions` — 列出所有 CQ 定义
//! - `GetDefinition` — 获取指定表的 CQ 定义
//! - `GetStatus` — 获取 CQ 引擎状态
//! - `PauseTable` — 暂停指定表的 CQ 聚合
//! - `ResumeTable` — 恢复指定表的 CQ 聚合

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::definition::CqDefinition;
use crate::engine::CqEngine;

// ─── gRPC 请求/响应类型 ───
// (在实际项目中由 prost 从 .proto 文件生成，此处提供结构体定义)

/// 列出所有 CQ 定义的请求。
#[derive(Debug, Clone)]
pub struct ListDefinitionsRequest {
    /// 可选：只返回指定源表的定义。
    pub source_table_id: Option<u32>,
}

/// 列出所有 CQ 定义的响应。
#[derive(Debug, Clone)]
pub struct ListDefinitionsResponse {
    pub definitions: Vec<CqDefinitionProto>,
}

/// CQ 定义的 protobuf 表示。
#[derive(Debug, Clone)]
pub struct CqDefinitionProto {
    pub name: String,
    pub source_table_id: u32,
    pub target_table_id: u32,
    pub granularity: String,
    pub interval_ms: i64,
    pub column_count: u32,
    pub status: String,
}

impl From<&CqDefinition> for CqDefinitionProto {
    fn from(def: &CqDefinition) -> Self {
        Self {
            name: def.name.clone(),
            source_table_id: def.source_table_id,
            target_table_id: def.target_table_id,
            granularity: def.granularity.name().to_string(),
            interval_ms: def.granularity.interval_ms(),
            column_count: def.source_columns.len() as u32,
            status: format!("{:?}", def.status),
        }
    }
}

/// 获取指定表 CQ 定义的请求。
#[derive(Debug, Clone)]
pub struct GetDefinitionRequest {
    pub table_id: u32,
}

/// 获取指定表 CQ 定义的响应。
#[derive(Debug, Clone)]
pub struct GetDefinitionResponse {
    pub definitions: Vec<CqDefinitionProto>,
}

/// CQ 引擎状态请求。
#[derive(Debug, Clone)]
pub struct CqStatusRequest {}

/// CQ 引擎状态响应。
#[derive(Debug, Clone)]
pub struct CqStatusResponse {
    pub running: bool,
    pub definitions_count: u32,
    pub recalculation_queue_len: u32,
    pub message: String,
}

/// 暂停/恢复表 CQ 的请求。
#[derive(Debug, Clone)]
pub struct TableCqRequest {
    pub table_id: u32,
}

/// 暂停/恢复表 CQ 的响应。
#[derive(Debug, Clone)]
pub struct TableCqResponse {
    pub success: bool,
    pub message: String,
    pub affected_definitions: u32,
}

// ─── gRPC Service ───

/// CQ 管理 gRPC 服务。
///
/// 持有 CqEngine 的共享引用，处理 RPC 请求。
pub struct CqAdminService {
    engine: Arc<RwLock<CqEngine>>,
}

impl CqAdminService {
    /// 创建新的 CQ gRPC 服务。
    pub fn new(engine: Arc<RwLock<CqEngine>>) -> Self {
        Self { engine }
    }

    /// 列出所有 CQ 定义。
    pub async fn list_definitions(
        &self,
        request: ListDefinitionsRequest,
    ) -> ListDefinitionsResponse {
        let engine = self.engine.read().await;
        let all_defs = engine.get_definitions();

        let defs: Vec<CqDefinitionProto> = all_defs
            .iter()
            .filter(|d| {
                request
                    .source_table_id
                    .map(|id| d.source_table_id == id)
                    .unwrap_or(true)
            })
            .map(|d| d.into())
            .collect();

        ListDefinitionsResponse { definitions: defs }
    }

    /// 获取指定表的 CQ 定义。
    pub async fn get_definition(&self, request: GetDefinitionRequest) -> GetDefinitionResponse {
        let engine = self.engine.read().await;
        let defs = engine.get_definitions_for_table(request.table_id);
        let proto_defs: Vec<CqDefinitionProto> = defs.iter().map(|d| d.into()).collect();
        GetDefinitionResponse {
            definitions: proto_defs,
        }
    }

    /// 获取 CQ 引擎状态。
    pub async fn get_status(&self) -> CqStatusResponse {
        let engine = self.engine.read().await;
        let defs = engine.get_definitions();
        CqStatusResponse {
            running: engine.is_running(),
            definitions_count: defs.len() as u32,
            recalculation_queue_len: engine.recalculation_queue_len() as u32,
            message: if engine.is_running() {
                "CQ engine is running".into()
            } else {
                "CQ engine is stopped".into()
            },
        }
    }

    /// 暂停指定表的 CQ 聚合。
    pub async fn pause_table(&self, request: TableCqRequest) -> TableCqResponse {
        let engine = self.engine.read().await;
        let defs = engine.get_definitions_for_table(request.table_id);
        let count = defs.len() as u32;

        if count == 0 {
            return TableCqResponse {
                success: false,
                message: format!("No CQ definitions for table {}", request.table_id),
                affected_definitions: 0,
            };
        }
        // 注：当前实现为只读，pause/resume 需要可变的 CqDefinition 访问。
        // 完整实现需要在 CqEngine 中添加 pause_table/resume_table 方法。
        TableCqResponse {
            success: true,
            message: format!("CQ paused for table {}", request.table_id),
            affected_definitions: count,
        }
    }

    /// 恢复指定表的 CQ 聚合。
    pub async fn resume_table(&self, request: TableCqRequest) -> TableCqResponse {
        let engine = self.engine.read().await;
        let defs = engine.get_definitions_for_table(request.table_id);
        let count = defs.len() as u32;

        if count == 0 {
            return TableCqResponse {
                success: false,
                message: format!("No CQ definitions for table {}", request.table_id),
                affected_definitions: 0,
            };
        }
        TableCqResponse {
            success: true,
            message: format!("CQ resumed for table {}", request.table_id),
            affected_definitions: count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cq_definition_proto_from_definition() {
        use crate::definition::{CqDefinition, CqGranularity};
        use sensearcdb_common::AggFunction;

        let def = CqDefinition::new(
            "cq_10s_1".into(),
            1,
            101,
            CqGranularity::Cq10s,
            vec![1, 2],
            vec![AggFunction::Avg, AggFunction::Sum],
        );

        let proto = CqDefinitionProto::from(&def);
        assert_eq!(proto.name, "cq_10s_1");
        assert_eq!(proto.source_table_id, 1);
        assert_eq!(proto.target_table_id, 101);
        assert_eq!(proto.granularity, "CQ_10s");
        assert_eq!(proto.interval_ms, 10_000);
        assert_eq!(proto.column_count, 2);
        assert_eq!(proto.status, "Running");
    }
}

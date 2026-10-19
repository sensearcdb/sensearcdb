//! 流计算引擎管理 gRPC 服务。
//!
//! 提供流计算任务的管理 RPC 接口。
//! 使用 tonic + prost 实现。
//!
//! ## 服务方法
//!
//! - `ListTasks` — 列出所有流计算任务
//! - `GetTask` — 获取指定任务详情
//! - `CreateTask` — 创建任务（提交 SQL）
//! - `DeleteTask` — 删除任务
//! - `PauseTask` — 暂停任务
//! - `ResumeTask` — 恢复任务
//! - `GetStatus` — 获取引擎状态

use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

use sensearcdb_common::DataType;

use crate::engine::StreamComputeEngine;
use crate::sql_parser::{parse_stream_sql, StreamSqlStatement};
use crate::{stream_result_type, StreamCatalog};

// ─── gRPC 请求/响应类型 ───
// (在实际项目中由 prost 从 .proto 文件生成，此处提供结构体定义)

/// 列出所有任务的请求。
#[derive(Debug, Clone)]
pub struct ListStreamTasksRequest {
    /// 可选：过滤条件（按状态）
    pub status: Option<String>,
}

/// 列出所有任务的响应。
#[derive(Debug, Clone)]
pub struct ListStreamTasksResponse {
    pub tasks: Vec<StreamTaskProto>,
}

/// 流计算任务的 protobuf 表示。
#[derive(Debug, Clone)]
pub struct StreamTaskProto {
    pub task_id: String,
    pub name: String,
    pub source_table_id: u32,
    pub target_table_id: u32,
    pub window_type: String,
    pub window_spec_json: String,
    pub status: String,
    pub column_count: u32,
    pub created_at: i64,
}

/// 获取任务详情的请求。
#[derive(Debug, Clone)]
pub struct GetStreamTaskRequest {
    pub task_id: String,
}

/// 获取任务详情的响应。
#[derive(Debug, Clone)]
pub struct GetStreamTaskResponse {
    pub task: Option<StreamTaskProto>,
}

/// 创建任务的请求。
#[derive(Debug, Clone)]
pub struct CreateStreamTaskRequest {
    /// SQL 语句
    pub sql: String,
}

/// 创建任务的响应。
#[derive(Debug, Clone)]
pub struct CreateStreamTaskResponse {
    pub success: bool,
    pub task_id: String,
    pub name: String,
    pub message: String,
}

/// 删除/暂停/恢复任务的通用请求。
#[derive(Debug, Clone)]
pub struct StreamTaskActionRequest {
    pub task_id: String,
}

/// 删除/暂停/恢复任务的通用响应。
#[derive(Debug, Clone)]
pub struct StreamTaskActionResponse {
    pub success: bool,
    pub message: String,
}

/// 引擎状态请求。
#[derive(Debug, Clone)]
pub struct StreamStatusRequest {}

/// 引擎状态响应。
#[derive(Debug, Clone)]
pub struct StreamStatusResponse {
    pub running: bool,
    pub total_tasks: u32,
    pub running_tasks: u32,
    pub paused_tasks: u32,
    pub error_tasks: u32,
    pub total_active_partitions: u32,
    pub config_enabled: bool,
    pub dropped_events: u64,
    pub message: String,
}

// ─── 类型转换 ───

impl From<&crate::task::StreamTask> for StreamTaskProto {
    fn from(task: &crate::task::StreamTask) -> Self {
        let window_spec_json =
            serde_json::to_string(&task.window_spec).unwrap_or_else(|_| "{}".into());

        Self {
            task_id: task.id.to_string(),
            name: task.name.clone(),
            source_table_id: task.source_table_id,
            target_table_id: task.target_table_id,
            window_type: task.window_spec.type_name().to_string(),
            window_spec_json,
            status: format!("{:?}", task.status),
            column_count: task.projection.columns.len() as u32,
            created_at: task.created_at,
        }
    }
}

// ─── gRPC Service ───

/// 流计算管理 gRPC 服务。
///
/// 持有 StreamComputeEngine 的共享引用，处理 RPC 请求。
pub struct StreamAdminService {
    engine: Arc<TokioRwLock<StreamComputeEngine>>,
    /// Catalog 解析器（必选）
    catalog: Arc<dyn StreamCatalog>,
}

impl StreamAdminService {
    /// 创建新的流计算 gRPC 服务。
    pub fn new(
        engine: Arc<TokioRwLock<StreamComputeEngine>>,
        catalog: Arc<dyn StreamCatalog>,
    ) -> Self {
        Self { engine, catalog }
    }

    /// 列出所有流计算任务。
    pub async fn list_tasks(&self, request: ListStreamTasksRequest) -> ListStreamTasksResponse {
        let engine = self.engine.read().await;
        let all_tasks = engine.get_tasks();

        let tasks: Vec<StreamTaskProto> = all_tasks
            .iter()
            .filter(|task| {
                if let Some(ref status_filter) = request.status {
                    format!("{:?}", task.status).eq_ignore_ascii_case(status_filter)
                } else {
                    true
                }
            })
            .map(|task| task.into())
            .collect();

        ListStreamTasksResponse { tasks }
    }

    /// 获取指定任务详情。
    pub async fn get_task(&self, request: GetStreamTaskRequest) -> GetStreamTaskResponse {
        let task_id = uuid::Uuid::parse_str(&request.task_id).ok();
        let engine = self.engine.read().await;

        let task = task_id.and_then(|id| engine.get_task(id));
        GetStreamTaskResponse {
            task: task.as_ref().map(|t| t.into()),
        }
    }

    /// 创建流计算任务（SQL 定义）。
    pub async fn create_task(&self, request: CreateStreamTaskRequest) -> CreateStreamTaskResponse {
        let stmt = match parse_stream_sql(&request.sql) {
            Ok(s) => s,
            Err(e) => {
                return CreateStreamTaskResponse {
                    success: false,
                    task_id: String::new(),
                    name: String::new(),
                    message: format!("SQL parse error: {}", e),
                };
            }
        };

        match stmt {
            StreamSqlStatement::CreateStream {
                name,
                source_table: source_table_name,
                target_table,
                window_spec,
                aggregations,
                partition_by,
                ..
            } => {
                let catalog = &self.catalog;

                // 1. 解析源表名 → source_table_id（真解析，失败显式报错）
                let source_table_id: u32 = match catalog
                    .resolve_table(sensearcdb_common::TenantId::DEFAULT, &source_table_name)
                {
                    Some(id) => id,
                    None => {
                        return CreateStreamTaskResponse {
                            success: false,
                            task_id: String::new(),
                            name: String::new(),
                            message: format!("source table not found: {}", source_table_name),
                        };
                    }
                };

                // 2. 解析聚合列 → source_column_id + 真实结果类型
                let mut projection_columns = Vec::with_capacity(aggregations.len());
                let mut target_schema = Vec::with_capacity(aggregations.len());
                let mut target_columns: Vec<(String, DataType)> =
                    Vec::with_capacity(aggregations.len());

                for (i, agg) in aggregations.iter().enumerate() {
                    let source_column: u16 =
                        match catalog.resolve_column(source_table_id, &agg.source_column_name) {
                            Some(cid) => cid,
                            None => {
                                return CreateStreamTaskResponse {
                                    success: false,
                                    task_id: String::new(),
                                    name: String::new(),
                                    message: format!(
                                        "source column not found: {}",
                                        agg.source_column_name
                                    ),
                                };
                            }
                        };
                    let source_type = catalog
                        .resolve_column_type(source_table_id, &agg.source_column_name)
                        .unwrap_or(DataType::F64);
                    let result_type = stream_result_type(agg.function, source_type);

                    projection_columns.push(crate::task::ProjectionColumn::Aggregate {
                        function: agg.function,
                        source_column,
                        alias: agg.alias.clone(),
                    });
                    target_schema.push(crate::task::TargetColumnDef {
                        // 0-based：与写回路径 EngineStreamSink 的位置映射（`i as u16`）对齐，
                        // 消除 off-by-one（聚合结果列按投影位序 0 起始落目标表列）。
                        column_id: i as u16,
                        column_name: agg.alias.clone(),
                        data_type: result_type,
                    });
                    target_columns.push((agg.alias.clone(), result_type));
                }

                // 3. 目标表名：优先用户指定 `INTO <target_table>`，否则 `{name}_stream`
                let target_table_name = target_table.unwrap_or_else(|| format!("{}_stream", name));

                // 4. 自动创建目标表（真实列类型）
                let target_table_id: u32 = match catalog.auto_create_target_table(
                    sensearcdb_common::TenantId::DEFAULT,
                    &target_table_name,
                    &target_columns,
                ) {
                    Ok(id) => id,
                    Err(e) => {
                        return CreateStreamTaskResponse {
                            success: false,
                            task_id: String::new(),
                            name: String::new(),
                            message: format!("failed to create target table: {}", e),
                        };
                    }
                };

                let projection = crate::task::ProjectionSpec {
                    columns: projection_columns,
                    target_schema,
                };

                // 5. 解析分区键列 → column_id（真解析，失败显式报错）
                let partition_spec = match partition_by {
                    Some(col_name) => match catalog.resolve_column(source_table_id, &col_name) {
                        Some(cid) => Some(crate::task::PartitionBySpec {
                            source_column: cid,
                            source_column_name: col_name,
                        }),
                        None => {
                            return CreateStreamTaskResponse {
                                success: false,
                                task_id: String::new(),
                                name: String::new(),
                                message: format!("partition column not found: {}", col_name),
                            };
                        }
                    },
                    None => None,
                };

                let mut engine = self.engine.write().await;
                match engine
                    .create_task(
                        name.clone(),
                        sensearcdb_common::TenantId::DEFAULT,
                        source_table_id,
                        target_table_id,
                        window_spec,
                        projection,
                        partition_spec,
                    )
                    .await
                {
                    Ok(task_id) => CreateStreamTaskResponse {
                        success: true,
                        task_id: task_id.to_string(),
                        name,
                        message: "stream task created successfully".into(),
                    },
                    Err(e) => CreateStreamTaskResponse {
                        success: false,
                        task_id: String::new(),
                        name: String::new(),
                        message: format!("failed to create: {}", e),
                    },
                }
            }
            _ => CreateStreamTaskResponse {
                success: false,
                task_id: String::new(),
                name: String::new(),
                message: "SQL must be CREATE STREAM COMPUTE ...".into(),
            },
        }
    }

    /// 删除流计算任务。
    pub async fn delete_task(&self, request: StreamTaskActionRequest) -> StreamTaskActionResponse {
        let task_id = match uuid::Uuid::parse_str(&request.task_id) {
            Ok(id) => id,
            Err(_) => {
                return StreamTaskActionResponse {
                    success: false,
                    message: format!("invalid task_id: {}", request.task_id),
                };
            }
        };

        let mut engine = self.engine.write().await;
        match engine.drop_task(task_id).await {
            Ok(()) => StreamTaskActionResponse {
                success: true,
                message: format!("stream task {} deleted", task_id),
            },
            Err(e) => StreamTaskActionResponse {
                success: false,
                message: format!("{}", e),
            },
        }
    }

    /// 暂停流计算任务。
    pub async fn pause_task(&self, request: StreamTaskActionRequest) -> StreamTaskActionResponse {
        let task_id = match uuid::Uuid::parse_str(&request.task_id) {
            Ok(id) => id,
            Err(_) => {
                return StreamTaskActionResponse {
                    success: false,
                    message: format!("invalid task_id: {}", request.task_id),
                };
            }
        };

        let mut engine = self.engine.write().await;
        match engine.pause_task(task_id).await {
            Ok(()) => StreamTaskActionResponse {
                success: true,
                message: format!("stream task {} paused", task_id),
            },
            Err(e) => StreamTaskActionResponse {
                success: false,
                message: format!("{}", e),
            },
        }
    }

    /// 恢复流计算任务。
    pub async fn resume_task(&self, request: StreamTaskActionRequest) -> StreamTaskActionResponse {
        let task_id = match uuid::Uuid::parse_str(&request.task_id) {
            Ok(id) => id,
            Err(_) => {
                return StreamTaskActionResponse {
                    success: false,
                    message: format!("invalid task_id: {}", request.task_id),
                };
            }
        };

        let mut engine = self.engine.write().await;
        match engine.resume_task(task_id).await {
            Ok(()) => StreamTaskActionResponse {
                success: true,
                message: format!("stream task {} resumed", task_id),
            },
            Err(e) => StreamTaskActionResponse {
                success: false,
                message: format!("{}", e),
            },
        }
    }

    /// 获取引擎状态。
    pub async fn get_status(&self) -> StreamStatusResponse {
        let engine = self.engine.read().await;
        let summary = engine.status_summary();

        StreamStatusResponse {
            running: summary.running,
            total_tasks: summary.total_tasks as u32,
            running_tasks: summary.running_tasks as u32,
            paused_tasks: summary.paused_tasks as u32,
            error_tasks: summary.error_tasks as u32,
            total_active_partitions: summary.total_active_partitions as u32,
            config_enabled: summary.config_enabled,
            dropped_events: summary.dropped_events,
            message: if summary.running {
                "Stream compute engine is running".into()
            } else {
                "Stream compute engine is stopped".into()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_store::StreamTaskStore;
    use sensearcdb_common::{AppConfig, ConfigHandle, TypedValue};

    /// 测试用 catalog：表名/列名 → 固定 ID 映射。
    struct TestCatalog;

    impl StreamCatalog for TestCatalog {
        fn resolve_table(
            &self,
            _tenant_id: sensearcdb_common::TenantId,
            table_name: &str,
        ) -> Option<u32> {
            match table_name {
                "t_sensors" => Some(1),
                _ => None,
            }
        }

        fn resolve_column(&self, table_id: u32, column_name: &str) -> Option<u16> {
            let _ = table_id;
            match column_name {
                "value" => Some(0),
                "temp" => Some(1),
                "device_id" => Some(2),
                _ => None,
            }
        }

        fn resolve_column_type(&self, table_id: u32, column_name: &str) -> Option<DataType> {
            let _ = table_id;
            match column_name {
                "value" | "temp" => Some(DataType::F64),
                "device_id" => Some(DataType::String),
                _ => None,
            }
        }

        fn auto_create_target_table(
            &self,
            _tenant_id: sensearcdb_common::TenantId,
            _table_name: &str,
            _columns: &[(String, DataType)],
        ) -> Result<u32, String> {
            Ok(1000)
        }

        fn table_exists(&self, _table_id: u32) -> bool {
            true
        }
    }

    fn make_test_store() -> Arc<StreamTaskStore> {
        let path =
            std::env::temp_dir().join(format!("test_stream_grpc_{}.db", uuid::Uuid::new_v4()));
        Arc::new(StreamTaskStore::open(&path).unwrap())
    }

    fn make_test_engine() -> Arc<TokioRwLock<StreamComputeEngine>> {
        let config = ConfigHandle::new(AppConfig::default());
        struct NoopSink;
        impl crate::StreamSink for NoopSink {
            fn write_batch(
                &self,
                _target_table_id: u32,
                _rows: &[(i64, Vec<TypedValue>)],
            ) -> Result<(), String> {
                Ok(())
            }
        }
        Arc::new(TokioRwLock::new(StreamComputeEngine::new(
            config,
            Arc::new(NoopSink),
            make_test_store(),
        )))
    }

    fn make_catalog() -> Arc<dyn StreamCatalog> {
        Arc::new(TestCatalog)
    }

    #[tokio::test]
    async fn test_get_status_empty() {
        let engine = make_test_engine();
        let service = StreamAdminService::new(engine, make_catalog());

        let status = service.get_status().await;
        assert_eq!(status.total_tasks, 0);
        assert_eq!(status.running_tasks, 0);
    }

    #[tokio::test]
    async fn test_list_tasks_empty() {
        let engine = make_test_engine();
        let service = StreamAdminService::new(engine, make_catalog());

        let resp = service
            .list_tasks(ListStreamTasksRequest { status: None })
            .await;
        assert!(resp.tasks.is_empty());
    }

    #[tokio::test]
    async fn test_get_task_not_found() {
        let engine = make_test_engine();
        let service = StreamAdminService::new(engine, make_catalog());

        let resp = service
            .get_task(GetStreamTaskRequest {
                task_id: uuid::Uuid::new_v4().to_string(),
            })
            .await;
        assert!(resp.task.is_none());
    }

    #[tokio::test]
    async fn test_create_task_resolves_catalog() {
        let engine = make_test_engine();
        let service = StreamAdminService::new(engine, make_catalog());

        let resp = service
            .create_task(CreateStreamTaskRequest {
                sql: "CREATE STREAM COMPUTE s1 ON t_sensors AS SELECT AVG(temp) FROM t_sensors PARTITION BY device_id WINDOW TYPE TUMBLING SIZE 10s INTO s1_target".into(),
            })
            .await;

        assert!(resp.success, "create failed: {}", resp.message);
        assert!(!resp.task_id.is_empty());
        assert_eq!(resp.name, "s1");
    }

    #[tokio::test]
    async fn test_create_task_unknown_source_fails() {
        let engine = make_test_engine();
        let service = StreamAdminService::new(engine, make_catalog());

        let resp = service
            .create_task(CreateStreamTaskRequest {
                sql: "CREATE STREAM COMPUTE s2 ON no_such_table AS SELECT AVG(value) FROM no_such_table WINDOW TYPE TUMBLING SIZE 10s".into(),
            })
            .await;

        assert!(!resp.success);
        assert!(resp.message.contains("source table not found"));
    }
}

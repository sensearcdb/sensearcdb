//! gRPC SchemaService 实现。
//!
//! 基于 tonic 框架，实现 `SchemaService` trait：
//! - `GetTable`: 按 table_id 查询表结构
//! - `ListTables`: 列出所有未删除的表
//! - `WatchSchema`: 流式推送 Schema 变更事件

use crate::event::SchemaEvent;
use crate::manager::SchemaManager;
use crate::mirror::TableType;
use crate::schema_proto::schema_service_server::SchemaService as SchemaServiceTrait;
use crate::schema_proto::{
    ColumnDef as ProtoColumn, DataType as ProtoDataType, DeadbandConfig as ProtoDeadband,
    DeadbandType as ProtoDeadbandType, GetTableRequest, GetTableResponse, ListTablesRequest,
    ListTablesResponse, SchemaChangeEvent, TableAltered, TableCreated, TableDeleted,
    TableInfo as ProtoTableInfo, TableType as ProtoTableType, WatchSchemaRequest,
};
use sensearcdb_common::DataType;
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

/// gRPC Schema 服务。
///
/// 封装 `Arc<SchemaManager>`，每次 RPC 调用委托给 SchemaManager。
pub struct SchemaService {
    manager: Arc<SchemaManager>,
}

impl SchemaService {
    /// 创建新的 gRPC Schema 服务。
    pub fn new(manager: Arc<SchemaManager>) -> Self {
        Self { manager }
    }
}

#[tonic::async_trait]
impl SchemaServiceTrait for SchemaService {
    /// 按 table_id 获取表结构。
    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let req = request.into_inner();
        debug!(table_id = req.table_id, "gRPC GetTable");

        let table_info = self
            .manager
            .get_table(req.table_id)
            .map(|info| table_info_to_proto(&info));

        Ok(Response::new(GetTableResponse { table: table_info }))
    }

    /// 列出所有未删除的表。
    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        debug!("gRPC ListTables");

        let tables: Vec<ProtoTableInfo> = self
            .manager
            .list_tables()
            .iter()
            .map(table_info_to_proto)
            .collect();

        Ok(Response::new(ListTablesResponse { tables }))
    }

    /// 流式推送 Schema 变更事件。
    ///
    /// 订阅 `SchemaEventBus`，将每个 `SchemaEvent` 转换为 proto `SchemaChangeEvent`
    /// 并通过 gRPC stream 推送给客户端。
    type WatchSchemaStream =
        tokio_stream::wrappers::ReceiverStream<Result<SchemaChangeEvent, Status>>;

    async fn watch_schema(
        &self,
        _request: Request<WatchSchemaRequest>,
    ) -> Result<Response<Self::WatchSchemaStream>, Status> {
        debug!("gRPC WatchSchema: new subscription");

        let mut event_rx = self.manager.event_bus.subscribe();
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            loop {
                match event_rx.recv().await {
                    Ok(event) => {
                        let proto_event = schema_event_to_proto(event);
                        if tx.send(Ok(proto_event)).await.is_err() {
                            // 客户端断开连接
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "gRPC WatchSchema lagged");
                        // 发送一个特殊的 TableAltered(0) 事件让客户端做全量刷新
                        let refresh_event = SchemaChangeEvent {
                            event: Some(
                                crate::schema_proto::schema_change_event::Event::TableAltered(
                                    TableAltered {
                                        table_id: 0,
                                        new_version: 0,
                                        new_schema_id: 0,
                                    },
                                ),
                            ),
                        };
                        let _ = tx.send(Ok(refresh_event)).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

// ─── Proto conversion helpers ─────────────────────────────────────────────

fn table_info_to_proto(info: &crate::mirror::TableInfo) -> ProtoTableInfo {
    ProtoTableInfo {
        table_id: info.table_id,
        table_name: info.table_name.clone(),
        schema_id: info.schema_id,
        schema_version: info.schema_version,
        table_group: info.table_group.clone(),
        table_type: table_type_to_proto(info.table_type).into(),
        description: info.description.clone(),
        enabled: info.enabled,
        columns: info.columns.iter().map(column_def_to_proto).collect(),
        deleted_at_nanos: info.deleted_at.map(|ts| ts.as_nanos()).unwrap_or(0),
        created_at_nanos: info.created_at.as_nanos(),
        updated_at_nanos: info.updated_at.as_nanos(),
    }
}

fn table_type_to_proto(tt: TableType) -> ProtoTableType {
    match tt {
        TableType::Raw => ProtoTableType::Raw,
        TableType::Derived => ProtoTableType::Derived,
        TableType::Cq => ProtoTableType::Cq,
        TableType::Stream => ProtoTableType::Stream,
    }
}

fn column_def_to_proto(col: &crate::mirror::ColumnDef) -> ProtoColumn {
    ProtoColumn {
        column_id: col.column_id as u32,
        name: col.name.clone(),
        data_type: data_type_to_proto(col.data_type).into(),
        precision: col.precision,
        nullable: col.nullable,
        unit: col.unit.clone(),
        description: col.description.clone(),
        deadband: col.deadband.as_ref().map(|d| ProtoDeadband {
            deadband_type: deadband_type_to_proto(d.deadband_type).into(),
            value: d.value,
        }),
        enum_values: col.enum_values.clone().unwrap_or_default(),
        deleted_at_nanos: col.deleted_at.map(|ts| ts.as_nanos()).unwrap_or(0),
        range_low: col.range_low,
        range_high: col.range_high,
        instrument_range_low: col.instrument_range_low,
        instrument_range_high: col.instrument_range_high,
        display_digits: col.display_digits as u32,
    }
}

fn data_type_to_proto(dt: DataType) -> ProtoDataType {
    match dt {
        DataType::F64 => ProtoDataType::F64,
        DataType::F32 => ProtoDataType::F32,
        DataType::I64 => ProtoDataType::I64,
        DataType::U64 => ProtoDataType::U64,
        DataType::I32 => ProtoDataType::I32,
        DataType::Bool => ProtoDataType::Bool,
        DataType::String => ProtoDataType::String,
        DataType::Enum16 => ProtoDataType::Enum16,
    }
}

fn deadband_type_to_proto(dt: crate::mirror::DeadbandType) -> ProtoDeadbandType {
    match dt {
        crate::mirror::DeadbandType::None => ProtoDeadbandType::None,
        crate::mirror::DeadbandType::Absolute => ProtoDeadbandType::Absolute,
        crate::mirror::DeadbandType::Percent => ProtoDeadbandType::Percent,
    }
}

fn schema_event_to_proto(event: SchemaEvent) -> SchemaChangeEvent {
    use crate::schema_proto::schema_change_event::Event;
    match event {
        SchemaEvent::TableCreated { table_id, .. } => SchemaChangeEvent {
            event: Some(Event::TableCreated(TableCreated { table_id })),
        },
        SchemaEvent::TableAltered {
            table_id,
            new_version,
            new_schema_id,
        } => SchemaChangeEvent {
            event: Some(Event::TableAltered(TableAltered {
                table_id,
                new_version,
                new_schema_id,
            })),
        },
        SchemaEvent::TableDeleted { table_id } => SchemaChangeEvent {
            event: Some(Event::TableDeleted(TableDeleted { table_id })),
        },
    }
}

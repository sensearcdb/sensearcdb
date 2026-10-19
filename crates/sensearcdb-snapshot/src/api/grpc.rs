//! Snapshot gRPC 数据面（G3）。
//!
//! 快照统一走 gRPC，取代 REST/SSE。提供四个 RPC：
//!
//! - `GetSnapshot`：单点查询。
//! - `BatchGetSnapshot`：批量查询。
//! - `ListSnapshots`：游标分页全量枚举（弱一致）。
//! - `SubscribeSnapshots`：server-streaming 实时推送（首帧 INITIAL_SNAPSHOT，后续 DELTA）。

use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use sensearcdb_common::{TenantId, TypedValue};

use crate::entry::SnapshotEntry;
use crate::snapshot_proto::snapshot_event::EventType;
use crate::snapshot_proto::snapshot_service_server::{SnapshotService, SnapshotServiceServer};
use crate::snapshot_proto::{
    column_value, BatchGetSnapshotRequest, BatchGetSnapshotResponse, ColumnQuality,
    ColumnTimestamp, ColumnValue, GetSnapshotRequest, GetSnapshotResponse, ListSnapshotsRequest,
    ListSnapshotsResponse, SnapshotEntryProto, SnapshotEvent, SubscribeSnapshotsRequest,
};
use crate::store::{
    filter_entry_columns, ColumnVisibleFn, SnapshotCursor, SnapshotStore, TypedSnapshotValue,
};
use crate::subscription::{DataChangeNotification, SnapshotSubscriptionManager};

/// 快照 gRPC 租户解析器（与 ingest `GrpcTenantResolver` 同构的局部别名，
/// 避免 snapshot crate 反向依赖 ingest；`main.rs` 的 `build_grpc_tenant_resolver`
/// 产物可直接传入）。
pub type SnapshotTenantResolver =
    std::sync::Arc<dyn Fn(&tonic::metadata::MetadataMap) -> Option<TenantId> + Send + Sync>;

/// 列级可见性解析器：从请求 metadata 解析出「列级过滤闭包」。
///
/// `None` 返回 = 全可见（与 `VisibleTagSet.allowed = None` 对齐）。
/// 返回闭包语义 = `VisibleTagSet::contains(table_id, column_id)`。
pub type SnapshotColumnVisibilityResolver = std::sync::Arc<
    dyn Fn(&tonic::metadata::MetadataMap)
        -> Option<std::sync::Arc<dyn Fn(u32, u16) -> bool + Send + Sync>>
        + Send
        + Sync,
>;

/// Snapshot gRPC 服务共享状态。
///
/// 3.3 B1：`store` 为 `ArcSwap` 单一真相，每次请求 `.load()` 读最新 store，
/// Raft install 原子替换后立即可见。`subscription_manager` 跨 swap 共享
/// （startup 层注入），供订阅推送复用。
pub struct SnapshotGrpcService {
    store: Arc<ArcSwap<SnapshotStore>>,
    subscription_manager: Arc<SnapshotSubscriptionManager>,
    /// 租户解析器：从请求 metadata 解析租户（5206 数据面隔离）。
    tenant_resolver: Option<SnapshotTenantResolver>,
    /// 列级可见性解析器：从请求 metadata 解析列级过滤闭包（Q5，未注入 = 不过滤）。
    column_visibility_resolver: Option<SnapshotColumnVisibilityResolver>,
}

impl SnapshotGrpcService {
    /// 创建 gRPC 服务（默认不解析租户；挂载时需 `.with_tenant_resolver(...)`）。
    pub fn new(
        store: Arc<ArcSwap<SnapshotStore>>,
        subscription_manager: Arc<SnapshotSubscriptionManager>,
    ) -> Self {
        Self {
            store,
            subscription_manager,
            tenant_resolver: None,
            column_visibility_resolver: None,
        }
    }

    /// 注入租户解析器（`main.rs` 复用 5201/5202/5203 的 `build_grpc_tenant_resolver`）。
    pub fn with_tenant_resolver(mut self, resolver: SnapshotTenantResolver) -> Self {
        self.tenant_resolver = Some(resolver);
        self
    }

    /// 注入列级可见性解析器（`main.rs` 复用 `TagVisibilityService` 解析 principal）。
    pub fn with_column_visibility_resolver(
        mut self,
        resolver: SnapshotColumnVisibilityResolver,
    ) -> Self {
        self.column_visibility_resolver = Some(resolver);
        self
    }

    /// 解析请求租户；解析不出 → 401（与 QueryService 一致，无凭据/平台管理员一律 401）。
    fn resolve_tenant<T>(&self, req: &Request<T>) -> Result<TenantId, Status> {
        let resolver = self
            .tenant_resolver
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("missing tenant resolver"))?;
        resolver(req.metadata())
            .ok_or_else(|| Status::unauthenticated("missing credentials or unresolvable tenant"))
    }

    /// 转换为 tonic 可路由的 service。
    pub fn into_server(self) -> SnapshotServiceServer<Self> {
        SnapshotServiceServer::new(self)
    }
}

/// `SubscribeSnapshots` 的 server-streaming 响应流。
type SubscribeStream = Pin<Box<dyn Stream<Item = Result<SnapshotEvent, Status>> + Send>>;

/// 把 resolver 产出的 `Arc<dyn Fn + Send + Sync>` 转成 store 层期望的
/// `&ColumnVisibleFn`（丢弃 auto trait 的 coercion）。
fn column_visible_ref<'a>(
    v: &'a Option<std::sync::Arc<dyn Fn(u32, u16) -> bool + Send + Sync>>,
) -> Option<&'a ColumnVisibleFn<'a>> {
    v.as_ref().map(|f| {
        let g: &ColumnVisibleFn<'_> = f.as_ref();
        g
    })
}

/// 当前时间（epoch 纳秒），用于事件的 server_ts。
fn now_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

/// 由原始多列字段构造 `SnapshotEntryProto`（首列 f64/quality 为兼容视图）。
#[allow(clippy::too_many_arguments)]
fn proto_from_parts(
    tag_id: u32,
    values: &[(u16, TypedValue)],
    qualities: &[(u16, u8)],
    ts: i64,
    server_ts: i64,
    version: u64,
    is_derived: bool,
    ttl_expired: bool,
    ts_values: &[(u16, i64)],
    server_ts_values: &[(u16, i64)],
) -> SnapshotEntryProto {
    let value = values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0);
    let quality = qualities.first().map(|(_, q)| *q as u32).unwrap_or(0);

    let values = values
        .iter()
        .map(|(cid, v)| ColumnValue {
            column_id: *cid as u32,
            value: Some(match v {
                TypedValue::F64(x) => column_value::Value::F64Value(*x),
                TypedValue::F32(x) => column_value::Value::F32Value(*x),
                TypedValue::I64(x) => column_value::Value::I64Value(*x),
                TypedValue::U64(x) => column_value::Value::U64Value(*x),
                TypedValue::I32(x) => column_value::Value::I32Value(*x),
                TypedValue::Bool(x) => column_value::Value::BoolValue(*x),
                TypedValue::String(x) => column_value::Value::StringValue(x.clone()),
                TypedValue::Enum16(x) => column_value::Value::Enum16Value(*x as u32),
            }),
        })
        .collect();

    let qualities = qualities
        .iter()
        .map(|(cid, q)| ColumnQuality {
            column_id: *cid as u32,
            quality: *q as u32,
        })
        .collect();

    let ts_values = ts_values
        .iter()
        .map(|(cid, t)| ColumnTimestamp {
            column_id: *cid as u32,
            timestamp: *t,
        })
        .collect();

    let server_ts_values = server_ts_values
        .iter()
        .map(|(cid, t)| ColumnTimestamp {
            column_id: *cid as u32,
            timestamp: *t,
        })
        .collect();

    SnapshotEntryProto {
        tag_id,
        value,
        quality,
        ts,
        server_ts,
        version,
        is_derived,
        ttl_expired,
        values,
        qualities,
        ts_values,
        server_ts_values,
    }
}

fn entry_to_proto(tag_id: u32, entry: &SnapshotEntry) -> SnapshotEntryProto {
    proto_from_parts(
        tag_id,
        &entry.values,
        &entry.qualities,
        entry.ts,
        entry.server_ts,
        entry.version,
        entry.is_derived,
        entry.ttl_expired,
        &entry.ts_values,
        &entry.server_ts_values,
    )
}

fn typed_value_to_proto(tag_id: u32, v: &TypedSnapshotValue) -> SnapshotEntryProto {
    proto_from_parts(
        tag_id,
        &v.values,
        &v.qualities,
        v.ts,
        v.server_ts,
        v.version,
        v.is_derived,
        v.ttl_expired,
        &v.ts_values,
        &v.server_ts_values,
    )
}

#[tonic::async_trait]
impl SnapshotService for SnapshotGrpcService {
    type SubscribeSnapshotsStream = SubscribeStream;

    /// 单点查询。
    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let tenant = self.resolve_tenant(&request)?;
        let column_visible = self
            .column_visibility_resolver
            .as_ref()
            .and_then(|r| r(request.metadata()));
        let column_visible = column_visible_ref(&column_visible);
        let tag_id = request.into_inner().tag_id;
        let store = self.store.load();
        // 跨租户读返回 found=false（视为不存在），不返回 403，避免泄露存在性。
        let (entry, found) = match store.get_visible(tag_id, column_visible) {
            Some(e) if e.tenant_id == tenant => (Some(entry_to_proto(tag_id, e.as_ref())), true),
            _ => (None, false),
        };
        Ok(Response::new(GetSnapshotResponse { entry, found }))
    }

    /// 批量查询。
    async fn batch_get_snapshot(
        &self,
        request: Request<BatchGetSnapshotRequest>,
    ) -> Result<Response<BatchGetSnapshotResponse>, Status> {
        let tenant = self.resolve_tenant(&request)?;
        let column_visible = self
            .column_visibility_resolver
            .as_ref()
            .and_then(|r| r(request.metadata()));
        let column_visible = column_visible_ref(&column_visible);
        let tag_ids = request.into_inner().tag_ids;
        let store = self.store.load();
        // 仅返回本租户条目；他人条目直接过滤（不泄露存在性）。
        let entries = store
            .batch_get_typed_visible(&tag_ids, column_visible)
            .into_iter()
            .filter(|(_, v)| v.tenant_id == tenant)
            .map(|(tid, v)| typed_value_to_proto(tid, &v))
            .collect();
        Ok(Response::new(BatchGetSnapshotResponse { entries }))
    }

    /// 游标分页全量枚举（弱一致）。
    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let tenant = self.resolve_tenant(&request)?;
        let column_visible = self
            .column_visibility_resolver
            .as_ref()
            .and_then(|r| r(request.metadata()));
        let column_visible = column_visible_ref(&column_visible);
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            1000usize
        } else {
            (req.limit.clamp(1, 10_000)) as usize
        };

        let cursor: Option<SnapshotCursor> =
            if req.cursor.is_empty() {
                None
            } else {
                Some(bincode::deserialize(&req.cursor).map_err(|e| {
                    Status::invalid_argument(format!("invalid snapshot cursor: {e}"))
                })?)
            };

        let store = self.store.load();
        // store 内按 tenant 过滤，保证游标分页不重不漏。
        let (raw_entries, next_cursor) =
            store.list_all_visible(cursor, limit, tenant, column_visible);

        let entries: Vec<SnapshotEntryProto> = raw_entries
            .into_iter()
            .map(|(tid, v)| typed_value_to_proto(tid, &v))
            .collect();

        let has_more = next_cursor.is_some();
        let next_cursor = next_cursor
            .map(|c| bincode::serialize(&c).unwrap_or_default())
            .unwrap_or_default();

        Ok(Response::new(ListSnapshotsResponse {
            entries,
            next_cursor,
            has_more,
        }))
    }

    /// server-streaming 实时推送：首帧 INITIAL_SNAPSHOT，后续 DELTA。
    async fn subscribe_snapshots(
        &self,
        request: Request<SubscribeSnapshotsRequest>,
    ) -> Result<Response<Self::SubscribeSnapshotsStream>, Status> {
        let tenant = self.resolve_tenant(&request)?;
        let column_visible_arc = self
            .column_visibility_resolver
            .as_ref()
            .and_then(|r| r(request.metadata()));
        let column_visible = column_visible_ref(&column_visible_arc);
        let req = request.into_inner();
        let tag_ids = req.tag_ids.clone();

        let (tx, rx) = mpsc::channel::<Result<SnapshotEvent, Status>>(256);

        // 1. 首帧 INITIAL_SNAPSHOT：空 tag_ids = 全局订阅，返回本租户全量；
        //    非空 tag_ids 仅返回本租户命中的条目（他人条目过滤，不泄露存在性）。
        {
            let store = self.store.load();
            let global_version = store.current_version();
            let entries: Vec<SnapshotEntryProto> = if tag_ids.is_empty() {
                store
                    .iter_stream_visible(column_visible)
                    .filter(|(_, e)| e.tenant_id == tenant)
                    .map(|(tid, e)| entry_to_proto(tid, &e))
                    .collect()
            } else {
                store
                    .batch_get_typed_visible(&tag_ids, column_visible)
                    .into_iter()
                    .filter(|(_, v)| v.tenant_id == tenant)
                    .map(|(tid, v)| typed_value_to_proto(tid, &v))
                    .collect()
            };

            let event = SnapshotEvent {
                event_type: EventType::InitialSnapshot as i32,
                entries,
                global_version,
                server_ts: now_ns(),
            };
            let _ = tx.send(Ok(event)).await;
        }

        // 2. 注册订阅并桥接 delta 推送。
        let (sub_tx, mut sub_rx) = mpsc::channel::<DataChangeNotification>(256);
        let sub_id = self.subscription_manager.subscribe(tag_ids, sub_tx, false);
        let sub_mgr = self.subscription_manager.clone();
        let sub_id_clone = sub_id.clone();

        tokio::spawn(async move {
            while let Some(notif) = sub_rx.recv().await {
                // DELTA 跨租户静默丢弃（notif.entry 为 SnapshotEntry，含 tenant_id）。
                if notif.entry.tenant_id != tenant {
                    continue;
                }
                // 列级过滤：与首帧一致，仅推送可见列；整表列全被过滤则跳过该条
                // DELTA（防存在性泄露）。
                let filtered: Option<SnapshotEntry> = match column_visible_arc.as_ref() {
                    Some(f) => {
                        let visible: &ColumnVisibleFn<'_> = f.as_ref();
                        filter_entry_columns(&notif.entry, notif.table_id, visible)
                    }
                    None => Some((*notif.entry).clone()),
                };
                let entry = match filtered {
                    Some(e) => e,
                    None => continue,
                };
                let event = SnapshotEvent {
                    event_type: EventType::Delta as i32,
                    entries: vec![entry_to_proto(notif.table_id, &entry)],
                    global_version: notif.version,
                    server_ts: now_ns(),
                };
                if tx.send(Ok(event)).await.is_err() {
                    // 客户端断开
                    break;
                }
            }
            sub_mgr.unsubscribe(&sub_id_clone);
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::SnapshotMetrics;
    use crate::tag_metadata::{SchemaMirrorRef, TagMetadataStore};
    use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId};

    struct TestMirror;
    impl SchemaMirrorRef for TestMirror {
        fn get_table_name(&self, _: u32) -> Option<String> {
            Some("t".into())
        }
        fn get_table_description(&self, _: u32) -> Option<String> {
            None
        }
        fn get_range_low(&self, _: u32, _: u16) -> f64 {
            0.0
        }
        fn get_range_high(&self, _: u32, _: u16) -> f64 {
            100.0
        }
        fn get_instrument_range_low(&self, _: u32, _: u16) -> Option<f64> {
            None
        }
        fn get_instrument_range_high(&self, _: u32, _: u16) -> Option<f64> {
            None
        }
        fn get_unit(&self, _: u32, _: u16) -> Option<String> {
            None
        }
        fn get_display_digits(&self, _: u32, _: u16) -> u8 {
            2
        }
        fn get_data_type(&self, _: u32, _: u16) -> DataType {
            DataType::F64
        }
        fn is_enabled(&self, _: u32) -> bool {
            true
        }
    }

    fn make_store() -> Arc<ArcSwap<SnapshotStore>> {
        let mut cfg = AppConfig::default();
        cfg.snapshot.shard_multiplier = 1;
        let ch = ConfigHandle::new(cfg);
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestMirror)));
        let store = SnapshotStore::new(&ch, sub_mgr, metadata, SnapshotMetrics::noop());
        Arc::new(ArcSwap::new(Arc::new(store)))
    }

    fn default_tenant_resolver() -> SnapshotTenantResolver {
        std::sync::Arc::new(|_m| Some(TenantId::DEFAULT))
    }

    fn tenant_resolver_for(tenant: TenantId) -> SnapshotTenantResolver {
        std::sync::Arc::new(move |_m| Some(tenant))
    }

    fn make_service_for(store: Arc<ArcSwap<SnapshotStore>>) -> SnapshotGrpcService {
        let sub_mgr = store.load().subscription_manager().clone();
        SnapshotGrpcService::new(store, sub_mgr).with_tenant_resolver(default_tenant_resolver())
    }

    fn make_service_for_tenant(
        store: Arc<ArcSwap<SnapshotStore>>,
        tenant: TenantId,
    ) -> SnapshotGrpcService {
        let sub_mgr = store.load().subscription_manager().clone();
        SnapshotGrpcService::new(store, sub_mgr).with_tenant_resolver(tenant_resolver_for(tenant))
    }

    /// G3：`SnapshotEntryProto` 必须含列级 qualities/ts_values/server_ts_values。
    #[test]
    fn test_proto_contains_column_level_fields() {
        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::I64(42))],
            &[(0, 0xC0), (1, 0x00)],
            1000,
            2000,
            None,
            false,
            None,
        );

        let entry = store.load().get(1).unwrap();
        let proto = entry_to_proto(1, entry.as_ref());

        assert_eq!(proto.tag_id, 1);
        assert_eq!(proto.values.len(), 2);
        assert_eq!(proto.qualities.len(), 2);
        assert_eq!(proto.ts_values.len(), 2);
        assert_eq!(proto.server_ts_values.len(), 2);
        assert_eq!(proto.ts_values[0].column_id, 0);
        assert_eq!(proto.ts_values[0].timestamp, 1000);
        assert_eq!(proto.ts_values[1].timestamp, 1000);
        assert_eq!(proto.server_ts_values[0].timestamp, 2000);
        // 列级值类型保真：col1 为 I64。
        assert_eq!(proto.values[1].column_id, 1);
        assert!(matches!(
            proto.values[1].value,
            Some(column_value::Value::I64Value(42))
        ));
    }

    /// G3：单点/批量 gRPC handler 往返。
    #[tokio::test]
    async fn test_get_and_batch_snapshot_rpc() {
        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(3.14))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let svc = make_service_for(store);

        let resp = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 7 }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.found);
        let entry = resp.entry.expect("found entry");
        assert_eq!(entry.tag_id, 7);
        assert!((entry.value - 3.14).abs() < 1e-12);

        let batch = svc
            .batch_get_snapshot(Request::new(BatchGetSnapshotRequest {
                tag_ids: vec![7, 99],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(batch.entries.len(), 1, "未命中的 tag 不返回");
        assert_eq!(batch.entries[0].tag_id, 7);
    }

    /// G3：ListSnapshots 两页合并完整 + next_cursor/has_more 正确。
    #[tokio::test]
    async fn test_list_snapshots_pagination() {
        let store = make_store();
        for i in 1u32..=10u32 {
            store.load().update_typed(
                TenantId::DEFAULT,
                i,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                1000,
                2000,
                None,
                false,
                None,
            );
        }
        let svc = make_service_for(store);

        let page1 = svc
            .list_snapshots(Request::new(ListSnapshotsRequest {
                cursor: Vec::new(),
                limit: 6,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page1.entries.len(), 6);
        assert!(page1.has_more);
        assert!(!page1.next_cursor.is_empty());

        let page2 = svc
            .list_snapshots(Request::new(ListSnapshotsRequest {
                cursor: page1.next_cursor,
                limit: 6,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page2.entries.len(), 4, "剩余 4 条");
        assert!(!page2.has_more);
        assert!(page2.next_cursor.is_empty());

        let mut tids: Vec<u32> = page1
            .entries
            .iter()
            .chain(page2.entries.iter())
            .map(|e| e.tag_id)
            .collect();
        tids.sort_unstable();
        assert_eq!(tids, (1u32..=10u32).collect::<Vec<_>>());
    }

    // ─── 5206 认证 + 租户隔离 ───

    /// 用例 1（服务侧）：无 resolver（等价于无凭据未注入）→ 401。
    #[tokio::test]
    async fn test_get_snapshot_no_resolver_401() {
        let store = make_store();
        let sub_mgr = store.load().subscription_manager().clone();
        let svc = SnapshotGrpcService::new(store, sub_mgr); // 无 resolver

        let err = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 1 }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    /// 用例 4/5：同租户命中返回；跨租户 get 返回 found=false（不泄露存在性）。
    #[tokio::test]
    async fn test_get_snapshot_tenant_isolation() {
        let store = make_store();
        store.load().update_typed(
            TenantId(1),
            7,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        store.load().update_typed(
            TenantId(2),
            8,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let svc = make_service_for_tenant(store, TenantId(1));

        let ok = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 7 }))
            .await
            .unwrap()
            .into_inner();
        assert!(ok.found);
        assert_eq!(ok.entry.unwrap().tag_id, 7);

        let cross = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 8 }))
            .await
            .unwrap()
            .into_inner();
        assert!(!cross.found, "跨租户读视为不存在");
        assert!(cross.entry.is_none());
    }

    /// 用例 6：batch 仅返回本租户条目，他人条目被过滤。
    #[tokio::test]
    async fn test_batch_get_snapshot_tenant_filter() {
        let store = make_store();
        store.load().update_typed(
            TenantId(1),
            7,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        store.load().update_typed(
            TenantId(2),
            8,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let svc = make_service_for_tenant(store, TenantId(1));

        let resp = svc
            .batch_get_snapshot(Request::new(BatchGetSnapshotRequest {
                tag_ids: vec![7, 8],
            }))
            .await
            .unwrap()
            .into_inner();
        let tids: Vec<u32> = resp.entries.iter().map(|e| e.tag_id).collect();
        assert_eq!(tids, vec![7]);
    }

    /// 用例 7：list 仅返回本租户条目（store 内过滤，分页正确）。
    #[tokio::test]
    async fn test_list_snapshots_tenant_filter() {
        let store = make_store();
        for i in 1u32..=6u32 {
            let tenant = if i % 2 == 0 { TenantId(1) } else { TenantId(2) };
            store.load().update_typed(
                tenant,
                i,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                1000,
                2000,
                None,
                false,
                None,
            );
        }
        let svc = make_service_for_tenant(store, TenantId(1));

        let resp = svc
            .list_snapshots(Request::new(ListSnapshotsRequest {
                cursor: Vec::new(),
                limit: 100,
            }))
            .await
            .unwrap()
            .into_inner();
        let mut tids: Vec<u32> = resp.entries.iter().map(|e| e.tag_id).collect();
        tids.sort_unstable();
        assert_eq!(tids, vec![2u32, 4, 6]);
    }

    /// 用例 8/9：订阅 INITIAL 仅本租户；DELTA 跨租户静默丢弃、本租户推送。
    #[tokio::test]
    async fn test_subscribe_snapshots_tenant_filter() {
        use std::time::Duration;
        use tokio_stream::StreamExt;

        let store = make_store();
        store.load().update_typed(
            TenantId(1),
            7,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        store.load().update_typed(
            TenantId(2),
            8,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let svc = make_service_for_tenant(store.clone(), TenantId(1));

        let resp = svc
            .subscribe_snapshots(Request::new(SubscribeSnapshotsRequest {
                tag_ids: vec![],
                subscription_id: "test-sub".to_string(),
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();

        // INITIAL：全局订阅下仅本租户（tag 7）。
        let initial = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("initial timeout")
            .expect("stream ended")
            .expect("initial error");
        assert_eq!(initial.event_type, EventType::InitialSnapshot as i32);
        let tags: Vec<u32> = initial.entries.iter().map(|e| e.tag_id).collect();
        assert_eq!(tags, vec![7], "INITIAL 仅本租户");

        // 先写他人租户（应被丢弃），再写本租户（应推送）。
        store.load().update_typed(
            TenantId(2),
            8,
            &[(0, TypedValue::F64(3.0))],
            &[(0, 0xC0)],
            2000,
            3000,
            None,
            false,
            None,
        );
        store.load().update_typed(
            TenantId(1),
            7,
            &[(0, TypedValue::F64(4.0))],
            &[(0, 0xC0)],
            2000,
            3000,
            None,
            false,
            None,
        );

        let delta = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("delta timeout")
            .expect("stream ended")
            .expect("delta error");
        assert_eq!(delta.event_type, EventType::Delta as i32);
        assert_eq!(delta.entries.len(), 1);
        assert_eq!(
            delta.entries[0].tag_id, 7,
            "跨租户 delta 已丢弃，仅推送本租户"
        );
    }

    // ─── Q5 列级权限过滤 ───

    /// 构造固定白名单列级解析器；`None` = 全可见。
    fn column_resolver_for(
        allowed: Option<std::collections::HashSet<(u32, u16)>>,
    ) -> SnapshotColumnVisibilityResolver {
        std::sync::Arc::new(move |_m| {
            allowed.clone().map(|set| {
                let f: std::sync::Arc<dyn Fn(u32, u16) -> bool + Send + Sync> =
                    std::sync::Arc::new(move |tid: u32, cid: u16| set.contains(&(tid, cid)));
                f
            })
        })
    }

    /// Q5：get 列级过滤——只保留白名单列，越权列被过滤；整表过滤 → found=false。
    #[tokio::test]
    async fn test_get_snapshot_column_filter() {
        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let mut allowed = std::collections::HashSet::new();
        allowed.insert((7, 0));
        let svc = make_service_for(store.clone())
            .with_column_visibility_resolver(column_resolver_for(Some(allowed)));

        let resp = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 7 }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.found);
        let entry = resp.entry.expect("found entry");
        assert_eq!(entry.values.len(), 1, "越权列被过滤");
        assert_eq!(entry.values[0].column_id, 0);

        // 整表过滤：tag8 存在但白名单不含 (8, _) → found=false（防泄露存在性）。
        store.load().update_typed(
            TenantId::DEFAULT,
            8,
            &[(0, TypedValue::F64(3.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let resp8 = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 8 }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp8.found, "整表无可见列 → 视为不存在");
    }

    /// Q5：resolver 返回 None → 全可见不收缩。
    #[tokio::test]
    async fn test_get_snapshot_column_filter_none_unrestricted() {
        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let svc =
            make_service_for(store).with_column_visibility_resolver(column_resolver_for(None));
        let resp = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 7 }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.found);
        assert_eq!(resp.entry.expect("entry").values.len(), 2, "None 不得收缩");
    }

    /// Q5：batch 列级过滤。
    #[tokio::test]
    async fn test_batch_get_snapshot_column_filter() {
        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let mut allowed = std::collections::HashSet::new();
        allowed.insert((7, 1));
        let svc = make_service_for(store)
            .with_column_visibility_resolver(column_resolver_for(Some(allowed)));

        let resp = svc
            .batch_get_snapshot(Request::new(BatchGetSnapshotRequest {
                tag_ids: vec![7],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.entries.len(), 1);
        assert_eq!(resp.entries[0].values.len(), 1);
        assert_eq!(resp.entries[0].values[0].column_id, 1);
    }

    /// Q5：list 列级过滤。
    #[tokio::test]
    async fn test_list_snapshots_column_filter() {
        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let mut allowed = std::collections::HashSet::new();
        allowed.insert((7, 0));
        let svc = make_service_for(store)
            .with_column_visibility_resolver(column_resolver_for(Some(allowed)));

        let resp = svc
            .list_snapshots(Request::new(ListSnapshotsRequest {
                cursor: Vec::new(),
                limit: 100,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.entries.len(), 1);
        assert_eq!(resp.entries[0].values.len(), 1);
        assert_eq!(resp.entries[0].values[0].column_id, 0);
    }

    /// Q5：subscribe 首帧列级过滤。
    #[tokio::test]
    async fn test_subscribe_snapshots_column_filter() {
        use std::time::Duration;
        use tokio_stream::StreamExt;

        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let mut allowed = std::collections::HashSet::new();
        allowed.insert((7, 1));
        let svc = make_service_for(store)
            .with_column_visibility_resolver(column_resolver_for(Some(allowed)));

        let resp = svc
            .subscribe_snapshots(Request::new(SubscribeSnapshotsRequest {
                tag_ids: vec![],
                subscription_id: "test-sub-col".to_string(),
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();
        let initial = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("initial timeout")
            .expect("stream ended")
            .expect("initial error");
        assert_eq!(initial.event_type, EventType::InitialSnapshot as i32);
        assert_eq!(initial.entries.len(), 1);
        assert_eq!(initial.entries[0].values.len(), 1);
        assert_eq!(initial.entries[0].values[0].column_id, 1);
    }

    // ─── 独立 QA（fresh eyes）边界测试 ───

    /// D1 跨租户 + 列级过滤叠加：两个租户各自有可见列，验证租户隔离与列级
    /// 过滤同时生效、互不干扰（A.4 红线独立复验）。
    #[tokio::test]
    async fn qa_cross_tenant_plus_column_filter() {
        let store = make_store();
        // 租户 1：表 10，两列 {0,1}；租户 2：表 20，两列 {0,1}。
        store.load().update_typed(
            TenantId(1),
            10,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        store.load().update_typed(
            TenantId(2),
            20,
            &[(0, TypedValue::F64(3.0)), (1, TypedValue::F64(4.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        // 白名单同时含两租户各自的可视列：租户1 只见 (10,0)，租户2 只见 (20,1)。
        let mut allowed = std::collections::HashSet::new();
        allowed.insert((10, 0));
        allowed.insert((20, 1));
        let svc = make_service_for_tenant(store, TenantId(1))
            .with_column_visibility_resolver(column_resolver_for(Some(allowed)));

        // 租户 1 读表 10：只返回本租户列，且被列级过滤到 {0}。
        let resp10 = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 10 }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp10.found);
        let e10 = resp10.entry.expect("tenant1 own table");
        assert_eq!(e10.values.len(), 1, "租户1 列级过滤到仅 col0");
        assert_eq!(e10.values[0].column_id, 0);

        // 租户 1 读表 20（他人租户）：即使白名单含 (20,1)，也不得泄露 → found=false。
        let resp20 = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 20 }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp20.found, "跨租户读必须视为不存在，不得因列级白名单泄露");
        assert!(resp20.entry.is_none());
    }

    /// D2 Subscribe 首帧过滤 + 后续 DELTA 通知过滤：订阅建立时首帧过滤，
    /// 后续数据变更通知也必须过滤（不推送越权列）。
    #[tokio::test]
    async fn qa_subscribe_delta_column_filter() {
        use std::time::Duration;
        use tokio_stream::StreamExt;

        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        // 只授权 (7,0)。
        let mut allowed = std::collections::HashSet::new();
        allowed.insert((7, 0));
        let svc = make_service_for(store.clone())
            .with_column_visibility_resolver(column_resolver_for(Some(allowed)));

        let resp = svc
            .subscribe_snapshots(Request::new(SubscribeSnapshotsRequest {
                tag_ids: vec![7],
                subscription_id: "qa-delta-col".to_string(),
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();

        // 首帧：仅 col0。
        let initial = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("initial timeout")
            .expect("stream ended")
            .expect("initial error");
        assert_eq!(initial.event_type, EventType::InitialSnapshot as i32);
        assert_eq!(initial.entries.len(), 1);
        assert_eq!(initial.entries[0].values.len(), 1);
        assert_eq!(initial.entries[0].values[0].column_id, 0);

        // 变更 col0 与 col1 → DELTA 通知必须只推送可见列 col0，不得泄露 col1。
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(9.0)), (1, TypedValue::F64(8.0))],
            &[(0, 0x00), (1, 0x00)],
            3000,
            4000,
            None,
            false,
            None,
        );
        let delta = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("delta timeout")
            .expect("stream ended")
            .expect("delta error");
        assert_eq!(delta.event_type, EventType::Delta as i32);
        assert_eq!(delta.entries.len(), 1);
        // 关键断言：DELTA 推送的列数必须为 1（仅 col0），否则泄露 col1（越权）。
        assert_eq!(
            delta.entries[0].values.len(),
            1,
            "DELTA 通知必须应用列级过滤，不得泄露越权列"
        );
        assert_eq!(delta.entries[0].values[0].column_id, 0);
    }

    /// D2b 独立边界（fresh eyes）：3 列表仅授权 1 列，DELTA 通知只含该 1 列，
    /// 其余 2 列不得泄露（五组列级字段 values/qualities/ts_values/server_ts_values 全部收缩）。
    #[tokio::test]
    async fn qa_subscribe_delta_three_col_partial_filter() {
        use std::time::Duration;
        use tokio_stream::StreamExt;

        let store = make_store();
        // 3 列表 7：列 {0,1,2}。
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::F64(2.0)),
                (2, TypedValue::F64(3.0)),
            ],
            &[(0, 0xC0), (1, 0xC0), (2, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        // 只授权 (7,1)。
        let mut allowed = std::collections::HashSet::new();
        allowed.insert((7, 1));
        let svc = make_service_for(store.clone())
            .with_column_visibility_resolver(column_resolver_for(Some(allowed)));

        let resp = svc
            .subscribe_snapshots(Request::new(SubscribeSnapshotsRequest {
                tag_ids: vec![7],
                subscription_id: "qa-delta-3col".to_string(),
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();

        // 首帧：仅 col1。
        let initial = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("initial timeout")
            .expect("stream ended")
            .expect("initial error");
        assert_eq!(initial.event_type, EventType::InitialSnapshot as i32);
        assert_eq!(initial.entries.len(), 1);
        assert_eq!(initial.entries[0].values.len(), 1);
        assert_eq!(initial.entries[0].values[0].column_id, 1);

        // 变更 3 列 → DELTA 必须只推送授权列 col1，不泄露 col0/col2。
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[
                (0, TypedValue::F64(9.0)),
                (1, TypedValue::F64(8.0)),
                (2, TypedValue::F64(7.0)),
            ],
            &[(0, 0x00), (1, 0x00), (2, 0x00)],
            3000,
            4000,
            None,
            false,
            None,
        );
        let delta = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("delta timeout")
            .expect("stream ended")
            .expect("delta error");
        assert_eq!(delta.event_type, EventType::Delta as i32);
        assert_eq!(delta.entries.len(), 1);
        // 关键断言：DELTA 五组列级字段全部收缩到仅 col1，其余 2 列不得泄露。
        assert_eq!(
            delta.entries[0].values.len(),
            1,
            "3 列表仅授权 1 列，DELTA 必须只含该 1 列，其余 2 列不得泄露"
        );
        assert_eq!(delta.entries[0].values[0].column_id, 1, "仅保留授权列 col1");
        assert_eq!(delta.entries[0].qualities.len(), 1, "qualities 同步收缩");
        assert_eq!(delta.entries[0].ts_values.len(), 1, "ts_values 同步收缩");
        assert_eq!(
            delta.entries[0].server_ts_values.len(),
            1,
            "server_ts_values 同步收缩"
        );
    }

    /// D3 白名单含不存在的 column_id：不 panic、不误返回（存在性/列判定正确）。
    #[tokio::test]
    async fn qa_whitelist_with_missing_column_id() {
        let store = make_store();
        store.load().update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        // 白名单含 (7, 0) 与 (7, 999)（不存在的列）+ (999, 0)（不存在的表）。
        let mut allowed = std::collections::HashSet::new();
        allowed.insert((7, 0));
        allowed.insert((7, 999));
        allowed.insert((999, 0));
        let svc = make_service_for(store)
            .with_column_visibility_resolver(column_resolver_for(Some(allowed)));

        // 表 7 存在，仅 col0 可见；col999 不在 entry 中，不得 panic 也不得产出。
        let resp = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 7 }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.found);
        let entry = resp.entry.expect("entry");
        assert_eq!(entry.values.len(), 1);
        assert_eq!(entry.values[0].column_id, 0);

        // 表 999 不存在 → found=false（不因白名单含 (999,0) 而误报存在）。
        let resp999 = svc
            .get_snapshot(Request::new(GetSnapshotRequest { tag_id: 999 }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp999.found);
    }
}

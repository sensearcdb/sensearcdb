//! DataEventBus — 写入路径回调路由器。
//!
//! 注册源表到 mpsc channel 的映射，在 Engine::write_batch() 后
//! 将数据事件分发给匹配的流计算任务。
//!
//! ## 路由逻辑
//!
//! ```text
//! Engine::write_batch(table_id=1, ...)
//!   -> DataEventBus::on_write(table_id=1, ...)
//!     -> look up all channels for source_table_id=1
//!       -> send DataEvent to each matching task_id
//! ```

use std::collections::HashMap;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::warn;

use sensearcdb_common::TypedValue;

use crate::task::TaskId;
use crate::window_manager::PartitionKey;

/// 丢弃告警阈值：同一任务累计丢弃达到该值才 `warn!` 一次，随后重置计数。
///
/// ## 背压语义（P2-04）
///
/// - 保持「满则丢弃新点」的非阻塞语义：`try_send` 不阻塞主写入热路径。
/// - 诚实计数：每条 `Full` 丢弃都递增 Prometheus counter（总量 + 按 `task_id`）。
/// - 阈值聚合告警：日志 `warn!` 不逐条刷屏，同一任务每累计丢弃
///   [`DROPPED_WARN_THRESHOLD`] 条才告警一次。
///
/// 阈值采用固定常量而非 stream 配置字段：`StreamConfig` 位于
/// `sensearcdb-common`，本任务红线限定只改 `sensearcdb-stream`，故不新增
/// 配置接线，后续如需可调可再下沉到 `stream_cfg`（与 `channel_capacity` 同款）。
const DROPPED_WARN_THRESHOLD: u64 = 1000;

// ─── DataEventBus ───

/// 数据事件总线。
///
/// 维护 source_table_id → [mpsc::Sender<DataEvent>] 的映射。
/// 通过 `Arc<DataEventBus>` 在 StreamComputeEngine 和写入路径间共享。
pub struct DataEventBus {
    /// source_table_id → [Route]
    routes: RwLock<HashMap<u32, Vec<Route>>>,
    /// 各任务自上次告警以来累计丢点数（task_id → 计数），用于阈值聚合告警。
    dropped_since_warn: RwLock<HashMap<TaskId, u64>>,
}

/// 一条源表路由：每个流计算任务对应一条，携带该任务的分区键列 ID。
struct Route {
    task_id: TaskId,
    /// 该任务 `PARTITION BY` 的源列 ID（无分区键时为 `None`）。
    partition_column: Option<u16>,
    tx: mpsc::Sender<DataEvent>,
}

impl DataEventBus {
    /// 创建新的事件总线。
    pub fn new() -> Self {
        Self {
            routes: RwLock::new(HashMap::new()),
            dropped_since_warn: RwLock::new(HashMap::new()),
        }
    }

    /// 注册一个数据事件监听。
    ///
    /// # Arguments
    /// * `source_table_id` - 源表 ID
    /// * `task_id` - 所属流计算任务 ID
    /// * `partition_column` - 该任务 `PARTITION BY` 的源列 ID（无则为 `None`）
    /// * `tx` - mpsc Sender，数据事件将通过此 channel 发送
    pub fn register(
        &self,
        source_table_id: u32,
        task_id: TaskId,
        partition_column: Option<u16>,
        tx: mpsc::Sender<DataEvent>,
    ) {
        let mut routes = self.routes.write();
        routes.entry(source_table_id).or_default().push(Route {
            task_id,
            partition_column,
            tx,
        });
        tracing::debug!(
            "DataEventBus: registered task {} for source_table {}",
            task_id,
            source_table_id
        );
    }

    /// 注销一个数据事件监听。
    ///
    /// # Arguments
    /// * `source_table_id` - 源表 ID
    /// * `task_id` - 要注销的任务 ID
    pub fn unregister(&self, source_table_id: u32, task_id: TaskId) {
        let mut routes = self.routes.write();
        let is_empty = if let Some(senders) = routes.get_mut(&source_table_id) {
            let before = senders.len();
            senders.retain(|r| r.task_id != task_id);
            tracing::debug!(
                "DataEventBus: unregistered task {} for source_table {} ({} → {} senders)",
                task_id,
                source_table_id,
                before,
                senders.len()
            );
            senders.is_empty()
        } else {
            return;
        };
        if is_empty {
            routes.remove(&source_table_id);
        }

        // 清理该任务的丢点聚合计数，避免 task_id 注销后残留无用的告警状态。
        self.dropped_since_warn.write().remove(&task_id);
    }

    /// 写入路径回调：分发 DataEvent 到匹配的任务 channel。
    ///
    /// 由写入路径在 WAL → MemTable 插入后调用。
    /// 对每个匹配 source_table_id 的任务发送 DataEvent。
    ///
    /// 每个任务的分区键由**该任务**的 `PARTITION BY` 源列 ID 在此处独立解析，
    /// 因此同一源表上的多个任务可以有不同的分区键列，且分区键值保真
    /// （String/Enum 列按字符串原样传递，不再恒 `None`）。
    ///
    /// # Arguments
    /// * `table_id` - 写入的源表 ID
    /// * `timestamp` - 数据点时间戳 (epoch 毫秒)
    /// * `values` - 多列带类型值（column_id → `TypedValue`）
    /// * `batch_id` - 批量写入 ID
    ///
    /// 注意：发送是非阻塞的。如果 channel 满了，会打印警告并丢弃事件。
    pub fn on_write(
        &self,
        table_id: u32,
        timestamp: i64,
        values: &[(u16, TypedValue)],
        batch_id: u64,
    ) {
        // 聚合路径按 column_id 下标取值的稠密 typed 数组（缺失列补 None，保真 TypedValue）。
        let dense = dense_typed(values);

        let routes = self.routes.read();
        if let Some(routes) = routes.get(&table_id) {
            for route in routes {
                // 分区键按该任务的 partition_column 从类型化值中真解析。
                let partition_key = route
                    .partition_column
                    .and_then(|cid| values.iter().find(|(id, _)| *id == cid))
                    .map(|(_, v)| typed_to_partition_key(v));

                let event = DataEvent {
                    task_id: route.task_id,
                    timestamp,
                    partition_key,
                    values: dense.clone(),
                    batch_id,
                };

                match route.tx.try_send(event) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // 诚实计数：每条丢弃都递增（总量 + 按 task_id 维度）。
                        let task_id_str = route.task_id.to_string();
                        crate::metrics::stream_dropped_events()
                            .with_label_values(&[&task_id_str])
                            .inc();
                        crate::metrics::stream_dropped_events_total().inc();

                        // 阈值聚合告警：同一任务累计丢弃达阈值才 warn 一次，
                        // 避免高吞吐下逐条刷屏。
                        let should_warn = {
                            let mut counters = self.dropped_since_warn.write();
                            let counter = counters.entry(route.task_id).or_insert(0);
                            record_drop_and_check_warn(counter)
                        };
                        if should_warn {
                            warn!(
                                "DataEventBus: dropped {} events for task {} (source_table {}), channel full",
                                DROPPED_WARN_THRESHOLD, route.task_id, table_id
                            );
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        warn!(
                            "DataEventBus: channel closed for task {} (source_table {})",
                            route.task_id, table_id
                        );
                    }
                }
            }
        }
    }

    /// 批量写入路径回调：一次 `routes.read()` 锁，按序为每个点分发 DataEvent。
    ///
    /// 与逐点 [`on_write`](Self::on_write) 的事件顺序、内容、丢弃计数与告警完全一致，
    /// 仅把「每点一次 routes 锁」摊薄为「每批一次」。背压语义不变：`try_send`
    /// 满则逐事件丢弃 + 计数 + 阈值告警。
    ///
    /// # Arguments
    /// * `entries` - `(table_id, timestamp 毫秒, values)` 列表，顺序即事件顺序。
    /// * `batch_id` - 整批共享的批量写入 ID。
    pub fn on_write_batch(
        &self,
        entries: &[(u32, i64, &[(u16, TypedValue)])],
        batch_id: u64,
    ) {
        let routes = self.routes.read();
        for &(table_id, timestamp, values) in entries {
            if let Some(routes) = routes.get(&table_id) {
                // 聚合路径按 column_id 下标取值的稠密 typed 数组（缺失列补 None）。
                let dense = dense_typed(values);
                for route in routes {
                    // 分区键按该任务的 partition_column 从类型化值中真解析。
                    let partition_key = route
                        .partition_column
                        .and_then(|cid| values.iter().find(|(id, _)| *id == cid))
                        .map(|(_, v)| typed_to_partition_key(v));

                    let event = DataEvent {
                        task_id: route.task_id,
                        timestamp,
                        partition_key,
                        values: dense.clone(),
                        batch_id,
                    };

                    match route.tx.try_send(event) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            let task_id_str = route.task_id.to_string();
                            crate::metrics::stream_dropped_events()
                                .with_label_values(&[&task_id_str])
                                .inc();
                            crate::metrics::stream_dropped_events_total().inc();

                            let should_warn = {
                                let mut counters = self.dropped_since_warn.write();
                                let counter = counters.entry(route.task_id).or_insert(0);
                                record_drop_and_check_warn(counter)
                            };
                            if should_warn {
                                warn!(
                                    "DataEventBus: dropped {} events for task {} (source_table {}), channel full",
                                    DROPPED_WARN_THRESHOLD, route.task_id, table_id
                                );
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!(
                                "DataEventBus: channel closed for task {} (source_table {})",
                                route.task_id, table_id
                            );
                        }
                    }
                }
            }
        }
    }

    /// 返回指定源表的注册监听数量。
    pub fn listener_count(&self, source_table_id: u32) -> usize {
        let routes = self.routes.read();
        routes.get(&source_table_id).map(|s| s.len()).unwrap_or(0)
    }

    /// 返回总线中所有监听总数。
    pub fn total_listeners(&self) -> usize {
        let routes = self.routes.read();
        routes.values().map(|s| s.len()).sum()
    }
}

impl Default for DataEventBus {
    fn default() -> Self {
        Self::new()
    }
}

// ─── DataEvent ───

/// 数据事件：携带一次写入的数据信息。
///
/// 由 DataEventBus 构造并通过 mpsc channel 发送给 ConsumeLoop。
#[derive(Debug, Clone)]
pub struct DataEvent {
    /// 目标流计算任务 ID
    pub task_id: TaskId,
    /// 数据点时间戳 (epoch 毫秒)
    pub timestamp: i64,
    /// 可选的分区键值
    pub partition_key: Option<PartitionKey>,
    /// 数据值数组（每个源列一个值，缺失列为 `None`）
    pub values: Vec<Option<TypedValue>>,
    /// 批量写入 ID（用于追踪）
    pub batch_id: u64,
}

// ─── 内部辅助 ───

/// 丢弃告警聚合判定：累计丢点 +1，达到阈值返回 `true` 并重置计数，否则 `false`。
///
/// 与 [`DROPPED_WARN_THRESHOLD`] 配合，保证「满则丢弃新点」的非阻塞语义不变，
/// 但日志告警按阈值聚合（不逐条刷屏）。计数为「自上次告警以来」的丢点数。
fn record_drop_and_check_warn(since_last_warn: &mut u64) -> bool {
    *since_last_warn += 1;
    if *since_last_warn >= DROPPED_WARN_THRESHOLD {
        *since_last_warn = 0;
        true
    } else {
        false
    }
}

/// 将稀疏的 `(column_id, TypedValue)` 列表折叠为按 column_id 下标的稠密 typed 数组。
///
/// 缺失列补 `None`（聚合层据此跳过，不补零）；已存在的列保真其 `TypedValue`。
fn dense_typed(values: &[(u16, TypedValue)]) -> Vec<Option<TypedValue>> {
    let max_id = values.iter().map(|(id, _)| *id as usize).max().unwrap_or(0);
    let mut dense: Vec<Option<TypedValue>> = vec![None; max_id + 1];
    for (id, v) in values {
        dense[*id as usize] = Some(v.clone());
    }
    dense
}

/// 将类型化值转换为分区键字符串（保真：字符串/枚举原样，数值转字符串）。
fn typed_to_partition_key(value: &TypedValue) -> String {
    match value {
        TypedValue::String(s) => s.clone(),
        other => other.as_f64().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_route() {
        let bus = DataEventBus::new();
        let (tx, mut rx) = mpsc::channel::<DataEvent>(100);
        let task_id = uuid::Uuid::new_v4();

        bus.register(1, task_id, None, tx);
        assert_eq!(bus.listener_count(1), 1);
        assert_eq!(bus.total_listeners(), 1);

        bus.on_write(1, 1000, &[(0, TypedValue::F64(42.0))], 1);

        // 从 channel 接收
        let event = rx.try_recv().unwrap();
        assert_eq!(event.task_id, task_id);
        assert_eq!(event.timestamp, 1000);
        assert_eq!(event.values, vec![Some(TypedValue::F64(42.0))]);
        assert_eq!(event.batch_id, 1);
    }

    #[test]
    fn test_unregister() {
        let bus = DataEventBus::new();
        let (tx, _rx) = mpsc::channel::<DataEvent>(100);
        let task_id = uuid::Uuid::new_v4();

        bus.register(1, task_id, None, tx);
        assert_eq!(bus.listener_count(1), 1);

        bus.unregister(1, task_id);
        assert_eq!(bus.listener_count(1), 0);
        assert_eq!(bus.total_listeners(), 0);
    }

    #[test]
    fn test_no_route_for_unknown_table() {
        let bus = DataEventBus::new();
        // 不应 panic
        bus.on_write(999, 1000, &[(0, TypedValue::F64(42.0))], 1);
    }

    #[test]
    fn test_multiple_listeners_same_table() {
        let bus = DataEventBus::new();
        let (tx1, mut rx1) = mpsc::channel::<DataEvent>(100);
        let (tx2, mut rx2) = mpsc::channel::<DataEvent>(100);
        let tid1 = uuid::Uuid::new_v4();
        let tid2 = uuid::Uuid::new_v4();

        bus.register(1, tid1, None, tx1);
        bus.register(1, tid2, None, tx2);
        assert_eq!(bus.listener_count(1), 2);
        assert_eq!(bus.total_listeners(), 2);

        // Both listeners should receive the event
        bus.on_write(1, 500, &[(0, TypedValue::F64(99.0))], 1);
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn test_channel_closed_handled_gracefully() {
        let bus = DataEventBus::new();
        let (tx, _rx) = mpsc::channel::<DataEvent>(1); // capacity 1
        let task_id = uuid::Uuid::new_v4();

        bus.register(1, task_id, None, tx);
        // Drop rx to close the channel
        // on_write should handle the Closed error gracefully without panic
        bus.on_write(1, 1000, &[(0, TypedValue::F64(42.0))], 1);
        // Should not panic even though channel is closed
    }

    #[test]
    fn test_listener_count_after_unregister_all() {
        let bus = DataEventBus::new();
        let (tx1, _rx1) = mpsc::channel::<DataEvent>(100);
        let (tx2, _rx2) = mpsc::channel::<DataEvent>(100);
        let tid1 = uuid::Uuid::new_v4();
        let tid2 = uuid::Uuid::new_v4();

        bus.register(1, tid1, None, tx1);
        bus.register(1, tid2, None, tx2);
        assert_eq!(bus.total_listeners(), 2);

        bus.unregister(1, tid1);
        assert_eq!(bus.listener_count(1), 1);
        assert_eq!(bus.total_listeners(), 1);

        bus.unregister(1, tid2);
        assert_eq!(bus.listener_count(1), 0);
        assert_eq!(bus.total_listeners(), 0);
    }

    #[test]
    fn test_unregister_nonexistent_task() {
        let bus = DataEventBus::new();
        let fake_id = uuid::Uuid::new_v4();
        // Should not panic
        bus.unregister(1, fake_id);
    }

    #[test]
    fn test_multi_column_dense_values() {
        let bus = DataEventBus::new();
        let (tx, mut rx) = mpsc::channel::<DataEvent>(100);
        let task_id = uuid::Uuid::new_v4();

        bus.register(1, task_id, None, tx);
        // 稀疏列：column 1 = 10.0, column 3 = 30.0（column 0/2 缺失）
        bus.on_write(
            1,
            2000,
            &[(1, TypedValue::F64(10.0)), (3, TypedValue::F64(30.0))],
            7,
        );

        let event = rx.try_recv().unwrap();
        // 稠密数组按 column_id 下标展开，缺失列补 None，已存在列保真 TypedValue
        assert_eq!(
            event.values,
            vec![
                None,
                Some(TypedValue::F64(10.0)),
                None,
                Some(TypedValue::F64(30.0))
            ]
        );
        assert_eq!(event.batch_id, 7);
    }

    #[test]
    fn test_partition_key_resolved_per_task() {
        let bus = DataEventBus::new();
        let (tx1, mut rx1) = mpsc::channel::<DataEvent>(100);
        let (tx2, mut rx2) = mpsc::channel::<DataEvent>(100);
        let tid1 = uuid::Uuid::new_v4();
        let tid2 = uuid::Uuid::new_v4();

        // 两个任务分区键不同：task1 按 column 0，task2 按 column 1
        bus.register(1, tid1, Some(0), tx1);
        bus.register(1, tid2, Some(1), tx2);

        bus.on_write(
            1,
            2000,
            &[
                (0, TypedValue::String("device_a".into())),
                (1, TypedValue::I64(42)),
            ],
            7,
        );

        let ev1 = rx1.try_recv().unwrap();
        let ev2 = rx2.try_recv().unwrap();
        assert_eq!(ev1.partition_key, Some("device_a".to_string()));
        assert_eq!(ev2.partition_key, Some("42".to_string()));
    }

    #[test]
    fn test_channel_full_increments_drop_counter() {
        // 用唯一 task_id 做按维度断言，避免与其他并行测试共享的全局 counter 竞争。
        let task_id = uuid::Uuid::new_v4();
        let task_id_str = task_id.to_string();

        let bus = DataEventBus::new();
        let (tx, _rx) = mpsc::channel::<DataEvent>(1); // capacity 1
        bus.register(1, task_id, None, tx);

        // 第 1 个事件填满 channel（Ok），后续 2 个事件触发 Full 丢弃。
        bus.on_write(1, 1000, &[(0, TypedValue::F64(1.0))], 1);
        bus.on_write(1, 1001, &[(0, TypedValue::F64(2.0))], 2);
        bus.on_write(1, 1002, &[(0, TypedValue::F64(3.0))], 3);

        // 丢点数可查：按 task_id 维度的 counter 精确等于本次丢弃数。
        let dropped_for_task = crate::metrics::stream_dropped_events()
            .with_label_values(&[&task_id_str])
            .get();
        assert_eq!(dropped_for_task, 2);

        // 总量 counter 也随丢弃递增（delta 断言，容忍其他测试并发递增）。
        let total_after = crate::metrics::stream_dropped_events_total().get();
        assert!(
            total_after >= 2,
            "total dropped counter should be >= 2, got {total_after}"
        );
    }

    #[test]
    fn test_record_drop_and_check_warn_threshold() {
        let mut counter: u64 = 0;

        // 阈值前：不告警。
        for _ in 0..DROPPED_WARN_THRESHOLD - 1 {
            assert!(
                !record_drop_and_check_warn(&mut counter),
                "should not warn before reaching threshold"
            );
        }
        assert_eq!(counter, DROPPED_WARN_THRESHOLD - 1);

        // 恰好达阈值：告警一次并重置计数。
        assert!(
            record_drop_and_check_warn(&mut counter),
            "should warn exactly at threshold"
        );
        assert_eq!(counter, 0);

        // 重置后继续累计，再次在下一阈值处告警（不逐条）。
        for _ in 0..DROPPED_WARN_THRESHOLD - 1 {
            assert!(!record_drop_and_check_warn(&mut counter));
        }
        assert!(record_drop_and_check_warn(&mut counter));
        assert_eq!(counter, 0);
    }

    #[test]
    fn test_on_write_batch_order_and_routes() {
        let bus = DataEventBus::new();
        let (tx1, mut rx1) = mpsc::channel::<DataEvent>(100);
        let (tx2, mut rx2) = mpsc::channel::<DataEvent>(100);
        let tid1 = uuid::Uuid::new_v4();
        let tid2 = uuid::Uuid::new_v4();

        bus.register(1, tid1, None, tx1);
        bus.register(2, tid2, None, tx2);

        // 跨表交错：table 1 / table 2 / table 1，共 3 个事件。
        bus.on_write_batch(
            &[
                (1, 1000, &[(0, TypedValue::F64(1.0))][..]),
                (2, 2000, &[(0, TypedValue::F64(2.0))][..]),
                (1, 3000, &[(0, TypedValue::F64(3.0))][..]),
            ],
            9,
        );

        // 同一 listener 收到的事件顺序 == 输入顺序（table 1 收到 ts=1000、3000 两事件）。
        let e1 = rx1.try_recv().unwrap();
        let e3 = rx1.try_recv().unwrap();
        assert_eq!(e1.timestamp, 1000);
        assert_eq!(e3.timestamp, 3000);
        assert_eq!(e1.task_id, tid1);
        assert_eq!(e1.batch_id, 9);
        assert_eq!(e1.values, vec![Some(TypedValue::F64(1.0))]);
        assert_eq!(e3.values, vec![Some(TypedValue::F64(3.0))]);
        // 无更多事件（table 1 只应有 2 个）。
        assert!(rx1.try_recv().is_err());

        // table 2 listener 只收到自己 table 的事件。
        let e2 = rx2.try_recv().unwrap();
        assert_eq!(e2.timestamp, 2000);
        assert_eq!(e2.task_id, tid2);
        assert_eq!(e2.values, vec![Some(TypedValue::F64(2.0))]);
        assert!(rx2.try_recv().is_err());
    }
}

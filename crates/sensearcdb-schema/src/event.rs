//! SchemaEvent 与 SchemaEventBus。
//!
//! DDL 操作完成后通过 `SchemaEventBus` 广播变更事件，下游模块（engine、snapshot）
//! 接收事件后热更新其内部状态（SchemaMirror 更新、TagMetadataStore 缓存失效等）。
//!
//! 使用 `tokio::sync::broadcast` 实现多消费者广播，容量 256。

use crate::mirror::TableInfo;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Schema DDL 变更事件枚举。
///
/// 每次 DDL 操作（CREATE TABLE / ALTER TABLE / DROP TABLE）完成后广播对应事件。
#[derive(Debug, Clone)]
pub enum SchemaEvent {
    /// 表创建完成事件。
    TableCreated {
        /// 新分配的表 ID。
        table_id: u32,
        /// 完整的 TableInfo（包含所有列定义）。
        table_info: Arc<TableInfo>,
    },
    /// 表结构变更事件（增加/删除/修改列）。
    TableAltered {
        /// 被修改的表 ID。
        table_id: u32,
        /// 新的 schema 版本号（自增）。
        new_version: u32,
        /// 新的 schema_id（列变可能导致 hash 变化）。
        new_schema_id: u32,
    },
    /// 表删除事件。
    TableDeleted {
        /// 被删除的表 ID。
        table_id: u32,
    },
}

/// Schema 事件总线。
///
/// 基于 `tokio::sync::broadcast`，容量 256。
/// - DDL 变更频率 < 1Hz，256 容量足以在消费者 lag 时保留足够历史。
/// - 所有消费者通过 `subscribe()` 获取独立 `Receiver`。
///
/// # Usage
/// ```ignore
/// let bus = SchemaEventBus::new();
/// let mut rx = bus.subscribe();
/// bus.broadcast(SchemaEvent::TableCreated { table_id: 1, table_info: Arc::new(info) });
/// let event = rx.recv().await.unwrap();
/// ```
pub struct SchemaEventBus {
    tx: broadcast::Sender<SchemaEvent>,
}

impl SchemaEventBus {
    /// 创建新的事件总线，内部 channel 容量为 `capacity`。
    ///
    /// 推荐容量：256。
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }

    /// 订阅 schema 事件流。
    ///
    /// 返回的 `Receiver` 仅接收订阅后广播的事件，不回溯历史。
    pub fn subscribe(&self) -> broadcast::Receiver<SchemaEvent> {
        self.tx.subscribe()
    }

    /// 广播 schema 事件到所有订阅者。
    ///
    /// 如果没有任何活跃的 `Receiver`，事件被静默丢弃。
    /// 如果某个 `Receiver` lag 超过 256 条消息，其下一次 `recv()` 将返回
    /// `RecvError::Lagged(n)`，消费者应据此执行全量刷新。
    pub fn broadcast(&self, event: SchemaEvent) {
        let _ = self.tx.send(event);
    }

    /// 返回当前活跃的 Receiver 数量。
    #[allow(dead_code)]
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for SchemaEventBus {
    /// 默认容量 256。
    fn default() -> Self {
        Self::new(256)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bus() {
        let bus = SchemaEventBus::new(256);
        assert_eq!(bus.receiver_count(), 0);
    }

    #[tokio::test]
    async fn test_subscribe_and_receive() {
        let bus = SchemaEventBus::new(256);
        let mut rx = bus.subscribe();

        bus.broadcast(SchemaEvent::TableDeleted { table_id: 42 });

        match rx.recv().await {
            Ok(SchemaEvent::TableDeleted { table_id }) => assert_eq!(table_id, 42),
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_broadcast_without_subscriber_does_not_block() {
        let bus = SchemaEventBus::new(256);
        // No subscribers — should not panic or block
        bus.broadcast(SchemaEvent::TableDeleted { table_id: 1 });
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = SchemaEventBus::new(256);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        bus.broadcast(SchemaEvent::TableDeleted { table_id: 99 });

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();

        match (e1, e2) {
            (
                SchemaEvent::TableDeleted { table_id: t1 },
                SchemaEvent::TableDeleted { table_id: t2 },
            ) => {
                assert_eq!(t1, 99);
                assert_eq!(t2, 99);
            }
            _ => panic!("unexpected events"),
        }
    }

    #[test]
    fn test_default_capacity() {
        let bus = SchemaEventBus::default();
        let rx = bus.subscribe();
        let _rx2 = bus.subscribe();
        assert_eq!(bus.receiver_count(), 2);
        drop(rx);
    }
}

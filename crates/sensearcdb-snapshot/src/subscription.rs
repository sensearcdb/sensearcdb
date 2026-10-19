//! SnapshotSubscriptionManager — tag 级别的发布/订阅通知系统。
//!
//! ## 架构
//!
//! 维护三层订阅索引：
//! 1. `subscriptions` — `SubscriptionId → SnapshotSubscription`
//! 2. `tag_to_subscriptions` — `table_id → Set<SubscriptionId>`
//! 3. `global_subscriptions` — 全局订阅（接收所有 tag 变更）
//!
//! ## Fire-and-Forget 通知
//!
//! `notify()` 方法不阻塞写入路径：
//! - 对每个匹配的订阅尝试 `mpsc::Sender::try_send()`。
//! - 如果接收端通道已满（消费者太慢），**丢弃该通知**（不阻塞写入线程）。
//! - 日志记录丢弃次数，供运维监控。
//!
//! ## 与 SchemaEventBus 的区别
//!
//! | 特征              | SchemaEventBus         | SnapshotSubscriptionManager |
//! |-------------------|------------------------|-----------------------------|
//! | 底层通道           | tokio::sync::broadcast  | tokio::sync::mpsc (per-subscription) |
//! | 数据完整性          | 弱（Lagged→静默覆盖）    | 弱（try_send 失败→丢弃）    |
//! | 适用场景            | DDL 低频事件 (<1Hz)     | 数据变更通知 (高频)         |
//! | 无订阅者时行为       | send 静默成功           | subscriber_count==0 跳过   |

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::entry::SnapshotEntry;

/// 通道满告警节流间隔（10s）。
const FULL_WARN_INTERVAL_MS: i64 = 10_000;

/// 订阅唯一标识符（UUID v4 字符串）。
pub type SubscriptionId = String;

/// 订阅者接收的变更通知。
#[derive(Debug, Clone)]
pub struct DataChangeNotification {
    /// 发生变化的 table_id。
    pub table_id: u32,
    /// 变更后的快照条目。
    pub entry: Arc<SnapshotEntry>,
    /// 全局版本号。
    pub version: u64,
}

/// 快照订阅定义。
#[derive(Debug, Clone)]
pub struct SnapshotSubscription {
    /// 订阅 ID。
    pub id: SubscriptionId,
    /// 订阅的 tag 列表（空 = 订阅全部）。
    pub tag_ids: Vec<u32>,
    /// 通知通道发送端。
    pub tx: mpsc::Sender<DataChangeNotification>,
    /// 是否需要初始快照。
    pub wants_initial_snapshot: bool,
}

/// 快照订阅管理器。
///
/// 维护三层订阅索引，提供 fire-and-forget 通知。
pub struct SnapshotSubscriptionManager {
    /// SubscriptionId → SnapshotSubscription
    subscriptions: RwLock<HashMap<SubscriptionId, SnapshotSubscription>>,
    /// table_id → 订阅该 tag 的 SubscriptionId 集合
    tag_to_subscriptions: RwLock<HashMap<u32, HashSet<SubscriptionId>>>,
    /// 全局订阅（接收所有 tag 变更的 SubscriptionId 集合）
    global_subscriptions: RwLock<HashSet<SubscriptionId>>,
    /// tag 特定订阅通道满导致的累计丢弃计数（节流告警用）。
    dropped_tag: AtomicU64,
    /// 全局订阅通道满导致的累计丢弃计数（节流告警用）。
    dropped_global: AtomicU64,
    /// tag 特定订阅「通道满」汇总告警的节流时间戳（epoch 毫秒）。
    last_full_warn_ms_tag: AtomicI64,
    /// 全局订阅「通道满」汇总告警的节流时间戳（epoch 毫秒）。
    last_full_warn_ms_global: AtomicI64,
    /// 活跃订阅总数（保守上界，可能偏大绝不偏小）。用于写入路径无订阅者短路。
    subscriber_count: AtomicUsize,
}

impl SnapshotSubscriptionManager {
    /// 创建空的订阅管理器。
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            tag_to_subscriptions: RwLock::new(HashMap::new()),
            global_subscriptions: RwLock::new(HashSet::new()),
            dropped_tag: AtomicU64::new(0),
            dropped_global: AtomicU64::new(0),
            last_full_warn_ms_tag: AtomicI64::new(0),
            last_full_warn_ms_global: AtomicI64::new(0),
            subscriber_count: AtomicUsize::new(0),
        }
    }

    /// 注册订阅并返回 SubscriptionId。
    ///
    /// 自动生成 UUID v4 作为订阅 ID。
    /// 如果 `tag_ids` 为空，视为全局订阅（接收所有 tag 变更）。
    ///
    /// # Arguments
    ///
    /// - `tag_ids`: 订阅的 tag 列表（空 = 全局订阅）。
    /// - `tx`: 通知通道发送端。
    /// - `wants_initial_snapshot`: 是否需要初始快照。
    pub fn subscribe(
        &self,
        tag_ids: Vec<u32>,
        tx: mpsc::Sender<DataChangeNotification>,
        wants_initial_snapshot: bool,
    ) -> SubscriptionId {
        let id = Uuid::new_v4().to_string();

        let sub = SnapshotSubscription {
            id: id.clone(),
            tag_ids: tag_ids.clone(),
            tx,
            wants_initial_snapshot,
        };

        // 统一锁序：先取 `subscriptions` 写锁，再取 tag/global 索引写锁。
        // 持 `subscriptions` 写锁期间完成索引写入，既能保证「索引先于订阅对外可见」，
        // 又与 `unsubscribe`/`notify` 的锁序一致，杜绝 AB-BA 死锁。
        let mut subs = self.subscriptions.write();
        if tag_ids.is_empty() {
            self.global_subscriptions.write().insert(id.clone());
        } else {
            // 按 tag 索引
            let mut tag_map = self.tag_to_subscriptions.write();
            for &tag_id in &tag_ids {
                tag_map.entry(tag_id).or_default().insert(id.clone());
            }
        }
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);
        subs.insert(id.clone(), sub);
        id
    }

    /// 取消订阅。
    ///
    /// 返回 `true` 表示订阅存在并已取消，`false` 表示订阅不存在。
    pub fn unsubscribe(&self, id: &SubscriptionId) -> bool {
        let mut subs = self.subscriptions.write();
        if let Some(sub) = subs.remove(id) {
            // 从 tag 索引中移除
            if sub.tag_ids.is_empty() {
                self.global_subscriptions.write().remove(id);
            } else {
                let mut tag_map = self.tag_to_subscriptions.write();
                for tag_id in &sub.tag_ids {
                    if let std::collections::hash_map::Entry::Occupied(mut entry) =
                        tag_map.entry(*tag_id)
                    {
                        entry.get_mut().remove(id);
                        if entry.get().is_empty() {
                            entry.remove();
                        }
                    }
                }
            }
            self.subscriber_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// 记录一次通道满丢弃并节流告警。
    ///
    /// 高频数据路径上，逐条 `warn!` 会刷爆日志（L3 灌数曾产生 227 万条 WARN）。
    /// 本方法将告警降频为：
    /// - 首次丢弃立即 warn（快速暴露问题）；
    /// - 此后每 10s 汇总一条（`dropped_in_interval` 携带区间累计丢弃数）并清零。
    fn warn_throttled(
        &self,
        counter: &AtomicU64,
        last_warn_ms: &AtomicI64,
        kind: &str,
        table_id: u32,
        sub_id: &str,
    ) {
        let dropped = counter.fetch_add(1, Ordering::Relaxed) + 1;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let last = last_warn_ms.load(Ordering::Relaxed);

        if dropped == 1 {
            tracing::warn!(
                table_id = table_id,
                subscription_id = %sub_id,
                kind = kind,
                dropped = dropped,
                "snapshot subscription channel full, dropping notification (first in interval)"
            );
            last_warn_ms.store(now, Ordering::Relaxed);
        } else if now.saturating_sub(last) >= FULL_WARN_INTERVAL_MS {
            tracing::warn!(
                table_id = table_id,
                subscription_id = %sub_id,
                kind = kind,
                dropped_in_interval = dropped,
                interval_ms = FULL_WARN_INTERVAL_MS,
                "snapshot subscription channel full, dropping notifications (throttled summary)"
            );
            counter.store(0, Ordering::Relaxed);
            last_warn_ms.store(now, Ordering::Relaxed);
        }
    }

    /// Fire-and-forget 通知：向所有订阅了 `table_id` 的订阅者推送变更。
    ///
    /// ## 行为
    ///
    /// - 不阻塞写入路径：使用 `try_send`。
    /// - 通道满时丢弃通知并记录 warn 日志。
    /// - 无订阅者时跳过（零开销快路径）。
    /// - 先通知 tag 特定订阅，再通知全局订阅。
    pub fn notify(&self, table_id: u32, entry: Arc<SnapshotEntry>, version: u64) {
        let notification = DataChangeNotification {
            table_id,
            entry,
            version,
        };
        let mut global_count = 0usize;

        // 1. 通知 tag 特定订阅。
        //
        // 采用「短锁 + 克隆句柄」：先取 tag 索引克隆命中的订阅 ID 集合并释放锁，
        // 再取 `subscriptions` 克隆对应 Sender 并释放锁，最后在无锁状态下 `try_send`。
        // 任意时刻最多持有一把锁，因此不会与 subscribe/unsubscribe 形成 AB-BA 死锁，
        // 同时保留「无 tag 订阅者不取 subscriptions 锁」的热路径优化。
        {
            let sub_ids: Vec<SubscriptionId> = {
                let tag_map = self.tag_to_subscriptions.read();
                match tag_map.get(&table_id) {
                    Some(ids) => ids.iter().cloned().collect(),
                    None => Vec::new(),
                }
            };
            if !sub_ids.is_empty() {
                let targets: Vec<(SubscriptionId, mpsc::Sender<DataChangeNotification>)> = {
                    let subs = self.subscriptions.read();
                    sub_ids
                        .iter()
                        .filter_map(|id| subs.get(id).map(|s| (id.clone(), s.tx.clone())))
                        .collect()
                };
                for (sub_id, tx) in targets {
                    match tx.try_send(notification.clone()) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            self.warn_throttled(
                                &self.dropped_tag,
                                &self.last_full_warn_ms_tag,
                                "tag",
                                table_id,
                                &sub_id,
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            tracing::debug!(
                                subscription_id = %sub_id,
                                "subscription channel closed"
                            );
                        }
                    }
                }
            }
        }

        // 2. 通知全局订阅（同样采用短锁 + 克隆句柄，任意时刻最多持有一把锁）。
        {
            let global_ids: Vec<SubscriptionId> = {
                let global_subs = self.global_subscriptions.read();
                global_subs.iter().cloned().collect()
            };
            if !global_ids.is_empty() {
                let targets: Vec<(SubscriptionId, mpsc::Sender<DataChangeNotification>)> = {
                    let subs = self.subscriptions.read();
                    global_ids
                        .iter()
                        .filter_map(|id| subs.get(id).map(|s| (id.clone(), s.tx.clone())))
                        .collect()
                };
                for (sub_id, tx) in targets {
                    match tx.try_send(notification.clone()) {
                        Ok(_) => {
                            global_count += 1;
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            self.warn_throttled(
                                &self.dropped_global,
                                &self.last_full_warn_ms_global,
                                "global",
                                table_id,
                                &sub_id,
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            tracing::debug!(
                                subscription_id = %sub_id,
                                "global subscription channel closed"
                            );
                        }
                    }
                }
            }
        }

        tracing::trace!(
            table_id = table_id,
            version = version,
            global_notified = global_count,
            "snapshot notification dispatched"
        );
    }

    /// 返回订阅了 `table_id` 的订阅者数量（含全局订阅）。
    pub fn subscriber_count(&self, table_id: u32) -> usize {
        let tag_count = self
            .tag_to_subscriptions
            .read()
            .get(&table_id)
            .map(|s| s.len())
            .unwrap_or(0);
        let global_count = self.global_subscriptions.read().len();
        tag_count + global_count
    }

    /// 返回活跃订阅总数。
    pub fn total_subscriptions(&self) -> usize {
        self.subscriptions.read().len()
    }

    /// 是否存在任意活跃订阅者（保守上界，可能偏大绝不偏小）。
    ///
    /// 用于写入路径无订阅者短路：计数由 `subscribe`/`unsubscribe` 以
    /// `Relaxed` 维护，偏大仅导致多做一次 `Arc::clone`（不丢通知），
    /// 绝不漏通知。
    pub fn has_any_subscriber(&self) -> bool {
        self.subscriber_count.load(Ordering::Relaxed) > 0
    }

    /// 按 tag_id 批量获取订阅者数量。
    pub fn batch_subscriber_counts(&self, table_ids: &[u32]) -> Vec<(u32, usize)> {
        let tag_map = self.tag_to_subscriptions.read();
        let global_count = self.global_subscriptions.read().len();
        table_ids
            .iter()
            .map(|&tid| {
                let tag_count = tag_map.get(&tid).map(|s| s.len()).unwrap_or(0);
                (tid, tag_count + global_count)
            })
            .collect()
    }
}

impl Default for SnapshotSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SnapshotSubscriptionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotSubscriptionManager")
            .field("total_subscriptions", &self.total_subscriptions())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::SnapshotEntry;
    use sensearcdb_common::TypedValue;

    #[test]
    fn test_has_any_subscriber_flip() {
        let mgr = SnapshotSubscriptionManager::new();
        assert!(!mgr.has_any_subscriber());

        let (tx, _rx) = mpsc::channel(16);
        let id = mgr.subscribe(vec![1], tx, false);
        assert!(mgr.has_any_subscriber());

        assert!(mgr.unsubscribe(&id));
        assert!(!mgr.has_any_subscriber());
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let mgr = SnapshotSubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);

        let id = mgr.subscribe(vec![1, 2], tx, false);
        assert_eq!(mgr.total_subscriptions(), 1);
        assert_eq!(mgr.subscriber_count(1), 1);
        assert_eq!(mgr.subscriber_count(3), 0);

        assert!(mgr.unsubscribe(&id));
        assert_eq!(mgr.total_subscriptions(), 0);
        assert_eq!(mgr.subscriber_count(1), 0);

        // Channel should be closed (drop on mgr side)
        drop(mgr);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_global_subscription() {
        let mgr = SnapshotSubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        let id = mgr.subscribe(vec![], tx, false);
        assert_eq!(mgr.subscriber_count(1), 1);
        assert_eq!(mgr.subscriber_count(99), 1);

        assert!(mgr.unsubscribe(&id));
        assert_eq!(mgr.subscriber_count(1), 0);
    }

    #[test]
    fn test_notify_fire_and_forget() {
        let mgr = SnapshotSubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);

        mgr.subscribe(vec![42], tx, false);

        let entry = Arc::new(SnapshotEntry::new_typed(
            sensearcdb_common::TenantId::DEFAULT,
            vec![(0, TypedValue::F64(99.5))],
            vec![(0, 0xC0)],
            1000,
            1001,
            7,
            false,
            None,
        ));
        mgr.notify(42, entry, 7);

        // Should receive notification
        let notif = rx.try_recv().unwrap();
        assert_eq!(notif.table_id, 42);
        assert_eq!(notif.version, 7);
        assert_eq!(notif.entry.value(), 99.5);
    }

    #[test]
    fn test_notify_no_subscriber_short_circuit() {
        let mgr = SnapshotSubscriptionManager::new();
        // No subscribers — should not panic
        let entry = Arc::new(SnapshotEntry::default());
        mgr.notify(1, entry, 1);
        assert_eq!(mgr.subscriber_count(1), 0);
    }

    // ─── 独立 QA（qa-snapshot-subscribe）：限流节流正确性 ───

    /// 首次丢弃立即 warn（记录时间戳），10s 内仅累加不 summary，超 10s 触发 summary 并清零。
    #[test]
    fn qa_warn_throttled_first_then_summary_and_reset() {
        let mgr = SnapshotSubscriptionManager::new();

        // 首次：立即上报并记录时间戳。
        mgr.warn_throttled(
            &mgr.dropped_global,
            &mgr.last_full_warn_ms_global,
            "global",
            7,
            "sub-1",
        );
        assert_eq!(mgr.dropped_global.load(Ordering::Relaxed), 1);
        let first_ts = mgr.last_full_warn_ms_global.load(Ordering::Relaxed);
        assert!(first_ts > 0, "first warn should record a timestamp");

        // 10s 内：仅累加，不 summary，时间戳不变。
        for _ in 0..100 {
            mgr.warn_throttled(
                &mgr.dropped_global,
                &mgr.last_full_warn_ms_global,
                "global",
                7,
                "sub-1",
            );
        }
        assert_eq!(mgr.dropped_global.load(Ordering::Relaxed), 101);
        assert_eq!(
            mgr.last_full_warn_ms_global.load(Ordering::Relaxed),
            first_ts
        );

        // 时间推进 >10s：下一次触发 summary 并清零计数。
        mgr.last_full_warn_ms_global
            .store(first_ts - 11_000, Ordering::Relaxed);
        mgr.warn_throttled(
            &mgr.dropped_global,
            &mgr.last_full_warn_ms_global,
            "global",
            7,
            "sub-1",
        );
        assert_eq!(mgr.dropped_global.load(Ordering::Relaxed), 0);
        // 时间戳应被重置为「当前时间」（毫秒级分辨率下可能等于 first_ts，故用 >=）。
        assert!(mgr.last_full_warn_ms_global.load(Ordering::Relaxed) >= first_ts);
    }

    /// tag / global 两个计数器独立：tag 的首次丢弃不影响 global 计数与节流时间戳。
    #[test]
    fn qa_warn_throttled_tag_global_counters_independent() {
        let mgr = SnapshotSubscriptionManager::new();
        mgr.warn_throttled(&mgr.dropped_tag, &mgr.last_full_warn_ms_tag, "tag", 1, "s");
        mgr.warn_throttled(
            &mgr.dropped_global,
            &mgr.last_full_warn_ms_global,
            "global",
            1,
            "s",
        );
        assert_eq!(mgr.dropped_tag.load(Ordering::Relaxed), 1);
        assert_eq!(mgr.dropped_global.load(Ordering::Relaxed), 1);

        // 两个计数器各自持有节流时间戳：tag 的告警不应推进/占用 global 的窗口。
        assert!(mgr.last_full_warn_ms_tag.load(Ordering::Relaxed) > 0);
        assert!(mgr.last_full_warn_ms_global.load(Ordering::Relaxed) > 0);

        // 人为把 global 时间戳回拨到很久以前，验证 tag 时间戳仍保持独立（不被 global 覆盖）。
        let global_ts = mgr.last_full_warn_ms_global.load(Ordering::Relaxed);
        let tag_ts = mgr.last_full_warn_ms_tag.load(Ordering::Relaxed);
        mgr.last_full_warn_ms_global
            .store(global_ts - 20_000, Ordering::Relaxed);
        assert_eq!(mgr.last_full_warn_ms_tag.load(Ordering::Relaxed), tag_ts);
    }

    /// 通道满时 notify 不 panic、不阻塞，丢弃计数正确累加（多线程并发安全）。
    #[test]
    fn qa_notify_full_channel_concurrent_no_panic() {
        let mgr = Arc::new(SnapshotSubscriptionManager::new());
        let (tx, _rx) = mpsc::channel::<DataChangeNotification>(1);
        mgr.subscribe(Vec::new(), tx, false); // 全局订阅，容量 1

        let entry = Arc::new(SnapshotEntry::default());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let m = mgr.clone();
            let e = entry.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..500 {
                    m.notify(1, e.clone(), 1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // 容量 1 的全局通道只容纳 1 条，其余全部丢弃并计数（节流 warn 不 panic）。
        assert!(mgr.dropped_global.load(Ordering::Relaxed) > 0);
        assert_eq!(mgr.dropped_tag.load(Ordering::Relaxed), 0);
    }

    /// 高频 notify 与并发 subscribe/unsubscribe 同时发生，必须在超时内正常结束（无 AB-BA 死锁）。
    ///
    /// 旧实现的锁序为 notify: `tag/global → subscriptions`，unsubscribe:
    /// `subscriptions → tag/global`，二者相反，在高频写 + 断连并发下可触发 AB-BA 死锁。
    /// 本测试用 4 个 notify 线程 + 2 个反复订阅/退订线程高频竞争，通过「done 通道 + 超时」
    /// 证明所有工作线程都能在 30s 内完成。
    #[test]
    fn qa_notify_unsubscribe_concurrent_no_deadlock() {
        let mgr = Arc::new(SnapshotSubscriptionManager::new());
        let entry = Arc::new(SnapshotEntry::default());

        // 预注册一批 tag 订阅与一个全局订阅，保证 notify 同时命中 tag / global 两条路径。
        let mut seed_ids = Vec::new();
        for tag in 0..64u32 {
            let (tx, _rx) = mpsc::channel::<DataChangeNotification>(128);
            let id = mgr.subscribe(vec![tag], tx, false);
            seed_ids.push(id);
        }
        let (gtx, _grx) = mpsc::channel::<DataChangeNotification>(128);
        mgr.subscribe(Vec::new(), gtx, false);

        const WORKER_COUNT: usize = 6;
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        // 4 个高频 notify 线程。
        for _ in 0..4 {
            let m = mgr.clone();
            let e = entry.clone();
            let done = done_tx.clone();
            std::thread::spawn(move || {
                for i in 0..50_000u64 {
                    m.notify((i % 64) as u32, e.clone(), i);
                }
                let _ = done.send(());
            });
        }

        // 2 个反复 subscribe/unsubscribe 线程（模拟客户端断连清理）。
        for _ in 0..2 {
            let m = mgr.clone();
            let done = done_tx.clone();
            std::thread::spawn(move || {
                for _ in 0..5_000 {
                    let (tx, _rx) = mpsc::channel::<DataChangeNotification>(16);
                    let id = m.subscribe(vec![1, 2, 3], tx, false);
                    let _ = m.unsubscribe(&id);
                }
                let _ = done.send(());
            });
        }
        drop(done_tx); // 主线程不再持有发送端

        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();
        let mut received = 0usize;
        while received < WORKER_COUNT {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                panic!(
                    "deadlock detected: only {received}/{WORKER_COUNT} workers finished within 30s"
                );
            }
            match done_rx.recv_timeout(remaining) {
                Ok(()) => received += 1,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    panic!(
                        "deadlock detected: only {received}/{WORKER_COUNT} workers finished within 30s"
                    );
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        assert_eq!(received, WORKER_COUNT);

        // 清理种子订阅，验证并发竞争后索引与订阅表保持一致。
        for id in &seed_ids {
            assert!(mgr.unsubscribe(id));
        }
    }
}

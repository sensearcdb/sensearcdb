//! MemTableManager：`RwLock<MemTableState>` 双缓冲状态机管理。
//!
//! ## 并发正确性修复（方案 v5 §5 方案 1 + v6 §6 immutable 队列化）
//!
//! 用一把 `RwLock` 原子管理 `{ active, immutable }`，取代原先
//! 「`ArcSwap` active + 独立 `Mutex<Option>` immutable」的两步非原子结构，
//! 消除两个数据正确性竞态：
//!
//! - **竞态 1（漏读）**：`force_freeze` 在写锁内一步完成 active↔immutable 切换；
//!   查询在读锁内一次性取出 pair，不再存在「active 空 + immutable 未就位」间隙。
//! - **竞态 2（丢写）**：`insert`/`insert_batch` 拿读锁、`force_freeze` 拿写锁，
//!   二者互斥 —— insert 不可能在 freeze 换表之后继续写入旧表（孤儿表）。
//!
//! ## immutable 队列化（P5）+ 两级背压（P6，v6 §6.3）
//!
//! - `immutable` 为 **FIFO 队列**（`VecDeque`）：`force_freeze` `push_back`（不覆盖），
//!   FlushCoordinator `pop_immutable`（`pop_front`）消费；flush 失败时 `push_front` 重塞。
//! - 两级背压：低阈值（深度 > `soft_limit`）log error + metric；高阈值
//!   （深度 > `hard_limit` 或 active+Σimmutable 内存 > `mem_limit_bytes`）write stall
//!   背压写入，防止异常场景 OOM。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use sensearcdb_common::{write_attr, ConfigHandle};

use crate::engine::DataPoint;
use crate::memtable::col_vec::TypedValue;
use crate::memtable::table::ColumnarMemTable;

/// 低阈值软告警默认值（队列深度 > 4 → log error + metric）。
const DEFAULT_IMMUTABLE_SOFT_LIMIT: usize = 4;
/// 高阈值 write stall 默认值（队列深度 > 64 → 背压写入）。
const DEFAULT_IMMUTABLE_HARD_LIMIT: usize = 64;
/// 内存上限默认值（active + Σimmutable 估算字节 > 512MiB → write stall）。
const DEFAULT_MEMTABLE_MEMORY_LIMIT_BYTES: usize = 512 * 1024 * 1024;

/// MemTable 状态：active（写入侧）+ immutable（冻结待 flush 的 FIFO 队列）。
pub struct MemTableState {
    /// 活跃表（写入侧）。
    pub active: Arc<ColumnarMemTable>,
    /// 不可变表队列（FlushCoordinator 消费侧）：冻结后的旧表，按冻结先后 FIFO。
    pub immutable: VecDeque<Arc<ColumnarMemTable>>,
}

/// 计算 `state` 的估算内存占用（active + Σimmutable）。
fn total_mem(state: &MemTableState) -> usize {
    state.active.size_bytes()
        + state
            .immutable
            .iter()
            .map(|t| t.size_bytes())
            .sum::<usize>()
}

/// MemTable 管理器：RwLock 保护的「active + immutable 队列」状态机。
///
/// # 示例
///
/// ```ignore
/// let mgr = MemTableManager::new(&config);
/// mgr.insert(42, 100, &[(0, 3.14)], &[(0, 0xC0)], 0);
/// let result = mgr.force_freeze();
/// assert!(result.frozen);
/// ```
pub struct MemTableManager {
    /// `active + immutable` 的原子状态（读写共用一把 RwLock）。
    state: RwLock<MemTableState>,
    /// 配置句柄（freeze_threshold 热加载）。
    config: ConfigHandle,
    /// 背压低阈值（软告警）。
    soft_limit: usize,
    /// 背压高阈值（write stall）。
    hard_limit: usize,
    /// 内存上限（write stall）。
    mem_limit_bytes: usize,
    /// 软告警次数（metric）。
    soft_alarm_count: AtomicUsize,
    /// write stall 事件次数（metric）。
    write_stall_events: AtomicUsize,
}

impl MemTableManager {
    /// 创建新的 MemTableManager（使用默认背压阈值）。
    ///
    /// # 参数
    /// - `config`: 配置句柄，用于获取 freeze_threshold。
    pub fn new(config: &ConfigHandle) -> Self {
        Self::new_with_limits(
            config,
            DEFAULT_IMMUTABLE_SOFT_LIMIT,
            DEFAULT_IMMUTABLE_HARD_LIMIT,
            DEFAULT_MEMTABLE_MEMORY_LIMIT_BYTES,
        )
    }

    /// 创建带自定义背压阈值的 MemTableManager（供测试）。
    fn new_with_limits(
        config: &ConfigHandle,
        soft_limit: usize,
        hard_limit: usize,
        mem_limit_bytes: usize,
    ) -> Self {
        let freeze_threshold = config.get().engine.freeze_threshold;

        Self {
            state: RwLock::new(MemTableState {
                active: Arc::new(ColumnarMemTable::new(freeze_threshold)),
                immutable: VecDeque::new(),
            }),
            config: config.clone(),
            soft_limit,
            hard_limit,
            mem_limit_bytes,
            soft_alarm_count: AtomicUsize::new(0),
            write_stall_events: AtomicUsize::new(0),
        }
    }

    /// 向 active 表插入单点（读锁，共享；恒默认租户 0）。
    ///
    /// 批量热路径请用 [`insert_batch`](Self::insert_batch)；本方法供单点
    /// 场景（测试 / 低流量）使用。
    pub fn insert(
        &self,
        table_id: u32,
        ts: i64,
        values: &[(u16, TypedValue)],
        qualities: &[(u16, u8)],
        flags: u8,
    ) {
        let state = self.state.read();
        state.active.insert(table_id, ts, values, qualities, flags);
    }

    /// 批量插入（O1 修复）：外层读锁一次、内层按分片锁一次，锁获取 O(N)→O(1)。
    ///
    /// `seqs[i]` 为 `points[i]` 的 WAL 序列号（回填 Chunk 的 `max_wal_seq`，S7）；
    /// 长度可小于 `points.len()`（Raft 重放等无 seq 路径传空切片）。
    pub fn insert_batch(&self, points: &[DataPoint], seqs: &[u64]) {
        let state = write_attr::timed_read_lock(&self.state, write_attr::LockSlot::MemtableState);
        state.active.insert_batch(points, seqs);
    }

    /// 一次性取出 `(active, immutable 队列)` 的一致快照（读锁内克隆）。
    ///
    /// 供查询三源合并使用：消除「分别读 active 再读 immutable」的可见性间隙；
    /// 返回的 frozen 表列表按冻结先后排序（FIFO），查询需遍历所有 frozen 表。
    pub fn snapshot_state(&self) -> (Arc<ColumnarMemTable>, Vec<Arc<ColumnarMemTable>>) {
        let state = self.state.read();
        (
            state.active.clone(),
            state.immutable.iter().cloned().collect(),
        )
    }

    /// 获取 active 表快照（只读，向后兼容）。
    pub fn active_snapshot(&self) -> Arc<ColumnarMemTable> {
        self.state.read().active.clone()
    }

    /// 强制冻结：写锁内一步完成「旧表入队 → active 换新表」。
    ///
    /// # 返回值
    /// `FreezeResult` 包含冻结状态和数据点数。
    pub fn force_freeze(&self) -> crate::engine::FreezeResult {
        let mut state = self.state.write();
        // 热加载：每次 freeze 读取最新阈值
        let freeze_threshold = self.config.get().engine.freeze_threshold;

        let total_points = state.active.total_points();
        let frozen = total_points > 0;

        // P5：push_back（FIFO 入队，不再覆盖旧 frozen 表）；顺序反转——先入队再换表，
        // 因写锁独占，查询侧只能看到「切换后」的一致状态。
        if frozen {
            let old_table = state.active.clone();
            state.immutable.push_back(old_table);
        }
        state.active = Arc::new(ColumnarMemTable::new(freeze_threshold));

        // P6 低阈值软告警：队列深度 > soft_limit → log error + metric（只告警不阻塞）。
        if state.immutable.len() > self.soft_limit {
            self.soft_alarm_count.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                depth = state.immutable.len(),
                soft_limit = self.soft_limit,
                "memtable immutable queue exceeded soft limit (soft alarm)"
            );
        }

        crate::engine::FreezeResult {
            frozen,
            total_points,
        }
    }

    /// 从队头取出一个 immutable 表（供 FlushCoordinator 消费，FIFO）。
    pub fn pop_immutable(&self) -> Option<Arc<ColumnarMemTable>> {
        self.state.write().immutable.pop_front()
    }

    /// 把 flush 失败的表重塞回队头（P5：等下一轮重试，不丢弃）。
    pub fn push_front_immutable(&self, table: Arc<ColumnarMemTable>) {
        self.state.write().immutable.push_front(table);
    }

    /// 当前 immutable 队列深度。
    pub fn immutable_len(&self) -> usize {
        self.state.read().immutable.len()
    }

    /// 估算内存占用（active + Σimmutable）。
    pub fn total_memory_bytes(&self) -> usize {
        let state = self.state.read();
        total_mem(&state)
    }

    /// 是否触发 write stall（高阈值背压）。
    ///
    /// 触发条件：队列深度 > `hard_limit`，或 active+Σimmutable 内存 > `mem_limit_bytes`。
    pub fn write_stalled(&self) -> bool {
        let state = self.state.read();
        state.immutable.len() > self.hard_limit || total_mem(&state) > self.mem_limit_bytes
    }

    /// 记录一次 write stall 事件（metric）。
    pub fn record_write_stall(&self) {
        self.write_stall_events.fetch_add(1, Ordering::Relaxed);
    }

    /// 软告警累计次数（metric）。
    pub fn soft_alarm_count(&self) -> usize {
        self.soft_alarm_count.load(Ordering::Relaxed)
    }

    /// write stall 累计次数（metric）。
    pub fn write_stall_events(&self) -> usize {
        self.write_stall_events.load(Ordering::Relaxed)
    }

    /// 检查 active 表是否应触发冻结。
    pub fn should_freeze(&self) -> bool {
        self.state.read().active.should_freeze()
    }

    /// 计算「所有未 flush 数据的最小 WAL 数据 seq」（回收水位边界，C2）。
    ///
    /// 返回 `None` 表示当前没有任何 WAL 背书的未 flush 数据（SAC 落盘水位
    /// 无需被这些数据约束）。实现上：immutable 队列 FIFO，队头是最老未 flush
    /// 数据（min seq 最小）；队头无 seq（如纯 Raft 重放表）时顺延到下一个，
    /// 队列空则回退到 active 表。
    pub fn unflushed_min_seq(&self) -> Option<u64> {
        let state = self.state.read();
        for table in state.immutable.iter() {
            if let Some(m) = table.min_wal_seq() {
                return Some(m);
            }
        }
        state.active.min_wal_seq()
    }

    /// 判定 MemTable 是否已初始化且可服务。
    ///
    /// `active` 在构造时即初始化，恒有值，故当前恒返回 `true`；保留本方法
    /// 作为 readiness 探针的显式检查点，未来引入「未初始化/损坏」状态时可扩展。
    pub fn is_ready(&self) -> bool {
        let _state = self.state.read();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f64s(v: &[(u16, f64)]) -> Vec<(u16, TypedValue)> {
        v.iter().map(|(id, x)| (*id, TypedValue::F64(*x))).collect()
    }

    #[test]
    fn test_manager_create() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);
        let snapshot = mgr.active_snapshot();
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.total_points(), 0);
    }

    #[test]
    fn test_insert_into_active() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);
        mgr.insert(42, 100, &f64s(&[(0, 3.14)]), &[(0, 0xC0)], 0);

        let snapshot = mgr.active_snapshot();
        assert_eq!(snapshot.total_points(), 1);
        let chunk = snapshot.get_chunk(42).expect("chunk should exist");
        assert_eq!(chunk.point_count, 1);
        assert_eq!(chunk.get_value(0, 0), Some(3.14));
    }

    #[test]
    fn test_force_freeze_empty() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);
        let result = mgr.force_freeze();
        assert!(!result.frozen);
        assert_eq!(result.total_points, 0);
    }

    #[test]
    fn test_force_freeze_with_data() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);
        mgr.insert(1, 100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);

        let result = mgr.force_freeze();
        assert!(result.frozen);
        assert_eq!(result.total_points, 1);
        assert!(mgr.pop_immutable().is_some());
        // 冻结后 active 应为空
        assert!(mgr.active_snapshot().is_empty());
    }

    #[test]
    fn test_pop_immutable_none() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);
        assert!(mgr.pop_immutable().is_none());
    }

    #[test]
    fn test_should_freeze() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);
        // 默认阈值 460,000，空表不应冻结
        assert!(!mgr.should_freeze());
    }

    #[test]
    fn test_insert_typed_into_active() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);
        mgr.insert(9, 100, &[(0, TypedValue::I64(7))], &[(0, 0xC0)], 0);

        let snapshot = mgr.active_snapshot();
        let chunk = snapshot.get_chunk(9).expect("chunk should exist");
        assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::I64(7)));
    }

    /// 快照 pair 一致性：`snapshot_state` 读锁内一次取 (active, immutable 队列)，
    /// freeze 前后不存在「active 空 + immutable 空」的可见性间隙。
    #[test]
    fn test_snapshot_state_pair_atomic() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);
        mgr.insert(1, 100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);

        // 冻结前：active 有点、immutable 队列空。
        let (active, immutable) = mgr.snapshot_state();
        assert_eq!(active.total_points(), 1);
        assert!(immutable.is_empty());

        mgr.force_freeze();

        // 冻结后：active 空、immutable 队列有 1 表 —— pair 在任一时刻都覆盖全部数据。
        let (active, immutable) = mgr.snapshot_state();
        assert_eq!(active.total_points(), 0);
        assert_eq!(immutable.len(), 1);
        assert_eq!(immutable[0].total_points(), 1);
    }

    /// P5：多次 freeze 之间不 flush，队列累加不覆盖、不丢数据。
    #[test]
    fn test_force_freeze_accumulates_queue_no_overwrite() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);

        // 3 次 freeze，每次之间插入 1 点。
        for i in 0..3 {
            mgr.insert(1, (i + 1) * 100, &f64s(&[(0, i as f64)]), &[(0, 0xC0)], 0);
            mgr.force_freeze();
        }

        // 不再覆盖：队列深度 = 3。
        assert_eq!(mgr.immutable_len(), 3);

        // FIFO 排空，总点数 = 3，无丢失。
        let mut total = 0usize;
        while let Some(t) = mgr.pop_immutable() {
            total += t.total_points();
        }
        assert_eq!(total, 3);
        assert_eq!(mgr.immutable_len(), 0);
    }

    /// P6：低阈值软告警——队列深度超过 soft_limit 时告警计数递增（只告警不阻塞）。
    #[test]
    fn test_soft_alarm_triggered_above_limit() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);

        for i in 0..5 {
            mgr.insert(1, i * 100, &f64s(&[(0, i as f64)]), &[(0, 0xC0)], 0);
            mgr.force_freeze();
        }
        // 深度 5 > soft_limit 4 → 至少告警一次。
        assert!(mgr.soft_alarm_count() >= 1);
        assert!(mgr.immutable_len() > 4);
    }

    /// P6：高阈值 write stall（按深度）——超过 hard_limit 时 write_stalled() 为真，
    /// 排空后恢复。
    #[test]
    fn test_write_stalled_by_depth() {
        let config = ConfigHandle::new(Default::default());
        // hard_limit=2、mem_limit 极大（内存不触发）。
        let mgr = MemTableManager::new_with_limits(&config, 4, 2, usize::MAX);

        for i in 0..3 {
            mgr.insert(1, i * 100, &f64s(&[(0, i as f64)]), &[(0, 0xC0)], 0);
            mgr.force_freeze();
        }
        assert!(mgr.write_stalled(), "深度 3 > hard_limit 2 应触发 stall");

        while mgr.pop_immutable().is_some() {}
        assert!(!mgr.write_stalled(), "排空后应解除 stall");
    }

    /// P6：高阈值 write stall（按内存）——内存超过上限时 write_stalled() 为真。
    #[test]
    fn test_write_stalled_by_memory() {
        let config = ConfigHandle::new(Default::default());
        // mem_limit=1 字节：写入即超限。
        let mgr = MemTableManager::new_with_limits(&config, 4, 64, 1);

        mgr.insert(1, 100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        assert!(mgr.write_stalled(), "内存超限应触发 stall");
        assert!(mgr.total_memory_bytes() > 1);
    }

    /// QA 独立（方案 C 内存型 stall 排空机制）：`wait_writable` 依赖
    /// `force_freeze`（active→immutable，数据不丢）+ `pop_immutable`（flush 消费）
    /// 两步解除**内存型** stall。本测试逐段断言：内存型 stall → freeze 后数据仍被
    /// 持有（total_mem 不变）→ flush 排空后 stall 解除、点不丢。
    #[test]
    fn qa_memory_stall_force_freeze_then_drain_clears() {
        let config = ConfigHandle::new(Default::default());
        // mem_limit=1 字节、hard_limit 极大（深度不触发，纯内存型 stall）。
        let mgr = MemTableManager::new_with_limits(&config, 4, usize::MAX, 1);

        mgr.insert(1, 100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        assert!(mgr.write_stalled(), "内存超限应触发 stall（precondition）");
        let before = mgr.total_memory_bytes();
        assert!(before > 1);

        // force_freeze：active（有数据）→ immutable，total_mem 不变（只移动、不释放）。
        let fr = mgr.force_freeze();
        assert!(fr.frozen, "active 有数据时应冻结");
        assert_eq!(
            mgr.total_memory_bytes(),
            before,
            "freeze 只把 active 移入 immutable，不减少内存"
        );
        assert!(mgr.write_stalled(), "freeze 后仍 stalled（immutable 尚未 flush）");

        // 模拟 flush 消费：pop_immutable 取出冻结表 → 内存下降 → stall 解除。
        let popped = mgr.pop_immutable().expect("immutable 应可被 flush 消费");
        assert_eq!(popped.total_points(), 1, "冻结的表应被完整消费，不丢点");
        assert!(!mgr.write_stalled(), "flush 排空后内存型 stall 应解除");
        assert!(mgr.total_memory_bytes() < before, "排空后内存应下降");
    }

    /// P5：flush 失败重塞队头 —— `pop_immutable`（FIFO pop_front）+ `push_front_immutable`
    /// 后，重塞的表应回到队头（下一轮重试先取它），其余表顺序不变。
    #[test]
    fn test_push_front_immutable_requeue_order() {
        let config = ConfigHandle::new(Default::default());
        let mgr = MemTableManager::new(&config);

        // 3 次 freeze，产生 3 个 frozen 表（FIFO：t0, t1, t2）。
        for i in 0..3 {
            mgr.insert(1, (i + 1) * 100, &f64s(&[(0, i as f64)]), &[(0, 0xC0)], 0);
            mgr.force_freeze();
        }
        assert_eq!(mgr.immutable_len(), 3);

        // 取出队头 t0（第一个 frozen 表）。
        let t0 = mgr.pop_immutable().expect("t0");
        assert_eq!(t0.total_points(), 1);

        // 模拟 flush 失败：把 t0 重塞队头。
        mgr.push_front_immutable(t0);

        // 下一轮先取到 t0（重试），再取到 t1、t2。
        let again = mgr.pop_immutable().expect("requeued t0");
        assert_eq!(again.total_points(), 1);
        assert_eq!(mgr.immutable_len(), 2, "t1/t2 仍在队中");
    }

    /// 丢写竞态回归（竞态 2）：insert 与 force_freeze 并发时，任何点都不丢失。
    ///
    /// 每次 freeze 后立即 drain immutable（模拟 FlushCoordinator 排空）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_insert_during_force_freeze_no_loss() {
        let config = ConfigHandle::new(Default::default());
        let mgr = Arc::new(MemTableManager::new(&config));

        const TOTAL: i64 = 20_000;

        let writer = {
            let mgr = mgr.clone();
            tokio::spawn(async move {
                for ts in 0..TOTAL {
                    mgr.insert(1, ts, &[(0, TypedValue::F64(ts as f64))], &[(0, 0xC0)], 0);
                }
            })
        };

        // freezer：反复 force_freeze，每次 freeze 后 drain immutable（模拟 FlushCoordinator）。
        let freezer = {
            let mgr = mgr.clone();
            tokio::spawn(async move {
                let mut drained = 0usize;
                for _ in 0..50 {
                    mgr.force_freeze();
                    while let Some(t) = mgr.pop_immutable() {
                        drained += t.total_points();
                    }
                    tokio::task::yield_now().await;
                }
                drained
            })
        };

        writer.await.expect("writer should complete");
        let drained = freezer.await.expect("freezer should complete");

        // 不丢写：active + 已排空(immutable) 之和 == 写入总数。
        let total = mgr.active_snapshot().total_points() + drained;
        assert_eq!(
            total, TOTAL as usize,
            "insert 与 force_freeze 并发时不得丢写"
        );
    }
}

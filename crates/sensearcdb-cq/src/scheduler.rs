//! CQ 调度器。
//!
//! `CqScheduler` 以固定 1s tick 循环运行，负责：
//! - 遍历所有 CQ 定义，检查是否有窗口到期
//! - 优先处理错位重算队列中的 dirty 窗口
//! - 并发控制（通过 Semaphore 限制并行窗口数）
//! - 窗口对齐到边界（如 10s 窗口对齐到 10s 倍数）
//!
//! 窗口聚合结果通过固定辅助列布局写入目标 CQ 表：
//! `[value, _sum, _count, _min, _max, _first_ts, _last_ts]`（每源列）。

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;
use sensearcdb_common::{ConfigHandle, SenseArcError, TypedValue};
use tracing;

use crate::aggregation::CqAggregationExecutor;
use crate::column_resolver::CqColumnResolver;
use crate::definition::{CqDefinition, CqGranularity, CqStatus};
use crate::definition_store::CqDefinitionStore;
use crate::engine::{CqQueryExecutor, CqWriteTarget};
use crate::layout::{typed_max_sentinel, typed_min_sentinel, typed_zero, CqAuxColumn};
use crate::recalculation::{is_window_stale, RecalculationQueue};
use crate::window::{CqWindowKey, CqWindowState};

// ─── CqScheduler ───

/// CQ 调度器。
///
/// 持有对 CQ 引擎共享状态的引用，以 tokio task 形式运行 tick 循环。
pub struct CqScheduler {
    /// 所有 CQ 定义的引用。
    definitions: Arc<DashMap<u32, Vec<CqDefinition>>>,
    /// 窗口状态缓存的引用。
    window_states: Arc<DashMap<CqWindowKey, CqWindowState>>,
    /// 重算队列的引用。
    recalculation_queue: Arc<RwLock<RecalculationQueue>>,
    /// 查询执行器。
    query_executor: Arc<dyn CqQueryExecutor>,
    /// 写入目标。
    write_target: Arc<dyn CqWriteTarget>,
    /// 配置句柄。
    config: ConfigHandle,
    /// 并发控制。
    concurrency_limiter: Arc<tokio::sync::Semaphore>,
    /// 定义持久化存储（用于水位落盘，可选）。
    definition_store: Option<Arc<CqDefinitionStore>>,
    /// 辅助列 ID 解析器（schema 真实 ID / 公式回退）。
    column_resolver: Arc<dyn CqColumnResolver>,
    /// 调度器 tick 任务句柄（用于 stop）。
    _tick_handle: Option<tokio::task::JoinHandle<()>>,
}

impl CqScheduler {
    /// 创建新的调度器（不启动）。
    pub fn new(
        definitions: Arc<DashMap<u32, Vec<CqDefinition>>>,
        window_states: Arc<DashMap<CqWindowKey, CqWindowState>>,
        recalculation_queue: Arc<RwLock<RecalculationQueue>>,
        query_executor: Arc<dyn CqQueryExecutor>,
        write_target: Arc<dyn CqWriteTarget>,
        config: ConfigHandle,
        concurrency_limiter: Arc<tokio::sync::Semaphore>,
        definition_store: Option<Arc<CqDefinitionStore>>,
        column_resolver: Arc<dyn CqColumnResolver>,
    ) -> Self {
        Self {
            definitions,
            window_states,
            recalculation_queue,
            query_executor,
            write_target,
            config,
            concurrency_limiter,
            definition_store,
            column_resolver,
            _tick_handle: None,
        }
    }

    /// 启动调度器 tick 循环，返回调度器自身。
    ///
    /// tick 间隔由 `cq.tick_interval_ms` 配置（默认 1000ms）。
    /// 循环在 tokio task 中运行，无需外部管理。
    pub async fn start(mut self) -> Self {
        let tick_interval_ms = self.config.get().cq.tick_interval_ms;
        let tick_interval = Duration::from_millis(tick_interval_ms);

        let definitions = self.definitions.clone();
        let window_states = self.window_states.clone();
        let recalculation_queue = self.recalculation_queue.clone();
        let query_executor = self.query_executor.clone();
        let write_target = self.write_target.clone();
        let config = self.config.clone();
        let concurrency_limiter = self.concurrency_limiter.clone();
        let definition_store = self.definition_store.clone();
        let column_resolver = self.column_resolver.clone();

        let handle = tokio::spawn(async move {
            let mut tick_timer = tokio::time::interval(tick_interval);
            tick_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tracing::info!("CQ scheduler started, tick_interval={:?}", tick_interval);

            loop {
                tick_timer.tick().await;

                let cq_config = config.get().cq.clone();
                if !cq_config.enabled {
                    continue;
                }

                let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                let staleness_window_ns = cq_config.staleness_window_ms as i64 * 1_000_000;
                let _max_concurrency = cq_config.max_concurrency;
                let window_batch_size = cq_config.window_batch_size;

                // ─── Phase 1: 优先处理重算队列 ───
                let dirty_keys: Vec<CqWindowKey> = {
                    let mut queue = recalculation_queue.write();
                    queue.drain(window_batch_size)
                };

                for key in &dirty_keys {
                    // 语义检查：窗口已 stale（超出 staleness 窗口）或不再 dirty 时跳过。
                    // 「队列有 key 但 map 没状态」的不一致由不变量排除（Final / 删表同步
                    // 清两处），should_recalculate 内用 .expect 断言而非 None 兜底。
                    if !should_recalculate(&window_states, key, now_ns, staleness_window_ns) {
                        continue;
                    }

                    // 获取并发许可
                    let _permit = match concurrency_limiter.acquire().await {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // 获取 CQ 定义
                    let defs = definitions.get(&key.source_table_id);
                    if defs.is_none() {
                        continue;
                    }
                    let defs = defs.unwrap();
                    let found_def = defs.iter().find(|d| d.granularity == key.granularity);
                    if found_def.is_none() {
                        continue;
                    }
                    let def = found_def.unwrap().clone();

                    // 执行重算
                    match execute_window(
                        &def,
                        key.window_end,
                        &*query_executor,
                        &*write_target,
                        column_resolver.clone(),
                    )
                    .await
                    {
                        Ok(_) => {
                            if let Some(mut state) = window_states.get_mut(key) {
                                state.mark_calculated(now_ns);
                            }
                            // 更新 definition 中的 last_executed_window_end
                            let mut status = CqStatus::Running;
                            if let Some(mut defs) = definitions.get_mut(&def.source_table_id) {
                                for d in defs.iter_mut() {
                                    if d.granularity == def.granularity
                                        && d.target_table_id == def.target_table_id
                                    {
                                        d.record_success(key.window_end);
                                        status = d.status;
                                    }
                                }
                            }
                            persist_watermark(
                                &definition_store,
                                def.source_table_id,
                                def.granularity,
                                key.window_end,
                                0,
                                status,
                            );
                        }
                        Err(e) => {
                            tracing::error!("CQ recalculation failed for {:?}: {}", key, e);
                            let max_retries = cq_config.max_retries;
                            let mut failures = 0u32;
                            let mut status = CqStatus::Running;
                            if let Some(mut defs) = definitions.get_mut(&def.source_table_id) {
                                for d in defs.iter_mut() {
                                    if d.granularity == def.granularity
                                        && d.target_table_id == def.target_table_id
                                    {
                                        d.record_failure(max_retries);
                                        failures = d.consecutive_failures;
                                        status = d.status;
                                    }
                                }
                            }
                            persist_failure(
                                &definition_store,
                                def.source_table_id,
                                def.granularity,
                                failures,
                                status,
                            );
                        }
                    }
                }

                // ─── Phase 2: 处理新窗口 ───
                let mut window_count = 0usize;
                let all_defs: Vec<CqDefinition> = definitions
                    .iter()
                    .flat_map(|entry| entry.value().clone())
                    .collect();

                for def in &all_defs {
                    if window_count >= window_batch_size {
                        break;
                    }
                    if !def.is_schedulable() {
                        continue;
                    }

                    let interval_ns = def.interval_ns;
                    let next_window_end = if def.last_executed_window_end == 0 {
                        // 首次执行：对齐当前时间
                        align_to_next_window(now_ns, interval_ns)
                    } else {
                        def.last_executed_window_end + interval_ns
                    };

                    // 窗口未到期
                    if now_ns < next_window_end {
                        continue;
                    }

                    // 检查窗口状态
                    let key =
                        CqWindowKey::new(def.source_table_id, def.granularity, next_window_end);
                    {
                        let state = window_states
                            .entry(key.clone())
                            .or_insert_with(CqWindowState::new);
                        if !state.should_execute() {
                            continue;
                        }
                    }

                    // 获取并发许可
                    let _permit = match concurrency_limiter.acquire().await {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    window_count += 1;

                    let def_clone = def.clone();
                    let query_exec = query_executor.clone();
                    let write_tgt = write_target.clone();
                    let ws = window_states.clone();

                    // 执行窗口聚合
                    match execute_window(
                        &def_clone,
                        next_window_end,
                        &*query_exec,
                        &*write_tgt,
                        column_resolver.clone(),
                    )
                    .await
                    {
                        Ok(_) => {
                            if let Some(mut state) = ws.get_mut(&key) {
                                state.mark_calculated(now_ns);
                            }
                            let mut status = CqStatus::Running;
                            if let Some(mut defs) = definitions.get_mut(&def_clone.source_table_id)
                            {
                                for d in defs.iter_mut() {
                                    if d.granularity == def_clone.granularity
                                        && d.target_table_id == def_clone.target_table_id
                                    {
                                        d.record_success(next_window_end);
                                        status = d.status;
                                    }
                                }
                            }
                            persist_watermark(
                                &definition_store,
                                def_clone.source_table_id,
                                def_clone.granularity,
                                next_window_end,
                                0,
                                status,
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                "CQ execution failed for table={}, granularity={}, window_end={}: {}",
                                def.source_table_id,
                                def.granularity,
                                next_window_end,
                                e
                            );
                            let max_retries = cq_config.max_retries;
                            let mut failures = 0u32;
                            let mut status = CqStatus::Running;
                            if let Some(mut defs) = definitions.get_mut(&def_clone.source_table_id)
                            {
                                for d in defs.iter_mut() {
                                    if d.granularity == def_clone.granularity
                                        && d.target_table_id == def_clone.target_table_id
                                    {
                                        d.record_failure(max_retries);
                                        failures = d.consecutive_failures;
                                        status = d.status;
                                    }
                                }
                            }
                            persist_failure(
                                &definition_store,
                                def_clone.source_table_id,
                                def_clone.granularity,
                                failures,
                                status,
                            );
                        }
                    }

                    // ─── Phase 3: 清理 stale 窗口（方案 B）───
                    // 窗口进入 Final（终态，不可重算）后同步从 window_states 与
                    // recalculation_queue 移除，使 window_states 有界于 staleness 窗口内，
                    // 不再随时间无限累积，并维持「队列 key ⊆ window_states 键集」不变量。
                    finalize_stale_windows(&ws, &recalculation_queue, now_ns, staleness_window_ns);
                }
            }
        });

        self._tick_handle = Some(handle);

        self
    }
}

// ─── Helper: execute a single window ───

/// 执行单个窗口的 CQ 聚合。
async fn execute_window(
    def: &CqDefinition,
    window_end: i64,
    query_executor: &dyn CqQueryExecutor,
    write_target: &dyn CqWriteTarget,
    column_resolver: Arc<dyn CqColumnResolver>,
) -> Result<(), SenseArcError> {
    let interval_ns = def.interval_ns;
    let window_start = window_end - interval_ns;

    // 从 schema 解析辅助列真实 column_id（写路径与读路径共用同一份 ID）。
    let aux_ids =
        column_resolver.resolve_aux_column_ids(def.target_table_id, &def.source_columns)?;

    let executor = CqAggregationExecutor::with_resolver(column_resolver.clone());

    // 执行聚合
    let result = executor.execute(def, window_start, window_end, query_executor)?;

    // 将聚合结果写入目标 CQ 表（value + 6 辅助列，一次 write_batch）。
    // 时间戳使用 window_start（数据点归属到窗口起始）。
    let mut column_values: Vec<(u16, TypedValue)> =
        Vec::with_capacity(def.source_columns.len() * 7);

    for (i, &col_id) in def.source_columns.iter().enumerate() {
        let source_type = def.source_type(i);
        // value 列（按聚合结果类型写入，column_id 与源列一致）
        column_values.push((
            col_id,
            result
                .columns
                .get(i)
                .cloned()
                .unwrap_or_else(|| typed_zero(source_type)),
        ));
        let aux_base = i * 6;
        // 辅助列：_sum/_min/_max 按源类型，_count 写 U64，时间戳列写 I64。
        column_values.push((
            aux_ids[aux_base + CqAuxColumn::Sum as usize],
            result
                ._sum
                .get(i)
                .cloned()
                .unwrap_or_else(|| typed_zero(source_type)),
        ));
        column_values.push((
            aux_ids[aux_base + CqAuxColumn::Count as usize],
            TypedValue::U64(result._count.get(i).copied().unwrap_or(0)),
        ));
        column_values.push((
            aux_ids[aux_base + CqAuxColumn::Min as usize],
            result
                ._min
                .get(i)
                .cloned()
                .unwrap_or_else(|| typed_min_sentinel(source_type)),
        ));
        column_values.push((
            aux_ids[aux_base + CqAuxColumn::Max as usize],
            result
                ._max
                .get(i)
                .cloned()
                .unwrap_or_else(|| typed_max_sentinel(source_type)),
        ));
        // 时间戳列（i64 落库，不塞进 f64，D7）
        column_values.push((
            aux_ids[aux_base + CqAuxColumn::FirstTs as usize],
            TypedValue::I64(result._first_ts.get(i).copied().unwrap_or(0)),
        ));
        column_values.push((
            aux_ids[aux_base + CqAuxColumn::LastTs as usize],
            TypedValue::I64(result._last_ts.get(i).copied().unwrap_or(0)),
        ));
    }

    write_target.write_batch(def.target_table_id, window_start, &column_values)?;

    tracing::debug!(
        "CQ window executed: table={}, granularity={}, window_end={}, columns={}",
        def.source_table_id,
        def.granularity,
        window_end,
        result.columns.len()
    );

    Ok(())
}

/// 持久化成功水位（last_executed_window_end + 重置失败计数）。
fn persist_watermark(
    store: &Option<Arc<CqDefinitionStore>>,
    source_table_id: u32,
    granularity: CqGranularity,
    window_end: i64,
    failures: u32,
    status: CqStatus,
) {
    if let Some(store) = store {
        if let Err(e) =
            store.update_watermark(source_table_id, granularity, window_end, failures, status)
        {
            tracing::error!("failed to persist CQ watermark: {e}");
        }
    }
}

/// 持久化失败水位（递增失败计数，不触碰 `last_executed_window_end`）。
fn persist_failure(
    store: &Option<Arc<CqDefinitionStore>>,
    source_table_id: u32,
    granularity: CqGranularity,
    failures: u32,
    status: CqStatus,
) {
    if let Some(store) = store {
        if let Err(e) = store.update_status(source_table_id, granularity, failures, status) {
            tracing::error!("failed to persist CQ failure watermark: {e}");
        }
    }
}

/// 将时间戳对齐到下一个窗口边界。
///
/// # Arguments
/// * `ts` - 当前时间戳（epoch 纳秒）。
/// * `interval_ns` - 窗口长度（纳秒）。
///
/// # Returns
/// 下一个窗口的结束时间戳。
#[inline]
fn align_to_next_window(ts: i64, interval_ns: i64) -> i64 {
    if ts <= 0 {
        return interval_ns;
    }
    let rem = ts % interval_ns;
    if rem == 0 {
        ts
    } else {
        ts - rem + interval_ns
    }
}

/// 判断重算队列中的窗口 key 是否应执行重算（语义检查，非兜底）。
///
/// 返回 `false`（跳过执行）当且仅当：
/// 1. 窗口已 stale（超出 staleness 窗口）——同步 `window_states.remove` 并跳过；
/// 2. 窗口非 dirty——无需重算；
/// 3. 窗口已被并发移除（如删表）——`get` 为 `None`，合法跳过（见下）。
///
/// # 不变量
/// 「重算队列的 key ⊆ window_states 的键集」约束的是**当前在队列里的 key**。
/// 本函数处理的是 Phase 1 顶部已 `drain` 离队的「在途 key」——它们本就不在该不变量
/// 范围内，可能被并发删表（`on_table_deleted`）移除。因此 `get` 返回 `None` 是
/// **合法的并发语义**（低频 DDL），跳过即可，而非「只 remove 一处」的孤儿 bug
/// （后者已由「同步 remove」结构性消除，不应再靠 None 兜底掩盖）。
///
/// 函数返回前内部的 `DashMap` shard 读锁已释放（`state` 引用 drop），
/// 不会跨调用方的 `await` 持有锁。
fn should_recalculate(
    window_states: &DashMap<CqWindowKey, CqWindowState>,
    key: &CqWindowKey,
    now_ns: i64,
    staleness_window_ns: i64,
) -> bool {
    let Some(state) = window_states.get(key) else {
        // 合法并发：窗口在 drain 后被并发移除（通常为删表）。低频 DDL 语义，跳过。
        tracing::debug!(
            "CQ recalc skipped: window removed concurrently (table deleted?) key={:?}",
            key
        );
        return false;
    };

    if is_window_stale(&state, key.window_end, now_ns, staleness_window_ns) {
        drop(state);
        window_states.remove(key);
        return false;
    }

    state.dirty
}

/// 将超出 staleness 窗口的窗口从 `window_states` 与 `recalculation_queue` 同步移除（方案 B）。
///
/// 窗口进入 Final（终态，`CqWindowState::mark_final` 后不可重算）后不再参与任何路径。
/// 必须**同时**清 `window_states` 与 `recalculation_queue`，维持不变量
/// 「重算队列的 key ⊆ window_states 的键集」（约束的是**当前在队列里的 key**），
/// 避免「队列有 key、map 没状态」的不一致。已在 Phase 1 drain 离队的「在途 key」
/// 不在该不变量范围内，可能被并发删表移除（见 [`should_recalculate`]）。
///
/// # 锁顺序
/// 先 `window_states`（逐个 remove，shard 锁瞬时获取释放），再 `recalculation_queue`
/// （单次 write 锁批量 remove）。两把锁**从不嵌套持有**，与写路径 `on_data_written`
/// （先 `window_states.get_mut` 持 shard 锁，再 `recalculation_queue.write()`）保持
/// 同一方向，避免 ABBA 死锁。
///
/// # Returns
/// 实际移除的窗口数量。
fn finalize_stale_windows(
    window_states: &DashMap<CqWindowKey, CqWindowState>,
    recalculation_queue: &RwLock<RecalculationQueue>,
    now_ns: i64,
    staleness_window_ns: i64,
) -> usize {
    let stale_keys: Vec<CqWindowKey> = window_states
        .iter()
        .filter(|entry| {
            is_window_stale(
                entry.value(),
                entry.key().window_end,
                now_ns,
                staleness_window_ns,
            )
        })
        .map(|entry| entry.key().clone())
        .collect();

    let removed = stale_keys.len();
    // 1) 先清 window_states（不持 queue 锁）。
    for key in &stale_keys {
        window_states.remove(key);
    }
    // 2) 再批量清 queue（不持 window_states 锁），维持不变量。
    if removed > 0 {
        let mut queue = recalculation_queue.write();
        for key in &stale_keys {
            queue.remove(key);
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_to_next_window() {
        let interval = 10_000_000_000i64; // 10s
        assert_eq!(align_to_next_window(0, interval), interval);
        assert_eq!(
            align_to_next_window(5_000_000_000, interval),
            10_000_000_000
        );
        assert_eq!(
            align_to_next_window(10_000_000_000, interval),
            10_000_000_000
        );
        assert_eq!(
            align_to_next_window(15_000_000_000, interval),
            20_000_000_000
        );
    }

    /// 方案 B 核心：Final（超出 staleness）窗口从 window_states 与重算队列同步移除，
    /// 未到期窗口保留，两处长度都有界。
    #[test]
    fn test_finalize_stale_windows_removes_stale_keeps_fresh() {
        let ws: DashMap<CqWindowKey, CqWindowState> = DashMap::new();
        let queue = RwLock::new(RecalculationQueue::new());
        let staleness_ns = 86_400_000_000_000i64; // 24h
        let now_ns = 1_000_000_000_000_000i64;

        // 3 个已 stale 的窗口（window_end 远早于 now - staleness），其中 2 个同时入队。
        for i in 0..3 {
            let key = CqWindowKey::new(
                1,
                CqGranularity::Cq10s,
                now_ns - staleness_ns - (i + 1) as i64 * 10_000_000_000,
            );
            let mut state = CqWindowState::new();
            state.mark_calculated(now_ns - staleness_ns);
            ws.insert(key.clone(), state);
            if i < 2 {
                queue.write().push(key);
            }
        }
        // 2 个未到期窗口（window_end 在当前 staleness 内）。
        for i in 0..2 {
            let key = CqWindowKey::new(
                1,
                CqGranularity::Cq10s,
                now_ns - (i + 1) as i64 * 10_000_000_000,
            );
            let mut state = CqWindowState::new();
            state.mark_calculated(now_ns);
            ws.insert(key, state);
        }

        assert_eq!(ws.len(), 5);
        assert_eq!(queue.read().len(), 2);
        let removed = finalize_stale_windows(&ws, &queue, now_ns, staleness_ns);
        assert_eq!(removed, 3, "应移除 3 个 stale 窗口");
        assert_eq!(ws.len(), 2, "移除后只保留未到期窗口，长度有界");
        assert_eq!(queue.read().len(), 0, "Final 窗口必须同步从重算队列移除");
    }

    /// 不变量「重算队列 key ⊆ window_states 键集」：Final 移除必须同步清两处。
    /// 构造一个已入队的 stale 窗口，finalize 后断言它同时从 map 与 queue 消失。
    #[test]
    fn test_finalize_removes_window_from_both_map_and_queue() {
        let ws: DashMap<CqWindowKey, CqWindowState> = DashMap::new();
        let queue = RwLock::new(RecalculationQueue::new());
        let staleness_ns = 86_400_000_000_000i64;
        let now_ns = 1_000_000_000_000_000i64;

        let key = CqWindowKey::new(
            7,
            CqGranularity::Cq1m,
            now_ns - staleness_ns - 60_000_000_000,
        );
        let mut state = CqWindowState::new();
        state.mark_calculated(now_ns - staleness_ns);
        state.mark_dirty();
        ws.insert(key.clone(), state);
        queue.write().push(key.clone());

        let removed = finalize_stale_windows(&ws, &queue, now_ns, staleness_ns);
        assert_eq!(removed, 1);
        assert!(ws.get(&key).is_none(), "Final 后 window_states 必须移除");
        assert_eq!(
            queue.read().len(),
            0,
            "Final 后重算队列必须同步清空（不变量）"
        );
    }

    /// 反例防护：未超出 staleness 的 dirty 窗口必须保留并可被重算，
    /// 防止「过早 remove」误删仍需重算的窗口。
    #[test]
    fn test_non_stale_dirty_window_is_not_removed() {
        let ws: DashMap<CqWindowKey, CqWindowState> = DashMap::new();
        let queue = RwLock::new(RecalculationQueue::new());
        let staleness_ns = 86_400_000_000_000i64;
        let now_ns = 1_000_000_000_000_000i64;

        // window_end 在 staleness 内，且 dirty（有迟到数据待重算）。
        let key = CqWindowKey::new(7, CqGranularity::Cq1m, now_ns - 1_000_000_000);
        let mut state = CqWindowState::new();
        state.mark_calculated(now_ns);
        state.mark_dirty();
        ws.insert(key.clone(), state);
        queue.write().push(key.clone());

        let removed = finalize_stale_windows(&ws, &queue, now_ns, staleness_ns);
        assert_eq!(removed, 0, "未 stale 的 dirty 窗口不得被移除");
        assert!(ws.get(&key).is_some(), "仍需重算的窗口必须保留");
        assert_eq!(queue.read().len(), 1, "仍需重算的窗口必须留在队列中");
    }

    /// 不变量断言（阳性对照）：队列里的 key 必须仍在 window_states 中。
    /// 未 stale 的 dirty 窗口应被重算（should_recalculate 返回 true）。
    #[test]
    fn test_dirty_fresh_window_is_recalculated() {
        let ws: DashMap<CqWindowKey, CqWindowState> = DashMap::new();
        let staleness_ns = 86_400_000_000_000i64;
        let now_ns = 1_000_000_000_000_000i64;

        let key = CqWindowKey::new(42, CqGranularity::Cq1m, now_ns - 1_000_000_000);
        let mut state = CqWindowState::new();
        state.mark_calculated(now_ns);
        state.mark_dirty();
        ws.insert(key.clone(), state);

        assert!(
            should_recalculate(&ws, &key, now_ns, staleness_ns),
            "未 stale 的 dirty 窗口应被重算"
        );
    }

    /// 语义检查（非兜底）：窗口仍留在 map 但已 stale（尚未经 Phase 3 remove），
    /// should_recalculate 应同步 remove window_states 并返回 false。
    /// 注意：该 key 在 Phase 1 顶部已被 drain 出队列，故此处无需（也不应）碰队列。
    #[test]
    fn test_stale_window_in_queue_is_removed_and_skipped() {
        let ws: DashMap<CqWindowKey, CqWindowState> = DashMap::new();
        let staleness_ns = 86_400_000_000_000i64;
        let now_ns = 1_000_000_000_000_000i64;

        let key = CqWindowKey::new(
            42,
            CqGranularity::Cq1m,
            now_ns - staleness_ns - 60_000_000_000,
        );
        let mut state = CqWindowState::new();
        state.mark_calculated(now_ns - staleness_ns);
        state.mark_dirty();
        ws.insert(key.clone(), state);

        // 直接调守卫（key 仍在 map 中），应触发 stale 分支：remove + skip。
        assert!(!should_recalculate(&ws, &key, now_ns, staleness_ns));
        assert!(ws.get(&key).is_none(), "stale 分支应同步 remove，保持有界");
    }

    /// 并发语义（非兜底）：`get` 返回 None（窗口在 drain 后被并发移除，如删表）
    /// 时应合法跳过（返回 false），而不是 panic。不变量只约束「在队 key」，
    /// 「在途 key」本就不在约束范围内。
    #[test]
    fn test_should_recalculate_skips_missing_window_state() {
        let ws: DashMap<CqWindowKey, CqWindowState> = DashMap::new();
        let staleness_ns = 86_400_000_000_000i64;
        let now_ns = 1_000_000_000_000_000i64;

        let key = CqWindowKey::new(42, CqGranularity::Cq1m, now_ns - 1_000_000_000);
        // key 不在 window_states 中（模拟并发删表后）：合法跳过，不 panic。
        assert!(
            !should_recalculate(&ws, &key, now_ns, staleness_ns),
            "get None 应合法跳过而非 panic（并发删表语义）"
        );
    }
}

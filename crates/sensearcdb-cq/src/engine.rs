//! CQ 引擎主结构体。
//!
//! `CqEngine` 是 CQ 模块的入口，负责：
//! - 管理 CQ 定义的生命周期（创建/删除/变更）
//! - 接收写入路径回调（标记 dirty 窗口）
//! - 持有调度器和重算队列
//! - 提供 AutoRouter 所需的 CQ 窗口信息
//!
//! Schema 变更通过顶层 `CqSchemaSynchronizer`（位于 `src/startup.rs`）接入：
//! 该同步器订阅 `sensearcdb-schema` 的 `SchemaEventBus`，负责分配/创建/删除
//! CQ 物理表，再调用本引擎的 `on_table_created` / `on_table_deleted` /
//! `on_table_altered` 作为唯一同步 API。CQ crate 不直接依赖 schema crate。

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;
use sensearcdb_common::{
    ConfigHandle, CqRouteHint, CqRouteProvider, DataType, SenseArcError, TypedValue,
};
use sensearcdb_schema::Catalog;
use tracing;

use crate::column_resolver::{CqColumnResolver, SchemaColumnResolver};
use crate::definition::{CqDefinition, CqGranularity};
use crate::definition_store::CqDefinitionStore;
use crate::recalculation::RecalculationQueue;
use crate::scheduler::CqScheduler;
use crate::window::{CqWindowKey, CqWindowState, CqWindowStatus};

// ─── WriteEvent (minimal) ───

/// 写入事件（CQ 所需的最小定义）。
///
/// 由引擎的 write_batch 路径发出，CQ 据此判断哪些窗口需要重算。
#[derive(Debug, Clone)]
pub struct WriteEvent {
    /// 写入的目标表 ID。
    pub table_id: u32,
    /// 写入数据点的时间戳列表（epoch 纳秒）。
    pub timestamps: Vec<i64>,
    /// 写入的 batch_id（用于去重）。
    pub batch_id: u64,
}

// ─── QueryExecutor trait ───

/// CQ 内部查询接口（由引擎提供具体实现）。
///
/// CQ 通过此 trait 从源表（raw 表或下层 CQ 表）读取原始数据以执行聚合。
/// 查询绕过 AutoRouter（bypass_autorouter），直接读取 SAC/MemTable。
pub trait CqQueryExecutor: Send + Sync {
    /// 执行 CQ 内部查询（仅返回每列值，丢弃逐行时间戳）。
    ///
    /// 默认实现基于 [`query_raw_with_ts`](Self::query_raw_with_ts)。
    fn query_raw(
        &self,
        table_id: u32,
        column_ids: &[u16],
        time_start: i64,
        time_end: i64,
    ) -> Result<Vec<Vec<Option<TypedValue>>>, SenseArcError> {
        self.query_raw_with_ts(table_id, column_ids, time_start, time_end)
            .map(|(columns, _timestamps)| columns)
    }

    /// 执行 CQ 内部查询并返回逐行时间戳。
    ///
    /// # Arguments
    /// * `table_id` - 源表 ID。
    /// * `column_ids` - 需要读取的列 ID 列表。
    /// * `time_start` - 时间范围起始（epoch 纳秒）。
    /// * `time_end` - 时间范围结束（epoch 纳秒）。
    ///
    /// # Returns
    /// `(Vec<Vec<Option<TypedValue>>>, Vec<i64>)` — 每列的类型化值数组
    /// （列顺序与 `column_ids` 一致；缺失列/缺失值 → `None`）与逐行时间戳。
    /// 各列长度与时间戳长度一致（表示行数相同）。
    fn query_raw_with_ts(
        &self,
        table_id: u32,
        column_ids: &[u16],
        time_start: i64,
        time_end: i64,
    ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError>;
}

/// CQ 写入接口（由引擎提供具体实现）。
///
/// CQ 通过此 trait 将聚合结果写入目标 CQ 表。
pub trait CqWriteTarget: Send + Sync {
    /// 写入 CQ 聚合结果。
    ///
    /// # Arguments
    /// * `table_id` - 目标 CQ 表 ID。
    /// * `timestamp` - 数据点时间戳（epoch 纳秒，窗口起始时间）。
    /// * `column_values` - 列 ID → 类型化值（`TypedValue` 直通，不再经 f64 折叠）。
    fn write_batch(
        &self,
        table_id: u32,
        timestamp: i64,
        column_values: &[(u16, TypedValue)],
    ) -> Result<(), SenseArcError>;
}

// ─── CqEngine ───

/// CQ 引擎主结构体。
///
/// 持有所有 CQ 定义和窗口状态，协调调度器、重算队列和聚合执行器。
pub struct CqEngine {
    /// 所有 CQ 定义 (source_table_id → Vec<CqDefinition>)。
    definitions: Arc<DashMap<u32, Vec<CqDefinition>>>,
    /// 窗口状态缓存 (CqWindowKey → CqWindowState)。
    window_states: Arc<DashMap<CqWindowKey, CqWindowState>>,
    /// 调度器。
    scheduler: Option<CqScheduler>,
    /// 错位重算队列。
    recalculation_queue: Arc<RwLock<RecalculationQueue>>,
    /// 查询执行器（CQ 内部查询源表数据）。
    query_executor: Arc<dyn CqQueryExecutor>,
    /// 写入目标（CQ 结果写入目标表）。
    write_target: Arc<dyn CqWriteTarget>,
    /// 定义持久化存储（config.db `cq_definitions`）。
    definition_store: Option<Arc<CqDefinitionStore>>,
    /// 辅助列 ID 解析器（schema 真实 ID / 公式回退）。
    column_resolver: Arc<dyn CqColumnResolver>,
    /// 配置句柄。
    config: ConfigHandle,
    /// 并发控制信号量。
    concurrency_limiter: Arc<tokio::sync::Semaphore>,
    /// 已处理的 (table_id, batch_id) 集合（防止同表同批重复触发；跨表同批不冲突）。
    processed_batches: Arc<DashSet<(u32, u64)>>,
    /// 是否存在任何 CQ 定义（写路径快路径标志，0 定义时跳过 CQ 标脏）。
    has_cq_definitions: AtomicBool,
    /// CQ 引擎是否正在运行。
    running: Arc<RwLock<bool>>,
}

impl CqEngine {
    /// 创建新的 CQ 引擎。
    ///
    /// 辅助列 ID 解析器使用注入的共享 [`Catalog`]（[`SchemaColumnResolver`]），
    /// 不再自建 Catalog 连接，也不再在 open 失败时静默回退公式解析器。
    ///
    /// # Arguments
    /// * `query_executor` - 查询执行器（用于从源表读取数据）。
    /// * `write_target` - 写入目标（用于将聚合结果写入 CQ 表）。
    /// * `config` - 配置句柄。
    /// * `definition_store` - 定义持久化存储（可选；`None` 时定义仅驻内存）。
    /// * `catalog` - 共享 schema Catalog（必须已打开）。
    pub fn new(
        query_executor: Arc<dyn CqQueryExecutor>,
        write_target: Arc<dyn CqWriteTarget>,
        config: ConfigHandle,
        definition_store: Option<Arc<CqDefinitionStore>>,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self::new_with_column_resolver(
            query_executor,
            write_target,
            config,
            definition_store,
            Arc::new(SchemaColumnResolver::new(catalog)),
        )
    }

    /// 创建带指定辅助列 ID 解析器的 CQ 引擎。
    ///
    /// 测试可注入 mock resolver（如 [`FormulaColumnResolver`](crate::column_resolver::FormulaColumnResolver)）；
    /// 生产由 [`new`](Self::new) 注入基于共享 Catalog 的 [`SchemaColumnResolver`]。
    pub fn new_with_column_resolver(
        query_executor: Arc<dyn CqQueryExecutor>,
        write_target: Arc<dyn CqWriteTarget>,
        config: ConfigHandle,
        definition_store: Option<Arc<CqDefinitionStore>>,
        column_resolver: Arc<dyn CqColumnResolver>,
    ) -> Self {
        let cq_config = config.get().cq.clone();
        let concurrency_limiter = Arc::new(tokio::sync::Semaphore::new(cq_config.max_concurrency));

        Self {
            definitions: Arc::new(DashMap::new()),
            window_states: Arc::new(DashMap::new()),
            scheduler: None,
            recalculation_queue: Arc::new(RwLock::new(RecalculationQueue::new())),
            query_executor,
            write_target,
            definition_store,
            column_resolver,
            config,
            concurrency_limiter,
            processed_batches: Arc::new(DashSet::new()),
            has_cq_definitions: AtomicBool::new(false),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// 是否存在任何 CQ 定义（写路径快路径标志）。
    pub fn has_cq_definitions(&self) -> bool {
        self.has_cq_definitions.load(Ordering::Relaxed)
    }

    /// 从 config.db 恢复所有 CQ 定义到内存 map。
    ///
    /// 在启动时调用（`start()` 之前），使重启后 CQ 定义不丢失。
    /// 返回恢复的定义条数。
    pub fn restore_definitions(&mut self) -> Result<usize, SenseArcError> {
        let Some(store) = self.definition_store.as_ref() else {
            return Ok(0);
        };
        let defs = store.load_all()?;
        let mut count = 0usize;
        for def in defs {
            self.definitions
                .entry(def.source_table_id)
                .or_default()
                .push(def);
            count += 1;
        }
        if count > 0 {
            self.has_cq_definitions.store(true, Ordering::Relaxed);
            tracing::info!("CQ definitions restored from config.db: {count} definition(s)");
        }
        Ok(count)
    }

    /// 启动 CQ 引擎。
    ///
    /// 创建并启动调度器 tokio task。
    pub async fn start(&mut self) -> Result<(), SenseArcError> {
        let cq_config = self.config.get().cq.clone();
        if !cq_config.enabled {
            tracing::info!("CQ engine disabled, skipping start");
            return Ok(());
        }

        *self.running.write() = true;

        // 启动调度器
        let scheduler = CqScheduler::new(
            self.definitions.clone(),
            self.window_states.clone(),
            self.recalculation_queue.clone(),
            self.query_executor.clone(),
            self.write_target.clone(),
            self.config.clone(),
            self.concurrency_limiter.clone(),
            self.definition_store.clone(),
            self.column_resolver.clone(),
        );

        let scheduler = scheduler.start().await;

        // 将调度器句柄保存下来
        self.scheduler = Some(scheduler);

        tracing::info!("CQ engine started");
        Ok(())
    }

    /// 停止 CQ 引擎。
    pub fn stop(&mut self) {
        *self.running.write() = false;
        self.scheduler = None;
        tracing::info!("CQ engine stopped");
    }

    /// 写入路径回调：收到新写入数据时调用。
    ///
    /// 检查写入时间戳是否落在已计算的 CQ 窗口内，
    /// 如果是则将对应窗口标记为 dirty 并加入重算队列。
    ///
    /// # Arguments
    /// * `event` - 写入事件（包含表 ID、时间戳列表、batch_id）。
    pub fn on_data_written(&self, event: WriteEvent) {
        let cq_config = self.config.get().cq.clone();
        if !cq_config.enabled {
            return;
        }

        // 去重：同表同 batch 不重复处理（跨表同 batch 不冲突）
        if !self.processed_batches.insert((event.table_id, event.batch_id)) {
            return;
        }

        // 清理旧 batch_id（保留最近 1024 个）
        if self.processed_batches.len() > 1024 {
            // 简单的清理策略：全部清空
            self.processed_batches.clear();
        }

        // 查找该表的所有 CQ 定义
        let Some(defs) = self.definitions.get(&event.table_id) else {
            return;
        };

        for def in defs.iter() {
            if !def.is_schedulable() {
                continue;
            }

            let interval_ns = def.interval_ns;

            // 批内窗口去重：同 (粒度, window_end) 只 get_mut + mark_dirty 一次，
            // 把「每 ts 一次 get_mut」降为「每窗口一次」。锁顺序不变：
            // 先 window_states.get_mut（shard 锁），再 recalculation_queue.write()。
            let mut seen: HashSet<(CqGranularity, i64)> = HashSet::new();

            for &ts in &event.timestamps {
                // 对齐到窗口边界
                let window_end = align_timestamp_up(ts, interval_ns);

                if !seen.insert((def.granularity, window_end)) {
                    continue;
                }

                // 检查窗口状态
                let key = CqWindowKey::new(def.source_table_id, def.granularity, window_end);

                if let Some(mut state) = self.window_states.get_mut(&key) {
                    if state.status == CqWindowStatus::Calculated && !state.dirty {
                        state.mark_dirty();
                        let mut queue = self.recalculation_queue.write();
                        queue.push(key.clone());
                        tracing::debug!(
                            "marked window dirty: table={}, granularity={}, window_end={}",
                            def.source_table_id,
                            def.granularity,
                            window_end
                        );
                    }
                }
            }
        }
    }

    /// 表创建回调：为新表创建默认 CQ 定义。
    ///
    /// 对每个粒度自动创建 CQ 定义，为表的全部数值列配置 AVG 聚合。
    ///
    /// # Arguments
    /// * `source_table_id` - 新创建的源 raw 表 ID。
    /// * `target_table_ids` - 各粒度对应的目标 CQ 表 ID（按 Cq10s/Cq1m/Cq1h/Cq1d 顺序）。
    /// * `column_specs` - 表中需要聚合的数值列（`(column_id, source_type)`）。
    pub fn on_table_created(
        &mut self,
        source_table_id: u32,
        target_table_ids: &[u32; 4],
        column_specs: &[(u16, DataType)],
    ) {
        let cq_config = self.config.get().cq.clone();
        if !cq_config.enabled {
            return;
        }

        // 幂等（P2 兜底，非治本机制）：`reconcile_cq_tables`（启动补齐 / Lagged 恢复）
        // 现在先做「状态 diff」（P0-2），只对缺失源表调用本方法，正常不会重复进入。
        // 此处 `contains_key` 早退作为最后防呆，避免极端并发下重复注册/重复落盘。
        if self.definitions.contains_key(&source_table_id) {
            return;
        }

        let column_ids: Vec<u16> = column_specs.iter().map(|(id, _)| *id).collect();
        let source_types: Vec<DataType> = column_specs.iter().map(|(_, dt)| *dt).collect();

        let granularities = CqGranularity::all();
        let mut definitions: Vec<CqDefinition> = Vec::with_capacity(4);

        for (i, gran) in granularities.iter().enumerate() {
            let name = format!("cq_{}_{}", gran.short_name(), source_table_id);
            let aggregations = vec![sensearcdb_common::AggFunction::Avg; column_ids.len()];

            let mut def = CqDefinition::new(
                name,
                source_table_id,
                target_table_ids[i],
                *gran,
                column_ids.clone(),
                aggregations,
            );
            def.source_types = source_types.clone();

            // 填充下层 CQ 表 ID：CQ_10s 无下层（None），其余粒度指向下一层 CQ 表。
            if i > 0 {
                def.set_lower_target_table_id(target_table_ids[i - 1]);
            }

            definitions.push(def);
        }

        // 先落内存（O(1)），再把阻塞的 SQLite 落盘移出 CqEngine 的 tokio 写锁临界区。
        // `on_table_created` 由 `CqSchemaSynchronizer` 在持有 `cq_engine.write()` 时调用；
        // 若在此同步执行 4 次 upsert，写锁被阻塞 I/O 长时间占用，写路径 fanout 的
        // `try_read` 会持续失败（丢脏窗口标记）。这里改为「内存先可见 + 阻塞持久化
        // 交给 blocking 线程池」，使写锁只在内存插入期间短暂持有。
        self.definitions
            .insert(source_table_id, definitions.clone());
        self.has_cq_definitions.store(true, Ordering::Relaxed);

        if let Some(store) = self.definition_store.clone() {
            persist_blocking(move || {
                if let Err(e) = store.upsert_batch(&definitions) {
                    tracing::error!("failed to persist CQ definitions: {e}");
                }
            });
        }

        tracing::info!(
            "CQ definitions created for table {} with {} columns",
            source_table_id,
            column_ids.len()
        );
    }

    /// 表删除回调：移除该表的所有 CQ 定义和窗口状态。
    ///
    /// # Arguments
    /// * `table_id` - 被删除的表 ID。
    pub fn on_table_deleted(&mut self, table_id: u32) {
        self.definitions.remove(&table_id);
        if self.definitions.is_empty() {
            self.has_cq_definitions.store(false, Ordering::Relaxed);
        }

        // 清理窗口状态（需要遍历，因为 DashMap 不支持按前缀删除）
        let keys_to_remove: Vec<CqWindowKey> = self
            .window_states
            .iter()
            .filter(|entry| entry.key().source_table_id == table_id)
            .map(|entry| entry.key().clone())
            .collect();

        // 不变量「重算队列 key ⊆ window_states 键集」：删表必须同步清两处，否则队列里
        // 残留的 key 会在 Phase 1 的 `.expect` 处触发 invariant 断言。
        // 锁顺序：先 window_states（逐个 remove），再 recalculation_queue（单次 write
        // 锁批量 remove），两锁不嵌套持有，与 on_data_written（先 get_mut shard 锁再
        // queue.write()）同一方向，避免 ABBA 死锁。
        for key in &keys_to_remove {
            self.window_states.remove(key);
        }
        if !keys_to_remove.is_empty() {
            let mut queue = self.recalculation_queue.write();
            for key in &keys_to_remove {
                queue.remove(key);
            }
        }

        if let Some(store) = self.definition_store.as_ref() {
            if let Err(e) = store.delete_by_source(table_id) {
                tracing::error!("failed to delete CQ definitions for table {table_id}: {e}");
            }
        }

        tracing::info!("CQ definitions and states removed for table {}", table_id);
    }

    /// 表变更回调：更新 CQ 定义中的列列表与源类型。
    ///
    /// # Arguments
    /// * `table_id` - 变更的表 ID。
    /// * `added_columns` - 新增的数值列（`(column_id, source_type)`）。
    /// * `dropped_columns` - 删除的列 ID 列表。
    pub fn on_table_altered(
        &mut self,
        table_id: u32,
        added_columns: &[(u16, DataType)],
        dropped_columns: &[u16],
    ) {
        let Some(mut defs) = self.definitions.get_mut(&table_id) else {
            return;
        };

        for def in defs.iter_mut() {
            // 构建旧列 → 聚合映射（按 column_id 而非位置索引）。
            // 这样中间删列时，剩余列仍能从映射取回各自的聚合函数，
            // 避免位置错位导致的聚合函数错配。
            let old_agg_by_col: HashMap<u16, sensearcdb_common::AggFunction> = def
                .source_columns
                .iter()
                .copied()
                .zip(def.aggregations.iter().copied())
                .collect();

            // 同步构建旧列 → 源类型映射（按 column_id），保证删列/重排后
            // source_types 仍与 source_columns 一一对应，绝不按位置错位。
            let old_type_by_col: HashMap<u16, DataType> = def
                .source_columns
                .iter()
                .copied()
                .zip(def.source_types.iter().copied())
                .collect();

            // 新的 source_columns：丢弃被删列，保留其余列（顺序不变）。
            let mut new_columns: Vec<u16> = def
                .source_columns
                .iter()
                .copied()
                .filter(|col| !dropped_columns.contains(col))
                .collect();

            // 重对齐聚合：保留列从旧映射取对应聚合，未知列（理论上不应出现）补 AVG。
            let mut new_aggregations: Vec<sensearcdb_common::AggFunction> = new_columns
                .iter()
                .map(|col| {
                    *old_agg_by_col
                        .get(col)
                        .unwrap_or(&sensearcdb_common::AggFunction::Avg)
                })
                .collect();

            // 重对齐源类型：保留列从旧映射取对应类型，未知列回退 F64。
            let mut new_types: Vec<DataType> = new_columns
                .iter()
                .map(|col| *old_type_by_col.get(col).unwrap_or(&DataType::F64))
                .collect();

            // 追加新增列（默认 AVG 聚合 + 携带其源类型）。防御性跳过已存在的列，保持一一对应。
            for &(col, col_type) in added_columns {
                if new_columns.contains(&col) {
                    continue;
                }
                new_columns.push(col);
                new_aggregations.push(sensearcdb_common::AggFunction::Avg);
                new_types.push(col_type);
            }

            def.source_columns = new_columns;
            def.aggregations = new_aggregations;
            def.source_types = new_types;
        }

        // 持久化更新后的定义
        if let Some(store) = self.definition_store.as_ref() {
            if let Some(defs) = self.definitions.get(&table_id) {
                for def in defs.iter() {
                    if let Err(e) = store.upsert(def) {
                        tracing::error!(
                            "failed to persist altered CQ definition {}: {e}",
                            def.name
                        );
                    }
                }
            }
        }

        tracing::info!(
            "CQ definitions updated for table {}: +{} columns, -{} columns",
            table_id,
            added_columns.len(),
            dropped_columns.len()
        );
    }

    /// 获取 CQ 引擎是否正在运行。
    #[inline]
    pub fn is_running(&self) -> bool {
        *self.running.read()
    }

    /// 获取所有 CQ 定义（用于 AutoRouter 和管理接口）。
    pub fn get_definitions(&self) -> Vec<CqDefinition> {
        self.definitions
            .iter()
            .flat_map(|entry| entry.value().clone())
            .collect()
    }

    /// 获取指定源表的 CQ 定义。
    pub fn get_definitions_for_table(&self, table_id: u32) -> Vec<CqDefinition> {
        self.definitions
            .get(&table_id)
            .map(|defs| defs.clone())
            .unwrap_or_default()
    }

    /// 获取已注册 CQ 定义的源表 ID 集合（对账 diff 用，不克隆定义体）。
    ///
    /// P0-2：`CqSchemaSynchronizer::reconcile_cq_tables` 据此对「mirror 里的 Raw 表」
    /// 与「已建 CQ 定义的源表」做差集，只补缺失项，O(缺失数) 而非 O(全量×4)。
    pub fn defined_source_tables(&self) -> std::collections::HashSet<u32> {
        self.definitions.iter().map(|entry| *entry.key()).collect()
    }

    /// 对账真相源：config.db 已持久化的源表 ID 集合。
    /// 无持久化存储（纯内存模式/测试）时回退到内存 `defined_source_tables`，保持语义一致。
    pub fn persisted_source_tables(&self) -> Result<std::collections::HashSet<u32>, SenseArcError> {
        match &self.definition_store {
            Some(store) => store.list_source_table_ids(),
            None => Ok(self.defined_source_tables()),
        }
    }

    /// 同步将指定源表的 CQ 定义落盘（对账兜底：fire-and-forget 失败后 db 缺定义时补落）。
    /// 不持 CqEngine 写锁（只 DashMap 读 + 同步 SQLite），供 `reconcile_cq_tables` 在写锁外调用。
    pub fn persist_definitions_sync(&self, source_table_id: u32) -> Result<(), SenseArcError> {
        let Some(store) = self.definition_store.as_ref() else {
            return Ok(());
        };
        let defs = self.get_definitions_for_table(source_table_id);
        if defs.is_empty() {
            return Ok(());
        }
        store.upsert_batch(&defs)
    }

    /// 启用/禁用指定源表的全部 CQ 定义，并持久化到 config.db。
    ///
    /// - `enabled=true`：调用 [`CqDefinition::enable`]（Disabled → Running）。
    /// - `enabled=false`：调用 [`CqDefinition::disable`]（置 Disabled，停止调度聚合）。
    ///
    /// 返回实际更新的定义条数。
    pub fn set_definitions_enabled(&self, table_id: u32, enabled: bool) -> usize {
        let mut updated = 0usize;
        if let Some(mut defs) = self.definitions.get_mut(&table_id) {
            for def in defs.iter_mut() {
                if enabled {
                    def.enable();
                } else {
                    def.disable();
                }
                updated += 1;
            }
        }

        // 状态变更落盘（与定义存储共用 upsert，保持重启后可恢复）。
        if let Some(store) = self.definition_store.as_ref() {
            if let Some(defs) = self.definitions.get(&table_id) {
                for def in defs.iter() {
                    if let Err(e) = store.upsert(def) {
                        tracing::error!(
                            "failed to persist CQ enable/disable for {}: {e}",
                            def.name
                        );
                    }
                }
            }
        }
        updated
    }

    /// 获取指定窗口的状态。
    pub fn get_window_state(&self, key: &CqWindowKey) -> Option<CqWindowState> {
        self.window_states.get(key).map(|s| s.clone())
    }

    /// 获取重算队列长度。
    pub fn recalculation_queue_len(&self) -> usize {
        self.recalculation_queue.read().len()
    }
}

// ─── CqRouteProvider impl ───

impl CqRouteProvider for CqEngine {
    /// 检查查询是否可以路由到 CQ 路径。
    ///
    /// 根据当前已注册的 CQ 定义，判断是否有匹配查询时间范围的
    /// 预聚合窗口。引擎的 `AutoRouter` 通过此方法判断使用 Raw
    /// 还是 CQ 执行路径。
    fn route_query(&self, table_id: u32, time_range_ms: i64) -> Option<CqRouteHint> {
        use crate::auto_router::AutoRouter;

        let definitions = self.get_definitions_for_table(table_id);
        if definitions.is_empty() {
            return None;
        }

        let mut router = AutoRouter::new();
        for def in &definitions {
            router.add_window(crate::auto_router::CqWindowInfo {
                source_table_id: def.source_table_id,
                target_table_id: def.target_table_id,
                granularity: def.granularity,
                interval_ns: def.interval_ns,
                column_ids: def.source_columns.clone(),
            });
        }

        router
            .select_granularity(table_id, time_range_ms)
            .map(|(target_table_id, _granularity)| CqRouteHint { target_table_id })
    }
}

// ─── Helper ───

/// 将时间戳向上对齐到最近的窗口边界。
///
/// # Arguments
/// * `ts` - 时间戳（epoch 纳秒）。
/// * `interval_ns` - 窗口长度（纳秒）。
///
/// # Returns
/// 对齐后的窗口结束时间戳。
#[inline]
pub fn align_timestamp_up(ts: i64, interval_ns: i64) -> i64 {
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

/// 将时间戳向下对齐到最近的窗口边界。
///
/// # Returns
/// 对齐后的窗口起始时间戳。
#[inline]
pub fn align_timestamp_down(ts: i64, interval_ns: i64) -> i64 {
    if ts <= 0 {
        return 0;
    }
    ts - (ts % interval_ns)
}

/// 在 tokio runtime 可用时把阻塞 SQLite 持久化派发到 blocking 线程池，
/// 避免在 `CqEngine` 的 tokio 写锁临界区执行阻塞 I/O；无 runtime 时同步执行
/// （单元测试 / 同步上下文兜底，保持既有落盘语义）。
fn persist_blocking<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            let _ = handle.spawn_blocking(f);
        }
        Err(_) => f(),
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_resolver::FormulaColumnResolver;

    #[test]
    fn test_align_timestamp_up() {
        // 10s window = 10_000_000_000 ns
        let interval = 10_000_000_000i64;

        assert_eq!(align_timestamp_up(0, interval), interval);
        assert_eq!(align_timestamp_up(5_000_000_000, interval), 10_000_000_000);
        assert_eq!(align_timestamp_up(10_000_000_000, interval), 10_000_000_000);
        assert_eq!(align_timestamp_up(15_000_000_000, interval), 20_000_000_000);
    }

    #[test]
    fn test_align_timestamp_down() {
        let interval = 10_000_000_000i64;

        assert_eq!(align_timestamp_down(0, interval), 0);
        assert_eq!(align_timestamp_down(5_000_000_000, interval), 0);
        assert_eq!(
            align_timestamp_down(10_000_000_000, interval),
            10_000_000_000
        );
        assert_eq!(
            align_timestamp_down(15_000_000_000, interval),
            10_000_000_000
        );
    }

    /// 中间删列后，剩余列的聚合函数必须按 column_id 重对齐，不得错位。
    ///
    /// 场景：[0,1,2] + [Avg,Sum,Min]，删列 1、加列 3
    /// → 列 [0,2,3] + [Avg,Min,Avg]（列 2 保留 Min，而非错位成 Sum）。
    #[test]
    fn test_on_table_altered_realigns_aggregations_by_column_id() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }

        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let mut engine = CqEngine::new_with_column_resolver(
            std::sync::Arc::new(MockExecutor),
            std::sync::Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        let table_id = 42u32;
        let mut def = CqDefinition::new(
            "cq_10s_42".into(),
            table_id,
            142,
            CqGranularity::Cq10s,
            vec![0, 1, 2],
            vec![AggFunction::Avg, AggFunction::Sum, AggFunction::Min],
        );
        def.source_types = vec![DataType::F64, DataType::U64, DataType::I32];
        engine.definitions.insert(table_id, vec![def]);

        engine.on_table_altered(table_id, &[(3, DataType::I64)], &[1]);

        let defs = engine.definitions.get(&table_id).unwrap();
        let def = &defs[0];
        assert_eq!(def.source_columns, vec![0, 2, 3]);
        assert_eq!(
            def.aggregations,
            vec![AggFunction::Avg, AggFunction::Min, AggFunction::Avg]
        );
        // source_types 必须按 column_id 同步重对齐：删列 1 后列 2 保留 I32（而非错位成 U64），
        // 新增列 3 携带 I64。
        assert_eq!(
            def.source_types,
            vec![DataType::F64, DataType::I32, DataType::I64]
        );
    }

    /// 不变量「重算队列 key ⊆ window_states 键集」：删表必须同步清两处。
    /// 删表后该表所有窗口从 window_states 与重算队列同时消失，其它表不受影响。
    #[test]
    fn test_on_table_deleted_clears_window_states_and_queue() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let mut engine = CqEngine::new_with_column_resolver(
            std::sync::Arc::new(MockExecutor),
            std::sync::Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        // 表 42（待删）与表 43（保留）各注册一个定义。
        for (table_id, target) in [(42u32, 142u32), (43u32, 143u32)] {
            let def = CqDefinition::new(
                format!("cq_10s_{table_id}"),
                table_id,
                target,
                CqGranularity::Cq10s,
                vec![0],
                vec![AggFunction::Avg],
            );
            engine.definitions.insert(table_id, vec![def]);
        }

        // 两个表各有两个窗口，全部标记 dirty 并入队（模拟已入队的迟到窗口）。
        let keys: Vec<CqWindowKey> = vec![
            CqWindowKey::new(42, CqGranularity::Cq10s, 10_000_000_000),
            CqWindowKey::new(42, CqGranularity::Cq1m, 60_000_000_000),
            CqWindowKey::new(43, CqGranularity::Cq10s, 10_000_000_000),
            CqWindowKey::new(43, CqGranularity::Cq1m, 60_000_000_000),
        ];
        for key in &keys {
            let mut state = CqWindowState::new();
            state.mark_calculated(1_000_000_000);
            state.mark_dirty();
            engine.window_states.insert(key.clone(), state);
            engine.recalculation_queue.write().push(key.clone());
        }

        assert_eq!(engine.window_states.len(), 4);
        assert_eq!(engine.recalculation_queue_len(), 4);

        engine.on_table_deleted(42);

        // 删表后：表 42 的窗口从两处同时消失，表 43 的窗口保留。
        assert_eq!(
            engine.window_states.len(),
            2,
            "删表后 window_states 只应保留其它表窗口"
        );
        assert_eq!(
            engine.recalculation_queue_len(),
            2,
            "删表后重算队列必须同步清空该表窗口（不变量）"
        );
        assert!(engine.window_states.get(&keys[0]).is_none());
        assert!(engine.window_states.get(&keys[1]).is_none());
        assert!(engine.window_states.get(&keys[2]).is_some());
        assert!(engine.window_states.get(&keys[3]).is_some());

        // 队列剩余元素只含表 43 的 key（FIFO 顺序：先 10s 后 1m）。
        let mut remaining = engine.recalculation_queue.write();
        assert_eq!(remaining.pop(), Some(keys[2].clone()));
        assert_eq!(remaining.pop(), Some(keys[3].clone()));
        assert!(remaining.is_empty());
    }

    /// P2-02：enable/disable 必须真实改变定义状态并持久化，
    /// disable 后定义不可调度（聚合停止），enable 后恢复可调度。
    #[test]
    fn test_set_definitions_enabled_persists_and_affects_schedulability() {
        use crate::definition::CqStatus;
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("config.db");
        let store = Arc::new(CqDefinitionStore::open(&db).unwrap());

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            Some(store.clone()),
            Arc::new(FormulaColumnResolver),
        );

        let table_id = 42u32;
        let mut def = CqDefinition::new(
            "cq_10s_42".into(),
            table_id,
            142,
            CqGranularity::Cq10s,
            vec![0],
            vec![AggFunction::Avg],
        );
        def.source_types = vec![DataType::F64];
        engine.definitions.insert(table_id, vec![def]);

        // disable：状态 Disabled + 不可调度 + 落库。
        assert_eq!(engine.set_definitions_enabled(table_id, false), 1);
        let defs = engine.get_definitions_for_table(table_id);
        assert_eq!(defs[0].status, CqStatus::Disabled);
        assert!(!defs[0].is_schedulable(), "禁用后 CQ 不得再触发聚合");
        assert_eq!(store.load_all().unwrap()[0].status, CqStatus::Disabled);

        // enable：恢复 Running + 可调度 + 落库。
        assert_eq!(engine.set_definitions_enabled(table_id, true), 1);
        let defs = engine.get_definitions_for_table(table_id);
        assert_eq!(defs[0].status, CqStatus::Running);
        assert!(defs[0].is_schedulable(), "启用后 CQ 应恢复聚合");
        assert_eq!(store.load_all().unwrap()[0].status, CqStatus::Running);
    }

    /// `persist_definitions_sync` 在无持久化 store（纯内存/测试）时必须返回 Ok 且不 panic，
    /// 保持纯内存模式语义一致。
    #[test]
    fn test_persist_definitions_sync_without_store_is_noop_ok() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        let def = CqDefinition::new(
            "cq_10s_42".into(),
            42,
            142,
            CqGranularity::Cq10s,
            vec![0],
            vec![AggFunction::Avg],
        );
        engine.definitions.insert(42, vec![def]);

        // 无 store：直接 Ok，不做任何 I/O。
        assert!(engine.persist_definitions_sync(42).is_ok());
    }

    /// `persist_definitions_sync` 在有 store 时必须把内存定义同步落进 config.db
    /// （对账兜底：fire-and-forget 失败后 db 缺定义时补落）。
    #[test]
    fn test_persist_definitions_sync_persists_to_store() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("config.db");
        let store = Arc::new(CqDefinitionStore::open(&db).unwrap());

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            Some(store.clone()),
            Arc::new(FormulaColumnResolver),
        );

        let mut def = CqDefinition::new(
            "cq_10s_42".into(),
            42,
            142,
            CqGranularity::Cq10s,
            vec![0],
            vec![AggFunction::Avg],
        );
        def.source_types = vec![DataType::F64];
        engine.definitions.insert(42, vec![def]);

        engine.persist_definitions_sync(42).unwrap();

        let ids = store.list_source_table_ids().unwrap();
        assert!(
            ids.contains(&42),
            "config.db 必须被同步补落 source_table_id=42"
        );
    }

    /// P3 语义一致性：同一批内多个时间戳落在同一窗口时，只标脏一次、
    /// 只入队一次（批内窗口去重后 get_mut 次数 = 去重后窗口数）。
    #[test]
    fn test_on_data_written_batch_window_dedup() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        let table_id = 42u32;
        let def = CqDefinition::new(
            "cq_10s_42".into(),
            table_id,
            142,
            CqGranularity::Cq10s,
            vec![0],
            vec![AggFunction::Avg],
        );
        engine.definitions.insert(table_id, vec![def]);

        // 预置一个已计算的 10s 窗口（window_end = 10s），可被迟到数据标脏。
        let key = CqWindowKey::new(table_id, CqGranularity::Cq10s, 10_000_000_000);
        let mut state = CqWindowState::new();
        state.mark_calculated(1_000_000_000);
        engine.window_states.insert(key.clone(), state);

        // 同一批内 3 个时间戳都落在同一个 10s 窗口（0~10s），只应标脏一次、入队一次。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![5_000_000_000, 6_000_000_000, 7_000_000_000],
            batch_id: 1,
        });

        let st = engine.window_states.get(&key).unwrap();
        assert!(st.dirty, "同窗口多点应标脏（首次）");
        assert_eq!(
            engine.recalculation_queue_len(),
            1,
            "同窗口多点只入队一次（批内窗口去重）"
        );
    }

    /// P1 数据正确性：跨表同 `batch_id` 不得误去重——两张表的窗口都必须被标脏。
    ///
    /// 根因：`fanout_batch` 按 table 分组后共享同一 batch_id，`processed_batches`
    /// 过去只按 batch_id 去重，导致第二张表起被静默丢弃。修复后去重键为
    /// `(table_id, batch_id)`：跨表同批不冲突、同表同批仍去重。
    #[test]
    fn test_on_data_written_multi_table_same_batch_id_marks_both_tables() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        // 两张源表各一条 CQ 定义。
        for (tid, target) in [(42u32, 142u32), (43u32, 143u32)] {
            let def = CqDefinition::new(
                format!("cq_10s_{tid}"),
                tid,
                target,
                CqGranularity::Cq10s,
                vec![0],
                vec![AggFunction::Avg],
            );
            engine.definitions.insert(tid, vec![def]);
        }

        // 预置两张表各自的已计算 10s 窗口（window_end = 10s）。
        for tid in [42u32, 43u32] {
            let key = CqWindowKey::new(tid, CqGranularity::Cq10s, 10_000_000_000);
            let mut state = CqWindowState::new();
            state.mark_calculated(1_000_000_000);
            engine.window_states.insert(key, state);
        }

        // 同一 batch_id=1 跨两张表：修复前只第一张表被标脏（去重误杀），修复后两张都标脏。
        engine.on_data_written(WriteEvent {
            table_id: 42,
            timestamps: vec![5_000_000_000],
            batch_id: 1,
        });
        engine.on_data_written(WriteEvent {
            table_id: 43,
            timestamps: vec![5_000_000_000],
            batch_id: 1,
        });

        let st42 = engine
            .window_states
            .get(&CqWindowKey::new(42, CqGranularity::Cq10s, 10_000_000_000))
            .unwrap();
        let st43 = engine
            .window_states
            .get(&CqWindowKey::new(43, CqGranularity::Cq10s, 10_000_000_000))
            .unwrap();
        assert!(st42.dirty, "表 42 窗口必须被标脏");
        assert!(st43.dirty, "表 43 窗口必须被标脏（跨表同 batch_id 不得误去重）");
        assert_eq!(engine.recalculation_queue_len(), 2, "两张表各入队一次");

        // 同表同批重复调用必须仍去重：第二次写入表 42（同 batch_id）不得再入队。
        engine.on_data_written(WriteEvent {
            table_id: 42,
            timestamps: vec![6_000_000_000],
            batch_id: 1,
        });
        assert_eq!(
            engine.recalculation_queue_len(),
            2,
            "同表同批重复调用必须去重（不重复入队）"
        );
    }

    /// D1（fresh eyes）：跨表同批 + 同表同批混合场景。
    ///
    /// 一批里 2 张表各 2 点（同表两点落在同一窗口），验证：
    /// - 跨表同 batch_id 不冲突（两张表窗口都标脏）；
    /// - 同表同窗口多点只标脏一次（批内窗口去重，不重复入队）。
    #[test]
    fn test_mixed_cross_table_and_same_table_batch_dedup() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        // 两张源表各一条 CQ 定义，并预置各自已计算的 10s 窗口。
        for (tid, target) in [(42u32, 142u32), (43u32, 143u32)] {
            let def = CqDefinition::new(
                format!("cq_10s_{tid}"),
                tid,
                target,
                CqGranularity::Cq10s,
                vec![0],
                vec![AggFunction::Avg],
            );
            engine.definitions.insert(tid, vec![def]);

            let key = CqWindowKey::new(tid, CqGranularity::Cq10s, 10_000_000_000);
            let mut state = CqWindowState::new();
            state.mark_calculated(1_000_000_000);
            engine.window_states.insert(key, state);
        }

        // 同一 batch_id=9：表 42 与表 43 各 2 点（均落在同一 10s 窗口 0~10s）。
        engine.on_data_written(WriteEvent {
            table_id: 42,
            timestamps: vec![5_000_000_000, 7_000_000_000],
            batch_id: 9,
        });
        engine.on_data_written(WriteEvent {
            table_id: 43,
            timestamps: vec![5_000_000_000, 7_000_000_000],
            batch_id: 9,
        });

        let st42 = engine
            .window_states
            .get(&CqWindowKey::new(42, CqGranularity::Cq10s, 10_000_000_000))
            .unwrap();
        let st43 = engine
            .window_states
            .get(&CqWindowKey::new(43, CqGranularity::Cq10s, 10_000_000_000))
            .unwrap();
        assert!(st42.dirty, "表 42 窗口必须标脏（跨表同批不冲突）");
        assert!(st43.dirty, "表 43 窗口必须标脏（跨表同批不冲突）");
        assert_eq!(
            engine.recalculation_queue_len(),
            2,
            "每表只入队一次：跨表不冲突（2 表各 1）、同表同窗口去重（每表 2 点合并为 1）"
        );
    }

    /// D2（fresh eyes）：同 (table_id, batch_id) 重放幂等（不重复标脏）。
    ///
    /// 同表同批用不同窗口的时间戳重放，验证 processed_batches 的去重键
    /// `(table_id, batch_id)` 在窗口层面也能拦住（整批早退，不进入窗口遍历）。
    #[test]
    fn test_duplicate_batch_replay_is_idempotent() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        let table_id = 42u32;
        let def = CqDefinition::new(
            "cq_10s_42".into(),
            table_id,
            142,
            CqGranularity::Cq10s,
            vec![0],
            vec![AggFunction::Avg],
        );
        engine.definitions.insert(table_id, vec![def]);

        // 预置两个已计算窗口：window_end=10s 与 window_end=20s。
        for window_end in [10_000_000_000i64, 20_000_000_000i64] {
            let key = CqWindowKey::new(table_id, CqGranularity::Cq10s, window_end);
            let mut state = CqWindowState::new();
            state.mark_calculated(1_000_000_000);
            engine.window_states.insert(key, state);
        }

        // 首次：batch_id=7 命中窗口 A（5s → window_end=10s）。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![5_000_000_000],
            batch_id: 7,
        });
        assert_eq!(engine.recalculation_queue_len(), 1, "首次标脏窗口 A");

        // 重放：同 (table_id=42, batch_id=7) 命中不同窗口 B（15s → window_end=20s）。
        // 必须整批去重早退，窗口 B 不得被标脏、不得入队。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![15_000_000_000],
            batch_id: 7,
        });
        assert_eq!(
            engine.recalculation_queue_len(),
            1,
            "同 (table_id, batch_id) 重放必须幂等：窗口 B 不标脏"
        );
        {
            // 窗口 B 状态读守卫限定在独立作用域内，块结束立即释放，
            // 避免与后续 on_data_written 内 get_mut 的写锁在同一 shard 死锁。
            let st_b = engine
                .window_states
                .get(&CqWindowKey::new(table_id, CqGranularity::Cq10s, 20_000_000_000))
                .unwrap();
            assert!(!st_b.dirty, "重放不得标脏窗口 B（幂等）");
        }

        // 新 batch_id=8 命中窗口 B：应正常标脏并入队。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![15_000_000_000],
            batch_id: 8,
        });
        assert_eq!(engine.recalculation_queue_len(), 2, "新 batch_id 应正常标脏窗口 B");
        {
            let st_b = engine
                .window_states
                .get(&CqWindowKey::new(table_id, CqGranularity::Cq10s, 20_000_000_000))
                .unwrap();
            assert!(st_b.dirty, "新 batch_id 应标脏窗口 B");
        }
    }

    /// QA 边界（独立重写）：跨表同批 + 同表同窗口多点混合，并直接断言
    /// `processed_batches` 的去重键为 `(table_id, batch_id)`。
    ///
    /// 断言点：
    /// - 跨表同 batch_id 不冲突：两张表窗口都标脏，状态/计算次数不变；
    /// - 同表同窗口两点只标脏一次：重算队列总长 = 2（每表各 1）；
    /// - `processed_batches` 保留两条跨表记录 `(101,77)` 与 `(102,77)`。
    #[test]
    fn test_qa_boundary_mixed_batch_marks_each_table_once() {
        use std::collections::HashSet;
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        // 两张源表，各一条 Cq10s 定义，预置已计算窗口 window_end=10s。
        for (tid, target) in [(101u32, 201u32), (102u32, 202u32)] {
            let def = CqDefinition::new(
                format!("qa_cq10_{tid}"),
                tid,
                target,
                CqGranularity::Cq10s,
                vec![0],
                vec![AggFunction::Avg],
            );
            engine.definitions.insert(tid, vec![def]);
            let key = CqWindowKey::new(tid, CqGranularity::Cq10s, 10_000_000_000);
            let mut state = CqWindowState::new();
            state.mark_calculated(1_000_000_000);
            engine.window_states.insert(key, state);
        }

        // 同一 batch_id=77：表 101 与 102 各 2 点（同表两点同窗口）。
        engine.on_data_written(WriteEvent {
            table_id: 101,
            timestamps: vec![5_000_000_000, 7_000_000_000],
            batch_id: 77,
        });
        engine.on_data_written(WriteEvent {
            table_id: 102,
            timestamps: vec![5_000_000_000, 7_000_000_000],
            batch_id: 77,
        });

        // 跨表不冲突：两表窗口都标脏，状态仍为 Calculated、计算次数不变。
        for tid in [101u32, 102u32] {
            let st = engine
                .window_states
                .get(&CqWindowKey::new(tid, CqGranularity::Cq10s, 10_000_000_000))
                .unwrap();
            assert!(st.dirty, "表 {tid} 窗口必须标脏（跨表同批不冲突）");
            assert_eq!(st.status, CqWindowStatus::Calculated, "标脏不改窗口状态");
            assert_eq!(st.calculation_count, 1, "标脏不增加计算次数");
        }

        // 同表同窗口两点只标脏一次：队列总长 = 2（每表各 1）。
        assert_eq!(engine.recalculation_queue_len(), 2, "每表只入队一次（同表同窗口去重）");

        // 直接断言去重键 (table_id, batch_id)：两条跨表记录都保留。
        assert_eq!(engine.processed_batches.len(), 2, "跨表同批应保留两条记录");
        assert!(engine.processed_batches.contains(&(101u32, 77u64)));
        assert!(engine.processed_batches.contains(&(102u32, 77u64)));

        // 队列内容恰为两表各自的窗口键（顺序无关）。
        let drained: HashSet<CqWindowKey> = engine
            .recalculation_queue
            .write()
            .drain(10)
            .into_iter()
            .collect();
        let expected: HashSet<CqWindowKey> = [101u32, 102u32]
            .into_iter()
            .map(|tid| CqWindowKey::new(tid, CqGranularity::Cq10s, 10_000_000_000))
            .collect();
        assert_eq!(drained, expected, "队列应恰好包含两表各自的窗口键");
    }

    /// QA 边界（独立重写）：同 `(table_id, batch_id)` 重放幂等，直接断言
    /// `processed_batches` 不因重放膨胀、窗口不重复标脏。
    #[test]
    fn test_qa_boundary_duplicate_batch_replay_idempotent() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        let table_id = 303u32;
        let def = CqDefinition::new(
            "qa_cq10_303".into(),
            table_id,
            403,
            CqGranularity::Cq10s,
            vec![0],
            vec![AggFunction::Avg],
        );
        engine.definitions.insert(table_id, vec![def]);

        // 预置两个已计算窗口：window_end=10s 与 20s。
        for window_end in [10_000_000_000i64, 20_000_000_000i64] {
            let key = CqWindowKey::new(table_id, CqGranularity::Cq10s, window_end);
            let mut state = CqWindowState::new();
            state.mark_calculated(1_000_000_000);
            engine.window_states.insert(key, state);
        }

        // 首次：batch=909 命中窗口 A（5s → window_end=10s）。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![5_000_000_000],
            batch_id: 909,
        });
        assert_eq!(engine.recalculation_queue_len(), 1, "首次标脏窗口 A");
        assert_eq!(engine.processed_batches.len(), 1);
        assert!(engine.processed_batches.contains(&(table_id, 909u64)));

        // 重放同 (303, 909)，但时间戳落在窗口 B（15s）：必须整批去重早退。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![15_000_000_000],
            batch_id: 909,
        });
        assert_eq!(engine.recalculation_queue_len(), 1, "重放不新增入队");
        assert_eq!(engine.processed_batches.len(), 1, "重放不膨胀去重集");
        {
            let st_b = engine
                .window_states
                .get(&CqWindowKey::new(table_id, CqGranularity::Cq10s, 20_000_000_000))
                .unwrap();
            assert!(!st_b.dirty, "重放不得标脏窗口 B（幂等）");
        }

        // 新 batch=910 命中窗口 B：应正常标脏并入队。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![15_000_000_000],
            batch_id: 910,
        });
        assert_eq!(engine.recalculation_queue_len(), 2, "新 batch 正常标脏窗口 B");
        assert_eq!(engine.processed_batches.len(), 2);
        {
            let st_b = engine
                .window_states
                .get(&CqWindowKey::new(table_id, CqGranularity::Cq10s, 20_000_000_000))
                .unwrap();
            assert!(st_b.dirty, "新 batch 标脏窗口 B");
        }
    }

    /// QA fresh-eyes 边界 A（本轮独立重写）：跨表同批 + 同表同窗口多点，且时间戳乱序并含重复点。
    ///
    /// 与既有边界测试的差异：时间戳刻意乱序（9s/2s/5s/2s）并含重复点，
    /// 全部归并到同一 10s 窗口；额外断言「已 dirty 窗口被新 batch 再次命中时不重复入队」。
    #[test]
    fn test_qa_fresh_cross_table_same_window_out_of_order() {
        use std::collections::HashSet;
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        // 两张源表各一条 Cq10s 定义，预置已计算窗口 window_end=10s。
        for (tid, target) in [(501u32, 601u32), (502u32, 602u32)] {
            let def = CqDefinition::new(
                format!("qa_fresh_cq10_{tid}"),
                tid,
                target,
                CqGranularity::Cq10s,
                vec![0],
                vec![AggFunction::Avg],
            );
            engine.definitions.insert(tid, vec![def]);
            let key = CqWindowKey::new(tid, CqGranularity::Cq10s, 10_000_000_000);
            let mut state = CqWindowState::new();
            state.mark_calculated(1_000_000_000);
            engine.window_states.insert(key, state);
        }

        // 同 batch=5555：表 501 时间戳乱序且含重复点，表 502 常规两点，全部落在 0~10s 窗口。
        engine.on_data_written(WriteEvent {
            table_id: 501,
            timestamps: vec![9_000_000_000, 2_000_000_000, 5_000_000_000, 2_000_000_000],
            batch_id: 5555,
        });
        engine.on_data_written(WriteEvent {
            table_id: 502,
            timestamps: vec![4_000_000_000, 8_000_000_000],
            batch_id: 5555,
        });

        // 跨表不冲突，乱序/重复点归并到同一窗口：两表窗口都标脏，状态/计数不变。
        for tid in [501u32, 502u32] {
            let st = engine
                .window_states
                .get(&CqWindowKey::new(tid, CqGranularity::Cq10s, 10_000_000_000))
                .unwrap();
            assert!(st.dirty, "表 {tid} 窗口必须标脏（跨表同批不冲突）");
            assert_eq!(st.status, CqWindowStatus::Calculated, "标脏不改窗口状态");
            assert_eq!(st.calculation_count, 1, "标脏不增加计算次数");
        }

        assert_eq!(
            engine.recalculation_queue_len(),
            2,
            "乱序/重复点归并后每表只入队一次（同表同窗口去重）"
        );
        assert_eq!(engine.processed_batches.len(), 2, "跨表同批保留两条去重记录");
        assert!(engine.processed_batches.contains(&(501u32, 5555u64)));
        assert!(engine.processed_batches.contains(&(502u32, 5555u64)));

        // 队列内容恰为两表各自的窗口键（顺序无关）。
        let drained: HashSet<CqWindowKey> = engine
            .recalculation_queue
            .write()
            .drain(10)
            .into_iter()
            .collect();
        let expected: HashSet<CqWindowKey> = [501u32, 502u32]
            .into_iter()
            .map(|tid| CqWindowKey::new(tid, CqGranularity::Cq10s, 10_000_000_000))
            .collect();
        assert_eq!(drained, expected, "队列应恰好包含两表各自的窗口键");

        // 已 dirty 窗口被新 batch 命中：不得重复入队（此时队列已被 drain 清空，若重复标脏会变成 1）。
        engine.on_data_written(WriteEvent {
            table_id: 501,
            timestamps: vec![6_000_000_000],
            batch_id: 5556,
        });
        assert_eq!(
            engine.recalculation_queue_len(),
            0,
            "已 dirty 窗口被新 batch 命中不应重复入队（同表同窗口只标脏一次，跨批也成立）"
        );
        assert_eq!(engine.processed_batches.len(), 3, "新 batch 正常记录去重键，但不重复入队");
    }

    /// QA fresh-eyes 边界 B（本轮独立重写）：同 `(table_id, batch_id)` 重放幂等，
    /// 覆盖「同窗口重放」与「跨窗口重放」两种重放形态，并断言 `processed_batches` 不膨胀。
    ///
    /// 与既有测试差异：额外覆盖同窗口重放（5s → 仍窗口 A）这一形态。
    #[test]
    fn test_qa_fresh_duplicate_batch_replay_idempotent() {
        use sensearcdb_common::{AggFunction, AppConfig};

        struct MockExecutor;
        impl CqQueryExecutor for MockExecutor {
            fn query_raw_with_ts(
                &self,
                _table_id: u32,
                _column_ids: &[u16],
                _time_start: i64,
                _time_end: i64,
            ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
        struct MockWriteTarget;
        impl CqWriteTarget for MockWriteTarget {
            fn write_batch(
                &self,
                _table_id: u32,
                _timestamp: i64,
                _column_values: &[(u16, TypedValue)],
            ) -> Result<(), SenseArcError> {
                Ok(())
            }
        }

        let engine = CqEngine::new_with_column_resolver(
            Arc::new(MockExecutor),
            Arc::new(MockWriteTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            Arc::new(FormulaColumnResolver),
        );

        let table_id = 701u32;
        let def = CqDefinition::new(
            "qa_fresh_cq10_701".into(),
            table_id,
            801,
            CqGranularity::Cq10s,
            vec![0],
            vec![AggFunction::Avg],
        );
        engine.definitions.insert(table_id, vec![def]);

        // 预置两个已计算窗口：window_end=10s 与 20s。
        for window_end in [10_000_000_000i64, 20_000_000_000i64] {
            let key = CqWindowKey::new(table_id, CqGranularity::Cq10s, window_end);
            let mut state = CqWindowState::new();
            state.mark_calculated(1_000_000_000);
            engine.window_states.insert(key, state);
        }

        // 首次 batch=777 命中窗口 A（5s → 10s）。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![5_000_000_000],
            batch_id: 777,
        });
        assert_eq!(engine.recalculation_queue_len(), 1, "首次标脏窗口 A");
        assert_eq!(engine.processed_batches.len(), 1);
        assert!(engine.processed_batches.contains(&(table_id, 777u64)));

        // 重放形态 1：同窗口重放（5s → 仍窗口 A），必须幂等。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![5_000_000_000],
            batch_id: 777,
        });
        assert_eq!(engine.recalculation_queue_len(), 1, "同窗口重放不新增入队");
        assert_eq!(engine.processed_batches.len(), 1, "重放不膨胀去重集");

        // 重放形态 2：跨窗口重放（15s → 窗口 B），整批去重早退，窗口 B 不得标脏。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![15_000_000_000],
            batch_id: 777,
        });
        assert_eq!(engine.recalculation_queue_len(), 1, "跨窗口重放不新增入队");
        assert_eq!(engine.processed_batches.len(), 1, "跨窗口重放仍不膨胀去重集");
        {
            let st_b = engine
                .window_states
                .get(&CqWindowKey::new(table_id, CqGranularity::Cq10s, 20_000_000_000))
                .unwrap();
            assert!(!st_b.dirty, "跨窗口重放不得标脏窗口 B（幂等）");
        }

        // 全新 batch=778 命中窗口 B：正常标脏入队。
        engine.on_data_written(WriteEvent {
            table_id,
            timestamps: vec![15_000_000_000],
            batch_id: 778,
        });
        assert_eq!(engine.recalculation_queue_len(), 2, "新 batch 正常标脏窗口 B");
        assert_eq!(engine.processed_batches.len(), 2);
        assert!(engine.processed_batches.contains(&(table_id, 778u64)));
        {
            let st_b = engine
                .window_states
                .get(&CqWindowKey::new(table_id, CqGranularity::Cq10s, 20_000_000_000))
                .unwrap();
            assert!(st_b.dirty, "新 batch 标脏窗口 B");
        }
    }
}

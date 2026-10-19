//! Derived tag engine: the core orchestrator.
//!
//! # Architecture
//!
//! The [`DerivedTagEngine`] is the central component that:
//! 1. Receives source-change notifications via `on_source_changed`.
//! 2. Determines which derived tags are affected (transitive dependency traversal).
//! 3. Evaluates them in topological order using the DAG.
//! 4. Writes results through the standard write path.
//!
//! # Recursion Protection (Three Layers)
//!
//! 1. **session_id**: Each `on_source_changed` call generates a unique session.
//!    Tags calculated within the same session are not re-triggered.
//! 2. **call_depth**: Maximum nesting depth for derived-on-derived chains.
//! 3. **cycle_detection**: The dependency graph rejects cycles at definition time.
//!
//! # Trait Abstractions
//!
//! To avoid circular crate dependencies, the engine uses trait objects for
//! external interfaces (snapshot reads, writes, schema access).

use crate::ast::ExprAst;
use crate::deadband::{check_deadband, DeadbandConfig, DeadbandResult};
use crate::debounce::{BatchIdTracker, DebounceState};
use crate::definition_store::{DerivedTagRecord, DerivedTagStore, SourceRefDef};
use crate::dependency_graph::{DependencyGraph, DerivedTagNode, NodeState, TagKey};
use crate::error::DerivedError;
use crate::evaluator::EvalContext;
use crate::fallback::{FallbackPolicy, LastGoodCache};
use crate::lexer::Lexer;
use crate::parser::PrattParser;
use parking_lot::RwLock;
use sensearcdb_common::{ColumnRef, ConfigHandle, Quality, TenantId};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use uuid::Uuid;

// ─── Trait Abstractions (to avoid circular crate deps) ───

/// Snapshot reader abstraction for looking up current source tag values.
pub trait SnapshotRead: Send + Sync {
    /// Batch-read the latest value, quality, and timestamp for a set of columns.
    ///
    /// Each entry is addressed by [`ColumnRef`] `(table_id, column_id)` — the
    /// wide-table column-level primitive. Returns `column → (value, quality,
    /// timestamp_nanos)`. Columns without an entry in the snapshot are simply
    /// absent from the returned map.
    fn batch_get_columns(&self, columns: &[ColumnRef]) -> HashMap<ColumnRef, (f64, u8, i64)>;
}

/// Write sink abstraction for persisting derived tag results.
pub trait WriteSink: Send + Sync {
    /// Write a batch of computed derived tag values through the standard write path.
    ///
    /// Each entry: `(table_id, tag_name, value, quality, timestamp_nanos)`.
    /// Derived tags remain independent single-column tables (column_id = 0).
    fn write_batch(&self, entries: &[(u32, String, f64, u8, i64)]);
}

/// Schema access abstraction for looking up table and column metadata.
pub trait SchemaAccess: Send + Sync {
    /// 反查 table_id → 归属租户（派生标签自身目标表的 tenant）。
    ///
    /// 返回 `None` 表示 mirror miss（异常路径，调用方回退 `DEFAULT` + warn）。
    fn table_tenant(&self, table_id: u32) -> Option<TenantId>;

    /// Resolve a source reference `(table_name, optional column_name)` to a
    /// [`ColumnRef`] `(table_id, column_id)`（租户作用域）。
    ///
    /// - `column_name == None` → the N=1 single-column case; resolve the table
    ///   and return `ColumnRef::single(table_id)` (column_id = 0).
    /// - `column_name == Some(col)` → resolve the specific wide-table column.
    ///
    /// Returns `None` when the table (or the referenced column) does not exist.
    fn resolve_column_ref(
        &self,
        tenant_id: TenantId,
        table_name: &str,
        column_name: Option<&str>,
    ) -> Option<ColumnRef>;
}

// ─── DerivedTagEngine ───

/// Configuration for the derived tag engine at runtime.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Whether the engine is enabled.
    pub enabled: bool,
    /// Debounce interval in milliseconds.
    pub debounce_interval_ms: u64,
    /// Max tags to recalculate per tick.
    pub max_recalc_per_tick: usize,
    /// Max AST depth.
    pub max_ast_depth: usize,
    /// Max graph nodes.
    pub max_graph_nodes: usize,
    /// Max retries for failed calculations.
    pub max_retries: u32,
    /// Calculation timeout per tag (ms).
    pub calc_timeout_ms: u64,
    /// Max staleness tolerance for cold-start (seconds).
    pub max_stale_secs: u64,
    /// Max call depth for derived-on-derived chains.
    pub max_call_depth: u32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            debounce_interval_ms: 100,
            max_recalc_per_tick: 500,
            max_ast_depth: 32,
            max_graph_nodes: 10000,
            max_retries: 3,
            calc_timeout_ms: 100,
            max_stale_secs: 300,
            max_call_depth: 10,
        }
    }
}

impl EngineConfig {
    /// Create config from the workspace DerivedConfig.
    pub fn from_derived_config(cfg: &sensearcdb_common::config::DerivedConfig) -> Self {
        Self {
            enabled: cfg.enabled,
            debounce_interval_ms: cfg.debounce_interval_ms,
            max_recalc_per_tick: cfg.max_recalc_per_tick,
            max_ast_depth: cfg.max_ast_depth,
            max_graph_nodes: cfg.max_graph_nodes,
            max_retries: cfg.max_retries,
            calc_timeout_ms: cfg.calc_timeout_ms,
            max_stale_secs: cfg.max_stale_secs,
            max_call_depth: 10, // hardcoded safe default
        }
    }
}

/// The derived tag engine: orchestrates expression parsing, dependency tracking,
/// evaluation, and result writing.
pub struct DerivedTagEngine {
    /// Unique session ID for the current `on_source_changed` invocation.
    /// Used for recursion guard (layer 1).
    session_id: RwLock<Uuid>,

    /// Dependency graph of all registered derived tags.
    dep_graph: Arc<RwLock<DependencyGraph>>,

    /// Batch ID tracker: prevents processing the same write batch twice.
    processed_batches: RwLock<BatchIdTracker>,

    /// Snapshot reader for fetching source tag values.
    snapshot: Arc<dyn SnapshotRead>,

    /// Write sink for persisting computed results.
    writer: Arc<dyn WriteSink>,

    /// Schema access for tag/table lookups.
    schema: Arc<dyn SchemaAccess>,

    /// Definition persistence store (config.db `derived_tags`).
    ///
    /// `None` 时定义仅驻内存（测试 / 未挂载持久化）；生产启动为 `Some`（D7 必选）。
    store: Option<Arc<DerivedTagStore>>,

    /// Configuration handle (hot-reloadable).
    config: ConfigHandle,

    /// Debounce state.
    debounce: Arc<RwLock<DebounceState>>,

    /// Cache of last-good values for lenient fallback.
    last_good_cache: RwLock<LastGoodCache>,

    /// Set of tags currently being calculated (for call_depth guard, layer 2).
    /// Maps (tenant_id, tag_name) → current call depth.
    in_flight: RwLock<HashMap<TagKey, u32>>,

    /// 是否注册了任何派生标签（写路径快路径标志，0 标签时跳过 derived fanout）。
    has_derived_tags: AtomicBool,

    /// 唤醒去抖 flusher 的信号（写路径 `mark_dirty` 后 notify，flusher 消费）。
    flush_notify: tokio::sync::Notify,
}

impl DerivedTagEngine {
    /// Create a new derived tag engine.
    pub fn new(
        snapshot: Arc<dyn SnapshotRead>,
        writer: Arc<dyn WriteSink>,
        schema: Arc<dyn SchemaAccess>,
        config: ConfigHandle,
        store: Option<Arc<DerivedTagStore>>,
    ) -> Self {
        let app_cfg = config.get();
        let engine_cfg = EngineConfig::from_derived_config(&app_cfg.derived);

        Self {
            session_id: RwLock::new(Uuid::new_v4()),
            dep_graph: Arc::new(RwLock::new(DependencyGraph::new())),
            processed_batches: RwLock::new(BatchIdTracker::new(10_000)),
            snapshot,
            writer,
            schema,
            store,
            config,
            debounce: Arc::new(RwLock::new(DebounceState::new(
                engine_cfg.debounce_interval_ms,
            ))),
            last_good_cache: RwLock::new(LastGoodCache::new()),
            in_flight: RwLock::new(HashMap::new()),
            has_derived_tags: AtomicBool::new(false),
            flush_notify: tokio::sync::Notify::new(),
        }
    }

    /// 是否注册了任何派生标签（写路径快路径标志）。
    pub fn has_derived_tags(&self) -> bool {
        self.has_derived_tags.load(Ordering::Relaxed)
    }

    /// Get the current engine configuration.
    pub fn get_config(&self) -> EngineConfig {
        let app_cfg = self.config.get();
        EngineConfig::from_derived_config(&app_cfg.derived)
    }

    /// Get a reference to the dependency graph.
    pub fn dep_graph(&self) -> &Arc<RwLock<DependencyGraph>> {
        &self.dep_graph
    }

    /// 反查目标表 `table_id` 的归属租户（同步注册入口的跨租户写授权用）。
    ///
    /// 返回 `None` 表示 mirror miss（表不存在或尚未同步）。此方法与
    /// [`Self::register_tag`] 内部的 D4/D6 归属反查解耦：后者仍负责异步写回
    /// 路径的归属判定（miss 回退 `DEFAULT` + warn），此处仅用于 handler 层
    /// 校验「请求方 tenant == 目标表归属 tenant」。
    pub fn table_tenant(&self, table_id: u32) -> Option<TenantId> {
        self.schema.table_tenant(table_id)
    }

    // ─── Tag Registration ───

    /// Register (or update) a derived tag definition.
    ///
    /// # Arguments
    /// * `table_id` - Target table ID.
    /// * `tag_name` - Name of the derived tag.
    /// * `expression` - The expression string (e.g. "`sensor_a` + `sensor_b` * 2").
    /// * `fallback_policy` - Fallback policy for this tag.
    /// * `deadband` - Deadband filter config.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Expression parsing fails
    /// - AST depth exceeds limit
    /// - Registering creates a circular dependency
    /// - Too many nodes in graph
    pub fn register_tag(
        &self,
        table_id: u32,
        tag_name: &str,
        expression: &str,
        fallback_policy: FallbackPolicy,
        deadband: DeadbandConfig,
    ) -> Result<(), DerivedError> {
        let cfg = self.get_config();

        // D4/D6：派生标签归属由其目标物理表 tenant 定义（与请求上下文解耦）。
        // mirror miss 属异常路径（建表后必有），回退 DEFAULT + warn 便于观测。
        let tenant_id = self.schema.table_tenant(table_id).unwrap_or_else(|| {
            tracing::warn!(
                table_id,
                "derived tag target table not in mirror; fallback DEFAULT tenant"
            );
            TenantId::DEFAULT
        });

        // Lex
        let mut lexer = Lexer::new(expression);
        let tokens = lexer.tokenize()?;

        // Parse
        let mut parser = PrattParser::with_max_depth(tokens, cfg.max_ast_depth);
        let ast = parser.parse_expression()?;

        // Validate source tags: every referenced tag must resolve to either
        // a physical schema tag or an already-registered derived tag.
        //
        // Topology constraint (derived-on-derived): a derived tag referenced by
        // another derived tag MUST be registered first. `register_tag` call
        // order must therefore respect dependency order (sources before
        // dependents); otherwise registration fails with `UnknownSourceTag`.
        let source_tags = ast.extract_source_tags();

        // Pre-resolve each source tag name to a column reference (cross-table,
        // column-level). `` `table.col` `` → `(table_id, column_id)`; a plain
        // `` `tag` `` → `(table_id, column_id = 0)` (N=1 single-column case).
        // - Catalog/mirror lookup first (covers raw and derived tables, and
        //   wide-table column resolution).
        // - In-memory dependency graph as fallback for derived-on-derived
        //   references whose derived table has not yet been synced to the mirror.
        let mut source_table_ids: Vec<(String, ColumnRef)> = Vec::with_capacity(source_tags.len());
        for source_tag in &source_tags {
            let (table_name, column_name) = Self::split_source_ref(source_tag);
            let resolved = self.resolve_source(tenant_id, &table_name, column_name.as_deref());
            match resolved {
                Some(col_ref) => source_table_ids.push((source_tag.clone(), col_ref)),
                None => {
                    return Err(DerivedError::UnknownSourceTag {
                        tag_name: source_tag.clone(),
                    });
                }
            }
        }

        // Compute expression hash (FNV-1a)
        let expression_hash = Self::fnv1a_hash(expression);

        // Create node
        let node = DerivedTagNode::new(
            table_id,
            tag_name.to_string(),
            ast,
            expression_hash,
            fallback_policy,
            deadband,
            source_table_ids,
        );

        // 校验前移（D6 修正）：把 add_node 内「可能失败」的图校验
        // （自引用环 / 传递环 / max_nodes 上限）以 dry-run 方式提前到 persist
        // 之前。任何校验失败都发生在 upsert 之前，库不会被污染；通过后
        // upsert → add_node（add_node 内部会再跑一次同款校验，兜住校验与
        // 写内存之间的极小 TOCTOU 窗口，此时库内定义已是通过校验的合法定义）。
        self.dep_graph
            .read()
            .validate_node(tenant_id, &node, cfg.max_graph_nodes)?;

        // Persist-first (D6)：先落库成功，再更新内存。upsert 失败返回错误且内存不变。
        //
        // TOCTOU 收口：在 upsert 之前先读旧定义，记录「重注册 vs 新注册」。
        // 这样即使 `add_node` 在拿写锁后因并发插入成环/超限而失败，也能据此把
        // 已写库的定义回滚到与内存（未变）一致的旧状态，避免「库新内存旧」。
        let mut old_record: Option<DerivedTagRecord> = None;
        if let Some(store) = self.store.as_ref() {
            old_record =
                store
                    .get(tenant_id, tag_name)
                    .map_err(|e| DerivedError::PersistenceError {
                        message: format!("{e}"),
                    })?;

            let source_refs: Vec<SourceRefDef> = node
                .source_table_ids
                .iter()
                .map(|(name, col_ref)| SourceRefDef {
                    name: name.clone(),
                    table_id: col_ref.table_id,
                    column_id: col_ref.column_id,
                })
                .collect();
            let now = now_millis();
            let record = DerivedTagRecord::new(
                tenant_id,
                node.name.clone(),
                node.table_id,
                expression.to_string(),
                node.fallback_policy.clone(),
                node.deadband.clone(),
                source_refs,
                now,
                now,
            );
            store
                .upsert(&record)
                .map_err(|e| DerivedError::PersistenceError {
                    message: format!("{e}"),
                })?;
        }

        // Add to graph (validates cycles and max_nodes). `add_node` 内部会再跑一次
        // 同款校验，兜住「校验前移」与写内存之间的极小并发窗口；一旦在此失败，
        // 说明库已写但内存未变，需要回滚库。
        if let Err(add_err) = self
            .dep_graph
            .write()
            .add_node(tenant_id, node, cfg.max_graph_nodes)
        {
            self.rollback_persist(tenant_id, tag_name, old_record.as_ref());
            return Err(add_err);
        }

        self.has_derived_tags.store(true, Ordering::Relaxed);

        Ok(())
    }

    /// 回滚 `add_node` 失败后已写入库的定义，使库与内存（未变）保持一致。
    ///
    /// - `old` 存在（重注册）→ `upsert(old)` 恢复旧定义；
    /// - `old` 不存在（新注册）→ `delete(tenant_id, tag_name)` 删掉刚插入的行；
    /// - 回滚自身失败 → `warn` 兜底（嵌套失败概率更低，不 panic）。
    fn rollback_persist(
        &self,
        tenant_id: TenantId,
        tag_name: &str,
        old: Option<&DerivedTagRecord>,
    ) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        let rollback = match old {
            Some(old_record) => store.upsert(old_record),
            None => store.delete(tenant_id, tag_name),
        };
        if let Err(e) = rollback {
            tracing::warn!(
                tenant_id = tenant_id.as_u32(),
                tag_name = %tag_name,
                error = %e,
                "derived register rollback failed: store may be inconsistent with memory"
            );
        }
    }

    /// Unregister a derived tag（租户作用域）。
    ///
    /// Delete-first (D6)：先删库成功，再移除内存节点；delete 失败返回错误且内存不变。
    ///
    /// Returns the names of tags that were cascade-disabled.
    pub fn unregister_tag(
        &self,
        tenant_id: TenantId,
        tag_name: &str,
    ) -> Result<Vec<String>, DerivedError> {
        if let Some(store) = self.store.as_ref() {
            store
                .delete(tenant_id, tag_name)
                .map_err(|e| DerivedError::PersistenceError {
                    message: format!("{e}"),
                })?;
        }
        let removed = self.dep_graph.write().remove_node(tenant_id, tag_name);
        if self.dep_graph.read().is_empty() {
            self.has_derived_tags.store(false, Ordering::Relaxed);
        }
        Ok(removed)
    }

    /// 从 config.db 恢复全部派生标签定义到内存依赖图（启动恢复，D8/D9）。
    ///
    /// 按 `created_at ASC` 顺序逐条恢复（store 内部排序保证「源先于依赖」拓扑序）：
    /// 1. 重解析 `expression` → `ast`（失败 skip + warn）。
    /// 2. 逐源重解析引用（schema mirror 优先、内存图兜底）；失败回退持久化
    ///    `source_table_ids` 中同名项；仍无 → skip + warn。
    /// 3. 重算 `expression_hash`，重建 `DerivedTagNode` 并 `add_node`
    ///    （循环/上限校验失败 → skip + warn）。
    ///
    /// 求值态（debounce / in_flight / last_good_cache / state / last_source_max_ts）
    /// 不持久化，重建节点时自然复位。
    ///
    /// 返回成功恢复条数；坏行 skip 不阻断启动。
    pub fn restore_definitions(&self) -> Result<usize, DerivedError> {
        let Some(store) = self.store.as_ref() else {
            return Ok(0);
        };

        let records = store.load_all().map_err(|e| DerivedError::RestoreError {
            message: format!("{e}"),
        })?;
        let cfg = self.get_config();
        let mut restored = 0usize;

        for record in records {
            let DerivedTagRecord {
                tenant_id,
                tag_name,
                table_id,
                expression,
                fallback_policy,
                deadband,
                source_table_ids: persisted_source_refs,
                ..
            } = record;

            // 1. 重解析 expression → ast。
            let ast = match Self::validate_expression(&expression, cfg.max_ast_depth) {
                Ok((ast, _)) => ast,
                Err(e) => {
                    tracing::warn!(
                        tenant_id = tenant_id.as_u32(),
                        tag_name = %tag_name,
                        error = %e,
                        "derived restore: expression parse failed, skipping"
                    );
                    continue;
                }
            };
            let source_tags = ast.extract_source_tags();

            // 2. 逐源重解析；失败回退持久化 source_table_ids 同名项；仍无 → skip。
            let mut source_table_ids: Vec<(String, ColumnRef)> =
                Vec::with_capacity(source_tags.len());
            let mut resolved_all = true;
            for source_tag in &source_tags {
                let (table_name, column_name) = Self::split_source_ref(source_tag);
                match self.resolve_source(tenant_id, &table_name, column_name.as_deref()) {
                    Some(col_ref) => source_table_ids.push((source_tag.clone(), col_ref)),
                    None => match persisted_source_refs.iter().find(|s| s.name == *source_tag) {
                        Some(sref) => source_table_ids.push((
                            source_tag.clone(),
                            ColumnRef::new(sref.table_id, sref.column_id),
                        )),
                        None => {
                            tracing::warn!(
                                tenant_id = tenant_id.as_u32(),
                                tag_name = %tag_name,
                                source_tag = %source_tag,
                                "derived restore: source unresolvable, skipping"
                            );
                            resolved_all = false;
                            break;
                        }
                    },
                }
            }
            if !resolved_all {
                continue;
            }

            // 3. 重算 expression hash + 重建节点。
            let expression_hash = Self::fnv1a_hash(&expression);
            let node = DerivedTagNode::new(
                table_id,
                tag_name.clone(),
                ast,
                expression_hash,
                fallback_policy,
                deadband,
                source_table_ids,
            );

            // 4. 加入依赖图（循环/上限校验，失败 skip + warn）。
            if let Err(e) = self
                .dep_graph
                .write()
                .add_node(tenant_id, node, cfg.max_graph_nodes)
            {
                tracing::warn!(
                    tenant_id = tenant_id.as_u32(),
                    tag_name = %tag_name,
                    error = %e,
                    "derived restore: add_node failed, skipping"
                );
                continue;
            }
            restored += 1;
        }

        if restored > 0 {
            self.has_derived_tags.store(true, Ordering::Relaxed);
            tracing::info!(
                "derived tag definitions restored from config.db: {restored} definition(s)"
            );
        }
        Ok(restored)
    }

    /// Validate a derived tag expression without registering it.
    ///
    /// Returns the parsed AST and the set of source tags if valid.
    pub fn validate_expression(
        expression: &str,
        max_depth: usize,
    ) -> Result<(ExprAst, Vec<String>), DerivedError> {
        let mut lexer = Lexer::new(expression);
        let tokens = lexer.tokenize()?;
        let mut parser = PrattParser::with_max_depth(tokens, max_depth);
        let ast = parser.parse_expression()?;
        let source_tags = ast.extract_source_tags();
        Ok((ast, source_tags))
    }

    // ─── Source Change Handler (Main Entry Point) ───

    /// Handle a source tag change notification.
    ///
    /// This is the primary callback from the snapshot layer. It:
    /// 1. Generates a new session ID (recursion guard layer 1).
    /// 2. Deduplicates by batch_id.
    /// 3. Finds all transitively affected derived tags.
    /// 4. Marks them dirty (debounce).
    /// 5. Triggers recalculation.
    ///
    /// # Arguments
    /// * `source_keys` - List of `(tenant_id, table_id, table_name)` that changed.
    /// * `batch_id` - Write batch ID for deduplication.
    /// * `timestamp_ns` - Timestamp of the change (epoch nanoseconds).
    ///
    /// `timestamp_ns` 语义 = 批内最大源变化 ts（调用方对整批取 `max(ts)` 传入）；
    /// 本方法接受多源切片，批级化后同一批的多个 source 一次性传并集（去重后）。
    pub fn on_source_changed(
        &self,
        source_keys: &[(TenantId, u32, String)],
        batch_id: u64,
        timestamp_ns: i64,
    ) {
        let Some(eval_order) = self.collect_affected(source_keys, batch_id) else {
            return;
        };

        // Mark dirty for debounce
        {
            let mut debounce = self.debounce.write();
            debounce.mark_dirty(&eval_order);
        }

        // 同步重算（保留原语义：调用返回后结果已写入）。
        self.recalculate(&eval_order, timestamp_ns, 0);
    }

    /// 写路径入口（P4）：只标脏，不内联重算。
    ///
    /// 重算移到独立去抖 flusher（[`Self::flush_ready`]）异步执行。与
    /// [`Self::on_source_changed`]（同步）的区别：本方法不调用 `recalculate`，
    /// 只 `mark_dirty_at` 记录源变化 ts 并唤醒 flusher，彻底消除「写路径内联
    /// 读 snapshot」的时序耦合（改由 flusher 在 snapshot flush 之后重算）。
    pub fn mark_dirty(
        &self,
        source_keys: &[(TenantId, u32, String)],
        batch_id: u64,
        timestamp_ns: i64,
    ) {
        let Some(eval_order) = self.collect_affected(source_keys, batch_id) else {
            return;
        };

        {
            let mut debounce = self.debounce.write();
            debounce.mark_dirty_at(&eval_order, timestamp_ns);
        }

        // 唤醒去抖 flusher（异步重算）。
        self.flush_notify.notify_one();
    }

    /// 公共入口前置：生成新 session + batch_id 去重 + 找传递闭包受影响 tag + 拓扑排序。
    ///
    /// 返回 `None` 表示本批无需处理（引擎关闭 / 已处理 / 无受影响 tag）。
    fn collect_affected(
        &self,
        source_keys: &[(TenantId, u32, String)],
        batch_id: u64,
    ) -> Option<Vec<TagKey>> {
        let cfg = self.get_config();
        if !cfg.enabled {
            return None;
        }

        // Layer 1: session_id recursion guard
        // Generate a new session ID; any nested calls within this session
        // will be detected because the session_id is updated atomically.
        let new_session = Uuid::new_v4();
        {
            let mut sid = self.session_id.write();
            *sid = new_session;
        }

        // batch_id dedup
        {
            let mut pb = self.processed_batches.write();
            if !pb.mark_processed(batch_id) {
                // Already processed in this session
                return None;
            }
        }

        // Extract tenant-scoped source keys (tenant, source_tag_name).
        let source_tag_keys: Vec<TagKey> = source_keys
            .iter()
            .map(|(tenant, _, name)| (*tenant, name.clone()))
            .collect();

        // Find all affected derived tags (transitive closure)
        let affected: HashSet<TagKey> = self
            .dep_graph
            .read()
            .get_transitive_derived_tags(&source_tag_keys);

        if affected.is_empty() {
            return None;
        }

        // Sort affected tags topologically
        Some(self.dep_graph.read().topological_subset(&affected))
    }

    /// 去抖 flusher 消费入口：取就绪 tag（带源 ts）→ 拓扑排序 → 逐 tag 重算。
    ///
    /// 由 [`crate::flusher::DerivedFlusher`] 定时/notify 触发。时序编排保证调用
    /// 本方法前 snapshot 已 flush（flusher 的 `pre_flush` hook）。
    pub fn flush_ready(&self) {
        let cfg = self.get_config();
        if !cfg.enabled {
            return;
        }

        let ready = {
            let mut debounce = self.debounce.write();
            debounce.take_ready_tags_with_ts()
        };
        if ready.is_empty() {
            return;
        }

        // dirty 集合无序，重排拓扑序以满足 derived-on-derived 依赖顺序。
        let ready_set: HashSet<TagKey> = ready.iter().map(|(k, _)| k.clone()).collect();
        let eval_order = self.dep_graph.read().topological_subset(&ready_set);

        // (tag → 源变化 ts) 映射，逐 tag 用各自源 ts 作为结果时间戳。
        let ts_map: HashMap<TagKey, i64> = ready.into_iter().collect();

        // 单次 tick 重算数受 `max_recalc_per_tick` 约束（与同步路径 `recalculate`
        // 的 `take(limit)` 语义一致）；超限的 tag 重新标脏（保留源 ts），下次 tick 再算，
        // 不因 `take_ready_tags_with_ts` 已清 dirty 而丢失。
        let limit = cfg.max_recalc_per_tick;
        let mut processed = 0usize;
        for tag in eval_order {
            let ts = ts_map.get(&tag).copied().unwrap_or(0);
            if processed >= limit {
                self.debounce
                    .write()
                    .mark_dirty_at(std::slice::from_ref(&tag), ts);
                continue;
            }
            self.recalculate(std::slice::from_ref(&tag), ts, 0);
            processed += 1;
        }
    }

    /// 返回 flusher 可等待的通知 future（写路径 `mark_dirty` 会 notify）。
    pub fn flush_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.flush_notify.notified()
    }

    /// 是否有待重算的 dirty tag（测试 / 可观测性用）。
    pub fn has_dirty(&self) -> bool {
        self.debounce.read().has_dirty()
    }

    // ─── Recalculation ───

    /// Recalculate a list of derived tags in topological order.
    ///
    /// # Arguments
    /// * `tags` - Ordered list of tag names to recalculate.
    /// * `timestamp_ns` - Timestamp for the results.
    /// * `call_depth` - Current call depth (layer 2 recursion guard).
    fn recalculate(&self, tags: &[TagKey], timestamp_ns: i64, call_depth: u32) {
        let cfg = self.get_config();
        if call_depth > cfg.max_call_depth {
            tracing::warn!(
                "DerivedTagEngine: max call depth ({}) exceeded, aborting recalculation",
                cfg.max_call_depth
            );
            return;
        }

        let recalc_limit = cfg.max_recalc_per_tick.min(tags.len());
        let now_epoch_secs = timestamp_ns / 1_000_000_000;

        for tag_key in tags.iter().take(recalc_limit) {
            // Debounce check
            {
                let debounce = self.debounce.read();
                if debounce.is_in_cooldown(tag_key) {
                    continue;
                }
            }

            // Layer 2: call_depth guard — check if already in-flight
            {
                let in_flight = self.in_flight.read();
                if in_flight.contains_key(tag_key) {
                    continue; // Already being calculated up the call stack
                }
            }

            // Mark in-flight
            {
                let mut in_flight = self.in_flight.write();
                in_flight.insert(tag_key.clone(), call_depth);
            }

            // Evaluate and write
            match self.evaluate_and_write(tag_key, timestamp_ns, now_epoch_secs) {
                Ok(_) => {
                    // Record successful calculation
                    let mut debounce = self.debounce.write();
                    debounce.record_calculation(tag_key);
                }
                Err(e) => {
                    tracing::error!(
                        "DerivedTagEngine: failed to calculate '{}': {}",
                        tag_key.1,
                        e
                    );
                    self.dep_graph.write().set_state(
                        tag_key.0,
                        &tag_key.1,
                        NodeState::Error(format!("{}", e)),
                    );
                }
            }

            // Remove from in-flight
            {
                let mut in_flight = self.in_flight.write();
                in_flight.remove(tag_key);
            }
        }
    }

    /// Evaluate a single derived tag and write the result.
    fn evaluate_and_write(
        &self,
        tag_key: &TagKey,
        timestamp_ns: i64,
        now_epoch_secs: i64,
    ) -> Result<(), DerivedError> {
        let tenant_id = tag_key.0;
        let tag_name = &tag_key.1;

        // Get the node from the graph
        let node = match self.dep_graph.read().get(tenant_id, tag_name) {
            Some(n) => n.clone(),
            None => {
                return Err(DerivedError::TagNotFound {
                    tag_name: tag_name.to_string(),
                });
            }
        };

        // Gather source values by their resolved (cross-table) column references.
        let columns: Vec<ColumnRef> = node.source_table_ids.iter().map(|(_, c)| *c).collect();
        let source_values = self.snapshot.batch_get_columns(&columns);

        // Build evaluation context
        let mut ctx = EvalContext::new();
        for (source_tag, col_ref) in &node.source_table_ids {
            match source_values.get(col_ref) {
                Some((value, quality, ts)) => {
                    ctx.set(source_tag, *value, *quality, *ts);

                    // Update last-good cache
                    if Quality(*quality).is_good() {
                        self.last_good_cache.write().update(
                            tenant_id,
                            source_tag,
                            *value,
                            now_epoch_secs,
                        );
                    }
                }
                None => {
                    // Source not found; try fallback
                    match &node.fallback_policy {
                        FallbackPolicy::Strict => {
                            return Err(DerivedError::TagNotFound {
                                tag_name: source_tag.clone(),
                            });
                        }
                        FallbackPolicy::Lenient {
                            last_good_max_age_secs,
                        } => {
                            let cached = self.last_good_cache.read().get_if_fresh(
                                tenant_id,
                                source_tag,
                                now_epoch_secs,
                                *last_good_max_age_secs,
                            );
                            match cached {
                                Some(value) => {
                                    ctx.set(source_tag, value, Quality::UNCERTAIN.0, 0);
                                }
                                None => {
                                    ctx.set(source_tag, f64::NAN, Quality::BAD.0, 0);
                                }
                            }
                        }
                        FallbackPolicy::Coalesce { defaults } => {
                            let default = defaults.get(source_tag).copied().unwrap_or(f64::NAN);
                            ctx.set(
                                source_tag,
                                default,
                                if default.is_nan() {
                                    Quality::BAD.0
                                } else {
                                    Quality::UNCERTAIN.0
                                },
                                0,
                            );
                        }
                    }
                }
            }
        }

        // Evaluate the AST
        let result = node.ast.evaluate(&ctx)?;

        // Deadband check
        // For deadband we need previous value; we query the snapshot for the derived tag itself
        let prev_col = ColumnRef::single(node.table_id);
        let prev_values = self.snapshot.batch_get_columns(&[prev_col]);
        let (prev_value, prev_ts) = if let Some((val, _, ts)) = prev_values.get(&prev_col) {
            (*val, *ts / 1_000_000_000) // convert ns → seconds
        } else {
            (0.0, 0i64)
        };

        let deadband_result = check_deadband(
            result.value,
            prev_value,
            prev_ts,
            now_epoch_secs,
            &node.deadband,
        );

        if deadband_result == DeadbandResult::Suppress {
            // Update state only
            self.dep_graph
                .write()
                .set_state(tenant_id, tag_name, NodeState::Active);
            return Ok(());
        }

        // Quality is already propagated by the evaluator.
        // If any source tag is Bad, result.quality will be 0x00.
        // If all are Good, it will be 0xC0.
        let final_quality = result.quality;

        // Write result through standard write path
        let entries = vec![(
            node.table_id,
            tag_name.to_string(),
            result.value,
            final_quality,
            timestamp_ns,
        )];
        self.writer.write_batch(&entries);

        // Update state
        self.dep_graph
            .write()
            .set_state(tenant_id, tag_name, NodeState::Active);
        self.dep_graph
            .write()
            .set_last_source_max_ts(tenant_id, tag_name, timestamp_ns);

        // Update last-good cache for this derived tag (so downstream dependents can use it)
        if Quality(final_quality).is_good() {
            self.last_good_cache
                .write()
                .update(tenant_id, tag_name, result.value, now_epoch_secs);
        }

        Ok(())
    }

    // ─── Utility ───

    /// Split a source tag reference into `(table_name, optional column_name)`.
    ///
    /// Column names never contain `.` (schema validator forbids it) while table
    /// names may (path-style, e.g. `planta.line1.tt101`). Split on the **last**
    /// dot: everything before the last dot is the table name, the trailing
    /// segment is the column name. A reference without a dot is the N=1
    /// single-column case (`column_name == None`).
    fn split_source_ref(source_tag: &str) -> (String, Option<String>) {
        match source_tag.rfind('.') {
            Some(idx) => (
                source_tag[..idx].to_string(),
                Some(source_tag[idx + 1..].to_string()),
            ),
            None => (source_tag.to_string(), None),
        }
    }

    /// Resolve a source reference to a [`ColumnRef`].
    ///
    /// Schema/mirror lookup first; in-memory dependency graph fallback for
    /// derived-on-derived references (derived tags are independent
    /// single-column tables, so the fallback only applies to the plain-name
    /// N=1 case — a `.col` reference cannot target a derived tag).
    fn resolve_source(
        &self,
        tenant_id: TenantId,
        table_name: &str,
        column_name: Option<&str>,
    ) -> Option<ColumnRef> {
        if let Some(col_ref) = self
            .schema
            .resolve_column_ref(tenant_id, table_name, column_name)
        {
            return Some(col_ref);
        }
        if column_name.is_none() {
            if let Some(node) = self.dep_graph.read().get(tenant_id, table_name) {
                return Some(ColumnRef::single(node.table_id));
            }
        }
        None
    }

    /// Compute FNV-1a 64-bit hash of a string.
    fn fnv1a_hash(s: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in s.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

/// 当前 epoch 毫秒时间戳（与 store 的 `created_at`/`updated_at` 单位一致）。
fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deadband::DeadbandConfig;
    use crate::fallback::FallbackPolicy;
    use sensearcdb_common::{AppConfig, ConfigHandle, Quality};

    /// Mock [`SnapshotRead`] backed by an in-memory `column → value` map.
    struct MockSnapshot {
        values: HashMap<ColumnRef, (f64, u8, i64)>,
    }

    impl SnapshotRead for MockSnapshot {
        fn batch_get_columns(&self, columns: &[ColumnRef]) -> HashMap<ColumnRef, (f64, u8, i64)> {
            columns
                .iter()
                .filter_map(|c| self.values.get(c).map(|v| (*c, *v)))
                .collect()
        }
    }

    /// Mock [`WriteSink`] capturing written entries for assertions.
    struct MockWriter {
        entries: parking_lot::Mutex<Vec<(u32, String, f64, u8, i64)>>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                entries: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn written(&self) -> Vec<(u32, String, f64, u8, i64)> {
            self.entries.lock().clone()
        }
    }

    impl WriteSink for MockWriter {
        fn write_batch(&self, entries: &[(u32, String, f64, u8, i64)]) {
            self.entries.lock().extend_from_slice(entries);
        }
    }

    /// Mock [`SchemaAccess`] resolving table/column names to column references.
    struct MockSchema {
        tables: HashMap<String, u32>,
        columns: HashMap<(String, String), u16>,
    }

    impl SchemaAccess for MockSchema {
        fn table_tenant(&self, _table_id: u32) -> Option<TenantId> {
            Some(TenantId::DEFAULT)
        }

        fn resolve_column_ref(
            &self,
            _tenant_id: TenantId,
            table_name: &str,
            column_name: Option<&str>,
        ) -> Option<ColumnRef> {
            let table_id = *self.tables.get(table_name)?;
            match column_name {
                None => Some(ColumnRef::single(table_id)),
                Some(col) => self
                    .columns
                    .get(&(table_name.to_string(), col.to_string()))
                    .map(|&cid| ColumnRef::new(table_id, cid)),
            }
        }
    }

    /// Build an engine with full control over schema mapping and snapshot values.
    ///
    /// Returns the engine plus the writer handle for inspecting written results.
    fn make_engine_with(
        schema_map: HashMap<String, u32>,
        snapshot_values: HashMap<u32, (f64, u8, i64)>,
    ) -> (DerivedTagEngine, Arc<MockWriter>) {
        let values: HashMap<ColumnRef, (f64, u8, i64)> = snapshot_values
            .into_iter()
            .map(|(table_id, v)| (ColumnRef::single(table_id), v))
            .collect();
        let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot { values });
        let writer = Arc::new(MockWriter::new());
        let writer_handle: Arc<dyn WriteSink> = writer.clone();
        let schema: Arc<dyn SchemaAccess> = Arc::new(MockSchema {
            tables: schema_map,
            columns: HashMap::new(),
        });
        let config = ConfigHandle::new(AppConfig::default());
        let engine = DerivedTagEngine::new(snapshot, writer_handle, schema, config, None);
        (engine, writer)
    }

    /// Build an engine whose physical tags are mapped to sequential table IDs.
    fn make_engine(physical_tags: &[&str]) -> DerivedTagEngine {
        let schema_map = physical_tags
            .iter()
            .enumerate()
            .map(|(i, s)| (s.to_string(), (i as u32) + 1))
            .collect();
        make_engine_with(schema_map, HashMap::new()).0
    }

    #[test]
    fn test_register_tag_unknown_physical_tag() {
        let engine = make_engine(&["sensor_a"]);
        let result = engine.register_tag(
            42,
            "derived_1",
            "sensor_a + missing_tag",
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
        );
        match result {
            Err(DerivedError::UnknownSourceTag { tag_name }) => {
                assert_eq!(tag_name, "missing_tag");
            }
            other => panic!("expected UnknownSourceTag, got {:?}", other),
        }
    }

    #[test]
    fn test_register_tag_real_physical_tag() {
        let engine = make_engine(&["sensor_a", "sensor_b"]);
        let result = engine.register_tag(
            42,
            "derived_1",
            "sensor_a + sensor_b",
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
        );
        assert!(result.is_ok(), "expected Ok, got {:?}", result);
    }

    #[test]
    fn test_register_tag_derived_on_derived() {
        // The referenced derived tag must be registered first (topology order).
        let engine = make_engine(&["sensor_a"]);

        engine
            .register_tag(
                42,
                "derived_base",
                "sensor_a * 2",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        let result = engine.register_tag(
            42,
            "derived_child",
            "derived_base + 1",
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
        );
        assert!(result.is_ok(), "expected Ok, got {:?}", result);
    }

    #[test]
    fn test_register_tag_derived_missing_dependency_order() {
        // Referencing a derived tag that has NOT been registered yet must fail,
        // because it is neither a physical tag nor an existing derived tag.
        let engine = make_engine(&["sensor_a"]);
        let result = engine.register_tag(
            42,
            "derived_child",
            "derived_base + 1",
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
        );
        match result {
            Err(DerivedError::UnknownSourceTag { tag_name }) => {
                assert_eq!(tag_name, "derived_base");
            }
            other => panic!("expected UnknownSourceTag, got {:?}", other),
        }
    }

    #[test]
    fn test_cross_table_source_evaluation() {
        // `efficiency = output_power / input_power * 100`, where the two source
        // tags live in different tables (101 and 202) than the derived tag (42).
        // Before the fix, both sources were pinned to table_id 42 and evaluation
        // would read nothing. After the fix, each source reads its own table.
        let mut schema_map = HashMap::new();
        schema_map.insert("output_power".to_string(), 101);
        schema_map.insert("input_power".to_string(), 202);

        let mut snapshot_values = HashMap::new();
        snapshot_values.insert(101, (100.0, Quality::GOOD.0, 1_000_000_000));
        snapshot_values.insert(202, (50.0, Quality::GOOD.0, 1_000_000_000));

        let (engine, writer) = make_engine_with(schema_map, snapshot_values);

        engine
            .register_tag(
                42,
                "efficiency",
                "output_power / input_power * 100",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        let ts_ns = 2_000_000_000i64;
        engine
            .evaluate_and_write(
                &(TenantId::DEFAULT, "efficiency".to_string()),
                ts_ns,
                ts_ns / 1_000_000_000,
            )
            .unwrap();

        let written = writer.written();
        assert_eq!(written.len(), 1);
        let (table_id, tag_name, value, quality, ts) = written[0].clone();
        assert_eq!(table_id, 42);
        assert_eq!(tag_name, "efficiency");
        assert!(
            (value - 200.0).abs() < 1e-9,
            "expected 200.0, got {}",
            value
        );
        assert_eq!(quality, Quality::GOOD.0);
        assert_eq!(ts, ts_ns);
    }

    #[test]
    fn test_derived_on_derived_resolves_via_schema_first() {
        // The derived source tag's table is already in the mirror → schema lookup wins.
        let mut schema_map = HashMap::new();
        schema_map.insert("sensor_a".to_string(), 7);
        schema_map.insert("derived_base".to_string(), 99);

        let (engine, _writer) = make_engine_with(schema_map, HashMap::new());

        engine
            .register_tag(
                42,
                "derived_base",
                "sensor_a * 2",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        engine
            .register_tag(
                43,
                "derived_child",
                "derived_base + 1",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        let graph = engine.dep_graph().read();
        let child = graph.get(TenantId::DEFAULT, "derived_child").unwrap();
        assert_eq!(
            child.source_table_ids,
            vec![("derived_base".to_string(), ColumnRef::single(99))]
        );
    }

    #[test]
    fn test_derived_on_derived_resolves_via_dep_graph_fallback() {
        // "derived_base" is NOT in the schema map (simulating a mirror sync gap),
        // so the child must resolve it via the in-memory dependency graph.
        let mut schema_map = HashMap::new();
        schema_map.insert("sensor_a".to_string(), 7);

        let (engine, _writer) = make_engine_with(schema_map, HashMap::new());

        engine
            .register_tag(
                42,
                "derived_base",
                "sensor_a * 2",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        engine
            .register_tag(
                43,
                "derived_child",
                "derived_base + 1",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        let graph = engine.dep_graph().read();
        let child = graph.get(TenantId::DEFAULT, "derived_child").unwrap();
        assert_eq!(
            child.source_table_ids,
            vec![("derived_base".to_string(), ColumnRef::single(42))]
        );
    }

    #[test]
    fn test_split_source_ref_last_dot() {
        assert_eq!(
            DerivedTagEngine::split_source_ref("sensor_a"),
            ("sensor_a".to_string(), None)
        );
        assert_eq!(
            DerivedTagEngine::split_source_ref("device_01.col_0"),
            ("device_01".to_string(), Some("col_0".to_string()))
        );
        // 路径式表名（含点号）+ 列名 → 按最后一个点号拆分。
        assert_eq!(
            DerivedTagEngine::split_source_ref("planta.line1.tt101.temperature"),
            (
                "planta.line1.tt101".to_string(),
                Some("temperature".to_string())
            )
        );
    }

    #[test]
    fn test_wide_table_column_reference() {
        // 宽表列级：`` `device_01.temp` `` 与 `` `device_02.pressure` `` 分别指向
        // 不同表的非零列，重算必须按 (table_id, column_id) 读源。
        let mut tables = HashMap::new();
        tables.insert("device_01".to_string(), 100u32);
        tables.insert("device_02".to_string(), 200u32);
        let mut columns = HashMap::new();
        columns.insert(("device_01".to_string(), "temp".to_string()), 1u16);
        columns.insert(("device_02".to_string(), "pressure".to_string()), 2u16);

        let mut snapshot_values = HashMap::new();
        snapshot_values.insert(
            ColumnRef::new(100, 1),
            (30.0, Quality::GOOD.0, 1_000_000_000),
        );
        snapshot_values.insert(
            ColumnRef::new(200, 2),
            (70.0, Quality::GOOD.0, 1_000_000_000),
        );

        let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot {
            values: snapshot_values,
        });
        let writer = Arc::new(MockWriter::new());
        let writer_handle: Arc<dyn WriteSink> = writer.clone();
        let schema: Arc<dyn SchemaAccess> = Arc::new(MockSchema { tables, columns });
        let config = ConfigHandle::new(AppConfig::default());
        let engine = DerivedTagEngine::new(snapshot, writer_handle, schema, config, None);

        engine
            .register_tag(
                42,
                "sum_wide",
                "`device_01.temp` + `device_02.pressure`",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        {
            let graph = engine.dep_graph().read();
            let node = graph.get(TenantId::DEFAULT, "sum_wide").unwrap();
            assert_eq!(
                node.source_table_ids,
                vec![
                    ("device_01.temp".to_string(), ColumnRef::new(100, 1)),
                    ("device_02.pressure".to_string(), ColumnRef::new(200, 2)),
                ]
            );
        } // 释放读锁，避免 evaluate_and_write 内部写锁死锁

        let ts_ns = 2_000_000_000i64;
        engine
            .evaluate_and_write(
                &(TenantId::DEFAULT, "sum_wide".to_string()),
                ts_ns,
                ts_ns / 1_000_000_000,
            )
            .unwrap();

        let written = writer.written();
        assert_eq!(written.len(), 1);
        assert!(
            (written[0].2 - 100.0).abs() < 1e-9,
            "expected 100.0, got {}",
            written[0].2
        );
    }

    #[test]
    fn test_wide_table_unknown_column_errors() {
        // 列不存在 → 诚实报错，不降级伪造。
        let mut tables = HashMap::new();
        tables.insert("device_01".to_string(), 100u32);
        let columns: HashMap<(String, String), u16> = HashMap::new();

        let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot {
            values: HashMap::new(),
        });
        let writer = Arc::new(MockWriter::new());
        let writer_handle: Arc<dyn WriteSink> = writer.clone();
        let schema: Arc<dyn SchemaAccess> = Arc::new(MockSchema { tables, columns });
        let config = ConfigHandle::new(AppConfig::default());
        let engine = DerivedTagEngine::new(snapshot, writer_handle, schema, config, None);

        let result = engine.register_tag(
            42,
            "bad_wide",
            "`device_01.missing_col`",
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
        );
        match result {
            Err(DerivedError::UnknownSourceTag { tag_name }) => {
                assert_eq!(tag_name, "device_01.missing_col");
            }
            other => panic!("expected UnknownSourceTag, got {:?}", other),
        }
    }

    /// 跨租户同名派生标签不冲突、结果不串（A3 D4 + D1 写路径隔离）。
    #[test]
    fn test_cross_tenant_derived_name_isolation() {
        struct TenantSchema {
            tenants: HashMap<u32, TenantId>,
            tables: HashMap<String, u32>,
        }
        impl SchemaAccess for TenantSchema {
            fn table_tenant(&self, table_id: u32) -> Option<TenantId> {
                self.tenants.get(&table_id).copied()
            }
            fn resolve_column_ref(
                &self,
                _tenant_id: TenantId,
                table_name: &str,
                column_name: Option<&str>,
            ) -> Option<ColumnRef> {
                let table_id = *self.tables.get(table_name)?;
                match column_name {
                    None => Some(ColumnRef::single(table_id)),
                    Some(_) => None,
                }
            }
        }

        let mut tenants = HashMap::new();
        tenants.insert(7u32, TenantId(1));
        tenants.insert(8u32, TenantId(2));
        tenants.insert(100u32, TenantId(1));
        tenants.insert(200u32, TenantId(2));
        let mut tables = HashMap::new();
        tables.insert("sensor_a".to_string(), 7u32);
        tables.insert("sensor_b".to_string(), 8u32);

        let mut values = HashMap::new();
        values.insert(ColumnRef::single(7), (10.0, Quality::GOOD.0, 1_000_000_000));
        values.insert(ColumnRef::single(8), (20.0, Quality::GOOD.0, 1_000_000_000));

        let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot { values });
        let writer = Arc::new(MockWriter::new());
        let writer_handle: Arc<dyn WriteSink> = writer.clone();
        let schema: Arc<dyn SchemaAccess> = Arc::new(TenantSchema { tenants, tables });
        let config = ConfigHandle::new(AppConfig::default());
        let engine = DerivedTagEngine::new(snapshot, writer_handle, schema, config, None);

        // 租户 1 与租户 2 各自注册同名 "dup"，互不冲突。
        engine
            .register_tag(
                100,
                "dup",
                "sensor_a",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();
        engine
            .register_tag(
                200,
                "dup",
                "sensor_b",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();
        assert_eq!(engine.dep_graph().read().len(), 2);

        // 各自求值：结果落各自目标表（100 / 200），值取自各自源（10 / 20）。
        let ts_ns = 2_000_000_000i64;
        engine
            .evaluate_and_write(
                &(TenantId(1), "dup".to_string()),
                ts_ns,
                ts_ns / 1_000_000_000,
            )
            .unwrap();
        engine
            .evaluate_and_write(
                &(TenantId(2), "dup".to_string()),
                ts_ns,
                ts_ns / 1_000_000_000,
            )
            .unwrap();

        let written = writer.written();
        assert_eq!(written.len(), 2);
        let by_table: HashMap<u32, f64> = written.iter().map(|e| (e.0, e.2)).collect();
        assert_eq!(by_table.get(&100), Some(&10.0));
        assert_eq!(by_table.get(&200), Some(&20.0));
    }

    // ─── 持久化 / 恢复测试（T02 / T03） ───

    fn temp_db() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("test_derived_engine_{}.db", uuid::Uuid::new_v4()))
    }

    /// 构建带 `Some(store)` 的引擎（恢复测试用）。
    fn make_engine_with_store(
        schema_map: HashMap<String, u32>,
        store: DerivedTagStore,
    ) -> DerivedTagEngine {
        let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot {
            values: HashMap::new(),
        });
        let writer: Arc<dyn WriteSink> = Arc::new(MockWriter::new());
        let schema: Arc<dyn SchemaAccess> = Arc::new(MockSchema {
            tables: schema_map,
            columns: HashMap::new(),
        });
        let config = ConfigHandle::new(AppConfig::default());
        DerivedTagEngine::new(snapshot, writer, schema, config, Some(Arc::new(store)))
    }

    /// register 成功落库 + 重启（新引擎）restore 后定义与依赖边一致。
    #[test]
    fn test_register_tag_persists_and_restore_roundtrip() {
        let path = temp_db();
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let engine =
                make_engine_with_store(HashMap::from([("sensor_a".to_string(), 7u32)]), store);
            engine
                .register_tag(
                    42,
                    "derived_a",
                    "sensor_a * 2",
                    FallbackPolicy::Strict,
                    DeadbandConfig::default(),
                )
                .unwrap();
            assert_eq!(engine.dep_graph().read().len(), 1);
        }
        // 模拟重启：重新 open store + 新引擎 restore。
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let engine =
                make_engine_with_store(HashMap::from([("sensor_a".to_string(), 7u32)]), store);
            let restored = engine.restore_definitions().unwrap();
            assert_eq!(restored, 1);
            let graph = engine.dep_graph().read();
            assert_eq!(graph.len(), 1);
            let node = graph.get(TenantId::DEFAULT, "derived_a").unwrap();
            assert_eq!(node.table_id, 42);
            assert_eq!(node.source_tags, vec!["sensor_a".to_string()]);
            assert_eq!(
                node.source_table_ids,
                vec![("sensor_a".to_string(), ColumnRef::single(7))]
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// 派生套派生按 created_at 序恢复：父先于子，依赖边（含传递闭包）恢复。
    #[test]
    fn test_restore_derived_on_derived_by_created_at_order() {
        let path = temp_db();
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let parent = DerivedTagRecord::new(
                TenantId::DEFAULT,
                "derived_base".into(),
                42,
                "sensor_a * 2".into(),
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
                vec![SourceRefDef {
                    name: "sensor_a".into(),
                    table_id: 7,
                    column_id: 0,
                }],
                1000,
                1000,
            );
            let child = DerivedTagRecord::new(
                TenantId::DEFAULT,
                "derived_child".into(),
                43,
                "derived_base + 1".into(),
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
                vec![SourceRefDef {
                    name: "derived_base".into(),
                    table_id: 42,
                    column_id: 0,
                }],
                2000,
                2000,
            );
            store.upsert(&parent).unwrap();
            store.upsert(&child).unwrap();
        }
        {
            let store = DerivedTagStore::open(&path).unwrap();
            // 物理表只映射 sensor_a；derived_base 走内存图兜底 → 需父先于子恢复。
            let engine =
                make_engine_with_store(HashMap::from([("sensor_a".to_string(), 7u32)]), store);
            let restored = engine.restore_definitions().unwrap();
            assert_eq!(restored, 2);
            let graph = engine.dep_graph().read();
            let child = graph.get(TenantId::DEFAULT, "derived_child").unwrap();
            assert_eq!(
                child.source_table_ids,
                vec![("derived_base".to_string(), ColumnRef::single(42))]
            );
            let affected =
                graph.get_transitive_derived_tags(&[(TenantId::DEFAULT, "sensor_a".into())]);
            assert!(affected.contains(&(TenantId::DEFAULT, "derived_base".into())));
            assert!(affected.contains(&(TenantId::DEFAULT, "derived_child".into())));
        }
        let _ = std::fs::remove_file(&path);
    }

    /// 坏表达式行 skip + 不阻断其余定义恢复。
    #[test]
    fn test_restore_bad_expression_skipped() {
        let path = temp_db();
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let good = DerivedTagRecord::new(
                TenantId::DEFAULT,
                "good".into(),
                1,
                "sensor_a".into(),
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
                vec![],
                1000,
                1000,
            );
            let bad = DerivedTagRecord::new(
                TenantId::DEFAULT,
                "bad".into(),
                2,
                "sensor_a + ".into(),
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
                vec![],
                2000,
                2000,
            );
            store.upsert(&good).unwrap();
            store.upsert(&bad).unwrap();
        }
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let engine =
                make_engine_with_store(HashMap::from([("sensor_a".to_string(), 7u32)]), store);
            let restored = engine.restore_definitions().unwrap();
            assert_eq!(restored, 1);
            assert!(engine
                .dep_graph()
                .read()
                .contains(TenantId::DEFAULT, "good"));
            assert!(!engine.dep_graph().read().contains(TenantId::DEFAULT, "bad"));
        }
        let _ = std::fs::remove_file(&path);
    }

    /// 源表被删（源不可解析 + 无持久化兜底）→ skip，不阻断启动。
    #[test]
    fn test_restore_unresolvable_source_skipped() {
        let path = temp_db();
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let orphan = DerivedTagRecord::new(
                TenantId::DEFAULT,
                "orphan".into(),
                42,
                "missing_source".into(),
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
                vec![], // 无持久化 source_table_ids 兜底
                1000,
                1000,
            );
            store.upsert(&orphan).unwrap();
        }
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let engine = make_engine_with_store(HashMap::new(), store);
            let restored = engine.restore_definitions().unwrap();
            assert_eq!(restored, 0);
            assert!(engine.dep_graph().read().is_empty());
        }
        let _ = std::fs::remove_file(&path);
    }

    /// 跨租户同名恢复隔离：各租户同名定义独立恢复、源解析互不串。
    #[test]
    fn test_restore_cross_tenant_same_name_isolation() {
        struct TenantSchema {
            tables: HashMap<(u32, String), u32>,
        }
        impl SchemaAccess for TenantSchema {
            fn table_tenant(&self, _table_id: u32) -> Option<TenantId> {
                None
            }
            fn resolve_column_ref(
                &self,
                tenant_id: TenantId,
                table_name: &str,
                column_name: Option<&str>,
            ) -> Option<ColumnRef> {
                let table_id = *self
                    .tables
                    .get(&(tenant_id.as_u32(), table_name.to_string()))?;
                match column_name {
                    None => Some(ColumnRef::single(table_id)),
                    Some(_) => None,
                }
            }
        }

        let path = temp_db();
        {
            let store = DerivedTagStore::open(&path).unwrap();
            store
                .upsert(&DerivedTagRecord::new(
                    TenantId(1),
                    "dup".into(),
                    100,
                    "sensor_a".into(),
                    FallbackPolicy::Strict,
                    DeadbandConfig::default(),
                    vec![SourceRefDef {
                        name: "sensor_a".into(),
                        table_id: 7,
                        column_id: 0,
                    }],
                    1000,
                    1000,
                ))
                .unwrap();
            store
                .upsert(&DerivedTagRecord::new(
                    TenantId(2),
                    "dup".into(),
                    200,
                    "sensor_b".into(),
                    FallbackPolicy::Strict,
                    DeadbandConfig::default(),
                    vec![SourceRefDef {
                        name: "sensor_b".into(),
                        table_id: 8,
                        column_id: 0,
                    }],
                    2000,
                    2000,
                ))
                .unwrap();
        }
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let mut tables = HashMap::new();
            tables.insert((1u32, "sensor_a".to_string()), 7u32);
            tables.insert((2u32, "sensor_b".to_string()), 8u32);
            let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot {
                values: HashMap::new(),
            });
            let writer: Arc<dyn WriteSink> = Arc::new(MockWriter::new());
            let schema: Arc<dyn SchemaAccess> = Arc::new(TenantSchema { tables });
            let config = ConfigHandle::new(AppConfig::default());
            let engine =
                DerivedTagEngine::new(snapshot, writer, schema, config, Some(Arc::new(store)));

            let restored = engine.restore_definitions().unwrap();
            assert_eq!(restored, 2);
            let graph = engine.dep_graph().read();
            assert_eq!(graph.len(), 2);
            let a = graph.get(TenantId(1), "dup").unwrap();
            let b = graph.get(TenantId(2), "dup").unwrap();
            assert_eq!(a.table_id, 100);
            assert_eq!(b.table_id, 200);
            assert_eq!(
                a.source_table_ids,
                vec![("sensor_a".to_string(), ColumnRef::single(7))]
            );
            assert_eq!(
                b.source_table_ids,
                vec![("sensor_b".to_string(), ColumnRef::single(8))]
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// persist-first 失败：upsert 报错 → register_tag 返回 PersistenceError 且内存无节点。
    #[test]
    fn test_persist_first_failure_leaves_memory_unchanged() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        // 破坏持久化层（drop 表），使后续 upsert 失败。
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch("DROP TABLE derived_tags;").unwrap();
        }
        let engine = make_engine_with_store(HashMap::from([("sensor_a".to_string(), 7u32)]), store);
        let result = engine.register_tag(
            42,
            "x",
            "sensor_a",
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
        );
        assert!(
            matches!(result, Err(DerivedError::PersistenceError { .. })),
            "expected persistence error, got {:?}",
            result
        );
        assert_eq!(
            engine.dep_graph().read().len(),
            0,
            "persist 失败时内存不应新增节点"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// QA finding 复现（§4.1）：re-register 环表达式 → 库不被污染（旧定义保留）
    /// + 客户端收到 CircularDependency + 重启 restore 后 tag 仍在（不再静默丢失）。
    #[test]
    fn test_reregister_cycle_does_not_pollute_store() {
        let path = temp_db();
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let engine =
                make_engine_with_store(HashMap::from([("sensor_a".to_string(), 7u32)]), store);

            // 1. 正常注册 a = sensor_a，落库 + 内存各一份。
            engine
                .register_tag(
                    42,
                    "a",
                    "sensor_a",
                    FallbackPolicy::Strict,
                    DeadbandConfig::default(),
                )
                .unwrap();
            assert_eq!(engine.dep_graph().read().len(), 1);

            // 2. re-register a 为自引用 `a + 1` → 必须返回 CircularDependency。
            let result = engine.register_tag(
                42,
                "a",
                "a + 1",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            );
            assert!(
                matches!(result, Err(DerivedError::CircularDependency { .. })),
                "expected CircularDependency, got {:?}",
                result
            );

            // 3. 库中表达式仍为旧定义 sensor_a（未被环表达式污染）。
            let records = engine.store.as_ref().unwrap().load_all().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(
                records[0].expression, "sensor_a",
                "失败 re-register 后库内定义应保持旧值"
            );

            // 4. 内存仍是旧定义（source_tags == ["sensor_a"]）。
            let graph = engine.dep_graph().read();
            let node = graph.get(TenantId::DEFAULT, "a").unwrap();
            assert_eq!(node.source_tags, vec!["sensor_a".to_string()]);
        }
        // 5. 模拟重启：restore 后 tag a 仍在，不再静默丢失。
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let engine =
                make_engine_with_store(HashMap::from([("sensor_a".to_string(), 7u32)]), store);
            let restored = engine.restore_definitions().unwrap();
            assert_eq!(restored, 1);
            assert!(engine.dep_graph().read().contains(TenantId::DEFAULT, "a"));
        }
        let _ = std::fs::remove_file(&path);
    }

    /// max_nodes 校验失败发生在 upsert 之前：库不新增行、内存不变、重启不丢。
    #[test]
    fn test_max_nodes_failure_does_not_pollute_store() {
        let path = temp_db();
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot {
                values: HashMap::new(),
            });
            let writer: Arc<dyn WriteSink> = Arc::new(MockWriter::new());
            let schema: Arc<dyn SchemaAccess> = Arc::new(MockSchema {
                tables: HashMap::from([("sensor_a".to_string(), 7u32)]),
                columns: HashMap::new(),
            });
            let mut app_cfg = AppConfig::default();
            app_cfg.derived.max_graph_nodes = 1;
            let config = ConfigHandle::new(app_cfg);
            let engine =
                DerivedTagEngine::new(snapshot, writer, schema, config, Some(Arc::new(store)));

            // 1. 注册 a（占满唯一名额）。
            engine
                .register_tag(
                    42,
                    "a",
                    "sensor_a",
                    FallbackPolicy::Strict,
                    DeadbandConfig::default(),
                )
                .unwrap();

            // 2. 注册 b → 触发 max_nodes 上限。
            let result = engine.register_tag(
                43,
                "b",
                "sensor_a",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            );
            assert!(
                matches!(result, Err(DerivedError::TooManyNodes { .. })),
                "expected TooManyNodes, got {:?}",
                result
            );

            // 3. 库中仅 a，b 未被持久化。
            let records = engine.store.as_ref().unwrap().load_all().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].tag_name, "a");

            // 4. 内存仅 a。
            assert_eq!(engine.dep_graph().read().len(), 1);
            assert!(engine.dep_graph().read().contains(TenantId::DEFAULT, "a"));
        }
        // 5. 模拟重启：a 仍在，b 从未落库。
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let engine =
                make_engine_with_store(HashMap::from([("sensor_a".to_string(), 7u32)]), store);
            let restored = engine.restore_definitions().unwrap();
            assert_eq!(restored, 1);
            assert!(engine.dep_graph().read().contains(TenantId::DEFAULT, "a"));
        }
        let _ = std::fs::remove_file(&path);
    }

    /// 并发成环回归（核心）：两个 register 目标互成环时，无论并发如何交错，
    /// 最终库/内存一致、无环定义落库、重启 restore 后 tag 不丢。
    ///
    /// 先注册 `a = sensor_x`、`b = sensor_y`，再并发 `register(a = b)` 与
    /// `register(b = a)`。两线程的 `add_node` 由依赖图写锁串行化，因此二者中
    /// 至少一个会因成环失败；失败的持久化写必须被回滚，否则库内会残留
    /// `a=b 且 b=a` 的环定义，重启 restore 时成环失败 → tag 静默丢失。
    ///
    /// 注意：并发时序无法精确控制（失败可能发生在 `add_node` 后的回滚，也可能
    /// 直接发生在 `validate_node` 前置校验），故本测试只断言「任意交错下库/内存
    /// 最终一致 + 无环落库 + 重启不丢」，不依赖特定时序。
    #[test]
    fn test_concurrent_register_cycle_rolls_back_store() {
        for _ in 0..16 {
            let path = temp_db();
            let store = DerivedTagStore::open(&path).unwrap();
            let schema_map = HashMap::from([
                ("sensor_x".to_string(), 7u32),
                ("sensor_y".to_string(), 8u32),
            ]);
            let engine = Arc::new(make_engine_with_store(schema_map.clone(), store));

            // 基线：a / b 各自依赖独立物理源，互不依赖。
            engine
                .register_tag(
                    41,
                    "a",
                    "sensor_x",
                    FallbackPolicy::Strict,
                    DeadbandConfig::default(),
                )
                .unwrap();
            engine
                .register_tag(
                    42,
                    "b",
                    "sensor_y",
                    FallbackPolicy::Strict,
                    DeadbandConfig::default(),
                )
                .unwrap();

            // 并发重注册：a=b 与 b=a 互相指向 → 必有一个成环失败。
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let mut handles = Vec::new();
            for (table_id, tag, expr) in [(41u32, "a", "b"), (42u32, "b", "a")] {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                handles.push(std::thread::spawn(move || {
                    barrier.wait();
                    engine.register_tag(
                        table_id,
                        tag,
                        expr,
                        FallbackPolicy::Strict,
                        DeadbandConfig::default(),
                    )
                }));
            }
            let results: Vec<Result<(), DerivedError>> =
                handles.into_iter().map(|h| h.join().unwrap()).collect();

            // 至少一个失败（成环）；不写死「恰好一个」，避免脆弱时序断言。
            let failures = results.iter().filter(|r| r.is_err()).count();
            assert!(
                failures >= 1,
                "并发成环至少一个 register 应失败，got {:?}",
                results
            );

            // 内存：仍 2 节点且无环。
            let (ea_mem, eb_mem) = {
                let graph = engine.dep_graph().read();
                assert_eq!(graph.len(), 2, "内存应保留 a/b 两个节点");
                assert!(graph.topological_sort().is_ok(), "内存依赖图不应成环");
                (
                    graph
                        .get(TenantId::DEFAULT, "a")
                        .unwrap()
                        .source_tags
                        .clone(),
                    graph
                        .get(TenantId::DEFAULT, "b")
                        .unwrap()
                        .source_tags
                        .clone(),
                )
            };

            // 库：2 条记录，且不存在 `a=b 且 b=a` 的环定义。
            let records = engine.store.as_ref().unwrap().load_all().unwrap();
            assert_eq!(records.len(), 2, "库应恰好两条定义");
            let expr_by_name: HashMap<String, String> = records
                .iter()
                .map(|r| (r.tag_name.clone(), r.expression.clone()))
                .collect();
            let ea = expr_by_name.get("a").expect("a 定义应存在").as_str();
            let eb = expr_by_name.get("b").expect("b 定义应存在").as_str();
            assert!(
                !(ea == "b" && eb == "a"),
                "环定义 (a=b 且 b=a) 不应同时落库"
            );

            // 库/内存一致：每个 tag 的库 expression 与内存 source_tags 一致。
            assert_eq!(ea_mem, vec![ea.to_string()], "a 库/内存不一致");
            assert_eq!(eb_mem, vec![eb.to_string()], "b 库/内存不一致");

            // 模拟重启：restore 后 a/b 都不丢，且无环。
            {
                let store2 = DerivedTagStore::open(&path).unwrap();
                let engine2 = make_engine_with_store(schema_map, store2);
                let restored = engine2.restore_definitions().unwrap();
                assert_eq!(restored, 2, "重启后 a/b 均应恢复，不得静默丢失");
                let graph2 = engine2.dep_graph().read();
                assert!(graph2.contains(TenantId::DEFAULT, "a"));
                assert!(graph2.contains(TenantId::DEFAULT, "b"));
                assert!(graph2.topological_sort().is_ok(), "重启后依赖图不应成环");
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    /// 同 `make_engine_with`，但可自定义 debounce 间隔（0 = 立即重算，便于直接观测 on_source_changed 结果）。
    fn make_engine_with_debounce(
        schema_map: HashMap<String, u32>,
        snapshot_values: HashMap<u32, (f64, u8, i64)>,
        debounce_ms: u64,
    ) -> (DerivedTagEngine, Arc<MockWriter>) {
        let values: HashMap<ColumnRef, (f64, u8, i64)> = snapshot_values
            .into_iter()
            .map(|(table_id, v)| (ColumnRef::single(table_id), v))
            .collect();
        let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot { values });
        let writer = Arc::new(MockWriter::new());
        let writer_handle: Arc<dyn WriteSink> = writer.clone();
        let schema: Arc<dyn SchemaAccess> = Arc::new(MockSchema {
            tables: schema_map,
            columns: HashMap::new(),
        });
        let mut cfg = AppConfig::default();
        cfg.derived.debounce_interval_ms = debounce_ms;
        let config = ConfigHandle::new(cfg);
        let engine = DerivedTagEngine::new(snapshot, writer_handle, schema, config, None);
        (engine, writer)
    }

    /// P1 语义一致性：同一 source 一批内多次出现，批级传并集后只重算一次，
    /// 结果值 = 用最新 source 值的表达式值。
    #[test]
    fn test_on_source_changed_batch_dedup_same_source() {
        let mut schema_map = HashMap::new();
        schema_map.insert("sensor_a".to_string(), 1u32);
        let mut snapshot_values = HashMap::new();
        snapshot_values.insert(1u32, (100.0, Quality::GOOD.0, 1_000_000_000));
        let (engine, writer) = make_engine_with_debounce(schema_map, snapshot_values, 0);

        engine
            .register_tag(
                42,
                "derived_1",
                "sensor_a * 2",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        let ts_ns = 2_000_000_000i64;
        // 同一 source (table 1) 重复 3 次，批级一次传入（调用方 startup.rs 会先按 table_id 去重，
        // 这里验证 engine 层对多 source 并集 + 同源重复的归并）。
        engine.on_source_changed(
            &[
                (TenantId::DEFAULT, 1, "sensor_a".to_string()),
                (TenantId::DEFAULT, 1, "sensor_a".to_string()),
                (TenantId::DEFAULT, 1, "sensor_a".to_string()),
            ],
            1,
            ts_ns,
        );

        let written = writer.written();
        assert_eq!(written.len(), 1, "同源批内多点只应重算一次，got {written:?}");
        let (table_id, name, value, _quality, _ts) = &written[0];
        assert_eq!(*table_id, 42);
        assert_eq!(name, "derived_1");
        assert_eq!(*value, 200.0);
    }

    /// P1 语义一致性：批级一次 on_source_changed 与逐点多次调用，最终派生值一致
    /// （derived 读 snapshot 最新值，只减重算次数，不改结果）。
    #[test]
    fn test_batch_vs_per_point_consistency() {
        let build = || {
            let mut schema_map = HashMap::new();
            schema_map.insert("sensor_a".to_string(), 1u32);
            let mut snapshot_values = HashMap::new();
            snapshot_values.insert(1u32, (100.0, Quality::GOOD.0, 1_000_000_000));
            make_engine_with_debounce(schema_map, snapshot_values, 0)
        };

        // 批级：一次 on_source_changed（含同 source 多点，模拟批内去重后传并集）。
        let (engine_batch, writer_batch) = build();
        engine_batch
            .register_tag(
                42,
                "d",
                "sensor_a * 2",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();
        engine_batch.on_source_changed(
            &[(TenantId::DEFAULT, 1, "sensor_a".to_string())],
            1,
            2_000_000_000,
        );

        // 逐点：同一 source 3 点，各用不同 batch_id（模拟旧逐点路径）。
        let (engine_pt, writer_pt) = build();
        engine_pt
            .register_tag(
                42,
                "d",
                "sensor_a * 2",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();
        for (i, bid) in [10u64, 11, 12].iter().enumerate() {
            engine_pt.on_source_changed(
                &[(TenantId::DEFAULT, 1, "sensor_a".to_string())],
                *bid,
                2_000_000_000 + i as i64,
            );
        }

        let last_batch = writer_batch.written().last().map(|w| w.2).unwrap();
        let last_pt = writer_pt.written().last().map(|w| w.2).unwrap();
        assert_eq!(last_batch, 200.0);
        assert_eq!(last_batch, last_pt, "批级与逐点最终派生值必须一致");
    }

    /// P4 去抖 + 异步重算：`mark_dirty` 只标脏不内联重算；`flush_ready` 一次合并
    /// 去抖只重算一次，结果 ts 取最近一次源变化 ts。
    #[test]
    fn test_mark_dirty_flush_ready_recalculates_once() {
        let mut schema_map = HashMap::new();
        schema_map.insert("sensor_a".to_string(), 1u32);
        let mut snapshot_values = HashMap::new();
        snapshot_values.insert(1u32, (100.0, Quality::GOOD.0, 1_000_000_000));
        let (engine, writer) = make_engine_with_debounce(schema_map, snapshot_values, 0);

        engine
            .register_tag(
                42,
                "derived_1",
                "sensor_a * 2",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        // 同源多点 mark_dirty（不同 batch_id）→ 只标脏，不内联重算。
        engine.mark_dirty(
            &[(TenantId::DEFAULT, 1, "sensor_a".to_string())],
            1,
            2_000_000_000,
        );
        engine.mark_dirty(
            &[(TenantId::DEFAULT, 1, "sensor_a".to_string())],
            2,
            3_000_000_000,
        );
        assert!(writer.written().is_empty(), "mark_dirty 不应内联重算");
        assert!(engine.has_dirty(), "标脏后应有 dirty tag");

        // flush_ready 一次：去抖合并，只重算一次，ts 取 max。
        engine.flush_ready();
        let written = writer.written();
        assert_eq!(written.len(), 1, "同源多点去抖后只重算一次，got {written:?}");
        let (table_id, name, value, _quality, ts) = &written[0];
        assert_eq!(*table_id, 42);
        assert_eq!(name, "derived_1");
        assert_eq!(*value, 200.0);
        assert_eq!(*ts, 3_000_000_000, "结果 ts 应为最近一次源变化 ts");
        assert!(!engine.has_dirty());
    }

    /// 独立 QA（D 节）：异步重算不丢最后一批——连续 mark_dirty 多批后，
    /// `flush_ready` 的派生值 = 用「最新源值」算出的结果（不漏最后一次变化）。
    #[test]
    fn test_mark_dirty_multi_batch_not_lose_last_change() {
        // 可变快照：源值可随时间变化，验证异步重算读到的是最新值。
        struct MutableSnapshot {
            values: parking_lot::Mutex<HashMap<ColumnRef, (f64, u8, i64)>>,
        }
        impl SnapshotRead for MutableSnapshot {
            fn batch_get_columns(
                &self,
                columns: &[ColumnRef],
            ) -> HashMap<ColumnRef, (f64, u8, i64)> {
                let v = self.values.lock();
                columns.iter().filter_map(|c| v.get(c).map(|x| (*c, *x))).collect()
            }
        }

        let snapshot = Arc::new(MutableSnapshot {
            values: parking_lot::Mutex::new(HashMap::new()),
        });
        // 初始源值 sensor_a（table 1）= 10。
        snapshot
            .values
            .lock()
            .insert(ColumnRef::single(1), (10.0, Quality::GOOD.0, 1_000_000_000));

        let writer = Arc::new(MockWriter::new());
        let writer_handle: Arc<dyn WriteSink> = writer.clone();
        let schema: Arc<dyn SchemaAccess> = Arc::new(MockSchema {
            tables: HashMap::from([("sensor_a".to_string(), 1u32)]),
            columns: HashMap::new(),
        });
        let mut cfg = AppConfig::default();
        cfg.derived.debounce_interval_ms = 0; // 立即就绪，便于同步观测
        let config = ConfigHandle::new(cfg);
        let engine = DerivedTagEngine::new(snapshot.clone(), writer_handle, schema, config, None);
        engine
            .register_tag(
                42,
                "d",
                "sensor_a * 2",
                FallbackPolicy::Strict,
                DeadbandConfig::default(),
            )
            .unwrap();

        // 第一批标脏（源值仍 10）。
        engine.mark_dirty(
            &[(TenantId::DEFAULT, 1, "sensor_a".to_string())],
            1,
            1_000_000_000,
        );
        // 源值更新到 20，第二批标脏（同源去抖合并，ts 取 max = 2e9）。
        snapshot.values.lock().insert(
            ColumnRef::single(1),
            (20.0, Quality::GOOD.0, 2_000_000_000),
        );
        engine.mark_dirty(
            &[(TenantId::DEFAULT, 1, "sensor_a".to_string())],
            2,
            2_000_000_000,
        );

        assert!(writer.written().is_empty(), "mark_dirty 不应内联重算");
        engine.flush_ready();

        let written = writer.written();
        assert_eq!(written.len(), 1, "去抖后只重算一次，got {written:?}");
        let (table_id, name, value, _quality, ts) = &written[0];
        assert_eq!(*table_id, 42);
        assert_eq!(name, "d");
        assert!(
            (value - 40.0).abs() < 1e-9,
            "应用最新源值 20 → 40，而非旧值 10 → 20，got {value}"
        );
        assert_eq!(*ts, 2_000_000_000, "结果 ts 应为最新源变化 ts");
        assert!(!engine.has_dirty());
    }
}

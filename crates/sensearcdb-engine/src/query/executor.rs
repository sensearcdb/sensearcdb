//! 查询执行器：k-way merge（多源归并 + 去重）→ Arrow RecordBatch。
//!
//! QueryExecutor 执行 `QueryPlan`，从三个数据源合并数据：
//! 1. MemTable (active)
//! 2. MemTable (frozen / immutable)
//! 3. SAC Blocks (L1 索引定位)
//!
//! 去重优先级：active > frozen > SAC（同 `ts` 时保留较新来源）。
//!
//! P1（多列投影 + 类型保真）起，读路径全程使用 `TypedValue`，
//! 弃用 `as_f64` / `unwrap_or(0.0)` 折叠，`None` 传播到 Arrow null bitmap。

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringBuilder,
    UInt16Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sensearcdb_common::{
    agg_result_type, AggFunction, AggregateState, ColumnRef, DataType as PhysicalType, Numeric,
    SenseArcError, TenantId,
};

use crate::encryption::SharedEncryptor;
use crate::engine::QueryResult;
use crate::memtable::chunk::ColumnarChunk;
use crate::memtable::manager::MemTableManager;
use crate::memtable::TypedValue;
use crate::query::planner::{
    resolve_table_id, AggArg, AggCall, CaseExpr, CmpOp, OrderKey, OrderKeyColumn, Predicate,
    ProjectionExpr, QueryPlan, ScalarExpr, TimeAlign, TimeBucket, WindowArg, WindowFunction,
    WindowSpec,
};
use crate::sac::{SacNaming, SacReader};

/// 内部多列排序行（列值对齐 `column_ids`）。
#[derive(Debug, Clone)]
struct QueryRow {
    /// 时间戳。
    ts: i64,
    /// 各列值（顺序与查询传入的 `column_ids` 一致；缺失列 → `None`）。
    values: Vec<Option<TypedValue>>,
    /// 来源流索引（0=active, 1=frozen, 2=SAC；越小优先级越高）。
    source: usize,
}

impl PartialEq for QueryRow {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts
    }
}

impl Eq for QueryRow {}

impl PartialOrd for QueryRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueryRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts
            .cmp(&other.ts)
            .then_with(|| self.source.cmp(&other.source))
    }
}

/// 内部通配收集行（列值按键 `column_id` 对齐，用于 `SELECT *` 全列收集）。
#[derive(Debug, Clone)]
struct MapRow {
    /// 时间戳。
    ts: i64,
    /// 各列值（`column_id → Option<TypedValue>`）。
    values: HashMap<u16, Option<TypedValue>>,
    /// 来源流索引。
    source: usize,
}

impl PartialEq for MapRow {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts
    }
}

impl Eq for MapRow {}

impl PartialOrd for MapRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MapRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts
            .cmp(&other.ts)
            .then_with(|| self.source.cmp(&other.source))
    }
}

/// 查询时间范围（含边界开闭语义）。
///
/// 扫描阶段仍用底层 `get_range`/`read_range` 的闭区间 `[start, end]` 做索引定位，
/// 边界排他（`>`/`<`）在合并去重后由 [`TsRange::contains`] 统一过滤。
#[derive(Debug, Clone, Copy)]
struct TsRange {
    start: i64,
    end: i64,
    start_inclusive: bool,
    end_inclusive: bool,
}

impl TsRange {
    /// 构造闭区间范围（供 `execute_range` / `execute_range_columns_typed` 等内部 API）。
    fn inclusive(start: i64, end: i64) -> Self {
        Self {
            start,
            end,
            start_inclusive: true,
            end_inclusive: true,
        }
    }

    /// 判断时间戳是否落在该范围内（含边界开闭语义）。
    #[inline]
    fn contains(&self, ts: i64) -> bool {
        let start_ok = if self.start_inclusive {
            ts >= self.start
        } else {
            ts > self.start
        };
        let end_ok = if self.end_inclusive {
            ts <= self.end
        } else {
            ts < self.end
        };
        start_ok && end_ok
    }
}

/// 已解析的单个输出列。
#[derive(Debug, Clone)]
struct ProjectedColumn {
    /// Arrow 输出列名。
    name: String,
    /// `None` = 非数据列（ts 伪列 或 标量表达式）；`Some((column_id, data_index))` = 数据列，
    /// `data_index` 为该列在 `QueryRow.values` 中的下标。
    source: Option<(u16, usize)>,
    /// 标量表达式（CASE WHEN 独立投影）；与 `source` 互斥，且 `source` 为 `None` 时才有意义。
    expr: Option<ScalarExpr>,
}

/// 查询执行器。
///
/// 执行 `QueryPlan`，从多源数据中执行 k-way merge，
/// 输出 Arrow RecordBatch。
pub struct QueryExecutor {
    /// MemTable 管理器（active + frozen）。
    memtable: Arc<MemTableManager>,
    /// SAC 文件根目录。
    sac_dir: PathBuf,
    /// 可选加密器（TDE 关闭时为 `None`）。
    encryptor: SharedEncryptor,
    /// 分区窗口时长（毫秒）——SAC 目录级时间剪枝用（默认 24h）。
    partition_duration_ms: u64,
}

impl QueryExecutor {
    /// 创建新的查询执行器（TDE 关闭）。
    ///
    /// # 参数
    /// - `memtable`: MemTable 管理器。
    /// - `sac_dir`: SAC 文件根目录。
    pub fn new(memtable: Arc<MemTableManager>, sac_dir: PathBuf) -> Self {
        Self {
            memtable,
            sac_dir,
            encryptor: None,
            partition_duration_ms: 86_400_000,
        }
    }

    /// 设置分区窗口时长（毫秒），返回自身（builder 风格）。
    ///
    /// 目录级时间剪枝据此判定「分区目录名（partition_ns）是否与查询时间范围重叠」。
    /// 默认 24h；引擎主路径用 `cfg.partition.duration_ms` 覆盖，避免剪枝误伤数据。
    pub fn with_partition_duration(mut self, duration_ms: u64) -> Self {
        self.partition_duration_ms = duration_ms;
        self
    }

    /// 创建带 TDE 加密器的查询执行器。
    ///
    /// # 参数
    /// - `memtable`: MemTable 管理器。
    /// - `sac_dir`: SAC 文件根目录。
    /// - `encryptor`: 可选加密器（`None` = TDE 关）。
    pub fn new_with_encryptor(
        memtable: Arc<MemTableManager>,
        sac_dir: PathBuf,
        encryptor: SharedEncryptor,
    ) -> Self {
        Self {
            memtable,
            sac_dir,
            encryptor,
            partition_duration_ms: 86_400_000,
        }
    }

    /// 执行查询。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID（数据面隔离：三源均按此过滤）。
    /// - `sql`: SQL 查询语句。
    ///
    /// # 返回值
    /// `QueryResult` 包含 Arrow RecordBatch 列表。
    pub async fn execute(
        &self,
        tenant_id: TenantId,
        sql: &str,
    ) -> Result<QueryResult, SenseArcError> {
        let plan = crate::query::planner::QueryPlanner::plan(sql)?;
        self.execute_plan(tenant_id, &plan).await
    }

    /// 按 table_id 直接执行范围查询（供集成层使用）。
    ///
    /// 语义等价于 `SELECT ts, value FROM table_id`，其中 `value` = 列 0，
    /// 且类型保真（不再折叠 f64）。
    pub async fn execute_range(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        ts_start: i64,
        ts_end: i64,
    ) -> Result<QueryResult, SenseArcError> {
        let projected = vec![
            ProjectedColumn {
                name: "ts".to_string(),
                source: None,
                expr: None,
            },
            ProjectedColumn {
                name: "value".to_string(),
                source: Some((0, 0)),
                expr: None,
            },
        ];
        let all_rows = self
            .collect_multi_rows(
                tenant_id,
                table_id,
                &[0],
                TsRange::inclusive(ts_start, ts_end),
                None,
            )
            .await?;
        let batch = Self::build_record_batch(&all_rows, &projected, &[0]);
        Ok(QueryResult {
            batches: batch.map(|b| vec![b]).unwrap_or_default(),
            warning: None,
        })
    }

    /// 按 table_id + 列投影执行范围查询（三源合并），直接返回**类型化**逐列值 + 逐行 ts。
    ///
    /// 绕过 Arrow RecordBatch，避免 f64 折叠，且不与单列 Arrow 查询路径重复
    /// （设计 §7 风险 6：单一实现路径）。
    ///
    /// # 返回
    /// `(columns, timestamps)`：`columns[i]` 对应 `column_ids[i]` 的逐行值
    /// （缺失列/缺失值 → `None`），`timestamps` 为逐行纳秒时间戳。各列长度与时间戳一致。
    pub async fn execute_range_columns_typed(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        column_ids: &[u16],
        ts_start: i64,
        ts_end: i64,
    ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
        if column_ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let all_rows = self
            .collect_multi_rows(
                tenant_id,
                table_id,
                column_ids,
                TsRange::inclusive(ts_start, ts_end),
                None,
            )
            .await?;

        let timestamps: Vec<i64> = all_rows.iter().map(|r| r.ts).collect();
        let mut columns: Vec<Vec<Option<TypedValue>>> = Vec::with_capacity(column_ids.len());
        for col_idx in 0..column_ids.len() {
            columns.push(all_rows.iter().map(|r| r.values[col_idx].clone()).collect());
        }

        Ok((columns, timestamps))
    }

    /// 执行查询计划。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID（三源按此过滤）。
    /// - `plan`: 查询计划。
    pub async fn execute_plan(
        &self,
        tenant_id: TenantId,
        plan: &QueryPlan,
    ) -> Result<QueryResult, SenseArcError> {
        // 解析 table_name[.col_name] → (table_id, Option<column_id>)
        // （引擎不依赖 Schema，做轻量解析；`column_id` 为 Some 时配合通配只投影该列）
        let (table_id, from_column) = match resolve_table_id(&plan.table_name) {
            Some(resolved) => resolved,
            None => {
                // 无法解析 table_id，返回空结果
                return Ok(QueryResult {
                    batches: Vec::new(),
                    warning: Some(format!("unresolved table name: {}", plan.table_name)),
                });
            }
        };

        let range = TsRange {
            start: plan.ts_start.unwrap_or(i64::MIN),
            end: plan.ts_end.unwrap_or(i64::MAX),
            start_inclusive: plan.ts_start_inclusive,
            end_inclusive: plan.ts_end_inclusive,
        };

        let wildcard = plan
            .projection
            .iter()
            .any(|e| matches!(e, ProjectionExpr::Wildcard));

        let has_aggregate = plan
            .projection
            .iter()
            .any(|e| matches!(e, ProjectionExpr::Aggregate(_)));

        let has_window = plan
            .projection
            .iter()
            .any(|e| matches!(e, ProjectionExpr::Window(_)));

        if has_window {
            return self
                .execute_window(
                    tenant_id,
                    table_id,
                    &plan.projection,
                    &plan.group_by,
                    plan.time_bucket.as_ref(),
                    range,
                    plan.predicate.as_ref(),
                    plan.limit,
                )
                .await;
        }

        if has_aggregate {
            return self
                .execute_aggregate(
                    tenant_id,
                    table_id,
                    &plan.projection,
                    &plan.group_by,
                    plan.time_bucket.as_ref(),
                    range,
                    plan.predicate.as_ref(),
                    plan.limit,
                )
                .await;
        }

        if wildcard {
            self.execute_wildcard(
                tenant_id,
                table_id,
                from_column,
                range,
                plan.predicate.as_ref(),
                plan.limit,
            )
            .await
        } else {
            self.execute_projected(
                tenant_id,
                table_id,
                &plan.projection,
                range,
                plan.predicate.as_ref(),
                plan.limit,
            )
            .await
        }
    }

    /// 显式投影路径（不含通配）：按 projection 逐列读取并构建 Arrow。
    ///
    /// `predicate` 中引用的列即使未出现在投影中也会一并收集（供过滤），
    /// 但只投影 `projection` 声明的列。
    async fn execute_projected(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        projection: &[ProjectionExpr],
        range: TsRange,
        predicate: Option<&Predicate>,
        limit: Option<usize>,
    ) -> Result<QueryResult, SenseArcError> {
        let (mut projected, data_ids) = resolve_explicit_projection(projection);
        let combined_ids = combine_column_ids(&data_ids, predicate);

        // 谓词额外列被追加到 `combined_ids` 尾部，需重映射投影列的数据下标。
        for p in &mut projected {
            if let Some((cid, idx)) = &mut p.source {
                *idx = combined_ids
                    .iter()
                    .position(|&x| x == *cid)
                    .expect("projection column must exist in combined ids");
            }
        }

        let mut all_rows = self
            .collect_multi_rows(tenant_id, table_id, &combined_ids, range, predicate)
            .await?;

        if let Some(limit) = limit {
            all_rows.truncate(limit);
        }

        let batch = Self::build_record_batch(&all_rows, &projected, &combined_ids);
        Ok(QueryResult {
            batches: batch.map(|b| vec![b]).unwrap_or_default(),
            warning: None,
        })
    }

    /// 通配投影路径（`SELECT *`）：输出 `ts` + 全部实际存在的列。
    ///
    /// `from_column` 为 `Some(cid)` 时（`FROM table_42.col_7` 列限定通配），
    /// 退化为 `ts + col_cid` 单列投影。
    async fn execute_wildcard(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        from_column: Option<u16>,
        range: TsRange,
        predicate: Option<&Predicate>,
        limit: Option<usize>,
    ) -> Result<QueryResult, SenseArcError> {
        // 列限定通配：只投影该列。
        if let Some(cid) = from_column {
            let projected = vec![
                ProjectedColumn {
                    name: "ts".to_string(),
                    source: None,
                    expr: None,
                },
                ProjectedColumn {
                    name: format!("col_{}", cid),
                    source: Some((cid, 0)),
                    expr: None,
                },
            ];
            // cid 恒为 combined_ids[0]，谓词列追加在尾部，投影下标无需重映射。
            let combined_ids = combine_column_ids(&[cid], predicate);
            let mut all_rows = self
                .collect_multi_rows(tenant_id, table_id, &combined_ids, range, predicate)
                .await?;
            if let Some(limit) = limit {
                all_rows.truncate(limit);
            }
            let batch = Self::build_record_batch(&all_rows, &projected, &combined_ids);
            return Ok(QueryResult {
                batches: batch.map(|b| vec![b]).unwrap_or_default(),
                warning: None,
            });
        }

        // 无列限定：三源全列收集（单一读取，避免「先探列再重读」）。
        let (map_rows, ids) = self
            .collect_all_rows(tenant_id, table_id, range, predicate)
            .await?;

        let projected: Vec<ProjectedColumn> = std::iter::once(ProjectedColumn {
            name: "ts".to_string(),
            source: None,
            expr: None,
        })
        .chain(ids.iter().enumerate().map(|(idx, &id)| ProjectedColumn {
            name: format!("col_{}", id),
            source: Some((id, idx)),
            expr: None,
        }))
        .collect();

        let mut all_rows: Vec<QueryRow> = map_rows
            .into_iter()
            .map(|mr| QueryRow {
                ts: mr.ts,
                values: ids
                    .iter()
                    .map(|id| mr.values.get(id).cloned().unwrap_or(None))
                    .collect(),
                source: mr.source,
            })
            .collect();

        if let Some(limit) = limit {
            all_rows.truncate(limit);
        }

        let batch = Self::build_record_batch(&all_rows, &projected, &ids);
        Ok(QueryResult {
            batches: batch.map(|b| vec![b]).unwrap_or_default(),
            warning: None,
        })
    }

    /// 聚合路径：扫描 + 谓词过滤后按分组键分组，复用 `AggregateState` 做 typed 增量聚合。
    ///
    /// - 无 `GROUP BY` → 单组（空输入也产出一行空态结果）；
    /// - 有 `GROUP BY` → 按键分组，输出按「首次出现」（ts 升序扫描）顺序。
    async fn execute_aggregate(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        projection: &[ProjectionExpr],
        group_by: &[ColumnRef],
        time_bucket: Option<&TimeBucket>,
        range: TsRange,
        predicate: Option<&Predicate>,
        limit: Option<usize>,
    ) -> Result<QueryResult, SenseArcError> {
        let has_bucket = time_bucket.is_some();

        // 1) 拆分输出描述：聚合列 + 分组键列（时间桶键作为隐式首列，见第 5 步）。
        let mut agg_specs: Vec<AggCall> = Vec::new();
        let mut out_specs: Vec<AggOut> = Vec::with_capacity(projection.len());
        for e in projection {
            match e {
                ProjectionExpr::Aggregate(call) => {
                    agg_specs.push(call.clone());
                    out_specs.push(AggOut::Aggregate(agg_specs.len() - 1));
                }
                ProjectionExpr::Column(col) => {
                    if !group_by.iter().any(|g| g.column_id == col.column_id) {
                        return Err(SenseArcError::Internal(format!(
                            "projection column col_{} must appear in GROUP BY",
                            col.column_id
                        )));
                    }
                    out_specs.push(AggOut::Group(col.column_id));
                }
                ProjectionExpr::Timestamp => {
                    return Err(SenseArcError::Internal(
                        "aggregate query does not support `ts` projection (time bucket is output implicitly)"
                            .into(),
                    ));
                }
                ProjectionExpr::Wildcard => {
                    return Err(SenseArcError::Internal(
                        "aggregate query does not support `*` projection".into(),
                    ));
                }
                ProjectionExpr::Expr(_) => {
                    return Err(SenseArcError::Internal(
                        "aggregate query does not support CASE scalar projection (CASE only allowed as aggregate argument)"
                            .into(),
                    ));
                }
                ProjectionExpr::Window(_) => {
                    return Err(SenseArcError::Internal(
                        "window function must be routed through the window operator".into(),
                    ));
                }
            }
        }

        // 2) 收集所需列：分组键 + 聚合参数列（含 CASE/表达式引用列）+ FILTER 谓词列，再并入 WHERE 谓词列。
        let mut base_ids: Vec<u16> = group_by.iter().map(|g| g.column_id).collect();
        for spec in &agg_specs {
            if let Some(arg) = &spec.arg {
                let mut refs = BTreeSet::new();
                collect_agg_arg_columns(arg, &mut refs);
                for cid in refs {
                    if !base_ids.contains(&cid) {
                        base_ids.push(cid);
                    }
                }
            }
            if let Some(filter) = &spec.filter {
                let mut refs = BTreeSet::new();
                collect_predicate_columns(filter, &mut refs);
                for cid in refs {
                    if !base_ids.contains(&cid) {
                        base_ids.push(cid);
                    }
                }
            }
        }
        let combined_ids = combine_column_ids(&base_ids, predicate);
        let rows = self
            .collect_multi_rows(tenant_id, table_id, &combined_ids, range, predicate)
            .await?;

        let group_idxs: Vec<usize> = group_by
            .iter()
            .map(|g| idx_of(&combined_ids, g.column_id))
            .collect();

        // 3) 类型保真：聚合源类型扫描全部非 NULL 参数值并统一（CASE 分支混合提升 F64）。
        let source_types = determine_source_types(&agg_specs, &combined_ids, &rows);

        // 4) 分组聚合（复用 AggregateState，COUNT 直接累加 pub `count` 字段）。
        let mut groups: Vec<GroupResult> = Vec::new();
        let mut index: HashMap<GroupKey, usize> = HashMap::new();

        // 无时间桶且无 GROUP BY：预置单组（空输入也产出 COUNT=0 / SUM=0 等空态结果）。
        if !has_bucket && group_by.is_empty() {
            index.insert(GroupKey(Vec::new()), 0);
            groups.push(GroupResult {
                key: GroupKey(Vec::new()),
                states: make_empty_states(&agg_specs, &source_types),
            });
        }

        for row in &rows {
            let key = build_group_key(row, time_bucket, &group_idxs);
            let gi = match index.get(&key) {
                Some(&i) => i,
                None => {
                    let i = groups.len();
                    index.insert(key.clone(), i);
                    groups.push(GroupResult {
                        key,
                        states: make_empty_states(&agg_specs, &source_types),
                    });
                    i
                }
            };
            accumulate_row(&mut groups[gi].states, &agg_specs, &combined_ids, row);
        }

        if let Some(limit) = limit {
            groups.truncate(limit);
        }

        // 5) 构建 Arrow：时间桶键（隐式首列 `ts`）+ 分组键列 + 聚合结果列。
        let bucket_offset = usize::from(has_bucket);
        let mut fields: Vec<Field> = Vec::with_capacity(out_specs.len() + bucket_offset);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(out_specs.len() + bucket_offset);

        if has_bucket {
            let ts_values: Vec<i64> = groups
                .iter()
                .map(|g| match &g.key.0[0] {
                    Some(TypedValue::I64(x)) => *x,
                    _ => unreachable!("time bucket key must be I64"),
                })
                .collect();
            fields.push(Field::new("ts".to_string(), DataType::Int64, false));
            arrays.push(Arc::new(Int64Array::from(ts_values)));
        }

        for out in &out_specs {
            match out {
                AggOut::Group(cid) => {
                    let gi = group_by
                        .iter()
                        .position(|g| g.column_id == *cid)
                        .expect("group column must exist");
                    let key_idx = bucket_offset + gi;
                    let values: Vec<Option<TypedValue>> =
                        groups.iter().map(|g| g.key.0[key_idx].clone()).collect();
                    let dt = values
                        .iter()
                        .find_map(|v| v.as_ref().map(TypedValue::data_type))
                        .unwrap_or(PhysicalType::F64);
                    fields.push(Field::new(
                        format!("col_{}", cid),
                        arrow_data_type(dt),
                        true,
                    ));
                    arrays.push(build_typed_array(dt, &values));
                }
                AggOut::Aggregate(si) => {
                    let spec = &agg_specs[*si];
                    let result_type = agg_result_type(spec.function, source_types[*si]);
                    let values: Vec<Option<TypedValue>> = groups
                        .iter()
                        .map(|g| Some(g.states[*si].result(spec.function)))
                        .collect();
                    fields.push(Field::new(
                        agg_column_name(spec),
                        arrow_data_type(result_type),
                        false,
                    ));
                    arrays.push(build_typed_array(result_type, &values));
                }
            }
        }

        let batch = if groups.is_empty() {
            None
        } else {
            RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).ok()
        };

        Ok(QueryResult {
            batches: batch.map(|b| vec![b]).unwrap_or_default(),
            warning: None,
        })
    }

    /// 窗口路径（P6）：`Scan → Filter → [GroupByAggregate] → Window → Project → Arrow`。
    ///
    /// - 无聚合：窗口直接作用在原始有序行流（ts 升序）上。
    /// - 有聚合/时间桶：先复用 `AggregateState` 分组聚合，再对「聚合结果行」执行窗口
    ///   （设计 §4.6：窗口在 `GroupByAggregate` 之后执行；窗口与聚合解耦、独立实现）。
    #[allow(clippy::too_many_arguments)]
    async fn execute_window(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        projection: &[ProjectionExpr],
        group_by: &[ColumnRef],
        time_bucket: Option<&TimeBucket>,
        range: TsRange,
        predicate: Option<&Predicate>,
        limit: Option<usize>,
    ) -> Result<QueryResult, SenseArcError> {
        let has_aggregate = projection.iter().any(|e| match e {
            ProjectionExpr::Aggregate(_) => true,
            // 窗口参数为聚合（如 lead(avg(col_0))）时，同样需要先走分组聚合。
            ProjectionExpr::Window(spec) => matches!(spec.arg, WindowArg::Aggregate(_)),
            _ => false,
        });
        let has_bucket = time_bucket.is_some();

        // 1) 拆分输出描述：窗口规格 + 聚合规格 + 输出列（含分组键/ts）。
        let mut window_specs: Vec<WindowSpec> = Vec::new();
        let mut agg_specs: Vec<AggCall> = Vec::new();
        let mut out_specs: Vec<WindowOut> = Vec::with_capacity(projection.len());
        for e in projection {
            match e {
                ProjectionExpr::Window(spec) => {
                    window_specs.push(spec.clone());
                    out_specs.push(WindowOut::Window(window_specs.len() - 1));
                }
                ProjectionExpr::Aggregate(call) => {
                    agg_specs.push(call.clone());
                    out_specs.push(WindowOut::Aggregate(agg_specs.len() - 1));
                }
                ProjectionExpr::Column(col) => {
                    if has_aggregate && !group_by.iter().any(|g| g.column_id == col.column_id) {
                        return Err(SenseArcError::Internal(format!(
                            "projection column col_{} must appear in GROUP BY",
                            col.column_id
                        )));
                    }
                    out_specs.push(WindowOut::Column(col.column_id));
                }
                ProjectionExpr::Timestamp => {
                    if has_aggregate {
                        return Err(SenseArcError::Internal(
                            "window + aggregate query does not support `ts` projection (time bucket is output implicitly)"
                                .into(),
                        ));
                    }
                    out_specs.push(WindowOut::Timestamp);
                }
                ProjectionExpr::Expr(_) => {
                    return Err(SenseArcError::Internal(
                        "window query does not support CASE scalar projection".into(),
                    ));
                }
                ProjectionExpr::Wildcard => {
                    return Err(SenseArcError::Internal(
                        "window query does not support `*` projection".into(),
                    ));
                }
            }
        }

        // 窗口参数中的内嵌聚合（如 `lead(avg(col_0))` 无兄弟 avg 投影时）也须纳入聚合规格，
        // 保证「先聚合、再窗口」时该聚合结果被计算；按值去重（`AggCall` PartialEq）。
        for spec in &window_specs {
            if let WindowArg::Aggregate(call) = &spec.arg {
                if !agg_specs.contains(call) {
                    agg_specs.push(call.clone());
                }
            }
        }

        // 2) 聚合态下，窗口引用的数据列（arg/partition/order）必须是分组键。
        if has_aggregate {
            for spec in &window_specs {
                let mut refs = BTreeSet::new();
                if let WindowArg::Column(col) = &spec.arg {
                    refs.insert(col.column_id);
                }
                for p in &spec.partition_by {
                    refs.insert(p.column_id);
                }
                for k in &spec.order_by {
                    if let OrderKeyColumn::Column(c) = &k.column {
                        refs.insert(c.column_id);
                    }
                }
                for cid in refs {
                    if !group_by.iter().any(|g| g.column_id == cid) {
                        return Err(SenseArcError::Internal(format!(
                            "window column col_{cid} must appear in GROUP BY when combined with aggregation"
                        )));
                    }
                }
            }
        }

        // 3) 收集 `WindowRow.data` 所需数据列：分组键 + 投影列 + 窗口引用列 + 聚合参数/过滤列。
        let mut base_set = BTreeSet::new();
        for g in group_by {
            base_set.insert(g.column_id);
        }
        for e in projection {
            match e {
                ProjectionExpr::Column(col) => {
                    base_set.insert(col.column_id);
                }
                ProjectionExpr::Window(spec) => {
                    match &spec.arg {
                        WindowArg::Column(col) => {
                            base_set.insert(col.column_id);
                        }
                        WindowArg::Aggregate(call) => {
                            if let Some(arg) = &call.arg {
                                collect_agg_arg_columns(arg, &mut base_set);
                            }
                            if let Some(filter) = &call.filter {
                                collect_predicate_columns(filter, &mut base_set);
                            }
                        }
                    }
                    for p in &spec.partition_by {
                        base_set.insert(p.column_id);
                    }
                    for k in &spec.order_by {
                        if let OrderKeyColumn::Column(c) = &k.column {
                            base_set.insert(c.column_id);
                        }
                    }
                }
                ProjectionExpr::Aggregate(call) => {
                    if let Some(arg) = &call.arg {
                        collect_agg_arg_columns(arg, &mut base_set);
                    }
                    if let Some(filter) = &call.filter {
                        collect_predicate_columns(filter, &mut base_set);
                    }
                }
                _ => {}
            }
        }
        let base_ids: Vec<u16> = base_set.into_iter().collect();
        let combined_ids = combine_column_ids(&base_ids, predicate);
        let rows = self
            .collect_multi_rows(tenant_id, table_id, &combined_ids, range, predicate)
            .await?;

        // 4) 构造窗口输入行流 + 聚合结果值（不在此处截断：LIMIT 须在窗口计算后作用于输出）。
        let (mut window_rows, agg_source_types) = if has_aggregate {
            let group_idxs: Vec<usize> = group_by
                .iter()
                .map(|g| idx_of(&combined_ids, g.column_id))
                .collect();
            let source_types = determine_source_types(&agg_specs, &combined_ids, &rows);

            let mut groups: Vec<GroupResult> = Vec::new();
            let mut group_first_ts: Vec<i64> = Vec::new();
            let mut index: HashMap<GroupKey, usize> = HashMap::new();

            if !has_bucket && group_by.is_empty() {
                index.insert(GroupKey(Vec::new()), 0);
                groups.push(GroupResult {
                    key: GroupKey(Vec::new()),
                    states: make_empty_states(&agg_specs, &source_types),
                });
                group_first_ts.push(0);
            }

            for row in &rows {
                let key = build_group_key(row, time_bucket, &group_idxs);
                let gi = match index.get(&key) {
                    Some(&i) => i,
                    None => {
                        let i = groups.len();
                        index.insert(key.clone(), i);
                        groups.push(GroupResult {
                            key,
                            states: make_empty_states(&agg_specs, &source_types),
                        });
                        group_first_ts.push(row.ts);
                        i
                    }
                };
                accumulate_row(&mut groups[gi].states, &agg_specs, &combined_ids, row);
            }

            let bucket_offset = usize::from(has_bucket);
            let mut wrows = Vec::with_capacity(groups.len());
            for (gi, g) in groups.iter().enumerate() {
                let ts = if has_bucket {
                    match &g.key.0[0] {
                        Some(TypedValue::I64(x)) => *x,
                        _ => unreachable!("time bucket key must be I64"),
                    }
                } else {
                    group_first_ts[gi]
                };
                let mut data = HashMap::new();
                for (k, gkey) in group_by.iter().enumerate() {
                    data.insert(gkey.column_id, g.key.0[bucket_offset + k].clone());
                }
                let agg_values: Vec<TypedValue> = agg_specs
                    .iter()
                    .enumerate()
                    .map(|(si, spec)| g.states[si].result(spec.function))
                    .collect();
                wrows.push(WindowRow {
                    ts,
                    data,
                    agg_values,
                });
            }
            (wrows, source_types)
        } else {
            let wrows: Vec<WindowRow> = rows
                .iter()
                .map(|r| {
                    let mut data = HashMap::with_capacity(combined_ids.len());
                    for (i, &cid) in combined_ids.iter().enumerate() {
                        data.insert(cid, r.values[i].clone());
                    }
                    WindowRow {
                        ts: r.ts,
                        data,
                        agg_values: Vec::new(),
                    }
                })
                .collect();
            (wrows, Vec::new())
        };

        // 5) 窗口算子：partition → sort → frame（lead/lag）。
        let mut window_results = compute_window_columns(&window_rows, &window_specs, &agg_specs);

        // LIMIT 作用于窗口计算之后的输出行（保留窗口帧的完整可见性）。
        if let Some(limit) = limit {
            window_rows.truncate(limit);
            for col in &mut window_results {
                col.truncate(limit);
            }
        }

        // 6) 构建 Arrow。
        let batch = build_window_batch(
            &window_rows,
            &window_results,
            &out_specs,
            &window_specs,
            &agg_specs,
            &agg_source_types,
            has_bucket,
        );

        Ok(QueryResult {
            batches: batch.map(|b| vec![b]).unwrap_or_default(),
            warning: None,
        })
    }

    /// 三源合并（多列投影），返回按 `(ts, source)` 排序并去重后的 `QueryRow` 列表。
    ///
    /// 与显式投影 / 通配投影共用；`column_ids` 允许为空（仅投影 `ts` 时）。
    async fn collect_multi_rows(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        column_ids: &[u16],
        range: TsRange,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<QueryRow>, SenseArcError> {
        let mut all_rows: Vec<QueryRow> = Vec::new();

        // 源 1 + 2: MemTable (active + frozen 队列)
        // `snapshot_state` 原子取 pair + `with_chunk` 只读目标分片。
        let (active, frozen_tables) = self.memtable.snapshot_state();
        active.with_chunk(table_id, |chunk| {
            if let Some(chunk) = chunk {
                // S2：tenant 谓词——非本租户 chunk 视为不存在（跳过）。
                if chunk.tenant_id != tenant_id {
                    return;
                }
                collect_chunk_rows_multi(
                    chunk,
                    column_ids,
                    range.start,
                    range.end,
                    0,
                    &mut all_rows,
                );
            }
        });
        // frozen 队列：新→旧 赋递增 source（1..N）。
        for (i, frozen) in frozen_tables.iter().rev().enumerate() {
            let source = i + 1;
            frozen.with_chunk(table_id, |chunk| {
                if let Some(chunk) = chunk {
                    if chunk.tenant_id != tenant_id {
                        return;
                    }
                    collect_chunk_rows_multi(
                        chunk,
                        column_ids,
                        range.start,
                        range.end,
                        source,
                        &mut all_rows,
                    );
                }
            });
        }

        // 源 3: SAC Blocks（跨文件 LWW）。
        let sac_base = frozen_tables.len() + 1;
        collect_sac_rows_multi(
            &self.sac_dir,
            tenant_id,
            table_id,
            column_ids,
            range.start,
            range.end,
            &mut all_rows,
            self.encryptor.clone(),
            sac_base,
            self.partition_duration_ms,
        )?;

        // 排序 + 去重（同 ts 保留 source 较小者）
        all_rows.sort_by(|a, b| a.cmp(b));
        all_rows.dedup_by(|a, b| a.ts == b.ts);

        // ts 边界排他（`>`/`<`）二次过滤：底层扫描为闭区间，此处剔除边界值。
        all_rows.retain(|r| range.contains(r.ts));

        // 列谓词下推：合并去重后按逐行 typed 值求值（NULL → UNKNOWN → 不命中）。
        if let Some(p) = predicate {
            all_rows.retain(|r| eval_predicate(p, column_ids, &r.values));
        }

        Ok(all_rows)
    }

    /// 三源全列收集（通配投影），返回按 `(ts, source)` 排序去重后的 map 行
    /// 与该表在查询范围内实际存在的列 ID 升序并集。
    async fn collect_all_rows(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        range: TsRange,
        predicate: Option<&Predicate>,
    ) -> Result<(Vec<MapRow>, Vec<u16>), SenseArcError> {
        let mut all_rows: Vec<MapRow> = Vec::new();
        let mut col_set: BTreeSet<u16> = BTreeSet::new();

        let (active, frozen_tables) = self.memtable.snapshot_state();
        active.with_chunk(table_id, |chunk| {
            if let Some(chunk) = chunk {
                if chunk.tenant_id != tenant_id {
                    return;
                }
                collect_chunk_rows_all(
                    chunk,
                    range.start,
                    range.end,
                    0,
                    &mut all_rows,
                    &mut col_set,
                );
            }
        });
        for (i, frozen) in frozen_tables.iter().rev().enumerate() {
            let source = i + 1;
            frozen.with_chunk(table_id, |chunk| {
                if let Some(chunk) = chunk {
                    if chunk.tenant_id != tenant_id {
                        return;
                    }
                    collect_chunk_rows_all(
                        chunk,
                        range.start,
                        range.end,
                        source,
                        &mut all_rows,
                        &mut col_set,
                    );
                }
            });
        }

        let sac_base = frozen_tables.len() + 1;
        collect_sac_rows_all(
            &self.sac_dir,
            tenant_id,
            table_id,
            range.start,
            range.end,
            &mut all_rows,
            &mut col_set,
            self.encryptor.clone(),
            sac_base,
            self.partition_duration_ms,
        )?;

        all_rows.sort_by(|a, b| a.cmp(b));
        all_rows.dedup_by(|a, b| a.ts == b.ts);

        // ts 边界排他（`>`/`<`）二次过滤：底层扫描为闭区间，此处剔除边界值。
        all_rows.retain(|r| range.contains(r.ts));

        // 列谓词下推：map 行按列 ID 直接取值求值。
        if let Some(p) = predicate {
            all_rows.retain(|r| eval_predicate_map(p, &r.values));
        }

        Ok((all_rows, col_set.into_iter().collect()))
    }

    /// 从排序行构建 Arrow RecordBatch（按投影吐 N 列，类型保真）。
    ///
    /// - `ts` 伪列 → `Int64`（非空）；
    /// - 数据列 → 按 `TypedValue → Arrow DataType` 映射表构建，`None` 走 null bitmap；
    /// - 标量表达式（CASE）→ 逐行求值后按统一类型构建。
    fn build_record_batch(
        rows: &[QueryRow],
        projected: &[ProjectedColumn],
        column_ids: &[u16],
    ) -> Option<RecordBatch> {
        if rows.is_empty() {
            return None;
        }

        let mut fields: Vec<Field> = Vec::with_capacity(projected.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(projected.len());

        for p in projected {
            if let Some(expr) = &p.expr {
                let raw: Vec<Option<TypedValue>> = rows
                    .iter()
                    .map(|r| eval_scalar(expr, column_ids, &r.values))
                    .collect();
                let dt = unify_value_types(&raw).unwrap_or(PhysicalType::F64);
                let values = coerce_values_to_type(dt, &raw);
                fields.push(Field::new(p.name.clone(), arrow_data_type(dt), true));
                arrays.push(build_typed_array(dt, &values));
                continue;
            }
            match p.source {
                None => {
                    fields.push(Field::new(p.name.clone(), DataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.ts).collect::<Vec<i64>>(),
                    )));
                }
                Some((_column_id, data_index)) => {
                    let raw: Vec<Option<TypedValue>> =
                        rows.iter().map(|r| r.values[data_index].clone()).collect();
                    let dt = unify_value_types(&raw).unwrap_or(PhysicalType::F64);
                    let values = coerce_values_to_type(dt, &raw);
                    fields.push(Field::new(p.name.clone(), arrow_data_type(dt), true));
                    arrays.push(build_typed_array(dt, &values));
                }
            }
        }

        Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).ok()?)
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new(
            Arc::new(MemTableManager::new(&sensearcdb_common::ConfigHandle::new(
                Default::default(),
            ))),
            PathBuf::new(),
        )
    }
}

/// 将显式投影（不含通配）解析为 `(ProjectedColumn 列表, 数据列 ID 列表)`。
fn resolve_explicit_projection(projection: &[ProjectionExpr]) -> (Vec<ProjectedColumn>, Vec<u16>) {
    let mut projected = Vec::with_capacity(projection.len());
    let mut data_ids = Vec::new();

    for expr in projection {
        match expr {
            ProjectionExpr::Timestamp => projected.push(ProjectedColumn {
                name: "ts".to_string(),
                source: None,
                expr: None,
            }),
            ProjectionExpr::Column(col) => {
                let idx = data_ids.len();
                data_ids.push(col.column_id);
                projected.push(ProjectedColumn {
                    name: format!("col_{}", col.column_id),
                    source: Some((col.column_id, idx)),
                    expr: None,
                });
            }
            ProjectionExpr::Expr(scalar) => {
                // 标量表达式（CASE）：收集其引用的所有列，供 collect + 求值。
                let mut refs = BTreeSet::new();
                collect_scalar_columns(scalar, &mut refs);
                for cid in refs {
                    if !data_ids.contains(&cid) {
                        data_ids.push(cid);
                    }
                }
                projected.push(ProjectedColumn {
                    name: "case".to_string(),
                    source: None,
                    expr: Some(scalar.clone()),
                });
            }
            ProjectionExpr::Wildcard => {
                // 调用方（execute_plan）已把通配路径分流；此处防御性忽略。
                continue;
            }
            ProjectionExpr::Aggregate(_) => {
                // 聚合路径由 execute_aggregate 处理；此函数仅服务显式列投影。
                continue;
            }
            ProjectionExpr::Window(_) => {
                // 窗口路径由 execute_window 处理；此函数仅服务显式列投影。
                continue;
            }
        }
    }

    (projected, data_ids)
}

/// 将投影列与谓词引用列合并为实际收集列 ID 列表（投影列在前，谓词独有列追加在后）。
fn combine_column_ids(base: &[u16], predicate: Option<&Predicate>) -> Vec<u16> {
    let mut ids = base.to_vec();
    if let Some(p) = predicate {
        let mut extra = BTreeSet::new();
        collect_predicate_columns(p, &mut extra);
        for &cid in &extra {
            if !ids.contains(&cid) {
                ids.push(cid);
            }
        }
    }
    ids
}

/// 收集谓词树中所有叶子引用的列 ID。
fn collect_predicate_columns(pred: &Predicate, out: &mut BTreeSet<u16>) {
    match pred {
        Predicate::Comparison { column, .. }
        | Predicate::In { column, .. }
        | Predicate::Between { column, .. } => {
            out.insert(*column);
        }
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            collect_predicate_columns(l, out);
            collect_predicate_columns(r, out);
        }
    }
}

/// 收集标量表达式引用的所有列 ID（CASE 条件 + THEN/ELSE 值 + 嵌套 CASE）。
fn collect_scalar_columns(expr: &ScalarExpr, out: &mut BTreeSet<u16>) {
    match expr {
        ScalarExpr::Literal(_) => {}
        ScalarExpr::Column(col) => {
            out.insert(col.column_id);
        }
        ScalarExpr::Case(case) => {
            for (cond, then_expr) in &case.branches {
                collect_predicate_columns(cond, out);
                collect_scalar_columns(then_expr, out);
            }
            if let Some(else_expr) = &case.else_expr {
                collect_scalar_columns(else_expr, out);
            }
        }
    }
}

/// 收集聚合参数引用的所有列 ID。
fn collect_agg_arg_columns(arg: &AggArg, out: &mut BTreeSet<u16>) {
    match arg {
        AggArg::Column(col) => {
            out.insert(col.column_id);
        }
        AggArg::Expr(expr) => collect_scalar_columns(expr, out),
    }
}

/// 对逐列 `QueryRow.values`（对齐 `column_ids`）求值谓词。
fn eval_predicate(pred: &Predicate, column_ids: &[u16], values: &[Option<TypedValue>]) -> bool {
    eval_predicate_with(pred, &mut |cid| {
        column_ids
            .iter()
            .position(|&c| c == cid)
            .and_then(|i| values.get(i))
            .cloned()
            .flatten()
    })
}

/// 对通配 `MapRow.values`（`column_id → Option<TypedValue>`）求值谓词。
fn eval_predicate_map(pred: &Predicate, values: &HashMap<u16, Option<TypedValue>>) -> bool {
    eval_predicate_with(pred, &mut |cid| values.get(&cid).cloned().flatten())
}

/// 谓词求值核心：`resolve` 把 `column_id` 解析为 `Option<TypedValue>`（缺失 → `None`）。
fn eval_predicate_with<F>(pred: &Predicate, resolve: &mut F) -> bool
where
    F: FnMut(u16) -> Option<TypedValue>,
{
    match pred {
        Predicate::Comparison {
            column,
            op,
            literal,
        } => {
            let col = resolve(*column);
            compare_leaf(*op, col.as_ref(), literal)
        }
        Predicate::And(l, r) => eval_predicate_with(l, resolve) && eval_predicate_with(r, resolve),
        Predicate::Or(l, r) => eval_predicate_with(l, resolve) || eval_predicate_with(r, resolve),
        Predicate::In { column, values } => {
            let col = resolve(*column);
            values
                .iter()
                .any(|lit| compare_leaf(CmpOp::Eq, col.as_ref(), lit))
        }
        Predicate::Between { column, low, high } => {
            let col = resolve(*column);
            compare_leaf(CmpOp::GtEq, col.as_ref(), low)
                && compare_leaf(CmpOp::LtEq, col.as_ref(), high)
        }
    }
}

/// 单次叶子比较：`None`（NULL）恒不命中（SQL 三值逻辑）。
fn compare_leaf(op: CmpOp, col: Option<&TypedValue>, literal: &TypedValue) -> bool {
    match col {
        None => false,
        Some(v) => cmp_op(op, cmp_typed(v, literal)),
    }
}

/// 把比较结果（`Option<Ordering>`）映射为布尔；`None` = 不可比较。
fn cmp_op(op: CmpOp, ordering: Option<std::cmp::Ordering>) -> bool {
    use std::cmp::Ordering;
    match op {
        CmpOp::Eq => matches!(ordering, Some(Ordering::Equal)),
        CmpOp::Ne => !matches!(ordering, Some(Ordering::Equal)),
        CmpOp::Lt => matches!(ordering, Some(Ordering::Less)),
        CmpOp::LtEq => matches!(ordering, Some(Ordering::Less) | Some(Ordering::Equal)),
        CmpOp::Gt => matches!(ordering, Some(Ordering::Greater)),
        CmpOp::GtEq => matches!(ordering, Some(Ordering::Greater) | Some(Ordering::Equal)),
    }
}

/// 对逐列 `QueryRow.values`（对齐 `column_ids`）求值标量表达式（P5 CASE）。
fn eval_scalar(
    expr: &ScalarExpr,
    column_ids: &[u16],
    values: &[Option<TypedValue>],
) -> Option<TypedValue> {
    match expr {
        ScalarExpr::Literal(v) => Some(v.clone()),
        ScalarExpr::Column(col) => column_ids
            .iter()
            .position(|&c| c == col.column_id)
            .and_then(|i| values.get(i))
            .cloned()
            .flatten(),
        ScalarExpr::Case(case) => eval_case(case, column_ids, values),
    }
}

/// 按顺序短路求值 CASE WHEN：首个满足条件的分支值；否则 ELSE（缺省 NULL）。
fn eval_case(
    case: &CaseExpr,
    column_ids: &[u16],
    values: &[Option<TypedValue>],
) -> Option<TypedValue> {
    for (cond, then_expr) in &case.branches {
        if eval_predicate(cond, column_ids, values) {
            return eval_scalar(then_expr, column_ids, values);
        }
    }
    match &case.else_expr {
        Some(e) => eval_scalar(e, column_ids, values),
        None => None,
    }
}

/// `TypedValue` 类型保真比较：数值走 `Numeric`（同变体精确、跨类型退化 f64），
/// 非数值（Bool/String/Enum16）仅支持同变体比较。
fn cmp_typed(a: &TypedValue, b: &TypedValue) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if let (Some(x), Some(y)) = (to_numeric(a), to_numeric(b)) {
        return Some(if Numeric::equal(&x, &y) {
            Ordering::Equal
        } else if Numeric::less(&x, &y) {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    match (a, b) {
        (TypedValue::Bool(x), TypedValue::Bool(y)) => Some(x.cmp(y)),
        (TypedValue::String(x), TypedValue::String(y)) => Some(x.cmp(y)),
        (TypedValue::Enum16(x), TypedValue::Enum16(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// 将数值型 `TypedValue` 提取为 `Numeric`（非数值返回 `None`）。
fn to_numeric(v: &TypedValue) -> Option<Numeric> {
    match v {
        TypedValue::F64(x) => Some(Numeric::F64(*x)),
        TypedValue::F32(x) => Some(Numeric::F32(*x)),
        TypedValue::I64(x) => Some(Numeric::I64(*x)),
        TypedValue::U64(x) => Some(Numeric::U64(*x)),
        TypedValue::I32(x) => Some(Numeric::I32(*x)),
        _ => None,
    }
}

// ─── P3 聚合辅助 ───

/// 聚合查询的输出列描述。
enum AggOut {
    /// 分组键列（输出该组的分组键值）。
    Group(u16),
    /// 聚合列（`usize` 为 `agg_specs` 下标）。
    Aggregate(usize),
}

/// 分组聚合中间结果。
struct GroupResult {
    /// 分组键（用于输出分组键列）。
    key: GroupKey,
    /// 每个聚合函数对应的增量聚合状态。
    states: Vec<AggregateState>,
}

// ─── P6 窗口辅助 ───

/// 窗口查询的输出列描述。
enum WindowOut {
    /// ts 伪列（无聚合窗口路径）。
    Timestamp,
    /// 数据列（无聚合为原始列；聚合态为分组键列）。
    Column(u16),
    /// 聚合结果列（`usize` 为 `agg_specs` 下标）。
    Aggregate(usize),
    /// 窗口结果列（`usize` 为 `window_specs` 下标）。
    Window(usize),
}

/// 窗口算子输入行：`ts` + 数据列（分组键/原始列）+ 聚合结果值。
struct WindowRow {
    /// 时间戳（原始行 ts，或聚合结果行的桶起始/组首行 ts）。
    ts: i64,
    /// 数据列值（按键对齐）——供 `arg`/`partition_by`/`order_by` 与普通列投影引用。
    data: HashMap<u16, Option<TypedValue>>,
    /// 聚合结果值（与 `agg_specs` 对齐；无聚合时为空）。
    agg_values: Vec<TypedValue>,
}

/// 在有序行流上执行窗口函数（lead/lag），返回每个窗口规格的结果列（与 `rows` 对齐）。
fn compute_window_columns(
    rows: &[WindowRow],
    specs: &[WindowSpec],
    agg_specs: &[AggCall],
) -> Vec<Vec<Option<TypedValue>>> {
    specs
        .iter()
        .map(|spec| compute_window_column(rows, spec, agg_specs))
        .collect()
}

/// 单窗口规格的列计算：partition → sort → frame。
///
/// 结果与 `rows` 对齐（输出位置 = 输入行位置，不重排输出）。
fn compute_window_column(
    rows: &[WindowRow],
    spec: &WindowSpec,
    agg_specs: &[AggCall],
) -> Vec<Option<TypedValue>> {
    let n = rows.len();
    let mut result = vec![None; n];
    let offset = spec.offset.max(1) as usize;

    // 1) 按分区键划分分区（空 partition_by = 单分区）。
    let mut partitions: HashMap<GroupKey, Vec<usize>> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let key = window_partition_key(row, &spec.partition_by);
        partitions.entry(key).or_default().push(i);
    }

    // 2) 分区内排序 + 前向/后向帧取值。
    for indices in partitions.values_mut() {
        sort_window_indices(indices, rows, &spec.order_by);
        for (pos, &row_idx) in indices.iter().enumerate() {
            let target_pos = match spec.func {
                WindowFunction::Lead => pos.checked_add(offset),
                WindowFunction::Lag => pos.checked_sub(offset),
            };
            match target_pos.and_then(|p| indices.get(p)).copied() {
                Some(target_idx) => {
                    // 命中帧内行：取 arg 值（数据列 或 聚合结果列）；源为 None → 传播 None。
                    result[row_idx] = window_arg_value(&rows[target_idx], &spec.arg, agg_specs);
                }
                None => {
                    // 越界（lead 越过分区尾 / lag 越过分区首）→ default（未指定 NULL）。
                    result[row_idx] = spec.default.clone();
                }
            }
        }
    }

    result
}

/// 取某行的窗口参数值：数据列 → `data`；聚合 → `agg_values`（按值匹配索引）。
fn window_arg_value(row: &WindowRow, arg: &WindowArg, agg_specs: &[AggCall]) -> Option<TypedValue> {
    match arg {
        WindowArg::Column(col) => row.data.get(&col.column_id).cloned().flatten(),
        WindowArg::Aggregate(call) => {
            let idx = agg_specs.iter().position(|s| s == call)?;
            row.agg_values.get(idx).cloned()
        }
    }
}

/// 由分区键构造 `GroupKey`（复用浮点归一化 Eq/Hash）。
fn window_partition_key(row: &WindowRow, partition_by: &[ColumnRef]) -> GroupKey {
    let vals = partition_by
        .iter()
        .map(|c| row.data.get(&c.column_id).cloned().flatten())
        .collect();
    GroupKey(vals)
}

/// 分区内按 `order_by` 排序索引；空 `order_by` 退化 ts 自然升序（输入已 ts 有序）。
fn sort_window_indices(indices: &mut [usize], rows: &[WindowRow], order_by: &[OrderKey]) {
    if order_by.is_empty() {
        indices.sort_by_key(|&i| rows[i].ts);
        return;
    }
    indices.sort_by(|&a, &b| {
        for key in order_by {
            let va = window_order_value(&rows[a], key);
            let vb = window_order_value(&rows[b], key);
            let ord = cmp_window_values(&va, &vb);
            if ord != std::cmp::Ordering::Equal {
                return if key.asc { ord } else { ord.reverse() };
            }
        }
        // 稳定 tie-break：ts 升序。
        rows[a].ts.cmp(&rows[b].ts)
    });
}

/// 取某行在指定排序键上的值。
fn window_order_value(row: &WindowRow, key: &OrderKey) -> Option<TypedValue> {
    match &key.column {
        OrderKeyColumn::Ts => Some(TypedValue::I64(row.ts)),
        OrderKeyColumn::Column(col) => row.data.get(&col.column_id).cloned().flatten(),
    }
}

/// 窗口排序值比较：`None`（NULL）恒排最前（确定性约定）。
fn cmp_window_values(a: &Option<TypedValue>, b: &Option<TypedValue>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => cmp_typed(x, y).unwrap_or(Ordering::Equal),
    }
}

/// 构建窗口查询的 Arrow RecordBatch（含隐式时间桶首列）。
#[allow(clippy::too_many_arguments)]
fn build_window_batch(
    rows: &[WindowRow],
    window_results: &[Vec<Option<TypedValue>>],
    out_specs: &[WindowOut],
    window_specs: &[WindowSpec],
    agg_specs: &[AggCall],
    agg_source_types: &[PhysicalType],
    has_bucket: bool,
) -> Option<RecordBatch> {
    if rows.is_empty() {
        return None;
    }

    let bucket_offset = usize::from(has_bucket);
    let mut fields: Vec<Field> = Vec::with_capacity(out_specs.len() + bucket_offset);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(out_specs.len() + bucket_offset);

    if has_bucket {
        let ts_values: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        fields.push(Field::new("ts".to_string(), DataType::Int64, false));
        arrays.push(Arc::new(Int64Array::from(ts_values)));
    }

    for out in out_specs {
        match out {
            WindowOut::Timestamp => {
                let ts_values: Vec<i64> = rows.iter().map(|r| r.ts).collect();
                fields.push(Field::new("ts".to_string(), DataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(ts_values)));
            }
            WindowOut::Column(cid) => {
                let raw: Vec<Option<TypedValue>> = rows
                    .iter()
                    .map(|r| r.data.get(cid).cloned().flatten())
                    .collect();
                let dt = unify_value_types(&raw).unwrap_or(PhysicalType::F64);
                let values = coerce_values_to_type(dt, &raw);
                fields.push(Field::new(
                    format!("col_{}", cid),
                    arrow_data_type(dt),
                    true,
                ));
                arrays.push(build_typed_array(dt, &values));
            }
            WindowOut::Aggregate(si) => {
                let spec = &agg_specs[*si];
                let result_type = agg_result_type(spec.function, agg_source_types[*si]);
                let values: Vec<Option<TypedValue>> = rows
                    .iter()
                    .map(|r| Some(r.agg_values[*si].clone()))
                    .collect();
                fields.push(Field::new(
                    agg_column_name(spec),
                    arrow_data_type(result_type),
                    false,
                ));
                arrays.push(build_typed_array(result_type, &values));
            }
            WindowOut::Window(wi) => {
                let raw = &window_results[*wi];
                let dt = unify_value_types(raw).unwrap_or(PhysicalType::F64);
                let values = coerce_values_to_type(dt, raw);
                fields.push(Field::new(
                    window_column_name(&window_specs[*wi]),
                    arrow_data_type(dt),
                    true,
                ));
                arrays.push(build_typed_array(dt, &values));
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).ok()
}

/// 窗口结果列名（如 `lead(col_0)` / `lag(avg(col_0))`）。
fn window_column_name(spec: &WindowSpec) -> String {
    let fn_name = match spec.func {
        WindowFunction::Lead => "lead",
        WindowFunction::Lag => "lag",
    };
    let arg_name = match &spec.arg {
        WindowArg::Column(col) => format!("col_{}", col.column_id),
        WindowArg::Aggregate(call) => agg_column_name(call),
    };
    format!("{fn_name}({arg_name})")
}

/// 分组键：`Vec<Option<TypedValue>>` 的 Eq/Hash 安全包装。
///
/// 归一化浮点 `-0.0` 与 `NaN`，避免「`0.0 == -0.0` 但 hash 不同」破坏
/// `HashMap` 契约；`NaN` 视为与自身相等（SQL 分组直觉：同组聚合）。
#[derive(Debug, Clone)]
struct GroupKey(Vec<Option<TypedValue>>);

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(a, b)| typed_value_eq(a, b))
    }
}

impl Eq for GroupKey {}

impl std::hash::Hash for GroupKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for v in &self.0 {
            hash_typed_value(v, state);
        }
    }
}

/// 逐列求值相等（`Option` 层）。
fn typed_value_eq(a: &Option<TypedValue>, b: &Option<TypedValue>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => typed_eq(x, y),
        _ => false,
    }
}

/// `TypedValue` 相等（浮点归一化 `-0.0`/`NaN`）。
fn typed_eq(a: &TypedValue, b: &TypedValue) -> bool {
    use TypedValue::*;
    match (a, b) {
        (F64(x), F64(y)) => (x == y) || (x.is_nan() && y.is_nan()),
        (F32(x), F32(y)) => (x == y) || (x.is_nan() && y.is_nan()),
        (I64(x), I64(y)) => x == y,
        (U64(x), U64(y)) => x == y,
        (I32(x), I32(y)) => x == y,
        (Bool(x), Bool(y)) => x == y,
        (String(x), String(y)) => x == y,
        (Enum16(x), Enum16(y)) => x == y,
        _ => false,
    }
}

/// 逐列 hash（与 [`typed_value_eq`] 保持一致）。
fn hash_typed_value<H: std::hash::Hasher>(v: &Option<TypedValue>, state: &mut H) {
    match v {
        None => state.write_u8(0),
        Some(TypedValue::F64(x)) => {
            state.write_u8(1);
            state.write_u64(normalize_f64(*x).to_bits());
        }
        Some(TypedValue::F32(x)) => {
            state.write_u8(2);
            state.write_u32(normalize_f32(*x).to_bits());
        }
        Some(TypedValue::I64(x)) => {
            state.write_u8(3);
            state.write_i64(*x);
        }
        Some(TypedValue::U64(x)) => {
            state.write_u8(4);
            state.write_u64(*x);
        }
        Some(TypedValue::I32(x)) => {
            state.write_u8(5);
            state.write_i32(*x);
        }
        Some(TypedValue::Bool(x)) => {
            state.write_u8(6);
            state.write_u8(*x as u8);
        }
        Some(TypedValue::String(s)) => {
            state.write_u8(7);
            state.write(s.as_bytes());
            state.write_u8(0);
        }
        Some(TypedValue::Enum16(x)) => {
            state.write_u8(8);
            state.write_u16(*x);
        }
    }
}

/// 归一化 `f64`（`-0.0` → `0.0`；`NaN` 保持 `NaN`）。
fn normalize_f64(x: f64) -> f64 {
    if x.is_nan() {
        f64::NAN
    } else if x == 0.0 {
        0.0
    } else {
        x
    }
}

/// 归一化 `f32`（`-0.0` → `0.0`；`NaN` 保持 `NaN`）。
fn normalize_f32(x: f32) -> f32 {
    if x.is_nan() {
        f32::NAN
    } else if x == 0.0 {
        0.0
    } else {
        x
    }
}

/// 构造空聚合状态（源类型已知后）。
fn make_empty_states(specs: &[AggCall], source_types: &[PhysicalType]) -> Vec<AggregateState> {
    specs
        .iter()
        .enumerate()
        .map(|(i, _)| AggregateState::new(source_types[i]))
        .collect()
}

/// 确定每个聚合的源类型：扫描全部非 NULL 参数值并统一类型（CASE 分支混合时提升 F64）。
fn determine_source_types(
    specs: &[AggCall],
    column_ids: &[u16],
    rows: &[QueryRow],
) -> Vec<PhysicalType> {
    specs
        .iter()
        .map(|spec| match spec.function {
            AggFunction::Count => PhysicalType::U64,
            _ => {
                let mut unified: Option<PhysicalType> = None;
                for row in rows {
                    if let Some(v) = agg_value(spec, column_ids, &row.values) {
                        let t = v.data_type();
                        unified = Some(match unified {
                            None => t,
                            Some(prev) => unify_physical_types(prev, t),
                        });
                    }
                }
                unified.unwrap_or(PhysicalType::F64)
            }
        })
        .collect()
}

/// 对单行累加进对应分组的各聚合状态（含 FILTER 谓词预过滤）。
fn accumulate_row(
    states: &mut [AggregateState],
    specs: &[AggCall],
    column_ids: &[u16],
    row: &QueryRow,
) {
    for (si, spec) in specs.iter().enumerate() {
        // P5：`FILTER (WHERE ...)` —— 不满足（FALSE/UNKNOWN）的行不参与该聚合。
        if let Some(filter) = &spec.filter {
            if !eval_predicate(filter, column_ids, &row.values) {
                continue;
            }
        }
        match spec.function {
            AggFunction::Count => match &spec.arg {
                None => {
                    // COUNT(*) FILTER：计数「满足 filter 的行」。
                    states[si].count += 1;
                }
                Some(arg) => {
                    // COUNT(expr) FILTER：仅计数满足 filter 且值为非 NULL 的行。
                    if eval_agg_arg(arg, column_ids, &row.values).is_some() {
                        states[si].count += 1;
                    }
                }
            },
            _ => {
                if let Some(arg) = &spec.arg {
                    if let Some(v) = eval_agg_arg(arg, column_ids, &row.values) {
                        states[si].accumulate(&v);
                    }
                }
            }
        }
    }
}

/// 聚合结果列名（如 `count(*)` / `sum(col_1)` / `sum(case)`）。
fn agg_column_name(call: &AggCall) -> String {
    let fn_name = match call.function {
        AggFunction::Count => "count",
        AggFunction::Avg => "avg",
        AggFunction::Sum => "sum",
        AggFunction::Min => "min",
        AggFunction::Max => "max",
        AggFunction::First => "first",
        AggFunction::Last => "last",
        AggFunction::StdDev => "stddev",
    };
    match &call.arg {
        None => format!("{fn_name}(*)"),
        Some(AggArg::Column(col)) => format!("{fn_name}(col_{})", col.column_id),
        Some(AggArg::Expr(_)) => format!("{fn_name}(case)"),
    }
}

/// 求值聚合参数值（`AggArg`）：列 → 直接取值；表达式 → `eval_scalar`。
fn eval_agg_arg(
    arg: &AggArg,
    column_ids: &[u16],
    values: &[Option<TypedValue>],
) -> Option<TypedValue> {
    match arg {
        AggArg::Column(col) => column_ids
            .iter()
            .position(|&c| c == col.column_id)
            .and_then(|i| values.get(i))
            .cloned()
            .flatten(),
        AggArg::Expr(expr) => eval_scalar(expr, column_ids, values),
    }
}

/// 计算聚合参数的每行值（`COUNT(*)` 为 `None`）。
fn agg_value(
    spec: &AggCall,
    column_ids: &[u16],
    values: &[Option<TypedValue>],
) -> Option<TypedValue> {
    match &spec.arg {
        None => None,
        Some(arg) => eval_agg_arg(arg, column_ids, values),
    }
}

/// 统一一组值的物理类型（用于 CASE 分支类型统一与列类型确定）。
fn unify_value_types(values: &[Option<TypedValue>]) -> Option<PhysicalType> {
    let mut unified: Option<PhysicalType> = None;
    for v in values.iter().flatten() {
        let t = v.data_type();
        unified = Some(match unified {
            None => t,
            Some(prev) => unify_physical_types(prev, t),
        });
    }
    unified
}

/// 合并两个物理类型：同类型保持不变；不同类型提升为 F64（`AggregateState` 通用兜底域）。
fn unify_physical_types(a: PhysicalType, b: PhysicalType) -> PhysicalType {
    if a == b {
        a
    } else {
        PhysicalType::F64
    }
}

/// 把一组值统一到目标物理类型（避免 `build_typed_array` 严格变体匹配把异型值丢成 NULL）。
///
/// 同类型值原样保留；目标为 F64 时数值型（F32/I64/U64/I32）提升为 `F64(as_f64)`。
fn coerce_values_to_type(
    dt: PhysicalType,
    values: &[Option<TypedValue>],
) -> Vec<Option<TypedValue>> {
    values
        .iter()
        .map(|v| v.as_ref().and_then(|x| promote_to_type(x, dt)))
        .collect()
}

/// 单值类型提升：已匹配目标类型则原样返回；F64 目标下数值型提升为 f64，其余不转换。
fn promote_to_type(v: &TypedValue, dt: PhysicalType) -> Option<TypedValue> {
    if v.data_type() == dt {
        return Some(v.clone());
    }
    if dt == PhysicalType::F64 {
        match v {
            TypedValue::F64(x) => Some(TypedValue::F64(*x)),
            TypedValue::F32(x) => Some(TypedValue::F64(*x as f64)),
            TypedValue::I64(x) => Some(TypedValue::F64(*x as f64)),
            TypedValue::U64(x) => Some(TypedValue::F64(*x as f64)),
            TypedValue::I32(x) => Some(TypedValue::F64(*x as f64)),
            _ => None,
        }
    } else {
        None
    }
}

/// 取列 ID 在 `ids` 中的下标（`execute_aggregate` 保证列必存在）。
fn idx_of(ids: &[u16], cid: u16) -> usize {
    ids.iter()
        .position(|&x| x == cid)
        .expect("column id must be present in collected ids")
}

/// 构造分组键：时间桶（若存在）→ `Some(I64(bucket_start))` 作为首分量，后接非时间分组列值。
fn build_group_key(
    row: &QueryRow,
    time_bucket: Option<&TimeBucket>,
    group_idxs: &[usize],
) -> GroupKey {
    let mut vals = Vec::with_capacity(group_idxs.len() + usize::from(time_bucket.is_some()));
    if let Some(b) = time_bucket {
        vals.push(Some(TypedValue::I64(bucket_start(row.ts, b))));
    }
    for &i in group_idxs {
        vals.push(row.values[i].clone());
    }
    GroupKey(vals)
}

/// 计算 ts 所属时间桶的起始时间戳（epoch 纳秒）。
///
/// 日历桶锚点（设计 §4.4 正确性关键）：
/// - `Epoch`（10s/1m/1h）：`floor(ts / interval) * interval`；
/// - `Day`（1d）：epoch 锚定 = UTC 午夜（Unix epoch 恰为 UTC 00:00:00）；
/// - `Week`（1w）：ISO 周一锚定；
/// - `Month`（1mon）：UTC 月首锚定（civil date 算法）。
fn bucket_start(ts: i64, bucket: &TimeBucket) -> i64 {
    const DAY_NS: i64 = 86_400_000_000_000;
    match bucket.align {
        TimeAlign::Epoch => ts.div_euclid(bucket.interval_ns) * bucket.interval_ns,
        TimeAlign::Day => ts.div_euclid(DAY_NS) * DAY_NS,
        TimeAlign::Week => {
            let days = ts.div_euclid(DAY_NS);
            // 1970-01-01（day 0）是周四，`+3` 使 `rem_euclid(7)` 对齐到 ISO 周一。
            let monday = days - (days + 3).rem_euclid(7);
            monday * DAY_NS
        }
        TimeAlign::Month => {
            let days = ts.div_euclid(DAY_NS);
            let (y, m, _) = civil_from_days(days);
            days_from_civil(y, m, 1) * DAY_NS
        }
    }
}

/// Howard Hinnant civil date：`(年, 月, 日)` → 自 1970-01-01 起的天数。
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = ((m as i64 + 9) % 12) as i64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Howard Hinnant civil date：自 1970-01-01 起的天数 → `(年, 月, 日)`。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m_i64 = if mp < 10 { mp + 3 } else { mp - 9 };
    let m = m_i64 as u32; // [1, 12]
    let y_final = if m <= 2 { y + 1 } else { y };
    (y_final, m, d)
}

/// `TypedValue`（`PhysicalType`）→ Arrow `DataType` 映射（设计 §4.1）。
fn arrow_data_type(dt: PhysicalType) -> DataType {
    match dt {
        PhysicalType::F64 => DataType::Float64,
        PhysicalType::F32 => DataType::Float32,
        PhysicalType::I64 => DataType::Int64,
        PhysicalType::U64 => DataType::UInt64,
        PhysicalType::I32 => DataType::Int32,
        PhysicalType::Bool => DataType::Boolean,
        PhysicalType::String => DataType::Utf8,
        PhysicalType::Enum16 => DataType::UInt16,
    }
}

/// 按确定好的物理类型把逐行 `Option<TypedValue>` 构建为 Arrow 数组。
///
/// `None` → null bitmap；类型不匹配的防御性兜底同样落 null（正常数据列类型恒定，
/// 该分支只在数据损坏时触发，绝不静默折 f64）。
fn build_typed_array(dt: PhysicalType, values: &[Option<TypedValue>]) -> ArrayRef {
    match dt {
        PhysicalType::F64 => {
            let v: Vec<Option<f64>> = values
                .iter()
                .map(|x| match x {
                    Some(TypedValue::F64(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(Float64Array::from(v))
        }
        PhysicalType::F32 => {
            let v: Vec<Option<f32>> = values
                .iter()
                .map(|x| match x {
                    Some(TypedValue::F32(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(Float32Array::from(v))
        }
        PhysicalType::I64 => {
            let v: Vec<Option<i64>> = values
                .iter()
                .map(|x| match x {
                    Some(TypedValue::I64(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(Int64Array::from(v))
        }
        PhysicalType::U64 => {
            let v: Vec<Option<u64>> = values
                .iter()
                .map(|x| match x {
                    Some(TypedValue::U64(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(UInt64Array::from(v))
        }
        PhysicalType::I32 => {
            let v: Vec<Option<i32>> = values
                .iter()
                .map(|x| match x {
                    Some(TypedValue::I32(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(Int32Array::from(v))
        }
        PhysicalType::Bool => {
            let v: Vec<Option<bool>> = values
                .iter()
                .map(|x| match x {
                    Some(TypedValue::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect();
            Arc::new(BooleanArray::from(v))
        }
        PhysicalType::String => {
            let mut builder = StringBuilder::new();
            for x in values {
                match x {
                    Some(TypedValue::String(s)) => builder.append_value(s.as_str()),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        PhysicalType::Enum16 => {
            let v: Vec<Option<u16>> = values
                .iter()
                .map(|x| match x {
                    Some(TypedValue::Enum16(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(UInt16Array::from(v))
        }
    }
}

/// 收集单个 Chunk 中命中时间范围的多列数据行（列值对齐 `column_ids`）。
fn collect_chunk_rows_multi(
    chunk: &ColumnarChunk,
    column_ids: &[u16],
    ts_start: i64,
    ts_end: i64,
    source: usize,
    out: &mut Vec<QueryRow>,
) {
    for (ts, row_idx) in chunk.get_range(ts_start, ts_end) {
        let values: Vec<Option<TypedValue>> = column_ids
            .iter()
            .map(|&cid| chunk.get_typed_value(cid, row_idx))
            .collect();
        out.push(QueryRow { ts, values, source });
    }
}

/// 收集单个 Chunk 中命中时间范围的全列数据行（列值按键对齐），并把列 ID 并入并集。
fn collect_chunk_rows_all(
    chunk: &ColumnarChunk,
    ts_start: i64,
    ts_end: i64,
    source: usize,
    out: &mut Vec<MapRow>,
    col_set: &mut BTreeSet<u16>,
) {
    let mut ids: Vec<u16> = chunk.columns.keys().copied().collect();
    ids.sort_unstable();
    col_set.extend(ids.iter().copied());

    for (ts, row_idx) in chunk.get_range(ts_start, ts_end) {
        let values: HashMap<u16, Option<TypedValue>> = ids
            .iter()
            .map(|&cid| (cid, chunk.get_typed_value(cid, row_idx)))
            .collect();
        out.push(MapRow { ts, values, source });
    }
}

/// SAC 文件「枚举 + 读」的重试上限。
///
/// 查询与 compaction 可能并发：compaction 的 copy-on-write 先 `finish()`（rename 出
/// 新 `.sac`）再 `delete_files()`（删源 `.sac`）。若查询在「枚举 → 读 header/body」
/// 的间隙正好撞上源文件被删，会看到 `File::open` NotFound。此时整轮重扫目录重试，
/// 改读替换后的新文件，杜绝静默丢点（`qa_restart_new_writes_no_data_loss` flaky 根因）。
const SAC_READ_MAX_RETRY: usize = 3;

/// 列出某表在时间范围内命中的 .sac 文件路径，按 `(write_epoch DESC, seq DESC)` 排序。
///
/// 目录级时间剪枝（收益落点）：分区目录名即 `partition_ns`（数据时间天边界），据此
/// O(1) 判定 `[partition_ns, partition_ns+duration)` 是否与 `[ts_start, ts_end)` 重叠，
/// 只枚举命中的分区目录，跳过无关天分区。
///
/// belt-and-suspenders：header.tenant_id 与本租户不符的文件直接剔除（读段即不混租户）。
///
/// copy-on-write 容错：`read_dir` 枚举出的文件若在 `read_header` 前被并发 compaction
/// 删除，本函数重扫目录重试，而不是把该文件静默跳过（静默跳过 = 丢点）。
fn list_sac_files_sorted(
    sac_dir: &PathBuf,
    tenant_id: TenantId,
    table_id: u32,
    ts_start: i64,
    ts_end: i64,
    duration_ms: u64,
) -> Result<Vec<PathBuf>, SenseArcError> {
    let _ = table_id; // v9 文件多表聚合，按 table_id 过滤下沉到 read_range；此处仅保留签名。

    for _ in 0..SAC_READ_MAX_RETRY {
        // v9：路径只留 tenant 一层，文件名 `{partition_ns}_{level}_{seq}.sac`。
        let tenant_dir = sac_dir.join(tenant_id.as_u32().to_string());
        if !tenant_dir.exists() {
            return Ok(Vec::new());
        }

        let duration_ns = (duration_ms as i64).saturating_mul(1_000_000);

        let mut file_paths: Vec<PathBuf> = Vec::new();
        let files = match std::fs::read_dir(&tenant_dir) {
            Ok(d) => d,
            Err(_) => return Ok(Vec::new()),
        };
        for file_entry in files.flatten() {
            let file_path = file_entry.path();
            if file_path.extension().and_then(|e| e.to_str()) != Some("sac") {
                continue;
            }
            // 文件名级时间剪枝：文件名首段即 partition_ns（数据时间边界）。
            let partition_ns: i64 = match file_path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|n| SacNaming::parse_file_name(n).map(|(pn, _, _)| pn))
            {
                Some(pn) => pn,
                None => continue,
            };
            let partition_end = partition_ns.saturating_add(duration_ns);
            // 重叠判定：`partition_ns < ts_end && partition_end > ts_start`。
            if partition_ns >= ts_end || partition_end <= ts_start {
                continue;
            }
            file_paths.push(file_path);
        }

        let mut file_meta: Vec<(u64, u16, PathBuf)> = Vec::with_capacity(file_paths.len());
        let mut vanished = false;
        for path in file_paths {
            match SacReader::read_header(&path) {
                Ok(h) => {
                    if h.tenant_id == tenant_id.as_u32() {
                        file_meta.push((h.write_epoch, h.seq, path));
                    }
                }
                Err(_) => {
                    // 枚举后被并发 compaction 删除（copy-on-write 源清理）→ 重扫重试。
                    if !path.exists() {
                        vanished = true;
                        break;
                    }
                    // 其他读错误（损坏等）：保留旧行为，跳过该文件。
                }
            }
        }

        if !vanished {
            file_meta.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
            return Ok(file_meta.into_iter().map(|(_, _, p)| p).collect());
        }
    }

    // 重试耗尽：目录持续剧烈变动（compaction 连续删除）。宁可显式报错，也不返回
    // 半成品文件列表造成漏读。
    Err(SenseArcError::Internal(
        "SAC file list retry exhausted due to concurrent compaction".into(),
    ))
}

/// 收集 SAC 文件中指定表的多列数据行（列值对齐 `column_ids`）。
///
/// 跨文件 LWW：按 `(write_epoch DESC, seq DESC)` 排序赋 source（新文件 source 小）。
#[allow(clippy::too_many_arguments)]
fn collect_sac_rows_multi(
    sac_dir: &PathBuf,
    tenant_id: TenantId,
    table_id: u32,
    column_ids: &[u16],
    ts_start: i64,
    ts_end: i64,
    out: &mut Vec<QueryRow>,
    encryptor: SharedEncryptor,
    base_source: usize,
    partition_duration_ms: u64,
) -> Result<(), SenseArcError> {
    // 先落到局部缓冲，整轮读全后才 extend 进 `out`：一旦中途检测到「源文件被并发
    // compaction 删除」，清空本轮局部缓冲并重扫重试，绝不把半成品（漏点）混入结果。
    let mut sac_rows: Vec<QueryRow> = Vec::new();

    for _ in 0..SAC_READ_MAX_RETRY {
        sac_rows.clear();
        let files = list_sac_files_sorted(
            sac_dir,
            tenant_id,
            table_id,
            ts_start,
            ts_end,
            partition_duration_ms,
        )?;

        let mut vanished = false;
        for (idx, path) in files.iter().enumerate() {
            match SacReader::read_range(
                path,
                tenant_id,
                table_id,
                ts_start,
                ts_end,
                column_ids,
                encryptor.clone(),
            ) {
                Ok(rows) => {
                    for row in rows {
                        let value_map: HashMap<u16, TypedValue> = row
                            .values
                            .iter()
                            .map(|(cid, v)| (*cid, v.clone()))
                            .collect();
                        let values: Vec<Option<TypedValue>> = column_ids
                            .iter()
                            .map(|cid| value_map.get(cid).cloned())
                            .collect();
                        sac_rows.push(QueryRow {
                            ts: row.ts,
                            values,
                            source: base_source + idx,
                        });
                    }
                }
                Err(_) => {
                    // 源文件在枚举后被并发 compaction 删除（copy-on-write）→ 重扫重试。
                    if !path.exists() {
                        vanished = true;
                        break;
                    }
                    // 其他读错误（损坏等）：保留旧行为，跳过该文件。
                }
            }
        }

        if !vanished {
            out.extend(sac_rows.drain(..));
            return Ok(());
        }
    }

    // 重试耗尽：目录持续剧烈变动。宁可显式报错也不静默丢点。
    Err(SenseArcError::Internal(format!(
        "SAC read retry exhausted due to concurrent compaction (table {table_id})"
    )))
}

/// 收集 SAC 文件中指定表的全列数据行（列值按键对齐），并把列 ID 并入并集。
#[allow(clippy::too_many_arguments)]
fn collect_sac_rows_all(
    sac_dir: &PathBuf,
    tenant_id: TenantId,
    table_id: u32,
    ts_start: i64,
    ts_end: i64,
    out: &mut Vec<MapRow>,
    col_set: &mut BTreeSet<u16>,
    encryptor: SharedEncryptor,
    base_source: usize,
    partition_duration_ms: u64,
) -> Result<(), SenseArcError> {
    // 先落到局部缓冲，整轮读全后才 extend 进 `out`/`col_set`：一旦中途检测到
    // 「源文件被并发 compaction 删除」，清空局部缓冲并重扫重试，绝不混入半成品。
    let mut sac_rows: Vec<MapRow> = Vec::new();
    let mut sac_cols: BTreeSet<u16> = BTreeSet::new();

    for _ in 0..SAC_READ_MAX_RETRY {
        sac_rows.clear();
        sac_cols.clear();
        let files = list_sac_files_sorted(
            sac_dir,
            tenant_id,
            table_id,
            ts_start,
            ts_end,
            partition_duration_ms,
        )?;

        let mut vanished = false;
        for (idx, path) in files.iter().enumerate() {
            // 空 projection = 读全列（SacReader::read_range 的 want_all 语义）。
            match SacReader::read_range(
                path,
                tenant_id,
                table_id,
                ts_start,
                ts_end,
                &[],
                encryptor.clone(),
            ) {
                Ok(rows) => {
                    for row in rows {
                        let mut values = HashMap::with_capacity(row.values.len());
                        for (cid, v) in &row.values {
                            sac_cols.insert(*cid);
                            values.insert(*cid, Some(v.clone()));
                        }
                        sac_rows.push(MapRow {
                            ts: row.ts,
                            values,
                            source: base_source + idx,
                        });
                    }
                }
                Err(_) => {
                    // 源文件在枚举后被并发 compaction 删除（copy-on-write）→ 重扫重试。
                    if !path.exists() {
                        vanished = true;
                        break;
                    }
                    // 其他读错误（损坏等）：保留旧行为，跳过该文件。
                }
            }
        }

        if !vanished {
            out.extend(sac_rows.drain(..));
            col_set.extend(sac_cols.iter().copied());
            return Ok(());
        }
    }

    // 重试耗尽：目录持续剧烈变动。宁可显式报错也不静默丢点。
    Err(SenseArcError::Internal(format!(
        "SAC read retry exhausted due to concurrent compaction (table {table_id})"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::TypedValue;
    use crate::query::planner::QueryPlan;
    use arrow::array::Array;
    use sensearcdb_common::ColumnRef;

    #[test]
    fn test_execute_empty() {
        let executor = QueryExecutor::default();
        let plan = QueryPlan {
            table_name: "table_1".into(),
            projection: vec![
                ProjectionExpr::Timestamp,
                ProjectionExpr::Column(ColumnRef::new(1, 0)),
            ],
            ts_start: Some(0),
            ts_end: Some(1000),
            ts_start_inclusive: true,
            ts_end_inclusive: true,
            predicate: None,
            group_by: Vec::new(),
            time_bucket: None,
            limit: None,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute_plan(TenantId::DEFAULT, &plan))
            .unwrap();
        assert!(result.batches.is_empty());
    }

    /// 查询剪枝（收益落点）：`list_sac_files_sorted` 只枚举与查询时间范围重叠的分区目录。
    #[test]
    fn test_list_sac_files_sorted_prunes_by_partition() {
        use crate::compression::CompressionType;
        use crate::memtable::col_vec::ColVec;
        use crate::sac::header::SacLevel;
        use crate::sac::writer::SacWriter;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac").to_path_buf();
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        // 两个相邻天分区：day0 与 day1（UTC+8 00:00 边界）。
        let boundary = 1_704_038_400_000_000_000_i64; // 2024-01-01 00:00 UTC+8
        let day0 = boundary;
        let day1 = boundary + 86_400_000_000_000_i64;

        let write_file = |pn: i64, seq: u16, ts: i64| -> std::path::PathBuf {
            let mut columns = std::collections::HashMap::new();
            columns.insert(0, ColVec::F64(vec![1.0]));
            let ts_index: std::collections::BTreeMap<i64, usize> =
                [(ts, 0usize)].into_iter().collect();
            let chunk = ColumnarChunk {
                table_id: 42,
                tenant_id: TenantId::DEFAULT,
                timestamps: vec![ts],
                columns,
                presence: std::collections::HashMap::new(),
                qualities: vec![0xC0],
                ts_index,
                point_count: 1,
                out_of_order: false,
                max_wal_seq: None,
                min_wal_seq: None,
            };
            writer
                .write_chunk(&chunk, pn, SacLevel::L0, seq, None)
                .unwrap()
        };

        let f_day0 = write_file(day0, 1, day0 + 1);
        let f_day1 = write_file(day1, 1, day1 + 1);
        assert!(f_day0.exists() && f_day1.exists());

        // 只查 [day0, day1)：day1 分区 partition_ns == ts_end（右开）→ 不命中。
        let files =
            list_sac_files_sorted(&sac_dir, TenantId::DEFAULT, 42, day0, day1, 86_400_000).unwrap();
        assert_eq!(files.len(), 1, "只应枚举 day0 一个分区");
        assert_eq!(files[0].file_name(), f_day0.file_name());

        // 全范围：两个分区都应命中。
        let files_all = list_sac_files_sorted(
            &sac_dir,
            TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            86_400_000,
        )
        .unwrap();
        assert_eq!(files_all.len(), 2, "全范围应枚举两个分区");
    }

    #[test]
    fn test_resolve_table_id() {
        assert_eq!(resolve_table_id("table_42"), Some((42, None)));
        assert_eq!(resolve_table_id("42"), Some((42, None)));
        assert_eq!(resolve_table_id("my_table"), None);

        // 列限定：`table_42.col_7` / `42.7` → 只投影列 7。
        assert_eq!(resolve_table_id("table_42.col_7"), Some((42, Some(7))));
        assert_eq!(resolve_table_id("42.7"), Some((42, Some(7))));
        assert_eq!(resolve_table_id("table_42.col_x"), None);
    }

    #[test]
    fn test_query_row_ordering() {
        let r1 = QueryRow {
            ts: 100,
            values: vec![Some(TypedValue::F64(1.0))],
            source: 0,
        };
        let r2 = QueryRow {
            ts: 200,
            values: vec![Some(TypedValue::F64(2.0))],
            source: 1,
        };
        assert!(r1 < r2); // ts 小的优先
        let r3 = QueryRow {
            ts: 200,
            values: vec![Some(TypedValue::F64(9.0))],
            source: 0,
        };
        assert!(r3 < r2); // 同 ts，source 小的优先
    }

    #[test]
    fn test_memtable_query() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(7, 100, &[(0, TypedValue::F64(1.5))], &[(0, 0xC0)], 0);
        memtable.insert(7, 200, &[(0, TypedValue::F64(2.5))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute_range(TenantId::DEFAULT, 7, 0, 300))
            .unwrap();
        assert_eq!(result.batches.len(), 1);
        assert_eq!(result.batches[0].num_rows(), 2);
    }

    /// P1：多列投影返回正确列数与列类型。
    #[test]
    fn test_multi_column_projection_builds_n_columns() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(
            7,
            100,
            &[(0, TypedValue::F64(1.5)), (1, TypedValue::I64(42))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            7,
            200,
            &[(0, TypedValue::F64(2.5)), (1, TypedValue::I64(43))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, col_1 FROM table_7 WHERE ts BETWEEN 0 AND 1000",
            ))
            .unwrap();

        assert_eq!(result.batches.len(), 1);
        let batch = &result.batches[0];
        assert_eq!(batch.num_columns(), 2, "投影两列 → 输出两列");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "col_0");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(1).name(), "col_1");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
    }

    /// P1：i64 不折 f64（类型保真）。
    #[test]
    fn test_projection_i64_not_folded_to_f64() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let big = 9_007_199_254_740_993i64; // 2^53 + 1（折 f64 会丢尾位）
        memtable.insert(9, 100, &[(1, TypedValue::I64(big))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_1 FROM table_9 WHERE ts BETWEEN 0 AND 200",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), big, "i64 必须原样直通，不得经 f64 折叠");
        assert_eq!(
            arr.value(0) as f64 as i64,
            big - 1,
            "判别：折 f64 会丢失 2^53+1 的尾位"
        );
    }

    /// P1：NULL（缺失列 / 缺失值）走 Arrow null bitmap，不补 0.0。
    #[test]
    fn test_projection_null_uses_null_bitmap() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        // col_0 存在，col_5 不存在（稀疏列）。
        memtable.insert(9, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, col_5 FROM table_9 WHERE ts BETWEEN 0 AND 200",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_columns(), 2);

        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.null_count(), 0);
        assert_eq!(c0.value(0), 1.0);

        let c5 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c5.null_count(), 1, "缺失列必须走 null bitmap，不得补 0.0");
        assert!(c5.is_null(0));
    }

    /// P1：通配投影输出 `ts` + 全部实际列（含 U64 类型保真）。
    #[test]
    fn test_wildcard_projection_includes_ts_and_all_columns() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(
            7,
            100,
            &[(0, TypedValue::F64(1.5)), (2, TypedValue::U64(7))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT * FROM table_7 WHERE ts BETWEEN 0 AND 1000",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_columns(), 3, "ts + col_0 + col_2");
        assert_eq!(batch.schema().field(0).name(), "ts");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).name(), "col_0");
        assert_eq!(batch.schema().field(2).name(), "col_2");

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 100);

        let c2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(c2.value(0), 7, "U64 必须保真");
    }

    #[test]
    fn test_memtable_query_columns_typed() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(
            7,
            100,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(10.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            7,
            200,
            &[(0, TypedValue::F64(2.0)), (1, TypedValue::F64(20.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            7,
            300,
            &[(0, TypedValue::F64(3.0)), (1, TypedValue::F64(30.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (columns, timestamps) = rt
            .block_on(executor.execute_range_columns_typed(TenantId::DEFAULT, 7, &[0, 1], 100, 300))
            .unwrap();

        assert_eq!(timestamps, vec![100, 200, 300]);
        assert_eq!(columns.len(), 2);
        assert_eq!(
            columns[0],
            vec![
                Some(TypedValue::F64(1.0)),
                Some(TypedValue::F64(2.0)),
                Some(TypedValue::F64(3.0))
            ]
        );
        assert_eq!(
            columns[1],
            vec![
                Some(TypedValue::F64(10.0)),
                Some(TypedValue::F64(20.0)),
                Some(TypedValue::F64(30.0))
            ]
        );
    }

    /// 查询层 typed 读取保真：U64 大值（> 2^53）、F32、I32 必须原样直通，不得经 f64 折叠。
    #[test]
    fn test_memtable_query_columns_typed_fidelity() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));

        let u64_big = 9_007_199_254_740_993u64; // 2^53 + 1（f64 折叠会丢尾位）
        let f32_val = 1.25f32;
        let i32_val = -2_000_000_000i32;

        memtable.insert(
            9,
            100,
            &[
                (0, TypedValue::U64(u64_big)),
                (1, TypedValue::F32(f32_val)),
                (2, TypedValue::I32(i32_val)),
            ],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (columns, timestamps) = rt
            .block_on(executor.execute_range_columns_typed(
                TenantId::DEFAULT,
                9,
                &[0, 1, 2],
                0,
                200,
            ))
            .unwrap();

        assert_eq!(timestamps, vec![100]);
        assert_eq!(columns[0], vec![Some(TypedValue::U64(u64_big))]);
        assert_eq!(columns[1], vec![Some(TypedValue::F32(f32_val))]);
        assert_eq!(columns[2], vec![Some(TypedValue::I32(i32_val))]);

        // 关键判别：f64 折叠会把 2^53+1 折叠成 2^53，此处必须精确等于 2^53+1。
        match columns[0][0] {
            Some(TypedValue::U64(v)) => assert_eq!(v, u64_big),
            ref other => panic!("U64 保真失败，实际 {other:?}"),
        }
    }

    /// 缺失列语义：投影列不存在时返回 `None`（不再补 0.0）。
    #[test]
    fn test_memtable_query_columns_missing_is_none() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(9, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (columns, timestamps) = rt
            .block_on(executor.execute_range_columns_typed(TenantId::DEFAULT, 9, &[0, 5], 0, 200))
            .unwrap();

        assert_eq!(timestamps, vec![100]);
        assert_eq!(columns[0], vec![Some(TypedValue::F64(1.0))]);
        assert_eq!(columns[1], vec![None]); // 列 5 不存在 → None
    }

    /// T7：查询集成——`execute_range_columns_typed` 从 SAC 读稀疏列，null 位置返回 `None`。
    ///
    /// 稀疏 chunk 经 flush → SAC → 查询层三源合并后，稀疏列的 null 必须原样 `None`，
    /// 不得静默变占位值（0.0/false/""）。
    #[test]
    fn test_execute_range_columns_typed_sparse_none() {
        use crate::compression::CompressionType;
        use crate::sac::header::SacLevel;
        use crate::sac::writer::SacWriter;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 7);
        chunk.append(100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        chunk.append(
            200,
            &[(0, TypedValue::F64(2.0)), (1, TypedValue::F64(20.0))],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(300, &[(0, TypedValue::F64(3.0))], &[(0, 0xC0)], 0);
        writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write");

        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let executor = QueryExecutor::new(memtable, sac_dir);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (columns, timestamps) = rt
            .block_on(executor.execute_range_columns_typed(TenantId::DEFAULT, 7, &[0, 1], 0, 500))
            .unwrap();

        assert_eq!(timestamps, vec![100, 200, 300]);
        assert_eq!(
            columns[0],
            vec![
                Some(TypedValue::F64(1.0)),
                Some(TypedValue::F64(2.0)),
                Some(TypedValue::F64(3.0))
            ]
        );
        assert_eq!(
            columns[1],
            vec![None, Some(TypedValue::F64(20.0)), None],
            "稀疏列 null 必须返回 None，而非占位值"
        );
    }

    /// 并发正确性回归（热读深拷贝修复的「快照一致性等价」验证）：
    /// 写者持续追加数据的同时，查询侧并发读。每次查询返回的行必须是「完整行」
    /// （投影的两列都不能缺失 = 无撕裂读），且 ts 严格升序（排序去重后不变量）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_query_during_concurrent_writes_is_consistent() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let executor = QueryExecutor::new(memtable.clone(), PathBuf::new());

        const TOTAL: i64 = 50_000;

        // 同步栅栏：只保证「首行（ts=0）已提交」后才开始查询，杜绝 CPU 竞争下
        // 写者首次 poll 被拖延到 300 轮查询全部结束后（查询返回 0 行是正确语义）
        // 却使 `observed_any_row` 误判失败的时序脆弱性。绝不等待全部写完，
        // 否则测试退化为非并发、失去并发验证意义。
        let first_row_written = Arc::new(tokio::sync::Notify::new());

        // 写者：逐点追加两列数据（ts 0..TOTAL，两列值都非 None）。
        let writer = {
            let memtable = memtable.clone();
            let first_row_written = first_row_written.clone();
            tokio::spawn(async move {
                for i in 0..TOTAL {
                    memtable.insert(
                        1,
                        i,
                        &[
                            (0, TypedValue::F64(i as f64)),
                            (1, TypedValue::F64(i as f64 * 2.0)),
                        ],
                        &[(0, 0xC0)],
                        0,
                    );
                    if i == 0 {
                        first_row_written.notify_one();
                    }
                }
            })
        };

        // 等首行提交后再进入查询循环。
        first_row_written.notified().await;

        // 查询侧：在写入进行的同时反复查询，校验无撕裂读 + ts 升序。
        let mut observed_any_row = false;
        for _ in 0..300 {
            let (columns, timestamps) = executor
                .execute_range_columns_typed(TenantId::DEFAULT, 1, &[0, 1], 0, TOTAL - 1)
                .await
                .unwrap();

            for w in timestamps.windows(2) {
                assert!(w[0] < w[1], "ts 必须严格升序，实际 {:?}", w);
            }
            assert_eq!(columns.len(), 2, "投影两列");
            for row in 0..timestamps.len() {
                assert!(columns[0][row].is_some(), "col0 撕裂读（ts 已见但值缺失）");
                assert!(columns[1][row].is_some(), "col1 撕裂读（ts 已见但值缺失）");
            }
            observed_any_row |= !timestamps.is_empty();
            tokio::task::yield_now().await;
        }

        writer.await.expect("writer task should complete");

        // 写完后全量查询应恰好返回 TOTAL 行（两列齐全）。
        let (columns, timestamps) = executor
            .execute_range_columns_typed(TenantId::DEFAULT, 1, &[0, 1], 0, TOTAL - 1)
            .await
            .unwrap();
        assert_eq!(timestamps.len(), TOTAL as usize, "写完后应查回全部点");
        assert_eq!(columns.len(), 2);
        assert!(observed_any_row, "并发查询至少应观察到部分行");
    }

    /// 更强并发压力：多写者（各写互斥 ts 区间）+ 多读者并发。
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_query_during_multi_writer_and_reader_is_consistent() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let executor = Arc::new(QueryExecutor::new(memtable.clone(), PathBuf::new()));

        const TOTAL: i64 = 100_000;
        const WRITERS: i64 = 4;
        const PER: i64 = TOTAL / WRITERS;

        // 写者：各写互斥 ts 区间 [w*PER, (w+1)*PER)，两列都非 None。
        let mut writers = Vec::new();
        for w in 0..WRITERS {
            let memtable = memtable.clone();
            let start = w * PER;
            let end = start + PER;
            writers.push(tokio::spawn(async move {
                for ts in start..end {
                    memtable.insert(
                        2,
                        ts,
                        &[
                            (0, TypedValue::F64(ts as f64)),
                            (1, TypedValue::F64(ts as f64 * 2.0)),
                        ],
                        &[(0, 0xC0)],
                        0,
                    );
                }
            }));
        }

        // 读者：并发反复查询全量范围，校验升序 + 无撕裂读。
        let mut readers = Vec::new();
        for _ in 0..4 {
            let executor = executor.clone();
            readers.push(tokio::spawn(async move {
                for _ in 0..200 {
                    let (columns, timestamps) = executor
                        .execute_range_columns_typed(TenantId::DEFAULT, 2, &[0, 1], 0, TOTAL - 1)
                        .await
                        .unwrap();
                    for w in timestamps.windows(2) {
                        assert!(w[0] < w[1], "ts 必须严格升序，实际 {:?}", w);
                    }
                    assert_eq!(columns.len(), 2, "投影两列");
                    for row in 0..timestamps.len() {
                        assert!(columns[0][row].is_some(), "col0 撕裂读");
                        assert!(columns[1][row].is_some(), "col1 撕裂读");
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }

        for w in writers {
            w.await.expect("writer task should complete");
        }
        for r in readers {
            r.await.expect("reader task should complete");
        }

        // 写完后全量查询：恰好 TOTAL 行、ts 恰为 0..TOTAL（无丢失/重复/错序）。
        let (columns, timestamps) = executor
            .execute_range_columns_typed(TenantId::DEFAULT, 2, &[0, 1], 0, TOTAL - 1)
            .await
            .unwrap();
        assert_eq!(
            timestamps,
            (0..TOTAL).collect::<Vec<i64>>(),
            "写完后应查回全部点且无重复/错序"
        );
        assert_eq!(columns.len(), 2);
    }

    /// force_freeze 与查询并发竞态（P1 RwLock 原子切换验证）：查询必须恒读到完整一致视图。
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_query_during_force_freeze_is_consistent() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let executor = Arc::new(QueryExecutor::new(memtable.clone(), PathBuf::new()));

        const TOTAL: i64 = 50_000;

        // 一次性写满（两列都非 None），不并发写，隔离出「freeze vs query」竞态。
        for ts in 0..TOTAL {
            memtable.insert(
                3,
                ts,
                &[
                    (0, TypedValue::F64(ts as f64)),
                    (1, TypedValue::F64(ts as f64 * 3.0)),
                ],
                &[(0, 0xC0)],
                0,
            );
        }

        // freezer：反复 force_freeze，制造 active/immutable 交替。
        let freezer = {
            let memtable = memtable.clone();
            tokio::spawn(async move {
                for _ in 0..20 {
                    memtable.force_freeze();
                    tokio::task::yield_now().await;
                }
            })
        };

        // 读者：并发反复查询全量范围。
        let mut readers = Vec::new();
        for _ in 0..4 {
            let executor = executor.clone();
            readers.push(tokio::spawn(async move {
                for _ in 0..200 {
                    let (columns, timestamps) = executor
                        .execute_range_columns_typed(TenantId::DEFAULT, 3, &[0, 1], 0, TOTAL - 1)
                        .await
                        .unwrap();
                    // 关键不变量（P1 后无可见性间隙）：每次查询必须精确返回完整 0..TOTAL。
                    assert_eq!(
                        timestamps,
                        (0..TOTAL).collect::<Vec<i64>>(),
                        "force_freeze 期间查询必须恒返回完整 0..TOTAL，不得漏读/部分读"
                    );
                    for w in timestamps.windows(2) {
                        assert!(w[0] < w[1], "ts 必须严格升序");
                    }
                    assert_eq!(columns.len(), 2, "投影两列");
                    for row in 0..timestamps.len() {
                        assert!(columns[0][row].is_some(), "col0 撕裂读");
                        assert!(columns[1][row].is_some(), "col1 撕裂读");
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }

        freezer.await.expect("freezer task should complete");
        for r in readers {
            r.await.expect("reader task should complete");
        }

        // freeze 结束 + 稳定后：全量查询必须回到完整 0..TOTAL。
        let (columns, timestamps) = executor
            .execute_range_columns_typed(TenantId::DEFAULT, 3, &[0, 1], 0, TOTAL - 1)
            .await
            .unwrap();
        assert_eq!(
            timestamps,
            (0..TOTAL).collect::<Vec<i64>>(),
            "freeze 后应查回全部点且无丢失/重复"
        );
        assert_eq!(columns.len(), 2);
    }

    /// S2 Step 2：MemTable 三源 tenant 谓词——跨租户 table_id 互不可见。
    #[tokio::test]
    async fn test_query_tenant_isolation_memtable() {
        use crate::engine::DataPoint;

        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));

        fn point(table_id: u32, tenant_id: u32, ts: i64, v: f64) -> DataPoint {
            DataPoint {
                table_id,
                tenant_id: sensearcdb_common::TenantId(tenant_id),
                ts,
                server_ts: ts + 1,
                values: vec![(0, TypedValue::F64(v))],
                qualities: vec![(0, 0xC0)],
                schema_version: 1,
                flags: 0,
            }
        }

        memtable.insert_batch(
            &[
                point(42, 1, 100, 1.0),
                point(42, 1, 200, 2.0),
                point(7, 2, 300, 30.0),
            ],
            &[],
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());

        // tenant=1 查 table 42 → 命中本租户数据（ts 100/200）。
        let (columns, timestamps) = executor
            .execute_range_columns_typed(TenantId(1), 42, &[0], 0, 1000)
            .await
            .unwrap();
        assert_eq!(timestamps, vec![100, 200]);
        assert_eq!(
            columns[0],
            vec![Some(TypedValue::F64(1.0)), Some(TypedValue::F64(2.0))]
        );

        // tenant=1 查 table 7（属于 tenant=2）→ 跨租户不可见，返回空。
        let (columns, timestamps) = executor
            .execute_range_columns_typed(TenantId(1), 7, &[0], 0, 1000)
            .await
            .unwrap();
        assert!(timestamps.is_empty(), "跨租户 table_id 应视为不存在");
        assert!(columns.is_empty() || columns[0].is_empty());

        // tenant=2 查 table 7 → 命中本租户数据（ts 300）。
        let (columns, timestamps) = executor
            .execute_range_columns_typed(TenantId(2), 7, &[0], 0, 1000)
            .await
            .unwrap();
        assert_eq!(timestamps, vec![300]);
        assert_eq!(columns[0], vec![Some(TypedValue::F64(30.0))]);
    }

    /// P2：单列比较谓词下推过滤。
    #[test]
    fn test_predicate_comparison_filters_rows() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100, 1.0), (200, 6.0), (300, 3.0)] {
            memtable.insert(7, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0 FROM table_7 WHERE col_0 > 5",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1, "只有 col_0=6.0 命中 > 5");
        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 6.0);
    }

    /// P2：AND / OR 组合谓词。
    #[test]
    fn test_predicate_and_or() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100, 1.0), (200, 5.0), (300, 10.0), (400, 0.0)] {
            memtable.insert(7, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0 FROM table_7 WHERE (col_0 > 1 AND col_0 < 9) OR col_0 = 0",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2, "命中 col_0=5 与 col_0=0");
        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let vals: Vec<f64> = (0..batch.num_rows()).map(|i| c0.value(i)).collect();
        assert_eq!(vals, vec![5.0, 0.0], "按 ts 升序：ts200=5.0 先于 ts400=0.0");
    }

    /// P2：IN 列表谓词。
    #[test]
    fn test_predicate_in() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100, 1.0), (200, 10.0), (300, 20.0), (400, 30.0)] {
            memtable.insert(7, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0 FROM table_7 WHERE col_0 IN (10, 30)",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2);
    }

    /// P2：BETWEEN 谓词（两端含）。
    #[test]
    fn test_predicate_between() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100, 1.0), (200, 2.0), (300, 3.0), (400, 4.0), (500, 5.0)] {
            memtable.insert(7, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0 FROM table_7 WHERE col_0 BETWEEN 2 AND 4",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3, "命中 2/3/4");
    }

    /// P2：NULL 三值逻辑——缺失列不命中谓词。
    #[test]
    fn test_predicate_null_not_matched() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(7, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        // ts 200 无 col_0（稀疏）→ 该行 col_0 为 None。
        memtable.insert(7, 200, &[(1, TypedValue::F64(9.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0 FROM table_7 WHERE col_0 < 10",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1, "col_0 为 NULL 的行不命中 < 10");
        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 1.0);
    }

    /// P2：ts 范围 + 列过滤组合——ts 走索引，列谓词二次过滤。
    #[test]
    fn test_predicate_ts_range_plus_column_filter() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(50, 10.0), (150, 20.0), (250, 5.0)] {
            memtable.insert(7, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0 FROM table_7 WHERE ts BETWEEN 100 AND 200 AND col_0 > 15",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1, "仅 ts150=20.0 命中");
        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 20.0);
    }

    /// P2：谓词列不在投影中时仍能正确过滤（需收集谓词列并重映射下标）。
    #[test]
    fn test_predicate_on_non_projected_column() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(
            7,
            100,
            &[(0, TypedValue::F64(6.0)), (1, TypedValue::I64(42))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            7,
            200,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::I64(43))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_1 FROM table_7 WHERE col_0 > 5",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_columns(), 1, "只投影 col_1");
        assert_eq!(batch.schema().field(0).name(), "col_1");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let c1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(c1.value(0), 42, "col_0>5 命中的行 col_1=42");
    }

    /// P2：I64 类型保真比较——大整数字面量不折 f64。
    #[test]
    fn test_predicate_i64_type_fidelity() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let big = 9_007_199_254_740_993i64; // 2^53 + 1
        memtable.insert(9, 100, &[(1, TypedValue::I64(big))], &[(0, 0xC0)], 0);
        memtable.insert(9, 200, &[(1, TypedValue::I64(big + 1))], &[(0, 0xC0)], 0);

        let sql = format!("SELECT col_1 FROM table_9 WHERE col_1 = {big}");
        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(TenantId::DEFAULT, &sql))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1, "精确匹配 2^53+1，不误命中 2^53+2");
        let c1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(c1.value(0), big);
    }

    /// 修复 2：`ts > x` / `ts < x` 排边界，`ts >= x` / `ts <= x` 含边界。
    #[test]
    fn test_ts_boundary_inclusivity() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for ts in [100i64, 150, 200] {
            memtable.insert(7, ts, &[(0, TypedValue::F64(ts as f64))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();

        // 开区间：排除 ts=100 与 ts=200。
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT ts FROM table_7 WHERE ts > 100 AND ts < 200",
            ))
            .unwrap();
        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1, "ts > 100 AND ts < 200 只应命中 ts=150");
        let ts_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_col.value(0), 150);

        // 闭区间：包含 ts=100 与 ts=200。
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT ts FROM table_7 WHERE ts >= 100 AND ts <= 200",
            ))
            .unwrap();
        let batch = &result.batches[0];
        assert_eq!(
            batch.num_rows(),
            3,
            "ts >= 100 AND ts <= 200 应命中全部 3 行"
        );
    }

    /// P3：单组 COUNT(*)（无 GROUP BY）。
    #[test]
    fn test_aggregate_count_star() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(8, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(8, 200, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);
        memtable.insert(8, 300, &[(1, TypedValue::F64(5.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(TenantId::DEFAULT, "SELECT count(*) FROM table_8"))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "count(*)");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
        let c = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(c.value(0), 3, "COUNT(*) 计数全部 3 个数据点");
    }

    /// P3：GROUP BY 分组 + 分组键列输出。
    #[test]
    fn test_aggregate_group_by() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(
            9,
            100,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(10.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            9,
            200,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(20.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            9,
            300,
            &[(0, TypedValue::F64(2.0)), (1, TypedValue::F64(30.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, count(*) FROM table_9 GROUP BY col_0",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2, "两个分组：col_0=1.0 与 col_0=2.0");
        assert_eq!(batch.num_columns(), 2);

        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 1.0);
        assert_eq!(c0.value(1), 2.0);

        let cnt = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 2, "col_0=1.0 出现 2 次");
        assert_eq!(cnt.value(1), 1, "col_0=2.0 出现 1 次");
    }

    /// P3：各聚合函数（COUNT/AVG/SUM/MIN/MAX/FIRST/LAST）。
    #[test]
    fn test_aggregate_functions() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100i64, 1.0f64), (200, 2.0), (300, 3.0)] {
            memtable.insert(10, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let sql = "SELECT count(*), avg(col_0), sum(col_0), min(col_0), max(col_0), first(col_0), last(col_0) FROM table_10";
        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(TenantId::DEFAULT, sql))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 7);

        let cnt = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 3);

        for (i, expect) in [2.0f64, 6.0, 1.0, 3.0, 1.0, 3.0].iter().enumerate() {
            let col = batch
                .column(i + 1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(col.value(0), *expect, "第 {} 个聚合结果", i + 1);
        }
    }

    /// P3：聚合结果类型保真——COUNT→UInt64；I64 列 SUM 不折 f64。
    #[test]
    fn test_aggregate_result_type_fidelity() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let a = 9_007_199_254_740_993i64; // 2^53 + 1
        memtable.insert(11, 100, &[(1, TypedValue::I64(a))], &[(0, 0xC0)], 0);
        memtable.insert(11, 200, &[(1, TypedValue::I64(1))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT sum(col_1), count(*) FROM table_11",
            ))
            .unwrap();

        let batch = &result.batches[0];
        // sum(col_1) → Int64（源 I64 保真）。
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let sum = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum.value(0), a + 1, "I64 SUM 不折 f64");

        // count(*) → UInt64。
        assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
        let cnt = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 2);
    }

    /// P3：NULL 语义——COUNT(col) 只计非 NULL，COUNT(*) 计行。
    #[test]
    fn test_aggregate_count_null_semantics() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(12, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        // ts 200 无 col_0（稀疏，COUNT(col_0) 不应计数该行）。
        memtable.insert(12, 200, &[(1, TypedValue::F64(9.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT count(*), count(col_0) FROM table_12",
            ))
            .unwrap();

        let batch = &result.batches[0];
        let cnt_star = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let cnt_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt_star.value(0), 2, "COUNT(*) 计行");
        assert_eq!(cnt_col.value(0), 1, "COUNT(col_0) 只计非 NULL");
    }

    /// P3：聚合 + P2 谓词组合。
    #[test]
    fn test_aggregate_with_predicate() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(
            13,
            100,
            &[(0, TypedValue::F64(6.0)), (1, TypedValue::F64(10.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            13,
            200,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(20.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            13,
            300,
            &[(0, TypedValue::F64(8.0)), (1, TypedValue::F64(30.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT avg(col_1) FROM table_13 WHERE col_0 > 5",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        let avg = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(avg.value(0), 20.0, "(10.0 + 30.0) / 2");
    }

    /// P4：SAMPLE BY 1d 跨日边界 + UTC 日历日锚定（非滑动桶）。
    #[test]
    fn test_time_bucket_sample_by_1d() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        // 2016-01-01T23:59:59Z 与 2016-01-02T00:00:01Z 分属不同 UTC 日。
        let day1_end = 1_451_692_799_000_000_000i64; // 2016-01-01 23:59:59
        let day2_start = 1_451_692_801_000_000_000i64; // 2016-01-02 00:00:01
        memtable.insert(14, day1_end, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(
            14,
            day2_start,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT count(*), avg(col_0) FROM table_14 SAMPLE BY 1d",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "ts");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);

        // 桶起始必须锚定 UTC 午夜，而非查询起点或首个数据点。
        let jan1 = 1_451_606_400_000_000_000i64; // 2016-01-01T00:00:00Z
        let jan2 = 1_451_692_800_000_000_000i64; // 2016-01-02T00:00:00Z
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), jan1);
        assert_eq!(ts.value(1), jan2);

        let cnt = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 1);
        assert_eq!(cnt.value(1), 1);

        let avg = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(avg.value(0), 1.0);
        assert_eq!(avg.value(1), 2.0);
    }

    /// P4：SAMPLE BY 10m epoch 对齐。
    #[test]
    fn test_time_bucket_sample_by_10m_epoch_aligned() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let ten_min = 600_000_000_000i64; // 10 分钟（纳秒）
        memtable.insert(15, 0, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(
            15,
            ten_min - 1,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(15, ten_min, &[(0, TypedValue::F64(3.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT count(*), sum(col_0) FROM table_15 SAMPLE BY 10m",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2);

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 0, "epoch 对齐到 0");
        assert_eq!(ts.value(1), ten_min);

        let cnt = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 2, "0 与 10m-1 同桶");
        assert_eq!(cnt.value(1), 1);

        let sum = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sum.value(0), 3.0);
        assert_eq!(sum.value(1), 3.0);
    }

    /// P4：1d 桶锚定 UTC 午夜，绝不锚定 query_start（非滑动桶）。
    #[test]
    fn test_time_bucket_1d_not_sliding() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        // 两个点都在 2016-01-01 UTC 日内（同桶），但 query_start 取 09:00（非午夜）。
        let t1 = 1_451_642_400_000_000_000i64; // 2016-01-01 10:00:00
        let t2 = 1_451_645_999_000_000_000i64; // 2016-01-01 10:59:59
        memtable.insert(16, t1, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(16, t2, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        // query_start = 2016-01-01 09:00:00（非午夜）；若错误地锚定 query_start 会得到不同桶起点。
        let sql = "SELECT count(*) FROM table_16 WHERE ts >= 1451638800000000000 SAMPLE BY 1d";
        let result = rt
            .block_on(executor.execute(TenantId::DEFAULT, sql))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1, "同一 UTC 日 → 单桶");
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // 桶起点 = UTC 午夜 2016-01-01T00:00:00Z，而非 query_start。
        assert_eq!(ts.value(0), 1_451_606_400_000_000_000i64);
        let cnt = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 2);
    }

    /// P4：GROUP BY time('1d') 时间桶 + 额外分组列组合。
    #[test]
    fn test_time_bucket_group_by_time_with_column() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let jan1 = 1_451_606_400_000_000_000i64; // 2016-01-01T00:00:00Z
        let jan1_late = jan1 + 3_600_000_000_000; // +1h，同日
        memtable.insert(
            17,
            jan1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            17,
            jan1_late,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(3.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, sum(col_1) FROM table_17 GROUP BY time('1d'), col_0",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        // 列顺序：隐式 ts 桶键、分组键 col_0、聚合 sum(col_1)。
        assert_eq!(batch.schema().field(0).name(), "ts");
        assert_eq!(batch.schema().field(1).name(), "col_0");
        assert_eq!(batch.schema().field(2).name(), "sum(col_1)");

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), jan1);
        let c0 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 1.0);
        let sum = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sum.value(0), 5.0, "2.0 + 3.0");
    }

    /// P4：bucket_start 日历桶锚点（Week → ISO 周一，Month → UTC 月首）。
    #[test]
    fn test_time_bucket_bucket_start_calendar_anchor() {
        const DAY_NS: i64 = 86_400_000_000_000;
        let jan1 = 1_451_606_400_000_000_000i64; // 2016-01-01T00:00:00Z（周五）
        let jan2 = 1_451_692_800_000_000_000i64; // 2016-01-02T00:00:00Z

        // Day：同日内任意 ts 锚定 UTC 午夜。
        let day = TimeBucket {
            interval_ns: DAY_NS,
            align: TimeAlign::Day,
        };
        assert_eq!(bucket_start(jan1 + 86_399_000_000_000, &day), jan1);

        // Week：2016-01-01（周五）归入 ISO 周，周一为 2015-12-28。
        let week = TimeBucket {
            interval_ns: 7 * DAY_NS,
            align: TimeAlign::Week,
        };
        assert_eq!(bucket_start(jan1, &week), 1_451_260_800_000_000_000i64);

        // Month：2016-01-02 锚定 2016-01-01 月首。
        let month = TimeBucket {
            interval_ns: 0,
            align: TimeAlign::Month,
        };
        assert_eq!(bucket_start(jan2, &month), jan1);
    }

    /// P5：`COUNT(*) FILTER (WHERE col_0 = 0)` 条件计数 + 不污染无 FILTER 的聚合。
    #[test]
    fn test_filter_count_and_no_cross_contamination() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(
            14,
            100,
            &[(0, TypedValue::I64(0)), (1, TypedValue::F64(10.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            14,
            200,
            &[(0, TypedValue::I64(1)), (1, TypedValue::F64(20.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            14,
            300,
            &[(0, TypedValue::I64(0)), (1, TypedValue::F64(30.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT count(*) FILTER (WHERE col_0 = 0), count(*) FROM table_14",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        let filtered = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let total = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(filtered.value(0), 2, "col_0=0 有 2 行");
        assert_eq!(total.value(0), 3, "FILTER 不应污染无 FILTER 的 count(*)");
    }

    /// P5：`SUM(CASE WHEN ... THEN 1 ELSE 0 END)` + CASE 分支 I64 类型保真。
    #[test]
    fn test_case_sum_i64_fidelity() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(15, 100, &[(0, TypedValue::I64(0))], &[(0, 0xC0)], 0);
        memtable.insert(15, 200, &[(0, TypedValue::I64(1))], &[(0, 0xC0)], 0);
        memtable.insert(15, 300, &[(0, TypedValue::I64(0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT sum(CASE WHEN col_0 = 0 THEN 1 ELSE 0 END) FROM table_15",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Int64,
            "CASE 分支 I64 应保真为 Int64"
        );
        let sum = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum.value(0), 2, "1 + 0 + 1");
    }

    /// P5：FILTER 中 NULL 列值不满足（三值逻辑 UNKNOWN → 不计数）。
    #[test]
    fn test_filter_null_not_match() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(16, 100, &[(0, TypedValue::I64(0))], &[(0, 0xC0)], 0);
        // ts=200 无 col_0（稀疏 NULL），仅 col_1。
        memtable.insert(16, 200, &[(1, TypedValue::F64(5.0))], &[(0, 0xC0)], 0);
        memtable.insert(16, 300, &[(0, TypedValue::I64(0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT count(*) FILTER (WHERE col_0 = 0) FROM table_16",
            ))
            .unwrap();

        let batch = &result.batches[0];
        let cnt = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 2, "col_0 为 NULL 的行不满足 FILTER");
    }

    /// P5：条件聚合（FILTER）+ 时间桶（SAMPLE BY 1d）组合。
    #[test]
    fn test_filter_sample_by_1d() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let jan1 = 1_451_606_400_000_000_000i64; // 2016-01-01T00:00:00Z
        let jan2 = 1_451_692_800_000_000_000i64; // 2016-01-02T00:00:00Z
        memtable.insert(
            17,
            jan1 + 3_600_000_000_000,
            &[(0, TypedValue::I64(0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            17,
            jan1 + 7_200_000_000_000,
            &[(0, TypedValue::I64(1))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            17,
            jan2 + 3_600_000_000_000,
            &[(0, TypedValue::I64(0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT count(*) FILTER (WHERE col_0 = 0) FROM table_17 SAMPLE BY 1d",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2, "两个日历日桶");
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), jan1);
        assert_eq!(ts.value(1), jan2);
        let cnt = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 1, "第 1 天 col_0=0 有 1 行");
        assert_eq!(cnt.value(1), 1, "第 2 天 col_0=0 有 1 行");
    }

    /// P5：CASE WHEN 作为独立投影列。
    #[test]
    fn test_case_standalone_projection() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(18, 100, &[(0, TypedValue::I64(0))], &[(0, 0xC0)], 0);
        memtable.insert(18, 200, &[(0, TypedValue::I64(1))], &[(0, 0xC0)], 0);
        memtable.insert(18, 300, &[(0, TypedValue::I64(0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT CASE WHEN col_0 = 0 THEN 1 ELSE 0 END FROM table_18",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "case");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let c = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(c.value(0), 1);
        assert_eq!(c.value(1), 0);
        assert_eq!(c.value(2), 1);
    }

    /// 返工：混合类型 CASE 独立投影——整数分支（THEN 1）不得被严格变体匹配丢成 NULL。
    #[test]
    fn test_case_mixed_type_projection_no_null_drop() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(19, 100, &[(0, TypedValue::I64(0))], &[(0, 0xC0)], 0);
        memtable.insert(19, 200, &[(0, TypedValue::I64(1))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT CASE WHEN col_0 = 0 THEN 1 ELSE 1.5 END FROM table_19",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        let c = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c.value(0), 1.0, "THEN 1（I64）应提升为 1.0，而非 NULL");
        assert_eq!(c.value(1), 1.5, "ELSE 1.5 应原样输出");
    }

    /// P6：LEAD 基础——ts 升序自然序，分区尾越界 → NULL。
    #[test]
    fn test_window_lead_basic() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100i64, 1.0f64), (200, 2.0), (300, 3.0)] {
            memtable.insert(20, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, lead(col_0) OVER (ORDER BY ts) FROM table_20",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(1).name(), "lead(col_0)");

        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 1.0);
        assert_eq!(c0.value(1), 2.0);
        assert_eq!(c0.value(2), 3.0);

        let lead = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 2.0);
        assert_eq!(lead.value(1), 3.0);
        assert!(lead.is_null(2), "lead 越过分区尾 → NULL");
    }

    /// P6：LAG 基础——分区首越界 → NULL。
    #[test]
    fn test_window_lag_basic() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100i64, 10.0f64), (200, 20.0), (300, 30.0)] {
            memtable.insert(21, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT lag(col_0) OVER (ORDER BY ts) FROM table_21",
            ))
            .unwrap();

        let batch = &result.batches[0];
        let lag = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(lag.is_null(0), "lag 越过分区首 → NULL");
        assert_eq!(lag.value(1), 10.0);
        assert_eq!(lag.value(2), 20.0);
    }

    /// P6：offset = n（lead 向前跨 2 行）。
    #[test]
    fn test_window_offset_n() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100i64, 1.0f64), (200, 2.0), (300, 3.0), (400, 4.0)] {
            memtable.insert(22, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT lead(col_0, 2) OVER (ORDER BY ts) FROM table_22",
            ))
            .unwrap();

        let batch = &result.batches[0];
        let lead = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 3.0);
        assert_eq!(lead.value(1), 4.0);
        assert!(lead.is_null(2));
        assert!(lead.is_null(3));
    }

    /// P6：越界 default（I64 类型保真）。
    #[test]
    fn test_window_out_of_boundary_default() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100i64, 1i64), (200, 2), (300, 3)] {
            memtable.insert(24, ts, &[(0, TypedValue::I64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT lead(col_0, 1, 99) OVER (ORDER BY ts) FROM table_24",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let lead = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 2);
        assert_eq!(lead.value(1), 3);
        assert_eq!(lead.value(2), 99, "越界 → default 99");
    }

    /// P6：PARTITION BY 分区隔离——lead 不得跨分区取值。
    #[test]
    fn test_window_partition_isolation() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(
            25,
            100,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::I64(1))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            25,
            200,
            &[(0, TypedValue::F64(2.0)), (1, TypedValue::I64(1))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            25,
            300,
            &[(0, TypedValue::F64(10.0)), (1, TypedValue::I64(2))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(
            25,
            400,
            &[(0, TypedValue::F64(20.0)), (1, TypedValue::I64(2))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT lead(col_0) OVER (PARTITION BY col_1 ORDER BY ts) FROM table_25",
            ))
            .unwrap();

        let batch = &result.batches[0];
        let lead = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 2.0);
        assert!(
            lead.is_null(1),
            "分区 col_1=1 的末行 lead 不得跨到分区 col_1=2"
        );
        assert_eq!(lead.value(2), 20.0);
        assert!(lead.is_null(3));
    }

    /// P6：ORDER BY 数据列排序——lead 尊重 ORDER BY 列序而非 ts 序。
    #[test]
    fn test_window_order_by_sorting() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        // ts 升序，但 col_0 乱序：3.0 → 1.0 → 2.0。
        memtable.insert(26, 100, &[(0, TypedValue::F64(3.0))], &[(0, 0xC0)], 0);
        memtable.insert(26, 200, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(26, 300, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, lead(col_0) OVER (ORDER BY col_0) FROM table_26",
            ))
            .unwrap();

        let batch = &result.batches[0];
        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 3.0, "输出保持 ts 序（非排序后重排）");
        assert_eq!(c0.value(1), 1.0);
        assert_eq!(c0.value(2), 2.0);

        let lead = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // 按 col_0 排序为 1.0→2.0→3.0：lead 依次为 2.0 / 3.0 / NULL。
        assert!(lead.is_null(0), "col_0=3.0 是排序最大者 → lead NULL");
        assert_eq!(
            lead.value(1),
            2.0,
            "col_0=1.0 的下一个（按 col_0 排序）是 2.0"
        );
        assert_eq!(lead.value(2), 3.0, "col_0=2.0 的下一个是 3.0");
    }

    /// P6：I64 类型保真——lead 不折 f64。
    #[test]
    fn test_window_i64_type_fidelity() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let big = 9_007_199_254_740_993i64; // 2^53 + 1
        memtable.insert(27, 100, &[(0, TypedValue::I64(big))], &[(0, 0xC0)], 0);
        memtable.insert(27, 200, &[(0, TypedValue::I64(big + 1))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT lead(col_0) OVER (ORDER BY ts) FROM table_27",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let lead = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(lead.value(0), big + 1, "I64 必须原样直通");
        assert!(lead.is_null(1));
    }

    /// P6：稀疏 NULL 传播——帧内源值为 None 的 lead 输出 None（非 default）。
    ///
    /// 直接对窗口算子做单元验证：中间行 `col_0 = None` 时，其前一行的 lead
    /// 必须输出 `None`（而非 default），证明「源 NULL 透传」语义与「越界 default」解耦。
    #[test]
    fn test_window_compute_null_passthrough() {
        let rows = vec![
            WindowRow {
                ts: 100,
                data: HashMap::from([(0u16, Some(TypedValue::F64(1.0)))]),
                agg_values: vec![],
            },
            WindowRow {
                ts: 200,
                data: HashMap::from([(0u16, None)]),
                agg_values: vec![],
            },
            WindowRow {
                ts: 300,
                data: HashMap::from([(0u16, Some(TypedValue::F64(3.0)))]),
                agg_values: vec![],
            },
        ];
        let spec = WindowSpec {
            func: WindowFunction::Lead,
            arg: WindowArg::Column(ColumnRef::new(1, 0)),
            offset: 1,
            default: Some(TypedValue::F64(-1.0)),
            partition_by: vec![],
            order_by: vec![OrderKey {
                column: OrderKeyColumn::Ts,
                asc: true,
            }],
        };

        let col = compute_window_column(&rows, &spec, &[]);
        assert_eq!(
            col[0], None,
            "目标行 col_0 为 None → lead 输出 None（源 NULL 透传，非 default）"
        );
        assert_eq!(col[1], Some(TypedValue::F64(3.0)));
        assert_eq!(col[2], Some(TypedValue::F64(-1.0)), "越界 → default");
    }

    /// P6：窗口 + WHERE 谓词组合。
    #[test]
    fn test_window_with_predicate() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100i64, 1.0f64), (200, 2.0), (300, 3.0), (400, 4.0)] {
            memtable.insert(29, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, lead(col_0) OVER (ORDER BY ts) FROM table_29 WHERE col_0 > 1",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3, "过滤后剩余 ts200/300/400");
        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 2.0);
        let lead = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 3.0);
        assert_eq!(lead.value(1), 4.0);
        assert!(lead.is_null(2));
    }

    /// P6：窗口在 GroupByAggregate 之后执行（GROUP BY + LAG）。
    #[test]
    fn test_window_after_group_by() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(30, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(30, 200, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(30, 300, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);
        memtable.insert(30, 400, &[(0, TypedValue::F64(3.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, count(*), lag(col_0) OVER (ORDER BY col_0) FROM table_30 GROUP BY col_0",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3, "3 个分组");
        assert_eq!(batch.num_columns(), 3);

        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 1.0);
        assert_eq!(c0.value(1), 2.0);
        assert_eq!(c0.value(2), 3.0);

        let cnt = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 2);
        assert_eq!(cnt.value(1), 1);
        assert_eq!(cnt.value(2), 1);

        // lag 作用在聚合结果行（每分组一行），按 col_0 排序。
        let lag = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(lag.is_null(0));
        assert_eq!(lag.value(1), 1.0);
        assert_eq!(lag.value(2), 2.0);
    }

    /// P6：窗口在时间桶聚合之后执行（SAMPLE BY + LEAD，TSBS #13 形态）。
    #[test]
    fn test_window_after_time_bucket() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let ten_min = 600_000_000_000i64;
        memtable.insert(31, 0, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(
            31,
            ten_min - 1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(31, ten_min, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);
        memtable.insert(
            31,
            ten_min * 2,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, count(*), lead(col_0) OVER (ORDER BY ts) FROM table_31 GROUP BY time('10m'), col_0",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3, "三个 10m 桶");
        // 隐式桶首列 ts + col_0 + count(*) + lead(col_0)。
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).name(), "ts");

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 0);
        assert_eq!(ts.value(1), ten_min);
        assert_eq!(ts.value(2), ten_min * 2);

        let c0 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 1.0);
        assert_eq!(c0.value(1), 2.0);
        assert_eq!(c0.value(2), 2.0);

        let cnt = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 2);
        assert_eq!(cnt.value(1), 1);
        assert_eq!(cnt.value(2), 1);

        // lead 作用在桶结果上：桶0 → 桶1 的 col_0 = 2.0；桶1 → 桶2 的 col_0 = 2.0；末桶 NULL。
        let lead = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 2.0);
        assert_eq!(lead.value(1), 2.0);
        assert!(lead.is_null(2));
    }

    /// P6：LIMIT 在窗口计算之后作用于输出（lead 仍能看到被截断的行）。
    #[test]
    fn test_window_limit_applied_after_window() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        for (ts, v) in [(100i64, 1.0f64), (200, 2.0), (300, 3.0)] {
            memtable.insert(32, ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
        }

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0, lead(col_0) OVER (ORDER BY ts) FROM table_32 LIMIT 2",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2, "LIMIT 截断输出为 2 行");
        let lead = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // 若 LIMIT 错误地在窗口前截断，ts200 会成为分区尾，lead 输出 NULL。
        assert_eq!(lead.value(0), 2.0, "lead 应看到 ts300，LIMIT 在窗口之后");
        assert_eq!(lead.value(1), 3.0, "lead 应看到 ts300，LIMIT 在窗口之后");
    }

    /// P0 稀疏列存储修复：中间行稀疏缺失，谓词过滤读到 None 而非错位值。
    #[test]
    fn test_sparse_middle_row_predicate_reads_none() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(33, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        // 中间行 col_0 缺失（稀疏），仅 col_1。
        memtable.insert(33, 200, &[(1, TypedValue::F64(9.0))], &[(0, 0xC0)], 0);
        memtable.insert(33, 300, &[(0, TypedValue::F64(3.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT col_0 FROM table_33 WHERE col_0 < 10",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 2, "col_0 为 None 的中间行不命中 <10");
        let c0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c0.value(0), 1.0);
        assert_eq!(c0.value(1), 3.0);
    }

    /// P0 稀疏列存储修复：聚合 `count(col)` 不计数中间稀疏行，`count(*)` 计行。
    #[test]
    fn test_sparse_middle_row_aggregate_count_none() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(34, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(34, 200, &[(1, TypedValue::F64(9.0))], &[(0, 0xC0)], 0);
        memtable.insert(34, 300, &[(0, TypedValue::F64(3.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT count(*), count(col_0) FROM table_34",
            ))
            .unwrap();

        let batch = &result.batches[0];
        let cnt_star = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let cnt_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cnt_star.value(0), 3, "count(*) 计 3 行");
        assert_eq!(
            cnt_col.value(0),
            2,
            "count(col_0) 只计非 NULL（中间稀疏行不计）"
        );
    }

    /// P0 稀疏列存储修复：窗口 lead 对中间稀疏行透传 None（而非错位读到相邻值）。
    #[test]
    fn test_sparse_middle_row_window_null_passthrough() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(35, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(35, 200, &[(1, TypedValue::F64(9.0))], &[(0, 0xC0)], 0);
        memtable.insert(35, 300, &[(0, TypedValue::F64(3.0))], &[(0, 0xC0)], 0);

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT lead(col_0) OVER (ORDER BY ts) FROM table_35",
            ))
            .unwrap();

        let batch = &result.batches[0];
        let lead = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // 修复前：ts200 的 col_0 会错位读到 3.0，导致 lead[0]=3.0（错误）。
        assert!(
            lead.is_null(0),
            "目标行 ts200 的 col_0 为 None → lead 输出 None"
        );
        assert_eq!(lead.value(1), 3.0);
        assert!(lead.is_null(2), "分区尾越界 → NULL");
    }

    /// 缺口 1（TSBS #13 形态）：`avg + lead(avg)` 端到端——先桶聚合、再窗口翻转。
    #[test]
    fn test_window_lead_avg_over_time_bucket() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let ten_min = 600_000_000_000i64;
        memtable.insert(36, 0, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(
            36,
            300_000_000_000,
            &[(0, TypedValue::F64(3.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(36, ten_min, &[(0, TypedValue::F64(10.0))], &[(0, 0xC0)], 0);
        memtable.insert(
            36,
            ten_min * 2,
            &[(0, TypedValue::F64(30.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT avg(col_0), lead(avg(col_0)) OVER (ORDER BY ts) FROM table_36 SAMPLE BY 10m",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3, "三个 10m 桶");
        assert_eq!(batch.num_columns(), 3, "隐式 ts + avg + lead(avg)");

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 0);
        assert_eq!(ts.value(1), ten_min);
        assert_eq!(ts.value(2), ten_min * 2);

        let avg = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(avg.value(0), 2.0, "桶0 avg = (1+3)/2");
        assert_eq!(avg.value(1), 10.0);
        assert_eq!(avg.value(2), 30.0);

        // lead(avg) 结果类型 = avg 结果类型（F64），类型保真。
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(2).name(), "lead(avg(col_0))");
        let lead = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 10.0);
        assert_eq!(lead.value(1), 30.0);
        assert!(lead.is_null(2), "末桶越界 → NULL");
    }

    /// 缺口 1：内嵌聚合（无兄弟 avg 投影）`lead(avg(col_0))` 也能独立计算。
    #[test]
    fn test_window_lead_avg_embedded_only() {
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        let ten_min = 600_000_000_000i64;
        memtable.insert(37, 0, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.insert(
            37,
            300_000_000_000,
            &[(0, TypedValue::F64(3.0))],
            &[(0, 0xC0)],
            0,
        );
        memtable.insert(37, ten_min, &[(0, TypedValue::F64(10.0))], &[(0, 0xC0)], 0);
        memtable.insert(
            37,
            ten_min * 2,
            &[(0, TypedValue::F64(30.0))],
            &[(0, 0xC0)],
            0,
        );

        let executor = QueryExecutor::new(memtable, PathBuf::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(executor.execute(
                TenantId::DEFAULT,
                "SELECT lead(avg(col_0)) OVER (ORDER BY ts) FROM table_37 SAMPLE BY 10m",
            ))
            .unwrap();

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch.num_columns(),
            2,
            "隐式 ts + lead(avg)（avg 不单独输出）"
        );

        let lead = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 10.0);
        assert_eq!(lead.value(1), 30.0);
        assert!(lead.is_null(2));
    }
}

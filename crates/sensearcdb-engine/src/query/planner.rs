//! QueryPlanner：SQL 解析 → LogicalPlan + 列投影 + LIMIT 下推。
//!
//! QueryPlanner 将 SQL 查询字符串解析为 `QueryPlan`，
//! 包含表名解析、列投影、时间范围过滤和 LIMIT 下推。

use sensearcdb_common::{AggFunction, ColumnRef, SenseArcError, TypedValue};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr, OrderByExpr, Select,
    SelectItem, SetExpr, Statement, TableFactor, UnaryOperator, Value as SqlValue, WindowType,
};
use sqlparser::parser::Parser;

use crate::query::dialect::{extract_sample_by, SenseArcDialect};

/// 投影表达式。
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionExpr {
    /// `*`：由执行器展开为该表全部实际列（含 `ts` 时间轴）。
    Wildcard,
    /// 普通数据列：`ColumnRef { table_id, column_id }`。
    Column(ColumnRef),
    /// 时间戳伪列（epoch 纳秒，Int64 非空）。
    Timestamp,
    /// 聚合表达式：`COUNT/AVG/SUM/MIN/MAX/FIRST/LAST/STDDEV(col)`。
    Aggregate(AggCall),
    /// 标量表达式（P5：`CASE WHEN ... THEN ... [ELSE ...] END` 独立投影）。
    Expr(ScalarExpr),
    /// 窗口函数（P6：`LEAD/LAG(col[, offset[, default]]) OVER (...)`）。
    Window(WindowSpec),
}

/// 标量值表达式（P5：CASE 的 THEN/ELSE 值，及独立投影标量表达式）。
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarExpr {
    /// 字面量（类型保真：整数 → I64/U64，浮点 → F64，字符串 → String）。
    Literal(TypedValue),
    /// 数据列引用。
    Column(ColumnRef),
    /// 嵌套 CASE（searched CASE；`Box` 打破递归类型）。
    Case(Box<CaseExpr>),
}

/// `CASE WHEN cond THEN value [WHEN cond THEN value ...] [ELSE value] END`。
#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpr {
    /// `(条件, 值)` 分支，按顺序短路求值。
    pub branches: Vec<(Predicate, ScalarExpr)>,
    /// `ELSE` 值；缺省返回 NULL。
    pub else_expr: Option<ScalarExpr>,
}

/// 聚合参数（P5）：普通列 或 标量表达式（CASE / 字面量）。
#[derive(Debug, Clone, PartialEq)]
pub enum AggArg {
    /// 普通数据列。
    Column(ColumnRef),
    /// 标量表达式（CASE WHEN 等）。
    Expr(ScalarExpr),
}

/// 聚合调用描述（P3 + P5）。
///
/// `COUNT(*)` 的 `arg` 为 `None`；其余聚合的 `arg` 为 `Some(AggArg)`。
/// `filter` 为 `FILTER (WHERE ...)` 的列谓词（P5，复用 P2 的 `Predicate`）。
#[derive(Debug, Clone, PartialEq)]
pub struct AggCall {
    /// 聚合函数（复用 common `AggFunction`）。
    pub function: AggFunction,
    /// 聚合参数；`COUNT(*)` 为 `None`。
    pub arg: Option<AggArg>,
    /// `FILTER (WHERE ...)` 谓词；`None` 表示无条件聚合。
    pub filter: Option<Predicate>,
}

/// 窗口函数种类（P6：LEAD/LAG OVER）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunction {
    /// `LEAD(col[, offset[, default]])`：取当前行之后第 `offset` 行的值。
    Lead,
    /// `LAG(col[, offset[, default]])`：取当前行之前第 `offset` 行的值。
    Lag,
}

/// 窗口排序键目标（数据列 或 ts 伪列）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderKeyColumn {
    /// `ts` 时间轴（自然升序）。
    Ts,
    /// 数据列。
    Column(ColumnRef),
}

/// 窗口排序键（`OVER (ORDER BY ...)` 的单键）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    /// 排序列。
    pub column: OrderKeyColumn,
    /// `true` = ASC；`false` = DESC。
    pub asc: bool,
}

/// 窗口参数（缺口 1：支持 `lead(avg(col))` 引用聚合结果列）。
#[derive(Debug, Clone, PartialEq)]
pub enum WindowArg {
    /// 普通数据列。
    Column(ColumnRef),
    /// 聚合调用：引用同 SELECT 中的一个聚合结果列（按值匹配兄弟聚合，或作为内嵌聚合独立计算）。
    Aggregate(AggCall),
}

/// 窗口描述（P6，设计 §4.6）。
///
/// 单 `WindowSpec` 描述一个窗口函数：`func(arg[, offset[, default]]) OVER (...)`。
/// 多个窗口函数对应多个 `WindowSpec`（各自挂在投影 `ProjectionExpr::Window` 上）。
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    /// 窗口函数。
    pub func: WindowFunction,
    /// 窗口参数（数据列 或 聚合结果列）。
    pub arg: WindowArg,
    /// 偏移量（>= 1；0 或负数在解析期显式报错）。
    pub offset: i64,
    /// 越界默认值；`None` = NULL（未指定或显式 `NULL`）。
    pub default: Option<TypedValue>,
    /// 分区键（空 = 单分区）。
    pub partition_by: Vec<ColumnRef>,
    /// 排序键（空 = ts 自然升序）。
    pub order_by: Vec<OrderKey>,
}

/// 时间桶对齐模式（P4，日历桶正确性关键）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeAlign {
    /// epoch 对齐：`floor(ts / interval) * interval`（10s/1m/1h）。
    Epoch,
    /// 日历日：UTC 00:00:00（Unix epoch 锚定天然等于 UTC 午夜）。
    Day,
    /// 日历周：ISO 周一锚定。
    Week,
    /// 日历月：UTC 月首锚定。
    Month,
}

/// 时间桶定义（P4）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeBucket {
    /// 桶宽（纳秒）；日历桶（Day/Week/Month）不使用该值（宽度可变），仅为语义占位。
    pub interval_ns: i64,
    /// 桶对齐模式。
    pub align: TimeAlign,
}

/// 列值比较运算符（WHERE 非 ts 谓词）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `=`
    Eq,
    /// `!=` / `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
}

/// 列值谓词树（WHERE 非 ts 部分）。
///
/// 叶子作用于 `column_id`（宽表列）；`ts` 谓词不进入该树，仍单独提取为
/// `QueryPlan.ts_start/ts_end`（下推时间索引）。SQL 三值逻辑：列值为 `None`
/// 时任何比较结果为 `UNKNOWN`，该行不命中。
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// 单列比较：`column <op> literal`。
    Comparison {
        column: u16,
        op: CmpOp,
        literal: TypedValue,
    },
    /// `left AND right`。
    And(Box<Predicate>, Box<Predicate>),
    /// `left OR right`。
    Or(Box<Predicate>, Box<Predicate>),
    /// `column IN (values...)`。
    In {
        column: u16,
        values: Vec<TypedValue>,
    },
    /// `column BETWEEN low AND high`（两端含）。
    Between {
        column: u16,
        low: TypedValue,
        high: TypedValue,
    },
}

/// 查询计划。
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// 目标表名称（支持 `table_42.col_7` 列限定语法）。
    pub table_name: String,
    /// 需要投影的列表达式列表。
    pub projection: Vec<ProjectionExpr>,
    /// 时间范围起始（epoch 纳秒）。None 表示无限制。
    pub ts_start: Option<i64>,
    /// 时间范围结束（epoch 纳秒）。None 表示无限制。
    pub ts_end: Option<i64>,
    /// 起始边界是否含（`>=` 为 true、`>` 为 false）。仅当 `ts_start` 为 Some 时有效。
    pub ts_start_inclusive: bool,
    /// 结束边界是否含（`<=` 为 true、`<` 为 false）。仅当 `ts_end` 为 Some 时有效。
    pub ts_end_inclusive: bool,
    /// WHERE 非 ts 列谓词（下推扫描阶段过滤）。None 表示无列过滤。
    pub predicate: Option<Predicate>,
    /// GROUP BY 分组键（P3：非时间键；时间桶 GROUP BY ts 由 P4 支持）。
    pub group_by: Vec<ColumnRef>,
    /// 时间桶（P4：`SAMPLE BY <interval>` 或 `GROUP BY time(<interval>)`）。
    pub time_bucket: Option<TimeBucket>,
    /// LIMIT 子句（下推优化）。
    pub limit: Option<usize>,
}

impl Default for QueryPlan {
    fn default() -> Self {
        Self {
            table_name: String::new(),
            projection: vec![ProjectionExpr::Wildcard],
            ts_start: None,
            ts_end: None,
            ts_start_inclusive: true,
            ts_end_inclusive: true,
            predicate: None,
            group_by: Vec::new(),
            time_bucket: None,
            limit: None,
        }
    }
}

/// SQL 查询规划器。
///
/// 将用户 SQL 解析为引擎可执行的 `QueryPlan`。
pub struct QueryPlanner;

impl QueryPlanner {
    /// 解析 SQL 查询。
    ///
    /// # 参数
    /// - `sql`: SQL 查询字符串。
    ///
    /// # 返回值
    /// 解析后的 `QueryPlan`。
    pub fn plan(sql: &str) -> Result<QueryPlan, SenseArcError> {
        let dialect = SenseArcDialect;

        // P4：预提取 SAMPLE BY 子句（sqlparser 不识别该扩展），再接活死代码 `extract_sample_by`。
        let (cleaned_sql, sample_by) = extract_sample_by(sql).map_err(SenseArcError::Internal)?;
        let ast = Parser::parse_sql(&dialect, &cleaned_sql)
            .map_err(|e| SenseArcError::Internal(format!("SQL parse error: {}", e)))?;

        if ast.is_empty() {
            return Err(SenseArcError::Internal("empty SQL statement".into()));
        }

        let mut plan = QueryPlan::default();

        // P4：SAMPLE BY 预提取后先写入 time_bucket，使 `process_select` 的
        // 「时间桶需伴随聚合」与「SAMPLE BY 与 GROUP BY time 冲突」校验可见。
        if let Some(sb) = sample_by {
            plan.time_bucket = Some(parse_time_bucket(&sb.interval_raw)?);
        }

        match &ast[0] {
            Statement::Query(query) => {
                Self::process_query(&mut plan, query)?;
            }
            _ => {
                return Err(SenseArcError::Internal(
                    "only SELECT statements are supported".into(),
                ));
            }
        }

        Ok(plan)
    }

    /// 处理查询语句。
    fn process_query(
        plan: &mut QueryPlan,
        query: &sqlparser::ast::Query,
    ) -> Result<(), SenseArcError> {
        match &*query.body {
            SetExpr::Select(select) => {
                Self::process_select(plan, select)?;
            }
            _ => {
                return Err(SenseArcError::Internal(
                    "only simple SELECT is supported".into(),
                ));
            }
        }

        // LIMIT 下推
        if let Some(limit_expr) = &query.limit {
            if let Expr::Value(sqlparser::ast::Value::Number(num, _)) = limit_expr {
                if let Ok(n) = num.parse::<usize>() {
                    plan.limit = Some(n);
                }
            }
        }

        Ok(())
    }

    /// 处理 SELECT 子句。
    fn process_select(plan: &mut QueryPlan, select: &Select) -> Result<(), SenseArcError> {
        // 解析 FROM 子句
        if select.from.is_empty() {
            return Err(SenseArcError::Internal("missing FROM clause".into()));
        }

        for table_with_joins in &select.from {
            match &table_with_joins.relation {
                TableFactor::Table { name, .. } => {
                    plan.table_name = name.to_string();
                }
                _ => {
                    return Err(SenseArcError::Internal(
                        "only simple table references are supported".into(),
                    ));
                }
            }
        }

        // 解析投影列（结构化 `ProjectionExpr`）。
        //
        // table_id 从 FROM 表名解析；不可解析时用 0 占位（执行器会先拒绝
        // 不可解析表，投影列的 table_id 占位不会生效）。
        let table_id = resolve_table_id(&plan.table_name)
            .map(|(t, _)| t)
            .unwrap_or(0);
        plan.projection = select
            .projection
            .iter()
            .map(|item| Self::parse_projection(item, table_id))
            .collect::<Result<Vec<_>, _>>()?;

        // 解析 WHERE 子句：ts 谓词提取为时间范围，其余列谓词组成 `Predicate` 树。
        if let Some(selection) = &select.selection {
            plan.predicate = Self::parse_where(plan, selection)?;
        }

        // 解析 GROUP BY 分组键（含 `GROUP BY time(interval)` 时间桶提取）。
        plan.group_by = Self::parse_group_by(plan, select, table_id)?;

        // 校验：GROUP BY / 时间桶必须伴随聚合函数（避免静默丢弃分组语义）。
        // 缺口 1：窗口参数为聚合（如 `lead(avg(col_0))`）同样视作「存在聚合」。
        let has_aggregate = plan.projection.iter().any(|e| match e {
            ProjectionExpr::Aggregate(_) => true,
            ProjectionExpr::Window(spec) => matches!(spec.arg, WindowArg::Aggregate(_)),
            _ => false,
        });
        if !plan.group_by.is_empty() && !has_aggregate {
            return Err(SenseArcError::Internal(
                "GROUP BY 需要至少一个聚合函数（P3 仅支持「聚合 + 分组」，纯去重查询暂不支持）"
                    .into(),
            ));
        }
        if plan.time_bucket.is_some() && !has_aggregate {
            return Err(SenseArcError::Internal(
                "SAMPLE BY / 时间桶需要至少一个聚合函数".into(),
            ));
        }

        Ok(())
    }

    /// 将单个投影项解析为 `ProjectionExpr`。
    fn parse_projection(item: &SelectItem, table_id: u32) -> Result<ProjectionExpr, SenseArcError> {
        match item {
            SelectItem::Wildcard(_) => Ok(ProjectionExpr::Wildcard),
            SelectItem::QualifiedWildcard(..) => Ok(ProjectionExpr::Wildcard),
            SelectItem::UnnamedExpr(expr) => Self::parse_projection_expr(expr, table_id),
            SelectItem::ExprWithAlias { expr, .. } => Self::parse_projection_expr(expr, table_id),
        }
    }

    /// 将投影表达式解析为 `ProjectionExpr`。
    ///
    /// schema-less 引擎无法把任意列名映射到列 ID，因此采用显式约定：
    /// - `ts` / `time`（大小写不敏感）→ `Timestamp` 伪列；
    /// - `value` → 列 0（N=1 单列退化语义，兼容既有 `SELECT value` 查询）；
    /// - `col_<id>` / `col<id>` / `<数字>` → 数字列 ID。
    /// 其余列名在 P1 阶段显式报错（不再静默退化为「首列」，避免读错列）。
    fn parse_projection_expr(expr: &Expr, table_id: u32) -> Result<ProjectionExpr, SenseArcError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                if name == "ts" || name == "time" {
                    return Ok(ProjectionExpr::Timestamp);
                }
                if name == "value" {
                    return Ok(ProjectionExpr::Column(ColumnRef::new(table_id, 0)));
                }
                if let Some(cid) = parse_column_id(&ident.value) {
                    return Ok(ProjectionExpr::Column(ColumnRef::new(table_id, cid)));
                }
                Err(SenseArcError::Internal(format!(
                    "unsupported projection column `{}`: schema-less engine supports `ts`, `value`, `col_<id>` or a numeric column id",
                    ident.value
                )))
            }
            Expr::Value(sqlparser::ast::Value::Number(num, _)) => parse_column_id(num)
                .map(|cid| ProjectionExpr::Column(ColumnRef::new(table_id, cid)))
                .ok_or_else(|| {
                    SenseArcError::Internal(format!("invalid numeric column id: {}", num))
                }),
            Expr::Function(func) => {
                if func.over.is_some() {
                    Self::parse_window(func, table_id)
                } else {
                    Self::parse_aggregate(func, table_id)
                }
            }
            Expr::Case { .. } => Ok(ProjectionExpr::Expr(ScalarExpr::Case(Box::new(
                Self::parse_case(expr, table_id)?,
            )))),
            _ => Err(SenseArcError::Internal(format!(
                "unsupported projection expression: {}",
                expr
            ))),
        }
    }

    /// 将聚合函数调用解析为 `ProjectionExpr::Aggregate`。
    fn parse_aggregate(func: &Function, table_id: u32) -> Result<ProjectionExpr, SenseArcError> {
        Ok(ProjectionExpr::Aggregate(Self::parse_agg_call(
            func, table_id,
        )?))
    }

    /// 将聚合函数调用解析为 `AggCall`（供投影与窗口参数共用，缺口 1）。
    fn parse_agg_call(func: &Function, table_id: u32) -> Result<AggCall, SenseArcError> {
        let name = func.name.to_string().to_ascii_uppercase();
        let function = match name.as_str() {
            "COUNT" => AggFunction::Count,
            "AVG" => AggFunction::Avg,
            "SUM" => AggFunction::Sum,
            "MIN" => AggFunction::Min,
            "MAX" => AggFunction::Max,
            "FIRST" => AggFunction::First,
            "LAST" => AggFunction::Last,
            "STDDEV" => AggFunction::StdDev,
            other => {
                return Err(SenseArcError::Internal(format!(
                    "unsupported aggregate function: {other}"
                )));
            }
        };

        if func.distinct {
            return Err(SenseArcError::Internal("聚合 DISTINCT 暂不支持".into()));
        }

        // P5：`FILTER (WHERE ...)` 复用 P2 的 Predicate 解析。
        let filter = match func.filter.as_deref() {
            Some(where_expr) => Some(Self::parse_predicate_required(where_expr)?),
            None => None,
        };

        let arg = Self::parse_aggregate_arg(func, function, table_id)?;
        Ok(AggCall {
            function,
            arg,
            filter,
        })
    }

    /// 解析窗口函数调用为 `ProjectionExpr::Window`（P6）。
    ///
    /// 语法：`LEAD(col[, offset[, default]]) OVER (PARTITION BY ... ORDER BY ...)`。
    /// 仅支持内联窗口描述（`WindowType::WindowSpec`），不支持命名窗口。
    fn parse_window(func: &Function, table_id: u32) -> Result<ProjectionExpr, SenseArcError> {
        let name = func.name.to_string().to_ascii_uppercase();
        let func_kind = match name.as_str() {
            "LEAD" => WindowFunction::Lead,
            "LAG" => WindowFunction::Lag,
            other => {
                return Err(SenseArcError::Internal(format!(
                    "unsupported window function: {other}（P6 仅支持 LEAD/LAG）"
                )));
            }
        };

        let window_spec = match func.over.as_ref() {
            Some(WindowType::WindowSpec(spec)) => spec,
            Some(WindowType::NamedWindow(name)) => {
                return Err(SenseArcError::Internal(format!(
                    "named window `{name}` 暂不支持，请使用内联 OVER (...)"
                )));
            }
            None => unreachable!("parse_window 仅由 func.over.is_some() 分支调用"),
        };

        if func.distinct {
            return Err(SenseArcError::Internal(
                "window function does not support DISTINCT".into(),
            ));
        }

        // lead/lag 使用 offset 定义帧，无需显式 window frame；显式 frame 显式报错（不静默忽略）。
        if window_spec.window_frame.is_some() {
            return Err(SenseArcError::Internal(
                "lead/lag 暂不支持显式 window frame（ROWS/RANGE ...），请仅用 lead/lag(col[, offset[, default]])".into(),
            ));
        }

        if func.args.is_empty() || func.args.len() > 3 {
            return Err(SenseArcError::Internal(format!(
                "{name} expects 1..3 arguments: col[, offset[, default]]"
            )));
        }

        let arg = Self::parse_window_arg(&func.args[0], table_id)?;

        let mut offset: i64 = 1;
        if func.args.len() >= 2 {
            offset = Self::parse_window_offset(&func.args[1])?;
        }
        // 设计 §4.6：offset 为 0 或负数的边界校验（显式拒绝）。
        if offset <= 0 {
            return Err(SenseArcError::Internal(format!(
                "window offset must be >= 1, got {offset}"
            )));
        }

        let mut default: Option<TypedValue> = None;
        if func.args.len() == 3 {
            default = Self::parse_window_default(&func.args[2])?;
        }

        let partition_by = window_spec
            .partition_by
            .iter()
            .map(|e| Self::parse_window_partition_key(e, table_id))
            .collect::<Result<Vec<_>, _>>()?;

        let order_by = window_spec
            .order_by
            .iter()
            .map(|e| Self::parse_window_order_key(e, table_id))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ProjectionExpr::Window(WindowSpec {
            func: func_kind,
            arg,
            offset,
            default,
            partition_by,
            order_by,
        }))
    }

    /// 解析窗口函数首参（`arg`）：数据列 或 聚合调用（缺口 1：`lead(avg(col))`）。
    fn parse_window_arg(arg: &FunctionArg, table_id: u32) -> Result<WindowArg, SenseArcError> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
                // 聚合调用（无 OVER）→ 引用聚合结果列。
                Expr::Function(f) if f.over.is_none() => {
                    Ok(WindowArg::Aggregate(Self::parse_agg_call(f, table_id)?))
                }
                _ => Ok(WindowArg::Column(Self::parse_window_column(
                    expr,
                    table_id,
                    "window argument",
                )?)),
            },
            _ => Err(SenseArcError::Internal(
                "window argument must be a column reference or aggregate".into(),
            )),
        }
    }

    /// 解析窗口偏移量：整数字面量。
    fn parse_window_offset(arg: &FunctionArg) -> Result<i64, SenseArcError> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                Self::eval_i64(expr).ok_or_else(|| {
                    SenseArcError::Internal("window offset must be an integer literal".into())
                })
            }
            _ => Err(SenseArcError::Internal(
                "window offset must be an integer literal".into(),
            )),
        }
    }

    /// 解析窗口默认值：字面量 或 显式 `NULL`（→ `None`）。
    fn parse_window_default(arg: &FunctionArg) -> Result<Option<TypedValue>, SenseArcError> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(SqlValue::Null))) => Ok(None),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                Ok(Some(Self::eval_literal(expr)?))
            }
            _ => Err(SenseArcError::Internal(
                "window default must be a literal or NULL".into(),
            )),
        }
    }

    /// 解析窗口分区键：数据列。
    fn parse_window_partition_key(expr: &Expr, table_id: u32) -> Result<ColumnRef, SenseArcError> {
        Self::parse_window_column(expr, table_id, "window partition")
    }

    /// 解析窗口 `ORDER BY` 单键：ts 伪列 或 数据列。
    fn parse_window_order_key(
        expr: &OrderByExpr,
        table_id: u32,
    ) -> Result<OrderKey, SenseArcError> {
        let column = match &expr.expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                if name == "ts" || name == "time" {
                    OrderKeyColumn::Ts
                } else if name == "value" {
                    OrderKeyColumn::Column(ColumnRef::new(table_id, 0))
                } else if let Some(cid) = parse_column_id(&ident.value) {
                    OrderKeyColumn::Column(ColumnRef::new(table_id, cid))
                } else {
                    return Err(SenseArcError::Internal(format!(
                        "unknown window ORDER BY column `{}`",
                        ident.value
                    )));
                }
            }
            Expr::Value(SqlValue::Number(num, _)) => parse_column_id(num)
                .map(|cid| OrderKeyColumn::Column(ColumnRef::new(table_id, cid)))
                .ok_or_else(|| {
                    SenseArcError::Internal(format!("invalid numeric column id: {num}"))
                })?,
            _ => {
                return Err(SenseArcError::Internal(format!(
                    "unsupported window ORDER BY expression: {}",
                    expr.expr
                )));
            }
        };
        let asc = expr.asc.unwrap_or(true);
        Ok(OrderKey { column, asc })
    }

    /// 解析窗口列引用（`arg` / 分区键）：必须是数据列（ts 伪列显式拒绝）。
    fn parse_window_column(
        expr: &Expr,
        table_id: u32,
        context: &str,
    ) -> Result<ColumnRef, SenseArcError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                if name == "ts" || name == "time" {
                    return Err(SenseArcError::Internal(format!(
                        "{context} 不支持 ts 伪列（窗口参数/分区键必须是数据列）"
                    )));
                }
                if name == "value" {
                    return Ok(ColumnRef::new(table_id, 0));
                }
                if let Some(cid) = parse_column_id(&ident.value) {
                    return Ok(ColumnRef::new(table_id, cid));
                }
                Err(SenseArcError::Internal(format!(
                    "unknown {context} column `{}`",
                    ident.value
                )))
            }
            Expr::Value(SqlValue::Number(num, _)) => parse_column_id(num)
                .map(|cid| ColumnRef::new(table_id, cid))
                .ok_or_else(|| {
                    SenseArcError::Internal(format!("invalid numeric column id: {num}"))
                }),
            _ => Err(SenseArcError::Internal(format!(
                "unsupported {context} expression: {expr}"
            ))),
        }
    }

    /// 解析聚合函数参数：`COUNT(*)` → `None`；其余 → `Some(AggArg)`。
    fn parse_aggregate_arg(
        func: &Function,
        function: AggFunction,
        table_id: u32,
    ) -> Result<Option<AggArg>, SenseArcError> {
        if func.args.len() != 1 {
            return Err(SenseArcError::Internal(format!(
                "aggregate function `{function:?}` expects exactly one argument"
            )));
        }

        match &func.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                if function == AggFunction::Count {
                    Ok(None)
                } else {
                    Err(SenseArcError::Internal(
                        "only COUNT(*) accepts `*` as argument".into(),
                    ))
                }
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                Ok(Some(Self::parse_aggregate_arg_expr(expr, table_id)?))
            }
            _ => Err(SenseArcError::Internal(
                "unsupported aggregate argument (expected a column, CASE or `*`)".into(),
            )),
        }
    }

    /// 将聚合参数表达式解析为 `AggArg`：普通列 或 标量表达式（CASE）。
    fn parse_aggregate_arg_expr(expr: &Expr, table_id: u32) -> Result<AggArg, SenseArcError> {
        match expr {
            Expr::Identifier(_) | Expr::Value(SqlValue::Number(_, _)) => Ok(AggArg::Column(
                Self::parse_aggregate_column(expr, table_id)?,
            )),
            Expr::Case { .. } => Ok(AggArg::Expr(ScalarExpr::Case(Box::new(Self::parse_case(
                expr, table_id,
            )?)))),
            _ => Err(SenseArcError::Internal(format!(
                "unsupported aggregate argument expression: {expr}"
            ))),
        }
    }

    /// 将聚合参数列解析为 `ColumnRef`。
    fn parse_aggregate_column(expr: &Expr, table_id: u32) -> Result<ColumnRef, SenseArcError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                if name == "ts" || name == "time" {
                    return Err(SenseArcError::Internal(
                        "聚合参数不支持 ts 伪列（时间桶聚合由 P4 支持）".into(),
                    ));
                }
                if name == "value" {
                    return Ok(ColumnRef::new(table_id, 0));
                }
                if let Some(cid) = parse_column_id(&ident.value) {
                    return Ok(ColumnRef::new(table_id, cid));
                }
                Err(SenseArcError::Internal(format!(
                    "unknown aggregate column `{}`",
                    ident.value
                )))
            }
            Expr::Value(SqlValue::Number(num, _)) => parse_column_id(num)
                .map(|cid| ColumnRef::new(table_id, cid))
                .ok_or_else(|| {
                    SenseArcError::Internal(format!("invalid numeric column id: {num}"))
                }),
            _ => Err(SenseArcError::Internal(format!(
                "unsupported aggregate argument expression: {expr}"
            ))),
        }
    }

    /// 解析必须产出列谓词的表达式（用于 `FILTER (WHERE ...)` / `CASE WHEN` 条件）。
    fn parse_predicate_required(expr: &Expr) -> Result<Predicate, SenseArcError> {
        // 用临时 plan 捕获 ts 谓词：`parse_where` 会把 ts 部分写回 scratch 时间范围。
        // 必须整体拒绝 ts（含「ts + 列」混用），否则 ts 部分会被静默丢弃。
        let mut scratch = QueryPlan::default();
        let pred = Self::parse_where(&mut scratch, expr)?;
        if scratch.ts_start.is_some() || scratch.ts_end.is_some() {
            return Err(SenseArcError::Internal(
                "FILTER/CASE WHEN 条件不支持 ts 谓词，请放到主查询 WHERE".into(),
            ));
        }
        match pred {
            Some(p) => Ok(p),
            None => Err(SenseArcError::Internal(
                "FILTER/CASE WHEN 条件必须是列比较谓词".into(),
            )),
        }
    }

    /// 解析标量值表达式（CASE 的 THEN/ELSE 值）。
    fn parse_scalar_expr(expr: &Expr, table_id: u32) -> Result<ScalarExpr, SenseArcError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                if name == "value" {
                    return Ok(ScalarExpr::Column(ColumnRef::new(table_id, 0)));
                }
                if let Some(cid) = parse_column_id(&ident.value) {
                    return Ok(ScalarExpr::Column(ColumnRef::new(table_id, cid)));
                }
                if name == "ts" || name == "time" {
                    return Err(SenseArcError::Internal(
                        "CASE 值表达式不支持 ts 伪列".into(),
                    ));
                }
                Err(SenseArcError::Internal(format!(
                    "unknown CASE 值列 `{}`",
                    ident.value
                )))
            }
            Expr::Value(_) | Expr::UnaryOp { .. } => {
                Ok(ScalarExpr::Literal(Self::eval_literal(expr)?))
            }
            Expr::Case { .. } => Ok(ScalarExpr::Case(Box::new(Self::parse_case(
                expr, table_id,
            )?))),
            _ => Err(SenseArcError::Internal(format!(
                "unsupported CASE 值表达式: {expr}"
            ))),
        }
    }

    /// 解析 searched CASE：`CASE WHEN cond THEN v [WHEN ...] [ELSE v] END`。
    fn parse_case(expr: &Expr, table_id: u32) -> Result<CaseExpr, SenseArcError> {
        match expr {
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if operand.is_some() {
                    return Err(SenseArcError::Internal(
                        "simple CASE（CASE x WHEN ...）暂不支持，请用 searched CASE（CASE WHEN ...）".into(),
                    ));
                }
                if conditions.is_empty() {
                    return Err(SenseArcError::Internal(
                        "CASE 至少需要一个 WHEN 分支".into(),
                    ));
                }
                if conditions.len() != results.len() {
                    return Err(SenseArcError::Internal(
                        "CASE 的 WHEN 与 THEN 数量不一致".into(),
                    ));
                }

                let mut branches = Vec::with_capacity(conditions.len());
                for (cond, then_expr) in conditions.iter().zip(results.iter()) {
                    let pred = Self::parse_predicate_required(cond)?;
                    let value = Self::parse_scalar_expr(then_expr, table_id)?;
                    branches.push((pred, value));
                }
                let else_expr = match else_result {
                    Some(e) => Some(Self::parse_scalar_expr(e, table_id)?),
                    None => None,
                };
                Ok(CaseExpr {
                    branches,
                    else_expr,
                })
            }
            _ => Err(SenseArcError::Internal("expected CASE expression".into())),
        }
    }

    /// 解析 GROUP BY 分组键（P3 非时间键 + P4 `GROUP BY time(interval)` 时间桶）。
    ///
    /// `GROUP BY time('1d')` 会写入 `plan.time_bucket` 并从分组键中剔除（时间桶
    /// 由执行器作为隐式首键处理）。
    fn parse_group_by(
        plan: &mut QueryPlan,
        select: &Select,
        table_id: u32,
    ) -> Result<Vec<ColumnRef>, SenseArcError> {
        match &select.group_by {
            GroupByExpr::Expressions(exprs) => {
                let mut keys = Vec::with_capacity(exprs.len());
                for e in exprs {
                    if let Some(bucket) = Self::try_parse_group_by_time(e)? {
                        if plan.time_bucket.is_some() {
                            return Err(SenseArcError::Internal(
                                "不能同时使用 SAMPLE BY 与 GROUP BY time(...)".into(),
                            ));
                        }
                        plan.time_bucket = Some(bucket);
                    } else {
                        keys.push(Self::parse_group_key(e, table_id)?);
                    }
                }
                Ok(keys)
            }
            GroupByExpr::All => Err(SenseArcError::Internal("GROUP BY ALL 暂不支持".into())),
        }
    }

    /// 识别 `GROUP BY time('interval')` / `time_bucket('interval')`，解析为 `TimeBucket`。
    ///
    /// 返回 `Ok(None)` 表示该表达式不是时间桶函数（交由普通分组键解析）。
    fn try_parse_group_by_time(expr: &Expr) -> Result<Option<TimeBucket>, SenseArcError> {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_ascii_lowercase();
                if name != "time" && name != "time_bucket" {
                    return Ok(None);
                }
                if func.args.len() != 1 {
                    return Err(SenseArcError::Internal(
                        "time(interval) expects exactly one argument".into(),
                    ));
                }
                match &func.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s),
                    ))) => Ok(Some(parse_time_bucket(s)?)),
                    _ => Err(SenseArcError::Internal(
                        "time(interval) 的 interval 必须是字符串字面量（如 time('1d')）".into(),
                    )),
                }
            }
            _ => Ok(None),
        }
    }

    /// 将单个 GROUP BY 键解析为数据列 `ColumnRef`（ts 时间键由 P4 支持）。
    fn parse_group_key(expr: &Expr, table_id: u32) -> Result<ColumnRef, SenseArcError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                if name == "ts" || name == "time" {
                    return Err(SenseArcError::Internal(
                        "GROUP BY ts（时间桶）由 P4 支持，P3 仅支持非时间列分组".into(),
                    ));
                }
                if name == "value" {
                    return Ok(ColumnRef::new(table_id, 0));
                }
                if let Some(cid) = parse_column_id(&ident.value) {
                    return Ok(ColumnRef::new(table_id, cid));
                }
                Err(SenseArcError::Internal(format!(
                    "unknown GROUP BY column `{}`",
                    ident.value
                )))
            }
            Expr::Value(SqlValue::Number(num, _)) => parse_column_id(num)
                .map(|cid| ColumnRef::new(table_id, cid))
                .ok_or_else(|| {
                    SenseArcError::Internal(format!("invalid numeric column id: {num}"))
                }),
            _ => Err(SenseArcError::Internal(format!(
                "unsupported GROUP BY expression: {expr}"
            ))),
        }
    }

    /// 解析 WHERE 子句：ts 谓词提取为时间范围，其余列谓词组成 `Predicate` 树。
    ///
    /// 返回 `None` 表示该子表达式不含列谓词（例如纯 ts 范围）。
    fn parse_where(plan: &mut QueryPlan, expr: &Expr) -> Result<Option<Predicate>, SenseArcError> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let l = Self::parse_where(plan, left)?;
                    let r = Self::parse_where(plan, right)?;
                    Ok(match (l, r) {
                        (Some(a), Some(b)) => Some(Predicate::And(Box::new(a), Box::new(b))),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        (None, None) => None,
                    })
                }
                BinaryOperator::Or => {
                    // OR 两侧必须都是列谓词：ts 谓词无法拆解到「ts 范围 + 行谓词」模型。
                    let l = Self::parse_where_required(left)?;
                    let r = Self::parse_where_required(right)?;
                    Ok(Some(Predicate::Or(Box::new(l), Box::new(r))))
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => Self::parse_comparison(plan, left, op, right),
                _ => Err(SenseArcError::Internal(format!(
                    "unsupported WHERE operator: {op}"
                ))),
            },
            Expr::Between {
                expr: col_expr,
                negated,
                low,
                high,
            } => Self::parse_between(plan, col_expr, *negated, low, high),
            Expr::InList {
                expr: col_expr,
                list,
                negated,
            } => Self::parse_in(plan, col_expr, *negated, list),
            Expr::Nested(inner) => Self::parse_where(plan, inner),
            _ => Err(SenseArcError::Internal(format!(
                "unsupported WHERE expression: {expr}"
            ))),
        }
    }

    /// 解析必须产出列谓词的子表达式（用于 OR 两侧）。
    fn parse_where_required(expr: &Expr) -> Result<Predicate, SenseArcError> {
        // 用临时 plan 丢弃可能被解析出的 ts 范围；纯 ts 谓词会得到 `None` 并报错。
        let mut scratch = QueryPlan::default();
        match Self::parse_where(&mut scratch, expr)? {
            Some(p) => Ok(p),
            None => Err(SenseArcError::Internal(
                "OR 两侧不支持混入纯时间戳谓词（请用 AND 将 ts 范围与列谓词组合）".into(),
            )),
        }
    }

    /// 解析二元比较：ts 谓词写回时间范围，列谓词产出 `Comparison`。
    fn parse_comparison(
        plan: &mut QueryPlan,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<Option<Predicate>, SenseArcError> {
        match Self::resolve_where_column(left)? {
            WhereColumn::Ts => {
                Self::apply_ts_comparison(plan, op, right)?;
                Ok(None)
            }
            WhereColumn::Data(cid) => {
                let literal = Self::eval_literal(right)?;
                let cmp = map_cmp_op(op)?;
                Ok(Some(Predicate::Comparison {
                    column: cid,
                    op: cmp,
                    literal,
                }))
            }
        }
    }

    /// 解析 BETWEEN：ts 写回范围；列谓词产出 `Between`（`NOT BETWEEN` 展开为 `OR`）。
    fn parse_between(
        plan: &mut QueryPlan,
        col_expr: &Expr,
        negated: bool,
        low: &Expr,
        high: &Expr,
    ) -> Result<Option<Predicate>, SenseArcError> {
        match Self::resolve_where_column(col_expr)? {
            WhereColumn::Ts => {
                if negated {
                    return Err(SenseArcError::Internal("ts NOT BETWEEN 暂不支持".into()));
                }
                let low_v = Self::eval_ts_bound(low)?;
                let high_v = Self::eval_ts_bound(high)?;
                merge_lower_bound(plan, low_v, true);
                merge_upper_bound(plan, high_v, true);
                Ok(None)
            }
            WhereColumn::Data(cid) => {
                let low_v = Self::eval_literal(low)?;
                let high_v = Self::eval_literal(high)?;
                if negated {
                    // NOT BETWEEN → (col < low) OR (col > high)
                    Ok(Some(Predicate::Or(
                        Box::new(Predicate::Comparison {
                            column: cid,
                            op: CmpOp::Lt,
                            literal: low_v,
                        }),
                        Box::new(Predicate::Comparison {
                            column: cid,
                            op: CmpOp::Gt,
                            literal: high_v,
                        }),
                    )))
                } else {
                    Ok(Some(Predicate::Between {
                        column: cid,
                        low: low_v,
                        high: high_v,
                    }))
                }
            }
        }
    }

    /// 解析 IN：列谓词产出 `In`（`NOT IN` 展开为 `AND` 链）。
    fn parse_in(
        _plan: &mut QueryPlan,
        col_expr: &Expr,
        negated: bool,
        list: &[Expr],
    ) -> Result<Option<Predicate>, SenseArcError> {
        match Self::resolve_where_column(col_expr)? {
            WhereColumn::Ts => Err(SenseArcError::Internal("ts IN 谓词暂不支持".into())),
            WhereColumn::Data(cid) => {
                if list.is_empty() {
                    return Err(SenseArcError::Internal("empty IN list".into()));
                }
                let values = list
                    .iter()
                    .map(Self::eval_literal)
                    .collect::<Result<Vec<_>, _>>()?;
                if negated {
                    // NOT IN → (col != v1) AND (col != v2) AND ...
                    let mut acc: Option<Predicate> = None;
                    for v in values {
                        let cmp = Predicate::Comparison {
                            column: cid,
                            op: CmpOp::Ne,
                            literal: v,
                        };
                        acc = Some(match acc {
                            None => cmp,
                            Some(prev) => Predicate::And(Box::new(prev), Box::new(cmp)),
                        });
                    }
                    Ok(acc)
                } else {
                    Ok(Some(Predicate::In {
                        column: cid,
                        values,
                    }))
                }
            }
        }
    }

    /// 将 ts 二元比较写回 `QueryPlan` 时间范围。
    ///
    /// 返回 `Result`：`ts != x` 无法用单区间表示，显式报错（消除静默丢值）。
    fn apply_ts_comparison(
        plan: &mut QueryPlan,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<(), SenseArcError> {
        match op {
            BinaryOperator::Gt => {
                let v = Self::eval_ts_bound(right)?;
                merge_lower_bound(plan, v, false);
                Ok(())
            }
            BinaryOperator::GtEq => {
                let v = Self::eval_ts_bound(right)?;
                merge_lower_bound(plan, v, true);
                Ok(())
            }
            BinaryOperator::Lt => {
                let v = Self::eval_ts_bound(right)?;
                merge_upper_bound(plan, v, false);
                Ok(())
            }
            BinaryOperator::LtEq => {
                let v = Self::eval_ts_bound(right)?;
                merge_upper_bound(plan, v, true);
                Ok(())
            }
            BinaryOperator::Eq => {
                let v = Self::eval_ts_bound(right)?;
                merge_lower_bound(plan, v, true);
                merge_upper_bound(plan, v, true);
                Ok(())
            }
            BinaryOperator::NotEq => Err(SenseArcError::Internal(
                "ts != 谓词暂不支持：其语义为「ts < x OR ts > x」两个区间，\
                 当前查询计划仅支持单一 ts 范围"
                    .into(),
            )),
            other => Err(SenseArcError::Internal(format!(
                "unsupported ts comparison operator: {other}"
            ))),
        }
    }

    /// 解析 ts 比较右值为 i64 纳秒字面量；失败（浮点/列引用等）显式报错。
    fn eval_ts_bound(expr: &Expr) -> Result<i64, SenseArcError> {
        Self::eval_i64(expr).ok_or_else(|| {
            SenseArcError::Internal("ts 比较右值必须是整数纳秒字面量（如 ts > 100）".into())
        })
    }

    /// 解析 WHERE 中的列引用：`ts` 伪列 vs 数据列 ID。
    fn resolve_where_column(expr: &Expr) -> Result<WhereColumn, SenseArcError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                if name == "ts" || name == "time" {
                    return Ok(WhereColumn::Ts);
                }
                if name == "value" {
                    return Ok(WhereColumn::Data(0));
                }
                if let Some(cid) = parse_column_id(&ident.value) {
                    return Ok(WhereColumn::Data(cid));
                }
                Err(SenseArcError::Internal(format!(
                    "unknown column `{}` in WHERE: schema-less engine supports `ts`, `value`, `col_<id>`, `col<id>` or a numeric column id",
                    ident.value
                )))
            }
            Expr::Value(SqlValue::Number(num, _)) => {
                parse_column_id(num).map(WhereColumn::Data).ok_or_else(|| {
                    SenseArcError::Internal(format!("invalid numeric column id: {num}"))
                })
            }
            _ => Err(SenseArcError::Internal(format!(
                "unsupported column reference in WHERE: {expr}"
            ))),
        }
    }

    /// 将 SQL 字面量求值为 `TypedValue`。
    ///
    /// 整数（无小数点/指数）优先 i64，超 i64 范围退 u64；浮点 → f64；
    /// 字符串 → String；布尔 → Bool。负数字面量由 `UnaryOp::Minus` 取反。
    fn eval_literal(expr: &Expr) -> Result<TypedValue, SenseArcError> {
        match expr {
            Expr::Value(SqlValue::Number(num, _)) => parse_number_literal(num),
            Expr::Value(SqlValue::SingleQuotedString(s))
            | Expr::Value(SqlValue::DoubleQuotedString(s)) => Ok(TypedValue::String(s.clone())),
            Expr::Value(SqlValue::Boolean(b)) => Ok(TypedValue::Bool(*b)),
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Minus => match expr.as_ref() {
                    Expr::Value(SqlValue::Number(num, _)) => {
                        negate_literal(parse_number_literal(num)?)
                    }
                    _ => Err(SenseArcError::Internal(format!(
                        "unsupported negative literal: {expr}"
                    ))),
                },
                UnaryOperator::Plus => Self::eval_literal(expr),
                _ => Err(SenseArcError::Internal(format!(
                    "unsupported unary operator in literal: {op}"
                ))),
            },
            _ => Err(SenseArcError::Internal(format!(
                "unsupported literal in WHERE: {expr}"
            ))),
        }
    }

    /// 尝试将表达式求值为 i64。
    fn eval_i64(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(SqlValue::Number(num, _)) => num.parse().ok(),
            Expr::UnaryOp { op, expr } => {
                if *op == UnaryOperator::Minus {
                    Self::eval_i64(expr).map(|v| -v)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// WHERE 列引用的分类结果。
enum WhereColumn {
    /// `ts` / `time` 时间戳伪列。
    Ts,
    /// 数据列（宽表列 ID）。
    Data(u16),
}

/// 将 sqlparser 比较运算符映射为内部 `CmpOp`。
fn map_cmp_op(op: &BinaryOperator) -> Result<CmpOp, SenseArcError> {
    match op {
        BinaryOperator::Eq => Ok(CmpOp::Eq),
        BinaryOperator::NotEq => Ok(CmpOp::Ne),
        BinaryOperator::Lt => Ok(CmpOp::Lt),
        BinaryOperator::LtEq => Ok(CmpOp::LtEq),
        BinaryOperator::Gt => Ok(CmpOp::Gt),
        BinaryOperator::GtEq => Ok(CmpOp::GtEq),
        other => Err(SenseArcError::Internal(format!(
            "unsupported comparison operator: {other}"
        ))),
    }
}

/// 合并 ts 下界：取「更严格」的约束（值更大者；同值则取「不含」更严）。
fn merge_lower_bound(plan: &mut QueryPlan, v: i64, inclusive: bool) {
    match plan.ts_start {
        None => {
            plan.ts_start = Some(v);
            plan.ts_start_inclusive = inclusive;
        }
        Some(cur) => {
            if v > cur {
                plan.ts_start = Some(v);
                plan.ts_start_inclusive = inclusive;
            } else if v == cur {
                plan.ts_start_inclusive = plan.ts_start_inclusive && inclusive;
            }
        }
    }
}

/// 合并 ts 上界：取「更严格」的约束（值更小者；同值则取「不含」更严）。
fn merge_upper_bound(plan: &mut QueryPlan, v: i64, inclusive: bool) {
    match plan.ts_end {
        None => {
            plan.ts_end = Some(v);
            plan.ts_end_inclusive = inclusive;
        }
        Some(cur) => {
            if v < cur {
                plan.ts_end = Some(v);
                plan.ts_end_inclusive = inclusive;
            } else if v == cur {
                plan.ts_end_inclusive = plan.ts_end_inclusive && inclusive;
            }
        }
    }
}

/// 解析数字字面量字符串为 `TypedValue`（整数优先，浮点兜底）。
fn parse_number_literal(s: &str) -> Result<TypedValue, SenseArcError> {
    if s.contains('.') || s.contains('e') || s.contains('E') {
        return s
            .parse::<f64>()
            .map(TypedValue::F64)
            .map_err(|_| SenseArcError::Internal(format!("invalid numeric literal: {s}")));
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(TypedValue::I64(i));
    }
    if let Ok(u) = s.parse::<u64>() {
        return Ok(TypedValue::U64(u));
    }
    s.parse::<f64>()
        .map(TypedValue::F64)
        .map_err(|_| SenseArcError::Internal(format!("invalid numeric literal: {s}")))
}

/// 对字面量取反（保持整数域；`i64::MIN` 溢出时退化为 f64）。
fn negate_literal(v: TypedValue) -> Result<TypedValue, SenseArcError> {
    match v {
        TypedValue::F64(x) => Ok(TypedValue::F64(-x)),
        TypedValue::F32(x) => Ok(TypedValue::F32(-x)),
        TypedValue::I64(x) => Ok(x
            .checked_neg()
            .map(TypedValue::I64)
            .unwrap_or_else(|| TypedValue::F64(-(x as f64)))),
        TypedValue::U64(x) => {
            if x > i64::MAX as u64 {
                Ok(TypedValue::F64(-(x as f64)))
            } else {
                Ok(TypedValue::I64(-(x as i64)))
            }
        }
        other => Err(SenseArcError::Internal(format!(
            "cannot negate non-numeric literal: {other:?}"
        ))),
    }
}

/// 解析时间桶 interval（10s/1m/10m/1h/1d/1w/1mon）为 `TimeBucket`。
///
/// - `s/m/h` → epoch 对齐；
/// - `1d` → 日历日（UTC 午夜）；`1w` → ISO 周一；`1mon` → UTC 月首。
fn parse_time_bucket(interval: &str) -> Result<TimeBucket, SenseArcError> {
    let s = interval.trim();
    let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if num_end == 0 {
        return Err(SenseArcError::Internal(format!(
            "invalid time bucket interval: {interval}"
        )));
    }
    let num: i64 = s[..num_end].parse().map_err(|_| {
        SenseArcError::Internal(format!("invalid time bucket number: {}", &s[..num_end]))
    })?;
    if num <= 0 {
        return Err(SenseArcError::Internal(
            "time bucket interval must be positive".into(),
        ));
    }

    let unit = s[num_end..].to_ascii_lowercase();
    let (interval_ns, align) = match unit.as_str() {
        "s" => (num.checked_mul(1_000_000_000), TimeAlign::Epoch),
        "m" => (num.checked_mul(60_000_000_000), TimeAlign::Epoch),
        "h" => (num.checked_mul(3_600_000_000_000), TimeAlign::Epoch),
        "d" => {
            if num != 1 {
                return Err(SenseArcError::Internal(
                    "calendar day bucket only supports 1d".into(),
                ));
            }
            (Some(86_400_000_000_000i64), TimeAlign::Day)
        }
        "w" => {
            if num != 1 {
                return Err(SenseArcError::Internal(
                    "calendar week bucket only supports 1w".into(),
                ));
            }
            (Some(7 * 86_400_000_000_000i64), TimeAlign::Week)
        }
        "mon" => {
            if num != 1 {
                return Err(SenseArcError::Internal(
                    "calendar month bucket only supports 1mon".into(),
                ));
            }
            // 月宽可变，interval_ns 不使用（仅语义占位）。
            (Some(0), TimeAlign::Month)
        }
        other => {
            return Err(SenseArcError::Internal(format!(
                "unknown time bucket unit: {other}"
            )));
        }
    };

    let interval_ns = interval_ns
        .ok_or_else(|| SenseArcError::Internal("time bucket interval overflow".into()))?;
    Ok(TimeBucket { interval_ns, align })
}

/// 从 `table_name` 轻量解析 `(table_id, Option<column_id>)`。
///
/// 支持列限定语法（对齐写路径 `tag = (table_id, column_id)`）：
/// - `table_42.col_7` / `42.7` → `(42, Some(7))`：只投影列 7
/// - `table_42` / `42`       → `(42, None)`：整表多列
pub(crate) fn resolve_table_id(table_name: &str) -> Option<(u32, Option<u16>)> {
    if let Some((table_part, col_part)) = table_name.rsplit_once('.') {
        let table_id = parse_table_id(table_part)?;
        let column_id = parse_column_id(col_part)?;
        return Some((table_id, Some(column_id)));
    }
    let table_id = parse_table_id(table_name)?;
    Some((table_id, None))
}

/// 解析 `table_<id>` 或纯数字形式的 table_id。
pub(crate) fn parse_table_id(s: &str) -> Option<u32> {
    if let Some(rest) = s.strip_prefix("table_") {
        return rest.parse::<u32>().ok();
    }
    s.parse::<u32>().ok()
}

/// 解析 `col_<id>` / `col<id>` 或纯数字形式的 column_id。
///
/// `col<id>`（无下划线）对齐设计文档 §1 的 `SELECT col0, col1, ...` 目标语法。
pub(crate) fn parse_column_id(s: &str) -> Option<u16> {
    if let Some(rest) = s.strip_prefix("col_") {
        return rest.parse::<u16>().ok();
    }
    if let Some(rest) = s.strip_prefix("col") {
        return rest.parse::<u16>().ok();
    }
    s.parse::<u16>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_simple_select() {
        let plan = QueryPlanner::plan("SELECT value FROM table_42 WHERE ts BETWEEN 100 AND 200")
            .expect("plan failed");
        assert_eq!(plan.table_name, "table_42");
        assert_eq!(plan.ts_start, Some(100));
        assert_eq!(plan.ts_end, Some(200));
        assert_eq!(
            plan.projection,
            vec![ProjectionExpr::Column(ColumnRef::new(42, 0))]
        );
    }

    #[test]
    fn test_plan_with_limit() {
        let plan = QueryPlanner::plan("SELECT * FROM t WHERE ts BETWEEN 0 AND 1000 LIMIT 10")
            .expect("plan failed");
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.projection, vec![ProjectionExpr::Wildcard]);
    }

    #[test]
    fn test_plan_multi_column_projection() {
        let plan = QueryPlanner::plan("SELECT col_0, col_1 FROM table_5").expect("plan failed");
        assert_eq!(plan.table_name, "table_5");
        assert!(plan.ts_start.is_none());
        assert!(plan.ts_end.is_none());
        assert_eq!(
            plan.projection,
            vec![
                ProjectionExpr::Column(ColumnRef::new(5, 0)),
                ProjectionExpr::Column(ColumnRef::new(5, 1)),
            ]
        );
    }

    #[test]
    fn test_plan_col_without_underscore_projection() {
        // 设计文档 §1 目标语法 `SELECT col0, col1, ...`。
        let plan = QueryPlanner::plan("SELECT col0, col1 FROM table_5").expect("plan failed");
        assert_eq!(
            plan.projection,
            vec![
                ProjectionExpr::Column(ColumnRef::new(5, 0)),
                ProjectionExpr::Column(ColumnRef::new(5, 1)),
            ]
        );
    }

    #[test]
    fn test_plan_timestamp_projection() {
        let plan = QueryPlanner::plan("SELECT ts, col_0 FROM table_42").expect("plan failed");
        assert_eq!(
            plan.projection,
            vec![
                ProjectionExpr::Timestamp,
                ProjectionExpr::Column(ColumnRef::new(42, 0)),
            ]
        );
    }

    #[test]
    fn test_plan_numeric_projection() {
        let plan = QueryPlanner::plan("SELECT 0, 1 FROM table_7").expect("plan failed");
        assert_eq!(
            plan.projection,
            vec![
                ProjectionExpr::Column(ColumnRef::new(7, 0)),
                ProjectionExpr::Column(ColumnRef::new(7, 1)),
            ]
        );
    }

    #[test]
    fn test_plan_unknown_column_rejected() {
        let result = QueryPlanner::plan("SELECT foo FROM table_42");
        assert!(result.is_err(), "未知列名应显式报错而非静默读错列");
    }

    #[test]
    fn test_plan_invalid_sql() {
        let result = QueryPlanner::plan("NOT A VALID SQL!!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_column_qualified_table() {
        // 宽表列限定：FROM `table_42.col_0` 原样保留为表名，
        // 由 executor 的 resolve_table_id 解析出 (table_id, column_id)。
        let plan = QueryPlanner::plan("SELECT * FROM table_42.col_0 WHERE ts BETWEEN 100 AND 200")
            .expect("plan failed");
        assert_eq!(plan.table_name, "table_42.col_0");
        assert_eq!(plan.ts_start, Some(100));
        assert_eq!(plan.ts_end, Some(200));
        assert_eq!(plan.projection, vec![ProjectionExpr::Wildcard]);
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
    fn test_parse_column_id_forms() {
        assert_eq!(parse_column_id("col_0"), Some(0));
        assert_eq!(parse_column_id("col0"), Some(0));
        assert_eq!(parse_column_id("col_7"), Some(7));
        assert_eq!(parse_column_id("col7"), Some(7));
        assert_eq!(parse_column_id("7"), Some(7));
        assert_eq!(parse_column_id("col_x"), None);
        assert_eq!(parse_column_id("value"), None);
    }

    /// P2：单列比较谓词解析为 `Predicate::Comparison`，整数保真为 I64。
    #[test]
    fn test_predicate_comparison() {
        let plan = QueryPlanner::plan("SELECT col_1 FROM table_7 WHERE col_1 > 100").expect("plan");
        assert!(plan.ts_start.is_none());
        assert!(plan.ts_end.is_none());
        assert_eq!(
            plan.predicate,
            Some(Predicate::Comparison {
                column: 1,
                op: CmpOp::Gt,
                literal: TypedValue::I64(100),
            })
        );
    }

    /// P2：AND / OR 组合与括号嵌套。
    #[test]
    fn test_predicate_and_or() {
        let plan = QueryPlanner::plan(
            "SELECT col_0 FROM table_7 WHERE (col_0 > 1 AND col_0 < 9) OR col_0 = 0",
        )
        .expect("plan");
        let pred = plan.predicate.expect("predicate");
        assert!(matches!(pred, Predicate::Or(_, _)));
        if let Predicate::Or(l, _r) = &pred {
            assert!(matches!(l.as_ref(), Predicate::And(_, _)));
        }
    }

    /// P2：IN 列表解析。
    #[test]
    fn test_predicate_in() {
        let plan =
            QueryPlanner::plan("SELECT col_2 FROM table_7 WHERE col_2 IN (1, 2, 3)").expect("plan");
        assert_eq!(
            plan.predicate,
            Some(Predicate::In {
                column: 2,
                values: vec![TypedValue::I64(1), TypedValue::I64(2), TypedValue::I64(3),],
            })
        );
    }

    /// P2：BETWEEN 解析（两端含）。
    #[test]
    fn test_predicate_between() {
        let plan = QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE col_0 BETWEEN 1.5 AND 2.5")
            .expect("plan");
        assert_eq!(
            plan.predicate,
            Some(Predicate::Between {
                column: 0,
                low: TypedValue::F64(1.5),
                high: TypedValue::F64(2.5),
            })
        );
    }

    /// P2：ts 范围 + 列过滤组合——ts 提取为范围，列谓词入树。
    #[test]
    fn test_predicate_ts_range_plus_column_filter() {
        let plan = QueryPlanner::plan(
            "SELECT col_0 FROM table_7 WHERE ts BETWEEN 100 AND 200 AND col_0 > 5",
        )
        .expect("plan");
        assert_eq!(plan.ts_start, Some(100));
        assert_eq!(plan.ts_end, Some(200));
        assert_eq!(
            plan.predicate,
            Some(Predicate::Comparison {
                column: 0,
                op: CmpOp::Gt,
                literal: TypedValue::I64(5),
            })
        );
    }

    /// P2：未知列名谓词显式报错，不再静默丢弃。
    #[test]
    fn test_predicate_unknown_column_rejected() {
        let result = QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE foo > 5");
        assert!(result.is_err(), "未知列名谓词应显式报错");
    }

    /// P2：I64 大字面量保真（不折 f64）。
    #[test]
    fn test_predicate_i64_literal_fidelity() {
        let big = 9_007_199_254_740_993i64; // 2^53 + 1
        let sql = format!("SELECT col_1 FROM table_7 WHERE col_1 = {big}");
        let plan = QueryPlanner::plan(&sql).expect("plan");
        assert_eq!(
            plan.predicate,
            Some(Predicate::Comparison {
                column: 1,
                op: CmpOp::Eq,
                literal: TypedValue::I64(big),
            })
        );
    }

    /// P2：NOT IN 展开为 `AND` 链。
    #[test]
    fn test_predicate_not_in() {
        let plan = QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE col_0 NOT IN (1, 2)")
            .expect("plan");
        let pred = plan.predicate.expect("predicate");
        // NOT IN (1,2) → (col_0 != 1) AND (col_0 != 2)
        assert!(matches!(pred, Predicate::And(_, _)));
    }

    /// 修复 1：`ts != x` 显式报错，不再静默返回全量。
    #[test]
    fn test_ts_neq_rejected() {
        let result = QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE ts != 100");
        assert!(result.is_err(), "ts != 谓词应显式报错而非静默忽略");
    }

    /// 修复 2：`ts > x` 与 `ts >= x` 边界开闭区分。
    #[test]
    fn test_ts_boundary_inclusivity() {
        let gt = QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE ts > 100").expect("plan");
        assert_eq!(gt.ts_start, Some(100));
        assert!(!gt.ts_start_inclusive, "ts > 100 不含 100");

        let gte = QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE ts >= 100").expect("plan");
        assert_eq!(gte.ts_start, Some(100));
        assert!(gte.ts_start_inclusive, "ts >= 100 含 100");

        let lt = QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE ts < 200").expect("plan");
        assert_eq!(lt.ts_end, Some(200));
        assert!(!lt.ts_end_inclusive, "ts < 200 不含 200");

        let lte = QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE ts <= 200").expect("plan");
        assert_eq!(lte.ts_end, Some(200));
        assert!(lte.ts_end_inclusive, "ts <= 200 含 200");
    }

    /// 修复 3：ts 右值非整数纳秒字面量（浮点/列引用）显式报错，不再静默丢 WHERE。
    #[test]
    fn test_ts_non_integer_rhs_rejected() {
        assert!(
            QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE ts > 100.5").is_err(),
            "ts > 100.5 应显式报错而非静默忽略"
        );
        assert!(
            QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE ts > col_0").is_err(),
            "ts > col_0 应显式报错而非静默忽略"
        );
        assert!(
            QueryPlanner::plan("SELECT col_0 FROM table_7 WHERE ts BETWEEN 100.5 AND 200").is_err(),
            "ts BETWEEN 浮点边界应显式报错而非静默忽略"
        );
    }

    /// P3：聚合函数投影解析为 `ProjectionExpr::Aggregate`。
    #[test]
    fn test_plan_aggregate_projection() {
        let plan = QueryPlanner::plan("SELECT avg(col_1) FROM table_7").expect("plan");
        assert!(plan.group_by.is_empty());
        assert_eq!(
            plan.projection,
            vec![ProjectionExpr::Aggregate(AggCall {
                function: AggFunction::Avg,
                arg: Some(AggArg::Column(ColumnRef::new(7, 1))),
                filter: None,
            })]
        );
    }

    /// P3：COUNT(*) 解析为 `arg = None`。
    #[test]
    fn test_plan_count_star() {
        let plan = QueryPlanner::plan("SELECT count(*) FROM table_7").expect("plan");
        assert_eq!(
            plan.projection,
            vec![ProjectionExpr::Aggregate(AggCall {
                function: AggFunction::Count,
                arg: None,
                filter: None,
            })]
        );
    }

    /// P3：GROUP BY 解析，且投影中的分组键列与聚合并存。
    #[test]
    fn test_plan_group_by() {
        let plan = QueryPlanner::plan("SELECT col_0, sum(col_1) FROM table_7 GROUP BY col_0")
            .expect("plan");
        assert_eq!(plan.group_by, vec![ColumnRef::new(7, 0)]);
        assert_eq!(
            plan.projection,
            vec![
                ProjectionExpr::Column(ColumnRef::new(7, 0)),
                ProjectionExpr::Aggregate(AggCall {
                    function: AggFunction::Sum,
                    arg: Some(AggArg::Column(ColumnRef::new(7, 1))),
                    filter: None,
                }),
            ]
        );
    }

    /// P3：未知聚合函数显式报错。
    #[test]
    fn test_plan_unknown_aggregate_rejected() {
        assert!(QueryPlanner::plan("SELECT median(col_0) FROM table_7").is_err());
    }

    /// P3：GROUP BY 无聚合函数显式报错（不静默丢弃 group_by）。
    #[test]
    fn test_plan_group_by_requires_aggregate() {
        assert!(
            QueryPlanner::plan("SELECT col_0 FROM table_7 GROUP BY col_0").is_err(),
            "GROUP BY 无聚合函数应显式报错"
        );
    }

    /// P4：SAMPLE BY 提取为时间桶（epoch 对齐 + 日历桶）。
    #[test]
    fn test_plan_sample_by() {
        let epoch =
            QueryPlanner::plan("SELECT avg(col_0) FROM table_7 SAMPLE BY 10m").expect("plan");
        assert_eq!(
            epoch.time_bucket,
            Some(TimeBucket {
                interval_ns: 600_000_000_000,
                align: TimeAlign::Epoch
            })
        );
        assert!(epoch.group_by.is_empty());

        let day = QueryPlanner::plan("SELECT count(*) FROM table_7 SAMPLE BY 1d").expect("plan");
        assert_eq!(
            day.time_bucket,
            Some(TimeBucket {
                interval_ns: 86_400_000_000_000,
                align: TimeAlign::Day
            })
        );
    }

    /// P4：`GROUP BY time('1d')` 与 SAMPLE BY 等价，写入 `time_bucket`。
    #[test]
    fn test_plan_group_by_time() {
        let plan =
            QueryPlanner::plan("SELECT count(*) FROM table_7 GROUP BY time('1d')").expect("plan");
        assert_eq!(
            plan.time_bucket,
            Some(TimeBucket {
                interval_ns: 86_400_000_000_000,
                align: TimeAlign::Day
            })
        );
        assert!(plan.group_by.is_empty());
    }

    /// P4：`parse_time_bucket` 各日历单位锚点与非法多日历单位报错。
    #[test]
    fn test_parse_time_bucket_units() {
        assert_eq!(
            parse_time_bucket("1w").unwrap(),
            TimeBucket {
                interval_ns: 7 * 86_400_000_000_000,
                align: TimeAlign::Week
            }
        );
        assert_eq!(
            parse_time_bucket("1mon").unwrap(),
            TimeBucket {
                interval_ns: 0,
                align: TimeAlign::Month
            }
        );
        assert!(parse_time_bucket("2d").is_err(), "多日历日桶不支持");
        assert!(parse_time_bucket("2w").is_err(), "多日历周桶不支持");
        assert!(parse_time_bucket("2mon").is_err(), "多日历月桶不支持");
    }

    /// P4：SAMPLE BY 无聚合函数显式报错。
    #[test]
    fn test_sample_by_requires_aggregate() {
        assert!(QueryPlanner::plan("SELECT col_0 FROM table_7 SAMPLE BY 1d").is_err());
    }

    /// P4：多个时间桶（GROUP BY 中出现两个 time(...)）显式报错。
    #[test]
    fn test_sample_by_group_by_time_conflict() {
        assert!(
            QueryPlanner::plan("SELECT count(*) FROM table_7 GROUP BY time('1d'), time('1h')")
                .is_err()
        );
    }

    /// P5：`COUNT(*) FILTER (WHERE col_0 = 0)` 解析出 `AggCall.filter`。
    #[test]
    fn test_plan_filter() {
        let plan = QueryPlanner::plan("SELECT count(*) FILTER (WHERE col_0 = 0) FROM table_7")
            .expect("plan");
        match &plan.projection[0] {
            ProjectionExpr::Aggregate(call) => {
                assert_eq!(call.function, AggFunction::Count);
                assert!(call.arg.is_none());
                assert!(call.filter.is_some(), "FILTER 应解析为 Predicate");
            }
            other => panic!("expected aggregate, got {other:?}"),
        }
    }

    /// P5：`SUM(CASE WHEN ... THEN ... ELSE ... END)` 解析为 `AggArg::Expr(ScalarExpr::Case)`。
    #[test]
    fn test_plan_case_aggregate_arg() {
        let plan =
            QueryPlanner::plan("SELECT sum(CASE WHEN col_0 = 0 THEN 1 ELSE 0 END) FROM table_7")
                .expect("plan");
        match &plan.projection[0] {
            ProjectionExpr::Aggregate(call) => {
                assert_eq!(call.function, AggFunction::Sum);
                assert!(call.filter.is_none());
                assert!(
                    matches!(&call.arg, Some(AggArg::Expr(ScalarExpr::Case(_)))),
                    "CASE 应解析为标量表达式参数，实际 {:?}",
                    call.arg
                );
            }
            other => panic!("expected aggregate, got {other:?}"),
        }
    }

    /// 返工收尾：FILTER 内纯 ts 谓词显式报错（不静默丢弃）。
    #[test]
    fn test_plan_filter_ts_rejected() {
        assert!(
            QueryPlanner::plan("SELECT count(*) FILTER (WHERE ts > 100) FROM table_7").is_err(),
            "FILTER 内纯 ts 谓词应报错"
        );
    }

    /// 返工收尾：FILTER 内混 ts+列 谓词整体显式报错（不静默丢弃 ts 部分）。
    #[test]
    fn test_plan_filter_mixed_ts_rejected() {
        assert!(
            QueryPlanner::plan(
                "SELECT count(*) FILTER (WHERE ts >= 100 AND col_0 = 0) FROM table_7"
            )
            .is_err(),
            "FILTER 内混 ts+列 谓词应整体报错"
        );
    }

    /// P6：`lead(col)` 基础解析（默认 offset=1，无 default，无 partition/order）。
    #[test]
    fn test_plan_window_lead_basic() {
        let plan = QueryPlanner::plan("SELECT lead(col_0) OVER () FROM table_7").expect("plan");
        match &plan.projection[0] {
            ProjectionExpr::Window(spec) => {
                assert_eq!(spec.func, WindowFunction::Lead);
                assert_eq!(spec.arg, WindowArg::Column(ColumnRef::new(7, 0)));
                assert_eq!(spec.offset, 1);
                assert!(spec.default.is_none());
                assert!(spec.partition_by.is_empty());
                assert!(spec.order_by.is_empty());
            }
            other => panic!("expected window, got {other:?}"),
        }
    }

    /// P6：`lag(col, n, default)` + `PARTITION BY` + `ORDER BY` 解析。
    #[test]
    fn test_plan_window_lag_full() {
        let plan = QueryPlanner::plan(
            "SELECT lag(col_1, 3, 0) OVER (PARTITION BY col_0 ORDER BY ts DESC) FROM table_7",
        )
        .expect("plan");
        match &plan.projection[0] {
            ProjectionExpr::Window(spec) => {
                assert_eq!(spec.func, WindowFunction::Lag);
                assert_eq!(spec.arg, WindowArg::Column(ColumnRef::new(7, 1)));
                assert_eq!(spec.offset, 3);
                assert_eq!(spec.default, Some(TypedValue::I64(0)));
                assert_eq!(spec.partition_by, vec![ColumnRef::new(7, 0)]);
                assert_eq!(
                    spec.order_by,
                    vec![OrderKey {
                        column: OrderKeyColumn::Ts,
                        asc: false
                    }]
                );
            }
            other => panic!("expected window, got {other:?}"),
        }
    }

    /// P6：offset 为 0 或负数显式报错。
    #[test]
    fn test_plan_window_offset_nonpositive_rejected() {
        assert!(QueryPlanner::plan("SELECT lead(col_0, 0) OVER () FROM table_7").is_err());
        assert!(QueryPlanner::plan("SELECT lag(col_0, -1) OVER () FROM table_7").is_err());
    }

    /// P6：未知窗口函数显式报错。
    #[test]
    fn test_plan_window_unknown_function_rejected() {
        assert!(QueryPlanner::plan("SELECT rank() OVER () FROM table_7").is_err());
    }

    /// P6：命名窗口（WINDOW w AS (...)）暂不支持显式报错。
    #[test]
    fn test_plan_window_named_rejected() {
        assert!(
            QueryPlanner::plan("SELECT lead(col_0) OVER w FROM table_7 WINDOW w AS ()").is_err(),
            "命名窗口应显式报错"
        );
    }

    /// P6：显式 window frame（ROWS/RANGE ...）暂不支持显式报错（不静默忽略）。
    #[test]
    fn test_plan_window_frame_rejected() {
        assert!(
            QueryPlanner::plan(
                "SELECT lead(col_0) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM table_7"
            )
            .is_err(),
            "显式 window frame 应显式报错"
        );
    }

    /// 缺口 1：窗口参数为聚合（`lead(avg(col_0))`）解析为 `WindowArg::Aggregate`。
    #[test]
    fn test_plan_window_aggregate_arg() {
        let plan = QueryPlanner::plan(
            "SELECT avg(col_0), lead(avg(col_0)) OVER (ORDER BY ts) FROM table_7 SAMPLE BY 10m",
        )
        .expect("plan");
        match &plan.projection[1] {
            ProjectionExpr::Window(spec) => {
                assert_eq!(spec.func, WindowFunction::Lead);
                match &spec.arg {
                    WindowArg::Aggregate(call) => {
                        assert_eq!(call.function, AggFunction::Avg);
                    }
                    other => panic!("expected aggregate arg, got {other:?}"),
                }
            }
            other => panic!("expected window, got {other:?}"),
        }
    }
}

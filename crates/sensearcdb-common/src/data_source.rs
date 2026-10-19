//! AI 数据源桥接 trait。
//!
//! 定义 AI crate（forecast / condition / anomaly / ai-ops）读取时序历史、
//! 实时快照与枚举 tag 的最小抽象，避免 AI crate 反向依赖
//! `engine` / `snapshot` / `schema` crate，破坏分层边界。
//!
//! 由集成层（`src/startup.rs`）用 adapter 实现并在**构造时**注入，
//! 与既有 `SnapshotUpdateSink`（`engine/engine.rs`）、derived 的
//! `SnapshotRead/WriteSink/SchemaAccess`、`ReplaySink` 完全同范式。

use async_trait::async_trait;

use crate::types::ColumnRef;
use crate::SenseArcError;

/// 历史序列读取器——三源合并查询（WAL / MemTable / SAC）。
///
/// 返回 `[ts_start, ts_end]`（纳秒）区间内的 value 序列，按 ts 升序排列。
#[async_trait]
pub trait HistoryReader: Send + Sync {
    /// 查询指定 tag 在时间窗内的历史值序列（仅 value 列 f64）。
    async fn query_values(
        &self,
        table_id: u32,
        ts_start: i64,
        ts_end: i64,
    ) -> Result<Vec<f64>, SenseArcError>;

    /// 查询 `[ts_start, ts_end]` 内的 `(ts_ns, value)` 序列，按 ts 升序。
    ///
    /// 默认返回空序列：未覆写的 impl 继承默认实现，
    /// forecast / condition / anomaly 现有 impl 零破坏。
    async fn query_values_with_ts(
        &self,
        table_id: u32,
        ts_start: i64,
        ts_end: i64,
    ) -> Result<Vec<(i64, f64)>, SenseArcError> {
        let _ = (table_id, ts_start, ts_end);
        Ok(Vec::new())
    }

    /// 查询指定列（tag）在时间窗内的历史值序列（列级）。
    ///
    /// 默认实现退回旧单列接口 [`query_values`](Self::query_values)
    /// （即 `column_id == 0` 的 N=1 便捷版）；S2~S5 各实现逐步覆写为
    /// 真正的列级读取，现有 impl 零破坏。
    async fn query_column_values(
        &self,
        col: ColumnRef,
        ts_start: i64,
        ts_end: i64,
    ) -> Result<Vec<f64>, SenseArcError> {
        self.query_values(col.table_id, ts_start, ts_end).await
    }
}

/// 实时快照读取器——单点最新值（单一真相）。
pub trait SnapshotReader: Send + Sync {
    /// 返回指定 table_id 的最新值；无数据时为 `None`。
    fn latest(&self, table_id: u32) -> Option<f64>;

    /// 返回指定列的最新值（列级）。
    ///
    /// 默认实现退回旧单列接口 [`latest`](Self::latest)
    /// （即 `column_id == 0` 的 N=1 便捷版）；现有 impl 零破坏。
    fn latest_column(&self, col: ColumnRef) -> Option<f64> {
        self.latest(col.table_id)
    }
}

/// Tag 枚举器——遍历全部 tag（table_id）。
pub trait TagEnumerator: Send + Sync {
    /// 返回全部 table_id 列表。
    fn table_ids(&self) -> Vec<u32>;

    /// 返回 `table_id` 对应的表名（用于日志 / 理由展示）。默认 `None`。
    fn table_name(&self, table_id: u32) -> Option<String> {
        let _ = table_id;
        None
    }

    /// 返回 `table_id` 所属的设备分组（`TableInfo.table_group`，即 device_id）。
    ///
    /// 默认返回 `None`：未覆写的 impl（含既有实现与测试 mock）零破坏，
    /// 不强制各实现提供 device 维度。
    fn device_group(&self, table_id: u32) -> Option<String> {
        let _ = table_id;
        None
    }

    /// 返回全部测点（列）列表。
    ///
    /// 默认实现返回空（零破坏），S2~S5 逐步覆写为真正的列级枚举。
    fn columns(&self) -> Vec<ColumnRef> {
        Vec::new()
    }

    /// 返回某列所属表名。默认退回 [`table_name`](Self::table_name)
    /// （单列 N=1 便捷版）。
    fn column_name(&self, col: ColumnRef) -> Option<String> {
        self.table_name(col.table_id)
    }
}

// ─── 异常 / 工况数据源桥接（ai-ops 根因诊断前置，见 phase7-b8 D4）───────────

/// 一条异常事件记录（由 anomaly 引擎产出，供 ai-ops 根因诊断消费）。
///
/// 与 `sensearcdb-anomaly::AnomalyEvent` 字段一一对应，但定义在 common 以
/// 保持「AI crate 数据源桥接 trait」分层约定：ai-ops 不反向依赖 anomaly crate。
#[derive(Debug, Clone)]
pub struct AnomalyRecord {
    /// 异常 ID。
    pub anomaly_id: String,
    /// Tag 标识——表 ID（`tag = (table_id, column_id)` 的表分量）。
    pub tag_id: u32,
    /// Tag 标识——列 ID（N=1 单列表时为 0）。
    pub column_id: u16,
    /// 设备 ID（异常触发时经 `TagEnumerator::device_group` 回填，反查不到为空串）。
    pub device_id: String,
    /// 融合异常分数。
    pub score: f64,
    /// 严重程度（"Info" | "Warning" | "Critical"）。
    pub severity: String,
    /// 检出时间（epoch 毫秒）。
    pub detected_at: i64,
    /// 描述。
    pub description: String,
}

/// 一条工况快照（由 condition 引擎产出，供 ai-ops 根因诊断富化）。
#[derive(Debug, Clone)]
pub struct ConditionSnapshot {
    /// 设备 ID。
    pub device_id: String,
    /// 工况 ID。
    pub condition_id: u32,
    /// 置信度。
    pub confidence: f64,
    /// 快照时间戳（epoch 毫秒）。
    pub ts: i64,
}

/// 异常记录读取器——供 ai-ops 根因诊断按设备/全局读取异常事件。
pub trait AnomalyReader: Send + Sync {
    /// 返回最近触发的异常记录（时间倒序）。
    fn recent_anomalies(&self) -> Vec<AnomalyRecord>;

    /// 返回匹配指定 `device_id` 的异常记录（时间倒序）。
    fn anomalies_for_device(&self, device_id: &str) -> Vec<AnomalyRecord>;
}

/// 工况读取器——供 ai-ops 根因诊断读取设备当前工况（可选富化）。
pub trait ConditionReader: Send + Sync {
    /// 返回指定 `device_id` 的当前工况快照；无数据时为 `None`。
    fn current_condition(&self, device_id: &str) -> Option<ConditionSnapshot>;
}

/// 退化严重度读取器——供 asset 健康度聚合读取测点退化趋势。
///
/// 为避免 common 依赖 anomaly 的 `DegradationSeverity` 枚举，方法用字符串返回
/// （"Normal" | "Accelerated" | "Critical"）；无数据/不可用时为 `None`（诚实降级）。
#[async_trait]
pub trait DegradationReader: Send + Sync {
    /// 返回指定测点（列）的退化严重度；无数据/不可用时为 `None`。
    async fn degradation_severity(&self, table_id: u32, column_id: u16) -> Option<String>;
}

// ─── 拓扑数据源桥接（ai-ops 根因拓扑化，见 phase-1 任务⑤ / M3）────────────

/// 两测点之间的 ISA-95 物理拓扑关系。
///
/// 根因诊断用它做两件事：**剪枝**（剔除「物理不相连且非同一子树」的候选）
/// 与**加权**（上下游升权、未知降权）。`Unknown` 表示「无法确定关系」，
/// 既可能是 asset 未启用，也可能是两测点分属不同子树。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopologyRelation {
    /// 候选是告警测点的**上游**（祖先侧，物理上驱动告警测点）。
    Upstream,
    /// 候选是告警测点的**下游**（子孙侧，被告警测点驱动）。
    Downstream,
    /// 候选与告警测点同属一个父节点（同一子树、同级）。
    Sibling,
    /// 关系未知：asset 未启用，或两测点未绑定资产，或分属不同子树。
    Unknown,
}

/// 拓扑提供器——根因诊断的 ISA-95 物理拓扑先验来源。
///
/// 由集成层（`src/startup.rs`）用 `AssetManager` 实现并注入；ai-ops **只依赖
/// 此窄 trait，绝不反向依赖 asset crate**（避免循环依赖）。asset 未启用时注入
/// [`NoopTopologyProvider`]，行为退化为「无拓扑先验」。
pub trait TopologyProvider: Send + Sync {
    /// 是否提供真实拓扑。
    ///
    /// 返回 `false` 表示未启用 / 无拓扑数据（如 [`NoopTopologyProvider`]），
    /// 调用方应跳过**剪枝**（退化现状），避免「全 Unknown」被误剪为空。
    /// 默认为 `true`。
    fn enabled(&self) -> bool {
        true
    }

    /// 返回 `alert_col` 与 `candidate_col` 之间的拓扑关系。
    fn relation(&self, alert_col: ColumnRef, candidate_col: ColumnRef) -> TopologyRelation;
}

// ─── 空实现（测试 / AI 关闭场景）────────────────────────────────────────────

/// 空历史读取器：恒返回空序列。
#[derive(Debug, Default)]
pub struct NoopHistoryReader;

#[async_trait]
impl HistoryReader for NoopHistoryReader {
    async fn query_values(
        &self,
        _table_id: u32,
        _ts_start: i64,
        _ts_end: i64,
    ) -> Result<Vec<f64>, SenseArcError> {
        Ok(Vec::new())
    }
}

/// 空快照读取器：恒返回 `None`。
#[derive(Debug, Default)]
pub struct NoopSnapshotReader;

impl SnapshotReader for NoopSnapshotReader {
    fn latest(&self, _table_id: u32) -> Option<f64> {
        None
    }
}

/// 空 tag 枚举器：恒返回空列表。
#[derive(Debug, Default)]
pub struct NoopTagEnumerator;

impl TagEnumerator for NoopTagEnumerator {
    fn table_ids(&self) -> Vec<u32> {
        Vec::new()
    }
}

/// 空异常读取器：恒返回空列表（AI 关闭 / 测试场景）。
#[derive(Debug, Default)]
pub struct NoopAnomalyReader;

impl AnomalyReader for NoopAnomalyReader {
    fn recent_anomalies(&self) -> Vec<AnomalyRecord> {
        Vec::new()
    }

    fn anomalies_for_device(&self, _device_id: &str) -> Vec<AnomalyRecord> {
        Vec::new()
    }
}

/// 空退化读取器：恒返回 `None`（AI 关闭 / 测试场景）。
#[derive(Debug, Default)]
pub struct NoopDegradationReader;

#[async_trait]
impl DegradationReader for NoopDegradationReader {
    async fn degradation_severity(&self, _table_id: u32, _column_id: u16) -> Option<String> {
        None
    }
}

/// 空工况读取器：恒返回 `None`（AI 关闭 / 测试场景）。
#[derive(Debug, Default)]
pub struct NoopConditionReader;

impl ConditionReader for NoopConditionReader {
    fn current_condition(&self, _device_id: &str) -> Option<ConditionSnapshot> {
        None
    }
}

/// 空拓扑提供器：不启用（`enabled() == false`），关系恒为 [`TopologyRelation::Unknown`]。
///
/// 用于 asset 未启用的场景，根因诊断据此退化为「无拓扑先验」（不剪枝、不加权）。
#[derive(Debug, Default)]
pub struct NoopTopologyProvider;

impl TopologyProvider for NoopTopologyProvider {
    fn enabled(&self) -> bool {
        false
    }

    fn relation(&self, _alert_col: ColumnRef, _candidate_col: ColumnRef) -> TopologyRelation {
        TopologyRelation::Unknown
    }
}

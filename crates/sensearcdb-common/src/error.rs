//! SenseArcDB 统一错误类型。
//!
//! 错误码规范：`<HTTP状态码><2位序号>` → 5 位数字。
//!
//! | 域       | 前缀  | 范围             |
//! |----------|-------|------------------|
//! | Schema   | 4xxxx | 40001-42201      |
//! | Engine   | 5xxxx | 50010-50040      |
//! | Snapshot | 4xxxx | 40401, 42901      |
//! | Security | 4xxxx | 40100-40199, 40300-40399（复用 40001/40401 语义码值） |
//! | RateLimit| 4xxxx | 42900-42999       |
//! | Partition| 4xxxx | 40410-42210      |
//! | Ingest   | 5xxxx | 50050-50053, 50301 |
//! | Lifecycle| 5xxxx | 50100-50103      |
//! | Cluster  | 8xxxx | 80001-80099      |
//! | Shard    | 8xxxx | 80101-80199      |
//! | Edge     | 8xxxx | 80201-80299      |
//! | Event    | 8xxxx | 80301-80399      |
//! | Alert    | 8xxxx | 80401-80499      |
//! | Forecast | 8xxxx | 81001-81099      |
//! | Condition| 8xxxx | 82001-82099      |
//! | Anomaly  | 8xxxx | 83001-83099      |
//! | AI Ops   | 8xxxx | 84001-84099      |
//! | Asset    | 8xxxx | 85001-85999      |
//! | Observe  | 9xxxx | 91001-91099      |
//! | Deploy   | 9xxxx | 92001-92099      |
//! | CLI      | 9xxxx | 93001-93099      |
//!
//! 注：`42901`（TooManyTags，Snapshot 域）已占用，RateLimit 段从 `42900` 起，不重号。
//!
//! REST 响应格式：`{ "code": 40401, "message": "...", "data": null }`

use crate::types::DataType;

/// SenseArcDB 统一错误枚举。
///
/// 所有公共 API 均通过此枚举传播错误。各模块内部的错误类型
/// 可通过 `From` trait 转换为此统一错误。
#[derive(Debug, thiserror::Error)]
pub enum SenseArcError {
    // ─── Schema 错误 (4xxxx) ───
    /// 表不存在。错误码: 40401
    #[error("table not found: {0}")]
    TableNotFound(u32),

    /// 列不存在。错误码: 40402
    #[error("column not found: {0}")]
    ColumnNotFound(u16),

    /// Schema 版本不存在。错误码: 40403
    #[error("schema version not found: {0}")]
    SchemaVersionNotFound(u32),

    /// 表已存在。错误码: 40901
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    /// 列已存在。错误码: 40902
    #[error("column already exists: {0}")]
    ColumnAlreadyExists(String),

    /// 表已删除。错误码: 41001
    #[error("table deleted: {0}")]
    TableDeleted(u32),

    /// 列已弃用。错误码: 41002
    #[error("column deprecated: {0}")]
    ColumnDeprecated(u16),

    /// 租户不存在。错误码: 40404
    #[error("tenant not found: {0}")]
    TenantNotFound(u32),

    /// 租户已存在（slug 冲突）。错误码: 40903
    #[error("tenant already exists: {0}")]
    TenantAlreadyExists(String),

    /// 非法租户状态转换。错误码: 42202
    #[error("invalid tenant state transition: {from} → {to}")]
    InvalidTenantState { from: String, to: String },

    /// 列类型不匹配。错误码: 42201
    #[error("column type mismatch: expected {expected:?}, got {got:?}")]
    ColumnTypeMismatch { expected: DataType, got: DataType },

    /// 非法列名。错误码: 40001
    #[error("invalid column name: {0}")]
    InvalidColumnName(String),

    /// 非法表名。错误码: 40002
    #[error("invalid table name: {0}")]
    InvalidTableName(String),

    /// 非法范围/区间。错误码: 40003
    #[error("invalid range: {0}")]
    InvalidRange(String),

    /// 非法数据类型。错误码: 40001（复用 40001，语义=HTTP 400 bad request）
    #[error("invalid data type: {0}")]
    InvalidDataType(String),

    /// 层级聚合路径不支持该聚合函数。错误码: 40004
    ///
    /// CQ 层级传递（CQ_1m/1h/1d 从下层 CQ 表聚合）仅支持 AVG/SUM/COUNT；
    /// MIN/MAX/FIRST/LAST/STDDEV 需要下层持久化额外辅助列，当前尚未支持。
    #[error("hierarchical aggregation unsupported: {0}")]
    UnsupportedAggregation(String),

    // ─── 引擎错误 (5xxxx) ───
    /// SAC I/O 错误。错误码: 50010
    #[error("SAC I/O error: {0}")]
    SacIoError(String),

    /// SAC CRC 校验失败。错误码: 50011
    #[error("SAC CRC mismatch")]
    SacCrcMismatch,

    /// WAL I/O 错误。错误码: 50020
    #[error("WAL I/O error: {0}")]
    WalIoError(String),

    /// WAL 段损坏。错误码: 50021
    #[error("WAL segment corrupted")]
    WalCorrupted,

    /// MemTable 已满。错误码: 50030
    #[error("MemTable full")]
    MemTableFull,

    /// Compaction 错误。错误码: 50040
    #[error("compaction error: {0}")]
    CompactionError(String),

    /// TDE 加密/解密错误。错误码: 50041
    #[error("TDE error: {0}")]
    TdeError(String),

    // ─── Snapshot 错误 ───
    /// 快照中 tag 不存在。错误码: 40401
    #[error("tag not found in snapshot: {0}")]
    SnapshotTagNotFound(u32),

    /// 单次查询 tag 数超限。错误码: 42901
    #[error("too many tags: {0} > {1}")]
    TooManyTags(usize, usize),

    // ─── 安全错误 (4xxxx: 40100-40199, 40300-40399) ───
    //
    // 401xx 段为「未授权/认证失败」专属段（HTTP 401 语义自洽：http_status()=code/100=401）；
    // 403xx 段为「无权限/禁止访问」专属段（HTTP 403 语义自洽）。
    // 阶段 4 一次性豁免「禁止另起新码段」红线后补登记；后续 auth 错误仅能在
    // 本段内追加序号，不得另起任何新的 3 位前缀段。
    /// 未授权/认证失败。错误码: 40100
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// 无权限/禁止访问。错误码: 40300
    #[error("forbidden: {0}")]
    Forbidden(String),

    // 以下 4 个变体复用既有码值，不另起新 3 位前缀段：
    // RoleNotFound/ApiKeyNotFound/UserNotFound 是「资源查无」语义，复用 40401（HTTP 404），
    // 非 auth 失败（auth 失败仍归 401xx/403xx）；InvalidSecurityRequest 是「非法请求」语义，
    // 复用 40001（HTTP 400）。精确语义由变体名 + message 承载。
    /// 角色不存在。错误码: 40401
    #[error("role not found: {0}")]
    RoleNotFound(String),

    /// API Key 不存在。错误码: 40401
    #[error("api key not found: {0}")]
    ApiKeyNotFound(String),

    /// 用户不存在。错误码: 40401
    #[error("user not found: {0}")]
    UserNotFound(String),

    /// 非法安全请求（参数语义错误）。错误码: 40001
    #[error("invalid security request: {0}")]
    InvalidSecurityRequest(String),

    // ─── 限流错误 (4xxxx: 42900-42999) ───
    //
    // 429xx 段为「请求限流」专属段。注意 42901（TooManyTags，Snapshot 域）已占用，
    // 本段从 42900 起，不重号。
    /// 请求被限流。错误码: 42900
    #[error("rate limited")]
    RateLimited,

    /// 租户配额超限（测点数/存储/写入速率硬限制）。错误码: 42902
    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),

    // ─── 分区错误 (4xxxx) ───
    /// 分区不存在。错误码: 40410
    #[error("partition not found: {0}")]
    PartitionNotFound(u64),

    /// 分区已存在。错误码: 40910
    #[error("partition already exists: {0}")]
    PartitionAlreadyExists(u64),

    /// 非法分区状态转换。错误码: 42210
    #[error("invalid partition state transition: {from} → {to}")]
    InvalidPartitionState { from: String, to: String },

    /// 分区路由失败（迟到数据）。错误码: 42211
    #[error("write rejected: partition {0} is not active")]
    WriteRejectedPartition(u64),

    // ─── 接入层错误 (5xxxx) ───
    /// Collector 错误。错误码: 50050
    #[error("collector error: {0}")]
    CollectorError(String),

    /// 驱动错误。错误码: 50051
    #[error("driver error: {0}")]
    DriverError(String),

    /// 写入被拒绝（非 Active 分区）。错误码: 50052
    #[error("write rejected: {0}")]
    WriteRejected(String),

    /// 断线缓存 (Store & Forward) 错误。错误码: 50053
    #[error("store-and-forward error: {0}")]
    StoreForwardError(String),

    /// 服务未就绪（readiness 探测失败）。错误码: 50301
    #[error("service not ready: {0}")]
    ServiceNotReady(String),

    // ─── 生命周期错误 (5xxxx) ───
    /// TTL 过期处理错误。错误码: 50100
    #[error("TTL error: {0}")]
    TtlError(String),

    /// 备份失败。错误码: 50101
    #[error("backup failed: {0}")]
    BackupFailed(String),

    /// 恢复失败。错误码: 50102
    #[error("restore failed: {0}")]
    RestoreFailed(String),

    /// 分层存储错误。错误码: 50103
    #[error("tiering error: {0}")]
    TieringError(String),

    // ─── Wave 4: 集群错误 (8xxxx: 80001-80099) ───
    /// 节点不可达。错误码: 80001
    #[error("node unreachable: {0}")]
    NodeUnreachable(String),

    /// 非 Leader 节点拒绝了写请求。错误码: 80002
    #[error("not leader: leader is {0}")]
    NotLeader(String),

    /// Raft 日志不一致。错误码: 80003
    #[error("raft log inconsistency: {0}")]
    RaftLogInconsistency(String),

    /// Raft 配置变更失败。错误码: 80004
    #[error("raft config change failed: {0}")]
    RaftConfigChangeFailed(String),

    /// 集群未就绪。错误码: 80005
    #[error("cluster not ready")]
    ClusterNotReady,

    // ─── Wave 4: 分片错误 (8xxxx: 80101-80199) ───
    /// 分片不存在。错误码: 80101
    #[error("shard not found: {0}")]
    ShardNotFound(u64),

    /// 分片路由失败。错误码: 80102
    #[error("shard routing failed: {0}")]
    ShardRoutingFailed(String),

    /// 分片迁移冲突。错误码: 80103
    #[error("shard migration conflict: {0}")]
    ShardMigrationConflict(String),

    /// 分片副本不足。错误码: 80104
    #[error("insufficient shard replicas: {0}")]
    InsufficientShardReplicas(String),

    /// 分片迁移进行中（slot 被冻结，写被拒绝，上层应重试）。错误码: 80105
    #[error("shard migration in progress: {0}")]
    ShardMigrationInProgress(String),

    /// 并发迁移数超限（非终态迁移数已达 `rebalance_max_concurrent`）。错误码: 80106
    #[error("shard migration limit exceeded: {0}")]
    ShardMigrationLimitExceeded(String),

    // ─── Wave 4: 边缘错误 (8xxxx: 80201-80299) ───
    /// 边缘节点离线。错误码: 80201
    #[error("edge node offline: {0}")]
    EdgeNodeOffline(String),

    /// 边缘同步冲突。错误码: 80202
    #[error("edge sync conflict: {0}")]
    EdgeSyncConflict(String),

    /// 边缘缓冲区溢出。错误码: 80203
    #[error("edge buffer overflow: {0}")]
    EdgeBufferOverflow(String),

    /// 边缘带宽超限。错误码: 80204
    #[error("edge bandwidth exceeded: {0}")]
    EdgeBandwidthExceeded(String),

    // ─── Wave 4: 事件错误 (8xxxx: 80301-80399) ───
    /// 事件处理器超时。错误码: 80301
    #[error("event handler timeout: {0}")]
    EventHandlerTimeout(String),

    /// 事件队列满。错误码: 80302
    #[error("event queue full")]
    EventQueueFull,

    /// 事件投递失败。错误码: 80303
    #[error("event delivery failed: {0}")]
    EventDeliveryFailed(String),

    /// 死信队列满。错误码: 80304
    #[error("dead letter queue full")]
    DeadLetterQueueFull,

    // ─── Wave 4: 告警资源查无（复用 40401 = HTTP 404，与 RoleNotFound 同码值）───
    /// 告警规则不存在。错误码: 40401
    #[error("alert rule not found: {0}")]
    AlertRuleNotFound(String),

    /// 静默规则不存在。错误码: 40401
    #[error("silence rule not found: {0}")]
    SilenceRuleNotFound(String),

    /// AI 推荐不存在。错误码: 40401
    #[error("recommendation not found: {0}")]
    RecommendationNotFound(String),

    /// 告警规则输入参数非法（前端 DTO 校验失败，复用 40001 = HTTP 400）。
    ///
    /// 与 [`SenseArcError::InvalidAlertRule`]（80401，引擎内部规则非法）区分：
    /// 本变体用于**用户输入**校验失败（如 condition=eq/ne 无对应 limit_type 语义）。
    #[error("invalid alert rule input: {0}")]
    InvalidAlertRuleInput(String),

    // ─── Wave 4: 告警错误 (8xxxx: 80401-80499) ───
    /// 告警规则无效。错误码: 80401
    #[error("invalid alert rule: {0}")]
    InvalidAlertRule(String),

    /// 告警通知失败。错误码: 80402
    #[error("alert notification failed: {0}")]
    AlertNotificationFailed(String),

    /// 告警规则数超限。错误码: 80403
    #[error("too many alert rules: {0}")]
    TooManyAlertRules(usize),

    /// 告警评估超时。错误码: 80404
    #[error("alert evaluation timeout: {0}")]
    AlertEvaluationTimeout(String),

    // ─── Wave 5: 预测错误 (8xxxx: 81001-81099) ───
    /// 模型加载失败。错误码: 81001
    #[error("model load failed: {0}")]
    ModelLoadFailed(String),

    /// 推理执行失败。错误码: 81002
    #[error("inference failed: {0}")]
    InferenceFailed(String),

    /// 输入数据维度错误。错误码: 81003
    #[error("input dimension mismatch: expected {expected:?}, got {got:?}")]
    InputDimensionMismatch {
        expected: Vec<usize>,
        got: Vec<usize>,
    },

    /// 预测超出范围。错误码: 81004
    #[error("prediction out of range: {0}")]
    PredictionOutOfRange(String),

    /// ONNX 会话耗尽。错误码: 81005
    #[error("no available ONNX session")]
    NoAvailableSession,

    // ─── Wave 5: 条件评估错误 (8xxxx: 82001-82099) ───
    /// 条件表达式非法。错误码: 82001
    #[error("invalid condition expression: {0}")]
    InvalidConditionExpression(String),

    /// 条件评估超时。错误码: 82002
    #[error("condition evaluation timeout: {0}")]
    ConditionEvaluationTimeout(String),

    /// 条件数超限。错误码: 82003
    #[error("too many conditions: {0}")]
    TooManyConditions(usize),

    /// 条件循环依赖。错误码: 82004
    #[error("condition circular dependency detected")]
    ConditionCircularDependency,

    // ─── Wave 5: 异常检测错误 (8xxxx: 83001-83099) ───
    /// 异常记录不存在（按 anomalyId 查询诊断未命中）。
    ///
    /// 复用 40401 = HTTP 404（「资源查无」语义，对齐 `RoleNotFound` /
    /// `AlertRuleNotFound`，见 phase7-b8 D6）。
    #[error("anomaly not found: {0}")]
    AnomalyNotFound(String),

    /// 异常模型未初始化。错误码: 83001
    #[error("anomaly model not initialized")]
    AnomalyModelNotInitialized,

    /// 异常检测数据不足。错误码: 83002
    #[error("insufficient data for anomaly detection: need {need}, got {got}")]
    AnomalyInsufficientData { need: usize, got: usize },

    /// 异常分数阈值无效。错误码: 83003
    #[error("invalid anomaly score threshold: {0}")]
    AnomalyInvalidThreshold(f64),

    /// 异常特征提取失败。错误码: 83004
    #[error("anomaly feature extraction failed: {0}")]
    AnomalyFeatureExtractionFailed(String),

    // ─── Wave 5: AI 运维错误 (8xxxx: 84001-84099) ───
    /// 根因分析失败。错误码: 84001
    #[error("root cause analysis failed: {0}")]
    RootCauseAnalysisFailed(String),

    /// 相关性计算失败。错误码: 84002
    #[error("correlation computation failed: {0}")]
    CorrelationComputationFailed(String),

    /// 基线数据不足。错误码: 84003
    #[error("insufficient baseline data: {0}")]
    InsufficientBaselineData(String),

    /// 多模型投票不一致。错误码: 84004
    #[error("multi-model voting inconclusive: votes {votes} < threshold {threshold}")]
    MultiModelVoteInconclusive { votes: usize, threshold: usize },

    /// AI 运维引擎未就绪。错误码: 84005
    #[error("AI ops engine not ready")]
    AiOpsNotReady,

    // ─── Wave 5: 资产模型错误 (8xxxx: 85001-85999) ───
    /// 资产不存在。错误码: 85001
    #[error("asset not found: {0}")]
    AssetNotFound(String),

    /// 资产层级配置非法。错误码: 85002
    #[error("invalid asset hierarchy: {0}")]
    InvalidAssetHierarchy(String),

    /// 资产数超限。错误码: 85003
    #[error("too many assets: {0}")]
    TooManyAssets(usize),

    /// 状态传播失败。错误码: 85004
    #[error("asset state propagation failed: {0}")]
    AssetStatePropagationFailed(String),

    /// 健康评分计算失败。错误码: 85005
    #[error("health score computation failed: {0}")]
    HealthScoreComputationFailed(String),

    /// 资产模型未初始化。错误码: 85006
    #[error("asset model not initialized")]
    AssetModelNotInitialized,

    // ─── Wave 6: 可观测性错误 (9xxxx: 91001-91099) ───
    /// Prometheus 指标注册失败。错误码: 91001
    #[error("metrics registration failed: {0}")]
    MetricsRegistrationFailed(String),

    /// 慢查询日志写入失败。错误码: 91002
    #[error("slow query log write failed: {0}")]
    SlowQueryLogWriteFailed(String),

    // ─── Wave 6: 部署运维错误 (9xxxx: 92001-92099) ───
    /// FORMAT_VERSION 不兼容。错误码: 92001
    #[error("format version mismatch: current={0}, required={1}")]
    FormatVersionMismatch(u32, u32),

    /// 进程锁获取失败（已有实例运行）。错误码: 92002
    #[error("process lock failed: {0}")]
    ProcessLockFailed(String),

    // ─── Wave 6: CLI 错误 (9xxxx: 93001-93099) ───
    /// CLI 命令执行失败。错误码: 93001
    #[error("CLI command failed: {0}")]
    CliCommandFailed(String),

    // ─── 内部错误 ───
    /// 内部错误。错误码: 50001
    #[error("internal: {0}")]
    Internal(String),

    /// Catalog (SQLite config.db) 错误。错误码: 50002
    #[error("catalog error: {0}")]
    CatalogError(String),
}

impl SenseArcError {
    /// 返回该错误对应的 5 位数字错误码。
    pub fn code(&self) -> u32 {
        match self {
            // Schema 错误
            SenseArcError::TableNotFound(_) => 40401,
            SenseArcError::ColumnNotFound(_) => 40402,
            SenseArcError::SchemaVersionNotFound(_) => 40403,
            SenseArcError::TableAlreadyExists(_) => 40901,
            SenseArcError::ColumnAlreadyExists(_) => 40902,
            SenseArcError::TableDeleted(_) => 41001,
            SenseArcError::ColumnDeprecated(_) => 41002,
            SenseArcError::TenantNotFound(_) => 40404,
            SenseArcError::TenantAlreadyExists(_) => 40903,
            SenseArcError::InvalidTenantState { .. } => 42202,
            SenseArcError::ColumnTypeMismatch { .. } => 42201,
            SenseArcError::InvalidColumnName(_) => 40001,
            SenseArcError::InvalidTableName(_) => 40002,
            SenseArcError::InvalidRange(_) => 40003,
            SenseArcError::InvalidDataType(_) => 40001,
            SenseArcError::UnsupportedAggregation(_) => 40004,
            // 引擎错误
            SenseArcError::SacIoError(_) => 50010,
            SenseArcError::SacCrcMismatch => 50011,
            SenseArcError::WalIoError(_) => 50020,
            SenseArcError::WalCorrupted => 50021,
            SenseArcError::MemTableFull => 50030,
            SenseArcError::CompactionError(_) => 50040,
            SenseArcError::TdeError(_) => 50041,
            // Snapshot 错误
            SenseArcError::SnapshotTagNotFound(_) => 40401,
            SenseArcError::TooManyTags(_, _) => 42901,
            // 安全错误 (401xx/403xx + 复用 40401/40001)
            SenseArcError::Unauthorized(_) => 40100,
            SenseArcError::Forbidden(_) => 40300,
            SenseArcError::RoleNotFound(_) => 40401,
            SenseArcError::ApiKeyNotFound(_) => 40401,
            SenseArcError::UserNotFound(_) => 40401,
            SenseArcError::InvalidSecurityRequest(_) => 40001,
            // 限流错误 (429xx)
            SenseArcError::RateLimited => 42900,
            SenseArcError::QuotaExceeded(_) => 42902,
            // 分区错误
            SenseArcError::PartitionNotFound(_) => 40410,
            SenseArcError::PartitionAlreadyExists(_) => 40910,
            SenseArcError::InvalidPartitionState { .. } => 42210,
            SenseArcError::WriteRejectedPartition(_) => 42211,
            // 接入层错误
            SenseArcError::CollectorError(_) => 50050,
            SenseArcError::DriverError(_) => 50051,
            SenseArcError::WriteRejected(_) => 50052,
            SenseArcError::StoreForwardError(_) => 50053,
            SenseArcError::ServiceNotReady(_) => 50301,
            // 生命周期错误
            SenseArcError::TtlError(_) => 50100,
            SenseArcError::BackupFailed(_) => 50101,
            SenseArcError::RestoreFailed(_) => 50102,
            SenseArcError::TieringError(_) => 50103,
            // Wave 4: 集群错误
            SenseArcError::NodeUnreachable(_) => 80001,
            SenseArcError::NotLeader(_) => 80002,
            SenseArcError::RaftLogInconsistency(_) => 80003,
            SenseArcError::RaftConfigChangeFailed(_) => 80004,
            SenseArcError::ClusterNotReady => 80005,
            // Wave 4: 分片错误
            SenseArcError::ShardNotFound(_) => 80101,
            SenseArcError::ShardRoutingFailed(_) => 80102,
            SenseArcError::ShardMigrationConflict(_) => 80103,
            SenseArcError::InsufficientShardReplicas(_) => 80104,
            SenseArcError::ShardMigrationInProgress(_) => 80105,
            SenseArcError::ShardMigrationLimitExceeded(_) => 80106,
            // Wave 4: 边缘错误
            SenseArcError::EdgeNodeOffline(_) => 80201,
            SenseArcError::EdgeSyncConflict(_) => 80202,
            SenseArcError::EdgeBufferOverflow(_) => 80203,
            SenseArcError::EdgeBandwidthExceeded(_) => 80204,
            // Wave 4: 事件错误
            SenseArcError::EventHandlerTimeout(_) => 80301,
            SenseArcError::EventQueueFull => 80302,
            SenseArcError::EventDeliveryFailed(_) => 80303,
            SenseArcError::DeadLetterQueueFull => 80304,
            // Wave 4: 告警资源查无（复用 40401）
            SenseArcError::AlertRuleNotFound(_) => 40401,
            SenseArcError::SilenceRuleNotFound(_) => 40401,
            SenseArcError::InvalidAlertRuleInput(_) => 40001,
            // Wave 5: AI 推荐资源查无（复用 40401）
            SenseArcError::RecommendationNotFound(_) => 40401,
            // Wave 4: 告警错误
            SenseArcError::InvalidAlertRule(_) => 80401,
            SenseArcError::AlertNotificationFailed(_) => 80402,
            SenseArcError::TooManyAlertRules(_) => 80403,
            SenseArcError::AlertEvaluationTimeout(_) => 80404,
            // Wave 5: 预测错误
            SenseArcError::ModelLoadFailed(_) => 81001,
            SenseArcError::InferenceFailed(_) => 81002,
            SenseArcError::InputDimensionMismatch { .. } => 81003,
            SenseArcError::PredictionOutOfRange(_) => 81004,
            SenseArcError::NoAvailableSession => 81005,
            // Wave 5: 条件评估错误
            SenseArcError::InvalidConditionExpression(_) => 82001,
            SenseArcError::ConditionEvaluationTimeout(_) => 82002,
            SenseArcError::TooManyConditions(_) => 82003,
            SenseArcError::ConditionCircularDependency => 82004,
            // Wave 5: 异常检测错误
            SenseArcError::AnomalyNotFound(_) => 40401,
            SenseArcError::AnomalyModelNotInitialized => 83001,
            SenseArcError::AnomalyInsufficientData { .. } => 83002,
            SenseArcError::AnomalyInvalidThreshold(_) => 83003,
            SenseArcError::AnomalyFeatureExtractionFailed(_) => 83004,
            // Wave 5: AI 运维错误
            SenseArcError::RootCauseAnalysisFailed(_) => 84001,
            SenseArcError::CorrelationComputationFailed(_) => 84002,
            SenseArcError::InsufficientBaselineData(_) => 84003,
            SenseArcError::MultiModelVoteInconclusive { .. } => 84004,
            SenseArcError::AiOpsNotReady => 84005,
            // Wave 5: 资产模型错误
            SenseArcError::AssetNotFound(_) => 85001,
            SenseArcError::InvalidAssetHierarchy(_) => 85002,
            SenseArcError::TooManyAssets(_) => 85003,
            SenseArcError::AssetStatePropagationFailed(_) => 85004,
            SenseArcError::HealthScoreComputationFailed(_) => 85005,
            SenseArcError::AssetModelNotInitialized => 85006,
            // Wave 6: 可观测性错误
            SenseArcError::MetricsRegistrationFailed(_) => 91001,
            SenseArcError::SlowQueryLogWriteFailed(_) => 91002,
            // Wave 6: 部署错误
            SenseArcError::FormatVersionMismatch(_, _) => 92001,
            SenseArcError::ProcessLockFailed(_) => 92002,
            // Wave 6: CLI 错误
            SenseArcError::CliCommandFailed(_) => 93001,
            // 内部错误
            SenseArcError::Internal(_) => 50001,
            SenseArcError::CatalogError(_) => 50002,
        }
    }

    /// 返回对应的 HTTP 状态码。
    pub fn http_status(&self) -> u16 {
        let code = self.code();
        (code / 100) as u16
    }
}

// ─── 标准转换实现 ───

impl From<std::io::Error> for SenseArcError {
    fn from(e: std::io::Error) -> Self {
        SenseArcError::Internal(format!("I/O error: {}", e))
    }
}

impl From<String> for SenseArcError {
    fn from(s: String) -> Self {
        SenseArcError::Internal(s)
    }
}

impl From<&str> for SenseArcError {
    fn from(s: &str) -> Self {
        SenseArcError::Internal(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(SenseArcError::TableNotFound(1).code(), 40401);
        assert_eq!(SenseArcError::SacIoError("test".into()).code(), 50010);
        assert_eq!(SenseArcError::SacCrcMismatch.code(), 50011);
        assert_eq!(SenseArcError::WalIoError("test".into()).code(), 50020);
        assert_eq!(SenseArcError::WalCorrupted.code(), 50021);
        assert_eq!(SenseArcError::MemTableFull.code(), 50030);
        assert_eq!(SenseArcError::SnapshotTagNotFound(1).code(), 40401);
        assert_eq!(SenseArcError::TooManyTags(300_000, 200_000).code(), 42901);
        assert_eq!(SenseArcError::Internal("test".into()).code(), 50001);
        assert_eq!(SenseArcError::InvalidRange("x".into()).code(), 40003);
        assert_eq!(SenseArcError::SchemaVersionNotFound(2).code(), 40403);
        assert_eq!(SenseArcError::ColumnAlreadyExists("c".into()).code(), 40902);
        assert_eq!(SenseArcError::CatalogError("e".into()).code(), 50002);
        assert_eq!(SenseArcError::Unauthorized("x".into()).code(), 40100);
        assert_eq!(SenseArcError::Forbidden("x".into()).code(), 40300);
        assert_eq!(SenseArcError::RateLimited.code(), 42900);
        assert_eq!(SenseArcError::RoleNotFound("r".into()).code(), 40401);
        assert_eq!(SenseArcError::ApiKeyNotFound("k".into()).code(), 40401);
        assert_eq!(SenseArcError::UserNotFound("u".into()).code(), 40401);
        assert_eq!(
            SenseArcError::InvalidSecurityRequest("x".into()).code(),
            40001
        );
        assert_eq!(SenseArcError::InvalidDataType("x".into()).code(), 40001);
        assert_eq!(
            SenseArcError::UnsupportedAggregation("Min".into()).code(),
            40004
        );
        assert_eq!(SenseArcError::ServiceNotReady("x".into()).code(), 50301);
        assert_eq!(
            SenseArcError::ShardMigrationLimitExceeded("x".into()).code(),
            80106
        );
        assert_eq!(
            SenseArcError::AnomalyNotFound("anom-1-1".into()).code(),
            40401
        );
    }

    #[test]
    fn test_http_status() {
        assert_eq!(SenseArcError::TableNotFound(1).http_status(), 404);
        assert_eq!(SenseArcError::SacIoError("test".into()).http_status(), 500);
        assert_eq!(
            SenseArcError::TableAlreadyExists("t".into()).http_status(),
            409
        );
        assert_eq!(SenseArcError::InvalidRange("x".into()).http_status(), 400);
        assert_eq!(SenseArcError::SchemaVersionNotFound(2).http_status(), 404);
        assert_eq!(SenseArcError::CatalogError("e".into()).http_status(), 500);
        assert_eq!(SenseArcError::Unauthorized("x".into()).http_status(), 401);
        assert_eq!(SenseArcError::Forbidden("x".into()).http_status(), 403);
        assert_eq!(SenseArcError::RateLimited.http_status(), 429);
        assert_eq!(SenseArcError::RoleNotFound("r".into()).http_status(), 404);
        assert_eq!(SenseArcError::ApiKeyNotFound("k".into()).http_status(), 404);
        assert_eq!(SenseArcError::UserNotFound("u".into()).http_status(), 404);
        assert_eq!(
            SenseArcError::InvalidSecurityRequest("x".into()).http_status(),
            400
        );
        assert_eq!(
            SenseArcError::InvalidDataType("x".into()).http_status(),
            400
        );
        assert_eq!(
            SenseArcError::ServiceNotReady("x".into()).http_status(),
            503
        );
        assert_eq!(
            SenseArcError::AnomalyNotFound("anom-1-1".into()).http_status(),
            404
        );
    }

    #[test]
    fn test_error_display() {
        let err = SenseArcError::TableNotFound(42);
        assert_eq!(format!("{}", err), "table not found: 42");
    }
}

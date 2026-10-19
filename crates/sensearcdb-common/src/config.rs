//! SenseArcDB 全局配置管理（统一入口）。
//!
//! ## 四层配置优先级（低 → 高）
//!
//! 1. `AppConfig::default()` — 硬编码出厂默认值
//! 2. `config/default.toml`  — 发布包携带的默认配置
//! 3. `./config.toml`       — 用户自定义覆盖
//! 4. 环境变量                — `SENSEARCDB_<SECTION>__<KEY>=<VALUE>`
//!
//! ## 热加载
//!
//! `ConfigHandle` 内部使用 `Arc<ArcSwap<AppConfig>>` 实现无锁热加载。
//! `get()` 返回当前配置快照（< 10ns）。发送 SIGHUP 信号触发 `reload()`。
//!
//! Wave 1: 20 个可配置参数（16 热加载 + 4 需重启）。
//! Wave 2: 新增 58 个可配置参数（48 热加载 + 10 需重启），累计 78 个。

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ─── 根配置 ───

/// SenseArcDB 全局配置根结构体。
///
/// 包含引擎、快照、路径、采集器、分区、保留、分层、备份、恢复子配置 section。
/// 所有字段使用 `#[serde(default)]`，缺失时回退到 `Default` 实现。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    /// 引擎配置（MemTable / Compaction / WAL）。
    #[serde(default)]
    pub engine: EngineConfig,

    /// 快照与实时数据库配置。
    #[serde(default)]
    pub snapshot: SnapshotConfig,

    /// 文件系统路径配置。
    #[serde(default)]
    pub paths: PathsConfig,

    // ─── Wave 2 新增 ───
    /// 采集器配置（OPC UA / MQTT / Modbus 等协议接入）。
    #[serde(default)]
    pub collector: CollectorConfig,

    /// 分区管理配置（时间分区 + Compaction）。
    #[serde(default)]
    pub partition: PartitionConfig,

    /// TTL 数据保留配置。
    #[serde(default)]
    pub retention: RetentionConfig,

    /// 冷热分层存储配置。
    #[serde(default)]
    pub tiering: TieringConfig,

    /// 备份配置（全量 + 增量 + WAL 归档）。
    #[serde(default)]
    pub backup: BackupConfig,

    /// 恢复配置（PITR + 并行下载 + 校验）。
    #[serde(default)]
    pub restore: RestoreConfig,

    // ─── Wave 3 新增 ───
    /// CQ 引擎配置（定时驱动四层窗口预聚合）。
    #[serde(default)]
    pub cq: CqConfig,

    /// 流计算引擎配置（数据驱动窗口聚合）。
    #[serde(default)]
    pub stream: StreamConfig,

    /// 派生标签引擎配置（表达式求值 + DAG 依赖）。
    #[serde(default)]
    pub derived: DerivedConfig,

    /// 安全体系配置（认证/授权/审计/加密/限流）。
    #[serde(default)]
    pub security: SecurityConfig,

    // ─── Wave 4 新增 ───
    /// 集群配置（Raft 共识 + 成员管理）。
    #[serde(default)]
    pub cluster: ClusterConfig,

    /// 分片配置（数据分片 + 副本 + 路由）。
    #[serde(default)]
    pub shard: ShardConfig,

    /// 边缘节点配置（边缘计算 + 数据同步）。
    #[serde(default)]
    pub edge: EdgeConfig,

    /// 事件引擎配置（事件处理 + 过滤 + 死信）。
    #[serde(default)]
    pub event: EventConfig,

    /// 告警引擎配置（规则评估 + 通知 + 模板）。
    #[serde(default)]
    pub alert: AlertConfig,

    // ─── Wave 5 新增 ───
    /// 预测引擎配置（DLinear ONNX 推理）。
    #[serde(default)]
    pub forecast: ForecastConfig,

    /// 条件评估引擎配置。
    #[serde(default)]
    pub condition: ConditionConfig,

    /// 异常检测引擎配置（IsolationForest ONNX 推理）。
    #[serde(default)]
    pub anomaly: AnomalyConfig,

    /// AI 运维引擎配置（根因分析 + 预测关联）。
    #[serde(default)]
    pub ai_ops: AiOpsConfig,

    /// 资产模型引擎配置（层级管理 + 健康评分）。
    #[serde(default)]
    pub asset_model: AssetModelConfig,

    // ─── Wave 6 新增 ───
    /// 可观测性配置（Prometheus /metrics + health + slow query + tracing）。
    #[serde(default)]
    pub observe: ObserveConfig,

    /// 部署运维配置（FORMAT_VERSION + 进程锁 + 优雅关闭）。
    #[serde(default)]
    pub deploy: DeployConfig,

    /// 前端控制台配置（WebSocket + heartbeat）。
    #[serde(default)]
    pub console: ConsoleConfig,

    // ─── Wave 7 多租户（S4）新增 ───
    /// 租户生命周期配置（CRUD / 冷静期 / 配额模板）。
    #[serde(default)]
    pub tenant: TenantConfig,
}

impl AppConfig {
    /// 从配置文件加载，缺失字段使用 `Default`，环境变量自动覆盖。
    ///
    /// # 参数
    /// - `path`: 用户配置文件路径（如 `"./config.toml"`）。
    ///
    /// # 优先级
    /// `Default → default.toml → path → SENSEARCDB_* env vars`
    pub fn load(path: &str) -> Result<Self, config::ConfigError> {
        config::Config::builder()
            // 第一层：硬编码默认值
            .add_source(config::Config::try_from(&AppConfig::default())?)
            // 第二层：发布包默认配置（可选）
            .add_source(config::File::with_name("config/default.toml").required(false))
            // 第三层：用户配置文件
            .add_source(config::File::with_name(path).required(false))
            // 第四层：环境变量 SENSEARCDB_<SECTION>__<KEY>
            .add_source(
                config::Environment::with_prefix("SENSEARCDB")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            engine: EngineConfig::default(),
            snapshot: SnapshotConfig::default(),
            paths: PathsConfig::default(),
            collector: CollectorConfig::default(),
            partition: PartitionConfig::default(),
            retention: RetentionConfig::default(),
            tiering: TieringConfig::default(),
            backup: BackupConfig::default(),
            restore: RestoreConfig::default(),
            cq: CqConfig::default(),
            stream: StreamConfig::default(),
            derived: DerivedConfig::default(),
            security: SecurityConfig::default(),
            cluster: ClusterConfig::default(),
            shard: ShardConfig::default(),
            edge: EdgeConfig::default(),
            event: EventConfig::default(),
            alert: AlertConfig::default(),
            forecast: ForecastConfig::default(),
            condition: ConditionConfig::default(),
            anomaly: AnomalyConfig::default(),
            ai_ops: AiOpsConfig::default(),
            asset_model: AssetModelConfig::default(),
            observe: ObserveConfig::default(),
            deploy: DeployConfig::default(),
            console: ConsoleConfig::default(),
            tenant: TenantConfig::default(),
        }
    }
}

// ─── 配置句柄（热加载） ───

/// 全局可热加载的配置句柄。
///
/// 内部使用 `Arc<ArcSwap<AppConfig>>`，`get()` 为无锁读（< 10ns）。
/// 应作为进程内单例在所有模块间共享。
#[derive(Debug, Clone)]
pub struct ConfigHandle {
    inner: Arc<ArcSwap<AppConfig>>,
}

impl ConfigHandle {
    /// 创建新的配置句柄。
    pub fn new(config: AppConfig) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(config)),
        }
    }

    /// 获取当前配置快照（无锁读）。
    ///
    /// 每次调用返回最新配置的 `Guard`，适用于热加载场景。
    /// 禁止在循环外缓存返回值（违反热加载语义）。
    #[inline]
    pub fn get(&self) -> arc_swap::Guard<Arc<AppConfig>> {
        self.inner.load()
    }

    /// SIGHUP 触发热重载。
    ///
    /// 重新读取配置文件并原子替换内部指针。
    /// 所有模块下一次 `get()` 调用自动获取新配置，无需额外通知机制。
    pub fn reload(&self, path: &str) -> Result<(), config::ConfigError> {
        let new_config = AppConfig::load(path)?;
        self.inner.store(Arc::new(new_config));
        tracing::info!("config reloaded successfully");
        Ok(())
    }

    /// 运行时原子替换配置（systemConfig PUT 热加载）。
    ///
    /// 与 `reload` 不同，本方法不读配置文件，而是直接接受调用方已修改好的
    /// `AppConfig` 快照。所有模块下一次 `get()` 调用自动获取新配置。
    pub fn swap(&self, config: AppConfig) {
        self.inner.store(Arc::new(config));
        tracing::info!("config swapped (runtime PUT)");
    }
}

// ─── 引擎配置 ───

/// 存储引擎配置（MemTable / Compaction / WAL）。
///
/// 所有 `u64` 类型的时间参数单位均为秒。
/// 每个字段使用 `#[serde(default)]` + 关联 `const fn` 提供默认值。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EngineConfig {
    /// MemTable 冻结阈值（数据点点数）。🔄 热加载
    #[serde(default = "EngineConfig::default_freeze_threshold")]
    pub freeze_threshold: usize,

    // Compaction 调度参数 — 全部 🔄 热加载
    /// Compaction 轮询间隔（秒）。
    #[serde(default = "EngineConfig::default_compaction_poll_interval_secs")]
    pub compaction_poll_interval_secs: u64,

    /// Compaction timeout 兜底（秒）。Level 在首次就绪后等待的最长时间。
    #[serde(default = "EngineConfig::default_compaction_max_wait_secs")]
    pub compaction_max_wait_secs: u64,

    /// 连续空转 N 次后进入暂停。
    #[serde(default = "EngineConfig::default_compaction_idle_threshold")]
    pub compaction_idle_threshold: u32,

    /// 暂停间隔（秒）。
    #[serde(default = "EngineConfig::default_compaction_idle_pause_secs")]
    pub compaction_idle_pause_secs: u64,

    /// L0 文件数触发阈值。
    #[serde(default = "EngineConfig::default_compaction_file_count")]
    pub compaction_default_file_count: usize,

    /// L0 数据量触发阈值 (GB)。
    #[serde(default = "EngineConfig::default_compaction_size_gb")]
    pub compaction_default_size_gb: u64,

    /// L1 文件数触发阈值（分代合并 L1→L2，默认 4）。
    #[serde(default = "EngineConfig::default_compaction_l1_file_count")]
    pub compaction_l1_file_count: usize,

    /// L2 文件数触发阈值（分代合并 L2→L3，默认 4）。
    #[serde(default = "EngineConfig::default_compaction_l2_file_count")]
    pub compaction_l2_file_count: usize,

    // WAL Group Commit 参数 — 全部 🔄 热加载
    /// 攒批上限（字节）。
    #[serde(default = "EngineConfig::default_wal_max_bytes")]
    pub wal_group_commit_max_bytes: usize,

    /// 自旋等待（微秒）。
    #[serde(default = "EngineConfig::default_wal_max_wait_us")]
    pub wal_group_commit_max_wait_us: u64,

    /// 兜底 flush 间隔（秒）。
    #[serde(default = "EngineConfig::default_wal_max_interval_secs")]
    pub wal_group_commit_max_interval_secs: u64,

    /// 空 batch 主动推进 watermark 间隔（秒）。
    #[serde(default = "EngineConfig::default_wal_watermark_interval_secs")]
    pub wal_flush_watermark_advance_interval_secs: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            freeze_threshold: 460_000,
            compaction_poll_interval_secs: 8,
            compaction_max_wait_secs: 3600,
            compaction_idle_threshold: 10,
            compaction_idle_pause_secs: 300,
            compaction_default_file_count: 16,
            compaction_default_size_gb: 1,
            compaction_l1_file_count: 4,
            compaction_l2_file_count: 4,
            wal_group_commit_max_bytes: 1_048_576,
            wal_group_commit_max_wait_us: 200,
            wal_group_commit_max_interval_secs: 1,
            wal_flush_watermark_advance_interval_secs: 1,
        }
    }
}

impl EngineConfig {
    const fn default_freeze_threshold() -> usize {
        460_000
    }
    const fn default_compaction_poll_interval_secs() -> u64 {
        8
    }
    const fn default_compaction_max_wait_secs() -> u64 {
        3600
    }
    const fn default_compaction_idle_threshold() -> u32 {
        10
    }
    const fn default_compaction_idle_pause_secs() -> u64 {
        300
    }
    const fn default_compaction_file_count() -> usize {
        16
    }
    const fn default_compaction_size_gb() -> u64 {
        1
    }
    const fn default_compaction_l1_file_count() -> usize {
        4
    }
    const fn default_compaction_l2_file_count() -> usize {
        4
    }
    const fn default_wal_max_bytes() -> usize {
        1_048_576
    }
    const fn default_wal_max_wait_us() -> u64 {
        200
    }
    const fn default_wal_max_interval_secs() -> u64 {
        1
    }
    const fn default_wal_watermark_interval_secs() -> u64 {
        1
    }
}

// ─── Snapshot 配置 ───

/// 快照与实时数据库配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SnapshotConfig {
    /// 分片倍数：`shard_count = N_CPU * shard_multiplier`。🔁 需重启
    #[serde(default = "SnapshotConfig::default_shard_multiplier")]
    pub shard_multiplier: usize,

    /// 浮点比较 epsilon。🔄 热加载
    #[serde(default = "SnapshotConfig::default_f64_epsilon")]
    pub f64_epsilon: f64,

    /// snapshot.bin 持久化间隔（秒）。🔄 热加载
    #[serde(default = "SnapshotConfig::default_snapshot_interval_secs")]
    pub snapshot_interval_secs: u64,

    /// SSE heartbeat 间隔（秒）。🔄 热加载
    #[serde(default = "SnapshotConfig::default_sse_heartbeat_secs")]
    pub sse_heartbeat_secs: u64,

    /// 单次批量查询 tag 上限。🔄 热加载
    #[serde(default = "SnapshotConfig::default_max_batch_tags")]
    pub max_batch_query_tags: usize,

    /// pending 刷新间隔（毫秒）。🔄 热加载（flusher 每次循环读）
    #[serde(default = "SnapshotConfig::default_flush_interval_ms")]
    pub flush_interval_ms: u64,

    /// pending 满点阈值。🔁 构造时读（首版不热加载）
    #[serde(default = "SnapshotConfig::default_flush_batch_size")]
    pub flush_batch_size: usize,

    /// pending 满字节阈值。🔁 构造时读
    #[serde(default = "SnapshotConfig::default_flush_bytes")]
    pub flush_bytes: usize,

    /// 回放配置子 section。
    #[serde(default)]
    pub replay: ReplayConfig,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            shard_multiplier: 8,
            f64_epsilon: 1e-12,
            snapshot_interval_secs: 5,
            sse_heartbeat_secs: 30,
            max_batch_query_tags: 200_000,
            flush_interval_ms: 1000,
            flush_batch_size: 8192,
            flush_bytes: 4 * 1024 * 1024,
            replay: ReplayConfig::default(),
        }
    }
}

impl SnapshotConfig {
    const fn default_shard_multiplier() -> usize {
        8
    }
    const fn default_f64_epsilon() -> f64 {
        1e-12
    }
    const fn default_snapshot_interval_secs() -> u64 {
        5
    }
    const fn default_sse_heartbeat_secs() -> u64 {
        30
    }
    const fn default_max_batch_tags() -> usize {
        200_000
    }
    const fn default_flush_interval_ms() -> u64 {
        1000
    }
    const fn default_flush_batch_size() -> usize {
        8192
    }
    const fn default_flush_bytes() -> usize {
        4 * 1024 * 1024
    }
}

// ─── 回放配置 ───

/// 回放引擎配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplayConfig {
    /// 最大并发回放会话数。🔄 热加载
    #[serde(default = "ReplayConfig::default_max_sessions")]
    pub max_sessions: usize,

    /// 单会话 entry 上限。🔄 热加载
    #[serde(default = "ReplayConfig::default_max_entries")]
    pub max_entries_per_session: usize,

    /// 事件环形缓冲区容量。🔁 需重启
    #[serde(default = "ReplayConfig::default_event_buffer")]
    pub event_buffer_size: usize,

    /// 空闲会话清理超时（秒）。🔄 热加载
    #[serde(default = "ReplayConfig::default_cleanup_secs")]
    pub session_idle_cleanup_secs: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            max_sessions: 5,
            max_entries_per_session: 500_000,
            event_buffer_size: 2000,
            session_idle_cleanup_secs: 300,
        }
    }
}

impl ReplayConfig {
    const fn default_max_sessions() -> usize {
        5
    }
    const fn default_max_entries() -> usize {
        500_000
    }
    const fn default_event_buffer() -> usize {
        2000
    }
    const fn default_cleanup_secs() -> u64 {
        300
    }
}

// ─── 路径配置 ───

/// 文件系统路径配置。🔁 全部需重启
///
/// 唯一根 = `data_dir`。所有子目录字段都是「相对名」，
/// 通过 getter 与 `data_dir` join 得到 PathBuf。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PathsConfig {
    /// 唯一数据根。所有运行时数据都在其下。
    #[serde(default = "PathsConfig::default_data_dir")]
    pub data_dir: String,

    /// SAC 数据子目录名（相对 data_dir）。
    #[serde(default = "PathsConfig::default_sac_dir")]
    pub sac_dir_name: String,

    /// WAL 数据子目录名（相对 data_dir）。
    #[serde(default = "PathsConfig::default_wal_dir")]
    pub wal_dir_name: String,

    /// 快照子目录名（相对 data_dir）。
    #[serde(default = "PathsConfig::default_snapshot_dir")]
    pub snapshot_dir_name: String,

    /// 快照文件名（相对 snapshot_dir）。
    #[serde(default = "PathsConfig::default_snapshot_file")]
    pub snapshot_file_name: String,

    /// 元数据库子目录名（相对 data_dir）。
    #[serde(default = "PathsConfig::default_metadata_dir")]
    pub metadata_dir_name: String,

    /// Schema 元数据库文件名（相对 metadata_dir）。
    #[serde(default = "PathsConfig::default_config_db")]
    pub config_db_name: String,

    /// 审计子目录名（相对 data_dir）。
    #[serde(default = "PathsConfig::default_audit_dir")]
    pub audit_dir_name: String,

    /// 本地备份子目录名（相对 data_dir）。
    #[serde(default = "PathsConfig::default_backup_dir")]
    pub backup_dir_name: String,

    /// TDE 密钥子目录名（相对 data_dir）。
    #[serde(default = "PathsConfig::default_keys_dir")]
    pub keys_dir_name: String,

    /// AI 运维模型注册数据库文件名（相对 metadata_dir）。
    #[serde(default = "PathsConfig::default_ai_ops_registry_db_path")]
    pub ai_ops_registry_db_name: String,

    /// AI 运维模型 Blob 存储目录名（相对 data_dir）。
    #[serde(default = "PathsConfig::default_ai_ops_blob_dir")]
    pub ai_ops_blob_dir_name: String,
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".into(),
            sac_dir_name: "sac".into(),
            wal_dir_name: "wal".into(),
            snapshot_dir_name: "snapshot".into(),
            snapshot_file_name: "snapshot.bin".into(),
            metadata_dir_name: "metadata".into(),
            config_db_name: "config.db".into(),
            audit_dir_name: "audit".into(),
            backup_dir_name: "backup".into(),
            keys_dir_name: "keys".into(),
            ai_ops_registry_db_name: "ai_ops_registry.db".into(),
            ai_ops_blob_dir_name: "ai_ops_blobs".into(),
        }
    }
}

impl PathsConfig {
    fn default_data_dir() -> String {
        "data".into()
    }
    fn default_sac_dir() -> String {
        "sac".into()
    }
    fn default_wal_dir() -> String {
        "wal".into()
    }
    fn default_snapshot_dir() -> String {
        "snapshot".into()
    }
    fn default_snapshot_file() -> String {
        "snapshot.bin".into()
    }
    fn default_metadata_dir() -> String {
        "metadata".into()
    }
    fn default_config_db() -> String {
        "config.db".into()
    }
    fn default_audit_dir() -> String {
        "audit".into()
    }
    fn default_backup_dir() -> String {
        "backup".into()
    }
    fn default_keys_dir() -> String {
        "keys".into()
    }
    fn default_ai_ops_registry_db_path() -> String {
        "ai_ops_registry.db".into()
    }
    fn default_ai_ops_blob_dir() -> String {
        "ai_ops_blobs".into()
    }

    /// 唯一数据根（可能为相对 cwd 或绝对路径）。
    pub fn data_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.data_dir)
    }

    /// SAC 数据目录 = data_dir/sac_dir_name。
    pub fn sac_dir(&self) -> std::path::PathBuf {
        self.data_dir().join(&self.sac_dir_name)
    }

    /// WAL 数据目录 = data_dir/wal_dir_name。
    pub fn wal_dir(&self) -> std::path::PathBuf {
        self.data_dir().join(&self.wal_dir_name)
    }

    /// 快照目录 = data_dir/snapshot_dir_name。
    pub fn snapshot_dir(&self) -> std::path::PathBuf {
        self.data_dir().join(&self.snapshot_dir_name)
    }

    /// 快照文件 = snapshot_dir/snapshot_file_name。
    pub fn snapshot_file(&self) -> std::path::PathBuf {
        self.snapshot_dir().join(&self.snapshot_file_name)
    }

    /// 元数据库目录 = data_dir/metadata_dir_name。
    pub fn metadata_dir(&self) -> std::path::PathBuf {
        self.data_dir().join(&self.metadata_dir_name)
    }

    /// Schema 元库 = metadata_dir/config_db_name。
    pub fn config_db_path(&self) -> std::path::PathBuf {
        self.metadata_dir().join(&self.config_db_name)
    }

    /// 审计目录 = data_dir/audit_dir_name。
    pub fn audit_dir(&self) -> std::path::PathBuf {
        self.data_dir().join(&self.audit_dir_name)
    }

    /// 备份目录 = data_dir/backup_dir_name。
    pub fn backup_dir(&self) -> std::path::PathBuf {
        self.data_dir().join(&self.backup_dir_name)
    }

    /// 密钥目录 = data_dir/keys_dir_name。
    pub fn keys_dir(&self) -> std::path::PathBuf {
        self.data_dir().join(&self.keys_dir_name)
    }

    /// AI 运维注册库 = metadata_dir/ai_ops_registry_db_name。
    pub fn ai_ops_registry_db_path(&self) -> std::path::PathBuf {
        self.metadata_dir().join(&self.ai_ops_registry_db_name)
    }

    /// AI 运维 blob 目录 = data_dir/ai_ops_blob_dir_name。
    pub fn ai_ops_blob_dir(&self) -> std::path::PathBuf {
        self.data_dir().join(&self.ai_ops_blob_dir_name)
    }

    /// 任意元数据库文件 = metadata_dir/<name>（供 asset.db/events.db 等复用）。
    pub fn metadata_db_path(&self, name: &str) -> std::path::PathBuf {
        self.metadata_dir().join(name)
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Wave 2 新增配置 section
// ══════════════════════════════════════════════════════════════════════════════

// ─── Collector 配置 ───

/// 采集器配置（Collector 通道 + 断线缓存 + 重连 + OPC UA）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CollectorConfig {
    /// 运行模式："embedded" | "remote"。🔁 需重启
    #[serde(default = "CollectorConfig::default_mode")]
    pub mode: String,

    /// Collector 唯一标识。🔁 需重启
    #[serde(default = "CollectorConfig::default_id")]
    pub collector_id: String,

    /// remote 模式远端 WriteService endpoint（如 "http://center-01:5201"）。🔁 需重启
    /// None → 回退 http://127.0.0.1:5201（本机，兼容现状）。
    #[serde(default)]
    pub remote_endpoint: Option<String>,

    /// remote 模式认证 API Key（随 ingest 注入 `x-api-key`，服务端据此推导 tenant）。🔁 需重启
    /// None → 不携带凭据（会被服务端 401 拒绝；gRPC 强制认证）。
    #[serde(default)]
    pub api_key: Option<String>,

    /// N×F 采集速率（点/秒）。🔄 热加载
    #[serde(default = "CollectorConfig::default_points_per_sec")]
    pub points_per_sec: u64,

    /// 单点字节估算值 B。🔄 热加载
    #[serde(default = "CollectorConfig::default_bytes_per_point")]
    pub bytes_per_point: u64,

    /// 通道配置子 section。
    #[serde(default)]
    pub channel: CollectorChannelConfig,

    /// 断线缓存 (Store & Forward) 配置。
    #[serde(default)]
    pub store_and_forward: SfConfig,

    /// 通用重连配置。
    #[serde(default)]
    pub reconnect: ReconnectConfig,

    /// OPC UA 重连配置。
    #[serde(default)]
    pub opcua: OpcUaReconnectConfig,

    /// 异构协议驱动清单（`[[collector.drivers]]`，`type` 字段区分协议）。
    ///
    /// 默认空清单 → 行为等价于「不接线」（不构造任何驱动、不启动采集循环）。
    /// 已实现 `modbus` / `mqtt` / `opcua` / `opcda` 四变体；opcda 仅 Windows 生效。
    #[serde(default)]
    pub drivers: Vec<DriverConfig>,

    /// 1 表 1 tag 模式的位号→表号 stride：`S<server>.<TagN>` 的表号 =
    /// `(server - 1) * stride + n`（默认 20000）。仅 "1 tag = 1 table" 约定使用。
    #[serde(default = "CollectorConfig::default_tag_table_stride")]
    pub tag_table_stride: u32,

    /// remote 模式 `table_id → table_name` 映射（客户端侧 wire 名解析）。
    ///
    /// 独立采集器进程不依赖 schema crate，无法像主进程那样经 `schema_mirror`
    /// 反查表名；改为显式配置映射。命中返回真实表名；未命中 resolver 返回
    /// `None`（GrpcWriter 报 `TableNotFound`，数据转 S&F，不误写）。
    #[serde(default)]
    pub table_names: std::collections::HashMap<u32, String>,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            mode: "embedded".into(),
            collector_id: "collector-01".into(),
            remote_endpoint: None,
            api_key: None,
            points_per_sec: 100_000,
            bytes_per_point: 100,
            channel: CollectorChannelConfig::default(),
            store_and_forward: SfConfig::default(),
            reconnect: ReconnectConfig::default(),
            opcua: OpcUaReconnectConfig::default(),
            drivers: Vec::new(),
            tag_table_stride: Self::default_tag_table_stride(),
            table_names: std::collections::HashMap::new(),
        }
    }
}

impl CollectorConfig {
    fn default_mode() -> String {
        "embedded".into()
    }
    fn default_id() -> String {
        "collector-01".into()
    }
    const fn default_points_per_sec() -> u64 {
        100_000
    }
    const fn default_bytes_per_point() -> u64 {
        100
    }
    pub const fn default_tag_table_stride() -> u32 {
        20_000
    }
}

// ─── Collector 驱动清单（Phase 7 D3：真实协议接入） ───

/// 异构协议驱动清单。
///
/// 使用 serde internally-tagged 表示：TOML 写
/// `type = "modbus" | "mqtt" | "opcua" | "opcda"`，
/// 其余字段按变体平铺在同一数组表内（`[[collector.drivers]]`）。
///
/// 注意：serde 的 internally-tagged 表示要求**所有变体都是结构体变体**（带命名字段），
/// 因此不能用 `Modbus(ModbusDriverConfig)` 这种 newtype 变体；字段在此直接平铺。
/// 已实现 modbus / mqtt / opcua / opcda 四变体（opcda 仅 Windows 生效）。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DriverConfig {
    /// Modbus TCP 驱动（轮询型）。
    Modbus {
        /// 目标表 ID（`Option` 缺省，缺省时解析为 0 = 默认表），解决 PointBatch 表归属。
        #[serde(default)]
        table_id: Option<u16>,
        /// 驱动唯一标识（作为 driver_id）。
        id: String,
        /// Modbus TCP 服务器地址。
        host: String,
        /// 端口（默认 502）。
        #[serde(default = "default_modbus_port")]
        port: u16,
        /// Modbus 从站 unit/slave ID（默认 1）。
        #[serde(default = "default_unit_id")]
        unit_id: u8,
        /// 轮询间隔（毫秒，默认 1000）。
        #[serde(default = "default_poll_ms")]
        poll_interval_ms: u64,
        /// 标签（寄存器）清单。
        tags: Vec<ModbusTagConfig>,
    },
    /// MQTT 驱动（订阅型）。
    Mqtt {
        /// 目标表 ID（缺省解析为 0 = 默认表）。
        #[serde(default)]
        table_id: Option<u16>,
        /// 驱动唯一标识（作为 driver_id）。
        id: String,
        /// MQTT broker 地址。
        host: String,
        /// 端口（默认 1883）。
        #[serde(default = "default_mqtt_port")]
        port: u16,
        /// MQTT client ID。
        client_id: String,
        /// 用户名（可选）。
        #[serde(default)]
        username: Option<String>,
        /// 密码（可选）。
        #[serde(default)]
        password: Option<String>,
        /// QoS：0 | 1 | 2（默认 1 = AtLeastOnce）。
        #[serde(default = "default_qos")]
        qos: u8,
        /// 传输：tcp | tls | ws（默认 tcp）。
        #[serde(default = "default_transport")]
        transport: String,
        /// 订阅主题清单。
        topics: Vec<MqttTopicConfig>,
    },
    /// OPC UA 驱动（poll / subscribe 双模式）。
    Opcua {
        /// 目标表 ID（缺省解析为 0 = 默认表）。
        #[serde(default)]
        table_id: Option<u16>,
        /// 驱动唯一标识（作为 driver_id）。
        id: String,
        /// OPC UA endpoint URL（如 `opc.tcp://127.0.0.1:4840`）。
        endpoint: String,
        /// 安全策略（MVP 仅支持 None；`Sign`/`SignAndEncrypt` 延后）。
        #[serde(default)]
        security_policy: Option<String>,
        /// 用户名（可选，缺省走 Anonymous）。
        #[serde(default)]
        username: Option<String>,
        /// 密码（可选，配合 username）。
        #[serde(default)]
        password: Option<String>,
        /// 采集模式："poll" | "subscribe"（默认 subscribe）。
        #[serde(default = "default_ua_mode")]
        mode: String,
        /// 轮询间隔（毫秒，仅 poll 模式使用，默认 1000）。
        #[serde(default = "default_poll_ms")]
        poll_interval_ms: u64,
        /// 标签清单（`name` = 逻辑点名，`node_id` = OPC UA NodeId 字符串）。
        tags: Vec<OpcUaTagConfig>,
    },
    /// OPC DA 驱动（Windows COM，轮询型）。
    ///
    /// 仅 Windows 生效：Windows 下为真 COM 连接/真读（`IOPCSyncIO::Read`），
    /// 非 Windows 下工厂跳过注册（不产生无用连接失败日志），显式实例化时
    /// `connect()` 保留报错兜底。
    Opcda {
        /// 目标表 ID（缺省解析为 0 = 默认表）。
        #[serde(default)]
        table_id: Option<u16>,
        /// 驱动唯一标识（作为 driver_id）。
        id: String,
        /// OPC DA server ProgID（如 `"Matrikon.OPC.Simulation.1"`）。
        prog_id: String,
        /// OPC DA server 主机名（空 = 本机；远程 DCOM 暂未实现）。
        #[serde(default)]
        host: String,
        /// 轮询间隔（毫秒，默认 1000）。
        ///
        /// 同时作为 OPC group 的 `dwRequestedUpdateRate` 传入 COM，替换
        /// 旧实现的硬编码 1000ms。
        #[serde(default = "default_poll_ms")]
        poll_interval_ms: u64,
        /// 标签清单（OPC item IDs，如 `"Random.Int1"`）。
        tags: Vec<String>,
    },
}

impl DriverConfig {
    /// 返回该驱动数据应归属的表 ID（`None` 或 0 均解析为 0）。
    pub fn table_id(&self) -> u32 {
        match self {
            DriverConfig::Modbus { table_id, .. } => table_id.unwrap_or(0) as u32,
            DriverConfig::Mqtt { table_id, .. } => table_id.unwrap_or(0) as u32,
            DriverConfig::Opcua { table_id, .. } => table_id.unwrap_or(0) as u32,
            DriverConfig::Opcda { table_id, .. } => table_id.unwrap_or(0) as u32,
        }
    }
}

/// Modbus 单个标签（寄存器）配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModbusTagConfig {
    /// 逻辑点名（作为 TagValue 的 tag_id / PointBatch 的 column_name）。
    pub name: String,
    /// 寄存器/线圈起始地址。
    pub address: u16,
    /// 功能码："coil" | "discrete" | "holding" | "input"。
    pub function: String,
    /// 数据类型："u16" | "i16" | "f32" | "u32" | "i32" | "bool"。
    pub data_type: String,
    /// 寄存器数量（f32/u32/i32 需 2，默认 1）。
    #[serde(default = "default_count")]
    pub count: u16,
}

/// MQTT 单个订阅主题配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MqttTopicConfig {
    /// 订阅主题过滤器（可含 `+` / `#` 通配符）。
    pub topic: String,
    /// 逻辑名（缺省 = topic），作为该主题消息的 tag_id。
    #[serde(default)]
    pub name: Option<String>,
}

impl MqttTopicConfig {
    /// 返回主题对应的逻辑名（缺省时回退为 topic 本身）。
    pub fn logical_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.topic)
    }
}

/// OPC UA 单个标签配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpcUaTagConfig {
    /// 逻辑点名（作为 TagValue 的 tag_id / PointBatch 的 column_name）。
    pub name: String,
    /// OPC UA NodeId 字符串（如 `ns=2;s=Temperature`）。
    pub node_id: String,
}

/// Modbus 默认端口。
pub fn default_modbus_port() -> u16 {
    502
}

/// Modbus 默认 unit/slave ID。
pub fn default_unit_id() -> u8 {
    1
}

/// 默认轮询间隔（毫秒）。
pub fn default_poll_ms() -> u64 {
    1_000
}

/// MQTT 默认端口。
pub fn default_mqtt_port() -> u16 {
    1_883
}

/// 默认 QoS。
pub fn default_qos() -> u8 {
    1
}

/// 默认传输方式。
pub fn default_transport() -> String {
    "tcp".into()
}

/// OPC UA 默认采集模式（subscribe）。
pub fn default_ua_mode() -> String {
    "subscribe".into()
}

/// 默认寄存器数量。
pub fn default_count() -> u16 {
    1
}

/// 采集器内部通道配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CollectorChannelConfig {
    /// mpsc channel 容量。🔁 需重启
    #[serde(default = "CollectorChannelConfig::default_capacity")]
    pub capacity: usize,

    /// 缓冲目标时延 T_buf（秒）。🔄 热加载
    #[serde(default = "CollectorChannelConfig::default_buffer_secs")]
    pub buffer_seconds: u64,

    /// 安全系数 K。🔄 热加载
    #[serde(default = "CollectorChannelConfig::default_safety_factor")]
    pub safety_factor: u64,
}

impl Default for CollectorChannelConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            buffer_seconds: 5,
            safety_factor: 2,
        }
    }
}

impl CollectorChannelConfig {
    const fn default_capacity() -> usize {
        10_000
    }
    const fn default_buffer_secs() -> u64 {
        5
    }
    const fn default_safety_factor() -> u64 {
        2
    }
}

/// 断线缓存 (Store & Forward) 配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SfConfig {
    /// 是否启用 S&F。🔁 需重启
    #[serde(default = "SfConfig::default_enabled")]
    pub enabled: bool,

    /// S&F SQLite 数据库路径。🔁 需重启（空 → `paths.metadata_db_path("collector_sf.db")`）。
    #[serde(default = "SfConfig::default_db_path")]
    pub db_path: String,

    /// 最大缓冲区字节数 M_sf (512MB)。🔄 热加载
    #[serde(default = "SfConfig::default_max_buffer_bytes")]
    pub max_buffer_bytes: u64,

    /// 最大数据保留时间（小时）。🔄 热加载
    #[serde(default = "SfConfig::default_max_retention_hours")]
    pub max_retention_hours: u64,
}

impl Default for SfConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            db_path: String::new(),
            max_buffer_bytes: 536_870_912,
            max_retention_hours: 72,
        }
    }
}

impl SfConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_db_path() -> String {
        String::new()
    }
    const fn default_max_buffer_bytes() -> u64 {
        536_870_912
    }
    const fn default_max_retention_hours() -> u64 {
        72
    }
}

/// 通用重连退避配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReconnectConfig {
    /// 最大重试次数（0=无限）。🔄 热加载
    #[serde(default = "ReconnectConfig::default_max_retries")]
    pub max_retries: u32,

    /// 首次退避（毫秒）。🔄 热加载
    #[serde(default = "ReconnectConfig::default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,

    /// 最大退避（毫秒）。🔄 热加载
    #[serde(default = "ReconnectConfig::default_max_backoff_ms")]
    pub max_backoff_ms: u64,

    /// 退避乘数。🔄 热加载
    #[serde(default = "ReconnectConfig::default_multiplier")]
    pub backoff_multiplier: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_retries: 0,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 30_000,
            backoff_multiplier: 2.0,
        }
    }
}

impl ReconnectConfig {
    const fn default_max_retries() -> u32 {
        0
    }
    const fn default_initial_backoff_ms() -> u64 {
        1_000
    }
    const fn default_max_backoff_ms() -> u64 {
        30_000
    }
    const fn default_multiplier() -> f64 {
        2.0
    }
}

/// OPC UA 重连配置（含全断保护）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpcUaReconnectConfig {
    /// 会话首次退避（毫秒）。🔄 热加载
    #[serde(default = "OpcUaReconnectConfig::default_session_initial_backoff_ms")]
    pub session_initial_backoff_ms: u64,

    /// 退避乘数。🔄 热加载
    #[serde(default = "OpcUaReconnectConfig::default_session_multiplier")]
    pub session_backoff_multiplier: f64,

    /// 最大退避（毫秒）。🔄 热加载
    #[serde(default = "OpcUaReconnectConfig::default_session_max_backoff_ms")]
    pub session_max_backoff_ms: u64,

    /// 最大重试次数（0=无限）。🔄 热加载
    #[serde(default = "OpcUaReconnectConfig::default_session_max_retries")]
    pub session_max_retries: u32,

    /// 并发重连上限。🔄 热加载
    #[serde(default = "OpcUaReconnectConfig::default_max_concurrent")]
    pub max_concurrent_reconnects: usize,

    /// 全断保护配置。
    #[serde(default)]
    pub global_backoff: GlobalBackoffConfig,
}

impl Default for OpcUaReconnectConfig {
    fn default() -> Self {
        Self {
            session_initial_backoff_ms: 1_000,
            session_backoff_multiplier: 2.0,
            session_max_backoff_ms: 60_000,
            session_max_retries: 0,
            max_concurrent_reconnects: 5,
            global_backoff: GlobalBackoffConfig::default(),
        }
    }
}

impl OpcUaReconnectConfig {
    const fn default_session_initial_backoff_ms() -> u64 {
        1_000
    }
    const fn default_session_multiplier() -> f64 {
        2.0
    }
    const fn default_session_max_backoff_ms() -> u64 {
        60_000
    }
    const fn default_session_max_retries() -> u32 {
        0
    }
    const fn default_max_concurrent() -> usize {
        5
    }
}

/// 全断保护配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobalBackoffConfig {
    /// 触发阈值比例（会话失败占比）。🔄 热加载
    #[serde(default = "GlobalBackoffConfig::default_threshold_ratio")]
    pub threshold_ratio: f64,

    /// 最少触发数量。🔄 热加载
    #[serde(default = "GlobalBackoffConfig::default_threshold_min")]
    pub threshold_min_count: usize,

    /// 统计时间窗口（秒）。🔄 热加载
    #[serde(default = "GlobalBackoffConfig::default_window_secs")]
    pub window_secs: u64,

    /// 全局退避持续时长（秒）。🔄 热加载
    #[serde(default = "GlobalBackoffConfig::default_duration_secs")]
    pub duration_secs: u64,
}

impl Default for GlobalBackoffConfig {
    fn default() -> Self {
        Self {
            threshold_ratio: 0.8,
            threshold_min_count: 3,
            window_secs: 30,
            duration_secs: 30,
        }
    }
}

impl GlobalBackoffConfig {
    const fn default_threshold_ratio() -> f64 {
        0.8
    }
    const fn default_threshold_min() -> usize {
        3
    }
    const fn default_window_secs() -> u64 {
        30
    }
    const fn default_duration_secs() -> u64 {
        30
    }
}

// ─── Partition 配置 ───

/// 分区管理配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartitionConfig {
    /// 分区时长（毫秒），默认 24h。🔁 需重启
    #[serde(default = "PartitionConfig::default_duration_ms")]
    pub duration_ms: u64,

    /// 时区偏移（秒），默认 UTC+8。🔁 需重启
    #[serde(default = "PartitionConfig::default_tz_offset_secs")]
    pub timezone_offset_secs: i64,

    /// 切换检查间隔（毫秒）。🔄 热加载
    #[serde(default = "PartitionConfig::default_check_interval_ms")]
    pub check_interval_ms: u64,

    /// inflight flush 等待超时（毫秒）。🔄 热加载
    #[serde(default = "PartitionConfig::default_closing_timeout_ms")]
    pub closing_timeout_ms: u64,

    /// 单分区最大 force_freeze 次数。🔄 热加载
    #[serde(default = "PartitionConfig::default_max_freezes")]
    pub max_freezes_per_partition: u32,

    /// 分区关闭后的 late 写入 grace 窗口（秒）。🔄 热加载
    ///
    /// 分区进入 Frozen 后，需再等这么久（默认 1 天）才允许 final compaction，
    /// 给回填/补传数据留出落旁路 L0 的口子（不变式 I4）。
    #[serde(default = "PartitionConfig::default_late_write_grace_secs")]
    pub late_write_grace_secs: u64,

    /// 最大可回填窗口（秒）。🔄 热加载
    ///
    /// 超过该窗口（默认 7 天）的 late 数据不再触发 re-final，避免无限追历史
    /// 导致 final 反复重写大文件（不变式 I6 的硬边界兜底）。
    #[serde(default = "PartitionConfig::default_max_late_window_secs")]
    pub max_late_window_secs: u64,

    /// flush 批量聚合表数（v9 多表聚合 L0）。🔄 热加载
    ///
    /// 冻结表攒批达到该数量即触发一次批量 flush；否则等 `flush_batch_timeout_ms`
    /// 兜底触发。默认 256 张表。
    #[serde(default = "PartitionConfig::default_flush_batch_size")]
    pub flush_batch_size: usize,

    /// flush 批量聚合兜底超时（毫秒）。🔄 热加载
    ///
    /// 冻结表数不足 `flush_batch_size` 时，等待该时长后仍触发一次批量 flush。
    /// 默认 1000ms。
    #[serde(default = "PartitionConfig::default_flush_batch_timeout_ms")]
    pub flush_batch_timeout_ms: u64,

    /// Compaction 子配置。
    #[serde(default)]
    pub compaction: PartitionCompactionConfig,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            duration_ms: 86_400_000,
            timezone_offset_secs: 28_800,
            check_interval_ms: 60_000,
            closing_timeout_ms: 30_000,
            max_freezes_per_partition: 1_000,
            late_write_grace_secs: 14_400,
            max_late_window_secs: 604_800,
            flush_batch_size: 256,
            flush_batch_timeout_ms: 1_000,
            compaction: PartitionCompactionConfig::default(),
        }
    }
}

impl PartitionConfig {
    const fn default_duration_ms() -> u64 {
        86_400_000
    }
    const fn default_tz_offset_secs() -> i64 {
        28_800
    }
    const fn default_check_interval_ms() -> u64 {
        60_000
    }
    const fn default_closing_timeout_ms() -> u64 {
        30_000
    }
    const fn default_max_freezes() -> u32 {
        1_000
    }
    const fn default_late_write_grace_secs() -> u64 {
        14_400
    }
    const fn default_max_late_window_secs() -> u64 {
        604_800
    }
    const fn default_flush_batch_size() -> usize {
        256
    }
    const fn default_flush_batch_timeout_ms() -> u64 {
        1_000
    }
}

/// 分区 Compaction 子配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartitionCompactionConfig {
    /// 并行 L2 Compaction 数。🔄 热加载
    #[serde(default = "PartitionCompactionConfig::default_max_concurrent")]
    pub max_concurrent_l2: usize,

    /// L2 Compaction 重试次数。🔄 热加载
    #[serde(default = "PartitionCompactionConfig::default_retry_max")]
    pub l2_retry_max: u32,

    /// .dead 文件延迟删除秒数。🔄 热加载
    #[serde(default = "PartitionCompactionConfig::default_cleanup_delay")]
    pub cleanup_delay_secs: u64,
}

impl Default for PartitionCompactionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_l2: 2,
            l2_retry_max: 3,
            cleanup_delay_secs: 60,
        }
    }
}

impl PartitionCompactionConfig {
    const fn default_max_concurrent() -> usize {
        2
    }
    const fn default_retry_max() -> u32 {
        3
    }
    const fn default_cleanup_delay() -> u64 {
        60
    }
}

// ─── Retention 配置 ───

/// TTL 数据保留配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetentionConfig {
    /// 是否启用 TTL。🔄 热加载
    #[serde(default = "RetentionConfig::default_enabled")]
    pub enabled: bool,

    /// 数据库级默认保留期（如 "90d"）。🔄 热加载
    #[serde(default = "RetentionConfig::default_retention")]
    pub default_retention: String,

    /// 过期后宽限期（如 "24h"）。🔄 热加载
    #[serde(default = "RetentionConfig::default_grace_period")]
    pub ttl_grace_period: String,

    /// TTL 扫描间隔（秒）。🔄 热加载
    #[serde(default = "RetentionConfig::default_scan_interval")]
    pub scan_interval_secs: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_retention: "90d".into(),
            ttl_grace_period: "24h".into(),
            scan_interval_secs: 3_600,
        }
    }
}

impl RetentionConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_retention() -> String {
        "90d".into()
    }
    fn default_grace_period() -> String {
        "24h".into()
    }
    const fn default_scan_interval() -> u64 {
        3_600
    }
}

// ─── Tiering 配置 ───

/// 冷热分层存储配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TieringConfig {
    /// 是否启用分层（默认关闭）。🔁 需重启
    #[serde(default = "TieringConfig::default_enabled")]
    pub enabled: bool,

    /// 分层扫描间隔（秒）。🔄 热加载
    #[serde(default = "TieringConfig::default_scan_interval")]
    pub scan_interval_secs: u64,

    /// 并行迁移分区数上限。🔄 热加载
    #[serde(default = "TieringConfig::default_migration_max")]
    pub migration_max_concurrent: usize,

    /// Hot 层配置。
    #[serde(default)]
    pub hot: HotTierConfig,

    /// Cold 层配置。
    #[serde(default)]
    pub cold: ColdTierConfig,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            scan_interval_secs: 3_600,
            migration_max_concurrent: 2,
            hot: HotTierConfig::default(),
            cold: ColdTierConfig::default(),
        }
    }
}

impl TieringConfig {
    fn default_enabled() -> bool {
        false
    }
    const fn default_scan_interval() -> u64 {
        3_600
    }
    const fn default_migration_max() -> usize {
        2
    }
}

/// Hot 层配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HotTierConfig {
    /// Hot 层保留时长（如 "720h" = 30d）。🔄 热加载
    #[serde(default = "HotTierConfig::default_retention")]
    pub retention: String,
}

impl Default for HotTierConfig {
    fn default() -> Self {
        Self {
            retention: "720h".into(),
        }
    }
}

impl HotTierConfig {
    fn default_retention() -> String {
        "720h".into()
    }
}

/// Cold 层配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColdTierConfig {
    /// 是否启用 Cold 层。🔁 需重启
    #[serde(default = "ColdTierConfig::default_enabled")]
    pub enabled: bool,

    /// Cold 层路径（S3/NFS）。🔁 需重启
    #[serde(default = "ColdTierConfig::default_path")]
    pub path: String,

    /// S3 区域。🔁 需重启
    #[serde(default = "ColdTierConfig::default_region")]
    pub region: String,

    /// Cold 层存储上限（如 "8760h" = 1y）。🔄 热加载
    #[serde(default = "ColdTierConfig::default_retention")]
    pub retention: String,
}

impl Default for ColdTierConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "s3://sensearc-cold/".into(),
            region: "ap-east-1".into(),
            retention: "8760h".into(),
        }
    }
}

impl ColdTierConfig {
    fn default_enabled() -> bool {
        false
    }
    fn default_path() -> String {
        "s3://sensearc-cold/".into()
    }
    fn default_region() -> String {
        "ap-east-1".into()
    }
    fn default_retention() -> String {
        "8760h".into()
    }
}

// ─── Backup 配置 ───

/// 备份配置（全量 + 增量 + WAL 归档）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackupConfig {
    /// 是否启用备份。🔁 需重启
    #[serde(default = "BackupConfig::default_enabled")]
    pub enabled: bool,

    /// 全量备份配置。
    #[serde(default)]
    pub full: FullBackupConfig,

    /// 增量备份配置。
    #[serde(default)]
    pub incremental: IncrementalBackupConfig,

    /// WAL 归档配置。
    #[serde(default)]
    pub wal_archiving: WalArchivingConfig,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            full: FullBackupConfig::default(),
            incremental: IncrementalBackupConfig::default(),
            wal_archiving: WalArchivingConfig::default(),
        }
    }
}

impl BackupConfig {
    fn default_enabled() -> bool {
        true
    }
}

/// 全量备份配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FullBackupConfig {
    /// 是否启用全量备份。🔄 热加载
    #[serde(default = "FullBackupConfig::default_enabled")]
    pub enabled: bool,

    /// cron 调度表达式。🔄 热加载
    #[serde(default = "FullBackupConfig::default_schedule")]
    pub schedule: String,

    /// 备份目标路径。🔄 热加载
    #[serde(default = "FullBackupConfig::default_target")]
    pub target: String,

    /// 保留最近 N 个全量备份。🔄 热加载
    #[serde(default = "FullBackupConfig::default_retention_count")]
    pub retention_count: u32,
}

impl Default for FullBackupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            schedule: "0 2 * * *".into(),
            target: "s3://sensearc-backup/prod/full/".into(),
            retention_count: 7,
        }
    }
}

impl FullBackupConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_schedule() -> String {
        "0 2 * * *".into()
    }
    fn default_target() -> String {
        "s3://sensearc-backup/prod/full/".into()
    }
    const fn default_retention_count() -> u32 {
        7
    }
}

/// 增量备份配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncrementalBackupConfig {
    /// 是否启用增量备份。🔄 热加载
    #[serde(default = "IncrementalBackupConfig::default_enabled")]
    pub enabled: bool,

    /// cron 调度表达式。🔄 热加载
    #[serde(default = "IncrementalBackupConfig::default_schedule")]
    pub schedule: String,

    /// 增量备份目标路径。🔄 热加载
    #[serde(default = "IncrementalBackupConfig::default_target")]
    pub target: String,

    /// 基线选择策略。🔄 热加载
    #[serde(default = "IncrementalBackupConfig::default_base_on")]
    pub base_on: String,
}

impl Default for IncrementalBackupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            schedule: "0 */6 * * *".into(),
            target: "s3://sensearc-backup/prod/incremental/".into(),
            base_on: "latest_full".into(),
        }
    }
}

impl IncrementalBackupConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_schedule() -> String {
        "0 */6 * * *".into()
    }
    fn default_target() -> String {
        "s3://sensearc-backup/prod/incremental/".into()
    }
    fn default_base_on() -> String {
        "latest_full".into()
    }
}

/// WAL 归档配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WalArchivingConfig {
    /// 是否启用 WAL 归档。🔄 热加载
    #[serde(default = "WalArchivingConfig::default_enabled")]
    pub enabled: bool,

    /// WAL 归档目标路径。🔄 热加载
    #[serde(default = "WalArchivingConfig::default_target")]
    pub target: String,

    /// 压缩算法：none | lz4 | zstd。🔄 热加载
    #[serde(default = "WalArchivingConfig::default_compress")]
    pub compress: String,

    /// 本地最多堆积未归档段数。🔄 热加载
    #[serde(default = "WalArchivingConfig::default_max_pending")]
    pub max_local_pending: usize,

    /// 告警阈值（超过此数触发 WARN 日志）。🔄 热加载
    #[serde(default = "WalArchivingConfig::default_warn_threshold")]
    pub warn_threshold: usize,
}

impl Default for WalArchivingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            target: "s3://sensearc-backup/prod/wal_archive/".into(),
            compress: "lz4".into(),
            max_local_pending: 64,
            warn_threshold: 32,
        }
    }
}

impl WalArchivingConfig {
    fn default_enabled() -> bool {
        false
    }
    fn default_target() -> String {
        "s3://sensearc-backup/prod/wal_archive/".into()
    }
    fn default_compress() -> String {
        "lz4".into()
    }
    const fn default_max_pending() -> usize {
        64
    }
    const fn default_warn_threshold() -> usize {
        32
    }
}

// ─── Restore 配置 ───

/// 恢复配置（PITR + 并行下载 + 校验）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RestoreConfig {
    /// 默认恢复源。🔄 热加载
    #[serde(default = "RestoreConfig::default_target")]
    pub default_target: String,

    /// 恢复临时目录。🔄 热加载
    #[serde(default = "RestoreConfig::default_temp_dir")]
    pub temp_dir: String,

    /// 并行下载数。🔄 热加载
    #[serde(default = "RestoreConfig::default_max_parallel")]
    pub max_parallel_downloads: usize,

    /// 是否校验 checksum。🔄 热加载
    #[serde(default = "RestoreConfig::default_verify")]
    pub verify_checksums: bool,
}

impl Default for RestoreConfig {
    fn default() -> Self {
        Self {
            default_target: "s3://sensearc-backup/prod/".into(),
            temp_dir: "/tmp/sensearc_restore/".into(),
            max_parallel_downloads: 8,
            verify_checksums: true,
        }
    }
}

impl RestoreConfig {
    fn default_target() -> String {
        "s3://sensearc-backup/prod/".into()
    }
    fn default_temp_dir() -> String {
        "/tmp/sensearc_restore/".into()
    }
    const fn default_max_parallel() -> usize {
        8
    }
    fn default_verify() -> bool {
        true
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Wave 3 新增配置 section
// ══════════════════════════════════════════════════════════════════════════════

// ─── CQ 配置 ───

/// CQ 引擎配置（定时驱动四层窗口预聚合）。
///
/// CQ 引擎以固定 1s tick 调度，在 CQ_10s / CQ_1m / CQ_1h / CQ_1d
/// 四层 Tumbling 窗口上执行增量聚合，结果通过 AutoRouter 供查询引擎使用。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CqConfig {
    /// CQ 引擎总开关。🔄 热加载
    #[serde(default = "CqConfig::default_enabled")]
    pub enabled: bool,

    /// 调度器 tick 间隔（毫秒）。🔄 热加载
    #[serde(default = "CqConfig::default_tick_interval_ms")]
    pub tick_interval_ms: u64,

    /// 并行执行的 CQ 窗口数上限。🔄 热加载
    #[serde(default = "CqConfig::default_max_concurrency")]
    pub max_concurrency: usize,

    /// 单次 tick 最多窗口数。🔄 热加载
    #[serde(default = "CqConfig::default_window_batch_size")]
    pub window_batch_size: usize,

    /// CQ 表 MemTable 最大驻留时间（秒），超时强制 freeze。🔄 热加载
    #[serde(default = "CqConfig::default_flush_interval_secs")]
    pub flush_interval_secs: u64,

    /// AutoRouter 最大返回点数。🔄 热加载
    #[serde(default = "CqConfig::default_autorouter_max_points")]
    pub autorouter_max_points: usize,

    /// 窗口 stale 判定时长（毫秒），默认 24h。🔄 热加载
    #[serde(default = "CqConfig::default_staleness_window_ms")]
    pub staleness_window_ms: u64,

    /// 窗口状态缓存上限。🔄 热加载
    #[serde(default = "CqConfig::default_max_window_states")]
    pub max_window_states: usize,

    /// CQ 执行失败最大重试次数。🔄 热加载
    #[serde(default = "CqConfig::default_max_retries")]
    pub max_retries: u32,

    /// 重试延迟（毫秒）。🔄 热加载
    #[serde(default = "CqConfig::default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

impl Default for CqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick_interval_ms: 1_000,
            max_concurrency: 4,
            window_batch_size: 32,
            flush_interval_secs: 300,
            autorouter_max_points: 10_000,
            staleness_window_ms: 86_400_000,
            max_window_states: 100_000,
            max_retries: 3,
            retry_delay_ms: 1_000,
        }
    }
}

impl CqConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_tick_interval_ms() -> u64 {
        1_000
    }
    const fn default_max_concurrency() -> usize {
        4
    }
    const fn default_window_batch_size() -> usize {
        32
    }
    const fn default_flush_interval_secs() -> u64 {
        300
    }
    const fn default_autorouter_max_points() -> usize {
        10_000
    }
    const fn default_staleness_window_ms() -> u64 {
        86_400_000
    }
    const fn default_max_window_states() -> usize {
        100_000
    }
    const fn default_max_retries() -> u32 {
        3
    }
    const fn default_retry_delay_ms() -> u64 {
        1_000
    }
}

// ─── Stream 配置 ───

/// 流计算引擎配置（数据驱动窗口聚合）。
///
/// 用户通过 `CREATE STREAM COMPUTE` SQL 定义 Tumbling/Hopping/Session 窗口，
/// 引擎以数据驱动方式触发聚合，结果写入用户指定的目标表。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamConfig {
    /// 流计算引擎总开关。🔄 热加载
    #[serde(default = "StreamConfig::default_enabled")]
    pub enabled: bool,

    /// mpsc channel 容量。🔁 需重启
    #[serde(default = "StreamConfig::default_channel_capacity")]
    pub channel_capacity: usize,

    /// Channel 满策略："DropOldest" | "Block"。🔄 热加载
    #[serde(default = "StreamConfig::default_channel_full_mode")]
    pub channel_full_mode: String,

    /// 消费并发度。🔄 热加载
    #[serde(default = "StreamConfig::default_max_concurrency")]
    pub max_concurrency: usize,

    /// Session 窗口默认 GAP（毫秒）。🔄 热加载
    #[serde(default = "StreamConfig::default_session_gap_ms")]
    pub session_default_gap_ms: u64,

    /// Session Timer 最大检查间隔（毫秒）。🔄 热加载
    #[serde(default = "StreamConfig::default_session_max_check_ms")]
    pub session_max_check_interval_ms: u64,

    /// Session Timer 最小检查间隔（毫秒）。🔄 热加载
    #[serde(default = "StreamConfig::default_session_min_check_ms")]
    pub session_min_check_interval_ms: u64,

    /// 窗口桶活跃上限（防 OOM）。🔄 热加载
    #[serde(default = "StreamConfig::default_max_active_buckets")]
    pub max_active_buckets: usize,

    /// 最大窗口大小（毫秒），默认 24h。🔄 热加载
    #[serde(default = "StreamConfig::default_max_window_ms")]
    pub max_window_interval_ms: u64,

    /// 每任务最大分区数。🔄 热加载
    #[serde(default = "StreamConfig::default_max_partitions")]
    pub max_partitions_per_task: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channel_capacity: 10_000,
            channel_full_mode: "DropOldest".into(),
            max_concurrency: 4,
            session_default_gap_ms: 30_000,
            session_max_check_interval_ms: 60_000,
            session_min_check_interval_ms: 1_000,
            max_active_buckets: 10_000,
            max_window_interval_ms: 86_400_000,
            max_partitions_per_task: 10_000,
        }
    }
}

impl StreamConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_channel_capacity() -> usize {
        10_000
    }
    fn default_channel_full_mode() -> String {
        "DropOldest".into()
    }
    const fn default_max_concurrency() -> usize {
        4
    }
    const fn default_session_gap_ms() -> u64 {
        30_000
    }
    const fn default_session_max_check_ms() -> u64 {
        60_000
    }
    const fn default_session_min_check_ms() -> u64 {
        1_000
    }
    const fn default_max_active_buckets() -> usize {
        10_000
    }
    const fn default_max_window_ms() -> u64 {
        86_400_000
    }
    const fn default_max_partitions() -> usize {
        10_000
    }
}

// ─── Derived 配置 ───

/// 派生标签引擎配置（表达式求值 + DAG 依赖）。
///
/// 用户通过 `CREATE DERIVED TAG` 定义表达式（`tag_c = tag_a + tag_b`），
/// 引擎在源变化时自动重算，支持去抖、Deadband、冷启动 Stale 检测。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DerivedConfig {
    /// 派生标签引擎总开关。🔄 热加载
    #[serde(default = "DerivedConfig::default_enabled")]
    pub enabled: bool,

    /// 去抖间隔（毫秒）。🔄 热加载
    #[serde(default = "DerivedConfig::default_debounce_ms")]
    pub debounce_interval_ms: u64,

    /// 单次 tick 最大重算标签数。🔄 热加载
    #[serde(default = "DerivedConfig::default_max_recalc")]
    pub max_recalc_per_tick: usize,

    /// 表达式 AST 最大深度。🔄 热加载
    #[serde(default = "DerivedConfig::default_max_ast_depth")]
    pub max_ast_depth: usize,

    /// 依赖图最大节点数。🔄 热加载
    #[serde(default = "DerivedConfig::default_max_graph_nodes")]
    pub max_graph_nodes: usize,

    /// 计算失败最大重试次数。🔄 热加载
    #[serde(default = "DerivedConfig::default_max_retries")]
    pub max_retries: u32,

    /// 单标签计算超时（毫秒）。🔄 热加载
    #[serde(default = "DerivedConfig::default_calc_timeout_ms")]
    pub calc_timeout_ms: u64,

    /// 冷启动 Stale 检测滞后容忍（秒）。🔄 热加载
    #[serde(default = "DerivedConfig::default_max_stale_secs")]
    pub max_stale_secs: u64,
}

impl Default for DerivedConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            debounce_interval_ms: 100,
            max_recalc_per_tick: 500,
            max_ast_depth: 32,
            max_graph_nodes: 10_000,
            max_retries: 3,
            calc_timeout_ms: 100,
            max_stale_secs: 300,
        }
    }
}

impl DerivedConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_debounce_ms() -> u64 {
        100
    }
    const fn default_max_recalc() -> usize {
        500
    }
    const fn default_max_ast_depth() -> usize {
        32
    }
    const fn default_max_graph_nodes() -> usize {
        10_000
    }
    const fn default_max_retries() -> u32 {
        3
    }
    const fn default_calc_timeout_ms() -> u64 {
        100
    }
    const fn default_max_stale_secs() -> u64 {
        300
    }
}

// ─── Security 配置 ───

/// 安全体系根配置（认证/授权/审计/加密/限流）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityConfig {
    /// 合规配置。
    #[serde(default)]
    pub compliance: SecurityComplianceConfig,

    /// 认证配置。
    #[serde(default)]
    pub auth: SecurityAuthConfig,

    /// 授权配置。
    #[serde(default)]
    pub authorization: SecurityAuthorizationConfig,

    /// 审计配置。
    #[serde(default)]
    pub audit: SecurityAuditConfig,

    /// 加密配置。
    #[serde(default)]
    pub encryption: SecurityEncryptionConfig,

    /// 限流配置。
    #[serde(default)]
    pub rate_limiting: SecurityRateLimitingConfig,

    /// 控制面路由表 fail-closed 降级开关（D12/Q4，默认 false=fail-closed）。
    ///
    /// `false`：挂载端点未登记入路由表 → 启动断言 panic（把漏登记变启动失败）。
    /// `true`：未映射 → 运行时 403 + 告警，不 panic（K8s 滚动 CrashLoop 降级）。
    #[serde(default = "SecurityConfig::default_allow_unmapped_routes")]
    pub allow_unmapped_routes: bool,

    /// WS ticket 配置（D9 短命小 JWT + 绑 IP）。
    #[serde(default)]
    pub ws_ticket: SecurityWsTicketConfig,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            compliance: SecurityComplianceConfig::default(),
            auth: SecurityAuthConfig::default(),
            authorization: SecurityAuthorizationConfig::default(),
            audit: SecurityAuditConfig::default(),
            encryption: SecurityEncryptionConfig::default(),
            rate_limiting: SecurityRateLimitingConfig::default(),
            allow_unmapped_routes: false,
            ws_ticket: SecurityWsTicketConfig::default(),
        }
    }
}

impl SecurityConfig {
    const fn default_allow_unmapped_routes() -> bool {
        false
    }
}

// ─── Security: WS Ticket ───

/// WS ticket 配置（D9：短命小 JWT + 绑 IP，无状态不引 Redis）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityWsTicketConfig {
    /// ticket 有效期（秒），覆盖前端重连退避上限 30s 留裕量。
    #[serde(default = "SecurityWsTicketConfig::default_ttl_secs")]
    pub ttl_secs: u64,

    /// 信任反代 IP/CIDR（用于 `X-Forwarded-For` 取真实源 IP；默认空 = 不信任）。
    #[serde(default)]
    pub trusted_proxies: Vec<String>,

    /// 允许的浏览器 Origin 白名单（防 CSWSH；空 = 仅无 Origin 头的非浏览器客户端）。
    #[serde(default)]
    pub origin_allowlist: Vec<String>,
}

impl Default for SecurityWsTicketConfig {
    fn default() -> Self {
        Self {
            ttl_secs: 60,
            trusted_proxies: Vec::new(),
            origin_allowlist: Vec::new(),
        }
    }
}

impl SecurityWsTicketConfig {
    const fn default_ttl_secs() -> u64 {
        60
    }
}

// ─── Security: Compliance ───

/// 合规配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityComplianceConfig {
    /// 合规模式："standard" | "equal3"。🔁 需重启
    #[serde(default = "SecurityComplianceConfig::default_profile")]
    pub profile: String,
}

impl Default for SecurityComplianceConfig {
    fn default() -> Self {
        Self {
            profile: "standard".into(),
        }
    }
}

impl SecurityComplianceConfig {
    fn default_profile() -> String {
        "standard".into()
    }
}

// ─── Security: Auth ───

/// 认证配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityAuthConfig {
    /// JWT 认证配置。
    #[serde(default)]
    pub jwt: SecurityJwtConfig,

    /// 密码策略配置。
    #[serde(default)]
    pub password: SecurityPasswordConfig,

    /// API Key 配置。
    #[serde(default)]
    pub api_key: SecurityApiKeyConfig,

    /// 紧急运维通道配置。
    #[serde(default)]
    pub break_glass: SecurityBreakGlassConfig,

    /// mTLS 配置。
    #[serde(default)]
    pub mtls: SecurityMtlsConfig,
}

impl Default for SecurityAuthConfig {
    fn default() -> Self {
        Self {
            jwt: SecurityJwtConfig::default(),
            password: SecurityPasswordConfig::default(),
            api_key: SecurityApiKeyConfig::default(),
            break_glass: SecurityBreakGlassConfig::default(),
            mtls: SecurityMtlsConfig::default(),
        }
    }
}

/// JWT 认证配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityJwtConfig {
    /// 签名算法："Ed25519" | "SM2"。🔁 需重启
    #[serde(default = "SecurityJwtConfig::default_algorithm")]
    pub algorithm: String,

    /// Access token 有效期（秒）。🔄 热加载
    #[serde(default = "SecurityJwtConfig::default_access_ttl")]
    pub access_token_ttl_sec: u64,

    /// Refresh token 有效期（秒）。🔄 热加载
    #[serde(default = "SecurityJwtConfig::default_refresh_ttl")]
    pub refresh_token_ttl_sec: u64,

    /// 签名密钥路径。🔁 需重启
    #[serde(default = "SecurityJwtConfig::default_signing_key_path")]
    pub signing_key_path: String,

    /// 密钥轮换间隔（天）。🔄 热加载
    #[serde(default = "SecurityJwtConfig::default_key_rotation_days")]
    pub key_rotation_interval_days: u32,
}

impl Default for SecurityJwtConfig {
    fn default() -> Self {
        Self {
            algorithm: "Ed25519".into(),
            access_token_ttl_sec: 3_600,
            refresh_token_ttl_sec: 86_400,
            signing_key_path: "/etc/sensearc/jwt/signing.key".into(),
            key_rotation_interval_days: 30,
        }
    }
}

impl SecurityJwtConfig {
    fn default_algorithm() -> String {
        "Ed25519".into()
    }
    const fn default_access_ttl() -> u64 {
        3_600
    }
    const fn default_refresh_ttl() -> u64 {
        86_400
    }
    fn default_signing_key_path() -> String {
        "/etc/sensearc/jwt/signing.key".into()
    }
    const fn default_key_rotation_days() -> u32 {
        30
    }
}

/// 密码策略配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityPasswordConfig {
    /// 最小密码长度。🔄 热加载
    #[serde(default = "SecurityPasswordConfig::default_min_length")]
    pub min_length: usize,

    /// 需大写字母。🔄 热加载
    #[serde(default = "SecurityPasswordConfig::default_require_upper")]
    pub require_upper: bool,

    /// 需小写字母。🔄 热加载
    #[serde(default = "SecurityPasswordConfig::default_require_lower")]
    pub require_lower: bool,

    /// 需数字。🔄 热加载
    #[serde(default = "SecurityPasswordConfig::default_require_digit")]
    pub require_digit: bool,

    /// 需特殊字符。🔄 热加载
    #[serde(default = "SecurityPasswordConfig::default_require_special")]
    pub require_special: bool,

    /// 最大登录失败次数。🔄 热加载
    #[serde(default = "SecurityPasswordConfig::default_max_login_attempts")]
    pub max_login_attempts: u32,

    /// 锁定时长（秒）。🔄 热加载
    #[serde(default = "SecurityPasswordConfig::default_lockout_duration")]
    pub lockout_duration_sec: u64,
}

impl Default for SecurityPasswordConfig {
    fn default() -> Self {
        Self {
            min_length: 8,
            require_upper: true,
            require_lower: true,
            require_digit: true,
            require_special: true,
            max_login_attempts: 5,
            lockout_duration_sec: 900,
        }
    }
}

impl SecurityPasswordConfig {
    const fn default_min_length() -> usize {
        8
    }
    fn default_require_upper() -> bool {
        true
    }
    fn default_require_lower() -> bool {
        true
    }
    fn default_require_digit() -> bool {
        true
    }
    fn default_require_special() -> bool {
        true
    }
    const fn default_max_login_attempts() -> u32 {
        5
    }
    const fn default_lockout_duration() -> u64 {
        900
    }
}

/// API Key 配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityApiKeyConfig {
    /// 默认有效期（天）。🔄 热加载
    #[serde(default = "SecurityApiKeyConfig::default_ttl_days")]
    pub default_ttl_days: u32,

    /// API Key 前缀。🔁 需重启
    #[serde(default = "SecurityApiKeyConfig::default_prefix")]
    pub prefix: String,

    /// 哈希算法。🔁 需重启
    #[serde(default = "SecurityApiKeyConfig::default_hash_algorithm")]
    pub hash_algorithm: String,

    /// 轮转 grace period（小时）。🔄 热加载
    #[serde(default = "SecurityApiKeyConfig::default_grace_period_hours")]
    pub rotation_grace_period_hours: u32,
}

impl Default for SecurityApiKeyConfig {
    fn default() -> Self {
        Self {
            default_ttl_days: 90,
            prefix: "sak_".into(),
            hash_algorithm: "sha256".into(),
            rotation_grace_period_hours: 24,
        }
    }
}

impl SecurityApiKeyConfig {
    const fn default_ttl_days() -> u32 {
        90
    }
    fn default_prefix() -> String {
        "sak_".into()
    }
    fn default_hash_algorithm() -> String {
        "sha256".into()
    }
    const fn default_grace_period_hours() -> u32 {
        24
    }
}

/// 紧急运维通道配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityBreakGlassConfig {
    /// 紧急运维通道开关。🔁 需重启
    #[serde(default = "SecurityBreakGlassConfig::default_enabled")]
    pub enabled: bool,

    /// Unix socket 路径。🔁 需重启
    #[serde(default = "SecurityBreakGlassConfig::default_socket_path")]
    pub socket_path: String,

    /// 令牌路径。🔁 需重启
    #[serde(default = "SecurityBreakGlassConfig::default_token_path")]
    pub token_path: String,
}

impl Default for SecurityBreakGlassConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            socket_path: "/var/run/sensearc/break-glass.sock".into(),
            token_path: "/etc/sensearc/break-glass.token".into(),
        }
    }
}

impl SecurityBreakGlassConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_socket_path() -> String {
        "/var/run/sensearc/break-glass.sock".into()
    }
    fn default_token_path() -> String {
        "/etc/sensearc/break-glass.token".into()
    }
}

/// mTLS 证书算法（阶段 6 裁决 #5：仅 Ed25519，删除 "SM2" 变体）。
///
/// rcgen 0.12 + rustls/aws-lc-rs 均不支持 SM2 证书 / 国密 TLS 套件；保留一个
/// 无法兑现的 "SM2" 枚举变体即「虚假能力占位」。删除后，用户配置
/// `cert_algorithm = "SM2"` 在**配置反序列化阶段**即报「未知枚举值」，比启动时
/// 二次校验更早、更干净。未来国密 TLS 成熟后作为新特性重新引入，走独立迁移。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum MtlsCertAlgorithm {
    /// Ed25519 证书（满足等保 §8.1.4 通信保密性；国密覆盖于 JWT-SM2/TDE-SM4-GCM/SM3）。
    Ed25519,
}

impl Default for MtlsCertAlgorithm {
    fn default() -> Self {
        MtlsCertAlgorithm::Ed25519
    }
}

/// mTLS 配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityMtlsConfig {
    /// mTLS 开关（默认关闭，沿用 phase33 D2「mTLS 默认 off、真启用不造假」）。🔁 需重启
    #[serde(default = "SecurityMtlsConfig::default_enabled")]
    pub enabled: bool,

    /// CA 证书路径。🔁 需重启
    #[serde(default = "SecurityMtlsConfig::default_ca_cert_path")]
    pub ca_cert_path: String,

    /// 服务端证书路径。🔁 需重启
    #[serde(default = "SecurityMtlsConfig::default_server_cert_path")]
    pub server_cert_path: String,

    /// 服务端密钥路径。🔁 需重启
    #[serde(default = "SecurityMtlsConfig::default_server_key_path")]
    pub server_key_path: String,

    /// 证书算法（仅 Ed25519）。🔁 需重启
    #[serde(default)]
    pub cert_algorithm: MtlsCertAlgorithm,

    /// 是否在证书缺失时自动生成自签证书（开发便利开关）。
    ///
    /// 显式替代旧的 `cfg!(debug_assertions)` 编译期绑定，使「开发自签 vs 生产拒绝」
    /// 由配置决定而非编译 profile。默认值跟随编译 profile 以保持既有行为：
    /// debug 构建默认 `true`、release 构建默认 `false`；生产环境可显式置 `false`
    /// 强制拒绝静默自签。🔁 需重启
    #[serde(default = "SecurityMtlsConfig::default_auto_generate")]
    pub auto_generate: bool,
}

impl Default for SecurityMtlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert_path: "/etc/sensearc/tls/ca.crt".into(),
            server_cert_path: "/etc/sensearc/tls/server.crt".into(),
            server_key_path: "/etc/sensearc/tls/server.key".into(),
            cert_algorithm: MtlsCertAlgorithm::Ed25519,
            auto_generate: cfg!(debug_assertions),
        }
    }
}

impl SecurityMtlsConfig {
    fn default_enabled() -> bool {
        false
    }
    fn default_ca_cert_path() -> String {
        "/etc/sensearc/tls/ca.crt".into()
    }
    fn default_server_cert_path() -> String {
        "/etc/sensearc/tls/server.crt".into()
    }
    fn default_server_key_path() -> String {
        "/etc/sensearc/tls/server.key".into()
    }
    const fn default_auto_generate() -> bool {
        cfg!(debug_assertions)
    }
}

// ─── Security: Authorization ───

/// 授权配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityAuthorizationConfig {
    /// 默认拒绝策略。🔄 热加载
    #[serde(default = "SecurityAuthorizationConfig::default_policy")]
    pub default_policy: String,

    /// 权限缓存配置。
    #[serde(default)]
    pub cache: SecurityAuthzCacheConfig,

    /// 标签级行过滤配置。
    #[serde(default)]
    pub tag_filtering: SecurityTagFilteringConfig,
}

impl Default for SecurityAuthorizationConfig {
    fn default() -> Self {
        Self {
            default_policy: "deny".into(),
            cache: SecurityAuthzCacheConfig::default(),
            tag_filtering: SecurityTagFilteringConfig::default(),
        }
    }
}

impl SecurityAuthorizationConfig {
    fn default_policy() -> String {
        "deny".into()
    }
}

/// 权限缓存配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityAuthzCacheConfig {
    /// 权限缓存开关。🔄 热加载
    #[serde(default = "SecurityAuthzCacheConfig::default_enabled")]
    pub enabled: bool,

    /// 缓存 TTL（秒）。🔄 热加载
    #[serde(default = "SecurityAuthzCacheConfig::default_ttl_sec")]
    pub ttl_sec: u64,

    /// 最大缓存条目。🔄 热加载
    #[serde(default = "SecurityAuthzCacheConfig::default_max_entries")]
    pub max_entries: usize,
}

impl Default for SecurityAuthzCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_sec: 60,
            max_entries: 100_000,
        }
    }
}

impl SecurityAuthzCacheConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_ttl_sec() -> u64 {
        60
    }
    const fn default_max_entries() -> usize {
        100_000
    }
}

/// 标签级行过滤配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityTagFilteringConfig {
    /// 标签级行过滤开关。🔄 热加载
    #[serde(default = "SecurityTagFilteringConfig::default_enabled")]
    pub enabled: bool,
}

impl Default for SecurityTagFilteringConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl SecurityTagFilteringConfig {
    fn default_enabled() -> bool {
        true
    }
}

// ─── Security: Audit ───

/// 审计配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityAuditConfig {
    /// 审计总开关。🔄 热加载
    #[serde(default = "SecurityAuditConfig::default_enabled")]
    pub enabled: bool,

    /// 审计存储配置。
    #[serde(default)]
    pub storage: SecurityAuditStorageConfig,

    /// 审计事件配置。
    #[serde(default)]
    pub events: SecurityAuditEventsConfig,

    /// 采样配置。
    #[serde(default)]
    pub sampling: SecurityAuditSamplingConfig,

    /// 哈希链配置。
    #[serde(default)]
    pub hash_chain: SecurityAuditHashChainConfig,
}

impl Default for SecurityAuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            storage: SecurityAuditStorageConfig::default(),
            events: SecurityAuditEventsConfig::default(),
            sampling: SecurityAuditSamplingConfig::default(),
            hash_chain: SecurityAuditHashChainConfig::default(),
        }
    }
}

impl SecurityAuditConfig {
    fn default_enabled() -> bool {
        true
    }
}

/// 审计存储配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityAuditStorageConfig {
    /// 存储后端："sac" | "syslog"。🔁 需重启
    #[serde(default = "SecurityAuditStorageConfig::default_backend")]
    pub backend: String,

    /// 最大存储量（GB）。🔄 热加载
    #[serde(default = "SecurityAuditStorageConfig::default_max_size_gb")]
    pub max_size_gb: u64,

    /// 保留天数。🔄 热加载
    #[serde(default = "SecurityAuditStorageConfig::default_retention_days")]
    pub retention_days: u32,
}

impl Default for SecurityAuditStorageConfig {
    fn default() -> Self {
        Self {
            backend: "sac".into(),
            max_size_gb: 100,
            retention_days: 180,
        }
    }
}

impl SecurityAuditStorageConfig {
    fn default_backend() -> String {
        "sac".into()
    }
    const fn default_max_size_gb() -> u64 {
        100
    }
    const fn default_retention_days() -> u32 {
        180
    }
}

/// 审计事件配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityAuditEventsConfig {
    /// 认证事件。🔄 热加载
    #[serde(default = "SecurityAuditEventsConfig::default_auth")]
    pub auth: bool,

    /// 授权事件。🔄 热加载
    #[serde(default = "SecurityAuditEventsConfig::default_authorization")]
    pub authorization: bool,

    /// 管理事件。🔄 热加载
    #[serde(default = "SecurityAuditEventsConfig::default_admin")]
    pub admin: bool,

    /// 数据读写事件（默认关闭）。🔄 热加载
    #[serde(default = "SecurityAuditEventsConfig::default_data_access")]
    pub data_access: bool,

    /// 配置变更事件。🔄 热加载
    #[serde(default = "SecurityAuditEventsConfig::default_config")]
    pub config: bool,

    /// 系统事件。🔄 热加载
    #[serde(default = "SecurityAuditEventsConfig::default_system")]
    pub system: bool,
}

impl Default for SecurityAuditEventsConfig {
    fn default() -> Self {
        Self {
            auth: true,
            authorization: true,
            admin: true,
            data_access: false,
            config: true,
            system: true,
        }
    }
}

impl SecurityAuditEventsConfig {
    fn default_auth() -> bool {
        true
    }
    fn default_authorization() -> bool {
        true
    }
    fn default_admin() -> bool {
        true
    }
    fn default_data_access() -> bool {
        false
    }
    fn default_config() -> bool {
        true
    }
    fn default_system() -> bool {
        true
    }
}

/// 审计采样配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityAuditSamplingConfig {
    /// data_access 采样率。🔄 热加载
    #[serde(default = "SecurityAuditSamplingConfig::default_data_access_rate")]
    pub data_access_rate: f64,
}

impl Default for SecurityAuditSamplingConfig {
    fn default() -> Self {
        Self {
            data_access_rate: 0.01,
        }
    }
}

impl SecurityAuditSamplingConfig {
    const fn default_data_access_rate() -> f64 {
        0.01
    }
}

/// 哈希链配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityAuditHashChainConfig {
    /// 哈希链开关。🔄 热加载
    #[serde(default = "SecurityAuditHashChainConfig::default_enabled")]
    pub enabled: bool,

    /// 哈希算法："SHA-256" | "SM3"。🔄 热加载
    #[serde(default = "SecurityAuditHashChainConfig::default_hash_algorithm")]
    pub hash_algorithm: String,
}

impl Default for SecurityAuditHashChainConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            hash_algorithm: "SHA-256".into(),
        }
    }
}

impl SecurityAuditHashChainConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_hash_algorithm() -> String {
        "SHA-256".into()
    }
}

// ─── Security: Encryption ───

/// 加密配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityEncryptionConfig {
    /// TDE 配置。
    #[serde(default)]
    pub tde: SecurityTdeConfig,

    /// 密钥管理配置。
    #[serde(default)]
    pub key_management: SecurityKeyManagementConfig,

    /// 硬件加速配置。
    #[serde(default)]
    pub hardware_acceleration: SecurityHwAccelConfig,
}

impl Default for SecurityEncryptionConfig {
    fn default() -> Self {
        Self {
            tde: SecurityTdeConfig::default(),
            key_management: SecurityKeyManagementConfig::default(),
            hardware_acceleration: SecurityHwAccelConfig::default(),
        }
    }
}

/// TDE 配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityTdeConfig {
    /// TDE 开关。🔁 需重启
    #[serde(default = "SecurityTdeConfig::default_enabled")]
    pub enabled: bool,

    /// 加密算法："AES-256-GCM" | "SM4-GCM"。🔁 需重启
    #[serde(default = "SecurityTdeConfig::default_algorithm")]
    pub algorithm: String,

    /// Page 大小（字节）。🔁 需重启
    #[serde(default = "SecurityTdeConfig::default_page_size")]
    pub page_size_bytes: usize,

    /// WAL 加密开关。🔁 需重启
    #[serde(default = "SecurityTdeConfig::default_wal_encryption")]
    pub wal_encryption: bool,
}

impl Default for SecurityTdeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: "AES-256-GCM".into(),
            page_size_bytes: 65_536,
            wal_encryption: true,
        }
    }
}

impl SecurityTdeConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_algorithm() -> String {
        "AES-256-GCM".into()
    }
    const fn default_page_size() -> usize {
        65_536
    }
    fn default_wal_encryption() -> bool {
        true
    }
}

/// 密钥管理配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityKeyManagementConfig {
    /// 密钥管理模式："local" | "vault" | "aws_kms"。🔁 需重启
    #[serde(default = "SecurityKeyManagementConfig::default_mode")]
    pub mode: String,

    /// 本地密钥配置。
    #[serde(default)]
    pub local: SecurityKmsLocalConfig,

    /// DEK 轮换配置。
    #[serde(default)]
    pub dek_rotation: SecurityDekRotationConfig,

    /// DEK 内存配置。
    #[serde(default)]
    pub dek_memory: SecurityDekMemoryConfig,
}

impl Default for SecurityKeyManagementConfig {
    fn default() -> Self {
        Self {
            mode: "local".into(),
            local: SecurityKmsLocalConfig::default(),
            dek_rotation: SecurityDekRotationConfig::default(),
            dek_memory: SecurityDekMemoryConfig::default(),
        }
    }
}

impl SecurityKeyManagementConfig {
    fn default_mode() -> String {
        "local".into()
    }
}

/// 本地 KMS 配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityKmsLocalConfig {
    /// KEK 文件路径。🔁 需重启
    #[serde(default = "SecurityKmsLocalConfig::default_master_key_file")]
    pub master_key_file: String,

    /// KEK 派生算法。🔁 需重启
    #[serde(default = "SecurityKmsLocalConfig::default_derivation")]
    pub master_key_derivation: String,

    /// PBKDF2 迭代次数。🔄 热加载
    #[serde(default = "SecurityKmsLocalConfig::default_pbkdf2_iterations")]
    pub pbkdf2_iterations: u32,
}

impl Default for SecurityKmsLocalConfig {
    fn default() -> Self {
        Self {
            master_key_file: "/etc/sensearc/tde/master.key".into(),
            master_key_derivation: "sha256_pbkdf2".into(),
            pbkdf2_iterations: 600_000,
        }
    }
}

impl SecurityKmsLocalConfig {
    fn default_master_key_file() -> String {
        "/etc/sensearc/tde/master.key".into()
    }
    fn default_derivation() -> String {
        "sha256_pbkdf2".into()
    }
    const fn default_pbkdf2_iterations() -> u32 {
        600_000
    }
}

/// DEK 轮换配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityDekRotationConfig {
    /// DEK 轮换间隔（小时）。🔄 热加载
    #[serde(default = "SecurityDekRotationConfig::default_interval_hours")]
    pub interval_hours: u32,

    /// 每 DEK 最大加密字节数 (10GB)。🔄 热加载
    #[serde(default = "SecurityDekRotationConfig::default_max_bytes")]
    pub max_bytes_per_key: u64,
}

impl Default for SecurityDekRotationConfig {
    fn default() -> Self {
        Self {
            interval_hours: 24,
            max_bytes_per_key: 10_737_418_240,
        }
    }
}

impl SecurityDekRotationConfig {
    const fn default_interval_hours() -> u32 {
        24
    }
    const fn default_max_bytes() -> u64 {
        10_737_418_240
    }
}

/// DEK 内存配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityDekMemoryConfig {
    /// 锁定 DEK 内存页 (mlock)。🔁 需重启
    #[serde(default = "SecurityDekMemoryConfig::default_mlock")]
    pub mlock: bool,

    /// Drop 时清零 (zeroize)。🔁 需重启
    #[serde(default = "SecurityDekMemoryConfig::default_zeroize")]
    pub zeroize_on_drop: bool,
}

impl Default for SecurityDekMemoryConfig {
    fn default() -> Self {
        Self {
            mlock: true,
            zeroize_on_drop: true,
        }
    }
}

impl SecurityDekMemoryConfig {
    fn default_mlock() -> bool {
        true
    }
    fn default_zeroize() -> bool {
        true
    }
}

/// 硬件加速配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityHwAccelConfig {
    /// AES-NI 自动检测。🔄 热加载
    #[serde(default = "SecurityHwAccelConfig::default_aes_ni")]
    pub aes_ni: bool,

    /// ARM SM4-CE 自动检测。🔄 热加载
    #[serde(default = "SecurityHwAccelConfig::default_arm_sm4_ce")]
    pub arm_sm4_ce: bool,
}

impl Default for SecurityHwAccelConfig {
    fn default() -> Self {
        Self {
            aes_ni: true,
            arm_sm4_ce: true,
        }
    }
}

impl SecurityHwAccelConfig {
    fn default_aes_ni() -> bool {
        true
    }
    fn default_arm_sm4_ce() -> bool {
        true
    }
}

// ─── Security: Rate Limiting ───

/// 限流配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityRateLimitingConfig {
    /// 限流总开关。🔄 热加载
    #[serde(default = "SecurityRateLimitingConfig::default_enabled")]
    pub enabled: bool,

    /// 全局限流配置。
    #[serde(default)]
    pub global: SecurityRateLimitGlobalConfig,

    /// API Key 默认限流配置。
    #[serde(default)]
    pub api_key_defaults: SecurityRateLimitApiKeyDefaultsConfig,

    /// 用户默认限流配置。
    #[serde(default)]
    pub user_defaults: SecurityRateLimitUserDefaultsConfig,

    /// 端点限流配置。
    #[serde(default)]
    pub endpoints: SecurityRateLimitEndpointsConfig,

    /// 审计限流防抖配置。
    #[serde(default)]
    pub audit_throttle: SecurityRateLimitAuditThrottleConfig,
}

impl Default for SecurityRateLimitingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            global: SecurityRateLimitGlobalConfig::default(),
            api_key_defaults: SecurityRateLimitApiKeyDefaultsConfig::default(),
            user_defaults: SecurityRateLimitUserDefaultsConfig::default(),
            endpoints: SecurityRateLimitEndpointsConfig::default(),
            audit_throttle: SecurityRateLimitAuditThrottleConfig::default(),
        }
    }
}

impl SecurityRateLimitingConfig {
    fn default_enabled() -> bool {
        true
    }
}

/// 全局限流配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityRateLimitGlobalConfig {
    /// 最大连接数（0=自动: CPU×100, min 500）。🔄 热加载
    #[serde(default = "SecurityRateLimitGlobalConfig::default_max_connections")]
    pub max_connections: usize,

    /// 超级用户预留连接。🔄 热加载
    #[serde(default = "SecurityRateLimitGlobalConfig::default_superuser_reserved")]
    pub superuser_reserved_connections: usize,

    /// 最大并发查询数（0=自动: CPU×4）。🔄 热加载
    #[serde(default = "SecurityRateLimitGlobalConfig::default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,

    /// 写入吞吐限制（字节/秒，0=不限）。🔄 热加载
    #[serde(default = "SecurityRateLimitGlobalConfig::default_write_throughput")]
    pub write_throughput_bytes_per_sec: u64,

    /// 全局查询排队超时（秒）。🔄 热加载
    #[serde(default = "SecurityRateLimitGlobalConfig::default_query_queue_timeout")]
    pub query_queue_timeout_sec: u64,

    /// 查询队列最大深度（0=自动: 100×CPU）。🔄 热加载
    #[serde(default = "SecurityRateLimitGlobalConfig::default_query_queue_depth")]
    pub query_queue_max_depth: usize,
}

impl Default for SecurityRateLimitGlobalConfig {
    fn default() -> Self {
        Self {
            max_connections: 0,
            superuser_reserved_connections: 5,
            max_concurrent_queries: 0,
            write_throughput_bytes_per_sec: 0,
            query_queue_timeout_sec: 60,
            query_queue_max_depth: 0,
        }
    }
}

impl SecurityRateLimitGlobalConfig {
    const fn default_max_connections() -> usize {
        0
    }
    const fn default_superuser_reserved() -> usize {
        5
    }
    const fn default_max_concurrent_queries() -> usize {
        0
    }
    const fn default_write_throughput() -> u64 {
        0
    }
    const fn default_query_queue_timeout() -> u64 {
        60
    }
    const fn default_query_queue_depth() -> usize {
        0
    }
}

/// API Key 默认限流配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityRateLimitApiKeyDefaultsConfig {
    /// 写入 QPS。🔄 热加载
    #[serde(default = "SecurityRateLimitApiKeyDefaultsConfig::default_write_qps")]
    pub write_qps: u64,

    /// 写入吞吐（字节/秒）。🔄 热加载
    #[serde(default = "SecurityRateLimitApiKeyDefaultsConfig::default_write_throughput")]
    pub write_throughput_bytes_per_sec: u64,

    /// Token Bucket 突发倍数。🔄 热加载
    #[serde(default = "SecurityRateLimitApiKeyDefaultsConfig::default_burst_multiplier")]
    pub write_burst_multiplier: u32,

    /// API Key 最大连接数。🔄 热加载
    #[serde(default = "SecurityRateLimitApiKeyDefaultsConfig::default_max_connections")]
    pub max_connections: usize,
}

impl Default for SecurityRateLimitApiKeyDefaultsConfig {
    fn default() -> Self {
        Self {
            write_qps: 1_000,
            write_throughput_bytes_per_sec: 10_485_760,
            write_burst_multiplier: 3,
            max_connections: 4,
        }
    }
}

impl SecurityRateLimitApiKeyDefaultsConfig {
    const fn default_write_qps() -> u64 {
        1_000
    }
    const fn default_write_throughput() -> u64 {
        10_485_760
    }
    const fn default_burst_multiplier() -> u32 {
        3
    }
    const fn default_max_connections() -> usize {
        4
    }
}

/// 用户默认限流配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityRateLimitUserDefaultsConfig {
    /// 查询 QPS。🔄 热加载
    #[serde(default = "SecurityRateLimitUserDefaultsConfig::default_query_qps")]
    pub query_qps: u64,

    /// 并发查询数。🔄 热加载
    #[serde(default = "SecurityRateLimitUserDefaultsConfig::default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,

    /// Token Bucket 突发倍数。🔄 热加载
    #[serde(default = "SecurityRateLimitUserDefaultsConfig::default_burst_multiplier")]
    pub query_burst_multiplier: u32,

    /// 用户最大连接数。🔄 热加载
    #[serde(default = "SecurityRateLimitUserDefaultsConfig::default_max_connections")]
    pub max_connections: usize,

    /// 用户级排队超时（秒）。🔄 热加载
    #[serde(default = "SecurityRateLimitUserDefaultsConfig::default_query_queue_timeout")]
    pub query_queue_timeout_sec: u64,
}

impl Default for SecurityRateLimitUserDefaultsConfig {
    fn default() -> Self {
        Self {
            query_qps: 50,
            max_concurrent_queries: 8,
            query_burst_multiplier: 5,
            max_connections: 5,
            query_queue_timeout_sec: 30,
        }
    }
}

impl SecurityRateLimitUserDefaultsConfig {
    const fn default_query_qps() -> u64 {
        50
    }
    const fn default_max_concurrent_queries() -> usize {
        8
    }
    const fn default_burst_multiplier() -> u32 {
        5
    }
    const fn default_max_connections() -> usize {
        5
    }
    const fn default_query_queue_timeout() -> u64 {
        30
    }
}

/// 端点限流配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityRateLimitEndpointsConfig {
    /// 登录端点限流。
    #[serde(default)]
    pub auth: SecurityEndpointRateLimitConfig,

    /// 管理端点限流。
    #[serde(default)]
    pub admin: SecurityEndpointRateLimitConfig,

    /// 查询端点限流。
    #[serde(default)]
    pub query: SecurityEndpointRateLimitConfig,
}

impl Default for SecurityRateLimitEndpointsConfig {
    fn default() -> Self {
        Self {
            auth: SecurityEndpointRateLimitConfig { qps: 20, burst: 5 },
            admin: SecurityEndpointRateLimitConfig { qps: 100, burst: 0 },
            query: SecurityEndpointRateLimitConfig {
                qps: 1_000,
                burst: 50,
            },
        }
    }
}

/// 单端点限流参数。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityEndpointRateLimitConfig {
    /// 每秒请求数上限。🔄 热加载
    #[serde(default = "SecurityEndpointRateLimitConfig::default_qps")]
    pub qps: u64,

    /// 突发容量（0=不启用单独突发）。🔄 热加载
    #[serde(default = "SecurityEndpointRateLimitConfig::default_burst")]
    pub burst: u64,
}

impl SecurityEndpointRateLimitConfig {
    const fn default_qps() -> u64 {
        0
    }
    const fn default_burst() -> u64 {
        0
    }
}

impl Default for SecurityEndpointRateLimitConfig {
    fn default() -> Self {
        Self { qps: 0, burst: 0 }
    }
}

/// 审计限流防抖配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityRateLimitAuditThrottleConfig {
    /// 同主体限流日志防抖间隔（秒）。🔄 热加载
    #[serde(default = "SecurityRateLimitAuditThrottleConfig::default_same_limit_interval")]
    pub same_limit_interval_sec: u64,
}

impl Default for SecurityRateLimitAuditThrottleConfig {
    fn default() -> Self {
        Self {
            same_limit_interval_sec: 30,
        }
    }
}

impl SecurityRateLimitAuditThrottleConfig {
    const fn default_same_limit_interval() -> u64 {
        30
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Wave 4 新增配置 section
// ══════════════════════════════════════════════════════════════════════════════

// ─── Cluster 配置 ───

/// 初始集群成员条目（bootstrap 阶段显式携带 node_id → 地址映射）。
///
/// `bootstrap_members` 是**全量成员列表（含自身）**：每个条目显式声明一个集群
/// 成员的节点 id、Raft 地址，以及（shard 模式下）该成员自己承载的 shard 集合
/// [`hosted_shards`](Self::hosted_shards)。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterPeerConfig {
    /// 节点 Raft ID（必须与 `cluster.node_id` 的 `node-<N>` 解析结果一致）。
    ///
    /// 本字段描述的是该成员条目**自身**（含本节点），而非单纯「对端」。
    pub node_id: u64,
    /// 该节点 Raft RPC 地址（`host:port`）。
    pub raft_addr: String,
    /// 该节点 HTTP API 地址（可选，供管理面/重定向展示）。
    #[serde(default)]
    pub api_addr: Option<String>,
    /// 该成员声明自己承载（instance）的 Shard ID 列表（shard 模式承载分布的**唯一真相源**）。
    ///
    /// 语义（阶段 7 收官项 1c 修订）：`hosted_shards` = 该成员承载的 shard 集合，
    /// 与 primary 正交——primary 由 `primary_for_shard` 纯函数唯一确定，
    /// `hosted_shards` 只决定该成员 instance 哪些 shard（承载过滤）。
    ///
    /// 多节点下按**字面**解释（空 = 不承载）；「空 = 全承载」仅作用于单节点
    /// self 缺失回退（由 startup 层处理，非本字段语义）。
    #[serde(default)]
    pub hosted_shards: Vec<u32>,
}

/// 集群配置（Raft 共识 + 成员管理）。
///
/// SenseArcDB 集群使用 Raft 协议实现强一致性复制。
/// 单机模式下 `enabled = false`，其他字段仅集群模式生效。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterConfig {
    /// 集群模式开关（单机默认关闭）。🔁 需重启
    #[serde(default = "ClusterConfig::default_enabled")]
    pub enabled: bool,

    /// 节点唯一标识。🔁 需重启
    #[serde(default = "ClusterConfig::default_node_id")]
    pub node_id: String,

    /// Raft 内部通信地址。🔁 需重启
    #[serde(default = "ClusterConfig::default_bind_addr")]
    pub bind_addr: String,

    /// 对外广播地址（NAT/容器场景）。🔁 需重启
    #[serde(default = "ClusterConfig::default_advertise_addr")]
    pub advertise_addr: String,

    /// Raft 状态持久化目录。🔁 需重启
    #[serde(default = "ClusterConfig::default_data_dir")]
    pub data_dir: String,

    /// 初始集群成员（含 node_id/raft_addr/api_addr）。🔁 需重启
    ///
    /// 非空即「本节点属初始集群，走 `initialize`」；与 `join_addr` 互斥，
    /// 两者皆空时走单节点 bootstrap（兼容单机）。
    #[serde(default)]
    pub bootstrap_members: Vec<ClusterPeerConfig>,

    /// 非初始成员时，向该地址所在 leader 发起加入（`host:port`）。🔁 需重启
    #[serde(default)]
    pub join_addr: Option<String>,

    /// Raft 选举超时（毫秒）。🔄 热加载
    #[serde(default = "ClusterConfig::default_election_timeout_ms")]
    pub election_timeout_ms: u64,

    /// Leader 心跳间隔（毫秒）。🔄 热加载
    #[serde(default = "ClusterConfig::default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,

    /// 单次 AppendEntries 最大日志数。🔄 热加载
    #[serde(default = "ClusterConfig::default_max_log_batch_size")]
    pub max_log_batch_size: usize,

    /// 每 N 条日志触发快照。🔄 热加载
    #[serde(default = "ClusterConfig::default_snapshot_interval_logs")]
    pub snapshot_interval_logs: u64,

    /// 快照传输分块大小（字节）。🔄 热加载
    #[serde(default = "ClusterConfig::default_snapshot_chunk_size")]
    pub snapshot_chunk_size: usize,

    /// 单次 RPC 最大载荷（字节）。🔄 热加载
    #[serde(default = "ClusterConfig::default_max_payload_bytes")]
    pub max_payload_bytes: usize,

    /// 集群 gRPC 配置。
    #[serde(default)]
    pub grpc: ClusterGrpcConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: "node-01".into(),
            bind_addr: "0.0.0.0:5300".into(),
            advertise_addr: "127.0.0.1:5300".into(),
            data_dir: "data/cluster".into(),
            bootstrap_members: Vec::new(),
            join_addr: None,
            election_timeout_ms: 3_000,
            heartbeat_interval_ms: 500,
            max_log_batch_size: 1_000,
            snapshot_interval_logs: 10_000,
            snapshot_chunk_size: 1_048_576,
            max_payload_bytes: 4_194_304,
            grpc: ClusterGrpcConfig::default(),
        }
    }
}

impl ClusterConfig {
    fn default_enabled() -> bool {
        false
    }
    fn default_node_id() -> String {
        "node-01".into()
    }
    fn default_bind_addr() -> String {
        "0.0.0.0:5300".into()
    }
    fn default_advertise_addr() -> String {
        "127.0.0.1:5300".into()
    }
    fn default_data_dir() -> String {
        "data/cluster".into()
    }
    const fn default_election_timeout_ms() -> u64 {
        3_000
    }
    const fn default_heartbeat_interval_ms() -> u64 {
        500
    }
    const fn default_max_log_batch_size() -> usize {
        1_000
    }
    const fn default_snapshot_interval_logs() -> u64 {
        10_000
    }
    const fn default_snapshot_chunk_size() -> usize {
        1_048_576
    }
    const fn default_max_payload_bytes() -> usize {
        4_194_304
    }
}

/// 集群 gRPC 配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterGrpcConfig {
    /// 集群 gRPC 服务地址。🔁 需重启
    #[serde(default = "ClusterGrpcConfig::default_server_addr")]
    pub server_addr: String,

    /// 最大并发流。🔄 热加载
    #[serde(default = "ClusterGrpcConfig::default_max_concurrent_streams")]
    pub max_concurrent_streams: usize,

    /// gRPC keepalive 间隔（秒）。🔄 热加载
    #[serde(default = "ClusterGrpcConfig::default_keepalive_interval_secs")]
    pub keepalive_interval_secs: u64,
}

impl Default for ClusterGrpcConfig {
    fn default() -> Self {
        Self {
            server_addr: "0.0.0.0:5300".into(),
            max_concurrent_streams: 256,
            keepalive_interval_secs: 30,
        }
    }
}

impl ClusterGrpcConfig {
    fn default_server_addr() -> String {
        "0.0.0.0:5300".into()
    }
    const fn default_max_concurrent_streams() -> usize {
        256
    }
    const fn default_keepalive_interval_secs() -> u64 {
        30
    }
}

// ─── Shard 配置 ───

/// 分片配置（数据分片 + 副本 + 路由）。
///
/// 分片策略决定数据如何在节点间分布。单机模式下 `enabled = false`。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardConfig {
    /// 分片模式开关（单机默认关闭）。🔁 需重启
    #[serde(default = "ShardConfig::default_enabled")]
    pub enabled: bool,

    /// 总分片数。🔁 需重启
    #[serde(default = "ShardConfig::default_total_shards")]
    pub total_shards: usize,

    /// 副本因子。🔁 需重启
    #[serde(default = "ShardConfig::default_replication_factor")]
    pub replication_factor: usize,

    /// Shard 0（MetaShard）的 ShardService gRPC 地址（`host:port`）。🔁 需重启
    ///
    /// 非 Shard 0 节点用它定位 Shard 0，做拓扑上报（`RegisterShards` push）+
    /// 拓扑拉取（`GetShardMap` pull）。空 = 单节点全承载退路（本节点即 Shard 0，
    /// 无需发现）——5c-3d 拓扑传播双向接线。
    #[serde(default)]
    pub meta_shard_addr: Option<String>,

    /// 放置策略："hash_ring" | "range" | "manual"。🔄 热加载
    #[serde(default = "ShardConfig::default_placement_strategy")]
    pub placement_strategy: String,

    /// 一致性哈希虚拟节点数。🔄 热加载
    #[serde(default = "ShardConfig::default_hash_ring_vnodes")]
    pub hash_ring_vnodes: usize,

    /// 触发 rebalance 的不均衡阈值。🔄 热加载
    #[serde(default = "ShardConfig::default_rebalance_threshold")]
    pub rebalance_threshold: f64,

    /// 并行迁移分片数上限。🔄 热加载
    #[serde(default = "ShardConfig::default_rebalance_max_concurrent")]
    pub rebalance_max_concurrent: usize,

    /// 分片迁移超时（秒）。🔄 热加载
    #[serde(default = "ShardConfig::default_migration_timeout_secs")]
    pub migration_timeout_secs: u64,

    /// Leader lease 最大时长（毫秒）。🔄 热加载
    #[serde(default = "ShardConfig::default_max_leader_lease_ms")]
    pub max_leader_lease_ms: u64,

    /// 路由缓存配置。
    #[serde(default)]
    pub routing_cache: ShardRoutingCacheConfig,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            total_shards: 8,
            replication_factor: 3,
            meta_shard_addr: None,
            placement_strategy: "hash_ring".into(),
            hash_ring_vnodes: 256,
            rebalance_threshold: 0.1,
            rebalance_max_concurrent: 2,
            migration_timeout_secs: 3_600,
            max_leader_lease_ms: 10_000,
            routing_cache: ShardRoutingCacheConfig::default(),
        }
    }
}

impl ShardConfig {
    fn default_enabled() -> bool {
        false
    }
    const fn default_total_shards() -> usize {
        8
    }
    const fn default_replication_factor() -> usize {
        3
    }
    fn default_placement_strategy() -> String {
        "hash_ring".into()
    }
    const fn default_hash_ring_vnodes() -> usize {
        256
    }
    const fn default_rebalance_threshold() -> f64 {
        0.1
    }
    const fn default_rebalance_max_concurrent() -> usize {
        2
    }
    const fn default_migration_timeout_secs() -> u64 {
        3_600
    }
    const fn default_max_leader_lease_ms() -> u64 {
        10_000
    }
}

/// 分片路由缓存配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardRoutingCacheConfig {
    /// 路由缓存开关。🔄 热加载
    #[serde(default = "ShardRoutingCacheConfig::default_enabled")]
    pub enabled: bool,

    /// 缓存 TTL（秒）。🔄 热加载
    #[serde(default = "ShardRoutingCacheConfig::default_ttl_sec")]
    pub ttl_sec: u64,

    /// 最大缓存条目。🔄 热加载
    #[serde(default = "ShardRoutingCacheConfig::default_max_entries")]
    pub max_entries: usize,
}

impl Default for ShardRoutingCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_sec: 30,
            max_entries: 100_000,
        }
    }
}

impl ShardRoutingCacheConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_ttl_sec() -> u64 {
        30
    }
    const fn default_max_entries() -> usize {
        100_000
    }
}

// ─── Edge 配置 ───

/// 边缘节点配置（边缘计算 + 数据同步）。
///
/// 边缘节点可部署在工厂/基站等近数据源位置，支持离线运行和断线重连后增量同步。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EdgeConfig {
    /// 边缘节点模式开关。🔁 需重启
    #[serde(default = "EdgeConfig::default_enabled")]
    pub enabled: bool,

    /// 边缘节点唯一标识。🔁 需重启
    #[serde(default = "EdgeConfig::default_edge_id")]
    pub edge_id: String,

    /// 上游集群地址列表。🔄 热加载
    #[serde(default = "EdgeConfig::default_upstream_addrs")]
    pub upstream_addrs: Vec<String>,

    /// 本节点 EdgeSyncService gRPC 监听地址（接收下级子节点 PushSyncBatch）。🔁 需重启
    #[serde(default = "EdgeConfig::default_server_addr")]
    pub server_addr: String,

    /// 同步模式："full" | "incremental" | "hybrid"。🔄 热加载
    #[serde(default = "EdgeConfig::default_sync_mode")]
    pub sync_mode: String,

    /// 同步间隔（秒）。🔄 热加载
    #[serde(default = "EdgeConfig::default_sync_interval_secs")]
    pub sync_interval_secs: u64,

    /// 本地缓冲上限（MB）。🔄 热加载
    #[serde(default = "EdgeConfig::default_local_buffer_max_mb")]
    pub local_buffer_max_mb: u64,

    /// 本地数据保留时长（小时）。🔄 热加载
    #[serde(default = "EdgeConfig::default_local_retention_hours")]
    pub local_retention_hours: u64,

    /// 离线数据 TTL（秒）。🔄 热加载
    #[serde(default = "EdgeConfig::default_offline_ttl_secs")]
    pub offline_ttl_secs: u64,

    /// 冲突解决策略："lww" | "crdt" | "upstream_wins"。🔄 热加载
    #[serde(default = "EdgeConfig::default_conflict_resolution")]
    pub conflict_resolution: String,

    /// 带宽控制配置。
    #[serde(default)]
    pub bandwidth: EdgeBandwidthConfig,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            edge_id: "edge-01".into(),
            upstream_addrs: vec!["127.0.0.1:5300".into()],
            server_addr: "0.0.0.0:5204".into(),
            sync_mode: "incremental".into(),
            sync_interval_secs: 60,
            local_buffer_max_mb: 2_048,
            local_retention_hours: 168,
            offline_ttl_secs: 86_400,
            conflict_resolution: "lww".into(),
            bandwidth: EdgeBandwidthConfig::default(),
        }
    }
}

impl EdgeConfig {
    fn default_enabled() -> bool {
        false
    }
    fn default_edge_id() -> String {
        "edge-01".into()
    }
    fn default_upstream_addrs() -> Vec<String> {
        vec!["127.0.0.1:5300".into()]
    }
    fn default_server_addr() -> String {
        "0.0.0.0:5204".into()
    }
    fn default_sync_mode() -> String {
        "incremental".into()
    }
    const fn default_sync_interval_secs() -> u64 {
        60
    }
    const fn default_local_buffer_max_mb() -> u64 {
        2_048
    }
    const fn default_local_retention_hours() -> u64 {
        168
    }
    const fn default_offline_ttl_secs() -> u64 {
        86_400
    }
    fn default_conflict_resolution() -> String {
        "lww".into()
    }
}

/// 边缘节点带宽控制配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EdgeBandwidthConfig {
    /// 上行带宽上限（Kbps）。🔄 热加载
    #[serde(default = "EdgeBandwidthConfig::default_max_upload_kbps")]
    pub max_upload_kbps: u64,

    /// 下行带宽上限（Kbps）。🔄 热加载
    #[serde(default = "EdgeBandwidthConfig::default_max_download_kbps")]
    pub max_download_kbps: u64,

    /// QoS 开关。🔄 热加载
    #[serde(default = "EdgeBandwidthConfig::default_qos_enabled")]
    pub qos_enabled: bool,
}

impl Default for EdgeBandwidthConfig {
    fn default() -> Self {
        Self {
            max_upload_kbps: 10_240,
            max_download_kbps: 10_240,
            qos_enabled: true,
        }
    }
}

impl EdgeBandwidthConfig {
    const fn default_max_upload_kbps() -> u64 {
        10_240
    }
    const fn default_max_download_kbps() -> u64 {
        10_240
    }
    fn default_qos_enabled() -> bool {
        true
    }
}

// ─── Event 配置 ───

/// 事件引擎配置（事件处理 + 过滤 + 死信）。
///
/// 事件引擎负责订阅数据变化并触发用户定义的处理逻辑。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventConfig {
    /// 事件引擎总开关。🔄 热加载
    #[serde(default = "EventConfig::default_enabled")]
    pub enabled: bool,

    /// 最大并发事件处理器数。🔄 热加载
    #[serde(default = "EventConfig::default_max_concurrent_handlers")]
    pub max_concurrent_handlers: usize,

    /// 单事件处理超时（秒）。🔄 热加载
    #[serde(default = "EventConfig::default_handler_timeout_secs")]
    pub handler_timeout_secs: u64,

    /// 事件队列容量。🔁 需重启
    #[serde(default = "EventConfig::default_queue_capacity")]
    pub queue_capacity: usize,

    /// 投递语义："at_most_once" | "at_least_once"。🔄 热加载
    #[serde(default = "EventConfig::default_delivery_semantics")]
    pub delivery_semantics: String,

    /// 事件投递最大重试次数。🔄 热加载
    #[serde(default = "EventConfig::default_retry_max")]
    pub retry_max: u32,

    /// 事件重试退避间隔（毫秒）。🔄 热加载
    #[serde(default = "EventConfig::default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,

    /// 死信队列开关。🔄 热加载
    #[serde(default = "EventConfig::default_dead_letter_enabled")]
    pub dead_letter_enabled: bool,

    /// 死信队列最大条目。🔄 热加载
    #[serde(default = "EventConfig::default_dead_letter_max_size")]
    pub dead_letter_max_size: usize,

    /// 过滤规则配置。
    #[serde(default)]
    pub filters: EventFiltersConfig,
}

impl Default for EventConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_handlers: 32,
            handler_timeout_secs: 30,
            queue_capacity: 10_000,
            delivery_semantics: "at_least_once".into(),
            retry_max: 3,
            retry_backoff_ms: 1_000,
            dead_letter_enabled: true,
            dead_letter_max_size: 10_000,
            filters: EventFiltersConfig::default(),
        }
    }
}

impl EventConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_max_concurrent_handlers() -> usize {
        32
    }
    const fn default_handler_timeout_secs() -> u64 {
        30
    }
    const fn default_queue_capacity() -> usize {
        10_000
    }
    fn default_delivery_semantics() -> String {
        "at_least_once".into()
    }
    const fn default_retry_max() -> u32 {
        3
    }
    const fn default_retry_backoff_ms() -> u64 {
        1_000
    }
    fn default_dead_letter_enabled() -> bool {
        true
    }
    const fn default_dead_letter_max_size() -> usize {
        10_000
    }
}

/// 事件过滤规则配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventFiltersConfig {
    /// 每订阅最大过滤规则数。🔄 热加载
    #[serde(default = "EventFiltersConfig::default_max_rules")]
    pub max_rules_per_subscription: usize,

    /// 过滤规则评估超时（毫秒）。🔄 热加载
    #[serde(default = "EventFiltersConfig::default_evaluation_timeout_ms")]
    pub evaluation_timeout_ms: u64,
}

impl Default for EventFiltersConfig {
    fn default() -> Self {
        Self {
            max_rules_per_subscription: 100,
            evaluation_timeout_ms: 10,
        }
    }
}

impl EventFiltersConfig {
    const fn default_max_rules() -> usize {
        100
    }
    const fn default_evaluation_timeout_ms() -> u64 {
        10
    }
}

// ─── Alert 配置 ───

/// 告警引擎配置（规则评估 + 通知 + 模板）。
///
/// 告警引擎周期性地评估告警规则，触发通知并管理告警生命周期。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertConfig {
    /// 告警引擎总开关。🔄 热加载
    #[serde(default = "AlertConfig::default_enabled")]
    pub enabled: bool,

    /// 告警规则评估间隔（秒）。🔄 热加载
    #[serde(default = "AlertConfig::default_evaluation_interval_secs")]
    pub evaluation_interval_secs: u64,

    /// 并行评估上限。🔄 热加载
    #[serde(default = "AlertConfig::default_max_concurrent_evaluations")]
    pub max_concurrent_evaluations: usize,

    /// 最大告警规则数。🔄 热加载
    #[serde(default = "AlertConfig::default_max_rules")]
    pub max_rules: usize,

    /// 活跃告警数上限。🔄 热加载
    #[serde(default = "AlertConfig::default_max_active_alerts")]
    pub max_active_alerts: usize,

    /// 自动恢复超时（秒）。🔄 热加载
    #[serde(default = "AlertConfig::default_auto_resolve_timeout_secs")]
    pub auto_resolve_timeout_secs: u64,

    /// 通知冷却时间（秒）。🔄 热加载
    #[serde(default = "AlertConfig::default_notification_cooldown_secs")]
    pub notification_cooldown_secs: u64,

    /// 告警分组窗口（秒）。🔄 热加载
    #[serde(default = "AlertConfig::default_grouping_window_secs")]
    pub grouping_window_secs: u64,

    /// 通知配置。
    #[serde(default)]
    pub notification: AlertNotificationConfig,

    /// 模板配置。
    #[serde(default)]
    pub templates: AlertTemplatesConfig,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            evaluation_interval_secs: 30,
            max_concurrent_evaluations: 16,
            max_rules: 1_000,
            max_active_alerts: 5_000,
            auto_resolve_timeout_secs: 300,
            notification_cooldown_secs: 300,
            grouping_window_secs: 10,
            notification: AlertNotificationConfig::default(),
            templates: AlertTemplatesConfig::default(),
        }
    }
}

impl AlertConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_evaluation_interval_secs() -> u64 {
        30
    }
    const fn default_max_concurrent_evaluations() -> usize {
        16
    }
    const fn default_max_rules() -> usize {
        1_000
    }
    const fn default_max_active_alerts() -> usize {
        5_000
    }
    const fn default_auto_resolve_timeout_secs() -> u64 {
        300
    }
    const fn default_notification_cooldown_secs() -> u64 {
        300
    }
    const fn default_grouping_window_secs() -> u64 {
        10
    }
}

/// 告警通知配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertNotificationConfig {
    /// 并行通知数上限。🔄 热加载
    #[serde(default = "AlertNotificationConfig::default_max_concurrent")]
    pub max_concurrent: usize,

    /// 通知重试次数。🔄 热加载
    #[serde(default = "AlertNotificationConfig::default_retry_max")]
    pub retry_max: u32,

    /// 通知重试退避（毫秒）。🔄 热加载
    #[serde(default = "AlertNotificationConfig::default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,

    /// 通知通道配置。
    #[serde(default)]
    pub channels: AlertChannelsConfig,
}

impl Default for AlertNotificationConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 8,
            retry_max: 3,
            retry_backoff_ms: 5_000,
            channels: AlertChannelsConfig::default(),
        }
    }
}

impl AlertNotificationConfig {
    const fn default_max_concurrent() -> usize {
        8
    }
    const fn default_retry_max() -> u32 {
        3
    }
    const fn default_retry_backoff_ms() -> u64 {
        5_000
    }
}

/// 告警通知通道配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertChannelsConfig {
    /// Webhook 通道配置。
    #[serde(default)]
    pub webhook: AlertWebhookConfig,

    /// 邮件通道配置。
    #[serde(default)]
    pub email: AlertEmailConfig,

    /// Slack 通道配置。
    #[serde(default)]
    pub slack: AlertSlackConfig,
}

impl Default for AlertChannelsConfig {
    fn default() -> Self {
        Self {
            webhook: AlertWebhookConfig::default(),
            email: AlertEmailConfig::default(),
            slack: AlertSlackConfig::default(),
        }
    }
}

/// Webhook 通知通道配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertWebhookConfig {
    /// Webhook 通道开关。🔄 热加载
    #[serde(default = "AlertWebhookConfig::default_enabled")]
    pub enabled: bool,

    /// 默认 Webhook URL。🔄 热加载
    #[serde(default = "AlertWebhookConfig::default_url")]
    pub default_url: String,

    /// Webhook 超时（秒）。🔄 热加载
    #[serde(default = "AlertWebhookConfig::default_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for AlertWebhookConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_url: "http://localhost:8080/alerts".into(),
            timeout_secs: 10,
        }
    }
}

impl AlertWebhookConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_url() -> String {
        "http://localhost:8080/alerts".into()
    }
    const fn default_timeout_secs() -> u64 {
        10
    }
}

/// 邮件通知通道配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertEmailConfig {
    /// 邮件通道开关（默认关闭）。🔄 热加载
    #[serde(default = "AlertEmailConfig::default_enabled")]
    pub enabled: bool,

    /// SMTP 服务器。🔄 热加载
    #[serde(default = "AlertEmailConfig::default_smtp_host")]
    pub smtp_host: String,

    /// SMTP 端口。🔄 热加载
    #[serde(default = "AlertEmailConfig::default_smtp_port")]
    pub smtp_port: u16,

    /// SMTP TLS 开关。🔄 热加载
    #[serde(default = "AlertEmailConfig::default_smtp_use_tls")]
    pub smtp_use_tls: bool,
}

impl Default for AlertEmailConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            smtp_host: "smtp.example.com".into(),
            smtp_port: 587,
            smtp_use_tls: true,
        }
    }
}

impl AlertEmailConfig {
    fn default_enabled() -> bool {
        false
    }
    fn default_smtp_host() -> String {
        "smtp.example.com".into()
    }
    const fn default_smtp_port() -> u16 {
        587
    }
    fn default_smtp_use_tls() -> bool {
        true
    }
}

/// Slack 通知通道配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertSlackConfig {
    /// Slack 通道开关（默认关闭）。🔄 热加载
    #[serde(default = "AlertSlackConfig::default_enabled")]
    pub enabled: bool,
}

impl Default for AlertSlackConfig {
    fn default() -> Self {
        Self { enabled: false }
    }
}

impl AlertSlackConfig {
    fn default_enabled() -> bool {
        false
    }
}

/// 告警模板配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertTemplatesConfig {
    /// 最大模板数。🔄 热加载
    #[serde(default = "AlertTemplatesConfig::default_max_templates")]
    pub max_templates: usize,

    /// 默认模板主题。🔄 热加载
    #[serde(default = "AlertTemplatesConfig::default_theme")]
    pub default_theme: String,
}

impl Default for AlertTemplatesConfig {
    fn default() -> Self {
        Self {
            max_templates: 100,
            default_theme: "standard".into(),
        }
    }
}

impl AlertTemplatesConfig {
    const fn default_max_templates() -> usize {
        100
    }
    fn default_theme() -> String {
        "standard".into()
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Wave 5 新增配置 section
// ══════════════════════════════════════════════════════════════════════════════

// ─── Forecast 配置 ───

/// 预测引擎配置（DLinear ONNX 推理）。
///
/// 预测引擎加载 Python 训练的 DLinear ONNX 模型，对时序数据进行
/// 多步前向预测。支持批量推理、并发会话池和推理失败降级策略。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForecastConfig {
    /// 预测引擎总开关。🔄 热加载
    #[serde(default = "ForecastConfig::default_enabled")]
    pub enabled: bool,

    /// ONNX 模型目录路径（裸名 `lstm.onnx`/`dlinear.onnx` 作全局模型）。🔁 需重启
    #[serde(default = "ForecastConfig::default_model_path")]
    pub model_path: String,

    /// 预测步长（点数）。🔄 热加载
    #[serde(default = "ForecastConfig::default_horizon")]
    pub horizon: usize,

    /// 批量推理大小。🔄 热加载
    #[serde(default = "ForecastConfig::default_batch_size")]
    pub batch_size: usize,

    /// 最大上下文窗口（输入序列长度）。🔄 热加载
    #[serde(default = "ForecastConfig::default_max_context_window")]
    pub max_context_window: usize,

    /// 单次推理超时（毫秒）。🔄 热加载
    #[serde(default = "ForecastConfig::default_inference_timeout_ms")]
    pub inference_timeout_ms: u64,

    /// 并发 ONNX 会话数。🔁 需重启
    #[serde(default = "ForecastConfig::default_max_concurrent_sessions")]
    pub max_concurrent_sessions: usize,

    /// 推理失败降级策略。🔄 热加载
    #[serde(default = "ForecastConfig::default_fallback_strategy")]
    pub fallback_strategy: String,
}

impl Default for ForecastConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            model_path: "models/".into(),
            horizon: 96,
            batch_size: 64,
            max_context_window: 672,
            inference_timeout_ms: 5_000,
            max_concurrent_sessions: 4,
            fallback_strategy: "last_value".into(),
        }
    }
}

impl ForecastConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_model_path() -> String {
        "models/".into()
    }
    const fn default_horizon() -> usize {
        96
    }
    const fn default_batch_size() -> usize {
        64
    }
    const fn default_max_context_window() -> usize {
        672
    }
    const fn default_inference_timeout_ms() -> u64 {
        5_000
    }
    const fn default_max_concurrent_sessions() -> usize {
        4
    }
    fn default_fallback_strategy() -> String {
        "last_value".into()
    }
}

// ─── Condition 配置 ───

/// 条件评估引擎配置。
///
/// 条件评估引擎实时评估用户定义的条件表达式，当条件满足时触发关联动作。
/// 支持去抖、死区和表达式超时保护。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConditionConfig {
    /// 条件评估引擎总开关。🔄 热加载
    #[serde(default = "ConditionConfig::default_enabled")]
    pub enabled: bool,

    /// 最大条件数。🔄 热加载
    #[serde(default = "ConditionConfig::default_max_conditions")]
    pub max_conditions: usize,

    /// 条件评估间隔（毫秒）。🔄 热加载
    #[serde(default = "ConditionConfig::default_evaluation_interval_ms")]
    pub evaluation_interval_ms: u64,

    /// 并行评估上限。🔄 热加载
    #[serde(default = "ConditionConfig::default_max_concurrent_evaluations")]
    pub max_concurrent_evaluations: usize,

    /// 条件去抖间隔（毫秒）。🔄 热加载
    #[serde(default = "ConditionConfig::default_deadband_ms")]
    pub deadband_ms: u64,

    /// 单条件表达式求值超时（毫秒）。🔄 热加载
    #[serde(default = "ConditionConfig::default_expression_timeout_ms")]
    pub expression_timeout_ms: u64,

    /// 表达式 AST 最大深度。🔄 热加载
    #[serde(default = "ConditionConfig::default_max_expression_depth")]
    pub max_expression_depth: usize,

    /// 状态缓存上限。🔄 热加载
    #[serde(default = "ConditionConfig::default_state_cache_max_entries")]
    pub state_cache_max_entries: usize,
}

impl Default for ConditionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_conditions: 10_000,
            evaluation_interval_ms: 1_000,
            max_concurrent_evaluations: 16,
            deadband_ms: 100,
            expression_timeout_ms: 50,
            max_expression_depth: 16,
            state_cache_max_entries: 50_000,
        }
    }
}

impl ConditionConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_max_conditions() -> usize {
        10_000
    }
    const fn default_evaluation_interval_ms() -> u64 {
        1_000
    }
    const fn default_max_concurrent_evaluations() -> usize {
        16
    }
    const fn default_deadband_ms() -> u64 {
        100
    }
    const fn default_expression_timeout_ms() -> u64 {
        50
    }
    const fn default_max_expression_depth() -> usize {
        16
    }
    const fn default_state_cache_max_entries() -> usize {
        50_000
    }
}

// ─── Anomaly 配置 ───

/// 异常检测引擎配置（IsolationForest ONNX 推理）。
///
/// 异常检测引擎使用孤立森林模型对时序数据点进行异常评分，
/// 低于阈值的点被标记为异常。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnomalyConfig {
    /// 异常检测引擎总开关。🔄 热加载
    #[serde(default = "AnomalyConfig::default_enabled")]
    pub enabled: bool,

    /// IsolationForest / TCN-AE ONNX 模型目录路径（裸名 `iforest.onnx`/`tcn_ae.onnx`）。🔁 需重启
    #[serde(default = "AnomalyConfig::default_model_path")]
    pub model_path: String,

    /// 异常分数阈值（decision_function < 0 为异常）。🔄 热加载
    #[serde(default = "AnomalyConfig::default_score_threshold")]
    pub score_threshold: f64,

    /// 预期异常比例（用于训练）。🔄 热加载
    #[serde(default = "AnomalyConfig::default_contamination")]
    pub contamination: f64,

    /// 批量推理大小。🔄 热加载
    #[serde(default = "AnomalyConfig::default_batch_size")]
    pub batch_size: usize,

    /// 单次推理超时（毫秒）。🔄 热加载
    #[serde(default = "AnomalyConfig::default_inference_timeout_ms")]
    pub inference_timeout_ms: u64,

    /// 并发 ONNX 会话数。🔁 需重启
    #[serde(default = "AnomalyConfig::default_max_concurrent_sessions")]
    pub max_concurrent_sessions: usize,

    /// 滑动窗口大小（点数）。🔄 热加载
    #[serde(default = "AnomalyConfig::default_window_size")]
    pub window_size: usize,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            model_path: "models/".into(),
            score_threshold: 0.0,
            contamination: 0.1,
            batch_size: 256,
            inference_timeout_ms: 3_000,
            max_concurrent_sessions: 2,
            window_size: 100,
        }
    }
}

impl AnomalyConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_model_path() -> String {
        "models/".into()
    }
    const fn default_score_threshold() -> f64 {
        0.0
    }
    const fn default_contamination() -> f64 {
        0.1
    }
    const fn default_batch_size() -> usize {
        256
    }
    const fn default_inference_timeout_ms() -> u64 {
        3_000
    }
    const fn default_max_concurrent_sessions() -> usize {
        2
    }
    const fn default_window_size() -> usize {
        100
    }
}

// ─── AI Ops 配置 ───

/// AI 运维引擎配置（根因分析 + 多模型投票 + 预测缓存）。
///
/// AI 运维引擎在告警触发时自动执行根因分析，通过相关性计算和多模型
/// 投票机制定位问题源头。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AiOpsConfig {
    /// AI 运维引擎总开关。🔄 热加载
    #[serde(default = "AiOpsConfig::default_enabled")]
    pub enabled: bool,

    /// 根因分析候选数上限。🔄 热加载
    #[serde(default = "AiOpsConfig::default_root_cause_max_candidates")]
    pub root_cause_max_candidates: usize,

    /// 相关性阈值。🔄 热加载
    #[serde(default = "AiOpsConfig::default_correlation_threshold")]
    pub correlation_threshold: f64,

    /// 基线窗口（小时）。🔄 热加载
    #[serde(default = "AiOpsConfig::default_baseline_window_hours")]
    pub baseline_window_hours: u64,

    /// 异常投票阈值（多模型一致判定）。🔄 热加载
    #[serde(default = "AiOpsConfig::default_anomaly_vote_threshold")]
    pub anomaly_vote_threshold: usize,

    /// 预测缓存 TTL（秒）。🔄 热加载
    #[serde(default = "AiOpsConfig::default_prediction_cache_ttl_secs")]
    pub prediction_cache_ttl_secs: u64,

    /// 并行分析上限。🔄 热加载
    #[serde(default = "AiOpsConfig::default_max_parallel_analysis")]
    pub max_parallel_analysis: usize,

    /// 单次分析超时（秒）。🔄 热加载
    #[serde(default = "AiOpsConfig::default_analysis_timeout_secs")]
    pub analysis_timeout_secs: u64,
}

impl Default for AiOpsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            root_cause_max_candidates: 20,
            correlation_threshold: 0.7,
            baseline_window_hours: 24,
            anomaly_vote_threshold: 3,
            prediction_cache_ttl_secs: 300,
            max_parallel_analysis: 4,
            analysis_timeout_secs: 30,
        }
    }
}

impl AiOpsConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_root_cause_max_candidates() -> usize {
        20
    }
    const fn default_correlation_threshold() -> f64 {
        0.7
    }
    const fn default_baseline_window_hours() -> u64 {
        24
    }
    const fn default_anomaly_vote_threshold() -> usize {
        3
    }
    const fn default_prediction_cache_ttl_secs() -> u64 {
        300
    }
    const fn default_max_parallel_analysis() -> usize {
        4
    }
    const fn default_analysis_timeout_secs() -> u64 {
        30
    }
}

// ─── Asset Model 配置 ───

/// 资产模型引擎配置（层级管理 + 健康评分 + 状态传播）。
///
/// 资产模型引擎管理设备/资产的层级拓扑关系，支持健康评分计算和
/// 状态自底向上传播（子资产故障 → 父资产降级）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AssetModelConfig {
    /// 资产模型引擎总开关。🔄 热加载
    #[serde(default = "AssetModelConfig::default_enabled")]
    pub enabled: bool,

    /// 最大资产数。🔄 热加载
    #[serde(default = "AssetModelConfig::default_max_assets")]
    pub max_assets: usize,

    /// 资产层级最大深度。🔄 热加载
    #[serde(default = "AssetModelConfig::default_max_hierarchy_depth")]
    pub max_hierarchy_depth: usize,

    /// 每资产最大标签数。🔄 热加载
    #[serde(default = "AssetModelConfig::default_max_tags_per_asset")]
    pub max_tags_per_asset: usize,

    /// 模板缓存大小。🔄 热加载
    #[serde(default = "AssetModelConfig::default_template_cache_size")]
    pub template_cache_size: usize,

    /// 状态传播超时（毫秒）。🔄 热加载
    #[serde(default = "AssetModelConfig::default_propagation_timeout_ms")]
    pub propagation_timeout_ms: u64,

    /// 健康分衰减周期（小时）。🔄 热加载
    #[serde(default = "AssetModelConfig::default_health_score_decay_hours")]
    pub health_score_decay_hours: u64,
}

impl Default for AssetModelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_assets: 100_000,
            max_hierarchy_depth: 10,
            max_tags_per_asset: 1_000,
            template_cache_size: 1_000,
            propagation_timeout_ms: 5_000,
            health_score_decay_hours: 1,
        }
    }
}

impl AssetModelConfig {
    fn default_enabled() -> bool {
        true
    }
    const fn default_max_assets() -> usize {
        100_000
    }
    const fn default_max_hierarchy_depth() -> usize {
        10
    }
    const fn default_max_tags_per_asset() -> usize {
        1_000
    }
    const fn default_template_cache_size() -> usize {
        1_000
    }
    const fn default_propagation_timeout_ms() -> u64 {
        5_000
    }
    const fn default_health_score_decay_hours() -> u64 {
        1
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Wave 6 新增配置 section
// ══════════════════════════════════════════════════════════════════════════════

// ─── Observe 配置 ───

/// 可观测性配置（Prometheus /metrics + health + slow query + tracing）。
///
/// 控制日志级别与输出、TRACE 采样率、Prometheus metrics 端点、系统指标采集、
/// 慢查询日志和健康检查端点的行为。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObserveConfig {
    /// 日志级别: trace/debug/info/warn/error。🔄 热加载
    #[serde(default = "ObserveConfig::default_log_level")]
    pub log_level: String,

    /// 日志输出目标: stdout | stderr | file。🔁 需重启
    #[serde(default = "ObserveConfig::default_log_output")]
    pub log_output: String,

    /// 日志文件路径（log_output="file" 时生效）。🔄 热加载
    #[serde(default)]
    pub log_file: String,

    /// TRACE 采样率 (1/N)，N=10000 表示 1/10000。🔄 热加载
    #[serde(default = "ObserveConfig::default_trace_sample_rate")]
    pub trace_sample_rate: u32,

    /// Prometheus /metrics 端点端口。🔁 需重启
    #[serde(default = "ObserveConfig::default_metrics_port")]
    pub metrics_port: u16,

    /// /metrics scrape HTTP 超时（秒），生产推荐 3s。🔄 热加载
    #[serde(default = "ObserveConfig::default_scrape_timeout_secs")]
    pub scrape_timeout_secs: u64,

    /// 系统指标采集间隔（秒）。🔄 热加载
    #[serde(default = "ObserveConfig::default_system_metrics_interval_secs")]
    pub system_metrics_interval_secs: u64,

    /// 慢查询日志配置。
    #[serde(default)]
    pub slow_query: SlowQueryConfig,

    /// 健康检查端点配置。
    #[serde(default)]
    pub health: HealthEndpointConfig,
}

impl Default for ObserveConfig {
    fn default() -> Self {
        Self {
            log_level: "info".into(),
            log_output: "stdout".into(),
            log_file: String::new(),
            trace_sample_rate: 10_000,
            metrics_port: 5101,
            scrape_timeout_secs: 1,
            system_metrics_interval_secs: 15,
            slow_query: SlowQueryConfig::default(),
            health: HealthEndpointConfig::default(),
        }
    }
}

impl ObserveConfig {
    fn default_log_level() -> String {
        "info".into()
    }
    fn default_log_output() -> String {
        "stdout".into()
    }
    const fn default_trace_sample_rate() -> u32 {
        10_000
    }
    const fn default_metrics_port() -> u16 {
        5101
    }
    const fn default_scrape_timeout_secs() -> u64 {
        1
    }
    const fn default_system_metrics_interval_secs() -> u64 {
        15
    }
}

/// 慢查询日志配置。
///
/// 控制慢查询日志的开关、阈值、文件存储和自动 rotate 行为。
/// 异步写入使用有界 Channel (channel_capacity)，满则丢弃并递增 dropped Counter。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SlowQueryConfig {
    /// 是否启用慢查询日志。🔄 热加载
    #[serde(default = "const_true")]
    pub enabled: bool,

    /// 慢查询阈值（毫秒）。🔄 热加载
    #[serde(default = "SlowQueryConfig::default_threshold_ms")]
    pub threshold_ms: u64,

    /// 慢查询日志文件路径。🔄 热加载（空 → `logs/slow_query.log`）。
    #[serde(default = "SlowQueryConfig::default_log_file")]
    pub log_file: String,

    /// 自动 rotate 文件大小上限（MB）。🔄 热加载
    #[serde(default = "SlowQueryConfig::default_max_file_size_mb")]
    pub max_file_size_mb: u64,

    /// rotate 保留备份文件数。🔄 热加载
    #[serde(default = "SlowQueryConfig::default_max_backup_files")]
    pub max_backup_files: u32,

    /// 异步写入 Channel 容量。🔁 需重启
    #[serde(default = "SlowQueryConfig::default_channel_capacity")]
    pub channel_capacity: usize,
}

impl Default for SlowQueryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold_ms: 1_000,
            log_file: String::new(),
            max_file_size_mb: 100,
            max_backup_files: 5,
            channel_capacity: 1_000,
        }
    }
}

impl SlowQueryConfig {
    const fn default_threshold_ms() -> u64 {
        1_000
    }
    fn default_log_file() -> String {
        String::new()
    }
    const fn default_max_file_size_mb() -> u64 {
        100
    }
    const fn default_max_backup_files() -> u32 {
        5
    }
    const fn default_channel_capacity() -> usize {
        1_000
    }
}

/// 健康检查端点配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthEndpointConfig {
    /// /ready Leader 选举最大等待时间（秒）。🔄 热加载
    #[serde(default = "HealthEndpointConfig::default_ready_leader_wait_secs")]
    pub ready_leader_wait_secs: u64,
}

impl Default for HealthEndpointConfig {
    fn default() -> Self {
        Self {
            ready_leader_wait_secs: 30,
        }
    }
}

impl HealthEndpointConfig {
    const fn default_ready_leader_wait_secs() -> u64 {
        30
    }
}

// ─── Deploy 配置 ───

/// 部署运维配置（FORMAT_VERSION + 进程锁 + 优雅关闭）。
///
/// 控制进程启动安全检查链、优雅关闭超时、systemd 通知和首次运行行为。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeployConfig {
    /// 优雅关闭最大等待时间（秒）。🔄 热加载
    #[serde(default = "DeployConfig::default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,

    /// 是否启用 systemd notify（sd_notify READY=1/STOPPING=1）。🔁 需重启
    #[serde(default = "const_true")]
    pub systemd_notify: bool,

    /// 启动配置校验模式: strict | warn。🔄 热加载
    #[serde(default = "DeployConfig::default_validation_mode")]
    pub validation_mode: String,

    /// 首次运行配置。
    #[serde(default)]
    pub first_run: FirstRunConfig,
}

impl Default for DeployConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout_secs: 30,
            systemd_notify: true,
            validation_mode: "strict".into(),
            first_run: FirstRunConfig::default(),
        }
    }
}

impl DeployConfig {
    const fn default_shutdown_timeout_secs() -> u64 {
        30
    }
    fn default_validation_mode() -> String {
        "strict".into()
    }
}

/// 首次运行初始化配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FirstRunConfig {
    /// 首次 admin 密码（空=自动生成）。🔁 需重启
    #[serde(default)]
    pub admin_password: String,

    /// 非交互模式（SENSEARCDB_NO_TTY 环境变量）。🔁 需重启
    #[serde(default)]
    pub no_tty: bool,
}

impl Default for FirstRunConfig {
    fn default() -> Self {
        Self {
            admin_password: String::new(),
            no_tty: false,
        }
    }
}

// ─── Console 配置 ───

/// 前端控制台配置（WebSocket + heartbeat，后端侧）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConsoleConfig {
    /// WebSocket 最大连接数。🔄 热加载
    #[serde(default = "ConsoleConfig::default_ws_max_connections")]
    pub ws_max_connections: u32,

    /// WS 心跳间隔（秒）。🔄 热加载
    #[serde(default = "ConsoleConfig::default_ws_heartbeat_secs")]
    pub ws_heartbeat_secs: u64,

    /// WS 心跳超时（秒）。🔄 热加载
    #[serde(default = "ConsoleConfig::default_ws_heartbeat_timeout_secs")]
    pub ws_heartbeat_timeout_secs: u64,
}

impl Default for ConsoleConfig {
    fn default() -> Self {
        Self {
            ws_max_connections: 100,
            ws_heartbeat_secs: 30,
            ws_heartbeat_timeout_secs: 90,
        }
    }
}

impl ConsoleConfig {
    const fn default_ws_max_connections() -> u32 {
        100
    }
    const fn default_ws_heartbeat_secs() -> u64 {
        30
    }
    const fn default_ws_heartbeat_timeout_secs() -> u64 {
        90
    }
}

// ─── 租户生命周期配置（S4） ───

/// 租户生命周期配置（CRUD / 两段式删除冷静期）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TenantConfig {
    /// 两段式删除冷静期（天）：`SUSPENDED → DELETING` 后到物理清除前的冷却时长。
    /// 冷静期内 slug 保留、不可被新租户抢占。🔄 热加载
    #[serde(default = "TenantConfig::default_deletion_cooling_period_days")]
    pub deletion_cooling_period_days: u32,

    /// 后台删除 sweeper 扫描间隔（秒）：扫描 `DELETING` 且 `purge_at <= now` 的租户，
    /// 触发 `finalize_deletion` 物理清除。🔁 启动时读取（重启生效）。
    #[serde(default = "TenantConfig::default_deletion_sweep_interval_secs")]
    pub deletion_sweep_interval_secs: u64,
}

impl Default for TenantConfig {
    fn default() -> Self {
        Self {
            deletion_cooling_period_days: 7,
            deletion_sweep_interval_secs: 3600,
        }
    }
}

impl TenantConfig {
    const fn default_deletion_cooling_period_days() -> u32 {
        7
    }

    const fn default_deletion_sweep_interval_secs() -> u64 {
        3600
    }
}

// ─── 辅助函数 ───

/// serde default 辅助：bool true
const fn const_true() -> bool {
    true
}

mod tests {
    use super::*;

    #[test]
    fn test_default_app_config() {
        let cfg = AppConfig::default();
        assert_eq!(cfg.engine.freeze_threshold, 460_000);
        assert_eq!(cfg.engine.compaction_poll_interval_secs, 8);
        assert_eq!(cfg.engine.wal_group_commit_max_bytes, 1_048_576);
        assert_eq!(cfg.snapshot.shard_multiplier, 8);
        assert_eq!(cfg.snapshot.f64_epsilon, 1e-12);
        assert_eq!(cfg.snapshot.replay.max_sessions, 5);
        assert_eq!(cfg.paths.data_dir, "data");
    }

    #[test]
    fn test_config_handle_get() {
        let cfg = AppConfig::default();
        let handle = ConfigHandle::new(cfg);
        let guard = handle.get();
        assert_eq!(guard.engine.freeze_threshold, 460_000);
    }

    #[test]
    fn test_engine_config_defaults() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.freeze_threshold, 460_000);
        assert_eq!(cfg.compaction_poll_interval_secs, 8);
        assert_eq!(cfg.compaction_max_wait_secs, 3600);
        assert_eq!(cfg.compaction_idle_threshold, 10);
        assert_eq!(cfg.compaction_idle_pause_secs, 300);
        assert_eq!(cfg.compaction_default_file_count, 16);
        assert_eq!(cfg.compaction_default_size_gb, 1);
        assert_eq!(cfg.wal_group_commit_max_bytes, 1_048_576);
        assert_eq!(cfg.wal_group_commit_max_wait_us, 200);
        assert_eq!(cfg.wal_group_commit_max_interval_secs, 1);
        assert_eq!(cfg.wal_flush_watermark_advance_interval_secs, 1);
    }

    #[test]
    fn test_snapshot_config_defaults() {
        let cfg = SnapshotConfig::default();
        assert_eq!(cfg.shard_multiplier, 8);
        assert_eq!(cfg.f64_epsilon, 1e-12);
        assert_eq!(cfg.snapshot_interval_secs, 5);
        assert_eq!(cfg.sse_heartbeat_secs, 30);
        assert_eq!(cfg.max_batch_query_tags, 200_000);
        assert_eq!(cfg.flush_interval_ms, 1000);
        assert_eq!(cfg.flush_batch_size, 8192);
        assert_eq!(cfg.flush_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn test_replay_config_defaults() {
        let cfg = ReplayConfig::default();
        assert_eq!(cfg.max_sessions, 5);
        assert_eq!(cfg.max_entries_per_session, 500_000);
        assert_eq!(cfg.event_buffer_size, 2000);
        assert_eq!(cfg.session_idle_cleanup_secs, 300);
    }

    #[test]
    fn test_paths_config_defaults() {
        let cfg = PathsConfig::default();
        assert_eq!(cfg.data_dir, "data");
        assert_eq!(cfg.sac_dir_name, "sac");
        assert_eq!(cfg.wal_dir_name, "wal");
        assert_eq!(cfg.snapshot_dir_name, "snapshot");
        assert_eq!(cfg.snapshot_file_name, "snapshot.bin");
        assert_eq!(cfg.metadata_dir_name, "metadata");
        assert_eq!(cfg.config_db_name, "config.db");
        assert_eq!(cfg.audit_dir_name, "audit");
        assert_eq!(cfg.backup_dir_name, "backup");
        assert_eq!(cfg.keys_dir_name, "keys");
        assert_eq!(cfg.ai_ops_registry_db_name, "ai_ops_registry.db");
        assert_eq!(cfg.ai_ops_blob_dir_name, "ai_ops_blobs");
        // 派生 getter 均以 data_dir 为唯一根。
        assert_eq!(cfg.sac_dir(), std::path::PathBuf::from("data/sac"));
        assert_eq!(cfg.wal_dir(), std::path::PathBuf::from("data/wal"));
        assert_eq!(
            cfg.snapshot_file(),
            std::path::PathBuf::from("data/snapshot/snapshot.bin")
        );
        assert_eq!(
            cfg.config_db_path(),
            std::path::PathBuf::from("data/metadata/config.db")
        );
        assert_eq!(
            cfg.metadata_db_path("asset.db"),
            std::path::PathBuf::from("data/metadata/asset.db")
        );
    }

    // ─── Wave 2 新增默认值测试 ───

    #[test]
    fn test_collector_config_defaults() {
        let cfg = CollectorConfig::default();
        assert_eq!(cfg.mode, "embedded");
        assert_eq!(cfg.collector_id, "collector-01");
        assert_eq!(cfg.points_per_sec, 100_000);
        assert_eq!(cfg.channel.capacity, 10_000);
        assert_eq!(cfg.store_and_forward.enabled, true);
        assert_eq!(cfg.store_and_forward.max_buffer_bytes, 536_870_912);
        assert_eq!(cfg.reconnect.initial_backoff_ms, 1_000);
        assert_eq!(cfg.reconnect.max_backoff_ms, 30_000);
        assert_eq!(cfg.opcua.session_max_backoff_ms, 60_000);
        assert_eq!(cfg.opcua.max_concurrent_reconnects, 5);
        assert_eq!(cfg.opcua.global_backoff.threshold_ratio, 0.8);
        assert_eq!(cfg.opcua.global_backoff.duration_secs, 30);
    }

    #[test]
    fn test_partition_config_defaults() {
        let cfg = PartitionConfig::default();
        assert_eq!(cfg.duration_ms, 86_400_000);
        assert_eq!(cfg.timezone_offset_secs, 28_800);
        assert_eq!(cfg.check_interval_ms, 60_000);
        assert_eq!(cfg.closing_timeout_ms, 30_000);
        assert_eq!(cfg.max_freezes_per_partition, 1_000);
        assert_eq!(cfg.late_write_grace_secs, 14_400);
        assert_eq!(cfg.max_late_window_secs, 604_800);
        assert_eq!(cfg.compaction.max_concurrent_l2, 2);
        assert_eq!(cfg.compaction.l2_retry_max, 3);
    }

    #[test]
    fn test_retention_config_defaults() {
        let cfg = RetentionConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.default_retention, "90d");
        assert_eq!(cfg.ttl_grace_period, "24h");
        assert_eq!(cfg.scan_interval_secs, 3_600);
    }

    #[test]
    fn test_tiering_config_defaults() {
        let cfg = TieringConfig::default();
        assert_eq!(cfg.enabled, false);
        assert_eq!(cfg.scan_interval_secs, 3_600);
        assert_eq!(cfg.hot.retention, "720h");
        assert_eq!(cfg.cold.enabled, false);
        assert_eq!(cfg.cold.region, "ap-east-1");
    }

    #[test]
    fn test_backup_config_defaults() {
        let cfg = BackupConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.full.enabled, true);
        assert_eq!(cfg.full.schedule, "0 2 * * *");
        assert_eq!(cfg.full.retention_count, 7);
        assert_eq!(cfg.incremental.schedule, "0 */6 * * *");
        assert_eq!(cfg.incremental.base_on, "latest_full");
        assert_eq!(cfg.wal_archiving.compress, "lz4");
        assert_eq!(cfg.wal_archiving.max_local_pending, 64);
    }

    #[test]
    fn test_restore_config_defaults() {
        let cfg = RestoreConfig::default();
        assert_eq!(cfg.max_parallel_downloads, 8);
        assert_eq!(cfg.verify_checksums, true);
    }

    #[test]
    fn test_wave2_appconfig_has_all_sections() {
        let cfg = AppConfig::default();
        // Wave 2 sections accessible
        let _ = &cfg.collector;
        let _ = &cfg.partition;
        let _ = &cfg.retention;
        let _ = &cfg.tiering;
        let _ = &cfg.backup;
        let _ = &cfg.restore;
    }

    #[test]
    fn test_wave3_appconfig_has_all_sections() {
        let cfg = AppConfig::default();
        // Wave 3 sections accessible
        let _ = &cfg.cq;
        let _ = &cfg.stream;
        let _ = &cfg.derived;
        let _ = &cfg.security;
    }

    #[test]
    fn test_cq_config_defaults() {
        let cfg = CqConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.tick_interval_ms, 1_000);
        assert_eq!(cfg.max_concurrency, 4);
        assert_eq!(cfg.window_batch_size, 32);
        assert_eq!(cfg.flush_interval_secs, 300);
        assert_eq!(cfg.autorouter_max_points, 10_000);
        assert_eq!(cfg.staleness_window_ms, 86_400_000);
        assert_eq!(cfg.max_window_states, 100_000);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.retry_delay_ms, 1_000);
    }

    #[test]
    fn test_stream_config_defaults() {
        let cfg = StreamConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.channel_capacity, 10_000);
        assert_eq!(cfg.channel_full_mode, "DropOldest");
        assert_eq!(cfg.max_concurrency, 4);
        assert_eq!(cfg.session_default_gap_ms, 30_000);
        assert_eq!(cfg.session_max_check_interval_ms, 60_000);
        assert_eq!(cfg.session_min_check_interval_ms, 1_000);
        assert_eq!(cfg.max_active_buckets, 10_000);
        assert_eq!(cfg.max_window_interval_ms, 86_400_000);
        assert_eq!(cfg.max_partitions_per_task, 10_000);
    }

    #[test]
    fn test_derived_config_defaults() {
        let cfg = DerivedConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.debounce_interval_ms, 100);
        assert_eq!(cfg.max_recalc_per_tick, 500);
        assert_eq!(cfg.max_ast_depth, 32);
        assert_eq!(cfg.max_graph_nodes, 10_000);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.calc_timeout_ms, 100);
        assert_eq!(cfg.max_stale_secs, 300);
    }

    #[test]
    fn test_security_config_defaults() {
        let cfg = SecurityConfig::default();
        // Compliance
        assert_eq!(cfg.compliance.profile, "standard");
        // Auth: JWT
        assert_eq!(cfg.auth.jwt.algorithm, "Ed25519");
        assert_eq!(cfg.auth.jwt.access_token_ttl_sec, 3_600);
        assert_eq!(cfg.auth.jwt.refresh_token_ttl_sec, 86_400);
        assert_eq!(cfg.auth.jwt.key_rotation_interval_days, 30);
        // Auth: Password
        assert_eq!(cfg.auth.password.min_length, 8);
        assert_eq!(cfg.auth.password.require_upper, true);
        assert_eq!(cfg.auth.password.max_login_attempts, 5);
        assert_eq!(cfg.auth.password.lockout_duration_sec, 900);
        // Auth: API Key
        assert_eq!(cfg.auth.api_key.default_ttl_days, 90);
        assert_eq!(cfg.auth.api_key.prefix, "sak_");
        assert_eq!(cfg.auth.api_key.hash_algorithm, "sha256");
        // Auth: Break Glass
        assert_eq!(cfg.auth.break_glass.enabled, true);
        // Auth: mTLS
        assert_eq!(cfg.auth.mtls.enabled, false);
        assert_eq!(cfg.auth.mtls.cert_algorithm, MtlsCertAlgorithm::Ed25519);
        // Authorization
        assert_eq!(cfg.authorization.default_policy, "deny");
        assert_eq!(cfg.authorization.cache.enabled, true);
        assert_eq!(cfg.authorization.cache.ttl_sec, 60);
        assert_eq!(cfg.authorization.cache.max_entries, 100_000);
        // Audit
        assert_eq!(cfg.audit.enabled, true);
        assert_eq!(cfg.audit.storage.backend, "sac");
        assert_eq!(cfg.audit.storage.max_size_gb, 100);
        assert_eq!(cfg.audit.storage.retention_days, 180);
        assert_eq!(cfg.audit.hash_chain.enabled, true);
        assert_eq!(cfg.audit.hash_chain.hash_algorithm, "SHA-256");
        // Encryption
        assert_eq!(cfg.encryption.tde.enabled, true);
        assert_eq!(cfg.encryption.tde.algorithm, "AES-256-GCM");
        assert_eq!(cfg.encryption.tde.page_size_bytes, 65_536);
        assert_eq!(cfg.encryption.tde.wal_encryption, true);
        assert_eq!(cfg.encryption.key_management.mode, "local");
        assert_eq!(
            cfg.encryption.key_management.dek_rotation.interval_hours,
            24
        );
        assert_eq!(cfg.encryption.key_management.dek_memory.mlock, true);
        assert_eq!(
            cfg.encryption.key_management.dek_memory.zeroize_on_drop,
            true
        );
        assert_eq!(cfg.encryption.hardware_acceleration.aes_ni, true);
        // Rate Limiting
        assert_eq!(cfg.rate_limiting.enabled, true);
        assert_eq!(cfg.rate_limiting.global.max_connections, 0);
        assert_eq!(cfg.rate_limiting.global.superuser_reserved_connections, 5);
        assert_eq!(cfg.rate_limiting.api_key_defaults.write_qps, 1_000);
        assert_eq!(cfg.rate_limiting.user_defaults.query_qps, 50);
        assert_eq!(cfg.rate_limiting.endpoints.auth.qps, 20);
        assert_eq!(cfg.rate_limiting.endpoints.query.qps, 1_000);
        assert_eq!(cfg.rate_limiting.audit_throttle.same_limit_interval_sec, 30);
    }

    #[test]
    fn test_wave4_appconfig_has_all_sections() {
        let cfg = AppConfig::default();
        // Wave 4 sections accessible
        let _ = &cfg.cluster;
        let _ = &cfg.shard;
        let _ = &cfg.edge;
        let _ = &cfg.event;
        let _ = &cfg.alert;
    }

    #[test]
    fn test_cluster_config_defaults() {
        let cfg = ClusterConfig::default();
        assert_eq!(cfg.enabled, false);
        assert_eq!(cfg.node_id, "node-01");
        assert_eq!(cfg.bind_addr, "0.0.0.0:5300");
        assert_eq!(cfg.advertise_addr, "127.0.0.1:5300");
        assert_eq!(cfg.data_dir, "data/cluster");
        assert!(cfg.bootstrap_members.is_empty());
        assert!(cfg.join_addr.is_none());
        assert_eq!(cfg.election_timeout_ms, 3_000);
        assert_eq!(cfg.heartbeat_interval_ms, 500);
        assert_eq!(cfg.max_log_batch_size, 1_000);
        assert_eq!(cfg.snapshot_interval_logs, 10_000);
        assert_eq!(cfg.snapshot_chunk_size, 1_048_576);
        assert_eq!(cfg.max_payload_bytes, 4_194_304);
        assert_eq!(cfg.grpc.server_addr, "0.0.0.0:5300");
        assert_eq!(cfg.grpc.max_concurrent_streams, 256);
        assert_eq!(cfg.grpc.keepalive_interval_secs, 30);
    }

    #[test]
    fn test_shard_config_defaults() {
        let cfg = ShardConfig::default();
        assert_eq!(cfg.enabled, false);
        assert_eq!(cfg.total_shards, 8);
        assert_eq!(cfg.replication_factor, 3);
        assert_eq!(cfg.placement_strategy, "hash_ring");
        assert_eq!(cfg.hash_ring_vnodes, 256);
        assert_eq!(cfg.rebalance_threshold, 0.1);
        assert_eq!(cfg.rebalance_max_concurrent, 2);
        assert_eq!(cfg.migration_timeout_secs, 3_600);
        assert_eq!(cfg.max_leader_lease_ms, 10_000);
        assert_eq!(cfg.routing_cache.enabled, true);
        assert_eq!(cfg.routing_cache.ttl_sec, 30);
        assert_eq!(cfg.routing_cache.max_entries, 100_000);
    }

    #[test]
    fn test_edge_config_defaults() {
        let cfg = EdgeConfig::default();
        assert_eq!(cfg.enabled, false);
        assert_eq!(cfg.edge_id, "edge-01");
        assert_eq!(cfg.upstream_addrs, vec!["127.0.0.1:5300"]);
        assert_eq!(cfg.sync_mode, "incremental");
        assert_eq!(cfg.sync_interval_secs, 60);
        assert_eq!(cfg.local_buffer_max_mb, 2_048);
        assert_eq!(cfg.local_retention_hours, 168);
        assert_eq!(cfg.offline_ttl_secs, 86_400);
        assert_eq!(cfg.conflict_resolution, "lww");
        assert_eq!(cfg.bandwidth.max_upload_kbps, 10_240);
        assert_eq!(cfg.bandwidth.max_download_kbps, 10_240);
        assert_eq!(cfg.bandwidth.qos_enabled, true);
    }

    #[test]
    fn test_event_config_defaults() {
        let cfg = EventConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.max_concurrent_handlers, 32);
        assert_eq!(cfg.handler_timeout_secs, 30);
        assert_eq!(cfg.queue_capacity, 10_000);
        assert_eq!(cfg.delivery_semantics, "at_least_once");
        assert_eq!(cfg.retry_max, 3);
        assert_eq!(cfg.retry_backoff_ms, 1_000);
        assert_eq!(cfg.dead_letter_enabled, true);
        assert_eq!(cfg.dead_letter_max_size, 10_000);
        assert_eq!(cfg.filters.max_rules_per_subscription, 100);
        assert_eq!(cfg.filters.evaluation_timeout_ms, 10);
    }

    #[test]
    fn test_alert_config_defaults() {
        let cfg = AlertConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.evaluation_interval_secs, 30);
        assert_eq!(cfg.max_concurrent_evaluations, 16);
        assert_eq!(cfg.max_rules, 1_000);
        assert_eq!(cfg.max_active_alerts, 5_000);
        assert_eq!(cfg.auto_resolve_timeout_secs, 300);
        assert_eq!(cfg.notification_cooldown_secs, 300);
        assert_eq!(cfg.grouping_window_secs, 10);
        assert_eq!(cfg.notification.max_concurrent, 8);
        assert_eq!(cfg.notification.retry_max, 3);
        assert_eq!(cfg.notification.retry_backoff_ms, 5_000);
        assert_eq!(cfg.notification.channels.webhook.enabled, true);
        assert_eq!(cfg.notification.channels.email.enabled, false);
        assert_eq!(cfg.notification.channels.slack.enabled, false);
        assert_eq!(cfg.templates.max_templates, 100);
        assert_eq!(cfg.templates.default_theme, "standard");
    }

    #[test]
    fn test_wave5_appconfig_has_all_sections() {
        let cfg = AppConfig::default();
        let _ = &cfg.forecast;
        let _ = &cfg.condition;
        let _ = &cfg.anomaly;
        let _ = &cfg.ai_ops;
        let _ = &cfg.asset_model;
    }

    #[test]
    fn test_forecast_config_defaults() {
        let cfg = ForecastConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.model_path, "models/");
        assert_eq!(cfg.horizon, 96);
        assert_eq!(cfg.batch_size, 64);
        assert_eq!(cfg.max_context_window, 672);
        assert_eq!(cfg.inference_timeout_ms, 5_000);
        assert_eq!(cfg.max_concurrent_sessions, 4);
        assert_eq!(cfg.fallback_strategy, "last_value");
    }

    #[test]
    fn test_condition_config_defaults() {
        let cfg = ConditionConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.max_conditions, 10_000);
        assert_eq!(cfg.evaluation_interval_ms, 1_000);
        assert_eq!(cfg.max_concurrent_evaluations, 16);
        assert_eq!(cfg.deadband_ms, 100);
        assert_eq!(cfg.expression_timeout_ms, 50);
        assert_eq!(cfg.max_expression_depth, 16);
        assert_eq!(cfg.state_cache_max_entries, 50_000);
    }

    #[test]
    fn test_anomaly_config_defaults() {
        let cfg = AnomalyConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.model_path, "models/");
        assert_eq!(cfg.score_threshold, 0.0);
        assert_eq!(cfg.contamination, 0.1);
        assert_eq!(cfg.batch_size, 256);
        assert_eq!(cfg.inference_timeout_ms, 3_000);
        assert_eq!(cfg.max_concurrent_sessions, 2);
        assert_eq!(cfg.window_size, 100);
    }

    #[test]
    fn test_ai_ops_config_defaults() {
        let cfg = AiOpsConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.root_cause_max_candidates, 20);
        assert_eq!(cfg.correlation_threshold, 0.7);
        assert_eq!(cfg.baseline_window_hours, 24);
        assert_eq!(cfg.anomaly_vote_threshold, 3);
        assert_eq!(cfg.prediction_cache_ttl_secs, 300);
        assert_eq!(cfg.max_parallel_analysis, 4);
        assert_eq!(cfg.analysis_timeout_secs, 30);
    }

    #[test]
    fn test_asset_model_config_defaults() {
        let cfg = AssetModelConfig::default();
        assert_eq!(cfg.enabled, true);
        assert_eq!(cfg.max_assets, 100_000);
        assert_eq!(cfg.max_hierarchy_depth, 10);
        assert_eq!(cfg.max_tags_per_asset, 1_000);
        assert_eq!(cfg.template_cache_size, 1_000);
        assert_eq!(cfg.propagation_timeout_ms, 5_000);
        assert_eq!(cfg.health_score_decay_hours, 1);
    }
}

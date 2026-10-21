//! 存储引擎顶层 API。
//!
//! `Engine` 是 SenseArcDB 存储引擎的唯一对外入口，封装了 WAL 写入、
//! MemTable 管理、Compaction 调度和查询执行。
//!
//! `Engine::new` 负责组装 WAL → MemTable 关联，并启动 FlushCoordinator、
//! Compaction 与 Group Commit 三个后台任务。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use sensearcdb_common::{write_attr, ConfigHandle, Durability, SenseArcError, TenantId};

use crate::compaction::scheduler::CompactionScheduler;
use crate::compression::CompressionType;
use crate::encryption::SharedEncryptor;
use crate::memtable::flush::FlushCoordinator;
use crate::memtable::manager::MemTableManager;
use crate::memtable::TypedValue;
use crate::metrics::EngineMetrics;
use crate::query::executor::QueryExecutor;
use crate::quota::{QuotaDecision, QuotaRejectReason, TenantQuotaGate};
use crate::sac::watermark::SacWatermark;
use crate::sac::writer::SacWriter;
use crate::wal::writer::WalWriter;

/// 写入请求中的一批数据点。
///
/// 每个 `DataPoint` 包含一个 table 内多个 column 在同一时间戳下的值。
#[derive(Debug, Clone)]
pub struct DataPoint {
    /// 目标表 ID。
    pub table_id: u32,
    /// 租户 ID（0 = 默认租户；S1 全链路恒 0）。
    pub tenant_id: TenantId,
    /// 源时间戳（epoch 纳秒）。
    pub ts: i64,
    /// 服务器接收时间戳（epoch 纳秒）。
    pub server_ts: i64,
    /// 各列的值（column_id → `TypedValue`，多类型保真，不再折叠为 f64）。
    pub values: Vec<(u16, TypedValue)>,
    /// 各列的质量码（column_id → u8）。
    pub qualities: Vec<(u16, u8)>,
    /// Schema 版本号（写入时的 Schema 版本，用于 WAL 重放校验）。
    pub schema_version: u32,
    /// 标记位。
    pub flags: u8,
}

/// 批量写入请求。
#[derive(Debug, Clone)]
pub struct PointBatch {
    /// 数据点列表。
    pub points: Vec<DataPoint>,
}

/// 写入结果。
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// 成功写入的数据点数。
    pub written: usize,
    /// 分配的 WAL 序列号范围起始。
    pub wal_seq_start: u64,
    /// 逐点 WAL 序列号（与写入点一一对应；空批为空）。
    ///
    /// 与 `written` 点数对齐，顺序与 `PointBatch.points` 输入顺序一致。
    /// 供下游（SnapshotStore LWW 决胜键）取用每个点精确的 write_seq。
    pub seqs: Vec<u64>,
}

/// 表级持久化强度解析器（`docs/wal-tiered-durability-design.md` §4.2）。
///
/// 写路径按 point 所属 `table_id` 解析 durability，分流到 group/durable/async 三档。
/// 契约：`None`（表不在 mirror）→ 显式返回 `Durability::Group` 安全降级，不报错。
pub trait DurabilityResolver: Send + Sync {
    /// 解析 `table_id` 的持久化强度；表不存在时返回 `Durability::Group`。
    fn resolve(&self, table_id: u32) -> Durability;
}

/// 强制冻结结果。
#[derive(Debug, Clone)]
pub struct FreezeResult {
    /// 是否发生了冻结（false 表示 active 表为空，无需冻结）。
    pub frozen: bool,
    /// 冻结时的总数据点数。
    pub total_points: usize,
}

/// 查询结果。
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Arrow RecordBatch 列表。
    pub batches: Vec<arrow::record_batch::RecordBatch>,
    /// 查询过程中的警告信息（如 Block 跳过）。
    pub warning: Option<String>,
}

/// 写入后回调：将最新值推送到下游（如 SnapshotStore）。
///
/// 引擎 crate 不依赖 snapshot crate，通过此 trait 由集成层（`src/startup.rs`）
/// 注入真实实现，实现「WAL → MemTable → SnapshotStore」端到端链路。
pub trait SnapshotUpdateSink: Send + Sync {
    /// 单点写入后回调（多列带类型）。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID（写路径显式透传，禁止丢 tenant）。
    /// - `table_id`: 目标 tag/table ID。
    /// - `values`: 多列最新值（column_id → `TypedValue`）。
    /// - `qualities`: 多列质量码（column_id → u8）。
    /// - `ts`: 源时间戳（epoch 纳秒）。
    /// - `server_ts`: 服务器接收时间戳（epoch 纳秒）。
    /// - `write_seq`: 写入序号（LWW 决胜键）。单机 = WAL 逐点 seq；集群 = Raft
    ///   index；PITR / 迁移 / 导入 = `None`。
    fn on_write(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        values: &[(u16, TypedValue)],
        qualities: &[(u16, u8)],
        ts: i64,
        server_ts: i64,
        write_seq: Option<u64>,
    );

    /// 批量写入后回调：默认逐点回退到 [`SnapshotUpdateSink::on_write`]。
    ///
    /// 实现者可按需覆盖（如 fanout 将外围 `load`/`clone` 提到循环外去重）。
    fn on_write_batch(&self, points: &[DataPoint], seqs: &[u64]) {
        for (point, seq) in points.iter().zip(seqs.iter()) {
            self.on_write(
                point.tenant_id,
                point.table_id,
                &point.values,
                &point.qualities,
                point.ts,
                point.server_ts,
                Some(*seq),
            );
        }
    }
}

/// 存储引擎顶层 API。
///
/// # 示例
///
/// ```ignore
/// let engine = Engine::new(&config_handle, None).await?; // None = TDE 关闭
/// let result = engine.write_batch(batch).await?;
/// let query = engine.query("SELECT * FROM table_42 WHERE ts BETWEEN 0 AND 1000000000").await?;
/// ```
pub struct Engine {
    /// WAL 写入器（Group Commit）。
    pub wal: Arc<WalWriter>,
    /// MemTable 管理器（active + immutable）。
    pub memtable: Arc<MemTableManager>,
    /// Compaction 调度器。
    pub compaction: Arc<CompactionScheduler>,
    /// 查询执行器。
    pub query: Arc<QueryExecutor>,
    /// Prometheus 指标。
    pub metrics: EngineMetrics,
    /// 写入后回调（SnapshotStore 桥接，启动后注入）。
    snapshot_sink: RwLock<Option<Arc<dyn SnapshotUpdateSink>>>,
    /// FlushCoordinator 句柄（阶段 7 / 6a-1 flush barrier 用；非迁移期行为不变）。
    flush_coordinator: Arc<FlushCoordinator>,
    /// SAC 文件根目录（SAC 段枚举 / 落盘用）。
    sac_dir: PathBuf,
    /// 数据面配额闸口（可选，None = 不启用配额，N=1 兼容 + 测试便捷）。
    quota_gate: RwLock<Option<Arc<TenantQuotaGate>>>,
    /// 表级持久化强度解析器（可选，None = 全表按 `Group` 安全降级）。
    ///
    /// 由集成层（`src/startup.rs`）用 `SchemaMirrorDurabilityResolver` 注入；
    /// 写路径 `write_batch` 按 point 所属 `table_id` 解析 durability 后分流。
    durability_resolver: RwLock<Option<Arc<dyn DurabilityResolver>>>,
    /// SAC 落盘水位 `S`（C2）：flush 完成后推进，供回收器（C3）与恢复层 2（C4）使用。
    sac_watermark: Arc<SacWatermark>,
    /// TDE 加密器（供 WAL 回放读取器使用；`None` = TDE 关）。
    encryptor: SharedEncryptor,
}

impl Engine {
    /// 创建新的 Engine 实例。
    ///
    /// 初始化 WAL、MemTable、Compaction 调度器和查询执行器，并启动：
    /// - Group Commit 后台循环
    /// - FlushCoordinator（MemTable → SAC 落盘）
    /// - Compaction 调度主循环
    ///
    /// 传入 `ConfigHandle` 以支持运行时热加载配置参数；`encryptor` 为可选 TDE 加密器
    /// （`None` = TDE 关闭），下发给 WalWriter / QueryExecutor / SacWriter / CompactionScheduler。
    pub async fn new(
        config: &ConfigHandle,
        encryptor: SharedEncryptor,
    ) -> Result<Self, SenseArcError> {
        let cfg = config.get();
        let sac_dir = cfg.paths.sac_dir();

        let memtable = Arc::new(MemTableManager::new(config));

        // 组装 WAL → MemTable 关联（GAP-ENG-21）
        let mut wal = WalWriter::new_with_encryptor(config, encryptor.clone())?;
        wal.set_memtable(memtable.clone());
        let wal = Arc::new(wal);

        let compaction = Arc::new(CompactionScheduler::new_with_encryptor(
            config,
            encryptor.clone(),
        ));
        let query = Arc::new(
            QueryExecutor::new_with_encryptor(memtable.clone(), sac_dir.clone(), encryptor.clone())
                .with_partition_duration(cfg.partition.duration_ms),
        );
        let metrics = EngineMetrics::new();

        // SAC 落盘水位 S（C2）：加载上一轮已持久化值（首次启动为 0）。
        let sac_watermark = Arc::new(SacWatermark::new(&sac_dir));

        // FlushCoordinator 句柄提前构造：Engine 持有 Arc 以支持 `flush_immutable_sync`
        // （flush barrier），同时 spawn 其后台排空循环。
        let flush_coordinator = Arc::new(FlushCoordinator::new_with_partition(
            Arc::new(SacWriter::new_with_encryptor(
                sac_dir.clone(),
                CompressionType::Lz4,
                encryptor.clone(),
            )),
            memtable.clone(),
            sac_watermark.clone(),
            cfg.partition.duration_ms,
            cfg.partition.timezone_offset_secs,
        ));

        let engine = Self {
            wal: wal.clone(),
            memtable: memtable.clone(),
            compaction: compaction.clone(),
            query,
            metrics,
            snapshot_sink: RwLock::new(None),
            flush_coordinator: flush_coordinator.clone(),
            sac_dir: sac_dir.clone(),
            quota_gate: RwLock::new(None),
            durability_resolver: RwLock::new(None),
            sac_watermark: sac_watermark.clone(),
            encryptor: encryptor.clone(),
        };

        // 启动 Group Commit 后台循环（durable 入队触发 + group 1MB 阈值 + 1s 兜底 fsync）
        let wal_for_loop = wal.clone();
        tokio::spawn(async move {
            wal_for_loop.group_commit_loop().await;
        });

        // 启动 FlushCoordinator（10s 轮询 immutable → SAC 落盘；含 flush barrier 唤醒通道）
        tokio::spawn(async move {
            flush_coordinator.run().await;
        });

        // 启动 Compaction 调度主循环
        let compaction_for_loop = compaction.clone();
        tokio::spawn(async move {
            compaction_for_loop.run_loop().await;
        });

        tracing::info!("Engine background tasks started (group commit / flush / compaction)");

        Ok(engine)
    }

    /// 注入写入后回调（SnapshotStore 桥接）。
    ///
    /// 由集成层在 SnapshotStore 初始化后调用。
    pub fn set_snapshot_sink(&self, sink: Arc<dyn SnapshotUpdateSink>) {
        *self.snapshot_sink.write() = Some(sink);
    }

    /// 注入数据面配额闸口（可选，None = 不启用配额）。
    ///
    /// 由集成层（S6）用 `TenantQuotaGate` 实现后注入；裁决点落在
    /// `write_batch` / `apply_batch_in_memory`，绕过 API 直写也拦。
    pub fn set_quota_gate(&self, gate: Arc<TenantQuotaGate>) {
        *self.quota_gate.write() = Some(gate);
    }

    /// 注入表级持久化强度解析器（可选，None = 全表按 `Group` 安全降级）。
    ///
    /// 由集成层（`src/startup.rs`）在 `Engine::new` 之后、开写之前注入。
    /// 不注入时 `write_batch` 对所有 point 按 `Durability::Group` 处理，
    /// 等价于既有「整批 group commit」行为，保证测试与旧调用方零改动兼容。
    pub fn set_durability_resolver(&self, resolver: Arc<dyn DurabilityResolver>) {
        *self.durability_resolver.write() = Some(resolver);
    }

    /// 数据面配额裁决（内部）：按批内首点租户裁决，超限返回统一错误。
    fn check_quota(&self, batch: &PointBatch) -> Result<(), SenseArcError> {
        let gate = match self.quota_gate.read().clone() {
            Some(g) => g,
            None => return Ok(()),
        };
        let tenant_id = batch
            .points
            .first()
            .map(|p| p.tenant_id)
            .unwrap_or(TenantId::DEFAULT);
        match gate.admit(tenant_id, batch) {
            QuotaDecision::Admit | QuotaDecision::Warn { .. } => Ok(()),
            QuotaDecision::Reject { reason } => match reason {
                QuotaRejectReason::RateLimited => {
                    tracing::warn!(
                        tenant_id = tenant_id.as_u32(),
                        "write batch rejected by data-plane rate limit"
                    );
                    Err(SenseArcError::RateLimited)
                }
                QuotaRejectReason::QuotaExceeded => {
                    tracing::warn!(
                        tenant_id = tenant_id.as_u32(),
                        "write batch rejected by data-plane quota"
                    );
                    Err(SenseArcError::QuotaExceeded(format!(
                        "tenant {} quota exceeded",
                        tenant_id.as_u32()
                    )))
                }
            },
        }
    }

    /// 批量写入数据点（表级 durability 分级分流）。
    ///
    /// 流程：背压/配额 → 按 point 所属 table 的 durability 分组 →
    /// group/durable/async 三档各自写 WAL → MemTable → SnapshotStore 回调 → ACK。
    ///
    /// 语义：**单一 LWW**——同 ts 后写覆盖前写（`ts_index.insert` 天然覆盖），
    /// 无「默认拒收」分流。
    ///
    /// # 分流规则（`docs/wal-tiered-durability-design.md` §4.2，v1.4）
    /// - `Group`（默认）：`append_frame_v2` 写 page cache + MemTable → **立即 ACK**，后台 1MB/1s 触发 fsync；
    /// - `Durable`：走 `durable_pending`，入队即唤醒 flusher，fsync 后 ACK（零丢失）；
    /// - `Async`：直接组帧写 OS 缓冲 + MemTable，不 fsync，立即 ACK。
    /// - 无 resolver / 表不在 mirror → `Group` 安全降级，不报错。
    ///
    /// 整批 ACK = 三子批 ACK 的汇合（`tokio::join!` 并发入队，等最慢子批）。
    ///
    /// # 参数
    /// - `batch`: 待写入的数据点批次（可混多表多档）。
    ///
    /// # 返回
    /// - `WriteResult`: 包含成功写入点数和 WAL 序列号。
    pub async fn write_batch(&self, batch: PointBatch) -> Result<WriteResult, SenseArcError> {
        // P2 归因埋点：write_batch 墙钟闭合校验（同步段，结束时 flush thread_local）。
        let wb_t0 = Instant::now();
        // 问题 2 三级方案 ①：拒绝式背压——写入口查 immutable 队列深度/内存超阈值，
        // 立即返回 backpressure 错误（拒绝发生在 ingress、未落 WAL，客户端可重试）。
        // ② WAL 兜底：已落 WAL 的数据不丢；此处拒绝的是「新写」，非「丢已写」。
        if self.memtable.write_stalled() {
            return Err(SenseArcError::MemTableFull);
        }

        // S4 Step 3：数据面配额裁决（WAL 前）。拒绝则该 batch 不落 WAL/不进 MemTable。
        self.check_quota(&batch)?;

        // ── 按 durability 分组（batch 可混多表多档），同时记录原批内下标以对齐 seqs ──
        // P2 归因埋点：durability 分组 + point.clone()（逐行分配）。
        let dg_t0 = Instant::now();
        let resolver = self.durability_resolver.read().clone();
        let mut group_points: Vec<DataPoint> = Vec::new();
        let mut durable_points: Vec<DataPoint> = Vec::new();
        let mut async_points: Vec<DataPoint> = Vec::new();
        let mut group_indices: Vec<usize> = Vec::new();
        let mut durable_indices: Vec<usize> = Vec::new();
        let mut async_indices: Vec<usize> = Vec::new();
        for (idx, point) in batch.points.iter().enumerate() {
            let durability = match &resolver {
                Some(r) => r.resolve(point.table_id),
                None => Durability::Group,
            };
            match durability {
                Durability::Group => {
                    group_points.push(point.clone());
                    group_indices.push(idx);
                }
                Durability::Durable => {
                    durable_points.push(point.clone());
                    durable_indices.push(idx);
                }
                Durability::Async => {
                    async_points.push(point.clone());
                    async_indices.push(idx);
                }
            }
        }
        write_attr::add_work(
            write_attr::WorkSlot::DurabilityGroup,
            dg_t0.elapsed().as_nanos() as u64,
        );

        // 三子批并发入队，互不混 fsync；空子批由 write_* 入口短路返回 0。
        let (group_result, durable_result, async_result) = tokio::join!(
            self.wal.write_group(group_points),
            self.wal.write_durable(durable_points),
            self.wal.write_async(async_points),
        );
        let group_wr = group_result?;
        let durable_wr = durable_result?;
        let async_wr = async_result?;

        let written = group_wr.written + durable_wr.written + async_wr.written;
        // 整批 WAL 序列号起点 = 所有「非空子批」的最小 seq_start。
        // 空子批 write_* 入口短路返回 wal_seq_start: 0，若直接取 min 会把 0 当作
        // 真实起点（任何含空子批的 batch 恒返回 0）；因此用 `written > 0` 过滤空子批，
        // 仅对真正分配过 seq 的子批取 min（FAIL-3）。
        let wal_seq_start = [&group_wr, &durable_wr, &async_wr]
            .into_iter()
            .filter(|wr| wr.written > 0)
            .map(|wr| wr.wal_seq_start)
            .min()
            .unwrap_or(0);

        // 组装与输入 batch.points 顺序对齐的逐点 seqs（供 WriteResult.seqs 与 sink 透传）。
        let mut seqs = vec![0u64; batch.points.len()];
        for (idx, seq) in group_indices.iter().zip(group_wr.seqs.iter()) {
            seqs[*idx] = *seq;
        }
        for (idx, seq) in durable_indices.iter().zip(durable_wr.seqs.iter()) {
            seqs[*idx] = *seq;
        }
        for (idx, seq) in async_indices.iter().zip(async_wr.seqs.iter()) {
            seqs[*idx] = *seq;
        }

        // WAL + MemTable 已落地，推送到 SnapshotStore（GAP-INT-02）。
        // 多类型直通：透传完整多列 values/qualities，不再取首列折叠为 f64。
        // write_seq 透传各点精确 WAL seq（LWW 决胜键）。
        if let Some(sink) = self.snapshot_sink.read().clone() {
            // P2 归因埋点：snapshot fanout（sink.on_write 循环，含 bucket 锁等待）。
            let snap_t0 = Instant::now();
            sink.on_write_batch(&batch.points, &seqs);
            write_attr::add_work(
                write_attr::WorkSlot::SnapshotPush,
                snap_t0.elapsed().as_nanos() as u64,
            );
        }

        // 写路径接线（L3 灌数卡死修复）：active 达到 freeze_threshold 时立即冻结并
        // 唤醒 FlushCoordinator 落盘。否则 active 无限堆积，直到 `write_stalled()`
        // 的 512MiB 内存上限触发 backpressure → 永久卡死（force_freeze 此前只在
        // shutdown/bench/测试调用，正常 Ingest 期间从不触发）。
        if self.memtable.should_freeze() {
            // P2 归因埋点：force_freeze 墙钟 + 触发次数。
            let freeze_t0 = Instant::now();
            let freeze = self.force_freeze();
            if freeze.frozen {
                self.flush_coordinator.request_flush();
            }
            write_attr::add_freeze(freeze_t0.elapsed().as_nanos() as u64);
            write_attr::add_freeze_count();
        }

        self.metrics.record_write(written as u64);
        // P2 归因埋点：闭合校验（write_batch 墙钟）→ flush thread_local 到全局。
        write_attr::add_write_batch_total(wb_t0.elapsed().as_nanos() as u64);
        write_attr::add_write_batch_count();
        write_attr::flush();
        Ok(WriteResult {
            written,
            wal_seq_start,
            seqs,
        })
    }

    /// 无 WAL 的 apply 入口（供 Raft 状态机重放调用，P-3）。
    ///
    /// Raft 日志已持久化，apply 是「重放」，只落 MemTable + SnapshotStore，
    /// **绝不写 WAL**（避免重复写 WAL 破坏「WAL = Raft Log 单一恢复路径」）。
    ///
    /// # 参数
    /// - `batch`: 已从 Raft 日志反序列化的数据点批次。
    /// - `write_seq`: 写入序号（LWW 决胜键）。集群 Raft apply 传 `Some(raft_index)`；
    ///   PITR 回放 / 分片导入无序号来源传 `None`。
    ///
    /// # 返回
    /// 成功落地的数据点数。
    pub fn apply_batch_in_memory(
        &self,
        batch: PointBatch,
        write_seq: Option<u64>,
    ) -> Result<usize, SenseArcError> {
        // P6 背压：Raft 重放路径（同步）超阈值时拒绝新写，防异常场景 OOM。
        if self.memtable.write_stalled() {
            return Err(SenseArcError::MemTableFull);
        }

        // S4 Step 3：数据面配额裁决（MemTable 前）。拒绝则该 batch 不落地。
        self.check_quota(&batch)?;

        // 1. MemTable 批量插入（与单机重放路径一致的确定性落地；无 WAL seq，
        //    传空 seqs —— 重放路径不维护 S7 的 max_wal_seq）。
        self.memtable.insert_batch(&batch.points, &[]);

        // 写路径接线：Raft 重放同样需按阈值冻结，防止 active 无限堆积触发背压死锁。
        if self.memtable.should_freeze() {
            let freeze = self.force_freeze();
            if freeze.frozen {
                self.flush_coordinator.request_flush();
            }
        }

        // 2. 推送到 SnapshotStore（实时快照，经注入的 sink 桥接）
        if let Some(sink) = self.snapshot_sink.read().clone() {
            for point in &batch.points {
                sink.on_write(
                    point.tenant_id,
                    point.table_id,
                    &point.values,
                    &point.qualities,
                    point.ts,
                    point.server_ts,
                    write_seq,
                );
            }
        }

        Ok(batch.points.len())
    }

    /// 启动恢复：回放 WAL 中 `raft_index > SAC 落盘水位 S` 的帧，重建 MemTable（历史查询缓冲）。
    ///
    /// 只插 MemTable、不写 WAL、不 fanout snapshot sink（三条硬约束）。
    /// 逐点 seq = 帧首点 seq(raft_index) + 帧内下标（alloc_seqs 连续分配）。
    /// 返回 (回放帧数, 回放点数)。
    pub fn recover_memtable_from_wal(&self) -> Result<(usize, usize), SenseArcError> {
        let after_seq = self.sac_watermark.current();
        let wal_dir = self.wal.wal_dir();
        let reader = crate::wal::WalReader::new_with_encryptor(wal_dir, self.encryptor.clone());
        let records = reader.replay_after(after_seq, u32::MAX)?;

        let mut frames = 0usize;
        let mut points = 0usize;
        for rec in &records {
            let seqs: Vec<u64> = (0..rec.points.len()).map(|i| rec.raft_index + i as u64).collect();
            self.memtable.insert_batch(&rec.points, &seqs);
            frames += 1;
            points += rec.points.len();

            // 回放 tail 可能远超 freeze_threshold，逐帧检查冻结，避免 active 无限堆积触发 write_stalled。
            if self.memtable.should_freeze() {
                let freeze = self.force_freeze();
                if freeze.frozen {
                    self.flush_coordinator.request_flush();
                }
            }
        }
        tracing::info!(frames, points, after_seq, "MemTable rebuilt from WAL replay");
        Ok((frames, points))
    }

    /// 执行 SQL 查询。
    ///
    /// 支持 SELECT ... FROM ... WHERE ts BETWEEN ... AND ...
    /// 三源合并：MemTable(active) + MemTable(frozen) + SAC Blocks。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID（三源按此过滤；SQL 文本不含 tenant，由调用方显式传入）。
    /// - `sql`: SQL 查询语句。
    ///
    /// # 返回
    /// - `QueryResult`: Arrow RecordBatch 列表。
    pub async fn query(
        &self,
        tenant_id: TenantId,
        sql: &str,
    ) -> Result<QueryResult, SenseArcError> {
        self.query.execute(tenant_id, sql).await
    }

    /// 按 table_id 查询指定时间范围（供集成层使用，绕过 SQL 解析）。
    pub async fn query_table(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        ts_start: i64,
        ts_end: i64,
    ) -> Result<QueryResult, SenseArcError> {
        self.query
            .execute_range(tenant_id, table_id, ts_start, ts_end)
            .await
    }

    /// 按 table_id + 列投影查询指定时间范围（三源合并，返回类型化逐列值 + 逐行 ts）。
    ///
    /// 与 [`query_table`](Self::query_table) 的区别：支持投影任意 `column_ids`，
    /// 返回类型化值（绕过 Arrow，保真 F32/I64/U64/I32，不做 f64 折叠）。
    /// 供 CQ 引擎读窗口内历史时间序列（多列聚合 + 层级辅助列读取）使用。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID（三源按此过滤）。
    /// - `table_id`: 目标表 ID。
    /// - `column_ids`: 需投影的列 ID 列表（非空）。
    /// - `ts_start` / `ts_end`: 时间范围（epoch 纳秒）。
    ///
    /// # 返回
    /// `(columns, timestamps)`：`columns[i]` 对应 `column_ids[i]` 的逐行值
    /// （缺失列/缺失值 → `None`），`timestamps` 为逐行纳秒时间戳。
    pub async fn query_table_columns(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        column_ids: &[u16],
        ts_start: i64,
        ts_end: i64,
    ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
        self.query
            .execute_range_columns_typed(tenant_id, table_id, column_ids, ts_start, ts_end)
            .await
    }

    /// 强制冻结当前 active MemTable。
    ///
    /// 将 active 表原子替换为空表，旧表移至 immutable 槽位供 FlushCoordinator 消费。
    ///
    /// # 返回
    /// - `FreezeResult`: 冻结状态和数据点数。
    pub fn force_freeze(&self) -> FreezeResult {
        self.memtable.force_freeze()
    }

    /// 同步 flush barrier（阶段 7 / 6a-1）：等待 FlushCoordinator 把当前 immutable
    /// 排空落 SAC 后返回。
    ///
    /// 供迁移流程在 `force_freeze` 后调用，保证「冻结瞬间的数据」已写入 SAC 文件、
    /// 才能被后续「历史 SAC 段枚举 + 传输」完整覆盖。实现基于真 oneshot 回执
    /// （FlushCoordinator 完成本轮 `pop_immutable`（drain-all）+ FlushPipeline 后回执），
    /// 禁止用「sleep 固定时长」冒充 barrier。
    ///
    /// # 返回
    /// 本轮落盘的 SAC Block 数（无 immutable 时返回 `0`）。
    pub async fn flush_immutable_sync(&self) -> Result<usize, SenseArcError> {
        self.flush_coordinator.flush_now().await
    }

    /// 背压等待（方案 C）：memtable 停滞时不立即拒绝，而是触发排空并轮询等待
    /// 恢复可写，超时返回 `false`。
    ///
    /// 排空动作 = `force_freeze`（把 active 移入 immutable）+ `request_flush`（唤醒
    /// FlushCoordinator 落盘，释放 immutable 队列深度与内存）。写路径在
    /// `write_stalled()` 时于落 WAL **之前**拒绝，因此等待成功后重试同一批是
    /// 幂等安全的（不会重复落 WAL）。
    ///
    /// # 参数
    /// - `timeout`: 最长等待时间。
    ///
    /// # 返回
    /// `true` = 超时内恢复可写；`false` = 超时（调用方应转回 RESOURCE_EXHAUSTED）。
    pub async fn wait_writable(&self, timeout: Duration) -> bool {
        const POLL_INTERVAL: Duration = Duration::from_millis(20);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if !self.memtable.write_stalled() {
                return true;
            }

            // 排空：active 冻结为 immutable（active 有数据时），并**始终**唤醒
            // FlushCoordinator 落盘（immutable 队列深度型 stall 时 active 为空，
            // force_freeze 不冻结，但仍需唤醒 flush 排空队列）。
            let _freeze = self.force_freeze();
            self.flush_coordinator.request_flush();

            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// 问题 2 三级方案 ③：FlushCoordinator 健康探针（watchdog）。
    ///
    /// 供 readiness 探针调用：FlushCoordinator 挂起（心跳超 `3 × poll_interval`）
    /// 时返回 `false`（readiness fail + critical 告警 + 由上层监督者自愈）。
    pub fn flush_coordinator_healthy(&self) -> bool {
        self.flush_coordinator.is_healthy()
    }

    /// 获取 SAC 文件根目录（SAC 段枚举 / 落盘用）。
    pub fn sac_dir(&self) -> &Path {
        &self.sac_dir
    }

    /// 获取 SAC 落盘水位 `S` 句柄（C2；供回收器 C3 与恢复层 C4 使用）。
    pub fn sac_watermark(&self) -> Arc<SacWatermark> {
        self.sac_watermark.clone()
    }

    /// 优雅关闭引擎。
    ///
    /// 依次执行：force_freeze → WAL flush → 资源释放。
    pub async fn shutdown(&self) -> Result<(), SenseArcError> {
        // 1. 冻结当前 MemTable
        self.force_freeze();

        // 2. 刷新 WAL
        self.wal.flush().await?;

        tracing::info!("Engine shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_result() {
        let wr = WriteResult {
            written: 100,
            wal_seq_start: 42,
            seqs: vec![42, 43, 44],
        };
        assert_eq!(wr.written, 100);
        assert_eq!(wr.wal_seq_start, 42);
        assert_eq!(wr.seqs, vec![42, 43, 44]);
    }

    #[test]
    fn test_freeze_result() {
        let fr = FreezeResult {
            frozen: true,
            total_points: 500_000,
        };
        assert!(fr.frozen);
        assert_eq!(fr.total_points, 500_000);
    }

    /// 阶段 7 / 6a-1：flush barrier 真排空——`force_freeze` 后 `flush_immutable_sync`
    /// 返回时，冻结的 immutable 已落 SAC（`{sac_dir}/{table_id}/` 下出现 .sac）。
    #[tokio::test]
    async fn test_flush_immutable_sync_drains_immutable_to_sac() {
        use crate::sac::SacNaming;
        use sensearcdb_common::AppConfig;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let engine = Engine::new(&handle, None).await.expect("engine");

        // 写一个点（同步落 WAL + MemTable）。
        let batch = PointBatch {
            points: vec![DataPoint {
                table_id: 42,
                tenant_id: TenantId::DEFAULT,
                ts: 1_692_180_000_000_000_000,
                server_ts: 1_692_180_000_000_000_001,
                values: vec![(0, TypedValue::F64(3.14))],
                qualities: vec![(0, 0xC0)],
                schema_version: 1,
                flags: 0,
            }],
        };
        engine.write_batch(batch).await.expect("write");

        // force_freeze：active → immutable。
        let freeze = engine.force_freeze();
        assert!(freeze.frozen, "有数据点时应发生冻结");

        // flush barrier：同步等 immutable 排空落 SAC。
        let blocks = engine.flush_immutable_sync().await.expect("flush barrier");
        assert_eq!(blocks, 1, "本轮应落 1 个 SAC Block");

        // 冻结数据已落 SAC：{sac_dir}/0/ 下至少一个 .sac 段。
        let naming = SacNaming::new(engine.sac_dir());
        let segments = naming.list_tenant_segments(0).expect("list segments");
        assert!(
            !segments.is_empty(),
            "flush barrier 后 immutable 应已落 SAC（否则漏传冻结数据）"
        );
    }

    fn point(table_id: u32, ts: i64, v: f64) -> DataPoint {
        DataPoint {
            table_id,
            tenant_id: TenantId::DEFAULT,
            ts,
            server_ts: ts + 1,
            values: vec![(0, TypedValue::F64(v))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }
    }

    /// 静态 durability 解析器：table 1 → Durable、table 2 → Async、其余 → Group。
    struct StaticResolver;

    impl DurabilityResolver for StaticResolver {
        fn resolve(&self, table_id: u32) -> Durability {
            match table_id {
                1 => Durability::Durable,
                2 => Durability::Async,
                _ => Durability::Group,
            }
        }
    }

    /// 分级分流：一个 batch 混 group/durable/async 三档，全部落库且 written 计数正确。
    #[tokio::test]
    async fn test_write_batch_routes_by_durability() {
        use std::sync::Arc;

        let engine = test_engine().await;
        engine.set_durability_resolver(Arc::new(StaticResolver));

        let batch = PointBatch {
            points: vec![
                point(1, 100, 1.0), // durable
                point(2, 200, 2.0), // async
                point(3, 300, 3.0), // group
            ],
        };
        let result = engine
            .write_batch(batch)
            .await
            .expect("mixed durability write must succeed");
        assert_eq!(result.written, 3);
    }

    /// I3：async 帧不推进 `committed_watermark`，直到被后续 durable fsync 覆盖。
    #[tokio::test]
    async fn test_async_does_not_advance_committed_watermark() {
        use std::sync::Arc;

        let engine = test_engine().await;
        engine.set_durability_resolver(Arc::new(StaticResolver));

        // async 写（table 2）：写 OS 缓冲，不 fsync。
        engine
            .write_batch(PointBatch {
                points: vec![point(2, 100, 1.0)],
            })
            .await
            .unwrap();
        assert_eq!(
            engine.wal.current_watermark(),
            0,
            "async 帧不得单独推进 committed_watermark"
        );

        // durable 写（table 1）：入队即唤醒 flusher，fsync 覆盖 async 帧。
        engine
            .write_batch(PointBatch {
                points: vec![point(1, 200, 2.0)],
            })
            .await
            .unwrap();
        assert!(
            engine.wal.current_watermark() > 0,
            "durable fsync 后 committed_watermark 应推进"
        );
    }

    /// 无 resolver（None）→ 全表按 Group 安全降级，写成功且不报错。
    #[tokio::test]
    async fn test_write_batch_without_resolver_degrades_to_group() {
        let engine = test_engine().await;
        // 不注入 resolver（保持 None）；任意 table_id（含 mirror 中不存在的表）都应安全降级 Group。
        let result = engine
            .write_batch(PointBatch {
                points: vec![point(999, 100, 1.0)],
            })
            .await
            .expect("None resolver must degrade to Group, not error");
        assert_eq!(result.written, 1);
    }

    /// FAIL-3 回归：纯 group 批（其余两档为空子批）的 `wal_seq_start` 必须返回
    /// 真实分配的最小 seq（> 0），而非被空子批的 0 哨兵污染成 0。
    #[tokio::test]
    async fn test_write_batch_pure_group_returns_nonzero_wal_seq_start() {
        let engine = test_engine().await;
        // 不注入 resolver → 三点全走 group；durable/async 子批为空。
        let result = engine
            .write_batch(PointBatch {
                points: vec![point(7, 100, 1.0), point(7, 200, 2.0), point(7, 300, 3.0)],
            })
            .await
            .expect("pure group write must succeed");
        assert_eq!(result.written, 3);
        assert!(
            result.wal_seq_start > 0,
            "纯 group 批的 wal_seq_start 应非 0，实际 {}",
            result.wal_seq_start
        );
    }

    async fn test_engine() -> Engine {
        use sensearcdb_common::AppConfig;
        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        Engine::new(&handle, None).await.expect("engine")
    }

    /// L3 灌数卡死回归：`write_batch` 在 active 达到 `freeze_threshold` 时必须立即冻结，
    /// 不能再让 active 无限堆积触发 `write_stalled()` 的 512MiB 背压死锁。
    #[tokio::test]
    async fn test_write_batch_freezes_active_at_threshold() {
        use sensearcdb_common::AppConfig;
        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.freeze_threshold = 10; // 小阈值：10 点即冻结
        let handle = ConfigHandle::new(cfg);
        let engine = Engine::new(&handle, None).await.expect("engine");

        // 一批 10 点：达到阈值 → write_batch 返回前应已完成同步 freeze（active 归零）。
        let points: Vec<DataPoint> = (0..10)
            .map(|i| point(1, i as i64 * 100, i as f64))
            .collect();
        let result = engine
            .write_batch(PointBatch { points })
            .await
            .expect("write");
        assert_eq!(result.written, 10);

        // 同步不变量：force_freeze 在 write_batch 内同步执行，返回时 active 已重置，
        // 不可能继续堆积（这是修复前卡死的根因）。
        assert_eq!(
            engine.memtable.active_snapshot().total_points(),
            0,
            "达到 freeze_threshold 后 active 应被冻结归零，不能再无限堆积"
        );
    }

    /// S4 Step 3：数据面配额裁决——绕过 API 直写 `engine.write_batch` 超限也被拦。
    #[tokio::test]
    async fn test_data_plane_quota_gate_rejects_third_point() {
        use crate::quota::{QuotaSnapshot, QuotaSource, TenantQuotaGate};
        use std::sync::Arc;

        struct StaticSource {
            snapshot: Option<QuotaSnapshot>,
        }
        impl QuotaSource for StaticSource {
            fn snapshot(&self, _tenant_id: TenantId) -> Option<QuotaSnapshot> {
                self.snapshot.clone()
            }
        }

        let engine = test_engine().await;
        let gate = Arc::new(TenantQuotaGate::new(Arc::new(StaticSource {
            snapshot: Some(QuotaSnapshot {
                points_limit: Some(2),
                ..Default::default()
            }),
        })));
        engine.set_quota_gate(gate.clone());

        // 前 2 点放行。
        engine
            .write_batch(PointBatch {
                points: vec![point(1, 100, 1.0), point(1, 200, 2.0)],
            })
            .await
            .expect("first two points admitted");

        // 第 3 点超限被拒（数据面，非 API 层）。
        let err = engine
            .write_batch(PointBatch {
                points: vec![point(1, 300, 3.0)],
            })
            .await
            .expect_err("third point must be rejected");
        assert!(
            matches!(err, SenseArcError::QuotaExceeded(_)),
            "expected QuotaExceeded, got {err:?}"
        );
        assert_eq!(gate.point_count(TenantId::DEFAULT), 2);
    }

    /// S4 Step 3 补测（无旁路）：`apply_batch_in_memory`（Raft 重放 / 分片导入 / PITR
    /// 回放的落地入口）同样受数据面配额闸口约束，不能绕过 `write_batch` 直写 MemTable。
    #[tokio::test]
    async fn test_quota_gate_applies_to_apply_batch_in_memory() {
        use crate::quota::{QuotaSnapshot, QuotaSource, TenantQuotaGate};
        use std::sync::Arc;

        struct StaticSource {
            snapshot: Option<QuotaSnapshot>,
        }
        impl QuotaSource for StaticSource {
            fn snapshot(&self, _tenant_id: TenantId) -> Option<QuotaSnapshot> {
                self.snapshot.clone()
            }
        }

        let engine = test_engine().await;
        let gate = Arc::new(TenantQuotaGate::new(Arc::new(StaticSource {
            snapshot: Some(QuotaSnapshot {
                points_limit: Some(2),
                ..Default::default()
            }),
        })));
        engine.set_quota_gate(gate.clone());

        // 单点放行（累计 1 ≤ 2）。
        engine
            .apply_batch_in_memory(
                PointBatch {
                    points: vec![point(1, 100, 1.0)],
                },
                None,
            )
            .expect("first point admitted");

        // 两点 → 累计 3 > 2 → QuotaExceeded（第 2 个落地入口同样被拦）。
        let err = engine
            .apply_batch_in_memory(
                PointBatch {
                    points: vec![point(1, 200, 2.0), point(1, 300, 3.0)],
                },
                None,
            )
            .expect_err("third point must be rejected on apply path");
        assert!(
            matches!(err, SenseArcError::QuotaExceeded(_)),
            "expected QuotaExceeded, got {err:?}"
        );
        // 被拒批次未计入计量。
        assert_eq!(gate.point_count(TenantId::DEFAULT), 1);
    }

    /// Step 4：SAC 跨文件 LWW——同 ts 落在两个 SAC 文件时，write_epoch 更大的新文件赢。
    #[tokio::test]
    async fn test_sac_cross_file_lww_newer_wins() {
        let engine = test_engine().await;

        // 第一代：ts=100 v=1.0，freeze+flush → SAC file1（write_epoch 较小）。
        engine
            .write_batch(PointBatch {
                points: vec![point(7, 100, 1.0)],
            })
            .await
            .unwrap();
        engine.force_freeze();
        engine.flush_immutable_sync().await.unwrap();

        // 第二代：ts=100 v=9.0（单一 LWW），freeze+flush → SAC file2（write_epoch 更大）。
        engine
            .write_batch(PointBatch {
                points: vec![point(7, 100, 9.0)],
            })
            .await
            .unwrap();
        engine.force_freeze();
        engine.flush_immutable_sync().await.unwrap();

        // 查询：跨文件同 ts → 新文件（write_epoch 大）应赢。
        let (columns, timestamps) = engine
            .query_table_columns(TenantId::DEFAULT, 7, &[0], 100, 100)
            .await
            .unwrap();
        assert_eq!(timestamps, vec![100]);
        assert_eq!(columns[0], vec![Some(TypedValue::F64(9.0))]);
    }

    /// Step 5 ①：拒绝式背压——immutable 队列深度超阈值时，write_batch 入口立即返回错误（非阻塞）。
    #[tokio::test]
    async fn test_write_batch_backpressure_rejects() {
        let engine = test_engine().await;

        // 制造 immutable 队列深度 65 > 硬阈值 64：写 1 点 + freeze，重复 65 次。
        for i in 0..65 {
            engine
                .write_batch(PointBatch {
                    points: vec![point(7, i * 100, i as f64)],
                })
                .await
                .unwrap();
            engine.force_freeze();
        }
        assert!(engine.memtable.write_stalled(), "队列深度 65 应触发 stall");

        // 拒绝式背压：写入口返回 Err（未落 WAL，客户端可重试）。
        let r = engine
            .write_batch(PointBatch {
                points: vec![point(7, 9999, 1.0)],
            })
            .await;
        assert!(r.is_err(), "write_stalled 时 write_batch 应拒绝（非阻塞）");

        // watchdog：FlushCoordinator 心跳新鲜 → 健康。
        assert!(engine.flush_coordinator_healthy());
    }

    /// 方案 C 背压等待：stalled 时 `wait_writable` 通过 force_freeze + request_flush
    /// 排空 immutable 队列，在超时内恢复可写（返回 true），随后写批不再被拒。
    #[tokio::test]
    async fn test_wait_writable_drains_stalled_memtable() {
        let engine = test_engine().await;

        // 制造 stall：队列深度 65 > 硬阈值 64。
        for i in 0..65 {
            engine
                .write_batch(PointBatch {
                    points: vec![point(7, i * 100, i as f64)],
                })
                .await
                .unwrap();
            engine.force_freeze();
        }
        assert!(engine.memtable.write_stalled(), "precondition: stalled");

        // 背压等待：应触发排空并在超时内返回 true。
        let recovered = engine
            .wait_writable(std::time::Duration::from_secs(30))
            .await;
        assert!(recovered, "wait_writable 应在排空后恢复可写");

        // 排空后不再 stalled，写批应成功。
        assert!(!engine.memtable.write_stalled(), "排空后应解除 stall");
        let wr = engine
            .write_batch(PointBatch {
                points: vec![point(7, 9999, 1.0)],
            })
            .await
            .expect("排空后应恢复可写");
        assert_eq!(wr.written, 1);
    }

    // ───────────── QA 独立边界（fresh eyes，不依赖工程师自测复述）─────────────

    /// QA 独立边界：`wait_writable` 在 stalled 且 `timeout = 0` 时必须立即返回 `false`，
    /// 不挂起、不提前返回 true（deadline 兜底可靠）。
    #[tokio::test]
    async fn qa_wait_writable_zero_timeout_stalled_returns_false() {
        let engine = test_engine().await;

        for i in 0..65 {
            engine
                .write_batch(PointBatch {
                    points: vec![point(7, i * 100, i as f64)],
                })
                .await
                .unwrap();
            engine.force_freeze();
        }
        assert!(engine.memtable.write_stalled(), "precondition: stalled");

        let ok = engine.wait_writable(std::time::Duration::ZERO).await;
        assert!(!ok, "timeout=0 且 stalled 应返回 false（不挂起）");
    }

    /// QA 独立边界：`wait_writable` 未 stall 时应立即返回 `true`（首轮检查命中，
    /// 不做无谓 force_freeze/request_flush）。
    #[tokio::test]
    async fn qa_wait_writable_immediate_when_not_stalled() {
        let engine = test_engine().await;
        assert!(!engine.memtable.write_stalled(), "precondition: not stalled");

        let ok = engine.wait_writable(std::time::Duration::ZERO).await;
        assert!(ok, "未 stall 应立即返回 true");
    }

    /// QA 独立核心红线（幂等依据）：`MemTableFull` 拒绝发生在落 WAL **之前**，
    /// 被拒批次不得消耗任何 WAL seq。若被拒写也分配了 seq，方案 C 的「重写同一批」
    /// 就会重复落 WAL。本测试用 seq 空洞直接验证「拒绝零消耗」。
    #[tokio::test]
    async fn qa_memtablefull_rejection_consumes_no_wal_seq() {
        let engine = test_engine().await;

        // 基线：1 点成功写，记首 seq。
        let a = engine
            .write_batch(PointBatch {
                points: vec![point(7, 1, 1.0)],
            })
            .await
            .unwrap();
        assert_eq!(a.written, 1);

        // 制造 depth stall：65 × (写 1 点 + freeze)。
        for i in 0..65 {
            engine
                .write_batch(PointBatch {
                    points: vec![point(7, 1000 + i * 100, i as f64)],
                })
                .await
                .unwrap();
            engine.force_freeze();
        }
        assert!(engine.memtable.write_stalled(), "precondition: stalled");

        // 被拒写：必须 Err(MemTableFull)，且不消耗 seq。
        let rejected = engine
            .write_batch(PointBatch {
                points: vec![point(7, 99_999, 42.0)],
            })
            .await;
        assert!(matches!(rejected, Err(SenseArcError::MemTableFull)));

        // 排空恢复。
        assert!(engine.wait_writable(std::time::Duration::from_secs(30)).await);
        assert!(!engine.memtable.write_stalled());

        // 恢复后写 1 点：其 seq = 基线 + 66（1 基线 + 65 stall；被拒 1 点不占 seq）。
        let y = engine
            .write_batch(PointBatch {
                points: vec![point(7, 88_888, 7.0)],
            })
            .await
            .unwrap();
        assert_eq!(
            y.wal_seq_start - a.wal_seq_start,
            66,
            "被拒写不得消耗 WAL seq（否则重试会重复落 WAL，破坏幂等）"
        );
    }

    /// S2 Step 2 端到端：写 tenant=0 与 tenant=1 → freeze+flush → 逐租户读回互不混入，
    /// 跨租户 table_id 查询返回空（三源 tenant 谓词在 SAC 路径上验证）。
    ///
    /// 注意：`table_id` 全局唯一（设计决策 C），故用**不同 table_id** 区分租户，
    /// 跨租户隔离体现在「tenant=0 查 tenant=1 的 table_id → 空」。
    #[tokio::test]
    async fn test_multi_tenant_query_isolation_end_to_end() {
        let engine = test_engine().await;

        fn point_tenant(table_id: u32, tenant_id: u32, ts: i64, v: f64) -> DataPoint {
            DataPoint {
                table_id,
                tenant_id: TenantId(tenant_id),
                ts,
                server_ts: ts + 1,
                values: vec![(0, TypedValue::F64(v))],
                qualities: vec![(0, 0xC0)],
                schema_version: 1,
                flags: 0,
            }
        }

        // tenant0：表 7 / 8；tenant1：表 9 / 10（table_id 全局唯一，跨租户不重叠）。
        engine
            .write_batch(PointBatch {
                points: vec![point_tenant(7, 0, 100, 1.0), point_tenant(8, 0, 200, 2.0)],
            })
            .await
            .unwrap();
        engine
            .write_batch(PointBatch {
                points: vec![point_tenant(9, 1, 300, 3.0), point_tenant(10, 1, 400, 4.0)],
            })
            .await
            .unwrap();

        engine.force_freeze();
        engine.flush_immutable_sync().await.unwrap();

        // tenant=0 读本租户表 → 命中，值正确。
        let (c0, t0) = engine
            .query_table_columns(TenantId::DEFAULT, 7, &[0], 0, 1000)
            .await
            .unwrap();
        assert_eq!(t0, vec![100]);
        assert_eq!(c0[0], vec![Some(TypedValue::F64(1.0))]);

        // tenant=1 读本租户表 → 命中，值正确。
        let (c1, t1) = engine
            .query_table_columns(TenantId(1), 9, &[0], 0, 1000)
            .await
            .unwrap();
        assert_eq!(t1, vec![300]);
        assert_eq!(c1[0], vec![Some(TypedValue::F64(3.0))]);

        // 跨租户 table_id：tenant=0 查表 9（属 tenant1）→ 空；tenant=1 查表 7（属 tenant0）→ 空。
        let (c_cross, t_cross) = engine
            .query_table_columns(TenantId::DEFAULT, 9, &[0], 0, 1000)
            .await
            .unwrap();
        assert!(t_cross.is_empty(), "tenant=0 查 tenant=1 的表应返回空");
        assert!(c_cross.is_empty() || c_cross[0].is_empty());

        let (c_cross2, t_cross2) = engine
            .query_table_columns(TenantId(1), 7, &[0], 0, 1000)
            .await
            .unwrap();
        assert!(t_cross2.is_empty(), "tenant=1 查 tenant=0 的表应返回空");
        assert!(c_cross2.is_empty() || c_cross2[0].is_empty());
    }
}

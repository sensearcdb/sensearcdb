//! Compaction 调度器：三重触发 + 空转检测 + 主循环。
//!
//! 调度器以固定间隔轮询 L0 和 L1 层级，根据三类触发条件启动合并：
//! 1. 文件数量触发
//! 2. 数据量触发
//! 3. Timeout 兜底触发
//!
//! 连续空转 N 次后进入暂停模式（`idle_pause`），避免无效 CPU 消耗。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sensearcdb_common::ConfigHandle;

use crate::compaction::level_state::{PerLevelState, SacFileMeta};
use crate::compaction::merge::{DataPoint, DedupIter, DedupIterByFileRank, KWayMerge};
use crate::compaction::strategy::CompactionStrategy;
use crate::compression::CompressionType;
use crate::encryption::SharedEncryptor;
use crate::memtable::chunk::ColumnarChunk;
use crate::memtable::col_vec::ColVec;
use crate::sac::footer::SacFooter;
use crate::sac::header::{SacHeader, SacLevel};
use crate::sac::l2_descriptor::L2Descriptor;
use crate::sac::naming::SacNaming;
use crate::sac::reader::{SacReader, SacRow, SacRowIter};
use crate::sac::writer::{SacFileBuilder, SacWriter};

/// 流式 merge 的攒块行数（与 [`SacFileBuilder::append_chunk`] 内部的 4096 行切块一致，
/// 保证每个批次恰好产出 1 个 Block，不改变 Block 边界）。
const SAC_BLOCK_ROWS: usize = 4096;

/// Compaction 目标层级对。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactionTarget {
    /// L0 → L1 合并。
    L0ToL1,
    /// L1 → L2 合并。
    L1ToL2,
    /// L2 → L3 合并（分代合并封顶层，L3 不再向上合并）。
    L2ToL3,
    /// 分区 final：把该分区内所有 L0/L1/L2/L3/旁路 late 合并为恰好 1 个 L4（ZSTD9）。
    FinalizePartition,
}

/// 一个「租户 × 分区」内的 SAC 文件集合（v9 多表聚合，按 (tenant, partition) 分组）。
#[derive(Debug, Clone)]
struct PartitionGroup {
    /// 租户 ID。
    tenant_id: u32,
    /// 分区键（数据时间边界）。
    partition_ns: i64,
    /// 该分区内的 L0 文件。
    l0: Vec<SacFileMeta>,
    /// 该分区内的 L1 文件。
    l1: Vec<SacFileMeta>,
    /// 该分区内的 L2 文件。
    l2: Vec<SacFileMeta>,
    /// 该分区内的 L3 文件（分代合并封顶层）。
    l3: Vec<SacFileMeta>,
    /// 该分区内的 L4 文件（final 最终态，一天一文件）。
    l4: Vec<SacFileMeta>,
    /// 该分区最后写入时间（epoch 纳秒），取 L0/L1 的 `max(write_epoch)`。
    ///
    /// 用于 final 的「re-final 窗口」判定（替代 wall-clock）：`0` 表示无 L0/L1
    /// 或历史文件缺 write_epoch，final 保守放行。
    last_write_wall_ns: i64,
}

impl PartitionGroup {
    /// 该分区所有 SAC 文件（用于 final 全量合并）。
    fn all_files(&self) -> Vec<&SacFileMeta> {
        self.l0
            .iter()
            .chain(self.l1.iter())
            .chain(self.l2.iter())
            .chain(self.l3.iter())
            .chain(self.l4.iter())
            .collect()
    }

    /// 是否已经是「恰好 1 个 L4、无 L0/L1/L2/L3」的最终态（final 幂等判断，I3/I4）。
    fn is_finalized(&self) -> bool {
        self.l0.is_empty()
            && self.l1.is_empty()
            && self.l2.is_empty()
            && self.l3.is_empty()
            && self.l4.len() == 1
    }
}

/// Compaction 状态。
#[derive(Debug)]
struct CompactionState {
    /// 各层级的状态（仅用于 L0→L1 触发计数；final 用 `partitions` 维度）。
    pub levels: HashMap<CompactionTarget, PerLevelState>,
    /// 按 (tenant, partition) 分组的文件清单（v9 多表聚合；I5：绝不跨 tenant/partition 合并）。
    pub partitions: HashMap<(u32, i64), PartitionGroup>,
    /// 连续空转计数。
    pub idle_count: u32,
}

impl Default for CompactionState {
    fn default() -> Self {
        let mut levels = HashMap::new();
        levels.insert(CompactionTarget::L0ToL1, PerLevelState::new());
        levels.insert(CompactionTarget::L1ToL2, PerLevelState::new());
        levels.insert(CompactionTarget::L2ToL3, PerLevelState::new());
        levels.insert(CompactionTarget::FinalizePartition, PerLevelState::new());
        Self {
            levels,
            partitions: HashMap::new(),
            idle_count: 0,
        }
    }
}

/// Compaction 调度器。
///
/// 以可配置的间隔轮询，检查各层级是否需要合并。
/// 支持热加载：每次循环通过 `config.get()` 获取最新参数。
pub struct CompactionScheduler {
    /// Compaction 状态。
    pub state: Mutex<CompactionState>,
    /// 唤醒信号（新 L0 文件写入 / 手动触发）。
    pub wakeup: tokio::sync::Notify,
    /// 轮询间隔。
    pub poll_interval: Duration,
    /// Timeout 兜底等待时间。
    pub max_wait: Duration,
    /// 空转阈值（连续 N 次无 Compaction 后进入暂停）。
    pub idle_threshold: u32,
    /// 暂停间隔。
    pub idle_pause: Duration,
    /// 合并策略。
    strategy: CompactionStrategy,
    /// 配置句柄（热加载）。
    config: ConfigHandle,
    /// SAC 文件根目录。
    sac_dir: PathBuf,
    /// 可选加密器（TDE 关闭时为 `None`）。
    encryptor: SharedEncryptor,
    /// 迁移停摆门（阶段 7 / 6a-2）：`true` = 暂停 compaction，
    /// 主循环跳过扫描/合并，保证迁移期间源段集合不被改写。
    ///
    /// 用 `Arc<AtomicBool>` 承载：`merge_paths_to_partition` 会把克隆传入
    /// `spawn_blocking`（要求 `'static`），使同步 merge 在「finish rename 之后、
    /// 删源之前」也能读到最新停摆状态（不缓存旧值）。
    paused: Arc<AtomicBool>,
    /// 分区窗口时长（毫秒）——final 触发段据此计算「天边界 + grace」到期。
    partition_duration_ms: u64,
    /// late 写入 grace 窗口（秒）。
    late_write_grace_secs: u64,
    /// 最大可回填窗口（秒）。
    max_late_window_secs: u64,
    /// 正在执行 final 的 (tenant, partition) 集合（同分区互斥）。
    ///
    /// final 与 L0→L1 同分区并发会同时删除源文件（copy-on-write 冲突），
    /// 故 final 后台任务执行期间 claim，L0→L1 检查此集合跳过同分区。
    finalizing: Mutex<HashSet<(u32, i64)>>,
    /// 正在执行 L2→L3 的 (tenant, partition) 集合（同分区互斥）。
    l2_to_l3_inflight: Mutex<HashSet<(u32, i64)>>,
}

impl CompactionScheduler {
    /// 创建新的 Compaction 调度器（TDE 关闭）。
    ///
    /// # 参数
    /// - `config`: 配置句柄（热加载）。
    pub fn new(config: &ConfigHandle) -> Self {
        Self::new_with_encryptor(config, None)
    }

    /// 创建带 TDE 加密器的 Compaction 调度器。
    ///
    /// # 参数
    /// - `config`: 配置句柄（热加载）。
    /// - `encryptor`: 可选加密器（`None` = TDE 关）。
    pub fn new_with_encryptor(config: &ConfigHandle, encryptor: SharedEncryptor) -> Self {
        let cfg = config.get();
        Self {
            state: Mutex::new(CompactionState::default()),
            wakeup: tokio::sync::Notify::new(),
            poll_interval: Duration::from_secs(cfg.engine.compaction_poll_interval_secs),
            max_wait: Duration::from_secs(cfg.engine.compaction_max_wait_secs),
            idle_threshold: cfg.engine.compaction_idle_threshold,
            idle_pause: Duration::from_secs(cfg.engine.compaction_idle_pause_secs),
            strategy: CompactionStrategy::new(config),
            config: config.clone(),
            sac_dir: cfg.paths.sac_dir(),
            encryptor,
            paused: Arc::new(AtomicBool::new(false)),
            partition_duration_ms: cfg.partition.duration_ms,
            late_write_grace_secs: cfg.partition.late_write_grace_secs,
            max_late_window_secs: cfg.partition.max_late_window_secs,
            finalizing: Mutex::new(HashSet::new()),
            l2_to_l3_inflight: Mutex::new(HashSet::new()),
        }
    }

    /// Compaction 主循环（应在 `tokio::spawn` 中运行）。
    ///
    /// 使用 `tokio::select!` 串行化 wakeup 与轮询，避免并行重复合并。
    /// 暂停（`pause()`）时跳过扫描/合并，恢复（`resume()`）后照常执行。
    pub async fn run_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(self.poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {},
                _ = self.wakeup.notified() => {},
            }

            // 阶段 7 / 6a-2：暂停时真停摆（跳过扫描 + 合并），禁止「置位不生效」。
            if self.is_paused() {
                continue;
            }

            self.run_tick().await;
        }
    }

    /// 主循环单轮（解耦版）：扫描 → L0→L1（await）→ 触发后台 final（不 await）→ 空转暂停。
    ///
    /// 与保留的 [`run_once`](Self::run_once)（同步整轮，供测试）共享扫描/触发逻辑，
    /// 唯一区别是 final 改为 `tokio::spawn` 后台执行，不阻塞主循环导致 L0 堆积。
    async fn run_tick(self: &Arc<Self>) {
        // 扫描 SAC 文件，重建 (tenant, partition) 分组文件清单。
        self.scan_files().await;

        // 双保险：执行合并前再查一次停摆门（防止扫描期间被 pause）。
        if self.is_paused() {
            return;
        }

        // L0→L1（ZSTD9，全局触发阈值 16）。
        let l0_triggered = self.check_triggers(CompactionTarget::L0ToL1);
        if l0_triggered {
            tracing::info!(target = ?CompactionTarget::L0ToL1, "compaction triggered");
            if let Err(e) = self.execute_compaction(CompactionTarget::L0ToL1).await {
                tracing::error!(target = ?CompactionTarget::L0ToL1, error = %e, "compaction failed");
            }
            self.reset_idle();
        }

        // 分代合并（刀1）：L1→L2 分区级阈值 4；L2→L3 后台 detach。
        let mut did_generational = false;
        match self.execute_l1_to_l2().await {
            Ok(n) => did_generational |= n > 0,
            Err(e) => {
                tracing::error!(target = ?CompactionTarget::L1ToL2, error = %e, "compaction failed")
            }
        }
        // L2→L3 解耦（长重活）：claim + spawn 后台重压，不 await，主循环不阻塞。
        self.schedule_l2_to_l3_detached();

        // final 解耦：若 L0→L1 或分代合并已改磁盘，先重扫再判定到期分区 → spawn 后台 merge，
        // 不 await ZSTD9 长重活，主循环立即进入下一轮扫描，避免 L0 堆积。
        self.schedule_finalization_detached(l0_triggered || did_generational)
            .await;

        // 空转暂停
        if self.should_idle_pause() {
            tracing::debug!("compaction idle, pausing");
            tokio::time::sleep(self.idle_pause).await;
            self.reset_idle();
        }
    }

    /// 单轮 compaction：扫描 SAC 文件 → 检查触发 → 执行合并 → 空转暂停。
    ///
    /// 独立成方法供单测直接驱动（避免依赖 `run_loop` 无限循环），
    /// 与 [`run_loop`](Self::run_loop) 共用同一份真实执行逻辑。
    async fn run_once(self: &Arc<Self>) {
        // 扫描 SAC 文件，重建 (tenant, table, partition) 分组文件清单。
        self.scan_files().await;

        // 双保险：执行合并前再查一次停摆门（防止扫描期间被 pause）。
        if self.is_paused() {
            return;
        }

        // L0→L1（ZSTD9，全局触发阈值 16）。
        let l0_triggered = self.check_triggers(CompactionTarget::L0ToL1);
        if l0_triggered {
            tracing::info!(target = ?CompactionTarget::L0ToL1, "compaction triggered");
            if let Err(e) = self.execute_compaction(CompactionTarget::L0ToL1).await {
                tracing::error!(target = ?CompactionTarget::L0ToL1, error = %e, "compaction failed");
            }
            self.reset_idle();
        }

        // 分代合并（刀1）：L1→L2 分区级阈值 4；L2→L3 后台 detach。
        let mut did_generational = false;
        match self.execute_l1_to_l2().await {
            Ok(n) => did_generational |= n > 0,
            Err(e) => {
                tracing::error!(target = ?CompactionTarget::L1ToL2, error = %e, "compaction failed")
            }
        }
        // L2→L3 解耦（长重活）：claim + spawn 后台重压，不 await，主循环不阻塞。
        self.schedule_l2_to_l3_detached();

        // final 触发段（定时兜底 / truth source）：扫描 grace 到期的分区 → final → 单 L4。
        // 加固 #4：L0→L1 与分代合并均未触发时磁盘视图未变，复用本轮的首次扫描，避免一轮扫多次。
        if let Err(e) = self.run_finalization(l0_triggered || did_generational).await {
            tracing::error!(error = %e, "final compaction failed");
        }

        // 空转暂停
        if self.should_idle_pause() {
            tracing::debug!("compaction idle, pausing");
            tokio::time::sleep(self.idle_pause).await;
            self.reset_idle();
        }
    }

    /// 暂停 compaction（阶段 7 / 6a-2 迁移停摆）。
    ///
    /// 真停摆：主循环在 `run_loop` / `run_once` 开头检查本门，
    /// 暂停期间不扫描、不合并、不删源文件。
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// 恢复 compaction（迁移结束 COMPLETED / rollback / forward_recover 后调用）。
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        // 恢复后立即唤醒主循环，使其尽快回到正常扫描节奏。
        self.wakeup.notify_one();
    }

    /// 是否处于暂停（停摆）状态。
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// 扫描 SAC 目录，重建按 (tenant, table, partition) 分组的文件清单。
    ///
    /// 目录遍历 + 逐文件读 footer 是同步 IO，经 `spawn_blocking` 跑在线程池，
    /// 不占用 tokio 异步 worker（加固 #3）。
    async fn scan_files(&self) {
        let sac_dir = self.sac_dir.clone();
        let groups = match tokio::task::spawn_blocking(move || scan_sac_dir(&sac_dir)).await {
            Ok(Ok(g)) => g,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "compaction file scan failed");
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "compaction file scan join failed");
                return;
            }
        };

        // 摊平 L0/L1 供 L0→L1 的全局触发计数（文件数/大小/timeout）。
        let mut l0 = Vec::new();
        let mut l1 = Vec::new();
        for g in &groups {
            l0.extend(g.l0.iter().cloned());
            l1.extend(g.l1.iter().cloned());
        }

        let mut state = self.state.lock().unwrap();
        state.partitions.clear();
        for g in groups {
            state.partitions.insert((g.tenant_id, g.partition_ns), g);
        }

        let l0_state = state
            .levels
            .entry(CompactionTarget::L0ToL1)
            .or_insert_with(PerLevelState::new);
        l0_state.files = l0;
        let l1_state = state
            .levels
            .entry(CompactionTarget::L1ToL2)
            .or_insert_with(PerLevelState::new);
        l1_state.files = l1;
    }

    /// 检查指定目标是否应触发 Compaction。
    ///
    /// OR 逻辑：文件数 / 数据量 / timeout 任一满足即返回 true。
    pub fn check_triggers(&self, target: CompactionTarget) -> bool {
        let cfg = self.config.get();
        let mut state = self.state.lock().unwrap();

        let level_state = state
            .levels
            .entry(target)
            .or_insert_with(PerLevelState::new);

        let file_count = level_state.files.len();
        // 阈值按 target 分派（刀1）：各层级独立阈值 16/4/4；final 不走此处。
        let file_count_threshold = match target {
            CompactionTarget::L0ToL1 => cfg.engine.compaction_default_file_count,
            CompactionTarget::L1ToL2 => cfg.engine.compaction_l1_file_count,
            CompactionTarget::L2ToL3 => cfg.engine.compaction_l2_file_count,
            CompactionTarget::FinalizePartition => 1,
        };

        // 触发 1: 文件数超限
        if file_count >= file_count_threshold {
            tracing::info!(
                target = ?target,
                file_count = file_count,
                threshold = file_count_threshold,
                "Compaction triggered: file count"
            );
            state.idle_count = 0;
            return true;
        }

        // 触发 2: 数据量超限
        let total_bytes: u64 = level_state.files.iter().map(|f| f.size_bytes).sum();
        let size_threshold = self.strategy.size_threshold_bytes();
        if total_bytes >= size_threshold {
            tracing::info!(
                target = ?target,
                total_bytes = total_bytes,
                threshold = size_threshold,
                "Compaction triggered: size"
            );
            state.idle_count = 0;
            return true;
        }

        // 触发 3: Timeout 兜底
        if !level_state.files.is_empty() {
            level_state.mark_ready();
            if let Some(first_ready) = level_state.first_ready_at {
                let elapsed = first_ready.elapsed();
                let max_wait = Duration::from_secs(cfg.engine.compaction_max_wait_secs);
                if elapsed >= max_wait {
                    tracing::info!(
                        target = ?target,
                        elapsed_secs = elapsed.as_secs(),
                        max_wait_secs = max_wait.as_secs(),
                        "Compaction triggered: timeout"
                    );
                    state.idle_count = 0;
                    return true;
                }
            }
        }

        // 未触发：更新空转计数
        state.idle_count += 1;
        false
    }

    /// 执行 Compaction（L0→L1 / L1→L2，均为分区内合并；I5）。
    async fn execute_compaction(
        &self,
        target: CompactionTarget,
    ) -> Result<(), sensearcdb_common::SenseArcError> {
        match target {
            CompactionTarget::L0ToL1 => self.execute_l0_to_l1().await,
            CompactionTarget::L1ToL2 => self.execute_l1_to_l2().await.map(|_| ()),
            CompactionTarget::L2ToL3 => Err(sensearcdb_common::SenseArcError::Internal(
                "L2ToL3 must be driven by schedule_l2_to_l3_detached".to_string(),
            )),
            CompactionTarget::FinalizePartition => Err(sensearcdb_common::SenseArcError::Internal(
                "FinalizePartition must be driven by run_finalization".to_string(),
            )),
        }
    }

    /// 分区内 L0→L1（ZSTD9，刀2 压缩前移）：把每个分区内的重叠小 L0 并成 1 个不重叠 L1，
    /// 输出到源分区。
    async fn execute_l0_to_l1(&self) -> Result<(), sensearcdb_common::SenseArcError> {
        let groups = self.partition_groups_snapshot();
        let mut merged_files = 0usize;

        for g in groups {
            if g.l0.is_empty() {
                continue;
            }
            // 同分区互斥：final 正在合并该分区时跳过，避免并发删源文件
            // （copy-on-write：先 finish 新 L2 再删源，但并发仍会交叉删同一批源）。
            if self.is_finalizing(g.tenant_id, g.partition_ns) {
                continue;
            }
            let paths: Vec<PathBuf> = g.l0.iter().map(|f| PathBuf::from(&f.path)).collect();
            merged_files += self
                .merge_paths_to_partition(
                    g.tenant_id,
                    g.partition_ns,
                    &paths,
                    SacLevel::L1,
                    CompressionType::Zstd9,
                )
                .await?;
        }

        tracing::info!(target = ?CompactionTarget::L0ToL1, files = merged_files, "compaction complete");
        Ok(())
    }

    /// 分区内 L1→L2（ZSTD9，block 拼接）：每分区 L1 数 ≥ 阈值（4）时合并为 1 个 L2。
    async fn execute_l1_to_l2(&self) -> Result<usize, sensearcdb_common::SenseArcError> {
        let threshold = self.strategy.l1_file_count_threshold();
        let groups = self.partition_groups_snapshot();
        let mut merged_files = 0usize;
        for g in groups {
            if g.l1.len() < threshold {
                continue;
            }
            if self.is_finalizing(g.tenant_id, g.partition_ns) {
                continue;
            }
            let paths: Vec<PathBuf> = g.l1.iter().map(|f| PathBuf::from(&f.path)).collect();
            merged_files += self
                .merge_paths_to_partition(
                    g.tenant_id,
                    g.partition_ns,
                    &paths,
                    SacLevel::L2,
                    CompressionType::Zstd9,
                )
                .await?;
        }
        if merged_files > 0 {
            tracing::info!(target = ?CompactionTarget::L1ToL2, files = merged_files, "generational compaction complete");
        }
        Ok(merged_files)
    }

    /// 分区内 L2→L3（ZSTD9，点级重压）：每分区 L2 数 ≥ 阈值（4）时 claim 后
    /// `tokio::spawn` 后台重压为 1 个 L3（封顶）。长重活不 await，避免阻塞主循环。
    fn schedule_l2_to_l3_detached(self: &Arc<Self>) {
        let threshold = self.strategy.l2_file_count_threshold();
        let groups = self.partition_groups_snapshot();
        for g in groups {
            if g.l2.len() < threshold {
                continue;
            }
            // 同分区互斥：final 正在合并该分区时跳过。
            if self.is_finalizing(g.tenant_id, g.partition_ns) {
                continue;
            }
            let key = (g.tenant_id, g.partition_ns);
            {
                let mut inflight = self.l2_to_l3_inflight.lock().unwrap();
                if !inflight.insert(key) {
                    continue;
                }
            }
            let this = self.clone();
            tokio::spawn(async move {
                let _guard = L2ToL3InflightGuard {
                    scheduler: this.clone(),
                    key,
                };
                // 迁移停摆门：spawn 后、重活前再查一次（与 detached final 同语义）。
                if this.is_paused() {
                    tracing::debug!(
                        tenant_id = g.tenant_id,
                        partition_ns = g.partition_ns,
                        "detached L2->L3 skipped: compaction paused"
                    );
                    return;
                }
                let paths: Vec<PathBuf> = g.l2.iter().map(|f| PathBuf::from(&f.path)).collect();
                let result = this
                    .merge_paths_to_partition(
                        g.tenant_id,
                        g.partition_ns,
                        &paths,
                        SacLevel::L3,
                        CompressionType::Zstd9,
                    )
                    .await;
                if let Err(e) = result {
                    tracing::error!(
                        tenant_id = g.tenant_id,
                        partition_ns = g.partition_ns,
                        error = %e,
                        "detached L2->L3 compaction failed"
                    );
                }
            });
        }
    }

    /// final 触发段（定时兜底 / truth source）：扫描 grace 到期的分区 → final → 单 L4。
    ///
    /// 判定：`now >= partition_ns + duration + late_write_grace`。分区已处于「恰好 1 个 L4」
    /// 最终态时跳过（幂等，I4）；re-final 的 `max_late_window` 硬边界改用「最后写入
    /// 新近度」（write_epoch 派生）替代 wall-clock（问题2）。
    ///
    /// `rescan`：是否重扫磁盘。仅当 `run_once` 内 L0→L1 实际执行过（磁盘视图已变）
    /// 才需要；否则复用首扫结果，避免一轮扫两次（加固 #4）。
    async fn run_finalization(&self, rescan: bool) -> Result<(), sensearcdb_common::SenseArcError> {
        if rescan {
            // L0→L1 已删除源 L0 并写入新 L1，旧快照可能引用已被删的 L0 文件。
            self.scan_files().await;
        }

        let now_ns = current_epoch_ns();
        let candidates = self.finalization_candidates(now_ns);
        for g in candidates {
            let paths: Vec<PathBuf> = g
                .all_files()
                .iter()
                .map(|f| PathBuf::from(&f.path))
                .collect();
            self.merge_paths_to_partition(
                g.tenant_id,
                g.partition_ns,
                &paths,
                SacLevel::L4,
                CompressionType::Zstd9,
            )
            .await?;
        }

        Ok(())
    }

    /// 计算应执行 final 的分区候选（到期且非最终态、且未超 max_late_window 硬边界）。
    ///
    /// 问题2：硬边界用「最后写入新近度」（`last_write_wall_ns`，由 L0/L1 的
    /// `max(write_epoch)` 派生）替代 wall-clock。回填数据的数据时间虽陈旧，但
    /// write_epoch 是落盘时刻；只要最近写入未超窗口，就应把 late L0/L1 并入 L2。
    /// `last_write_wall_ns == 0`（无 L0/L1 或历史文件缺 write_epoch）保守放行。
    fn finalization_candidates(&self, now_ns: i64) -> Vec<PartitionGroup> {
        let duration_ns = (self.partition_duration_ms as i64) * 1_000_000;
        let grace_ns = (self.late_write_grace_secs as i64) * 1_000_000_000;
        let max_late_ns = (self.max_late_window_secs as i64) * 1_000_000_000;

        let groups = self.partition_groups_snapshot();
        let mut out = Vec::new();
        for g in groups {
            let end_ns = g.partition_ns.saturating_add(duration_ns);
            let closed_boundary = end_ns.saturating_add(grace_ns);
            if now_ns < closed_boundary {
                continue;
            }
            if g.is_finalized() {
                continue;
            }
            // 硬边界：仅对「已 final 过、又来了 late 数据」的 re-final 施加窗口上限。
            // 用 write_epoch（最后写入新近度）而非 wall-clock 判窗口（问题2）。
            // 「已 final 过」现在表现为 l4.len()==1（最终态是 L4）。
            if !g.l4.is_empty()
                && g.last_write_wall_ns > 0
                && now_ns.saturating_sub(g.last_write_wall_ns) >= max_late_ns
            {
                tracing::warn!(
                    tenant_id = g.tenant_id,
                    partition_ns = g.partition_ns,
                    l0 = g.l0.len(),
                    l1 = g.l1.len(),
                    l2 = g.l2.len(),
                    l3 = g.l3.len(),
                    l4 = g.l4.len(),
                    last_write_wall_ns = g.last_write_wall_ns,
                    "final skipped: partition beyond max_late_window"
                );
                continue;
            }
            out.push(g);
        }
        out
    }

    /// final 解耦入口：可选重扫 → 判定到期/幂等/硬边界 → 对每个候选分区 claim
    /// `finalizing` 后 `tokio::spawn` 后台执行 ZSTD9 merge（不 await）。
    ///
    /// 主循环只等待一次（至多）轻量扫描，重活交给后台任务，避免阻塞 L0→L1。
    async fn schedule_finalization_detached(self: &Arc<Self>, rescan: bool) {
        if rescan {
            self.scan_files().await;
        }

        let now_ns = current_epoch_ns();
        let candidates = self.finalization_candidates(now_ns);
        for g in candidates {
            let key = (g.tenant_id, g.partition_ns);
            // 对称互斥：L2→L3 正在重压该分区时跳过 final，避免并发删同一批 L2 源 → 数据重复。
            if self.is_l2_to_l3_inflight(key.0, key.1) {
                continue;
            }
            // claim：同分区只允许一个 final 在飞；已在 finalizing 则跳过（幂等）。
            {
                let mut fin = self.finalizing.lock().unwrap();
                if !fin.insert(key) {
                    continue;
                }
            }

            let this = self.clone();
            tokio::spawn(async move {
                // 迁移停摆门：spawn 后、真正开始 ZSTD9 重活前再查一次（不缓存旧值），
                // 避免 pause 期间启动无用的 final merge（省 CPU）。若 merge 中途才被
                // pause，则由 `merge_paths_to_partition_blocking` 的「删源前二道门」兜底。
                if this.is_paused() {
                    this.finalizing.lock().unwrap().remove(&key);
                    tracing::debug!(
                        tenant_id = g.tenant_id,
                        partition_ns = g.partition_ns,
                        "detached final skipped: compaction paused"
                    );
                    return;
                }

                let paths: Vec<PathBuf> = g
                    .all_files()
                    .iter()
                    .map(|f| PathBuf::from(&f.path))
                    .collect();
                let result = this
                    .merge_paths_to_partition(
                        g.tenant_id,
                        g.partition_ns,
                        &paths,
                        SacLevel::L4,
                        CompressionType::Zstd9,
                    )
                    .await;
                // 完成（成功或失败）后释放 finalizing，允许后续轮次重试/收敛。
                this.finalizing.lock().unwrap().remove(&key);
                if let Err(e) = result {
                    tracing::error!(
                        tenant_id = g.tenant_id,
                        partition_ns = g.partition_ns,
                        error = %e,
                        "detached final compaction failed"
                    );
                }
            });
        }
    }

    /// 是否正在对该分区执行 final（同分区互斥检查）。
    fn is_finalizing(&self, tenant_id: u32, partition_ns: i64) -> bool {
        self.finalizing
            .lock()
            .unwrap()
            .contains(&(tenant_id, partition_ns))
    }

    /// 是否正在对该分区执行 L2→L3（同分区互斥检查）。
    fn is_l2_to_l3_inflight(&self, tenant_id: u32, partition_ns: i64) -> bool {
        self.l2_to_l3_inflight
            .lock()
            .unwrap()
            .contains(&(tenant_id, partition_ns))
    }

    /// 把一组 SAC 文件合并为「输出到源分区目录」的单个目标层级文件（copy-on-write，I6）。
    ///
    /// 流式流程（根治内存暴涨）：逐文件 `SacRowIter` 惰性读 → [`KWayMerge`] 堆逐点归并 →
    /// [`DedupIter`] 边去重边按 [`SAC_BLOCK_ROWS`] 行攒块 → 逐块 `rows_to_chunk` +
    /// `SacFileBuilder::append_chunk` 写盘。内存 O(block × 源文件数)，不再物化整分区
    /// 4~5 份全量副本。
    ///
    /// 正确性不变量：
    /// - D1 去重语义逐字节不变（`DedupIter` 与旧 `dedup_sorted` 逐点一致）。
    /// - D2 k-way (table_id, ts, source_id) 排序序不变（`KWayMerge` 与旧 `k_way_merge` 逐点一致）。
    /// - D3 只接收同一 (tenant, partition) 的 `paths`，绝不跨分区合并（v9 多表聚合）。
    /// - D4 空结果删除源文件（与旧路径 `rows.is_empty()` 行为一致），final 幂等。
    /// - D5 流式产物与旧全量 merge 逐点一致（去重/LWW 语义不变）。
    /// - D6 迁移停摆门：`finish` rename 之后、删源之前若 `paused`，保留源文件并返回
    ///   `Ok(0)`（不删源、不丢数据；resume 后下一轮重新收敛）。
    ///
    /// 重活（同步 IO + CPU 编解码）经 [`tokio::task::spawn_blocking`] 跑在线程池，
    /// 不占用 tokio 异步 worker（加固 #3）。
    async fn merge_paths_to_partition(
        &self,
        tenant_id: u32,
        partition_ns: i64,
        paths: &[PathBuf],
        dst_level: SacLevel,
        compression: CompressionType,
    ) -> Result<usize, sensearcdb_common::SenseArcError> {
        // 防御性断言（debug 构建生效，release 零开销）：compaction 写侧传入的
        // `compression` 必须与目标 level 的默认压缩一致。读侧由
        // `SacLevel::compression()`（header.rs:55）派生解压算法，写读若错配会
        // 静默回退原始字节造成数据错乱。此处统一兜底所有调用方（L0→L1、final）。
        debug_assert_eq!(
            compression,
            dst_level.compression(),
            "compaction compression must match target level"
        );

        let sac_dir = self.sac_dir.clone();
        let encryptor = self.encryptor.clone();
        let paused = self.paused.clone();
        let paths = paths.to_vec();
        tokio::task::spawn_blocking(move || {
            // 刀3 分流：L1 走点级旧路径（L0→L1 去重语义不变）；L3 走点级 file_rank
            // 决胜重压（归并 block 到 4096 点/块，提压缩率）；L2/L4 走 block 拼接。
            if dst_level == SacLevel::L1 {
                merge_paths_to_partition_blocking(
                    sac_dir,
                    encryptor,
                    tenant_id,
                    partition_ns,
                    &paths,
                    dst_level,
                    compression,
                    paused.as_ref(),
                )
            } else if dst_level == SacLevel::L3 {
                merge_paths_to_partition_by_file_rank_blocking(
                    sac_dir,
                    encryptor,
                    tenant_id,
                    partition_ns,
                    &paths,
                    dst_level,
                    compression,
                    paused.as_ref(),
                )
            } else {
                splice_blocks_to_partition_blocking(
                    sac_dir,
                    encryptor,
                    tenant_id,
                    partition_ns,
                    &paths,
                    dst_level,
                    compression,
                    paused.as_ref(),
                )
            }
        })
        .await
        .map_err(|e| {
            sensearcdb_common::SenseArcError::Internal(format!("compaction join: {}", e))
        })?
    }

    /// 取出当前 (tenant, partition) 分组快照。
    fn partition_groups_snapshot(&self) -> Vec<PartitionGroup> {
        let state = self.state.lock().unwrap();
        state.partitions.values().cloned().collect()
    }

    /// 检查是否应进入空转暂停模式。
    pub fn should_idle_pause(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.idle_count >= self.idle_threshold
    }

    /// 重置空转计数（Compaction 实际执行后调用）。
    pub fn reset_idle(&self) {
        let mut state = self.state.lock().unwrap();
        state.idle_count = 0;
    }

    /// 通知有新文件（唤醒调度器）。
    pub fn notify_new_files(&self) {
        self.wakeup.notify_one();
    }

    /// 获取合并策略引用。
    pub fn strategy(&self) -> &CompactionStrategy {
        &self.strategy
    }
}

/// L2→L3 detached 任务的 inflight claim 守卫：Drop 时自动释放（panic 也不泄漏）。
struct L2ToL3InflightGuard {
    scheduler: Arc<CompactionScheduler>,
    key: (u32, i64),
}

impl Drop for L2ToL3InflightGuard {
    fn drop(&mut self) {
        self.scheduler.l2_to_l3_inflight.lock().unwrap().remove(&self.key);
    }
}

/// 流式 merge 的同步实现（在 `spawn_blocking` 线程池中运行）。
///
/// 见 [`CompactionScheduler::merge_paths_to_partition`] 的 doc：k-way 堆逐点归并 →
/// 边去重边按 [`SAC_BLOCK_ROWS`] 行攒块 → 逐块写盘。所有数据均为 owned 入参，
/// 便于 `'static` 闭包捕获。
#[allow(clippy::too_many_arguments)]
fn merge_paths_to_partition_blocking(
    sac_dir: PathBuf,
    encryptor: SharedEncryptor,
    tenant_id: u32,
    partition_ns: i64,
    paths: &[PathBuf],
    dst_level: SacLevel,
    compression: CompressionType,
    paused: &AtomicBool,
) -> Result<usize, sensearcdb_common::SenseArcError> {
    // 流式读期间捕获到的首个读错误（迭代器遇错即终止，事后统一上抛 + 回滚）。
    let read_err: Arc<Mutex<Option<sensearcdb_common::SenseArcError>>> = Arc::new(Mutex::new(None));

    // 每个源文件一个惰性迭代器；`SacRowIter::new_all` 立即完成 header/footer/
    // TableIndex/L2 解析与租户校验（open 错误在此立刻上抛），读全表（v9 多表聚合）。
    let mut streams: Vec<RowStream> = Vec::with_capacity(paths.len());
    for (source_id, p) in paths.iter().enumerate() {
        let iter = SacRowIter::new_all(
            p,
            sensearcdb_common::TenantId(tenant_id),
            i64::MIN,
            i64::MAX,
            &[],
            encryptor.clone(),
        )?;
        streams.push(RowStream {
            inner: iter,
            source_id,
            err: read_err.clone(),
        });
    }

    let writer = SacWriter::new_with_encryptor(&sac_dir, compression, encryptor.clone());
    let tenant = sensearcdb_common::TenantId(tenant_id);

    // 惰性开启输出：首个有效数据点到达时才创建 .tmp（空结果不落盘、不留 .tmp）。
    let mut builder: Option<SacFileBuilder> = None;
    let mut batch: Vec<SacRow> = Vec::with_capacity(SAC_BLOCK_ROWS);
    let mut batch_table: Option<u32> = None;

    for dp in DedupIter::new(KWayMerge::new(streams)) {
        if builder.is_none() {
            let tenant_dir = sac_dir.join(tenant_id.to_string());
            // 输出到源分区：seq 取「该分区该 level 现有最大 seq + 1」，避免覆盖既有文件。
            let seq = next_seq_for_partition(&tenant_dir, partition_ns, dst_level);
            let now_ns = current_epoch_ns();
            builder = Some(writer.begin_write(tenant_id, partition_ns, dst_level, seq, now_ns)?);
        }

        // 块不可跨表：table_id 切换时先冲刷上一表的残批（k-way 已按 (table_id, ts) 排序）。
        if batch_table.is_some() && batch_table != Some(dp.table_id) && !batch.is_empty() {
            let chunk = rows_to_chunk(tenant, batch_table.unwrap(), std::mem::take(&mut batch));
            builder
                .as_mut()
                .expect("builder must be initialized before first row")
                .append_chunk(&chunk, None)?;
        }
        batch_table = Some(dp.table_id);

        batch.push(data_point_to_row(dp));
        if batch.len() >= SAC_BLOCK_ROWS {
            let chunk = rows_to_chunk(tenant, batch_table.unwrap(), std::mem::take(&mut batch));
            builder
                .as_mut()
                .expect("builder must be initialized before first row")
                .append_chunk(&chunk, None)?;
        }
    }

    // 读错误优先上抛（流式 iterator 遇错即终止），并回滚已写的 .tmp，不删源文件。
    if let Some(e) = read_err.lock().unwrap().take() {
        if let Some(b) = builder {
            b.abort();
        }
        return Err(e);
    }

    // 末尾不足 4096 行的批次。
    if !batch.is_empty() {
        let chunk = rows_to_chunk(tenant, batch_table.unwrap(), batch);
        builder
            .as_mut()
            .expect("builder must be initialized before first row")
            .append_chunk(&chunk, None)?;
    }

    let dest_path: Option<PathBuf> = match builder {
        Some(b) => Some(b.finish()?),
        None => {
            // 源文件无有效数据：仍删除源文件（内容为空），避免空文件堆积。
            None
        }
    };

    // 迁移停摆门（阶段 7 / 6a-2）：finish rename 之后、删源之前再查一次 paused。
    // detached final 是长任务，pause 可能发生在 merge 中途；此时输出文件已落盘
    // （copy-on-write，新文件 seq 已 +1，与源路径不 overlap），但绝不删源——
    // 源文件保留，resume 后下一轮重新收敛，既不丢数据、也不在迁移期间改写源段集合。
    // 每次实时读取 SeqCst，不缓存旧值。
    if paused.load(Ordering::SeqCst) {
        tracing::warn!(
            tenant_id = tenant_id,
            partition_ns = partition_ns,
            level = ?dst_level,
            "compaction paused before deleting source files; source files retained for resume"
        );
        return Ok(0);
    }

    // 删除源文件（新输出文件 seq 已 +1，路径与源文件不重叠）。
    delete_files(paths);

    // 观测日志：记录本次 compaction 合并的「源文件列表 → 目标文件」映射，
    // 让用户能追溯多 partition 并行 flush / final / late re-final 交叉的合并关系。
    if let Some(dest_path) = &dest_path {
        let source_files: Vec<String> = paths
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .collect();
        tracing::info!(
            target: "compaction_merge",
            source_files = ?source_files,
            dest_file = %dest_path.display(),
            level = ?dst_level,
            "compaction merged"
        );
    }

    Ok(paths.len())
}

/// L2→L3 点级重压（file_rank 决胜，刀3 分流）：与 [`merge_paths_to_partition_blocking`]
/// 几乎一致，唯一差异是源文件按 `(write_epoch DESC, seq DESC)` 排序赋 file_rank
/// （0=最新），决胜迭代器用 [`DedupIterByFileRank`]（file_rank 优先，禁用 wal_seq）。
#[allow(clippy::too_many_arguments)]
fn merge_paths_to_partition_by_file_rank_blocking(
    sac_dir: PathBuf,
    encryptor: SharedEncryptor,
    tenant_id: u32,
    partition_ns: i64,
    paths: &[PathBuf],
    dst_level: SacLevel,
    compression: CompressionType,
    paused: &AtomicBool,
) -> Result<usize, sensearcdb_common::SenseArcError> {
    // 流式读期间捕获到的首个读错误（迭代器遇错即终止，事后统一上抛 + 回滚）。
    let read_err: Arc<Mutex<Option<sensearcdb_common::SenseArcError>>> = Arc::new(Mutex::new(None));

    // 1. 读源文件 header，按 (write_epoch DESC, seq DESC) 排序赋 file_rank（0=最新）。
    //    与查询跨文件 LWW（write_epoch DESC, seq DESC）语义一致。
    let mut source_headers: Vec<(PathBuf, u64, u16)> = Vec::with_capacity(paths.len());
    for p in paths {
        let h = SacReader::read_header(p)?;
        source_headers.push((p.clone(), h.write_epoch, h.seq));
    }
    source_headers.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)));

    // 2. 每个源文件一个惰性迭代器，source_id = file_rank（决胜键）。
    let mut streams: Vec<RowStream> = Vec::with_capacity(source_headers.len());
    for (file_rank, (p, _, _)) in source_headers.iter().enumerate() {
        let iter = SacRowIter::new_all(
            p,
            sensearcdb_common::TenantId(tenant_id),
            i64::MIN,
            i64::MAX,
            &[],
            encryptor.clone(),
        )?;
        streams.push(RowStream {
            inner: iter,
            source_id: file_rank,
            err: read_err.clone(),
        });
    }

    let writer = SacWriter::new_with_encryptor(&sac_dir, compression, encryptor.clone());
    let tenant = sensearcdb_common::TenantId(tenant_id);

    // 惰性开启输出：首个有效数据点到达时才创建 .tmp（空结果不落盘、不留 .tmp）。
    let mut builder: Option<SacFileBuilder> = None;
    let mut batch: Vec<SacRow> = Vec::with_capacity(SAC_BLOCK_ROWS);
    let mut batch_table: Option<u32> = None;

    // 3. file_rank 决胜点级重压（DedupIterByFileRank 而非 DedupIter）。
    for dp in DedupIterByFileRank::new(KWayMerge::new(streams)) {
        if builder.is_none() {
            let tenant_dir = sac_dir.join(tenant_id.to_string());
            // 输出到源分区：seq 取「该分区该 level 现有最大 seq + 1」，避免覆盖既有文件。
            let seq = next_seq_for_partition(&tenant_dir, partition_ns, dst_level);
            let now_ns = current_epoch_ns();
            builder = Some(writer.begin_write(tenant_id, partition_ns, dst_level, seq, now_ns)?);
        }

        // 块不可跨表：table_id 切换时先冲刷上一表的残批（k-way 已按 (table_id, ts) 排序）。
        if batch_table.is_some() && batch_table != Some(dp.table_id) && !batch.is_empty() {
            let chunk = rows_to_chunk(tenant, batch_table.unwrap(), std::mem::take(&mut batch));
            builder
                .as_mut()
                .expect("builder must be initialized before first row")
                .append_chunk(&chunk, None)?;
        }
        batch_table = Some(dp.table_id);

        batch.push(data_point_to_row(dp));
        if batch.len() >= SAC_BLOCK_ROWS {
            let chunk = rows_to_chunk(tenant, batch_table.unwrap(), std::mem::take(&mut batch));
            builder
                .as_mut()
                .expect("builder must be initialized before first row")
                .append_chunk(&chunk, None)?;
        }
    }

    // 读错误优先上抛（流式 iterator 遇错即终止），并回滚已写的 .tmp，不删源文件。
    if let Some(e) = read_err.lock().unwrap().take() {
        if let Some(b) = builder {
            b.abort();
        }
        return Err(e);
    }

    // 末尾不足 4096 行的批次。
    if !batch.is_empty() {
        let chunk = rows_to_chunk(tenant, batch_table.unwrap(), batch);
        builder
            .as_mut()
            .expect("builder must be initialized before first row")
            .append_chunk(&chunk, None)?;
    }

    let dest_path: Option<PathBuf> = match builder {
        Some(b) => Some(b.finish()?),
        None => {
            // 源文件无有效数据：仍删除源文件（内容为空），避免空文件堆积。
            None
        }
    };

    // 迁移停摆门（阶段 7 / 6a-2）：finish rename 之后、删源之前再查一次 paused。
    if paused.load(Ordering::SeqCst) {
        tracing::warn!(
            tenant_id = tenant_id,
            partition_ns = partition_ns,
            level = ?dst_level,
            "compaction paused before deleting source files; source files retained for resume"
        );
        return Ok(0);
    }

    // 删除源文件（新输出文件 seq 已 +1，路径与源文件不重叠）。
    delete_files(paths);

    // 观测日志：区分「block splice」与「L2->L3 point-level recompress」。
    if let Some(dest_path) = &dest_path {
        let source_files: Vec<String> = paths
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .collect();
        tracing::info!(
            target: "compaction_merge",
            source_files = ?source_files,
            dest_file = %dest_path.display(),
            level = ?dst_level,
            "compaction merged (L2->L3 point-level recompress)"
        );
    }

    Ok(paths.len())
}

/// block 拼接的块元数据（刀3）。
struct SpliceBlock {
    /// 表 ID。
    table_id: u32,
    /// Block 内最小/最大时间戳。
    min_ts: i64,
    max_ts: i64,
    /// Block 内列数。
    column_count: u16,
    /// 源文件决胜键（0=最新，按 `write_epoch DESC, seq DESC` 排序后下标）。
    file_rank: usize,
    /// 源文件路径。
    source_path: PathBuf,
    /// Block 在源文件内的字节偏移与大小。
    block_offset: u64,
    block_size: u32,
    /// 源文件 DEK 版本（TDE-1 校验）。
    dek_version: u32,
    /// 源文件层级对应的块级压缩算法（由源 header.level 派生）。
    ///
    /// copy 路径的硬边界：仅当「源压缩 == 目标层级压缩」时才可逐字节复制；否则
    /// （如 final 把 LZ4 的 L0 block 拼进 ZSTD9 的 L4 文件）必须降级点级重编码，
    /// 否则 reader 按目标层级 ZSTD9 解压 LZ4 流会读到垃圾（刀2 压缩前移连锁）。
    source_compression: CompressionType,
}

/// 分代合并 / final 的 block 级拼接归并核心（刀3）：非重叠 Block 逐字节复制，
/// 重叠 Block 点级去重（决胜键 = 源文件 file_rank，禁用 wal_seq，D8）。
///
/// 数据流（设计 §3.1）：
/// 1. 读每个源文件 header（write_epoch/seq/dek_version）
/// 2. 按 `(write_epoch DESC, seq DESC)` 排序源文件，赋 file_rank（0=最新）
/// 3. 读全部 L2Descriptor，K-way 归并为全局 block 流（`(table_id, min_ts, file_rank)` 序）
/// 4. 同表 run 区间合并检测重叠：run==1 且 DEK 一致 → 复制字节；否则 → 点级去重
/// 5. `append_raw_block`（复制）/ `append_chunk`（去重）写盘
/// 6. `finish()` 重建 L2/TableIndex/footer/CRC
/// 7. copy-on-write：paused 门 → `delete_files`
///
/// 不变式 I1-I8 + TDE-1（见设计 §5）。
#[allow(clippy::too_many_arguments)]
fn splice_blocks_to_partition_blocking(
    sac_dir: PathBuf,
    encryptor: SharedEncryptor,
    tenant_id: u32,
    partition_ns: i64,
    paths: &[PathBuf],
    dst_level: SacLevel,
    compression: CompressionType,
    paused: &AtomicBool,
) -> Result<usize, sensearcdb_common::SenseArcError> {
    let tenant = sensearcdb_common::TenantId(tenant_id);
    // 目标文件活跃 DEK 版本（明文=0）。TDE-1：源 block 的 dek_version 须一致才可复制。
    let target_dek = match &encryptor {
        Some(e) if e.is_enabled() => e.active_dek_version(tenant),
        _ => 0,
    };

    // 1. 读源文件 header，按 (write_epoch DESC, seq DESC) 排序赋 file_rank（0=最新）。
    //    同时记下源文件层级派生的压缩算法（copy 硬边界，刀2 压缩前移连锁）。
    let mut source_headers: Vec<(PathBuf, u64, u16, u32, CompressionType)> =
        Vec::with_capacity(paths.len());
    for p in paths {
        let h = SacReader::read_header(p)?;
        source_headers.push((
            p.clone(),
            h.write_epoch,
            h.seq,
            h.dek_version,
            h.level.compression(),
        ));
    }
    source_headers.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)));
    let sources: Vec<(PathBuf, usize, u32, CompressionType)> = source_headers
        .into_iter()
        .enumerate()
        .map(|(rank, (path, _, _, dek_version, source_compression))| {
            (path, rank, dek_version, source_compression)
        })
        .collect();

    // 2. 读全部 L2Descriptor，构造全局 block 流。
    let mut blocks: Vec<SpliceBlock> = Vec::new();
    for (path, file_rank, dek_version, source_compression) in &sources {
        for d in SacReader::read_l2_descriptors(path)? {
            blocks.push(SpliceBlock {
                table_id: d.table_id,
                min_ts: d.min_ts,
                max_ts: d.max_ts,
                column_count: d.column_count,
                file_rank: *file_rank,
                source_path: path.clone(),
                block_offset: d.block_offset,
                block_size: d.block_size,
                dek_version: *dek_version,
                source_compression: *source_compression,
            });
        }
    }
    blocks.sort_by(|a, b| {
        a.table_id
            .cmp(&b.table_id)
            .then_with(|| a.min_ts.cmp(&b.min_ts))
            .then_with(|| a.file_rank.cmp(&b.file_rank))
    });

    let writer = SacWriter::new_with_encryptor(&sac_dir, compression, encryptor.clone());
    let mut builder: Option<SacFileBuilder> = None;

    let mut i = 0usize;
    while i < blocks.len() {
        // run 区间合并（§3.3）：同表传递性重叠链（`min_ts <= run_max_ts` 含等号）归入一个 run。
        let table_id = blocks[i].table_id;
        let mut run_end = i + 1;
        let mut run_max_ts = blocks[i].max_ts;
        while run_end < blocks.len() && blocks[run_end].table_id == table_id {
            if blocks[run_end].min_ts <= run_max_ts {
                run_max_ts = run_max_ts.max(blocks[run_end].max_ts);
                run_end += 1;
            } else {
                break;
            }
        }
        let run = &blocks[i..run_end];

        // 惰性开启输出：首个 block 到达时才创建 .tmp（空结果不落盘、不留 .tmp）。
        if builder.is_none() {
            let tenant_dir = sac_dir.join(tenant_id.to_string());
            let seq = next_seq_for_partition(&tenant_dir, partition_ns, dst_level);
            let now_ns = current_epoch_ns();
            builder = Some(writer.begin_write(tenant_id, partition_ns, dst_level, seq, now_ns)?);
        }

        // 复制条件：run 仅 1 块、dek_version == target_dek（TDE-1）、且源压缩 == 目标层级
        // 压缩。压缩不匹配（如 LZ4 的 L0 block 拼进 ZSTD9 的 L4/L2/L3）必须降级点级重编码。
        if run.len() == 1
            && run[0].dek_version == target_dek
            && run[0].source_compression == compression
        {
            let blk = &run[0];
            let block_bytes =
                SacReader::read_block_bytes(&blk.source_path, blk.block_offset, blk.block_size)?;
            // 读 48B mini-header 的 row_count（偏移 6..10，LE）补 TableIndex。
            let row_count = u32::from_le_bytes(
                block_bytes
                    .get(6..10)
                    .map(|s| s.try_into().unwrap_or([0u8; 4]))
                    .unwrap_or([0u8; 4]),
            );
            builder.as_mut().unwrap().append_raw_block(
                &block_bytes,
                blk.table_id,
                blk.min_ts,
                blk.max_ts,
                blk.column_count,
                row_count,
            )?;
        } else {
            // 点级去重（重叠 block 或 DEK 版本不一致降级重编码）。
            let run_min_ts = run.iter().map(|x| x.min_ts).min().unwrap_or(i64::MIN);
            let result = splice_dedup_run(
                builder.as_mut().unwrap(),
                run,
                tenant,
                table_id,
                run_min_ts,
                run_max_ts,
                encryptor.clone(),
            );
            if let Err(e) = result {
                // 读错误优先上抛并回滚已写的 .tmp（不删源文件）。
                if let Some(b) = builder.take() {
                    b.abort();
                }
                return Err(e);
            }
        }

        i = run_end;
    }

    let dest_path: Option<PathBuf> = match builder {
        Some(b) => Some(b.finish()?),
        None => None,
    };

    // 迁移停摆门：finish rename 之后、删源之前再查 paused（与点级路径同语义）。
    if paused.load(Ordering::SeqCst) {
        tracing::warn!(
            tenant_id = tenant_id,
            partition_ns = partition_ns,
            level = ?dst_level,
            "compaction paused before deleting source files; source files retained for resume"
        );
        return Ok(0);
    }

    // 删除源文件（新输出文件 seq 已 +1，路径与源文件不重叠）。
    delete_files(paths);

    if let Some(dest_path) = &dest_path {
        let source_files: Vec<String> = paths
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .collect();
        tracing::info!(
            target: "compaction_merge",
            source_files = ?source_files,
            dest_file = %dest_path.display(),
            level = ?dst_level,
            "compaction merged (block splice)"
        );
    }

    Ok(paths.len())
}

/// 点级去重单个重叠 run（刀3）：读 run 内各 block 所属源文件的该表该时间窗行 →
/// [`KWayMerge`] → [`DedupIterByFileRank`]（file_rank 决胜，禁用 wal_seq，D8）→
/// 重编码重压（[`rows_to_chunk`] + `append_chunk`）。
#[allow(clippy::too_many_arguments)]
fn splice_dedup_run(
    builder: &mut SacFileBuilder,
    run: &[SpliceBlock],
    tenant: sensearcdb_common::TenantId,
    table_id: u32,
    run_min_ts: i64,
    run_max_ts: i64,
    encryptor: SharedEncryptor,
) -> Result<(), sensearcdb_common::SenseArcError> {
    // 收集 run 内唯一源文件（path → file_rank），按 file_rank 升序读。
    let mut unique_sources: Vec<(PathBuf, usize)> = Vec::new();
    for blk in run {
        if !unique_sources.iter().any(|(p, _)| p == &blk.source_path) {
            unique_sources.push((blk.source_path.clone(), blk.file_rank));
        }
    }

    // 流式读期间捕获到的首个读错误（迭代器遇错即终止，事后统一上抛 + 回滚）。
    let read_err: Arc<Mutex<Option<sensearcdb_common::SenseArcError>>> =
        Arc::new(Mutex::new(None));

    let mut streams: Vec<RowStream> = Vec::with_capacity(unique_sources.len());
    for (path, file_rank) in &unique_sources {
        let iter = SacRowIter::new(
            path,
            tenant,
            table_id,
            run_min_ts,
            run_max_ts,
            &[],
            encryptor.clone(),
        )?;
        streams.push(RowStream {
            inner: iter,
            source_id: *file_rank,
            err: read_err.clone(),
        });
    }

    // k-way 归并 + file_rank 决胜去重 + 逐块重编码。
    let mut batch: Vec<SacRow> = Vec::with_capacity(SAC_BLOCK_ROWS);
    for dp in DedupIterByFileRank::new(KWayMerge::new(streams)) {
        batch.push(data_point_to_row(dp));
        if batch.len() >= SAC_BLOCK_ROWS {
            let chunk = rows_to_chunk(tenant, table_id, std::mem::take(&mut batch));
            builder.append_chunk(&chunk, None)?;
        }
    }

    // 读错误优先上抛（不删源文件；builder 由调用方 abort）。
    if let Some(e) = read_err.lock().unwrap().take() {
        return Err(e);
    }

    if !batch.is_empty() {
        let chunk = rows_to_chunk(tenant, table_id, batch);
        builder.append_chunk(&chunk, None)?;
    }

    Ok(())
}

/// 扫描 SAC 目录，按 `(tenant, partition)` 分组收集 L0/L1/L2/L3/L4 文件元数据（v9 多表聚合）。
///
/// 文件名首段即 `partition_ns`（数据时间边界）。L2/L3 在此也被纳入（final 需要全量
/// 合并 L0/L1/L2/L3；旧实现把 L2 丢弃导致 final 无源文件）。
fn scan_sac_dir(sac_dir: &Path) -> Result<Vec<PartitionGroup>, sensearcdb_common::SenseArcError> {
    let mut groups: HashMap<(u32, i64), PartitionGroup> = HashMap::new();

    if !sac_dir.exists() {
        return Ok(Vec::new());
    }

    // v9：路径只留 tenant 一层，文件名 `{partition_ns}_{level}_{seq}.sac`。
    for tenant_entry in std::fs::read_dir(sac_dir)
        .map_err(|e| sensearcdb_common::SenseArcError::Internal(format!("read sac dir: {}", e)))?
    {
        let tenant_dir = tenant_entry
            .map_err(|e| {
                sensearcdb_common::SenseArcError::Internal(format!("read sac dir entry: {}", e))
            })?
            .path();
        if !tenant_dir.is_dir() {
            continue;
        }
        // 目录名非 u32（非 tenant 目录）→ 跳过。
        let tenant_id: u32 = match tenant_dir
            .file_name()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse().ok())
        {
            Some(id) => id,
            None => continue,
        };

        for file_entry in std::fs::read_dir(&tenant_dir).map_err(|e| {
            sensearcdb_common::SenseArcError::Internal(format!("read tenant dir: {}", e))
        })? {
            let file_path = file_entry
                .map_err(|e| {
                    sensearcdb_common::SenseArcError::Internal(format!("read file entry: {}", e))
                })?
                .path();
            if file_path.extension().and_then(|e| e.to_str()) != Some("sac") {
                continue;
            }
            let (partition_ns, _, _) = match file_path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|n| SacNaming::parse_file_name(n))
            {
                Some(parsed) => parsed,
                None => continue,
            };

            if let Some(meta) = read_sac_meta(&file_path, tenant_id) {
                let group =
                    groups
                        .entry((tenant_id, partition_ns))
                        .or_insert_with(|| PartitionGroup {
                            tenant_id,
                            partition_ns,
                            l0: Vec::new(),
                            l1: Vec::new(),
                            l2: Vec::new(),
                            l3: Vec::new(),
                            l4: Vec::new(),
                            last_write_wall_ns: 0,
                        });
                match meta.level {
                    0 => {
                        // last_write_wall_ns 只统计 L0/L1（late 写入的新近度）；L2/L3/L4 是
                        // compaction/final 产物（write_epoch 可能已旧），不计入。
                        group.last_write_wall_ns =
                            group.last_write_wall_ns.max(write_epoch_to_wall_ns(meta.write_epoch));
                        group.l0.push(meta);
                    }
                    1 => {
                        group.last_write_wall_ns =
                            group.last_write_wall_ns.max(write_epoch_to_wall_ns(meta.write_epoch));
                        group.l1.push(meta);
                    }
                    2 => group.l2.push(meta),
                    3 => group.l3.push(meta),
                    4 => group.l4.push(meta),
                    _ => {}
                }
            }
        }
    }

    // 结果按 (tenant, partition) 排序，保证确定性（测试与日志可复现）。
    let mut out: Vec<PartitionGroup> = groups.into_values().collect();
    out.sort_by(|a, b| (a.tenant_id, a.partition_ns).cmp(&(b.tenant_id, b.partition_ns)));
    Ok(out)
}

/// 删除一组 SAC 源文件，并清空随之变空的 tenant 目录（只删一层，不递归）。
fn delete_files(paths: &[PathBuf]) {
    let mut parent_dirs: Vec<PathBuf> = Vec::new();
    for p in paths {
        if let Some(parent) = p.parent() {
            parent_dirs.push(parent.to_path_buf());
        }
        if let Err(e) = std::fs::remove_file(p) {
            tracing::warn!(path = %p.display(), error = %e, "failed to remove compacted source file");
        }
    }
    parent_dirs.sort();
    parent_dirs.dedup();
    for dir in parent_dirs {
        // 只删空目录（非空返回 DirectoryNotEmpty，忽略——通常还有新输出文件存活）。
        let _ = std::fs::remove_dir(&dir);
    }
}

/// 计算某租户目录下、指定 (partition, level) 的下一个可用 seq（现有最大 seq + 1）。
///
/// 输出到「源分区」而非 now_ns 新分区后，必须避免新文件与既有同 level 文件同名覆盖。
/// 复用 [`SacNaming::next_seq_in_dir`] 作为唯一扫盘实现（flush 亦复用），保证 seq 续号口径一致。
fn next_seq_for_partition(tenant_dir: &Path, partition_ns: i64, level: SacLevel) -> u16 {
    SacNaming::next_seq_in_dir(tenant_dir, partition_ns, level)
}

/// 读取单个 SAC 文件的元数据（大小 + 32B header 的 write_epoch + footer 时间范围 + Block 数）。
fn read_sac_meta(path: &Path, tenant_id: u32) -> Option<SacFileMeta> {
    use sensearcdb_common::constants::{SAC_FOOTER_SIZE, SAC_HEADER_SIZE};

    let size_bytes = std::fs::metadata(path).ok()?.len();

    let mut file = std::fs::File::open(path).ok()?;
    use std::io::{Read, Seek, SeekFrom};
    let file_len = size_bytes;
    if file_len < (SAC_HEADER_SIZE + SAC_FOOTER_SIZE) as u64 {
        return None;
    }

    // 读 32B header 取 write_epoch（跨文件 LWW 排序键 / final re-final 窗口判定）。
    file.seek(SeekFrom::Start(0)).ok()?;
    let mut hdr_buf = [0u8; SAC_HEADER_SIZE];
    file.read_exact(&mut hdr_buf).ok()?;
    let header = SacHeader::from_bytes(&hdr_buf)?;
    let write_epoch = header.write_epoch;

    // 读 footer（时间范围 + Block 数）。
    file.seek(SeekFrom::Start(file_len - SAC_FOOTER_SIZE as u64))
        .ok()?;
    let mut buf = [0u8; SAC_FOOTER_SIZE];
    file.read_exact(&mut buf).ok()?;
    let footer = SacFooter::from_bytes(&buf)?;

    let file_name = path.file_name()?.to_str()?;
    let (_partition_ns, level, _seq) = SacNaming::parse_file_name(file_name)?;

    Some(SacFileMeta::new(
        path.to_string_lossy().to_string(),
        tenant_id,
        size_bytes,
        footer.min_ts,
        footer.max_ts,
        level as u8,
        footer.block_count,
        write_epoch,
    ))
}

/// 把 header 的 `write_epoch`（u64）转为 `i64` 墙上时间，超界饱和。
fn write_epoch_to_wall_ns(write_epoch: u64) -> i64 {
    i64::try_from(write_epoch).unwrap_or(i64::MAX)
}

/// 从 SAC 文件路径解析 tenant_id（`{sac_dir}/{tenant_id}/{partition_ns}_{level}_{seq}.sac`）。
///
/// 取路径倒数第 1 层目录名（tenant 目录）解析为 `u32`（与 `read_range` 同口径）。
///
/// 注：SAC v9 起 table_id 已不在文件名/目录名中（多表聚合），故不再提供
/// `parse_table_id_from_path`；tenant_id 仍由租户目录段承载。
#[allow(dead_code)] // 主路径已改走 scan_sac_dir 的分组解析，保留供测试/迁移路径使用。
fn parse_tenant_id_from_path(path: &Path) -> Option<u32> {
    path.parent()?.file_name()?.to_str()?.parse::<u32>().ok()
}

/// 把单个 SAC 文件的流式读取适配为 Compaction 内部 `DataPoint` 流。
///
/// `SacRowIter` 是 `Iterator<Item = Result<SacRow, _>>`，本适配器把每行映射为
/// `DataPoint`（`source_id` 用于 k-way 排序稳定性），读错误写入共享 `err` 槽后终止，
/// 由 `merge_paths_to_partition` 事后统一上抛 + 回滚。
struct RowStream {
    /// 底层惰性 SAC 行迭代器（逐 Block 产出行，O(block) 内存）。
    inner: SacRowIter,
    /// 源文件下标（k-way merge 排序稳定性键）。
    source_id: usize,
    /// 共享读错误槽（任一源流出错即终止整条 merge 链）。
    err: Arc<Mutex<Option<sensearcdb_common::SenseArcError>>>,
}

impl Iterator for RowStream {
    type Item = DataPoint;

    fn next(&mut self) -> Option<DataPoint> {
        match self.inner.next()? {
            Ok(row) => Some(row_to_data_point(row, self.source_id)),
            Err(e) => {
                *self.err.lock().unwrap() = Some(e);
                None
            }
        }
    }
}

/// 将 `SacRow` 转换为 Compaction 内部 `DataPoint`。
///
/// SAC 不单独持久化 server_ts，此处回退为源时间戳 `ts`（去重优先级第 2 条退化为按 ts）；
/// `wal_seq` 由 Block mini-header 的 `max_wal_seq` 携带（第 1 条优先级完整保留）。
/// `table_id` 由 `SacRow`（v9 多表聚合）携带。
fn row_to_data_point(row: SacRow, source_id: usize) -> DataPoint {
    DataPoint {
        table_id: row.table_id,
        ts: row.ts,
        server_ts: row.ts,
        values: row.values,
        qualities: row.qualities,
        wal_seq: row.wal_seq,
        source_id,
    }
}

/// 将去重后的 `DataPoint` 转回 `SacRow`（供 [`rows_to_chunk`] 重编码）。
fn data_point_to_row(p: DataPoint) -> SacRow {
    SacRow {
        table_id: p.table_id,
        ts: p.ts,
        values: p.values,
        qualities: p.qualities,
        wal_seq: p.wal_seq,
    }
}

/// 将 SacRow 列表转换为 ColumnarChunk。
///
/// 依据 `SacRow.values` 中带类型的值按列建对应 `ColVec` 变体（多类型保真）。
fn rows_to_chunk(
    tenant_id: sensearcdb_common::TenantId,
    table_id: u32,
    rows: Vec<crate::sac::reader::SacRow>,
) -> ColumnarChunk {
    let mut timestamps = Vec::with_capacity(rows.len());
    let mut qualities = Vec::with_capacity(rows.len());
    let mut columns: HashMap<u16, ColVec> = HashMap::new();

    // 第一遍：按每列首次出现的物理类型建对应 ColVec 变体
    let mut col_types: HashMap<u16, sensearcdb_common::DataType> = HashMap::new();
    for row in &rows {
        for (col_id, v) in &row.values {
            col_types.entry(*col_id).or_insert_with(|| v.data_type());
        }
    }
    let mut col_ids: Vec<u16> = col_types.keys().copied().collect();
    col_ids.sort_unstable();
    for id in &col_ids {
        columns.insert(*id, ColVec::new_for_type(col_types[id]));
    }

    // 稀疏对齐：每列每行都 push_typed 或 push_absent，保证
    // 「列长度 == timestamps 长度 == presence 长度」，从根上消除 encode_column 的越界 panic。
    // null（读侧在 SacRow.values 中省略的条目）在此重建为 push_absent + presence=false。
    let mut presence: HashMap<u16, Vec<bool>> = col_ids
        .iter()
        .map(|id| (*id, Vec::with_capacity(rows.len())))
        .collect();

    for row in &rows {
        timestamps.push(row.ts);
        qualities.push(row.qualities.first().map(|(_, q)| *q).unwrap_or(0xC0));
        for id in &col_ids {
            let col = columns.get_mut(id).unwrap();
            match row.values.iter().find(|(c, _)| c == id) {
                Some((_, v)) => {
                    col.push_typed(v);
                    presence.get_mut(id).unwrap().push(true);
                }
                None => {
                    col.push_absent();
                    presence.get_mut(id).unwrap().push(false);
                }
            }
        }
    }

    let point_count = rows.len();
    // A4/A5：ts_index 是唯一真相源，必须填充 ts→pos（compaction 已去重，ts 唯一）。
    let ts_index: std::collections::BTreeMap<i64, usize> = timestamps
        .iter()
        .enumerate()
        .map(|(i, ts)| (*ts, i))
        .collect();
    ColumnarChunk {
        table_id,
        tenant_id,
        timestamps,
        columns,
        presence,
        qualities,
        ts_index,
        point_count,
        out_of_order: false,
        // S7: Compaction 输出侧显式 None（去重已消费 seq）
        max_wal_seq: None,
        min_wal_seq: None,
    }
}

/// 获取当前 epoch 纳秒时间戳。
fn current_epoch_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sac::reader::SacReader;

    #[test]
    fn test_compaction_targets() {
        assert_ne!(CompactionTarget::L0ToL1, CompactionTarget::L1ToL2);
    }

    #[test]
    fn test_scheduler_create() {
        let config = ConfigHandle::new(Default::default());
        let scheduler = CompactionScheduler::new(&config);
        assert_eq!(scheduler.poll_interval, Duration::from_secs(8));
        assert_eq!(scheduler.idle_threshold, 10);
    }

    /// 写一个合法的 L0 SAC 文件（含 5 个 F64 点），返回其路径。
    fn write_l0_segment(sac_dir: &Path, table_id: u32, partition_ns: i64, seq: u16) -> PathBuf {
        let writer = SacWriter::new(sac_dir, CompressionType::Lz4);
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        let ts_index: std::collections::BTreeMap<i64, usize> = [100i64, 200, 300, 400, 500]
            .into_iter()
            .enumerate()
            .map(|(i, ts)| (ts, i))
            .collect();
        let chunk = ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps: vec![100, 200, 300, 400, 500],
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; 5],
            ts_index,
            point_count: 5,
            out_of_order: false,
            max_wal_seq: Some(7),
            min_wal_seq: None,
        };
        writer
            .write_chunk(&chunk, partition_ns, SacLevel::L0, seq, Some(7))
            .expect("write l0 segment")
    }

    /// 阶段 7 / 6a-2：暂停后 `run_once` 不执行 compaction，源段不被改写/删除。
    #[tokio::test]
    async fn test_compaction_pause_prevents_source_rewrite() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 1; // 1 个文件即触发合并
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let l0 = write_l0_segment(&sac_dir, 42, 100, 1);
        assert!(l0.exists());

        scheduler.pause();
        assert!(scheduler.is_paused());

        scheduler.run_once().await;

        assert!(
            l0.exists(),
            "暂停后源段不得被合并/删除（compaction 必须真停摆）"
        );
    }

    /// 阶段 7 / 6a-2：未暂停时 `run_once` 正常执行 compaction，合并并删除源 L0。
    #[tokio::test]
    async fn test_compaction_resume_allows_compaction() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 1;
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let l0 = write_l0_segment(&sac_dir, 42, 100, 1);
        assert!(l0.exists());

        // 未暂停：compaction 正常执行，源 L0 被合并删除。
        scheduler.run_once().await;

        assert!(
            !l0.exists(),
            "未暂停时 compaction 应正常合并并删除源段（证明 pause 是唯一拦截点）"
        );
    }

    /// 日对齐分区（I5）：compaction 输出必须落到「源分区目录」，而不是 now_ns 新分区。
    ///
    /// 单 L0 → L0→L1 合并后，源 L0 被删除，partition 目录内出现 L1 输出（目录仍在）。
    #[tokio::test]
    async fn test_compaction_outputs_to_source_partition() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 1;
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let l0 = write_l0_segment(&sac_dir, 42, 100, 1);
        let tenant_dir = l0.parent().unwrap().to_path_buf();
        assert!(l0.exists());
        assert!(tenant_dir.is_dir(), "tenant 目录必须先存在");

        scheduler.run_once().await;

        assert!(!l0.exists(), "源段必须被合并删除");
        assert!(
            tenant_dir.is_dir(),
            "输出应落到源租户目录（不再是 now_ns 新分区），故目录必须仍在"
        );

        // 源租户目录内出现 1 个输出段（可能是 L1，也可能被同轮 final 收敛成 L4）。
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1, "源租户目录内应恰好 1 个输出段");
        let header = SacReader::read_header(&files[0]).unwrap();
        assert!(
            header.level == SacLevel::L1 || header.level == SacLevel::L4,
            "输出应为 L1 或 L4（取决于同轮是否 final）"
        );
    }

    /// 写一个 L0 SAC 文件（指定时间戳 / 值 / WAL seq），返回其路径。
    fn write_l0_segment_with(
        sac_dir: &Path,
        table_id: u32,
        partition_ns: i64,
        seq: u16,
        timestamps: Vec<i64>,
        values: Vec<f64>,
        wal_seq: Option<u64>,
    ) -> PathBuf {
        let writer = SacWriter::new(sac_dir, CompressionType::Lz4);
        let point_count = timestamps.len();
        // A5：ts_index 是唯一真相源，填充 ts→pos。
        let ts_index: std::collections::BTreeMap<i64, usize> = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| (*ts, i))
            .collect();
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(values));
        let chunk = ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps,
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; point_count],
            ts_index,
            point_count,
            out_of_order: false,
            max_wal_seq: wal_seq,
            min_wal_seq: None,
        };
        writer
            .write_chunk(&chunk, partition_ns, SacLevel::L0, seq, wal_seq)
            .expect("write l0 segment")
    }

    /// 递归收集目录下所有 `.sac` 文件路径。
    fn list_sac_files(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        if !dir.exists() {
            return out;
        }
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                out.extend(list_sac_files(&path));
            } else if path.extension().and_then(|e| e.to_str()) == Some("sac") {
                out.push(path);
            }
        }
        out
    }

    /// P2-01：Compaction 输出必须对跨 L0 同 ts 数据点去重，
    /// 且去重按 WAL seq 优先级保留更高 seq 的数据点。
    #[tokio::test]
    async fn test_compaction_dedups_duplicate_ts_across_l0() {
        use crate::memtable::col_vec::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 1;
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        // 两个 L0 文件在 ts=100 重复：file A（seq 低）与 file B（seq 高）。
        // 二者落同一分区（I5：跨分区不合并），才应被 L0→L1 归并去重。
        let a = write_l0_segment_with(
            &sac_dir,
            42,
            100,
            1,
            vec![100, 200],
            vec![1.0, 2.0],
            Some(5),
        );
        let b = write_l0_segment_with(
            &sac_dir,
            42,
            100,
            2,
            vec![100, 300],
            vec![10.0, 30.0],
            Some(9),
        );
        assert!(a.exists());
        assert!(b.exists());

        scheduler.run_once().await;

        // 源 L0 已合并删除，输出为 L1。
        assert!(!a.exists());
        assert!(!b.exists());

        let mut rows = Vec::new();
        for f in list_sac_files(&sac_dir) {
            rows.extend(
                SacReader::read_range(
                    &f,
                    sensearcdb_common::TenantId::DEFAULT,
                    42,
                    i64::MIN,
                    i64::MAX,
                    &[],
                    None,
                )
                .unwrap(),
            );
        }
        rows.sort_by_key(|r| r.ts);

        let tss: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        assert_eq!(tss, vec![100, 200, 300], "跨 L0 同 ts 数据点必须去重");

        // ts=100 应保留 WAL seq 更高（file B）的值 10.0。
        let winner = rows.iter().find(|r| r.ts == 100).unwrap();
        assert_eq!(winner.values, vec![(0, TypedValue::F64(10.0))]);
    }

    /// 流式 merge 跨 Block（>4096 行）+ 跨文件去重（LWW）：
    /// 源文件 A（5000 行 → 2 个 Block）与源文件 B（101 行，覆盖 A 的 ts 2500..2600，
    /// wal_seq 更高）合并后应无丢无重，且覆盖点取 B 的新值。
    /// 同时覆盖 `SacRowIter` 多块惰性读、`KWayMerge`/`DedupIter` 跨块边界、以及
    /// 4096 行攒块写出的流式路径（D1/D2/D5）。
    #[tokio::test]
    async fn test_streaming_merge_cross_block_dedup() {
        use crate::memtable::col_vec::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 1;
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        // A：ts 1..=5000（5000 行 → 2 个 Block），值 = ts。
        let a_ts: Vec<i64> = (1..=5000).collect();
        let a_vals: Vec<f64> = a_ts.iter().map(|&t| t as f64).collect();
        // B：ts 2500..=2600（101 行），值 = ts*10，wal_seq Some(100) 覆盖 A。
        let b_ts: Vec<i64> = (2500..=2600).collect();
        let b_vals: Vec<f64> = b_ts.iter().map(|&t| t as f64 * 10.0).collect();

        let a = write_l0_segment_with(&sac_dir, 42, 100, 1, a_ts, a_vals, None);
        let b = write_l0_segment_with(&sac_dir, 42, 100, 2, b_ts, b_vals, Some(100));
        assert!(a.exists() && b.exists());

        scheduler.run_once().await;

        assert!(!a.exists() && !b.exists(), "源 L0 应被合并删除");

        let outputs = list_sac_files(&sac_dir);
        assert_eq!(outputs.len(), 1, "应产出恰好 1 个输出段");
        let mut rows = SacReader::read_range(
            &outputs[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);

        assert_eq!(rows.len(), 5000, "无丢无重（B 全部落在 A 范围内）");

        // 覆盖区间取 B 新值（wal_seq Some 优先于 None）。
        for t in 2500..=2600 {
            let row = rows.iter().find(|r| r.ts == t).expect("覆盖点必须存在");
            assert_eq!(row.values, vec![(0, TypedValue::F64(t as f64 * 10.0))]);
        }
        // 未被覆盖的点仍取 A 值。
        for &t in &[1, 1000, 2499, 2601, 4000, 5000] {
            let row = rows.iter().find(|r| r.ts == t).expect("未覆盖点必须存在");
            assert_eq!(row.values, vec![(0, TypedValue::F64(t as f64))]);
        }
    }

    /// 写一个双列 L0 SAC 文件：col0 全 present，col1 仅在 `col1_present_ts` 中 present
    /// （稀疏列，用于验证流式攒块跨 4096 块边界时稀疏列的列存在性不丢数据）。
    fn write_l0_segment_sparse_two_col(
        sac_dir: &Path,
        table_id: u32,
        partition_ns: i64,
        seq: u16,
        timestamps: Vec<i64>,
        col0_vals: Vec<f64>,
        col1_present_ts: &[i64],
    ) -> PathBuf {
        let writer = SacWriter::new(sac_dir, CompressionType::Lz4);
        let point_count = timestamps.len();
        let ts_index: std::collections::BTreeMap<i64, usize> = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| (*ts, i))
            .collect();
        // col1 值仅在 present 位置有意义（null 位置不读，值任意）。
        let col1_vals: Vec<f64> = timestamps
            .iter()
            .map(|&ts| {
                if col1_present_ts.contains(&ts) {
                    ts as f64 * 100.0
                } else {
                    0.0
                }
            })
            .collect();
        let presence1: Vec<bool> = timestamps
            .iter()
            .map(|ts| col1_present_ts.contains(ts))
            .collect();
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(col0_vals));
        columns.insert(1, ColVec::F64(col1_vals));
        let mut presence = HashMap::new();
        presence.insert(0u16, vec![true; point_count]);
        presence.insert(1u16, presence1);
        let chunk = ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps,
            columns,
            presence,
            qualities: vec![0xC0; point_count],
            ts_index,
            point_count,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        };
        writer
            .write_chunk(&chunk, partition_ns, SacLevel::L0, seq, None)
            .expect("write l0 sparse")
    }

    /// QA 独立验证（D5 稀疏列 + 跨块边界）：稀疏列 col1 只出现在第二个 4096 块
    /// （ts 4097..=5000），第一个块（ts 1..=4096）col1 全 null。流式攒块重构把
    /// `rows_to_chunk` 从「整分区一次」改为「每 4096 行一次」，必须保证：
    /// 1) 无丢无重（5000 行）；2) col1 在 block0 可整列省略、block1 正常 present，
    ///    读回时 null 仍正确省略、present 值正确，不串列、不越界。
    #[tokio::test]
    async fn test_streaming_merge_sparse_column_across_block_boundary() {
        use crate::memtable::col_vec::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 1;
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        // 5000 行（2 块）：col0 全 present（值=ts），col1 仅 ts 4097..=5000 present（值=ts*100）。
        let a_ts: Vec<i64> = (1..=5000).collect();
        let a_col0: Vec<f64> = a_ts.iter().map(|&t| t as f64).collect();
        let col1_present: Vec<i64> = (4097..=5000).collect();
        let a = write_l0_segment_sparse_two_col(&sac_dir, 42, 100, 1, a_ts, a_col0, &col1_present);
        assert!(a.exists());

        scheduler.run_once().await;

        assert!(!a.exists(), "源 L0 应被合并删除");
        let outputs = list_sac_files(&sac_dir);
        assert_eq!(outputs.len(), 1, "应产出恰好 1 个输出段");

        let mut rows = SacReader::read_range(
            &outputs[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);
        assert_eq!(rows.len(), 5000, "无丢无重");

        for r in &rows {
            let t = r.ts;
            // col0 恒 present 且值 = ts。
            assert_eq!(
                r.values
                    .iter()
                    .find(|(c, _)| *c == 0)
                    .map(|(_, v)| v.clone()),
                Some(TypedValue::F64(t as f64))
            );
            if (4097..=5000).contains(&t) {
                // col1 present，值 = ts*100。
                assert_eq!(
                    r.values
                        .iter()
                        .find(|(c, _)| *c == 1)
                        .map(|(_, v)| v.clone()),
                    Some(TypedValue::F64(t as f64 * 100.0)),
                    "ts={t} col1 应 present"
                );
            } else {
                // col1 null → 应从 values 省略。
                assert!(
                    !r.values.iter().any(|(c, _)| *c == 1),
                    "ts={t} col1 应为 null（省略），不得出现占位 0.0"
                );
            }
        }
    }

    /// 写一个指定 tenant 的 L0 SAC 文件（F64 单列），返回其路径。
    fn write_l0_segment_for_tenant(
        sac_dir: &Path,
        tenant_id: u32,
        table_id: u32,
        partition_ns: i64,
        seq: u16,
        timestamps: Vec<i64>,
        values: Vec<f64>,
    ) -> PathBuf {
        let writer = SacWriter::new(sac_dir, CompressionType::Lz4);
        let point_count = timestamps.len();
        let ts_index: std::collections::BTreeMap<i64, usize> = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| (*ts, i))
            .collect();
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(values));
        let chunk = ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId(tenant_id),
            timestamps,
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; point_count],
            ts_index,
            point_count,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        };
        writer
            .write_chunk(&chunk, partition_ns, SacLevel::L0, seq, None)
            .expect("write l0 segment")
    }

    /// S2 Step 3：compaction 按 (tenant_id, table_id) 分组——输出 chunk 保留真实 tenant，
    /// 同名 table_id 的不同租户不会被合并到同一输出段。
    #[tokio::test]
    async fn test_compaction_preserves_tenant_grouping() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 1;
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        // tenant=1 table=42 两个 L0 段（同一分区，才可跨段合并）；tenant=0 table=42 一个 L0 段。
        let a =
            write_l0_segment_for_tenant(&sac_dir, 1, 42, 100, 1, vec![100, 200], vec![1.0, 2.0]);
        let b = write_l0_segment_for_tenant(&sac_dir, 1, 42, 100, 2, vec![300], vec![3.0]);
        let c = write_l0_segment_for_tenant(&sac_dir, 0, 42, 100, 1, vec![500], vec![50.0]);
        assert!(a.exists() && b.exists() && c.exists());

        scheduler.run_once().await;

        // 源 L0 全部合并删除。
        assert!(
            !a.exists() && !b.exists() && !c.exists(),
            "源 L0 应被合并删除"
        );

        let outputs = list_sac_files(&sac_dir);
        assert_eq!(outputs.len(), 2, "两租户各产出一个 L1 段，不跨租户合并");

        // 每个输出 header.tenant_id 与路径 tenant 段一致。
        for p in &outputs {
            let h = SacReader::read_header(p).unwrap();
            assert_eq!(
                h.tenant_id,
                parse_tenant_id_from_path(p).unwrap(),
                "header.tenant_id 应与路径 tenant 段一致"
            );
        }

        // tenant=1 读回 3 个点（跨 L0 合并且不混入 tenant=0 的 50.0）。
        let t1_path = outputs
            .iter()
            .find(|p| parse_tenant_id_from_path(p) == Some(1))
            .expect("tenant=1 输出段存在");
        let mut t1_rows = SacReader::read_range(
            t1_path,
            sensearcdb_common::TenantId(1),
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        t1_rows.sort_by_key(|r| r.ts);
        let tss: Vec<i64> = t1_rows.iter().map(|r| r.ts).collect();
        assert_eq!(tss, vec![100, 200, 300]);
        let vals: Vec<f64> = t1_rows
            .iter()
            .map(|r| r.values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0))
            .collect();
        assert_eq!(vals, vec![1.0, 2.0, 3.0]);

        // tenant=0 读回 1 个点（50.0），不混入 tenant=1。
        let t0_path = outputs
            .iter()
            .find(|p| parse_tenant_id_from_path(p) == Some(0))
            .expect("tenant=0 输出段存在");
        let t0_rows = SacReader::read_range(
            t0_path,
            sensearcdb_common::TenantId(0),
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(t0_rows.len(), 1);
        assert_eq!(
            t0_rows[0].values.first().map(|(_, v)| v.as_f64()),
            Some(50.0)
        );
    }

    /// T6：compaction 往返——稀疏 SacRow → `rows_to_chunk` 稀疏对齐 + presence 重建，
    /// 重写后不越界 panic，且 null 仍保真。
    #[test]
    fn test_rows_to_chunk_sparse_alignment_and_roundtrip() {
        use crate::memtable::col_vec::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");

        let rows = vec![
            SacRow {
                table_id: 1,
                ts: 100,
                values: vec![(0, TypedValue::F64(1.0))],
                qualities: vec![(0, 0xC0)],
                wal_seq: None,
            },
            SacRow {
                table_id: 1,
                ts: 200,
                values: vec![(0, TypedValue::F64(2.0)), (1, TypedValue::F64(20.0))],
                qualities: vec![(0, 0xC0)],
                wal_seq: None,
            },
            SacRow {
                table_id: 1,
                ts: 300,
                values: vec![(0, TypedValue::F64(3.0))],
                qualities: vec![(0, 0xC0)],
                wal_seq: None,
            },
        ];

        let chunk = rows_to_chunk(sensearcdb_common::TenantId::DEFAULT, 1, rows);

        // 稀疏对齐：每列长度 == timestamps 长度 == presence 长度。
        assert_eq!(chunk.timestamps.len(), 3);
        assert_eq!(chunk.columns.get(&0).unwrap().len(), 3);
        assert_eq!(chunk.columns.get(&1).unwrap().len(), 3);
        assert_eq!(chunk.presence.get(&0), Some(&vec![true, true, true]));
        assert_eq!(chunk.presence.get(&1), Some(&vec![false, true, false]));

        // 重写后 null 仍保真（不再出现「列短于 timestamps → v[p] 越界」）。
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);
        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write");
        let read_rows = SacReader::read_range(
            &path,
            sensearcdb_common::TenantId::DEFAULT,
            1,
            0,
            500,
            &[0, 1],
            None,
        )
        .expect("read");

        assert_eq!(read_rows.len(), 3);
        assert_eq!(read_rows[0].values, vec![(0, TypedValue::F64(1.0))]);
        assert_eq!(
            read_rows[1].values,
            vec![(0, TypedValue::F64(2.0)), (1, TypedValue::F64(20.0))]
        );
        assert_eq!(read_rows[2].values, vec![(0, TypedValue::F64(3.0))]);
    }

    /// 不变式 I3：final 后分区恰好 1 个 L4 文件。
    ///
    /// 用默认阈值（16）让 L0→L1 不触发，隔离 final 路径：3 个 L0 → 1 个 L4。
    #[tokio::test]
    async fn test_finalize_partition_collapses_to_single_l2() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        // 默认 16 阈值：3 个 L0 不会触发 L0→L1，只触发 final。
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition_ns = 1_000_i64;
        let a = write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        let b = write_l0_segment_with(&sac_dir, 42, partition_ns, 2, vec![200], vec![2.0], None);
        let c = write_l0_segment_with(&sac_dir, 42, partition_ns, 3, vec![300], vec![3.0], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();
        assert!(a.exists() && b.exists() && c.exists());

        scheduler.run_once().await;

        assert!(
            !a.exists() && !b.exists() && !c.exists(),
            "源 L0 应被 final 合并删除"
        );
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1, "final 后分区应恰好 1 个文件");
        let header = SacReader::read_header(&files[0]).unwrap();
        assert_eq!(header.level, SacLevel::L4, "final 产物必须是 L4");
    }

    /// 不变式 I4：final 幂等——重复触发 no-op，分区仍是恰好 1 个 L4。
    #[tokio::test]
    async fn test_finalize_partition_idempotent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition_ns = 2_000_i64;
        let a = write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();

        scheduler.run_once().await;
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1);
        let before = std::fs::metadata(&files[0]).unwrap().len();

        // 第二次 run_once：已是单 L4 → no-op（不重写、不新增、不报错）。
        scheduler.run_once().await;
        let files2 = list_sac_files(&tenant_dir);
        assert_eq!(files2.len(), 1, "final 幂等：仍恰好 1 个 L4");
        assert_eq!(files2, files, "final 幂等：文件路径不变（未重写）");
        let after = std::fs::metadata(&files2[0]).unwrap().len();
        assert_eq!(before, after, "final 幂等：文件大小不变（未重写）");
    }

    /// 不变式 I6：late 旁路 L0 写入 + 惰性 re-final。
    ///
    /// 分区 final 成单 L4 后，晚到数据写旁路 L0（同分区目录），下一次 final 触发
    /// 把「原 L4 + 旁路 L0」重新收敛成新的单 L4（copy-on-write，不 in-place 改 L4）。
    #[tokio::test]
    async fn test_late_bypass_l0_and_refinal() {
        use crate::memtable::col_vec::TypedValue;
        use sensearcdb_common::align_to_partition;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        // 用「3 天前」的天边界分区：已过 grace（1 天）、又落在 max_late_window（7 天）内，
        // 保证 re-final 不被硬边界跳过。
        let now_ns = current_epoch_ns();
        let three_days_ns = 3_i64 * 86_400_000_000_000_i64;
        let partition_ns = align_to_partition(now_ns - three_days_ns, 86_400_000, 28_800);

        // 初次 final：ts=100 val=1.0 → 单 L4。
        let first =
            write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        let tenant_dir = first.parent().unwrap().to_path_buf();
        scheduler.run_once().await;
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1);
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );

        // late 旁路 L0：同分区目录写 ts=100 val=9.0（更新覆盖）。
        let late = write_l0_segment_with(
            &sac_dir,
            42,
            partition_ns,
            2,
            vec![100],
            vec![9.0],
            Some(100),
        );
        assert!(late.exists());
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 2, "late 旁路 L0 与已有 L2 并存");

        // 惰性 re-final：下一次触发把 L4 + 旁路 L0 重新收敛成单 L4。
        scheduler.run_once().await;
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1, "re-final 后应回到恰好 1 个 L4");
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );

        // 读回：late 数据（wal_seq 更高）应覆盖初次数据。
        let rows = SacReader::read_range(
            &files[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts, 100);
        assert_eq!(rows[0].values, vec![(0, TypedValue::F64(9.0))]);
    }

    /// QA 独立验证（I6 数据正确性）：late 写入同时包含「既有 ts 覆盖 + 新 ts 追加」时，
    /// re-final 后单 L4 内数据必须：覆盖点取 late 新值、新增点保留、无丢无重。
    #[tokio::test]
    async fn test_late_refinal_data_correctness_overwrite_and_append() {
        use crate::memtable::col_vec::TypedValue;
        use sensearcdb_common::align_to_partition;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        // 用 3 天前的天边界分区：已过 grace(1天)、又在 max_late_window(7天) 内。
        let now_ns = current_epoch_ns();
        let partition_ns =
            align_to_partition(now_ns - 3 * 86_400_000_000_000_i64, 86_400_000, 28_800);

        // 初次 final：ts 100/200/300 → 1.0/2.0/3.0。
        let first = write_l0_segment_with(
            &sac_dir,
            42,
            partition_ns,
            1,
            vec![100, 200, 300],
            vec![1.0, 2.0, 3.0],
            None,
        );
        let tenant_dir = first.parent().unwrap().to_path_buf();
        scheduler.run_once().await;
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1);
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );

        // late 旁路 L0：覆盖 ts=200→20.0；新增 ts=150→15.0、ts=250→25.0。
        let late = write_l0_segment_with(
            &sac_dir,
            42,
            partition_ns,
            2,
            vec![150, 200, 250],
            vec![15.0, 20.0, 25.0],
            Some(1000),
        );
        assert!(late.exists());
        assert_eq!(list_sac_files(&tenant_dir).len(), 2, "late 与 L2 并存");

        // 惰性 re-final。
        scheduler.run_once().await;
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1, "re-final 后回到单 L4");
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );

        let mut rows = SacReader::read_range(
            &files[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);
        let tss: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        assert_eq!(tss, vec![100, 150, 200, 250, 300], "无丢无重");
        let vals: Vec<f64> = rows
            .iter()
            .map(|r| r.values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0))
            .collect();
        assert_eq!(
            vals,
            vec![1.0, 15.0, 20.0, 25.0, 3.0],
            "覆盖点取 late 新值，新增点保留"
        );
    }

    /// 不变式 I4 边界：final 只在「grace 到期」后执行。当天分区（partition_ns 对齐
    /// 到今日天边界）即使有多个 L0 也不应被 final（grace 未到期）。
    #[tokio::test]
    async fn test_final_waits_for_grace() {
        use sensearcdb_common::align_to_partition;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let now_ns = current_epoch_ns();
        let today = align_to_partition(now_ns, 86_400_000, 28_800);
        let ts0 = today + 1;
        let a = write_l0_segment_with(&sac_dir, 42, today, 1, vec![ts0], vec![1.0], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();

        scheduler.run_once().await;

        // grace 未到期：不应产出 L2。L0 仍在（默认 16 阈值下 L0→L1 也不触发）。
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1, "grace 未到期时不应 final，也不应触发 L0→L1");
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L0
        );
    }

    // ===================== QA 独立验证（final 两修复：write_epoch 判窗 + final 解耦）=====================

    /// 写一个 L0 SAC 文件，显式指定 write_epoch（覆盖「历史文件缺 write_epoch == 0」场景）。
    #[allow(clippy::too_many_arguments)]
    fn write_l0_segment_with_epoch(
        sac_dir: &Path,
        table_id: u32,
        partition_ns: i64,
        seq: u16,
        timestamps: Vec<i64>,
        values: Vec<f64>,
        wal_seq: Option<u64>,
        write_epoch_ns: i64,
    ) -> PathBuf {
        let writer = SacWriter::new(sac_dir, CompressionType::Lz4);
        let point_count = timestamps.len();
        let ts_index: std::collections::BTreeMap<i64, usize> = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| (*ts, i))
            .collect();
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(values));
        let chunk = ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps,
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; point_count],
            ts_index,
            point_count,
            out_of_order: false,
            max_wal_seq: wal_seq,
            min_wal_seq: None,
        };
        writer
            .write_chunk_with_epoch(
                &chunk,
                partition_ns,
                SacLevel::L0,
                seq,
                wal_seq,
                write_epoch_ns,
            )
            .expect("write l0 with epoch")
    }

    /// 轮询直到 `cond` 为真或超时（期间 sleep 让出 tokio，驱动后台 spawn 任务推进）。
    async fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if cond() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// write_epoch 判窗底层：u64→i64 饱和转换边界。
    #[test]
    fn test_write_epoch_to_wall_ns_saturation() {
        assert_eq!(write_epoch_to_wall_ns(0), 0);
        assert_eq!(
            write_epoch_to_wall_ns(1_700_000_000_000_000_000),
            1_700_000_000_000_000_000
        );
        assert_eq!(write_epoch_to_wall_ns(i64::MAX as u64), i64::MAX);
        assert_eq!(write_epoch_to_wall_ns(u64::MAX), i64::MAX);
    }

    /// write_epoch 判窗底层：scan_sac_dir 对 L0/L1 取 max(write_epoch) → last_write_wall_ns；
    /// L4（final 产物）不计入；write_epoch==0 历史文件按 0 保守处理。
    #[test]
    fn test_scan_sac_dir_last_write_wall_ns_from_write_epoch() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let partition_ns = 1_000_i64;
        let now = current_epoch_ns();
        let older = now - 86_400_000_000_000_i64; // now 往前 1 天

        let _a = write_l0_segment_with_epoch(
            &sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None, older,
        );
        let _b = write_l0_segment_with_epoch(
            &sac_dir, 42, partition_ns, 2, vec![200], vec![2.0], None, now,
        );

        let groups = scan_sac_dir(&sac_dir).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].last_write_wall_ns, now,
            "last_write_wall_ns 应取 L0 的 max(write_epoch)"
        );
        assert_eq!(groups[0].l0.len(), 2);

        // 单独旧文件（write_epoch=0）→ last_write_wall_ns == 0（保守放行依据）。
        let tmp2 = tempfile::TempDir::new().unwrap();
        let sac_dir2 = tmp2.path().join("sac");
        write_l0_segment_with_epoch(
            &sac_dir2, 42, partition_ns, 1, vec![100], vec![1.0], None, 0,
        );
        let groups2 = scan_sac_dir(&sac_dir2).unwrap();
        assert_eq!(
            groups2[0].last_write_wall_ns, 0,
            "write_epoch==0 的历史文件应保守按 0 处理"
        );
    }

    /// 问题2 核心：回填数据（数据时间 2016，远早于 max_late_window=7 天）在
    /// write_epoch==落盘时刻(now) 下必须能 re-final 收敛成单 L4，不再被 wall-clock
    /// 判窗误跳过（旧逻辑 now(2026)-data(2016) >> 7d 会恒跳过 → 残留 1×L2+1×L0）。
    #[tokio::test]
    async fn test_backfill_old_partition_refinal_converges() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        // 数据时间 = 2016-01-01 00:00:00 UTC（10 年前）。
        let partition_ns = 1_451_606_400_000_000_000_i64;

        // 初次 final：ts 100/200/300 → 1.0/2.0/3.0 → 单 L4。
        let first = write_l0_segment_with(
            &sac_dir, 42, partition_ns, 1,
            vec![100, 200, 300], vec![1.0, 2.0, 3.0], None,
        );
        let tenant_dir = first.parent().unwrap().to_path_buf();
        scheduler.run_once().await;
        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1);
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );

        // late 回填旁路 L0（write_epoch=now，数据时间仍 2016）：覆盖 ts=200→20.0，新增 150/250。
        let late = write_l0_segment_with(
            &sac_dir, 42, partition_ns, 2,
            vec![150, 200, 250], vec![15.0, 20.0, 25.0], Some(1000),
        );
        assert!(late.exists());
        assert_eq!(list_sac_files(&tenant_dir).len(), 2, "late 与 L2 并存");

        // 惰性 re-final：必须把 L4 + late L0 重新收敛成单 L4。
        scheduler.run_once().await;
        let files = list_sac_files(&tenant_dir);
        assert_eq!(
            files.len(), 1,
            "回填数据 re-final 必须收敛到单 L4，而非残留 1×L4+1×L0"
        );
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );

        let mut rows = SacReader::read_range(
            &files[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);
        let tss: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        assert_eq!(tss, vec![100, 150, 200, 250, 300], "无丢无重");
        let vals: Vec<f64> = rows
            .iter()
            .map(|r| r.values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0))
            .collect();
        assert_eq!(
            vals,
            vec![1.0, 15.0, 20.0, 25.0, 3.0],
            "覆盖点取 late 新值，新增点保留"
        );
    }

    /// 问题2 边界：late L0 的 write_epoch==0（历史文件无此字段）→ last_write_wall_ns==0，
    /// final 保守放行（不因「0 与 now 差值超窗」被误跳过），re-final 仍收敛单 L4。
    #[tokio::test]
    async fn test_refinal_write_epoch_zero_conservative_pass() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition_ns = 1_451_606_400_000_000_000_i64;
        let first = write_l0_segment_with(
            &sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None,
        );
        let tenant_dir = first.parent().unwrap().to_path_buf();
        scheduler.run_once().await;
        assert_eq!(list_sac_files(&tenant_dir).len(), 1);

        // late L0 但 write_epoch==0（模拟历史文件缺字段）。
        let late = write_l0_segment_with_epoch(
            &sac_dir, 42, partition_ns, 2, vec![200], vec![2.0], None, 0,
        );
        assert!(late.exists());

        scheduler.run_once().await;
        let files = list_sac_files(&tenant_dir);
        assert_eq!(
            files.len(), 1,
            "write_epoch==0 应保守放行，re-final 收敛单 L4"
        );
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );

        let mut rows = SacReader::read_range(
            &files[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);
        let tss: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        assert_eq!(tss, vec![100, 200], "无丢无重");
    }

    /// 问题3 核心：生产路径 run_tick 走 detached final——final 后台跑，主循环不被阻塞；
    /// 分区最终仍收敛单 L4，finalizing 集合正确 claim/release。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_detached_final_converges_and_claims_releases() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition_ns = 1_451_606_400_000_000_000_i64;
        let a = write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        let b = write_l0_segment_with(&sac_dir, 42, partition_ns, 2, vec![200], vec![2.0], None);
        let c = write_l0_segment_with(&sac_dir, 42, partition_ns, 3, vec![300], vec![3.0], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();
        assert!(a.exists() && b.exists() && c.exists());

        scheduler.run_tick().await;

        let ok = wait_until(Duration::from_secs(10), || {
            list_sac_files(&tenant_dir).len() == 1 && !scheduler.is_finalizing(0, partition_ns)
        })
        .await;
        assert!(
            ok,
            "detached final 应在超时内收敛单 L4 并释放 finalizing"
        );

        let files = list_sac_files(&tenant_dir);
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );
        let mut rows = SacReader::read_range(
            &files[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);
        let tss: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        assert_eq!(tss, vec![100, 200, 300], "无丢无重");
    }

    /// 问题3 解耦路径：同一轮内 L0→L1（await）先跑，随后 schedule_finalization_detached
    /// 因 l0_triggered=true 重扫磁盘，final 源必须引用「新 L1」而非已删除的 L0，
    /// 最终收敛单 L4 且无 stale-source 报错/丢数据（copy-on-write 不变式）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_run_tick_l0_to_l1_then_detached_final() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 2; // 2 个 L0 即触发 L0→L1
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition_ns = 1_451_606_400_000_000_000_i64;
        let a = write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        let b = write_l0_segment_with(&sac_dir, 42, partition_ns, 2, vec![200], vec![2.0], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();
        assert!(a.exists() && b.exists());

        scheduler.run_tick().await;

        let ok = wait_until(Duration::from_secs(10), || {
            list_sac_files(&tenant_dir).len() == 1 && !scheduler.is_finalizing(0, partition_ns)
        })
        .await;
        assert!(ok, "同轮 L0→L1 + final 应在超时内收敛单 L4");

        let files = list_sac_files(&tenant_dir);
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L4
        );
        assert!(!a.exists() && !b.exists(), "源 L0 应被合并删除");
        let rows = SacReader::read_range(
            &files[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 2, "无丢无重");
    }

    /// 问题3 结构护栏：schedule_finalization_detached 仅 claim + spawn，立即返回
    /// （不 await ZSTD9 重活）。current_thread 运行时下 spawn 任务尚未被驱动，
    /// 返回后 finalizing 集合必须仍非空——证明主循环不被 final 阻塞（L0 不堆积）。
    #[tokio::test]
    async fn test_schedule_finalization_detached_returns_without_blocking() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition_ns = 1_451_606_400_000_000_000_i64;
        let l0 = write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        let tenant_dir = l0.parent().unwrap().to_path_buf();
        assert!(l0.exists());

        scheduler.scan_files().await;
        assert!(scheduler.finalizing.lock().unwrap().is_empty());

        // rescan=false：函数体无 await 点，同步 claim + spawn 后立即返回。
        scheduler.schedule_finalization_detached(false).await;

        assert!(
            scheduler.is_finalizing(0, partition_ns),
            "detached final 必须立即返回（claim 后在后台执行），不得阻塞主循环"
        );

        // 驱动后台任务完成，验证最终收敛 + 释放。
        let ok = wait_until(Duration::from_secs(10), || {
            list_sac_files(&tenant_dir).len() == 1 && !scheduler.is_finalizing(0, partition_ns)
        })
        .await;
        assert!(ok, "后台 final 应最终完成并释放 finalizing");
    }

    /// final 与 L0→L1 同分区互斥：finalizing 集合 claim 后，L0→L1 必须跳过该分区
    /// （不删源文件）；释放后恢复合并。这是「copy-on-write 无并发删源」的关键护栏。
    #[tokio::test]
    async fn test_l0_to_l1_skips_finalizing_partition() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_default_file_count = 1;
        let handle = ConfigHandle::new(cfg);
        let scheduler = CompactionScheduler::new(&handle);

        let partition_ns = 1_000_i64;
        let l0 = write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        assert!(l0.exists());

        // claim：模拟 final 正在后台合并该分区。
        scheduler.finalizing.lock().unwrap().insert((0, partition_ns));
        scheduler.scan_files().await;
        scheduler.execute_l0_to_l1().await.unwrap();

        assert!(
            l0.exists(),
            "finalizing 期间 L0→L1 必须跳过该分区，源 L0 不得被删"
        );

        // release：final 完成。
        scheduler.finalizing.lock().unwrap().remove(&(0, partition_ns));
        scheduler.scan_files().await;
        scheduler.execute_l0_to_l1().await.unwrap();

        assert!(!l0.exists(), "释放后 L0→L1 恢复合并，源 L0 应被删除并产出 L1");
        let files = list_sac_files(&sac_dir);
        assert_eq!(files.len(), 1);
        assert_eq!(
            SacReader::read_header(&files[0]).unwrap().level,
            SacLevel::L1
        );
    }

    /// 迁移停摆门（detached final 前置门）：pause 后即使已 claim + spawn，后台 final
    /// 也应在真正开始 ZSTD9 merge 前退出——源文件保留、finalizing 释放（不丢数据、
    /// 不浪费 CPU）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_detached_final_paused_skips_merge_and_keeps_source() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition_ns = 1_451_606_400_000_000_000_i64;
        let l0 = write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        let tenant_dir = l0.parent().unwrap().to_path_buf();
        assert!(l0.exists());

        // 先扫描让 final 候选可见；再 pause；然后触发 detached final。
        scheduler.scan_files().await;
        scheduler.pause();
        scheduler.schedule_finalization_detached(false).await;

        let ok = wait_until(Duration::from_secs(10), || {
            !scheduler.is_finalizing(0, partition_ns)
        })
        .await;
        assert!(ok, "paused 后后台 final 应释放 finalizing");

        assert!(
            l0.exists(),
            "pause 期间 detached final 不得删源（源文件保留，resume 后重新收敛）"
        );
        assert_eq!(
            list_sac_files(&tenant_dir).len(),
            1,
            "pause 前置门应在 merge 前拦截，不产出新文件"
        );
    }

    /// 迁移停摆门（detached final 删源前置门）：merge 中途被 pause 时，
    /// `merge_paths_to_partition_blocking` 在 finish rename 后、`delete_files` 前读到
    /// `paused==true`，必须保留源文件（输出文件可留，resume 后下一轮重新收敛）。
    #[test]
    fn test_blocking_merge_paused_skips_source_delete() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let a = write_l0_segment_with(&sac_dir, 42, 1_000, 1, vec![100], vec![1.0], None);
        let b = write_l0_segment_with(&sac_dir, 42, 1_000, 2, vec![200], vec![2.0], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();
        let paths = vec![a.clone(), b.clone()];
        assert!(a.exists() && b.exists());

        let paused = AtomicBool::new(true);
        let merged = merge_paths_to_partition_blocking(
            sac_dir.clone(),
            None,
            0,
            1_000,
            &paths,
            SacLevel::L2,
            CompressionType::Zstd9,
            &paused,
        )
        .unwrap();

        assert_eq!(merged, 0, "pause 门应返回 0（未删源，视为未完成）");
        assert!(
            a.exists() && b.exists(),
            "pause 期间绝不删源（源文件保留）"
        );
        assert_eq!(
            list_sac_files(&tenant_dir).len(),
            3,
            "merge 已完成输出 L2，但 2 个源文件必须保留（resume 后重新收敛）"
        );
    }

    // ===================== QA 独立验证（detached final 不检查 is_paused() 修复） =====================

    /// QA 独立验证（时序②：merge 中途 pause → 删源门拦截 → resume 重新收敛）。
    ///
    /// 中途 pause 后：源 L0(a,b) 保留 + 已落盘 L2 输出 1 个（共 3 文件）。resume 后
    /// 下一轮 re-final 必须把这 3 个文件重新收敛成恰好 1 个 L2，数据无丢无重、
    /// 无重复/残留文件（幂等 I3/I4 + 路径不 overlap 不误删新文件）。
    #[test]
    fn qa_pause_midmerge_then_resume_reconverges() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let a = write_l0_segment_with(&sac_dir, 42, 1_000, 1, vec![100], vec![1.0], None);
        let b = write_l0_segment_with(&sac_dir, 42, 1_000, 2, vec![200], vec![2.0], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();
        assert!(a.exists() && b.exists());

        // 1) pause 置位：模拟 merge 已跑完、finish 已 rename、删源前被 pause。
        let paused = AtomicBool::new(true);
        let merged = merge_paths_to_partition_blocking(
            sac_dir.clone(),
            None,
            0,
            1_000,
            &[a.clone(), b.clone()],
            SacLevel::L2,
            CompressionType::Zstd9,
            &paused,
        )
        .unwrap();
        assert_eq!(merged, 0, "pause 门应返回 0（未删源）");
        assert!(a.exists() && b.exists(), "pause 期间绝不删源");
        let files_after_pause = list_sac_files(&tenant_dir);
        assert_eq!(
            files_after_pause.len(),
            3,
            "源 2 个 L0 + 已落盘 L2 输出 1 个（copy-on-write，路径不 overlap）"
        );

        // 2) resume：paused=false，re-final 把全部 3 个文件收敛成单 L2。
        let paused2 = AtomicBool::new(false);
        let merged2 = merge_paths_to_partition_blocking(
            sac_dir.clone(),
            None,
            0,
            1_000,
            &files_after_pause,
            SacLevel::L2,
            CompressionType::Zstd9,
            &paused2,
        )
        .unwrap();
        assert_eq!(merged2, 3, "resume 后应删除全部 3 个源并产出 1 个 L2");

        let files_final = list_sac_files(&tenant_dir);
        assert_eq!(files_final.len(), 1, "resume 后必须收敛到恰好 1 个 L2");
        assert_eq!(
            SacReader::read_header(&files_final[0]).unwrap().level,
            SacLevel::L2,
            "resume 后最终产物必须是 L2"
        );

        let mut rows = SacReader::read_range(
            &files_final[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);
        let tss: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        assert_eq!(tss, vec![100, 200], "resume 后无丢无重");
        let vals: Vec<f64> = rows
            .iter()
            .map(|r| r.values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0))
            .collect();
        assert_eq!(vals, vec![1.0, 2.0], "resume 后数据逐点一致");
    }

    /// QA 独立验证（时序①：spawn 后、merge 前 pause → 前置门拦截 → resume 收敛）。
    ///
    /// 覆盖 detached final 的完整生产路径：pause 前置门必须「释放 finalizing + 不删源 +
    /// 不产出新文件」；resume 后 run_tick 重新 claim + 收敛单 L4，数据无丢无重。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn qa_pause_before_merge_then_resume_converges() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition_ns = 1_451_606_400_000_000_000_i64;
        let a = write_l0_segment_with(&sac_dir, 42, partition_ns, 1, vec![100], vec![1.0], None);
        let b = write_l0_segment_with(&sac_dir, 42, partition_ns, 2, vec![200], vec![2.0], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();
        assert!(a.exists() && b.exists());

        // pause 在 spawn 之前置位，前置门应同步拦截。
        scheduler.scan_files().await;
        scheduler.pause();
        assert!(scheduler.is_paused());
        scheduler.schedule_finalization_detached(false).await;

        let ok = wait_until(Duration::from_secs(10), || {
            !scheduler.is_finalizing(0, partition_ns)
        })
        .await;
        assert!(ok, "pause 前置门必须释放 finalizing");
        assert!(a.exists() && b.exists(), "pause 前置门不得删源");
        assert_eq!(
            list_sac_files(&tenant_dir).len(),
            2,
            "前置门在 merge 前拦截，不产出新文件"
        );

        // resume：run_tick 重新 claim + final 收敛单 L4。
        scheduler.resume();
        assert!(!scheduler.is_paused());
        scheduler.run_tick().await;

        let ok2 = wait_until(Duration::from_secs(10), || {
            list_sac_files(&tenant_dir).len() == 1 && !scheduler.is_finalizing(0, partition_ns)
        })
        .await;
        assert!(ok2, "resume 后 detached final 必须收敛单 L4 并释放 finalizing");

        let files = list_sac_files(&tenant_dir);
        assert_eq!(files.len(), 1, "resume 后恰好 1 个 L4");
        assert_eq!(SacReader::read_header(&files[0]).unwrap().level, SacLevel::L4);

        let mut rows = SacReader::read_range(
            &files[0],
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);
        let tss: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        assert_eq!(tss, vec![100, 200], "resume 后无丢无重");
    }

    /// QA 独立验证（时序③：pause 置位后 spawn 的 detached final 绝不删源 + 空结果分支）。
    ///
    /// 覆盖 `builder == None`（源文件无有效数据）时，pause 门仍须保留源文件，
    /// 不能走到 `delete_files`（删空源文件是「不 pause」时才允许的行为）。
    #[test]
    fn qa_pause_empty_source_still_keeps_source() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        // 空点 L0：无有效数据行 → merge 的 builder 保持 None。
        let a = write_l0_segment_with(&sac_dir, 42, 1_000, 1, vec![], vec![], None);
        let tenant_dir = a.parent().unwrap().to_path_buf();
        assert!(a.exists());

        let paused = AtomicBool::new(true);
        let merged = merge_paths_to_partition_blocking(
            sac_dir.clone(),
            None,
            0,
            1_000,
            &[a.clone()],
            SacLevel::L2,
            CompressionType::Zstd9,
            &paused,
        )
        .unwrap();
        assert_eq!(merged, 0, "空结果 + pause 也必须返回 0");
        assert!(a.exists(), "空结果 + pause 也绝不删源（删空源仅在未 pause 时允许）");
        assert_eq!(list_sac_files(&tenant_dir).len(), 1, "空结果 + pause 不产出新文件");

        // 对照组：未 pause 时空结果按 D4 删除空源文件。
        let paused2 = AtomicBool::new(false);
        let merged2 = merge_paths_to_partition_blocking(
            sac_dir.clone(),
            None,
            0,
            1_000,
            &[a.clone()],
            SacLevel::L2,
            CompressionType::Zstd9,
            &paused2,
        )
        .unwrap();
        assert_eq!(merged2, 1, "未 pause 时空结果应删除空源文件");
        assert!(!a.exists(), "未 pause 时空源文件应被删除");
    }

    // ===================== 四刀改造测试盲区补强（QA 独立验证发现） =====================

    /// 用指定压缩/层级写一个单列 F64 SAC 文件（供直接构造 Zstd9 的 L1/L2 源文件）。
    fn write_sac_segment(
        sac_dir: &Path,
        compression: CompressionType,
        level: SacLevel,
        table_id: u32,
        partition_ns: i64,
        seq: u16,
        timestamps: Vec<i64>,
        values: Vec<f64>,
    ) -> PathBuf {
        let writer = SacWriter::new(sac_dir, compression);
        let point_count = timestamps.len();
        let ts_index: std::collections::BTreeMap<i64, usize> = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| (*ts, i))
            .collect();
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(values));
        let chunk = ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps,
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; point_count],
            ts_index,
            point_count,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        };
        writer
            .write_chunk(&chunk, partition_ns, level, seq, None)
            .expect("write sac segment")
    }

    /// 递归收集指定目录下某层级的 `.sac` 文件（按路径字典序排序，保证确定性）。
    fn list_files_by_level(dir: &Path, level: SacLevel) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for p in list_sac_files(dir) {
            if let Some(fname) = p.file_name().and_then(|s| s.to_str()) {
                if let Some((_pn, lvl, _seq)) = SacNaming::parse_file_name(fname) {
                    if lvl == level {
                        out.push(p);
                    }
                }
            }
        }
        out.sort();
        out
    }

    /// 从 SAC 原始字节解析 Footer。
    fn read_footer(raw: &[u8]) -> SacFooter {
        let footer_size = sensearcdb_common::constants::SAC_FOOTER_SIZE;
        SacFooter::from_bytes(
            &raw[raw.len() - footer_size..]
                .try_into()
                .expect("footer slice"),
        )
        .expect("parse footer")
    }

    /// 从 SAC 原始字节解析全部 TableIndex 条目。
    fn read_table_index_entries(raw: &[u8], footer: &SacFooter) -> Vec<crate::sac::table_index::TableIndexEntry> {
        let entry_size = sensearcdb_common::constants::SAC_TABLE_INDEX_ENTRY_SIZE;
        let start = footer.table_index_offset as usize;
        let mut entries = Vec::new();
        for i in 0..footer.table_count as usize {
            let s = start + i * entry_size;
            let e = s + entry_size;
            let entry = crate::sac::table_index::TableIndexEntry::from_bytes(
                &raw[s..e].try_into().expect("table entry slice"),
            )
            .expect("parse table entry");
            entries.push(entry);
        }
        entries
    }

    /// 校验整文件 CRC（header + blocks + L2 + TableIndex == footer.crc32c）。
    fn footer_crc_valid(raw: &[u8], footer: &SacFooter) -> bool {
        use std::hash::Hasher;
        let footer_size = sensearcdb_common::constants::SAC_FOOTER_SIZE;
        let mut hasher = crc32c::Crc32cHasher::new(0);
        hasher.write(&raw[..raw.len() - footer_size]);
        hasher.finish() as u32 == footer.crc32c
    }

    /// 刀3 补测：`append_raw_block` 非重叠 block 逐字节复制路径（此前零覆盖）。
    ///
    /// 构造真实 Zstd9 L1 源（先 L0→L1 归并），触发 L1→L2 的 block 拼接复制分支，
    /// 断言：复制路径真被走到（逐块字节 == 源）、row_count/table_count/block_count
    /// 正确、footer CRC 正确、读回逐点一致。
    #[tokio::test]
    async fn test_generational_block_splice_copy_path() {
        use crate::memtable::col_vec::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        cfg.engine.compaction_l1_file_count = 1; // 1 个 L1 即触发 L1→L2（隔离复制路径）
        let handle = ConfigHandle::new(cfg);
        let scheduler = CompactionScheduler::new(&handle);

        // 1) 构造真实 Zstd9 L1 源：写 L0 → execute_l0_to_l1 归并。
        let n = 5000usize; // 2 个 block（4096 + 904）
        let ts: Vec<i64> = (1..=n as i64).collect();
        let vals: Vec<f64> = ts.iter().map(|&t| t as f64).collect();
        let l0 = write_l0_segment_with(&sac_dir, 42, 100, 1, ts, vals, None);
        assert!(l0.exists());

        scheduler.scan_files().await;
        scheduler.execute_l0_to_l1().await.expect("L0→L1");

        let l1_files = list_files_by_level(&sac_dir, SacLevel::L1);
        assert_eq!(l1_files.len(), 1, "L0→L1 应产出 1 个 L1");
        let src = &l1_files[0];
        assert_eq!(
            SacReader::read_header(src).unwrap().level.compression(),
            CompressionType::Zstd9,
            "L1 源必须是 Zstd9，才命中 block 复制分支（source_compression == 目标压缩）"
        );

        let src_l2 = SacReader::read_l2_descriptors(src).unwrap();
        assert_eq!(src_l2.len(), 2, "5000 行应产出 2 个 block");
        let src_raw = std::fs::read(src).unwrap();
        let src_footer = read_footer(&src_raw);
        assert_eq!(src_footer.table_count, 1);
        assert_eq!(src_footer.block_count, 2);
        let src_blocks: Vec<Vec<u8>> = src_l2
            .iter()
            .map(|d| SacReader::read_block_bytes(src, d.block_offset, d.block_size).unwrap())
            .collect();

        // 2) 触发 L1→L2（阈值 1），走 block 拼接复制路径。
        scheduler.scan_files().await;
        let merged = scheduler.execute_l1_to_l2().await.expect("L1→L2");
        assert_eq!(merged, 1, "应合并 1 个源 L1");

        let l2_files = list_files_by_level(&sac_dir, SacLevel::L2);
        assert_eq!(l2_files.len(), 1, "L1→L2 应产出 1 个 L2");
        let dst = &l2_files[0];
        let dst_l2 = SacReader::read_l2_descriptors(dst).unwrap();
        assert_eq!(dst_l2.len(), 2, "复制路径应保持 block 数 == 源（2）");

        let dst_raw = std::fs::read(dst).unwrap();
        let dst_footer = read_footer(&dst_raw);
        assert_eq!(dst_footer.table_count, 1, "footer.table_count 应与源一致");
        assert_eq!(dst_footer.block_count, 2, "footer.block_count 应与源一致");

        // 3a) 复制路径真被走到：目标逐块字节 == 源逐块字节（append_raw_block 逐字节复制，
        //     而非 splice_dedup_run 的降级重编码——重编码会改变 zstd 帧字节）。
        for (i, d) in dst_l2.iter().enumerate() {
            let dst_bytes = SacReader::read_block_bytes(dst, d.block_offset, d.block_size).unwrap();
            assert_eq!(
                dst_bytes, src_blocks[i],
                "block {i} 必须逐字节复制（非重编码），复制路径未命中"
            );
        }

        // 3b) 复制块读 48B mini-header 补的 row_count 正确 → TableIndex 重建正确。
        let dst_ti = read_table_index_entries(&dst_raw, &dst_footer);
        assert_eq!(dst_ti.len(), 1, "单表应产出 1 条 TableIndex");
        assert_eq!(dst_ti[0].table_id, 42);
        assert_eq!(dst_ti[0].block_count, 2, "TableIndex.block_count 应 == 2");
        assert_eq!(
            dst_ti[0].row_count, n as u32,
            "TableIndex.row_count 应由复制块 mini-header(offset 6..10) 正确重建"
        );

        // 3c) footer CRC 正确（读回能通过整文件 CRC 校验）。
        assert!(footer_crc_valid(&dst_raw, &dst_footer), "L2 footer CRC 必须正确");

        // 3d) 拼接后 L2 读回逐点 == 源（数据不丢不错序）。
        let rows = SacReader::read_range(
            dst,
            sensearcdb_common::TenantId::DEFAULT,
            42,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), n, "复制后读回应无丢无重");
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.ts, (i + 1) as i64, "ts 必须升序且不丢");
            assert_eq!(
                r.values,
                vec![(0, TypedValue::F64((i + 1) as f64))],
                "值必须逐点一致"
            );
        }
    }

    /// 刀1 补测：分代收敛端到端（L0→L1→L2→L3→final→L4）。
    ///
    /// 灌数据 → 16 L0 → L0→L1 → 1 L1 → 凑 4 L1 → L1→L2 → 1 L2 → 凑 4 L2 →
    /// L2→L3 → 1 L3 → final → 1 L4，断言 16→4→4 收敛与 is_finalized()。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_generational_convergence_end_to_end() {
        use crate::memtable::col_vec::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        // 保持默认阈值：16（L0→L1）/ 4（L1→L2）/ 4（L2→L3）。
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition = 100i64;
        let table = 42u32;
        let mut next_ts = 1i64;
        let mut total_rows = 0usize;

        // ── 阶段 1：16 个 L0 → L0→L1 → 1 个 L1（16:1）──
        for seq in 1..=16u16 {
            let t: Vec<i64> = (next_ts..next_ts + 5).collect();
            let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
            next_ts += 5;
            total_rows += 5;
            write_sac_segment(
                &sac_dir, CompressionType::Lz4, SacLevel::L0, table, partition, seq, t, v,
            );
        }
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L0).len(), 16);
        scheduler.scan_files().await;
        assert!(
            scheduler.check_triggers(CompactionTarget::L0ToL1),
            "16 个 L0 应触发 L0→L1（阈值 16）"
        );
        scheduler.execute_l0_to_l1().await.expect("L0→L1");
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L0).len(), 0);
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L1).len(),
            1,
            "16 L0 → 1 L1"
        );

        // ── 阶段 2：再补 3 个 L1（直接构造 Zstd9），凑 4 个 → L1→L2（4:1）──
        for seq in 2..=4u16 {
            let t: Vec<i64> = (next_ts..next_ts + 5).collect();
            let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
            next_ts += 5;
            total_rows += 5;
            write_sac_segment(
                &sac_dir, CompressionType::Zstd9, SacLevel::L1, table, partition, seq, t, v,
            );
        }
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L1).len(), 4);
        scheduler.scan_files().await;
        let merged = scheduler.execute_l1_to_l2().await.expect("L1→L2");
        assert_eq!(merged, 4, "4 个 L1 应合并为 1 个 L2");
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L1).len(), 0);
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L2).len(),
            1,
            "4 L1 → 1 L2"
        );

        // ── 阶段 3：再补 3 个 L2（直接构造 Zstd9），凑 4 个 → L2→L3（4:1，封顶）──
        for seq in 2..=4u16 {
            let t: Vec<i64> = (next_ts..next_ts + 5).collect();
            let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
            next_ts += 5;
            total_rows += 5;
            write_sac_segment(
                &sac_dir, CompressionType::Zstd9, SacLevel::L2, table, partition, seq, t, v,
            );
        }
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L2).len(), 4);

        // 阶段 3：4 个 L2 → L2→L3（detach）
        scheduler.scan_files().await;
        scheduler.schedule_l2_to_l3_detached();
        let ok = wait_until(Duration::from_secs(10), || {
            list_files_by_level(&sac_dir, SacLevel::L3).len() == 1
                && !scheduler.is_l2_to_l3_inflight(0, partition)
        })
        .await;
        assert!(ok, "L2→L3 应在超时内产出 1 个 L3 并释放 inflight");
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L2).len(), 0);
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L3).len(),
            1,
            "4 L2 → 1 L3"
        );

        // 封顶语义：无 L2 时 schedule 不 claim
        scheduler.scan_files().await;
        scheduler.schedule_l2_to_l3_detached();
        assert!(
            !scheduler.is_l2_to_l3_inflight(0, partition),
            "L3 封顶，无 L2 不应再触发"
        );

        // ── 阶段 4：final 收敛到 1 个 L4（一天一文件）──
        scheduler.run_finalization(true).await.expect("final");
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L3).len(), 0);
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L4).len(),
            1,
            "final 应收敛到 1 个 L4"
        );

        // ── 阶段 5：is_finalized()（l4==1 且 l0/l1/l2/l3 全空）──
        scheduler.scan_files().await;
        let groups = scheduler.partition_groups_snapshot();
        let g = groups
            .iter()
            .find(|g| g.tenant_id == 0 && g.partition_ns == partition)
            .expect("分区存在");
        assert!(
            g.is_finalized(),
            "最终态必须 l4==1 且 l0/l1/l2/l3 全空"
        );
        assert_eq!(g.l4.len(), 1);
        assert!(g.l0.is_empty() && g.l1.is_empty() && g.l2.is_empty() && g.l3.is_empty());

        // ── 阶段 6：数据完整性：L4 读回逐点 == 全部写入点（无丢无重、ts 升序）──
        let l4 = list_files_by_level(&sac_dir, SacLevel::L4);
        let rows = SacReader::read_range(
            &l4[0],
            sensearcdb_common::TenantId::DEFAULT,
            table,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), total_rows, "L4 读回应无丢无重");
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.ts, (i + 1) as i64, "ts 必须升序连续");
            assert_eq!(
                r.values,
                vec![(0, TypedValue::F64((i + 1) as f64))],
                "值必须逐点一致"
            );
        }
    }

    /// 刀1 补测：分代合并逐分区判定（不跨分区累加文件数）。
    ///
    /// 两个分区各 3 个 L1（共 6 个，每分区 < 4）→ L1→L2 不触发；某分区补到 4 个后
    /// 仅该分区触发合并，另一分区 L1 保留。
    #[tokio::test]
    async fn test_generational_per_partition_threshold() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        // 保持默认 L1→L2 阈值 4。
        let handle = ConfigHandle::new(cfg);
        let scheduler = CompactionScheduler::new(&handle);

        let mut next_ts = 1i64;
        for partition in [100i64, 200i64] {
            for seq in 1..=3u16 {
                let t: Vec<i64> = (next_ts..next_ts + 5).collect();
                let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
                next_ts += 5;
                write_sac_segment(
                    &sac_dir, CompressionType::Zstd9, SacLevel::L1, 42, partition, seq, t, v,
                );
            }
        }
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L1).len(), 6);

        scheduler.scan_files().await;
        let merged = scheduler.execute_l1_to_l2().await.expect("L1→L2");
        assert_eq!(
            merged, 0,
            "每分区 < 4 个 L1 时不得跨分区累加触发合并"
        );
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L1).len(),
            6,
            "源 L1 全部保留（未合并）"
        );

        // 给分区 100 再补 1 个 L1（凑 4）→ 仅该分区触发 L1→L2。
        let t: Vec<i64> = (next_ts..next_ts + 5).collect();
        let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
        write_sac_segment(&sac_dir, CompressionType::Zstd9, SacLevel::L1, 42, 100, 4, t, v);

        scheduler.scan_files().await;
        let merged = scheduler.execute_l1_to_l2().await.expect("L1→L2");
        assert_eq!(merged, 4, "分区 100 凑 4 个 L1 后应仅合并该分区");
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L1).len(),
            3,
            "分区 200 的 3 个 L1 应保留"
        );
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L2).len(),
            1,
            "分区 100 产出 1 个 L2"
        );
    }

    // ===================== QA 独立验证（L2→L3 点级 file_rank 重压 + detach） =====================

    /// 写一个 L2 SAC 文件（ZSTD9），显式指定 write_epoch，用于精确构造「新旧文件」的
    /// file_rank 决胜顺序（旧文件 write_epoch 小 → rank 大 → 应被覆盖）。
    #[allow(clippy::too_many_arguments)]
    fn write_l2_segment_with_epoch(
        sac_dir: &Path,
        table_id: u32,
        partition_ns: i64,
        seq: u16,
        timestamps: Vec<i64>,
        values: Vec<f64>,
        write_epoch_ns: i64,
    ) -> PathBuf {
        let writer = SacWriter::new(sac_dir, CompressionType::Zstd9);
        let point_count = timestamps.len();
        let ts_index: std::collections::BTreeMap<i64, usize> = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| (*ts, i))
            .collect();
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(values));
        let chunk = ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps,
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; point_count],
            ts_index,
            point_count,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        };
        writer
            .write_chunk_with_epoch(
                &chunk,
                partition_ns,
                SacLevel::L2,
                seq,
                None,
                write_epoch_ns,
            )
            .expect("write l2 segment with epoch")
    }

    /// P0-1 数据正确性：L2→L3 的 file_rank 决胜 LWW 语义（新文件覆盖旧文件）。
    ///
    /// 构造 4 个 L2 源文件，其中「新文件」seq 最大（4）但 write_epoch 最大（9000），
    /// 「旧文件」seq 最小（1）但 write_epoch 最小（1000），且两文件在 ts=200 重叠、值不同。
    /// 若实现按「seq ASC / 输入顺序」（`g.l2` 路径序）而非 `write_epoch DESC` 赋 file_rank，
    /// 旧文件会被误判为最新，ts=200 读回旧值 2.0 —— 本测试据此抓错。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn qa_l2_to_l3_file_rank_lww_new_file_wins() {
        use crate::memtable::col_vec::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition = 100i64;
        let table = 42u32;

        // 旧文件（seq=1, write_epoch=1000）：ts 100/200/300。
        let old = write_l2_segment_with_epoch(
            &sac_dir, table, partition, 1,
            vec![100, 200, 300], vec![1.0, 2.0, 3.0], 1000,
        );
        // 中间文件 C/D：不重叠。
        let c = write_l2_segment_with_epoch(
            &sac_dir, table, partition, 2,
            vec![400, 500], vec![4.0, 5.0], 2000,
        );
        let d = write_l2_segment_with_epoch(
            &sac_dir, table, partition, 3,
            vec![600, 700], vec![6.0, 7.0], 3000,
        );
        // 新文件（seq=4, write_epoch=9000）：ts 150/200/250，其中 ts=200 覆盖旧值。
        let new = write_l2_segment_with_epoch(
            &sac_dir, table, partition, 4,
            vec![150, 200, 250], vec![15.0, 20.0, 25.0], 9000,
        );

        // 防御性校验：构造的 write_epoch 与预期一致（新 > 中间 > 旧）。
        let epoch = |p: &PathBuf| SacReader::read_header(p).unwrap().write_epoch;
        assert_eq!(epoch(&old), 1000);
        assert_eq!(epoch(&c), 2000);
        assert_eq!(epoch(&d), 3000);
        assert_eq!(epoch(&new), 9000);
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L2).len(), 4);

        scheduler.scan_files().await;
        scheduler.schedule_l2_to_l3_detached();
        let ok = wait_until(Duration::from_secs(10), || {
            list_files_by_level(&sac_dir, SacLevel::L3).len() == 1
                && !scheduler.is_l2_to_l3_inflight(0, partition)
        })
        .await;
        assert!(ok, "L2→L3 应在超时内产出 1 个 L3 并释放 inflight");
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L2).len(), 0, "源 L2 应被删除");

        let l3 = list_files_by_level(&sac_dir, SacLevel::L3);
        assert_eq!(l3.len(), 1);
        let mut rows = SacReader::read_range(
            &l3[0],
            sensearcdb_common::TenantId::DEFAULT,
            table,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        rows.sort_by_key(|r| r.ts);

        let tss: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        assert_eq!(
            tss,
            vec![100, 150, 200, 250, 300, 400, 500, 600, 700],
            "无丢无重：9 个唯一 ts 全部保留且升序"
        );
        let vals: Vec<f64> = rows
            .iter()
            .map(|r| r.values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0))
            .collect();
        assert_eq!(
            vals,
            vec![1.0, 15.0, 20.0, 25.0, 3.0, 4.0, 5.0, 6.0, 7.0],
            "ts=200 必须取新文件值 20.0（新文件覆盖旧文件），新增点保留"
        );
        // 单点强断言：LWW 决胜关键点。
        let ts200 = rows.iter().find(|r| r.ts == 200).expect("ts=200 必须存在");
        assert_eq!(
            ts200.values,
            vec![(0, TypedValue::F64(20.0))],
            "同 ts 冲突必须由 file_rank 更小（更新）的文件胜出"
        );
    }

    /// P0-2a：`L2ToL3InflightGuard` 的 Drop 必须在「正常返回」与「panic」两条路径下
    /// 都释放 `l2_to_l3_inflight`，杜绝残留导致该分区后续轮次永远被跳过。
    #[test]
    fn qa_l2_to_l3_inflight_guard_releases_on_drop_and_panic() {
        let config = ConfigHandle::new(Default::default());
        let scheduler = Arc::new(CompactionScheduler::new(&config));

        // 正常 Drop 释放。
        scheduler.l2_to_l3_inflight.lock().unwrap().insert((0, 42));
        assert!(scheduler.is_l2_to_l3_inflight(0, 42));
        drop(L2ToL3InflightGuard {
            scheduler: scheduler.clone(),
            key: (0, 42),
        });
        assert!(!scheduler.is_l2_to_l3_inflight(0, 42), "Drop 后必须释放 inflight");

        // panic 路径：Drop 在 unwinding 时同样执行，不得泄漏。
        scheduler.l2_to_l3_inflight.lock().unwrap().insert((0, 43));
        assert!(scheduler.is_l2_to_l3_inflight(0, 43));
        let sched = scheduler.clone();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = L2ToL3InflightGuard {
                scheduler: sched,
                key: (0, 43),
            };
            panic!("simulate detached L2->L3 task panic");
        }));
        assert!(r.is_err(), "应捕获 panic");
        assert!(
            !scheduler.is_l2_to_l3_inflight(0, 43),
            "panic 后 Drop 也必须释放 inflight"
        );
    }

    /// P0-2b 幂等：分区 L2→L3 已在飞时，再次 `schedule_l2_to_l3_detached` 不得重复
    /// claim（`insert` 返回 false → skip），不得 spawn 第二个任务或删除源文件。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn qa_l2_to_l3_schedule_idempotent_skip_when_inflight() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition = 100i64;
        let mut next_ts = 1i64;
        for seq in 1..=4u16 {
            let t: Vec<i64> = (next_ts..next_ts + 5).collect();
            let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
            next_ts += 5;
            write_sac_segment(
                &sac_dir, CompressionType::Zstd9, SacLevel::L2, 42, partition, seq, t, v,
            );
        }
        scheduler.scan_files().await;

        // 预 claim：模拟已有一个 detach 任务在飞。
        scheduler.l2_to_l3_inflight.lock().unwrap().insert((0, partition));
        assert!(scheduler.is_l2_to_l3_inflight(0, partition));

        scheduler.schedule_l2_to_l3_detached();
        // 给（若有错误 spawn 的）任务留出推进窗口。
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(
            list_files_by_level(&sac_dir, SacLevel::L3).is_empty(),
            "inflight 已占位时不得重复 claim/spawn"
        );
        assert!(
            scheduler.is_l2_to_l3_inflight(0, partition),
            "预 claim 的 inflight 不得被幽灵任务释放"
        );
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L2).len(),
            4,
            "源 L2 不得被删除"
        );

        // 释放后重新 schedule → 正常收敛（证明跳过是幂等而非死锁）。
        scheduler.l2_to_l3_inflight.lock().unwrap().remove(&(0, partition));
        scheduler.schedule_l2_to_l3_detached();
        let ok = wait_until(Duration::from_secs(10), || {
            list_files_by_level(&sac_dir, SacLevel::L3).len() == 1
                && !scheduler.is_l2_to_l3_inflight(0, partition)
        })
        .await;
        assert!(ok, "释放 inflight 后 L2→L3 应正常收敛");
    }

    /// P0-2c 对称互斥（方向 1）：`finalizing` 已 claim 该分区时，
    /// `schedule_l2_to_l3_detached` 必须跳过，且不得 claim L2→L3 inflight。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn qa_l2_to_l3_skipped_when_finalizing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition = 100i64;
        let mut next_ts = 1i64;
        for seq in 1..=4u16 {
            let t: Vec<i64> = (next_ts..next_ts + 5).collect();
            let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
            next_ts += 5;
            write_sac_segment(
                &sac_dir, CompressionType::Zstd9, SacLevel::L2, 42, partition, seq, t, v,
            );
        }
        scheduler.scan_files().await;

        // 预 claim finalizing。
        scheduler.finalizing.lock().unwrap().insert((0, partition));
        assert!(scheduler.is_finalizing(0, partition));

        scheduler.schedule_l2_to_l3_detached();
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(
            !scheduler.is_l2_to_l3_inflight(0, partition),
            "finalizing 时不得 claim L2→L3 inflight"
        );
        assert!(
            list_files_by_level(&sac_dir, SacLevel::L3).is_empty(),
            "finalizing 时不得启动 L2→L3"
        );
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L2).len(),
            4,
            "源 L2 保留"
        );
    }

    /// P0-2c 对称互斥（方向 2）：`l2_to_l3_inflight` 已 claim 该分区时，
    /// `schedule_finalization_detached` 必须跳过，且不得 claim `finalizing`。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn qa_final_skipped_when_l2_to_l3_inflight() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        // 用 2016 年的旧分区（grace 早已到期、非 finalized），保证是 final 候选。
        let partition = 1_451_606_400_000_000_000_i64;
        let mut next_ts = 1i64;
        for seq in 1..=4u16 {
            let t: Vec<i64> = (next_ts..next_ts + 5).collect();
            let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
            next_ts += 5;
            write_sac_segment(
                &sac_dir, CompressionType::Zstd9, SacLevel::L2, 42, partition, seq, t, v,
            );
        }
        scheduler.scan_files().await;

        // 预 claim l2_to_l3_inflight。
        scheduler.l2_to_l3_inflight.lock().unwrap().insert((0, partition));
        assert!(scheduler.is_l2_to_l3_inflight(0, partition));

        scheduler.schedule_finalization_detached(false).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(
            !scheduler.is_finalizing(0, partition),
            "L2→L3 在飞时 final 不得 claim"
        );
        assert!(
            list_files_by_level(&sac_dir, SacLevel::L4).is_empty(),
            "L2→L3 在飞时 final 不得产出 L4"
        );
        assert_eq!(
            list_files_by_level(&sac_dir, SacLevel::L2).len(),
            4,
            "源 L2 保留"
        );
    }

    /// P0-3 压缩率归并效果：点级重压把碎片 block 归并到 ~4096 点/块，而非 block 拼接保留碎片。
    ///
    /// 4 个 L2 源文件，每个 4196 点（4096 满块 + 100 尾块 = 2 block），共 8 block。
    /// 点级重压后：总 16784 点 → ceil(16784/4096) = 5 block（4 满块 + 1 个 400 尾块）。
    /// block 拼接路径会保留 8 block，本断言据此证明走的是点级重压路径。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn qa_l2_to_l3_point_recompress_merges_blocks() {
        use crate::memtable::col_vec::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let scheduler = Arc::new(CompactionScheduler::new(&handle));

        let partition = 100i64;
        let table = 42u32;
        let points_per_file = 4096usize + 100usize;
        let mut next_ts = 1i64;
        let mut total = 0usize;
        let mut source_block_count = 0usize;

        for seq in 1..=4u16 {
            let t: Vec<i64> = (next_ts..next_ts + points_per_file as i64).collect();
            let v: Vec<f64> = t.iter().map(|&x| x as f64).collect();
            next_ts += points_per_file as i64;
            total += points_per_file;
            let p = write_sac_segment(
                &sac_dir, CompressionType::Zstd9, SacLevel::L2, table, partition, seq, t, v,
            );
            source_block_count += SacReader::read_l2_descriptors(&p).unwrap().len();
        }
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L2).len(), 4);
        assert_eq!(source_block_count, 8, "4 源文件 × 2 block = 8 block");

        scheduler.scan_files().await;
        scheduler.schedule_l2_to_l3_detached();
        let ok = wait_until(Duration::from_secs(10), || {
            list_files_by_level(&sac_dir, SacLevel::L3).len() == 1
                && !scheduler.is_l2_to_l3_inflight(0, partition)
        })
        .await;
        assert!(ok, "L2→L3 应产出 1 个 L3 并释放 inflight");
        assert_eq!(list_files_by_level(&sac_dir, SacLevel::L2).len(), 0, "源 L2 应被删除");

        let l3 = list_files_by_level(&sac_dir, SacLevel::L3);
        assert_eq!(l3.len(), 1);
        let l3_block_count = SacReader::read_l2_descriptors(&l3[0]).unwrap().len();
        assert_eq!(
            l3_block_count, 5,
            "点级重压应把 8 个碎片 block 归并到 5 个满 block（拼接路径会保留 8 block）"
        );

        let rows = SacReader::read_range(
            &l3[0],
            sensearcdb_common::TenantId::DEFAULT,
            table,
            i64::MIN,
            i64::MAX,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), total, "读回无丢无重");
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.ts, (i + 1) as i64, "ts 必须升序连续");
            assert_eq!(
                r.values,
                vec![(0, TypedValue::F64((i + 1) as f64))],
                "值必须逐点一致"
            );
        }
    }
}

//! FlushCoordinator + FlushPipeline：MemTable → SAC 文件刷盘。
//!
//! FlushCoordinator 以固定间隔（10s）轮询 `MemTableManager::immutable`，
//! 发现有冻结的 MemTable 则启动 FlushPipeline 将其写入 SAC 文件。
//!
//! ## FlushPipeline 流程
//!
//! 1. 遍历 immutable MemTable 的所有 Chunk
//! 2. 对每个 Chunk 调用 SacWriter::write_chunk()
//! 3. 写入完成后清理 immutable 槽位
//!
//! ## Flush barrier（阶段 7 / 6a-1）
//!
//! `flush_now()` 通过「无界请求通道 + oneshot 回执」实现同步 barrier：
//! 调用方向协调器发一次 flush 请求并等待回执，协调器在本轮把 immutable
//! 队列 drain-all（pop_immutable + FlushPipeline）落 SAC 后统一回执。
//! **禁止 sleep 固定时长冒充 barrier**——回执只在本轮 drain-all 真实完成后发送。

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rayon::prelude::*;
use sensearcdb_common::{align_to_partition, SenseArcError};
use tokio::time;
use tracing;

use crate::memtable::chunk::ColumnarChunk;
use crate::memtable::col_vec::ColVec;
use crate::memtable::manager::MemTableManager;
use crate::sac::header::SacLevel;
use crate::sac::naming::SacNaming;
use crate::sac::watermark::SacWatermark;
use crate::sac::writer::SacWriter;

/// Flush barrier 回执通道（协调器完成本轮排空后 `send` 本轮的落盘 Block 数）。
type FlushRequest = tokio::sync::oneshot::Sender<usize>;

/// Flush barrier 兜底超时：仅作死锁保护网，barrier 本身由 oneshot 回执保证，
/// 不是「睡固定时长等落盘」。
const FLUSH_BARRIER_TIMEOUT: Duration = Duration::from_secs(30);

// ─────────────────────────────────────────────────────────────────────────────
// flush_timing 埋点（保留用于验收）
//
// `execute_blocking` 内的 `split_bucket` / `split_chunk_ms` / `bucket_push_ms` /
// `bucket_sort_ms` / `split_fast_path_count` / `split_cross_day_count` 字段保留，
// 用于验收 split_bucket 耗时与「单天快路径 vs 跨天重建」频率。
//
// 「逐点×逐列」下钻诊断用的临时 AtomicU64 计数器（SplitDrill 系列）已随
// `build_sub_chunk` 按列批量拷贝优化一并移除——热点已定位并消除，不再需要
// 逐元素级下钻计数。
// ─────────────────────────────────────────────────────────────────────────────

/// Flush 协调器：定期轮询 immutable MemTable 并触发 SAC 写入。
pub struct FlushCoordinator {
    /// SAC 写入器引用。
    sac_writer: Arc<SacWriter>,
    /// MemTable 管理器引用。
    memtable: Arc<MemTableManager>,
    /// 轮询间隔。
    poll_interval: Duration,
    /// Flush barrier 请求发送端（`flush_now` 发请求，`run` 消费）。
    flush_tx: tokio::sync::mpsc::UnboundedSender<FlushRequest>,
    /// Flush barrier 请求接收端（`run` 启动时 take 一次）。
    flush_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<FlushRequest>>>,
    /// 健康心跳（epoch ns）：每轮 `flush_once` 刷新，供 watchdog 判断 FlushCoordinator 是否挂起。
    heartbeat: std::sync::atomic::AtomicU64,
    /// 立即 flush 通知（写路径 freeze 后 `request_flush` 唤醒 `run` 即时排空 immutable）。
    flush_notify: Arc<tokio::sync::Notify>,
    /// SAC 落盘水位 `S`（C2）：SAC fsync 完成后推进「已 flush 最大数据 seq」。
    sac_watermark: Arc<SacWatermark>,
    /// 分区窗口时长（毫秒），默认 24h。flush 据此做「数据时间 → 天分区键」对齐。
    partition_duration_ms: u64,
    /// 分区边界时区偏移（秒），默认 UTC+8。与 `partition_duration_ms` 一起决定天边界。
    partition_tz_offset_secs: i64,
}

/// 心跳超时倍数：`now - heartbeat > N × poll_interval` 判定 FlushCoordinator 不健康。
const HEARTBEAT_TIMEOUT_MULTIPLIER: u64 = 3;

impl FlushCoordinator {
    /// 创建新的 FlushCoordinator。
    ///
    /// # 参数
    /// - `sac_writer`: SAC 写入器。
    /// - `memtable`: MemTable 管理器。
    /// - `sac_watermark`: SAC 落盘水位（C2），flush 完成后据此推进 S。
    pub fn new(
        sac_writer: Arc<SacWriter>,
        memtable: Arc<MemTableManager>,
        sac_watermark: Arc<SacWatermark>,
    ) -> Self {
        Self::new_with_partition(sac_writer, memtable, sac_watermark, 86_400_000, 28_800)
    }

    /// 创建带分区对齐配置的 FlushCoordinator（引擎主路径使用）。
    ///
    /// # 参数
    /// - `partition_duration_ms`: 分区窗口时长（毫秒）。
    /// - `partition_tz_offset_secs`: 时区偏移（秒）。
    pub fn new_with_partition(
        sac_writer: Arc<SacWriter>,
        memtable: Arc<MemTableManager>,
        sac_watermark: Arc<SacWatermark>,
        partition_duration_ms: u64,
        partition_tz_offset_secs: i64,
    ) -> Self {
        let (flush_tx, flush_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            sac_writer,
            memtable,
            poll_interval: Duration::from_secs(10),
            flush_tx,
            flush_rx: Mutex::new(Some(flush_rx)),
            heartbeat: std::sync::atomic::AtomicU64::new(current_epoch_ns() as u64),
            flush_notify: Arc::new(tokio::sync::Notify::new()),
            sac_watermark,
            partition_duration_ms,
            partition_tz_offset_secs,
        }
    }

    /// 健康探针：FlushCoordinator 心跳是否新鲜（问题 2 三级方案 ③ watchdog）。
    ///
    /// 挂起判定：`now - last_heartbeat > 3 × poll_interval`。
    pub fn is_healthy(&self) -> bool {
        self.heartbeat_age_ns()
            < HEARTBEAT_TIMEOUT_MULTIPLIER * self.poll_interval.as_nanos() as u64
    }

    /// 距上次心跳的纳秒数。
    pub fn heartbeat_age_ns(&self) -> u64 {
        let now = current_epoch_ns() as u64;
        let last = self.heartbeat.load(std::sync::atomic::Ordering::Relaxed);
        now.saturating_sub(last)
    }

    /// 刷新心跳（每轮 `flush_once` 后调用）。
    fn touch_heartbeat(&self) {
        self.heartbeat.store(
            current_epoch_ns() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// 同步 flush barrier：请求协调器立即排空一次 immutable 并等待回执。
    ///
    /// 供 `Engine::flush_immutable_sync`（阶段 7 / 6a-1）在 `force_freeze` 后调用，
    /// 保证冻结数据已落 SAC 才返回。回执由 [`FlushCoordinator::run`] 在本轮排空后发送。
    ///
    /// # 返回
    /// 本轮落盘的 SAC Block 数（无 immutable 时返回 `0`）。
    pub async fn flush_now(&self) -> Result<usize, SenseArcError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.flush_tx.send(tx).map_err(|_| {
            SenseArcError::Internal("flush coordinator stopped (channel closed)".to_string())
        })?;
        // barrier = oneshot 回执；timeout 仅作死锁保护网。
        let blocks = tokio::time::timeout(FLUSH_BARRIER_TIMEOUT, rx)
            .await
            .map_err(|_| SenseArcError::Internal("flush barrier timeout".to_string()))?
            .map_err(|_| SenseArcError::Internal("flush coordinator dropped".to_string()))?;
        Ok(blocks)
    }

    /// 非阻塞请求立即排空一次 immutable（写路径 freeze 后调用）。
    ///
    /// 与 [`flush_now`](Self::flush_now)（带 oneshot 回执的同步 barrier）不同，
    /// 本方法只 `notify_one` 唤醒 `run` 主循环、不等回执，适合写入热路径
    /// （每 `freeze_threshold` 点一次，开销可忽略）。
    pub fn request_flush(&self) {
        self.flush_notify.notify_one();
    }

    /// 启动 Flush 主循环（阻塞式，应在 tokio::spawn 中调用）。
    ///
    /// 每 10s 检查一次 immutable 槽位（或被 flush barrier 请求 / 写路径 freeze 唤醒）：
    /// - 若存在：执行 flush → SAC 写入
    /// - 若不存在：继续等待
    ///
    /// 每轮排空结束后，把本轮落盘 Block 数回执给所有 pending 的 flush barrier 请求。
    pub async fn run(&self) {
        let mut flush_rx = self
            .flush_rx
            .lock()
            .unwrap()
            .take()
            .expect("flush receiver already taken");
        let mut pending: Vec<FlushRequest> = Vec::new();

        loop {
            tokio::select! {
                _ = time::sleep(self.poll_interval) => {},
                req = flush_rx.recv() => {
                    match req {
                        Some(r) => pending.push(r),
                        None => break,
                    }
                }
                _ = self.flush_notify.notified() => {},
            }

            let blocks = self.flush_once().await;

            // 问题 2 三级方案 ③：每轮刷新心跳（watchdog 判断 FlushCoordinator 是否挂起）。
            self.touch_heartbeat();

            // 回执所有 pending 请求：本轮排空已真实完成（drain-all pop_immutable + FlushPipeline）。
            for req in pending.drain(..) {
                let _ = req.send(blocks);
            }
        }
    }

    /// 排空 immutable 队列：`pop_immutable`（FIFO）→ `FlushPipeline` 落 SAC，
    /// **drain-all**（循环 pop 到队列空）。
    ///
    /// P5：flush 失败时把表 `push_front` 塞回队头，等下一轮重试（不丢弃）。
    ///
    /// # 返回
    /// 本轮落盘的 SAC Block 总数（无 immutable 时返回 `0`）。
    async fn flush_once(&self) -> usize {
        let mut total_blocks = 0usize;

        // drain-all：一次轮询排空所有 frozen 表。
        while let Some(mt) = self.memtable.pop_immutable() {
            tracing::info!(
                total_points = mt.total_points(),
                table_count = mt.table_count(),
                "FlushCoordinator: flushing immutable MemTable"
            );

            let mut pipeline = FlushPipeline::new(
                Arc::clone(&self.sac_writer),
                self.partition_duration_ms,
                self.partition_tz_offset_secs,
            );
            match pipeline.execute(&mt).await {
                Ok(count) => {
                    tracing::info!(blocks_written = count, "FlushPipeline complete");
                    total_blocks += count;
                    // C2（I2）：本张 immutable 表已全部 fsync 落 SAC，推进 SAC 落盘水位 S。
                    self.advance_sac_watermark();
                }
                Err(e) => {
                    // P5：flush 失败重塞队头，等下一轮重试（绝不 log 后丢弃）。
                    tracing::error!(
                        error = %e,
                        "FlushPipeline error; requeue immutable to queue front"
                    );
                    self.memtable.push_front_immutable(mt);
                    break; // 停止本轮，避免对同一失败表无限重试
                }
            }
        }

        total_blocks
    }

    /// 推进 SAC 落盘水位 `S`（C2）。
    ///
    /// 语义：`S = 未 flush 数据的最小 WAL 帧 raft_index - 1`。所有 `raft_index ≤ S`
    /// 的 WAL 帧其数据均已 fsync 进 SAC（I2/I5）。`advance_to` 单调不减 + 先落盘
    /// 后推进，崩溃在任意点都保守安全。
    fn advance_sac_watermark(&self) {
        if let Some(min_seq) = self.memtable.unflushed_min_seq() {
            let new_s = min_seq.saturating_sub(1);
            if let Err(e) = self.sac_watermark.advance_to(new_s) {
                tracing::error!(error = %e, "advance SAC watermark failed");
            }
        }
    }
}

/// Flush 管线：将 MemTable 的数据写入 SAC 文件。
pub struct FlushPipeline {
    /// SAC 写入器。
    sac_writer: Arc<SacWriter>,
    /// 分区窗口时长（毫秒）。
    partition_duration_ms: u64,
    /// 分区边界时区偏移（秒）。
    partition_tz_offset_secs: i64,
}

impl FlushPipeline {
    /// 创建新的 FlushPipeline。
    ///
    /// # 参数
    /// - `sac_writer`: SAC 写入器。
    /// - `partition_duration_ms`: 分区窗口时长（毫秒）。
    /// - `partition_tz_offset_secs`: 时区偏移（秒）。
    pub fn new(
        sac_writer: Arc<SacWriter>,
        partition_duration_ms: u64,
        partition_tz_offset_secs: i64,
    ) -> Self {
        Self {
            sac_writer,
            partition_duration_ms,
            partition_tz_offset_secs,
        }
    }

    /// 执行 Flush 管线。
    ///
    /// # 参数
    /// - `mt`: 待刷盘的 ColumnarMemTable。
    ///
    /// # 返回值
    /// 成功写入的 SAC Block 数量。
    pub async fn execute(
        &mut self,
        mt: &crate::memtable::table::ColumnarMemTable,
    ) -> Result<usize, SenseArcError> {
        let flush_start = Instant::now();
        let now_ns = current_epoch_ns();
        // 冻结表不可变：Arc 只读借用快照（零拷贝共享），owned + Send 可移入 spawn_blocking。
        // 计时埋点（flush_timing）：这一段含 table.rs `chunks_arc_snapshot` 的
        // `Arc::new(v.clone())` 深拷贝，是本次实测的核心观测对象。
        let snapshot_start = Instant::now();
        let chunks = mt.chunks_arc_snapshot();
        let snapshot_ms = phase_ms(snapshot_start);
        tracing::debug!(
            target: "flush_timing",
            phase = "chunks_snapshot",
            duration_ms = snapshot_ms,
            chunk_count = chunks.len(),
        );
        let sac_writer = Arc::clone(&self.sac_writer);
        let partition_duration_ms = self.partition_duration_ms;
        let partition_tz_offset_secs = self.partition_tz_offset_secs;

        // §5.0 单层包裹：整个同步段（seq 预扫 → rayon 并行编码/写盘 → fsync）移出
        // async reactor，与 compaction `scheduler.rs` 的 spawn_blocking 同构。
        // rayon 任务内不再有 spawn_blocking/await（防嵌套死锁）。
        let result = tokio::task::spawn_blocking(move || {
            Self::execute_blocking(
                sac_writer,
                chunks,
                now_ns,
                partition_duration_ms,
                partition_tz_offset_secs,
            )
        })
        .await
        .map_err(|e| SenseArcError::Internal(format!("flush spawn_blocking join: {}", e)))?;

        // 总 flush wall-clock（chunks_snapshot + spawn_blocking 调度 + execute_blocking）。
        let total_ms = phase_ms(flush_start);
        tracing::debug!(
            target: "flush_timing",
            phase = "flush_total",
            duration_ms = total_ms,
        );
        result
    }

    /// `execute` 的同步实现（在 `spawn_blocking` 线程池中运行）。
    ///
    /// 分桶 + seq 预扫描（串行）→ 独立 rayon 池按 partition 并行编码+写盘 → 全 Ok 才
    /// 返回 `Ok(total_blocks)`；任一 partition 失败返回 `Err`（上层重塞队头、不推进 S）。
    fn execute_blocking(
        sac_writer: Arc<SacWriter>,
        chunks: HashMap<u32, Arc<ColumnarChunk>>,
        now_ns: i64,
        partition_duration_ms: u64,
        partition_tz_offset_secs: i64,
    ) -> Result<usize, SenseArcError> {
        let blocking_start = Instant::now();

        // v9 多表聚合：一次 MemTable 的 chunks = 一个 flush 批次。先把每张表的 chunk
        // 按「数据时间边界」硬切分（跨天拆分，不变式 I2），再按 (tenant_id, partition_ns)
        // 分桶，每桶一个 L0 文件，桶内按 table_id 升序排序（不变式 I2：块按
        // (table_id, min_ts) 升序）。不变式 I6：单文件只属于一个 (tenant, partition_ns)，
        // 因此分桶键必须同时含 tenant_id（同一 immutable 队列可跨租户攒批）。
        // 计时埋点（flush_timing）：分桶 + split_chunk_by_partition（含 build_sub_chunk）。
        // 下钻：拆成 split_chunk / bucket_push / bucket_sort 三段，并统计「单天快路径
        // （Arc::clone 复用）」vs「跨天 build_sub_chunk」的频率（3007ms 热点定位）。
        let split_start = Instant::now();
        let mut by_partition: BTreeMap<(u32, i64), Vec<Arc<ColumnarChunk>>> = BTreeMap::new();
        let mut split_chunk_ms: f64 = 0.0;
        let mut bucket_push_ms: f64 = 0.0;
        let mut split_fast_path_count: u64 = 0;
        let mut split_cross_day_count: u64 = 0;
        for (_table_id, chunk) in &chunks {
            if chunk.point_count == 0 {
                continue;
            }
            let tenant_id = chunk.tenant_id.as_u32();

            let sc_start = Instant::now();
            let sub_chunks = split_chunk_by_partition(
                chunk,
                partition_duration_ms,
                partition_tz_offset_secs,
            );
            split_chunk_ms += phase_ms(sc_start);

            // 快路径判定：单天复用 → len==1 且指针相同；否则真跨天走了 build_sub_chunk。
            if sub_chunks.len() == 1 && Arc::ptr_eq(&sub_chunks[0], chunk) {
                split_fast_path_count += 1;
            } else {
                split_cross_day_count += 1;
            }

            let bp_start = Instant::now();
            for sub in sub_chunks {
                // partition_ns = 数据时间天边界（非写入时间）。单天快路径下所有点
                // 落在同一天，任一 timestamps 元素对齐结果一致。
                let partition_ns = sub
                    .timestamps
                    .first()
                    .copied()
                    .map(|ts| align_to_partition(ts, partition_duration_ms, partition_tz_offset_secs))
                    .unwrap_or(0);
                by_partition
                    .entry((tenant_id, partition_ns))
                    .or_default()
                    .push(sub);
            }
            bucket_push_ms += phase_ms(bp_start);
        }

        // I2：桶内按 table_id 升序（同表内块已按 ts 升序，切分后天然成立）。
        let sort_start = Instant::now();
        for bucket in by_partition.values_mut() {
            bucket.sort_by_key(|c| c.table_id);
        }
        let bucket_sort_ms = phase_ms(sort_start);

        let split_ms = phase_ms(split_start);
        let partition_count = by_partition.len();
        tracing::debug!(
            target: "flush_timing",
            phase = "split_bucket",
            duration_ms = split_ms,
            partition_count = partition_count,
            split_chunk_ms = split_chunk_ms,
            bucket_push_ms = bucket_push_ms,
            bucket_sort_ms = bucket_sort_ms,
            split_fast_path_count = split_fast_path_count,
            split_cross_day_count = split_cross_day_count,
        );

        // §5.0：唯一同步 IO（seq 扫盘 read_dir）提前到 rayon 外串行预扫描一次
        // （≤4 次 read_dir），避免 rayon 任务内做同步 IO。seq 命名空间
        // `(tenant, partition_ns, L0)` 天然隔离，不同 partition 无竞态（不变式 1）。
        // 计时埋点（flush_timing）：rayon 外的 seq 预扫描（read_dir）。
        let seq_start = Instant::now();
        let mut seqs: BTreeMap<(u32, i64), u16> = BTreeMap::new();
        for &(tenant_id, partition_ns) in by_partition.keys() {
            let tenant_dir = sac_writer.sac_dir().join(tenant_id.to_string());
            let seq = SacNaming::next_seq_in_dir(&tenant_dir, partition_ns, SacLevel::L0);
            seqs.insert((tenant_id, partition_ns), seq);
        }
        let seq_ms = phase_ms(seq_start);
        tracing::debug!(
            target: "flush_timing",
            phase = "seq_prescan",
            duration_ms = seq_ms,
            scan_count = seqs.len(),
        );

        // 独立 rayon 池：按 partition 并行编码 + 写盘。每 partition 一个独立文件、
        // 独立 seq、独立 fd，零共享可变状态；同批次共享 now_ns（write_epoch）。
        // 计时埋点（flush_timing）：这一整段 wall-clock = 各 partition 并行
        // encode+compress+finish 的完成耗时（含 fsync+rename）。
        let parallel_start = Instant::now();
        let result = crate::sac::parallel::flush_pool().install(|| {
            by_partition
                .par_iter_mut()
                .map(|(&(tenant_id, partition_ns), bucket)| {
                    let seq = seqs[&(tenant_id, partition_ns)];
                    flush_partition(
                        &sac_writer,
                        tenant_id,
                        partition_ns,
                        bucket.as_slice(),
                        now_ns,
                        seq,
                    )
                })
                .collect::<Result<Vec<()>, SenseArcError>>()
        });
        let parallel_ms = phase_ms(parallel_start);
        tracing::debug!(
            target: "flush_timing",
            phase = "encode_compress_finish",
            duration_ms = parallel_ms,
            partition_count = partition_count,
        );

        let blocking_ms = phase_ms(blocking_start);
        tracing::debug!(
            target: "flush_timing",
            phase = "flush_total_blocking",
            duration_ms = blocking_ms,
        );

        // all-or-nothing：任一 partition 失败 → Err（上层 P5 重塞队头，S 不推进）。
        result.map(|_| partition_count)
    }
}

/// 写单个 `(tenant, partition_ns)` 分桶为一个 L0 SAC 文件。
///
/// 纯同步：`begin_write → append_chunk × N → finish`（finish 内含 fsync + rename）。
/// 本函数在 flush 的独立 rayon 池任务内执行，内部只做纯 CPU 编码与文件写盘，
/// 不再嵌套 `spawn_blocking`/`await`（§5.0）。
fn flush_partition(
    sac_writer: &SacWriter,
    tenant_id: u32,
    partition_ns: i64,
    bucket: &[Arc<ColumnarChunk>],
    now_ns: i64,
    seq: u16,
) -> Result<(), SenseArcError> {
    let mut builder = sac_writer.begin_write(tenant_id, partition_ns, SacLevel::L0, seq, now_ns)?;
    // 跨表批量编码：一个 partition 桶内所有 chunk 的 block 展平后跨表并行编码 + 顺序写盘，
    // `max_wal_seq` 在编码内从 `chunk.max_wal_seq` 取（S7：flush 已回填）。
    builder.append_chunks(bucket)?;
    let path = builder.finish()?;

    tracing::debug!(
        tenant_id = tenant_id,
        partition_ns = partition_ns,
        path = %path.display(),
        tables = bucket.len(),
        points = bucket.iter().map(|c| c.point_count).sum::<usize>(),
        "SAC file written (multi-table batch)"
    );

    Ok(())
}

/// 按「数据时间天边界」把一个可能跨天的 `ColumnarChunk` 硬切分成多个子 chunk。
///
/// 切分依据是 `ts_index`（BTreeMap，已按 ts 升序）的排序位置序列：连续遍历有序
/// 位置，按 `align_to_partition(timestamps[pos])` 分桶，相邻同天位置归入同段。
/// 因此切分后每个子 chunk 的 `[min_ts, max_ts)` 必落在单天分区窗口内（不变式 I2）。
///
/// 已排序位置必然按天边界连续，所以分桶只需比较「当前段的 key」是否等于「上一点
/// 的 key」，无需排序（O(n)）。
fn split_chunk_by_partition(
    chunk: &Arc<ColumnarChunk>,
    duration_ms: u64,
    tz_offset_secs: i64,
) -> Vec<Arc<ColumnarChunk>> {
    let sorted_positions: Vec<usize> = chunk.sorted_positions();

    // 快路径（拷贝消除 #2，§5.2-1）：整 chunk 落在单天（常见态）时直接复用原 chunk
    // 的 `Arc` 引用，跳过 `build_sub_chunk` 的第二遍全量深拷贝。仅真跨天才逐段重建
    // 子 chunk（跨天硬切分不变式 I3 保留在下方分支）。
    let all_same_day = match sorted_positions.first() {
        Some(&first_pos) => {
            let first_key = align_to_partition(
                chunk.timestamps[first_pos],
                duration_ms,
                tz_offset_secs,
            );
            sorted_positions.iter().all(|&pos| {
                align_to_partition(chunk.timestamps[pos], duration_ms, tz_offset_secs) == first_key
            })
        }
        None => true,
    };
    if all_same_day {
        return vec![Arc::clone(chunk)];
    }

    // 真跨天：按天边界分桶（保持 ts 升序），逐段重建子 chunk（确定性，不并行同 chunk
    // 内的 position 序列）。
    let mut groups: Vec<(i64, Vec<usize>)> = Vec::new();
    for &pos in &sorted_positions {
        let ts = chunk.timestamps[pos];
        let key = align_to_partition(ts, duration_ms, tz_offset_secs);
        match groups.last_mut() {
            Some((cur_key, positions)) if *cur_key == key => positions.push(pos),
            _ => groups.push((key, vec![pos])),
        }
    }

    groups
        .into_iter()
        .map(|(_, positions)| Arc::new(build_sub_chunk(chunk.as_ref(), &positions)))
        .collect()
}

/// 复刻 `ColumnarChunk::is_present` 的按位判定（presence 位图优先；位图缺失按密集列
/// 回退），并额外并入「源列内层 `Vec` 下标有效」约束——等价于旧实现 `get_typed_value`
/// 的 `is_present && v.get(pos).is_some()` 取值条件。
///
/// 批量化时把 presence 位图切片与源列长度提升到「逐列解析一次」，避免逐元素
/// HashMap 查找，同时逐字节保持旧的稀疏语义（缺失绝不能错填默认值污染数据）。
#[inline]
fn sub_col_present(src_presence: Option<&Vec<bool>>, src_len: usize, pos: usize) -> bool {
    match src_presence {
        Some(p) => p.get(pos).copied().unwrap_or(false) && pos < src_len,
        None => pos < src_len,
    }
}

/// 从源 chunk 抽取 `positions`（按 ts 升序）构造一个稀疏对齐的子 chunk。
///
/// 按列批量拷贝（拷贝消除 #3）：把「逐点×逐列」的 `get_typed_value` + `push_typed`
/// 换成「逐列 × 批量点」——每列仅 match 一次源 `ColVec` 变体拿到内层 `Vec`，再
/// `for &pos in positions` 直接 push；presence 用源位图切片直读。消除逐元素
/// HashMap 查找 + 8-way match + `TypedValue` 枚举包装（实测 ~717ns/列点）。
///
/// 目标列已用 `ColVec::new_for_type(chunk.columns[id].data_type())` 预建，故源列
/// 变体 == 目标列变体，match 后同变体 `push` 即可。String 列 `clone()`（引用语义）；
/// 其余列 Copy 直接 `src[pos]`。稀疏语义逐位保持（`sub_col_present`）。
/// `max_wal_seq` / `min_wal_seq` 沿用源 chunk 的极值（保守：子集真实极值落在其内）。
fn build_sub_chunk(chunk: &ColumnarChunk, positions: &[usize]) -> ColumnarChunk {
    let mut col_ids: Vec<u16> = chunk.columns.keys().copied().collect();
    col_ids.sort_unstable();

    let mut columns: HashMap<u16, ColVec> = HashMap::with_capacity(col_ids.len());
    let mut presence: HashMap<u16, Vec<bool>> = HashMap::with_capacity(col_ids.len());
    for &id in &col_ids {
        let data_type = chunk.columns.get(&id).map(|c| c.data_type());
        columns.insert(
            id,
            ColVec::new_for_type(data_type.unwrap_or(sensearcdb_common::DataType::F64)),
        );
        presence.insert(id, Vec::with_capacity(positions.len()));
    }

    let mut timestamps = Vec::with_capacity(positions.len());
    let mut qualities = Vec::with_capacity(positions.len());
    let mut ts_index: BTreeMap<i64, usize> = BTreeMap::new();

    for (new_idx, &pos) in positions.iter().enumerate() {
        let ts = chunk.timestamps[pos];
        timestamps.push(ts);
        qualities.push(chunk.qualities[pos]);
        ts_index.insert(ts, new_idx);
    }

    // 按列批量拷贝：每列 match 一次源变体，内层 Vec 批量 push；presence 切片直读。
    // 局部宏把外层上下文（positions/src_presence/dst_presence）显式传入，展开后即
    // 普通 for 循环，无逐元素 HashMap/match/TypedValue 开销。Copy 列直接 `src[pos]`，
    // String 列 `src[pos].clone()`（引用语义需 clone）。
    macro_rules! batch_copy_copy {
        ($dst:ident, $src:ident, $dst_presence:ident, $src_presence:expr, $positions:expr, $absent:expr) => {{
            let src_len = $src.len();
            for &pos in $positions {
                if sub_col_present($src_presence, src_len, pos) {
                    $dst.push($src[pos]);
                    $dst_presence.push(true);
                } else {
                    $dst.push($absent);
                    $dst_presence.push(false);
                }
            }
        }};
    }
    macro_rules! batch_copy_clone {
        ($dst:ident, $src:ident, $dst_presence:ident, $src_presence:expr, $positions:expr, $absent:expr) => {{
            let src_len = $src.len();
            for &pos in $positions {
                if sub_col_present($src_presence, src_len, pos) {
                    $dst.push($src[pos].clone());
                    $dst_presence.push(true);
                } else {
                    $dst.push($absent);
                    $dst_presence.push(false);
                }
            }
        }};
    }
    macro_rules! fill_absent {
        ($dst:ident, $dst_presence:ident, $positions:expr, $absent:expr) => {{
            for _ in $positions {
                $dst.push($absent);
                $dst_presence.push(false);
            }
        }};
    }

    for &id in &col_ids {
        let dst_col = columns.get_mut(&id).expect("column pre-inserted");
        let dst_presence = presence.get_mut(&id).expect("presence pre-inserted");
        let src_col = chunk.columns.get(&id);
        let src_presence = chunk.presence.get(&id);

        // 目标列已由源列 data_type() 预建，源/目标变体必匹配；`src_col` 为 None 或
        // 变体不匹配（均不可能）时走 `fill_absent!` 兜底，保持列长度 == positions.len()。
        match dst_col {
            ColVec::F64(dst) => match src_col {
                Some(ColVec::F64(src)) => {
                    batch_copy_copy!(dst, src, dst_presence, src_presence, positions, 0.0)
                }
                _ => fill_absent!(dst, dst_presence, positions, 0.0),
            },
            ColVec::F32(dst) => match src_col {
                Some(ColVec::F32(src)) => {
                    batch_copy_copy!(dst, src, dst_presence, src_presence, positions, 0.0)
                }
                _ => fill_absent!(dst, dst_presence, positions, 0.0),
            },
            ColVec::I64(dst) => match src_col {
                Some(ColVec::I64(src)) => {
                    batch_copy_copy!(dst, src, dst_presence, src_presence, positions, 0)
                }
                _ => fill_absent!(dst, dst_presence, positions, 0),
            },
            ColVec::U64(dst) => match src_col {
                Some(ColVec::U64(src)) => {
                    batch_copy_copy!(dst, src, dst_presence, src_presence, positions, 0)
                }
                _ => fill_absent!(dst, dst_presence, positions, 0),
            },
            ColVec::I32(dst) => match src_col {
                Some(ColVec::I32(src)) => {
                    batch_copy_copy!(dst, src, dst_presence, src_presence, positions, 0)
                }
                _ => fill_absent!(dst, dst_presence, positions, 0),
            },
            ColVec::Bool(dst) => match src_col {
                Some(ColVec::Bool(src)) => {
                    batch_copy_copy!(dst, src, dst_presence, src_presence, positions, false)
                }
                _ => fill_absent!(dst, dst_presence, positions, false),
            },
            ColVec::String(dst) => match src_col {
                Some(ColVec::String(src)) => {
                    batch_copy_clone!(dst, src, dst_presence, src_presence, positions, String::new())
                }
                _ => fill_absent!(dst, dst_presence, positions, String::new()),
            },
            ColVec::Enum16(dst) => match src_col {
                Some(ColVec::Enum16(src)) => {
                    batch_copy_copy!(dst, src, dst_presence, src_presence, positions, 0)
                }
                _ => fill_absent!(dst, dst_presence, positions, 0),
            },
        }
    }

    ColumnarChunk {
        table_id: chunk.table_id,
        tenant_id: chunk.tenant_id,
        timestamps,
        columns,
        presence,
        qualities,
        ts_index,
        point_count: positions.len(),
        out_of_order: false,
        max_wal_seq: chunk.max_wal_seq,
        min_wal_seq: chunk.min_wal_seq,
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

/// 计时埋点辅助：把 `Instant` 起始点换算为「毫秒（精确到 0.1ms）」的 f64。
///
/// 仅用于 `flush_timing` 观测日志；`Instant::elapsed` 为纳秒级单调时钟，开销
/// 相对被测段（毫秒级）可忽略，不影响测量值。
#[inline]
fn phase_ms(start: Instant) -> f64 {
    (start.elapsed().as_secs_f64() * 1000.0 * 10.0).round() / 10.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionType;
    use crate::memtable::TypedValue;

    #[test]
    fn test_current_epoch_ns() {
        let ns = current_epoch_ns();
        // 2024年之后的时间戳应 > 1.7e18
        assert!(ns > 1_700_000_000_000_000_000);
    }

    /// 不变式 I2：跨天 chunk 必须被硬切分成「各自落在单天分区窗口」的子 chunk。
    #[test]
    fn test_split_chunk_by_partition_cross_day() {
        let boundary = 1_704_038_400_000_000_000_i64; // 2024-01-01 00:00:00 UTC+8
        let day0_ts = boundary + 1_000;
        let day1_ts = boundary + 86_400_000_000_000_i64 + 1_000;

        let mut chunk = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, 7);
        chunk.append(day0_ts, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        chunk.append(day1_ts, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);
        let chunk = Arc::new(chunk);

        let subs = split_chunk_by_partition(&chunk, 86_400_000, 28_800);
        assert_eq!(subs.len(), 2, "跨天 chunk 应切成 2 段");

        assert_eq!(subs[0].timestamps, vec![day0_ts]);
        assert_eq!(subs[1].timestamps, vec![day1_ts]);
        assert_eq!(
            align_to_partition(subs[0].timestamps[0], 86_400_000, 28_800),
            boundary
        );
        assert_eq!(
            align_to_partition(subs[1].timestamps[0], 86_400_000, 28_800),
            boundary + 86_400_000_000_000_i64
        );
        // 子 chunk 的列值稀疏对齐，读回保真。
        assert_eq!(subs[0].get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
        assert_eq!(subs[1].get_typed_value(0, 0), Some(TypedValue::F64(2.0)));
    }

    /// 不变式 I2 反向：单天 chunk 不应被切分。
    #[test]
    fn test_split_chunk_by_partition_single_day_noop() {
        let boundary = 1_704_038_400_000_000_000_i64;
        let mut chunk = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, 7);
        chunk.append(boundary + 1, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        chunk.append(boundary + 2, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);
        let chunk = Arc::new(chunk);

        let subs = split_chunk_by_partition(&chunk, 86_400_000, 28_800);
        assert_eq!(subs.len(), 1, "单天 chunk 不应被切分");
        assert_eq!(subs[0].point_count, 2);
    }

    /// QA 独立验证（突变测试护栏）：跨天切分在「恰好天边界」处的归属正确 + 多天数据无丢无重。
    ///
    /// 护栏语义：若切分边界逻辑错位（把 `boundary` 误归前一天，等价于 `<=` 与 `<`
    /// 的分割错误），本测试必须 FAIL；任何点被丢弃或重复也会 FAIL。
    #[test]
    fn test_split_cross_day_boundary_exact_no_loss_no_dup() {
        let boundary = 1_704_038_400_000_000_000_i64; // 2024-01-01 00:00:00 UTC+8
        let day_ns = 86_400_000_000_000_i64;

        // 跨 3 天：day0(2023-12-31) 2 点、day1(2024-01-01) 3 点（含恰好边界点）、day2 1 点。
        // 故意乱序 append，验证切分依赖 ts_index 排序而非 append 顺序。
        let mut chunk = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, 7);
        chunk.append(boundary + 1, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);
        chunk.append(boundary - 1, &[(0, TypedValue::F64(0.0))], &[(0, 0xC0)], 0);
        chunk.append(
            boundary + day_ns,
            &[(0, TypedValue::F64(5.0))],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(boundary, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        chunk.append(boundary - 2, &[(0, TypedValue::F64(-1.0))], &[(0, 0xC0)], 0);
        chunk.append(
            boundary + day_ns - 1,
            &[(0, TypedValue::F64(4.0))],
            &[(0, 0xC0)],
            0,
        );
        let chunk = Arc::new(chunk);

        let subs = split_chunk_by_partition(&chunk, 86_400_000, 28_800);

        // 3 天 → 3 个子 chunk。
        assert_eq!(subs.len(), 3, "跨 3 天应切成 3 段");

        // 每个子 chunk 的 [min_ts, max_ts) 落单天窗口（不变式 I2）。
        for sub in &subs {
            assert!(!sub.timestamps.is_empty());
            let first = sub.timestamps[0];
            let last = *sub.timestamps.last().unwrap();
            let first_key = align_to_partition(first, 86_400_000, 28_800);
            let last_key = align_to_partition(last, 86_400_000, 28_800);
            assert_eq!(first_key, last_key, "子 chunk 不得跨天");
        }

        // 无丢无重：所有子 chunk 的 ts 并集 == 原 chunk ts_index 键集（有序）。
        let mut expected: Vec<i64> = chunk.ts_index.keys().copied().collect();
        let mut got: Vec<i64> = subs
            .iter()
            .flat_map(|s| s.timestamps.iter().copied())
            .collect();
        expected.sort_unstable();
        got.sort_unstable();
        assert_eq!(got, expected, "切分后必须无丢无重");

        // 边界归属（突变测试核心）：boundary 必须落 day1，绝不落 day0。
        let day0 = subs
            .iter()
            .find(|s| s.timestamps.contains(&(boundary - 1)))
            .unwrap();
        let day1 = subs
            .iter()
            .find(|s| s.timestamps.contains(&boundary))
            .unwrap();
        let day2 = subs
            .iter()
            .find(|s| s.timestamps.contains(&(boundary + day_ns)))
            .unwrap();
        assert!(
            !day0.timestamps.contains(&boundary),
            "boundary 不得归入前一天"
        );
        assert!(day1.timestamps.contains(&boundary), "boundary 必须归入当天");
        assert!(
            day2.timestamps.contains(&(boundary + day_ns)),
            "下一 day 边界点归下一天"
        );

        // 值保真：每个点读回值正确（无错位/丢列）。
        for sub in &subs {
            for (idx, ts) in sub.timestamps.iter().enumerate() {
                let want = match *ts {
                    t if t == boundary - 2 => -1.0,
                    t if t == boundary - 1 => 0.0,
                    t if t == boundary => 1.0,
                    t if t == boundary + 1 => 2.0,
                    t if t == boundary + day_ns - 1 => 4.0,
                    t if t == boundary + day_ns => 5.0,
                    other => panic!("unexpected ts {}", other),
                };
                assert_eq!(sub.get_typed_value(0, idx), Some(TypedValue::F64(want)));
            }
        }
    }

    /// P5：flush 失败重塞队头后重试成功。
    ///
    /// 1. 冻结一批数据；用指向「不可写目录」的 SacWriter 触发 write_chunk 失败；
    /// 2. `flush_once` 失败后把表 `push_front` 重塞队头（队列深度不变，不丢）；
    /// 3. 换「可写目录」的 SacWriter 重试，`flush_once` 成功排空队列。
    #[tokio::test]
    async fn test_flush_once_requeues_on_failure_then_retries() {
        let tmp = tempfile::tempdir().expect("tempdir");

        // 1. 冻结 1 张表（1 个点）。
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert(1, 100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        memtable.force_freeze();
        assert_eq!(memtable.immutable_len(), 1);

        // 2. 不可写目录：让 sac_dir 的父级是一个普通文件，create_dir_all 失败。
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, b"x").expect("write blocker file");
        let bad_sac_dir = blocker.join("sub");
        let bad_writer = Arc::new(SacWriter::new_with_encryptor(
            bad_sac_dir,
            CompressionType::Lz4,
            None,
        ));
        let bad_coord = FlushCoordinator::new(
            bad_writer,
            memtable.clone(),
            Arc::new(SacWatermark::new(tmp.path())),
        );

        let blocks = bad_coord.flush_once().await;
        assert_eq!(blocks, 0, "写失败应返回 0 blocks");
        assert_eq!(
            memtable.immutable_len(),
            1,
            "flush 失败后表应重塞队头，不丢"
        );

        // 3. 可写目录重试 → 成功排空。
        let good_writer = Arc::new(SacWriter::new_with_encryptor(
            tmp.path().join("good_sac"),
            CompressionType::Lz4,
            None,
        ));
        let good_coord = FlushCoordinator::new(
            good_writer,
            memtable.clone(),
            Arc::new(SacWatermark::new(tmp.path())),
        );

        let blocks2 = good_coord.flush_once().await;
        assert_eq!(blocks2, 1, "重试应成功写入 1 个 block");
        assert_eq!(memtable.immutable_len(), 0, "重试成功后队列应排空");
    }

    /// C2：flush 完成后推进 SAC 落盘水位 S = (未 flush 最小 seq) - 1。
    #[tokio::test]
    async fn test_flush_advances_sac_watermark() {
        use crate::engine::DataPoint;

        let tmp = tempfile::tempdir().expect("tempdir");
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));

        let point = |table_id: u32, ts: i64| DataPoint {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            ts,
            server_ts: ts + 1,
            values: vec![(0, TypedValue::F64(ts as f64))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        };

        // 第一代：seq 10/11 → freeze 成 immutable（队头，min=10）。
        memtable.insert_batch(&[point(1, 100), point(1, 200)], &[10, 11]);
        memtable.force_freeze();

        // 第二代：seq 20/21 → 仍在 active（min=20）。
        memtable.insert_batch(&[point(1, 300), point(1, 400)], &[20, 21]);

        let writer = Arc::new(SacWriter::new_with_encryptor(
            tmp.path().join("sac"),
            CompressionType::Lz4,
            None,
        ));
        let wm = Arc::new(SacWatermark::new(tmp.path()));
        let coord = FlushCoordinator::new(writer, memtable.clone(), wm.clone());

        let blocks = coord.flush_once().await;
        assert_eq!(blocks, 1, "应 flush 队头那张 immutable 表（1 chunk）");

        // 队头已 flush，剩余未 flush 最小 seq = active min = 20 → S = 19。
        assert_eq!(wm.current(), 19, "S 应推进到未 flush 最小 seq - 1");
    }

    /// C2/I2 突变测试护栏：SAC flush **失败**时（数据未 fsync 进 SAC），
    /// SAC 落盘水位 S 绝不得推进。若实现被改成「先推进 S 再 flush SAC」，
    /// 本测试必须 FAIL——否则回收器会误删「数据仍在 WAL 里、未落 SAC」的段。
    #[tokio::test]
    async fn test_flush_failure_does_not_advance_sac_watermark() {
        use crate::engine::DataPoint;

        let tmp = tempfile::tempdir().expect("tempdir");
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));

        let point = |table_id: u32, ts: i64| DataPoint {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            ts,
            server_ts: ts + 1,
            values: vec![(0, TypedValue::F64(ts as f64))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        };

        // 第一代 seq 10/11 → freeze（队头）；第二代 seq 20/21 → active（min=20）。
        memtable.insert_batch(&[point(1, 100), point(1, 200)], &[10, 11]);
        memtable.force_freeze();
        memtable.insert_batch(&[point(1, 300), point(1, 400)], &[20, 21]);

        // 不可写目录 → write_chunk 失败（SAC 无法 fsync 落盘）。
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, b"x").expect("write blocker file");
        let bad_sac_dir = blocker.join("sub");
        let bad_writer = Arc::new(SacWriter::new_with_encryptor(
            bad_sac_dir,
            CompressionType::Lz4,
            None,
        ));
        let wm = Arc::new(SacWatermark::new(tmp.path()));
        let coord = FlushCoordinator::new(bad_writer, memtable.clone(), wm.clone());

        let blocks = coord.flush_once().await;
        assert_eq!(blocks, 0, "写失败应返回 0 blocks");
        assert_eq!(
            wm.current(),
            0,
            "I2：SAC flush 失败时 S 不得推进（否则回收器会误删未落盘数据）"
        );
    }

    /// 阶段一验收：按 partition 并行 flush 后 seq 不覆盖（不变式 1）。
    ///
    /// 连续两轮 flush，每轮跨 2 天（= 2 个 partition 文件）。断言盘上最终有
    /// `轮数 × partition 数 = 4` 个 `.sac` 文件，且每个 partition 的 seq 为 `[1, 2]`
    /// （第二轮续号，不重名覆盖、不回退）。
    #[tokio::test]
    async fn test_parallel_flush_seq_no_overwrite_across_rounds() {
        use crate::engine::DataPoint;

        let tmp = tempfile::tempdir().expect("tempdir");
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));

        let day_ns: i64 = 86_400_000_000_000;
        let boundary: i64 = 1_704_038_400_000_000_000; // 2024-01-01 00:00:00 UTC+8

        let point = |table_id: u32, ts: i64| DataPoint {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            ts,
            server_ts: ts + 1,
            values: vec![(0, TypedValue::F64(ts as f64))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        };

        let writer = Arc::new(SacWriter::new_with_encryptor(
            tmp.path().join("sac"),
            CompressionType::Lz4,
            None,
        ));
        let coord = FlushCoordinator::new(
            writer.clone(),
            memtable.clone(),
            Arc::new(SacWatermark::new(tmp.path())),
        );

        // 第一轮：2 张表分属 2 天 → 2 个 partition 文件。
        memtable.insert_batch(
            &[point(1, boundary + 1), point(2, boundary + day_ns + 1)],
            &[10, 11],
        );
        memtable.force_freeze();
        let blocks = coord.flush_once().await;
        assert_eq!(blocks, 2, "跨 2 天应写 2 个 partition 文件");

        // 第二轮：再 2 张表分属 2 天 → 又 2 个 partition 文件（seq 续号）。
        memtable.insert_batch(
            &[point(3, boundary + 2), point(4, boundary + day_ns + 2)],
            &[20, 21],
        );
        memtable.force_freeze();
        let blocks2 = coord.flush_once().await;
        assert_eq!(blocks2, 2, "第二轮仍应写 2 个 partition 文件");

        // 盘上应恰好 4 个 .sac（2 partition × 2 轮），无重名覆盖。
        let naming = SacNaming::new(tmp.path().join("sac"));
        let segments = naming.list_tenant_segments(0).expect("list segments");
        assert_eq!(
            segments.len(),
            4,
            "并行 flush 后 seq 不得覆盖，文件数 == 轮数 × partition 数"
        );

        // 每个 partition 的 seq 序列应为 [1, 2]（无覆盖、无回退）。
        let mut seqs_by_partition: BTreeMap<i64, Vec<u16>> = BTreeMap::new();
        for p in &segments {
            let fname = p.file_name().unwrap().to_string_lossy().into_owned();
            let (pn, _level, seq) = SacNaming::parse_file_name(&fname).expect("parse file name");
            seqs_by_partition.entry(pn).or_default().push(seq);
        }
        assert_eq!(seqs_by_partition.len(), 2, "应有 2 个 partition");
        for (pn, mut seqs) in seqs_by_partition {
            seqs.sort_unstable();
            assert_eq!(seqs, vec![1, 2], "partition {pn} 的 seq 应连续 1,2 无覆盖");
        }
    }

    // ===================== QA 独立验证（fresh eyes，非工程师自测复述）=====================

    /// QA 独立验证：跨「重启」seq 续号不覆盖存量 SAC。
    ///
    /// 与工程师自测（同一 coordinator 连续两轮）不同，本测试在第二轮使用**全新的
    /// SacWriter/FlushCoordinator/MemTableManager**（仅共享同一 sac_dir），专门覆盖
    /// `next_seq_in_dir` 从磁盘重扫的跨重启续号语义。
    #[tokio::test]
    async fn qa_restart_seq_continues_no_overwrite() {
        use crate::engine::DataPoint;

        let tmp = tempfile::tempdir().expect("tempdir");
        let sac_dir = tmp.path().join("sac");
        let day_ns: i64 = 86_400_000_000_000;
        let boundary: i64 = 1_704_038_400_000_000_000;

        let point = |table_id: u32, ts: i64| DataPoint {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            ts,
            server_ts: ts + 1,
            values: vec![(0, TypedValue::F64(ts as f64))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        };

        // 第一代（进程内第一次 flush）。
        {
            let config = sensearcdb_common::ConfigHandle::new(Default::default());
            let memtable = Arc::new(MemTableManager::new(&config));
            let writer = Arc::new(SacWriter::new_with_encryptor(
                sac_dir.clone(),
                CompressionType::Lz4,
                None,
            ));
            let coord = FlushCoordinator::new(
                writer,
                memtable.clone(),
                Arc::new(SacWatermark::new(tmp.path())),
            );
            memtable.insert_batch(
                &[point(1, boundary + 1), point(2, boundary + day_ns + 1)],
                &[10, 11],
            );
            memtable.force_freeze();
            assert_eq!(coord.flush_once().await, 2, "第一代应写 2 个 partition 文件");
        }

        // 第二代（模拟重启：全新对象，同一 sac_dir）。
        {
            let config = sensearcdb_common::ConfigHandle::new(Default::default());
            let memtable = Arc::new(MemTableManager::new(&config));
            let writer = Arc::new(SacWriter::new_with_encryptor(
                sac_dir.clone(),
                CompressionType::Lz4,
                None,
            ));
            let coord = FlushCoordinator::new(
                writer,
                memtable.clone(),
                Arc::new(SacWatermark::new(tmp.path())),
            );
            memtable.insert_batch(
                &[point(3, boundary + 2), point(4, boundary + day_ns + 2)],
                &[20, 21],
            );
            memtable.force_freeze();
            assert_eq!(coord.flush_once().await, 2, "第二代应再写 2 个 partition 文件");
        }

        let naming = SacNaming::new(sac_dir);
        let segments = naming.list_tenant_segments(0).expect("list");
        assert_eq!(
            segments.len(),
            4,
            "跨重启后文件数 == 2 轮 × 2 partition，无重名覆盖"
        );

        let mut by_pn: BTreeMap<i64, Vec<u16>> = BTreeMap::new();
        for p in &segments {
            let (pn, _lvl, seq) =
                SacNaming::parse_file_name(p.file_name().unwrap().to_str().unwrap()).expect("parse");
            by_pn.entry(pn).or_default().push(seq);
        }
        assert_eq!(by_pn.len(), 2, "应仍为 2 个 partition");
        for (pn, mut seqs) in by_pn {
            seqs.sort_unstable();
            assert_eq!(seqs, vec![1, 2], "partition {pn} 的 seq 应跨重启连续 1,2");
        }
    }

    /// QA 独立验证：多 partition 部分失败 → S 水位 all-or-nothing 不推进、表重塞队头。
    ///
    /// 2 个 partition（两天），预先在第二个 partition 的 tmp 路径放一个同名目录使
    /// `File::create` 失败；另一 partition 正常落盘。断言：好 partition 的 .sac 已写
    /// （部分落盘允许残留，但不参与 S）、S 水位不推进、表重塞队头不丢弃。
    #[tokio::test]
    async fn qa_multi_partition_partial_failure_all_or_nothing() {
        use crate::engine::DataPoint;

        let tmp = tempfile::tempdir().expect("tempdir");
        let sac_dir = tmp.path().join("sac");
        let day_ns: i64 = 86_400_000_000_000;
        let boundary: i64 = 1_704_038_400_000_000_000;

        let point = |table_id: u32, ts: i64| DataPoint {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            ts,
            server_ts: ts + 1,
            values: vec![(0, TypedValue::F64(ts as f64))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        };

        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = Arc::new(MemTableManager::new(&config));
        memtable.insert_batch(
            &[point(1, boundary + 1), point(2, boundary + day_ns + 1)],
            &[10, 11],
        );
        memtable.force_freeze();
        assert_eq!(memtable.immutable_len(), 1);

        // 预先在「第二天 partition」的 tmp 路径放一个同名目录，使该 partition 写失败。
        let bad_pn = align_to_partition(boundary + day_ns + 1, 86_400_000, 28_800);
        let bad_tmp = SacNaming::new(sac_dir.clone()).tmp_path(0, bad_pn, SacLevel::L0, 1);
        std::fs::create_dir_all(&bad_tmp).expect("pre-create blocking dir");

        let writer = Arc::new(SacWriter::new_with_encryptor(
            sac_dir.clone(),
            CompressionType::Lz4,
            None,
        ));
        let wm = Arc::new(SacWatermark::new(tmp.path()));
        let coord = FlushCoordinator::new(writer, memtable.clone(), wm.clone());

        let blocks = coord.flush_once().await;
        assert_eq!(blocks, 0, "任一 partition 失败 → 整体失败（返回 0 blocks）");

        // 好 partition 已落盘（部分落盘允许残留）。
        let segments = SacNaming::new(sac_dir).list_tenant_segments(0).expect("list");
        assert_eq!(
            segments.len(),
            1,
            "好 partition 应已写 1 个 .sac（部分落盘残留，但不得参与 S 推进）"
        );

        // all-or-nothing：S 水位绝不推进。
        assert_eq!(
            wm.current(),
            0,
            "部分 partition 失败时 S 不得推进（all-or-nothing）"
        );

        // 表重塞队头，不丢弃。
        assert_eq!(memtable.immutable_len(), 1, "失败表应重塞队头");
    }

    /// QA 独立验证（拷贝消除 #2）：单天快路径 Arc::clone 复用原 chunk（零拷贝 + 零语义变化）。
    #[test]
    fn qa_single_day_fast_path_reuses_arc_zero_copy() {
        let boundary = 1_704_038_400_000_000_000_i64;
        let mut chunk = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, 7);
        chunk.append(boundary + 1, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        chunk.append(boundary + 2, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0);
        let chunk = Arc::new(chunk);

        let subs = split_chunk_by_partition(&chunk, 86_400_000, 28_800);
        assert_eq!(subs.len(), 1, "单天 chunk 不应切分");
        assert!(
            Arc::ptr_eq(&subs[0], &chunk),
            "单天快路径必须 Arc::clone 复用原 chunk，不得 build_sub_chunk 二次拷贝"
        );
        assert_eq!(subs[0].point_count, chunk.point_count);
        assert_eq!(subs[0].timestamps, chunk.timestamps);
    }

    /// 参照实现：旧「逐点×逐列」二次拷贝（`get_typed_value` + `push_typed`）。
    ///
    /// 仅用于 [`qa_build_sub_chunk_batch_copy_byte_identical`] 的逐字节一致性断言，
    /// 生产路径不再使用。与优化前的 `build_sub_chunk` 逐行等价。
    fn build_sub_chunk_reference(chunk: &ColumnarChunk, positions: &[usize]) -> ColumnarChunk {
        let mut col_ids: Vec<u16> = chunk.columns.keys().copied().collect();
        col_ids.sort_unstable();

        let mut columns: HashMap<u16, ColVec> = HashMap::with_capacity(col_ids.len());
        let mut presence: HashMap<u16, Vec<bool>> = HashMap::with_capacity(col_ids.len());
        for &id in &col_ids {
            let data_type = chunk.columns.get(&id).map(|c| c.data_type());
            columns.insert(
                id,
                ColVec::new_for_type(data_type.unwrap_or(sensearcdb_common::DataType::F64)),
            );
            presence.insert(id, Vec::with_capacity(positions.len()));
        }

        let mut timestamps = Vec::with_capacity(positions.len());
        let mut qualities = Vec::with_capacity(positions.len());
        let mut ts_index: BTreeMap<i64, usize> = BTreeMap::new();

        for (new_idx, &pos) in positions.iter().enumerate() {
            let ts = chunk.timestamps[pos];
            timestamps.push(ts);
            qualities.push(chunk.qualities[pos]);
            ts_index.insert(ts, new_idx);

            for &id in &col_ids {
                let col = columns.get_mut(&id).expect("column pre-inserted");
                match chunk.get_typed_value(id, pos) {
                    Some(v) => {
                        col.push_typed(&v);
                        presence
                            .get_mut(&id)
                            .expect("presence pre-inserted")
                            .push(true);
                    }
                    None => {
                        col.push_absent();
                        presence
                            .get_mut(&id)
                            .expect("presence pre-inserted")
                            .push(false);
                    }
                }
            }
        }

        ColumnarChunk {
            table_id: chunk.table_id,
            tenant_id: chunk.tenant_id,
            timestamps,
            columns,
            presence,
            qualities,
            ts_index,
            point_count: positions.len(),
            out_of_order: false,
            max_wal_seq: chunk.max_wal_seq,
            min_wal_seq: chunk.min_wal_seq,
        }
    }

    /// 构造覆盖 8 种物理类型列 + 稀疏缺失列的源 chunk（部分行缺列，presence 位图含 false）。
    fn mixed_type_sparse_chunk() -> ColumnarChunk {
        let mut chunk = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, 99);
        chunk.append(
            100,
            &[
                (0, TypedValue::F64(1.5)),
                (1, TypedValue::F32(2.5)),
                (2, TypedValue::I64(-3)),
                (3, TypedValue::U64(4)),
                (4, TypedValue::I32(-5)),
                (5, TypedValue::Bool(true)),
                (6, TypedValue::String("a".to_string())),
                (7, TypedValue::Enum16(11)),
            ],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(
            200,
            &[
                (0, TypedValue::F64(9.5)),
                (6, TypedValue::String("b".to_string())),
            ],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(
            300,
            &[
                (2, TypedValue::I64(-33)),
                (3, TypedValue::U64(44)),
                (5, TypedValue::Bool(false)),
            ],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(
            400,
            &[
                (1, TypedValue::F32(22.5)),
                (4, TypedValue::I32(-55)),
                (7, TypedValue::Enum16(77)),
            ],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(
            500,
            &[
                (0, TypedValue::F64(0.0)),
                (1, TypedValue::F32(0.0)),
                (2, TypedValue::I64(0)),
                (3, TypedValue::U64(0)),
                (4, TypedValue::I32(0)),
                (5, TypedValue::Bool(true)),
                (6, TypedValue::String(String::new())),
                (7, TypedValue::Enum16(0)),
            ],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(600, &[(7, TypedValue::Enum16(88))], &[(0, 0xC0)], 0);
        chunk
    }

    /// 拷贝消除 #3 逐字节一致性：批量拷贝 vs 旧逐点实现，输出（timestamps/columns/
    /// presence/qualities/ts_index）必须逐字节一致，覆盖 8 种类型列 + 稀疏缺失列。
    #[test]
    fn qa_build_sub_chunk_batch_copy_byte_identical() {
        let chunk = mixed_type_sparse_chunk();

        // 全量 + 多个子集（覆盖部分 pos 缺失、非连续子集）。
        let cases: Vec<Vec<usize>> = vec![
            vec![0, 1, 2, 3, 4, 5],
            vec![1, 3, 4],
            vec![0, 2, 5],
            vec![5, 0, 3],
        ];
        for positions in cases {
            let got = build_sub_chunk(&chunk, &positions);
            let want = build_sub_chunk_reference(&chunk, &positions);

            assert_eq!(got.timestamps, want.timestamps, "timestamps 逐字节一致");
            assert_eq!(got.qualities, want.qualities, "qualities 逐字节一致");
            assert_eq!(got.ts_index, want.ts_index, "ts_index 逐字节一致");
            assert_eq!(got.point_count, want.point_count);
            assert_eq!(got.table_id, want.table_id);
            assert_eq!(got.tenant_id, want.tenant_id);
            assert_eq!(got.columns, want.columns, "columns 逐字节一致（含稀疏占位）");
            assert_eq!(got.presence, want.presence, "presence 逐字节一致");
            assert_eq!(got.out_of_order, want.out_of_order);
            assert_eq!(got.max_wal_seq, want.max_wal_seq);
            assert_eq!(got.min_wal_seq, want.min_wal_seq);

            // 额外护栏：稀疏列读回保真（部分 pos 缺失必须返回 None，不得错填默认值）。
            for (idx, &pos) in positions.iter().enumerate() {
                for id in 0..8u16 {
                    let want_v = chunk.get_typed_value(id, pos);
                    assert_eq!(
                        got.get_typed_value(id, idx),
                        want_v,
                        "col {id} row {idx} (src pos {pos}) 读回保真"
                    );
                }
            }
        }
    }

    // =========================================================================
    // 以下为 QA 独立边界用例（fresh eyes，不依赖工程师自测结论）。核心是覆盖
    // `sub_col_present` 的「presence 位图缺失 → 密集回退」分支，该分支在 append
    // 构造的 chunk 中永不可达（append 恒为每列写 presence 位图），工程师的
    // `qa_build_sub_chunk_batch_copy_byte_identical` 未覆盖。
    // =========================================================================

    /// 直接构造列/presence 完全受控的 chunk（不走 append，避免 append 自动补 presence
    /// 位图），从而显式覆盖「presence 位图缺失 → 密集回退」等外部构造/SAC 密集读回场景。
    fn raw_chunk(
        timestamps: Vec<i64>,
        columns: HashMap<u16, ColVec>,
        presence: HashMap<u16, Vec<bool>>,
    ) -> ColumnarChunk {
        let point_count = timestamps.len();
        let qualities = vec![0xC0; point_count];
        let mut ts_index = BTreeMap::new();
        for (i, &ts) in timestamps.iter().enumerate() {
            ts_index.insert(ts, i);
        }
        ColumnarChunk {
            table_id: 42,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps,
            columns,
            presence,
            qualities,
            ts_index,
            point_count,
            out_of_order: false,
            max_wal_seq: Some(7),
            min_wal_seq: Some(3),
        }
    }

    /// 逐字段（非仅整体 struct）比对 build_sub_chunk 与参照实现，并额外读回保真。
    fn assert_byte_identical(
        got: &ColumnarChunk,
        want: &ColumnarChunk,
        positions: &[usize],
        src: &ColumnarChunk,
    ) {
        assert_eq!(got.timestamps, want.timestamps, "timestamps");
        assert_eq!(got.qualities, want.qualities, "qualities");
        assert_eq!(got.ts_index, want.ts_index, "ts_index");
        assert_eq!(got.columns, want.columns, "columns（含稀疏占位逐字节）");
        assert_eq!(got.presence, want.presence, "presence 位图逐字节");
        assert_eq!(got.point_count, want.point_count, "point_count");
        assert_eq!(got.table_id, want.table_id, "table_id");
        assert_eq!(got.tenant_id, want.tenant_id, "tenant_id");
        assert_eq!(got.out_of_order, want.out_of_order, "out_of_order");
        assert_eq!(got.max_wal_seq, want.max_wal_seq, "max_wal_seq");
        assert_eq!(got.min_wal_seq, want.min_wal_seq, "min_wal_seq");

        // 读回保真：稀疏缺失必须返回 None，绝不把占位默认值当真实值暴露。
        for (idx, &pos) in positions.iter().enumerate() {
            for &id in src.columns.keys() {
                assert_eq!(
                    got.get_typed_value(id, idx),
                    src.get_typed_value(id, pos),
                    "col {id} row {idx} (src pos {pos}) 读回保真"
                );
            }
        }
    }

    /// `sub_col_present` 语义直接单测：presence 位图优先、位图越界 false、
    /// 位图 true 但内层 Vec 短 → 缺失、presence 缺失 → 密集回退 len 判断。
    #[test]
    fn qa_independent_sub_col_present_semantics() {
        let p = vec![true, false, true];
        // 位图优先。
        assert!(sub_col_present(Some(&p), 9, 0));
        assert!(!sub_col_present(Some(&p), 9, 1));
        assert!(sub_col_present(Some(&p), 9, 2));
        // 位图越界（pos >= p.len()）→ false（复刻 is_present 的 unwrap_or(false)）。
        assert!(!sub_col_present(Some(&p), 9, 3), "位图越界应 false");
        assert!(!sub_col_present(Some(&p), 9, 999), "位图越界应 false");
        // 位图 true 但内层 Vec 短（pos >= src_len）→ 缺失（复刻 get_typed_value 的
        // is_present && v.get(pos).is_some() 取值条件，绝不越界读源 Vec）。
        assert!(sub_col_present(Some(&p), 3, 2));
        assert!(!sub_col_present(Some(&p), 3, 3), "位图 true 但 pos>=len 应缺失");
        // presence 缺失 → 密集回退（复刻 is_present 的 c.len() > row_idx）。
        assert!(sub_col_present(None, 3, 2));
        assert!(!sub_col_present(None, 3, 3));
        assert!(!sub_col_present(None, 3, 999));
    }

    /// 独立边界：8 种物理类型 × {密集回退 / 部分密集 / 整列稀疏 / 位图短于 Vec /
    /// 位图长于 Vec} 组合，逐字节比对 build_sub_chunk 与旧逐点参照实现。
    #[test]
    fn qa_independent_dense_fallback_presence_edges() {
        let ts = vec![100, 200, 300, 400];
        let mut columns = HashMap::new();
        // col0 F64：密集列，presence 缺失（整列密集回退），len=4。
        columns.insert(0u16, ColVec::F64(vec![1.0, 2.0, 3.0, 4.0]));
        // col1 F32：presence 位图短于 Vec（len=4，位图 [true,false]）。
        columns.insert(1u16, ColVec::F32(vec![1.0, 2.0, 3.0, 4.0]));
        // col2 I64：整列稀疏（len=4，presence 全 false）。
        columns.insert(2u16, ColVec::I64(vec![1, 2, 3, 4]));
        // col3 U64：位图含 true 但 Vec 短（len=2，位图全 true）→ pos>=2 应缺失。
        columns.insert(3u16, ColVec::U64(vec![1, 2]));
        // col4 I32：部分密集（len=2，presence 缺失）→ pos>=2 应缺失（密集回退 len 判断）。
        columns.insert(4u16, ColVec::I32(vec![10, 20]));
        // col5 Bool：密集，presence 缺失，len=4。
        columns.insert(5u16, ColVec::Bool(vec![true, false, true, false]));
        // col6 String：部分密集（len=3，presence 缺失）。
        columns.insert(
            6u16,
            ColVec::String(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        );
        // col7 Enum16：密集，presence 缺失，len=4。
        columns.insert(7u16, ColVec::Enum16(vec![11, 22, 33, 44]));

        let mut presence = HashMap::new();
        presence.insert(1u16, vec![true, false]);
        presence.insert(2u16, vec![false, false, false, false]);
        presence.insert(3u16, vec![true, true, true, true]);

        let chunk = raw_chunk(ts, columns, presence);

        let cases: Vec<Vec<usize>> = vec![
            vec![0, 1, 2, 3],
            vec![3, 0, 2],
            vec![1],
            vec![2, 3],
            vec![0],
        ];
        for positions in cases {
            let got = build_sub_chunk(&chunk, &positions);
            let want = build_sub_chunk_reference(&chunk, &positions);
            assert_byte_identical(&got, &want, &positions, &chunk);
        }
    }

    /// 独立边界：空 positions → 空子 chunk（列/presence 长度恒 0，无越界）。
    #[test]
    fn qa_independent_empty_positions() {
        let ts = vec![100, 200];
        let mut columns = HashMap::new();
        columns.insert(0u16, ColVec::F64(vec![1.0, 2.0]));
        columns.insert(1u16, ColVec::String(vec!["x".to_string()]));
        let chunk = raw_chunk(ts, columns, HashMap::new());

        let positions: Vec<usize> = vec![];
        let got = build_sub_chunk(&chunk, &positions);
        let want = build_sub_chunk_reference(&chunk, &positions);
        assert_byte_identical(&got, &want, &positions, &chunk);
        assert_eq!(got.point_count, 0, "空 positions 应产出空子 chunk");
        assert!(got.timestamps.is_empty());
        assert!(got.columns.get(&0).map(|c| c.len()).unwrap_or(1) == 0);
        assert!(got.columns.get(&1).map(|c| c.len()).unwrap_or(1) == 0);
        assert!(got.presence.get(&0).map(|p| p.is_empty()).unwrap_or(true));
        assert!(got.presence.get(&1).map(|p| p.is_empty()).unwrap_or(true));
    }
}

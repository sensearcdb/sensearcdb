//! WAL 写入器：Group Commit 策略（全模式统一 WAL2）。
//!
//! WalWriter 实现表级分级持久化（`docs/wal-tiered-durability-design.md` v1.4）：
//! - `group`（默认）/ `async`：`append_frame_v2` 写 page cache + MemTable 后**立即 ACK**；
//! - `durable`：入 `durable_pending`，单 flusher drain → append → fsync → `ack_watermark`；
//! - group commit 后台由「1MB 脏阈值 + 1s 兜底」驱动 fsync（`dirty_bytes` / `idle_duration`）。
//!
//! 单机写路径与集群 Raft 日志写路径共用本写入器：
//! - 单机：`write_group` / `write_durable` / `write_async` 三分级（group/async 立即 ACK、durable 等 fsync）；
//! - 集群：`append_raft_entries`（WAL2 帧，`raft_term/raft_index` 取自 Raft 日志，逐帧 fsync）。

use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use sensearcdb_common::{
    constants::WAL_FRAME_HEADER_SIZE_V2, write_attr, ConfigHandle, SenseArcError, TenantId,
};
use tokio::sync::{watch, Notify};

use crate::encryption::{EncryptContext, SharedEncryptor};
use crate::engine::{DataPoint, PointBatch, WriteResult};
use crate::memtable::col_vec::TypedValue;
use crate::memtable::manager::MemTableManager;
use crate::wal::format::{
    compute_wal_crc, WalFrameFlag, WalFrameHeaderV2, WAL_ENC_MAGIC, WAL_ENC_PREFIX_LEN,
};
use crate::wal::manifest::{SegmentMeta, WalManifest};
use crate::wal::segment::WalSegment;

/// 一条待持久化的 Raft 日志帧。
///
/// 由 cluster 层将 `openraft::Entry` bincode 序列化后传入；engine 侧不依赖
/// openraft 类型，只负责 LZ4 压缩 → TDE 加密 → 组 WAL2 帧 → fsync。
pub struct WalRaftFrame {
    /// Raft term。
    pub raft_term: u64,
    /// Raft log index。
    pub raft_index: u64,
    /// 已序列化的 entry 字节（bincode，未压缩）。
    pub data: Vec<u8>,
    /// 帧上记录的 Schema 版本号。
    pub schema_version: u32,
}

/// 一条已解码的 WAL2 帧（供 LogStore `build_index` 读取 Raft 日志）。
///
/// payload 为已解压 + 解密的明文（**未**按 DataPoint 反序列化），
/// 由调用方决定如何解释（Raft entry bincode 或 DataPoint 列表）。
#[derive(Debug, Clone)]
pub struct DecodedWalFrame {
    /// WAL 段序号。
    pub segment_seq: u64,
    /// 帧在段内的偏移。
    pub frame_offset: u64,
    /// Raft term。
    pub raft_term: u64,
    /// Raft log index。
    pub raft_index: u64,
    /// Schema 版本号。
    pub schema_version: u32,
    /// 明文 payload（解压 + 解密后）。
    pub payload: Vec<u8>,
}

/// WAL 写入器。
///
/// 接收 `PointBatch`，LZ4 压缩后写入 WAL 段文件，然后同步更新 MemTable。
///
/// 分级持久化（表级 durability，`docs/wal-tiered-durability-design.md` v1.4）：
/// - `group` / `async`：`append_frame_v2` 写 page cache + MemTable → **立即 ACK（不等 fsync）**；
/// - `durable`：入 `durable_pending` → 单 flusher drain 至空 → append → fsync → `ack_watermark`；
/// - group commit 后台由「1MB 脏阈值 + 1s 兜底」驱动 fsync（`dirty_bytes` / `idle_duration`）。
pub struct WalWriter {
    /// 全局 WAL 序列号（单调递增）。
    /// 单机模式下同时复用为 WAL2 帧的 `raft_index`（`raft_term=0`）。
    seq: AtomicU64,
    /// 已 fsync 的最大 WAL2 帧 seq（`raft_index`，单机自增）。
    /// 语义：所有 raft_index <= 该水位的 WAL 帧均已持久化，供 snapshot 恢复边界对齐
    /// （3.3 档 A：从 server_ts 时间水位切 seq 精确边界）。
    committed_watermark: AtomicU64,
    /// 当前活跃的 WAL 段。
    segment: RwLock<WalSegment>,
    /// WAL 文件目录。
    wal_dir: PathBuf,
    /// durable 档缓冲（入队即唤醒 flusher，立即 fsync）。
    durable_pending: Mutex<Vec<PendingEntry>>,
    /// 自上次 fsync 以来累计 append 的 **group 档脏字节数**（1MB 阈值 + 1s 兜底触发源）。
    /// async/durable 帧不计数：async 只搭顺风车、durable 走自身立即 fsync。
    dirty_bytes: AtomicU64,
    /// 自上次 fsync 以来累计 append 的**最大帧 seq（`raft_index`）**（跨 group/async/durable）。
    /// 每次 fsync 后据此推进 `committed_watermark`，随后清零（I3）。
    appended_seq: AtomicU64,
    /// 上次 flush 的时间。
    last_flush: Mutex<Instant>,
    /// 配置句柄（热加载友好）。
    config: ConfigHandle,
    /// 关联的 MemTable（写入后同步插入）。
    memtable: Option<Arc<MemTableManager>>,
    /// 可选加密器（TDE 关闭时为 `None`）。
    encryptor: SharedEncryptor,
    /// WAL 可写状态位：构造成功即 `true`；发生致命 I/O 错误后置 `false`。
    /// 供 readiness 探针判定「WAL 可写」。
    writable: AtomicBool,
    /// ACK 完成水位（`watch`）：**仅服务 durable 档**。flusher 每次 fsync 后
    /// `send(本轮覆盖的最大 ticket)`；等待者 `wait_for(|wm| *wm >= ticket)`。
    ack_watermark: watch::Sender<u64>,
    /// ACK 水位的长寿 receiver：常驻以阻止 `watch` channel 因「零接收者」被关闭
    /// （否则 flusher 先 `send` 会返回 `SendError`、值不落盘，等待者永远等不到）。
    /// 等待者经 [`WalWriter::wait_ack`] 用 `clone()` 派生独立 receiver 读最新值。
    ack_rx: watch::Receiver<u64>,
    /// 单调递增写入票号（仅 durable 子批入队时领取）。
    ticket: AtomicU64,
    /// flusher 唤醒信号（durable 入队即 `notify_one`；group 1MB 阈值也 `notify_one`）。
    /// 与 ACK `watch` 分离：flusher 唤醒用 `Notify`（丢唤醒无害，drain-loop 会追平）。
    flusher_notify: Notify,
    /// WAL 段 → 数据 seq 索引（C1）。rollover 时追加记录并原子写盘。
    manifest: Mutex<WalManifest>,
    /// 当前活跃段内**最小**数据 seq（帧 `raft_index`，`u64::MAX` = 未写入）。
    /// 供 rollover 时 finalize 进 manifest。
    cur_min_seq: AtomicU64,
    /// 当前活跃段内**最大**数据 seq（帧 `raft_index`，`0` = 未写入）。
    cur_max_seq: AtomicU64,
}

/// 一个待 flush 的 durable 写入子批（durable 档的入队单元）。
struct PendingEntry {
    /// ACK 票号（本子批完成条件 = `ack_watermark >= ticket`）。
    ticket: u64,
    /// 本子批首点 WAL seq（即该帧 `raft_index`）。
    seq_start: u64,
    /// 数据点。
    points: Vec<DataPoint>,
    /// 各点 WAL seq（`seq_start..seq_start+n`），供 MemTable 回填 `max_wal_seq`。
    seqs: Vec<u64>,
}

/// 一帧已完成「serialize + LZ4 + TDE 加密 + 组帧头」的 WAL2 帧（P2 锁外预计算产物）。
///
/// 只差锁内的物理 append（rollover + 追加 + seq 收口）。锁外计算与锁内计算逐字节一致，
/// 因为 TDE 加密的 AAD 阶段 1 恒空（见 [`WalWriter::prepare_frame`]）。
struct PreparedFrame {
    /// WAL2 帧头（CRC 已在锁外算好）。
    header: WalFrameHeaderV2,
    /// 帧 payload（已压缩 + 加密，含 WALE 自描述前缀）。
    payload: Vec<u8>,
    /// 本帧首点 WAL seq（`raft_index`）。
    seq_start: u64,
}

/// 扫描 `wal_dir` 下所有 `.wal` 段，返回三值：
/// `(min_frame_seq, max_frame_seq, max_point_seq)`。
///
/// - `min_frame_seq` / `max_frame_seq`：帧级 `raft_index` 的最小/最大（帧首点 seq）。
/// - `max_point_seq`：**点级上界** `raft_index + point_count - 1` 的最大值（`point_count=0`
///   按 1 点处理，即点级上界 = `raft_index`），用于跨重启 seq 续号。
///
/// 只读每帧 37B 帧头（[`WalFrameHeaderV2::from_bytes`]），**不解密 payload、不做 CRC
/// 校验**；用 `data_len` 跳过 payload。`magic` 不匹配或读不满帧头 → 停止该段（保守：
/// 不再向后猜，绝不基于残缺帧续号）。段内读到 raft_index=0 的帧（如控制帧）不影响
/// max，但会拉低 min；min 仅用于 `first_data_seq`（reaper 判删只看 `last_data_seq`），
/// 故此处不按 flag 过滤。
///
/// 空目录 / 无有效帧 → `(u64::MAX, 0, 0)`。
///
/// ## 用途（跨重启 seq 续号，消除双 epoch，根治 🟥 隐患）
///
/// WAL 数据 seq（单机 `raft_index`）此前每进程硬编码从 1 重启（`seq: AtomicU64::new(1)`），
/// 而 SAC 水位 `S`（`sac_watermark.bin`）与 snapshot 水位 `wal_watermark` 按旧 seq 空间
/// 跨重启持久化。叠加段文件 `append` 复用 + manifest 重置为空，导致复用段里上一进程
/// 「已 fsync 但未 flush 进 SAC」的数据（seq > S）在 reaper 按本轮小 seq 判 `≤R` 时被
/// 连带删除。启动时扫描既有段最大点级 seq 并从 `max_point_seq+1` 续号，让
/// S / wal_watermark / manifest / seq 回到同一 seq 空间。
///
/// 关键粒度：`seq` 是**点级**计数器（`fetch_add(n)`），一帧可含 N 个点（`raft_index`
/// 只记首点 seq）。帧头因此新增 `point_count`，续号必须用点级上界 `max_point_seq`，
/// 而帧级变量（S / committed_watermark / manifest / cur_min/cur_max）仍用帧级 seq。
fn scan_raft_index_range(wal_dir: &Path) -> (u64, u64, u64) {
    let mut segments: Vec<PathBuf> = match fs::read_dir(wal_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "wal"))
            .collect(),
        Err(_) => return (u64::MAX, 0, 0),
    };
    // 按文件名排序（段序号递增），确定性扫描。
    segments.sort();

    let mut min_frame = u64::MAX;
    let mut max_frame = 0u64;
    let mut max_point = 0u64;

    for seg in &segments {
        let mut file = match File::open(seg) {
            Ok(f) => f,
            Err(_) => continue,
        };
        loop {
            let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE_V2];
            if file.read_exact(&mut header_buf).is_err() {
                break; // 读不满 37B 帧头 → 停止该段
            }
            let header = match WalFrameHeaderV2::from_bytes(&header_buf) {
                Some(h) => h,
                None => break, // magic 不匹配 → 停止该段
            };
            min_frame = min_frame.min(header.raft_index);
            max_frame = max_frame.max(header.raft_index);
            // 点级上界 = 首点 seq + 点数 - 1；point_count=0（控制帧 / Raft 单 entry 帧）按 1 点。
            let point_upper = header
                .raft_index
                .saturating_add(header.point_count.saturating_sub(1) as u64);
            max_point = max_point.max(point_upper);
            let data_len = header.data_len as i64;
            if data_len > 0 && file.seek(SeekFrom::Current(data_len)).is_err() {
                break; // payload 越界/不可 seek → 停止该段
            }
        }
    }

    (min_frame, max_frame, max_point)
}

/// 扫描 `wal_dir` 下所有 `.wal` 段，返回全局最大**帧级**数据 seq（`raft_index`）。
///
/// 空目录 / 无有效帧 → `0`。仅读帧头、不解密、不校验 CRC，启动期快速扫描。
/// 供 frame 级水位 / manifest / cur_min/cur_max 对齐使用（**不**用于点级 seq 续号）。
pub fn scan_max_raft_index(wal_dir: &Path) -> u64 {
    scan_raft_index_range(wal_dir).1
}

/// 扫描 `wal_dir` 下所有 `.wal` 段，返回全局最大**点级**数据 seq 上界。
///
/// 空目录 / 无有效帧 → `0`。用于 [`WalWriter::new_with_encryptor`] 跨重启续号：
/// `seq = max_point_seq + 1`。多点帧（一帧 N 点）只有 `raft_index`（首点 seq）时会被
/// 低估，本函数用帧头 `point_count` 计算点级上界 `raft_index + point_count - 1`。
pub fn scan_max_point_seq(wal_dir: &Path) -> u64 {
    scan_raft_index_range(wal_dir).2
}

impl WalWriter {
    /// 创建新的 WAL 写入器（TDE 关闭）。
    ///
    /// # 参数
    /// - `config`: 配置句柄（支持运行时热加载 WAL 参数）。
    pub fn new(config: &ConfigHandle) -> Result<Self, SenseArcError> {
        Self::new_with_encryptor(config, None)
    }

    /// 创建带 TDE 加密器的 WAL 写入器。
    ///
    /// # 参数
    /// - `config`: 配置句柄（支持运行时热加载 WAL 参数）。
    /// - `encryptor`: 可选加密器（`None` = TDE 关）。
    pub fn new_with_encryptor(
        config: &ConfigHandle,
        encryptor: SharedEncryptor,
    ) -> Result<Self, SenseArcError> {
        let cfg = config.get();
        let wal_dir = cfg.paths.wal_dir();

        // 确保 WAL 目录存在
        fs::create_dir_all(&wal_dir)
            .map_err(|e| SenseArcError::WalIoError(format!("mkdir: {}", e)))?;

        // 🟥 修复（跨重启 seq 续号）：扫描既有 WAL 段的「最小/最大帧级 seq + 最大点级 seq」。
        // 空目录 → (u64::MAX, 0, 0)。必须在 `Ok(Self{..})` 之前完成，seq 从 max_point_seq+1
        // 续号（点级上界），让 S / wal_watermark / manifest / seq 回到同一 seq 空间（消除双 epoch）。
        let (min_frame_seq, max_frame_seq, max_point_seq) = scan_raft_index_range(&wal_dir);

        let segment = WalSegment::create_new(&wal_dir, 0)
            .map_err(|e| SenseArcError::WalIoError(format!("create segment: {}", e)))?;

        let (ack_tx, ack_rx) = watch::channel(0u64);

        // 🟥 修复：manifest **不再重置为空**，而是 load 既有 manifest（缺失/损坏→空）。
        // seq 跨重启续号后，旧 manifest 条目（旧段的数据 seq 区间）与新一轮 seq 空间一致，
        // 必须保留，否则 reaper 仍不知道旧段区间、或复用段 finalize 后漏掉上一进程区间。
        let manifest = WalManifest::load(&wal_dir)?;

        Ok(Self {
            // 🟥 修复：seq 从「扫描出的最大**点级** seq 上界 + 1」续号（空目录 → 1，与原行为一致），
            // 不再硬编码从 1 重启。多点帧必须用 max_point_seq（raft_index + point_count - 1），
            // 而非帧首 max_frame_seq，否则与旧帧剩余点 seq 碰撞。
            seq: AtomicU64::new(max_point_seq + 1),
            committed_watermark: AtomicU64::new(0),
            segment: RwLock::new(segment),
            wal_dir,
            durable_pending: Mutex::new(Vec::new()),
            dirty_bytes: AtomicU64::new(0),
            appended_seq: AtomicU64::new(0),
            last_flush: Mutex::new(Instant::now()),
            config: config.clone(),
            memtable: None,
            encryptor,
            writable: AtomicBool::new(true),
            ack_watermark: ack_tx,
            ack_rx,
            ticket: AtomicU64::new(0),
            flusher_notify: Notify::new(),
            manifest: Mutex::new(manifest),
            // 🟥 修复：cur_min/cur_max 初始化为「复用段里上一进程旧帧的 seq 区间」，
            // 而非 u64::MAX/0。这样复用段 finalize 时：
            //  - `last_data_seq`（cur_max_seq）≥ 段内真实最大帧 seq（reaper 判删依据，绝不低估）；
            //  - `first_data_seq`（cur_min_seq）覆盖上一进程区间（不漏记最老帧）。
            // 空目录 → (u64::MAX, 0)，与原行为一致。
            // 关键粒度：cur_min/cur_max 是**帧级**变量（manifest 只记帧首 seq），故用帧级
            // min_frame_seq / max_frame_seq，而非点级 max_point_seq。
            cur_min_seq: AtomicU64::new(min_frame_seq),
            cur_max_seq: AtomicU64::new(max_frame_seq),
        })
    }

    /// 设置关联的 MemTable。
    ///
    /// 写入 WAL 后数据会同步插入到此 MemTable。
    pub fn set_memtable(&mut self, memtable: Arc<MemTableManager>) {
        self.memtable = Some(memtable);
    }

    /// 当前活跃 WAL 段的段 seq（文件名 `{seq:08}.wal` 中的 seq）。
    ///
    /// 供 WAL 旧段回收器（C3）作为「绝不删活跃段」的额外护栏（I1）。manifest 只含
    /// 已 rollover（finalized）的段，活跃段本就不在其中，此护栏为双保险。
    pub fn active_segment_seq(&self) -> u64 {
        self.segment.read().seq()
    }

    /// WAL 文件目录路径。
    ///
    /// 供 WAL 旧段回收器（C3）定位段文件与 manifest。
    pub fn wal_dir(&self) -> PathBuf {
        self.wal_dir.clone()
    }

    /// 领取一个单调递增写入票号。
    fn alloc_ticket(&self) -> u64 {
        self.ticket.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// 为 `n` 个点分配连续 WAL seq，返回 `(首点 seq, 各点 seq)`。
    ///
    /// 用一次 `fetch_add(n)` 原子取区间起点，`start..start+n` 生成连续 seq，保证
    /// 「首点 seq 与各点 seq」严格一致且多写者并发下区间全局无重叠（修复 start 的
    /// `load` 快照与 `seqs` 的 `fetch_add` 之间无原子性的并发撞车 bug）。
    fn alloc_seqs(&self, n: usize) -> (u64, Vec<u64>) {
        let start = self.seq.fetch_add(n as u64, Ordering::SeqCst);
        let seqs: Vec<u64> = (start..start + n as u64).collect();
        (start, seqs)
    }

    /// 阻塞等待本票号被某次 fsync 覆盖（`ack_watermark >= ticket`）。
    ///
    /// 用 `watch::Receiver::wait_for`（持状态值、先判后等、不丢事件），
    /// 而非 `Notify`（其 `notified()` future 一次性、丢唤醒），见设计 §5.3。
    async fn wait_ack(&self, ticket: u64) -> Result<(), SenseArcError> {
        // 从长寿 receiver 克隆：`ack_rx` 常驻保证 channel 永不因「零接收者」被关闭
        // （否则 flusher 先 `send` 时 `watch::Sender::send` 会返回 SendError，值不落盘）。
        let mut rx = self.ack_rx.clone();
        rx.wait_for(|wm| *wm >= ticket)
            .await
            .map_err(|e| SenseArcError::Internal(format!("ack watermark closed: {e}")))?;
        Ok(())
    }

    /// 批量写入数据点（单机写路径，兼容入口：等价「直接 append + 立即同步 fsync」）。
    ///
    /// 生产 `Engine::write_batch` 已按 durability 分流到 `write_group`/`write_durable`/
    /// `write_async`，本方法保留给直接使用 `WalWriter` 的调用方与测试（无后台 loop 也自洽：
    /// 同步 append + fsync，返回前数据已落盘）。
    pub async fn write_batch(&self, batch: PointBatch) -> Result<WriteResult, SenseArcError> {
        let points = batch.points;
        if points.is_empty() {
            return Ok(WriteResult {
                written: 0,
                wal_seq_start: 0,
                seqs: vec![],
            });
        }
        let written = points.len();
        let (seq_start, seqs) = self.alloc_seqs(written);
        // P2：锁外预计算帧（serialize+lz4+encrypt），锁内只做物理 append + fsync。
        let prepared = self.prepare_frame(&points, seq_start)?;
        {
            let mut segment = self.segment.write();
            self.append_prepared_locked(&mut segment, &prepared)?;
            self.fsync_and_commit_locked(&mut segment)?;
        }
        if let Some(ref memtable) = self.memtable {
            memtable.insert_batch(&points, &seqs);
        }
        Ok(WriteResult {
            written,
            wal_seq_start: seq_start,
            seqs,
        })
    }

    /// group 档写入口（v1.4）：`append_frame_v2` 写 page cache + MemTable → **立即 ACK**。
    ///
    /// 不领取 ticket、不等 `ack_watermark`（I5）。其 fsync 由后台 group commit 的
    /// 「1MB 脏阈值 / 1s 兜底」驱动（写路径仅在 dirty_bytes ≥ 阈值时 notify 一次）。
    pub async fn write_group(&self, points: Vec<DataPoint>) -> Result<WriteResult, SenseArcError> {
        if points.is_empty() {
            return Ok(WriteResult {
                written: 0,
                wal_seq_start: 0,
                seqs: vec![],
            });
        }
        let written = points.len();
        let (seq_start, seqs) = self.alloc_seqs(written);
        // P2：锁外预计算帧（serialize+lz4+encrypt），锁内只做物理 append。
        let prepared = self.prepare_frame(&points, seq_start)?;
        // P2 归因埋点：锁内物理 append（含 lock_wait_wal，锁内只 I/O + seq 收口）。
        let wai_t0 = Instant::now();
        {
            let mut segment = write_attr::timed_write_lock(&self.segment, write_attr::LockSlot::WalSegment);
            self.append_prepared_locked(&mut segment, &prepared)?;
        }
        write_attr::add_work(
            write_attr::WorkSlot::WalAppendIo,
            wai_t0.elapsed().as_nanos() as u64,
        );
        // 复用 §5.2 字节估算口径：累计 group 档脏字节数（1MB 阈值触发源，与压缩率无关）。
        let dirty_estimate = estimate_points_bytes(&points);
        self.dirty_bytes
            .fetch_add(dirty_estimate as u64, Ordering::SeqCst);
        if let Some(ref memtable) = self.memtable {
            // P2 归因埋点：memtable append 实际 CPU（含 lock_wait_memtable）。
            let mti_t0 = Instant::now();
            memtable.insert_batch(&points, &seqs);
            write_attr::add_work(
                write_attr::WorkSlot::MemtableInsert,
                mti_t0.elapsed().as_nanos() as u64,
            );
        }
        // I1 + 1MB 阈值：group 脏字节达阈值即唤醒后台 fsync（ACK 本就不等它）。
        if self.dirty_bytes.load(Ordering::SeqCst)
            >= self.config.get().engine.wal_group_commit_max_bytes as u64
        {
            self.flusher_notify.notify_one();
        }
        Ok(WriteResult {
            written,
            wal_seq_start: seq_start,
            seqs,
        })
    }

    /// durable 档写入口：入队 `durable_pending`，入队即唤醒 flusher（独立立即触发）。
    pub async fn write_durable(
        &self,
        points: Vec<DataPoint>,
    ) -> Result<WriteResult, SenseArcError> {
        if points.is_empty() {
            return Ok(WriteResult {
                written: 0,
                wal_seq_start: 0,
                seqs: vec![],
            });
        }
        let written = points.len();
        let ticket = self.alloc_ticket();
        let (seq_start, seqs) = self.alloc_seqs(written);
        // 入队会把 points/seqs 移入 PendingEntry；先 clone 一份用于 WriteResult 回传。
        let result_seqs = seqs.clone();
        {
            let mut d = self.durable_pending.lock().unwrap();
            d.push(PendingEntry {
                ticket,
                seq_start,
                points,
                seqs,
            });
        }
        // I2：durable 入队即唤醒 flusher，绝不等到 group 的 1MB/1s。
        self.flusher_notify.notify_one();
        self.wait_ack(ticket).await?;
        Ok(WriteResult {
            written,
            wal_seq_start: seq_start,
            seqs: result_seqs,
        })
    }

    /// async 档写入口：组帧写 OS 缓冲 + MemTable，**不 fsync**，立即 ACK（v1.4 与 group 同路径）。
    pub async fn write_async(&self, points: Vec<DataPoint>) -> Result<WriteResult, SenseArcError> {
        if points.is_empty() {
            return Ok(WriteResult {
                written: 0,
                wal_seq_start: 0,
                seqs: vec![],
            });
        }
        let written = points.len();
        let (seq_start, seqs) = self.alloc_seqs(written);
        // P2：锁外预计算帧（serialize+lz4+encrypt），锁内只做物理 append。
        let prepared = self.prepare_frame(&points, seq_start)?;
        // P2 归因埋点：锁内物理 append（含 lock_wait_wal）。
        let wai_t0 = Instant::now();
        {
            let mut segment = write_attr::timed_write_lock(&self.segment, write_attr::LockSlot::WalSegment);
            self.append_prepared_locked(&mut segment, &prepared)?;
        }
        write_attr::add_work(
            write_attr::WorkSlot::WalAppendIo,
            wai_t0.elapsed().as_nanos() as u64,
        );
        if let Some(ref memtable) = self.memtable {
            // P2 归因埋点：memtable append 实际 CPU（含 lock_wait_memtable）。
            let mti_t0 = Instant::now();
            memtable.insert_batch(&points, &seqs);
            write_attr::add_work(
                write_attr::WorkSlot::MemtableInsert,
                mti_t0.elapsed().as_nanos() as u64,
            );
        }
        Ok(WriteResult {
            written,
            wal_seq_start: seq_start,
            seqs,
        })
    }

    /// 将一组已序列化的 Raft 日志 entry 持久化为 WAL2 帧（集群写路径）。
    ///
    /// 流程：LZ4 压缩 →（可选 TDE 加密，复用 WALE 前缀管线）→ 组 WAL2 帧 → fsync。
    ///
    /// # 参数
    /// - `frames`: 待持久化的 Raft 日志帧（entry 已 bincode 序列化）。
    ///
    /// # 返回
    /// 每个帧落盘后的 `(segment_seq, frame_offset)`，供上层重建 `raft_index → 位置` 索引。
    pub fn append_raft_entries(
        &self,
        frames: &[WalRaftFrame],
    ) -> Result<Vec<(u64, u64)>, SenseArcError> {
        let mut locations = Vec::with_capacity(frames.len());

        for frame in frames {
            let compressed = crate::compression::compress_lz4(&frame.data);

            // 加密上下文（AAD 阶段 1 恒空，取值不影响密文正确性）。
            // Raft 日志帧不携带单租户语义，按默认租户 0 处理（控制面）。
            let (ctx_seg_seq, ctx_offset) = {
                let seg = self.segment.read();
                (seg.seq(), seg.bytes_written())
            };
            let frame_payload =
                self.encrypt_payload(TenantId::DEFAULT, &compressed, ctx_seg_seq, ctx_offset)?;

            let header = Self::build_v2_header(
                WalFrameFlag::Data,
                frame.schema_version,
                frame.raft_term,
                frame.raft_index,
                0, // point_count：Raft 单 entry 帧按 1 点（0 → 扫描按 1 点处理）
                &frame_payload,
            );

            let mut segment = self.segment.write();
            self.rollover_if_needed(&mut segment, frame_payload.len())?;
            let offset = segment.bytes_written();
            segment
                .append_frame_v2(&header, &frame_payload)
                .map_err(|e| SenseArcError::WalIoError(format!("write: {}", e)))?;
            // 集群 Raft 路径语义由 Raft 保证，按帧 fsync（保持拆分前 write_frame_v2 的逐帧落盘行为）。
            segment
                .fsync()
                .map_err(|e| SenseArcError::WalIoError(format!("fsync: {}", e)))?;
            locations.push((segment.seq(), offset));
        }

        Ok(locations)
    }

    /// 扫描 WAL 段文件并解码所有 WAL2 帧（header + LZ4 解压 + TDE 解密）。
    ///
    /// 返回的 payload 为明文，供 cluster `LogStore::build_index` 重建 Raft 索引。
    /// 解码失败（CRC/解密失败/截断）的帧按尽力恢复跳过。
    pub fn scan_frames(&self) -> Result<Vec<DecodedWalFrame>, SenseArcError> {
        let mut frames = Vec::new();

        let mut segment_paths: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(&self.wal_dir)
            .map_err(|e| SenseArcError::WalIoError(format!("read dir: {}", e)))?
        {
            let entry = entry.map_err(|e| SenseArcError::WalIoError(format!("entry: {}", e)))?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "wal") {
                segment_paths.push(path);
            }
        }
        segment_paths.sort();

        for seg_path in segment_paths {
            let seg_seq = seg_path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            let data = fs::read(&seg_path)
                .map_err(|e| SenseArcError::WalIoError(format!("read segment: {}", e)))?;

            let mut offset: usize = 0;
            let hdr_size = WAL_FRAME_HEADER_SIZE_V2;
            while offset + hdr_size <= data.len() {
                let hdr_bytes: [u8; WAL_FRAME_HEADER_SIZE_V2] =
                    match data[offset..offset + hdr_size].try_into() {
                        Ok(b) => b,
                        Err(_) => break,
                    };

                let header = match WalFrameHeaderV2::from_bytes(&hdr_bytes) {
                    Some(h) => h,
                    None => break,
                };

                let payload_start = offset + hdr_size;
                let payload_len = header.data_len as usize;
                if payload_start + payload_len > data.len() {
                    break;
                }

                let stored = &data[payload_start..payload_start + payload_len];

                // CRC 校验
                if !header.verify_crc(stored) {
                    tracing::warn!(
                        segment = seg_seq,
                        offset = offset,
                        "WAL frame CRC mismatch, skipping"
                    );
                    offset = payload_start + payload_len;
                    continue;
                }

                // 解密（WALE 前缀自描述）或明文
                let plain: Vec<u8> = if stored.len() >= WAL_ENC_PREFIX_LEN
                    && stored[0..4] == *WAL_ENC_MAGIC
                {
                    let encryptor = match &self.encryptor {
                        Some(e) => e,
                        None => {
                            return Err(crate::encryption::EncryptError::NoEncryptor.into());
                        }
                    };
                    let tenant_id = TenantId(u32::from_le_bytes(
                        stored[4..8].try_into().unwrap_or([0u8; 4]),
                    ));
                    let dek_version =
                        u32::from_le_bytes(stored[8..12].try_into().unwrap_or([0u8; 4]));
                    let ctx =
                        EncryptContext::wal_frame(tenant_id, seg_seq, offset as u64, dek_version);
                    match encryptor.decrypt(&stored[WAL_ENC_PREFIX_LEN..], &ctx) {
                        Ok(plain) => plain,
                        Err(e) => {
                            tracing::warn!(segment = seg_seq, offset = offset, error = %e, "WAL frame decrypt failed, skipping");
                            offset = payload_start + payload_len;
                            continue;
                        }
                    }
                } else {
                    stored.to_vec()
                };

                let decompressed = crate::compression::decompress_lz4(&plain);

                frames.push(DecodedWalFrame {
                    segment_seq: seg_seq,
                    frame_offset: offset as u64,
                    raft_term: header.raft_term,
                    raft_index: header.raft_index,
                    schema_version: header.schema_version,
                    payload: decompressed,
                });

                offset = payload_start + payload_len;
            }
        }

        Ok(frames)
    }

    /// Group Commit 后台主循环（v1.4：单 flusher + 双触发）。
    ///
    /// 触发源（OR）：
    /// 1. durable 入队 —— `write_durable` 已 `notify_one`（立即）；
    /// 2. group 1MB 脏阈值 —— `write_group` 已 `notify_one`；
    /// 3. 1s 兜底 —— 本循环 `sleep(max_interval)` 超时（低流量推进 committed_watermark）。
    ///
    /// 应由 `Engine::new` 通过 `tokio::spawn` 启动。
    pub async fn group_commit_loop(&self) {
        loop {
            {
                let max_interval = Duration::from_secs(
                    self.config.get().engine.wal_group_commit_max_interval_secs,
                );
                tokio::select! {
                    _ = self.flusher_notify.notified() => {}
                    _ = tokio::time::sleep(max_interval) => {}
                }
            }
            if let Err(e) = self.background_tick().await {
                tracing::warn!(error = %e, "group commit background tick failed");
                self.mark_unwritable();
            }
        }
    }

    /// 单次后台 tick：先 drain durable（append→fsync→ack），再按 1MB/1s 触发 group commit fsync。
    async fn background_tick(&self) -> Result<(), SenseArcError> {
        // 1. durable flusher：drain durable_pending → append → fsync → ack_watermark。
        if !self.durable_pending.lock().unwrap().is_empty() {
            self.drain_durable_flush().await?;
        }
        // 2. group commit：group 脏字节 ≥ 1MB，或空闲 ≥ 1s（兜底）→ 一次 fsync。
        let max_bytes = self.config.get().engine.wal_group_commit_max_bytes as u64;
        let max_interval =
            Duration::from_secs(self.config.get().engine.wal_group_commit_max_interval_secs);
        let dirty = self.dirty_bytes.load(Ordering::SeqCst);
        if dirty > 0 && (dirty >= max_bytes || self.idle_duration() >= max_interval) {
            let mut segment = self.segment.write();
            self.fsync_and_commit_locked(&mut segment)?;
        }
        Ok(())
    }

    /// durable 单 flusher：drain-loop 直到 `durable_pending` 空（I4）。
    ///
    /// 每轮：一次性 `mem::take` 取空（禁止部分 drain）→ append 全部帧 → 一次 fsync →
    /// MemTable 插入 → 推进 `committed_watermark` 与 `ack_watermark`。完成后立即复查，
    /// 仍非空则无需等新触发、继续下一轮（有界性由入口背压保证）。
    async fn drain_durable_flush(&self) -> Result<(), SenseArcError> {
        loop {
            let durable_entries: Vec<PendingEntry> = {
                let mut d = self.durable_pending.lock().unwrap();
                std::mem::take(&mut *d)
            };
            if durable_entries.is_empty() {
                return Ok(());
            }

            let mut max_ticket = 0u64;
            // P2：锁外预计算全部帧（serialize+lz4+encrypt），锁内只做物理 append + 一次 fsync。
            let prepared_frames: Vec<PreparedFrame> = durable_entries
                .iter()
                .map(|entry| self.prepare_frame(&entry.points, entry.seq_start))
                .collect::<Result<_, _>>()?;
            // 持有段写锁贯穿「追加全部帧 + 一次 fsync」，避免 group/async 帧交错破坏恢复顺序。
            {
                let mut segment = write_attr::timed_write_lock(&self.segment, write_attr::LockSlot::WalSegment);
                for (entry, frame) in durable_entries.iter().zip(prepared_frames.iter()) {
                    self.append_prepared_locked(&mut segment, frame)?;
                    max_ticket = max_ticket.max(entry.ticket);
                }
                self.fsync_and_commit_locked(&mut segment)?;
            }

            // MemTable 插入（fsync 之后，与既有「先 WAL 后 MemTable」顺序一致）。
            if let Some(ref memtable) = self.memtable {
                for entry in &durable_entries {
                    memtable.insert_batch(&entry.points, &entry.seqs);
                }
            }

            // ACK：推进到本轮覆盖的最大票号（单调递增，仅 durable 档消费）。
            let _ = self.ack_watermark.send(max_ticket);
        }
    }

    /// 对段执行一次 fsync，并在锁内推进 `committed_watermark`、清零脏计数（I3）。
    ///
    /// 调用方必须已持有段写锁。fsync 是 `sync_all`（整文件），会把此前已 append 的
    /// 所有未落盘帧（group/async/durable）一并落盘（顺风车），随后水位推进到
    /// `appended_seq`（跨三档的最大已 append seq）。
    fn fsync_and_commit_locked(&self, segment: &mut WalSegment) -> Result<(), SenseArcError> {
        segment
            .fsync()
            .map_err(|e| SenseArcError::WalIoError(format!("fsync: {}", e)))?;
        let max_seq = self.appended_seq.load(Ordering::SeqCst);
        if max_seq > 0 {
            self.committed_watermark
                .fetch_max(max_seq, Ordering::SeqCst);
        }
        self.dirty_bytes.store(0, Ordering::SeqCst);
        self.appended_seq.store(0, Ordering::SeqCst);
        {
            let mut last_flush = self.last_flush.lock().unwrap();
            *last_flush = Instant::now();
        }
        Ok(())
    }

    /// 锁外纯函数：serialize + LZ4 + TDE 加密 + 组帧头（**不含**物理 append）。
    ///
    /// 🟥 AAD 依赖不变式：`EncryptContext::wal_frame` 阶段 1 AAD 恒空（`segment_seq` /
    /// `frame_offset` 被 `let _ =` 丢弃，见 `crate::encryption`），密文只依赖
    /// `(tenant_id, dek_version, compressed)`，与段内物理位置无关。故本函数在锁外以
    /// 段参数 `0` 调 `encrypt_payload`，与锁内逐字节一致。
    ///
    /// ⚠️ 若未来启用 AAD：必须改用 `seq_start`（锁外可知、全局唯一无重叠）绑定，
    /// **严禁**段位置（`segment_seq`/`frame_offset`）绑定——后者锁内才知，会破坏锁外
    /// 预加密，且 `scan_frames` 解密时用段位置重建 ctx 必然失败。
    fn prepare_frame(
        &self,
        points: &[DataPoint],
        seq_start: u64,
    ) -> Result<PreparedFrame, SenseArcError> {
        // P2 归因埋点：serialize + lz4 + encrypt 实际 CPU（锁外预计算帧）。
        let wse_t0 = Instant::now();
        let payload = serialize_points(points);
        let compressed = crate::compression::compress_lz4(&payload);

        let frame_tenant = points
            .first()
            .map(|p| p.tenant_id)
            .unwrap_or(TenantId::DEFAULT);
        // AAD 恒空：段参数传 0，密文与锁内一致。
        let frame_payload = self.encrypt_payload(frame_tenant, &compressed, 0, 0)?;

        let schema_version = points.first().map(|p| p.schema_version).unwrap_or(0);
        let header = Self::build_v2_header(
            WalFrameFlag::Data,
            schema_version,
            0, // raft_term = 0（单机）
            seq_start,
            points.len() as u32, // point_count：帧内数据点数（供点级 seq 续号）
            &frame_payload,
        );

        // 固化 AAD 恒空依赖：若未来 `wal_frame` 填充 AAD，此断言在 debug/test 下立刻暴露，
        // 提醒开发者改用 seq_start 绑定而非段位置绑定。
        debug_assert!(
            EncryptContext::wal_frame(frame_tenant, 0, 0, 0).aad.is_empty(),
            "P2 锁外预加密依赖 AAD 恒空；启用 AAD 后必须用 seq_start 绑定，严禁段位置绑定"
        );

        write_attr::add_work(
            write_attr::WorkSlot::WalSerializeEncrypt,
            wse_t0.elapsed().as_nanos() as u64,
        );
        Ok(PreparedFrame {
            header,
            payload: frame_payload,
            seq_start,
        })
    }

    /// 锁内收口：对已预计算的帧做物理 append（rollover + 追加 + seq 水位推进）。
    ///
    /// 调用方必须已持有 `segment` 写锁。本方法只做 I/O 与 seq 收口，**不做**
    /// serialize/lz4/encrypt（这些已在 [`Self::prepare_frame`] 锁外完成）。
    fn append_prepared_locked(
        &self,
        segment: &mut WalSegment,
        frame: &PreparedFrame,
    ) -> Result<(), SenseArcError> {
        self.rollover_if_needed(segment, frame.payload.len())?;
        segment
            .append_frame_v2(&frame.header, &frame.payload)
            .map_err(|e| SenseArcError::WalIoError(format!("write: {}", e)))?;

        self.record_frame_seq(frame.seq_start);
        self.appended_seq.fetch_max(frame.seq_start, Ordering::SeqCst);
        Ok(())
    }

    /// 加密压缩后的 payload，产出 WAL 帧 payload（含 WALE 自描述前缀）。
    ///
    /// 前缀布局（S4 Step 2）：`"WALE"(4B) || tenant_id(u32 LE 4B) || dek_version(u32 LE 4B)`。
    fn encrypt_payload(
        &self,
        tenant_id: TenantId,
        compressed: &[u8],
        segment_seq: u64,
        frame_offset: u64,
    ) -> Result<Vec<u8>, SenseArcError> {
        match &self.encryptor {
            Some(e) if e.is_enabled() => {
                let dek_version = e.active_dek_version(tenant_id);
                let ctx =
                    EncryptContext::wal_frame(tenant_id, segment_seq, frame_offset, dek_version);
                let blob = e.encrypt(compressed, &ctx)?;
                let mut out = Vec::with_capacity(WAL_ENC_PREFIX_LEN + blob.len());
                out.extend_from_slice(WAL_ENC_MAGIC);
                out.extend_from_slice(&tenant_id.as_u32().to_le_bytes());
                out.extend_from_slice(&dek_version.to_le_bytes());
                out.extend_from_slice(&blob);
                Ok(out)
            }
            _ => Ok(compressed.to_vec()),
        }
    }

    /// 若当前段写不下 `payload_len` 字节，则切换到下一段。
    ///
    /// C1：切换前把旧段 `{segment_seq, first_data_seq, last_data_seq}` 记录进
    /// 持久 manifest（原子写），这是回收器判断「段内最大数据 seq ≤ 水位」的依据。
    fn rollover_if_needed(
        &self,
        segment: &mut WalSegment,
        payload_len: usize,
    ) -> Result<(), SenseArcError> {
        let frame_total = WAL_FRAME_HEADER_SIZE_V2 as u64 + payload_len as u64;
        if segment.bytes_written() + frame_total > sensearcdb_common::constants::WAL_SEGMENT_SIZE {
            self.finalize_current_segment(segment.seq())?;
            let new_seq = segment.seq() + 1;
            *segment = WalSegment::create_new(&self.wal_dir, new_seq)
                .map_err(|e| SenseArcError::WalIoError(format!("create segment: {}", e)))?;
            self.reset_segment_trackers();
        }
        Ok(())
    }

    /// 把当前段的数据 seq 范围记录进 manifest 并原子写盘（C1）。
    ///
    /// 仅当本段有 `append_prepared_locked`（单机数据帧）写入时记录；`0` 表示本段无单机
    /// 数据帧（如纯 Raft 日志路径），跳过（Raft 日志回收留阶段 7）。
    fn finalize_current_segment(&self, segment_seq: u64) -> Result<(), SenseArcError> {
        let last = self.cur_max_seq.load(Ordering::SeqCst);
        if last == 0 {
            return Ok(());
        }
        let first = self.cur_min_seq.load(Ordering::SeqCst);
        let meta = SegmentMeta {
            segment_seq,
            first_data_seq: first,
            last_data_seq: last,
        };
        let mut manifest = self.manifest.lock().unwrap_or_else(|e| e.into_inner());
        manifest.push(meta);
        manifest.write_atomic(&self.wal_dir)
    }

    /// 切换段后重置当前段的数据 seq 跟踪器。
    fn reset_segment_trackers(&self) {
        self.cur_min_seq.store(u64::MAX, Ordering::SeqCst);
        self.cur_max_seq.store(0, Ordering::SeqCst);
    }

    /// 记录一帧的数据 seq 到当前段（`cur_min_seq` 取 min，`cur_max_seq` 取 max）。
    ///
    /// 帧可能因并发 append 而乱序落段，故 min/max 都按原子语义维护（I4：数据 seq）。
    fn record_frame_seq(&self, seq_start: u64) {
        self.cur_max_seq.fetch_max(seq_start, Ordering::SeqCst);
        // atomic min：CAS 循环。
        let mut cur = self.cur_min_seq.load(Ordering::SeqCst);
        while seq_start < cur {
            match self.cur_min_seq.compare_exchange_weak(
                cur,
                seq_start,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// 构建 WAL2 帧头并计算 CRC。
    ///
    /// # 参数
    /// - `point_count`: 帧内数据点数（Control/Checkpoint/Raft 单 entry 帧传 0）。
    fn build_v2_header(
        flag: WalFrameFlag,
        schema_version: u32,
        raft_term: u64,
        raft_index: u64,
        point_count: u32,
        payload: &[u8],
    ) -> WalFrameHeaderV2 {
        let mut hdr = WalFrameHeaderV2::new(
            flag,
            schema_version,
            raft_term,
            raft_index,
            payload.len() as u32,
            point_count,
            0,
        );
        let hdr_bytes = hdr.to_bytes();
        let mut prefix = [0u8; 33];
        prefix.copy_from_slice(&hdr_bytes[..33]);
        let crc = compute_wal_crc(&prefix, payload);
        hdr.crc32c = crc;
        hdr
    }

    /// 强制刷新所有待写入数据（durable 子批 drain + 剩余脏字节无条件 fsync）。
    ///
    /// 供 `Engine::shutdown` 等关闭路径调用：把 durable_pending 与已 append 但未 fsync 的
    /// group/async 帧全部落盘（不遵循 1MB/1s 阈值门控）。
    pub async fn flush(&self) -> Result<(), SenseArcError> {
        // durable 子批：drain → append → fsync → ack。
        self.drain_durable_flush().await?;
        // 剩余已 append 未 fsync 的帧（group/async）：无条件一次 fsync 落盘。
        if self.appended_seq.load(Ordering::SeqCst) > 0 {
            let mut segment = self.segment.write();
            self.fsync_and_commit_locked(&mut segment)?;
        }
        Ok(())
    }

    /// 检查自上次 flush 以来的空闲时间。
    pub fn idle_duration(&self) -> Duration {
        let last_flush = self.last_flush.lock().unwrap();
        last_flush.elapsed()
    }

    /// 获取当前 WAL 序列号。
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// 获取当前 WAL 写入位置 `(segment_seq, bytes_written)`。
    ///
    /// 供 cluster `LogStore::truncate` 记录「截断死区」终点（append-only 文件
    /// 无法物理回退，逻辑删除区间以本位置作为闭开区间终点）。
    pub fn current_position(&self) -> (u64, u64) {
        let seg = self.segment.read();
        (seg.seq(), seg.bytes_written())
    }

    /// 获取已 fsync 的最大 WAL2 帧 seq（`raft_index`，单机自增）。
    ///
    /// 语义：所有 raft_index <= 该水位的 WAL 帧均已持久化。
    /// 供 snapshot crate 的 `WalWatermarkProvider` 注入，用于恢复边界对齐。
    pub fn current_watermark(&self) -> u64 {
        self.committed_watermark.load(Ordering::SeqCst)
    }

    /// 判定 WAL 是否可写（未发生致命 I/O 错误 / 未被显式关闭）。
    ///
    /// 构造成功即返回 `true`；后台 Group Commit 发生持久化失败后返回 `false`。
    /// 供 readiness 探针判定「WAL 可写」。
    pub fn is_writable(&self) -> bool {
        self.writable.load(Ordering::SeqCst)
    }

    /// 将 WAL 标记为不可写（致命 I/O 错误或显式关闭后调用）。
    pub fn mark_unwritable(&self) {
        self.writable.store(false, Ordering::SeqCst);
    }
}

/// 估算一组数据点的原始字节数（§5.2 复用既有口径：`4+8+8+4 + values*10 + qualities*4`）。
///
/// 用于累计 group 档脏字节数（`dirty_bytes`）驱动 1MB 阈值触发 fsync；用原始口径而非
/// 压缩后帧长，使触发点与压缩率无关、稳定可预期。
fn estimate_points_bytes(points: &[DataPoint]) -> usize {
    points
        .iter()
        .map(|p| 4 + 8 + 8 + 4 + p.values.len() * 10 + p.qualities.len() * 4)
        .sum()
}

/// 将 DataPoint 列表序列化为二进制 payload。
///
/// 供 cluster 层把 engine 数据点编码进 `RaftPayload.data`（Raft 日志 payload），
/// 与 WAL 帧 payload 同构。
pub fn serialize_points(points: &[DataPoint]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(points.len() * 64);

    // 写入点数
    buf.extend_from_slice(&(points.len() as u32).to_le_bytes());

    for p in points {
        // table_id (4B)
        buf.extend_from_slice(&p.table_id.to_le_bytes());
        // tenant_id (4B)
        buf.extend_from_slice(&p.tenant_id.as_u32().to_le_bytes());
        // ts (8B)
        buf.extend_from_slice(&p.ts.to_le_bytes());
        // server_ts (8B)
        buf.extend_from_slice(&p.server_ts.to_le_bytes());
        // schema_version (4B)
        buf.extend_from_slice(&p.schema_version.to_le_bytes());
        // flags (1B)
        buf.push(p.flags);
        // value count (2B)
        buf.extend_from_slice(&(p.values.len() as u16).to_le_bytes());
        // values: (col_id: u16, tag: u8, payload) —— 多类型直通编码
        for (col_id, val) in &p.values {
            buf.extend_from_slice(&col_id.to_le_bytes());
            append_typed_value(&mut buf, val);
        }
        // quality count (2B)
        buf.extend_from_slice(&(p.qualities.len() as u16).to_le_bytes());
        // qualities: (col_id: u16, qual: u8) pairs
        for (col_id, qual) in &p.qualities {
            buf.extend_from_slice(&col_id.to_le_bytes());
            buf.push(*qual);
        }
    }

    buf
}

/// 从二进制 payload 反序列化 DataPoint 列表。
pub fn deserialize_points(payload: &[u8]) -> Vec<DataPoint> {
    if payload.len() < 4 {
        return Vec::new();
    }

    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap_or([0u8; 4])) as usize;
    let mut pos = 4usize;
    let mut points = Vec::with_capacity(count);

    for _ in 0..count {
        if pos + 29 > payload.len() {
            break;
        }

        let table_id = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let tenant_id = TenantId(u32::from_le_bytes(
            payload[pos..pos + 4].try_into().unwrap(),
        ));
        pos += 4;
        let ts = i64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let server_ts = i64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let schema_version = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let flags = payload[pos];
        pos += 1;

        let value_count = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            if pos + 3 > payload.len() {
                break;
            }
            let col_id = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap());
            pos += 2;
            match read_typed_value(payload, &mut pos) {
                Some(val) => values.push((col_id, val)),
                None => break,
            }
        }

        let qual_count = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let mut qualities = Vec::with_capacity(qual_count);
        for _ in 0..qual_count {
            if pos + 3 > payload.len() {
                break;
            }
            let col_id = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap());
            pos += 2;
            let qual = payload[pos];
            pos += 1;
            qualities.push((col_id, qual));
        }

        points.push(DataPoint {
            table_id,
            tenant_id,
            ts,
            server_ts,
            values,
            qualities,
            schema_version,
            flags,
        });
    }

    points
}

/// 将单个 `TypedValue` 追加到 WAL payload 缓冲区。
///
/// 编码布局：`[tag u8][payload]`，tag 复用 `DataType` 的 discriminant：
/// - `F64`(0) = 8B LE
/// - `F32`(1) = 4B LE
/// - `I64`(2) = 8B LE
/// - `U64`(3) = 8B LE
/// - `I32`(4) = 4B LE
/// - `Bool`(5) = 1B（0/1）
/// - `String`(6) = `u32 LE` 长度 + UTF-8 字节
/// - `Enum16`(7) = 2B LE
fn append_typed_value(buf: &mut Vec<u8>, value: &TypedValue) {
    buf.push(value.data_type() as u8);
    match value {
        TypedValue::F64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        TypedValue::F32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        TypedValue::I64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        TypedValue::U64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        TypedValue::I32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        TypedValue::Bool(v) => buf.push(if *v { 1u8 } else { 0u8 }),
        TypedValue::String(s) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        TypedValue::Enum16(v) => buf.extend_from_slice(&v.to_le_bytes()),
    }
}

/// 从 WAL payload 缓冲区读取单个 `TypedValue`。
///
/// 成功时推进 `pos` 并返回解析出的值；数据截断或 tag 未知时返回 `None`。
fn read_typed_value(payload: &[u8], pos: &mut usize) -> Option<TypedValue> {
    let tag = *payload.get(*pos)?;
    *pos += 1;

    match tag {
        0 => {
            // F64
            let b = payload.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(TypedValue::F64(f64::from_le_bytes(b.try_into().ok()?)))
        }
        1 => {
            // F32
            let b = payload.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(TypedValue::F32(f32::from_le_bytes(b.try_into().ok()?)))
        }
        2 => {
            // I64
            let b = payload.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(TypedValue::I64(i64::from_le_bytes(b.try_into().ok()?)))
        }
        3 => {
            // U64
            let b = payload.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(TypedValue::U64(u64::from_le_bytes(b.try_into().ok()?)))
        }
        4 => {
            // I32
            let b = payload.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(TypedValue::I32(i32::from_le_bytes(b.try_into().ok()?)))
        }
        5 => {
            // Bool
            let b = *payload.get(*pos)?;
            *pos += 1;
            Some(TypedValue::Bool(b != 0))
        }
        6 => {
            // String
            let len_bytes = payload.get(*pos..*pos + 4)?;
            *pos += 4;
            let len = u32::from_le_bytes(len_bytes.try_into().ok()?) as usize;
            let s_bytes = payload.get(*pos..*pos + len)?;
            *pos += len;
            Some(TypedValue::String(
                String::from_utf8(s_bytes.to_vec()).ok()?,
            ))
        }
        7 => {
            // Enum16
            let b = payload.get(*pos..*pos + 2)?;
            *pos += 2;
            Some(TypedValue::Enum16(u16::from_le_bytes(b.try_into().ok()?)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let points = vec![
            DataPoint {
                table_id: 1,
                tenant_id: TenantId::DEFAULT,
                ts: 1000,
                server_ts: 1001,
                values: vec![(0, TypedValue::F64(42.5)), (1, TypedValue::F64(-3.14))],
                qualities: vec![(0, 0xC0), (1, 0x80)],
                schema_version: 1,
                flags: 0,
            },
            DataPoint {
                table_id: 2,
                tenant_id: TenantId::DEFAULT,
                ts: 2000,
                server_ts: 2001,
                values: vec![(0, TypedValue::F64(99.9))],
                qualities: vec![],
                schema_version: 2,
                flags: 1,
            },
        ];

        let payload = serialize_points(&points);
        let recovered = deserialize_points(&payload);

        assert_eq!(recovered.len(), 2, "should recover both points");
        assert_eq!(recovered[0].table_id, 1);
        assert_eq!(recovered[0].ts, 1000);
        assert_eq!(recovered[0].server_ts, 1001);
        assert_eq!(recovered[0].schema_version, 1);
        assert_eq!(recovered[0].flags, 0);
        assert_eq!(recovered[0].values.len(), 2);
        assert_eq!(recovered[0].values[0].1, TypedValue::F64(42.5));
        assert_eq!(recovered[0].qualities.len(), 2);

        assert_eq!(recovered[1].table_id, 2);
        assert_eq!(recovered[1].ts, 2000);
        assert_eq!(recovered[1].values.len(), 1);
        assert!(recovered[1].qualities.is_empty());
        assert_eq!(recovered[1].flags, 1);
    }

    #[test]
    fn test_serialize_deserialize_preserves_tenant_id() {
        // S1 铁律：tenant_id 必须被真实编码进 payload 并回读（非零值证明非默认回填）。
        let points = vec![DataPoint {
            table_id: 5,
            tenant_id: TenantId(7),
            ts: 1000,
            server_ts: 1001,
            values: vec![(0, TypedValue::F64(1.0))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }];

        let payload = serialize_points(&points);
        let recovered = deserialize_points(&payload);

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].tenant_id, TenantId(7));
        // 默认租户 0 也必须 roundtrip 一致（不是靠默认回填碰巧等于 0）。
        let zero_points = vec![DataPoint {
            table_id: 6,
            tenant_id: TenantId::DEFAULT,
            ts: 2000,
            server_ts: 2001,
            values: vec![(0, TypedValue::F64(2.0))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }];
        let zero_recovered = deserialize_points(&serialize_points(&zero_points));
        assert_eq!(zero_recovered[0].tenant_id, TenantId::DEFAULT);
    }

    #[test]
    fn test_serialize_empty() {
        let points: Vec<DataPoint> = vec![];
        let payload = serialize_points(&points);
        assert_eq!(payload.len(), 4); // only the length prefix
        let recovered = deserialize_points(&payload);
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_deserialize_truncated_payload() {
        let too_short = vec![0u8; 3];
        let result = deserialize_points(&too_short);
        assert!(result.is_empty(), "truncated payload should yield empty");

        // Payload says 1 point but is truncated before full frame
        let mut truncated = vec![0u8; 4];
        truncated[0] = 1; // count = 1
        let result = deserialize_points(&truncated);
        assert!(result.is_empty(), "truncated point should be skipped");
    }

    #[test]
    fn test_serialize_single_point_multi_column() {
        let points = vec![DataPoint {
            table_id: 42,
            tenant_id: TenantId::DEFAULT,
            ts: 999999,
            server_ts: 1000000,
            values: vec![
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::F64(2.0)),
                (2, TypedValue::F64(3.0)),
                (3, TypedValue::F64(4.0)),
            ],
            qualities: vec![(0, 0xC0), (2, 0x00)],
            schema_version: 3,
            flags: 0xAB,
        }];

        let payload = serialize_points(&points);
        let recovered = deserialize_points(&payload);

        assert_eq!(recovered.len(), 1);
        let dp = &recovered[0];
        assert_eq!(dp.table_id, 42);
        assert_eq!(dp.ts, 999999);
        assert_eq!(dp.server_ts, 1000000);
        assert_eq!(dp.schema_version, 3);
        assert_eq!(dp.flags, 0xAB);
        assert_eq!(dp.values.len(), 4);
        assert_eq!(dp.qualities.len(), 2);
        // Verify specific column ids are preserved
        assert_eq!(dp.values[2].0, 2);
        assert_eq!(dp.values[2].1, TypedValue::F64(3.0));
    }

    #[test]
    fn test_serialize_deserialize_multitype_round_trip() {
        let points = vec![DataPoint {
            table_id: 7,
            tenant_id: TenantId::DEFAULT,
            ts: 123456789,
            server_ts: 123456790,
            values: vec![
                (0, TypedValue::F32(1.5)),
                (1, TypedValue::I64(-42)),
                (2, TypedValue::U64(18_446_744_073_709_551_615)),
                (3, TypedValue::I32(-7)),
                (4, TypedValue::Bool(true)),
                (5, TypedValue::String("open".to_string())),
                (6, TypedValue::Enum16(65535)),
            ],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }];

        let payload = serialize_points(&points);
        let recovered = deserialize_points(&payload);

        assert_eq!(recovered.len(), 1);
        let dp = &recovered[0];
        assert_eq!(dp.values.len(), 7);
        assert_eq!(dp.values[0], (0, TypedValue::F32(1.5)));
        assert_eq!(dp.values[1], (1, TypedValue::I64(-42)));
        assert_eq!(
            dp.values[2],
            (2, TypedValue::U64(18_446_744_073_709_551_615))
        );
        assert_eq!(dp.values[3], (3, TypedValue::I32(-7)));
        assert_eq!(dp.values[4], (4, TypedValue::Bool(true)));
        assert_eq!(dp.values[5], (5, TypedValue::String("open".to_string())));
        assert_eq!(dp.values[6], (6, TypedValue::Enum16(65535)));
    }

    /// 端到端多类型直通：DataPoint → WAL 序列化 → 反序列化 → MemTable 插入 → 类型保真读回。
    #[test]
    fn test_multitype_passthrough_to_memtable() {
        let point = DataPoint {
            table_id: 11,
            tenant_id: TenantId::DEFAULT,
            ts: 1000,
            server_ts: 1001,
            values: vec![
                (0, TypedValue::I64(-123_456_789)),
                (1, TypedValue::U64(18_446_744_073_709_551_615)),
                (2, TypedValue::String("phase-a".to_string())),
                (3, TypedValue::Bool(false)),
            ],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        };

        // WAL 序列化 → 反序列化（模拟重启后的重放路径）
        let payload = serialize_points(&[point.clone()]);
        let recovered = deserialize_points(&payload);
        assert_eq!(recovered.len(), 1);

        // 送入 MemTable 多类型插入路径
        let config = sensearcdb_common::ConfigHandle::new(Default::default());
        let memtable = MemTableManager::new(&config);
        memtable.insert(
            recovered[0].table_id,
            recovered[0].ts,
            &recovered[0].values,
            &recovered[0].qualities,
            recovered[0].flags,
        );

        // 从 MemTable 读回，验证类型保真（不再折叠为 f64）
        let chunk = memtable.active_snapshot().get_chunk(11).unwrap();
        assert_eq!(
            chunk.get_typed_value(0, 0),
            Some(TypedValue::I64(-123_456_789))
        );
        assert_eq!(
            chunk.get_typed_value(1, 0),
            Some(TypedValue::U64(18_446_744_073_709_551_615))
        );
        assert_eq!(
            chunk.get_typed_value(2, 0),
            Some(TypedValue::String("phase-a".to_string()))
        );
        assert_eq!(chunk.get_typed_value(3, 0), Some(TypedValue::Bool(false)));
    }

    /// alloc_seqs 并发回归：`start` 与 `seqs` 严格连续一致，且多线程并发下全局无重叠。
    #[test]
    fn test_alloc_seqs_concurrent_no_overlap() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = sensearcdb_common::ConfigHandle::new(cfg);
        let wal = std::sync::Arc::new(WalWriter::new(&handle).expect("create WalWriter"));

        const THREADS: usize = 8;
        const ITERS_PER_THREAD: usize = 200;

        let results: std::sync::Arc<std::sync::Mutex<Vec<(u64, Vec<u64>)>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let wal = std::sync::Arc::clone(&wal);
            let results = std::sync::Arc::clone(&results);
            handles.push(std::thread::spawn(move || {
                for i in 0..ITERS_PER_THREAD {
                    let n = (i % 5) + 1; // 每个线程交替分配 1..=5 个点
                    let (start, seqs) = wal.alloc_seqs(n);
                    // 断言 ①：seqs[i] == start + i（start 与 seqs 严格一致）。
                    for (j, s) in seqs.iter().enumerate() {
                        assert_eq!(*s, start + j as u64, "seqs 必须与 start 连续一致");
                    }
                    results.lock().unwrap().push((start, seqs));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // 断言 ②：所有分配的 seq 全局唯一、无重叠（去重后数量等于总量）。
        let guard = results.lock().unwrap();
        let mut collected: Vec<u64> = guard
            .iter()
            .flat_map(|(_, seqs)| seqs.iter().copied())
            .collect();
        let total = collected.len();
        collected.sort_unstable();
        collected.dedup();
        assert_eq!(
            collected.len(),
            total,
            "并发分配的所有 seq 必须全局唯一无重叠（去重后应等于总量）"
        );
    }

    /// 向 `{segment_seq:08}.wal` 追加一帧（37B 帧头 + 4B 占位 payload），供 seq 续号测试用。
    /// `point_count=1`：表示单点帧（点级上界 = raft_index）。
    fn write_frame(wal_dir: &Path, segment_seq: u64, raft_index: u64) {
        use std::io::Write;
        let path = wal_dir.join(format!("{segment_seq:08}.wal"));
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let header = WalFrameHeaderV2::new(WalFrameFlag::Data, 1, 0, raft_index, 4, 1, 0);
        file.write_all(&header.to_bytes()).unwrap();
        file.write_all(&[0u8; 4]).unwrap();
        file.flush().unwrap();
    }

    /// scan_max_raft_index 空目录 → 0。
    #[test]
    fn test_scan_max_raft_index_empty_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        assert_eq!(scan_max_raft_index(&wal_dir), 0);
    }

    /// scan_max_raft_index 多段取最大 + 损坏帧头停止该段（不读到后续合法帧）。
    #[test]
    fn test_scan_max_raft_index_multi_segment_and_corrupt() {
        use std::io::Write;
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        // 段 0：raft_index 1、5、3 → 本段 max=5。
        write_frame(&wal_dir, 0, 1);
        write_frame(&wal_dir, 0, 5);
        write_frame(&wal_dir, 0, 3);
        // 段 1：raft_index 100、50 → 本段 max=100。
        write_frame(&wal_dir, 1, 100);
        write_frame(&wal_dir, 1, 50);

        assert_eq!(scan_max_raft_index(&wal_dir), 100);
        // min 也必须正确（cur_min_seq 初始化用）：段 0 最老 = 1。
        // point_count=1 → 点级上界 == 帧级上界 == 100。
        assert_eq!(scan_raft_index_range(&wal_dir), (1, 100, 100));
        assert_eq!(scan_max_point_seq(&wal_dir), 100);

        // 段 2：首帧 magic 损坏 → 扫描在该段停止，后续合法帧（9999）不被计入。
        let bad_path = wal_dir.join("00000002.wal");
        std::fs::write(&bad_path, b"NOT-A-WAL2-FRAME!").unwrap();
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&bad_path)
            .unwrap();
        let hdr = WalFrameHeaderV2::new(WalFrameFlag::Data, 1, 0, 9999, 4, 1, 0);
        f.write_all(&hdr.to_bytes()).unwrap();
        f.write_all(&[0u8; 4]).unwrap();

        assert_eq!(
            scan_max_raft_index(&wal_dir),
            100,
            "损坏段后的合法帧不得计入"
        );
    }

    /// 跨重启 seq 续号：既有段 max raft_index=100，new 后第一次 alloc_seqs 从 101 开始。
    #[test]
    fn test_seq_continuation_after_restart() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        for i in 1..=100u64 {
            write_frame(&wal_dir, 0, i);
        }

        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = sensearcdb_common::ConfigHandle::new(cfg);
        let wal = WalWriter::new(&handle).expect("create WalWriter (restart)");

        let (start, seqs) = wal.alloc_seqs(3);
        assert_eq!(start, 101, "重启后 seq 必须从扫描 max+1 续号，而非重置回 1");
        assert_eq!(seqs, vec![101, 102, 103]);
    }

    /// 跨重启回归：manifest 保留（不重置）+ reaper 不误删含 Run1 未 flush 数据的复用段。
    #[test]
    fn test_restart_manifest_preserved_and_reaper_protects() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        // Run1：segment 0 写 raft_index 1..100，其中 51..100 未 flush 进 SAC（S=50）；
        // manifest 记 {0: 1..100}。
        for i in 1..=100u64 {
            write_frame(&wal_dir, 0, i);
        }
        let mut manifest = WalManifest::default();
        manifest.push(SegmentMeta {
            segment_seq: 0,
            first_data_seq: 1,
            last_data_seq: 100,
        });
        manifest.write_atomic(&wal_dir).unwrap();

        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = sensearcdb_common::ConfigHandle::new(cfg);
        let wal = WalWriter::new(&handle).expect("create WalWriter (restart)");

        // ① manifest 保留（不再重置为空）。
        let loaded = WalManifest::load(&wal_dir).unwrap();
        assert_eq!(loaded.entries.len(), 1, "manifest 不得被重置为空");
        assert_eq!(loaded.entries[0].segment_seq, 0);
        assert_eq!(loaded.entries[0].last_data_seq, 100);

        // ② R = min(wal_watermark=50, S=50) = 50；segment 0 含 seq 51..100 未 flush 数据。
        //    段 0 当前为 active（seq 0 == active 0）→ 不删。
        let reaper = crate::wal::reaper::WalReaper::new(&wal_dir, 1);
        let stats = reaper.reap(50, wal.active_segment_seq()).unwrap();
        assert_eq!(stats.deleted, 0, "active 复用段绝不删");
        assert!(wal_dir.join("00000000.wal").exists());

        // ③ 即使段 0 已 finalize（active 推进到 1），last_data_seq=100 > R=50 仍不删。
        let stats2 = reaper.reap(50, 1).unwrap();
        assert_eq!(stats2.deleted, 0, "含 seq>R 未 flush 数据的段绝不删");
        assert!(wal_dir.join("00000000.wal").exists());
    }

    /// scan_max_point_seq：点级上界 = `raft_index + point_count - 1`，且 `point_count=0`
    /// （控制帧 / Raft 单 entry 帧）按 1 点处理（点级上界 = `raft_index`）。
    #[test]
    fn test_scan_max_point_seq_point_count_and_zero() {
        use std::io::Write;
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let path = wal_dir.join("00000000.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        // 帧 1：raft_index=10, point_count=0 → 点级上界 10（按 1 点处理）。
        // 帧 2：raft_index=20, point_count=5 → 点级上界 24。
        // 帧 3：raft_index=23, point_count=1 → 点级上界 23。
        for (raft_index, point_count) in [(10u64, 0u32), (20, 5), (23, 1)] {
            let header =
                WalFrameHeaderV2::new(WalFrameFlag::Data, 1, 0, raft_index, 4, point_count, 0);
            file.write_all(&header.to_bytes()).unwrap();
            file.write_all(&[0u8; 4]).unwrap();
        }
        file.flush().unwrap();

        assert_eq!(scan_max_raft_index(&wal_dir), 23, "帧级最大 = 23");
        assert_eq!(
            scan_max_point_seq(&wal_dir),
            24,
            "点级上界最大 = 24（帧 2：20+5-1），大于帧级 23，绝不低估"
        );
        assert_eq!(
            scan_raft_index_range(&wal_dir),
            (10, 23, 24),
            "三值：(min_frame, max_frame, max_point)"
        );
    }

    /// 🟥 核心回归：真实写路径多点帧（单 batch N 点 → 一帧）跨重启 seq 续号，
    /// 必须用点级上界（`max_point_seq+1`），而非帧首 `raft_index`（会低估碰撞）。
    #[tokio::test]
    async fn test_multi_point_frame_seq_continuation_uses_point_upper_bound() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let mut cfg = sensearcdb_common::AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = sensearcdb_common::ConfigHandle::new(cfg);

        // Run1：单 batch 100 点 → 单帧，raft_index=1，点级 seq 1..100。
        {
            let wal = WalWriter::new(&handle).expect("create WalWriter (Run1)");
            let points: Vec<DataPoint> = (0..100)
                .map(|i| DataPoint {
                    table_id: 1,
                    tenant_id: TenantId::DEFAULT,
                    ts: i,
                    server_ts: i + 1,
                    values: vec![(0, TypedValue::F64(i as f64))],
                    qualities: vec![(0, 0xC0)],
                    schema_version: 1,
                    flags: 0,
                })
                .collect();
            let wr = wal
                .write_batch(PointBatch { points })
                .await
                .expect("write_batch Run1");
            assert_eq!(wr.wal_seq_start, 1, "Run1 首帧 seq_start=1");
            assert_eq!(wal.current_seq(), 101, "Run1 单帧 100 点后 seq 计数器=101");
        }

        // 帧级 scan 只能看到帧首 raft_index=1；点级上界必须为 100。
        assert_eq!(
            scan_max_raft_index(&wal_dir),
            1,
            "帧级 scan 只读帧首 raft_index"
        );
        assert_eq!(
            scan_max_point_seq(&wal_dir),
            100,
            "点级上界 = 1 + 100 - 1 = 100"
        );

        // Run2：重建 WalWriter，seq 必须从 101 续号（点级上界 100 + 1），不与旧帧 2..100 碰撞。
        let wal = WalWriter::new(&handle).expect("create WalWriter (Run2)");
        assert_eq!(wal.current_seq(), 101, "重启后 seq 必须从点级上界 101 续号");
        let (start, seqs) = wal.alloc_seqs(1);
        assert_eq!(start, 101);
        assert_eq!(seqs, vec![101]);
    }
}

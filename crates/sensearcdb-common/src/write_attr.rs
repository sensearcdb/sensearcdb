//! P2 归因埋点：写路径细粒度计时（诊断用，可随时撤）。
//!
//! ## 设计（规避 writepath_timing 被 8 worker 争抢 + revert 的教训）
//!
//! 1. **同步段 thread_local 累加**：写路径同步段（无 `.await`、不跨线程迁移）直接往
//!    `thread_local` 记 ns，零原子开销。
//! 2. **每 `write_batch` 结束 flush**：把 thread_local delta `fetch_add` 进全局 padded 原子
//!    （每个计数器独立 cache line），每批仅一次原子写。
//! 3. **锁等待只测争抢分支**：`try_write()` / `try_read()` 成功 = 无竞争零计时；
//!    失败才 `Instant::now()` + 阻塞 `write()` / `read()` 计时计入 lock_wait 槽。
//! 4. **跨 await / 跨线程的槽用直接原子**（grpc gap、proto/convert）——频率低（每批一次），
//!    不参与 write_batch 闭合。
//!
//! ## 槽位
//!
//! ### 锁等待 / 后台（write_batch 内）
//! - `lock_wait_wal_segment` / `lock_wait_memtable_state` / `lock_wait_snapshot_bucket`
//! - `freeze_total` / `freeze_count`
//!
//! ### 实际工作段（write_batch 内，闭合到 write_batch_total）
//! - `durability_group`：按 durability 分组 + `point.clone()`（逐行分配）
//! - `wal_serialize_encrypt`：`prepare_frame`（serialize_points + lz4 + TDE 加密 + 组头）
//! - `wal_append_io`：锁内物理 append（含 lock_wait_wal，锁内只做 I/O + seq 收口）
//! - `memtable_insert`：`insert_batch` 实际 append CPU（含 lock_wait_memtable）
//! - `snapshot_push`：`sink.on_write` fanout（含 lock_wait_snapshot）
//! - `other`：write_batch_total − Σ上述（quota/alloc_seqs/seqs 组装/metrics 等）
//!
//! ### 入口开销（write_batch 外，ingest 层，直接原子）
//! - `proto_receive`：`stream.message().await`（网络接收 + prost decode）
//! - `convert_batch`：proto PointBatch → ingest PointBatch（列式）
//! - `convert_engine`：ingest PointBatch → engine DataPoint（行式逐行分配）

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// 64 字节对齐的原子计数器（独立 cache line，避免伪共享）。
#[repr(align(64))]
struct PaddedCounter(AtomicU64);

impl PaddedCounter {
    const fn new() -> Self {
        PaddedCounter(AtomicU64::new(0))
    }
    #[inline]
    fn add(&self, delta: u64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }
    #[inline]
    fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

// 全局 padded 原子（每个计数器独立 cache line）。
static LOCK_WAIT_WAL_SEGMENT: PaddedCounter = PaddedCounter::new();
static LOCK_WAIT_MEMTABLE_STATE: PaddedCounter = PaddedCounter::new();
static LOCK_WAIT_SNAPSHOT_BUCKET: PaddedCounter = PaddedCounter::new();
static FREEZE_TOTAL: PaddedCounter = PaddedCounter::new();
static FREEZE_COUNT: PaddedCounter = PaddedCounter::new();
static GRPC_STREAM_AWAIT: PaddedCounter = PaddedCounter::new();
static GRPC_BATCH_COUNT: PaddedCounter = PaddedCounter::new();
static WRITE_BATCH_TOTAL: PaddedCounter = PaddedCounter::new();
static WRITE_BATCH_COUNT: PaddedCounter = PaddedCounter::new();
// 实际工作段（write_batch 内）。
static DURABILITY_GROUP: PaddedCounter = PaddedCounter::new();
static WAL_SERIALIZE_ENCRYPT: PaddedCounter = PaddedCounter::new();
static WAL_APPEND_IO: PaddedCounter = PaddedCounter::new();
static MEMTABLE_INSERT: PaddedCounter = PaddedCounter::new();
static SNAPSHOT_PUSH: PaddedCounter = PaddedCounter::new();
// 入口开销（ingest 层，write_batch 外）。
static PROTO_RECEIVE: PaddedCounter = PaddedCounter::new();
static CONVERT_BATCH: PaddedCounter = PaddedCounter::new();
static CONVERT_ENGINE: PaddedCounter = PaddedCounter::new();

/// 锁槽位（供 `timed_write_lock` / `timed_read_lock` 选择累加目标）。
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LockSlot {
    WalSegment,
    MemtableState,
    SnapshotBucket,
}

/// 实际工作槽位（write_batch 内，闭合到 write_batch_total）。
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WorkSlot {
    DurabilityGroup,
    WalSerializeEncrypt,
    WalAppendIo,
    MemtableInsert,
    SnapshotPush,
}

/// thread_local 累加器（同步段使用，零原子开销）。
#[derive(Default)]
struct Accum {
    lock_wait_wal_segment: u64,
    lock_wait_memtable_state: u64,
    lock_wait_snapshot_bucket: u64,
    freeze_total: u64,
    freeze_count: u64,
    write_batch_total: u64,
    write_batch_count: u64,
    durability_group: u64,
    wal_serialize_encrypt: u64,
    wal_append_io: u64,
    memtable_insert: u64,
    snapshot_push: u64,
}

impl Accum {
    #[inline]
    fn is_zero(&self) -> bool {
        self.lock_wait_wal_segment == 0
            && self.lock_wait_memtable_state == 0
            && self.lock_wait_snapshot_bucket == 0
            && self.freeze_total == 0
            && self.freeze_count == 0
            && self.write_batch_total == 0
            && self.write_batch_count == 0
            && self.durability_group == 0
            && self.wal_serialize_encrypt == 0
            && self.wal_append_io == 0
            && self.memtable_insert == 0
            && self.snapshot_push == 0
    }
}

thread_local! {
    static ACCUM: RefCell<Accum> = RefCell::new(Accum::default());
}

#[inline]
fn with_accum<F: FnOnce(&mut Accum)>(f: F) {
    ACCUM.with(|cell| f(&mut cell.borrow_mut()));
}

/// 争抢分支计时：`try_write()` 成功零计时；失败才计时阻塞 `write()`。
#[inline]
pub fn timed_write_lock<T>(
    lock: &parking_lot::RwLock<T>,
    slot: LockSlot,
) -> parking_lot::RwLockWriteGuard<'_, T> {
    match lock.try_write() {
        Some(g) => g,
        None => {
            let t = Instant::now();
            let g = lock.write();
            add_lock_wait(slot, t.elapsed().as_nanos() as u64);
            g
        }
    }
}

/// 争抢分支计时：`try_read()` 成功零计时；失败才计时阻塞 `read()`。
#[inline]
pub fn timed_read_lock<T>(
    lock: &parking_lot::RwLock<T>,
    slot: LockSlot,
) -> parking_lot::RwLockReadGuard<'_, T> {
    match lock.try_read() {
        Some(g) => g,
        None => {
            let t = Instant::now();
            let g = lock.read();
            add_lock_wait(slot, t.elapsed().as_nanos() as u64);
            g
        }
    }
}

/// 累加锁等待（thread_local，写路径同步段使用）。
#[inline]
pub fn add_lock_wait(slot: LockSlot, ns: u64) {
    with_accum(|a| match slot {
        LockSlot::WalSegment => a.lock_wait_wal_segment += ns,
        LockSlot::MemtableState => a.lock_wait_memtable_state += ns,
        LockSlot::SnapshotBucket => a.lock_wait_snapshot_bucket += ns,
    });
}

/// 累加实际工作段（thread_local，write_batch 内同步段使用）。
#[inline]
pub fn add_work(slot: WorkSlot, ns: u64) {
    with_accum(|a| match slot {
        WorkSlot::DurabilityGroup => a.durability_group += ns,
        WorkSlot::WalSerializeEncrypt => a.wal_serialize_encrypt += ns,
        WorkSlot::WalAppendIo => a.wal_append_io += ns,
        WorkSlot::MemtableInsert => a.memtable_insert += ns,
        WorkSlot::SnapshotPush => a.snapshot_push += ns,
    });
}

/// 累加 force_freeze 墙钟（thread_local）。
#[inline]
pub fn add_freeze(ns: u64) {
    with_accum(|a| a.freeze_total += ns);
}

/// 累加 force_freeze 触发次数（thread_local）。
#[inline]
pub fn add_freeze_count() {
    with_accum(|a| a.freeze_count += 1);
}

/// 累加 write_batch 墙钟（thread_local）。
#[inline]
pub fn add_write_batch_total(ns: u64) {
    with_accum(|a| a.write_batch_total += ns);
}

/// 累加 write_batch 次数（thread_local）。
#[inline]
pub fn add_write_batch_count() {
    with_accum(|a| a.write_batch_count += 1);
}

/// 累加 grpc await 空闲 gap（直接原子：跨 await 点线程迁移，不用 thread_local）。
#[inline]
pub fn add_grpc_stream_await(ns: u64) {
    GRPC_STREAM_AWAIT.add(ns);
}

/// 累加 grpc 处理批次数（直接原子）。
#[inline]
pub fn add_grpc_batch_count() {
    GRPC_BATCH_COUNT.add(1);
}

/// 累加 `stream.message().await` 接收+decode 耗时（直接原子，ingest 层）。
#[inline]
pub fn add_proto_receive(ns: u64) {
    PROTO_RECEIVE.add(ns);
}

/// 累加 proto PointBatch → ingest PointBatch 转换耗时（直接原子，ingest 层）。
#[inline]
pub fn add_convert_batch(ns: u64) {
    CONVERT_BATCH.add(ns);
}

/// 累加 ingest PointBatch → engine DataPoint 转换耗时（直接原子，ingest 层）。
#[inline]
pub fn add_convert_engine(ns: u64) {
    CONVERT_ENGINE.add(ns);
}

/// 把当前线程的 thread_local delta flush 进全局 padded 原子。
///
/// 由 `Engine::write_batch` 每批结束时调用（同步段在同一线程上累加，flush 同线程）。
#[inline]
pub fn flush() {
    with_accum(|a| {
        if a.is_zero() {
            return;
        }
        LOCK_WAIT_WAL_SEGMENT.add(a.lock_wait_wal_segment);
        LOCK_WAIT_MEMTABLE_STATE.add(a.lock_wait_memtable_state);
        LOCK_WAIT_SNAPSHOT_BUCKET.add(a.lock_wait_snapshot_bucket);
        FREEZE_TOTAL.add(a.freeze_total);
        FREEZE_COUNT.add(a.freeze_count);
        WRITE_BATCH_TOTAL.add(a.write_batch_total);
        WRITE_BATCH_COUNT.add(a.write_batch_count);
        DURABILITY_GROUP.add(a.durability_group);
        WAL_SERIALIZE_ENCRYPT.add(a.wal_serialize_encrypt);
        WAL_APPEND_IO.add(a.wal_append_io);
        MEMTABLE_INSERT.add(a.memtable_insert);
        SNAPSHOT_PUSH.add(a.snapshot_push);
        *a = Accum::default();
    });
}

/// 清零全部全局计数器（复跑前手动 reset 用）。
pub fn reset() {
    flush();
    let counters: [&PaddedCounter; 17] = [
        &LOCK_WAIT_WAL_SEGMENT,
        &LOCK_WAIT_MEMTABLE_STATE,
        &LOCK_WAIT_SNAPSHOT_BUCKET,
        &FREEZE_TOTAL,
        &FREEZE_COUNT,
        &GRPC_STREAM_AWAIT,
        &GRPC_BATCH_COUNT,
        &WRITE_BATCH_TOTAL,
        &WRITE_BATCH_COUNT,
        &DURABILITY_GROUP,
        &WAL_SERIALIZE_ENCRYPT,
        &WAL_APPEND_IO,
        &MEMTABLE_INSERT,
        &SNAPSHOT_PUSH,
        &PROTO_RECEIVE,
        &CONVERT_BATCH,
        &CONVERT_ENGINE,
    ];
    for c in counters {
        c.0.store(0, Ordering::Relaxed);
    }
}

fn pct2(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        ((part as f64 * 100.0) / whole as f64 * 100.0).round() / 100.0
    }
}

/// 低频 report（每 10s，`target: "write_attr"`）。
///
/// 输出累计值 + 各段占 `write_batch_total` 的百分比。
/// `other = write_batch_total − Σ(工作段 + 锁等待 + freeze)`，是闭合校验的余量。
pub fn report() {
    flush();

    let wb_total = WRITE_BATCH_TOTAL.load();
    let wb_count = WRITE_BATCH_COUNT.load();
    let lw_wal = LOCK_WAIT_WAL_SEGMENT.load();
    let lw_mem = LOCK_WAIT_MEMTABLE_STATE.load();
    let lw_snap = LOCK_WAIT_SNAPSHOT_BUCKET.load();
    let freeze_total = FREEZE_TOTAL.load();
    let freeze_count = FREEZE_COUNT.load();
    let grpc_await = GRPC_STREAM_AWAIT.load();
    let grpc_count = GRPC_BATCH_COUNT.load();
    let dg = DURABILITY_GROUP.load();
    let wse = WAL_SERIALIZE_ENCRYPT.load();
    let wai = WAL_APPEND_IO.load();
    let mti = MEMTABLE_INSERT.load();
    let snp = SNAPSHOT_PUSH.load();
    let proto_recv = PROTO_RECEIVE.load();
    let conv_batch = CONVERT_BATCH.load();
    let conv_engine = CONVERT_ENGINE.load();

    // 注意：snapshot_push 含 lock_wait_snapshot；wal_append_io 含 lock_wait_wal；
    // memtable_insert 含 lock_wait_memtable。闭合时这些锁不重复加。
    let accounted = dg + wse + wai + mti + snp + freeze_total;
    let other = wb_total.saturating_sub(accounted);

    tracing::info!(
        target: "write_attr",
        write_batch_total_ns = wb_total,
        write_batch_count = wb_count,
        durability_group_ns = dg,
        wal_serialize_encrypt_ns = wse,
        wal_append_io_ns = wai,
        memtable_insert_ns = mti,
        snapshot_push_ns = snp,
        freeze_total_ns = freeze_total,
        freeze_count = freeze_count,
        lock_wait_wal_segment_ns = lw_wal,
        lock_wait_memtable_state_ns = lw_mem,
        lock_wait_snapshot_bucket_ns = lw_snap,
        other_ns = other,
        grpc_stream_await_ns = grpc_await,
        grpc_batch_count = grpc_count,
        proto_receive_ns = proto_recv,
        convert_batch_ns = conv_batch,
        convert_engine_ns = conv_engine,
        durability_group_pct = pct2(dg, wb_total),
        wal_serialize_encrypt_pct = pct2(wse, wb_total),
        wal_append_io_pct = pct2(wai, wb_total),
        memtable_insert_pct = pct2(mti, wb_total),
        snapshot_push_pct = pct2(snp, wb_total),
        freeze_pct = pct2(freeze_total, wb_total),
        lock_wal_pct = pct2(lw_wal, wb_total),
        lock_mem_pct = pct2(lw_mem, wb_total),
        lock_snap_pct = pct2(lw_snap, wb_total),
        other_pct = pct2(other, wb_total),
        "write_attr cumulative report"
    );
}

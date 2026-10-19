//! flush 并行编码专用 rayon 池。
//!
//! 设计依据（`docs/flush-parallelization-design.md` §5.0）：
//! - 「编码 + 压缩 + TDE-GCM」为纯 CPU，直接用 rayon（CPU 池）并行；
//! - flush 使用**独立 rayon 池**，避免与 compaction 的 `spawn_blocking` 线程池争抢 CPU；
//! - rayon 任务内**禁止**再 `spawn_blocking`/`await`（防嵌套死锁），故本模块只暴露
//!   同步的 `install` 入口，seq 扫盘 / 写盘 / fsync / rename 一律放池外串行。
//!
//! 本池是进程内唯一实例（`OnceLock`），线程数取 `available_parallelism`；后续可依据
//! §7 实测把配额调成可配置（如 `flush_cpu_quota` 配置项），此处先以「不饿死 compaction」
//! 与「不超订 CPU」的默认值落地。

use std::sync::OnceLock;

use rayon::{ThreadPool, ThreadPoolBuilder};

/// flush 编码池实例（进程内唯一，懒初始化）。
static FLUSH_POOL: OnceLock<ThreadPool> = OnceLock::new();

/// 计算 flush 独立 rayon 池的线程配额。
///
/// 当前按「系统可用并行度」取值（下限 1）。§7.2 性能实测后可将配额收敛为
/// `flush_cpu_quota`（例如 `max(2, min(parallelism, 8))`），此处保留单点以便校准。
fn flush_cpu_quota() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1)
}

/// 获取 flush 编码专用 rayon 池（懒初始化，进程内唯一）。
///
/// 调用方在池上 `install` 执行纯 CPU 的编码/压缩并行段；同步 IO 不得进入该闭包。
pub(crate) fn flush_pool() -> &'static ThreadPool {
    FLUSH_POOL.get_or_init(|| {
        let quota = flush_cpu_quota();
        ThreadPoolBuilder::new()
            .num_threads(quota)
            .thread_name(|i| format!("sac-flush-{i}"))
            .build()
            .unwrap_or_else(|e| panic!("failed to build flush rayon pool: {e}"))
    })
}

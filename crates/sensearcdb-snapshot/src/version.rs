//! 全局单调递增版本号管理。
//!
//! 使用 `AtomicU64` 实现无锁版本号分配，每次更新递增，
//! 用于 SSE 订阅的变更追踪和快照版本标识。

use std::sync::atomic::{AtomicU64, Ordering};

/// 全局版本号管理器。
///
/// 封装 `AtomicU64`，提供单调递增的版本号分配。
/// 每次调用 [`GlobalVersion::next`] 返回上一个值并递增。
///
/// ## 用途
///
/// 1. **快照版本标识**：每次 `SnapshotStore::update()` 分配新版本号，
///    版本号作为 SnapshotEntry.version 存储。
/// 2. **SSE 变更追踪**：客户端通过比较版本号判断是否需要全量同步。
/// 3. **崩溃恢复**：snapshot.bin 记录持久化时的版本号，
///    用于恢复时确定 WAL 重放的起点。
#[derive(Debug)]
pub struct GlobalVersion {
    inner: AtomicU64,
}

impl GlobalVersion {
    /// 创建版本号管理器，初始值为 0。
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: AtomicU64::new(0),
        }
    }

    /// 创建带指定初始值的版本号管理器（用于恢复场景）。
    #[inline]
    pub fn with_value(initial: u64) -> Self {
        Self {
            inner: AtomicU64::new(initial),
        }
    }

    /// 递增并返回新版本号（fetch_add 语义：返回旧值，存储递增后的值）。
    ///
    /// 等价于 `fetch_add(1, Relaxed) + 1` 的语义——
    /// 返回的是**本次分配**的版本号。
    #[inline]
    pub fn next(&self) -> u64 {
        self.inner.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// 返回当前版本号（不递增）。
    #[inline]
    pub fn current(&self) -> u64 {
        self.inner.load(Ordering::Relaxed)
    }

    /// 设置版本号（仅用于恢复场景，设置一个较高的初始值）。
    #[inline]
    pub fn set(&self, value: u64) {
        self.inner.store(value, Ordering::Relaxed);
    }

    /// 条件更新：仅当 `value > current` 时设置（防止版本号倒退）。
    #[inline]
    pub fn set_max(&self, value: u64) {
        self.inner.fetch_max(value, Ordering::Relaxed);
    }
}

impl Default for GlobalVersion {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_monotonic() {
        let ver = GlobalVersion::new();
        let v1 = ver.next();
        let v2 = ver.next();
        let v3 = ver.next();
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert_eq!(v3, 3);
        assert_eq!(ver.current(), 3);
    }

    #[test]
    fn test_version_with_initial() {
        let ver = GlobalVersion::with_value(100);
        assert_eq!(ver.current(), 100);
        assert_eq!(ver.next(), 101);
    }

    #[test]
    fn test_set_max() {
        let ver = GlobalVersion::with_value(10);
        ver.set_max(5); // 不应降低
        assert_eq!(ver.current(), 10);
        ver.set_max(20);
        assert_eq!(ver.current(), 20);
    }
}

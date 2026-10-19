//! 错位重算队列。
//!
//! 当迟到数据写入后，对应窗口被标记为 dirty
//! 并加入重算队列。调度器在下一个 tick 优先处理
//! 重算队列中的窗口，确保 CQ 结果始终反映最新数据。
//!
//! ## Final 判定
//!
//! 窗口超出 staleness 窗口后自动标记为 Final，
//! Final 窗口不再参与重算。

use std::collections::VecDeque;

use crate::window::{CqWindowKey, CqWindowState, CqWindowStatus};

// ─── RecalculationQueue ───

/// 错位重算队列。
///
/// 使用 `VecDeque` 作为 FIFO 队列，按到达顺序处理重算请求。
/// 同一个窗口在队列中最多出现一次（去重）。
#[derive(Debug)]
pub struct RecalculationQueue {
    /// 待重算的窗口键队列。
    queue: VecDeque<CqWindowKey>,
    /// 快速去重集合（窗口键已在队列中则跳过 push）。
    in_queue: std::collections::HashSet<CqWindowKey>,
}

impl RecalculationQueue {
    /// 创建空的重算队列。
    #[inline]
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            in_queue: std::collections::HashSet::new(),
        }
    }

    /// 将窗口加入重算队列。
    ///
    /// 如果窗口已在队列中，则跳过（避免重复）。
    ///
    /// # Arguments
    /// * `key` - 需要重算的窗口键。
    pub fn push(&mut self, key: CqWindowKey) {
        if self.in_queue.insert(key.clone()) {
            self.queue.push_back(key);
        }
    }

    /// 批量将窗口加入重算队列。
    ///
    /// # Arguments
    /// * `keys` - 需要重算的窗口键列表。
    pub fn push_batch(&mut self, keys: Vec<CqWindowKey>) {
        for key in keys {
            self.push(key);
        }
    }

    /// 从队列中取出下一个待重算的窗口。
    ///
    /// 返回 `None` 表示队列为空。
    pub fn pop(&mut self) -> Option<CqWindowKey> {
        if let Some(key) = self.queue.pop_front() {
            self.in_queue.remove(&key);
            Some(key)
        } else {
            None
        }
    }

    /// 批量取出最多 `max_count` 个待重算窗口。
    ///
    /// # Arguments
    /// * `max_count` - 最大取出数量。
    ///
    /// # Returns
    /// 取出的窗口键列表（可能少于 max_count）。
    pub fn drain(&mut self, max_count: usize) -> Vec<CqWindowKey> {
        let count = max_count.min(self.queue.len());
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(key) = self.queue.pop_front() {
                self.in_queue.remove(&key);
                result.push(key);
            }
        }
        result
    }

    /// 从队列中移除指定窗口（用于窗口 Final / 删表时同步清理，维持不变量）。
    ///
    /// 用 `in_queue` 做 O(1) 命中判定，命中时才 `queue.retain`（O(n) 最坏，n=队列长）。
    /// 返回 `true` 表示 key 确实在队列中并被移除，`false` 表示本就不在队列中。
    pub fn remove(&mut self, key: &CqWindowKey) -> bool {
        if self.in_queue.remove(key) {
            self.queue.retain(|k| k != key);
            true
        } else {
            false
        }
    }

    /// 清空队列。
    #[inline]
    pub fn clear(&mut self) {
        self.queue.clear();
        self.in_queue.clear();
    }

    /// 队列长度。
    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// 队列是否为空。
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl Default for RecalculationQueue {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// ─── Dirty Marking ───

/// 标记指定窗口为 dirty，如果窗口处于 Calculated 状态。
///
/// dirty 的窗口将在下次 tick 时被重新计算。
///
/// # Arguments
/// * `window_state` - 可变的窗口状态引用。
/// * `key` - 窗口键。
/// * `queue` - 重算队列。
pub fn mark_window_dirty(
    window_state: &mut CqWindowState,
    key: CqWindowKey,
    queue: &mut RecalculationQueue,
) {
    if window_state.status == CqWindowStatus::Calculated && !window_state.dirty {
        window_state.mark_dirty();
        queue.push(key);
    }
}

// ─── Final 判定 ───

/// 检查窗口是否应该被标记为 Final。
///
/// 判定条件：`now - window_end > staleness_window_ns`
///
/// # Arguments
/// * `window_state` - 窗口状态。
/// * `now` - 当前时间（epoch 纳秒）。
/// * `staleness_window_ns` - staleness 窗口时长（纳秒）。
///
/// # Returns
/// `true` 如果窗口应被标记为 Final。
#[inline]
pub fn is_window_stale(
    window_state: &CqWindowState,
    window_end: i64,
    now: i64,
    staleness_window_ns: i64,
) -> bool {
    window_state.status != CqWindowStatus::Final && (now - window_end) > staleness_window_ns
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::CqGranularity;

    #[test]
    fn test_queue_push_pop() {
        let mut queue = RecalculationQueue::new();
        let key1 = CqWindowKey::new(1, CqGranularity::Cq10s, 10_000_000_000);
        let key2 = CqWindowKey::new(1, CqGranularity::Cq10s, 20_000_000_000);

        queue.push(key1.clone());
        queue.push(key2.clone());
        assert_eq!(queue.len(), 2);

        // 重复 push 应被去重
        queue.push(key1.clone());
        assert_eq!(queue.len(), 2);

        let popped = queue.pop().unwrap();
        assert_eq!(popped, key1);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_queue_drain() {
        let mut queue = RecalculationQueue::new();
        for i in 0..10 {
            queue.push(CqWindowKey::new(
                1,
                CqGranularity::Cq10s,
                i * 10_000_000_000,
            ));
        }

        let batch = queue.drain(5);
        assert_eq!(batch.len(), 5);
        assert_eq!(queue.len(), 5);

        let remaining = queue.drain(10);
        assert_eq!(remaining.len(), 5);
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
    }

    /// 队列 remove：命中时同时清 `queue` 与 `in_queue`（VecDeque 与 HashSet 保持同步），
    /// 未命中返回 false 且不改动队列。
    #[test]
    fn test_queue_remove() {
        let mut queue = RecalculationQueue::new();
        let key1 = CqWindowKey::new(1, CqGranularity::Cq10s, 10_000_000_000);
        let key2 = CqWindowKey::new(1, CqGranularity::Cq10s, 20_000_000_000);
        let key3 = CqWindowKey::new(1, CqGranularity::Cq10s, 30_000_000_000);

        queue.push(key1.clone());
        queue.push(key2.clone());
        assert_eq!(queue.len(), 2);

        // 移除中间元素（非队头）：retain 保持 FIFO 顺序。
        assert!(queue.remove(&key1));
        assert_eq!(queue.len(), 1);
        assert!(!queue.remove(&key1), "重复 remove 应返回 false");
        // key2 仍在队列，可被正常弹出。
        assert_eq!(queue.pop(), Some(key2));

        // 未在队列中的 key 返回 false。
        assert!(!queue.remove(&key3));
    }

    #[test]
    fn test_mark_window_dirty() {
        use crate::window::CqWindowState;

        let mut state = CqWindowState::new();
        state.mark_calculated(1_000_000_000);
        let key = CqWindowKey::new(1, CqGranularity::Cq1m, 60_000_000_000);
        let mut queue = RecalculationQueue::new();

        mark_window_dirty(&mut state, key.clone(), &mut queue);
        assert!(state.dirty);
        assert_eq!(queue.len(), 1);

        // 重复标记不会重复入队
        let mut state2 = state.clone();
        state2.mark_dirty(); // 已经是 dirty
        mark_window_dirty(&mut state2, key, &mut queue);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_final_window_not_marked_dirty() {
        use crate::window::CqWindowState;

        let mut state = CqWindowState::new();
        state.mark_calculated(1_000_000_000);
        state.mark_final();
        let key = CqWindowKey::new(1, CqGranularity::Cq1m, 60_000_000_000);
        let mut queue = RecalculationQueue::new();

        mark_window_dirty(&mut state, key, &mut queue);
        assert!(!state.dirty);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_is_window_stale() {
        use crate::window::CqWindowState;

        let state = CqWindowState::new();
        let staleness_ns = 86_400_000_000_000i64; // 24h
        let window_end = 1_000_000_000_000i64;

        // now 在 staleness 内
        assert!(!is_window_stale(
            &state,
            window_end,
            window_end + staleness_ns / 2,
            staleness_ns
        ));

        // now 超出 staleness
        assert!(is_window_stale(
            &state,
            window_end,
            window_end + staleness_ns + 1,
            staleness_ns
        ));
    }
}

//! Debounce mechanism for derived tag recalculation.
//!
//! Prevents excessive recalculation by coalescing rapid successive changes
//! to the same source tags into a single recalculation batch.
//!
//! # Design
//!
//! - **Dirty set**: Tracks tags whose source data has changed but haven't been recalculated yet.
//! - **Last calc time**: Records when each tag was last calculated (for min-interval enforcement).
//! - **batch_id dedup**: Prevents processing the same write batch twice.

use sensearcdb_common::TenantId;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Tenant-scoped tag key（与 [`crate::dependency_graph::TagKey`] 同构，避免反向依赖）。
pub type TagKey = (TenantId, String);

/// State for the debounce mechanism.
///
/// Tracks which tags are "dirty" (need recalculation) and maintains
/// timing information to enforce minimum intervals between calculations.
#[derive(Debug)]
pub struct DebounceState {
    /// (tenant_id, tag_name) → timestamp of last calculation
    last_calc_times: HashMap<TagKey, Instant>,
    /// Tags currently marked as dirty (need recalculation)
    dirty_tags: HashSet<TagKey>,
    /// 每个 dirty tag 最近一次标脏时的源变化 ts（epoch 纳秒）。
    ///
    /// 写路径只 `mark_dirty`（不再内联重算），由去抖 flusher 异步重算；
    /// 结果时间戳必须取源变化 ts 而非 flusher 触发时刻，故在此持久化。
    pending_ts: HashMap<TagKey, i64>,
    /// The debounce interval (minimum time between recalculations)
    debounce_interval_ms: u64,
}

impl DebounceState {
    /// Create a new debounce state with the given interval.
    pub fn new(debounce_interval_ms: u64) -> Self {
        Self {
            last_calc_times: HashMap::new(),
            dirty_tags: HashSet::new(),
            pending_ts: HashMap::new(),
            debounce_interval_ms,
        }
    }

    /// Update the debounce interval.
    pub fn set_interval(&mut self, interval_ms: u64) {
        self.debounce_interval_ms = interval_ms;
    }

    /// Mark tags as dirty (their source data changed).
    ///
    /// Tags that are within their debounce cooldown period are not added.
    pub fn mark_dirty(&mut self, tags: &[TagKey]) {
        let now = Instant::now();
        for tag in tags {
            self.dirty_tags.insert(tag.clone());
            // Record or update the time so the timer knows
            self.last_calc_times.entry(tag.clone()).or_insert(now);
        }
    }

    /// 标脏并记录源变化 ts（供异步 flusher 重算时作为结果时间戳）。
    ///
    /// 同 [`Self::mark_dirty`]，但额外把 `ts_ns` 写入 [`Self::pending_ts`]，
    /// 取 max 保证乱序标脏时保留最新 ts。
    pub fn mark_dirty_at(&mut self, tags: &[TagKey], ts_ns: i64) {
        let now = Instant::now();
        for tag in tags {
            self.dirty_tags.insert(tag.clone());
            self.last_calc_times.entry(tag.clone()).or_insert(now);
            self.pending_ts
                .entry(tag.clone())
                .and_modify(|old| {
                    if ts_ns > *old {
                        *old = ts_ns;
                    }
                })
                .or_insert(ts_ns);
        }
    }

    /// Check if a specific tag is within its debounce cooldown period.
    ///
    /// Returns `true` if the tag should be suppressed (i.e., it was
    /// calculated too recently).
    pub fn is_in_cooldown(&self, tag: &TagKey) -> bool {
        if let Some(last) = self.last_calc_times.get(tag) {
            let elapsed = last.elapsed().as_millis() as u64;
            elapsed < self.debounce_interval_ms
        } else {
            false
        }
    }

    /// Record that a tag was just calculated (updates the timestamp).
    pub fn record_calculation(&mut self, tag: &TagKey) {
        self.last_calc_times.insert(tag.clone(), Instant::now());
        self.dirty_tags.remove(tag);
        self.pending_ts.remove(tag);
    }

    /// Take all dirty tags that are past their cooldown and ready for calculation.
    ///
    /// Returns the list of tag keys to recalculate, removing them from the dirty set.
    pub fn take_ready_tags(&mut self) -> Vec<TagKey> {
        let now = Instant::now();
        let interval = std::time::Duration::from_millis(self.debounce_interval_ms);

        let ready: Vec<TagKey> = self
            .dirty_tags
            .iter()
            .filter(|tag| {
                self.last_calc_times
                    .get(*tag)
                    .map(|t| now.duration_since(*t) >= interval)
                    .unwrap_or(true) // no last calc time → ready
            })
            .cloned()
            .collect();

        for tag in &ready {
            self.dirty_tags.remove(tag);
        }

        ready
    }

    /// Take all dirty tags past cooldown, each paired with its recorded source ts.
    ///
    /// 同 [`Self::take_ready_tags`]，但每个就绪 tag 附带 [`Self::pending_ts`]
    /// 记录的源变化 ts（无记录时默认 0），并从 pending_ts 中移除。供异步
    /// flusher 在重算时作为派生结果时间戳。
    pub fn take_ready_tags_with_ts(&mut self) -> Vec<(TagKey, i64)> {
        let now = Instant::now();
        let interval = std::time::Duration::from_millis(self.debounce_interval_ms);

        let ready: Vec<(TagKey, i64)> = self
            .dirty_tags
            .iter()
            .filter(|tag| {
                self.last_calc_times
                    .get(*tag)
                    .map(|t| now.duration_since(*t) >= interval)
                    .unwrap_or(true) // no last calc time → ready
            })
            .map(|tag| (tag.clone(), self.pending_ts.get(tag).copied().unwrap_or(0)))
            .collect();

        for (tag, _) in &ready {
            self.dirty_tags.remove(tag);
            self.pending_ts.remove(tag);
        }

        ready
    }

    /// Get all currently dirty tags without removing them.
    pub fn dirty_tags(&self) -> &HashSet<TagKey> {
        &self.dirty_tags
    }

    /// Check if there are any dirty tags.
    pub fn has_dirty(&self) -> bool {
        !self.dirty_tags.is_empty()
    }

    /// Clear all dirty tags.
    pub fn clear_dirty(&mut self) {
        self.dirty_tags.clear();
    }

    /// Get the number of dirty tags.
    pub fn dirty_count(&self) -> usize {
        self.dirty_tags.len()
    }
}

/// Batch ID tracker for preventing duplicate processing of the same write batch.
///
/// Write batches may trigger `on_source_changed` multiple times for the same
/// batch. This tracker prevents processing the same batch twice within a session.
#[derive(Debug, Default)]
pub struct BatchIdTracker {
    /// Set of already-processed batch IDs in the current session.
    processed: HashSet<u64>,
    /// Maximum number of batch IDs to track before eviction.
    max_size: usize,
}

impl BatchIdTracker {
    /// Create a new batch ID tracker.
    pub fn new(max_size: usize) -> Self {
        Self {
            processed: HashSet::new(),
            max_size,
        }
    }

    /// Check if a batch has already been processed.
    pub fn is_processed(&self, batch_id: u64) -> bool {
        self.processed.contains(&batch_id)
    }

    /// Mark a batch as processed. Returns `true` if it was newly marked.
    pub fn mark_processed(&mut self, batch_id: u64) -> bool {
        if self.processed.len() >= self.max_size {
            // Evict oldest: just clear half
            let to_remove: Vec<u64> = self
                .processed
                .iter()
                .take(self.max_size / 2)
                .copied()
                .collect();
            for id in to_remove {
                self.processed.remove(&id);
            }
        }
        self.processed.insert(batch_id)
    }

    /// Clear all tracked batch IDs.
    pub fn clear(&mut self) {
        self.processed.clear();
    }

    /// Return the number of tracked batch IDs.
    pub fn len(&self) -> usize {
        self.processed.len()
    }

    /// Return `true` if no batch IDs are tracked.
    pub fn is_empty(&self) -> bool {
        self.processed.is_empty()
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    fn k(t: TenantId, name: &str) -> TagKey {
        (t, name.to_string())
    }

    #[test]
    fn test_mark_and_take_dirty() {
        let mut state = DebounceState::new(0); // no cooldown
        state.mark_dirty(&[k(TenantId::DEFAULT, "tag_a"), k(TenantId::DEFAULT, "tag_b")]);

        assert!(state.has_dirty());
        assert_eq!(state.dirty_count(), 2);

        let ready = state.take_ready_tags();
        assert_eq!(ready.len(), 2);
        assert!(!state.has_dirty());
    }

    #[test]
    fn test_cooldown() {
        let mut state = DebounceState::new(1_000_000); // 1000 sec cooldown
        let tag = k(TenantId::DEFAULT, "tag_a");
        state.mark_dirty(&[tag.clone()]);
        state.record_calculation(&tag);

        // Should be in cooldown
        assert!(state.is_in_cooldown(&tag));
    }

    #[test]
    fn test_record_calculation_removes_dirty() {
        let mut state = DebounceState::new(100);
        let tag = k(TenantId::DEFAULT, "tag_a");
        state.mark_dirty(&[tag.clone()]);
        assert!(state.dirty_tags().contains(&tag));

        state.record_calculation(&tag);
        assert!(!state.dirty_tags().contains(&tag));
    }

    /// P4 去抖：同 tag 多次 `mark_dirty_at`（含乱序 ts）应合并为一次 ready，ts 取 max。
    #[test]
    fn test_mark_dirty_at_coalesces_and_takes_max_ts() {
        let mut state = DebounceState::new(0);
        let tag = k(TenantId::DEFAULT, "tag_a");
        state.mark_dirty_at(&[tag.clone()], 100);
        state.mark_dirty_at(&[tag.clone()], 200);
        state.mark_dirty_at(&[tag.clone()], 150); // 乱序，应取 max

        let ready = state.take_ready_tags_with_ts();
        assert_eq!(ready.len(), 1, "同 tag 多次标脏应合并为一次 ready");
        assert_eq!(ready[0].0, tag);
        assert_eq!(ready[0].1, 200, "ts 应取 max");
        assert!(!state.has_dirty(), "take 后 dirty 应清空");
    }

    #[test]
    fn test_clear_dirty() {
        let mut state = DebounceState::new(100);
        state.mark_dirty(&[k(TenantId::DEFAULT, "tag_a"), k(TenantId::DEFAULT, "tag_b")]);
        state.clear_dirty();
        assert!(!state.has_dirty());
    }

    #[test]
    fn test_tenant_isolated_cooldown() {
        let mut state = DebounceState::new(1_000_000);
        let a = k(TenantId(1), "dup");
        let b = k(TenantId(2), "dup");
        state.mark_dirty(&[a.clone()]);
        state.record_calculation(&a);
        // 租户 A 的 dup 进入 cooldown，租户 B 的 dup 不受影响。
        assert!(state.is_in_cooldown(&a));
        assert!(!state.is_in_cooldown(&b));
    }

    #[test]
    fn test_batch_id_tracker() {
        let mut tracker = BatchIdTracker::new(100);

        assert!(!tracker.is_processed(42));
        assert!(tracker.mark_processed(42));
        assert!(tracker.is_processed(42));

        // Re-marking returns false (was already present)
        assert!(!tracker.mark_processed(42));
    }

    #[test]
    fn test_batch_id_overflow() {
        let mut tracker = BatchIdTracker::new(10);
        for i in 0..20 {
            tracker.mark_processed(i);
        }
        // Should not panic; some entries evicted
        assert!(tracker.len() <= 10);
    }

    #[test]
    fn test_batch_id_clear() {
        let mut tracker = BatchIdTracker::new(100);
        tracker.mark_processed(1);
        tracker.mark_processed(2);
        tracker.clear();
        assert!(tracker.is_empty());
    }
}

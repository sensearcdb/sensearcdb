//! 列级 write_seq LWW 决胜纯函数（S4 后仅保留 [`new_wins_tie`]）。
//!
//! `merge_columns_typed`（列级 ts 单调归并）已在 S4 删除——列式写路径的列级
//! LWW 决胜由 `columnar.rs` 的 `lww_new_wins` / `lww_new_wins_cand` 直接复用
//! [`new_wins_tie`] 同构实现，WAL 回放改批量 `update_batch` 后不再需要独立归并函数。

/// 同 source_ts 的决胜判定：返回 `true` 表示 new 胜出（覆盖 old）。
///
/// # 规则（与 `compaction::dedup_tiebreaker` 的 wal_seq → server_ts 口径对齐，
///    source_ts 已由归并主比较消费、此处恒相等）
///
/// 1. `write_seq`：`Some > None` 优先；同为 `Some` 时大者胜；
/// 2. `server_ts`：仅当 seq 都 `None` 或 seq 相等时进入，大者胜；
/// 3. 完全平局：返回 `false`（保留 old，确定性兜底）。
#[inline]
pub fn new_wins_tie(
    new_seq: Option<u64>,
    old_seq: Option<u64>,
    new_sts: i64,
    old_sts: i64,
) -> bool {
    match (new_seq, old_seq) {
        (Some(n), Some(o)) => {
            if n != o {
                return n > o;
            }
            // seq 相等 → 退 server_ts。
        }
        (Some(_), None) => return true,
        (None, Some(_)) => return false,
        (None, None) => {}
    }
    if new_sts != old_sts {
        return new_sts > old_sts;
    }
    // 完全平局：保留 old。
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_wins_tie_ordering() {
        // Some > None。
        assert!(new_wins_tie(Some(1), None, 0, 0));
        assert!(!new_wins_tie(None, Some(1), 0, 0));
        // 同为 Some，大者胜。
        assert!(new_wins_tie(Some(5), Some(3), 0, 0));
        assert!(!new_wins_tie(Some(3), Some(5), 0, 0));
        // 同为 Some 且相等 → 退 server_ts 大者胜。
        assert!(new_wins_tie(Some(5), Some(5), 999, 200));
        assert!(!new_wins_tie(Some(5), Some(5), 200, 999));
        // 同为 None → 退 server_ts 大者胜。
        assert!(new_wins_tie(None, None, 999, 200));
        assert!(!new_wins_tie(None, None, 200, 999));
        // 完全平局 → 保留 old。
        assert!(!new_wins_tie(None, None, 200, 200));
        assert!(!new_wins_tie(Some(5), Some(5), 200, 200));
    }
}

//! k-way merge + dedup + re-encode。
//!
//! Compaction 的合并核心：从多个 SAC Block 读取数据，
//! 按 (table_id, ts) 去重，重新编码为新的 Block。
//!
//! ## 去重优先级（硬规则）
//!
//! 同 (table_id, ts) 重复时的去重优先级：
//! 1. WAL seq（从 Block mini-header 的 `max_wal_seq` 获取）
//! 2. server_ts（服务器接收时间戳）
//! 3. source_ts（原始数据时间戳）
//!
//! ## max_wal_seq 三态语义（硬规则，锁死不可改）
//!
//! | 数据来源                         | max_wal_seq   |
//! |----------------------------------|---------------|
//! | WAL → MemTable → SAC (Flush)     | `Some(seq)`   |
//! | Compaction 合并输出              | `None`        |
//! | 回放 (Replay) 产生的 SAC Block   | `None`        |
//! | 批量导入 (Bulk Import)           | `None`        |
//!
//! **Compaction 输出必须设为 None**：去重已消费 seq，避免后续跨 Block 错误比较。

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::iter::Peekable;

use crate::memtable::col_vec::TypedValue;

/// Compaction 时使用的内部数据点表示。
#[derive(Debug, Clone)]
pub struct DataPoint {
    /// 表 ID。
    pub table_id: u32,
    /// 源时间戳。
    pub ts: i64,
    /// 服务器时间戳。
    pub server_ts: i64,
    /// 各列的值（column_id → `TypedValue`，类型化保真）。
    pub values: Vec<(u16, TypedValue)>,
    /// 各列的质量码（column_id → u8）。
    pub qualities: Vec<(u16, u8)>,
    /// WAL 序列号（None = 无 WAL seq，如 Compaction 输出或 Replay 数据）。
    pub wal_seq: Option<u64>,
    /// 源 Block 的 ID（用于 merge 排序稳定性）。
    pub source_id: usize,
}

impl PartialEq for DataPoint {
    fn eq(&self, other: &Self) -> bool {
        self.table_id == other.table_id && self.ts == other.ts
    }
}

impl Eq for DataPoint {}

impl PartialOrd for DataPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        // 按 (table_id, ts) 排序，小顶堆取最小值
        self.table_id
            .cmp(&other.table_id)
            .then_with(|| self.ts.cmp(&other.ts))
            .then_with(|| self.source_id.cmp(&other.source_id))
            .reverse() // BinaryHeap 是最大堆，反转以得到最小堆
    }
}

/// 同 (table_id, ts) 重复时的去重优先级。
///
/// 1. WAL seq: 大者优先
/// 2. server_ts: 大者优先
/// 3. source_ts（ts）: 大者优先
///
/// Compaction 输出必须设 `max_wal_seq = None`。
pub fn dedup_tiebreaker(a: &DataPoint, b: &DataPoint) -> Ordering {
    // 1. WAL seq: a 大则 a 优先；Some > None（flush 最新数据优先于 compaction 旧输出）
    match (a.wal_seq, b.wal_seq) {
        (Some(sa), Some(sb)) if sa != sb => return sa.cmp(&sb),
        (Some(_), None) => return Ordering::Greater, // a 有 wal_seq → a 优先
        (None, Some(_)) => return Ordering::Less,    // b 有 wal_seq → b 优先
        _ => {}
    }
    // 2. server_ts: a 大则 a 优先
    if a.server_ts != b.server_ts {
        return a.server_ts.cmp(&b.server_ts);
    }
    // 3. source_ts: a 大则 a 优先
    a.ts.cmp(&b.ts)
}

/// k-way 合并多个有序数据流。
///
/// 使用小顶堆实现。
///
/// # 参数
/// - `streams`: 多个有序的数据点迭代器。
///
/// # 返回值
/// 合并后按 (table_id, ts) 排序的数据点列表。
pub fn k_way_merge<I>(streams: Vec<I>) -> Vec<DataPoint>
where
    I: IntoIterator<Item = DataPoint>,
{
    let mut heap = BinaryHeap::new();

    // 从每个流中取第一个元素
    let mut iterators: Vec<_> = streams
        .into_iter()
        .map(|s| s.into_iter().peekable())
        .collect();

    for (idx, iter) in iterators.iter_mut().enumerate() {
        if let Some(point) = iter.next() {
            heap.push((point, idx));
        }
    }

    let mut result = Vec::new();

    while let Some((point, idx)) = heap.pop() {
        result.push(point);

        // 从同一流中取下一个元素
        if let Some(next_point) = iterators[idx].next() {
            heap.push((next_point, idx));
        }
    }

    result
}

/// 去重：对已排序的数据点，按 (table_id, ts) 去重。
///
/// 使用 `dedup_tiebreaker` 决定保留哪个。
///
/// # 参数
/// - `points`: 已按 (table_id, ts) 排序的数据点。
///
/// # 返回值
/// 去重后的数据点列表。
pub fn dedup_sorted(points: Vec<DataPoint>) -> Vec<DataPoint> {
    if points.is_empty() {
        return points;
    }

    let mut result = Vec::with_capacity(points.len());
    let mut current = points[0].clone();

    for point in points.into_iter().skip(1) {
        if point.table_id == current.table_id && point.ts == current.ts {
            // 重复：使用 tiebreaker 决定保留哪个
            if dedup_tiebreaker(&point, &current) == Ordering::Greater {
                current = point;
            }
        } else {
            result.push(current);
            current = point;
        }
    }
    result.push(current);

    result
}

/// 流式 k-way 归并：不物化整份结果，逐点产出。
///
/// 与 [`k_way_merge`] 语义逐点一致：多个已按 (table_id, ts) 升序的流，
/// 按 (table_id, ts, source_id) 全局有序地归并。内存 O(stream 数)。
///
/// 关键不变量 D2：排序序 `(table_id, ts, source_id)` 与 `DataPoint::cmp`
/// 完全一致，仅把「结果累积到 Vec」换成「随取随出」。
pub struct KWayMerge<I: Iterator<Item = DataPoint>> {
    /// 小顶堆（借 `DataPoint::Ord` 的 reverse 实现），元素为 (点, 源流下标)。
    heap: BinaryHeap<(DataPoint, usize)>,
    /// 各源流（peekable，用于堆弹出后补充下一元素）。
    iterators: Vec<Peekable<I>>,
}

impl<I: Iterator<Item = DataPoint>> KWayMerge<I> {
    /// 从多个有序流构建归并器。
    pub fn new(streams: Vec<I>) -> Self {
        let mut heap = BinaryHeap::new();
        let mut iterators: Vec<Peekable<I>> = streams.into_iter().map(|s| s.peekable()).collect();

        for (idx, iter) in iterators.iter_mut().enumerate() {
            if let Some(point) = iter.next() {
                heap.push((point, idx));
            }
        }

        Self { heap, iterators }
    }
}

impl<I: Iterator<Item = DataPoint>> Iterator for KWayMerge<I> {
    type Item = DataPoint;

    fn next(&mut self) -> Option<DataPoint> {
        let (point, idx) = self.heap.pop()?;
        if let Some(next_point) = self.iterators[idx].next() {
            self.heap.push((next_point, idx));
        }
        Some(point)
    }
}

/// 流式去重：对已按 (table_id, ts) 排序的流做相邻去重，逐点产出。
///
/// 与 [`dedup_sorted`] 语义逐点一致（不变量 D1）：
/// 同 (table_id, ts) 仅保留 [`dedup_tiebreaker`] 胜者；跨 key 边界立即产出。
/// 内存 O(1)。
pub struct DedupIter<I: Iterator<Item = DataPoint>> {
    /// 上游（已全局有序）数据流。
    inner: I,
    /// 当前运行段的胜者（尚未产出）。
    current: Option<DataPoint>,
}

impl<I: Iterator<Item = DataPoint>> DedupIter<I> {
    /// 从有序流构建去重器。
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            current: None,
        }
    }
}

impl<I: Iterator<Item = DataPoint>> Iterator for DedupIter<I> {
    type Item = DataPoint;

    fn next(&mut self) -> Option<DataPoint> {
        if self.current.is_none() {
            self.current = Some(self.inner.next()?);
        }

        loop {
            match self.inner.next() {
                None => return self.current.take(),
                Some(next) => {
                    let (same_key, replace) = {
                        let cur = self.current.as_ref().expect("current must be present");
                        let same_key = next.table_id == cur.table_id && next.ts == cur.ts;
                        let replace = same_key && dedup_tiebreaker(&next, cur) == Ordering::Greater;
                        (same_key, replace)
                    };
                    if replace {
                        self.current = Some(next);
                    } else if !same_key {
                        // 跨 key 边界：产出当前胜者，把 next 存为下一段的胜者。
                        return self.current.replace(next);
                    }
                    // same_key && !replace：丢弃 next，保留当前胜者，继续扫描。
                }
            }
        }
    }
}

/// 拼接路径专用决胜键（D8）：源文件 file_rank 决胜，禁用 wal_seq。
///
/// `source_id` 承载 file_rank（0 = 最新文件，按 `write_epoch DESC, seq DESC` 排序后
/// 的源文件下标）。rank 小者赢（新文件覆盖旧文件），与查询的跨文件 LWW
/// （`write_epoch DESC, seq DESC`，`executor.rs:2304`）语义一致。同 rank 退化为
/// 按 ts 大者优先（确定性兜底）。
///
/// 为什么禁用 wal_seq：L1 起全是 compaction 产物，`max_wal_seq=None` 恒退化，
/// 无法区分新旧；唯一可靠且与查询一致的是文件头 `write_epoch` + `seq`。
pub fn dedup_tiebreaker_by_file_rank(a: &DataPoint, b: &DataPoint) -> Ordering {
    if a.source_id != b.source_id {
        // a 的 rank 更小（更新）→ b.source_id.cmp(&a.source_id) == Greater → a 赢
        return b.source_id.cmp(&a.source_id);
    }
    a.ts.cmp(&b.ts)
}

/// 流式去重（拼接路径专用）：对已按 (table_id, ts) 排序的流做相邻去重，
/// 决胜键用 [`dedup_tiebreaker_by_file_rank`]（file_rank 优先，禁用 wal_seq）。
///
/// 与 [`DedupIter`] 同构，仅决胜键不同；不改变 L0→L1 点级路径的 `dedup_tiebreaker`。
pub struct DedupIterByFileRank<I: Iterator<Item = DataPoint>> {
    /// 上游（已全局有序）数据流。
    inner: I,
    /// 当前运行段的胜者（尚未产出）。
    current: Option<DataPoint>,
}

impl<I: Iterator<Item = DataPoint>> DedupIterByFileRank<I> {
    /// 从有序流构建拼接路径去重器。
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            current: None,
        }
    }
}

impl<I: Iterator<Item = DataPoint>> Iterator for DedupIterByFileRank<I> {
    type Item = DataPoint;

    fn next(&mut self) -> Option<DataPoint> {
        if self.current.is_none() {
            self.current = Some(self.inner.next()?);
        }

        loop {
            match self.inner.next() {
                None => return self.current.take(),
                Some(next) => {
                    let cur = self.current.as_ref().expect("current must be present");
                    let same_key = next.table_id == cur.table_id && next.ts == cur.ts;
                    if !same_key {
                        // 跨 key 边界：产出当前胜者，把 next 存为下一段的胜者。
                        return self.current.replace(next);
                    }
                    if dedup_tiebreaker_by_file_rank(&next, cur) == Ordering::Greater {
                        self.current = Some(next);
                    }
                    // same_key && !replace：丢弃 next，保留当前胜者，继续扫描。
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_point(table_id: u32, ts: i64, wal_seq: Option<u64>, server_ts: i64) -> DataPoint {
        DataPoint {
            table_id,
            ts,
            server_ts,
            values: Vec::new(),
            qualities: Vec::new(),
            wal_seq,
            source_id: 0,
        }
    }

    fn make_point_with_source(
        table_id: u32,
        ts: i64,
        wal_seq: Option<u64>,
        server_ts: i64,
        source_id: usize,
    ) -> DataPoint {
        DataPoint {
            table_id,
            ts,
            server_ts,
            values: Vec::new(),
            qualities: Vec::new(),
            wal_seq,
            source_id,
        }
    }

    #[test]
    fn test_k_way_merge_single() {
        let points = vec![
            make_point(1, 100, None, 100),
            make_point(1, 200, None, 200),
            make_point(1, 300, None, 300),
        ];
        let result = k_way_merge(vec![points]);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_k_way_merge_two_streams() {
        let s1 = vec![make_point(1, 100, None, 100), make_point(1, 300, None, 300)];
        let s2 = vec![make_point(1, 200, None, 200), make_point(1, 400, None, 400)];
        let result = k_way_merge(vec![s1, s2]);
        assert_eq!(result.len(), 4);
        let tss: Vec<i64> = result.iter().map(|p| p.ts).collect();
        assert_eq!(tss, vec![100, 200, 300, 400]);
    }

    #[test]
    fn test_k_way_merge_three_streams() {
        let s1 = vec![
            make_point_with_source(1, 100, None, 100, 0),
            make_point_with_source(1, 400, None, 400, 0),
        ];
        let s2 = vec![
            make_point_with_source(1, 200, None, 200, 1),
            make_point_with_source(1, 500, None, 500, 1),
        ];
        let s3 = vec![
            make_point_with_source(1, 300, None, 300, 2),
            make_point_with_source(1, 600, None, 600, 2),
        ];
        let result = k_way_merge(vec![s1, s2, s3]);
        assert_eq!(result.len(), 6);
        let tss: Vec<i64> = result.iter().map(|p| p.ts).collect();
        assert_eq!(tss, vec![100, 200, 300, 400, 500, 600]);
    }

    #[test]
    fn test_k_way_merge_cross_table() {
        let s1 = vec![make_point(1, 100, None, 100), make_point(1, 200, None, 200)];
        let s2 = vec![make_point(2, 150, None, 150), make_point(2, 250, None, 250)];
        let result = k_way_merge(vec![s1, s2]);
        // Sorted by (table_id, ts): table 1 first, then table 2
        assert_eq!(result.len(), 4);
        let keys: Vec<(u32, i64)> = result.iter().map(|p| (p.table_id, p.ts)).collect();
        assert_eq!(keys, vec![(1, 100), (1, 200), (2, 150), (2, 250)]);
    }

    #[test]
    fn test_k_way_merge_empty_streams() {
        let s1: Vec<DataPoint> = vec![];
        let s2 = vec![make_point(1, 100, None, 100)];
        let result = k_way_merge(vec![s1, s2]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].ts, 100);
    }

    #[test]
    fn test_k_way_merge_all_empty() {
        let result: Vec<DataPoint> = k_way_merge::<Vec<DataPoint>>(vec![vec![], vec![], vec![]]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_dedup_by_wal_seq() {
        let a = make_point(1, 100, Some(10), 100);
        let b = make_point(1, 100, Some(20), 100);
        // b 的 WAL seq 更大 → b 优先
        assert_eq!(dedup_tiebreaker(&b, &a), Ordering::Greater);
    }

    #[test]
    fn test_dedup_by_wal_seq_some_over_none() {
        // Some(来自 flush 的最新数据) 必须优先于 None(来自 compaction 的旧输出)
        let with_seq = make_point(1, 100, Some(1), 100);
        let no_seq = make_point(1, 100, None, 999);
        // a=Some 时 a 优先（即使 b.server_ts 更大）
        assert_eq!(dedup_tiebreaker(&with_seq, &no_seq), Ordering::Greater);
        // a=None 时 b(Some) 优先
        assert_eq!(dedup_tiebreaker(&no_seq, &with_seq), Ordering::Less);
    }

    #[test]
    fn test_dedup_by_wal_seq_some_some_equal_unchanged() {
        // Some vs Some 等值：不比较 seq，退到 server_ts → ts（原语义不变）
        let a = make_point(1, 100, Some(1), 100);
        let b = make_point(1, 100, Some(1), 100);
        assert_eq!(dedup_tiebreaker(&a, &b), Ordering::Equal);
    }

    #[test]
    fn test_dedup_by_server_ts() {
        let a = make_point(1, 100, None, 100);
        let b = make_point(1, 100, None, 200);
        // b 的 server_ts 更大 → b 优先
        assert_eq!(dedup_tiebreaker(&b, &a), Ordering::Greater);
    }

    #[test]
    fn test_dedup_by_source_ts() {
        let a = make_point(1, 100, None, 100);
        let b = make_point(1, 200, None, 100);
        assert_eq!(dedup_tiebreaker(&b, &a), Ordering::Greater);
    }

    #[test]
    fn test_dedup_sorted() {
        let points = vec![
            make_point(1, 100, Some(5), 100),
            make_point(1, 100, Some(10), 100), // 重复，WAL seq 更大
            make_point(1, 200, None, 200),
            make_point(1, 300, None, 300),
            make_point(1, 300, None, 400), // 重复，server_ts 更大
        ];
        let result = dedup_sorted(points);
        assert_eq!(result.len(), 3);
        // 第一个保留 WAL seq=10 的
        assert_eq!(result[0].wal_seq, Some(10));
        // 第三个保留 server_ts=400 的
        assert_eq!(result[2].server_ts, 400);
    }

    #[test]
    fn test_dedup_sorted_no_duplicates() {
        let points = vec![
            make_point(1, 100, None, 100),
            make_point(1, 200, None, 200),
            make_point(2, 100, None, 300),
        ];
        let result = dedup_sorted(points);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_dedup_sorted_single() {
        let points = vec![make_point(1, 100, None, 100)];
        let result = dedup_sorted(points);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_dedup_sorted_empty() {
        let result = dedup_sorted(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_k_way_merge_then_dedup() {
        // Integration: merge 3 streams then dedup
        let s1 = vec![
            make_point_with_source(1, 100, Some(5), 100, 0),
            make_point_with_source(1, 300, None, 300, 0),
        ];
        let s2 = vec![
            make_point_with_source(1, 100, Some(20), 100, 1), // dup, higher WAL seq
            make_point_with_source(1, 200, None, 200, 1),
        ];
        let s3 = vec![
            make_point_with_source(1, 300, None, 500, 2), // dup, higher server_ts
        ];

        let merged = k_way_merge(vec![s1, s2, s3]);
        assert_eq!(merged.len(), 5); // before dedup

        let deduped = dedup_sorted(merged);
        assert_eq!(deduped.len(), 3); // after dedup: 100, 200, 300

        // Check the winning entries
        assert_eq!(deduped[0].ts, 100);
        assert_eq!(deduped[0].wal_seq, Some(20)); // WAL seq winner

        assert_eq!(deduped[2].ts, 300);
        assert_eq!(deduped[2].server_ts, 500); // server_ts winner
    }

    /// D2 护栏：流式 `KWayMerge` 与旧全量 `k_way_merge` 逐点一致。
    #[test]
    fn test_kway_stream_eq_full_materialize() {
        let s1 = vec![
            make_point_with_source(1, 100, None, 100, 0),
            make_point_with_source(1, 400, None, 400, 0),
        ];
        let s2 = vec![
            make_point_with_source(1, 200, None, 200, 1),
            make_point_with_source(2, 150, None, 150, 1),
        ];
        let s3 = vec![make_point_with_source(2, 250, None, 250, 2)];

        let full = k_way_merge(vec![s1.clone(), s2.clone(), s3.clone()]);
        let streamed: Vec<DataPoint> =
            KWayMerge::new(vec![s1.into_iter(), s2.into_iter(), s3.into_iter()]).collect();
        assert_eq!(full, streamed);
    }

    /// D1 护栏：流式 `DedupIter` 与旧全量 `dedup_sorted` 逐点一致。
    #[test]
    fn test_dedup_stream_eq_full_materialize() {
        let points = vec![
            make_point(1, 100, Some(5), 100),
            make_point(1, 100, Some(10), 100), // dup, higher WAL seq wins
            make_point(1, 200, None, 200),
            make_point(1, 300, None, 300),
            make_point(1, 300, None, 400), // dup, higher server_ts wins
            make_point(2, 100, None, 100), // 跨 table 边界
        ];

        let full = dedup_sorted(points.clone());
        let streamed: Vec<DataPoint> = DedupIter::new(points.into_iter()).collect();
        assert_eq!(full, streamed);
    }

    /// D1+D2 集成护栏：k-way → dedup 的流式链与旧全量链逐点一致。
    #[test]
    fn test_stream_chain_eq_full_chain() {
        let s1 = vec![
            make_point_with_source(1, 100, Some(5), 100, 0),
            make_point_with_source(1, 300, None, 300, 0),
        ];
        let s2 = vec![
            make_point_with_source(1, 100, Some(20), 100, 1), // dup, higher seq
            make_point_with_source(1, 200, None, 200, 1),
        ];
        let s3 = vec![
            make_point_with_source(1, 300, None, 500, 2), // dup, higher server_ts
        ];

        let full_merged = k_way_merge(vec![s1.clone(), s2.clone(), s3.clone()]);
        let full_deduped = dedup_sorted(full_merged);

        let streamed: Vec<DataPoint> = DedupIter::new(KWayMerge::new(vec![
            s1.into_iter(),
            s2.into_iter(),
            s3.into_iter(),
        ]))
        .collect();
        assert_eq!(full_deduped, streamed);
    }

    /// D8：拼接决胜键 file_rank 小者赢（0=最新），且禁用 wal_seq（即使 Some 也不优先）。
    #[test]
    fn test_dedup_tiebreaker_by_file_rank() {
        // file_rank 0（新文件）vs file_rank 1（旧文件）：0 赢，即使旧文件带 wal_seq。
        let newer = make_point_with_source(1, 100, None, 100, 0);
        let older = make_point_with_source(1, 100, Some(999), 100, 1);
        assert_eq!(
            dedup_tiebreaker_by_file_rank(&newer, &older),
            Ordering::Greater
        );
        assert_eq!(
            dedup_tiebreaker_by_file_rank(&older, &newer),
            Ordering::Less
        );

        // 同 file_rank → 退化为 ts 大者优先。
        let a = make_point_with_source(1, 100, None, 100, 0);
        let b = make_point_with_source(1, 200, None, 100, 0);
        assert_eq!(dedup_tiebreaker_by_file_rank(&b, &a), Ordering::Greater);
    }

    /// D8：`DedupIterByFileRank` 跨文件同 ts 取 file_rank 小者（新文件赢）。
    #[test]
    fn test_dedup_iter_by_file_rank_keeps_newer_file() {
        // 旧文件（rank 1）与 新文件（rank 0）都含 ts=100，新文件值应赢。
        let older = make_point_with_source(1, 100, None, 100, 1);
        let newer = make_point_with_source(1, 100, None, 100, 0);
        let tail = make_point_with_source(1, 200, None, 200, 0);
        let merged: Vec<DataPoint> = DedupIterByFileRank::new(
            vec![older, newer, tail].into_iter(),
        )
        .collect();
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].ts, 100);
        assert_eq!(merged[0].source_id, 0, "同 ts 应保留 file_rank 更小（新）的");
        assert_eq!(merged[1].ts, 200);
    }
}

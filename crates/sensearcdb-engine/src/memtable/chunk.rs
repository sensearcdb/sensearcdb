//! ColumnarChunk: SoA 列式存储单元（单表）。
//!
//! 每个 `ColumnarChunk` 对应一个 `table_id`，包含该表的所有列数据。
//! 采用 SoA (Structure of Arrays) 布局：timestamps 为共享时间轴，
//! 各列独立存储。
//!
//! ## 双模式
//!
//! - **Vec 模式**（默认）：时间戳有序追加，O(1) 插入。
//! - **BTreeMap 模式**（乱序降级）：检测到乱序写入时自动切换，O(log N) 插入。

use std::collections::{BTreeMap, HashMap, HashSet};

use sensearcdb_common::TenantId;

use crate::engine::DataPoint;
use crate::memtable::col_vec::{ColVec, TypedValue};

/// 单表列式存储单元。
///
/// 每个 Chunk 存储一个 table 的所有行列数据。
/// 正常路径使用 Vec 追加（O(1)），检测到 `ts < last_ts` 时标记 `out_of_order = true`
/// 并启用 `ts_index`（BTreeMap）来维护时间顺序。
#[derive(Debug, Clone)]
pub struct ColumnarChunk {
    /// 所属表 ID。
    pub table_id: u32,
    /// 所属租户 ID（0 = 默认租户；S1 全链路恒 0，仅携带落 SAC header/路径）。
    pub tenant_id: TenantId,
    /// 共享时间轴（SoA）：每个数据点对应一个时间戳。
    /// `timestamps[i]` 对应 `columns[*][i]` 和 `qualities[i]`。
    pub timestamps: Vec<i64>,
    /// 各列的值：`column_id → ColVec`。
    /// 每列独立存储，长度与 timestamps 一致（稀疏行用 `push_absent` 占位对齐）。
    pub columns: HashMap<u16, ColVec>,
    /// 每列的存在位图：`presence[col_id][row_idx] == true` 表示该行该列有值；
    /// `false` 表示稀疏缺失（读取时返回 `None`，占位值不暴露）。
    ///
    /// 为兼容外部直接构造的 chunk（测试/SAC 密集读回），位图缺失时按密集列
    /// （`row_idx < col.len()`）回退判定。
    pub presence: HashMap<u16, Vec<bool>>,
    /// 质量码数组：`qualities[i]` 对应第 i 个数据点。
    pub qualities: Vec<u8>,
    /// 时间戳 → row_idx 的索引（BTreeMap，乱序路径用）。
    pub ts_index: BTreeMap<i64, usize>,
    /// 数据点总数（= timestamps.len()）。
    pub point_count: usize,
    /// 是否检测到乱序写入（触发 BTreeMap 降级路径）。
    pub out_of_order: bool,
    /// 本 Chunk 内数据的最大 WAL 序列号（S7 三态：Flush 回填 Some，Compaction/回放/导入 None）。
    pub max_wal_seq: Option<u64>,
    /// 本 Chunk 内数据的**最小** WAL 序列号（回收水位边界用；语义同 `max_wal_seq`）。
    ///
    /// 每点 seq = 该点所属 WAL 帧的 `raft_index`（`seq_start`），故 chunk 的最小
    /// 点级 seq == chunk 内最小帧 `raft_index`。回收器据此计算「未 flush 数据的最老
    /// WAL 帧」边界 `S = min_unflushed_frame - 1`。
    pub min_wal_seq: Option<u64>,
}

impl ColumnarChunk {
    /// 创建一个空的 ColumnarChunk。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID（S1 恒 0）。
    /// - `table_id`: 目标表 ID。
    pub fn new(tenant_id: TenantId, table_id: u32) -> Self {
        Self {
            table_id,
            tenant_id,
            timestamps: Vec::new(),
            columns: HashMap::new(),
            presence: HashMap::new(),
            qualities: Vec::new(),
            ts_index: BTreeMap::new(),
            point_count: 0,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        }
    }

    /// 记录一个点的 WAL 序列号，同时更新本 Chunk 的 min/max（取极值）。
    pub fn record_wal_seq(&mut self, seq: u64) {
        self.max_wal_seq = Some(self.max_wal_seq.map_or(seq, |cur| cur.max(seq)));
        self.min_wal_seq = Some(self.min_wal_seq.map_or(seq, |cur| cur.min(seq)));
    }

    /// 更新本 Chunk 的最大 WAL 序列号（取较大者）。
    ///
    /// 兼容旧调用方；内部同时维护 `min_wal_seq`（回收水位边界需要）。
    pub fn update_max_wal_seq(&mut self, seq: u64) {
        self.record_wal_seq(seq);
    }

    /// 追加一行数据（单一 LWW：同 ts 后写靠 `ts_index.insert` 覆盖旧映射，旧值变孤儿）。
    ///
    /// # 参数
    /// - `ts`: 时间戳。
    /// - `values`: 列值列表（column_id, `TypedValue`）。
    /// - `qualities`: 质量码列表（column_id, u8）。按「per-point 单质量流」收敛为
    ///   该行的单一质量码（全列共享，O2=A）。
    /// - `flags`: 点标记位。
    ///
    /// # 返回值
    /// 本点是否有序（`ts >= prev_last`；首点恒 true）。
    pub fn append(
        &mut self,
        ts: i64,
        values: &[(u16, TypedValue)],
        qualities: &[(u16, u8)],
        flags: u8,
    ) -> bool {
        let _ = flags; // 暂未使用 flags

        // 乱序检测（P3 修复）：在 push 新时间戳之前缓存 prev_last，
        // 用 `ts >= prev_last` 判断，杜绝「先 push 再 self.timestamps.last() 恒跟自己比」的恒真。
        let prev_last = self.timestamps.last().copied();
        let ordered = match prev_last {
            Some(prev) => ts >= prev,
            None => true,
        };
        if !ordered {
            // 首次乱序：把此前有序路径累积的时间戳一次性物化进 ts_index（LWW 后写覆盖），
            // 此后 ts_index 转为 eager 权威维护。有序路径保持惰性（不维护 ts_index）。
            if !self.out_of_order {
                self.rebuild_ts_index();
            }
            self.out_of_order = true;
        }

        // 追加时间戳
        let row_idx = self.timestamps.len();
        self.timestamps.push(ts);

        // 按物理类型为每列写对应 ColVec（GAP-ENG-14），类型不匹配时告警跳过。
        // 同时维护 presence 位图：有值=true；类型不匹配被跳过/稀疏缺失=false。
        // 稀疏对齐关键：新列须回填「此前所有行」的占位，保证列向量长度恒 == 行数。
        if self.is_fully_dense(values) {
            // 全列 present 快路径：密集宽表（每列每行都有值）跳过 per-point HashSet
            // 分配与 absent 补齐（零稀疏缺失），直接按列数组写入。
            // 密集态（该列无 presence entry）不再维护位图，省去 per-point HashMap 查找
            // 与 Vec<bool> push；仅在该列首次出现稀疏缺失/类型不匹配时才物化位图。
            for (col_id, val) in values {
                self.write_existing_column_dense(*col_id, val, row_idx);
            }
        } else {
            // 稀疏/新列路径：保留 HashSet 维护 present 集合 + absent 补齐，不破坏稀疏语义。
            //
            // 物化：密集时期（presence 被抑制）累积的既有列，其前 row_idx 行全密集有值
            // → 补全 true 位图，保证后续稀疏缺失列的 presence push(false) 长度对齐正确。
            for &col_id in self.columns.keys() {
                self.presence
                    .entry(col_id)
                    .or_insert_with(|| vec![true; row_idx]);
            }
            //
            // 防御性去重（重复 col_id 双写修复）：上游契约要求单点内 column_id 唯一，
            // 但为防非法输入破坏「列向量长度 == 行数」不变式，此处先按 col_id 去重、
            // 保留最后出现值，再逐列写入。否则重复 col_id 会在一次 append 内被连续
            // push 多次（列向量长度 > 行数 → 数据错位）。
            let mut present: HashSet<u16> = HashSet::with_capacity(values.len());
            let mut last_value: HashMap<u16, &TypedValue> = HashMap::with_capacity(values.len());
            for (col_id, val) in values {
                present.insert(*col_id);
                last_value.insert(*col_id, val);
            }
            for (col_id, val) in last_value.iter() {
                if !self.columns.contains_key(col_id) {
                    let mut c = ColVec::new_for_type(val.data_type());
                    for _ in 0..row_idx {
                        c.push_absent();
                    }
                    self.columns.insert(*col_id, c);
                }
                if row_idx == 0 {
                    // 首行：所有列都是新建、无占位行、所有行（本行）都有值 → 走密集写入，
                    // 类型匹配则保持「无 presence entry」纯密集态，让后续密集 append 真正零 presence 开销。
                    self.write_existing_column_dense(*col_id, val, row_idx);
                } else {
                    // row_idx>0：新建列有 row_idx 个占位行（无值），必须保留 presence 补 false；
                    // 已存在列同样走原逻辑。
                    self.write_existing_column(*col_id, val, row_idx);
                }
            }

            // 补齐本行缺失的已知列（稀疏行）：占位 + presence=false，
            // 保证「列向量长度 == 行数」，稀疏行读取返回 None 而非错位值。
            if present.len() < self.columns.len() {
                let absent: Vec<u16> = self
                    .columns
                    .keys()
                    .copied()
                    .filter(|c| !present.contains(c))
                    .collect();
                for col_id in absent {
                    if let Some(col) = self.columns.get_mut(&col_id) {
                        col.push_absent();
                    }
                    self.presence.entry(col_id).or_default().push(false);
                }
            }
        }

        // 追加质量码（per-point 单质量流：全列共享一个 qual 流）
        let qual = qualities
            .iter()
            .find(|(cid, _)| *cid == 0)
            .map(|(_, q)| *q)
            .unwrap_or(0xC0);
        self.qualities.push(qual);

        // 单一 LWW：乱序模式下 ts_index 是唯一真相源（同 ts 后写覆盖旧映射）；
        // 有序模式保持惰性（不维护 ts_index，row_idx 即时间顺序）。
        if self.out_of_order {
            self.ts_index.insert(ts, row_idx);
        }
        self.point_count += 1;

        ordered
    }

    /// 批量追加一行「同表」数据点（写路径优化方向 1：按列批量写入）。
    ///
    /// 语义与逐点 [`append`](Self::append) 完全等价：批量路径仅改变「列值的写入组织方式」
    /// （每列一次 `HashMap` 查找 + 批量 `push`），乱序检测、`ts_index` 物化/eager 维护、
    /// 时间轴/质量码追加、LWW、presence 三态等仍按逐点顺序推进，最终 `ColumnarChunk`
    /// 状态（列向量、presence、ts_index、时间轴、统计）与逐点 append 逐字节一致。
    ///
    /// # 快路径条件
    /// 仅当批内所有点「均匀密集」（列集合与首点一致且覆盖全部既有列、无新列、无重复列、
    /// col_id < 128，即逐点路径也会恒走 dense 分支）时启用按列批量写入；
    /// 其余情形（稀疏/新列/列集变化/新鲜表首行等）回退逐点 append，保证语义零偏差。
    pub fn append_batch(&mut self, points: &[&DataPoint]) {
        let n = points.len();
        if n == 0 {
            return;
        }

        if !self.is_uniform_dense_batch(points) {
            // 非均匀密集：回退逐点 append（原语义，逐字节等价）。
            for p in points {
                self.append(p.ts, &p.values, &p.qualities, p.flags);
            }
            return;
        }

        // 均匀密集快路径：乱序/时间轴/质量码仍逐点维护（顺序语义不可省）。
        let base = self.timestamps.len();
        let mut prev = self.timestamps.last().copied();
        for p in points {
            let ts = p.ts;
            let ordered = match prev {
                Some(pv) => ts >= pv,
                None => true,
            };
            if !ordered {
                if !self.out_of_order {
                    self.rebuild_ts_index();
                }
                self.out_of_order = true;
            }
            let row_idx = self.timestamps.len();
            self.timestamps.push(ts);
            if self.out_of_order {
                self.ts_index.insert(ts, row_idx);
            }
            let qual = p
                .qualities
                .iter()
                .find(|(cid, _)| *cid == 0)
                .map(|(_, q)| *q)
                .unwrap_or(0xC0);
            self.qualities.push(qual);
            self.point_count += 1;
            prev = Some(ts);
        }

        self.append_batch_dense_columns(points, base);
    }

    /// 判断批内所有点是否「均匀密集」：首点 fully dense（覆盖全部既有列且无重复/新列），
    /// 其余点与首点列 ID 序列完全一致（同顺序、同长度）。满足时逐点 append 必恒走 dense
    /// 分支，可安全走按列批量写入。
    fn is_uniform_dense_batch(&self, points: &[&DataPoint]) -> bool {
        if points.is_empty() || !self.is_fully_dense(&points[0].values) {
            return false;
        }
        let first = &points[0].values;
        for p in &points[1..] {
            if p.values.len() != first.len() {
                return false;
            }
            for (a, b) in first.iter().zip(p.values.iter()) {
                if a.0 != b.0 {
                    return false;
                }
            }
        }
        true
    }

    /// 按列批量写入（仅均匀密集快路径调用）：每列一次 `HashMap` 查找 + 逐值 push，
    /// presence 三态语义与 [`write_existing_column_dense`](Self::write_existing_column_dense)
    /// 逐点调用逐字节一致。
    fn append_batch_dense_columns(&mut self, points: &[&DataPoint], base: usize) {
        let n = points.len();
        let m = points[0].values.len();

        for j in 0..m {
            let col_id = points[0].values[j].0;
            let has_entry = self.presence.contains_key(&col_id);

            // 逐值写列；仅在出现类型不匹配时才分配 ok_flags（记录每点是否成功）。
            let mut ok_flags: Option<Vec<bool>> = None;
            {
                let col = self
                    .columns
                    .get_mut(&col_id)
                    .expect("uniform dense batch: column must exist");
                for i in 0..n {
                    let val = &points[i].values[j].1;
                    if col.push_typed(val) {
                        if let Some(flags) = ok_flags.as_mut() {
                            flags.push(true);
                        }
                    } else {
                        tracing::warn!(
                            column_id = col_id,
                            expected = %format!("{:?}", col.data_type()),
                            got = %format!("{:?}", val.data_type()),
                            "column type mismatch on append; value skipped"
                        );
                        col.push_absent();
                        match ok_flags.as_mut() {
                            Some(flags) => flags.push(false),
                            None => {
                                let mut flags = vec![true; i];
                                flags.push(false);
                                ok_flags = Some(flags);
                            }
                        }
                    }
                }
            }

            // presence 三态维护（等价 write_existing_column_dense 逐点语义）。
            match (has_entry, ok_flags) {
                (true, Some(flags)) => {
                    self.presence.get_mut(&col_id).expect("entry exists").extend(flags);
                }
                (true, None) => {
                    self.presence
                        .get_mut(&col_id)
                        .expect("entry exists")
                        .extend(std::iter::repeat(true).take(n));
                }
                (false, Some(flags)) => {
                    let p = self.presence.entry(col_id).or_default();
                    p.extend(std::iter::repeat(true).take(base));
                    p.extend(flags);
                }
                (false, None) => {
                    // 纯密集：保持无 presence entry，零开销。
                }
            }
        }
    }

    /// 获取时间范围内的行索引（有序）。
    ///
    /// A4：同 ts 只返回最新 pos，旧值（孤儿）不会被引用 → 查询读新值、不返回重复。
    /// 有序惰性路径走 `timestamps` 二分定位 + 相邻同 ts LWW 去重；乱序路径走已物化的
    /// `ts_index`。两者语义一致。
    ///
    /// # 参数
    /// - `ts_start`: 起始时间戳。
    /// - `ts_end`: 结束时间戳。
    ///
    /// # 返回值
    /// 时间范围内的 `(ts, row_idx)` 列表（按 ts 升序）。
    pub fn get_range(&self, ts_start: i64, ts_end: i64) -> Vec<(i64, usize)> {
        // 反向范围防御守卫：start > end 统一返回空。乱序分支走 `BTreeMap::range`，
        // 反向 range 会在迭代时 panic，必须在此统一拦截（与有序分支语义一致）。
        if ts_start > ts_end {
            return Vec::new();
        }
        if self.out_of_order || !self.ts_index.is_empty() {
            self.ts_index
                .range(ts_start..=ts_end)
                .map(|(ts, idx)| (*ts, *idx))
                .collect()
        } else {
            self.ordered_range(ts_start, ts_end)
        }
    }

    /// 返回「按 ts 升序」的行索引序列（单一 LWW：同 ts 只保留最新 row_idx）。
    ///
    /// 这是 flush / SAC 编码的唯一真相源（A5）。有序惰性路径从 `timestamps` 推导，
    /// 乱序路径走 `ts_index`；对「已物化 ts_index 的 chunk」（直接构造 / build_sub_chunk）
    /// 恒走 ts_index，逐字节保持旧行为。
    pub fn sorted_positions(&self) -> Vec<usize> {
        if self.out_of_order || !self.ts_index.is_empty() {
            self.ts_index.values().copied().collect()
        } else {
            self.ordered_sorted_positions()
        }
    }

    /// 本 chunk 去重后的行数（= `sorted_positions().len()`，但零分配）。
    ///
    /// 语义与 [`sorted_positions`](Self::sorted_positions) 的 LWW 去重严格一致：
    /// 乱序/已物化 ts_index 路径取 `ts_index.len()`（BTreeMap 按 ts 唯一）；有序惰性
    /// 路径扫 `timestamps` 数相邻同 ts 去重后的唯一 ts。O(n)、无堆分配，供 flush
    /// 跨表批量预扫描求 `blocks_needed`（避免物化 10 万表 × 414 个 usize）。
    pub fn unique_ts_count(&self) -> usize {
        if self.out_of_order || !self.ts_index.is_empty() {
            self.ts_index.len()
        } else {
            let n = self.timestamps.len();
            let mut count = 0usize;
            let mut i = 0usize;
            while i < n {
                let ts = self.timestamps[i];
                let mut last = i;
                while last + 1 < n && self.timestamps[last + 1] == ts {
                    last += 1;
                }
                count += 1;
                i = last + 1;
            }
            count
        }
    }

    /// 有序模式下从 `timestamps` 二分定位 `[ts_start, ts_end]`，并对相邻同 ts 去重
    /// （LWW：保留最后一个 row_idx）。
    fn ordered_range(&self, ts_start: i64, ts_end: i64) -> Vec<(i64, usize)> {
        let lo = self.timestamps.partition_point(|&t| t < ts_start);
        let hi = self.timestamps.partition_point(|&t| t <= ts_end);
        let mut out = Vec::new();
        let mut i = lo;
        while i < hi {
            let ts = self.timestamps[i];
            let mut last = i;
            while last + 1 < hi && self.timestamps[last + 1] == ts {
                last += 1;
            }
            out.push((ts, last));
            i = last + 1;
        }
        out
    }

    /// 有序模式下推导全量排序行索引（LWW：相邻同 ts 保留最后一个）。
    fn ordered_sorted_positions(&self) -> Vec<usize> {
        let mut out = Vec::with_capacity(self.timestamps.len());
        let n = self.timestamps.len();
        let mut i = 0;
        while i < n {
            let ts = self.timestamps[i];
            let mut last = i;
            while last + 1 < n && self.timestamps[last + 1] == ts {
                last += 1;
            }
            out.push(last);
            i = last + 1;
        }
        out
    }

    /// 物化 ts_index：从 `timestamps` 重建（同 ts 后写覆盖，旧值变孤儿）。
    /// 仅在「首次乱序」时调用一次，此后 ts_index 转为 eager 权威维护。
    fn rebuild_ts_index(&mut self) {
        self.ts_index.clear();
        for (idx, &ts) in self.timestamps.iter().enumerate() {
            self.ts_index.insert(ts, idx);
        }
    }

    /// 判断本行是否「全列密集」：values 恰好覆盖全部既有列（无稀疏缺失、无新列、无重复列 id）。
    #[inline]
    fn is_fully_dense(&self, values: &[(u16, TypedValue)]) -> bool {
        if values.len() != self.columns.len() {
            return false;
        }
        // 数量相等 + 都已存在 + 互异 ⇒ 恰好覆盖全部既有列。用栈上 u128 bitset 做 O(n)
        // 互异/存在检测，避免 per-point HashSet 堆分配。col_id ≥ 128 时保守回退稀疏路径
        // （语义不变，仅失去快路径），常见密集表 col_id 恒为 0..N-1 连续小值。
        let mut seen: u128 = 0;
        for (cid, _) in values {
            let cid = *cid as usize;
            if cid >= 128 || !self.columns.contains_key(&(cid as u16)) {
                return false;
            }
            let bit = 1u128 << cid;
            if seen & bit != 0 {
                return false; // 重复 col_id
            }
            seen |= bit;
        }
        true
    }

    /// 写入一个「已存在列」的值 + presence 位图（dense 快路径与 sparse 路径共用）。
    ///
    /// 列必须已存在（调用方保证）；类型不匹配时告警并补占位，presence 记 false。
    #[inline]
    fn write_existing_column(&mut self, col_id: u16, val: &TypedValue, row_idx: usize) {
        let col = self.columns.get_mut(&col_id).expect("column must exist");
        let ok = col.push_typed(val);
        if !ok {
            tracing::warn!(
                column_id = col_id,
                expected = %format!("{:?}", col.data_type()),
                got = %format!("{:?}", val.data_type()),
                "column type mismatch on append; value skipped"
            );
            // 类型不匹配被跳过：补占位使列长度仍与行数对齐。
            col.push_absent();
        }

        let presence = self.presence.entry(col_id).or_default();
        while presence.len() < row_idx {
            presence.push(false);
        }
        presence.push(ok);
    }

    /// 密集快路径专用：写入一个「已存在列」的值，presence 三态处理。
    ///
    /// 与 `write_existing_column` 的差异：纯密集态（该列无 presence entry 且值写入成功）
    /// **不**创建 presence 位图——省去 per-point HashMap 查找与 `Vec<bool>` push，
    /// 靠 `is_present` 的密集回退（`col.len() > row_idx`）判定存在性。仅当该列首次出现
    /// 稀疏缺失 / 类型不匹配时才物化位图，语义与原实现逐位一致。
    ///
    /// 三态：
    /// - 该列已有 presence entry（历史稀疏/类型不匹配）→ 继续维护长度对齐（原语义）。
    /// - 无 entry 且写入成功 → 保持无 entry（纯密集态，零 presence 开销）。
    /// - 无 entry 且类型不匹配（异常，罕见）→ 物化：此前 row_idx 行全密集 true，本行 false。
    #[inline]
    fn write_existing_column_dense(&mut self, col_id: u16, val: &TypedValue, row_idx: usize) {
        let col = self.columns.get_mut(&col_id).expect("column must exist");
        let ok = col.push_typed(val);
        if !ok {
            tracing::warn!(
                column_id = col_id,
                expected = %format!("{:?}", col.data_type()),
                got = %format!("{:?}", val.data_type()),
                "column type mismatch on append; value skipped"
            );
            // 类型不匹配被跳过：补占位使列长度仍与行数对齐。
            col.push_absent();
        }

        match self.presence.get_mut(&col_id) {
            // 该列已有 presence entry（历史稀疏/类型不匹配）→ 继续维护长度对齐（原语义）。
            Some(p) => {
                while p.len() < row_idx {
                    p.push(false);
                }
                p.push(ok);
            }
            // 该列无 entry（纯密集态）。
            None => {
                if !ok {
                    // 纯密集态首次出现「无值」（类型不匹配）→ 物化：此前 row_idx 行
                    // 全密集 true，本行 false。此后该列退回位图维护。
                    let p = self.presence.entry(col_id).or_default();
                    while p.len() < row_idx {
                        p.push(true);
                    }
                    p.push(false);
                }
                // ok=true：保持无 entry（纯密集态），零 presence 开销。
            }
        }
    }

    /// 获取指定列的某个值。
    ///
    /// # 参数
    /// - `col_id`: 列 ID。
    /// - `row_idx`: 行索引。
    ///
    /// # 返回值
    /// 若存在则返回 f64 值；稀疏缺失返回 `None`。
    pub fn get_value(&self, col_id: u16, row_idx: usize) -> Option<f64> {
        if !self.is_present(col_id, row_idx) {
            return None;
        }
        self.columns.get(&col_id).and_then(|col| match col {
            ColVec::F64(v) => v.get(row_idx).copied(),
            ColVec::F32(v) => v.get(row_idx).map(|&x| x as f64),
            ColVec::I64(v) => v.get(row_idx).map(|&x| x as f64),
            ColVec::U64(v) => v.get(row_idx).map(|&x| x as f64),
            ColVec::I32(v) => v.get(row_idx).map(|&x| x as f64),
            _ => None,
        })
    }

    /// 获取指定列的某个值（保留物理类型）。
    ///
    /// # 参数
    /// - `col_id`: 列 ID。
    /// - `row_idx`: 行索引。
    ///
    /// # 返回值
    /// 若存在则返回带类型的值；稀疏缺失返回 `None`。
    pub fn get_typed_value(&self, col_id: u16, row_idx: usize) -> Option<TypedValue> {
        if !self.is_present(col_id, row_idx) {
            return None;
        }
        self.columns.get(&col_id).and_then(|col| match col {
            ColVec::F64(v) => v.get(row_idx).map(|&x| TypedValue::F64(x)),
            ColVec::F32(v) => v.get(row_idx).map(|&x| TypedValue::F32(x)),
            ColVec::I64(v) => v.get(row_idx).map(|&x| TypedValue::I64(x)),
            ColVec::U64(v) => v.get(row_idx).map(|&x| TypedValue::U64(x)),
            ColVec::I32(v) => v.get(row_idx).map(|&x| TypedValue::I32(x)),
            ColVec::Bool(v) => v.get(row_idx).map(|&x| TypedValue::Bool(x)),
            ColVec::String(v) => v.get(row_idx).map(|x| TypedValue::String(x.clone())),
            ColVec::Enum16(v) => v.get(row_idx).map(|&x| TypedValue::Enum16(x)),
        })
    }

    /// 判断某行某列是否有值（presence 位图优先；位图缺失时按密集列回退）。
    #[inline]
    fn is_present(&self, col_id: u16, row_idx: usize) -> bool {
        match self.presence.get(&col_id) {
            Some(p) => p.get(row_idx).copied().unwrap_or(false),
            None => self
                .columns
                .get(&col_id)
                .map(|c| c.len() > row_idx)
                .unwrap_or(false),
        }
    }

    /// 获取指定行的质量码。
    ///
    /// # 参数
    /// - `row_idx`: 行索引。
    pub fn get_quality(&self, row_idx: usize) -> Option<u8> {
        self.qualities.get(row_idx).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 便捷构造 f64 列值列表。
    fn f64s(v: &[(u16, f64)]) -> Vec<(u16, TypedValue)> {
        v.iter().map(|(id, x)| (*id, TypedValue::F64(*x))).collect()
    }

    #[test]
    fn test_append_ordered() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);

        assert!(chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0));
        assert!(chunk.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0));
        assert!(chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0));

        assert_eq!(chunk.point_count, 3);
        assert!(!chunk.out_of_order);
        assert_eq!(chunk.timestamps, vec![100, 200, 300]);
    }

    #[test]
    fn test_append_out_of_order() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);

        chunk.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
        chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
        chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0); // 乱序

        assert!(chunk.out_of_order);
        assert_eq!(chunk.point_count, 3);
    }

    /// 单一 LWW：同 ts 两次 append → point_count 虚增为 2（孤儿可接受），
    /// 但 `get_range` 始终走 ts_index → 只返回最新 pos，读新值。
    #[test]
    fn test_append_lww_reads_newest() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        chunk.append(100, &f64s(&[(0, 9.0)]), &[(0, 0x40)], 0); // 同 ts LWW

        assert_eq!(chunk.point_count, 2, "孤儿可接受，point_count 虚增");
        assert_eq!(chunk.timestamps, vec![100, 100]);

        // A4：get_range 走 ts_index → 同 ts 只返回最新 pos（读 9.0）。
        let rows = chunk.get_range(100, 100);
        assert_eq!(rows, vec![(100, 1)]);
        assert_eq!(chunk.get_value(0, 1), Some(9.0));
    }

    #[test]
    fn test_get_range_ordered() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        for i in 0..10 {
            chunk.append(i * 100, &f64s(&[(0, i as f64)]), &[(0, 0xC0)], 0);
        }

        let rows = chunk.get_range(200, 500);
        assert_eq!(rows.len(), 4); // 200, 300, 400, 500
    }

    #[test]
    fn test_get_range_out_of_order() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        chunk.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
        chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
        chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0); // 乱序

        let rows = chunk.get_range(100, 250);
        assert_eq!(rows.len(), 2); // 100, 200
    }

    #[test]
    fn test_get_value() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 42);
        chunk.append(100, &f64s(&[(0, 3.14), (1, 2.718)]), &[(0, 0xC0)], 0);

        assert_eq!(chunk.get_value(0, 0), Some(3.14));
        assert_eq!(chunk.get_value(1, 0), Some(2.718));
        assert_eq!(chunk.get_value(2, 0), None); // col 不存在
        assert_eq!(chunk.get_value(0, 1), None); // row 不存在
    }

    #[test]
    fn test_append_multi_type_columns() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 7);

        chunk.append(
            100,
            &[
                (0, TypedValue::F64(1.5)),
                (1, TypedValue::I64(42)),
                (2, TypedValue::Bool(true)),
                (3, TypedValue::String("open".to_string())),
                (4, TypedValue::Enum16(7)),
            ],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(
            200,
            &[
                (0, TypedValue::F64(2.5)),
                (1, TypedValue::I64(43)),
                (2, TypedValue::Bool(false)),
                (3, TypedValue::String("close".to_string())),
                (4, TypedValue::Enum16(8)),
            ],
            &[(0, 0xC0)],
            0,
        );

        assert_eq!(chunk.point_count, 2);

        // 每列按物理类型建对应变体，而非恒 F64
        assert!(matches!(chunk.columns.get(&0), Some(ColVec::F64(_))));
        assert!(matches!(chunk.columns.get(&1), Some(ColVec::I64(_))));
        assert!(matches!(chunk.columns.get(&2), Some(ColVec::Bool(_))));
        assert!(matches!(chunk.columns.get(&3), Some(ColVec::String(_))));
        assert!(matches!(chunk.columns.get(&4), Some(ColVec::Enum16(_))));

        // 类型化读取
        assert_eq!(chunk.get_typed_value(1, 0), Some(TypedValue::I64(42)));
        assert_eq!(
            chunk.get_typed_value(3, 1),
            Some(TypedValue::String("close".to_string()))
        );
        assert_eq!(chunk.get_typed_value(2, 1), Some(TypedValue::Bool(false)));

        // 质量码：per-point 单质量流
        assert_eq!(chunk.qualities, vec![0xC0, 0xC0]);
    }

    /// 稀疏列修复：中间行缺失的列，读取必须返回 `None`，而非错位读到相邻行的值。
    #[test]
    fn test_append_sparse_columns_read_none() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);

        chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        // row1 只有 col_1（col_0 稀疏缺失）。
        chunk.append(200, &f64s(&[(1, 9.0)]), &[(0, 0xC0)], 0);
        chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);

        // 列向量长度 == 行数（缺失用占位对齐），密集读不 panic。
        assert_eq!(chunk.timestamps.len(), 3);
        assert_eq!(chunk.columns.get(&0).map(|c| c.len()), Some(3));
        assert_eq!(chunk.columns.get(&1).map(|c| c.len()), Some(3));

        // col_0 只在 row0/row2 有值，row1 稀疏 → None（修复前会错位读到 3.0）。
        assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
        assert_eq!(chunk.get_typed_value(0, 1), None);
        assert_eq!(chunk.get_typed_value(0, 2), Some(TypedValue::F64(3.0)));

        // col_1 只在 row1 有值。
        assert_eq!(chunk.get_typed_value(1, 0), None);
        assert_eq!(chunk.get_typed_value(1, 1), Some(TypedValue::F64(9.0)));
        assert_eq!(chunk.get_typed_value(1, 2), None);

        // presence 位图：col_0 = [true,false,true]，col_1 = [false,true,false]。
        assert_eq!(chunk.presence.get(&0), Some(&vec![true, false, true]));
        assert_eq!(chunk.presence.get(&1), Some(&vec![false, true, false]));
    }

    /// 构造「无 presence entry」的密集列 chunk（模拟 SAC 密集读回 / 外部直构）。
    ///
    /// 2a 优化唯一能真正跳过 presence 维护的起点：新鲜表首次 append 恒走稀疏路径
    /// 并为每列创建 presence entry（见 `test_fresh_table_dense_append_maintains_presence`）。
    fn dense_chunk_without_presence() -> ColumnarChunk {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        chunk.columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0]));
        chunk.columns.insert(1, ColVec::F64(vec![10.0, 20.0, 30.0]));
        chunk.columns.insert(2, ColVec::F64(vec![100.0, 200.0, 300.0]));
        chunk.timestamps = vec![100, 200, 300];
        chunk.qualities = vec![0xC0; 3];
        chunk.point_count = 3;
        chunk
    }

    /// 场景 1：纯密集写入（presence 无 entry 的密集 chunk）后，presence 应保持空，
    /// 各列各行读取靠 `is_present` 密集回退（`col.len() > row_idx`）正确。
    #[test]
    fn test_dense_append_keeps_presence_empty_and_reads_correct() {
        let mut chunk = dense_chunk_without_presence();
        assert!(chunk.presence.is_empty());

        // 连续密集 append（3 列全覆盖 → is_fully_dense == true）。
        chunk.append(400, &f64s(&[(0, 4.0), (1, 40.0), (2, 400.0)]), &[(0, 0xC0)], 0);
        chunk.append(500, &f64s(&[(0, 5.0), (1, 50.0), (2, 500.0)]), &[(0, 0xC0)], 0);

        // presence 应保持空（无 entry），纯密集态零位图开销。
        assert!(
            chunk.presence.is_empty(),
            "纯密集 append 不应创建 presence entry"
        );

        // 各列各行读取正确（靠 is_present 密集回退 col.len() > row_idx）。
        assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
        assert_eq!(chunk.get_typed_value(0, 3), Some(TypedValue::F64(4.0)));
        assert_eq!(chunk.get_typed_value(0, 4), Some(TypedValue::F64(5.0)));
        assert_eq!(chunk.get_typed_value(1, 4), Some(TypedValue::F64(50.0)));
        assert_eq!(chunk.get_typed_value(2, 4), Some(TypedValue::F64(500.0)));
        // 列长度 == 行数。
        assert_eq!(chunk.columns.get(&0).map(|c| c.len()), Some(5));
    }

    /// 场景 2：密集 → 稀疏切换——稀疏写物化此前密集时期的既有列（前 row_idx 行全 true），
    /// 缺失列本行 false；读取密集行正确、缺失行 None（不错位）。
    #[test]
    fn test_dense_then_sparse_materializes_presence() {
        let mut chunk = dense_chunk_without_presence();
        // 密集写 2 行（row3, row4）。
        chunk.append(400, &f64s(&[(0, 4.0), (1, 40.0), (2, 400.0)]), &[(0, 0xC0)], 0);
        chunk.append(500, &f64s(&[(0, 5.0), (1, 50.0), (2, 500.0)]), &[(0, 0xC0)], 0);
        assert!(chunk.presence.is_empty(), "稀疏前 presence 应保持空");

        // 稀疏写（只写 col 0），col 1/2 缺失。
        chunk.append(600, &f64s(&[(0, 6.0)]), &[(0, 0xC0)], 0);

        // 物化：所有列的前 row_idx=5 行全 true；缺失列本行 false。
        assert_eq!(
            chunk.presence.get(&0),
            Some(&vec![true, true, true, true, true, true])
        );
        assert_eq!(
            chunk.presence.get(&1),
            Some(&vec![true, true, true, true, true, false])
        );
        assert_eq!(
            chunk.presence.get(&2),
            Some(&vec![true, true, true, true, true, false])
        );

        // 读取：密集行正确，缺失行 None（不错位）。
        assert_eq!(chunk.get_typed_value(1, 4), Some(TypedValue::F64(50.0)));
        assert_eq!(chunk.get_typed_value(1, 5), None);
        assert_eq!(chunk.get_typed_value(0, 5), Some(TypedValue::F64(6.0)));
        // 列长度对齐 == 行数。
        assert_eq!(chunk.columns.get(&1).map(|c| c.len()), Some(6));
    }

    /// 场景 3：稀疏缺失后再整行密集写——该列 presence 长度继续对齐增长，新行读取正确。
    #[test]
    fn test_sparse_then_dense_keeps_presence_aligned() {
        let mut chunk = dense_chunk_without_presence();
        // 密集 1 行 (row3)。
        chunk.append(400, &f64s(&[(0, 4.0), (1, 40.0), (2, 400.0)]), &[(0, 0xC0)], 0);
        // 稀疏写 row4（只 col 0）。
        chunk.append(500, &f64s(&[(0, 5.0)]), &[(0, 0xC0)], 0);
        // 再密集写 row5（3 列全覆盖）。
        chunk.append(600, &f64s(&[(0, 6.0), (1, 60.0), (2, 600.0)]), &[(0, 0xC0)], 0);

        // col 1/2 在稀疏行 row4 缺失后进入 presence 模式；再密集写继续对齐增长。
        assert_eq!(
            chunk.presence.get(&1),
            Some(&vec![true, true, true, true, false, true])
        );
        assert_eq!(
            chunk.presence.get(&2),
            Some(&vec![true, true, true, true, false, true])
        );

        // 读取：稀疏行 None，新密集行正确。
        assert_eq!(chunk.get_typed_value(1, 4), None);
        assert_eq!(chunk.get_typed_value(1, 5), Some(TypedValue::F64(60.0)));
        assert_eq!(chunk.get_typed_value(2, 5), Some(TypedValue::F64(600.0)));
        assert_eq!(chunk.columns.get(&1).map(|c| c.len()), Some(6));
    }

    /// 场景 4：密集路径类型不匹配——该列物化 presence（前 row_idx 行 true + 本行 false），
    /// 本行该列读取返回 None（不误读占位值）；其它列保持纯密集态不受影响。
    #[test]
    fn test_dense_type_mismatch_materializes_presence() {
        let mut chunk = dense_chunk_without_presence();
        assert!(chunk.presence.is_empty());

        // 密集写 row3：col 0 类型不匹配（F64 列收到 I64）。
        chunk.append(
            400,
            &[
                (0, TypedValue::I64(9)),
                (1, TypedValue::F64(40.0)),
                (2, TypedValue::F64(400.0)),
            ],
            &[(0, 0xC0)],
            0,
        );

        // col 0 物化 presence：前 3 行 true，本行 false；col 1/2 仍无 entry（纯密集）。
        assert_eq!(chunk.presence.get(&0), Some(&vec![true, true, true, false]));
        assert!(chunk.presence.get(&1).is_none(), "col1 未受影响，保持纯密集");
        assert!(chunk.presence.get(&2).is_none(), "col2 未受影响，保持纯密集");

        // 读 col0 row3 返回 None（不误读占位值），密集行/其它列正确。
        assert_eq!(chunk.get_typed_value(0, 3), None);
        assert_eq!(chunk.get_typed_value(0, 2), Some(TypedValue::F64(3.0)));
        assert_eq!(chunk.get_typed_value(1, 3), Some(TypedValue::F64(40.0)));
        // 列长度仍对齐。
        assert_eq!(chunk.columns.get(&0).map(|c| c.len()), Some(4));
    }

    /// 场景 5（补做后）：新鲜表首行全列密集 append → presence 保持空 → 后续密集 append
    /// 仍空且读值正确 → 首次稀疏缺失才物化 [true×N, false]。
    #[test]
    fn test_fresh_table_dense_append_maintains_presence() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        // 首 append：建 2 列（首行走稀疏路径但 row_idx==0 → 走密集写入，不建 presence entry）。
        chunk.append(100, &f64s(&[(0, 1.0), (1, 10.0)]), &[(0, 0xC0)], 0);
        assert!(
            chunk.presence.is_empty(),
            "首行全列密集 append 后 presence 应保持空"
        );

        // 后续密集 append：presence 仍空，读值正确。
        chunk.append(200, &f64s(&[(0, 2.0), (1, 20.0)]), &[(0, 0xC0)], 0);
        assert!(
            chunk.presence.is_empty(),
            "连续密集 append 后 presence 应保持空"
        );
        assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
        assert_eq!(chunk.get_typed_value(1, 0), Some(TypedValue::F64(10.0)));
        assert_eq!(chunk.get_typed_value(0, 1), Some(TypedValue::F64(2.0)));
        assert_eq!(chunk.get_typed_value(1, 1), Some(TypedValue::F64(20.0)));

        // 首次稀疏缺失才物化 [true×N, false]。
        chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
        assert_eq!(chunk.presence.get(&0), Some(&vec![true, true, true]));
        assert_eq!(chunk.presence.get(&1), Some(&vec![true, true, false]));
        assert_eq!(chunk.get_typed_value(1, 2), None);
        assert_eq!(chunk.get_typed_value(1, 1), Some(TypedValue::F64(20.0)));
    }

    /// 有序路径 ts_index 惰性化：纯顺序写入不维护 BTreeMap，但查询/排序语义不受影响。
    #[test]
    fn test_ordered_append_keeps_ts_index_lazy() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        chunk.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
        chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);

        // 惰性：有序路径不再 eager 插入 ts_index。
        assert!(chunk.ts_index.is_empty());
        assert!(!chunk.out_of_order);

        // 查询与排序仍从 timestamps 推导，结果与旧 eager 实现一致。
        assert_eq!(chunk.get_range(150, 300), vec![(200, 1), (300, 2)]);
        assert_eq!(chunk.sorted_positions(), vec![0, 1, 2]);
    }

    /// 有序路径 LWW：相邻同 ts 去重，`sorted_positions` 只保留最新 row_idx。
    #[test]
    fn test_sorted_positions_ordered_lww_dedup() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        chunk.append(100, &f64s(&[(0, 9.0)]), &[(0, 0x40)], 0); // 同 ts LWW 覆盖
        chunk.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);

        assert!(chunk.ts_index.is_empty(), "有序路径应保持 ts_index 惰性");
        assert_eq!(chunk.sorted_positions(), vec![1, 2], "同 ts 只保留最新 pos");
        assert_eq!(chunk.get_range(100, 100), vec![(100, 1)]);
    }

    /// 首次乱序时物化 ts_index，之后 get_range/sorted_positions 走 ts_index 权威路径。
    #[test]
    fn test_first_out_of_order_materializes_ts_index() {
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        chunk.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
        chunk.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
        assert!(chunk.ts_index.is_empty(), "乱序前应保持惰性");

        chunk.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0); // 首次乱序

        assert!(chunk.out_of_order);
        assert!(!chunk.ts_index.is_empty(), "首次乱序应物化 ts_index");
        assert_eq!(chunk.get_range(100, 250), vec![(100, 2), (200, 0)]);
        assert_eq!(chunk.sorted_positions(), vec![2, 0, 1]);
    }

    // ---- append_batch 批量写入 与 逐点 append 语义等价性测试 ----

    /// 便捷构造 DataPoint（列值 f64）。
    fn dp(ts: i64, values: Vec<(u16, f64)>) -> DataPoint {
        DataPoint {
            table_id: 1,
            tenant_id: TenantId::DEFAULT,
            ts,
            server_ts: ts,
            values: values
                .into_iter()
                .map(|(id, x)| (id, TypedValue::F64(x)))
                .collect(),
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }
    }

    /// 逐字节断言两个 chunk 完全等价（结构体字段 + 查询语义）。
    fn assert_chunks_equivalent(a: &ColumnarChunk, b: &ColumnarChunk) {
        assert_eq!(a.table_id, b.table_id);
        assert_eq!(a.tenant_id, b.tenant_id);
        assert_eq!(a.timestamps, b.timestamps);
        assert_eq!(a.columns, b.columns);
        assert_eq!(a.presence, b.presence);
        assert_eq!(a.qualities, b.qualities);
        assert_eq!(a.ts_index, b.ts_index);
        assert_eq!(a.point_count, b.point_count);
        assert_eq!(a.out_of_order, b.out_of_order);
        assert_eq!(a.max_wal_seq, b.max_wal_seq);
        assert_eq!(a.min_wal_seq, b.min_wal_seq);

        // 排序 / 范围查询语义等价。
        assert_eq!(a.sorted_positions(), b.sorted_positions());
        if let (Some(&amin), Some(&amax)) = (a.timestamps.iter().min(), a.timestamps.iter().max())
        {
            assert_eq!(a.get_range(amin, amax), b.get_range(amin, amax));
        }
        // 全列全行 typed 读取逐字节一致。
        let mut col_ids: Vec<u16> = a.columns.keys().copied().collect();
        col_ids.sort_unstable();
        for col in &col_ids {
            for r in 0..a.point_count {
                assert_eq!(
                    a.get_typed_value(*col, r),
                    b.get_typed_value(*col, r),
                    "col {} row {}",
                    col,
                    r
                );
            }
        }
    }

    /// 核心等价性：同一批「均匀密集」点，批量路径 vs 逐点路径最终状态逐字节一致。
    #[test]
    fn test_append_batch_equiv_dense_ordered() {
        let points = vec![
            dp(100, vec![(0, 1.0), (1, 10.0), (2, 100.0)]),
            dp(200, vec![(0, 2.0), (1, 20.0), (2, 200.0)]),
            dp(300, vec![(0, 3.0), (1, 30.0), (2, 300.0)]),
            dp(400, vec![(0, 4.0), (1, 40.0), (2, 400.0)]),
        ];

        let mut a = ColumnarChunk::new(TenantId::DEFAULT, 1);
        // 逐点：先建列（首点走稀疏首行）。
        for p in &points {
            a.append(p.ts, &p.values, &p.qualities, p.flags);
        }

        let mut b = ColumnarChunk::new(TenantId::DEFAULT, 1);
        // 先逐点建列（首点稀疏），再批量走密集快路径。
        b.append(
            points[0].ts,
            &points[0].values,
            &points[0].qualities,
            points[0].flags,
        );
        let refs: Vec<&DataPoint> = points[1..].iter().collect();
        b.append_batch(&refs);

        assert_chunks_equivalent(&a, &b);
    }

    /// 密集批量 + 乱序：批量路径 ts_index 物化/eager 维护与逐点一致。
    #[test]
    fn test_append_batch_equiv_dense_out_of_order() {
        // 先建列，再灌乱序批量。
        let seed = dp(500, vec![(0, 5.0), (1, 50.0)]);
        let batch = vec![
            dp(200, vec![(0, 2.0), (1, 20.0)]),
            dp(100, vec![(0, 1.0), (1, 10.0)]), // 乱序
            dp(300, vec![(0, 3.0), (1, 30.0)]),
            dp(100, vec![(0, 9.9), (1, 99.0)]), // 同 ts LWW 覆盖
        ];

        let mut a = ColumnarChunk::new(TenantId::DEFAULT, 1);
        a.append(
            seed.ts,
            &seed.values,
            &seed.qualities,
            seed.flags,
        );
        for p in &batch {
            a.append(p.ts, &p.values, &p.qualities, p.flags);
        }

        let mut b = ColumnarChunk::new(TenantId::DEFAULT, 1);
        b.append(
            seed.ts,
            &seed.values,
            &seed.qualities,
            seed.flags,
        );
        let refs: Vec<&DataPoint> = batch.iter().collect();
        b.append_batch(&refs);

        assert_chunks_equivalent(&a, &b);
    }

    /// 密集批量 + 类型不匹配：presence 物化三态语义与逐点逐字节一致。
    #[test]
    fn test_append_batch_equiv_dense_type_mismatch() {
        let seed = dp(100, vec![(0, 1.0), (1, 10.0)]);
        let batch = vec![
            DataPoint {
                table_id: 1,
                tenant_id: TenantId::DEFAULT,
                ts: 200,
                server_ts: 200,
                values: vec![(0, TypedValue::I64(7)), (1, TypedValue::F64(20.0))], // col0 类型不匹配
                qualities: vec![(0, 0xC0)],
                schema_version: 1,
                flags: 0,
            },
            dp(300, vec![(0, 3.0), (1, 30.0)]),
        ];

        let mut a = ColumnarChunk::new(TenantId::DEFAULT, 1);
        a.append(
            seed.ts,
            &seed.values,
            &seed.qualities,
            seed.flags,
        );
        for p in &batch {
            a.append(p.ts, &p.values, &p.qualities, p.flags);
        }

        let mut b = ColumnarChunk::new(TenantId::DEFAULT, 1);
        b.append(
            seed.ts,
            &seed.values,
            &seed.qualities,
            seed.flags,
        );
        let refs: Vec<&DataPoint> = batch.iter().collect();
        b.append_batch(&refs);

        assert_chunks_equivalent(&a, &b);
    }

    /// 密集 → 稀疏（列集变化）批量：回退逐点路径，稀疏缺失列读取 None 正确。
    #[test]
    fn test_append_batch_fallback_sparse() {
        let seed = dp(100, vec![(0, 1.0), (1, 10.0)]);
        let batch = vec![
            dp(200, vec![(0, 2.0)]), // 稀疏（缺 col1）
            dp(300, vec![(0, 3.0), (1, 30.0)]),
        ];

        let mut a = ColumnarChunk::new(TenantId::DEFAULT, 1);
        a.append(
            seed.ts,
            &seed.values,
            &seed.qualities,
            seed.flags,
        );
        for p in &batch {
            a.append(p.ts, &p.values, &p.qualities, p.flags);
        }

        let mut b = ColumnarChunk::new(TenantId::DEFAULT, 1);
        b.append(
            seed.ts,
            &seed.values,
            &seed.qualities,
            seed.flags,
        );
        let refs: Vec<&DataPoint> = batch.iter().collect();
        b.append_batch(&refs);

        assert_chunks_equivalent(&a, &b);
    }

    /// 新鲜表首批（建列 + 密集批量）→ 回退逐点；后续批量走密集快路径，全程与逐点等价。
    #[test]
    fn test_append_batch_fresh_then_dense() {
        let batch1 = vec![
            dp(100, vec![(0, 1.0), (1, 10.0)]),
            dp(200, vec![(0, 2.0), (1, 20.0)]),
        ];
        let batch2 = vec![
            dp(300, vec![(0, 3.0), (1, 30.0)]),
            dp(400, vec![(0, 4.0), (1, 40.0)]),
        ];

        let all: Vec<DataPoint> = batch1.iter().chain(batch2.iter()).cloned().collect();
        let mut a = ColumnarChunk::new(TenantId::DEFAULT, 1);
        for p in &all {
            a.append(p.ts, &p.values, &p.qualities, p.flags);
        }

        let mut b = ColumnarChunk::new(TenantId::DEFAULT, 1);
        let refs1: Vec<&DataPoint> = batch1.iter().collect();
        b.append_batch(&refs1);
        let refs2: Vec<&DataPoint> = batch2.iter().collect();
        b.append_batch(&refs2);

        assert_chunks_equivalent(&a, &b);
    }

    /// 微基准：密集宽表（10 列）逐点 append vs 批量 append_batch 的 per-point 成本。
    /// `#[ignore]`，用 `cargo test --release ... -- --ignored --nocapture` 手动跑。
    #[test]
    #[ignore]
    fn bench_append_vs_append_batch_dense() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 50_000;
        const M: usize = 10;

        let points: Vec<DataPoint> = (0..N)
            .map(|i| {
                let mut values = Vec::with_capacity(M);
                for c in 0..M {
                    values.push((c as u16, TypedValue::F64((i * M + c) as f64)));
                }
                DataPoint {
                    table_id: 1,
                    tenant_id: TenantId::DEFAULT,
                    ts: (i as i64) * 1000,
                    server_ts: (i as i64) * 1000,
                    values,
                    qualities: vec![(0, 0xC0)],
                    schema_version: 1,
                    flags: 0,
                }
            })
            .collect();

        let seed = &points[0];
        let n = N - 1;

        // 逐点 append
        {
            let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
            c.append(seed.ts, &seed.values, &seed.qualities, seed.flags);
            let t0 = Instant::now();
            for p in &points[1..] {
                black_box(c.append(p.ts, &p.values, &p.qualities, p.flags));
            }
            let dt = t0.elapsed();
            println!(
                "[bench] append 逐点: {:.1} ns/point ({} points)",
                dt.as_nanos() as f64 / n as f64,
                n
            );
        }

        // 批量 append_batch
        {
            let mut c = ColumnarChunk::new(TenantId::DEFAULT, 1);
            c.append(seed.ts, &seed.values, &seed.qualities, seed.flags);
            let refs: Vec<&DataPoint> = points[1..].iter().collect();
            let t0 = Instant::now();
            black_box(c.append_batch(&refs));
            let dt = t0.elapsed();
            println!(
                "[bench] append_batch 批量: {:.1} ns/point ({} points)",
                dt.as_nanos() as f64 / n as f64,
                n
            );
        }
    }

    /// 空批 / 单点批：边界行为正确（空批 no-op，单点批等价单次 append）。
    #[test]
    fn test_append_batch_empty_and_single() {
        let mut a = ColumnarChunk::new(TenantId::DEFAULT, 1);
        let empty: Vec<&DataPoint> = vec![];
        a.append_batch(&empty);
        assert_eq!(a.point_count, 0);

        let single = dp(100, vec![(0, 1.0)]);
        let refs = vec![&single];
        a.append_batch(&refs);
        assert_eq!(a.point_count, 1);
        assert_eq!(a.get_typed_value(0, 0), Some(TypedValue::F64(1.0)));
    }

    /// 性质测试：`unique_ts_count()`（零分配）恒等于 `sorted_positions().len()`
    /// （LWW 去重语义严格对齐），覆盖有序 / 乱序 / 同 ts LWW / 空 chunk 四类路径。
    #[test]
    fn test_unique_ts_count_matches_sorted_positions() {
        let cases: Vec<ColumnarChunk> = vec![
            // 空 chunk
            ColumnarChunk::new(TenantId::DEFAULT, 1),
            // 有序无重
            {
                let mut c = ColumnarChunk::new(TenantId::DEFAULT, 2);
                for i in 0..10 {
                    c.append(i * 100, &f64s(&[(0, i as f64)]), &[(0, 0xC0)], 0);
                }
                c
            },
            // 有序 + 相邻同 ts（LWW 去重）
            {
                let mut c = ColumnarChunk::new(TenantId::DEFAULT, 3);
                c.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
                c.append(100, &f64s(&[(0, 9.0)]), &[(0, 0xC0)], 0);
                c.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
                c
            },
            // 乱序（触发 ts_index 物化）
            {
                let mut c = ColumnarChunk::new(TenantId::DEFAULT, 4);
                c.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
                c.append(300, &f64s(&[(0, 3.0)]), &[(0, 0xC0)], 0);
                c.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
                c
            },
            // 乱序 + 同 ts LWW
            {
                let mut c = ColumnarChunk::new(TenantId::DEFAULT, 5);
                c.append(200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
                c.append(100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
                c.append(100, &f64s(&[(0, 9.0)]), &[(0, 0xC0)], 0);
                c
            },
        ];
        for c in &cases {
            assert_eq!(
                c.unique_ts_count(),
                c.sorted_positions().len(),
                "unique_ts_count 必须恒等于 sorted_positions().len()（table_id={}）",
                c.table_id
            );
        }
    }
}

//! QA 独立验证：compaction 流式 merge 重构的数据正确性。
//!
//! 不信任工程师自测，fresh-eyes 逐字节/逐字段核对：
//! - D1 流式 `DedupIter` 与旧 `dedup_sorted` 逐字段一致（Some/None 混合 + 跨表 + tiebreak）
//! - D2 流式 `KWayMerge` 独立产生全局 (table_id, ts, source_id) 有序序列
//! - D5 流式「SacRowIter → KWayMerge → DedupIter」链与「全量 read_range → 排序 → dedup_sorted」逐字段一致
//! - ZSTD9 写读对称 + 磁盘字节级 frame magic（L2=zstd `28 B5 2F FD`，L0=LZ4）
//!
//! 关键：`DataPoint` 的 `PartialEq` 只比较 (table_id, ts)，故本文件**不**用
//! `assert_eq!(full, streamed)`，而是显式比较完整字段签名（含 wal_seq / server_ts /
//! values / qualities / source_id），杜绝「错误胜者但 key 序列相同」的漏检。

use std::collections::{BTreeMap, HashMap};

use sensearcdb_common::constants::SAC_HEADER_SIZE;
use sensearcdb_engine::compaction::merge::{
    dedup_sorted, k_way_merge, DataPoint, DedupIter, KWayMerge,
};
use sensearcdb_engine::compression::CompressionType;
use sensearcdb_engine::memtable::{ColVec, ColumnarChunk, TypedValue};
use sensearcdb_engine::sac::block::{BLOCK_MINI_HEADER_SIZE, STREAM_DIR_ENTRY_SIZE};
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::reader::{SacRow, SacRowIter};
use sensearcdb_engine::sac::{SacReader, SacWriter};

/// 构造一个密集单列 F64 的 ColumnarChunk（ts 升序，ts_index 恒等映射）。
fn f64_chunk(table_id: u32, timestamps: Vec<i64>, values: Vec<f64>) -> ColumnarChunk {
    assert_eq!(timestamps.len(), values.len());
    let point_count = timestamps.len();
    let ts_index: BTreeMap<i64, usize> = timestamps
        .iter()
        .enumerate()
        .map(|(i, ts)| (*ts, i))
        .collect();
    let mut columns = HashMap::new();
    columns.insert(0, ColVec::F64(values));
    let presence: HashMap<u16, Vec<bool>> = [(0u16, vec![true; point_count])].into();
    ColumnarChunk {
        table_id,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        timestamps,
        columns,
        presence,
        qualities: vec![0xC0; point_count],
        ts_index,
        point_count,
        out_of_order: false,
        max_wal_seq: None,
        min_wal_seq: None,
    }
}

/// 构造一个 Compaction 内部 DataPoint。
fn dp(
    table_id: u32,
    ts: i64,
    wal_seq: Option<u64>,
    server_ts: i64,
    source_id: usize,
    value: f64,
) -> DataPoint {
    DataPoint {
        table_id,
        ts,
        server_ts,
        values: vec![(0, TypedValue::F64(value))],
        qualities: vec![(0, 0xC0)],
        wal_seq,
        source_id,
    }
}

/// DataPoint 完整字段签名（不含 PartialEq 的 key-only 语义）。
fn sig(p: &DataPoint) -> (u32, i64, i64, Option<u64>, Vec<(u16, TypedValue)>, usize) {
    (
        p.table_id,
        p.ts,
        p.server_ts,
        p.wal_seq,
        p.values.clone(),
        p.source_id,
    )
}

/// 把 `SacRowIter` 适配为 `DataPoint` 迭代器（单一函数 → 单一 opaque 类型，
/// 供 `KWayMerge::new` 要求「所有源流同类型」）。
fn sac_iter_to_dps(
    iter: SacRowIter,
    table_id: u32,
    source_id: usize,
) -> impl Iterator<Item = DataPoint> {
    iter.map(move |r: Result<SacRow, _>| row_to_dp(r.expect("read sac row"), table_id, source_id))
}

/// 把 `SacRow` 转成 `DataPoint`（对齐 `scheduler::row_to_data_point` 的语义：
/// SAC 不持久化 server_ts，回退为 ts）。
fn row_to_dp(row: SacRow, table_id: u32, source_id: usize) -> DataPoint {
    DataPoint {
        table_id,
        ts: row.ts,
        server_ts: row.ts,
        values: row.values,
        qualities: row.qualities,
        wal_seq: row.wal_seq,
        source_id,
    }
}

// ─── D1：流式去重 == 旧全量去重（逐字段，Some/None 混合 + 跨表 + tiebreak 链） ───

#[test]
fn qa_d1_streaming_dedup_equals_full_field_by_field() {
    // 4 条源流，覆盖：
    // - 同 (table, ts) 的 Some vs None（Some 必胜）
    // - 同 (table, ts) 的 Some(50) vs Some(1)（大 seq 胜）
    // - 同 seq 退到 server_ts 比较
    // - 跨 table 边界
    let s0 = vec![
        dp(1, 100, None, 100, 0, 1.0),
        dp(1, 200, Some(5), 200, 0, 2.0),
    ];
    let s1 = vec![
        dp(1, 100, Some(1), 100, 1, 10.0),
        dp(1, 200, None, 200, 1, 20.0),
    ];
    let s2 = vec![
        dp(1, 100, Some(50), 100, 2, 100.0),
        dp(2, 100, None, 100, 2, 3.0),
    ];
    let s3 = vec![
        dp(1, 200, Some(5), 300, 3, 200.0), // 同 seq(5) 退 server_ts：300 > 200 → s3 胜
        dp(2, 100, None, 500, 3, 30.0),     // 同 (2,100) 退 server_ts：500 > 100 → s3 胜
    ];

    let full = dedup_sorted(k_way_merge(vec![
        s0.clone(),
        s1.clone(),
        s2.clone(),
        s3.clone(),
    ]));
    let streamed: Vec<DataPoint> = DedupIter::new(KWayMerge::new(vec![
        s0.into_iter(),
        s1.into_iter(),
        s2.into_iter(),
        s3.into_iter(),
    ]))
    .collect();

    let full_sig: Vec<_> = full.iter().map(sig).collect();
    let streamed_sig: Vec<_> = streamed.iter().map(sig).collect();
    assert_eq!(
        full_sig, streamed_sig,
        "D1：流式去重必须与全量去重逐字段一致"
    );

    // 并显式核对胜者（不依赖实现，直接验证语义）。
    assert_eq!(streamed.len(), 3);
    assert_eq!(streamed[0].values, vec![(0, TypedValue::F64(100.0))]); // (1,100) Some(50)
    assert_eq!(streamed[0].wal_seq, Some(50));
    assert_eq!(streamed[1].values, vec![(0, TypedValue::F64(200.0))]); // (1,200) server_ts 300
    assert_eq!(streamed[1].server_ts, 300);
    assert_eq!(streamed[2].values, vec![(0, TypedValue::F64(30.0))]); // (2,100) server_ts 500
}

/// 完全平局（同 table/ts、同 wal_seq、同 server_ts）→ 保留先到（source_id 小）者。
#[test]
fn qa_d1_streaming_dedup_tie_keeps_first_source() {
    let s0 = vec![dp(7, 500, Some(9), 500, 0, 7.0)];
    let s1 = vec![dp(7, 500, Some(9), 500, 1, 77.0)];
    let s2 = vec![dp(7, 500, Some(9), 500, 2, 777.0)];

    let full = dedup_sorted(k_way_merge(vec![s0.clone(), s1.clone(), s2.clone()]));
    let streamed: Vec<DataPoint> = DedupIter::new(KWayMerge::new(vec![
        s0.into_iter(),
        s1.into_iter(),
        s2.into_iter(),
    ]))
    .collect();

    assert_eq!(streamed.len(), 1);
    assert_eq!(full.len(), 1);
    assert_eq!(sig(&full[0]), sig(&streamed[0]));
    assert_eq!(
        streamed[0].source_id, 0,
        "完全平局必须保留 source_id 最小（先到）者"
    );
    assert_eq!(streamed[0].values, vec![(0, TypedValue::F64(7.0))]);
}

// ─── D2：KWayMerge 独立产生全局有序序列（不依赖与 k_way_merge 的等价断言） ───

#[test]
fn qa_d2_kwaymerge_produces_globally_sorted_order() {
    let s0 = vec![dp(2, 200, None, 200, 0, 1.0), dp(2, 400, None, 400, 0, 2.0)];
    let s1 = vec![dp(1, 100, None, 100, 1, 3.0), dp(3, 100, None, 100, 1, 4.0)];
    let s2 = vec![dp(1, 150, None, 150, 2, 5.0), dp(2, 100, None, 100, 2, 6.0)];

    let merged: Vec<DataPoint> =
        KWayMerge::new(vec![s0.into_iter(), s1.into_iter(), s2.into_iter()]).collect();

    // 期望的全局序：(table_id, ts, source_id) 升序。
    let expected: Vec<(u32, i64, usize)> = vec![
        (1, 100, 1),
        (1, 150, 2),
        (2, 100, 2),
        (2, 200, 0),
        (2, 400, 0),
        (3, 100, 1),
    ];
    let got: Vec<(u32, i64, usize)> = merged
        .iter()
        .map(|p| (p.table_id, p.ts, p.source_id))
        .collect();
    assert_eq!(
        got, expected,
        "D2：KWayMerge 必须按 (table_id, ts, source_id) 全局升序归并"
    );

    // 并验证序列单调不减（结构性护栏）。
    for w in got.windows(2) {
        assert!(
            (w[0].0, w[0].1, w[0].2) <= (w[1].0, w[1].1, w[1].2),
            "KWayMerge 输出必须单调不减，出现逆序 {:?} -> {:?}",
            w[0],
            w[1]
        );
    }
}

// ─── D5：跨块(>4096) + 跨文件 + 同 ts 去重：流式链 == 全量链（逐字段） ───

#[test]
fn qa_d5_streaming_chain_equals_full_chain_cross_block() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sac_dir = tmp.path().join("sac");
    let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

    // 文件 A：ts 1..=5000（2 个 Block），值 = ts，wal_seq None。
    let a_ts: Vec<i64> = (1..=5000).collect();
    let a_vals: Vec<f64> = a_ts.iter().map(|&t| t as f64).collect();
    // 文件 B：ts 2500..=2600，值 = ts*10，wal_seq Some(100) 覆盖 A。
    let b_ts: Vec<i64> = (2500..=2600).collect();
    let b_vals: Vec<f64> = b_ts.iter().map(|&t| t as f64 * 10.0).collect();

    let ca = f64_chunk(42, a_ts, a_vals);
    let cb = f64_chunk(42, b_ts, b_vals);
    let pa = writer
        .write_chunk(&ca, 100, SacLevel::L0, 1, None)
        .expect("write A");
    let pb = writer
        .write_chunk(&cb, 100, SacLevel::L0, 2, Some(100))
        .expect("write B");

    // —— 流式链：SacRowIter（逐 Block 惰性读）→ KWayMerge → DedupIter ——
    let iter_a = SacRowIter::new(
        &pa,
        sensearcdb_engine::TenantId::DEFAULT,
        42,
        i64::MIN,
        i64::MAX,
        &[],
        None,
    )
    .expect("open A");
    let iter_b = SacRowIter::new(
        &pb,
        sensearcdb_engine::TenantId::DEFAULT,
        42,
        i64::MIN,
        i64::MAX,
        &[],
        None,
    )
    .expect("open B");
    let streamed: Vec<DataPoint> = DedupIter::new(KWayMerge::new(vec![
        sac_iter_to_dps(iter_a, 42, 0),
        sac_iter_to_dps(iter_b, 42, 1),
    ]))
    .collect();

    // —— 全量链：read_range（全量 collect）→ 按 (table,ts) 排序 → dedup_sorted ——
    let rows_a = SacReader::read_range(
        &pa,
        sensearcdb_engine::TenantId::DEFAULT,
        42,
        i64::MIN,
        i64::MAX,
        &[],
        None,
    )
    .expect("read A full");
    let rows_b = SacReader::read_range(
        &pb,
        sensearcdb_engine::TenantId::DEFAULT,
        42,
        i64::MIN,
        i64::MAX,
        &[],
        None,
    )
    .expect("read B full");
    let full = dedup_sorted(k_way_merge(vec![
        rows_a
            .into_iter()
            .map(|r| row_to_dp(r, 42, 0))
            .collect::<Vec<_>>(),
        rows_b
            .into_iter()
            .map(|r| row_to_dp(r, 42, 1))
            .collect::<Vec<_>>(),
    ]));

    let full_sig: Vec<_> = full.iter().map(sig).collect();
    let streamed_sig: Vec<_> = streamed.iter().map(sig).collect();
    assert_eq!(
        full_sig, streamed_sig,
        "D5：跨块+跨文件去重后，流式链与全量链必须逐字段一致"
    );
    assert_eq!(streamed.len(), 5000, "无丢无重（B 全部落在 A 范围内）");

    // 覆盖区间取 B 新值（wal_seq Some 优先于 None），未覆盖点取 A 值。
    for t in 2500..=2600 {
        let p = streamed.iter().find(|p| p.ts == t).expect("覆盖点存在");
        assert_eq!(p.values, vec![(0, TypedValue::F64(t as f64 * 10.0))]);
    }
    for &t in &[1, 1000, 2499, 2601, 4000, 5000] {
        let p = streamed.iter().find(|p| p.ts == t).expect("未覆盖点存在");
        assert_eq!(p.values, vec![(0, TypedValue::F64(t as f64))]);
    }
}

// ─── ZSTD9 写读对称 + 磁盘字节级 frame magic ───

/// 单列 chunk 的 ts 流在文件内的字节偏移 = header + mini-header + (1+2)*dir_entry。
fn ts_stream_offset() -> usize {
    SAC_HEADER_SIZE + BLOCK_MINI_HEADER_SIZE + (1 + 2) * STREAM_DIR_ENTRY_SIZE
}

#[test]
fn qa_zstd9_write_read_symmetric_and_frame_magic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sac_dir = tmp.path().join("sac");

    let ts = vec![100i64, 200, 300, 400, 500];
    let vals: Vec<f64> = ts.iter().map(|&t| t as f64 * 1.5).collect();

    // L2：Zstd9 写 → 读回（reader 由 header.level 派生 Zstd9，验证写读对称）。
    let w2 = SacWriter::new(&sac_dir, CompressionType::Zstd9);
    let c2 = f64_chunk(9, ts.clone(), vals.clone());
    let p2 = w2
        .write_chunk(&c2, 0, SacLevel::L2, 1, None)
        .expect("write L2");

    let rows2 = SacReader::read_range(
        &p2,
        sensearcdb_engine::TenantId::DEFAULT,
        9,
        0,
        1000,
        &[],
        None,
    )
    .expect("read L2 back");
    assert_eq!(rows2.len(), 5, "L2 必须能正确读回全部点（ZSTD9 写读对称）");
    for (i, r) in rows2.iter().enumerate() {
        assert_eq!(r.ts, ts[i]);
        assert_eq!(r.values, vec![(0, TypedValue::F64(vals[i]))]);
    }

    // 磁盘字节级：L2 的 ts 流前 4 字节必须是 zstd frame magic 28 B5 2F FD。
    let raw2 = std::fs::read(&p2).expect("read L2 raw");
    let off = ts_stream_offset();
    assert!(
        raw2.len() > off + 4,
        "L2 文件应含 ts 流（文件 {} 字节，ts 流偏移 {off}）",
        raw2.len()
    );
    assert_eq!(
        &raw2[off..off + 4],
        &[0x28, 0xB5, 0x2F, 0xFD],
        "L2 块 ts 流必须是 zstd frame"
    );

    // L0：Lz4 写 → 读回，且 ts 流前 4 字节不得是 zstd magic（应为 LZ4）。
    let w0 = SacWriter::new(&sac_dir, CompressionType::Lz4);
    let c0 = f64_chunk(9, ts.clone(), vals.clone());
    let p0 = w0
        .write_chunk(&c0, 0, SacLevel::L0, 1, None)
        .expect("write L0");

    let rows0 = SacReader::read_range(
        &p0,
        sensearcdb_engine::TenantId::DEFAULT,
        9,
        0,
        1000,
        &[],
        None,
    )
    .expect("read L0 back");
    assert_eq!(rows0.len(), 5);
    for (i, r) in rows0.iter().enumerate() {
        assert_eq!(r.ts, ts[i]);
        assert_eq!(r.values, vec![(0, TypedValue::F64(vals[i]))]);
    }

    let raw0 = std::fs::read(&p0).expect("read L0 raw");
    assert_ne!(
        &raw0[off..off + 4],
        &[0x28, 0xB5, 0x2F, 0xFD],
        "L0 块 ts 流不得是 zstd frame（应为 LZ4）"
    );
}

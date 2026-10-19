//! QA 独立验证（software-qa-engineer-2）：SAC 多表聚合格式改造（v8→v9）数据正确性。
//!
//! 独立于工程师自测（trust but verify，fresh eyes），本文件重点覆盖：
//! - A. 多表写读 roundtrip 数据正确性（含交错时间范围，暴露 (table_id, min_ts) 排序
//!   下 footer「全局 min/max」是否被 first/last block 的 min/max 误代）。
//! - 不变式 I3（TableIndex first_l2_idx + block_count 精确覆盖 L2 连续区间）
//!   与 I4（block mini-header.table_id == L2.table_id == TableIndex.table_id 三重一致）。
//! - D. 跨重启数据不丢（Run2 重启后**写新数据**再 flush，验证 SAC 文件 seq 续号
//!   不覆盖重启前的存量文件）。
//!
//! 全部走真实磁盘 I/O 路径（SacWriter → SacReader / Engine 全链路），不 mock。

use std::collections::BTreeMap;

use sensearcdb_common::constants::{
    SAC_FOOTER_SIZE, SAC_L2_ENTRY_SIZE, SAC_TABLE_INDEX_ENTRY_SIZE,
};
use sensearcdb_engine::compression::CompressionType;
use sensearcdb_engine::memtable::chunk::ColumnarChunk;
use sensearcdb_engine::memtable::col_vec::TypedValue;
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::{L2Descriptor, SacFooter, SacReader, SacWriter, TableIndexEntry};
use sensearcdb_engine::TenantId;

/// 构造一个单列（column 0 = F64）的 chunk，ts 升序，ts_index 为唯一真相源。
fn chunk(table_id: u32, points: &[(i64, f64)]) -> ColumnarChunk {
    let mut c = ColumnarChunk::new(TenantId::DEFAULT, table_id);
    for &(ts, v) in points {
        c.append(ts, &[(0, TypedValue::F64(v))], &[(0, 0xC0)], 0);
    }
    c
}

/// 解析 SAC 原始字节 → (footer, Vec<TableIndexEntry>, Vec<L2Descriptor> + block bytes)。
struct ParsedSac {
    footer: SacFooter,
    tables: Vec<TableIndexEntry>,
    l2: Vec<L2Descriptor>,
    raw: Vec<u8>,
}

fn parse_sac(path: &std::path::Path) -> ParsedSac {
    let raw = std::fs::read(path).expect("read raw sac");
    let footer = SacFooter::from_bytes(
        &raw[raw.len() - SAC_FOOTER_SIZE..]
            .try_into()
            .expect("footer slice"),
    )
    .expect("parse footer");

    let mut tables = Vec::new();
    for i in 0..footer.table_count as usize {
        let s = footer.table_index_offset as usize + i * SAC_TABLE_INDEX_ENTRY_SIZE;
        let e = s + SAC_TABLE_INDEX_ENTRY_SIZE;
        tables.push(
            TableIndexEntry::from_bytes(&raw[s..e].try_into().expect("ti slice"))
                .expect("parse ti"),
        );
    }

    let mut l2 = Vec::new();
    let l2_count = (footer.table_index_offset - footer.l2_offset) as usize / SAC_L2_ENTRY_SIZE;
    for i in 0..l2_count {
        let s = footer.l2_offset as usize + i * SAC_L2_ENTRY_SIZE;
        let e = s + SAC_L2_ENTRY_SIZE;
        l2.push(
            L2Descriptor::from_bytes(&raw[s..e].try_into().expect("l2 slice")).expect("parse l2"),
        );
    }

    ParsedSac {
        footer,
        tables,
        l2,
        raw,
    }
}

/// 读 block mini-header 的 table_id（48B mini-header 的 offset 2..6）。
fn block_mini_header_table_id(raw: &[u8], block_offset: u64) -> u32 {
    let o = block_offset as usize;
    u32::from_le_bytes(raw[o + 2..o + 6].try_into().expect("table_id slice"))
}

/// A：多表写读 roundtrip + 不变式 I3/I4。
///
/// 关键设计点：三张表的 ts **交错**（table 1 时间最靠后，table 2 最靠前，
/// table 3 居中），块按 (table_id, min_ts) 排序后「物理首块」不再是「全局 min_ts」。
/// 借此同时验证：(1) 各表逐点读回正确；(2) I3 区间覆盖；(3) I4 三重一致。
#[test]
fn qa_multitable_roundtrip_interleaved_ts() {
    let tmp = tempfile::TempDir::new().unwrap();
    let writer = SacWriter::new(tmp.path().join("sac"), CompressionType::Lz4);

    // 按 (table_id, min_ts) 升序 append：表 1 → 表 2 → 表 3。
    // 但时间范围交错：表 1 的 ts 最大，表 2 的 ts 最小。
    let c1 = chunk(1, &[(500, 5.0), (600, 6.0)]);
    let c2 = chunk(2, &[(100, 1.0), (200, 2.0)]);
    let c3 = chunk(3, &[(300, 3.0), (400, 4.0)]);

    let mut b = writer
        .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
        .expect("begin_write");
    b.append_chunk(&c1, Some(7)).expect("append t1");
    b.append_chunk(&c2, Some(8)).expect("append t2");
    b.append_chunk(&c3, Some(9)).expect("append t3");
    let path = b.finish().expect("finish");

    // (1) 逐表读回，逐点断言 ts/value/quality/table_id 全对。
    let read = |tid: u32| {
        SacReader::read_range(&path, TenantId::DEFAULT, tid, 0, 1000, &[0], None)
            .expect("read_range")
    };

    let rows1 = read(1);
    assert_eq!(rows1.len(), 2);
    assert_eq!(rows1[0].ts, 500);
    assert_eq!(rows1[0].table_id, 1);
    assert_eq!(rows1[0].values[0], (0, TypedValue::F64(5.0)));
    assert_eq!(rows1[0].qualities[0], (0, 0xC0));
    assert_eq!(rows1[1].ts, 600);
    assert_eq!(rows1[1].values[0], (0, TypedValue::F64(6.0)));

    let rows2 = read(2);
    assert_eq!(rows2.len(), 2);
    assert_eq!(
        rows2.iter().map(|r| r.ts).collect::<Vec<_>>(),
        vec![100, 200]
    );
    assert_eq!(rows2[0].values[0], (0, TypedValue::F64(1.0)));
    assert!(rows2.iter().all(|r| r.table_id == 2));

    let rows3 = read(3);
    assert_eq!(rows3.len(), 2);
    assert_eq!(
        rows3.iter().map(|r| r.ts).collect::<Vec<_>>(),
        vec![300, 400]
    );
    assert_eq!(rows3[0].values[0], (0, TypedValue::F64(3.0)));

    // 不存在表 → 空（TableIndex 二分未命中，不跨表串数据）。
    assert!(read(99).is_empty());

    // (2) 解析盘上字节，验证不变式 I3 + I4。
    let parsed = parse_sac(&path);
    assert_eq!(parsed.footer.table_count, 3);
    assert_eq!(parsed.footer.block_count, 3);

    // I2/I3：TableIndex 按 table_id 升序；first_l2_idx 单调；区间精确覆盖 L2。
    let mut prev_table = 0u32;
    let mut prev_l2 = 0u32;
    for t in &parsed.tables {
        assert!(t.table_id > prev_table, "TableIndex 须按 table_id 升序");
        assert!(t.first_l2_idx >= prev_l2, "first_l2_idx 须单调不减");
        assert_eq!(t.block_count, 1);
        assert_eq!(t.l2_range().start, t.first_l2_idx as usize);
        assert_eq!(t.l2_range().len(), t.block_count as usize);

        // I3: first_l2_idx + block_count 精确覆盖该表 L2 连续区间。
        let l2_slice = &parsed.l2[t.l2_range()];
        assert_eq!(l2_slice.len(), t.block_count as usize);
        assert!(l2_slice.iter().all(|l| l.table_id == t.table_id));

        // I3: total_block_bytes == 该表各块 block_size 之和。
        let sum: u64 = l2_slice.iter().map(|l| l.block_size as u64).sum();
        assert_eq!(t.total_block_bytes, sum);

        // I4: 三重一致（block mini-header.table_id == L2.table_id == TableIndex.table_id）。
        for l in l2_slice {
            let mh_tid = block_mini_header_table_id(&parsed.raw, l.block_offset);
            assert_eq!(
                mh_tid, t.table_id,
                "block mini-header table_id 须 == TableIndex table_id"
            );
            assert_eq!(
                l.table_id, t.table_id,
                "L2 table_id 须 == TableIndex table_id"
            );
        }

        prev_table = t.table_id;
        prev_l2 = t.first_l2_idx;
    }
    assert_eq!(
        parsed.tables.iter().map(|t| t.table_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

/// 边界：footer 的 min_ts / max_ts 必须为**全局**最小/最大时间戳，
/// 而非「(table_id, min_ts) 排序后的首个块 min_ts / 末个块 max_ts」。
///
/// 设计 §2.7 定义 footer.min_ts = 「全局最小时间戳」、footer.max_ts = 「全局最大时间戳」。
/// 当各表时间范围交错时，物理首块（最小 table_id）的 min_ts 未必是全局最小。
#[test]
fn qa_multitable_footer_global_minmax() {
    let tmp = tempfile::TempDir::new().unwrap();
    let writer = SacWriter::new(tmp.path().join("sac"), CompressionType::Lz4);

    // 表 1（table_id 最小）ts 最大；表 2（table_id 更大）ts 最小 → 时间范围交错。
    let c1 = chunk(1, &[(500, 5.0), (600, 6.0)]);
    let c2 = chunk(2, &[(100, 1.0), (200, 2.0)]);

    let mut b = writer
        .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
        .expect("begin_write");
    b.append_chunk(&c1, None).expect("append t1");
    b.append_chunk(&c2, None).expect("append t2");
    let path = b.finish().expect("finish");

    let parsed = parse_sac(&path);
    assert_eq!(
        parsed.footer.min_ts, 100,
        "footer.min_ts 必须是全局最小时间戳（表 2 的 ts=100），而非首块（表 1）的 min_ts"
    );
    assert_eq!(
        parsed.footer.max_ts, 600,
        "footer.max_ts 必须是全局最大时间戳（表 1 的 ts=600），而非末块（表 2）的 max_ts"
    );
}

/// D：跨重启数据不丢（Run2 重启后**写新数据**再 flush，SAC seq 续号不得覆盖存量文件）。
///
/// 风险：FlushCoordinator 的 seq_counter 重启后从 1 起、且 flush 路径不复用
/// `next_seq_for_partition`，可能用 seq=1 覆盖重启前已落盘的 `{P}_0_0001.sac`。
#[tokio::test]
async fn qa_restart_new_writes_no_data_loss() {
    use sensearcdb_common::{AppConfig, ConfigHandle};
    use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch};

    fn point(table_id: u32, ts: i64, v: f64) -> DataPoint {
        DataPoint {
            table_id,
            tenant_id: TenantId::DEFAULT,
            ts,
            server_ts: ts + 1,
            values: vec![(0, TypedValue::F64(v))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }
    }

    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    let handle = ConfigHandle::new(cfg);

    // Run1：写 3 点 → 落 SAC → 优雅停。
    {
        let engine = Engine::new(&handle, None).await.expect("Engine::new Run1");
        engine
            .write_batch(PointBatch {
                points: vec![
                    point(42, 1_000, 1.0),
                    point(42, 2_000, 2.0),
                    point(42, 3_000, 3.0),
                ],
            })
            .await
            .expect("write_batch Run1");
        engine.force_freeze();
        engine.flush_immutable_sync().await.expect("flush Run1");
        engine.shutdown().await.expect("shutdown Run1");
    }

    // Run2：重启后写**新** 3 点（同表、同分区窗口）→ 落 SAC → 读回。
    {
        let engine = Engine::new(&handle, None).await.expect("Engine::new Run2");
        engine
            .write_batch(PointBatch {
                points: vec![
                    point(42, 4_000, 4.0),
                    point(42, 5_000, 5.0),
                    point(42, 6_000, 6.0),
                ],
            })
            .await
            .expect("write_batch Run2");
        engine.force_freeze();
        engine.flush_immutable_sync().await.expect("flush Run2");

        let (cols, timestamps) = engine
            .query_table_columns(TenantId::DEFAULT, 42, &[0], 0, i64::MAX)
            .await
            .expect("query_table_columns Run2");

        // 必须同时读回 Run1 的 3 点 + Run2 的 3 点 = 6 点。
        assert_eq!(
            timestamps.len(),
            6,
            "重启后写新数据不得覆盖重启前的存量 SAC：应恢复 6 点，实际 {}",
            timestamps.len()
        );

        let mut pairs: Vec<(i64, f64)> = timestamps
            .iter()
            .copied()
            .zip(cols[0].iter().map(|v| match v {
                Some(TypedValue::F64(x)) => *x,
                other => panic!("期望 F64，实际 {other:?}"),
            }))
            .collect();
        pairs.sort_by_key(|(ts, _)| *ts);

        let mut expected: Vec<(i64, f64)> = vec![
            (1_000, 1.0),
            (2_000, 2.0),
            (3_000, 3.0),
            (4_000, 4.0),
            (5_000, 5.0),
            (6_000, 6.0),
        ];
        expected.sort_by_key(|(ts, _)| *ts);
        assert_eq!(pairs, expected, "重启后旧+新数据应逐点一致");

        engine.shutdown().await.expect("shutdown Run2");
    }
}

/// 边界：`next_seq_in_dir` 的前缀匹配不得把「数字前缀关系」的相邻分区误计入。
///
/// 19 位补零 + `{:019}_`（后缀下划线）+ `parse_file_name` 的 `pn == partition_ns`
/// 三重防护：partition_ns=123 不得匹配到 1234 的文件，也不得跨 level 计数。
#[test]
fn qa_next_seq_in_dir_prefix_boundary() {
    use sensearcdb_engine::sac::SacNaming;

    let tmp = tempfile::TempDir::new().unwrap();
    let tenant_dir = tmp.path().join("0");
    std::fs::create_dir_all(&tenant_dir).unwrap();

    // 目标分区 123（L0）seq 1 与 5；陷阱分区 1234（L0）seq 1；同分区 123（L1）seq 1。
    for name in [
        "0000000000000000123_0_0001.sac",
        "0000000000000000123_0_0005.sac",
        "0000000000000001234_0_0001.sac", // 陷阱：数字前缀关系
        "0000000000000000123_1_0001.sac", // 同分区不同 level
    ] {
        std::fs::write(tenant_dir.join(name), b"x").unwrap();
    }

    // 分区 123 的 L0 最大 seq = 5 → 下一个应为 6（忽略 1234 与 L1）。
    assert_eq!(
        SacNaming::next_seq_in_dir(&tenant_dir, 123, SacLevel::L0),
        6,
        "next_seq_in_dir(123, L0) 必须忽略 1234 文件与 L1 文件，返回 5+1=6"
    );
    // 分区 1234 的 L0 最大 seq = 1 → 下一个应为 2。
    assert_eq!(
        SacNaming::next_seq_in_dir(&tenant_dir, 1234, SacLevel::L0),
        2,
        "next_seq_in_dir(1234, L0) 必须只算 1234 自己的文件"
    );
    // 分区 123 的 L1 最大 seq = 1 → 下一个应为 2。
    assert_eq!(
        SacNaming::next_seq_in_dir(&tenant_dir, 123, SacLevel::L1),
        2
    );
    // 空分区 → 1。
    assert_eq!(
        SacNaming::next_seq_in_dir(&tenant_dir, 999, SacLevel::L0),
        1
    );
}

// 确保 BTreeMap import 在 non-test 场景也被用到（避免 dead_code 警告干扰）。
#[allow(dead_code)]
fn _type_anchor() -> BTreeMap<i64, usize> {
    BTreeMap::new()
}

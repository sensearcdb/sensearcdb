//! QA 独立回归验证（阶段 1）：SAC 格式对齐（S1~S7）边界用例。
//!
//! 验证契约定稿（sac-format-final-ruling.md）：
//! - S1  Footer 48B + block_count_mirror 撕裂写校验
//! - S3  L2 per-block 描述符（block_offset/block_size/column_count）
//! - S4  Block 内 Stream Directory（entry[0]=ts、entry[1..=N]=val、entry[N+1]=qual）
//! - S5  多类型编码 + F32 原生（O3=A）+ String 返回原串
//! - S6  每 Block 尾部 CRC32C，读侧失败 seek 跳过 + warning（不整文件报错）
//!
//! 本文件独立构造数据，不经工程师自测用例，覆盖：
//! 全类型 round-trip / 空表 / 单列 / 多列宽表 / footer mirror 不一致 / per-block CRC 损坏。

use std::collections::{BTreeMap, HashMap};
use std::fs;

use sensearcdb_common::constants::{SAC_FOOTER_SIZE, SAC_L2_ENTRY_SIZE};
use sensearcdb_engine::compression::CompressionType;
use sensearcdb_engine::memtable::{ColVec, ColumnarChunk, TypedValue};
use sensearcdb_engine::sac::footer::SacFooter;
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::l2_descriptor::L2Descriptor;
use sensearcdb_engine::sac::reader::SacRow;
use sensearcdb_engine::sac::{SacReader, SacWriter};

/// 便捷构造一个 ColumnarChunk（SoA 布局，qualities 长度与 point_count 对齐）。
fn chunk(table_id: u32, timestamps: Vec<i64>, columns: HashMap<u16, ColVec>) -> ColumnarChunk {
    let point_count = timestamps.len();
    let ts_index: BTreeMap<i64, usize> = timestamps
        .iter()
        .enumerate()
        .map(|(i, ts)| (*ts, i))
        .collect();
    // 本文件夹具均为密集列（每列长度 == point_count），presence 全 true，
    // 与生产 append 路径「有值 push true」的语义一致。
    let presence: HashMap<u16, Vec<bool>> = columns
        .iter()
        .map(|(cid, _)| (*cid, vec![true; point_count]))
        .collect();
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

/// 写入一个 chunk，返回 `(TempDir, 路径)`，TempDir 保证文件在测试期间存活。
fn write(chunk: &ColumnarChunk) -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().join("sac");
    let writer = SacWriter::new(&dir, CompressionType::Lz4);
    let path = writer
        .write_chunk(chunk, 0, SacLevel::L0, 1, chunk.max_wal_seq)
        .expect("write failed");
    (tmp, path)
}

/// 返回按 (col_id, value) 排序后的单行值。
fn sorted_values(row: &SacRow) -> Vec<(u16, TypedValue)> {
    let mut v = row.values.clone();
    v.sort_by_key(|(c, _)| *c);
    v
}

// ─── 全类型 round-trip ────────────────────────────────────────────────────

#[test]
fn qa_sac_roundtrip_f64() {
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0, -1.5, 0.0]));
    let c = chunk(1, vec![100, 200, 300, 400, 500], cols);
    let (_tmp, path) = write(&c);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].values, vec![(0, TypedValue::F64(1.0))]);
    assert_eq!(rows[3].values, vec![(0, TypedValue::F64(-1.5))]);
    assert_eq!(rows[4].values, vec![(0, TypedValue::F64(0.0))]);
}

#[test]
fn qa_sac_roundtrip_f32_native() {
    // O3=A：F32 原生编码，不 upcast f64，round-trip 后仍为 F32。
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::F32(vec![1.5f32, -2.25, 0.0, 100.125]));
    let c = chunk(1, vec![100, 200, 300, 400], cols);
    let (_tmp, path) = write(&c);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].values, vec![(0, TypedValue::F32(1.5))]);
    assert_eq!(rows[1].values, vec![(0, TypedValue::F32(-2.25))]);
    assert_eq!(rows[3].values, vec![(0, TypedValue::F32(100.125))]);
}

#[test]
fn qa_sac_roundtrip_i64_positive_bool_string() {
    // I64（非负）/ Bool / String 各保其类型（String 返回原串，非 f64/NaN）。
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::I64(vec![10, 20, 30]));
    cols.insert(1, ColVec::Bool(vec![true, false, true]));
    cols.insert(
        2,
        ColVec::String(vec![
            "open".to_string(),
            "close".to_string(),
            "open".to_string(),
        ]),
    );
    let c = chunk(9, vec![100, 200, 300], cols);
    let (_tmp, path) = write(&c);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        9,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        sorted_values(&rows[0]),
        vec![
            (0, TypedValue::I64(10)),
            (1, TypedValue::Bool(true)),
            (2, TypedValue::String("open".to_string())),
        ]
    );
    assert_eq!(
        sorted_values(&rows[1]),
        vec![
            (0, TypedValue::I64(20)),
            (1, TypedValue::Bool(false)),
            (2, TypedValue::String("close".to_string())),
        ]
    );
}

/// 负值 I64 应无损 round-trip（Simple8b 声称 zigzag 编码，负值必须支持）。
///
/// 工业场景（如负温度）I64 可为负，编码层不得因负值丢失整列数据。
#[test]
fn qa_sac_roundtrip_i64_negative() {
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::I64(vec![10, -20, 30]));
    let c = chunk(1, vec![100, 200, 300], cols);
    let (_tmp, path) = write(&c);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 3);
    // 期望：负值 I64 无损还原。
    assert_eq!(rows[0].values, vec![(0, TypedValue::I64(10))]);
    assert_eq!(rows[1].values, vec![(0, TypedValue::I64(-20))]);
    assert_eq!(rows[2].values, vec![(0, TypedValue::I64(30))]);
}

/// 整数子类型保真：U64 / I32 / Enum16 应 round-trip 回其原始类型。
///
/// 契约定稿 O3 附注「存储层忠实于 schema（F32 存 F32 而非 upcast f64）」确立
/// 类型保真原则；`TypedValue` 枚举含 U64/I32/Enum16 变体，写→读应还原原类型。
#[test]
fn qa_sac_roundtrip_integer_subtype_fidelity() {
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::U64(vec![1, 2, 3]));
    cols.insert(1, ColVec::I32(vec![100, 200, 300]));
    cols.insert(2, ColVec::Enum16(vec![7, 8, 9]));
    let c = chunk(5, vec![100, 200, 300], cols);
    let (_tmp, path) = write(&c);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        5,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 3);

    let v0 = sorted_values(&rows[0]);
    // 期望：忠实还原 U64 / I32 / Enum16 原类型。
    assert_eq!(v0[0], (0, TypedValue::U64(1)));
    assert_eq!(v0[1], (1, TypedValue::I32(100)));
    assert_eq!(v0[2], (2, TypedValue::Enum16(7)));
}

// ─── 空表 / 单列 / 多列宽表 ───────────────────────────────────────────────

#[test]
fn qa_sac_empty_chunk() {
    let c = chunk(1, vec![], HashMap::new());
    let (_tmp, path) = write(&c);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert!(rows.is_empty());
}

#[test]
fn qa_sac_single_column() {
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::F64(vec![42.0, 43.0]));
    let c = chunk(1, vec![100, 200], cols);
    let (_tmp, path) = write(&c);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values, vec![(0, TypedValue::F64(42.0))]);
}

#[test]
fn qa_sac_multi_column_wide() {
    // 8 列稠密 0..=7，各列独立编码 + 独立 LZ4 + 按列投影。
    let mut cols = HashMap::new();
    for col in 0..8u16 {
        let vals: Vec<f64> = (0..4).map(|i| (col as f64) * 10.0 + i as f64).collect();
        cols.insert(col, ColVec::F64(vals));
    }
    let c = chunk(1, vec![100, 200, 300, 400], cols);
    let (_tmp, path) = write(&c);

    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 4);

    let v0 = sorted_values(&rows[0]);
    assert_eq!(v0.len(), 8);
    for (col, _) in &v0 {
        assert!(*col < 8);
    }
    assert_eq!(v0[0], (0, TypedValue::F64(0.0)));
    assert_eq!(v0[7], (7, TypedValue::F64(70.0)));

    // 按列投影：只取第 2 列。
    let projected = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[2],
        None,
    )
    .expect("read failed");
    assert_eq!(projected.len(), 4);
    assert_eq!(projected[0].values, vec![(2, TypedValue::F64(20.0))]);
}

/// 稀疏列 round-trip（SAC v6 关键用例）：写 col 0 与 col 2（跳过 col 1，模拟删列）。
///
/// 读回必须 col 0 → 0、col 2 → 2，不得产生 col 1 错位。
/// 旧 v5 的 dense 位置推导会把第 2 列误读成 col 1，本用例证明 column_id 已显式落盘。
#[test]
fn qa_sac_sparse_columns_roundtrip() {
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0]));
    cols.insert(2, ColVec::F64(vec![100.0, 200.0, 300.0]));
    let c = chunk(1, vec![100, 200, 300], cols);
    let (_tmp, path) = write(&c);

    // 全列读回：col 0 → 0、col 2 → 2（无 col 1 错位）。
    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[],
        None,
    )
    .expect("read failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        sorted_values(&rows[0]),
        vec![(0, TypedValue::F64(1.0)), (2, TypedValue::F64(100.0)),]
    );
    assert_eq!(
        sorted_values(&rows[2]),
        vec![(0, TypedValue::F64(3.0)), (2, TypedValue::F64(300.0)),]
    );

    // 按列投影 col 2：仍须是 col 2（不得因稀疏列错位成 col 1）。
    let projected = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[2],
        None,
    )
    .expect("read failed");
    assert_eq!(projected.len(), 3);
    assert_eq!(projected[0].values, vec![(2, TypedValue::F64(100.0))]);
    assert_eq!(projected[1].values, vec![(2, TypedValue::F64(200.0))]);
    assert_eq!(projected[2].values, vec![(2, TypedValue::F64(300.0))]);
}

// ─── footer mirror 不一致 / per-block CRC 损坏 ─────────────────────────────

#[test]
fn qa_sac_footer_mirror_mismatch_errors() {
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0]));
    let c = chunk(1, vec![100, 200, 300], cols);
    let (_tmp, path) = write(&c);

    // 破坏 footer 的 block_count_mirror 字段（64B 布局中偏移 40..44）。
    let mut bytes = fs::read(&path).unwrap();
    let len = bytes.len();
    bytes[len - SAC_FOOTER_SIZE + 40..len - SAC_FOOTER_SIZE + 44]
        .copy_from_slice(&999u32.to_le_bytes());
    fs::write(&path, &bytes).unwrap();

    let result = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        1000,
        &[],
        None,
    );
    assert!(result.is_err(), "footer mirror mismatch should be rejected");
}

#[test]
fn qa_sac_per_block_crc_corruption_skips_block() {
    // 5000 点 → 2 个 Block（4096 + 904）。
    let n = 5000usize;
    let timestamps: Vec<i64> = (0..n).map(|i| i as i64).collect();
    let vals: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::F64(vals));
    let c = chunk(1, timestamps, cols);
    let (_tmp, path) = write(&c);

    // 定位 block 0 的 block_offset/block_size（经 footer.l2_offset + 首条 L2）。
    let bytes = fs::read(&path).unwrap();
    let len = bytes.len();
    let footer_bytes: [u8; SAC_FOOTER_SIZE] = bytes[len - SAC_FOOTER_SIZE..].try_into().unwrap();
    let footer = SacFooter::from_bytes(&footer_bytes).expect("footer parse failed");
    let l2_entry: [u8; SAC_L2_ENTRY_SIZE] = bytes
        [footer.l2_offset as usize..footer.l2_offset as usize + SAC_L2_ENTRY_SIZE]
        .try_into()
        .unwrap();
    let l2 = L2Descriptor::from_bytes(&l2_entry).expect("l2 parse failed");

    // 破坏 block 0 尾部 CRC32C 的最后一个字节。
    let mut corrupted = bytes.clone();
    let crc_last = (l2.block_offset as usize + l2.block_size as usize) - 1;
    corrupted[crc_last] ^= 0xFF;
    fs::write(&path, &corrupted).unwrap();

    // S6：读侧应跳过损坏 Block，返回其余 Block 的 904 行（而非整文件报错）。
    let rows = SacReader::read_range(
        &path,
        sensearcdb_engine::TenantId::DEFAULT,
        1,
        0,
        n as i64,
        &[],
        None,
    )
    .expect("read should not error");
    assert_eq!(
        rows.len(),
        n - 4096,
        "should skip corrupted block and keep the rest"
    );
    assert_eq!(rows[0].ts, 4096);
    assert_eq!(rows.last().unwrap().ts, (n - 1) as i64);
    assert_eq!(rows[0].values, vec![(0, TypedValue::F64(4096.0))]);
}

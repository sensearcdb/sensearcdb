//! QA 第二轮回归验证（阶段 1）：整数边界补验。
//!
//! 工程师修复后补验：
//! 1. U64 大值（u64::MAX 附近 / 接近 2^63）不再 `as i64` 溢出，且 Simple8b 不丢值。
//! 2. I32 极值（i32::MIN / i32::MAX）zigzag 往返无损。

use std::collections::{BTreeMap, HashMap};

use sensearcdb_engine::compression::CompressionType;
use sensearcdb_engine::memtable::{ColVec, ColumnarChunk, TypedValue};
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::{SacReader, SacWriter};

fn chunk(table_id: u32, timestamps: Vec<i64>, columns: HashMap<u16, ColVec>) -> ColumnarChunk {
    let point_count = timestamps.len();
    let ts_index: BTreeMap<i64, usize> = timestamps
        .iter()
        .enumerate()
        .map(|(i, ts)| (*ts, i))
        .collect();
    // 密集列夹具：每列长度 == point_count，presence 全 true（生产 append 语义）。
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

fn write(chunk: &ColumnarChunk) -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().join("sac");
    let writer = SacWriter::new(&dir, CompressionType::Lz4);
    let path = writer
        .write_chunk(chunk, 0, SacLevel::L0, 1, chunk.max_wal_seq)
        .expect("write failed");
    (tmp, path)
}

/// U64 大值（u64::MAX 附近 / 2^63 附近）应无损 round-trip，不 `as i64` 溢出。
#[test]
fn qa_sac_u64_large_values_roundtrip() {
    let big = [
        u64::MAX - 5,
        u64::MAX,
        1u64 << 63,
        (1u64 << 63) - 1,
        1u64 << 62,
    ];
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::U64(big.to_vec()));
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
    assert_eq!(rows.len(), 5, "U64 大值不得导致整列/整块数据丢失");

    for (i, expect) in big.iter().enumerate() {
        assert_eq!(
            rows[i].values,
            vec![(0, TypedValue::U64(*expect))],
            "U64 值 {:?} 未无损还原（row {})",
            expect,
            i
        );
    }
}

/// I32 极值（i32::MIN / i32::MAX）应 zigzag 往返无损。
#[test]
fn qa_sac_i32_extreme_values_roundtrip() {
    let vals = [i32::MIN, -1, 0, 1, i32::MAX];
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::I32(vals.to_vec()));
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

    for (i, expect) in vals.iter().enumerate() {
        assert_eq!(
            rows[i].values,
            vec![(0, TypedValue::I32(*expect))],
            "I32 值 {:?} 未无损还原（row {})",
            expect,
            i
        );
    }
}

/// I64 极端正负值（i64::MAX / i64::MIN 附近）应 zigzag 往返无损。
#[test]
fn qa_sac_i64_extreme_roundtrip() {
    let vals = [
        i64::MIN,
        i64::MIN + 1,
        -(1i64 << 60),
        -100,
        0,
        i64::MAX,
        i64::MAX - 1,
    ];
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::I64(vals.to_vec()));
    let c = chunk(1, vec![100, 200, 300, 400, 500, 600, 700], cols);
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
    assert_eq!(rows.len(), 7, "I64 极端值不得导致整列数据丢失");

    for (i, expect) in vals.iter().enumerate() {
        assert_eq!(
            rows[i].values,
            vec![(0, TypedValue::I64(*expect))],
            "I64 值 {:?} 未无损还原（row {})",
            expect,
            i
        );
    }
}

/// U64 恰好落在 Simple8b 60-bit 上限以内（2^59、2^60-1）应无损。
#[test]
fn qa_sac_u64_within_simple8b_range() {
    let vals = [1u64 << 59, (1u64 << 60) - 1, 42];
    let mut cols = HashMap::new();
    cols.insert(0, ColVec::U64(vals.to_vec()));
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

    for (i, expect) in vals.iter().enumerate() {
        assert_eq!(rows[i].values, vec![(0, TypedValue::U64(*expect))]);
    }
}

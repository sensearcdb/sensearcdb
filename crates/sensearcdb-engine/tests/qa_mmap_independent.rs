//! QA 独立验证（fresh eyes）：SAC 冷读 TableIndex mmap 优化的正确性与边界安全。
//!
//! 不信任工程师自测（reader.rs 内的 `test_mmap_vs_full_read_byte_identical`），
//! 独立聚焦 mmap 引入的**新风险面**：
//!   1. mmap 元数据切片 vs 原 read_exact 的**直接逐字节一致**（不经过行解码，更硬核）；
//!   2. 损坏 footer（table_count 巨大 / offset 越界 / l2_offset > table_index_offset）
//!      必须返回 Err，**不得 panic**（这是 mmap 切片 panic 风险的核心防线）；
//!   3. 空文件 / 空表 / 空 L2；
//!   4. `first_l2_idx + block_count` 恰好等于/超过 `l2_count` 的边界（不 panic、语义与旧一致）；
//!   5. Windows 全文件 mmap 与并发 rename/remove 的交互（compaction 替换文件隐患）。

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use memmap2::MmapOptions;
use sensearcdb_common::constants::{SAC_FOOTER_SIZE, SAC_TABLE_INDEX_ENTRY_SIZE};
use sensearcdb_engine::compression::CompressionType;
use sensearcdb_engine::encryption::{EncryptContext, EncryptError, Encryptor, SharedEncryptor};
use sensearcdb_engine::memtable::{ColVec, ColumnarChunk, TypedValue};
use sensearcdb_engine::sac::footer::SacFooter;
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::{SacReader, SacWriter};
use sensearcdb_engine::TenantId;

// ─── 夹具 ────────────────────────────────────────────────────────────────

/// 密集单列 chunk。
fn f64_chunk(table_id: u32, timestamps: &[i64], values: &[f64]) -> ColumnarChunk {
    let mut columns = HashMap::new();
    columns.insert(0, ColVec::F64(values.to_vec()));
    let ts_index: BTreeMap<i64, usize> = timestamps
        .iter()
        .copied()
        .enumerate()
        .map(|(i, ts)| (ts, i))
        .collect();
    ColumnarChunk {
        table_id,
        tenant_id: TenantId::DEFAULT,
        timestamps: timestamps.to_vec(),
        columns,
        presence: HashMap::new(),
        qualities: vec![0xC0; timestamps.len()],
        ts_index,
        point_count: timestamps.len(),
        out_of_order: false,
        max_wal_seq: None,
        min_wal_seq: None,
    }
}

/// 写一个多表聚合 SAC 文件（每表单块），返回路径。
fn write_multi_table(dir: &Path, tables: &[(u32, &[i64], &[f64])]) -> std::path::PathBuf {
    let writer = SacWriter::new(dir, CompressionType::Lz4);
    let mut builder = writer
        .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
        .expect("begin_write");
    for (tid, ts, vals) in tables {
        builder
            .append_chunk(&f64_chunk(*tid, ts, vals), None)
            .expect("append_chunk");
    }
    builder.finish().expect("finish")
}

/// 读取文件 footer。
fn read_footer(path: &Path) -> SacFooter {
    let bytes = std::fs::read(path).unwrap();
    let len = bytes.len();
    SacFooter::from_bytes(&bytes[len - SAC_FOOTER_SIZE..].try_into().unwrap())
        .expect("footer parse failed")
}

// ─── 1. mmap 元数据切片 vs read_exact 直接逐字节一致 ──────────────────────

/// 直接对比「全文件 mmap 切片」与「read_exact 读回的元数据字节」，二者必须逐字节相等。
///
/// 这比行级 `SacRow` 相等更强：不经过 Block 解码，直接证明 mmap 路径读取的
/// TableIndex 与 L2 字节 == 原 read_exact 路径读回的字节，从根上排除 mmap 错位/缺页。
fn assert_metadata_bytes_identical(path: &Path) {
    let footer = read_footer(path);
    let table_index_size =
        (footer.table_count as u64).saturating_mul(SAC_TABLE_INDEX_ENTRY_SIZE as u64) as usize;
    let table_index_end = (footer.table_index_offset as usize) + table_index_size;
    let l2_size = (footer.table_index_offset - footer.l2_offset) as usize;

    // read_exact 原路径读回元数据（优化前基线）。
    let mut file = File::open(path).unwrap();
    let mut table_index_bytes = vec![0u8; table_index_size];
    file.seek(SeekFrom::Start(footer.table_index_offset)).unwrap();
    file.read_exact(&mut table_index_bytes).unwrap();

    let mut l2_bytes = vec![0u8; l2_size];
    file.seek(SeekFrom::Start(footer.l2_offset)).unwrap();
    file.read_exact(&mut l2_bytes).unwrap();

    // mmap 全文件（与 open_sac_file 同口径）。
    let file = File::open(path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file) }.unwrap();

    // 逐字节对比 TableIndex 区。
    assert_eq!(
        &mmap[footer.table_index_offset as usize..table_index_end],
        &table_index_bytes[..],
        "TableIndex mmap 切片必须与 read_exact 逐字节一致"
    );
    // 逐字节对比 L2 区。
    assert_eq!(
        &mmap[footer.l2_offset as usize..footer.table_index_offset as usize],
        &l2_bytes[..],
        "L2 mmap 切片必须与 read_exact 逐字节一致"
    );
}

#[test]
fn qa_mmap_metadata_bytes_identical_plaintext_multitable() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[
            (10, &[100, 200, 300], &[1.0, 2.0, 3.0]),
            (20, &[150, 250], &[10.0, 20.0]),
            (30, &[180], &[100.0]),
        ],
    );
    assert_metadata_bytes_identical(&path);
}

/// XOR 加密桩（28B 开销，与 engine 约定一致）。
struct XorEncryptor;
impl Encryptor for XorEncryptor {
    fn encrypt(&self, p: &[u8], _c: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        let mut out = Vec::with_capacity(28 + p.len());
        out.extend_from_slice(&[0x3Cu8; 12]);
        out.extend_from_slice(&[0xC3u8; 16]);
        out.extend(p.iter().map(|b| b ^ 0x5A));
        Ok(out)
    }
    fn decrypt(&self, blob: &[u8], _c: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        if blob.len() < 28 {
            return Err(EncryptError::TooShort(blob.len(), 28));
        }
        Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
    }
    fn active_dek_version(&self, _t: TenantId) -> u32 {
        1
    }
    fn is_enabled(&self) -> bool {
        true
    }
}

#[test]
fn qa_mmap_metadata_bytes_identical_encrypted() {
    let tmp = tempfile::TempDir::new().unwrap();
    let enc: SharedEncryptor = Some(Arc::new(XorEncryptor));
    let writer = SacWriter::new_with_encryptor(
        &tmp.path().join("sac"),
        CompressionType::Lz4,
        enc.clone(),
    );
    let mut builder = writer
        .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
        .expect("begin_write");
    builder
        .append_chunk(&f64_chunk(100, &[1000, 2000, 3000], &[1.5, 2.5, 3.5]), None)
        .expect("append");
    let path = builder.finish().expect("finish");

    // 密文文件：元数据（TableIndex/L2）仍必须明文且与 read_exact 逐字节一致。
    assert_metadata_bytes_identical(&path);
    // 且密文块仍能解密读回（mmap 不触碰 Block 密文页，解密走 read_exact）。
    let rows = SacReader::read_range(&path, TenantId::DEFAULT, 100, 0, 1_000_000, &[], enc)
        .expect("encrypted read");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].values, vec![(0, TypedValue::F64(1.5))]);
}

// ─── 2. 损坏 footer → 必须 Err，不得 panic ────────────────────────────────

/// 辅助：把某文件 footer 的指定字节区间覆写后写回，返回临时路径。
fn corrupt_footer_range(path: &Path, offset_from_footer_start: usize, bytes: &[u8]) {
    let mut raw = std::fs::read(path).unwrap();
    let len = raw.len();
    let base = len - SAC_FOOTER_SIZE;
    raw[base + offset_from_footer_start..base + offset_from_footer_start + bytes.len()]
        .copy_from_slice(bytes);
    std::fs::write(path, &raw).unwrap();
}

/// 合法 footer 布局（SAC v9 64B）：
///   table_count        @ 20..24 (u32)
///   l2_offset          @ 24..32 (u64)
///   table_index_offset @ 32..40 (u64)
///   table_count_mirror @ 44..48 (u32)

#[test]
fn qa_mmap_corrupted_footer_huge_table_count_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[(10, &[100, 200], &[1.0, 2.0])],
    );

    // 同时把 table_count 与 mirror 都设为 u32::MAX（镜像一致，绕开 from_bytes 的镜像校验，
    // 专门打到 open_sac_file 的边界校验：table_index_size 巨大 → table_index_end > file_len）。
    corrupt_footer_range(&path, 20, &u32::MAX.to_le_bytes());
    corrupt_footer_range(&path, 44, &u32::MAX.to_le_bytes());

    let result = SacReader::read_range(&path, TenantId::DEFAULT, 10, 0, 1000, &[], None);
    assert!(
        result.is_err(),
        "损坏 footer（table_count 巨大）必须返回 Err，不得 panic，got {:?}",
        result
    );
}

#[test]
fn qa_mmap_corrupted_footer_offset_oob_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[(10, &[100, 200], &[1.0, 2.0])],
    );

    // table_index_offset 指向 u64::MAX（越界），table_count=0（size=0）。
    corrupt_footer_range(&path, 32, &u64::MAX.to_le_bytes());

    let result = SacReader::read_range(&path, TenantId::DEFAULT, 10, 0, 1000, &[], None);
    assert!(
        result.is_err(),
        "损坏 footer（table_index_offset 越界）必须返回 Err，不得 panic，got {:?}",
        result
    );
}

#[test]
fn qa_mmap_corrupted_footer_l2_gt_table_index_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[(10, &[100, 200], &[1.0, 2.0])],
    );

    // l2_offset > table_index_offset（会使 l2_bytes() 切片 start > end → panic 风险）。
    let footer = read_footer(&path);
    let bogus_l2 = footer.table_index_offset + 100;
    corrupt_footer_range(&path, 24, &bogus_l2.to_le_bytes());

    let result = SacReader::read_range(&path, TenantId::DEFAULT, 10, 0, 1000, &[], None);
    assert!(
        result.is_err(),
        "损坏 footer（l2_offset > table_index_offset）必须返回 Err，不得 panic，got {:?}",
        result
    );
}

#[test]
fn qa_mmap_corrupted_footer_table_index_end_overflow_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[(10, &[100, 200], &[1.0, 2.0])],
    );

    // table_index_offset 接近 u64::MAX，table_count=1 → checked_add 溢出 → 应报 overflow 错。
    corrupt_footer_range(&path, 32, &(u64::MAX - 10).to_le_bytes());
    corrupt_footer_range(&path, 20, &1u32.to_le_bytes());
    corrupt_footer_range(&path, 44, &1u32.to_le_bytes());

    let result = SacReader::read_range(&path, TenantId::DEFAULT, 10, 0, 1000, &[], None);
    assert!(
        result.is_err(),
        "损坏 footer（table_index_end 溢出）必须返回 Err，不得 panic，got {:?}",
        result
    );
}

/// 🟨2 修复验证：损坏 footer（table_count 巨大）走 `list_table_ids`（分区恢复路径）
/// 必须返回 Err，不得触发 `vec![0u8; table_count * 48]` 的大分配/OOM，也不得 panic。
///
/// 与 `qa_mmap_corrupted_footer_huge_table_count_no_panic`（读路径）同构，但覆盖
/// `list_table_ids` 这条独立入口——它是分区恢复扫描的冷路径，之前未做边界校验。
#[test]
fn qa_mmap_list_table_ids_corrupted_footer_huge_table_count_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[(10, &[100, 200], &[1.0, 2.0])],
    );

    // 同时把 table_count 与 mirror 都设为 u32::MAX（镜像一致，绕开 from_bytes 的镜像校验），
    // 专门打到 list_table_ids 的边界校验：table_index_size ≈ 206GB → table_index_end > file_len。
    corrupt_footer_range(&path, 20, &u32::MAX.to_le_bytes());
    corrupt_footer_range(&path, 44, &u32::MAX.to_le_bytes());

    let result = SacReader::list_table_ids(&path);
    assert!(
        result.is_err(),
        "损坏 footer（table_count 巨大）list_table_ids 必须返回 Err，不得 panic/OOM，got {:?}",
        result
    );
}

/// 🟨2 固化：损坏 footer（table_index_offset 越界 > file_len）走 `list_table_ids`
/// 必须返回 Err，不得 panic。
#[test]
fn qa_mmap_list_table_ids_corrupted_footer_offset_oob_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[(10, &[100, 200], &[1.0, 2.0])],
    );

    // table_index_offset 指向 u64::MAX（越界），table_count=0（size=0）。
    corrupt_footer_range(&path, 32, &u64::MAX.to_le_bytes());

    let result = SacReader::list_table_ids(&path);
    assert!(
        result.is_err(),
        "损坏 footer（table_index_offset 越界）list_table_ids 必须返回 Err，不得 panic，got {:?}",
        result
    );
}

/// 🟨2 固化：损坏 footer（l2_offset > table_index_offset）走 `list_table_ids`
/// 必须返回 Err，不得 panic（防 TableIndex 区切片 start > end 的 panic 风险）。
#[test]
fn qa_mmap_list_table_ids_corrupted_footer_l2_gt_table_index_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[(10, &[100, 200], &[1.0, 2.0])],
    );

    // l2_offset > table_index_offset。
    let footer = read_footer(&path);
    let bogus_l2 = footer.table_index_offset + 100;
    corrupt_footer_range(&path, 24, &bogus_l2.to_le_bytes());

    let result = SacReader::list_table_ids(&path);
    assert!(
        result.is_err(),
        "损坏 footer（l2_offset > table_index_offset）list_table_ids 必须返回 Err，不得 panic，got {:?}",
        result
    );
}

/// 🟨2 固化：损坏 footer（table_index_offset 接近 u64::MAX，table_count>0 → checked_add 溢出）
/// 走 `list_table_ids` 必须返回 Err，不得 panic。
#[test]
fn qa_mmap_list_table_ids_corrupted_footer_table_index_end_overflow_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[(10, &[100, 200], &[1.0, 2.0])],
    );

    // table_index_offset 接近 u64::MAX，table_count=1 → checked_add 溢出 → 应报 overflow 错。
    corrupt_footer_range(&path, 32, &(u64::MAX - 10).to_le_bytes());
    corrupt_footer_range(&path, 20, &1u32.to_le_bytes());
    corrupt_footer_range(&path, 44, &1u32.to_le_bytes());

    let result = SacReader::list_table_ids(&path);
    assert!(
        result.is_err(),
        "损坏 footer（table_index_end 溢出）list_table_ids 必须返回 Err，不得 panic，got {:?}",
        result
    );
}

// ─── 3. 空文件 / 空表 / 空 L2 ────────────────────────────────────────────

#[test]
fn qa_mmap_empty_file_zero_tables() {
    let tmp = tempfile::TempDir::new().unwrap();
    let writer = SacWriter::new(&tmp.path().join("sac"), CompressionType::Lz4);
    // begin_write + 不 append + finish → table_count=0 / block_count=0 / 空 TableIndex / 空 L2。
    let path = writer
        .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
        .expect("begin_write")
        .finish()
        .expect("finish");

    // 空表：读任意表 → 空结果（binary_search 空 TableIndex → None）。
    let rows = SacReader::read_range(&path, TenantId::DEFAULT, 1, 0, 1_000_000, &[], None)
        .expect("empty file read");
    assert!(rows.is_empty(), "空文件读任何表都应返回空，got {} 行", rows.len());

    // list_table_ids 也应返回空（分区恢复路径）。
    let ids = SacReader::list_table_ids(&path).expect("list_table_ids empty");
    assert!(ids.is_empty(), "空文件 list_table_ids 应返回空，got {ids:?}");
}

// ─── 4. first_l2_idx + block_count 边界（不 panic，语义与旧一致） ─────────

#[test]
fn qa_mmap_boundary_first_l2_idx_over_l2_count_no_panic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[
            (10, &[100, 200], &[1.0, 2.0]),
            (20, &[300, 400], &[3.0, 4.0]),
            (30, &[500], &[5.0]),
        ],
    );
    let footer = read_footer(&path);
    assert_eq!(footer.table_count, 3);
    assert_eq!(footer.block_count, 3); // l2_count == 3

    // 破坏表 10 的 TableIndex 条目的 first_l2_idx 为 u32::MAX，block_count=1。
    // start=u32::MAX, end=min(u32::MAX+1, l2_count=3)=3 → start>end → 空区间，不 panic。
    let raw = std::fs::read(&path).unwrap();
    let entry0_off = footer.table_index_offset as usize;
    let mut corrupted = raw.clone();
    corrupted[entry0_off + 24..entry0_off + 28].copy_from_slice(&u32::MAX.to_le_bytes());
    corrupted[entry0_off + 4..entry0_off + 8].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(&path, &corrupted).unwrap();

    let rows = SacReader::read_range(&path, TenantId::DEFAULT, 10, 0, 1_000_000, &[], None)
        .expect("first_l2_idx 越界读必须不 panic");
    // 空区间 → 空结果（与旧实现 min 语义一致：不会迭代到越界下标）。
    assert!(rows.is_empty(), "first_l2_idx 越界应返回空，got {} 行", rows.len());
}

#[test]
fn qa_mmap_boundary_block_count_clamped_to_l2_count() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = write_multi_table(
        &tmp.path().join("sac"),
        &[
            (10, &[100, 200], &[1.0, 2.0]),
            (20, &[300, 400], &[3.0, 4.0]),
            (30, &[500], &[5.0]),
        ],
    );
    let footer = read_footer(&path);
    assert_eq!(footer.block_count, 3); // l2_count == 3

    // 破坏表 20 的 TableIndex 条目 block_count 为 100（first_l2_idx=1）。
    // end = min(1 + 100, 3) = 3 → 读取 L2[1..3]（含表 30 的块）。
    // 关键断言：不 panic，且因 min(l2_count) 钳制不越界读 L2。
    let raw = std::fs::read(&path).unwrap();
    let mut corrupted = raw.clone();
    // 表 20 是第二条 TableIndex（每表 1 块），first_l2_idx=1。
    let entry1_off = footer.table_index_offset as usize + SAC_TABLE_INDEX_ENTRY_SIZE;
    corrupted[entry1_off + 4..entry1_off + 8].copy_from_slice(&100u32.to_le_bytes());
    std::fs::write(&path, &corrupted).unwrap();

    // 读表 20：block_count 被钳制到 l2_count，不得 panic（结果可能含相邻块，属损坏文件行为）。
    let rows = SacReader::read_range(&path, TenantId::DEFAULT, 20, 0, 1_000_000, &[], None)
        .expect("block_count 钳制读必须不 panic");
    assert!(!rows.is_empty(), "钳制后应至少读到表 20 自己的块");
}

// ─── 5. Windows 全文件 mmap 与并发 rename/remove 交互 ────────────────────

/// 实证 Windows 上「文件被 mmap 持有期间，rename / remove_file」的行为。
///
/// 对应两类并发替换场景：
///   - TTL purge 用 `rename(.sac → .dead)`（purge.rs:213）；
///   - Compaction 用 `remove_file`（scheduler.rs:764）。
///
/// 关键安全属性（平台无关）：
///   (a) rename/remove 要么干净成功、要么干净 Err，绝不 crash/UB；
///   (b) remove_file 成功后 mmap 仍能读到一致旧内容（Unix unlink 等价语义，
///       或 Windows delete-pending 语义），不会读到被截断/乱码的数据；
///   (c) drop mmap 后无泄漏（文件已被删或可被删）。
/// 精确验证「读旧文件 vs 读到被替换的文件」：mmap 绑定文件对象（非路径），
/// 文件被 rename 走、同路径写入新文件后，mmap 仍必须读到**旧**内容，而非新文件内容。
#[test]
fn qa_mmap_reads_old_file_not_replaced_file() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("victim.sac");

    // 旧文件内容 = 0xAA。
    std::fs::write(&path, vec![0xAAu8; 4096]).unwrap();
    let file = File::open(&path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file) }.unwrap();
    assert_eq!(mmap[0], 0xAA);

    // 模拟 compaction/替换：把旧文件 rename 走。
    let old_path = tmp.path().join("victim.old.sac");
    std::fs::rename(&path, &old_path).expect("rename away old file");
    // 同路径写一个**新**文件（内容 = 0xBB）。
    std::fs::write(&path, vec![0xBBu8; 4096]).unwrap();

    // 关键断言：mmap 仍读到旧内容 0xAA（绑定旧文件对象），绝不读到新文件 0xBB。
    assert_eq!(
        mmap[0], 0xAA,
        "mmap 必须读旧文件内容，不得读到同路径被替换后的新文件内容"
    );
    assert_eq!(
        std::fs::read(&path).unwrap()[0],
        0xBB,
        "新文件内容应为 0xBB（证明替换确已发生，而非测试构造错误）"
    );

    drop(mmap);
    drop(file);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&old_path);
}

#[test]
fn qa_mmap_windows_concurrent_remove_rename_behavior() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("victim.sac");
    std::fs::write(&path, vec![0xAAu8; 4096]).unwrap();

    let file = File::open(&path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file) }.unwrap();
    // 强制触碰一页，确保映射确实生效。
    assert_eq!(mmap[0], 0xAA);

    // (a) 持有 mmap 期间 rename（TTL purge 路径）。
    let renamed_path = tmp.path().join("victim.renamed");
    let rename_while_mapped = std::fs::rename(&path, &renamed_path);
    eprintln!(
        "QA-CONCURRENCY-OBSERVE: rename_while_mapped={:?}",
        rename_while_mapped.as_ref().map(|_| "Ok".to_string()).unwrap_or_else(|e| format!("Err({e:?})"))
    );

    // (b) 持有 mmap 期间 remove_file（compaction 删除路径）。仅当 rename 未生效时才有原文件可删。
    let remove_while_mapped = if path.exists() {
        std::fs::remove_file(&path)
    } else {
        // rename 已成功（非 Windows 典型行为），则删 renamed。
        std::fs::remove_file(&renamed_path)
    };
    eprintln!(
        "QA-CONCURRENCY-OBSERVE: remove_while_mapped={:?}",
        remove_while_mapped.as_ref().map(|_| "Ok".to_string()).unwrap_or_else(|e| format!("Err({e:?})"))
    );

    // (c) remove_file 成功后，mmap 必须仍读到一致旧内容（不读乱码/不 SIGBUS 等价崩溃）。
    // 若 remove 成功，验证映射仍返回 0xAA；若 remove 失败（文件仍在），同样应读到 0xAA。
    assert_eq!(
        mmap[0], 0xAA,
        "remove/rename 期间 mmap 必须保持读到一致的旧内容"
    );

    // (d) drop mmap 后无永久泄漏：无论 (a)(b) 结果，两个候选路径最终都应可清理/已不存在。
    drop(mmap);
    drop(file);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&renamed_path);
    assert!(!path.exists() && !renamed_path.exists(), "drop 后文件应已清理或可清理");
}

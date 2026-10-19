//! SAC 读取器：Footer → TableIndex → L2 → Block 偏移链定位 → 列投影 → 解码。
//!
//! SacReader 负责从 SAC 文件读取指定时间范围的数据（SAC v9 定稿）。
//! 读取流程：
//! 1. 读取 Header（验证 magic）
//! 2. 读取 Footer（定位 L2/TableIndex 区，校验 block/table 计数镜像）
//! 3. 读取 TableIndex（按 table_id 二分定位候选 L2 下标范围）
//! 4. 读取 L2 描述符（`block_offset/block_size` 定位与跳 Block）
//! 5. 读取 Block 内 Stream Directory → 按目录取各流 → 解压 → 按 tag 类型化解码
//! 6. 每 Block 尾部 CRC32C 校验，失败则 `block_offset+block_size` 跳过 + 记 warning

use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use memmap2::MmapOptions;

use sensearcdb_common::{
    constants::{SAC_FOOTER_SIZE, SAC_HEADER_SIZE, SAC_L2_ENTRY_SIZE, SAC_TABLE_INDEX_ENTRY_SIZE},
    SenseArcError, TenantId,
};

use crate::compression::{decompress, CompressionType};
use crate::encoding::{
    dictionary::DictionaryDecoder, dod::DoDDecoder, gorilla::GorillaDecoder,
    gorilla::GorillaF32Decoder, rle::RleDecoder, simple8b, simple8b::Simple8bDecoder, Decoder,
    VALUE_TAG_BASE_MASK, VALUE_TAG_BOOL_RLE, VALUE_TAG_DICTIONARY, VALUE_TAG_GORILLA,
    VALUE_TAG_GORILLA_F32, VALUE_TAG_NULL_BITMAP, VALUE_TAG_SIMPLE8B, VALUE_TAG_SIMPLE8B_ENUM16,
    VALUE_TAG_SIMPLE8B_I32, VALUE_TAG_SIMPLE8B_U64,
};
use crate::encryption::{decrypt_with, EncryptContext, EncryptError, Encryptor, SharedEncryptor};
use crate::memtable::col_vec::TypedValue;
use crate::sac::block::{
    BlockMiniHeader, StreamDirectory, BLOCK_CRC_SIZE, BLOCK_MINI_HEADER_SIZE, STREAM_DIR_ENTRY_SIZE,
};
use crate::sac::footer::SacFooter;
use crate::sac::header::SacHeader;
use crate::sac::l2_descriptor::L2Descriptor;
use crate::sac::table_index::{binary_search_table_index, TableIndexEntry};

/// 从 SAC 文件读取的单行数据。
#[derive(Debug, Clone, PartialEq)]
pub struct SacRow {
    /// 表 ID（v9 多表聚合，块自描述）。
    pub table_id: u32,
    /// 时间戳（epoch 纳秒）。
    pub ts: i64,
    /// 各列的值（column_id → `TypedValue`，类型化，String 列返回原串）。
    pub values: Vec<(u16, TypedValue)>,
    /// 各列的质量码（column_id → u8）。
    pub qualities: Vec<(u16, u8)>,
    /// 本行所属 Block 的最大 WAL 序列号（Block mini-header 的 `max_wal_seq`）。
    ///
    /// 用于 Compaction 去重优先级（merge.rs 硬规则第 1 条）。
    /// `None` = Compaction / 回放 / 批量导入产出，无 WAL seq 可比较。
    pub wal_seq: Option<u64>,
}

/// SAC 读取器。
///
/// 支持按时间范围和列投影读取 SAC 文件数据。
pub struct SacReader;

impl SacReader {
    /// 读取 SAC 文件头（32B），供查询按 `write_epoch` 排序（跨文件 LWW）。
    ///
    /// # 参数
    /// - `path`: SAC 文件路径。
    ///
    /// # 返回值
    /// 解析后的 `SacHeader`；magic 不匹配或读取失败返回 `Err`。
    pub fn read_header(path: &Path) -> Result<SacHeader, SenseArcError> {
        let mut file =
            File::open(path).map_err(|e| SenseArcError::SacIoError(format!("open: {}", e)))?;
        let mut header_buf = [0u8; SAC_HEADER_SIZE];
        file.read_exact(&mut header_buf)
            .map_err(|e| SenseArcError::SacIoError(format!("read header: {}", e)))?;
        SacHeader::from_bytes(&header_buf)
            .ok_or_else(|| SenseArcError::SacIoError("bad header magic".into()))
    }

    /// 读取 SAC 文件的全部 L2 描述符（按 `(table_id, min_ts)` 升序）。
    ///
    /// 供 block 级拼接归并（刀3）读取各 Block 的 `table_id/block_offset/block_size/
    /// min_ts/max_ts/column_count` 元数据。只读 header + footer + L2 区，不触碰 Block 数据。
    pub fn read_l2_descriptors(path: &Path) -> Result<Vec<L2Descriptor>, SenseArcError> {
        let mut file =
            File::open(path).map_err(|e| SenseArcError::SacIoError(format!("open: {}", e)))?;
        let file_len = file
            .metadata()
            .map_err(|e| SenseArcError::SacIoError(format!("metadata: {}", e)))?
            .len();
        if file_len < (SAC_HEADER_SIZE + SAC_FOOTER_SIZE) as u64 {
            return Err(SenseArcError::SacIoError("file too small".into()));
        }

        // Header（校验 magic）
        let mut header_buf = [0u8; SAC_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))
            .map_err(|e| SenseArcError::SacIoError(format!("seek header: {}", e)))?;
        file.read_exact(&mut header_buf)
            .map_err(|e| SenseArcError::SacIoError(format!("read header: {}", e)))?;
        SacHeader::from_bytes(&header_buf)
            .ok_or_else(|| SenseArcError::SacIoError("bad header magic".into()))?;

        // Footer（定位 L2 区）
        let footer_offset = file_len - SAC_FOOTER_SIZE as u64;
        let mut footer_buf = [0u8; SAC_FOOTER_SIZE];
        file.seek(SeekFrom::Start(footer_offset))
            .map_err(|e| SenseArcError::SacIoError(format!("seek footer: {}", e)))?;
        file.read_exact(&mut footer_buf)
            .map_err(|e| SenseArcError::SacIoError(format!("read footer: {}", e)))?;
        let footer = SacFooter::from_bytes(&footer_buf)
            .ok_or_else(|| SenseArcError::SacIoError("bad footer".into()))?;

        // 元数据区边界校验（防损坏 footer 越界导致 panic）。
        if footer.l2_offset > footer.table_index_offset || footer.table_index_offset > file_len {
            return Err(SenseArcError::SacIoError("bad metadata region".into()));
        }
        let l2_size = footer.table_index_offset - footer.l2_offset;
        let l2_count = (l2_size / SAC_L2_ENTRY_SIZE as u64) as usize;

        file.seek(SeekFrom::Start(footer.l2_offset))
            .map_err(|e| SenseArcError::SacIoError(format!("seek l2: {}", e)))?;
        let mut entries = Vec::with_capacity(l2_count);
        for _ in 0..l2_count {
            let mut l2_buf = [0u8; SAC_L2_ENTRY_SIZE];
            file.read_exact(&mut l2_buf)
                .map_err(|e| SenseArcError::SacIoError(format!("read l2 entry: {}", e)))?;
            let l2 = L2Descriptor::from_bytes(&l2_buf)
                .ok_or_else(|| SenseArcError::SacIoError("bad l2 entry".into()))?;
            entries.push(l2);
        }
        Ok(entries)
    }

    /// 读取单个 Block 的原始字节（含 mini-header/目录/流/尾部 CRC，密文原样返回）。
    ///
    /// 供 block 级拼接归并（刀3）复制非重叠 Block：逐字节复制，不解压/不重编码/不重压。
    /// Block 尾部 CRC32C 自包含，逐字节复制后仍有效。
    pub fn read_block_bytes(
        path: &Path,
        block_offset: u64,
        block_size: u32,
    ) -> Result<Vec<u8>, SenseArcError> {
        let mut file =
            File::open(path).map_err(|e| SenseArcError::SacIoError(format!("open: {}", e)))?;
        file.seek(SeekFrom::Start(block_offset))
            .map_err(|e| SenseArcError::SacIoError(format!("seek block: {}", e)))?;
        let mut bytes = vec![0u8; block_size as usize];
        file.read_exact(&mut bytes)
            .map_err(|e| SenseArcError::SacIoError(format!("read block bytes: {}", e)))?;
        Ok(bytes)
    }

    /// 读取 SAC 文件的 TableIndex，返回文件内包含的 table_id 升序列表。
    ///
    /// 供分区恢复（`sensearcdb-partition` 的 `discover_partitions_from_disk`）使用：
    /// v9 多表聚合后 table_id 不再出现在目录/文件名，只能从 TableIndex 反查。
    /// 仅读 header + footer + TableIndex，不触碰 Block 数据区。
    ///
    /// # 参数
    /// - `path`: SAC 文件路径。
    ///
    /// # 返回值
    /// 按升序排列的 table_id 列表；文件损坏或格式不符返回 `Err`。
    pub fn list_table_ids(path: &Path) -> Result<Vec<u32>, SenseArcError> {
        let file =
            File::open(path).map_err(|e| SenseArcError::SacIoError(format!("open: {}", e)))?;

        let file_len = file
            .metadata()
            .map_err(|e| SenseArcError::SacIoError(format!("metadata: {}", e)))?
            .len();

        if file_len < (SAC_HEADER_SIZE + SAC_FOOTER_SIZE) as u64 {
            return Err(SenseArcError::SacIoError("file too small".into()));
        }

        // 1. mmap 全文件：TableIndex 是明文元数据（TDE 只加密 Block 数据），
        //    页按需 page fault，不再 seek + read_exact 全量读 4.8MB 到堆。
        //    SAFETY：文件只读打开且 `file` 存活到本函数结束（不先 drop）；
        //    SAC 文件不可变（落盘后不再写入/截断），映射期间无并发修改。
        let mmap = unsafe { MmapOptions::new().map(&file) }
            .map_err(|e| SenseArcError::SacIoError(format!("mmap: {}", e)))?;

        // 2. Header 直接从 mmap 前 32B 读取并校验 magic（与 `open_sac_file` 同口径）。
        let header_arr: &[u8; SAC_HEADER_SIZE] = mmap[..SAC_HEADER_SIZE]
            .try_into()
            .map_err(|_| SenseArcError::SacIoError("header too short".into()))?;
        SacHeader::from_bytes(header_arr)
            .ok_or_else(|| SenseArcError::SacIoError("bad header magic".into()))?;

        // 3. Footer 直接从 mmap 尾部 64B 读取（定位 TableIndex 区）。
        let footer_start = file_len as usize - SAC_FOOTER_SIZE;
        let footer_arr: &[u8; SAC_FOOTER_SIZE] = mmap[footer_start..]
            .try_into()
            .map_err(|_| SenseArcError::SacIoError("footer too short".into()))?;
        let footer = SacFooter::from_bytes(footer_arr)
            .ok_or_else(|| SenseArcError::SacIoError("bad footer".into()))?;

        // 4. 校验 TableIndex 区边界（对齐 `open_sac_file`，防损坏 footer 越界/大分配 OOM）。
        let table_index_size =
            (footer.table_count as u64).saturating_mul(SAC_TABLE_INDEX_ENTRY_SIZE as u64);
        let table_index_end = footer
            .table_index_offset
            .checked_add(table_index_size)
            .ok_or_else(|| SenseArcError::SacIoError("table index size overflow".into()))?;
        if footer.l2_offset > footer.table_index_offset || table_index_end > file_len {
            return Err(SenseArcError::SacIoError("bad metadata region".into()));
        }

        // 5. TableIndex 区切片，逐条提取 table_id（读的是与原版同一批字节）。
        let table_index = &mmap[footer.table_index_offset as usize..table_index_end as usize];
        let mut table_ids = Vec::with_capacity(footer.table_count as usize);
        for idx in 0..footer.table_count as usize {
            let entry = read_table_index_entry(table_index, idx)
                .ok_or_else(|| SenseArcError::SacIoError("bad table index entry".into()))?;
            table_ids.push(entry.table_id);
        }
        Ok(table_ids)
    }

    /// 从 SAC 文件读取指定 table 在时间范围内的数据。
    ///
    /// # 参数
    /// - `path`: SAC 文件路径。
    /// - `tenant_id`: 请求租户 ID。读 header 后校验 `header.tenant_id == tenant_id`
    ///   且路径 tenant 目录段 == tenant_id；任一不符返回统一「不存在/损坏」错误
    ///   （`SacIoError`，与 `bad header magic` 同变体，不泄露文件存在性）。
    /// - `table_id`: 目标表 ID。
    /// - `ts_start`: 查询起始时间戳（epoch 纳秒）。
    /// - `ts_end`: 查询结束时间戳（epoch 纳秒）。
    /// - `column_ids`: 需要投影的列 ID 列表（空表示所有列）。
    /// - `encryptor`: 可选加密器（TDE 关闭时为 `None`）。
    ///
    /// # 返回值
    /// 按时间戳排序的行列表。单 Block CRC 校验失败时跳过该 Block（不返回 Err）。
    pub fn read_range(
        path: &Path,
        tenant_id: TenantId,
        table_id: u32,
        ts_start: i64,
        ts_end: i64,
        column_ids: &[u16],
        encryptor: SharedEncryptor,
    ) -> Result<Vec<SacRow>, SenseArcError> {
        let mut state = open_sac_file(
            path,
            tenant_id,
            Some(table_id),
            ts_start,
            ts_end,
            &encryptor,
        )?;
        let want_all = column_ids.is_empty();
        // D6：按文件层级决定块级压缩算法（L0=LZ4，L1/L2/L3/L4=ZSTD9），写读一致。
        let compression = state.header.level.compression();

        // 5. 读取匹配的 Blocks 并解码
        let mut rows: Vec<SacRow> = Vec::new();

        for l2_idx in &state.candidate_l2 {
            let l2 = state
                .l2_entry(*l2_idx)
                .ok_or_else(|| SenseArcError::SacIoError("bad l2 entry".into()))?;
            if !l2.overlaps(ts_start, ts_end) {
                continue;
            }

            match read_block(
                &mut state.file,
                l2.block_offset,
                l2.block_size,
                ts_start,
                ts_end,
                column_ids,
                want_all,
                encryptor.as_deref(),
                state.header.tde_encrypted,
                state.header.dek_version,
                tenant_id,
                l2.table_id as u64,
                *l2_idx as u64,
                compression,
            )? {
                Some(rows_in_block) => rows.extend(rows_in_block),
                None => {
                    // S6：CRC 失败，跳过该 Block（seek 到 block_offset + block_size 已由 read_block 完成）
                    tracing::warn!(
                        block_offset = l2.block_offset,
                        block_size = l2.block_size,
                        "SAC block CRC mismatch; skipping block"
                    );
                }
            }
        }

        // 保证按时间戳排序
        rows.sort_by_key(|r| r.ts);
        Ok(rows)
    }
}

/// 已打开并解析好 Header/Footer/TableIndex/L2 的 SAC 文件（待按 Block 产出行）。
///
/// 供 [`SacReader::read_range`]（单表全量读）与 [`SacRowIter`]（流式读）共用，
/// 保证两条路径的租户校验、候选 L2 定位、层级压缩判定完全一致。
struct OpenSacFile {
    /// 文件句柄（Block 读取用，非 Clone）。
    file: File,
    /// 全文件 mmap：TableIndex / L2 明文元数据按需 page fault（O(log N) 只 touch 少量页），
    /// Block 数据仍走 `read_block` 的 read_exact + 解密路径，不通过 mmap 读明文。
    mmap: memmap2::Mmap,
    /// L2 描述符区在文件内的字节偏移。
    l2_offset: u64,
    /// TableIndex 区在文件内的字节偏移（= L2 区终点）。
    table_index_offset: u64,
    /// 与查询范围重叠的候选 L2 下标（升序）。
    candidate_l2: Vec<usize>,
    /// 已解析的 Header（含 level/tenant/dek_version/TDE 标志）。
    header: SacHeader,
}

impl OpenSacFile {
    /// L2 描述符区原始字节切片。
    #[inline]
    fn l2_bytes(&self) -> &[u8] {
        let start = self.l2_offset as usize;
        let end = self.table_index_offset as usize;
        &self.mmap[start..end]
    }

    /// 按下标惰性解析第 `idx` 条 L2 描述符（单条 O(1)，不触发全量 L2 解析）。
    #[inline]
    fn l2_entry(&self, idx: usize) -> Option<L2Descriptor> {
        let start = idx * SAC_L2_ENTRY_SIZE;
        let end = start + SAC_L2_ENTRY_SIZE;
        let arr: &[u8; SAC_L2_ENTRY_SIZE] = self.l2_bytes().get(start..end)?.try_into().ok()?;
        L2Descriptor::from_bytes(arr)
    }
}

/// 打开 SAC 文件并解析 Header/Footer/TableIndex/L2 + 候选 L2（`read_range` 的前 6 步）。
///
/// `table_id`：`Some(tid)` 只定位单表（TableIndex 二分）；`None` 读全表（compaction）。
#[allow(clippy::too_many_arguments)]
fn open_sac_file(
    path: &Path,
    tenant_id: TenantId,
    table_id: Option<u32>,
    ts_start: i64,
    ts_end: i64,
    encryptor: &SharedEncryptor,
) -> Result<OpenSacFile, SenseArcError> {
    let mut file =
        File::open(path).map_err(|e| SenseArcError::SacIoError(format!("open: {}", e)))?;

    let file_len = file
        .metadata()
        .map_err(|e| SenseArcError::SacIoError(format!("metadata: {}", e)))?
        .len();

    if file_len < (SAC_HEADER_SIZE + SAC_FOOTER_SIZE) as u64 {
        return Err(SenseArcError::SacIoError("file too small".into()));
    }

    // 1. 读取 Header（验证 magic）
    let mut header_buf = [0u8; SAC_HEADER_SIZE];
    file.seek(SeekFrom::Start(0))
        .map_err(|e| SenseArcError::SacIoError(format!("seek header: {}", e)))?;
    file.read_exact(&mut header_buf)
        .map_err(|e| SenseArcError::SacIoError(format!("read header: {}", e)))?;
    let header = SacHeader::from_bytes(&header_buf)
        .ok_or_else(|| SenseArcError::SacIoError("bad header magic".into()))?;

    // S2：tenant 校验（数据面隔离，不泄露文件存在性——统一「不存在/损坏」错误）。
    // header 与路径目录段双重一致校验：任一不符即拒绝，避免「错租户文件混入」。
    if header.tenant_id != tenant_id.as_u32() {
        return Err(SenseArcError::SacIoError("tenant mismatch".into()));
    }
    if path_tenant_id(path) != Some(tenant_id.as_u32()) {
        return Err(SenseArcError::SacIoError("tenant mismatch".into()));
    }

    // TDE：密文文件无密钥必须硬报错，绝不把密文当明文返回（O-1 硬约束）。
    if header.tde_encrypted && encryptor.is_none() {
        return Err(EncryptError::NoEncryptor.into());
    }

    // 2. 读取 Footer
    let footer_offset = file_len - SAC_FOOTER_SIZE as u64;
    let mut footer_buf = [0u8; SAC_FOOTER_SIZE];
    file.seek(SeekFrom::Start(footer_offset))
        .map_err(|e| SenseArcError::SacIoError(format!("seek footer: {}", e)))?;
    file.read_exact(&mut footer_buf)
        .map_err(|e| SenseArcError::SacIoError(format!("read footer: {}", e)))?;
    let footer = SacFooter::from_bytes(&footer_buf)
        .ok_or_else(|| SenseArcError::SacIoError("bad footer".into()))?;

    // 3. 校验元数据区边界（防损坏 footer 的越界 offset 导致后续切片 panic）。
    let table_index_size =
        (footer.table_count as u64).saturating_mul(SAC_TABLE_INDEX_ENTRY_SIZE as u64);
    let table_index_end = footer
        .table_index_offset
        .checked_add(table_index_size)
        .ok_or_else(|| SenseArcError::SacIoError("table index size overflow".into()))?;
    if footer.l2_offset > footer.table_index_offset || table_index_end > file_len {
        return Err(SenseArcError::SacIoError("bad metadata region".into()));
    }
    let l2_size = footer.table_index_offset - footer.l2_offset;
    let l2_count = (l2_size / SAC_L2_ENTRY_SIZE as u64) as usize;

    // 4. mmap 全文件：TableIndex / L2 是明文元数据（TDE 只加密 Block 数据），
    //    页按需 page fault；单表二分只 touch O(log N) 页，不再全量读 9.6MB/11MB。
    //    SAFETY：文件只读打开且本结构同时持有 `file`（不先 drop）；SAC 文件不可变，
    //    映射期间无写入/截断。Block 数据仍走下方 `read_block` 的 read_exact + 解密路径。
    let mmap = unsafe { MmapOptions::new().map(&file) }
        .map_err(|e| SenseArcError::SacIoError(format!("mmap: {}", e)))?;

    // 5. 候选 L2 下标：单表 = TableIndex 二分 + 表内 L2 区间；全表 = 全部 L2。
    //    语义与原实现完全一致，仅输入从 `Vec<u8>` 换成 mmap 的 `&[u8]`。
    let table_index = &mmap[footer.table_index_offset as usize..table_index_end as usize];
    let mut candidate_l2: Vec<usize> = Vec::new();
    match table_id {
        Some(tid) => {
            if let Some(idx) = binary_search_table_index(table_index, tid) {
                let entry = read_table_index_entry(table_index, idx)
                    .ok_or_else(|| SenseArcError::SacIoError("bad table index entry".into()))?;
                let start = entry.first_l2_idx as usize;
                let end = (entry.first_l2_idx as usize + entry.block_count as usize).min(l2_count);
                for i in start..end {
                    candidate_l2.push(i);
                }
            }
        }
        None => {
            candidate_l2.extend(0..l2_count);
        }
    }

    Ok(OpenSacFile {
        file,
        mmap,
        l2_offset: footer.l2_offset,
        table_index_offset: footer.table_index_offset,
        candidate_l2,
        header,
    })
}

/// 从 TableIndex 字节数组中读取第 `idx` 条条目。
fn read_table_index_entry(bytes: &[u8], idx: usize) -> Option<TableIndexEntry> {
    let start = idx * SAC_TABLE_INDEX_ENTRY_SIZE;
    let end = start + SAC_TABLE_INDEX_ENTRY_SIZE;
    let arr: &[u8; SAC_TABLE_INDEX_ENTRY_SIZE] = bytes.get(start..end)?.try_into().ok()?;
    TableIndexEntry::from_bytes(arr)
}

/// 流式读取单个 SAC 文件：按 Block 惰性产出行，内存 O(block)（不再全量累积）。
///
/// 供 Compaction 流式 merge 使用——k-way 堆里每个源文件只占 1 个元素 + 1 个
/// 当前 Block 的行缓冲，避免把整文件/整分区读进内存。
pub struct SacRowIter {
    /// 已打开的 SAC 文件读取态。
    state: OpenSacFile,
    /// 当前 Block 的行缓冲（`IntoIter`，逐行产出）。
    pending: std::vec::IntoIter<SacRow>,
    /// 候选 L2 数组游标。
    cursor: usize,
    /// 目标表 ID（`None` = 读全表，供 compaction 使用；`Some` = 单表过滤）。
    table_id: Option<u32>,
    /// 查询时间范围。
    ts_start: i64,
    ts_end: i64,
    /// 投影列（空 = 全列）。
    column_ids: Vec<u16>,
    want_all: bool,
    /// 加密器（可选）。
    encryptor: SharedEncryptor,
    /// 请求租户 ID。
    tenant_id: TenantId,
    /// 块级压缩算法（由 header.level 派生）。
    compression: CompressionType,
}

impl SacRowIter {
    /// 打开文件并构建流式迭代器（单表过滤，TableIndex 二分定位）。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: &Path,
        tenant_id: TenantId,
        table_id: u32,
        ts_start: i64,
        ts_end: i64,
        column_ids: &[u16],
        encryptor: SharedEncryptor,
    ) -> Result<Self, SenseArcError> {
        Self::new_inner(
            path,
            tenant_id,
            Some(table_id),
            ts_start,
            ts_end,
            column_ids,
            encryptor,
        )
    }

    /// 打开文件并构建流式迭代器（读全表，供 compaction k 路归并多表聚合文件）。
    #[allow(clippy::too_many_arguments)]
    pub fn new_all(
        path: &Path,
        tenant_id: TenantId,
        ts_start: i64,
        ts_end: i64,
        column_ids: &[u16],
        encryptor: SharedEncryptor,
    ) -> Result<Self, SenseArcError> {
        Self::new_inner(
            path, tenant_id, None, ts_start, ts_end, column_ids, encryptor,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        path: &Path,
        tenant_id: TenantId,
        table_id: Option<u32>,
        ts_start: i64,
        ts_end: i64,
        column_ids: &[u16],
        encryptor: SharedEncryptor,
    ) -> Result<Self, SenseArcError> {
        let state = open_sac_file(path, tenant_id, table_id, ts_start, ts_end, &encryptor)?;
        let want_all = column_ids.is_empty();
        let compression = state.header.level.compression();
        Ok(Self {
            state,
            pending: Vec::new().into_iter(),
            cursor: 0,
            table_id,
            ts_start,
            ts_end,
            column_ids: column_ids.to_vec(),
            want_all,
            encryptor,
            tenant_id,
            compression,
        })
    }
}

impl Iterator for SacRowIter {
    type Item = Result<SacRow, SenseArcError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.pending.next() {
                return Some(Ok(row));
            }

            // 定位下一个「非跳过」的候选 Block（单表已由 TableIndex 收敛，此处按时间过滤）。
            let l2_idx = loop {
                if self.cursor >= self.state.candidate_l2.len() {
                    return None;
                }
                let idx = self.state.candidate_l2[self.cursor];
                self.cursor += 1;
                let l2 = match self.state.l2_entry(idx) {
                    Some(l2) => l2,
                    None => {
                        return Some(Err(SenseArcError::SacIoError("bad l2 entry".into())));
                    }
                };
                if !l2.overlaps(self.ts_start, self.ts_end) {
                    continue;
                }
                break idx;
            };

            let l2 = match self.state.l2_entry(l2_idx) {
                Some(l2) => l2,
                None => return Some(Err(SenseArcError::SacIoError("bad l2 entry".into()))),
            };
            let (block_offset, block_size, block_table_id) =
                (l2.block_offset, l2.block_size, l2.table_id);

            match read_block(
                &mut self.state.file,
                block_offset,
                block_size,
                self.ts_start,
                self.ts_end,
                &self.column_ids,
                self.want_all,
                self.encryptor.as_deref(),
                self.state.header.tde_encrypted,
                self.state.header.dek_version,
                self.tenant_id,
                block_table_id as u64,
                l2_idx as u64,
                self.compression,
            ) {
                Ok(Some(rows)) => self.pending = rows.into_iter(),
                Ok(None) => {
                    // S6：CRC 失败，跳过该 Block（seek 已由 read_block 完成）。
                    tracing::warn!(
                        block_offset = block_offset,
                        block_size = block_size,
                        "SAC block CRC mismatch; skipping block"
                    );
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// 从 SAC 文件路径解析 tenant 目录段（`{sac_dir}/{tenant_id}/{partition_ns}_{level}_{seq}.sac`）。
///
/// 取路径倒数第 1 层目录名（tenant 目录）解析为 `u32`；解析失败返回 `None`。
fn path_tenant_id(path: &Path) -> Option<u32> {
    path.parent()?.file_name()?.to_str()?.parse::<u32>().ok()
}

/// 读取并解码单个 Block。
///
/// 返回 `Ok(None)` 表示该 Block CRC 校验失败（或数据不足）已跳过，不视为致命错误。
#[allow(clippy::too_many_arguments)]
fn read_block(
    file: &mut File,
    block_offset: u64,
    block_size: u32,
    ts_start: i64,
    ts_end: i64,
    column_ids: &[u16],
    want_all: bool,
    encryptor: Option<&dyn Encryptor>,
    tde_encrypted: bool,
    dek_version: u32,
    tenant_id: TenantId,
    file_id: u64,
    block_id: u64,
    compression: CompressionType,
) -> Result<Option<Vec<SacRow>>, SenseArcError> {
    if block_size < (BLOCK_MINI_HEADER_SIZE + BLOCK_CRC_SIZE) as u32 {
        // 数据不足，跳过并 seek 到下一 Block
        file.seek(SeekFrom::Start(block_offset + block_size as u64))
            .map_err(|e| SenseArcError::SacIoError(format!("seek block: {}", e)))?;
        return Ok(None);
    }

    file.seek(SeekFrom::Start(block_offset))
        .map_err(|e| SenseArcError::SacIoError(format!("seek block: {}", e)))?;

    let mut block_bytes = vec![0u8; block_size as usize];
    file.read_exact(&mut block_bytes)
        .map_err(|e| SenseArcError::SacIoError(format!("read block: {}", e)))?;

    // 读取 mini-header
    let mh_bytes: [u8; BLOCK_MINI_HEADER_SIZE] =
        block_bytes[..BLOCK_MINI_HEADER_SIZE]
            .try_into()
            .map_err(|_| SenseArcError::SacIoError("block too small for mini-header".into()))?;
    let header = match BlockMiniHeader::from_bytes(&mh_bytes) {
        Some(h) => h,
        None => {
            file.seek(SeekFrom::Start(block_offset + block_size as u64))
                .map_err(|e| SenseArcError::SacIoError(format!("seek block: {}", e)))?;
            return Ok(None);
        }
    };

    let column_count = header.column_count as usize;
    let dir_size = (column_count + 2) * STREAM_DIR_ENTRY_SIZE;

    if block_bytes.len() < BLOCK_MINI_HEADER_SIZE + dir_size + BLOCK_CRC_SIZE {
        file.seek(SeekFrom::Start(block_offset + block_size as u64))
            .map_err(|e| SenseArcError::SacIoError(format!("seek block: {}", e)))?;
        return Ok(None);
    }

    // Stream Directory（S4）
    let dir_bytes = &block_bytes[BLOCK_MINI_HEADER_SIZE..BLOCK_MINI_HEADER_SIZE + dir_size];
    let directory = match StreamDirectory::from_bytes(dir_bytes) {
        Some(d) => d,
        None => {
            file.seek(SeekFrom::Start(block_offset + block_size as u64))
                .map_err(|e| SenseArcError::SacIoError(format!("seek block: {}", e)))?;
            return Ok(None);
        }
    };

    // S6：尾部 CRC32C 校验（覆盖 mini-header + directory + 全部流，不含 CRC 自身）
    let body = &block_bytes[..block_bytes.len() - BLOCK_CRC_SIZE];
    let stored_crc = u32::from_le_bytes(
        block_bytes[block_bytes.len() - BLOCK_CRC_SIZE..]
            .try_into()
            .unwrap_or([0u8; 4]),
    );
    let mut hasher = crc32c::Crc32cHasher::new(0);
    hasher.write(body);
    let computed_crc = hasher.finish() as u32;

    if computed_crc != stored_crc {
        file.seek(SeekFrom::Start(block_offset + block_size as u64))
            .map_err(|e| SenseArcError::SacIoError(format!("seek block: {}", e)))?;
        return Ok(None);
    }

    // 解码时间戳流（entry[0]）——密文则先解密
    let ts_entry = directory.entries[0];
    let ts_slice = stream_slice(&block_bytes, ts_entry.offset, ts_entry.size)?;
    let ts_ctx = EncryptContext::sac_stream(tenant_id, file_id, block_id, 0, dek_version);
    let ts_plain = match decrypt_or_skip(
        file,
        block_offset,
        block_size,
        decrypt_with(encryptor, tde_encrypted, ts_slice, &ts_ctx),
    )? {
        Some(p) => p,
        None => return Ok(None),
    };
    let timestamps = DoDDecoder::decode(&decompress(&ts_plain, compression));

    // 解码质量码流（entry[N+1]，per-point 单质量流）
    let qual_entry = directory.entries[column_count + 1];
    let qual_slice = stream_slice(&block_bytes, qual_entry.offset, qual_entry.size)?;
    let qual_ctx = EncryptContext::sac_stream(
        tenant_id,
        file_id,
        block_id,
        column_count as u32 + 1,
        dek_version,
    );
    let qual_plain = match decrypt_or_skip(
        file,
        block_offset,
        block_size,
        decrypt_with(encryptor, tde_encrypted, qual_slice, &qual_ctx),
    )? {
        Some(p) => p,
        None => return Ok(None),
    };
    let qualities = RleDecoder::decode(&decompress(&qual_plain, compression));

    // 按列投影解码各 val 流（entry[1..=N]）。column_id 显式落盘（SAC v6），
    // 按 entry.column_id 匹配投影，不再 dense 位置推导（稀疏列安全）。
    let mut col_values: HashMap<u16, Vec<Option<TypedValue>>> = HashMap::new();
    for i in 1..=column_count {
        let col_id = directory.entries[i].column_id;
        let wanted = want_all || column_ids.contains(&col_id);
        if !wanted {
            continue;
        }

        let entry = directory.entries[i];
        let val_slice = stream_slice(&block_bytes, entry.offset, entry.size)?;
        if val_slice.is_empty() {
            continue;
        }
        // val 流布局：[tag u8][密文 blob 或 LZ4 payload]——tag 明文，只解密其后的 payload
        let tag = val_slice[0];
        // 加密上下文 stream id 保持「目录条目位置索引 i」（1..=N），与 column_id 解耦：
        // writer 用 enumerate_index+1 写、reader 用 i 读，两端一致（稀疏列不改变该语义）。
        let val_ctx =
            EncryptContext::sac_stream(tenant_id, file_id, block_id, i as u32, dek_version);
        let val_plain = match decrypt_or_skip(
            file,
            block_offset,
            block_size,
            decrypt_with(encryptor, tde_encrypted, &val_slice[1..], &val_ctx),
        )? {
            Some(p) => p,
            None => return Ok(None),
        };
        let payload = decompress(&val_plain, compression);
        let typed = decode_payload_typed(tag, &payload, header.row_count);
        col_values.insert(col_id, typed);
    }

    // 组装行
    let mut rows = Vec::with_capacity(timestamps.len());
    for (row_idx, &ts) in timestamps.iter().enumerate() {
        if ts < ts_start || ts > ts_end {
            continue;
        }

        let mut values = Vec::new();
        let mut quals = Vec::new();

        let wanted: Vec<u16> = if want_all {
            let mut ids: Vec<u16> = col_values.keys().copied().collect();
            ids.sort_unstable();
            ids
        } else {
            column_ids.to_vec()
        };

        for col_id in wanted {
            if let Some(col_vals) = col_values.get(&col_id) {
                // 稀疏列：null 位置解码为 `Some(None)`，此处省略（不 push）以保持
                // `SacRow.values` 的「null = 该 (col_id, value) 不出现」语义。
                if let Some(Some(v)) = col_vals.get(row_idx) {
                    values.push((col_id, v.clone()));
                }
            }
            if row_idx < qualities.len() {
                quals.push((col_id, qualities[row_idx]));
            }
        }

        rows.push(SacRow {
            table_id: header.table_id,
            ts,
            values,
            qualities: quals,
            wal_seq: header.max_wal_seq,
        });
    }

    Ok(Some(rows))
}

/// 单流解密：区分「无密钥硬报错」与「认证失败跳过 Block」两类错误。
///
/// - `Ok(Some(plain))`：解密成功。
/// - `Err(NoEncryptor)`：读到密文但未提供密钥——O-1 硬约束，原样上抛，绝不跳过。
/// - 其他解密失败（`AuthFailed` / 错误密钥 / 数据被篡改）：`warn` + seek 到下一
///   Block 起点 + 返回 `Ok(None)`，与同函数内 CRC 失败（S6）的 per-block 跳过语义对齐。
fn decrypt_or_skip(
    file: &mut File,
    block_offset: u64,
    block_size: u32,
    result: Result<Vec<u8>, EncryptError>,
) -> Result<Option<Vec<u8>>, SenseArcError> {
    match result {
        Ok(plain) => Ok(Some(plain)),
        Err(EncryptError::NoEncryptor) => Err(EncryptError::NoEncryptor.into()),
        Err(e) => {
            tracing::warn!(
                error = %e,
                block_offset = block_offset,
                block_size = block_size,
                "SAC block stream decrypt failed; skipping block"
            );
            file.seek(SeekFrom::Start(block_offset + block_size as u64))
                .map_err(|e| SenseArcError::SacIoError(format!("seek block: {}", e)))?;
            Ok(None)
        }
    }
}

/// 从 Block 字节中按目录条目取流切片（offset/size 相对 Block 起点）。
fn stream_slice<'a>(
    block_bytes: &'a [u8],
    offset: u32,
    size: u32,
) -> Result<&'a [u8], SenseArcError> {
    let start = offset as usize;
    let end = start + size as usize;
    if end > block_bytes.len() {
        return Err(SenseArcError::SacIoError(
            "stream out of block bounds".into(),
        ));
    }
    Ok(&block_bytes[start..end])
}

/// 按编码标签解码单个列的 payload 为类型化值（S5）。
///
/// 返回 `Vec<Option<TypedValue>>`：`Some(v)` 表示 present，`None` 表示该行该列 null。
/// String 列返回原字符串（不再 `parse::<f64>`）；F32 列用原生 f32 Gorilla 解码。
///
/// 若 `tag & 0x80` 置位，payload 头部携带 `ceil(row_count/8)` 字节 presence 位图，
/// 位图 bit=1 表示 present；否则整列为 dense，直接 `Some(...)` 包裹。
fn decode_payload_typed(tag: u8, payload: &[u8], row_count: u32) -> Vec<Option<TypedValue>> {
    let has_nulls = tag & VALUE_TAG_NULL_BITMAP != 0;
    let base_tag = tag & VALUE_TAG_BASE_MASK;

    // 拆位图与值 payload
    let (bitmap, values_payload) = if has_nulls {
        let n = ((row_count as usize) + 7) / 8;
        (&payload[..n], &payload[n..])
    } else {
        (&[][..], payload)
    };

    // 按 base_tag 解出 Vec<TypedValue>（原 8 分支，tag 换成 base_tag）
    let values: Vec<TypedValue> = decode_values(base_tag, values_payload);

    if !has_nulls {
        return values.into_iter().map(Some).collect();
    }
    values
        .into_iter()
        .enumerate()
        .map(|(i, v)| {
            if bitmap[i / 8] & (1 << (i % 8)) != 0 {
                Some(v)
            } else {
                None
            }
        })
        .collect()
}

/// 按基础编码标签（已去 `0x80`）解码值 payload 为 `Vec<TypedValue>`（8 变体）。
fn decode_values(tag: u8, payload: &[u8]) -> Vec<TypedValue> {
    match tag {
        VALUE_TAG_GORILLA => GorillaDecoder::decode(payload)
            .into_iter()
            .map(TypedValue::F64)
            .collect(),
        VALUE_TAG_GORILLA_F32 => GorillaF32Decoder::decode(payload)
            .into_iter()
            .map(TypedValue::F32)
            .collect(),
        VALUE_TAG_SIMPLE8B => Simple8bDecoder::decode(payload)
            .into_iter()
            .map(TypedValue::I64)
            .collect(),
        VALUE_TAG_SIMPLE8B_U64 => simple8b::decode_u64(payload)
            .into_iter()
            .map(TypedValue::U64)
            .collect(),
        VALUE_TAG_SIMPLE8B_I32 => Simple8bDecoder::decode(payload)
            .into_iter()
            .map(|v| TypedValue::I32(v as i32))
            .collect(),
        VALUE_TAG_SIMPLE8B_ENUM16 => simple8b::decode_u64(payload)
            .into_iter()
            .map(|v| TypedValue::Enum16(v as u16))
            .collect(),
        VALUE_TAG_BOOL_RLE => RleDecoder::decode(payload)
            .into_iter()
            .map(|v| TypedValue::Bool(v != 0))
            .collect(),
        VALUE_TAG_DICTIONARY => DictionaryDecoder::decode(payload)
            .into_iter()
            .map(TypedValue::String)
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionType;
    use crate::memtable::chunk::ColumnarChunk;
    use crate::memtable::col_vec::ColVec;
    use crate::sac::header::SacLevel;
    use crate::sac::writer::SacWriter;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn make_chunk(table_id: u32, columns: HashMap<u16, ColVec>) -> ColumnarChunk {
        let point_count = columns.values().next().map(|c| c.len()).unwrap_or(0);
        // A4/A5：ts_index 是唯一真相源，测试夹具必须与真实 append 路径一致地填充 ts→pos。
        let ts_index: BTreeMap<i64, usize> = [100i64, 200, 300]
            .into_iter()
            .enumerate()
            .map(|(i, ts)| (ts, i))
            .collect();
        ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps: vec![100, 200, 300],
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; 3],
            ts_index,
            point_count,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        }
    }

    #[test]
    fn test_write_then_read() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let mut columns = std::collections::HashMap::new();
        columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0]));
        columns.insert(1, ColVec::F64(vec![10.0, 20.0, 30.0]));

        let chunk = make_chunk(1, columns);

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write failed");

        let rows = SacReader::read_range(&path, TenantId::DEFAULT, 1, 0, 500, &[0, 1], None)
            .expect("read failed");

        assert_eq!(rows.len(), 3, "expected 3 rows, got {}", rows.len());
        assert_eq!(rows[0].ts, 100);
        assert_eq!(rows[1].ts, 200);
        assert_eq!(rows[2].ts, 300);

        let mut v0: Vec<(u16, TypedValue)> = rows[0].values.clone();
        v0.sort_by_key(|(c, _)| *c);
        assert_eq!(v0[0], (0, TypedValue::F64(1.0)));
        assert_eq!(v0[1], (1, TypedValue::F64(10.0)));
    }

    #[test]
    fn test_write_then_read_int_columns() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let mut columns = std::collections::HashMap::new();
        columns.insert(0, ColVec::I64(vec![1, 2, 3]));
        columns.insert(1, ColVec::Bool(vec![true, false, true]));

        let chunk = make_chunk(7, columns);

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write failed");

        let rows = SacReader::read_range(&path, TenantId::DEFAULT, 7, 0, 500, &[0, 1], None)
            .expect("read failed");

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], (0, TypedValue::I64(1)));
        assert_eq!(rows[1].values[1], (1, TypedValue::Bool(false)));
        assert_eq!(rows[2].values[1], (1, TypedValue::Bool(true)));
    }

    #[test]
    fn test_write_then_read_f32_and_string_columns() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let mut columns = std::collections::HashMap::new();
        columns.insert(0, ColVec::F32(vec![1.5, 2.5, 3.5]));
        columns.insert(
            1,
            ColVec::String(vec![
                "open".to_string(),
                "close".to_string(),
                "open".to_string(),
            ]),
        );

        let chunk = make_chunk(9, columns);

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write failed");

        let rows = SacReader::read_range(&path, TenantId::DEFAULT, 9, 0, 500, &[0, 1], None)
            .expect("read failed");

        assert_eq!(rows.len(), 3);
        // F32 原生解码
        assert_eq!(rows[0].values[0], (0, TypedValue::F32(1.5)));
        // String 列返回原串（不再退化为 f64/NaN）
        assert_eq!(
            rows[0].values[1],
            (1, TypedValue::String("open".to_string()))
        );
        assert_eq!(
            rows[1].values[1],
            (1, TypedValue::String("close".to_string()))
        );
    }

    /// E2：乱序 chunk flush 前按 ts_index 排序——读回 ts 升序 + 值按 ts 正确对应。
    #[test]
    fn test_write_then_read_out_of_order_sorted() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let writer = SacWriter::new(tmp_dir.path().join("sac"), CompressionType::Lz4);

        // 物理 append 顺序 [300, 100, 200]；ts_index 映射 ts→物理 pos。
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(vec![30.0, 10.0, 20.0]));
        let mut ts_index = BTreeMap::new();
        ts_index.insert(100, 1);
        ts_index.insert(200, 2);
        ts_index.insert(300, 0);
        let chunk = ColumnarChunk {
            table_id: 5,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps: vec![300, 100, 200],
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; 3],
            ts_index,
            point_count: 3,
            out_of_order: true,
            max_wal_seq: None,
            min_wal_seq: None,
        };

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write failed");

        let rows = SacReader::read_range(&path, TenantId::DEFAULT, 5, 0, 500, &[0], None)
            .expect("read failed");

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].ts, 100);
        assert_eq!(rows[0].values[0], (0, TypedValue::F64(10.0)));
        assert_eq!(rows[1].ts, 200);
        assert_eq!(rows[1].values[0], (0, TypedValue::F64(20.0)));
        assert_eq!(rows[2].ts, 300);
        assert_eq!(rows[2].values[0], (0, TypedValue::F64(30.0)));
    }

    /// S2 Step 1：跨租户读必须返回统一「不存在/损坏」错误（不泄露存在性、不返回数据）。
    #[test]
    fn test_read_range_tenant_mismatch_returns_error() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0]));
        let chunk = make_chunk(1, columns); // tenant_id = DEFAULT (0)

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write failed");

        // 文件 tenant=0，用 tenant=1 读 → 统一错误（非空数据、非可区分变体）。
        let err = SacReader::read_range(&path, TenantId(1), 1, 0, 500, &[0], None)
            .expect_err("tenant mismatch must error");
        assert!(
            matches!(err, SenseArcError::SacIoError(_)),
            "tenant 不匹配应复用 SacIoError 变体（不泄露存在性），got {err:?}"
        );
    }

    /// S2 Step 1：header 与路径目录段双重校验——文件被复制到 `1/` 目录
    /// （header 仍为 0）时，用 tenant=1 读必须返回统一错误（header 与目录段双重不符）。
    #[test]
    fn test_read_range_tenant_header_dir_mismatch_returns_error() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0]));
        let chunk = make_chunk(1, columns); // header.tenant_id = 0

        let src = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write failed");

        // 复制到 `1/0000000000000000000_0_0001.sac`：路径 tenant 段 = 1，header 仍 = 0。
        let dst_dir = sac_dir.join("1");
        std::fs::create_dir_all(&dst_dir).unwrap();
        let dst = dst_dir.join("0000000000000000000_0_0001.sac");
        std::fs::copy(&src, &dst).unwrap();

        // header(0) != 1 且 path 段(1) == 1 → 双重不符（header 不符即报错）。
        let err = SacReader::read_range(&dst, TenantId(1), 1, 0, 500, &[0], None)
            .expect_err("header/dir double mismatch must error");
        assert!(
            matches!(err, SenseArcError::SacIoError(_)),
            "header 与目录段双重不符应复用 SacIoError 变体，got {err:?}"
        );
    }

    /// T1：稀疏 chunk（col0 全 present，col1 仅第 1 行 present）往返——
    /// null 行省略 col1 条目，且绝不呈现为 0.0。
    #[test]
    fn test_sparse_roundtrip_null_omitted() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);
        chunk.append(100, &[(0, TypedValue::F64(1.0))], &[(0, 0xC0)], 0);
        chunk.append(
            200,
            &[(0, TypedValue::F64(2.0)), (1, TypedValue::F64(20.0))],
            &[(0, 0xC0)],
            0,
        );
        chunk.append(300, &[(0, TypedValue::F64(3.0))], &[(0, 0xC0)], 0);

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write");
        let rows = SacReader::read_range(&path, TenantId::DEFAULT, 1, 0, 500, &[0, 1], None)
            .expect("read");

        assert_eq!(rows.len(), 3);
        // 第 0/2 行 col1 为 null → values 不含 col1（≠ 0.0）。
        assert_eq!(rows[0].values, vec![(0, TypedValue::F64(1.0))]);
        assert_eq!(
            rows[1].values,
            vec![(0, TypedValue::F64(2.0)), (1, TypedValue::F64(20.0))]
        );
        assert_eq!(rows[2].values, vec![(0, TypedValue::F64(3.0))]);
        assert!(
            !rows[0].values.iter().any(|(c, _)| *c == 1),
            "null 不得残留占位值"
        );
        assert!(
            !rows[2].values.iter().any(|(c, _)| *c == 1),
            "null 不得残留占位值"
        );
    }

    /// T4：8 类型各一列含 null（F64/F32/I64/U64/I32/Bool/String/Enum16）往返保真。
    #[test]
    fn test_all_types_null_roundtrip() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let row_vals = |a: f64, b: f32, c: i64, d: u64, e: i32, f: bool, g: &str, h: u16| {
            vec![
                (0u16, TypedValue::F64(a)),
                (1, TypedValue::F32(b)),
                (2, TypedValue::I64(c)),
                (3, TypedValue::U64(d)),
                (4, TypedValue::I32(e)),
                (5, TypedValue::Bool(f)),
                (6, TypedValue::String(g.to_string())),
                (7, TypedValue::Enum16(h)),
            ]
        };

        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 9);
        chunk.append(
            100,
            &row_vals(1.0, 1.5, 1, 1, 1, true, "a", 1),
            &[(0, 0xC0)],
            0,
        );
        chunk.append(200, &[], &[(0, 0xC0)], 0); // 全 null 行
        chunk.append(
            300,
            &row_vals(3.0, 3.5, 3, 3, 3, true, "c", 3),
            &[(0, 0xC0)],
            0,
        );

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write");
        let ids: Vec<u16> = (0..8).collect();
        let rows =
            SacReader::read_range(&path, TenantId::DEFAULT, 9, 0, 500, &ids, None).expect("read");

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values.len(), 8);
        assert_eq!(rows[1].values.len(), 0, "全 null 行的 8 列均应省略");
        assert_eq!(rows[2].values.len(), 8);

        // 抽查 present 值 + 类型保真。
        assert_eq!(rows[0].values[0], (0, TypedValue::F64(1.0)));
        assert_eq!(rows[0].values[1], (1, TypedValue::F32(1.5)));
        assert_eq!(rows[0].values[2], (2, TypedValue::I64(1)));
        assert_eq!(rows[0].values[3], (3, TypedValue::U64(1)));
        assert_eq!(rows[0].values[4], (4, TypedValue::I32(1)));
        assert_eq!(rows[0].values[5], (5, TypedValue::Bool(true)));
        assert_eq!(rows[0].values[6], (6, TypedValue::String("a".to_string())));
        assert_eq!(rows[0].values[7], (7, TypedValue::Enum16(1)));
        assert_eq!(rows[2].values[5], (5, TypedValue::Bool(true)));
    }

    /// T5：跨块（>4096 行、两个块均含 null）——每位图按各自块 row_count 计算，不串位。
    #[test]
    fn test_cross_block_nulls() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        const TOTAL: i64 = 8192; // 恰好两个 4096 行块
        let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 10);
        for i in 0..TOTAL {
            let ts = 100 + i;
            let mut vals = vec![(0u16, TypedValue::F64(i as f64))];
            if i % 2 == 0 {
                vals.push((1u16, TypedValue::F64(i as f64 * 10.0)));
            }
            chunk.append(ts, &vals, &[(0, 0xC0)], 0);
        }

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write");
        let rows =
            SacReader::read_range(&path, TenantId::DEFAULT, 10, 0, 100 + TOTAL, &[0, 1], None)
                .expect("read");

        assert_eq!(rows.len(), TOTAL as usize);
        for row in &rows {
            let idx = row.ts - 100;
            if idx % 2 == 0 {
                assert_eq!(
                    row.values,
                    vec![
                        (0, TypedValue::F64(idx as f64)),
                        (1, TypedValue::F64(idx as f64 * 10.0))
                    ]
                );
            } else {
                assert_eq!(row.values, vec![(0, TypedValue::F64(idx as f64))]);
            }
        }
    }

    /// XOR 型加密桩（密文布局 nonce12B||tag16B||ct，与 engine 28B 开销约定一致）。
    struct XorEncryptor {
        version: u32,
    }

    impl Encryptor for XorEncryptor {
        fn encrypt(&self, plaintext: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
            let mut out = Vec::with_capacity(28 + plaintext.len());
            out.extend_from_slice(&[0x3Cu8; 12]); // nonce
            out.extend_from_slice(&[0xC3u8; 16]); // tag
            out.extend(plaintext.iter().map(|b| b ^ 0x5A));
            Ok(out)
        }

        fn decrypt(&self, blob: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
            if blob.len() < 28 {
                return Err(EncryptError::TooShort(blob.len(), 28));
            }
            Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
        }

        fn active_dek_version(&self, _tenant_id: TenantId) -> u32 {
            self.version
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    /// 明文多表夹具：不同表列集合不同（稀疏列跨表），表内也有 null（稀疏）。
    fn plain_tables() -> Vec<(u32, Vec<(i64, Vec<(u16, TypedValue)>)>)> {
        vec![
            (
                10,
                vec![
                    (100, vec![(0, TypedValue::F64(1.0))]),
                    (
                        200,
                        vec![(0, TypedValue::F64(2.0)), (1, TypedValue::F64(20.0))],
                    ),
                    (300, vec![(0, TypedValue::F64(3.0))]),
                ],
            ),
            (
                20,
                vec![
                    (150, vec![(0, TypedValue::I64(7))]),
                    (250, vec![(0, TypedValue::I64(8))]),
                ],
            ),
            (30, vec![(180, vec![(2, TypedValue::Bool(true))])]),
            (
                40,
                vec![
                    (
                        300,
                        vec![(0, TypedValue::F64(9.0)), (3, TypedValue::String("x".into()))],
                    ),
                    (310, vec![(0, TypedValue::F64(10.0))]),
                ],
            ),
        ]
    }

    /// 加密多表夹具：与明文夹具同构，用于覆盖「加密 Block + mmap 元数据」组合。
    fn enc_tables() -> Vec<(u32, Vec<(i64, Vec<(u16, TypedValue)>)>)> {
        vec![
            (
                100,
                vec![
                    (1000, vec![(0, TypedValue::F64(1.5))]),
                    (
                        2000,
                        vec![(0, TypedValue::F64(2.5)), (1, TypedValue::I64(-20))],
                    ),
                    (3000, vec![(0, TypedValue::F64(3.5))]),
                ],
            ),
            (
                200,
                vec![
                    (1500, vec![(0, TypedValue::F64(10.0))]),
                    (2500, vec![(0, TypedValue::F64(20.0))]),
                ],
            ),
            (300, vec![(1800, vec![(2, TypedValue::F32(0.5))])]),
        ]
    }

    /// 用 `begin_write + append_chunk` 写一个多表聚合 SAC 文件（chunk 按 (table_id,min_ts) 升序）。
    fn write_multi_table_sac(
        sac_dir: &Path,
        tables: &[(u32, Vec<(i64, Vec<(u16, TypedValue)>)>)],
        encryptor: SharedEncryptor,
    ) -> PathBuf {
        let writer = SacWriter::new_with_encryptor(sac_dir, CompressionType::Lz4, encryptor);
        let mut builder = writer
            .begin_write(TenantId::DEFAULT.as_u32(), 0, SacLevel::L0, 1, 0)
            .expect("begin_write");
        for (table_id, rows) in tables {
            let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, *table_id);
            for (ts, vals) in rows {
                chunk.append(*ts, vals, &[(0, 0xC0)], 0);
            }
            builder.append_chunk(&chunk, None).expect("append_chunk");
        }
        builder.finish().expect("finish")
    }

    /// 参考实现：复刻优化前「TableIndex 全量 read_exact + 全量 L2 read_exact」读路径，
    /// 用同一 `read_block` 解码 Block，作为 mmap 路径的逐字节一致对照基准。
    fn read_range_reference(
        path: &Path,
        tenant_id: TenantId,
        table_id: u32,
        ts_start: i64,
        ts_end: i64,
        column_ids: &[u16],
        encryptor: SharedEncryptor,
    ) -> Result<Vec<SacRow>, SenseArcError> {
        let mut file =
            File::open(path).map_err(|e| SenseArcError::SacIoError(format!("open: {}", e)))?;
        let file_len = file
            .metadata()
            .map_err(|e| SenseArcError::SacIoError(format!("metadata: {}", e)))?
            .len();
        if file_len < (SAC_HEADER_SIZE + SAC_FOOTER_SIZE) as u64 {
            return Err(SenseArcError::SacIoError("file too small".into()));
        }

        let mut header_buf = [0u8; SAC_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))
            .map_err(|e| SenseArcError::SacIoError(format!("seek header: {}", e)))?;
        file.read_exact(&mut header_buf)
            .map_err(|e| SenseArcError::SacIoError(format!("read header: {}", e)))?;
        let header = SacHeader::from_bytes(&header_buf)
            .ok_or_else(|| SenseArcError::SacIoError("bad header magic".into()))?;
        if header.tenant_id != tenant_id.as_u32() {
            return Err(SenseArcError::SacIoError("tenant mismatch".into()));
        }
        if path_tenant_id(path) != Some(tenant_id.as_u32()) {
            return Err(SenseArcError::SacIoError("tenant mismatch".into()));
        }
        if header.tde_encrypted && encryptor.is_none() {
            return Err(EncryptError::NoEncryptor.into());
        }

        let footer_offset = file_len - SAC_FOOTER_SIZE as u64;
        let mut footer_buf = [0u8; SAC_FOOTER_SIZE];
        file.seek(SeekFrom::Start(footer_offset))
            .map_err(|e| SenseArcError::SacIoError(format!("seek footer: {}", e)))?;
        file.read_exact(&mut footer_buf)
            .map_err(|e| SenseArcError::SacIoError(format!("read footer: {}", e)))?;
        let footer = SacFooter::from_bytes(&footer_buf)
            .ok_or_else(|| SenseArcError::SacIoError("bad footer".into()))?;

        // 原全量 TableIndex read_exact（对照基线）。
        let table_index_size =
            (footer.table_count as u64).saturating_mul(SAC_TABLE_INDEX_ENTRY_SIZE as u64);
        let mut table_index_bytes = vec![0u8; table_index_size as usize];
        file.seek(SeekFrom::Start(footer.table_index_offset))
            .map_err(|e| SenseArcError::SacIoError(format!("seek table index: {}", e)))?;
        file.read_exact(&mut table_index_bytes)
            .map_err(|e| SenseArcError::SacIoError(format!("read table index: {}", e)))?;

        // 原全量 L2 read_exact（对照基线）。
        let l2_size = footer.table_index_offset.saturating_sub(footer.l2_offset);
        let l2_count = (l2_size / SAC_L2_ENTRY_SIZE as u64) as usize;
        let mut l2_entries: Vec<L2Descriptor> = Vec::with_capacity(l2_count);
        file.seek(SeekFrom::Start(footer.l2_offset))
            .map_err(|e| SenseArcError::SacIoError(format!("seek l2: {}", e)))?;
        for _ in 0..l2_count {
            let mut l2_buf = [0u8; SAC_L2_ENTRY_SIZE];
            file.read_exact(&mut l2_buf)
                .map_err(|e| SenseArcError::SacIoError(format!("read l2 entry: {}", e)))?;
            let l2 = L2Descriptor::from_bytes(&l2_buf)
                .ok_or_else(|| SenseArcError::SacIoError("bad l2 entry".into()))?;
            l2_entries.push(l2);
        }

        let mut candidate_l2: Vec<usize> = Vec::new();
        if let Some(idx) = binary_search_table_index(&table_index_bytes, table_id) {
            let entry = read_table_index_entry(&table_index_bytes, idx)
                .ok_or_else(|| SenseArcError::SacIoError("bad table index entry".into()))?;
            let start = entry.first_l2_idx as usize;
            let end = (entry.first_l2_idx as usize + entry.block_count as usize)
                .min(l2_entries.len());
            for i in start..end {
                candidate_l2.push(i);
            }
        }

        let want_all = column_ids.is_empty();
        let compression = header.level.compression();
        let mut rows: Vec<SacRow> = Vec::new();
        for l2_idx in &candidate_l2 {
            let l2 = &l2_entries[*l2_idx];
            if !l2.overlaps(ts_start, ts_end) {
                continue;
            }
            match read_block(
                &mut file,
                l2.block_offset,
                l2.block_size,
                ts_start,
                ts_end,
                column_ids,
                want_all,
                encryptor.as_deref(),
                header.tde_encrypted,
                header.dek_version,
                tenant_id,
                l2.table_id as u64,
                *l2_idx as u64,
                compression,
            )? {
                Some(rows_in_block) => rows.extend(rows_in_block),
                None => {}
            }
        }
        rows.sort_by_key(|r| r.ts);
        Ok(rows)
    }

    /// 逐字节一致：mmap 路径（`SacReader::read_range`） vs 原全量 read_exact 路径。
    ///
    /// 覆盖多表、稀疏列（跨表 + 表内 null）、加密 Block；对每个表（含未命中表）断言
    /// 两条路径返回的 `Vec<SacRow>` 完全相等（`SacRow` 派生 `PartialEq`，逐字段比较）。
    #[test]
    fn test_mmap_vs_full_read_byte_identical() {
        // 明文：多表 + 稀疏列。
        let plain_dir = tempfile::TempDir::new().unwrap();
        let plain_path = write_multi_table_sac(plain_dir.path(), &plain_tables(), None);
        let plain_ids: Vec<u16> = vec![0, 1, 2, 3];
        for tid in [10u32, 20, 30, 40, 999] {
            let mmap_rows = SacReader::read_range(
                &plain_path,
                TenantId::DEFAULT,
                tid,
                0,
                1_000_000,
                &plain_ids,
                None,
            )
            .expect("mmap read plain");
            let ref_rows = read_range_reference(
                &plain_path,
                TenantId::DEFAULT,
                tid,
                0,
                1_000_000,
                &plain_ids,
                None,
            )
            .expect("reference read plain");
            assert_eq!(mmap_rows, ref_rows, "明文多表 table {tid} 逐字节不一致");
        }

        // 加密：多表 + 稀疏列 + 加密 Block。
        let enc_dir = tempfile::TempDir::new().unwrap();
        let enc: SharedEncryptor = Some(std::sync::Arc::new(XorEncryptor { version: 1 }));
        let enc_path = write_multi_table_sac(enc_dir.path(), &enc_tables(), enc.clone());
        let enc_ids: Vec<u16> = vec![0, 1, 2];
        for tid in [100u32, 200, 300, 999] {
            let mmap_rows = SacReader::read_range(
                &enc_path,
                TenantId::DEFAULT,
                tid,
                0,
                1_000_000,
                &enc_ids,
                enc.clone(),
            )
            .expect("mmap read enc");
            let ref_rows = read_range_reference(
                &enc_path,
                TenantId::DEFAULT,
                tid,
                0,
                1_000_000,
                &enc_ids,
                enc.clone(),
            )
            .expect("reference read enc");
            assert_eq!(mmap_rows, ref_rows, "加密多表 table {tid} 逐字节不一致");
        }
    }
}

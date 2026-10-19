//! SAC 写入器：编码管线 → .tmp → rename → .sac。
//!
//! SacWriter 负责将 MemTable 中的 `ColumnarChunk` 编码为 SAC Block 格式并写入磁盘。
//! 写入流程（SAC v9 定稿）：
//! 1. 创建 `.tmp` 临时文件
//! 2. 写入 `SacHeader`
//! 3. 对每个 Block：编码 ts/qual/各列 val 流 → 写 mini-header + Stream Directory +
//!    各流 + 每 Block 尾部 CRC32C
//! 4. 写入 L2 描述符区（每 Block 一条：table_id/block_offset/block_size/...）
//! 5. 单遍扫描 L2 构建 TableIndex（连续同 table_id 归并）→ 写 TableIndex 区
//! 6. 写入 `SacFooter`（含 block/table 计数镜像 + 整文件 CRC32C）
//! 7. fsync → rename → .sac
//!
//! ## Block 磁盘布局（SAC v9）
//!
//! ```text
//! [BlockMiniHeader: 48B]
//! [Stream Directory: (column_count+2) × 12B]
//! [ts_stream bytes]    (DoD → LZ4)
//! [val_c0: tag u8 + bytes]   (Gorilla/Simple8b/Dictionary/BoolRLE → 独立 LZ4)
//! [val_c1: tag u8 + bytes]
//! ...
//! [qual_stream bytes]  (RLE → LZ4)
//! [CRC32C: 4B]
//! ```
//!
//! ## 不变式 I2（块排序）
//!
//! 调用方必须按 `(table_id, min_ts)` 升序 `append_chunk`；`finish` 据此单遍构建
//! TableIndex。flush 批量按 table_id 排序、compaction k 路归并按 (table_id, ts) 排序，
//! 二者天然满足该契约。

use std::fs::{self, File};
use std::hash::Hasher;
use std::io::{BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use sensearcdb_common::{SenseArcError, TenantId};

use crate::compression::{CompressionType, Compressor};
use crate::encoding::{
    dictionary::DictionaryEncoder, dod::DoDEncoder, gorilla::GorillaEncoder,
    gorilla::GorillaF32Encoder, rle::RleEncoder, simple8b, simple8b::Simple8bEncoder, Encoder,
    VALUE_TAG_BOOL_RLE, VALUE_TAG_DICTIONARY, VALUE_TAG_GORILLA, VALUE_TAG_GORILLA_F32,
    VALUE_TAG_NULL_BITMAP, VALUE_TAG_SIMPLE8B, VALUE_TAG_SIMPLE8B_ENUM16, VALUE_TAG_SIMPLE8B_I32,
    VALUE_TAG_SIMPLE8B_U64,
};
use crate::encryption::{encrypt_with, EncryptContext, EncryptError, Encryptor, SharedEncryptor};
use crate::memtable::chunk::ColumnarChunk;
use crate::memtable::col_vec::ColVec;
use crate::sac::block::{
    Block, BlockMiniHeader, BlockValStream, StreamDirEntry, StreamDirectory, QUAL_STREAM_COLUMN_ID,
    TS_STREAM_COLUMN_ID,
};
use crate::sac::footer::SacFooter;
use crate::sac::header::{SacHeader, SacLevel};
use crate::sac::l2_descriptor::L2Descriptor;
use crate::sac::naming::SacNaming;
use crate::sac::table_index::TableIndexEntry;

/// 块内并行编码的最小行数阈值（§5.3-6 / §7.2 待实测校准：候选 1024/4096/16384）。
///
/// `4096` 等于 [`BLOCK_DEFAULT_ROW_COUNT`]，即「满块并行、末块（<4096 行）退化为串行」。
/// 小块串行可避免 rayon 调度开销大于编码收益；此阈值是单点，§7 阈值扫描后统一校准。
const PARALLEL_ENCODE_MIN_ROWS: usize = 4096;

/// `append_chunks` 跨表批量的 wave 大小（按 chunk=表 计数）。
///
/// 10 万表 flush 若一次物化全部 block 任务 + 压缩字节会 ~100-150MB，故按波分批：
/// 每波 `WAVE_CHUNKS` 个 chunk（表），波内并行编码 + 顺序写盘，波间续 `block_idx`
/// 与 `file_crc`。单波压缩字节 ~几 MB（< 16MB 内存上界，§2.6 / §5 验收点 6）。
const WAVE_CHUNKS: usize = 4096;

/// SAC 写入器。
///
/// 负责将 `ColumnarChunk` 编码为 SAC 文件格式并持久化到磁盘。
pub struct SacWriter {
    /// 文件命名工具。
    naming: SacNaming,
    /// 压缩类型。
    compression: CompressionType,
    /// 可选加密器（TDE 关闭时为 `None`）。
    encryptor: SharedEncryptor,
}

impl SacWriter {
    /// 创建新的 SAC 写入器（TDE 关闭）。
    ///
    /// # 参数
    /// - `sac_dir`: SAC 文件根目录。
    /// - `compression`: 压缩算法（LZ4 或 ZSTD9）。
    pub fn new(sac_dir: impl Into<PathBuf>, compression: CompressionType) -> Self {
        Self {
            naming: SacNaming::new(sac_dir),
            compression,
            encryptor: None,
        }
    }

    /// 创建带 TDE 加密器的 SAC 写入器。
    ///
    /// # 参数
    /// - `sac_dir`: SAC 文件根目录。
    /// - `compression`: 压缩算法（LZ4 或 ZSTD9）。
    /// - `encryptor`: 可选加密器（`None` = TDE 关）。
    pub fn new_with_encryptor(
        sac_dir: impl Into<PathBuf>,
        compression: CompressionType,
        encryptor: SharedEncryptor,
    ) -> Self {
        Self {
            naming: SacNaming::new(sac_dir),
            compression,
            encryptor,
        }
    }

    /// 将一个 ColumnarChunk 写入 SAC 文件（write_epoch 取当前系统时间）。
    ///
    /// 便捷包装：分区键日对齐落地后，`write_epoch` 必须与 `partition_ns` 解耦
    /// （不变式 I1）。本方法内部取「当前时间」作为写入时间，等价于旧语义下
    /// flush 传入 `now_ns` 的场景；需要显式指定写入时间时用
    /// [`write_chunk_with_epoch`](Self::write_chunk_with_epoch)。
    ///
    /// # 参数
    /// - `chunk`: 待写入的列式数据块。
    /// - `partition_ns`: 分区键（数据时间天边界，epoch 纳秒）。
    /// - `level`: SAC 层级。
    /// - `seq`: 序列号。
    /// - `max_wal_seq`: Block 内最大 WAL seq 三态（S7）。
    pub fn write_chunk(
        &self,
        chunk: &ColumnarChunk,
        partition_ns: i64,
        level: SacLevel,
        seq: u16,
        max_wal_seq: Option<u64>,
    ) -> Result<PathBuf, SenseArcError> {
        self.write_chunk_with_epoch(
            chunk,
            partition_ns,
            level,
            seq,
            max_wal_seq,
            current_epoch_ns(),
        )
    }

    /// 将一个 ColumnarChunk 写入 SAC 文件（显式指定 write_epoch）。
    ///
    /// 与 [`write_chunk`](Self::write_chunk) 的区别：`write_epoch_ns` 由调用方显式
    /// 传入（flush / compaction 传 `now_ns`），彻底解耦「数据时间分区键」与「文件
    /// 写入时间」（不变式 I1）。
    ///
    /// # 参数
    /// - `chunk`: 待写入的列式数据块。
    /// - `partition_ns`: 分区键（数据时间天边界，epoch 纳秒）。
    /// - `level`: SAC 层级。
    /// - `seq`: 序列号。
    /// - `max_wal_seq`: Block 内最大 WAL seq 三态（S7）：
    ///   `Some(seq)` = Flush 产出；`None` = Compaction / 回放 / 批量导入。
    /// - `write_epoch_ns`: 文件写入时间（epoch 纳秒，跨文件 LWW 排序键）。
    ///
    /// # 返回值
    /// 写入的 SAC 文件路径。
    pub fn write_chunk_with_epoch(
        &self,
        chunk: &ColumnarChunk,
        partition_ns: i64,
        level: SacLevel,
        seq: u16,
        max_wal_seq: Option<u64>,
        write_epoch_ns: i64,
    ) -> Result<PathBuf, SenseArcError> {
        let tenant_id = chunk.tenant_id.as_u32();
        let mut builder = self.begin_write(tenant_id, partition_ns, level, seq, write_epoch_ns)?;
        builder.append_chunk(chunk, max_wal_seq)?;
        builder.finish()
    }

    /// 开启一个 SAC 文件的增量写入（写 header，返回 builder）。
    ///
    /// 供「单 chunk 一次性写」（`write_chunk_with_epoch`）与「流式逐块写」
    /// （Compaction 流式 merge）共用同一套字节布局：header → blocks → L2 → TableIndex →
    /// footer → fsync → rename。避免两条写路径产出字节不一致（D5 数据正确性红线）。
    ///
    /// v9 不再传 `table_id`：一个文件聚合多个表，`append_chunk` 内取 `chunk.table_id`。
    pub fn begin_write(
        &self,
        tenant_id: u32,
        partition_ns: i64,
        level: SacLevel,
        seq: u16,
        write_epoch_ns: i64,
    ) -> Result<SacFileBuilder, SenseArcError> {
        let final_path = self.naming.file_path(tenant_id, partition_ns, level, seq);
        let tmp_path = self.naming.tmp_path(tenant_id, partition_ns, level, seq);

        // 确保父目录存在
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| SenseArcError::SacIoError(format!("mkdir: {}", e)))?;
        }

        // 写入 .tmp 文件
        let file = File::create(&tmp_path)
            .map_err(|e| SenseArcError::SacIoError(format!("create tmp: {}", e)))?;
        let mut writer = BufWriter::new(file);

        // 1. 写入 Header（TDE 开启时回填加密标志 + dek_version；按租户活跃版本）
        let (tde_encrypted, dek_version) = match &self.encryptor {
            Some(e) if e.is_enabled() => (true, e.active_dek_version(TenantId(tenant_id))),
            _ => (false, 0),
        };
        let mut header = SacHeader::new(partition_ns, level, seq);
        if tde_encrypted {
            header.tde_encrypted = true;
            header.dek_version = dek_version;
        }
        // S1：写真实 tenant_id（chunk.tenant_id，当前恒 0 = 默认租户）。
        // 不变式 I1：write_epoch = 文件写入时间（now_ns），与 partition_ns（数据时间
        // 天边界）解耦，二者不再混用。
        header.tenant_id = tenant_id;
        header.write_epoch = write_epoch_ns as u64;
        let header_bytes = header.to_bytes();
        writer
            .write_all(&header_bytes)
            .map_err(|e| SenseArcError::SacIoError(format!("write header: {}", e)))?;

        // 整文件 CRC（覆盖 Header + 所有 Block + L2 + TableIndex），供 Footer 强校验
        let mut file_crc = crc32c::Crc32cHasher::new(0);
        file_crc.write(&header_bytes);

        Ok(SacFileBuilder {
            writer,
            file_crc,
            l2_entries: Vec::new(),
            row_counts: Vec::new(),
            final_path,
            tmp_path,
            block_idx: 0usize,
            compression: self.compression,
            encryptor: self.encryptor.clone(),
            dek_version,
            tenant_id,
            level,
        })
    }

    /// 获取 SAC 根目录。
    pub fn sac_dir(&self) -> &Path {
        self.naming.sac_dir()
    }

    /// 获取压缩类型。
    pub fn compression(&self) -> CompressionType {
        self.compression
    }
}

/// SAC 文件增量写入器：`begin_write` 后逐 chunk/逐块 `append_chunk`，最后 `finish`。
///
/// 与 `write_chunk_with_epoch` 共用同一套字节布局（见其 doc），供 Compaction 流式
/// merge 在「单文件、多 Block」下以 O(block) 内存写出，而不是先物化整份 ColumnarChunk。
pub struct SacFileBuilder {
    /// 已打开 .tmp 的缓冲写句柄。
    writer: BufWriter<File>,
    /// 整文件 CRC32C（header + blocks + L2 + TableIndex）。
    file_crc: crc32c::Crc32cHasher,
    /// L2 描述符（每块一条，按 (table_id, min_ts) 升序）。
    l2_entries: Vec<L2Descriptor>,
    /// 与 `l2_entries` 平行的每块行数（TableIndex.row_count 聚合需要）。
    row_counts: Vec<u32>,
    /// 最终 .sac 路径（fsync 后 rename 目标）。
    final_path: PathBuf,
    /// .tmp 临时路径。
    tmp_path: PathBuf,
    /// 已写入的 Block 数（跨 `append_chunk` 调用全局递增，保证 TDE 上下文不重复）。
    block_idx: usize,
    /// 块级压缩算法（L0=LZ4，L1/L2/L3/L4=ZSTD9）。
    compression: CompressionType,
    /// 可选加密器。
    encryptor: SharedEncryptor,
    /// 活跃 DEK 版本（明文为 0）。
    dek_version: u32,
    /// 租户 ID（header + 路径 + 加密上下文）。
    tenant_id: u32,
    /// SAC 层级（L0=flush 直出，L1/L2=compaction）。仅用于 flush_timing 观测日志
    /// 区分 flush 与 compaction 的 `encode_compress` / `finish_*` 埋点。
    level: SacLevel,
}

/// 跨表批量编码的最小任务单元：一个 (chunk, 行区间) 对应一个 Block。
///
/// 展平后 10 万表 × 1 block = 10 万任务，交由 flush rayon 池跨表并行编码。
struct BlockTask<'a> {
    /// 归属 chunk（=单表）。
    chunk: &'a ColumnarChunk,
    /// 本块在 `sorted_order` 中的起始行（含）。
    start_row: usize,
    /// 本块在 `sorted_order` 中的结束行（不含）。
    end_row: usize,
    /// 预分配的全局连续 block_id（`self.block_idx + running_total`）。
    block_id: u64,
    /// Block 内最大 WAL seq（三态 S7；逐表路径取显式入参，批量路径取 `chunk.max_wal_seq`）。
    max_wal_seq: Option<u64>,
}

impl SacFileBuilder {
    /// 追加一个 `ColumnarChunk` 的 Block（chunk 内部按 4096 行切块，与一次性写一致）。
    ///
    /// 不变量：每次调用独立编码、独立写盘、独立推进 `file_crc` 与 L2 索引；
    /// 跨调用共享全局 `block_idx`，因此多块 TDE 加密上下文不会碰撞。
    ///
    /// 调用方必须按 `(table_id, min_ts)` 升序追加 chunk（不变式 I2）。
    ///
    /// 本方法现为「单 chunk = 单元素批量」的薄包装：展平本 chunk 的 block 任务后走
    /// [`append_block_tasks`](Self::append_block_tasks)，与 [`append_chunks`](Self::append_chunks)
    /// 走完全相同的编码/写盘路径（产出逐字节一致，红线 #4）。
    pub fn append_chunk(
        &mut self,
        chunk: &ColumnarChunk,
        max_wal_seq: Option<u64>,
    ) -> Result<(), SenseArcError> {
        // A5：`ts_index` 是唯一真相源——始终按 `ts_index` 排序（不改取值与顺序，不变式 4）。
        // 预扫描（无分配）求去重行数 → blocks_needed（= sorted_positions().len()）。
        let row_count = chunk.unique_ts_count();
        let blocks_needed = (row_count + 4095) / 4096;
        let mut tasks: Vec<BlockTask<'_>> = Vec::with_capacity(blocks_needed);
        for i in 0..blocks_needed {
            tasks.push(BlockTask {
                chunk,
                start_row: i * 4096,
                end_row: ((i + 1) * 4096).min(row_count),
                block_id: self.block_idx as u64 + i as u64,
                max_wal_seq,
            });
        }
        self.append_block_tasks(&tasks)
    }

    /// 跨表批量追加：一个 partition 桶内所有 chunk 的 block 展平为任务列表，跨表并行编码 + 顺序写盘。
    ///
    /// 与逐表 [`append_chunk`](Self::append_chunk) 产出**逐字节一致**（红线 #4），但编码粒度
    /// 从「逐 chunk」提升到「跨表 block 平铺」：10 万表 × 1 block = 10 万任务，交由 flush
    /// rayon 池跨表并行，线程局部复用 `EncodeScratch`（压缩器上下文 + ts/qual 缓冲）。
    ///
    /// `max_wal_seq` 从 `chunk.max_wal_seq` 取（S7：flush 已回填）。
    pub fn append_chunks(&mut self, chunks: &[Arc<ColumnarChunk>]) -> Result<(), SenseArcError> {
        for wave in chunks.chunks(WAVE_CHUNKS) {
            // 预扫描（串行，O(表数)，无堆分配）：展平本 wave 的 block 任务，预分配连续 block_id。
            let mut tasks: Vec<BlockTask<'_>> = Vec::new();
            let mut running = 0usize;
            for chunk in wave {
                let row_count = chunk.unique_ts_count();
                let blocks_needed = (row_count + 4095) / 4096;
                for i in 0..blocks_needed {
                    tasks.push(BlockTask {
                        chunk: chunk.as_ref(),
                        start_row: i * 4096,
                        end_row: ((i + 1) * 4096).min(row_count),
                        block_id: self.block_idx as u64 + running as u64,
                        max_wal_seq: chunk.max_wal_seq,
                    });
                    running += 1;
                }
            }
            self.append_block_tasks(&tasks)?;
        }
        Ok(())
    }

    /// 追加一个已编码 Block 的原始字节（不解压/不重编码/不重压，刀3 block 拼接复制路径）。
    ///
    /// 供分代合并 L1→L2 / L2→L3 / final 的 block 级拼接：非重叠 Block 逐字节复制。
    /// 调用方按 `(table_id, min_ts)` 升序追加（不变式 I2），`finish` 据此单遍重建
    /// TableIndex/footer/CRC。Block 尾部 CRC32C 自包含，逐字节复制后仍有效。
    pub fn append_raw_block(
        &mut self,
        block_bytes: &[u8],
        table_id: u32,
        min_ts: i64,
        max_ts: i64,
        column_count: u16,
        row_count: u32,
    ) -> Result<(), SenseArcError> {
        let base_offset = self
            .writer
            .stream_position()
            .map_err(|e| SenseArcError::SacIoError(format!("seek: {}", e)))?;
        self.l2_entries.push(L2Descriptor::new(
            table_id,
            base_offset,
            block_bytes.len() as u32,
            min_ts,
            max_ts,
            column_count,
        ));
        self.row_counts.push(row_count);
        self.writer
            .write_all(block_bytes)
            .map_err(|e| SenseArcError::SacIoError(format!("write raw block: {}", e)))?;
        self.file_crc.write(block_bytes);
        self.block_idx += 1;
        Ok(())
    }

    /// 编码 + 写盘一批 block 任务（[`append_chunk`]/[`append_chunks`] 的共享核心）。
    ///
    /// - 并行只编码（纯 CPU）：`par_iter + map_init` 每 worker 一个 `EncodeScratch`；索引迭代
    ///   + `collect` 保序，`blocks` 天然按 block_id 升序。
    /// - 顺序写盘：一次 `stream_position` + 单缓冲 `write_all` + 顺序 `file_crc` / push L2/row，
    ///   最后 `block_idx += tasks.len()`（跨 wave / 跨调用连续不碰撞）。
    fn append_block_tasks(&mut self, tasks: &[BlockTask<'_>]) -> Result<(), SenseArcError> {
        if tasks.is_empty() {
            return Ok(());
        }

        let encode_start = Instant::now();
        let compression = self.compression;
        let encryptor: Option<&dyn Encryptor> = self.encryptor.as_deref();
        let dek_version = self.dek_version;
        let tenant_id = self.tenant_id;

        let blocks: Vec<Block> = crate::sac::parallel::flush_pool().install(|| {
            tasks
                .par_iter()
                .map_init(
                    || EncodeScratch::new(compression),
                    |scratch, task| {
                        let sorted_order = task.chunk.sorted_positions();
                        encode_block_with_scratch(
                            task.chunk,
                            &sorted_order,
                            scratch,
                            task.start_row,
                            task.end_row,
                            task.chunk.columns.len() as u16,
                            task.max_wal_seq,
                            compression,
                            encryptor,
                            dek_version,
                            TenantId(tenant_id),
                            task.chunk.table_id as u64,
                            task.block_id,
                            (task.end_row - task.start_row) >= PARALLEL_ENCODE_MIN_ROWS,
                        )
                    },
                )
                .collect::<Result<Vec<Block>, EncryptError>>()
        })?;

        let encode_ms = phase_ms(encode_start);
        let total_rows: u32 = tasks.iter().map(|t| (t.end_row - t.start_row) as u32).sum();
        let mut tables_count = 0usize;
        let mut last_table: Option<u32> = None;
        for t in tasks {
            if last_table != Some(t.chunk.table_id) {
                tables_count += 1;
                last_table = Some(t.chunk.table_id);
            }
        }
        tracing::debug!(
            target: "flush_timing",
            phase = "encode_compress",
            duration_ms = encode_ms,
            level = self.level as u8,
            tables = tables_count,
            rows = total_rows,
            blocks = blocks.len(),
        );

        // 按序组装（§5.3-4）：并行只编码，写盘仍按 block_id 升序回填（不变式 2）。
        // 一次 `stream_position` 取基准偏移，算术累加块大小得到每块 offset；块字节拼接为
        // 单缓冲后一次 `write_all`（减少 syscall），再顺序累加 file_crc、顺序 push L2/row。
        let mut base_offset = self
            .writer
            .stream_position()
            .map_err(|e| SenseArcError::SacIoError(format!("seek: {}", e)))?;
        let mut all_bytes: Vec<u8> = Vec::new();
        for block in &blocks {
            let mut block_bytes = block.to_bytes();
            self.l2_entries.push(L2Descriptor::new(
                block.header.table_id,
                base_offset,
                block_bytes.len() as u32,
                block.header.min_ts,
                block.header.max_ts,
                block.header.column_count,
            ));
            self.row_counts.push(block.header.row_count);
            base_offset += block_bytes.len() as u64;
            all_bytes.append(&mut block_bytes);
        }
        self.writer
            .write_all(&all_bytes)
            .map_err(|e| SenseArcError::SacIoError(format!("write block: {}", e)))?;
        self.file_crc.write(&all_bytes);

        self.block_idx += tasks.len();

        Ok(())
    }

    /// 收尾：单遍扫 L2 建 TableIndex → 写 L2/TableIndex 区 → 写 Footer → fsync → rename。
    pub fn finish(mut self) -> Result<PathBuf, SenseArcError> {
        let total_blocks = self.l2_entries.len();
        // 计时埋点（flush_timing）：TableIndex + L2 + footer 段。
        let finish_start = Instant::now();

        // 单遍扫描 L2（已按 (table_id, min_ts) 升序）构建 TableIndex。
        let mut table_entries: Vec<TableIndexEntry> = Vec::new();
        for (idx, l2) in self.l2_entries.iter().enumerate() {
            match table_entries.last_mut() {
                Some(last) if last.table_id == l2.table_id => {
                    last.block_count += 1;
                    last.max_ts = l2.max_ts; // (table_id, min_ts) 升序 → 末块 max_ts 最大
                    last.row_count += self.row_counts[idx];
                    last.total_block_bytes += l2.block_size as u64;
                }
                _ => {
                    table_entries.push(TableIndexEntry::new(
                        l2.table_id,
                        1,
                        l2.min_ts,
                        l2.max_ts,
                        idx as u32,
                        self.row_counts[idx],
                        l2.block_size as u64,
                        l2.column_count,
                        0,
                    ));
                }
            }
        }

        // 3. 写入 L2 描述符区
        let l2_offset = self
            .writer
            .stream_position()
            .map_err(|e| SenseArcError::SacIoError(format!("seek l2: {}", e)))?;

        for l2 in &self.l2_entries {
            let l2_bytes = l2.to_bytes();
            self.writer
                .write_all(&l2_bytes)
                .map_err(|e| SenseArcError::SacIoError(format!("write l2: {}", e)))?;
            self.file_crc.write(&l2_bytes);
        }

        // 4. 写入 TableIndex 区
        let table_index_offset = self
            .writer
            .stream_position()
            .map_err(|e| SenseArcError::SacIoError(format!("seek table index: {}", e)))?;

        for entry in &table_entries {
            let entry_bytes = entry.to_bytes();
            self.writer
                .write_all(&entry_bytes)
                .map_err(|e| SenseArcError::SacIoError(format!("write table index: {}", e)))?;
            self.file_crc.write(&entry_bytes);
        }

        // 5. 写入 Footer
        // 全局 min_ts/max_ts：块按 (table_id, min_ts) 排序后，物理首/末块不一定是
        // 全局时间最小/最大（表时间范围交错时），必须遍历 L2 取全局 min/max（O(M)）。
        let global_min_ts = self
            .l2_entries
            .iter()
            .map(|l2| l2.min_ts)
            .min()
            .unwrap_or(0);
        let global_max_ts = self
            .l2_entries
            .iter()
            .map(|l2| l2.max_ts)
            .max()
            .unwrap_or(0);
        let crc32c_val = self.file_crc.finish() as u32;

        let footer = SacFooter::new(
            global_min_ts,
            global_max_ts,
            total_blocks as u32,
            table_entries.len() as u32,
            l2_offset,
            table_index_offset,
            crc32c_val,
        );

        self.writer
            .write_all(&footer.to_bytes())
            .map_err(|e| SenseArcError::SacIoError(format!("write footer: {}", e)))?;

        let index_footer_ms = phase_ms(finish_start);
        tracing::debug!(
            target: "flush_timing",
            phase = "finish_index_footer",
            duration_ms = index_footer_ms,
            level = self.level as u8,
            blocks = total_blocks as u32,
            tables = table_entries.len() as u32,
        );

        // 6. fsync
        let fsync_start = Instant::now();
        self.writer
            .flush()
            .map_err(|e| SenseArcError::SacIoError(format!("flush: {}", e)))?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| SenseArcError::SacIoError(format!("fsync: {}", e)))?;

        // 7. 原子 rename .tmp → .sac
        fs::rename(&self.tmp_path, &self.final_path)
            .map_err(|e| SenseArcError::SacIoError(format!("rename: {}", e)))?;

        let fsync_rename_ms = phase_ms(fsync_start);
        tracing::debug!(
            target: "flush_timing",
            phase = "finish_fsync_rename",
            duration_ms = fsync_rename_ms,
            level = self.level as u8,
            blocks = total_blocks as u32,
        );

        // 观测日志：补全数据时间范围（min_ts/max_ts 纳秒原始值 + 人类可读 UTC）
        // 与 level/seq（从最终文件名解析），让用户能追溯「数据时间 vs 落盘时间」。
        let seq = self
            .final_path
            .file_name()
            .and_then(|s| s.to_str())
            .and_then(SacNaming::parse_file_name)
            .map(|(_, _, seq)| seq)
            .unwrap_or(0);

        tracing::info!(
            blocks = total_blocks as u32,
            tables = table_entries.len() as u32,
            level = self.level as u8,
            seq = seq,
            min_ts = global_min_ts,
            max_ts = global_max_ts,
            min_ts_str = ns_to_utc_string(global_min_ts),
            max_ts_str = ns_to_utc_string(global_max_ts),
            path = %self.final_path.display(),
            "SAC file written"
        );

        Ok(self.final_path)
    }

    /// 中止写入：删除 `.tmp` 文件，不产出 `.sac`。
    ///
    /// 供流式 merge 中途遇到读错误时回滚（此时源文件不得被删除，输出也不得落盘）。
    pub fn abort(self) {
        if let Err(e) = fs::remove_file(&self.tmp_path) {
            tracing::warn!(path = %self.tmp_path.display(), error = %e, "failed to abort sac tmp file");
        }
    }
}

/// 编码一个 Block（并行策略自动判定）：ts=DoD、qual=RLE、各列 val 按 DataType 分派独立编码 + 块级压缩。
///
/// 块级压缩算法由 `compression` 决定（D6：L0=LZ4，L1/L2/L3/L4=ZSTD9）。TDE 开启时在
/// 压缩之后、组 Block 之前对三个数据流（ts/val/qual）加密，Stream Directory 记录
/// 加密后 size，尾部 CRC 覆盖密文（encrypt-then-CRC）。
///
/// 输出 `Block` 已包含就绪的 Stream Directory 与尾部 CRC32C。
///
/// 仅供测试（串行参照 + D6 ZSTD/LZ4 判定）与 `encode_block_impl` 使用；生产写路径
/// 一律走 [`encode_block_with_scratch`]（复用缓冲）。
#[cfg(test)]
fn encode_block(
    chunk: &ColumnarChunk,
    sorted_order: &[usize],
    start_row: usize,
    end_row: usize,
    column_count: u16,
    max_wal_seq: Option<u64>,
    compression: CompressionType,
    encryptor: Option<&dyn Encryptor>,
    dek_version: u32,
    tenant_id: TenantId,
    file_id: u64,
    block_id: u64,
) -> Result<Block, EncryptError> {
    // §5.3-6：块内行数达到阈值才启用「块×列」并行；小块串行避免调度开销。
    let parallel = (end_row - start_row) >= PARALLEL_ENCODE_MIN_ROWS;
    encode_block_impl(
        chunk,
        sorted_order,
        start_row,
        end_row,
        column_count,
        max_wal_seq,
        compression,
        encryptor,
        dek_version,
        tenant_id,
        file_id,
        block_id,
        parallel,
    )
}

/// 线程局部复用的编码 scratch：跨该线程全部 block 复用压缩器上下文 + ts/qual 编码缓冲。
///
/// 每 worker 线程一份（`map_init` 创建），把 10 万次「建 CCtx / 分配输出 buffer /
/// 分配 ts_slice/qual_slice」降为 ~N 次（N = flush 池线程数）。`Block` 仍拥有其
/// 流字节（最终 owned 分配不可免，但只剩「压缩流 → Block」这一次）。
pub(crate) struct EncodeScratch {
    /// 可复用压缩上下文（LZ4 复用输出 buffer，ZSTD9 复用 CCtx）。
    compressor: Compressor,
    /// ts 流编码缓冲（替代每表新建 `Vec<i64>`）。
    ts_slice: Vec<i64>,
    /// qual 流编码缓冲（替代每表新建 `Vec<u8>`）。
    qual_slice: Vec<u8>,
}

impl EncodeScratch {
    fn new(ct: CompressionType) -> Self {
        Self {
            compressor: Compressor::new(ct),
            ts_slice: Vec::new(),
            qual_slice: Vec::new(),
        }
    }
}

/// `encode_block` 的实现（`parallel` 由调用方显式指定，供测试对比串/并行字节一致性）。
///
/// 仅供测试（串/并行字节一致性参照）与 `encode_block` 使用；生产写路径一律走
/// [`encode_block_with_scratch`]（复用缓冲）。与 `encode_block_with_scratch` 逐字节一致。
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn encode_block_impl(
    chunk: &ColumnarChunk,
    sorted_order: &[usize],
    start_row: usize,
    end_row: usize,
    column_count: u16,
    max_wal_seq: Option<u64>,
    compression: CompressionType,
    encryptor: Option<&dyn Encryptor>,
    dek_version: u32,
    tenant_id: TenantId,
    file_id: u64,
    block_id: u64,
    parallel: bool,
) -> Result<Block, EncryptError> {
    let mut scratch = EncodeScratch::new(compression);
    encode_block_with_scratch(
        chunk,
        sorted_order,
        &mut scratch,
        start_row,
        end_row,
        column_count,
        max_wal_seq,
        compression,
        encryptor,
        dek_version,
        tenant_id,
        file_id,
        block_id,
        parallel,
    )
}

/// 用线程局部 `scratch` 编码一个 Block（与 `encode_block_impl` 逐字节一致，仅复用缓冲）。
///
/// `compression` 仍作为入参传入，供列级分支为每条 val 流创建独立的 fresh `Compressor`
/// （fresh 实例首压恒确定，实例差异不改变字节；见下方 CCtx 复用字节不确定性说明）。
#[allow(clippy::too_many_arguments)]
fn encode_block_with_scratch(
    chunk: &ColumnarChunk,
    sorted_order: &[usize],
    scratch: &mut EncodeScratch,
    start_row: usize,
    end_row: usize,
    column_count: u16,
    max_wal_seq: Option<u64>,
    compression: CompressionType,
    encryptor: Option<&dyn Encryptor>,
    dek_version: u32,
    tenant_id: TenantId,
    file_id: u64,
    block_id: u64,
    parallel: bool,
) -> Result<Block, EncryptError> {
    // E2：按 sorted_order 取本 Block 的行位置（有序 path 恒为连续下标；乱序 path 为 ts 升序的物理位置）。
    let positions: &[usize] = &sorted_order[start_row..end_row];

    // 时间戳流（DoD → 压缩 → 加密）
    scratch.ts_slice.clear();
    scratch
        .ts_slice
        .extend(positions.iter().map(|&p| chunk.timestamps[p]));
    let ts_encoded = DoDEncoder::encode(&scratch.ts_slice);
    let ts_compressed = scratch.compressor.compress(&ts_encoded);
    let ts_ctx = EncryptContext::sac_stream(tenant_id, file_id, block_id, 0, dek_version);
    let ts_stream = encrypt_with(encryptor, ts_compressed, &ts_ctx)?;

    // 质量码流（RLE → 压缩 → 加密，per-point 单质量流，O2=A）
    scratch.qual_slice.clear();
    scratch
        .qual_slice
        .extend(positions.iter().map(|&p| chunk.qualities[p]));
    let qual_encoded = RleEncoder::encode(&scratch.qual_slice);
    let qual_compressed = scratch.compressor.compress(&qual_encoded);
    let qual_stream_id = column_count as u32 + 1;
    let qual_ctx =
        EncryptContext::sac_stream(tenant_id, file_id, block_id, qual_stream_id, dek_version);
    let qual_stream = encrypt_with(encryptor, qual_compressed, &qual_ctx)?;

    // 各列 val 流（按 column_id 升序，每列独立编码 + 独立压缩 + 独立加密，O1=A）。
    // val 流的 type_tag 保持明文，只加密其后的压缩 payload。
    let mut col_ids: Vec<u16> = chunk.columns.keys().copied().collect();
    col_ids.sort_unstable();

    // §5.3-3「块×列」扁平并行：列数 > 1 且 `parallel` 置位时按列并行编码 + 压缩；
    // 小块/单列退化为串行（§5.3-6）。rayon ordered collect 保序 → 目录仍按 column_id 升序。
    //
    // 关键正确性约束（ZSTD9 CCtx 复用字节不确定性）：`zstd::bulk::Compressor` 虽在每次
    // `ZSTD_compress2` 调用开头执行 `ZSTD_CCtx_reset(session_only)`，但复用 workspace 的
    // 窗口大小 / 帧头协商仍受**先前压缩历史**影响——实测同一 val 输入，fresh CCtx 与
    // 「被 ts/qual 预热过的 CCtx」产出不同字节（帧头/总长不同）。若并行分支用 `map_init`
    // 每 worker 复用一个 CCtx，各列预热序列取决于 rayon 调度 → 输出不确定、且与串行分支
    // 不一致。因此**每条 val 流都用独立 fresh `Compressor`**（fresh 实例首压恒确定），
    // 串/并行字节严格一致。ts/qual 流仍复用 `scratch.compressor`（同一 writer 内序列
    // 恒为 ts→qual，跨 block 复用确定性由「批量 vs 逐表」字节一致测试锁死）。
    let val_streams: Vec<BlockValStream> = if parallel && col_ids.len() > 1 {
            crate::sac::parallel::flush_pool().install(|| {
                col_ids
                    .par_iter()
                    .enumerate()
                    .map(|(i, col_id)| {
                        let col = &chunk.columns[col_id];
                        let (tag, encoded) =
                            encode_column(col, positions, chunk.presence.get(col_id));
                        let mut comp = Compressor::new(compression);
                        let compressed = comp.compress(&encoded);
                        let val_ctx = EncryptContext::sac_stream(
                            tenant_id,
                            file_id,
                            block_id,
                            (i + 1) as u32,
                            dek_version,
                        );
                        let data = encrypt_with(encryptor, compressed, &val_ctx)?;
                        Ok(BlockValStream {
                            column_id: *col_id,
                            tag,
                            data,
                        })
                    })
                    .collect::<Result<Vec<BlockValStream>, EncryptError>>()
            })?
        } else {
            let mut streams = Vec::with_capacity(col_ids.len());
            for (i, col_id) in col_ids.iter().enumerate() {
                let col = &chunk.columns[col_id];
                let (tag, encoded) =
                    encode_column(col, positions, chunk.presence.get(col_id));
                let mut comp = Compressor::new(compression);
                let compressed = comp.compress(&encoded);
                let val_ctx = EncryptContext::sac_stream(
                    tenant_id,
                    file_id,
                    block_id,
                    (i + 1) as u32,
                    dek_version,
                );
                let data = encrypt_with(encryptor, compressed, &val_ctx)?;
                streams.push(BlockValStream {
                    column_id: *col_id,
                    tag,
                    data,
                });
            }
            streams
        };

    let block_min_ts = scratch.ts_slice.first().copied().unwrap_or(0);
    let block_max_ts = scratch.ts_slice.last().copied().unwrap_or(0);
    let block_row_count = (end_row - start_row) as u32;

    // Stream Directory：entry[0]=ts、entry[1..=N]=val 各列、entry[N+1]=qual（S4）
    let dir_entry_count = column_count as usize + 2;
    let dir_size = dir_entry_count * crate::sac::block::STREAM_DIR_ENTRY_SIZE;

    let mut entries: Vec<StreamDirEntry> = Vec::with_capacity(dir_entry_count);
    let mut cursor: usize = crate::sac::block::BLOCK_MINI_HEADER_SIZE + dir_size;

    // entry[0] = ts（加密后 size），column_id 用哨兵 0xFFFF
    entries.push(StreamDirEntry {
        column_id: TS_STREAM_COLUMN_ID,
        reserved: 0,
        offset: cursor as u32,
        size: ts_stream.len() as u32,
    });
    cursor += ts_stream.len();

    // entry[1..=N] = val 各列（tag 明文 + 加密后 payload），column_id 落各列真实 col_id
    for s in &val_streams {
        entries.push(StreamDirEntry {
            column_id: s.column_id,
            reserved: 0,
            offset: cursor as u32,
            size: (1 + s.data.len()) as u32,
        });
        cursor += 1 + s.data.len();
    }

    // entry[N+1] = qual（加密后 size），column_id 用哨兵 0xFFFE
    entries.push(StreamDirEntry {
        column_id: QUAL_STREAM_COLUMN_ID,
        reserved: 0,
        offset: cursor as u32,
        size: qual_stream.len() as u32,
    });

    let stream_directory = StreamDirectory { entries };

    // mini-header 的 val/qual_stream_offset 降级为「冗余兼容字段」（S4），
    // 仍写入首个 val 流 / qual 流的目录偏移以保持自描述，reader 以目录为准。
    let val_stream_offset = stream_directory
        .entries
        .get(1)
        .map(|e| e.offset)
        .unwrap_or(0);
    let qual_stream_offset = stream_directory
        .entries
        .last()
        .map(|e| e.offset)
        .unwrap_or(0);

    let mini_header = BlockMiniHeader::new(
        column_count,
        chunk.table_id,
        block_row_count,
        block_min_ts,
        block_max_ts,
        val_stream_offset,
        qual_stream_offset,
        max_wal_seq,
        0,
    );

    let mut block = Block {
        header: mini_header,
        stream_directory,
        val_streams,
        qual_stream,
        ts_stream,
        crc32c: 0,
    };
    // S6：每 Block 尾部 CRC32C，覆盖 mini-header + Stream Directory + 全部（密文）流
    block.crc32c = block.compute_crc();
    Ok(block)
}

/// 获取当前 epoch 纳秒时间戳（用于 write_epoch 缺省值）。
fn current_epoch_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// 计时埋点辅助：把 `Instant` 起始点换算为「毫秒（精确到 0.1ms）」的 f64。
#[inline]
fn phase_ms(start: Instant) -> f64 {
    (start.elapsed().as_secs_f64() * 1000.0 * 10.0).round() / 10.0
}

/// 把 epoch 纳秒时间戳格式化为 `YYYY-MM-DD HH:MM:SS` 的 UTC 人类可读字符串。
///
/// 仅用于观测日志：让 `min_ts` / `max_ts` 的纳秒原始值有对应的人类可读形式，
/// 便于用户一眼区分「数据时间」与「落盘时间」。不参与任何数据逻辑。
#[inline]
fn ns_to_utc_string(ts_ns: i64) -> String {
    let secs = ts_ns.div_euclid(1_000_000_000);
    let nsecs = ts_ns.rem_euclid(1_000_000_000) as u32;
    match chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => "<out-of-range>".to_string(),
    }
}

/// 按列物理类型分派编码器，返回 `(tag, encoded_bytes)`。
///
/// | ColVec | 编码器 | tag |
/// |--------|--------|-----|
/// | F64 | Gorilla (f64) | `VALUE_TAG_GORILLA` |
/// | F32 | Gorilla (f32 原生, O3=A) | `VALUE_TAG_GORILLA_F32` |
/// | I64 | Simple8b (zigzag) | `VALUE_TAG_SIMPLE8B` |
/// | U64 | Simple8b (原始位宽) | `VALUE_TAG_SIMPLE8B_U64` |
/// | I32 | Simple8b (zigzag) | `VALUE_TAG_SIMPLE8B_I32` |
/// | Enum16 | Simple8b (原始位宽) | `VALUE_TAG_SIMPLE8B_ENUM16` |
/// | String | Dictionary | `VALUE_TAG_DICTIONARY` |
/// | Bool | RLE(0/1) | `VALUE_TAG_BOOL_RLE` |
///
/// 若本列本块含 null，在值 payload 前拼 presence 位图并把 tag 置 `0x80`；
/// 全 dense 列直接返回 `(base_tag, encoded)`（零额外字节）。
fn encode_column(col: &ColVec, positions: &[usize], presence: Option<&Vec<bool>>) -> (u8, Vec<u8>) {
    // 1) 原 8 变体分派，得到 (基础 tag, 值编码字节)。
    let (base_tag, encoded) = encode_values(col, positions);

    // 2) 构建位图；presence 为 None（外部直构 chunk / 旧测试夹具）→ 按 dense 回退。
    match build_null_bitmap(positions, presence) {
        None => (base_tag, encoded),
        Some(bitmap) => {
            let mut payload = bitmap;
            payload.extend_from_slice(&encoded); // [bitmap][encoded] 一起进 LZ4
            (base_tag | VALUE_TAG_NULL_BITMAP, payload)
        }
    }
}

/// 按列物理类型分派编码器，返回 `(base_tag, encoded_bytes)`（8 变体，不含 null 位图）。
fn encode_values(col: &ColVec, positions: &[usize]) -> (u8, Vec<u8>) {
    match col {
        ColVec::F64(v) => {
            let s: Vec<f64> = positions.iter().map(|&p| v[p]).collect();
            (VALUE_TAG_GORILLA, GorillaEncoder::encode(&s))
        }
        ColVec::F32(v) => {
            let s: Vec<f32> = positions.iter().map(|&p| v[p]).collect();
            (VALUE_TAG_GORILLA_F32, GorillaF32Encoder::encode(&s))
        }
        ColVec::I64(v) => {
            let s: Vec<i64> = positions.iter().map(|&p| v[p]).collect();
            (VALUE_TAG_SIMPLE8B, Simple8bEncoder::encode(&s))
        }
        ColVec::U64(v) => {
            let s: Vec<u64> = positions.iter().map(|&p| v[p]).collect();
            (VALUE_TAG_SIMPLE8B_U64, simple8b::encode_u64(&s))
        }
        ColVec::I32(v) => {
            let s: Vec<i64> = positions.iter().map(|&p| v[p] as i64).collect();
            (VALUE_TAG_SIMPLE8B_I32, Simple8bEncoder::encode(&s))
        }
        ColVec::Enum16(v) => {
            let s: Vec<u64> = positions.iter().map(|&p| v[p] as u64).collect();
            (VALUE_TAG_SIMPLE8B_ENUM16, simple8b::encode_u64(&s))
        }
        ColVec::Bool(v) => {
            let s: Vec<u8> = positions
                .iter()
                .map(|&p| if v[p] { 1u8 } else { 0u8 })
                .collect();
            (VALUE_TAG_BOOL_RLE, RleEncoder::encode(&s))
        }
        ColVec::String(v) => {
            let s: Vec<String> = positions.iter().map(|&p| v[p].clone()).collect();
            (VALUE_TAG_DICTIONARY, DictionaryEncoder::encode(&s))
        }
    }
}

/// 扫描 positions 上每行的 presence：全 present 返回 `None`；否则返回 `ceil(n/8)` 字节 bitset。
///
/// bit i = 1 表示 `positions[i]` present。presence 越界时按 present 处理（保守，不吞真值）。
fn build_null_bitmap(positions: &[usize], presence: Option<&Vec<bool>>) -> Option<Vec<u8>> {
    let presence = presence?; // None → dense 快路径
    let mut any_null = false;
    let mut bitmap = vec![0u8; (positions.len() + 7) / 8];
    for (i, &p) in positions.iter().enumerate() {
        let present = presence.get(p).copied().unwrap_or(true);
        if !present {
            any_null = true;
        } else {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    if any_null {
        Some(bitmap)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::VALUE_TAG_BASE_MASK;
    use crate::memtable::col_vec::{ColVec, TypedValue};
    use crate::sac::reader::SacReader;
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[test]
    fn test_ns_to_utc_string_human_readable() {
        // 1451606400s = 2016-01-01 00:00:00 UTC（任务示例值）。
        assert_eq!(
            ns_to_utc_string(1_451_606_400_000_000_000),
            "2016-01-01 00:00:00"
        );
        // 1451692799s = 2016-01-01 23:59:59 UTC（任务示例值）。
        assert_eq!(
            ns_to_utc_string(1_451_692_799_000_000_000),
            "2016-01-01 23:59:59"
        );
        // epoch 原点。
        assert_eq!(ns_to_utc_string(0), "1970-01-01 00:00:00");
    }

    #[test]
    fn test_write_and_read_sac() {
        let tmp_dir = TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0, 4.0, 5.0]));

        // A5：ts_index 是唯一真相源，测试夹具需填充 ts→pos。
        let ts_index: std::collections::BTreeMap<i64, usize> = [100i64, 200, 300, 400, 500]
            .into_iter()
            .enumerate()
            .map(|(i, ts)| (ts, i))
            .collect();

        let chunk = ColumnarChunk {
            table_id: 42,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps: vec![100, 200, 300, 400, 500],
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; 5],
            ts_index,
            point_count: 5,
            out_of_order: false,
            max_wal_seq: Some(7),
            min_wal_seq: None,
        };

        let path = writer
            .write_chunk(&chunk, 1_692_180_000_000_000_000, SacLevel::L0, 1, Some(7))
            .expect("write failed");

        assert!(path.exists());
        assert!(path.to_str().unwrap().ends_with(".sac"));
    }

    /// D6 验收：ZSTD9 真接入——L2 块 ts 流必须是 zstd frame（magic 28 B5 2F FD），
    /// L0 块仍是 LZ4 block（不带 zstd magic；L1 起已是 ZSTD9）。写读一致性由 reader 侧
    /// `SacLevel::compression()` 分派保证。
    #[test]
    fn test_l2_block_uses_zstd_not_lz4() {
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(vec![1.0, 2.0, 3.0, 4.0, 5.0]));

        let ts_index: std::collections::BTreeMap<i64, usize> = [100i64, 200, 300, 400, 500]
            .into_iter()
            .enumerate()
            .map(|(i, ts)| (ts, i))
            .collect();
        let chunk = ColumnarChunk {
            table_id: 42,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps: vec![100, 200, 300, 400, 500],
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; 5],
            ts_index,
            point_count: 5,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        };
        let sorted_order: Vec<usize> = chunk.sorted_positions();

        let zstd_block = encode_block(
            &chunk,
            &sorted_order,
            0,
            5,
            1,
            None,
            CompressionType::Zstd9,
            None,
            0,
            sensearcdb_common::TenantId::DEFAULT,
            42,
            0,
        )
        .expect("encode zstd block");

        // Zstandard frame magic = 0xFD2FB528（小端）→ 字节序 28 B5 2F FD。
        assert!(
            zstd_block.ts_stream.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]),
            "L2 块必须用 zstd 压缩（ts 流应带 zstd frame magic），实际前 4 字节 {:02x?}",
            &zstd_block.ts_stream.get(..4).unwrap_or(&[])
        );

        let lz4_block = encode_block(
            &chunk,
            &sorted_order,
            0,
            5,
            1,
            None,
            CompressionType::Lz4,
            None,
            0,
            sensearcdb_common::TenantId::DEFAULT,
            42,
            0,
        )
        .expect("encode lz4 block");

        assert!(
            !lz4_block.ts_stream.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]),
            "L0 块必须用 lz4 压缩（ts 流不得带 zstd frame magic；L1 起为 ZSTD9）"
        );
    }

    /// A4+A5 端到端（真实 append 路径 → flush → 读回）：
    /// 同 ts 重写产生孤儿 + 乱序 append 的混合场景，flush 后 SAC 必须
    /// 1) 同 ts 只 1 条（跳过孤儿、取最新值）；2) ts 升序、值按 ts 正确对应。
    #[test]
    fn test_flush_lww_orphan_skip_and_out_of_order() {
        let tmp_dir = TempDir::new().unwrap();
        let writer = SacWriter::new(tmp_dir.path().join("sac"), CompressionType::Lz4);

        let f64s = |v: &[(u16, f64)]| -> Vec<(u16, TypedValue)> {
            v.iter().map(|(id, x)| (*id, TypedValue::F64(*x))).collect()
        };

        let mut chunk = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, 7);
        // 乱序 + 同 ts 重写：物理 append 顺序 [300, 100, 200, 100]。
        chunk.append(300, &f64s(&[(0, 30.0)]), &[(0, 0xC0)], 0);
        chunk.append(100, &f64s(&[(0, 10.0)]), &[(0, 0xC0)], 0); // 乱序
        chunk.append(200, &f64s(&[(0, 20.0)]), &[(0, 0xC0)], 0);
        chunk.append(100, &f64s(&[(0, 99.0)]), &[(0, 0xC0)], 0); // 同 ts LWW 覆盖

        // 4 次 append，但唯一 ts = 3（ts=100 被覆盖为孤儿）。
        assert_eq!(chunk.point_count, 4);
        assert_eq!(chunk.ts_index.len(), 3, "唯一 ts 应为 3（孤儿不计入）");

        let path = writer
            .write_chunk(&chunk, 0, SacLevel::L0, 1, None)
            .expect("write failed");

        let rows = SacReader::read_range(
            &path,
            sensearcdb_common::TenantId::DEFAULT,
            7,
            0,
            500,
            &[0],
            None,
        )
        .expect("read failed");

        assert_eq!(
            rows.len(),
            3,
            "同 ts 孤儿应被跳过，SAC 只含 3 条（不产生两条记录）"
        );
        assert_eq!(rows[0].ts, 100);
        assert_eq!(rows[0].values[0], (0, TypedValue::F64(99.0)));
        assert_eq!(rows[1].ts, 200);
        assert_eq!(rows[1].values[0], (0, TypedValue::F64(20.0)));
        assert_eq!(rows[2].ts, 300);
        assert_eq!(rows[2].values[0], (0, TypedValue::F64(30.0)));
    }

    /// T01/T02：全 dense 列零额外字节——presence 为 None 或全 present 时
    /// `encode_column` 不加 `0x80`，payload 与原格式逐字节一致。
    #[test]
    fn test_encode_column_dense_no_bitmap() {
        let col = ColVec::F64(vec![1.0, 2.0, 3.0]);
        let positions = vec![0, 1, 2];

        // presence None（外部直构 chunk / 旧夹具）→ dense 快路径。
        let (tag_none, payload_none) = encode_column(&col, &positions, None);
        assert_eq!(
            tag_none & VALUE_TAG_NULL_BITMAP,
            0,
            "presence None 不应置 0x80"
        );

        // 全 present → 同样 dense 快路径，且 payload 与 None 路径一致。
        let (tag_all, payload_all) = encode_column(&col, &positions, Some(&vec![true, true, true]));
        assert_eq!(tag_all & VALUE_TAG_NULL_BITMAP, 0, "全 present 不应置 0x80");
        assert_eq!(payload_none, payload_all, "dense payload 应与无位图时一致");
    }

    /// T01/T03：含 null 列置 `0x80`，payload = `[bitmap][encoded]`，位图 bit0/bit(N-1) 正确。
    #[test]
    fn test_encode_column_sparse_sets_bitmap() {
        let col = ColVec::F64(vec![1.0, 0.0, 3.0]);
        let positions = vec![0, 1, 2];
        let presence = vec![true, false, true];

        let (tag, payload) = encode_column(&col, &positions, Some(&presence));
        assert_eq!(
            tag & VALUE_TAG_NULL_BITMAP,
            VALUE_TAG_NULL_BITMAP,
            "含 null 必须置 0x80"
        );
        assert_eq!(
            tag & VALUE_TAG_BASE_MASK,
            VALUE_TAG_GORILLA,
            "基础 tag 应保留编码算法"
        );

        // payload[0] = 3 行位图：bit0=1, bit1=0, bit2=1。
        assert_eq!(payload[0], 0b0000_0101);

        // 位图之后的字节 = 原值编码。
        let (_, encoded) = encode_values(&col, &positions);
        assert_eq!(&payload[1..], &encoded[..]);
    }

    /// T03：`build_null_bitmap` 首/尾 null 位、全 present、presence None、越界保守 present。
    #[test]
    fn test_build_null_bitmap_first_last_and_bounds() {
        let positions = vec![0, 1, 2];

        // 首行 + 末行 null → 仅 bit1 present。
        let bm = build_null_bitmap(&positions, Some(&vec![false, true, false])).expect("has nulls");
        assert_eq!(bm.len(), 1);
        assert_eq!(bm[0], 0b0000_0010, "bit0 与 bit2 应为 null，bit1 present");

        // 全 present → None。
        assert!(build_null_bitmap(&positions, Some(&vec![true, true, true])).is_none());

        // presence None → None（dense 快路径）。
        assert!(build_null_bitmap(&positions, None).is_none());

        // presence 越界按 present 保守：仅 row0 已知 null，row1/row2 越界 → present。
        let bm2 = build_null_bitmap(&positions, Some(&vec![false])).expect("has nulls");
        assert_eq!(bm2[0], 0b0000_0110, "越界行必须保守按 present，不吞真值");
    }

    /// 构造一个含 F64 列的单表 chunk（ts 升序，ts_index 为唯一真相源）。
    fn f64_chunk(table_id: u32, timestamps: &[i64], values: &[f64]) -> ColumnarChunk {
        let mut columns = HashMap::new();
        columns.insert(0, ColVec::F64(values.to_vec()));
        let ts_index: std::collections::BTreeMap<i64, usize> = timestamps
            .iter()
            .copied()
            .enumerate()
            .map(|(i, ts)| (ts, i))
            .collect();
        ColumnarChunk {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            timestamps: timestamps.to_vec(),
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0; timestamps.len()],
            ts_index,
            point_count: timestamps.len(),
            out_of_order: false,
            max_wal_seq: Some(7),
            min_wal_seq: None,
        }
    }

    /// 必测项 4：多表写入 → 单文件聚合 → 按表读回，数据一致且互不串表。
    ///
    /// 通过 `begin_write` + 逐 chunk `append_chunk` + `finish` 走「批量 flush」真实
    /// 路径：一个 L0 文件聚合 3 张表，footer.table_count == 3，TableIndex 二分能定位
    /// 每张表并正确读回。
    #[test]
    fn test_multi_table_write_read_roundtrip() {
        let tmp_dir = TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        // 按 (table_id, min_ts) 升序 append：表 1 → 表 2 → 表 3。
        let c1 = f64_chunk(1, &[100, 200, 300], &[1.0, 2.0, 3.0]);
        let c2 = f64_chunk(2, &[400, 500], &[4.0, 5.0]);
        let c3 = f64_chunk(3, &[600], &[6.0]);

        let mut builder = writer
            .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
            .expect("begin_write");
        builder.append_chunk(&c1, Some(7)).expect("append t1");
        builder.append_chunk(&c2, Some(7)).expect("append t2");
        builder.append_chunk(&c3, Some(7)).expect("append t3");
        let path = builder.finish().expect("finish");

        // 每张表读回：TableIndex 二分定位各自 L2 区间，不串表。
        let rows1 = SacReader::read_range(
            &path,
            sensearcdb_common::TenantId::DEFAULT,
            1,
            0,
            1000,
            &[0],
            None,
        )
        .expect("read t1");
        assert_eq!(rows1.len(), 3);
        assert_eq!(
            rows1.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![100, 200, 300]
        );
        assert_eq!(rows1[0].table_id, 1);
        assert_eq!(rows1[0].values[0], (0, TypedValue::F64(1.0)));
        assert_eq!(rows1[2].values[0], (0, TypedValue::F64(3.0)));

        let rows2 = SacReader::read_range(
            &path,
            sensearcdb_common::TenantId::DEFAULT,
            2,
            0,
            1000,
            &[0],
            None,
        )
        .expect("read t2");
        assert_eq!(rows2.len(), 2);
        assert_eq!(
            rows2.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![400, 500]
        );
        assert_eq!(rows2.iter().all(|r| r.table_id == 2), true);

        let rows3 = SacReader::read_range(
            &path,
            sensearcdb_common::TenantId::DEFAULT,
            3,
            0,
            1000,
            &[0],
            None,
        )
        .expect("read t3");
        assert_eq!(rows3.len(), 1);
        assert_eq!(rows3[0].ts, 600);
        assert_eq!(rows3[0].table_id, 3);
        assert_eq!(rows3[0].values[0], (0, TypedValue::F64(6.0)));

        // 不存在的表 → 空（TableIndex 二分未命中）。
        let rows_none = SacReader::read_range(
            &path,
            sensearcdb_common::TenantId::DEFAULT,
            99,
            0,
            1000,
            &[0],
            None,
        )
        .expect("read missing table");
        assert!(rows_none.is_empty());
    }

    /// 必测项 5：不变式 I2/I3——写入后盘上块按 `(table_id, min_ts)` 升序，
    /// TableIndex 条目按 table_id 升序且 `first_l2_idx` 单调，L2 与 TableIndex 完全覆盖。
    #[test]
    fn test_block_sorting_invariant_table_id_min_ts() {
        let tmp_dir = TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        // 每表多个 chunk（跨多块/多段），仍按 (table_id, min_ts) 升序 append。
        let c1 = f64_chunk(1, &[100, 200], &[1.0, 2.0]);
        let c1b = f64_chunk(1, &[300, 400], &[3.0, 4.0]); // 同表续块（min_ts 升序）
        let c2 = f64_chunk(2, &[500], &[5.0]);
        let c3 = f64_chunk(3, &[600, 700], &[6.0, 7.0]);

        let mut builder = writer
            .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
            .expect("begin_write");
        builder.append_chunk(&c1, None).expect("append c1");
        builder.append_chunk(&c1b, None).expect("append c1b");
        builder.append_chunk(&c2, None).expect("append c2");
        builder.append_chunk(&c3, None).expect("append c3");
        let path = builder.finish().expect("finish");

        let raw = std::fs::read(&path).expect("read raw sac");
        let footer_size = sensearcdb_common::constants::SAC_FOOTER_SIZE;
        let footer = SacFooter::from_bytes(
            &raw[raw.len() - footer_size..]
                .try_into()
                .expect("footer slice"),
        )
        .expect("parse footer");

        assert_eq!(footer.table_count, 3, "3 张表应产出 3 条 TableIndex");
        assert_eq!(footer.block_count, 4, "4 个 chunk 应产出 4 个 block");

        // 读 TableIndex：按 table_id 升序 + first_l2_idx 单调。
        let entry_size = sensearcdb_common::constants::SAC_TABLE_INDEX_ENTRY_SIZE;
        let ti_start = footer.table_index_offset as usize;
        let mut tables = Vec::new();
        for i in 0..footer.table_count as usize {
            let s = ti_start + i * entry_size;
            let e = s + entry_size;
            let entry = TableIndexEntry::from_bytes(&raw[s..e].try_into().expect("entry slice"))
                .expect("parse table entry");
            tables.push(entry);
        }
        let mut prev_table = 0u32;
        let mut prev_l2 = 0u32;
        for t in &tables {
            assert!(t.table_id > prev_table, "TableIndex 必须按 table_id 升序");
            assert!(t.first_l2_idx >= prev_l2, "first_l2_idx 必须单调不减");
            assert!(t.block_count > 0, "每表至少 1 块");
            prev_table = t.table_id;
            prev_l2 = t.first_l2_idx;
        }
        assert_eq!(tables[0].table_id, 1);
        assert_eq!(tables[1].table_id, 2);
        assert_eq!(tables[2].table_id, 3);
        // 表 1 有两个连续块（c1 + c1b）。
        assert_eq!(tables[0].block_count, 2);
        assert_eq!(tables[0].first_l2_idx, 0);
        assert_eq!(tables[1].first_l2_idx, 2);
        assert_eq!(tables[2].first_l2_idx, 3);

        // 读 L2：整数组按 (table_id, min_ts) 非降序，且与 TableIndex 区间吻合。
        let l2_size = sensearcdb_common::constants::SAC_L2_ENTRY_SIZE;
        let l2_start = footer.l2_offset as usize;
        let l2_count = footer.block_count as usize;
        let mut prev = (0u32, i64::MIN);
        for i in 0..l2_count {
            let s = l2_start + i * l2_size;
            let e = s + l2_size;
            let l2 = L2Descriptor::from_bytes(&raw[s..e].try_into().expect("l2 slice"))
                .expect("parse l2");
            let key = (l2.table_id, l2.min_ts);
            assert!(
                key >= prev,
                "L2 必须按 (table_id, min_ts) 非降序，got {key:?} after {prev:?}"
            );
            prev = key;
        }
    }

    /// 构造含 `ncols` 列（奇数列 F64、偶数列 I64）的 `n` 行 chunk（ts 升序）。
    fn typed_chunk(table_id: u32, n: usize, ncols: usize) -> ColumnarChunk {
        let mut chunk = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, table_id);
        for i in 0..n {
            let mut values = Vec::with_capacity(ncols);
            for c in 0..ncols {
                if c % 2 == 0 {
                    values.push((c as u16, TypedValue::F64(i as f64 + c as f64)));
                } else {
                    values.push((c as u16, TypedValue::I64(i as i64 + c as i64)));
                }
            }
            chunk.append((i as i64) * 1000, &values, &[(0, 0xC0)], 0);
        }
        chunk
    }

    /// 阶段二验收：块×列并行编码结果与串行编码逐字节一致。
    ///
    /// 通过 `encode_block_impl(parallel=true/false)` 强制两条路径编码同一块，
    /// 断言字节级完全一致（含 Stream Directory 顺序、CRC、压缩流），证明并行化
    /// 未改变编码输出（不变式 2/4）。同时覆盖 LZ4 与 ZSTD9：每条 val 流都用独立
    /// fresh `Compressor`，串/并行仅调度方式不同，字节仍须一致（锁死 fresh 实例确定性）。
    #[test]
    fn test_parallel_vs_serial_encode_block_byte_identical() {
        let n = 4096; // 满块：恰好触发列级并行阈值。
        let chunk = typed_chunk(7, n, 3);
        let sorted_order: Vec<usize> = chunk.sorted_positions();
        assert_eq!(sorted_order.len(), n);
        let column_count = chunk.columns.len() as u16;

        for compression in [CompressionType::Lz4, CompressionType::Zstd9] {
            let parallel = encode_block_impl(
                &chunk,
                &sorted_order,
                0,
                n,
                column_count,
                None,
                compression,
                None,
                0,
                sensearcdb_common::TenantId::DEFAULT,
                7,
                0,
                true,
            )
            .expect("parallel encode");

            let serial = encode_block_impl(
                &chunk,
                &sorted_order,
                0,
                n,
                column_count,
                None,
                compression,
                None,
                0,
                sensearcdb_common::TenantId::DEFAULT,
                7,
                0,
                false,
            )
            .expect("serial encode");

            assert_eq!(
                parallel.to_bytes(),
                serial.to_bytes(),
                "块×列并行编码必须与串行编码逐字节一致（{compression:?}）"
            );
        }
    }

    /// 阶段二验收：多块 chunk 并行编码后 block_id 连续、写盘按序组装（不变式 2）。
    ///
    /// 9000 行 → 3 块（4096 + 4096 + 808）。断言 footer.block_count == 3、L2 按
    /// `(table_id, min_ts)` 升序、块 offset 首尾相接无间隙，并读回 9000 行无损无重。
    #[test]
    fn test_append_chunk_parallel_blocks_ordered_contiguous() {
        let tmp_dir = TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);

        let n = 9000usize; // 3 blocks
        let chunk = typed_chunk(42, n, 1);
        let sorted_order: Vec<usize> = chunk.sorted_positions();
        assert_eq!(sorted_order.len(), n);

        let mut builder = writer
            .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
            .expect("begin_write");
        builder.append_chunk(&chunk, Some(7)).expect("append_chunk");
        let path = builder.finish().expect("finish");

        let raw = std::fs::read(&path).expect("read raw sac");
        let footer_size = sensearcdb_common::constants::SAC_FOOTER_SIZE;
        let footer = SacFooter::from_bytes(
            &raw[raw.len() - footer_size..]
                .try_into()
                .expect("footer slice"),
        )
        .expect("parse footer");
        assert_eq!(footer.block_count, 3, "9000 行应产出 3 个 block");

        let l2_size = sensearcdb_common::constants::SAC_L2_ENTRY_SIZE;
        let l2_start = footer.l2_offset as usize;
        let mut prev_min_ts = i64::MIN;
        let mut prev_end: u64 = 0;
        for i in 0..footer.block_count as usize {
            let s = l2_start + i * l2_size;
            let l2 = L2Descriptor::from_bytes(&raw[s..s + l2_size].try_into().expect("l2 slice"))
                .expect("parse l2");
            assert_eq!(l2.table_id, 42);
            assert!(
                l2.min_ts >= prev_min_ts,
                "块必须按 (table_id, min_ts) 升序（block_id 连续有序的落盘体现）"
            );
            if i > 0 {
                assert_eq!(
                    l2.block_offset, prev_end,
                    "块 offset 必须首尾相接（并行按 block_id 排序后顺序写盘，无间隙/乱序）"
                );
            }
            prev_min_ts = l2.min_ts;
            prev_end = l2.block_offset + l2.block_size as u64;
        }

        // 读回完整性：9000 行无损无重、ts 升序。
        let rows = SacReader::read_range(
            &path,
            sensearcdb_common::TenantId::DEFAULT,
            42,
            0,
            i64::MAX,
            &[0],
            None,
        )
        .expect("read back");
        assert_eq!(rows.len(), n, "并行编码后读回应无损无重");
        assert_eq!(rows.first().map(|r| r.ts), Some(0));
        assert_eq!(rows.last().map(|r| r.ts), Some(((n - 1) * 1000) as i64));
    }

    // ===================== QA 独立验证（fresh eyes，非工程师自测复述）=====================

    /// 构造多类型列 chunk：col0=F64、col1=I64、col2=String、col3=Bool，覆盖
    /// Gorilla / Simple8b(zigzag，含负值) / Dictionary / BoolRLE 四类编码器。
    fn qa_mixed_type_chunk(table_id: u32, n: usize) -> ColumnarChunk {
        let mut chunk = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, table_id);
        for i in 0..n {
            let values = vec![
                (0u16, TypedValue::F64(i as f64)),
                (1u16, TypedValue::I64(i as i64 - 5000)),
                (2u16, TypedValue::String(format!("tag-{}", i % 8))),
                (3u16, TypedValue::Bool(i % 2 == 0)),
            ];
            chunk.append((i as i64) * 1000, &values, &[(0, 0xC0)], 0);
        }
        chunk
    }

    /// QA 独立验证（强于工程师自测）：文件级确定性 + 并行块字节 == 串行块字节 + L2 连续 + file_crc。
    ///
    /// 9000 行（3 块：4096+4096+808）× 4 列（F64/I64/String/Bool）：
    /// 1) 同一 chunk 独立写两次 → 两个 .sac 逐字节一致（并行调度不引入不确定性）；
    /// 2) 盘上每个 block 的字节 == `encode_block_impl(parallel=false)` 的字节（真·并行==串行逐字节）；
    /// 3) 重新累加 header+blocks+L2+TableIndex 的 crc32c == footer.crc32c（file_crc 顺序正确）；
    /// 4) 读回 9000 行无损。
    #[test]
    fn qa_parallel_file_bytes_identical_l2_contiguous_crc_consistent() {
        let tmp_dir = TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let writer = SacWriter::new(&sac_dir, CompressionType::Lz4);
        // 第二个写入器指向不同目录但用相同 seq=1，隔离「header.seq 字段」差异以专测
        // 并行编码本身的确定性（同数据 + 同 seq → 逐字节一致）。
        let writer2 = SacWriter::new(tmp_dir.path().join("sac2"), CompressionType::Lz4);

        let n = 9000usize;
        let chunk = qa_mixed_type_chunk(42, n);
        let sorted_order: Vec<usize> = chunk.sorted_positions();
        assert_eq!(sorted_order.len(), n);
        let column_count = chunk.columns.len() as u16;

        let mut b1 = writer
            .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
            .expect("begin1");
        b1.append_chunk(&chunk, Some(7)).expect("append1");
        let p1 = b1.finish().expect("finish1");

        let mut b2 = writer2
            .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
            .expect("begin2");
        b2.append_chunk(&chunk, Some(7)).expect("append2");
        let p2 = b2.finish().expect("finish2");

        let raw1 = std::fs::read(&p1).expect("read1");
        let raw2 = std::fs::read(&p2).expect("read2");
        assert_eq!(raw1, raw2, "同一 chunk 并行 flush 两次必须逐字节一致（确定性）");

        let footer_size = sensearcdb_common::constants::SAC_FOOTER_SIZE;
        let footer = SacFooter::from_bytes(
            &raw1[raw1.len() - footer_size..]
                .try_into()
                .expect("footer slice"),
        )
        .expect("parse footer");
        assert_eq!(footer.block_count, 3, "9000 行应产出 3 块");

        let l2_size = sensearcdb_common::constants::SAC_L2_ENTRY_SIZE;
        let l2_start = footer.l2_offset as usize;
        let mut prev_end = 0u64;
        for i in 0..footer.block_count as usize {
            let s = l2_start + i * l2_size;
            let l2 = L2Descriptor::from_bytes(&raw1[s..s + l2_size].try_into().expect("l2 slice"))
                .expect("parse l2");
            assert_eq!(l2.table_id, 42);
            if i > 0 {
                assert_eq!(l2.block_offset, prev_end, "块 offset 必须首尾相接无间隙");
            }
            prev_end = l2.block_offset + l2.block_size as u64;

            let start = i * 4096;
            let end = ((i + 1) * 4096).min(n);
            let serial = encode_block_impl(
                &chunk,
                &sorted_order,
                start,
                end,
                column_count,
                Some(7),
                CompressionType::Lz4,
                None,
                0,
                sensearcdb_common::TenantId::DEFAULT,
                42,
                i as u64,
                false,
            )
            .expect("serial encode");
            let on_disk =
                &raw1[l2.block_offset as usize..(l2.block_offset + l2.block_size as u64) as usize];
            assert_eq!(
                on_disk,
                serial.to_bytes().as_slice(),
                "block {i} 并行落盘字节必须 == 串行编码字节（逐字节）"
            );
        }

        // file_crc：重新累加 header + blocks + L2 + TableIndex，须 == footer.crc32c。
        let mut hasher = crc32c::Crc32cHasher::new(0);
        hasher.write(&raw1[..raw1.len() - footer_size]);
        assert_eq!(
            hasher.finish() as u32,
            footer.crc32c,
            "file_crc 必须按序覆盖 header+blocks+L2+TableIndex"
        );

        let rows = SacReader::read_range(
            &p1,
            sensearcdb_common::TenantId::DEFAULT,
            42,
            0,
            i64::MAX,
            &[],
            None,
        )
        .expect("read back");
        assert_eq!(rows.len(), n, "读回应无损无重");
        assert_eq!(rows.first().map(|r| r.ts), Some(0));
        assert_eq!(rows.last().map(|r| r.ts), Some(((n - 1) * 1000) as i64));
    }

    /// QA 独立验证 TDE：mock 加密器（XOR + 唯一 nonce 计数），验证并行编码后
    /// nonce 唯一、解密读回逐字节一致、block_id 预分配不影响解密。
    struct QaXorEncryptor {
        counter: std::sync::atomic::AtomicU64,
        seen: std::sync::Mutex<std::collections::HashSet<[u8; 12]>>,
    }

    impl QaXorEncryptor {
        fn new() -> Self {
            Self {
                counter: std::sync::atomic::AtomicU64::new(0),
                seen: std::sync::Mutex::new(std::collections::HashSet::new()),
            }
        }
        fn unique_nonce_count(&self) -> usize {
            self.seen.lock().unwrap().len()
        }
    }

    impl Encryptor for QaXorEncryptor {
        fn encrypt(
            &self,
            plaintext: &[u8],
            _ctx: &EncryptContext,
        ) -> Result<Vec<u8>, EncryptError> {
            let n = self
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut nonce = [0u8; 12];
            nonce[..8].copy_from_slice(&n.to_le_bytes());
            self.seen.lock().unwrap().insert(nonce);
            let mut out = Vec::with_capacity(12 + 16 + plaintext.len());
            out.extend_from_slice(&nonce);
            out.extend_from_slice(&[0u8; 16]); // mock GCM tag
            for (i, b) in plaintext.iter().enumerate() {
                out.push(b ^ nonce[i % 12]);
            }
            Ok(out)
        }
        fn decrypt(&self, blob: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
            if blob.len() < 28 {
                return Err(EncryptError::TooShort(blob.len(), 28));
            }
            let nonce = &blob[0..12];
            let ct = &blob[28..];
            let mut out = Vec::with_capacity(ct.len());
            for (i, b) in ct.iter().enumerate() {
                out.push(b ^ nonce[i % 12]);
            }
            Ok(out)
        }
        fn active_dek_version(&self, _tenant_id: sensearcdb_common::TenantId) -> u32 {
            1
        }
        fn is_enabled(&self) -> bool {
            true
        }
    }

    #[test]
    fn qa_parallel_tde_nonce_unique_and_roundtrip() {
        let tmp_dir = TempDir::new().unwrap();
        let sac_dir = tmp_dir.path().join("sac");
        let enc = std::sync::Arc::new(QaXorEncryptor::new());
        let writer =
            SacWriter::new_with_encryptor(&sac_dir, CompressionType::Lz4, Some(enc.clone()));

        let n = 9000usize;
        let chunk = qa_mixed_type_chunk(42, n);

        let mut b = writer
            .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
            .expect("begin");
        b.append_chunk(&chunk, Some(7)).expect("append");
        let path = b.finish().expect("finish");

        let rows = SacReader::read_range(
            &path,
            sensearcdb_common::TenantId::DEFAULT,
            42,
            0,
            i64::MAX,
            &[],
            Some(enc.clone()),
        )
        .expect("read back (TDE decrypt)");
        assert_eq!(rows.len(), n, "TDE 并行编码后读回应无损");

        // 逐列解密保真（含 String/Bool）。
        assert_eq!(rows[0].values[0], (0, TypedValue::F64(0.0)));
        assert_eq!(rows[0].values[1], (1, TypedValue::I64(-5000)));
        assert_eq!(
            rows[0].values[2],
            (2, TypedValue::String("tag-0".to_string()))
        );
        assert_eq!(rows[0].values[3], (3, TypedValue::Bool(true)));
        assert_eq!(rows[5].values[0], (0, TypedValue::F64(5.0)));
        assert_eq!(rows[5].values[1], (1, TypedValue::I64(-4995)));
        assert_eq!(
            rows[5].values[2],
            (2, TypedValue::String("tag-5".to_string()))
        );
        assert_eq!(rows[5].values[3], (3, TypedValue::Bool(false)));

        // nonce 唯一：3 块 × (1 ts + 4 val + 1 qual) = 18 个流，每个流一个独立 nonce。
        let streams = 3 * (1 + 4 + 1);
        assert_eq!(
            enc.unique_nonce_count(),
            streams,
            "并行编码每个流都应产生独立 nonce，无碰撞/复用"
        );
    }

    /// 第 4 步验收（红线 #4）：跨表批量 `append_chunks` 与逐表 `append_chunk` 产出逐字节一致。
    ///
    /// 覆盖 1 表 1 tag（小单列 chunk）、宽表（多列多类型）、乱序 + 同 ts LWW、
    /// 稀疏 null 列、多块（9000 行 3 列跨 3 block，验证跨块 block_id 连续），并追加
    /// 400 个单列 chunk（= 400 block，远超 flush rayon 池核数），强制触发 `map_init`
    /// 每 worker 的 `EncodeScratch`（含 `Compressor` CCtx）**跨 block 复用**路径。
    /// 分别在 LZ4 与 ZSTD9 下断言两条路径的 .sac 字节完全一致（含 header/L2/TableIndex/footer/CRC）。
    #[test]
    fn test_batch_cross_table_vs_per_table_byte_identical() {
        for compression in [CompressionType::Lz4, CompressionType::Zstd9] {
            let tmp_dir = TempDir::new().unwrap();
            let writer_batch = SacWriter::new(tmp_dir.path().join("batch"), compression);
            let writer_per = SacWriter::new(tmp_dir.path().join("per"), compression);

            let mut chunks = batch_mixed_chunks();
            // 追加 400 表 × 1 block（table_id 6..=405），推高 block 数以覆盖跨 block CCtx 复用。
            chunks.extend(batch_many_single_col_chunks(6, 400));

            // 跨表批量路径。
            let mut bb = writer_batch
                .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
                .expect("begin batch");
            bb.append_chunks(&chunks).expect("append_chunks");
            let pb = bb.finish().expect("finish batch");

            // 逐表路径（append_chunk 已切到同一套新 Compressor / encode_block_with_scratch）。
            let mut bp = writer_per
                .begin_write(0, 0, SacLevel::L0, 1, 1_700_000_000_000_000_000)
                .expect("begin per");
            for c in &chunks {
                bp.append_chunk(c.as_ref(), c.max_wal_seq)
                    .expect("append_chunk per");
            }
            let pp = bp.finish().expect("finish per");

            let raw_b = std::fs::read(&pb).expect("read batch sac");
            let raw_p = std::fs::read(&pp).expect("read per sac");
            assert_eq!(
                raw_b, raw_p,
                "append_chunks 与逐表 append_chunk 必须逐字节一致（{compression:?}）"
            );
        }
    }

    /// 构造一组「表号升序」的混合 chunk，覆盖 1 表 1 tag / 宽表 / 乱序 LWW / 稀疏 null / 多块。
    fn batch_mixed_chunks() -> Vec<Arc<ColumnarChunk>> {
        let mut chunks: Vec<Arc<ColumnarChunk>> = Vec::new();

        // t1：1 表 1 tag，单 F64 列 3 行（max_wal_seq 由 f64_chunk 置 Some(7)）。
        chunks.push(Arc::new(f64_chunk(1, &[100, 200, 300], &[1.0, 2.0, 3.0])));

        // t2：乱序 + 同 ts LWW（4 次 append → 3 个唯一 ts）。
        {
            let mut c = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, 2);
            c.append(300, &[(0, TypedValue::F64(30.0))], &[(0, 0xC0)], 0);
            c.append(100, &[(0, TypedValue::F64(10.0))], &[(0, 0xC0)], 0);
            c.append(200, &[(0, TypedValue::F64(20.0))], &[(0, 0xC0)], 0);
            c.append(100, &[(0, TypedValue::F64(99.0))], &[(0, 0xC0)], 0);
            chunks.push(Arc::new(c));
        }

        // t3：宽表多类型（F64/I64/String/Bool）。
        chunks.push(Arc::new(qa_mixed_type_chunk(3, 64)));

        // t4：稀疏 null 列（col1 在第 2 行缺失 → presence 位图）。
        {
            let mut c = ColumnarChunk::new(sensearcdb_common::TenantId::DEFAULT, 4);
            c.append(
                0,
                &[(0, TypedValue::F64(1.0)), (1, TypedValue::I64(10))],
                &[(0, 0xC0)],
                0,
            );
            c.append(1000, &[(0, TypedValue::F64(2.0))], &[(0, 0xC0)], 0); // col1 缺失 → 稀疏
            c.append(
                2000,
                &[(0, TypedValue::F64(3.0)), (1, TypedValue::I64(30))],
                &[(0, 0xC0)],
                0,
            );
            chunks.push(Arc::new(c));
        }

        // t5：多块宽表（9000 行 3 列 → 3 block），验证跨块 block_id 连续 + 块级并行。
        chunks.push(Arc::new(typed_chunk(5, 9000, 3)));

        chunks
    }

    /// 批量生成 `count` 个单列 F64 chunk（每个 3 行 = 1 block，table_id 从 `base_table` 起递增）。
    ///
    /// 用于把字节一致性测试的 block 数推到超过 flush rayon 池核数，强制触发 `map_init`
    /// 每个 worker 的 `EncodeScratch`（含 `Compressor` CCtx）跨 block 复用路径。
    fn batch_many_single_col_chunks(base_table: u32, count: usize) -> Vec<Arc<ColumnarChunk>> {
        (0..count)
            .map(|i| {
                Arc::new(f64_chunk(
                    base_table + i as u32,
                    &[100, 200, 300],
                    &[1.0, 2.0, 3.0],
                ))
            })
            .collect()
    }
}

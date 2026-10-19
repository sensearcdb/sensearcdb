//! SAC Block: mini-header + Stream Directory + 三流布局。
//!
//! 每个 Block 包含固定行数（默认 4096 行）的数据，采用列式流布局：
//! 1. 时间戳流 (ts stream): DoD 编码 + LZ4，共享时间轴，固定第一个流
//! 2. 值流 (val streams): 每列独立编码（Gorilla/Simple8b/Dictionary/BoolRLE）+ 独立 LZ4
//! 3. 质量码流 (qual stream): RLE 编码 + LZ4，per-point 单质量码（全列共享）
//!
//! ## Block 磁盘布局（SAC v9 定稿）
//!
//! ```text
//! [BlockMiniHeader: 48B]
//! [Stream Directory: (column_count+2) × 12B]   (每条含 column_id u16 + reserved u16 + offset u32 + size u32)
//! [ts_stream bytes]                    (DoD → LZ4)
//! [val_c{column_id}: tag u8 + bytes]   (Gorilla/Simple8b/Dictionary/BoolRLE → LZ4)
//! [val_c{column_id}: tag u8 + bytes]   (tag 高位 0x80 = payload 前带 presence 位图)
//! ...
//! [val_c{column_id}: tag u8 + bytes]
//! [qual_stream bytes]                  (RLE → LZ4)
//! [CRC32C: 4B]                         (覆盖 mini-header + directory + 全部流)
//! ```
//!
//! ## BlockMiniHeader 二进制布局（48B, v9 定稿 §2.3）
//!
//! | 偏移 | 大小 | 字段 |
//! |------|------|------|
//! | 0    | 2    | column_count: u16 LE |
//! | 2    | 4    | table_id: u32 LE（块归属表，块自描述，v9 新增） |
//! | 6    | 4    | row_count: u32 LE |
//! | 10   | 8    | min_ts: i64 LE |
//! | 18   | 8    | max_ts: i64 LE |
//! | 26   | 4    | val_stream_offset: u32 LE（冗余兼容字段，reader 以 Stream Directory 为准） |
//! | 30   | 4    | qual_stream_offset: u32 LE（冗余兼容字段，reader 以 Stream Directory 为准） |
//! | 34   | 8    | max_wal_seq: u64 LE (0 = None) |
//! | 42   | 1    | flags: u8 |
//! | 43   | 5    | reserved: 对齐到 48B |

use std::hash::Hasher;

/// Block mini-header 序列化大小: 48 字节。
pub const BLOCK_MINI_HEADER_SIZE: usize = 48;

/// Block 默认行数。
pub const BLOCK_DEFAULT_ROW_COUNT: u32 = 4096;

/// Stream Directory 单条目大小: 12 字节（column_id u16 + reserved u16 + offset u32 + size u32）。
pub const STREAM_DIR_ENTRY_SIZE: usize = 12;

/// ts 流目录条目的哨兵 column_id（区别于数据列 0..0xFFFD）。
pub const TS_STREAM_COLUMN_ID: u16 = 0xFFFF;
/// qual 流目录条目的哨兵 column_id（区别于数据列 0..0xFFFD）。
pub const QUAL_STREAM_COLUMN_ID: u16 = 0xFFFE;

/// Block 尾部 CRC32C 大小: 4 字节。
pub const BLOCK_CRC_SIZE: usize = 4;

/// Block mini-header：每个 Block 的元数据。
///
/// 位于 Block 数据流之前，提供列数、行数、时间范围和流偏移信息。
/// 其中 `val_stream_offset` / `qual_stream_offset` 自 S4 起降级为「冗余兼容字段」，
/// reader 一律以紧跟其后的 Stream Directory 为准。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMiniHeader {
    /// 列数。
    pub column_count: u16,
    /// 块归属表 ID（v9 多表聚合，块自描述）。
    pub table_id: u32,
    /// 行数。
    pub row_count: u32,
    /// Block 内最小时间戳（epoch 纳秒）。
    pub min_ts: i64,
    /// Block 内最大时间戳（epoch 纳秒）。
    pub max_ts: i64,
    /// 值流在 Block 内的字节偏移（冗余兼容字段，reader 以 Stream Directory 为准）。
    pub val_stream_offset: u32,
    /// 质量码流在 Block 内的字节偏移（冗余兼容字段，reader 以 Stream Directory 为准）。
    pub qual_stream_offset: u32,
    /// Block 内数据的最大 WAL 序列号。
    ///
    /// ## 三态语义（硬规则，锁死不可改，S7）
    ///
    /// - `Some(seq)`: 仅 WAL→MemTable→SAC Flush 产出。seq = Block 内最大 WAL 序列号。
    /// - `None`: Compaction / 回放 / 批量导入产出（编码为 `0`，WAL seq 从 1 起编）。
    ///
    /// Compaction 去重已消费 seq，输出设 None 防止后续 Compaction
    /// 跨 Block 错误比较 seq。
    pub max_wal_seq: Option<u64>,
    /// 标志位。
    pub flags: u8,
}

impl BlockMiniHeader {
    /// 创建新的 Block mini-header。
    pub fn new(
        column_count: u16,
        table_id: u32,
        row_count: u32,
        min_ts: i64,
        max_ts: i64,
        val_stream_offset: u32,
        qual_stream_offset: u32,
        max_wal_seq: Option<u64>,
        flags: u8,
    ) -> Self {
        Self {
            column_count,
            table_id,
            row_count,
            min_ts,
            max_ts,
            val_stream_offset,
            qual_stream_offset,
            max_wal_seq,
            flags,
        }
    }

    /// 序列化为字节数组。
    pub fn to_bytes(&self) -> [u8; BLOCK_MINI_HEADER_SIZE] {
        let mut buf = [0u8; BLOCK_MINI_HEADER_SIZE];

        buf[0..2].copy_from_slice(&self.column_count.to_le_bytes());
        buf[2..6].copy_from_slice(&self.table_id.to_le_bytes());
        buf[6..10].copy_from_slice(&self.row_count.to_le_bytes());
        buf[10..18].copy_from_slice(&self.min_ts.to_le_bytes());
        buf[18..26].copy_from_slice(&self.max_ts.to_le_bytes());
        buf[26..30].copy_from_slice(&self.val_stream_offset.to_le_bytes());
        buf[30..34].copy_from_slice(&self.qual_stream_offset.to_le_bytes());

        let wal_seq = self.max_wal_seq.unwrap_or(0);
        buf[34..42].copy_from_slice(&wal_seq.to_le_bytes());

        buf[42] = self.flags;
        // buf[43..48] = 0 (reserved)

        buf
    }

    /// 从字节数组反序列化。
    ///
    /// # 返回值
    /// 解析后的 `BlockMiniHeader`；若字节数组长度不对则返回 `None`。
    pub fn from_bytes(bytes: &[u8; BLOCK_MINI_HEADER_SIZE]) -> Option<Self> {
        let column_count = u16::from_le_bytes(bytes[0..2].try_into().ok()?);
        let table_id = u32::from_le_bytes(bytes[2..6].try_into().ok()?);
        let row_count = u32::from_le_bytes(bytes[6..10].try_into().ok()?);
        let min_ts = i64::from_le_bytes(bytes[10..18].try_into().ok()?);
        let max_ts = i64::from_le_bytes(bytes[18..26].try_into().ok()?);
        let val_stream_offset = u32::from_le_bytes(bytes[26..30].try_into().ok()?);
        let qual_stream_offset = u32::from_le_bytes(bytes[30..34].try_into().ok()?);

        let wal_seq_raw = u64::from_le_bytes(bytes[34..42].try_into().ok()?);
        let max_wal_seq = if wal_seq_raw == 0 {
            None
        } else {
            Some(wal_seq_raw)
        };

        let flags = bytes[42];

        Some(Self {
            column_count,
            table_id,
            row_count,
            min_ts,
            max_ts,
            val_stream_offset,
            qual_stream_offset,
            max_wal_seq,
            flags,
        })
    }
}

impl Default for BlockMiniHeader {
    fn default() -> Self {
        Self {
            column_count: 1,
            table_id: 0,
            row_count: 0,
            min_ts: 0,
            max_ts: 0,
            val_stream_offset: 0,
            qual_stream_offset: 0,
            max_wal_seq: None,
            flags: 0,
        }
    }
}

/// Stream Directory 单条目：`(column_id u16, reserved u16, offset u32, size u32)`，12 字节。
///
/// SAC v6 起 `column_id` 显式落盘，reader 按 `column_id` 匹配投影，
/// 不再依赖 dense 位置推导（稀疏列安全）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDirEntry {
    /// 列 ID。数据列取真实 `column_id`（0..0xFFFD）；
    /// 哨兵：ts 流 = [`TS_STREAM_COLUMN_ID`]，qual 流 = [`QUAL_STREAM_COLUMN_ID`]。
    pub column_id: u16,
    /// 保留字段，恒 0。
    pub reserved: u16,
    /// 流在 Block 内的字节偏移（相对 Block 起始 = mini-header 起点）。
    pub offset: u32,
    /// 流大小（字节）。
    pub size: u32,
}

impl StreamDirEntry {
    /// 序列化为 12 字节（column_id / reserved 用小端 u16，offset / size 用 u32）。
    pub fn to_bytes(&self) -> [u8; STREAM_DIR_ENTRY_SIZE] {
        let mut buf = [0u8; STREAM_DIR_ENTRY_SIZE];
        buf[0..2].copy_from_slice(&self.column_id.to_le_bytes());
        buf[2..4].copy_from_slice(&self.reserved.to_le_bytes());
        buf[4..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.size.to_le_bytes());
        buf
    }

    /// 从 12 字节反序列化。
    pub fn from_bytes(bytes: &[u8; STREAM_DIR_ENTRY_SIZE]) -> Self {
        let column_id = u16::from_le_bytes(bytes[0..2].try_into().unwrap_or([0u8; 2]));
        let reserved = u16::from_le_bytes(bytes[2..4].try_into().unwrap_or([0u8; 2]));
        let offset = u32::from_le_bytes(bytes[4..8].try_into().unwrap_or([0u8; 4]));
        let size = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0u8; 4]));
        Self {
            column_id,
            reserved,
            offset,
            size,
        }
    }
}

/// Block 内 Stream Directory（流目录）。
///
/// 布局（SAC v6）：`(column_count + 2) × 12B`，条目顺序固定：
/// - `entries[0]`       = ts 流（共享时间轴，固定第一个流），column_id = `0xFFFF`
/// - `entries[1..=N]`   = val 各列流（按 column_id 升序），column_id = 各列真实列 ID
/// - `entries[N+1]`     = qual 流（per-point 单质量码，全列共享），column_id = `0xFFFE`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDirectory {
    /// 目录条目列表。
    pub entries: Vec<StreamDirEntry>,
}

impl StreamDirectory {
    /// 创建空目录。
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// 目录条目总数。
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// 目录字节大小 = entry_count × 12。
    pub fn size_bytes(&self) -> usize {
        self.entries.len() * STREAM_DIR_ENTRY_SIZE
    }

    /// 序列化为字节数组。
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.size_bytes());
        for e in &self.entries {
            buf.extend_from_slice(&e.to_bytes());
        }
        buf
    }

    /// 从字节数组反序列化（长度须为 12 的整数倍）。
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() % STREAM_DIR_ENTRY_SIZE != 0 {
            return None;
        }
        let mut entries = Vec::with_capacity(bytes.len() / STREAM_DIR_ENTRY_SIZE);
        for chunk in bytes.chunks_exact(STREAM_DIR_ENTRY_SIZE) {
            let arr: [u8; STREAM_DIR_ENTRY_SIZE] = chunk.try_into().ok()?;
            entries.push(StreamDirEntry::from_bytes(&arr));
        }
        Some(Self { entries })
    }
}

impl Default for StreamDirectory {
    fn default() -> Self {
        Self::new()
    }
}

/// SAC Block：mini-header + Stream Directory + 流数据。
///
/// Block 是 SAC 文件的基本存储单元，包含固定行数的列式数据。
/// 每个 val 列独立编码 + 独立 LZ4（O1=A），reader 只解压目标列。
#[derive(Debug, Clone)]
pub struct Block {
    /// Mini-header: 元数据。
    pub header: BlockMiniHeader,
    /// Stream Directory：记录 ts / 各 val 列 / qual 流的 offset+size。
    pub stream_directory: StreamDirectory,
    /// 各列值流（独立 LZ4），按 column_id 升序。
    pub val_streams: Vec<BlockValStream>,
    /// 质量码流（RLE 编码 → LZ4）。
    pub qual_stream: Vec<u8>,
    /// 时间戳流（DoD 编码 → LZ4）。
    pub ts_stream: Vec<u8>,
    /// 每 Block 尾部 CRC32C（覆盖 mini-header + directory + 全部流，不含自身）。
    pub crc32c: u32,
}

/// 单个列值流。
#[derive(Debug, Clone)]
pub struct BlockValStream {
    /// 列 ID。
    pub column_id: u16,
    /// 编码标签（`VALUE_TAG_*`）。高 7 位为编码算法，bit7（`0x80`）置位表示
    /// `data` 解密/LZ4 解压后 payload 头部携带 presence 位图（本块本列含 null）。
    pub tag: u8,
    /// LZ4 压缩后的编码字节。
    pub data: Vec<u8>,
}

impl Block {
    /// 创建一个新的空 Block。
    pub fn new(header: BlockMiniHeader) -> Self {
        Self {
            header,
            stream_directory: StreamDirectory::new(),
            val_streams: Vec::new(),
            qual_stream: Vec::new(),
            ts_stream: Vec::new(),
            crc32c: 0,
        }
    }

    /// 计算 Block 的总字节大小（含 mini-header + 目录 + 流 + 尾部 CRC）。
    pub fn total_size(&self) -> usize {
        BLOCK_MINI_HEADER_SIZE
            + self.stream_directory.size_bytes()
            + self.ts_stream.len()
            + self
                .val_streams
                .iter()
                .map(|s| 1 + s.data.len()) // tag u8 + data
                .sum::<usize>()
            + self.qual_stream.len()
            + BLOCK_CRC_SIZE
    }

    /// 计算每 Block 尾部 CRC32C，覆盖 mini-header + Stream Directory + 全部流（不含 CRC 自身）。
    pub fn compute_crc(&self) -> u32 {
        let mut hasher = crc32c::Crc32cHasher::new(0);
        hasher.write(&self.header.to_bytes());
        hasher.write(&self.stream_directory.to_bytes());
        hasher.write(&self.ts_stream);
        for s in &self.val_streams {
            hasher.write(&[s.tag]);
            hasher.write(&s.data);
        }
        hasher.write(&self.qual_stream);
        hasher.finish() as u32
    }

    /// 校验 Block 的 CRC32C。
    pub fn verify_crc(&self) -> bool {
        self.compute_crc() == self.crc32c
    }

    /// 序列化 Block 为完整字节（mini-header + directory + 流 + 尾部 CRC）。
    ///
    /// 调用前须确保 `stream_directory` 与 `crc32c` 已就绪。
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.total_size());
        buf.extend_from_slice(&self.header.to_bytes());
        buf.extend_from_slice(&self.stream_directory.to_bytes());
        buf.extend_from_slice(&self.ts_stream);
        for s in &self.val_streams {
            buf.push(s.tag);
            buf.extend_from_slice(&s.data);
        }
        buf.extend_from_slice(&self.qual_stream);
        buf.extend_from_slice(&self.crc32c.to_le_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_mini_header_round_trip() {
        let hdr = BlockMiniHeader::new(4, 7, 1000, 100, 2000, 256, 512, Some(42), 0);
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), BLOCK_MINI_HEADER_SIZE);

        let decoded = BlockMiniHeader::from_bytes(&bytes).expect("decoding failed");
        assert_eq!(decoded, hdr);
        assert_eq!(decoded.column_count, 4);
        assert_eq!(decoded.table_id, 7);
        assert_eq!(decoded.row_count, 1000);
        assert_eq!(decoded.max_wal_seq, Some(42));
    }

    #[test]
    fn test_block_mini_header_no_wal_seq() {
        let hdr = BlockMiniHeader::new(1, 0, 500, 0, 100, 0, 0, None, 0);
        let bytes = hdr.to_bytes();
        let decoded = BlockMiniHeader::from_bytes(&bytes).expect("decoding failed");
        assert_eq!(decoded.max_wal_seq, None);
    }

    #[test]
    fn test_stream_dir_round_trip() {
        let dir = StreamDirectory {
            entries: vec![
                StreamDirEntry {
                    column_id: TS_STREAM_COLUMN_ID,
                    reserved: 0,
                    offset: 40,
                    size: 100,
                },
                StreamDirEntry {
                    column_id: 0,
                    reserved: 0,
                    offset: 140,
                    size: 20,
                },
                StreamDirEntry {
                    column_id: QUAL_STREAM_COLUMN_ID,
                    reserved: 0,
                    offset: 160,
                    size: 30,
                },
            ],
        };
        let bytes = dir.to_bytes();
        assert_eq!(bytes.len(), 3 * STREAM_DIR_ENTRY_SIZE);

        let decoded = StreamDirectory::from_bytes(&bytes).expect("decode failed");
        assert_eq!(decoded, dir);
    }

    #[test]
    fn test_stream_dir_bad_len() {
        assert!(StreamDirectory::from_bytes(&[0u8; 5]).is_none());
    }

    #[test]
    fn test_block_total_size() {
        let hdr = BlockMiniHeader::default();
        let mut block = Block::new(hdr);
        block.stream_directory = StreamDirectory {
            entries: vec![
                StreamDirEntry {
                    column_id: TS_STREAM_COLUMN_ID,
                    reserved: 0,
                    offset: 40,
                    size: 50,
                },
                StreamDirEntry {
                    column_id: 0,
                    reserved: 0,
                    offset: 90,
                    size: 100,
                },
                StreamDirEntry {
                    column_id: QUAL_STREAM_COLUMN_ID,
                    reserved: 0,
                    offset: 190,
                    size: 10,
                },
            ],
        };
        block.ts_stream = vec![0u8; 50];
        block.val_streams = vec![BlockValStream {
            column_id: 0,
            tag: 0,
            data: vec![0u8; 100],
        }];
        block.qual_stream = vec![0u8; 10];

        // 48 (header) + 3*12 (dir) + 50 (ts) + 1+100 (val) + 10 (qual) + 4 (crc)
        assert_eq!(block.total_size(), 48 + 36 + 50 + 101 + 10 + 4);
    }

    #[test]
    fn test_block_mini_header_size_constant() {
        assert_eq!(BLOCK_MINI_HEADER_SIZE, 48);
    }

    /// 必测项 2：48B mini-header 字段偏移布局（v9 在偏移 2 处新增 table_id u32）。
    #[test]
    fn test_block_mini_header_48b_layout() {
        let hdr = BlockMiniHeader::new(4, 0xDEAD_BEEF, 1000, 100, 2000, 256, 512, Some(42), 0x0F);
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), 48);

        assert_eq!(
            u16::from_le_bytes(bytes[0..2].try_into().unwrap()),
            4,
            "column_count @0"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[2..6].try_into().unwrap()),
            0xDEAD_BEEF,
            "table_id @2"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[6..10].try_into().unwrap()),
            1000,
            "row_count @6"
        );
        assert_eq!(
            i64::from_le_bytes(bytes[10..18].try_into().unwrap()),
            100,
            "min_ts @10"
        );
        assert_eq!(
            i64::from_le_bytes(bytes[18..26].try_into().unwrap()),
            2000,
            "max_ts @18"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[26..30].try_into().unwrap()),
            256,
            "val_stream_offset @26"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[30..34].try_into().unwrap()),
            512,
            "qual_stream_offset @30"
        );
        assert_eq!(
            u64::from_le_bytes(bytes[34..42].try_into().unwrap()),
            42,
            "max_wal_seq @34"
        );
        assert_eq!(bytes[42], 0x0F, "flags @42");
        assert_eq!(&bytes[43..48], &[0u8; 5], "reserved @43..48");
    }
}

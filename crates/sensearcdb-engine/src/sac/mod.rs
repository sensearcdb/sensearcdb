//! SAC (SenseArc Columnar) 文件格式模块。
//!
//! SAC 是 SenseArcDB 的列式存储文件格式，每个文件包含：
//! - `SacHeader` (32B): 文件类型、版本、分区键、层级、序列号
//! - 若干 `Block`: 每个 Block 包含 mini-header + 三流（值流/质量码流/时间戳流）
//! - `L2Descriptor` 区: 文件级元数据描述符
//! - `TableIndex` 区: 表级索引（按 table_id 升序，v9 新增，取代 L1）
//! - `SacFooter` (64B): 全局 min/max 时间戳、Block/Table 计数、L2/TableIndex 偏移、CRC32C

pub mod block;
pub mod footer;
pub mod header;
pub mod l2_descriptor;
pub mod naming;
pub mod parallel;
pub mod reader;
pub mod table_index;
pub mod watermark;
pub mod writer;

pub use block::{Block, BlockMiniHeader};
pub use footer::SacFooter;
pub use header::SacHeader;
pub use l2_descriptor::L2Descriptor;
pub use naming::SacNaming;
pub use reader::SacReader;
pub use table_index::TableIndexEntry;
pub use watermark::{SacWatermark, SAC_WATERMARK_FILE};
pub use writer::SacWriter;

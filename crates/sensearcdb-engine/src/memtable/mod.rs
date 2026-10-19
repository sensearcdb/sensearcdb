//! MemTable 模块：内存中的列式存储表。
//!
//! MemTable 是 WAL 和 SAC 之间的桥梁。写入数据先进入 active MemTable，
//! 达到阈值后冻结（force_freeze），然后被 FlushCoordinator 刷入 SAC 文件。
//!
//! ## 架构
//!
//! - `MemTableManager`: 双缓冲状态机管理（`RwLock<{ active, immutable }>`）
//! - `ColumnarMemTable`: 按 table_id 分片 chunk 的内存表（分片锁）
//! - `ColumnarChunk`: 单表 SoA 列式存储（timestamps + columns + qualities）
//! - `ColVec`: 列值枚举（F64/F32/I64/U64/I32/Bool/String/Enum16）
//! - `FlushCoordinator`: 10s 轮询，发现 immutable 后触发 SAC 写入

pub mod chunk;
pub mod col_vec;
pub mod flush;
pub mod manager;
pub mod table;

pub use chunk::ColumnarChunk;
pub use col_vec::{ColVec, TypedValue};
pub use flush::{FlushCoordinator, FlushPipeline};
pub use manager::MemTableManager;
pub use table::ColumnarMemTable;

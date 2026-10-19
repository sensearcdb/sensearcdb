//! # sensearcdb-engine
//!
//! SenseArcDB 存储引擎核心：SAC 文件、WAL、MemTable、Compaction、查询。
//!
//! ## 模块概览
//!
//! | 模块 | 职责 |
//! |------|------|
//! | [`engine`] | 顶层 API：`write_batch` / `query` / `force_freeze` |
//! | [`sac`] | SAC 文件格式：Header/Footer/Block/TableIndex/L2Descriptor/Writer/Reader |
//! | [`encoding`] | 时序编码：DoD / Gorilla / RLE / Simple8b / Dictionary |
//! | [`compression`] | LZ4 / ZSTD9 统一压缩包装 |
//! | [`wal`] | WAL：Group Commit / 64MB Segment / 重放读取 |
//! | [`memtable`] | 内存表：ColumnarChunk / ColumnarMemTable / ArcSwap RCU |
//! | [`compaction`] | Compaction：三重触发 / L0→L1→L2 / k-way merge |
//! | [`query`] | 查询引擎：sqlparser-rs dialect / QueryPlanner / k-way merge executor |
//! | [`metrics`] | Prometheus 指标注册 |

pub mod compaction;
pub mod compression;
pub mod encoding;
pub mod encryption;
pub mod engine;
pub mod memtable;
pub mod metrics;
pub mod query;
pub mod quota;
pub mod sac;
pub mod wal;

pub use sensearcdb_common::TenantId;

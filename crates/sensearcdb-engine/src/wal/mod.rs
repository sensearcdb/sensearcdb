//! WAL (Write-Ahead Log) 模块。
//!
//! WAL 确保数据在 MemTable 写入前已持久化到磁盘，提供崩溃恢复保证。
//!
//! ## 架构
//!
//! - `WalWriter`: Group Commit 策略（1MB / 200μs / 1s 三触发），统一写 WAL2
//! - `WalReader`: 顺序读取 WAL 段文件，解析 WAL2 帧，支持按 seq 过滤重放
//! - `WalSegment`: 64MB 段文件管理 + flush_watermark

pub mod format;
pub mod manifest;
pub mod reader;
pub mod reaper;
pub mod segment;
pub mod writer;

pub use format::{WalFrameFlag, WalFrameHeaderV2};
pub use manifest::{SegmentMeta, WalManifest};
pub use reader::{WalReader, WalRecord};
pub use reaper::{ReapStats, WalReaper, DEFAULT_WAL_RECYCLE_KEEP_SEGMENTS};
pub use segment::WalSegment;
pub use writer::{deserialize_points, serialize_points, DecodedWalFrame, WalRaftFrame, WalWriter};

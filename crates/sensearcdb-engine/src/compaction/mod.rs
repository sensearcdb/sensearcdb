//! Compaction 模块：SAC 文件合并与层级管理。
//!
//! Compaction 将多个小 SAC 文件合并为更大的文件，减少文件数量，
//! 提高查询效率。采用类 LSM 的五层架构（L0 → L1 → L2 → L3 → L4）。
//!
//! ## 层次说明
//!
//! - **L0**: MemTable 直接刷出的文件。无序，可能包含重叠时间范围。
//! - **L1**: L0 合并后的文件。有序，时间范围不重叠。默认 ZSTD9 压缩。
//! - **L2**: L1 合并后的文件。有序，ZSTD9 压缩（高压缩率）。
//!
//! ## 三重触发
//!
//! 1. 文件数量 ≥ `compaction_default_file_count`（默认 16）
//! 2. 数据量 ≥ `compaction_default_size_gb`（默认 1GB）
//! 3. 等待 ≥ `compaction_max_wait_secs`（默认 3600s）—— timeout 兜底

pub mod level_state;
pub mod merge;
pub mod scheduler;
pub mod strategy;

pub use level_state::PerLevelState;
pub use merge::{dedup_tiebreaker, DataPoint};
pub use scheduler::CompactionScheduler;
pub use strategy::CompactionStrategy;

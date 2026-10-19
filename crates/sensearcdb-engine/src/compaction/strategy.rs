//! Compaction 合并策略：L0→L1 重叠 merge，L1→L2 ZSTD9。
//!
//! ## 策略规则
//!
//! ### L0 → L1
//!
//! - 选择所有 L0 文件（时间范围可能重叠）
//! - k-way merge 排序 + 去重
//! - 输出为时间范围不重叠的 L1 文件
//! - 压缩：ZSTD9（L1 起全 ZSTD9，刀2）
//!
//! ### L1 → L2
//!
//! - 选择相邻的 L1 文件（时间范围连续）
//! - k-way merge 排序 + 去重
//! - 输出为更大的 L2 文件
//! - 压缩：ZSTD9（最大化压缩率）
//!
//! ### Column 补齐规则（§7.7）
//!
//! Compaction 时按「旧 Block 列数」和「当前 Schema 有效列数」的最大值
//! 补齐输出 Block 的 `column_count`。缺失的列用 NULL 填充。

use sensearcdb_common::ConfigHandle;

use crate::compaction::scheduler::CompactionTarget;
use crate::compression::CompressionType;

/// Compaction 合并策略。
pub struct CompactionStrategy {
    /// L0 文件数量触发阈值。
    file_count_threshold: usize,
    /// L1 文件数量触发阈值（L1→L2）。
    l1_file_count_threshold: usize,
    /// L2 文件数量触发阈值（L2→L3）。
    l2_file_count_threshold: usize,
    /// 数据量触发阈值 (GB)。
    size_threshold_gb: u64,
}

impl CompactionStrategy {
    /// 创建新的合并策略。
    ///
    /// # 参数
    /// - `config`: 配置句柄。
    pub fn new(config: &ConfigHandle) -> Self {
        let cfg = config.get();
        Self {
            file_count_threshold: cfg.engine.compaction_default_file_count,
            l1_file_count_threshold: cfg.engine.compaction_l1_file_count,
            l2_file_count_threshold: cfg.engine.compaction_l2_file_count,
            size_threshold_gb: cfg.engine.compaction_default_size_gb,
        }
    }

    /// 获取指定目标的压缩类型。
    ///
    /// - L0→L1: ZSTD9（刀2：L1 起全 ZSTD9，与 `SacLevel::L1.compression()` 一致）
    /// - L1→L2: ZSTD9
    /// - L2→L3: ZSTD9（刀2：L1 起全 ZSTD9）
    /// - FinalizePartition: ZSTD9（冷数据收敛到单文件，最大化压缩率）
    pub fn compression_for(&self, target: CompactionTarget) -> CompressionType {
        match target {
            CompactionTarget::L0ToL1 => CompressionType::Zstd9,
            CompactionTarget::L1ToL2 => CompressionType::Zstd9,
            CompactionTarget::L2ToL3 => CompressionType::Zstd9,
            CompactionTarget::FinalizePartition => CompressionType::Zstd9,
        }
    }

    /// 计算 Compaction 需要的最大列数（列补齐规则）。
    ///
    /// 取「旧 Block 列数」和「当前 Schema 有效列数」的最大值。
    ///
    /// # 参数
    /// - `old_column_counts`: 各旧 Block 的列数。
    /// - `current_schema_columns`: 当前 Schema 的有效列数。
    ///
    /// # 返回值
    /// 输出 Block 应使用的 column_count。
    pub fn merge_column_count(
        &self,
        old_column_counts: &[u16],
        current_schema_columns: u16,
    ) -> u16 {
        let old_max = old_column_counts.iter().max().copied().unwrap_or(1);
        old_max.max(current_schema_columns)
    }

    /// 获取文件数量触发阈值（L0→L1）。
    pub fn file_count_threshold(&self) -> usize {
        self.file_count_threshold
    }

    /// 获取 L1 文件数量触发阈值（L1→L2）。
    pub fn l1_file_count_threshold(&self) -> usize {
        self.l1_file_count_threshold
    }

    /// 获取 L2 文件数量触发阈值（L2→L3）。
    pub fn l2_file_count_threshold(&self) -> usize {
        self.l2_file_count_threshold
    }

    /// 获取数据量触发阈值（字节）。
    pub fn size_threshold_bytes(&self) -> u64 {
        self.size_threshold_gb * 1024 * 1024 * 1024
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_for_l0_l1() {
        let config = ConfigHandle::new(Default::default());
        let strategy = CompactionStrategy::new(&config);
        assert_eq!(
            strategy.compression_for(CompactionTarget::L0ToL1),
            CompressionType::Zstd9
        );
    }

    #[test]
    fn test_compression_for_l1_l2() {
        let config = ConfigHandle::new(Default::default());
        let strategy = CompactionStrategy::new(&config);
        assert_eq!(
            strategy.compression_for(CompactionTarget::L1ToL2),
            CompressionType::Zstd9
        );
    }

    #[test]
    fn test_compression_for_finalize_partition() {
        let config = ConfigHandle::new(Default::default());
        let strategy = CompactionStrategy::new(&config);
        assert_eq!(
            strategy.compression_for(CompactionTarget::FinalizePartition),
            CompressionType::Zstd9
        );
    }

    #[test]
    fn test_compression_for_l2_l3() {
        let config = ConfigHandle::new(Default::default());
        let strategy = CompactionStrategy::new(&config);
        assert_eq!(
            strategy.compression_for(CompactionTarget::L2ToL3),
            CompressionType::Zstd9
        );
    }

    #[test]
    fn test_merge_column_count() {
        let config = ConfigHandle::new(Default::default());
        let strategy = CompactionStrategy::new(&config);

        // 旧 Block 列数更大
        assert_eq!(strategy.merge_column_count(&[8, 8, 8], 5), 8);
        // Schema 列数更大
        assert_eq!(strategy.merge_column_count(&[4, 4], 10), 10);
        // 空旧 Block
        assert_eq!(strategy.merge_column_count(&[], 5), 5);
    }

    #[test]
    fn test_thresholds() {
        let config = ConfigHandle::new(Default::default());
        let strategy = CompactionStrategy::new(&config);
        assert_eq!(strategy.file_count_threshold(), 16);
        assert_eq!(strategy.l1_file_count_threshold(), 4);
        assert_eq!(strategy.l2_file_count_threshold(), 4);
        assert_eq!(strategy.size_threshold_bytes(), 1024 * 1024 * 1024);
    }
}

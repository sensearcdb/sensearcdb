//! 每层级状态管理。
//!
//! PerLevelState 维护单个 Compaction 目标层级的文件清单和就绪计时器。
//! 当层级首次满足触发条件（如文件数）时记录 `first_ready_at`，
//! timeout 兜底触发时使用此时间戳计算等待时长。

use std::time::Instant;

/// 单个 Compaction 层级的状态。
#[derive(Debug, Clone)]
pub struct PerLevelState {
    /// 该层级的文件清单。
    pub files: Vec<SacFileMeta>,
    /// 首次满足 Compaction 触发条件的时间。
    /// 用于 timeout 兜底触发（`compaction_max_wait_secs`）。
    pub first_ready_at: Option<Instant>,
}

impl PerLevelState {
    /// 创建新的层级状态。
    pub fn new() -> Self {
        Self {
            files: Vec::new(),
            first_ready_at: None,
        }
    }

    /// 记录首次就绪时间。
    pub fn mark_ready(&mut self) {
        if self.first_ready_at.is_none() {
            self.first_ready_at = Some(Instant::now());
        }
    }

    /// 重置状态（Compaction 完成后调用）。
    pub fn reset(&mut self) {
        self.files.clear();
        self.first_ready_at = None;
    }

    /// 获取文件数量。
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// 添加文件元数据。
    pub fn add_file(&mut self, meta: SacFileMeta) {
        self.files.push(meta);
    }

    /// 检查是否为空。
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

impl Default for PerLevelState {
    fn default() -> Self {
        Self::new()
    }
}

/// SAC 文件元数据（Compaction 调度用）。
#[derive(Debug, Clone)]
pub struct SacFileMeta {
    /// SAC 文件路径。
    pub path: String,
    /// 租户 ID（从路径 tenant 目录段解析；0 = 默认租户）。
    pub tenant_id: u32,
    /// 文件大小（字节）。
    pub size_bytes: u64,
    /// 最小时间戳。
    pub min_ts: i64,
    /// 最大时间戳。
    pub max_ts: i64,
    /// SAC 层级。
    pub level: u8,
    /// Block 数量。
    pub block_count: u32,
    /// 文件写入时间（epoch 纳秒，来自 32B header 的 write_epoch，跨文件 LWW 排序键）。
    ///
    /// 与数据时间 `min_ts`/`max_ts` 解耦；final 用「最后写入新近度」判 re-final 窗口，
    /// 替代 wall-clock（避免回填数据因数据时间陈旧被恒判超窗）。
    pub write_epoch: u64,
}

impl SacFileMeta {
    /// 创建新的 SAC 文件元数据。
    pub fn new(
        path: String,
        tenant_id: u32,
        size_bytes: u64,
        min_ts: i64,
        max_ts: i64,
        level: u8,
        block_count: u32,
        write_epoch: u64,
    ) -> Self {
        Self {
            path,
            tenant_id,
            size_bytes,
            min_ts,
            max_ts,
            level,
            block_count,
            write_epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_per_level_state_mark_ready() {
        let mut state = PerLevelState::new();
        assert!(state.first_ready_at.is_none());

        state.mark_ready();
        assert!(state.first_ready_at.is_some());

        // 二次调用不应改变时间
        let first = state.first_ready_at;
        std::thread::sleep(Duration::from_millis(1));
        state.mark_ready();
        assert_eq!(state.first_ready_at, first);
    }

    #[test]
    fn test_per_level_state_reset() {
        let mut state = PerLevelState::new();
        state.add_file(SacFileMeta::new("test.sac".into(), 0, 1024, 0, 100, 0, 1, 0));
        state.mark_ready();

        state.reset();
        assert!(state.is_empty());
        assert!(state.first_ready_at.is_none());
    }

    #[test]
    fn test_sac_file_meta() {
        let meta = SacFileMeta::new("/sac/0_0001.sac".into(), 0, 4096, 100, 200, 0, 10, 0);
        assert_eq!(meta.size_bytes, 4096);
        assert_eq!(meta.block_count, 10);
        assert_eq!(meta.min_ts, 100);
    }
}

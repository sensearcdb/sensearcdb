//! CQ AutoRouter 粒度选择表。
//!
//! 提供查询窗口大小到最优 CQ 粒度的映射。
//! 查询引擎（sensearcdb-engine）的 `AutoRouter` 使用此模块
//! 确定查询应路由到 Raw 路径还是 CQ 路径，以及选择哪个 CQ 粒度。
//!
//! ## 选择策略
//!
//! - 查询窗口 ≤ 10s → CQ_10s (或 Raw，取决于是否有 CQ 定义)
//! - 查询窗口 ≤ 1min → CQ_1m
//! - 查询窗口 ≤ 1h → CQ_1h
//! - 查询窗口 ≤ 1d → CQ_1d
//! - 查询窗口 > 1d → Raw（不适用 CQ 加速）

use crate::definition::CqGranularity;

// ─── CqWindowInfo ───

/// CQ 窗口信息（供引擎 AutoRouter 使用）。
///
/// 描述一个已注册的 CQ 聚合窗口，引擎的 `AutoRouter`
/// 据此判断查询是否可以路由到 CQ 路径。
#[derive(Debug, Clone)]
pub struct CqWindowInfo {
    /// 源 raw 表 ID。
    pub source_table_id: u32,
    /// CQ 目标表 ID。
    pub target_table_id: u32,
    /// CQ 粒度层级。
    pub granularity: CqGranularity,
    /// 窗口长度（纳秒）。
    pub interval_ns: i64,
    /// 聚合的列 ID 列表。
    pub column_ids: Vec<u16>,
}

// ─── AutoRouter ───

/// CQ AutoRouter 粒度选择器。
///
/// 根据查询的时间范围和表 ID，选择最优的 CQ 粒度（如果有）。
/// 结果供引擎 `AutoRouter` 的 `route()` 方法使用。
pub struct AutoRouter {
    /// 已注册的 CQ 窗口（按 source_table_id 索引）。
    windows: Vec<CqWindowInfo>,
    /// 最大返回点数（超出时退回 Raw）。
    max_points: usize,
}

impl AutoRouter {
    /// 创建新的 AutoRouter。
    pub fn new() -> Self {
        Self {
            windows: Vec::new(),
            max_points: 10_000,
        }
    }

    /// 使用指定配置创建 AutoRouter。
    pub fn with_config(max_points: usize) -> Self {
        Self {
            windows: Vec::new(),
            max_points,
        }
    }

    /// 注册 CQ 窗口信息。
    ///
    /// 通常在 CQ 引擎启动或定义变更时调用。
    pub fn register_windows(&mut self, windows: Vec<CqWindowInfo>) {
        self.windows = windows;
    }

    /// 添加单个 CQ 窗口。
    pub fn add_window(&mut self, window: CqWindowInfo) {
        self.windows.push(window);
    }

    /// 为查询选择最优 CQ 粒度。
    ///
    /// # Arguments
    /// * `table_id` - 查询的源表 ID。
    /// * `query_window_ms` - 查询的时间窗口长度（毫秒），如果不适用则为 0。
    ///
    /// # Returns
    /// `Some((target_table_id, granularity))` 如果找到匹配的 CQ 窗口，
    /// `None` 如果应使用 Raw 路径。
    pub fn select_granularity(
        &self,
        table_id: u32,
        query_window_ms: i64,
    ) -> Option<(u32, CqGranularity)> {
        if self.windows.is_empty() {
            return None;
        }

        // 确定查询所需的粒度
        let required_granularity = if query_window_ms <= 0 {
            // 无时间窗口信息：选择最粗粒度
            None
        } else if query_window_ms <= 10_000 {
            Some(CqGranularity::Cq10s)
        } else if query_window_ms <= 60_000 {
            Some(CqGranularity::Cq1m)
        } else if query_window_ms <= 3_600_000 {
            Some(CqGranularity::Cq1h)
        } else if query_window_ms <= 86_400_000 {
            Some(CqGranularity::Cq1d)
        } else {
            // 查询窗口太大，CQ 无法加速
            return None;
        };

        // 查找匹配的 CQ 窗口
        let candidates: Vec<&CqWindowInfo> = self
            .windows
            .iter()
            .filter(|w| w.source_table_id == table_id)
            .collect();

        if candidates.is_empty() {
            return None;
        }

        match required_granularity {
            Some(req_gran) => {
                // 精确匹配或找最接近的粗粒度
                let exact = candidates.iter().find(|w| w.granularity == req_gran);

                if let Some(win) = exact {
                    return Some((win.target_table_id, win.granularity));
                }

                // 找最接近且不细于需求的粒度
                let req_ordinal = req_gran.ordinal();
                let best = candidates
                    .iter()
                    .filter(|w| w.granularity.ordinal() >= req_ordinal)
                    .min_by_key(|w| w.granularity.ordinal());

                if let Some(win) = best {
                    return Some((win.target_table_id, win.granularity));
                }

                // 回退：使用最粗可用粒度
                candidates
                    .iter()
                    .max_by_key(|w| w.granularity.ordinal())
                    .map(|w| (w.target_table_id, w.granularity))
            }
            None => {
                // 无窗口信息：选择最粗可用粒度
                candidates
                    .iter()
                    .max_by_key(|w| w.granularity.ordinal())
                    .map(|w| (w.target_table_id, w.granularity))
            }
        }
    }

    /// 检查是否有针对指定表的 CQ 定义。
    pub fn has_cq_for_table(&self, table_id: u32) -> bool {
        self.windows.iter().any(|w| w.source_table_id == table_id)
    }

    /// 获取已注册的 CQ 窗口数量。
    pub fn window_count(&self) -> usize {
        self.windows.len()
    }

    /// 更新最大返回点数。
    pub fn set_max_points(&mut self, max_points: usize) {
        self.max_points = max_points;
    }
}

impl Default for AutoRouter {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    fn make_window(source_id: u32, target_id: u32, gran: CqGranularity) -> CqWindowInfo {
        CqWindowInfo {
            source_table_id: source_id,
            target_table_id: target_id,
            granularity: gran,
            interval_ns: gran.interval_ns(),
            column_ids: vec![1, 2],
        }
    }

    #[test]
    fn test_select_exact_match() {
        let mut router = AutoRouter::new();
        router.register_windows(vec![
            make_window(1, 101, CqGranularity::Cq10s),
            make_window(1, 102, CqGranularity::Cq1m),
            make_window(1, 103, CqGranularity::Cq1h),
            make_window(1, 104, CqGranularity::Cq1d),
        ]);

        // Query window = 5s → should select CQ_10s
        let result = router.select_granularity(1, 5_000);
        assert!(result.is_some());
        let (target_id, gran) = result.unwrap();
        assert_eq!(target_id, 101);
        assert_eq!(gran, CqGranularity::Cq10s);

        // Query window = 30s → should select CQ_1m
        let result = router.select_granularity(1, 30_000);
        assert!(result.is_some());
        let (target_id, gran) = result.unwrap();
        assert_eq!(target_id, 102);
        assert_eq!(gran, CqGranularity::Cq1m);
    }

    #[test]
    fn test_select_no_cq_for_table() {
        let mut router = AutoRouter::new();
        router.register_windows(vec![make_window(1, 101, CqGranularity::Cq10s)]);

        // Query a different table
        let result = router.select_granularity(999, 5_000);
        assert!(result.is_none());
    }

    #[test]
    fn test_select_too_large_window() {
        let mut router = AutoRouter::new();
        router.register_windows(vec![make_window(1, 101, CqGranularity::Cq10s)]);

        // Query window = 2d → no CQ match
        let result = router.select_granularity(1, 172_800_000); // 2d
        assert!(result.is_none());
    }

    #[test]
    fn test_select_fallback_coarser() {
        let mut router = AutoRouter::new();
        // Only CQ_1h and CQ_1d available (no CQ_10s or CQ_1m)
        router.register_windows(vec![
            make_window(1, 103, CqGranularity::Cq1h),
            make_window(1, 104, CqGranularity::Cq1d),
        ]);

        // Query window = 5s → should fall back to CQ_1h (coarsest ≤ available)
        let result = router.select_granularity(1, 5_000);
        assert!(result.is_some());
        let (target_id, _gran) = result.unwrap();
        assert_eq!(target_id, 103);
    }

    #[test]
    fn test_empty_router() {
        let router = AutoRouter::new();
        assert_eq!(router.window_count(), 0);
        assert!(!router.has_cq_for_table(1));
        assert!(router.select_granularity(1, 5_000).is_none());
    }
}

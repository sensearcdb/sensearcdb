//! AutoRouter：查询路由到最优执行路径。
//!
//! AutoRouter 根据查询特征（目标表、时间窗口大小）将查询自动路由到：
//! - **Raw** 模式：直接从 SAC / MemTable 读取原始数据
//! - **Cq** 模式：利用连续查询（CQ）的预计算缓存加速聚合查询
//! - **Hybrid** 模式：根据查询窗口大小自动选择 Raw 或 Cq
//!
//! ## 集成方式
//!
//! 通过 `CqRouteProvider` trait（定义在 `sensearcdb-common`）连接 CQ 引擎。
//! 调用方使用 `set_cq_provider()` 注入 CQ 引擎引用；`route()` 方法会优先通过
//! provider 尝试 CQ 路由，若不可用则回退到已注册的窗口信息或 Raw 模式。
//!
//! ## 路由逻辑
//!
//! - 有 CQ provider → 调用 `route_query()`，有匹配则 Cq，无则退回
//! - 无 CQ provider → 基于已注册窗口的路由逻辑
//! - 无 CQ 定义 → 始终 Raw
//! - 有 CQ 定义 + 查询窗口匹配 → Cq
//! - Hybrid 模式 → 自动选择（有匹配 CQ 则 Cq，否则 Raw）

use std::collections::HashMap;
use std::sync::Arc;

use sensearcdb_common::CqRouteProvider;

// ─── 路由模式 ───

/// 路由模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteMode {
    /// 原始数据读取（直接从 SAC / MemTable 查询）。
    Raw,
    /// 连续查询缓存（从 CQ 预聚合表读取）。
    Cq,
    /// 混合模式（根据查询窗口大小自动选择 Raw 或 Cq）。
    Hybrid,
}

impl Default for RouteMode {
    fn default() -> Self {
        RouteMode::Raw
    }
}

impl RouteMode {
    /// 返回模式的字符串表示。
    pub fn as_str(&self) -> &'static str {
        match self {
            RouteMode::Raw => "raw",
            RouteMode::Cq => "cq",
            RouteMode::Hybrid => "hybrid",
        }
    }
}

// ─── CQ 粒度 ───

/// CQ 窗口粒度（AutoRouter 本地定义，不依赖 CQ crate）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CqGranularity {
    /// 10 秒窗口。
    Cq10s,
    /// 1 分钟窗口。
    Cq1m,
    /// 1 小时窗口。
    Cq1h,
    /// 1 天窗口。
    Cq1d,
}

impl CqGranularity {
    /// 返回窗口长度（毫秒）。
    #[inline]
    pub fn interval_ms(self) -> i64 {
        match self {
            CqGranularity::Cq10s => 10_000,
            CqGranularity::Cq1m => 60_000,
            CqGranularity::Cq1h => 3_600_000,
            CqGranularity::Cq1d => 86_400_000,
        }
    }

    /// 返回层级序数（0 = 最细）。
    #[inline]
    pub fn ordinal(self) -> u8 {
        match self {
            CqGranularity::Cq10s => 0,
            CqGranularity::Cq1m => 1,
            CqGranularity::Cq1h => 2,
            CqGranularity::Cq1d => 3,
        }
    }
}

// ─── CQ 窗口信息 ───

/// 已注册的 CQ 窗口信息。
///
/// CQ 引擎通过 `register_cq_window()` 注册，
/// AutoRouter 据此判断查询是否可以走 CQ 路径。
#[derive(Debug, Clone)]
pub struct CqWindowInfo {
    /// 源 raw 表 ID。
    pub source_table_id: u32,
    /// CQ 目标表 ID（聚合结果所在的表）。
    pub target_table_id: u32,
    /// 窗口粒度。
    pub granularity: CqGranularity,
    /// 窗口长度（毫秒）。
    pub interval_ms: i64,
}

// ─── 路由结果 ───

/// 路由决策结果。
#[derive(Debug, Clone)]
pub struct RouteResult {
    /// 选定的执行模式。
    pub mode: RouteMode,
    /// CQ 目标表 ID（仅 Cq 模式时有效）。
    pub target_table_id: Option<u32>,
    /// CQ 粒度（仅 Cq 模式时有效）。
    pub granularity: Option<CqGranularity>,
}

impl RouteResult {
    /// 创建 Raw 路由结果。
    #[inline]
    pub fn raw() -> Self {
        Self {
            mode: RouteMode::Raw,
            target_table_id: None,
            granularity: None,
        }
    }

    /// 创建 Cq 路由结果。
    #[inline]
    pub fn cq(target_table_id: u32, granularity: CqGranularity) -> Self {
        Self {
            mode: RouteMode::Cq,
            target_table_id: Some(target_table_id),
            granularity: Some(granularity),
        }
    }
}

// ─── AutoRouter ───

/// 自动路由器。
///
/// 持有已注册的 CQ 窗口信息，根据查询参数选择最优执行路径。
///
/// # 使用示例
///
/// ```
/// use sensearcdb_engine::query::autorouter::{AutoRouter, RouteMode, CqWindowInfo, CqGranularity};
///
/// let mut router = AutoRouter::with_mode(RouteMode::Cq);
/// router.register_cq_window(CqWindowInfo {
///     source_table_id: 1,
///     target_table_id: 101,
///     granularity: CqGranularity::Cq10s,
///     interval_ms: 10_000,
/// });
///
/// // 查询 5s 窗口 → CQ 加速
/// let result = router.route(1, 5_000);
/// assert_eq!(result.mode, RouteMode::Cq);
/// assert_eq!(result.target_table_id, Some(101));
/// ```
pub struct AutoRouter {
    /// CQ route provider（通过 trait 抽象，避免循环依赖）。
    cq_provider: Option<Arc<dyn CqRouteProvider>>,
    /// 已注册的 CQ 窗口（source_table_id → Vec<CqWindowInfo>）。
    cq_windows: HashMap<u32, Vec<CqWindowInfo>>,
    /// 默认路由模式。
    default_mode: RouteMode,
}

impl AutoRouter {
    /// 创建新的 AutoRouter（默认 Raw 模式）。
    pub fn new() -> Self {
        Self {
            cq_provider: None,
            cq_windows: HashMap::new(),
            default_mode: RouteMode::Raw,
        }
    }

    /// 创建使用指定默认模式的 AutoRouter。
    pub fn with_mode(default_mode: RouteMode) -> Self {
        Self {
            cq_provider: None,
            cq_windows: HashMap::new(),
            default_mode,
        }
    }

    /// 注入 CQ route provider（CQ 引擎通过此方法注册）。
    ///
    /// `route()` 会优先通过 provider 尝试 CQ 路由，
    /// 若 provider 返回 `None` 则回退到已注册的窗口信息。
    pub fn set_cq_provider(&mut self, provider: Arc<dyn CqRouteProvider>) {
        self.cq_provider = Some(provider);
    }

    /// 移除 CQ route provider。
    pub fn clear_cq_provider(&mut self) {
        self.cq_provider = None;
    }

    /// 注册单个 CQ 窗口。
    ///
    /// CQ 引擎在创建 CQ 定义后调用此方法注册窗口信息。
    pub fn register_cq_window(&mut self, window: CqWindowInfo) {
        self.cq_windows
            .entry(window.source_table_id)
            .or_default()
            .push(window);
    }

    /// 批量注册 CQ 窗口。
    pub fn register_cq_windows(&mut self, windows: Vec<CqWindowInfo>) {
        for window in windows {
            self.register_cq_window(window);
        }
    }

    /// 移除指定表的所有 CQ 窗口。
    pub fn unregister_table(&mut self, table_id: u32) {
        self.cq_windows.remove(&table_id);
    }

    /// 清空所有已注册的 CQ 窗口。
    pub fn clear(&mut self) {
        self.cq_windows.clear();
    }

    /// 路由查询到最优执行模式。
    ///
    /// # Arguments
    /// * `table_id` - 查询的源表 ID。
    /// * `query_window_ms` - 查询的时间窗口长度（毫秒）。
    ///   0 表示无时间窗口约束（如全表扫描）。
    ///
    /// # Returns
    /// `RouteResult` 包含选定的模式和目标表信息。
    ///
    /// # 路由逻辑
    ///
    /// 1. 有 CQ provider → 调用 `route_query()`，有匹配则 Cq
    /// 2. 无 provider / provider 无匹配 → 基于已注册窗口的路由
    /// 3. Raw 模式 → 始终 Raw
    /// 4. Cq / Hybrid 模式 → 查找匹配窗口 → 有则 Cq，无则 Raw
    pub fn route(&self, table_id: u32, query_window_ms: i64) -> RouteResult {
        // 优先通过 CQ provider 尝试路由
        if let Some(ref provider) = self.cq_provider {
            if let Some(hint) = provider.route_query(table_id, query_window_ms) {
                // CQ provider 返回了目标表；查找对应粒度以填充 RouteResult
                let gran = self
                    .cq_windows
                    .get(&table_id)
                    .and_then(|ws| {
                        ws.iter()
                            .find(|w| w.target_table_id == hint.target_table_id)
                    })
                    .map(|w| w.granularity);
                return RouteResult {
                    mode: RouteMode::Cq,
                    target_table_id: Some(hint.target_table_id),
                    granularity: gran,
                };
            }
            // Provider 无匹配 → 回退到 Raw
            return RouteResult::raw();
        }

        // 回退：使用已注册窗口的路由逻辑
        match self.default_mode {
            RouteMode::Raw => RouteResult::raw(),
            RouteMode::Cq | RouteMode::Hybrid => self.try_route_cq(table_id, query_window_ms),
        }
    }

    /// 兼容旧接口：接受 SQL 字符串（忽略内容，始终使用默认模式）。
    ///
    /// 保留此方法以兼容已有调用方。新代码应使用 `route(table_id, query_window_ms)`。
    #[deprecated(since = "0.3.0", note = "Use route(table_id, query_window_ms) instead")]
    pub fn route_by_sql(&self, _sql: &str) -> RouteMode {
        if self.cq_windows.is_empty() {
            RouteMode::Raw
        } else {
            self.default_mode
        }
    }

    /// 尝试将查询路由到 CQ 路径。
    ///
    /// 根据查询窗口大小选择最优 CQ 粒度：
    /// - query_window ≤ 10s → 尝试 CQ_10s
    /// - query_window ≤ 1min → 尝试 CQ_1m
    /// - query_window ≤ 1h → 尝试 CQ_1h
    /// - query_window ≤ 1d → 尝试 CQ_1d
    /// - query_window > 1d → 退回 Raw
    fn try_route_cq(&self, table_id: u32, query_window_ms: i64) -> RouteResult {
        let candidates = match self.cq_windows.get(&table_id) {
            Some(windows) => windows,
            None => return RouteResult::raw(),
        };

        if candidates.is_empty() {
            return RouteResult::raw();
        }

        // 根据查询窗口大小确定所需粒度
        let required_gran = if query_window_ms <= 0 {
            // 无时间窗口：选择最粗粒度
            candidates
                .iter()
                .max_by_key(|w| w.granularity.ordinal())
                .map(|w| (w.target_table_id, w.granularity))
        } else if query_window_ms <= 10_000 {
            Self::find_best_match(candidates, CqGranularity::Cq10s)
        } else if query_window_ms <= 60_000 {
            Self::find_best_match(candidates, CqGranularity::Cq1m)
        } else if query_window_ms <= 3_600_000 {
            Self::find_best_match(candidates, CqGranularity::Cq1h)
        } else if query_window_ms <= 86_400_000 {
            Self::find_best_match(candidates, CqGranularity::Cq1d)
        } else {
            // 查询窗口超过 1 天，CQ 无法有效加速
            None
        };

        match required_gran {
            Some((target_id, gran)) => RouteResult::cq(target_id, gran),
            None => RouteResult::raw(),
        }
    }

    /// 在候选窗口中查找最佳匹配。
    ///
    /// 优先精确匹配；若不存在则使用不细于需求的最近粗粒度。
    fn find_best_match(
        candidates: &[CqWindowInfo],
        required: CqGranularity,
    ) -> Option<(u32, CqGranularity)> {
        let req_ordinal = required.ordinal();

        // 精确匹配
        if let Some(win) = candidates.iter().find(|w| w.granularity == required) {
            return Some((win.target_table_id, win.granularity));
        }

        // 找最接近且不细于需求的粗粒度
        candidates
            .iter()
            .filter(|w| w.granularity.ordinal() >= req_ordinal)
            .min_by_key(|w| w.granularity.ordinal())
            .map(|w| (w.target_table_id, w.granularity))
    }

    /// 检查是否有指定表的 CQ 窗口。
    pub fn has_cq_for_table(&self, table_id: u32) -> bool {
        self.cq_windows
            .get(&table_id)
            .map(|w| !w.is_empty())
            .unwrap_or(false)
    }

    /// 获取已注册的 CQ 窗口总数。
    pub fn cq_window_count(&self) -> usize {
        self.cq_windows.values().map(|v| v.len()).sum()
    }

    /// 获取指定表的 CQ 窗口信息。
    pub fn get_cq_windows(&self, table_id: u32) -> Vec<CqWindowInfo> {
        self.cq_windows.get(&table_id).cloned().unwrap_or_default()
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
            interval_ms: gran.interval_ms(),
        }
    }

    // ─── RouteMode tests ───

    #[test]
    fn test_default_route_mode() {
        assert_eq!(RouteMode::default(), RouteMode::Raw);
    }

    #[test]
    fn test_route_mode_as_str() {
        assert_eq!(RouteMode::Raw.as_str(), "raw");
        assert_eq!(RouteMode::Cq.as_str(), "cq");
        assert_eq!(RouteMode::Hybrid.as_str(), "hybrid");
    }

    // ─── CqGranularity tests ───

    #[test]
    fn test_granularity_intervals() {
        assert_eq!(CqGranularity::Cq10s.interval_ms(), 10_000);
        assert_eq!(CqGranularity::Cq1m.interval_ms(), 60_000);
        assert_eq!(CqGranularity::Cq1h.interval_ms(), 3_600_000);
        assert_eq!(CqGranularity::Cq1d.interval_ms(), 86_400_000);
    }

    #[test]
    fn test_granularity_ordinal() {
        assert_eq!(CqGranularity::Cq10s.ordinal(), 0);
        assert_eq!(CqGranularity::Cq1m.ordinal(), 1);
        assert_eq!(CqGranularity::Cq1h.ordinal(), 2);
        assert_eq!(CqGranularity::Cq1d.ordinal(), 3);
    }

    // ─── AutoRouter: empty ───

    #[test]
    fn test_empty_router_returns_raw() {
        let router = AutoRouter::new();
        let result = router.route(1, 5_000);
        assert_eq!(result.mode, RouteMode::Raw);
        assert_eq!(result.target_table_id, None);
    }

    #[test]
    fn test_empty_router_no_cq_for_table() {
        let router = AutoRouter::new();
        assert!(!router.has_cq_for_table(1));
        assert_eq!(router.cq_window_count(), 0);
    }

    // ─── AutoRouter: Raw mode ───

    #[test]
    fn test_raw_mode_always_raw() {
        let mut router = AutoRouter::with_mode(RouteMode::Raw);
        router.register_cq_window(make_window(1, 101, CqGranularity::Cq10s));

        let result = router.route(1, 5_000);
        assert_eq!(result.mode, RouteMode::Raw);
    }

    // ─── AutoRouter: Cq mode ───

    #[test]
    fn test_cq_mode_exact_match() {
        let mut router = AutoRouter::with_mode(RouteMode::Cq);
        router.register_cq_window(make_window(1, 101, CqGranularity::Cq10s));
        router.register_cq_window(make_window(1, 102, CqGranularity::Cq1m));
        router.register_cq_window(make_window(1, 103, CqGranularity::Cq1h));
        router.register_cq_window(make_window(1, 104, CqGranularity::Cq1d));

        // 5s query → CQ_10s
        let result = router.route(1, 5_000);
        assert_eq!(result.mode, RouteMode::Cq);
        assert_eq!(result.target_table_id, Some(101));
        assert_eq!(result.granularity, Some(CqGranularity::Cq10s));

        // 30s query → CQ_1m
        let result = router.route(1, 30_000);
        assert_eq!(result.mode, RouteMode::Cq);
        assert_eq!(result.target_table_id, Some(102));

        // 30min query → CQ_1h
        let result = router.route(1, 1_800_000);
        assert_eq!(result.mode, RouteMode::Cq);
        assert_eq!(result.target_table_id, Some(103));

        // 12h query → CQ_1d
        let result = router.route(1, 43_200_000);
        assert_eq!(result.mode, RouteMode::Cq);
        assert_eq!(result.target_table_id, Some(104));
    }

    #[test]
    fn test_cq_mode_fallback_coarser() {
        let mut router = AutoRouter::with_mode(RouteMode::Cq);
        // Only CQ_1m available (no CQ_10s)
        router.register_cq_window(make_window(1, 102, CqGranularity::Cq1m));

        // 5s query → falls back to CQ_1m
        let result = router.route(1, 5_000);
        assert_eq!(result.mode, RouteMode::Cq);
        assert_eq!(result.target_table_id, Some(102));
    }

    #[test]
    fn test_cq_mode_too_large_window() {
        let mut router = AutoRouter::with_mode(RouteMode::Cq);
        router.register_cq_window(make_window(1, 101, CqGranularity::Cq10s));

        // 2-day query → too large, falls back to Raw
        let result = router.route(1, 172_800_000);
        assert_eq!(result.mode, RouteMode::Raw);
    }

    #[test]
    fn test_cq_mode_wrong_table() {
        let mut router = AutoRouter::with_mode(RouteMode::Cq);
        router.register_cq_window(make_window(1, 101, CqGranularity::Cq10s));

        // Query table 999 → Raw
        let result = router.route(999, 5_000);
        assert_eq!(result.mode, RouteMode::Raw);
    }

    // ─── AutoRouter: Hybrid mode ───

    #[test]
    fn test_hybrid_mode_behaves_like_cq_when_available() {
        let mut router = AutoRouter::with_mode(RouteMode::Hybrid);
        router.register_cq_window(make_window(1, 101, CqGranularity::Cq10s));

        let result = router.route(1, 5_000);
        assert_eq!(result.mode, RouteMode::Cq);
    }

    #[test]
    fn test_hybrid_mode_falls_back_to_raw() {
        let router = AutoRouter::with_mode(RouteMode::Hybrid);
        // No CQ windows registered

        let result = router.route(1, 5_000);
        assert_eq!(result.mode, RouteMode::Raw);
    }

    // ─── AutoRouter: registration ───

    #[test]
    fn test_register_and_clear() {
        let mut router = AutoRouter::new();
        assert_eq!(router.cq_window_count(), 0);

        router.register_cq_window(make_window(1, 101, CqGranularity::Cq10s));
        router.register_cq_window(make_window(2, 201, CqGranularity::Cq1m));
        assert_eq!(router.cq_window_count(), 2);
        assert!(router.has_cq_for_table(1));
        assert!(router.has_cq_for_table(2));

        router.unregister_table(1);
        assert_eq!(router.cq_window_count(), 1);
        assert!(!router.has_cq_for_table(1));

        router.clear();
        assert_eq!(router.cq_window_count(), 0);
    }

    #[test]
    fn test_get_cq_windows() {
        let mut router = AutoRouter::new();
        router.register_cq_window(make_window(1, 101, CqGranularity::Cq10s));
        router.register_cq_window(make_window(1, 102, CqGranularity::Cq1m));

        let windows = router.get_cq_windows(1);
        assert_eq!(windows.len(), 2);
    }

    #[test]
    fn test_route_result_raw() {
        let result = RouteResult::raw();
        assert_eq!(result.mode, RouteMode::Raw);
        assert!(result.target_table_id.is_none());
        assert!(result.granularity.is_none());
    }

    #[test]
    fn test_route_result_cq() {
        let result = RouteResult::cq(101, CqGranularity::Cq10s);
        assert_eq!(result.mode, RouteMode::Cq);
        assert_eq!(result.target_table_id, Some(101));
        assert_eq!(result.granularity, Some(CqGranularity::Cq10s));
    }
}

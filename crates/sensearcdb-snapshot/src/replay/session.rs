//! ReplaySessionHandle — 回放会话状态机。
//!
//! ## 状态机
//!
//! ```text
//! Idle → Playing → Paused → Playing (恢复)
//!                  → Completed
//!                  → Error
//!
//! 任何状态 → Stopped (终止)
//! ```
//!
//! ## 超限立即清理（§7.8.2）
//!
//! 当 ReplaySnapshotStore 插入返回 `ResourceExhausted` 时：
//! - state → Error("entry_limit_exceeded")
//! - 停止 SAC 读取循环（cancel_token 设 false）
//! - ReplayEngine 检测到 Error 后立即从 sessions map 移除
//! - ⚠️ 不等待 session_idle_cleanup_secs 定时器

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::replay::clock::ReplaySpeed;
use crate::replay::event::ReplayEvent;
use crate::replay::store::ReplaySnapshotStore;

/// 回放状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayState {
    /// 初始空闲。
    Idle,
    /// 正在播放。
    Playing,
    /// 已暂停。
    Paused,
    /// 已完成。
    Completed,
    /// 已停止。
    Stopped,
    /// 错误。
    Error(String),
}

impl ReplayState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Error(_))
    }
}

impl std::fmt::Display for ReplayState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::Playing => write!(f, "Playing"),
            Self::Paused => write!(f, "Paused"),
            Self::Completed => write!(f, "Completed"),
            Self::Stopped => write!(f, "Stopped"),
            Self::Error(e) => write!(f, "Error({})", e),
        }
    }
}

/// 回放参数（启动时传入）。
#[derive(Debug, Clone)]
pub struct ReplayParams {
    /// 要回放的 tag ID。
    pub tag_ids: Vec<u32>,
    /// 起始时间戳 (epoch ns)。
    pub start_ts: i64,
    /// 结束时间戳 (epoch ns)。
    pub end_ts: i64,
    /// 回放速度。
    pub speed: ReplaySpeed,
    /// 是否包含派生标签计算。
    pub include_derived: bool,
    /// 是否包含告警评估。
    pub include_alerts: bool,
}

/// 回放统计。
#[derive(Debug, Clone, Default)]
pub struct ReplayStats {
    /// 已处理的数据点数。
    pub processed_count: u64,
    /// 派生标签计算次数。
    pub derived_count: u64,
    /// 告警事件数。
    pub alert_count: u64,
    /// 错误计数。
    pub error_count: u64,
}

/// 取消令牌（替代 tokio_util::CancellationToken）。
#[derive(Debug, Clone)]
pub struct CancelToken {
    cancelled: Arc<AtomicBool>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

/// 回放会话句柄。
///
/// 每个回放会话的生命周期由此结构管理。
pub struct ReplaySessionHandle {
    /// 会话 ID。
    pub id: String,
    /// 回放参数。
    pub params: ReplayParams,
    /// 当前状态。
    pub state: RwLock<ReplayState>,
    /// Session 专属 ReplaySnapshotStore。
    pub store: Arc<ReplaySnapshotStore>,
    /// 回放统计。
    pub stats: RwLock<ReplayStats>,
    /// 最近事件环形缓冲区。
    pub recent_events: RwLock<VecDeque<ReplayEvent>>,
    /// 事件缓冲区容量。
    event_buffer_size: usize,
    /// 创建时间。
    pub created_at: Instant,
    /// 取消令牌。
    pub cancel_token: CancelToken,
    /// Seek 目标时间戳 (epoch ns)。
    seek_target_ms: AtomicI64,
}

impl ReplaySessionHandle {
    /// 创建新的回放会话。
    pub fn new(
        id: String,
        params: ReplayParams,
        store: Arc<ReplaySnapshotStore>,
        event_buffer_size: usize,
    ) -> Self {
        Self {
            id,
            params,
            store,
            state: RwLock::new(ReplayState::Idle),
            stats: RwLock::new(ReplayStats::default()),
            recent_events: RwLock::new(VecDeque::with_capacity(event_buffer_size)),
            event_buffer_size,
            created_at: Instant::now(),
            cancel_token: CancelToken::new(),
            seek_target_ms: AtomicI64::new(0),
        }
    }

    // ─── 状态转换 ───

    /// 开始播放。
    pub fn play(&self) -> Result<ReplayState, String> {
        let mut state = self.state.write();
        match &*state {
            ReplayState::Idle | ReplayState::Paused => {
                let old = state.clone();
                *state = ReplayState::Playing;
                Ok(old)
            }
            s => Err(format!("cannot play from state {}", s)),
        }
    }

    /// 暂停。
    pub fn pause(&self) -> Result<(), String> {
        let mut state = self.state.write();
        match &*state {
            ReplayState::Playing => {
                *state = ReplayState::Paused;
                Ok(())
            }
            s => Err(format!("cannot pause from state {}", s)),
        }
    }

    /// 恢复（从 Paused 回到 Playing）。
    pub fn resume(&self) -> Result<(), String> {
        let mut state = self.state.write();
        match &*state {
            ReplayState::Paused => {
                *state = ReplayState::Playing;
                Ok(())
            }
            s => Err(format!("cannot resume from state {}", s)),
        }
    }

    /// 停止回放。
    pub fn stop(&self) {
        let old_state = {
            let mut state = self.state.write();
            let old = state.clone();
            *state = ReplayState::Stopped;
            old
        };
        self.cancel_token.cancel();
        tracing::info!(
            session_id = %self.id,
            from = %old_state,
            "replay session stopped"
        );
    }

    /// 标记为完成。
    pub fn complete(&self) {
        let mut state = self.state.write();
        *state = ReplayState::Completed;
        self.cancel_token.cancel();
    }

    /// 标记为错误（触发立即清理）。
    pub fn set_error(&self, error_msg: impl Into<String>) {
        let msg = error_msg.into();
        {
            let mut state = self.state.write();
            *state = ReplayState::Error(msg.clone());
        }
        self.cancel_token.cancel();
        tracing::error!(
            session_id = %self.id,
            error = %msg,
            "replay session error — immediate cleanup"
        );
    }

    /// 获取当前状态。
    pub fn current_state(&self) -> ReplayState {
        self.state.read().clone()
    }

    /// 是否已取消。
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    // ─── 事件缓冲区 ───

    /// 向环形缓冲区追加事件。
    pub fn push_event(&self, event: ReplayEvent) {
        let mut events = self.recent_events.write();
        if events.len() >= self.event_buffer_size {
            events.pop_front();
        }
        events.push_back(event);
    }

    /// 获取最近的事件列表。
    pub fn get_recent_events(&self, limit: usize) -> Vec<ReplayEvent> {
        let events = self.recent_events.read();
        let start = if events.len() > limit {
            events.len() - limit
        } else {
            0
        };
        events.iter().skip(start).cloned().collect()
    }

    // ─── Seek ───

    /// 设置 seek 目标时间戳。
    pub fn set_seek_target(&self, ts_ms: i64) {
        self.seek_target_ms.store(ts_ms, Ordering::Relaxed);
    }

    /// 获取 seek 目标时间戳。
    pub fn get_seek_target(&self) -> i64 {
        self.seek_target_ms.load(Ordering::Relaxed)
    }

    // ─── 统计 ───

    /// 增量更新统计。
    pub fn inc_processed(&self) {
        self.stats.write().processed_count += 1;
    }

    /// 增量派生标签计数。
    pub fn inc_derived(&self) {
        self.stats.write().derived_count += 1;
    }

    /// 增量告警计数。
    pub fn inc_alerts(&self) {
        self.stats.write().alert_count += 1;
    }

    /// 增量错误计数。
    pub fn inc_errors(&self) {
        self.stats.write().error_count += 1;
    }

    /// 获取回放进度 (0.0 ~ 1.0)。
    pub fn progress(&self) -> f64 {
        let total_range = (self.params.end_ts - self.params.start_ts) as f64;
        if total_range <= 0.0 {
            return 1.0;
        }
        let processed = self.stats.read().processed_count;
        processed.min(1000) as f64 / 1000f64.max(1.0)
    }
}

impl std::fmt::Debug for ReplaySessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaySessionHandle")
            .field("id", &self.id)
            .field("state", &self.current_state())
            .field("store.total_entries", &self.store.total_entries())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_session() -> ReplaySessionHandle {
        let params = ReplayParams {
            tag_ids: vec![1, 2],
            start_ts: 0,
            end_ts: 1_000_000_000,
            speed: ReplaySpeed(1.0),
            include_derived: false,
            include_alerts: false,
        };
        let store = Arc::new(ReplaySnapshotStore::new("test-session".into(), 4, 1000));
        ReplaySessionHandle::new("test-session".into(), params, store, 100)
    }

    #[test]
    fn test_state_machine_idle_to_playing() {
        let session = make_session();
        assert_eq!(session.current_state(), ReplayState::Idle);

        let old = session.play().unwrap();
        assert_eq!(old, ReplayState::Idle);
        assert_eq!(session.current_state(), ReplayState::Playing);
    }

    #[test]
    fn test_state_machine_playing_pause_resume() {
        let session = make_session();
        session.play().unwrap();
        session.pause().unwrap();
        assert_eq!(session.current_state(), ReplayState::Paused);
        session.resume().unwrap();
        assert_eq!(session.current_state(), ReplayState::Playing);
    }

    #[test]
    fn test_state_machine_cannot_pause_from_idle() {
        let session = make_session();
        assert!(session.pause().is_err());
    }

    #[test]
    fn test_state_machine_stop() {
        let session = make_session();
        session.play().unwrap();
        session.stop();
        assert_eq!(session.current_state(), ReplayState::Stopped);
    }

    #[test]
    fn test_state_machine_complete() {
        let session = make_session();
        session.play().unwrap();
        session.complete();
        assert_eq!(session.current_state(), ReplayState::Completed);
    }

    #[test]
    fn test_set_error() {
        let session = make_session();
        session.set_error("entry_limit_exceeded");
        match session.current_state() {
            ReplayState::Error(msg) => assert!(msg.contains("entry_limit_exceeded")),
            _ => panic!("expected Error state"),
        }
    }

    #[test]
    fn test_push_event_ring_buffer() {
        let session = make_session();
        for i in 0..150 {
            session.push_event(ReplayEvent::next_value(i as i64, i, 0.0));
        }
        let events = session.get_recent_events(200);
        assert_eq!(events.len(), 100);
    }

    #[test]
    fn test_stats() {
        let session = make_session();
        session.inc_processed();
        session.inc_processed();
        session.inc_derived();
        let stats = session.stats.read();
        assert_eq!(stats.processed_count, 2);
        assert_eq!(stats.derived_count, 1);
    }

    #[test]
    fn test_terminal_states() {
        assert!(!ReplayState::Idle.is_terminal());
        assert!(!ReplayState::Playing.is_terminal());
        assert!(!ReplayState::Paused.is_terminal());
        assert!(ReplayState::Completed.is_terminal());
        assert!(ReplayState::Stopped.is_terminal());
        assert!(ReplayState::Error("test".into()).is_terminal());
    }
}

//! ReplayEngine — 独立回放管线。
//!
//! ## 关键隔离保证
//!
//! 1. **独立 ReplaySnapshotStore**：每个 session 拥有独立的 store，不污染线上数据。
//! 2. **独立引擎实例**：FormulaEngine 和 AlertRuleEngine 为独立实例（T04 占位）。
//! 3. **独立数据源**：通过 SacRangeReader trait 直接读取 SAC 文件。
//! 4. **状态不回流**：派生标签和告警仅写入 ReplaySnapshotStore。

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::metrics::SnapshotMetrics;
use crate::replay::clock::ReplayClock;
use crate::replay::derive::{ReplayAlerts, ReplayDerived};
use crate::replay::event::ReplayEvent;
use crate::replay::reader::SacReaderRef;
use crate::replay::session::{ReplayParams, ReplaySessionHandle, ReplayState};
use crate::replay::store::{ReplaySnapshotEntry, ReplaySnapshotStore};
use sensearcdb_common::{ConfigHandle, SenseArcError, TenantId};

/// 回放引擎。
pub struct ReplayEngine {
    sessions: RwLock<HashMap<String, Arc<ReplaySessionHandle>>>,
    sac_reader: SacReaderRef,
    _config_handle: ConfigHandle,
    metrics: SnapshotMetrics,
    max_sessions: usize,
    event_buffer_size: usize,
    max_entries_per_session: usize,
    shard_count: usize,
    /// 派生重算注入（None = 回退旧行为，不回放派生）。
    derived: Option<Arc<dyn ReplayDerived>>,
    /// 告警评估注入（None = 回退旧行为，不回放告警）。
    alerts: Option<Arc<dyn ReplayAlerts>>,
}

impl ReplayEngine {
    /// 创建回放引擎。
    pub fn new(
        config_handle: &ConfigHandle,
        sac_reader: SacReaderRef,
        metrics: SnapshotMetrics,
        shard_count: usize,
        derived: Option<Arc<dyn ReplayDerived>>,
        alerts: Option<Arc<dyn ReplayAlerts>>,
    ) -> Self {
        let config = config_handle.get();
        let replay_cfg = &config.snapshot.replay;

        Self {
            sessions: RwLock::new(HashMap::new()),
            sac_reader,
            _config_handle: config_handle.clone(),
            metrics,
            max_sessions: replay_cfg.max_sessions,
            event_buffer_size: replay_cfg.event_buffer_size,
            max_entries_per_session: replay_cfg.max_entries_per_session,
            shard_count,
            derived,
            alerts,
        }
    }

    /// 启动回放会话。
    pub async fn start_session(&self, params: ReplayParams) -> Result<String, ReplayEngineError> {
        {
            let sessions = self.sessions.read();
            if sessions.len() >= self.max_sessions {
                return Err(ReplayEngineError::TooManySessions {
                    current: sessions.len(),
                    max: self.max_sessions,
                });
            }
        }

        let session_id = uuid::Uuid::new_v4().to_string();
        let store = Arc::new(ReplaySnapshotStore::new(
            session_id.clone(),
            self.shard_count,
            self.max_entries_per_session,
        ));

        let session = Arc::new(ReplaySessionHandle::new(
            session_id.clone(),
            params.clone(),
            store,
            self.event_buffer_size,
        ));

        {
            let mut sessions = self.sessions.write();
            sessions.insert(session_id.clone(), session.clone());
        }

        // 状态机：start() → Playing（GAP-SNP-09）
        // 会话创建后立即进入 Playing，再 spawn 回放循环，避免循环永远停在 Idle。
        if let Err(e) = session.play() {
            tracing::warn!(session_id = %session_id, error = %e, "failed to transition replay session to Playing");
        }

        self.metrics
            .replay_active_sessions
            .with_label_values(&[])
            .set(self.sessions.read().len() as f64);

        let session_clone = session.clone();
        let sac_reader = self.sac_reader.clone();
        let derived = self.derived.clone();
        let alerts = self.alerts.clone();

        tokio::spawn(async move {
            Self::run_replay_loop(session_clone, sac_reader, derived, alerts).await;
        });

        tracing::info!(
            session_id = %session_id,
            tag_count = params.tag_ids.len(),
            speed = %params.speed,
            "replay session started"
        );

        Ok(session_id)
    }

    /// 暂停回放会话。
    pub fn pause_session(&self, session_id: &str) -> Result<(), ReplayEngineError> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(session_id)
            .ok_or(ReplayEngineError::SessionNotFound(session_id.to_string()))?;
        session.pause().map_err(ReplayEngineError::StateError)?;
        Ok(())
    }

    /// 恢复回放会话。
    pub fn resume_session(&self, session_id: &str) -> Result<(), ReplayEngineError> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(session_id)
            .ok_or(ReplayEngineError::SessionNotFound(session_id.to_string()))?;
        session.resume().map_err(ReplayEngineError::StateError)?;
        Ok(())
    }

    /// 停止回放会话。
    pub fn stop_session(&self, session_id: &str) -> Result<(), ReplayEngineError> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(session_id)
            .ok_or(ReplayEngineError::SessionNotFound(session_id.to_string()))?;
        session.stop();
        Ok(())
    }

    /// 获取会话句柄。
    pub fn get_session(&self, session_id: &str) -> Option<Arc<ReplaySessionHandle>> {
        self.sessions.read().get(session_id).cloned()
    }

    /// 列出所有会话 ID。
    pub fn list_sessions(&self) -> Vec<String> {
        self.sessions.read().keys().cloned().collect()
    }

    /// 清理已终止的会话。
    pub fn cleanup_terminated(&self) -> usize {
        let mut sessions = self.sessions.write();
        let before = sessions.len();
        sessions.retain(|_, session| !session.current_state().is_terminal());
        let removed = before - sessions.len();

        if removed > 0 {
            self.metrics
                .replay_active_sessions
                .with_label_values(&[])
                .set(sessions.len() as f64);
            tracing::info!(removed = removed, "cleaned up terminated replay sessions");
        }

        removed
    }

    /// 回放主循环（后台 tokio task）。
    async fn run_replay_loop(
        session: Arc<ReplaySessionHandle>,
        sac_reader: SacReaderRef,
        derived: Option<Arc<dyn ReplayDerived>>,
        alerts: Option<Arc<dyn ReplayAlerts>>,
    ) {
        // Wait for Playing state
        {
            let mut attempts = 0;
            while session.current_state() == ReplayState::Idle && attempts < 100 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                attempts += 1;
            }
            if session.current_state() != ReplayState::Playing {
                tracing::warn!(
                    session_id = %session.id,
                    "session did not transition to Playing, aborting"
                );
                return;
            }
        }

        session.push_event(ReplayEvent::state_change("Idle", "Playing", 0));
        let mut clock = ReplayClock::new(session.params.speed, session.params.start_ts);

        let tag_ids = session.params.tag_ids.clone();
        let start_ts = session.params.start_ts;
        let end_ts = session.params.end_ts;

        for table_id in &tag_ids {
            if session.is_cancelled() {
                break;
            }

            // Check if paused
            while session.current_state() == ReplayState::Paused {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                if session.is_cancelled() {
                    break;
                }
            }
            if session.is_cancelled() {
                break;
            }

            match sac_reader.read_range(
                sensearcdb_common::TenantId::DEFAULT,
                *table_id,
                start_ts,
                end_ts,
            ) {
                Ok(rows) => {
                    for row in rows {
                        if session.is_cancelled() {
                            break;
                        }

                        clock.advance_to(row.ts).await;

                        let entry = ReplaySnapshotEntry {
                            value: row.value,
                            quality: row.quality,
                            event_ts: row.ts,
                            version: 0,
                            derived_value: None,
                            derived_quality: None,
                        };

                        match session.store.insert(*table_id, entry) {
                            Ok(_) => {
                                session.inc_processed();
                                session.push_event(ReplayEvent::next_value(
                                    row.ts,
                                    session.stats.read().processed_count,
                                    session.progress(),
                                ));
                            }
                            Err(e) => {
                                session.set_error(e.to_string());
                                break;
                            }
                        }

                        if session.is_cancelled() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    session.inc_errors();
                    session.push_event(ReplayEvent::error(
                        format!("SAC read error for tag {}: {}", table_id, e),
                        clock.current_ts(),
                    ));
                }
            }
        }

        // ─── 阶段 2：派生重算（两遍法 as-of join，禁止边回放边派生）───
        let mut derived_streams: HashMap<String, Vec<(i64, f64, u8)>> = HashMap::new();
        if !session.is_cancelled() && session.params.include_derived {
            match &derived {
                Some(adapter) => {
                    let output = adapter.recompute(&sac_reader, start_ts, end_ts).await;
                    if output.definition_count == 0 {
                        session.push_event(ReplayEvent::notice(
                            "derived_definitions=0",
                            clock.current_ts(),
                        ));
                        tracing::warn!(
                            session_id = %session.id,
                            "include_derived=true but derived_definitions=0"
                        );
                    }
                    derived_streams = output.derived_streams;
                    for p in output.points {
                        session
                            .store
                            .set_derived(p.table_id, p.value, p.quality, p.event_ts);
                        session.inc_derived();
                        session.push_event(ReplayEvent::derived(
                            p.table_id, p.value, p.quality, p.event_ts,
                        ));
                    }
                }
                None => {
                    session.push_event(ReplayEvent::notice(
                        "derived_definitions=0: derived replay not wired",
                        clock.current_ts(),
                    ));
                    tracing::warn!(
                        session_id = %session.id,
                        "include_derived=true but derived adapter not injected"
                    );
                }
            }
        }

        // ─── 阶段 3：告警评估（规则快照 + evaluate_at(event_ts)）───
        if !session.is_cancelled() && session.params.include_alerts {
            match &alerts {
                Some(adapter) => {
                    let output = adapter
                        .evaluate(&sac_reader, start_ts, end_ts, &derived_streams)
                        .await;
                    if output.rule_count == 0 {
                        session.push_event(ReplayEvent::notice(
                            "enabled_alert_rules=0",
                            clock.current_ts(),
                        ));
                        tracing::warn!(
                            session_id = %session.id,
                            "include_alerts=true but enabled_alert_rules=0"
                        );
                    }
                    for ev in output.events {
                        session.inc_alerts();
                        session.push_event(ReplayEvent::alert(
                            ev.rule_id,
                            ev.severity,
                            ev.message,
                            ev.event_ts,
                        ));
                    }
                }
                None => {
                    session.push_event(ReplayEvent::notice(
                        "enabled_alert_rules=0: alert replay not wired",
                        clock.current_ts(),
                    ));
                    tracing::warn!(
                        session_id = %session.id,
                        "include_alerts=true but alert adapter not injected"
                    );
                }
            }
        }

        if session.current_state() == ReplayState::Playing {
            session.complete();
            session.push_event(ReplayEvent::completed(
                session.stats.read().processed_count,
                session.created_at.elapsed().as_nanos() as u64,
                clock.current_ts(),
            ));
            session.push_event(ReplayEvent::state_change(
                "Playing",
                "Completed",
                clock.current_ts(),
            ));
        }

        tracing::info!(
            session_id = %session.id,
            state = %session.current_state(),
            processed = session.stats.read().processed_count,
            "replay loop finished"
        );
    }
}

impl std::fmt::Debug for ReplayEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayEngine")
            .field("active_sessions", &self.sessions.read().len())
            .field("max_sessions", &self.max_sessions)
            .finish()
    }
}

/// 回放引擎错误。
#[derive(Debug, thiserror::Error)]
pub enum ReplayEngineError {
    #[error("session not found: {0}")]
    SessionNotFound(String),
    #[error("too many sessions: {current}/{max}")]
    TooManySessions { current: usize, max: usize },
    #[error("state error: {0}")]
    StateError(String),
}

/// 回放引擎错误 → 统一错误收敛（内部基础设施错误）。
impl From<ReplayEngineError> for SenseArcError {
    fn from(e: ReplayEngineError) -> Self {
        SenseArcError::Internal(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::clock::ReplaySpeed;
    use crate::replay::derive::{
        ReplayAlertEvent, ReplayAlertOutput, ReplayDerivedOutput, ReplayDerivedPoint,
    };
    use crate::replay::reader::{NoopSacRangeReader, SacDataRow, SacRangeReader, SacReaderError};
    use async_trait::async_trait;
    use sensearcdb_common::AppConfig;

    fn make_engine() -> ReplayEngine {
        let cfg = ConfigHandle::new(AppConfig::default());
        let sac_reader = Arc::new(NoopSacRangeReader);
        ReplayEngine::new(&cfg, sac_reader, SnapshotMetrics::noop(), 4, None, None)
    }

    #[tokio::test]
    async fn test_start_session() {
        let engine = make_engine();
        let params = ReplayParams {
            tag_ids: vec![1],
            start_ts: 0,
            end_ts: 1000,
            speed: ReplaySpeed(0.0),
            include_derived: false,
            include_alerts: false,
        };
        let session_id = engine.start_session(params).await.unwrap();
        assert!(!session_id.is_empty());
        assert!(engine.get_session(&session_id).is_some());
    }

    #[tokio::test]
    async fn test_session_not_found() {
        let engine = make_engine();
        assert!(engine.pause_session("nonexistent").is_err());
    }

    #[test]
    fn test_cleanup_terminated() {
        let engine = make_engine();
        assert_eq!(engine.cleanup_terminated(), 0);
    }

    // ─── 回放派生/告警接线回归（铁律：真读 SAC、真 evaluate_at、隔离写回）───

    /// 返回固定 raw 行的 SAC reader，驱动回放主循环。
    struct RowSacReader;

    impl SacRangeReader for RowSacReader {
        fn read_range(
            &self,
            _tenant_id: TenantId,
            _table_id: u32,
            _start_ts: i64,
            _end_ts: i64,
        ) -> Result<Vec<SacDataRow>, SacReaderError> {
            Ok(vec![SacDataRow {
                tenant_id: TenantId::DEFAULT,
                ts: 1000,
                value: 1.0,
                quality: 0xC0,
                server_ts: 0,
                is_derived: false,
                values: vec![],
                qualities: vec![],
            }])
        }
    }

    /// 返回固定派生点的 mock adapter。
    struct MockDerived;

    #[async_trait]
    impl ReplayDerived for MockDerived {
        async fn recompute(
            &self,
            _sac_reader: &SacReaderRef,
            _start_ts: i64,
            _end_ts: i64,
        ) -> ReplayDerivedOutput {
            ReplayDerivedOutput {
                definition_count: 1,
                points: vec![ReplayDerivedPoint {
                    table_id: 99,
                    value: 42.0,
                    quality: 0xC0,
                    event_ts: 2000,
                }],
                derived_streams: HashMap::new(),
            }
        }
    }

    /// 返回 0 定义的 mock adapter（触发显式降级提示）。
    struct EmptyDerived;

    #[async_trait]
    impl ReplayDerived for EmptyDerived {
        async fn recompute(
            &self,
            _sac_reader: &SacReaderRef,
            _start_ts: i64,
            _end_ts: i64,
        ) -> ReplayDerivedOutput {
            ReplayDerivedOutput {
                definition_count: 0,
                points: vec![],
                derived_streams: HashMap::new(),
            }
        }
    }

    /// 返回固定告警事件的 mock adapter。
    struct MockAlerts;

    #[async_trait]
    impl ReplayAlerts for MockAlerts {
        async fn evaluate(
            &self,
            _sac_reader: &SacReaderRef,
            _start_ts: i64,
            _end_ts: i64,
            _derived_streams: &HashMap<String, Vec<(i64, f64, u8)>>,
        ) -> ReplayAlertOutput {
            ReplayAlertOutput {
                rule_count: 1,
                events: vec![ReplayAlertEvent {
                    rule_id: "r1".into(),
                    severity: 1,
                    message: "Hi: 85 > 80".into(),
                    event_ts: 3000,
                }],
            }
        }
    }

    fn make_engine_with(
        derived: Option<Arc<dyn ReplayDerived>>,
        alerts: Option<Arc<dyn ReplayAlerts>>,
    ) -> ReplayEngine {
        let cfg = ConfigHandle::new(AppConfig::default());
        let sac_reader: SacReaderRef = Arc::new(RowSacReader);
        ReplayEngine::new(
            &cfg,
            sac_reader,
            SnapshotMetrics::noop(),
            4,
            derived,
            alerts,
        )
    }

    async fn run_and_wait(
        engine: &ReplayEngine,
        include_derived: bool,
        include_alerts: bool,
    ) -> Arc<ReplaySessionHandle> {
        let params = ReplayParams {
            tag_ids: vec![1],
            start_ts: 0,
            end_ts: 5000,
            speed: ReplaySpeed(0.0),
            include_derived,
            include_alerts,
        };
        let id = engine.start_session(params).await.unwrap();
        let handle = engine.get_session(&id).unwrap();
        for _ in 0..200 {
            if handle.current_state().is_terminal() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        handle
    }

    #[tokio::test]
    async fn test_derived_recompute_writes_store_and_emits_event() {
        let engine = make_engine_with(Some(Arc::new(MockDerived)), None);
        let handle = run_and_wait(&engine, true, false).await;

        assert_eq!(handle.current_state(), ReplayState::Completed);
        // 阶段 1：raw 照旧落库。
        assert_eq!(handle.stats.read().processed_count, 1);
        // 阶段 2：派生结果只写 derived 字段。
        let entry = handle.store.get(99).unwrap();
        assert_eq!(entry.derived_value, Some(42.0));
        assert_eq!(entry.derived_quality, Some(0xC0));
        assert_eq!(handle.stats.read().derived_count, 1);
        assert!(handle
            .get_recent_events(100)
            .iter()
            .any(|e| matches!(e, ReplayEvent::Derived { .. })));
    }

    #[tokio::test]
    async fn test_alert_evaluation_increments_and_emits_event() {
        let engine = make_engine_with(None, Some(Arc::new(MockAlerts)));
        let handle = run_and_wait(&engine, false, true).await;

        assert_eq!(handle.current_state(), ReplayState::Completed);
        assert_eq!(handle.stats.read().alert_count, 1);
        assert!(handle
            .get_recent_events(100)
            .iter()
            .any(|e| matches!(e, ReplayEvent::Alert { .. })));
    }

    /// 「先越界后恢复」SAC 序列：1000=50(正常)、2000=120(越界)、3000=60(恢复)。
    struct RecoverSeriesReader;

    impl SacRangeReader for RecoverSeriesReader {
        fn read_range(
            &self,
            _tenant_id: TenantId,
            table_id: u32,
            _start_ts: i64,
            _end_ts: i64,
        ) -> Result<Vec<SacDataRow>, SacReaderError> {
            assert_eq!(table_id, 1);
            Ok(vec![
                SacDataRow {
                    tenant_id: TenantId::DEFAULT,
                    ts: 1000,
                    value: 50.0,
                    quality: 0xC0,
                    server_ts: 0,
                    is_derived: false,
                    values: vec![],
                    qualities: vec![],
                },
                SacDataRow {
                    tenant_id: TenantId::DEFAULT,
                    ts: 2000,
                    value: 120.0,
                    quality: 0xC0,
                    server_ts: 0,
                    is_derived: false,
                    values: vec![],
                    qualities: vec![],
                },
                SacDataRow {
                    tenant_id: TenantId::DEFAULT,
                    ts: 3000,
                    value: 60.0,
                    quality: 0xC0,
                    server_ts: 0,
                    is_derived: false,
                    values: vec![],
                    qualities: vec![],
                },
            ])
        }
    }

    /// 逐 event_ts 推进的告警适配器：读 `sac_reader` 全序列，逐点比较阈值。
    ///
    /// 这是「修复后」行为的最小镜像，用于在 snapshot 层锁定新 trait 契约：
    /// 引擎必须把 SAC reader 传给 `evaluate`，并把返回的每个 Alert 事件
    /// 全部 emit（含中间越界点），而非只在窗口末报一次。
    struct PerPointAlerts;

    #[async_trait]
    impl ReplayAlerts for PerPointAlerts {
        async fn evaluate(
            &self,
            sac_reader: &SacReaderRef,
            start_ts: i64,
            end_ts: i64,
            _derived_streams: &HashMap<String, Vec<(i64, f64, u8)>>,
        ) -> ReplayAlertOutput {
            let rows = sac_reader
                .read_range(sensearcdb_common::TenantId::DEFAULT, 1, start_ts, end_ts)
                .unwrap();
            let mut events = Vec::new();
            for r in rows {
                if r.value > 100.0 {
                    events.push(ReplayAlertEvent {
                        rule_id: "r1".into(),
                        severity: 1,
                        message: format!("Hi: {} > 100", r.value),
                        event_ts: r.ts,
                    });
                }
            }
            ReplayAlertOutput {
                rule_count: 1,
                events,
            }
        }
    }

    #[tokio::test]
    async fn test_replay_alerts_emit_intermediate_recovered_point() {
        let cfg = ConfigHandle::new(AppConfig::default());
        let sac_reader: SacReaderRef = Arc::new(RecoverSeriesReader);
        let engine = ReplayEngine::new(
            &cfg,
            sac_reader,
            SnapshotMetrics::noop(),
            4,
            None,
            Some(Arc::new(PerPointAlerts)),
        );

        let params = ReplayParams {
            tag_ids: vec![1],
            start_ts: 0,
            end_ts: 5000,
            speed: ReplaySpeed(0.0),
            include_derived: false,
            include_alerts: true,
        };
        let id = engine.start_session(params).await.unwrap();
        let handle = engine.get_session(&id).unwrap();
        for _ in 0..200 {
            if handle.current_state().is_terminal() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert_eq!(handle.current_state(), ReplayState::Completed);
        assert_eq!(
            handle.stats.read().alert_count,
            1,
            "只应在中间越界点报一次，恢复点不得再报"
        );
        let alert_ts: Vec<i64> = handle
            .get_recent_events(100)
            .into_iter()
            .filter_map(|e| match e {
                ReplayEvent::Alert { event_ts, .. } => Some(event_ts),
                _ => None,
            })
            .collect();
        assert_eq!(
            alert_ts,
            vec![2000],
            "Alert 必须落在中间越界点 2000，而非窗口末 3000"
        );
    }

    #[tokio::test]
    async fn test_degrade_notice_when_no_definitions() {
        let engine = make_engine_with(Some(Arc::new(EmptyDerived)), None);
        let handle = run_and_wait(&engine, true, false).await;

        assert_eq!(handle.current_state(), ReplayState::Completed);
        assert_eq!(handle.stats.read().derived_count, 0);
        assert!(handle
            .get_recent_events(100)
            .iter()
            .any(|e| matches!(e, ReplayEvent::Notice { message, .. } if message.contains("derived_definitions=0"))));
    }

    #[tokio::test]
    async fn test_no_derived_when_include_false() {
        let engine = make_engine_with(Some(Arc::new(MockDerived)), None);
        let handle = run_and_wait(&engine, false, false).await;

        assert_eq!(handle.current_state(), ReplayState::Completed);
        assert_eq!(handle.stats.read().derived_count, 0);
        assert_eq!(handle.stats.read().alert_count, 0);
        assert!(!handle
            .get_recent_events(100)
            .iter()
            .any(|e| matches!(e, ReplayEvent::Derived { .. })));
    }
}

//! Derived 去抖 flusher（P4）：异步批量重算派生标签。
//!
//! 写路径只 [`DerivedTagEngine::mark_dirty`] 标脏，不再内联重算；本 flusher 定时
//! （`derived.debounce_interval_ms`）或 notify 双触发 [`DerivedTagEngine::flush_ready`]。
//!
//! # 时序编排（读前已 flush）
//!
//! 可选 `pre_flush` hook 由集成层注入 snapshot flush。每次重算前先执行
//! `pre_flush()`（= `SnapshotFlusher::flush_now()`），把本批源值同步 flush 到
//! snapshot store，再 `take_ready_tags` → `recalculate`，保证 derived 读 snapshot
//! 时已是「最新已 flush 值」，消除写路径同步读异步 flush 的时序耦合。
//!
//! 仿 `sensearcdb-snapshot` 的 `SnapshotFlusher`「定时 + notify」范式。

use std::sync::Arc;
use std::time::Duration;

use sensearcdb_common::ConfigHandle;

use crate::engine::DerivedTagEngine;

/// 独立去抖 flusher：定时 / notify 双触发，消费 dirty tag 批量重算。
pub struct DerivedFlusher {
    engine: Arc<DerivedTagEngine>,
    config: ConfigHandle,
    /// 每次重算前执行的钩子（集成层注入 snapshot flush，保证读前已 flush）。
    pre_flush: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl DerivedFlusher {
    pub fn new(
        engine: Arc<DerivedTagEngine>,
        config: ConfigHandle,
        pre_flush: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Self {
        Self {
            engine,
            config,
            pre_flush,
        }
    }

    /// 主循环：定时唤醒 或 notify 唤醒 → pre_flush → flush_ready。
    pub async fn run(self: Arc<Self>) {
        loop {
            let interval = {
                let cfg = self.config.get();
                Duration::from_millis(cfg.derived.debounce_interval_ms.max(1))
            };
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = self.engine.flush_notified() => {}
            }
            // 时序编排：先确保 snapshot 已 flush（读前已 flush），再重算。
            if let Some(pre_flush) = &self.pre_flush {
                pre_flush();
            }
            self.engine.flush_ready();
        }
    }
}

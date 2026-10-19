//! ReplayEngine — 历史数据回放子系统。
//!
//! ## 模块结构
//!
//! - `engine` — ReplayEngine 主控制器，管理所有会话。
//! - `session` — ReplaySessionHandle 会话句柄和状态机。
//! - `store` — ReplaySnapshotStore per-session 独立存储。
//! - `clock` — 回放时钟：速度控制 + 耗时补偿。
//! - `reader` — SacRangeReader trait：SAC 数据读取抽象。
//! - `api` — 回放 REST 端点。
//! - `event` — ReplayEvent 事件类型定义。

pub mod api;
pub mod clock;
pub mod derive;
pub mod engine;
pub mod event;
pub mod reader;
pub mod session;
pub mod store;

// Re-exports
pub use clock::{ReplayClock, ReplaySpeed};
pub use derive::{
    ReplayAlertEvent, ReplayAlertOutput, ReplayAlerts, ReplayDerived, ReplayDerivedOutput,
    ReplayDerivedPoint,
};
pub use engine::ReplayEngine;
pub use event::ReplayEvent;
pub use reader::{NoopSacRangeReader, SacDataRow, SacRangeReader, SacReaderError, SacReaderRef};
pub use session::{ReplayParams, ReplaySessionHandle, ReplayState, ReplayStats};
pub use store::ReplaySnapshotStore;

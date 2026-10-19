//! Replay computation traits (dependency inversion).
//!
//! The replay engine computes derived tags and alerts in an **isolated** session
//! context. To keep `sensearcdb-snapshot` free of any compile-time dependency on
//! `sensearcdb-derived` / `sensearcdb-alert`, those computations are abstracted
//! behind the [`ReplayDerived`] and [`ReplayAlerts`] traits below. The integration
//! layer (`src/startup.rs`) supplies concrete adapters that reuse the online
//! formula/rule engines' pure evaluators.

use async_trait::async_trait;
use std::collections::HashMap;

use crate::replay::reader::SacReaderRef;

/// A single recomputed derived-tag point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplayDerivedPoint {
    /// The derived tag's table ID.
    pub table_id: u32,
    /// Computed value.
    pub value: f64,
    /// Computed quality code.
    pub quality: u8,
    /// Event timestamp (epoch ns) of the point.
    pub event_ts: i64,
}

/// The output of a derived recompute pass.
#[derive(Debug, Clone, Default)]
pub struct ReplayDerivedOutput {
    /// Number of derived-tag definitions frozen for this session.
    pub definition_count: usize,
    /// All computed points, in dependency order.
    pub points: Vec<ReplayDerivedPoint>,
    /// Per-tag output streams keyed by derived tag name, in dependency (topo)
    /// order. Each stream is an `(event_ts, value, quality)` sequence sorted by
    /// `event_ts` ascending. The alert stage consumes these to advance
    /// derived-source rules point-by-point (single source, no cross-table
    /// as-of join).
    pub derived_streams: HashMap<String, Vec<(i64, f64, u8)>>,
}

/// A single replay alert event.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayAlertEvent {
    /// Alert rule ID.
    pub rule_id: String,
    /// Severity (0=Info, 1=Warning, 2=Critical, 3=Emergency).
    pub severity: u8,
    /// Human-readable alert message.
    pub message: String,
    /// Event timestamp (epoch ns).
    pub event_ts: i64,
}

/// The output of an alert evaluation pass.
#[derive(Debug, Clone, Default)]
pub struct ReplayAlertOutput {
    /// Number of enabled alert rules evaluated.
    pub rule_count: usize,
    /// Fired alerts.
    pub events: Vec<ReplayAlertEvent>,
}

/// Derived-tag recompute abstraction.
///
/// Implementations must perform the **cross-table as-of join** against SAC history
/// (two-pass baseline), not read `ReplaySnapshotStore` (which only holds latest
/// values). Results are returned to the engine, which writes them via
/// [`ReplaySnapshotStore::set_derived`] and emits [`crate::replay::event::ReplayEvent::Derived`].
#[async_trait]
pub trait ReplayDerived: Send + Sync {
    /// Recompute all derived tags for a session.
    ///
    /// # Arguments
    /// * `sac_reader` - Reader for raw source-tag histories.
    /// * `start_ts` / `end_ts` - Replay time range (epoch ns, inclusive).
    async fn recompute(
        &self,
        sac_reader: &SacReaderRef,
        start_ts: i64,
        end_ts: i64,
    ) -> ReplayDerivedOutput;
}

/// Alert evaluation abstraction.
///
/// Implementations evaluate the session-frozen enabled threshold rules point-by-
/// point over the replay time range and return fired alerts. They must **not**
/// persist to `AlertStore` or route notifications — results only feed
/// [`crate::replay::event::ReplayEvent::Alert`].
#[async_trait]
pub trait ReplayAlerts: Send + Sync {
    /// Evaluate all enabled alert rules over the replay time range.
    ///
    /// Each rule is advanced point-by-point over its source tag's event-time
    /// series: raw sources read SAC history via `sac_reader`, derived sources
    /// consume the already-computed `derived_streams`. Implementations stamp
    /// results with the event timestamp (`evaluate_at`) rather than wall-clock
    /// time, and must **not** persist to `AlertStore` — results only feed
    /// [`crate::replay::event::ReplayEvent::Alert`].
    async fn evaluate(
        &self,
        sac_reader: &SacReaderRef,
        start_ts: i64,
        end_ts: i64,
        derived_streams: &HashMap<String, Vec<(i64, f64, u8)>>,
    ) -> ReplayAlertOutput;
}

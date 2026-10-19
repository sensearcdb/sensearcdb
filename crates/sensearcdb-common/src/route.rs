//! CQ route provider trait.
//!
//! Defines a minimal trait for CQ engine route query capability.
//! This trait lives in `sensearcdb-common` to avoid circular dependencies
//! between `sensearcdb-engine` and `sensearcdb-cq`.
//!
//! Both crates depend on `sensearcdb-common`, enabling the engine's
//! `AutoRouter` to call into the CQ engine through this trait without
//! a direct compile-time dependency.

/// Hint returned by the CQ route provider when a CQ path is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CqRouteHint {
    /// The CQ target table ID that should be queried instead of the raw table.
    pub target_table_id: u32,
}

/// Trait for querying whether a CQ-accelerated route exists.
///
/// Implemented by the CQ engine and consumed by the query engine's `AutoRouter`.
/// The engine holds an `Arc<dyn CqRouteProvider>` — since both crates share
/// `sensearcdb-common`, there is zero circular dependency.
pub trait CqRouteProvider: Send + Sync {
    /// Check whether a query can be routed to a CQ target table.
    ///
    /// # Arguments
    /// * `table_id` — the source (raw) table being queried.
    /// * `time_range_ms` — the query window length in milliseconds; 0 means
    ///   no window constraint.
    ///
    /// # Returns
    /// `Some(CqRouteHint)` when a matching CQ window exists, otherwise `None`.
    fn route_query(&self, table_id: u32, time_range_ms: i64) -> Option<CqRouteHint>;
}

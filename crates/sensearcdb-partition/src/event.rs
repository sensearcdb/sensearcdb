//! Partition event bus for asynchronous lifecycle notifications.
//!
//! The `PartitionEventBus` is a `tokio::sync::broadcast` channel with a
//! **hard-coded capacity of 1024**. This capacity is welded shut — do not
//! modify it. The large buffer handles partition-switch storms (e.g., batch
//! backfill triggering hundreds of simultaneous switches) without dropping
//! events, preventing downstream subscribers from falling back to their
//! periodic full-scan slow path.
//!
//! # Architecture Contract (§10)
//!
//! Events sent through this bus are **best-effort** for latency reduction.
//! Downstream consumers (TTLCleaner, TieredStorageManager, BackupManager)
//! **MUST** also perform periodic full scans as their truth source. They
//! must **never** assume that a `PartitionEvent` was delivered.

use sensearcdb_common::TenantId;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Events emitted by the partition manager during lifecycle transitions.
///
/// Each variant carries the minimum identifying information that a subscriber
/// needs to react. Subscribers should use this as a hint and validate against
/// the `PartitionManager` for ground truth.
#[derive(Debug, Clone)]
pub enum PartitionEvent {
    /// A partition has been advanced: the old one was frozen,
    /// and a new active partition was created.
    Advanced {
        /// The table that was advanced.
        table_id: u32,
        /// The partition_ns of the partition that was frozen.
        partition_ns: i64,
    },

    /// A partition has completed L1→L2 compaction and is now Archived.
    Archived {
        /// The table containing the archived partition.
        table_id: u32,
        /// The partition_ns of the archived partition.
        partition_ns: i64,
        /// The maximum timestamp observed in this partition.
        max_ts: i64,
    },

    /// A partition's TTL has expired and it is now in the Expired state.
    Expired {
        /// The table containing the expired partition.
        table_id: u32,
        /// 过期分区所属租户（F5：快速路径物理删除按真实租户定位 SAC 目录）。
        tenant_id: TenantId,
        /// The partition_ns of the expired partition.
        partition_ns: i64,
    },

    /// A backup operation has started. Downstream should freeze
    /// relevant partition state transitions.
    BackupStarted {
        /// Unique identifier for this backup operation.
        backup_id: String,
    },

    /// A backup operation has completed. Downstream may unfreeze
    /// partition state transitions.
    BackupCompleted {
        /// Unique identifier for this backup operation.
        backup_id: String,
    },
}

/// Broadcast event bus for partition lifecycle events.
///
/// # Capacity
///
/// The capacity is **hard-coded to 1024** and must not be changed.
/// See module-level documentation for rationale.
///
/// # Lagged receivers
///
/// When a receiver is too slow and the channel buffer fills up,
/// `tokio::sync::broadcast` marks the receiver as `Lagged`. This
/// implementation logs an ERROR-level message when `send()` returns
/// `Ok(0)` (all receivers lagged) as an early-warning signal, but
/// the system continues operating normally — subscribers fall back
/// to their periodic full-scan truth source.
pub struct PartitionEventBus {
    tx: broadcast::Sender<PartitionEvent>,
}

impl PartitionEventBus {
    /// Create a new event bus with capacity 1024.
    ///
    /// # Panics
    ///
    /// This constructor does not panic. The `tokio::sync::broadcast::channel`
    /// only panics when capacity is 0, which is statically impossible here.
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self { tx }
    }

    /// Subscribe to partition events.
    ///
    /// The returned receiver will receive all events broadcast after
    /// the subscription is created. It will **not** receive events
    /// that were sent before the subscription.
    pub fn subscribe(&self) -> broadcast::Receiver<PartitionEvent> {
        self.tx.subscribe()
    }

    /// Send an event to all subscribers.
    ///
    /// Returns the number of receivers that received the event.
    ///
    /// # Errors
    ///
    /// Returns `SendError` if there are no active receivers.
    ///
    /// # Lagged receivers
    ///
    /// When all receivers have lagged, this returns `Ok(0)`. An ERROR-level
    /// log is emitted in this case as an early-warning diagnostic.
    pub fn send(
        &self,
        event: PartitionEvent,
    ) -> Result<usize, broadcast::error::SendError<PartitionEvent>> {
        match self.tx.send(event) {
            Ok(n) => {
                if n == 0 {
                    tracing::error!(
                        "PartitionEventBus::send: all receivers have lagged — \
                         subscribers will fall back to periodic full scan"
                    );
                }
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }

    /// Return the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for PartitionEventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PartitionEventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionEventBus")
            .field("capacity", &1024)
            .field("receiver_count", &self.tx.receiver_count())
            .finish()
    }
}

/// Convenience type alias for an `Arc<PartitionEventBus>`.
pub type SharedEventBus = Arc<PartitionEventBus>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_bus_creation() {
        let bus = PartitionEventBus::new();
        assert_eq!(bus.receiver_count(), 0);
    }

    #[test]
    fn test_event_bus_send_and_receive() {
        let bus = PartitionEventBus::new();
        let mut rx = bus.subscribe();

        let event = PartitionEvent::Advanced {
            table_id: 1,
            partition_ns: 1000,
        };

        let sent_count = bus.send(event.clone()).unwrap();
        assert_eq!(sent_count, 1);

        let received = rx.try_recv().unwrap();
        match received {
            PartitionEvent::Advanced {
                table_id,
                partition_ns,
            } => {
                assert_eq!(table_id, 1);
                assert_eq!(partition_ns, 1000);
            }
            _ => panic!("expected Advanced event"),
        }
    }

    #[test]
    fn test_event_bus_multiple_subscribers() {
        let bus = PartitionEventBus::new();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        let event = PartitionEvent::Archived {
            table_id: 2,
            partition_ns: 2000,
            max_ts: 2500,
        };

        let sent_count = bus.send(event).unwrap();
        assert_eq!(sent_count, 2);

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn test_event_bus_default() {
        let bus = PartitionEventBus::default();
        assert_eq!(bus.receiver_count(), 0);

        let mut rx = bus.subscribe();
        let _ = bus
            .send(PartitionEvent::Expired {
                table_id: 3,
                tenant_id: TenantId::DEFAULT,
                partition_ns: 3000,
            })
            .unwrap();

        let received = rx.try_recv().unwrap();
        match received {
            PartitionEvent::Expired {
                table_id,
                tenant_id,
                partition_ns,
            } => {
                assert_eq!(table_id, 3);
                assert_eq!(tenant_id, TenantId::DEFAULT);
                assert_eq!(partition_ns, 3000);
            }
            _ => panic!("expected Expired event"),
        }
    }

    #[test]
    fn test_event_bus_debug_format() {
        let bus = PartitionEventBus::new();
        let debug_str = format!("{:?}", bus);
        assert!(debug_str.contains("capacity"));
        assert!(debug_str.contains("1024"));
    }

    #[test]
    fn test_all_event_variants() {
        let bus = PartitionEventBus::new();

        // Advanced
        let mut rx1 = bus.subscribe();
        bus.send(PartitionEvent::Advanced {
            table_id: 1,
            partition_ns: 100,
        })
        .unwrap();
        assert!(rx1.try_recv().is_ok());

        // Archived
        let mut rx2 = bus.subscribe();
        bus.send(PartitionEvent::Archived {
            table_id: 2,
            partition_ns: 200,
            max_ts: 299,
        })
        .unwrap();
        assert!(rx2.try_recv().is_ok());

        // Expired
        let mut rx3 = bus.subscribe();
        bus.send(PartitionEvent::Expired {
            table_id: 3,
            tenant_id: TenantId(7),
            partition_ns: 300,
        })
        .unwrap();
        assert!(rx3.try_recv().is_ok());

        // BackupStarted
        let mut rx4 = bus.subscribe();
        bus.send(PartitionEvent::BackupStarted {
            backup_id: "backup-001".to_string(),
        })
        .unwrap();
        assert!(rx4.try_recv().is_ok());

        // BackupCompleted
        let mut rx5 = bus.subscribe();
        bus.send(PartitionEvent::BackupCompleted {
            backup_id: "backup-001".to_string(),
        })
        .unwrap();
        assert!(rx5.try_recv().is_ok());
    }

    #[test]
    fn test_shared_event_bus() {
        let bus = Arc::new(PartitionEventBus::new());
        let bus2 = Arc::clone(&bus);
        let mut rx = bus2.subscribe();
        bus.send(PartitionEvent::Advanced {
            table_id: 99,
            partition_ns: 0,
        })
        .unwrap();
        assert!(rx.try_recv().is_ok());
    }
}

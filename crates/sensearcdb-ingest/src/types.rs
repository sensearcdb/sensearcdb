//! Write result types for the ingest layer.
//!
//! Defines `WriteReceipt`, `WriteStatus`, and `EngineWriteError`
//! returned by `EngineWriter::write_batch()`.

use sensearcdb_common::SenseArcError;

/// Receipt returned after a write operation.
///
/// Contains the number of points written, any failures, latency,
/// and the status indicating whether the write was accepted or buffered.
#[derive(Debug, Clone)]
pub struct WriteReceipt {
    /// Number of data points successfully written.
    pub points_written: u32,
    /// Number of data points that failed to write.
    pub points_failed: u32,
    /// End-to-end write latency in milliseconds.
    pub latency_ms: u64,
    /// Status of the write operation.
    pub write_status: WriteStatus,
}

impl WriteReceipt {
    /// Create a new accepted WriteReceipt.
    pub fn accepted(wal_seq: u64, points: u32, latency_ms: u64) -> Self {
        Self {
            points_written: points,
            points_failed: 0,
            latency_ms,
            write_status: WriteStatus::Accepted { wal_seq },
        }
    }

    /// Create a new buffered (store-and-forward) WriteReceipt.
    pub fn buffered(sf_seq: u64, points: u32, backlog_count: u64, latency_ms: u64) -> Self {
        Self {
            points_written: points,
            points_failed: 0,
            latency_ms,
            write_status: WriteStatus::Buffered {
                sf_seq,
                sf_backlog_count: backlog_count,
            },
        }
    }

    /// Create a WriteReceipt for a rejected write.
    pub fn rejected(points: u32, reason: &str, latency_ms: u64) -> Self {
        Self {
            points_written: 0,
            points_failed: points,
            latency_ms,
            write_status: WriteStatus::Rejected {
                reason: reason.to_string(),
            },
        }
    }

    /// Returns `true` if all points were accepted.
    #[inline]
    pub fn is_fully_accepted(&self) -> bool {
        matches!(self.write_status, WriteStatus::Accepted { .. }) && self.points_failed == 0
    }
}

/// The status outcome of a write operation.
#[derive(Debug, Clone)]
pub enum WriteStatus {
    /// Write accepted by the engine, assigned a WAL sequence number.
    Accepted {
        /// WAL sequence number assigned to this write.
        wal_seq: u64,
    },
    /// Write buffered in store-and-forward (engine not reachable).
    Buffered {
        /// Store-and-forward sequence number.
        sf_seq: u64,
        /// Current backlog count in the store-and-forward buffer.
        sf_backlog_count: u64,
    },
    /// Write was rejected.
    Rejected {
        /// Human-readable reason for rejection.
        reason: String,
    },
}

/// Errors that can occur when writing to the engine.
#[derive(Debug, thiserror::Error)]
pub enum EngineWriteError {
    /// The internal write channel is full (backpressure).
    #[error("channel full")]
    ChannelFull,

    /// The engine memtable is stalled (backpressure): the batch was NOT written,
    /// so the client may retry the same batch without duplicating data.
    #[error("engine backpressure: memtable stalled")]
    Backpressure,

    /// The target table has been deleted.
    #[error("table deleted: {0}")]
    TableDeleted(u32),

    /// The target table was not found.
    #[error("table not found: {0}")]
    TableNotFound(u32),

    /// The target partition is frozen and not accepting writes.
    #[error("partition frozen: table={0}, partition={1}")]
    PartitionFrozen(u32, i64),

    /// The engine is not reachable (network or process down).
    #[error("not reachable")]
    NotReachable,

    /// Write was rejected for another reason.
    #[error("rejected: {0}")]
    Rejected(String),
}

impl EngineWriteError {
    /// Returns `true` if retrying this write might succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            EngineWriteError::ChannelFull
                | EngineWriteError::Backpressure
                | EngineWriteError::NotReachable
        )
    }

    /// Returns `true` if this error is a backpressure signal that should be
    /// surfaced to the client as gRPC `RESOURCE_EXHAUSTED` (retry the same
    /// batch) rather than silently dropped.
    pub fn is_backpressure(&self) -> bool {
        matches!(
            self,
            EngineWriteError::ChannelFull | EngineWriteError::Backpressure
        )
    }
}

/// 引擎写错误 → 统一错误收敛（表缺失/删除归 Schema 段，其余归 5xxxx 接入层段）。
impl From<EngineWriteError> for SenseArcError {
    fn from(e: EngineWriteError) -> Self {
        match e {
            EngineWriteError::ChannelFull => SenseArcError::WriteRejected("channel full".into()),
            EngineWriteError::Backpressure => {
                SenseArcError::WriteRejected("engine backpressure: memtable stalled".into())
            }
            EngineWriteError::TableDeleted(id) => SenseArcError::TableDeleted(id),
            EngineWriteError::TableNotFound(id) => SenseArcError::TableNotFound(id),
            EngineWriteError::PartitionFrozen(table, partition) => SenseArcError::WriteRejected(
                format!("partition frozen: table={}, partition={}", table, partition),
            ),
            EngineWriteError::NotReachable => SenseArcError::WriteRejected("not reachable".into()),
            EngineWriteError::Rejected(reason) => SenseArcError::WriteRejected(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_receipt_accepted() {
        let receipt = WriteReceipt::accepted(42, 100, 5);
        assert!(receipt.is_fully_accepted());
        assert_eq!(receipt.points_written, 100);
        assert_eq!(receipt.points_failed, 0);
    }

    #[test]
    fn test_write_receipt_buffered() {
        let receipt = WriteReceipt::buffered(7, 50, 200, 3);
        assert!(!receipt.is_fully_accepted());
        match &receipt.write_status {
            WriteStatus::Buffered {
                sf_seq,
                sf_backlog_count,
            } => {
                assert_eq!(*sf_seq, 7);
                assert_eq!(*sf_backlog_count, 200);
            }
            _ => panic!("expected Buffered"),
        }
    }

    #[test]
    fn test_engine_write_error_retryable() {
        assert!(EngineWriteError::ChannelFull.is_retryable());
        assert!(EngineWriteError::Backpressure.is_retryable());
        assert!(EngineWriteError::NotReachable.is_retryable());
        assert!(!EngineWriteError::TableNotFound(1).is_retryable());
        assert!(!EngineWriteError::TableDeleted(1).is_retryable());
    }

    #[test]
    fn test_engine_write_error_is_backpressure() {
        assert!(EngineWriteError::ChannelFull.is_backpressure());
        assert!(EngineWriteError::Backpressure.is_backpressure());
        assert!(!EngineWriteError::NotReachable.is_backpressure());
        assert!(!EngineWriteError::Rejected("x".into()).is_backpressure());
    }
}

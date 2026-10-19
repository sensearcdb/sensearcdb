//! Columnar PointBatch — the core data transfer format for ingestion.
//!
//! Aligned with the engine's `ColumnarMemTable` for efficient copy-free transfer.
//! Each batch contains SoA (Structure of Arrays) storage: timestamps, columns,
//! qualities, and flags.

use serde::{Deserialize, Serialize};

use sensearcdb_common::TenantId;

/// A batch of data points in columnar (SoA) format.
///
/// All columns MUST have the same row count as `timestamps.len()`.
/// Column access uses `col_id` as direct index into the `columns` Vec (O(1)).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointBatch {
    /// Target table identifier.
    pub table_id: u32,
    /// 租户 ID（0 = 默认租户；S1 全链路恒 0）。
    #[serde(default)]
    pub tenant_id: TenantId,
    /// SoA timestamps (epoch nanoseconds).
    pub timestamps: Vec<i64>,
    /// Column data: `col_id` is the index, O(1) access.
    pub columns: Vec<ColVec>,
    /// Column names for schema validation.
    pub column_names: Vec<String>,
    /// Logical column ids (one per entry in [`Self::columns`]), carried
    /// explicitly so the write path maps `tag = (table_id, column_id)` rather
    /// than by array position. This makes sparse column sets (e.g. `{0, 2}`
    /// after a dropped column) unambiguous.
    ///
    /// `#[serde(default)]` keeps store-and-forward buffers written before this
    /// field existed deserializable (missing ids degrade to the legacy dense
    /// positional layout).
    #[serde(default)]
    pub column_ids: Vec<u16>,
    /// Per-column per-row presence bitmap, parallel to [`Self::columns`]:
    /// `column_presence[col_idx][row_idx] == false` means that column's value at
    /// that row is null (absent). An empty inner vec means dense (all rows
    /// present); an empty outer vec (legacy buffers) means no sparse info at all.
    #[serde(default)]
    pub column_presence: Vec<Vec<bool>>,
    /// OPC UA QQSSSSLL quality codes (one per row).
    pub qualities: Vec<u8>,
    /// Point flags: bit0=INTERPOLATED, bit1=BACKFILL, bit2=GAP_MARKER.
    pub flags: Vec<u8>,
    /// Batch metadata.
    pub metadata: BatchMeta,
}

impl PointBatch {
    /// Create a new empty PointBatch for the given table.
    pub fn new(table_id: u32) -> Self {
        Self {
            table_id,
            tenant_id: TenantId::DEFAULT,
            timestamps: Vec::new(),
            columns: Vec::new(),
            column_names: Vec::new(),
            column_ids: Vec::new(),
            column_presence: Vec::new(),
            qualities: Vec::new(),
            flags: Vec::new(),
            metadata: BatchMeta::default(),
        }
    }

    /// Returns the number of rows in this batch.
    #[inline]
    pub fn row_count(&self) -> usize {
        self.timestamps.len()
    }

    /// Returns the number of columns in this batch.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns the logical column id for the column at `index`.
    ///
    /// Uses the explicit [`Self::column_ids`] when populated; otherwise falls
    /// back to the positional (dense) index to preserve legacy batch producers
    /// that never set `column_ids`.
    #[inline]
    pub fn column_id_at(&self, index: usize) -> u16 {
        self.column_ids.get(index).copied().unwrap_or(index as u16)
    }

    /// Returns `true` if this batch contains no rows.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    /// Returns the minimum timestamp in this batch, or `None` if empty.
    pub fn min_timestamp(&self) -> Option<i64> {
        self.timestamps.iter().copied().min()
    }

    /// Returns the maximum timestamp in this batch, or `None` if empty.
    pub fn max_timestamp(&self) -> Option<i64> {
        self.timestamps.iter().copied().max()
    }

    /// Validate that all columns have the expected row count.
    ///
    /// Returns `Ok(())` if consistent, or `Err` with the mismatched column index.
    pub fn validate_consistency(&self) -> Result<(), String> {
        let expected = self.timestamps.len();
        for (i, col) in self.columns.iter().enumerate() {
            if col.len() != expected {
                return Err(format!(
                    "column {} ({}) has {} rows, expected {}",
                    i,
                    self.column_names.get(i).map(|s| s.as_str()).unwrap_or("?"),
                    col.len(),
                    expected
                ));
            }
        }
        if !self.column_ids.is_empty() && self.column_ids.len() != self.columns.len() {
            return Err(format!(
                "column_ids has {} entries, expected {}",
                self.column_ids.len(),
                self.columns.len()
            ));
        }
        if !self.column_presence.is_empty() && self.column_presence.len() != self.columns.len() {
            return Err(format!(
                "column_presence has {} entries, expected {}",
                self.column_presence.len(),
                self.columns.len()
            ));
        }
        if self.qualities.len() != expected {
            return Err(format!(
                "qualities has {} entries, expected {}",
                self.qualities.len(),
                expected
            ));
        }
        if self.flags.len() != expected {
            return Err(format!(
                "flags has {} entries, expected {}",
                self.flags.len(),
                expected
            ));
        }
        Ok(())
    }

    /// Reserve capacity for `additional` more rows in all vectors.
    pub fn reserve(&mut self, additional: usize) {
        self.timestamps.reserve(additional);
        self.qualities.reserve(additional);
        self.flags.reserve(additional);
        for col in &mut self.columns {
            col.reserve(additional);
        }
    }

    /// Estimate memory usage in bytes.
    pub fn estimated_bytes(&self) -> usize {
        let mut total = self.timestamps.capacity() * 8;
        total += self.qualities.capacity() * 1;
        total += self.flags.capacity() * 1;
        for col in &self.columns {
            total += col.estimated_bytes();
        }
        total += self
            .column_names
            .iter()
            .map(|n| n.capacity())
            .sum::<usize>();
        total += self.column_ids.capacity() * std::mem::size_of::<u16>();
        total
    }

    /// Clear all data, retaining allocated capacity.
    pub fn clear(&mut self) {
        self.timestamps.clear();
        for col in &mut self.columns {
            col.clear();
        }
        self.qualities.clear();
        self.flags.clear();
        self.column_ids.clear();
        self.column_presence.clear();
    }
}

/// A single column of typed data within a PointBatch.
///
/// Uses a tagged enum to store different physical types.
/// All variants use `Vec<T>` for SoA storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColVec {
    /// 64-bit floating point (IEEE 754 double).
    F64(Vec<f64>),
    /// 32-bit floating point (IEEE 754 single).
    F32(Vec<f32>),
    /// 64-bit signed integer.
    I64(Vec<i64>),
    /// 64-bit unsigned integer.
    U64(Vec<u64>),
    /// 32-bit signed integer.
    I32(Vec<i32>),
    /// Boolean values (stored as bytes, 1 per value).
    Bool(Vec<u8>),
    /// UTF-8 string values.
    String(Vec<String>),
}

impl ColVec {
    /// Returns the number of elements in this column.
    pub fn len(&self) -> usize {
        match self {
            ColVec::F64(v) => v.len(),
            ColVec::F32(v) => v.len(),
            ColVec::I64(v) => v.len(),
            ColVec::U64(v) => v.len(),
            ColVec::I32(v) => v.len(),
            ColVec::Bool(v) => v.len(),
            ColVec::String(v) => v.len(),
        }
    }

    /// Returns `true` if the column is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reserve capacity for `additional` more elements.
    pub fn reserve(&mut self, additional: usize) {
        match self {
            ColVec::F64(v) => v.reserve(additional),
            ColVec::F32(v) => v.reserve(additional),
            ColVec::I64(v) => v.reserve(additional),
            ColVec::U64(v) => v.reserve(additional),
            ColVec::I32(v) => v.reserve(additional),
            ColVec::Bool(v) => v.reserve(additional),
            ColVec::String(v) => v.reserve(additional),
        }
    }

    /// Clear all data, retaining allocated capacity.
    pub fn clear(&mut self) {
        match self {
            ColVec::F64(v) => v.clear(),
            ColVec::F32(v) => v.clear(),
            ColVec::I64(v) => v.clear(),
            ColVec::U64(v) => v.clear(),
            ColVec::I32(v) => v.clear(),
            ColVec::Bool(v) => v.clear(),
            ColVec::String(v) => v.clear(),
        }
    }

    /// Estimate memory usage in bytes.
    pub fn estimated_bytes(&self) -> usize {
        match self {
            ColVec::F64(v) => v.capacity() * 8,
            ColVec::F32(v) => v.capacity() * 4,
            ColVec::I64(v) => v.capacity() * 8,
            ColVec::U64(v) => v.capacity() * 8,
            ColVec::I32(v) => v.capacity() * 4,
            ColVec::Bool(v) => v.capacity() * 1,
            ColVec::String(v) => {
                v.iter().map(|s| s.capacity()).sum::<usize>()
                    + v.capacity() * std::mem::size_of::<String>()
            }
        }
    }

    /// Returns the data type tag corresponding to this ColVec variant.
    pub fn data_type_tag(&self) -> u8 {
        match self {
            ColVec::F64(_) => 0,
            ColVec::F32(_) => 1,
            ColVec::I64(_) => 2,
            ColVec::U64(_) => 3,
            ColVec::I32(_) => 4,
            ColVec::Bool(_) => 5,
            ColVec::String(_) => 6,
        }
    }

    /// Create a new empty ColVec for the given data type tag (0=F64, 1=F32, ...).
    pub fn new_for_tag(tag: u8) -> Self {
        match tag {
            0 => ColVec::F64(Vec::new()),
            1 => ColVec::F32(Vec::new()),
            2 => ColVec::I64(Vec::new()),
            3 => ColVec::U64(Vec::new()),
            4 => ColVec::I32(Vec::new()),
            5 => ColVec::Bool(Vec::new()),
            6 => ColVec::String(Vec::new()),
            _ => ColVec::F64(Vec::new()), // fallback
        }
    }
}

/// Metadata attached to each PointBatch.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchMeta {
    /// Source of this batch.
    pub source: BatchSource,
    /// Timestamp when the batch was collected (epoch nanoseconds).
    pub collected_at: i64,
}

/// Identifies the origin of a PointBatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BatchSource {
    /// Collected from a protocol driver.
    Collector {
        /// Driver identifier.
        driver_id: String,
        /// Group/subscription identifier within the driver.
        group_id: String,
    },
    /// Received via gRPC.
    Grpc {
        /// Client identifier.
        client_id: String,
    },
    /// Received via REST API.
    Rest {
        /// API key identifier used for authentication.
        api_key_id: String,
    },
}

impl Default for BatchSource {
    fn default() -> Self {
        BatchSource::Collector {
            driver_id: "unknown".into(),
            group_id: "default".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_batch_new() {
        let batch = PointBatch::new(1);
        assert_eq!(batch.table_id, 1);
        assert_eq!(batch.row_count(), 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_colvec_len() {
        let v = ColVec::F64(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.len(), 3);
        assert_eq!(v.data_type_tag(), 0);
    }

    #[test]
    fn test_point_batch_consistency_ok() {
        let mut batch = PointBatch::new(1);
        batch.timestamps = vec![100, 200, 300];
        batch.columns = vec![ColVec::F64(vec![1.1, 2.2, 3.3])];
        batch.column_names = vec!["value".into()];
        batch.qualities = vec![0xC0; 3];
        batch.flags = vec![0; 3];
        assert!(batch.validate_consistency().is_ok());
        assert_eq!(batch.min_timestamp(), Some(100));
        assert_eq!(batch.max_timestamp(), Some(300));
    }

    #[test]
    fn test_point_batch_consistency_mismatch() {
        let mut batch = PointBatch::new(1);
        batch.timestamps = vec![100, 200, 300];
        batch.columns = vec![ColVec::F64(vec![1.1, 2.2])]; // only 2 rows
        batch.column_names = vec!["value".into()];
        batch.qualities = vec![0xC0; 3];
        batch.flags = vec![0; 3];
        assert!(batch.validate_consistency().is_err());
    }

    #[test]
    fn test_point_batch_column_ids_sparse() {
        // Sparse column ids ({0, 2}) must be carried explicitly and map by id,
        // not by array position.
        let mut batch = PointBatch::new(1);
        batch.timestamps = vec![100];
        batch.columns = vec![ColVec::F64(vec![1.1]), ColVec::F64(vec![2.2])];
        batch.column_names = vec!["table_1.col_0".into(), "table_1.col_2".into()];
        batch.column_ids = vec![0, 2];
        batch.qualities = vec![0xC0; 1];
        batch.flags = vec![0; 1];

        assert!(batch.validate_consistency().is_ok());
        assert_eq!(batch.column_id_at(0), 0);
        assert_eq!(batch.column_id_at(1), 2);
    }

    #[test]
    fn test_point_batch_column_id_missing_degrades_to_position() {
        let batch = PointBatch::new(1);
        // No explicit column_ids → positional (dense) fallback.
        assert_eq!(batch.column_id_at(0), 0);
        assert_eq!(batch.column_id_at(3), 3);
    }

    #[test]
    fn test_point_batch_column_ids_length_mismatch() {
        let mut batch = PointBatch::new(1);
        batch.timestamps = vec![100];
        batch.columns = vec![ColVec::F64(vec![1.1]), ColVec::F64(vec![2.2])];
        batch.column_names = vec!["a".into(), "b".into()];
        batch.column_ids = vec![0]; // only one id for two columns
        batch.qualities = vec![0xC0; 1];
        batch.flags = vec![0; 1];
        assert!(batch.validate_consistency().is_err());
    }
}

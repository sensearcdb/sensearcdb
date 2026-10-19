//! SacRangeReader trait — 回放引擎的 SAC 数据读取抽象。
//!
//! 回放引擎通过此 trait 从 SAC 文件读取历史数据，避免对
//! `sensearcdb-engine` 的编译期交叉依赖。
//!
//! T05 集成时由 `main.rs` 注入 `SacReader` 的具体实现。

use std::sync::Arc;

use sensearcdb_common::{SenseArcError, TenantId, TypedValue};

/// SAC 文件中的单行数据（回放用）。
#[derive(Debug, Clone)]
pub struct SacDataRow {
    /// 租户 ID。
    pub tenant_id: TenantId,
    /// 时间戳 (epoch ns)。
    pub ts: i64,
    /// 值（首列 f64 视图，回放引擎消费）。
    pub value: f64,
    /// 质量码（首列质量码，回放引擎消费）。
    pub quality: u8,
    /// 服务器时间戳 (epoch ns)。
    pub server_ts: i64,
    /// 是否为派生标签。
    pub is_derived: bool,
    /// 多列值（column_id 升序，多类型直通透传）。
    pub values: Vec<(u16, TypedValue)>,
    /// 多列质量码（与 values 对齐）。
    pub qualities: Vec<(u16, u8)>,
}

/// SAC 范围读取器 trait。
///
/// 回放引擎通过此 trait 读取指定 tag 在指定时间范围内
/// 的历史数据，不依赖 `sensearcdb-engine` 的具体实现。
///
/// ## T04 默认实现
///
/// T04 阶段提供 `NoopSacRangeReader`（返回空结果），
/// T05 集成时由 `main.rs` 注入 `SacReader` 的具体实现。
pub trait SacRangeReader: Send + Sync {
    /// 读取指定 tag 在 [start_ts, end_ts] 范围内的所有数据点。
    ///
    /// 返回按时间戳升序排列的数据行。
    ///
    /// # Arguments
    ///
    /// - `tenant_id`: 租户 ID。
    /// - `table_id`: tag (table) id。
    /// - `start_ts`: 起始时间戳（含，epoch ns）。
    /// - `end_ts`: 结束时间戳（含，epoch ns）。
    fn read_range(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<SacDataRow>, SacReaderError>;
}

/// SAC 读取器错误。
#[derive(Debug, thiserror::Error)]
pub enum SacReaderError {
    #[error("tag not found: {0}")]
    TagNotFound(u32),
    #[error("SAC read error: {0}")]
    ReadError(String),
    #[error("internal error: {0}")]
    Internal(String),
}

/// SAC 读取器错误 → 统一错误收敛（内部基础设施错误）。
impl From<SacReaderError> for SenseArcError {
    fn from(e: SacReaderError) -> Self {
        SenseArcError::Internal(e.to_string())
    }
}

/// No-op SAC 范围读取器（T04 默认实现）。
///
/// 所有查询返回空结果。T05 集成时替换为 `SacReader` 的真实实现。
#[derive(Debug, Clone)]
pub struct NoopSacRangeReader;

impl SacRangeReader for NoopSacRangeReader {
    fn read_range(
        &self,
        _tenant_id: TenantId,
        _table_id: u32,
        _start_ts: i64,
        _end_ts: i64,
    ) -> Result<Vec<SacDataRow>, SacReaderError> {
        tracing::debug!("NoopSacRangeReader: returning empty range");
        Ok(Vec::new())
    }
}

/// SAC 读取器包装（Arc trait object）。
pub type SacReaderRef = Arc<dyn SacRangeReader>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_reader() {
        let reader = NoopSacRangeReader;
        let rows = reader
            .read_range(sensearcdb_common::TenantId::DEFAULT, 1, 0, 1000)
            .unwrap();
        assert!(rows.is_empty());
    }
}

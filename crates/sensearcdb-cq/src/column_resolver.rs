//! CQ 辅助列 column_id 解析器。
//!
//! CQ 运行时（聚合读路径 / 调度写路径）需要知道 `_sum`/`_count`/`_min`/`_max`/
//! `_first_ts`/`_last_ts` 六类辅助列的真实 `column_id`。历史上这些 ID 由
//! `layout::aux_column_id` 现场按公式编造，与 schema 解耦，存在「编造 ID 与
//! schema 实际注册 ID 不一致」的隐患。
//!
//! 本模块统一抽象为 [`CqColumnResolver`]：
//! - [`FormulaColumnResolver`]：公式回退（无 schema 句柄时的兜底，保留旧行为）。
//! - [`SchemaColumnResolver`]：从 schema（`sensearcdb_schema::Catalog`）按
//!   CQ 目标表真实列名查回 `column_id`，不再现场编造。
//!
//! CQ crate 由此直接依赖 `sensearcdb-schema`（只读调用，不修改 schema crate）。

use std::sync::Arc;

use sensearcdb_common::{SenseArcError, TenantId};
use sensearcdb_schema::Catalog;

use crate::layout::{aux_column_id, CqAuxColumn, AUX_COLUMNS_PER_SOURCE};

/// CQ 辅助列 ID 解析器。
///
/// 给定 CQ 目标表与源列列表，返回按「源列下标 × 6 类辅助列」顺序展平的
/// `column_id` 列表（与 [`CqAuxColumn::ALL`] 顺序一致）。
pub trait CqColumnResolver: Send + Sync {
    /// 解析辅助列 ID 列表。
    ///
    /// # Arguments
    /// * `target_table_id` - CQ 目标表 ID（辅助列所在表）。
    /// * `source_columns` - 源 raw 表的数值列 ID 列表。
    ///
    /// # Returns
    /// 长度为 `source_columns.len() * AUX_COLUMNS_PER_SOURCE` 的 `column_id` 列表。
    fn resolve_aux_column_ids(
        &self,
        target_table_id: u32,
        source_columns: &[u16],
    ) -> Result<Vec<u16>, SenseArcError>;
}

/// 公式回退解析器：无 schema 句柄时保持旧布局公式（不新增行为，仅兜底）。
#[derive(Debug, Clone, Default)]
pub struct FormulaColumnResolver;

impl CqColumnResolver for FormulaColumnResolver {
    fn resolve_aux_column_ids(
        &self,
        _target_table_id: u32,
        source_columns: &[u16],
    ) -> Result<Vec<u16>, SenseArcError> {
        let mut ids = Vec::with_capacity(source_columns.len() * AUX_COLUMNS_PER_SOURCE);
        for (source_index, _) in source_columns.iter().enumerate() {
            for kind in CqAuxColumn::ALL {
                ids.push(aux_column_id(source_columns, source_index, kind));
            }
        }
        Ok(ids)
    }
}

/// schema 回退解析器：按 CQ 目标表真实列名查回 `column_id`。
pub struct SchemaColumnResolver {
    catalog: Arc<Catalog>,
}

impl SchemaColumnResolver {
    /// 用已存在的 `Catalog` 构造解析器。
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }
}

impl CqColumnResolver for SchemaColumnResolver {
    fn resolve_aux_column_ids(
        &self,
        target_table_id: u32,
        source_columns: &[u16],
    ) -> Result<Vec<u16>, SenseArcError> {
        let table = self
            .catalog
            .get_table(target_table_id)?
            .ok_or_else(|| SenseArcError::TableNotFound(target_table_id))?;

        let mut ids = Vec::with_capacity(source_columns.len() * AUX_COLUMNS_PER_SOURCE);
        for &src_id in source_columns {
            for kind in CqAuxColumn::ALL {
                let name = format!("value_{src_id}{}", kind.name());
                let col_id = table
                    .columns
                    .iter()
                    .find(|c| c.name == name && c.deleted_at.is_none())
                    .map(|c| c.column_id)
                    .ok_or_else(|| {
                        SenseArcError::CatalogError(format!(
                            "CQ aux column {name} not found in table {target_table_id}"
                        ))
                    })?;
                ids.push(col_id);
            }
        }
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::aux_column_type;
    use sensearcdb_common::{DataType, Durability, TimestampNanos};
    use sensearcdb_schema::{ColumnDef, SchemaManager, SchemaMirror, TableInfo, TableType};

    fn col(name: &str, dt: DataType) -> ColumnDef {
        col_with_id(name, dt, 0)
    }

    fn col_with_id(name: &str, dt: DataType, column_id: u16) -> ColumnDef {
        ColumnDef {
            column_id,
            name: name.to_string(),
            data_type: dt,
            precision: 0.0,
            nullable: true,
            unit: None,
            description: None,
            deadband: None,
            enum_values: None,
            deleted_at: None,
            range_low: 0.0,
            range_high: 0.0,
            instrument_range_low: None,
            instrument_range_high: None,
            display_digits: 2,
        }
    }

    fn cq_table_info(name: &str, cols: Vec<ColumnDef>) -> TableInfo {
        let now = TimestampNanos(1);
        TableInfo {
            table_id: 0,
            tenant_id: TenantId::DEFAULT,
            table_name: name.to_string(),
            schema_id: 0,
            schema_version: 1,
            table_group: None,
            table_type: TableType::Cq,
            durability: Durability::Group,
            description: None,
            enabled: true,
            columns: cols,
            deleted_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// P2-03：辅助列 ID 必须来自 schema 真实分配（`SchemaManager::add_column` 的
    /// max+1 分配），`SchemaColumnResolver` 读回的 ID 与 schema 一致（不再编造）。
    #[tokio::test]
    async fn test_schema_resolver_matches_schema_allocated_aux_ids() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("config.db");
        let catalog = Arc::new(Catalog::open(&db).unwrap());
        let mirror = Arc::new(SchemaMirror::new());
        let event_bus = Arc::new(sensearcdb_schema::SchemaEventBus::new(256));
        let mgr = SchemaManager::new(catalog.clone(), mirror, event_bus);

        // 仅先建 value 列（column_id 与源列一致），辅助列全部交给 schema 分配。
        let created = mgr
            .create_table(cq_table_info(
                "cq_10s_1",
                vec![
                    col_with_id("value_0", DataType::F64, 0),
                    col_with_id("value_1", DataType::U64, 1),
                ],
            ))
            .await
            .unwrap();

        let source_columns = [0u16, 1u16];
        let source_types = [DataType::F64, DataType::U64];
        let mut allocated = Vec::new();

        // 逐列走 SchemaManager::add_column，记录 schema 真实分配的 column_id。
        for (i, &src_id) in source_columns.iter().enumerate() {
            for kind in CqAuxColumn::ALL {
                let added = mgr
                    .add_column(
                        created.table_id,
                        col(
                            &format!("value_{src_id}{}", kind.name()),
                            aux_column_type(kind, source_types[i]),
                        ),
                    )
                    .await
                    .unwrap();
                allocated.push(added.column_id);
            }
        }

        let resolver = SchemaColumnResolver::new(catalog);
        let resolved = resolver
            .resolve_aux_column_ids(created.table_id, &source_columns)
            .unwrap();

        assert_eq!(
            resolved, allocated,
            "解析出的辅助列 ID 必须与 schema 分配一致"
        );
        assert_eq!(resolved.len(), 12);
        // 确认不是 0（确实拿到了 schema 分配的真实 ID）。
        assert!(resolved.iter().all(|id| *id != 0));
    }

    /// 公式回退解析器保持旧布局（与 layout::aux_column_id 一致）。
    #[test]
    fn test_formula_resolver_matches_layout() {
        let resolver = FormulaColumnResolver;
        let ids = resolver.resolve_aux_column_ids(999, &[0, 1]).unwrap();
        let mut expected = Vec::new();
        for i in 0..2usize {
            for kind in CqAuxColumn::ALL {
                expected.push(aux_column_id(&[0, 1], i, kind));
            }
        }
        assert_eq!(ids, expected);
    }
}

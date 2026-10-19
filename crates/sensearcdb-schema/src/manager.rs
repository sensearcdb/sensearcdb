//! SchemaManager: DDL 操作入口。
//!
//! 编排 Catalog (持久化) → SchemaMirror (内存缓存) → SchemaEventBus (广播) 三方。
//! 所有 DDL 操作均通过此模块执行，确保三者状态一致。

use crate::catalog::Catalog;
use crate::event::{SchemaEvent, SchemaEventBus};
use crate::mirror::{ColumnDef, SchemaMirror, TableInfo, TableType};
use crate::schema_id::compute_schema_id;
use crate::validator::{normalize_table_name, validate_column_name, validate_table_name};
use sensearcdb_common::{Durability, SenseArcError, TenantId, TimestampNanos};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Schema DDL 操作管理器。
///
/// # 字段
/// - `catalog`: SQLite 持久化
/// - `mirror`: 内存缓存（热路径零拷贝读取）
/// - `event_bus`: DDL 事件广播（下游 engine/snapshot 热更新）
pub struct SchemaManager {
    pub catalog: Arc<Catalog>,
    pub mirror: Arc<SchemaMirror>,
    pub event_bus: Arc<SchemaEventBus>,
}

impl SchemaManager {
    /// 创建新的 SchemaManager。
    pub fn new(
        catalog: Arc<Catalog>,
        mirror: Arc<SchemaMirror>,
        event_bus: Arc<SchemaEventBus>,
    ) -> Self {
        Self {
            catalog,
            mirror,
            event_bus,
        }
    }

    /// 启动时全量加载：从 config.db 读取所有表 → 填充 SchemaMirror。
    ///
    /// 必须在任何 DDL 或写入操作之前调用。
    pub fn load_all(&self) -> Result<(), SenseArcError> {
        info!("loading all tables into SchemaMirror");
        let tables = self.catalog.list_tables()?;
        for table in tables {
            self.mirror.insert_table(table);
        }
        info!(
            table_count = self.mirror.table_count(),
            "SchemaMirror loaded"
        );
        Ok(())
    }

    // ─── CREATE TABLE ──────────────────────────────────────────────────────

    /// 创建新表。
    ///
    /// # 流程
    /// 1. 校验 table_name 和所有 column_name
    /// 2. 通过 Catalog 持久化到 SQLite（事务性）
    /// 3. 插入 SchemaMirror
    /// 4. 广播 `SchemaEvent::TableCreated`
    ///
    /// # Errors
    /// - `InvalidTableName` / `InvalidColumnName`: 名称不合法
    /// - `TableAlreadyExists`: 同名表已存在
    pub async fn create_table(&self, info: TableInfo) -> Result<TableInfo, SenseArcError> {
        self.create_table_inner(info, true).await
    }

    /// 静默创建新表（不广播 `TableCreated`）。
    ///
    /// 供「对账 / 派生表补齐」路径专用（P0-1）。`CqSchemaSynchronizer` 为 Raw 表补建
    /// 4 张 CQ 下采样表时，若走正常 [`create_table`](Self::create_table) 会再次广播
    /// `TableCreated(Cq)`，使「对账 → 广播 → 再对账」的自指环不断（livelock）。CQ 表是
    /// 派生表，无下游消费者订阅其 `TableCreated` 事件，静默建表既安全又切断自指环。
    ///
    /// 正常建表路径（REST/Raft DDL 建 Raw 表）仍走 [`create_table`](Self::create_table)
    /// 保持广播，满足不变式 I1。
    pub async fn create_table_silent(&self, info: TableInfo) -> Result<TableInfo, SenseArcError> {
        self.create_table_inner(info, false).await
    }

    /// 建表内部实现：校验 → 持久化 → 更新 mirror →（可选）广播事件。
    async fn create_table_inner(
        &self,
        info: TableInfo,
        emit_event: bool,
    ) -> Result<TableInfo, SenseArcError> {
        // 统一小写表名（PRD §4.2）
        let mut info = info;
        info.table_name = normalize_table_name(&info.table_name);

        // 校验名称
        validate_table_name(&info.table_name)?;
        for col in &info.columns {
            validate_column_name(&col.name)?;
        }

        // 持久化
        let created = self.catalog.create_table(&info)?;

        // 更新缓存
        self.mirror.insert_table(created.clone());

        // 广播事件（正常路径广播；对账/派生表静默，切断自指环）
        if emit_event {
            let table_info = Arc::new(created.clone());
            self.event_bus.broadcast(SchemaEvent::TableCreated {
                table_id: created.table_id,
                table_info,
            });
        }

        info!(
            table_id = created.table_id,
            table_name = %created.table_name,
            schema_id = created.schema_id,
            silent = !emit_event,
            "table created"
        );

        Ok(created)
    }

    // ─── DROP TABLE ───────────────────────────────────────────────────────

    /// 逻辑删除表（设置 `deleted_at`）。
    ///
    /// # 流程
    /// 1. 从 SchemaMirror 移除
    /// 2. Catalog 中标记 deleted_at
    /// 3. 广播 `SchemaEvent::TableDeleted`
    pub async fn drop_table(&self, table_id: u32) -> Result<(), SenseArcError> {
        // 检查表是否存在且未被删除
        let table = self
            .mirror
            .get_table(table_id)
            .ok_or(SenseArcError::TableNotFound(table_id))?;

        if table.deleted_at.is_some() {
            return Err(SenseArcError::TableDeleted(table_id));
        }

        let now = now_nanos();
        self.catalog.drop_table(table_id, now)?;
        self.mirror.remove_table(table_id);
        self.event_bus
            .broadcast(SchemaEvent::TableDeleted { table_id });

        info!(table_id, table_name = %table.table_name, "table dropped");
        Ok(())
    }

    // ─── ALTER TABLE ──────────────────────────────────────────────────────

    /// 修改表结构（替换所有列定义）。
    ///
    /// # 流程
    /// 1. 校验新列名
    /// 2. 读取当前状态获取 schema_version
    /// 3. Catalog 中增量更新（旧列标记 deleted_at，插入新列）
    /// 4. SchemaMirror 缓存更新
    /// 5. 广播 `SchemaEvent::TableAltered`
    pub async fn alter_table(
        &self,
        table_id: u32,
        new_columns: Vec<ColumnDef>,
    ) -> Result<TableInfo, SenseArcError> {
        let old = self
            .mirror
            .get_table(table_id)
            .ok_or(SenseArcError::TableNotFound(table_id))?;

        if old.deleted_at.is_some() {
            return Err(SenseArcError::TableDeleted(table_id));
        }

        // 校验新列名
        for col in &new_columns {
            validate_column_name(&col.name)?;
        }

        let new_version = old.schema_version + 1;
        let new_schema_id = compute_schema_id(&new_columns);
        let now = now_nanos();

        let updated = self
            .catalog
            .alter_table(table_id, new_columns, new_version, now)?;

        self.mirror.update_table(updated.clone());
        self.event_bus.broadcast(SchemaEvent::TableAltered {
            table_id,
            new_version,
            new_schema_id,
        });

        info!(
            table_id,
            table_name = %updated.table_name,
            new_version,
            new_schema_id,
            "table altered"
        );

        Ok(updated)
    }

    // ─── ADD COLUMN ───────────────────────────────────────────────────────

    /// 向已有表添加新列。
    ///
    /// 新列的 `column_id` 为当前最大 column_id + 1。
    pub async fn add_column(
        &self,
        table_id: u32,
        col: ColumnDef,
    ) -> Result<ColumnDef, SenseArcError> {
        let old = self
            .mirror
            .get_table(table_id)
            .ok_or(SenseArcError::TableNotFound(table_id))?;

        if old.deleted_at.is_some() {
            return Err(SenseArcError::TableDeleted(table_id));
        }

        validate_column_name(&col.name)?;

        // 计算新 column_id
        let new_column_id = old
            .columns
            .iter()
            .map(|c| c.column_id)
            .max()
            .map(|id| id + 1)
            .unwrap_or(0u16);

        let mut new_col = col;
        new_col.column_id = new_column_id;

        // 更新缓存：用新列列表 rebuild TableInfo（alter_table 负责持久化）
        let mut new_columns: Vec<ColumnDef> = old
            .columns
            .iter()
            .filter(|c| c.deleted_at.is_none())
            .cloned()
            .collect();
        new_columns.push(new_col.clone());
        new_columns.sort_by_key(|c| c.column_id);

        let new_schema_id = compute_schema_id(&new_columns);
        let new_version = old.schema_version + 1;
        let now = now_nanos();

        // 通过 alter_table 更新 schema_version 和 schema_id
        let _updated = self
            .catalog
            .alter_table(table_id, new_columns.clone(), new_version, now)?;

        let mut updated_info = (*old).clone();
        updated_info.columns = new_columns;
        updated_info.schema_id = new_schema_id;
        updated_info.schema_version = new_version;
        updated_info.updated_at = now;

        self.mirror.update_table(updated_info);
        self.event_bus.broadcast(SchemaEvent::TableAltered {
            table_id,
            new_version,
            new_schema_id,
        });

        debug!(table_id, column_name = %new_col.name, "column added");

        Ok(new_col)
    }

    /// 按列名幂等加列：列名已存在则返回已有 `column_id`，否则 max+1 新增并落库。
    ///
    /// 宽表语义：先建设备表（实体），再按测点名动态加列。同名列重复调用
    /// 返回同一 `column_id`（幂等收敛）；列不存在时委托 [`add_column`](Self::add_column)
    /// 按「max+1 永不回收」分配新列并落库、刷新 mirror、广播
    /// `SchemaEvent::TableAltered`。
    ///
    /// # Returns
    /// 该列（已存在或新建）的 `column_id`。
    pub async fn get_or_add_column(
        &self,
        table_id: u32,
        column: ColumnDef,
    ) -> Result<u16, SenseArcError> {
        let old = self
            .mirror
            .get_table(table_id)
            .ok_or(SenseArcError::TableNotFound(table_id))?;

        if old.deleted_at.is_some() {
            return Err(SenseArcError::TableDeleted(table_id));
        }

        validate_column_name(&column.name)?;

        // 幂等：同名列（未删除）已存在 → 直接返回其 column_id。
        if let Some(existing) = old
            .columns
            .iter()
            .find(|c| c.name == column.name && c.deleted_at.is_none())
        {
            debug!(
                table_id,
                column_name = %column.name,
                column_id = existing.column_id,
                "column already exists (idempotent)"
            );
            return Ok(existing.column_id);
        }

        // 否则走既有 add_column（max+1 分配 + 落库 + mirror 刷新 + 广播）。
        let added = self.add_column(table_id, column).await?;
        Ok(added.column_id)
    }

    // ─── DROP COLUMN ──────────────────────────────────────────────────────

    /// 逻辑删除列（保留列元数据，永不物理删除）。
    ///
    /// 对齐 schema PRD §4.4 / architecture-wave1 §7.7：
    /// 只标记 `deleted_at`，`column_id` 永不回收。
    pub async fn drop_column(&self, table_id: u32, column_name: &str) -> Result<(), SenseArcError> {
        let old = self
            .mirror
            .get_table(table_id)
            .ok_or(SenseArcError::TableNotFound(table_id))?;

        if old.deleted_at.is_some() {
            return Err(SenseArcError::TableDeleted(table_id));
        }

        // 验证列存在且未删除
        let col_idx = old
            .columns
            .iter()
            .position(|c| c.name == column_name && c.deleted_at.is_none())
            .ok_or_else(|| SenseArcError::ColumnNotFound(0))?;

        let now = now_nanos();

        // 1. Catalog 逻辑删除（UPDATE ... SET deleted_at）
        self.catalog.drop_column(table_id, column_name, now)?;

        // 2. 保留列元数据：仅标记 deleted_at，不物理移除
        let mut new_columns = old.columns.clone();
        new_columns[col_idx].deleted_at = Some(now);

        let new_version = old.schema_version + 1;
        let new_schema_id = compute_schema_id(
            &new_columns
                .iter()
                .filter(|c| c.deleted_at.is_none())
                .cloned()
                .collect::<Vec<_>>(),
        );

        let mut updated_info = (*old).clone();
        updated_info.columns = new_columns;
        updated_info.schema_id = new_schema_id;
        updated_info.schema_version = new_version;
        updated_info.updated_at = now;

        self.mirror.update_table(updated_info);
        self.event_bus.broadcast(SchemaEvent::TableAltered {
            table_id,
            new_version,
            new_schema_id,
        });

        debug!(table_id, column_name, "column logically dropped");
        Ok(())
    }

    // ─── ALTER COLUMN ─────────────────────────────────────────────────────

    /// 修改列的属性（重命名、改精度等）。
    ///
    /// # Arguments
    /// - `table_id`: 表 ID
    /// - `column_name`: 当前列名
    /// - `new_col`: 修改后的列定义（column_id 不变）
    pub async fn alter_column(
        &self,
        table_id: u32,
        column_name: &str,
        new_col: ColumnDef,
    ) -> Result<(), SenseArcError> {
        let old = self
            .mirror
            .get_table(table_id)
            .ok_or(SenseArcError::TableNotFound(table_id))?;

        if old.deleted_at.is_some() {
            return Err(SenseArcError::TableDeleted(table_id));
        }

        validate_column_name(&new_col.name)?;

        // 验证列存在
        let _col_idx = old
            .columns
            .iter()
            .position(|c| c.name == column_name && c.deleted_at.is_none())
            .ok_or_else(|| SenseArcError::ColumnNotFound(0))?;

        let now = now_nanos();

        // 重建列列表（替换目标列）
        let mut new_columns: Vec<ColumnDef> = old
            .columns
            .iter()
            .filter(|c| c.name != column_name)
            .cloned()
            .collect();
        new_columns.push(new_col.clone());
        new_columns.sort_by_key(|c| c.column_id);

        let new_schema_id = compute_schema_id(&new_columns);
        let new_version = old.schema_version + 1;

        // 通过 alter_table 持久化变更
        let _updated = self
            .catalog
            .alter_table(table_id, new_columns.clone(), new_version, now)?;

        let mut updated_info = (*old).clone();
        updated_info.columns = new_columns;
        updated_info.schema_id = new_schema_id;
        updated_info.schema_version = new_version;
        updated_info.updated_at = now;

        self.mirror.update_table(updated_info);
        self.event_bus.broadcast(SchemaEvent::TableAltered {
            table_id,
            new_version,
            new_schema_id,
        });

        debug!(table_id, column_name = %new_col.name, "column altered");
        Ok(())
    }

    // ─── GET OR CREATE TABLE (幂等) ───────────────────────────────────────

    /// 幂等地获取或创建表。
    ///
    /// 如果表已存在且列定义匹配 → 直接返回已有表。
    /// 如果表已存在但列定义不匹配 → 返回已有表（不修改）。
    /// 如果表不存在 → 创建新表。
    ///
    /// # 并发安全性
    /// 两个并发请求同时创建同名表时：
    /// 1. 先执行者成功创建
    /// 2. 后执行者遇到 UNIQUE 约束冲突 → 回退到查询已有表 → 返回已有表
    ///
    /// 语义上保证：对于同一组 `(name, columns)` 参数，始终返回同一个表。
    pub async fn get_or_create_table(
        &self,
        tenant_id: TenantId,
        name: &str,
        columns: Vec<ColumnDef>,
    ) -> Result<TableInfo, SenseArcError> {
        // 统一小写表名
        let name = normalize_table_name(name);

        // 快速路径：表已在缓存中
        if let Some(existing) = self.mirror.get_table_by_name(tenant_id, &name) {
            if existing.deleted_at.is_none() {
                debug!(table_name = %name, "get_or_create_table: found existing");
                return Ok((*existing).clone());
            }
        }

        // 慢速路径：可能表在 DB 中存在但未加载到缓存（理论上不应该）
        if let Some(db_table) = self.catalog.get_table_by_name(tenant_id, &name)? {
            if db_table.deleted_at.is_none() {
                self.mirror.insert_table(db_table.clone());
                return Ok(db_table);
            }
        }

        // 创建路径：表不存在（归属真实 tenant，而非硬编码默认租户）
        let info = TableInfo {
            table_id: 0,
            tenant_id,
            table_name: name.clone(),
            schema_id: 0,
            schema_version: 1,
            table_group: None,
            table_type: TableType::Raw,
            durability: Durability::Group,
            description: None,
            enabled: true,
            columns,
            deleted_at: None,
            created_at: now_nanos(),
            updated_at: now_nanos(),
        };

        match self.catalog.create_table(&info) {
            Ok(created) => {
                self.mirror.insert_table(created.clone());
                let table_info = Arc::new(created.clone());
                self.event_bus.broadcast(SchemaEvent::TableCreated {
                    table_id: created.table_id,
                    table_info,
                });
                info!(
                    table_id = created.table_id,
                    table_name = %created.table_name,
                    "get_or_create_table: created new"
                );
                Ok(created)
            }
            Err(SenseArcError::TableAlreadyExists(_)) => {
                // 并发冲突：另一个请求抢先创建了
                warn!(table_name = %name, "get_or_create_table: race detected, reading existing");
                let existing = self
                    .catalog
                    .get_table_by_name(tenant_id, &name)?
                    .ok_or_else(|| SenseArcError::Internal("table existed but not found".into()))?;
                self.mirror.insert_table(existing.clone());
                Ok(existing)
            }
            Err(e) => Err(e),
        }
    }

    /// 获取表信息。
    pub fn get_table(&self, table_id: u32) -> Option<Arc<TableInfo>> {
        self.mirror.get_table(table_id)
    }

    /// 按 (tenant_id, name) 获取表信息（租户内唯一）。
    pub fn get_table_by_name(&self, tenant_id: TenantId, name: &str) -> Option<Arc<TableInfo>> {
        self.mirror.get_table_by_name(tenant_id, name)
    }

    /// 列出所有未删除的表。
    pub fn list_tables(&self) -> Vec<TableInfo> {
        self.mirror
            .tables
            .iter()
            .filter(|entry| entry.value().deleted_at.is_none())
            .map(|entry| entry.value().as_ref().clone())
            .collect()
    }
}

/// 获取当前时间作为 TimestampNanos（内部辅助函数）。
fn now_nanos() -> TimestampNanos {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    TimestampNanos(nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::event::SchemaEventBus;
    use crate::mirror::{ColumnDef, SchemaMirror};
    use sensearcdb_common::DataType;

    fn setup() -> (Arc<Catalog>, Arc<SchemaMirror>, Arc<SchemaEventBus>) {
        let path = std::env::temp_dir().join(format!("test_mgr_{}.db", uuid::Uuid::new_v4()));
        let catalog = Arc::new(Catalog::open(&path).unwrap());
        let mirror = Arc::new(SchemaMirror::new());
        let event_bus = Arc::new(SchemaEventBus::new(256));
        // 注册清理（测试结束后删除）
        let _ = path; // 由 temp dir 管理
        (catalog, mirror, event_bus)
    }

    fn make_column(name: &str, dt: DataType) -> ColumnDef {
        ColumnDef {
            column_id: 0,
            name: name.to_string(),
            data_type: dt,
            precision: 0.0,
            nullable: false,
            unit: None,
            description: None,
            deadband: None,
            enum_values: None,
            deleted_at: None,
            range_low: 0.0,
            range_high: 100.0,
            instrument_range_low: None,
            instrument_range_high: None,
            display_digits: 2,
        }
    }

    fn make_table_info(name: &str) -> TableInfo {
        TableInfo {
            table_id: 0,
            tenant_id: TenantId::DEFAULT,
            table_name: name.to_string(),
            schema_id: 0,
            schema_version: 1,
            table_group: None,
            table_type: TableType::Raw,
            durability: Durability::Group,
            description: None,
            enabled: true,
            columns: vec![make_column("value", DataType::F64)],
            deleted_at: None,
            created_at: TimestampNanos(1000),
            updated_at: TimestampNanos(1000),
        }
    }

    #[tokio::test]
    async fn test_create_table_and_load() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = SchemaManager::new(catalog.clone(), mirror.clone(), event_bus.clone());

        let created = mgr
            .create_table(make_table_info("test_async"))
            .await
            .unwrap();
        assert_ne!(created.table_id, 0);

        let cached = mgr.get_table(created.table_id).unwrap();
        assert_eq!(cached.table_name, "test_async");
    }

    #[tokio::test]
    async fn test_drop_table() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = SchemaManager::new(catalog, mirror, event_bus);

        let created = mgr.create_table(make_table_info("to_drop")).await.unwrap();
        assert!(mgr.get_table(created.table_id).is_some());

        mgr.drop_table(created.table_id).await.unwrap();
        // drop 后 get_table 返回 None（被移除出缓存）
        assert!(mgr.get_table(created.table_id).is_none());
    }

    #[tokio::test]
    async fn test_get_or_create_table_idempotent() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = SchemaManager::new(catalog, mirror, event_bus);
        let cols = vec![make_column("value", DataType::F64)];

        let t1 = mgr
            .get_or_create_table(TenantId::DEFAULT, "idem_table", cols.clone())
            .await
            .unwrap();
        let t2 = mgr
            .get_or_create_table(TenantId::DEFAULT, "idem_table", cols.clone())
            .await
            .unwrap();

        assert_eq!(t1.table_id, t2.table_id);
        assert_eq!(t1.schema_id, t2.schema_id);
    }

    #[tokio::test]
    async fn test_get_or_create_table_cross_tenant_same_name() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = SchemaManager::new(catalog, mirror, event_bus);
        let cols = vec![make_column("value", DataType::F64)];

        let t1 = mgr
            .get_or_create_table(TenantId(1), "pump1", cols.clone())
            .await
            .unwrap();
        let t2 = mgr
            .get_or_create_table(TenantId(2), "pump1", cols.clone())
            .await
            .unwrap();

        assert_ne!(t1.table_id, t2.table_id);
        assert_eq!(t1.tenant_id, TenantId(1));
        assert_eq!(t2.tenant_id, TenantId(2));
        assert!(mgr.get_table_by_name(TenantId(1), "pump1").is_some());
        assert!(mgr.get_table_by_name(TenantId(2), "pump1").is_some());
    }

    #[tokio::test]
    async fn test_get_or_create_table_concurrent() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = Arc::new(SchemaManager::new(catalog, mirror, event_bus));
        let cols = vec![make_column("val", DataType::F64)];

        let mgr1 = mgr.clone();
        let mgr2 = mgr.clone();
        let cols1 = cols.clone();
        let cols2 = cols.clone();

        let (r1, r2) = tokio::join!(
            mgr1.get_or_create_table(TenantId::DEFAULT, "concurrent_table", cols1),
            mgr2.get_or_create_table(TenantId::DEFAULT, "concurrent_table", cols2),
        );

        let t1 = r1.unwrap();
        let t2 = r2.unwrap();

        // 两个并发请求应返回同一个 table_id
        assert_eq!(t1.table_id, t2.table_id);
    }

    #[tokio::test]
    async fn test_add_column() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = SchemaManager::new(catalog, mirror, event_bus);

        let created = mgr.create_table(make_table_info("col_test")).await.unwrap();
        let new_col = make_column("quality", DataType::U64);

        let added = mgr.add_column(created.table_id, new_col).await.unwrap();
        assert_eq!(added.name, "quality");
        assert_eq!(added.column_id, 1);

        let updated = mgr.get_table(created.table_id).unwrap();
        assert_eq!(updated.columns.len(), 2);
    }

    #[tokio::test]
    async fn test_get_or_add_column_idempotent() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = SchemaManager::new(catalog, mirror, event_bus);

        let created = mgr.create_table(make_table_info("idem_col")).await.unwrap();

        // 首次加列：max+1 分配为 1（表已有 column_id=0 的 value 列）。
        let id1 = mgr
            .get_or_add_column(created.table_id, make_column("temperature", DataType::F64))
            .await
            .unwrap();
        assert_eq!(id1, 1);

        // 重复加同名列：幂等返回同一 column_id，不新增。
        let id2 = mgr
            .get_or_add_column(created.table_id, make_column("temperature", DataType::F64))
            .await
            .unwrap();
        assert_eq!(id2, 1);

        let updated = mgr.get_table(created.table_id).unwrap();
        assert_eq!(updated.columns.len(), 2); // value + temperature
    }

    #[tokio::test]
    async fn test_load_all() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = SchemaManager::new(catalog.clone(), mirror.clone(), event_bus.clone());

        mgr.create_table(make_table_info("t1")).await.unwrap();
        mgr.create_table(make_table_info("t2")).await.unwrap();

        // 创建新的 mirror 并重新加载
        let mirror2 = Arc::new(SchemaMirror::new());
        let mgr2 = SchemaManager::new(catalog, mirror2, event_bus);
        mgr2.load_all().unwrap();

        let tables = mgr2.list_tables();
        assert_eq!(tables.len(), 2);
    }

    #[tokio::test]
    async fn test_invalid_table_name() {
        let (catalog, mirror, event_bus) = setup();
        let mgr = SchemaManager::new(catalog, mirror, event_bus);

        let info = make_table_info("123 bad"); // 空格非法
        let result = mgr.create_table(info).await;
        assert!(result.is_err());
    }
}

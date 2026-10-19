//! SchemaMirror：内存中的 Schema 缓存。
//!
//! 三组 DashMap 提供 O(1) 的按 table_id、按名称、按 schema_id 查找。
//! 所有价值类型为 `Arc<TableInfo>`，热路径零拷贝。
//!
//! # 并发语义
//! - 读路径：`get_table()` / `get_table_by_name()` 无锁（DashMap 读）
//! - 写路径：`insert_table()` 同时更新三组索引（原子性由 DashMap 各条目保证）
//! - `is_writable()` 返回 `Err` 当表不存在、已删除或禁用

use dashmap::DashMap;
use sensearcdb_common::{ColumnRef, DataType, Durability, SenseArcError, TenantId, TimestampNanos};
use std::sync::Arc;

// ─── SchemaMirror ──────────────────────────────────────────────────────────

/// 内存中的全量 Schema 缓存。
///
/// 三组索引：
/// - `tables`: table_id → Arc<TableInfo>
/// - `name_to_id`: table_name → table_id
/// - `schema_to_tables`: schema_id → Vec<table_id>（同构表聚类）
#[derive(Debug)]
pub struct SchemaMirror {
    /// table_id → TableInfo（主索引，热路径零拷贝返回 Arc）
    pub tables: DashMap<u32, Arc<TableInfo>>,
    /// (tenant_id, table_name) → table_id（租户内唯一解析）
    pub name_to_id: DashMap<(TenantId, String), u32>,
    /// schema_id → Vec<table_id>（同构表聚类）
    pub schema_to_tables: DashMap<u32, Vec<u32>>,
}

impl SchemaMirror {
    /// 创建空的 SchemaMirror。
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            name_to_id: DashMap::new(),
            schema_to_tables: DashMap::new(),
        }
    }

    /// 按 table_id 查询（O(1)，返回 Arc 引用）。
    pub fn get_table(&self, table_id: u32) -> Option<Arc<TableInfo>> {
        self.tables.get(&table_id).map(|r| Arc::clone(r.value()))
    }

    /// 按 (tenant_id, table_name) 查询（O(1)，name_to_id → tables 两步查找）。
    ///
    /// 键已租户化：跨租户同名表各自持有独立条目，互不覆盖。
    pub fn get_table_by_name(&self, tenant_id: TenantId, name: &str) -> Option<Arc<TableInfo>> {
        let table_id = self.name_to_id.get(&(tenant_id, name.to_string()))?;
        self.get_table(*table_id)
    }

    /// 按「表名 + 列名」解析列引用 `(table_id, column_id)`（只读查询，宽表列级）。
    ///
    /// 仅匹配未逻辑删除（`deleted_at.is_none()`）的列；表或列不存在返回 `None`。
    /// 表名可能含点号（路径式，见 [`crate::validator`]），列名恒不含点号，
    /// 因此调用方必须以**最后一个**点号拆分 `table.col` 后传入。
    pub fn resolve_column(
        &self,
        tenant_id: TenantId,
        table_name: &str,
        column_name: &str,
    ) -> Option<ColumnRef> {
        let table = self.get_table_by_name(tenant_id, table_name)?;
        let column_id = table
            .columns
            .iter()
            .find(|c| c.deleted_at.is_none() && c.name == column_name)
            .map(|c| c.column_id)?;
        Some(ColumnRef::new(table.table_id, column_id))
    }

    /// 查询同 schema_id 的所有表 ID 列表。
    pub fn get_tables_by_schema(&self, schema_id: u32) -> Vec<u32> {
        self.schema_to_tables
            .get(&schema_id)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    /// 检查表是否可写入。
    ///
    /// 返回 `Ok(())` 当且仅当：
    /// - 表存在
    /// - 未被逻辑删除 (`deleted_at.is_none()`)
    /// - 已启用 (`enabled == true`)
    ///
    /// # Errors
    /// - `TableNotFound`：表不存在
    /// - `TableDeleted`：表已逻辑删除
    /// - `Internal`：表被禁用
    pub fn is_writable(&self, table_id: u32) -> Result<(), SenseArcError> {
        let entry = self
            .tables
            .get(&table_id)
            .ok_or(SenseArcError::TableNotFound(table_id))?;

        let info = entry.value();
        if info.deleted_at.is_some() {
            return Err(SenseArcError::TableDeleted(table_id));
        }
        if !info.enabled {
            return Err(SenseArcError::Internal(format!(
                "table {} is disabled",
                info.table_name
            )));
        }
        Ok(())
    }

    /// 插入一张新表到三组索引。
    pub fn insert_table(&self, info: TableInfo) {
        let table_id = info.table_id;
        let name = info.table_name.clone();
        let schema_id = info.schema_id;

        // 更新 schema_to_tables 聚类
        self.schema_to_tables
            .entry(schema_id)
            .or_default()
            .push(table_id);

        // 更新 (tenant_id, name) → id 映射
        self.name_to_id.insert((info.tenant_id, name), table_id);

        // 更新主索引
        self.tables.insert(table_id, Arc::new(info));
    }

    /// 移除表（逻辑删除后或出错时使用）。
    ///
    /// 返回被移除的 `Arc<TableInfo>`（如果存在）。
    pub fn remove_table(&self, table_id: u32) -> Option<Arc<TableInfo>> {
        let info = self.tables.remove(&table_id)?;
        let removed = info.1;

        // 清理 (tenant_id, name) 索引
        self.name_to_id
            .remove(&(removed.tenant_id, removed.table_name.clone()));

        // 清理 schema_to_tables 聚类（移除该 table_id）
        if let Some(mut entry) = self.schema_to_tables.get_mut(&removed.schema_id) {
            entry.value_mut().retain(|id| *id != table_id);
        }

        Some(removed)
    }

    /// 原地更新表的缓存（ALTER TABLE 后使用）。
    ///
    /// 如果 name 变更或 schema_id 变更，同步更新其他两个索引。
    pub fn update_table(&self, new_info: TableInfo) {
        let table_id = new_info.table_id;

        // 获取旧条目以清理旧索引
        if let Some(old) = self.tables.get(&table_id) {
            let old_info = old.value();

            // 如果 (tenant_id, name) 变了，删除旧映射
            if old_info.table_name != new_info.table_name
                || old_info.tenant_id != new_info.tenant_id
            {
                self.name_to_id
                    .remove(&(old_info.tenant_id, old_info.table_name.clone()));
                self.name_to_id
                    .insert((new_info.tenant_id, new_info.table_name.clone()), table_id);
            }

            // 如果 schema_id 变了，从旧聚类移除并加入新聚类
            if old_info.schema_id != new_info.schema_id {
                if let Some(mut entry) = self.schema_to_tables.get_mut(&old_info.schema_id) {
                    entry.value_mut().retain(|id| *id != table_id);
                }
                self.schema_to_tables
                    .entry(new_info.schema_id)
                    .or_default()
                    .push(table_id);
            }
        } else {
            // 旧条目不存在，当作新插入
            self.name_to_id
                .insert((new_info.tenant_id, new_info.table_name.clone()), table_id);
            self.schema_to_tables
                .entry(new_info.schema_id)
                .or_default()
                .push(table_id);
        }

        self.tables.insert(table_id, Arc::new(new_info));
    }

    /// 返回缓存的表总数（含已删除）。
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

impl Default for SchemaMirror {
    fn default() -> Self {
        Self::new()
    }
}

// ─── TableInfo ─────────────────────────────────────────────────────────────

/// 表级元数据。
///
/// `column_id` 等于列在 `columns` 向量中的下标（0..N-1），永不回收。
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// SQLite AUTOINCREMENT 分配的表 ID。
    pub table_id: u32,
    /// 租户 ID（0 = 默认租户，`1..` = 真实租户；S4 Step 4 起落 `tables.tenant_id`）。
    pub tenant_id: TenantId,
    /// 表名（全局唯一）。
    pub table_name: String,
    /// 由列定义计算的 FNV-1a hash（同构表相同 schema_id）。
    pub schema_id: u32,
    /// Schema 版本号（每次 ALTER 递增）。
    pub schema_version: u32,
    /// 表分组（可选，用于组织管理）。
    pub table_group: Option<String>,
    /// 表类型。
    pub table_type: TableType,
    /// 表级 WAL 持久化强度（`tables.durability`，默认 `Group`）。
    pub durability: Durability,
    /// 描述信息。
    pub description: Option<String>,
    /// 是否启用。
    pub enabled: bool,
    /// 列定义列表（column_id = 下标）。
    pub columns: Vec<ColumnDef>,
    /// 逻辑删除时间（None = 未删除）。
    pub deleted_at: Option<TimestampNanos>,
    /// 创建时间。
    pub created_at: TimestampNanos,
    /// 最后修改时间。
    pub updated_at: TimestampNanos,
}

// ─── TableType ─────────────────────────────────────────────────────────────

/// 表类型枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    /// 原始数据表（采集器直接写入）。
    Raw,
    /// 派生标签表（公式引擎计算产出的中间结果）。
    Derived,
    /// 连续查询表（CQ 引擎产出的预聚合结果）。
    Cq,
    /// 流计算表（Stream 引擎产出的窗口聚合结果）。
    Stream,
}

impl TableType {
    /// 从字符串解析（大小写不敏感）。
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "RAW" => Some(Self::Raw),
            "DERIVED" => Some(Self::Derived),
            "CQ" => Some(Self::Cq),
            "STREAM" => Some(Self::Stream),
            _ => None,
        }
    }

    /// 转换为 SQLite 存储字符串。
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Raw => "RAW",
            Self::Derived => "DERIVED",
            Self::Cq => "CQ",
            Self::Stream => "STREAM",
        }
    }
}

// ─── ColumnDef ─────────────────────────────────────────────────────────────

/// 列级元数据。
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// 列 ID（0..N-1，永不回收）。
    pub column_id: u16,
    /// 列名。
    pub name: String,
    /// 物理数据类型。
    pub data_type: DataType,
    /// 数值精度（用于 schema_id 计算和显示）。
    pub precision: f64,
    /// 是否允许 NULL。
    pub nullable: bool,
    /// 工程单位（如 "MPa", "°C"）。
    pub unit: Option<String>,
    /// 列描述。
    pub description: Option<String>,
    /// 死区配置。
    pub deadband: Option<DeadbandConfig>,
    /// ENUM16 映射表。
    pub enum_values: Option<Vec<String>>,
    /// 逻辑删除时间。
    pub deleted_at: Option<TimestampNanos>,
    /// 量程下限。
    pub range_low: f64,
    /// 量程上限。
    pub range_high: f64,
    /// 仪表量程下限（传感器物理极限）。
    pub instrument_range_low: Option<f64>,
    /// 仪表量程上限。
    pub instrument_range_high: Option<f64>,
    /// 显示精度（小数位数），默认 2。
    pub display_digits: u16,
}

// ─── DeadbandConfig ───────────────────────────────────────────────────────

/// 死区（Deadband）配置，用于抑制微小变化的通知。
#[derive(Debug, Clone)]
pub struct DeadbandConfig {
    /// 死区类型。
    pub deadband_type: DeadbandType,
    /// 死区值（绝对值或百分比，取决于类型）。
    pub value: f64,
}

/// 死区类型枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadbandType {
    /// 无死区（所有变化都触发通知）。
    None,
    /// 绝对死区（值变化绝对值 > value 时触发）。
    Absolute,
    /// 百分比死区（值变化百分比 > value% 时触发）。
    Percent,
}

impl DeadbandType {
    /// 从字符串解析（大小写不敏感）。
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NONE" => Some(DeadbandType::None),
            "ABSOLUTE" => Some(DeadbandType::Absolute),
            "PERCENT" => Some(DeadbandType::Percent),
            _ => None,
        }
    }

    /// 转换为 SQLite 存储字符串。
    pub fn as_str(&self) -> &'static str {
        match self {
            DeadbandType::None => "NONE",
            DeadbandType::Absolute => "ABSOLUTE",
            DeadbandType::Percent => "PERCENT",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table_info(id: u32, name: &str, schema_id: u32) -> TableInfo {
        TableInfo {
            table_id: id,
            tenant_id: TenantId::DEFAULT,
            table_name: name.to_string(),
            schema_id,
            schema_version: 1,
            table_group: None,
            table_type: TableType::Raw,
            durability: Durability::Group,
            description: None,
            enabled: true,
            columns: vec![],
            deleted_at: None,
            created_at: TimestampNanos(0),
            updated_at: TimestampNanos(0),
        }
    }

    #[test]
    fn test_insert_and_get() {
        let mirror = SchemaMirror::new();
        mirror.insert_table(make_table_info(1, "test_table", 100));

        let info = mirror.get_table(1).unwrap();
        assert_eq!(info.table_name, "test_table");
        assert_eq!(info.schema_id, 100);
    }

    #[test]
    fn test_get_by_name() {
        let mirror = SchemaMirror::new();
        mirror.insert_table(make_table_info(42, "boiler_temp", 200));

        let info = mirror
            .get_table_by_name(TenantId::DEFAULT, "boiler_temp")
            .unwrap();
        assert_eq!(info.table_id, 42);
    }

    /// 多租户：跨租户同名表同时入 mirror，按 (tenant, name) 各查各的，互不覆盖。
    #[test]
    fn test_cross_tenant_same_name_not_overridden() {
        let mirror = SchemaMirror::new();
        let mut a = make_table_info(10, "pump1", 100);
        a.tenant_id = TenantId(1);
        let mut b = make_table_info(20, "pump1", 200);
        b.tenant_id = TenantId(2);
        mirror.insert_table(a);
        mirror.insert_table(b);

        assert_eq!(mirror.table_count(), 2);
        assert_eq!(
            mirror
                .get_table_by_name(TenantId(1), "pump1")
                .unwrap()
                .table_id,
            10
        );
        assert_eq!(
            mirror
                .get_table_by_name(TenantId(2), "pump1")
                .unwrap()
                .table_id,
            20
        );
        assert!(mirror.get_table_by_name(TenantId(3), "pump1").is_none());
    }

    #[test]
    fn test_resolve_column() {
        let mirror = SchemaMirror::new();
        let mut info = make_table_info(7, "device_01", 100);
        info.columns = vec![
            ColumnDef {
                column_id: 0,
                name: "value".to_string(),
                data_type: DataType::F64,
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
            },
            ColumnDef {
                column_id: 3,
                name: "temperature".to_string(),
                data_type: DataType::F64,
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
            },
        ];
        mirror.insert_table(info);

        assert_eq!(
            mirror.resolve_column(TenantId::DEFAULT, "device_01", "temperature"),
            Some(ColumnRef::new(7, 3))
        );
        assert_eq!(
            mirror.resolve_column(TenantId::DEFAULT, "device_01", "missing"),
            None
        );
        assert_eq!(
            mirror.resolve_column(TenantId::DEFAULT, "missing", "value"),
            None
        );
    }

    #[test]
    fn test_get_by_schema() {
        let mirror = SchemaMirror::new();
        mirror.insert_table(make_table_info(1, "t1", 500));
        mirror.insert_table(make_table_info(2, "t2", 500));
        mirror.insert_table(make_table_info(3, "t3", 600));

        let ids = mirror.get_tables_by_schema(500);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));

        let ids = mirror.get_tables_by_schema(600);
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&3));
    }

    #[test]
    fn test_is_writable() {
        let mirror = SchemaMirror::new();
        mirror.insert_table(make_table_info(1, "t1", 100));
        assert!(mirror.is_writable(1).is_ok());
        assert!(mirror.is_writable(999).is_err());
    }

    #[test]
    fn test_is_writable_deleted() {
        let mirror = SchemaMirror::new();
        let mut info = make_table_info(1, "t1", 100);
        info.deleted_at = Some(TimestampNanos(1));
        mirror.insert_table(info);

        match mirror.is_writable(1) {
            Err(SenseArcError::TableDeleted(1)) => {}
            other => panic!("expected TableDeleted, got {:?}", other),
        }
    }

    #[test]
    fn test_is_writable_disabled() {
        let mirror = SchemaMirror::new();
        let mut info = make_table_info(1, "t1", 100);
        info.enabled = false;
        mirror.insert_table(info);

        match mirror.is_writable(1) {
            Err(SenseArcError::Internal(msg)) => {
                assert!(msg.contains("disabled"));
            }
            other => panic!("expected Internal(disabled), got {:?}", other),
        }
    }

    #[test]
    fn test_remove_table() {
        let mirror = SchemaMirror::new();
        mirror.insert_table(make_table_info(1, "t1", 100));
        assert_eq!(mirror.table_count(), 1);

        let removed = mirror.remove_table(1).unwrap();
        assert_eq!(removed.table_name, "t1");
        assert_eq!(mirror.table_count(), 0);
        assert!(mirror.get_table(1).is_none());
        assert!(mirror.get_table_by_name(TenantId::DEFAULT, "t1").is_none());
    }

    #[test]
    fn test_update_table_name_change() {
        let mirror = SchemaMirror::new();
        mirror.insert_table(make_table_info(1, "old_name", 100));

        let mut new_info = make_table_info(1, "new_name", 100);
        new_info.schema_version = 2;
        mirror.update_table(new_info);

        assert!(mirror
            .get_table_by_name(TenantId::DEFAULT, "old_name")
            .is_none());
        let info = mirror
            .get_table_by_name(TenantId::DEFAULT, "new_name")
            .unwrap();
        assert_eq!(info.schema_version, 2);
    }

    #[test]
    fn test_update_table_schema_change() {
        let mirror = SchemaMirror::new();
        mirror.insert_table(make_table_info(1, "t1", 100));

        let new_info = make_table_info(1, "t1", 200);
        mirror.update_table(new_info);

        assert_eq!(mirror.get_tables_by_schema(100).len(), 0);
        assert_eq!(mirror.get_tables_by_schema(200).len(), 1);
    }

    #[test]
    fn test_table_type_from_str() {
        assert_eq!(TableType::from_str("RAW"), Some(TableType::Raw));
        assert_eq!(TableType::from_str("derived"), Some(TableType::Derived));
        assert_eq!(TableType::from_str("CQ"), Some(TableType::Cq));
        assert_eq!(TableType::from_str("stream"), Some(TableType::Stream));
        assert_eq!(TableType::Stream.as_str(), "STREAM");
        assert_eq!(TableType::from_str("unknown"), None);
    }

    #[test]
    fn test_deadband_type_from_str() {
        assert_eq!(DeadbandType::from_str("NONE"), Some(DeadbandType::None));
        assert_eq!(
            DeadbandType::from_str("absolute"),
            Some(DeadbandType::Absolute)
        );
        assert_eq!(
            DeadbandType::from_str("PERCENT"),
            Some(DeadbandType::Percent)
        );
    }
}

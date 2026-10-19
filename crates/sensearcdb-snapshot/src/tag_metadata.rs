//! TagMetadataStore — 标签元数据缓存（量程/单位/精度）。
//!
//! ## 设计
//!
//! - 内部维护 `HashMap<(u32, u16), TagMetadata>` 缓存，键 = `(table_id, column_id)`。
//! - 惰性加载：首次查询 `(table_id, column_id)` 时从 SchemaMirror 构建 TagMetadata，构建后缓存。
//! - SchemaEventBus 集成：监听 DDL 事件（TableAltered/TableDeleted）触发缓存失效。
//! - 失效策略：惰性重建——只删除缓存条目，下次查询时自然触发重建。
//!
//! ## SchemaMirror 桥接
//!
//! T04 阶段使用 `SchemaMirrorRef` trait 作为抽象层，避免对 `sensearcdb-schema`
//! 的编译期交叉依赖。T05 集成时由 `main.rs` 注入具体实现。

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use sensearcdb_common::DataType;

/// SchemaMirror 抽象 trait——T04 阶段避免对 sensearcdb-schema 的编译期依赖。
///
/// T05 集成时由 `main.rs` 提供 `sensearcdb_schema::SchemaMirror` 的实现。
pub trait SchemaMirrorRef: Send + Sync {
    /// 根据 table_id 获取表信息。
    fn get_table_name(&self, table_id: u32) -> Option<String>;
    /// 获取表的描述信息。
    fn get_table_description(&self, table_id: u32) -> Option<String>;
    /// 获取指定列的量程下限。
    fn get_range_low(&self, table_id: u32, column_id: u16) -> f64;
    /// 获取指定列的量程上限。
    fn get_range_high(&self, table_id: u32, column_id: u16) -> f64;
    /// 获取指定列的仪表量程下限。
    fn get_instrument_range_low(&self, table_id: u32, column_id: u16) -> Option<f64>;
    /// 获取指定列的仪表量程上限。
    fn get_instrument_range_high(&self, table_id: u32, column_id: u16) -> Option<f64>;
    /// 获取指定列的单位。
    fn get_unit(&self, table_id: u32, column_id: u16) -> Option<String>;
    /// 获取指定列的显示精度。
    fn get_display_digits(&self, table_id: u32, column_id: u16) -> u8;
    /// 获取指定列的数据类型。
    fn get_data_type(&self, table_id: u32, column_id: u16) -> DataType;
    /// 获取表的启用状态。
    fn is_enabled(&self, table_id: u32) -> bool;
}

/// 标签元数据（某一测点列的元数据）。
///
/// 所有字段均来源于 SchemaMirror 的 ColumnDef/TableInfo 映射，
/// 不维护独立的数据副本。
#[derive(Debug, Clone)]
pub struct TagMetadata {
    /// 标签名称（表名）。
    pub tag_name: String,
    /// 所属列 ID（配合表名定位具体测点列）。
    pub column_id: u16,
    /// 标签描述。
    pub description: String,
    /// 工程单位。
    pub engineering_units: String,
    /// 量程下限。
    pub range_low: f64,
    /// 量程上限。
    pub range_high: f64,
    /// 仪表量程下限。
    pub instrument_range_low: Option<f64>,
    /// 仪表量程上限。
    pub instrument_range_high: Option<f64>,
    /// 显示精度（小数位数）。
    pub display_digits: u8,
    /// 数据类型。
    pub data_type: DataType,
    /// 是否启用。
    pub enabled: bool,
    /// 是否启用归档。
    pub archiving_enabled: bool,
}

/// 标签元数据存储。
///
/// 独立 `RwLock`，与 SnapshotStore 的 shard 锁分离，避免元数据查询阻塞数据读写。
pub struct TagMetadataStore {
    /// `(table_id, column_id)` → TagMetadata 缓存。
    metadata: RwLock<HashMap<(u32, u16), TagMetadata>>,

    /// SchemaMirror 引用——T04 阶段通过 trait 抽象注入。
    schema_mirror: Arc<dyn SchemaMirrorRef>,
}

impl TagMetadataStore {
    /// 创建元数据存储。
    pub fn new(schema_mirror: Arc<dyn SchemaMirrorRef>) -> Self {
        Self {
            metadata: RwLock::new(HashMap::new()),
            schema_mirror,
        }
    }

    /// 惰性加载：获取或构建指定 `(table_id, column_id)` 的 TagMetadata。
    ///
    /// 1. 先查缓存。
    /// 2. 缓存未命中 → 从 SchemaMirror 构建。
    /// 3. 构建后写入缓存。
    pub fn get_or_build(&self, table_id: u32, column_id: u16) -> Option<TagMetadata> {
        let key = (table_id, column_id);

        // 1. 先查缓存
        {
            let cache = self.metadata.read();
            if let Some(cached) = cache.get(&key) {
                return Some(cached.clone());
            }
        }

        // 2. 缓存未命中 → 从 SchemaMirror 构建
        let table_name = self.schema_mirror.get_table_name(table_id)?;
        let metadata = TagMetadata {
            tag_name: table_name,
            column_id,
            description: self
                .schema_mirror
                .get_table_description(table_id)
                .unwrap_or_default(),
            engineering_units: self
                .schema_mirror
                .get_unit(table_id, column_id)
                .unwrap_or_default(),
            range_low: self.schema_mirror.get_range_low(table_id, column_id),
            range_high: self.schema_mirror.get_range_high(table_id, column_id),
            instrument_range_low: self
                .schema_mirror
                .get_instrument_range_low(table_id, column_id),
            instrument_range_high: self
                .schema_mirror
                .get_instrument_range_high(table_id, column_id),
            display_digits: self.schema_mirror.get_display_digits(table_id, column_id),
            data_type: self.schema_mirror.get_data_type(table_id, column_id),
            enabled: self.schema_mirror.is_enabled(table_id),
            archiving_enabled: true, // 默认 true，T05 从 tables 表覆盖
        };

        self.metadata.write().insert(key, metadata.clone());
        Some(metadata)
    }

    /// 直接获取已缓存的元数据（不触发惰性加载）。
    pub fn get_cached(&self, table_id: u32, column_id: u16) -> Option<TagMetadata> {
        self.metadata.read().get(&(table_id, column_id)).cloned()
    }

    /// 批量获取元数据。
    pub fn batch_get(&self, column_refs: &[(u32, u16)]) -> Vec<((u32, u16), Option<TagMetadata>)> {
        column_refs
            .iter()
            .map(|&(tid, cid)| ((tid, cid), self.get_or_build(tid, cid)))
            .collect()
    }

    /// Schema DDL 事件触发的缓存失效（列级）。
    ///
    /// 失效策略：惰性重建——只删除缓存条目，不主动重建。
    /// 下次 `get_or_build()` 时自然触发重建。
    pub fn invalidate(&self, table_id: u32, column_id: u16) {
        self.metadata.write().remove(&(table_id, column_id));
        tracing::debug!(
            table_id = table_id,
            column_id = column_id,
            "tag metadata cache invalidated"
        );
    }

    /// 整表失效（TableDeleted / TableAltered 全列失效用）。
    pub fn invalidate_table(&self, table_id: u32) {
        self.metadata.write().retain(|&(tid, _), _| tid != table_id);
        tracing::debug!(
            table_id = table_id,
            "tag metadata cache invalidated (whole table)"
        );
    }

    /// 全量清空缓存（用于 SchemaEventBus Lagged 恢复）。
    pub fn invalidate_all(&self) {
        let count = self.metadata.read().len();
        self.metadata.write().clear();
        tracing::warn!(
            cleared_entries = count,
            "tag metadata cache fully invalidated"
        );
    }

    /// 返回缓存条目数。
    pub fn cache_size(&self) -> usize {
        self.metadata.read().len()
    }
}

impl std::fmt::Debug for TagMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TagMetadataStore")
            .field("cache_size", &self.cache_size())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock SchemaMirror for testing.
    #[derive(Clone)]
    pub(crate) struct MockSchemaMirror;

    impl SchemaMirrorRef for MockSchemaMirror {
        fn get_table_name(&self, table_id: u32) -> Option<String> {
            if table_id == 0 {
                None
            } else {
                Some(format!("tag_{}", table_id))
            }
        }
        fn get_table_description(&self, _table_id: u32) -> Option<String> {
            Some("test tag".into())
        }
        fn get_range_low(&self, _table_id: u32, _column_id: u16) -> f64 {
            0.0
        }
        fn get_range_high(&self, _table_id: u32, _column_id: u16) -> f64 {
            100.0
        }
        fn get_instrument_range_low(&self, _table_id: u32, _column_id: u16) -> Option<f64> {
            Some(0.0)
        }
        fn get_instrument_range_high(&self, _table_id: u32, _column_id: u16) -> Option<f64> {
            Some(120.0)
        }
        fn get_unit(&self, _table_id: u32, _column_id: u16) -> Option<String> {
            Some("°C".into())
        }
        fn get_display_digits(&self, _table_id: u32, _column_id: u16) -> u8 {
            2
        }
        fn get_data_type(&self, _table_id: u32, _column_id: u16) -> DataType {
            DataType::F64
        }
        fn is_enabled(&self, _table_id: u32) -> bool {
            true
        }
    }

    #[test]
    fn test_get_or_build() {
        let store = TagMetadataStore::new(Arc::new(MockSchemaMirror));
        let meta = store.get_or_build(1, 0);
        assert!(meta.is_some());
        let meta = meta.unwrap();
        assert_eq!(meta.tag_name, "tag_1");
        assert_eq!(meta.column_id, 0);
        assert_eq!(meta.engineering_units, "°C");
        assert_eq!(meta.range_low, 0.0);
        assert_eq!(meta.range_high, 100.0);
        assert_eq!(meta.display_digits, 2);
        assert_eq!(store.cache_size(), 1);
    }

    #[test]
    fn test_get_or_build_per_column_key() {
        let store = TagMetadataStore::new(Arc::new(MockSchemaMirror));
        let _ = store.get_or_build(1, 0);
        let _ = store.get_or_build(1, 3);
        let _ = store.get_or_build(2, 0);
        // 同一表不同列 / 不同表不同列 → 独立缓存条目。
        assert_eq!(store.cache_size(), 3);
        assert_eq!(store.get_cached(1, 0).unwrap().column_id, 0);
        assert_eq!(store.get_cached(1, 3).unwrap().column_id, 3);
        assert_eq!(store.get_cached(2, 0).unwrap().column_id, 0);
    }

    #[test]
    fn test_get_or_build_not_found() {
        let store = TagMetadataStore::new(Arc::new(MockSchemaMirror));
        let meta = store.get_or_build(0, 0);
        assert!(meta.is_none());
    }

    #[test]
    fn test_cache_hit() {
        let store = TagMetadataStore::new(Arc::new(MockSchemaMirror));
        // First call: cache miss → build
        let _ = store.get_or_build(1, 0);
        assert_eq!(store.cache_size(), 1);
        // Second call: cache hit
        let meta = store.get_cached(1, 0);
        assert!(meta.is_some());
    }

    #[test]
    fn test_invalidate() {
        let store = TagMetadataStore::new(Arc::new(MockSchemaMirror));
        let _ = store.get_or_build(1, 0);
        let _ = store.get_or_build(1, 1);
        assert_eq!(store.cache_size(), 2);
        store.invalidate(1, 0);
        assert_eq!(store.cache_size(), 1);
        assert!(store.get_cached(1, 0).is_none());
        assert!(store.get_cached(1, 1).is_some());
    }

    #[test]
    fn test_invalidate_table() {
        let store = TagMetadataStore::new(Arc::new(MockSchemaMirror));
        let _ = store.get_or_build(1, 0);
        let _ = store.get_or_build(1, 1);
        let _ = store.get_or_build(2, 0);
        assert_eq!(store.cache_size(), 3);
        store.invalidate_table(1);
        assert_eq!(store.cache_size(), 1);
        assert!(store.get_cached(1, 0).is_none());
        assert!(store.get_cached(1, 1).is_none());
        assert!(store.get_cached(2, 0).is_some());
    }

    #[test]
    fn test_invalidate_all() {
        let store = TagMetadataStore::new(Arc::new(MockSchemaMirror));
        let _ = store.get_or_build(1, 0);
        let _ = store.get_or_build(2, 0);
        assert_eq!(store.cache_size(), 2);
        store.invalidate_all();
        assert_eq!(store.cache_size(), 0);
    }

    fn cache_size(store: &TagMetadataStore) -> usize {
        store.cache_size()
    }
}

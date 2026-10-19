//! Derived 派生标签定义持久化存储。
//!
//! `DerivedTagStore` 将派生标签定义落盘到 config.db 的 `derived_tags` 表，
//! 使「tag_name → 表达式 → 依赖图」的定义态在进程重启后可从 SQLite 恢复
//! （修复「派生标签定义重启即丢」的功能完整性缺口）。
//!
//! ## 与 Stream / CQ 持久化共享模式
//!
//! - 同库：同一个 `config.db`（`cfg.paths.config_db_path`）。
//! - 同连接模式：WAL。
//! - 分表：`derived_tags` 与 `stream_tasks` / `cq_definitions` 各自独立表，
//!   互不侵入。
//!
//! ## 序列化约定（架构决策 D3/D4）
//!
//! - `expression` 存**原始字符串**（唯一权威定义），恢复时重新 Lex + Pratt parse。
//! - `fallback_policy_json` / `deadband_json` / `source_table_ids_json` 存 JSON。
//! - `source_table_ids` 使用可序列化 DTO [`SourceRefDef`]（`ColumnRef` 未派生 serde）。
//!
//! ## 复合唯一约束（D2）
//!
//! `PRIMARY KEY (tenant_id, tag_name)`，与内存 `DependencyGraph` 键
//! `(TenantId, String)` 及 `stream_tasks.UNIQUE(tenant_id, name)` 语义一致。

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};
use sensearcdb_common::{SenseArcError, TenantId};
use serde::{Deserialize, Serialize};

use crate::deadband::DeadbandConfig;
use crate::fallback::FallbackPolicy;

/// `derived_tags` 建表 DDL（幂等，直接建表、不写迁移）。
const DERIVED_TAGS_DDL: &str = "
CREATE TABLE IF NOT EXISTS derived_tags (
    tenant_id             INTEGER NOT NULL,
    tag_name              TEXT    NOT NULL,
    table_id              INTEGER NOT NULL,
    expression            TEXT    NOT NULL,
    fallback_policy_json  TEXT    NOT NULL,
    deadband_json         TEXT    NOT NULL,
    source_table_ids_json TEXT    NOT NULL,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, tag_name)
);
";

/// 跨表源列引用的可序列化 DTO（对应内存态的 `(String, ColumnRef)`）。
///
/// `ColumnRef` 未派生 serde，故持久化 `source_table_ids` 时用本结构承接；
/// 恢复时再转换回 `ColumnRef::new(table_id, column_id)`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRefDef {
    /// 源 tag 名（与 `source_tags` 对齐，如 `` `sensor_a` `` 或 `` `device.col` ``）。
    pub name: String,
    /// 源表 ID。
    pub table_id: u32,
    /// 源列 ID（N=1 单列时为 0）。
    pub column_id: u16,
}

/// `derived_tags` 一行对应的类型化定义记录（反序列化后的 DTO）。
///
/// 该记录是 store 与引擎之间的唯一交换载体：`register_tag` 构造后 `upsert`，
/// `restore_definitions` 经 `load_all` 取回后重建 `DerivedTagNode`。
#[derive(Debug, Clone, PartialEq)]
pub struct DerivedTagRecord {
    /// 归属租户（复合键组成之一，0=默认租户，1..=真实租户）。
    pub tenant_id: TenantId,
    /// 派生标签名（租户内唯一，复合键组成之一）。
    pub tag_name: String,
    /// 目标物理表 ID（派生结果写入目标，单列 `column_id=0`）。
    pub table_id: u32,
    /// 权威表达式原始字符串。
    pub expression: String,
    /// 回退策略。
    pub fallback_policy: FallbackPolicy,
    /// 死区配置。
    pub deadband: DeadbandConfig,
    /// 解析后的源列引用（兜底用，权威重建仍以 `expression` 为准）。
    pub source_table_ids: Vec<SourceRefDef>,
    /// 创建时间（epoch 毫秒）。
    pub created_at: i64,
    /// 最近更新时间（epoch 毫秒）。
    pub updated_at: i64,
}

impl DerivedTagRecord {
    /// 构造一条派生标签定义记录。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        tag_name: String,
        table_id: u32,
        expression: String,
        fallback_policy: FallbackPolicy,
        deadband: DeadbandConfig,
        source_table_ids: Vec<SourceRefDef>,
        created_at: i64,
        updated_at: i64,
    ) -> Self {
        Self {
            tenant_id,
            tag_name,
            table_id,
            expression,
            fallback_policy,
            deadband,
            source_table_ids,
            created_at,
            updated_at,
        }
    }
}

/// Derived 派生标签定义持久化存储。
///
/// 内部使用 `Mutex<Connection>` 保证线程安全；连接启用 WAL 模式。
pub struct DerivedTagStore {
    conn: Mutex<Connection>,
}

impl DerivedTagStore {
    /// 打开（或创建）config.db 中的 `derived_tags` 表。
    ///
    /// # Arguments
    /// - `db_path`: config.db 文件路径。
    ///
    /// 打开失败（含建表失败）返回错误，由调用方决定是否中止启动（D7：必选）。
    pub fn open(db_path: &Path) -> Result<Self, SenseArcError> {
        let conn = Connection::open(db_path).map_err(|e| {
            SenseArcError::CatalogError(format!("failed to open config.db for derived tags: {e}"))
        })?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| {
                SenseArcError::CatalogError(format!("failed to set WAL mode for derived tags: {e}"))
            })?;

        let store = Self {
            conn: Mutex::new(conn),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    /// 确保 `derived_tags` 表存在（幂等）。
    fn ensure_schema(&self) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("derived tag store lock error: {e}"))
        })?;
        conn.execute_batch(DERIVED_TAGS_DDL).map_err(|e| {
            SenseArcError::CatalogError(format!("failed to ensure derived_tags schema: {e}"))
        })?;
        Ok(())
    }

    /// upsert 一条派生标签定义（幂等，以 `(tenant_id, tag_name)` 复合主键为准）。
    ///
    /// 更新时 `created_at` 保持首次创建值不变（仅刷新 `updated_at`），
    /// 以维持「created_at 升序 ≈ 注册拓扑序」的恢复顺序约束。
    pub fn upsert(&self, record: &DerivedTagRecord) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("derived tag store lock error: {e}"))
        })?;

        let fallback_policy_json = serde_json::to_string(&record.fallback_policy)
            .map_err(|e| SenseArcError::CatalogError(format!("serialize fallback_policy: {e}")))?;
        let deadband_json = serde_json::to_string(&record.deadband)
            .map_err(|e| SenseArcError::CatalogError(format!("serialize deadband: {e}")))?;
        let source_table_ids_json = serde_json::to_string(&record.source_table_ids)
            .map_err(|e| SenseArcError::CatalogError(format!("serialize source_table_ids: {e}")))?;
        let now = now_millis();

        conn.execute(
            "INSERT INTO derived_tags (
                tenant_id, tag_name, table_id, expression,
                fallback_policy_json, deadband_json, source_table_ids_json,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(tenant_id, tag_name) DO UPDATE SET
                table_id = excluded.table_id,
                expression = excluded.expression,
                fallback_policy_json = excluded.fallback_policy_json,
                deadband_json = excluded.deadband_json,
                source_table_ids_json = excluded.source_table_ids_json,
                updated_at = excluded.updated_at",
            params![
                record.tenant_id.as_u32(),
                record.tag_name,
                record.table_id,
                record.expression,
                fallback_policy_json,
                deadband_json,
                source_table_ids_json,
                record.created_at,
                now,
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("upsert derived_tags error: {e}")))?;

        Ok(())
    }

    /// 删除指定租户下的派生标签定义（`unregister_tag` 时调用）。
    ///
    /// 删除不存在的行视为成功（0 行受影响），与 `remove_node` 的空操作语义一致。
    pub fn delete(&self, tenant_id: TenantId, tag_name: &str) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("derived tag store lock error: {e}"))
        })?;
        conn.execute(
            "DELETE FROM derived_tags WHERE tenant_id = ?1 AND tag_name = ?2",
            params![tenant_id.as_u32(), tag_name],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("delete derived_tags error: {e}")))?;
        Ok(())
    }

    /// 按 `(tenant_id, tag_name)` 单查一条派生标签定义。
    ///
    /// 不存在返回 `Ok(None)`（区别于 SQL 层错误的 `Err`）。供 `register_tag` 在
    /// `upsert` 之前判断「重注册 vs 新注册」，以及 `add_node` 失败回滚时恢复旧定义。
    ///
    /// 与 [`load_all`](Self::load_all) 一致：坏 JSON 行 skip + warn 并视为不存在
    /// （返回 `Ok(None)`），避免因历史脏数据阻断正常重注册自愈。
    pub fn get(
        &self,
        tenant_id: TenantId,
        tag_name: &str,
    ) -> Result<Option<DerivedTagRecord>, SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("derived tag store lock error: {e}"))
        })?;

        let sql = "SELECT tenant_id, tag_name, table_id, expression,
                          fallback_policy_json, deadband_json, source_table_ids_json,
                          created_at, updated_at
                   FROM derived_tags
                   WHERE tenant_id = ?1 AND tag_name = ?2";
        let mut stmt = conn.prepare(sql).map_err(|e| {
            SenseArcError::CatalogError(format!("derived_tags get prepare error: {e}"))
        })?;

        let mut rows = stmt
            .query_map(params![tenant_id.as_u32(), tag_name], |row| {
                Ok(DerivedTagRow {
                    tenant_id: row.get(0)?,
                    tag_name: row.get(1)?,
                    table_id: row.get(2)?,
                    expression: row.get(3)?,
                    fallback_policy_json: row.get(4)?,
                    deadband_json: row.get(5)?,
                    source_table_ids_json: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })
            .map_err(|e| {
                SenseArcError::CatalogError(format!("derived_tags get query error: {e}"))
            })?;

        let Some(row) = rows.next() else {
            return Ok(None);
        };
        let row = row
            .map_err(|e| SenseArcError::CatalogError(format!("derived_tags get row error: {e}")))?;

        match row.to_record() {
            Ok(record) => Ok(Some(record)),
            Err(e) => {
                tracing::warn!(
                    tenant_id = tenant_id.as_u32(),
                    tag_name = %tag_name,
                    error = %e,
                    "derived_tags: get hit a corrupt row, treating as absent"
                );
                Ok(None)
            }
        }
    }

    /// 加载全部派生标签定义（启动恢复），按 `created_at ASC, rowid ASC` 返回。
    ///
    /// `rowid` 作为同毫秒注册时的确定性次序（插入次序）兜底，保证
    /// 派生-套-派生的「源先于依赖」拓扑序在恢复时成立。
    pub fn load_all(&self) -> Result<Vec<DerivedTagRecord>, SenseArcError> {
        self.load_where(None)
    }

    /// 加载指定租户的全部派生标签定义，排序同 [`load_all`](Self::load_all)。
    pub fn load_for_tenant(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<DerivedTagRecord>, SenseArcError> {
        self.load_where(Some(tenant_id))
    }

    /// 内部加载实现：`where_tenant == None` 时加载全部，否则按租户过滤。
    ///
    /// 坏 JSON 行 skip + warn（不阻断整机启动，§5.2）；SQL 层错误仍原样返回。
    fn load_where(
        &self,
        where_tenant: Option<TenantId>,
    ) -> Result<Vec<DerivedTagRecord>, SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("derived tag store lock error: {e}"))
        })?;

        let sql = "SELECT tenant_id, tag_name, table_id, expression,
                          fallback_policy_json, deadband_json, source_table_ids_json,
                          created_at, updated_at
                   FROM derived_tags
                   WHERE (?1 IS NULL OR tenant_id = ?1)
                   ORDER BY created_at ASC, rowid ASC";
        let tenant_filter: Option<u32> = where_tenant.map(|t| t.as_u32());

        let mut stmt = conn.prepare(sql).map_err(|e| {
            SenseArcError::CatalogError(format!("derived_tags load prepare error: {e}"))
        })?;

        let rows = stmt
            .query_map(params![tenant_filter], |row| {
                Ok(DerivedTagRow {
                    tenant_id: row.get(0)?,
                    tag_name: row.get(1)?,
                    table_id: row.get(2)?,
                    expression: row.get(3)?,
                    fallback_policy_json: row.get(4)?,
                    deadband_json: row.get(5)?,
                    source_table_ids_json: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })
            .map_err(|e| {
                SenseArcError::CatalogError(format!("derived_tags load query error: {e}"))
            })?;

        let mut out = Vec::new();
        for row in rows {
            let row = match row {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error = %e, "derived_tags: skipping unreadable row");
                    continue;
                }
            };
            let tenant_id = row.tenant_id;
            let tag_name = row.tag_name.clone();
            match row.to_record() {
                Ok(record) => out.push(record),
                Err(e) => {
                    tracing::warn!(
                        tenant_id,
                        tag_name = %tag_name,
                        error = %e,
                        "derived_tags: skipping row with corrupt definition"
                    );
                }
            }
        }
        Ok(out)
    }
}

/// 内部行类型（反序列化前）。
struct DerivedTagRow {
    tenant_id: u32,
    tag_name: String,
    table_id: u32,
    expression: String,
    fallback_policy_json: String,
    deadband_json: String,
    source_table_ids_json: String,
    created_at: i64,
    updated_at: i64,
}

impl DerivedTagRow {
    /// 反序列化 JSON 字段为类型化记录。
    fn to_record(self) -> Result<DerivedTagRecord, SenseArcError> {
        let fallback_policy: FallbackPolicy = serde_json::from_str(&self.fallback_policy_json)
            .map_err(|e| {
                SenseArcError::CatalogError(format!("deserialize fallback_policy: {e}"))
            })?;
        let deadband: DeadbandConfig = serde_json::from_str(&self.deadband_json)
            .map_err(|e| SenseArcError::CatalogError(format!("deserialize deadband: {e}")))?;
        let source_table_ids: Vec<SourceRefDef> = serde_json::from_str(&self.source_table_ids_json)
            .map_err(|e| {
                SenseArcError::CatalogError(format!("deserialize source_table_ids: {e}"))
            })?;

        Ok(DerivedTagRecord {
            tenant_id: TenantId(self.tenant_id),
            tag_name: self.tag_name,
            table_id: self.table_id,
            expression: self.expression,
            fallback_policy,
            deadband,
            source_table_ids,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

/// 当前 epoch 毫秒时间戳（内部辅助，与 `created_at`/`updated_at` 单位一致）。
fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn temp_db() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("test_derived_tags_{}.db", uuid::Uuid::new_v4()))
    }

    fn sample_record(tenant: u32, name: &str, table_id: u32, created_at: i64) -> DerivedTagRecord {
        DerivedTagRecord::new(
            TenantId(tenant),
            name.to_string(),
            table_id,
            format!("`{}` * 2", "sensor_a"),
            FallbackPolicy::Lenient {
                last_good_max_age_secs: 300,
            },
            DeadbandConfig {
                abs: 0.1,
                rel: 0.01,
                max_time_secs: 60,
            },
            vec![SourceRefDef {
                name: "sensor_a".to_string(),
                table_id: 100,
                column_id: 0,
            }],
            created_at,
            created_at,
        )
    }

    #[test]
    fn test_upsert_and_load_roundtrip() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();

        store
            .upsert(&sample_record(0, "derived_a", 42, 1000))
            .unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        let d = &loaded[0];
        assert_eq!(d.tenant_id, TenantId::DEFAULT);
        assert_eq!(d.tag_name, "derived_a");
        assert_eq!(d.table_id, 42);
        assert_eq!(d.expression, "`sensor_a` * 2");
        assert!(d.fallback_policy.is_lenient());
        assert_eq!(d.deadband.abs, 0.1);
        assert_eq!(d.source_table_ids.len(), 1);
        assert_eq!(d.source_table_ids[0].name, "sensor_a");
        assert_eq!(d.source_table_ids[0].table_id, 100);
        assert_eq!(d.source_table_ids[0].column_id, 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_upsert_idempotent() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        let rec = sample_record(0, "dup", 42, 1000);
        store.upsert(&rec).unwrap();
        store.upsert(&rec).unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        // 更新不改 created_at
        assert_eq!(loaded[0].created_at, 1000);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_upsert_update_refreshes_updated_at_and_keeps_created_at() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        store.upsert(&sample_record(0, "d", 42, 1000)).unwrap();

        let mut updated = sample_record(0, "d", 43, 1000);
        updated.expression = "`sensor_b` + 1".into();
        store.upsert(&updated).unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].table_id, 43);
        assert_eq!(loaded[0].expression, "`sensor_b` + 1");
        assert_eq!(loaded[0].created_at, 1000); // 保持首次创建时间
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_delete() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        store
            .upsert(&sample_record(0, "to_delete", 1, 1000))
            .unwrap();
        store.delete(TenantId::DEFAULT, "to_delete").unwrap();
        assert!(store.load_all().unwrap().is_empty());
        // 删除不存在的行也成功（幂等）
        store.delete(TenantId::DEFAULT, "to_delete").unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_get_single() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        store.upsert(&sample_record(1, "a", 1, 1000)).unwrap();
        store.upsert(&sample_record(1, "b", 2, 2000)).unwrap();
        store.upsert(&sample_record(2, "a", 3, 3000)).unwrap();

        // 命中：返回类型化记录。
        let got = store.get(TenantId(1), "a").unwrap().unwrap();
        assert_eq!(got.tag_name, "a");
        assert_eq!(got.table_id, 1);

        // 跨租户不串：同名的 tenant 2 记录独立。
        let got = store.get(TenantId(2), "a").unwrap().unwrap();
        assert_eq!(got.table_id, 3);

        // 缺失 → Ok(None)（非 Err）。
        assert!(store.get(TenantId(1), "missing").unwrap().is_none());
        assert!(store.get(TenantId(99), "a").unwrap().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_cross_tenant_same_name_roundtrip() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();

        store.upsert(&sample_record(1, "dup", 101, 1000)).unwrap();
        store.upsert(&sample_record(2, "dup", 102, 2000)).unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 2);
        let tenants: std::collections::HashSet<u32> =
            loaded.iter().map(|t| t.tenant_id.as_u32()).collect();
        assert_eq!(tenants, [1u32, 2u32].into_iter().collect());

        // 同租户同名 upsert 幂等（不报错、仍 1 行）
        store.upsert(&sample_record(1, "dup", 103, 3000)).unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded
            .iter()
            .any(|d| d.tenant_id == TenantId(1) && d.table_id == 103));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_composite_pk_rejects_duplicate_insert() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        store.upsert(&sample_record(0, "dup", 42, 1000)).unwrap();

        // 直接 INSERT（非 upsert）同 (tenant_id, tag_name) → 复合主键报错。
        let conn = Connection::open(&path).unwrap();
        let res = conn.execute(
            "INSERT INTO derived_tags (
                tenant_id, tag_name, table_id, expression,
                fallback_policy_json, deadband_json, source_table_ids_json,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                0u32,
                "dup",
                99u32,
                "1",
                "\"Strict\"",
                "{\"abs\":0.0,\"rel\":0.0,\"max_time_secs\":0}",
                "[]",
                1000i64,
                1000i64,
            ],
        );
        assert!(
            res.is_err(),
            "复合主键 (tenant_id, tag_name) 应拒绝重复 INSERT"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_restart_recovery() {
        let path = temp_db();
        {
            let store = DerivedTagStore::open(&path).unwrap();
            store.upsert(&sample_record(0, "s1", 1, 1000)).unwrap();
            store.upsert(&sample_record(0, "s2", 2, 2000)).unwrap();
        }
        // 模拟重启：重新 open + load
        {
            let store = DerivedTagStore::open(&path).unwrap();
            let loaded = store.load_all().unwrap();
            assert_eq!(loaded.len(), 2);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_load_for_tenant_filters() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        store.upsert(&sample_record(1, "a", 1, 1000)).unwrap();
        store.upsert(&sample_record(2, "b", 2, 2000)).unwrap();
        store.upsert(&sample_record(1, "c", 3, 3000)).unwrap();

        let tenant1 = store.load_for_tenant(TenantId(1)).unwrap();
        assert_eq!(tenant1.len(), 2);
        assert!(tenant1.iter().all(|d| d.tenant_id == TenantId(1)));

        let tenant2 = store.load_for_tenant(TenantId(2)).unwrap();
        assert_eq!(tenant2.len(), 1);
        assert_eq!(tenant2[0].tag_name, "b");

        let empty = store.load_for_tenant(TenantId(99)).unwrap();
        assert!(empty.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_load_all_orders_by_created_at() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        // 故意乱序插入，验证 load_all 按 created_at 升序返回。
        store.upsert(&sample_record(0, "third", 3, 3000)).unwrap();
        store.upsert(&sample_record(0, "first", 1, 1000)).unwrap();
        store.upsert(&sample_record(0, "second", 2, 2000)).unwrap();

        let loaded = store.load_all().unwrap();
        let names: Vec<&str> = loaded.iter().map(|d| d.tag_name.as_str()).collect();
        assert_eq!(names, vec!["first", "second", "third"]);
        let _ = std::fs::remove_file(&path);
    }

    /// 坏 JSON 行 skip + 不致命：其余有效行照常恢复，load_all 不返回 Err。
    #[test]
    fn test_corrupt_json_row_skipped_not_fatal() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        store.upsert(&sample_record(0, "good", 1, 1000)).unwrap();

        // 直接插入一行 fallback_policy_json 损坏的数据。
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO derived_tags (
                tenant_id, tag_name, table_id, expression,
                fallback_policy_json, deadband_json, source_table_ids_json,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                0u32,
                "corrupt",
                2u32,
                "`sensor_a`",
                "{not-valid-json",
                "{\"abs\":0.0,\"rel\":0.0,\"max_time_secs\":0}",
                "[]",
                500i64,
                500i64,
            ],
        )
        .unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1, "坏行应被 skip，仅保留有效行");
        assert_eq!(loaded[0].tag_name, "good");

        let _ = std::fs::remove_file(&path);
    }

    /// 空表 load 返回空 Vec（不报错）。
    #[test]
    fn test_empty_table_load() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        assert!(store.load_all().unwrap().is_empty());
        assert!(store.load_for_tenant(TenantId::DEFAULT).unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
    }

    /// `FallbackPolicy::Coalesce` 的 HashMap defaults 序列化 roundtrip。
    #[test]
    fn test_fallback_coalesce_roundtrip() {
        let path = temp_db();
        let store = DerivedTagStore::open(&path).unwrap();
        let mut defaults = HashMap::new();
        defaults.insert("sensor_a".to_string(), 0.0);
        let mut rec = sample_record(0, "coalesce_tag", 42, 1000);
        rec.fallback_policy = FallbackPolicy::Coalesce { defaults };

        store.upsert(&rec).unwrap();
        let loaded = store.load_all().unwrap();
        match &loaded[0].fallback_policy {
            FallbackPolicy::Coalesce { defaults } => {
                assert_eq!(defaults.get("sensor_a"), Some(&0.0));
            }
            other => panic!("expected Coalesce, got {:?}", other),
        }
        let _ = std::fs::remove_file(&path);
    }
}

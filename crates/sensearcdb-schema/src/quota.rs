//! 租户配额模型 + 持久化（`TenantQuotaStore`）。
//!
//! 三张表（S4 Step 1 新增，幂等 DDL 见 [`crate::catalog`]）：
//! - `quota_templates`：配额模板（软硬限制 + 告警比例）。
//! - `tenant_quotas`：租户级配额覆盖（无行则回退模板）。
//! - `tenant_usage`：租户计量（测点数 / 存储 / 采集器 / 用户）。

use std::sync::Arc;

use rusqlite::{params, OptionalExtension};
use sensearcdb_common::{SenseArcError, TenantId};

use crate::catalog::Catalog;

/// 配额模板（对应 `quota_templates` 表）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QuotaTemplate {
    /// 模板 ID（主键，如 "DEFAULT" / "small" / "large"）。
    pub template_id: String,
    /// 模板展示名。
    pub name: String,
    /// 测点数上限（NULL = 不限）。
    pub points_limit: Option<i64>,
    /// 存储上限 GB（NULL = 不限）。
    pub storage_limit_gb: Option<i64>,
    /// 写入速率 points/s（NULL = 不限）。
    pub write_rate_limit: Option<i64>,
    /// 保留期天（NULL = 默认）。
    pub retention_days: Option<i64>,
    /// 采集器数上限。
    pub collector_limit: Option<i64>,
    /// 用户数上限。
    pub user_limit: Option<i64>,
    /// 并发查询/SSE 上限。
    pub concurrent_limit: Option<i64>,
    /// 软告警阈值比例（默认 0.8）。
    pub warn_ratio: f64,
    /// 创建时间（epoch ns）。
    pub created_at: i64,
    /// 更新时间（epoch ns）。
    pub updated_at: i64,
}

/// 租户配额（对应 `tenant_quotas` 表）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TenantQuota {
    /// 租户 ID。
    pub tenant_id: TenantId,
    /// 来源模板 ID（可选，仅用于追溯）。
    pub template_id: Option<String>,
    /// 测点数上限。
    pub points_limit: Option<i64>,
    /// 存储上限 GB。
    pub storage_limit_gb: Option<i64>,
    /// 写入速率 points/s。
    pub write_rate_limit: Option<i64>,
    /// 保留期天。
    pub retention_days: Option<i64>,
    /// 采集器数上限。
    pub collector_limit: Option<i64>,
    /// 用户数上限。
    pub user_limit: Option<i64>,
    /// 并发上限。
    pub concurrent_limit: Option<i64>,
    /// 软告警阈值比例。
    pub warn_ratio: f64,
    /// 更新时间（epoch ns）。
    pub updated_at: i64,
}

impl TenantQuota {
    /// 从模板展开为租户配额快照（不落库，仅内存解析）。
    pub fn from_template(tenant_id: TenantId, tpl: &QuotaTemplate) -> Self {
        Self {
            tenant_id,
            template_id: Some(tpl.template_id.clone()),
            points_limit: tpl.points_limit,
            storage_limit_gb: tpl.storage_limit_gb,
            write_rate_limit: tpl.write_rate_limit,
            retention_days: tpl.retention_days,
            collector_limit: tpl.collector_limit,
            user_limit: tpl.user_limit,
            concurrent_limit: tpl.concurrent_limit,
            warn_ratio: tpl.warn_ratio,
            updated_at: tpl.updated_at,
        }
    }
}

/// 租户计量（对应 `tenant_usage` 表）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TenantUsage {
    /// 租户 ID。
    pub tenant_id: TenantId,
    /// 当前测点数。
    pub point_count: i64,
    /// 当前存储字节数。
    pub storage_bytes: i64,
    /// 当前采集器数。
    pub collector_count: i64,
    /// 当前用户数。
    pub user_count: i64,
    /// 更新时间（epoch ns）。
    pub updated_at: i64,
}

/// 默认配额模板 ID（未显式指定时回退）。
pub const DEFAULT_TEMPLATE_ID: &str = "DEFAULT";

/// 租户配额存储层。
///
/// 依赖 `Catalog`（config.db）连接，提供配额模板 / 租户配额 / 计量三组 CRUD，
/// 以及「模板展开」的 `resolve_quota`。
pub struct TenantQuotaStore {
    catalog: Arc<Catalog>,
}

impl TenantQuotaStore {
    /// 创建配额存储层。
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    // ─── 配额模板 ──────────────────────────────────────────────────────────

    /// 插入或更新配额模板。
    pub fn upsert_template(&self, tpl: &QuotaTemplate) -> Result<(), SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.execute(
            "INSERT INTO quota_templates (
                 template_id, name, points_limit, storage_limit_gb, write_rate_limit,
                 retention_days, collector_limit, user_limit, concurrent_limit,
                 warn_ratio, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)
             ON CONFLICT(template_id) DO UPDATE SET
                 name = excluded.name,
                 points_limit = excluded.points_limit,
                 storage_limit_gb = excluded.storage_limit_gb,
                 write_rate_limit = excluded.write_rate_limit,
                 retention_days = excluded.retention_days,
                 collector_limit = excluded.collector_limit,
                 user_limit = excluded.user_limit,
                 concurrent_limit = excluded.concurrent_limit,
                 warn_ratio = excluded.warn_ratio,
                 updated_at = excluded.updated_at",
            params![
                tpl.template_id,
                tpl.name,
                tpl.points_limit,
                tpl.storage_limit_gb,
                tpl.write_rate_limit,
                tpl.retention_days,
                tpl.collector_limit,
                tpl.user_limit,
                tpl.concurrent_limit,
                tpl.warn_ratio,
                tpl.updated_at,
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("upsert quota_template: {}", e)))?;
        Ok(())
    }

    /// 读取配额模板。
    pub fn get_template(&self, template_id: &str) -> Result<Option<QuotaTemplate>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.query_row(
            "SELECT template_id, name, points_limit, storage_limit_gb, write_rate_limit,
                    retention_days, collector_limit, user_limit, concurrent_limit,
                    warn_ratio, created_at, updated_at
             FROM quota_templates WHERE template_id = ?1",
            params![template_id],
            row_to_template,
        )
        .optional()
        .map_err(|e| SenseArcError::CatalogError(format!("query quota_template: {}", e)))
    }

    // ─── 租户配额 ──────────────────────────────────────────────────────────

    /// 插入或更新租户配额覆盖。
    pub fn upsert_quota(&self, q: &TenantQuota) -> Result<(), SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.execute(
            "INSERT INTO tenant_quotas (
                 tenant_id, template_id, points_limit, storage_limit_gb, write_rate_limit,
                 retention_days, collector_limit, user_limit, concurrent_limit,
                 warn_ratio, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(tenant_id) DO UPDATE SET
                 template_id = excluded.template_id,
                 points_limit = excluded.points_limit,
                 storage_limit_gb = excluded.storage_limit_gb,
                 write_rate_limit = excluded.write_rate_limit,
                 retention_days = excluded.retention_days,
                 collector_limit = excluded.collector_limit,
                 user_limit = excluded.user_limit,
                 concurrent_limit = excluded.concurrent_limit,
                 warn_ratio = excluded.warn_ratio,
                 updated_at = excluded.updated_at",
            params![
                q.tenant_id.as_u32(),
                q.template_id,
                q.points_limit,
                q.storage_limit_gb,
                q.write_rate_limit,
                q.retention_days,
                q.collector_limit,
                q.user_limit,
                q.concurrent_limit,
                q.warn_ratio,
                q.updated_at,
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("upsert tenant_quota: {}", e)))?;
        Ok(())
    }

    /// 读取租户配额覆盖（无行返回 `None`）。
    pub fn get_quota(&self, tenant_id: TenantId) -> Result<Option<TenantQuota>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.query_row(
            "SELECT tenant_id, template_id, points_limit, storage_limit_gb, write_rate_limit,
                    retention_days, collector_limit, user_limit, concurrent_limit,
                    warn_ratio, updated_at
             FROM tenant_quotas WHERE tenant_id = ?1",
            params![tenant_id.as_u32()],
            row_to_tenant_quota,
        )
        .optional()
        .map_err(|e| SenseArcError::CatalogError(format!("query tenant_quota: {}", e)))
    }

    /// 删除租户配额覆盖。
    pub fn delete_quota(&self, tenant_id: TenantId) -> Result<(), SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.execute(
            "DELETE FROM tenant_quotas WHERE tenant_id = ?1",
            params![tenant_id.as_u32()],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("delete tenant_quota: {}", e)))?;
        Ok(())
    }

    /// 解析租户配额：显式 `tenant_quotas` 行 > `tenants.quota_template_id` 模板 > 默认模板。
    pub fn resolve_quota(&self, tenant_id: TenantId) -> Result<Option<TenantQuota>, SenseArcError> {
        // 1. 显式覆盖优先。
        if let Some(q) = self.get_quota(tenant_id)? {
            return Ok(Some(q));
        }

        // 2. 模板展开：tenants.quota_template_id（缺失回退 DEFAULT）。
        // 注意：连接锁在查询 tenants 后立即释放，避免调用 get_template 时重入死锁。
        let template_id: Option<String> = {
            let conn =
                self.catalog.conn().lock().map_err(|e| {
                    SenseArcError::CatalogError(format!("catalog lock error: {}", e))
                })?;
            conn.query_row(
                "SELECT quota_template_id FROM tenants WHERE tenant_id = ?1",
                params![tenant_id.as_u32()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| {
                SenseArcError::CatalogError(format!("query tenants.quota_template_id: {}", e))
            })?
            .flatten()
            .or_else(|| Some(DEFAULT_TEMPLATE_ID.to_string()))
        };

        if let Some(template_id) = template_id {
            if let Some(tpl) = self.get_template(&template_id)? {
                return Ok(Some(TenantQuota::from_template(tenant_id, &tpl)));
            }
        }
        Ok(None)
    }

    // ─── 计量 ──────────────────────────────────────────────────────────────

    /// 插入或更新租户计量。
    pub fn upsert_usage(&self, u: &TenantUsage) -> Result<(), SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.execute(
            "INSERT INTO tenant_usage (tenant_id, point_count, storage_bytes, collector_count, user_count, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(tenant_id) DO UPDATE SET
                 point_count = excluded.point_count,
                 storage_bytes = excluded.storage_bytes,
                 collector_count = excluded.collector_count,
                 user_count = excluded.user_count,
                 updated_at = excluded.updated_at",
            params![
                u.tenant_id.as_u32(),
                u.point_count,
                u.storage_bytes,
                u.collector_count,
                u.user_count,
                u.updated_at,
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("upsert tenant_usage: {}", e)))?;
        Ok(())
    }

    /// 读取租户计量。
    pub fn get_usage(&self, tenant_id: TenantId) -> Result<Option<TenantUsage>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.query_row(
            "SELECT tenant_id, point_count, storage_bytes, collector_count, user_count, updated_at
             FROM tenant_usage WHERE tenant_id = ?1",
            params![tenant_id.as_u32()],
            |row| {
                Ok(TenantUsage {
                    tenant_id: TenantId(row.get::<_, i64>(0)? as u32),
                    point_count: row.get(1)?,
                    storage_bytes: row.get(2)?,
                    collector_count: row.get(3)?,
                    user_count: row.get(4)?,
                    updated_at: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(|e| SenseArcError::CatalogError(format!("query tenant_usage: {}", e)))
    }
}

fn row_to_template(row: &rusqlite::Row<'_>) -> rusqlite::Result<QuotaTemplate> {
    Ok(QuotaTemplate {
        template_id: row.get(0)?,
        name: row.get(1)?,
        points_limit: row.get(2)?,
        storage_limit_gb: row.get(3)?,
        write_rate_limit: row.get(4)?,
        retention_days: row.get(5)?,
        collector_limit: row.get(6)?,
        user_limit: row.get(7)?,
        concurrent_limit: row.get(8)?,
        warn_ratio: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

fn row_to_tenant_quota(row: &rusqlite::Row<'_>) -> rusqlite::Result<TenantQuota> {
    Ok(TenantQuota {
        tenant_id: TenantId(row.get::<_, i64>(0)? as u32),
        template_id: row.get(1)?,
        points_limit: row.get(2)?,
        storage_limit_gb: row.get(3)?,
        write_rate_limit: row.get(4)?,
        retention_days: row.get(5)?,
        collector_limit: row.get(6)?,
        user_limit: row.get(7)?,
        concurrent_limit: row.get(8)?,
        warn_ratio: row.get(9)?,
        updated_at: row.get(10)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn current_ns() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    }

    fn open_store() -> (tempfile::TempDir, Arc<Catalog>, TenantQuotaStore) {
        let tmp = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(Catalog::open(&tmp.path().join("config.db")).unwrap());
        let store = TenantQuotaStore::new(catalog.clone());
        (tmp, catalog, store)
    }

    #[test]
    fn test_template_roundtrip() {
        let (_t, _c, store) = open_store();
        let now = current_ns();
        let tpl = QuotaTemplate {
            template_id: "small".to_string(),
            name: "Small".to_string(),
            points_limit: Some(1000),
            storage_limit_gb: Some(10),
            write_rate_limit: Some(500),
            retention_days: Some(30),
            collector_limit: Some(5),
            user_limit: Some(10),
            concurrent_limit: Some(4),
            warn_ratio: 0.8,
            created_at: now,
            updated_at: now,
        };
        store.upsert_template(&tpl).unwrap();
        let got = store.get_template("small").unwrap().unwrap();
        assert_eq!(got, tpl);
        assert!(store.get_template("missing").unwrap().is_none());
    }

    #[test]
    fn test_quota_roundtrip_and_delete() {
        let (_t, _c, store) = open_store();
        let q = TenantQuota {
            tenant_id: TenantId(3),
            template_id: Some("small".to_string()),
            points_limit: Some(500),
            storage_limit_gb: None,
            write_rate_limit: Some(100),
            retention_days: None,
            collector_limit: None,
            user_limit: None,
            concurrent_limit: None,
            warn_ratio: 0.9,
            updated_at: current_ns(),
        };
        store.upsert_quota(&q).unwrap();
        assert_eq!(store.get_quota(TenantId(3)).unwrap().unwrap(), q);
        store.delete_quota(TenantId(3)).unwrap();
        assert!(store.get_quota(TenantId(3)).unwrap().is_none());
    }

    #[test]
    fn test_usage_roundtrip() {
        let (_t, _c, store) = open_store();
        let u = TenantUsage {
            tenant_id: TenantId(4),
            point_count: 42,
            storage_bytes: 1024,
            collector_count: 3,
            user_count: 2,
            updated_at: current_ns(),
        };
        store.upsert_usage(&u).unwrap();
        assert_eq!(store.get_usage(TenantId(4)).unwrap().unwrap(), u);
    }

    #[test]
    fn test_resolve_quota_explicit_and_template() {
        let (_t, catalog, store) = open_store();
        let now = current_ns();

        // 默认模板。
        store
            .upsert_template(&QuotaTemplate {
                template_id: "DEFAULT".to_string(),
                name: "Default".to_string(),
                points_limit: Some(100),
                storage_limit_gb: None,
                write_rate_limit: None,
                retention_days: Some(7),
                collector_limit: None,
                user_limit: None,
                concurrent_limit: None,
                warn_ratio: 0.8,
                created_at: now,
                updated_at: now,
            })
            .unwrap();

        // 无 tenants 行时回退 DEFAULT 模板。
        let resolved = store.resolve_quota(TenantId(99)).unwrap().unwrap();
        assert_eq!(resolved.points_limit, Some(100));
        assert_eq!(resolved.template_id.as_deref(), Some("DEFAULT"));

        // 显式覆盖优先于模板。
        store
            .upsert_quota(&TenantQuota {
                tenant_id: TenantId(99),
                template_id: None,
                points_limit: Some(999),
                storage_limit_gb: None,
                write_rate_limit: None,
                retention_days: None,
                collector_limit: None,
                user_limit: None,
                concurrent_limit: None,
                warn_ratio: 0.7,
                updated_at: now,
            })
            .unwrap();
        let resolved = store.resolve_quota(TenantId(99)).unwrap().unwrap();
        assert_eq!(resolved.points_limit, Some(999));
        assert_eq!(resolved.template_id, None);

        // 有 tenants 行 quota_template_id 指定模板。
        let conn = catalog.conn().lock().unwrap();
        conn.execute(
            "INSERT INTO tenants (tenant_id, slug, display_name, status, quota_template_id, created_at, updated_at)
             VALUES (5, 't5', 'T5', 'ACTIVE', 'small', ?1, ?1)",
            params![now],
        )
        .unwrap();
        drop(conn);
        store
            .upsert_template(&QuotaTemplate {
                template_id: "small".to_string(),
                name: "Small".to_string(),
                points_limit: Some(50),
                storage_limit_gb: None,
                write_rate_limit: None,
                retention_days: None,
                collector_limit: None,
                user_limit: None,
                concurrent_limit: None,
                warn_ratio: 0.5,
                created_at: now,
                updated_at: now,
            })
            .unwrap();
        let resolved = store.resolve_quota(TenantId(5)).unwrap().unwrap();
        assert_eq!(resolved.points_limit, Some(50));
        assert_eq!(resolved.template_id.as_deref(), Some("small"));
    }
}

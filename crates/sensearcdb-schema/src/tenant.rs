//! 租户目录：Tenant 模型 + 默认租户 seed + 租户 CRUD / 两段式删除状态机。
//!
//! S4 Step 1：在 S1 地基（建表 + seed 默认租户）之上补齐控制面——
//! 全量 CRUD、`TenantStatus` 状态机、slug 两段式生命周期（Q3 已拍板），
//! 以及默认租户业务管理员 seed 的供给 trait（由 S6 接线 `AuthManager`）。

use std::sync::Arc;

use rusqlite::{params, Connection, OptionalExtension};
use sensearcdb_common::{SenseArcError, TenantId};
use tracing::info;

use crate::catalog::Catalog;
use crate::purge_certificate::DeletionRecord;

/// 租户状态枚举（两段式删除状态机，Q3 已拍板）。
///
/// ```text
/// Active ──suspend()────────────────▶ Suspended
/// Suspended ──reactivate()──────────▶ Active
/// Suspended ──delete(confirmed)─────▶ Deleting   # slug 保留、不可抢占
/// Deleting ──cancel_deletion()──────▶ Suspended  # 仅 now < purge_at
/// Deleting ──[sweeper: now>=purge_at]▶ Purged     # slug 置 NULL 释放（终态）
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantStatus {
    /// 正常服务中。
    Active,
    /// 已停用（禁止写入，可恢复）。
    Suspended,
    /// 冷静期（等待物理清除，slug 保留不可抢占，可撤销）。
    Deleting,
    /// 已物理清除（终态，slug 置 NULL 释放可回收）。
    Purged,
}

impl TenantStatus {
    /// 落库字符串（`tenants.status` 列值）。
    pub fn as_str(&self) -> &'static str {
        match self {
            TenantStatus::Active => "ACTIVE",
            TenantStatus::Suspended => "SUSPENDED",
            TenantStatus::Deleting => "DELETING",
            TenantStatus::Purged => "PURGED",
        }
    }

    /// 从落库字符串解析（大小写敏感；`DELETED` 为废弃值，读侧兼容映射为 `Purged`）。
    pub fn from_str(s: &str) -> Self {
        match s {
            "ACTIVE" => TenantStatus::Active,
            "SUSPENDED" => TenantStatus::Suspended,
            "DELETING" => TenantStatus::Deleting,
            "PURGED" | "DELETED" => TenantStatus::Purged,
            _ => TenantStatus::Active,
        }
    }
}

impl std::fmt::Display for TenantStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 租户记录（对应 `tenants` 表）。
#[derive(Debug, Clone)]
pub struct Tenant {
    /// 租户 ID：`0` = 默认租户 default；`1..` = 真实租户。
    pub tenant_id: TenantId,
    /// 唯一短标识（如 "default"）。`PURGED` 后置 NULL 释放可回收。
    pub slug: Option<String>,
    /// 展示名（如 "Default"）。
    pub display_name: String,
    /// 状态（列序列化字符串）。
    pub status: String,
    /// 配额模板 ID（可选）。
    pub quota_template_id: Option<String>,
    /// 初始管理员（可选）。
    pub initial_admin: Option<String>,
    /// 软删除时间（epoch ns，可选）。
    pub deleted_at: Option<i64>,
    /// 清理时间（epoch ns，可选）。
    pub purge_at: Option<i64>,
    /// 创建时间（epoch ns）。
    pub created_at: i64,
    /// 更新时间（epoch ns）。
    pub updated_at: i64,
}

impl Tenant {
    /// 便捷访问：状态枚举（由列字符串解析）。
    pub fn status_enum(&self) -> TenantStatus {
        TenantStatus::from_str(&self.status)
    }
}

/// 初始管理员供给器 trait（S6 用 `AuthManager::register_user + assign_role` 实现）。
///
/// 抽象化「检查是否存在归属租户的用户」与「供给租户管理员」两个动作，
/// 使 schema crate 不反向依赖 security crate。
pub trait InitialAdminProvisioner: Send + Sync {
    /// 是否存在归属 `tenant_id` 的任意用户。
    fn has_tenant_user(&self, tenant_id: TenantId) -> bool;

    /// 供给租户管理员（含随机密码生成 + 强制首登改密），返回一次性随机密码。
    fn provision_tenant_admin(
        &self,
        tenant_id: TenantId,
        username: &str,
    ) -> Result<String, SenseArcError>;
}

/// 租户管理器。
pub struct TenantManager {
    catalog: Arc<Catalog>,
    /// 两段式删除冷静期（天）。
    deletion_cooling_period_days: u32,
}

impl TenantManager {
    /// 创建租户管理器（冷静期默认 7 天，可用 [`with_cooling_period_days`](Self::with_cooling_period_days) 覆盖）。
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            deletion_cooling_period_days: 7,
        }
    }

    /// 覆盖冷静期（builder）。
    pub fn with_cooling_period_days(mut self, days: u32) -> Self {
        self.deletion_cooling_period_days = days;
        self
    }

    /// 设置冷静期（运行时，S6 从 `AppConfig.tenant.deletion_cooling_period_days` 下发）。
    pub fn set_cooling_period_days(&mut self, days: u32) {
        self.deletion_cooling_period_days = days;
    }

    /// 当前冷静期（天）。
    pub fn cooling_period_days(&self) -> u32 {
        self.deletion_cooling_period_days
    }

    /// 幂等 seed 默认租户：`tenant_id=0, slug="default", display_name="Default"`。
    ///
    /// 若 `tenants` 表已存在 `tenant_id=0` 则不重复插入。
    pub fn ensure_default_tenant(&self) -> Result<(), SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let exists = conn
            .query_row("SELECT 1 FROM tenants WHERE tenant_id = 0", [], |_| Ok(()))
            .optional()
            .map_err(|e| SenseArcError::CatalogError(format!("query tenants: {}", e)))?
            .is_some();

        if !exists {
            let now = current_epoch_ns();
            conn.execute(
                "INSERT INTO tenants (tenant_id, slug, display_name, status, created_at, updated_at)
                 VALUES (0, 'default', 'Default', 'ACTIVE', ?1, ?1)",
                params![now],
            )
            .map_err(|e| SenseArcError::CatalogError(format!("insert default tenant: {}", e)))?;
            info!("seeded default tenant (tenant_id=0, slug=default)");
        }
        Ok(())
    }

    /// 幂等 seed 默认租户业务管理员（Q2 已拍板）。
    ///
    /// 若不存在归属默认租户（`tenant_id=0`）的用户，则经注入的
    /// [`InitialAdminProvisioner`] 供给 `tenant_admin`（随机密码 + 强制首登改密）。
    /// 返回 `Some(password)` 表示本次生成的一次性密码（调用方打印一次）；`None` 表示已存在、跳过。
    pub fn ensure_default_tenant_admin(
        &self,
        provisioner: &dyn InitialAdminProvisioner,
    ) -> Result<Option<String>, SenseArcError> {
        if provisioner.has_tenant_user(TenantId::DEFAULT) {
            return Ok(None);
        }
        let password = provisioner.provision_tenant_admin(TenantId::DEFAULT, "tenant_admin")?;
        Ok(Some(password))
    }

    // ─── CRUD ──────────────────────────────────────────────────────────────

    /// 创建真实租户（`tenant_id >= 1`，0 已被默认租户占用）。
    ///
    /// slug 可用性校验：`slug` 已被 `ACTIVE/SUSPENDED/DELETING` 行占用则拒绝；
    /// `PURGED` 行 `slug` 已 NULL 不占位，可复用。
    pub fn create(
        &self,
        slug: &str,
        display_name: &str,
        quota_template_id: Option<&str>,
        initial_admin: Option<&str>,
    ) -> Result<Tenant, SenseArcError> {
        let slug_trim = slug.trim();
        if slug_trim.is_empty() {
            return Err(SenseArcError::InvalidSecurityRequest(
                "tenant slug must not be empty".to_string(),
            ));
        }
        if display_name.trim().is_empty() {
            return Err(SenseArcError::InvalidSecurityRequest(
                "tenant display_name must not be empty".to_string(),
            ));
        }

        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        // slug 可用性：仅 ACTIVE/SUSPENDED/DELETING 占用。
        let slug_taken = conn
            .query_row(
                "SELECT 1 FROM tenants
                 WHERE slug = ?1 AND status IN ('ACTIVE','SUSPENDED','DELETING') LIMIT 1",
                params![slug_trim],
                |_| Ok(()),
            )
            .optional()
            .map_err(|e| SenseArcError::CatalogError(format!("check slug: {}", e)))?
            .is_some();
        if slug_taken {
            return Err(SenseArcError::TenantAlreadyExists(slug_trim.to_string()));
        }

        // 分配 tenant_id = MAX(tenant_id)+1（PURGED 行保留、不复用）。
        let next_id: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(tenant_id), 0) + 1 FROM tenants",
                [],
                |row| row.get(0),
            )
            .map_err(|e| SenseArcError::CatalogError(format!("allocate tenant_id: {}", e)))?;

        let now = current_epoch_ns();
        conn.execute(
            "INSERT INTO tenants (tenant_id, slug, display_name, status, quota_template_id, initial_admin, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'ACTIVE', ?4, ?5, ?6, ?6)",
            params![next_id, slug_trim, display_name.trim(), quota_template_id, initial_admin, now],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("insert tenant: {}", e)))?;

        let tenant = fetch_tenant(&conn, TenantId(next_id as u32))?
            .ok_or_else(|| SenseArcError::Internal("inserted tenant not found".to_string()))?;
        info!(tenant_id = next_id, slug = %slug_trim, "tenant created");
        Ok(tenant)
    }

    /// 配置租户（更新显示名 / 配额模板；slug 在存活期内不可变）。
    pub fn configure(
        &self,
        tenant_id: TenantId,
        display_name: Option<&str>,
        quota_template_id: Option<&str>,
    ) -> Result<Tenant, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tenant = fetch_tenant(&conn, tenant_id)?
            .ok_or(SenseArcError::TenantNotFound(tenant_id.as_u32()))?;
        if tenant.status_enum() == TenantStatus::Purged {
            return Err(SenseArcError::InvalidTenantState {
                from: tenant.status,
                to: TenantStatus::Purged.as_str().to_string(),
            });
        }

        let new_display = display_name
            .map(|s| s.trim().to_string())
            .unwrap_or(tenant.display_name.clone());
        if new_display.is_empty() {
            return Err(SenseArcError::InvalidSecurityRequest(
                "tenant display_name must not be empty".to_string(),
            ));
        }
        // 三态语义：None = 不更新；Some(空串) = 清除模板（NULL）；Some(非空) = 覆盖模板。
        let new_template = match quota_template_id {
            None => tenant.quota_template_id.clone(),
            Some(s) if s.trim().is_empty() => None,
            Some(s) => Some(s.trim().to_string()),
        };

        let now = current_epoch_ns();
        conn.execute(
            "UPDATE tenants SET display_name = ?1, quota_template_id = ?2, updated_at = ?3 WHERE tenant_id = ?4",
            params![new_display, new_template, now, tenant_id.as_u32()],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("configure tenant: {}", e)))?;

        fetch_tenant(&conn, tenant_id)?
            .ok_or_else(|| SenseArcError::Internal("configured tenant not found".to_string()))
    }

    /// 停用租户：`ACTIVE → SUSPENDED`。
    pub fn suspend(&self, tenant_id: TenantId) -> Result<Tenant, SenseArcError> {
        self.transition(tenant_id, TenantStatus::Active, TenantStatus::Suspended)
    }

    /// 启用租户：`SUSPENDED → ACTIVE`（`DELETING`/`PURGED` 不可恢复）。
    pub fn reactivate(&self, tenant_id: TenantId) -> Result<Tenant, SenseArcError> {
        self.transition(tenant_id, TenantStatus::Suspended, TenantStatus::Active)
    }

    /// 两段式删除入口：`SUSPENDED → DELETING`。
    ///
    /// - `confirmed` 双重确认由调用层（S6 服务层）断言；此处 `false` 直接拒绝。
    /// - 写 `deleted_at=now` + `purge_at=now+cooling_period`。
    /// - 冷静期内 slug 保留、不可抢占；同步写 `tenant_deletions` 初步台账（保留 operator）。
    pub fn delete(
        &self,
        tenant_id: TenantId,
        operator: &str,
        confirmed: bool,
    ) -> Result<Tenant, SenseArcError> {
        if !confirmed {
            return Err(SenseArcError::InvalidSecurityRequest(
                "tenant deletion requires explicit confirmation".to_string(),
            ));
        }
        let operator = operator.trim();
        if operator.is_empty() {
            return Err(SenseArcError::InvalidSecurityRequest(
                "tenant deletion operator must not be empty".to_string(),
            ));
        }

        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tenant = fetch_tenant(&conn, tenant_id)?
            .ok_or(SenseArcError::TenantNotFound(tenant_id.as_u32()))?;
        if tenant.status_enum() != TenantStatus::Suspended {
            return Err(SenseArcError::InvalidTenantState {
                from: tenant.status,
                to: TenantStatus::Deleting.as_str().to_string(),
            });
        }

        let now = current_epoch_ns();
        let cooling_ns = self.deletion_cooling_period_days as i64 * DAY_NS;
        let purge_at = now.saturating_add(cooling_ns);

        conn.execute(
            "UPDATE tenants SET status = 'DELETING', deleted_at = ?1, purge_at = ?2, updated_at = ?1 WHERE tenant_id = ?3",
            params![now, purge_at, tenant_id.as_u32()],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("delete tenant: {}", e)))?;

        // 初步删除台账（保留 operator + slug + 时间戳，供后台 sweeper / finalize 使用）。
        let slug = tenant.slug.clone().unwrap_or_default();
        conn.execute(
            "INSERT INTO tenant_deletions (
                 tenant_id, slug, display_name, operator, delete_initiated_at, purge_due_at,
                 purge_completed_at, data_scope, purge_method, audit_anchor, certificate_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, '', '', '', '{}')
             ON CONFLICT(tenant_id) DO UPDATE SET
                 slug = excluded.slug,
                 display_name = excluded.display_name,
                 operator = excluded.operator,
                 delete_initiated_at = excluded.delete_initiated_at,
                 purge_due_at = excluded.purge_due_at",
            params![
                tenant_id.as_u32(),
                slug,
                tenant.display_name,
                operator,
                now,
                purge_at
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("insert tenant_deletions: {}", e)))?;

        fetch_tenant(&conn, tenant_id)?
            .ok_or_else(|| SenseArcError::Internal("deleted tenant not found".to_string()))
    }

    /// 撤销删除：`DELETING → SUSPENDED`（整租户恢复）。
    ///
    /// 仅 `now < purge_at`（冷静期内）可撤销；`purge_at` 已过则拒绝（只能走物理清除）。
    pub fn cancel_deletion(&self, tenant_id: TenantId) -> Result<Tenant, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tenant = fetch_tenant(&conn, tenant_id)?
            .ok_or(SenseArcError::TenantNotFound(tenant_id.as_u32()))?;
        if tenant.status_enum() != TenantStatus::Deleting {
            return Err(SenseArcError::InvalidTenantState {
                from: tenant.status,
                to: TenantStatus::Suspended.as_str().to_string(),
            });
        }

        let now = current_epoch_ns();
        if let Some(purge_at) = tenant.purge_at {
            if now >= purge_at {
                return Err(SenseArcError::InvalidTenantState {
                    from: tenant.status,
                    to: TenantStatus::Suspended.as_str().to_string(),
                });
            }
        }

        conn.execute(
            "UPDATE tenants SET status = 'SUSPENDED', deleted_at = NULL, purge_at = NULL, updated_at = ?1 WHERE tenant_id = ?2",
            params![now, tenant_id.as_u32()],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("cancel_deletion tenant: {}", e)))?;

        // 撤销后删除初步台账（保留审计追溯的权威来源由 finalize 重新写入）。
        let _ = conn.execute(
            "DELETE FROM tenant_deletions WHERE tenant_id = ?1",
            params![tenant_id.as_u32()],
        );

        fetch_tenant(&conn, tenant_id)?
            .ok_or_else(|| SenseArcError::Internal("canceled tenant not found".to_string()))
    }

    /// 物理清除终态：`DELETING → PURGED`，`slug=NULL` 释放（可被新租户复用）。
    ///
    /// 同步写入/更新 `tenant_deletions` 完整台账（`DeletionRecord`，含清除证明）。
    /// 由 Step 6 `finalize_deletion` 在物理清除完成后调用。
    pub fn mark_purged(
        &self,
        tenant_id: TenantId,
        record: &DeletionRecord,
    ) -> Result<(), SenseArcError> {
        if record.tenant_id != tenant_id {
            return Err(SenseArcError::InvalidSecurityRequest(
                "deletion record tenant_id mismatch".to_string(),
            ));
        }

        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tenant = fetch_tenant(&conn, tenant_id)?
            .ok_or(SenseArcError::TenantNotFound(tenant_id.as_u32()))?;
        if tenant.status_enum() != TenantStatus::Deleting {
            return Err(SenseArcError::InvalidTenantState {
                from: tenant.status,
                to: TenantStatus::Purged.as_str().to_string(),
            });
        }

        let now = current_epoch_ns();
        conn.execute(
            "UPDATE tenants SET status = 'PURGED', slug = NULL, updated_at = ?1 WHERE tenant_id = ?2",
            params![now, tenant_id.as_u32()],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("mark_purged tenant: {}", e)))?;

        conn.execute(
            "INSERT INTO tenant_deletions (
                 tenant_id, slug, display_name, operator, delete_initiated_at, purge_due_at,
                 purge_completed_at, data_scope, purge_method, audit_anchor, certificate_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(tenant_id) DO UPDATE SET
                 slug = excluded.slug,
                 display_name = excluded.display_name,
                 operator = excluded.operator,
                 delete_initiated_at = excluded.delete_initiated_at,
                 purge_due_at = excluded.purge_due_at,
                 purge_completed_at = excluded.purge_completed_at,
                 data_scope = excluded.data_scope,
                 purge_method = excluded.purge_method,
                 audit_anchor = excluded.audit_anchor,
                 certificate_json = excluded.certificate_json",
            params![
                record.tenant_id.as_u32(),
                record.slug,
                record.display_name,
                record.operator,
                record.delete_initiated_at,
                record.purge_due_at,
                record.purge_completed_at,
                record.data_scope,
                record.purge_method,
                record.audit_anchor,
                record.certificate_json,
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("mark_purged tenant_deletions: {}", e)))?;

        info!(
            tenant_id = tenant_id.as_u32(),
            "tenant purged (slug released)"
        );
        Ok(())
    }

    /// 读取删除台账（供 finalize 组装清除证明）。
    pub fn get_deletion_record(
        &self,
        tenant_id: TenantId,
    ) -> Result<Option<DeletionRecord>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.query_row(
            "SELECT tenant_id, slug, display_name, operator, delete_initiated_at, purge_due_at,
                    purge_completed_at, data_scope, purge_method, audit_anchor, certificate_json
             FROM tenant_deletions WHERE tenant_id = ?1",
            params![tenant_id.as_u32()],
            |row| {
                Ok(DeletionRecord {
                    tenant_id: TenantId(row.get::<_, i64>(0)? as u32),
                    slug: row.get(1)?,
                    display_name: row.get(2)?,
                    operator: row.get(3)?,
                    delete_initiated_at: row.get(4)?,
                    purge_due_at: row.get(5)?,
                    purge_completed_at: row.get(6)?,
                    data_scope: row.get(7)?,
                    purge_method: row.get(8)?,
                    audit_anchor: row.get(9)?,
                    certificate_json: row.get(10)?,
                })
            },
        )
        .optional()
        .map_err(|e| SenseArcError::CatalogError(format!("query tenant_deletions: {}", e)))
    }

    // ─── 查询 ──────────────────────────────────────────────────────────────

    /// 按 slug 解析（仅匹配 `ACTIVE/SUSPENDED/DELETING` 且 `slug IS NOT NULL`）。
    pub fn resolve_slug(&self, slug: &str) -> Result<Option<Tenant>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        conn.query_row(
            "SELECT tenant_id, slug, display_name, status, quota_template_id, initial_admin,
                    deleted_at, purge_at, created_at, updated_at
             FROM tenants
             WHERE slug = ?1 AND status IN ('ACTIVE','SUSPENDED','DELETING')",
            params![slug],
            row_to_tenant,
        )
        .optional()
        .map_err(|e| SenseArcError::CatalogError(format!("resolve slug: {}", e)))
    }

    /// 按 tenant_id 查询。
    pub fn get_tenant(&self, tenant_id: TenantId) -> Result<Option<Tenant>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        fetch_tenant(&conn, tenant_id)
    }

    /// 列出全部租户（含 `PURGED`，供平台审计视图）。
    pub fn list_tenants(&self) -> Result<Vec<Tenant>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        list_tenants_where(&conn, "")
    }

    /// 列出活跃租户（排除 `PURGED`）。
    pub fn list_active_tenants(&self) -> Result<Vec<Tenant>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        list_tenants_where(&conn, "WHERE status != 'PURGED'")
    }

    /// 默认租户（`tenant_id=0`）。
    pub fn default_tenant(&self) -> Result<Tenant, SenseArcError> {
        self.get_tenant(TenantId::DEFAULT)?
            .ok_or_else(|| SenseArcError::TenantNotFound(0))
    }

    /// 枚举 `purge_at <= now` 的 `DELETING` 租户（供后台 sweeper 物理清除）。
    pub fn list_deleting_due(&self, now: i64) -> Result<Vec<Tenant>, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;
        let mut stmt = conn
            .prepare(
                "SELECT tenant_id, slug, display_name, status, quota_template_id, initial_admin,
                        deleted_at, purge_at, created_at, updated_at
                 FROM tenants
                 WHERE status = 'DELETING' AND purge_at IS NOT NULL AND purge_at <= ?1
                 ORDER BY tenant_id",
            )
            .map_err(|e| {
                SenseArcError::CatalogError(format!("prepare list_deleting_due: {}", e))
            })?;
        let rows = stmt
            .query_map(params![now], row_to_tenant)
            .map_err(|e| SenseArcError::CatalogError(format!("query list_deleting_due: {}", e)))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| SenseArcError::CatalogError(format!("collect list_deleting_due: {}", e)))
    }

    // ─── 状态机内部 ─────────────────────────────────────────────────────────

    /// 校验当前态并转换（`from == current` 否则 `InvalidTenantState`）。
    fn transition(
        &self,
        tenant_id: TenantId,
        from: TenantStatus,
        to: TenantStatus,
    ) -> Result<Tenant, SenseArcError> {
        let conn = self
            .catalog
            .conn()
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tenant = fetch_tenant(&conn, tenant_id)?
            .ok_or(SenseArcError::TenantNotFound(tenant_id.as_u32()))?;
        if tenant.status_enum() != from {
            return Err(SenseArcError::InvalidTenantState {
                from: tenant.status,
                to: to.as_str().to_string(),
            });
        }

        let now = current_epoch_ns();
        conn.execute(
            "UPDATE tenants SET status = ?1, updated_at = ?2 WHERE tenant_id = ?3",
            params![to.as_str(), now, tenant_id.as_u32()],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("transition tenant: {}", e)))?;

        fetch_tenant(&conn, tenant_id)?
            .ok_or_else(|| SenseArcError::Internal("transitioned tenant not found".to_string()))
    }
}

/// 单租户行 → `Tenant`。
fn row_to_tenant(row: &rusqlite::Row<'_>) -> rusqlite::Result<Tenant> {
    Ok(Tenant {
        tenant_id: TenantId(row.get::<_, i64>(0)? as u32),
        slug: row.get(1)?,
        display_name: row.get(2)?,
        status: row.get(3)?,
        quota_template_id: row.get(4)?,
        initial_admin: row.get(5)?,
        deleted_at: row.get(6)?,
        purge_at: row.get(7)?,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
    })
}

/// 按 tenant_id 查询（内部，复用连接锁）。
fn fetch_tenant(conn: &Connection, tenant_id: TenantId) -> Result<Option<Tenant>, SenseArcError> {
    conn.query_row(
        "SELECT tenant_id, slug, display_name, status, quota_template_id, initial_admin,
                deleted_at, purge_at, created_at, updated_at
         FROM tenants WHERE tenant_id = ?1",
        params![tenant_id.as_u32()],
        row_to_tenant,
    )
    .optional()
    .map_err(|e| SenseArcError::CatalogError(format!("query tenant: {}", e)))
}

/// 列表查询（复用连接锁；`extra` 为可空 WHERE 子句）。
fn list_tenants_where(conn: &Connection, extra: &str) -> Result<Vec<Tenant>, SenseArcError> {
    let sql = format!(
        "SELECT tenant_id, slug, display_name, status, quota_template_id, initial_admin,
                deleted_at, purge_at, created_at, updated_at
         FROM tenants {} ORDER BY tenant_id",
        extra
    );
    conn.prepare(&sql)
        .map_err(|e| SenseArcError::CatalogError(format!("prepare list_tenants: {}", e)))?
        .query_map([], row_to_tenant)
        .map_err(|e| SenseArcError::CatalogError(format!("query list_tenants: {}", e)))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SenseArcError::CatalogError(format!("collect list_tenants: {}", e)))
}

/// 每天纳秒数（冷静期计算）。
const DAY_NS: i64 = 24 * 60 * 60 * 1_000_000_000;

/// 当前 epoch 纳秒时间戳。
pub(crate) fn current_epoch_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopProvisioner;

    impl InitialAdminProvisioner for NoopProvisioner {
        fn has_tenant_user(&self, _tenant_id: TenantId) -> bool {
            false
        }
        fn provision_tenant_admin(
            &self,
            _tenant_id: TenantId,
            _username: &str,
        ) -> Result<String, SenseArcError> {
            Ok("random-password".to_string())
        }
    }

    fn open_manager(cooling_days: u32) -> (tempfile::TempDir, Arc<Catalog>, TenantManager) {
        let tmp = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(Catalog::open(&tmp.path().join("config.db")).unwrap());
        let manager = TenantManager::new(catalog.clone()).with_cooling_period_days(cooling_days);
        manager.ensure_default_tenant().unwrap();
        (tmp, catalog, manager)
    }

    #[test]
    fn test_ensure_default_tenant_idempotent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(Catalog::open(&tmp.path().join("config.db")).unwrap());
        let manager = TenantManager::new(catalog.clone());

        manager.ensure_default_tenant().unwrap();
        manager.ensure_default_tenant().unwrap();

        let conn = catalog.conn().lock().unwrap();
        let (id, slug, display): (i64, Option<String>, String) = conn
            .query_row(
                "SELECT tenant_id, slug, display_name FROM tenants WHERE tenant_id = 0",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(slug.as_deref(), Some("default"));
        assert_eq!(display, "Default");
    }

    #[test]
    fn test_create_allocates_sequential_ids_and_unique_slug() {
        let (_t, _c, manager) = open_manager(7);

        let t1 = manager.create("acme", "Acme", None, None).unwrap();
        assert!(t1.tenant_id.as_u32() >= 1);
        assert_eq!(t1.status_enum(), TenantStatus::Active);

        let t2 = manager.create("beta", "Beta", None, None).unwrap();
        assert!(t2.tenant_id.as_u32() > t1.tenant_id.as_u32());

        // 重复 slug 报错。
        let err = manager.create("acme", "Acme2", None, None).unwrap_err();
        assert!(matches!(err, SenseArcError::TenantAlreadyExists(_)));
    }

    #[test]
    fn test_suspend_reactivate_state_machine() {
        let (_t, _c, manager) = open_manager(7);
        let t = manager.create("t1", "T1", None, None).unwrap();

        // ACTIVE 直接 delete 拒绝。
        assert!(matches!(
            manager.delete(t.tenant_id, "op", true),
            Err(SenseArcError::InvalidTenantState { .. })
        ));
        // 未确认拒绝。
        assert!(manager.suspend(t.tenant_id).is_ok());
        // 重复 suspend 拒绝（SUSPENDED 不能再 suspend）。
        assert!(matches!(
            manager.suspend(t.tenant_id),
            Err(SenseArcError::InvalidTenantState { .. })
        ));

        assert!(manager.reactivate(t.tenant_id).is_ok());
        assert_eq!(
            manager
                .get_tenant(t.tenant_id)
                .unwrap()
                .unwrap()
                .status_enum(),
            TenantStatus::Active
        );
    }

    #[test]
    fn test_two_phase_deletion_and_slug_lifecycle() {
        // 冷静期 1 天。
        let (_t, _c, manager) = open_manager(1);
        let t = manager.create("acme", "Acme", None, None).unwrap();
        let tid = t.tenant_id;

        manager.suspend(tid).unwrap();
        let deleting = manager.delete(tid, "platform-admin", true).unwrap();
        assert_eq!(deleting.status_enum(), TenantStatus::Deleting);
        assert!(deleting.deleted_at.is_some());
        assert!(deleting.purge_at.is_some());

        // DELETING 期间 slug 保留、不可抢占。
        assert!(matches!(
            manager.create("acme", "Acme2", None, None),
            Err(SenseArcError::TenantAlreadyExists(_))
        ));
        // resolve_slug 仍返回 DELETING 行。
        assert_eq!(
            manager.resolve_slug("acme").unwrap().unwrap().tenant_id,
            tid
        );

        // cancel_deletion（冷静期内）→ SUSPENDED。
        let suspended = manager.cancel_deletion(tid).unwrap();
        assert_eq!(suspended.status_enum(), TenantStatus::Suspended);
        assert!(suspended.deleted_at.is_none());

        // 再次进入 DELETING 后物理清除。
        manager.delete(tid, "platform-admin", true).unwrap();
        let record = DeletionRecord {
            tenant_id: tid,
            slug: "acme".to_string(),
            display_name: Some("Acme".to_string()),
            operator: "platform-admin".to_string(),
            delete_initiated_at: current_epoch_ns(),
            purge_due_at: current_epoch_ns(),
            purge_completed_at: current_epoch_ns(),
            data_scope: "assets=0,points=0".to_string(),
            purge_method: "crypto-shred + physical-delete".to_string(),
            audit_anchor: "deadbeef".repeat(8),
            certificate_json: "{}".to_string(),
        };
        manager.mark_purged(tid, &record).unwrap();

        let purged = manager.get_tenant(tid).unwrap().unwrap();
        assert_eq!(purged.status_enum(), TenantStatus::Purged);
        assert!(purged.slug.is_none());

        // slug 释放，可被新租户复用。
        let reused = manager.create("acme", "Acme Reborn", None, None).unwrap();
        assert_ne!(reused.tenant_id, tid);
        assert_eq!(reused.slug.as_deref(), Some("acme"));

        // resolve_slug 不再返回 PURGED 行（匹配新租户）。
        assert_eq!(
            manager.resolve_slug("acme").unwrap().unwrap().tenant_id,
            reused.tenant_id
        );
    }

    #[test]
    fn test_delete_requires_suspended_and_confirmation() {
        let (_t, _c, manager) = open_manager(7);
        let t = manager.create("t1", "T1", None, None).unwrap();

        // 未确认拒绝。
        assert!(matches!(
            manager.delete(t.tenant_id, "op", false),
            Err(SenseArcError::InvalidSecurityRequest(_))
        ));
        // ACTIVE 直接 delete 拒绝。
        assert!(matches!(
            manager.delete(t.tenant_id, "op", true),
            Err(SenseArcError::InvalidTenantState { .. })
        ));
    }

    #[test]
    fn test_list_deleting_due() {
        let (_t, _c, manager) = open_manager(0); // 冷静期 0 天 → purge_at = now。
        let t = manager.create("t1", "T1", None, None).unwrap();
        manager.suspend(t.tenant_id).unwrap();
        manager.delete(t.tenant_id, "op", true).unwrap();

        let due = manager.list_deleting_due(current_epoch_ns()).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].tenant_id, t.tenant_id);

        // 过去时间点无到期（purge_at 恒为未来/当前 epoch ns，远大于 0）。
        assert!(manager.list_deleting_due(0).unwrap().is_empty());
    }

    #[test]
    fn test_configure_updates_display_and_template_not_slug() {
        let (_t, _c, manager) = open_manager(7);
        let t = manager.create("t1", "T1", None, None).unwrap();

        let updated = manager
            .configure(t.tenant_id, Some("T1 Renamed"), Some("small"))
            .unwrap();
        assert_eq!(updated.display_name, "T1 Renamed");
        assert_eq!(updated.quota_template_id.as_deref(), Some("small"));
        assert_eq!(updated.slug.as_deref(), Some("t1"));
    }

    #[test]
    fn test_list_tenants_and_active() {
        let (_t, _c, manager) = open_manager(1);
        let a = manager.create("a", "A", None, None).unwrap();
        let b = manager.create("b", "B", None, None).unwrap();

        manager.suspend(b.tenant_id).unwrap();
        manager.delete(b.tenant_id, "op", true).unwrap();
        manager
            .mark_purged(
                b.tenant_id,
                &DeletionRecord {
                    tenant_id: b.tenant_id,
                    slug: "b".to_string(),
                    display_name: Some("B".to_string()),
                    operator: "op".to_string(),
                    delete_initiated_at: 1,
                    purge_due_at: 1,
                    purge_completed_at: 1,
                    data_scope: "".to_string(),
                    purge_method: "".to_string(),
                    audit_anchor: "".to_string(),
                    certificate_json: "{}".to_string(),
                },
            )
            .unwrap();

        let all = manager.list_tenants().unwrap();
        // default(0) + a + b = 3（含 PURGED）。
        assert_eq!(all.len(), 3);
        assert!(all
            .iter()
            .any(|x| x.tenant_id == b.tenant_id && x.status_enum() == TenantStatus::Purged));

        let active = manager.list_active_tenants().unwrap();
        assert_eq!(active.len(), 2);
        assert!(active
            .iter()
            .all(|x| x.status_enum() != TenantStatus::Purged));
        assert!(active.iter().any(|x| x.tenant_id == a.tenant_id));
    }

    #[test]
    fn test_default_tenant_and_admin_seed() {
        let (_t, _c, manager) = open_manager(7);
        assert_eq!(
            manager.default_tenant().unwrap().tenant_id,
            TenantId::DEFAULT
        );

        // 无用户时 seed 返回密码。
        let pwd = manager
            .ensure_default_tenant_admin(&NoopProvisioner)
            .unwrap();
        assert_eq!(pwd.as_deref(), Some("random-password"));
    }
}

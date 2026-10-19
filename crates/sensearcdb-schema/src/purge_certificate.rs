//! 清除证明（PurgeCertificate）+ 租户删除台账（DeletionRecord）。
//!
//! 两段式删除的审计追溯**唯一权威来源**是 `tenant_deletions` 表（`DeletionRecord`），
//! 不依赖 `tenants` 行占位或 slug 保留（Q3 已拍板）。`PurgeCertificate` 是删除完成时
//! 生成的 8 字段最低模板 + 「密钥销毁」字段，序列化后写入 `DeletionRecord.certificate_json`，
//! 其 SHA-256 摘要即 `audit_anchor`，供审计链锚定。

use sensearcdb_common::TenantId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// 清除证明（PurgeCertificate）——8 字段最低模板 + 密钥销毁字段。
///
/// 8 字段：
/// 1. 租户标识（`tenant_id` + `slug` + `display_name`）
/// 2. 操作者（`operator`）
/// 3. 删除发起时间（`delete_initiated_at`，epoch ns）
/// 4. 冷静期到期（`purge_due_at`，epoch ns）
/// 5. 物理清除完成时间（`purge_completed_at`，epoch ns）
/// 6. 数据范围（`data_scope`：资产/测点/主体/存储分片清单摘要）
/// 7. 清除方式（`purge_method`：`"crypto-shred + physical-delete"`）
/// 8. 审计锚点（`audit_anchor`：SHA-256(certificate_json)）
///
/// 附加（Q1/★H）：`key_shredded` 记录销毁的 `(tenant_id, dek_version)` 列表。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurgeCertificate {
    /// 租户 ID（保留不复用）。
    pub tenant_id: TenantId,
    /// 释放前原始 slug（审计追溯）。
    pub slug: String,
    /// 展示名。
    pub display_name: String,
    /// 发起删除的操作者。
    pub operator: String,
    /// 删除发起时间（epoch ns）。
    pub delete_initiated_at: i64,
    /// 冷静期到期时间（epoch ns）。
    pub purge_due_at: i64,
    /// 物理清除完成时间（epoch ns）。
    pub purge_completed_at: i64,
    /// 数据范围摘要（资产/测点/主体/存储分片清单）。
    pub data_scope: String,
    /// 清除方式（`"crypto-shred + physical-delete"`）。
    pub purge_method: String,
    /// 密钥销毁字段：销毁的 `(tenant_id, dek_version)` 列表。
    pub key_shredded: Vec<(u32, u32)>,
    /// 审计锚点：证书内容序列化后的 SHA-256（本字段不参与自身 hash）。
    pub audit_anchor: String,
}

impl PurgeCertificate {
    /// 构造清除证明，`audit_anchor` 先置空，随后调用 [`seal`](Self::seal) 盖章。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        slug: String,
        display_name: String,
        operator: String,
        delete_initiated_at: i64,
        purge_due_at: i64,
        purge_completed_at: i64,
        data_scope: String,
        purge_method: String,
        key_shredded: Vec<(u32, u32)>,
    ) -> Self {
        Self {
            tenant_id,
            slug,
            display_name,
            operator,
            delete_initiated_at,
            purge_due_at,
            purge_completed_at,
            data_scope,
            purge_method,
            key_shredded,
            audit_anchor: String::new(),
        }
    }

    /// 计算 `audit_anchor = SHA-256(certificate_json)`（不含 audit_anchor 自身，避免循环依赖）。
    pub fn compute_audit_anchor(&self) -> String {
        let mut copy = self.clone();
        copy.audit_anchor = String::new();
        let json = serde_json::to_string(&copy).unwrap_or_default();
        hex_encode(&Sha256::digest(json.as_bytes()))
    }

    /// 盖章：写入 `audit_anchor` 并返回自身。
    pub fn seal(mut self) -> Self {
        self.audit_anchor = self.compute_audit_anchor();
        self
    }

    /// 证书完整 JSON 序列化（含 audit_anchor，用于 `tenant_deletions.certificate_json`）。
    pub fn certificate_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// 租户删除台账（对应 `tenant_deletions` 表）。
///
/// 是审计追溯的**唯一权威来源**：`tenant_id` 保留不复用、原始 slug、三时间戳、
/// `audit_anchor` 与 `certificate_json` 齐全，物理清除后仍可完整追溯。
#[derive(Debug, Clone)]
pub struct DeletionRecord {
    /// 租户 ID（保留、不复用）。
    pub tenant_id: TenantId,
    /// 释放前原始 slug。
    pub slug: String,
    /// 展示名（可选）。
    pub display_name: Option<String>,
    /// 发起删除的操作者。
    pub operator: String,
    /// 删除发起时间（epoch ns）。
    pub delete_initiated_at: i64,
    /// 冷静期到期时间（epoch ns）。
    pub purge_due_at: i64,
    /// 物理清除完成时间（epoch ns）。
    pub purge_completed_at: i64,
    /// 数据范围摘要。
    pub data_scope: String,
    /// 清除方式。
    pub purge_method: String,
    /// 审计锚点 hash。
    pub audit_anchor: String,
    /// `PurgeCertificate` 完整 JSON。
    pub certificate_json: String,
}

impl From<&PurgeCertificate> for DeletionRecord {
    fn from(cert: &PurgeCertificate) -> Self {
        Self {
            tenant_id: cert.tenant_id,
            slug: cert.slug.clone(),
            display_name: Some(cert.display_name.clone()),
            operator: cert.operator.clone(),
            delete_initiated_at: cert.delete_initiated_at,
            purge_due_at: cert.purge_due_at,
            purge_completed_at: cert.purge_completed_at,
            data_scope: cert.data_scope.clone(),
            purge_method: cert.purge_method.clone(),
            audit_anchor: cert.audit_anchor.clone(),
            certificate_json: cert.certificate_json(),
        }
    }
}

/// 字节切片 → 小写 hex 字符串。
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_purge_certificate_eight_fields_and_anchor() {
        let cert = PurgeCertificate::new(
            TenantId(7),
            "acme".to_string(),
            "Acme Corp".to_string(),
            "platform-admin".to_string(),
            1_000,
            1_000_000,
            2_000_000,
            "assets=3,points=12,users=2,storage=1/{table}".to_string(),
            "crypto-shred + physical-delete".to_string(),
            vec![(7, 1), (7, 2)],
        );

        // 8 字段非空 + 密钥销毁字段。
        assert_eq!(cert.tenant_id, TenantId(7));
        assert_eq!(cert.slug, "acme");
        assert!(!cert.display_name.is_empty());
        assert!(!cert.operator.is_empty());
        assert!(cert.delete_initiated_at > 0);
        assert!(cert.purge_due_at > 0);
        assert!(cert.purge_completed_at > 0);
        assert!(!cert.data_scope.is_empty());
        assert!(!cert.purge_method.is_empty());
        assert_eq!(cert.key_shredded, vec![(7, 1), (7, 2)]);
        assert!(cert.audit_anchor.is_empty());
    }

    #[test]
    fn test_audit_anchor_is_sha256_of_json() {
        let cert = PurgeCertificate::new(
            TenantId(1),
            "t1".to_string(),
            "T1".to_string(),
            "op".to_string(),
            1,
            2,
            3,
            "scope".to_string(),
            "crypto-shred + physical-delete".to_string(),
            vec![(1, 1)],
        )
        .seal();

        // 64 个 hex 字符 = 32 字节 SHA-256。
        assert_eq!(cert.audit_anchor.len(), 64);
        // 确定性：重算一致。
        assert_eq!(cert.audit_anchor, cert.compute_audit_anchor());
        // certificate_json 含 audit_anchor。
        let json = cert.certificate_json();
        assert!(json.contains(&cert.audit_anchor));
    }

    #[test]
    fn test_deletion_record_from_certificate() {
        let cert = PurgeCertificate::new(
            TenantId(2),
            "t2".to_string(),
            "T2".to_string(),
            "op".to_string(),
            1,
            2,
            3,
            "scope".to_string(),
            "crypto-shred + physical-delete".to_string(),
            vec![],
        )
        .seal();

        let rec = DeletionRecord::from(&cert);
        assert_eq!(rec.tenant_id, TenantId(2));
        assert_eq!(rec.slug, "t2");
        assert_eq!(rec.display_name.as_deref(), Some("T2"));
        assert_eq!(rec.audit_anchor, cert.audit_anchor);
        assert_eq!(rec.certificate_json, cert.certificate_json());
    }
}

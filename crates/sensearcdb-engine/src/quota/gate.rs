//! 数据面配额裁决点 `TenantQuotaGate`（S4 Step 3）。
//!
//! 裁决点落在 **engine 数据面**（`Engine::write_batch` / `apply_batch_in_memory`），
//! 非仅 REST/API 层：绕过 API 直写 `engine.write_batch` 超限同样被拦。
//!
//! 硬限制三要素（测点数 / 存储 / 写入速率）+ 软告警（`warn_ratio`）。
//! 保留期（retention_days）不落本 gate（生命周期淘汰归 per-tenant TTL）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sensearcdb_common::TenantId;

use crate::engine::PointBatch;

/// 配额快照（仅限制值，不含当前用量；由 [`QuotaSource`] 下发）。
#[derive(Debug, Clone)]
pub struct QuotaSnapshot {
    /// 测点数上限（None = 不限）。
    pub points_limit: Option<u64>,
    /// 存储上限字节（None = 不限）。
    pub storage_limit_bytes: Option<u64>,
    /// 写入速率 points/s（None = 不限）。
    pub write_rate_limit: Option<u64>,
    /// 软告警阈值比例（默认 0.8）。
    pub warn_ratio: f64,
}

impl Default for QuotaSnapshot {
    fn default() -> Self {
        Self {
            points_limit: None,
            storage_limit_bytes: None,
            write_rate_limit: None,
            warn_ratio: 0.8,
        }
    }
}

/// 配额来源 trait：S6 用 `TenantQuotaStore::resolve_quota` 实现。
pub trait QuotaSource: Send + Sync {
    /// 返回某租户的配额快照；`None` = 无配额（放行）。
    fn snapshot(&self, tenant_id: TenantId) -> Option<QuotaSnapshot>;
}

/// 裁决拒绝原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaRejectReason {
    /// 写入速率超限（token bucket）。
    RateLimited,
    /// 测点数/存储硬限制超限。
    QuotaExceeded,
}

/// 裁决结果。
#[derive(Debug, Clone, PartialEq)]
pub enum QuotaDecision {
    /// 放行。
    Admit,
    /// 放行但接近软告警阈值（`ratio` 为当前占用比例）。
    Warn { ratio: f64 },
    /// 拒绝。
    Reject { reason: QuotaRejectReason },
}

/// 进程内 token bucket（非持久化，重启清零重计，与现有 rate_limiting 一致）。
struct TokenBucket {
    /// 桶容量（tokens/s）。
    capacity: u64,
    /// 当前令牌数。
    tokens: f64,
    /// 上次补充时间。
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: u64) -> Self {
        Self {
            capacity,
            tokens: capacity as f64,
            last_refill: Instant::now(),
        }
    }

    /// 按流逝时间补充令牌。
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.capacity as f64).min(self.capacity as f64);
        self.last_refill = now;
    }

    /// 尝试消费 `n` 个令牌，成功返回 `true`。
    fn try_consume(&mut self, n: u64) -> bool {
        self.refill();
        if self.tokens >= n as f64 {
            self.tokens -= n as f64;
            true
        } else {
            false
        }
    }
}

/// 门内状态（token bucket + 进程内计量计数）。
struct GateState {
    buckets: HashMap<TenantId, TokenBucket>,
    point_counts: HashMap<TenantId, u64>,
    storage_bytes: HashMap<TenantId, u64>,
}

/// 数据面配额闸口。
pub struct TenantQuotaGate {
    source: Arc<dyn QuotaSource>,
    state: Mutex<GateState>,
}

impl TenantQuotaGate {
    /// 构造闸口。
    pub fn new(source: Arc<dyn QuotaSource>) -> Self {
        Self {
            source,
            state: Mutex::new(GateState {
                buckets: HashMap::new(),
                point_counts: HashMap::new(),
                storage_bytes: HashMap::new(),
            }),
        }
    }

    /// 裁决一个写批次。
    ///
    /// 顺序：写速率 token bucket → 测点数硬限制 → 存储硬限制；
    /// 任一超限返回 `Reject`（该 batch 不进 MemTable）；否则更新计量并可能返回 `Warn`。
    pub fn admit(&self, tenant_id: TenantId, batch: &PointBatch) -> QuotaDecision {
        let snapshot = match self.source.snapshot(tenant_id) {
            Some(s) => s,
            None => return QuotaDecision::Admit, // 无配额 = 放行（N=1 兼容）
        };

        let point_count = batch.points.len() as u64;
        let est_bytes = estimate_batch_bytes(batch);

        let mut state = self.state.lock().unwrap();

        // 1. 写速率 token bucket。
        if let Some(rate) = snapshot.write_rate_limit {
            if rate > 0 {
                let bucket = state
                    .buckets
                    .entry(tenant_id)
                    .or_insert_with(|| TokenBucket::new(rate));
                if !bucket.try_consume(point_count) {
                    return QuotaDecision::Reject {
                        reason: QuotaRejectReason::RateLimited,
                    };
                }
            }
        }

        // 2. 测点数硬限制。
        let current_points = state.point_counts.get(&tenant_id).copied().unwrap_or(0);
        if let Some(limit) = snapshot.points_limit {
            if current_points.saturating_add(point_count) > limit {
                return QuotaDecision::Reject {
                    reason: QuotaRejectReason::QuotaExceeded,
                };
            }
        }

        // 3. 存储硬限制（按批次估算字节）。
        let current_bytes = state.storage_bytes.get(&tenant_id).copied().unwrap_or(0);
        if let Some(limit) = snapshot.storage_limit_bytes {
            if current_bytes.saturating_add(est_bytes) > limit {
                return QuotaDecision::Reject {
                    reason: QuotaRejectReason::QuotaExceeded,
                };
            }
        }

        // 更新计量。
        let new_points = current_points + point_count;
        state.point_counts.insert(tenant_id, new_points);
        let new_bytes = current_bytes + est_bytes;
        state.storage_bytes.insert(tenant_id, new_bytes);

        // 软告警。
        if snapshot.warn_ratio > 0.0 {
            let point_ratio = snapshot
                .points_limit
                .filter(|l| *l > 0)
                .map(|l| new_points as f64 / l as f64)
                .unwrap_or(0.0);
            let storage_ratio = snapshot
                .storage_limit_bytes
                .filter(|l| *l > 0)
                .map(|l| new_bytes as f64 / l as f64)
                .unwrap_or(0.0);
            let ratio = point_ratio.max(storage_ratio);
            if ratio >= snapshot.warn_ratio {
                tracing::warn!(
                    tenant_id = tenant_id.as_u32(),
                    ratio,
                    "tenant quota approaching limit (soft warning)"
                );
                return QuotaDecision::Warn { ratio };
            }
        }

        QuotaDecision::Admit
    }

    /// 当前已计量测点数（供测试/观测）。
    pub fn point_count(&self, tenant_id: TenantId) -> u64 {
        self.state
            .lock()
            .unwrap()
            .point_counts
            .get(&tenant_id)
            .copied()
            .unwrap_or(0)
    }
}

/// 粗略估算一个批次的落盘字节数（用于存储硬限制判定）。
fn estimate_batch_bytes(batch: &PointBatch) -> u64 {
    batch
        .points
        .iter()
        .map(|p| {
            // ts(8B) + server_ts 不计 + 每值 8B + 每质量 1B + 固定开销 16B。
            8 + (p.values.len() as u64 * 8) + (p.qualities.len() as u64) + 16
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::DataPoint;
    use crate::memtable::TypedValue;

    struct StaticSource {
        snapshot: Option<QuotaSnapshot>,
    }

    impl QuotaSource for StaticSource {
        fn snapshot(&self, _tenant_id: TenantId) -> Option<QuotaSnapshot> {
            self.snapshot.clone()
        }
    }

    fn batch(n: usize, tenant_id: TenantId) -> PointBatch {
        PointBatch {
            points: (0..n)
                .map(|i| DataPoint {
                    table_id: 1,
                    tenant_id,
                    ts: i as i64,
                    server_ts: i as i64,
                    values: vec![(0, TypedValue::F64(i as f64))],
                    qualities: vec![(0, 0xC0)],
                    schema_version: 1,
                    flags: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn test_no_quota_admits() {
        let gate = TenantQuotaGate::new(Arc::new(StaticSource { snapshot: None }));
        assert_eq!(
            gate.admit(TenantId(1), &batch(1000, TenantId(1))),
            QuotaDecision::Admit
        );
    }

    #[test]
    fn test_points_limit_rejects_and_isolates_tenants() {
        let gate = TenantQuotaGate::new(Arc::new(StaticSource {
            snapshot: Some(QuotaSnapshot {
                points_limit: Some(2),
                warn_ratio: 0.0, // 关闭软告警，聚焦硬限制。
                ..Default::default()
            }),
        }));

        // 租户 1 前 2 点放行，第 3 点拒绝。
        assert_eq!(
            gate.admit(TenantId(1), &batch(1, TenantId(1))),
            QuotaDecision::Admit
        );
        assert_eq!(
            gate.admit(TenantId(1), &batch(1, TenantId(1))),
            QuotaDecision::Admit
        );
        assert_eq!(
            gate.admit(TenantId(1), &batch(1, TenantId(1))),
            QuotaDecision::Reject {
                reason: QuotaRejectReason::QuotaExceeded
            }
        );

        // 租户 2 独立计数，不受租户 1 影响。
        assert_eq!(
            gate.admit(TenantId(2), &batch(2, TenantId(2))),
            QuotaDecision::Admit
        );
        assert_eq!(gate.point_count(TenantId(2)), 2);
    }

    #[test]
    fn test_storage_limit_rejects() {
        let gate = TenantQuotaGate::new(Arc::new(StaticSource {
            snapshot: Some(QuotaSnapshot {
                storage_limit_bytes: Some(50),
                ..Default::default()
            }),
        }));

        // 单点估算 8 + 8 + 1 + 16 = 33 字节 < 50；两点 66 > 50。
        assert_eq!(
            gate.admit(TenantId(1), &batch(1, TenantId(1))),
            QuotaDecision::Admit
        );
        assert_eq!(
            gate.admit(TenantId(1), &batch(1, TenantId(1))),
            QuotaDecision::Reject {
                reason: QuotaRejectReason::QuotaExceeded
            }
        );
    }

    #[test]
    fn test_warn_ratio_returns_warn() {
        let gate = TenantQuotaGate::new(Arc::new(StaticSource {
            snapshot: Some(QuotaSnapshot {
                points_limit: Some(10),
                warn_ratio: 0.5,
                ..Default::default()
            }),
        }));

        // 4/10 = 0.4 < 0.5 → Admit。
        assert_eq!(
            gate.admit(TenantId(1), &batch(4, TenantId(1))),
            QuotaDecision::Admit
        );
        // 5/10 = 0.5 >= 0.5 → Warn。
        assert_eq!(
            gate.admit(TenantId(1), &batch(1, TenantId(1))),
            QuotaDecision::Warn { ratio: 0.5 }
        );
    }

    #[test]
    fn test_rate_limit_token_bucket() {
        let gate = TenantQuotaGate::new(Arc::new(StaticSource {
            snapshot: Some(QuotaSnapshot {
                write_rate_limit: Some(1), // 1 point/s
                points_limit: Some(100),
                ..Default::default()
            }),
        }));

        // 首次消费 1 点成功（桶初始满 1）。
        assert_eq!(
            gate.admit(TenantId(1), &batch(1, TenantId(1))),
            QuotaDecision::Admit
        );
        // 立即再写 1 点 → 令牌不足 → RateLimited。
        assert_eq!(
            gate.admit(TenantId(1), &batch(1, TenantId(1))),
            QuotaDecision::Reject {
                reason: QuotaRejectReason::RateLimited
            }
        );
    }
}

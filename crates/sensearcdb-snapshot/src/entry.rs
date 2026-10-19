//! SnapshotEntry — 实时快照数据库的单个快照条目。
//!
//! 每个 SnapshotEntry 对应一个 tag (table_id) 的最新状态。
//! 多类型化后，value 采用多列带类型模型：
//! `values: Vec<(u16, TypedValue)>` + `qualities: Vec<(u16, u8)>`（column_id 升序）。
//!
//! 单列测点统一多列模型（单列即 `values.len() == 1`），通过
//! [`SnapshotEntry::value`] / [`SnapshotEntry::quality`] 提供首列便捷访问器。

use sensearcdb_common::{TenantId, TypedValue};
use serde::{Deserialize, Serialize};

/// 快照条目：tag 的最新实时状态。
///
/// 每个 `tag_id` (即 `table_id`) 对应一个 `SnapshotEntry`，
/// 由 [`ColumnarShard`](crate::columnar::ColumnarShard) 的列式行投影产出（兼容投影类型）。
///
/// ## 字段说明
///
/// - `values`: 最新值（多列带类型，column_id 升序）。
/// - `qualities`: 每列 OPC UA QQSSSSLL 质量码 u8 编码（与 values 对齐）。
/// - `ts`: 源时间戳 (SourceTimestamp)，epoch 纳秒。
/// - `server_ts`: 服务器接收时间戳 (ServerTimestamp)，epoch 纳秒。**必填 i64，非 Option**。
///   双用途：(1) 诊断采集延迟 (2) AI 新鲜度门禁判断依据。
/// - `version`: 单调递增全局版本号。
/// - `prev_value` / `prev_ts`: 前值追踪（内部状态，下游禁止依赖）。
///   `prev_value` 多类型化后降级为首列前值。
/// - `is_derived`: 是否为派生标签（公式计算产出）。
/// - `writer_session_id`: 写入来源的内部 session_id（递归防护）。
/// - `ttl_expired`: TTL 过期标记。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    /// 租户 ID（0 = 默认租户；S1 恒 0）。
    #[serde(default)]
    pub tenant_id: TenantId,
    /// 最新值（多列带类型，column_id 升序）。
    pub values: Vec<(u16, TypedValue)>,
    /// 每列 OPC UA 质量码 (QQSSSSLL)，与 `values` 对齐。
    pub qualities: Vec<(u16, u8)>,
    /// 源时间戳 (SourceTimestamp, epoch ns)。
    pub ts: i64,
    /// 服务器时间戳 (ServerTimestamp, epoch ns)——必填 i64。
    ///
    /// 双用途：
    ///   1. 诊断采集延迟：当 |server_ts - ts| > threshold 时质量码降级。
    ///   2. AI 新鲜度门禁：AI 模块据此判断数据是否足够新鲜用于推理。
    pub server_ts: i64,
    /// 单调递增版本号。
    pub version: u64,
    /// 前值 (SnapshotStore 内部状态, 下游禁止依赖)——降级为首列前值。
    pub prev_value: Option<TypedValue>,
    /// 前值时间戳。
    pub prev_ts: Option<i64>,
    /// 是否派生标签。
    pub is_derived: bool,
    /// 写入来源 session_id (递归防护)——内部 u64。
    ///
    /// 生成方式：启动时分配单调递增 u64 session_id，
    /// 或 FNV-1a hash 外部 UUID → u64。
    pub writer_session_id: Option<u64>,
    /// TTL 过期标记。
    pub ttl_expired: bool,
    /// 列级源时间戳 (SourceTimestamp)，与 `values` 对齐（column_id 升序）。
    ///
    /// 每个 `(table_id, column_id)` 独立持有 source timestamp（OPC UA `DataValue`
    /// item 级三元组）。`ts` 兼容字段 = `max(ts_values)`。
    pub ts_values: Vec<(u16, i64)>,
    /// 列级服务器接收时间戳 (ServerTimestamp)，与 `values` 对齐（column_id 升序）。
    ///
    /// `server_ts` 兼容字段 = `max(server_ts_values)`。
    pub server_ts_values: Vec<(u16, i64)>,
    /// 列级写入序号（write_seq，LWW 决胜键），与 `values` 对齐（column_id 升序）。
    ///
    /// `None` = 无序号（遗留快照 / 跨节点迁移 / 批量导入 / SAC 重建 / compaction 输出）。
    /// 同 source_ts 时按 `write_seq 大者胜（Some > None）→ server_ts 大者胜` 决胜。
    /// `#[serde(default)]` 保证旧快照反序列化缺字段时按空 Vec（= 全 None）处理。
    #[serde(default)]
    pub write_seqs: Vec<(u16, Option<u64>)>,
}

impl SnapshotEntry {
    /// 创建新的多列带类型快照条目。
    ///
    /// 调用方保证 `values` / `qualities` 均按 column_id 升序排列。
    #[inline]
    pub fn new_typed(
        tenant_id: TenantId,
        values: Vec<(u16, TypedValue)>,
        qualities: Vec<(u16, u8)>,
        ts: i64,
        server_ts: i64,
        version: u64,
        is_derived: bool,
        session_id: Option<u64>,
    ) -> Self {
        // 整行重建语义：本次写入的所有列共享同一 row ts / server_ts，派生为列级时间戳。
        let ts_values: Vec<(u16, i64)> = values.iter().map(|(c, _)| (*c, ts)).collect();
        let server_ts_values: Vec<(u16, i64)> =
            values.iter().map(|(c, _)| (*c, server_ts)).collect();
        Self {
            tenant_id,
            values,
            qualities,
            ts,
            server_ts,
            version,
            prev_value: None,
            prev_ts: None,
            is_derived,
            writer_session_id: session_id,
            ttl_expired: false,
            ts_values,
            server_ts_values,
            // 构造器不接收逐列 seq；空 Vec = 全列 None（写路径经 store.rs 直接构造带 seq 的条目）。
            write_seqs: Vec::new(),
        }
    }

    /// 创建带前值的多列快照条目。
    #[inline]
    pub fn with_prev_typed(
        tenant_id: TenantId,
        values: Vec<(u16, TypedValue)>,
        qualities: Vec<(u16, u8)>,
        ts: i64,
        server_ts: i64,
        version: u64,
        is_derived: bool,
        session_id: Option<u64>,
        prev_value: Option<TypedValue>,
        prev_ts: Option<i64>,
    ) -> Self {
        // 整行重建语义：本次写入的所有列共享同一 row ts / server_ts，派生为列级时间戳。
        let ts_values: Vec<(u16, i64)> = values.iter().map(|(c, _)| (*c, ts)).collect();
        let server_ts_values: Vec<(u16, i64)> =
            values.iter().map(|(c, _)| (*c, server_ts)).collect();
        Self {
            tenant_id,
            values,
            qualities,
            ts,
            server_ts,
            version,
            prev_value,
            prev_ts,
            is_derived,
            writer_session_id: session_id,
            ttl_expired: false,
            ts_values,
            server_ts_values,
            // 构造器不接收逐列 seq；空 Vec = 全列 None。
            write_seqs: Vec::new(),
        }
    }

    /// 首列便捷访问器：返回第一列的 f64 视图（`as_f64`，String 列返回 NaN）。
    ///
    /// 用于下游尚未多类型化的 f64 消费方过渡；多列表请直接读取 `values`。
    #[inline]
    pub fn value(&self) -> f64 {
        self.values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0)
    }

    /// 首列便捷访问器：返回第一列的质量码。
    #[inline]
    pub fn quality(&self) -> u8 {
        self.qualities.first().map(|(_, q)| *q).unwrap_or(0)
    }

    /// 标记为 TTL 过期。
    #[inline]
    pub fn mark_ttl_expired(&mut self) {
        self.ttl_expired = true;
    }
}

impl Default for SnapshotEntry {
    fn default() -> Self {
        Self {
            tenant_id: TenantId::DEFAULT,
            values: Vec::new(),
            qualities: Vec::new(),
            ts: 0,
            server_ts: 0,
            version: 0,
            prev_value: None,
            prev_ts: None,
            is_derived: false,
            writer_session_id: None,
            ttl_expired: false,
            ts_values: Vec::new(),
            server_ts_values: Vec::new(),
            write_seqs: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_new() {
        let entry = SnapshotEntry::new_typed(
            sensearcdb_common::TenantId::DEFAULT,
            vec![(0, TypedValue::F64(42.5))],
            vec![(0, 0xC0)],
            1_000_000,
            1_000_001,
            7,
            false,
            None,
        );
        assert_eq!(entry.value(), 42.5);
        assert_eq!(entry.quality(), 0xC0);
        assert_eq!(entry.ts, 1_000_000);
        assert_eq!(entry.server_ts, 1_000_001);
        assert_eq!(entry.version, 7);
        assert!(!entry.is_derived);
        assert!(!entry.ttl_expired);
        assert!(entry.prev_value.is_none());
        assert_eq!(entry.values, vec![(0, TypedValue::F64(42.5))]);
    }

    #[test]
    fn test_entry_new_typed_derives_column_ts() {
        // 整行重建：所有列共享同一 row ts/server_ts，派生为列级时间戳。
        let entry = SnapshotEntry::new_typed(
            sensearcdb_common::TenantId::DEFAULT,
            vec![(0, TypedValue::F64(1.5)), (1, TypedValue::I64(42))],
            vec![(0, 0xC0), (1, 0xC0)],
            2_000,
            2_001,
            10,
            false,
            None,
        );
        assert_eq!(entry.ts_values, vec![(0, 2_000), (1, 2_000)]);
        assert_eq!(entry.server_ts_values, vec![(0, 2_001), (1, 2_001)]);
        assert_eq!(entry.ts, 2_000);
        assert_eq!(entry.server_ts, 2_001);
    }

    #[test]
    fn test_entry_new_typed_multi_column() {
        let entry = SnapshotEntry::new_typed(
            sensearcdb_common::TenantId::DEFAULT,
            vec![
                (0, TypedValue::F64(1.5)),
                (1, TypedValue::I64(42)),
                (2, TypedValue::String("open".into())),
            ],
            vec![(0, 0xC0), (1, 0xC0), (2, 0x40)],
            2_000,
            2_001,
            10,
            false,
            None,
        );
        // 首列 facade 兼容
        assert_eq!(entry.value(), 1.5);
        assert_eq!(entry.quality(), 0xC0);
        // 多列保真
        assert_eq!(entry.values.len(), 3);
        assert_eq!(entry.values[1], (1, TypedValue::I64(42)));
        assert_eq!(entry.values[2], (2, TypedValue::String("open".into())));
        assert_eq!(entry.qualities[2], (2, 0x40));
    }

    #[test]
    fn test_entry_with_prev() {
        let entry = SnapshotEntry::with_prev_typed(
            sensearcdb_common::TenantId::DEFAULT,
            vec![(0, TypedValue::F64(99.0))],
            vec![(0, 0xC0)],
            2_000,
            2_001,
            10,
            true,
            Some(42),
            Some(98.0).map(TypedValue::F64),
            Some(1_000),
        );
        assert_eq!(entry.value(), 99.0);
        assert_eq!(entry.prev_value, Some(TypedValue::F64(98.0)));
        assert_eq!(entry.prev_ts, Some(1_000));
        assert!(entry.is_derived);
    }

    #[test]
    fn test_entry_ttl_expired() {
        let mut entry = SnapshotEntry::default();
        assert!(!entry.ttl_expired);
        entry.mark_ttl_expired();
        assert!(entry.ttl_expired);
    }

    #[test]
    fn test_tenant_id_serde_roundtrip_and_default() {
        // 非零 tenant_id 必须被真实序列化并回读保留（S2 Step 4：new_typed 显式传 tenant 入参）。
        let e = SnapshotEntry::new_typed(
            TenantId(7),
            vec![(0, TypedValue::F64(1.0))],
            vec![(0, 0xC0)],
            1000,
            1001,
            7,
            false,
            None,
        );
        assert_eq!(e.tenant_id, TenantId(7));
        let json = serde_json::to_string(&e).unwrap();
        let back: SnapshotEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.tenant_id, TenantId(7));

        // 旧快照缺 tenant_id 字段 → #[serde(default)] → 默认租户 0（向后兼容）。
        // 注：列级时间戳字段（ts_values/server_ts_values）无 serde(default)，
        // 缺失时按「永不向前兼容」应报错，故此处显式补齐新字段以仅测 tenant_id 兼容。
        let missing = r#"{"values":[],"qualities":[],"ts":0,"server_ts":0,"version":0,"prev_value":null,"prev_ts":null,"is_derived":false,"writer_session_id":null,"ttl_expired":false,"ts_values":[],"server_ts_values":[]}"#;
        let legacy: SnapshotEntry = serde_json::from_str(missing).unwrap();
        assert_eq!(legacy.tenant_id, TenantId::DEFAULT);

        // 永不向前兼容：缺 ts_values/server_ts_values 的旧 JSON 必须反序列化失败。
        let legacy_missing_ts = r#"{"values":[],"qualities":[],"ts":0,"server_ts":0,"version":0,"prev_value":null,"prev_ts":null,"is_derived":false,"writer_session_id":null,"ttl_expired":false}"#;
        assert!(serde_json::from_str::<SnapshotEntry>(legacy_missing_ts).is_err());
    }
}

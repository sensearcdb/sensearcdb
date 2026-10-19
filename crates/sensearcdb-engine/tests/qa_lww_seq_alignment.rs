//! QA 独立验证（软件 QA 工程师 #3）—— 单机 `write_batch` 逐点 write_seq 对齐。
//!
//! 这是 LWW 改造「最容易错」的地方：批量写入按 durability 分流到 group/durable/async
//! 三档后，各子批 `alloc_seqs` 分配的逐点 seq 必须**精确对齐回原批中的那个点**，
//! 否则 SnapshotStore 会拿到「张冠李戴」的 write_seq，导致同 ts 决胜错乱。
//!
//! 验证点：
//! - 三档混批（group/durable/async 交错）下 `WriteResult.seqs` 长度 == 点数、全非零、全局唯一。
//! - 每个点经 `SnapshotUpdateSink::on_write` 收到的 `write_seq` 与 `WriteResult.seqs[i]`
//!   在该点下标处严格一致（即 sink 收到的 seq 精确绑定到正确的点）。
//! - 每个 durability 子批内部，seq 严格连续 +1（验证 alloc_seqs 逐点续号无 off-by-one）。

use std::sync::{Arc, Mutex};

use sensearcdb_common::{AppConfig, ConfigHandle, Durability, TenantId, TypedValue};
use sensearcdb_engine::engine::{
    DataPoint, DurabilityResolver, Engine, PointBatch, SnapshotUpdateSink,
};

/// 捕获 sink 收到的 (table_id, write_seq)，保持调用顺序。
struct CapturingSink {
    writes: Mutex<Vec<(u32, Option<u64>)>>,
}

impl SnapshotUpdateSink for CapturingSink {
    fn on_write(
        &self,
        _tenant_id: TenantId,
        table_id: u32,
        _values: &[(u16, TypedValue)],
        _qualities: &[(u16, u8)],
        _ts: i64,
        _server_ts: i64,
        write_seq: Option<u64>,
    ) {
        self.writes.lock().unwrap().push((table_id, write_seq));
    }
}

/// table_id % 3 → durability：0=Group、1=Durable、2=Async。
struct ModResolver;

impl DurabilityResolver for ModResolver {
    fn resolve(&self, table_id: u32) -> Durability {
        match table_id % 3 {
            0 => Durability::Group,
            1 => Durability::Durable,
            _ => Durability::Async,
        }
    }
}

fn point(table_id: u32, v: f64) -> DataPoint {
    DataPoint {
        table_id,
        tenant_id: TenantId::DEFAULT,
        ts: 100,
        server_ts: 200,
        values: vec![(0, TypedValue::F64(v))],
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    }
}

#[tokio::test]
async fn test_write_batch_per_point_seq_alignment() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    let handle = ConfigHandle::new(cfg);
    let engine = Engine::new(&handle, None).await.expect("engine");

    let sink = Arc::new(CapturingSink {
        writes: Mutex::new(Vec::new()),
    });
    engine.set_snapshot_sink(sink.clone());
    engine.set_durability_resolver(Arc::new(ModResolver));

    // 三档交错 + 非平凡顺序，制造「若 seq 张冠李戴必被暴露」的场景。
    // table_id % 3: 10=Group, 11=Durable, 12=Async, 13=Group, 14=Durable, 15=Async, ...
    let points: Vec<DataPoint> = (0..9)
        .map(|i| point(10 + i as u32, i as f64))
        .collect();

    let result = engine
        .write_batch(PointBatch {
            points: points.clone(),
        })
        .await
        .expect("mixed durability write must succeed");

    // ① 长度对齐。
    assert_eq!(result.written, points.len());
    assert_eq!(result.seqs.len(), points.len());

    // ② 全非零 + 全局唯一（alloc_seqs 区间无重叠）。
    assert!(
        result.seqs.iter().all(|&s| s > 0),
        "每个点都必须拿到真实 WAL seq（>0），seqs={:?}",
        result.seqs
    );
    let mut uniq = result.seqs.clone();
    uniq.sort_unstable();
    uniq.dedup();
    assert_eq!(uniq.len(), points.len(), "逐点 seq 必须全局唯一，不得撞车");

    // ③ sink 收到的 (table_id, write_seq) 逐点对齐（关键：不张冠李戴）。
    let writes = sink.writes.lock().unwrap();
    assert_eq!(writes.len(), points.len(), "sink 应收到与点数相同的回调");
    for (i, (table_id, seq)) in writes.iter().enumerate() {
        assert_eq!(
            *table_id, points[i].table_id,
            "第 {i} 个 sink 回调的 table_id 必须等于 batch.points[{i}].table_id"
        );
        assert_eq!(
            *seq,
            Some(result.seqs[i]),
            "第 {i} 个点（table_id={}）收到的 write_seq 必须等于 WriteResult.seqs[{i}]，不张冠李戴",
            points[i].table_id
        );
    }

    // ④ 每个 durability 子批内部 seq 严格连续 +1（alloc_seqs 逐点续号无 off-by-one）。
    for tier in [0u32, 1u32, 2u32] {
        let tier_seq: Vec<u64> = points
            .iter()
            .enumerate()
            .filter(|(_, p)| p.table_id % 3 == tier)
            .map(|(i, _)| result.seqs[i])
            .collect();
        assert!(!tier_seq.is_empty(), "每个 durability 档都应有至少一个点");
        for w in tier_seq.windows(2) {
            assert_eq!(
                w[1],
                w[0] + 1,
                "同一 durability 子批内 seq 必须连续 +1（tier={tier}），否则续号错位"
            );
        }
    }
}

/// 单档（纯 durable）多点的 seq 也必须逐点连续 + 对齐（覆盖 write_durable 的
/// PendingEntry 保存/ACK 回传路径，避免 ACK 后 seqs 顺序错乱）。
#[tokio::test]
async fn test_write_batch_pure_durable_seq_alignment() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    let handle = ConfigHandle::new(cfg);
    let engine = Engine::new(&handle, None).await.expect("engine");

    let sink = Arc::new(CapturingSink {
        writes: Mutex::new(Vec::new()),
    });
    engine.set_snapshot_sink(sink.clone());

    // 全部 table_id % 3 == 1 → 全走 durable 子批（单一子批，多点多表）。
    struct DurableOnlyResolver;
    impl DurabilityResolver for DurableOnlyResolver {
        fn resolve(&self, _table_id: u32) -> Durability {
            Durability::Durable
        }
    }
    engine.set_durability_resolver(Arc::new(DurableOnlyResolver));

    let points: Vec<DataPoint> = (0..5).map(|i| point(101 + i as u32, i as f64)).collect();
    let result = engine
        .write_batch(PointBatch {
            points: points.clone(),
        })
        .await
        .expect("pure durable write must succeed");

    assert_eq!(result.seqs.len(), points.len());
    assert!(result.seqs.iter().all(|&s| s > 0));
    for w in result.seqs.windows(2) {
        assert_eq!(w[1], w[0] + 1, "纯 durable 子批 seq 必须连续 +1");
    }

    let writes = sink.writes.lock().unwrap();
    assert_eq!(writes.len(), points.len());
    for (i, (table_id, seq)) in writes.iter().enumerate() {
        assert_eq!(*table_id, points[i].table_id);
        assert_eq!(*seq, Some(result.seqs[i]));
    }
}

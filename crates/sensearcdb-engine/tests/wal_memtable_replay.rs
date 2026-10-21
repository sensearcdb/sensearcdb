//! 集成测试：启动时回放 WAL 重建 MemTable（方案 A，`Engine::recover_memtable_from_wal`）。
//!
//! 覆盖三条硬约束与数据正确性：
//! - 回放只写 MemTable、不写 WAL；
//! - 回放绝不 fanout snapshot sink；
//! - 逐点 seq 重建（帧首点 seq = `raft_index`，帧内下标连续）回填 chunk `min/max_wal_seq`。
//!
//! 测试套路：Run1 建 Engine 写数据（WAL + MemTable）→ 置 SAC 落盘水位 S（模拟已 flush）
//! → Run2 在同一 `data_dir` 重建 Engine（空 MemTable）→ 调 `recover_memtable_from_wal`
//! 断言（帧数、点数、ts 命中、min/max seq、幂等）。

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::encryption::{EncryptContext, EncryptError, Encryptor, SharedEncryptor};
use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch, SnapshotUpdateSink};
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_engine::TenantId;

/// 构造一个使用临时 `data_dir` 的 `ConfigHandle`。
fn temp_handle() -> (ConfigHandle, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    (ConfigHandle::new(cfg), tmp)
}

/// 构造单列（col 0 = F64）数据点。
fn make_point(table_id: u32, ts: i64, v: f64) -> DataPoint {
    DataPoint {
        table_id,
        tenant_id: TenantId::DEFAULT,
        ts,
        server_ts: ts + 1,
        values: vec![(0u16, TypedValue::F64(v))],
        qualities: vec![(0u16, 0xC0)],
        schema_version: 1,
        flags: 0,
    }
}

/// 统计 `wal_dir` 下 `.wal` 文件数与总字节数。
fn wal_stats(wal_dir: &Path) -> (usize, u64) {
    let mut count = 0usize;
    let mut bytes = 0u64;
    if let Ok(entries) = std::fs::read_dir(wal_dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_file() {
                count += 1;
                bytes += p.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    (count, bytes)
}

/// XOR 型加密桩（复用 `qa_tde_roundtrip.rs` 的 `TestEncryptor` 语义）。
struct TestEncryptor {
    version: u32,
    enabled: bool,
}

impl Encryptor for TestEncryptor {
    fn encrypt(&self, plaintext: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        let mut out = Vec::with_capacity(28 + plaintext.len());
        out.extend_from_slice(&[0xA1u8; 12]); // nonce
        out.extend_from_slice(&[0xB2u8; 16]); // tag
        for b in plaintext {
            out.push(b ^ 0x5A);
        }
        Ok(out)
    }

    fn decrypt(&self, blob: &[u8], _ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        if blob.len() < 28 {
            return Err(EncryptError::TooShort(blob.len(), 28));
        }
        Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
    }

    fn active_dek_version(&self, _tenant_id: TenantId) -> u32 {
        self.version
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

fn enc() -> SharedEncryptor {
    Some(Arc::new(TestEncryptor {
        version: 1,
        enabled: true,
    }))
}

/// 计数 mock sink：记录 `on_write` / `on_write_batch` 调用次数。
struct CountingSink {
    writes: AtomicUsize,
    batch_writes: AtomicUsize,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            writes: AtomicUsize::new(0),
            batch_writes: AtomicUsize::new(0),
        }
    }
}

impl SnapshotUpdateSink for CountingSink {
    fn on_write(
        &self,
        _tenant_id: TenantId,
        _table_id: u32,
        _values: &[(u16, TypedValue)],
        _qualities: &[(u16, u8)],
        _ts: i64,
        _server_ts: i64,
        _write_seq: Option<u64>,
    ) {
        self.writes.fetch_add(1, Ordering::SeqCst);
    }

    fn on_write_batch(&self, points: &[DataPoint], seqs: &[u64]) {
        self.batch_writes.fetch_add(1, Ordering::SeqCst);
        for (point, seq) in points.iter().zip(seqs.iter()) {
            self.on_write(
                point.tenant_id,
                point.table_id,
                &point.values,
                &point.qualities,
                point.ts,
                point.server_ts,
                Some(*seq),
            );
        }
    }
}

/// 1. 单帧 3 点：逐点 seq = 1,2,3 回填 chunk `min_wal_seq`/`max_wal_seq` 正确。
#[tokio::test]
async fn test_replay_seq_reconstruction_multi_point_frame() {
    let (handle, _tmp) = temp_handle();

    {
        let engine = Engine::new(&handle, None).await.expect("Run1 Engine");
        engine
            .write_batch(PointBatch {
                points: vec![
                    make_point(1, 100, 1.0),
                    make_point(1, 200, 2.0),
                    make_point(1, 300, 3.0),
                ],
            })
            .await
            .expect("write_batch Run1");
    }

    let engine = Engine::new(&handle, None).await.expect("Run2 Engine");
    let (frames, points) = engine.recover_memtable_from_wal().expect("replay");
    assert_eq!((frames, points), (1, 3), "一 batch 3 点 → 1 帧 3 点");

    let snapshot = engine.memtable.active_snapshot();
    let chunk = snapshot.get_chunk(1).expect("chunk should exist");
    assert_eq!(chunk.min_wal_seq, Some(1), "首点 seq 应为 1");
    assert_eq!(chunk.max_wal_seq, Some(3), "末点 seq 应为 3（逐点连续）");
}

/// 2. 回放不写 WAL：`.wal` 文件数与总字节数在回放前后不变。
#[tokio::test]
async fn test_replay_does_not_write_wal() {
    let (handle, _tmp) = temp_handle();

    {
        let engine = Engine::new(&handle, None).await.expect("Run1 Engine");
        engine
            .write_batch(PointBatch {
                points: vec![make_point(1, 100, 1.0), make_point(1, 200, 2.0)],
            })
            .await
            .expect("write_batch Run1");
    }

    let wal_dir = handle.get().paths.wal_dir();
    let engine = Engine::new(&handle, None).await.expect("Run2 Engine");
    let before = wal_stats(&wal_dir);
    let (_frames, _points) = engine.recover_memtable_from_wal().expect("replay");
    let after = wal_stats(&wal_dir);
    assert_eq!(before, after, "回放期间 WAL 文件数与字节数不得变化");
}

/// 3. 回放绝不 fanout snapshot sink：注入计数 mock 后回放，on_write / on_write_batch 均为 0。
#[tokio::test]
async fn test_replay_does_not_fanout_snapshot_sink() {
    let (handle, _tmp) = temp_handle();

    {
        let engine = Engine::new(&handle, None).await.expect("Run1 Engine");
        engine
            .write_batch(PointBatch {
                points: vec![make_point(1, 100, 1.0), make_point(1, 200, 2.0)],
            })
            .await
            .expect("write_batch Run1");
    }

    let engine = Engine::new(&handle, None).await.expect("Run2 Engine");
    let sink = Arc::new(CountingSink::new());
    engine.set_snapshot_sink(sink.clone());

    let (frames, points) = engine.recover_memtable_from_wal().expect("replay");
    assert_eq!((frames, points), (1, 2), "回放应恢复 1 帧 2 点");

    assert_eq!(
        sink.writes.load(Ordering::SeqCst),
        0,
        "回放不得触发 on_write"
    );
    assert_eq!(
        sink.batch_writes.load(Ordering::SeqCst),
        0,
        "回放不得触发 on_write_batch"
    );
}

/// 4. S=0：写 3 帧（raft_index 1/2/3），回放返回 (3,3)，3 个 ts 全命中。
#[tokio::test]
async fn test_replay_full_when_s_zero() {
    let (handle, _tmp) = temp_handle();

    {
        let engine = Engine::new(&handle, None).await.expect("Run1 Engine");
        for (i, ts) in [100i64, 200, 300].iter().enumerate() {
            engine
                .write_batch(PointBatch {
                    points: vec![make_point(1, *ts, (i + 1) as f64)],
                })
                .await
                .expect("write_batch Run1");
        }
    }

    let engine = Engine::new(&handle, None).await.expect("Run2 Engine");
    let (frames, points) = engine.recover_memtable_from_wal().expect("replay");
    assert_eq!((frames, points), (3, 3), "S=0 应回放全部 3 帧 3 点");

    let snapshot = engine.memtable.active_snapshot();
    let chunk = snapshot.get_chunk(1).expect("chunk should exist");
    assert_eq!(chunk.unique_ts_count(), 3, "3 个 ts 应全部命中");
    assert_eq!(
        chunk.get_range(0, i64::MAX),
        vec![(100, 0), (200, 1), (300, 2)]
    );
}

/// 5. S>0：Run1 写 3 点后 `advance_to(1)`，Run2 回放返回 (2,2)，仅 raft_index>1 的 2 点命中。
#[tokio::test]
async fn test_replay_tail_only_when_s_positive() {
    let (handle, _tmp) = temp_handle();
    let sac_dir = handle.get().paths.sac_dir();
    std::fs::create_dir_all(&sac_dir).expect("create sac_dir for watermark");

    {
        let engine = Engine::new(&handle, None).await.expect("Run1 Engine");
        for (i, ts) in [100i64, 200, 300].iter().enumerate() {
            engine
                .write_batch(PointBatch {
                    points: vec![make_point(1, *ts, (i + 1) as f64)],
                })
                .await
                .expect("write_batch Run1");
        }
        // 模拟前 1 帧已 flush：推进 SAC 落盘水位到 1。
        engine.sac_watermark().advance_to(1).expect("advance_to(1)");
    }

    let engine = Engine::new(&handle, None).await.expect("Run2 Engine");
    assert_eq!(engine.sac_watermark().current(), 1, "Run2 应读回水位 1");
    let (frames, points) = engine.recover_memtable_from_wal().expect("replay");
    assert_eq!((frames, points), (2, 2), "仅回放 raft_index>1 的 2 帧");

    let snapshot = engine.memtable.active_snapshot();
    let chunk = snapshot.get_chunk(1).expect("chunk should exist");
    assert_eq!(chunk.unique_ts_count(), 2, "仅 2 个 ts 命中");
    assert_eq!(chunk.get_range(0, i64::MAX), vec![(200, 0), (300, 1)]);
    assert!(
        chunk.get_range(100, 100).is_empty(),
        "已落盘 ts=100 不得被回放"
    );
}

/// 6. 空 WAL 目录：回放返回 (0,0)，不 panic。
#[tokio::test]
async fn test_replay_empty_wal_dir() {
    let (handle, _tmp) = temp_handle();
    let engine = Engine::new(&handle, None).await.expect("Engine");
    let (frames, points) = engine.recover_memtable_from_wal().expect("replay");
    assert_eq!((frames, points), (0, 0), "空 WAL 目录应回放 0 帧 0 点");
}

/// 7. TDE 关闭：`Engine::new(&h, None)` 两端，回放点数与写入一致。
#[tokio::test]
async fn test_replay_tde_off() {
    let (handle, _tmp) = temp_handle();

    {
        let engine = Engine::new(&handle, None).await.expect("Run1 Engine");
        engine
            .write_batch(PointBatch {
                points: vec![make_point(7, 100, 1.0), make_point(7, 200, 2.0)],
            })
            .await
            .expect("write_batch Run1");
    }

    let engine = Engine::new(&handle, None).await.expect("Run2 Engine");
    let (frames, points) = engine.recover_memtable_from_wal().expect("replay");
    assert_eq!((frames, points), (1, 2), "TDE 关：回放点数应与写入一致");
}

/// 8. TDE 开启：`Engine::new(&h, Some(encryptor))` 两端，密文帧解密后点数一致。
#[tokio::test]
async fn test_replay_tde_on() {
    let (handle, _tmp) = temp_handle();

    {
        let engine = Engine::new(&handle, enc()).await.expect("Run1 Engine");
        engine
            .write_batch(PointBatch {
                points: vec![make_point(8, 100, 1.0), make_point(8, 200, 2.0)],
            })
            .await
            .expect("write_batch Run1");
    }

    let engine = Engine::new(&handle, enc()).await.expect("Run2 Engine");
    let (frames, points) = engine.recover_memtable_from_wal().expect("replay");
    assert_eq!((frames, points), (1, 2), "TDE 开：密文帧解密后点数一致");

    let snapshot = engine.memtable.active_snapshot();
    let chunk = snapshot.get_chunk(8).expect("chunk should exist");
    assert_eq!(chunk.unique_ts_count(), 2);
}

/// 9. 幂等：连续回放 2 次，active_snapshot 查询返回唯一 ts 数 == 写入数（LWW 去重，不翻倍）。
#[tokio::test]
async fn test_replay_idempotent() {
    let (handle, _tmp) = temp_handle();

    {
        let engine = Engine::new(&handle, None).await.expect("Run1 Engine");
        for (i, ts) in [100i64, 200, 300].iter().enumerate() {
            engine
                .write_batch(PointBatch {
                    points: vec![make_point(1, *ts, (i + 1) as f64)],
                })
                .await
                .expect("write_batch Run1");
        }
    }

    let engine = Engine::new(&handle, None).await.expect("Run2 Engine");
    let (f1, p1) = engine.recover_memtable_from_wal().expect("replay #1");
    assert_eq!((f1, p1), (3, 3), "首次回放 3 帧 3 点");
    let (f2, p2) = engine.recover_memtable_from_wal().expect("replay #2");
    assert_eq!((f2, p2), (3, 3), "二次回放同样读到 3 帧 3 点");

    let snapshot = engine.memtable.active_snapshot();
    let chunk = snapshot.get_chunk(1).expect("chunk should exist");
    assert_eq!(
        chunk.unique_ts_count(),
        3,
        "幂等：唯一 ts 数应仍为写入数 3，不翻倍"
    );
}

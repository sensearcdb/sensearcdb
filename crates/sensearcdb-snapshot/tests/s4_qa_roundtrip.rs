//! S4（Arrow IPC File + SM4-GCM）独立 QA 边界测试。
//!
//! 由独立 QA 编写，不复用 serialize.rs / recovery.rs 内的工程师自测桩。
//! 覆盖三条红线：
//!   1. write_seq 混合 None / Some(0) / Some(u64::MAX) 经 IPC 往返逐点保真；
//!   2. 加密序列化 → 以不同 shard_count 解密反序列化 → 条目不丢 + write_seq 保真；
//!   3. 两次独立构造（乱序 vs 升序）to_record_batch + IPC 序列化，字节级一致。

use std::io::Cursor;

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use sensearcdb_common::{TenantId, TypedValue};
use sensearcdb_snapshot::{
    deserialize_from_file, serialize_to_file, ColumnarShard, SnapshotEncryptError, SnapshotEncryptor,
    SnapshotEntry, SnapshotMetrics,
};

/// 构造单列（column_id = col）带指定 write_seq 的 SnapshotEntry。
fn entry_with_seq(table_id: u32, col: u16, seq: Option<u64>, value: f64) -> SnapshotEntry {
    SnapshotEntry {
        tenant_id: TenantId::DEFAULT,
        values: vec![(col, TypedValue::F64(value))],
        qualities: vec![(col, 0xC0)],
        ts: 1000,
        server_ts: 1001,
        version: table_id as u64,
        prev_value: None,
        prev_ts: None,
        is_derived: false,
        writer_session_id: None,
        ttl_expired: false,
        ts_values: vec![(col, 1000)],
        server_ts_values: vec![(col, 1001)],
        write_seqs: vec![(col, seq)],
    }
}

/// 把 RecordBatch 序列化为 Arrow IPC File 字节（同 serialize.rs 的 payload 生成路径）。
fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut ipc = FileWriter::try_new(&mut cursor, batch.schema().as_ref()).unwrap();
        ipc.write(batch).unwrap();
        ipc.finish().unwrap();
    }
    cursor.into_inner()
}

/// 从 Arrow IPC File 字节读回 RecordBatch。
fn read_ipc(bytes: &[u8]) -> RecordBatch {
    let reader = FileReader::try_new(Cursor::new(bytes.to_vec()), None).unwrap();
    reader.into_iter().next().unwrap().unwrap()
}

/// XOR 型加密桩（独立实现，不依赖工程师自测桩），产出 nonce(12)||tag(16)||ct。
struct QaXorEncryptor;

impl SnapshotEncryptor for QaXorEncryptor {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, SnapshotEncryptError> {
        let mut out = Vec::with_capacity(28 + plaintext.len());
        out.extend_from_slice(&[0xA1u8; 12]);
        out.extend_from_slice(&[0xB2u8; 16]);
        out.extend(plaintext.iter().map(|b| b ^ 0x5A));
        Ok(out)
    }

    fn decrypt(&self, blob: &[u8], _dek_version: u32) -> Result<Vec<u8>, SnapshotEncryptError> {
        if blob.len() < 28 {
            return Err(SnapshotEncryptError::TooShort(blob.len(), 28));
        }
        Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
    }

    fn active_dek_version(&self) -> u32 {
        1
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

/// 红线 1：同一 shard 内 write_seq 混 None / Some(0) / Some(u64::MAX)，
/// 经 to_record_batch → Arrow IPC File → from_record_batch 逐点区分，绝不 None→Some(0)。
#[test]
fn s4_qa_write_seq_none_zero_max_ipc_roundtrip() {
    let mut shard = ColumnarShard::new();
    // 三个 table 同 column 0，write_seq 分别 None / Some(0) / Some(u64::MAX)。
    shard.upsert_from_entry(1, entry_with_seq(1, 0, None, 1.0));
    shard.upsert_from_entry(2, entry_with_seq(2, 0, Some(0), 2.0));
    shard.upsert_from_entry(3, entry_with_seq(3, 0, Some(u64::MAX), 3.0));

    let batch = shard.to_record_batch();
    let bytes = ipc_bytes(&batch);
    let restored = ColumnarShard::from_record_batch(&read_ipc(&bytes));

    assert_eq!(
        restored.get(1).unwrap().write_seqs,
        vec![(0, None)],
        "None 不得变 Some(0)"
    );
    assert_eq!(
        restored.get(2).unwrap().write_seqs,
        vec![(0, Some(0))],
        "Some(0) 不得变 None"
    );
    assert_eq!(
        restored.get(3).unwrap().write_seqs,
        vec![(0, Some(u64::MAX))],
        "Some(u64::MAX) 不得截断"
    );
    // 值列也保真。
    assert_eq!(restored.get(1).unwrap().values, vec![(0, TypedValue::F64(1.0))]);
    assert_eq!(restored.get(3).unwrap().values, vec![(0, TypedValue::F64(3.0))]);
}

/// 红线 2：加密序列化（4 桶）→ 以 16 桶解密反序列化 → 条目按 fxhash % 16 重分桶、
/// 不丢条目、write_seq 保真。
#[test]
fn s4_qa_encrypted_serialize_rehash_shard_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("enc_rehash.bin");
    let metrics = SnapshotMetrics::noop();
    let enc = QaXorEncryptor;

    // 旧文件：4 桶，100 条目，write_seq 按 table_id 三态混合。
    let old_locks: Vec<parking_lot::RwLock<ColumnarShard>> = (0..4)
        .map(|_| parking_lot::RwLock::new(ColumnarShard::new()))
        .collect();
    for i in 1u32..=100u32 {
        let seq = match i % 3 {
            0 => None,
            1 => Some(0),
            _ => Some(i as u64),
        };
        let idx = fxhash::hash(&i) as usize % 4;
        old_locks[idx]
            .write()
            .upsert_from_entry(i, entry_with_seq(i, 0, seq, i as f64));
    }
    let guards: Vec<_> = old_locks.iter().map(|l| l.read()).collect();
    serialize_to_file(&path, &guards, 7, &metrics, Some(&enc), 0).unwrap();
    drop(guards);

    // 升级：16 桶 + 解密恢复。
    let (restored, version, _wm) =
        deserialize_from_file(&path, 16, &metrics, Some(&enc)).unwrap();
    assert_eq!(version, 7);
    assert_eq!(restored.len(), 16);

    let mut total = 0usize;
    for (bucket_idx, bucket) in restored.iter().enumerate() {
        for tid in bucket.iter_table_ids() {
            assert_eq!(
                fxhash::hash(&tid) as usize % 16,
                bucket_idx,
                "table_id {} 未重放到正确桶（数据错位）",
                tid
            );
            let want_seq = match tid % 3 {
                0 => None,
                1 => Some(0),
                _ => Some(tid as u64),
            };
            assert_eq!(
                bucket.get(tid).unwrap().write_seqs,
                vec![(0, want_seq)],
                "table_id {} write_seq 经加密+rehash 后不保真",
                tid
            );
            total += 1;
        }
    }
    assert_eq!(total, 100, "加密 + shard_count 变更后不得丢条目");
}

/// 红线 3：确定性——两次独立构造（升序 vs 乱序插入，数据相同）的
/// to_record_batch + IPC 序列化字节级一致，证明显式排序而非依赖插入序。
#[test]
fn s4_qa_deterministic_ipc_bytes_cross_construction() {
    let mut s_asc = ColumnarShard::new();
    let mut s_desc = ColumnarShard::new();
    s_asc.shard_version = 42;
    s_desc.shard_version = 42;

    // shard A：升序 table_id 插入（table 5 / 7 / 10，各一列，write_seq 覆盖 None / Some(0) / Some(MAX)）。
    s_asc.upsert_from_entry(5, entry_with_seq(5, 0, Some(0), 2.0));
    s_asc.upsert_from_entry(7, entry_with_seq(7, 1, Some(u64::MAX), 3.0));
    s_asc.upsert_from_entry(10, entry_with_seq(10, 0, None, 1.0));

    // shard B：乱序 table_id 插入（模拟不同进程/到达顺序），最终数据相同。
    s_desc.upsert_from_entry(10, entry_with_seq(10, 0, None, 1.0));
    s_desc.upsert_from_entry(7, entry_with_seq(7, 1, Some(u64::MAX), 3.0));
    s_desc.upsert_from_entry(5, entry_with_seq(5, 0, Some(0), 2.0));

    let b1 = ipc_bytes(&s_asc.to_record_batch());
    let b2 = ipc_bytes(&s_desc.to_record_batch());

    assert_eq!(b1, b2, "乱序构造不得产生不同 IPC 字节（确定性排序缺失）");

    // 附带验证：往返后条目集一致（3 个 table），write_seq 逐点保真。
    let restored = ColumnarShard::from_record_batch(&read_ipc(&b1));
    assert_eq!(restored.entry_count(), 3);
    assert_eq!(restored.get(5).unwrap().write_seqs, vec![(0, Some(0))]);
    assert_eq!(
        restored.get(7).unwrap().write_seqs,
        vec![(1, Some(u64::MAX))]
    );
    assert_eq!(restored.get(10).unwrap().write_seqs, vec![(0, None)]);
}

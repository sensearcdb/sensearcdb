//! QA 独立验证（qa-p2-wal-lock）：P2 WAL 段锁优化（锁外预计算 + 锁内收口）的正确性。
//!
//! 不信任工程师自测，本文件独立走真实文件 I/O 路径，验证：
//! - C1 跨重启 round-trip（写 → 正常关闭 → 重建 → 重放读回一致）
//! - C2 崩溃半帧（写一半 kill 的等价物：尾部半帧 → 重放只跳过半帧、不 panic）
//! - C3 durable 档 ACK 语义（fsync 后才 ACK，用 committed_watermark 推进作为 fsync 已发生的证据）
//! - C4 group/async/durable 三档混写，重放后三档数据都正确
//! - C5 乱序写入 + 锁外预加密，重放后按 seq（raft_index）重建逻辑顺序
//! - C6（I6 加密语义）锁外 prepare_frame 以 (0,0) 加密，reader 以 (seg_seq,offset) 解密，
//!   通过「断言 encrypt/decrypt 收到的 aad 恒空」锁定 AAD 恒空依赖，密文逐字节位置无关。

use std::sync::{Arc, Mutex};

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::encryption::{EncryptContext, EncryptError, Encryptor, SharedEncryptor};
use sensearcdb_engine::engine::{DataPoint, PointBatch};
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_engine::wal::{WalReader, WalWriter};
use sensearcdb_engine::TenantId;

fn temp_wal_config() -> (ConfigHandle, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    (ConfigHandle::new(cfg), tmp)
}

fn make_point(table_id: u32, ts: i64) -> DataPoint {
    DataPoint {
        table_id,
        tenant_id: TenantId::DEFAULT,
        ts,
        server_ts: ts + 1,
        values: vec![(0u16, TypedValue::F64(ts as f64))],
        qualities: vec![(0u16, 0xC0)],
        schema_version: 1,
        flags: 0,
    }
}

// ─── C1：跨重启 round-trip ─────────────────────────────────────────────────

#[tokio::test]
async fn qa_p2_cross_restart_roundtrip() {
    let (cfg, _tmp) = temp_wal_config();

    // Run1：一批 100 点（一帧 raft_index=1，点级 seq 1..100）落盘。
    {
        let wal = WalWriter::new(&cfg).expect("create WalWriter (Run1)");
        let points: Vec<DataPoint> = (0..100).map(|i| make_point(1, i as i64)).collect();
        let wr = wal
            .write_batch(PointBatch { points })
            .await
            .expect("write_batch Run1");
        assert_eq!(wr.wal_seq_start, 1);
        assert_eq!(wal.current_seq(), 101);
    }

    // Run2：重建 WalWriter + WalReader 重放，读回全部 100 点，值与写入一致。
    let wal = WalWriter::new(&cfg).expect("create WalWriter (Run2)");
    assert_eq!(wal.current_seq(), 101, "跨重启 seq 续号必须一致");

    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new(wal_dir);
    let records = reader.replay_all(1).expect("replay_all");
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 100, "应恢复全部 100 点");
    // 逐点比对：ts==i → value==i（f64 精确整数）。
    for (i, p) in recovered.iter().enumerate() {
        assert_eq!(p.table_id, 1);
        assert_eq!(p.ts, i as i64);
        assert_eq!(p.values[0], (0u16, TypedValue::F64(i as f64)));
    }
}

// ─── C2：崩溃半帧（尾部截断帧）不 panic、只跳过半帧 ─────────────────────────

#[tokio::test]
async fn qa_p2_crash_half_frame_skipped() {
    let (cfg, _tmp) = temp_wal_config();

    // 3 个完整单点帧（raft_index 1..3）。
    {
        let wal = WalWriter::new(&cfg).expect("create WalWriter");
        for i in 0..3 {
            wal.write_batch(PointBatch {
                points: vec![make_point(1, i as i64)],
            })
            .await
            .expect("write_batch");
        }
    }

    // 模拟崩溃：在 .wal 文件尾部追加一个「声明 data_len=4096 但只写了 10B payload」的
    // 半帧（header 合法、payload 截断）。这是进程在 write_all(payload) 中途被 kill 的等价物。
    let wal_dir = cfg.get().paths.wal_dir();
    let seg_path = wal_dir.join("00000000.wal");
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .expect("open segment");
        // 37B 帧头：magic "WAL2" + flag 0 + schema 1 + raft_term 0 + raft_index 99 + data_len 4096。
        let mut hdr = [0u8; 37];
        hdr[0..4].copy_from_slice(b"WAL2");
        hdr[4] = 0; // flag Data
        hdr[5..9].copy_from_slice(&1u32.to_le_bytes()); // schema_version
        hdr[9..17].copy_from_slice(&0u64.to_le_bytes()); // raft_term
        hdr[17..25].copy_from_slice(&99u64.to_le_bytes()); // raft_index（半帧不应被恢复）
        hdr[25..29].copy_from_slice(&4096u32.to_le_bytes()); // data_len 远大于实际
        f.write_all(&hdr).expect("write partial header");
        f.write_all(&[0u8; 10]).expect("write partial payload");
        f.flush().expect("flush");
    }

    // 重放：只恢复 3 个完整帧，半帧被跳过、不 panic、不产生第 4 条记录。
    let reader = WalReader::new(wal_dir.clone());
    let records = reader.replay_all(1).expect("replay_all must not error on half frame");
    assert_eq!(records.len(), 3, "半帧必须被跳过，只恢复 3 个完整帧");
    let rafts: Vec<u64> = records.iter().map(|r| r.raft_index).collect();
    assert_eq!(rafts, vec![1, 2, 3], "完整帧 raft_index 应为 1..3，无 99");
}

// ─── C3：durable 档 ACK 语义（fsync 后才 ACK） ──────────────────────────────

#[tokio::test]
async fn qa_p2_durable_ack_after_fsync() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = Arc::new(WalWriter::new(&cfg).expect("create WalWriter"));

    // 后台 group_commit_loop 提供 flusher（drain durable → append → fsync → ack）。
    let loop_handle = tokio::spawn({
        let wal = wal.clone();
        async move { wal.group_commit_loop().await }
    });

    let wr = wal
        .write_durable(vec![make_point(1, 100)])
        .await
        .expect("write_durable");

    // durable ACK 返回时，committed_watermark 必须已 >= seq_start（只有 fsync_and_commit_locked
    // 在 fsync 之后才会推进 committed_watermark）。这证明 ACK 在 fsync 之后。
    let watermark = wal.current_watermark();
    assert!(
        watermark >= wr.wal_seq_start,
        "durable ACK 必须在 fsync（推进 watermark）之后，watermark={watermark}, seq_start={}",
        wr.wal_seq_start
    );
    assert_eq!(wr.wal_seq_start, 1);

    // 帧内容也已落盘可重放。
    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new(wal_dir);
    let records = reader.replay_all(1).expect("replay_all");
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].table_id, 1);
    assert_eq!(recovered[0].values[0], (0u16, TypedValue::F64(100.0)));

    loop_handle.abort();
}

// ─── C4：group/async/durable 三档混写，重放后三档都正确 ────────────────────

#[tokio::test]
async fn qa_p2_three_tier_mixed_replay() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = Arc::new(WalWriter::new(&cfg).expect("create WalWriter"));

    let loop_handle = tokio::spawn({
        let wal = wal.clone();
        async move { wal.group_commit_loop().await }
    });

    // 三档各写一个表（table_id 区分档），交错执行。
    let g = wal
        .write_group(vec![make_point(100, 1000)])
        .await
        .expect("write_group");
    let a = wal
        .write_async(vec![make_point(200, 2000)])
        .await
        .expect("write_async");
    let d = wal
        .write_durable(vec![make_point(300, 3000)])
        .await
        .expect("write_durable");

    // 三档 seq 严格递增、无重叠。
    assert!(g.wal_seq_start < a.wal_seq_start);
    assert!(a.wal_seq_start < d.wal_seq_start);

    // 重放后三档数据都恢复，且按 table_id 能定位到正确的值。
    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new(wal_dir);
    let records = reader.replay_all(1).expect("replay_all");
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered.len(), 3, "三档各一点，应恢复 3 点");

    let mut by_table: std::collections::HashMap<u32, f64> = std::collections::HashMap::new();
    for p in &recovered {
        by_table.insert(p.table_id, p.values[0].1.as_f64());
    }
    assert_eq!(by_table.get(&100), Some(&1000.0), "group 档数据错误");
    assert_eq!(by_table.get(&200), Some(&2000.0), "async 档数据错误");
    assert_eq!(by_table.get(&300), Some(&3000.0), "durable 档数据错误");

    loop_handle.abort();
}

// ─── C5：乱序写入 + 锁外预加密，重放按 seq 重建逻辑顺序 ────────────────────

#[tokio::test]
async fn qa_p2_out_of_order_seq_recovery() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = Arc::new(WalWriter::new(&cfg).expect("create WalWriter"));

    // 构造确定性乱序：先让 durable 分配 seq=1 但**不立即落盘**（无 flusher，阻塞在 ack），
    // 再让 group 分配 seq=2 并立即物理 append；最后 drain durable 把 seq=1 物理 append 到 seq=2 之后。
    let durable_task = tokio::spawn({
        let wal = wal.clone();
        async move { wal.write_durable(vec![make_point(10, 100)]).await }
    });

    // 等到 durable 已分配 seq=1（seq 计数器从 1 推进到 2，即 fetch_add 已发生）。
    while wal.current_seq() < 2 {
        tokio::task::yield_now().await;
    }
    // 给 durable_task 足够时间完成「入队 + notify + 进入 wait_ack」。
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // group 立即 append，分配 seq=2（物理顺序：seq=2 先落盘）。
    wal.write_group(vec![make_point(20, 200)])
        .await
        .expect("write_group");

    // 现在 drain durable：seq=1 物理 append 在 seq=2 之后。
    wal.flush().await.expect("flush drains durable");
    let wr = durable_task.await.expect("durable task join").expect("write_durable");
    assert_eq!(wr.wal_seq_start, 1, "durable 帧 seq_start 应为 1");

    // 重放：物理顺序是 [seq=2(group), seq=1(durable)]（乱序），但两条都完整恢复。
    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new(wal_dir);
    let records = reader.replay_all(1).expect("replay_all");
    assert_eq!(records.len(), 2, "乱序物理 append 不应丢帧");

    let physical: Vec<u64> = records.iter().map(|r| r.raft_index).collect();
    assert_eq!(
        physical,
        vec![2, 1],
        "物理顺序应为 [2(group), 1(durable)]，证明乱序落盘真实发生"
    );

    // 按 seq（raft_index）排序重建逻辑顺序：seq=1 → table 10，seq=2 → table 20。
    let mut by_seq: Vec<(u64, u32)> = records
        .iter()
        .map(|r| (r.raft_index, r.points[0].table_id))
        .collect();
    by_seq.sort_by_key(|(seq, _)| *seq);
    assert_eq!(
        by_seq,
        vec![(1, 10), (2, 20)],
        "恢复必须按 seq 重建逻辑顺序，与物理顺序无关"
    );
}

// ─── C6（I6）：锁外 prepare_frame 以 (0,0) 加密，密文位置无关（AAD 恒空） ──

/// 记录 encrypt/decrypt 收到的 ctx.aad，并断言恒空（P2 锁外预加密的正确性前提）。
struct AadRecordingEncryptor {
    encrypt_aads: Mutex<Vec<Vec<u8>>>,
    decrypt_aads: Mutex<Vec<Vec<u8>>>,
}

impl AadRecordingEncryptor {
    fn new() -> Self {
        Self {
            encrypt_aads: Mutex::new(Vec::new()),
            decrypt_aads: Mutex::new(Vec::new()),
        }
    }
}

impl Encryptor for AadRecordingEncryptor {
    fn encrypt(&self, plaintext: &[u8], ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        self.encrypt_aads.lock().unwrap().push(ctx.aad.clone());
        let mut out = Vec::with_capacity(28 + plaintext.len());
        out.extend_from_slice(&[0xA1u8; 12]); // nonce
        out.extend_from_slice(&[0xB2u8; 16]); // tag
        for b in plaintext {
            out.push(b ^ 0x5A);
        }
        Ok(out)
    }

    fn decrypt(&self, blob: &[u8], ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError> {
        self.decrypt_aads.lock().unwrap().push(ctx.aad.clone());
        if blob.len() < 28 {
            return Err(EncryptError::TooShort(blob.len(), 28));
        }
        Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
    }

    fn active_dek_version(&self, _tenant_id: TenantId) -> u32 {
        1
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn qa_p2_lock_outer_encrypt_aad_invariant() {
    let (cfg, _tmp) = temp_wal_config();
    let encryptor = Arc::new(AadRecordingEncryptor::new());
    let shared: SharedEncryptor = Some(encryptor.clone() as Arc<dyn Encryptor>);

    // 锁外 prepare_frame（write_batch 路径）会以 (0,0) 调 encrypt_payload。
    let wal = WalWriter::new_with_encryptor(&cfg, shared).expect("create WalWriter");
    wal.write_batch(PointBatch {
        points: vec![make_point(7, 123)],
    })
    .await
    .expect("write_batch");

    // reader 解密路径用 (seg_seq, frame_offset) 重建 ctx。
    let wal_dir = cfg.get().paths.wal_dir();
    let reader = WalReader::new_with_encryptor(
        wal_dir,
        Some(encryptor.clone() as Arc<dyn Encryptor>),
    );
    let records = reader.replay_all(1).expect("replay_all");
    assert_eq!(records.len(), 1);
    let recovered: Vec<&DataPoint> = records.iter().flat_map(|r| &r.points).collect();
    assert_eq!(recovered[0].values[0], (0u16, TypedValue::F64(123.0)));

    // 断言：写侧（锁外 prepare_frame）与读侧（scan/replay）收到的 aad 恒空。
    // 这是 P2「锁外算 == 锁内算，逐字节一致」的根：密文只依赖 (tenant, dek_version, compressed)。
    let enc_aads = encryptor.encrypt_aads.lock().unwrap();
    assert!(!enc_aads.is_empty(), "应至少发生一次 encrypt");
    assert!(
        enc_aads.iter().all(|a| a.is_empty()),
        "锁外 encrypt 收到的 aad 必须恒空"
    );
    let dec_aads = encryptor.decrypt_aads.lock().unwrap();
    assert!(!dec_aads.is_empty(), "应至少发生一次 decrypt");
    assert!(
        dec_aads.iter().all(|a| a.is_empty()),
        "reader decrypt 收到的 aad 必须恒空"
    );
}

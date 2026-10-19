//! QA 独立验证（qa-seq-verify）：WAL 数据 seq 跨重启续号修复的正确性。
//!
//! 独立于工程师在 `wal/writer.rs` 内的单元测试，本文件走**真实文件 I/O 路径**：
//! `WalWriter`（真实 append_frame → 真实 CRC → fsync → 落盘）
//!   → 重建 `WalWriter`（扫描既有段 → seq 续号）
//!   → `WalReader::replay_all`（帧头 + CRC + 解压 + 反序列化），
//! 验证「scan 的 seq 续号」与「recovery 的恢复边界」一致，且续号不与旧数据碰撞。
//!
//! 关键差异：工程师的 `test_seq_continuation_after_restart` 用 `write_frame` 合成
//! **单点帧**（`data_len=4`、`crc=0`），而本文件额外覆盖引擎真实写路径的
//! **多点帧**（一个 batch N 个点 → 一帧，`raft_index = seq_start`，点级 seq 为
//! `seq_start..seq_start+N`），这是 seq 续号正确性的真正试金石。

use std::path::PathBuf;

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::engine::{DataPoint, PointBatch};
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_engine::wal::format::{compute_wal_crc, WalFrameFlag, WalFrameHeaderV2};
use sensearcdb_engine::wal::writer::scan_max_raft_index;
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

/// 控制组：单点帧（1 batch = 1 point）跨重启续号。
///
/// Run1 写 100 个单点帧（raft_index 1..100），Run2 重建后 seq 必须从 101 续号。
#[tokio::test]
async fn qa_seq_restart_single_point_frames_continues() {
    let (cfg, _tmp) = temp_wal_config();

    // Run1：100 个单点帧。
    {
        let wal = WalWriter::new(&cfg).expect("create WalWriter (Run1)");
        for i in 0..100 {
            let wr = wal
                .write_batch(PointBatch {
                    points: vec![make_point(1, i)],
                })
                .await
                .expect("write_batch Run1");
            assert_eq!(
                wr.wal_seq_start,
                (i + 1) as u64,
                "Run1 单点帧 seq 应从 1 递增"
            );
        }
        assert_eq!(wal.current_seq(), 101, "Run1 后 seq 计数器应为 101");
    }

    // Run2：重建 WalWriter，扫描既有段续号。
    let wal = WalWriter::new(&cfg).expect("create WalWriter (Run2)");
    assert_eq!(
        wal.current_seq(),
        101,
        "单点帧场景：重启后 seq 必须从扫描 max(100)+1=101 续号"
    );
    let wr = wal
        .write_batch(PointBatch {
            points: vec![make_point(1, 1000)],
        })
        .await
        .expect("write_batch Run2");
    assert_eq!(wr.wal_seq_start, 101, "重启后首个分配的 seq 必须为 101");
}

/// 🟥 核心：多点帧（1 batch = N points → 1 帧）跨重启续号。
///
/// 引擎真实写路径（`Engine::write_batch` → `write_group`）把一整批点序列化进
/// **单个 WAL2 帧**，帧头 `raft_index = seq_start`（首点 seq），而点级 seq 为
/// `seq_start..seq_start+N`。若 scan 只读 `raft_index` 续号，会**低估**真实
/// 已分配的 seq 上界，导致新进程的 seq 与旧帧剩余点（`seq_start+1..`）碰撞。
#[tokio::test]
async fn qa_seq_restart_multi_point_frame_continues() {
    let (cfg, _tmp) = temp_wal_config();

    // Run1：单个 batch 100 个点 → 单帧，raft_index=1，点级 seq 1..100。
    {
        let wal = WalWriter::new(&cfg).expect("create WalWriter (Run1)");
        let points: Vec<DataPoint> = (0..100).map(|i| make_point(1, i)).collect();
        let wr = wal
            .write_batch(PointBatch { points })
            .await
            .expect("write_batch Run1");
        assert_eq!(wr.wal_seq_start, 1, "Run1 首帧 seq_start=1");
        assert_eq!(
            wal.current_seq(),
            101,
            "Run1 单帧 100 点后，seq 计数器必须推进到 101（fetch_add(100)）"
        );
    }

    // 落盘事实：scan 只读帧头，只能看到 raft_index=1。
    let wal_dir = cfg.get().paths.wal_dir();
    assert_eq!(
        scan_max_raft_index(&wal_dir),
        1,
        "scan 只能读到帧头 raft_index=1"
    );

    // Run2：重建 WalWriter。正确续号应为 101（旧帧消耗了 seq 1..100）。
    let wal = WalWriter::new(&cfg).expect("create WalWriter (Run2)");
    assert_eq!(
        wal.current_seq(),
        101,
        "多点帧场景：重启后 seq 必须从真实上界 101 续号，否则与旧帧剩余点(2..100)碰撞"
    );
    let wr = wal
        .write_batch(PointBatch {
            points: vec![make_point(1, 1000)],
        })
        .await
        .expect("write_batch Run2");
    assert_eq!(wr.wal_seq_start, 101, "重启后首个分配的 seq 必须为 101");
}

/// 验证 scan 的续号边界 ≥ recovery 的恢复边界（单点帧，与工程师修复同模型）。
///
/// 用**真实 CRC 帧**（经 `write_batch` 落盘），确认 `scan_max_raft_index` 与
/// `WalReader` 恢复出的最大 `raft_index` 一致，续号 `max+1` 不与恢复数据碰撞。
#[tokio::test]
async fn qa_scan_upper_bound_covers_recovery_boundary() {
    let (cfg, _tmp) = temp_wal_config();

    {
        let wal = WalWriter::new(&cfg).expect("create WalWriter (Run1)");
        for i in 0..50 {
            wal.write_batch(PointBatch {
                points: vec![make_point(1, i)],
            })
            .await
            .expect("write_batch Run1");
        }
    }

    let wal_dir = cfg.get().paths.wal_dir();
    // scan 上界 == 恢复边界（最大 raft_index）。
    assert_eq!(scan_max_raft_index(&wal_dir), 50);

    // recovery 真实恢复出的最大 raft_index。
    let reader = WalReader::new(wal_dir.clone());
    let records = reader.replay_all(1).expect("replay_all");
    assert_eq!(records.len(), 50, "50 个单点帧应全部恢复");
    let max_recovered = records.iter().map(|r| r.raft_index).max().unwrap_or(0);
    assert_eq!(max_recovered, 50, "恢复边界最大 raft_index 应为 50");

    // 续号 = scan_max + 1 = 51 > 恢复边界 50 → 无碰撞。
    let wal = WalWriter::new(&cfg).expect("create WalWriter (Run2)");
    assert!(
        wal.current_seq() > max_recovered,
        "续号必须 > 恢复边界，绝不碰撞"
    );
    assert_eq!(wal.current_seq(), 51);
}

/// 空目录全新启动：seq 从 1 起（兼容原行为，不因 scan 空目录 (u64::MAX,0) 破坏）。
#[tokio::test]
async fn qa_seq_fresh_start_begins_at_one() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new(&cfg).expect("create WalWriter (fresh)");
    assert_eq!(wal.current_seq(), 1, "空目录全新启动 seq 必须从 1 起");
    let wr = wal
        .write_batch(PointBatch {
            points: vec![make_point(1, 0)],
        })
        .await
        .expect("write_batch fresh");
    assert_eq!(wr.wal_seq_start, 1);
}

/// 独立构造一帧，验证 37B 帧头 CRC 对称性 + point_count 往返（不依赖工程师自测）。
#[test]
fn qa_wal_header_37b_crc_and_point_count_roundtrip() {
    let payload = b"qa_payload_for_crc_roundtrip";
    let mut hdr = WalFrameHeaderV2::new(
        WalFrameFlag::Data,
        1,
        0,
        42, // raft_index
        payload.len() as u32,
        100, // point_count
        0,
    );

    // CRC 覆盖 header 前 33B（magic..point_count）+ payload，不含 crc32c 自身。
    let bytes = hdr.to_bytes();
    assert_eq!(bytes.len(), 37, "帧头必须为 37B");
    let mut prefix = [0u8; 33];
    prefix.copy_from_slice(&bytes[..33]);
    hdr.crc32c = compute_wal_crc(&prefix, payload);

    assert!(hdr.verify_crc(payload), "CRC 正例必须通过");
    assert!(
        !hdr.verify_crc(b"tampered"),
        "CRC 负例（payload 篡改）必须失败"
    );

    // point_count 与 raft_index 往返保真。
    let decoded = WalFrameHeaderV2::from_bytes(&hdr.to_bytes()).unwrap();
    assert_eq!(decoded.point_count, 100);
    assert_eq!(decoded.raft_index, 42);
    assert_eq!(decoded.data_len, payload.len() as u32);
}

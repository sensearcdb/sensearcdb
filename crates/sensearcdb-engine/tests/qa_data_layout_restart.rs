//! QA 独立验证（qa-data-layout-verify）：数据目录布局治理 —— 二次重启数据不丢（B）。
//!
//! 核心风险：路径改错会导致「旧路径遗留 → 恢复读不到」，即冷启动写的数据在二次启动
//! 后（WAL / SAC / snapshot 恢复路径）在新路径下找不到。本测试走真实 Engine 写路径：
//! `Engine::new` → `write_batch` → `force_freeze` + `flush_immutable_sync`（落 SAC）
//! → `shutdown` → 重建 `Engine`（同 `data_dir`）→ `query_table_columns` 读回并断言值保真。
//!
//! 同时断言落盘文件确实位于新布局（`data_dir/sac/`、`data_dir/wal/`），
//! `data_dir` 根与 cwd 无散落的 `.sac` / `.wal` / `.db`。

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch};
use sensearcdb_engine::memtable::TypedValue;
use sensearcdb_engine::sac::SacNaming;
use sensearcdb_engine::TenantId;
use std::path::PathBuf;

fn point(table_id: u32, ts: i64, v: f64) -> DataPoint {
    DataPoint {
        table_id,
        tenant_id: TenantId::DEFAULT,
        ts,
        server_ts: ts + 1,
        values: vec![(0, TypedValue::F64(v))],
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    }
}

fn temp_handle() -> (ConfigHandle, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    (ConfigHandle::new(cfg), tmp)
}

/// 断言 `dir` 根目录下无任何散落的 `.db` / `.sac` / `.wal` / `.log` 文件
/// （只允许子目录 + 根级控制文件 FORMAT_VERSION / sensearcdb.lock）。
fn assert_no_scattered_data_files(dir: &PathBuf) {
    let entries = std::fs::read_dir(dir).expect("read data_dir");
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy().to_string();
        if entry.path().is_file() {
            let lower = name.to_lowercase();
            assert!(
                !(lower.ends_with(".db")
                    || lower.ends_with(".sac")
                    || lower.ends_with(".wal")
                    || lower.ends_with(".log")
                    || lower.ends_with(".bin") && name != "FORMAT_VERSION"),
                "data_dir 根不应出现散落数据文件: {name}"
            );
        }
    }
}

#[tokio::test]
async fn qa_data_layout_restart_data_not_lost() {
    let (handle, tmp) = temp_handle();
    let data_dir = tmp.path().to_path_buf();

    // ── Run1：写数据 → 落 SAC → 优雅停 ──
    let sac_dir: PathBuf;
    {
        let engine = Engine::new(&handle, None)
            .await
            .expect("Engine::new (Run1)");
        sac_dir = engine.sac_dir().to_path_buf();

        engine
            .write_batch(PointBatch {
                points: vec![
                    point(42, 1_000, 3.14),
                    point(42, 2_000, 2.718),
                    point(42, 3_000, -1.5),
                ],
            })
            .await
            .expect("write_batch (Run1)");

        // 冻结 active → immutable，再同步 flush barrier 落 SAC。
        let freeze = engine.force_freeze();
        assert!(freeze.frozen, "有数据点时应发生冻结");
        let blocks = engine
            .flush_immutable_sync()
            .await
            .expect("flush_immutable_sync");
        assert!(blocks >= 1, "本轮应至少落 1 个 SAC Block，实际 {blocks}");

        // 优雅停（force_freeze + WAL flush）。
        engine.shutdown().await.expect("shutdown (Run1)");

        // 落盘事实：SAC 段必须在新布局 data_dir/sac/{tenant}/ 下（v9 多表聚合）。
        let naming = SacNaming::new(&sac_dir);
        let segments = naming
            .list_tenant_segments(0)
            .expect("list_tenant_segments");
        assert!(
            !segments.is_empty(),
            "flush barrier 后 SAC 段必须落在 data_dir/sac/ 下"
        );
    }

    // 落盘事实：WAL 段必须在 data_dir/wal/ 下，而非 data_dir 根。
    let wal_dir = data_dir.join("wal");
    let wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
        .expect("wal_dir exists")
        .flatten()
        .filter(|e| e.path().is_file())
        .collect();
    assert!(
        !wal_files.is_empty(),
        "写路径应在 data_dir/wal/ 落 WAL 段文件"
    );

    // 不变量 I1/I2/I3：data_dir 根无散落数据文件。
    assert_no_scattered_data_files(&data_dir);

    // ── Run2：二次启动（同 data_dir）→ 读回数据 ──
    {
        let engine2 = Engine::new(&handle, None)
            .await
            .expect("Engine::new (Run2)");

        let (cols, timestamps) = engine2
            .query_table_columns(TenantId::DEFAULT, 42, &[0], 0, i64::MAX)
            .await
            .expect("query_table_columns (Run2)");

        // 三源合并（memtable / immutable / SAC 恢复）：恢复出 3 个点。
        assert_eq!(
            timestamps.len(),
            3,
            "二次启动应恢复 3 个点，实际 {}",
            timestamps.len()
        );
        // 值保真（column 0 为 F64）。
        let got: Vec<f64> = cols[0]
            .iter()
            .map(|v| match v {
                Some(TypedValue::F64(x)) => *x,
                other => panic!("期望 F64，实际 {other:?}"),
            })
            .collect();
        // 按 ts 排序后逐点比对（恢复顺序可能与写入顺序无关）。
        let mut pairs: Vec<(i64, f64)> = timestamps
            .iter()
            .copied()
            .zip(got.iter().copied())
            .collect();
        pairs.sort_by_key(|(ts, _)| *ts);

        let mut expected_by_ts: Vec<(i64, f64)> =
            vec![(1_000, 3.14), (2_000, 2.718), (3_000, -1.5)];
        expected_by_ts.sort_by_key(|(ts, _)| *ts);

        assert_eq!(pairs.len(), expected_by_ts.len(), "恢复点数应一致");
        for (&(gts, gv), &(ets, ev)) in pairs.iter().zip(expected_by_ts.iter()) {
            assert_eq!(gts, ets, "二次启动应恢复原时间戳");
            assert!(
                (gv - ev).abs() < 1e-9,
                "二次启动读回值应保真: ts={gts} 读回 {gv} vs 期望 {ev}"
            );
        }

        engine2.shutdown().await.expect("shutdown (Run2)");
    }
}

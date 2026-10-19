//! QA 独立回归验证（阶段 5 收尾 · T4 health readiness 的 WAL 探针）。
//!
//! 对照 PM 裁决 phase5-wrapup-ruling.md §2 #6 + §4 跟进事项 #3：
//!   - `WalWriter` 需补轻量「closed/error」状态位以支撑「WAL 可写」判定，
//!     不能是恒 `true` 的硬编码。
//!
//! 独立于工程师自测，本文件验证 `WalWriter` 的 `writable` 状态位是「真实探针」：
//!   - 构造成功 → `is_writable() == true`；
//!   - 显式 `mark_unwritable()` 后 → `is_writable() == false`。
//! 该状态位正是 `src/routes.rs` readiness 闭包判定「WAL 可写」的数据源，
//! 若它是硬编码恒 true，则「WAL 不可写 → /health/ready 503」将永远无法触发。

use sensearcdb_common::{AppConfig, ConfigHandle};
use sensearcdb_engine::wal::WalWriter;

fn temp_wal_config() -> (ConfigHandle, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
    (ConfigHandle::new(cfg), tmp)
}

#[test]
fn qa_phase5_wal_writer_probe_is_not_hardcoded() {
    let (cfg, _tmp) = temp_wal_config();
    let wal = WalWriter::new(&cfg).expect("create WalWriter");

    // 构造成功 → 可写。
    assert!(wal.is_writable(), "新建 WalWriter 应可写");

    // 显式标记不可写（模拟致命 I/O 错误 / 关闭）→ 探针必须翻转为 false。
    wal.mark_unwritable();
    assert!(
        !wal.is_writable(),
        "mark_unwritable 后 is_writable 必须返回 false（否则 readiness 探针形同虚设）"
    );
}

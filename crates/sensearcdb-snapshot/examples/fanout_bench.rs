//! 临时：量化 snapshot `update_typed`（写路径 on_write fanout 的核心成本）。
//!
//! 模拟 TSBS 宽表（10 列）逐点调用 update_typed，测每点/每列成本。
//! 完成后随埋点一起移除。

use std::sync::Arc;
use std::time::Instant;

use sensearcdb_common::{
    AppConfig, ConfigHandle, DataType, TenantId, TypedValue,
};
use sensearcdb_snapshot::{
    SchemaMirrorRef, SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
};

struct TestMirror;
impl SchemaMirrorRef for TestMirror {
    fn get_table_name(&self, _: u32) -> Option<String> {
        Some("tag".into())
    }
    fn get_table_description(&self, _: u32) -> Option<String> {
        None
    }
    fn get_range_low(&self, _: u32, _: u16) -> f64 {
        0.0
    }
    fn get_range_high(&self, _: u32, _: u16) -> f64 {
        100.0
    }
    fn get_instrument_range_low(&self, _: u32, _: u16) -> Option<f64> {
        None
    }
    fn get_instrument_range_high(&self, _: u32, _: u16) -> Option<f64> {
        None
    }
    fn get_unit(&self, _: u32, _: u16) -> Option<String> {
        None
    }
    fn get_display_digits(&self, _: u32, _: u16) -> u8 {
        2
    }
    fn get_data_type(&self, _: u32, _: u16) -> DataType {
        DataType::F64
    }
    fn is_enabled(&self, _: u32) -> bool {
        true
    }
}

fn main() {
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = 2;
    let ch = ConfigHandle::new(cfg);
    let store = SnapshotStore::new(
        &ch,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestMirror))),
        SnapshotMetrics::noop(),
    );

    const COLS: usize = 10;
    const N_POINTS: usize = 3_000_000;
    // 4000 张表（对齐 TSBS），表 id 轮转。
    const N_TABLES: u32 = 4000;

    let mut values: Vec<(u16, TypedValue)> = Vec::with_capacity(COLS);
    for c in 0..9u16 {
        values.push((c, TypedValue::F64(c as f64)));
    }
    values.push((9u16, TypedValue::I64(1)));
    let qualities: Vec<(u16, u8)> = (0..10u16).map(|c| (c, 0xC0u8)).collect();

    let wall = Instant::now();
    for i in 0..N_POINTS {
        let table_id = (i as u32) % N_TABLES + 1;
        store.update_typed(
            TenantId::DEFAULT,
            table_id,
            &values,
            &qualities,
            i as i64,
            i as i64 + 1,
            Some(i as u64),
            false,
            None,
        );
    }
    let wall_s = wall.elapsed().as_secs_f64();
    let n = N_POINTS as f64;

    println!("=== snapshot update_typed fanout bench ===");
    println!(
        "points={} wall={:.3}s throughput={:.0} points/s  {:.1} ns/point",
        N_POINTS,
        wall_s,
        n / wall_s,
        wall.elapsed().as_nanos() as f64 / n
    );
}

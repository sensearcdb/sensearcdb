//! 独立 micro-benchmark：实测 `ColumnarChunk::append` 的 ns/point。
//!
//! 目标：验证 P1 2a 优化（密集宽表跳过 presence 位图维护）在「新鲜表首行起连续
//! 密集 append」主路径上是否真正生效，并与 378ns/point 基线对比。
//!
//! 两个场景：
//! - 场景 A：新鲜表从空开始，连续密集 append 10 列宽表（TSBS 灌数主路径）。
//! - 场景 B：同样 10 列 chunk，但每点只写 1 列（稀疏），测 presence 维护路径基线。
//!
//! 运行：`cargo run --release --example chunk_append_bench`

use std::hint::black_box;
use std::time::Instant;

use sensearcdb_common::TenantId;
use sensearcdb_engine::memtable::{ColumnarChunk, TypedValue};

const N_COLS: u16 = 10; // column_id 0..9，全 F64 宽表
const WARMUP: usize = 100_000; // 预热点数（不计时）
const N_POINTS: usize = 1_000_000; // 计时点数

/// 构造「覆盖全部 10 列」的密集 values 切片（静态，append 只读、不修改切片，
/// 复用同一切片可隔离 append 本身成本，剔除每点 Vec 构造噪声）。
fn dense_values() -> Vec<(u16, TypedValue)> {
    (0..N_COLS)
        .map(|c| (c, TypedValue::F64(c as f64 + 0.5)))
        .collect()
}

fn sparse_values() -> Vec<(u16, TypedValue)> {
    vec![(0, TypedValue::F64(1.5))]
}

fn main() {
    // ── 场景 A：新鲜表首行起连续密集 append ──────────────────────────────
    let values = dense_values();
    let qualities: Vec<(u16, u8)> = vec![(0, 0xC0)];

    let mut chunk = ColumnarChunk::new(TenantId::DEFAULT, 1);

    // 预热
    let mut ts = 1_692_180_000_000_000_000i64;
    for _ in 0..WARMUP {
        black_box(chunk.append(black_box(ts), &values, &qualities, 0));
        ts += 1;
    }

    // 计时
    let start = Instant::now();
    for _ in 0..N_POINTS {
        black_box(chunk.append(black_box(ts), &values, &qualities, 0));
        ts += 1;
    }
    let elapsed = start.elapsed();

    let ns_pp_a = elapsed.as_nanos() as f64 / N_POINTS as f64;
    println!("=== 场景 A：新鲜表 10 列密集 append（2a 生效主路径） ===");
    println!("  N              = {}", N_POINTS);
    println!("  total          = {:.3} ms", elapsed.as_secs_f64() * 1e3);
    println!("  ns/point       = {:.1}", ns_pp_a);
    println!("  point_count    = {}", chunk.point_count);
    println!("  columns.len    = {}", chunk.columns.len());
    println!("  presence.empty = {}", chunk.presence.is_empty());
    println!("  out_of_order   = {}", chunk.out_of_order);
    println!(
        "  vs 378ns 基线  = {:.1} ns（下降 {:.1}%）",
        378.0 - ns_pp_a,
        (378.0 - ns_pp_a) / 378.0 * 100.0
    );

    // ── 场景 B：10 列 chunk 每点只写 1 列（稀疏，presence 维护基线） ──────
    let svalues = sparse_values();
    let mut chunk2 = ColumnarChunk::new(TenantId::DEFAULT, 1);
    // 先建 10 列（首行密集），使后续稀疏点触发 presence 物化/维护。
    chunk2.append(0, &values, &qualities, 0);

    let mut ts2 = 1i64;
    for _ in 0..WARMUP {
        black_box(chunk2.append(black_box(ts2), &svalues, &qualities, 0));
        ts2 += 1;
    }

    let start2 = Instant::now();
    for _ in 0..N_POINTS {
        black_box(chunk2.append(black_box(ts2), &svalues, &qualities, 0));
        ts2 += 1;
    }
    let elapsed2 = start2.elapsed();

    let ns_pp_b = elapsed2.as_nanos() as f64 / N_POINTS as f64;
    println!();
    println!("=== 场景 B：10 列 chunk 每点 1 列稀疏 append（presence 维护路径） ===");
    println!("  N              = {}", N_POINTS);
    println!("  total          = {:.3} ms", elapsed2.as_secs_f64() * 1e3);
    println!("  ns/point       = {:.1}", ns_pp_b);
    println!("  point_count    = {}", chunk2.point_count);
    println!("  columns.len    = {}", chunk2.columns.len());
    println!("  presence.empty = {}", chunk2.presence.is_empty());

    println!();
    println!(
        "总结：密集 ns/point = {:.1}，稀疏 ns/point = {:.1}，presence 维护开销 ≈ {:.1} ns/point",
        ns_pp_a,
        ns_pp_b,
        ns_pp_b - ns_pp_a
    );
}

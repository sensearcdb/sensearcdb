//! SenseArcDB N=1 单租户性能基线 harness。
//!
//! 目的：在多租户改造（S1~S5）之前，固化「单租户」数据面引擎的性能基线，
//! 作为后续「多租户相对单租户 P99 劣化 < 5%」验收的对照基准（PRD §11 A4）。
//!
//! 基准路径选型：直接调用 `sensearcdb_engine::engine::Engine` 的
//! `write_batch` / `query_table`（数据面引擎 API），而非 gRPC/REST 压测。
//! 理由见 `docs/performance-baseline-n1.md`。
//!
//! 覆盖指标：
//! 1. 写吞吐（ops/s = 数据点/秒）
//! 2. 写延迟 P50/P95/P99/P99.9（ms，per-write_batch 调用）
//! 3. 查询延迟 P50/P95/P99/P99.9（ms）——两个场景：
//!    a. 「MemTable 热读」（全量 active MemTable 驻留，freeze 之前）
//!    b. 「SAC 冷读」（freeze + flush 之后，纯 SAC 路径，主口径）
//! 4. 查询吞吐（queries/s 与 rows/s，补充）
//!
//! 确定性：固定测点数、固定批次、固定时间窗口、固定列数；查询窗口用等步长
//! 循环（无随机数依赖）。

use std::time::Instant;

use sensearcdb_common::{AppConfig, ConfigHandle, TypedValue};
use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch};

/// 预热数据的时间戳基准（与实测区隔离，避免污染查询窗口）。
const WARM_BASE_TS: i64 = 1_000_000_000_000_000_000;
/// 实测数据的时间戳基准（epoch 纳秒，UTC）。
const MEAS_BASE_TS: i64 = 1_700_000_000_000_000_000;

/// 命令行参数（全部有默认值，保证「一条命令可复现」）。
#[derive(Debug, Clone)]
struct Args {
    /// 实测写入的数据点总数。
    points: u64,
    /// 单次 `write_batch` 的点数（固定批次）。
    batch: usize,
    /// 表（table_id）数量。
    tables: u32,
    /// 每个数据点的列（column）数量。
    columns: u16,
    /// 单次查询期望返回的行数。
    rows_per_query: i64,
    /// 查询次数（热读/冷读各跑一次）。
    queries: usize,
    /// 预热写入的数据点数。
    warmup_points: u64,
    /// 显式数据目录（缺省用临时目录，跑完自动清理）。
    data_dir: Option<String>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            points: 1_000_000,
            batch: 1024,
            tables: 8,
            columns: 4,
            rows_per_query: 1_000,
            queries: 10_000,
            warmup_points: 100_000,
            data_dir: None,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--points" => {
                i += 1;
                if let Some(v) = raw.get(i) {
                    a.points = v.parse().unwrap_or(a.points);
                }
            }
            "--batch" => {
                i += 1;
                if let Some(v) = raw.get(i) {
                    a.batch = v.parse().unwrap_or(a.batch);
                }
            }
            "--tables" => {
                i += 1;
                if let Some(v) = raw.get(i) {
                    a.tables = v.parse().unwrap_or(a.tables);
                }
            }
            "--columns" => {
                i += 1;
                if let Some(v) = raw.get(i) {
                    a.columns = v.parse().unwrap_or(a.columns);
                }
            }
            "--rows-per-query" => {
                i += 1;
                if let Some(v) = raw.get(i) {
                    a.rows_per_query = v.parse().unwrap_or(a.rows_per_query);
                }
            }
            "--queries" => {
                i += 1;
                if let Some(v) = raw.get(i) {
                    a.queries = v.parse().unwrap_or(a.queries);
                }
            }
            "--warmup-points" => {
                i += 1;
                if let Some(v) = raw.get(i) {
                    a.warmup_points = v.parse().unwrap_or(a.warmup_points);
                }
            }
            "--data-dir" => {
                i += 1;
                a.data_dir = raw.get(i).cloned();
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                eprintln!("未知参数: {other}");
                print_usage();
                std::process::exit(2);
            }
        }
        i += 1;
    }
    a
}

fn print_usage() {
    eprintln!(
        "用法: sensearcdb-bench [--points N] [--batch B] [--tables T] [--columns C] \
         [--rows-per-query R] [--queries Q] [--warmup-points W] [--data-dir PATH]"
    );
}

/// 构造一个确定性的数据点。
fn make_point(table_id: u32, ts: i64, columns: u16) -> DataPoint {
    let values: Vec<(u16, TypedValue)> = (0..columns)
        .map(|c| {
            (
                c,
                TypedValue::F64(ts as f64 * 0.5 + table_id as f64 + c as f64),
            )
        })
        .collect();
    DataPoint {
        table_id,
        tenant_id: sensearcdb_engine::TenantId::DEFAULT,
        ts,
        server_ts: ts + 1,
        values,
        qualities: vec![(0, 0xC0)],
        schema_version: 1,
        flags: 0,
    }
}

/// 写入一段数据（round-robin 分配表），返回每次 `write_batch` 的延迟（ns）。
async fn write_range(
    engine: &Engine,
    base_ts: i64,
    total: u64,
    batch: usize,
    tables: u32,
    columns: u16,
    record_latency: bool,
) -> Vec<f64> {
    let n_batches = (total as usize + batch - 1) / batch;
    let mut latencies = Vec::new();
    for b in 0..n_batches {
        let s = b * batch;
        let e = ((b + 1) * batch).min(total as usize);
        let points: Vec<DataPoint> = (s..e)
            .map(|i| {
                let i = i as u64;
                make_point((i % tables as u64) as u32, base_ts + i as i64, columns)
            })
            .collect();
        let t = Instant::now();
        engine
            .write_batch(PointBatch { points })
            .await
            .expect("write_batch should succeed");
        if record_latency {
            latencies.push(t.elapsed().as_nanos() as f64);
        }
    }
    latencies
}

/// 预计算查询窗口（等步长循环取模，落在 `[base_ts, base_ts + span)` 内）。
fn build_query_windows(
    base_ts: i64,
    span: i64,
    tables: u32,
    rows_per_query: i64,
    queries: usize,
) -> Vec<(u32, i64, i64)> {
    let window_ts = rows_per_query * tables as i64;
    let step = ((span - window_ts).max(1) / queries.max(1) as i64).max(1);

    (0..queries)
        .map(|j| {
            let table_id = (j % tables as usize) as u32;
            let start = base_ts + (j as i64 * step) % span.max(1);
            let end = (start + window_ts).min(base_ts + span);
            (table_id, start, end)
        })
        .collect()
}

/// 运行一组查询，返回（每次查询延迟 ns，累计返回行数）。
async fn run_queries(engine: &Engine, windows: &[(u32, i64, i64)]) -> (Vec<f64>, u64) {
    let mut latencies = Vec::with_capacity(windows.len());
    let mut total_rows: u64 = 0;
    for &(table_id, start, end) in windows {
        let t = Instant::now();
        let res = engine
            .query_table(sensearcdb_common::TenantId::DEFAULT, table_id, start, end)
            .await
            .expect("query_table should succeed");
        latencies.push(t.elapsed().as_nanos() as f64);
        total_rows += res.batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
    }
    (latencies, total_rows)
}

/// 最近邻排名法（nearest-rank）分位数。返回 (p50, p95, p99, p99.9)，单位 ms。
fn percentiles(ns: &[f64]) -> (f64, f64, f64, f64) {
    let mut s = ns.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pick = |q: f64| -> f64 {
        if s.is_empty() {
            0.0
        } else {
            let idx = ((q / 100.0) * (s.len() as f64 - 1.0)).round() as usize;
            s[idx.min(s.len() - 1)] / 1_000_000.0
        }
    };
    (pick(50.0), pick(95.0), pick(99.0), pick(99.9))
}

fn mean(ns: &[f64]) -> f64 {
    if ns.is_empty() {
        0.0
    } else {
        ns.iter().sum::<f64>() / ns.len() as f64
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    // 数据目录：默认临时目录（自动清理），或显式 --data-dir（可复现到固定路径）。
    let _tmp = match &args.data_dir {
        Some(_) => None,
        None => Some(tempfile::tempdir().expect("create temp dir")),
    };
    let root = match &args.data_dir {
        Some(p) => std::path::PathBuf::from(p),
        None => _tmp.as_ref().unwrap().path().to_path_buf(),
    };
    std::fs::create_dir_all(&root).expect("create data dir");

    let mut cfg = AppConfig::default();
    cfg.paths.data_dir = root.to_string_lossy().into_owned();
    let handle = ConfigHandle::new(cfg);

    println!("=== SenseArcDB N=1 单租户性能基线 ===");
    println!(
        "配置: points={} batch={} tables={} columns={} rows_per_query={} queries={} warmup_points={}",
        args.points, args.batch, args.tables, args.columns, args.rows_per_query, args.queries,
        args.warmup_points
    );
    println!("数据目录: {}", root.display());

    let engine = Engine::new(&handle, None).await.expect("engine init");

    // ── 预热 ────────────────────────────────────────────────
    write_range(
        &engine,
        WARM_BASE_TS,
        args.warmup_points,
        args.batch,
        args.tables,
        args.columns,
        false,
    )
    .await;
    println!("预热完成: {} points", args.warmup_points);

    // ── 写吞吐 + 写延迟 ─────────────────────────────────────
    let w_t0 = Instant::now();
    let write_lat = write_range(
        &engine,
        MEAS_BASE_TS,
        args.points,
        args.batch,
        args.tables,
        args.columns,
        true,
    )
    .await;
    let write_elapsed = w_t0.elapsed();
    let write_ops = args.points as f64 / write_elapsed.as_secs_f64();
    let (w50, w95, w99, w999) = percentiles(&write_lat);
    println!(
        "写入完成: {} points in {:.3}s  =>  {:.0} ops/s（数据点/秒）",
        args.points,
        write_elapsed.as_secs_f64(),
        write_ops
    );
    println!(
        "写入延迟: mean={:.3}ms P50={:.3}ms P95={:.3}ms P99={:.3}ms P99.9={:.3}ms",
        mean(&write_lat) / 1_000_000.0,
        w50,
        w95,
        w99,
        w999
    );

    // ── 查询：MemTable 热读（全量 active MemTable 驻留 ≈ 110 万点，freeze 前）──
    // 修复前此场景因 `chunks_snapshot()` 深拷贝为 O(总点数) 病态慢；
    // 修复后改为 `with_chunks` 读锁内收集，O(查询范围)。
    let windows = build_query_windows(
        MEAS_BASE_TS,
        args.points as i64,
        args.tables,
        args.rows_per_query,
        args.queries,
    );
    let (hot_lat, hot_rows) = run_queries(&engine, &windows).await;
    let (h50, h95, h99, h999) = percentiles(&hot_lat);
    let h_qps = windows.len() as f64 / (hot_lat.iter().sum::<f64>() / 1e9);
    let h_rps = hot_rows as f64 / (hot_lat.iter().sum::<f64>() / 1e9);
    println!(
        "查询[MemTable热读(全量)]: queries={} total_rows={} P50={:.3}ms P95={:.3}ms P99={:.3}ms P99.9={:.3}ms mean={:.3}ms  => {:.0} q/s, {:.0} rows/s",
        windows.len(),
        hot_rows,
        h50,
        h95,
        h99,
        h999,
        mean(&hot_lat) / 1_000_000.0,
        h_qps,
        h_rps
    );
    if hot_rows == 0 {
        eprintln!("警告: MemTable 热读返回 0 行，请排查。");
    }

    // ── 冻结 + 落 SAC（把实测数据推入 SAC，主查询路径 = SAC 冷读）──
    let freeze = engine.force_freeze();
    let blocks = engine.flush_immutable_sync().await.expect("flush barrier");
    println!(
        "冻结+落盘: frozen={} total_points={} SAC_blocks={}",
        freeze.frozen, freeze.total_points, blocks
    );

    // ── 查询：SAC 冷读（主口径）─────────────────────────────
    let (cold_lat, cold_rows) = run_queries(&engine, &windows).await;
    let (q50, q95, q99, q999) = percentiles(&cold_lat);
    let qps = windows.len() as f64 / (cold_lat.iter().sum::<f64>() / 1e9);
    let rps = cold_rows as f64 / (cold_lat.iter().sum::<f64>() / 1e9);
    println!(
        "查询[SAC冷读]: queries={} total_rows={} P50={:.3}ms P95={:.3}ms P99={:.3}ms P99.9={:.3}ms mean={:.3}ms  => {:.0} q/s, {:.0} rows/s",
        windows.len(),
        cold_rows,
        q50,
        q95,
        q99,
        q999,
        mean(&cold_lat) / 1_000_000.0,
        qps,
        rps
    );
    if cold_rows == 0 {
        eprintln!("警告: SAC 冷读返回 0 行，数据可能未正确落盘，请排查。");
    }

    println!("=== 基线输出（机器可读）===");
    println!("write_ops_per_sec={:.0}", write_ops);
    println!(
        "write_latency_ms_p50={:.3} p95={:.3} p99={:.3} p99_9={:.3}",
        w50, w95, w99, w999
    );
    println!(
        "query_hot_memtable_p99_ms={:.3} p50={:.3} p95={:.3} p99_9={:.3} qps={:.0} rows_per_sec={:.0} total_rows={}",
        h99, h50, h95, h999, h_qps, h_rps, hot_rows
    );
    println!(
        "query_sac_cold_p99_ms={:.3} p50={:.3} p95={:.3} p99_9={:.3} qps={:.0} rows_per_sec={:.0} total_rows={}",
        q99, q50, q95, q999, qps, rps, cold_rows
    );
}

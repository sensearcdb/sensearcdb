//! 隔离实验：block 大小对最终压缩体积的影响（碎片化 vs 全量归并）。
//!
//! 背景：真实采集 10 万表 × 1 表 1 tag，flush=46 万点 → 每表每 flush 仅 ~4.6 点。
//! 刀3「block 拼接」在 L1→L2 / L2→L3 只复制字节、不归并，导致 block 停在 ~74 点；
//! 老项目「逐级重压」在 L1→L2 / L2→L3 点级归并，block 会归并回 4096 点。
//!
//! 本实验把「block 大小」作为唯一变量，用同一份数据（ts 抖动 ±10% + 值带噪声漂移）
//! 分别按 5 / 74 / 4096 点/block 编码，量出：
//!   - 固定开销（48B 头 + 目录 + 4B CRC ≈ 88B/block）随块变小被摊薄的程度
//!   - ZSTD9 窗口随块变小对压缩率的损失
//! 两者叠加 = 碎片化对最终 L4 体积的总影响。

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use sensearcdb_engine::compression::{compress_zstd9, CompressionType};
use sensearcdb_engine::memtable::{ColVec, ColumnarChunk};
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::writer::SacWriter;
use sensearcdb_engine::TenantId;

/// 每表点数（24h @ 1s）。
const POINTS: usize = 86_400;
/// 起始时间戳（epoch 纳秒）。
const START_TS: i64 = 1_760_000_000_000_000_000;
/// 基准时间步长（1s）。
const INTERVAL_NS: i64 = 1_000_000_000;

/// 确定性 PRNG（xorshift64）。
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// 生成 1 表 1 tag 数据：ts 抖动 ±10%（非严格 1s），值带平滑漂移 + 周期 + 小幅噪声。
/// 值噪声幅度设为相邻差 ~0.0067 量级（对应真实测点 Gorilla XOR 有效位 ~40 位）。
fn generate(rng_seed: u64) -> ColumnarChunk {
    let mut rng = Rng(rng_seed);
    let n = POINTS;

    // ts：非严格 1s，间隔在 [0.9s, 1.1s] 抖动，累加。
    let mut timestamps = Vec::with_capacity(n);
    let mut ts = START_TS;
    timestamps.push(ts);
    for _ in 1..n {
        let jitter_ns = ((rng.next_f64() - 0.5) * 0.2 * INTERVAL_NS as f64) as i64; // ±10%
        ts += INTERVAL_NS + jitter_ns;
        timestamps.push(ts);
    }

    // 值：基准 + 慢漂移 + 日周期 + 小幅噪声（相邻差 ~0.0067 量级）。
    let base = 30.0;
    let drift = 2.0; // 全天慢漂移 ±2
    let amp = 1.5; // 日周期幅度
    let amp2 = 0.3; // 慢周期
    let noise_scale = 0.003; // 单点噪声，叠加后相邻差 ~0.0067 量级
    let mut values = Vec::with_capacity(n);
    let mut v = base;
    for i in 0..n {
        let t = i as f64;
        let day = t / 86_400.0;
        let target = base
            + drift * day
            + amp * (t / 7200.0).sin()
            + amp2 * (t / 900.0).sin();
        // 值向 target 平滑逼近 + 噪声，产生「相邻差小、但整体有变化」的真实测点特征
        v = v + (target - v) * 0.01 + (rng.next_f64() - 0.5) * 2.0 * noise_scale;
        values.push(v);
    }

    let mut columns: HashMap<u16, ColVec> = HashMap::new();
    columns.insert(0, ColVec::F64(values));

    ColumnarChunk {
        table_id: 1,
        tenant_id: TenantId::DEFAULT,
        timestamps,
        columns,
        presence: HashMap::new(),
        qualities: vec![0xC0; n],
        ts_index: BTreeMap::new(),
        point_count: n,
        out_of_order: false,
        max_wal_seq: None,
        min_wal_seq: None,
    }
}

/// 把 chunk 切成 `block_rows` 点/块的小 chunk 列表。
fn split_chunk(chunk: &ColumnarChunk, block_rows: usize) -> Vec<ColumnarChunk> {
    let n = chunk.point_count;
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < n {
        let end = (start + block_rows).min(n);
        let mut columns = HashMap::new();
        for (cid, col) in &chunk.columns {
            let sliced = match col {
                ColVec::F64(v) => ColVec::F64(v[start..end].to_vec()),
                _ => panic!("unsupported col type"),
            };
            columns.insert(*cid, sliced);
        }
        out.push(ColumnarChunk {
            table_id: chunk.table_id,
            tenant_id: chunk.tenant_id,
            timestamps: chunk.timestamps[start..end].to_vec(),
            columns,
            presence: HashMap::new(),
            qualities: chunk.qualities[start..end].to_vec(),
            ts_index: BTreeMap::new(),
            point_count: end - start,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        });
        start = end;
    }
    out
}

/// 按给定 block_rows 粒度编码为 ZSTD9 文件，返回 (总字节, block 数)。
fn encode_with_block_size(dir: &Path, chunk: &ColumnarChunk, block_rows: usize) -> (u64, usize) {
    let writer = SacWriter::new(dir, CompressionType::Zstd9);
    let tenant_id = TenantId::DEFAULT.as_u32();
    let mut builder = writer
        .begin_write(tenant_id, START_TS, SacLevel::L2, 1, START_TS)
        .expect("begin_write");
    let sub_chunks = split_chunk(chunk, block_rows);
    let block_count = sub_chunks.len();
    for sc in &sub_chunks {
        builder.append_chunk(sc, None).expect("append_chunk");
    }
    let path = builder.finish().expect("finish");
    let size = std::fs::metadata(&path).expect("metadata").len();
    (size, block_count)
}

fn main() {
    let tmp = std::env::temp_dir().join("sensearc_block_size_effect");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).expect("mkdir");

    let chunk = generate(0x1234_5678_9ABC_DEF0u64);
    let n = chunk.point_count as u64;
    println!("数据：1 表 1 tag，{} 点，ts 抖动 ±10%，值带噪声漂移", n);
    println!();

    // 固定开销常量（1 列）：48B mini-header + (1+2)*12B 目录 + 4B CRC = 88B
    const FIXED_PER_BLOCK: u64 = 88;

    println!("{:>10} | {:>14} | {:>12} | {:>10} | {:>12}", "block大小", "总字节", "B/点", "固定开销B/点", "纯压缩B/点");
    println!("{}", "-".repeat(75));
    for block_rows in [5usize, 74, 4096] {
        let dir = tmp.join(format!("b{}", block_rows));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let (size, blocks) = encode_with_block_size(&dir, &chunk, block_rows);
        let bpp = size as f64 / n as f64;
        let fixed_bpp = (blocks as u64 * FIXED_PER_BLOCK) as f64 / n as f64;
        let comp_bpp = bpp - fixed_bpp;
        println!(
            "{:>9}点 | {:>14} | {:>11.2} | {:>12.3} | {:>13.3}",
            block_rows, size, bpp, fixed_bpp, comp_bpp
        );
    }

    let _ = std::fs::remove_dir_all(&tmp);

    raw_zstd_probe();
}

/// 把 f64 数组按 little-endian 转字节（原始值编码，不做 Gorilla）。
fn f64_to_bytes(v: &[f64]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// 相邻差值（f64 差）→ 字节。
fn delta_f64_to_bytes(v: &[f64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 8);
    out.extend_from_slice(&v[0].to_le_bytes());
    for w in v.windows(2) {
        out.extend_from_slice(&(w[1] - w[0]).to_le_bytes());
    }
    out
}

/// 独立测试：不同「值编码方案 × ZSTD9」的 B/点，定位压缩方案根因。
fn raw_zstd_probe() {
    let chunk = generate(0x1234_5678_9ABC_DEF0u64);
    let vals: Vec<f64> = match &chunk.columns[&0] {
        ColVec::F64(v) => v.clone(),
        _ => unreachable!(),
    };
    let ts = chunk.timestamps.clone();
    let n = vals.len() as f64;

    let raw = f64_to_bytes(&vals);
    let raw_z = compress_zstd9(&raw);

    let delta = delta_f64_to_bytes(&vals);
    let delta_z = compress_zstd9(&delta);

    // ts：原始 i64 与 delta-of-delta（Gorilla 风格）
    let ts_raw: Vec<u8> = ts.iter().flat_map(|x| x.to_le_bytes()).collect();
    let ts_raw_z = compress_zstd9(&ts_raw);
    let mut ts_dod: Vec<u8> = Vec::with_capacity(ts.len() * 8);
    ts_dod.extend_from_slice(&ts[0].to_le_bytes());
    let mut prev_delta = 0i64;
    for w in ts.windows(2) {
        let d = w[1] - w[0];
        ts_dod.extend_from_slice(&(d - prev_delta).to_le_bytes());
        prev_delta = d;
    }
    let ts_dod_z = compress_zstd9(&ts_dod);

    println!();
    println!("=== 值/时间戳 编码方案 × ZSTD9 对比（{} 点，整段一次性压） ===", n as usize);
    println!("值: 原始f64→ZSTD9       = {:8.2} B/点", raw_z.len() as f64 / n);
    println!("值: delta(f64差)→ZSTD9  = {:8.2} B/点", delta_z.len() as f64 / n);
    println!("ts: 原始i64→ZSTD9       = {:8.2} B/点", ts_raw_z.len() as f64 / n);
    println!("ts: delta-of-delta→ZSTD9= {:8.2} B/点", ts_dod_z.len() as f64 / n);
}

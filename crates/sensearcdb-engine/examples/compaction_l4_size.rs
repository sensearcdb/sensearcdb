//! compaction 压缩策略评估 benchmark（独立 harness，不改生产代码）。
//!
//! 回答唯一实验问题：在最终归档文件（L4）层，两种方式的文件大小对比——
//!
//! - **方式 A（block 拼接）**：L0 flush = 列编码 + LZ4；L0→L1 = 归并 + ZSTD9（只压这一次）；
//!   L1→L2 / L2→L3 / final→L4 全部只做 block 级拼接（复制已编码字节，不解码/不重编码/不再压缩）。
//! - **方式 B（逐级重压）**：L0 flush 相同；L0→L1、L1→L2、L2→L3、final→L4 每一级都做
//!   「解码 → 归并排序 → 重新列编码(Gorilla/DoD/Simple8b) → ZSTD9」。
//!
//! 关键简化（口径一致）：
//! 1. L0 的 LZ4 只是过渡态，两种方式在 L0→L1 都会丢弃 LZ4 字节改压 ZSTD9，
//!    因此 harness 直接从「每表 ColumnarChunk 数据」出发，不落 L0 的 LZ4 字节。
//! 2. 方式 A 的 block 拼接「复制字节、不改 block 内容」，故 L4 的 block 集合 == L1 的
//!    block 集合，L4 文件大小 == L1 文件大小（header/L2/TableIndex/footer 结构相同，
//!    block 数 / table 数相同）。harness 用「一次 ZSTD9 encode」代表方式 A 的 L4。
//!    依据设计文档 docs/compaction-merge-optimization-design.md §3.1：非重叠 block 直接
//!    复制原始字节，不重压；顺序数据下重编码占比 ≈ 0。
//! 3. 方式 B 用「encode → SacRowIter 读回 → 重建 chunk → 再 encode」循环 4 次，
//!    对应 L0→L1、L1→L2、L2→L3、final→L4 四级的真实解码-重压链路。
//! 4. 当前 `SacLevel` 枚举只有 L0/L1/L2（header.rs），L3/L4 尚未落代码；harness 统一用
//!    `SacLevel::L2`（compression() = ZSTD9）表示「ZSTD9 归档级」。这不影响 block 字节
//!    （压缩算法都是 ZSTD9），且两种方式用同一 level，口径一致。
//!
//! 数据：1 表 1 tag 核心场景 —— 1000 张表（卡车）× 每表 1 列（1 个 f64 测点），
//! 24h（1s 等间隔，86_400 点/表，共 8640 万点），浮点传感值带平滑漂移 + 周期 + 小幅噪声
//! （模拟工业测点如温度/压力/油耗），时间戳等间隔、顺序数据。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use sensearcdb_engine::compression::CompressionType;
use sensearcdb_engine::memtable::ColumnarChunk;
use sensearcdb_engine::memtable::ColVec;
use sensearcdb_engine::sac::header::SacLevel;
use sensearcdb_engine::sac::reader::SacRowIter;
use sensearcdb_engine::sac::writer::SacWriter;
use sensearcdb_engine::TenantId;

/// 表数量（卡车数量）：1000 表，1 表 1 tag。
const N_TABLES: u32 = 1000;
/// 每表点数（24h @ 1s）。
const POINTS_PER_TABLE: usize = 86_400;
/// 等间隔时间戳步长（1s = 1e9 ns）。
const INTERVAL_NS: i64 = 1_000_000_000;
/// 起始时间戳（epoch 纳秒，任意一天边界，24h 跨度）。
const START_TS: i64 = 1_760_000_000_000_000_000;

/// 确定性 PRNG（xorshift64），保证数据可复现。
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
    /// 均匀 [0, 1)。
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// 均匀 [-0.5, 0.5)。
    fn noise(&mut self) -> f64 {
        self.next_f64() - 0.5
    }
}

/// 生成单表（单卡车）24h 顺序数据：1 列 1 个 f64 测点，带平滑漂移 + 周期 + 小幅噪声。
///
/// 1 表 1 tag 场景（本项目核心定位）：每表仅 column_id=0 一个 f64 传感值。
/// 模拟工业测点（温度/压力/油耗）：值随一天时间平滑漂移，叠加日周期与慢周期正弦，
/// 再加确定性小幅噪声（非单一值/单点），保证 Gorilla XOR 编码下的真实分布。
fn generate_table(table_id: u32) -> ColumnarChunk {
    let mut rng = Rng(
        0x9E37_79B9_7F4A_7C15u64 ^ (table_id as u64).wrapping_mul(0x0000_0100_0000_01B3),
    );
    let n = POINTS_PER_TABLE;

    let timestamps: Vec<i64> = (0..n)
        .map(|i| START_TS + (i as i64) * INTERVAL_NS)
        .collect();

    // 表间差异基准（每表不同，保证 1000 表之间值域/相位不重复）。
    let base = 20.0 + rng.next_f64() * 20.0; // 基准值 20~40（温度/油耗尺度）
    let drift = (rng.next_f64() - 0.5) * 10.0; // 全天慢漂移 ±5 单位/天
    let amp = 0.5 + rng.next_f64() * 1.5; // 日周期主幅度
    let amp2 = 0.1 + rng.next_f64() * 0.3; // 慢周期次幅度
    let phase = rng.next_f64() * std::f64::consts::TAU; // 相位偏移

    let mut values = Vec::with_capacity(n);
    for i in 0..n {
        let t = i as f64;
        let day = t / 86_400.0; // 0..1 天
        let v = base
            + drift * day
            + amp * (t / 7200.0).sin()
            + amp2 * (t / 900.0 + phase).sin()
            + rng.noise() * 0.05; // 小幅噪声
        values.push(v);
    }

    let mut columns: HashMap<u16, ColVec> = HashMap::new();
    columns.insert(0, ColVec::F64(values));

    ColumnarChunk {
        table_id,
        tenant_id: TenantId::DEFAULT,
        timestamps,
        columns,
        presence: HashMap::new(), // 全 dense，无 presence 位图
        qualities: vec![0xC0; n],
        ts_index: BTreeMap::new(), // 顺序数据，惰性路径
        point_count: n,
        out_of_order: false,
        max_wal_seq: None,
        min_wal_seq: None,
    }
}

/// 将一组 chunk 按 (table_id) 升序 encode 为一个 ZSTD9 归档文件，返回 (路径, 文件字节数)。
///
/// 使用生产写路径 `SacFileBuilder::append_chunk`（内部按 4096 行切 block + 列编码 +
/// ZSTD9 压缩），level 统一用 `SacLevel::L2`（ZSTD9）。
fn encode_chunks(dir: &Path, chunks: &[ColumnarChunk]) -> (PathBuf, u64) {
    let writer = SacWriter::new(dir, CompressionType::Zstd9);
    let tenant_id = TenantId::DEFAULT.as_u32();
    let mut builder = writer
        .begin_write(tenant_id, START_TS, SacLevel::L2, 1, START_TS)
        .expect("begin_write");
    for chunk in chunks {
        builder.append_chunk(chunk, None).expect("append_chunk");
    }
    let path = builder.finish().expect("finish");
    let size = std::fs::metadata(&path).expect("metadata").len();
    (path, size)
}

/// 读回一个 SAC 文件，重建 `Vec<ColumnarChunk>`（每表一个，按 table_id 升序）。
///
/// 用生产读路径 `SacRowIter::new_all`（全表流式读）→ `SacRow` → `ColumnarChunk::append`
/// 重建；并断言读回总点数 == `total_points`（口径一致、无丢点）。
fn read_all_chunks(path: &Path, total_points: usize) -> Vec<ColumnarChunk> {
    let end_ts = START_TS + (POINTS_PER_TABLE as i64 - 1) * INTERVAL_NS;
    let iter = SacRowIter::new_all(path, TenantId::DEFAULT, START_TS, end_ts, &[], None)
        .expect("open sac");

    // 流式读回：不物化整份 Vec<SacRow>（8640 万点会触发海量 per-row 堆分配 + OOM），
    // 逐行重建 chunk，随读随 append。
    let mut chunks: Vec<ColumnarChunk> = Vec::new();
    let mut cur: Option<ColumnarChunk> = None;
    let mut count = 0usize;
    for row in iter {
        let row = row.expect("read sac");
        count += 1;
        if cur.as_ref().map(|c| c.table_id) != Some(row.table_id) {
            if let Some(c) = cur.take() {
                chunks.push(c);
            }
            cur = Some(ColumnarChunk::new(TenantId::DEFAULT, row.table_id));
        }
        let c = cur.as_mut().expect("cur must be present");
        c.append(row.ts, &row.values, &row.qualities, 0);
    }
    if let Some(c) = cur {
        chunks.push(c);
    }
    assert_eq!(
        count, total_points,
        "口径不一致：读回 {} 点 != 原始 {} 点",
        count, total_points
    );
    chunks
}

fn main() {
    let tmp = std::env::temp_dir().join("sensearc_compaction_l4_bench");
    let _ = std::fs::remove_dir_all(&tmp);
    let dir_a = tmp.join("A");
    let dir_b = tmp.join("B");
    std::fs::create_dir_all(&dir_a).expect("mkdir A");
    std::fs::create_dir_all(&dir_b).expect("mkdir B");

    // 1. 生成数据（两种方式共享同一份）。
    let t0 = std::time::Instant::now();
    let chunks: Vec<ColumnarChunk> = (0..N_TABLES).map(generate_table).collect();
    let total_points: usize = chunks.iter().map(|c| c.point_count).sum();
    eprintln!(
        "[data] {} 表 × {} 点 = {} 点，生成耗时 {:?}",
        N_TABLES,
        POINTS_PER_TABLE,
        total_points,
        t0.elapsed()
    );

    // 2. 方式 A：L0→L1 压一次 ZSTD9，后续 block 拼接（复制字节，不改 block 内容）。
    //    因此 L4 大小 == L1 文件大小。
    let t_a = std::time::Instant::now();
    let (_, size_a) = encode_chunks(&dir_a, &chunks);
    eprintln!("[方式A] encode 一次(ZSTD9) 耗时 {:?}", t_a.elapsed());

    // 3. 方式 B：逐级重压 4 次（L0→L1→L2→L3→final→L4），每级解码-归并-重编码-ZSTD9。
    let t_b = std::time::Instant::now();
    let enc1 = encode_chunks(&dir_b, &chunks); // L1
    // 方式 B 的 L1 与方式 A 的 L4 应逐字节同大小（同一份数据、同一次 ZSTD9 encode）。
    assert_eq!(enc1.1, size_a, "方式B的L1 与 方式A的L4 大小应一致（共同起点）");
    let mut cur = read_all_chunks(&enc1.0, total_points);
    let mut size_b = enc1.1;
    for _ in 0..3 {
        let enc = encode_chunks(&dir_b, &cur); // L2 / L3 / L4
        size_b = enc.1;
        cur = read_all_chunks(&enc.0, total_points);
    }
    eprintln!("[方式B] 逐级重压 4 次 耗时 {:?}", t_b.elapsed());

    // 4. 结果。
    let (a, b) = (size_a as i128, size_b as i128);
    let diff = b - a;
    let pct = if a == 0 {
        0.0
    } else {
        diff as f64 / a as f64 * 100.0
    };
    println!("========== 结果（仅 L4 层） ==========");
    println!("方式 A (block 拼接)  L4 大小: {} 字节", a);
    println!("方式 B (逐级重压)    L4 大小: {} 字节", b);
    println!("差值 (B - A): {} 字节 ({:.4}%)", diff, pct);

    let _ = std::fs::remove_dir_all(&tmp);
}

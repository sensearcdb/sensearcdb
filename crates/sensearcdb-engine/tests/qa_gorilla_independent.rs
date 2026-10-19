//! 独立 QA 验证（fresh eyes，非工程师自测复述）：
//! `encoding::gorilla` 的 `BitWriter` 由逐 bit 写改为 u64 位缓冲批量打包后，
//! 编码输出必须与「直白逐 bit 参考实现」**逐字节一致**（SAC 文件格式红线）。
//!
//! 本文件的参考实现与工程师测试模块里的 `RefBitWriter` 结构不同：
//! - 工程师版：`current_byte |= 1 << (7 - bits_in_byte)`（按位或写字节）。
//! - 本文件版：`acc = (acc << 1) | bit`（左移累积，finish 时左对齐）。
//! 两者若都正确应产出相同字节；若工程师参考实现有隐藏 bug，本独立实现可交叉发现。

use sensearcdb_engine::encoding::gorilla::{
    GorillaDecoder, GorillaEncoder, GorillaF32Decoder, GorillaF32Encoder,
};
use sensearcdb_engine::encoding::{Decoder, Encoder};

// ───────────────────────── 独立逐 bit 参考写入器 ─────────────────────────

struct IndependentBitWriter {
    out: Vec<u8>,
    acc: u8,
    n: u32,
}

impl IndependentBitWriter {
    fn new(cap: usize) -> Self {
        Self {
            out: Vec::with_capacity(cap),
            acc: 0,
            n: 0,
        }
    }

    /// MSB-first 逐 bit 写入（字节内高位先行）。
    fn bit(&mut self, b: bool) {
        self.acc = (self.acc << 1) | (b as u8);
        self.n += 1;
        if self.n == 8 {
            self.out.push(self.acc);
            self.acc = 0;
            self.n = 0;
        }
    }

    /// 写入 `count` 个 bit，`value` 的 MSB（bit count-1）先写。
    fn bits(&mut self, value: u64, count: u32) {
        for i in (0..count).rev() {
            self.bit(((value >> i) & 1) == 1);
        }
    }

    /// 剩余 bit 左对齐 padding 到整字节。
    fn finish(mut self) -> Vec<u8> {
        if self.n > 0 {
            self.out.push(self.acc << (8 - self.n));
        }
        self.out
    }
}

// ───────────────────────── 独立参考编码器（直白逐 bit） ─────────────────────────

fn ref_encode_f64(data: &[f64]) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    let mut w = IndependentBitWriter::new(data.len() * 8);
    let first = data[0].to_bits();
    w.bits(first, 64);
    let mut prev = first;
    for &v in &data[1..] {
        let cur = v.to_bits();
        let xor = prev ^ cur;
        if xor == 0 {
            w.bit(false);
        } else {
            w.bit(true);
            let lz = xor.leading_zeros();
            let tz = xor.trailing_zeros();
            let mb = 64 - lz - tz;
            w.bits(lz as u64, 6);
            w.bits((mb - 1) as u64, 6);
            w.bits(xor >> tz, mb);
        }
        prev = cur;
    }
    let mut r = Vec::with_capacity(4 + w.out.len());
    r.extend_from_slice(&(data.len() as u32).to_le_bytes());
    r.extend(w.finish());
    r
}

fn ref_encode_f32(data: &[f32]) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    let mut w = IndependentBitWriter::new(data.len() * 4);
    let first = data[0].to_bits();
    w.bits(first as u64, 32);
    let mut prev = first;
    for &v in &data[1..] {
        let cur = v.to_bits();
        let xor = prev ^ cur;
        if xor == 0 {
            w.bit(false);
        } else {
            w.bit(true);
            let lz = xor.leading_zeros();
            let tz = xor.trailing_zeros();
            let mb = 32 - lz - tz;
            w.bits(lz as u64, 5);
            w.bits((mb - 1) as u64, 5);
            w.bits((xor >> tz) as u64, mb);
        }
        prev = cur;
    }
    let mut r = Vec::with_capacity(4 + w.out.len());
    r.extend_from_slice(&(data.len() as u32).to_le_bytes());
    r.extend(w.finish());
    r
}

// ───────────────────────── 断言辅助 ─────────────────────────

fn assert_f64_bytes_and_roundtrip(data: &[f64], ctx: &str) {
    let new_bytes = GorillaEncoder::encode(data);
    let ref_bytes = ref_encode_f64(data);
    assert_eq!(
        new_bytes, ref_bytes,
        "f64 位缓冲编码 != 独立逐 bit 参考（{}）: data={:?}",
        ctx,
        data.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
    );

    let decoded = GorillaDecoder::decode(&new_bytes);
    assert_eq!(decoded.len(), data.len(), "round-trip 长度不符（{}）", ctx);
    for (a, b) in data.iter().zip(decoded.iter()) {
        if a.is_nan() {
            assert!(b.is_nan(), "期望 NaN（{}），实际 bits={:016x}", ctx, b.to_bits());
        } else {
            assert_eq!(a.to_bits(), b.to_bits(), "round-trip bit 不一致（{}）", ctx);
        }
    }
}

fn assert_f32_bytes_and_roundtrip(data: &[f32], ctx: &str) {
    let new_bytes = GorillaF32Encoder::encode(data);
    let ref_bytes = ref_encode_f32(data);
    assert_eq!(
        new_bytes, ref_bytes,
        "f32 位缓冲编码 != 独立逐 bit 参考（{}）: data={:?}",
        ctx,
        data.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
    );

    let decoded = GorillaF32Decoder::decode(&new_bytes);
    assert_eq!(decoded.len(), data.len(), "round-trip 长度不符（{}）", ctx);
    for (a, b) in data.iter().zip(decoded.iter()) {
        if a.is_nan() {
            assert!(b.is_nan(), "期望 NaN（{}）", ctx);
        } else {
            assert_eq!(a.to_bits(), b.to_bits(), "round-trip bit 不一致（{}）", ctx);
        }
    }
}

// ───────────────────────── 确定性随机源（xorshift64） ─────────────────────────

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

// ───────────────────────── 边界用例 ─────────────────────────

#[test]
fn qa_gorilla_f64_boundary_bytes_identical() {
    let cases: Vec<(&str, Vec<f64>)> = vec![
        ("empty", vec![]),
        ("single +0", vec![0.0]),
        ("single -0", vec![-0.0]),
        ("single min", vec![f64::MIN]),
        ("single max", vec![f64::MAX]),
        ("single min_positive", vec![f64::MIN_POSITIVE]),
        ("single inf", vec![f64::INFINITY]),
        ("single neg_inf", vec![f64::NEG_INFINITY]),
        ("single nan_canonical", vec![f64::from_bits(0x7ff8_0000_0000_0000)]),
        ("single nan_negative", vec![f64::from_bits(0xfff8_0000_0000_0000)]),
        ("repeat zero", vec![0.0; 8]),
        ("repeat negzero", vec![-0.0; 8]),
        // -0.0 与 0.0 的 xor：仅符号位（bit 63）翻转 → leading=0, trailing=63, meaningful=1。
        ("negzero zero sign flip", vec![-0.0, 0.0, -0.0, 0.0]),
        ("sign flip 1/-1", vec![1.0, -1.0, 2.0, -2.0, 3.0, -3.0]),
        ("inf sign flip", vec![f64::INFINITY, f64::NEG_INFINITY, f64::INFINITY]),
        ("min max min", vec![f64::MIN, f64::MAX, f64::MIN]),
        // 次正规数（denormal）：MIN_POSITIVE 及其相邻位模式。
        (
            "denormals",
            vec![
                f64::from_bits(0x0000_0000_0000_0001), // 最小次正规
                f64::from_bits(0x0000_0000_0000_0002),
                f64::from_bits(0x0000_0000_0000_0003),
                f64::from_bits(0x000F_FFFF_FFFF_FFFF), // 最大次正规
                f64::MIN_POSITIVE,                      // 最小正规 = 0x0010_0000_0000_0000
                f64::from_bits(0x0010_0000_0000_0001),
                0.0,
            ],
        ),
        // meaningful == 64：xor 的 bit 63 与 bit 0 同时置位（0x8000_0000_0000_0001）。
        (
            "meaningful_64",
            vec![
                f64::from_bits(0x0000_0000_0000_0000),
                f64::from_bits(0x8000_0000_0000_0001),
                f64::from_bits(0xFFFF_FFFF_FFFF_FFFF),
            ],
        ),
        // 连续相同值（xor==0 → leading/trailing 全零路径）。
        ("repeat 1.5", vec![1.5f64; 100]),
        // NaN 位模式族：非 canonical NaN 的 xor 产生非常规 leading/trailing。
        (
            "nan_bit_variants",
            vec![
                f64::from_bits(0x7ff8_0000_0000_0000), // canonical quiet NaN
                f64::from_bits(0x7ff8_0000_0000_0001), // quiet NaN + payload
                f64::from_bits(0x7ff0_0000_0000_0001), // signaling NaN
                f64::from_bits(0x7fff_ffff_ffff_ffff), // NaN 全 mantissa 置位
                f64::from_bits(0x7ff0_0000_0000_0000), // NaN mantissa=0 (inf-like)
                f64::from_bits(0xfff8_0000_0000_0000), // 负 canonical NaN
                f64::from_bits(0x7ff8_0000_0000_0001),
                1.0,
            ],
        ),
        // 极小差（相邻浮点，leading 多 / trailing 多）。
        (
            "adjacent floats",
            vec![1.0, 1.0000000000000002, 1.0, 1.0000000000000004],
        ),
        ("pi e mix", vec![std::f64::consts::PI, std::f64::consts::E, 0.0, 1e300, -1e300, 1e-300]),
    ];

    for (ctx, data) in cases {
        assert_f64_bytes_and_roundtrip(&data, ctx);
    }
}

#[test]
fn qa_gorilla_f32_boundary_bytes_identical() {
    let cases: Vec<(&str, Vec<f32>)> = vec![
        ("empty", vec![]),
        ("single +0", vec![0.0f32]),
        ("single -0", vec![-0.0f32]),
        ("single min", vec![f32::MIN]),
        ("single max", vec![f32::MAX]),
        ("single min_positive", vec![f32::MIN_POSITIVE]),
        ("single inf", vec![f32::INFINITY]),
        ("single nan", vec![f32::from_bits(0x7fc0_0000)]),
        ("negzero sign flip", vec![-0.0f32, 0.0f32, -0.0f32, 0.0f32]),
        ("sign flip", vec![1.0f32, -1.0f32, 2.0f32, -2.0f32]),
        ("min max min", vec![f32::MIN, f32::MAX, f32::MIN]),
        (
            "denormals",
            vec![
                f32::from_bits(0x0000_0001),
                f32::from_bits(0x0000_0002),
                f32::from_bits(0x007F_FFFF),
                f32::MIN_POSITIVE, // 0x0080_0000
                f32::from_bits(0x0080_0001),
                0.0f32,
            ],
        ),
        // meaningful == 32：xor 的 bit 31 与 bit 0 同时置位（0x8000_0001）。
        (
            "meaningful_32",
            vec![
                f32::from_bits(0x0000_0000),
                f32::from_bits(0x8000_0001),
                f32::from_bits(0xFFFF_FFFF),
            ],
        ),
        ("repeat 1.5", vec![1.5f32; 50]),
        (
            "nan_bit_variants",
            vec![
                f32::from_bits(0x7fc0_0000),
                f32::from_bits(0x7fc0_0001),
                f32::from_bits(0x7f80_0001),
                f32::from_bits(0x7fff_ffff),
                f32::from_bits(0xffc0_0000),
                1.0f32,
            ],
        ),
    ];

    for (ctx, data) in cases {
        assert_f32_bytes_and_roundtrip(&data, ctx);
    }
}

// ───────────────────────── 随机 fuzz（确定性种子） ─────────────────────────

#[test]
fn qa_gorilla_f64_random_fuzz_bytes_identical() {
    let mut rng = XorShift64::new(0x9E37_79B9_7F4A_7C15);
    for round in 0..5000u32 {
        let len = 1 + (rng.next() % 200) as usize;
        let data: Vec<f64> = (0..len).map(|_| f64::from_bits(rng.next())).collect();
        assert_f64_bytes_and_roundtrip(&data, &format!("fuzz round {}", round));
    }
}

#[test]
fn qa_gorilla_f32_random_fuzz_bytes_identical() {
    let mut rng = XorShift64::new(0xC0FFEE_1234_5678);
    for round in 0..5000u32 {
        let len = 1 + (rng.next() % 200) as usize;
        let data: Vec<f32> = (0..len).map(|_| f32::from_bits(rng.next() as u32)).collect();
        assert_f32_bytes_and_roundtrip(&data, &format!("fuzz round {}", round));
    }
}

// ───────────────────────── 跨 nbits 边界（flush 边界全覆盖） ─────────────────────────

/// 构造 bit 长度在 1..=64 之间各异的 xor 序列，确保 flush_bytes 在 nbits=0..7
/// 各种偏移处都被触发（覆盖 buffer 跨字节对齐的每一种相位）。
#[test]
fn qa_gorilla_f64_flush_phase_coverage() {
    // 用一组精心挑选的位模式，使相邻 xor 的 meaningful 覆盖 1..=64 的多种取值。
    let patterns: Vec<u64> = vec![
        0x0000_0000_0000_0001, // lz=63 tz=0  mb=1
        0x8000_0000_0000_0000, // lz=0  tz=63 mb=1
        0x0000_0000_0000_00FF, // lz=56 tz=0  mb=8
        0x0000_0000_0000_FF00, // lz=48 tz=8  mb=8
        0xFFFF_FFFF_0000_0000, // lz=0  tz=32 mb=32
        0x0000_0000_FFFF_FFFF, // lz=32 tz=0  mb=32
        0x8000_0000_0000_0001, // lz=0  tz=0  mb=64
        0x7FFF_FFFF_FFFF_FFFF, // lz=1  tz=0  mb=63
        0xFFFF_FFFF_FFFF_FFFE, // lz=0  tz=1  mb=63
        0x000F_FFFF_FFFF_FFFF, // lz=12 tz=0  mb=52
        0x5555_5555_5555_5555, // lz=1  tz=0  mb=63
        0xAAAA_AAAA_AAAA_AAAA, // lz=0  tz=1  mb=63
    ];
    let mut data: Vec<f64> = vec![f64::from_bits(0)];
    let mut cur: u64 = 0;
    for p in patterns.iter().cycle().take(500) {
        cur ^= p; // 每次 xor 产生固定 leading/trailing 模式
        data.push(f64::from_bits(cur));
    }
    assert_f64_bytes_and_roundtrip(&data, "flush phase f64");
}

#[test]
fn qa_gorilla_f32_flush_phase_coverage() {
    let patterns: Vec<u32> = vec![
        0x0000_0001, // mb=1
        0x8000_0000, // mb=1
        0x0000_00FF, // mb=8
        0x0000_FF00, // mb=8
        0xFFFF_0000, // mb=16
        0x0000_FFFF, // mb=16
        0x8000_0001, // mb=32
        0x7FFF_FFFF, // mb=31
        0xFFFF_FFFE, // mb=31
        0x007F_FFFF, // mb=23
        0x5555_5555, // mb=31
        0xAAAA_AAAA, // mb=31
    ];
    let mut data: Vec<f32> = vec![f32::from_bits(0)];
    let mut cur: u32 = 0;
    for p in patterns.iter().cycle().take(500) {
        cur ^= p;
        data.push(f32::from_bits(cur));
    }
    assert_f32_bytes_and_roundtrip(&data, "flush phase f32");
}

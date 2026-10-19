//! Gorilla f64 编解码压力测试：随机位模式全分布 roundtrip。
//!
//! 目的：复现生产环境 `gorilla.rs:276`（decode `64 - leading - meaningful`
//! 下溢 panic）。若纯 encode→decode roundtrip 就炸，说明 BitWriter/解码器
//! 自身不自洽；不炸则问题在 SAC block 多列流封装层。

use sensearcdb_engine::encoding::gorilla::{GorillaDecoder, GorillaEncoder};
use sensearcdb_engine::encoding::{Decoder, Encoder};

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
}

fn main() {
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let mut cases = 0u64;
    let mut values_checked = 0u64;

    // 1. 纯随机位模式（全分布：leading/trailing/meaningful 全覆盖）。
    for _ in 0..200_000 {
        let len = 2 + (rng.next_u64() % 64) as usize;
        let data: Vec<f64> = (0..len).map(|_| f64::from_bits(rng.next_u64())).collect();
        roundtrip(&data, &mut cases, &mut values_checked);
    }

    // 2. 极端值模式：NaN 全变体 / inf / 0 / 负零 / min / max / 同值 / 递增低位。
    let extremes: Vec<f64> = vec![
        f64::NAN,
        f64::from_bits(0x7FF0_0000_0000_0001), // sNaN
        f64::from_bits(0xFFF8_0000_0000_0001), // -qNaN 变体
        f64::INFINITY,
        f64::NEG_INFINITY,
        0.0,
        -0.0,
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::from_bits(u64::MAX), // 全 1（NaN 变体）
        f64::from_bits(1),        // 最小次正规
        1.0,
        -1.0,
    ];
    for a in &extremes {
        for b in &extremes {
            let data = vec![*a, *b, *a, *b, *b, *a];
            roundtrip(&data, &mut cases, &mut values_checked);
        }
    }

    // 3. 相邻值仅差最低位（meaningful=1）/ 仅差最高位（leading=0, meaningful 大）。
    for i in 0..200_000u64 {
        let base = rng.next_u64();
        let flip = 1u64 << (i % 64);
        let data = vec![f64::from_bits(base), f64::from_bits(base ^ flip)];
        roundtrip(&data, &mut cases, &mut values_checked);
    }

    println!(
        "PASS: {cases} 组 roundtrip 全部一致，共校验 {values_checked} 个值"
    );
}

fn roundtrip(data: &[f64], cases: &mut u64, values: &mut u64) {
    let enc = GorillaEncoder::encode(data);
    let dec = GorillaDecoder::decode(&enc);
    assert_eq!(
        dec.len(),
        data.len(),
        "长度不一致: expect {} got {}",
        data.len(),
        dec.len()
    );
    for (i, (a, b)) in data.iter().zip(dec.iter()).enumerate() {
        assert_eq!(
            a.to_bits(),
            b.to_bits(),
            "第 {cases} 组第 {i} 值位模式不一致: {:016x} vs {:016x}",
            a.to_bits(),
            b.to_bits()
        );
    }
    *cases += 1;
    *values += data.len() as u64;
}

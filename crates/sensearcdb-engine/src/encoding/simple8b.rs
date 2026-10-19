//! Simple8b 整数编码器。
//!
//! Simple8b 是一种紧凑的整数压缩算法，将多个小整数打包到一个 64 位字中。
//! 每个 64 位字的前 4 位是 selector（0-15），指示打包模式（每个整数的 bit 数和数量）。
//!
//! ## Selector 表
//!
//! | Selector | 每个整数的 bit 数 | 整数数量 |
//! |----------|-------------------|----------|
//! | 0        | 0                 | 240      |
//! | 1        | 0                 | 120      |
//! | 2        | 1                 | 60       |
//! | 3        | 2                 | 30       |
//! | 4        | 3                 | 20       |
//! | 5        | 4                 | 15       |
//! | 6        | 5                 | 12       |
//! | 7        | 6                 | 10       |
//! | 8        | 7                 | 8        |
//! | 9        | 8                 | 7        |
//! | 10       | 10                | 6        |
//! | 11       | 12                | 5        |
//! | 12       | 15                | 4        |
//! | 13       | 20                | 3        |
//! | 14       | 30                | 2        |
//! | 15       | 60                | 1        |
//!
//! ## 有符号 vs 无符号
//!
//! - `Simple8bEncoder`/`Simple8bDecoder` 处理 I64：先做 **zigzag** 编码把有符号整数
//!   映射为非负整数（负值不再被 `as u64` 强转成巨大值），再按 Simple8b 打包。
//! - `encode_u64`/`decode_u64` 处理 U64/Enum16：按 **原始位宽** 直接打包，不 zigzag，
//!   避免 `u64 > i64::MAX` 经 `as i64` 静默溢出。
//!
//! ## 大值 fallback（raw64）
//!
//! Simple8b selector 表上限为 60 bits。任何「zigzag 后 / 原始位宽」需要 >60 bits 的值
//! 都无法被打包。为避免**静默丢弃整列数据**，payload 首字节为模式标记：
//!
//! ```text
//! [mode u8] + body
//!   mode == 0 (MODE_PACKED): body = [count u32 LE][Simple8b 64 位字 × N]
//!   mode == 1 (MODE_RAW64):  body = [count u32 LE][原始 u64 LE × count]
//! ```
//!
//! 只要序列中存在任一需要 >60 bits 的值，整列 fallback 到 `MODE_RAW64`（原样 8 字节），
//! 读侧按 mode 分支还原；≤60 bits 的值仍走压缩路径，行为与之前完全一致。

use crate::encoding::{Decoder, Encoder};

/// 模式标记：Simple8b 压缩打包。
const MODE_PACKED: u8 = 0;
/// 模式标记：raw64 原样 8 字节存储（>60-bit 值 fallback）。
const MODE_RAW64: u8 = 1;

/// Simple8b 编码器。
pub struct Simple8bEncoder;

/// Simple8b 解码器。
pub struct Simple8bDecoder;

/// Selector 配置表。
#[derive(Debug, Clone, Copy)]
struct Simple8bSelector {
    /// 每个整数的 bit 数。
    bits_per_int: u32,
    /// 可打包的整数数量。
    count: usize,
}

const SELECTOR_TABLE: [Simple8bSelector; 16] = [
    Simple8bSelector {
        bits_per_int: 0,
        count: 240,
    },
    Simple8bSelector {
        bits_per_int: 0,
        count: 120,
    },
    Simple8bSelector {
        bits_per_int: 1,
        count: 60,
    },
    Simple8bSelector {
        bits_per_int: 2,
        count: 30,
    },
    Simple8bSelector {
        bits_per_int: 3,
        count: 20,
    },
    Simple8bSelector {
        bits_per_int: 4,
        count: 15,
    },
    Simple8bSelector {
        bits_per_int: 5,
        count: 12,
    },
    Simple8bSelector {
        bits_per_int: 6,
        count: 10,
    },
    Simple8bSelector {
        bits_per_int: 7,
        count: 8,
    },
    Simple8bSelector {
        bits_per_int: 8,
        count: 7,
    },
    Simple8bSelector {
        bits_per_int: 10,
        count: 6,
    },
    Simple8bSelector {
        bits_per_int: 12,
        count: 5,
    },
    Simple8bSelector {
        bits_per_int: 15,
        count: 4,
    },
    Simple8bSelector {
        bits_per_int: 20,
        count: 3,
    },
    Simple8bSelector {
        bits_per_int: 30,
        count: 2,
    },
    Simple8bSelector {
        bits_per_int: 60,
        count: 1,
    },
];

/// 找到适合一批值的 selector。
///
/// 返回 `(selector 下标, 可打包数量)`；若这批值中的最大值所需的 bit 数超过
/// selector 表上限（60 bit），返回 `None`。
fn find_selector(values: &[u64], start: usize) -> Option<(usize, usize)> {
    // 找这批值中最大的，确定所需 bit 数
    let max_val = values[start..].iter().take(240).max().copied().unwrap_or(0);
    let bits_needed = if max_val == 0 {
        0
    } else {
        64 - max_val.leading_zeros()
    };

    // 在 selector 表中找到合适且能装最多的（从低 selector 开始 = 更多值/字）
    for (sel_idx, sel) in SELECTOR_TABLE.iter().enumerate() {
        if sel.bits_per_int as u32 >= bits_needed {
            let count = sel.count.min(values.len() - start);
            return Some((sel_idx, count));
        }
    }

    None
}

/// 将非负 u64 序列按 Simple8b 压缩打包（`MODE_PACKED` 的 body，含 4 字节计数前缀）。
///
/// 前置条件：所有值均 ≤ 60 bits（由调用方 `encode_u64_payload` 预检保证）。
fn pack_u64(values: &[u64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut buf = Vec::with_capacity(values.len() * 2); // 预估
    let mut pos = 0usize;

    while pos < values.len() {
        if let Some((sel_idx, count)) = find_selector(values, pos) {
            let sel = &SELECTOR_TABLE[sel_idx];

            let mut word: u64 = (sel_idx as u64) << 60; // selector 在高 4 位

            for i in 0..count {
                let val = values[pos + i];
                let shift = sel.bits_per_int * i as u32;
                word |= val << shift;
            }

            buf.extend_from_slice(&word.to_le_bytes());
            pos += count;
        } else {
            // 理论上不可达：`encode_u64_payload` 已把含 >60-bit 值的序列路由到 raw64。
            // 保留分支仅为防御，正常路径绝不静默丢值。
            debug_assert!(false, "pack_u64 received a value needing >60 bits");
            break;
        }
    }

    // 前缀写入数据点数
    let mut result = Vec::with_capacity(4 + buf.len());
    result.extend_from_slice(&(values.len() as u32).to_le_bytes());
    result.extend(buf);
    result
}

/// 从 `MODE_PACKED` body（4 字节计数前缀 + 64 位字）解包为非负 u64 序列。
fn unpack_u64(bytes: &[u8]) -> Vec<u64> {
    if bytes.len() < 4 {
        return Vec::new();
    }

    let total_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap_or([0u8; 4])) as usize;
    if total_count == 0 {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(total_count);

    let word_bytes = &bytes[4..];
    let num_words = word_bytes.len() / 8;

    for wi in 0..num_words {
        let word = u64::from_le_bytes(
            word_bytes[wi * 8..(wi + 1) * 8]
                .try_into()
                .unwrap_or([0u8; 8]),
        );

        let sel_idx = (word >> 60) as usize;
        if sel_idx >= SELECTOR_TABLE.len() {
            break;
        }

        let sel = &SELECTOR_TABLE[sel_idx];

        for i in 0..sel.count {
            let shift = sel.bits_per_int * i as u32;
            let mask = (1u64 << sel.bits_per_int).wrapping_sub(1);
            let val = (word >> shift) & mask;
            result.push(val);

            if result.len() >= total_count {
                break;
            }
        }

        if result.len() >= total_count {
            break;
        }
    }

    result.truncate(total_count);
    result
}

/// 从 `MODE_RAW64` body（4 字节计数前缀 + 原始 u64 LE）解包为非负 u64 序列。
fn unpack_raw64(bytes: &[u8]) -> Vec<u64> {
    if bytes.len() < 4 {
        return Vec::new();
    }

    let total_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap_or([0u8; 4])) as usize;
    if total_count == 0 {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(total_count);
    let mut off = 4usize;

    for _ in 0..total_count {
        if off + 8 > bytes.len() {
            break;
        }
        let v = u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap_or([0u8; 8]));
        result.push(v);
        off += 8;
    }

    result
}

/// 将非负 u64 序列编码为「1 字节模式标记 + body」的 Simple8b payload。
///
/// 含任一 >60-bit 值时整列 fallback 到 `MODE_RAW64`，绝不静默丢值。
fn encode_u64_payload(values: &[u64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    // 任一值 ≥ 2^60（需要 >60 bits）→ 整列 raw64。
    let needs_raw = values.iter().any(|&v| v >> 60 != 0);

    if needs_raw {
        let mut result = Vec::with_capacity(1 + 4 + values.len() * 8);
        result.push(MODE_RAW64);
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for &v in values {
            result.extend_from_slice(&v.to_le_bytes());
        }
        result
    } else {
        let mut result = Vec::with_capacity(1 + values.len() * 2);
        result.push(MODE_PACKED);
        result.extend_from_slice(&pack_u64(values));
        result
    }
}

/// 从「1 字节模式标记 + body」的 Simple8b payload 解包为非负 u64 序列。
fn decode_u64_payload(bytes: &[u8]) -> Vec<u64> {
    if bytes.is_empty() {
        return Vec::new();
    }

    match bytes[0] {
        MODE_PACKED => unpack_u64(&bytes[1..]),
        MODE_RAW64 => unpack_raw64(&bytes[1..]),
        _ => Vec::new(),
    }
}

/// Zigzag 编码：将有符号 i64 映射为非负 u64。
///
/// 映射关系：`0 → 0`、`-1 → 1`、`1 → 2`、`-2 → 3`、`2 → 4`……（小负数 → 小正数）。
#[inline]
fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Zigzag 解码：将非负 u64 还原为有符号 i64。
#[inline]
fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// 无符号 u64 序列的 Simple8b 编码（原始位宽，无 zigzag）。
///
/// 含 >60-bit 值时自动 fallback 到 raw64，不丢值。
///
/// # 参数
/// - `data`: 非负 u64 整数序列。
///
/// # 返回值
/// 编码后的字节数组（1 字节模式标记 + body）。
pub fn encode_u64(data: &[u64]) -> Vec<u8> {
    encode_u64_payload(data)
}

/// 无符号 u64 序列的 Simple8b 解码（原始位宽，无 zigzag）。
///
/// # 参数
/// - `bytes`: `encode_u64` 产生的字节数组。
///
/// # 返回值
/// 解码后的 u64 序列。
pub fn decode_u64(bytes: &[u8]) -> Vec<u64> {
    decode_u64_payload(bytes)
}

impl Encoder<i64> for Simple8bEncoder {
    /// 对 i64 序列进行 Simple8b 编码。
    ///
    /// 先做 zigzag 编码将负值映射为非负值，再按 Simple8b 打包；
    /// zigzag 后需要 >60 bits 的极端值 fallback 到 raw64，不丢值。
    ///
    /// # 参数
    /// - `data`: i64 整数序列。
    ///
    /// # 返回值
    /// 编码后的字节数组（1 字节模式标记 + body）。
    fn encode(data: &[i64]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let unsigned: Vec<u64> = data.iter().map(|&v| zigzag_encode(v)).collect();
        encode_u64_payload(&unsigned)
    }
}

impl Decoder<i64> for Simple8bDecoder {
    /// 从 Simple8b 编码的字节数组解码 i64 序列。
    ///
    /// # 参数
    /// - `bytes`: Simple8b 编码的字节数组。
    ///
    /// # 返回值
    /// 解码后的 i64 序列。
    fn decode(bytes: &[u8]) -> Vec<i64> {
        decode_u64_payload(bytes)
            .into_iter()
            .map(zigzag_decode)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple8b_round_trip_empty() {
        let data: Vec<i64> = vec![];
        let encoded = Simple8bEncoder::encode(&data);
        let decoded = Simple8bDecoder::decode(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_simple8b_round_trip_small() {
        let data: Vec<i64> = (0..50).collect();
        let encoded = Simple8bEncoder::encode(&data);
        let decoded = Simple8bDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_simple8b_round_trip_large_values() {
        let data: Vec<i64> = vec![1_000_000_000_000, 2_000_000_000_000, 3_000_000_000_000];
        let encoded = Simple8bEncoder::encode(&data);
        let decoded = Simple8bDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_simple8b_round_trip_mixed() {
        let data: Vec<i64> = vec![0, 1, 2, 1000, 1_000_000, 0, 5, 10, 20, 30];
        let encoded = Simple8bEncoder::encode(&data);
        let decoded = Simple8bDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_simple8b_round_trip_negative() {
        // 负值 I64 应无损 round-trip（真 zigzag，而非 `as u64` 强转）。
        let data: Vec<i64> = vec![10, -20, 30, -1, 0, -1000, 100000, -2_000_000_000];
        let encoded = Simple8bEncoder::encode(&data);
        let decoded = Simple8bDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_simple8b_i64_extreme_round_trip() {
        // I64 极值 zigzag 后需要 >60 bits → 应 fallback raw64，不丢值。
        let data: Vec<i64> = vec![i64::MIN, i64::MIN + 1, -(1i64 << 60), -100, 0, i64::MAX];
        let encoded = Simple8bEncoder::encode(&data);
        let decoded = Simple8bDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_simple8b_u64_round_trip() {
        // U64 原始位宽 round-trip，不 zigzag、不 `as i64`。
        let data: Vec<u64> = vec![1, 2, 3, 1000, 1_000_000_000_000];
        let encoded = encode_u64(&data);
        let decoded = decode_u64(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_simple8b_u64_large_round_trip() {
        // U64 大值（≥ 2^60）应 fallback raw64，不静默丢值。
        let data: Vec<u64> = vec![
            u64::MAX - 5,
            u64::MAX,
            1u64 << 63,
            (1u64 << 63) - 1,
            1u64 << 62,
            1,
        ];
        let encoded = encode_u64(&data);
        assert_eq!(encoded[0], MODE_RAW64);
        let decoded = decode_u64(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_simple8b_u64_within_range_round_trip() {
        // 恰好 ≤60 bits 的值应仍走 packed 路径（MODE_PACKED），且无损。
        let data: Vec<u64> = vec![1u64 << 59, (1u64 << 60) - 1, 42];
        let encoded = encode_u64(&data);
        assert_eq!(encoded[0], MODE_PACKED);
        let decoded = decode_u64(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_simple8b_compression() {
        // 小值应该有良好压缩率（zigzag 会把正值翻倍，阈值按 zigzag 后 bit 数放宽）。
        let data: Vec<i64> = (0..240).collect();
        let encoded = Simple8bEncoder::encode(&data);
        // 240 个 i64 = 1920 字节 → 应远小于此（约 329 字节，含 1 字节模式标记）
        assert!(encoded.len() < 400);
    }
}

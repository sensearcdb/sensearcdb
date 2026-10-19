//! Delta-of-Delta 时间戳编码器。
//!
//! 适用于单调递增的时间戳序列。编码过程：
//! 1. 计算相邻时间戳的 delta: `delta[i] = ts[i] - ts[i-1]`
//! 2. 计算 delta 的 delta: `dod[i] = delta[i] - delta[i-1]`
//! 3. 对每个 DoD 值使用 varint 编码
//!
//! Block 边界（每 4096 行）重置编码状态。

use crate::encoding::{Decoder, Encoder};

/// Delta-of-Delta 时间戳编码器。
pub struct DoDEncoder;

impl Encoder<i64> for DoDEncoder {
    /// 对 i64 时间戳序列进行 Delta-of-Delta 编码。
    ///
    /// 编码格式：
    /// - 前 8 字节: 第一个时间戳的原始值 (i64 LE)
    /// - 后续: 逐个 varint 编码的 DoD 值
    ///
    /// # 参数
    /// - `data`: 时间戳序列（必须非空且单调递增）。
    ///
    /// # 返回值
    /// 编码后的字节数组。
    fn encode(data: &[i64]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut buf = Vec::with_capacity(data.len() * 8);

        // 写入第一个时间戳（锚点）
        buf.extend_from_slice(&data[0].to_le_bytes());

        if data.len() == 1 {
            return buf;
        }

        // 计算 delta 和 delta-of-delta
        let mut prev_delta = data[1] - data[0];
        write_varint(&mut buf, prev_delta);

        for i in 2..data.len() {
            let delta = data[i] - data[i - 1];
            let dod = delta - prev_delta;
            write_varint(&mut buf, dod);
            prev_delta = delta;
        }

        buf
    }
}

/// Delta-of-Delta 时间戳解码器。
pub struct DoDDecoder;

impl Decoder<i64> for DoDDecoder {
    /// 从 DoD 编码的字节数组解码 i64 时间戳序列。
    ///
    /// # 参数
    /// - `bytes`: DoD 编码的字节数组。
    ///
    /// # 返回值
    /// 解码后的时间戳序列。
    fn decode(bytes: &[u8]) -> Vec<i64> {
        if bytes.is_empty() {
            return Vec::new();
        }
        if bytes.len() < 8 {
            return Vec::new();
        }

        let mut pos = 0usize;

        // 读取第一个时间戳（锚点）
        let first_ts = i64::from_le_bytes(bytes[0..8].try_into().unwrap_or([0u8; 8]));
        pos += 8;

        let mut result = vec![first_ts];

        if pos >= bytes.len() {
            return result;
        }

        // 读取第一个 delta
        let (first_delta, delta_len) = read_varint(&bytes[pos..]);
        if first_delta == 0 && delta_len == 0 {
            return result;
        }
        pos += delta_len;
        result.push(first_ts + first_delta);
        let mut prev_delta = first_delta;

        // 读取后续 DoD
        while pos < bytes.len() {
            let (dod, dod_len) = read_varint(&bytes[pos..]);
            if dod_len == 0 {
                break;
            }
            pos += dod_len;
            let delta = prev_delta + dod;
            let ts = result.last().copied().unwrap_or(0) + delta;
            result.push(ts);
            prev_delta = delta;
        }

        result
    }
}

/// 写入有符号 varint（zigzag 编码 + LEB128）。
fn write_varint(buf: &mut Vec<u8>, value: i64) {
    // ZigZag 编码：将有符号数映射为无符号数
    let encoded = ((value << 1) ^ (value >> 63)) as u64;

    // LEB128 编码
    let mut v = encoded;
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// 读取有符号 varint。
///
/// # 返回值
/// `(value, bytes_consumed)`。如果无法解析则返回 `(0, 0)`。
fn read_varint(bytes: &[u8]) -> (i64, usize) {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut pos = 0usize;

    while pos < bytes.len() {
        let byte = bytes[pos];
        pos += 1;

        value |= ((byte & 0x7F) as u64) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            break;
        }

        if shift >= 64 {
            // 溢出保护
            return (0, 0);
        }
    }

    if pos == 0 {
        return (0, 0);
    }

    // ZigZag 解码
    let decoded = ((value >> 1) as i64) ^ (-((value & 1) as i64));
    (decoded, pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dod_round_trip_empty() {
        let data: Vec<i64> = vec![];
        let encoded = DoDEncoder::encode(&data);
        let decoded = DoDDecoder::decode(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_dod_round_trip_single() {
        let data = vec![100i64];
        let encoded = DoDEncoder::encode(&data);
        let decoded = DoDDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_dod_round_trip_uniform() {
        // 每秒一个点（等间距）
        let data: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let encoded = DoDEncoder::encode(&data);
        let decoded = DoDDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_dod_round_trip_irregular() {
        // 不规则间隔
        let data = vec![
            0i64,
            1_000_000_000,
            2_500_000_000,
            4_000_000_000,
            4_100_000_000,
            5_000_000_000,
        ];
        let encoded = DoDEncoder::encode(&data);
        let decoded = DoDDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_dod_compression_ratio() {
        // 等间距数据压缩率应很高
        let data: Vec<i64> = (0..1000).map(|i| i * 1_000_000_000).collect();
        let encoded = DoDEncoder::encode(&data);
        // 1000 个 i64 = 8000 字节原始 → 编码后应远小于此
        assert!(encoded.len() < 1200); // 等间距时 DoD 全是 0，远小于原始 8000 字节
    }
}

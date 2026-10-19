//! Run-Length Encoding (RLE) 编码器。
//!
//! 适用于质量码等重复值较多的 u8 序列。
//! 编码格式：
//! - `[run_length: u16 LE][value: u8]` 对
//! - run_length = 连续重复次数

use crate::encoding::{Decoder, Encoder};

/// Run-Length Encoding 编码器。
pub struct RleEncoder;

impl Encoder<u8> for RleEncoder {
    /// 对 u8 序列进行 RLE 编码。
    ///
    /// 编码格式：重复的 `(count: u16 LE, value: u8)` 对。
    ///
    /// # 参数
    /// - `data`: u8 值序列（如质量码）。
    ///
    /// # 返回值
    /// 编码后的字节数组。
    fn encode(data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut buf = Vec::with_capacity(data.len());

        let mut run_start = 0usize;
        let mut current_val = data[0];

        for i in 1..data.len() {
            if data[i] != current_val || (i - run_start) >= u16::MAX as usize {
                // 写入当前 run
                let run_len = (i - run_start) as u16;
                buf.extend_from_slice(&run_len.to_le_bytes());
                buf.push(current_val);

                run_start = i;
                current_val = data[i];
            }
        }

        // 写入最后一个 run
        let run_len = (data.len() - run_start) as u16;
        buf.extend_from_slice(&run_len.to_le_bytes());
        buf.push(current_val);

        buf
    }
}

/// Run-Length Encoding 解码器。
pub struct RleDecoder;

impl Decoder<u8> for RleDecoder {
    /// 从 RLE 编码的字节数组解码 u8 序列。
    ///
    /// # 参数
    /// - `bytes`: RLE 编码的字节数组。
    ///
    /// # 返回值
    /// 解码后的 u8 序列。
    fn decode(bytes: &[u8]) -> Vec<u8> {
        if bytes.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut pos = 0usize;

        while pos + 3 <= bytes.len() {
            let run_len =
                u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap_or([0u8; 2])) as usize;
            let value = bytes[pos + 2];
            pos += 3;

            result.resize(result.len() + run_len, value);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_round_trip_empty() {
        let data: Vec<u8> = vec![];
        let encoded = RleEncoder::encode(&data);
        let decoded = RleDecoder::decode(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_rle_round_trip_uniform() {
        // 全相同值
        let data = vec![0xC0u8; 1000];
        let encoded = RleEncoder::encode(&data);
        // 应该只有 1 个 run: 3 字节
        assert_eq!(encoded.len(), 3);
        let decoded = RleDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_rle_round_trip_varied() {
        let data = vec![0xC0u8, 0xC0, 0xC0, 0x00, 0x00, 0xC0, 0x40];
        let encoded = RleEncoder::encode(&data);
        let decoded = RleDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_rle_single_value() {
        let data = vec![42u8];
        let encoded = RleEncoder::encode(&data);
        assert_eq!(encoded.len(), 3);
        let decoded = RleDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_rle_alternating() {
        // 交替模式：每个 run 长度 = 1
        let data = vec![0u8, 1, 0, 1, 0, 1, 0, 1];
        let encoded = RleEncoder::encode(&data);
        // 8 个 run × 3 字节 = 24
        assert_eq!(encoded.len(), 24);
        let decoded = RleDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }
}

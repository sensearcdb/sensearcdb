//! XOR-Gorilla 浮点编码器。
//!
//! 基于 Facebook Gorilla 论文的浮点值时序压缩算法。
//! 核心思想：相邻浮点值的 XOR 结果通常有大量前导零和尾随零。
//!
//! 编码格式：
//! - 第一个值：原始 64 位写入
//! - 后续值：
//!   - 若 XOR == 0（完全相同）：写入 1 bit '0'
//!   - 否则：写入 '1' + 前导零数 + 有效位数 + 有效位
//!
//! Block 边界（每 4096 行）重置编码状态。

use crate::encoding::{Decoder, Encoder};

/// 前导零计数（bit 级别）。
#[inline]
fn leading_zeros_u64(v: u64) -> u32 {
    v.leading_zeros()
}

/// 尾随零计数（bit 级别）。
#[inline]
fn trailing_zeros_u64(v: u64) -> u32 {
    v.trailing_zeros()
}

/// Bit 写入器：用于构建变长比特流。
///
/// 采用 u64 位缓冲批量打包：待写 bit 攒进 `buffer`，凑满整字节即 flush，
/// 替代旧实现逐 bit 循环（flush 编码段主热点）。bit 序 / 字节序 / padding 与
/// 旧逐 bit 实现**逐字节一致**（MSB-first，字节内高位先行）。
struct BitWriter {
    buf: Vec<u8>,
    /// 待写 bit 累积缓冲：最旧 bit 位于最高位，最新 bit 位于最低位。
    buffer: u64,
    /// `buffer` 中当前有效 bit 数（恒 < 8，因为 flush 会及时排空整字节）。
    nbits: u32,
}

impl BitWriter {
    fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            buffer: 0,
            nbits: 0,
        }
    }

    /// 写入单个 bit。
    #[inline]
    fn write_bit(&mut self, bit: bool) {
        self.buffer = (self.buffer << 1) | (bit as u64);
        self.nbits += 1;
        self.flush_bytes();
    }

    /// 写入 `count` 个 bit（`value` 的 MSB 先写）。
    ///
    /// 采用「>32 位时拆成高/低两段」避免 `u64 << 64` 移位溢出；单段 ≤32 bit 且
    /// `nbits < 8`，故 `nbits + count < 64`，移位安全。
    #[inline]
    fn write_bits(&mut self, value: u64, count: u32) {
        if count == 0 {
            return;
        }
        if count > 32 {
            // 高段（value 的 bit count-1 .. 32）先写，低 32 位后写，保持 MSB-first。
            self.write_bits(value >> 32, count - 32);
            self.write_bits(value & 0xFFFF_FFFF, 32);
            return;
        }
        let mask = if count == 32 {
            u32::MAX as u64
        } else {
            (1u64 << count) - 1
        };
        self.buffer = (self.buffer << count) | (value & mask);
        self.nbits += count;
        self.flush_bytes();
    }

    /// 从 `buffer` 顶部（最旧 bit）整字节 flush 到 `buf`（MSB-first 字节序）。
    #[inline]
    fn flush_bytes(&mut self) {
        while self.nbits >= 8 {
            let byte = (self.buffer >> (self.nbits - 8)) as u8;
            self.buf.push(byte);
            self.nbits -= 8;
            if self.nbits > 0 {
                self.buffer &= (1u64 << self.nbits) - 1;
            } else {
                self.buffer = 0;
            }
        }
    }

    /// 完成写入并返回字节数组。
    fn finish(mut self) -> Vec<u8> {
        if self.nbits > 0 {
            // 剩余 bit 左对齐到字节高位（与旧逐 bit 实现的 padding 一致）。
            let byte = (self.buffer << (8 - self.nbits)) as u8;
            self.buf.push(byte);
        }
        self.buf
    }
}

/// Bit 读取器：从字节流中按 bit 读取。
struct BitReader<'a> {
    bytes: &'a [u8],
    byte_pos: usize,
    bit_pos: u32,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    /// 读取单个 bit。
    fn read_bit(&mut self) -> Option<bool> {
        if self.byte_pos >= self.bytes.len() {
            return None;
        }
        let byte = self.bytes[self.byte_pos];
        let bit = (byte >> (7 - self.bit_pos)) & 1 != 0;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.bit_pos = 0;
            self.byte_pos += 1;
        }
        Some(bit)
    }

    /// 读取 `count` 个 bit。
    fn read_bits(&mut self, count: u32) -> Option<u64> {
        let mut value: u64 = 0;
        for _ in 0..count {
            let bit = self.read_bit()?;
            value = (value << 1) | (bit as u64);
        }
        Some(value)
    }
}

/// XOR-Gorilla 浮点编码器。
pub struct GorillaEncoder;

impl Encoder<f64> for GorillaEncoder {
    /// 对 f64 序列进行 XOR-Gorilla 编码。
    ///
    /// 算法（来源：Facebook Gorilla 论文 §2.4）：
    /// 1. 第一个值：64-bit 原始存储
    /// 2. 后续值：计算 XOR = prev ^ current
    ///    - XOR == 0: 写 1 bit '0'
    ///    - XOR != 0: 写 '1'，然后：
    ///      - 前导零数 (5 bits) + 有效位数 (6 bits) + 有效位
    ///
    /// # 参数
    /// - `data`: f64 值序列。
    ///
    /// # 返回值
    /// 编码后的字节数组。
    fn encode(data: &[f64]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut writer = BitWriter::new(data.len() * 8);

        // 存储数据点数（前 4 字节）
        let count = data.len() as u32;

        // 第一个值：64-bit 原始
        let first_bits = data[0].to_bits();
        writer.write_bits(first_bits, 64);

        let mut prev = first_bits;

        for i in 1..data.len() {
            let current = data[i].to_bits();
            let xor = prev ^ current;

            if xor == 0 {
                // 完全相同：1 bit '0'
                writer.write_bit(false);
            } else {
                // 不同：1 bit '1' + 前导零 + 有效位
                writer.write_bit(true);

                let leading = leading_zeros_u64(xor);
                let trailing = trailing_zeros_u64(xor);
                let meaningful = 64 - leading - trailing;

                writer.write_bits(leading as u64, 6); // 最多 63 个前导零，6 bits 足够
                writer.write_bits((meaningful - 1) as u64, 6); // 有效位长度-1（至少 1 bit）
                writer.write_bits(xor >> trailing, meaningful);
            }

            prev = current;
        }

        // 先写计数再写数据
        let mut result = Vec::with_capacity(4 + writer.buf.capacity());
        result.extend_from_slice(&count.to_le_bytes());
        result.extend(writer.finish());
        result
    }
}

/// XOR-Gorilla 浮点解码器。
pub struct GorillaDecoder;

impl Decoder<f64> for GorillaDecoder {
    /// 从 Gorilla 编码的字节数组解码 f64 序列。
    ///
    /// # 参数
    /// - `bytes`: Gorilla 编码的字节数组。
    ///
    /// # 返回值
    /// 解码后的 f64 值序列。
    fn decode(bytes: &[u8]) -> Vec<f64> {
        if bytes.is_empty() {
            return Vec::new();
        }
        if bytes.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap_or([0u8; 4])) as usize;
        if count == 0 {
            return Vec::new();
        }

        let mut reader = BitReader::new(&bytes[4..]);

        // 第一个值：64-bit 原始
        let first_bits = match reader.read_bits(64) {
            Some(v) => v,
            None => return Vec::new(),
        };
        let first_val = f64::from_bits(first_bits);

        let mut result = vec![first_val];
        let mut prev = first_bits;

        for _ in 1..count {
            let bit = match reader.read_bit() {
                Some(b) => b,
                None => break,
            };

            if !bit {
                // XOR == 0：值不变
                result.push(f64::from_bits(prev));
            } else {
                // 读取前导零和有效位
                let leading = match reader.read_bits(6) {
                    Some(v) => v as u32,
                    None => break,
                };
                let meaningful = match reader.read_bits(6) {
                    Some(v) => (v + 1) as u32,
                    None => break,
                };
                let xor_bits = match reader.read_bits(meaningful) {
                    Some(v) => v,
                    None => break,
                };

                let xor = xor_bits << (64 - leading - meaningful);
                let current = prev ^ xor;
                result.push(f64::from_bits(current));
                prev = current;
            }
        }

        result
    }
}

/// XOR-Gorilla f32 原生编码器（O3=A）。
///
/// 与 f64 Gorilla 同构，仅位宽从 64-bit 降为 32-bit：
/// 前导零/有效位字段各 5 bits（覆盖 0..=31）。
pub struct GorillaF32Encoder;

impl Encoder<f32> for GorillaF32Encoder {
    /// 对 f32 序列进行 32-bit XOR-Gorilla 编码。
    ///
    /// # 参数
    /// - `data`: f32 值序列。
    ///
    /// # 返回值
    /// 编码后的字节数组。
    fn encode(data: &[f32]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut writer = BitWriter::new(data.len() * 4);
        let count = data.len() as u32;

        // 第一个值：32-bit 原始
        let first_bits = data[0].to_bits();
        writer.write_bits(first_bits as u64, 32);

        let mut prev = first_bits;

        for i in 1..data.len() {
            let current = data[i].to_bits();
            let xor = prev ^ current;

            if xor == 0 {
                writer.write_bit(false);
            } else {
                writer.write_bit(true);

                let leading = xor.leading_zeros();
                let trailing = xor.trailing_zeros();
                let meaningful = 32 - leading - trailing;

                writer.write_bits(leading as u64, 5); // 0..=31，5 bits 足够
                writer.write_bits((meaningful - 1) as u64, 5); // 有效位长度-1（至少 1）
                writer.write_bits((xor >> trailing) as u64, meaningful);
            }

            prev = current;
        }

        let mut result = Vec::with_capacity(4 + writer.buf.capacity());
        result.extend_from_slice(&count.to_le_bytes());
        result.extend(writer.finish());
        result
    }
}

/// XOR-Gorilla f32 原生解码器。
pub struct GorillaF32Decoder;

impl Decoder<f32> for GorillaF32Decoder {
    /// 从 32-bit Gorilla 编码的字节数组解码 f32 序列。
    ///
    /// # 参数
    /// - `bytes`: Gorilla 编码的字节数组。
    ///
    /// # 返回值
    /// 解码后的 f32 值序列。
    fn decode(bytes: &[u8]) -> Vec<f32> {
        if bytes.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap_or([0u8; 4])) as usize;
        if count == 0 {
            return Vec::new();
        }

        let mut reader = BitReader::new(&bytes[4..]);

        let first_bits = match reader.read_bits(32) {
            Some(v) => v as u32,
            None => return Vec::new(),
        };
        let mut result = vec![f32::from_bits(first_bits)];
        let mut prev = first_bits;

        for _ in 1..count {
            let bit = match reader.read_bit() {
                Some(b) => b,
                None => break,
            };

            if !bit {
                result.push(f32::from_bits(prev));
            } else {
                let leading = match reader.read_bits(5) {
                    Some(v) => v as u32,
                    None => break,
                };
                let meaningful = match reader.read_bits(5) {
                    Some(v) => (v + 1) as u32,
                    None => break,
                };
                let xor_bits = match reader.read_bits(meaningful) {
                    Some(v) => v as u32,
                    None => break,
                };

                let xor = xor_bits << (32 - leading - meaningful);
                let current = prev ^ xor;
                result.push(f32::from_bits(current));
                prev = current;
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gorilla_round_trip_empty() {
        let data: Vec<f64> = vec![];
        let encoded = GorillaEncoder::encode(&data);
        let decoded = GorillaDecoder::decode(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_gorilla_round_trip_single() {
        let data = vec![3.14f64];
        let encoded = GorillaEncoder::encode(&data);
        let decoded = GorillaDecoder::decode(&encoded);
        assert_eq!(decoded.len(), 1);
        assert!((decoded[0] - 3.14).abs() < 1e-10);
    }

    #[test]
    fn test_gorilla_round_trip_repeating() {
        // 相同值：高压缩率
        let data = vec![1.5f64; 100];
        let encoded = GorillaEncoder::encode(&data);
        // 重复值应极小（远小于原始 800 字节）
        assert!(encoded.len() < 30);
        let decoded = GorillaDecoder::decode(&encoded);
        assert_eq!(decoded.len(), 100);
        for v in &decoded {
            assert!((v - 1.5).abs() < 1e-10);
        }
    }

    #[test]
    fn test_gorilla_round_trip_varied() {
        let data: Vec<f64> = vec![1.0, 1.1, 1.15, 1.2, 1.18, 100.0, 100.1];
        let encoded = GorillaEncoder::encode(&data);
        let decoded = GorillaDecoder::decode(&encoded);
        assert_eq!(decoded.len(), data.len());
        for (a, b) in data.iter().zip(decoded.iter()) {
            assert!((a - b).abs() < 1e-10, "mismatch: {} vs {}", a, b);
        }
    }

    #[test]
    fn test_gorilla_nan_values() {
        let data = vec![f64::NAN, 1.0, 2.0];
        let encoded = GorillaEncoder::encode(&data);
        let decoded = GorillaDecoder::decode(&encoded);
        assert_eq!(decoded.len(), 3);
        assert!(decoded[0].is_nan());
        assert!((decoded[1] - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_gorilla_f32_round_trip_empty() {
        let data: Vec<f32> = vec![];
        let encoded = GorillaF32Encoder::encode(&data);
        let decoded = GorillaF32Decoder::decode(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_gorilla_f32_round_trip_single() {
        let data = vec![3.14f32];
        let encoded = GorillaF32Encoder::encode(&data);
        let decoded = GorillaF32Decoder::decode(&encoded);
        assert_eq!(decoded.len(), 1);
        assert!((decoded[0] - 3.14f32).abs() < 1e-6);
    }

    #[test]
    fn test_gorilla_f32_round_trip_repeating() {
        let data = vec![1.5f32; 100];
        let encoded = GorillaF32Encoder::encode(&data);
        // 重复值应极小（远小于原始 400 字节）
        assert!(encoded.len() < 30);
        let decoded = GorillaF32Decoder::decode(&encoded);
        assert_eq!(decoded.len(), 100);
        for v in &decoded {
            assert!((v - 1.5f32).abs() < 1e-6);
        }
    }

    #[test]
    fn test_gorilla_f32_round_trip_varied() {
        let data: Vec<f32> = vec![1.0, 1.1, 1.15, 1.2, 1.18, 100.0, 100.1];
        let encoded = GorillaF32Encoder::encode(&data);
        let decoded = GorillaF32Decoder::decode(&encoded);
        assert_eq!(decoded.len(), data.len());
        for (a, b) in data.iter().zip(decoded.iter()) {
            assert!((a - b).abs() < 1e-6, "mismatch: {} vs {}", a, b);
        }
    }

    // ── 逐字节回归：位缓冲打包（新）必须与逐 bit（旧）逐字节一致 ──────────────

    /// 参考实现：旧逐 bit 写入器（MSB-first，字节内高位先行）。
    struct RefBitWriter {
        buf: Vec<u8>,
        current_byte: u8,
        bits_in_byte: u32,
    }

    impl RefBitWriter {
        fn new(capacity: usize) -> Self {
            Self {
                buf: Vec::with_capacity(capacity),
                current_byte: 0,
                bits_in_byte: 0,
            }
        }

        fn write_bit(&mut self, bit: bool) {
            if bit {
                self.current_byte |= 1 << (7 - self.bits_in_byte);
            }
            self.bits_in_byte += 1;
            if self.bits_in_byte == 8 {
                self.buf.push(self.current_byte);
                self.current_byte = 0;
                self.bits_in_byte = 0;
            }
        }

        fn write_bits(&mut self, value: u64, count: u32) {
            for i in (0..count).rev() {
                self.write_bit((value >> i) & 1 != 0);
            }
        }

        fn finish(mut self) -> Vec<u8> {
            if self.bits_in_byte > 0 {
                self.buf.push(self.current_byte);
            }
            self.buf
        }
    }

    /// 旧逐 bit f64 Gorilla 编码参考实现（与位缓冲版算法逻辑完全一致）。
    fn encode_f64_reference(data: &[f64]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }
        let mut writer = RefBitWriter::new(data.len() * 8);
        let count = data.len() as u32;
        let first_bits = data[0].to_bits();
        writer.write_bits(first_bits, 64);
        let mut prev = first_bits;
        for i in 1..data.len() {
            let current = data[i].to_bits();
            let xor = prev ^ current;
            if xor == 0 {
                writer.write_bit(false);
            } else {
                writer.write_bit(true);
                let leading = xor.leading_zeros();
                let trailing = xor.trailing_zeros();
                let meaningful = 64 - leading - trailing;
                writer.write_bits(leading as u64, 6);
                writer.write_bits((meaningful - 1) as u64, 6);
                writer.write_bits(xor >> trailing, meaningful);
            }
            prev = current;
        }
        let mut result = Vec::with_capacity(4 + writer.buf.capacity());
        result.extend_from_slice(&count.to_le_bytes());
        result.extend(writer.finish());
        result
    }

    /// 旧逐 bit f32 Gorilla 编码参考实现。
    fn encode_f32_reference(data: &[f32]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }
        let mut writer = RefBitWriter::new(data.len() * 4);
        let count = data.len() as u32;
        let first_bits = data[0].to_bits();
        writer.write_bits(first_bits as u64, 32);
        let mut prev = first_bits;
        for i in 1..data.len() {
            let current = data[i].to_bits();
            let xor = prev ^ current;
            if xor == 0 {
                writer.write_bit(false);
            } else {
                writer.write_bit(true);
                let leading = xor.leading_zeros();
                let trailing = xor.trailing_zeros();
                let meaningful = 32 - leading - trailing;
                writer.write_bits(leading as u64, 5);
                writer.write_bits((meaningful - 1) as u64, 5);
                writer.write_bits((xor >> trailing) as u64, meaningful);
            }
            prev = current;
        }
        let mut result = Vec::with_capacity(4 + writer.buf.capacity());
        result.extend_from_slice(&count.to_le_bytes());
        result.extend(writer.finish());
        result
    }

    /// f64 逐字节回归：位缓冲编码 == 逐 bit 参考编码，覆盖边界 bit 模式。
    #[test]
    fn test_gorilla_f64_byte_identical_vs_reference() {
        let cases: Vec<Vec<f64>> = vec![
            vec![],
            vec![0.0],
            vec![-0.0],
            vec![0.0, 0.0, 0.0, 0.0],
            vec![-0.0, 0.0, -0.0, 0.0],
            vec![f64::NAN, f64::NAN, f64::NAN],
            vec![f64::INFINITY, f64::NEG_INFINITY, f64::INFINITY],
            vec![f64::MIN, f64::MAX, 0.0, f64::MIN],
            vec![f64::MIN_POSITIVE, f64::MAX, f64::MIN],
            // 符号位翻转：xor 的 bit 63 置位。
            vec![1.0, -1.0, 2.0, -2.0, 3.0, -3.0],
            // 连续相同值：leading/trailing 全零路径（xor==0）。
            vec![1.5f64; 100],
            // 极小差（相邻浮点）：leading 多 / trailing 多路径。
            vec![1.0, 1.0000000000000002, 1.0, 1.0000000000000004],
            // 触发 meaningful == 64：xor 的 bit 63 与 bit 0 同时置位（-0.0 ^ MIN_POSITIVE）。
            vec![-0.0, f64::MIN_POSITIVE, f64::MAX],
            vec![
                std::f64::consts::PI,
                std::f64::consts::E,
                0.0,
                1e300,
                -1e300,
                1e-300,
            ],
        ];

        for data in &cases {
            let new_bytes = GorillaEncoder::encode(data);
            let ref_bytes = encode_f64_reference(data);
            assert_eq!(
                new_bytes, ref_bytes,
                "f64 位缓冲编码与逐 bit 参考实现不一致: {:?}",
                data
            );
            // 附加 round-trip 校验（NaN 用 is_nan 单独判定）。
            let decoded = GorillaDecoder::decode(&new_bytes);
            assert_eq!(decoded.len(), data.len());
            for (a, b) in data.iter().zip(decoded.iter()) {
                if a.is_nan() {
                    assert!(b.is_nan(), "期望 NaN，实际 {}", b);
                } else {
                    assert_eq!(a.to_bits(), b.to_bits(), "round-trip bit 不一致");
                }
            }
        }
    }

    /// f32 逐字节回归：位缓冲编码 == 逐 bit 参考编码。
    #[test]
    fn test_gorilla_f32_byte_identical_vs_reference() {
        let cases: Vec<Vec<f32>> = vec![
            vec![],
            vec![0.0f32],
            vec![-0.0f32, 0.0f32, -0.0f32],
            vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY],
            vec![f32::MIN, f32::MAX, f32::MIN_POSITIVE],
            vec![1.0f32, -1.0f32, 2.0f32, -2.0f32],
            vec![1.5f32; 50],
            // 触发 meaningful == 32：xor 的 bit 31 与 bit 0 同时置位。
            vec![-0.0f32, f32::MIN_POSITIVE, f32::MAX],
            vec![1.0f32, 1.1f32, 1.15f32, 1.2f32, 1.18f32],
        ];
        for data in &cases {
            assert_eq!(
                GorillaF32Encoder::encode(data),
                encode_f32_reference(data),
                "f32 位缓冲编码与逐 bit 参考实现不一致: {:?}",
                data
            );
        }
    }

    /// 位缓冲（新）vs 逐 bit（旧参考）编码耗时对比（仅 release + --ignored 下运行）。
    #[test]
    #[ignore]
    fn bench_gorilla_encode_speedup() {
        let n = 500_000usize;
        // 模拟 TSBS 宽表 F64 列：单调小步进 + 周期扰动（产生非平凡 leading/trailing）。
        let data: Vec<f64> = (0..n)
            .map(|i| (i as f64) * 0.001 + ((i % 7) as f64) * 1e-9)
            .collect();

        // 预热
        let _ = GorillaEncoder::encode(&data[..2048]);
        let _ = encode_f64_reference(&data[..2048]);

        let t0 = std::time::Instant::now();
        let new_bytes = GorillaEncoder::encode(&data);
        let new_ms = t0.elapsed().as_secs_f64() * 1e3;

        let t1 = std::time::Instant::now();
        let old_bytes = encode_f64_reference(&data);
        let old_ms = t1.elapsed().as_secs_f64() * 1e3;

        assert_eq!(new_bytes, old_bytes, "bench 前必须逐字节一致");
        println!(
            "[gorilla bench] n={} new={:.3}ms old={:.3}ms speedup={:.2}x",
            n,
            new_ms,
            old_ms,
            old_ms / new_ms
        );
    }
}

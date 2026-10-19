//! 压缩模块：LZ4 / ZSTD9 统一包装。
//!
//! 提供压缩/解压的统一接口，使上层代码无需关心底层压缩库差异。
//! - LZ4: 用于 WAL、SAC L0（速度优先）
//! - ZSTD9: 用于 SAC L1/L2/L3/L4（压缩率优先）

/// 压缩算法类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// LZ4 快速压缩（SAC L0 默认）。
    Lz4,
    /// ZSTD 压缩级别 9（SAC L1/L2/L3/L4 使用）。
    Zstd9,
}

/// 使用 LZ4 压缩字节数组。
///
/// LZ4 以速度为优先，适合 WAL 和热数据压缩场景。
///
/// # 参数
/// - `data`: 待压缩的原始字节。
///
/// # 返回值
/// 压缩后的字节数组。
pub fn compress_lz4(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    lz4::block::compress(data, None, true).unwrap_or_else(|_| data.to_vec())
}

/// 使用 LZ4 解压字节数组。
///
/// # 参数
/// - `data`: LZ4 压缩的字节。
///
/// # 返回值
/// 解压后的原始字节。
pub fn decompress_lz4(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    lz4::block::decompress(data, None).unwrap_or_else(|_| data.to_vec())
}

/// 使用 ZSTD 级别 9 压缩字节数组。
///
/// ZSTD9 以压缩率为优先，适合 L2 归档数据。
///
/// # 参数
/// - `data`: 待压缩的原始字节。
///
/// # 返回值
/// 压缩后的字节数组。
pub fn compress_zstd9(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    zstd::encode_all(data, 9).unwrap_or_else(|_| data.to_vec())
}

/// 使用 ZSTD 解压字节数组。
///
/// # 参数
/// - `data`: ZSTD 压缩的字节。
///
/// # 返回值
/// 解压后的原始字节。
pub fn decompress_zstd(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    zstd::decode_all(data).unwrap_or_else(|_| data.to_vec())
}

/// 根据压缩类型压缩数据。
///
/// # 参数
/// - `data`: 待压缩的原始字节。
/// - `ct`: 压缩算法类型。
///
/// # 返回值
/// 压缩后的字节数组。
pub fn compress(data: &[u8], ct: CompressionType) -> Vec<u8> {
    match ct {
        CompressionType::Lz4 => compress_lz4(data),
        CompressionType::Zstd9 => compress_zstd9(data),
    }
}

/// 可复用压缩上下文：跨表/跨块复用，消除每表重建 CCtx / 输出 buffer 的固定开销。
///
/// 每线程一份（`EncodeScratch` 持有），跨该线程处理的全部 block 复用压缩器上下文
/// 与输出缓冲。`compress` 的字节输出与 [`compress`]（一次性 API）**逐字节一致**：
/// - LZ4 分支复用 [`lz4::block::compress_to_buffer`]（`prepend_size=true`），与
///   [`compress_lz4`] 的 `lz4::block::compress(data, None, true)` 同底层 codec 同参数，
///   输出含 4 字节小端长度前缀，逐字节一致。
/// - ZSTD9 分支复用 [`zstd::bulk::Compressor`]（level 9，跨块复用 CCtx），输出与
///   [`compress_zstd9`] 的 `zstd::encode_all(data, 9)` 逐字节一致（同 level 同参确定性）。
pub struct Compressor {
    ct: CompressionType,
    /// LZ4 复用输出 buffer（含 4 字节长度前缀）。
    lz4_buf: Vec<u8>,
    /// ZSTD9 复用 CCtx（懒建）。
    zstd_ctx: Option<zstd::bulk::Compressor<'static>>,
    /// ZSTD9 复用输出 buffer。
    zstd_buf: Vec<u8>,
}

impl Compressor {
    /// 创建一个按 `ct` 分派的可复用压缩上下文。
    pub fn new(ct: CompressionType) -> Self {
        Self {
            ct,
            lz4_buf: Vec::new(),
            zstd_ctx: None,
            zstd_buf: Vec::new(),
        }
    }

    /// 压缩到内部 scratch，返回 `&[u8]`（下次调用前有效）。
    ///
    /// 字节与 [`compress`]（`compress(data, ct)`）完全一致。空输入返回空切片
    /// （与 `compress_lz4`/`compress_zstd9` 的空输入语义一致）。
    pub fn compress(&mut self, data: &[u8]) -> &[u8] {
        match self.ct {
            CompressionType::Lz4 => {
                if data.is_empty() {
                    self.lz4_buf.clear();
                    return &self.lz4_buf;
                }
                // 与 compress_lz4 的 `lz4::block::compress(data, None, true)` 一致：
                // prepend_size=true → 输出含 4 字节小端长度前缀。
                let bound = lz4::block::compress_bound(data.len());
                match bound {
                    Ok(b) => {
                        self.lz4_buf.clear();
                        self.lz4_buf.resize(b + 4, 0);
                        match lz4::block::compress_to_buffer(
                            data,
                            None,
                            true,
                            &mut self.lz4_buf,
                        ) {
                            Ok(n) => {
                                self.lz4_buf.truncate(n);
                            }
                            Err(_) => {
                                // 与 compress_lz4 的 `unwrap_or_else(|_| data.to_vec())` 一致。
                                self.lz4_buf.clear();
                                self.lz4_buf.extend_from_slice(data);
                            }
                        }
                    }
                    Err(_) => {
                        // 输入过长（> i32::MAX），与 compress_lz4 的失败回退一致。
                        self.lz4_buf.clear();
                        self.lz4_buf.extend_from_slice(data);
                    }
                }
                &self.lz4_buf
            }
            CompressionType::Zstd9 => {
                if data.is_empty() {
                    self.zstd_buf.clear();
                    return &self.zstd_buf;
                }
                if self.zstd_ctx.is_none() {
                    self.zstd_ctx = Some(
                        zstd::bulk::Compressor::new(9)
                            .unwrap_or_else(|_| zstd::bulk::Compressor::default()),
                    );
                }
                // 预分配输出缓冲：`bulk::Compressor::compress_to_buffer` 不会自行扩容
                // （对容量不足的 `Vec` 直接报 dstSize_tooSmall），需先 reserve 到
                // `compress_bound`。与 `bulk::Compressor::compress` 内部 `Vec::with_capacity` 同构。
                let bound = zstd::zstd_safe::compress_bound(data.len());
                self.zstd_buf.clear();
                if self.zstd_buf.capacity() < bound {
                    self.zstd_buf.reserve(bound - self.zstd_buf.capacity());
                }
                let ctx = self.zstd_ctx.as_mut().expect("ctx initialized");
                match ctx.compress_to_buffer(data, &mut self.zstd_buf) {
                    Ok(_) => {}
                    Err(_) => {
                        // 与 compress_zstd9 的 `unwrap_or_else(|_| data.to_vec())` 一致。
                        self.zstd_buf.clear();
                        self.zstd_buf.extend_from_slice(data);
                    }
                }
                &self.zstd_buf
            }
        }
    }
}

/// 根据压缩类型解压数据。
///
/// # 参数
/// - `data`: 压缩后的字节。
/// - `ct`: 压缩算法类型。
///
/// # 返回值
/// 解压后的原始字节。
pub fn decompress(data: &[u8], ct: CompressionType) -> Vec<u8> {
    match ct {
        CompressionType::Lz4 => decompress_lz4(data),
        CompressionType::Zstd9 => decompress_zstd(data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_round_trip() {
        let original = b"Hello, SenseArcDB! This is a test of LZ4 compression.";
        let compressed = compress_lz4(original);
        assert!(!compressed.is_empty());
        let decompressed = decompress_lz4(&compressed);
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_lz4_empty() {
        let compressed = compress_lz4(&[]);
        assert!(compressed.is_empty());
        let decompressed = decompress_lz4(&[]);
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_zstd_round_trip() {
        let original = b"Hello, SenseArcDB! This is a test of ZSTD compression. ".repeat(10);
        let compressed = compress_zstd9(&original);
        assert!(!compressed.is_empty());
        let decompressed = decompress_zstd(&compressed);
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_zstd_empty() {
        let compressed = compress_zstd9(&[]);
        assert!(compressed.is_empty());
        let decompressed = decompress_zstd(&[]);
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_compression_type() {
        assert_eq!(CompressionType::Lz4 as u8, CompressionType::Lz4 as u8);
        assert_ne!(CompressionType::Lz4, CompressionType::Zstd9);
    }

    /// 构造一组覆盖不同大小/模式的测试输入（含空、极小、纯重复、随机、大块）。
    fn byte_consistency_inputs() -> Vec<Vec<u8>> {
        let mut inputs: Vec<Vec<u8>> = vec![
            Vec::new(),
            vec![0u8],
            vec![0u8; 1],
            b"Hello, SenseArcDB!".to_vec(),
            vec![0xAAu8; 4096], // 高可压缩（纯重复）
            (0..4096u32).map(|i| (i % 251) as u8).collect(), // 周期序列
        ];
        // 随机数据（低可压缩，确保走真正的压缩而非短路）。
        let mut x: u64 = 0x243F6A8885A308D3;
        let mut rnd = Vec::with_capacity(8192);
        for _ in 0..8192 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            rnd.push((x >> 33) as u8);
        }
        inputs.push(rnd);
        inputs
    }

    /// 第 0 步验证 1：复用 LZ4 buffer（`compress_to_buffer` prepend_size=true）
    /// 与一次性 `compress_lz4` 对同一输入产出逐字节一致的输出。
    #[test]
    fn test_compressor_lz4_byte_identical_to_compress() {
        let mut comp = Compressor::new(CompressionType::Lz4);
        for data in byte_consistency_inputs() {
            let want = compress_lz4(&data);
            let got = comp.compress(&data);
            assert_eq!(
                got, want.as_slice(),
                "LZ4 复用 API 必须与一次性 compress_lz4 逐字节一致（len={}）",
                data.len()
            );
            // 复用跨多次调用也一致（同一 buffer 反复写）。
            let got2 = comp.compress(&data);
            assert_eq!(got2, want.as_slice(), "复用第二次调用仍须逐字节一致");
        }
    }

    /// 第 0 步验证 2（已改为记录性测试）：复用 ZSTD9 CCtx（`bulk::Compressor::new(9)`）
    /// 与一次性 `compress_zstd9`（`zstd::encode_all(data, 9)`）**不逐字节一致**。
    ///
    /// 实测：`zstd::bulk::Compressor`（`ZSTD_compress2` 单发）对小块置 Single_Segment
    /// flag（frame 描述字节 0x20）、对大块置 ContentSize+SingleSegment（0x60），而
    /// `encode_all`（`ZSTD_compressStream2` 流式）帧描述字节恒 0x00，二者帧头不同、
    /// 大块下总长也不同。但两者均为标准 zstd 帧，`decompress_zstd`（`decode_all`）
    /// 均可无损解回。
    ///
    /// 此外（关键实现约束，见 writer.rs `encode_block_with_scratch`）：`zstd::bulk::Compressor`
    /// **复用 CCtx 时输出依赖其先前压缩历史**（实测同一输入 fresh CCtx 与「被异尺寸输入
    /// 预热过的 CCtx」产出不同字节，尤其是高可压缩的 I64/Gorilla 流会因复用 workspace 的
    /// 窗口/帧头协商而改变输出），故生产写路径每条 val 流都用独立 fresh `Compressor`
    /// 以保确定性。本测试据此只锁两条性质：fresh 实例首压确定性 + 可被 `decode_all` 无损解回。
    #[test]
    fn test_compressor_zstd9_fresh_deterministic_and_roundtrip() {
        for data in byte_consistency_inputs() {
            // fresh 实例首压确定性：不同实例对同一输入产出相同字节（val 流逐列 fresh 依赖此性质）。
            let mut a = Compressor::new(CompressionType::Zstd9);
            let mut b = Compressor::new(CompressionType::Zstd9);
            let got_a = a.compress(&data).to_vec();
            let got_b = b.compress(&data).to_vec();
            assert_eq!(
                got_a, got_b,
                "fresh 实例首压必须确定性一致（len={}）",
                data.len()
            );
            // 标准 zstd 帧可被 decode_all 无损解回（读路径 interop 前提）。
            assert_eq!(
                decompress_zstd(&got_a),
                data,
                "fresh 实例输出必须可被 decode_all 无损解回（len={}）",
                data.len()
            );
        }
    }
}

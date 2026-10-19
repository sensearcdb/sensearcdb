//! 时序数据编码模块。
//!
//! 为不同类型的数据列提供专用的时序编码器，以最小化 SAC Block 的存储空间：
//!
//! | 编码器 | 适用类型 | 算法 |
//! |--------|---------|------|
//! | [`DoDEncoder`](dod::DoDEncoder) | i64 时间戳 | Delta-of-Delta |
//! | [`GorillaEncoder`](gorilla::GorillaEncoder) | f64/f32 浮点值 | XOR-Gorilla |
//! | [`RleEncoder`](rle::RleEncoder) | u8 质量码 | Run-Length Encoding |
//! | [`Simple8bEncoder`](simple8b::Simple8bEncoder) | I64/U64/I32 | Simple8b |
//! | [`DictionaryEncoder`](dictionary::DictionaryEncoder) | String | Dictionary + LZ4 |

pub mod dictionary;
pub mod dod;
pub mod gorilla;
pub mod rle;
pub mod simple8b;

/// 编码器 trait：所有编码器必须实现 `encode` 方法。
pub trait Encoder<T> {
    /// 将数据编码为字节数组。
    fn encode(data: &[T]) -> Vec<u8>;
}

/// 解码器 trait：所有解码器必须实现 `decode` 方法。
pub trait Decoder<T> {
    /// 从字节数组解码数据。
    fn decode(bytes: &[u8]) -> Vec<T>;
}

// ─── SAC 值流列编码标签 ───
//
// 值流为自描述格式：`[col_count u16][col_id u16][tag u8][len u32][payload]...`。
// Reader 通过 tag 选择对应解码器，无需持有 Schema 即可按列投影解码。

/// Gorilla 编码标签（F64 浮点列）。
pub const VALUE_TAG_GORILLA: u8 = 0;
/// Simple8b 编码标签（I64 有符号整数列，zigzag）。
pub const VALUE_TAG_SIMPLE8B: u8 = 1;
/// Dictionary 编码标签（String 列）。
pub const VALUE_TAG_DICTIONARY: u8 = 2;
/// RLE 编码标签（Bool 列，0/1）。
pub const VALUE_TAG_BOOL_RLE: u8 = 3;
/// Gorilla f32 原生编码标签（F32 浮点列，32-bit XOR 位宽变体，O3=A）。
pub const VALUE_TAG_GORILLA_F32: u8 = 4;
/// Simple8b 编码标签（U64 无符号整数列，原始位宽，不 zigzag）。
pub const VALUE_TAG_SIMPLE8B_U64: u8 = 5;
/// Simple8b 编码标签（I32 有符号整数列，zigzag 后按位宽还原）。
pub const VALUE_TAG_SIMPLE8B_I32: u8 = 6;
/// Simple8b 编码标签（Enum16 枚举列，原始位宽，不 zigzag）。
pub const VALUE_TAG_SIMPLE8B_ENUM16: u8 = 7;

/// 值流 tag 高位标志：置位表示该列本块存在 null，payload 头部携带 presence 位图。
pub const VALUE_TAG_NULL_BITMAP: u8 = 0x80;
/// 去掉 null 标志后的基础编码 tag 掩码。
pub const VALUE_TAG_BASE_MASK: u8 = 0x7F;

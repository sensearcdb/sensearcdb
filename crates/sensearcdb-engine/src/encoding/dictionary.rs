//! Dictionary + LZ4 字符串编码器。
//!
//! 适用于 STRING 类型的列。编码流程：
//! 1. 构建字典：收集所有唯一字符串，分配 ID
//! 2. 编码为 `[dict_size: u16 LE][dict_entry_0_len: u16 LE][dict_entry_0_bytes...][...][indices: u16 LE...]`
//! 3. 整体 LZ4 压缩

use std::collections::HashMap;

use crate::compression::{compress_lz4, decompress_lz4};
use crate::encoding::{Decoder, Encoder};

/// Dictionary + LZ4 字符串编码器。
///
/// 先将字符串映射为字典索引，再对索引序列和字典本身做 LZ4 压缩。
pub struct DictionaryEncoder;

impl Encoder<String> for DictionaryEncoder {
    /// 对字符串序列进行 Dictionary + LZ4 编码。
    ///
    /// 编码格式：
    /// - `dict_size: u16 LE` (字典条目数)
    /// - 对每条字典条目: `len: u16 LE` + `bytes: [u8; len]`
    /// - `indices: [u16 LE; N]` (每个输入字符串对应的字典索引)
    /// - 整体 LZ4 压缩
    ///
    /// # 参数
    /// - `data`: 字符串序列。
    ///
    /// # 返回值
    /// LZ4 压缩后的字节数组。
    fn encode(data: &[String]) -> Vec<u8> {
        if data.is_empty() {
            // 空输入：返回空字典标记
            let raw = 0u16.to_le_bytes().to_vec();
            return compress_lz4(&raw);
        }

        // 构建字典
        let mut dict: Vec<&str> = Vec::new();
        let mut str_to_id: HashMap<&str, u16> = HashMap::new();

        for s in data {
            if !str_to_id.contains_key(s.as_str()) {
                let id = dict.len() as u16;
                dict.push(s.as_str());
                str_to_id.insert(s.as_str(), id);
            }
        }

        // 编码
        let mut raw = Vec::new();

        // dict_size
        raw.extend_from_slice(&(dict.len() as u16).to_le_bytes());

        // dict entries
        for entry in &dict {
            let entry_bytes = entry.as_bytes();
            raw.extend_from_slice(&(entry_bytes.len() as u16).to_le_bytes());
            raw.extend_from_slice(entry_bytes);
        }

        // indices
        for s in data {
            let id = str_to_id.get(s.as_str()).copied().unwrap_or(0);
            raw.extend_from_slice(&id.to_le_bytes());
        }

        // LZ4 压缩
        compress_lz4(&raw)
    }
}

/// Dictionary + LZ4 字符串解码器。
pub struct DictionaryDecoder;

impl Decoder<String> for DictionaryDecoder {
    /// 从 Dictionary+LZ4 编码的字节数组解码字符串序列。
    ///
    /// # 参数
    /// - `bytes`: Dictionary+LZ4 编码的字节数组。
    ///
    /// # 返回值
    /// 解码后的字符串序列。
    fn decode(bytes: &[u8]) -> Vec<String> {
        if bytes.is_empty() {
            return Vec::new();
        }

        // LZ4 解压
        let raw = decompress_lz4(bytes);
        if raw.is_empty() {
            return Vec::new();
        }

        let mut pos = 0usize;

        // 读取 dict_size
        if pos + 2 > raw.len() {
            return Vec::new();
        }
        let dict_size =
            u16::from_le_bytes(raw[pos..pos + 2].try_into().unwrap_or([0u8; 2])) as usize;
        pos += 2;

        if dict_size == 0 {
            return Vec::new();
        }

        // 读取 dict entries
        let mut dict: Vec<String> = Vec::with_capacity(dict_size);
        for _ in 0..dict_size {
            if pos + 2 > raw.len() {
                break;
            }
            let entry_len =
                u16::from_le_bytes(raw[pos..pos + 2].try_into().unwrap_or([0u8; 2])) as usize;
            pos += 2;

            if pos + entry_len > raw.len() {
                break;
            }
            let entry_bytes = &raw[pos..pos + entry_len];
            let s = String::from_utf8_lossy(entry_bytes).to_string();
            dict.push(s);
            pos += entry_len;
        }

        // 读取 indices
        let mut result = Vec::new();
        while pos + 2 <= raw.len() {
            let idx = u16::from_le_bytes(raw[pos..pos + 2].try_into().unwrap_or([0u8; 2])) as usize;
            pos += 2;
            if idx < dict.len() {
                result.push(dict[idx].clone());
            } else {
                result.push(String::new());
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_round_trip_empty() {
        let data: Vec<String> = vec![];
        let encoded = DictionaryEncoder::encode(&data);
        let decoded = DictionaryDecoder::decode(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_dictionary_round_trip_single() {
        let data = vec!["hello".to_string()];
        let encoded = DictionaryEncoder::encode(&data);
        let decoded = DictionaryDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_dictionary_round_trip_repeated() {
        let data = vec![
            "open".to_string(),
            "close".to_string(),
            "open".to_string(),
            "open".to_string(),
            "close".to_string(),
        ];
        let encoded = DictionaryEncoder::encode(&data);
        let decoded = DictionaryDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_dictionary_round_trip_many_unique() {
        let data: Vec<String> = (0..100).map(|i| format!("tag_{}", i)).collect();
        let encoded = DictionaryEncoder::encode(&data);
        let decoded = DictionaryDecoder::decode(&encoded);
        assert_eq!(decoded, data);
    }
}

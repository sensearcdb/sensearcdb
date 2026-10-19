//! SAC 文件命名与目录结构（SAC v9 多表聚合）。
//!
//! SAC 文件路径格式：`{sac_dir}/{tenant_id}/{partition_ns}_{level}_{seq}.sac`
//!
//! # 目录结构示例
//!
//! ```text
//! sac/
//! └── 0/                                     # tenant_id（0 = 默认租户）
//!     ├── 1692180000000000000_0_0001.sac   # partition_ns=..., L0, seq=1
//!     ├── 1692180000000000000_0_0002.sac
//!     └── 1692180000000000000_2_0001.sac   # L2 final
//! ```
//!
//! v9 去掉 `table_id` 与 `partition_ns` 两层目录，只保留 tenant 一层。文件名
//! `{partition_ns}_{level}_{seq}` 中 `partition_ns` 用 19 位十进制左补 0（保证
//! 字典序 == 时间序），`seq` 用至少 4 位（`{:04}`）。

use std::path::{Path, PathBuf};

use sensearcdb_common::SenseArcError;

use crate::sac::header::SacLevel;

/// SAC 文件命名工具。
///
/// 提供 SAC 文件路径的构造和解析方法。
pub struct SacNaming {
    /// SAC 文件根目录。
    sac_dir: PathBuf,
}

impl SacNaming {
    /// 创建新的 SAC 命名实例。
    ///
    /// # 参数
    /// - `sac_dir`: SAC 文件根目录路径。
    pub fn new(sac_dir: impl Into<PathBuf>) -> Self {
        Self {
            sac_dir: sac_dir.into(),
        }
    }

    /// 构造 SAC 文件完整路径。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID（0 = 默认租户）。
    /// - `partition_ns`: 分区键（数据时间边界，epoch 纳秒）。
    /// - `level`: SAC 层级。
    /// - `seq`: 序列号。
    ///
    /// # 返回值
    /// 格式为 `{sac_dir}/{tenant_id}/{partition_ns:019}_{level}_{seq:04}.sac` 的路径。
    pub fn file_path(
        &self,
        tenant_id: u32,
        partition_ns: i64,
        level: SacLevel,
        seq: u16,
    ) -> PathBuf {
        // 使用 String 拼接确保跨平台统一使用 '/' 分隔符。
        let path_str = format!(
            "{}/{}/{:019}_{}_{:04}.sac",
            self.sac_dir.display(),
            tenant_id,
            partition_ns,
            level as u8,
            seq
        );
        PathBuf::from(path_str)
    }

    /// 构造 SAC 文件所在的租户目录：`{sac_dir}/{tenant_id}/`。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID。
    pub fn tenant_dir(&self, tenant_id: u32) -> PathBuf {
        PathBuf::from(format!("{}/{}", self.sac_dir.display(), tenant_id))
    }

    /// 枚举指定租户目录下的全部 SAC 段文件（全 partition、全 level）。
    ///
    /// 返回**完整路径**（相对 sac_dir 可由调用方 `strip_prefix(sac_dir)` 获得）。
    /// 目录不存在时返回空列表（幂等）。结果按路径字典序排序，保证确定性。
    ///
    /// # 参数
    /// - `tenant_id`: 租户 ID。
    pub fn list_tenant_segments(&self, tenant_id: u32) -> Result<Vec<PathBuf>, SenseArcError> {
        let tenant_dir = self.tenant_dir(tenant_id);
        if !tenant_dir.exists() {
            return Ok(Vec::new());
        }

        let mut segments: Vec<PathBuf> = Vec::new();
        for file_entry in std::fs::read_dir(&tenant_dir).map_err(|e| {
            SenseArcError::SacIoError(format!("read tenant dir {}: {}", tenant_dir.display(), e))
        })? {
            let file_path = file_entry
                .map_err(|e| SenseArcError::SacIoError(format!("read tenant entry: {}", e)))?
                .path();
            if file_path.extension().and_then(|e| e.to_str()) != Some("sac") {
                continue;
            }
            segments.push(file_path);
        }

        segments.sort();
        Ok(segments)
    }

    /// 构造 SAC 临时文件路径（写入中）。
    ///
    /// 格式: `{file_path}.tmp`。
    /// 写入完成后通过 POSIX `rename` 原子替换为正式文件。
    pub fn tmp_path(
        &self,
        tenant_id: u32,
        partition_ns: i64,
        level: SacLevel,
        seq: u16,
    ) -> PathBuf {
        let mut p = self.file_path(tenant_id, partition_ns, level, seq);
        p.set_extension("sac.tmp");
        p
    }

    /// 解析 SAC 文件名，提取 partition_ns、level 和 seq。
    ///
    /// # 参数
    /// - `file_name`: 文件名（不含目录路径），如 `"1692180000000000000_0_0001.sac"`。
    ///
    /// # 返回值
    /// `(partition_ns, level, seq)`；若解析失败返回 `None`。
    pub fn parse_file_name(file_name: &str) -> Option<(i64, SacLevel, u16)> {
        let stem = file_name.strip_suffix(".sac")?;
        let mut parts = stem.split('_');
        let partition_ns: i64 = parts.next()?.parse().ok()?;
        let level_raw: u8 = parts.next()?.parse().ok()?;
        let seq: u16 = parts.next()?.parse().ok()?;
        let level = SacLevel::from_u8(level_raw)?;
        // 四段（partition_ns_level_seq），不允许有多余段。
        if parts.next().is_some() {
            return None;
        }
        Some((partition_ns, level, seq))
    }

    /// 计算指定 (tenant, partition_ns, level) 的下一个可用 seq（现有最大 seq + 1）。
    ///
    /// 扫盘取该 (tenant, partition_ns, level) 内既有文件的最大 seq + 1（设计 §1.4），
    /// 跨重启续号，不覆盖存量文件。seq 按 (tenant, partition_ns, level) 独立编号。
    pub fn next_seq(&self, tenant_id: u32, partition_ns: i64, level: SacLevel) -> u16 {
        Self::next_seq_in_dir(&self.tenant_dir(tenant_id), partition_ns, level)
    }

    /// 在指定目录下计算 (partition_ns, level) 的最大 seq + 1（共享扫盘实现）。
    ///
    /// 供 flush（L0 落盘）与 compaction（L1/L2 落盘）共用，避免两处各自实现导致
    /// seq 续号口径不一致。19 位补零保证 `{partition_ns}_` 前缀匹配的字典序 ==
    /// 数值序，再按 level 精确过滤，不误计同分区其他 level 的 seq。
    pub fn next_seq_in_dir(tenant_dir: &Path, partition_ns: i64, level: SacLevel) -> u16 {
        let mut max_seq = 0u16;
        let prefix = format!("{:019}_", partition_ns);
        if let Ok(entries) = std::fs::read_dir(tenant_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if !name.starts_with(&prefix) {
                        continue;
                    }
                    if let Some((pn, lvl, seq)) = SacNaming::parse_file_name(name) {
                        if pn == partition_ns && lvl == level {
                            max_seq = max_seq.max(seq);
                        }
                    }
                }
            }
        }
        max_seq.wrapping_add(1)
    }

    /// 获取 SAC 文件根目录。
    pub fn sac_dir(&self) -> &Path {
        &self.sac_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_path() {
        let naming = SacNaming::new("/data/sac");
        let path = naming.file_path(0, 1_692_180_000_000_000_000, SacLevel::L0, 1);
        assert_eq!(
            path.to_str().unwrap(),
            "/data/sac/0/1692180000000000000_0_0001.sac"
        );
    }

    #[test]
    fn test_file_path_zero_pads_partition_ns() {
        let naming = SacNaming::new("/data/sac");
        let path = naming.file_path(0, 42, SacLevel::L1, 5);
        assert_eq!(
            path.to_str().unwrap(),
            "/data/sac/0/0000000000000000042_1_0005.sac"
        );
    }

    #[test]
    fn test_file_path_seq_exceeds_4_digits() {
        let naming = SacNaming::new("/data/sac");
        let path = naming.file_path(0, 0, SacLevel::L0, 65535);
        assert!(path.to_str().unwrap().ends_with("_0_65535.sac"));
    }

    #[test]
    fn test_tmp_path() {
        let naming = SacNaming::new("/data/sac");
        let path = naming.tmp_path(0, 100, SacLevel::L1, 5);
        assert!(path.to_str().unwrap().ends_with(".sac.tmp"));
    }

    #[test]
    fn test_parse_file_name() {
        let result = SacNaming::parse_file_name("1692180000000000000_0_0001.sac").unwrap();
        assert_eq!(result, (1_692_180_000_000_000_000, SacLevel::L0, 1));

        let result = SacNaming::parse_file_name("0000000000000000042_2_0042.sac").unwrap();
        assert_eq!(result, (42, SacLevel::L2, 42));
    }

    #[test]
    fn test_parse_file_name_invalid() {
        assert!(SacNaming::parse_file_name("bad.sac").is_none());
        assert!(SacNaming::parse_file_name("3_01.sac").is_none()); // invalid level
        assert!(SacNaming::parse_file_name("0_abc.sac").is_none());
        assert!(SacNaming::parse_file_name("0_0_0001_extra.sac").is_none());
    }

    #[test]
    fn test_tenant_dir() {
        let naming = SacNaming::new("/data/sac");
        assert_eq!(naming.tenant_dir(0).to_str().unwrap(), "/data/sac/0");
    }

    #[test]
    fn test_list_tenant_segments_empty_when_dir_missing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let naming = SacNaming::new(tmp.path().join("sac"));
        let segments = naming.list_tenant_segments(0).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn test_list_tenant_segments_enumerates_all_files() {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let sac_dir = tmp.path().join("sac");
        let naming = SacNaming::new(&sac_dir);

        let tenant_dir = naming.tenant_dir(0);
        fs::create_dir_all(&tenant_dir).unwrap();
        fs::write(tenant_dir.join("100_0_0001.sac"), b"a").unwrap();
        fs::write(tenant_dir.join("200_1_0001.sac"), b"b").unwrap();
        fs::write(tenant_dir.join("300_0_0001.sac.tmp"), b"c").unwrap(); // 不应被枚举

        let segments = naming.list_tenant_segments(0).unwrap();
        let mut rel: Vec<String> = segments
            .iter()
            .map(|p| {
                p.strip_prefix(&sac_dir)
                    .unwrap()
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .collect();
        rel.sort();

        assert_eq!(
            rel,
            vec![
                "0/100_0_0001.sac".to_string(),
                "0/200_1_0001.sac".to_string(),
            ]
        );
    }
}

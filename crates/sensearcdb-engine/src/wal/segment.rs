//! WAL 段管理：64MB 段文件 + flush_watermark。
//!
//! WAL 由一系列 64MB 的段文件组成，每个段文件名为 `{seq:08}.wal`。
//! 段序号单调递增，新段在旧段写满 64MB 后自动创建。
//!
//! `flush_watermark` 记录已安全 fsync 的最大 WAL 偏移，
//! 崩溃恢复时从 watermark 之后开始重放。
//!
//! 全模式统一写 WAL2 帧（37B header），无 V1 帧写入路径。

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use sensearcdb_common::{constants::WAL_SEGMENT_SIZE, SenseArcError};

use crate::wal::format::WalFrameHeaderV2;

/// WAL 段文件管理器。
///
/// 每个段文件最大 64MB。当段写满时，创建新段继续写入。
pub struct WalSegment {
    /// 段文件序号（单调递增）。
    seq: u64,
    /// 段文件完整路径。
    path: PathBuf,
    /// 段文件的 BufWriter。
    writer: BufWriter<File>,
    /// 当前段已写入的字节数。
    written: u64,
    /// flush_watermark: 已安全 fsync 的最大字节偏移。
    watermark: u64,
}

impl WalSegment {
    /// 创建新的 WAL 段文件。
    ///
    /// # 参数
    /// - `wal_dir`: WAL 文件目录。
    /// - `seq`: 段序号。
    ///
    /// # 返回值
    /// 新创建的 `WalSegment`。
    pub fn create_new(wal_dir: &Path, seq: u64) -> Result<Self, SenseArcError> {
        fs::create_dir_all(wal_dir)
            .map_err(|e| SenseArcError::WalIoError(format!("mkdir: {}", e)))?;

        let file_name = format!("{:08}.wal", seq);
        let path = wal_dir.join(&file_name);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| SenseArcError::WalIoError(format!("open segment: {}", e)))?;

        let metadata = file
            .metadata()
            .map_err(|e| SenseArcError::WalIoError(format!("metadata: {}", e)))?;

        let file_len = metadata.len();

        Ok(Self {
            seq,
            path,
            writer: BufWriter::new(file),
            written: file_len,
            watermark: file_len,
        })
    }

    /// 写入一个 WAL2 帧（write + flush，**不 fsync**）。
    ///
    /// 供三档持久化共用：`append_frame_v2` 只把帧写进内核 page cache（`flush`），
    /// 不调用 `sync_all`；真正的落盘由后续 [`fsync`](Self::fsync) 显式触发
    /// （分级持久化的硬前置：写帧与 fsync 解耦，`group` 攒批后一次 fsync）。
    ///
    /// # 参数
    /// - `header`: WAL2 帧头（37B）。
    /// - `payload`: 帧 payload（已压缩/加密）。
    ///
    /// # 返回值
    /// 写入后段文件的总字节数。
    pub fn append_frame_v2(
        &mut self,
        header: &WalFrameHeaderV2,
        payload: &[u8],
    ) -> Result<u64, SenseArcError> {
        let header_bytes = header.to_bytes();
        self.writer
            .write_all(&header_bytes)
            .map_err(|e| SenseArcError::WalIoError(format!("write header: {}", e)))?;
        self.writer
            .write_all(payload)
            .map_err(|e| SenseArcError::WalIoError(format!("write payload: {}", e)))?;
        self.writer
            .flush()
            .map_err(|e| SenseArcError::WalIoError(format!("flush: {}", e)))?;

        let frame_size = (header_bytes.len() + payload.len()) as u64;
        self.written += frame_size;

        Ok(self.written)
    }

    /// 对当前段文件执行一次 `fsync`（`sync_all`），并把 `watermark` 推进到 `written`。
    ///
    /// 与 [`append_frame_v2`](Self::append_frame_v2) 解耦：分级持久化中，
    /// `group`/`durable` 攒批多帧后只调一次 `fsync`；`async` 帧不调 `fsync`
    /// （直到被后续任意一次 group/durable fsync 顺带落盘）。
    pub fn fsync(&mut self) -> Result<(), SenseArcError> {
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| SenseArcError::WalIoError(format!("fsync: {}", e)))?;
        self.watermark = self.written;
        Ok(())
    }

    /// 写入空 flush marker（用于低负载时推进 watermark）。
    ///
    /// 当 idle 超过一定时间时，即使没有数据也写入一个空标记，
    /// 防止下游（SnapshotStore）饥饿。
    pub fn write_flush_marker(&mut self) -> Result<(), SenseArcError> {
        // 空标记：写入 magic + 0 长度帧
        let marker = b"SARC_FLUSH_MARKER____";
        self.writer
            .write_all(marker)
            .map_err(|e| SenseArcError::WalIoError(format!("write marker: {}", e)))?;
        self.writer
            .flush()
            .map_err(|e| SenseArcError::WalIoError(format!("flush marker: {}", e)))?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| SenseArcError::WalIoError(format!("fsync marker: {}", e)))?;

        self.written += marker.len() as u64;
        self.watermark = self.written;

        Ok(())
    }

    /// 获取已写入的字节数。
    pub fn bytes_written(&self) -> u64 {
        self.written
    }

    /// 获取 flush_watermark。
    pub fn flush_watermark(&self) -> u64 {
        self.watermark
    }

    /// 获取段序号。
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// 获取段文件路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 检查段是否已写满（≥ 64MB）。
    pub fn is_full(&self) -> bool {
        self.written >= WAL_SEGMENT_SIZE
    }

    /// 获取剩余可写字节数。
    pub fn remaining(&self) -> u64 {
        WAL_SEGMENT_SIZE.saturating_sub(self.written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::format::WalFrameFlag;
    use tempfile::TempDir;

    #[test]
    fn test_create_segment() {
        let tmp = TempDir::new().unwrap();
        let seg = WalSegment::create_new(tmp.path(), 0).unwrap();
        assert_eq!(seg.seq(), 0);
        assert_eq!(seg.bytes_written(), 0);
        assert!(!seg.is_full());
    }

    #[test]
    fn test_append_frame_v2_and_fsync() {
        let tmp = TempDir::new().unwrap();
        let mut seg = WalSegment::create_new(tmp.path(), 1).unwrap();

        let header = WalFrameHeaderV2::new(WalFrameFlag::Data, 1, 3, 42, 10, 1, 0);
        let payload = vec![0u8; 10];

        // append 只写页缓存，不推进 watermark。
        let written = seg.append_frame_v2(&header, &payload).unwrap();
        assert_eq!(written, 37 + 10);
        assert_eq!(seg.bytes_written(), 47);
        assert_eq!(
            seg.flush_watermark(),
            0,
            "append must not advance watermark"
        );

        // fsync 后才推进 watermark。
        seg.fsync().unwrap();
        assert_eq!(seg.flush_watermark(), 47);
    }

    #[test]
    fn test_flush_marker() {
        let tmp = TempDir::new().unwrap();
        let mut seg = WalSegment::create_new(tmp.path(), 0).unwrap();

        seg.write_flush_marker().unwrap();
        assert!(seg.bytes_written() > 0);
    }

    #[test]
    fn test_segment_size_limit() {
        // 64MB 段大小常量
        assert_eq!(WAL_SEGMENT_SIZE, 64 * 1024 * 1024);
    }
}

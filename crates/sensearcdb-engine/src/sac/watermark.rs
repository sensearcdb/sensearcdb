//! SAC 落盘水位 `S`（C2）。
//!
//! 恢复层 2（SAC 全量扫描 + WAL 增量）此前硬编码 `read_after(0)` 全量回放
//! （`recovery.rs:321`），根因是**没有任何「SAC 已落盘的最大数据 seq」水位**。
//! 本模块补齐这个水位：MemTable freeze → flush SAC 完成后，先把「已 flush 的
//! 最大数据 seq」原子写盘，回收器（C3）才能据此判断「哪些 WAL 段的数据已经
//! 不需要 WAL 就能恢复」。
//!
//! ## 不变式 I2（先落盘后推进）
//!
//! 调用方（`FlushCoordinator`）保证：SAC 文件 `fsync` 完成后才调用
//! [`advance_to`](Self::advance_to)；本模块先落盘水位文件、再推进内存值。
//! 崩溃在任意点：要么 WAL 段还在（冗余无害），要么 S 已推进且数据已在 SAC
//! （不丢）。
//!
//! ## 不变式 I5（durable 零丢失不被回收破坏）
//!
//! 水位 `S` 单调不减，且只覆盖「已 `fsync` 进 SAC」的数据 seq；回收器只删
//! `last_data_seq ≤ R = min(snapshot.bin wal_watermark, S)` 的段，不触碰
//! `committed_watermark` 及未落盘帧。

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use sensearcdb_common::SenseArcError;

/// SAC 落盘水位文件名（放在 SAC 根目录下）。
pub const SAC_WATERMARK_FILE: &str = "sac_watermark.bin";

/// 水位文件 magic: "SACW"。
const WATERMARK_MAGIC: &[u8; 4] = b"SACW";

/// 水位文件格式版本号。
const WATERMARK_VERSION: u32 = 1;

/// 水位文件字节数：magic(4) + version(4) + value(8)。
const WATERMARK_SIZE: usize = 16;

/// SAC 落盘水位：记录「已 flush 到 SAC 的最大数据 seq」。
pub struct SacWatermark {
    /// 水位文件路径。
    path: PathBuf,
    /// 内存当前值（单调不减）。
    current: AtomicU64,
    /// 串行化「检查 → 落盘 → 推进」三步，避免并发推进交错写 `.tmp`。
    write_lock: Mutex<()>,
}

impl SacWatermark {
    /// 从 `sac_dir` 加载现有水位并构造（文件缺失/损坏 → 0）。
    pub fn new(sac_dir: &Path) -> Self {
        let path = sac_dir.join(SAC_WATERMARK_FILE);
        let current = Self::load_value(&path);
        Self {
            path,
            current: AtomicU64::new(current),
            write_lock: Mutex::new(()),
        }
    }

    /// 只读加载 `sac_dir` 下已持久化的水位（文件缺失/损坏 → 0）。
    ///
    /// 供恢复层/集成层在启动时读取上一轮运行的水位。
    pub fn load(sac_dir: &Path) -> u64 {
        Self::load_value(&sac_dir.join(SAC_WATERMARK_FILE))
    }

    fn load_value(path: &Path) -> u64 {
        match fs::read(path) {
            Ok(data) if data.len() >= WATERMARK_SIZE && data[0..4] == *WATERMARK_MAGIC => {
                let version = u32::from_le_bytes(data[4..8].try_into().unwrap_or([0u8; 4]));
                if version != WATERMARK_VERSION {
                    tracing::warn!(
                        path = %path.display(),
                        version,
                        "SAC watermark version mismatch; treating as 0"
                    );
                    return 0;
                }
                u64::from_le_bytes(data[8..16].try_into().unwrap_or([0u8; 8]))
            }
            _ => 0,
        }
    }

    /// 当前水位（数据 seq）。
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::SeqCst)
    }

    /// 单调推进水位到 `seq`（仅当 `seq > 当前值`），并原子落盘。
    ///
    /// ## I2 顺序
    ///
    /// 先写盘、再推进内存值：落盘失败时内存值不变，回收器保持保守，
    /// 绝不可能出现「内存已推进、磁盘未推进」导致的删段激进。
    pub fn advance_to(&self, seq: u64) -> Result<(), SenseArcError> {
        let _guard = self.write_lock.lock();
        let prev = self.current.load(Ordering::SeqCst);
        if seq <= prev {
            return Ok(()); // 已推进到更高或相等，无需重复落盘。
        }
        self.persist(seq)?; // 1. 先落盘（fsync + rename）
        self.current.store(seq, Ordering::SeqCst); // 2. 再推进内存值
        Ok(())
    }

    /// 原子写水位文件：`.tmp` → `fsync` → `rename` → 父目录 `fsync`。
    fn persist(&self, value: u64) -> Result<(), SenseArcError> {
        let tmp_path = self
            .path
            .with_file_name(format!("{SAC_WATERMARK_FILE}.tmp"));

        let mut buf = Vec::with_capacity(WATERMARK_SIZE);
        buf.extend_from_slice(WATERMARK_MAGIC);
        buf.extend_from_slice(&WATERMARK_VERSION.to_le_bytes());
        buf.extend_from_slice(&value.to_le_bytes());

        let write_result: Result<(), SenseArcError> = (|| {
            let mut file = fs::File::create(&tmp_path)
                .map_err(|e| SenseArcError::SacIoError(format!("create watermark tmp: {e}")))?;
            file.write_all(&buf)
                .map_err(|e| SenseArcError::SacIoError(format!("write watermark: {e}")))?;
            file.sync_all()
                .map_err(|e| SenseArcError::SacIoError(format!("fsync watermark: {e}")))?;
            drop(file);

            fs::rename(&tmp_path, &self.path)
                .map_err(|e| SenseArcError::SacIoError(format!("rename watermark: {e}")))?;
            if let Some(parent) = self.path.parent() {
                if let Ok(dir) = fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
            Ok(())
        })();

        if write_result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        write_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_new_missing_returns_zero() {
        let tmp = TempDir::new().unwrap();
        let wm = SacWatermark::new(tmp.path());
        assert_eq!(wm.current(), 0);
    }

    #[test]
    fn test_advance_monotonic_and_persisted() {
        let tmp = TempDir::new().unwrap();
        let wm = SacWatermark::new(tmp.path());

        wm.advance_to(100).unwrap();
        assert_eq!(wm.current(), 100);

        // 不回退：推进到更小值应被忽略。
        wm.advance_to(50).unwrap();
        assert_eq!(wm.current(), 100);

        // 持久化后可被 `load` 读回。
        assert_eq!(SacWatermark::load(tmp.path()), 100);

        // 重新构造（模拟重启）也能读回。
        let wm2 = SacWatermark::new(tmp.path());
        assert_eq!(wm2.current(), 100);
    }

    #[test]
    fn test_advance_no_tmp_residue() {
        let tmp = TempDir::new().unwrap();
        let wm = SacWatermark::new(tmp.path());
        wm.advance_to(7).unwrap();
        assert!(tmp.path().join(SAC_WATERMARK_FILE).exists());
        assert!(!tmp
            .path()
            .join(format!("{SAC_WATERMARK_FILE}.tmp"))
            .exists());
    }

    #[test]
    fn test_corrupt_returns_zero() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join(SAC_WATERMARK_FILE), b"junk").unwrap();
        assert_eq!(SacWatermark::load(tmp.path()), 0);
    }

    /// I2 突变测试护栏：`advance_to` 内部必须先落盘、后推进内存值。
    ///
    /// 当 `persist` 失败（如水位文件父目录不存在）时，内存值**不得**推进，
    /// 否则「内存已推进、磁盘未推进」会让回收器变得激进（误删 WAL 段）。
    /// 若实现被改成「先 `store` 再 `persist`」，本测试必须 FAIL。
    #[test]
    fn test_advance_failed_persist_does_not_advance_memory() {
        let tmp = TempDir::new().unwrap();
        // 指向不存在的子目录 → persist 的 File::create 必然失败。
        let bad_dir = tmp.path().join("nonexistent");
        let wm = SacWatermark::new(&bad_dir);
        assert_eq!(wm.current(), 0);

        let result = wm.advance_to(100);
        assert!(result.is_err(), "persist 失败必须向上报错");
        assert_eq!(
            wm.current(),
            0,
            "I2：落盘失败时内存水位不得推进（先落盘、后推进）"
        );
    }
}

//! WAL 旧段回收器（C3）。
//!
//! 「SAC 落盘水位驱动的段级整删 + 固定保留 N 段余量 + 原子两步删除」。
//!
//! 回收水位 `R = min(snapshot.bin wal_watermark, SAC 落盘水位 S)` 由调用方
//! （集成层后台任务）计算后传入 [`reap`](WalReaper::reap)。本模块只负责：
//!
//! 1. 读 manifest（C1）拿到每段 `last_data_seq`；
//! 2. 只整段删「`last_data_seq ≤ R` 且非活跃段」的 `{seq}.wal`；
//! 3. 保留最近 `keep_segments` 段余量（默认 2）；
//! 4. 两步删除：`rename → {seq}.wal.del` → `fsync(wal_dir)` → `unlink`。
//!
//! ## 不变式
//!
//! - **I1**：只删整段覆盖，绝不删活跃段、绝不删半段；
//! - **I3**：删除只依据「数据 seq ≤ R」，任何恢复层 `read_after(X)` 的 `X ≥ R`；
//! - **I4**：删段判断用数据 seq（manifest `last_data_seq`），不用段文件名 seq；
//! - 两步删除（rename 成 `.del` 后缀）保证 `read_dir` 只看 `.wal`，半删段不会被误读。

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use sensearcdb_common::SenseArcError;

use crate::wal::manifest::{SegmentMeta, WalManifest};

/// 保留最近 N 段余量（安全缓冲，参考 InfluxDB3 `snapshotted-wal-files-to-keep`；
/// 本地盘场景 2 段足够，默认取 2）。
pub const DEFAULT_WAL_RECYCLE_KEEP_SEGMENTS: u64 = 2;

/// 一次回收的结果统计。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReapStats {
    /// 本轮判定为「可删候选」的段数。
    pub candidate_count: usize,
    /// 实际删除的段数。
    pub deleted: usize,
    /// 删除后 manifest 仍保留的段数。
    pub remaining: usize,
}

/// WAL 旧段回收器。
#[derive(Debug, Clone)]
pub struct WalReaper {
    /// WAL 文件目录。
    wal_dir: PathBuf,
    /// 保留最近 N 段余量（≥1）。
    keep_segments: u64,
}

impl WalReaper {
    /// 创建回收器。
    ///
    /// `keep_segments` 被钳制到至少 1（至少保留 1 段余量，绝不删光）。
    pub fn new(wal_dir: impl Into<PathBuf>, keep_segments: u64) -> Self {
        Self {
            wal_dir: wal_dir.into(),
            keep_segments: keep_segments.max(1),
        }
    }

    /// 执行一次回收。
    ///
    /// # 参数
    /// - `r`: 回收水位（数据 seq）。只删 `last_data_seq ≤ r` 的段。
    /// - `active_segment_seq`: 当前活跃段文件名 seq（额外护栏，绝不删活跃段）。
    pub fn reap(&self, r: u64, active_segment_seq: u64) -> Result<ReapStats, SenseArcError> {
        let mut manifest = WalManifest::load(&self.wal_dir)?;
        let mut stats = ReapStats::default();

        // 1. 候选：finalized 段 + last_data_seq ≤ r + 非活跃段（I1）。
        let mut candidates: Vec<SegmentMeta> = manifest
            .entries
            .iter()
            .filter(|e| e.segment_seq < active_segment_seq && e.last_data_seq <= r)
            .copied()
            .collect();
        // 防御性排序：保证按段 seq 升序取「最老在前」。
        candidates.sort_by_key(|e| e.segment_seq);
        stats.candidate_count = candidates.len();

        // 2. 保留最近 N 段余量：跳过末尾 N 个（最新 N 个可删候选），只删更老的。
        let keep = self.keep_segments as usize;
        let delete_count = candidates.len().saturating_sub(keep);
        let to_delete: Vec<SegmentMeta> = candidates.into_iter().take(delete_count).collect();

        // 3. 两步删除：rename → .del → fsync(dir) → unlink → fsync(dir)。
        let mut deleted_seqs: HashSet<u64> = HashSet::with_capacity(to_delete.len());
        let mut deleted = 0usize;
        for meta in &to_delete {
            if self.delete_segment_two_step(meta.segment_seq)? {
                deleted += 1;
                deleted_seqs.insert(meta.segment_seq);
            }
        }
        stats.deleted = deleted;

        // 4. 更新 manifest（移除已删段）并原子持久化。
        if deleted > 0 {
            manifest
                .entries
                .retain(|e| !deleted_seqs.contains(&e.segment_seq));
            manifest.write_atomic(&self.wal_dir)?;
        }
        stats.remaining = manifest.entries.len();

        if deleted > 0 {
            tracing::info!(
                r = r,
                active_segment = active_segment_seq,
                candidates = stats.candidate_count,
                deleted = deleted,
                "WAL reaper deleted old segments"
            );
        }
        Ok(stats)
    }

    /// 两步删除单个段：`rename → .del` → `fsync(wal_dir)` → `unlink`。
    ///
    /// `read_dir` 只认 `.wal` 后缀（`reader.rs:129` / `writer.rs:368`），
    /// rename 成 `.del` 后即使崩溃残留也天然被忽略，杜绝半删段进入 replay。
    ///
    /// 返回 `Ok(true)` = 已删除；`Ok(false)` = 文件本就不存在（幂等跳过）。
    fn delete_segment_two_step(&self, segment_seq: u64) -> Result<bool, SenseArcError> {
        let file_name = format!("{segment_seq:08}.wal");
        let src = self.wal_dir.join(&file_name);
        let del = self.wal_dir.join(format!("{file_name}.del"));

        if !src.exists() {
            // 上次删除中途崩溃可能已 rename/unlink；幂等跳过。
            return Ok(false);
        }

        // I1/I3：先 rename 成非 `.wal` 后缀，避免半删段被 read_dir 误读。
        fs::rename(&src, &del)
            .map_err(|e| SenseArcError::WalIoError(format!("rename segment to .del: {e}")))?;
        fsync_dir(&self.wal_dir)?;

        fs::remove_file(&del)
            .map_err(|e| SenseArcError::WalIoError(format!("unlink .del segment: {e}")))?;
        fsync_dir(&self.wal_dir)?;

        Ok(true)
    }
}

/// 对目录做一次 `sync_all`（best-effort；Linux rename/unlink 持久化要求）。
fn fsync_dir(dir: &Path) -> Result<(), SenseArcError> {
    if let Ok(f) = fs::File::open(dir) {
        let _ = f.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn meta(seq: u64, first: u64, last: u64) -> SegmentMeta {
        SegmentMeta {
            segment_seq: seq,
            first_data_seq: first,
            last_data_seq: last,
        }
    }

    /// 构造 wal_dir + 若干 `.wal` 段文件 + manifest。
    fn setup(entries: &[(u64, u64, u64)]) -> (TempDir, PathBuf) {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let mut manifest = WalManifest::default();
        for (seq, first, last) in entries {
            fs::write(wal_dir.join(format!("{seq:08}.wal")), b"placeholder").unwrap();
            manifest.push(meta(*seq, *first, *last));
        }
        manifest.write_atomic(&wal_dir).unwrap();
        (tmp, wal_dir)
    }

    #[test]
    fn test_reap_only_deletes_eligible_non_active() {
        let (_tmp, wal_dir) = setup(&[(0, 1, 50), (1, 51, 100), (2, 101, 150)]);
        let reaper = WalReaper::new(&wal_dir, 1);

        // R=100：段 0（last=50）与段 1（last=100）可删，段 2（last=150）不可删。
        // keep=1 → 保留最近 1 个候选（段 1），只删最老的段 0。
        let stats = reaper.reap(100, 3).unwrap();
        assert_eq!(stats.candidate_count, 2);
        assert_eq!(stats.deleted, 1);

        assert!(!wal_dir.join("00000000.wal").exists(), "段 0 应被删除");
        assert!(
            wal_dir.join("00000001.wal").exists(),
            "段 1 应保留（N 段余量）"
        );
        assert!(
            wal_dir.join("00000002.wal").exists(),
            "段 2（last>R）应保留"
        );

        // manifest 已移除段 0。
        let m = WalManifest::load(&wal_dir).unwrap();
        assert_eq!(m.entries.len(), 2);
        assert!(m.entries.iter().all(|e| e.segment_seq != 0));
    }

    #[test]
    fn test_reap_never_deletes_active_segment() {
        let (_tmp, wal_dir) = setup(&[(0, 1, 50), (1, 51, 100), (2, 101, 150)]);
        let reaper = WalReaper::new(&wal_dir, 1);

        // 即使 R 覆盖所有段，活跃段 2 也绝不删（I1）。
        let stats = reaper.reap(150, 2).unwrap();
        assert!(wal_dir.join("00000002.wal").exists(), "活跃段绝不删");
        assert_ne!(stats.remaining, 0, "回收后 manifest 应至少保留活跃段/余量");
    }

    #[test]
    fn test_reap_keep_n_segments() {
        let (_tmp, wal_dir) = setup(&[
            (0, 1, 10),
            (1, 11, 20),
            (2, 21, 30),
            (3, 31, 40),
            (4, 41, 50),
        ]);
        // keep=2：可删候选 5 个，只删最老的 3 个（0/1/2），保留最近 2 个（3/4）。
        let reaper = WalReaper::new(&wal_dir, 2);
        let stats = reaper.reap(50, 99).unwrap();
        assert_eq!(stats.deleted, 3);
        assert!(wal_dir.join("00000003.wal").exists());
        assert!(wal_dir.join("00000004.wal").exists());
        assert!(!wal_dir.join("00000000.wal").exists());
    }

    #[test]
    fn test_reap_two_step_no_half_deleted_segment() {
        // 两个候选 + keep=1 → 删最老的段 0，保留段 1。
        let (_tmp, wal_dir) = setup(&[(0, 1, 10), (1, 11, 20)]);
        let reaper = WalReaper::new(&wal_dir, 1);
        let stats = reaper.reap(20, 99).unwrap();
        assert_eq!(stats.deleted, 1);

        // 两步删除后：既无 `.wal` 也无 `.del` 残留。
        assert!(!wal_dir.join("00000000.wal").exists());
        assert!(!wal_dir.join("00000000.wal.del").exists());
        assert!(
            wal_dir.join("00000001.wal").exists(),
            "段 1 应保留（N 段余量）"
        );
        // 目录里不再有任何 `.del`。
        let leftovers: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("del"))
            .collect();
        assert!(leftovers.is_empty(), "删除后不得残留 .del 半删段");
    }

    #[test]
    fn test_reap_r_zero_deletes_nothing() {
        let (_tmp, wal_dir) = setup(&[(0, 1, 10)]);
        let reaper = WalReaper::new(&wal_dir, 1);
        let stats = reaper.reap(0, 99).unwrap();
        assert_eq!(stats.deleted, 0);
        assert!(wal_dir.join("00000000.wal").exists());
    }

    /// I1 边界：回收水位 R 是闭区间（`last_data_seq ≤ R` 即可删），且活跃段
    /// 即使其 `last_data_seq == R` 也绝不删除。
    ///
    /// 构造「活跃段恰好落在 R 边界」：段 2 为活跃段且 last == R，回收器必须
    /// 跳过它（`segment_seq < active` 护栏 + manifest 本就不含活跃段的双保险）。
    #[test]
    fn test_reap_r_boundary_inclusive_and_active_excluded() {
        let (_tmp, wal_dir) = setup(&[
            (0, 1, 100),   // last == R → 可删（闭区间）
            (1, 101, 100), // last == R → 候选，但作为 N 余量保留
            (2, 101, 100), // 活跃段：last == R，但绝不删
        ]);
        let reaper = WalReaper::new(&wal_dir, 1);
        let stats = reaper.reap(100, 2).unwrap();

        // 只有 seg0/seg1 进候选；活跃段 seg2 被 `segment_seq < active` 排除。
        assert_eq!(stats.candidate_count, 2, "活跃段不得进入候选");
        assert_eq!(stats.deleted, 1, "keep=1：删最老 seg0，保留 seg1 余量");

        assert!(
            !wal_dir.join("00000000.wal").exists(),
            "last==R 且非活跃 → 删"
        );
        assert!(wal_dir.join("00000001.wal").exists(), "N 段余量保留");
        assert!(wal_dir.join("00000002.wal").exists(), "活跃段绝不删");
    }

    /// I1 边界：`last_data_seq > R` 的段绝不进入候选（严格按数据 seq 判界）。
    #[test]
    fn test_reap_last_data_seq_above_r_excluded() {
        let (_tmp, wal_dir) = setup(&[
            (0, 1, 100),   // last == R → 候选
            (1, 101, 101), // last == R+1 → 排除
        ]);
        let reaper = WalReaper::new(&wal_dir, 1);
        let stats = reaper.reap(100, 99).unwrap();

        assert_eq!(stats.candidate_count, 1, "last > R 的段不得进入候选");
        assert!(wal_dir.join("00000001.wal").exists(), "last=R+1 段必须保留");
    }
}

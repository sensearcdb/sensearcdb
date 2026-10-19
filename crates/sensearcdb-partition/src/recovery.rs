//! Disk-based partition recovery at startup.
//!
//! On startup, the partition manager scans the SAC data directory to
//! discover existing partitions. Disk is the **ground truth** — if the
//! metadata in `config.db` disagrees with what's on disk, the disk
//! state wins and metadata is overwritten.
//!
//! # SAC Directory Structure (v9 扁平多表聚合布局)
//!
//! ```text
//! sac/
//! └── {tenant_id}/                       # tenant_id（0 = 默认租户）
//!     ├── {partition_ns:019}_{level}_{seq:04}.sac
//!     └── ...（一个文件聚合多张表，table_id 记录在文件 TableIndex 内）
//! ```
//!
//! # State Inference Rules
//!
//! 对每张表、每个 `partition_ns`（表集合从文件 TableIndex 反查）：
//!
//! | 该表所在文件层级               | Inferred State |
//! |--------------------------------|----------------|
//! | 仅 L2                          | Archived       |
//! | 有 L1（无论有无 L2）           | Frozen         |
//! | 仅 L0（无 L1/L2）              | Active         |
//!
//! 某张表 `partition_ns` 最大的分区若被推断为 `Frozen`，强制改回 `Active`
//! （处理「最新分区有 L1 无 L0」的窗口场景）。

use crate::metadata::PartitionMetadataStore;
use crate::state::{PartitionInfo, PartitionState};
use dashmap::DashMap;
use sensearcdb_common::TenantId;
use sensearcdb_engine::sac::{SacNaming, SacReader};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;

/// Metadata extracted from a SAC file.
#[derive(Debug, Clone)]
struct SacFileMeta {
    /// The compaction level: 0, 1, or 2.
    level: u8,
    /// File size in bytes.
    size: u64,
    /// 该文件内包含的表 ID（升序，来自 TableIndex）。
    ///
    /// v9 多表聚合后 table_id 不再出现在路径，恢复时只能从文件 TableIndex 反查。
    table_ids: Vec<u32>,
}

/// Discover partitions by scanning the SAC directory on disk.
///
/// This is the startup recovery function. It:
/// 1. Scans `sac_dir/{tenant_id}/` for flat SAC files
///    `{partition_ns:019}_{level}_{seq:04}.sac`（S1: 默认租户 0）。
/// 2. Reads each file's TableIndex to recover the set of `table_id`s.
/// 3. Infers each `(table_id, partition_ns)` state from the levels of the files
///    containing that table.
/// 4. Populates the in-memory `partitions` map.
/// 5. Persists (or updates) metadata in `config.db`.
///
/// # Arguments
///
/// * `partitions` - The in-memory partition map to populate.
/// * `metadata` - The metadata store to persist to.
/// * `sac_dir` - The root SAC data directory.
/// * `duration_ms` - Partition window duration, used for time-range fields.
pub async fn discover_partitions_from_disk(
    partitions: &DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>>,
    metadata: &PartitionMetadataStore,
    sac_dir: &Path,
    duration_ms: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 覆盖「目录扫描 + latest_per_table + 收集 + 批量落库」完整恢复过程，供端到端验收计时。
    let started = std::time::Instant::now();

    if !sac_dir.exists() {
        tracing::warn!(
            sac_dir = %sac_dir.display(),
            "discover: SAC directory does not exist, starting with empty partition map"
        );
        return Ok(());
    }

    // v9 扁平布局：`{sac_dir}/{tenant_id}/{partition_ns:019}_{level}_{seq:04}.sac`。
    // 一个文件聚合多表，table_id 从文件 TableIndex 读取（路径不再携带 table_id）。
    // key: (tenant_id, partition_ns) → 该分区下的全部 SAC 文件元数据。
    let mut by_partition: BTreeMap<(TenantId, i64), Vec<SacFileMeta>> = BTreeMap::new();

    // S5：遍历 sac_dir 下全部 tenant 目录（不再硬编码默认租户 0）。
    let mut tenant_entries = fs::read_dir(sac_dir).await?;
    let mut tenant_dirs: Vec<(TenantId, PathBuf)> = Vec::new();
    while let Some(tenant_entry) = tenant_entries.next_entry().await? {
        let tenant_path = tenant_entry.path();
        if !tenant_path.is_dir() {
            continue;
        }
        let tenant_name = tenant_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if let Ok(tenant_id) = tenant_name.parse::<u32>() {
            tenant_dirs.push((TenantId(tenant_id), tenant_path));
        }
    }
    tenant_dirs.sort_by_key(|(t, _)| t.as_u32());

    for (tenant_id, tenant_dir) in tenant_dirs {
        let mut entries = fs::read_dir(&tenant_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("sac") {
                continue;
            }

            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            // 解析 `{partition_ns:019}_{level}_{seq:04}.sac` → (partition_ns, level, seq)。
            let Some((partition_ns, level, _seq)) = SacNaming::parse_file_name(file_name) else {
                tracing::debug!(
                    file = %path.display(),
                    "discover: skipping unrecognized SAC filename"
                );
                continue;
            };

            let size = fs::metadata(&path).await.map(|m| m.len()).unwrap_or(0);
            let table_ids = match SacReader::list_table_ids(&path) {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::warn!(
                        file = %path.display(),
                        error = %e,
                        "discover: failed to read table ids from SAC file, skipping"
                    );
                    continue;
                }
            };
            if table_ids.is_empty() {
                tracing::warn!(
                    file = %path.display(),
                    "discover: SAC file has empty TableIndex, skipping"
                );
                continue;
            }

            by_partition
                .entry((tenant_id, partition_ns))
                .or_default()
                .push(SacFileMeta {
                    level: level as u8,
                    size,
                    table_ids,
                });
        }
    }

    // 确定每张表的最大 partition_ns（「最新」分区）。
    let mut latest_per_table: BTreeMap<(TenantId, u32), i64> = BTreeMap::new();
    for ((tenant_id, partition_ns), files) in &by_partition {
        for file in files {
            for table_id in &file.table_ids {
                let key = (*tenant_id, *table_id);
                let cur = latest_per_table.entry(key).or_insert(*partition_ns);
                if *partition_ns > *cur {
                    *cur = *partition_ns;
                }
            }
        }
    }

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    // 收集阶段：内存 map 立即填充（磁盘为准的缓存），DB 写推迟到批量阶段（#3）。
    // 每 5000 条一个事务，避免逐条 execute 的隐式提交 + fsync 开销。
    let mut total_partitions: u64 = 0;
    let mut batch: Vec<PartitionInfo> = Vec::with_capacity(5000);

    for ((tenant_id, partition_ns), files) in &by_partition {
        // 倒排索引：table_id -> 该表在本分区内出现的文件下标（升序，天然按文件遍历顺序）。
        // 一次扫描全部文件的 table_ids，把 O(F·T²) 降到 O(F·T)（#2）。
        let mut table_to_files: HashMap<u32, Vec<usize>> = HashMap::new();
        for (file_idx, file) in files.iter().enumerate() {
            for &tid in &file.table_ids {
                table_to_files.entry(tid).or_default().push(file_idx);
            }
        }

        // 该分区涉及的全部表（升序、去重），由倒排索引的 key 派生。
        let mut all_tables: Vec<u32> = table_to_files.keys().copied().collect();
        all_tables.sort_unstable();

        for table_id in all_tables {
            // 每张表的 state 仅由「包含该表的文件」决定（多表聚合下各表层级可能不同）。
            // O(1) 取该表文件下标，再按 idx 取 SacFileMeta 引用；无需 .cloned() 深拷贝。
            let table_files: Vec<&SacFileMeta> = table_to_files[&table_id]
                .iter()
                .map(|&idx| &files[idx])
                .collect();

            let is_latest = latest_per_table
                .get(&(*tenant_id, table_id))
                .map_or(false, |latest| *latest == *partition_ns);

            let state = infer_state(&table_files, is_latest);
            let (min_ts, max_ts) = compute_time_range(&table_files, *partition_ns, duration_ms);
            let total_bytes: u64 = table_files.iter().map(|f| f.size).sum();
            let file_count = table_files.len() as u32;

            let info = PartitionInfo {
                table_id,
                tenant_id: *tenant_id,
                partition_ns: *partition_ns,
                state,
                file_count,
                total_bytes,
                min_ts,
                max_ts,
                created_at: *partition_ns, // Best estimate
                frozen_at: if state != PartitionState::Active {
                    Some(now_ns)
                } else {
                    None
                },
                compacting_at: None,
                archived_at: if state == PartitionState::Archived {
                    Some(now_ns)
                } else {
                    None
                },
                freeze_count: 0,
            };

            // #4：逐表 info 降级为 debug，避免 50 万条 info 的格式化/输出成本。
            tracing::debug!(
                tenant_id = tenant_id.as_u32(),
                table_id = table_id,
                partition_ns = *partition_ns,
                state = %info.state.as_str(),
                file_count = file_count,
                total_bytes = total_bytes,
                is_latest = is_latest,
                "discover: recovered partition from disk"
            );

            // 内存 map 填充仍在收集循环内完成。
            let arc_info = Arc::new(info);
            partitions
                .entry(arc_info.table_id)
                .or_insert_with(BTreeMap::new)
                .insert(arc_info.partition_ns, Arc::clone(&arc_info));

            // 只有 DB 写推迟到批量阶段。
            batch.push((*arc_info).clone());
            total_partitions += 1;
            if batch.len() >= 5000 {
                metadata.insert_partitions_batch(&batch)?;
                batch.clear();
            }
        }
    }

    // flush 剩余不足一批的分区。
    if !batch.is_empty() {
        metadata.insert_partitions_batch(&batch)?;
    }

    let total_tables = latest_per_table.len() as u64;
    let elapsed = started.elapsed();
    tracing::info!(
        total_partitions = total_partitions,
        total_tables = total_tables,
        elapsed_ms = elapsed.as_millis() as u64,
        "discover: partition recovery complete"
    );

    Ok(())
}

/// Infer the partition state from the SAC files present.
///
/// # Rules
///
/// - If L2 exists and no L0 or L1 exists → `Archived`
/// - If L1 exists (regardless of L2) → `Frozen`
/// - If only L0 exists (no L1, no L2) → `Active`
/// - If the partition is the latest (largest partition_ns) for its table
///   and would otherwise be `Frozen`, force it to `Active`.
fn infer_state(files: &[&SacFileMeta], is_latest: bool) -> PartitionState {
    let has_l0 = files.iter().any(|f| f.level == 0);
    let has_l1 = files.iter().any(|f| f.level == 1);
    let has_l2 = files.iter().any(|f| f.level == 2);

    if has_l2 && !has_l0 && !has_l1 {
        // Pure L2: compacted and archived
        PartitionState::Archived
    } else if has_l1 {
        // Has L1 (with or without L2): Frozen, awaiting final compaction
        if is_latest && !has_l0 {
            // Latest partition with L1 but no L0 — force Active (new writes go here)
            PartitionState::Active
        } else {
            PartitionState::Frozen
        }
    } else if has_l0 {
        // Only L0, no L1/L2: still accepting writes
        PartitionState::Active
    } else {
        // No recognizable SAC files (shouldn't reach here due to early filter)
        if is_latest {
            PartitionState::Active
        } else {
            PartitionState::Frozen
        }
    }
}

/// Estimate the time range covered by a partition from its SAC files.
///
/// This is a best-effort estimate. When SAC files are present, we set:
/// - `min_ts` = `partition_ns` (start of window)
/// - `max_ts` = `partition_ns + duration_ns` (end of window)
///
/// A more accurate range could be read from SAC file headers in production.
fn compute_time_range(
    _files: &[&SacFileMeta],
    partition_ns: i64,
    duration_ms: u64,
) -> (Option<i64>, Option<i64>) {
    let duration_ns = (duration_ms as i64) * 1_000_000;
    (Some(partition_ns), Some(partition_ns + duration_ns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sensearcdb_engine::compression::CompressionType;
    use sensearcdb_engine::memtable::chunk::ColumnarChunk;
    use sensearcdb_engine::memtable::col_vec::ColVec;
    use sensearcdb_engine::sac::header::SacLevel;
    use sensearcdb_engine::sac::writer::SacWriter;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use tokio::fs;

    /// 构造一个最小但合法的单点单列 `ColumnarChunk`（供测试写真实 SAC 文件）。
    fn make_test_chunk(table_id: u32, tenant_id: TenantId) -> ColumnarChunk {
        let ts = 1_000_000_000i64; // 任意时间戳；恢复只读 TableIndex，不校验时间范围。
        let mut ts_index = BTreeMap::new();
        ts_index.insert(ts, 0usize);
        let mut columns = HashMap::new();
        columns.insert(0u16, ColVec::F64(vec![42.0]));
        ColumnarChunk {
            table_id,
            tenant_id,
            timestamps: vec![ts],
            columns,
            presence: HashMap::new(),
            qualities: vec![0xC0],
            ts_index,
            point_count: 1,
            out_of_order: false,
            max_wal_seq: None,
            min_wal_seq: None,
        }
    }

    /// 在 v9 扁平布局下为指定表写一个真实 SAC 文件。
    ///
    /// `files` = `(level, seq)` 列表；路径为
    /// `{base}/{tenant_id}/{partition_ns:019}_{level}_{seq:04}.sac`。
    async fn create_sac_structure_for_tenant(
        base: &Path,
        tenant_id: TenantId,
        table_id: u32,
        partition_ns: i64,
        files: &[(u8, u16)],
    ) {
        let writer = SacWriter::new(base.to_path_buf(), CompressionType::Lz4);
        for (level_raw, seq) in files {
            let level = SacLevel::from_u8(*level_raw).expect("valid sac level");
            let chunk = make_test_chunk(table_id, tenant_id);
            writer
                .write_chunk(&chunk, partition_ns, level, *seq, None)
                .expect("write test sac file");
        }
    }

    async fn create_sac_structure(
        base: &Path,
        table_id: u32,
        partition_ns: i64,
        files: &[(u8, u16)],
    ) {
        create_sac_structure_for_tenant(base, TenantId::DEFAULT, table_id, partition_ns, files)
            .await;
    }

    #[test]
    fn test_infer_state_archived() {
        let files = vec![SacFileMeta {
            level: 2,
            size: 1024,
            table_ids: vec![],
        }];
        let refs: Vec<&SacFileMeta> = files.iter().collect();
        assert_eq!(infer_state(&refs, false), PartitionState::Archived);
    }

    #[test]
    fn test_infer_state_frozen() {
        let files = vec![
            SacFileMeta {
                level: 1,
                size: 1024,
                table_ids: vec![],
            },
            SacFileMeta {
                level: 2,
                size: 2048,
                table_ids: vec![],
            },
        ];
        let refs: Vec<&SacFileMeta> = files.iter().collect();
        assert_eq!(infer_state(&refs, false), PartitionState::Frozen);
    }

    #[test]
    fn test_infer_state_active_l0_only() {
        let files = vec![SacFileMeta {
            level: 0,
            size: 512,
            table_ids: vec![],
        }];
        let refs: Vec<&SacFileMeta> = files.iter().collect();
        assert_eq!(infer_state(&refs, false), PartitionState::Active);
    }

    #[test]
    fn test_infer_state_latest_frozen_to_active() {
        // Latest partition with L1 but no L0: should be Active, not Frozen
        let files = vec![SacFileMeta {
            level: 1,
            size: 1024,
            table_ids: vec![],
        }];
        let refs: Vec<&SacFileMeta> = files.iter().collect();
        assert_eq!(infer_state(&refs, true), PartitionState::Active);
    }

    #[tokio::test]
    async fn test_discover_empty_directory() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");
        fs::create_dir_all(&sac_dir).await.unwrap();

        let db_path = dir.path().join("config.db");
        let metadata = PartitionMetadataStore::open(&db_path).unwrap();
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();

        discover_partitions_from_disk(&partitions, &metadata, &sac_dir, 86_400_000)
            .await
            .unwrap();

        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_discover_with_sac_files() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");

        // 扁平布局：{tenant}/1703980800000000000_0_0001.sac 等。
        // table 1, partition N: L0（Active）。
        create_sac_structure(&sac_dir, 1, 1_703_980_800_000_000_000, &[(0, 1)]).await;
        // table 1, partition N-1: L1 + L2（Frozen, older）。
        create_sac_structure(&sac_dir, 1, 1_703_894_400_000_000_000, &[(1, 1), (2, 1)]).await;
        // table 2, partition N-1: L2 only（Archived；seq 错开避免与 table 1 的 L2 撞名）。
        create_sac_structure(&sac_dir, 2, 1_703_894_400_000_000_000, &[(2, 2)]).await;

        let db_path = dir.path().join("config.db");
        let metadata = PartitionMetadataStore::open(&db_path).unwrap();
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();

        discover_partitions_from_disk(&partitions, &metadata, &sac_dir, 86_400_000)
            .await
            .unwrap();

        // Table 1 should have 2 partitions
        let t1 = partitions.get(&1).unwrap();
        assert_eq!(t1.len(), 2);

        // The latest partition should be Active
        let latest_ns = 1_703_980_800_000_000_000;
        let latest = t1.get(&latest_ns).unwrap();
        assert_eq!(latest.state, PartitionState::Active);

        // The older partition should be Frozen
        let older_ns = 1_703_894_400_000_000_000;
        let older = t1.get(&older_ns).unwrap();
        assert_eq!(older.state, PartitionState::Frozen);

        // Table 2 should have 1 Archived partition
        let t2 = partitions.get(&2).unwrap();
        assert_eq!(t2.len(), 1);
        let archived = t2.get(&1_703_894_400_000_000_000).unwrap();
        assert_eq!(archived.state, PartitionState::Archived);
    }

    /// 写一个「多表聚合」SAC 文件：同一 (partition_ns, level, seq) 下聚合多个表。
    ///
    /// 用于覆盖 #2 倒排索引的「单分区多文件多表交叉」等价性场景。
    fn create_multi_table_sac_file(
        base: &Path,
        tenant_id: TenantId,
        table_ids: &[u32],
        partition_ns: i64,
        level: u8,
        seq: u16,
    ) {
        let writer = SacWriter::new(base.to_path_buf(), CompressionType::Lz4);
        let level = SacLevel::from_u8(level).expect("valid sac level");
        let mut builder = writer
            .begin_write(tenant_id.as_u32(), partition_ns, level, seq, 0)
            .expect("begin write sac file");
        for &tid in table_ids {
            let chunk = make_test_chunk(tid, tenant_id);
            builder.append_chunk(&chunk, None).expect("append chunk");
        }
        builder.finish().expect("finish sac file");
    }

    /// #2 倒排索引等价性：单分区内 3 个 SAC 文件交叉含表，逐表 `file_count` / `state` /
    /// `total_bytes` 必须与旧「逐表 filter」逻辑严格一致。
    ///
    /// 布局（同 partition_ns、同 L0、seq 错开）：
    /// - 文件 A（seq 1）：含表 {1, 2}
    /// - 文件 B（seq 2）：含表 {2, 3}
    /// - 文件 C（seq 3）：含表 {1, 3}
    /// 期望：表1 ∈ {A,C}、表2 ∈ {A,B}、表3 ∈ {B,C}，各 file_count=2、state=Active（仅 L0）。
    #[tokio::test]
    async fn test_discover_inverted_index_equivalence_cross_tables() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");

        let ns = 1_703_980_800_000_000_000i64;
        create_multi_table_sac_file(&sac_dir, TenantId::DEFAULT, &[1, 2], ns, 0, 1);
        create_multi_table_sac_file(&sac_dir, TenantId::DEFAULT, &[2, 3], ns, 0, 2);
        create_multi_table_sac_file(&sac_dir, TenantId::DEFAULT, &[1, 3], ns, 0, 3);

        // 读 3 个文件的真实大小，用于逐表 total_bytes 严格对照。
        let tenant_dir = sac_dir.join("0");
        let size_a = std::fs::metadata(tenant_dir.join(format!("{:019}_0_{:04}.sac", ns, 1)))
            .unwrap()
            .len();
        let size_b = std::fs::metadata(tenant_dir.join(format!("{:019}_0_{:04}.sac", ns, 2)))
            .unwrap()
            .len();
        let size_c = std::fs::metadata(tenant_dir.join(format!("{:019}_0_{:04}.sac", ns, 3)))
            .unwrap()
            .len();

        let db_path = dir.path().join("config.db");
        let metadata = PartitionMetadataStore::open(&db_path).unwrap();
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();

        discover_partitions_from_disk(&partitions, &metadata, &sac_dir, 86_400_000)
            .await
            .unwrap();

        // 表1 含于 A、C。
        let t1 = partitions.get(&1).unwrap();
        assert_eq!(t1.len(), 1);
        let p1 = t1.get(&ns).unwrap();
        assert_eq!(p1.file_count, 2);
        assert_eq!(p1.state, PartitionState::Active);
        assert_eq!(p1.total_bytes, size_a + size_c);

        // 表2 含于 A、B。
        let t2 = partitions.get(&2).unwrap();
        assert_eq!(t2.len(), 1);
        let p2 = t2.get(&ns).unwrap();
        assert_eq!(p2.file_count, 2);
        assert_eq!(p2.state, PartitionState::Active);
        assert_eq!(p2.total_bytes, size_a + size_b);

        // 表3 含于 B、C。
        let t3 = partitions.get(&3).unwrap();
        assert_eq!(t3.len(), 1);
        let p3 = t3.get(&ns).unwrap();
        assert_eq!(p3.file_count, 2);
        assert_eq!(p3.state, PartitionState::Active);
        assert_eq!(p3.total_bytes, size_b + size_c);
    }

    /// S5：多租户目录恢复——各租户 PartitionInfo.tenant_id 正确且不混租户。
    #[tokio::test]
    async fn test_discover_multi_tenant_partitions() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");

        // 租户 1：table 10，partition N（Active）；租户 2：table 20，partition N-1（Archived）。
        create_sac_structure_for_tenant(
            &sac_dir,
            TenantId(1),
            10,
            1_703_980_800_000_000_000,
            &[(0, 1)],
        )
        .await;
        create_sac_structure_for_tenant(
            &sac_dir,
            TenantId(2),
            20,
            1_703_894_400_000_000_000,
            &[(2, 1)],
        )
        .await;

        let db_path = dir.path().join("config.db");
        let metadata = PartitionMetadataStore::open(&db_path).unwrap();
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();

        discover_partitions_from_disk(&partitions, &metadata, &sac_dir, 86_400_000)
            .await
            .unwrap();

        let t10 = partitions.get(&10).unwrap();
        assert_eq!(t10.len(), 1);
        assert_eq!(
            t10.get(&1_703_980_800_000_000_000).unwrap().tenant_id,
            TenantId(1)
        );

        let t20 = partitions.get(&20).unwrap();
        assert_eq!(t20.len(), 1);
        assert_eq!(
            t20.get(&1_703_894_400_000_000_000).unwrap().tenant_id,
            TenantId(2)
        );
        assert_eq!(
            t20.get(&1_703_894_400_000_000_000).unwrap().state,
            PartitionState::Archived
        );
    }

    #[tokio::test]
    async fn test_discover_missing_directory_no_panic() {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("nonexistent_sac");

        let db_path = dir.path().join("config.db");
        let metadata = PartitionMetadataStore::open(&db_path).unwrap();
        let partitions: DashMap<u32, BTreeMap<i64, Arc<PartitionInfo>>> = DashMap::new();

        let result =
            discover_partitions_from_disk(&partitions, &metadata, &sac_dir, 86_400_000).await;
        assert!(result.is_ok());
        assert!(partitions.is_empty());
    }
}

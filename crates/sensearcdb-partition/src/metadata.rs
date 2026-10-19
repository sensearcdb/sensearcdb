//! SQLite-backed partition metadata persistence.
//!
//! The `PartitionMetadataStore` manages the `partitions` table in
//! `config.db`. This table is the authoritative source for partition
//! state metadata. Disk recovery (`recovery.rs`) may override these
//! records during startup — disk is the ground truth for existence,
//! but metadata carries the state transitions.

use crate::state::{PartitionInfo, PartitionState};
use parking_lot::Mutex;
use rusqlite::{params, Connection, Result as SqlResult};
use std::path::Path;

/// SQL statements for the `partitions` table.
const CREATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS partitions (
    table_id      INTEGER NOT NULL,
    partition_ns  INTEGER NOT NULL,
    state         TEXT    NOT NULL DEFAULT 'Active',
    file_count    INTEGER NOT NULL DEFAULT 0,
    total_bytes   INTEGER NOT NULL DEFAULT 0,
    min_ts        INTEGER,
    max_ts        INTEGER,
    created_at    INTEGER NOT NULL,
    frozen_at     INTEGER,
    compacting_at INTEGER,
    archived_at   INTEGER,
    freeze_count  INTEGER NOT NULL DEFAULT 0,
    tenant_id     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (table_id, partition_ns)
);
";

const CREATE_INDEX: &str = "
CREATE INDEX IF NOT EXISTS idx_partitions_state
ON partitions(table_id, state);
";

const INSERT_PARTITION: &str = "
INSERT OR REPLACE INTO partitions
(table_id, partition_ns, state, file_count, total_bytes, min_ts, max_ts,
 created_at, frozen_at, compacting_at, archived_at, freeze_count, tenant_id)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13);
";

const UPDATE_STATE: &str = "
UPDATE partitions SET state = ?3 WHERE table_id = ?1 AND partition_ns = ?2;
";

const UPDATE_STATE_AND_FROZEN_AT: &str = "
UPDATE partitions SET state = ?3, frozen_at = ?4 WHERE table_id = ?1 AND partition_ns = ?2;
";

const UPDATE_STATE_AND_COMPACTING_AT: &str = "
UPDATE partitions SET state = ?3, compacting_at = ?4 WHERE table_id = ?1 AND partition_ns = ?2;
";

const UPDATE_STATE_AND_ARCHIVED_AT: &str = "
UPDATE partitions SET state = ?3, archived_at = ?4 WHERE table_id = ?1 AND partition_ns = ?2;
";

const SELECT_BY_TABLE: &str = "
SELECT table_id, partition_ns, state, file_count, total_bytes, min_ts, max_ts,
       created_at, frozen_at, compacting_at, archived_at, freeze_count, tenant_id
FROM partitions WHERE table_id = ?1 ORDER BY partition_ns;
";

const SELECT_ONE: &str = "
SELECT table_id, partition_ns, state, file_count, total_bytes, min_ts, max_ts,
       created_at, frozen_at, compacting_at, archived_at, freeze_count, tenant_id
FROM partitions WHERE table_id = ?1 AND partition_ns = ?2;
";

const SELECT_ALL_ACTIVE: &str = "
SELECT table_id, partition_ns, state, file_count, total_bytes, min_ts, max_ts,
       created_at, frozen_at, compacting_at, archived_at, freeze_count, tenant_id
FROM partitions WHERE state = 'Active' ORDER BY table_id, partition_ns;
";

const SELECT_ALL: &str = "
SELECT table_id, partition_ns, state, file_count, total_bytes, min_ts, max_ts,
       created_at, frozen_at, compacting_at, archived_at, freeze_count, tenant_id
FROM partitions ORDER BY table_id, partition_ns;
";

const DELETE_PARTITION: &str = "
DELETE FROM partitions WHERE table_id = ?1 AND partition_ns = ?2;
";

const UPDATE_STATS: &str = "
UPDATE partitions SET file_count = ?3, total_bytes = ?4, min_ts = ?5, max_ts = ?6
WHERE table_id = ?1 AND partition_ns = ?2;
";

/// Persistence layer for partition metadata in `config.db`.
///
/// All methods that modify data are internally synchronized via a `Mutex`
/// on the connection. Reads also use the mutex to avoid SQLITE_BUSY on
/// concurrent access from the scheduler and API handlers.
pub struct PartitionMetadataStore {
    db: Mutex<Connection>,
}

impl PartitionMetadataStore {
    /// Open (or create) the `config.db` database and initialize the
    /// `partitions` table and indexes.
    ///
    /// # Errors
    ///
    /// Returns a `rusqlite::Error` if the database cannot be opened
    /// or the table cannot be created.
    pub fn open(db_path: &Path) -> SqlResult<Self> {
        // 确保元库父目录存在（`data_dir/metadata/`），否则 `Connection::open` 会因
        // 父目录缺失而失败（SQLITE_CANTOPEN）。best-effort 幂等：失败时交由下方
        // `Connection::open` 以自然错误上抛，不吞真实 IO 错误。
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        let conn = Connection::open(db_path)?;
        // Enable WAL mode for concurrent reads, plus NORMAL synchronous.
        //
        // `partitions` 表只是「存在性 + 状态机」的派生缓存，其唯一 ground truth 是 SAC
        // 目录（recovery.rs「disk wins」）。synchronous=NORMAL 下若进程崩溃/掉电导致
        // 最近提交的事务未落盘，下次启动 `discover_partitions_from_disk` 会从磁盘重扫
        // 重建这些行，不丢数据、不丢正确性；真实数据的持久性由 SAC 文件 + WAL 保证，
        // 与此 PRAGMA 无关。因此无需每批强制 `wal_checkpoint`，沿用 SQLite 默认
        // autocheckpoint 即可。
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA synchronous=NORMAL;",
        )?;
        conn.execute_batch(CREATE_TABLE)?;
        conn.execute_batch(CREATE_INDEX)?;
        Ok(Self {
            db: Mutex::new(conn),
        })
    }

    /// Insert or replace a partition record.
    ///
    /// If a record with the same `(table_id, partition_ns)` already exists,
    /// it is replaced. This is used during disk recovery to overwrite
    /// stale metadata with disk-ground-truth.
    pub fn insert_partition(&self, info: &PartitionInfo) -> SqlResult<()> {
        let db = self.db.lock();
        db.execute(
            INSERT_PARTITION,
            params![
                info.table_id,
                info.partition_ns,
                info.state.as_str(),
                info.file_count,
                info.total_bytes,
                info.min_ts,
                info.max_ts,
                info.created_at,
                info.frozen_at,
                info.compacting_at,
                info.archived_at,
                info.freeze_count,
                info.tenant_id.as_u32(),
            ],
        )?;
        Ok(())
    }

    /// Insert or replace multiple partition records in a single transaction.
    ///
    /// Used during disk recovery to bulk-persist derived metadata. A single
    /// transaction plus a reused prepared statement avoids the per-row
    /// implicit-commit + re-prepare + fsync cost of [`insert_partition`](Self::insert_partition).
    pub fn insert_partitions_batch(&self, infos: &[PartitionInfo]) -> SqlResult<()> {
        if infos.is_empty() {
            return Ok(());
        }
        let mut db = self.db.lock();
        let tx = db.transaction()?;
        {
            let mut stmt = tx.prepare_cached(INSERT_PARTITION)?;
            for info in infos {
                stmt.execute(params![
                    info.table_id,
                    info.partition_ns,
                    info.state.as_str(),
                    info.file_count,
                    info.total_bytes,
                    info.min_ts,
                    info.max_ts,
                    info.created_at,
                    info.frozen_at,
                    info.compacting_at,
                    info.archived_at,
                    info.freeze_count,
                    info.tenant_id.as_u32(),
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Update only the state of a partition.
    pub fn update_state(
        &self,
        table_id: u32,
        partition_ns: i64,
        state: PartitionState,
    ) -> SqlResult<()> {
        let db = self.db.lock();
        db.execute(
            UPDATE_STATE,
            params![table_id, partition_ns, state.as_str()],
        )?;
        Ok(())
    }

    /// Update state and set `frozen_at` timestamp.
    pub fn update_state_frozen(
        &self,
        table_id: u32,
        partition_ns: i64,
        frozen_at_ns: i64,
    ) -> SqlResult<()> {
        let db = self.db.lock();
        db.execute(
            UPDATE_STATE_AND_FROZEN_AT,
            params![
                table_id,
                partition_ns,
                PartitionState::Frozen.as_str(),
                frozen_at_ns
            ],
        )?;
        Ok(())
    }

    /// Update state and set `compacting_at` timestamp.
    pub fn update_state_compacting(
        &self,
        table_id: u32,
        partition_ns: i64,
        compacting_at_ns: i64,
    ) -> SqlResult<()> {
        let db = self.db.lock();
        db.execute(
            UPDATE_STATE_AND_COMPACTING_AT,
            params![
                table_id,
                partition_ns,
                PartitionState::Compacting.as_str(),
                compacting_at_ns
            ],
        )?;
        Ok(())
    }

    /// Update state and set `archived_at` timestamp.
    pub fn update_state_archived(
        &self,
        table_id: u32,
        partition_ns: i64,
        archived_at_ns: i64,
    ) -> SqlResult<()> {
        let db = self.db.lock();
        db.execute(
            UPDATE_STATE_AND_ARCHIVED_AT,
            params![
                table_id,
                partition_ns,
                PartitionState::Archived.as_str(),
                archived_at_ns
            ],
        )?;
        Ok(())
    }

    /// Update file stats (count, bytes, time range).
    pub fn update_stats(
        &self,
        table_id: u32,
        partition_ns: i64,
        file_count: u32,
        total_bytes: u64,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> SqlResult<()> {
        let db = self.db.lock();
        db.execute(
            UPDATE_STATS,
            params![
                table_id,
                partition_ns,
                file_count,
                total_bytes,
                min_ts,
                max_ts
            ],
        )?;
        Ok(())
    }

    /// List all partitions for a given table, ordered by `partition_ns` ascending.
    pub fn list_by_table(&self, table_id: u32) -> SqlResult<Vec<PartitionInfo>> {
        let db = self.db.lock();
        let mut stmt = db.prepare(SELECT_BY_TABLE)?;
        let rows = stmt.query_map(params![table_id], row_to_partition_info)?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Get a specific partition by `(table_id, partition_ns)`.
    pub fn get_partition(
        &self,
        table_id: u32,
        partition_ns: i64,
    ) -> SqlResult<Option<PartitionInfo>> {
        let db = self.db.lock();
        let mut stmt = db.prepare(SELECT_ONE)?;
        let mut rows = stmt.query_map(params![table_id, partition_ns], row_to_partition_info)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// List all partitions currently in the `Active` state.
    pub fn list_all_active(&self) -> SqlResult<Vec<PartitionInfo>> {
        let db = self.db.lock();
        let mut stmt = db.prepare(SELECT_ALL_ACTIVE)?;
        let rows = stmt.query_map([], row_to_partition_info)?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// List all partitions across all tables.
    pub fn list_all(&self) -> SqlResult<Vec<PartitionInfo>> {
        let db = self.db.lock();
        let mut stmt = db.prepare(SELECT_ALL)?;
        let rows = stmt.query_map([], row_to_partition_info)?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Delete a partition record.
    pub fn delete_partition(&self, table_id: u32, partition_ns: i64) -> SqlResult<()> {
        let db = self.db.lock();
        db.execute(DELETE_PARTITION, params![table_id, partition_ns])?;
        Ok(())
    }
}

/// Map a rusqlite row to a `PartitionInfo`.
fn row_to_partition_info(row: &rusqlite::Row<'_>) -> SqlResult<PartitionInfo> {
    let state_str: String = row.get(2)?;
    let state = PartitionState::from_str(&state_str).unwrap_or(PartitionState::Active);
    Ok(PartitionInfo {
        table_id: row.get(0)?,
        tenant_id: sensearcdb_common::TenantId(row.get::<_, i64>(12)? as u32),
        partition_ns: row.get(1)?,
        state,
        file_count: row.get(3)?,
        total_bytes: row.get::<_, i64>(4)? as u64,
        min_ts: row.get(5)?,
        max_ts: row.get(6)?,
        created_at: row.get(7)?,
        frozen_at: row.get(8)?,
        compacting_at: row.get(9)?,
        archived_at: row.get(10)?,
        freeze_count: row.get::<_, i64>(11)? as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_info(table_id: u32, partition_ns: i64, state: PartitionState) -> PartitionInfo {
        PartitionInfo {
            table_id,
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            partition_ns,
            state,
            file_count: 0,
            total_bytes: 0,
            min_ts: None,
            max_ts: None,
            created_at: 1000,
            frozen_at: None,
            compacting_at: None,
            archived_at: None,
            freeze_count: 0,
        }
    }

    #[test]
    fn test_create_and_insert() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        let info = make_info(1, 0, PartitionState::Active);
        store.insert_partition(&info).unwrap();

        let retrieved = store.get_partition(1, 0).unwrap();
        assert!(retrieved.is_some());
        let p = retrieved.unwrap();
        assert_eq!(p.table_id, 1);
        assert_eq!(p.partition_ns, 0);
        assert_eq!(p.state, PartitionState::Active);
    }

    #[test]
    fn test_update_state() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        let info = make_info(1, 100, PartitionState::Active);
        store.insert_partition(&info).unwrap();

        store.update_state(1, 100, PartitionState::Frozen).unwrap();

        let p = store.get_partition(1, 100).unwrap().unwrap();
        assert_eq!(p.state, PartitionState::Frozen);
    }

    #[test]
    fn test_update_state_frozen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        let info = make_info(2, 200, PartitionState::Active);
        store.insert_partition(&info).unwrap();

        let now = 5_000_000;
        store.update_state_frozen(2, 200, now).unwrap();

        let p = store.get_partition(2, 200).unwrap().unwrap();
        assert_eq!(p.state, PartitionState::Frozen);
        assert_eq!(p.frozen_at, Some(now));
    }

    #[test]
    fn test_full_state_machine_crud() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        // Insert Active
        let info = make_info(42, 1_000_000, PartitionState::Active);
        store.insert_partition(&info).unwrap();

        // Active → Frozen
        store.update_state_frozen(42, 1_000_000, 2_000_000).unwrap();
        let p = store.get_partition(42, 1_000_000).unwrap().unwrap();
        assert_eq!(p.state, PartitionState::Frozen);

        // Frozen → Compacting
        store
            .update_state_compacting(42, 1_000_000, 3_000_000)
            .unwrap();
        let p = store.get_partition(42, 1_000_000).unwrap().unwrap();
        assert_eq!(p.state, PartitionState::Compacting);

        // Compacting → Archived
        store
            .update_state_archived(42, 1_000_000, 4_000_000)
            .unwrap();
        let p = store.get_partition(42, 1_000_000).unwrap().unwrap();
        assert_eq!(p.state, PartitionState::Archived);

        // Archived → Expired
        store
            .update_state(42, 1_000_000, PartitionState::Expired)
            .unwrap();
        let p = store.get_partition(42, 1_000_000).unwrap().unwrap();
        assert_eq!(p.state, PartitionState::Expired);
    }

    #[test]
    fn test_list_by_table() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        store
            .insert_partition(&make_info(1, 0, PartitionState::Active))
            .unwrap();
        store
            .insert_partition(&make_info(1, 100, PartitionState::Frozen))
            .unwrap();
        store
            .insert_partition(&make_info(2, 0, PartitionState::Active))
            .unwrap();

        let list1 = store.list_by_table(1).unwrap();
        assert_eq!(list1.len(), 2);
        assert_eq!(list1[0].partition_ns, 0);
        assert_eq!(list1[1].partition_ns, 100);

        let list2 = store.list_by_table(2).unwrap();
        assert_eq!(list2.len(), 1);
    }

    #[test]
    fn test_list_all_active() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        store
            .insert_partition(&make_info(1, 0, PartitionState::Active))
            .unwrap();
        store
            .insert_partition(&make_info(1, 100, PartitionState::Frozen))
            .unwrap();
        store
            .insert_partition(&make_info(2, 200, PartitionState::Active))
            .unwrap();

        let active = store.list_all_active().unwrap();
        assert_eq!(active.len(), 2);
    }

    #[test]
    fn test_delete_partition() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        store
            .insert_partition(&make_info(1, 0, PartitionState::Active))
            .unwrap();
        assert!(store.get_partition(1, 0).unwrap().is_some());

        store.delete_partition(1, 0).unwrap();
        assert!(store.get_partition(1, 0).unwrap().is_none());
    }

    #[test]
    fn test_update_stats() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        let info = make_info(1, 0, PartitionState::Active);
        store.insert_partition(&info).unwrap();

        store
            .update_stats(1, 0, 5, 1024 * 1024, Some(100), Some(500))
            .unwrap();

        let p = store.get_partition(1, 0).unwrap().unwrap();
        assert_eq!(p.file_count, 5);
        assert_eq!(p.total_bytes, 1024 * 1024);
        assert_eq!(p.min_ts, Some(100));
        assert_eq!(p.max_ts, Some(500));
    }

    #[test]
    fn test_list_all() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_config.db");
        let store = PartitionMetadataStore::open(&db_path).unwrap();

        store
            .insert_partition(&make_info(1, 0, PartitionState::Active))
            .unwrap();
        store
            .insert_partition(&make_info(2, 0, PartitionState::Archived))
            .unwrap();
        store
            .insert_partition(&make_info(3, 0, PartitionState::Expired))
            .unwrap();

        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 3);
    }
}

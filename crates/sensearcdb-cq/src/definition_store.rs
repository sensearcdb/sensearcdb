//! CQ 定义持久化存储。
//!
//! `CqDefinitionStore` 将 `CqDefinition` 落盘到 config.db 的 `cq_definitions` 表，
//! 使 CQ 定义与运行水位（`last_executed_window_end` / `consecutive_failures` / `status`）
//! 在进程重启后可从 SQLite 恢复，不再依赖纯内存。
//!
//! ## 与 Stream 持久化共享模式（B4 D4）
//!
//! - 同库：同一个 `config.db`（`cfg.paths.config_db_path`）。
//! - 同连接模式：WAL。
//! - 分表：`cq_definitions` 与 `stream_tasks` 各自独立表，互不侵入。
//!
//! 本模块仅负责 `cq_definitions` 表（B4 范畴），不代 B5 实现 `stream_tasks`。

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, Transaction, TransactionBehavior};
use sensearcdb_common::{AggFunction, SenseArcError};

use crate::definition::{CqDefinition, CqGranularity, CqStatus};

/// `cq_definitions` 建表 DDL（幂等）。
const CQ_DEFINITIONS_DDL: &str = "
CREATE TABLE IF NOT EXISTS cq_definitions (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    name                     TEXT    NOT NULL UNIQUE,
    source_table_id          INTEGER NOT NULL,
    target_table_id          INTEGER NOT NULL,
    lower_target_table_id    INTEGER,
    granularity              TEXT    NOT NULL,
    interval_ns              INTEGER NOT NULL,
    source_columns           TEXT    NOT NULL,
    aggregations             TEXT    NOT NULL,
    source_types             TEXT    NOT NULL DEFAULT '[]',
    status                   TEXT    NOT NULL DEFAULT 'Running',
    last_executed_window_end INTEGER NOT NULL DEFAULT 0,
    consecutive_failures     INTEGER NOT NULL DEFAULT 0,
    created_at               INTEGER NOT NULL,
    updated_at               INTEGER NOT NULL,
    UNIQUE (source_table_id, granularity)
);
";

/// `cq_definitions` 单条 upsert SQL（以 `(source_table_id, granularity)` 为唯一键）。
const UPSERT_SQL: &str = "
    INSERT INTO cq_definitions (
        name, source_table_id, target_table_id, lower_target_table_id, granularity,
        interval_ns, source_columns, aggregations, source_types, status,
        last_executed_window_end, consecutive_failures, created_at, updated_at
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
    ON CONFLICT(source_table_id, granularity) DO UPDATE SET
        name = excluded.name,
        target_table_id = excluded.target_table_id,
        lower_target_table_id = excluded.lower_target_table_id,
        interval_ns = excluded.interval_ns,
        source_columns = excluded.source_columns,
        aggregations = excluded.aggregations,
        source_types = excluded.source_types,
        status = excluded.status,
        last_executed_window_end = excluded.last_executed_window_end,
        consecutive_failures = excluded.consecutive_failures,
        updated_at = excluded.updated_at
";

/// CQ 定义持久化存储。
///
/// 内部使用 `Arc<Mutex<Connection>>` 保证线程安全，并允许复用外部共享连接
/// （[`from_shared_conn`](Self::from_shared_conn)）收敛单一 writer；连接启用 WAL 模式。
pub struct CqDefinitionStore {
    conn: Arc<Mutex<Connection>>,
}

impl CqDefinitionStore {
    /// 打开（或创建）config.db 中的 `cq_definitions` 表。
    ///
    /// # Arguments
    /// - `db_path`: config.db 文件路径。
    pub fn open(db_path: &Path) -> Result<Self, SenseArcError> {
        let conn = Connection::open(db_path).map_err(|e| {
            SenseArcError::CatalogError(format!("failed to open config.db for CQ definitions: {e}"))
        })?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| {
                SenseArcError::CatalogError(format!(
                    "failed to set WAL mode for CQ definitions: {e}"
                ))
            })?;

        Self::from_shared_conn(Arc::new(Mutex::new(conn)))
    }

    /// 复用外部已打开的共享连接（P1-1：config.db 写收敛单一 writer）。
    ///
    /// 生产路径由 `Catalog::shared_conn()` 提供同一 `Mutex<Connection>`，使
    /// `Catalog` 与 `CqDefinitionStore` 不再各自 `Connection::open` 同一 config.db，
    /// 消灭 SQLite WAL 单写者约束下的双连接写锁竞争。连接上的 WAL / busy_timeout
    /// 已由 `Catalog::open` 配置；此处仅补 `cq_definitions` 建表与列迁移（幂等）。
    pub fn from_shared_conn(conn: Arc<Mutex<Connection>>) -> Result<Self, SenseArcError> {
        let store = Self { conn };
        store.ensure_schema()?;
        Ok(store)
    }

    /// 确保 `cq_definitions` 表存在（幂等），并执行老库列迁移。
    fn ensure_schema(&self) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("CQ definition store lock error: {e}"))
        })?;
        conn.execute_batch(CQ_DEFINITIONS_DDL).map_err(|e| {
            SenseArcError::CatalogError(format!("failed to ensure cq_definitions schema: {e}"))
        })?;

        // 老库迁移：补 `source_types` 列（TEXT，存 serde_json `Vec<DataType>`）。
        // 容错「列已存在」错误（新库 DDL 已含该列，ALTER 会因 duplicate column 失败）。
        //
        // 双通道容错（B3）：本 store 的幂等 ALTER 是「store 独立 open 自洽」的兜底；
        // 正式版本化迁移由 `sensearcdb_schema::migration` 的 v2 承担（Catalog 启动时统一跑）。
        // 两处均容错 duplicate column name，全新库 / B4 前老库 / B5 后库三态幂等收敛。
        match conn.execute_batch(
            "ALTER TABLE cq_definitions ADD COLUMN source_types TEXT NOT NULL DEFAULT '[]';",
        ) {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("duplicate column name") || msg.contains("already exists") {
                    // 列已存在，幂等跳过。
                } else {
                    return Err(SenseArcError::CatalogError(format!(
                        "failed to migrate cq_definitions.source_types column: {e}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// upsert 一条 CQ 定义（幂等，以 `(source_table_id, granularity)` 为唯一键）。
    ///
    /// 委托 [`upsert_batch`](Self::upsert_batch) 的单元素实现，保持既有调用语义不变。
    pub fn upsert(&self, def: &CqDefinition) -> Result<(), SenseArcError> {
        self.upsert_batch(std::slice::from_ref(def))
    }

    /// 批量 upsert CQ 定义（单个 `BEGIN IMMEDIATE` 事务，P1-2）。
    ///
    /// `on_table_created` 一次要为 4 粒度落 4 条定义，`reconcile_cq_tables` 对账同理；
    /// 若走单条 autocommit 会形成 4×N 次「抢锁/释放锁」，在数千表规模下放大写锁竞争。
    /// 这里把多条 upsert 合成一个事务（`BEGIN IMMEDIATE` + 批量 upsert + `COMMIT`），
    /// 将 16000 次锁竞争收敛为「每表 1 次」。
    pub fn upsert_batch(&self, defs: &[CqDefinition]) -> Result<(), SenseArcError> {
        if defs.is_empty() {
            return Ok(());
        }

        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("CQ definition store lock error: {e}"))
        })?;

        // BEGIN IMMEDIATE：提前拿 RESERVED 写锁，避免 deferred「先读后写」升级写锁时
        // 跳过 busy handler 导致微秒级 `database is locked`（与 Catalog 同策略）。
        let tx =
            Transaction::new_unchecked(&conn, TransactionBehavior::Immediate).map_err(|e| {
                SenseArcError::CatalogError(format!("begin cq definition batch tx error: {e}"))
            })?;

        let now = now_nanos();
        let mut stmt = tx.prepare_cached(UPSERT_SQL).map_err(|e| {
            SenseArcError::CatalogError(format!("prepare cq definition upsert error: {e}"))
        })?;

        for def in defs {
            let source_columns = serde_json::to_string(&def.source_columns).map_err(|e| {
                SenseArcError::CatalogError(format!("serialize source_columns: {e}"))
            })?;
            let aggregations = serde_json::to_string(&def.aggregations)
                .map_err(|e| SenseArcError::CatalogError(format!("serialize aggregations: {e}")))?;
            let source_types = serde_json::to_string(&def.source_types)
                .map_err(|e| SenseArcError::CatalogError(format!("serialize source_types: {e}")))?;

            stmt.execute(params![
                def.name,
                def.source_table_id,
                def.target_table_id,
                def.lower_target_table_id,
                def.granularity.name(),
                def.interval_ns,
                source_columns,
                aggregations,
                source_types,
                def.status.as_str(),
                def.last_executed_window_end,
                def.consecutive_failures,
                now,
                now,
            ])
            .map_err(|e| SenseArcError::CatalogError(format!("upsert cq_definition error: {e}")))?;
        }

        // 显式 drop Statement（借用 tx），再提交事务。
        drop(stmt);
        tx.commit().map_err(|e| {
            SenseArcError::CatalogError(format!("commit cq definition batch error: {e}"))
        })?;

        Ok(())
    }

    /// 删除指定源表的全部 CQ 定义。
    pub fn delete_by_source(&self, source_table_id: u32) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("CQ definition store lock error: {e}"))
        })?;
        conn.execute(
            "DELETE FROM cq_definitions WHERE source_table_id = ?1",
            params![source_table_id],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("delete cq_definition error: {e}")))?;
        Ok(())
    }

    /// 更新运行水位与状态（调度器 record_success 后调用）。
    pub fn update_watermark(
        &self,
        source_table_id: u32,
        granularity: CqGranularity,
        window_end: i64,
        consecutive_failures: u32,
        status: CqStatus,
    ) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("CQ definition store lock error: {e}"))
        })?;
        let now = now_nanos();
        conn.execute(
            "UPDATE cq_definitions
             SET last_executed_window_end = ?1,
                 consecutive_failures = ?2,
                 status = ?3,
                 updated_at = ?4
             WHERE source_table_id = ?5 AND granularity = ?6",
            params![
                window_end,
                consecutive_failures,
                status.as_str(),
                now,
                source_table_id,
                granularity.name(),
            ],
        )
        .map_err(|e| {
            SenseArcError::CatalogError(format!("update cq_definition watermark error: {e}"))
        })?;
        Ok(())
    }

    /// 更新失败计数与状态（record_failure 后调用，不触碰 `last_executed_window_end`）。
    pub fn update_status(
        &self,
        source_table_id: u32,
        granularity: CqGranularity,
        consecutive_failures: u32,
        status: CqStatus,
    ) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("CQ definition store lock error: {e}"))
        })?;
        let now = now_nanos();
        conn.execute(
            "UPDATE cq_definitions
             SET consecutive_failures = ?1,
                 status = ?2,
                 updated_at = ?3
             WHERE source_table_id = ?4 AND granularity = ?5",
            params![
                consecutive_failures,
                status.as_str(),
                now,
                source_table_id,
                granularity.name(),
            ],
        )
        .map_err(|e| {
            SenseArcError::CatalogError(format!("update cq_definition status error: {e}"))
        })?;
        Ok(())
    }

    /// 加载全部 CQ 定义（启动恢复）。
    pub fn load_all(&self) -> Result<Vec<CqDefinition>, SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("CQ definition store lock error: {e}"))
        })?;

        let mut stmt = conn
            .prepare(
                "SELECT name, source_table_id, target_table_id, lower_target_table_id,
                        granularity, interval_ns, source_columns, aggregations, source_types,
                        status, last_executed_window_end, consecutive_failures
                 FROM cq_definitions ORDER BY source_table_id, granularity",
            )
            .map_err(|e| SenseArcError::CatalogError(format!("load_all prepare error: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(CqDefinitionRow {
                    name: row.get(0)?,
                    source_table_id: row.get(1)?,
                    target_table_id: row.get(2)?,
                    lower_target_table_id: row.get(3)?,
                    granularity: row.get(4)?,
                    interval_ns: row.get(5)?,
                    source_columns: row.get(6)?,
                    aggregations: row.get(7)?,
                    source_types: row.get(8)?,
                    status: row.get(9)?,
                    last_executed_window_end: row.get(10)?,
                    consecutive_failures: row.get(11)?,
                })
            })
            .map_err(|e| SenseArcError::CatalogError(format!("load_all query error: {e}")))?;

        let mut out = Vec::new();
        for row in rows {
            let row =
                row.map_err(|e| SenseArcError::CatalogError(format!("load_all row error: {e}")))?;
            out.push(row.to_definition()?);
        }
        Ok(out)
    }

    /// 列出 config.db 中已持久化 CQ 定义的源表 ID 集合（对账真相源，不反序列化定义体）。
    pub fn list_source_table_ids(&self) -> Result<std::collections::HashSet<u32>, SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("CQ definition store lock error: {e}"))
        })?;
        let mut stmt = conn
            .prepare("SELECT DISTINCT source_table_id FROM cq_definitions")
            .map_err(|e| {
                SenseArcError::CatalogError(format!("list_source_table_ids prepare error: {e}"))
            })?;
        let ids = stmt
            .query_map([], |row| row.get::<_, u32>(0))
            .map_err(|e| {
                SenseArcError::CatalogError(format!("list_source_table_ids query error: {e}"))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(ids)
    }
}

/// 内部行类型（反序列化前）。
struct CqDefinitionRow {
    name: String,
    source_table_id: u32,
    target_table_id: u32,
    lower_target_table_id: Option<u32>,
    granularity: String,
    interval_ns: i64,
    source_columns: String,
    aggregations: String,
    source_types: String,
    status: String,
    last_executed_window_end: i64,
    consecutive_failures: u32,
}

impl CqDefinitionRow {
    fn to_definition(self) -> Result<CqDefinition, SenseArcError> {
        let granularity = CqGranularity::from_name(&self.granularity).ok_or_else(|| {
            SenseArcError::CatalogError(format!(
                "unknown cq granularity in config.db: {}",
                self.granularity
            ))
        })?;
        let source_columns: Vec<u16> = serde_json::from_str(&self.source_columns)
            .map_err(|e| SenseArcError::CatalogError(format!("deserialize source_columns: {e}")))?;
        let aggregations: Vec<AggFunction> = serde_json::from_str(&self.aggregations)
            .map_err(|e| SenseArcError::CatalogError(format!("deserialize aggregations: {e}")))?;
        let source_types: Vec<sensearcdb_common::DataType> =
            serde_json::from_str(&self.source_types).map_err(|e| {
                SenseArcError::CatalogError(format!("deserialize source_types: {e}"))
            })?;

        Ok(CqDefinition {
            name: self.name,
            source_table_id: self.source_table_id,
            target_table_id: self.target_table_id,
            lower_target_table_id: self.lower_target_table_id,
            granularity,
            interval_ns: self.interval_ns,
            source_columns,
            aggregations,
            source_types,
            status: CqStatus::from_str(&self.status),
            last_executed_window_end: self.last_executed_window_end,
            consecutive_failures: self.consecutive_failures,
        })
    }
}

/// 当前 epoch 纳秒时间戳（内部辅助）。
fn now_nanos() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sensearcdb_common::AggFunction;

    fn temp_db() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("test_cq_def_{}.db", uuid::Uuid::new_v4()))
    }

    fn sample_def(source: u32, gran: CqGranularity, target: u32) -> CqDefinition {
        let mut def = CqDefinition::new(
            format!("cq_{}_{}", gran.short_name(), source),
            source,
            target,
            gran,
            vec![0, 1],
            vec![AggFunction::Avg, AggFunction::Sum],
        );
        def.source_types = vec![
            sensearcdb_common::DataType::F64,
            sensearcdb_common::DataType::U64,
        ];
        def
    }

    #[test]
    fn test_upsert_and_load_roundtrip() {
        let path = temp_db();
        let store = CqDefinitionStore::open(&path).unwrap();

        let mut def = sample_def(42, CqGranularity::Cq10s, 100);
        def.set_lower_target_table_id(0);
        def.last_executed_window_end = 10_000_000_000;
        def.consecutive_failures = 2;
        store.upsert(&def).unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        let d = &loaded[0];
        assert_eq!(d.name, "cq_10s_42");
        assert_eq!(d.source_table_id, 42);
        assert_eq!(d.target_table_id, 100);
        assert_eq!(d.granularity, CqGranularity::Cq10s);
        assert_eq!(d.source_columns, vec![0, 1]);
        assert_eq!(d.aggregations, vec![AggFunction::Avg, AggFunction::Sum]);
        assert_eq!(
            d.source_types,
            vec![
                sensearcdb_common::DataType::F64,
                sensearcdb_common::DataType::U64
            ]
        );
        assert_eq!(d.last_executed_window_end, 10_000_000_000);
        assert_eq!(d.consecutive_failures, 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_upsert_idempotent() {
        let path = temp_db();
        let store = CqDefinitionStore::open(&path).unwrap();
        let def = sample_def(7, CqGranularity::Cq1m, 200);
        store.upsert(&def).unwrap();
        store.upsert(&def).unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_delete_by_source() {
        let path = temp_db();
        let store = CqDefinitionStore::open(&path).unwrap();
        store
            .upsert(&sample_def(1, CqGranularity::Cq10s, 11))
            .unwrap();
        store
            .upsert(&sample_def(1, CqGranularity::Cq1m, 12))
            .unwrap();
        store
            .upsert(&sample_def(2, CqGranularity::Cq10s, 21))
            .unwrap();
        store.delete_by_source(1).unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].source_table_id, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_list_source_table_ids() {
        let path = temp_db();
        let store = CqDefinitionStore::open(&path).unwrap();

        // 空库返回空集合。
        assert!(store.list_source_table_ids().unwrap().is_empty());

        // upsert 后返回对应 source_table_id 集合（跨粒度自动去重）。
        store
            .upsert(&sample_def(1, CqGranularity::Cq10s, 11))
            .unwrap();
        store
            .upsert(&sample_def(1, CqGranularity::Cq1m, 12))
            .unwrap();
        store
            .upsert(&sample_def(2, CqGranularity::Cq10s, 21))
            .unwrap();

        let ids = store.list_source_table_ids().unwrap();
        assert_eq!(ids, std::collections::HashSet::from([1, 2]));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_update_watermark() {
        let path = temp_db();
        let store = CqDefinitionStore::open(&path).unwrap();
        store
            .upsert(&sample_def(9, CqGranularity::Cq1h, 90))
            .unwrap();
        store
            .update_watermark(
                9,
                CqGranularity::Cq1h,
                3_600_000_000_000,
                5,
                CqStatus::Error,
            )
            .unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded[0].last_executed_window_end, 3_600_000_000_000);
        assert_eq!(loaded[0].consecutive_failures, 5);
        assert_eq!(loaded[0].status, CqStatus::Error);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_restart_recovery() {
        let path = temp_db();
        {
            let store = CqDefinitionStore::open(&path).unwrap();
            for g in CqGranularity::all() {
                store
                    .upsert(&sample_def(3, g, 100 + g.ordinal() as u32))
                    .unwrap();
            }
        }
        // 模拟重启：重新 open + load
        {
            let store = CqDefinitionStore::open(&path).unwrap();
            let loaded = store.load_all().unwrap();
            assert_eq!(loaded.len(), 4);
        }
        let _ = std::fs::remove_file(&path);
    }

    /// P1-1：`from_shared_conn` 必须复用传入的同一连接对象，而非再次 `Connection::open`
    /// 打开一个新连接（消灭 Catalog 与 CqDefinitionStore 双连接写锁竞争）。
    #[test]
    fn test_from_shared_conn_reuses_same_connection() {
        let path = temp_db();
        let conn = Arc::new(Mutex::new(Connection::open(&path).unwrap()));
        conn.lock()
            .unwrap()
            .execute_batch("PRAGMA journal_mode=WAL;")
            .unwrap();

        let store = CqDefinitionStore::from_shared_conn(conn.clone()).unwrap();
        assert!(
            Arc::ptr_eq(&store.conn, &conn),
            "from_shared_conn must reuse the exact same connection object"
        );

        let _ = std::fs::remove_file(&path);
    }

    /// P1-2：`upsert_batch` 单个事务——批量写入全部成功，且任一条目失败时整体回滚
    /// （验证 `BEGIN IMMEDIATE ... COMMIT` 的原子性）。
    #[test]
    fn test_upsert_batch_atomic_writes_all_or_rolls_back() {
        let path = temp_db();
        let store = CqDefinitionStore::open(&path).unwrap();

        // 1) 正常批量写入 4 条。
        let defs: Vec<CqDefinition> = (0..4u32)
            .map(|i| sample_def(i, CqGranularity::Cq10s, 100 + i))
            .collect();
        store.upsert_batch(&defs).unwrap();
        assert_eq!(store.load_all().unwrap().len(), 4);

        // 2) 制造 `name` UNIQUE 冲突：第 2 条 name 与第 1 条重复 → 事务整体回滚，
        //    第 1 条也不得残留（证明不是逐条 autocommit）。
        let d1 = sample_def(10, CqGranularity::Cq1m, 110);
        let mut d2 = sample_def(11, CqGranularity::Cq1m, 111);
        d2.name = d1.name.clone();
        let result = store.upsert_batch(&[d1, d2]);
        assert!(result.is_err(), "duplicate name must fail the whole batch");

        let loaded = store.load_all().unwrap();
        assert!(
            !loaded.iter().any(|d| d.source_table_id == 10),
            "batch rollback must not persist the first row"
        );

        let _ = std::fs::remove_file(&path);
    }

    /// 老库迁移：无 `source_types` 列的旧 config.db 打开时应补列且不报错，
    /// 旧行 `source_types` 回退为 `[]`。
    #[test]
    fn test_legacy_schema_migration_adds_source_types_column() {
        use rusqlite::Connection;

        let path = temp_db();
        {
            let conn = Connection::open(&path).unwrap();
            // 旧 DDL：不含 source_types 列
            conn.execute_batch(
                "CREATE TABLE cq_definitions (
                    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                    name                     TEXT    NOT NULL UNIQUE,
                    source_table_id          INTEGER NOT NULL,
                    target_table_id          INTEGER NOT NULL,
                    lower_target_table_id    INTEGER,
                    granularity              TEXT    NOT NULL,
                    interval_ns              INTEGER NOT NULL,
                    source_columns           TEXT    NOT NULL,
                    aggregations             TEXT    NOT NULL,
                    status                   TEXT    NOT NULL DEFAULT 'Running',
                    last_executed_window_end INTEGER NOT NULL DEFAULT 0,
                    consecutive_failures     INTEGER NOT NULL DEFAULT 0,
                    created_at               INTEGER NOT NULL,
                    updated_at               INTEGER NOT NULL,
                    UNIQUE (source_table_id, granularity)
                );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO cq_definitions
                    (name, source_table_id, target_table_id, lower_target_table_id, granularity,
                     interval_ns, source_columns, aggregations, status, last_executed_window_end,
                     consecutive_failures, created_at, updated_at)
                 VALUES ('cq_10s_1', 1, 11, NULL, 'CQ_10s', 10000000000, '[1]', '[\"Avg\"]',
                         'Running', 0, 0, 0, 0)",
                [],
            )
            .unwrap();
        }

        // 打开时执行迁移，随后 load 成功且 source_types 回退为 []
        let store = CqDefinitionStore::open(&path).unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].source_types,
            Vec::<sensearcdb_common::DataType>::new()
        );

        // 迁移后 upsert 应能写入 source_types
        store
            .upsert(&sample_def(1, CqGranularity::Cq10s, 11))
            .unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(
            loaded[0].source_types,
            vec![
                sensearcdb_common::DataType::F64,
                sensearcdb_common::DataType::U64
            ]
        );

        let _ = std::fs::remove_file(&path);
    }
}

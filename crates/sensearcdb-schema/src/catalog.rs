//! Catalog: SQLite config.db CRUD。
//!
//! 管理 `tables`、`columns`、`schema_versions` 四张表的持久化存储。
//! 连接使用 WAL 模式以获得更好的并发读取性能。

use crate::mirror::{ColumnDef, DeadbandConfig, DeadbandType, TableInfo, TableType};
use crate::schema_id::compute_schema_id;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sensearcdb_common::{DataType, Durability, SenseArcError, TenantId, TimestampNanos};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

/// DDL 幂等三态判定结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlApplyState {
    /// `applied_ddl` 已有 `(table_id, ddl_hash)` → 幂等跳过（已完成）。
    AlreadyApplied,
    /// `pending_ddl` 有、`applied_ddl` 无 → 续跑执行（上次在步骤 2 崩溃）。
    PendingResume,
    /// 两者都无 → 首次执行。
    NotStarted,
}

/// 幂等补列：`PRAGMA table_info` 判定列不存在时执行 `ALTER TABLE`。
fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    ddl: &str,
) -> Result<(), SenseArcError> {
    let exists = conn
        .prepare(&format!("PRAGMA table_info({})", table))
        .map_err(|e| SenseArcError::CatalogError(format!("pragma table_info: {}", e)))?
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| SenseArcError::CatalogError(format!("pragma query: {}", e)))?
        .filter_map(Result::ok)
        .any(|name| name == column);

    if !exists {
        conn.execute_batch(ddl)
            .map_err(|e| SenseArcError::CatalogError(format!("ensure_column {}: {}", column, e)))?;
    }
    Ok(())
}

/// SQLite Catalog 持久化层。
///
/// 内部使用 `Arc<Mutex<Connection>>` 确保线程安全，并允许跨 crate 共享同一写连接
/// （`shared_conn`）以收敛单一 writer。
/// 连接启用 WAL 模式以支持高并发读。
pub struct Catalog {
    conn: Arc<Mutex<Connection>>,
}

impl Catalog {
    /// 打开（或创建）`config.db`，启用 WAL 模式，执行建表 DDL。
    ///
    /// # Arguments
    /// - `db_path`: SQLite 数据库文件路径。
    pub fn open(db_path: &Path) -> Result<Self, SenseArcError> {
        let conn = Connection::open(db_path)
            .map_err(|e| SenseArcError::CatalogError(format!("failed to open config.db: {}", e)))?;

        // 启用 WAL 模式
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| SenseArcError::CatalogError(format!("failed to set WAL mode: {}", e)))?;

        // 启用外键约束
        conn.execute_batch("PRAGMA foreign_keys=ON;").map_err(|e| {
            SenseArcError::CatalogError(format!("failed to enable foreign keys: {}", e))
        })?;

        let catalog = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        catalog.ensure_schema()?;
        info!("Catalog opened: {:?} (WAL mode)", db_path);
        Ok(catalog)
    }

    /// 供同 crate 的 [`crate::tenant::TenantManager`] 访问底层连接（S1 tenants seed）。
    pub(crate) fn conn(&self) -> &Mutex<Connection> {
        self.conn.as_ref()
    }

    /// 共享底层 `Mutex<Connection>`（P1-1：config.db 写收敛单一 writer）。
    ///
    /// 供跨 crate 复用同一写连接——`sensearcdb-cq` 的 `CqDefinitionStore` 通过此方法
    /// 拿到与 `Catalog` 相同的连接，从而消灭「Catalog 与 CqDefinitionStore 各自
    /// `Connection::open` 同一 config.db 抢写锁」的双连接竞争（SQLite WAL 单写者）。
    pub fn shared_conn(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }

    /// 以 `BEGIN IMMEDIATE` 开启一个写事务。
    ///
    /// # 为什么必须用 IMMEDIATE 而非默认的 DEFERRED（P0：database is locked）
    ///
    /// `Catalog` 的写事务普遍是「先读后写」序列（例如 `create_table` 先
    /// `SELECT COUNT(*)` 查重再 `INSERT`）。默认的 `BEGIN`（DEFERRED）在首个
    /// 读语句时才建立读快照、在首个写语句时才尝试升级写锁；在共享 WAL 模式
    /// `config.db` 上，当另一个独立连接（如 `CqDefinitionStore`）正持有写锁时，
    /// 这种「读快照 → 写锁」升级会**立即**返回 `SQLITE_BUSY`（`database is
    /// locked`），且**不触发 busy handler**——因此 `PRAGMA busy_timeout=5000`
    /// 完全无效，错误在微秒级返回（实测 ~33µs）。
    ///
    /// `BEGIN IMMEDIATE` 在任何读之前先取得写（RESERVED）锁，锁竞争改由
    /// busy handler 处理（busy_timeout 生效，实测等待 ~5s 后按需重试），从而
    /// 根治跨连接写锁竞争导致的建表 500。参见 SQLite WAL 文档：写事务应使用
    /// `BEGIN IMMEDIATE` 以避免读升级写时跳过 busy handler。
    fn begin_write_tx(conn: &Connection) -> Result<Transaction<'_>, rusqlite::Error> {
        Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
    }

    /// 确保数据库表结构存在。
    fn ensure_schema(&self) -> Result<(), SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS schema_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tables (
                table_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name     TEXT    NOT NULL,
                schema_id      INTEGER NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                tenant_id      INTEGER NOT NULL DEFAULT 0,
                table_group    TEXT,
                table_type     TEXT    NOT NULL DEFAULT 'RAW',
                durability     TEXT    NOT NULL DEFAULT 'group',
                description    TEXT,
                enabled        INTEGER NOT NULL DEFAULT 1,
                deleted_at     INTEGER,
                created_at     INTEGER NOT NULL,
                updated_at     INTEGER NOT NULL
            );

            -- tables.table_name 租户内唯一化（多租户语义）：去掉内联 UNIQUE，改 partial 唯一索引。
            -- 逻辑删除（deleted_at IS NOT NULL）的同名行不占唯一名额，删后可同名重建。
            CREATE UNIQUE INDEX IF NOT EXISTS uq_tables_tenant_name
                ON tables (tenant_id, table_name)
                WHERE deleted_at IS NULL;

            CREATE TABLE IF NOT EXISTS columns (
                column_id              INTEGER NOT NULL,
                table_id               INTEGER NOT NULL,
                name                   TEXT    NOT NULL,
                data_type              TEXT    NOT NULL,
                precision              REAL    NOT NULL DEFAULT 0.0,
                nullable               INTEGER NOT NULL DEFAULT 0,
                unit                   TEXT,
                description            TEXT,
                deadband_type          TEXT,
                deadband_value         REAL,
                range_low              REAL    NOT NULL DEFAULT 0.0,
                range_high             REAL    NOT NULL DEFAULT 100.0,
                instrument_range_low   REAL,
                instrument_range_high  REAL,
                display_digits         INTEGER NOT NULL DEFAULT 2,
                enum_values            TEXT,
                deleted_at             INTEGER,
                PRIMARY KEY (table_id, column_id),
                FOREIGN KEY (table_id) REFERENCES tables(table_id)
            );

            CREATE TABLE IF NOT EXISTS schema_versions (
                version_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id        INTEGER NOT NULL,
                schema_version  INTEGER NOT NULL,
                schema_id       INTEGER NOT NULL,
                operation       TEXT    NOT NULL,
                detail          TEXT,
                created_at      INTEGER NOT NULL,
                FOREIGN KEY (table_id) REFERENCES tables(table_id)
            );

            CREATE TABLE IF NOT EXISTS pending_ddl (
                table_id   INTEGER PRIMARY KEY,
                ddl_hash   BLOB    NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS applied_ddl (
                table_id   INTEGER PRIMARY KEY,
                ddl_hash   BLOB    NOT NULL,
                applied_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS migration_meta (
                migration_id TEXT    PRIMARY KEY,
                task         BLOB    NOT NULL,
                state        TEXT    NOT NULL,
                updated_at   INTEGER NOT NULL
            );

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

            CREATE TABLE IF NOT EXISTS stream_tasks (
                id                TEXT    PRIMARY KEY,
                name              TEXT    NOT NULL,
                tenant_id         INTEGER NOT NULL DEFAULT 0,
                source_table_id   INTEGER NOT NULL,
                target_table_id   INTEGER NOT NULL,
                window_spec_json  TEXT    NOT NULL,
                projection_json   TEXT    NOT NULL,
                partition_by_json TEXT,
                status            TEXT    NOT NULL DEFAULT 'Running',
                created_at        INTEGER NOT NULL,
                updated_at        INTEGER NOT NULL,
                UNIQUE (tenant_id, name)
            );

            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id         INTEGER PRIMARY KEY,
                slug              TEXT    UNIQUE,
                display_name      TEXT    NOT NULL,
                status            TEXT    NOT NULL DEFAULT 'ACTIVE',
                quota_template_id TEXT,
                initial_admin     TEXT,
                deleted_at        INTEGER,
                purge_at          INTEGER,
                created_at        INTEGER NOT NULL,
                updated_at        INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS quota_templates (
                template_id       TEXT    PRIMARY KEY,
                name              TEXT    NOT NULL,
                points_limit      INTEGER,
                storage_limit_gb  INTEGER,
                write_rate_limit  INTEGER,
                retention_days    INTEGER,
                collector_limit   INTEGER,
                user_limit        INTEGER,
                concurrent_limit  INTEGER,
                warn_ratio        REAL    NOT NULL DEFAULT 0.8,
                created_at        INTEGER NOT NULL,
                updated_at        INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tenant_quotas (
                tenant_id         INTEGER PRIMARY KEY,
                template_id       TEXT,
                points_limit      INTEGER,
                storage_limit_gb  INTEGER,
                write_rate_limit  INTEGER,
                retention_days    INTEGER,
                collector_limit   INTEGER,
                user_limit        INTEGER,
                concurrent_limit  INTEGER,
                warn_ratio        REAL    NOT NULL DEFAULT 0.8,
                updated_at        INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tenant_usage (
                tenant_id        INTEGER PRIMARY KEY,
                point_count      INTEGER NOT NULL DEFAULT 0,
                storage_bytes    INTEGER NOT NULL DEFAULT 0,
                collector_count  INTEGER NOT NULL DEFAULT 0,
                user_count       INTEGER NOT NULL DEFAULT 0,
                updated_at       INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tenant_deletions (
                tenant_id           INTEGER PRIMARY KEY,
                slug                TEXT    NOT NULL,
                display_name        TEXT,
                operator            TEXT    NOT NULL,
                delete_initiated_at INTEGER NOT NULL,
                purge_due_at        INTEGER NOT NULL,
                purge_completed_at  INTEGER NOT NULL,
                data_scope          TEXT    NOT NULL,
                purge_method        TEXT    NOT NULL,
                audit_anchor        TEXT    NOT NULL,
                certificate_json    TEXT    NOT NULL
            );
            ",
        )
        .map_err(|e| SenseArcError::CatalogError(format!("failed to ensure schema: {}", e)))?;

        // S4 Step 4：幂等补列（存量 config.db 无 tenant_id 时 ALTER 追加，零存量场景直接建表已有）。
        ensure_column(
            &conn,
            "tables",
            "tenant_id",
            "ALTER TABLE tables ADD COLUMN tenant_id INTEGER NOT NULL DEFAULT 0",
        )?;

        Ok(())
    }

    // ─── 全局 schema 版本（schema_meta.schema_version）─────────────────────

    /// 读取 config.db 的全局 schema 版本。
    ///
    /// `SELECT value FROM schema_meta WHERE key='schema_version'`；
    /// 无记录返回 `None`（调用方视为 0）；记录存在但非合法 `u32` 时 warn 并按 `None` 处理。
    pub fn get_schema_version(&self) -> Result<Option<u32>, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let value: Option<String> = conn
            .query_row(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| SenseArcError::CatalogError(format!("get_schema_version error: {}", e)))?;

        match value {
            None => Ok(None),
            Some(s) => match s.parse::<u32>() {
                Ok(v) => Ok(Some(v)),
                Err(_) => {
                    warn!(
                        value = %s,
                        "schema_meta.schema_version is not a valid u32; treating as unset"
                    );
                    Ok(None)
                }
            },
        }
    }

    /// 应用单条 schema 迁移：事务内「执行 DDL + 写 schema_version」原子提交。
    ///
    /// - 成功：执行 `sql` → `INSERT OR REPLACE INTO schema_meta('schema_version', target_version)` → commit。
    /// - DDL 失败且为「duplicate column name」（列已存在）时容错，视同已应用（仍写版本号，幂等收敛）。
    /// - 其它失败：回滚整条事务，版本不变，后续迁移不续跑。
    pub fn apply_migration(&self, target_version: u32, sql: &str) -> Result<(), SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tx = Self::begin_write_tx(&conn)
            .map_err(|e| SenseArcError::CatalogError(format!("begin migration tx error: {}", e)))?;

        if let Err(e) = tx.execute_batch(sql) {
            if is_duplicate_column(&e.to_string()) {
                // 列已存在，视为已应用（幂等跳过该 DDL，但仍推进版本号）。
                debug!(
                    target = target_version,
                    "migration DDL reports duplicate column, treating as already applied"
                );
            } else {
                let _ = tx.rollback();
                return Err(SenseArcError::CatalogError(format!(
                    "apply_migration v{} sql error: {}",
                    target_version, e
                )));
            }
        }

        if let Err(e) = tx.execute(
            "INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('schema_version', ?1)",
            params![target_version.to_string()],
        ) {
            let _ = tx.rollback();
            return Err(SenseArcError::CatalogError(format!(
                "apply_migration v{} write version error: {}",
                target_version, e
            )));
        }

        tx.commit().map_err(|e| {
            SenseArcError::CatalogError(format!(
                "apply_migration v{} commit error: {}",
                target_version, e
            ))
        })?;

        Ok(())
    }

    // ─── Slot 迁移 durable marker（migration_meta）──────────────────────────

    /// 落/更新迁移 durable marker（T5c：PREPARE 起每阶段推进落 Shard 0 catalog）。
    ///
    /// # 参数
    /// - `migration_id`: 迁移任务唯一标识。
    /// - `task`: bincode 序列化的 `MigrationTask` 字节（不透明）。
    /// - `state`: 迁移阶段稳定字符串（Prepare/Migrating/Verify/Switchover/Cleanup/Completed/Failed）。
    pub fn migration_upsert(
        &self,
        migration_id: &str,
        task: &[u8],
        state: &str,
    ) -> Result<(), SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let now = chrono::Utc::now().timestamp_millis();
        conn.execute(
            "INSERT OR REPLACE INTO migration_meta (migration_id, task, state, updated_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![migration_id, task, state, now],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("migration_upsert error: {}", e)))?;

        Ok(())
    }

    /// 读取迁移 durable marker（供崩溃续跑 / 校验用）。
    ///
    /// 返回 `Some((task 字节, state 字符串))`，未找到返回 `None`。
    pub fn migration_get(
        &self,
        migration_id: &str,
    ) -> Result<Option<(Vec<u8>, String)>, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let row: Option<(Vec<u8>, String)> = conn
            .query_row(
                "SELECT task, state FROM migration_meta WHERE migration_id = ?1",
                params![migration_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| SenseArcError::CatalogError(format!("migration_get error: {}", e)))?;

        Ok(row)
    }

    /// 枚举所有**非终态**迁移 durable marker（`state != Completed/Failed`）。
    ///
    /// 供启动时 `ShardManager::recover_migrations` 扫描崩溃遗留的未终态任务。
    /// `state` 仅用于「枚举哪些任务未终态」，**不**用于判定「切没切」——
    /// 切换点判定由恢复方读当前 ShardMap 决定（见 phase7-t5c2 拍板点④防坑说明）。
    ///
    /// # Returns
    /// `Vec<(migration_id, task 字节, state 字符串)>`，按迁移 ID 顺序返回。
    pub fn migration_list_non_terminal(
        &self,
    ) -> Result<Vec<(String, Vec<u8>, String)>, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let mut stmt = conn
            .prepare(
                "SELECT migration_id, task, state FROM migration_meta \
                 WHERE state != ?1 AND state != ?2 ORDER BY migration_id",
            )
            .map_err(|e| {
                SenseArcError::CatalogError(format!("migration_list_non_terminal prepare: {}", e))
            })?;

        let rows = stmt
            .query_map(params!["Completed", "Failed"], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|e| {
                SenseArcError::CatalogError(format!("migration_list_non_terminal query: {}", e))
            })?;

        let mut out = Vec::new();
        for row in rows {
            let (migration_id, task, state) = row.map_err(|e| {
                SenseArcError::CatalogError(format!("migration_list_non_terminal row: {}", e))
            })?;
            out.push((migration_id, task, state));
        }
        Ok(out)
    }

    // ─── DDL 幂等三态（pending_ddl / applied_ddl）──────────────────────────

    /// 判定 DDL 的幂等三态（architecture-master §6.14）。
    ///
    /// # 参数
    /// - `table_id`: 目标表 ID。
    /// - `ddl_hash`: DDL 幂等 key（blake3，跨节点确定性）。
    pub fn ddl_apply_state(
        &self,
        table_id: u32,
        ddl_hash: &[u8],
    ) -> Result<DdlApplyState, SenseArcError> {
        if self.ddl_is_applied(table_id, ddl_hash)? {
            Ok(DdlApplyState::AlreadyApplied)
        } else if self.ddl_is_pending(table_id, ddl_hash)? {
            Ok(DdlApplyState::PendingResume)
        } else {
            Ok(DdlApplyState::NotStarted)
        }
    }

    /// 查询 `applied_ddl` 是否已有 `(table_id, ddl_hash)`。
    pub fn ddl_is_applied(&self, table_id: u32, ddl_hash: &[u8]) -> Result<bool, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM applied_ddl WHERE table_id = ?1 AND ddl_hash = ?2",
                params![table_id, ddl_hash],
                |row| row.get(0),
            )
            .map_err(|e| SenseArcError::CatalogError(format!("ddl_is_applied error: {}", e)))?;

        Ok(count > 0)
    }

    /// 查询 `pending_ddl` 是否已有 `(table_id, ddl_hash)`。
    pub fn ddl_is_pending(&self, table_id: u32, ddl_hash: &[u8]) -> Result<bool, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pending_ddl WHERE table_id = ?1 AND ddl_hash = ?2",
                params![table_id, ddl_hash],
                |row| row.get(0),
            )
            .map_err(|e| SenseArcError::CatalogError(format!("ddl_is_pending error: {}", e)))?;

        Ok(count > 0)
    }

    /// 记录 DDL 开始执行（写 `pending_ddl`）。
    ///
    /// 首次执行时调用；崩溃恢复时该记录用于判定「续跑」。
    pub fn ddl_begin(&self, table_id: u32, ddl_hash: &[u8], now: i64) -> Result<(), SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        conn.execute(
            "INSERT OR REPLACE INTO pending_ddl (table_id, ddl_hash, created_at) \
             VALUES (?1, ?2, ?3)",
            params![table_id, ddl_hash, now],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("ddl_begin error: {}", e)))?;

        Ok(())
    }

    /// 记录 DDL 完成（事务性：删 `pending_ddl` + 写 `applied_ddl`）。
    ///
    /// 与 [`ddl_begin`](Self::ddl_begin) 构成两步原子写，保证崩溃恢复时
    /// 三态判定可精确区分「未开始 / 进行中 / 已完成」。
    pub fn ddl_complete(
        &self,
        table_id: u32,
        ddl_hash: &[u8],
        now: i64,
    ) -> Result<(), SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tx = Self::begin_write_tx(&conn)
            .map_err(|e| SenseArcError::CatalogError(format!("begin tx error: {}", e)))?;

        tx.execute(
            "DELETE FROM pending_ddl WHERE table_id = ?1",
            params![table_id],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("ddl_complete delete error: {}", e)))?;

        tx.execute(
            "INSERT OR REPLACE INTO applied_ddl (table_id, ddl_hash, applied_at) \
             VALUES (?1, ?2, ?3)",
            params![table_id, ddl_hash, now],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("ddl_complete insert error: {}", e)))?;

        tx.commit().map_err(|e| {
            SenseArcError::CatalogError(format!("ddl_complete commit error: {}", e))
        })?;

        Ok(())
    }

    // ─── Table CRUD ────────────────────────────────────────────────────────

    /// 创建新表及其列定义（事务性）。
    ///
    /// 返回分配了 `table_id` 的 `TableInfo`。
    /// 如果表名已存在（`UNIQUE` 约束冲突），返回 `TableAlreadyExists` 错误。
    pub fn create_table(&self, info: &TableInfo) -> Result<TableInfo, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tx = Self::begin_write_tx(&conn)
            .map_err(|e| SenseArcError::CatalogError(format!("begin tx error: {}", e)))?;

        let schema_id = compute_schema_id(&info.columns);

        // 应用层按 (tenant_id, table_name) 显式查重（active 表，deleted_at IS NULL）。
        // 租户内同名已存在 → TableAlreadyExists；跨租户同名不冲突。
        // 唯一索引 uq_tables_tenant_name 仍保留，作为并发双建的兜底。
        let dup_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM tables \
                 WHERE tenant_id = ?1 AND table_name = ?2 AND deleted_at IS NULL",
                params![info.tenant_id.as_u32(), info.table_name],
                |row| row.get(0),
            )
            .map_err(|e| {
                SenseArcError::CatalogError(format!("table name dup check error: {}", e))
            })?;
        if dup_count > 0 {
            let _ = tx.rollback();
            return Err(SenseArcError::TableAlreadyExists(info.table_name.clone()));
        }

        // 预分配 table_id 支持（MetaShard / Raft DDL 路径）。
        //
        // `info.table_id == 0` 走 AUTOINCREMENT 分配（单机 REST 建表路径）；
        // `info.table_id != 0` 显式插入预分配 table_id，保证 Raft apply 后
        // schema_mirror 与 MetaShard 预分配的 table_id 一致（table_id→Slot→Shard 路由）。
        let explicit_table_id = info.table_id != 0;
        let insert_result = if explicit_table_id {
            tx.execute(
                "INSERT INTO tables (table_id, table_name, schema_id, schema_version, tenant_id, \
                 table_group, table_type, durability, description, enabled, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    info.table_id,
                    info.table_name,
                    schema_id,
                    info.schema_version,
                    info.tenant_id.as_u32(),
                    info.table_group,
                    info.table_type.as_str(),
                    info.durability.as_str(),
                    info.description,
                    info.enabled as i32,
                    info.created_at.as_nanos(),
                    info.updated_at.as_nanos(),
                ],
            )
        } else {
            tx.execute(
                "INSERT INTO tables (table_name, schema_id, schema_version, tenant_id, table_group, \
                 table_type, durability, description, enabled, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    info.table_name,
                    schema_id,
                    info.schema_version,
                    info.tenant_id.as_u32(),
                    info.table_group,
                    info.table_type.as_str(),
                    info.durability.as_str(),
                    info.description,
                    info.enabled as i32,
                    info.created_at.as_nanos(),
                    info.updated_at.as_nanos(),
                ],
            )
        };

        insert_result.map_err(|e| {
            if is_unique_constraint(&e) {
                SenseArcError::TableAlreadyExists(info.table_name.clone())
            } else {
                SenseArcError::CatalogError(format!("insert table error: {}", e))
            }
        })?;

        let table_id: u32 = if explicit_table_id {
            info.table_id
        } else {
            tx.last_insert_rowid() as u32
        };

        // 插入 columns 行
        for col in &info.columns {
            Self::insert_column_in_tx(&tx, table_id, col)?;
        }

        // 记录 schema 版本
        tx.execute(
            "INSERT INTO schema_versions (table_id, schema_version, schema_id, operation, detail, created_at) \
             VALUES (?1, ?2, ?3, 'CREATE', ?4, ?5)",
            params![
                table_id,
                1u32,
                schema_id,
                format!("created table {}", info.table_name),
                info.created_at.as_nanos(),
            ],
        )
        .map_err(|e| {
            SenseArcError::CatalogError(format!("insert schema_version error: {}", e))
        })?;

        tx.commit()
            .map_err(|e| SenseArcError::CatalogError(format!("commit error: {}", e)))?;

        debug!(table_id, table_name = %info.table_name, schema_id, "table created");

        let mut result = info.clone();
        result.table_id = table_id;
        result.schema_id = schema_id;
        Ok(result)
    }

    /// 查询单张表（按 table_id），含所有列定义。
    pub fn get_table(&self, table_id: u32) -> Result<Option<TableInfo>, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let table_row = conn
            .query_row(
                "SELECT table_id, table_name, schema_id, schema_version, tenant_id, table_group, \
                 table_type, durability, description, enabled, deleted_at, created_at, updated_at \
                 FROM tables WHERE table_id = ?1",
                params![table_id],
                read_table_row,
            )
            .optional()
            .map_err(|e| SenseArcError::CatalogError(format!("get_table error: {}", e)))?;

        match table_row {
            Some(row) => {
                let columns = self.get_columns_inner(&conn, table_id)?;
                Ok(Some(row_to_table_info(row, columns)))
            }
            None => Ok(None),
        }
    }

    /// 查询单张表（按 tenant_id + table_name，租户内唯一）。
    ///
    /// 跨租户允许同名，故必须携带 `tenant_id` 才能唯一定位；无租户上下文的
    /// 调用方须显式传 [`TenantId::DEFAULT`]（多租户读隔离另立项）。
    pub fn get_table_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<Option<TableInfo>, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let table_row = conn
            .query_row(
                "SELECT table_id, table_name, schema_id, schema_version, tenant_id, table_group, \
                 table_type, durability, description, enabled, deleted_at, created_at, updated_at \
                 FROM tables WHERE table_name = ?1 AND tenant_id = ?2 AND deleted_at IS NULL",
                params![name, tenant_id.as_u32()],
                read_table_row,
            )
            .optional()
            .map_err(|e| SenseArcError::CatalogError(format!("get_table_by_name error: {}", e)))?;

        match table_row {
            Some(row) => {
                let columns = self.get_columns_inner(&conn, row.table_id)?;
                Ok(Some(row_to_table_info(row, columns)))
            }
            None => Ok(None),
        }
    }

    /// 按 (tenant_id, table_name) 查重（仅统计未逻辑删除的 active 表）。
    ///
    /// 与唯一索引 `uq_tables_tenant_name`（`WHERE deleted_at IS NULL`）同口径：
    /// 逻辑删除后的同名表不占唯一名额。供建表前的显式判定与测试断言使用。
    pub fn table_name_exists(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<bool, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tables \
                 WHERE tenant_id = ?1 AND table_name = ?2 AND deleted_at IS NULL",
                params![tenant_id.as_u32(), name],
                |row| row.get(0),
            )
            .map_err(|e| SenseArcError::CatalogError(format!("table_name_exists error: {}", e)))?;

        Ok(count > 0)
    }

    /// 列出所有表（不含已逻辑删除的）。
    pub fn list_tables(&self) -> Result<Vec<TableInfo>, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let mut stmt = conn
            .prepare(
                "SELECT table_id, table_name, schema_id, schema_version, tenant_id, table_group, \
                 table_type, durability, description, enabled, deleted_at, created_at, updated_at \
                 FROM tables WHERE deleted_at IS NULL ORDER BY table_id",
            )
            .map_err(|e| SenseArcError::CatalogError(format!("list_tables error: {}", e)))?;

        let rows: Vec<TableRow> = stmt
            .query_map([], read_table_row)
            .map_err(|e| SenseArcError::CatalogError(format!("list_tables error: {}", e)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SenseArcError::CatalogError(format!("list_tables error: {}", e)))?;

        // #5：一次取全部未删除列，按 table_id 内存分组，消除逐表 N+1 查询。
        let mut cols_by_table: HashMap<u32, Vec<ColumnDef>> = HashMap::new();
        for (tid, col) in self.get_all_columns_inner(&conn)? {
            cols_by_table.entry(tid).or_default().push(col);
        }

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let columns = cols_by_table.remove(&row.table_id).unwrap_or_default();
            result.push(row_to_table_info(row, columns));
        }
        Ok(result)
    }

    /// 列出某租户的表（不含已逻辑删除的；平台视角用 [`list_tables`](Self::list_tables)）。
    pub fn list_tables_for_tenant(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<TableInfo>, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let mut stmt = conn
            .prepare(
                "SELECT table_id, table_name, schema_id, schema_version, tenant_id, table_group, \
                 table_type, durability, description, enabled, deleted_at, created_at, updated_at \
                 FROM tables WHERE deleted_at IS NULL AND tenant_id = ?1 ORDER BY table_id",
            )
            .map_err(|e| {
                SenseArcError::CatalogError(format!("list_tables_for_tenant error: {}", e))
            })?;

        let rows: Vec<TableRow> = stmt
            .query_map(params![tenant_id.as_u32()], read_table_row)
            .map_err(|e| {
                SenseArcError::CatalogError(format!("list_tables_for_tenant error: {}", e))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                SenseArcError::CatalogError(format!("list_tables_for_tenant error: {}", e))
            })?;

        // #5：一次取全部未删除列，按 table_id 内存分组；仅该租户的表会被取走，
        // 其余租户的列留在 map 中不参与组装，语义与逐表查询完全一致。
        let mut cols_by_table: HashMap<u32, Vec<ColumnDef>> = HashMap::new();
        for (tid, col) in self.get_all_columns_inner(&conn)? {
            cols_by_table.entry(tid).or_default().push(col);
        }

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let columns = cols_by_table.remove(&row.table_id).unwrap_or_default();
            result.push(row_to_table_info(row, columns));
        }
        Ok(result)
    }

    /// 逻辑删除表（设置 `deleted_at` 时间戳）。
    pub fn drop_table(
        &self,
        table_id: u32,
        deleted_at: TimestampNanos,
    ) -> Result<(), SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let affected = conn
            .execute(
                "UPDATE tables SET deleted_at = ?1, updated_at = ?1 WHERE table_id = ?2 AND deleted_at IS NULL",
                params![deleted_at.as_nanos(), table_id],
            )
            .map_err(|e| SenseArcError::CatalogError(format!("drop_table error: {}", e)))?;

        if affected == 0 {
            return Err(SenseArcError::TableNotFound(table_id));
        }

        debug!(table_id, "table dropped");
        Ok(())
    }

    /// 更新表元数据（ALTER TABLE）。
    ///
    /// 同时记录新的 schema_version。
    pub fn alter_table(
        &self,
        table_id: u32,
        new_columns: Vec<ColumnDef>,
        new_schema_version: u32,
        updated_at: TimestampNanos,
    ) -> Result<TableInfo, SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let tx = Self::begin_write_tx(&conn)
            .map_err(|e| SenseArcError::CatalogError(format!("begin tx error: {}", e)))?;

        let new_schema_id = compute_schema_id(&new_columns);

        // 更新 schema 版本
        tx.execute(
            "UPDATE tables SET schema_id = ?1, schema_version = ?2, updated_at = ?3 \
             WHERE table_id = ?4",
            params![
                new_schema_id,
                new_schema_version,
                updated_at.as_nanos(),
                table_id
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("alter_table error: {}", e)))?;

        // 删除旧列（audit trail 保存在 schema_versions 中）
        tx.execute("DELETE FROM columns WHERE table_id = ?1", params![table_id])
            .map_err(|e| {
                SenseArcError::CatalogError(format!("alter_table delete columns error: {}", e))
            })?;

        // 插入新列
        for col in &new_columns {
            Self::insert_column_in_tx(&tx, table_id, col)?;
        }

        // 记录 schema 版本变更
        tx.execute(
            "INSERT INTO schema_versions (table_id, schema_version, schema_id, operation, detail, created_at) \
             VALUES (?1, ?2, ?3, 'ALTER', ?4, ?5)",
            params![
                table_id,
                new_schema_version,
                new_schema_id,
                format!("altered table (v{})", new_schema_version),
                updated_at.as_nanos(),
            ],
        )
        .map_err(|e| {
            SenseArcError::CatalogError(format!("insert schema_version error: {}", e))
        })?;

        tx.commit()
            .map_err(|e| SenseArcError::CatalogError(format!("commit error: {}", e)))?;

        // 直接从已持有的 conn 读取表行（避免调用 self.get_table() 导致同一 Mutex 重入死锁）
        let table_row = conn
            .query_row(
                "SELECT table_id, table_name, schema_id, schema_version, tenant_id, table_group, \
                 table_type, durability, description, enabled, deleted_at, created_at, updated_at \
                 FROM tables WHERE table_id = ?1",
                params![table_id],
                read_table_row,
            )
            .map_err(|e| {
                SenseArcError::CatalogError(format!("alter_table read back error: {}", e))
            })?;

        Ok(row_to_table_info(table_row, new_columns))
    }

    // ─── Column CRUD ───────────────────────────────────────────────────────

    /// 添加新列到已有表。
    pub fn add_column(&self, table_id: u32, col: &ColumnDef) -> Result<(), SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        conn.execute(
            "INSERT INTO columns (column_id, table_id, name, data_type, precision, nullable, \
             unit, description, deadband_type, deadband_value, range_low, range_high, \
             instrument_range_low, instrument_range_high, display_digits, enum_values) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                col.column_id,
                table_id,
                col.name,
                data_type_to_str(col.data_type),
                col.precision,
                col.nullable as i32,
                col.unit,
                col.description,
                col.deadband.as_ref().map(|d| d.deadband_type.as_str()),
                col.deadband.as_ref().map(|d| d.value),
                col.range_low,
                col.range_high,
                col.instrument_range_low,
                col.instrument_range_high,
                col.display_digits,
                col.enum_values.as_ref().map(|v| v.join(",")),
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("add_column error: {}", e)))?;

        Ok(())
    }

    /// 逻辑删除列。
    pub fn drop_column(
        &self,
        table_id: u32,
        column_name: &str,
        deleted_at: TimestampNanos,
    ) -> Result<(), SenseArcError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| SenseArcError::CatalogError(format!("catalog lock error: {}", e)))?;

        let affected = conn
            .execute(
                "UPDATE columns SET deleted_at = ?1 WHERE table_id = ?2 AND name = ?3 AND deleted_at IS NULL",
                params![deleted_at.as_nanos(), table_id, column_name],
            )
            .map_err(|e| {
                SenseArcError::CatalogError(format!("drop_column error: {}", e))
            })?;

        if affected == 0 {
            return Err(SenseArcError::ColumnNotFound(0));
        }

        Ok(())
    }

    // ─── Helpers ───────────────────────────────────────────────────────────

    /// 在事务中插入一条列定义。
    fn insert_column_in_tx(
        tx: &Transaction,
        table_id: u32,
        col: &ColumnDef,
    ) -> Result<(), SenseArcError> {
        tx.execute(
            "INSERT INTO columns (column_id, table_id, name, data_type, precision, nullable, \
             unit, description, deadband_type, deadband_value, range_low, range_high, \
             instrument_range_low, instrument_range_high, display_digits, enum_values) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                col.column_id,
                table_id,
                col.name,
                data_type_to_str(col.data_type),
                col.precision,
                col.nullable as i32,
                col.unit,
                col.description,
                col.deadband.as_ref().map(|d| d.deadband_type.as_str()),
                col.deadband.as_ref().map(|d| d.value),
                col.range_low,
                col.range_high,
                col.instrument_range_low,
                col.instrument_range_high,
                col.display_digits,
                col.enum_values.as_ref().map(|v| v.join(",")),
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("insert column error: {}", e)))?;
        Ok(())
    }

    /// 读取一张表的所有未删除列。
    fn get_columns_inner(
        &self,
        conn: &Connection,
        table_id: u32,
    ) -> Result<Vec<ColumnDef>, SenseArcError> {
        let mut stmt = conn
            .prepare(
                "SELECT column_id, name, data_type, precision, nullable, unit, description, \
                 deadband_type, deadband_value, range_low, range_high, instrument_range_low, \
                 instrument_range_high, display_digits, enum_values \
                 FROM columns WHERE table_id = ?1 AND deleted_at IS NULL ORDER BY column_id",
            )
            .map_err(|e| SenseArcError::CatalogError(format!("get_columns error: {}", e)))?;

        let columns: Vec<ColumnDef> = stmt
            .query_map(params![table_id], |row| read_column_def(row, 0))
            .map_err(|e| SenseArcError::CatalogError(format!("get_columns error: {}", e)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SenseArcError::CatalogError(format!("get_columns error: {}", e)))?;

        Ok(columns)
    }

    /// 一次读取所有未删除列（跨全部表），返回 `(table_id, ColumnDef)`，
    /// 按 `(table_id, column_id)` 排序，供 [`list_tables`](Self::list_tables) /
    /// [`list_tables_for_tenant`](Self::list_tables_for_tenant) 内存分组以消除 N+1。
    ///
    /// 列序在 [`get_columns_inner`](Self::get_columns_inner) 基础上前置 `table_id`；
    /// 逐字段读取逻辑与 [`read_column_def`] 完全一致（deleted_at 过滤、column_id 排序、
    /// 列定义逐字段等价），保证批量加载与逐表加载语义零差异。
    fn get_all_columns_inner(
        &self,
        conn: &Connection,
    ) -> Result<Vec<(u32, ColumnDef)>, SenseArcError> {
        let mut stmt = conn
            .prepare(
                "SELECT table_id, column_id, name, data_type, precision, nullable, unit, \
                 description, deadband_type, deadband_value, range_low, range_high, \
                 instrument_range_low, instrument_range_high, display_digits, enum_values \
                 FROM columns WHERE deleted_at IS NULL ORDER BY table_id, column_id",
            )
            .map_err(|e| SenseArcError::CatalogError(format!("get_columns error: {}", e)))?;

        let columns: Vec<(u32, ColumnDef)> = stmt
            .query_map([], |row| Ok((row.get(0)?, read_column_def(row, 1)?)))
            .map_err(|e| SenseArcError::CatalogError(format!("get_columns error: {}", e)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SenseArcError::CatalogError(format!("get_columns error: {}", e)))?;

        Ok(columns)
    }
}

// ─── Internal row type ─────────────────────────────────────────────────────

struct TableRow {
    table_id: u32,
    table_name: String,
    schema_id: u32,
    schema_version: u32,
    tenant_id: TenantId,
    table_group: Option<String>,
    table_type: String,
    durability: Durability,
    description: Option<String>,
    enabled: bool,
    deleted_at: Option<i64>,
    created_at: i64,
    updated_at: i64,
}

/// 按统一列序读 TableRow：`table_id, table_name, schema_id, schema_version, tenant_id,
/// table_group, table_type, durability, description, enabled, deleted_at, created_at, updated_at`。
fn read_table_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TableRow> {
    Ok(TableRow {
        table_id: row.get(0)?,
        table_name: row.get(1)?,
        schema_id: row.get(2)?,
        schema_version: row.get(3)?,
        tenant_id: TenantId(row.get::<_, i64>(4)? as u32),
        table_group: row.get(5)?,
        table_type: row.get(6)?,
        durability: Durability::from_str(&row.get::<_, String>(7)?),
        description: row.get(8)?,
        enabled: row.get::<_, i32>(9)? != 0,
        deleted_at: row.get::<_, Option<i64>>(10)?,
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
    })
}

/// 从 `row` 的 `offset` 偏移处读取一条 `ColumnDef`（固定列序：
/// `column_id, name, data_type, precision, nullable, unit, description, deadband_type,
/// deadband_value, range_low, range_high, instrument_range_low, instrument_range_high,
/// display_digits, enum_values`）。
///
/// 供单表查询（`offset = 0`）与批量全列查询（`offset = 1`，前置 `table_id`）复用，
/// 保证两条路径的列定义逐字段一致。
fn read_column_def(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<ColumnDef> {
    let deadband_type_str: Option<String> = row.get(offset + 7)?;
    let deadband_value: Option<f64> = row.get(offset + 8)?;
    let deadband = match (deadband_type_str, deadband_value) {
        (Some(t), Some(v)) => Some(DeadbandConfig {
            deadband_type: DeadbandType::from_str(&t).unwrap_or(DeadbandType::None),
            value: v,
        }),
        _ => None,
    };

    let enum_values_str: Option<String> = row.get(offset + 14)?;
    let enum_values = enum_values_str.map(|s| {
        s.split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect::<Vec<_>>()
    });

    Ok(ColumnDef {
        column_id: row.get(offset)?,
        name: row.get(offset + 1)?,
        data_type: str_to_data_type(row.get::<_, String>(offset + 2)?.as_str()),
        precision: row.get(offset + 3)?,
        nullable: row.get::<_, i32>(offset + 4)? != 0,
        unit: row.get(offset + 5)?,
        description: row.get(offset + 6)?,
        deadband,
        range_low: row.get(offset + 9)?,
        range_high: row.get(offset + 10)?,
        instrument_range_low: row.get(offset + 11)?,
        instrument_range_high: row.get(offset + 12)?,
        display_digits: row.get::<_, i32>(offset + 13)? as u16,
        enum_values,
        deleted_at: None,
    })
}

fn row_to_table_info(row: TableRow, columns: Vec<ColumnDef>) -> TableInfo {
    TableInfo {
        table_id: row.table_id,
        tenant_id: row.tenant_id,
        table_name: row.table_name,
        schema_id: row.schema_id,
        schema_version: row.schema_version,
        table_group: row.table_group,
        table_type: TableType::from_str(&row.table_type).unwrap_or(TableType::Raw),
        durability: row.durability,
        description: row.description,
        enabled: row.enabled,
        columns,
        deleted_at: row.deleted_at.map(TimestampNanos),
        created_at: TimestampNanos(row.created_at),
        updated_at: TimestampNanos(row.updated_at),
    }
}

// ─── Type conversion helpers ───────────────────────────────────────────────

fn data_type_to_str(dt: DataType) -> &'static str {
    match dt {
        DataType::F64 => "F64",
        DataType::F32 => "F32",
        DataType::I64 => "I64",
        DataType::U64 => "U64",
        DataType::I32 => "I32",
        DataType::Bool => "BOOL",
        DataType::String => "STRING",
        DataType::Enum16 => "ENUM16",
    }
}

fn str_to_data_type(s: &str) -> DataType {
    match s.to_uppercase().as_str() {
        "F64" => DataType::F64,
        "F32" => DataType::F32,
        "I64" => DataType::I64,
        "U64" => DataType::U64,
        "I32" => DataType::I32,
        "BOOL" => DataType::Bool,
        "STRING" => DataType::String,
        "ENUM16" => DataType::Enum16,
        _ => DataType::F64, // 默认值，不应到达
    }
}

fn is_unique_constraint(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                ..
            },
            _,
        )
    )
}

/// 判定是否为「列已存在」类错误（`ALTER TABLE ... ADD COLUMN` 幂等容错用）。
fn is_duplicate_column(msg: &str) -> bool {
    msg.contains("duplicate column name") || msg.contains("already exists")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("test_config_{}.db", uuid::Uuid::new_v4()))
    }

    fn make_table(name: &str) -> TableInfo {
        TableInfo {
            table_id: 0, // auto
            tenant_id: TenantId::DEFAULT,
            table_name: name.to_string(),
            schema_id: 0, // computed
            schema_version: 1,
            table_group: None,
            table_type: TableType::Raw,
            durability: Durability::Group,
            description: None,
            enabled: true,
            columns: vec![ColumnDef {
                column_id: 0,
                name: "value".to_string(),
                data_type: DataType::F64,
                precision: 0.001,
                nullable: false,
                unit: Some("°C".to_string()),
                description: None,
                deadband: None,
                enum_values: None,
                deleted_at: None,
                range_low: 0.0,
                range_high: 100.0,
                instrument_range_low: None,
                instrument_range_high: None,
                display_digits: 2,
            }],
            deleted_at: None,
            created_at: TimestampNanos(1000),
            updated_at: TimestampNanos(1000),
        }
    }

    /// P0 回归：并发写锁竞争下建表不得微秒级失败（`database is locked`）。
    ///
    /// 复现跨连接写竞争（第二个连接持有写锁，模拟 `CqDefinitionStore`），
    /// 验证 `create_table` 使用 `BEGIN IMMEDIATE` 后经 busy handler 等待锁释放，
    /// 而非默认 `BEGIN DEFERRED` 下「先读后写」升级写锁时微秒级 `SQLITE_BUSY`。
    #[test]
    fn test_create_table_waits_for_write_lock_under_contention() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        // 独立连接（模拟 CqDefinitionStore）持有写锁。
        let blocker = rusqlite::Connection::open(&path).unwrap();
        blocker
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE;").unwrap();

        // 200ms 后释放锁；create_table 应等待锁而非立即失败。
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            blocker.execute_batch("COMMIT;").unwrap();
        });

        let t0 = std::time::Instant::now();
        let result = cat.create_table(&make_table("under_contention"));
        let elapsed = t0.elapsed();
        releaser.join().unwrap();

        assert!(result.is_ok(), "create_table failed: {:?}", result.err());
        assert!(
            elapsed >= std::time::Duration::from_millis(100),
            "expected busy_timeout wait under contention, got {elapsed:?}"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_create_and_get_table() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let info = make_table("test_table");
        let created = cat.create_table(&info).unwrap();
        assert_ne!(created.table_id, 0);
        assert_ne!(created.schema_id, 0);

        let fetched = cat.get_table(created.table_id).unwrap().unwrap();
        assert_eq!(fetched.table_name, "test_table");
        assert_eq!(fetched.columns.len(), 1);
        assert_eq!(fetched.columns[0].name, "value");
        assert_eq!(fetched.columns[0].unit.as_deref(), Some("°C"));

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    /// S4 Step 4：`tables.tenant_id` roundtrip + 分租户列表过滤。
    #[test]
    fn test_table_tenant_id_roundtrip_and_filter() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let mut t1 = make_table("tenant_1_table");
        t1.tenant_id = TenantId(1);
        let mut t2 = make_table("tenant_2_table");
        t2.tenant_id = TenantId(2);

        let c1 = cat.create_table(&t1).unwrap();
        let c2 = cat.create_table(&t2).unwrap();
        assert_eq!(c1.tenant_id, TenantId(1));
        assert_eq!(c2.tenant_id, TenantId(2));

        // 读回保留 tenant_id。
        assert_eq!(
            cat.get_table(c1.table_id).unwrap().unwrap().tenant_id,
            TenantId(1)
        );

        // 分租户过滤：tenant 1 列表不含 tenant 2。
        let l1 = cat.list_tables_for_tenant(TenantId(1)).unwrap();
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].table_id, c1.table_id);
        let l2 = cat.list_tables_for_tenant(TenantId(2)).unwrap();
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].table_id, c2.table_id);

        // 平台视角（list_tables）含两者。
        assert_eq!(cat.list_tables().unwrap().len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_duplicate_table_name() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let info = make_table("dup_table");
        cat.create_table(&info).unwrap();

        let result = cat.create_table(&info);
        match result {
            Err(SenseArcError::TableAlreadyExists(name)) => {
                assert_eq!(name, "dup_table");
            }
            other => panic!("expected TableAlreadyExists, got {:?}", other),
        }

        let _ = std::fs::remove_file(&path);
    }

    /// 多租户：跨租户同名表各自成功创建，按 (tenant, name) 各查各的，互不影响。
    #[test]
    fn test_cross_tenant_same_name() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let mut a = make_table("pump1");
        a.tenant_id = TenantId(1);
        let mut b = make_table("pump1");
        b.tenant_id = TenantId(2);

        let ca = cat.create_table(&a).unwrap();
        let cb = cat.create_table(&b).unwrap();
        assert_ne!(ca.table_id, cb.table_id);

        // 按租户过滤各自命中，不串。
        let got_a = cat
            .get_table_by_name(TenantId(1), "pump1")
            .unwrap()
            .unwrap();
        let got_b = cat
            .get_table_by_name(TenantId(2), "pump1")
            .unwrap()
            .unwrap();
        assert_eq!(got_a.table_id, ca.table_id);
        assert_eq!(got_b.table_id, cb.table_id);
        assert_eq!(got_a.tenant_id, TenantId(1));
        assert_eq!(got_b.tenant_id, TenantId(2));

        // 无此租户的 name 返回 None。
        assert!(cat
            .get_table_by_name(TenantId(3), "pump1")
            .unwrap()
            .is_none());

        let _ = std::fs::remove_file(&path);
    }

    /// 多租户：同租户重名仍报 TableAlreadyExists（消息字面不变）。
    #[test]
    fn test_same_tenant_duplicate_name() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let mut a = make_table("dup_tenant");
        a.tenant_id = TenantId(7);
        cat.create_table(&a).unwrap();

        let result = cat.create_table(&a);
        match result {
            Err(SenseArcError::TableAlreadyExists(name)) => assert_eq!(name, "dup_tenant"),
            other => panic!("expected TableAlreadyExists, got {:?}", other),
        }

        let _ = std::fs::remove_file(&path);
    }

    /// 多租户：逻辑删除后同租户可同名重建（partial 索引不占名额）。
    #[test]
    fn test_recreate_after_drop_same_tenant() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let mut a = make_table("pump1");
        a.tenant_id = TenantId(1);
        let created = cat.create_table(&a).unwrap();
        cat.drop_table(created.table_id, TimestampNanos(9999))
            .unwrap();

        // 逻辑删除后，同租户同名可重建。
        let recreated = cat.create_table(&a).unwrap();
        assert_ne!(recreated.table_id, created.table_id);

        let _ = std::fs::remove_file(&path);
    }

    /// 回归（P3 潜伏缺陷修复）：逻辑删除后同名重建，`get_table_by_name` 必须返回
    /// active 行（新 id），而非旧的 deleted 行。
    ///
    /// 同 `(tenant_id, table_name)` 会同时存在 deleted 行 + active 行；`query_row`
    /// 无 `ORDER BY` 时取首行（先插入的旧 deleted 行），故需 SQL 显式过滤
    /// `deleted_at IS NULL`，与 `list_tables` / `table_name_exists` 口径一致。
    #[test]
    fn test_get_table_by_name_returns_active_after_recreate() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let mut a = make_table("recreate_probe");
        a.tenant_id = TenantId(1);

        let created = cat.create_table(&a).unwrap();
        cat.drop_table(created.table_id, TimestampNanos(9999))
            .unwrap();

        // 同名重建（partial 唯一索引允许逻辑删除后重建）。
        let recreated = cat.create_table(&a).unwrap();
        assert_ne!(recreated.table_id, created.table_id);

        // 按名查表应返回 active 的重建行，而非旧的 deleted 行。
        let got = cat
            .get_table_by_name(TenantId(1), "recreate_probe")
            .unwrap()
            .unwrap();
        assert_eq!(got.table_id, recreated.table_id);
        assert!(got.deleted_at.is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_list_tables() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        cat.create_table(&make_table("t1")).unwrap();
        cat.create_table(&make_table("t2")).unwrap();

        let tables = cat.list_tables().unwrap();
        assert_eq!(tables.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_drop_table() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let created = cat.create_table(&make_table("to_drop")).unwrap();
        cat.drop_table(created.table_id, TimestampNanos(9999))
            .unwrap();

        // 被删除的表不出现在 list 中
        let tables = cat.list_tables().unwrap();
        assert!(tables.is_empty());

        // 但按 id 仍可查询
        let fetched = cat.get_table(created.table_id).unwrap().unwrap();
        assert!(fetched.deleted_at.is_some());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_add_column() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let created = cat.create_table(&make_table("t1")).unwrap();
        let new_col = ColumnDef {
            column_id: 1,
            name: "quality".to_string(),
            data_type: DataType::U64,
            precision: 0.0,
            nullable: true,
            unit: None,
            description: Some("quality code".into()),
            deadband: Some(DeadbandConfig {
                deadband_type: DeadbandType::Absolute,
                value: 1.0,
            }),
            enum_values: None,
            deleted_at: None,
            range_low: 0.0,
            range_high: 255.0,
            instrument_range_low: None,
            instrument_range_high: None,
            display_digits: 0,
        };

        cat.add_column(created.table_id, &new_col).unwrap();

        let fetched = cat.get_table(created.table_id).unwrap().unwrap();
        assert_eq!(fetched.columns.len(), 2);
        assert_eq!(fetched.columns[1].name, "quality");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_ddl_idempotency_three_state() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        let table_id = 42u32;
        let hash: &[u8] = b"deadbeef-deadbeef-deadbeef-00";

        // 1. 初始：未开始
        assert_eq!(
            cat.ddl_apply_state(table_id, hash).unwrap(),
            DdlApplyState::NotStarted
        );

        // 2. 开始：pending
        cat.ddl_begin(table_id, hash, 1000).unwrap();
        assert!(cat.ddl_is_pending(table_id, hash).unwrap());
        assert!(!cat.ddl_is_applied(table_id, hash).unwrap());
        assert_eq!(
            cat.ddl_apply_state(table_id, hash).unwrap(),
            DdlApplyState::PendingResume
        );

        // 3. 完成：pending 删除 + applied 写入
        cat.ddl_complete(table_id, hash, 1001).unwrap();
        assert!(!cat.ddl_is_pending(table_id, hash).unwrap());
        assert!(cat.ddl_is_applied(table_id, hash).unwrap());
        assert_eq!(
            cat.ddl_apply_state(table_id, hash).unwrap(),
            DdlApplyState::AlreadyApplied
        );

        // 4. 重复 apply：幂等跳过（状态不变）
        assert!(cat.ddl_is_applied(table_id, hash).unwrap());

        let _ = std::fs::remove_file(&path);
    }
}

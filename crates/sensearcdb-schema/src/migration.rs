//! Migration: 全局 DDL 版本管理。
//!
//! 使用 `schema_meta` 表中的 `schema_version` 键跟踪全局 schema 版本。
//! 启动时自动执行所有待应用的迁移 SQL，确保 config.db 表结构与代码一致。
//!
//! ## 与 `ensure_schema` 的分工（单一事实源）
//!
//! - `ensure_schema` 管「幂等初始/加表」（`CREATE TABLE IF NOT EXISTS`）。
//! - `MIGRATIONS` 管「增量 DDL」（加列/改约束等 `IF NOT EXISTS` 覆盖不到的变化）。
//! - 判定规则：加表走幂等建表不 bump；给已存在的表加列/改约束/回填才 bump + 追加迁移。

use crate::catalog::Catalog;
use sensearcdb_common::SenseArcError;
use tracing::{info, warn};

/// 当前代码期望的全局 schema 版本。
///
/// 当此值 > config.db 中已记录的版本时，将依次执行 `MIGRATIONS` 中
/// `target_version > current_version` 的所有迁移。
const CURRENT_SCHEMA_VERSION: u32 = 3;

/// 单条迁移：`target_version` + 要执行的 SQL。
///
/// `target_version` 必须严格单调递增（`2, 3, ...`），
/// 迁移 SQL 应为增量 DDL（加列/改约束等），并容错「已存在」错误以幂等收敛。
struct Migration {
    target_version: u32,
    description: &'static str,
    sql: &'static str,
}

/// 迁移列表——按 `target_version` 升序排列。
///
/// v1 隐含为「ensure_schema 已建好全部初始表」，故不在此处重复双写；
/// 迁移从 v2 开始，只放增量 DDL。
static MIGRATIONS: &[Migration] = &[
    // v2: 给 cq_definitions 补 `source_types` 列（B5 Step3 的「正式版本化迁移」）。
    // 容错 duplicate column name：全新库（ensure_schema 已含该列）与 B5 后库
    // （store 手写 ALTER 已补列）启动时 ALTER 会因列已存在而失败，视同已应用。
    Migration {
        target_version: 2,
        description: "add cq_definitions.source_types column (TEXT, default '[]')",
        sql: "ALTER TABLE cq_definitions ADD COLUMN source_types TEXT NOT NULL DEFAULT '[]';",
    },
    // v3: 给 tables 补 `durability` 列（表级 WAL 持久化强度，默认 'group'）。
    // 容错 duplicate column name：全新库（ensure_schema 已含该列）与「store 手改已补列」
    // 启动时 ALTER 会因列已存在而失败，视同已应用（同 v2 source_types 先例）。
    Migration {
        target_version: 3,
        description: "add tables.durability column (TEXT, default 'group')",
        sql: "ALTER TABLE tables ADD COLUMN durability TEXT NOT NULL DEFAULT 'group';",
    },
];

/// 运行所有待应用的迁移。
///
/// 从 `schema_meta` 读取当前版本，依次执行所有 `target_version > current_version`
/// 的迁移，每条迁移在单个事务内「执行 SQL + 写 schema_version」原子提交。
///
/// 迁移只前进不后退：DB 版本 > 代码版本时只 warn，不降级、不执行。
///
/// # Arguments
/// - `catalog`: 已打开的 Catalog 实例（内部持有 Mutex<Connection>）。
pub fn run_migrations(catalog: &Catalog) -> Result<(), SenseArcError> {
    run_migrations_counted(catalog).map(|_| ())
}

/// `run_migrations` 的内部实现，返回实际应用的迁移条数（便于测试断言）。
fn run_migrations_counted(catalog: &Catalog) -> Result<u32, SenseArcError> {
    info!(
        current_code_version = CURRENT_SCHEMA_VERSION,
        "running schema migrations"
    );

    let current = catalog.get_schema_version()?.unwrap_or(0);

    // 只前进不后退：DB 版本 > 代码版本说明代码被回退，只 warn，不执行任何迁移、不降级。
    if current > CURRENT_SCHEMA_VERSION {
        warn!(
            db_version = current,
            code_version = CURRENT_SCHEMA_VERSION,
            "config.db schema version is newer than code; skipping migrations (forward-only)"
        );
        return Ok(0);
    }

    let mut applied_count = 0u32;
    for m in MIGRATIONS.iter() {
        if m.target_version > current {
            catalog.apply_migration(m.target_version, m.sql)?;
            applied_count += 1;
            info!(
                target = m.target_version,
                description = m.description,
                "applied schema migration"
            );
        }
    }

    info!(
        applied = applied_count,
        target = CURRENT_SCHEMA_VERSION,
        "migration complete"
    );

    Ok(applied_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;

    fn temp_db_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("test_migration_{}.db", uuid::Uuid::new_v4()))
    }

    /// 通过只读连接判断某表某列是否存在（PRAGMA table_info）。
    fn column_exists(path: &std::path::Path, table: &str, column: &str) -> bool {
        let conn = rusqlite::Connection::open(path).unwrap();
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        names.iter().any(|name| name == column)
    }

    /// 旧 DDL：不含 `source_types` 列（B4 前老库形态）。
    const LEGACY_CQ_DEFINITIONS_DDL: &str = "
        CREATE TABLE cq_definitions (
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
        );
    ";

    /// 新 DDL：含 `source_types` 列（B5 后库形态）。
    const WITH_SOURCE_TYPES_DDL: &str = "
        CREATE TABLE cq_definitions (
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

    /// 旧 `tables` DDL：不含 `durability` 列（v3 前老库形态）。
    const LEGACY_TABLES_DDL: &str = "
        CREATE TABLE tables (
            table_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name     TEXT    NOT NULL,
            schema_id      INTEGER NOT NULL,
            schema_version INTEGER NOT NULL DEFAULT 1,
            tenant_id      INTEGER NOT NULL DEFAULT 0,
            table_group    TEXT,
            table_type     TEXT    NOT NULL DEFAULT 'RAW',
            description    TEXT,
            enabled        INTEGER NOT NULL DEFAULT 1,
            deleted_at     INTEGER,
            created_at     INTEGER NOT NULL,
            updated_at     INTEGER NOT NULL
        );
    ";

    #[test]
    fn test_migrations_array_not_empty() {
        assert!(!MIGRATIONS.is_empty());
    }

    #[test]
    fn test_migration_versions_monotonic() {
        let mut last = 0u32;
        for m in MIGRATIONS.iter() {
            assert!(
                m.target_version > last,
                "migration target_version must be monotonic: {} <= {}",
                m.target_version,
                last
            );
            last = m.target_version;
        }
    }

    /// 全新库：ensure_schema 已建含 source_types 的 cq_definitions 与含 durability 的 tables，
    /// v2/v3 ALTER 命中 duplicate 容错跳过，版本仍推进到 3，applied_count=2。
    #[test]
    fn test_run_migrations_on_fresh_db() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        // 全新库 ensure_schema 已含 source_types / durability。
        assert!(column_exists(&path, "cq_definitions", "source_types"));
        assert!(column_exists(&path, "tables", "durability"));
        // 初始无版本记录。
        assert_eq!(cat.get_schema_version().unwrap(), None);

        let applied = run_migrations_counted(&cat).unwrap();
        assert_eq!(applied, 2);
        assert_eq!(cat.get_schema_version().unwrap(), Some(3));

        // 二次运行：无待应用迁移，applied_count=0。
        let applied_again = run_migrations_counted(&cat).unwrap();
        assert_eq!(applied_again, 0);
        assert_eq!(cat.get_schema_version().unwrap(), Some(3));

        let _ = std::fs::remove_file(&path);
    }

    /// 老库（B4 前，无 source_types 列）：v2 真补 source_types，v3 duplicate 容错，版本推进到 3。
    #[test]
    fn test_run_migrations_legacy_db_adds_source_types() {
        let path = temp_db_path();
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(LEGACY_CQ_DEFINITIONS_DDL).unwrap();
        }

        let cat = Catalog::open(&path).unwrap();
        assert!(!column_exists(&path, "cq_definitions", "source_types"));
        assert_eq!(cat.get_schema_version().unwrap(), None);

        let applied = run_migrations_counted(&cat).unwrap();
        assert_eq!(applied, 2);
        assert!(column_exists(&path, "cq_definitions", "source_types"));
        // tables 由 ensure_schema 新建（已含 durability），v3 duplicate 容错。
        assert!(column_exists(&path, "tables", "durability"));
        assert_eq!(cat.get_schema_version().unwrap(), Some(3));

        let _ = std::fs::remove_file(&path);
    }

    /// B5 后库（已含 source_types 列，但无版本记录）：v2/v3 duplicate 容错，不报错。
    #[test]
    fn test_run_migrations_b5_after_db_duplicate_tolerated() {
        let path = temp_db_path();
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(WITH_SOURCE_TYPES_DDL).unwrap();
        }

        let cat = Catalog::open(&path).unwrap();
        assert!(column_exists(&path, "cq_definitions", "source_types"));
        assert_eq!(cat.get_schema_version().unwrap(), None);

        let result = run_migrations(&cat);
        assert!(
            result.is_ok(),
            "duplicate column must be tolerated: {:?}",
            result
        );
        assert_eq!(cat.get_schema_version().unwrap(), Some(3));

        let _ = std::fs::remove_file(&path);
    }

    /// 老库（tables 无 durability 列）：v3 真补列，版本推进到 3。
    ///
    /// 这是 durability 迁移三形态中的「老库需真补列」形态：仅建旧 tables 表，
    /// ensure_schema 对 `CREATE TABLE IF NOT EXISTS tables` 跳过，v3 ALTER 真加列。
    #[test]
    fn test_run_migrations_legacy_tables_adds_durability() {
        let path = temp_db_path();
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(LEGACY_TABLES_DDL).unwrap();
        }

        let cat = Catalog::open(&path).unwrap();
        assert!(!column_exists(&path, "tables", "durability"));
        assert_eq!(cat.get_schema_version().unwrap(), None);

        let applied = run_migrations_counted(&cat).unwrap();
        assert_eq!(applied, 2);
        assert!(column_exists(&path, "tables", "durability"));
        assert_eq!(cat.get_schema_version().unwrap(), Some(3));

        let _ = std::fs::remove_file(&path);
    }

    /// 版本号读写：get 初始 None，apply_migration 后写入并读回。
    #[test]
    fn test_schema_version_read_write() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        assert_eq!(cat.get_schema_version().unwrap(), None);

        // 用一条空 DDL 直接验证版本号写读原语（与迁移 SQL 解耦）。
        cat.apply_migration(2, "SELECT 1;").unwrap();
        assert_eq!(cat.get_schema_version().unwrap(), Some(2));

        // 覆盖写：INSERT OR REPLACE。
        cat.apply_migration(3, "SELECT 1;").unwrap();
        assert_eq!(cat.get_schema_version().unwrap(), Some(3));

        let _ = std::fs::remove_file(&path);
    }

    /// 只前进不后退：DB 版本 > 代码版本时，不执行、不降级、只 warn。
    #[test]
    fn test_run_migrations_forward_only_no_downgrade() {
        let path = temp_db_path();
        let cat = Catalog::open(&path).unwrap();

        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('schema_version', '5')",
                [],
            )
            .unwrap();
        }

        let applied = run_migrations_counted(&cat).unwrap();
        assert_eq!(applied, 0);
        assert_eq!(cat.get_schema_version().unwrap(), Some(5));

        let _ = std::fs::remove_file(&path);
    }
}

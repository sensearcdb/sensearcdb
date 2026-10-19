//! systemConfig 持久化：config.db `system_config` 表（key TEXT PK + value TEXT）。
//!
//! 复用 `CqDefinitionStore` 范式：WAL + 幂等 DDL + `Mutex<Connection>` + 独立 open。
//! 只存「白名单字段」的覆盖值；启动时加载并覆盖 `AppConfig` 默认值（见 startup.rs）。

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};

use crate::error::SenseArcError;

/// `system_config` 建表 DDL（幂等）。
const SYSTEM_CONFIG_DDL: &str = "
CREATE TABLE IF NOT EXISTS system_config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";

/// systemConfig 持久化存储。
///
/// 内部使用 `Mutex<Connection>` 保证线程安全；连接启用 WAL 模式。
pub struct SystemConfigStore {
    conn: Mutex<Connection>,
}

impl SystemConfigStore {
    /// 打开（或创建）config.db 中的 `system_config` 表。
    pub fn open(db_path: &Path) -> Result<Self, SenseArcError> {
        let conn = Connection::open(db_path).map_err(|e| {
            SenseArcError::CatalogError(format!("failed to open config.db for system_config: {e}"))
        })?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| {
                SenseArcError::CatalogError(format!(
                    "failed to set WAL mode for system_config: {e}"
                ))
            })?;

        let store = Self {
            conn: Mutex::new(conn),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    fn ensure_schema(&self) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("system_config store lock error: {e}"))
        })?;
        conn.execute_batch(SYSTEM_CONFIG_DDL).map_err(|e| {
            SenseArcError::CatalogError(format!("failed to ensure system_config schema: {e}"))
        })?;
        Ok(())
    }

    /// upsert 一个 key/value 覆盖值。
    pub fn upsert(&self, key: &str, value: &str) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("system_config store lock error: {e}"))
        })?;
        conn.execute(
            "INSERT INTO system_config (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("upsert system_config error: {e}")))?;
        Ok(())
    }

    /// 加载全部覆盖值为 `key → value`（启动恢复）。
    pub fn load_all(&self) -> Result<HashMap<String, String>, SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("system_config store lock error: {e}"))
        })?;
        let mut stmt = conn
            .prepare("SELECT key, value FROM system_config")
            .map_err(|e| {
                SenseArcError::CatalogError(format!("system_config load_all prepare error: {e}"))
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| {
                SenseArcError::CatalogError(format!("system_config load_all query error: {e}"))
            })?;

        let mut out = HashMap::new();
        for row in rows {
            let (key, value) = row.map_err(|e| {
                SenseArcError::CatalogError(format!("system_config load_all row error: {e}"))
            })?;
            out.insert(key, value);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("test_syscfg_{}_{}.db", std::process::id(), nanos))
    }

    #[test]
    fn test_upsert_and_load_roundtrip() {
        let path = temp_db();
        {
            let store = SystemConfigStore::open(&path).unwrap();
            store
                .upsert("alert.evaluation_interval_secs", "77")
                .unwrap();
            store.upsert("stream.max_concurrency", "8").unwrap();
        }
        {
            let store = SystemConfigStore::open(&path).unwrap();
            let loaded = store.load_all().unwrap();
            assert_eq!(
                loaded
                    .get("alert.evaluation_interval_secs")
                    .map(String::as_str),
                Some("77")
            );
            assert_eq!(
                loaded.get("stream.max_concurrency").map(String::as_str),
                Some("8")
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_upsert_overwrites_existing_key() {
        let path = temp_db();
        let store = SystemConfigStore::open(&path).unwrap();
        store.upsert("cq.tick_interval_ms", "1000").unwrap();
        store.upsert("cq.tick_interval_ms", "2000").unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(
            loaded.get("cq.tick_interval_ms").map(String::as_str),
            Some("2000")
        );
        let _ = std::fs::remove_file(&path);
    }
}

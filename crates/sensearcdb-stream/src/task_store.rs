//! Stream 任务定义持久化存储。
//!
//! `StreamTaskStore` 将 `StreamTask` 落盘到 config.db 的 `stream_tasks` 表，
//! 使任务定义与运行状态（`status`）在进程重启后可从 SQLite 恢复。
//!
//! ## 与 CQ 持久化共享模式（B4 D4）
//!
//! - 同库：同一个 `config.db`（`cfg.paths.config_db_path`）。
//! - 同连接模式：WAL。
//! - 分表：`stream_tasks` 与 `cq_definitions` 各自独立表，互不侵入。
//!
//! ## 水位语义（拍板点③）
//!
//! Stream 是事件驱动增量聚合，重启后未 Final 窗口的中间态丢失语义上可接受，
//! 因此**只持久化任务定义 + status，不落窗口水位**。
//!
//! 若后续出现第三个 config.db store，再抽 `ConfigDbStore` 基类
//! （open + WAL + ensure_schema 模板），当前与 `CqDefinitionStore` 保持各自独立。

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};
use sensearcdb_common::{SenseArcError, TenantId};

use crate::task::{StreamTask, TaskId, TaskStatus};

/// `stream_tasks` 建表 DDL（幂等）。
const STREAM_TASKS_DDL: &str = "
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
";

/// Stream 任务定义持久化存储。
///
/// 内部使用 `Mutex<Connection>` 保证线程安全；连接启用 WAL 模式。
pub struct StreamTaskStore {
    conn: Mutex<Connection>,
}

impl StreamTaskStore {
    /// 打开（或创建）config.db 中的 `stream_tasks` 表。
    ///
    /// # Arguments
    /// - `db_path`: config.db 文件路径。
    pub fn open(db_path: &Path) -> Result<Self, SenseArcError> {
        let conn = Connection::open(db_path).map_err(|e| {
            SenseArcError::CatalogError(format!("failed to open config.db for stream tasks: {e}"))
        })?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| {
                SenseArcError::CatalogError(format!("failed to set WAL mode for stream tasks: {e}"))
            })?;

        let store = Self {
            conn: Mutex::new(conn),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    /// 确保 `stream_tasks` 表存在（幂等）。
    fn ensure_schema(&self) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("stream task store lock error: {e}"))
        })?;
        conn.execute_batch(STREAM_TASKS_DDL).map_err(|e| {
            SenseArcError::CatalogError(format!("failed to ensure stream_tasks schema: {e}"))
        })?;
        Ok(())
    }

    /// upsert 一条 Stream 任务定义（幂等，以 `id` 为主键）。
    pub fn upsert(&self, task: &StreamTask) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("stream task store lock error: {e}"))
        })?;

        let window_spec_json = serde_json::to_string(&task.window_spec)
            .map_err(|e| SenseArcError::CatalogError(format!("serialize window_spec: {e}")))?;
        let projection_json = serde_json::to_string(&task.projection)
            .map_err(|e| SenseArcError::CatalogError(format!("serialize projection: {e}")))?;
        let partition_by_json = serde_json::to_string(&task.partition_by)
            .map_err(|e| SenseArcError::CatalogError(format!("serialize partition_by: {e}")))?;
        let status_json = serde_json::to_string(&task.status)
            .map_err(|e| SenseArcError::CatalogError(format!("serialize status: {e}")))?;
        let now = now_millis();

        conn.execute(
            "INSERT INTO stream_tasks (
                id, name, tenant_id, source_table_id, target_table_id,
                window_spec_json, projection_json, partition_by_json,
                status, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                tenant_id = excluded.tenant_id,
                source_table_id = excluded.source_table_id,
                target_table_id = excluded.target_table_id,
                window_spec_json = excluded.window_spec_json,
                projection_json = excluded.projection_json,
                partition_by_json = excluded.partition_by_json,
                status = excluded.status,
                updated_at = excluded.updated_at",
            params![
                task.id.to_string(),
                task.name,
                task.tenant_id.as_u32(),
                task.source_table_id,
                task.target_table_id,
                window_spec_json,
                projection_json,
                partition_by_json,
                status_json,
                task.created_at,
                now,
            ],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("upsert stream_task error: {e}")))?;

        Ok(())
    }

    /// 删除指定任务（`drop_task` 时调用）。
    pub fn delete(&self, id: TaskId) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("stream task store lock error: {e}"))
        })?;
        conn.execute(
            "DELETE FROM stream_tasks WHERE id = ?1",
            params![id.to_string()],
        )
        .map_err(|e| SenseArcError::CatalogError(format!("delete stream_task error: {e}")))?;
        Ok(())
    }

    /// 更新任务状态（`pause` / `resume` / `Error` 时调用，不触碰窗口水位）。
    pub fn update_status(&self, id: TaskId, status: &TaskStatus) -> Result<(), SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("stream task store lock error: {e}"))
        })?;
        let status_json = serde_json::to_string(status)
            .map_err(|e| SenseArcError::CatalogError(format!("serialize status: {e}")))?;
        let now = now_millis();
        conn.execute(
            "UPDATE stream_tasks SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status_json, now, id.to_string()],
        )
        .map_err(|e| {
            SenseArcError::CatalogError(format!("update stream_task status error: {e}"))
        })?;
        Ok(())
    }

    /// 加载全部 Stream 任务定义（启动恢复）。
    pub fn load_all(&self) -> Result<Vec<StreamTask>, SenseArcError> {
        let conn = self.conn.lock().map_err(|e| {
            SenseArcError::CatalogError(format!("stream task store lock error: {e}"))
        })?;

        let mut stmt = conn
            .prepare(
                "SELECT id, name, tenant_id, source_table_id, target_table_id,
                        window_spec_json, projection_json, partition_by_json,
                        status, created_at
                 FROM stream_tasks ORDER BY created_at, id",
            )
            .map_err(|e| {
                SenseArcError::CatalogError(format!("stream_task load_all prepare error: {e}"))
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(StreamTaskRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    tenant_id: row.get(2)?,
                    source_table_id: row.get(3)?,
                    target_table_id: row.get(4)?,
                    window_spec_json: row.get(5)?,
                    projection_json: row.get(6)?,
                    partition_by_json: row.get(7)?,
                    status_json: row.get(8)?,
                    created_at: row.get(9)?,
                })
            })
            .map_err(|e| {
                SenseArcError::CatalogError(format!("stream_task load_all query error: {e}"))
            })?;

        let mut out = Vec::new();
        for row in rows {
            let row = row.map_err(|e| {
                SenseArcError::CatalogError(format!("stream_task load_all row error: {e}"))
            })?;
            out.push(row.to_task()?);
        }
        Ok(out)
    }
}

/// 内部行类型（反序列化前）。
struct StreamTaskRow {
    id: String,
    name: String,
    tenant_id: u32,
    source_table_id: u32,
    target_table_id: u32,
    window_spec_json: String,
    projection_json: String,
    partition_by_json: String,
    status_json: String,
    created_at: i64,
}

impl StreamTaskRow {
    fn to_task(self) -> Result<StreamTask, SenseArcError> {
        let id = uuid::Uuid::parse_str(&self.id).map_err(|e| {
            SenseArcError::CatalogError(format!(
                "invalid stream task id in config.db: {} ({e})",
                self.id
            ))
        })?;
        let window_spec = serde_json::from_str(&self.window_spec_json)
            .map_err(|e| SenseArcError::CatalogError(format!("deserialize window_spec: {e}")))?;
        let projection = serde_json::from_str(&self.projection_json)
            .map_err(|e| SenseArcError::CatalogError(format!("deserialize projection: {e}")))?;
        let partition_by = serde_json::from_str(&self.partition_by_json)
            .map_err(|e| SenseArcError::CatalogError(format!("deserialize partition_by: {e}")))?;
        let status = serde_json::from_str(&self.status_json)
            .map_err(|e| SenseArcError::CatalogError(format!("deserialize status: {e}")))?;

        Ok(StreamTask {
            id,
            name: self.name,
            tenant_id: TenantId(self.tenant_id),
            source_table_id: self.source_table_id,
            target_table_id: self.target_table_id,
            window_spec,
            projection,
            partition_by,
            status,
            created_at: self.created_at,
        })
    }
}

/// 当前 epoch 毫秒时间戳（内部辅助，与 `StreamTask.created_at` 单位一致）。
fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{ProjectionColumn, ProjectionSpec, TargetColumnDef, WindowSpec};
    use sensearcdb_common::{AggFunction, DataType};

    fn temp_db() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("test_stream_tasks_{}.db", uuid::Uuid::new_v4()))
    }

    fn sample_task(name: &str, source: u32, target: u32) -> StreamTask {
        StreamTask {
            id: uuid::Uuid::new_v4(),
            name: name.to_string(),
            tenant_id: TenantId::DEFAULT,
            source_table_id: source,
            target_table_id: target,
            window_spec: WindowSpec::Tumbling {
                interval_ms: 10_000,
            },
            projection: ProjectionSpec {
                columns: vec![
                    ProjectionColumn::Aggregate {
                        function: AggFunction::Avg,
                        source_column: 1,
                        alias: "avg_val".into(),
                    },
                    ProjectionColumn::Aggregate {
                        function: AggFunction::Sum,
                        source_column: 2,
                        alias: "sum_val".into(),
                    },
                ],
                target_schema: vec![
                    TargetColumnDef {
                        column_id: 1,
                        column_name: "avg_val".into(),
                        data_type: DataType::F64,
                    },
                    TargetColumnDef {
                        column_id: 2,
                        column_name: "sum_val".into(),
                        data_type: DataType::F64,
                    },
                ],
            },
            partition_by: None,
            status: TaskStatus::Running,
            created_at: 1000,
        }
    }

    #[test]
    fn test_upsert_and_load_roundtrip() {
        let path = temp_db();
        let store = StreamTaskStore::open(&path).unwrap();

        let task = sample_task("my_stream", 42, 100);
        store.upsert(&task).unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        let d = &loaded[0];
        assert_eq!(d.id, task.id);
        assert_eq!(d.name, "my_stream");
        assert_eq!(d.source_table_id, 42);
        assert_eq!(d.target_table_id, 100);
        assert!(matches!(
            d.window_spec,
            WindowSpec::Tumbling {
                interval_ms: 10_000
            }
        ));
        assert_eq!(d.projection.columns.len(), 2);
        assert_eq!(d.status, TaskStatus::Running);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_upsert_idempotent() {
        let path = temp_db();
        let store = StreamTaskStore::open(&path).unwrap();
        let task = sample_task("dup_stream", 7, 200);
        store.upsert(&task).unwrap();
        store.upsert(&task).unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_delete() {
        let path = temp_db();
        let store = StreamTaskStore::open(&path).unwrap();
        let task = sample_task("to_delete", 1, 11);
        store.upsert(&task).unwrap();
        store.delete(task.id).unwrap();
        assert!(store.load_all().unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_update_status() {
        let path = temp_db();
        let store = StreamTaskStore::open(&path).unwrap();
        let task = sample_task("status_task", 9, 90);
        store.upsert(&task).unwrap();

        let err = TaskStatus::Error {
            reason: "source table dropped".into(),
        };
        store.update_status(task.id, &err).unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded[0].status, err);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_restart_recovery() {
        let path = temp_db();
        {
            let store = StreamTaskStore::open(&path).unwrap();
            store.upsert(&sample_task("s1", 1, 101)).unwrap();
            store.upsert(&sample_task("s2", 2, 102)).unwrap();
        }
        // 模拟重启：重新 open + load
        {
            let store = StreamTaskStore::open(&path).unwrap();
            let loaded = store.load_all().unwrap();
            assert_eq!(loaded.len(), 2);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_cross_tenant_same_name_roundtrip() {
        let path = temp_db();
        let store = StreamTaskStore::open(&path).unwrap();

        let mut a = sample_task("dup", 1, 101);
        a.tenant_id = TenantId(1);
        let mut b = sample_task("dup", 2, 102);
        b.tenant_id = TenantId(2);

        store.upsert(&a).unwrap();
        store.upsert(&b).unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 2);
        let tenants: std::collections::HashSet<u32> =
            loaded.iter().map(|t| t.tenant_id.as_u32()).collect();
        assert_eq!(tenants, [1u32, 2u32].into_iter().collect());

        // 同租户重名违反复合唯一索引 → 报错。
        let mut dup = sample_task("dup", 3, 103);
        dup.tenant_id = TenantId(1);
        assert!(store.upsert(&dup).is_err());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_partition_by_roundtrip() {
        use crate::task::PartitionBySpec;
        let path = temp_db();
        let store = StreamTaskStore::open(&path).unwrap();
        let mut task = sample_task("partitioned", 3, 103);
        task.partition_by = Some(PartitionBySpec {
            source_column: 0,
            source_column_name: "device_id".into(),
        });
        store.upsert(&task).unwrap();
        let loaded = store.load_all().unwrap();
        let p = loaded[0].partition_by.as_ref().unwrap();
        assert_eq!(p.source_column, 0);
        assert_eq!(p.source_column_name, "device_id");
        let _ = std::fs::remove_file(&path);
    }
}

//! 流计算任务定义与注册表。
//!
//! 包含：
//! - `StreamTask`: 单个流计算任务的定义
//! - `TaskRegistry`: 任务注册表，支持按 ID/名称/源表索引
//! - 窗口规格、投影规格、分区规格等核心类型
//!
//! 任务通过 config.db 的 `stream_tasks` 表持久化。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use sensearcdb_common::{AggFunction, DataType, TenantId};

// ─── 类型别名 ───

/// 任务唯一标识符（UUID v4）
pub type TaskId = Uuid;

// ─── StreamTask ───

/// 流计算任务定义。
///
/// 每个 StreamTask 对应一个 `CREATE STREAM COMPUTE` SQL 语句。
/// 定义了源表、目标表、窗口类型、投影列和分区键。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamTask {
    /// 任务唯一 ID (UUID v4)
    pub id: TaskId,
    /// 任务名称（用户定义，租户内唯一）
    pub name: String,
    /// 租户 ID（0=默认租户，1..=真实租户）
    pub tenant_id: TenantId,
    /// 源表 ID（数据来源）
    pub source_table_id: u32,
    /// 目标表 ID（聚合结果写入目标）
    pub target_table_id: u32,
    /// 窗口规格
    pub window_spec: WindowSpec,
    /// 投影规格（聚合列 + 透传列）
    pub projection: ProjectionSpec,
    /// 可选的分区键
    pub partition_by: Option<PartitionBySpec>,
    /// 任务状态
    pub status: TaskStatus,
    /// 创建时间 (epoch 毫秒)
    pub created_at: i64,
}

// ─── WindowSpec ───

/// 窗口规格。
///
/// 支持三种窗口类型：
/// - **Tumbling**: 固定大小、不重叠的窗口
/// - **Hopping**: 固定大小、可重叠的滑动窗口
/// - **Session**: 基于事件间隔的动态窗口
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowSpec {
    /// Tumbling 窗口：固定大小，不重叠。
    /// - `interval_ms`: 窗口大小（毫秒）
    Tumbling {
        /// 窗口大小（毫秒）
        interval_ms: u64,
    },
    /// Hopping 窗口：固定大小，可重叠。
    /// - `interval_ms`: 窗口大小（毫秒）
    /// - `step_ms`: 滑动步长（毫秒），必须 ≤ interval_ms
    Hopping {
        /// 窗口大小（毫秒）
        interval_ms: u64,
        /// 滑动步长（毫秒）
        step_ms: u64,
    },
    /// Session 窗口：基于事件间隔的动态窗口。
    /// - `gap_ms`: 会话超时间隔（毫秒），超过此间隔无新事件则关闭窗口
    Session {
        /// 会话超时间隔（毫秒）
        gap_ms: u64,
    },
}

impl WindowSpec {
    /// 返回窗口类型名称。
    pub fn type_name(&self) -> &'static str {
        match self {
            WindowSpec::Tumbling { .. } => "Tumbling",
            WindowSpec::Hopping { .. } => "Hopping",
            WindowSpec::Session { .. } => "Session",
        }
    }

    /// 返回窗口大小（毫秒）。Session 窗口返回 0（无固定大小）。
    pub fn interval_ms(&self) -> u64 {
        match self {
            WindowSpec::Tumbling { interval_ms } => *interval_ms,
            WindowSpec::Hopping { interval_ms, .. } => *interval_ms,
            WindowSpec::Session { .. } => 0,
        }
    }

    /// 返回是否为 Session 窗口。
    pub fn is_session(&self) -> bool {
        matches!(self, WindowSpec::Session { .. })
    }
}

// ─── ProjectionSpec ───

/// 投影规格：定义从源数据到目标列的计算方式。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionSpec {
    /// 投影列列表（聚合列或透传列）
    pub columns: Vec<ProjectionColumn>,
    /// 目标表 schema 定义（column_id → 名称/类型映射）
    pub target_schema: Vec<TargetColumnDef>,
}

/// 投影列：聚合计算列或原始值透传列。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProjectionColumn {
    /// 聚合列：对源列应用聚合函数
    Aggregate {
        /// 聚合函数类型
        function: AggFunction,
        /// 源列 ID
        source_column: u16,
        /// 输出别名
        alias: String,
    },
    /// 透传列：原样输出源列值（用于 GROUP BY 键等）
    Passthrough {
        /// 源列 ID
        source_column: u16,
        /// 输出别名
        alias: String,
    },
}

/// 目标表列定义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetColumnDef {
    /// 列 ID
    pub column_id: u16,
    /// 列名称
    pub column_name: String,
    /// 数据类型
    pub data_type: DataType,
}

// ─── PartitionBySpec ───

/// 分区键规格。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionBySpec {
    /// 源列 ID（分区键所在列）
    pub source_column: u16,
    /// 源列名称
    pub source_column_name: String,
}

// ─── TaskStatus ───

/// 任务运行状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// 正常运行
    Running,
    /// 已暂停
    Paused,
    /// 错误状态（含错误原因）
    Error {
        /// 错误原因描述
        reason: String,
    },
    /// 已停止
    Stopped,
}

// ─── TaskRegistry ───

/// 任务注册表。
///
/// 维护所有 StreamTask 的注册信息，支持按 ID、名称、源表 ID 快速查找。
/// 通过 `RwLock<TaskRegistry>` 包装实现线程安全。
#[derive(Debug, Clone)]
pub struct TaskRegistry {
    /// task_id → StreamTask
    tasks: HashMap<TaskId, StreamTask>,
    /// source_table_id → [TaskId] 反向索引
    source_index: HashMap<u32, Vec<TaskId>>,
    /// (tenant_id, name) → TaskId 索引（租户内唯一）
    name_index: HashMap<(TenantId, String), TaskId>,
}

impl TaskRegistry {
    /// 创建空的注册表。
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            source_index: HashMap::new(),
            name_index: HashMap::new(),
        }
    }

    /// 注册一个新任务。
    ///
    /// # Errors
    /// 如果任务名已存在或 task_id 已存在则返回错误。
    pub fn register(&mut self, task: StreamTask) -> anyhow::Result<()> {
        let name_key = (task.tenant_id, task.name.clone());
        if self.name_index.contains_key(&name_key) {
            return Err(anyhow::anyhow!(
                "stream task name already exists: {}",
                task.name
            ));
        }
        if self.tasks.contains_key(&task.id) {
            return Err(anyhow::anyhow!(
                "stream task id already exists: {}",
                task.id
            ));
        }

        self.name_index.insert(name_key, task.id);
        self.source_index
            .entry(task.source_table_id)
            .or_default()
            .push(task.id);
        self.tasks.insert(task.id, task);

        Ok(())
    }

    /// 移除一个任务。
    pub fn remove(&mut self, task_id: TaskId) -> anyhow::Result<()> {
        let task = self
            .tasks
            .remove(&task_id)
            .ok_or_else(|| anyhow::anyhow!("stream task not found: {}", task_id))?;

        self.name_index.remove(&(task.tenant_id, task.name.clone()));

        if let Some(ids) = self.source_index.get_mut(&task.source_table_id) {
            ids.retain(|id| *id != task_id);
            if ids.is_empty() {
                self.source_index.remove(&task.source_table_id);
            }
        }

        Ok(())
    }

    /// 获取指定 ID 的任务。
    pub fn get(&self, task_id: TaskId) -> Option<&StreamTask> {
        self.tasks.get(&task_id)
    }

    /// 按 (tenant_id, name) 查找任务（租户内唯一）。
    pub fn get_by_name(&self, tenant_id: TenantId, name: &str) -> Option<&StreamTask> {
        self.name_index
            .get(&(tenant_id, name.to_string()))
            .and_then(|id| self.tasks.get(id))
    }

    /// 按源表 ID 查找所有匹配的任务。
    pub fn find_by_source(&self, source_table_id: u32) -> Vec<&StreamTask> {
        self.source_index
            .get(&source_table_id)
            .map(|ids| ids.iter().filter_map(|id| self.tasks.get(id)).collect())
            .unwrap_or_default()
    }

    /// 获取指定状态的所有任务。
    pub fn get_by_status(&self, status: &TaskStatus) -> Vec<StreamTask> {
        self.tasks
            .values()
            .filter(|t| std::mem::discriminant(&t.status) == std::mem::discriminant(status))
            .cloned()
            .collect()
    }

    /// 更新任务状态。
    pub fn update_status(&mut self, task_id: TaskId, status: TaskStatus) -> anyhow::Result<()> {
        let task = self
            .tasks
            .get_mut(&task_id)
            .ok_or_else(|| anyhow::anyhow!("stream task not found: {}", task_id))?;
        task.status = status;
        Ok(())
    }

    /// 返回所有任务。
    pub fn all(&self) -> Vec<&StreamTask> {
        self.tasks.values().collect()
    }

    /// 返回指定租户的所有任务（读侧隔离）。
    pub fn all_for_tenant(&self, tenant_id: TenantId) -> Vec<&StreamTask> {
        self.tasks
            .values()
            .filter(|t| t.tenant_id == tenant_id)
            .collect()
    }

    /// 返回任务总数。
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// 检查注册表是否为空。
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_task(id: Uuid, name: &str, source_table_id: u32) -> StreamTask {
        StreamTask {
            id,
            name: name.to_string(),
            tenant_id: TenantId::DEFAULT,
            source_table_id,
            target_table_id: 100 + source_table_id,
            window_spec: WindowSpec::Tumbling {
                interval_ms: 10_000,
            },
            projection: ProjectionSpec {
                columns: vec![],
                target_schema: vec![],
            },
            partition_by: None,
            status: TaskStatus::Running,
            created_at: 1000,
        }
    }

    #[test]
    fn test_register_and_lookup() {
        let mut registry = TaskRegistry::new();
        let id = Uuid::new_v4();
        let task = make_test_task(id, "my_stream", 1);

        registry.register(task.clone()).unwrap();
        assert_eq!(registry.len(), 1);

        let found = registry.get(id).unwrap();
        assert_eq!(found.name, "my_stream");

        let by_name = registry
            .get_by_name(TenantId::DEFAULT, "my_stream")
            .unwrap();
        assert_eq!(by_name.id, id);

        let by_source = registry.find_by_source(1);
        assert_eq!(by_source.len(), 1);
        assert_eq!(by_source[0].name, "my_stream");
    }

    #[test]
    fn test_duplicate_name() {
        let mut registry = TaskRegistry::new();
        let t1 = make_test_task(Uuid::new_v4(), "dup_name", 1);
        let t2 = make_test_task(Uuid::new_v4(), "dup_name", 2);

        registry.register(t1).unwrap();
        let result = registry.register(t2);
        assert!(result.is_err());
    }

    #[test]
    fn test_same_name_across_tenants() {
        let mut registry = TaskRegistry::new();
        let mut t1 = make_test_task(Uuid::new_v4(), "dup_name", 1);
        t1.tenant_id = TenantId(1);
        let mut t2 = make_test_task(Uuid::new_v4(), "dup_name", 2);
        t2.tenant_id = TenantId(2);

        registry.register(t1).unwrap();
        registry.register(t2).unwrap();
        assert_eq!(registry.len(), 2);

        assert!(registry.get_by_name(TenantId(1), "dup_name").is_some());
        assert!(registry.get_by_name(TenantId(2), "dup_name").is_some());
        assert_eq!(registry.all_for_tenant(TenantId(1)).len(), 1);
        assert_eq!(registry.all_for_tenant(TenantId(2)).len(), 1);
    }

    #[test]
    fn test_remove() {
        let mut registry = TaskRegistry::new();
        let id = Uuid::new_v4();
        let task = make_test_task(id, "to_remove", 1);
        registry.register(task).unwrap();

        registry.remove(id).unwrap();
        assert!(registry.get(id).is_none());
        assert!(registry
            .get_by_name(TenantId::DEFAULT, "to_remove")
            .is_none());
        assert!(registry.find_by_source(1).is_empty());
    }

    #[test]
    fn test_update_status() {
        let mut registry = TaskRegistry::new();
        let id = Uuid::new_v4();
        let task = make_test_task(id, "status_test", 1);
        registry.register(task).unwrap();

        registry.update_status(id, TaskStatus::Paused).unwrap();
        let task = registry.get(id).unwrap();
        assert_eq!(task.status, TaskStatus::Paused);
    }

    #[test]
    fn test_get_by_status() {
        let mut registry = TaskRegistry::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        registry
            .register(make_test_task(id1, "running_task", 1))
            .unwrap();

        let mut paused = make_test_task(id2, "paused_task", 2);
        paused.status = TaskStatus::Paused;
        registry.register(paused).unwrap();

        let running = registry.get_by_status(&TaskStatus::Running);
        assert_eq!(running.len(), 1);

        let paused_tasks = registry.get_by_status(&TaskStatus::Paused);
        assert_eq!(paused_tasks.len(), 1);
    }

    #[test]
    fn test_window_spec_type_names() {
        assert_eq!(
            WindowSpec::Tumbling { interval_ms: 1000 }.type_name(),
            "Tumbling"
        );
        assert_eq!(
            WindowSpec::Hopping {
                interval_ms: 5000,
                step_ms: 1000
            }
            .type_name(),
            "Hopping"
        );
        assert_eq!(WindowSpec::Session { gap_ms: 30000 }.type_name(), "Session");
    }

    #[test]
    fn test_window_spec_is_session() {
        assert!(!WindowSpec::Tumbling { interval_ms: 1000 }.is_session());
        assert!(WindowSpec::Session { gap_ms: 10000 }.is_session());
    }
}

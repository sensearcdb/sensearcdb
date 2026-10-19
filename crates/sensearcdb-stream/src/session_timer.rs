//! SessionTimer — Session 窗口超时检查器。
//!
//! 独立 tokio task，定期检查所有 Session 窗口任务的超时状态。
//! 当 session 最后事件时间与当前时间的间隔超过 gap_ms 时，关闭窗口。
//!
//! ## 检查策略
//!
//! SessionTimer 维护检查间隔，按每个 Session 任务的 gap_ms 动态计算：
//! - `check_interval = min(max(gap_ms / 10, min_check_ms), max_check_ms)`
//! - 每个 tick 遍历所有 Session 任务的 WindowManager，检查超时

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{debug, info};

use crate::task::{TaskId, TaskRegistry, TaskStatus};
use crate::window_manager::{WindowManager, WindowResult};
use crate::StreamSink;

// ─── SessionTimer ───

/// Session 窗口超时检查器。
///
/// 在后台 tokio task 中运行，定期检查 Session 窗口是否超时。
pub struct SessionTimer {
    /// task_id → WindowManager 引用（共享）
    window_managers: Arc<RwLock<HashMap<TaskId, WindowManager>>>,
    /// TaskRegistry 引用（检查任务状态）
    task_registry: Arc<RwLock<TaskRegistry>>,
    /// 写入 sink，将窗口结果写入目标表
    sink: Arc<dyn StreamSink>,
    /// 最小检查间隔（毫秒）
    min_check_interval_ms: u64,
    /// 最大检查间隔（毫秒）
    max_check_interval_ms: u64,
}

impl SessionTimer {
    /// 创建新的 Session 定时器。
    ///
    /// # Arguments
    /// * `window_managers` - 所有任务的 WindowManager 映射
    /// * `task_registry` - 任务注册表
    /// * `min_check_interval_ms` - 最小检查间隔
    /// * `max_check_interval_ms` - 最大检查间隔
    pub fn new(
        window_managers: Arc<RwLock<HashMap<TaskId, WindowManager>>>,
        task_registry: Arc<RwLock<TaskRegistry>>,
        sink: Arc<dyn StreamSink>,
        min_check_interval_ms: u64,
        max_check_interval_ms: u64,
    ) -> Self {
        Self {
            window_managers,
            task_registry,
            sink,
            min_check_interval_ms,
            max_check_interval_ms,
        }
    }

    /// 运行 Session 定时器循环。
    ///
    /// 持续运行直到被 abort。
    pub async fn run(&self) {
        info!(
            "SessionTimer started: min_check={}ms, max_check={}ms",
            self.min_check_interval_ms, self.max_check_interval_ms
        );

        loop {
            // 每次 tick 动态计算检查间隔
            let check_interval_ms = self.calculate_check_interval();
            tokio::time::sleep(tokio::time::Duration::from_millis(check_interval_ms)).await;

            let now = chrono::Utc::now().timestamp_millis();
            self.check_all_sessions(now);
        }
    }

    /// 检查所有 Session 窗口任务的超时状态。
    fn check_all_sessions(&self, now: i64) {
        let task_registry = self.task_registry.read();
        let wms = self.window_managers.read();

        let mut total_closed: usize = 0;

        for (task_id, wm) in wms.iter() {
            // 只检查 Running 状态的 Session 任务
            let should_check = task_registry
                .get(*task_id)
                .map(|task| task.window_spec.is_session() && task.status == TaskStatus::Running)
                .unwrap_or(false);

            if !should_check {
                continue;
            }

            let closed_windows = wm.check_session_timeout(now);

            if !closed_windows.is_empty() {
                total_closed += closed_windows.len();

                for result in &closed_windows {
                    debug!(
                        "SessionTimer: session closed task={}, window=[{}, {}), pk={:?}",
                        task_id, result.window_start, result.window_end, result.partition_key,
                    );

                    // 写入目标表
                    if let Some(task) = task_registry.get(*task_id) {
                        let rows = vec![(result.window_start, result.columns.clone())];
                        if let Err(e) = self.sink.write_batch(task.target_table_id, &rows) {
                            tracing::error!(
                                "SessionTimer: failed to write result for task {}: {}",
                                task_id,
                                e
                            );
                        } else {
                            tracing::info!(
                                "SessionTimer: wrote session result task={}, window=[{}, {}), pk={:?}",
                                task_id, result.window_start, result.window_end, result.partition_key,
                            );
                        }
                    }
                }
            }
        }

        if total_closed > 0 {
            info!(
                "SessionTimer: closed {} session windows across {} tasks",
                total_closed,
                wms.len()
            );
        }
    }

    /// 计算动态检查间隔。
    ///
    /// 遍历所有 Session 任务，取最小 gap_ms 的 1/10 作为检查间隔。
    fn calculate_check_interval(&self) -> u64 {
        let task_registry = self.task_registry.read();

        let min_gap_ms: Option<u64> = task_registry
            .all()
            .iter()
            .filter(|task| task.window_spec.is_session() && task.status == TaskStatus::Running)
            .filter_map(|task| match &task.window_spec {
                crate::task::WindowSpec::Session { gap_ms } => Some(*gap_ms),
                _ => None,
            })
            .min();

        match min_gap_ms {
            Some(gap) => {
                let interval = gap / 10;
                interval.clamp(self.min_check_interval_ms, self.max_check_interval_ms)
            }
            None => self.max_check_interval_ms, // 没有活跃 Session 任务，降到最大间隔
        }
    }

    /// 写入窗口结果到目标表（通过 StreamSink trait）。
    ///
    /// 完整实现通过 Engine::write_batch() 写入目标表。
    fn write_result(&self, task_id: &TaskId, result: &WindowResult) {
        let task_registry = self.task_registry.read();
        if let Some(task) = task_registry.get(*task_id) {
            let rows = vec![(result.window_start, result.columns.clone())];
            if let Err(e) = self.sink.write_batch(task.target_table_id, &rows) {
                tracing::error!("SessionTimer: write failed for task {}: {}", task_id, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{ProjectionColumn, ProjectionSpec, StreamTask, TargetColumnDef, WindowSpec};
    use sensearcdb_common::{AggFunction, DataType, TypedValue};

    fn make_session_task(gap_ms: u64) -> StreamTask {
        StreamTask {
            id: uuid::Uuid::new_v4(),
            name: format!("session_test_{}", gap_ms),
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            source_table_id: 1,
            target_table_id: 100,
            window_spec: WindowSpec::Session { gap_ms },
            projection: ProjectionSpec {
                columns: vec![ProjectionColumn::Aggregate {
                    function: AggFunction::Avg,
                    source_column: 1,
                    alias: "avg".into(),
                }],
                target_schema: vec![TargetColumnDef {
                    column_id: 1,
                    column_name: "avg".into(),
                    data_type: DataType::F64,
                }],
            },
            partition_by: None,
            status: TaskStatus::Running,
            created_at: 1000,
        }
    }

    /// No-op sink for tests.
    struct NoopSink;
    impl StreamSink for NoopSink {
        fn write_batch(
            &self,
            _target_table_id: u32,
            _rows: &[(i64, Vec<TypedValue>)],
        ) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn test_calculate_check_interval_no_tasks() {
        let wms = Arc::new(RwLock::new(HashMap::new()));
        let registry = Arc::new(RwLock::new(TaskRegistry::new()));
        let sink = Arc::new(NoopSink);
        let timer = SessionTimer::new(wms, registry, sink, 1000, 60000);

        let interval = timer.calculate_check_interval();
        assert_eq!(interval, 60000); // 回退到最大间隔
    }

    #[test]
    fn test_calculate_check_interval_with_tasks() {
        let wms = Arc::new(RwLock::new(HashMap::new()));
        let mut registry = TaskRegistry::new();

        // 添加 gap=30000ms 的 Session 任务
        registry.register(make_session_task(30_000)).unwrap();
        // 添加 gap=60000ms 的 Session 任务
        registry.register(make_session_task(60_000)).unwrap();

        let registry = Arc::new(RwLock::new(registry));
        let sink = Arc::new(NoopSink);
        let timer = SessionTimer::new(wms, registry, sink, 1000, 60000);

        let interval = timer.calculate_check_interval();
        // 最小 gap=30000，interval = 30000/10 = 3000，在 [1000, 60000] 范围内
        assert_eq!(interval, 3_000);
    }
}

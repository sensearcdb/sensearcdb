//! StreamComputeEngine — 流计算引擎主结构体。
//!
//! 负责：
//! - 任务注册表管理（创建/删除/暂停/恢复 StreamTask）
//! - 数据事件总线路由（写入路径回调 → channel → ConsumeLoop）
//! - Session 定时器生命周期
//! - 启动恢复（从 config.db 加载持久化任务）
//!
//! ## 架构
//!
//! ```text
//! Engine::write_batch() -> DataEventBus::on_write()
//!   -> mpsc::channel -> ConsumeLoop::recv()
//!     -> WindowManager::on_data() -> WindowResult
//!       -> Engine::write_batch() (write to target table)
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

use sensearcdb_common::{ConfigHandle, TenantId, TypedValue};

use crate::consume_loop::ConsumeLoop;
use crate::data_event_bus::{DataEvent, DataEventBus};
use crate::metrics::stream_dropped_events_total;
use crate::session_timer::SessionTimer;
use crate::task::{
    PartitionBySpec, ProjectionSpec, StreamTask, TaskId, TaskRegistry, TaskStatus, WindowSpec,
};
use crate::task_store::StreamTaskStore;
use crate::window_manager::WindowManager;
use crate::StreamSink;

// ─── StreamComputeEngine ───

/// 流计算引擎主结构体。
///
/// 管理所有 StreamTask 的生命周期，协调 DataEventBus、ConsumeLoop 和 SessionTimer。
/// 通过 `Arc<RwLock<StreamComputeEngine>>` 在多个模块间共享。
pub struct StreamComputeEngine {
    /// 任务注册表（task_id → StreamTask）
    task_registry: Arc<RwLock<TaskRegistry>>,
    /// 数据事件总线（写入路径回调路由）
    data_event_bus: Arc<DataEventBus>,
    /// 每个任务的 WindowManager（task_id → WindowManager）
    window_managers: Arc<RwLock<HashMap<TaskId, WindowManager>>>,
    /// 消费循环句柄
    consume_handles: Vec<tokio::task::JoinHandle<()>>,
    /// Session 定时器句柄
    session_timer_handle: Option<tokio::task::JoinHandle<()>>,
    /// 写入 sink（将窗口结果写入目标表）
    sink: Arc<dyn StreamSink>,
    /// 任务定义持久化存储（config.db `stream_tasks` 表）
    store: Arc<StreamTaskStore>,
    /// 配置句柄
    config: ConfigHandle,
    /// 引擎是否正在运行
    running: bool,
}

impl StreamComputeEngine {
    /// 创建新的流计算引擎实例。
    ///
    /// # Arguments
    /// * `config` - 全局配置句柄
    /// * `sink` - 窗口结果写入 sink
    /// * `store` - 任务定义持久化存储（config.db `stream_tasks` 表）
    pub fn new(
        config: ConfigHandle,
        sink: Arc<dyn StreamSink>,
        store: Arc<StreamTaskStore>,
    ) -> Self {
        Self {
            task_registry: Arc::new(RwLock::new(TaskRegistry::new())),
            data_event_bus: Arc::new(DataEventBus::new()),
            window_managers: Arc::new(RwLock::new(HashMap::new())),
            consume_handles: Vec::new(),
            session_timer_handle: None,
            sink,
            store,
            config,
            running: false,
        }
    }

    /// 启动流计算引擎。
    ///
    /// 从 config.db 恢复持久化任务，启动 ConsumeLoop 和 SessionTimer。
    pub async fn start(&mut self) -> anyhow::Result<()> {
        if self.running {
            info!("StreamComputeEngine already running");
            return Ok(());
        }

        let stream_cfg = self.config.get().stream.clone();
        if !stream_cfg.enabled {
            info!("StreamComputeEngine is disabled in config, skipping start");
            return Ok(());
        }

        info!(
            "StreamComputeEngine starting with concurrency={}, channel_capacity={}",
            stream_cfg.max_concurrency, stream_cfg.channel_capacity
        );

        // 启动恢复：从 config.db `stream_tasks` 表加载全部任务定义，
        // 逐条注入 TaskRegistry，仅对 Running 任务 reactivate（真实恢复）。
        let tasks: Vec<StreamTask> = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("failed to load stream tasks from config.db: {e}"))?;

        for task in &tasks {
            let mut registry = self.task_registry.write();
            if let Err(e) = registry.register(task.clone()) {
                return Err(anyhow::anyhow!(
                    "failed to register restored stream task '{}': {e}",
                    task.name
                ));
            }
        }

        for task in &tasks {
            if matches!(task.status, TaskStatus::Running) {
                self.reactivate_task(task).await?;
            }
        }

        // 启动 SessionTimer（监控所有 Session 窗口任务）
        self.start_session_timer().await;

        self.running = true;
        info!(
            "StreamComputeEngine started successfully ({} tasks active)",
            tasks.len()
        );
        Ok(())
    }

    /// 停止流计算引擎。
    ///
    /// 优雅关闭所有 ConsumeLoop 和 SessionTimer。
    pub async fn stop(&mut self) {
        if !self.running {
            return;
        }

        info!("StreamComputeEngine stopping...");

        // 停止 SessionTimer
        if let Some(handle) = self.session_timer_handle.take() {
            handle.abort();
            info!("SessionTimer stopped");
        }

        // 停止所有 ConsumeLoop
        for handle in self.consume_handles.drain(..) {
            handle.abort();
        }

        self.running = false;
        info!("StreamComputeEngine stopped");
    }

    /// 创建新的流计算任务。
    ///
    /// 解析 SQL 定义后调用此方法注册任务，创建 WindowManager 和 channel，
    /// 启动对应的 ConsumeLoop。
    ///
    /// # Arguments
    /// * `name` - 任务名称（租户内唯一）
    /// * `tenant_id` - 租户 ID
    /// * `source_table_id` - 源表 ID
    /// * `target_table_id` - 目标表 ID
    /// * `window_spec` - 窗口规格（Tumbling/Hopping/Session）
    /// * `projection` - 投影规格（聚合列 + 透传列）
    /// * `partition_by` - 可选的分区键
    pub async fn create_task(
        &mut self,
        name: String,
        tenant_id: TenantId,
        source_table_id: u32,
        target_table_id: u32,
        window_spec: WindowSpec,
        projection: ProjectionSpec,
        partition_by: Option<PartitionBySpec>,
    ) -> anyhow::Result<TaskId> {
        let task_id = Uuid::new_v4();

        let task = StreamTask {
            id: task_id,
            name: name.clone(),
            tenant_id,
            source_table_id,
            target_table_id,
            window_spec: window_spec.clone(),
            projection: projection.clone(),
            partition_by: partition_by.clone(),
            status: TaskStatus::Running,
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        // 注册到 TaskRegistry
        {
            let mut registry = self.task_registry.write();
            registry.register(task.clone())?;
        }

        // 落盘到 config.db（P1-13）。失败则回滚内存注册，避免「内存有、库无」。
        if let Err(e) = self.store.upsert(&task) {
            let mut registry = self.task_registry.write();
            let _ = registry.remove(task_id);
            return Err(anyhow::anyhow!(
                "failed to persist stream task '{}': {e}",
                task.name
            ));
        }

        // 创建 WindowManager
        let stream_cfg = self.config.get().stream.clone();
        let window_manager = WindowManager::new(
            task_id,
            window_spec,
            projection,
            stream_cfg.max_partitions_per_task,
        );

        {
            let mut wms = self.window_managers.write();
            wms.insert(task_id, window_manager);
        }

        // 创建 channel 并注册到 DataEventBus（携带分区键列 ID）
        let (tx, rx) = mpsc::channel::<DataEvent>(stream_cfg.channel_capacity);
        let partition_column = task.partition_by.as_ref().map(|p| p.source_column);
        self.data_event_bus
            .register(source_table_id, task_id, partition_column, tx);

        // 启动 ConsumeLoop
        let wm = {
            let wms = self.window_managers.read();
            wms.get(&task_id).cloned()
        };

        if let Some(wm) = wm {
            let consume_loop = ConsumeLoop::new(
                task_id,
                target_table_id,
                rx,
                wm,
                self.sink.clone(),
                self.config.clone(),
            );
            let handle = tokio::spawn(async move {
                consume_loop.run().await;
            });
            self.consume_handles.push(handle);
        }

        info!(
            "Stream task created: id={}, name={}, source_table={}, target_table={}",
            task_id, name, source_table_id, target_table_id
        );

        Ok(task_id)
    }

    /// 删除一个流计算任务。
    ///
    /// 停止相关的 ConsumeLoop，从 DataEventBus 注销，清理 WindowManager。
    pub async fn drop_task(&mut self, task_id: TaskId) -> anyhow::Result<()> {
        let task = {
            let registry = self.task_registry.read();
            registry.get(task_id).cloned()
        };

        let task = match task {
            Some(t) => t,
            None => {
                return Err(anyhow::anyhow!("stream task not found: {}", task_id));
            }
        };

        // 从 DataEventBus 注销
        self.data_event_bus
            .unregister(task.source_table_id, task_id);

        // 从 config.db 删除任务定义（P1-13）
        if let Err(e) = self.store.delete(task_id) {
            return Err(anyhow::anyhow!(
                "failed to delete stream task {} from config.db: {e}",
                task_id
            ));
        }

        // 从 TaskRegistry 移除
        {
            let mut registry = self.task_registry.write();
            registry.remove(task_id)?;
        }

        // 清理 WindowManager
        {
            let mut wms = self.window_managers.write();
            wms.remove(&task_id);
        }

        info!("Stream task dropped: id={}, name={}", task_id, task.name);
        Ok(())
    }

    /// 暂停一个流计算任务。
    pub async fn pause_task(&mut self, task_id: TaskId) -> anyhow::Result<()> {
        {
            let mut registry = self.task_registry.write();
            registry.update_status(task_id, TaskStatus::Paused)?;
        }
        // 状态落库（P1-13）
        if let Err(e) = self.store.update_status(task_id, &TaskStatus::Paused) {
            return Err(anyhow::anyhow!("failed to persist pause status: {e}"));
        }
        info!("Stream task paused: {}", task_id);
        Ok(())
    }

    /// 恢复一个流计算任务。
    pub async fn resume_task(&mut self, task_id: TaskId) -> anyhow::Result<()> {
        {
            let mut registry = self.task_registry.write();
            registry.update_status(task_id, TaskStatus::Running)?;
        }
        // 状态落库（P1-13）
        if let Err(e) = self.store.update_status(task_id, &TaskStatus::Running) {
            return Err(anyhow::anyhow!("failed to persist resume status: {e}"));
        }
        info!("Stream task resumed: {}", task_id);
        Ok(())
    }

    /// 将任务标记为错误状态（源列被删除等 schema 失效场景）。
    ///
    /// 与 `pause_task`/`resume_task` 同构：先改内存 registry，再 `update_status`
    /// 落库 config.db，保证「内存有、库有」一致性。
    pub async fn mark_error(&mut self, task_id: TaskId, reason: String) -> anyhow::Result<()> {
        let status = TaskStatus::Error {
            reason: reason.clone(),
        };
        {
            let mut registry = self.task_registry.write();
            registry.update_status(task_id, status.clone())?;
        }
        // 状态落库（P1-13）
        if let Err(e) = self.store.update_status(task_id, &status) {
            return Err(anyhow::anyhow!("failed to persist error status: {e}"));
        }
        info!("Stream task marked error: {} ({})", task_id, reason);
        Ok(())
    }

    /// 写入路径回调：分发数据到匹配的流计算任务。
    ///
    /// 由 Engine::write_batch() 在 WAL → MemTable 插入后调用。
    /// 通过 DataEventBus 路由到对应 source_table_id 的 channel。
    ///
    /// # Arguments
    /// * `table_id` - 写入的源表 ID
    /// * `timestamp` - 数据点时间戳 (epoch 毫秒)
    /// * `values` - 多列带类型值（column_id → `TypedValue`）
    /// * `batch_id` - 批量写入 ID
    pub fn on_write(
        &self,
        table_id: u32,
        timestamp: i64,
        values: &[(u16, TypedValue)],
        batch_id: u64,
    ) {
        self.data_event_bus
            .on_write(table_id, timestamp, values, batch_id);
    }

    /// 批量写入回调：一次 `routes.read()` 分发整批点（见 [`DataEventBus::on_write_batch`]）。
    ///
    /// # Arguments
    /// * `entries` - `(table_id, timestamp 毫秒, values)` 列表，顺序即事件顺序。
    /// * `batch_id` - 整批共享的批量写入 ID。
    pub fn on_write_batch(&self, entries: &[(u32, i64, &[(u16, TypedValue)])], batch_id: u64) {
        self.data_event_bus.on_write_batch(entries, batch_id);
    }

    /// 总线中所有监听总数（写路径快路径：0 时跳过 stream fanout）。
    pub fn total_listeners(&self) -> usize {
        self.data_event_bus.total_listeners()
    }

    /// 获取所有任务列表。
    pub fn get_tasks(&self) -> Vec<StreamTask> {
        let registry = self.task_registry.read();
        registry.all().into_iter().cloned().collect()
    }

    /// 获取指定租户的所有任务（读侧隔离）。
    pub fn get_tasks_for_tenant(&self, tenant_id: TenantId) -> Vec<StreamTask> {
        let registry = self.task_registry.read();
        registry
            .all_for_tenant(tenant_id)
            .into_iter()
            .cloned()
            .collect()
    }

    /// 获取指定任务。
    pub fn get_task(&self, task_id: TaskId) -> Option<StreamTask> {
        let registry = self.task_registry.read();
        registry.get(task_id).cloned()
    }

    /// 获取引擎运行状态。
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// 获取引擎状态摘要。
    pub fn status_summary(&self) -> StreamEngineStatus {
        let registry = self.task_registry.read();
        let wms = self.window_managers.read();
        let tasks = registry.all();
        self.build_status_summary(&tasks, &*wms)
    }

    /// 获取指定租户的引擎状态摘要（读侧隔离）。
    pub fn status_summary_for_tenant(&self, tenant_id: TenantId) -> StreamEngineStatus {
        let registry = self.task_registry.read();
        let wms = self.window_managers.read();
        let tasks = registry.all_for_tenant(tenant_id);
        self.build_status_summary(&tasks, &*wms)
    }

    /// 由任务列表构建状态摘要（内部共享）。
    fn build_status_summary(
        &self,
        tasks: &[&StreamTask],
        wms: &HashMap<TaskId, WindowManager>,
    ) -> StreamEngineStatus {
        let mut running_count: usize = 0;
        let mut paused_count: usize = 0;
        let mut error_count: usize = 0;
        let mut total_active_partitions: usize = 0;

        for task in tasks {
            match task.status {
                TaskStatus::Running => running_count += 1,
                TaskStatus::Paused => paused_count += 1,
                TaskStatus::Error { .. } => error_count += 1,
                TaskStatus::Stopped => {}
            }
            if let Some(wm) = wms.get(&task.id) {
                total_active_partitions += wm.active_partitions();
            }
        }

        StreamEngineStatus {
            running: self.running,
            total_tasks: tasks.len(),
            running_tasks: running_count,
            paused_tasks: paused_count,
            error_tasks: error_count,
            total_active_partitions,
            config_enabled: self.config.get().stream.enabled,
            dropped_events: stream_dropped_events_total().get(),
        }
    }

    // ─── 内部方法 ───

    /// 重新激活一个持久化的任务（启动恢复时调用）。
    async fn reactivate_task(&mut self, task: &StreamTask) -> anyhow::Result<()> {
        let stream_cfg = self.config.get().stream.clone();

        let window_manager = WindowManager::new(
            task.id,
            task.window_spec.clone(),
            task.projection.clone(),
            stream_cfg.max_partitions_per_task,
        );

        let (tx, rx) = mpsc::channel::<DataEvent>(stream_cfg.channel_capacity);
        let partition_column = task.partition_by.as_ref().map(|p| p.source_column);
        self.data_event_bus
            .register(task.source_table_id, task.id, partition_column, tx);

        {
            let mut wms = self.window_managers.write();
            wms.insert(task.id, window_manager.clone());
        }

        let consume_loop = ConsumeLoop::new(
            task.id,
            task.target_table_id,
            rx,
            window_manager,
            self.sink.clone(),
            self.config.clone(),
        );
        let handle = tokio::spawn(async move {
            consume_loop.run().await;
        });
        self.consume_handles.push(handle);

        Ok(())
    }

    /// 启动 Session 定时器。
    async fn start_session_timer(&mut self) {
        let stream_cfg = self.config.get().stream.clone();
        let wm_ref = self.window_managers.clone();
        let task_registry_ref = self.task_registry.clone();
        let sink = self.sink.clone();

        let handle = tokio::spawn(async move {
            let timer = SessionTimer::new(
                wm_ref,
                task_registry_ref,
                sink,
                stream_cfg.session_min_check_interval_ms,
                stream_cfg.session_max_check_interval_ms,
            );
            timer.run().await;
        });

        self.session_timer_handle = Some(handle);
        info!("SessionTimer started");
    }
}

impl Clone for StreamComputeEngine {
    fn clone(&self) -> Self {
        Self {
            task_registry: self.task_registry.clone(),
            data_event_bus: self.data_event_bus.clone(),
            window_managers: self.window_managers.clone(),
            consume_handles: Vec::new(),
            session_timer_handle: None,
            config: self.config.clone(),
            sink: self.sink.clone(),
            store: self.store.clone(),
            running: self.running,
        }
    }
}

// ─── StreamEngineStatus ───

/// 流计算引擎状态摘要。
#[derive(Debug, Clone, serde::Serialize)]
pub struct StreamEngineStatus {
    /// 引擎是否在运行
    pub running: bool,
    /// 总任务数
    pub total_tasks: usize,
    /// 运行中任务数
    pub running_tasks: usize,
    /// 已暂停任务数
    pub paused_tasks: usize,
    /// 错误状态任务数
    pub error_tasks: usize,
    /// 活跃分区总数
    pub total_active_partitions: usize,
    /// 配置是否启用
    pub config_enabled: bool,
    /// 累计丢点数（channel 满导致丢弃的数据事件总数）
    pub dropped_events: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{ProjectionColumn, ProjectionSpec, TargetColumnDef, WindowSpec};
    use sensearcdb_common::{AggFunction, AppConfig, DataType};

    fn make_test_config() -> ConfigHandle {
        let mut cfg = AppConfig::default();
        cfg.stream.enabled = true;
        cfg.stream.channel_capacity = 100;
        cfg.stream.max_concurrency = 1;
        cfg.stream.max_partitions_per_task = 100;
        ConfigHandle::new(cfg)
    }

    fn make_test_store() -> Arc<StreamTaskStore> {
        let path =
            std::env::temp_dir().join(format!("test_stream_engine_{}.db", uuid::Uuid::new_v4()));
        Arc::new(StreamTaskStore::open(&path).unwrap())
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

    fn make_test_projection() -> ProjectionSpec {
        ProjectionSpec {
            columns: vec![ProjectionColumn::Aggregate {
                function: AggFunction::Avg,
                source_column: 1,
                alias: "avg_val".into(),
            }],
            target_schema: vec![TargetColumnDef {
                column_id: 1,
                column_name: "avg_val".into(),
                data_type: DataType::F64,
            }],
        }
    }

    #[tokio::test]
    async fn test_engine_create_and_drop_task() {
        let config = make_test_config();
        let sink = Arc::new(NoopSink);
        let store = make_test_store();
        let mut engine = StreamComputeEngine::new(config.clone(), sink, store);

        let task_id = engine
            .create_task(
                "test_stream".into(),
                TenantId::DEFAULT,
                1,   // source_table_id
                100, // target_table_id
                WindowSpec::Tumbling {
                    interval_ms: 10_000,
                },
                make_test_projection(),
                None,
            )
            .await
            .unwrap();

        let tasks = engine.get_tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].name, "test_stream");
        assert_eq!(tasks[0].id, task_id);

        engine.drop_task(task_id).await.unwrap();
        let tasks = engine.get_tasks();
        assert_eq!(tasks.len(), 0);
    }

    #[tokio::test]
    async fn test_engine_pause_resume_task() {
        let config = make_test_config();
        let sink = Arc::new(NoopSink);
        let store = make_test_store();
        let mut engine = StreamComputeEngine::new(config.clone(), sink, store);

        let task_id = engine
            .create_task(
                "test_pause".into(),
                TenantId::DEFAULT,
                2,
                200,
                WindowSpec::Tumbling { interval_ms: 5_000 },
                make_test_projection(),
                None,
            )
            .await
            .unwrap();

        engine.pause_task(task_id).await.unwrap();
        let task = engine.get_task(task_id).unwrap();
        assert!(matches!(task.status, TaskStatus::Paused));

        engine.resume_task(task_id).await.unwrap();
        let task = engine.get_task(task_id).unwrap();
        assert!(matches!(task.status, TaskStatus::Running));
    }

    #[tokio::test]
    async fn test_engine_status_summary() {
        let config = make_test_config();
        let sink = Arc::new(NoopSink);
        let store = make_test_store();
        let mut engine = StreamComputeEngine::new(config.clone(), sink, store);

        engine
            .create_task(
                "t1".into(),
                TenantId::DEFAULT,
                1,
                101,
                WindowSpec::Tumbling {
                    interval_ms: 10_000,
                },
                make_test_projection(),
                None,
            )
            .await
            .unwrap();

        engine
            .create_task(
                "t2".into(),
                TenantId::DEFAULT,
                2,
                102,
                WindowSpec::Tumbling { interval_ms: 5_000 },
                make_test_projection(),
                None,
            )
            .await
            .unwrap();

        let status = engine.status_summary();
        assert_eq!(status.total_tasks, 2);
        assert_eq!(status.running_tasks, 2);
        assert_eq!(status.paused_tasks, 0);
    }

    #[tokio::test]
    async fn test_engine_status_summary_includes_dropped_events() {
        let config = make_test_config();
        let sink = Arc::new(NoopSink);
        let store = make_test_store();
        let mut engine = StreamComputeEngine::new(config.clone(), sink, store);

        // status_summary 暴露的累计丢点数必须来自真实的 Prometheus counter。
        // 用「读取前后窗口」断言（monotonic）而非绝对值，规避并行测试对
        // 全局 counter 的并发递增导致的竞态。
        let before = stream_dropped_events_total().get();
        let status = engine.status_summary();
        let after = stream_dropped_events_total().get();

        assert!(
            status.dropped_events >= before && status.dropped_events <= after,
            "dropped_events {} should be within [{}, {}]",
            status.dropped_events,
            before,
            after
        );
    }

    #[tokio::test]
    async fn test_engine_restart_recovers_running_task() {
        let config = make_test_config();
        let sink = Arc::new(NoopSink);
        let db_path =
            std::env::temp_dir().join(format!("test_stream_restart_{}.db", uuid::Uuid::new_v4()));

        // 第一次启动：创建任务并落库
        {
            let store = Arc::new(StreamTaskStore::open(&db_path).unwrap());
            let mut engine = StreamComputeEngine::new(config.clone(), sink.clone(), store);
            let task_id = engine
                .create_task(
                    "recover_me".into(),
                    TenantId::DEFAULT,
                    7,
                    700,
                    WindowSpec::Tumbling {
                        interval_ms: 10_000,
                    },
                    make_test_projection(),
                    None,
                )
                .await
                .unwrap();
            assert!(!engine.get_tasks().is_empty());
            let _ = task_id;
        }

        // 第二次启动（模拟重启）：从 config.db 恢复
        {
            let store = Arc::new(StreamTaskStore::open(&db_path).unwrap());
            let mut engine = StreamComputeEngine::new(config.clone(), sink.clone(), store);
            engine.start().await.unwrap();
            let tasks = engine.get_tasks();
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].name, "recover_me");
            assert_eq!(tasks[0].source_table_id, 7);
            assert_eq!(tasks[0].target_table_id, 700);
            assert!(matches!(tasks[0].status, TaskStatus::Running));
            engine.stop().await;
        }

        let _ = std::fs::remove_file(&db_path);
    }
}

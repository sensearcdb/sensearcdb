//! ConsumeLoop — 消费循环 (tokio task)。
//!
//! 每个 StreamTask 对应一个 ConsumeLoop，运行在独立的 tokio task 中。
//! 从 mpsc channel 接收 DataEvent，路由到 WindowManager 处理，
//! 将窗口关闭结果写入目标表。
//!
//! ## 职责
//!
//! 1. 从 channel 接收 DataEvent
//! 2. 调用 WindowManager::on_data() 处理事件
//! 3. 接收窗口关闭结果 (WindowResult)
//! 4. 通过 Engine::write_batch() 写入目标表
//!
//! ## 并发
//!
//! ConsumeLoop 的数量由 `stream.max_concurrency` 控制（默认 4）。
//! 每个 ConsumeLoop 独占其 WindowManager，无需内部锁。

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use std::sync::Arc;

use sensearcdb_common::ConfigHandle;

use crate::data_event_bus::DataEvent;
use crate::task::TaskId;
use crate::window_manager::{WindowManager, WindowResult};
use crate::StreamSink;

// ─── ConsumeLoop ───

/// 消费循环。
///
/// 在后台 tokio task 中运行，持续消费 DataEvent 并处理窗口聚合。
pub struct ConsumeLoop {
    /// 所属任务 ID
    task_id: TaskId,
    /// 目标表 ID（聚合结果写入目标）
    target_table_id: u32,
    /// 数据事件接收端
    rx: mpsc::Receiver<DataEvent>,
    /// 窗口管理器
    window_manager: WindowManager,
    /// 写入 sink（将窗口结果写入目标表）
    sink: Arc<dyn StreamSink>,
    /// 配置句柄
    config: ConfigHandle,
    /// 已处理事件计数
    events_processed: u64,
    /// 已关闭窗口计数
    windows_closed: u64,
}

impl ConsumeLoop {
    /// 创建新的消费循环。
    ///
    /// # Arguments
    /// * `task_id` - 所属任务 ID
    /// * `target_table_id` - 目标表 ID
    /// * `rx` - 数据事件接收端
    /// * `window_manager` - 窗口管理器
    /// * `config` - 配置句柄
    pub fn new(
        task_id: TaskId,
        target_table_id: u32,
        rx: mpsc::Receiver<DataEvent>,
        window_manager: WindowManager,
        sink: Arc<dyn StreamSink>,
        config: ConfigHandle,
    ) -> Self {
        Self {
            task_id,
            target_table_id,
            rx,
            window_manager,
            sink,
            config,
            events_processed: 0,
            windows_closed: 0,
        }
    }

    /// 运行消费循环（阻塞当前 task）。
    ///
    /// 持续从 channel 接收事件，直到 channel 关闭。
    pub async fn run(mut self) {
        info!(
            "ConsumeLoop started: task_id={}, target_table={}",
            self.task_id, self.target_table_id
        );

        loop {
            tokio::select! {
                event = self.rx.recv() => {
                    match event {
                        Some(event) => {
                            self.process_event(event).await;
                        }
                        None => {
                            info!(
                                "ConsumeLoop: channel closed for task {}, shutting down (processed={}, closed={})",
                                self.task_id, self.events_processed, self.windows_closed
                            );
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
                    // 定期输出统计
                    debug!(
                        "ConsumeLoop stats: task_id={}, events={}, windows={}",
                        self.task_id, self.events_processed, self.windows_closed
                    );
                }
            }
        }
    }

    /// 处理单个数据事件。
    async fn process_event(&mut self, event: DataEvent) {
        let stream_cfg = self.config.get().stream.clone();

        // 检查引擎是否启用
        if !stream_cfg.enabled {
            return;
        }

        match self
            .window_manager
            .on_data(event.timestamp, event.partition_key, &event.values)
        {
            Ok(closed_windows) => {
                self.events_processed += 1;

                if !closed_windows.is_empty() {
                    self.windows_closed += closed_windows.len() as u64;

                    for result in &closed_windows {
                        debug!(
                            "ConsumeLoop: window closed task={}, window=[{}, {}), pk={:?}, columns={:?}",
                            self.task_id,
                            result.window_start,
                            result.window_end,
                            result.partition_key,
                            result.columns,
                        );

                        // 将聚合结果通过 StreamSink 写入目标表
                        let rows = vec![(result.window_start, result.columns.clone())];
                        if let Err(e) = self.sink.write_batch(self.target_table_id, &rows) {
                            error!(
                                "ConsumeLoop: failed to write window result for task {}: {}",
                                self.task_id, e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    "ConsumeLoop: error processing event for task {}: {}",
                    self.task_id, e
                );
            }
        }
    }

    /// 写入窗口结果到目标表（通过 StreamSink trait）。
    ///
    /// 完整实现中，此处调用 `sink.write_batch()` → `Engine::write_batch()`。
    fn write_window_result(&self, result: &WindowResult) {
        let rows = vec![(result.window_start, result.columns.clone())];
        if let Err(e) = self.sink.write_batch(self.target_table_id, &rows) {
            tracing::error!("ConsumeLoop: write failed for task {}: {}", self.task_id, e);
        } else {
            tracing::info!(
                target: "sensearcdb.stream.window_result",
                "task_id={} target_table={} window_start={} window_end={} pk={:?}",
                self.task_id,
                self.target_table_id,
                result.window_start,
                result.window_end,
                result.partition_key,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{ProjectionColumn, ProjectionSpec, TargetColumnDef, WindowSpec};
    use sensearcdb_common::{AggFunction, AppConfig, DataType, TypedValue};

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

    fn make_test_config() -> ConfigHandle {
        let mut cfg = AppConfig::default();
        cfg.stream.enabled = true;
        cfg.stream.channel_capacity = 100;
        ConfigHandle::new(cfg)
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

    #[tokio::test]
    async fn test_consume_loop_process_event() {
        let (tx, rx) = mpsc::channel::<DataEvent>(100);
        let task_id = uuid::Uuid::new_v4();
        let wm = WindowManager::new(
            task_id,
            WindowSpec::Tumbling {
                interval_ms: 10_000,
            },
            make_test_projection(),
            100,
        );
        let config = make_test_config();
        let sink = Arc::new(NoopSink);

        let consume_loop = ConsumeLoop::new(task_id, 200, rx, wm, sink, config);

        // 发送一个不会关闭窗口的事件
        tx.send(DataEvent {
            task_id,
            timestamp: 5000,
            partition_key: None,
            values: vec![Some(TypedValue::F64(42.0))],
            batch_id: 1,
        })
        .await
        .unwrap();

        // 发送一个会关闭窗口的事件 (t=15000, 窗口 [0,10000) 和 [10000,20000))
        tx.send(DataEvent {
            task_id,
            timestamp: 15000,
            partition_key: None,
            values: vec![Some(TypedValue::F64(100.0))],
            batch_id: 2,
        })
        .await
        .unwrap();

        drop(tx); // 关闭 channel 以终止循环

        // 运行消费循环
        let handle = tokio::spawn(async move {
            consume_loop.run().await;
        });

        // 等待完成
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    }
}

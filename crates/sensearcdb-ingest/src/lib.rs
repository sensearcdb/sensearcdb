//! sensearcdb-ingest: Data ingestion layer for SenseArcDB.
//!
//! Provides gRPC/REST ingest endpoints and the write path into the storage
//! engine (embedded direct write, embedded channel, or remote gRPC).
//!
//! # Architecture
//!
//! ```text
//! REST / gRPC endpoint
//!   → EngineWriter (Embedded or Direct or gRPC)
//!     → Engine (WAL → MemTable)
//! ```
//!
//! # IngestLayer
//!
//! The `IngestLayer` is the top-level struct that owns the ingest subsystem.
//! It holds a reference to the engine for embedded writes and provides
//! REST router construction.
//!
//! Proto-generated modules are compiled from `src/proto/*.proto` via `build.rs`.

use std::sync::Arc;

use sensearcdb_common::config::ConfigHandle;
use sensearcdb_engine::engine::Engine;

pub mod batch;
pub mod types;

pub mod column;
pub mod grpc;
pub mod rest;
pub mod writer;

/// Proto-generated code for ingest services and messages.
pub mod proto {
    tonic::include_proto!("sensearcdb.ingest");
}

// ─── IngestLayer ───────────────────────────────────────────────────────────

/// Top-level ingest subsystem.
///
/// Created at startup, owns the engine reference for embedded writes.
pub struct IngestLayer {
    /// Configuration handle for hot-reloadable settings.
    pub config: ConfigHandle,

    /// Reference to the storage engine.
    pub engine: Arc<Engine>,
}

impl IngestLayer {
    /// Create a new `IngestLayer`.
    ///
    /// # Arguments
    /// * `config` — Hot-reloadable configuration handle.
    /// * `engine` — Shared reference to the storage engine.
    pub async fn new(
        config: ConfigHandle,
        engine: Arc<Engine>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("IngestLayer: initialized");

        Ok(Self { config, engine })
    }

    /// Create a REST router for the ingest layer.
    ///
    /// Uses an `EngineDirectWriter` for embedded writes.
    ///
    /// Readiness probe 聚合 WAL 可写 + MemTable 可服务两态（本入口无集群上下文，
    /// 不检查 Leader；主进程 `src/routes.rs` 会注入含 Leader 判定的完整三态探针）。
    pub fn create_rest_router(&self) -> axum::Router {
        let writer: Arc<dyn writer::EngineWriter> = Arc::new(
            writer::EngineDirectWriter::new(self.engine.clone()),
        );
        let engine = self.engine.clone();
        let readiness_probe: rest::health::ReadinessProbe = Arc::new(move || {
            if !engine.wal.is_writable() {
                return rest::health::ReadinessState::not_ready("wal not writable");
            }
            if !engine.memtable.is_ready() {
                return rest::health::ReadinessState::not_ready("memtable not ready");
            }
            rest::health::ReadinessState::ready()
        });
        rest::router::build_router(
            writer,
            false,
            false,
            None,
            None,
            Some(self.engine.clone()),
            readiness_probe,
        )
    }
}

impl std::fmt::Debug for IngestLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestLayer").finish()
    }
}

// ─── Re-exports: most commonly used types ───

pub use batch::{BatchMeta, BatchSource, ColVec, PointBatch};
pub use writer::{
    convert_to_engine_batch, EmbeddedWriter, EngineDirectWriter, EngineWriter, GrpcWriter,
    WriterType,
};
pub use types::{EngineWriteError, WriteReceipt, WriteStatus};

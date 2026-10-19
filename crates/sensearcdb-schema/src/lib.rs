//! # sensearcdb-schema
//!
//! SenseArcDB Schema 管理：Catalog (SQLite)、SchemaMirror (DashMap×3)、
//! SchemaManager (DDL)、SchemaEventBus (tokio::broadcast)。
//!
//! # 模块架构
//!
//! ```text
//! REST/gRPC → SchemaManager → Catalog (SQLite)
//!                            → SchemaMirror (内存缓存)
//!                            → SchemaEventBus (广播)
//! ```
//!
//! # 使用示例
//!
//! ```ignore
//! use sensearcdb_schema::{
//!     Catalog, SchemaMirror, SchemaEventBus, SchemaManager,
//!     TableInfo, ColumnDef, TableType,
//! };
//! use std::sync::Arc;
//!
//! let catalog = Arc::new(Catalog::open("config.db").unwrap());
//! let mirror = Arc::new(SchemaMirror::new());
//! let event_bus = Arc::new(SchemaEventBus::new(256));
//! let manager = SchemaManager::new(catalog, mirror, event_bus);
//! manager.load_all().unwrap();
//! ```

// ─── 子模块 ───
pub mod api;
pub mod catalog;
pub mod event;
pub mod manager;
pub mod migration;
pub mod mirror;
pub mod purge_certificate;
pub mod quota;
pub mod schema_id;
pub mod tenant;
pub mod validator;

// ─── Generated protobuf code ───
pub mod schema_proto {
    tonic::include_proto!("sensearcdb.schema.v1");
}

// ─── Re-exports: 最常用类型提升到 crate 根 ───

pub use catalog::Catalog;
pub use event::{SchemaEvent, SchemaEventBus};
pub use manager::SchemaManager;
pub use mirror::{ColumnDef, DeadbandConfig, DeadbandType, SchemaMirror, TableInfo, TableType};
pub use purge_certificate::{DeletionRecord, PurgeCertificate};
pub use quota::{QuotaTemplate, TenantQuota, TenantQuotaStore, TenantUsage};
pub use schema_id::compute_schema_id;
pub use tenant::{InitialAdminProvisioner, Tenant, TenantManager, TenantStatus};
pub use validator::{validate_column_name, validate_table_name};

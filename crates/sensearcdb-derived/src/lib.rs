//! # sensearcdb-derived
//!
//! SenseArcDB Derived Tag Engine.
//!
//! Pratt-parser based expression evaluation with DAG dependency graph,
//! Kahn topological sort, data-driven recalculation, debounce, deadband filtering,
//! and cold-start stale detection.
//!
//! # Architecture
//!
//! ```text
//! Expression String
//!       │
//!       ▼
//!   ┌─────────┐
//!   │  Lexer  │  → Token stream
//!   └────┬────┘
//!        ▼
//!   ┌──────────┐
//!   │  Parser  │  → ExprAst (Pratt parsing)
//!   └────┬─────┘
//!        ▼
//!   ┌──────────┐
//!   │ Evaluator │  → (value, quality) at runtime
//!   └────┬─────┘
//!        ▼
//!   ┌──────────────┐
//!   │  WriteBatch  │  → Standard write path (WAL→MemTable→SAC)
//!   └──────────────┘
//! ```
//!
//! # Recursion Protection
//!
//! 1. **session_id**: Unique per `on_source_changed` call — prevents same-batch re-trigger.
//! 2. **call_depth**: Limits derived-on-derived chain depth.
//! 3. **cycle_detection**: DAG rejects circular dependencies at definition time.
//!
//! # Modules
//!
//! | Module               | Purpose                                          |
//! |----------------------|--------------------------------------------------|
//! | [`engine`]           | Core orchestrator (DerivedTagEngine)             |
//! | [`lexer`]            | Tokenizer for expression strings                 |
//! | [`parser`]           | Pratt parser (Token → AST)                       |
//! | [`ast`]              | Expression AST types + utilities                 |
//! | [`evaluator`]        | Runtime evaluation with quality propagation      |
//! | [`dependency_graph`] | DAG with Kahn topological sort                   |
//! | [`builtin_functions`]| abs/sqrt/min/max/if/is_null/coalesce etc.        |
//! | [`debounce`]         | Change coalescence + batch_id dedup              |
//! | [`deadband`]         | Output deadband filtering                        |
//! | [`fallback`]         | Fallback policies (strict/lenient/coalesce)      |
//! | [`api`]              | REST and gRPC management endpoints               |
//! | [`error`]            | Error types for the derived engine               |

pub mod ast;
pub mod builtin_functions;
pub mod deadband;
pub mod debounce;
pub mod definition_store;
pub mod dependency_graph;
pub mod engine;
pub mod error;
pub mod evaluator;
pub mod fallback;
pub mod flusher;
pub mod lexer;
pub mod parser;
pub mod replay;

pub mod api;

// ─── Convenience re-exports ───

pub use ast::{BinaryOp, ExprAst, UnaryOp};
pub use deadband::{check_deadband, DeadbandConfig, DeadbandResult};
pub use debounce::{BatchIdTracker, DebounceState};
pub use definition_store::{DerivedTagRecord, DerivedTagStore, SourceRefDef};
pub use dependency_graph::{DependencyGraph, DerivedTagNode, NodeState};
pub use engine::{DerivedTagEngine, EngineConfig, SchemaAccess, SnapshotRead, WriteSink};
pub use error::DerivedError;
pub use evaluator::{evaluate_ast, EvalContext, EvalResult};
pub use fallback::{FallbackPolicy, LastGoodCache};
pub use flusher::DerivedFlusher;
pub use lexer::{Lexer, Token};
pub use parser::PrattParser;
pub use replay::{evaluate_node, evaluate_series};

//! Error types for the sensearcdb-derived crate.
//!
//! All errors in the derived tag engine are represented by [`DerivedError`],
//! which can be converted to the workspace-wide [`sensearcdb_common::SenseArcError`].

use std::fmt;

/// Errors specific to the derived tag engine.
#[derive(Debug, Clone)]
pub enum DerivedError {
    /// Lexer error: invalid token or malformed input.
    LexerError { message: String, position: usize },

    /// Parser error: syntax error or unexpected token.
    ParseError { message: String, position: usize },

    /// Evaluation error: runtime error during expression evaluation.
    EvalError { message: String },

    /// A referenced source tag has no data available.
    TagNotFound { tag_name: String },

    /// A referenced source tag does not exist in the physical schema,
    /// nor has it been registered as a derived tag.
    UnknownSourceTag { tag_name: String },

    /// A source tag's quality is Bad (and fallback is Strict).
    BadQuality { tag_name: String },

    /// Expression has a circular dependency on itself.
    CircularDependency { cycle: Vec<String> },

    /// Maximum AST depth exceeded.
    AstTooDeep { depth: usize, max_depth: usize },

    /// Maximum number of graph nodes exceeded.
    TooManyNodes { count: usize, max_nodes: usize },

    /// A function name is unknown.
    UnknownFunction { name: String },

    /// Wrong number of arguments to a function.
    WrongArgumentCount {
        function: String,
        expected: usize,
        got: usize,
    },

    /// Invalid argument type for a function.
    InvalidArgument { function: String, message: String },

    /// Internal error (code bug or unexpected state).
    InternalError { message: String },

    /// 定义持久化失败（config.db `derived_tags` 读写失败）。
    ///
    /// `register_tag`（upsert）/ `unregister_tag`（delete）在 persist-first
    /// 写路径下，store 操作失败即返回本错误，且内存状态保持不变。
    PersistenceError { message: String },

    /// 启动恢复失败（`load_all` 或整体恢复流程失败）。
    RestoreError { message: String },
}

impl DerivedError {
    /// Returns a human-readable error code string.
    pub fn code(&self) -> &'static str {
        match self {
            DerivedError::LexerError { .. } => "DERIVED_LEXER_ERROR",
            DerivedError::ParseError { .. } => "DERIVED_PARSE_ERROR",
            DerivedError::EvalError { .. } => "DERIVED_EVAL_ERROR",
            DerivedError::TagNotFound { .. } => "DERIVED_TAG_NOT_FOUND",
            DerivedError::UnknownSourceTag { .. } => "DERIVED_UNKNOWN_SOURCE_TAG",
            DerivedError::BadQuality { .. } => "DERIVED_BAD_QUALITY",
            DerivedError::CircularDependency { .. } => "DERIVED_CIRCULAR_DEPENDENCY",
            DerivedError::AstTooDeep { .. } => "DERIVED_AST_TOO_DEEP",
            DerivedError::TooManyNodes { .. } => "DERIVED_TOO_MANY_NODES",
            DerivedError::UnknownFunction { .. } => "DERIVED_UNKNOWN_FUNCTION",
            DerivedError::WrongArgumentCount { .. } => "DERIVED_WRONG_ARG_COUNT",
            DerivedError::InvalidArgument { .. } => "DERIVED_INVALID_ARG",
            DerivedError::InternalError { .. } => "DERIVED_INTERNAL_ERROR",
            DerivedError::PersistenceError { .. } => "DERIVED_PERSISTENCE_ERROR",
            DerivedError::RestoreError { .. } => "DERIVED_RESTORE_ERROR",
        }
    }
}

impl fmt::Display for DerivedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DerivedError::LexerError { message, position } => {
                write!(f, "lexer error at position {}: {}", position, message)
            }
            DerivedError::ParseError { message, position } => {
                write!(f, "parse error at position {}: {}", position, message)
            }
            DerivedError::EvalError { message } => {
                write!(f, "evaluation error: {}", message)
            }
            DerivedError::TagNotFound { tag_name } => {
                write!(f, "tag not found: '{}'", tag_name)
            }
            DerivedError::UnknownSourceTag { tag_name } => {
                write!(f, "unknown source tag '{}'", tag_name)
            }
            DerivedError::BadQuality { tag_name } => {
                write!(f, "bad quality for tag: '{}'", tag_name)
            }
            DerivedError::CircularDependency { cycle } => {
                write!(f, "circular dependency detected: {}", cycle.join(" -> "))
            }
            DerivedError::AstTooDeep { depth, max_depth } => {
                write!(f, "AST too deep: {} exceeds max {}", depth, max_depth)
            }
            DerivedError::TooManyNodes { count, max_nodes } => {
                write!(
                    f,
                    "too many graph nodes: {} exceeds max {}",
                    count, max_nodes
                )
            }
            DerivedError::UnknownFunction { name } => {
                write!(f, "unknown function: '{}'", name)
            }
            DerivedError::WrongArgumentCount {
                function,
                expected,
                got,
            } => {
                write!(
                    f,
                    "function '{}' expects {} argument(s), got {}",
                    function, expected, got
                )
            }
            DerivedError::InvalidArgument { function, message } => {
                write!(
                    f,
                    "invalid argument for function '{}': {}",
                    function, message
                )
            }
            DerivedError::InternalError { message } => {
                write!(f, "internal error: {}", message)
            }
            DerivedError::PersistenceError { message } => {
                write!(f, "persistence error: {}", message)
            }
            DerivedError::RestoreError { message } => {
                write!(f, "restore error: {}", message)
            }
        }
    }
}

impl std::error::Error for DerivedError {}

impl From<DerivedError> for sensearcdb_common::SenseArcError {
    fn from(e: DerivedError) -> Self {
        sensearcdb_common::SenseArcError::Internal(format!("{}", e))
    }
}

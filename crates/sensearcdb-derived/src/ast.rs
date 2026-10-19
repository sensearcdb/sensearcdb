//! AST nodes for derived tag expressions.
//!
//! Defines the expression AST (`ExprAst`) produced by the Pratt parser,
//! along with utilities for extracting source tag references and computing
//! AST complexity scores.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

// ─── AST Node ───

/// Expression AST node representing a derived tag formula.
///
/// Produced by [`crate::parser::PrattParser`] and consumed by
/// [`crate::evaluator`] for runtime evaluation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExprAst {
    /// Numeric literal (e.g. `42.0`, `3.14`).
    Literal { value: f64 },
    /// Boolean literal (`true` / `false`).
    BoolLiteral { value: bool },
    /// Reference to a source tag by name.
    TagRef { name: String },
    /// Binary arithmetic or comparison operation.
    BinaryOp {
        op: BinaryOp,
        left: Box<ExprAst>,
        right: Box<ExprAst>,
    },
    /// Unary operation (negation or logical NOT).
    UnaryOp { op: UnaryOp, expr: Box<ExprAst> },
    /// Function call with positional arguments.
    FunctionCall { name: String, args: Vec<ExprAst> },
}

// ─── Binary Operator ───

/// Binary operators supported in derived tag expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOp {
    Add, // +
    Sub, // -
    Mul, // *
    Div, // /
    Mod, // %
    Pow, // ^
    Gt,  // >
    Gte, // >=
    Lt,  // <
    Lte, // <=
    Eq,  // ==
    Neq, // !=
    And, // &&
    Or,  // ||
}

impl BinaryOp {
    /// Returns a human-readable symbol for the operator.
    pub fn symbol(&self) -> &'static str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
            BinaryOp::Pow => "^",
            BinaryOp::Gt => ">",
            BinaryOp::Gte => ">=",
            BinaryOp::Lt => "<",
            BinaryOp::Lte => "<=",
            BinaryOp::Eq => "==",
            BinaryOp::Neq => "!=",
            BinaryOp::And => "&&",
            BinaryOp::Or => "||",
        }
    }
}

// ─── Unary Operator ───

/// Unary operators supported in derived tag expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    /// Numeric negation (`-x`).
    Neg,
    /// Logical NOT (`!x`).
    Not,
}

impl UnaryOp {
    /// Returns a human-readable symbol for the operator.
    pub fn symbol(&self) -> &'static str {
        match self {
            UnaryOp::Neg => "-",
            UnaryOp::Not => "!",
        }
    }
}

// ─── AST Utilities ───

impl ExprAst {
    /// Extract all unique source tag names referenced in this AST.
    ///
    /// Walks the entire AST and collects every [`TagRef`](ExprAst::TagRef) name.
    /// Results are deduplicated; order is unspecified.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // AST for: `tag_a + tag_b * 2`
    /// let tags = ast.extract_source_tags();
    /// assert_eq!(tags.len(), 2);
    /// assert!(tags.contains(&"tag_a".to_string()));
    /// assert!(tags.contains(&"tag_b".to_string()));
    /// ```
    pub fn extract_source_tags(&self) -> Vec<String> {
        let mut tags = HashSet::new();
        self.collect_tags(&mut tags);
        let mut result: Vec<String> = tags.into_iter().collect();
        result.sort();
        result
    }

    /// Recursively collect tag names into the provided set.
    fn collect_tags(&self, out: &mut HashSet<String>) {
        match self {
            ExprAst::TagRef { name } => {
                out.insert(name.clone());
            }
            ExprAst::BinaryOp { left, right, .. } => {
                left.collect_tags(out);
                right.collect_tags(out);
            }
            ExprAst::UnaryOp { expr, .. } => {
                expr.collect_tags(out);
            }
            ExprAst::FunctionCall { args, .. } => {
                for arg in args {
                    arg.collect_tags(out);
                }
            }
            ExprAst::Literal { .. } | ExprAst::BoolLiteral { .. } => {}
        }
    }

    /// Compute AST complexity score.
    ///
    /// Score = total number of AST nodes + maximum function nesting depth.
    /// Used to guard against overly complex expressions.
    ///
    /// # Returns
    /// A `u32` score where higher values indicate more complex expressions.
    pub fn compression_score(&self) -> u32 {
        self.node_count() as u32 + self.max_depth()
    }

    /// Count total number of nodes in the AST.
    fn node_count(&self) -> usize {
        match self {
            ExprAst::Literal { .. } | ExprAst::BoolLiteral { .. } | ExprAst::TagRef { .. } => 1,
            ExprAst::UnaryOp { expr, .. } => 1 + expr.node_count(),
            ExprAst::BinaryOp { left, right, .. } => 1 + left.node_count() + right.node_count(),
            ExprAst::FunctionCall { args, .. } => {
                1 + args.iter().map(|a| a.node_count()).sum::<usize>()
            }
        }
    }

    /// Find maximum nesting depth (function call depth) in the AST.
    fn max_depth(&self) -> u32 {
        match self {
            ExprAst::Literal { .. } | ExprAst::BoolLiteral { .. } | ExprAst::TagRef { .. } => 0,
            ExprAst::UnaryOp { expr, .. } => expr.max_depth(),
            ExprAst::BinaryOp { left, right, .. } => left.max_depth().max(right.max_depth()),
            ExprAst::FunctionCall { args, .. } => {
                1 + args.iter().map(|a| a.max_depth()).max().unwrap_or(0)
            }
        }
    }
}

// ─── Display ───

impl std::fmt::Display for ExprAst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExprAst::Literal { value } => write!(f, "{}", value),
            ExprAst::BoolLiteral { value } => write!(f, "{}", value),
            ExprAst::TagRef { name } => write!(f, "`{}`", name),
            ExprAst::BinaryOp { op, left, right } => {
                write!(f, "({} {} {})", left, op.symbol(), right)
            }
            ExprAst::UnaryOp { op, expr } => {
                write!(f, "({}{})", op.symbol(), expr)
            }
            ExprAst::FunctionCall { name, args } => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
        }
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_source_tags_simple() {
        let ast = ExprAst::TagRef {
            name: "sensor_1".into(),
        };
        let tags = ast.extract_source_tags();
        assert_eq!(tags, vec!["sensor_1"]);
    }

    #[test]
    fn test_extract_source_tags_binary() {
        let ast = ExprAst::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(ExprAst::TagRef { name: "a".into() }),
            right: Box::new(ExprAst::TagRef { name: "b".into() }),
        };
        let tags = ast.extract_source_tags();
        assert_eq!(tags, vec!["a", "b"]);
    }

    #[test]
    fn test_extract_source_tags_dedup() {
        // AST: a + a (same tag referenced twice)
        let ast = ExprAst::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(ExprAst::TagRef { name: "a".into() }),
            right: Box::new(ExprAst::TagRef { name: "a".into() }),
        };
        let tags = ast.extract_source_tags();
        assert_eq!(tags, vec!["a"]);
    }

    #[test]
    fn test_extract_source_tags_nested() {
        // AST: abs(a + b)
        let ast = ExprAst::FunctionCall {
            name: "abs".into(),
            args: vec![ExprAst::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(ExprAst::TagRef { name: "a".into() }),
                right: Box::new(ExprAst::TagRef { name: "b".into() }),
            }],
        };
        let tags = ast.extract_source_tags();
        assert_eq!(tags, vec!["a", "b"]);
    }

    #[test]
    fn test_no_tags() {
        let ast = ExprAst::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(ExprAst::Literal { value: 3.0 }),
            right: Box::new(ExprAst::Literal { value: 4.0 }),
        };
        let tags = ast.extract_source_tags();
        assert!(tags.is_empty());
    }

    #[test]
    fn test_compression_score_literal() {
        let ast = ExprAst::Literal { value: 42.0 };
        assert_eq!(ast.compression_score(), 1); // 1 node + 0 depth
    }

    #[test]
    fn test_compression_score_complex() {
        // AST: abs(a + b * c)
        let ast = ExprAst::FunctionCall {
            name: "abs".into(),
            args: vec![ExprAst::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(ExprAst::TagRef { name: "a".into() }),
                right: Box::new(ExprAst::BinaryOp {
                    op: BinaryOp::Mul,
                    left: Box::new(ExprAst::TagRef { name: "b".into() }),
                    right: Box::new(ExprAst::TagRef { name: "c".into() }),
                }),
            }],
        };
        // Nodes: FunctionCall(1) + BinaryOp(+) + TagRef(a) + BinaryOp(*) + TagRef(b) + TagRef(c) = 6
        // Depth: max_depth(FunctionCall) = 2 (func depth 1 + inner 1)
        let score = ast.compression_score();
        assert_eq!(score, 7); // 6 nodes + 1 depth
    }

    #[test]
    fn test_display() {
        let ast = ExprAst::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(ExprAst::TagRef { name: "x".into() }),
            right: Box::new(ExprAst::Literal { value: 1.0 }),
        };
        assert_eq!(format!("{}", ast), "(`x` + 1)");
    }
}

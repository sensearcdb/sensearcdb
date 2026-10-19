//! Runtime evaluator for derived tag expressions.
//!
//! Walks an [`ExprAst`](crate::ast::ExprAst) tree and computes the result
//! using variables provided via [`EvalContext`].
//!
//! # Quality Propagation
//!
//! The evaluator propagates quality codes from source tags through to the result:
//! - Any source tag with `quality == 0` (Bad) results in `quality = 0` for the output.
//! - Otherwise, `quality = 0xC0` (Good) is used.
//! - The result quality is the bitwise AND of all contributing source qualities.

use crate::ast::{BinaryOp, ExprAst, UnaryOp};
use crate::builtin_functions::call_builtin;
use crate::error::DerivedError;
use sensearcdb_common::Quality;
use std::collections::HashMap;

// ─── EvalContext ───

/// Evaluation context providing variable bindings for source tags.
///
/// Maps tag names to `(value, quality, timestamp)` tuples.
#[derive(Debug, Clone)]
pub struct EvalContext {
    /// tag_name → (value, quality, ts)
    pub variables: HashMap<String, (f64, u8, i64)>,
}

impl EvalContext {
    /// Create a new empty evaluation context.
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    /// Create a context with pre-populated variables.
    pub fn with_variables(variables: HashMap<String, (f64, u8, i64)>) -> Self {
        Self { variables }
    }

    /// Insert a variable binding.
    pub fn set(&mut self, name: &str, value: f64, quality: u8, ts: i64) {
        self.variables
            .insert(name.to_string(), (value, quality, ts));
    }

    /// Get a variable's value by name.
    ///
    /// Returns `None` if the tag is not found in the context.
    pub fn get(&self, name: &str) -> Option<&(f64, u8, i64)> {
        self.variables.get(name)
    }
}

impl Default for EvalContext {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Evaluation ───

/// Result of evaluating an expression.
///
/// Contains the computed value and the propagated quality code.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EvalResult {
    /// Computed numeric value.
    pub value: f64,
    /// Propagated quality (bitwise AND of all source qualities, or 0xC0 if no sources).
    pub quality: u8,
}

impl EvalResult {
    /// Create a good result from a value alone.
    pub fn good(value: f64) -> Self {
        Self {
            value,
            quality: Quality::GOOD.0,
        }
    }

    /// Create a bad result from a value.
    pub fn bad(value: f64) -> Self {
        Self {
            value,
            quality: Quality::BAD.0,
        }
    }

    /// Combine two quality codes (bitwise AND for propagation).
    pub fn combine_quality(q1: u8, q2: u8) -> u8 {
        q1 & q2
    }
}

impl ExprAst {
    /// Evaluate this AST in the given context.
    ///
    /// Returns an [`EvalResult`] with the computed value and propagated quality.
    ///
    /// # Quality Propagation Rule
    /// - If any contributing source tag has Bad quality (0x00), the result is Bad.
    /// - The final quality is the bitwise AND of all source tag qualities.
    ///
    /// # Errors
    /// Returns [`DerivedError::TagNotFound`] if a referenced tag is missing from the
    /// context and no fallback is active (the engine handles fallback separately).
    pub fn evaluate(&self, ctx: &EvalContext) -> Result<EvalResult, DerivedError> {
        match self {
            ExprAst::Literal { value } => Ok(EvalResult::good(*value)),

            ExprAst::BoolLiteral { value } => {
                let v = if *value { 1.0 } else { 0.0 };
                Ok(EvalResult::good(v))
            }

            ExprAst::TagRef { name } => {
                let (value, quality, _ts) =
                    ctx.get(name).ok_or_else(|| DerivedError::TagNotFound {
                        tag_name: name.clone(),
                    })?;
                Ok(EvalResult {
                    value: *value,
                    quality: *quality,
                })
            }

            ExprAst::UnaryOp { op, expr } => {
                let inner = expr.evaluate(ctx)?;
                let value = match op {
                    UnaryOp::Neg => -inner.value,
                    UnaryOp::Not => {
                        if inner.value != 0.0 && !inner.value.is_nan() {
                            0.0
                        } else {
                            1.0
                        }
                    }
                };
                Ok(EvalResult {
                    value,
                    quality: inner.quality,
                })
            }

            ExprAst::BinaryOp { op, left, right } => {
                let l = left.evaluate(ctx)?;
                let r = right.evaluate(ctx)?;

                let value = match op {
                    BinaryOp::Add => l.value + r.value,
                    BinaryOp::Sub => l.value - r.value,
                    BinaryOp::Mul => l.value * r.value,
                    BinaryOp::Div => {
                        if r.value == 0.0 {
                            return Err(DerivedError::EvalError {
                                message: "division by zero".to_string(),
                            });
                        }
                        l.value / r.value
                    }
                    BinaryOp::Mod => {
                        if r.value == 0.0 {
                            return Err(DerivedError::EvalError {
                                message: "modulo by zero".to_string(),
                            });
                        }
                        l.value % r.value
                    }
                    BinaryOp::Pow => l.value.powf(r.value),
                    BinaryOp::Gt => {
                        if l.value > r.value {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    BinaryOp::Gte => {
                        if l.value >= r.value {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    BinaryOp::Lt => {
                        if l.value < r.value {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    BinaryOp::Lte => {
                        if l.value <= r.value {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    BinaryOp::Eq => {
                        if (l.value - r.value).abs() < f64::EPSILON {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    BinaryOp::Neq => {
                        if (l.value - r.value).abs() >= f64::EPSILON {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    BinaryOp::And => {
                        let l_truthy = l.value != 0.0 && !l.value.is_nan();
                        let r_truthy = r.value != 0.0 && !r.value.is_nan();
                        if l_truthy && r_truthy {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    BinaryOp::Or => {
                        let l_truthy = l.value != 0.0 && !l.value.is_nan();
                        let r_truthy = r.value != 0.0 && !r.value.is_nan();
                        if l_truthy || r_truthy {
                            1.0
                        } else {
                            0.0
                        }
                    }
                };

                let quality = EvalResult::combine_quality(l.quality, r.quality);
                Ok(EvalResult { value, quality })
            }

            ExprAst::FunctionCall { name, args } => {
                let evaled: Vec<EvalResult> = args
                    .iter()
                    .map(|arg| arg.evaluate(ctx))
                    .collect::<Result<Vec<_>, _>>()?;

                let values: Vec<f64> = evaled.iter().map(|r| r.value).collect();
                let value = call_builtin(name, &values)?;

                // Quality = AND of all argument qualities
                let quality = evaled.iter().fold(Quality::GOOD.0, |acc, r| {
                    EvalResult::combine_quality(acc, r.quality)
                });

                Ok(EvalResult { value, quality })
            }
        }
    }
}

// ─── Convenience Methods ───

/// Evaluate an expression AST with the given variable bindings.
///
/// This is a convenience wrapper that constructs an `EvalContext`
/// and calls `ExprAst::evaluate`.
///
/// # Arguments
/// * `ast` - The parsed expression AST.
/// * `variables` - Variable bindings as `(name, (value, quality, ts))`.
///
/// # Returns
/// `(value, quality)` tuple.
pub fn evaluate_ast(
    ast: &ExprAst,
    variables: &HashMap<String, (f64, u8, i64)>,
) -> Result<EvalResult, DerivedError> {
    let ctx = EvalContext::with_variables(variables.clone());
    ast.evaluate(&ctx)
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;
    use crate::parser::PrattParser;

    fn parse(input: &str) -> ExprAst {
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = PrattParser::new(tokens);
        parser.parse_expression().unwrap()
    }

    fn eval(ast: &ExprAst, vars: &[(&str, f64, u8)]) -> EvalResult {
        let mut ctx = EvalContext::new();
        for (name, value, quality) in vars {
            ctx.set(name, *value, *quality, 0);
        }
        ast.evaluate(&ctx).unwrap()
    }

    #[test]
    fn test_eval_literal() {
        let ast = parse("42.0");
        let result = eval(&ast, &[]);
        assert_eq!(result.value, 42.0);
        assert_eq!(result.quality, Quality::GOOD.0);
    }

    #[test]
    fn test_eval_tag_ref() {
        let ast = parse("my_tag");
        let result = eval(&ast, &[("my_tag", 100.0, Quality::GOOD.0)]);
        assert_eq!(result.value, 100.0);
        assert_eq!(result.quality, Quality::GOOD.0);
    }

    #[test]
    fn test_eval_tag_not_found() {
        let ast = parse("missing_tag");
        let ctx = EvalContext::new();
        let result = ast.evaluate(&ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_addition() {
        let ast = parse("1 + 2");
        let result = eval(&ast, &[]);
        assert_eq!(result.value, 3.0);
    }

    #[test]
    fn test_eval_complex() {
        let ast = parse("`sensor-a` + abs(`sensor-b` * 2.5) / 3");
        let result = eval(
            &ast,
            &[
                ("sensor-a", 10.0, Quality::GOOD.0),
                ("sensor-b", 6.0, Quality::GOOD.0),
            ],
        );
        // 10 + abs(6 * 2.5) / 3 = 10 + 15 / 3 = 10 + 5 = 15
        assert!((result.value - 15.0).abs() < 1e-10);
    }

    #[test]
    fn test_quality_propagation_good() {
        let ast = parse("a + b");
        let result = eval(
            &ast,
            &[("a", 1.0, Quality::GOOD.0), ("b", 2.0, Quality::GOOD.0)],
        );
        assert_eq!(result.quality, Quality::GOOD.0);
    }

    #[test]
    fn test_quality_propagation_bad() {
        let ast = parse("a + b");
        let result = eval(
            &ast,
            &[("a", 1.0, Quality::BAD.0), ("b", 2.0, Quality::GOOD.0)],
        );
        // AND: 0x00 & 0xC0 = 0x00 = Bad
        assert_eq!(result.quality, Quality::BAD.0);
    }

    #[test]
    fn test_quality_propagation_uncertain() {
        let ast = parse("a + b");
        let result = eval(
            &ast,
            &[
                ("a", 1.0, Quality::UNCERTAIN.0),
                ("b", 2.0, Quality::GOOD.0),
            ],
        );
        // AND: 0x40 & 0xC0 = 0x40 = Uncertain
        assert_eq!(result.quality, Quality::UNCERTAIN.0);
    }

    #[test]
    fn test_eval_division_by_zero() {
        let ast = parse("1 / 0");
        let ctx = EvalContext::new();
        let result = ast.evaluate(&ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_comparison() {
        let ast = parse("a > b");
        let result = eval(
            &ast,
            &[("a", 5.0, Quality::GOOD.0), ("b", 3.0, Quality::GOOD.0)],
        );
        assert_eq!(result.value, 1.0);

        let result = eval(
            &ast,
            &[("a", 2.0, Quality::GOOD.0), ("b", 3.0, Quality::GOOD.0)],
        );
        assert_eq!(result.value, 0.0);
    }

    #[test]
    fn test_eval_logical_and_or() {
        let ast = parse("a && b");
        let result = eval(
            &ast,
            &[("a", 1.0, Quality::GOOD.0), ("b", 0.0, Quality::GOOD.0)],
        );
        assert_eq!(result.value, 0.0);

        let ast = parse("a || b");
        let result = eval(
            &ast,
            &[("a", 0.0, Quality::GOOD.0), ("b", 1.0, Quality::GOOD.0)],
        );
        assert_eq!(result.value, 1.0);
    }

    #[test]
    fn test_eval_func_call() {
        let ast = parse("abs(-5)");
        let result = eval(&ast, &[]);
        assert_eq!(result.value, 5.0);
    }

    #[test]
    fn test_quality_propagation_nan_source() {
        // NaN source value should still propagate quality correctly
        let ast = parse("a + b");
        let result = eval(
            &ast,
            &[("a", f64::NAN, Quality::BAD.0), ("b", 2.0, Quality::GOOD.0)],
        );
        assert!(result.value.is_nan());
        // BAD (0x00) & GOOD (0xC0) = BAD (0x00)
        assert_eq!(result.quality, Quality::BAD.0);
    }

    #[test]
    fn test_quality_propagation_function_call() {
        // min(a, b) where a is BAD → result quality should be BAD
        let ast = parse("min(a, b)");
        let result = eval(
            &ast,
            &[("a", 1.0, Quality::BAD.0), ("b", 2.0, Quality::GOOD.0)],
        );
        assert_eq!(result.value, 1.0);
        // BAD AND GOOD = BAD
        assert_eq!(result.quality, Quality::BAD.0);
    }

    #[test]
    fn test_quality_propagation_all_good_func() {
        let ast = parse("max(a, b)");
        let result = eval(
            &ast,
            &[("a", 1.0, Quality::GOOD.0), ("b", 2.0, Quality::GOOD.0)],
        );
        assert_eq!(result.value, 2.0);
        assert_eq!(result.quality, Quality::GOOD.0);
    }

    #[test]
    fn test_quality_propagation_if_func() {
        // if(cond, a, b): all three args contribute to quality
        let ast = parse("if(c, a, b)");
        let result = eval(
            &ast,
            &[
                ("c", 1.0, Quality::GOOD.0),
                ("a", 10.0, Quality::BAD.0),
                ("b", 20.0, Quality::GOOD.0),
            ],
        );
        assert_eq!(result.value, 10.0);
        // GOOD & BAD & GOOD = BAD
        assert_eq!(result.quality, Quality::BAD.0);
    }

    #[test]
    fn test_quality_propagation_three_source_uncertain() {
        // Three sources, one UNCERTAIN → result is UNCERTAIN
        let ast = parse("a + b + c");
        let result = eval(
            &ast,
            &[
                ("a", 1.0, Quality::GOOD.0),
                ("b", 2.0, Quality::UNCERTAIN.0),
                ("c", 3.0, Quality::GOOD.0),
            ],
        );
        assert_eq!(result.value, 6.0);
        // 0xC0 & 0x40 & 0xC0 = 0x40 = UNCERTAIN
        assert_eq!(result.quality, Quality::UNCERTAIN.0);
    }

    #[test]
    fn test_eval_result_combine_quality() {
        assert_eq!(EvalResult::combine_quality(0xC0, 0xC0), 0xC0); // GOOD & GOOD = GOOD
        assert_eq!(EvalResult::combine_quality(0x00, 0xC0), 0x00); // BAD & GOOD = BAD
        assert_eq!(EvalResult::combine_quality(0xC0, 0x40), 0x40); // GOOD & UNCERTAIN = UNCERTAIN
        assert_eq!(EvalResult::combine_quality(0x00, 0x00), 0x00); // BAD & BAD = BAD
    }

    #[test]
    fn test_eval_unary_not() {
        let ast = parse("!a");
        // a=1 (truthy) → !a = 0
        let result = eval(&ast, &[("a", 1.0, Quality::GOOD.0)]);
        assert_eq!(result.value, 0.0);
        // a=0 (falsy) → !a = 1
        let result = eval(&ast, &[("a", 0.0, Quality::GOOD.0)]);
        assert_eq!(result.value, 1.0);
    }

    #[test]
    fn test_eval_modulo_by_zero() {
        let ast = parse("5 % 0");
        let ctx = EvalContext::new();
        let result = ast.evaluate(&ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_equality_ops() {
        let ast = parse("a == b");
        let result = eval(
            &ast,
            &[("a", 1.0, Quality::GOOD.0), ("b", 1.0, Quality::GOOD.0)],
        );
        assert_eq!(result.value, 1.0);

        let ast = parse("a != b");
        let result = eval(
            &ast,
            &[("a", 1.0, Quality::GOOD.0), ("b", 2.0, Quality::GOOD.0)],
        );
        assert_eq!(result.value, 1.0);
    }
}

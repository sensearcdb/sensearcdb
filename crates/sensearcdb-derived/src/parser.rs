//! Pratt parser for derived tag expressions.
//!
//! Converts a token stream from the [`Lexer`](crate::lexer::Lexer) into an
//! [`ExprAst`](crate::ast::ExprAst) using top-down operator precedence (Pratt) parsing.
//!
//! Operator precedence (lowest to highest):
//!   1. `||` (logical OR)
//!   2. `&&` (logical AND)
//!   3. `==`, `!=` (equality)
//!   4. `<`, `>`, `<=`, `>=` (comparison)
//!   5. `+`, `-` (addition/subtraction)
//!   6. `*`, `/`, `%` (multiplication/division/modulo)
//!   7. `^` (exponentiation, right-associative)
//!   8. Prefix: `-` (negation), `!` (logical NOT)
//!   9. Function call, grouping `(...)`, literals, tag refs (atomic)

use crate::ast::{BinaryOp, ExprAst, UnaryOp};
use crate::error::DerivedError;
use crate::lexer::Token;

/// Pratt parser for derived tag expressions.
///
/// # Examples
///
/// ```ignore
/// use sensearcdb_derived::lexer::Lexer;
/// use sensearcdb_derived::parser::PrattParser;
///
/// let mut lexer = Lexer::new("`a` + `b` * 2");
/// let tokens = lexer.tokenize()?;
/// let mut parser = PrattParser::new(tokens);
/// let ast = parser.parse_expression()?;
/// ```
pub struct PrattParser {
    /// Token stream from the lexer.
    tokens: Vec<Token>,
    /// Current parse position.
    position: usize,
    /// Maximum allowed AST depth (from config).
    max_depth: usize,
}

impl PrattParser {
    /// Create a new parser from a token stream.
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
            max_depth: 32,
        }
    }

    /// Create a new parser with a custom maximum AST depth.
    pub fn with_max_depth(tokens: Vec<Token>, max_depth: usize) -> Self {
        Self {
            tokens,
            position: 0,
            max_depth,
        }
    }

    /// Parse the full expression and return the AST.
    ///
    /// Consumes all tokens up to (but not including) `Eof`.
    ///
    /// # Errors
    ///
    /// Returns [`DerivedError::ParseError`] on syntax errors.
    pub fn parse_expression(&mut self) -> Result<ExprAst, DerivedError> {
        let expr = self.expr_bp(0)?;
        if !self.is_eof() {
            let token = self.current();
            return Err(self.error(&format!("unexpected token after expression: {:?}", token)));
        }
        // Check depth
        let score = expr.compression_score();
        if score > self.max_depth as u32 {
            return Err(DerivedError::AstTooDeep {
                depth: score as usize,
                max_depth: self.max_depth,
            });
        }
        Ok(expr)
    }

    // ─── Pratt core ───

    /// Parse expression with minimum binding power `min_bp`.
    ///
    /// This is the core Pratt parsing loop:
    /// 1. Parse a prefix (nud) expression.
    /// 2. While the next token is an infix operator with binding power >= min_bp,
    ///    consume it and parse the right operand (led).
    fn expr_bp(&mut self, min_bp: u8) -> Result<ExprAst, DerivedError> {
        // NUD: parse prefix
        let mut lhs = self.parse_prefix()?;

        // LED loop: consume infix operators
        loop {
            if self.is_eof() {
                break;
            }
            let token = self.current().clone();
            let (left_bp, right_bp) = match self.infix_binding_power(&token) {
                Some(bp) => bp,
                None => break,
            };
            if left_bp < min_bp {
                break;
            }
            self.advance(); // consume the operator
            let rhs = self.expr_bp(right_bp)?;
            let op = self.token_to_binary_op(&token)?;
            lhs = ExprAst::BinaryOp {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    /// Parse a prefix (atomic / unary) expression.
    fn parse_prefix(&mut self) -> Result<ExprAst, DerivedError> {
        if self.is_eof() {
            return Err(self.error("unexpected end of expression"));
        }

        let token = self.current().clone();
        self.advance();

        match token {
            Token::Number(value) => Ok(ExprAst::Literal { value }),
            Token::Bool(value) => Ok(ExprAst::BoolLiteral { value }),
            Token::Identifier(name) => self.parse_identifier_or_func(name),
            Token::BacktickIdent(name) => Ok(ExprAst::TagRef { name }),
            Token::Minus => {
                // Unary negation: right_bp = 9 (highest prefix precedence)
                let expr = self.expr_bp(9)?;
                Ok(ExprAst::UnaryOp {
                    op: UnaryOp::Neg,
                    expr: Box::new(expr),
                })
            }
            Token::Not => {
                let expr = self.expr_bp(9)?;
                Ok(ExprAst::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(expr),
                })
            }
            Token::LParen => {
                let expr = self.expr_bp(0)?;
                self.expect(Token::RParen)?;
                Ok(expr)
            }
            _ => Err(self.error(&format!("unexpected token: {:?}", token))),
        }
    }

    /// Handle an identifier: it could be a `TagRef` or a `FunctionCall`.
    fn parse_identifier_or_func(&mut self, name: String) -> Result<ExprAst, DerivedError> {
        // Check if followed by '(' → function call
        if !self.is_eof() && matches!(self.current(), Token::LParen) {
            self.advance(); // consume '('
            let mut args = Vec::new();
            if !matches!(self.current(), Token::RParen) {
                loop {
                    args.push(self.expr_bp(0)?);
                    if matches!(self.current(), Token::RParen) {
                        break;
                    }
                    self.expect(Token::Comma)?;
                }
            }
            self.expect(Token::RParen)?;
            Ok(ExprAst::FunctionCall { name, args })
        } else {
            // Plain tag reference
            Ok(ExprAst::TagRef { name })
        }
    }

    // ─── Operator precedence ───

    /// Get (left_bp, right_bp) for an infix operator.
    ///
    /// Right-binding-power is one higher than left for left-associative ops.
    /// For right-associative ops (e.g. `^`), right_bp equals left_bp.
    fn infix_binding_power(&self, token: &Token) -> Option<(u8, u8)> {
        match token {
            Token::Or => Some((1, 2)),              // left-assoc
            Token::And => Some((3, 4)),             // left-assoc
            Token::Eq | Token::Neq => Some((5, 6)), // left-assoc
            Token::Lt | Token::Lte | Token::Gt | Token::Gte => Some((7, 8)), // left-assoc
            Token::Plus | Token::Minus => Some((9, 10)), // left-assoc
            Token::Star | Token::Slash | Token::Percent => Some((11, 12)), // left-assoc
            Token::Caret => Some((14, 13)),         // right-assoc: right_bp < left_bp
            _ => None,
        }
    }

    /// Convert a token to the corresponding `BinaryOp`.
    fn token_to_binary_op(&self, token: &Token) -> Result<BinaryOp, DerivedError> {
        match token {
            Token::Plus => Ok(BinaryOp::Add),
            Token::Minus => Ok(BinaryOp::Sub),
            Token::Star => Ok(BinaryOp::Mul),
            Token::Slash => Ok(BinaryOp::Div),
            Token::Percent => Ok(BinaryOp::Mod),
            Token::Caret => Ok(BinaryOp::Pow),
            Token::Gt => Ok(BinaryOp::Gt),
            Token::Gte => Ok(BinaryOp::Gte),
            Token::Lt => Ok(BinaryOp::Lt),
            Token::Lte => Ok(BinaryOp::Lte),
            Token::Eq => Ok(BinaryOp::Eq),
            Token::Neq => Ok(BinaryOp::Neq),
            Token::And => Ok(BinaryOp::And),
            Token::Or => Ok(BinaryOp::Or),
            _ => Err(self.error(&format!("not a binary operator: {:?}", token))),
        }
    }

    // ─── Helpers ───

    /// Return the current token without consuming it.
    fn current(&self) -> &Token {
        &self.tokens[self.position]
    }

    /// Advance to the next token.
    fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }

    /// Check if we've consumed all tokens.
    fn is_eof(&self) -> bool {
        self.position >= self.tokens.len()
    }

    /// Expect a specific token, consuming it. Returns an error if not found.
    fn expect(&mut self, expected: Token) -> Result<(), DerivedError> {
        if self.is_eof() {
            return Err(self.error(&format!("expected {:?}, found end of expression", expected)));
        }
        let actual = self.current();
        if std::mem::discriminant(actual) != std::mem::discriminant(&expected) {
            return Err(self.error(&format!("expected {:?}, found {:?}", expected, actual)));
        }
        self.advance();
        Ok(())
    }

    /// Build a parse error at the current position.
    fn error(&self, msg: &str) -> DerivedError {
        DerivedError::ParseError {
            message: msg.to_string(),
            position: self.position,
        }
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;

    fn parse(input: &str) -> Result<ExprAst, DerivedError> {
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize()?;
        let mut parser = PrattParser::new(tokens);
        parser.parse_expression()
    }

    #[test]
    fn test_literal() {
        let ast = parse("42.0").unwrap();
        assert_eq!(ast, ExprAst::Literal { value: 42.0 });
    }

    #[test]
    fn test_bool() {
        let ast = parse("true").unwrap();
        assert_eq!(ast, ExprAst::BoolLiteral { value: true });
    }

    #[test]
    fn test_tag_ref() {
        let ast = parse("sensor_a").unwrap();
        assert_eq!(
            ast,
            ExprAst::TagRef {
                name: "sensor_a".into()
            }
        );
    }

    #[test]
    fn test_backtick_tag_ref() {
        let ast = parse("`sensor-a`").unwrap();
        assert_eq!(
            ast,
            ExprAst::TagRef {
                name: "sensor-a".into()
            }
        );
    }

    #[test]
    fn test_simple_addition() {
        let ast = parse("1 + 2").unwrap();
        match ast {
            ExprAst::BinaryOp { op, left, right } => {
                assert_eq!(op, BinaryOp::Add);
                assert_eq!(*left, ExprAst::Literal { value: 1.0 });
                assert_eq!(*right, ExprAst::Literal { value: 2.0 });
            }
            _ => panic!("expected BinaryOp"),
        }
    }

    #[test]
    fn test_operator_precedence() {
        // 1 + 2 * 3 → 1 + (2 * 3), NOT (1 + 2) * 3
        let ast = parse("1 + 2 * 3").unwrap();
        match ast {
            ExprAst::BinaryOp { op, left, right } => {
                assert_eq!(op, BinaryOp::Add);
                assert_eq!(*left, ExprAst::Literal { value: 1.0 });
                match *right {
                    ExprAst::BinaryOp { op, left, right } => {
                        assert_eq!(op, BinaryOp::Mul);
                        assert_eq!(*left, ExprAst::Literal { value: 2.0 });
                        assert_eq!(*right, ExprAst::Literal { value: 3.0 });
                    }
                    _ => panic!("expected nested BinaryOp(Mul)"),
                }
            }
            _ => panic!("expected BinaryOp(Add)"),
        }
    }

    #[test]
    fn test_right_associative_power() {
        // 2^3^2 → 2^(3^2)
        let ast = parse("2 ^ 3 ^ 2").unwrap();
        match ast {
            ExprAst::BinaryOp { op, left, right } => {
                assert_eq!(op, BinaryOp::Pow);
                assert_eq!(*left, ExprAst::Literal { value: 2.0 });
                match *right {
                    ExprAst::BinaryOp { op, left, right } => {
                        assert_eq!(op, BinaryOp::Pow);
                        assert_eq!(*left, ExprAst::Literal { value: 3.0 });
                        assert_eq!(*right, ExprAst::Literal { value: 2.0 });
                    }
                    _ => panic!("expected nested BinaryOp(Pow)"),
                }
            }
            _ => panic!("expected BinaryOp(Pow)"),
        }
    }

    #[test]
    fn test_parentheses() {
        // (1 + 2) * 3
        let ast = parse("(1 + 2) * 3").unwrap();
        match ast {
            ExprAst::BinaryOp { op, left, right } => {
                assert_eq!(op, BinaryOp::Mul);
                assert_eq!(*right, ExprAst::Literal { value: 3.0 });
                match *left {
                    ExprAst::BinaryOp { op, left, right } => {
                        assert_eq!(op, BinaryOp::Add);
                        assert_eq!(*left, ExprAst::Literal { value: 1.0 });
                        assert_eq!(*right, ExprAst::Literal { value: 2.0 });
                    }
                    _ => panic!("expected nested BinaryOp(Add)"),
                }
            }
            _ => panic!("expected BinaryOp(Mul)"),
        }
    }

    #[test]
    fn test_unary_negation() {
        let ast = parse("-5").unwrap();
        assert_eq!(
            ast,
            ExprAst::UnaryOp {
                op: UnaryOp::Neg,
                expr: Box::new(ExprAst::Literal { value: 5.0 }),
            }
        );
    }

    #[test]
    fn test_unary_not() {
        let ast = parse("!true").unwrap();
        assert_eq!(
            ast,
            ExprAst::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(ExprAst::BoolLiteral { value: true }),
            }
        );
    }

    #[test]
    fn test_function_call_no_args() {
        // Note: no-arg functions won't parse as function calls since identifiers
        // without '(' are treated as tag refs. This is expected.
        // But we can test with actual no-arg functions that exist.
        // In our grammar, bare identifiers are TagRef, not function calls.
        // Functions require '()'.
        let ast = parse("rand()").unwrap();
        match ast {
            ExprAst::FunctionCall { name, args } => {
                assert_eq!(name, "rand");
                assert!(args.is_empty());
            }
            _ => panic!("expected FunctionCall"),
        }
    }

    #[test]
    fn test_function_call_with_args() {
        let ast = parse("min(1, 2, 3)").unwrap();
        match ast {
            ExprAst::FunctionCall { name, args } => {
                assert_eq!(name, "min");
                assert_eq!(args.len(), 3);
            }
            _ => panic!("expected FunctionCall"),
        }
    }

    #[test]
    fn test_function_call_nested() {
        let ast = parse("abs(`a` - `b`)").unwrap();
        match ast {
            ExprAst::FunctionCall { name, args } => {
                assert_eq!(name, "abs");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("expected FunctionCall"),
        }
    }

    #[test]
    fn test_comparison_chain() {
        let ast = parse("a > 5 && b < 10").unwrap();
        match ast {
            ExprAst::BinaryOp { op, .. } => {
                assert_eq!(op, BinaryOp::And);
            }
            _ => panic!("expected BinaryOp(And)"),
        }
    }

    #[test]
    fn test_complex_expression() {
        // `sensor-a` + abs(`sensor-b` * 2.5) / 3
        let ast = parse("`sensor-a` + abs(`sensor-b` * 2.5) / 3").unwrap();
        // Verify it parses (structure is Add(left, Div(FuncCall, Literal)))
        match ast {
            ExprAst::BinaryOp { op, .. } => {
                assert_eq!(op, BinaryOp::Add);
            }
            _ => panic!("expected BinaryOp(Add)"),
        }
    }

    #[test]
    fn test_err_unexpected_token() {
        let result = parse("a + )");
        assert!(result.is_err());
    }

    #[test]
    fn test_err_empty() {
        let result = parse("");
        assert!(result.is_err());
    }
}

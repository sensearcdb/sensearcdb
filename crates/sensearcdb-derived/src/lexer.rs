//! Lexer (tokenizer) for derived tag expressions.
//!
//! Converts a raw expression string into a stream of [`Token`]s
//! consumed by the [`PrattParser`](crate::parser::PrattParser).
//!
//! Supports:
//! - Numeric literals (integer and floating-point)
//! - Identifiers (unquoted and backtick-quoted for names with special chars)
//! - Boolean literals (`true`, `false`)
//! - Arithmetic operators (`+`, `-`, `*`, `/`, `%`, `^`)
//! - Comparison operators (`>`, `>=`, `<`, `<=`, `==`, `!=`)
//! - Logical operators (`!`, `&&`, `||`)
//! - Parentheses and commas

use crate::error::DerivedError;

// ─── Token ───

/// A single token produced by the lexer.
///
/// Tokens represent the smallest meaningful units of a derived tag expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// Floating-point numeric literal.
    Number(f64),
    /// Unquoted identifier (alphanumeric + underscore, starting with letter or `_`).
    Identifier(String),
    /// Backtick-quoted identifier: `` `tag-name` ``.
    BacktickIdent(String),
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `*`
    Star,
    /// `/`
    Slash,
    /// `%`
    Percent,
    /// `^`
    Caret,
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// `==`
    Eq,
    /// `!=`
    Neq,
    /// `!`
    Not,
    /// `&&`
    And,
    /// `||`
    Or,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `,`
    Comma,
    /// `true` or `false` (keyword literals).
    Bool(bool),
    /// End-of-input marker.
    Eof,
}

impl Token {
    /// Returns a human-readable description of the token variant.
    pub fn name(&self) -> &'static str {
        match self {
            Token::Number(_) => "number",
            Token::Identifier(_) => "identifier",
            Token::BacktickIdent(_) => "backtick-identifier",
            Token::Plus => "'+'",
            Token::Minus => "'-'",
            Token::Star => "'*'",
            Token::Slash => "'/'",
            Token::Percent => "'%'",
            Token::Caret => "'^'",
            Token::Gt => "'>'",
            Token::Gte => "'>='",
            Token::Lt => "'<'",
            Token::Lte => "'<='",
            Token::Eq => "'=='",
            Token::Neq => "'!='",
            Token::Not => "'!'",
            Token::And => "'&&'",
            Token::Or => "'||'",
            Token::LParen => "'('",
            Token::RParen => "')'",
            Token::Comma => "','",
            Token::Bool(_) => "boolean",
            Token::Eof => "end of expression",
        }
    }
}

// ─── Lexer ───

/// Lexer that tokenizes a raw expression string.
///
/// # Examples
///
/// ```ignore
/// let mut lexer = Lexer::new("`tag-a` + 42");
/// let tokens = lexer.tokenize()?;
/// ```
pub struct Lexer {
    /// Input characters (as `Vec<char>` for O(1) indexing).
    input: Vec<char>,
    /// Current position in the input.
    position: usize,
}

impl Lexer {
    /// Create a new lexer for the given expression string.
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

    /// Tokenize the entire input, returning all tokens (excluding `Eof`).
    ///
    /// # Errors
    ///
    /// Returns [`DerivedError::LexerError`] on invalid input (e.g. unmatched backtick).
    pub fn tokenize(&mut self) -> Result<Vec<Token>, DerivedError> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                break;
            }
            tokens.push(token);
        }
        Ok(tokens)
    }

    /// Advance to the next token.
    ///
    /// Skips whitespace. Returns `Token::Eof` when input is exhausted.
    pub fn next_token(&mut self) -> Result<Token, DerivedError> {
        self.skip_whitespace();

        if self.is_eof() {
            return Ok(Token::Eof);
        }

        let ch = self.current();

        match ch {
            // Backtick-quoted identifier
            '`' => self.read_backtick_ident(),

            // Numbers
            '0'..='9' => self.read_number(),

            // Identifiers and keywords
            'a'..='z' | 'A'..='Z' | '_' => self.read_identifier_or_keyword(),

            // Single-character operators and delimiters
            '+' => {
                self.advance();
                Ok(Token::Plus)
            }
            '-' => {
                self.advance();
                Ok(Token::Minus)
            }
            '*' => {
                self.advance();
                Ok(Token::Star)
            }
            '/' => {
                self.advance();
                Ok(Token::Slash)
            }
            '%' => {
                self.advance();
                Ok(Token::Percent)
            }
            '^' => {
                self.advance();
                Ok(Token::Caret)
            }
            '(' => {
                self.advance();
                Ok(Token::LParen)
            }
            ')' => {
                self.advance();
                Ok(Token::RParen)
            }
            ',' => {
                self.advance();
                Ok(Token::Comma)
            }

            // Multi-character operators
            '>' => {
                if self.peek() == Some('=') {
                    self.advance();
                    self.advance();
                    Ok(Token::Gte)
                } else {
                    self.advance();
                    Ok(Token::Gt)
                }
            }
            '<' => {
                if self.peek() == Some('=') {
                    self.advance();
                    self.advance();
                    Ok(Token::Lte)
                } else {
                    self.advance();
                    Ok(Token::Lt)
                }
            }
            '=' => {
                if self.peek() == Some('=') {
                    self.advance();
                    self.advance();
                    Ok(Token::Eq)
                } else {
                    Err(self.error("expected '==' (comparison), single '=' is not supported"))
                }
            }
            '!' => {
                if self.peek() == Some('=') {
                    self.advance();
                    self.advance();
                    Ok(Token::Neq)
                } else {
                    self.advance();
                    Ok(Token::Not)
                }
            }
            '&' => {
                if self.peek() == Some('&') {
                    self.advance();
                    self.advance();
                    Ok(Token::And)
                } else {
                    Err(self.error("expected '&&', single '&' is not supported"))
                }
            }
            '|' => {
                if self.peek() == Some('|') {
                    self.advance();
                    self.advance();
                    Ok(Token::Or)
                } else {
                    Err(self.error("expected '||', single '|' is not supported"))
                }
            }

            // Unmatched character
            _ => Err(self.error(&format!("unexpected character: '{}'", ch))),
        }
    }

    // ─── Internal helpers ───

    /// Return the character at the current position.
    fn current(&self) -> char {
        self.input[self.position]
    }

    /// Advance position by one.
    fn advance(&mut self) {
        self.position += 1;
    }

    /// Peek at the next character without advancing.
    fn peek(&self) -> Option<char> {
        if self.position + 1 < self.input.len() {
            Some(self.input[self.position + 1])
        } else {
            None
        }
    }

    /// Check if we've reached end of input.
    fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }

    /// Skip whitespace characters.
    fn skip_whitespace(&mut self) {
        while !self.is_eof() && self.current().is_whitespace() {
            self.advance();
        }
    }

    /// Build a lexer error at the current position.
    fn error(&self, msg: &str) -> DerivedError {
        DerivedError::LexerError {
            message: msg.to_string(),
            position: self.position,
        }
    }

    /// Read a backtick-quoted identifier: `` `some-tag-name` ``.
    fn read_backtick_ident(&mut self) -> Result<Token, DerivedError> {
        self.advance(); // consume opening backtick
        let mut ident = String::new();
        loop {
            if self.is_eof() {
                return Err(self.error("unmatched backtick: missing closing '`'"));
            }
            let ch = self.current();
            if ch == '`' {
                self.advance(); // consume closing backtick
                if ident.is_empty() {
                    return Err(self.error("empty backtick identifier"));
                }
                return Ok(Token::BacktickIdent(ident));
            }
            ident.push(ch);
            self.advance();
        }
    }

    /// Read a numeric literal (integer or floating-point).
    fn read_number(&mut self) -> Result<Token, DerivedError> {
        let start = self.position;
        while !self.is_eof() && self.current().is_ascii_digit() {
            self.advance();
        }
        // Optional fractional part
        if !self.is_eof() && self.current() == '.' {
            self.advance();
            // Must have at least one digit after decimal point
            if self.is_eof() || !self.current().is_ascii_digit() {
                return Err(self.error("expected digit after decimal point"));
            }
            while !self.is_eof() && self.current().is_ascii_digit() {
                self.advance();
            }
        }
        let num_str: String = self.input[start..self.position].iter().collect();
        let value: f64 = num_str
            .parse()
            .map_err(|_| self.error(&format!("invalid number: '{}'", num_str)))?;
        Ok(Token::Number(value))
    }

    /// Read an unquoted identifier. Then check if it's a keyword (`true`, `false`).
    fn read_identifier_or_keyword(&mut self) -> Result<Token, DerivedError> {
        let start = self.position;
        while !self.is_eof() && (self.current().is_alphanumeric() || self.current() == '_') {
            self.advance();
        }
        let ident: String = self.input[start..self.position].iter().collect();
        match ident.as_str() {
            "true" => Ok(Token::Bool(true)),
            "false" => Ok(Token::Bool(false)),
            _ => Ok(Token::Identifier(ident)),
        }
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenize(input: &str) -> Result<Vec<Token>, DerivedError> {
        let mut lexer = Lexer::new(input);
        lexer.tokenize()
    }

    #[test]
    fn test_numbers() {
        let tokens = tokenize("42").unwrap();
        assert_eq!(tokens, vec![Token::Number(42.0)]);

        let tokens = tokenize("3.14").unwrap();
        assert_eq!(tokens, vec![Token::Number(3.14)]);

        let tokens = tokenize("0.5").unwrap();
        assert_eq!(tokens, vec![Token::Number(0.5)]);
    }

    #[test]
    fn test_identifiers() {
        let tokens = tokenize("abc").unwrap();
        assert_eq!(tokens, vec![Token::Identifier("abc".into())]);

        let tokens = tokenize("tag_name_1").unwrap();
        assert_eq!(tokens, vec![Token::Identifier("tag_name_1".into())]);
    }

    #[test]
    fn test_backtick_ident() {
        let tokens = tokenize("`tag-name`").unwrap();
        assert_eq!(tokens, vec![Token::BacktickIdent("tag-name".into())]);

        let tokens = tokenize("`my tag with spaces`").unwrap();
        assert_eq!(
            tokens,
            vec![Token::BacktickIdent("my tag with spaces".into())]
        );
    }

    #[test]
    fn test_backtick_ident_with_dot() {
        // 宽表列引用：`` `table.col` `` 的点号在反引号内是标识符的一部分，
        // 不作为运算符处理；引擎按最后一个点号拆分为表名 + 列名。
        let tokens = tokenize("`device_01.temperature`").unwrap();
        assert_eq!(
            tokens,
            vec![Token::BacktickIdent("device_01.temperature".into())]
        );
    }

    #[test]
    fn test_booleans() {
        let tokens = tokenize("true").unwrap();
        assert_eq!(tokens, vec![Token::Bool(true)]);

        let tokens = tokenize("false").unwrap();
        assert_eq!(tokens, vec![Token::Bool(false)]);
    }

    #[test]
    fn test_arithmetic_ops() {
        let tokens = tokenize("+ - * / % ^").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::Percent,
                Token::Caret,
            ]
        );
    }

    #[test]
    fn test_comparison_ops() {
        let tokens = tokenize("> >= < <= == !=").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Gt,
                Token::Gte,
                Token::Lt,
                Token::Lte,
                Token::Eq,
                Token::Neq,
            ]
        );
    }

    #[test]
    fn test_logical_ops() {
        let tokens = tokenize("! && ||").unwrap();
        assert_eq!(tokens, vec![Token::Not, Token::And, Token::Or]);
    }

    #[test]
    fn test_parens_and_comma() {
        let tokens = tokenize("( ) ,").unwrap();
        assert_eq!(tokens, vec![Token::LParen, Token::RParen, Token::Comma]);
    }

    #[test]
    fn test_complex_expression() {
        let tokens = tokenize("`sensor-a` + abs(`sensor-b` * 2.5)").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::BacktickIdent("sensor-a".into()),
                Token::Plus,
                Token::Identifier("abs".into()),
                Token::LParen,
                Token::BacktickIdent("sensor-b".into()),
                Token::Star,
                Token::Number(2.5),
                Token::RParen,
            ]
        );
    }

    #[test]
    fn test_err_unmatched_backtick() {
        let result = tokenize("`unclosed");
        assert!(result.is_err());
    }

    #[test]
    fn test_err_empty_backtick() {
        let result = tokenize("``");
        assert!(result.is_err());
    }

    #[test]
    fn test_err_single_equal() {
        let result = tokenize("a = b");
        assert!(result.is_err());
    }

    #[test]
    fn test_err_single_ampersand() {
        let result = tokenize("a & b");
        assert!(result.is_err());
    }

    #[test]
    fn test_whitespace_handling() {
        let tokens = tokenize("  a   +   b  ").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Identifier("a".into()),
                Token::Plus,
                Token::Identifier("b".into()),
            ]
        );
    }

    #[test]
    fn test_bool_is_not_identifier() {
        let tokens = tokenize("trueVal").unwrap();
        assert_eq!(tokens, vec![Token::Identifier("trueVal".into())]);
    }
}

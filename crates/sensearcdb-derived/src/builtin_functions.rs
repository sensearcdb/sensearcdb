//! Built-in functions for derived tag expressions.
//!
//! Provides the standard library of functions callable within derived tag
//! expressions. Each function takes `f64` arguments and returns an `f64` result.
//!
//! # Supported Functions
//!
//! | Function    | Args | Description                          |
//! |-------------|------|--------------------------------------|
//! | `abs(x)`    | 1    | Absolute value                       |
//! | `sqrt(x)`   | 1    | Square root (error if x < 0)         |
//! | `min(...)`  | 2+   | Minimum of arguments                 |
//! | `max(...)`  | 2+   | Maximum of arguments                 |
//! | `if(c,t,f)` | 3    | If c != 0 → t, else → f             |
//! | `is_null(x)`| 1    | 1.0 if x is NaN, else 0.0           |
//! | `coalesce(...)`| 1+ | First non-NaN argument              |
//! | `round(x)`  | 1    | Round to nearest integer             |
//! | `ceil(x)`   | 1    | Round up                             |
//! | `floor(x)`  | 1    | Round down                           |
//! | `pow(b,e)`  | 2    | b^e (same as `^` operator)           |
//! | `log(x)`    | 1    | Natural logarithm (error if x <= 0)  |
//! | `exp(x)`    | 1    | e^x                                  |
//! | `sin(x)`    | 1    | Sine (radians)                       |
//! | `cos(x)`    | 1    | Cosine (radians)                     |
//! | `tan(x)`    | 1    | Tangent (radians)                    |

use crate::error::DerivedError;

/// Check if a function name is a known built-in.
pub fn is_builtin(name: &str) -> bool {
    matches!(
        name,
        "abs"
            | "sqrt"
            | "min"
            | "max"
            | "if"
            | "is_null"
            | "coalesce"
            | "round"
            | "ceil"
            | "floor"
            | "pow"
            | "log"
            | "exp"
            | "sin"
            | "cos"
            | "tan"
    )
}

/// Validate the argument count for a built-in function.
///
/// Returns `Ok(())` if the count matches, or `Err(DerivedError::WrongArgumentCount)`.
pub fn validate_arg_count(name: &str, count: usize) -> Result<(), DerivedError> {
    match name {
        "abs" | "sqrt" | "is_null" | "round" | "ceil" | "floor" | "log" | "exp" | "sin" | "cos"
        | "tan" => {
            if count != 1 {
                return Err(DerivedError::WrongArgumentCount {
                    function: name.to_string(),
                    expected: 1,
                    got: count,
                });
            }
            Ok(())
        }
        "pow" => {
            if count != 2 {
                return Err(DerivedError::WrongArgumentCount {
                    function: name.to_string(),
                    expected: 2,
                    got: count,
                });
            }
            Ok(())
        }
        "if" => {
            if count != 3 {
                return Err(DerivedError::WrongArgumentCount {
                    function: name.to_string(),
                    expected: 3,
                    got: count,
                });
            }
            Ok(())
        }
        "min" | "max" | "coalesce" => {
            if count < 1 {
                return Err(DerivedError::WrongArgumentCount {
                    function: name.to_string(),
                    expected: 1,
                    got: count,
                });
            }
            Ok(())
        }
        _ => Err(DerivedError::UnknownFunction {
            name: name.to_string(),
        }),
    }
}

/// Evaluate a built-in function call.
///
/// # Arguments
/// * `name` - Function name.
/// * `args` - Pre-evaluated argument values (must be `f64`).
///
/// # Returns
/// The function result as `f64`. Returns `f64::NAN` on domain errors.
///
/// # Errors
/// Returns [`DerivedError`] for unknown functions or wrong argument counts.
pub fn call_builtin(name: &str, args: &[f64]) -> Result<f64, DerivedError> {
    validate_arg_count(name, args.len())?;

    match name {
        "abs" => Ok(args[0].abs()),
        "sqrt" => {
            if args[0] < 0.0 {
                return Err(DerivedError::InvalidArgument {
                    function: "sqrt".into(),
                    message: "cannot compute square root of negative number".into(),
                });
            }
            Ok(args[0].sqrt())
        }
        "min" => Ok(args.iter().cloned().fold(f64::INFINITY, f64::min)),
        "max" => Ok(args.iter().cloned().fold(f64::NEG_INFINITY, f64::max)),
        "if" => {
            if args[0] != 0.0 && !args[0].is_nan() {
                Ok(args[1])
            } else {
                Ok(args[2])
            }
        }
        "is_null" => {
            if args[0].is_nan() {
                Ok(1.0)
            } else {
                Ok(0.0)
            }
        }
        "coalesce" => {
            for &arg in args {
                if !arg.is_nan() {
                    return Ok(arg);
                }
            }
            Ok(f64::NAN)
        }
        "round" => Ok(args[0].round()),
        "ceil" => Ok(args[0].ceil()),
        "floor" => Ok(args[0].floor()),
        "pow" => Ok(args[0].powf(args[1])),
        "log" => {
            if args[0] <= 0.0 {
                return Err(DerivedError::InvalidArgument {
                    function: "log".into(),
                    message: "cannot compute log of non-positive number".into(),
                });
            }
            Ok(args[0].ln())
        }
        "exp" => Ok(args[0].exp()),
        "sin" => Ok(args[0].sin()),
        "cos" => Ok(args[0].cos()),
        "tan" => Ok(args[0].tan()),
        _ => Err(DerivedError::UnknownFunction {
            name: name.to_string(),
        }),
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_builtin() {
        assert!(is_builtin("abs"));
        assert!(is_builtin("sqrt"));
        assert!(is_builtin("min"));
        assert!(is_builtin("coalesce"));
        assert!(!is_builtin("unknown_fn"));
    }

    #[test]
    fn test_abs() {
        assert_eq!(call_builtin("abs", &[-5.0]).unwrap(), 5.0);
        assert_eq!(call_builtin("abs", &[3.0]).unwrap(), 3.0);
        assert_eq!(call_builtin("abs", &[0.0]).unwrap(), 0.0);
    }

    #[test]
    fn test_sqrt() {
        assert_eq!(call_builtin("sqrt", &[16.0]).unwrap(), 4.0);
        assert_eq!(call_builtin("sqrt", &[2.0]).unwrap(), 2.0_f64.sqrt());
        assert!(call_builtin("sqrt", &[-1.0]).is_err());
    }

    #[test]
    fn test_min_max() {
        assert_eq!(call_builtin("min", &[3.0, 1.0, 2.0]).unwrap(), 1.0);
        assert_eq!(call_builtin("max", &[3.0, 1.0, 2.0]).unwrap(), 3.0);
        assert_eq!(call_builtin("min", &[5.0]).unwrap(), 5.0);
    }

    #[test]
    fn test_if() {
        assert_eq!(call_builtin("if", &[1.0, 10.0, 20.0]).unwrap(), 10.0);
        assert_eq!(call_builtin("if", &[0.0, 10.0, 20.0]).unwrap(), 20.0);
        assert_eq!(call_builtin("if", &[-1.0, 10.0, 20.0]).unwrap(), 10.0);
    }

    #[test]
    fn test_is_null() {
        assert_eq!(call_builtin("is_null", &[f64::NAN]).unwrap(), 1.0);
        assert_eq!(call_builtin("is_null", &[42.0]).unwrap(), 0.0);
    }

    #[test]
    fn test_coalesce() {
        assert_eq!(
            call_builtin("coalesce", &[f64::NAN, f64::NAN, 42.0]).unwrap(),
            42.0
        );
        assert_eq!(call_builtin("coalesce", &[10.0, 20.0]).unwrap(), 10.0);
        assert!(call_builtin("coalesce", &[f64::NAN]).unwrap().is_nan());
    }

    #[test]
    fn test_round_ceil_floor() {
        assert_eq!(call_builtin("round", &[3.4]).unwrap(), 3.0);
        assert_eq!(call_builtin("round", &[3.6]).unwrap(), 4.0);
        assert_eq!(call_builtin("ceil", &[3.1]).unwrap(), 4.0);
        assert_eq!(call_builtin("floor", &[3.9]).unwrap(), 3.0);
    }

    #[test]
    fn test_pow() {
        assert_eq!(call_builtin("pow", &[2.0, 3.0]).unwrap(), 8.0);
        assert_eq!(call_builtin("pow", &[4.0, 0.5]).unwrap(), 2.0);
    }

    #[test]
    fn test_log() {
        assert!((call_builtin("log", &[std::f64::consts::E]).unwrap() - 1.0).abs() < 1e-10);
        assert!(call_builtin("log", &[0.0]).is_err());
        assert!(call_builtin("log", &[-1.0]).is_err());
    }

    #[test]
    fn test_trig() {
        assert!((call_builtin("sin", &[0.0]).unwrap()).abs() < 1e-10);
        assert!((call_builtin("cos", &[0.0]).unwrap() - 1.0).abs() < 1e-10);
        assert!((call_builtin("tan", &[0.0]).unwrap()).abs() < 1e-10);
    }

    #[test]
    fn test_wrong_arg_count() {
        assert!(call_builtin("abs", &[]).is_err());
        assert!(call_builtin("abs", &[1.0, 2.0]).is_err());
        assert!(call_builtin("if", &[1.0, 2.0]).is_err());
        assert!(call_builtin("pow", &[1.0]).is_err());
        assert!(call_builtin("min", &[]).is_err());
    }

    #[test]
    fn test_unknown_function() {
        assert!(call_builtin("nonexistent", &[1.0]).is_err());
    }
}

//! Pure evaluation helpers for the replay subsystem.
//!
//! The replay path recomputes derived tags from SAC history using a **cross-table
//! as-of join** (see `docs/phase7-replay-derived-alert-design.md` §七). These
//! functions are the reusable, side-effect-free core that the integration layer
//! (`ReplayDerivedAdapter`) calls. They do **not** perform any I/O — reading the
//! SAC source series and writing results back are the caller's responsibility.

use crate::deadband::{check_deadband, DeadbandResult};
use crate::dependency_graph::DerivedTagNode;
use crate::error::DerivedError;
use crate::evaluator::EvalContext;
use crate::fallback::FallbackPolicy;
use sensearcdb_common::Quality;
use std::collections::HashMap;

/// Evaluate a single derived-tag node given already-joined source values.
///
/// This is the pure, side-effect-free core of `DerivedTagEngine::evaluate_and_write`.
/// It:
/// 1. Builds an [`EvalContext`] from `source_values` (keyed by source tag name),
///    applying the node's [`FallbackPolicy`] for any missing source.
/// 2. Evaluates the expression AST with quality propagation (bitwise AND).
/// 3. Applies the output deadband filter.
///
/// # Arguments
/// * `node` - The derived-tag definition (expression, fallback, deadband).
/// * `source_values` - Already-joined source values, keyed by source tag name.
///   A missing entry means the source has no value `≤` the current event time.
/// * `prev_value` - The node's previously emitted value (for deadband).
/// * `prev_ts` - Epoch **seconds** of the previous emit (for deadband).
/// * `now_secs` - Current epoch seconds (for deadband `max_time_secs`).
///
/// # Returns
/// * `Ok((value, quality))` - the value/quality to write on a deadband pass.
/// * `Ok((prev_value, GOOD))` - on deadband suppression the previous value is
///   returned to signal "no new point".
pub fn evaluate_node(
    node: &DerivedTagNode,
    source_values: &HashMap<String, (f64, u8, i64)>,
    prev_value: f64,
    prev_ts: i64,
    now_secs: i64,
) -> Result<(f64, u8), DerivedError> {
    let mut ctx = EvalContext::new();
    for source_tag in &node.source_tags {
        match source_values.get(source_tag) {
            Some(&(value, quality, ts)) => {
                ctx.set(source_tag, value, quality, ts);
            }
            None => match &node.fallback_policy {
                FallbackPolicy::Strict => {
                    return Err(DerivedError::TagNotFound {
                        tag_name: source_tag.clone(),
                    });
                }
                FallbackPolicy::Lenient { .. } => {
                    // No last-good cache on the pure path: a source that has no
                    // value ≤ ts (predates all history) has nothing to reuse.
                    ctx.set(source_tag, f64::NAN, Quality::BAD.0, 0);
                }
                FallbackPolicy::Coalesce { defaults } => {
                    let default = defaults.get(source_tag).copied().unwrap_or(f64::NAN);
                    ctx.set(
                        source_tag,
                        default,
                        if default.is_nan() {
                            Quality::BAD.0
                        } else {
                            Quality::UNCERTAIN.0
                        },
                        0,
                    );
                }
            },
        }
    }

    let result = node.ast.evaluate(&ctx)?;

    let deadband_result =
        check_deadband(result.value, prev_value, prev_ts, now_secs, &node.deadband);
    match deadband_result {
        DeadbandResult::Write => Ok((result.value, result.quality)),
        DeadbandResult::Suppress => Ok((prev_value, Quality::GOOD.0)),
    }
}

/// Two-pass cross-table as-of join evaluation of one derived node.
///
/// The sources' time series are already materialized (sorted ascending by
/// timestamp). Pass 1 collects the union of all source event timestamps; pass 2,
/// for each timestamp in ascending order, takes the most recent value of each
/// source `≤ ts` (as-of join), evaluates the node, and applies deadband.
///
/// # Arguments
/// * `node` - The derived-tag definition to evaluate.
/// * `sources` - Source time series keyed by source tag name. Each series is a
///   sorted `Vec<(ts, value, quality)>`.
///
/// # Returns
/// The node's output series `Vec<(ts, value, quality)>`, sorted ascending by `ts`.
/// Points suppressed by deadband are omitted.
pub fn evaluate_series(
    node: &DerivedTagNode,
    sources: &HashMap<String, Vec<(i64, f64, u8)>>,
) -> Vec<(i64, f64, u8)> {
    // Pass 1: union of all source event timestamps.
    let mut event_ts: Vec<i64> = sources
        .values()
        .flat_map(|series| series.iter().map(|p| p.0))
        .collect();
    event_ts.sort_unstable();
    event_ts.dedup();

    let mut out: Vec<(i64, f64, u8)> = Vec::with_capacity(event_ts.len());
    let mut prev_value: f64 = 0.0;
    let mut prev_ts: i64 = 0;

    for ts in event_ts {
        let now_secs = ts / 1_000_000_000;

        // Pass 2: as-of join — most recent value of each source ≤ ts.
        let mut source_values: HashMap<String, (f64, u8, i64)> =
            HashMap::with_capacity(sources.len());
        for (name, series) in sources {
            if let Some((value, quality, src_ts)) = asof(series, ts) {
                source_values.insert(name.clone(), (value, quality, src_ts));
            }
        }

        match evaluate_node(node, &source_values, prev_value, prev_ts, now_secs) {
            Ok((value, quality)) => {
                // `evaluate_node` returns `prev_value` on deadband suppression.
                // Re-check here to decide whether to emit (a value equal to the
                // previous one is only suppressible when deadband is configured).
                if check_deadband(value, prev_value, prev_ts, now_secs, &node.deadband)
                    == DeadbandResult::Suppress
                {
                    continue;
                }
                out.push((ts, value, quality));
                prev_value = value;
                prev_ts = now_secs;
            }
            Err(_e) => {
                // Strict fallback with a source missing before this ts → no point.
                continue;
            }
        }
    }

    out
}

/// As-of lookup: the most recent point in `series` with `ts ≤ target`.
fn asof(series: &[(i64, f64, u8)], target: i64) -> Option<(f64, u8, i64)> {
    let idx = series.partition_point(|p| p.0 <= target);
    if idx == 0 {
        None
    } else {
        let (ts, value, quality) = series[idx - 1];
        Some((value, quality, ts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deadband::DeadbandConfig;
    use crate::lexer::Lexer;
    use crate::parser::PrattParser;
    use sensearcdb_common::ColumnRef;

    fn parse(expr: &str) -> crate::ast::ExprAst {
        let mut lexer = Lexer::new(expr);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = PrattParser::new(tokens);
        parser.parse_expression().unwrap()
    }

    fn make_node(
        name: &str,
        expr: &str,
        source_table_ids: Vec<(String, ColumnRef)>,
    ) -> DerivedTagNode {
        let ast = parse(expr);
        let source_tags = ast.extract_source_tags();
        let mut ids = source_table_ids;
        if ids.is_empty() {
            ids = source_tags
                .iter()
                .map(|s| (s.clone(), ColumnRef::single(999u32)))
                .collect();
        }
        DerivedTagNode::new(
            42,
            name.to_string(),
            ast,
            0,
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
            ids,
        )
    }

    #[test]
    fn test_evaluate_node_quality_propagation() {
        let node = make_node("efficiency", "a / b * 100", vec![]);
        let mut sources = HashMap::new();
        sources.insert("a".to_string(), (100.0, Quality::GOOD.0, 1_000));
        sources.insert("b".to_string(), (50.0, Quality::BAD.0, 1_000));

        let (value, quality) = evaluate_node(&node, &sources, 0.0, 0, 1).unwrap();
        assert!((value - 200.0).abs() < 1e-9);
        assert_eq!(quality, Quality::BAD.0);
    }

    #[test]
    fn test_evaluate_node_strict_missing_source_errors() {
        let node = make_node("d", "a + b", vec![]);
        let sources = HashMap::new();
        assert!(matches!(
            evaluate_node(&node, &sources, 0.0, 0, 1),
            Err(DerivedError::TagNotFound { .. })
        ));
    }

    #[test]
    fn test_evaluate_node_coalesce_fallback() {
        let mut defaults = HashMap::new();
        defaults.insert("a".to_string(), 7.0);
        defaults.insert("b".to_string(), 3.0);
        let ast = parse("a + b");
        let node = DerivedTagNode::new(
            42,
            "d".into(),
            ast,
            0,
            FallbackPolicy::Coalesce { defaults },
            DeadbandConfig::default(),
            vec![
                ("a".to_string(), ColumnRef::single(1)),
                ("b".to_string(), ColumnRef::single(2)),
            ],
        );
        // No sources present → both use defaults (UNCERTAIN quality).
        let (value, quality) = evaluate_node(&node, &HashMap::new(), 0.0, 0, 1).unwrap();
        assert!((value - 10.0).abs() < 1e-9);
        assert_eq!(quality, Quality::UNCERTAIN.0);
    }

    #[test]
    fn test_evaluate_node_deadband_suppress_returns_prev() {
        let node = make_node("d", "a", vec![]);
        // Deadband: absolute 100 → a change of 1 from 100 is suppressed.
        let node = DerivedTagNode::new(
            42,
            "d".into(),
            node.ast,
            0,
            FallbackPolicy::Strict,
            DeadbandConfig::absolute(100.0),
            vec![("a".to_string(), ColumnRef::single(1))],
        );
        let mut sources = HashMap::new();
        sources.insert("a".to_string(), (101.0, Quality::GOOD.0, 2_000_000_000));
        let (value, _q) = evaluate_node(&node, &sources, 100.0, 1, 2).unwrap();
        // Suppressed → returns previous value.
        assert_eq!(value, 100.0);
    }

    #[test]
    fn test_cross_table_asof_join_sparse_sampling() {
        // C = A + B, where A and B live in different tables with misaligned
        // timestamps. Each event_ts must take each source's ≤ ts nearest value.
        let node = make_node("sum", "a + b", vec![]);
        let mut sources: HashMap<String, Vec<(i64, f64, u8)>> = HashMap::new();
        // A: values at t=1000 (10), t=3000 (30)
        sources.insert(
            "a".to_string(),
            vec![
                (1_000, 10.0, Quality::GOOD.0),
                (3_000, 30.0, Quality::GOOD.0),
            ],
        );
        // B: values at t=2000 (20), t=4000 (40)
        sources.insert(
            "b".to_string(),
            vec![
                (2_000, 20.0, Quality::GOOD.0),
                (4_000, 40.0, Quality::GOOD.0),
            ],
        );

        let out = evaluate_series(&node, &sources);

        // Union of event_ts: [1000, 2000, 3000, 4000].
        // With Strict fallback:
        //   t=1000 → B missing (no value ≤ 1000) → error, no point.
        //   t=2000 → A=10 (asof), B=20 → 30.
        //   t=3000 → A=30, B=20 → 50.
        //   t=4000 → A=30, B=40 → 70.
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].0, 2_000);
        assert!((out[0].1 - 30.0).abs() < 1e-9);
        assert_eq!(out[1].0, 3_000);
        assert!((out[1].1 - 50.0).abs() < 1e-9);
        assert_eq!(out[2].0, 4_000);
        assert!((out[2].1 - 70.0).abs() < 1e-9);
    }

    #[test]
    fn test_cross_table_asof_join_coalesce() {
        // Same as above but with Coalesce fallback: t=1000 (B missing) uses default.
        let mut defaults = HashMap::new();
        defaults.insert("a".to_string(), 0.0);
        defaults.insert("b".to_string(), 0.0);
        let ast = parse("a + b");
        let node = DerivedTagNode::new(
            42,
            "sum".into(),
            ast,
            0,
            FallbackPolicy::Coalesce { defaults },
            DeadbandConfig::default(),
            vec![
                ("a".to_string(), ColumnRef::single(1)),
                ("b".to_string(), ColumnRef::single(2)),
            ],
        );
        let mut sources: HashMap<String, Vec<(i64, f64, u8)>> = HashMap::new();
        sources.insert(
            "a".to_string(),
            vec![
                (1_000, 10.0, Quality::GOOD.0),
                (3_000, 30.0, Quality::GOOD.0),
            ],
        );
        sources.insert(
            "b".to_string(),
            vec![
                (2_000, 20.0, Quality::GOOD.0),
                (4_000, 40.0, Quality::GOOD.0),
            ],
        );

        let out = evaluate_series(&node, &sources);
        assert_eq!(out.len(), 4);
        // t=1000: A=10, B=0(default) → 10
        assert_eq!(out[0].0, 1_000);
        assert!((out[0].1 - 10.0).abs() < 1e-9);
        // t=2000: A=10, B=20 → 30
        assert!((out[1].1 - 30.0).abs() < 1e-9);
        // t=3000: A=30, B=20 → 50
        assert!((out[2].1 - 50.0).abs() < 1e-9);
        // t=4000: A=30, B=40 → 70
        assert!((out[3].1 - 70.0).abs() < 1e-9);
    }

    #[test]
    fn test_derived_on_derived_output_series_chaining() {
        // base = a * 2 ; child = base + 1 (child references base's output).
        let base = make_node("base", "a * 2", vec![]);
        let child = make_node(
            "child",
            "base + 1",
            vec![("base".to_string(), ColumnRef::single(42))],
        );

        let mut raw: HashMap<String, Vec<(i64, f64, u8)>> = HashMap::new();
        raw.insert(
            "a".to_string(),
            vec![(1_000, 5.0, Quality::GOOD.0), (2_000, 7.0, Quality::GOOD.0)],
        );

        // First evaluate base → output series becomes child's source.
        let base_out = evaluate_series(&base, &raw);
        let mut child_sources: HashMap<String, Vec<(i64, f64, u8)>> = HashMap::new();
        child_sources.insert("base".to_string(), base_out);
        let child_out = evaluate_series(&child, &child_sources);

        // base at t=1000 → 10, t=2000 → 14. child = base+1 → 11 and 15.
        assert_eq!(child_out.len(), 2);
        assert!((child_out[0].1 - 11.0).abs() < 1e-9);
        assert!((child_out[1].1 - 15.0).abs() < 1e-9);
    }
}

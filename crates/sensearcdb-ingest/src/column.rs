//! Column-reference parsing helpers for the wide-table write path.
//!
//! These functions were extracted from the (closed-source) protocol-driver
//! layer so the open-source ingest write path can resolve logical tag/column
//! identifiers into [`ColumnRef`]s without depending on any driver code.

use sensearcdb_common::ColumnRef;

/// Parse a logical `tag_id` string into a `(table_id, column_id)` [`ColumnRef`].
///
/// Supports the wide-table write-path convention (§2.3), aligned with the
/// engine SQL `table_<id>.col_<id>` style:
///
/// - `table_42.col_7` / `42.7` → `(42, 7)` (explicit column)
/// - `table_42` / `42`       → `(42, 0)` (single-column N=1 degradation)
///
/// Returns `None` when neither a table id nor a column id can be parsed.
pub fn parse_tag_ref(tag_id: &str) -> Option<ColumnRef> {
    if let Some((table_part, col_part)) = tag_id.rsplit_once('.') {
        let table_id = parse_table_id(table_part)?;
        let column_id = parse_column_id(col_part)?;
        return Some(ColumnRef::new(table_id, column_id));
    }
    let table_id = parse_table_id(tag_id)?;
    Some(ColumnRef::single(table_id))
}

/// Parse the "1 tag = 1 table" convention `S<server>.<TagNNNNN>` into a
/// single-table `ColumnRef` (column 0).
///
/// Table id = `(server - 1) * stride + tag_number`, where `stride` is the
/// per-server tag capacity (configurable, default 20000). The `S`/`s` and
/// `Tag`/`tag` prefixes are accepted case-insensitively. Returns `None` when
/// the tag id does not match this convention (callers fall back to wide-table
/// resolution).
pub fn parse_single_tag_table(tag_id: &str, stride: u32) -> Option<ColumnRef> {
    let after_s = tag_id
        .strip_prefix('S')
        .or_else(|| tag_id.strip_prefix('s'))?;
    let (server_part, tag_part) = after_s.split_once('.')?;
    let server: u32 = server_part.parse().ok()?;
    if server == 0 {
        return None;
    }
    let tag_prefix = tag_part.get(..3)?;
    if !tag_prefix.eq_ignore_ascii_case("Tag") {
        return None;
    }
    let tag_num: u32 = tag_part.get(3..)?.parse().ok()?;
    let table_id = (server - 1).checked_mul(stride)?.checked_add(tag_num)?;
    Some(ColumnRef::new(table_id, 0))
}

/// Parse a table id from `table_<id>` or a bare numeric id.
fn parse_table_id(s: &str) -> Option<u32> {
    if let Some(rest) = s.strip_prefix("table_") {
        return rest.parse::<u32>().ok();
    }
    s.parse::<u32>().ok()
}

/// Parse a column id from `col_<id>` or a bare numeric id.
pub fn parse_column_id(s: &str) -> Option<u16> {
    if let Some(rest) = s.strip_prefix("col_") {
        return rest.parse::<u16>().ok();
    }
    s.parse::<u16>().ok()
}

/// Resolve a column name to its logical `column_id`.
///
/// Recognized forms, in priority order:
/// 1. bare column id — `col_7` / `7` → `7`;
/// 2. full tag ref   — `table_42.col_7` / `42.7` → `7`;
/// 3. anything else  — `fallback` (debug logged).
///
/// The single-column write path passes `fallback = 0` (the N=1 wide-table
/// case); multi-column batch producers pass the dense positional index so
/// plain column names retain their legacy dense layout. We never fabricate a
/// column id silently — every degradation is logged.
pub fn resolve_column_id(column_name: &str, fallback: u16) -> u16 {
    if let Some(id) = parse_column_id(column_name) {
        return id;
    }
    if let Some(r) = parse_tag_ref(column_name) {
        return r.column_id;
    }
    tracing::debug!(
        column_name = %column_name,
        column_id = fallback,
        "column name carries no column id; using fallback column id"
    );
    fallback
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tag_ref_explicit_column() {
        assert_eq!(parse_tag_ref("table_42.col_7"), Some(ColumnRef::new(42, 7)));
        assert_eq!(parse_tag_ref("42.7"), Some(ColumnRef::new(42, 7)));
    }

    #[test]
    fn test_parse_tag_ref_single_column() {
        assert_eq!(parse_tag_ref("table_42"), Some(ColumnRef::new(42, 0)));
        assert_eq!(parse_tag_ref("42"), Some(ColumnRef::new(42, 0)));
    }

    #[test]
    fn test_parse_tag_ref_unparseable() {
        assert_eq!(parse_tag_ref("Temperature"), None);
        assert_eq!(parse_tag_ref("table_42.col_x"), None);
        assert_eq!(parse_tag_ref(""), None);
    }

    #[test]
    fn test_resolve_column_id_free_fn() {
        // Bare column ids.
        assert_eq!(resolve_column_id("col_7", 0), 7);
        assert_eq!(resolve_column_id("7", 0), 7);
        // Full tag refs.
        assert_eq!(resolve_column_id("table_42.col_7", 0), 7);
        assert_eq!(resolve_column_id("42.7", 0), 7);
        // Unqualified → fallback.
        assert_eq!(resolve_column_id("Temperature", 0), 0);
        assert_eq!(resolve_column_id("Temperature", 3), 3);
    }

    #[test]
    fn test_parse_single_tag_table() {
        assert_eq!(
            parse_single_tag_table("S1.Tag00001", 20_000),
            Some(ColumnRef::new(1, 0))
        );
        assert_eq!(
            parse_single_tag_table("S2.Tag00001", 20_000),
            Some(ColumnRef::new(20_001, 0))
        );
        assert_eq!(
            parse_single_tag_table("S5.Tag20000", 20_000),
            Some(ColumnRef::new(100_000, 0))
        );
    }

    #[test]
    fn test_parse_single_tag_table_case_insensitive() {
        assert_eq!(
            parse_single_tag_table("s1.Tag00001", 20_000),
            Some(ColumnRef::new(1, 0))
        );
        assert_eq!(
            parse_single_tag_table("S1.tag00001", 20_000),
            Some(ColumnRef::new(1, 0))
        );
    }

    #[test]
    fn test_parse_single_tag_table_unparseable() {
        assert_eq!(parse_single_tag_table("Temperature", 20_000), None);
        assert_eq!(parse_single_tag_table("table_1.col_0", 20_000), None);
        assert_eq!(parse_single_tag_table("42", 20_000), None);
        assert_eq!(parse_single_tag_table("S1.NotATag", 20_000), None);
        assert_eq!(parse_single_tag_table("S0.Tag00001", 20_000), None);
    }
}

//! QA 独立回归验证（阶段 1）：错误码补齐。
//!
//! 验证契约定稿（contract-final-ruling.md §5.1 C1/O1）：
//! - `InvalidRange` = 40003
//! - `SchemaVersionNotFound` = 40403
//! - `ColumnAlreadyExists` = 40902
//! - `CatalogError` = 50002
//! - `Internal` 仍为 50001（未被改动，O1=A）
//!
//! 同时验证 `http_status() = code / 100` 派生规则（契约红线 #5）。

use sensearcdb_common::SenseArcError;

#[test]
fn qa_new_error_codes_match_ruling() {
    assert_eq!(SenseArcError::InvalidRange("x".into()).code(), 40003);
    assert_eq!(SenseArcError::SchemaVersionNotFound(2).code(), 40403);
    assert_eq!(SenseArcError::ColumnAlreadyExists("c".into()).code(), 40902);
    assert_eq!(SenseArcError::CatalogError("e".into()).code(), 50002);
}

#[test]
fn qa_internal_unchanged() {
    // 契约 O1=A：Internal 保持 50001，CatalogError 另立 50002，二者不冲突。
    assert_eq!(SenseArcError::Internal("e".into()).code(), 50001);
    assert_ne!(
        SenseArcError::Internal("e".into()).code(),
        SenseArcError::CatalogError("e".into()).code()
    );
}

#[test]
fn qa_new_error_codes_http_status() {
    assert_eq!(SenseArcError::InvalidRange("x".into()).http_status(), 400);
    assert_eq!(SenseArcError::SchemaVersionNotFound(2).http_status(), 404);
    assert_eq!(
        SenseArcError::ColumnAlreadyExists("c".into()).http_status(),
        409
    );
    assert_eq!(SenseArcError::CatalogError("e".into()).http_status(), 500);
    assert_eq!(SenseArcError::Internal("e".into()).http_status(), 500);
}

#[test]
fn qa_error_codes_are_five_digit_nonzero() {
    let codes = [
        SenseArcError::InvalidRange("x".into()).code(),
        SenseArcError::SchemaVersionNotFound(1).code(),
        SenseArcError::ColumnAlreadyExists("c".into()).code(),
        SenseArcError::CatalogError("e".into()).code(),
        SenseArcError::Internal("e".into()).code(),
    ];
    for code in codes {
        assert!(code >= 10000 && code <= 99999, "code {} not 5-digit", code);
    }
}

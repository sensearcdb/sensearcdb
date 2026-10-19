//! QA 独立回归验证（阶段 4：契约统一）——错误码 + 端口常量。
//!
//! 对照 PM 裁决 `phase4-contract-unification-ruling.md`：
//! - #7 auth 40100 补登记：`Unauthorized` 变体 = 40100，`http_status()` 派生 401（非 400/404）。
//! - #2 端口常量契约：删 5200、补 5201/5202/5203 三常量。
//!
//! 以全新视角独立验证，不依赖工程师自测。

use sensearcdb_common::constants::{
    DEFAULT_GRPC_ADMIN_PORT, DEFAULT_GRPC_QUERY_PORT, DEFAULT_GRPC_WRITE_PORT,
};
use sensearcdb_common::SenseArcError;

#[test]
fn qa_phase4_unauthorized_code_is_40100() {
    // 裁决 #7：auth 40100 补登记进 error.rs（401xx 段，一次性豁免）。
    assert_eq!(
        SenseArcError::Unauthorized("bad token".into()).code(),
        40100
    );
}

#[test]
fn qa_phase4_unauthorized_http_status_is_401() {
    // 裁决 #7 语义自洽：http_status = code / 100 = 401，驱动 REST 鉴权重定向/重试。
    // 若改归 400/404 会腐蚀派生正确性（这正是裁决 #7 要锁死的）。
    let err = SenseArcError::Unauthorized("x".into());
    assert_eq!(err.http_status(), 401);
    assert_ne!(err.http_status(), 400);
    assert_ne!(err.http_status(), 404);
}

#[test]
fn qa_phase4_port_constants_5201_5202_5203() {
    // 裁决 #2 + 端口表锁死：5201 WriteService / 5202 Query+Flight / 5203 CollectorAdmin。
    assert_eq!(DEFAULT_GRPC_WRITE_PORT, 5201);
    assert_eq!(DEFAULT_GRPC_QUERY_PORT, 5202);
    assert_eq!(DEFAULT_GRPC_ADMIN_PORT, 5203);
}

#[test]
fn qa_phase4_grpc_ports_distinct_and_non_5200() {
    // 5200 废弃永不复用（裁决 #2）；三 gRPC 端口互不相同。
    let ports = [
        DEFAULT_GRPC_WRITE_PORT,
        DEFAULT_GRPC_QUERY_PORT,
        DEFAULT_GRPC_ADMIN_PORT,
    ];
    for p in &ports {
        assert_ne!(*p, 5200, "5200 已废弃，不得复用");
    }
    assert_ne!(ports[0], ports[1]);
    assert_ne!(ports[1], ports[2]);
    assert_ne!(ports[0], ports[2]);
}

#[test]
fn qa_phase4_success_code_convention_is_zero() {
    // 契约 O2：成功码 = 0（REST envelope code 字段）。这里从错误码角度反证：
    // 任何错误码都不应等于 0（0 保留给成功，禁止复用为错误码）。
    let codes = [
        SenseArcError::Unauthorized("x".into()).code(),
        SenseArcError::Internal("x".into()).code(),
        SenseArcError::TableNotFound(1).code(),
    ];
    for c in codes {
        assert_ne!(c, 0, "错误码不得为 0（0 = 成功）");
        assert!(c >= 10000 && c <= 99999, "错误码必须 5 位：{}", c);
    }
}

// ─── 第二轮复核：PM 补裁决 403/429 新码段 ─────────────────────────────

#[test]
fn qa_phase4_forbidden_code_is_40300() {
    // PM 补裁决：403xx 段「无权限/禁止访问」，从 40300 起。
    assert_eq!(SenseArcError::Forbidden("no perm".into()).code(), 40300);
    assert_eq!(
        SenseArcError::Forbidden("no perm".into()).http_status(),
        403
    );
}

#[test]
fn qa_phase4_rate_limited_code_is_42900() {
    // PM 补裁决：429xx 段「请求限流」，从 42900 起（42901 TooManyTags 已占用不重号）。
    assert_eq!(SenseArcError::RateLimited.code(), 42900);
    assert_eq!(SenseArcError::RateLimited.http_status(), 429);
}

#[test]
fn qa_phase4_rate_limited_distinct_from_too_many_tags() {
    // 42900（限流）与 42901（Snapshot TooManyTags）不重号。
    assert_ne!(
        SenseArcError::RateLimited.code(),
        SenseArcError::TooManyTags(2, 1).code()
    );
    assert_eq!(SenseArcError::TooManyTags(2, 1).code(), 42901);
}

#[test]
fn qa_phase4_security_codes_are_five_digit() {
    // 401/403/429 三语义段全部 5 位码，禁止 3 位字面量。
    let codes = [
        SenseArcError::Unauthorized("x".into()).code(),
        SenseArcError::Forbidden("x".into()).code(),
        SenseArcError::RateLimited.code(),
    ];
    for c in codes {
        assert!(c >= 10000 && c <= 99999, "安全错误码必须 5 位：{}", c);
    }
}

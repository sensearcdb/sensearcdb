//! QA 独立回归验证（阶段 6c 裁决 #5）——`cert_algorithm` "SM2" 变体已删除。
//!
//! 对照 PM 裁决 `phase6-security-ruling.md` #5：
//! - 直接删除 `cert_algorithm` 的 "SM2" 选项（非保留占位）。
//! - 用户配置 `cert_algorithm = "SM2"` 在**配置反序列化阶段**即报「未知枚举值」，
//!   而非启动时二次校验拒绝。
//!
//! 以全新视角独立验证，不依赖工程师自测。

use sensearcdb_common::config::{MtlsCertAlgorithm, SecurityMtlsConfig};

/// `MtlsCertAlgorithm` 枚举仅含 `Ed25519`，反序列化 "SM2" 必须报「未知变体」。
#[test]
fn qa_phase6_cert_algorithm_sm2_unknown_variant_rejected() {
    // 合法：Ed25519 可反序列化。
    let ok: MtlsCertAlgorithm = serde_json::from_str("\"Ed25519\"").expect("Ed25519 可解析");
    assert_eq!(ok, MtlsCertAlgorithm::Ed25519);

    // 裁决 #5：删除 "SM2" 变体 → 反序列化阶段报未知枚举值（不是启动时二次拒绝）。
    let err =
        serde_json::from_str::<MtlsCertAlgorithm>("\"SM2\"").expect_err("SM2 应在反序列化阶段报错");
    let msg = format!("{}", err);
    assert!(
        msg.contains("unknown variant") && msg.contains("SM2"),
        "错误信息应含 unknown variant + SM2，实际: {msg}"
    );
}

/// 完整 `SecurityMtlsConfig` 配置写 `cert_algorithm = "SM2"` → 反序列化阶段整体拒绝。
#[test]
fn qa_phase6_mtls_config_sm2_field_rejected_at_deserialize() {
    let toml_with_sm2 = r#"{
        "enabled": true,
        "ca_cert_path": "/tmp/ca.crt",
        "server_cert_path": "/tmp/server.crt",
        "server_key_path": "/tmp/server.key",
        "cert_algorithm": "SM2"
    }"#;
    let err = serde_json::from_str::<SecurityMtlsConfig>(toml_with_sm2)
        .expect_err("cert_algorithm=SM2 必须在反序列化阶段整体报错");
    assert!(
        format!("{}", err).contains("unknown variant"),
        "实际: {err}"
    );
}

/// 默认值：mTLS 关闭（enabled=false）+ 证书算法仅 Ed25519（裁决 #5 + phase33 D2）。
#[test]
fn qa_phase6_mtls_config_default_disabled_ed25519() {
    let cfg = SecurityMtlsConfig::default();
    assert!(
        !cfg.enabled,
        "mTLS 默认关闭（enabled=false 明文，phase33 D2）"
    );
    assert_eq!(cfg.cert_algorithm, MtlsCertAlgorithm::Ed25519);
}

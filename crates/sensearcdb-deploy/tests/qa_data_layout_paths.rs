//! QA 独立验证（qa-data-layout-verify）：数据目录布局治理 —— 路径解析与相对名逃逸拒绝。
//!
//! 独立于工程师自测，覆盖设计文档 `docs/data-layout-design.md` 的不变式：
//! - **I2/I4 派生 getter 正确性**：`sac_dir()/wal_dir()/snapshot_file()/config_db_path()/
//!   metadata_db_path()` 必须由唯一根 `data_dir` join 相对名得到。
//! - **I4 相对名不可逃逸**：`ConfigValidator` 必须拒绝绝对路径与含 `..` 的 `*_name`，
//!   允许正向多级子目录（如 `ssd/sac`）。

use sensearcdb_common::config::AppConfig;
use sensearcdb_deploy::config_validator::ConfigValidator;
use std::path::PathBuf;

/// 断言一个 `*_name` 字段触发「父目录逃逸」校验错误。
fn assert_rejects_parent_dir(field_setter: impl FnOnce(&mut AppConfig), field_name: &str) {
    let mut config = AppConfig::default();
    config.deploy.validation_mode = "warn".into(); // warn 模式：错误落 result.errors，不直接 Err
    field_setter(&mut config);

    let result = ConfigValidator::validate(&config).expect("warn 模式应返回 Ok");
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.contains(field_name) && e.contains("..")),
        "字段 {field_name} 含 '..' 应被拒绝，实际 errors={:?}",
        result.errors
    );
}

/// 断言一个 `*_name` 字段触发「绝对路径」校验错误。
fn assert_rejects_absolute(field_setter: impl FnOnce(&mut AppConfig), field_name: &str) {
    let mut config = AppConfig::default();
    config.deploy.validation_mode = "warn".into();
    field_setter(&mut config);

    let result = ConfigValidator::validate(&config).expect("warn 模式应返回 Ok");
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.contains(field_name) && e.contains("绝对路径")),
        "字段 {field_name} 绝对路径应被拒绝，实际 errors={:?}",
        result.errors
    );
}

/// 断言一个 `*_name` 字段触发「根路径/盘符」校验错误（`/evil` 含 RootDir，
/// 在 Windows 上 `is_absolute()=false` 但仍须拒绝；在 POSIX 上为绝对路径同样拒绝）。
fn assert_rejects_rootdir(field_setter: impl FnOnce(&mut AppConfig), field_name: &str) {
    let mut config = AppConfig::default();
    config.deploy.validation_mode = "warn".into();
    field_setter(&mut config);

    let result = ConfigValidator::validate(&config).expect("warn 模式应返回 Ok");
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.contains(field_name) && e.contains("绝对路径")),
        "字段 {field_name} 前导斜杠 /evil 应被拒绝，实际 errors={:?}",
        result.errors
    );
}

#[test]
fn qa_getters_join_under_data_dir() {
    let config = AppConfig::default();
    let p = &config.paths;

    assert_eq!(p.data_dir(), PathBuf::from("data"));
    assert_eq!(p.sac_dir(), PathBuf::from("data").join("sac"));
    assert_eq!(p.wal_dir(), PathBuf::from("data").join("wal"));
    assert_eq!(p.snapshot_dir(), PathBuf::from("data").join("snapshot"));
    assert_eq!(
        p.snapshot_file(),
        PathBuf::from("data").join("snapshot").join("snapshot.bin")
    );
    assert_eq!(p.metadata_dir(), PathBuf::from("data").join("metadata"));
    assert_eq!(
        p.config_db_path(),
        PathBuf::from("data").join("metadata").join("config.db")
    );
    assert_eq!(p.audit_dir(), PathBuf::from("data").join("audit"));
    assert_eq!(p.backup_dir(), PathBuf::from("data").join("backup"));
    assert_eq!(p.keys_dir(), PathBuf::from("data").join("keys"));
    assert_eq!(
        p.ai_ops_registry_db_path(),
        PathBuf::from("data")
            .join("metadata")
            .join("ai_ops_registry.db")
    );
    // metadata_db_path 是任意元库的复用入口。
    assert_eq!(
        p.metadata_db_path("asset.db"),
        PathBuf::from("data").join("metadata").join("asset.db")
    );
}

#[test]
fn qa_default_names_are_plain_relative_single_segment() {
    let p = &AppConfig::default().paths;
    // 默认相对名必须是最简单段，且绝无 `..` / 绝对路径（I4 的默认态）。
    for name in [
        &p.sac_dir_name,
        &p.wal_dir_name,
        &p.snapshot_dir_name,
        &p.snapshot_file_name,
        &p.metadata_dir_name,
        &p.config_db_name,
        &p.audit_dir_name,
        &p.backup_dir_name,
        &p.keys_dir_name,
        &p.ai_ops_registry_db_name,
        &p.ai_ops_blob_dir_name,
    ] {
        let path = std::path::Path::new(name);
        assert!(!path.is_absolute(), "默认相对名 {name:?} 不应为绝对路径");
        assert!(
            !path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir)),
            "默认相对名 {name:?} 不应含 '..'"
        );
    }
}

#[test]
fn qa_sac_dir_name_multi_level_stays_under_data_dir() {
    // I4 正向：多级子目录名合法，解析结果仍在 data_dir 下。
    let mut config = AppConfig::default();
    config.deploy.validation_mode = "warn".into();
    config.paths.sac_dir_name = "ssd/sac".into();

    let result = ConfigValidator::validate(&config).expect("多级相对名应合法");
    assert!(
        !result.errors.iter().any(|e| e.contains("sac_dir_name")),
        "多级相对名 'ssd/sac' 不应触发校验错误，实际 errors={:?}",
        result.errors
    );
    assert_eq!(
        config.paths.sac_dir(),
        PathBuf::from("data").join("ssd").join("sac"),
        "sac_dir() 必须仍落在 data_dir/ssd/sac"
    );
}

#[test]
fn qa_sac_dir_name_parent_dir_rejected() {
    assert_rejects_parent_dir(|c| c.paths.sac_dir_name = "../evil".into(), "sac_dir_name");
}

#[test]
fn qa_wal_dir_name_parent_dir_rejected() {
    assert_rejects_parent_dir(
        |c| c.paths.wal_dir_name = "a/../../evil".into(),
        "wal_dir_name",
    );
}

#[test]
fn qa_snapshot_dir_name_parent_dir_rejected() {
    assert_rejects_parent_dir(
        |c| c.paths.snapshot_dir_name = "..".into(),
        "snapshot_dir_name",
    );
}

#[test]
fn qa_metadata_dir_name_absolute_rejected() {
    // Windows 盘符形态（"D:/evil" 会被 Path::is_absolute() 识别为绝对路径并拒绝）。
    assert_rejects_absolute(
        |c| c.paths.metadata_dir_name = "D:/evil".into(),
        "metadata_dir_name",
    );
}

/// 固化「为什么必须拒绝 RootDir」的路径语义（已修复，见 config_validator.rs）：
/// `Path::new("/etc/evil").is_absolute()` 在 Windows 上为 **false**（无盘符前缀），
/// 但 `components()` 含 `RootDir`，且 `PathBuf::from("data").join("/etc/evil")`
/// 会得到 `/etc/evil`（逃逸 data_dir）。这正是校验器必须补查 `RootDir|Prefix` 的原因。
#[test]
fn qa_document_windows_rootdir_escape_semantics() {
    let p = std::path::Path::new("/etc/evil");
    assert!(
        !p.is_absolute(),
        "Windows 上 POSIX 风格根路径 is_absolute()=false（前提）"
    );
    assert!(
        p.components()
            .any(|c| matches!(c, std::path::Component::RootDir)),
        "但含 RootDir 组件（逃逸前提）"
    );
    assert_eq!(
        std::path::PathBuf::from("data").join("/etc/evil"),
        std::path::PathBuf::from("/etc/evil"),
        "join 后逃逸 data_dir（固化语义，供后续加固校验）"
    );
}

#[test]
fn qa_keys_dir_name_absolute_rejected() {
    // Windows 盘符形态（即使本机是 Windows，POSIX 根路径也要能识别）。
    assert_rejects_absolute(
        |c| c.paths.keys_dir_name = "C:/evil".into(),
        "keys_dir_name",
    );
}

#[test]
fn qa_ai_ops_registry_db_name_parent_dir_rejected() {
    assert_rejects_parent_dir(
        |c| c.paths.ai_ops_registry_db_name = "../evil.db".into(),
        "ai_ops_registry_db_name",
    );
}

/// 独立边界：RootDir（前导斜杠 `/evil`）拒绝必须覆盖**全部 11 个 `*_name` 字段**，
/// 而非仅 `sac_dir_name`。所有字段走同一循环，逐一设 `/evil` 断言被拒。
#[test]
fn qa_rootdir_rejected_across_all_name_fields() {
    let setters: [(&str, fn(&mut AppConfig)); 11] = [
        ("sac_dir_name", |c| c.paths.sac_dir_name = "/evil".into()),
        ("wal_dir_name", |c| c.paths.wal_dir_name = "/evil".into()),
        ("snapshot_dir_name", |c| {
            c.paths.snapshot_dir_name = "/evil".into()
        }),
        ("snapshot_file_name", |c| {
            c.paths.snapshot_file_name = "/evil".into()
        }),
        ("metadata_dir_name", |c| {
            c.paths.metadata_dir_name = "/evil".into()
        }),
        ("config_db_name", |c| {
            c.paths.config_db_name = "/evil".into()
        }),
        ("audit_dir_name", |c| {
            c.paths.audit_dir_name = "/evil".into()
        }),
        ("backup_dir_name", |c| {
            c.paths.backup_dir_name = "/evil".into()
        }),
        ("keys_dir_name", |c| c.paths.keys_dir_name = "/evil".into()),
        ("ai_ops_registry_db_name", |c| {
            c.paths.ai_ops_registry_db_name = "/evil".into()
        }),
        ("ai_ops_blob_dir_name", |c| {
            c.paths.ai_ops_blob_dir_name = "/evil".into()
        }),
    ];

    for (field, setter) in setters {
        assert_rejects_rootdir(setter, field);
    }
}

/// 独立边界：Windows 盘符**相对**路径（`C:evil`，`is_absolute()=false` 但含 `Prefix`）
/// 也必须被拒，且覆盖全部 11 个字段（专门命中 `Component::Prefix(_)` 分支）。
#[cfg(windows)]
#[test]
fn qa_windows_prefix_rejected_across_all_name_fields() {
    let setters: [(&str, fn(&mut AppConfig)); 11] = [
        ("sac_dir_name", |c| c.paths.sac_dir_name = "C:evil".into()),
        ("wal_dir_name", |c| c.paths.wal_dir_name = "C:evil".into()),
        ("snapshot_dir_name", |c| {
            c.paths.snapshot_dir_name = "C:evil".into()
        }),
        ("snapshot_file_name", |c| {
            c.paths.snapshot_file_name = "C:evil".into()
        }),
        ("metadata_dir_name", |c| {
            c.paths.metadata_dir_name = "C:evil".into()
        }),
        ("config_db_name", |c| {
            c.paths.config_db_name = "C:evil".into()
        }),
        ("audit_dir_name", |c| {
            c.paths.audit_dir_name = "C:evil".into()
        }),
        ("backup_dir_name", |c| {
            c.paths.backup_dir_name = "C:evil".into()
        }),
        ("keys_dir_name", |c| c.paths.keys_dir_name = "C:evil".into()),
        ("ai_ops_registry_db_name", |c| {
            c.paths.ai_ops_registry_db_name = "C:evil".into()
        }),
        ("ai_ops_blob_dir_name", |c| {
            c.paths.ai_ops_blob_dir_name = "C:evil".into()
        }),
    ];

    for (field, setter) in setters {
        assert_rejects_rootdir(setter, field);
    }
}

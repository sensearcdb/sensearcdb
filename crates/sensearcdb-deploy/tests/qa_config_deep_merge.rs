//! QA 独立回归验证（阶段 2 · 改动 A）：配置 6 级字段级深度合并（GAP6-DEP-03）。
//!
//! 验证字段级（非 section 级整体替换）合并：
//! 只覆盖深层叶子字段时，同级其它字段与其它 section 字段必须保留默认值。

use sensearcdb_deploy::bootstrap::BootstrapArgs;
use sensearcdb_deploy::ConfigLoader;

struct Args {
    config: Option<String>,
}

impl BootstrapArgs for Args {
    fn config_path(&self) -> Option<&str> {
        self.config.as_deref()
    }
}

/// 三级深度（security → auth → jwt）只覆盖 `access_token_ttl_sec`，
/// 其余叶子字段保留默认值（算法名、refresh TTL、密钥路径等）。
#[test]
fn qa_config_three_level_deep_merge() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("custom.toml");
    std::fs::write(&path, "[security.auth.jwt]\naccess_token_ttl_sec = 12345\n").unwrap();

    let args = Args {
        config: Some(path.to_string_lossy().into_owned()),
    };
    let (cfg, _source) = ConfigLoader::load(&args).unwrap();

    // 被覆盖的叶子字段
    assert_eq!(cfg.security.auth.jwt.access_token_ttl_sec, 12_345);
    // 同级叶子字段保留默认值（未被 section 级替换）
    assert_eq!(cfg.security.auth.jwt.algorithm, "Ed25519");
    assert_eq!(cfg.security.auth.jwt.refresh_token_ttl_sec, 86_400);
    assert_eq!(
        cfg.security.auth.jwt.signing_key_path,
        "/etc/sensearc/jwt/signing.key"
    );
    assert_eq!(cfg.security.auth.jwt.key_rotation_interval_days, 30);
}

/// 跨 section 隔离：只覆盖 `[paths].data_dir`，engine / partition 等其它 section 保持默认。
#[test]
fn qa_config_cross_section_preserved() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("custom.toml");
    std::fs::write(&path, "[paths]\ndata_dir = \"/custom/data\"\n").unwrap();

    let args = Args {
        config: Some(path.to_string_lossy().into_owned()),
    };
    let (cfg, _source) = ConfigLoader::load(&args).unwrap();

    // 被覆盖字段
    assert_eq!(cfg.paths.data_dir, "/custom/data");
    // paths 同 section 其它字段保留默认
    assert_eq!(cfg.paths.wal_dir_name, "wal");
    assert_eq!(cfg.paths.sac_dir_name, "sac");
    assert_eq!(cfg.paths.snapshot_file_name, "snapshot.bin");
    assert_eq!(cfg.paths.config_db_name, "config.db");
    // 跨 section 字段保留默认
    assert_eq!(cfg.engine.freeze_threshold, 460_000);
    assert_eq!(cfg.partition.duration_ms, 86_400_000);
    assert_eq!(cfg.retention.default_retention, "90d");
}

//! QA 独立验证（阶段 7 T1-1）—— ClusterConfig 新结构反序列化 + node_id 严格解析语义。
//!
//! 对照 PM 裁决 `phase7-t1-singlegroup-multinode-ruling.md` 拍板点 ①②④：
//! - `peers: Vec<String>` 已删除，替换为 `bootstrap_members: Vec<ClusterPeerConfig>` + `join_addr: Option<String>`。
//! - `ClusterPeerConfig { node_id: u64, raft_addr: String, api_addr: Option<String> }`。
//! - 本节点 `cluster.node_id` 必须严格匹配 `node-<number>`，任何非该形式的命名在启动时
//!   报错（删除 `unwrap_or(1)` 静默兜底）。
//!
//! 本文件不依赖工程师自测，走真实 `AppConfig::load` 反序列化路径验证 TOML 加载，
//! 并锁死 node_id 严格解析语义（语义镜像 startup.rs 的解析表达式，防回归 `unwrap_or(1)`）。

use std::path::PathBuf;

use sensearcdb_common::AppConfig;

/// 生成进程内唯一的临时 TOML 文件路径（避免并行测试碰撞）。
fn temp_toml_path(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "qa_t1_cluster_config_{}_{}_{}.toml",
        tag,
        std::process::id(),
        nanos
    ))
}

/// 写临时 TOML 文件，返回路径；drop 时由调用方负责清理。
fn write_temp_toml(tag: &str, content: &str) -> PathBuf {
    let path = temp_toml_path(tag);
    std::fs::write(&path, content).expect("write temp toml");
    path
}

/// 清理临时文件（测试尾部调用，失败不 panic）。
fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
}

// ─── 1. bootstrap_members / join_addr 的 TOML 反序列化 ────────────────────────

#[test]
fn qa_t1_bootstrap_members_and_join_addr_deserialize_from_toml() {
    let path = write_temp_toml(
        "bootstrap",
        r#"
[cluster]
enabled = true
node_id = "node-02"
bootstrap_members = [
  { node_id = 1, raft_addr = "127.0.0.1:5300", api_addr = "127.0.0.1:5100" },
  { node_id = 2, raft_addr = "127.0.0.1:5301" },
]
join_addr = "127.0.0.1:5300"
"#,
    );

    let cfg = AppConfig::load(path.to_str().unwrap()).expect("load config");
    cleanup(&path);

    let cluster = &cfg.cluster;

    // node_id String 保留（严格解析在 startup 派生 u64）。
    assert_eq!(cluster.node_id, "node-02");

    // bootstrap_members 逐项精确反序列化。
    assert_eq!(cluster.bootstrap_members.len(), 2);
    let m0 = &cluster.bootstrap_members[0];
    assert_eq!(m0.node_id, 1);
    assert_eq!(m0.raft_addr, "127.0.0.1:5300");
    assert_eq!(m0.api_addr.as_deref(), Some("127.0.0.1:5100"));

    // 缺省 api_addr 时 `#[serde(default)]` 生效 → None（关键：api_addr 可省略）。
    let m1 = &cluster.bootstrap_members[1];
    assert_eq!(m1.node_id, 2);
    assert_eq!(m1.raft_addr, "127.0.0.1:5301");
    assert_eq!(m1.api_addr, None, "api_addr 省略应反序列化为 None");

    // join_addr 正确加载。
    assert_eq!(cluster.join_addr.as_deref(), Some("127.0.0.1:5300"));
}

#[test]
fn qa_t1_bootstrap_members_empty_and_join_addr_absent_by_default() {
    // 未显式给出 cluster 段时，默认值：bootstrap_members 空、join_addr None（单节点分支）。
    let path = write_temp_toml("defaults", "[cluster]\nenabled = false\n");

    let cfg = AppConfig::load(path.to_str().unwrap()).expect("load config");
    cleanup(&path);

    assert!(cfg.cluster.bootstrap_members.is_empty());
    assert!(cfg.cluster.join_addr.is_none());
}

// ─── 2. node_id 严格解析语义（语义镜像 startup.rs:1596-1612）───────────────────
//
// 注：真实解析在 `src/startup.rs`（二进制 crate，非 lib，无法直接单测）。
// 此处以「语义镜像」锁死契约：`node-<数字>` 通过、任何其它形式拒绝，
// 从测试层面防「`trim_start_matches(!digit).parse().unwrap_or(1)`」这类静默兜底回归。

/// 与 startup.rs:1596-1612 完全一致的严格派生逻辑。
fn strict_parse_node_id(raw: &str) -> Option<u64> {
    raw.strip_prefix("node-").and_then(|digits| {
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            None
        } else {
            digits.parse::<u64>().ok()
        }
    })
}

#[test]
fn qa_t1_node_id_strict_parse_accepts_node_number() {
    assert_eq!(strict_parse_node_id("node-01"), Some(1));
    assert_eq!(strict_parse_node_id("node-1"), Some(1));
    assert_eq!(strict_parse_node_id("node-42"), Some(42));
    assert_eq!(strict_parse_node_id("node-00000042"), Some(42));
}

#[test]
fn qa_t1_node_id_strict_parse_rejects_non_node_number() {
    // 任何非 `node-<number>` 命名都必须被拒绝（启动报错），而非静默撞 id=1。
    assert_eq!(strict_parse_node_id("node-x"), None);
    assert_eq!(strict_parse_node_id("abc"), None);
    assert_eq!(strict_parse_node_id("node-"), None); // 空前缀后的空 digits
    assert_eq!(strict_parse_node_id("node-01a"), None); // 数字后带杂质
    assert_eq!(strict_parse_node_id("node- 01"), None);
    assert_eq!(strict_parse_node_id("Node-01"), None); // 大小写敏感
    assert_eq!(strict_parse_node_id("node--1"), None);
}

//! systemd service 单元文件模板。
//!
//! 提供 `sensearcdb.service` 模板内容，供安装脚本使用。

/// 返回 sensearcdb.service 模板内容。
pub fn service_template() -> &'static str {
    r#"[Unit]
Description=SenseArcDB High-Performance Real-Time Time-Series Database
Documentation=https://docs.sensearcdb.com
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
User=sensearcdb
Group=sensearcdb
ExecStart=/usr/bin/sensearcdb
ExecReload=/bin/kill -HUP $MAINPID
ExecStop=/bin/kill -TERM $MAINPID
Restart=on-failure
RestartSec=5s

# 资源限制
LimitNOFILE=65536
LimitNPROC=4096
MemoryHigh=8G
MemoryMax=10G

# 安全加固
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/sensearcdb /var/log/sensearcdb
ReadOnlyPaths=/etc/sensearcdb

# 环境变量
Environment="SENSEARCDB_DATA_DIR=/var/lib/sensearcdb"
Environment="RUST_LOG=info"

# 超时设置
TimeoutStartSec=60s
TimeoutStopSec=30s

[Install]
WantedBy=multi-user.target
"#
}

/// 返回推荐的 systemd drop-in 目录路径。
pub fn dropin_dir() -> &'static str {
    "/etc/systemd/system/sensearcdb.service.d"
}

/// 返回推荐的 override.conf 模板。
pub fn override_template() -> &'static str {
    r#"[Service]
# 在此覆盖默认配置
# Environment="SENSEARCDB_CONFIG=/etc/sensearcdb/production.toml"
# Environment="RUST_LOG=debug"
"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_template_contains_required_sections() {
        let tmpl = service_template();
        assert!(tmpl.contains("[Unit]"));
        assert!(tmpl.contains("[Service]"));
        assert!(tmpl.contains("[Install]"));
        assert!(tmpl.contains("Type=notify"));
        assert!(tmpl.contains("sensearcdb"));
    }
}

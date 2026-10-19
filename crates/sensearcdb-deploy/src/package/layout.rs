//! 安装包目录结构定义。
//!
//! 定义各平台安装包的标准化目录布局。

use super::PackageType;

/// 安装包目录布局。
#[derive(Debug, Clone)]
pub struct PackageLayout {
    /// 包类型
    pub package_type: PackageType,
    /// 架构 (x86_64, aarch64)
    pub arch: String,
    /// 二进制安装路径
    pub bin_dir: String,
    /// 配置文件路径
    pub config_dir: String,
    /// 数据目录路径
    pub data_dir: String,
    /// 日志目录路径
    pub log_dir: String,
    /// systemd service 路径 (仅 Linux)
    pub service_path: Option<String>,
}

impl PackageLayout {
    /// 为目标平台和包类型创建布局。
    pub fn new(package_type: PackageType, arch: &str) -> Self {
        match package_type {
            PackageType::TarGz | PackageType::Deb | PackageType::Rpm | PackageType::Docker => {
                Self {
                    package_type,
                    arch: arch.to_string(),
                    bin_dir: "/usr/bin".into(),
                    config_dir: "/etc/sensearcdb".into(),
                    data_dir: "/var/lib/sensearcdb".into(),
                    log_dir: "/var/log/sensearcdb".into(),
                    service_path: Some("/etc/systemd/system/sensearcdb.service".into()),
                }
            }
            PackageType::Msi | PackageType::Zip => Self {
                package_type,
                arch: arch.to_string(),
                bin_dir: r"C:\Program Files\SenseArcDB\bin".into(),
                config_dir: r"C:\ProgramData\SenseArcDB".into(),
                data_dir: r"C:\ProgramData\SenseArcDB\data".into(),
                log_dir: r"C:\ProgramData\SenseArcDB\logs".into(),
                service_path: None,
            },
        }
    }

    /// 返回安装包文件名。
    pub fn filename(&self, version: &str) -> String {
        let os = self.package_type.platform();
        let ext = self.package_type.name();
        match self.package_type {
            PackageType::Docker => format!("sensearcdb-{}.tar", version),
            _ => format!("sensearcdb-{}-{}-{}.{}", version, os, self.arch, ext),
        }
    }

    /// 返回所有需要创建的目录列表。
    pub fn directories(&self) -> Vec<&str> {
        let dirs = vec![
            self.bin_dir.as_str(),
            self.config_dir.as_str(),
            self.data_dir.as_str(),
            self.log_dir.as_str(),
        ];
        let _ = dirs;
        if let Some(ref sp) = self.service_path {
            let _parent = std::path::Path::new(sp).parent();
        }
        dirs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linux_deb_layout() {
        let layout = PackageLayout::new(PackageType::Deb, "amd64");
        assert_eq!(layout.bin_dir, "/usr/bin");
        assert_eq!(layout.config_dir, "/etc/sensearcdb");
        assert!(layout.service_path.is_some());
    }

    #[test]
    fn test_windows_zip_layout() {
        let layout = PackageLayout::new(PackageType::Zip, "x86_64");
        assert!(layout.bin_dir.contains("Program Files"));
        assert!(layout.service_path.is_none());
    }

    #[test]
    fn test_filename_format() {
        let layout = PackageLayout::new(PackageType::TarGz, "x86_64");
        let name = layout.filename("1.0.0");
        assert!(name.contains("1.0.0"));
        assert!(name.contains("tar.gz"));
    }

    #[test]
    fn test_docker_filename() {
        let layout = PackageLayout::new(PackageType::Docker, "x86_64");
        let name = layout.filename("1.0.0");
        assert!(name.ends_with(".tar"));
    }
}

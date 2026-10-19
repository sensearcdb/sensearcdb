//! 安装包目录结构定义。
//!
//! 定义 SenseArcDB 安装包的标准化目录布局，
//! 供 DEB/RPM/MSI/tar.gz/Docker 等打包脚本使用。

pub mod layout;

/// 安装包类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageType {
    /// tar.gz — Linux 通用
    TarGz,
    /// DEB — Debian/Ubuntu
    Deb,
    /// RPM — RHEL/CentOS/Fedora
    Rpm,
    /// MSI — Windows
    Msi,
    /// ZIP — Windows 绿色版
    Zip,
    /// Docker — 容器镜像
    Docker,
}

impl PackageType {
    /// 返回包类型名称。
    pub fn name(&self) -> &'static str {
        match self {
            PackageType::TarGz => "tar.gz",
            PackageType::Deb => "deb",
            PackageType::Rpm => "rpm",
            PackageType::Msi => "msi",
            PackageType::Zip => "zip",
            PackageType::Docker => "docker",
        }
    }

    /// 返回目标平台。
    pub fn platform(&self) -> &'static str {
        match self {
            PackageType::TarGz | PackageType::Deb | PackageType::Rpm | PackageType::Docker => {
                "linux"
            }
            PackageType::Msi | PackageType::Zip => "windows",
        }
    }
}

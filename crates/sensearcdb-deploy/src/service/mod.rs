//! 服务管理子模块。

pub mod systemd_unit;

#[cfg(windows)]
pub mod windows_service;
#[cfg(not(windows))]
pub mod windows_service {
    //! Windows Service 占位（非 Windows 平台）。
    pub fn run_service(_service_name: &str) -> Result<(), String> {
        Err("Windows Service 仅支持 Windows 平台".into())
    }
}

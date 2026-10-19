//! Windows Service 控制。
//!
//! 使用 `windows-service` crate 实现 Windows 服务集成。
//! 支持 `sc start/stop` 等标准服务管理命令。

#[cfg(windows)]
mod inner {
    use std::ffi::OsString;
    use std::sync::Mutex;
    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::service_dispatcher;

    /// 全局停止回调。
    static ON_STOP: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);
    /// 全局启动回调。
    static ON_START: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

    /// 运行 Windows Service。
    pub fn run_service<F, G>(service_name: &str, on_start: F, on_stop: G) -> Result<(), String>
    where
        F: FnOnce() + Send + 'static,
        G: FnOnce() + Send + 'static,
    {
        // 存储回调到全局静态变量
        {
            let mut guard = ON_STOP.lock().map_err(|e| format!("Lock error: {}", e))?;
            *guard = Some(Box::new(on_stop));
        }
        {
            let mut guard = ON_START.lock().map_err(|e| format!("Lock error: {}", e))?;
            *guard = Some(Box::new(on_start));
        }

        let name = OsString::from(service_name);

        // 定义外部 C 函数作为 service_main
        extern "system" fn service_main(_argc: u32, _argv: *mut *mut u16) {
            // 注册控制处理器
            let event_handler = move |control_event| -> ServiceControlHandlerResult {
                match control_event {
                    ServiceControl::Stop | ServiceControl::Shutdown => {
                        let mut guard = ON_STOP.lock().unwrap();
                        if let Some(on_stop) = guard.take() {
                            on_stop();
                        }
                        ServiceControlHandlerResult::NoError
                    }
                    ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                    _ => ServiceControlHandlerResult::NotImplemented,
                }
            };

            // 注册控制处理器并获取状态句柄
            match service_control_handler::register("SenseArcDB", event_handler) {
                Ok(status_handle) => {
                    // 通知 SCM 服务正在启动
                    let _ = status_handle.set_service_status(ServiceStatus {
                        service_type: ServiceType::OWN_PROCESS,
                        current_state: ServiceState::Running,
                        controls_accepted: ServiceControlAccept::STOP,
                        exit_code: ServiceExitCode::Win32(0),
                        checkpoint: 0,
                        wait_hint: std::time::Duration::from_secs(0),
                        process_id: None,
                    });

                    // 执行启动回调
                    let mut guard = ON_START.lock().unwrap();
                    if let Some(on_start) = guard.take() {
                        on_start();
                    }
                }
                Err(e) => {
                    eprintln!("Failed to register service control handler: {:?}", e);
                }
            }
        }

        // 启动服务分发器
        service_dispatcher::start(&name, service_main)
            .map_err(|e| format!("Service dispatcher error: {:?}", e))?;

        Ok(())
    }
}

#[cfg(windows)]
pub use inner::run_service;

#[cfg(not(windows))]
pub fn run_service<F, G>(_service_name: &str, _on_start: F, _on_stop: G) -> Result<(), String>
where
    F: FnOnce() + Send + 'static,
    G: FnOnce() + Send + 'static,
{
    Err("Windows Service 仅支持 Windows 平台".into())
}

//! systemd sd_notify 集成。
//!
//! 在 Linux + systemd `Type=notify` 模式下：
//! - 启动完成后调用 `notify_ready()` → sd_notify("READY=1")
//! - 开始关闭时调用 `notify_stopping()` → sd_notify("STOPPING=1")
//!
//! 非 Linux 平台或 systemd 不可用时，这些函数为 no-op。

/// 通知 systemd 服务已就绪。
pub fn notify_ready() {
    notify(&[NotifyMsg::Ready]);
}

/// 通知 systemd 服务正在停止。
pub fn notify_stopping() {
    notify(&[NotifyMsg::Stopping]);
}

/// 发送状态消息给 systemd。
pub fn notify_status(status: &str) {
    notify(&[NotifyMsg::Status(status.into())]);
}

/// 通知消息类型。
#[derive(Debug, Clone)]
enum NotifyMsg {
    Ready,
    Stopping,
    Status(String),
}

/// 底层 sd_notify 调用。
#[cfg(target_os = "linux")]
fn notify(msgs: &[NotifyMsg]) {
    if std::env::var("NOTIFY_SOCKET").is_err() {
        return;
    }

    let states: Vec<sd_notify::NotifyState> = msgs
        .iter()
        .map(|m| match m {
            NotifyMsg::Ready => sd_notify::NotifyState::Ready,
            NotifyMsg::Stopping => sd_notify::NotifyState::Stopping,
            NotifyMsg::Status(s) => sd_notify::NotifyState::Status(s),
        })
        .collect();

    if let Err(e) = sd_notify::notify(false, &states) {
        tracing::warn!("sd_notify 失败: {}", e);
    } else {
        tracing::debug!("sd_notify: {:?}", msgs);
    }
}

/// 非 Linux 平台的 no-op 实现。
#[cfg(not(target_os = "linux"))]
fn notify(msgs: &[NotifyMsg]) {
    tracing::debug!("sd_notify (no-op): {:?}", msgs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notify_no_panic() {
        notify_ready();
        notify_stopping();
        notify_status("test");
    }
}

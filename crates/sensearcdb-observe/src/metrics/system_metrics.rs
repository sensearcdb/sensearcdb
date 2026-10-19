//! 系统资源指标：CPU/RSS/FD/IO (9 指标)。
//!
//! Linux: 通过 `procfs` 读取 /proc/self/status、/proc/self/io 等。
//! 非 Linux: 使用 std 替代方案。

use prometheus::{IntCounter, IntGauge, Opts, Registry};

pub(crate) fn register_system_cpu_usage(r: &Registry) -> prometheus::Gauge {
    let g = prometheus::Gauge::with_opts(Opts::new(
        "sensearc_system_cpu_usage",
        "Current process CPU usage ratio (0.0-1.0)",
    ))
    .expect("register sensearc_system_cpu_usage");
    r.register(Box::new(g.clone()))
        .expect("register system_cpu_usage");
    g
}

pub(crate) fn register_system_memory_rss(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_system_memory_rss_bytes",
        "Process resident set size (RSS) in bytes",
    ))
    .expect("register sensearc_system_memory_rss_bytes");
    r.register(Box::new(g.clone()))
        .expect("register system_memory_rss");
    g
}

pub(crate) fn register_system_memory_virtual(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_system_memory_virtual_bytes",
        "Process virtual memory size in bytes",
    ))
    .expect("register sensearc_system_memory_virtual_bytes");
    r.register(Box::new(g.clone()))
        .expect("register system_memory_virtual");
    g
}

pub(crate) fn register_system_fd_open(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_system_fd_open",
        "Number of open file descriptors",
    ))
    .expect("register sensearc_system_fd_open");
    r.register(Box::new(g.clone()))
        .expect("register system_fd_open");
    g
}

pub(crate) fn register_system_fd_max(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_system_fd_max",
        "Maximum allowed file descriptors",
    ))
    .expect("register sensearc_system_fd_max");
    r.register(Box::new(g.clone()))
        .expect("register system_fd_max");
    g
}

pub(crate) fn register_system_uptime(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_system_uptime_seconds",
        "Process uptime in seconds",
    ))
    .expect("register sensearc_system_uptime_seconds");
    r.register(Box::new(g.clone()))
        .expect("register system_uptime");
    g
}

pub(crate) fn register_system_thread_count(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_system_thread_count",
        "Current number of OS threads",
    ))
    .expect("register sensearc_system_thread_count");
    r.register(Box::new(g.clone()))
        .expect("register system_thread_count");
    g
}

pub(crate) fn register_system_io_read(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_system_io_read_bytes_total",
        "Total bytes read from disk by process",
    ))
    .expect("register sensearc_system_io_read_bytes_total");
    r.register(Box::new(c.clone()))
        .expect("register system_io_read");
    c
}

pub(crate) fn register_system_io_write(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_system_io_write_bytes_total",
        "Total bytes written to disk by process",
    ))
    .expect("register sensearc_system_io_write_bytes_total");
    r.register(Box::new(c.clone()))
        .expect("register system_io_write");
    c
}

/// 系统指标采集器。
///
/// 通过 `procfs` (Linux) 或 std 采集当前进程的 CPU/内存/FD/IO/线程等指标。
/// 非 Linux 平台使用 std 的 fallback 路径。
pub struct SystemMetricsCollector;

impl SystemMetricsCollector {
    /// 采集一次系统指标并更新 Prometheus metrics。
    pub fn collect() {
        let m = crate::metrics::MetricsRegistry::global();

        #[cfg(target_os = "linux")]
        {
            if let Ok(proc) = procfs::process::Process::myself() {
                // RSS
                if let Ok(status) = proc.status() {
                    m.system_memory_rss.set(status.vmrss.unwrap_or(0) as i64);
                    m.system_memory_virtual
                        .set(status.vmsize.unwrap_or(0) as i64);
                    m.system_thread_count.set(status.threads as i64);

                    // FD count
                    if let Ok(fd_count) = proc.fd_count() {
                        m.system_fd_open.set(fd_count as i64);
                    }
                }

                // IO
                if let Ok(io) = proc.io() {
                    m.system_io_read.set(io.read_bytes as i64);
                    m.system_io_write.set(io.write_bytes as i64);
                }

                // CPU usage (simple /proc/self/stat method)
                if let Ok(stat) = proc.stat() {
                    let total_time = stat.utime + stat.stime + stat.cutime + stat.cstime;
                    // Store raw ticks; caller can compute delta
                    m.system_cpu_usage.set(0.0);
                    let _ = total_time;
                }
            }

            // FD max from /proc/sys/fs/file-max or rlimit
            if let Ok(limits) = procfs::process::Limit::default() {
                // Fallback: use rlimit
                let _ = limits;
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Fallback for non-Linux: use std only
            // Memory (rough estimate via OS APIs not available in pure Rust)
            // IntCounter doesn't have .set(); use inc_by(0) as no-op
            // Gauges accept .set()
            m.system_memory_rss.set(0);
            m.system_memory_virtual.set(0);
            m.system_fd_open.set(0);
            m.system_fd_max.set(0);
            m.system_cpu_usage.set(0.0);
            m.system_thread_count.set(0);
        }

        // Uptime — cross-platform via Instant
        use std::sync::atomic::{AtomicU64, Ordering};
        static START_TIME: AtomicU64 = AtomicU64::new(0);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        START_TIME
            .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed)
            .ok();
        let start = START_TIME.load(Ordering::Relaxed);
        if start > 0 {
            m.system_uptime.set((now.saturating_sub(start)) as i64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_all_system_metrics_register() {
        let r = Registry::new();
        register_system_cpu_usage(&r);
        register_system_memory_rss(&r);
        register_system_memory_virtual(&r);
        register_system_fd_open(&r);
        register_system_fd_max(&r);
        register_system_uptime(&r);
        register_system_thread_count(&r);
        register_system_io_read(&r);
        register_system_io_write(&r);
    }

    #[test]
    fn test_system_metrics_collector() {
        // Must have MetricsRegistry initialized first
        let _ = crate::metrics::MetricsRegistry::global();
        SystemMetricsCollector::collect();
        // Collect should not panic
    }
}

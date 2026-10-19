//! 日志与构建信息指标：log_messages, slow_query_count/dropped, scrape_timeout, build_info (4 指标)。

use prometheus::{IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry};

pub(crate) fn register_log_messages(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new("sensearc_log_messages_total", "Total log messages by level"),
        &["level"],
    )
    .expect("register sensearc_log_messages_total");
    r.register(Box::new(v.clone()))
        .expect("register log_messages");
    v
}

pub(crate) fn register_slow_query_count(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_slow_query_count_total",
        "Total number of slow queries logged",
    ))
    .expect("register sensearc_slow_query_count_total");
    r.register(Box::new(c.clone()))
        .expect("register slow_query_count");
    c
}

pub(crate) fn register_slow_query_dropped(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_slow_query_dropped_total",
        "Total slow query logs dropped (channel full)",
    ))
    .expect("register sensearc_slow_query_dropped_total");
    r.register(Box::new(c.clone()))
        .expect("register slow_query_dropped");
    c
}

pub(crate) fn register_scrape_timeout_total(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_scrape_timeout_total",
        "Total /metrics scrape timeouts",
    ))
    .expect("register sensearc_scrape_timeout_total");
    r.register(Box::new(c.clone()))
        .expect("register scrape_timeout_total");
    c
}

pub(crate) fn register_build_info(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new("sensearc_build_info", "SenseArcDB build information"),
        &["version", "commit", "build_date", "rust_version"],
    )
    .expect("register sensearc_build_info");
    r.register(Box::new(v.clone()))
        .expect("register build_info");
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_all_log_metrics_register() {
        let r = Registry::new();
        register_log_messages(&r);
        register_slow_query_count(&r);
        register_slow_query_dropped(&r);
        register_scrape_timeout_total(&r);
        register_build_info(&r);
    }
}

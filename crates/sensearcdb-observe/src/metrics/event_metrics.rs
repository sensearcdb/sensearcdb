//! 事件框架指标：发布/限流/SSE连接/Store lag (5 指标)。

use prometheus::{IntCounterVec, IntGauge, Opts, Registry};

pub(crate) fn register_event_published(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_event_published_total",
            "Total events published by category",
        ),
        &["category"],
    )
    .expect("register sensearc_event_published_total");
    r.register(Box::new(v.clone()))
        .expect("register event_published");
    v
}

pub(crate) fn register_event_throttled(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_event_throttled_total",
            "Total events throttled by category",
        ),
        &["category"],
    )
    .expect("register sensearc_event_throttled_total");
    r.register(Box::new(v.clone()))
        .expect("register event_throttled");
    v
}

pub(crate) fn register_event_channel_capacity_ratio(r: &Registry) -> prometheus::Gauge {
    let g = prometheus::Gauge::with_opts(Opts::new(
        "sensearc_event_channel_capacity_ratio",
        "Event bus channel capacity usage ratio (0.0-1.0)",
    ))
    .expect("register sensearc_event_channel_capacity_ratio");
    r.register(Box::new(g.clone()))
        .expect("register event_channel_capacity_ratio");
    g
}

pub(crate) fn register_event_sse_connections(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_event_sse_connections",
        "Current number of SSE connections",
    ))
    .expect("register sensearc_event_sse_connections");
    r.register(Box::new(g.clone()))
        .expect("register event_sse_connections");
    g
}

pub(crate) fn register_event_store_lag(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_event_store_lag",
        "Event store lag (events buffered but not persisted)",
    ))
    .expect("register sensearc_event_store_lag");
    r.register(Box::new(g.clone()))
        .expect("register event_store_lag");
    g
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_all_event_metrics_register() {
        let r = Registry::new();
        register_event_published(&r);
        register_event_throttled(&r);
        register_event_channel_capacity_ratio(&r);
        register_event_sse_connections(&r);
        register_event_store_lag(&r);
    }
}

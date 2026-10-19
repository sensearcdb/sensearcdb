//! 集群指标：Raft term/commit/lag/election/heartbeat (8 指标)。

use prometheus::{HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts, Registry};

pub(crate) fn register_cluster_node_state(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new(
            "sensearc_cluster_node_state",
            "Cluster node state (0=follower, 1=candidate, 2=leader)",
        ),
        &["node_id", "role"],
    )
    .expect("register sensearc_cluster_node_state");
    r.register(Box::new(v.clone()))
        .expect("register cluster_node_state");
    v
}

pub(crate) fn register_cluster_term(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new("sensearc_cluster_term", "Current Raft term"))
        .expect("register sensearc_cluster_term");
    r.register(Box::new(g.clone()))
        .expect("register cluster_term");
    g
}

pub(crate) fn register_cluster_committed_index(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_cluster_committed_index",
        "Last committed Raft log index",
    ))
    .expect("register sensearc_cluster_committed_index");
    r.register(Box::new(g.clone()))
        .expect("register cluster_committed_index");
    g
}

pub(crate) fn register_cluster_last_applied(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_cluster_last_applied",
        "Last applied Raft log index",
    ))
    .expect("register sensearc_cluster_last_applied");
    r.register(Box::new(g.clone()))
        .expect("register cluster_last_applied");
    g
}

pub(crate) fn register_cluster_replication_lag(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new(
            "sensearc_cluster_replication_lag",
            "Replication lag by node (log index difference)",
        ),
        &["node_id"],
    )
    .expect("register sensearc_cluster_replication_lag");
    r.register(Box::new(v.clone()))
        .expect("register cluster_replication_lag");
    v
}

pub(crate) fn register_cluster_replication_latency(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_cluster_replication_latency_seconds",
            "Replication latency in seconds",
        )
        .buckets(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
        ]),
    )
    .expect("register sensearc_cluster_replication_latency_seconds");
    r.register(Box::new(h.clone()))
        .expect("register cluster_replication_latency");
    h
}

pub(crate) fn register_cluster_leader_elections(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_cluster_leader_elections_total",
        "Total number of leader elections",
    ))
    .expect("register sensearc_cluster_leader_elections_total");
    r.register(Box::new(c.clone()))
        .expect("register cluster_leader_elections");
    c
}

pub(crate) fn register_cluster_heartbeat_latency(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_cluster_heartbeat_latency_seconds",
            "Raft heartbeat latency in seconds",
        )
        .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
    )
    .expect("register sensearc_cluster_heartbeat_latency_seconds");
    r.register(Box::new(h.clone()))
        .expect("register cluster_heartbeat_latency");
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_all_cluster_metrics_register() {
        let r = Registry::new();
        register_cluster_node_state(&r);
        register_cluster_term(&r);
        register_cluster_committed_index(&r);
        register_cluster_last_applied(&r);
        register_cluster_replication_lag(&r);
        register_cluster_replication_latency(&r);
        register_cluster_leader_elections(&r);
        register_cluster_heartbeat_latency(&r);
    }
}

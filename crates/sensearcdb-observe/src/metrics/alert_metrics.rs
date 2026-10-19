//! 告警引擎指标：评估/触发/通知/DLQ (6 指标)。

use prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

pub(crate) fn register_alert_evaluations(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_alert_evaluations_total",
            "Total alert rule evaluations by evaluator",
        ),
        &["evaluator"],
    )
    .expect("register sensearc_alert_evaluations_total");
    r.register(Box::new(v.clone()))
        .expect("register alert_evaluations");
    v
}

pub(crate) fn register_alert_fired(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_alert_fired_total",
            "Total alerts fired by severity level",
        ),
        &["level"],
    )
    .expect("register sensearc_alert_fired_total");
    r.register(Box::new(v.clone()))
        .expect("register alert_fired");
    v
}

pub(crate) fn register_alert_active_count(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new(
            "sensearc_alert_active_count",
            "Currently active alerts by severity",
        ),
        &["level"],
    )
    .expect("register sensearc_alert_active_count");
    r.register(Box::new(v.clone()))
        .expect("register alert_active_count");
    v
}

pub(crate) fn register_alert_notification_sent(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_alert_notification_sent_total",
            "Total alert notifications sent by channel",
        ),
        &["channel"],
    )
    .expect("register sensearc_alert_notification_sent_total");
    r.register(Box::new(v.clone()))
        .expect("register alert_notification_sent");
    v
}

pub(crate) fn register_alert_notification_failed(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_alert_notification_failed_total",
            "Total alert notification failures by channel",
        ),
        &["channel"],
    )
    .expect("register sensearc_alert_notification_failed_total");
    r.register(Box::new(v.clone()))
        .expect("register alert_notification_failed");
    v
}

pub(crate) fn register_alert_dlq_backlog(r: &Registry) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(
        "sensearc_alert_dlq_backlog",
        "Alert dead letter queue backlog count",
    ))
    .expect("register sensearc_alert_dlq_backlog");
    r.register(Box::new(g.clone()))
        .expect("register alert_dlq_backlog");
    g
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_all_alert_metrics_register() {
        let r = Registry::new();
        register_alert_evaluations(&r);
        register_alert_fired(&r);
        register_alert_active_count(&r);
        register_alert_notification_sent(&r);
        register_alert_notification_failed(&r);
        register_alert_dlq_backlog(&r);
    }
}

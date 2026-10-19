//! AI 模块指标：推理/引擎状态/MAPE/OOD (9 指标)。

use prometheus::{HistogramOpts, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry};

pub(crate) fn register_ai_inference_requests(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_ai_inference_requests_total",
            "Total AI inference requests by engine",
        ),
        &["engine"],
    )
    .expect("register sensearc_ai_inference_requests_total");
    r.register(Box::new(v.clone()))
        .expect("register ai_inference_requests");
    v
}

pub(crate) fn register_ai_inference_latency(r: &Registry) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "sensearc_ai_inference_latency_seconds",
            "AI inference latency in seconds",
        )
        .buckets(vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]),
    )
    .expect("register sensearc_ai_inference_latency_seconds");
    r.register(Box::new(h.clone()))
        .expect("register ai_inference_latency");
    h
}

pub(crate) fn register_ai_inference_errors(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_ai_inference_errors_total",
            "Total AI inference errors by engine",
        ),
        &["engine"],
    )
    .expect("register sensearc_ai_inference_errors_total");
    r.register(Box::new(v.clone()))
        .expect("register ai_inference_errors");
    v
}

pub(crate) fn register_ai_model_switch(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_ai_model_switch_total",
        "Total number of AI model switches",
    ))
    .expect("register sensearc_ai_model_switch_total");
    r.register(Box::new(c.clone()))
        .expect("register ai_model_switch");
    c
}

pub(crate) fn register_ai_anomaly_triggered(r: &Registry) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(
        "sensearc_ai_anomaly_triggered_total",
        "Total number of anomaly detections triggered by AI",
    ))
    .expect("register sensearc_ai_anomaly_triggered_total");
    r.register(Box::new(c.clone()))
        .expect("register ai_anomaly_triggered");
    c
}

pub(crate) fn register_ai_engine_state(r: &Registry) -> IntGaugeVec {
    let v = IntGaugeVec::new(
        Opts::new(
            "sensearc_ai_engine_state",
            "AI engine state (0=idle, 1=running, 2=error) by engine",
        ),
        &["engine"],
    )
    .expect("register sensearc_ai_engine_state");
    r.register(Box::new(v.clone()))
        .expect("register ai_engine_state");
    v
}

pub(crate) fn register_ai_current_mape(r: &Registry) -> prometheus::GaugeVec {
    let v = prometheus::GaugeVec::new(
        Opts::new(
            "sensearc_ai_current_mape",
            "Current MAPE (Mean Absolute Percentage Error) by engine",
        ),
        &["engine"],
    )
    .expect("register sensearc_ai_current_mape");
    r.register(Box::new(v.clone()))
        .expect("register ai_current_mape");
    v
}

pub(crate) fn register_ai_training_progress(r: &Registry) -> prometheus::Gauge {
    let g = prometheus::Gauge::with_opts(Opts::new(
        "sensearc_ai_training_progress",
        "AI model training progress (0.0-1.0)",
    ))
    .expect("register sensearc_ai_training_progress");
    r.register(Box::new(g.clone()))
        .expect("register ai_training_progress");
    g
}

pub(crate) fn register_ai_ood_detections(r: &Registry) -> IntCounterVec {
    let v = IntCounterVec::new(
        Opts::new(
            "sensearc_ai_ood_detections_total",
            "Total out-of-distribution detections by engine",
        ),
        &["engine"],
    )
    .expect("register sensearc_ai_ood_detections_total");
    r.register(Box::new(v.clone()))
        .expect("register ai_ood_detections");
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_all_ai_metrics_register() {
        let r = Registry::new();
        register_ai_inference_requests(&r);
        register_ai_inference_latency(&r);
        register_ai_inference_errors(&r);
        register_ai_model_switch(&r);
        register_ai_anomaly_triggered(&r);
        register_ai_engine_state(&r);
        register_ai_current_mape(&r);
        register_ai_training_progress(&r);
        register_ai_ood_detections(&r);
    }
}

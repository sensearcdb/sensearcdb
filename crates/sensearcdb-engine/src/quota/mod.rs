//! 数据面配额裁决点（S4 Step 3）。

pub mod gate;

pub use gate::{QuotaDecision, QuotaRejectReason, QuotaSnapshot, QuotaSource, TenantQuotaGate};

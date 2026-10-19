//! 时间分区边界对齐工具。
//!
//! `align_to_partition` 是「数据时间 → 天分区键」的唯一真相源。分区键 =
//! `k * duration_ns - tz_offset_ns`（默认 UTC+8 00:00）。
//!
//! 放在 `sensearcdb-common` 而非 `sensearcdb-partition` 的原因：引擎
//! （`sensearcdb-engine`）的 flush / compaction / query 三条主路径需要复用
//! 同一公式，而 `sensearcdb-partition` 依赖 `sensearcdb-engine`（反向依赖），
//! 若引擎再依赖 partition 会形成循环。故把公式下沉到 common，两 crate 共用
//! 单一实现，避免两份公式漂移。

/// 把 epoch 纳秒时间戳对齐到其所在分区窗口的起始边界。
///
/// 分区边界对齐到给定时区的天边界（默认 UTC+8 00:00）。公式：
///
/// ```text
/// // 分区边界（UTC epoch）: k * duration_ns - tz_offset_ns
/// k = floor((ts_ns + tz_offset_ns) / duration_ns)
/// partition_ns = k * duration_ns - tz_offset_ns
/// ```
///
/// # 参数
/// - `ts_ns`: epoch 纳秒（UTC）。
/// - `duration_ms`: 分区窗口时长（毫秒）。
/// - `tz_offset_secs`: 时区偏移（秒，例如 UTC+8 = 28800）。
///
/// # 返回
/// 该时间戳所在分区窗口的起始 epoch 纳秒值。
pub fn align_to_partition(ts_ns: i64, duration_ms: u64, tz_offset_secs: i64) -> i64 {
    let duration_ns = (duration_ms as i64) * 1_000_000;
    if duration_ns <= 0 {
        return ts_ns;
    }
    let tz_offset_ns = tz_offset_secs * 1_000_000_000;

    // 分区边界（UTC）: k * duration_ns - tz_offset_ns
    // 求 k 使 boundary <= ts < next_boundary。
    let numerator = ts_ns + tz_offset_ns;
    let k = if numerator >= 0 {
        numerator / duration_ns
    } else {
        // 处理负时间戳（首个边界之前）。
        (numerator - duration_ns + 1) / duration_ns
    };
    k * duration_ns - tz_offset_ns
}

#[cfg(test)]
mod tests {
    use super::*;

    const DURATION_MS: u64 = 86_400_000; // 24h
    const TZ_OFFSET_SECS: i64 = 28_800; // UTC+8

    #[test]
    fn test_align_utc8_midnight() {
        // 2024-01-01 00:00:00 UTC+8 = 2023-12-31 16:00:00 UTC
        let jan1_utc8 = 1_704_038_400_000_000_000_i64;
        assert_eq!(
            align_to_partition(jan1_utc8, DURATION_MS, TZ_OFFSET_SECS),
            jan1_utc8
        );

        let noon = jan1_utc8 + 12 * 3_600_000_000_000_i64;
        assert_eq!(
            align_to_partition(noon, DURATION_MS, TZ_OFFSET_SECS),
            jan1_utc8
        );
    }

    #[test]
    fn test_align_utc_midnight() {
        let jan1_utc = 1_704_067_200_000_000_000_i64;
        assert_eq!(align_to_partition(jan1_utc, DURATION_MS, 0), jan1_utc);
    }

    #[test]
    fn test_align_cross_day_split_points() {
        // 两个相邻天边界的交界点必须落到不同的分区键。
        let day0 = 1_704_038_400_000_000_000_i64; // 2024-01-01 UTC+8 00:00
        let day1 = day0 + 86_400_000_000_000_i64;
        assert_eq!(align_to_partition(day0, DURATION_MS, TZ_OFFSET_SECS), day0);
        assert_eq!(
            align_to_partition(day0 - 1, DURATION_MS, TZ_OFFSET_SECS),
            day0 - 86_400_000_000_000_i64
        );
        assert_eq!(
            align_to_partition(day1 - 1, DURATION_MS, TZ_OFFSET_SECS),
            day0
        );
        assert_eq!(align_to_partition(day1, DURATION_MS, TZ_OFFSET_SECS), day1);
    }
}

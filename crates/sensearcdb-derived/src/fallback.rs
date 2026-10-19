//! Fallback policies for derived tag evaluation.
//!
//! When a source tag is unavailable (missing, Bad quality, or stale),
//! the fallback policy determines how the derived tag engine should proceed.
//!
//! # Policies
//!
//! | Policy        | Behavior                                                |
//! |---------------|---------------------------------------------------------|
//! | `Strict`      | Source unavailable → result is NaN with Bad quality      |
//! | `Lenient`     | Use last-known-good cached value within `max_age_secs`   |
//! | `Coalesce`    | Use a configured default value for each source tag       |

use sensearcdb_common::TenantId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── FallbackPolicy ───

/// Fallback policy for handling unavailable source tag data.
///
/// Determines how the evaluator should proceed when a source tag's
/// value is missing or its quality is Bad.
///
/// 派生 `Serialize/Deserialize` 用于持久化到 `derived_tags.fallback_policy_json`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FallbackPolicy {
    /// Strict mode: if any source is unavailable, the result is NaN with Bad quality.
    /// This is the default and safest policy.
    Strict,

    /// Lenient mode: use the last-good cached value if it's within `max_age_secs`.
    /// If the cached value is too old, fall through to NaN.
    Lenient {
        /// Maximum age (in seconds) for a cached value to be considered valid.
        last_good_max_age_secs: u32,
    },

    /// Coalesce mode: use configured default values for missing source tags.
    /// The defaults are specified as `tag_name → default_value`.
    Coalesce {
        /// Per-tag default values to use when the source is unavailable.
        defaults: HashMap<String, f64>,
    },
}

impl Default for FallbackPolicy {
    fn default() -> Self {
        FallbackPolicy::Strict
    }
}

impl FallbackPolicy {
    /// Create a lenient policy with the given max age in seconds.
    pub fn lenient(max_age_secs: u32) -> Self {
        FallbackPolicy::Lenient {
            last_good_max_age_secs: max_age_secs,
        }
    }

    /// Create a coalesce policy with the given defaults.
    pub fn coalesce(defaults: HashMap<String, f64>) -> Self {
        FallbackPolicy::Coalesce { defaults }
    }

    /// Returns `true` if this is the `Strict` policy.
    pub fn is_strict(&self) -> bool {
        matches!(self, FallbackPolicy::Strict)
    }

    /// Returns `true` if this is the `Lenient` policy.
    pub fn is_lenient(&self) -> bool {
        matches!(self, FallbackPolicy::Lenient { .. })
    }

    /// Returns `true` if this is the `Coalesce` policy.
    pub fn is_coalesce(&self) -> bool {
        matches!(self, FallbackPolicy::Coalesce { .. })
    }
}

// ─── LastGoodCache ───

/// Cache of last-good values for lenient fallback.
///
/// Stores the most recent good-quality value for each tag along with
/// its timestamp for staleness checking.
#[derive(Debug, Clone, Default)]
pub struct LastGoodCache {
    /// (tenant_id, tag_name) → (value, timestamp_epoch_secs)
    entries: HashMap<(TenantId, String), (f64, i64)>,
}

impl LastGoodCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Update a tag's last-good value.
    ///
    /// Called when a tag is evaluated successfully with Good quality.
    pub fn update(&mut self, tenant_id: TenantId, tag_name: &str, value: f64, ts: i64) {
        self.entries
            .insert((tenant_id, tag_name.to_string()), (value, ts));
    }

    /// Get a tag's last-good value if it's within the max age.
    ///
    /// # Arguments
    /// * `tenant_id` - Tenant scope of the tag.
    /// * `tag_name` - The source tag name.
    /// * `now` - Current epoch seconds.
    /// * `max_age_secs` - Maximum allowed age of the cached value.
    ///
    /// # Returns
    /// `Some(value)` if a valid cached value exists, `None` otherwise.
    pub fn get_if_fresh(
        &self,
        tenant_id: TenantId,
        tag_name: &str,
        now: i64,
        max_age_secs: u32,
    ) -> Option<f64> {
        if let Some((value, ts)) = self.entries.get(&(tenant_id, tag_name.to_string())) {
            let age = (now - ts) as u32;
            if age <= max_age_secs {
                return Some(*value);
            }
        }
        None
    }

    /// Check if a tag has a cached value (regardless of age).
    pub fn has(&self, tenant_id: TenantId, tag_name: &str) -> bool {
        self.entries
            .contains_key(&(tenant_id, tag_name.to_string()))
    }

    /// Remove a tag from the cache.
    pub fn remove(&mut self, tenant_id: TenantId, tag_name: &str) {
        self.entries.remove(&(tenant_id, tag_name.to_string()));
    }

    /// Clear all cached entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Get the number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strict_policy() {
        let policy = FallbackPolicy::Strict;
        assert!(policy.is_strict());
        assert!(!policy.is_lenient());
        assert!(!policy.is_coalesce());
    }

    #[test]
    fn test_lenient_policy() {
        let policy = FallbackPolicy::lenient(300);
        assert!(policy.is_lenient());
        assert!(!policy.is_strict());

        match policy {
            FallbackPolicy::Lenient {
                last_good_max_age_secs,
            } => {
                assert_eq!(last_good_max_age_secs, 300);
            }
            _ => panic!("expected Lenient"),
        }
    }

    #[test]
    fn test_coalesce_policy() {
        let mut defaults = HashMap::new();
        defaults.insert("sensor_a".into(), 0.0);
        let policy = FallbackPolicy::coalesce(defaults);
        assert!(policy.is_coalesce());

        match policy {
            FallbackPolicy::Coalesce { defaults } => {
                assert_eq!(defaults.get("sensor_a"), Some(&0.0));
            }
            _ => panic!("expected Coalesce"),
        }
    }

    #[test]
    fn test_last_good_cache_update_and_get() {
        let mut cache = LastGoodCache::new();
        cache.update(TenantId::DEFAULT, "tag_a", 42.0, 1000);

        assert!(cache.has(TenantId::DEFAULT, "tag_a"));

        let val = cache.get_if_fresh(TenantId::DEFAULT, "tag_a", 1100, 300);
        assert_eq!(val, Some(42.0));

        // Too old
        let val = cache.get_if_fresh(TenantId::DEFAULT, "tag_a", 2000, 300);
        assert_eq!(val, None);
    }

    #[test]
    fn test_last_good_cache_missing() {
        let cache = LastGoodCache::new();
        assert_eq!(
            cache.get_if_fresh(TenantId::DEFAULT, "nonexistent", 1000, 300),
            None
        );
    }

    #[test]
    fn test_last_good_cache_remove() {
        let mut cache = LastGoodCache::new();
        cache.update(TenantId::DEFAULT, "tag_a", 42.0, 1000);
        cache.remove(TenantId::DEFAULT, "tag_a");
        assert!(!cache.has(TenantId::DEFAULT, "tag_a"));
    }

    #[test]
    fn test_last_good_cache_clear() {
        let mut cache = LastGoodCache::new();
        cache.update(TenantId::DEFAULT, "tag_a", 1.0, 1000);
        cache.update(TenantId::DEFAULT, "tag_b", 2.0, 1000);
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_last_good_cache_tenant_isolated() {
        let mut cache = LastGoodCache::new();
        cache.update(TenantId(1), "tag_a", 42.0, 1000);
        cache.update(TenantId(2), "tag_a", 7.0, 1000);

        assert_eq!(
            cache.get_if_fresh(TenantId(1), "tag_a", 1100, 300),
            Some(42.0)
        );
        assert_eq!(
            cache.get_if_fresh(TenantId(2), "tag_a", 1100, 300),
            Some(7.0)
        );
    }
}

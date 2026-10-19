//! Per-connection rate limiting for gRPC services.
//!
//! Uses a token bucket algorithm per connection, identified by client ID.
//! Prevents a single client from overwhelming the ingest pipeline.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tonic::Status;
use tracing::warn;

/// Token bucket rate limiter for a single connection.
#[derive(Debug)]
struct TokenBucket {
    /// Tokens currently available.
    tokens: f64,
    /// Maximum tokens (burst capacity).
    max_tokens: f64,
    /// Token refill rate (tokens per second).
    refill_rate: f64,
    /// Last refill timestamp.
    last_refill: Instant,
}

impl TokenBucket {
    /// Create a new token bucket.
    fn new(max_tokens: f64, refill_rate: f64) -> Self {
        Self {
            tokens: max_tokens,
            max_tokens,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume `count` tokens.
    ///
    /// Returns `true` if allowed, `false` if rate limit exceeded.
    fn try_consume(&mut self, count: f64) -> bool {
        self.refill();
        if self.tokens >= count {
            self.tokens -= count;
            true
        } else {
            false
        }
    }

    /// Refill tokens based on elapsed time.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        self.last_refill = now;
    }
}

/// Per-connection rate limiter configuration.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Points per second allowed per connection.
    pub points_per_sec: f64,
    /// Maximum burst size (points).
    pub burst_size: f64,
    /// Maximum number of tracked connections (LRU eviction of oldest).
    pub max_connections: usize,
    /// Cleanup interval for stale connections (seconds).
    pub cleanup_interval_secs: u64,
    /// Stale connection timeout (seconds).
    pub stale_timeout_secs: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            points_per_sec: 500_000.0,
            burst_size: 1_000_000.0,
            max_connections: 10_000,
            cleanup_interval_secs: 60,
            stale_timeout_secs: 300,
        }
    }
}

/// Rate limiter managing per-connection token buckets.
pub struct RateLimiter {
    config: RateLimitConfig,
    buckets: Mutex<HashMap<String, (TokenBucket, Instant)>>,
    last_cleanup: Mutex<Instant>,
}

impl RateLimiter {
    /// Create a new rate limiter.
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
            last_cleanup: Mutex::new(Instant::now()),
        }
    }

    /// Check if a request from `client_id` with `point_count` points is allowed.
    ///
    /// Returns `Ok(())` if allowed, or `Err(Status)` if rate limit exceeded.
    pub fn check(&self, client_id: &str, point_count: u64) -> Result<(), Status> {
        let now = Instant::now();
        let mut buckets = self.buckets.lock();

        // Periodic cleanup of stale connections
        if now.duration_since(*self.last_cleanup.lock()).as_secs()
            >= self.config.cleanup_interval_secs
        {
            *self.last_cleanup.lock() = now;
            buckets.retain(|_, (_, last_seen)| {
                now.duration_since(*last_seen).as_secs() < self.config.stale_timeout_secs
            });
        }

        let entry = buckets.entry(client_id.to_string()).or_insert_with(|| {
            (
                TokenBucket::new(self.config.burst_size, self.config.points_per_sec),
                now,
            )
        });

        entry.1 = now; // Update last seen

        if entry.0.try_consume(point_count as f64) {
            Ok(())
        } else {
            warn!(
                client_id = %client_id,
                point_count = point_count,
                "rate limit exceeded"
            );
            Err(Status::resource_exhausted("rate limit exceeded"))
        }
    }

    /// Get the number of tracked connections.
    pub fn connection_count(&self) -> usize {
        self.buckets.lock().len()
    }

    /// Remove a connection from tracking.
    pub fn remove(&self, client_id: &str) {
        self.buckets.lock().remove(client_id);
    }
}

/// Tonic interceptor for rate limiting.
///
/// Returns a closure that can be used as a tonic interceptor.
pub fn rate_limit_interceptor(
    limiter: Arc<RateLimiter>,
) -> impl Fn(tonic::Request<()>) -> Result<tonic::Request<()>, Status> + Clone {
    move |req: tonic::Request<()>| {
        // Extract client identity from auth context or connection metadata
        let client_id = req
            .extensions()
            .get::<crate::grpc::auth::AuthContext>()
            .map(|ctx| ctx.client_id.as_str())
            .unwrap_or("unknown");

        // Default point count estimate per gRPC message
        limiter.check(client_id, 1000)?;
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_allows_within_limit() {
        let mut bucket = TokenBucket::new(100.0, 100.0);
        assert!(bucket.try_consume(50.0));
        assert!(bucket.try_consume(25.0));
        assert!(!bucket.try_consume(50.0)); // Only ~25 tokens left
    }

    #[test]
    fn test_rate_limiter_basic() {
        let config = RateLimitConfig {
            points_per_sec: 1000.0,
            burst_size: 2000.0,
            max_connections: 10,
            cleanup_interval_secs: 3600,
            stale_timeout_secs: 3600,
        };
        let limiter = RateLimiter::new(config);

        // First request should be allowed
        assert!(limiter.check("client-1", 500).is_ok());
        // Second request within burst
        assert!(limiter.check("client-1", 500).is_ok());
        // Large request should trigger limit
        assert!(limiter.check("client-1", 10_000).is_err());
    }
}

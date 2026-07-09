//! Token-bucket rate limiter — port of
//! `references/cordn/src/server/rateLimit.ts`. One bucket per key (the caller's
//! stable pubkey at the adapter layer), refilled continuously up to the burst
//! cap, with idle-bucket eviction.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub struct TokenBucketRateLimitConfig {
    pub enabled: bool,
    pub refill_per_minute: f64,
    pub burst: f64,
    pub idle_ttl_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct TokenBucketState {
    tokens: f64,
    last_refill_at: i64,
    last_seen_at: i64,
}

fn clamp_non_negative(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        0.0
    }
}

pub struct TokenBucketRateLimiter {
    buckets: Mutex<HashMap<String, TokenBucketState>>,
    enabled: bool,
    burst: f64,
    idle_ttl_ms: f64,
    refill_tokens_per_ms: f64,
}

impl TokenBucketRateLimiter {
    pub fn new(config: TokenBucketRateLimitConfig) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            enabled: config.enabled,
            burst: clamp_non_negative(config.burst),
            idle_ttl_ms: clamp_non_negative(config.idle_ttl_ms),
            refill_tokens_per_ms: clamp_non_negative(config.refill_per_minute) / 60_000.0,
        }
    }

    /// Returns `true` if the action under `key` is allowed (and consumes one
    /// token), `false` if the bucket is empty. `now` is milliseconds since the
    /// epoch, supplied by the caller so tests (and the coordinator clock) stay
    /// deterministic.
    pub fn check(&self, key: &str, now: i64) -> bool {
        if !self.enabled {
            return true;
        }

        let mut buckets = self.buckets.lock().unwrap();
        self.evict_idle_buckets(&mut buckets, now);

        let Some(bucket) = buckets.get_mut(key) else {
            if self.burst < 1.0 {
                return false;
            }
            buckets.insert(
                key.to_owned(),
                TokenBucketState {
                    tokens: self.burst - 1.0,
                    last_refill_at: now,
                    last_seen_at: now,
                },
            );
            return true;
        };

        let elapsed_ms = (now - bucket.last_refill_at).max(0) as f64;
        let refilled = elapsed_ms * self.refill_tokens_per_ms;
        let available = (bucket.tokens + refilled).min(self.burst);

        bucket.last_refill_at = now;
        bucket.last_seen_at = now;

        if available < 1.0 {
            bucket.tokens = available;
            return false;
        }

        bucket.tokens = available - 1.0;
        true
    }

    fn evict_idle_buckets(&self, buckets: &mut HashMap<String, TokenBucketState>, now: i64) {
        if self.idle_ttl_ms <= 0.0 || buckets.is_empty() {
            return;
        }
        buckets.retain(|_, bucket| ((now - bucket.last_seen_at) as f64) < self.idle_ttl_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(burst: f64, per_min: f64, ttl_ms: f64) -> TokenBucketRateLimitConfig {
        TokenBucketRateLimitConfig {
            enabled: true,
            refill_per_minute: per_min,
            burst,
            idle_ttl_ms: ttl_ms,
        }
    }

    #[test]
    fn disabled_always_allows() {
        let rl = TokenBucketRateLimiter::new(TokenBucketRateLimitConfig {
            enabled: false,
            refill_per_minute: 0.0,
            burst: 0.0,
            idle_ttl_ms: 0.0,
        });
        assert!(rl.check("alice", 0));
        assert!(rl.check("alice", 0));
    }

    #[test]
    fn burst_then_deny_then_refill() {
        // 5-token burst, 60 tokens/min = 1 token / 1000ms.
        let rl = TokenBucketRateLimiter::new(cfg(5.0, 60.0, 0.0));
        for _ in 0..5 {
            assert!(rl.check("alice", 0));
        }
        assert!(!rl.check("alice", 0)); // bucket empty
        assert!(rl.check("alice", 1_000)); // +1 token after 1s
        assert!(!rl.check("alice", 1_000));
    }

    #[test]
    fn refill_caps_at_burst() {
        let rl = TokenBucketRateLimiter::new(cfg(3.0, 6_000.0, 0.0)); // 100 tokens/s
        assert!(rl.check("alice", 0));
        // After a long idle, refill must not exceed burst (3).
        assert!(rl.check("alice", 10_000));
        assert!(rl.check("alice", 10_000));
        assert!(rl.check("alice", 10_000));
        assert!(!rl.check("alice", 10_000)); // burst exhausted
    }

    #[test]
    fn keys_are_isolated() {
        let rl = TokenBucketRateLimiter::new(cfg(1.0, 0.0, 0.0));
        assert!(rl.check("alice", 0));
        assert!(!rl.check("alice", 0));
        assert!(rl.check("bob", 0)); // independent bucket
    }

    #[test]
    fn idle_buckets_are_evicted() {
        let rl = TokenBucketRateLimiter::new(cfg(1.0, 0.0, 1_000.0));
        assert!(rl.check("alice", 0));
        // After idle_ttl, the bucket is gone — a fresh bucket is created,
        // granting a new token.
        assert!(rl.check("alice", 1_500));
    }

    #[test]
    fn zero_burst_denies_everything() {
        let rl = TokenBucketRateLimiter::new(cfg(0.0, 60.0, 0.0));
        assert!(!rl.check("alice", 0));
    }
}

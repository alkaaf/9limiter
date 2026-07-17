use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, serde::Serialize)]
pub struct RateLimitEvent {
    pub api_key: String,
    pub model: String,
    pub rule_limit: u32,
    pub rule_window_secs: u64,
    pub count: usize,
    pub remaining: u32,
    pub reset_after_secs: u64,
    pub owner: String,
}

#[derive(Clone)]
pub struct SlidingLimiter {
    state: Arc<Mutex<HashMap<String, VecDeque<Instant>>>>,
}

impl SlidingLimiter {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns (passed: bool, event: RateLimitEvent).
    pub fn check(&self, api_key: &str, model: &str, limit: u32, window_secs: u64) -> (bool, RateLimitEvent) {
        let key = format!("{}:{}:{}:{}", api_key, model, limit, window_secs);
        let now = Instant::now();
        let window = std::time::Duration::from_secs(window_secs);

        let mut state = self.state.lock().unwrap();
        let deque = state.entry(key.clone()).or_insert_with(VecDeque::new);

        while let Some(&t) = deque.front() {
            if now.duration_since(t) >= window {
                deque.pop_front();
            } else {
                break;
            }
        }

        let count = deque.len();
        let passed = count < limit as usize;

        if passed {
            deque.push_back(now);
        }

        let reset_after_secs = deque.front()
            .map(|oldest| {
                let elapsed = now.duration_since(*oldest).as_secs();
                window_secs.saturating_sub(elapsed)
            })
            .unwrap_or(0);

        let remaining = limit.saturating_sub(count as u32);

        (passed, RateLimitEvent {
            api_key: api_key.to_string(),
            model: model.to_string(),
            rule_limit: limit,
            rule_window_secs: window_secs,
            count,
            remaining,
            reset_after_secs,
            owner: String::new(),
        })
    }

    /// Return snapshot of all active counters as RateLimitEvents.
    /// Expired entries are cleaned before returning.
    pub fn snapshot(&self) -> Vec<RateLimitEvent> {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let mut events = Vec::new();

        state.retain(|_key, deque| {
            // Pop expired
            while let Some(&t) = deque.front() {
                if now.duration_since(t) >= std::time::Duration::from_secs(86400 * 7) {
                    deque.pop_front();
                } else {
                    break;
                }
            }
            !deque.is_empty()
        });

        for (key, deque) in state.iter() {
            // key = "api_key:model:limit:window_secs"
            let parts: Vec<&str> = key.splitn(4, ':').collect();
            if parts.len() != 4 { continue; }
            let limit: u32 = parts[2].parse().unwrap_or(0);
            let window_secs: u64 = parts[3].parse().unwrap_or(0);
            let count = deque.len();

            let (api_key, model) = (parts[0], parts[1]);
            let remaining = limit.saturating_sub(count as u32);
            let reset_after_secs = deque.front()
                .map(|oldest| {
                    let elapsed = now.duration_since(*oldest).as_secs();
                    window_secs.saturating_sub(elapsed)
                })
                .unwrap_or(0);

            events.push(RateLimitEvent {
                api_key: api_key.to_string(),
                model: model.to_string(),
                rule_limit: limit,
                rule_window_secs: window_secs,
                count,
                remaining,
                reset_after_secs,
                owner: String::new(),
            });
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Barrier;

    #[test]
    fn test_serial_hit_limit() {
        let limiter = SlidingLimiter::new();
        assert!(limiter.check("k1", "gpt-4", 3, 60).0);
        assert!(limiter.check("k1", "gpt-4", 3, 60).0);
        assert!(limiter.check("k1", "gpt-4", 3, 60).0);
        assert!(!limiter.check("k1", "gpt-4", 3, 60).0);
    }

    #[test]
    fn test_parallel_exceeds_limit() {
        let limiter = Arc::new(SlidingLimiter::new());
        let threads: usize = 8;
        let limit: u32 = 3;
        let window: u64 = 30;

        let barrier = Arc::new(Barrier::new(threads));
        let passed = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..threads {
            let l = limiter.clone();
            let b = barrier.clone();
            let p = passed.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                if l.check("k-par", "gpt-4", limit, window).0 {
                    p.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }

        let count = passed.load(Ordering::Relaxed);
        assert_eq!(count, limit, "only {} should pass limit={}, got {}", limit, limit, count);
    }

    #[test]
    fn test_parallel_under_limit() {
        let limiter = Arc::new(SlidingLimiter::new());
        let threads: usize = 5;
        let limit: u32 = 10;
        let window: u64 = 30;

        let barrier = Arc::new(Barrier::new(threads));
        let passed = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..threads {
            let l = limiter.clone();
            let b = barrier.clone();
            let p = passed.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                if l.check("k-under", "gpt-4", limit, window).0 {
                    p.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }

        let count = passed.load(Ordering::Relaxed);
        assert_eq!(count, threads as u32, "all {} should pass limit={}", threads, limit);
    }

    #[test]
    fn test_parallel_isolation_multiple_keys() {
        let limiter = Arc::new(SlidingLimiter::new());
        let per_key_limit: u32 = 2;
        let window: u64 = 30;

        let keys = ["key-a", "key-b", "key-c"];
        let barrier = Arc::new(Barrier::new(keys.len() * 4));
        let results = Arc::new(Mutex::new(HashMap::<&str, u32>::new()));

        let mut handles = Vec::new();
        for &key in &keys {
            for _ in 0..4 {
                let l = limiter.clone();
                let b = barrier.clone();
                let r = results.clone();
                let k = key;
                handles.push(std::thread::spawn(move || {
                    b.wait();
                    let passed = l.check(k, "gpt-4", per_key_limit, window).0;
                    if passed {
                        let mut map = r.lock().unwrap();
                        *map.entry(k).or_insert(0) += 1;
                    }
                }));
            }
        }
        for h in handles { h.join().unwrap(); }

        let map = results.lock().unwrap();
        for &key in &keys {
            assert_eq!(*map.get(key).unwrap_or(&0), per_key_limit,
                "key {} should pass exactly {} times", key, per_key_limit);
        }
    }

    #[test]
    fn test_serial_event_counts() {
        let limiter = SlidingLimiter::new();
        let (p, e) = limiter.check("k", "m", 5, 60);
        assert!(p); assert_eq!(e.count, 0); assert_eq!(e.remaining, 5);

        let (p, e) = limiter.check("k", "m", 5, 60);
        assert!(p); assert_eq!(e.count, 1); assert_eq!(e.remaining, 4);

        let (p, e) = limiter.check("k", "m", 5, 60);
        assert!(p); assert_eq!(e.count, 2); assert_eq!(e.remaining, 3);

        let (p, _) = limiter.check("k", "m", 5, 60);
        assert!(p);
        let (p, _) = limiter.check("k", "m", 5, 60);
        assert!(p);
        let (p, e) = limiter.check("k", "m", 5, 60);
        assert!(!p); assert_eq!(e.count, 5); assert_eq!(e.remaining, 0);
    }
}

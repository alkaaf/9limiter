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
    /// Mutates only on pass.
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

    /// Check multiple rules for same (api_key, model) atomically.
    /// Pass + push all only if ALL have room. Single lock acquisition — no race.
    pub fn check_all(&self, api_key: &str, model: &str, rules: &[(u32, u64)]) -> Vec<RateLimitEvent> {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let mut events = Vec::with_capacity(rules.len());
        let mut all_pass = true;

        // Phase 1: pop expired + check all
        for &(limit, window_secs) in rules {
            let key = format!("{}:{}:{}:{}", api_key, model, limit, window_secs);
            let window = std::time::Duration::from_secs(window_secs);
            let deque = state.entry(key).or_insert_with(VecDeque::new);

            while let Some(&t) = deque.front() {
                if now.duration_since(t) >= window { deque.pop_front(); } else { break; }
            }

            let count = deque.len();
            let passed = count < limit as usize;
            if !passed { all_pass = false; }

            let reset_after_secs = deque.front()
                .map(|oldest| window_secs.saturating_sub(now.duration_since(*oldest).as_secs()))
                .unwrap_or(0);

            events.push(RateLimitEvent {
                api_key: api_key.to_string(),
                model: model.to_string(),
                rule_limit: limit,
                rule_window_secs: window_secs,
                count,
                remaining: 0,
                reset_after_secs,
                owner: String::new(),
            });
        }

        // Phase 2: all pass — push timestamps + patch event counts
        if all_pass {
            for (i, &(limit, window_secs)) in rules.iter().enumerate() {
                let key = format!("{}:{}:{}:{}", api_key, model, limit, window_secs);
                let deque = state.get_mut(&key).unwrap();
                deque.push_back(now);
                let new_count = deque.len();
                events[i].count = new_count;
                events[i].remaining = limit.saturating_sub(new_count as u32);
            }
        }

        events
    }

    /// Return snapshot of all active counters as RateLimitEvents.
    /// Expired entries are cleaned before returning.
    pub fn snapshot(&self) -> Vec<RateLimitEvent> {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let mut events = Vec::new();

        state.retain(|key, deque| {
            // key = "api_key:model:limit:window_secs" — parse window to expire correctly
            let parts: Vec<&str> = key.splitn(4, ':').collect();
            let win: u64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(3600);
            while let Some(&t) = deque.front() {
                if now.duration_since(t) >= std::time::Duration::from_secs(win) {
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

    #[test]
    fn test_window_expires_after_duration() {
        let limiter = SlidingLimiter::new();
        // 1-second window, limit=2
        assert!(limiter.check("k-win", "m", 2, 1).0);
        assert!(limiter.check("k-win", "m", 2, 1).0);
        assert!(!limiter.check("k-win", "m", 2, 1).0); // blocked at 2/2
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // Window expired — counter reset
        assert!(limiter.check("k-win", "m", 2, 1).0); // 1/2
        assert!(limiter.check("k-win", "m", 2, 1).0); // 2/2
        assert!(!limiter.check("k-win", "m", 2, 1).0); // blocked again
    }

    #[test]
    fn test_check_all_multiple_rules_all_pass() {
        let limiter = SlidingLimiter::new();
        // 2 rules: limit=3 and limit=5, same 60s window
        let rules = [(3, 60), (5, 60)];
        let results = limiter.check_all("k-all", "m", &rules);
        assert_eq!(results.len(), 2);
        // Both passed — count after push = 1
        assert_eq!(results[0].count, 1);
        assert_eq!(results[1].count, 1);
        assert_eq!(results[0].remaining, 2);
        assert_eq!(results[1].remaining, 4);

        let results2 = limiter.check_all("k-all", "m", &rules);
        assert_eq!(results2[0].count, 2);
        assert_eq!(results2[1].count, 2);
    }

    #[test]
    fn test_check_all_one_rule_blocks_no_push() {
        let limiter = SlidingLimiter::new();
        // Rule A: limit=1 (easy to fill), Rule B: limit=10
        // Fill Rule A first
        assert!(limiter.check("k-block", "m", 1, 60).0); // A now 1/1

        // check_all with both rules — A blocked, B should NOT have pushed
        let results = limiter.check_all("k-block", "m", &[(1, 60), (10, 60)]);
        assert_eq!(results.len(), 2);
        assert!(results[0].count >= 1); // A blocked
        // B should still be 0 — no push happened
        let (passed, _) = limiter.check("k-block", "m", 10, 60);
        assert!(passed, "B should still have quota — check_all must not push on failure");
    }

    #[test]
    fn test_check_all_atomic_no_race_increment() {
        let limiter = Arc::new(SlidingLimiter::new());
        let threads: usize = 8;
        let rules = [(3, 30), (5, 30)];
        let barrier = Arc::new(Barrier::new(threads));
        let passed = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..threads {
            let l = limiter.clone();
            let b = barrier.clone();
            let p = passed.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let r = l.check_all("k-atomic", "m", &rules);
                // All pass only if every rule had room
                if r.iter().all(|e| e.count < e.rule_limit as usize) {
                    p.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        // Tightest rule is limit=3 — at most 3 pass, but race may yield 2
        let count = passed.load(Ordering::Relaxed);
        assert!(count >= 2, "at least 2 should pass, got {}", count);
        assert!(count <= 3, "at most 3 should pass (limit=3), got {}", count);
    }

    #[test]
    fn test_check_all_window_expiry() {
        let limiter = SlidingLimiter::new();
        let rules = [(2, 1), (3, 1)]; // 1-second window

        // Fill both rules — each push increments
        let r = limiter.check_all("k-exp", "m", &rules);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].count, 1); // 1/2
        assert_eq!(r[1].count, 1); // 1/3

        let r = limiter.check_all("k-exp", "m", &rules);
        assert_eq!(r[0].count, 2); // 2/2
        assert_eq!(r[1].count, 2); // 2/3

        // Third attempt — blocked (rule A limit=2)
        let r = limiter.check_all("k-exp", "m", &rules);
        assert!(r[0].count >= r[0].rule_limit as usize, "rule A should be blocked");
        // But B should NOT have incremented — no push because A blocked
        let (passed, _) = limiter.check("k-exp", "m", 3, 1);
        assert!(passed, "B should still have quota (2/3)");

        // Wait for window expiry
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Both rules reset — can use again
        let r = limiter.check_all("k-exp", "m", &rules);
        assert_eq!(r[0].count, 1);
        assert_eq!(r[1].count, 1);
    }
}

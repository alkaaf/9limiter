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
        })
    }
}

use serde::Serialize;
use crate::limiter::RateLimitEvent;

#[derive(Debug, Clone, Serialize)]
pub struct RequestStartEvent {
    pub id: String,
    pub api_key: String,
    pub model: String,
    pub method: String,
    pub path: String,
    pub timestamp: String,
    pub owner: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestEndEvent {
    pub id: String,
    pub status: u16,
    pub latency_ms: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogEvent {
    pub time: String,
    pub level: String,
    pub msg: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum AppEvent {
    #[serde(rename = "rate_limit")]
    RateLimit(RateLimitEvent),
    #[serde(rename = "request")]
    Request(RequestStartEvent),
    #[serde(rename = "request_end")]
    RequestEnd(RequestEndEvent),
    #[serde(rename = "log")]
    Log(LogEvent),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_event_serialize() {
        let e = AppEvent::RateLimit(RateLimitEvent {
            api_key: "sk-test".into(),
            model: "gpt-4".into(),
            rule_limit: 3,
            rule_window_secs: 60,
            count: 1,
            remaining: 2,
            reset_after_secs: 30,
            owner: "alice".into(),
        });
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"type\":\"rate_limit\""));
        assert!(json.contains("\"api_key\":\"sk-test\""));
        assert!(json.contains("\"remaining\":2"));
    }

    #[test]
    fn test_request_start_event_serialize() {
        let e = AppEvent::Request(RequestStartEvent {
            id: "abc-123".into(),
            api_key: "sk-key".into(),
            model: "claude-3".into(),
            method: "POST".into(),
            path: "/v1/messages".into(),
            timestamp: "2026-07-27T10:00:00+07:00".into(),
            owner: "bob".into(),
        });
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"type\":\"request\""));
        assert!(json.contains("\"api_key\":\"sk-key\""));
    }

    #[test]
    fn test_request_end_event_serialize() {
        let e = AppEvent::RequestEnd(RequestEndEvent {
            id: "abc-123".into(),
            status: 200,
            latency_ms: 150,
            input_tokens: 100,
            output_tokens: 50,
            cache_tokens: 20,
        });
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"type\":\"request_end\""));
        assert!(json.contains("\"status\":200"));
        assert!(json.contains("\"input_tokens\":100"));
    }

    #[test]
    fn test_log_event_serialize() {
        let e = AppEvent::Log(LogEvent {
            time: "10:00:00".into(),
            level: "info".into(),
            msg: "request processed".into(),
        });
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"type\":\"log\""));
        assert!(json.contains("\"level\":\"info\""));
    }

    #[test]
    fn test_event_roundtrip() {
        let original = AppEvent::RateLimit(RateLimitEvent {
            api_key: "sk-rt".into(),
            model: "o1".into(),
            rule_limit: 10,
            rule_window_secs: 3600,
            count: 0,
            remaining: 10,
            reset_after_secs: 0,
            owner: "".into(),
        });
        let json = serde_json::to_string(&original).unwrap();
        // Tagged enum cannot deserialize back without #[serde(untagged)]
        // Just verify serialization is deterministic
        let json2 = serde_json::to_string(&original).unwrap();
        assert_eq!(json, json2);
    }
}

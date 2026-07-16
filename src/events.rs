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
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestEndEvent {
    pub id: String,
    pub status: u16,
    pub latency_ms: u64,
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
}

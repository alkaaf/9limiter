use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use crate::{
    config::Config,
    events::{AppEvent, RequestStartEvent, RequestEndEvent},
    limiter::SlidingLimiter,
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<tokio::sync::RwLock<Config>>,
    pub limiter: SlidingLimiter,
    pub event_tx: tokio::sync::broadcast::Sender<AppEvent>,
}

pub async fn proxy_handler(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Response {
    let api_key = req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
        .to_string();

    if api_key.is_empty() {
        return (StatusCode::UNAUTHORIZED, "missing Authorization header").into_response();
    }

    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::BAD_REQUEST, "failed to read body").into_response(),
    };

    let model = extract_model(&bytes).unwrap_or_else(|| "*".to_string());

    // Phase 1: rate limit check — clone rule data to drop config guard
    let matching_rules: Vec<(u32, u64)> = {
        let config = state.config.read().await;
        let ruleset_name = match lookup_ruleset(&config, &api_key) {
            Some(name) => name.to_string(),
            None => return (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
        };
        let now = chrono::Local::now();
        let day_name = now.format("%a").to_string();
        let time_str = now.format("%H:%M").to_string();

        config.rulesets.iter()
            .find(|rs| rs.name == ruleset_name)
            .map(|rs| rs.rules.iter()
                .filter(|rule| rule_matches(rule, &model, &day_name, &time_str))
                .map(|r| (r.limit, r.window_secs))
                .collect::<Vec<_>>())
            .unwrap_or_default()
    };

    for &(limit, window_secs) in &matching_rules {
        let (passed, event) = state.limiter.check(&api_key, &model, limit, window_secs);
        let _ = state.event_tx.send(AppEvent::RateLimit(event));
        if !passed {
            let body = serde_json::json!({
                "error": "rate_limit_exceeded",
                "message": format!("Rate limit exceeded for model {}", model),
                "reset_after_secs": window_secs,
            });
            return (StatusCode::TOO_MANY_REQUESTS, serde_json::to_string(&body).unwrap()).into_response();
        }
    }

    // Phase 2: upstream selection
    let upstream_url = {
        let config = state.config.read().await;
        let upstream = config.upstreams.iter()
            .filter(|u| parts.uri.path().starts_with(&u.path_prefix))
            .max_by_key(|u| u.path_prefix.len());

        let upstream = match upstream {
            Some(u) => u,
            None => return (StatusCode::NOT_FOUND, "no matching upstream").into_response(),
        };

        let path = parts.uri.path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("");
        format!("{}{}", upstream.base_url.trim_end_matches('/'), path)
    };

    proxy_to_upstream(&state, parts, bytes, &upstream_url, &api_key, &model).await
}

pub fn rule_matches(rule: &crate::config::Rule, model: &str, day: &str, time: &str) -> bool {
    if rule.model != "*" && rule.model != model {
        return false;
    }
    if !rule.days.iter().any(|d| d == day) {
        return false;
    }
    time >= rule.time_start.as_str() && time < rule.time_end.as_str()
    // ponytail: string compare for HH:MM — fine until we need timezone-aware scheduling
}

fn lookup_ruleset<'a>(config: &'a Config, api_key: &str) -> Option<&'a str> {
    for entry in &config.api_keys {
        if entry.keys.iter().any(|k| k == api_key) {
            return Some(&entry.ruleset);
        }
    }
    config.fallback_ruleset.as_deref()
}

fn extract_model(body: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    v.get("model").and_then(|m| m.as_str()).map(|s| s.to_string())
}

async fn proxy_to_upstream(
    state: &AppState,
    parts: axum::http::request::Parts,
    body_bytes: bytes::Bytes,
    upstream_url: &str,
    api_key: &str,
    model: &str,
) -> Response {
    let req_id = uuid::Uuid::new_v4().to_string();
    let start = std::time::Instant::now();

    let _ = state.event_tx.send(AppEvent::Request(RequestStartEvent {
        id: req_id.clone(),
        api_key: api_key.to_string(),
        model: model.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path_and_query()
            .map(|pq| pq.to_string())
            .unwrap_or_default(),
        timestamp: chrono::Local::now().to_rfc3339(),
    }));

    let client = reqwest::Client::new();
    let mut upstream_req = client.request(parts.method.clone(), upstream_url)
        .body(body_bytes);

    for (name, value) in &parts.headers {
        if name != header::HOST {
            upstream_req = upstream_req.header(name, value);
        }
    }

    match upstream_req.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let latency = start.elapsed().as_millis() as u64;
            let content_type = resp.headers().get(header::CONTENT_TYPE)
                .cloned()
                .unwrap_or_else(|| header::HeaderValue::from_static("application/json"));

            let _ = state.event_tx.send(AppEvent::RequestEnd(RequestEndEvent {
                id: req_id,
                status,
                latency_ms: latency,
            }));

            let body = Body::from_stream(resp.bytes_stream());
            let mut response = Response::new(body);
            *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            response.headers_mut().insert(header::CONTENT_TYPE, content_type);
            response
        }
        Err(e) => {
            tracing::error!("upstream error: {}", e);
            let status = if e.is_timeout() { 504 } else { 502 };
            let _ = state.event_tx.send(AppEvent::RequestEnd(RequestEndEvent {
                id: req_id,
                status,
                latency_ms: start.elapsed().as_millis() as u64,
            }));
            (StatusCode::from_u16(status).unwrap(), "upstream error").into_response()
        }
    }
}

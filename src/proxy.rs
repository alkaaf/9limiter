use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use crate::{
    config::Config,
    events::{self, AppEvent, RequestStartEvent, RequestEndEvent},
    limiter::SlidingLimiter,
    stats::StatsHandle,
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<tokio::sync::RwLock<Config>>,
    pub limiter: SlidingLimiter,
    pub event_tx: tokio::sync::broadcast::Sender<AppEvent>,
    pub upstream_idx: Arc<Mutex<HashMap<String, usize>>>,
    pub key_owners: Arc<HashMap<String, String>>,
    pub tz: chrono::FixedOffset,
    pub stats_tx: tokio::sync::mpsc::UnboundedSender<(String, String, String, u64)>,
    pub stats_handle: Arc<StatsHandle>,
    pub request_log_tx: tokio::sync::mpsc::UnboundedSender<crate::stats::RequestLog>,
    pub stats_db_path: String,
    pub http_client: reqwest::Client,
    pub circuit_breaker: Arc<Mutex<HashMap<String, CircuitBreakerState>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct CircuitBreakerState {
    pub failures: u32,
    pub open_until: Option<std::time::Instant>,
}

impl CircuitBreakerState {
    fn is_dead(&self) -> bool {
        match self.open_until {
            Some(t) => std::time::Instant::now() < t,
            None => false,
        }
    }
}

const CB_THRESHOLD: u32 = 5;
const CB_COOLDOWN_SECS: u64 = 30;

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
    let req_id = uuid::Uuid::new_v4().to_string();
    let owner = state.key_owners.get(&api_key).cloned().unwrap_or_default();
    send_log(&state.event_tx, "info",
        format!("req={} {} {} model={} key={:.12} owner={}", &req_id[..8], parts.method, parts.uri.path(), &model, &api_key, &owner),
        &state.tz);

    // Phase 1: rate limit check — clone rule data to drop config guard
    let matching_rules: Vec<(u32, u64)> = {
        let config = state.config.read().await;
        let ruleset_name = match lookup_ruleset(&config, &api_key) {
            Some(name) => name.to_string(),
            None => return (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
        };
        let now = chrono::Utc::now().with_timezone(&state.tz);
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
        let mut event = event; event.owner = state.key_owners.get(&api_key).cloned().unwrap_or_default();
        let _ = state.event_tx.send(AppEvent::RateLimit(event.clone()));
        if passed {
            tracing::debug!("req={} rl=pass limit={} count={}", &req_id[..8], limit, event.count);
        } else {
            send_log(&state.event_tx, "warn",
                format!("req={} rate-limit BLOCKED ({}/{}) key={:.12} model={} owner={}", &req_id[..8], event.count, limit, &api_key, &model, &event.owner),
                &state.tz);
            let body = serde_json::json!({
                "error": "rate_limit_exceeded",
                "message": format!("Rate limit exceeded for model {}", model),
                "reset_after_secs": window_secs,
            });
            return (StatusCode::TOO_MANY_REQUESTS, serde_json::to_string(&body).unwrap()).into_response();
        }
    }

    // Phase 2: upstream selection (round-robin per prefix group)
    let upstream_url = {
        let cfg = state.config.read().await;
        let candidates: Vec<&crate::config::UpstreamEntry> = cfg.upstreams.iter()
            .filter(|u| parts.uri.path().starts_with(&u.path_prefix))
            .collect();

        let upstream = match candidates.as_slice() {
            [] => return (StatusCode::NOT_FOUND, "no matching upstream").into_response(),
            [u] => *u,
            multiple => {
                let prefix = &multiple[0].path_prefix;
                let mut idx_map = state.upstream_idx.lock().unwrap();
                let next = idx_map.entry(prefix.clone()).or_insert(0);
                let chosen = multiple[*next % multiple.len()];
                *next = (*next + 1) % multiple.len();
                chosen
            }
        };

        // Circuit breaker check — clone base_url before cfg drops
        let base_url = upstream.base_url.clone();
        {
            let cb = state.circuit_breaker.lock().unwrap();
            if let Some(cs) = cb.get(&base_url) {
                if cs.is_dead() {
                    send_log(&state.event_tx, "warn",
                        format!("circuit-breaker OPEN for {}, dropping request", upstream.base_url),
                        &state.tz);
                    return (StatusCode::SERVICE_UNAVAILABLE, "upstream temporarily unavailable").into_response();
                }
            }
        }

        let path = parts.uri.path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("");
        let suffix = path.strip_prefix(upstream.path_prefix.as_str()).unwrap_or(path);
        (format!("{}{}", upstream.base_url.trim_end_matches('/'), suffix), base_url)
    };

    proxy_to_upstream(&state, parts, bytes, &upstream_url.0, &api_key, &model, &state.circuit_breaker, &upstream_url.1).await
}

pub fn rule_matches(rule: &crate::config::Rule, model: &str, day: &str, time: &str) -> bool {
    if !rule.models.iter().any(|m| m == "*" || m == model) {
        return false;
    }
    if !rule.days.iter().any(|d| d == day) {
        return false;
    }
    if rule.time_start < rule.time_end {
        // Normal window (07:00-17:30)
        time >= rule.time_start.as_str() && time < rule.time_end.as_str()
    } else {
        // Overnight window (22:00-07:00)
        time >= rule.time_start.as_str() || time < rule.time_end.as_str()
    }
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

fn send_log(tx: &tokio::sync::broadcast::Sender<AppEvent>, level: &str, msg: String, tz: &chrono::FixedOffset) {
    let _ = tx.send(AppEvent::Log(events::LogEvent {
        time: chrono::Utc::now().with_timezone(tz).format("%H:%M:%S").to_string(),
        level: level.to_string(),
        msg,
    }));
}

fn extract_usage(body: &[u8]) -> Option<(u64, u64, u64)> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    // OpenAI: usage.total_tokens
    if let Some(total) = v.get("usage")?.get("total_tokens").and_then(|x| x.as_u64()) {
        let inp = v.get("usage").and_then(|u| u.get("prompt_tokens")).and_then(|x| x.as_u64()).unwrap_or(0);
        let out = v.get("usage").and_then(|u| u.get("completion_tokens")).and_then(|x| x.as_u64()).unwrap_or(0);
        let cached = v.get("usage").and_then(|u| u.get("prompt_tokens_details")).and_then(|d| d.get("cached_tokens")).and_then(|x| x.as_u64()).unwrap_or(0);
        return Some((inp, out, cached));
    }
    // Anthropic: usage.input_tokens + usage.output_tokens
    if let Some(u) = v.get("usage") {
        let inp = u.get("input_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
        let out = u.get("output_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
        let cached = u.get("cache_read_input_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
        if inp + out > 0 { return Some((inp, out, cached)); }
    }
    None
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
    circuit_breaker: &Arc<Mutex<HashMap<String, CircuitBreakerState>>>,
    upstream_base_url: &str,
) -> Response {
    let req_id = uuid::Uuid::new_v4().to_string();
    let start = std::time::Instant::now();

    tracing::debug!("req={} proxying to {} (key={:.12}, model={})", &req_id[..8], upstream_url, &api_key, &model);

    let owner = state.key_owners.get(api_key).cloned().unwrap_or_default();

    let _ = state.event_tx.send(AppEvent::Request(RequestStartEvent {
        id: req_id.clone(),
        api_key: api_key.to_string(),
        model: model.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path_and_query()
            .map(|pq| pq.to_string())
            .unwrap_or_default(),
        timestamp: chrono::Utc::now().with_timezone(&state.tz).to_rfc3339(),
        owner: owner.clone(),
    }));

    let mut upstream_req = state.http_client.request(parts.method.clone(), upstream_url)
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

            // Read full body to extract token usage, then reconstruct
            let body_bytes = match axum::body::to_bytes(Body::from_stream(resp.bytes_stream()), 256 * 1024).await {
                Ok(b) => b,
                Err(_) => return (StatusCode::BAD_GATEWAY, "failed to read upstream body").into_response(),
            };

            // Extract token usage and send to stats
            let (input_tokens, output_tokens, cache_tokens): (u64, u64, u64) = match extract_usage(&body_bytes) {
                Some((i, o, c)) => {
                    let total = i + o + c;
                    let model_family = model.split('/').next().unwrap_or(&model).to_string();
                    let hour = chrono::Utc::now().with_timezone(&state.tz).format("%Y-%m-%dT%H:00:00%:z").to_string();
                    let _ = state.stats_tx.send((api_key.to_string(), model_family, hour, total));
                    (i, o, c)
                }
                None => (0, 0, 0),
            };

            // Success — reset circuit breaker
            {
                let mut cb = circuit_breaker.lock().unwrap();
                cb.remove(upstream_base_url);
            }

            send_log(&state.event_tx, "info",
                format!("req={} {} {}ms key={:.12} model={} owner={}", &req_id[..8], status, latency, &api_key, &model, &owner),
                &state.tz);

            let _ = state.event_tx.send(AppEvent::RequestEnd(RequestEndEvent {
                id: req_id.clone(),
                status,
                latency_ms: latency,
                input_tokens,
                output_tokens,
                cache_tokens,
            }));
            let _ = state.request_log_tx.send(crate::stats::RequestLog {
                id: req_id.clone(),
                api_key: api_key.to_string(),
                owner: owner.clone(),
                model: model.to_string(),
                method: parts.method.to_string(),
                path: parts.uri.path_and_query().map(|pq| pq.to_string()).unwrap_or_default(),
                status,
                latency_ms: latency,
                input_tokens,
                output_tokens,
                cache_tokens,
                timestamp: chrono::Utc::now().with_timezone(&state.tz).to_rfc3339(),
            });

            let mut response = Response::new(Body::from(body_bytes));
            *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            response.headers_mut().insert(header::CONTENT_TYPE, content_type);
            response
        }
        Err(e) => {
            let status = if e.is_timeout() { 504 } else { 502 };

            // Track upstream failure — open circuit after threshold
            {
                let mut cb = circuit_breaker.lock().unwrap();
                let entry = cb.entry(upstream_base_url.to_string()).or_insert(CircuitBreakerState {
                    failures: 0,
                    open_until: None,
                });
                entry.failures += 1;
                if entry.failures >= CB_THRESHOLD && entry.open_until.is_none() {
                    entry.open_until = Some(std::time::Instant::now() + std::time::Duration::from_secs(CB_COOLDOWN_SECS));
                    entry.failures = 0;
                    tracing::warn!("circuit-breaker OPEN for {} ({} consecutive failures)",
                        upstream_base_url, CB_THRESHOLD);
                }
            }

            send_log(&state.event_tx, "error",
                format!("req={} upstream error ({}): key={:.12} model={}", &req_id[..8], status, &api_key, &model),
                &state.tz);
            let _ = state.event_tx.send(AppEvent::RequestEnd(RequestEndEvent {
                id: req_id.clone(),
                status,
                latency_ms: start.elapsed().as_millis() as u64,
                input_tokens: 0,
                output_tokens: 0,
                cache_tokens: 0,
            }));
            let _ = state.request_log_tx.send(crate::stats::RequestLog {
                id: req_id.clone(),
                api_key: api_key.to_string(),
                owner,
                model: model.to_string(),
                method: parts.method.to_string(),
                path: parts.uri.path_and_query().map(|pq| pq.to_string()).unwrap_or_default(),
                status: status as u16,
                latency_ms: start.elapsed().as_millis() as u64,
                input_tokens: 0,
                output_tokens: 0,
                cache_tokens: 0,
                timestamp: chrono::Utc::now().with_timezone(&state.tz).to_rfc3339(),
            });
            (StatusCode::from_u16(status).unwrap(), format!("upstream error ({})", status)).into_response()
        }
    }
}

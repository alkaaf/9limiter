use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use crate::limiter::RateLimitEvent;
use crate::proxy::AppState;
use std::collections::HashMap;

pub async fn dashboard_handler() -> impl IntoResponse {
    Html(include_str!("dashboard.html"))
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

fn patch_owner(mut event: RateLimitEvent, owners: &HashMap<String, String>) -> RateLimitEvent {
    if let Some(name) = owners.get(&event.api_key) {
        event.owner = name.clone();
    }
    event
}

async fn send_snapshot(sender: &mut futures_util::stream::SplitSink<WebSocket, Message>, state: &AppState) -> Result<(), ()> {
    for event in state.limiter.snapshot() {
        let event = patch_owner(event, &state.key_owners);
        let msg = json!({"type": "rate_limit", "data": event});
        if sender.send(Message::Text(msg.to_string().into())).await.is_err() {
            return Err(());
        }
    }
    Ok(())
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let mut rx = state.event_tx.subscribe();
    let (mut sender, mut receiver) = socket.split();

    // Send server time on connect
    let server_time = chrono::Utc::now().with_timezone(&state.tz).format("%H:%M:%S").to_string();
    let _ = sender.send(Message::Text(json!({"type":"clock","time":server_time}).to_string().into())).await;

    // Initial snapshot
    let _ = send_snapshot(&mut sender, &state).await;

    // Live events + incoming sync commands
    loop {
        tokio::select! {
            event = rx.recv() => {
                let event = match event {
                    Ok(e) => e,
                    Err(_) => break,
                };
                let json = serde_json::to_string(&event).unwrap_or_default();
                if sender.send(Message::Text(json.into())).await.is_err() {
                    break;
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                            if val.get("cmd").and_then(|v| v.as_str()) == Some("sync") {
                                let _ = send_snapshot(&mut sender, &state).await;
                            } else if val.get("cmd").and_then(|v| v.as_str()) == Some("clock") {
                                let t = chrono::Utc::now().with_timezone(&state.tz).format("%H:%M:%S").to_string();
                                let _ = sender.send(Message::Text(json!({"type":"clock","time":t}).to_string().into())).await;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

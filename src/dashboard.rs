use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse},
};
use futures_util::{SinkExt, StreamExt};
use crate::events::AppEvent;
use crate::proxy::AppState;

pub async fn dashboard_handler() -> impl IntoResponse {
    Html(include_str!("dashboard.html"))
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let mut rx = state.event_tx.subscribe();
    let (mut sender, _receiver) = socket.split();

    while let Ok(event) = rx.recv().await {
        let json = serde_json::to_string(&event).unwrap_or_default();
        if sender.send(Message::Text(json.into())).await.is_err() {
            break;
        }
    }
}

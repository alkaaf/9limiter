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

pub async fn graph_page_handler() -> impl IntoResponse {
    Html(include_str!("graph.html"))
}

pub async fn graph_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let mut rx = state.event_tx.subscribe();
    let (mut sender, _receiver) = socket.split();

    loop {
        match rx.recv().await {
            Ok(event @ AppEvent::Request(_)) => {
                let json = serde_json::to_string(&event).unwrap();
                let _ = sender.send(Message::Text(json.into())).await;
            }
            Ok(event @ AppEvent::RequestEnd(_)) => {
                let json = serde_json::to_string(&event).unwrap();
                let _ = sender.send(Message::Text(json.into())).await;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
}

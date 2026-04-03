use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, error, info, warn};

use herald_core::{ClientMessage, EndpointId, ErrorCode, ServerMessage};

use crate::error::BrokerError;
use crate::router::{EndpointSender, Router};
use crate::store::MessageStore;

/// Shared broker state behind a mutex.
pub struct BrokerState {
    pub router: Router,
    pub store: MessageStore,
    pub token: String,
}

/// Starts the WebSocket server and accepts connections until the shutdown signal.
pub async fn run(
    listener: TcpListener,
    state: Arc<Mutex<BrokerState>>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    info!("broker listening on {}", listener.local_addr().unwrap());

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, addr)) => {
                        let state = Arc::clone(&state);
                        tokio::spawn(handle_connection(stream, addr, state));
                    }
                    Err(e) => {
                        error!("accept error: {e}");
                    }
                }
            }
            _ = shutdown.recv() => {
                info!("shutting down server");
                break;
            }
        }
    }
}

/// Handles a single WebSocket connection through auth, registration, and message loop.
async fn handle_connection(stream: TcpStream, addr: SocketAddr, state: Arc<Mutex<BrokerState>>) {
    debug!("new connection from {addr}");

    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            warn!("WebSocket handshake failed for {addr}: {e}");
            return;
        }
    };

    let (mut ws_sink, mut ws_stream) = ws_stream.split();

    // Phase 1: Authenticate.
    let authenticated = match wait_for_auth(&mut ws_stream, &state).await {
        Some(true) => true,
        _ => {
            let msg = ServerMessage::AuthResult {
                success: false,
                error: Some("authentication failed or timed out".into()),
            };
            let _ = send_server_message(&mut ws_sink, &msg).await;
            return;
        }
    };

    if !authenticated {
        return;
    }

    let msg = ServerMessage::AuthResult {
        success: true,
        error: None,
    };
    if send_server_message(&mut ws_sink, &msg).await.is_err() {
        return;
    }

    // Phase 2: Validate and reserve endpoint name (without registering a sender yet).
    let endpoint_id = match wait_for_register(&mut ws_stream, &mut ws_sink, &state).await {
        Some(id) => id,
        None => return,
    };

    // Phase 3: Message loop — register with the real channel sender and deliver pending.
    let (tx, mut rx): (EndpointSender, mpsc::UnboundedReceiver<ServerMessage>) =
        mpsc::unbounded_channel();

    {
        let mut broker = state.lock().unwrap();
        let _ = broker.router.register(endpoint_id.clone(), tx);
        broker.router.deliver_pending(&endpoint_id, &broker.store);
    }

    info!("endpoint {endpoint_id} connected from {addr}");

    loop {
        tokio::select! {
            // Outgoing messages to client.
            Some(msg) = rx.recv() => {
                if send_server_message(&mut ws_sink, &msg).await.is_err() {
                    break;
                }
            }
            // Incoming messages from client.
            frame = ws_stream.next() => {
                match frame {
                    Some(Ok(WsMessage::Text(text))) => {
                        handle_client_text(&text, &endpoint_id, &state, &mut ws_sink).await;
                    }
                    Some(Ok(WsMessage::Close(_))) | None => {
                        debug!("endpoint {endpoint_id} disconnected");
                        break;
                    }
                    Some(Ok(_)) => {
                        // Ignore binary/ping/pong frames.
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error for {endpoint_id}: {e}");
                        break;
                    }
                }
            }
        }
    }

    // Cleanup.
    let mut broker = state.lock().unwrap();
    broker.router.unregister(&endpoint_id);
    info!("endpoint {endpoint_id} disconnected from {addr}");
}

/// Waits for the first message to be an Auth message. Returns Some(true) on success.
async fn wait_for_auth(
    ws_stream: &mut (
             impl StreamExt<Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>> + Unpin
         ),
    state: &Arc<Mutex<BrokerState>>,
) -> Option<bool> {
    while let Some(frame) = ws_stream.next().await {
        match frame {
            Ok(WsMessage::Text(text)) => {
                if let Ok(ClientMessage::Auth { token }) = serde_json::from_str(&text) {
                    let broker = state.lock().unwrap();
                    return Some(crate::auth::verify_token(&broker.token, &token));
                }
                return Some(false);
            }
            Ok(WsMessage::Close(_)) => return None,
            _ => continue,
        }
    }
    None
}

/// Waits for a Register message after authentication. Validates the name is
/// available but does not register a sender — the caller registers with the
/// real channel sender after this returns.
async fn wait_for_register(
    ws_stream: &mut (
             impl StreamExt<Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>> + Unpin
         ),
    ws_sink: &mut (impl SinkExt<WsMessage> + Unpin),
    state: &Arc<Mutex<BrokerState>>,
) -> Option<EndpointId> {
    while let Some(frame) = ws_stream.next().await {
        match frame {
            Ok(WsMessage::Text(text)) => {
                if let Ok(ClientMessage::Register { name }) = serde_json::from_str(&text) {
                    let name_taken = state.lock().unwrap().router.is_registered(&name);
                    if name_taken {
                        let msg = ServerMessage::Error {
                            code: ErrorCode::NameTaken,
                            message: format!("endpoint name '{name}' is already taken"),
                        };
                        let _ = send_server_message(ws_sink, &msg).await;
                        return None;
                    }

                    let msg = ServerMessage::Registered { name: name.clone() };
                    let _ = send_server_message(ws_sink, &msg).await;
                    return Some(name);
                }
                let msg = ServerMessage::Error {
                    code: ErrorCode::NotRegistered,
                    message: "expected Register message".into(),
                };
                let _ = send_server_message(ws_sink, &msg).await;
                return None;
            }
            Ok(WsMessage::Close(_)) => return None,
            _ => continue,
        }
    }
    None
}

/// Processes a text frame from an authenticated, registered client.
async fn handle_client_text(
    text: &str,
    endpoint_id: &EndpointId,
    state: &Arc<Mutex<BrokerState>>,
    ws_sink: &mut (impl SinkExt<WsMessage> + Unpin),
) {
    let client_msg: ClientMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            let msg = ServerMessage::Error {
                code: ErrorCode::InternalError,
                message: format!("invalid message: {e}"),
            };
            let _ = send_server_message(ws_sink, &msg).await;
            return;
        }
    };

    match client_msg {
        ClientMessage::Send { id, mut message } => {
            // Override the `from` field to prevent spoofing.
            message.from = endpoint_id.clone();
            {
                let broker = state.lock().unwrap();
                broker.router.route_message(&message, &broker.store);
            }
            let ack = ServerMessage::Ack { id };
            let _ = send_server_message(ws_sink, &ack).await;
        }
        ClientMessage::Subscribe { topic } => {
            let result = {
                let mut broker = state.lock().unwrap();
                broker.router.subscribe(endpoint_id, topic.clone())
            };
            match result {
                Ok(()) => {
                    let msg = ServerMessage::Subscribed { topic };
                    let _ = send_server_message(ws_sink, &msg).await;
                }
                Err(code) => {
                    let msg = ServerMessage::Error {
                        code,
                        message: "subscribe failed".into(),
                    };
                    let _ = send_server_message(ws_sink, &msg).await;
                }
            }
        }
        ClientMessage::Unsubscribe { topic } => {
            {
                let mut broker = state.lock().unwrap();
                broker.router.unsubscribe(endpoint_id, &topic);
            }
            let msg = ServerMessage::Unsubscribed { topic };
            let _ = send_server_message(ws_sink, &msg).await;
        }
        ClientMessage::ListEndpoints => {
            let endpoints = state.lock().unwrap().router.list_endpoints();
            let msg = ServerMessage::EndpointList { endpoints };
            let _ = send_server_message(ws_sink, &msg).await;
        }
        ClientMessage::ListTopics => {
            let topics = state.lock().unwrap().router.list_topics();
            let msg = ServerMessage::TopicList { topics };
            let _ = send_server_message(ws_sink, &msg).await;
        }
        ClientMessage::Auth { .. } => {
            let msg = ServerMessage::Error {
                code: ErrorCode::InternalError,
                message: "already authenticated".into(),
            };
            let _ = send_server_message(ws_sink, &msg).await;
        }
        ClientMessage::Register { .. } => {
            let msg = ServerMessage::Error {
                code: ErrorCode::InternalError,
                message: "already registered".into(),
            };
            let _ = send_server_message(ws_sink, &msg).await;
        }
    }
}

/// Serializes and sends a `ServerMessage` over the WebSocket.
async fn send_server_message(
    sink: &mut (impl SinkExt<WsMessage> + Unpin),
    msg: &ServerMessage,
) -> Result<(), BrokerError> {
    let json = serde_json::to_string(msg)?;
    sink.send(WsMessage::Text(json.into()))
        .await
        .map_err(|_| BrokerError::Protocol("failed to send message".into()))
}

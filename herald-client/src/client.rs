use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use herald_core::{Address, ClientMessage, EndpointId, Message, ServerMessage, Topic};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, warn};

use crate::config::ClientConfig;
use crate::error::ClientError;
use crate::reconnect::ReconnectPolicy;

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;
type Result<T> = std::result::Result<T, ClientError>;

/// Pending request state for commands that expect a specific response from the broker.
enum Pending {
    Subscribe(Topic, oneshot::Sender<Result<()>>),
    Unsubscribe(Topic, oneshot::Sender<Result<()>>),
    ListEndpoints(oneshot::Sender<Result<Vec<EndpointId>>>),
    ListTopics(oneshot::Sender<Result<Vec<Topic>>>),
    Send(oneshot::Sender<Result<()>>),
}

/// Commands sent from the public API to the background connection task.
enum Command {
    Send {
        message: Message,
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        topic: Topic,
        reply: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        topic: Topic,
        reply: oneshot::Sender<Result<()>>,
    },
    ListEndpoints {
        reply: oneshot::Sender<Result<Vec<EndpointId>>>,
    },
    ListTopics {
        reply: oneshot::Sender<Result<Vec<Topic>>>,
    },
    Disconnect {
        reply: oneshot::Sender<Result<()>>,
    },
}

/// A client for the Herald message broker.
///
/// Maintains a WebSocket connection, handles authentication, and provides
/// async methods for sending and receiving messages. Automatically reconnects
/// on connection loss using exponential backoff.
pub struct HeraldClient {
    cmd_tx: mpsc::Sender<Command>,
    msg_rx: mpsc::Receiver<Message>,
    _task: tokio::task::JoinHandle<()>,
}

impl HeraldClient {
    /// Connects to the broker, authenticates, and registers the given endpoint name.
    pub async fn connect(config: ClientConfig, name: &str) -> Result<Self> {
        let endpoint_id =
            EndpointId::new(name).map_err(|e| ClientError::Protocol(e.to_string()))?;

        let token = read_token(&config.token_path).await?;

        let (ws_sink, mut ws_stream) = establish_connection(&config.broker_url).await?;
        let ws_sink = Arc::new(Mutex::new(ws_sink));

        // Auth handshake.
        send_raw(
            &ws_sink,
            &ClientMessage::Auth {
                token: token.clone(),
            },
        )
        .await?;
        let auth_response = recv_raw(&mut ws_stream).await?;
        match auth_response {
            ServerMessage::AuthResult { success: true, .. } => {
                debug!("authenticated with broker");
            }
            ServerMessage::AuthResult {
                success: false,
                error,
            } => {
                return Err(ClientError::Auth(
                    error.unwrap_or_else(|| "unknown auth error".into()),
                ));
            }
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected AuthResult, got: {other:?}"
                )));
            }
        }

        // Register endpoint.
        send_raw(
            &ws_sink,
            &ClientMessage::Register {
                name: endpoint_id.clone(),
            },
        )
        .await?;
        let reg_response = recv_raw(&mut ws_stream).await?;
        match reg_response {
            ServerMessage::Registered { .. } => {
                debug!(name = %endpoint_id, "registered with broker");
            }
            ServerMessage::Error { code, message } => {
                return Err(ClientError::Broker { code, message });
            }
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected Registered, got: {other:?}"
                )));
            }
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (msg_tx, msg_rx) = mpsc::channel(256);

        let task = tokio::spawn(connection_task(
            config,
            endpoint_id,
            token,
            ws_sink,
            ws_stream,
            cmd_rx,
            msg_tx,
        ));

        Ok(Self {
            cmd_tx,
            msg_rx,
            _task: task,
        })
    }

    /// Sends a message to the given address.
    pub async fn send_message(
        &self,
        to: Address,
        payload: serde_json::Value,
        metadata: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let message = Message {
            from: EndpointId::new("placeholder").unwrap(),
            to,
            payload,
            metadata,
            timestamp: now_ms(),
        };
        self.cmd_tx
            .send(Command::Send {
                message,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ClientError::Send("client disconnected".into()))?;
        reply_rx
            .await
            .map_err(|_| ClientError::Send("no response from connection task".into()))?
    }

    /// Subscribes to a topic.
    pub async fn subscribe(&self, topic: Topic) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Subscribe {
                topic,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ClientError::Send("client disconnected".into()))?;
        reply_rx
            .await
            .map_err(|_| ClientError::Send("no response from connection task".into()))?
    }

    /// Unsubscribes from a topic.
    pub async fn unsubscribe(&self, topic: Topic) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Unsubscribe {
                topic,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ClientError::Send("client disconnected".into()))?;
        reply_rx
            .await
            .map_err(|_| ClientError::Send("no response from connection task".into()))?
    }

    /// Lists all endpoints currently registered with the broker.
    pub async fn list_endpoints(&self) -> Result<Vec<EndpointId>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::ListEndpoints { reply: reply_tx })
            .await
            .map_err(|_| ClientError::Send("client disconnected".into()))?;
        reply_rx
            .await
            .map_err(|_| ClientError::Send("no response from connection task".into()))?
    }

    /// Lists all active topics on the broker.
    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::ListTopics { reply: reply_tx })
            .await
            .map_err(|_| ClientError::Send("client disconnected".into()))?;
        reply_rx
            .await
            .map_err(|_| ClientError::Send("no response from connection task".into()))?
    }

    /// Returns a reference to the incoming message receiver.
    /// Use this to consume messages delivered to this endpoint.
    pub fn messages(&mut self) -> &mut mpsc::Receiver<Message> {
        &mut self.msg_rx
    }

    /// Disconnects from the broker gracefully.
    pub async fn disconnect(self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(Command::Disconnect { reply: reply_tx })
            .await;
        reply_rx
            .await
            .map_err(|_| ClientError::Send("no response from connection task".into()))?
    }
}

/// Background task that owns the WebSocket connection, processes commands,
/// dispatches incoming messages, and handles reconnection.
async fn connection_task(
    config: ClientConfig,
    endpoint_id: EndpointId,
    token: String,
    ws_sink: Arc<Mutex<WsSink>>,
    mut ws_stream: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    mut cmd_rx: mpsc::Receiver<Command>,
    msg_tx: mpsc::Sender<Message>,
) {
    let mut subscriptions: HashSet<Topic> = HashSet::new();
    let mut reconnect_policy = ReconnectPolicy::new(config.reconnect.clone());
    let mut pending: Vec<Pending> = Vec::new();

    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    Command::Send { message, reply } => {
                        let client_msg = ClientMessage::Send { id: None, message };
                        match send_raw(&ws_sink, &client_msg).await {
                            Ok(()) => {
                                pending.push(Pending::Send(reply));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Command::Subscribe { topic, reply } => {
                        let client_msg = ClientMessage::Subscribe { topic: topic.clone() };
                        match send_raw(&ws_sink, &client_msg).await {
                            Ok(()) => {
                                pending.push(Pending::Subscribe(topic, reply));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Command::Unsubscribe { topic, reply } => {
                        let client_msg = ClientMessage::Unsubscribe { topic: topic.clone() };
                        match send_raw(&ws_sink, &client_msg).await {
                            Ok(()) => {
                                pending.push(Pending::Unsubscribe(topic, reply));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Command::ListEndpoints { reply } => {
                        match send_raw(&ws_sink, &ClientMessage::ListEndpoints).await {
                            Ok(()) => {
                                pending.push(Pending::ListEndpoints(reply));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Command::ListTopics { reply } => {
                        match send_raw(&ws_sink, &ClientMessage::ListTopics).await {
                            Ok(()) => {
                                pending.push(Pending::ListTopics(reply));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Command::Disconnect { reply } => {
                        let mut sink = ws_sink.lock().await;
                        let result = sink.close().await
                            .map_err(|e| ClientError::Connection(e.to_string()));
                        let _ = reply.send(result);
                        return;
                    }
                }
            }
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(tungstenite::Message::Text(text))) => {
                        match serde_json::from_str::<ServerMessage>(&text) {
                            Ok(server_msg) => {
                                handle_server_message(
                                    server_msg,
                                    &mut pending,
                                    &mut subscriptions,
                                    &msg_tx,
                                ).await;
                            }
                            Err(e) => {
                                warn!("failed to parse server message: {e}");
                            }
                        }
                    }
                    Some(Ok(tungstenite::Message::Close(_))) | None => {
                        warn!("connection closed, attempting reconnect");
                        // Fail all pending requests.
                        fail_pending(&mut pending);

                        match attempt_reconnect(
                            &config,
                            &endpoint_id,
                            &token,
                            &subscriptions,
                            &mut reconnect_policy,
                        ).await {
                            Ok((new_sink, new_stream)) => {
                                *ws_sink.lock().await = new_sink;
                                ws_stream = new_stream;
                                reconnect_policy.reset();
                                debug!("reconnected to broker");
                            }
                            Err(e) => {
                                error!("reconnection failed permanently: {e}");
                                return;
                            }
                        }
                    }
                    Some(Ok(_)) => {
                        // Ignore non-text frames (ping/pong handled by tungstenite).
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error: {e}, attempting reconnect");
                        fail_pending(&mut pending);

                        match attempt_reconnect(
                            &config,
                            &endpoint_id,
                            &token,
                            &subscriptions,
                            &mut reconnect_policy,
                        ).await {
                            Ok((new_sink, new_stream)) => {
                                *ws_sink.lock().await = new_sink;
                                ws_stream = new_stream;
                                reconnect_policy.reset();
                                debug!("reconnected to broker");
                            }
                            Err(e) => {
                                error!("reconnection failed permanently: {e}");
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Dispatches a server message to the appropriate pending request or the
/// incoming message channel.
async fn handle_server_message(
    server_msg: ServerMessage,
    pending: &mut Vec<Pending>,
    subscriptions: &mut HashSet<Topic>,
    msg_tx: &mpsc::Sender<Message>,
) {
    match server_msg {
        ServerMessage::MessageReceived { message } => {
            if msg_tx.send(message).await.is_err() {
                warn!("message receiver dropped, discarding message");
            }
        }
        ServerMessage::Subscribed { topic } => {
            if let Some(idx) = pending
                .iter()
                .position(|p| matches!(p, Pending::Subscribe(t, _) if *t == topic))
            {
                subscriptions.insert(topic);
                if let Pending::Subscribe(_, reply) = pending.remove(idx) {
                    let _ = reply.send(Ok(()));
                }
            }
        }
        ServerMessage::Unsubscribed { topic } => {
            if let Some(idx) = pending
                .iter()
                .position(|p| matches!(p, Pending::Unsubscribe(t, _) if *t == topic))
            {
                subscriptions.remove(&topic);
                if let Pending::Unsubscribe(_, reply) = pending.remove(idx) {
                    let _ = reply.send(Ok(()));
                }
            }
        }
        ServerMessage::EndpointList { endpoints } => {
            if let Some(idx) = pending
                .iter()
                .position(|p| matches!(p, Pending::ListEndpoints(_)))
                && let Pending::ListEndpoints(reply) = pending.remove(idx)
            {
                let _ = reply.send(Ok(endpoints));
            }
        }
        ServerMessage::TopicList { topics } => {
            if let Some(idx) = pending
                .iter()
                .position(|p| matches!(p, Pending::ListTopics(_)))
                && let Pending::ListTopics(reply) = pending.remove(idx)
            {
                let _ = reply.send(Ok(topics));
            }
        }
        ServerMessage::Ack { .. } => {
            if let Some(idx) = pending.iter().position(|p| matches!(p, Pending::Send(_)))
                && let Pending::Send(reply) = pending.remove(idx)
            {
                let _ = reply.send(Ok(()));
            }
        }
        ServerMessage::Error { code, message } => {
            // Route the error to the oldest pending request.
            if !pending.is_empty() {
                let err = ClientError::Broker {
                    code,
                    message: message.clone(),
                };
                match pending.remove(0) {
                    Pending::Subscribe(_, reply) => {
                        let _ = reply.send(Err(err));
                    }
                    Pending::Unsubscribe(_, reply) => {
                        let _ = reply.send(Err(err));
                    }
                    Pending::ListEndpoints(reply) => {
                        let _ = reply.send(Err(err));
                    }
                    Pending::ListTopics(reply) => {
                        let _ = reply.send(Err(err));
                    }
                    Pending::Send(reply) => {
                        let _ = reply.send(Err(err));
                    }
                }
            } else {
                warn!("broker error with no pending request: [{code}] {message}");
            }
        }
        _ => {
            debug!("ignoring unexpected server message");
        }
    }
}

/// Fail all pending requests with a disconnection error.
fn fail_pending(pending: &mut Vec<Pending>) {
    let err_msg = "connection lost during pending request";
    for p in pending.drain(..) {
        match p {
            Pending::Subscribe(_, reply) => {
                let _ = reply.send(Err(ClientError::Connection(err_msg.into())));
            }
            Pending::Unsubscribe(_, reply) => {
                let _ = reply.send(Err(ClientError::Connection(err_msg.into())));
            }
            Pending::ListEndpoints(reply) => {
                let _ = reply.send(Err(ClientError::Connection(err_msg.into())));
            }
            Pending::ListTopics(reply) => {
                let _ = reply.send(Err(ClientError::Connection(err_msg.into())));
            }
            Pending::Send(reply) => {
                let _ = reply.send(Err(ClientError::Connection(err_msg.into())));
            }
        }
    }
}

/// Attempts to reconnect to the broker with exponential backoff, re-authenticate,
/// re-register, and re-subscribe to all previous topics.
async fn attempt_reconnect(
    config: &ClientConfig,
    endpoint_id: &EndpointId,
    token: &str,
    subscriptions: &HashSet<Topic>,
    policy: &mut ReconnectPolicy,
) -> Result<(
    WsSink,
    futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
)> {
    loop {
        let delay = policy
            .next_delay()
            .ok_or_else(|| ClientError::Connection("max reconnect retries exceeded".into()))?;

        debug!(?delay, "waiting before reconnect attempt");
        tokio::time::sleep(delay).await;

        // Try to establish a new connection.
        let (ws_sink, mut ws_stream) = match establish_connection(&config.broker_url).await {
            Ok(pair) => pair,
            Err(e) => {
                warn!("reconnect attempt failed: {e}");
                continue;
            }
        };
        let ws_sink = Arc::new(Mutex::new(ws_sink));

        // Re-authenticate.
        if let Err(e) = send_raw(
            &ws_sink,
            &ClientMessage::Auth {
                token: token.to_owned(),
            },
        )
        .await
        {
            warn!("reconnect auth send failed: {e}");
            continue;
        }
        match recv_raw(&mut ws_stream).await {
            Ok(ServerMessage::AuthResult { success: true, .. }) => {}
            Ok(ServerMessage::AuthResult {
                success: false,
                error,
            }) => {
                return Err(ClientError::Auth(
                    error.unwrap_or_else(|| "auth failed on reconnect".into()),
                ));
            }
            Ok(other) => {
                warn!("unexpected response during reconnect auth: {other:?}");
                continue;
            }
            Err(e) => {
                warn!("reconnect auth recv failed: {e}");
                continue;
            }
        }

        // Re-register.
        if let Err(e) = send_raw(
            &ws_sink,
            &ClientMessage::Register {
                name: endpoint_id.clone(),
            },
        )
        .await
        {
            warn!("reconnect register send failed: {e}");
            continue;
        }
        match recv_raw(&mut ws_stream).await {
            Ok(ServerMessage::Registered { .. }) => {}
            Ok(ServerMessage::Error { code, message }) => {
                return Err(ClientError::Broker { code, message });
            }
            Ok(other) => {
                warn!("unexpected response during reconnect register: {other:?}");
                continue;
            }
            Err(e) => {
                warn!("reconnect register recv failed: {e}");
                continue;
            }
        }

        // Re-subscribe to all topics.
        for topic in subscriptions {
            if let Err(e) = send_raw(
                &ws_sink,
                &ClientMessage::Subscribe {
                    topic: topic.clone(),
                },
            )
            .await
            {
                warn!(topic = %topic, "reconnect subscribe send failed: {e}");
                continue;
            }
            match recv_raw(&mut ws_stream).await {
                Ok(ServerMessage::Subscribed { .. }) => {}
                Ok(other) => {
                    warn!(topic = %topic, "unexpected response during reconnect subscribe: {other:?}");
                }
                Err(e) => {
                    warn!(topic = %topic, "reconnect subscribe recv failed: {e}");
                }
            }
        }

        let sink = Arc::into_inner(ws_sink)
            .expect("sink has no other references during reconnect")
            .into_inner();
        return Ok((sink, ws_stream));
    }
}

/// Establishes a raw WebSocket connection and splits it into read/write halves.
async fn establish_connection(
    url: &str,
) -> Result<(
    WsSink,
    futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
)> {
    let (ws_stream, _response) = connect_async(url)
        .await
        .map_err(|e| ClientError::Connection(e.to_string()))?;
    Ok(ws_stream.split())
}

/// Sends a serialized `ClientMessage` through the WebSocket sink.
async fn send_raw(sink: &Arc<Mutex<WsSink>>, msg: &ClientMessage) -> Result<()> {
    let text = serde_json::to_string(msg)?;
    let mut guard = sink.lock().await;
    guard
        .send(tungstenite::Message::text(text))
        .await
        .map_err(|e| ClientError::Send(e.to_string()))
}

/// Reads the next text frame from the WebSocket stream and deserializes it.
async fn recv_raw(
    stream: &mut futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
) -> Result<ServerMessage> {
    loop {
        match stream.next().await {
            Some(Ok(tungstenite::Message::Text(text))) => {
                return serde_json::from_str::<ServerMessage>(&text)
                    .map_err(|e| ClientError::Protocol(e.to_string()));
            }
            Some(Ok(_)) => {
                // Skip non-text frames.
                continue;
            }
            Some(Err(e)) => {
                return Err(ClientError::Receive(e.to_string()));
            }
            None => {
                return Err(ClientError::Receive("connection closed".into()));
            }
        }
    }
}

/// Reads the authentication token from the given path.
async fn read_token(path: &std::path::Path) -> Result<String> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| ClientError::TokenRead(format!("{}: {e}", path.display())))?;
    Ok(contents.trim().to_owned())
}

/// Returns the current time in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

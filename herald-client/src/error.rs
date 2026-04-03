use herald_core::ErrorCode;
use thiserror::Error;

/// Errors produced by the herald client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// WebSocket connection failure.
    #[error("connection error: {0}")]
    Connection(String),

    /// Authentication rejected by the broker.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// Failed to send a message through the WebSocket.
    #[error("send error: {0}")]
    Send(String),

    /// Failed to receive a message from the WebSocket.
    #[error("receive error: {0}")]
    Receive(String),

    /// Unexpected protocol message or format.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Failed to read the authentication token from disk.
    #[error("token read error: {0}")]
    TokenRead(String),

    /// The broker returned an error with a structured code.
    #[error("broker error ({code}): {message}")]
    Broker { code: ErrorCode, message: String },
}

impl From<tokio_tungstenite::tungstenite::Error> for ClientError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        ClientError::Connection(err.to_string())
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(err: serde_json::Error) -> Self {
        ClientError::Protocol(err.to_string())
    }
}

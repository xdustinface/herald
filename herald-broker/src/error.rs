use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),

    #[error("websocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("protocol error: {0}")]
    Protocol(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for BrokerError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "missing");
        let err = BrokerError::from(io_err);
        assert!(matches!(err, BrokerError::Io(_)));
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn protocol_error_displays() {
        let err = BrokerError::Protocol("bad frame".into());
        assert_eq!(err.to_string(), "protocol error: bad frame");
    }

    #[test]
    fn json_error_converts() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err = BrokerError::from(json_err);
        assert!(matches!(err, BrokerError::Json(_)));
    }
}

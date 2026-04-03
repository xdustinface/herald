use std::fmt;

use thiserror::Error;

/// Errors produced by herald-core operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Name validation failure.
    #[error("invalid name: {0}")]
    InvalidName(String),

    /// Serialization or deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Protocol-level violation.
    #[error("protocol error: {0}")]
    Protocol(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

/// Structured error codes sent over the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ErrorCode {
    AuthFailed,
    NameTaken,
    InvalidName,
    NotRegistered,
    EndpointNotFound,
    InternalError,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::AuthFailed => write!(f, "AuthFailed"),
            ErrorCode::NameTaken => write!(f, "NameTaken"),
            ErrorCode::InvalidName => write!(f, "InvalidName"),
            ErrorCode::NotRegistered => write!(f, "NotRegistered"),
            ErrorCode::EndpointNotFound => write!(f, "EndpointNotFound"),
            ErrorCode::InternalError => write!(f, "InternalError"),
        }
    }
}

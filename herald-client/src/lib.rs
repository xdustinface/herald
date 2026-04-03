//! Client library for connecting to the Herald message broker.
//!
//! Provides [`HeraldClient`] for WebSocket-based communication with the broker,
//! including authentication, message sending/receiving, topic subscriptions,
//! and automatic reconnection with exponential backoff.

mod client;
mod config;
mod error;
mod reconnect;

pub use client::HeraldClient;
pub use config::{ClientConfig, ClientConfigBuilder, ReconnectConfig};
pub use error::ClientError;

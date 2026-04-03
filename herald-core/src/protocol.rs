use serde::{Deserialize, Serialize};

use crate::{EndpointId, ErrorCode, Message, Topic};

/// Messages sent from a client to the broker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    /// Authenticate with the broker.
    Auth { token: String },
    /// Register an endpoint name.
    Register { name: EndpointId },
    /// Send a message through the broker.
    Send {
        /// Optional request ID for correlating with `Ack` responses.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        message: Message,
    },
    /// Subscribe to a topic.
    Subscribe { topic: Topic },
    /// Unsubscribe from a topic.
    Unsubscribe { topic: Topic },
    /// List all registered endpoints.
    ListEndpoints,
    /// List all active topics.
    ListTopics,
}

/// Messages sent from the broker to a client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    /// Result of an authentication attempt.
    AuthResult {
        success: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Confirmation that an endpoint was registered.
    Registered { name: EndpointId },
    /// A message delivered to this client.
    MessageReceived { message: Message },
    /// Confirmation that a topic subscription was created.
    Subscribed { topic: Topic },
    /// Confirmation that a topic subscription was removed.
    Unsubscribed { topic: Topic },
    /// List of all registered endpoints.
    EndpointList { endpoints: Vec<EndpointId> },
    /// List of all active topics.
    TopicList { topics: Vec<Topic> },
    /// An error from the broker.
    Error { code: ErrorCode, message: String },
    /// Acknowledgement of a processed message.
    Ack {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
}

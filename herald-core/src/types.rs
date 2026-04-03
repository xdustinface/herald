use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Error;

/// Maximum length for endpoint and topic names.
const MAX_NAME_LEN: usize = 64;

/// Validates a name string: alphanumeric + hyphens, 1-64 chars, starts with alphanumeric.
fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::InvalidName("name must not be empty".into()));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(Error::InvalidName(format!(
            "name exceeds maximum length of {MAX_NAME_LEN} characters"
        )));
    }
    if !name
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphanumeric())
    {
        return Err(Error::InvalidName(
            "name must start with an alphanumeric character".into(),
        ));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        return Err(Error::InvalidName(
            "name must contain only alphanumeric characters and hyphens".into(),
        ));
    }
    Ok(())
}

/// A unique identifier for an endpoint connected to the broker.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EndpointId(String);

impl EndpointId {
    /// Creates a new `EndpointId` after validating the name.
    pub fn new(name: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self(name))
    }

    /// Returns the name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EndpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for EndpointId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for EndpointId {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<EndpointId> for String {
    fn from(id: EndpointId) -> Self {
        id.0
    }
}

/// A topic for pub/sub messaging.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Topic(String);

impl Topic {
    /// Creates a new `Topic` after validating the name.
    pub fn new(name: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self(name))
    }

    /// Returns the topic name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Topic {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for Topic {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<Topic> for String {
    fn from(topic: Topic) -> Self {
        topic.0
    }
}

/// The target address for a message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Address {
    /// Send to a specific endpoint.
    Direct(EndpointId),
    /// Publish to a topic.
    Topic(Topic),
    /// Broadcast to all connected endpoints.
    Broadcast,
}

/// A message envelope exchanged through the broker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// The sender endpoint.
    pub from: EndpointId,
    /// The destination address.
    pub to: Address,
    /// Opaque JSON payload.
    pub payload: serde_json::Value,
    /// Optional key-value metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
}

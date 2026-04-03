//! Core types and protocol for the Herald message broker.

mod error;
mod protocol;
mod types;

pub use error::{Error, ErrorCode};
pub use protocol::{ClientMessage, ServerMessage};
pub use types::{Address, EndpointId, Message, Topic};

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;

    // --- EndpointId validation ---

    #[test]
    fn endpoint_id_valid_names() {
        assert!(EndpointId::new("alice").is_ok());
        assert!(EndpointId::new("Bob").is_ok());
        assert!(EndpointId::new("node-1").is_ok());
        assert!(EndpointId::new("a").is_ok());
        assert!(EndpointId::new("A1-b2-c3").is_ok());
        // exactly 64 chars
        let long = "a".repeat(64);
        assert!(EndpointId::new(long).is_ok());
    }

    #[test]
    fn endpoint_id_invalid_names() {
        // empty
        assert!(EndpointId::new("").is_err());
        // starts with hyphen
        assert!(EndpointId::new("-alice").is_err());
        // contains underscore
        assert!(EndpointId::new("alice_bob").is_err());
        // contains space
        assert!(EndpointId::new("alice bob").is_err());
        // too long
        let long = "a".repeat(65);
        assert!(EndpointId::new(long).is_err());
        // starts with digit is ok, but let's verify
        assert!(EndpointId::new("1abc").is_ok());
    }

    #[test]
    fn endpoint_id_display_and_from_str() {
        let id: EndpointId = "my-endpoint".parse().unwrap();
        assert_eq!(id.to_string(), "my-endpoint");
        assert_eq!(id.as_str(), "my-endpoint");
    }

    // --- Topic validation ---

    #[test]
    fn topic_valid_names() {
        assert!(Topic::new("events").is_ok());
        assert!(Topic::new("chat-room-1").is_ok());
    }

    #[test]
    fn topic_invalid_names() {
        assert!(Topic::new("").is_err());
        assert!(Topic::new("-bad").is_err());
        assert!(Topic::new("has space").is_err());
    }

    // --- EndpointId serde roundtrip ---

    #[test]
    fn endpoint_id_serde_roundtrip() {
        let id = EndpointId::new("test-ep").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"test-ep\"");
        let back: EndpointId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn endpoint_id_serde_rejects_invalid() {
        let result: Result<EndpointId, _> = serde_json::from_str("\"\"");
        assert!(result.is_err());
        let result: Result<EndpointId, _> = serde_json::from_str("\"-bad\"");
        assert!(result.is_err());
    }

    // --- Topic serde roundtrip ---

    #[test]
    fn topic_serde_roundtrip() {
        let topic = Topic::new("my-topic").unwrap();
        let json = serde_json::to_string(&topic).unwrap();
        assert_eq!(json, "\"my-topic\"");
        let back: Topic = serde_json::from_str(&json).unwrap();
        assert_eq!(topic, back);
    }

    // --- Address serialization ---

    #[test]
    fn address_serde_direct() {
        let addr = Address::Direct(EndpointId::new("bob").unwrap());
        let json = serde_json::to_string(&addr).unwrap();
        let back: Address = serde_json::from_str(&json).unwrap();
        assert_eq!(addr, back);
        // verify tagged format
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "Direct");
        assert_eq!(v["value"], "bob");
    }

    #[test]
    fn address_serde_topic() {
        let addr = Address::Topic(Topic::new("events").unwrap());
        let json = serde_json::to_string(&addr).unwrap();
        let back: Address = serde_json::from_str(&json).unwrap();
        assert_eq!(addr, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "Topic");
    }

    #[test]
    fn address_serde_broadcast() {
        let addr = Address::Broadcast;
        let json = serde_json::to_string(&addr).unwrap();
        let back: Address = serde_json::from_str(&json).unwrap();
        assert_eq!(addr, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "Broadcast");
    }

    // --- Message serialization roundtrip ---

    #[test]
    fn message_serde_roundtrip() {
        let msg = Message {
            from: EndpointId::new("alice").unwrap(),
            to: Address::Direct(EndpointId::new("bob").unwrap()),
            payload: json!({"text": "hello"}),
            metadata: None,
            timestamp: 1700000000000,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn message_with_metadata_roundtrip() {
        let mut meta = HashMap::new();
        meta.insert("priority".to_string(), json!("high"));
        meta.insert("ttl".to_string(), json!(60));

        let msg = Message {
            from: EndpointId::new("sender").unwrap(),
            to: Address::Topic(Topic::new("notifications").unwrap()),
            payload: json!([1, 2, 3]),
            metadata: Some(meta),
            timestamp: 1700000000000,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn message_metadata_omitted_when_none() {
        let msg = Message {
            from: EndpointId::new("a").unwrap(),
            to: Address::Broadcast,
            payload: json!(null),
            metadata: None,
            timestamp: 0,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("metadata"));
    }

    // --- ClientMessage serialization roundtrip ---

    #[test]
    fn client_message_auth_roundtrip() {
        let msg = ClientMessage::Auth {
            token: "secret".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "Auth");
    }

    #[test]
    fn client_message_register_roundtrip() {
        let msg = ClientMessage::Register {
            name: EndpointId::new("my-service").unwrap(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn client_message_send_roundtrip() {
        let msg = ClientMessage::Send {
            id: None,
            message: Message {
                from: EndpointId::new("a").unwrap(),
                to: Address::Direct(EndpointId::new("b").unwrap()),
                payload: json!("hello"),
                metadata: None,
                timestamp: 123,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        assert!(!json.contains("\"id\""));

        let msg_with_id = ClientMessage::Send {
            id: Some("req-42".into()),
            message: Message {
                from: EndpointId::new("a").unwrap(),
                to: Address::Direct(EndpointId::new("b").unwrap()),
                payload: json!("hello"),
                metadata: None,
                timestamp: 123,
            },
        };
        let json = serde_json::to_string(&msg_with_id).unwrap();
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg_with_id, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["id"], "req-42");
    }

    #[test]
    fn client_message_subscribe_roundtrip() {
        let msg = ClientMessage::Subscribe {
            topic: Topic::new("events").unwrap(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "Subscribe");
    }

    #[test]
    fn client_message_unsubscribe_roundtrip() {
        let msg = ClientMessage::Unsubscribe {
            topic: Topic::new("events").unwrap(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "Unsubscribe");
    }

    #[test]
    fn client_message_list_roundtrips() {
        for msg in [ClientMessage::ListEndpoints, ClientMessage::ListTopics] {
            let json = serde_json::to_string(&msg).unwrap();
            let back: ClientMessage = serde_json::from_str(&json).unwrap();
            assert_eq!(msg, back);
        }
    }

    // --- ServerMessage serialization roundtrip ---

    #[test]
    fn server_message_auth_result_roundtrip() {
        let msg = ServerMessage::AuthResult {
            success: true,
            error: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);

        let msg_err = ServerMessage::AuthResult {
            success: false,
            error: Some("bad token".into()),
        };
        let json = serde_json::to_string(&msg_err).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg_err, back);
    }

    #[test]
    fn server_message_registered_roundtrip() {
        let msg = ServerMessage::Registered {
            name: EndpointId::new("svc").unwrap(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn server_message_error_roundtrip() {
        let msg = ServerMessage::Error {
            code: ErrorCode::NameTaken,
            message: "already registered".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn server_message_endpoint_list_roundtrip() {
        let msg = ServerMessage::EndpointList {
            endpoints: vec![EndpointId::new("a").unwrap(), EndpointId::new("b").unwrap()],
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn server_message_received_roundtrip() {
        let msg = ServerMessage::MessageReceived {
            message: Message {
                from: EndpointId::new("alice").unwrap(),
                to: Address::Direct(EndpointId::new("bob").unwrap()),
                payload: json!({"text": "hello"}),
                metadata: None,
                timestamp: 1700000000000,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "MessageReceived");
    }

    #[test]
    fn server_message_subscribed_roundtrip() {
        let msg = ServerMessage::Subscribed {
            topic: Topic::new("events").unwrap(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "Subscribed");
    }

    #[test]
    fn server_message_unsubscribed_roundtrip() {
        let msg = ServerMessage::Unsubscribed {
            topic: Topic::new("events").unwrap(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "Unsubscribed");
    }

    #[test]
    fn server_message_topic_list_roundtrip() {
        let msg = ServerMessage::TopicList {
            topics: vec![Topic::new("events").unwrap(), Topic::new("chat").unwrap()],
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "TopicList");
    }

    #[test]
    fn server_message_ack_roundtrip() {
        let msg = ServerMessage::Ack { id: None };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
        assert!(!json.contains("id"));

        let msg = ServerMessage::Ack {
            id: Some("req-42".into()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    // --- Error display ---

    #[test]
    fn error_from_serde_json() {
        let serde_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err_msg = serde_err.to_string();
        let err: Error = serde_err.into();
        assert!(matches!(err, Error::Serialization(ref s) if s == &err_msg));
    }

    #[test]
    fn error_display() {
        let e = Error::InvalidName("bad".into());
        assert_eq!(e.to_string(), "invalid name: bad");

        let e = Error::Serialization("parse failed".into());
        assert_eq!(e.to_string(), "serialization error: parse failed");

        let e = Error::Protocol("unexpected frame".into());
        assert_eq!(e.to_string(), "protocol error: unexpected frame");
    }

    #[test]
    fn error_code_display() {
        assert_eq!(ErrorCode::AuthFailed.to_string(), "AuthFailed");
        assert_eq!(ErrorCode::NameTaken.to_string(), "NameTaken");
        assert_eq!(ErrorCode::EndpointNotFound.to_string(), "EndpointNotFound");
    }

    #[test]
    fn error_to_error_code_conversion() {
        assert_eq!(
            ErrorCode::from(&Error::InvalidName("bad".into())),
            ErrorCode::InvalidName,
        );
        assert_eq!(
            ErrorCode::from(&Error::Serialization("parse failed".into())),
            ErrorCode::InternalError,
        );
        assert_eq!(
            ErrorCode::from(&Error::Protocol("unexpected".into())),
            ErrorCode::InternalError,
        );
    }
}

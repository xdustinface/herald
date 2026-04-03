use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc;
use tracing::{debug, warn};

use herald_core::{Address, EndpointId, ErrorCode, Message, ServerMessage, Topic};

use crate::store::MessageStore;

/// Sender half for delivering messages to a connected endpoint.
pub type EndpointSender = mpsc::UnboundedSender<ServerMessage>;

/// Manages connected endpoints, topic subscriptions, and message routing.
pub struct Router {
    /// Connected endpoints and their message senders.
    endpoints: HashMap<EndpointId, EndpointSender>,
    /// Topic subscriptions: topic -> set of subscribed endpoint ids.
    subscriptions: HashMap<Topic, HashSet<EndpointId>>,
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Router {
    pub fn new() -> Self {
        Self {
            endpoints: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }

    /// Returns whether an endpoint with the given id is currently registered.
    pub(crate) fn is_registered(&self, id: &EndpointId) -> bool {
        self.endpoints.contains_key(id)
    }

    /// Registers a new endpoint. Returns an error if the name is already taken.
    pub fn register(&mut self, id: EndpointId, sender: EndpointSender) -> Result<(), ErrorCode> {
        if self.endpoints.contains_key(&id) {
            return Err(ErrorCode::NameTaken);
        }
        debug!("endpoint registered: {id}");
        self.endpoints.insert(id, sender);
        Ok(())
    }

    /// Removes an endpoint and its subscriptions.
    pub fn unregister(&mut self, id: &EndpointId) {
        self.endpoints.remove(id);
        for subscribers in self.subscriptions.values_mut() {
            subscribers.remove(id);
        }
        // Remove empty topics.
        self.subscriptions.retain(|_, subs| !subs.is_empty());
        debug!("endpoint unregistered: {id}");
    }

    /// Subscribes an endpoint to a topic.
    pub fn subscribe(&mut self, id: &EndpointId, topic: Topic) -> Result<(), ErrorCode> {
        if !self.endpoints.contains_key(id) {
            return Err(ErrorCode::NotRegistered);
        }
        self.subscriptions
            .entry(topic.clone())
            .or_default()
            .insert(id.clone());
        debug!("endpoint {id} subscribed to {topic}");
        Ok(())
    }

    /// Unsubscribes an endpoint from a topic.
    pub fn unsubscribe(&mut self, id: &EndpointId, topic: &Topic) {
        if let Some(subs) = self.subscriptions.get_mut(topic) {
            subs.remove(id);
            if subs.is_empty() {
                self.subscriptions.remove(topic);
            }
        }
        debug!("endpoint {id} unsubscribed from {topic}");
    }

    /// Routes a message to its destination. Persists to store if the recipient is offline.
    pub fn route_message(&self, message: &Message, store: &MessageStore) {
        match &message.to {
            Address::Direct(recipient) => {
                if let Some(sender) = self.endpoints.get(recipient) {
                    let msg = ServerMessage::MessageReceived {
                        message: message.clone(),
                    };
                    if sender.send(msg).is_err() {
                        warn!("failed to deliver message to {recipient}, persisting");
                        let _ = store.save_pending(recipient, message);
                    }
                } else {
                    debug!("recipient {recipient} offline, persisting message");
                    let _ = store.save_pending(recipient, message);
                }
            }
            Address::Topic(topic) => {
                if let Some(subscribers) = self.subscriptions.get(topic) {
                    let msg = ServerMessage::MessageReceived {
                        message: message.clone(),
                    };
                    for sub_id in subscribers {
                        // Don't echo back to sender.
                        if sub_id == &message.from {
                            continue;
                        }
                        if let Some(sender) = self.endpoints.get(sub_id) {
                            let _ = sender.send(msg.clone());
                        }
                    }
                }
            }
            Address::Broadcast => {
                let msg = ServerMessage::MessageReceived {
                    message: message.clone(),
                };
                for (ep_id, sender) in &self.endpoints {
                    if ep_id == &message.from {
                        continue;
                    }
                    let _ = sender.send(msg.clone());
                }
            }
        }
    }

    /// Returns a list of all connected endpoint ids.
    pub fn list_endpoints(&self) -> Vec<EndpointId> {
        self.endpoints.keys().cloned().collect()
    }

    /// Returns a list of all topics with at least one subscriber.
    pub fn list_topics(&self) -> Vec<Topic> {
        self.subscriptions.keys().cloned().collect()
    }

    /// Delivers any pending messages from the store to a newly connected endpoint.
    pub fn deliver_pending(&self, id: &EndpointId, store: &MessageStore) {
        let pending = match store.get_pending(id) {
            Ok(p) => p,
            Err(e) => {
                warn!("failed to load pending messages for {id}: {e}");
                return;
            }
        };

        if pending.is_empty() {
            return;
        }

        let sender = match self.endpoints.get(id) {
            Some(s) => s,
            None => return,
        };

        debug!("delivering {} pending messages to {id}", pending.len());
        for (msg_id, message) in pending {
            let msg = ServerMessage::MessageReceived { message };
            if sender.send(msg).is_ok() {
                let _ = store.mark_delivered(msg_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::sync::mpsc;

    use herald_core::{Address, EndpointId, ErrorCode, Message, ServerMessage, Topic};

    use super::*;
    use crate::store::MessageStore;

    fn make_endpoint(
        name: &str,
    ) -> (
        EndpointId,
        EndpointSender,
        mpsc::UnboundedReceiver<ServerMessage>,
    ) {
        let id = EndpointId::new(name).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        (id, tx, rx)
    }

    fn make_message(from: &str, to: Address) -> Message {
        Message {
            from: EndpointId::new(from).unwrap(),
            to,
            payload: json!({"text": "hello"}),
            metadata: None,
            timestamp: 1700000000000,
        }
    }

    #[test]
    fn register_and_unregister() {
        let mut router = Router::new();
        let (id, tx, _rx) = make_endpoint("alice");

        assert!(router.register(id.clone(), tx).is_ok());
        assert_eq!(router.list_endpoints().len(), 1);

        router.unregister(&id);
        assert!(router.list_endpoints().is_empty());
    }

    #[test]
    fn register_duplicate_name_rejected() {
        let mut router = Router::new();
        let (id, tx1, _rx1) = make_endpoint("alice");
        let (_, tx2, _rx2) = make_endpoint("alice");

        assert!(router.register(id.clone(), tx1).is_ok());
        assert_eq!(router.register(id, tx2).unwrap_err(), ErrorCode::NameTaken);
    }

    #[test]
    fn direct_message_to_online_endpoint() {
        let mut router = Router::new();
        let store = MessageStore::in_memory().unwrap();
        let (bob_id, bob_tx, mut bob_rx) = make_endpoint("bob");
        router.register(bob_id.clone(), bob_tx).unwrap();

        let msg = make_message("alice", Address::Direct(bob_id));
        router.route_message(&msg, &store);

        let received = bob_rx.try_recv().unwrap();
        assert!(matches!(received, ServerMessage::MessageReceived { .. }));
    }

    #[test]
    fn direct_message_to_offline_endpoint_persisted() {
        let router = Router::new();
        let store = MessageStore::in_memory().unwrap();
        let recipient = EndpointId::new("bob").unwrap();

        let msg = make_message("alice", Address::Direct(recipient.clone()));
        router.route_message(&msg, &store);

        let pending = store.get_pending(&recipient).unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn topic_fanout_to_subscribers() {
        let mut router = Router::new();
        let store = MessageStore::in_memory().unwrap();
        let topic = Topic::new("events").unwrap();

        let (alice_id, alice_tx, _alice_rx) = make_endpoint("alice");
        let (bob_id, bob_tx, mut bob_rx) = make_endpoint("bob");
        let (charlie_id, charlie_tx, mut charlie_rx) = make_endpoint("charlie");

        router.register(alice_id.clone(), alice_tx).unwrap();
        router.register(bob_id.clone(), bob_tx).unwrap();
        router.register(charlie_id.clone(), charlie_tx).unwrap();

        router.subscribe(&bob_id, topic.clone()).unwrap();
        router.subscribe(&charlie_id, topic.clone()).unwrap();
        router.subscribe(&alice_id, topic.clone()).unwrap();

        let msg = make_message("alice", Address::Topic(topic));
        router.route_message(&msg, &store);

        // Bob and Charlie get the message, Alice (sender) does not.
        assert!(bob_rx.try_recv().is_ok());
        assert!(charlie_rx.try_recv().is_ok());
    }

    #[test]
    fn broadcast_reaches_all_except_sender() {
        let mut router = Router::new();
        let store = MessageStore::in_memory().unwrap();

        let (alice_id, alice_tx, mut alice_rx) = make_endpoint("alice");
        let (bob_id, bob_tx, mut bob_rx) = make_endpoint("bob");

        router.register(alice_id, alice_tx).unwrap();
        router.register(bob_id, bob_tx).unwrap();

        let msg = make_message("alice", Address::Broadcast);
        router.route_message(&msg, &store);

        assert!(bob_rx.try_recv().is_ok());
        assert!(alice_rx.try_recv().is_err());
    }

    #[test]
    fn subscribe_requires_registration() {
        let mut router = Router::new();
        let id = EndpointId::new("ghost").unwrap();
        let topic = Topic::new("events").unwrap();

        assert_eq!(
            router.subscribe(&id, topic).unwrap_err(),
            ErrorCode::NotRegistered,
        );
    }

    #[test]
    fn unregister_cleans_up_subscriptions() {
        let mut router = Router::new();
        let topic = Topic::new("events").unwrap();
        let (id, tx, _rx) = make_endpoint("alice");

        router.register(id.clone(), tx).unwrap();
        router.subscribe(&id, topic.clone()).unwrap();
        assert_eq!(router.list_topics().len(), 1);

        router.unregister(&id);
        assert!(router.list_topics().is_empty());
    }

    #[test]
    fn deliver_pending_on_reconnect() {
        let mut router = Router::new();
        let store = MessageStore::in_memory().unwrap();
        let bob = EndpointId::new("bob").unwrap();

        // Persist a message while Bob is offline.
        let msg = make_message("alice", Address::Direct(bob.clone()));
        router.route_message(&msg, &store);
        assert_eq!(store.get_pending(&bob).unwrap().len(), 1);

        // Bob reconnects.
        let (bob_id, bob_tx, mut bob_rx) = make_endpoint("bob");
        router.register(bob_id.clone(), bob_tx).unwrap();
        router.deliver_pending(&bob_id, &store);

        assert!(bob_rx.try_recv().is_ok());
        assert!(store.get_pending(&bob).unwrap().is_empty());
    }

    #[test]
    fn list_topics_only_non_empty() {
        let mut router = Router::new();
        let topic = Topic::new("events").unwrap();
        let (id, tx, _rx) = make_endpoint("alice");

        router.register(id.clone(), tx).unwrap();
        router.subscribe(&id, topic.clone()).unwrap();
        assert_eq!(router.list_topics().len(), 1);

        router.unsubscribe(&id, &topic);
        assert!(router.list_topics().is_empty());
    }
}

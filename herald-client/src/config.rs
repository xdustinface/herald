use std::path::PathBuf;
use std::time::Duration;

/// Configuration for connecting to the herald broker.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// WebSocket URL of the broker.
    pub(crate) broker_url: String,
    /// Path to the authentication token file.
    pub(crate) token_path: PathBuf,
    /// Reconnect policy settings.
    pub(crate) reconnect: ReconnectConfig,
}

/// Reconnection behavior settings.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial delay before the first reconnect attempt.
    pub(crate) initial_delay: Duration,
    /// Maximum delay between reconnect attempts (backoff cap).
    pub(crate) max_delay: Duration,
    /// Maximum number of consecutive reconnect attempts before giving up.
    /// `None` means unlimited retries.
    pub(crate) max_retries: Option<u32>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            max_retries: Some(10),
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        let token_path = dirs_next()
            .map(|d| d.join(".herald").join("token"))
            .unwrap_or_else(|| PathBuf::from(".herald/token"));

        Self {
            broker_url: "ws://127.0.0.1:9419".into(),
            token_path,
            reconnect: ReconnectConfig::default(),
        }
    }
}

/// Returns the user's home directory.
fn dirs_next() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// Builder for constructing a `ClientConfig`.
#[derive(Debug, Default)]
pub struct ClientConfigBuilder {
    broker_url: Option<String>,
    token_path: Option<PathBuf>,
    initial_delay: Option<Duration>,
    max_delay: Option<Duration>,
    max_retries: Option<Option<u32>>,
}

impl ClientConfigBuilder {
    /// Creates a new builder with all defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the broker WebSocket URL.
    pub fn broker_url(mut self, url: impl Into<String>) -> Self {
        self.broker_url = Some(url.into());
        self
    }

    /// Sets the path to the authentication token file.
    pub fn token_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.token_path = Some(path.into());
        self
    }

    /// Sets the initial reconnect delay.
    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = Some(delay);
        self
    }

    /// Sets the maximum reconnect delay.
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = Some(delay);
        self
    }

    /// Sets the maximum number of reconnect retries. `None` means unlimited.
    pub fn max_retries(mut self, retries: Option<u32>) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Builds the `ClientConfig`.
    pub fn build(self) -> ClientConfig {
        let defaults = ClientConfig::default();
        let reconnect_defaults = ReconnectConfig::default();

        ClientConfig {
            broker_url: self.broker_url.unwrap_or(defaults.broker_url),
            token_path: self.token_path.unwrap_or(defaults.token_path),
            reconnect: ReconnectConfig {
                initial_delay: self
                    .initial_delay
                    .unwrap_or(reconnect_defaults.initial_delay),
                max_delay: self.max_delay.unwrap_or(reconnect_defaults.max_delay),
                max_retries: self.max_retries.unwrap_or(reconnect_defaults.max_retries),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = ClientConfig::default();
        assert_eq!(config.broker_url, "ws://127.0.0.1:9419");
        assert!(config.token_path.ends_with(".herald/token"));
        assert_eq!(config.reconnect.initial_delay, Duration::from_secs(1));
        assert_eq!(config.reconnect.max_delay, Duration::from_secs(30));
        assert_eq!(config.reconnect.max_retries, Some(10));
    }

    #[test]
    fn builder_defaults_match_config_defaults() {
        let from_builder = ClientConfigBuilder::new().build();
        let from_default = ClientConfig::default();
        assert_eq!(from_builder.broker_url, from_default.broker_url);
        assert_eq!(from_builder.token_path, from_default.token_path);
        assert_eq!(
            from_builder.reconnect.initial_delay,
            from_default.reconnect.initial_delay,
        );
        assert_eq!(
            from_builder.reconnect.max_delay,
            from_default.reconnect.max_delay,
        );
        assert_eq!(
            from_builder.reconnect.max_retries,
            from_default.reconnect.max_retries,
        );
    }

    #[test]
    fn builder_custom_values() {
        let config = ClientConfigBuilder::new()
            .broker_url("ws://custom:1234")
            .token_path("/tmp/token")
            .initial_delay(Duration::from_millis(500))
            .max_delay(Duration::from_secs(60))
            .max_retries(None)
            .build();

        assert_eq!(config.broker_url, "ws://custom:1234");
        assert_eq!(config.token_path, PathBuf::from("/tmp/token"));
        assert_eq!(config.reconnect.initial_delay, Duration::from_millis(500));
        assert_eq!(config.reconnect.max_delay, Duration::from_secs(60));
        assert_eq!(config.reconnect.max_retries, None);
    }

    #[test]
    fn builder_partial_override() {
        let config = ClientConfigBuilder::new()
            .broker_url("ws://other:9999")
            .build();

        assert_eq!(config.broker_url, "ws://other:9999");
        // Other fields keep defaults.
        assert!(config.token_path.ends_with(".herald/token"));
        assert_eq!(config.reconnect.max_retries, Some(10));
    }
}

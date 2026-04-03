use std::time::Duration;

use crate::config::ReconnectConfig;

/// Tracks reconnection state and computes backoff delays.
#[derive(Debug)]
pub(crate) struct ReconnectPolicy {
    config: ReconnectConfig,
    attempt: u32,
}

impl ReconnectPolicy {
    /// Creates a new policy from the given configuration.
    pub(crate) fn new(config: ReconnectConfig) -> Self {
        Self { config, attempt: 0 }
    }

    /// Returns the delay for the next reconnect attempt, or `None` if retries
    /// are exhausted.
    pub(crate) fn next_delay(&mut self) -> Option<Duration> {
        if let Some(max) = self.config.max_retries
            && self.attempt >= max
        {
            return None;
        }

        let delay = self
            .config
            .initial_delay
            .saturating_mul(1 << self.attempt.min(16));
        let capped = delay.min(self.config.max_delay);

        self.attempt += 1;
        Some(capped)
    }

    /// Returns the current delay without advancing the attempt counter.
    pub(crate) fn peek_delay(&self) -> Duration {
        let delay = self
            .config
            .initial_delay
            .saturating_mul(1 << self.attempt.min(16));
        delay.min(self.config.max_delay)
    }

    /// Resets the attempt counter after a successful reconnection.
    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_sequence() {
        let config = ReconnectConfig {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            max_retries: Some(5),
        };
        let mut policy = ReconnectPolicy::new(config);

        assert_eq!(policy.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(2)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(4)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(8)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(16)));
        // Exhausted after 5 attempts.
        assert_eq!(policy.next_delay(), None);
    }

    #[test]
    fn backoff_capped_at_max_delay() {
        let config = ReconnectConfig {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            max_retries: Some(10),
        };
        let mut policy = ReconnectPolicy::new(config);

        assert_eq!(policy.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(2)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(4)));
        // Would be 8, capped to 5.
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(5)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn unlimited_retries() {
        let config = ReconnectConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            max_retries: None,
        };
        let mut policy = ReconnectPolicy::new(config);

        for _ in 0..50 {
            assert!(policy.next_delay().is_some());
        }
    }

    #[test]
    fn reset_restarts_backoff() {
        let config = ReconnectConfig {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            max_retries: Some(3),
        };
        let mut policy = ReconnectPolicy::new(config);

        assert_eq!(policy.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(2)));

        policy.reset();

        // Starts over from initial delay.
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(2)));
        assert_eq!(policy.next_delay(), Some(Duration::from_secs(4)));
        assert_eq!(policy.next_delay(), None);
    }
}

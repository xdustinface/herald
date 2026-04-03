use std::net::IpAddr;
use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

/// Herald message broker server.
#[derive(Debug, Parser)]
#[command(name = "herald-broker", version, about)]
pub struct Cli {
    /// Bind address.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: IpAddr,

    /// Bind port.
    #[arg(long, default_value_t = 9419)]
    pub port: u16,

    /// Data directory for token, database, and config.
    #[arg(long, default_value_os_t = default_data_dir())]
    pub data_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

/// Optional TOML config file fields.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct FileConfig {
    pub(crate) host: Option<IpAddr>,
    pub(crate) port: Option<u16>,
    pub(crate) log_level: Option<String>,
}

/// Resolved broker configuration.
#[derive(Debug)]
pub struct BrokerConfig {
    pub host: IpAddr,
    pub port: u16,
    pub data_dir: PathBuf,
    pub log_level: String,
}

impl BrokerConfig {
    /// Merge CLI args with an optional TOML config file.
    /// CLI args take precedence over file config.
    pub fn from_cli(cli: Cli) -> Self {
        let config_path = cli.data_dir.join("config.toml");
        let file_config = std::fs::read_to_string(&config_path)
            .ok()
            .and_then(|s| toml::from_str::<FileConfig>(&s).ok())
            .unwrap_or_default();

        // CLI defaults match the clap defaults, so file config only applies
        // when the user hasn't explicitly set a CLI flag. Since clap doesn't
        // distinguish "user set" vs "default", file config acts as a fallback
        // layer that we merge conservatively: file values fill in only when
        // the CLI value equals the built-in default.
        let host = if cli.host == default_host() {
            file_config.host.unwrap_or(cli.host)
        } else {
            cli.host
        };

        let port = if cli.port == 9419 {
            file_config.port.unwrap_or(cli.port)
        } else {
            cli.port
        };

        let log_level = if cli.log_level == "info" {
            file_config.log_level.unwrap_or(cli.log_level)
        } else {
            cli.log_level
        };

        Self {
            host,
            port,
            data_dir: cli.data_dir,
            log_level,
        }
    }
}

fn default_host() -> IpAddr {
    "127.0.0.1".parse().unwrap()
}

fn default_data_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".herald")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::IpAddr;

    use super::*;

    fn cli_with_defaults(data_dir: PathBuf) -> Cli {
        Cli {
            host: "127.0.0.1".parse().unwrap(),
            port: 9419,
            data_dir,
            log_level: "info".into(),
        }
    }

    #[test]
    fn default_config_has_expected_values() {
        let dir = std::env::temp_dir().join(format!("herald-test-cfg-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let config = BrokerConfig::from_cli(cli_with_defaults(dir.clone()));

        assert_eq!(config.host, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(config.port, 9419);
        assert_eq!(config.log_level, "info");
        assert_eq!(config.data_dir, dir);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn toml_partial_overrides_defaults() {
        let dir = std::env::temp_dir().join(format!("herald-test-toml-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        fs::write(dir.join("config.toml"), "port = 8080\n").unwrap();

        let config = BrokerConfig::from_cli(cli_with_defaults(dir.clone()));

        assert_eq!(config.port, 8080);
        // Unset fields keep defaults.
        assert_eq!(config.host, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(config.log_level, "info");

        let _ = fs::remove_dir_all(&dir);
    }
}

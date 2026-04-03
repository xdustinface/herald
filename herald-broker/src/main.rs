use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::info;

use herald_broker::auth;
use herald_broker::config::{BrokerConfig, Cli};
use herald_broker::router::Router;
use herald_broker::server::{self, BrokerState};
use herald_broker::store::MessageStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let config = BrokerConfig::from_cli(cli);

    // Initialize tracing.
    let filter = tracing_subscriber::EnvFilter::try_new(&config.log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Ensure data directory exists.
    std::fs::create_dir_all(&config.data_dir)?;

    // Load or generate auth token.
    let token = auth::load_or_create_token(&config.data_dir)?;
    info!("token file: {}", config.data_dir.join("token").display());

    // Initialize SQLite store.
    let db_path = config.data_dir.join("herald.db");
    let store = MessageStore::open(&db_path)?;
    info!("database: {}", db_path.display());

    // Build shared state.
    let state = Arc::new(Mutex::new(BrokerState {
        router: Router::new(),
        store,
        token,
    }));

    // Bind listener.
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await?;

    // Shutdown signal.
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("received shutdown signal");
        let _ = shutdown_tx.send(());
    });

    // Periodic cleanup of expired pending messages (every hour, older than 7 days).
    let cleanup_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            let store_cleanup_result = {
                let broker = cleanup_state.lock().unwrap();
                broker.store.cleanup_old(7 * 24 * 3600)
            };
            match store_cleanup_result {
                Ok(n) if n > 0 => info!("cleaned up {n} expired messages"),
                Err(e) => tracing::warn!("message cleanup failed: {e}"),
                _ => {}
            }
        }
    });

    server::run(listener, state, shutdown_rx).await;

    info!("broker stopped");
    Ok(())
}

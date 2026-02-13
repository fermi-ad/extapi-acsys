use tracing::{Level, info};
mod env_var;
mod g_rpc;
mod graphql;
mod pubsub;

#[tokio::main]
async fn main() {
    // Set up logging.
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Unable to set global default subscriber");

    let _ = rustls::crypto::ring::default_provider().install_default();

    info!("starting");

    // Start the web server.

    graphql::start_service().await;
}

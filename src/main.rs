use tracing::{info, subscriber};
use tracing_subscriber::{
    Registry, filter::EnvFilter, fmt::layer, layer::SubscriberExt,
};
mod g_rpc;
mod graphql;
mod pubsub;

#[tokio::main]
async fn main() {
    // Set up logging.
    let fmt_layer = layer()
        .with_target(false)
        .with_file(true)
        .with_line_number(true);
    // The following reads the log levels specified in the RUST_LOG environment variable. Allows us to configure logging
    // at both the application level and for specific crates/modules.
    let level_layer = EnvFilter::from_default_env();
    let subscriber = Registry::default().with(fmt_layer).with(level_layer);

    subscriber::set_global_default(subscriber)
        .expect("Unable to set global default subscriber");

    let _ = rustls::crypto::ring::default_provider().install_default();

    info!("starting");

    // Start the web server.

    graphql::start_service().await;
}

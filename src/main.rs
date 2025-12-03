use clap::Parser;
use std::net::IpAddr;
use tracing::{info, Level};

mod env_var;
mod g_rpc;
mod graphql;
mod pubsub;

#[cfg(not(debug_assertions))]
const DEFAULT_GQL_PORT: u16 = 8000;
#[cfg(debug_assertions)]
const DEFAULT_GQL_PORT: u16 = 8001;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port number to listen on (must be non-privileged: 1024-65535)
    #[arg(
        short,
        long,
        env = "GRAPHQL_PORT",
        default_value_t = DEFAULT_GQL_PORT,
        value_parser = clap::value_parser!(u16).range(1024..)
    )]
    port: u16,

    /// Address to bind to
    #[arg(short, long, env = "GRAPHQL_ADDRESS", default_value = "0.0.0.0")]
    address: IpAddr,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Set up logging.

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Unable to set global default subscriber");

    info!("starting");

    // Start the web server.

    graphql::start_service(args.address, args.port).await;
}

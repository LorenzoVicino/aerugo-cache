use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use aerugo_cache::server::{run, ServerConfig};
use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug, Parser)]
#[command(name = "aerugo-cache")]
#[command(about = "A small Redis-compatible in-memory cache written in Rust.")]
struct Args {
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    host: IpAddr,

    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(long, value_name = "PATH")]
    append_only: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();
    let addr = SocketAddr::new(args.host, args.port);

    run(ServerConfig {
        addr,
        append_only: args.append_only,
    })
    .await
}

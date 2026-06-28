use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use aerugo_cache::server::{run, ServerConfig};
use aerugo_cache::storage::{EvictionPolicy, StoreConfig};
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

    #[arg(long, value_name = "BYTES", value_parser = parse_byte_size)]
    max_memory: Option<usize>,

    #[arg(long, default_value_t = EvictionPolicy::NoEviction)]
    eviction_policy: EvictionPolicy,
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
        store_config: StoreConfig {
            max_memory_bytes: args.max_memory,
            eviction_policy: args.eviction_policy,
        },
    })
    .await
}

fn parse_byte_size(value: &str) -> Result<usize, String> {
    let value = value.trim();
    let split_at = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split_at);

    if number.is_empty() {
        return Err("memory size must start with a number".to_string());
    }

    let number = number
        .parse::<usize>()
        .map_err(|_| "memory size is too large".to_string())?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        other => {
            return Err(format!(
                "unsupported memory unit '{other}', expected b, kb, mb, or gb"
            ))
        }
    };

    number
        .checked_mul(multiplier)
        .ok_or_else(|| "memory size is too large".to_string())
}

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use aerugo_cache::server::{run, ServerConfig};
use aerugo_cache::storage::{EvictionPolicy, StoreConfig};
use aerugo_cache::tui::{run as run_tui, DashboardConfig};
use clap::{Args as ClapArgs, Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug, Parser)]
#[command(name = "aerugo-cache")]
#[command(about = "A small Redis-compatible in-memory cache written in Rust.")]
struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,

    #[arg(long, global = true, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    host: IpAddr,

    #[arg(short, long, global = true, default_value_t = 6379)]
    port: u16,

    #[arg(long, value_name = "PATH")]
    append_only: Option<PathBuf>,

    #[arg(long, value_name = "BYTES", value_parser = parse_byte_size)]
    max_memory: Option<usize>,

    #[arg(long, default_value_t = EvictionPolicy::NoEviction)]
    eviction_policy: EvictionPolicy,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    #[command(about = "Open a terminal dashboard for a running Aerugo Cache server.")]
    Tui(TuiArgs),
}

#[derive(Debug, ClapArgs)]
struct TuiArgs {
    #[arg(long, default_value_t = 1000, value_name = "MS")]
    refresh_ms: u64,

    #[arg(long, default_value_t = 128, value_name = "COUNT")]
    limit: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let addr = SocketAddr::new(cli.host, cli.port);

    match cli.command {
        Some(CliCommand::Tui(args)) => {
            run_tui(DashboardConfig {
                addr,
                refresh_interval: Duration::from_millis(args.refresh_ms.max(250)),
                key_limit: args.limit,
            })
            .await?;
        }
        None => {
            init_logging();

            run(ServerConfig {
                addr,
                append_only: cli.append_only,
                store_config: StoreConfig {
                    max_memory_bytes: cli.max_memory,
                    eviction_policy: cli.eviction_policy,
                },
            })
            .await?;
        }
    }

    Ok(())
}

fn init_logging() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();
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

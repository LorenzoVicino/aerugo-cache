use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    time,
};
use tracing::{debug, error, info};

use crate::{
    command::Command,
    protocol::{read_frame, write_frame, Frame},
    storage::MemoryStore,
};

#[derive(Debug, Clone, Copy)]
pub struct ServerConfig {
    pub addr: SocketAddr,
}

pub async fn run(config: ServerConfig) -> std::io::Result<()> {
    let listener = TcpListener::bind(config.addr).await?;
    let store = Arc::new(MemoryStore::new());

    spawn_expiration_cleanup(Arc::clone(&store));

    info!(addr = %config.addr, "ferrocache listening");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let store = Arc::clone(&store);

        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, store).await {
                debug!(%peer_addr, %error, "connection closed with error");
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, store: Arc<MemoryStore>) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    while let Some(frame) = read_frame(&mut reader).await? {
        let response = match Command::from_frame(frame) {
            Ok(command) => command.execute(Arc::clone(&store)).await,
            Err(error) => {
                error!(%error, "command failed");
                Frame::Error(format!("ERR {error}"))
            }
        };

        write_frame(&mut write_half, &response).await?;
    }

    Ok(())
}

fn spawn_expiration_cleanup(store: Arc<MemoryStore>) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;
            let removed = store.cleanup_expired().await;

            if removed > 0 {
                debug!(removed, "cleaned expired keys");
            }
        }
    });
}

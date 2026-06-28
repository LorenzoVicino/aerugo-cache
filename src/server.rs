use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    time,
};
use tracing::{debug, error, info};

use crate::{
    command::Command,
    persistence::Aof,
    protocol::{read_frame, write_frame, Frame},
    storage::MemoryStore,
};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub addr: SocketAddr,
    pub append_only: Option<PathBuf>,
}

pub async fn run(config: ServerConfig) -> std::io::Result<()> {
    let store = Arc::new(MemoryStore::new());
    let aof = match &config.append_only {
        Some(path) => {
            let replayed = Aof::replay(path, Arc::clone(&store)).await?;
            let aof = Arc::new(Aof::open(path).await?);

            info!(path = %path.display(), replayed, "append-only file loaded");

            Some(aof)
        }
        None => None,
    };
    let listener = TcpListener::bind(config.addr).await?;

    spawn_expiration_cleanup(Arc::clone(&store));

    info!(addr = %config.addr, "aerugo cache listening");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let store = Arc::clone(&store);
        let aof = aof.clone();

        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, store, aof).await {
                debug!(%peer_addr, %error, "connection closed with error");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    store: Arc<MemoryStore>,
    aof: Option<Arc<Aof>>,
) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    while let Some(frame) = read_frame(&mut reader).await? {
        let response = match Command::from_frame(frame) {
            Ok(command) => {
                let aof_frame = command.to_aof_frame();

                match command.execute(Arc::clone(&store)).await {
                    Ok(response) => match append_command(&aof, aof_frame.as_ref()).await {
                        Ok(()) => response,
                        Err(error) => {
                            error!(%error, "failed to append command to AOF");
                            Frame::Error("ERR failed to persist command".to_string())
                        }
                    },
                    Err(error) => {
                        error!(%error, "command failed");
                        Command::error_frame(&error)
                    }
                }
            }
            Err(error) => {
                error!(%error, "command failed");
                Command::error_frame(&error)
            }
        };

        write_frame(&mut write_half, &response).await?;
    }

    Ok(())
}

async fn append_command(aof: &Option<Arc<Aof>>, frame: Option<&Frame>) -> std::io::Result<()> {
    let Some(aof) = aof else {
        return Ok(());
    };

    let Some(frame) = frame else {
        return Ok(());
    };

    aof.append(frame).await
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

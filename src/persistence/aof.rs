use std::{path::Path, sync::Arc};

use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncWriteExt, BufReader, BufWriter},
    sync::Mutex,
};
use tracing::warn;

use crate::{
    command::Command,
    protocol::{read_frame, write_frame, Frame},
    storage::MemoryStore,
};

#[derive(Debug)]
pub struct Aof {
    writer: Mutex<BufWriter<File>>,
}

impl Aof {
    pub async fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).await?;
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    pub async fn append(&self, frame: &Frame) -> std::io::Result<()> {
        let mut writer = self.writer.lock().await;

        write_frame(&mut *writer, frame).await?;
        writer.flush().await
    }

    pub async fn replay(path: impl AsRef<Path>, store: Arc<MemoryStore>) -> std::io::Result<usize> {
        let path = path.as_ref();

        if fs::metadata(path)
            .await
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return Ok(0);
        }

        let file = File::open(path).await?;
        let mut reader = BufReader::new(file);
        let mut count = 0;

        while let Some(frame) = read_frame(&mut reader).await? {
            match Command::from_frame(frame) {
                Ok(command) => {
                    match command.execute(Arc::clone(&store)).await {
                        Ok(_) => count += 1,
                        Err(error) => {
                            warn!(%error, "skipping failed command during AOF replay");
                        }
                    }
                }
                Err(error) => {
                    warn!(%error, "skipping invalid command during AOF replay");
                }
            }
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[tokio::test]
    async fn appends_and_replays_commands() {
        let path = std::env::temp_dir().join(format!(
            "aerugo-cache-aof-{}-{}.aof",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = Arc::new(MemoryStore::new());
        let aof = Aof::open(&path).await.unwrap();

        aof.append(&Frame::Array(vec![
            Frame::Bulk(b"SET".to_vec()),
            Frame::Bulk(b"project".to_vec()),
            Frame::Bulk(b"aerugo-cache".to_vec()),
        ]))
        .await
        .unwrap();
        aof.append(&Frame::Array(vec![
            Frame::Bulk(b"RPUSH".to_vec()),
            Frame::Bulk(b"events".to_vec()),
            Frame::Bulk(b"one".to_vec()),
            Frame::Bulk(b"two".to_vec()),
        ]))
        .await
        .unwrap();

        let replayed = Aof::replay(&path, Arc::clone(&store)).await.unwrap();

        assert_eq!(replayed, 2);
        assert_eq!(
            store.get("project").await.unwrap(),
            Some(b"aerugo-cache".to_vec())
        );
        assert_eq!(
            store.lrange("events", 0, -1).await.unwrap(),
            vec![b"one".to_vec(), b"two".to_vec()]
        );

        fs::remove_file(path).await.unwrap();
    }
}

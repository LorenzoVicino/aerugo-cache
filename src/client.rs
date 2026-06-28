use std::net::SocketAddr;

use thiserror::Error;
use tokio::{
    io::{BufReader, ReadHalf, WriteHalf},
    net::TcpStream,
};

use crate::protocol::{read_frame, write_frame, Frame};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("server closed the connection")]
    ConnectionClosed,
    #[error("server error: {0}")]
    Server(String),
}

pub struct RespClient {
    reader: BufReader<ReadHalf<TcpStream>>,
    writer: WriteHalf<TcpStream>,
}

impl RespClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = tokio::io::split(stream);

        Ok(Self {
            reader: BufReader::new(reader),
            writer,
        })
    }

    pub async fn command(&mut self, args: &[String]) -> Result<Frame, ClientError> {
        let request = Frame::Array(
            args.iter()
                .map(|arg| Frame::Bulk(arg.as_bytes().to_vec()))
                .collect(),
        );

        write_frame(&mut self.writer, &request).await?;

        let Some(response) = read_frame(&mut self.reader).await? else {
            return Err(ClientError::ConnectionClosed);
        };

        match response {
            Frame::Error(error) => Err(ClientError::Server(error)),
            frame => Ok(frame),
        }
    }
}

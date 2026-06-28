use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use thiserror::Error;

use crate::{protocol::Frame, storage::MemoryStore};

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("empty command")]
    Empty,
    #[error("expected RESP array command")]
    ExpectedArray,
    #[error("expected bulk string argument")]
    ExpectedBulk,
    #[error("invalid UTF-8 in command or key")]
    InvalidUtf8,
    #[error("value is not an integer or is out of range")]
    InvalidInteger,
    #[error("wrong number of arguments for '{0}'")]
    WrongArity(&'static str),
    #[error("unknown command '{0}'")]
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping(Option<Vec<u8>>),
    Echo(Vec<u8>),
    Set { key: String, value: Vec<u8> },
    Get { key: String },
    Del { keys: Vec<String> },
    Exists { keys: Vec<String> },
    Expire { key: String, seconds: u64 },
    ExpireAt { key: String, unix_seconds: u64 },
    Ttl { key: String },
    Persist { key: String },
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Self, CommandError> {
        let items = match frame {
            Frame::Array(items) => items,
            _ => return Err(CommandError::ExpectedArray),
        };

        let mut args = into_bulk_args(items)?;
        if args.is_empty() {
            return Err(CommandError::Empty);
        }

        let command_name = bytes_to_string(args.remove(0))?.to_ascii_uppercase();

        match command_name.as_str() {
            "PING" => match args.len() {
                0 => Ok(Self::Ping(None)),
                1 => Ok(Self::Ping(Some(args.remove(0)))),
                _ => Err(CommandError::WrongArity("PING")),
            },
            "ECHO" => match args.len() {
                1 => Ok(Self::Echo(args.remove(0))),
                _ => Err(CommandError::WrongArity("ECHO")),
            },
            "SET" => match args.len() {
                2 => Ok(Self::Set {
                    key: bytes_to_string(args.remove(0))?,
                    value: args.remove(0),
                }),
                _ => Err(CommandError::WrongArity("SET")),
            },
            "GET" => match args.len() {
                1 => Ok(Self::Get {
                    key: bytes_to_string(args.remove(0))?,
                }),
                _ => Err(CommandError::WrongArity("GET")),
            },
            "DEL" => {
                if args.is_empty() {
                    return Err(CommandError::WrongArity("DEL"));
                }
                Ok(Self::Del {
                    keys: into_keys(args)?,
                })
            }
            "EXISTS" => {
                if args.is_empty() {
                    return Err(CommandError::WrongArity("EXISTS"));
                }
                Ok(Self::Exists {
                    keys: into_keys(args)?,
                })
            }
            "EXPIRE" => match args.len() {
                2 => Ok(Self::Expire {
                    key: bytes_to_string(args.remove(0))?,
                    seconds: parse_u64(args.remove(0))?,
                }),
                _ => Err(CommandError::WrongArity("EXPIRE")),
            },
            "EXPIREAT" => match args.len() {
                2 => Ok(Self::ExpireAt {
                    key: bytes_to_string(args.remove(0))?,
                    unix_seconds: parse_u64(args.remove(0))?,
                }),
                _ => Err(CommandError::WrongArity("EXPIREAT")),
            },
            "TTL" => match args.len() {
                1 => Ok(Self::Ttl {
                    key: bytes_to_string(args.remove(0))?,
                }),
                _ => Err(CommandError::WrongArity("TTL")),
            },
            "PERSIST" => match args.len() {
                1 => Ok(Self::Persist {
                    key: bytes_to_string(args.remove(0))?,
                }),
                _ => Err(CommandError::WrongArity("PERSIST")),
            },
            other => Err(CommandError::Unknown(other.to_string())),
        }
    }

    pub async fn execute(self, store: Arc<MemoryStore>) -> Frame {
        match self {
            Self::Ping(None) => Frame::Simple("PONG".to_string()),
            Self::Ping(Some(value)) => Frame::Bulk(value),
            Self::Echo(value) => Frame::Bulk(value),
            Self::Set { key, value } => {
                store.set(key, value).await;
                Frame::Simple("OK".to_string())
            }
            Self::Get { key } => match store.get(&key).await {
                Some(value) => Frame::Bulk(value),
                None => Frame::Null,
            },
            Self::Del { keys } => Frame::Integer(store.del(&keys).await as i64),
            Self::Exists { keys } => Frame::Integer(store.exists(&keys).await as i64),
            Self::Expire { key, seconds } => Frame::Integer(if store.expire(&key, seconds).await {
                1
            } else {
                0
            }),
            Self::ExpireAt { key, unix_seconds } => {
                Frame::Integer(if store.expire_at_unix(&key, unix_seconds).await {
                    1
                } else {
                    0
                })
            }
            Self::Ttl { key } => Frame::Integer(store.ttl(&key).await.as_redis_integer()),
            Self::Persist { key } => Frame::Integer(if store.persist(&key).await { 1 } else { 0 }),
        }
    }

    pub fn to_aof_frame(&self) -> Option<Frame> {
        match self {
            Self::Set { key, value } => Some(Frame::Array(vec![
                bulk("SET"),
                Frame::Bulk(key.as_bytes().to_vec()),
                Frame::Bulk(value.clone()),
            ])),
            Self::Del { keys } => Some(Frame::Array(
                std::iter::once(bulk("DEL"))
                    .chain(keys.iter().map(|key| Frame::Bulk(key.as_bytes().to_vec())))
                    .collect(),
            )),
            Self::Expire { key, seconds } => Some(Frame::Array(vec![
                bulk("EXPIREAT"),
                Frame::Bulk(key.as_bytes().to_vec()),
                Frame::Bulk(expiration_deadline(*seconds).to_string().into_bytes()),
            ])),
            Self::ExpireAt { key, unix_seconds } => Some(Frame::Array(vec![
                bulk("EXPIREAT"),
                Frame::Bulk(key.as_bytes().to_vec()),
                Frame::Bulk(unix_seconds.to_string().into_bytes()),
            ])),
            Self::Persist { key } => Some(Frame::Array(vec![
                bulk("PERSIST"),
                Frame::Bulk(key.as_bytes().to_vec()),
            ])),
            Self::Ping(_)
            | Self::Echo(_)
            | Self::Get { .. }
            | Self::Exists { .. }
            | Self::Ttl { .. } => None,
        }
    }
}

fn into_bulk_args(items: Vec<Frame>) -> Result<Vec<Vec<u8>>, CommandError> {
    items
        .into_iter()
        .map(|item| match item {
            Frame::Bulk(value) => Ok(value),
            _ => Err(CommandError::ExpectedBulk),
        })
        .collect()
}

fn into_keys(args: Vec<Vec<u8>>) -> Result<Vec<String>, CommandError> {
    args.into_iter().map(bytes_to_string).collect()
}

fn bytes_to_string(bytes: Vec<u8>) -> Result<String, CommandError> {
    String::from_utf8(bytes).map_err(|_| CommandError::InvalidUtf8)
}

fn parse_u64(bytes: Vec<u8>) -> Result<u64, CommandError> {
    bytes_to_string(bytes)?
        .parse()
        .map_err(|_| CommandError::InvalidInteger)
}

fn bulk(value: &str) -> Frame {
    Frame::Bulk(value.as_bytes().to_vec())
}

fn expiration_deadline(seconds: u64) -> u64 {
    let deadline = SystemTime::now()
        .checked_add(Duration::from_secs(seconds))
        .unwrap_or(SystemTime::now());

    unix_seconds_ceil(deadline)
}

fn unix_seconds_ceil(time: SystemTime) -> u64 {
    let Ok(duration) = time.duration_since(UNIX_EPOCH) else {
        return 0;
    };

    duration.as_secs() + u64::from(duration.subsec_nanos() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_set_command() {
        let frame = Frame::Array(vec![
            Frame::Bulk(b"SET".to_vec()),
            Frame::Bulk(b"project".to_vec()),
            Frame::Bulk(b"aerugo-cache".to_vec()),
        ]);

        let command = Command::from_frame(frame).unwrap();

        assert_eq!(
            command,
            Command::Set {
                key: "project".to_string(),
                value: b"aerugo-cache".to_vec()
            }
        );
    }

    #[test]
    fn rejects_unknown_command() {
        let frame = Frame::Array(vec![Frame::Bulk(b"NOPE".to_vec())]);

        let error = Command::from_frame(frame).unwrap_err();

        assert!(matches!(error, CommandError::Unknown(command) if command == "NOPE"));
    }

    #[test]
    fn parses_expire_command() {
        let frame = Frame::Array(vec![
            Frame::Bulk(b"EXPIRE".to_vec()),
            Frame::Bulk(b"project".to_vec()),
            Frame::Bulk(b"30".to_vec()),
        ]);

        let command = Command::from_frame(frame).unwrap();

        assert_eq!(
            command,
            Command::Expire {
                key: "project".to_string(),
                seconds: 30
            }
        );
    }

    #[test]
    fn parses_expireat_command() {
        let frame = Frame::Array(vec![
            Frame::Bulk(b"EXPIREAT".to_vec()),
            Frame::Bulk(b"project".to_vec()),
            Frame::Bulk(b"1780000000".to_vec()),
        ]);

        let command = Command::from_frame(frame).unwrap();

        assert_eq!(
            command,
            Command::ExpireAt {
                key: "project".to_string(),
                unix_seconds: 1_780_000_000
            }
        );
    }

    #[test]
    fn serializes_set_to_aof_frame() {
        let command = Command::Set {
            key: "project".to_string(),
            value: b"aerugo-cache".to_vec(),
        };

        assert_eq!(
            command.to_aof_frame(),
            Some(Frame::Array(vec![
                Frame::Bulk(b"SET".to_vec()),
                Frame::Bulk(b"project".to_vec()),
                Frame::Bulk(b"aerugo-cache".to_vec()),
            ]))
        );
    }

    #[tokio::test]
    async fn ttl_command_reports_missing_key() {
        let store = Arc::new(MemoryStore::new());
        let response = Command::Ttl {
            key: "missing".to_string(),
        }
        .execute(store)
        .await;

        assert_eq!(response, Frame::Integer(-2));
    }
}

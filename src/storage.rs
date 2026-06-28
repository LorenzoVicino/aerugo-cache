use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct MemoryStore {
    entries: RwLock<HashMap<String, Entry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    WrongType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ttl {
    Missing,
    NoExpiration,
    Seconds(u64),
}

impl Ttl {
    pub fn as_redis_integer(&self) -> i64 {
        match self {
            Self::Missing => -2,
            Self::NoExpiration => -1,
            Self::Seconds(seconds) => (*seconds).min(i64::MAX as u64) as i64,
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expires_at: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set(&self, key: String, value: Vec<u8>) {
        self.entries.write().await.insert(
            key,
            Entry {
                value: Value::String(value),
                expires_at: None,
            },
        );
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        let Some(entry) = entries.get(key) else {
            return Ok(None);
        };

        match &entry.value {
            Value::String(value) => Ok(Some(value.clone())),
            Value::List(_) => Err(StoreError::WrongType),
        }
    }

    pub async fn del(&self, keys: &[String]) -> usize {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;

        keys.iter()
            .filter(|key| {
                let key = key.as_str();
                remove_if_expired(&mut entries, key, now);
                entries.remove(key).is_some()
            })
            .count()
    }

    pub async fn exists(&self, keys: &[String]) -> usize {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;

        keys.iter()
            .filter(|key| {
                let key = key.as_str();
                remove_if_expired(&mut entries, key, now);
                entries.contains_key(key)
            })
            .count()
    }

    pub async fn expire(&self, key: &str, seconds: u64) -> bool {
        let now = SystemTime::now();
        let expires_at = now.checked_add(Duration::from_secs(seconds)).unwrap_or(now);

        self.expire_at(key, expires_at).await
    }

    pub async fn expire_at_unix(&self, key: &str, unix_seconds: u64) -> bool {
        let expires_at = UNIX_EPOCH
            .checked_add(Duration::from_secs(unix_seconds))
            .unwrap_or(SystemTime::now());

        self.expire_at(key, expires_at).await
    }

    async fn expire_at(&self, key: &str, expires_at: SystemTime) -> bool {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        if expires_at <= now {
            return entries.remove(key).is_some();
        }

        let Some(entry) = entries.get_mut(key) else {
            return false;
        };

        entry.expires_at = Some(expires_at);
        true
    }

    pub async fn ttl(&self, key: &str) -> Ttl {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        let Some(entry) = entries.get(key) else {
            return Ttl::Missing;
        };

        match entry.expires_at {
            Some(expires_at) => Ttl::Seconds(
                expires_at
                    .duration_since(now)
                    .map_or(0, |duration| duration.as_secs()),
            ),
            None => Ttl::NoExpiration,
        }
    }

    pub async fn persist(&self, key: &str) -> bool {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        let Some(entry) = entries.get_mut(key) else {
            return false;
        };

        if entry.expires_at.is_none() {
            return false;
        }

        entry.expires_at = None;
        true
    }

    pub async fn cleanup_expired(&self) -> usize {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        let before = entries.len();

        entries.retain(|_, entry| !entry.is_expired(now));

        before - entries.len()
    }

    pub async fn lpush(&self, key: String, values: Vec<Vec<u8>>) -> Result<usize, StoreError> {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, &key, now);

        let entry = entries.entry(key).or_insert_with(|| Entry {
            value: Value::List(VecDeque::new()),
            expires_at: None,
        });

        let Value::List(list) = &mut entry.value else {
            return Err(StoreError::WrongType);
        };

        for value in values {
            list.push_front(value);
        }

        Ok(list.len())
    }

    pub async fn rpush(&self, key: String, values: Vec<Vec<u8>>) -> Result<usize, StoreError> {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, &key, now);

        let entry = entries.entry(key).or_insert_with(|| Entry {
            value: Value::List(VecDeque::new()),
            expires_at: None,
        });

        let Value::List(list) = &mut entry.value else {
            return Err(StoreError::WrongType);
        };

        for value in values {
            list.push_back(value);
        }

        Ok(list.len())
    }

    pub async fn lpop(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.pop(key, PopSide::Left).await
    }

    pub async fn rpop(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.pop(key, PopSide::Right).await
    }

    pub async fn lrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        let Some(entry) = entries.get(key) else {
            return Ok(Vec::new());
        };

        let Value::List(list) = &entry.value else {
            return Err(StoreError::WrongType);
        };

        let Some((start, stop)) = normalize_range(list.len(), start, stop) else {
            return Ok(Vec::new());
        };

        Ok(list
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .cloned()
            .collect())
    }

    async fn pop(&self, key: &str, side: PopSide) -> Result<Option<Vec<u8>>, StoreError> {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        let Some(entry) = entries.get_mut(key) else {
            return Ok(None);
        };

        let Value::List(list) = &mut entry.value else {
            return Err(StoreError::WrongType);
        };

        let value = match side {
            PopSide::Left => list.pop_front(),
            PopSide::Right => list.pop_back(),
        };
        let is_empty = list.is_empty();

        if is_empty {
            entries.remove(key);
        }

        Ok(value)
    }
}

#[derive(Debug, Clone, Copy)]
enum PopSide {
    Left,
    Right,
}

fn remove_if_expired(entries: &mut HashMap<String, Entry>, key: &str, now: SystemTime) {
    if entries.get(key).is_some_and(|entry| entry.is_expired(now)) {
        entries.remove(key);
    }
}

impl Entry {
    fn is_expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

fn normalize_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }

    let len = len as i64;
    let mut start = if start < 0 { len + start } else { start };
    let mut stop = if stop < 0 { len + stop } else { stop };

    if start < 0 {
        start = 0;
    }

    if stop < 0 || start >= len {
        return None;
    }

    if stop >= len {
        stop = len - 1;
    }

    (start <= stop).then_some((start as usize, stop as usize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_and_exists_work() {
        let store = MemoryStore::new();

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await;

        assert_eq!(
            store.get("project").await.unwrap(),
            Some(b"aerugo-cache".to_vec())
        );
        assert_eq!(store.exists(&["project".to_string()]).await, 1);
    }

    #[tokio::test]
    async fn expire_zero_deletes_key() {
        let store = MemoryStore::new();

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await;

        assert!(store.expire("project", 0).await);
        assert_eq!(store.get("project").await.unwrap(), None);
        assert_eq!(store.ttl("project").await, Ttl::Missing);
    }

    #[tokio::test]
    async fn expire_at_past_deletes_key() {
        let store = MemoryStore::new();

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await;

        assert!(store.expire_at_unix("project", 0).await);
        assert_eq!(store.get("project").await.unwrap(), None);
    }

    #[tokio::test]
    async fn ttl_reports_missing_and_persistent_keys() {
        let store = MemoryStore::new();

        assert_eq!(store.ttl("missing").await, Ttl::Missing);

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await;

        assert_eq!(store.ttl("project").await, Ttl::NoExpiration);
    }

    #[tokio::test]
    async fn persist_removes_expiration() {
        let store = MemoryStore::new();

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await;
        assert!(store.expire("project", 60).await);
        assert!(store.persist("project").await);
        assert_eq!(store.ttl("project").await, Ttl::NoExpiration);
    }

    #[tokio::test]
    async fn list_push_pop_and_range_work() {
        let store = MemoryStore::new();

        assert_eq!(
            store
                .rpush(
                    "events".to_string(),
                    vec![b"one".to_vec(), b"two".to_vec()]
                )
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            store.lpush("events".to_string(), vec![b"zero".to_vec()])
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            store.lrange("events", 0, -1).await.unwrap(),
            vec![b"zero".to_vec(), b"one".to_vec(), b"two".to_vec()]
        );
        assert_eq!(store.lpop("events").await.unwrap(), Some(b"zero".to_vec()));
        assert_eq!(store.rpop("events").await.unwrap(), Some(b"two".to_vec()));
        assert_eq!(
            store.lrange("events", 0, -1).await.unwrap(),
            vec![b"one".to_vec()]
        );
    }

    #[tokio::test]
    async fn list_commands_reject_string_values() {
        let store = MemoryStore::new();

        store.set("project".to_string(), b"aerugo-cache".to_vec()).await;

        assert_eq!(
            store.lpush("project".to_string(), vec![b"event".to_vec()])
                .await
                .unwrap_err(),
            StoreError::WrongType
        );
    }

    #[test]
    fn normalizes_redis_ranges() {
        assert_eq!(normalize_range(3, 0, -1), Some((0, 2)));
        assert_eq!(normalize_range(3, -2, -1), Some((1, 2)));
        assert_eq!(normalize_range(3, 5, 9), None);
        assert_eq!(normalize_range(3, 2, 1), None);
    }
}

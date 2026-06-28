use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct MemoryStore {
    entries: RwLock<HashMap<String, Entry>>,
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
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set(&self, key: String, value: Vec<u8>) {
        self.entries.write().await.insert(
            key,
            Entry {
                value,
                expires_at: None,
            },
        );
    }

    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let now = Instant::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        entries.get(key).map(|entry| entry.value.clone())
    }

    pub async fn del(&self, keys: &[String]) -> usize {
        let now = Instant::now();
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
        let now = Instant::now();
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
        let now = Instant::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        if seconds == 0 {
            return entries.remove(key).is_some();
        }

        let Some(entry) = entries.get_mut(key) else {
            return false;
        };

        let expires_at = now.checked_add(Duration::from_secs(seconds)).unwrap_or(now);
        entry.expires_at = Some(expires_at);
        true
    }

    pub async fn ttl(&self, key: &str) -> Ttl {
        let now = Instant::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, key, now);

        let Some(entry) = entries.get(key) else {
            return Ttl::Missing;
        };

        match entry.expires_at {
            Some(expires_at) => Ttl::Seconds(expires_at.saturating_duration_since(now).as_secs()),
            None => Ttl::NoExpiration,
        }
    }

    pub async fn persist(&self, key: &str) -> bool {
        let now = Instant::now();
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
        let now = Instant::now();
        let mut entries = self.entries.write().await;
        let before = entries.len();

        entries.retain(|_, entry| !entry.is_expired(now));

        before - entries.len()
    }
}

fn remove_if_expired(entries: &mut HashMap<String, Entry>, key: &str, now: Instant) {
    if entries.get(key).is_some_and(|entry| entry.is_expired(now)) {
        entries.remove(key);
    }
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at
            .is_some_and(|expires_at| expires_at <= now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_and_exists_work() {
        let store = MemoryStore::new();

        store.set("project".to_string(), b"ferrocache".to_vec()).await;

        assert_eq!(store.get("project").await, Some(b"ferrocache".to_vec()));
        assert_eq!(store.exists(&["project".to_string()]).await, 1);
    }

    #[tokio::test]
    async fn expire_zero_deletes_key() {
        let store = MemoryStore::new();

        store.set("project".to_string(), b"ferrocache".to_vec()).await;

        assert!(store.expire("project", 0).await);
        assert_eq!(store.get("project").await, None);
        assert_eq!(store.ttl("project").await, Ttl::Missing);
    }

    #[tokio::test]
    async fn ttl_reports_missing_and_persistent_keys() {
        let store = MemoryStore::new();

        assert_eq!(store.ttl("missing").await, Ttl::Missing);

        store.set("project".to_string(), b"ferrocache".to_vec()).await;

        assert_eq!(store.ttl("project").await, Ttl::NoExpiration);
    }

    #[tokio::test]
    async fn persist_removes_expiration() {
        let store = MemoryStore::new();

        store.set("project".to_string(), b"ferrocache".to_vec()).await;
        assert!(store.expire("project", 60).await);
        assert!(store.persist("project").await);
        assert_eq!(store.ttl("project").await, Ttl::NoExpiration);
    }
}

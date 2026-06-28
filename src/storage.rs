use std::{
    collections::{HashMap, VecDeque},
    fmt,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::RwLock;

const ENTRY_OVERHEAD_BYTES: usize = 64;
const LIST_ITEM_OVERHEAD_BYTES: usize = 24;

#[derive(Debug, Default)]
pub struct MemoryStore {
    entries: RwLock<HashMap<String, Entry>>,
    config: StoreConfig,
    counters: RwLock<StoreCounters>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreConfig {
    pub max_memory_bytes: Option<usize>,
    pub eviction_policy: EvictionPolicy,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: None,
            eviction_policy: EvictionPolicy::NoEviction,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysRandom,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::NoEviction
    }
}

impl EvictionPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysRandom => "allkeys-random",
        }
    }
}

impl fmt::Display for EvictionPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for EvictionPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "noeviction" => Ok(Self::NoEviction),
            "allkeys-random" => Ok(Self::AllKeysRandom),
            other => Err(format!(
                "unsupported eviction policy '{other}', expected noeviction or allkeys-random"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    WrongType,
    OutOfMemory,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreStats {
    pub total_keys: usize,
    pub string_keys: usize,
    pub list_keys: usize,
    pub expiring_keys: usize,
    pub key_bytes: usize,
    pub payload_bytes: usize,
    pub estimated_memory_bytes: usize,
    pub list_items: usize,
    pub max_memory_bytes: Option<usize>,
    pub eviction_policy: EvictionPolicy,
    pub expired_keys_cleaned: u64,
    pub evicted_keys: u64,
    pub rejected_writes: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct StoreCounters {
    expired_keys_cleaned: u64,
    evicted_keys: u64,
    rejected_writes: u64,
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
        Self::with_config(StoreConfig::default())
    }

    pub fn with_config(config: StoreConfig) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            config,
            counters: RwLock::new(StoreCounters::default()),
        }
    }

    pub async fn set(&self, key: String, value: Vec<u8>) -> Result<(), StoreError> {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, &key, now);

        let old = entries.insert(
            key.clone(),
            Entry {
                value: Value::String(value),
                expires_at: None,
            },
        );

        match enforce_memory_limit(&mut entries, self.config, Some(&key)) {
            Ok(evicted) => {
                drop(entries);
                self.record_evicted(evicted.len() as u64).await;
                Ok(())
            }
            Err(error) => {
                restore_entry(&mut entries, key, old);
                drop(entries);
                self.record_rejected_write().await;
                Err(error)
            }
        }
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

        let removed = before - entries.len();
        drop(entries);
        self.record_expired(removed as u64).await;

        removed
    }

    pub async fn lpush(&self, key: String, values: Vec<Vec<u8>>) -> Result<usize, StoreError> {
        self.push(key, values, PushSide::Left).await
    }

    pub async fn rpush(&self, key: String, values: Vec<Vec<u8>>) -> Result<usize, StoreError> {
        self.push(key, values, PushSide::Right).await
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

    pub async fn stats(&self) -> StoreStats {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        let before = entries.len();

        entries.retain(|_, entry| !entry.is_expired(now));
        let expired = before - entries.len();
        let mut stats = build_stats(&entries, self.config);

        drop(entries);
        self.record_expired(expired as u64).await;

        let counters = self.counters.read().await;
        stats.expired_keys_cleaned = counters.expired_keys_cleaned;
        stats.evicted_keys = counters.evicted_keys;
        stats.rejected_writes = counters.rejected_writes;
        stats
    }

    async fn push(
        &self,
        key: String,
        values: Vec<Vec<u8>>,
        side: PushSide,
    ) -> Result<usize, StoreError> {
        let now = SystemTime::now();
        let mut entries = self.entries.write().await;
        remove_if_expired(&mut entries, &key, now);

        let old = entries.get(&key).cloned();
        let len = {
            let entry = entries.entry(key.clone()).or_insert_with(|| Entry {
                value: Value::List(VecDeque::new()),
                expires_at: None,
            });

            let Value::List(list) = &mut entry.value else {
                return Err(StoreError::WrongType);
            };

            for value in values {
                match side {
                    PushSide::Left => list.push_front(value),
                    PushSide::Right => list.push_back(value),
                }
            }

            list.len()
        };

        match enforce_memory_limit(&mut entries, self.config, Some(&key)) {
            Ok(evicted) => {
                drop(entries);
                self.record_evicted(evicted.len() as u64).await;
                Ok(len)
            }
            Err(error) => {
                restore_entry(&mut entries, key, old);
                drop(entries);
                self.record_rejected_write().await;
                Err(error)
            }
        }
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

    async fn record_expired(&self, count: u64) {
        if count == 0 {
            return;
        }

        self.counters.write().await.expired_keys_cleaned += count;
    }

    async fn record_evicted(&self, count: u64) {
        if count == 0 {
            return;
        }

        self.counters.write().await.evicted_keys += count;
    }

    async fn record_rejected_write(&self) {
        self.counters.write().await.rejected_writes += 1;
    }
}

#[derive(Debug, Clone, Copy)]
enum PushSide {
    Left,
    Right,
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

fn restore_entry(entries: &mut HashMap<String, Entry>, key: String, old: Option<Entry>) {
    match old {
        Some(entry) => {
            entries.insert(key, entry);
        }
        None => {
            entries.remove(&key);
        }
    }
}

fn enforce_memory_limit(
    entries: &mut HashMap<String, Entry>,
    config: StoreConfig,
    protected_key: Option<&str>,
) -> Result<Vec<(String, Entry)>, StoreError> {
    let Some(max_memory_bytes) = config.max_memory_bytes else {
        return Ok(Vec::new());
    };

    if estimated_memory_bytes(entries) <= max_memory_bytes {
        return Ok(Vec::new());
    }

    match config.eviction_policy {
        EvictionPolicy::NoEviction => Err(StoreError::OutOfMemory),
        EvictionPolicy::AllKeysRandom => evict_until_within_limit(
            entries,
            max_memory_bytes,
            protected_key,
        ),
    }
}

fn evict_until_within_limit(
    entries: &mut HashMap<String, Entry>,
    max_memory_bytes: usize,
    protected_key: Option<&str>,
) -> Result<Vec<(String, Entry)>, StoreError> {
    let mut evicted = Vec::new();

    while estimated_memory_bytes(entries) > max_memory_bytes {
        let Some(key) = pick_eviction_key(entries, protected_key) else {
            restore_evicted(entries, evicted);
            return Err(StoreError::OutOfMemory);
        };

        if let Some(entry) = entries.remove(&key) {
            evicted.push((key, entry));
        }
    }

    Ok(evicted)
}

fn pick_eviction_key(
    entries: &HashMap<String, Entry>,
    protected_key: Option<&str>,
) -> Option<String> {
    let candidates = entries
        .keys()
        .filter(|key| protected_key != Some(key.as_str()))
        .count();

    if candidates == 0 {
        return None;
    }

    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos() as usize);
    let index = seed % candidates;

    entries
        .keys()
        .filter(|key| protected_key != Some(key.as_str()))
        .nth(index)
        .cloned()
}

fn restore_evicted(entries: &mut HashMap<String, Entry>, evicted: Vec<(String, Entry)>) {
    for (key, entry) in evicted {
        entries.insert(key, entry);
    }
}

impl Entry {
    fn is_expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

impl Value {
    fn payload_bytes(&self) -> usize {
        match self {
            Self::String(value) => value.len(),
            Self::List(list) => list.iter().map(Vec::len).sum(),
        }
    }

    fn estimated_memory_bytes(&self) -> usize {
        match self {
            Self::String(value) => value.len(),
            Self::List(list) => {
                list.iter()
                    .map(|value| value.len() + LIST_ITEM_OVERHEAD_BYTES)
                    .sum()
            }
        }
    }

    fn list_items(&self) -> usize {
        match self {
            Self::String(_) => 0,
            Self::List(list) => list.len(),
        }
    }
}

fn build_stats(entries: &HashMap<String, Entry>, config: StoreConfig) -> StoreStats {
    let mut stats = StoreStats {
        total_keys: entries.len(),
        string_keys: 0,
        list_keys: 0,
        expiring_keys: 0,
        key_bytes: 0,
        payload_bytes: 0,
        estimated_memory_bytes: 0,
        list_items: 0,
        max_memory_bytes: config.max_memory_bytes,
        eviction_policy: config.eviction_policy,
        expired_keys_cleaned: 0,
        evicted_keys: 0,
        rejected_writes: 0,
    };

    for (key, entry) in entries {
        stats.key_bytes += key.len();
        stats.payload_bytes += entry.value.payload_bytes();
        stats.estimated_memory_bytes += entry_estimated_memory_bytes(key, entry);
        stats.list_items += entry.value.list_items();

        if entry.expires_at.is_some() {
            stats.expiring_keys += 1;
        }

        match &entry.value {
            Value::String(_) => stats.string_keys += 1,
            Value::List(_) => stats.list_keys += 1,
        }
    }

    stats
}

fn estimated_memory_bytes(entries: &HashMap<String, Entry>) -> usize {
    entries
        .iter()
        .map(|(key, entry)| entry_estimated_memory_bytes(key, entry))
        .sum()
}

fn entry_estimated_memory_bytes(key: &str, entry: &Entry) -> usize {
    ENTRY_OVERHEAD_BYTES + key.len() + entry.value.estimated_memory_bytes()
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
            .await
            .unwrap();

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
            .await
            .unwrap();

        assert!(store.expire("project", 0).await);
        assert_eq!(store.get("project").await.unwrap(), None);
        assert_eq!(store.ttl("project").await, Ttl::Missing);
    }

    #[tokio::test]
    async fn expire_at_past_deletes_key() {
        let store = MemoryStore::new();

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await
            .unwrap();

        assert!(store.expire_at_unix("project", 0).await);
        assert_eq!(store.get("project").await.unwrap(), None);
    }

    #[tokio::test]
    async fn ttl_reports_missing_and_persistent_keys() {
        let store = MemoryStore::new();

        assert_eq!(store.ttl("missing").await, Ttl::Missing);

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await
            .unwrap();

        assert_eq!(store.ttl("project").await, Ttl::NoExpiration);
    }

    #[tokio::test]
    async fn persist_removes_expiration() {
        let store = MemoryStore::new();

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await
            .unwrap();
        assert!(store.expire("project", 60).await);
        assert!(store.persist("project").await);
        assert_eq!(store.ttl("project").await, Ttl::NoExpiration);
    }

    #[tokio::test]
    async fn list_push_pop_and_range_work() {
        let store = MemoryStore::new();

        assert_eq!(
            store
                .rpush("events".to_string(), vec![b"one".to_vec(), b"two".to_vec()])
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .lpush("events".to_string(), vec![b"zero".to_vec()])
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

        store
            .set("project".to_string(), b"aerugo-cache".to_vec())
            .await
            .unwrap();

        assert_eq!(
            store
                .lpush("project".to_string(), vec![b"event".to_vec()])
                .await
                .unwrap_err(),
            StoreError::WrongType
        );
    }

    #[tokio::test]
    async fn stats_report_keyspace_shape() {
        let store = MemoryStore::new();

        store.set("project".to_string(), b"aerugo-cache".to_vec()).await.unwrap();
        store
            .rpush("events".to_string(), vec![b"one".to_vec(), b"two".to_vec()])
            .await
            .unwrap();
        assert!(store.expire("project", 60).await);

        let stats = store.stats().await;

        assert_eq!(stats.total_keys, 2);
        assert_eq!(stats.string_keys, 1);
        assert_eq!(stats.list_keys, 1);
        assert_eq!(stats.expiring_keys, 1);
        assert_eq!(stats.list_items, 2);
        assert!(stats.payload_bytes >= "aerugo-cache".len() + "one".len() + "two".len());
    }

    #[tokio::test]
    async fn noeviction_rejects_writes_over_limit() {
        let store = MemoryStore::with_config(StoreConfig {
            max_memory_bytes: Some(80),
            eviction_policy: EvictionPolicy::NoEviction,
        });

        let error = store
            .set("large".to_string(), vec![0; 512])
            .await
            .unwrap_err();

        assert_eq!(error, StoreError::OutOfMemory);
        assert_eq!(store.get("large").await.unwrap(), None);
        assert_eq!(store.stats().await.rejected_writes, 1);
    }

    #[tokio::test]
    async fn allkeys_random_evicts_to_make_room() {
        let store = MemoryStore::with_config(StoreConfig {
            max_memory_bytes: Some(180),
            eviction_policy: EvictionPolicy::AllKeysRandom,
        });

        store.set("a".to_string(), vec![1; 20]).await.unwrap();
        store.set("b".to_string(), vec![2; 20]).await.unwrap();
        store.set("c".to_string(), vec![3; 20]).await.unwrap();

        let stats = store.stats().await;

        assert!(stats.total_keys < 3);
        assert!(stats.estimated_memory_bytes <= 180);
        assert!(stats.evicted_keys > 0);
    }

    #[test]
    fn parses_eviction_policy() {
        assert_eq!(
            "allkeys-random".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::AllKeysRandom
        );
        assert!("volatile-lru".parse::<EvictionPolicy>().is_err());
    }

    #[test]
    fn normalizes_redis_ranges() {
        assert_eq!(normalize_range(3, 0, -1), Some((0, 2)));
        assert_eq!(normalize_range(3, -2, -1), Some((1, 2)));
        assert_eq!(normalize_range(3, 5, 9), None);
        assert_eq!(normalize_range(3, 2, 1), None);
    }
}

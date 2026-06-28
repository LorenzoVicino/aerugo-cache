# Aerugo Cache

Aerugo Cache is a small Redis-compatible in-memory cache written in Rust.

The goal is not to replace Redis. The goal is to build a real networked system
that is small enough to understand, useful enough to run, and deep enough to
learn Rust properly.

## Status

Aerugo Cache is at the early MVP stage and already supports basic expiration.

Supported today:

| Command | Example | Notes |
| --- | --- | --- |
| `PING` | `PING` | Returns `PONG` |
| `ECHO` | `ECHO hello` | Returns the provided value |
| `SET` | `SET name aerugo-cache` | Stores a binary-safe value |
| `GET` | `GET name` | Returns a value or null |
| `DEL` | `DEL name other` | Returns deleted key count |
| `EXISTS` | `EXISTS name other` | Returns existing key count |
| `EXPIRE` | `EXPIRE name 30` | Sets a TTL in seconds |
| `EXPIREAT` | `EXPIREAT name 1780000000` | Sets an absolute Unix timestamp expiration |
| `TTL` | `TTL name` | Returns remaining TTL, `-1`, or `-2` |
| `PERSIST` | `PERSIST name` | Removes a key expiration |
| `LPUSH` | `LPUSH events one two` | Pushes one or more values to the head of a list |
| `RPUSH` | `RPUSH events three` | Pushes one or more values to the tail of a list |
| `LPOP` | `LPOP events` | Pops a value from the head of a list |
| `RPOP` | `RPOP events` | Pops a value from the tail of a list |
| `LRANGE` | `LRANGE events 0 -1` | Returns an inclusive list range |
| `DBSIZE` | `DBSIZE` | Returns the current number of keys |
| `INFO` | `INFO` | Returns Aerugo Cache stats as a bulk string |
| `AERUGO.STATS` | `AERUGO.STATS` | Alias for the stats payload used by tooling |
| `AERUGO.INSPECT` | `AERUGO.INSPECT 128` | Returns key metadata for dashboards and diagnostics |

Planned next:

- dashboard polish
- pub/sub as a stretch goal

## Quick Start

Run the server:

```bash
cargo run -- --host 127.0.0.1 --port 6379
```

Run with append-only persistence:

```bash
cargo run -- --append-only data/aerugo-cache.aof
```

Run with a memory limit:

```bash
cargo run -- --max-memory 64mb --eviction-policy allkeys-random
```

Open the terminal dashboard for a running server:

```bash
cargo run -- tui --host 127.0.0.1 --port 6379
```

Limit displayed key rows and adjust refresh speed:

```bash
cargo run -- tui --limit 50 --refresh-ms 500
```

Use it with `redis-cli`:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET language rust
redis-cli -p 6379 GET language
redis-cli -p 6379 EXPIRE language 60
redis-cli -p 6379 TTL language
redis-cli -p 6379 RPUSH events one two three
redis-cli -p 6379 LRANGE events 0 -1
redis-cli -p 6379 INFO
redis-cli -p 6379 AERUGO.INSPECT 10
redis-cli -p 6379 DEL language
```

Or open an interactive Redis CLI session:

```bash
redis-cli -p 6379
127.0.0.1:6379> SET project aerugo-cache
OK
127.0.0.1:6379> GET project
"aerugo-cache"
127.0.0.1:6379> EXPIRE project 30
(integer) 1
127.0.0.1:6379> TTL project
(integer) 29
```

## Why Build This?

Aerugo Cache is designed as a learning project for Rust developers who want to go
beyond syntax and build something systems-oriented:

- TCP networking with Tokio
- protocol parsing and encoding
- binary-safe values
- shared state with `Arc` and `RwLock`
- expiration metadata with `SystemTime`
- append-only persistence and replay
- typed values with strings and lists
- memory accounting, memory limits, and simple eviction
- terminal dashboards with `ratatui`
- command dispatch through enums and pattern matching
- explicit error handling with `Result`
- testable module boundaries

## Architecture

```text
src/
  main.rs             CLI, logging, process lifecycle
  lib.rs              public crate modules
  client.rs           small RESP client used by local tooling
  server.rs           TCP listener and connection loop
  command.rs          Redis command parsing and execution
  storage.rs          in-memory key-value engine
  tui.rs              terminal dashboard
  protocol/
    mod.rs
    frame.rs          RESP frame model
    parser.rs         RESP decoder
    encoder.rs        RESP encoder
```

The first protocol target is RESP2 because it is enough for `redis-cli`
compatibility and keeps the implementation approachable.

## Usage Model

Aerugo Cache can be used as:

- a local Redis-like cache for experiments
- a teaching project for async Rust and protocol design
- a small codebase for practicing open source contributions
- a foundation for comparing storage, persistence, and concurrency strategies

It should not be used as production infrastructure.

## Development

Run checks:

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test
```

Run benchmarks:

```bash
cargo bench
```

Run with debug logs:

```bash
RUST_LOG=aerugo_cache=debug cargo run
```

## Roadmap

### 0.1

- RESP2 parser and encoder
- TCP server
- in-memory string storage
- `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`

### 0.2

- expiration metadata
- `EXPIRE`, `EXPIREAT`, `TTL`, `PERSIST`
- lazy expiration on access
- background cleanup task

### 0.3

- append-only file persistence
- replay on startup
- RESP command serialization for durable mutations

### 0.4

- list values
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`
- `WRONGTYPE` errors for invalid type operations

### 0.5

- benchmark suite
- memory limits
- simple eviction policy
- `DBSIZE`, `INFO`, `AERUGO.STATS`

### 0.6

- terminal dashboard with `ratatui`
- `AERUGO.INSPECT` keyspace diagnostics
- RESP client module for local tooling
- memory and key inventory views

## Contributing

Small, focused pull requests are welcome. Good first areas:

- more RESP parser tests
- command compatibility improvements
- better error messages
- documentation examples
- benchmarks against Redis for supported commands

## License

MIT

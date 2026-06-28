# Ferrocache

Ferrocache is a small Redis-compatible in-memory cache written in Rust.

The goal is not to replace Redis. The goal is to build a real networked system
that is small enough to understand, useful enough to run, and deep enough to
learn Rust properly.

## Status

Ferrocache is at the early MVP stage and already supports basic expiration.

Supported today:

| Command | Example | Notes |
| --- | --- | --- |
| `PING` | `PING` | Returns `PONG` |
| `ECHO` | `ECHO hello` | Returns the provided value |
| `SET` | `SET name ferrocache` | Stores a binary-safe value |
| `GET` | `GET name` | Returns a value or null |
| `DEL` | `DEL name other` | Returns deleted key count |
| `EXISTS` | `EXISTS name other` | Returns existing key count |
| `EXPIRE` | `EXPIRE name 30` | Sets a TTL in seconds |
| `TTL` | `TTL name` | Returns remaining TTL, `-1`, or `-2` |
| `PERSIST` | `PERSIST name` | Removes a key expiration |

Planned next:

- append-only persistence
- lists with `LPUSH`, `RPUSH`, `LPOP`, and `LRANGE`
- basic benchmarks
- pub/sub as a stretch goal

## Quick Start

Run the server:

```bash
cargo run -- --host 127.0.0.1 --port 6379
```

Use it with `redis-cli`:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET language rust
redis-cli -p 6379 GET language
redis-cli -p 6379 EXPIRE language 60
redis-cli -p 6379 TTL language
redis-cli -p 6379 DEL language
```

Or open an interactive Redis CLI session:

```bash
redis-cli -p 6379
127.0.0.1:6379> SET project ferrocache
OK
127.0.0.1:6379> GET project
"ferrocache"
127.0.0.1:6379> EXPIRE project 30
(integer) 1
127.0.0.1:6379> TTL project
(integer) 29
```

## Why Build This?

Ferrocache is designed as a learning project for Rust developers who want to go
beyond syntax and build something systems-oriented:

- TCP networking with Tokio
- protocol parsing and encoding
- binary-safe values
- shared state with `Arc` and `RwLock`
- expiration metadata with `Instant`
- command dispatch through enums and pattern matching
- explicit error handling with `Result`
- testable module boundaries

## Architecture

```text
src/
  main.rs             CLI, logging, process lifecycle
  lib.rs              public crate modules
  server.rs           TCP listener and connection loop
  command.rs          Redis command parsing and execution
  storage.rs          in-memory key-value engine
  protocol/
    mod.rs
    frame.rs          RESP frame model
    parser.rs         RESP decoder
    encoder.rs        RESP encoder
```

The first protocol target is RESP2 because it is enough for `redis-cli`
compatibility and keeps the implementation approachable.

## Usage Model

Ferrocache can be used as:

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

Run with debug logs:

```bash
RUST_LOG=ferrocache=debug cargo run
```

## Roadmap

### 0.1

- RESP2 parser and encoder
- TCP server
- in-memory string storage
- `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`

### 0.2

- expiration metadata
- `EXPIRE`, `TTL`, `PERSIST`
- lazy expiration on access
- background cleanup task

### 0.3

- append-only file persistence
- replay on startup
- safe fsync configuration

### 0.4

- list values
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`

### 0.5

- benchmark suite
- memory limits
- simple eviction policy

## Contributing

Small, focused pull requests are welcome. Good first areas:

- more RESP parser tests
- command compatibility improvements
- better error messages
- documentation examples
- benchmarks against Redis for supported commands

## License

MIT

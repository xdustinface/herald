# CLAUDE.md

## Project Overview

Herald is a bidirectional message broker for Claude Code sessions. It enables real-time push-based communication between multiple Claude Code instances via MCP channel notifications and WebSocket transport.

## Repository Structure

- `herald-core/` — Core types: message envelope, endpoint identity, topic addressing, protocol, errors
- `herald-broker/` — Standalone broker binary: WebSocket server, routing, auth, SQLite persistence
- `herald-client/` — Client library: WebSocket connection, auto-reconnect, send/receive API
- `herald-mcp/` — MCP shim binary: stdio MCP server with channel notifications for Claude Code

## Build Commands

```bash
cargo build                    # Build all crates
cargo build -p herald-broker   # Build specific crate
```

## Test Commands

```bash
cargo test                     # Run all tests
cargo test -p herald-core --lib  # Run specific crate tests (--lib skips doc-tests)
cargo test test_name --lib     # Run specific test
```

## Lint & Format

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

## Code Style

- All `use` statements at the top of the enclosing module — never inside functions
- No inline fully-qualified paths to avoid importing
- Most restrictive visibility: prefer `pub(crate)` over `pub`
- Error handling with `thiserror` for library errors, `anyhow` for binaries
- Async code uses `tokio` runtime
- No AI attribution in commits
- Keep commit messages short with backtick highlighting for identifiers

## Architecture

Messages flow: Claude Code <-> stdio <-> herald-mcp <-> WebSocket <-> herald-broker <-> WebSocket <-> herald-mcp <-> stdio <-> Claude Code

The broker persists messages to SQLite for offline delivery. Endpoints register with self-chosen names and can subscribe to topics for pub/sub fan-out.

## Key Dependencies

- `tokio` + `tokio-tungstenite` — async runtime and WebSocket
- `rusqlite` — SQLite persistence
- `serde` + `serde_json` — serialization
- `clap` — CLI argument parsing
- `tracing` — structured logging

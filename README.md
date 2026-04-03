# Herald

[![CI](https://github.com/xdustinface/herald/actions/workflows/ci.yml/badge.svg)](https://github.com/xdustinface/herald/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/xdustinface/herald/graph/badge.svg)](https://codecov.io/gh/xdustinface/herald)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Bidirectional message broker enabling real-time communication between Claude Code sessions via MCP channel notifications and WebSocket. Named endpoints, topic-based routing, and SQLite persistence. Extensible to external sources like GitHub webhooks.

## Architecture

```
Claude Code A                                          Claude Code B
  | stdio                                               | stdio
herald-mcp shim --WebSocket--> herald-broker <--WebSocket-- herald-mcp shim
  (MCP tools:                   (SQLite,                   (MCP channel
   send, subscribe)              routing,                    notifications)
                                 topics)
                                   ^
                          Future: webhook adapters,
                          GitHub, CI, etc.
```

## Workspace

| Crate | Description |
|-------|-------------|
| `herald-core` | Core message types, protocol, and error types |
| `herald-broker` | Standalone WebSocket broker with SQLite persistence |
| `herald-client` | Client library for connecting to the broker |
| `herald-mcp` | MCP shim binary for Claude Code integration |

## Building

```bash
cargo build
```

## Testing

```bash
cargo test
```

## License

MIT

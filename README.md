# Hyperliquid Orderbook WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk.

This project has been further developed and maintained by [Imperator](https://hyperpc.app) to make the Hyperliquid public release production-ready and more efficient. Imperator provides no warranty, guarantee, or support obligation of any kind. The software is provided "as is" and you assume all risks associated with its use, including but not limited to data loss, system failure, or any other damages. Under no circumstances shall Imperator be held liable for any claim, damages, or other liability arising from the use of this software.

## Features

Real-time orderbook data from a local Hyperliquid node:

- **bbo** - Best Bid/Offer (top of book) with deduplication
- **allbbo** - Batched Best Bid/Offer feed for all enabled markets
- **l2Book** - Aggregated Level 2 orderbook with deduplication
- **trades** - Real-time trade feed
- **bookDiffs** - Raw book diff stream per coin
- **l4Book** - Full Level 4 orderbook with individual order details
- **orderUpdates** - User-specific order status stream

## Quick Start

### Prerequisites

1. **Hyperliquid Node** - Running with streaming enabled (Docker or systemctl)
2. **Rust** - For building from source

### System Requirements

The server runs co-located with a Hyperliquid node and is memory-light relative to the node itself. Figures below are measured in production with ~630 coins (all markets: perps + spot + HIP-3) and ~440 k live orders.

| Profile | CPU | RAM (steady-state) | Notes |
|---------|-----|---------------------|-------|
| **Full (`--features all --markets all`)** | 2 cores minimum, 4+ recommended | ~750 MB – 2 GB | Rises with connected clients and L2 subscription fanout. Reserve **4 GB** for headroom in production. |
| **Perps only (`--markets perps`)** | 2 cores | ~400 MB – 1 GB | Smaller public universe and fewer L2 variants to compute. |
| **BBO only (`--features bbo` or `--features allbbo`)** | 1 core | ~100 – 150 MB | Top-of-book only, no L2/L4/trades. |
| **Raw streams only (`--features trades,bookdiffs,orderupdates`)** | 1 core | lowest | Skips startup snapshot and in-memory orderbook state. |

Other requirements:

- **OS:** Linux with inotify (kernel ≥ 2.6.13). Tested on Ubuntu 22.04 / 24.04.
- **Architecture:** x86_64 or aarch64.
- **File descriptors:** raise `LimitNOFILE` in your service unit (the included one sets `1048576`). Each WebSocket client uses one fd.
- **Disk:** negligible for the server itself (~10 MB binary). The Hyperliquid node's `*_streaming/` directories grow with traffic — size them per the node's documentation, not this server's.
- **Network:** outbound bandwidth scales with `(connected clients) × (subscription mix)`. L2 snapshots with many `(coin, n_sig_figs, mantissa)` variants are the largest payloads — use `--compression-level 1` and/or `--markets perps` to bound it.

**Sizing rule of thumb.** Memory grows roughly with (a) the universe of coins the node serves, and (b) the number of distinct L2 subscription tuples your clients open. The 256-per-connection subscription cap and the broadcast channel capacity bound the worst case. If you see `channel_drops_total` rising or `broadcast_channel_lag` non-zero, you have a slow consumer — not a server-side leak.

### Build

```bash
git clone https://github.com/imperator-co/order_book_server.git
cd order_book_server
cargo build --release
```

### Run

**Docker mode** (node running via `docker compose`):
```bash
./target/release/orderbook_server \
    --address 0.0.0.0 \
    --port 8000 \
    --data-dir /root/.hyperliquid_rpc_hlnode_mainnet/volumes/hl/data
```

**Direct mode** (node running via systemctl / bare metal):
```bash
./target/release/orderbook_server \
    --snapshot-mode direct \
    --hlnode-binary /path/to/hl-node \
    --data-dir /path/to/volumes/hl/data
```

## Configuration

### Core Options

| Flag | Default | Description |
|------|---------|-------------|
| `--address` | `0.0.0.0` | Bind address |
| `--port` | `8000` | WebSocket port |
| `--compression-level` | `1` | WebSocket compression level (0-9). See [Compression](#compression) |
| `--secret` | unset | Require WebSocket clients to connect with `?token=<secret>` |
| `--markets` | `all` | Comma-delimited `perps`, `spot`, `hip3`, or `all` |
| `--features` | `all` | Comma-delimited `bbo`, `allbbo`, `l2book`, `l4book`, `trades`, `bookdiffs`, `orderupdates`, `stats`, or `all` |
| `--log-level` | `info` | `error`, `warn`, `info`, `debug`, `trace` |

### Compression

WebSocket messages (especially L2/L4 snapshots) can be large. The `--compression-level` flag controls `permessage-deflate` compression applied to every outgoing frame:

| Level | Behavior | Use case |
|-------|----------|----------|
| `0` | Disabled | Lowest latency. Use for HFT or when your client doesn't support `permessage-deflate` |
| `1` | Fast compression | Best tradeoff for most setups. Minimal CPU cost, significant bandwidth savings |
| `5` | Balanced | Good compression ratio with moderate CPU |
| `9` | Best ratio | Maximum compression. Higher CPU cost, useful for bandwidth-constrained links |

Your WebSocket client must support `permessage-deflate` for levels 1-9 to have any effect. If it doesn't, use `0`.

### Snapshot Mode

When `bbo`, `allbbo`, `l2book`, or `l4book` is enabled, the server needs a **full L4 orderbook snapshot** to initialize its in-memory state. It obtains this by calling the `hl-node` binary's CLI, which reads the node's `abci_state.rmp` file (the node's persistent state) and dumps a JSON snapshot of every order currently on the book. Raw-only configurations such as `--features trades` or `--features stats` skip this snapshot and start serving as soon as their file watchers are running.

The `--snapshot-mode` flag controls *how* the server invokes `hl-node`:

**`docker` (default)** - Use when your Hyperliquid node runs inside a Docker container (the standard `docker compose` setup). The server runs `docker exec <container> hl-node --dump-abci-state ...` to execute the snapshot command inside the container, where `hl-node` and the state files are accessible.

**`direct`** - Use when your node runs directly on the host via systemctl or bare metal. The server calls the `hl-node` binary directly on the host to generate the snapshot.

After the initial snapshot, the server stays up to date by watching the node's `*_streaming/` directories for real-time order diffs, fills, and status updates via inotify. No further snapshots are needed unless the server restarts.

### Feature Selection

The `--features` flag controls both the WebSocket channels the server accepts and the upstream work it performs.

| Feature | WebSocket type | Requires snapshot/orderbook state | Watched node stream |
|---------|----------------|------------------------------------|---------------------|
| `bbo` | `bbo` | Yes | order diffs + order statuses |
| `allbbo` | `allbbo` | Yes | order diffs + order statuses |
| `l2book` | `l2Book` | Yes | order diffs + order statuses |
| `l4book` | `l4Book` | Yes | order diffs + order statuses |
| `trades` | `trades` | No | fills |
| `bookdiffs` | `bookDiffs` | No | order diffs |
| `orderupdates` | `orderUpdates` | No | order statuses |
| `stats` | `stats` | No | fills + order diffs |

Examples:

```bash
--features bbo
--features allbbo
--features l2book
--features bbo,allbbo,l2book,trades
--features trades,orderupdates
--features stats
```

If a client subscribes to a disabled channel, the server returns an error and does not register that subscription. Raw-only modes skip snapshot-derived coin validation; a syntactically valid coin in the selected `--markets` category is accepted and receives data only if the node emits it.

| Flag | Default | Description |
|------|---------|-------------|
| `--snapshot-mode` | `docker` | `docker` or `direct` |
| `--docker-container` | `hyperliquid_hlnode` | Container name for `docker exec` (docker mode only) |
| `--hlnode-binary` | `hl-node` | Path to hl-node binary on host (direct mode only) |
| `--data-dir` | `~` | Path to the folder containing `node_fills_streaming/`, `node_order_statuses_streaming/`, and `node_raw_book_diffs_streaming/`. This is where the node writes its real-time event files |
| `--abci-state-path` | auto | Path to `abci_state.rmp`. Auto-detected at `<data-dir>/hl/hyperliquid_data/abci_state.rmp` in direct mode. Override if your node stores state in a non-standard location |
| `--snapshot-output-path` | auto | Path where `hl-node` writes its JSON snapshot output. Defaults to `/tmp/hl_snapshot.json`. Override if `/tmp` is not writable or you want snapshots stored elsewhere |
| `--visor-state-path` | auto | Path to `visor_abci_state.json`, which contains the current block height. Auto-detected relative to `--data-dir`. Override if your visor state is in a non-standard location |

### Market Types

The `--markets` flag accepts comma-delimited market names. The `all` value remains supported as a compatibility alias for `perps,spot,hip3`.

| Value | Description |
|-------|-------------|
| `perps` | Perpetual futures only (BTC, ETH, SOL...) |
| `spot` | Spot markets (`@*` coins, PURR/USDC) |
| `hip3` | HIP-3 markets (X:Y format, e.g., xyz:TSLA) |
| `perps,hip3` | Perpetual futures and HIP-3 markets, excluding spot |
| `perps,spot,hip3` | All markets explicitly |
| `all` | All markets compatibility alias (default) |

### Advanced Options

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics-port` | `9090` | Prometheus metrics port (0 to disable) |
| `--l2book-heartbeat-ms` | `0` | If > 0, resend the last `l2Book` payload for each active subscription every N ms when nothing has changed. See [Heartbeats](#heartbeats) |
| `--bbo-heartbeat-ms` | `0` | If > 0, resend the last `bbo` payload for each active subscription every N ms when nothing has changed. See [Heartbeats](#heartbeats) |
| `--allbbo-heartbeat-ms` | `0` | If > 0, resend the last `allbbo` payload for each active subscription every N ms when nothing has changed. See [Heartbeats](#heartbeats) |

### Heartbeats

By default, `l2Book`, `bbo`, and `allbbo` channels are **change-only**: a snapshot is only sent when the underlying book state actually moves. On quiet markets (low-liquidity coins like `Frudo`) this can produce zero messages for minutes at a time, which breaks downstream clients written against the official Hyperliquid API — that API pushes a snapshot every block as an implicit heartbeat, so consumers often treat silence as a dead stream and reconnect.

The `--l2book-heartbeat-ms`, `--bbo-heartbeat-ms`, and `--allbbo-heartbeat-ms` flags add an opt-in periodic resend:

- When set to `0` (default), the original change-only behavior is preserved. No new messages compared to previous versions.
- When set to e.g. `1000`, every active subscription receives a cached payload at most every 1000 ms even if nothing changed. The `time` field is refreshed to the current server time on every heartbeat so clients can distinguish "fresh-but-unchanged" from "stale".

Each subscription tracks its own `last_sent` timestamp, so a real change resets the heartbeat — you never get a real update and a heartbeat back-to-back. Per-subscription accounting also means many quiet coins do not produce a synchronized burst.

Pick a value that matches your downstream stall timer, typically `1000` ms for clients ported from the official API.

```bash
# Behave like the official HL l2Book stream
./target/release/orderbook_server \
    --l2book-heartbeat-ms 1000 \
    --bbo-heartbeat-ms 1000 \
    --allbbo-heartbeat-ms 1000 \
    --data-dir /path/to/data
```

## Recommended Configurations

### Low Latency Trading

Optimized for sub-millisecond BBO updates. Compression is disabled to eliminate encoding overhead. Market scope is narrowed to perps only, reducing the number of coins tracked and memory usage.

```bash
./target/release/orderbook_server \
    --features bbo \
    --compression-level 0 \
    --markets perps \
    --data-dir /path/to/data
```

### General Purpose

Balanced configuration for dashboards, analytics, or multi-market monitoring. Light compression (`1`) provides significant bandwidth savings with negligible CPU cost.

```bash
./target/release/orderbook_server \
    --compression-level 1 \
    --markets all \
    --data-dir /path/to/data
```

### BBO-Only (Lightweight)

Track only the top-of-book bid/ask for all coins. Uses ~100 MB RAM (vs ~1 GB for full markets). Ideal for price feeds, alerting, or environments with limited memory.

```bash
./target/release/orderbook_server \
    --features bbo \
    --compression-level 0 \
    --data-dir /path/to/data
```

## WebSocket API

If the server is started with `--secret YOUR_SECRET`, clients must include the
matching token in the WebSocket URL:

```text
ws://localhost:8000/ws?token=YOUR_SECRET
```

Without `--secret`, existing `/ws` connections remain accepted.

### Subscribe to BBO
```json
{ "method": "subscribe", "subscription": { "type": "bbo", "coin": "BTC" } }
```
Response:
```json
{ "channel": "bbo", "data": { "coin": "BTC", "time": 1702530000000, "bid": { "px": "100000.0", "sz": "0.5", "n": 1 }, "ask": { "px": "100001.0", "sz": "0.3", "n": 1 } } }
```

### Subscribe to All BBO
```json
{ "method": "subscribe", "subscription": { "type": "allbbo" } }
```
Initial response includes every current BBO matching `--markets`; later updates are throttled to 50 ms and include only changed coins.
```json
{ "channel": "allbbo", "data": { "time": 1702530000000, "bbos": [{ "coin": "BTC", "bid": { "px": "100000.0", "sz": "0.5", "n": 1 }, "ask": null }] } }
```

### Subscribe to Trades
```json
{ "method": "subscribe", "subscription": { "type": "trades", "coin": "BTC" } }
```
Response:
```json
{ "channel": "trades", "data": [{ "coin": "BTC", "side": "A", "px": "106296.0", "sz": "0.00017", "time": 1751430933565, "hash": "0x...", "tid": 293353986402527, "user": "0x..." }] }
```
Trades that are liquidations include an additional `liquidation` field with `liquidatedUser`, `markPx`, and `method`.

### Subscribe to Book Diffs
```json
{ "method": "subscribe", "subscription": { "type": "bookDiffs", "coin": "BTC" } }
```
Response:
```json
{ "channel": "bookDiffs", "data": [{ "user": "0x...", "oid": 123, "px": "50000.0", "coin": "BTC", "rawBookDiff": { "new": { "sz": "1.0" } } }] }
```
Streams raw order book diffs as they arrive. Each diff is one of: `new` (order added), `update` (size changed with `origSz` and `newSz`), or `remove` (order removed).

### Subscribe to L2 Orderbook
```json
{ "method": "subscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
```
Optional parameters: `nSigFigs` (2-5), `nLevels` (max 100, default 20), `mantissa` (2 or 5)

### Subscribe to L4 Orderbook
```json
{ "method": "subscribe", "subscription": { "type": "l4Book", "coin": "BTC" } }
```
> **Warning:** The initial L4 snapshot contains every individual order in the book and can be **very large** (several MB for liquid coins like BTC/ETH). Some WebSocket clients (e.g., Postman) may not handle payloads of this size. Use a capable client like `wscat` or `websocat`, or connect programmatically. After the initial snapshot, subsequent updates are incremental and lightweight.

### Subscribe to Order Updates (User-Specific)
Stream raw order status data for a specific user address:
```json
{ "method": "subscribe", "subscription": { "type": "orderUpdates", "user": "0x1234567890abcdef1234567890abcdef12345678" } }
```
Response:
```json
{ "channel": "orderUpdates", "data": [{ "user": "0x...", "time": 1702530000000, "height": 12345, "orderStatus": { "time": "...", "user": "0x...", "hash": "0x...", "status": "open", "order": { "coin": "BTC", "side": "B", "limitPx": "100000.0", "sz": "0.5", ... } } }] }
```
> **Note:** Requires node to run with `--write-order-statuses` flag enabled.

### Subscribe to Stats
```json
{ "method": "subscribe", "subscription": { "type": "stats" } }
```
Response, sent once per second:
```json
{ "channel": "stats", "data": { "time": 1750000000000, "tps": 123, "bps": 2, "height": 456789, "ops": 456 } }
```
`tps` counts parsed `node_fills` events in the last second, `bps` counts distinct node block numbers observed in that interval, `height` is the highest block seen, and `ops` counts place, update, cancel, and fill order operations.

### Ping/Pong
```json
{ "method": "ping" }
```
Response:
```json
{ "channel": "pong" }
```

### Unsubscribe
```json
{ "method": "unsubscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
```

## Node Requirements

The Hyperliquid node must run with **all** of these flags enabled:
- `--write-fills`
- `--write-order-statuses`
- `--write-raw-book-diffs`
- `--stream-with-block-info` — **required**, the server only reads from `*_streaming` directories
- `--disable-output-file-buffering` — ensures data is flushed immediately for low latency

## Architecture

```
┌──────────────────────┐     ┌──────────────────────────────────────────────┐
│   Hyperliquid Node   │     │  Orderbook Server                           │
│   (Docker/Direct)    │     │                                             │
│                      │     │  ┌──────────────────────────────┐           │
│  writes to:          │     │  │ Parallel File Watchers       │           │
│  - fills_streaming/  │─────▶  │ (3 inotify threads)          │           │
│  - order_statuses_   │     │  │  - order diffs (BBO-critical)│           │
│    streaming/        │     │  │  - order statuses            │           │
│  - book_diffs_       │     │  │  - fills                     │           │
│    streaming/        │     │  └──────────┬───────────────────┘           │
│                      │     │             │ crossbeam → tokio bridge      │
│                      │     │  ┌──────────▼───────────────────┐           │
│  snapshot via:       │     │  │ OrderBook State              │           │
│  hl-node CLI ◀───────│─────│  │  - L4 in-memory book         │           │
│  (startup only)      │     │  │  - L2 snapshot computation   │           │
│                      │     │  │  - BBO deduplication         │           │
│                      │     │  └──────────┬───────────────────┘           │
│                      │     │             │ broadcast channel             │
│                      │     │  ┌──────────▼──────┐  ┌─────────────────┐  │
│                      │     │  │ WebSocket Server │──│ Connected       │  │
│                      │     │  │ (axum + yawc)    │  │ Clients         │  │
└──────────────────────┘     │  └──────────────────┘  └─────────────────┘  │
                             │                                             │
                             │  ┌─────────────────┐                        │
                             │  │ Metrics Server   │  GET /metrics         │
                             │  │ (Prometheus)     │  GET /health          │
                             │  └─────────────────┘                        │
                             └──────────────────────────────────────────────┘
```

**Data flow:**
1. The Hyperliquid node writes real-time events to `*_streaming/` directories as newline-delimited JSON
2. Three parallel inotify file watchers detect changes immediately (one per event source)
3. Events are bridged from crossbeam channels (blocking I/O threads) to the tokio async runtime
4. The OrderBook State applies diffs/statuses independently (no block-level batching) for lowest latency
5. Changed BBOs and L2 snapshots are broadcast to subscribed WebSocket clients with deduplication

## Performance

### Deduplication

| Type | Behavior |
|------|----------|
| BBO | Only sends when bid/ask px/sz changes |
| L2Book | Only sends when snapshot hash changes |
| Trades | Only sends on fills |

### Latency

| Metric | Value |
|--------|-------|
| BBO update frequency | ~100+/sec (streaming) |
| BBO latency | ~100ms |
| BBO dedup overhead | ~1us |
| L2 dedup overhead | ~10us |
| Savings when unchanged | ~500us |

## Prometheus Metrics

Metrics are exposed on port 9090 by default (configurable via `--metrics-port`):

```bash
curl http://localhost:9090/metrics
```

### Available Metrics

| Category | Metric | Description |
|----------|--------|-------------|
| **Connections** | `ws_connections_active` | Current WebSocket connections |
| | `ws_connections_total` | Total connections since startup |
| | `ws_subscriptions_active{type}` | Active subscriptions by type (bbo/allbbo/l2Book/l4Book/trades/bookDiffs/orderUpdates) |
| | `broadcast_receivers` | Number of broadcast channel receivers |
| **Throughput** | `events_processed_total{type}` | Events by type (orders/diffs/fills) |
| | `broadcasts_total{channel}` | Broadcasts by channel (bbo/allbbo/l2/l4/trades/stats) |
| | `messages_sent_total` | Total WebSocket messages sent |
| | `bbo_changes_total{coin}` | BBO changes per coin |
| **Health** | `orderbook_height` | Current block height |
| | `orderbook_time_ms` | Orderbook timestamp |
| | `orderbook_orders_total` | Total orders in the book |
| | `orderbook_coins_count` | Number of coins tracked |
| | `pending_orders_cache_size` | Pending order statuses in HFT cache |
| | `pending_diffs_cache_size` | Pending book diffs in HFT cache |
| | `uptime_seconds` | Server uptime in seconds |
| | `server_start_time_seconds` | Server start timestamp (unix) |
| **Latency** | `bbo_broadcast_latency_seconds` | BBO broadcast latency histogram |
| | `l2_broadcast_latency_seconds` | L2 broadcast latency histogram |
| | `event_processing_latency_seconds{event_type}` | Per-event processing latency |
| **File Watcher** | `file_events_total{source}` | File events received by source |
| | `file_lines_parsed_total{source}` | Lines parsed from files by source |
| **Errors** | `parse_errors_total{type}` | JSON parse errors by source |
| | `ws_send_errors_total` | WebSocket send errors |
| | `channel_drops_total` | Messages dropped due to lag |
| | `broadcast_channel_lag` | Broadcast channel lag (receivers behind) |

### Disable Metrics

```bash
./target/release/orderbook_server --metrics-port 0
```

## Health Check

A health endpoint is available on the same port as the WebSocket server:

```bash
curl http://localhost:8000/health
```

Response:
```json
{"status":"ready","uptime_seconds":3600,"height":123456,"connections":5}
```

| Field | Description |
|-------|-------------|
| `status` | `ready` or `initializing` |
| `uptime_seconds` | Server uptime |
| `height` | Current block height |
| `connections` | Active WebSocket connections |

## Deployment

### Security & exposure model

**This server ships with no TLS, no authorization, and no per-IP rate limiting.** The optional `--secret` flag adds simple shared-token authentication for WebSocket handshakes via `?token=<secret>`, but transport security and abuse controls still live in your deployment infrastructure. If you bind the server to a public interface without a reverse proxy in front of it, an attacker can DoS it with a handful of TCP sockets. Treat the WebSocket port as you would a Postgres or Redis port: private, fronted, and rate-limited.

Recommended deployment topology:

```
   Public Internet ──HTTPS/WSS──▶  Nginx / Caddy / Cloudflare  ──HTTP/WS──▶  orderbook_server (127.0.0.1:8000)
                                   (TLS, Origin, rate limits)              (no public exposure)
```

Concretely:

1. **Bind the server to `127.0.0.1`** (or a private/VPC interface). The `--address 0.0.0.0` example in *Quick Start* is fine on a private network or behind a firewall but should not be used directly on the public internet.
2. **Terminate TLS at the reverse proxy.** Let's Encrypt + Caddy/Nginx is the easy default; cloud LBs / Cloudflare also work.
3. **Use `--secret` for shared-token WebSocket access** when clients connect through a private proxy or trusted network. Clients must use `/ws?token=<secret>`. URL-encode the token if it contains reserved URL characters.
4. **Set per-IP connection and message limits at the proxy.** A single client subscribing to a few thousand `(coin, n_sig_figs, mantissa)` tuples can already make every other client pay for it; the server applies a hard cap of 256 subscriptions per WS connection, but the proxy is your first line of defense.
5. **Enforce an `Origin` allowlist** if browser clients will connect, so other websites can't open WS connections from a user's session.
6. **Lock down the metrics endpoint.** `--metrics-port` binds to `0.0.0.0:9090` by default and exposes internal telemetry that helps an attacker fingerprint your fleet. Run with `--metrics-port 0` to disable it, or proxy/firewall it to your Prometheus scraper only. There is no auth on `/metrics`.
7. **Run `hl-node` and `orderbook_server` as separate Unix users.** `orderbook_server` only needs read access to the streaming directories (`node_*_streaming/`). It should *not* be able to write into the node's state.
8. **Use the included systemd unit as a starting point**, but tighten it: set `LimitNOFILE`, `LimitNPROC`, `MemoryHigh` / `MemoryMax`, `Restart=on-failure`, and a short `RestartSec`. A short restart loop is acceptable because the server reloads its snapshot on startup.

Minimal Nginx snippet for fronting the server (adjust paths and TLS as needed):

```nginx
upstream orderbook {
    server 127.0.0.1:8000;
    keepalive 4;
}

map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

limit_conn_zone $binary_remote_addr zone=ob_perip:10m;

server {
    listen 443 ssl http2;
    server_name your-domain.example;

    ssl_certificate     /etc/letsencrypt/live/your-domain.example/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.example/privkey.pem;

    location /ws {
        limit_conn ob_perip 8;            # at most 8 concurrent WS per IP
        proxy_pass http://orderbook;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_read_timeout 600s;          # match your heartbeat cadence
        client_max_body_size 1k;          # subscribe messages are tiny
    }
}
```

If you must run without a proxy (development, internal-only), at least bind to `127.0.0.1` and ssh-tunnel into it.

### Systemd Service

An example service file is included (`orderbook-server.service`):

```bash
# Copy and edit the service file
cp orderbook-server.service /etc/systemd/system/
vim /etc/systemd/system/orderbook-server.service  # adjust paths

# Enable and start
systemctl daemon-reload
systemctl enable orderbook-server
systemctl start orderbook-server

# View logs
journalctl -u orderbook-server -f
```

## Caveats

- **No untriggered orders** - Only shows orders on the book
- **Snapshot sync time** - Initial snapshot takes ~10-30 seconds

## Differences from the Hyperliquid Public Release

This project started from the [Hyperliquid public orderbook server](https://github.com/hyperliquid-dex/order_book) and was substantially rewritten for production use. Here is what changed:

### Event Processing: Block-Batched vs Event-by-Event

The original server batches events by block number and waits for an entire block's worth of updates before applying them. This adds latency proportional to block time.

This fork processes every order diff, status, and fill **the instant it arrives** from the node's streaming files, without waiting for block boundaries. The result is ~100+ BBO updates per second with ~100ms end-to-end latency.

### File Watching: Single Thread vs 3 Parallel Threads

The original uses a single file watcher thread that handles all three event sources (order statuses, book diffs, fills) sequentially.

This fork spawns dedicated inotify threads for the event sources required by `--features`, with independent crossbeam channels bridged to the tokio async runtime. Order diffs (the BBO-critical path) are never blocked by slow fill or status parsing.

### New Subscription Types

| Feature | Original | This Fork |
|---------|----------|-----------|
| L2Book | Yes | Yes |
| L4Book | Yes | Yes |
| Trades | Yes | Yes |
| **BBO** | No | Yes - dedicated top-of-book feed with per-coin deduplication |
| **orderUpdates** | No | Yes - per-user order status stream (filter by address) |

### Deduplication

The original sends every update to every subscriber regardless of whether anything changed.

This fork deduplicates at the WebSocket level:
- **BBO**: only sends when bid/ask px/sz actually changes (~1us overhead)
- **allbbo**: sends a full initial BBO snapshot, then 50 ms batches containing only changed coins
- **L2Book**: only sends when the snapshot hash changes (~10us overhead)
- Saves ~500us per unchanged update and significantly reduces client-side bandwidth

### Granular Feature Selection

The `--features` flag lets operators enable only the channels they need. For example, `--features bbo` replaces the old BBO-only mode, `--features allbbo` serves a single batched top-of-book stream, and raw-only configurations such as `--features trades,orderupdates` or `--features stats` skip the startup snapshot and in-memory orderbook maintenance entirely.

### Snapshot Modes: Docker & Direct

The original fetches snapshots via HTTP POST to `localhost:3001` (the node's local RPC).

This fork calls `hl-node --dump-abci-state` directly, with two modes:
- **Docker**: `docker exec <container> hl-node ...` for container-based setups
- **Direct**: calls the binary on the host for systemctl / bare metal deployments

All paths (`abci_state.rmp`, `snapshot.json`, `visor_abci_state.json`) are auto-detected with manual override options.

### Market Filtering

The original has a hard-coded `ignore_spot` flag. This fork adds a `--markets` CLI flag supporting comma-delimited `perps`, `spot`, and `hip3` values, plus `all` as a compatibility alias. Use `--markets perps,hip3` to exclude spot while keeping HIP-3 markets.

### Prometheus Metrics & Health Endpoint

The original has no monitoring. This fork adds:
- **`/health`** endpoint with status, uptime, block height, and connection count
- **`/metrics`** endpoint exposing 25+ Prometheus metrics covering connections, subscriptions, latency histograms, throughput, orderbook health, parse errors, and file watcher stats
- Pre-built Grafana dashboard included in `monitoring/`

### WebSocket Compression

Both use `yawc` with `permessage-deflate`. This fork increases the broadcast channel buffer from 100 to 256 to reduce "channel lagged" drops under load, and adds detailed documentation on compression level tradeoffs.

### Other Improvements

- **Graceful shutdown** via `Ctrl+C` / `SIGINT` signal handling
- **Configurable log levels** (`--log-level error|warn|info|debug|trace`)
- **Faster JSON parsing** with `sonic-rs` on the hot path
- **Systemd service file** included for production deployment
- **Comprehensive CLI** with auto-detected defaults and full override support

### Summary

| | Original | This Fork |
|---|----------|-----------|
| Event model | Block-batched | Event-by-event |
| File watchers | 1 thread | 3 parallel threads |
| BBO subscription | No | Yes + dedup |
| Order updates | No | Yes (per-user) |
| Granular feature flag | No | Yes (`--features ...`) |
| Metrics | None | 25+ Prometheus metrics |
| Health endpoint | No | Yes |
| Snapshot modes | HTTP RPC | Docker + Direct CLI |
| Market filtering | Hard-coded | --markets flag |
| Graceful shutdown | No | Yes |
| JSON parser | serde_json | sonic-rs |

## Managed Orderbook Service

If you'd rather skip running your own infrastructure, or you need capabilities beyond what this open-source server provides, check out our managed offering at **[hyperpc.app/products/data/orderbook-websocket](https://hyperpc.app/products/data/orderbook-websocket)**.

- **Hosted orderbook feeds** - Connect directly and consume real-time BBO, L2, L4, and trade data without managing a node or this server yourself
- **Lower latency via direct sentry peering** - We can peer your orderbook server directly with a Hyperliquid sentry node for reduced hop count and tighter latencies
- **Extended data services** - Recurring L4 snapshots, liquidation feeds, historical orderbook data, and custom data pipelines tailored to your trading infrastructure
- **Custom deployments** - Dedicated instances, co-located setups, or private infrastructure built around your specific latency and throughput requirements

Reach out to us at [hyperpc.app](https://hyperpc.app) to discuss what you need.

## License

MIT

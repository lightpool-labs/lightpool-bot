# lightpool-bot

LightPool trading bot fork based on [NautilusTrader](https://nautilustrader.io). It includes the `lightpool-strategies` crate with a live Polymarket liquidity maker.

## Prerequisites

- Rust **1.96.0** (see `rust-version` in `Cargo.toml`)
- Network access to Polymarket Gamma / CLOB HTTP and WebSocket APIs

## Build

From this directory:

```bash
cargo build -p lightpool-strategies --bin liquidity-maker
```

## Run liquidity maker

Subscribe to Polymarket order book deltas for a single **event slug** and print managed books from cache.

```bash
cargo run -p lightpool-strategies --bin liquidity-maker -- \
  --slug fed-rate-hike-in-2026
```

Optional flags:

```bash
cargo run -p lightpool-strategies --bin liquidity-maker -- \
  --slug fed-rate-hike-in-2026 \
  --depth 10 \
  --log-interval 50
```

| Flag | Default | Description |
|------|---------|-------------|
| `--slug` | (required) | Polymarket event slug, e.g. `fed-rate-hike-in-2026` from `https://polymarket.com/event/<slug>` |
| `--depth` | `10` | Book levels per side |
| `--log-interval` | `50` | Log cache book every N delta batches; use `1` for every batch, `0` to disable |

## Proxy

HTTP and WebSocket traffic use a proxy when set via environment variables (also loaded from `.env`):

```bash
export HTTPS_PROXY=http://127.0.0.1:7890
cargo run -p lightpool-strategies --bin liquidity-maker -- \
  --slug fed-rate-hike-in-2026
```

# lightpool-bot

LightPool trading bot based on [NautilusTrader](https://nautilustrader.io). The `lightpool-strategies` crate includes a dual-venue **liquidity maker** that mirrors Polymarket order books onto LightPool.

The bot talks to LightPool through **clob-index** (HTTP + WebSocket). It does not connect to the node RPC directly.

## Prerequisites

- Rust **1.96.0** (see `rust-version` in `Cargo.toml`)
- Sibling repos under `lightpool-labs/`:
  - `lightpool-node`
  - `lightpool-clob-index`
  - `event-contract-app` (backend + frontend)
  - `lightpool` (CLI scripts)
- Network access to Polymarket Gamma / CLOB HTTP and WebSocket APIs

## End-to-end flow

Run each step in its own terminal. Keep earlier services running.

### 1. LightPool node (`lightpool-node`)

```bash
cd ../lightpool-node
cargo build --release
source ./env.sh
lightpool
```

Default local ports: RPC `http://127.0.0.1:26300`, WS `ws://127.0.0.1:26400`, mempool `127.0.0.1:26000`.

See `lightpool-node/README.md` for wallet setup and multi-node scripts.

### 2. CLOB index (`lightpool-clob-index`)

Indexes chain events and exposes market / order book / tx APIs for the bot and app.

```bash
cd ../lightpool-clob-index
cp .env.example .env   # if present
cargo run --release
```

Default listen address: `http://127.0.0.1:3002` (WS on the same port).

### 3. Backend (`event-contract-app/backend`)

```bash
cd ../event-contract-app/backend
cp .env.example .env
cargo run
```

API: `http://127.0.0.1:3001/api`

### 4. Frontend (`event-contract-app/frontend`)

```bash
cd ../event-contract-app/frontend
cp .env.example .env.local
npm install
npm run dev
```

UI: `http://127.0.0.1:3000`

### 5. Bootstrap chain sample (`setup.py` via `lightpool-node`)

Creates USDT, transfers collateral, and creates a vault. Market create/mint is handled later by the liquidity maker (`--bootstrap-markets`).

From **`lightpool-node`** (so `lightpool-cli` from `env.sh` is on `PATH`):

```bash
cd ../lightpool-node
source ./env.sh
python3 scripts/event-contract-setup/setup.py
```

### 6. Run liquidity maker (`lightpool-bot`)

Bootstrap top-N Polymarket markets into LightPool, then mirror books:

```bash
cd ../lightpool-bot
cargo run -p lightpool-strategies --bin liquidity-maker -- \
  --polymarket-slug world-cup-winner \
  --bootstrap-markets \
  --max-markets 5
```

Or attach to an existing LightPool market slug:

```bash
cargo run -p lightpool-strategies --bin liquidity-maker -- \
  --polymarket-slug world-cup-winner \
  --lightpool-slug france-world-cup-2026
```

| Flag | Default | Description |
|------|---------|-------------|
| `--polymarket-slug` | (required) | Polymarket event slug |
| `--lightpool-slug` | | LightPool market slug (required unless `--bootstrap-markets` or `--polymarket-only`) |
| `--bootstrap-markets` | `false` | Create + mint top-N LightPool markets from Polymarket |
| `--max-markets` | `5` | Max markets to bootstrap / subscribe |
| `--depth` | `10` | Book levels per side |
| `--log-interval` | `50` | Log cache book every N delta batches; `0` disables |
| `--no-trading` | `false` | Data/logging only (no LightPool mirroring) |
| `--polymarket-only` | `false` | Disable LightPool data client |

## Environment

LightPool adapter (defaults shown):

```bash
export LIGHTPOOL_CLOB_INDEX_HTTP=http://127.0.0.1:3002
export LIGHTPOOL_CLOB_INDEX_WS=ws://127.0.0.1:3002
export LIGHTPOOL_PRIVATE_KEY=...   # base64 secret key; required for execution
```

HTTP / WebSocket proxy for Polymarket (also loaded from `.env`):

```bash
export HTTPS_PROXY=http://127.0.0.1:8118
```

## Build only

```bash
cargo build -p lightpool-strategies --bin liquidity-maker
```

# nautilus-lightpool

LightPool adapter for NautilusTrader / lightpool-bot.

- **Data**: clob-index HTTP book snapshots + WebSocket `orderbook_delta`
- **Execution**: on-chain `place_order` / `cancel_order` via `lightpool-sdk`

## Example

```sh
cd external/lightpool-bot
cargo run -p nautilus-lightpool --example lightpool-data-tester -- \
  --slug your-market-slug \
  --depth 10
```

Environment:

- `LIGHTPOOL_CLOB_INDEX_HTTP` (default `http://127.0.0.1:3002`)
- `LIGHTPOOL_CLOB_INDEX_WS` (default `ws://127.0.0.1:3002`)
- `LIGHTPOOL_NODE_RPC` (execution only, default `http://127.0.0.1:9000`)
- `LIGHTPOOL_PRIVATE_KEY` (execution only, base64 secret key)

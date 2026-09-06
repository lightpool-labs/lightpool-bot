# nautilus-lightpool

LightPool adapter for NautilusTrader / lightpool-bot.

**LightPool stack version:** `0.5.0`

- **Data**: clob-index HTTP market bootstrap + WebSocket `orderbook_delta`
- **Execution**: signed transactions submitted via clob-index `/api/tx/submit`

The bot does not connect to the lightpool node directly. Run `lightpool-clob-indexer` separately; it owns the node RPC/WS connection.

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
- `LIGHTPOOL_PRIVATE_KEY` (execution only, base64 secret key)

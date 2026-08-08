// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! LightPool WebSocket market-data tool.
//!
//! Mirrors Hyperliquid `bin/ws_data.rs`:
//! `request_instruments` → `cache_instruments` → `connect` → subscribe → `next_event`.
//!
//! Optional env:
//! - `LIGHTPOOL_WS_SYMBOL` — instrument id (e.g. `SLUG-YES.LIGHTPOOL`);
//!   default: first BinaryOption from `request_instruments`
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-ws-data
//! ```

use std::{env, time::Duration};

use anyhow::{Context, Result};
use nautilus_lightpool::websocket::clob_index::ClobIndexWsClient;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use tokio::{pin, signal};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    log::info!("Starting LightPool WebSocket data example");

    let mut client = ClobIndexWsClient::from_env();
    let instruments = client.request_instruments().await?;
    log::info!("Loaded {} instruments", instruments.len());

    let instrument_id = if let Ok(symbol) = env::var("LIGHTPOOL_WS_SYMBOL") {
        InstrumentId::from(symbol.trim())
    } else {
        instruments
            .iter()
            .find(|inst| matches!(inst, InstrumentAny::BinaryOption(_)))
            .or_else(|| instruments.first())
            .map(|inst| inst.id())
            .context("no instruments returned from request_instruments")?
    };
    log::info!("Using instrument={instrument_id}");

    client.cache_instruments(instruments);
    client.connect().await?;
    log::info!("Connected to LightPool WebSocket");

    tokio::time::sleep(Duration::from_millis(500)).await;

    log::info!("Subscribing to orderbook for {instrument_id}");
    client.subscribe_orderbook(instrument_id, 10).await?;

    log::info!("Subscribing to quotes for {instrument_id}");
    client.subscribe_quotes(instrument_id).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let sigint = signal::ctrl_c();
    pin!(sigint);

    let mut message_count = 0u64;

    loop {
        tokio::select! {
            maybe_message = client.next_event() => {
                match maybe_message {
                    Some(message) => {
                        message_count += 1;
                        log::info!("Message #{message_count}: {message:?}");
                    }
                    None => {
                        log::warn!("WebSocket event stream closed");
                        break;
                    }
                }
            }
            _ = &mut sigint => {
                log::info!("Received SIGINT, closing connection...");
                client.disconnect().await?;
                break;
            }
        }
    }

    log::info!("Received {message_count} total messages");
    Ok(())
}

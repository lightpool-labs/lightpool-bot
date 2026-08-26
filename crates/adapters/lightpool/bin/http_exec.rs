// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! LightPool HTTP execution smoke tool (place one limit order via clob-index).
//!
//! Mirrors Hyperliquid `bin/http_exec.rs`:
//! load instruments → submit a passive buy limit order.
//!
//! Optional env:
//! - `LIGHTPOOL_EXEC_PX` — limit price as probability (default `0.45`)
//! - `LIGHTPOOL_EXEC_QTY` — size in shares (default `1`)
//!
//! Prerequisites:
//! - `lightpool-clob-indexer` running
//! - `LIGHTPOOL_PRIVATE_KEY` or `~/.lightpool/wallet.json`
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-http-exec
//! ```

use std::{env, str::FromStr};

use anyhow::{Context, Result};
use nautilus_lightpool::http::clob_index::ClobIndexHttpClient;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let px = Price::from_str(
        env::var("LIGHTPOOL_EXEC_PX")
            .unwrap_or_else(|_| "0.45".to_string())
            .as_str(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let qty = Quantity::from_str(
        env::var("LIGHTPOOL_EXEC_QTY")
            .unwrap_or_else(|_| "1".to_string())
            .as_str(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let client = ClobIndexHttpClient::from_env().context(
        "Set LIGHTPOOL_PRIVATE_KEY or create ~/.lightpool/wallet.json",
    )?;
    let wallet = client.get_user_address()?;
    log::info!("Wallet: {wallet}");

    log::info!("Fetching instruments...");
    let instruments = client.request_instruments().await?;
    client.cache_instruments(instruments.clone());
    log::info!("Fetched {} instruments", instruments.len());

    let instrument = instruments
        .iter()
        .find(|inst| matches!(inst, InstrumentAny::BinaryOption(_)))
        .or_else(|| instruments.first())
        .context("no instruments returned from request_instruments")?;
    let instrument_id = instrument.id();
    log::info!("Using instrument={instrument_id}");

    log::info!("Placing order: buy {qty} @ {px}");
    let (digest, chain_order_id) = client
        .submit_order(
            instrument_id,
            OrderSide::Buy,
            OrderType::Limit,
            qty,
            TimeInForce::Gtc,
            Some(px),
        )
        .await?;
    log::info!("Order placed successfully!");
    log::info!("digest={digest}");
    log::info!("chain_order_id={chain_order_id}");

    log::info!("Done!");
    Ok(())
}

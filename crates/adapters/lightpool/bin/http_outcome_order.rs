// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Smoke test for LightPool outcome (BinaryOption) order placement.
//!
//! Mirrors Hyperliquid `bin/http_outcome_order.rs`:
//! load instruments → place a far-from-touch limit buy on a YES/NO outcome
//! spot market → cancel it.
//!
//! Optional env:
//! - `LIGHTPOOL_OUTCOME_SYMBOL` — instrument id (e.g. `SLUG-YES.LIGHTPOOL`);
//!   default: first BinaryOption from `request_instruments`
//! - `LIGHTPOOL_OUTCOME_PX` — limit price as probability (default `0.05`)
//! - `LIGHTPOOL_OUTCOME_QTY` — size in shares (default `1`)
//!
//! Prerequisites:
//! - `lightpool-clob-indexer` running
//! - `LIGHTPOOL_PRIVATE_KEY` or `~/.lightpool/wallet.json`
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-http-outcome-order
//! ```

use std::{env, str::FromStr};

use anyhow::{Context, Result};
use nautilus_lightpool::http::clob_index::ClobIndexHttpClient;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let px = Price::from_str(
        env::var("LIGHTPOOL_OUTCOME_PX")
            .unwrap_or_else(|_| "0.05".to_string())
            .as_str(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let qty = Quantity::from_str(
        env::var("LIGHTPOOL_OUTCOME_QTY")
            .unwrap_or_else(|_| "1".to_string())
            .as_str(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let client = ClobIndexHttpClient::from_env().context(
        "Set LIGHTPOOL_PRIVATE_KEY or create ~/.lightpool/wallet.json",
    )?;
    let wallet = client.get_user_address()?;
    log::info!("Wallet: {wallet}");

    log::info!("Loading instruments (including outcomes)...");
    let instruments = client.request_instruments().await?;
    client.cache_instruments(instruments.clone());
    log::info!("Cached {} instruments", instruments.len());

    let instrument_id = if let Ok(symbol) = env::var("LIGHTPOOL_OUTCOME_SYMBOL") {
        InstrumentId::from(symbol.trim())
    } else {
        instruments
            .iter()
            .find(|inst| matches!(inst, InstrumentAny::BinaryOption(_)))
            .map(|inst| inst.id())
            .context("no BinaryOption instruments returned from request_instruments")?
    };
    log::info!("LightPool outcome-order smoke: {instrument_id} buy {qty} @ {px}");

    log::info!("Submitting limit buy...");
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
    log::info!("Order accepted: digest={digest} chain_order_id={chain_order_id}");

    log::info!("Cancelling the resting order...");
    let cancel_digest = client.cancel_order(instrument_id, chain_order_id).await?;
    log::info!("Cancel acknowledged: digest={cancel_digest}");

    log::info!("Done");
    Ok(())
}

// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Seed a YES instrument order book with passive bid/ask levels via clob-index.
//!
//! Env:
//! - `LIGHTPOOL_SEED_LEVELS` — levels per side (default `10`)
//! - `LIGHTPOOL_SEED_QTY` — size per level in shares (default `1`)
//! - `LIGHTPOOL_SEED_MID` — mid probability (default `0.50`)
//! - `LIGHTPOOL_SEED_TICK` — price step between levels (default `0.001`)
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-http-seed-book
//! ```

use std::{env, str::FromStr};

use anyhow::{Context, Result};
use nautilus_lightpool::http::clob_index::ClobIndexHttpClient;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let levels: usize = env::var("LIGHTPOOL_SEED_LEVELS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(10)
        .max(1);
    let qty = Quantity::from_str(
        env::var("LIGHTPOOL_SEED_QTY")
            .unwrap_or_else(|_| "1".to_string())
            .as_str(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mid = Decimal::from_str(
        env::var("LIGHTPOOL_SEED_MID")
            .unwrap_or_else(|_| "0.50".to_string())
            .as_str(),
    )
    .context("LIGHTPOOL_SEED_MID")?;
    let tick = Decimal::from_str(
        env::var("LIGHTPOOL_SEED_TICK")
            .unwrap_or_else(|_| "0.001".to_string())
            .as_str(),
    )
    .context("LIGHTPOOL_SEED_TICK")?;
    if tick <= Decimal::ZERO {
        anyhow::bail!("LIGHTPOOL_SEED_TICK must be > 0");
    }

    let client = ClobIndexHttpClient::from_env().context(
        "Set LIGHTPOOL_PRIVATE_KEY or create ~/.lightpool/wallet.json",
    )?;
    let wallet = client.get_user_address()?;
    log::info!("Wallet: {wallet}");

    let instruments = client.request_instruments().await?;
    client.cache_instruments(instruments.clone());
    let instrument = instruments
        .iter()
        .find(|inst| {
            matches!(inst, InstrumentAny::BinaryOption(_))
                && instrument_is_yes(inst)
        })
        .or_else(|| {
            instruments
                .iter()
                .find(|inst| matches!(inst, InstrumentAny::BinaryOption(_)))
        })
        .or_else(|| instruments.first())
        .context("no instruments; run lightpool-http-bootstrap first")?;
    let instrument_id = instrument.id();
    let spot_market = instrument.raw_symbol().to_string();
    log::info!(
        "Seeding book instrument={instrument_id} spot_market={spot_market} levels={levels} qty={qty}"
    );

    for i in 0..levels {
        let offset = tick * Decimal::from((i + 1) as u64);
        let bid_dec = mid - offset;
        let ask_dec = mid + offset;
        if bid_dec <= Decimal::ZERO || ask_dec >= Decimal::ONE {
            anyhow::bail!(
                "level {i}: bid={bid_dec} ask={ask_dec} out of (0,1); adjust MID/TICK/LEVELS"
            );
        }
        let bid_px = Price::from_str(&bid_dec.normalize().to_string())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let ask_px = Price::from_str(&ask_dec.normalize().to_string())
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let (bid_digest, bid_oid) = client
            .submit_order(
                instrument_id,
                OrderSide::Buy,
                OrderType::Limit,
                qty,
                TimeInForce::Gtc,
                Some(bid_px),
            )
            .await
            .with_context(|| format!("bid level {} @ {bid_px}", i + 1))?;
        log::info!(
            "bid[{}] {} @ {} digest={bid_digest} oid={bid_oid}",
            i + 1,
            qty,
            bid_px
        );

        let (ask_digest, ask_oid) = client
            .submit_order(
                instrument_id,
                OrderSide::Sell,
                OrderType::Limit,
                qty,
                TimeInForce::Gtc,
                Some(ask_px),
            )
            .await
            .with_context(|| format!("ask level {} @ {ask_px}", i + 1))?;
        log::info!(
            "ask[{}] {} @ {} digest={ask_digest} oid={ask_oid}",
            i + 1,
            qty,
            ask_px
        );
    }

    let book = client.fetch_book_snapshot(&spot_market, levels as u32).await?;
    log::info!(
        "Book sequence={} bids={} asks={}",
        book.sequence,
        book.bids.len(),
        book.asks.len()
    );
    for (i, level) in book.bids.iter().enumerate() {
        log::info!("  bid[{}]: {} @ {}", i + 1, level.size, level.price);
    }
    for (i, level) in book.asks.iter().enumerate() {
        log::info!("  ask[{}]: {} @ {}", i + 1, level.size, level.price);
    }

    Ok(())
}

fn instrument_is_yes(inst: &InstrumentAny) -> bool {
    inst.id().symbol.as_str().to_ascii_uppercase().contains("YES")
}

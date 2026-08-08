// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! LightPool HTTP public API smoke tool (clob-index).
//!
//! Mirrors Hyperliquid `bin/http_public.rs`:
//! load instruments, then fetch a public book snapshot for one instrument.
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-http-public
//! ```

use nautilus_lightpool::{
    config::clob_index_http_from_env,
    http::clob_index::ClobIndexHttpClient,
};
use nautilus_model::instruments::{Instrument, InstrumentAny};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let http_url = clob_index_http_from_env();
    log::info!("Starting LightPool HTTP public example");
    log::info!("HTTP={http_url}");

    let client = ClobIndexHttpClient::new(&http_url);

    let instruments = client.request_instruments().await?;
    log::info!("Fetched {} instruments", instruments.len());

    let instrument = instruments
        .iter()
        .find(|inst| matches!(inst, InstrumentAny::BinaryOption(_)))
        .or_else(|| instruments.first())
        .ok_or("no instruments returned from request_instruments")?;
    let instrument_id = instrument.id();
    let spot_market = instrument.raw_symbol().to_string();
    log::info!("Using instrument={instrument_id} spot_market={spot_market}");

    if let Ok(info) = client.fetch_spot_info(&spot_market).await {
        log::info!(
            "Spot info state={} tick_size={} last_price={:?}",
            info.state,
            info.tick_size,
            info.last_price
        );
    }

    if let Ok(book) = client.fetch_book_snapshot(&spot_market, 10).await {
        let best_bid = book
            .bids
            .first()
            .map(|level| format!("{}@{}", level.size, level.price))
            .unwrap_or_else(|| "-".into());
        let best_ask = book
            .asks
            .first()
            .map(|level| format!("{}@{}", level.size, level.price))
            .unwrap_or_else(|| "-".into());
        log::info!(
            "Book sequence={} bids={} asks={} best bid: {best_bid}, best ask: {best_ask}",
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
    }

    Ok(())
}

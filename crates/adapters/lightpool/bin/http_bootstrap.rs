// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Seed LightPool with sample event markets via clob-index.
//!
//! Creates + mints complete sets so `request_instruments` returns data for
//! adapter smoke bins (`http-public`, `ws-data`, …).
//!
//! Env:
//! - `LIGHTPOOL_COLLATERAL_TOKEN` (default `0x0200000000000001`)
//! - `LIGHTPOOL_BOOTSTRAP_COUNT` — markets to create (default `2`)
//! - `LIGHTPOOL_BOOTSTRAP_MINT` — complete-set mint amount in raw units
//!   (default `1000000000` = 1000 tokens at 6 decimals)
//!
//! Prerequisites:
//! - lightpool node + `lightpool-clob-indexer` running
//! - default wallet `~/.lightpool/wallet.json` with enough collateral
//!   (`lightpool create-wallet`; optional `LIGHTPOOL_PRIVATE_KEY` override)
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-http-bootstrap
//! # or from lightpool-node:
//! #   ./scripts/bot-testing/seed_dev_markets.sh
//! ```

use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use nautilus_lightpool::{
    config::resolve_collateral_token,
    http::{
        bootstrap::bootstrap_one_market,
        clob_index::ClobIndexHttpClient,
    },
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let count: usize = env::var("LIGHTPOOL_BOOTSTRAP_COUNT")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(2)
        .max(1);
    let mint_amount: u64 = env::var("LIGHTPOOL_BOOTSTRAP_MINT")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(1_000_000_000); // 1000 tokens @ 6 decimals
    let collateral = resolve_collateral_token();

    let client = ClobIndexHttpClient::from_env().context(
        "Default wallet missing: run `lightpool create-wallet` (~/.lightpool/wallet.json)",
    )?;
    let wallet = client.get_user_address()?;
    log::info!("Wallet (default ~/.lightpool/wallet.json): {wallet}");
    log::info!("Collateral token: {collateral}");
    log::info!("Creating {count} market(s), mint_raw={mint_amount}");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let deadline = now.saturating_add(86_400 * 30);

    // Signer is inside client; bootstrap helpers still take &Signer.
    let private_key = nautilus_lightpool::config::resolve_private_key()?;
    let signer = nautilus_lightpool::common::signer::signer_from_private_key(&private_key)?;

    for i in 0..count {
        let question = format!("Dev seed market #{} — will it rain? ({now})", i + 1);
        log::info!("[{}/{}] create+mint: {question}", i + 1, count);
        let created = bootstrap_one_market(
            &client,
            &signer,
            &question,
            &collateral,
            deadline,
            mint_amount,
        )
        .await
        .with_context(|| format!("bootstrap market {}", i + 1))?;
        log::info!(
            "OK slug={} market={} yes={} no={}",
            created.slug,
            created.market_address,
            created.yes_token,
            created.no_token
        );
    }

    let instruments = client.request_instruments().await?;
    log::info!(
        "Done. clob-index now has {} instrument(s). Try: \
         cargo run -p nautilus-lightpool --bin lightpool-http-public",
        instruments.len()
    );
    Ok(())
}

// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Smoke test for LightPool complete-set mint/burn (HL `userOutcome` equivalent).
//!
//! Mirrors Hyperliquid `bin/http_user_outcome.rs`:
//! - `split` / `mint` → collateral → YES + NO (`mint_event_contract`)
//! - `merge` / `burn` → YES + NO → collateral (`burn_event_contract`)
//!
//! Env:
//! - `LIGHTPOOL_OP=split|merge|mint|burn` (default `split`)
//! - `LIGHTPOOL_MARKET_SLUG` — market slug (default: first market from clob-index)
//! - `LIGHTPOOL_OUTCOME_AMOUNT` — decimal shares (required for split/mint;
//!   optional for merge/burn: omit to burn min(YES, NO) available balance)
//!
//! Prerequisites:
//! - `lightpool-clob-indexer` running
//! - `LIGHTPOOL_PRIVATE_KEY` or `~/.lightpool/wallet.json`
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-http-user-outcome
//! ```

use std::{env, str::FromStr};

use anyhow::{Context, Result, bail};
use nautilus_lightpool::{
    common::currency::collateral_currency_code,
    http::{
        clob_index::ClobIndexHttpClient,
        models::{BalanceTokenSpec, Market},
    },
};
use rust_decimal::Decimal;

async fn resolve_market(client: &ClobIndexHttpClient) -> Result<Market> {
    if let Ok(slug) = env::var("LIGHTPOOL_MARKET_SLUG") {
        let slug = slug.trim();
        return client
            .get_market_by_slug(slug)
            .await
            .with_context(|| format!("market not found for LIGHTPOOL_MARKET_SLUG={slug}"));
    }

    let markets = client.fetch_all_markets().await?;
    markets
        .into_iter()
        .next()
        .context("no markets returned from clob-index; set LIGHTPOOL_MARKET_SLUG")
}

fn market_balance_specs(market: &Market) -> Vec<BalanceTokenSpec> {
    vec![
        BalanceTokenSpec {
            symbol: collateral_currency_code(),
            address: market.collateral_token.clone(),
        },
        BalanceTokenSpec {
            symbol: "YES".into(),
            address: market.yes_token.clone(),
        },
        BalanceTokenSpec {
            symbol: "NO".into(),
            address: market.no_token.clone(),
        },
    ]
}

async fn log_balances(client: &ClobIndexHttpClient, user: &str, market: &Market, label: &str) {
    match client
        .get_balances(user, &market_balance_specs(market))
        .await
    {
        Ok(balances) => {
            log::info!("{label}: {} entries", balances.len());
            for entry in balances {
                log::info!(
                    "  {} total={} locked={} available={}",
                    entry.symbol,
                    entry.total,
                    entry.locked,
                    entry.available
                );
            }
        }
        Err(error) => log::info!("{label}: failed to fetch balances: {error:#}"),
    }
}

async fn resolve_merge_amount(
    client: &ClobIndexHttpClient,
    user: &str,
    market: &Market,
) -> Result<Decimal> {
    let balances = client
        .get_balances(user, &market_balance_specs(market))
        .await?;
    let yes = balances
        .iter()
        .find(|entry| entry.symbol.eq_ignore_ascii_case("YES"))
        .map(|entry| Decimal::from_str(entry.available.trim()))
        .transpose()
        .context("parse YES available")?
        .unwrap_or(Decimal::ZERO);
    let no = balances
        .iter()
        .find(|entry| entry.symbol.eq_ignore_ascii_case("NO"))
        .map(|entry| Decimal::from_str(entry.available.trim()))
        .transpose()
        .context("parse NO available")?
        .unwrap_or(Decimal::ZERO);
    let amount = yes.min(no);
    if amount <= Decimal::ZERO {
        bail!("no YES/NO complete-set balance available to merge");
    }
    Ok(amount)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let op = env::var("LIGHTPOOL_OP").unwrap_or_else(|_| "split".to_string());
    let amount = match env::var("LIGHTPOOL_OUTCOME_AMOUNT") {
        Ok(value) => Some(Decimal::from_str(value.trim())?),
        Err(_) => None,
    };

    let client = ClobIndexHttpClient::from_env().context(
        "Set LIGHTPOOL_PRIVATE_KEY or create ~/.lightpool/wallet.json",
    )?;
    let wallet = client.get_user_address()?;
    log::info!("Wallet: {wallet}");

    let market = resolve_market(&client).await?;
    log::info!(
        "LightPool userOutcome smoke: op={op} slug={} market={} amount={amount:?}",
        market.slug,
        market.market_address
    );

    log_balances(&client, &wallet, &market, "Pre-op balances").await;

    let digest = match op.as_str() {
        "split" | "mint" => {
            let amount = amount.context(
                "LIGHTPOOL_OUTCOME_AMOUNT is required for split/mint",
            )?;
            log::info!("Submitting mint (splitOutcome equivalent)...");
            client.submit_mint_outcome(&market, amount).await?
        }
        "merge" | "burn" => {
            let amount = match amount {
                Some(value) => value,
                None => resolve_merge_amount(&client, &wallet, &market).await?,
            };
            log::info!("Submitting burn amount={amount} (mergeOutcome equivalent)...");
            client.submit_burn_outcome(&market, amount).await?
        }
        other => bail!("Unknown LIGHTPOOL_OP: {other} (use split|merge|mint|burn)"),
    };
    log::info!("{op} digest={digest}");

    log_balances(&client, &wallet, &market, "Post-op balances").await;
    log::info!("Done");
    Ok(())
}

// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! LightPool HTTP private API smoke tool (clob-index).
//!
//! Mirrors Hyperliquid `bin/http_private.rs`:
//! resolve wallet → list user orders → query one order.
//!
//! Prerequisites:
//! - `lightpool-clob-index` running
//! - `LIGHTPOOL_PRIVATE_KEY` or `~/.lightpool/wallet.json`
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-http-private
//! ```

use anyhow::Result;
use nautilus_lightpool::{
    common::signer::signer_from_private_key,
    config::{clob_index_http_from_env, resolve_private_key},
    http::clob_index::ClobIndexHttpClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let http_url = clob_index_http_from_env();
    log::info!("Starting LightPool HTTP private example");
    log::info!("HTTP={http_url}");

    let private_key = match resolve_private_key() {
        Ok(key) => key,
        Err(e) => {
            log::warn!(
                "No credentials found (LIGHTPOOL_PRIVATE_KEY / ~/.lightpool/wallet.json): {e:#}, \
                 skipping private examples"
            );
            return Ok(());
        }
    };
    let signer = signer_from_private_key(&private_key)?;
    let user_address = signer.address().to_string();
    log::info!("Wallet address: {user_address}");

    let client = ClobIndexHttpClient::new(&http_url);

    let orders = match client.list_orders(&user_address).await {
        Ok(orders) => {
            log::info!("Fetched {} orders", orders.len());
            for (i, order) in orders.iter().take(3).enumerate() {
                log::info!(
                    "Order {i}: id={} chain_order_id={} spot={} {} {} @ {} \
                     status={} size_raw={} filled_raw={} slug={}",
                    order.id,
                    order.chain_order_id,
                    order.spot_market,
                    order.side,
                    order.size,
                    order.price,
                    order.status,
                    order.size_raw,
                    order.filled_raw,
                    order.market_slug
                );
            }
            orders
        }
        Err(e) => {
            log::info!("Failed to list orders: {e:#}");
            Vec::new()
        }
    };

    if let Some(order) = orders.first() {
        if order.chain_order_id.is_empty() || order.spot_market.is_empty() {
            log::info!(
                "Skip query_order: first order missing chain_order_id/spot_market (id={})",
                order.id
            );
        } else {
            match client
                .query_order(
                    &order.spot_market,
                    &order.chain_order_id,
                    Some(&user_address),
                )
                .await
            {
                Ok(Some(queried)) => {
                    log::info!(
                        "Order status: id={} chain_order_id={} status={} size_raw={} filled_raw={}",
                        queried.order.id,
                        queried.chain_order_id,
                        queried.order.status,
                        queried.size_raw,
                        queried.filled_raw
                    );
                }
                Ok(None) => log::info!(
                    "Order status: not found for chain_order_id={}",
                    order.chain_order_id
                ),
                Err(e) => log::info!("Order status query failed: {e:#}"),
            }
        }
    } else {
        log::info!("No orders to query status for");
    }

    Ok(())
}

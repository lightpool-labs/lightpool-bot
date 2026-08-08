// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! LightPool WebSocket user-channel tool.
//!
//! Mirrors Hyperliquid user WS smoke:
//! resolve wallet → `connect` → `subscribe_user` → `next_event`.
//!
//! Address comes from `ClobIndexWsClient::get_user_address()`
//! (`LIGHTPOOL_PRIVATE_KEY` / `~/.lightpool/wallet.json`), or optional
//! `LIGHTPOOL_USER_ADDRESS` override.
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --bin lightpool-ws-user
//! ```

use std::env;

use anyhow::{Context, Result};
use nautilus_lightpool::websocket::clob_index::{ClobIndexWsClient, ClobIndexWsMessage};
use tokio::{pin, signal};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let mut client = ClobIndexWsClient::from_env();
    let user_address = if let Ok(address) = env::var("LIGHTPOOL_USER_ADDRESS") {
        let trimmed = address.trim().to_string();
        if trimmed.is_empty() {
            anyhow::bail!("LIGHTPOOL_USER_ADDRESS must be non-empty");
        }
        trimmed
    } else {
        client.get_user_address().context(
            "Set LIGHTPOOL_PRIVATE_KEY / ~/.lightpool/wallet.json, or LIGHTPOOL_USER_ADDRESS",
        )?
    };

    log::info!("Starting LightPool WebSocket user example");
    log::info!("Subscribing channel=user user_address={user_address}");

    client.connect().await.context("websocket connect failed")?;
    client
        .subscribe_user(&user_address)
        .await
        .context("subscribe_user failed")?;

    let sigint = signal::ctrl_c();
    pin!(sigint);

    let mut message_count = 0u64;

    loop {
        tokio::select! {
            maybe_message = client.next_event() => {
                match maybe_message {
                    Some(ClobIndexWsMessage::Error(error)) => {
                        log::error!("WS error: {error}");
                    }
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

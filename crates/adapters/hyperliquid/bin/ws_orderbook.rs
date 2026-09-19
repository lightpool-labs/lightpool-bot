// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{env, time::Instant};

use nautilus_hyperliquid::{
    common::{consts::ws_url, enums::HyperliquidEnvironment},
    http::HyperliquidHttpClient,
    websocket::{client::HyperliquidWebSocketClient, messages::NautilusWsMessage},
};
use nautilus_model::instruments::{Instrument, InstrumentAny};
use nautilus_network::websocket::TransportBackend;
use tokio::{pin, signal};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    nautilus_common::logging::ensure_logging_initialized();

    let args: Vec<String> = env::args().collect();
    let environment = if args.get(1).is_some_and(|s| s == "testnet") {
        HyperliquidEnvironment::Testnet
    } else {
        HyperliquidEnvironment::Mainnet
    };

    log::info!("Starting Hyperliquid WebSocket order book example");
    log::info!("Environment: {environment:?}");

    let http_client = HyperliquidHttpClient::new(environment, 60, None)?;
    let instruments = http_client.request_instruments().await?;
    log::info!("Loaded {} instruments", instruments.len());

    let aapl = instruments
        .iter()
        .find(|instrument| instrument.raw_symbol().as_str() == "xyz:AAPL")
        .ok_or("xyz:AAPL instrument not found")?;
    let instrument_id = match aapl {
        InstrumentAny::CryptoPerpetual(inst) => inst.id,
        _ => return Err("Expected CryptoPerpetual instrument for xyz:AAPL".into()),
    };
    log::info!("Using instrument: {instrument_id}");

    let ws_url = ws_url(environment);
    log::info!("WebSocket URL: {ws_url}");

    let mut client = HyperliquidWebSocketClient::new(
        Some(ws_url.to_string()),
        environment,
        None,
        TransportBackend::default(),
        None,
    );
    client.cache_instruments(instruments);
    client.connect().await?;
    log::info!("Connected to Hyperliquid WebSocket");

    log::info!("Subscribing to L2 book for {instrument_id}");
    client.subscribe_book(instrument_id).await?;

    let sigint = signal::ctrl_c();
    pin!(sigint);

    let mut book_count = 0u64;
    let mut last_recv: Option<Instant> = None;

    loop {
        tokio::select! {
            Some(message) = client.next_event() => {
                let NautilusWsMessage::Deltas(deltas) = message else {
                    continue;
                };
                if deltas.instrument_id != instrument_id {
                    continue;
                }
                book_count += 1;
                let now = Instant::now();
                if let Some(prev) = last_recv {
                    let gap = now.saturating_duration_since(prev);
                    log::info!(
                        "book #{book_count} gap={} ms ({} us) levels={}",
                        gap.as_millis(),
                        gap.as_micros(),
                        deltas.deltas.len(),
                    );
                } else {
                    log::info!(
                        "book #{book_count} first receive levels={}",
                        deltas.deltas.len(),
                    );
                }
                last_recv = Some(now);
            }
            _ = &mut sigint => {
                log::info!("Received SIGINT, closing connection...");
                client.disconnect().await?;
                break;
            }
            else => break,
        }
    }

    log::info!("Received {book_count} order book updates");
    Ok(())
}

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

//! Live runner for the dual-venue liquidity maker strategy.
//!
//! Subscribes to Polymarket and LightPool order book deltas and logs managed cache books.
//!
//! # Usage
//!
//! ```sh
//! cargo run -p lightpool-strategies --bin liquidity-maker -- \
//!   --polymarket-slug world-cup-winner \
//!   --lightpool-slug france-world-cup-2026 \
//!   --log-interval 50
//! ```

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use lightpool_strategies::{LiquidityMaker, LiquidityMakerConfig};
use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_lightpool::{
    config::{LightpoolDataClientConfig, LightpoolExecClientConfig, resolve_private_key},
    factories::{LightpoolDataClientFactory, LightpoolExecutionClientFactory},
};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{StrategyId, TraderId};
use nautilus_polymarket::{
    config::{PolymarketDataClientConfig, PolymarketInstrumentProviderConfig, proxy_url_from_env},
    factories::PolymarketDataClientFactory,
    filters::{EventQueryFilter, InstrumentFilter},
    http::query::GetGammaMarketsParams,
};

#[derive(Parser, Debug)]
#[command(
    about = "Dual-venue liquidity maker: Polymarket + LightPool order book delta logging."
)]
struct Args {
    /// Polymarket event slug (Gamma event slug).
    #[arg(long)]
    polymarket_slug: String,
    /// LightPool market slug (clob-index market slug).
    #[arg(long)]
    lightpool_slug: Option<String>,
    /// Number of book levels to track per side.
    #[arg(long, default_value_t = 10)]
    depth: usize,
    /// Log a book snapshot every N delta batches. `0` disables periodic logs.
    #[arg(long, default_value_t = 50)]
    log_interval: u64,
    /// Disable LightPool data client and subscriptions.
    #[arg(long, default_value_t = false)]
    polymarket_only: bool,
    /// Disable Polymarket order book log output (still subscribes for reference data).
    #[arg(long, default_value_t = false)]
    no_polymarket_log: bool,
    /// Disable LightPool order mirroring (data/logging only).
    #[arg(long, default_value_t = false)]
    no_trading: bool,
}

fn require_non_empty(name: &str, value: &str) -> Result<String> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        bail!("{name} must be non-empty");
    }
    Ok(trimmed)
}

const DEFAULT_PROXY: &str = "http://127.0.0.1:8118";

fn proxy_url() -> Option<String> {
    proxy_url_from_env().or_else(|| Some(DEFAULT_PROXY.to_string()))
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    let polymarket_slug = require_non_empty("--polymarket-slug", &args.polymarket_slug)?;
    let polymarket_slugs = vec![polymarket_slug.clone()];
    let lightpool_enabled = !args.polymarket_only;
    let lightpool_slug = if lightpool_enabled {
        let slug = args
            .lightpool_slug
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--lightpool-slug is required unless --polymarket-only"))?;
        Some(require_non_empty("--lightpool-slug", slug)?)
    } else {
        None
    };
    let lightpool_slugs = lightpool_slug.iter().cloned().collect::<Vec<_>>();

    let proxy_url = proxy_url();

    let environment = Environment::Live;
    let trader_id = TraderId::from("LIQUIDITY-MAKER-001");
    let strategy_id = StrategyId::from("LIQUIDITY_MAKER-001");

    let params = GetGammaMarketsParams {
        active: Some(true),
        closed: Some(false),
        archived: Some(false),
        ..Default::default()
    };

    let filters: Vec<Arc<dyn InstrumentFilter>> = vec![Arc::new(EventQueryFilter::from_queries(
        polymarket_slugs
            .iter()
            .map(|s| (s.clone(), params.clone()))
            .collect(),
    ))];

    let polymarket_data_config = PolymarketDataClientConfig {
        instrument_config: Some(PolymarketInstrumentProviderConfig {
            market_slugs: Some(polymarket_slugs.clone()),
            ..Default::default()
        }),
        proxy_url,
        filters,
        ..Default::default()
    };

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };

    let mut node_builder = LiveNode::builder(trader_id, environment)?
        .with_name("LIQUIDITY-MAKER".to_string())
        .with_logging(log_config)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(PolymarketDataClientFactory),
            Box::new(polymarket_data_config),
        )
        .context("failed to register Polymarket data client")?;

    let trading_enabled = lightpool_enabled && !args.no_trading;

    if lightpool_enabled {
        let lightpool_data_config = LightpoolDataClientConfig::new(lightpool_slugs.clone())
            .with_book_depth(u32::try_from(args.depth).unwrap_or(10));
        log::info!(
            "LightPool data via clob-index http={} ws={}",
            lightpool_data_config.clob_index_http_url,
            lightpool_data_config.clob_index_ws_url,
        );
        node_builder = node_builder
            .add_data_client(
                None,
                Box::new(LightpoolDataClientFactory),
                Box::new(lightpool_data_config),
            )
            .context("failed to register LightPool data client")?;
    }

    if trading_enabled {
        let private_key = resolve_private_key()
            .context("failed to load LightPool private key for execution")?;
        let lightpool_exec_config = LightpoolExecClientConfig {
            private_key: Some(private_key),
            ..Default::default()
        };
        log::info!(
            "LightPool execution via clob-index http={}",
            lightpool_exec_config.clob_index_http_url,
        );
        node_builder = node_builder
            .add_exec_client(
                None,
                Box::new(LightpoolExecutionClientFactory),
                Box::new(lightpool_exec_config),
            )
            .context("failed to register LightPool execution client")?;
    }

    let mut node = node_builder.build()?;

    let strategy_config = LiquidityMakerConfig::new(polymarket_slugs)
        .with_lightpool_slugs(lightpool_slugs)
        .with_depth(args.depth)
        .with_log_interval(args.log_interval)
        .with_lightpool_enabled(lightpool_enabled)
        .with_log_polymarket(!args.no_polymarket_log)
        .with_trading_enabled(trading_enabled)
        .with_strategy_id(strategy_id);

    log::info!(
        "Starting liquidity maker polymarket_slug={polymarket_slug} \
         lightpool_slug={} depth={} lightpool_enabled={lightpool_enabled} \
         trading_enabled={trading_enabled}",
        lightpool_slug.as_deref().unwrap_or("-"),
        args.depth,
    );

    let strategy = LiquidityMaker::new(strategy_config);
    node.add_strategy(strategy)?;
    node.run().await?;

    Ok(())
}

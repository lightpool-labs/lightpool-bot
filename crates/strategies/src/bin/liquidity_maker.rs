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

//! Live runner for the Polymarket liquidity maker strategy.
//!
//! Subscribes to Polymarket order book deltas and reads managed books from cache.
//!
//! # Usage
//!
//! ```sh
//! cargo run -p lightpool-strategies --bin liquidity-maker -- \
//!   --slug gta-vi-released-before-june-2026
//! ```

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use lightpool_strategies::{LiquidityMaker, LiquidityMakerConfig};
use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{StrategyId, TraderId};
use nautilus_polymarket::{
    config::{PolymarketDataClientConfig, PolymarketInstrumentProviderConfig, proxy_url_from_env},
    factories::PolymarketDataClientFactory,
    filters::{EventQueryFilter, InstrumentFilter},
    http::query::GetGammaMarketsParams,
};

#[derive(Parser, Debug)]
#[command(about = "Polymarket liquidity maker: subscribe order book deltas and read cache books.")]
struct Args {
    /// Polymarket event slug (Gamma event slug).
    #[arg(long)]
    slug: String,
    /// Number of book levels to track per side.
    #[arg(long, default_value_t = 10)]
    depth: usize,
    /// Log a book snapshot every N delta batches. `0` disables periodic logs.
    #[arg(long, default_value_t = 50)]
    log_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    let slug = args.slug.trim().to_string();
    if slug.is_empty() {
        bail!("--slug must be non-empty");
    }
    let slugs = vec![slug.clone()];
    let proxy_url = proxy_url_from_env();

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
        slugs
            .iter()
            .map(|s| (s.clone(), params.clone()))
            .collect(),
    ))];

    let data_config = PolymarketDataClientConfig {
        instrument_config: Some(PolymarketInstrumentProviderConfig {
            event_slugs: Some(slugs.clone()),
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

    let mut node = LiveNode::builder(trader_id, environment)?
        .with_name("LIQUIDITY-MAKER".to_string())
        .with_logging(log_config)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(PolymarketDataClientFactory),
            Box::new(data_config),
        )
        .context("failed to build live node")?
        .build()?;

    let strategy_config = LiquidityMakerConfig::new(slugs)
        .with_depth(args.depth)
        .with_log_interval(args.log_interval)
        .with_strategy_id(strategy_id);

    log::info!(
        "Starting liquidity maker slug={slug} depth={}",
        args.depth,
    );

    let strategy = LiquidityMaker::new(strategy_config);
    node.add_strategy(strategy)?;
    node.run().await?;

    Ok(())
}

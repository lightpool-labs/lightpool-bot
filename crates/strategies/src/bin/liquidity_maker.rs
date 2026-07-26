// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Live runner for the dual-venue liquidity maker strategy.
//!
//! Subscribes to Polymarket and LightPool order book deltas and mirrors depth onto LightPool.
//!
//! # Usage
//!
//! Bootstrap top-5 Polymarket markets into LightPool, then mirror:
//! ```sh
//! cargo run -p lightpool-strategies --bin liquidity-maker -- \
//!   --polymarket-slug world-cup-winner \
//!   --bootstrap-markets \
//!   --max-markets 5
//! ```
//!
//! Or use an existing LightPool market slug:
//! ```sh
//! cargo run -p lightpool-strategies --bin liquidity-maker -- \
//!   --polymarket-slug world-cup-winner \
//!   --lightpool-slug france-world-cup-2026
//! ```

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use lightpool_strategies::{
    BootstrapConfig, LiquidityMaker, LiquidityMakerConfig, MarketPair,
    bootstrap_markets_from_polymarket,
};
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
    about = "Dual-venue liquidity maker: Polymarket + LightPool order book mirroring."
)]
struct Args {
    /// Polymarket event slug (Gamma event slug).
    #[arg(long)]
    polymarket_slug: String,
    /// LightPool market slug (clob-index). Required unless --bootstrap-markets or --polymarket-only.
    #[arg(long)]
    lightpool_slug: Option<String>,
    /// Create+mint top-N LightPool markets from Polymarket before starting the node.
    #[arg(long, default_value_t = false)]
    bootstrap_markets: bool,
    /// Max Polymarket markets to bootstrap / subscribe (by liquidity).
    #[arg(long, default_value_t = 5)]
    max_markets: u32,
    /// Collateral amount to mint per market (raw units, 6 decimals). Default 1e9 tokens.
    #[arg(long, default_value_t = 1_000_000_000_000_000)]
    mint_amount: u64,
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
    /// Number of Polymarket book deltas to batch before reconciling once.
    #[arg(long, default_value_t = 10)]
    reconcile_delta_batch_size: u64,
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

    if args.bootstrap_markets && args.polymarket_only {
        bail!("--bootstrap-markets cannot be used with --polymarket-only");
    }
    if args.bootstrap_markets && args.lightpool_slug.is_some() {
        log::warn!("--lightpool-slug ignored when --bootstrap-markets is set");
    }

    let market_pairs: Vec<MarketPair> = if args.bootstrap_markets {
        let boot_cfg = BootstrapConfig {
            polymarket_event_slug: polymarket_slug.clone(),
            max_markets: args.max_markets.max(1),
            mint_amount: args.mint_amount,
            order_field: "liquidity".into(),
        };
        bootstrap_markets_from_polymarket(&boot_cfg)
            .await
            .context("bootstrap LightPool markets from Polymarket")?
    } else {
        Vec::new()
    };

    let lightpool_slugs: Vec<String> = if !market_pairs.is_empty() {
        market_pairs
            .iter()
            .map(|p| p.lightpool_slug.clone())
            .collect()
    } else if lightpool_enabled {
        let slug = args.lightpool_slug.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "--lightpool-slug is required unless --bootstrap-markets or --polymarket-only"
            )
        })?;
        vec![require_non_empty("--lightpool-slug", slug)?]
    } else {
        Vec::new()
    };

    let proxy_url = proxy_url();

    let environment = Environment::Live;
    let trader_id = TraderId::from("LIQUIDITY-MAKER-001");
    let strategy_id = StrategyId::from("LIQUIDITY_MAKER-001");

    let params = GetGammaMarketsParams {
        active: Some(true),
        closed: Some(false),
        archived: Some(false),
        order: Some("liquidity".into()),
        ascending: Some(false),
        max_markets: Some(args.max_markets.max(1)),
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
            event_slugs: Some(polymarket_slugs.clone()),
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
            market_slugs: lightpool_slugs.clone(),
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

    let mut strategy_config = LiquidityMakerConfig::new(polymarket_slugs)
        .with_depth(args.depth)
        .with_log_interval(args.log_interval)
        .with_log_polymarket(!args.no_polymarket_log)
        .with_trading_enabled(trading_enabled)
        .with_reconcile_delta_batch_size(args.reconcile_delta_batch_size)
        .with_strategy_id(strategy_id);

    if !market_pairs.is_empty() {
        for pair in &market_pairs {
            log::info!(
                "market pair condition={} -> slug={} ({})",
                pair.condition_id,
                pair.lightpool_slug,
                pair.question
            );
        }
        strategy_config = strategy_config.with_market_pairs(market_pairs);
    } else {
        strategy_config = strategy_config.with_lightpool_slugs(lightpool_slugs.clone());
    }

    log::info!(
        "Starting liquidity maker polymarket_slug={polymarket_slug} \
         lightpool_slugs={lightpool_slugs:?} depth={} trading_enabled={trading_enabled} \
         bootstrap={}",
        args.depth,
        args.bootstrap_markets,
    );

    let strategy = LiquidityMaker::new(strategy_config);
    node.add_strategy(strategy)?;
    node.run().await?;

    Ok(())
}

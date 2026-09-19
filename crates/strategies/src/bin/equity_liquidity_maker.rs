// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Live runner for the equity liquidity maker (Hyperliquid → LightPool spot).
//!
//! # Usage
//!
//! Resolve spot markets from the tokenized-stocks registry:
//! ```sh
//! cargo run -p lightpool-strategies --bin equity-liquidity-maker -- \
//!   --symbol AAPL,TSLA,INTC
//! ```
//!
//! Or pass spot ContractAddresses in the same order as symbols:
//! ```sh
//! cargo run -p lightpool-strategies --bin equity-liquidity-maker -- \
//!   --symbol AAPL,TSLA,INTC \
//!   --spot-market 0x03...,0x03...,0x03...
//! ```
//!
//! Maker inventory: Anvil #0 / `LIGHTPOOL_PRIVATE_KEY` wallet must hold USDT (quote)
//! and each stock base token. Hyperliquid coin is derived as `xyz:{SYMBOL}` (no mapping flag).

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;
use lightpool_strategies::{
    EquityLiquidityMaker, EquityLiquidityMakerConfig, EquityMarketPair,
};
use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_hyperliquid::{
    HyperliquidDataClientConfig, HyperliquidDataClientFactory,
    common::enums::HyperliquidEnvironment,
};
use nautilus_lightpool::{
    config::{
        LightpoolDataClientConfig, LightpoolExecClientConfig, SpotMarketBootstrap,
        resolve_private_key,
    },
    factories::{LightpoolDataClientFactory, LightpoolExecutionClientFactory},
};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{StrategyId, TraderId};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    about = "Equity liquidity maker: Hyperliquid xyz:SYMBOL L2 → LightPool spot mirroring.\n\
             Maker wallet must hold USDT (bids) and each stock base token (asks)."
)]
struct Args {
    /// Equity tickers (comma-separated), e.g. AAPL,TSLA,INTC. HL coin = xyz:{SYMBOL}.
    #[arg(long)]
    symbol: String,
    /// LightPool spot ContractAddresses aligned with --symbol (optional if registry resolves).
    #[arg(long)]
    spot_market: Option<String>,
    /// Number of book levels to track per side.
    #[arg(long, default_value_t = 10)]
    depth: usize,
    /// Number of Hyperliquid book deltas to batch before reconciling once.
    #[arg(long, default_value_t = 1)]
    reconcile_delta_batch_size: u64,
    /// Log a Hyperliquid book snapshot every N delta batches. `0` disables periodic logs.
    #[arg(long, default_value_t = 50)]
    log_interval: u64,
}

#[derive(Debug, Deserialize)]
struct RegistryFile {
    #[serde(default)]
    markets: Vec<RegistryMarket>,
}

#[derive(Debug, Deserialize)]
struct RegistryMarket {
    symbol: String,
    base_token: String,
    quote_token: String,
    spot_market: String,
}

fn parse_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn registry_candidates() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(env_path) = std::env::var("TOKENIZED_STOCKS_REGISTRY") {
        let trimmed = env_path.trim();
        if !trimmed.is_empty() {
            paths.push(PathBuf::from(trimmed));
        }
    }
    // Course data dir ($LABS/data/tokenized-stocks), resolved from common cwd layouts.
    paths.push(PathBuf::from("../data/tokenized-stocks/registry.json"));
    paths.push(PathBuf::from("../../data/tokenized-stocks/registry.json"));
    paths.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../data/tokenized-stocks/registry.json"),
    );
    paths
}

fn load_registry() -> Option<(PathBuf, RegistryFile)> {
    for path in registry_candidates() {
        if !path.exists() {
            continue;
        }
        match std::fs::read_to_string(&path) {
            Ok(raw) => match serde_json::from_str::<RegistryFile>(&raw) {
                Ok(registry) => return Some((path, registry)),
                Err(e) => {
                    log::warn!("Failed to parse registry {}: {e:#}", path.display());
                }
            },
            Err(e) => {
                log::warn!("Failed to read registry {}: {e:#}", path.display());
            }
        }
    }
    None
}

fn find_registry_market<'a>(
    registry: &'a RegistryFile,
    symbol: &str,
) -> Option<&'a RegistryMarket> {
    registry
        .markets
        .iter()
        .find(|m| m.symbol.eq_ignore_ascii_case(symbol))
}

fn resolve_markets(
    symbols: &[String],
    spot_markets_cli: Option<Vec<String>>,
) -> Result<Vec<EquityMarketPair>> {
    let registry = load_registry();
    if let Some((path, _)) = &registry {
        log::info!("Loaded tokenized-stocks registry from {}", path.display());
    }

    if let Some(spots) = &spot_markets_cli {
        if spots.len() != symbols.len() {
            bail!(
                "--spot-market count ({}) must match --symbol count ({})",
                spots.len(),
                symbols.len()
            );
        }
    }

    let mut pairs = Vec::with_capacity(symbols.len());
    for (idx, symbol) in symbols.iter().enumerate() {
        let registry_market = registry
            .as_ref()
            .and_then(|(_, reg)| find_registry_market(reg, symbol));

        let spot_market = spot_markets_cli
            .as_ref()
            .map(|spots| spots[idx].clone())
            .or_else(|| registry_market.map(|m| m.spot_market.clone()))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "missing spot_market for {symbol}: pass --spot-market or provide registry \
                     (TOKENIZED_STOCKS_REGISTRY / $LABS/data/tokenized-stocks/registry.json)"
                )
            })?;

        let (base_token, quote_token) = match registry_market {
            Some(m) => (m.base_token.clone(), m.quote_token.clone()),
            None => bail!(
                "missing base_token/quote_token for {symbol}: registry entry required \
                 (TOKENIZED_STOCKS_REGISTRY or $LABS/data/tokenized-stocks/registry.json)"
            ),
        };

        let hl_coin = format!("xyz:{symbol}");
        pairs.push(EquityMarketPair {
            symbol: symbol.clone(),
            hl_coin,
            spot: SpotMarketBootstrap {
                symbol: symbol.clone(),
                spot_market,
                base_token,
                quote_token,
            },
        });
    }

    Ok(pairs)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    let symbols: Vec<String> = parse_csv_list(&args.symbol)
        .into_iter()
        .map(|s| s.to_ascii_uppercase())
        .collect();
    if symbols.is_empty() {
        bail!("--symbol must list at least one equity ticker (e.g. AAPL,TSLA,INTC)");
    }

    let spot_markets_cli = args
        .spot_market
        .as_ref()
        .map(|raw| parse_csv_list(raw))
        .filter(|v| !v.is_empty());

    let markets = resolve_markets(&symbols, spot_markets_cli)?;
    for market in &markets {
        log::info!(
            "equity maker market symbol={} hl_coin={} spot={} base={} quote={}",
            market.symbol,
            market.hl_coin,
            market.spot.spot_market,
            market.spot.base_token,
            market.spot.quote_token,
        );
    }

    let spot_bootstraps: Vec<SpotMarketBootstrap> =
        markets.iter().map(|m| m.spot.clone()).collect();

    let environment = Environment::Live;
    let trader_id = TraderId::from("EQUITY-LIQUIDITY-MAKER-001");
    let strategy_id = StrategyId::from("EQUITY_LIQUIDITY_MAKER-001");

    let hyperliquid_data_config = HyperliquidDataClientConfig {
        environment: HyperliquidEnvironment::Mainnet,
        ..Default::default()
    };

    let lightpool_data_config = LightpoolDataClientConfig::default()
        .with_spot_markets(spot_bootstraps.clone())
        .with_book_depth(u32::try_from(args.depth).unwrap_or(10));

    let private_key =
        resolve_private_key().context("failed to load LightPool private key for execution")?;
    let lightpool_exec_config = LightpoolExecClientConfig {
        private_key: Some(private_key),
        spot_markets: spot_bootstraps,
        ..Default::default()
    };

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };

    log::info!(
        "LightPool data via clob-index http={} ws={}",
        lightpool_data_config.clob_index_http_url,
        lightpool_data_config.clob_index_ws_url,
    );
    log::info!(
        "LightPool execution via clob-index http={}",
        lightpool_exec_config.clob_index_http_url,
    );

    let mut node = LiveNode::builder(trader_id, environment)?
        .with_name("EQUITY-LIQUIDITY-MAKER".to_string())
        .with_logging(log_config)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(HyperliquidDataClientFactory::new()),
            Box::new(hyperliquid_data_config),
        )
        .context("failed to register Hyperliquid data client")?
        .add_data_client(
            None,
            Box::new(LightpoolDataClientFactory),
            Box::new(lightpool_data_config),
        )
        .context("failed to register LightPool data client")?
        .add_exec_client(
            None,
            Box::new(LightpoolExecutionClientFactory),
            Box::new(lightpool_exec_config),
        )
        .context("failed to register LightPool execution client")?
        .build()?;

    let strategy_config = EquityLiquidityMakerConfig::new(markets)
        .with_depth(args.depth)
        .with_log_interval(args.log_interval)
        .with_reconcile_delta_batch_size(args.reconcile_delta_batch_size)
        .with_strategy_id(strategy_id);

    log::info!(
        "Starting equity liquidity maker symbols={symbols:?} depth={} \
         reconcile_delta_batch_size={}",
        args.depth,
        args.reconcile_delta_batch_size,
    );

    let strategy = EquityLiquidityMaker::new(strategy_config);
    node.add_strategy(strategy)?;
    node.run().await?;

    Ok(())
}

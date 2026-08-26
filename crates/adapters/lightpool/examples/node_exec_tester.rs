// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Live execution smoke test for the LightPool adapter.
//!
//! Boots a `LiveNode` with LightPool data + execution clients and runs
//! [`ExecTester`] against one YES/NO instrument resolved from a market slug.
//!
//! Prerequisites:
//! - `lightpool-clob-indexer` running (HTTP/WS)
//! - Signing key via `LIGHTPOOL_PRIVATE_KEY` or `~/.lightpool/wallet.json`
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --example lightpool-exec-tester --features examples -- \
//!   --slug your-market-slug \
//!   --outcome yes \
//!   --order-qty 1
//! ```

use anyhow::{Context, Result, bail};
use clap::Parser;
use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_lightpool::{
    common::consts::LIGHTPOOL_CLIENT_ID,
    config::{LightpoolDataClientConfig, LightpoolExecClientConfig},
    factories::{LightpoolDataClientFactory, LightpoolExecutionClientFactory},
};
use nautilus_live::{config::LiveExecEngineConfig, node::LiveNode};
use nautilus_model::{
    identifiers::{InstrumentId, StrategyId, TraderId},
    types::Quantity,
};
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use nautilus_trading::strategy::StrategyConfig;

#[derive(Parser, Debug)]
#[command(about = "LightPool exec tester: place limit orders via clob-index")]
struct Args {
    /// Market slug configured in clob-index.
    #[arg(long)]
    slug: String,
    /// Outcome leg: yes or no.
    #[arg(long, default_value = "yes")]
    outcome: String,
    /// Book depth per side for data subscriptions.
    #[arg(long, default_value_t = 10)]
    depth: u32,
    /// Limit order quantity (token amount, e.g. 1 = 1 share).
    #[arg(long, default_value = "1")]
    order_qty: String,
    /// Offset from TOB in price ticks for limit orders.
    #[arg(long, default_value_t = 5)]
    tob_offset_ticks: u64,
    /// Dry run: wire the node but do not submit orders.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    let slug = args.slug.trim().to_string();
    if slug.is_empty() {
        bail!("--slug must be non-empty");
    }

    let outcome = args.outcome.trim().to_ascii_lowercase();
    let suffix = match outcome.as_str() {
        "yes" => "YES",
        "no" => "NO",
        other => bail!("--outcome must be yes or no, got {other}"),
    };

    let instrument_id = InstrumentId::from(format!("{slug}-{suffix}.LIGHTPOOL"));
    let client_id = *LIGHTPOOL_CLIENT_ID;
    let trader_id = TraderId::from("LIGHTPOOL-EXEC-TESTER-001");
    let node_name = "LIGHTPOOL-EXEC-TESTER-001".to_string();
    let environment = Environment::Live;

    let data_config = LightpoolDataClientConfig::new(vec![slug.clone()]).with_book_depth(args.depth);
    let exec_config = LightpoolExecClientConfig {
        market_slugs: vec![slug],
        ..Default::default()
    };

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };
    let exec_engine_config = LiveExecEngineConfig {
        open_check_interval_secs: Some(10.0),
        position_check_interval_secs: Some(30.0),
        ..Default::default()
    };

    let mut node = LiveNode::builder(trader_id, environment)?
        .with_name(node_name)
        .with_logging(log_config)
        .with_exec_engine_config(exec_engine_config)
        .add_data_client(
            None,
            Box::new(LightpoolDataClientFactory),
            Box::new(data_config),
        )
        .context("failed to add LightPool data client")?
        .add_exec_client(
            None,
            Box::new(LightpoolExecutionClientFactory),
            Box::new(exec_config),
        )
        .context("failed to add LightPool exec client")?
        .with_reconciliation(false)
        .with_delay_post_stop_secs(5)
        .build()?;

    let order_qty = Quantity::from(args.order_qty.as_str());
    let tester_config = ExecTesterConfig::builder()
        .base(StrategyConfig {
            strategy_id: Some(StrategyId::from("EXEC_TESTER-001")),
            external_order_claims: Some(vec![instrument_id]),
            use_hyphens_in_client_order_ids: true,
            ..Default::default()
        })
        .instrument_id(instrument_id)
        .client_id(client_id)
        .order_qty(order_qty)
        .subscribe_book(true)
        .subscribe_quotes(true)
        .subscribe_trades(false)
        .tob_offset_ticks(args.tob_offset_ticks)
        .use_post_only(false)
        .enable_stop_buys(false)
        .enable_stop_sells(false)
        .reduce_only_on_stop(false)
        .dry_run(args.dry_run)
        .log_data(false)
        .build();

    let tester = ExecTester::new(tester_config);
    node.add_strategy(tester)?;
    node.run().await?;

    Ok(())
}

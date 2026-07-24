// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Smoke test for the LightPool live data client.
//!
//! Run with:
//! `cargo run -p nautilus-lightpool --example lightpool-data-tester --features examples -- --slug your-market-slug`

use anyhow::{Context, Result, bail};
use clap::Parser;
use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_lightpool::{
    config::LightpoolDataClientConfig,
    factories::LightpoolDataClientFactory,
};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{ClientId, InstrumentId, TraderId};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};

#[derive(Parser, Debug)]
#[command(about = "LightPool data tester: subscribe order book deltas from clob-index.")]
struct Args {
    /// Market slug configured in clob-index.
    #[arg(long)]
    slug: String,
    /// Book depth per side.
    #[arg(long, default_value_t = 10)]
    depth: u32,
    /// Outcome leg to test: yes or no.
    #[arg(long, default_value = "yes")]
    outcome: String,
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
    let client_id = ClientId::from("LIGHTPOOL");
    let trader_id = TraderId::from("LIGHTPOOL-DATA-TESTER-001");

    let data_config = LightpoolDataClientConfig::new(vec![slug])
        .with_book_depth(args.depth);

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("LIGHTPOOL-DATA-TESTER".to_string())
        .with_logging(log_config)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(LightpoolDataClientFactory),
            Box::new(data_config),
        )
        .context("failed to build live node")?
        .build()?;

    let tester_config = DataTesterConfig::builder()
        .client_id(client_id)
        .instrument_ids(vec![instrument_id])
        .subscribe_book_deltas(true)
        .manage_book(true)
        .build();
    let tester = DataTester::new(tester_config);

    node.add_actor(tester)?;
    node.run().await?;

    Ok(())
}

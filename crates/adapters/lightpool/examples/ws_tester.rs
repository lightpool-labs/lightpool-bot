// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Direct clob-index orderbook WebSocket tester for the LightPool adapter.
//!
//! Resolves a market slug to a spot market via HTTP, prints an initial REST snapshot,
//! then streams `orderbook_delta` / `orderbook_snapshot` events from the WS client
//! used by `LightpoolDataClient`.
//!
//! Run:
//! ```sh
//! cargo run -p nautilus-lightpool --example lightpool-ws-tester -- \
//!   --slug will-france-win-the-2026-fifa-world-cup \
//!   --outcome yes \
//!   --depth 10
//! ```

use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use nautilus_lightpool::{
    config::{clob_index_http_from_env, clob_index_ws_from_env},
    http::{clob_index::ClobIndexHttpClient, models::BookLevel},
    websocket::clob_index::{BookWsEvent, ClobIndexBookWsClient},
};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(about = "LightPool clob-index orderbook WebSocket tester")]
struct Args {
    /// Market slug configured in clob-index.
    #[arg(long)]
    slug: String,
    /// Outcome leg: yes or no.
    #[arg(long, default_value = "yes")]
    outcome: String,
    /// Book depth per side for subscribe and HTTP snapshot.
    #[arg(long, default_value_t = 10)]
    depth: u32,
    /// clob-index HTTP base URL (defaults to LIGHTPOOL_CLOB_INDEX_HTTP or 127.0.0.1:3002).
    #[arg(long)]
    http_url: Option<String>,
    /// clob-index WS base URL (defaults to LIGHTPOOL_CLOB_INDEX_WS or ws://127.0.0.1:3002).
    #[arg(long)]
    ws_url: Option<String>,
    /// Re-fetch REST book every N seconds for comparison (0 = disabled).
    #[arg(long, default_value_t = 0)]
    poll_secs: u64,
    /// Skip the initial REST book snapshot (WebSocket-only mode).
    #[arg(long, default_value_t = false)]
    skip_http: bool,
    /// Print raw JSON text from the WebSocket before parsing.
    #[arg(long, default_value_t = false)]
    raw: bool,
}

fn format_levels(levels: &[BookLevel]) -> String {
    if levels.is_empty() {
        return "[-]".into();
    }
    levels
        .iter()
        .map(|level| {
            let price = cents_to_display(&level.price).unwrap_or_else(|_| "?".into());
            format!("{}@{}", level.size, price)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn cents_to_display(price: &str) -> Result<String> {
    let cents: u64 = price
        .trim()
        .parse()
        .with_context(|| format!("invalid cents price '{price}'"))?;
    let decimal = Decimal::from(cents) / Decimal::from(100);
    Ok(format!("{decimal:.2}"))
}

fn spot_market_for_outcome<'a>(
    market: &'a nautilus_lightpool::http::models::Market,
    outcome: &str,
) -> Result<&'a str> {
    match outcome {
        "yes" => Ok(market.yes_spot_market.as_str()),
        "no" => Ok(market.no_spot_market.as_str()),
        other => bail!("--outcome must be yes or no, got {other}"),
    }
}

async fn print_http_snapshot(
    http: &ClobIndexHttpClient,
    spot_market: &str,
    depth: u32,
    label: &str,
) -> Result<()> {
    let book = http.fetch_book_snapshot(spot_market, depth).await?;
    println!(
        "[HTTP {label}] spot_market={spot_market} sequence={} bids=[{}] asks=[{}] last_trade={:?}",
        book.sequence,
        format_levels(&book.bids),
        format_levels(&book.asks),
        book.last_trade_price,
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    let slug = args.slug.trim();
    if slug.is_empty() {
        bail!("--slug must be non-empty");
    }
    let outcome = args.outcome.trim().to_ascii_lowercase();

    let http_url = args
        .http_url
        .unwrap_or_else(clob_index_http_from_env);
    let ws_url = args.ws_url.unwrap_or_else(clob_index_ws_from_env);

    let http = ClobIndexHttpClient::new(&http_url);
    let market = http
        .get_market_by_slug(slug)
        .await
        .with_context(|| format!("failed to load market slug={slug} from {http_url}"))?;
    let spot_market = spot_market_for_outcome(&market, outcome.as_str())?.to_string();

    println!("Market slug={} question={}", market.slug, market.question);
    println!("Outcome={outcome} spot_market={spot_market}");
    println!("HTTP base={http_url} WS base={ws_url} depth={}", args.depth);

    if args.skip_http {
        println!("Skipping HTTP book snapshot (--skip-http)");
    } else if let Err(e) = print_http_snapshot(&http, &spot_market, args.depth, "initial").await {
        eprintln!("[HTTP initial] failed: {e:#}");
        eprintln!("Continuing with WebSocket subscription...");
    }

    if args.poll_secs > 0 {
        let poll_http = http.clone();
        let poll_spot = spot_market.clone();
        let poll_depth = args.depth;
        let poll_secs = args.poll_secs;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(poll_secs));
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = print_http_snapshot(&poll_http, &poll_spot, poll_depth, "poll").await
                {
                    eprintln!("[HTTP poll] failed: {e:#}");
                }
            }
        });
    }

    let ws = ClobIndexBookWsClient::new(&ws_url);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    ws.subscribe_orderbook(spot_market.clone(), args.depth, tx, cancel.clone())
        .await
        .context("WebSocket subscribe failed")?;

    println!("Subscribed to orderbook_delta; waiting for events (Ctrl+C to stop)...");

    let mut delta_count = 0u64;
    let mut snapshot_count = 0u64;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Stopping...");
                cancel.cancel();
                break;
            }
            event = rx.recv() => {
                match event {
                    Some(BookWsEvent::Snapshot(snapshot)) => {
                        snapshot_count += 1;
                        if args.raw {
                            println!("[WS raw snapshot #{snapshot_count}] {snapshot:?}");
                        }
                        println!(
                            "[WS snapshot #{snapshot_count}] spot_market={} sequence={} bids=[{}] asks=[{}] last_trade={:?}",
                            snapshot.spot_market,
                            snapshot.sequence,
                            format_levels(&snapshot.bids),
                            format_levels(&snapshot.asks),
                            snapshot.last_trade_price,
                        );
                    }
                    Some(BookWsEvent::Delta(delta)) => {
                        delta_count += 1;
                        if args.raw {
                            println!("[WS raw delta #{delta_count}] {delta:?}");
                        }
                        println!(
                            "[WS delta #{delta_count}] spot_market={} sequence={} block_num={} bids=[{}] asks=[{}] last_trade={:?}",
                            delta.spot_market,
                            delta.sequence,
                            delta.block_num,
                            format_levels(&delta.bids),
                            format_levels(&delta.asks),
                            delta.last_trade_price,
                        );
                    }
                    Some(BookWsEvent::Error(error)) => {
                        eprintln!("[WS error] {error}");
                    }
                    None => {
                        eprintln!("WebSocket channel closed");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

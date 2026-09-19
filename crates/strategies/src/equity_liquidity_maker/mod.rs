// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Equity liquidity maker: Hyperliquid `xyz:SYMBOL` L2 → LightPool spot mirroring.
//!
//! # Run
//!
//! ```sh
//! cargo run -p lightpool-strategies --bin equity-liquidity-maker -- \
//!   --symbol AAPL,TSLA,INTC
//! ```
//!
//! Optional `--spot-market` list (same order as symbols). When omitted, spot addresses
//! and tokens are loaded from `$LABS/data/tokenized-stocks/registry.json`
//! (or `TOKENIZED_STOCKS_REGISTRY`).
//!
//! Maker inventory: the signing wallet must hold **USDT (quote)** for bids and each
//! **stock base token** for asks. Fund via Admin before running.

pub mod config;
pub mod strategy;
mod sync;

pub use config::{EquityLiquidityMakerConfig, EquityMarketPair};
pub use strategy::EquityLiquidityMaker;

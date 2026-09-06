// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

pub const LIGHTPOOL: &str = "LIGHTPOOL";

/// LightPool stack release this adapter targets (node / sdk / indexer / bridge).
pub const LIGHTPOOL_STACK_VERSION: &str = "0.5.0";

pub static LIGHTPOOL_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(LIGHTPOOL)));

pub static LIGHTPOOL_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(LIGHTPOOL)));

/// Collateral token symbol used for LightPool spot markets (quote currency).
pub const DEFAULT_COLLATERAL_CURRENCY: &str = "USDT";

/// Default on-chain collateral token when `LIGHTPOOL_COLLATERAL_TOKEN` is unset.
pub const DEFAULT_COLLATERAL_TOKEN: &str = "0x0200000000000001";

/// LightPool spot prices are quoted in USDT probability (0-1).
pub const MIN_PRICE: &str = "0";
pub const MAX_PRICE: &str = "1";
pub const PRICE_TICK: &str = "0.001";

pub const DEFAULT_CLOB_INDEX_HTTP: &str = "http://127.0.0.1:3002";
pub const DEFAULT_CLOB_INDEX_WS: &str = "ws://127.0.0.1:3002";

/// Total HTTP request timeout when calling lightpool-clob-indexer (submit waits for receipt).
pub const DEFAULT_CLOB_INDEX_HTTP_TIMEOUT_SECS: u64 = 90;

/// TCP connect timeout for lightpool-clob-indexer HTTP.
pub const DEFAULT_CLOB_INDEX_HTTP_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Default spot tick size (0.001) in raw token units — matches liquidity_maker bootstrap.
pub const DEFAULT_TICK_SIZE_RAW: u64 = 1_000;

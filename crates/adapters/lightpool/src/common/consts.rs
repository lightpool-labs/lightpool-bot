use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

pub const LIGHTPOOL: &str = "LIGHTPOOL";

pub static LIGHTPOOL_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(LIGHTPOOL)));

pub static LIGHTPOOL_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(LIGHTPOOL)));

pub const LPUSD: &str = "LPUSD";

/// LightPool spot prices are quoted in cents (0-100).
pub const MIN_PRICE: &str = "0";
pub const MAX_PRICE: &str = "1";
pub const PRICE_TICK: &str = "0.01";

pub const DEFAULT_CLOB_INDEX_HTTP: &str = "http://127.0.0.1:3002";
pub const DEFAULT_CLOB_INDEX_WS: &str = "ws://127.0.0.1:3002";
pub const DEFAULT_NODE_RPC: &str = "http://127.0.0.1:9000";

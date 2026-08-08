// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Deserialize;

use crate::http::models::BookLevel;

#[derive(Debug, Clone, Deserialize)]
pub struct OrderBookDelta {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub sequence: u64,
    pub block_num: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    #[serde(default)]
    pub last_trade_price: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderBookWsSnapshot {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub sequence: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    #[serde(default)]
    pub last_trade_price: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WsError {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub error: String,
}

#[derive(Debug, Deserialize)]
pub struct WsSubscribed {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    pub key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuoteSnapshot {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub sequence: u64,
    #[serde(default)]
    pub best_bid: Option<BookLevel>,
    #[serde(default)]
    pub best_ask: Option<BookLevel>,
    #[serde(default)]
    pub last_trade_price: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuoteDelta {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub sequence: u64,
    pub block_num: u64,
    #[serde(default)]
    pub best_bid: Option<BookLevel>,
    #[serde(default)]
    pub best_ask: Option<BookLevel>,
    #[serde(default)]
    pub last_trade_price: Option<String>,
}

/// User order update from clob-index `channel=user` (`type=order`).
///
/// Extra flattened order fields are kept in `extra` for forward compatibility.
#[derive(Debug, Clone, Deserialize)]
pub struct UserOrderMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub event: String,
    pub user_address: String,
    pub chain_order_id: String,
    #[serde(default)]
    pub block_num: Option<u64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// User trade/fill update from clob-index `channel=user` (`type=trade`).
#[derive(Debug, Clone, Deserialize)]
pub struct UserTradeMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub user_address: String,
    pub chain_order_id: String,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub market_slug: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub fill_amount: Option<String>,
    #[serde(default)]
    pub remaining_amount: Option<String>,
    #[serde(default)]
    pub is_fully_filled: Option<bool>,
    #[serde(default)]
    pub spot_market: Option<String>,
    #[serde(default)]
    pub block_num: Option<u64>,
}

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

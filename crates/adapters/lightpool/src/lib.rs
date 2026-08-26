// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 LightPool. All rights reserved.
// -------------------------------------------------------------------------------------------------

//! NautilusTrader adapter for [LightPool](https://github.com/lightpool) prediction markets.
//!
//! Reads order books from `lightpool-clob-indexer` and submits signed transactions through clob-index.
//! The bot never opens RPC/WS connections to the lightpool node itself.

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]

pub mod common;
pub mod config;
pub mod data;
pub mod execution;
pub mod factories;
pub mod http;
pub mod parse;
pub mod websocket;

// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::{fmt::Debug, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::common::consts::{
    DEFAULT_COLLATERAL_TOKEN, DEFAULT_CLOB_INDEX_HTTP, DEFAULT_CLOB_INDEX_HTTP_CONNECT_TIMEOUT_SECS,
    DEFAULT_CLOB_INDEX_HTTP_TIMEOUT_SECS, DEFAULT_CLOB_INDEX_WS,
};

fn nonempty_env(var: &str) -> Option<String> {
    std::env::var(var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn reject_lightpool_node_endpoint(name: &str, url: &str) {
    if url.contains(":26300") || url.contains(":26400") {
        log::warn!(
            "{name}={url} looks like a lightpool node endpoint; \
             point it at lightpool-clob-index instead (default http://127.0.0.1:3002)"
        );
    }
}

#[must_use]
pub fn clob_index_http_from_env() -> String {
    let url =
        nonempty_env("LIGHTPOOL_CLOB_INDEX_HTTP").unwrap_or_else(|| DEFAULT_CLOB_INDEX_HTTP.into());
    reject_lightpool_node_endpoint("LIGHTPOOL_CLOB_INDEX_HTTP", &url);
    url
}

#[must_use]
pub fn clob_index_ws_from_env() -> String {
    let url =
        nonempty_env("LIGHTPOOL_CLOB_INDEX_WS").unwrap_or_else(|| DEFAULT_CLOB_INDEX_WS.into());
    reject_lightpool_node_endpoint("LIGHTPOOL_CLOB_INDEX_WS", &url);
    url
}

#[must_use]
pub fn clob_index_http_timeout_secs_from_env() -> u64 {
    std::env::var("LIGHTPOOL_CLOB_INDEX_HTTP_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_CLOB_INDEX_HTTP_TIMEOUT_SECS)
}

#[must_use]
pub fn clob_index_http_connect_timeout_secs_from_env() -> u64 {
    std::env::var("LIGHTPOOL_CLOB_INDEX_HTTP_CONNECT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_CLOB_INDEX_HTTP_CONNECT_TIMEOUT_SECS)
}

#[must_use]
pub fn private_key_from_env() -> Option<String> {
    nonempty_env("LIGHTPOOL_PRIVATE_KEY")
}

#[must_use]
pub fn collateral_token_from_env() -> Option<String> {
    nonempty_env("LIGHTPOOL_COLLATERAL_TOKEN")
}

/// Resolve collateral token contract: `LIGHTPOOL_COLLATERAL_TOKEN` or [`DEFAULT_COLLATERAL_TOKEN`].
#[must_use]
pub fn resolve_collateral_token() -> String {
    collateral_token_from_env().unwrap_or_else(|| DEFAULT_COLLATERAL_TOKEN.to_string())
}

fn cli_wallet_path() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".lightpool").join("wallet.json"))
}

#[derive(Deserialize)]
struct CliWalletFile {
    private_key: String,
}

fn private_key_from_cli_wallet() -> Option<String> {
    let path = cli_wallet_path()?;
    let json = std::fs::read_to_string(&path).ok()?;
    let wallet: CliWalletFile = serde_json::from_str(&json).ok()?;
    let key = wallet.private_key.trim().to_string();
    if key.is_empty() {
        None
    } else {
        Some(key)
    }
}

/// Resolve signing key: `LIGHTPOOL_PRIVATE_KEY` first, then `~/.lightpool/wallet.json`.
pub fn resolve_private_key() -> anyhow::Result<String> {
    private_key_from_env()
        .or_else(private_key_from_cli_wallet)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "LightPool private key not found: set LIGHTPOOL_PRIVATE_KEY or create a wallet with lightpool-cli (~/.lightpool/wallet.json)"
            )
        })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LightpoolDataClientConfig {
    pub clob_index_http_url: String,
    pub clob_index_ws_url: String,
    /// Market slugs to bootstrap from clob-index.
    pub market_slugs: Vec<String>,
    /// Default book depth for subscriptions and snapshots.
    pub book_depth: u32,
}

impl Default for LightpoolDataClientConfig {
    fn default() -> Self {
        Self {
            clob_index_http_url: clob_index_http_from_env(),
            clob_index_ws_url: clob_index_ws_from_env(),
            market_slugs: Vec::new(),
            book_depth: 10,
        }
    }
}

impl LightpoolDataClientConfig {
    #[must_use]
    pub fn new(market_slugs: Vec<String>) -> Self {
        Self {
            market_slugs,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_book_depth(mut self, depth: u32) -> Self {
        self.book_depth = depth;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LightpoolExecClientConfig {
    pub clob_index_http_url: String,
    pub private_key: Option<String>,
    /// Market slugs used to resolve collateral and outcome token addresses before cache is warm.
    pub market_slugs: Vec<String>,
}

impl Default for LightpoolExecClientConfig {
    fn default() -> Self {
        Self {
            clob_index_http_url: clob_index_http_from_env(),
            private_key: private_key_from_env(),
            market_slugs: Vec::new(),
        }
    }
}

impl LightpoolExecClientConfig {
    pub fn resolved_private_key(&self) -> anyhow::Result<String> {
        self.private_key
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                private_key_from_env().or_else(private_key_from_cli_wallet)
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "LightPool private key not found: set LIGHTPOOL_PRIVATE_KEY or create a wallet with lightpool-cli (~/.lightpool/wallet.json)"
                )
            })
    }
}

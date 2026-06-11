use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::common::consts::{
    DEFAULT_CLOB_INDEX_HTTP, DEFAULT_CLOB_INDEX_WS, DEFAULT_NODE_RPC,
};

fn nonempty_env(var: &str) -> Option<String> {
    std::env::var(var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[must_use]
pub fn clob_index_http_from_env() -> String {
    nonempty_env("LIGHTPOOL_CLOB_INDEX_HTTP").unwrap_or_else(|| DEFAULT_CLOB_INDEX_HTTP.into())
}

#[must_use]
pub fn clob_index_ws_from_env() -> String {
    nonempty_env("LIGHTPOOL_CLOB_INDEX_WS").unwrap_or_else(|| DEFAULT_CLOB_INDEX_WS.into())
}

#[must_use]
pub fn node_rpc_from_env() -> String {
    nonempty_env("LIGHTPOOL_NODE_RPC").unwrap_or_else(|| DEFAULT_NODE_RPC.into())
}

#[must_use]
pub fn private_key_from_env() -> Option<String> {
    nonempty_env("LIGHTPOOL_PRIVATE_KEY")
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
    pub node_rpc_url: String,
    pub clob_index_http_url: String,
    pub private_key: Option<String>,
}

impl Default for LightpoolExecClientConfig {
    fn default() -> Self {
        Self {
            node_rpc_url: node_rpc_from_env(),
            clob_index_http_url: clob_index_http_from_env(),
            private_key: private_key_from_env(),
        }
    }
}

impl LightpoolExecClientConfig {
    pub fn resolved_private_key(&self) -> anyhow::Result<String> {
        self.private_key
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(private_key_from_env)
            .ok_or_else(|| anyhow::anyhow!("LIGHTPOOL_PRIVATE_KEY is required for execution"))
    }
}

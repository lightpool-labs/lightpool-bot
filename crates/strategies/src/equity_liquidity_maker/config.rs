// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Configuration for the equity liquidity maker strategy.

use nautilus_lightpool::config::SpotMarketBootstrap;
use nautilus_model::identifiers::{ClientId, StrategyId};
use nautilus_trading::strategy::StrategyConfig;

/// One equity symbol mirrored from Hyperliquid onto a LightPool spot.
#[derive(Debug, Clone)]
pub struct EquityMarketPair {
    pub symbol: String,
    pub hl_coin: String,
    pub spot: SpotMarketBootstrap,
}

/// Configuration for mirroring Hyperliquid equity L2 onto LightPool spots.
#[derive(Debug, Clone)]
pub struct EquityLiquidityMakerConfig {
    pub base: StrategyConfig,
    pub markets: Vec<EquityMarketPair>,
    pub depth: usize,
    pub log_interval: u64,
    pub managed_book: bool,
    pub lightpool_client_id: ClientId,
    pub reconcile_delta_batch_size: u64,
}

impl EquityLiquidityMakerConfig {
    #[must_use]
    pub fn new(markets: Vec<EquityMarketPair>) -> Self {
        Self {
            base: StrategyConfig {
                strategy_id: Some(StrategyId::from("EQUITY_LIQUIDITY_MAKER-001")),
                order_id_tag: Some("ELM001".to_string()),
                ..Default::default()
            },
            markets,
            depth: 10,
            log_interval: 50,
            managed_book: true,
            lightpool_client_id: ClientId::from("LIGHTPOOL"),
            reconcile_delta_batch_size: 10,
        }
    }

    #[must_use]
    pub fn with_depth(mut self, depth: usize) -> Self {
        self.depth = depth.max(1);
        self
    }

    #[must_use]
    pub fn with_log_interval(mut self, log_interval: u64) -> Self {
        self.log_interval = log_interval;
        self
    }

    #[must_use]
    pub fn with_strategy_id(mut self, strategy_id: StrategyId) -> Self {
        self.base.strategy_id = Some(strategy_id);
        self
    }

    #[must_use]
    pub fn with_reconcile_delta_batch_size(mut self, reconcile_delta_batch_size: u64) -> Self {
        self.reconcile_delta_batch_size = reconcile_delta_batch_size.max(1);
        self
    }

    #[must_use]
    pub fn with_lightpool_client_id(mut self, client_id: ClientId) -> Self {
        self.lightpool_client_id = client_id;
        self
    }
}

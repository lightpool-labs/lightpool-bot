// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Equity liquidity maker: subscribe Hyperliquid book deltas and mirror onto LightPool spots.

use std::fmt::Debug;
use std::num::NonZeroUsize;

use ahash::{AHashMap, AHashSet};
use nautilus_common::actor::DataActor;
use nautilus_model::{
    data::OrderBookDeltas,
    enums::BookType,
    events::OrderCanceled,
    identifiers::InstrumentId,
};
use nautilus_trading::{nautilus_strategy, strategy::StrategyCore};

use super::config::EquityLiquidityMakerConfig;

const HYPERLIQUID_VENUE: &str = "HYPERLIQUID";

/// Mirrors Hyperliquid equity L2 depth onto LightPool spot markets via own orders.
pub struct EquityLiquidityMaker {
    pub(super) core: StrategyCore,
    pub(super) config: EquityLiquidityMakerConfig,
    pub(super) subscribed_instruments: AHashSet<InstrumentId>,
    pub(super) delta_batches: AHashMap<InstrumentId, u64>,
    /// Hyperliquid instrument id → LightPool spot instrument id.
    pub(super) hl_to_lp: AHashMap<InstrumentId, InstrumentId>,
    pub(super) pending_reconcile_hl_instruments: AHashSet<InstrumentId>,
    pub(super) pending_reconcile_delta_count: u64,
}

impl EquityLiquidityMaker {
    #[must_use]
    pub fn new(config: EquityLiquidityMakerConfig) -> Self {
        Self {
            core: StrategyCore::new(config.base.clone()),
            config,
            subscribed_instruments: AHashSet::new(),
            delta_batches: AHashMap::new(),
            hl_to_lp: AHashMap::new(),
            pending_reconcile_hl_instruments: AHashSet::new(),
            pending_reconcile_delta_count: 0,
        }
    }

    fn hl_instrument_id(symbol: &str) -> InstrumentId {
        InstrumentId::from(format!("xyz:{symbol}-USD-PERP.HYPERLIQUID").as_str())
    }

    fn lp_instrument_id(symbol: &str) -> InstrumentId {
        InstrumentId::from(format!("{symbol}-USDT.LIGHTPOOL").as_str())
    }

    fn rebuild_instrument_pairs(&mut self) {
        self.hl_to_lp.clear();
        for market in &self.config.markets {
            let hl_id = Self::hl_instrument_id(&market.symbol);
            let lp_id = Self::lp_instrument_id(&market.symbol);
            log::info!(
                "equity pair symbol={} hl={hl_id} lp={lp_id} spot={}",
                market.symbol,
                market.spot.spot_market,
            );
            self.hl_to_lp.insert(hl_id, lp_id);
        }
    }

    fn collect_hl_delta_for_reconcile(&mut self, instrument_id: InstrumentId) {
        self.pending_reconcile_hl_instruments.insert(instrument_id);
        self.pending_reconcile_delta_count += 1;
        if self.pending_reconcile_delta_count >= self.config.reconcile_delta_batch_size {
            self.reconcile_batched_hl_deltas();
        }
    }

    fn reconcile_batched_hl_deltas(&mut self) {
        let instrument_ids: Vec<InstrumentId> =
            self.pending_reconcile_hl_instruments.drain().collect();
        self.pending_reconcile_delta_count = 0;
        for instrument_id in instrument_ids {
            if let Err(e) = self.reconcile_from_hl_delta(instrument_id) {
                log::warn!("Failed to reconcile LightPool liquidity for {instrument_id}: {e:#}");
            }
        }
    }

    fn reconcile_subscriptions(&mut self) {
        let Some(depth) = NonZeroUsize::new(self.config.depth.max(1)) else {
            return;
        };

        let hl_ids: Vec<InstrumentId> = self.hl_to_lp.keys().copied().collect();
        for instrument_id in hl_ids {
            if self.subscribed_instruments.contains(&instrument_id) {
                continue;
            }
            self.subscribe_book_deltas(
                instrument_id,
                BookType::L2_MBP,
                Some(depth),
                None,
                true,
                None,
            );
            self.subscribed_instruments.insert(instrument_id);
            log::info!(
                "Subscribed to Hyperliquid order book deltas instrument_id={instrument_id} depth={}",
                self.config.depth,
            );
        }
    }

    fn maybe_log_book(&mut self, instrument_id: InstrumentId) {
        if self.config.log_interval == 0 {
            return;
        }
        let count = self.delta_batches.entry(instrument_id).or_insert(0);
        *count += 1;
        if *count % self.config.log_interval != 0 {
            return;
        }
        let depth = self.config.depth.max(1);
        let cache = self.cache();
        let Some(book) = cache.order_book(&instrument_id) else {
            return;
        };
        let bids: Vec<String> = book
            .bids(Some(depth))
            .map(|level| format!("{}@{}", level.size(), level.price))
            .collect();
        let asks: Vec<String> = book
            .asks(Some(depth))
            .map(|level| format!("{}@{}", level.size(), level.price))
            .collect();
        log::info!(
            "HL book {instrument_id} bids={} asks={}",
            if bids.is_empty() {
                "-".into()
            } else {
                bids.join(", ")
            },
            if asks.is_empty() {
                "-".into()
            } else {
                asks.join(", ")
            },
        );
    }
}

nautilus_strategy!(EquityLiquidityMaker, {});

impl Debug for EquityLiquidityMaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(EquityLiquidityMaker))
            .field(
                "symbols",
                &self
                    .config
                    .markets
                    .iter()
                    .map(|m| m.symbol.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("depth", &self.config.depth)
            .field("pairs", &self.hl_to_lp.len())
            .finish()
    }
}

impl DataActor for EquityLiquidityMaker {
    fn on_start(&mut self) -> anyhow::Result<()> {
        if !self.config.managed_book {
            anyhow::bail!("EquityLiquidityMaker requires managed_book=true");
        }
        if self.config.markets.is_empty() {
            anyhow::bail!("EquityLiquidityMaker requires at least one market");
        }

        self.rebuild_instrument_pairs();
        self.reconcile_subscriptions();
        log::info!(
            "EquityLiquidityMaker started symbols={:?} depth={} reconcile_delta_batch_size={}",
            self.config
                .markets
                .iter()
                .map(|m| m.symbol.as_str())
                .collect::<Vec<_>>(),
            self.config.depth,
            self.config.reconcile_delta_batch_size,
        );
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        log::info!("EquityLiquidityMaker stopping; unsubscribing Hyperliquid book deltas");
        for instrument_id in self.subscribed_instruments.clone() {
            self.unsubscribe_book_deltas(instrument_id, None, None);
        }
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        let instrument_id = deltas.instrument_id;
        if instrument_id.venue.as_str() != HYPERLIQUID_VENUE {
            return Ok(());
        }
        self.maybe_log_book(instrument_id);
        self.collect_hl_delta_for_reconcile(instrument_id);
        Ok(())
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) -> anyhow::Result<()> {
        self.reconcile_after_own_cancel(event.instrument_id)
    }
}

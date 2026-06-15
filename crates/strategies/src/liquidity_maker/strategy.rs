// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Liquidity maker strategy: subscribe Polymarket book deltas and mirror onto LightPool via orders.

use std::fmt::Debug;
use std::num::NonZeroUsize;

use ahash::{AHashMap, AHashSet};
use nautilus_common::actor::DataActor;
use nautilus_model::{
    data::OrderBookDeltas,
    enums::BookType,
    events::{OrderAccepted, OrderCanceled, OrderFilled, OrderUpdated},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    orderbook::OrderBook,
};
use nautilus_trading::{nautilus_strategy, strategy::StrategyCore};

use super::config::LiquidityMakerConfig;
use super::markets::{
    SlugMarketIds, assign_lightpool_markets_to_slugs, assign_markets_to_slugs,
    discover_lightpool_markets_from_cache, discover_markets_from_cache,
};

const POLYMARKET_VENUE: &str = "POLYMARKET";
const LIGHTPOOL_VENUE: &str = "LIGHTPOOL";

/// Subscribes to Polymarket `OrderBookDeltas` and mirrors depth onto LightPool using own orders.
///
/// Requires `managed_book = true` so the data engine `BookUpdater` maintains
/// Polymarket `cache.order_book()` while deltas stream in.
pub struct LiquidityMaker {
    pub(super) core: StrategyCore,
    pub(super) config: LiquidityMakerConfig,
    /// Event slug -> condition_id -> YES/NO instrument ids (Polymarket).
    pub(super) slug_markets: AHashMap<String, AHashMap<String, SlugMarketIds>>,
    /// Event slug -> condition ids discovered for that slug (Polymarket).
    pub(super) slug_to_conditions: AHashMap<String, AHashSet<String>>,
    /// condition_id -> YES/NO instrument ids (Polymarket).
    pub(super) markets: AHashMap<String, SlugMarketIds>,
    /// market_slug -> YES/NO instrument ids (LightPool).
    pub(super) lightpool_markets: AHashMap<String, SlugMarketIds>,
    pub(super) instrument_to_market_key: AHashMap<InstrumentId, String>,
    pub(super) subscribed_instruments: AHashSet<InstrumentId>,
    pub(super) delta_batches: AHashMap<InstrumentId, u64>,
    /// Polymarket instrument id -> paired LightPool instrument id.
    pub(super) pm_to_lp: AHashMap<InstrumentId, InstrumentId>,
    /// Polymarket instrument ids with deltas in the current reconcile batch.
    pub(super) pending_reconcile_pm_instruments: AHashSet<InstrumentId>,
    /// Polymarket delta count in the current reconcile batch.
    pub(super) pending_reconcile_delta_count: u64,
}

impl LiquidityMaker {
    /// Creates a new [`LiquidityMaker`] from config.
    #[must_use]
    pub fn new(config: LiquidityMakerConfig) -> Self {
        Self {
            core: StrategyCore::new(config.base.clone()),
            config,
            slug_markets: AHashMap::new(),
            slug_to_conditions: AHashMap::new(),
            markets: AHashMap::new(),
            lightpool_markets: AHashMap::new(),
            instrument_to_market_key: AHashMap::new(),
            subscribed_instruments: AHashSet::new(),
            delta_batches: AHashMap::new(),
            pm_to_lp: AHashMap::new(),
            pending_reconcile_pm_instruments: AHashSet::new(),
            pending_reconcile_delta_count: 0,
        }
    }

    fn collect_polymarket_delta_for_reconcile(&mut self, instrument_id: InstrumentId) {
        if !self.config.trading_enabled {
            return;
        }
        self.pending_reconcile_pm_instruments.insert(instrument_id);
        self.pending_reconcile_delta_count += 1;
        if self.pending_reconcile_delta_count >= self.config.reconcile_delta_batch_size {
            self.reconcile_batched_polymarket_deltas();
        }
    }

    fn reconcile_batched_polymarket_deltas(&mut self) {
        if !self.config.trading_enabled {
            self.pending_reconcile_pm_instruments.clear();
            self.pending_reconcile_delta_count = 0;
            return;
        }
        let instrument_ids: Vec<InstrumentId> = self.pending_reconcile_pm_instruments.drain().collect();
        self.pending_reconcile_delta_count = 0;
        for instrument_id in instrument_ids {
            if let Err(e) = self.reconcile_from_polymarket_delta(instrument_id) {
                log::warn!(
                    "Failed to reconcile LightPool liquidity for {instrument_id}: {e:#}"
                );
            }
        }
    }

    fn sync_polymarket_markets_from_cache(&mut self) -> usize {
        let discovered = discover_markets_from_cache(&self.cache());
        for (condition_id, market) in &discovered {
            self.markets
                .entry(condition_id.clone())
                .or_insert_with(|| market.clone());
        }
        assign_markets_to_slugs(
            &self.config.polymarket_slugs,
            &discovered,
            &mut self.slug_markets,
            &mut self.slug_to_conditions,
        )
    }

    fn sync_lightpool_markets_from_cache(&mut self) -> usize {
        if !self.config.lightpool_enabled {
            return 0;
        }
        let discovered = discover_lightpool_markets_from_cache(&self.cache());
        assign_lightpool_markets_to_slugs(
            &self.config.lightpool_slugs,
            &discovered,
            &mut self.lightpool_markets,
        )
    }

    fn sync_markets_from_cache(&mut self) -> usize {
        self.sync_polymarket_markets_from_cache() + self.sync_lightpool_markets_from_cache()
    }

    fn subscribe_market(
        &mut self,
        market: &SlugMarketIds,
        depth: NonZeroUsize,
        venue_label: &str,
    ) {
        for instrument_id in [market.yes_id, market.no_id] {
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
            self.instrument_to_market_key
                .insert(instrument_id, market.condition_id.clone());
            self.subscribed_instruments.insert(instrument_id);
            log::info!(
                "Subscribed to {venue_label} order book deltas instrument_id={instrument_id} \
                 market_key={} depth={}",
                market.condition_id,
                self.config.depth,
            );
        }
    }

    fn reconcile_subscriptions(&mut self) {
        let Some(depth) = NonZeroUsize::new(self.config.depth.max(1)) else {
            return;
        };

        let polymarket_markets: Vec<SlugMarketIds> = self.markets.values().cloned().collect();
        for market in polymarket_markets {
            self.subscribe_market(&market, depth, "Polymarket");
        }

    }

    fn slug_for_condition(&self, condition_id: &str) -> Option<&str> {
        self.slug_to_conditions
            .iter()
            .find_map(|(slug, conditions)| {
                if conditions.contains(condition_id) {
                    Some(slug.as_str())
                } else {
                    None
                }
            })
    }

    fn venue_label(instrument_id: InstrumentId) -> &'static str {
        match instrument_id.venue.as_str() {
            POLYMARKET_VENUE => "Polymarket",
            LIGHTPOOL_VENUE => "Lightpool",
            _ => "Unknown",
        }
    }

    fn warn_missing_lightpool_markets(&self) {
        if !self.config.lightpool_enabled || !self.lightpool_markets.is_empty() {
            return;
        }
        let discovered = discover_lightpool_markets_from_cache(&self.cache());
        log::warn!(
            "No LightPool markets matched lightpool_slugs={:?}; \
             available_lightpool_slugs_in_cache={:?}",
            self.config.lightpool_slugs,
            discovered.keys().collect::<Vec<_>>(),
        );
    }
}

fn format_book_side(book: &OrderBook, bids: bool, depth: usize) -> String {
    let levels: Vec<String> = if bids {
        book.bids(Some(depth))
            .map(|level| format!("{}@{}", level.size(), level.price))
            .collect()
    } else {
        book.asks(Some(depth))
            .map(|level| format!("{}@{}", level.size(), level.price))
            .collect()
    };

    if levels.is_empty() {
        "-".into()
    } else {
        levels.join(", ")
    }
}

nautilus_strategy!(LiquidityMaker, {
    // fn on_order_accepted(&mut self, event: OrderAccepted) {
    //     self.maybe_reconcile_lightpool(event.instrument_id);
    // }

    // fn on_order_updated(&mut self, event: OrderUpdated) {
    //     self.maybe_reconcile_lightpool(event.instrument_id);
    // }
});

impl Debug for LiquidityMaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(LiquidityMaker))
            .field("polymarket_slugs", &self.config.polymarket_slugs)
            .field("lightpool_slugs", &self.config.lightpool_slugs)
            .field("depth", &self.config.depth)
            .field("polymarket_markets", &self.markets.len())
            .field("lightpool_markets", &self.lightpool_markets.len())
            .finish()
    }
}

impl DataActor for LiquidityMaker {
    fn on_start(&mut self) -> anyhow::Result<()> {
        if !self.config.managed_book {
            anyhow::bail!("LiquidityMaker requires managed_book=true to read cache order books");
        }

        let synced = self.sync_markets_from_cache();
        log::info!(
            "LiquidityMaker started polymarket_slugs={:?} lightpool_slugs={:?} \
             synced_markets={synced} polymarket_markets={} lightpool_markets={} \
             lightpool_enabled={}",
            self.config.polymarket_slugs,
            self.config.lightpool_slugs,
            self.markets.len(),
            self.lightpool_markets.len(),
            self.config.lightpool_enabled,
        );
        self.reconcile_subscriptions();
        self.rebuild_instrument_pairs();
        self.warn_missing_lightpool_markets();
        if self.config.trading_enabled {
            log::info!(
                "LightPool mirroring enabled depth={} client_id={} reconcile_delta_batch_size={}",
                self.config.depth,
                self.config.lightpool_client_id,
                self.config.reconcile_delta_batch_size,
            );
        }
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        log::info!("LiquidityMaker stopping; unsubscribing Polymarket book deltas");
        for instrument_id in self.subscribed_instruments.clone() {
            self.unsubscribe_book_deltas(instrument_id, None, None);
        }
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        log::info!("OrderBookDeltas {:?}", deltas);

        let instrument_id = deltas.instrument_id;
        let venue = instrument_id.venue.as_str();

        if venue == POLYMARKET_VENUE {
            self.collect_polymarket_delta_for_reconcile(instrument_id);
        }

        Ok(())
    }

}

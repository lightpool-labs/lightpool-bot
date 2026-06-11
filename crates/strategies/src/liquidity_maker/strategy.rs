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

//! Liquidity maker strategy: subscribe Polymarket book deltas and read books from cache.

use std::fmt::Debug;
use std::num::NonZeroUsize;

use ahash::{AHashMap, AHashSet};
use nautilus_common::actor::DataActor;
use nautilus_model::{
    data::OrderBookDeltas,
    enums::BookType,
    identifiers::InstrumentId,
    orderbook::OrderBook,
};
use nautilus_trading::{nautilus_strategy, strategy::StrategyCore};

use super::config::LiquidityMakerConfig;
use super::markets::{SlugMarketIds, assign_markets_to_slugs, discover_markets_from_cache};

/// Subscribes to Polymarket `OrderBookDeltas` and reads the managed book from cache.
///
/// Requires `managed_book = true` so the data engine `BookUpdater` maintains
/// `cache.order_book()` while deltas stream in.
pub struct LiquidityMaker {
    pub(super) core: StrategyCore,
    pub(super) config: LiquidityMakerConfig,
    /// Event slug -> condition_id -> YES/NO instrument ids.
    pub(super) slug_markets: AHashMap<String, AHashMap<String, SlugMarketIds>>,
    /// Event slug -> condition ids discovered for that slug.
    pub(super) slug_to_conditions: AHashMap<String, AHashSet<String>>,
    /// condition_id -> YES/NO instrument ids.
    pub(super) markets: AHashMap<String, SlugMarketIds>,
    pub(super) instrument_to_condition: AHashMap<InstrumentId, String>,
    pub(super) subscribed_instruments: AHashSet<InstrumentId>,
    pub(super) delta_batches: AHashMap<InstrumentId, u64>,
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
            instrument_to_condition: AHashMap::new(),
            subscribed_instruments: AHashSet::new(),
            delta_batches: AHashMap::new(),
        }
    }

    fn sync_markets_from_cache(&mut self) -> usize {
        let discovered = discover_markets_from_cache(&self.cache());
        for (condition_id, market) in &discovered {
            self.markets
                .entry(condition_id.clone())
                .or_insert_with(|| market.clone());
        }
        assign_markets_to_slugs(
            &self.config.slugs,
            &discovered,
            &mut self.slug_markets,
            &mut self.slug_to_conditions,
        )
    }

    fn reconcile_subscriptions(&mut self) {
        let Some(depth) = NonZeroUsize::new(self.config.depth.max(1)) else {
            return;
        };

        let markets: Vec<SlugMarketIds> = self.markets.values().cloned().collect();
        for market in markets {
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
                self.instrument_to_condition
                    .insert(instrument_id, market.condition_id.clone());
                self.subscribed_instruments.insert(instrument_id);
                log::info!(
                    "Subscribed to Polymarket order book deltas instrument_id={instrument_id} \
                     condition_id={} depth={}",
                    market.condition_id,
                    self.config.depth,
                );
            }
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

    fn log_cache_book(&self, instrument_id: InstrumentId, batch: u64) {
        let condition_id = self
            .instrument_to_condition
            .get(&instrument_id)
            .map(String::as_str)
            .unwrap_or("unknown");
        let slug = self
            .slug_for_condition(condition_id)
            .unwrap_or("unknown");

        let snapshot = {
            let cache = self.cache();
            let Some(book) = cache.order_book(&instrument_id) else {
                log::warn!(
                    "cache.order_book() is None for slug={slug} condition_id={condition_id} \
                     instrument_id={instrument_id} batch={batch}; ensure managed_book=true"
                );
                return;
            };
            let depth = self.config.depth.max(1);
            (
                book.sequence,
                book.update_count,
                format_book_side(book, true, depth),
                format_book_side(book, false, depth),
            )
        };

        log::info!(
            "Polymarket cache order book slug={slug} condition_id={condition_id} \
             instrument_id={instrument_id} batch={batch} sequence={} update_count={} \
             bids=[{}] asks=[{}]",
            snapshot.0,
            snapshot.1,
            snapshot.2,
            snapshot.3,
        );
    }

    fn should_log(&self, batch: u64) -> bool {
        self.config.log_interval > 0 && batch.is_multiple_of(self.config.log_interval)
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

nautilus_strategy!(LiquidityMaker);

impl Debug for LiquidityMaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(LiquidityMaker))
            .field("slugs", &self.config.slugs)
            .field("depth", &self.config.depth)
            .field("markets", &self.markets.len())
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
            "LiquidityMaker started slugs={:?} synced_markets={synced} total_markets={}",
            self.config.slugs,
            self.markets.len(),
        );
        self.reconcile_subscriptions();
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        for instrument_id in self.subscribed_instruments.clone() {
            self.unsubscribe_book_deltas(instrument_id, None, None);
        }
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        if self.sync_markets_from_cache() > 0 {
            self.reconcile_subscriptions();
        }

        let instrument_id = deltas.instrument_id;
        let batch = {
            let count = self
                .delta_batches
                .entry(instrument_id)
                .and_modify(|count| *count = count.saturating_add(1))
                .or_insert(1);
            *count
        };

        if self.should_log(batch) {
            self.log_cache_book(instrument_id, batch);
        }

        Ok(())
    }
}

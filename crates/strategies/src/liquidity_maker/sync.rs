//! Reconcile LightPool resting liquidity against Polymarket cache books.

use indexmap::IndexMap;
use nautilus_model::{
    enums::OrderSide,
    identifiers::{InstrumentId, Venue},
    orderbook::OrderBook,
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_trading::strategy::Strategy;
use rust_decimal::Decimal;

use super::strategy::LiquidityMaker;

const LIGHTPOOL_VENUE: &str = "LIGHTPOOL";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BookSideSnapshot {
    pub levels: IndexMap<Decimal, Decimal>,
}

impl BookSideSnapshot {
    fn from_book(book: &OrderBook, depth: usize, bids: bool) -> Self {
        let levels = if bids {
            book.bids_as_map(Some(depth))
        } else {
            book.asks_as_map(Some(depth))
        };
        Self { levels }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BookSnapshot {
    pub bids: BookSideSnapshot,
    pub asks: BookSideSnapshot,
}

impl BookSnapshot {
    fn from_book(book: &OrderBook, depth: usize) -> Self {
        Self {
            bids: BookSideSnapshot::from_book(book, depth, true),
            asks: BookSideSnapshot::from_book(book, depth, false),
        }
    }
}

pub(super) fn books_match(reference: &BookSnapshot, actual: &BookSnapshot) -> bool {
    reference.bids.levels == actual.bids.levels && reference.asks.levels == actual.asks.levels
}

pub(super) struct MirrorLevel {
    pub side: OrderSide,
    pub price: Price,
    pub quantity: Quantity,
}

pub(super) fn mirror_levels(book: &OrderBook, depth: usize) -> Vec<MirrorLevel> {
    let mut levels = Vec::new();
    for level in book.bids(Some(depth)) {
        let Ok(quantity) = Quantity::from_decimal(level.size_decimal()) else {
            continue;
        };
        if quantity.is_zero() {
            continue;
        }
        levels.push(MirrorLevel {
            side: OrderSide::Buy,
            price: level.price.value,
            quantity,
        });
    }
    for level in book.asks(Some(depth)) {
        let Ok(quantity) = Quantity::from_decimal(level.size_decimal()) else {
            continue;
        };
        if quantity.is_zero() {
            continue;
        }
        levels.push(MirrorLevel {
            side: OrderSide::Sell,
            price: level.price.value,
            quantity,
        });
    }
    levels
}

impl LiquidityMaker {
    pub(super) fn rebuild_instrument_pairs(&mut self) {
        self.pm_to_lp.clear();
        if !self.config.lightpool_enabled || !self.config.trading_enabled {
            return;
        }

        let Some(lp_slug) = self.config.lightpool_slugs.first() else {
            return;
        };
        let Some(lp_market) = self.lightpool_markets.get(lp_slug) else {
            return;
        };
        let Some(pm_slug) = self.config.polymarket_slugs.first() else {
            return;
        };
        let Some(pm_markets) = self.slug_markets.get(pm_slug) else {
            return;
        };

        for pm_market in pm_markets.values() {
            self.pm_to_lp.insert(pm_market.yes_id, lp_market.yes_id);
            self.pm_to_lp.insert(pm_market.no_id, lp_market.no_id);
        }
    }

    pub(super) fn reconcile_from_polymarket_delta(
        &mut self,
        polymarket_instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        if !self.config.trading_enabled || !self.config.lightpool_enabled {
            return Ok(());
        }

        let Some(lightpool_instrument_id) = self.pm_to_lp.get(&polymarket_instrument_id).copied()
        else {
            return Ok(());
        };

        let depth = self.config.depth.max(1);
        let strategy_id = self.core.strategy_id();
        let client_id = self.config.lightpool_client_id;
        let venue = Venue::from(LIGHTPOOL_VENUE);

        let plan = {
            let cache = self.cache();
            let Some(polymarket_book) = cache.order_book(&polymarket_instrument_id) else {
                log::warn!(
                    "Polymarket book missing in cache for {polymarket_instrument_id}; skip reconcile"
                );
                return Ok(());
            };
            let Some(lightpool_book) = cache.order_book(&lightpool_instrument_id) else {
                log::warn!(
                    "LightPool book missing in cache for {lightpool_instrument_id}; skip reconcile"
                );
                return Ok(());
            };

            let reference = BookSnapshot::from_book(polymarket_book, depth);
            let actual = BookSnapshot::from_book(lightpool_book, depth);
            if books_match(&reference, &actual) {
                return Ok(());
            }

            let Some(strategy_id) = strategy_id else {
                return Ok(());
            };

            if !cache
                .orders_inflight(
                    Some(&venue),
                    Some(&lightpool_instrument_id),
                    Some(&strategy_id),
                    None,
                    None,
                )
                .is_empty()
            {
                log::debug!(
                    "Skip reconcile while inflight orders exist instrument_id={lightpool_instrument_id}"
                );
                return Ok(());
            }

            let open_orders = cache
                .orders_open(
                    Some(&venue),
                    Some(&lightpool_instrument_id),
                    Some(&strategy_id),
                    None,
                    None,
                )
                .into_iter()
                .map(|order| order.client_order_id())
                .collect::<Vec<_>>();

            if !open_orders.is_empty() {
                ReconcilePlan::Cancel(open_orders)
            } else {
                ReconcilePlan::Place(mirror_levels(polymarket_book, depth))
            }
        };

        match plan {
            ReconcilePlan::Cancel(open_orders) => {
                log::info!(
                    "Cancelling {} LightPool orders before mirroring Polymarket book \
                     polymarket_instrument_id={polymarket_instrument_id} \
                     lightpool_instrument_id={lightpool_instrument_id}",
                    open_orders.len(),
                );
                for client_order_id in open_orders {
                    if let Err(e) = self.cancel_order(client_order_id, Some(client_id), None) {
                        log::warn!("Failed to cancel {client_order_id}: {e}");
                    }
                }
            }
            ReconcilePlan::Place(levels) => {
                if levels.is_empty() {
                    return Ok(());
                }
                log::info!(
                    "Mirroring Polymarket book to LightPool polymarket_instrument_id={polymarket_instrument_id} \
                     lightpool_instrument_id={lightpool_instrument_id} levels={}",
                    levels.len(),
                );
                for level in levels {
                    if level.quantity.is_zero() {
                        continue;
                    }
                    let order = self.core.order_factory().limit(
                        lightpool_instrument_id,
                        level.side,
                        level.quantity,
                        level.price,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                    if let Err(e) = self.submit_order(order, None, Some(client_id), None) {
                        log::warn!(
                            "Failed to submit mirror order on {lightpool_instrument_id} {:?} {}@{}: {e}",
                            level.side,
                            level.quantity,
                            level.price,
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

enum ReconcilePlan {
    Cancel(Vec<nautilus_model::identifiers::ClientOrderId>),
    Place(Vec<MirrorLevel>),
}

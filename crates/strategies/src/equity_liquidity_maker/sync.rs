// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Reconcile LightPool resting liquidity against Hyperliquid cache books.

use indexmap::IndexMap;
use nautilus_model::{
    enums::OrderSide,
    identifiers::{ClientOrderId, InstrumentId, Venue},
    orderbook::OrderBook,
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};
use nautilus_trading::strategy::Strategy;
use rust_decimal::Decimal;

use super::strategy::EquityLiquidityMaker;

const LIGHTPOOL_VENUE: &str = "LIGHTPOOL";

fn is_mirrorable_size(size: Decimal) -> bool {
    size >= Decimal::new(1, 1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BookSideSnapshot {
    levels: IndexMap<Decimal, Decimal>,
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

    fn from_orders(orders: &[OrderAny], depth: usize, bids: bool) -> Self {
        let side = if bids {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let mut totals: IndexMap<Decimal, Decimal> = IndexMap::new();
        for order in orders {
            if order.order_side() != side {
                continue;
            }
            let Some(price) = order.price().map(|p| p.as_decimal()) else {
                continue;
            };
            let qty = order.quantity().as_decimal();
            if qty.is_zero() {
                continue;
            }
            *totals.entry(price).or_insert(Decimal::ZERO) += qty;
        }
        Self {
            levels: trim_side_levels(totals, depth, bids),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BookSnapshot {
    bids: BookSideSnapshot,
    asks: BookSideSnapshot,
}

fn format_dropped_levels(levels: &IndexMap<Decimal, Decimal>) -> String {
    let dropped: Vec<String> = levels
        .iter()
        .filter(|(_, size)| !is_mirrorable_size(**size))
        .map(|(price, size)| format!("{price}@{size}"))
        .collect();
    if dropped.is_empty() {
        "none".to_string()
    } else {
        dropped.join(", ")
    }
}

struct FilteredBook {
    snapshot: BookSnapshot,
    hl_bid_levels: usize,
    hl_ask_levels: usize,
    dropped_bids: String,
    dropped_asks: String,
}

impl BookSnapshot {
    fn from_book_filtered(book: &OrderBook, depth: usize) -> FilteredBook {
        let raw_bids = BookSideSnapshot::from_book(book, depth, true);
        let raw_asks = BookSideSnapshot::from_book(book, depth, false);
        let hl_bid_levels = raw_bids.levels.len();
        let hl_ask_levels = raw_asks.levels.len();
        let dropped_bids = format_dropped_levels(&raw_bids.levels);
        let dropped_asks = format_dropped_levels(&raw_asks.levels);
        let mut snapshot = Self {
            bids: raw_bids,
            asks: raw_asks,
        };
        snapshot.bids.levels.retain(|_, size| is_mirrorable_size(*size));
        snapshot.asks.levels.retain(|_, size| is_mirrorable_size(*size));
        FilteredBook {
            snapshot,
            hl_bid_levels,
            hl_ask_levels,
            dropped_bids,
            dropped_asks,
        }
    }

    fn from_open_orders(orders: &[OrderAny], depth: usize) -> Self {
        Self {
            bids: BookSideSnapshot::from_orders(orders, depth, true),
            asks: BookSideSnapshot::from_orders(orders, depth, false),
        }
    }
}

fn trim_side_levels(
    levels: IndexMap<Decimal, Decimal>,
    depth: usize,
    bids: bool,
) -> IndexMap<Decimal, Decimal> {
    let mut keys: Vec<Decimal> = levels.keys().copied().collect();
    if bids {
        keys.sort_by(|a, b| b.cmp(a));
    } else {
        keys.sort();
    }
    keys.truncate(depth);
    keys.into_iter()
        .filter_map(|price| levels.get(&price).copied().map(|size| (price, size)))
        .collect()
}

fn books_match(reference: &BookSnapshot, actual: &BookSnapshot) -> bool {
    reference.bids.levels == actual.bids.levels && reference.asks.levels == actual.asks.levels
}

#[derive(Debug, Clone)]
struct LevelOrder {
    client_order_id: ClientOrderId,
    quantity: Quantity,
    has_venue_id: bool,
}

#[derive(Debug, Default)]
struct OrdersByLevel {
    bids: IndexMap<Decimal, Vec<LevelOrder>>,
    asks: IndexMap<Decimal, Vec<LevelOrder>>,
}

impl OrdersByLevel {
    fn from_open_orders(orders: &[OrderAny]) -> Self {
        let mut grouped = Self::default();
        for order in orders {
            let Some(price) = order.price().map(|p| p.as_decimal()) else {
                continue;
            };
            let entry = LevelOrder {
                client_order_id: order.client_order_id(),
                quantity: order.quantity(),
                has_venue_id: order.venue_order_id().is_some(),
            };
            match order.order_side() {
                OrderSide::Buy => grouped.bids.entry(price).or_default().push(entry),
                OrderSide::Sell => grouped.asks.entry(price).or_default().push(entry),
                _ => {}
            }
        }
        grouped
    }
}

#[derive(Debug)]
enum ReconcileAction {
    Place {
        side: OrderSide,
        price: Price,
        quantity: Quantity,
    },
    Cancel {
        client_order_id: ClientOrderId,
    },
    Modify {
        client_order_id: ClientOrderId,
        quantity: Quantity,
    },
}

fn diff_side(
    side: OrderSide,
    reference: &IndexMap<Decimal, Decimal>,
    actual: &IndexMap<Decimal, Decimal>,
    orders: &IndexMap<Decimal, Vec<LevelOrder>>,
    actions: &mut Vec<ReconcileAction>,
) {
    let mut prices: Vec<Decimal> = reference.keys().chain(actual.keys()).copied().collect();
    prices.sort();
    prices.dedup();

    for price in prices {
        let ref_qty = reference.get(&price).copied().unwrap_or(Decimal::ZERO);
        let act_qty = actual.get(&price).copied().unwrap_or(Decimal::ZERO);
        if ref_qty == act_qty {
            continue;
        }

        let level_orders = orders.get(&price).cloned().unwrap_or_default();
        let Ok(price_value) = Price::from_decimal(price) else {
            continue;
        };

        if act_qty.is_zero() {
            let Ok(quantity) = Quantity::from_decimal(ref_qty) else {
                continue;
            };
            if quantity.is_zero() {
                continue;
            }
            actions.push(ReconcileAction::Place {
                side,
                price: price_value,
                quantity,
            });
            continue;
        }

        if ref_qty.is_zero() {
            for order in level_orders {
                if order.has_venue_id {
                    actions.push(ReconcileAction::Cancel {
                        client_order_id: order.client_order_id,
                    });
                }
            }
            continue;
        }

        let actionable: Vec<_> = level_orders
            .into_iter()
            .filter(|order| order.has_venue_id)
            .collect();
        if actionable.len() == 1 {
            let Ok(quantity) = Quantity::from_decimal(ref_qty) else {
                continue;
            };
            if quantity.is_zero() {
                continue;
            }
            actions.push(ReconcileAction::Modify {
                client_order_id: actionable[0].client_order_id,
                quantity,
            });
            continue;
        }

        for order in actionable {
            actions.push(ReconcileAction::Cancel {
                client_order_id: order.client_order_id,
            });
        }
        let Ok(quantity) = Quantity::from_decimal(ref_qty) else {
            continue;
        };
        if !quantity.is_zero() {
            actions.push(ReconcileAction::Place {
                side,
                price: price_value,
                quantity,
            });
        }
    }
}

fn build_reconcile_actions(
    reference: &BookSnapshot,
    actual: &BookSnapshot,
    orders_by_level: &OrdersByLevel,
) -> Vec<ReconcileAction> {
    let mut actions = Vec::new();
    diff_side(
        OrderSide::Buy,
        &reference.bids.levels,
        &actual.bids.levels,
        &orders_by_level.bids,
        &mut actions,
    );
    diff_side(
        OrderSide::Sell,
        &reference.asks.levels,
        &actual.asks.levels,
        &orders_by_level.asks,
        &mut actions,
    );
    actions
}

fn place_crosses_open_order(side: OrderSide, price: Decimal, open_orders: &[OrderAny]) -> bool {
    match side {
        OrderSide::Buy => open_orders.iter().any(|order| {
            order.order_side() == OrderSide::Sell
                && order
                    .price()
                    .is_some_and(|resting| resting.as_decimal() <= price)
        }),
        OrderSide::Sell => open_orders.iter().any(|order| {
            order.order_side() == OrderSide::Buy
                && order
                    .price()
                    .is_some_and(|resting| resting.as_decimal() >= price)
        }),
        _ => false,
    }
}

fn place_crosses_prices(side: OrderSide, price: Decimal, opposite: &[Decimal]) -> bool {
    match side {
        OrderSide::Buy => opposite.iter().any(|ask| *ask <= price),
        OrderSide::Sell => opposite.iter().any(|bid| *bid >= price),
        _ => false,
    }
}

fn without_crossing_places(
    actions: Vec<ReconcileAction>,
    open_orders: &[OrderAny],
) -> Vec<ReconcileAction> {
    let mut deferred = Vec::new();
    let mut kept = Vec::new();
    for action in actions {
        match &action {
            ReconcileAction::Place {
                side,
                price,
                quantity,
            } if place_crosses_open_order(*side, price.as_decimal(), open_orders) => {
                deferred.push(format!("{side:?} {quantity}@{price}"));
            }
            _ => kept.push(action),
        }
    }

    let bid_prices: Vec<Decimal> = kept
        .iter()
        .filter_map(|action| match action {
            ReconcileAction::Place {
                side: OrderSide::Buy,
                price,
                ..
            } => Some(price.as_decimal()),
            _ => None,
        })
        .collect();
    let ask_prices: Vec<Decimal> = kept
        .iter()
        .filter_map(|action| match action {
            ReconcileAction::Place {
                side: OrderSide::Sell,
                price,
                ..
            } => Some(price.as_decimal()),
            _ => None,
        })
        .collect();
    let ask_prices: Vec<Decimal> = ask_prices
        .into_iter()
        .filter(|ask| !bid_prices.iter().any(|bid| bid >= ask))
        .collect();

    kept.retain(|action| match action {
        ReconcileAction::Place {
            side,
            price,
            quantity,
        } => {
            let px = price.as_decimal();
            let crosses = match side {
                OrderSide::Buy => place_crosses_prices(*side, px, &ask_prices),
                OrderSide::Sell => place_crosses_prices(*side, px, &bid_prices),
                _ => false,
            };
            if crosses {
                deferred.push(format!("{side:?} {quantity}@{price}"));
                false
            } else {
                true
            }
        }
        _ => true,
    });

    if !deferred.is_empty() {
        log::info!(
            "Defer {} crossing place(s) so this batch cannot trade with itself: [{}]",
            deferred.len(),
            deferred.join(", "),
        );
    }
    kept
}

impl EquityLiquidityMaker {
    pub(super) fn reconcile_from_hl_delta(
        &mut self,
        hl_instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        let Some(lightpool_instrument_id) = self.hl_to_lp.get(&hl_instrument_id).copied() else {
            return Ok(());
        };
        self.reconcile_pair(hl_instrument_id, lightpool_instrument_id)
    }

    pub(super) fn reconcile_after_own_cancel(
        &mut self,
        lightpool_instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        if lightpool_instrument_id.venue.as_str() != LIGHTPOOL_VENUE {
            return Ok(());
        }
        let Some(hl_instrument_id) = self
            .hl_to_lp
            .iter()
            .find(|(_, lp_id)| **lp_id == lightpool_instrument_id)
            .map(|(hl_id, _)| *hl_id)
        else {
            return Ok(());
        };
        self.reconcile_pair(hl_instrument_id, lightpool_instrument_id)
    }

    fn reconcile_pair(
        &mut self,
        hl_instrument_id: InstrumentId,
        lightpool_instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        let depth = self.config.depth.max(1);
        let strategy_id = self.core.strategy_id();
        let client_id = self.config.lightpool_client_id;
        let venue = Venue::from(LIGHTPOOL_VENUE);

        let actions = {
            let cache = self.cache();
            let Some(hl_book) = cache.order_book(&hl_instrument_id) else {
                log::warn!("Hyperliquid book missing in cache for {hl_instrument_id}; skip");
                return Ok(());
            };

            let Some(strategy_id) = strategy_id else {
                return Ok(());
            };

            let inflight = cache.orders_inflight(
                Some(&venue),
                Some(&lightpool_instrument_id),
                Some(&strategy_id),
                None,
                None,
            );
            if !inflight.is_empty() {
                let summary: Vec<String> = inflight
                    .iter()
                    .map(|order| format!("{}:{:?}", order.client_order_id(), order.status()))
                    .collect();
                log::info!(
                    "Skip reconcile {lightpool_instrument_id}: {} inflight order(s) [{}]",
                    inflight.len(),
                    summary.join(", "),
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
                .map(|order| order.cloned())
                .collect::<Vec<_>>();

            let filtered = BookSnapshot::from_book_filtered(hl_book, depth);
            let hl_bid_levels = filtered.hl_bid_levels;
            let hl_ask_levels = filtered.hl_ask_levels;
            let dropped_bids = filtered.dropped_bids;
            let dropped_asks = filtered.dropped_asks;
            let reference = filtered.snapshot;
            let actual = BookSnapshot::from_open_orders(&open_orders, depth);
            let ref_bids = reference.bids.levels.len();
            let ref_asks = reference.asks.levels.len();
            let lp_bids = actual.bids.levels.len();
            let lp_asks = actual.asks.levels.len();
            if ref_bids < depth
                || ref_asks < depth
                || lp_bids < depth
                || lp_asks < depth
                || ref_bids != lp_bids
                || ref_asks != lp_asks
            {
                log::info!(
                    "Mirror book depth {hl_instrument_id} → {lightpool_instrument_id}: \
                     depth={depth} hl_top bids={hl_bid_levels} asks={hl_ask_levels} \
                     after_min_size(0.1) bids={ref_bids} asks={ref_asks} \
                     lp_open bids={lp_bids} asks={lp_asks} \
                     dropped_bids=[{dropped_bids}] dropped_asks=[{dropped_asks}]"
                );
            }
            if books_match(&reference, &actual) {
                return Ok(());
            }

            let orders_by_level = OrdersByLevel::from_open_orders(&open_orders);
            let actions = build_reconcile_actions(&reference, &actual, &orders_by_level);
            without_crossing_places(actions, &open_orders)
        };

        if actions.is_empty() {
            return Ok(());
        }

        for action in actions {
            match action {
                ReconcileAction::Cancel { client_order_id } => {
                    if let Err(e) = self.cancel_order(client_order_id, Some(client_id), None) {
                        log::warn!("Failed to cancel {client_order_id}: {e}");
                    }
                }
                ReconcileAction::Modify {
                    client_order_id,
                    quantity,
                } => {
                    if let Err(e) = self.modify_order(
                        client_order_id,
                        Some(quantity),
                        None,
                        None,
                        Some(client_id),
                        None,
                    ) {
                        log::warn!("Failed to modify {client_order_id}: {e}");
                    }
                }
                ReconcileAction::Place {
                    side,
                    price,
                    quantity,
                } => {
                    if quantity.is_zero() {
                        continue;
                    }
                    let order = self.core.order_factory().limit(
                        lightpool_instrument_id,
                        side,
                        quantity,
                        price,
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
                            side,
                            quantity,
                            price,
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

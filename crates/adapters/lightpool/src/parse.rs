use std::str::FromStr;

use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas},
    enums::{AssetClass, BookAction, OrderSide, RecordFlag},
    identifiers::{InstrumentId, Symbol},
    instruments::{BinaryOption, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    common::consts::{LIGHTPOOL_VENUE, LPUSD, MAX_PRICE, MIN_PRICE, PRICE_TICK},
    http::models::{BookLevel, BookSnapshot, Market},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LightpoolOutcome {
    Yes,
    No,
}

impl LightpoolOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Yes => "yes",
            Self::No => "no",
        }
    }
}

use nautilus_model::enums::CurrencyType;

fn get_currency(code: &str) -> Currency {
    Currency::try_from_str(code).unwrap_or_else(|| {
        let currency = Currency::new(code, 6, 0, code, CurrencyType::Crypto);
        if let Err(e) = Currency::register(currency, false) {
            log::error!("Failed to register currency '{code}': {e}");
        }
        currency
    })
}

pub fn create_instrument(
    market: &Market,
    outcome: LightpoolOutcome,
    spot_market: &str,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let suffix = match outcome {
        LightpoolOutcome::Yes => "YES",
        LightpoolOutcome::No => "NO",
    };
    let symbol = Symbol::new(format!("{}-{}", market.slug, suffix));
    let instrument_id = InstrumentId::new(symbol, *LIGHTPOOL_VENUE);
    let raw_symbol = Symbol::new(spot_market);
    let currency = get_currency(LPUSD);
    let price_increment = Price::from(PRICE_TICK);
    let size_increment = Quantity::from("0.000001");

    let outcome_token = match outcome {
        LightpoolOutcome::Yes => market.yes_token.as_str(),
        LightpoolOutcome::No => market.no_token.as_str(),
    };
    let mut info = Params::new();
    info.insert(
        "market_slug".to_string(),
        serde_json::Value::String(market.slug.clone()),
    );
    info.insert(
        "market_id".to_string(),
        serde_json::Value::String(market.id.to_string()),
    );
    info.insert(
        "outcome".to_string(),
        serde_json::Value::String(outcome.as_str().to_string()),
    );
    info.insert(
        "spot_market".to_string(),
        serde_json::Value::String(spot_market.to_string()),
    );
    info.insert(
        "collateral_token".to_string(),
        serde_json::Value::String(market.collateral_token.clone()),
    );
    info.insert(
        "outcome_token".to_string(),
        serde_json::Value::String(outcome_token.to_string()),
    );
    info.insert(
        "question".to_string(),
        serde_json::Value::String(market.question.clone()),
    );

    let binary_option = BinaryOption::new_checked(
        instrument_id,
        raw_symbol,
        AssetClass::Alternative,
        currency,
        UnixNanos::default(),
        UnixNanos::from((market.resolution_deadline as u64) * 1_000_000_000),
        2,
        6,
        price_increment,
        size_increment,
        Some(Ustr::from(outcome.as_str())),
        Some(Ustr::from(&market.question)),
        None,
        None,
        None,
        None,
        Some(Price::from(MAX_PRICE)),
        Some(Price::from(MIN_PRICE)),
        None,
        None,
        None,
        None,
        Some(info),
        ts_init,
        ts_init,
    )?;

    Ok(InstrumentAny::BinaryOption(binary_option))
}

pub fn instruments_for_market(market: &Market, ts_init: UnixNanos) -> anyhow::Result<Vec<InstrumentAny>> {
    Ok(vec![
        create_instrument(market, LightpoolOutcome::Yes, &market.yes_spot_market, ts_init)?,
        create_instrument(market, LightpoolOutcome::No, &market.no_spot_market, ts_init)?,
    ])
}

fn cents_to_decimal(price: &str) -> anyhow::Result<Decimal> {
    let cents: u64 = price
        .trim()
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid cents price '{price}': {e}"))?;
    Ok(Decimal::new(cents as i64, 2) / Decimal::from(100))
}

fn parse_price(price: &str) -> anyhow::Result<Price> {
    let value = cents_to_decimal(price)?;
    Price::from_decimal_dp(value, 2)
        .map_err(|e| anyhow::anyhow!("invalid price '{price}': {e}"))
}

fn parse_quantity(size: &str) -> anyhow::Result<Quantity> {
    let value = Decimal::from_str(size.trim())
        .map_err(|e| anyhow::anyhow!("invalid size '{size}': {e}"))?;
    Quantity::from_decimal_dp(value, 6)
        .map_err(|e| anyhow::anyhow!("invalid size '{size}': {e}"))
}

fn level_to_delta(
    instrument_id: InstrumentId,
    side: OrderSide,
    level: &BookLevel,
    flags: u8,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDelta> {
    let size_value = Decimal::from_str(level.size.trim()).unwrap_or(Decimal::ZERO);
    let action = if size_value.is_zero() {
        BookAction::Delete
    } else {
        BookAction::Update
    };
    let price = parse_price(&level.price)?;
    let size = parse_quantity(&level.size)?;
    let order = BookOrder::new(side, price, size, 0);
    OrderBookDelta::new_checked(
        instrument_id,
        action,
        order,
        flags,
        sequence,
        ts_event,
        ts_init,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))
}

pub fn parse_book_snapshot(
    snapshot: &BookSnapshot,
    instrument_id: InstrumentId,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    let ts_event = ts_init;
    let total = snapshot.bids.len() + snapshot.asks.len();
    let mut deltas = Vec::with_capacity(total + 1);
    deltas.push(OrderBookDelta::clear(instrument_id, snapshot.sequence, ts_event, ts_init));

    let snapshot_flag = RecordFlag::F_SNAPSHOT as u8;
    let mut count = 0usize;

    for level in &snapshot.bids {
        count += 1;
        let mut flags = snapshot_flag;
        if count == total {
            flags |= RecordFlag::F_LAST as u8;
        }
        deltas.push(level_to_delta(
            instrument_id,
            OrderSide::Buy,
            level,
            flags,
            snapshot.sequence,
            ts_event,
            ts_init,
        )?);
    }

    for level in &snapshot.asks {
        count += 1;
        let mut flags = snapshot_flag;
        if count == total {
            flags |= RecordFlag::F_LAST as u8;
        }
        deltas.push(level_to_delta(
            instrument_id,
            OrderSide::Sell,
            level,
            flags,
            snapshot.sequence,
            ts_event,
            ts_init,
        )?);
    }

    Ok(OrderBookDeltas::new(instrument_id, deltas))
}

pub fn parse_book_delta(
    bids: &[BookLevel],
    asks: &[BookLevel],
    instrument_id: InstrumentId,
    sequence: u64,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    let ts_event = ts_init;
    let total = bids.len() + asks.len();
    if total == 0 {
        anyhow::bail!("empty orderbook delta");
    }
    let mut deltas = Vec::with_capacity(total);
    let mut count = 0usize;

    for level in bids {
        count += 1;
        let mut flags = 0u8;
        if count == total {
            flags |= RecordFlag::F_LAST as u8;
        }
        deltas.push(level_to_delta(
            instrument_id,
            OrderSide::Buy,
            level,
            flags,
            sequence,
            ts_event,
            ts_init,
        )?);
    }

    for level in asks {
        count += 1;
        let mut flags = 0u8;
        if count == total {
            flags |= RecordFlag::F_LAST as u8;
        }
        deltas.push(level_to_delta(
            instrument_id,
            OrderSide::Sell,
            level,
            flags,
            sequence,
            ts_event,
            ts_init,
        )?);
    }

    Ok(OrderBookDeltas::new(instrument_id, deltas))
}

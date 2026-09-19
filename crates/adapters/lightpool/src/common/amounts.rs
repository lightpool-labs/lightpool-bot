// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::str::FromStr;

use lightpool_sdk::TOKEN_SCALE;
use nautilus_core::Params;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// How LightPool book / order prices are encoded for an instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PriceUnit {
    /// Event binary markets: API cents string, Nautilus probability in `[0, 1]`.
    #[default]
    Cents,
    /// Equity / currency-pair spots: human decimal (e.g. `331.5`).
    Decimal,
}

impl PriceUnit {
    #[must_use]
    pub fn from_info(info: Option<&Params>) -> Self {
        match info
            .and_then(|params| params.get("price_unit"))
            .and_then(|value| value.as_str())
        {
            Some(unit) if unit.eq_ignore_ascii_case("decimal") => Self::Decimal,
            _ => Self::Cents,
        }
    }
}

pub fn raw_to_decimal(raw: u64) -> Decimal {
    Decimal::from(raw) / Decimal::from(TOKEN_SCALE)
}

pub fn decimal_to_raw_amount(value: Decimal) -> anyhow::Result<u64> {
    value
        .checked_mul(Decimal::from(TOKEN_SCALE))
        .and_then(|scaled| scaled.round().to_u64())
        .ok_or_else(|| anyhow::anyhow!("invalid token amount"))
}

pub fn parse_token_amount_str(value: &str) -> anyhow::Result<u64> {
    let decimal = Decimal::from_str(value.trim())?;
    decimal_to_raw_amount(decimal)
}

pub fn align_raw_to_tick(raw: u64, tick_size: u64) -> u64 {
    if tick_size == 0 {
        return raw;
    }
    ((raw + tick_size / 2) / tick_size) * tick_size
}

pub fn probability_to_limit_price(price: Decimal, tick_size: u64) -> anyhow::Result<u64> {
    if price < Decimal::ZERO || price > Decimal::ONE {
        anyhow::bail!("price must be between 0 and 1");
    }
    let raw = decimal_to_raw_amount(price)?;
    let aligned = align_raw_to_tick(raw, tick_size);
    if aligned == 0 {
        anyhow::bail!("price rounds to zero");
    }
    Ok(aligned)
}

/// Convert a Nautilus limit price to on-chain raw units for the given price unit.
pub fn to_limit_price_raw(
    price: Decimal,
    tick_size: u64,
    unit: PriceUnit,
) -> anyhow::Result<u64> {
    match unit {
        PriceUnit::Cents => probability_to_limit_price(price, tick_size),
        PriceUnit::Decimal => {
            if price <= Decimal::ZERO {
                anyhow::bail!("price must be positive");
            }
            let raw = decimal_to_raw_amount(price)?;
            let aligned = align_raw_to_tick(raw, tick_size);
            if aligned == 0 {
                anyhow::bail!("price rounds to zero");
            }
            Ok(aligned)
        }
    }
}

pub fn format_token_amount(raw: u64) -> String {
    let whole = raw / TOKEN_SCALE;
    let frac = raw % TOKEN_SCALE;
    if frac == 0 {
        return whole.to_string();
    }
    format!("{whole}.{frac:06}", frac = frac)
}

pub fn tick_size_from_instrument_info(info: Option<&Params>) -> u64 {
    info.and_then(|params| params.get("tick_size_raw"))
        .and_then(|value| value.as_u64())
        .filter(|tick| *tick > 0)
        .unwrap_or(crate::common::consts::DEFAULT_TICK_SIZE_RAW)
}

pub fn format_price_pieces(raw: u64) -> String {
    let numerator = raw.saturating_mul(100);
    let whole = numerator / TOKEN_SCALE;
    let frac = numerator % TOKEN_SCALE;
    if frac == 0 {
        return whole.to_string();
    }
    let frac_str = format!("{frac:06}");
    let trimmed = frac_str.trim_end_matches('0');
    format!("{whole}.{trimmed}")
}

pub fn limit_price_string(price: Decimal, tick_size: u64) -> anyhow::Result<String> {
    Ok(format_price_pieces(probability_to_limit_price(price, tick_size)?))
}

/// Format a limit price for clob-index query matching (cents vs human decimal).
pub fn limit_price_string_for_unit(
    price: Decimal,
    tick_size: u64,
    unit: PriceUnit,
) -> anyhow::Result<String> {
    let raw = to_limit_price_raw(price, tick_size, unit)?;
    Ok(match unit {
        PriceUnit::Cents => format_price_pieces(raw),
        PriceUnit::Decimal => format_token_amount(raw),
    })
}

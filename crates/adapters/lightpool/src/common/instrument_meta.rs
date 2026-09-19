// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use nautilus_core::Params;
use nautilus_model::instruments::InstrumentAny;

use super::amounts::PriceUnit;

/// Returns instrument `info` params for BinaryOption and CurrencyPair.
#[must_use]
pub fn instrument_info(instrument: &InstrumentAny) -> Option<&Params> {
    match instrument {
        InstrumentAny::BinaryOption(binary_option) => binary_option.info.as_ref(),
        InstrumentAny::CurrencyPair(pair) => pair.info.as_ref(),
        _ => None,
    }
}

/// Resolve book/order price unit from instrument info (`price_unit=decimal` → equity spot).
#[must_use]
pub fn price_unit_for_instrument(instrument: &InstrumentAny) -> PriceUnit {
    PriceUnit::from_info(instrument_info(instrument))
}

/// Quote / collateral token address for buy locks.
#[must_use]
pub fn quote_token_from_info(info: Option<&Params>) -> Option<&str> {
    info.and_then(|params| {
        params
            .get("quote_token")
            .or_else(|| params.get("collateral_token"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    })
}

/// Base / outcome token address for sell locks.
#[must_use]
pub fn base_token_from_info(info: Option<&Params>) -> Option<&str> {
    info.and_then(|params| {
        params
            .get("base_token")
            .or_else(|| params.get("outcome_token"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    })
}

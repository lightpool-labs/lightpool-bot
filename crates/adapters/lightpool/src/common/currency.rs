// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use nautilus_model::{
    enums::CurrencyType,
    types::Currency,
};

use super::consts::DEFAULT_COLLATERAL_CURRENCY;

fn nonempty_env(var: &str) -> Option<String> {
    std::env::var(var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Collateral currency code for LightPool accounts and instruments.
#[must_use]
pub fn collateral_currency_code() -> String {
    nonempty_env("LIGHTPOOL_COLLATERAL_CURRENCY")
        .unwrap_or_else(|| DEFAULT_COLLATERAL_CURRENCY.to_string())
}

/// Returns the LightPool collateral currency, registering it if needed.
#[must_use]
pub fn collateral_currency() -> Currency {
    register_currency(&collateral_currency_code())
}

/// Register a crypto currency code for Nautilus portfolio accounting.
#[must_use]
pub fn register_currency(code: &str) -> Currency {
    Currency::try_from_str(code).unwrap_or_else(|| {
        let currency = Currency::new(code, 6, 0, code, CurrencyType::Crypto);
        if let Err(e) = Currency::register(currency, false) {
            log::error!("Failed to register currency '{code}': {e}");
        }
        currency
    })
}

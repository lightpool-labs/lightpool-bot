use std::str::FromStr;

use ahash::AHashSet;
use nautilus_common::cache::Cache;
use nautilus_core::Params;
use nautilus_model::{
    instruments::{Instrument, InstrumentAny},
    types::{AccountBalance, Currency},
};
use rust_decimal::Decimal;

use crate::{
    common::{
        consts::LIGHTPOOL_VENUE,
        currency::{collateral_currency, collateral_currency_code, register_currency},
    },
    config::resolve_collateral_token,
    http::{
        clob_index::ClobIndexHttpClient,
        models::{BalanceEntry, BalanceTokenSpec, Market},
    },
};

fn push_token_spec(
    specs: &mut Vec<BalanceTokenSpec>,
    seen: &mut AHashSet<String>,
    symbol: &str,
    address: &str,
) {
    let address = address.trim();
    if address.is_empty() {
        return;
    }
    let key = address.to_ascii_lowercase();
    if seen.insert(key) {
        specs.push(BalanceTokenSpec {
            symbol: symbol.to_string(),
            address: address.to_string(),
        });
    }
}

fn push_market_token_specs(
    specs: &mut Vec<BalanceTokenSpec>,
    seen: &mut AHashSet<String>,
    market: &Market,
) {
    let collateral_symbol = collateral_currency_code();
    push_token_spec(specs, seen, &collateral_symbol, &market.collateral_token);
    push_token_spec(specs, seen, "YES", &market.yes_token);
    push_token_spec(specs, seen, "NO", &market.no_token);
}

fn instrument_info(instrument: &InstrumentAny) -> Option<&Params> {
    match instrument {
        InstrumentAny::BinaryOption(binary_option) => binary_option.info.as_ref(),
        _ => None,
    }
}

fn outcome_symbol_from_instrument(instrument: &InstrumentAny) -> String {
    if let Some(info) = instrument_info(instrument) {
        if let Some(outcome) = info.get("outcome").and_then(|v| v.as_str()) {
            let upper = outcome.to_ascii_uppercase();
            if upper == "YES" || upper == "NO" {
                return upper;
            }
        }
    }

    let instrument_id = instrument.id();
    let symbol = instrument_id.symbol.as_str();
    if let Some(suffix) = symbol.rsplit('-').next() {
        let upper = suffix.to_ascii_uppercase();
        if upper == "YES" || upper == "NO" {
            return upper;
        }
    }

    "OUTCOME".to_string()
}

pub fn collect_balance_token_specs_from_cache(cache: &Cache) -> Vec<BalanceTokenSpec> {
    let mut specs = Vec::new();
    let mut seen = AHashSet::new();
    let collateral_symbol = collateral_currency_code();

    for instrument in cache.instruments(&*LIGHTPOOL_VENUE, None) {
        let Some(info) = instrument_info(instrument) else {
            continue;
        };

        if let Some(collateral) = info.get("collateral_token").and_then(|v| v.as_str()) {
            push_token_spec(&mut specs, &mut seen, &collateral_symbol, collateral);
        }

        if let Some(outcome_token) = info.get("outcome_token").and_then(|v| v.as_str()) {
            let symbol = outcome_symbol_from_instrument(instrument);
            push_token_spec(&mut specs, &mut seen, &symbol, outcome_token);
        }
    }

    specs
}

fn apply_default_collateral_spec(specs: &mut Vec<BalanceTokenSpec>) {
    if !specs.is_empty() {
        return;
    }
    let collateral_symbol = collateral_currency_code();
    let address = resolve_collateral_token();
    let mut seen = AHashSet::new();
    push_token_spec(specs, &mut seen, &collateral_symbol, &address);
}

pub async fn resolve_balance_token_specs(
    clob_client: &ClobIndexHttpClient,
    cache_specs: Vec<BalanceTokenSpec>,
    market_slugs: &[String],
) -> anyhow::Result<Vec<BalanceTokenSpec>> {
    let mut specs = cache_specs;
    if specs.is_empty() && !market_slugs.is_empty() {
        let markets = clob_client.fetch_markets_by_slugs(market_slugs).await?;
        let mut seen: AHashSet<String> = specs
            .iter()
            .map(|spec| spec.address.to_ascii_lowercase())
            .collect();
        for market in &markets {
            push_market_token_specs(&mut specs, &mut seen, market);
        }
    }

    apply_default_collateral_spec(&mut specs);
    Ok(specs)
}

pub fn currency_for_balance_symbol(symbol: &str) -> Currency {
    let collateral = collateral_currency_code();
    if symbol.eq_ignore_ascii_case(&collateral) || symbol.eq_ignore_ascii_case("USDT") {
        collateral_currency()
    } else {
        register_currency(symbol)
    }
}

pub fn parse_balance_entries(entries: &[BalanceEntry]) -> anyhow::Result<Vec<AccountBalance>> {
    let mut balances = Vec::with_capacity(entries.len());

    for entry in entries {
        let total = Decimal::from_str(entry.total.trim())
            .map_err(|e| anyhow::anyhow!("invalid balance total for {}: {e}", entry.symbol))?;
        let locked = Decimal::from_str(entry.locked.trim())
            .map_err(|e| anyhow::anyhow!("invalid balance locked for {}: {e}", entry.symbol))?;
        let currency = currency_for_balance_symbol(&entry.symbol);
        let balance = AccountBalance::from_total_and_locked(total, locked, currency)
            .map_err(|e| anyhow::anyhow!("invalid balance for {}: {e}", entry.symbol))?;
        balances.push(balance);
    }

    if balances.is_empty() {
        let code = collateral_currency_code();
        let zero = nautilus_model::types::Money::from(format!("0 {code}"));
        balances.push(AccountBalance::new(zero.clone(), zero.clone(), zero));
    }

    Ok(balances)
}

pub async fn fetch_account_balances(
    clob_client: &ClobIndexHttpClient,
    cache_specs: Vec<BalanceTokenSpec>,
    market_slugs: &[String],
    user_address: &str,
) -> anyhow::Result<Vec<AccountBalance>> {
    let specs = resolve_balance_token_specs(clob_client, cache_specs, market_slugs).await?;
    if specs.is_empty() {
        anyhow::bail!("no token addresses available for balance refresh");
    }

    let entries = clob_client.get_balances(user_address, &specs).await?;
    parse_balance_entries(&entries)
}

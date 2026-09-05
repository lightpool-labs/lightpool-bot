// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Bootstrap LightPool markets from top-N Polymarket markets in an event.

use anyhow::{Context, Result, bail};
use nautilus_lightpool::{
    common::signer::signer_from_private_key,
    config::{clob_index_http_from_env, resolve_collateral_token, resolve_private_key},
    http::bootstrap::{BootstrappedMarket, bootstrap_one_market},
    http::clob_index::ClobIndexHttpClient,
};
use nautilus_network::retry::RetryConfig;
use nautilus_polymarket::{
    config::proxy_url_from_env,
    http::{
        gamma::PolymarketGammaHttpClient,
        models::GammaMarket,
        query::GetGammaMarketsParams,
    },
};

/// One Polymarket condition paired with a LightPool market slug.
#[derive(Debug, Clone)]
pub struct MarketPair {
    pub condition_id: String,
    pub question: String,
    pub lightpool_slug: String,
    pub market_address: String,
}

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub polymarket_event_slug: String,
    pub max_markets: u32,
    pub mint_amount: u64,
    pub order_field: String,
    /// Skip markets where any Yes/No outcome price is >= this value (0–1 scale).
    /// Default 0.96 (= 96¢) keeps only still-active markets.
    pub max_outcome_price: f64,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            polymarket_event_slug: String::new(),
            max_markets: 5,
            mint_amount: 1_000_000_000_000_000, // 1e9 tokens at 6 decimals
            order_field: "liquidity".into(),
            max_outcome_price: 0.96,
        }
    }
}

fn parse_resolution_deadline(end_date: Option<&str>) -> u64 {
    // Prefer ISO date; fall back to end of 2026.
    const FALLBACK: u64 = 1_798_761_599; // 2026-12-31T23:59:59Z approx
    let Some(raw) = end_date.map(str::trim).filter(|s| !s.is_empty()) else {
        return FALLBACK;
    };
    // Accept "2026-12-31T23:59:59Z" or with fractional seconds.
    let cleaned = raw.trim_end_matches('Z');
    let cleaned = cleaned.split('.').next().unwrap_or(cleaned);
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(cleaned, "%Y-%m-%dT%H:%M:%S") {
        return dt.and_utc().timestamp().max(0) as u64;
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return d
            .and_hms_opt(23, 59, 59)
            .map(|dt| dt.and_utc().timestamp().max(0) as u64)
            .unwrap_or(FALLBACK);
    }
    FALLBACK
}

fn parse_outcome_prices(raw: &Option<String>) -> Option<Vec<f64>> {
    let raw = raw.as_ref()?.trim();
    let body = raw
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))?
        .trim();
    if body.is_empty() {
        return None;
    }
    let mut values = Vec::new();
    for part in body.split(',') {
        let token = part.trim().trim_matches('"').trim_matches('\'');
        values.push(token.parse::<f64>().ok()?);
    }
    (!values.is_empty()).then_some(values)
}

fn normalize_outcome_price(price: f64) -> f64 {
    // Gamma usually returns 0–1; accept cents (e.g. 96) as well.
    if price > 1.5 {
        price / 100.0
    } else {
        price
    }
}

/// Still-active market: every Yes/No outcome price is strictly below `max_price` (0–1).
fn is_still_active_market(market: &GammaMarket, max_price: f64) -> bool {
    let Some(prices) = parse_outcome_prices(&market.outcome_prices) else {
        return false;
    };
    if prices.is_empty() {
        return false;
    }
    prices
        .iter()
        .copied()
        .map(normalize_outcome_price)
        .all(|price| price < max_price)
}

async fn fetch_top_polymarket_markets(
    event_slug: &str,
    max_markets: u32,
    order_field: &str,
    max_outcome_price: f64,
) -> Result<Vec<GammaMarket>> {
    let proxy = proxy_url_from_env().or_else(|| Some("http://127.0.0.1:8118".into()));
    let client = PolymarketGammaHttpClient::new_with_proxy(
        None,
        proxy,
        30,
        RetryConfig::default(),
    )
    .context("create Polymarket Gamma HTTP client")?;

    // Fetch full sorted list first, then filter near-final markets, then take top-N.
    let params = GetGammaMarketsParams {
        active: Some(true),
        closed: Some(false),
        archived: Some(false),
        order: Some(order_field.to_string()),
        ascending: Some(false),
        max_markets: None,
        ..Default::default()
    };

    let markets = client
        .request_gamma_markets_by_event_query(event_slug, params)
        .await
        .with_context(|| format!("fetch Polymarket markets for event '{event_slug}'"))?;

    if markets.is_empty() {
        bail!("no Polymarket markets found for event '{event_slug}'");
    }

    let mut active = Vec::new();
    for market in markets {
        if is_still_active_market(&market, max_outcome_price) {
            active.push(market);
        } else {
            log::info!(
                "skip near-final PM market condition={} prices={:?} (need all < {max_outcome_price})",
                market.condition_id,
                market.outcome_prices,
            );
        }
        if active.len() >= max_markets as usize {
            break;
        }
    }

    if active.is_empty() {
        bail!(
            "no still-active Polymarket markets for event '{event_slug}' \
             (all outcomes must be < {max_outcome_price})"
        );
    }
    Ok(active)
}

/// Fetch top-N Polymarket markets, create+mint matching LightPool markets, return pairs.
pub async fn bootstrap_markets_from_polymarket(
    config: &BootstrapConfig,
) -> Result<Vec<MarketPair>> {
    let private_key = resolve_private_key().context("resolve LIGHTPOOL_PRIVATE_KEY")?;
    let signer = signer_from_private_key(&private_key)?;
    let collateral = resolve_collateral_token();
    let clob = ClobIndexHttpClient::new(clob_index_http_from_env());

    let pm_markets = fetch_top_polymarket_markets(
        &config.polymarket_event_slug,
        config.max_markets,
        &config.order_field,
        config.max_outcome_price,
    )
    .await?;

    log::info!(
        "Bootstrapping {} LightPool markets from Polymarket event '{}' \
         (mint_amount={}, max_outcome_price={})",
        pm_markets.len(),
        config.polymarket_event_slug,
        config.mint_amount,
        config.max_outcome_price,
    );

    let mut pairs = Vec::with_capacity(pm_markets.len());
    for (idx, pm) in pm_markets.iter().enumerate() {
        let question = pm.question.trim();
        if question.is_empty() {
            log::warn!("skip PM market {}: empty question", pm.condition_id);
            continue;
        }
        let deadline = parse_resolution_deadline(pm.end_date.as_deref());
        log::info!(
            "[{}/{}] create+mint LP market for condition={} question={question}",
            idx + 1,
            pm_markets.len(),
            pm.condition_id,
        );

        let created: BootstrappedMarket = bootstrap_one_market(
            &clob,
            &signer,
            question,
            &collateral,
            deadline,
            config.mint_amount,
        )
        .await
        .with_context(|| {
            format!(
                "bootstrap LightPool market for condition={}",
                pm.condition_id
            )
        })?;

        log::info!(
            "indexed LightPool market slug={} address={}",
            created.slug,
            created.market_address,
        );

        pairs.push(MarketPair {
            condition_id: pm.condition_id.clone(),
            question: question.to_string(),
            lightpool_slug: created.slug,
            market_address: created.market_address,
        });
    }

    if pairs.is_empty() {
        bail!("bootstrap produced zero LightPool markets");
    }
    Ok(pairs)
}

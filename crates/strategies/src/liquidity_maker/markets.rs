//! Slug-to-market id mapping built from Polymarket and LightPool instrument caches.

use ahash::{AHashMap, AHashSet};
use nautilus_common::cache::Cache;
use nautilus_model::{
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};

/// YES/NO instrument ids for a single prediction market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlugMarketIds {
    /// Polymarket condition id or LightPool market slug.
    pub condition_id: String,
    pub yes_id: InstrumentId,
    pub no_id: InstrumentId,
}

/// Builds paired YES/NO markets from Polymarket binary options in cache.
pub fn discover_markets_from_cache(cache: &Cache) -> AHashMap<String, SlugMarketIds> {
    let venue = Venue::from("POLYMARKET");
    let mut by_condition: AHashMap<String, (Option<InstrumentId>, Option<InstrumentId>)> =
        AHashMap::new();

    for instrument in cache.instruments(&venue, None) {
        let InstrumentAny::BinaryOption(opt) = instrument else {
            continue;
        };
        let id = instrument.id();
        let sym = id.symbol.as_str();
        let Some((condition_id, _token_id)) = sym.split_once('-') else {
            continue;
        };
        let outcome = opt
            .outcome
            .map(|v| v.to_string().to_lowercase())
            .unwrap_or_default();
        let entry = by_condition
            .entry(condition_id.to_string())
            .or_insert((None, None));
        match outcome.as_str() {
            "yes" | "up" => entry.0 = Some(id),
            "no" | "down" => entry.1 = Some(id),
            _ => {}
        }
    }

    let mut markets = AHashMap::new();
    for (condition_id, (yes_id, no_id)) in by_condition {
        let (Some(yes_id), Some(no_id)) = (yes_id, no_id) else {
            continue;
        };
        markets.insert(
            condition_id.clone(),
            SlugMarketIds {
                condition_id,
                yes_id,
                no_id,
            },
        );
    }
    markets
}

/// Builds paired YES/NO markets from LightPool binary options in cache.
pub fn discover_lightpool_markets_from_cache(cache: &Cache) -> AHashMap<String, SlugMarketIds> {
    let venue = Venue::from("LIGHTPOOL");
    let mut by_slug: AHashMap<String, (Option<InstrumentId>, Option<InstrumentId>)> =
        AHashMap::new();

    for instrument in cache.instruments(&venue, None) {
        let InstrumentAny::BinaryOption(opt) = instrument else {
            continue;
        };
        let id = instrument.id();
        let market_slug = opt
            .info
            .as_ref()
            .and_then(|params| params.get("market_slug"))
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| market_slug_from_symbol(id.symbol.as_str()));
        let outcome = opt
            .outcome
            .map(|v| v.to_string().to_lowercase())
            .unwrap_or_default();
        let entry = by_slug
            .entry(market_slug)
            .or_insert((None, None));
        match outcome.as_str() {
            "yes" => entry.0 = Some(id),
            "no" => entry.1 = Some(id),
            _ => {}
        }
    }

    let mut markets = AHashMap::new();
    for (market_slug, (yes_id, no_id)) in by_slug {
        let (Some(yes_id), Some(no_id)) = (yes_id, no_id) else {
            continue;
        };
        markets.insert(
            market_slug.clone(),
            SlugMarketIds {
                condition_id: market_slug,
                yes_id,
                no_id,
            },
        );
    }
    markets
}

fn market_slug_from_symbol(symbol: &str) -> String {
    symbol
        .strip_suffix("-YES")
        .or_else(|| symbol.strip_suffix("-NO"))
        .unwrap_or(symbol)
        .to_string()
}

/// Assigns discovered LightPool markets to configured slugs.
pub fn assign_lightpool_markets_to_slugs(
    slugs: &[String],
    discovered: &AHashMap<String, SlugMarketIds>,
    slug_markets: &mut AHashMap<String, SlugMarketIds>,
) -> usize {
    let mut new_count = 0;
    for slug in slugs {
        let Some(market) = discovered.get(slug) else {
            continue;
        };
        if slug_markets.insert(slug.clone(), market.clone()).is_none() {
            new_count += 1;
        }
    }
    new_count
}

/// Assigns discovered markets to configured event slugs.
pub fn assign_markets_to_slugs(
    slugs: &[String],
    discovered: &AHashMap<String, SlugMarketIds>,
    slug_markets: &mut AHashMap<String, AHashMap<String, SlugMarketIds>>,
    slug_to_conditions: &mut AHashMap<String, AHashSet<String>>,
) -> usize {
    if slugs.is_empty() {
        return 0;
    }

    let target_slugs: Vec<&String> = if slugs.len() == 1 {
        vec![&slugs[0]]
    } else {
        log::warn!(
            "Multiple slugs configured; cache markets are assigned to the first slug only"
        );
        vec![&slugs[0]]
    };

    let mut new_count = 0;
    for slug in target_slugs {
        let conditions = slug_to_conditions.entry(slug.clone()).or_default();
        let markets_for_slug = slug_markets.entry(slug.clone()).or_default();
        for (condition_id, market) in discovered {
            if markets_for_slug
                .insert(condition_id.clone(), market.clone())
                .is_none()
            {
                conditions.insert(condition_id.clone());
                new_count += 1;
            }
        }
    }
    new_count
}

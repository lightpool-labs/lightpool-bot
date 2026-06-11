//! Slug-to-market id mapping built from the Polymarket instrument cache.

use ahash::{AHashMap, AHashSet};
use nautilus_common::cache::Cache;
use nautilus_model::{
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};

/// YES/NO instrument ids for a single Polymarket condition market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlugMarketIds {
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

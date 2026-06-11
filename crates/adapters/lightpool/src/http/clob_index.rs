use reqwest::Client;

use super::models::{BookSnapshot, Market, MarketsPage, decode_response};

#[derive(Debug, Clone)]
pub struct ClobIndexHttpClient {
    client: Client,
    base_url: String,
}

impl ClobIndexHttpClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    pub async fn get_market_by_slug(&self, slug: &str) -> anyhow::Result<Market> {
        let url = format!("{}/api/markets/slug/{slug}", self.base_url);
        let response = self.client.get(&url).send().await?;
        decode_response(response).await
    }

    pub async fn fetch_book_snapshot(
        &self,
        spot_market: &str,
        depth: u32,
    ) -> anyhow::Result<BookSnapshot> {
        let url = format!(
            "{}/api/spot/{spot_market}/book?depth={depth}",
            self.base_url
        );
        let response = self.client.get(&url).send().await?;
        decode_response(response).await
    }

    pub async fn fetch_markets_by_slugs(&self, slugs: &[String]) -> anyhow::Result<Vec<Market>> {
        if slugs.is_empty() {
            return Ok(Vec::new());
        }
        let joined = slugs.join(",");
        let url = format!(
            "{}/api/markets?slugs={joined}&limit={}",
            self.base_url,
            slugs.len()
        );
        let response = self.client.get(&url).send().await?;
        let page: MarketsPage = decode_response(response).await?;
        Ok(page.markets)
    }
}

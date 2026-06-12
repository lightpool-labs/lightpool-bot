use reqwest::Client;

use super::models::{
    BookSnapshot, Market, MarketsPage, SubmitTxRequest, SubmitTxResponse, decode_response,
};
use lightpool_sdk::{
    lightpool_types::SignedTransaction,
    types::SubmitTransactionResponse,
};

#[derive(Debug, Clone)]
pub struct ClobIndexHttpClient {
    client: Client,
    base_url: String,
}

impl ClobIndexHttpClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let client = Client::builder()
            .http1_only()
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            client,
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

    pub async fn fetch_spot_info(&self, spot_market: &str) -> anyhow::Result<super::models::SpotMarketInfo> {
        let url = format!(
            "{}/api/spot/{spot_market}/info?account=0x0000000000000000000000000000000000000000",
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

    pub async fn submit_transaction(
        &self,
        tx: SignedTransaction,
    ) -> anyhow::Result<SubmitTransactionResponse> {
        let url = format!("{}/api/tx/submit", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&SubmitTxRequest { tx })
            .send()
            .await?;
        let body: SubmitTxResponse = decode_response(response).await?;
        Ok(SubmitTransactionResponse {
            digest: body.digest,
            receipt: body.receipt,
        })
    }
}

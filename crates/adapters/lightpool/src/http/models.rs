use serde::{Deserialize, Serialize};
use uuid::Uuid;

use lightpool_sdk::lightpool_types::SignedTransaction;
use lightpool_sdk::TransactionReceipt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub sequence: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    #[serde(default)]
    pub last_trade_price: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotMarketInfo {
    pub last_price: Option<String>,
    pub state: String,
    pub min_order_size: String,
    pub tick_size: String,
    pub maker_fee_bps: u16,
    pub taker_fee_bps: u16,
    pub allow_market_orders: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub id: Uuid,
    pub slug: String,
    pub question: String,
    #[serde(default)]
    pub icon_url: Option<String>,
    pub market_address: String,
    pub collateral_token: String,
    pub yes_token: String,
    pub no_token: String,
    pub yes_spot_market: String,
    pub no_spot_market: String,
    pub state: String,
    pub resolution_deadline: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketsPage {
    pub markets: Vec<Market>,
    pub total: usize,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Serialize)]
pub struct SubmitTxRequest {
    pub tx: SignedTransaction,
}

#[derive(Debug, Deserialize)]
pub struct SubmitTxResponse {
    pub digest: String,
    pub receipt: TransactionReceipt,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: String,
}

pub(crate) async fn decode_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> anyhow::Result<T> {
    let status = response.status();
    if status.is_success() {
        return Ok(response.json::<T>().await?);
    }
    let body = response
        .json::<ErrorBody>()
        .await
        .unwrap_or(ErrorBody {
            error: "unknown clob-index error".into(),
        });
    anyhow::bail!("clob-index HTTP {status}: {}", body.error);
}

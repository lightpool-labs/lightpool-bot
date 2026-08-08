// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use reqwest::Client;

use nautilus_core::{Params, time::get_atomic_clock_realtime};
use nautilus_model::{
    enums::{OrderSide as NautilusOrderSide, OrderType, TimeInForce as NautilusTimeInForce},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};

use super::models::{
    BalanceEntry, BalanceTokenSpec, BookSnapshot, CancelContextResponse, IndexedOrder, Market,
    MarketsPage, MarkCancelledResponse, OrderQueryResponse, SubmitTxRequest, SubmitTxResponse,
    decode_response,
};
use crate::{
    common::{
        amounts::{
            decimal_to_raw_amount, parse_token_amount_str, probability_to_limit_price,
            tick_size_from_instrument_info,
        },
        consts::DEFAULT_TICK_SIZE_RAW,
        signer::signer_from_private_key,
    },
    config::{
        clob_index_http_connect_timeout_secs_from_env, clob_index_http_from_env,
        clob_index_http_timeout_secs_from_env, resolve_private_key,
    },
    parse::instruments_for_market,
};
use lightpool_sdk::{
    spot_events::extract_order_id_from_events, ActionBuilder, BurnEventContractParams,
    CancelOrderParams, MintEventContractParams, OrderParamsType, OrderSide, PlaceOrderParams,
    Signer, TimeInForce, TransactionBuilder, lightpool_types::SignedTransaction,
    parse_token_contract, types::SubmitTransactionResponse,
};
use rust_decimal::Decimal;

#[derive(Clone)]
pub struct ClobIndexHttpClient {
    client: Client,
    base_url: String,
    signer: Option<Arc<Signer>>,
    instruments: Arc<Mutex<HashMap<InstrumentId, InstrumentAny>>>,
}

impl std::fmt::Debug for ClobIndexHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClobIndexHttpClient")
            .field("base_url", &self.base_url)
            .field("has_signer", &self.signer.is_some())
            .field(
                "cached_instruments",
                &self
                    .instruments
                    .lock()
                    .map(|guard| guard.len())
                    .unwrap_or(0),
            )
            .finish()
    }
}

impl ClobIndexHttpClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let connect_timeout_secs = clob_index_http_connect_timeout_secs_from_env();
        let timeout_secs = clob_index_http_timeout_secs_from_env();
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let client = Client::builder()
            .http1_only()
            .connect_timeout(Duration::from_secs(connect_timeout_secs))
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .unwrap_or_else(|_| Client::new());
        log::info!(
            "ClobIndexHttpClient base_url={base_url} connect_timeout_secs={connect_timeout_secs} \
             request_timeout_secs={timeout_secs}"
        );
        Self {
            client,
            base_url,
            signer: None,
            instruments: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build a client from env (`LIGHTPOOL_CLOB_INDEX_HTTP` + private key).
    pub fn from_env() -> anyhow::Result<Self> {
        let mut client = Self::new(clob_index_http_from_env());
        let private_key = resolve_private_key()?;
        client.set_signer(signer_from_private_key(&private_key)?);
        Ok(client)
    }

    pub fn set_signer(&mut self, signer: Signer) {
        self.signer = Some(Arc::new(signer));
    }

    pub fn with_signer(mut self, signer: Signer) -> Self {
        self.set_signer(signer);
        self
    }

    pub fn get_user_address(&self) -> anyhow::Result<String> {
        let signer = self.require_signer()?;
        Ok(signer.address().to_string())
    }

    pub fn cache_instrument(&self, instrument: InstrumentAny) {
        let id = instrument.id();
        if let Ok(mut guard) = self.instruments.lock() {
            guard.insert(id, instrument);
        }
    }

    pub fn cache_instruments(&self, instruments: impl IntoIterator<Item = InstrumentAny>) {
        if let Ok(mut guard) = self.instruments.lock() {
            for instrument in instruments {
                guard.insert(instrument.id(), instrument);
            }
        }
    }

    pub fn get_instrument(&self, instrument_id: &InstrumentId) -> Option<InstrumentAny> {
        self.instruments
            .lock()
            .ok()
            .and_then(|guard| guard.get(instrument_id).cloned())
    }

    fn require_signer(&self) -> anyhow::Result<Arc<Signer>> {
        self.signer
            .clone()
            .ok_or_else(|| anyhow::anyhow!("signer not set; use from_env() or set_signer()"))
    }

    fn require_instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        self.get_instrument(&instrument_id).ok_or_else(|| {
            anyhow::anyhow!(
                "instrument not found in cache: {instrument_id}. Ensure instruments are loaded."
            )
        })
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

    pub async fn fetch_spot_info(
        &self,
        spot_market: &str,
    ) -> anyhow::Result<super::models::SpotMarketInfo> {
        let url = format!(
            "{}/api/spot/{spot_market}/info?account=0x0000000000000000000000000000000000000000",
            self.base_url
        );
        let response = self.client.get(&url).send().await?;
        decode_response(response).await
    }

    pub async fn get_balances(
        &self,
        address: &str,
        tokens: &[BalanceTokenSpec],
    ) -> anyhow::Result<Vec<BalanceEntry>> {
        let address = address.trim();
        let url = format!("{}/api/accounts/{address}/balances", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&super::models::BalancesRequest {
                tokens: tokens.to_vec(),
            })
            .send()
            .await?;
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

    /// Fetch markets page-by-page from clob-index.
    pub async fn fetch_all_markets(&self) -> anyhow::Result<Vec<Market>> {
        let mut markets = Vec::new();
        let mut offset = 0u32;
        let limit = 100u32;
        loop {
            let url = format!(
                "{}/api/markets?limit={limit}&offset={offset}",
                self.base_url
            );
            let response = self.client.get(&url).send().await?;
            let page: MarketsPage = decode_response(response).await?;
            let batch_len = page.markets.len();
            markets.extend(page.markets);
            offset = offset.saturating_add(batch_len as u32);
            if batch_len == 0 || offset as usize >= page.total || batch_len < limit as usize {
                break;
            }
        }
        Ok(markets)
    }

    async fn resolve_tick_size_raw(&self, spot_market: &str) -> u64 {
        match self.fetch_spot_info(spot_market).await {
            Ok(info) => match parse_token_amount_str(&info.tick_size) {
                Ok(raw) if raw > 0 => raw,
                Ok(_) => DEFAULT_TICK_SIZE_RAW,
                Err(_) => DEFAULT_TICK_SIZE_RAW,
            },
            Err(_) => DEFAULT_TICK_SIZE_RAW,
        }
    }

    /// Fetch and parse all LightPool instruments from clob-index markets.
    pub async fn request_instruments(&self) -> anyhow::Result<Vec<InstrumentAny>> {
        self.request_instruments_for_slugs(&[]).await
    }

    /// Fetch and parse LightPool instruments for the given market slugs.
    ///
    /// When `slugs` is empty, loads all markets.
    pub async fn request_instruments_for_slugs(
        &self,
        slugs: &[String],
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let markets = if slugs.is_empty() {
            self.fetch_all_markets().await?
        } else {
            self.fetch_markets_by_slugs(slugs).await?
        };
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        let mut instruments = Vec::with_capacity(markets.len().saturating_mul(2));
        for market in markets {
            let yes_tick = self.resolve_tick_size_raw(&market.yes_spot_market).await;
            let no_tick = self.resolve_tick_size_raw(&market.no_spot_market).await;
            instruments.extend(instruments_for_market(&market, yes_tick, no_tick, ts_init)?);
        }
        Ok(instruments)
    }

    pub async fn fetch_markets_by_addresses(
        &self,
        addresses: &[String],
    ) -> anyhow::Result<Vec<Market>> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }
        let joined = addresses.join(",");
        let url = format!(
            "{}/api/markets?market_addresses={joined}&limit={}",
            self.base_url,
            addresses.len().max(1)
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

    /// Low-level place-order: build action, sign, submit.
    ///
    /// Returns `(digest, chain_order_id)`.
    pub async fn submit_order_params(
        &self,
        signer: &Signer,
        spot_market: &str,
        params: PlaceOrderParams,
    ) -> anyhow::Result<(String, u64)> {
        let spot = parse_token_contract(spot_market)
            .map_err(|e| anyhow::anyhow!("invalid spot market: {e}"))?;
        let action = ActionBuilder::place_order(spot, params)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let tx = TransactionBuilder::new()
            .sender(signer.address())
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_sign_only(signer)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let response = self.submit_transaction(tx).await?;
        if !response.receipt.is_success() {
            anyhow::bail!("place_order failed: {:?}", response.receipt.status);
        }
        let chain_order_id = extract_order_id_from_events(&response.receipt)
            .ok_or_else(|| anyhow::anyhow!("order_created event missing from receipt"))?;
        Ok((response.digest, chain_order_id))
    }

    /// Low-level cancel-order: build action, sign, submit.
    ///
    /// Returns the transaction digest.
    pub async fn cancel_order_params(
        &self,
        signer: &Signer,
        spot_market: &str,
        chain_order_id: u64,
    ) -> anyhow::Result<String> {
        let spot = parse_token_contract(spot_market)
            .map_err(|e| anyhow::anyhow!("invalid spot market: {e}"))?;
        let action = ActionBuilder::cancel_order(
            spot,
            CancelOrderParams {
                order_id: chain_order_id,
            },
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let tx = TransactionBuilder::new()
            .sender(signer.address())
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_sign_only(signer)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let response = self.submit_transaction(tx).await?;
        if !response.receipt.is_success() {
            anyhow::bail!("cancel_order failed: {:?}", response.receipt.status);
        }
        Ok(response.digest)
    }

    /// Place an order using cached instrument metadata and the client signer.
    ///
    /// Returns `(digest, chain_order_id)`.
    pub async fn submit_order(
        &self,
        instrument_id: InstrumentId,
        order_side: NautilusOrderSide,
        order_type: OrderType,
        quantity: Quantity,
        time_in_force: NautilusTimeInForce,
        price: Option<Price>,
    ) -> anyhow::Result<(String, u64)> {
        let signer = self.require_signer()?;
        let instrument = self.require_instrument(instrument_id)?;
        let spot_market = instrument.raw_symbol().to_string();
        let info = instrument_info(&instrument);
        let tick_size = tick_size_from_instrument_info(info);

        let side = match order_side {
            NautilusOrderSide::Buy => OrderSide::Buy,
            NautilusOrderSide::Sell => OrderSide::Sell,
            other => anyhow::bail!("unsupported order side: {other:?}"),
        };

        let amount = decimal_to_raw_amount(quantity.as_decimal())?;
        if amount == 0 {
            anyhow::bail!("order size must be greater than 0");
        }

        let order_type = match order_type {
            OrderType::Limit => {
                let tif = match time_in_force {
                    NautilusTimeInForce::Gtc => TimeInForce::GTC,
                    NautilusTimeInForce::Ioc => TimeInForce::IOC,
                    NautilusTimeInForce::Fok => TimeInForce::FOK,
                    other => anyhow::bail!("unsupported time in force: {other:?}"),
                };
                let Some(price) = price else {
                    anyhow::bail!("limit orders require a price");
                };
                let limit_price =
                    probability_to_limit_price(price.as_decimal(), tick_size)?;
                (
                    OrderParamsType::Limit { tif },
                    limit_price,
                )
            }
            other => anyhow::bail!("unsupported order type: {other:?}"),
        };
        let (order_type, limit_price) = order_type;

        let token_address = token_address_for_side(&instrument, side, &spot_market)?;
        let params = PlaceOrderParams {
            side,
            amount,
            order_type,
            limit_price,
            token_address,
        };
        self.submit_order_params(signer.as_ref(), &spot_market, params)
            .await
    }

    /// Cancel an order using cached instrument metadata and the client signer.
    ///
    /// Returns the transaction digest.
    pub async fn cancel_order(
        &self,
        instrument_id: InstrumentId,
        chain_order_id: u64,
    ) -> anyhow::Result<String> {
        let signer = self.require_signer()?;
        let instrument = self.require_instrument(instrument_id)?;
        let spot_market = instrument.raw_symbol().to_string();
        self.cancel_order_params(signer.as_ref(), &spot_market, chain_order_id)
            .await
    }

    /// Mint a complete set (collateral → YES + NO). LightPool equivalent of HL `splitOutcome`.
    ///
    /// Returns the transaction digest.
    pub async fn submit_mint_outcome(
        &self,
        market: &Market,
        amount: Decimal,
    ) -> anyhow::Result<String> {
        let signer = self.require_signer()?;
        let amount_raw = decimal_to_raw_amount(amount)?;
        if amount_raw == 0 {
            anyhow::bail!("mint amount must be greater than 0");
        }
        let market_address = parse_token_contract(&market.market_address)
            .map_err(|e| anyhow::anyhow!("invalid market address: {e}"))?;
        let params = MintEventContractParams {
            amount: amount_raw,
            collateral_token: parse_token_contract(&market.collateral_token)
                .map_err(|e| anyhow::anyhow!("invalid collateral token: {e}"))?,
            yes_token: parse_token_contract(&market.yes_token)
                .map_err(|e| anyhow::anyhow!("invalid yes token: {e}"))?,
            no_token: parse_token_contract(&market.no_token)
                .map_err(|e| anyhow::anyhow!("invalid no token: {e}"))?,
        };
        let action = ActionBuilder::mint_event_contract(market_address, params)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let tx = TransactionBuilder::new()
            .sender(signer.address())
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_sign_only(signer.as_ref())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let response = self.submit_transaction(tx).await?;
        if !response.receipt.is_success() {
            anyhow::bail!("mint_event_contract failed: {:?}", response.receipt.status);
        }
        Ok(response.digest)
    }

    /// Burn a complete set (YES + NO → collateral). LightPool equivalent of HL `mergeOutcome`.
    ///
    /// Returns the transaction digest.
    pub async fn submit_burn_outcome(
        &self,
        market: &Market,
        amount: Decimal,
    ) -> anyhow::Result<String> {
        let signer = self.require_signer()?;
        let amount_raw = decimal_to_raw_amount(amount)?;
        if amount_raw == 0 {
            anyhow::bail!("burn amount must be greater than 0");
        }
        let market_address = parse_token_contract(&market.market_address)
            .map_err(|e| anyhow::anyhow!("invalid market address: {e}"))?;
        let params = BurnEventContractParams {
            amount: amount_raw,
            collateral_token: parse_token_contract(&market.collateral_token)
                .map_err(|e| anyhow::anyhow!("invalid collateral token: {e}"))?,
            yes_token: parse_token_contract(&market.yes_token)
                .map_err(|e| anyhow::anyhow!("invalid yes token: {e}"))?,
            no_token: parse_token_contract(&market.no_token)
                .map_err(|e| anyhow::anyhow!("invalid no token: {e}"))?,
        };
        let action = ActionBuilder::burn_event_contract(market_address, params)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let tx = TransactionBuilder::new()
            .sender(signer.address())
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_sign_only(signer.as_ref())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let response = self.submit_transaction(tx).await?;
        if !response.receipt.is_success() {
            anyhow::bail!("burn_event_contract failed: {:?}", response.receipt.status);
        }
        Ok(response.digest)
    }

    /// List indexed orders for a user (`GET /api/orders?user_address=`).
    pub async fn list_orders(&self, user_address: &str) -> anyhow::Result<Vec<IndexedOrder>> {
        let mut url = reqwest::Url::parse(&format!("{}/api/orders", self.base_url))?;
        url.query_pairs_mut()
            .append_pair("user_address", user_address.trim());
        let response = self.client.get(url).send().await?;
        decode_response(response).await
    }

    pub async fn query_order(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        user_address: Option<&str>,
    ) -> anyhow::Result<Option<OrderQueryResponse>> {
        let mut url = reqwest::Url::parse(&format!("{}/api/orders/query", self.base_url))?;
        url.query_pairs_mut()
            .append_pair("spot_market", spot_market)
            .append_pair("chain_order_id", chain_order_id);
        if let Some(user_address) = user_address {
            url.query_pairs_mut()
                .append_pair("user_address", user_address);
        }

        let response = self.client.get(url).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(decode_response(response).await?))
    }

    pub async fn query_open_order_match(
        &self,
        spot_market: &str,
        user_address: &str,
        side: &str,
        price: &str,
        size_raw: u64,
    ) -> anyhow::Result<Option<OrderQueryResponse>> {
        let mut url = reqwest::Url::parse(&format!("{}/api/orders/query", self.base_url))?;
        url.query_pairs_mut()
            .append_pair("spot_market", spot_market)
            .append_pair("user_address", user_address)
            .append_pair("side", side)
            .append_pair("price", price)
            .append_pair("size_raw", &size_raw.to_string());

        let response = self.client.get(url).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(decode_response(response).await?))
    }

    /// Fetch cancel context for an open order (`GET /api/orders/:id/cancel-context`).
    pub async fn fetch_cancel_context(
        &self,
        order_id: uuid::Uuid,
        user_address: &str,
    ) -> anyhow::Result<Option<CancelContextResponse>> {
        let mut url = reqwest::Url::parse(&format!(
            "{}/api/orders/{order_id}/cancel-context",
            self.base_url
        ))?;
        url.query_pairs_mut()
            .append_pair("user_address", user_address.trim());
        let response = self.client.get(url).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(decode_response(response).await?))
    }

    /// Mark an indexed order cancelled (`POST /api/orders/:id/cancelled`).
    pub async fn mark_order_cancelled(
        &self,
        order_id: uuid::Uuid,
        user_address: &str,
    ) -> anyhow::Result<MarkCancelledResponse> {
        let mut url = reqwest::Url::parse(&format!(
            "{}/api/orders/{order_id}/cancelled",
            self.base_url
        ))?;
        url.query_pairs_mut()
            .append_pair("user_address", user_address.trim());
        let response = self.client.post(url).send().await?;
        decode_response(response).await
    }
}

fn instrument_info(instrument: &InstrumentAny) -> Option<&Params> {
    match instrument {
        InstrumentAny::BinaryOption(binary_option) => binary_option.info.as_ref(),
        _ => None,
    }
}

fn token_address_for_side(
    instrument: &InstrumentAny,
    side: OrderSide,
    spot_market: &str,
) -> anyhow::Result<lightpool_sdk::ContractAddress> {
    let info = instrument_info(instrument);
    if side == OrderSide::Buy {
        let collateral = info
            .and_then(|params| params.get("collateral_token"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        parse_token_contract(collateral)
            .or_else(|_| parse_token_contract(spot_market))
            .map_err(|e| anyhow::anyhow!("missing collateral token for buy order: {e}"))
    } else {
        let outcome_token = info
            .and_then(|params| params.get("outcome_token"))
            .and_then(|v| v.as_str())
            .unwrap_or(spot_market);
        parse_token_contract(outcome_token)
            .map_err(|e| anyhow::anyhow!("missing outcome token for sell order: {e}"))
    }
}

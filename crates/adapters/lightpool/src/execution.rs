// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use async_trait::async_trait;
use lightpool_sdk::{
    ActionBuilder, OrderParamsType, OrderSide, PlaceOrderParams, Signer, TimeInForce,
    TransactionBuilder, UpdateOrderParams, parse_token_contract,
};
use nautilus_common::{
    clients::ExecutionClient,
    live::{get_runtime, runner::get_exec_event_sender},
    messages::execution::{CancelOrder, ModifyOrder, QueryAccount, QueryOrder, SubmitOrder},
};
use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{OmsType, OrderSide as NautilusOrderSide, OrderStatus, OrderType},
    identifiers::{AccountId, ClientId, ClientOrderId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::OrderStatusReport,
    types::{AccountBalance, MarginBalance, Money, Quantity},
};

use crate::{
    common::{
        amounts::{
            decimal_to_raw_amount, format_token_amount, limit_price_string,
            probability_to_limit_price, tick_size_from_instrument_info,
        },
        balances::{
            collect_balance_token_specs_from_cache, fetch_account_balances,
        },
        currency::collateral_currency_code,
        signer::signer_from_private_key,
    },
    config::LightpoolExecClientConfig,
    http::{clob_index::ClobIndexHttpClient, models::{BalanceTokenSpec, OrderQueryResponse}},
};

pub struct LightpoolExecutionClient {
    core: ExecutionClientCore,
    emitter: ExecutionEventEmitter,
    config: LightpoolExecClientConfig,
    clob_client: ClobIndexHttpClient,
    private_key: Option<String>,
}

impl std::fmt::Debug for LightpoolExecutionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LightpoolExecutionClient")
            .field("core", &self.core)
            .field("emitter", &self.emitter)
            .field("config", &self.config)
            .field("has_private_key", &self.private_key.is_some())
            .finish()
    }
}

impl LightpoolExecutionClient {
    pub fn new(
        core: ExecutionClientCore,
        config: LightpoolExecClientConfig,
    ) -> anyhow::Result<Self> {
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );
        let clob_client = ClobIndexHttpClient::new(config.clob_index_http_url.clone());
        let private_key = config
            .resolved_private_key()
            .ok()
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty());

        Ok(Self {
            core,
            emitter,
            config,
            clob_client,
            private_key,
        })
    }

    fn ts_event(&self) -> UnixNanos {
        get_atomic_clock_realtime().get_time_ns()
    }

    fn placeholder_account_balances(&self) -> Vec<AccountBalance> {
        let code = collateral_currency_code();
        let zero = Money::from(format!("0 {code}"));
        vec![AccountBalance::new(zero.clone(), zero.clone(), zero)]
    }

    fn balance_token_specs_from_cache(&self) -> Vec<BalanceTokenSpec> {
        collect_balance_token_specs_from_cache(&*self.core.cache())
    }

    fn spawn_account_state_refresh(&self) {

        let Some(private_key) = self.private_key.clone() else {
            return;
        };

        spawn_account_balance_refresh(
            private_key,
            self.clob_client.clone(),
            self.emitter.clone(),
            self.balance_token_specs_from_cache(),
            self.config.market_slugs.clone(),
        );
    }

    fn submit_limit_order(&self, order: OrderAny) {
        let Some(private_key) = self.private_key.clone() else {
            self.emitter
                .emit_order_denied(&order, "LIGHTPOOL_PRIVATE_KEY not configured");
            return;
        };

        let instrument = match self.core.cache().instrument(&order.instrument_id()) {
            Some(instrument) => instrument.clone(),
            None => {
                self.emitter
                    .emit_order_denied(&order, "instrument not found in cache");
                return;
            }
        };

        let spot_market = instrument.raw_symbol().to_string();
        let emitter = self.emitter.clone();
        let clob_client = self.clob_client.clone();
        let ts_event = self.ts_event();

        self.emitter.emit_order_submitted(&order);

        get_runtime().spawn(async move {
            let signer = match signer_from_private_key(&private_key) {
                Ok(signer) => signer,
                Err(e) => {
                    let reason = format!("invalid signer: {e:#}");
                    emitter.emit_order_denied(&order, &reason);
                    return;
                }
            };
            match submit_limit_order_via_index(
                &clob_client,
                &signer,
                &instrument,
                &order,
                &spot_market,
            )
            .await
            {
                Ok(chain_order_id) => {
                    emitter.emit_order_accepted(
                        &order,
                        VenueOrderId::from(chain_order_id.as_str()),
                        ts_event,
                    );
                }
                Err(e) => {
                    emitter.emit_order_denied(&order, &e.to_string());
                }
            }
        });
    }
}

fn instrument_info(instrument: &InstrumentAny) -> Option<&Params> {
    match instrument {
        InstrumentAny::BinaryOption(binary_option) => binary_option.info.as_ref(),
        _ => None,
    }
}

fn order_side_label(side: NautilusOrderSide) -> Option<&'static str> {
    match side {
        NautilusOrderSide::Buy => Some("buy"),
        NautilusOrderSide::Sell => Some("sell"),
        _ => None,
    }
}

fn map_index_status(status: &str, filled_raw: u64) -> OrderStatus {
    match status {
        "filled" => OrderStatus::Filled,
        "cancelled" => OrderStatus::Canceled,
        "partial_filled" => OrderStatus::PartiallyFilled,
        "open" if filled_raw > 0 => OrderStatus::PartiallyFilled,
        "open" => OrderStatus::Accepted,
        _ => OrderStatus::Accepted,
    }
}

fn build_order_status_report(
    account_id: AccountId,
    order: &OrderAny,
    query: &OrderQueryResponse,
    ts_init: UnixNanos,
    ts_event: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let quantity = Quantity::from(query.order.size.as_str());
    let filled_qty = Quantity::from(format_token_amount(query.filled_raw).as_str());
    Ok(OrderStatusReport::new(
        account_id,
        order.instrument_id(),
        Some(order.client_order_id()),
        VenueOrderId::from(query.chain_order_id.as_str()),
        order.order_side(),
        order.order_type(),
        order.time_in_force(),
        map_index_status(&query.order.status, query.filled_raw),
        quantity,
        filled_qty,
        order.ts_accepted().unwrap_or(ts_event),
        ts_event,
        ts_init,
        None,
    ))
}

async fn query_order_from_index(
    clob_client: &ClobIndexHttpClient,
    instrument: &InstrumentAny,
    order: &OrderAny,
    spot_market: &str,
    venue_order_id: Option<VenueOrderId>,
    user_address: Option<&str>,
) -> anyhow::Result<Option<OrderQueryResponse>> {
    if let Some(venue_order_id) = venue_order_id {
        return clob_client
            .query_order(spot_market, venue_order_id.as_str(), user_address)
            .await;
    }

    let Some(user_address) = user_address else {
        return Ok(None);
    };
    let Some(side) = order_side_label(order.order_side()) else {
        return Ok(None);
    };
    let price_decimal = order
        .price()
        .ok_or_else(|| anyhow::anyhow!("limit order missing price"))?
        .as_decimal();
    let price = limit_price_string(
        price_decimal,
        tick_size_from_instrument_info(instrument_info(instrument)),
    )?;
    let size_raw = decimal_to_raw_amount(order.quantity().as_decimal())?;
    clob_client
        .query_open_order_match(spot_market, user_address, side, &price, size_raw)
        .await
}

async fn submit_limit_order_via_index(
    clob_client: &ClobIndexHttpClient,
    signer: &Signer,
    instrument: &InstrumentAny,
    order: &OrderAny,
    spot_market_str: &str,
) -> anyhow::Result<String> {
    let spot_market_display = spot_market_str.to_string();

    let price_decimal = order.price().map(|p| p.as_decimal());
    let price_decimal = price_decimal.ok_or_else(|| anyhow::anyhow!("limit order missing price"))?;
    let tick_size = tick_size_from_instrument_info(instrument_info(instrument));
    let limit_price = probability_to_limit_price(price_decimal, tick_size)?;

    let size_decimal = order.quantity().as_decimal();
    let amount = decimal_to_raw_amount(size_decimal)?;
    if amount == 0 {
        anyhow::bail!("order size must be greater than 0");
    }

    let side = match order.order_side() {
        NautilusOrderSide::Buy => OrderSide::Buy,
        NautilusOrderSide::Sell => OrderSide::Sell,
        other => anyhow::bail!("unsupported order side: {other:?}"),
    };

    let info = instrument_info(instrument);
    let token_address = if side == OrderSide::Buy {
        let collateral = info
            .and_then(|params| params.get("collateral_token"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        parse_token_contract(collateral)
            .or_else(|_| parse_token_contract(&spot_market_display))
            .map_err(|e| anyhow::anyhow!("missing collateral token for buy order: {e}"))?
    } else {
        let outcome_token = info
            .and_then(|params| params.get("outcome_token"))
            .and_then(|v| v.as_str())
            .unwrap_or(&spot_market_display);
        parse_token_contract(outcome_token)
            .map_err(|e| anyhow::anyhow!("missing outcome token for sell order: {e}"))?
    };

    let params = PlaceOrderParams {
        side,
        amount,
        order_type: OrderParamsType::Limit {
            tif: TimeInForce::GTC,
        },
        limit_price,
        token_address,
    };

    let (_digest, chain_order_id) = clob_client
        .submit_order_params(signer, spot_market_str, params)
        .await?;
    Ok(chain_order_id.to_string())
}

#[async_trait(?Send)]
impl ExecutionClient for LightpoolExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: nautilus_core::UnixNanos,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        let sender = get_exec_event_sender();
        self.emitter.set_sender(sender);
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.core.set_disconnected();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        match self.private_key.as_deref() {
            None => {
                log::warn!(
                    "Lightpool execution client started without signer; submits will be denied"
                );
            }
            Some(private_key) => match signer_from_private_key(private_key) {
                Ok(signer) => {
                    log::info!(
                        "Lightpool execution client signer address={} clob_index={}",
                        signer.address(),
                        self.config.clob_index_http_url,
                    );
                }
                Err(e) => log::warn!("Lightpool execution client invalid private key: {e:#}"),
            },
        }
        let ts_event = self.ts_event();
        let mut balances_reported = false;

        if let Some(private_key) = self.private_key.as_deref() {
            if let Ok(signer) = signer_from_private_key(private_key) {
                let address = signer.address().to_string();
                let cache_specs = self.balance_token_specs_from_cache();
                match fetch_account_balances(
                    &self.clob_client,
                    cache_specs,
                    &self.config.market_slugs,
                    &address,
                )
                .await
                {
                    Ok(balances) => {
                        log::info!(
                            "Lightpool account balances loaded address={address} entries={}",
                            balances.len()
                        );
                        self.generate_account_state(balances, vec![], true, ts_event)?;
                        balances_reported = true;
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to load Lightpool balances at connect address={address}: {e:#}"
                        );
                    }
                }
            }
        }

        if !balances_reported {
            self.generate_account_state(self.placeholder_account_balances(), vec![], false, ts_event)?;
        }
        log::info!(
            "Registered LightPool account_id={} collateral={}",
            self.account_id(),
            collateral_currency_code(),
        );
        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {

        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("order not found: {}", cmd.client_order_id))?
            .clone();

        match order.order_type() {
            OrderType::Limit => self.submit_limit_order(order),
            other => self.emitter.emit_order_denied(
                &order,
                &format!("unsupported order type for Lightpool: {other:?}"),
            ),
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("order not found: {}", cmd.client_order_id))?
            .clone();
        let venue_order_id = order
            .venue_order_id()
            .ok_or_else(|| anyhow::anyhow!("order has no venue order id"))?;

        let private_key = self
            .private_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("LIGHTPOOL_PRIVATE_KEY not configured"))?;
        let instrument = self
            .core
            .cache()
            .instrument(&order.instrument_id())
            .ok_or_else(|| anyhow::anyhow!("instrument not found"))?
            .clone();
        let spot_market = instrument.raw_symbol().to_string();
        let chain_order_id: u64 = venue_order_id
            .as_str()
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid venue order id: {e}"))?;
        let clob_client = self.clob_client.clone();
        let emitter = self.emitter.clone();
        let ts_event = self.ts_event();

        get_runtime().spawn(async move {
            let signer = match signer_from_private_key(&private_key) {
                Ok(signer) => signer,
                Err(e) => {
                    emitter.emit_order_cancel_rejected(
                        &order,
                        Some(venue_order_id),
                        &format!("invalid signer: {e:#}"),
                        ts_event,
                    );
                    return;
                }
            };
            match cancel_order_via_index(&clob_client, &signer, &spot_market, chain_order_id).await
            {
                Ok(()) => {
                    emitter.emit_order_canceled(&order, Some(venue_order_id), ts_event);
                }
                Err(e) => emitter.emit_order_cancel_rejected(
                    &order,
                    Some(venue_order_id),
                    &e.to_string(),
                    ts_event,
                ),
            }
        });
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("order not found: {}", cmd.client_order_id))?
            .clone();

        let private_key = self
            .private_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("LIGHTPOOL_PRIVATE_KEY not configured"))?;

        if cmd.price.is_some() {
            self.emitter.emit_order_modify_rejected(
                &order,
                cmd.venue_order_id,
                "LightPool update_order only supports quantity changes",
                self.ts_event(),
            );
            return Ok(());
        }

        let Some(new_quantity) = cmd.quantity else {
            return Ok(());
        };

        let venue_order_id = order
            .venue_order_id()
            .or(cmd.venue_order_id)
            .ok_or_else(|| anyhow::anyhow!("order has no venue order id"))?;
        let instrument = self
            .core
            .cache()
            .instrument(&order.instrument_id())
            .ok_or_else(|| anyhow::anyhow!("instrument not found"))?
            .clone();
        let spot_market = instrument.raw_symbol().to_string();
        let chain_order_id: u64 = venue_order_id
            .as_str()
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid venue order id: {e}"))?;
        log::debug!(
            "modify_order: start client_order_id={} venue_order_id={} instrument_id={} spot_market={} new_quantity={}",
            order.client_order_id(),
            venue_order_id,
            order.instrument_id(),
            spot_market,
            new_quantity,
        );
        let clob_client = self.clob_client.clone();
        let emitter = self.emitter.clone();
        let ts_event = self.ts_event();

        get_runtime().spawn(async move {
            let signer = match signer_from_private_key(&private_key) {
                Ok(signer) => signer,
                Err(e) => {
                    emitter.emit_order_modify_rejected(
                        &order,
                        Some(venue_order_id),
                        &format!("invalid signer: {e:#}"),
                        ts_event,
                    );
                    return;
                }
            };
            match update_order_via_index(
                &clob_client,
                &signer,
                &instrument,
                &order,
                &spot_market,
                chain_order_id,
                new_quantity,
            )
            .await
            {
                Ok(digest) => {
                    log::debug!(
                        "modify_order: receipt received client_order_id={} venue_order_id={} chain_order_id={} digest={digest} new_quantity={}",
                        order.client_order_id(),
                        venue_order_id,
                        chain_order_id,
                        new_quantity,
                    );
                    emitter.emit_order_updated(
                        &order,
                        venue_order_id,
                        new_quantity,
                        None,
                        None,
                        None,
                        ts_event,
                    );
                }
                Err(e) => {
                    log::warn!(
                        "modify_order: failed client_order_id={} venue_order_id={} chain_order_id={} error={e:#}",
                        order.client_order_id(),
                        venue_order_id,
                        chain_order_id,
                    );
                    emitter.emit_order_modify_rejected(
                        &order,
                        Some(venue_order_id),
                        &e.to_string(),
                        ts_event,
                    );
                }
            }
        });
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        self.spawn_account_state_refresh();
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let client_order_id = cmd.client_order_id;
        let Some(order) = self
            .core
            .cache()
            .order(&client_order_id)
            .map(|order_ref| order_ref.cloned())
        else {
            return Ok(());
        };

        let instrument = match self.core.cache().instrument(&order.instrument_id()) {
            Some(instrument) => instrument.clone(),
            None => return Ok(()),
        };

        let spot_market = instrument.raw_symbol().to_string();
        let venue_order_id = cmd.venue_order_id.or(order.venue_order_id());
        let private_key = self.private_key.clone();
        let clob_client = self.clob_client.clone();
        let emitter = self.emitter.clone();
        let account_id = self.core.account_id;
        let ts_init = cmd.ts_init;
        let ts_event = self.ts_event();

        get_runtime().spawn(async move {
            let user_address = private_key
                .as_deref()
                .and_then(|key| signer_from_private_key(key).ok())
                .map(|signer| signer.address().to_string());

            let result = query_order_from_index(
                &clob_client,
                &instrument,
                &order,
                &spot_market,
                venue_order_id,
                user_address.as_deref(),
            )
            .await;

            match result {
                Ok(Some(query)) => {
                    match build_order_status_report(account_id, &order, &query, ts_init, ts_event) {
                        Ok(report) => emitter.send_order_status_report(report),
                        Err(error) => log::warn!(
                            "query_order: failed to build status report for {}: {error:#}",
                            order.client_order_id()
                        ),
                    }
                    if order.venue_order_id().is_none() {
                        emitter.emit_order_accepted(
                            &order,
                            VenueOrderId::from(query.chain_order_id.as_str()),
                            ts_event,
                        );
                    }
                }
                Ok(None) => log::debug!(
                    "query_order: no indexed order found for {}",
                    order.client_order_id()
                ),
                Err(error) => log::warn!(
                    "query_order: clob-index lookup failed for {}: {error:#}",
                    order.client_order_id()
                ),
            }
        });

        Ok(())
    }
}

async fn cancel_order_via_index(
    clob_client: &ClobIndexHttpClient,
    signer: &Signer,
    spot_market: &str,
    chain_order_id: u64,
) -> anyhow::Result<()> {
    let _digest = clob_client
        .cancel_order_params(signer, spot_market, chain_order_id)
        .await?;
    Ok(())
}

fn token_address_for_order(
    instrument: &InstrumentAny,
    order: &OrderAny,
    spot_market_str: &str,
) -> anyhow::Result<lightpool_sdk::ContractAddress> {
    let side = order.order_side();
    let info = instrument_info(instrument);
    let spot_market_display = spot_market_str.to_string();

    if side == NautilusOrderSide::Buy {
        let collateral = info
            .and_then(|params| params.get("collateral_token"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        parse_token_contract(collateral)
            .or_else(|_| parse_token_contract(&spot_market_display))
            .map_err(|e| anyhow::anyhow!("missing collateral token for buy order: {e}"))
    } else {
        let outcome_token = info
            .and_then(|params| params.get("outcome_token"))
            .and_then(|v| v.as_str())
            .unwrap_or(&spot_market_display);
        parse_token_contract(outcome_token)
            .map_err(|e| anyhow::anyhow!("missing outcome token for sell order: {e}"))
    }
}

async fn update_order_via_index(
    clob_client: &ClobIndexHttpClient,
    signer: &Signer,
    instrument: &InstrumentAny,
    order: &OrderAny,
    spot_market_str: &str,
    chain_order_id: u64,
    new_quantity: nautilus_model::types::Quantity,
) -> anyhow::Result<String> {
    let spot_market = parse_token_contract(spot_market_str)
        .map_err(|e| anyhow::anyhow!("invalid spot market: {e}"))?;
    let amount = decimal_to_raw_amount(new_quantity.as_decimal())?;
    if amount == 0 {
        anyhow::bail!("order size must be greater than 0");
    }

    let token_address = token_address_for_order(instrument, order, spot_market_str)?;
    let params = UpdateOrderParams {
        order_id: chain_order_id,
        amount,
        token_address,
    };
    let action = ActionBuilder::update_order(spot_market, params)?;
    let tx = TransactionBuilder::new()
        .sender(signer.address())
        .expiration(u64::MAX)
        .add_action(action)
        .build_and_sign_only(signer)?;
    let digest = hex::encode(tx.digest().as_bytes());
    log::debug!(
        "modify_order: submitting HTTP update_order client_order_id={} chain_order_id={} spot_market={} digest={} amount_raw={}",
        order.client_order_id(),
        chain_order_id,
        spot_market_str,
        digest,
        amount,
    );
    let response = clob_client.submit_transaction(tx).await?;
    if !response.receipt.is_success() {
        anyhow::bail!("update_order failed: {:?}", response.receipt.status);
    }
    Ok(response.digest)
}

fn spawn_account_balance_refresh(
    private_key: String,
    clob_client: ClobIndexHttpClient,
    emitter: ExecutionEventEmitter,
    cache_specs: Vec<BalanceTokenSpec>,
    market_slugs: Vec<String>,
) {
    get_runtime().spawn(async move {

        let signer = match signer_from_private_key(&private_key) {
            Ok(signer) => signer,
            Err(e) => {
                log::warn!("account state refresh skipped: invalid signer: {e:#}");
                return;
            }
        };
        let address = signer.address().to_string();

        match fetch_account_balances(&clob_client, cache_specs, &market_slugs, &address).await {
            Ok(balances) => {
                let ts_event = get_atomic_clock_realtime().get_time_ns();
                log::debug!(
                    "Lightpool account balances refreshed address={address} entries={}",
                    balances.len()
                );
                emitter.emit_account_state(balances, vec![], true, ts_event);
            }
            Err(e) => {
                log::warn!(
                    "Lightpool account balance refresh failed address={address}: {e:#}"
                );
            }
        }
    });
}

pub fn default_account_id() -> AccountId {
    AccountId::new(format!("LIGHTPOOL-{}", collateral_currency_code()).as_str())
}

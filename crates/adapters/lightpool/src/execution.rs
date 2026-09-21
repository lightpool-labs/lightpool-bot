// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
};

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
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OmsType, OrderSide as NautilusOrderSide, OrderStatus, OrderType},
    identifiers::{AccountId, ClientId, TradeId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::OrderStatusReport,
    types::{AccountBalance, MarginBalance, Money, Quantity},
};
use tokio::task::JoinHandle;

use crate::{
        common::{
        amounts::{
            decimal_to_raw_amount, format_token_amount, limit_price_string_for_unit,
            tick_size_from_instrument_info, to_limit_price_raw,
        },
        balances::{
            collect_balance_token_specs_from_cache, fetch_account_balances,
        },
        currency::{collateral_currency, collateral_currency_code},
        instrument_meta::{
            base_token_from_info, instrument_info, price_unit_for_instrument, quote_token_from_info,
        },
        signer::signer_from_private_key,
    },
    config::{LightpoolExecClientConfig, clob_index_ws_from_env},
    http::{clob_index::ClobIndexHttpClient, models::{BalanceTokenSpec, OrderQueryResponse}},
    websocket::clob_index::{ClobIndexWsClient, ClobIndexWsMessage},
    websocket::models::{UserOrderMessage, UserTradeMessage},
};

pub struct LightpoolExecutionClient {
    core: ExecutionClientCore,
    emitter: ExecutionEventEmitter,
    config: LightpoolExecClientConfig,
    clob_client: ClobIndexHttpClient,
    private_key: Option<String>,
    ws_stream_handle: Mutex<Option<JoinHandle<()>>>,
    tracked_orders: Arc<Mutex<HashMap<String, OrderAny>>>,
    cloid_orders: Arc<Mutex<HashMap<String, OrderAny>>>,
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
            ws_stream_handle: Mutex::new(None),
            tracked_orders: Arc::new(Mutex::new(HashMap::new())),
            cloid_orders: Arc::new(Mutex::new(HashMap::new())),
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
            self.config.spot_markets.clone(),
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
        let tracked_orders = self.tracked_orders.clone();
        let cloid_orders = self.cloid_orders.clone();
        let cloid = order.client_order_id().to_string();
        cache_cloid(&cloid_orders, &cloid, &order);
        let ts_event = self.ts_event();

        self.emitter.emit_order_submitted(&order);

        get_runtime().spawn(async move {
            let signer = match signer_from_private_key(&private_key) {
                Ok(signer) => signer,
                Err(e) => {
                    remove_cloid(&cloid_orders, &cloid);
                    let reason = format!("invalid signer: {e:#}");
                    emitter.emit_order_rejected(&order, &reason, ts_event, false);
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
                Ok((chain_order_id, fully_matched)) => {
                    track_order(&tracked_orders, &spot_market, &chain_order_id, &order);
                    let venue_order_id = VenueOrderId::from(chain_order_id.as_str());
                    emitter.emit_order_accepted(&order, venue_order_id.clone(), ts_event);
                    if fully_matched {
                        emit_submit_fill(
                            &emitter,
                            &order,
                            venue_order_id,
                            ts_event,
                        );
                    }
                }
                Err(e) if submit_result_is_uncertain(&e) => {
                    log::error!(
                        "LightPool submit result uncertain, keep cloid={cloid} until chain events arrive: {e:#}"
                    );
                }
                Err(e) => {
                    remove_cloid(&cloid_orders, &cloid);
                    emitter.emit_order_rejected(&order, &e.to_string(), ts_event, false);
                }
            }
        });
    }

    async fn start_ws_stream(&mut self, user_address: String) -> anyhow::Result<()> {
        if let Some(handle) = self
            .ws_stream_handle
            .lock()
            .expect("ws stream handle")
            .take()
        {
            handle.abort();
        }

        let mut ws = ClobIndexWsClient::new(clob_index_ws_from_env());
        ws.connect()
            .await
            .map_err(|e| anyhow::anyhow!("LightPool user stream connect failed: {e:#}"))?;
        ws.subscribe_user(&user_address)
            .await
            .map_err(|e| anyhow::anyhow!("LightPool user stream subscribe failed: {e:#}"))?;
        log::info!("Subscribed to LightPool execution updates for {user_address}");

        let emitter = self.emitter.clone();
        let tracked_orders = self.tracked_orders.clone();
        let cloid_orders = self.cloid_orders.clone();
        let handle = get_runtime().spawn(async move {
            loop {
                match ws.next_event().await {
                    Some(ClobIndexWsMessage::UserOrder(message)) => {
                        apply_user_order(&tracked_orders, &cloid_orders, &emitter, &message);
                    }
                    Some(ClobIndexWsMessage::UserTrade(message)) => {
                        apply_user_trade(&tracked_orders, &cloid_orders, &emitter, &message);
                    }
                    Some(ClobIndexWsMessage::Error(error)) => {
                        log::warn!("LightPool user stream error: {error}");
                    }
                    None => {
                        log::debug!("LightPool user stream closed");
                        break;
                    }
                    _ => {}
                }
            }
        });

        *self.ws_stream_handle.lock().expect("ws stream handle") = Some(handle);
        log::info!("LightPool WebSocket execution stream started");
        Ok(())
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

fn chain_order_key(spot_market: &str, chain_order_id: &str) -> String {
    let market = parse_token_contract(spot_market)
        .map(|contract| contract.to_string())
        .unwrap_or_else(|_| spot_market.trim().to_string());
    format!("{market}:{chain_order_id}")
}

fn track_order(
    tracked: &Mutex<HashMap<String, OrderAny>>,
    spot_market: &str,
    chain_order_id: &str,
    order: &OrderAny,
) {
    if let Ok(mut tracked) = tracked.lock() {
        tracked.insert(chain_order_key(spot_market, chain_order_id), order.clone());
    }
}

fn cache_cloid(cloids: &Mutex<HashMap<String, OrderAny>>, cloid: &str, order: &OrderAny) {
    if cloid.is_empty() {
        return;
    }
    if let Ok(mut cloids) = cloids.lock() {
        cloids.insert(cloid.to_string(), order.clone());
    }
}

fn remove_cloid(cloids: &Mutex<HashMap<String, OrderAny>>, cloid: &str) {
    if let Ok(mut cloids) = cloids.lock() {
        cloids.remove(cloid);
    }
}

fn submit_result_is_uncertain(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("504")
        || msg.contains("timed out")
        || msg.contains("timeout")
        || msg.contains("connection")
        || msg.contains("connect failed")
        || msg.contains("error sending request")
}

fn tracked_order(
    tracked: &Mutex<HashMap<String, OrderAny>>,
    spot_market: &str,
    chain_order_id: &str,
) -> Option<OrderAny> {
    tracked.lock().ok().and_then(|tracked| {
        tracked
            .get(&chain_order_key(spot_market, chain_order_id))
            .cloned()
    })
}

fn resolve_order(
    tracked: &Mutex<HashMap<String, OrderAny>>,
    cloids: &Mutex<HashMap<String, OrderAny>>,
    spot_market: Option<&str>,
    chain_order_id: &str,
    cloid: Option<&str>,
    emitter: &ExecutionEventEmitter,
) -> Option<OrderAny> {
    if let Some(market) = spot_market.filter(|market| !market.is_empty()) {
        if let Some(order) = tracked_order(tracked, market, chain_order_id) {
            return Some(order);
        }
    }
    let cloid = cloid.filter(|value| !value.is_empty())?;
    let order = cloids.lock().ok()?.get(cloid).cloned()?;
    if let Some(market) = spot_market.filter(|market| !market.is_empty()) {
        track_order(tracked, market, chain_order_id, &order);
    }
    let ts_event = get_atomic_clock_realtime().get_time_ns();
    emitter.emit_order_accepted(
        &order,
        VenueOrderId::from(chain_order_id),
        ts_event,
    );
    Some(order)
}

fn apply_user_order(
    tracked: &Mutex<HashMap<String, OrderAny>>,
    cloids: &Mutex<HashMap<String, OrderAny>>,
    emitter: &ExecutionEventEmitter,
    message: &UserOrderMessage,
) {
    let cloid = message
        .extra
        .get("cloid")
        .and_then(|value| value.as_str());
    let Some(order) = resolve_order(
        tracked,
        cloids,
        message.spot_market.as_deref(),
        &message.chain_order_id,
        cloid,
        emitter,
    ) else {
        return;
    };
    if order.is_closed() {
        return;
    }
    let venue_order_id = VenueOrderId::from(message.chain_order_id.as_str());
    let ts_event = get_atomic_clock_realtime().get_time_ns();
    let status = message
        .extra
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if message.event == "cancellation" || status == "cancelled" || status == "canceled" {
        log::info!(
            "LightPool order cancelled client_order_id={} venue_order_id={venue_order_id}",
            order.client_order_id(),
        );
        emitter.emit_order_canceled(&order, Some(venue_order_id), ts_event);
        return;
    }
    if status == "filled" {
        emit_closing_fill(emitter, &order, venue_order_id, message.block_num.unwrap_or(0), ts_event);
        return;
    }
    if message.event != "update" {
        return;
    }
    let Some(size) = message.extra.get("size").and_then(|value| value.as_str()) else {
        return;
    };
    let Ok(quantity) = Quantity::from_str(size) else {
        log::warn!(
            "LightPool order update ignored client_order_id={} invalid size={size}",
            order.client_order_id(),
        );
        return;
    };
    if !order.is_pending_update() && order.quantity().as_decimal() == quantity.as_decimal() {
        return;
    }
    log::info!(
        "LightPool order updated client_order_id={} venue_order_id={venue_order_id} quantity={quantity}",
        order.client_order_id(),
    );
    emitter.emit_order_updated(
        &order,
        venue_order_id,
        quantity,
        None,
        None,
        None,
        ts_event,
    );
}

fn apply_user_trade(
    tracked: &Mutex<HashMap<String, OrderAny>>,
    cloids: &Mutex<HashMap<String, OrderAny>>,
    emitter: &ExecutionEventEmitter,
    message: &UserTradeMessage,
) {
    let Some(order) = resolve_order(
        tracked,
        cloids,
        message.spot_market.as_deref(),
        &message.chain_order_id,
        message.cloid.as_deref(),
        emitter,
    ) else {
        return;
    };
    if order.is_closed() {
        return;
    }
    let Some(fill_amount) = message.fill_amount.as_deref() else {
        return;
    };
    let fully_filled = message.is_fully_filled.unwrap_or(false)
        || message
            .remaining_amount
            .as_deref()
            .is_some_and(|amount| amount == "0" || amount == "0.0");
    let Some(last_qty) = fill_quantity_for_order(&order, fill_amount, fully_filled) else {
        return;
    };
    let Some(last_px) = order.price() else {
        return;
    };
    let venue_order_id = VenueOrderId::from(message.chain_order_id.as_str());
    let trade_id = lightpool_trade_id(
        &message.chain_order_id,
        message.block_num.unwrap_or(0),
        "trade",
    );
    let ts_event = get_atomic_clock_realtime().get_time_ns();
    log::info!(
        "LightPool order filled client_order_id={} venue_order_id={venue_order_id} last_qty={last_qty} last_px={last_px} fully_filled={fully_filled}",
        order.client_order_id(),
    );
    emitter.emit_order_filled(
        &order,
        venue_order_id,
        None,
        trade_id,
        last_qty,
        last_px,
        collateral_currency(),
        None,
        LiquiditySide::Maker,
        ts_event,
    );
}

fn emit_closing_fill(
    emitter: &ExecutionEventEmitter,
    order: &OrderAny,
    venue_order_id: VenueOrderId,
    block_num: u64,
    ts_event: UnixNanos,
) {
    let Some(last_qty) = order.leaves_qty().is_positive().then(|| order.leaves_qty()) else {
        return;
    };
    let Some(last_px) = order.price() else {
        return;
    };
    let trade_id = lightpool_trade_id(venue_order_id.as_str(), block_num, "filled");
    log::info!(
        "LightPool order filled from index status client_order_id={} venue_order_id={venue_order_id} last_qty={last_qty}",
        order.client_order_id(),
    );
    emitter.emit_order_filled(
        order,
        venue_order_id,
        None,
        trade_id,
        last_qty,
        last_px,
        collateral_currency(),
        None,
        LiquiditySide::Maker,
        ts_event,
    );
}

fn emit_submit_fill(
    emitter: &ExecutionEventEmitter,
    order: &OrderAny,
    venue_order_id: VenueOrderId,
    ts_event: UnixNanos,
) {
    let last_qty = order.quantity();
    let Some(last_px) = order.price() else {
        return;
    };
    let trade_id = lightpool_trade_id(venue_order_id.as_str(), 0, "submit-fill");
    log::info!(
        "LightPool order fully matched on submit client_order_id={} venue_order_id={venue_order_id} last_qty={last_qty} last_px={last_px}",
        order.client_order_id(),
    );
    emitter.emit_order_filled(
        order,
        venue_order_id,
        None,
        trade_id,
        last_qty,
        last_px,
        collateral_currency(),
        None,
        LiquiditySide::Taker,
        ts_event,
    );
}

fn fill_quantity_for_order(order: &OrderAny, reported: &str, fully_filled: bool) -> Option<Quantity> {
    let leaves = order.leaves_qty();
    if let Ok(parsed) = Quantity::from_str(reported) {
        if let Ok(aligned) =
            Quantity::from_decimal_dp(parsed.as_decimal(), order.quantity().precision)
        {
            if aligned.is_positive() {
                if leaves.is_positive() && aligned.as_decimal() > leaves.as_decimal() {
                    return Some(leaves);
                }
                return Some(aligned);
            }
        }
    }
    if fully_filled && leaves.is_positive() {
        return Some(leaves);
    }
    None
}

fn lightpool_trade_id(chain_order_id: &str, block_num: u64, kind: &str) -> TradeId {
    let trade_key = format!("lp-{kind}-{chain_order_id}-{block_num}");
    if trade_key.len() <= 36 {
        TradeId::new(trade_key)
    } else {
        TradeId::new(&trade_key[trade_key.len() - 36..])
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
    let price = limit_price_string_for_unit(
        price_decimal,
        tick_size_from_instrument_info(instrument_info(instrument)),
        price_unit_for_instrument(instrument),
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
) -> anyhow::Result<(String, bool)> {
    let spot_market_display = spot_market_str.to_string();

    let price_decimal = order.price().map(|p| p.as_decimal());
    let price_decimal = price_decimal.ok_or_else(|| anyhow::anyhow!("limit order missing price"))?;
    let info = instrument_info(instrument);
    let tick_size = tick_size_from_instrument_info(info);
    let price_unit = price_unit_for_instrument(instrument);
    let limit_price = to_limit_price_raw(price_decimal, tick_size, price_unit)?;

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

    let token_address = if side == OrderSide::Buy {
        let collateral = quote_token_from_info(info).unwrap_or("");
        parse_token_contract(collateral)
            .or_else(|_| parse_token_contract(&spot_market_display))
            .map_err(|e| anyhow::anyhow!("missing collateral/quote token for buy order: {e}"))?
    } else {
        let outcome_token = base_token_from_info(info).unwrap_or(&spot_market_display);
        parse_token_contract(outcome_token)
            .map_err(|e| anyhow::anyhow!("missing base/outcome token for sell order: {e}"))?
    };

    let params = PlaceOrderParams {
        side,
        amount,
        order_type: OrderParamsType::Limit {
            tif: TimeInForce::GTC,
        },
        limit_price,
        token_address,
        cloid: Some(order.client_order_id().to_string()),
    };

    let (_digest, chain_order_id, fully_matched) = clob_client
        .submit_order_params(signer, spot_market_str, params)
        .await?;
    Ok((chain_order_id.to_string(), fully_matched))
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
        if let Some(handle) = self
            .ws_stream_handle
            .lock()
            .expect("ws stream handle")
            .take()
        {
            handle.abort();
        }
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
                    &self.config.spot_markets,
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
        if let Some(private_key) = self.private_key.clone()
            && let Ok(signer) = signer_from_private_key(&private_key)
        {
            self.start_ws_stream(signer.address().to_string()).await?;
        }
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
                Err(e) if chain_order_missing(&e) => {
                    log::warn!(
                        "cancel_order: chain order missing, cancel locally client_order_id={} venue_order_id={} chain_order_id={} error={e:#}",
                        order.client_order_id(),
                        venue_order_id,
                        chain_order_id,
                    );
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
                Err(e) if chain_order_missing(&e) => {
                    log::warn!(
                        "modify_order: chain order missing, cancel locally client_order_id={} venue_order_id={} chain_order_id={} error={e:#}",
                        order.client_order_id(),
                        venue_order_id,
                        chain_order_id,
                    );
                    emitter.emit_order_canceled(&order, Some(venue_order_id), ts_event);
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

fn chain_order_missing(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains("Order meta not found")
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
        let collateral = quote_token_from_info(info).unwrap_or("");
        parse_token_contract(collateral)
            .or_else(|_| parse_token_contract(&spot_market_display))
            .map_err(|e| anyhow::anyhow!("missing collateral/quote token for buy order: {e}"))
    } else {
        let outcome_token = base_token_from_info(info).unwrap_or(&spot_market_display);
        parse_token_contract(outcome_token)
            .map_err(|e| anyhow::anyhow!("missing base/outcome token for sell order: {e}"))
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
    spot_markets: Vec<crate::config::SpotMarketBootstrap>,
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

        match fetch_account_balances(
            &clob_client,
            cache_specs,
            &market_slugs,
            &spot_markets,
            &address,
        )
        .await
        {
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

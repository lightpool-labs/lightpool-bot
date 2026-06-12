use std::sync::{
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use lightpool_sdk::{
    spot_events::extract_order_id_from_events, ActionBuilder, CancelOrderParams,
    OrderParamsType, OrderSide, PlaceOrderParams, Signer, TimeInForce, TransactionBuilder,
    UpdateOrderParams, parse_token_contract,
};
use nautilus_common::{
    clients::ExecutionClient,
    live::{get_runtime, runner::get_exec_event_sender},
    messages::execution::{CancelOrder, ModifyOrder, SubmitOrder},
};
use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{OmsType, OrderSide as NautilusOrderSide, OrderType},
    identifiers::{AccountId, ClientId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{AccountBalance, MarginBalance, Money},
};

use crate::{
    common::{
        amounts::{
            decimal_to_raw_amount, probability_to_limit_price, tick_size_from_instrument_info,
        },
        currency::collateral_currency_code,
        signer::signer_from_private_key,
    },
    config::LightpoolExecClientConfig,
    http::clob_index::ClobIndexHttpClient,
};

pub struct LightpoolExecutionClient {
    core: ExecutionClientCore,
    emitter: ExecutionEventEmitter,
    config: LightpoolExecClientConfig,
    clob_client: ClobIndexHttpClient,
    private_key: Option<String>,
    is_stopped: AtomicBool,
}

impl std::fmt::Debug for LightpoolExecutionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LightpoolExecutionClient")
            .field("core", &self.core)
            .field("emitter", &self.emitter)
            .field("config", &self.config)
            .field("has_private_key", &self.private_key.is_some())
            .field("is_stopped", &self.is_stopped)
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
            is_stopped: AtomicBool::new(false),
        })
    }

    fn ts_event(&self) -> UnixNanos {
        get_atomic_clock_realtime().get_time_ns()
    }

    fn initial_account_balances(&self) -> Vec<AccountBalance> {
        let code = collateral_currency_code();
        let zero = Money::from(format!("0 {code}"));
        vec![AccountBalance::new(zero.clone(), zero.clone(), zero)]
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
                    emitter.emit_order_denied(&order, &format!("invalid signer: {e:#}"));
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
                Err(e) => emitter.emit_order_denied(&order, &e.to_string()),
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

async fn submit_limit_order_via_index(
    clob_client: &ClobIndexHttpClient,
    signer: &Signer,
    instrument: &InstrumentAny,
    order: &OrderAny,
    spot_market_str: &str,
) -> anyhow::Result<String> {
    let spot_market = parse_token_contract(spot_market_str)
        .map_err(|e| anyhow::anyhow!("invalid spot market: {e}"))?;
    let spot_market_display = spot_market.to_string();

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

    let action = ActionBuilder::place_order(spot_market, params)?;
    let tx = TransactionBuilder::new()
        .sender(signer.address())
        .expiration(u64::MAX)
        .add_action(action)
        .build_and_sign_only(signer)?;

    let response = clob_client.submit_transaction(tx).await?;
    if !response.receipt.is_success() {
        anyhow::bail!("place_order failed: {:?}", response.receipt.status);
    }

    let chain_order_id = extract_order_id_from_events(&response.receipt)
        .ok_or_else(|| anyhow::anyhow!("order_created event missing from receipt"))?;
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
        if self.is_stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.core.set_stopped();
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
        self.generate_account_state(self.initial_account_balances(), vec![], false, ts_event)?;
        log::info!(
            "Registered LightPool account_id={} collateral={}",
            self.account_id(),
            collateral_currency_code(),
        );
        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.core.set_disconnected();
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
        let private_key = self
            .private_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("LIGHTPOOL_PRIVATE_KEY not configured"))?;
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("order not found: {}", cmd.client_order_id))?
            .clone();
        let venue_order_id = order
            .venue_order_id()
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
                Ok(()) => emitter.emit_order_canceled(&order, Some(venue_order_id), ts_event),
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
        let private_key = self
            .private_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("LIGHTPOOL_PRIVATE_KEY not configured"))?;
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("order not found: {}", cmd.client_order_id))?
            .clone();

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
                Ok(()) => emitter.emit_order_updated(
                    &order,
                    venue_order_id,
                    new_quantity,
                    None,
                    None,
                    None,
                    ts_event,
                ),
                Err(e) => emitter.emit_order_modify_rejected(
                    &order,
                    Some(venue_order_id),
                    &e.to_string(),
                    ts_event,
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
    let spot_market = parse_token_contract(spot_market)
        .map_err(|e| anyhow::anyhow!("invalid spot market: {e}"))?;
    let params = CancelOrderParams {
        order_id: chain_order_id,
    };
    let action = ActionBuilder::cancel_order(spot_market, params)?;
    let tx = TransactionBuilder::new()
        .sender(signer.address())
        .expiration(u64::MAX)
        .add_action(action)
        .build_and_sign_only(signer)?;
    let response = clob_client.submit_transaction(tx).await?;
    if !response.receipt.is_success() {
        anyhow::bail!("cancel_order failed: {:?}", response.receipt.status);
    }
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
) -> anyhow::Result<()> {
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
    let response = clob_client.submit_transaction(tx).await?;
    if !response.receipt.is_success() {
        anyhow::bail!("update_order failed: {:?}", response.receipt.status);
    }
    Ok(())
}

pub fn default_account_id() -> AccountId {
    AccountId::new(format!("LIGHTPOOL-{}", collateral_currency_code()).as_str())
}

// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use ahash::AHashMap;
use async_trait::async_trait;
use dashmap::DashMap;
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{
        DataEvent,
        data::{
            SubscribeBookDeltas, SubscribeQuotes, UnsubscribeBookDeltas, UnsubscribeQuotes,
        },
    },
};
use nautilus_core::{
    AtomicMap,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{Data as NautilusData, OrderBookDeltas_API},
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        amounts::parse_token_amount_str,
        consts::{DEFAULT_TICK_SIZE_RAW, LIGHTPOOL_VENUE},
    },
    config::LightpoolDataClientConfig,
    http::clob_index::ClobIndexHttpClient,
    parse::{
        instruments_for_market, parse_book_delta, parse_book_snapshot, parse_quote_delta,
        parse_quote_snapshot,
    },
    websocket::clob_index::{ClobIndexWsClient, ClobIndexWsMessage},
};

#[derive(Debug)]
pub struct LightpoolDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: LightpoolDataClientConfig,
    http_client: ClobIndexHttpClient,
    ws_client: ClobIndexWsClient,
    is_connected: AtomicBool,
    cancellation_token: CancellationToken,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    spot_market_by_instrument: Arc<DashMap<InstrumentId, String>>,
    instrument_by_spot_market: Arc<DashMap<String, InstrumentId>>,
    active_delta_subs: Arc<DashMap<InstrumentId, ()>>,
    active_quote_subs: Arc<DashMap<InstrumentId, ()>>,
    reader_task: Option<JoinHandle<()>>,
}

impl LightpoolDataClient {
    pub fn new(client_id: ClientId, config: LightpoolDataClientConfig) -> Self {
        let http_client = ClobIndexHttpClient::new(config.clob_index_http_url.clone());
        let ws_client = ClobIndexWsClient::new(config.clob_index_ws_url.clone());
        Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            config,
            http_client,
            ws_client,
            is_connected: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            data_sender: get_data_event_sender(),
            instruments: Arc::new(AtomicMap::new()),
            spot_market_by_instrument: Arc::new(DashMap::new()),
            instrument_by_spot_market: Arc::new(DashMap::new()),
            active_delta_subs: Arc::new(DashMap::new()),
            active_quote_subs: Arc::new(DashMap::new()),
            reader_task: None,
        }
    }

    async fn resolve_tick_size_raw(&self, spot_market: &str) -> u64 {
        match self.http_client.fetch_spot_info(spot_market).await {
            Ok(info) => match parse_token_amount_str(&info.tick_size) {
                Ok(raw) if raw > 0 => raw,
                Ok(_) => {
                    log::warn!(
                        "spot {spot_market} tick_size={} rounds to 0; using default={}",
                        info.tick_size,
                        DEFAULT_TICK_SIZE_RAW
                    );
                    DEFAULT_TICK_SIZE_RAW
                }
                Err(e) => {
                    log::warn!(
                        "invalid tick_size '{}' for spot {spot_market}: {e:#}; using default={}",
                        info.tick_size,
                        DEFAULT_TICK_SIZE_RAW
                    );
                    DEFAULT_TICK_SIZE_RAW
                }
            },
            Err(e) => {
                log::warn!(
                    "failed to fetch spot info for {spot_market}: {e:#}; using default={}",
                    DEFAULT_TICK_SIZE_RAW
                );
                DEFAULT_TICK_SIZE_RAW
            }
        }
    }

    async fn bootstrap_instruments(&mut self) -> anyhow::Result<()> {
        let markets = self
            .http_client
            .fetch_markets_by_slugs(&self.config.market_slugs)
            .await?;
        let ts_init = self.clock.get_time_ns();
        let mut count = 0usize;
        let mut cached = Vec::new();

        for market in markets {
            let yes_tick = self.resolve_tick_size_raw(&market.yes_spot_market).await;
            let no_tick = self.resolve_tick_size_raw(&market.no_spot_market).await;
            log::info!(
                "Lightpool market slug={} yes_tick_raw={yes_tick} no_tick_raw={no_tick}",
                market.slug,
            );
            for instrument in instruments_for_market(&market, yes_tick, no_tick, ts_init)? {
                let instrument_id = instrument.id();
                let spot_market = instrument.raw_symbol().to_string();
                self.spot_market_by_instrument
                    .insert(instrument_id, spot_market.clone());
                self.instrument_by_spot_market
                    .insert(spot_market, instrument_id);
                self.instruments.insert(instrument_id, instrument.clone());
                cached.push(instrument.clone());
                if let Err(e) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                    log::warn!("Failed to publish instrument {instrument_id}: {e}");
                }
                count += 1;
            }
        }

        self.ws_client.cache_instruments(cached);
        log::info!("Lightpool bootstrap loaded {count} instruments");
        Ok(())
    }

    fn spawn_ws_reader(
        &mut self,
        mut out_rx: tokio::sync::mpsc::UnboundedReceiver<ClobIndexWsMessage>,
    ) {
        let data_sender = self.data_sender.clone();
        let clock = self.clock;
        let instrument_by_spot_market = self.instrument_by_spot_market.clone();
        let active_delta_subs = self.active_delta_subs.clone();
        let active_quote_subs = self.active_quote_subs.clone();
        let cancel = self.cancellation_token.child_token();

        let handle = get_runtime().spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    event = out_rx.recv() => {
                        let Some(event) = event else { break };
                        let ts_init = clock.get_time_ns();
                        match event {
                            ClobIndexWsMessage::OrderBookSnapshot(snapshot) => {
                                let Some(instrument_id) = instrument_by_spot_market
                                    .get(&snapshot.spot_market)
                                    .map(|e| *e)
                                else {
                                    continue;
                                };
                                if !active_delta_subs.contains_key(&instrument_id) {
                                    continue;
                                }
                                let book = crate::http::models::BookSnapshot {
                                    sequence: snapshot.sequence,
                                    bids: snapshot.bids,
                                    asks: snapshot.asks,
                                    last_trade_price: snapshot.last_trade_price,
                                };
                                if let Ok(deltas) =
                                    parse_book_snapshot(&book, instrument_id, ts_init)
                                {
                                    let _ = data_sender.send(DataEvent::Data(
                                        NautilusData::Deltas(OrderBookDeltas_API::new(deltas)),
                                    ));
                                }
                            }
                            ClobIndexWsMessage::OrderBookDelta(delta) => {
                                let Some(instrument_id) = instrument_by_spot_market
                                    .get(&delta.spot_market)
                                    .map(|e| *e)
                                else {
                                    continue;
                                };
                                if !active_delta_subs.contains_key(&instrument_id) {
                                    continue;
                                }
                                if let Ok(deltas) = parse_book_delta(
                                    &delta.bids,
                                    &delta.asks,
                                    instrument_id,
                                    delta.sequence,
                                    ts_init,
                                ) {
                                    let _ = data_sender.send(DataEvent::Data(
                                        NautilusData::Deltas(OrderBookDeltas_API::new(deltas)),
                                    ));
                                }
                            }
                            ClobIndexWsMessage::QuoteSnapshot(snapshot) => {
                                let Some(instrument_id) = instrument_by_spot_market
                                    .get(&snapshot.spot_market)
                                    .map(|e| *e)
                                else {
                                    continue;
                                };
                                if !active_quote_subs.contains_key(&instrument_id) {
                                    continue;
                                }
                                match parse_quote_snapshot(&snapshot, instrument_id, ts_init) {
                                    Ok(Some(tick)) => {
                                        let _ = data_sender
                                            .send(DataEvent::Data(NautilusData::Quote(tick)));
                                    }
                                    Ok(None) => {}
                                    Err(e) => log::warn!(
                                        "Failed to parse quote snapshot for {instrument_id}: {e:#}"
                                    ),
                                }
                            }
                            ClobIndexWsMessage::Quote(delta) => {
                                let Some(instrument_id) = instrument_by_spot_market
                                    .get(&delta.spot_market)
                                    .map(|e| *e)
                                else {
                                    continue;
                                };
                                if !active_quote_subs.contains_key(&instrument_id) {
                                    continue;
                                }
                                match parse_quote_delta(&delta, instrument_id, ts_init) {
                                    Ok(Some(tick)) => {
                                        let _ = data_sender
                                            .send(DataEvent::Data(NautilusData::Quote(tick)));
                                    }
                                    Ok(None) => {}
                                    Err(e) => log::warn!(
                                        "Failed to parse quote for {instrument_id}: {e:#}"
                                    ),
                                }
                            }
                            ClobIndexWsMessage::Error(error) => {
                                log::warn!("LightPool WS error: {error}");
                            }
                            ClobIndexWsMessage::Subscribed { channel, key }
                            | ClobIndexWsMessage::Unsubscribed { channel, key } => {
                                log::debug!("LightPool WS {channel} key={key}");
                            }
                            ClobIndexWsMessage::UserOrder(_)
                            | ClobIndexWsMessage::UserTrade(_) => {}
                        }
                    }
                }
            }
        });
        self.reader_task = Some(handle);
    }
}

#[async_trait(?Send)]
impl DataClient for LightpoolDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*LIGHTPOOL_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Lightpool data client: {}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Lightpool data client: {}", self.client_id);
        self.cancellation_token.cancel();
        self.is_connected.store(false, Ordering::Relaxed);
        if let Some(handle) = self.reader_task.take() {
            handle.abort();
        }
        self.active_delta_subs.clear();
        self.active_quote_subs.clear();
        let mut ws = self.ws_client.clone();
        get_runtime().spawn(async move {
            let _ = ws.disconnect().await;
        });
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.instruments.store(AHashMap::new());
        self.spot_market_by_instrument.clear();
        self.instrument_by_spot_market.clear();
        self.cancellation_token = CancellationToken::new();
        self.ws_client = ClobIndexWsClient::new(self.config.clob_index_ws_url.clone());
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        log::info!(
            "Connecting Lightpool data client via clob-index http={} ws={}",
            self.config.clob_index_http_url,
            self.config.clob_index_ws_url
        );
        self.cancellation_token = CancellationToken::new();
        self.bootstrap_instruments().await?;
        self.ws_client.connect().await?;
        if let Some(out_rx) = self.ws_client.take_event_receiver() {
            self.spawn_ws_reader(out_rx);
        }
        self.is_connected.store(true, Ordering::Relaxed);
        log::info!("Connected Lightpool data client");
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!(
                "Lightpool only supports L2_MBP order book deltas, received {:?}",
                cmd.book_type
            );
        }
        let instrument_id = cmd.instrument_id;
        if !self.instruments.load().contains_key(&instrument_id) {
            anyhow::bail!("Instrument {instrument_id} not found in Lightpool cache");
        }
        self.active_delta_subs.insert(instrument_id, ());
        let ws = self.ws_client.clone();
        let depth = self.config.book_depth;
        get_runtime().spawn(async move {
            if let Err(e) = ws.subscribe_orderbook(instrument_id, depth).await {
                log::error!("Failed to subscribe orderbook for {instrument_id}: {e}");
            }
        });
        log::debug!("Subscribed to Lightpool book deltas for {instrument_id}");
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        self.active_delta_subs.remove(&instrument_id);
        let ws = self.ws_client.clone();
        get_runtime().spawn(async move {
            if let Err(e) = ws.unsubscribe_orderbook(instrument_id).await {
                log::warn!("Failed to unsubscribe orderbook for {instrument_id}: {e}");
            }
        });
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        if !self.instruments.load().contains_key(&instrument_id) {
            anyhow::bail!("Instrument {instrument_id} not found in Lightpool cache");
        }
        self.active_quote_subs.insert(instrument_id, ());
        let ws = self.ws_client.clone();
        get_runtime().spawn(async move {
            if let Err(e) = ws.subscribe_quotes(instrument_id).await {
                log::error!("Failed to subscribe quotes for {instrument_id}: {e}");
            }
        });
        log::debug!("Subscribed to Lightpool quotes for {instrument_id}");
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        self.active_quote_subs.remove(&instrument_id);
        let ws = self.ws_client.clone();
        get_runtime().spawn(async move {
            if let Err(e) = ws.unsubscribe_quotes(instrument_id).await {
                log::warn!("Failed to unsubscribe quotes for {instrument_id}: {e}");
            }
        });
        Ok(())
    }
}

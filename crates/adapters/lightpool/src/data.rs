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
        data::{SubscribeBookDeltas, UnsubscribeBookDeltas},
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
    parse::{instruments_for_market, parse_book_delta, parse_book_snapshot},
    websocket::clob_index::{BookWsEvent, ClobIndexBookWsClient},
};

#[derive(Debug)]
pub struct LightpoolDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: LightpoolDataClientConfig,
    http_client: ClobIndexHttpClient,
    ws_client: ClobIndexBookWsClient,
    is_connected: AtomicBool,
    cancellation_token: CancellationToken,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    spot_market_by_instrument: Arc<DashMap<InstrumentId, String>>,
    instrument_by_spot_market: Arc<DashMap<String, InstrumentId>>,
    active_delta_subs: Arc<DashMap<InstrumentId, ()>>,
    book_tasks: Arc<DashMap<InstrumentId, JoinHandle<()>>>,
}

impl LightpoolDataClient {
    pub fn new(client_id: ClientId, config: LightpoolDataClientConfig) -> Self {
        let http_client = ClobIndexHttpClient::new(config.clob_index_http_url.clone());
        let ws_client = ClobIndexBookWsClient::new(config.clob_index_ws_url.clone());
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
            book_tasks: Arc::new(DashMap::new()),
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
                if let Err(e) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                    log::warn!("Failed to publish instrument {instrument_id}: {e}");
                }
                count += 1;
            }
        }

        log::info!("Lightpool bootstrap loaded {count} instruments");
        Ok(())
    }

    fn spawn_book_subscription(&self, instrument_id: InstrumentId) {
        if self.book_tasks.contains_key(&instrument_id) {
            return;
        }

        let Some(spot_market) = self
            .spot_market_by_instrument
            .get(&instrument_id)
            .map(|entry| entry.clone())
        else {
            log::warn!("No spot market mapping for {instrument_id}");
            return;
        };

        let depth = self.config.book_depth;
        let ws_client = self.ws_client.clone();
        let data_sender = self.data_sender.clone();
        let clock = self.clock;
        let cancel = self.cancellation_token.child_token();
        let book_tasks = self.book_tasks.clone();
        let active_delta_subs = self.active_delta_subs.clone();

        let handle = get_runtime().spawn(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            if let Err(e) = ws_client
                .subscribe_orderbook(spot_market, depth, tx, cancel.clone())
                .await
            {
                log::error!("Failed to subscribe orderbook for {instrument_id}: {e}");
                active_delta_subs.remove(&instrument_id);
                book_tasks.remove(&instrument_id);
                return;
            }

            while let Some(event) = rx.recv().await {
                let ts_init = clock.get_time_ns();
                match event {
                    BookWsEvent::Snapshot(snapshot) => {
                        let book = crate::http::models::BookSnapshot {
                            sequence: snapshot.sequence,
                            bids: snapshot.bids,
                            asks: snapshot.asks,
                            last_trade_price: snapshot.last_trade_price,
                        };
                        if let Ok(deltas) = parse_book_snapshot(&book, instrument_id, ts_init) {
                            let _ = data_sender.send(DataEvent::Data(NautilusData::Deltas(
                                OrderBookDeltas_API::new(deltas),
                            )));
                        }
                    }
                    BookWsEvent::Delta(delta) => {
                        if let Ok(deltas) = parse_book_delta(
                            &delta.bids,
                            &delta.asks,
                            instrument_id,
                            delta.sequence,
                            ts_init,
                        ) {
                            let _ = data_sender.send(DataEvent::Data(NautilusData::Deltas(
                                OrderBookDeltas_API::new(deltas),
                            )));
                        }
                    }
                    BookWsEvent::Error(error) => {
                        log::warn!("Orderbook WS error for {instrument_id}: {error}");
                    }
                }
            }

            active_delta_subs.remove(&instrument_id);
            book_tasks.remove(&instrument_id);
        });

        self.book_tasks.insert(instrument_id, handle);
    }

    fn stop_book_subscription(&self, instrument_id: InstrumentId) {
        if let Some((_, handle)) = self.book_tasks.remove(&instrument_id) {
            handle.abort();
        }
        self.active_delta_subs.remove(&instrument_id);
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
        for entry in self.book_tasks.iter() {
            entry.value().abort();
        }
        self.book_tasks.clear();
        self.active_delta_subs.clear();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.instruments.store(AHashMap::new());
        self.spot_market_by_instrument.clear();
        self.instrument_by_spot_market.clear();
        self.cancellation_token = CancellationToken::new();
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
        log::info!("Connecting Lightpool data client via clob-index http={} ws={}", self.config.clob_index_http_url, self.config.clob_index_ws_url);
        self.cancellation_token = CancellationToken::new();
        self.bootstrap_instruments().await?;
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
        self.spawn_book_subscription(instrument_id);
        log::debug!("Subscribed to Lightpool book deltas for {instrument_id}");
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.stop_book_subscription(cmd.instrument_id);
        Ok(())
    }

}

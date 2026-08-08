// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::Arc;

use ahash::AHashMap;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::models::{
    OrderBookDelta, OrderBookWsSnapshot, QuoteDelta, QuoteSnapshot, UserOrderMessage,
    UserTradeMessage, WsError, WsSubscribed,
};
use crate::{
    common::signer::signer_from_private_key,
    config::{clob_index_http_from_env, clob_index_ws_from_env, resolve_private_key},
    http::clob_index::ClobIndexHttpClient,
};
use lightpool_sdk::Signer;

/// Unified outbound message stream (Hyperliquid `NautilusWsMessage` pattern).
#[derive(Debug, Clone)]
pub enum ClobIndexWsMessage {
    OrderBookSnapshot(OrderBookWsSnapshot),
    OrderBookDelta(OrderBookDelta),
    QuoteSnapshot(QuoteSnapshot),
    Quote(QuoteDelta),
    UserOrder(UserOrderMessage),
    UserTrade(UserTradeMessage),
    Subscribed { channel: String, key: String },
    Unsubscribed { channel: String, key: String },
    Error(String),
}

enum HandlerCommand {
    Send(String),
    Disconnect,
}

/// clob-index WebSocket client: one connection, demuxed `next_event()` stream.
pub struct ClobIndexWsClient {
    ws_url: String,
    http: ClobIndexHttpClient,
    signer: Option<Arc<Signer>>,
    instruments: Arc<DashMap<InstrumentId, InstrumentAny>>,
    cmd_tx: Option<mpsc::UnboundedSender<HandlerCommand>>,
    out_rx: Option<mpsc::UnboundedReceiver<ClobIndexWsMessage>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for ClobIndexWsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClobIndexWsClient")
            .field("ws_url", &self.ws_url)
            .field("has_signer", &self.signer.is_some())
            .field("cached_instruments", &self.instruments.len())
            .field("connected", &self.is_connected())
            .finish()
    }
}

impl Clone for ClobIndexWsClient {
    fn clone(&self) -> Self {
        Self {
            ws_url: self.ws_url.clone(),
            http: self.http.clone(),
            signer: self.signer.clone(),
            instruments: Arc::clone(&self.instruments),
            cmd_tx: self.cmd_tx.clone(),
            out_rx: None,
            task: None,
        }
    }
}

/// Backward-compatible alias used by older call sites / docs.
pub type ClobIndexBookWsClient = ClobIndexWsClient;

impl ClobIndexWsClient {
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            ws_url: ws_url.into().trim_end_matches('/').to_string(),
            http: ClobIndexHttpClient::new(clob_index_http_from_env()),
            signer: None,
            instruments: Arc::new(DashMap::new()),
            cmd_tx: None,
            out_rx: None,
            task: None,
        }
    }

    /// Build a client from env (`LIGHTPOOL_CLOB_INDEX_WS` / HTTP).
    ///
    /// Loads a signer when `LIGHTPOOL_PRIVATE_KEY` or `~/.lightpool/wallet.json` is available.
    #[must_use]
    pub fn from_env() -> Self {
        let mut client = Self::new(clob_index_ws_from_env());
        if let Ok(private_key) = resolve_private_key() {
            match signer_from_private_key(&private_key) {
                Ok(signer) => client.set_signer(signer),
                Err(error) => {
                    log::warn!("LightPool WS: ignoring invalid private key: {error:#}");
                }
            }
        }
        client
    }

    pub fn set_signer(&mut self, signer: Signer) {
        self.signer = Some(Arc::new(signer));
    }

    pub fn get_user_address(&self) -> anyhow::Result<String> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "signer not set; set LIGHTPOOL_PRIVATE_KEY or create ~/.lightpool/wallet.json"
                )
            })?;
        Ok(signer.address().to_string())
    }

    pub fn is_connected(&self) -> bool {
        self.cmd_tx.is_some()
    }

    /// Fetch instruments via clob-index HTTP (same venue as this WS client).
    pub async fn request_instruments(&self) -> anyhow::Result<Vec<InstrumentAny>> {
        self.http.request_instruments().await
    }

    /// Cache instruments for InstrumentId → spot_market resolution.
    pub fn cache_instruments(&mut self, instruments: impl IntoIterator<Item = InstrumentAny>) {
        self.instruments.clear();
        let mut by_id = AHashMap::new();
        for instrument in instruments {
            by_id.insert(instrument.id(), instrument);
        }
        let count = by_id.len();
        for (id, instrument) in by_id {
            self.instruments.insert(id, instrument);
        }
        log::info!("LightPool instrument cache initialized with {count} instruments");
    }

    pub fn get_instrument(&self, instrument_id: &InstrumentId) -> Option<InstrumentAny> {
        self.instruments
            .get(instrument_id)
            .map(|entry| entry.value().clone())
    }

    pub fn spot_market(&self, instrument_id: &InstrumentId) -> anyhow::Result<String> {
        self.get_instrument(instrument_id)
            .map(|instrument| instrument.raw_symbol().to_string())
            .ok_or_else(|| {
                anyhow::anyhow!("instrument {instrument_id} not found in LightPool WS cache")
            })
    }

    /// Open one WebSocket connection and start the demux handler.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            log::warn!("LightPool WebSocket already connected");
            return Ok(());
        }

        let ws_url = format!("{}/api/ws", self.ws_url);
        let (socket, _) = connect_async(&ws_url).await?;
        let (mut write, mut read) = socket.split();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<HandlerCommand>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<ClobIndexWsMessage>();

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(HandlerCommand::Send(text)) => {
                                if write.send(Message::Text(text.into())).await.is_err() {
                                    let _ = out_tx.send(ClobIndexWsMessage::Error(
                                        "failed to write websocket text".into(),
                                    ));
                                    break;
                                }
                            }
                            Some(HandlerCommand::Disconnect) | None => {
                                let _ = write.close().await;
                                break;
                            }
                        }
                    }
                    message = read.next() => {
                        match message {
                            Some(Ok(Message::Text(text))) => {
                                if let Err(e) = dispatch_text(&text, &out_tx) {
                                    log::warn!("Failed to parse clob-index WS message: {e}");
                                }
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                let _ = write.send(Message::Pong(payload)).await;
                            }
                            Some(Ok(Message::Close(_))) | None => {
                                break;
                            }
                            Some(Err(e)) => {
                                let _ = out_tx.send(ClobIndexWsMessage::Error(e.to_string()));
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        self.cmd_tx = Some(cmd_tx);
        self.out_rx = Some(out_rx);
        self.task = Some(task);
        log::info!("LightPool WebSocket connected: {ws_url}");
        Ok(())
    }

    pub async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(tx) = self.cmd_tx.take() {
            let _ = tx.send(HandlerCommand::Disconnect);
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        self.out_rx = None;
        Ok(())
    }

    /// Receive the next demuxed message (all channels).
    pub async fn next_event(&mut self) -> Option<ClobIndexWsMessage> {
        if let Some(ref mut rx) = self.out_rx {
            rx.recv().await
        } else {
            None
        }
    }

    /// Take the outbound receiver for a dedicated reader task (DataClient use).
    pub fn take_event_receiver(
        &mut self,
    ) -> Option<mpsc::UnboundedReceiver<ClobIndexWsMessage>> {
        self.out_rx.take()
    }

    pub async fn subscribe_orderbook(
        &self,
        instrument_id: InstrumentId,
        depth: u32,
    ) -> anyhow::Result<()> {
        let spot_market = self.spot_market(&instrument_id)?;
        self.send_json(&serde_json::json!({
            "op": "subscribe",
            "channel": "orderbook_delta",
            "spot_market": spot_market,
            "depth": depth,
        }))
    }

    pub async fn unsubscribe_orderbook(
        &self,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        let spot_market = self.spot_market(&instrument_id)?;
        self.send_json(&serde_json::json!({
            "op": "unsubscribe",
            "channel": "orderbook_delta",
            "spot_market": spot_market,
        }))
    }

    pub async fn subscribe_quotes(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let spot_market = self.spot_market(&instrument_id)?;
        self.send_json(&serde_json::json!({
            "op": "subscribe",
            "channel": "quote",
            "spot_market": spot_market,
        }))
    }

    pub async fn unsubscribe_quotes(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let spot_market = self.spot_market(&instrument_id)?;
        self.send_json(&serde_json::json!({
            "op": "unsubscribe",
            "channel": "quote",
            "spot_market": spot_market,
        }))
    }

    pub async fn subscribe_user(&self, user_address: &str) -> anyhow::Result<()> {
        self.send_json(&serde_json::json!({
            "op": "subscribe",
            "channel": "user",
            "user_address": user_address,
        }))
    }

    pub async fn unsubscribe_user(&self, user_address: &str) -> anyhow::Result<()> {
        self.send_json(&serde_json::json!({
            "op": "unsubscribe",
            "channel": "user",
            "user_address": user_address,
        }))
    }

    fn send_json(&self, value: &serde_json::Value) -> anyhow::Result<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("LightPool WebSocket not connected"))?;
        tx.send(HandlerCommand::Send(value.to_string()))
            .map_err(|e| anyhow::anyhow!("failed to enqueue websocket command: {e}"))?;
        Ok(())
    }
}

fn dispatch_text(
    text: &str,
    out_tx: &mpsc::UnboundedSender<ClobIndexWsMessage>,
) -> anyhow::Result<()> {
    let envelope: serde_json::Value = serde_json::from_str(text)?;
    let Some(msg_type) = envelope.get("type").and_then(|v| v.as_str()) else {
        log::warn!("clob-index WS message missing type field: {text}");
        return Ok(());
    };

    match msg_type {
        "error" => {
            let error = serde_json::from_value::<WsError>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::Error(error.error));
        }
        "subscribed" => {
            let msg = serde_json::from_value::<WsSubscribed>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::Subscribed {
                channel: msg.channel,
                key: msg.key,
            });
        }
        "unsubscribed" => {
            let msg = serde_json::from_value::<WsSubscribed>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::Unsubscribed {
                channel: msg.channel,
                key: msg.key,
            });
        }
        "orderbook_snapshot" => {
            let snapshot = serde_json::from_value::<OrderBookWsSnapshot>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::OrderBookSnapshot(snapshot));
        }
        "orderbook_delta" => {
            let delta = serde_json::from_value::<OrderBookDelta>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::OrderBookDelta(delta));
        }
        "quote_snapshot" => {
            let snapshot = serde_json::from_value::<QuoteSnapshot>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::QuoteSnapshot(snapshot));
        }
        "quote" => {
            let delta = serde_json::from_value::<QuoteDelta>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::Quote(delta));
        }
        "order" => {
            let order = serde_json::from_value::<UserOrderMessage>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::UserOrder(order));
        }
        "trade" => {
            let trade = serde_json::from_value::<UserTradeMessage>(envelope)?;
            let _ = out_tx.send(ClobIndexWsMessage::UserTrade(trade));
        }
        other => {
            log::warn!("Unhandled clob-index WS message type: {other}");
        }
    }
    Ok(())
}

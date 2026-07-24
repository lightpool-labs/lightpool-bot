// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::models::{OrderBookDelta, OrderBookWsSnapshot, WsError};

#[derive(Debug, Clone)]
pub enum BookWsEvent {
    Snapshot(OrderBookWsSnapshot),
    Delta(OrderBookDelta),
    Error(String),
}

#[derive(Debug, Clone)]
pub struct ClobIndexBookWsClient {
    ws_url: String,
}

impl ClobIndexBookWsClient {
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            ws_url: ws_url.into().trim_end_matches('/').to_string(),
        }
    }

    pub async fn subscribe_orderbook(
        &self,
        spot_market: String,
        depth: u32,
        tx: mpsc::UnboundedSender<BookWsEvent>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let ws_url = format!("{}/api/ws", self.ws_url);
        let (mut socket, _) = connect_async(&ws_url).await?;
        let subscribe = serde_json::json!({
            "op": "subscribe",
            "channel": "orderbook_delta",
            "spot_market": spot_market,
            "depth": depth,
        });
        socket
            .send(Message::Text(subscribe.to_string().into()))
            .await?;

        let ws_client = Arc::new(self.clone());
        let spot_market_for_unsub = spot_market.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = socket.send(Message::Text(serde_json::json!({
                            "op": "unsubscribe",
                            "channel": "orderbook_delta",
                            "spot_market": spot_market_for_unsub,
                        }).to_string().into())).await;
                        let _ = socket.close(None).await;
                        break;
                    }
                    message = socket.next() => {
                        match message {
                            Some(Ok(Message::Text(text))) => {
                                if let Err(e) = ws_client.handle_text(&text, &tx) {
                                    log::warn!("Failed to parse clob-index WS message: {e}");
                                }
                            }
                            Some(Ok(Message::Close(_))) | None => break,
                            Some(Err(e)) => {
                                let _ = tx.send(BookWsEvent::Error(e.to_string()));
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        Ok(())
    }

    fn handle_text(
        &self,
        text: &str,
        tx: &mpsc::UnboundedSender<BookWsEvent>,
    ) -> anyhow::Result<()> {
        let envelope: serde_json::Value = serde_json::from_str(text)?;
        let Some(msg_type) = envelope.get("type").and_then(|v| v.as_str()) else {
            log::warn!("clob-index WS message missing type field: {text}");
            return Ok(());
        };

        match msg_type {
            "error" => {
                let error = serde_json::from_value::<WsError>(envelope)?;
                let _ = tx.send(BookWsEvent::Error(error.error));
            }
            "subscribed" => {}
            "orderbook_snapshot" => {
                let snapshot = serde_json::from_value::<OrderBookWsSnapshot>(envelope)?;
                let _ = tx.send(BookWsEvent::Snapshot(snapshot));
            }
            "orderbook_delta" => {
                let delta = serde_json::from_value::<OrderBookDelta>(envelope)?;
                let _ = tx.send(BookWsEvent::Delta(delta));
            }
            other => {
                log::warn!("Unhandled clob-index WS message type: {other}");
            }
        }
        Ok(())
    }
}

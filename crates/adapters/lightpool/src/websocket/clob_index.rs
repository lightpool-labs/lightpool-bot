use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::models::{OrderBookDelta, OrderBookWsSnapshot, WsError, WsSubscribed};

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
        if let Ok(error) = serde_json::from_str::<WsError>(text) {
            if error.msg_type == "error" {
                let _ = tx.send(BookWsEvent::Error(error.error));
            }
            return Ok(());
        }
        if serde_json::from_str::<WsSubscribed>(text).is_ok() {
            return Ok(());
        }
        if let Ok(snapshot) = serde_json::from_str::<OrderBookWsSnapshot>(text) {
            if snapshot.msg_type == "orderbook_snapshot" {
                let _ = tx.send(BookWsEvent::Snapshot(snapshot));
            }
            return Ok(());
        }
        if let Ok(delta) = serde_json::from_str::<OrderBookDelta>(text) {
            if delta.msg_type == "orderbook_delta" {
                let _ = tx.send(BookWsEvent::Delta(delta));
            }
        }
        Ok(())
    }
}

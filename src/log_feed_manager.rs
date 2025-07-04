use crate::tradeslog::{ConcurrentTradesLog, Trade};
use futures_util::StreamExt;
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use thiserror::Error;
use metrics::{Counter, Gauge};

#[derive(Debug, Deserialize)]
pub struct BinanceTradeUpdate {
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "T")]
    pub timestamp: u64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

#[derive(Debug, Error)]
pub enum FeedError {
    #[error("WebSocket error: {0}")]
    Websocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Decimal conversion error")]
    DecimalConversion,
}

pub struct FeedMetrics {
    pub messages_received: Counter,
    pub trades_processed: Counter,
    pub connection_errors: Counter,
    pub current_connections: Gauge,
}

pub struct LogFeedManager {
    trades_log: ConcurrentTradesLog,
    uri: String,
    metrics: FeedMetrics,
}

impl LogFeedManager {
    pub fn new(uri: String, trades_log_capacity: usize) -> Self {
        let trades_log = ConcurrentTradesLog::new(trades_log_capacity);
        Self {
            trades_log,
            uri,
            metrics: FeedMetrics {
                messages_received: metrics::register_counter!("log_feed_messages_received"),
                trades_processed: metrics::register_counter!("log_feed_trades_processed"),
                connection_errors: metrics::register_counter!("log_feed_connection_errors"),
                current_connections: metrics::register_gauge!("log_feed_current_connections"),
            },
        }
    }

    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) {
        let mut retry_delay = Duration::from_secs(1);
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutting down trade feed manager");
                        break;
                    } else {
                        continue;
                    }
                }
                connect_result = connect_async(&self.uri) => {
                    match connect_result {
                        Ok((ws_stream, _)) => {
                            self.metrics.current_connections.set(1.0);
                            info!("Connected to Trade WebSocket at {}", self.uri);
                            let (_, mut read) = ws_stream.split();
                            loop {
                                tokio::select! {
                                    _ = shutdown_rx.changed() => {
                                        if *shutdown_rx.borrow() {
                                            info!("Shutting down trade feed manager");
                                            self.metrics.current_connections.set(0.0);
                                            return;
                                        }
                                    }
                                    message_result = read.next() => {
                                        if let Some(message_result) = message_result {
                                            self.metrics.messages_received.increment(1);
                                            match message_result {
                                                Ok(Message::Text(text)) => {
                                                    if let Err(err) = self.process_text_message(&text).await {
                                                        error!("Failed to process trade message: {}", err);
                                                    }
                                                }
                                                Ok(Message::Binary(bin)) => {
                                                    if let Ok(text) = String::from_utf8(bin) {
                                                        debug!("Trade Message (binary): {}", text);
                                                    }
                                                }
                                                Ok(_) => {}
                                                Err(err) => {
                                                    self.metrics.connection_errors.increment(1);
                                                    error!("WebSocket error: {}", err);
                                                    break;
                                                }
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                }
                            }
                            warn!("⚠️ Trade WebSocket stream closed for {}", self.uri);
                            self.metrics.current_connections.set(0.0);
                        }
                        Err(err) => {
                            self.metrics.connection_errors.increment(1);
                            error!("Failed to connect to {}: {}", self.uri, err);
                        }
                    }
                }
            }
            if *shutdown_rx.borrow() {
                break;
            }
            warn!("Reconnecting to {} in {:?}...", self.uri, retry_delay);
            sleep(retry_delay).await;
            retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60));
        }
    }

    async fn process_text_message(&self, text: &str) -> Result<(), FeedError> {
        let update: BinanceTradeUpdate = serde_json::from_str(text)?;
        let trade = Trade::try_from(update)?;
        self.trades_log.insert_trade(trade).await;
        self.metrics.trades_processed.increment(1);
        Ok(())
    }

    pub fn get_trades_log(&self) -> ConcurrentTradesLog {
        self.trades_log.clone()
    }
}

impl TryFrom<BinanceTradeUpdate> for Trade {
    type Error = FeedError;
    fn try_from(update: BinanceTradeUpdate) -> Result<Self, Self::Error> {
        Ok(Self {
            id: 0,
            price: Decimal::from_str(&update.price)
                .map_err(|_| FeedError::DecimalConversion)?,
            quantity: Decimal::from_str(&update.quantity)
                .map_err(|_| FeedError::DecimalConversion)?,
            timestamp: update.timestamp,
            is_buyer_maker: update.is_buyer_maker,
        })
    }
}
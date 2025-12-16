use crate::data::lob_feed_manager::{SharedConnectionStatus, ConnectionStatus};
use crate::data::tradeslog::{ConcurrentTradesLog, Trade};
use futures_util::StreamExt;
use log::{debug, error, info};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
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
    symbol: String,
    metrics: FeedMetrics,
    connection_status: SharedConnectionStatus,
}

impl LogFeedManager {
    pub fn new(symbol: &str, trades_log_capacity: usize) -> Self {
        let symbol_lower = symbol.to_lowercase();
        let trades_log = ConcurrentTradesLog::new(trades_log_capacity);
        Self {
            trades_log,
            symbol: symbol_lower,
            metrics: FeedMetrics {
                messages_received: metrics::register_counter!("log_feed_messages_received"),
                trades_processed: metrics::register_counter!("log_feed_trades_processed"),
                connection_errors: metrics::register_counter!("log_feed_connection_errors"),
                current_connections: metrics::register_gauge!("log_feed_current_connections"),
            },
            connection_status: SharedConnectionStatus::new(),
        }
    }

    /// Get the connection status for TUI display
    pub fn connection_status(&self) -> SharedConnectionStatus {
        self.connection_status.clone()
    }

    fn build_uri(&self) -> String {
        format!("wss://stream.binance.com:9443/ws/{}@trade", self.symbol)
    }

    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) {
        let mut retry_delay = Duration::from_secs(1);
        let uri = self.build_uri();
        loop {
            self.connection_status.set(ConnectionStatus::Connecting);
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutting down trade feed manager");
                        self.connection_status.set(ConnectionStatus::Disconnected);
                        break;
                    } else {
                        continue;
                    }
                }
                connect_result = connect_async(&uri) => {
                    match connect_result {
                        Ok((ws_stream, _)) => {
                            self.metrics.current_connections.set(1.0);
                            self.connection_status.set(ConnectionStatus::Connected);
                            debug!("Connected to Trade WebSocket at {}", uri);
                            retry_delay = Duration::from_secs(1); // Reset on successful connect
                            let (_, mut read) = ws_stream.split();
                            loop {
                                tokio::select! {
                                    _ = shutdown_rx.changed() => {
                                        if *shutdown_rx.borrow() {
                                            info!("Shutting down trade feed manager");
                                            self.metrics.current_connections.set(0.0);
                                            self.connection_status.set(ConnectionStatus::Disconnected);
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
                                                    if let Ok(text) = String::from_utf8(bin.to_vec()) {
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
                            // Stream closed - will reconnect
                            self.metrics.current_connections.set(0.0);
                            self.connection_status.set(ConnectionStatus::Reconnecting);
                        }
                        Err(err) => {
                            self.metrics.connection_errors.increment(1);
                            error!("Failed to connect to {}: {}", uri, err);
                            self.connection_status.set(ConnectionStatus::Reconnecting);
                        }
                    }
                }
            }
            if *shutdown_rx.borrow() {
                self.connection_status.set(ConnectionStatus::Disconnected);
                break;
            }
            debug!("Reconnecting to {} in {:?}...", uri, retry_delay);
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
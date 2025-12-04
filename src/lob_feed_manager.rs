use crate::orderbook::ConcurrentOrderBook;
use futures_util::StreamExt;
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio::task;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

/// Connection status for WebSocket feeds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectionStatus {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Reconnecting = 3,
}

impl From<u8> for ConnectionStatus {
    fn from(v: u8) -> Self {
        match v {
            1 => ConnectionStatus::Connecting,
            2 => ConnectionStatus::Connected,
            3 => ConnectionStatus::Reconnecting,
            _ => ConnectionStatus::Disconnected,
        }
    }
}

/// Shared connection status that can be read by TUI
#[derive(Clone)]
pub struct SharedConnectionStatus {
    status: Arc<AtomicU8>,
}

impl SharedConnectionStatus {
    pub fn new() -> Self {
        Self {
            status: Arc::new(AtomicU8::new(ConnectionStatus::Disconnected as u8)),
        }
    }

    pub fn set(&self, status: ConnectionStatus) {
        self.status.store(status as u8, Ordering::SeqCst);
    }

    pub fn get(&self) -> ConnectionStatus {
        ConnectionStatus::from(self.status.load(Ordering::SeqCst))
    }

    /// One-liner status for TUI display
    pub fn status_line(&self) -> &'static str {
        match self.get() {
            ConnectionStatus::Disconnected => "WS: DOWN",
            ConnectionStatus::Connecting => "WS: CONNECTING...",
            ConnectionStatus::Connected => "WS: OK",
            ConnectionStatus::Reconnecting => "WS: RECONNECTING...",
        }
    }
}

impl Default for SharedConnectionStatus {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
pub struct BinanceDepthUpdate {
    #[serde(rename = "b")]
    pub bids: Vec<(String, String)>,
    #[serde(rename = "a")]
    pub asks: Vec<(String, String)>,
}

pub struct LobFeedManager {
    order_book: ConcurrentOrderBook,
    symbol: String,
    connection_status: SharedConnectionStatus,
}

impl LobFeedManager {
    pub fn new(symbol: &str) -> Self {
        let symbol_lower = symbol.to_lowercase();
        Self {
            order_book: ConcurrentOrderBook::new(),
            symbol: symbol_lower,
            connection_status: SharedConnectionStatus::new(),
        }
    }

    /// Get the connection status for TUI display
    pub fn connection_status(&self) -> SharedConnectionStatus {
        self.connection_status.clone()
    }

    fn build_hf_uri(&self) -> String {
        format!("wss://stream.binance.com:9443/ws/{}@depth@100ms", self.symbol)
    }

    fn build_lf_uri(&self) -> String {
        format!("wss://stream.binance.com:9443/ws/{}@depth", self.symbol)
    }

    pub fn get_order_book(&self) -> ConcurrentOrderBook {
        self.order_book.clone()
    }

    pub async fn start(&self, shutdown_rx: tokio::sync::watch::Receiver<bool>) {
        let hf_book = self.order_book.clone();
        let lf_book = self.order_book.clone();
        let hf_uri = self.build_hf_uri();
        let lf_uri = self.build_lf_uri();
        let hf_shutdown = shutdown_rx.clone();
        let lf_shutdown = shutdown_rx.clone();
        let hf_status = self.connection_status.clone();
        let lf_status = self.connection_status.clone();
        let hf_task = task::spawn(Self::run_feed(hf_uri, hf_book, true, hf_shutdown, hf_status));
        let lf_task = task::spawn(Self::run_feed(lf_uri, lf_book, false, lf_shutdown, lf_status));
        let _ = tokio::join!(hf_task, lf_task);
    }

    async fn run_feed(
        uri: String,
        order_book: ConcurrentOrderBook,
        _is_delta: bool,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
        status: SharedConnectionStatus,
    ) {
        let mut retry_delay = Duration::from_secs(1);
        loop {
            status.set(ConnectionStatus::Connecting);
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutting down LOB feed {}", uri);
                        status.set(ConnectionStatus::Disconnected);
                        break;
                    } else {
                        continue;
                    }
                }
                connect_result = connect_async(&uri) => {
                    match connect_result {
                        Ok((ws_stream, _)) => {
                            status.set(ConnectionStatus::Connected);
                            debug!("Connected to WebSocket at {}", uri);
                            retry_delay = Duration::from_secs(1); // Reset on successful connect
                            let (_, mut read) = ws_stream.split();
                            loop {
                                tokio::select! {
                                    _ = shutdown_rx.changed() => {
                                        if *shutdown_rx.borrow() {
                                            info!("Shutting down LOB feed {}", uri);
                                            status.set(ConnectionStatus::Disconnected);
                                            return;
                                        }
                                    }
                                    maybe_msg = read.next() => {
                                        match maybe_msg {
                                            Some(msg) => {
                                                match msg {
                                                    Ok(Message::Text(text)) => {
                                                        if let Ok(parsed) = serde_json::from_str::<BinanceDepthUpdate>(&text) {
                                                            debug!("Parsed Binance depth update (text)");
                                                            let parsed_bids = Self::parse_levels(parsed.bids);
                                                            let parsed_asks = Self::parse_levels(parsed.asks);
                                                            order_book.apply_deltas(parsed_bids, parsed_asks).await;
                                                        } else {
                                                            warn!("Failed to parse depth update: {}", text);
                                                        }
                                                    }
                                                    Ok(Message::Binary(bin)) => {
                                                        if let Ok(text) = String::from_utf8(bin) {
                                                            if let Ok(parsed) = serde_json::from_str::<BinanceDepthUpdate>(&text) {
                                                                debug!("Parsed Binance depth update (binary)");
                                                                let parsed_bids = Self::parse_levels(parsed.bids);
                                                                let parsed_asks = Self::parse_levels(parsed.asks);
                                                                order_book.apply_deltas(parsed_bids, parsed_asks).await;
                                                            } else {
                                                                warn!("Failed to parse binary depth update: {}", text);
                                                            }
                                                        }
                                                    }
                                                    Ok(_) => {
                                                        // Ignore other message types
                                                    }
                                                    Err(e) => {
                                                        error!("WebSocket error on {}: {}", uri, e);
                                                        break;
                                                    }
                                                }
                                            }
                                            None => break,
                                        }
                                    }
                                }
                            }
                            // Stream closed - will reconnect
                            status.set(ConnectionStatus::Reconnecting);
                        }
                        Err(e) => {
                            error!("Failed to connect to {}: {}", uri, e);
                            status.set(ConnectionStatus::Reconnecting);
                        }
                    }
                }
            }
            if *shutdown_rx.borrow() {
                status.set(ConnectionStatus::Disconnected);
                break;
            }
            debug!("Reconnecting to {} in {:?}...", uri, retry_delay);
            sleep(retry_delay).await;
            retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60));
        }
    }

    async fn process_binance_update(update: BinanceDepthUpdate, order_book: &ConcurrentOrderBook) {
        let parsed_bids = LobFeedManager::parse_levels(update.bids);
        let parsed_asks = LobFeedManager::parse_levels(update.asks);
        order_book.apply_deltas(parsed_bids, parsed_asks).await;
    }

    fn parse_levels(levels: Vec<(String, String)>) -> Vec<(Decimal, Decimal)> {
        levels
            .into_iter()
            .filter_map(|(p, q)| {
                match (Decimal::from_str(&p), Decimal::from_str(&q)) {
                    (Ok(price), Ok(qty)) => Some((price, qty)),
                    _ => None,
                }
            })
            .collect()
    }
}
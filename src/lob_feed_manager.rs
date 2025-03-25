use crate::orderbook::ConcurrentOrderBook;
use futures_util::StreamExt;
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio::task;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Deserialize)]
pub struct BinanceDepthUpdate {
    #[serde(rename = "b")]
    pub bids: Vec<(String, String)>,
    #[serde(rename = "a")]
    pub asks: Vec<(String, String)>,
}

pub struct LobFeedManager {
    order_book: ConcurrentOrderBook,
    hf_uri: String,
    lf_uri: String,
}

impl LobFeedManager {

    pub fn new(hf_uri: String, lf_uri: String) -> Self {
        Self {
            order_book: ConcurrentOrderBook::new(),
            hf_uri,
            lf_uri,
        }
    }

    pub fn get_order_book(&self) -> ConcurrentOrderBook {
        self.order_book.clone()
    }

    pub async fn start(&self) {
        let hf_book = self.order_book.clone();
        let lf_book = self.order_book.clone();

        let hf_uri = self.hf_uri.clone();
        let lf_uri = self.lf_uri.clone();

        let hf_task = task::spawn(Self::run_feed(hf_uri, hf_book, true));
        let lf_task = task::spawn(Self::run_feed(lf_uri, lf_book, false));

        let _ = tokio::join!(hf_task, lf_task);
    }

    async fn run_feed(uri: String, order_book: ConcurrentOrderBook, _is_delta: bool) {
        let mut retry_delay = Duration::from_secs(1);
    
        loop {
            match connect_async(&uri).await {
                Ok((ws_stream, _)) => {
                    info!("✅ Connected to WebSocket at {}", uri);
                    let (_, mut read) = ws_stream.split();
    
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Ok(parsed) = serde_json::from_str::<BinanceDepthUpdate>(&text) {
                                    debug!("📥 Parsed Binance depth update (text)");
                                    let parsed_bids = Self::parse_levels(parsed.bids);
                                    let parsed_asks = Self::parse_levels(parsed.asks);
                                    order_book.apply_deltas(parsed_bids, parsed_asks).await;
                                } else {
                                    warn!("❌ Failed to parse depth update: {}", text);
                                }
                            }
                            Ok(Message::Binary(bin)) => {
                                if let Ok(text) = String::from_utf8(bin) {
                                    if let Ok(parsed) = serde_json::from_str::<BinanceDepthUpdate>(&text) {
                                        debug!("📥 Parsed Binance depth update (binary)");
                                        let parsed_bids = Self::parse_levels(parsed.bids);
                                        let parsed_asks = Self::parse_levels(parsed.asks);
                                        order_book.apply_deltas(parsed_bids, parsed_asks).await;
                                    } else {
                                        warn!("❌ Failed to parse binary depth update: {}", text);
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
    
                    warn!("⚠️ WebSocket stream closed for {}", uri);
                }
                Err(e) => {
                    error!("❌ Failed to connect to {}: {}", uri, e);
                }
            }
    
            warn!("🔁 Reconnecting to {} in {:?}...", uri, retry_delay);
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

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
#[serde(rename_all = "lowercase")]
pub enum LobMessage {
    Snapshot {
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
    },
    Delta {
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
    },
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

    async fn run_feed(uri: String, order_book: ConcurrentOrderBook, is_delta: bool) {
        let mut retry_delay = Duration::from_secs(1);

        loop {
            match connect_async(&uri).await {
                Ok((ws_stream, _)) => {
                    info!("Connected to WebSocket at {}", uri);
                    let (_, mut read) = ws_stream.split();

                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Ok(parsed) = serde_json::from_str::<LobMessage>(&text) {
                                    debug!("LOB MESSAGE RECEIVED (text): {}", &text);
                                    Self::process_message(parsed, &order_book, is_delta, &text).await;
                                } else {
                                    warn!("Failed to parse message: {}", text);
                                }
                            }
                            Ok(Message::Binary(bin)) => {
                                if let Ok(text) = String::from_utf8(bin) {
                                    if let Ok(parsed) = serde_json::from_str::<LobMessage>(&text) {
                                        debug!("LOB MESSAGE RECEIVED (binary): {}", &text);
                                        Self::process_message(parsed, &order_book, is_delta, &text).await;
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                error!("WebSocket error: {}", e);
                                break;
                            }
                        }
                    }

                    warn!("WebSocket stream closed for {}", uri);
                }
                Err(e) => {
                    error!("Failed to connect to {}: {}", uri, e);
                }
            }

            warn!("Reconnecting to {} in {:?}...", uri, retry_delay);
            sleep(retry_delay).await;
            retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60));
        }
    }

    async fn process_message(msg: LobMessage, order_book: &ConcurrentOrderBook, is_delta: bool, raw_json: &str) {
        match msg {
            LobMessage::Snapshot { bids, asks } => {
                let parsed_bids = Self::parse_levels(bids);
                let parsed_asks = Self::parse_levels(asks);
                if !is_delta {
                    order_book.apply_snapshot(parsed_bids, parsed_asks).await;
                    debug!("Snapshot applied for: {}", raw_json);
                }
            }
            LobMessage::Delta { bids, asks } => {
                let parsed_bids = Self::parse_levels(bids);
                let parsed_asks = Self::parse_levels(asks);
                if is_delta {
                    order_book.apply_deltas(parsed_bids, parsed_asks).await;
                    debug!("Delta applied for: {}", raw_json);
                }
            }
        }
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

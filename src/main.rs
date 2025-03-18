#![allow(warnings)]

use std::collections::VecDeque;
use std::sync::{Arc};
use std::thread;
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;

const LOB_URL: &str     = "wss://stream.binance.com:9443/ws/btcusdt@depth";
const TRADE_URL: &str   = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const QUEUE_SIZE: usize = 10000;

#[derive(Debug, Deserialize)]
struct DepthUpdate {
    e: String,
    E: u64,
    s: String,
    U: u64,
    u: u64,
    b: Vec<[String; 2]>,
    a: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct TradeUpdate {
    e: String,
    E: u64,
    s: String,
    t: u64,
    p: String,
    q: String,
    T: u64,
    m: bool,
    M: bool,
}

pub struct MarketState {
    pub bids: Arc<AsyncMutex<VecDeque<(f64, f64)>>>,
    pub asks: Arc<AsyncMutex<VecDeque<(f64, f64)>>>,
    pub trades: Arc<AsyncMutex<VecDeque<(f64, f64, i64)>>>,
}

impl MarketState {
    pub fn new() -> Self {
        Self {
            bids: Arc::new(AsyncMutex::new(VecDeque::with_capacity(1000))),
            asks: Arc::new(AsyncMutex::new(VecDeque::with_capacity(1000))),
            trades: Arc::new(AsyncMutex::new(VecDeque::with_capacity(1000))),
        }
    }
}

async fn connect_to_lob_websocket(market_state: Arc<MarketState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (ws_stream, _) = connect_async(LOB_URL).await?;
    println!("Connected to LOB WebSocket");
    let (_, mut read) = ws_stream.split();

    while let Some(Ok(message)) = read.next().await {
        if let Message::Text(text) = message {
            //println!("Raw LOB Message: {}", text);

            match serde_json::from_str::<DepthUpdate>(&text) {
                Ok(depth_update) => {
                    let mut bids = market_state.bids.lock().await;
                    let mut asks = market_state.asks.lock().await;

                    for bid in depth_update.b.iter() {
                        if let (Ok(price), Ok(qty)) = (bid[0].parse::<f64>(), bid[1].parse::<f64>()) {
                            bids.push_back((price, qty));
                            if bids.len() > 1000 { bids.pop_front(); }
                        }
                    }
                    for ask in depth_update.a.iter() {
                        if let (Ok(price), Ok(qty)) = (ask[0].parse::<f64>(), ask[1].parse::<f64>()) {
                            asks.push_back((price, qty));
                            if asks.len() > 1000 { asks.pop_front(); }
                        }
                    }

                    println!("Updated LOB: {} bids, {} asks", bids.len(), asks.len());
                }
                Err(err) => {
                    println!("Failed to parse LOB message: {}\nError: {}", text, err);
                }
            }
        }
    }
    Ok(())
}

async fn connect_to_trade_websocket(market_state: Arc<MarketState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (ws_stream, _) = connect_async(TRADE_URL).await?;
    println!("Connected to Trade WebSocket");
    let (_, mut read) = ws_stream.split();

    while let Some(Ok(message)) = read.next().await {
        if let Message::Text(text) = message {
            //println!("Raw Trade Message: {}", text);
            
            match serde_json::from_str::<TradeUpdate>(&text) {
                Ok(trade_update) => {
                    let mut trades = market_state.trades.lock().await;
                    if let (Ok(price), Ok(qty)) = (trade_update.p.parse::<f64>(), trade_update.q.parse::<f64>()) {
                        trades.push_back((price, qty, trade_update.T as i64));
                        if trades.len() > 10000 { trades.pop_front(); }
                        //println!("Stored Trade: Price: {}, Qty: {}, Time: {}", price, qty, trade_update.T);
                    }
                }
                Err(err) => {
                    println!("Failed to parse trade message: {}\nError: {}", text, err);
                }
            }
        }
    }
    Ok(())
}

fn periodic_printer(market_state: Arc<MarketState>) {
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(1));
            let bids = market_state.bids.blocking_lock();
            let asks = market_state.asks.blocking_lock();
            let trades = market_state.trades.blocking_lock();

            println!("\n=== LOB & Trade Data (Cumulative) ===");
            println!("Bids: {:?}", bids);
            println!("Asks: {:?}", asks);
            println!("Trades (Last {} entries): {:?}\n", trades.len(), trades.iter().rev().take(10).collect::<Vec<_>>());
        }
    });
}

#[tokio::main]
async fn main() {
    let market_state = Arc::new(MarketState::new());
    periodic_printer(Arc::clone(&market_state));

    let lob_task = tokio::spawn(connect_to_lob_websocket(Arc::clone(&market_state)));
    let trade_task = tokio::spawn(connect_to_trade_websocket(Arc::clone(&market_state)));

    let _ = tokio::join!(lob_task, trade_task);
}

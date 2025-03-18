#![allow(warnings)]

use std::collections::VecDeque;
use std::sync::{Arc};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;
use log::{info, warn, error, debug};
use env_logger;


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
    pub bids:   Arc<AsyncMutex<VecDeque<(f64, f64)>>>,
    pub asks:   Arc<AsyncMutex<VecDeque<(f64, f64)>>>,
    pub trades: Arc<AsyncMutex<VecDeque<(f64, f64, i64, bool)>>>,
}

impl MarketState {
    pub fn new() -> Self {
        Self {
            bids:   Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
            asks:   Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
            trades: Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
        }
    }

    pub async fn compute_midprice(&self) -> Option<f64> {
        let bids = self.bids.lock().await;
        let asks = self.asks.lock().await;

        let start = Instant::now();
        let best_bid = bids.iter().filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                                .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let best_ask = asks.iter().filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                                .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        let duration = start.elapsed();
        info!("midprice computed in {:?}", duration);

        match (best_bid, best_ask) 
        {
            (Some(bid), Some(ask)) => Some((bid.0 + ask.0) / 2.0),
            _ => None,
        }
    }


    pub async fn compute_spread(&self) -> Option<f64> {
        let bids = self.bids.lock().await;
        let asks = self.asks.lock().await;
    
        let start = Instant::now();
        let best_bid = bids.iter()
                           .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                           .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                           
        let best_ask = asks.iter()
                           .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                           .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        let duration = start.elapsed();
        info!("Spread computed in {:?}", duration);
    
        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => Some(ask.0 - bid.0),
            _ => None,
        }
    }


    pub async fn compute_imbalance(&self) -> Option<f64> {
        let bids = self.bids.lock().await;
        let asks = self.asks.lock().await;
    
        let start = Instant::now();
    
        let best_bid = bids.iter()
                           .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                           .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let best_ask = asks.iter()
                           .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                           .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        info!("Imbalance computed in {:?}", duration);
    
        match (best_bid, best_ask) {
            (Some((_, bid_qty)), Some((_, ask_qty))) if (bid_qty + ask_qty) > 0.0 => {
                Some((bid_qty - ask_qty) / (bid_qty + ask_qty))
            },
            _ => None,
        }
    }

    pub async fn compute_pwimbalance(&self) -> Option<f64> {
        let bids = self.bids.lock().await;
        let asks = self.asks.lock().await;
    
        let start = Instant::now();
    
        let best_bid = bids.iter()
                           .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                           .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let best_ask = asks.iter()
                           .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                           .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        info!("pwimbalance computed in {:?}", duration);
    
        match (best_bid, best_ask) {
            (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) => {
                let bid_product = bid_price * bid_qty;
                let ask_product = ask_price * ask_qty;
    
                if (bid_product + ask_product) > 0.0 {
                    Some((bid_product - ask_product) / (bid_product + ask_product))
                } else {
                    None
                }
            },
            _ => None,
        }
    }

    pub async fn compute_slope(&self) -> Option<f64> {
        let bids = self.bids.lock().await;
        let asks = self.asks.lock().await;
    
        let start = Instant::now();
    
        let best_bid = bids.iter()
                           .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                           .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let best_ask = asks.iter()
                           .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                           .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        info!("Slope computed in {:?}", duration);
    
        match (best_bid, best_ask) {
            (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) if (bid_qty + ask_qty) > 0.0 => {
                Some((ask_price - bid_price).abs() / (bid_qty + ask_qty))
            },
            _ => None,
        }
    }

    pub async fn compute_depthN(&self, n: usize) -> Option<f64> {
        let bids = self.bids.lock().await;
        let asks = self.asks.lock().await;
    
        let start = Instant::now();
    
        let mut sorted_bids = bids.iter()
                                  .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                                  .collect::<Vec<_>>();
        sorted_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let mut sorted_asks = asks.iter()
                                  .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
                                  .collect::<Vec<_>>();
        sorted_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let bid_depth: f64 = sorted_bids.iter().take(n).map(|&&(_, qty)| qty).sum();
        let ask_depth: f64 = sorted_asks.iter().take(n).map(|&&(_, qty)| qty).sum();
    
        let total_depth = bid_depth + ask_depth;
    
        let duration = start.elapsed();
        info!("Depth computed for top {} levels in {:?}", n, duration);
    
        if total_depth > 0.0 {
            Some(total_depth)
        } else {
            None
        }
    }

    pub async fn compute_vwap(&self, recent_ms: i64) -> Option<f64> {
        let trades = self.trades.lock().await;
    
        if trades.is_empty() {
            warn!("No trades available to compute VWAP");
            return None;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
    
        let start = Instant::now();
    
        // Clearly collect relevant trades first
        let relevant_trades: Vec<_> = trades.iter()
            .filter(|&&( _, _, ts, _)| latest_trade_ts - ts <= recent_ms)
            .collect();
    
        // Sum total volume separately and clearly
        let total_volume: f64 = relevant_trades.iter().map(|&&( _, qty, _, _)| qty).sum();
    
        if total_volume == 0.0 {
            debug!("VWAP: No volume in the last {} ms", recent_ms);
            return None;
        }
    
        // Compute VWAP separately and clearly
        let vwap_sum: f64 = relevant_trades.iter()
            .map(|&&(price, qty, _, _)| price * qty)
            .sum();
    
        let vwap = vwap_sum / total_volume;
    
        let duration = start.elapsed();
        info!("VWAP computed for last {} ms in {:?}", recent_ms, duration);
    
        Some(vwap)
    }
    
    pub async fn compute_trade_intensity(&self, recent_ms: i64) -> Option<f64> {
        let trades = self.trades.lock().await;
    
        if trades.is_empty() {
            warn!("No trades available to compute trade intensity");
            return None;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
    
        let start = Instant::now();
    
        let trade_count = trades.iter()
            .filter(|&&(_, _, ts, _)| latest_trade_ts - ts <= recent_ms)
            .count();

    
        if recent_ms == 0 {
            warn!("recent_ms cannot be zero for trade intensity calculation.");
            return None;
        }
    
        let intensity = (trade_count as f64) / (recent_ms as f64 / 1000.0);
    
        let duration = start.elapsed();
        info!(
            "Trade intensity computed for last {} ms ({} trades) in {:?}",
            recent_ms, trade_count, duration
        );
    
        Some(intensity)
    }

    pub async fn compute_trade_volume_imbalance(&self, recent_ms: i64) -> Option<f64> {
        let trades = self.trades.lock().await;
    
        if trades.is_empty() {
            warn!("No trades available to compute volume imbalance");
            return None;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
    
        let start = Instant::now();
    
        let relevant_trades: Vec<_> = trades.iter()
            .filter(|&&(_, _, ts, _)| latest_trade_ts - ts <= recent_ms)
            .collect();
    
        if relevant_trades.is_empty() {
            debug!("No trades in the recent {} ms for volume imbalance.", recent_ms);
            return None;
        }
    
        let mut buy_volume = 0.0;
        let mut sell_volume = 0.0;
    
        // Buyer_maker = true means trade was initiated by seller (aggressive sell)
        // Buyer_maker = false means trade was initiated by buyer (aggressive buy)
        for &&(price, qty, _, buyer_maker) in &relevant_trades {
            if buyer_maker {
                sell_volume += qty;  // aggressive sell
            } else {
                buy_volume += qty;   // aggressive buy
            }
        }
    
        let total_volume = buy_volume + sell_volume;
    
        if total_volume == 0.0 {
            debug!("Trade volumes neutral or insufficient to calculate imbalance.");
            return None;
        }
    
        let imbalance = (buy_volume - sell_volume) / total_volume;
    
        let duration = start.elapsed();
        info!(
            "Trade volume imbalance computed for last {} ms in {:?}",
            recent_ms, duration
        );
    
        Some(imbalance)
    }
    

}

async fn connect_to_lob_websocket(market_state: Arc<MarketState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> 
{
    let (ws_stream, _) = connect_async(LOB_URL).await?;
    info!("Connected to LOB WebSocket");
    let (_, mut read) = ws_stream.split();
    while let Some(Ok(message)) = read.next().await 
    {
        if let Message::Text(text) = message {
            let start = Instant::now();
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
                    let duration = start.elapsed();
                    info!("LOB ingestion completed ({} bids, {} asks) in {:?}", bids.len(), asks.len(), duration);
                }
                Err(err) => {
                    println!("Failed to parse LOB message: {}\nError: {}", text, err);
                }
            }
        }
    }
    Ok(())
}

async fn connect_to_trade_websocket(market_state: Arc<MarketState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> 
{
    let (ws_stream, _) = connect_async(TRADE_URL).await?;
    info!("Connected to Trade WebSocket");
    let (_, mut read) = ws_stream.split();
    while let Some(Ok(message)) = read.next().await {
        if let Message::Text(text) = message {
            let start = Instant::now();
            match serde_json::from_str::<TradeUpdate>(&text) {
                Ok(trade_update) => {
                    let mut trades = market_state.trades.lock().await;
                    if let (Ok(price), Ok(qty)) = (trade_update.p.parse::<f64>(), trade_update.q.parse::<f64>()) {
                        trades.push_back((price, qty, trade_update.T as i64, trade_update.m)); 
                        if trades.len() > 10000 { trades.pop_front(); }
                        //println!("Stored Trade: Price: {}, Qty: {}, Time: {}", price, qty, trade_update.T);
                    }
                    let duration = start.elapsed();
                    info!("Trade ingestion completed ({} total trades) in {:?}", trades.len(), duration);
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
    tokio::spawn(async move {
        loop {
            // wait 1 second before each snapshot
            tokio::time::sleep(Duration::from_secs(1)).await;

            
            let midprice = market_state.compute_midprice().await;

            let spread = market_state.compute_spread().await;

            let imbalance = market_state.compute_imbalance().await;

            let pwimbalance = market_state.compute_pwimbalance().await;

            let slope = market_state.compute_slope().await;

            let depth5 = market_state.compute_depthN(5).await;

            let depth10 = market_state.compute_depthN(10).await;
            

            // Obtain locks for state sizes (for additional informational logging)
            let bids = market_state.bids.lock().await;
            let asks = market_state.asks.lock().await;
            let trades = market_state.trades.lock().await;


            match midprice {
                Some(price) => {
                    info!(
                        "Snapshot => Midprice: {:.2}, Bids: {}, Asks: {}, Trades: {}",
                        price, bids.len(), asks.len(), trades.len()
                    );
                }
                None => {
                    warn!(
                        "Snapshot => Midprice not computable. Bids: {}, Asks: {}, Trades: {}",
                        bids.len(), asks.len(), trades.len()
                    );
                }
            }

            match spread {
                Some(spread) => {
                    info!("Spread: {:.2}", spread);
                }
                None => {
                    warn!("Spread not computable");
                }
            }

           match imbalance {
               Some(val) => info!("Current Imbalance: {:.3}", val),
               None => warn!("Imbalance not computable (missing best bid or ask)."),
            }

            match pwimbalance {
                Some(val) => info!("Current PW-Imbalance: {:.3}", val),
                None => warn!("PW-Imbalance not computable (missing best bid or ask)."),
            }
           

            match slope {
                Some(val) => info!("Current Order Book Slope: {:.6}", val),
                None => warn!("Order Book Slope not computable (missing best bid or ask)."),
            }


            match depth5 {
                Some(val) => info!("Total Order Book Depth (Top 5 levels): {:.3}", val),
                None => warn!("Depth N=5 not computable (no liquidity)."),
            }

            match depth10 {
                Some(val) => info!("Total Order Book Depth (Top 10 levels): {:.3}", val),
                None => warn!("Depth N=10 not computable (no liquidity)."),
            }
        }
    });
}

fn periodic_vwap_logger(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let vwap = market_state.compute_vwap(100).await;

            match vwap {
                Some(val) => info!("VWAP (100 ms): {:.2}", val),
                None => debug!("VWAP not computable in last 100 ms."),
            }
        }
    });
}

fn periodic_trade_intensity_logger(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let intensity = market_state.compute_trade_intensity(100).await;

            match intensity {
                Some(val) => info!("Trade Intensity (100 ms): {:.2} trades/sec", val),
                None => debug!("Trade intensity not computable in last 100 ms."),
            }
        }
    });
}

fn periodic_volume_imbalance_logger(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let imbalance = market_state.compute_trade_volume_imbalance(100).await;

            match imbalance {
                Some(val) => info!("Trade Volume Imbalance (100 ms): {:.3}", val),
                None => debug!("Trade volume imbalance not computable in last 100 ms."),
            }
        }
    });
}


#[tokio::main]
async fn main() 
{
    env_logger::init();

    let market_state = Arc::new(MarketState::new());
    
    periodic_printer(Arc::clone(&market_state));

    periodic_vwap_logger(Arc::clone(&market_state));   
    
    periodic_trade_intensity_logger(Arc::clone(&market_state)); 

    periodic_volume_imbalance_logger(Arc::clone(&market_state)); 

    let lob_task     = tokio::spawn(connect_to_lob_websocket(Arc::clone(&market_state)));
    let trade_task   = tokio::spawn(connect_to_trade_websocket(Arc::clone(&market_state)));
    
    let _            = tokio::join!(lob_task, trade_task);
}

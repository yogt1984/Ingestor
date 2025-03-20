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
use linregress::{FormulaRegressionBuilder, RegressionDataBuilder};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};


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
    pub bids:                               Arc<AsyncMutex<VecDeque<(f64, f64)>>>,
    pub asks:                               Arc<AsyncMutex<VecDeque<(f64, f64)>>>,
    pub trades:                             Arc<AsyncMutex<VecDeque<(f64, f64, i64, bool)>>>,
    last_vwap:                              AtomicU64,
    last_vwap_timestamp:                    AtomicU64,
    last_trade_intensity:                   AtomicU64, 
    last_trade_intensity_timestamp:         AtomicU64,
    last_trade_volume_imbalance:            AtomicU64, 
    last_trade_volume_imbalance_timestamp:  AtomicU64,
    last_cwtd:                              AtomicU64, 
    last_cwtd_timestamp:                    AtomicU64,
    last_amihud_lambda:                     AtomicU64,  
    last_amihud_timestamp:                  AtomicU64,  
    last_kyle_lambda:                       AtomicU64,
    last_kyle_timestamp:                    AtomicU64,
    last_price_impact:                      AtomicU64,  
    last_price_impact_timestamp:            AtomicU64,
    last_vwap_deviation:                    AtomicU64,  
    last_vwap_deviation_timestamp:          AtomicU64,
    last_intertrade_duration:               AtomicU64,
    last_intertrade_timestamp:              AtomicU64,
}

impl MarketState {
    pub fn new() -> Self {
        Self {
            bids:                                   Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
            asks:                                   Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
            trades:                                 Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
            last_vwap:                              AtomicU64::new(0),
            last_vwap_timestamp:                    AtomicU64::new(0),
            last_trade_intensity:                   AtomicU64::new(0),
            last_trade_intensity_timestamp:         AtomicU64::new(0),
            last_trade_volume_imbalance:            AtomicU64::new(0),
            last_trade_volume_imbalance_timestamp:  AtomicU64::new(0),
            last_cwtd:                              AtomicU64::new(0),
            last_cwtd_timestamp:                    AtomicU64::new(0),
            last_amihud_lambda:                     AtomicU64::new(0),
            last_amihud_timestamp:                  AtomicU64::new(0), 
            last_kyle_lambda:                       AtomicU64::new(0),
            last_kyle_timestamp:                    AtomicU64::new(0),
            last_price_impact:                      AtomicU64::new(0),
            last_price_impact_timestamp:            AtomicU64::new(0),
            last_vwap_deviation:                    AtomicU64::new(0),  
            last_vwap_deviation_timestamp:          AtomicU64::new(0),
            last_intertrade_duration:               AtomicU64::new(0),
            last_intertrade_timestamp:              AtomicU64::new(0),
        }
    }

    pub async fn update_lob(&self, new_bids: Vec<(f64, f64)>, new_asks: Vec<(f64, f64)>) {
        let mut bids = self.bids.lock().await;
        let mut asks = self.asks.lock().await;
    
        for (price, qty) in new_bids {
            bids.push_back((price, qty));
            if bids.len() > QUEUE_SIZE {
                bids.pop_front(); // Ensure bounded size
            }
        }
    
        for (price, qty) in new_asks {
            asks.push_back((price, qty));
            if asks.len() > QUEUE_SIZE {
                asks.pop_front(); // Ensure bounded size
            }
        }
    }
    
    pub async fn update_trades(&self, price: f64, qty: f64, ts: i64, buyer_maker: bool) {
        let mut trades = self.trades.lock().await;
    
        trades.push_back((price, qty, ts, buyer_maker));
        if trades.len() > QUEUE_SIZE {
            trades.pop_front(); // Ensure bounded size
        }
    }

    pub async fn compute_midprice(&self) -> Option<f64> {
        let start = Instant::now();
    
        // Concurrently acquire locks for bids and asks
        let (bids, asks) = tokio::join!(self.bids.lock(), self.asks.lock());
    
        // Find the best bid and best ask
        let best_bid = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let best_ask = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        info!("Midprice computed in {:?}", duration);
    
        // Compute midprice if both best bid and best ask exist
        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => Some((bid.0 + ask.0) / 2.0),
            _ => {
                warn!("Midprice not computable (missing best bid or ask).");
                None
            }
        }
    }
    
    pub async fn compute_spread(&self) -> Option<f64> {
        let start = Instant::now();
    
        // Concurrently acquire locks for bids and asks
        let (bids, asks) = tokio::join!(self.bids.lock(), self.asks.lock());
    
        // Find the best bid and best ask
        let best_bid = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let best_ask = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        info!("Spread computed in {:?}", duration);
    
        // Compute spread if both best bid and best ask exist
        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => Some(ask.0 - bid.0),
            _ => {
                warn!("Spread not computable (missing best bid or ask).");
                None
            }
        }
    }
    

    pub async fn compute_imbalance(&self) -> Option<f64> {
        let start = Instant::now();
    
        // Concurrently acquire locks for bids and asks
        let (bids, asks) = tokio::join!(self.bids.lock(), self.asks.lock());
    
        // Find the best bid and best ask with valid price and quantity
        let best_bid = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let best_ask = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        info!("Imbalance computed in {:?}", duration);
    
        // Compute imbalance only if both best bid and best ask exist
        match (best_bid, best_ask) {
            (Some((_, bid_qty)), Some((_, ask_qty))) => {
                let total_qty = bid_qty + ask_qty;
                if total_qty > 0.0 {
                    let imbalance = (bid_qty - ask_qty) / total_qty;
                    Some(imbalance)
                } else {
                    warn!("Imbalance not computable (zero total volume).");
                    None
                }
            }
            _ => {
                warn!("Imbalance not computable (missing best bid or ask).");
                None
            }
        }
    }
    
    pub async fn compute_pwimbalance(&self) -> Option<f64> {
        let start = Instant::now();
    
        // Concurrently acquire locks for bids and asks
        let (bids, asks) = tokio::join!(self.bids.lock(), self.asks.lock());
    
        // Find the best bid and best ask with valid price and quantity
        let best_bid = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let best_ask = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        info!("PW-Imbalance computed in {:?}", duration);
    
        // Compute price-weighted imbalance only if both best bid and best ask exist
        match (best_bid, best_ask) {
            (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) => {
                let bid_product = bid_price * bid_qty;
                let ask_product = ask_price * ask_qty;
                let total_product = bid_product + ask_product;
    
                if total_product > 0.0 {
                    let pw_imbalance = (bid_product - ask_product) / total_product;
                    Some(pw_imbalance)
                } else {
                    warn!("PW-Imbalance not computable (zero total weighted volume).");
                    None
                }
            }
            _ => {
                warn!("PW-Imbalance not computable (missing best bid or ask).");
                None
            }
        }
    }
    
    pub async fn compute_slope(&self) -> Option<f64> {
        let start = Instant::now();
    
        // Concurrently acquire locks for bids and asks
        let (bids, asks) = tokio::join!(self.bids.lock(), self.asks.lock());
    
        // Find the best bid and best ask with valid price and quantity
        let best_bid = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let best_ask = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        info!("Order Book Slope computed in {:?}", duration);
    
        // Compute slope if both best bid and best ask exist
        match (best_bid, best_ask) {
            (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) => {
                let total_qty = bid_qty + ask_qty;
                if total_qty > 0.0 {
                    let slope = (ask_price - bid_price).abs() / total_qty;
                    Some(slope)
                } else {
                    warn!("Slope not computable (zero total volume).");
                    None
                }
            }
            _ => {
                warn!("Slope not computable (missing best bid or ask).");
                None
            }
        }
    }

    pub async fn compute_depthN(&self, n: usize) -> Option<f64> {
        let start = Instant::now();
    
        // Concurrently acquire locks for bids and asks
        let (bids, asks) = tokio::join!(self.bids.lock(), self.asks.lock());
    
        // Filter and sort bids (descending order)
        let mut sorted_bids: Vec<_> = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .collect();
        sorted_bids.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    
        // Filter and sort asks (ascending order)
        let mut sorted_asks: Vec<_> = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .collect();
        sorted_asks.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        // Compute total depth for top `n` levels
        let bid_depth: f64 = sorted_bids.iter().take(n).map(|&&(_, qty)| qty).sum();
        let ask_depth: f64 = sorted_asks.iter().take(n).map(|&&(_, qty)| qty).sum();
        let total_depth = bid_depth + ask_depth;
    
        let duration = start.elapsed();
        info!("Total Depth for top {} levels computed in {:?}", n, duration);
    
        if total_depth > 0.0 {
            Some(total_depth)
        } else {
            warn!("Depth N={} not computable (no valid orders).", n);
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

        // Read last cached values (VWAP stored as scaled integer)
        let last_cached_vwap = self.last_vwap.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_vwap_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached VWAP
        if latest_trade_ts <= last_cached_ts {
            debug!("VWAP cache hit. Returning cached VWAP: {:.6}", last_cached_vwap);
            return Some(last_cached_vwap);
        }

        let start = Instant::now();

        // Filter relevant trades
        let relevant_trades: Vec<_> = trades.iter()
            .filter(|&&( _, _, ts, _)| latest_trade_ts - ts <= recent_ms)
            .collect();

        let total_volume: f64 = relevant_trades.iter().map(|&&( _, qty, _, _)| qty).sum();
        if total_volume == 0.0 {
            debug!("VWAP: No volume in the last {} ms", recent_ms);
            return None;
        }

        // Compute VWAP
        let vwap_sum: f64 = relevant_trades.iter()
            .map(|&&(price, qty, _, _)| price * qty)
            .sum();

        let vwap = vwap_sum / total_volume;

        // Store VWAP as an integer (scaled by 1_000_000)
        self.last_vwap.store((vwap * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_vwap_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!("VWAP computed for last {} ms in {:?}, Value: {:.6}", recent_ms, duration, vwap);

        Some(vwap)
    }
    
    pub async fn compute_trade_intensity(&self, recent_ms: i64) -> Option<f64> {
        if recent_ms == 0 {
            warn!("recent_ms cannot be zero for trade intensity calculation.");
            return None;
        }

        let trades = self.trades.lock().await;

        if trades.is_empty() {
            warn!("No trades available to compute trade intensity");
            return None;
        }

        let latest_trade_ts = trades.back().unwrap().2;

        // Read last cached values
        let last_cached_intensity = self.last_trade_intensity.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_trade_intensity_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached intensity
        if latest_trade_ts <= last_cached_ts {
            debug!("Trade intensity cache hit. Returning cached intensity: {:.6} trades/sec", last_cached_intensity);
            return Some(last_cached_intensity);
        }

        let start = Instant::now();

        // Filter recent trades
        let trade_count = trades.iter()
            .filter(|&&(_, _, ts, _)| latest_trade_ts - ts <= recent_ms)
            .count();

        let intensity = (trade_count as f64) / (recent_ms as f64 / 1000.0);

        // Store computed trade intensity
        self.last_trade_intensity.store((intensity * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_trade_intensity_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!(
            "Trade intensity computed for last {} ms ({} trades) in {:?}, Value: {:.6} trades/sec",
            recent_ms, trade_count, duration, intensity
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

        // Read last cached values (stored as scaled integer)
        let last_cached_imbalance = self.last_trade_volume_imbalance.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_trade_volume_imbalance_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached imbalance
        if latest_trade_ts <= last_cached_ts {
            debug!("Trade volume imbalance cache hit. Returning cached value: {:.6}", last_cached_imbalance);
            return Some(last_cached_imbalance);
        }

        let start = Instant::now();

        // Initialize volume counters
        let (mut buy_volume, mut sell_volume) = (0.0, 0.0);

        // Accumulate buy/sell volume efficiently
        for &(price, qty, ts, buyer_maker) in trades.iter().rev() {
            if latest_trade_ts - ts > recent_ms {
                break;
            }
            if buyer_maker {
                sell_volume += qty; // Aggressive sell
            } else {
                buy_volume += qty; // Aggressive buy
            }
        }

        let total_volume = buy_volume + sell_volume;
        if total_volume == 0.0 {
            debug!("Trade volumes neutral or insufficient to calculate imbalance.");
            return None;
        }

        let imbalance = (buy_volume - sell_volume) / total_volume;

        // Store computed imbalance (scaled for precision)
        self.last_trade_volume_imbalance.store((imbalance * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_trade_volume_imbalance_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!(
            "Trade volume imbalance computed for last {} ms in {:?}, Value: {:.6}",
            recent_ms, duration, imbalance
        );

        Some(imbalance)
    }
   
    pub async fn compute_cwtd(&self, recent_ms: i64) -> Option<f64> {
        let trades = self.trades.lock().await;

        if trades.is_empty() {
            warn!("No trades available to compute CWTD");
            return None;
        }

        let latest_trade_ts = trades.back().unwrap().2;

        // Read last cached CWTD
        let last_cached_cwtd = self.last_cwtd.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_cwtd_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached CWTD
        if latest_trade_ts <= last_cached_ts {
            debug!("CWTD cache hit. Returning cached value: {:.6}", last_cached_cwtd);
            return Some(last_cached_cwtd);
        }

        let start = Instant::now();

        // Compute CWTD in a single-pass iteration
        let cwtd: f64 = trades.iter().rev()
            .take_while(|&&(_, _, ts, _)| latest_trade_ts - ts <= recent_ms)
            .map(|&(_, qty, _, buyer_maker)| {
                let direction = if buyer_maker { -1.0 } else { 1.0 };
                qty * direction
            })
            .sum();

        // Cache computed CWTD (scaled for precision)
        self.last_cwtd.store((cwtd * 1_000_000.0)      as u64, Ordering::Relaxed);
        self.last_cwtd_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!(
            "CWTD computed for last {} ms in {:?}, result: {:.6}",
            recent_ms, duration, cwtd
        );

        Some(cwtd)
    }

    pub async fn compute_amihud_lambda(&self, interval_ms: i64, num_intervals: usize) -> Option<f64> {
        let trades = self.trades.lock().await;

        if trades.len() < 2 {
            warn!("Insufficient trades to compute Amihud's Lambda");
            return None;
        }

        let latest_trade_ts = trades.back().unwrap().2;
        let analysis_period_ms = interval_ms * num_intervals as i64;

        // Read last cached value
        let last_cached_lambda = self.last_amihud_lambda.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_amihud_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached Lambda
        if latest_trade_ts <= last_cached_ts {
            debug!("Amihud's Lambda cache hit. Returning cached value: {:.6}", last_cached_lambda);
            return Some(last_cached_lambda);
        }

        let start = Instant::now();

        // Efficient filtering using reverse iteration
        let mut interval_returns = Vec::with_capacity(num_intervals);
        let mut interval_volumes = Vec::with_capacity(num_intervals);

        let mut interval_start = latest_trade_ts - analysis_period_ms;
        let mut prev_price: Option<f64> = None;
        let mut interval_volume = 0.0;
        let mut last_trade_price: Option<f64> = None;

        for &(price, qty, ts, _) in trades.iter().rev() {
            if ts < interval_start {
                break;
            }

            if ts >= interval_start + interval_ms {
                if let (Some(prev_p), Some(last_p)) = (prev_price, last_trade_price) {
                    let ret = (last_p - prev_p).abs() / prev_p;
                    if interval_volume > 0.0 {
                        interval_returns.push(ret);
                        interval_volumes.push(interval_volume);
                    }
                }
                interval_start += interval_ms;
                interval_volume = 0.0;
                prev_price = last_trade_price;
            }

            interval_volume += qty;
            last_trade_price = Some(price);
            if prev_price.is_none() {
                prev_price = Some(price);
            }
        }

        // Compute Amihud's Lambda
        if interval_returns.is_empty() {
            debug!("No valid intervals for Amihud's Lambda calculation.");
            return None;
        }

        let amihud_lambda: f64 = interval_returns.iter()
            .zip(interval_volumes.iter())
            .map(|(&r, &v)| r / v)
            .sum::<f64>() / interval_returns.len() as f64;

        // Cache computed lambda
        self.last_amihud_lambda.store((amihud_lambda * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_amihud_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!(
            "Amihud's Lambda computed over {} intervals ({} ms each) in {:?}, value: {:.6}",
            interval_returns.len(), interval_ms, duration, amihud_lambda
        );
        Some(amihud_lambda)
    }

    pub async fn compute_kyle_lambda(&self, interval_ms: i64, num_intervals: usize) -> Option<f64> {
        let trades = self.trades.lock().await;

        if trades.len() < 2 {
            warn!("Insufficient trades to compute Kyle's Lambda");
            return None;
        }

        let latest_trade_ts = trades.back().unwrap().2;
        let analysis_period_ms = interval_ms * num_intervals as i64;

        // Read last cached value
        let last_cached_lambda = self.last_kyle_lambda.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_kyle_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached Lambda
        if latest_trade_ts <= last_cached_ts {
            debug!("Kyle's Lambda cache hit. Returning cached value: {:.6}", last_cached_lambda);
            return Some(last_cached_lambda);
        }

        let start = Instant::now();

        let mut returns = Vec::with_capacity(num_intervals);
        let mut signed_volumes = Vec::with_capacity(num_intervals);

        let mut interval_start = latest_trade_ts - analysis_period_ms;
        let mut prev_price: Option<f64> = None;
        let mut signed_volume = 0.0;
        let mut last_trade_price: Option<f64> = None;

        for &(price, qty, ts, buyer_maker) in trades.iter().rev() {
            if ts < interval_start {
                break;
            }

            if ts >= interval_start + interval_ms {
                if let (Some(prev_p), Some(last_p)) = (prev_price, last_trade_price) {
                    let ret = (last_p - prev_p) / prev_p;
                    returns.push(ret);
                    signed_volumes.push(signed_volume);
                }
                interval_start += interval_ms;
                signed_volume = 0.0;
                prev_price = last_trade_price;
            }

            let direction = if buyer_maker { -1.0 } else { 1.0 };
            signed_volume += qty * direction;
            last_trade_price = Some(price);
            if prev_price.is_none() {
                prev_price = Some(price);
            }
        }

        if returns.len() < 2 {
            debug!("Not enough intervals to run regression for Kyle's Lambda.");
            return None;
        }

        let data: Vec<(f64, Vec<f64>)> = returns.into_iter().zip(signed_volumes.into_iter().map(|v| vec![v])).collect();

        let regression_data = RegressionDataBuilder::new()
            .build_from(data.iter().map(|(y, x)| (y.to_string(), x.clone()))) // Convert to required format
            .ok()?;

        let model = FormulaRegressionBuilder::new()
            .data(&regression_data)
            .formula("y ~ x")
            .fit()
            .ok()?;

        let params = model.parameters();
        if params.len() < 2 {
            debug!("Regression model did not produce a valid slope coefficient.");
            return None;
        }

        let lambda = params[1]; // First parameter is intercept, second is Kyle's Lambda (slope)

        // Cache computed lambda
        self.last_kyle_lambda.store((lambda * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_kyle_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!(
            "Kyle's Lambda computed over {} intervals ({} ms each) in {:?}, value: {:.6}",
            num_intervals, interval_ms, duration, lambda
        );

        Some(lambda)
    }

    pub async fn compute_price_impact(&self, interval_ms: i64, num_intervals: usize) -> Option<f64> {
        let trades = self.trades.lock().await;

        if trades.len() < 2 {
            warn!("Insufficient trades to compute Price Impact");
            return None;
        }

        let latest_trade_ts = trades.back().unwrap().2;
        let analysis_period_ms = interval_ms * num_intervals as i64;

        // Read last cached value
        let last_cached_impact = self.last_price_impact.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_price_impact_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached Price Impact
        if latest_trade_ts <= last_cached_ts {
            debug!("Price Impact cache hit. Returning cached value: {:.6}", last_cached_impact);
            return Some(last_cached_impact);
        }

        let start = Instant::now();

        let mut price_impacts = Vec::with_capacity(num_intervals);
        let mut interval_start = latest_trade_ts - analysis_period_ms;
        let mut prev_price: Option<f64> = None;
        let mut interval_volume = 0.0;
        let mut last_trade_price: Option<f64> = None;

        for &(price, qty, ts, _) in trades.iter().rev() {
            if ts < interval_start {
                break;
            }

            if ts >= interval_start + interval_ms {
                if let (Some(prev_p), Some(last_p)) = (prev_price, last_trade_price) {
                    if interval_volume > 0.0 {
                        let impact = (last_p - prev_p).abs() / interval_volume;
                        price_impacts.push(impact);
                    }
                }
                interval_start += interval_ms;
                interval_volume = 0.0;
                prev_price = last_trade_price;
            }

            interval_volume += qty;
            last_trade_price = Some(price);
            if prev_price.is_none() {
                prev_price = Some(price);
            }
        }

        if price_impacts.is_empty() {
            debug!("No valid intervals for Price Impact calculation.");
            return None;
        }

        let avg_price_impact: f64 = price_impacts.iter().sum::<f64>() / price_impacts.len() as f64;

        // Cache computed Price Impact (scaled for precision)
        self.last_price_impact.store((avg_price_impact * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_price_impact_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!(
            "Price Impact computed over {} intervals ({} ms each) in {:?}, value: {:.6}",
            price_impacts.len(), interval_ms, duration, avg_price_impact
        );

        Some(avg_price_impact)
    }
   
    pub async fn compute_vwap_deviation(&self, recent_ms: i64) -> Option<f64> {
        let trades = self.trades.lock().await;

        if trades.is_empty() {
            warn!("No trades available to compute VWAP Deviation");
            return None;
        }

        let latest_trade = trades.back().unwrap();
        let latest_trade_ts = latest_trade.2;
        let p_current = latest_trade.0; // Most recent trade price

        // Read last cached value
        let last_cached_dev = self.last_vwap_deviation.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_vwap_deviation_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached VWAP Deviation
        if latest_trade_ts <= last_cached_ts {
            debug!("VWAP Deviation cache hit. Returning cached value: {:.6}", last_cached_dev);
            return Some(last_cached_dev);
        }

        let start = Instant::now();
        let vwap = self.compute_vwap(recent_ms).await?;

        if vwap == 0.0 {
            warn!("VWAP is zero, cannot compute deviation.");
            return None;
        }

        let vwap_deviation = (p_current - vwap) / vwap;

        // Cache computed VWAP Deviation (scaled for precision)
        self.last_vwap_deviation.store((vwap_deviation * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_vwap_deviation_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!(
            "VWAP Deviation computed for last {} ms in {:?}, value: {:.6}",
            recent_ms, duration, vwap_deviation
        );

        Some(vwap_deviation)
    }

    pub async fn compute_intertrade_duration(&self, recent_ms: i64) -> Option<f64> {
        let trades = self.trades.lock().await;

        if trades.len() < 2 {
            warn!("Insufficient trades to compute Intertrade Duration");
            return None;
        }

        let latest_trade_ts = trades.back().unwrap().2;

        // Read last cached value
        let last_cached_duration = self.last_intertrade_duration.load(Ordering::Relaxed) as f64 / 1_000.0;
        let last_cached_ts = self.last_intertrade_timestamp.load(Ordering::Relaxed) as i64;

        // If no new trades, return cached Intertrade Duration
        if latest_trade_ts <= last_cached_ts {
            debug!("Intertrade Duration cache hit. Returning cached value: {:.3} ms", last_cached_duration);
            return Some(last_cached_duration);
        }

        let start = Instant::now();
        let analysis_period_ms = recent_ms;

        // Collect relevant trade timestamps
        let trade_timestamps: Vec<i64> = trades.iter()
            .rev()
            .take_while(|&&(_, _, ts, _)| latest_trade_ts - ts <= analysis_period_ms)
            .map(|&(_, _, ts, _)| ts)
            .collect();

        if trade_timestamps.len() < 2 {
            debug!("Not enough trades in the last {} ms to compute Intertrade Duration.", recent_ms);
            return None;
        }

        // Compute time differences between consecutive trades
        let intertrade_durations: Vec<i64> = trade_timestamps.windows(2)
            .map(|window| window[0] - window[1]) // Ensure correct ordering
            .collect();

        // Compute average intertrade duration
        let avg_intertrade_duration: f64 = intertrade_durations.iter().sum::<i64>() as f64
            / intertrade_durations.len() as f64;

        // Cache computed Intertrade Duration (scaled for precision)
        self.last_intertrade_duration.store((avg_intertrade_duration * 1_000.0) as u64, Ordering::Relaxed);
        self.last_intertrade_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);

        let duration = start.elapsed();
        info!(
            "Intertrade Duration computed for last {} ms in {:?}, value: {:.3} ms",
            recent_ms, duration, avg_intertrade_duration
        );

        Some(avg_intertrade_duration)
    }
}

async fn connect_to_lob_websocket(market_state: Arc<MarketState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut retry_delay = Duration::from_secs(1); // Start with 1 second

    loop {
        match connect_async(LOB_URL).await {
            Ok((ws_stream, _)) => {
                info!("Connected to LOB WebSocket");
                let (_, mut read) = ws_stream.split();

                while let Some(Ok(message)) = read.next().await {
                    if let Message::Text(text) = message {
                        let start = Instant::now();
                        match serde_json::from_str::<DepthUpdate>(&text) {
                            Ok(depth_update) => {
                                let mut new_bids = Vec::new();
                                let mut new_asks = Vec::new();
                
                                for bid in &depth_update.b {
                                    if let (Ok(price), Ok(qty)) = (bid[0].parse::<f64>(), bid[1].parse::<f64>()) {
                                        new_bids.push((price, qty));
                                    }
                                }
                
                                for ask in &depth_update.a {
                                    if let (Ok(price), Ok(qty)) = (ask[0].parse::<f64>(), ask[1].parse::<f64>()) {
                                        new_asks.push((price, qty));
                                    }
                                }
                
                                // Call update_lob() instead of manually modifying the queues
                                market_state.update_lob(new_bids, new_asks).await;
                
                                let duration = start.elapsed();
                                debug!(
                                    "LOB ingestion completed in {:?}",
                                    duration
                                );
                            }
                            Err(err) => {
                                error!("LOB WebSocket: Failed to parse message: {}\nError: {}", text, err);
                            }
                        }
                    }
                }
                
                // If we reach here, connection was lost. Try to reconnect.
                warn!("LOB WebSocket connection lost. Reconnecting...");
            }
            Err(err) => {
                error!("Failed to connect to LOB WebSocket: {}", err);
            }
        }

        // Apply exponential backoff
        tokio::time::sleep(retry_delay).await;
        retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60)); // Max backoff: 60 seconds
    }
}

async fn connect_to_trade_websocket(market_state: Arc<MarketState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut retry_delay = Duration::from_secs(1); // Start with 1 second

    loop {
        match connect_async(TRADE_URL).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Trade WebSocket");
                let (_, mut read) = ws_stream.split();

                while let Some(Ok(message)) = read.next().await {
                    if let Message::Text(text) = message {
                        let start = Instant::now();
                        match serde_json::from_str::<TradeUpdate>(&text) {
                            Ok(trade_update) => {
                                if let (Ok(price), Ok(qty)) = (trade_update.p.parse::<f64>(), trade_update.q.parse::<f64>()) {
                                    // Call update_trades() instead of manually modifying trades
                                    market_state.update_trades(price, qty, trade_update.T as i64, trade_update.m).await;
                                } else {
                                    warn!("Trade WebSocket: Invalid price or quantity in message: {}", text);
                                }
                
                                let duration = start.elapsed();
                                debug!("Trade ingestion completed in {:?}", duration);
                            }
                            Err(err) => {
                                error!("Trade WebSocket: Failed to parse message: {}\nError: {}", text, err);
                            }
                        }
                    }
                }
                // If we reach here, connection was lost. Try to reconnect.
                warn!("Trade WebSocket connection lost. Reconnecting...");
            }
            Err(err) => {
                error!("Failed to connect to Trade WebSocket: {}", err);
            }
        }

        // Apply exponential backoff
        tokio::time::sleep(retry_delay).await;
        retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60)); // Max backoff: 60 seconds
    }
}

fn periodic_printer(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1000));
        loop {

            interval.tick().await;
            
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
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await; // Ensures consistent timing

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
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;

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
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;

            let imbalance = market_state.compute_trade_volume_imbalance(100).await;

            match imbalance {
                Some(val) => info!("Trade Volume Imbalance (100 ms): {:.3}", val),
                None => debug!("Trade volume imbalance not computable in last 100 ms."),
            }
        }
    });
}

fn periodic_cwtd_logger(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;

            let cwtd = market_state.compute_cwtd(100).await;

            match cwtd {
                Some(val) => info!("CWTD (100 ms): {:.3}", val),
                None => debug!("CWTD not computable in last 100 ms."),
            }
        }
    });
}

fn periodic_amihud_logger(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;

            let amihud_lambda = market_state.compute_amihud_lambda(100, 600).await;

            match amihud_lambda {
                Some(val) => info!("Amihud's Lambda (1-min window): {:.6}", val),
                None => debug!("Amihud's Lambda not computable."),
            }
        }
    });
}

fn periodic_kyle_lambda_logger(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        let interval_ms = 100;
        let num_intervals = (1000 / interval_ms) * 60;

        loop {
            interval.tick().await;

            let kyle_lambda = market_state
                .compute_kyle_lambda(interval_ms as i64, num_intervals as usize)
                .await;

            match kyle_lambda {
                Some(val) => info!("Kyle's Lambda (60s window): {:.6}", val),
                None => debug!("Kyle's Lambda not computable."),
            }
        }
    });
}

fn periodic_price_impact_logger(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let interval_ms = 100;
        let num_intervals = (1000 / interval_ms) * 1;

        loop {
            interval.tick().await;

            let price_impact = market_state
                .compute_price_impact(interval_ms as i64, num_intervals as usize)
                .await;

            match price_impact {
                Some(val) => info!("Price Impact (1s window): {:.6}", val),
                None => debug!("Price Impact not computable."),
            }
        }
    });
}

fn periodic_vwap_deviation_logger(market_state: Arc<MarketState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;

            let vwap_deviation = market_state.compute_vwap_deviation(60_000).await;

            match vwap_deviation {
                Some(val) => info!("VWAP Deviation (1-min window): {:.6}", val),
                None => debug!("VWAP Deviation not computable."),
            }
        }
    });
}


fn periodic_intertrade_duration_logger(market_state: Arc<MarketState>, update_period_as_secs: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(update_period_as_secs));
        loop {
            interval.tick().await;

            let intertrade_duration = market_state.compute_intertrade_duration(60_000).await;

            match intertrade_duration {
                Some(val) => info!("Intertrade Duration ({}s window): {:.3} ms", update_period_as_secs, val),
                None => debug!("Intertrade Duration not computable."),
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

    periodic_cwtd_logger(Arc::clone(&market_state));

    periodic_amihud_logger(Arc::clone(&market_state));

    periodic_kyle_lambda_logger(Arc::clone(&market_state));

    periodic_price_impact_logger(Arc::clone(&market_state));

    periodic_vwap_deviation_logger(Arc::clone(&market_state));

    periodic_intertrade_duration_logger(Arc::clone(&market_state), 60);

    let lob_task     = tokio::spawn(connect_to_lob_websocket(Arc::clone(&market_state)));
    let trade_task   = tokio::spawn(connect_to_trade_websocket(Arc::clone(&market_state)));
    
    let _            = tokio::join!(lob_task, trade_task);
}

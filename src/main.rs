#![allow(warnings)]

mod fsm;
mod ingestor;
mod connector;
mod orderbook;
mod lob_feed_manager;

use lob_feed_manager::LobFeedManager;

use crate::connector::LobConnector;
use crate::connector::TradesConnector;

use std::collections::VecDeque;
use std::sync::{Arc};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use log::{info, warn, error, debug};
use env_logger;
use linregress::{FormulaRegressionBuilder, RegressionDataBuilder};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::sleep;
use tokio::join;
use tokio::task;


const LOB_URL: &str     = "wss://stream.binance.com:9443/ws/btcusdt@depth";
const TRADE_URL: &str   = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const QUEUE_SIZE: usize = 10_000;

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
    pub bids_buffers:                       [Arc<AsyncMutex<VecDeque<(f64, f64)>>>; 7],
    pub asks_buffers:                       [Arc<AsyncMutex<VecDeque<(f64, f64)>>>; 7],
    pub trades:                             Arc<AsyncMutex<VecDeque<(f64, f64, i64, bool)>>>,
    pub pending_lob_updates:                mpsc::Sender<(Vec<(f64, f64)>, Vec<(f64, f64)>)>,
    last_midprice:                          AtomicU64,
    last_midprice_timestamp:                AtomicU64,
    last_spread:                            AtomicU64,
    last_spread_timestamp:                  AtomicU64,
    last_imbalance:                         AtomicU64,
    last_imbalance_timestamp:               AtomicU64,
    last_pw_imbalance:                      AtomicU64,
    last_pw_imbalance_timestamp:            AtomicU64,
    last_slope:                             AtomicU64,
    last_slope_timestamp:                   AtomicU64,
    last_depth5:                            AtomicU64,
    last_depth5_timestamp:                  AtomicU64,
    last_depth10:                           AtomicU64,
    last_depth10_timestamp:                 AtomicU64,
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
    pub fn new(sender: mpsc::Sender<(Vec<(f64, f64)>, Vec<(f64, f64)>)>) -> Self {
        Self {
            bids_buffers: [
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
            ],
            asks_buffers: [
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
                Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
            ],
            trades:                                 Arc::new(AsyncMutex::new(VecDeque::with_capacity(QUEUE_SIZE))),
            pending_lob_updates:                    sender,
            last_midprice:                          AtomicU64::new(0),
            last_midprice_timestamp:                AtomicU64::new(0),
            last_spread:                            AtomicU64::new(0),
            last_spread_timestamp:                  AtomicU64::new(0),
            last_imbalance:                         AtomicU64::new(0),
            last_imbalance_timestamp:               AtomicU64::new(0),
            last_pw_imbalance:                      AtomicU64::new(0),
            last_pw_imbalance_timestamp:            AtomicU64::new(0),
            last_slope:                             AtomicU64::new(0),
            last_slope_timestamp:                   AtomicU64::new(0),
            last_depth5:                            AtomicU64::new(0),
            last_depth5_timestamp:                  AtomicU64::new(0),
            last_depth10:                           AtomicU64::new(0),
            last_depth10_timestamp:                 AtomicU64::new(0),
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
        let mut clone_tasks = Vec::new();

        for i in 0..7 {
            let bids_buffer = self.bids_buffers[i].clone();
            let asks_buffer = self.asks_buffers[i].clone();
            let new_bids_clone = new_bids.clone();
            let new_asks_clone = new_asks.clone();

            let task = task::spawn(async move {
                let mut bids = bids_buffer.lock().await;
                let mut asks = asks_buffer.lock().await;

                bids.clear();
                asks.clear();

                bids.extend(new_bids_clone.iter().cloned());
                asks.extend(new_asks_clone.iter().cloned());

                // Maintain queue size limit
                while bids.len() > QUEUE_SIZE {
                    bids.pop_front();
                }
                while asks.len() > QUEUE_SIZE {
                    asks.pop_front();
                }
            });

            clone_tasks.push(task);
        }

        for task in clone_tasks {
            task.await.unwrap();
        }

        debug!("Order book cloned into 7 buffers in parallel successfully!");
    }
    
    pub async fn update_trades(&self, price: f64, qty: f64, ts: i64, buyer_maker: bool) {
        let mut trades = self.trades.lock().await;
    
        trades.push_back((price, qty, ts, buyer_maker));
        if trades.len() > QUEUE_SIZE {
            trades.pop_front(); // Ensure bounded size
        }

        debug!(
            "Trades Updated => Total Trades: {} (Queue Size: {})",
            trades.len(),
            QUEUE_SIZE
        );
    }

    pub async fn update_midprice(&self) {
        let start = Instant::now();
        let current_timestamp = Instant::now().elapsed().as_millis() as u64;
    
        // Lock bids and asks concurrently using buffer 0
        let (bids, asks) = join!(self.bids_buffers[0].lock(), self.asks_buffers[0].lock());
    
        // Get best bid
        let best_bid = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        // Get best ask
        let best_ask = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        // Compute midprice and spread
        if let (Some(&(bid_price, _)), Some(&(ask_price, _))) = (best_bid, best_ask) {
            let midprice = (bid_price + ask_price) / 2.0;
            let spread   = ask_price - bid_price;
    
            // Store values atomically
            self.last_midprice.store((midprice * 1_000_000.0) as u64, Ordering::Relaxed);
            self.last_midprice_timestamp.store(current_timestamp, Ordering::Relaxed);
            self.last_spread.store((spread * 1_000_000.0) as u64, Ordering::Relaxed);
            self.last_spread_timestamp.store(current_timestamp, Ordering::Relaxed);
    
            debug!(
                "midprice and spread updated: {:.6}, Spread: {:.6}, Timestamp: {}",
                midprice, spread, current_timestamp
            );
        } else {
            warn!(
                "Midprice update skipped: Missing best bid or ask. Bids: {}, Asks: {}",
                bids.len(),
                asks.len()
            );
        }
    
        debug!("Midprice update completed in {:?}", start.elapsed());
    }

    pub async fn update_imbalance(&self) {
        let start = Instant::now();
        let current_timestamp = Instant::now().elapsed().as_millis() as u64;
    
        // Lock bids and asks concurrently using buffer 1
        let (bids, asks) = join!(self.bids_buffers[1].lock(), self.asks_buffers[1].lock());
    
        // Get the quantity at the best bid
        let best_bid_qty = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, qty)| *qty);
    
        // Get the quantity at the best ask
        let best_ask_qty = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, qty)| *qty);
    
        let bid_qty = best_bid_qty.unwrap_or(1e-6);  // Prevent division by zero
        let ask_qty = best_ask_qty.unwrap_or(1e-6);
    
        let total_qty = bid_qty + ask_qty;
        if total_qty > 0.0 {
            let imbalance = (bid_qty - ask_qty) / total_qty;
    
            // Ensure we only update if there is new data
            let prev_timestamp = self.last_imbalance_timestamp.load(Ordering::Relaxed);
            if current_timestamp > prev_timestamp {
                self.last_imbalance.store((imbalance * 1_000_000.0) as u64, Ordering::Relaxed);
                self.last_imbalance_timestamp.store(current_timestamp, Ordering::Relaxed);
                debug!(
                    "imbalance updated: {:.6}, Timestamp: {}",
                    imbalance, current_timestamp
                );
            }
        } else {
            warn!(
                "Imbalance update skipped: Zero total volume. Bids: {}, Asks: {}",
                bids.len(),
                asks.len()
            );
        }
    
        debug!("Imbalance update completed in {:?}", start.elapsed());
    }
    
    pub async fn update_pwimbalance(&self) {
        let start = Instant::now();
        let current_timestamp = Instant::now().elapsed().as_millis() as u64;
    
        // Lock bids and asks concurrently using buffer 2
        let (bids, asks) = join!(self.bids_buffers[2].lock(), self.asks_buffers[2].lock());
    
        // Find the best bid with valid price and quantity
        let best_bid = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        // Find the best ask with valid price and quantity
        let best_ask = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        debug!("pw_imbalance computation time: {:?}", duration);
    
        let pw_imbalance = match (best_bid, best_ask) {
            (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) => {
                let bid_product = bid_price * bid_qty;
                let ask_product = ask_price * ask_qty;
                let total_product = bid_product + ask_product;
    
                if total_product > 0.0 {
                    let imbalance = (bid_product - ask_product) / total_product;
                    
                    Some(imbalance)
                } else {
                    warn!("pw_imbalance not computable (zero total weighted volume).");
                    None
                }
            }
            _ => {
                warn!(
                    "pw_imbalance not computable: Missing best bid or ask. Bids: {}, Asks: {}",
                    bids.len(),
                    asks.len()
                );
                None
            }
        };
    
        // Store computed imbalance in atomic variable (scaled for precision)
        let imbalance_value = (pw_imbalance.unwrap_or(0.0) * 1_000_000.0) as u64;
        self.last_pw_imbalance.store(imbalance_value, Ordering::Relaxed);
        self.last_pw_imbalance_timestamp.store(current_timestamp, Ordering::Relaxed);
    
        debug!(
            "updated pw_imbalance: {:.6} at timestamp {}",
            imbalance_value as f64 / 1_000_000.0,
            self.last_pw_imbalance_timestamp.load(Ordering::Relaxed)
        );
    }
    
    pub async fn update_slope(&self) {
        let start = Instant::now();
        let current_timestamp = Instant::now().elapsed().as_millis() as u64;
    
        // Lock bids and asks concurrently using buffer 3
        let (bids, asks) = join!(self.bids_buffers[3].lock(), self.asks_buffers[3].lock());
    
        // Find the best bid with valid price and quantity
        let best_bid = bids.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        // Find the best ask with valid price and quantity
        let best_ask = asks.iter()
            .filter(|&&(price, qty)| price > 0.0 && qty > 0.0)
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
        let duration = start.elapsed();
        debug!("slope computation time {:?}", duration);
    
        let slope = match (best_bid, best_ask) {
            (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) => {
                let total_qty = bid_qty + ask_qty;
                if total_qty > 0.0 {
                    Some((ask_price - bid_price).abs() / total_qty)
                } else {
                    warn!("Slope not computable (zero total volume).");
                    None
                }
            }
            _ => {
                warn!(
                    "Slope not computable: Missing best bid or ask. Bids: {}, Asks: {}",
                    bids.len(),
                    asks.len()
                );
                None
            }
        };
    
        // Store computed slope in atomic variable (scaled for precision)
        let slope_value = (slope.unwrap_or(0.0) * 1_000_000.0) as u64;
        self.last_slope.store(slope_value, Ordering::Relaxed);
        self.last_slope_timestamp.store(current_timestamp, Ordering::Relaxed);
    
        debug!(
            "slope updated slope: {:.6} at timestamp {}",
            slope_value as f64 / 1_000_000.0,
            self.last_slope_timestamp.load(Ordering::Relaxed)
        );
    }

    pub async fn update_depths(&self) {
        let start = Instant::now();
        let current_timestamp = Instant::now().elapsed().as_millis() as u64;
    
        // Lock bids and asks concurrently using buffer 4
        let (bids, asks) = join!(self.bids_buffers[4].lock(), self.asks_buffers[4].lock());
    
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
    
        // Compute total depth for top 5 levels
        let bid_depth5: f64 = sorted_bids.iter().take(5).map(|&&(_, qty)| qty).sum();
        let ask_depth5: f64 = sorted_asks.iter().take(5).map(|&&(_, qty)| qty).sum();
        let total_depth5 = bid_depth5 + ask_depth5;
    
        // Compute total depth for top 10 levels
        let bid_depth10: f64 = sorted_bids.iter().take(10).map(|&&(_, qty)| qty).sum();
        let ask_depth10: f64 = sorted_asks.iter().take(10).map(|&&(_, qty)| qty).sum();
        let total_depth10 = bid_depth10 + ask_depth10;
    
        let duration = start.elapsed();
        debug!("depts computation time: {:?}", duration);
    
        // Store depths as scaled integers for precision
        let depth5_value = (total_depth5 * 1_000_000.0) as u64;
        let depth10_value = (total_depth10 * 1_000_000.0) as u64;
    
        self.last_depth5.store(depth5_value, Ordering::Relaxed);
        self.last_depth5_timestamp.store(current_timestamp, Ordering::Relaxed);
    
        self.last_depth10.store(depth10_value, Ordering::Relaxed);
        self.last_depth10_timestamp.store(current_timestamp, Ordering::Relaxed);
    
        debug!(
            "depts updated => Depth5: {:.3}, Depth10: {:.3}, Timestamps: {} & {}",
            depth5_value as f64 / 1_000_000.0,
            depth10_value as f64 / 1_000_000.0,
            self.last_depth5_timestamp.load(Ordering::Relaxed),
            self.last_depth10_timestamp.load(Ordering::Relaxed)
        );
    }
    
    pub async fn update_vwap(&self, recent_ms: i64) {
        let trades = self.trades.lock().await;
    
        if trades.is_empty() {
            warn!("No trades available to compute VWAP");
            return;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
    
        // Read last cached values (VWAP stored as scaled integer)
        let last_cached_vwap = self.last_vwap.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_vwap_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, return cached VWAP
        if latest_trade_ts <= last_cached_ts {
            debug!("VWAP cache hit. Using cached VWAP: {:.6}", last_cached_vwap);
            return;
        }
    
        let start = Instant::now();
    
        // Efficient filtering: Reverse iterate until the time range is exceeded
        let mut total_volume = 0.0;
        let mut vwap_sum = 0.0;
    
        for &(price, qty, ts, _) in trades.iter().rev() {
            if latest_trade_ts - ts > recent_ms {
                break;
            }
            total_volume += qty;
            vwap_sum += price * qty;
        }
    
        if total_volume == 0.0 {
            debug!("VWAP: No volume in the last {} ms", recent_ms);
            return;
        }
    
        // Compute VWAP
        let vwap = vwap_sum / total_volume;
    
        // Store VWAP as an integer (scaled by 1_000_000)
        self.last_vwap.store((vwap * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_vwap_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);
    
        let duration = start.elapsed();
        info!(
            "VWAP updated for last {} ms in {:?}, Value: {:.6}",
            recent_ms, duration, vwap
        );
    }
    
    pub async fn update_trade_intensity(&self, recent_ms: i64) {
        if recent_ms == 0 {
            warn!("recent_ms cannot be zero for trade intensity calculation.");
            return;
        }
    
        let trades = self.trades.lock().await;
    
        if trades.is_empty() {
            warn!("No trades available to compute trade intensity");
            return;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
    
        // Read last cached values
        let last_cached_intensity = self.last_trade_intensity.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_trade_intensity_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, return cached intensity
        if latest_trade_ts <= last_cached_ts {
            debug!("Trade intensity cache hit. Using cached intensity: {:.6} trades/sec", last_cached_intensity);
            return;
        }
    
        let start = Instant::now();
    
        // Efficient filtering: Reverse iterate until time range is exceeded
        let trade_count = trades.iter()
            .rev()
            .take_while(|&&(_, _, ts, _)| latest_trade_ts - ts <= recent_ms)
            .count();
    
        let intensity = (trade_count as f64) / (recent_ms as f64 / 1000.0);
    
        // Store computed trade intensity
        self.last_trade_intensity.store((intensity * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_trade_intensity_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);
    
        let duration = start.elapsed();
        info!(
            "Trade intensity updated for last {} ms ({} trades) in {:?}, Value: {:.6} trades/sec",
            recent_ms, trade_count, duration, intensity
        );
    }
    
    pub async fn update_trade_volume_imbalance(&self, recent_ms: i64) {
        let trades = self.trades.lock().await;
    
        if trades.is_empty() {
            warn!("No trades available to compute volume imbalance");
            return;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
    
        // Read last cached values (stored as scaled integer)
        let last_cached_imbalance = self.last_trade_volume_imbalance.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_trade_volume_imbalance_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, return cached imbalance
        if latest_trade_ts <= last_cached_ts {
            debug!("Trade volume imbalance cache hit. Using cached value: {:.6}", last_cached_imbalance);
            return;
        }
    
        let start = Instant::now();
    
        // Initialize volume counters
        let (mut buy_volume, mut sell_volume) = (0.0, 0.0);
    
        // Accumulate buy/sell volume efficiently
        for &(price, qty, ts, buyer_maker) in trades.iter().rev() {
            if latest_trade_ts - ts > recent_ms {
                break; // Exit early if trade is older than the time window
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
            return;
        }
    
        let imbalance = (buy_volume - sell_volume) / total_volume;
    
        // Store computed imbalance (scaled for precision)
        self.last_trade_volume_imbalance.store((imbalance * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_trade_volume_imbalance_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);
    
        let duration = start.elapsed();
        info!(
            "Trade volume imbalance updated for last {} ms in {:?}, Value: {:.6}",
            recent_ms, duration, imbalance
        );
    }
    
    pub async fn update_cwtd(&self, recent_ms: i64) {
        let trades = self.trades.lock().await;
    
        if trades.is_empty() {
            warn!("No trades available to compute CWTD");
            return;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
    
        // Read last cached CWTD
        let last_cached_cwtd = self.last_cwtd.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_cwtd_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, return cached CWTD
        if latest_trade_ts <= last_cached_ts {
            debug!("CWTD cache hit. Using cached value: {:.6}", last_cached_cwtd);
            return;
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
        self.last_cwtd.store((cwtd * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_cwtd_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);
    
        let duration = start.elapsed();
        info!(
            "CWTD updated for last {} ms in {:?}, result: {:.6}",
            recent_ms, duration, cwtd
        );
    }
    
    pub async fn update_amihud_lambda(&self, interval_ms: i64, num_intervals: usize) {
        let trades = self.trades.lock().await;
    
        if trades.len() < 2 {
            warn!("Insufficient trades to compute Amihud's Lambda");
            return;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
        let analysis_period_ms = interval_ms * num_intervals as i64;
    
        // Read last cached value
        let last_cached_lambda = self.last_amihud_lambda.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_amihud_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, use cached Lambda
        if latest_trade_ts <= last_cached_ts {
            debug!("Amihud's Lambda cache hit. Using cached value: {:.6}", last_cached_lambda);
            return;
        }
    
        let start = Instant::now();
    
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
            return;
        }
    
        let amihud_lambda: f64 = interval_returns.iter()
            .zip(interval_volumes.iter())
            .map(|(&r, &v)| r / v)
            .sum::<f64>() / interval_returns.len() as f64;
    
        // Update stored lambda
        self.last_amihud_lambda.store((amihud_lambda * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_amihud_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);
    
        let duration = start.elapsed();
        info!(
            "Amihud's Lambda updated over {} intervals ({} ms each) in {:?}, value: {:.6}",
            interval_returns.len(), interval_ms, duration, amihud_lambda
        );
    }
    

    pub async fn update_kyle_lambda(&self, interval_ms: i64, num_intervals: usize) {
        let trades = self.trades.lock().await;
    
        if trades.len() < 2 {
            warn!("Insufficient trades to compute Kyle's Lambda");
            return;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
        let analysis_period_ms = interval_ms * num_intervals as i64;
    
        // Read last cached value
        let last_cached_lambda = self.last_kyle_lambda.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_kyle_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, use cached Lambda
        if latest_trade_ts <= last_cached_ts {
            debug!("Kyle's Lambda cache hit. Using cached value: {:.6}", last_cached_lambda);
            return;
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
            return;
        }
    
        let data: Vec<(f64, Vec<f64>)> = returns.into_iter().zip(signed_volumes.into_iter().map(|v| vec![v])).collect();
    
        let regression_data = RegressionDataBuilder::new()
            .build_from(data.iter().map(|(y, x)| (y.to_string(), x.clone()))) // Convert to required format
            .ok();
    
        let model = FormulaRegressionBuilder::new()
            .data(&regression_data.unwrap())
            .formula("y ~ x")
            .fit()
            .ok();
    
        if model.is_none() {
            debug!("Regression model failed for Kyle's Lambda.");
            return;
        }

        let model  = model.unwrap(); 
        let params = model.parameters();

        if params.len() < 2 {
            debug!("Regression model did not produce a valid slope coefficient.");
            return;
        }
    
        let lambda = params[1]; // First parameter is intercept, second is Kyle's Lambda (slope)
    
        // Update stored lambda
        self.last_kyle_lambda.store((lambda * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_kyle_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);
    
        let duration = start.elapsed();
        info!(
            "Kyle's Lambda updated over {} intervals ({} ms each) in {:?}, value: {:.6}",
            num_intervals, interval_ms, duration, lambda
        );
    }

    pub async fn update_price_impact(&self, interval_ms: i64, num_intervals: usize) {
        let trades = self.trades.lock().await;
    
        if trades.len() < 2 {
            warn!("Insufficient trades to compute Price Impact");
            return;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
        let analysis_period_ms = interval_ms * num_intervals as i64;
    
        // Read last cached value
        let last_cached_impact = self.last_price_impact.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_price_impact_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, use cached value
        if latest_trade_ts <= last_cached_ts {
            debug!("Price Impact cache hit. Using cached value: {:.6}", last_cached_impact);
            return;
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
            return;
        }
    
        let avg_price_impact: f64 = price_impacts.iter().sum::<f64>() / price_impacts.len() as f64;
    
        // Update stored Price Impact
        self.last_price_impact.store((avg_price_impact * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_price_impact_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);
    
        let duration = start.elapsed();
        info!(
            "Price Impact updated over {} intervals ({} ms each) in {:?}, value: {:.6}",
            price_impacts.len(), interval_ms, duration, avg_price_impact
        );
    }
   
    pub async fn update_vwap_deviation(&self, recent_ms: i64) {
        let trades = self.trades.lock().await;
    
        if trades.is_empty() {
            warn!("No trades available to compute VWAP Deviation");
            return;
        }
    
        let latest_trade = trades.back().unwrap();
        let latest_trade_ts = latest_trade.2;
        let p_current = latest_trade.0; // Most recent trade price
    
        // Read last cached value
        let last_cached_dev = self.last_vwap_deviation.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let last_cached_ts = self.last_vwap_deviation_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, use cached VWAP Deviation
        if latest_trade_ts <= last_cached_ts {
            debug!("VWAP Deviation cache hit. Using cached value: {:.6}", last_cached_dev);
            return;
        }
    
        let start = Instant::now();
    
        // Ensure VWAP is updated before using it
        self.update_vwap(recent_ms).await;
        let vwap = self.last_vwap.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    
        if vwap == 0.0 {
            warn!("VWAP is zero, cannot compute deviation.");
            return;
        }
    
        let vwap_deviation = (p_current - vwap) / vwap;
    
        // Update VWAP Deviation atomically
        self.last_vwap_deviation.store((vwap_deviation * 1_000_000.0) as u64, Ordering::Relaxed);
        self.last_vwap_deviation_timestamp.store(latest_trade_ts as u64, Ordering::Relaxed);
    
        let duration = start.elapsed();
        info!(
            "VWAP Deviation updated for last {} ms in {:?}, value: {:.6}",
            recent_ms, duration, vwap_deviation
        );
    }

    pub async fn update_intertrade_duration(&self, recent_ms: i64) {
        let trades = self.trades.lock().await;
    
        if trades.len() < 2 {
            warn!("Insufficient trades to compute Intertrade Duration");
            return;
        }
    
        let latest_trade_ts = trades.back().unwrap().2;
    
        // Read last cached value
        let last_cached_duration = self.last_intertrade_duration.load(Ordering::Relaxed) as f64 / 1_000.0;
        let last_cached_ts = self.last_intertrade_timestamp.load(Ordering::Relaxed) as i64;
    
        // If no new trades, skip computation
        if latest_trade_ts <= last_cached_ts {
            debug!("Intertrade Duration cache hit. Using cached value: {:.3} ms", last_cached_duration);
            return;
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
            return;
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
            "Intertrade Duration updated for last {} ms in {:?}, value: {:.3} ms",
            recent_ms, duration, avg_intertrade_duration
        );
    }
}

pub async fn process_lob_updates(
    market_state: Arc<MarketState>, 
    mut receiver: mpsc::Receiver<(Vec<(f64, f64)>, Vec<(f64, f64)>)>
) {
    while let Some((new_bids, new_asks)) = receiver.recv().await {
        let mut clone_tasks = Vec::new();

        for i in 0..7 {
            let bids_buffer = market_state.bids_buffers[i].clone();
            let asks_buffer = market_state.asks_buffers[i].clone();
            let new_bids_clone = new_bids.clone();
            let new_asks_clone = new_asks.clone();

            let task = task::spawn(async move {
                let mut bids = bids_buffer.lock().await;
                let mut asks = asks_buffer.lock().await;

                bids.truncate(0);
                asks.truncate(0);

                bids.extend(new_bids_clone.iter().cloned());
                asks.extend(new_asks_clone.iter().cloned());

                while bids.len() > QUEUE_SIZE {
                    bids.pop_front();
                }
                while asks.len() > QUEUE_SIZE {
                    asks.pop_front();
                }
            });

            clone_tasks.push(task);
        }

        for task in clone_tasks {
            task.await.unwrap();
        }

        debug!("LOB Processed: Updated all 7 buffers in parallel.");
    }
}

async fn connect_to_lob_websocket(
    market_state: Arc<MarketState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut retry_delay = Duration::from_secs(1); // Initial retry delay

    loop {
        match connect_async(LOB_URL).await {
            Ok((ws_stream, _)) => {
                info!("Connected to LOB WebSocket");
                let (_, mut read) = ws_stream.split();

                while let Some(Ok(message)) = read.next().await {
                    if let Message::Text(text) = message {
                        debug!("Raw JSON received: {}", text); // <<<< ADDED LINE

                        let start = Instant::now();

                        match serde_json::from_str::<DepthUpdate>(&text) {
                            Ok(depth_update) => {
                                debug!("Received LOB update: processing...");

                                let new_bids: Vec<(f64, f64)> = depth_update.b.iter()
                                    .filter_map(|bid| Some((bid[0].parse::<f64>().ok()?, bid[1].parse::<f64>().ok()?)))
                                    .collect();

                                let new_asks: Vec<(f64, f64)> = depth_update.a.iter()
                                    .filter_map(|ask| Some((ask[0].parse::<f64>().ok()?, ask[1].parse::<f64>().ok()?)))
                                    .collect();

                                if new_bids.is_empty() || new_asks.is_empty() {
                                    warn!("LOB update skipped: Empty bids or asks.");
                                    continue;
                                }

                                task::spawn({
                                    let market_state = market_state.clone();
                                    async move {
                                        market_state.update_lob(new_bids, new_asks).await;
                                        debug!("LOB updated successfully");
                                    }
                                });

                                let duration = start.elapsed();
                                debug!("LOB ingestion completed in {:?}", duration);
                            }
                            Err(err) => {
                                error!("Failed to parse LOB update: {}", err);
                            }
                        }
                    }
                }
            }
            Err(err) => {
                error!("Failed to connect to LOB WebSocket: {}", err);
                warn!("Reconnecting in {:?}...", retry_delay);
                sleep(retry_delay).await;
                retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60)); // Exponential backoff up to 60s
            }
        }
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

pub async fn periodic_lob_updater(market_state: Arc<MarketState>, update_periods: Vec<u64>) {
    loop {
        let start_time = tokio::time::Instant::now();

        let cloned_market_state = market_state.clone();

        let update_tasks = async {
            if update_periods.contains(&100) {
                cloned_market_state.update_midprice().await;
                sleep(tokio::time::Duration::from_millis(1)).await;
            }
            if update_periods.contains(&200) {
                cloned_market_state.update_imbalance().await;
                sleep(tokio::time::Duration::from_millis(1)).await;
            }
            if update_periods.contains(&300) {
                cloned_market_state.update_pwimbalance().await;
                sleep(tokio::time::Duration::from_millis(1)).await;
            }
            if update_periods.contains(&400) {
                cloned_market_state.update_slope().await;
                sleep(tokio::time::Duration::from_millis(1)).await;
            }
            if update_periods.contains(&500) {
                cloned_market_state.update_depths().await;
                sleep(tokio::time::Duration::from_millis(1)).await;
            }
            if update_periods.contains(&600) {
                cloned_market_state.update_vwap(500).await;
                sleep(tokio::time::Duration::from_millis(1)).await;
            }
        };

        tokio::join!(update_tasks);

        let elapsed = start_time.elapsed();
        if elapsed < Duration::from_millis(100) {  // Using std::time::Duration
            sleep(tokio::time::Duration::from_millis(100) - elapsed).await;
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    //let mut lob_100ms  = LobConnector::new("wss://stream.binance.com:9443/ws/btcusdt@depth@100ms".to_string());
    //let mut lob_1000ms = LobConnector::new("wss://stream.binance.com:9443/ws/btcusdt@depth".to_string());
    //let mut trades     = TradesConnector::new("wss://stream.binance.com:9443/ws/btcusdt@trade".to_string());

    // Run both concurrently
    //tokio::join!(
    //   lob_100ms.run_test(),
    //   lob_1000ms.run_test(),
    //   trades.run_test()
    //);

    let manager = LobFeedManager::new(
        "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms".to_string(),
        "wss://stream.binance.com:9443/ws/btcusdt@depth".to_string(),
    );

    manager.start().await;

}
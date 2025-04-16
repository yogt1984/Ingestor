use std::sync::Arc;
use tokio::sync::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use num::FromPrimitive;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Instant, Duration};

#[derive(Debug, Clone, Copy)]
pub enum OrderFlowEvent {
    BidOrder(Decimal),  
    AskOrder(Decimal),
    BidCancel,
    AskCancel,
}

#[derive(Debug, Clone)]
pub struct RollingFlowTracker {
    events: VecDeque<(Instant, OrderFlowEvent)>,
    window: Duration,
    cancel_penalty: Decimal,
    min_pressure: Decimal,
}

impl RollingFlowTracker {
    pub fn new(window_secs: u64) -> Self {
        Self {
            events: VecDeque::with_capacity(2000),
            window: Duration::from_secs(window_secs),
            cancel_penalty: dec!(0.35),
            min_pressure: dec!(2.5),
        }
    }

    pub fn add_event(&mut self, event: OrderFlowEvent) {
        let now = Instant::now();
        self.prune_old(now);
        self.events.push_back((now, event));
    }

    fn prune_old(&mut self, now: Instant) {
        let cutoff = now - self.window;
        while let Some((time, _)) = self.events.front() {
            if *time < cutoff {
                self.events.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn imbalance(&self) -> (Option<Decimal>, Decimal) {
        let mut bid_pressure = dec!(0);
        let mut ask_pressure = dec!(0);
        let now = Instant::now();

        for (time, event) in &self.events {
            let age_secs = (now - *time).as_secs_f64();
            let age_weight = 1.0 - (age_secs / self.window.as_secs_f64()).min(1.0);
            let weight = Decimal::from_f64(age_weight).unwrap_or(dec!(1));

            match event {
                OrderFlowEvent::BidOrder(qty) => bid_pressure += qty * weight,
                OrderFlowEvent::AskOrder(qty) => ask_pressure += qty * weight,
                OrderFlowEvent::BidCancel => bid_pressure -= self.cancel_penalty * weight,
                OrderFlowEvent::AskCancel => ask_pressure -= self.cancel_penalty * weight,
            }
        }

        let total_pressure = bid_pressure + ask_pressure;
        let imbalance = if total_pressure >= self.min_pressure {
            Some((bid_pressure - ask_pressure) / total_pressure)
        } else {
            None
        };

        (imbalance, total_pressure)
    }
}


#[derive(Debug, Clone)]
pub struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>, // price -> quantity (descending)
    asks: BTreeMap<Decimal, Decimal>, // price -> quantity (ascending)
    best_bid: Option<Decimal>,        // cached best bid price
    best_ask: Option<Decimal>,        // cached best ask price
    pub flow_tracker: RollingFlowTracker,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderBookSnapshot {
    pub best_bid: Option<(Decimal, Decimal)>,
    pub best_ask: Option<(Decimal, Decimal)>,
    pub mid_price: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub imbalance: Option<Decimal>,
    pub top_bids: Vec<(Decimal, Decimal)>,
    pub top_asks: Vec<(Decimal, Decimal)>,
    pub pwi_1: Option<Decimal>,
    pub pwi_5: Option<Decimal>,
    pub pwi_25: Option<Decimal>,
    pub pwi_50: Option<Decimal>,
    pub bid_slope: Option<Decimal>,
    pub ask_slope: Option<Decimal>,
    pub volume_imbalance_top5: Option<Decimal>,
    pub bid_depth_ratio: Option<Decimal>,
    pub ask_depth_ratio: Option<Decimal>,
    pub bid_volume_001: Option<Decimal>,
    pub ask_volume_001: Option<Decimal>,
    pub bid_avg_distance: Option<Decimal>,
    pub ask_avg_distance: Option<Decimal>,
    pub order_flow_imbalance: Option<Decimal>,
    pub order_flow_pressure: Decimal,  
}

impl OrderBook {
    /// Creates a new, empty order book.
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            best_bid: None,
            best_ask: None,
            flow_tracker: RollingFlowTracker::new(10),  // 10-second window
        }
    }

    /// Replaces current book state with full snapshot.
    pub fn apply_snapshot(&mut self, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) {
        self.bids.clear();
        self.asks.clear();

        for (price, quantity) in bids {
            if price >= dec!(0) && quantity >= dec!(0) {
                self.bids.insert(price, quantity);
            }
        }

        for (price, quantity) in asks {
            if price >= dec!(0) && quantity >= dec!(0) {
                self.asks.insert(price, quantity);
            }
        }

        self.update_best_bid_ask();
    }

    pub fn apply_deltas(&mut self, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) {
        // Process bids
        for (price, qty) in bids {
            let event = if qty == dec!(0) {
                if self.bids.contains_key(&price) {
                    OrderFlowEvent::BidCancel
                } else {
                    continue;  // Not a real cancel
                }
            } else {
                OrderFlowEvent::BidOrder(qty)
            };
            self.flow_tracker.add_event(event);

            // Update book
            if qty == dec!(0) {
                self.bids.remove(&price);
            } else {
                self.bids.insert(price, qty);
            }
        }

        // Process asks (mirror of bids)
        for (price, qty) in asks {
            let event = if qty == dec!(0) {
                if self.asks.contains_key(&price) {
                    OrderFlowEvent::AskCancel
                } else {
                    continue;
                }
            } else {
                OrderFlowEvent::AskOrder(qty)
            };
            self.flow_tracker.add_event(event);

            if qty == dec!(0) {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, qty);
            }
        }

        self.update_best_bid_ask();
    }

    fn update_best_bid_ask(&mut self) {
        self.best_bid = self.bids.keys().next_back().cloned();
        self.best_ask = self.asks.keys().next().cloned();
    }

    /// Returns the best bid price and quantity.
    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.best_bid
            .and_then(|price| self.bids.get(&price).map(|&qty| (price, qty)))
    }

    /// Returns the best ask price and quantity.
    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.best_ask
            .and_then(|price| self.asks.get(&price).map(|&qty| (price, qty)))
    }

    /// Computes mid-price = (best_bid + best_ask) / 2.
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / dec!(2)),
            _ => None,
        }
    }

    /// Computes order book imbalance.
    pub fn order_book_imbalance(&self) -> Option<Decimal> {
        let bid = self.best_bid?;
        let ask = self.best_ask?;
    
        let bid_qty = self.bids.get(&bid)?;
        let ask_qty = self.asks.get(&ask)?;
    
        let total = *bid_qty + *ask_qty;
        if total == dec!(0) {
            return None;
        }
    
        Some(*bid_qty / total)
    }

    pub fn price_weighted_imbalance_percent(&self, percent: Decimal) -> Option<Decimal> {
        let mid = self.mid_price()?;
        let range = mid * percent / dec!(100);
        let lower = mid - range;
        let upper = mid + range;
    
        let bid_weighted: Decimal = self.bids
            .iter()
            .filter(|(&price, _)| price >= lower)
            .map(|(&price, &qty)| price * qty)
            .sum();
    
        let ask_weighted: Decimal = self.asks
            .iter()
            .filter(|(&price, _)| price <= upper)
            .map(|(&price, &qty)| price * qty)
            .sum();
    
        let total = bid_weighted + ask_weighted;
        if total > dec!(0) {
            Some(bid_weighted / total)
        } else {
            None
        }
    }
    

    /// Returns volume at specific price (0 if not present).
    pub fn volume_at_price(&self, price: Decimal, is_bid: bool) -> Decimal {
        if is_bid {
            self.bids.get(&price).cloned().unwrap_or(dec!(0))
        } else {
            self.asks.get(&price).cloned().unwrap_or(dec!(0))
        }
    }

    /// Cumulative volume from price level and inwards.
    pub fn cumulative_volume_up_to(&self, price: Decimal, is_bid: bool) -> Decimal {
        let map = if is_bid { &self.bids } else { &self.asks };
        map.iter()
            .take_while(|(&p, _)| if is_bid { p >= price } else { p <= price })
            .map(|(_, &qty)| qty)
            .sum()
    }

    /// Returns the top N bids.
    pub fn top_bids(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.bids.iter().rev().take(n).map(|(&p, &q)| (p, q)).collect()
    }

    /// Returns the top N asks.
    pub fn top_asks(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.asks.iter().take(n).map(|(&p, &q)| (p, q)).collect()
    }

    /// Computes the spread (difference between best ask and best bid).
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    pub fn slope(&self, levels: usize) -> Option<(Decimal, Decimal)> {
        let best_bid = self.best_bid?;
        let best_ask = self.best_ask?;
    
        // Calculate bid slope
        let mut bid_numerator = dec!(0);
        let mut bid_denominator = dec!(0);
        for (price, qty) in self.bids.iter().rev().take(levels) {
            let dist = best_bid - *price;
            bid_numerator += dist * *qty;
            bid_denominator += *qty;
        }
        let bid_slope = if bid_denominator > dec!(0) {
            bid_numerator / bid_denominator
        } else {
            dec!(0)
        };
    
        // Calculate ask slope
        let mut ask_numerator = dec!(0);
        let mut ask_denominator = dec!(0);
        for (price, qty) in self.asks.iter().take(levels) {
            let dist = *price - best_ask;
            ask_numerator += dist * *qty;
            ask_denominator += *qty;
        }
        let ask_slope = if ask_denominator > dec!(0) {
            ask_numerator / ask_denominator
        } else {
            dec!(0)
        };
    
        Some((bid_slope, ask_slope))
    }

    pub fn volume_imbalance(&self) -> Option<Decimal> {
        let bid_qty: Decimal = self.bids.values().take(5).copied().sum();
        let ask_qty: Decimal = self.asks.values().take(5).copied().sum();
        let total = bid_qty + ask_qty;
        if total > dec!(0) {
            Some(bid_qty / total)
        } else {
            None
        }
    }

    pub fn depth_ratio(&self) -> Option<(Decimal, Decimal)> {
        let bid_top_3: Decimal = self.bids.iter().rev().take(3).map(|(_, &q)| q).sum();
        let bid_top_10: Decimal = self.bids.iter().rev().take(10).map(|(_, &q)| q).sum();

        let ask_top_3: Decimal = self.asks.iter().take(3).map(|(_, &q)| q).sum();
        let ask_top_10: Decimal = self.asks.iter().take(10).map(|(_, &q)| q).sum();

        let bid_ratio = if bid_top_10 > dec!(0) { bid_top_3 / bid_top_10 } else { dec!(0) };
        let ask_ratio = if ask_top_10 > dec!(0) { ask_top_3 / ask_top_10 } else { dec!(0) };

        Some((bid_ratio, ask_ratio))
    }

    pub fn volume_within_percent_range(&self, percent: Decimal) -> Option<(Decimal, Decimal)> {
        let mid = self.mid_price()?;
        let range = mid * percent / dec!(100);
    
        let lower = mid - range;
        let upper = mid + range;
    
        let bid_volume: Decimal = self.bids
            .iter()
            .filter(|(&p, _)| p >= lower)
            .map(|(_, &q)| q)
            .sum();
    
        let ask_volume: Decimal = self.asks
            .iter()
            .filter(|(&p, _)| p <= upper)
            .map(|(_, &q)| q)
            .sum();
    
        Some((bid_volume, ask_volume))
    }

    pub fn avg_price_distance(&self, levels: usize) -> Option<(Decimal, Decimal)> {
        let mid = self.mid_price()?;
    
        let bid_dist: Decimal = self.bids.iter().rev().take(levels)
            .map(|(&p, _)| mid - p)
            .sum();
        let ask_dist: Decimal = self.asks.iter().take(levels)
            .map(|(&p, _)| p - mid)
            .sum();
    
        let bid_avg = bid_dist / Decimal::from(levels as u64);
        let ask_avg = ask_dist / Decimal::from(levels as u64);
    
        Some((bid_avg, ask_avg))
    }

    pub fn get_snapshot(&self) -> OrderBookSnapshot {
        let best_bid = self.best_bid();
        let best_ask = self.best_ask();
        
        // Get flow metrics from the tracker
        let (flow_imbalance, flow_pressure) = self.flow_tracker.imbalance();
    
        OrderBookSnapshot {
            best_bid,
            best_ask,
            mid_price: self.mid_price(),
            spread: self.spread(),
            imbalance: self.order_book_imbalance(),
            top_bids: self.top_bids(5),
            top_asks: self.top_asks(5),
            pwi_1: self.price_weighted_imbalance_percent(dec!(1)),
            pwi_5: self.price_weighted_imbalance_percent(dec!(5)),
            pwi_25: self.price_weighted_imbalance_percent(dec!(25)),
            pwi_50: self.price_weighted_imbalance_percent(dec!(50)),
            bid_slope: self.slope(5).map(|(b, _)| b),
            ask_slope: self.slope(5).map(|(_, a)| a),
            volume_imbalance_top5: self.volume_imbalance(),
            bid_depth_ratio: self.depth_ratio().map(|(b, _)| b),
            ask_depth_ratio: self.depth_ratio().map(|(_, a)| a),
            bid_volume_001: self.volume_within_percent_range(dec!(0.01)).map(|(b, _)| b),
            ask_volume_001: self.volume_within_percent_range(dec!(0.01)).map(|(_, a)| a),
            bid_avg_distance: self.avg_price_distance(5).map(|(b, _)| b),
            ask_avg_distance: self.avg_price_distance(5).map(|(_, a)| a),
            order_flow_imbalance: flow_imbalance,
            order_flow_pressure: flow_pressure,
        }
    }
}

/// Thread-safe wrapper for the order book using Arc<RwLock<_>>.
#[derive(Debug, Clone)]
pub struct ConcurrentOrderBook {
    inner: Arc<RwLock<OrderBook>>,
}

impl ConcurrentOrderBook {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(OrderBook::new())),
        }
    }

    pub async fn apply_snapshot(&self, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) {
        let mut book = self.inner.write().await;
        book.apply_snapshot(bids, asks);
    }

    pub async fn apply_deltas(&self, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) {
        let mut book = self.inner.write().await;
        book.apply_deltas(bids, asks);
    }

    pub async fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        let book = self.inner.read().await;
        book.best_bid()
    }

    pub async fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        let book = self.inner.read().await;
        book.best_ask()
    }

    pub async fn mid_price(&self) -> Option<Decimal> {
        let book = self.inner.read().await;
        book.mid_price()
    }

    pub async fn order_book_imbalance(&self) -> Option<Decimal> {
        let book = self.inner.read().await;
        book.order_book_imbalance()
    }

    pub async fn volume_at_price(&self, price: Decimal, is_bid: bool) -> Decimal {
        let book = self.inner.read().await;
        book.volume_at_price(price, is_bid)
    }

    pub async fn cumulative_volume_up_to(&self, price: Decimal, is_bid: bool) -> Decimal {
        let book = self.inner.read().await;
        book.cumulative_volume_up_to(price, is_bid)
    }

    pub async fn top_bids(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        let book = self.inner.read().await;
        book.top_bids(n)
    }

    pub async fn top_asks(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        let book = self.inner.read().await;
        book.top_asks(n)
    }

    pub async fn spread(&self) -> Option<Decimal> {
        let book = self.inner.read().await;
        book.spread()
    }

    pub async fn slope(&self, levels: usize) -> Option<(Decimal, Decimal)> {
        let book = self.inner.read().await;
        book.slope(levels)
    }

    pub async fn volume_imbalance(&self) -> Option<Decimal> {
        let book = self.inner.read().await;
        book.volume_imbalance()
    }

    pub async fn price_weighted_imbalance_percent(&self, percent: Decimal) -> Option<Decimal> {
        let book = self.inner.read().await;
        book.price_weighted_imbalance_percent(percent)
    }

    pub async fn depth_ratio(&self) -> Option<(Decimal, Decimal)> {
        let book = self.inner.read().await;
        book.depth_ratio()
    }
    
    pub async fn volume_within_percent_range(&self, percent: Decimal) -> Option<(Decimal, Decimal)> {
        let book = self.inner.read().await;
        book.volume_within_percent_range(percent)
    }
    
    pub async fn avg_price_distance(&self, levels: usize) -> Option<(Decimal, Decimal)> {
        let book = self.inner.read().await;
        book.avg_price_distance(levels)
    }

    pub async fn get_flow_imbalance(&self) -> (Option<Decimal>, Decimal) {
        let book = self.inner.read().await;
        book.flow_tracker.imbalance()
    }

    pub async fn get_snapshot(&self) -> OrderBookSnapshot {
        let book = self.inner.read().await;
        let (flow_imb, flow_pressure) = book.flow_tracker.imbalance();
        book.get_snapshot()
    }
}
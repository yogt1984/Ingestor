use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Clone)]
pub struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>, // price -> quantity (descending)
    asks: BTreeMap<Decimal, Decimal>, // price -> quantity (ascending)
    best_bid: Option<Decimal>,        // cached best bid price
    best_ask: Option<Decimal>,        // cached best ask price
}

impl OrderBook {
    /// Creates a new, empty order book.
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            best_bid: None,
            best_ask: None,
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

    /// Applies incremental delta updates to the order book.
    pub fn apply_deltas(&mut self, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) {
        for (price, quantity) in bids {
            if price >= dec!(0) && quantity >= dec!(0) {
                if quantity == dec!(0) {
                    self.bids.remove(&price);
                } else {
                    self.bids.insert(price, quantity);
                }
            }
        }

        for (price, quantity) in asks {
            if price >= dec!(0) && quantity >= dec!(0) {
                if quantity == dec!(0) {
                    self.asks.remove(&price);
                } else {
                    self.asks.insert(price, quantity);
                }
            }
        }

        self.update_best_bid_ask();
    }

    /// Updates cached best bid/ask.
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
}
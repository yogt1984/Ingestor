use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use thiserror::Error;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct Trade {
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp: u64,
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone)]
pub struct TradesLog {
    trades: VecDeque<Trade>,
    max_len: usize,
    trade_count: usize,
    buy_volume: Decimal,
    sell_volume: Decimal,
    stats_dirty: bool,
    cached_stats: CachedStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeLogSnapshot {
    pub last_price: Option<Decimal>,
    pub trade_imbalance: Option<Decimal>,
    pub vwap_total: Option<Decimal>,
    pub price_change: Option<Decimal>,
    pub avg_trade_size: Option<Decimal>,
    pub signed_count_momentum: i64,
    pub trade_rate_10s: Option<f64>,
    pub vwap_10: Option<Decimal>,
    pub vwap_50: Option<Decimal>,
    pub vwap_100: Option<Decimal>,
    pub vwap_1000: Option<Decimal>,
    pub aggr_ratio_10: Option<Decimal>,
    pub aggr_ratio_50: Option<Decimal>,
    pub aggr_ratio_100: Option<Decimal>,
    pub aggr_ratio_1000: Option<Decimal>,
}

#[derive(Debug, Clone, Default)]
struct CachedStats {
    trade_imbalance: Option<Decimal>,
    vwap_total: Option<Decimal>,
    price_change: Option<Decimal>,
    last_price: Option<Decimal>,
    avg_trade_size: Option<Decimal>,
    signed_count_momentum: i64,
}

#[derive(Debug, Error)]
pub enum TradesLogError {
    #[error("Insufficient trades available")]
    InsufficientTrades,
    #[error("Zero volume in window")]
    ZeroVolume,
    #[error("Invalid window size")]
    InvalidWindowSize,
}

impl TradesLog {
    pub fn new(max_len: usize) -> Self {
        Self {
            trades: VecDeque::with_capacity(max_len),
            max_len,
            trade_count: 0,
            buy_volume: dec!(0),
            sell_volume: dec!(0),
            stats_dirty: true,
            cached_stats: CachedStats::default(),
        }
    }

    fn update_cached_stats(&mut self) {
        if !self.stats_dirty {
            return;
        }

        let total_volume = self.buy_volume + self.sell_volume;

        self.cached_stats.trade_imbalance = if total_volume > dec!(0) {
            Some(self.buy_volume / total_volume)
        } else {
            None
        };

        self.cached_stats.vwap_total = if total_volume > dec!(0) {
            let last_price = self.trades.back().map(|t| t.price).unwrap_or(dec!(0));
            Some((self.buy_volume + self.sell_volume) * last_price / total_volume)
        } else {
            None
        };

        self.cached_stats.price_change = match (self.trades.len(), self.cached_stats.last_price) {
            (_, None) => None,
            (0, _) => None,
            (_, Some(prev)) => {
                let current = self.trades.back().unwrap().price;
                Some(current - prev)
            }
        };

        self.cached_stats.last_price = self.trades.back().map(|t| t.price);

        self.cached_stats.avg_trade_size = if self.trade_count > 0 {
            Some(total_volume / Decimal::from(self.trade_count))
        } else {
            None
        };

        self.stats_dirty = false;
    }

    pub fn insert_trade(&mut self, trade: Trade) {
        // Handle trade eviction if buffer is full
        if self.trades.len() == self.max_len {
            let removed = self.trades.pop_front().unwrap();
            
            // Adjust volumes and momentum for removed trade
            if removed.is_buyer_maker {
                self.sell_volume -= removed.quantity;
                // When removing a sell trade, we need to increment momentum
                // because we're removing a -1 that was previously added
                self.cached_stats.signed_count_momentum += 1;
            } else {
                self.buy_volume -= removed.quantity;
                // When removing a buy trade, we need to decrement momentum
                // because we're removing a +1 that was previously added
                self.cached_stats.signed_count_momentum -= 1;
            }
        } else {
            self.trade_count += 1;
        }

        // Add new trade
        if trade.is_buyer_maker {
            self.sell_volume += trade.quantity;
            // Sell trades (maker) decrease momentum
            self.cached_stats.signed_count_momentum -= 1;
        } else {
            self.buy_volume += trade.quantity;
            // Buy trades (taker) increase momentum
            self.cached_stats.signed_count_momentum += 1;
        }

        self.stats_dirty = true;
        self.trades.push_back(trade);
    }

    pub fn last_n_trades(&self, n: usize) -> Vec<Trade> {
        self.trades.iter().rev().take(n).cloned().collect()
    }

    pub fn last_n_trades_ref(&self, n: usize) -> impl Iterator<Item = &Trade> + '_ {
        self.trades.iter().rev().take(n)
    }

    pub fn vwap(&self, window: usize) -> Result<Decimal, TradesLogError> {
        if window == 0 {
            return Err(TradesLogError::InvalidWindowSize);
        }
        
        if self.trades.len() < window {
            return Err(TradesLogError::InsufficientTrades);
        }
    
        let (sum_pq, sum_q) = self.trades
            .iter()
            .rev()
            .take(window)
            .fold((Decimal::ZERO, Decimal::ZERO), |(acc_pq, acc_q), trade| {
                (acc_pq + trade.price * trade.quantity, acc_q + trade.quantity)
            });
    
        if sum_q.is_zero() {
            Err(TradesLogError::ZeroVolume)
        } else {
            Ok(sum_pq / sum_q)
        }
    }

    pub fn trade_rate(&self, window_ms: u64) -> Result<f64, TradesLogError> {
        if self.trades.len() < 2 {
            return Err(TradesLogError::InsufficientTrades);
        }

        let now = self.trades.back().unwrap().timestamp;
        let start_time = now.saturating_sub(window_ms);

        let count = match self.trades.binary_search_by(|t| t.timestamp.cmp(&start_time)) {
            Ok(pos) | Err(pos) => self.trades.len() - pos,
        };

        Ok(count as f64 / (window_ms as f64 / 1000.0))
    }

    pub fn aggressor_volume_ratio(&self, n: usize) -> Result<Decimal, TradesLogError> {
        if n == 0 {
            return Err(TradesLogError::InvalidWindowSize);
        }
        if self.trades.is_empty() {  
            return Err(TradesLogError::InsufficientTrades);
        }

        let (buyer_volume, seller_volume) = self.last_n_trades_ref(n)
            .fold((dec!(0), dec!(0)), |(buy, sell), t| {
                if t.is_buyer_maker {
                    (buy, sell + t.quantity)
                } else {
                    (buy + t.quantity, sell)
                }
            });

        let total = buyer_volume + seller_volume;
        if total == dec!(0) {
            Err(TradesLogError::ZeroVolume)
        } else {
            Ok(buyer_volume / total)
        }
    }

    pub fn trade_imbalance(&mut self) -> Option<Decimal> {
        self.update_cached_stats();
        self.cached_stats.trade_imbalance
    }

    pub fn vwap_total(&mut self) -> Option<Decimal> {
        self.update_cached_stats();
        self.cached_stats.vwap_total
    }

    pub fn price_change(&mut self) -> Option<Decimal> {
        self.update_cached_stats();
        self.cached_stats.price_change
    }

    pub fn last_price(&self) -> Option<Decimal> {
        self.trades.back().map(|t| t.price)
    }

    pub fn avg_trade_size(&mut self) -> Option<Decimal> {
        self.update_cached_stats();
        self.cached_stats.avg_trade_size
    }

    pub fn signed_count_momentum(&self) -> i64 {
        self.cached_stats.signed_count_momentum
    }

    pub fn get_snapshot(&mut self) -> TradeLogSnapshot {
        self.update_cached_stats();
        
        TradeLogSnapshot {
            last_price: self.last_price(),
            trade_imbalance: self.trade_imbalance(),
            vwap_total: self.vwap_total(),
            price_change: self.price_change(),
            avg_trade_size: self.avg_trade_size(),
            signed_count_momentum: self.signed_count_momentum(),
            trade_rate_10s: self.trade_rate(10_000).ok(),
            vwap_10: self.vwap(10).ok(),  
            vwap_50: self.vwap(50).ok(),
            vwap_100: self.vwap(100).ok(),
            vwap_1000: self.vwap(1000).ok(),
            aggr_ratio_10: self.aggressor_volume_ratio(10).ok(),
            aggr_ratio_50: self.aggressor_volume_ratio(50).ok(),
            aggr_ratio_100: self.aggressor_volume_ratio(100).ok(),
            aggr_ratio_1000: self.aggressor_volume_ratio(1000).ok(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentTradesLog {
    inner: Arc<RwLock<TradesLog>>,
}

impl ConcurrentTradesLog {
    pub fn new(max_len: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TradesLog::new(max_len))),
        }
    }

    pub async fn insert_trade(&self, trade: Trade) {
        let mut log = self.inner.write().await;
        log.insert_trade(trade);
    }

    pub async fn last_n_trades(&self, n: usize) -> Vec<Trade> {
        let log = self.inner.read().await;
        log.last_n_trades(n)
    }

    pub async fn vwap(&self, n: usize) -> Result<Decimal, TradesLogError> {
        let log = self.inner.read().await;  
        log.vwap(n)
    }

    pub async fn trade_rate(&self, window_ms: u64) -> Result<f64, TradesLogError> {
        let log = self.inner.read().await;
        log.trade_rate(window_ms)
    }

    pub async fn aggressor_volume_ratio(&self, n: usize) -> Result<Decimal, TradesLogError> {
        let log = self.inner.read().await;
        log.aggressor_volume_ratio(n)
    }

    pub async fn trade_imbalance(&self) -> Option<Decimal> {
        let mut log = self.inner.write().await;
        log.trade_imbalance()
    }

    pub async fn vwap_total(&self) -> Option<Decimal> {
        let mut log = self.inner.write().await;
        log.vwap_total()
    }

    pub async fn price_change(&self) -> Option<Decimal> {
        let mut log = self.inner.write().await;
        log.price_change()
    }

    pub async fn last_price(&self) -> Option<Decimal> {
        let log = self.inner.read().await;
        log.last_price()
    }

    pub async fn avg_trade_size(&self) -> Option<Decimal> {
        let mut log = self.inner.write().await;
        log.avg_trade_size()
    }

    pub async fn signed_count_momentum(&self) -> i64 {
        let log = self.inner.read().await;
        log.signed_count_momentum()
    }

    pub async fn get_snapshot(&self) -> TradeLogSnapshot {
        let mut log = self.inner.write().await;
        log.get_snapshot()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn create_test_trade(price: Decimal, quantity: Decimal, is_buyer_maker: bool) -> Trade {
        Trade {
            price,
            quantity,
            timestamp: 0,
            is_buyer_maker,
        }
    }

    #[test]
    fn test_new_trades_log() {
        let log = TradesLog::new(100);
        assert_eq!(log.trades.len(), 0);
        assert_eq!(log.max_len, 100);
        assert_eq!(log.buy_volume, dec!(0));
        assert_eq!(log.sell_volume, dec!(0));
    }

    #[test]
    fn test_insert_trade() {
        let mut log = TradesLog::new(2); // max_len = 2
        
        // 1. Add first buy trade
        log.insert_trade(create_test_trade(dec!(100), dec!(1), false));
        assert_eq!(log.signed_count_momentum(), 1, "First buy should set momentum to 1");
        
        // 2. Add sell trade
        log.insert_trade(create_test_trade(dec!(101), dec!(1), true));
        assert_eq!(log.signed_count_momentum(), 0, "Sell should decrement momentum to 0");
        
        // 3. Add another buy trade (will evict the first trade)
        log.insert_trade(create_test_trade(dec!(102), dec!(1), false));
        
        // Breakdown of expected momentum calculation:
        // - Evict first buy trade (was +1): momentum -= 1 → -1
        // - Add new buy trade: momentum += 1 → 0
        // - Current trades in buffer: sell (-1) and buy (+1) → net 0
        assert_eq!(log.signed_count_momentum(), 0, "After eviction and new buy, momentum should be 0");
        
        // Verify volumes
        assert_eq!(log.buy_volume, dec!(1), "Buy volume should be 1");
        assert_eq!(log.sell_volume, dec!(1), "Sell volume should be 1");
    }

    #[test]
    fn test_vwap_calculation() {
        let mut log = TradesLog::new(10);
        
        // Empty log
        assert!(matches!(
            log.vwap(1),
            Err(TradesLogError::InsufficientTrades)
        ));
        
        // Add trades
        log.insert_trade(create_test_trade(dec!(100), dec!(1), false));
        log.insert_trade(create_test_trade(dec!(101), dec!(2), true));
        log.insert_trade(create_test_trade(dec!(102), dec!(3), false));
        
        // Test VWAP with approximate comparison
        let vwap = log.vwap(3).unwrap();
        let expected = dec!(101.3333333333333333333333333);
        assert!((vwap - expected).abs() < dec!(0.0000001));
        
        // Test zero volume error
        let mut empty_log = TradesLog::new(10);
        empty_log.insert_trade(create_test_trade(dec!(100), dec!(0), false));
        assert!(matches!(
            empty_log.vwap(1),
            Err(TradesLogError::ZeroVolume)
        ));
    }

    #[test]
    fn test_trade_rate() {
        let mut log = TradesLog::new(10);
        
        // Empty log
        assert!(matches!(
            log.trade_rate(1000),
            Err(TradesLogError::InsufficientTrades)
        ));
        
        // Add trades with timestamps
        let now = 100_000; // ms
        log.insert_trade(Trade {
            price: dec!(100),
            quantity: dec!(1),
            timestamp: now - 5000,
            is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            price: dec!(101),
            quantity: dec!(2),
            timestamp: now - 3000,
            is_buyer_maker: true,
        });
        log.insert_trade(Trade {
            price: dec!(102),
            quantity: dec!(3),
            timestamp: now,
            is_buyer_maker: false,
        });
        
        // Test trade rate with approximate comparison
        let rate = log.trade_rate(5000).unwrap();
        assert!((rate - 0.6).abs() < 0.0001); // 3 trades / 5 seconds
    }

    #[test]
    fn test_aggressor_volume_ratio() {
        let mut log = TradesLog::new(10);
        
        // Empty log
        assert!(matches!(
            log.aggressor_volume_ratio(1),
            Err(TradesLogError::InsufficientTrades)
        ));
        
        // Add trades
        log.insert_trade(create_test_trade(dec!(100), dec!(1), false));
        log.insert_trade(create_test_trade(dec!(101), dec!(2), true));
        
        // Use approximate comparison for decimal values
        let ratio = log.aggressor_volume_ratio(2).unwrap();
        assert!((ratio - dec!(0.3333333333333333333333333)).abs() < dec!(0.0000001));
    }

    #[test]
    fn test_snapshot() {
        let mut log = TradesLog::new(10);
        
        // Add trades
        log.insert_trade(create_test_trade(dec!(100), dec!(1), false));
        log.insert_trade(create_test_trade(dec!(101), dec!(2), true));
        
        let snapshot = log.get_snapshot();
        
        assert_eq!(snapshot.last_price, Some(dec!(101)));
        assert!((snapshot.trade_imbalance.unwrap() - dec!(0.3333333333333333333333333)).abs() < dec!(0.0000001));
        assert_eq!(snapshot.vwap_10, log.vwap(10).ok());
    }

    #[test]
    fn test_zero_quantity_trades() {
        let mut log = TradesLog::new(10);
        log.insert_trade(create_test_trade(dec!(100), dec!(0), false));
        assert_eq!(log.buy_volume, dec!(0));
        assert!(matches!(
            log.vwap(1),
            Err(TradesLogError::ZeroVolume)
        ));
    }

    #[test]
    fn test_insert_trade_momentum() {
        let mut log = TradesLog::new(3);
        
        // First buy trade
        log.insert_trade(create_test_trade(dec!(100), dec!(1), false));
        assert_eq!(log.signed_count_momentum(), 1);
        
        // Sell trade
        log.insert_trade(create_test_trade(dec!(101), dec!(1), true));
        assert_eq!(log.signed_count_momentum(), 0);
        
        // Another buy trade
        log.insert_trade(create_test_trade(dec!(102), dec!(1), false));
        assert_eq!(log.signed_count_momentum(), 1);
        
        // Force eviction of first trade
        log.insert_trade(create_test_trade(dec!(103), dec!(1), false));
        assert_eq!(log.signed_count_momentum(), 1); // Evicted buy (-1), added buy (+1)
    }

    #[test] 
    fn test_aggressor_ratio_edge_cases() {
        let mut log = TradesLog::new(10);
        
        // Single trade
        log.insert_trade(create_test_trade(dec!(100), dec!(1), false));
        assert_eq!(log.aggressor_volume_ratio(1).unwrap(), dec!(1.0));
        
        // All buys
        log.insert_trade(create_test_trade(dec!(101), dec!(2), false));
        assert_eq!(log.aggressor_volume_ratio(2).unwrap(), dec!(1.0));
        
        // All sells
        let mut sell_log = TradesLog::new(10);
        sell_log.insert_trade(create_test_trade(dec!(100), dec!(1), true));
        assert_eq!(sell_log.aggressor_volume_ratio(1).unwrap(), dec!(0.0));
    }
}

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::{mpsc, watch};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use thiserror::Error;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct Trade {
    pub id: u64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp: u64,
    pub is_buyer_maker: bool,
}

/// Pending trade for realized spread calculation
#[derive(Debug, Clone)]
struct PendingRealizedSpread {
    trade_price: Decimal,
    mid_price_at_trade: Decimal,
    timestamp: u64,
    is_buyer_maker: bool,
}

#[derive(Debug, Clone)]
pub struct TradesLog {
    trades: VecDeque<Trade>,
    max_len: usize,
    trade_count: usize,
    next_id: u64,
    buy_volume: Decimal,
    sell_volume: Decimal,
    stats_dirty: bool,
    cached_stats: CachedStats,
    // Phase 2: Effective spread tracking (rolling buffer)
    effective_spreads: VecDeque<Decimal>,
    effective_spread_window: usize,
    // Phase 2: Realized spread tracking
    pending_realized_spreads: VecDeque<PendingRealizedSpread>,
    realized_spreads: VecDeque<Decimal>,
    realized_spread_window: usize,
    realized_spread_delta_ms: u64,
    // Phase 2: Inter-trade duration tracking
    inter_trade_durations: VecDeque<f64>,
    inter_trade_duration_window: usize,
    last_trade_timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradesLogFeatures {
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
    // Phase 2 features
    pub effective_spread: Option<Decimal>,
    pub realized_spread: Option<Decimal>,
    pub inter_trade_duration_mean_ms: Option<f64>,
    pub inter_trade_duration_std_ms: Option<f64>,
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

const EFFECTIVE_SPREAD_WINDOW: usize = 100;
const REALIZED_SPREAD_WINDOW: usize = 100;
const REALIZED_SPREAD_DELTA_MS: u64 = 1000; // 1 second default
const INTER_TRADE_DURATION_WINDOW: usize = 100;

impl TradesLog {
    pub fn new(max_len: usize) -> Self {
        Self {
            trades: VecDeque::with_capacity(max_len),
            max_len,
            trade_count: 0,
            next_id: 0,
            buy_volume: dec!(0),
            sell_volume: dec!(0),
            stats_dirty: true,
            cached_stats: CachedStats::default(),
            // Phase 2 features
            effective_spreads: VecDeque::with_capacity(EFFECTIVE_SPREAD_WINDOW),
            effective_spread_window: EFFECTIVE_SPREAD_WINDOW,
            pending_realized_spreads: VecDeque::with_capacity(REALIZED_SPREAD_WINDOW),
            realized_spreads: VecDeque::with_capacity(REALIZED_SPREAD_WINDOW),
            realized_spread_window: REALIZED_SPREAD_WINDOW,
            realized_spread_delta_ms: REALIZED_SPREAD_DELTA_MS,
            inter_trade_durations: VecDeque::with_capacity(INTER_TRADE_DURATION_WINDOW),
            inter_trade_duration_window: INTER_TRADE_DURATION_WINDOW,
            last_trade_timestamp: None,
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

    pub fn insert_trade(&mut self, mut trade: Trade) {
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

        // Phase 2: Track inter-trade duration
        if let Some(last_ts) = self.last_trade_timestamp {
            if trade.timestamp > last_ts {
                let duration_ms = (trade.timestamp - last_ts) as f64;
                self.inter_trade_durations.push_back(duration_ms);
                if self.inter_trade_durations.len() > self.inter_trade_duration_window {
                    self.inter_trade_durations.pop_front();
                }
            }
        }
        self.last_trade_timestamp = Some(trade.timestamp);

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
        trade.id = self.next_id;
        self.next_id += 1;
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
        if window_ms == 0 {
            return Err(TradesLogError::InvalidWindowSize);
        }
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

    /// Records an effective spread measurement given the current mid price.
    /// Effective spread = 2 * |trade_price - mid_price|
    /// Should be called when a new trade arrives and mid_price is known.
    pub fn record_effective_spread(&mut self, mid_price: Decimal) {
        if let Some(trade) = self.trades.back() {
            let spread = dec!(2) * (trade.price - mid_price).abs();
            self.effective_spreads.push_back(spread);
            if self.effective_spreads.len() > self.effective_spread_window {
                self.effective_spreads.pop_front();
            }
        }
    }

    /// Returns the rolling average effective spread over the last N trades.
    pub fn effective_spread(&self) -> Option<Decimal> {
        if self.effective_spreads.is_empty() {
            return None;
        }
        let sum: Decimal = self.effective_spreads.iter().copied().sum();
        let count = Decimal::from(self.effective_spreads.len() as u64);
        Some(sum / count)
    }

    /// Records a pending realized spread measurement.
    /// Should be called when a trade occurs with known mid_price.
    pub fn record_pending_realized_spread(&mut self, mid_price: Decimal) {
        if let Some(trade) = self.trades.back() {
            self.pending_realized_spreads.push_back(PendingRealizedSpread {
                trade_price: trade.price,
                mid_price_at_trade: mid_price,
                timestamp: trade.timestamp,
                is_buyer_maker: trade.is_buyer_maker,
            });
            // Limit pending buffer size
            if self.pending_realized_spreads.len() > self.realized_spread_window * 2 {
                self.pending_realized_spreads.pop_front();
            }
        }
    }

    /// Updates realized spread calculations with current mid price and timestamp.
    /// Processes trades that are at least delta_ms old.
    pub fn update_realized_spread(&mut self, current_mid: Decimal, current_timestamp: u64) {
        // Process pending trades that have matured (age >= delta_ms)
        while let Some(pending) = self.pending_realized_spreads.front() {
            if current_timestamp >= pending.timestamp + self.realized_spread_delta_ms {
                let pending = self.pending_realized_spreads.pop_front().unwrap();

                // Realized spread = 2 * sign * (trade_price - future_mid)
                // For buyer (taker buy, maker sell): sign = +1 (price moved favorably for maker if it went down)
                // For seller (taker sell, maker buy): sign = -1
                let sign = if pending.is_buyer_maker { dec!(-1) } else { dec!(1) };
                let realized = dec!(2) * sign * (pending.trade_price - current_mid);

                self.realized_spreads.push_back(realized);
                if self.realized_spreads.len() > self.realized_spread_window {
                    self.realized_spreads.pop_front();
                }
            } else {
                break; // Remaining trades haven't matured yet
            }
        }
    }

    /// Returns the rolling average realized spread.
    pub fn realized_spread(&self) -> Option<Decimal> {
        if self.realized_spreads.is_empty() {
            return None;
        }
        let sum: Decimal = self.realized_spreads.iter().copied().sum();
        let count = Decimal::from(self.realized_spreads.len() as u64);
        Some(sum / count)
    }

    /// Returns the rolling mean of inter-trade durations in milliseconds.
    pub fn inter_trade_duration_mean(&self) -> Option<f64> {
        if self.inter_trade_durations.is_empty() {
            return None;
        }
        let sum: f64 = self.inter_trade_durations.iter().sum();
        Some(sum / self.inter_trade_durations.len() as f64)
    }

    /// Returns the rolling standard deviation of inter-trade durations in milliseconds.
    pub fn inter_trade_duration_std(&self) -> Option<f64> {
        if self.inter_trade_durations.len() < 2 {
            return None;
        }
        let mean = self.inter_trade_duration_mean()?;
        let variance: f64 = self.inter_trade_durations
            .iter()
            .map(|d| (d - mean).powi(2))
            .sum::<f64>()
            / (self.inter_trade_durations.len() - 1) as f64;
        Some(variance.sqrt())
    }

    pub fn get_snapshot(&mut self) -> TradesLogFeatures {
        self.update_cached_stats();

        TradesLogFeatures {
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
            // Phase 2 features
            effective_spread: self.effective_spread(),
            realized_spread: self.realized_spread(),
            inter_trade_duration_mean_ms: self.inter_trade_duration_mean(),
            inter_trade_duration_std_ms: self.inter_trade_duration_std(),
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

    pub async fn get_snapshot(&self) -> TradesLogFeatures {
        let mut log = self.inner.write().await;
        log.get_snapshot()
    }

    pub async fn trades_since(&self, last_id: u64) -> Vec<Trade> {
        let log = self.inner.read().await;
        log.trades
            .iter()
            .filter(|t| t.id > last_id)
            .cloned()
            .collect()
    }

    /// Records an effective spread given the current mid price.
    pub async fn record_effective_spread(&self, mid_price: Decimal) {
        let mut log = self.inner.write().await;
        log.record_effective_spread(mid_price);
    }

    /// Returns the rolling average effective spread.
    pub async fn effective_spread(&self) -> Option<Decimal> {
        let log = self.inner.read().await;
        log.effective_spread()
    }

    /// Records a pending realized spread for the last trade.
    pub async fn record_pending_realized_spread(&self, mid_price: Decimal) {
        let mut log = self.inner.write().await;
        log.record_pending_realized_spread(mid_price);
    }

    /// Updates realized spread calculations with current state.
    pub async fn update_realized_spread(&self, current_mid: Decimal, current_timestamp: u64) {
        let mut log = self.inner.write().await;
        log.update_realized_spread(current_mid, current_timestamp);
    }

    /// Returns the rolling average realized spread.
    pub async fn realized_spread(&self) -> Option<Decimal> {
        let log = self.inner.read().await;
        log.realized_spread()
    }

    /// Returns the rolling mean inter-trade duration in milliseconds.
    pub async fn inter_trade_duration_mean(&self) -> Option<f64> {
        let log = self.inner.read().await;
        log.inter_trade_duration_mean()
    }

    /// Returns the rolling std deviation of inter-trade duration in milliseconds.
    pub async fn inter_trade_duration_std(&self) -> Option<f64> {
        let log = self.inner.read().await;
        log.inter_trade_duration_std()
    }
}

#[derive(Debug, Clone)]
pub struct TradesLogEngineConfig {
    pub snapshot_interval_ms: u64,
}

impl Default for TradesLogEngineConfig {
    fn default() -> Self {
        Self {
            snapshot_interval_ms: 100,  
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradesLogEngine {
    trades_log: Arc<ConcurrentTradesLog>,
    snapshot_interval_ms: u64,
    tx: mpsc::Sender<TradesLogFeatures>,
}

impl TradesLogEngine {
    pub fn new(
        trades_log: Arc<ConcurrentTradesLog>,
        config: Option<TradesLogEngineConfig>,
        tx: mpsc::Sender<TradesLogFeatures>,
    ) -> Self {
        let cfg = config.unwrap_or_default();
        Self {
            trades_log,
            tx,
            snapshot_interval_ms: cfg.snapshot_interval_ms,
        }
    }

    pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(self.snapshot_interval_ms));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let snapshot = self.trades_log.get_snapshot().await;
                    if self.tx.send(snapshot).await.is_err() {
                        log::warn!("TradesLogEngine: receiver dropped, shutting down engine.");
                        break;
                    }
                }
                _ = shutdown_rx.changed() => {
                    log::info!("TradesLogEngine: shutdown signal received.");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn create_test_trade(price: Decimal, quantity: Decimal, is_buyer_maker: bool) -> Trade {
        Trade {
            id: 0, // Auto-assigned
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
            id: 0,
            price: dec!(100),
            quantity: dec!(1),
            timestamp: now - 5000,
            is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            id: 0,
            price: dec!(101),
            quantity: dec!(2),
            timestamp: now - 3000,
            is_buyer_maker: true,
        });
        log.insert_trade(Trade {
            id: 0,
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

    #[test]
    fn test_trade_rate_zero_window_error() {
        let mut log = TradesLog::new(10);

        // Add enough trades
        log.insert_trade(Trade {
            id: 0,
            price: dec!(100),
            quantity: dec!(1),
            timestamp: 1000,
            is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            id: 0,
            price: dec!(101),
            quantity: dec!(1),
            timestamp: 2000,
            is_buyer_maker: false,
        });

        // Zero window should return error
        assert!(matches!(
            log.trade_rate(0),
            Err(TradesLogError::InvalidWindowSize)
        ));
    }

    #[test]
    fn test_trade_rate_small_window_valid() {
        let mut log = TradesLog::new(10);

        // Add trades with timestamps
        log.insert_trade(Trade {
            id: 0,
            price: dec!(100),
            quantity: dec!(1),
            timestamp: 999,
            is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            id: 0,
            price: dec!(101),
            quantity: dec!(1),
            timestamp: 1000,
            is_buyer_maker: false,
        });

        // 1ms window should work
        let rate = log.trade_rate(1).unwrap();
        assert!(rate >= 0.0, "Trade rate should be non-negative");
    }

    #[test]
    fn test_trade_rate_normal_unchanged() {
        let mut log = TradesLog::new(10);

        let now = 10_000;
        log.insert_trade(Trade {
            id: 0,
            price: dec!(100),
            quantity: dec!(1),
            timestamp: now - 2000,
            is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            id: 0,
            price: dec!(101),
            quantity: dec!(1),
            timestamp: now - 1000,
            is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            id: 0,
            price: dec!(102),
            quantity: dec!(1),
            timestamp: now,
            is_buyer_maker: false,
        });

        // Normal window should compute correctly
        let rate = log.trade_rate(2000).unwrap();
        // 3 trades in 2000ms window = 1.5 trades/second
        assert!((rate - 1.5).abs() < 0.01, "Expected ~1.5 trades/sec, got {}", rate);
    }

    // Phase 2: Effective Spread Tests

    #[test]
    fn test_effective_spread_buy_above_mid() {
        let mut log = TradesLog::new(10);
        log.insert_trade(create_test_trade(dec!(101), dec!(1), false)); // Buy at 101

        // Mid price is 100, trade at 101 → effective spread = 2 * |101 - 100| = 2
        log.record_effective_spread(dec!(100));

        let spread = log.effective_spread().unwrap();
        assert_eq!(spread, dec!(2), "Effective spread should be 2 for buy above mid");
    }

    #[test]
    fn test_effective_spread_sell_below_mid() {
        let mut log = TradesLog::new(10);
        log.insert_trade(create_test_trade(dec!(99), dec!(1), true)); // Sell at 99

        // Mid price is 100, trade at 99 → effective spread = 2 * |99 - 100| = 2
        log.record_effective_spread(dec!(100));

        let spread = log.effective_spread().unwrap();
        assert_eq!(spread, dec!(2), "Effective spread should be 2 for sell below mid");
    }

    #[test]
    fn test_effective_spread_at_mid() {
        let mut log = TradesLog::new(10);
        log.insert_trade(create_test_trade(dec!(100), dec!(1), false)); // Trade at mid

        // Mid price is 100, trade at 100 → effective spread = 2 * |100 - 100| = 0
        log.record_effective_spread(dec!(100));

        let spread = log.effective_spread().unwrap();
        assert_eq!(spread, dec!(0), "Effective spread should be 0 for trade at mid");
    }

    #[test]
    fn test_effective_spread_rolling_avg() {
        let mut log = TradesLog::new(10);

        // Trade 1: 101 vs mid 100 → spread = 2
        log.insert_trade(create_test_trade(dec!(101), dec!(1), false));
        log.record_effective_spread(dec!(100));

        // Trade 2: 102 vs mid 100 → spread = 4
        log.insert_trade(create_test_trade(dec!(102), dec!(1), false));
        log.record_effective_spread(dec!(100));

        // Average should be (2 + 4) / 2 = 3
        let spread = log.effective_spread().unwrap();
        assert_eq!(spread, dec!(3), "Rolling average should be 3");
    }

    // Phase 2: Inter-Trade Duration Tests

    #[test]
    fn test_inter_trade_duration_computed() {
        let mut log = TradesLog::new(10);

        log.insert_trade(Trade {
            id: 0,
            price: dec!(100),
            quantity: dec!(1),
            timestamp: 1000,
            is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            id: 0,
            price: dec!(101),
            quantity: dec!(1),
            timestamp: 1500, // 500ms later
            is_buyer_maker: false,
        });

        let mean = log.inter_trade_duration_mean().unwrap();
        assert!((mean - 500.0).abs() < 0.01, "Duration should be 500ms, got {}", mean);
    }

    #[test]
    fn test_inter_trade_duration_first_trade() {
        let mut log = TradesLog::new(10);

        log.insert_trade(Trade {
            id: 0,
            price: dec!(100),
            quantity: dec!(1),
            timestamp: 1000,
            is_buyer_maker: false,
        });

        // First trade has no duration (nothing to compare to)
        assert!(log.inter_trade_duration_mean().is_none(), "First trade should have no duration");
    }

    #[test]
    fn test_inter_trade_duration_rolling_stats() {
        let mut log = TradesLog::new(10);

        // Insert trades with different intervals
        log.insert_trade(Trade {
            id: 0, price: dec!(100), quantity: dec!(1), timestamp: 1000, is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            id: 0, price: dec!(100), quantity: dec!(1), timestamp: 1100, is_buyer_maker: false, // 100ms
        });
        log.insert_trade(Trade {
            id: 0, price: dec!(100), quantity: dec!(1), timestamp: 1300, is_buyer_maker: false, // 200ms
        });
        log.insert_trade(Trade {
            id: 0, price: dec!(100), quantity: dec!(1), timestamp: 1600, is_buyer_maker: false, // 300ms
        });

        // Durations: [100, 200, 300], mean = 200
        let mean = log.inter_trade_duration_mean().unwrap();
        assert!((mean - 200.0).abs() < 0.01, "Mean should be 200ms, got {}", mean);

        // Std should be sqrt(((100-200)^2 + (200-200)^2 + (300-200)^2) / 2) = sqrt(10000) = 100
        let std = log.inter_trade_duration_std().unwrap();
        assert!((std - 100.0).abs() < 0.01, "Std should be 100ms, got {}", std);
    }

    #[test]
    fn test_snapshot_includes_phase2_features() {
        let mut log = TradesLog::new(10);

        // Add trades with timestamps
        log.insert_trade(Trade {
            id: 0, price: dec!(100), quantity: dec!(1), timestamp: 1000, is_buyer_maker: false,
        });
        log.insert_trade(Trade {
            id: 0, price: dec!(101), quantity: dec!(1), timestamp: 1200, is_buyer_maker: false,
        });

        // Record effective spread
        log.record_effective_spread(dec!(100));

        let snapshot = log.get_snapshot();

        // Check that phase 2 features are present
        assert!(snapshot.effective_spread.is_some(), "Effective spread should be in snapshot");
        assert!(snapshot.inter_trade_duration_mean_ms.is_some(), "Duration mean should be in snapshot");
    }

    // Phase 2: Realized Spread Tests

    #[test]
    fn test_realized_spread_favorable_fill() {
        let mut log = TradesLog::new(10);

        // Taker buy at 101, mid is 100
        log.insert_trade(Trade {
            id: 0, price: dec!(101), quantity: dec!(1), timestamp: 1000, is_buyer_maker: false,
        });
        log.record_pending_realized_spread(dec!(100));

        // 1 second later, mid price dropped to 99 (favorable for maker who sold at 101)
        // Realized spread = 2 * (+1) * (101 - 99) = 4
        log.update_realized_spread(dec!(99), 2001);

        let spread = log.realized_spread().unwrap();
        assert_eq!(spread, dec!(4), "Realized spread should be 4 for favorable fill");
    }

    #[test]
    fn test_realized_spread_adverse_fill() {
        let mut log = TradesLog::new(10);

        // Taker buy at 101, mid is 100
        log.insert_trade(Trade {
            id: 0, price: dec!(101), quantity: dec!(1), timestamp: 1000, is_buyer_maker: false,
        });
        log.record_pending_realized_spread(dec!(100));

        // 1 second later, mid price rose to 103 (adverse for maker who sold at 101)
        // Realized spread = 2 * (+1) * (101 - 103) = -4
        log.update_realized_spread(dec!(103), 2001);

        let spread = log.realized_spread().unwrap();
        assert_eq!(spread, dec!(-4), "Realized spread should be -4 for adverse fill");
    }

    #[test]
    fn test_realized_spread_no_future_mid() {
        let mut log = TradesLog::new(10);

        // Add trade but don't wait long enough for it to mature
        log.insert_trade(Trade {
            id: 0, price: dec!(101), quantity: dec!(1), timestamp: 1000, is_buyer_maker: false,
        });
        log.record_pending_realized_spread(dec!(100));

        // Only 500ms later (not yet matured at 1000ms delta)
        log.update_realized_spread(dec!(99), 1500);

        // Should return None since no trades have matured
        assert!(log.realized_spread().is_none(), "Should be None when no trades matured");
    }

    #[test]
    fn test_realized_spread_sell_side() {
        let mut log = TradesLog::new(10);

        // Taker sell (is_buyer_maker=true) at 99, mid is 100
        log.insert_trade(Trade {
            id: 0, price: dec!(99), quantity: dec!(1), timestamp: 1000, is_buyer_maker: true,
        });
        log.record_pending_realized_spread(dec!(100));

        // 1 second later, mid price rose to 101 (favorable for maker who bought at 99)
        // Realized spread = 2 * (-1) * (99 - 101) = 2 * (-1) * (-2) = 4
        log.update_realized_spread(dec!(101), 2001);

        let spread = log.realized_spread().unwrap();
        assert_eq!(spread, dec!(4), "Realized spread should be 4 for favorable sell fill");
    }
}

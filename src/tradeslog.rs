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
    pub vwap_50: Option<Decimal>,
    pub aggr_ratio_50: Option<Decimal>,
    pub trade_imbalance: Option<Decimal>,
    pub vwap_total: Option<Decimal>,
    pub price_change: Option<Decimal>,
    pub avg_trade_size: Option<Decimal>,
    pub signed_count_momentum: i64,
    pub trade_rate_10s: Option<f64>,
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

#[derive(Error, Debug)]
pub enum TradesLogError {
    #[error("Insufficient trades for calculation")]
    InsufficientTrades,
    #[error("Zero volume encountered in calculation")]
    ZeroVolume,
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
        if self.trades.len() == self.max_len {
            let removed = self.trades.pop_front().unwrap();
            if removed.is_buyer_maker {
                self.sell_volume -= removed.quantity;
                self.cached_stats.signed_count_momentum += 1;
            } else {
                self.buy_volume -= removed.quantity;
                self.cached_stats.signed_count_momentum -= 1;
            }
        } else {
            self.trade_count += 1;
        }

        if trade.is_buyer_maker {
            self.sell_volume += trade.quantity;
            self.cached_stats.signed_count_momentum -= 1;
        } else {
            self.buy_volume += trade.quantity;
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

    pub fn vwap(&self, n: usize) -> Result<Decimal, TradesLogError> {
        if n == 0 || self.trades.is_empty() {
            return Err(TradesLogError::InsufficientTrades);
        }

        let (weighted_sum, total_volume) = self.last_n_trades_ref(n)
            .map(|t| (t.price * t.quantity, t.quantity))
            .fold((dec!(0), dec!(0)), |(ws, tv), (wp, q)| (ws + wp, tv + q));

        if total_volume == dec!(0) {
            Err(TradesLogError::ZeroVolume)
        } else {
            Ok(weighted_sum / total_volume)
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
            vwap_50: self.vwap(50).ok(),
            aggr_ratio_50: self.aggressor_volume_ratio(50).ok(),
            trade_imbalance: self.trade_imbalance(),
            vwap_total: self.vwap_total(),
            price_change: self.price_change(),
            avg_trade_size: self.avg_trade_size(),
            signed_count_momentum: self.signed_count_momentum(),
            trade_rate_10s: self.trade_rate(10_000).ok(),
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

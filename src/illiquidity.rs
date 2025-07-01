use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, watch};
use rust_decimal::Decimal;
use rust_decimal::MathematicalOps;
use rust_decimal_macros::dec;
use serde::{Serialize, Deserialize};
use chrono::Utc;
use metrics::{Counter, Histogram, Gauge};
use crate::orderbook::{ConcurrentOrderBook, OrderBookSnapshot};
use crate::tradeslog::{ConcurrentTradesLog, TradeLogSnapshot, Trade};
use log;

#[derive(Debug, Clone, Deserialize)]
pub struct IlliquidityConfig {
    pub window_size:            usize,
    pub snapshot_interval_ms:   u64,
    pub min_price_diff:         Decimal,
    pub min_observations:       usize,
    pub amihud_window:          usize,
    pub kyle_min_observations:  usize,
    pub kyle_volume_scale:      Decimal,
    pub kyle_price_scale:       Decimal,
    pub vpin_bucket_size:       Decimal,     
    pub vpin_window_buckets:    usize,     
}

impl Default for IlliquidityConfig {
    fn default() -> Self {
        Self {
            window_size:            1000,
            snapshot_interval_ms:   100,
            min_price_diff:         dec!(0.00000001),
            min_observations:       30,
            amihud_window:          20,
            kyle_min_observations:  50,
            kyle_volume_scale:      dec!(1),
            kyle_price_scale:       dec!(1),
            vpin_bucket_size:       dec!(10),  
            vpin_window_buckets:    50,     
        }
    }
}

#[derive(Debug, Clone)]
struct HistoryBuffer {
    pub prices:     Vec<Decimal>,
    pub volumes:    Vec<Decimal>,
    pub directions: Vec<i8>,
}

#[derive(Debug, Serialize)]
pub struct IlliquidityMetrics {
    pub timestamp:          String,
    pub roll_spread:        Option<Decimal>,
    pub amihuds_lambda:     Option<Decimal>,
    pub kyles_lambda:       Option<Decimal>,
    pub hasbroucks_lambda:  Option<Decimal>,
    pub vpin:               Option<Decimal>,          
}

#[derive(Clone)]
pub struct IlliquidityEngineMetrics {
    pub computations:       Counter,
    pub computation_time:   Histogram,
    pub window_size:        Gauge,
}

struct VPIN {                           
    bucket_size:            Decimal,
    buy_volumes:            Vec<Decimal>,
    sell_volumes:           Vec<Decimal>,
    window_size:            usize,
    current_buy:            Decimal,
    current_sell:           Decimal,
    last_price:             Option<Decimal>,
}

impl VPIN {
    fn new(bucket_size: Decimal, window_size: usize) -> Self {
        Self {
            bucket_size,
            buy_volumes:    Vec::with_capacity(window_size),
            sell_volumes:   Vec::with_capacity(window_size),
            window_size,
            current_buy:    Decimal::ZERO,
            current_sell:   Decimal::ZERO,
            last_price:     None,
        }
    }

    fn update(&mut self, trades: &[Trade]) {
        for trade in trades {
            let direction = self.classify_trade(trade);
            match direction {
                1 => self.current_buy   += trade.quantity,  
                -1 => self.current_sell += trade.quantity, 
                _ => {}
            }
            if (self.current_buy + self.current_sell) >= self.bucket_size {
                self.buy_volumes.push(self.current_buy);
                self.sell_volumes.push(self.current_sell);
                self.current_buy  = Decimal::ZERO;
                self.current_sell = Decimal::ZERO;
                if self.buy_volumes.len() > self.window_size {
                    self.buy_volumes.remove(0);
                    self.sell_volumes.remove(0);
                }
            }
            self.last_price = Some(trade.price);
        }
    }
    
    fn classify_trade(&self, trade: &Trade) -> i8 {
        match (self.last_price, trade.is_buyer_maker) {  // Changed from aggressor to is_buyer_maker
            (Some(prev), _) if trade.price > prev => 1,
            (Some(prev), _) if trade.price < prev => -1,
            (_, false) => 1,   
            (_, true) => -1,  
        }
    }

    fn compute(&self) -> Decimal {
        if self.buy_volumes.is_empty() {
            return Decimal::ZERO;
        }

        let imbalance: Decimal = self.buy_volumes.iter().zip(&self.sell_volumes)
            .map(|(b, s)| (*b - *s).abs())
            .sum();

        imbalance / (Decimal::from(self.buy_volumes.len()) * self.bucket_size)
    }
}

pub struct IlliquidityEngine {
    order_book:     Arc<ConcurrentOrderBook>,
    trades_log:     Arc<ConcurrentTradesLog>,
    history:        Arc<Mutex<HistoryBuffer>>,
    metrics:        IlliquidityEngineMetrics,
    config:         IlliquidityConfig,
    vpin:           VPIN,                      
}


impl IlliquidityEngine {
    pub fn new(
        order_book: Arc<ConcurrentOrderBook>,
        trades_log: Arc<ConcurrentTradesLog>,
        config:     Option<IlliquidityConfig>,
    ) -> Self {
        let config       = config.unwrap_or_default();
        let config_clone = config.clone(); 
        
        Self {
            order_book,
            trades_log,
            history: Arc::new(Mutex::new(HistoryBuffer {
                prices:     Vec::with_capacity(config.window_size),
                volumes:    Vec::with_capacity(config.window_size),
                directions: Vec::with_capacity(config.window_size),
            })),
            metrics: IlliquidityEngineMetrics {
                computations:       metrics::register_counter!("illiquidity_computations"),
                computation_time:   metrics::register_histogram!("illiquidity_computation_time"),
                window_size:        metrics::register_gauge!("illiquidity_window_size"),
            },
            config, 
            vpin: VPIN::new(config_clone.vpin_bucket_size, config_clone.vpin_window_buckets), 
        }
    }

    pub async fn compute_metrics(&mut self) -> anyhow::Result<IlliquidityMetrics> {
        let (ob_snap, trade_snap) = tokio::join!(
            self.order_book.get_snapshot(),
            self.trades_log.get_snapshot()
        );
        
        self.update_history(&trade_snap).await;
        let history = self.history.lock().await;
        
        // Update VPIN with latest trades - changed to last_n_trades
        let trades = self.trades_log.last_n_trades(self.config.window_size).await;
        self.vpin.update(&trades);
        
        Ok(IlliquidityMetrics {
            timestamp:          Utc::now().to_rfc3339(),
            roll_spread:        Self::compute_roll_spread(&history.prices),
            amihuds_lambda:     self.compute_amihud(&history.prices, &history.volumes),
            kyles_lambda:       self.compute_kyle(&history.prices, &history.volumes, &history.directions),
            hasbroucks_lambda:  self.compute_hasbrouck(&history.prices, &history.volumes, &history.directions),
            vpin:               Some(self.vpin.compute()),
        })
    }

    async fn update_history(&self, trade_snap: &TradeLogSnapshot) {
        if let (Some(p), Some(v)) = (trade_snap.last_price, trade_snap.avg_trade_size) {
            if !v.is_zero() {
                let direction = trade_snap.trade_imbalance.map(|ti| if ti.is_sign_positive() { 1 } else { -1 }).unwrap_or(0);
                let mut history = self.history.lock().await;
                if history.prices.len() >= self.config.window_size {
                    history.prices.remove(0);
                    history.volumes.remove(0);
                    history.directions.remove(0);
                }
                history.prices.push(p * self.config.kyle_price_scale);
                history.volumes.push(v * self.config.kyle_volume_scale);
                history.directions.push(direction);
            }
        }
    }

    fn compute_roll_spread(prices: &[Decimal]) -> Option<Decimal> {
        if prices.len() < 2 {
            return None;
        }
        
        let price_changes: Vec<Decimal> = prices
            .windows(2)
            .filter_map(|w| w[1].checked_sub(w[0]))
            .collect();
            
        if price_changes.len() < 2 {
            return None;
        }
        
        let covariance = price_changes
            .windows(2)
            .filter_map(|w| w[0].checked_mul(w[1]))
            .sum::<Decimal>()
            .checked_div(Decimal::from(price_changes.len() - 1))?;
            
        covariance.abs().checked_mul(dec!(2))?.sqrt()
    }

    fn compute_amihud(&self, prices: &[Decimal], volumes: &[Decimal]) -> Option<Decimal> {
        if prices.len() < self.config.amihud_window || volumes.len() < self.config.amihud_window {
            return None;
        }
        
        let (ret_sum, vol_sum) = prices
            .windows(2)
            .zip(volumes.iter().skip(1))
            .try_fold((Decimal::ZERO, Decimal::ZERO), |(ret, vol), (w, v)| {
                let price_diff = w[1].checked_sub(w[0])?;
                let abs_diff = price_diff.abs();
                let ret_inc = if w[0].is_zero() {
                    Decimal::ZERO
                } else {
                    abs_diff.checked_div(w[0])?
                };
                Some((ret + ret_inc, vol + v))
            })?;
            
        if vol_sum.is_zero() {
            None
        } else {
            ret_sum.checked_div(vol_sum)
        }
    }

    fn compute_kyle(
        &self,
        prices: &[Decimal],
        volumes: &[Decimal],
        directions: &[i8],
    ) -> Option<Decimal> {
        if prices.len() < self.config.kyle_min_observations || 
           volumes.len() < self.config.kyle_min_observations ||
           directions.len() < self.config.kyle_min_observations {
            return None;
        }

        let mut price_changes = Vec::with_capacity(prices.len() - 1);
        let mut signed_volumes = Vec::with_capacity(volumes.len() - 1);
        
        for i in 1..prices.len() {
            if let (Some(price_diff), Some(volume), Some(dir)) = (
                prices[i].checked_sub(prices[i-1]),
                volumes.get(i-1),
                directions.get(i-1),
            ) {
                let signed_vol = volume * Decimal::from(*dir);
                price_changes.push(price_diff);
                signed_volumes.push(signed_vol);
            }
        }

        if price_changes.len() < self.config.kyle_min_observations {
            return None;
        }

        let mean_price_change = price_changes.iter().sum::<Decimal>() / Decimal::from(price_changes.len());
        let mean_signed_volume = signed_volumes.iter().sum::<Decimal>() / Decimal::from(signed_volumes.len());
        
        let covariance = price_changes.iter()
            .zip(signed_volumes.iter())
            .fold(Decimal::ZERO, |acc, (p, v)| {
                acc + (*p - mean_price_change) * (*v - mean_signed_volume)
            }) / Decimal::from(price_changes.len() - 1);
            
        let volume_variance = signed_volumes.iter()
            .fold(Decimal::ZERO, |acc, v| {
                acc + (*v - mean_signed_volume).powi(2)
            }) / Decimal::from(signed_volumes.len() - 1);

        if volume_variance.is_zero() {
            None
        } else {
            Some(covariance / volume_variance)
        }
    }

    fn compute_hasbrouck(
        &self,
        prices: &[Decimal],
        volumes: &[Decimal],
        directions: &[i8],
    ) -> Option<Decimal> {
        if prices.len() < 100 || volumes.len() < 100 || directions.len() < 100 {
            return None;
        }

        let mut returns = Vec::with_capacity(prices.len() - 1);
        let mut signed_volumes = Vec::with_capacity(volumes.len() - 1);
        
        for i in 1..prices.len() {
            if let (Some(price_diff), Some(volume), Some(dir)) = (
                prices[i].checked_sub(prices[i-1]),
                volumes.get(i-1),
                directions.get(i-1),
            ) {
                returns.push(price_diff);
                signed_volumes.push(volume * Decimal::from(*dir));
            }
        }

        if returns.len() < 30 {
            return None;
        }

        let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let mean_volume = signed_volumes.iter().sum::<Decimal>() / Decimal::from(signed_volumes.len());
        
        let mut cov_ret_vol = Decimal::ZERO;
        let mut var_vol = Decimal::ZERO;
        
        for (ret, vol) in returns.iter().zip(&signed_volumes) {
            let ret_diff = *ret - mean_return;
            let vol_diff = *vol - mean_volume;
            cov_ret_vol += ret_diff * vol_diff;
            var_vol += vol_diff * vol_diff;
        }
        
        if var_vol.is_zero() {
            None
        } else {
            Some(cov_ret_vol / var_vol)
        }
    }

    pub async fn run(
        mut self,
        mut shutdown_rx: watch::Receiver<bool>,
        persistence_tx: mpsc::Sender<IlliquidityMetrics>,
    ) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            self.config.snapshot_interval_ms,
        ));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let metrics = self.compute_metrics().await?;
                    if let Err(e) = persistence_tx.send(metrics).await {
                        log::error!("Failed to send metrics: {}", e);
                        break;
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        log::info!("Shutting down illiquidity engine");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

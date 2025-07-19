#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use chrono::{Utc, DateTime, Duration};
use log::{info, warn};
use crate::tradeslog::{ConcurrentTradesLog, Trade};
use num::FromPrimitive;
use crate::orderbook::ConcurrentOrderBook;

#[derive(Debug, Clone)]
pub struct EntropyConfig {
    pub snapshot_interval_ms: u64,
}

impl Default for EntropyConfig {
    fn default() -> Self {
        Self {
            snapshot_interval_ms: 100, 
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EntropyMetrics {
    pub timestamp:               String,
    pub tick_entropy_1s:         Option<Decimal>,
    pub tick_entropy_5s:         Option<Decimal>,
    pub tick_entropy_10s:        Option<Decimal>,
    pub tick_entropy_15s:        Option<Decimal>,
    pub tick_entropy_30s:        Option<Decimal>,
    pub tick_entropy_1m:         Option<Decimal>,
    pub tick_entropy_15m:        Option<Decimal>,
    pub volume_tick_entropy_1s:  Option<Decimal>,
    pub volume_tick_entropy_5s:  Option<Decimal>,
    pub volume_tick_entropy_10s: Option<Decimal>,
    pub volume_tick_entropy_15s: Option<Decimal>,
    pub volume_tick_entropy_30s: Option<Decimal>,
    pub volume_tick_entropy_1m:  Option<Decimal>,
    pub volume_tick_entropy_15m: Option<Decimal>,
}

#[derive(Debug, Clone)]
struct TradeSample {
    timestamp:  DateTime<Utc>,
    direction:  i8,
    volume:     Decimal,
}

struct RollingEntropy {
    buffer:     VecDeque<TradeSample>,
    window:     Duration,
    last_price: Option<Decimal>,
}

impl RollingEntropy {
    fn new(seconds: i64) -> Self {
        Self {
            buffer:     VecDeque::new(),
            window:     Duration::seconds(seconds),
            last_price: None,
        }
    }

    fn update(&mut self, trades: &[Trade]) {
        for trade in trades {
            let direction = match self.last_price {
                Some(prev) if trade.price > prev => 1,
                Some(prev) if trade.price < prev => -1,
                Some(_) => {
                    if trade.is_buyer_maker { -1 } else { 1 }
                },
                None => {
                    if trade.is_buyer_maker { -1 } else { 1 }
                }
            };
            
            self.last_price = Some(trade.price);
            self.buffer.push_back(TradeSample {
                timestamp: chrono::DateTime::<Utc>::from_timestamp_millis(trade.timestamp as i64)
                    .unwrap_or_else(|| chrono::Utc::now()),
                direction,
                volume: trade.quantity,
            });
        }
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        while let Some(front) = self.buffer.front() {
            if now - front.timestamp > self.window {
                self.buffer.pop_front();
            } else {
                break;
            }
        }
    }

    fn compute(&self) -> (Option<Decimal>, Option<Decimal>) {
        if self.buffer.is_empty() {
            return (None, None);
        }

        let mut counts  = [0u32; 3]; 
        let mut volumes = [Decimal::ZERO; 3]; 

        for sample in &self.buffer {
            let idx = match sample.direction {
                -1  => 0,
                0   => 1,
                1   => 2,
                _   => continue,
            };
            counts[idx]  += 1;
            volumes[idx] += sample.volume;
        }

        let total_count:  u32     = counts.iter().sum();
        let total_volume: Decimal = volumes.iter().sum();

        let tick_entropy = if total_count == 0 {
            None
        } else {
            let sum: Decimal = counts.iter()
                .filter(|&&c| c > 0)
                .map(|&c| {
                    let p = Decimal::from(c) / Decimal::from(total_count);
                    -p * Decimal::from_f64(p.to_f64().unwrap().ln()).unwrap()
                })
                .sum();
            Some(sum)
        };

        let volume_entropy = if total_volume.is_zero() {
            None
        } else {
            let sum: Decimal = volumes.iter()
                .filter(|&&v| !v.is_zero())
                .map(|&v| {
                    let p = v / total_volume;
                    -p * Decimal::from_f64(p.to_f64().unwrap().ln()).unwrap()
                })
                .sum();
            Some(sum)
        };

        (tick_entropy, volume_entropy)
    }
}

pub struct EntropyEngine {
    order_book: Arc<ConcurrentOrderBook>,
    trades_log: Arc<ConcurrentTradesLog>,
    config:     EntropyConfig,
    e1s:        RollingEntropy,
    e5s:        RollingEntropy,
    e10s:       RollingEntropy,
    e15s:       RollingEntropy,
    e30s:       RollingEntropy,
    e60s:       RollingEntropy,
    e900s:      RollingEntropy,
    last_processed_id:  u64,
    feature_tx: mpsc::Sender<EntropyMetrics>,
}

impl EntropyEngine {
    pub fn new(order_book: Arc<ConcurrentOrderBook>, 
               trades_log: Arc<ConcurrentTradesLog>, 
               config:     Option<EntropyConfig>,
               feature_tx: mpsc::Sender<EntropyMetrics>
            ) -> Self {
        Self {
            order_book,
            trades_log,
            config: config.unwrap_or_default(),
            e1s:    RollingEntropy::new(1),
            e5s:    RollingEntropy::new(5),
            e10s:   RollingEntropy::new(10),
            e15s:   RollingEntropy::new(15),
            e30s:   RollingEntropy::new(30),
            e60s:   RollingEntropy::new(60),
            e900s:  RollingEntropy::new(900),
            last_processed_id: 0,
            feature_tx,
        }
    }

    pub async fn compute_metrics(&mut self) -> anyhow::Result<EntropyMetrics> {
        let now                     = Utc::now();
        let new_trades              = self.trades_log.trades_since(self.last_processed_id).await;
        if let Some(last_trade)     = new_trades.last() {
            self.last_processed_id  = last_trade.id;
        }

        let mut engines = [
            &mut self.e1s,  &mut self.e5s,  &mut self.e10s,
            &mut self.e15s, &mut self.e30s, &mut self.e60s, &mut self.e900s,
        ];

        for engine in engines.iter_mut() {
            engine.update(&new_trades);
            engine.prune(now);
        }

        let (t1, v1)     = self.e1s.compute();
        let (t5, v5)     = self.e5s.compute();
        let (t10, v10)   = self.e10s.compute();
        let (t15, v15)   = self.e15s.compute();
        let (t30, v30)   = self.e30s.compute();
        let (t60, v60)   = self.e60s.compute();
        let (t900, v900) = self.e900s.compute();

        Ok(EntropyMetrics {
            timestamp: now.to_rfc3339(),
            tick_entropy_1s:         t1,
            tick_entropy_5s:         t5,
            tick_entropy_10s:        t10,
            tick_entropy_15s:        t15,
            tick_entropy_30s:        t30,
            tick_entropy_1m:         t60,
            tick_entropy_15m:        t900,
            volume_tick_entropy_1s:  v1,
            volume_tick_entropy_5s:  v5,
            volume_tick_entropy_10s: v10,
            volume_tick_entropy_15s: v15,
            volume_tick_entropy_30s: v30,
            volume_tick_entropy_1m:  v60,
            volume_tick_entropy_15m: v900,
        })
    }

    pub async fn run(
        mut self,
        mut shutdown_rx: watch::Receiver<bool>,
        _persistence_tx: mpsc::Sender<EntropyMetrics>,
    ) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            self.config.snapshot_interval_ms,
        ));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.compute_metrics().await {
                        Ok(metrics) => {
                            if let Err(e) = self.feature_tx.send(metrics.clone()).await {
                                warn!("Failed to send entropy features: {}", e);
                            }
                        }
                        Err(e) => warn!("Error computing entropy features: {}", e),
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutting down entropy engine");
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

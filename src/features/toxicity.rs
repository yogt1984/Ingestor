use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, Duration};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::data::orderbook::ConcurrentOrderBook;
use crate::data::tradeslog::ConcurrentTradesLog;

/// Order flow toxicity metrics
/// Measures the probability and cost of trading against informed traders
///
/// References:
/// - Easley, D., et al. (2012). "Flow Toxicity and Liquidity in a High-frequency World"
/// - Glosten, L. & Milgrom, P. (1985). "Bid, Ask and Transaction Prices"
/// - Kyle, A.S. (1985). "Continuous Auctions and Insider Trading"
#[derive(Debug, Clone, Serialize, Default)]
pub struct ToxicityMetrics {
    pub timestamp: String,

    /// Toxic Flow Ratio (relative to microprice)
    /// Proportion of volume that moves against fair value
    /// Range: [0, 1], higher = more toxic
    pub toxic_flow_ratio_micro: Option<Decimal>,

    /// Toxic Flow Ratio (relative to mid price)
    pub toxic_flow_ratio_mid: Option<Decimal>,

    /// Adverse Selection Cost (microprice basis)
    /// Expected loss per unit traded to informed traders
    /// In price units (e.g., USD)
    pub adverse_selection_micro: Option<Decimal>,

    /// Adverse Selection Cost (mid price basis)
    pub adverse_selection_mid: Option<Decimal>,

    /// Trade Arrival Asymmetry
    /// Difference between buy and sell arrival rates
    /// Positive = more buys, Negative = more sells
    pub arrival_asymmetry: Option<Decimal>,

    /// Size-Weighted Toxicity
    /// Are larger trades more informed?
    /// > 1.0 means large trades are more toxic
    pub size_toxicity_ratio: Option<Decimal>,

    /// Flow Toxicity Index (composite score)
    /// Weighted combination of toxicity measures
    /// Range: [0, 1], higher = more toxic environment
    pub toxicity_index: Option<Decimal>,
}

pub struct ToxicityConfig {
    pub snapshot_interval_ms: u64,
    pub lookback_trades: usize,
}

impl Default for ToxicityConfig {
    fn default() -> Self {
        Self {
            snapshot_interval_ms: 100,
            lookback_trades: 1000,
        }
    }
}

pub struct ToxicityEngine {
    order_book: Arc<ConcurrentOrderBook>,
    trades_log: Arc<ConcurrentTradesLog>,
    config: ToxicityConfig,
    tx: mpsc::Sender<ToxicityMetrics>,
}

impl ToxicityEngine {
    pub fn new(
        order_book: Arc<ConcurrentOrderBook>,
        trades_log: Arc<ConcurrentTradesLog>,
        config: Option<ToxicityConfig>,
        tx: mpsc::Sender<ToxicityMetrics>,
    ) -> Self {
        Self {
            order_book,
            trades_log,
            config: config.unwrap_or_default(),
            tx,
        }
    }

    pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) -> anyhow::Result<()> {
        let mut ticker = interval(Duration::from_millis(self.config.snapshot_interval_ms));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let metrics = self.compute_metrics().await;
                    if self.tx.send(metrics).await.is_err() {
                        break;
                    }
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn compute_metrics(&self) -> ToxicityMetrics {
        let timestamp = chrono::Utc::now().to_rfc3339();

        // Get current fair value estimates
        let ob_snapshot = self.order_book.get_snapshot().await;
        let microprice = ob_snapshot.microprice;
        let mid_price = ob_snapshot.mid_price;

        // Get recent trades
        let trades = self.trades_log.last_n_trades(self.config.lookback_trades).await;

        if trades.is_empty() || microprice.is_none() || mid_price.is_none() {
            return ToxicityMetrics {
                timestamp,
                ..Default::default()
            };
        }

        let microprice = microprice.unwrap();
        let mid_price = mid_price.unwrap();

        // Compute toxic flow ratios
        let (toxic_micro, toxic_mid) = self.compute_toxic_flow_ratio(&trades, microprice, mid_price);

        // Compute adverse selection costs
        let (adv_sel_micro, adv_sel_mid) = self.compute_adverse_selection(&trades, microprice, mid_price);

        // Compute arrival asymmetry
        let arrival_asymmetry = self.compute_arrival_asymmetry(&trades);

        // Compute size-weighted toxicity
        let size_toxicity = self.compute_size_toxicity(&trades, microprice);

        // Compute composite toxicity index
        let toxicity_index = self.compute_toxicity_index(
            toxic_micro,
            adv_sel_micro,
            arrival_asymmetry,
            size_toxicity,
        );

        ToxicityMetrics {
            timestamp,
            toxic_flow_ratio_micro: toxic_micro,
            toxic_flow_ratio_mid: toxic_mid,
            adverse_selection_micro: adv_sel_micro,
            adverse_selection_mid: adv_sel_mid,
            arrival_asymmetry,
            size_toxicity_ratio: size_toxicity,
            toxicity_index,
        }
    }

    /// Compute toxic flow ratio
    /// Toxic flow = trades that move price against the trader
    /// Buy at price > fair value or Sell at price < fair value
    fn compute_toxic_flow_ratio(
        &self,
        trades: &[crate::data::tradeslog::Trade],
        microprice: Decimal,
        mid_price: Decimal,
    ) -> (Option<Decimal>, Option<Decimal>) {
        if trades.is_empty() {
            return (None, None);
        }

        let mut toxic_volume_micro = dec!(0);
        let mut toxic_volume_mid = dec!(0);
        let mut total_volume = dec!(0);

        for trade in trades {
            let volume = trade.quantity;
            total_volume += volume;

            // is_buyer_maker = true means seller was aggressor (sell trade)
            // is_buyer_maker = false means buyer was aggressor (buy trade)
            let is_buy = !trade.is_buyer_maker;

            // Toxic for microprice: buy above microprice, sell below microprice
            if is_buy && trade.price > microprice {
                toxic_volume_micro += volume;
            } else if !is_buy && trade.price < microprice {
                toxic_volume_micro += volume;
            }

            // Toxic for mid price
            if is_buy && trade.price > mid_price {
                toxic_volume_mid += volume;
            } else if !is_buy && trade.price < mid_price {
                toxic_volume_mid += volume;
            }
        }

        if total_volume == dec!(0) {
            return (None, None);
        }

        (
            Some(toxic_volume_micro / total_volume),
            Some(toxic_volume_mid / total_volume),
        )
    }

    /// Compute adverse selection cost
    /// Average loss per trade to informed traders
    fn compute_adverse_selection(
        &self,
        trades: &[crate::data::tradeslog::Trade],
        microprice: Decimal,
        mid_price: Decimal,
    ) -> (Option<Decimal>, Option<Decimal>) {
        if trades.is_empty() {
            return (None, None);
        }

        let mut total_cost_micro = dec!(0);
        let mut total_cost_mid = dec!(0);
        let mut count = dec!(0);

        for trade in trades {
            let is_buy = !trade.is_buyer_maker;

            // Adverse selection = execution price - fair value (for buys)
            // or fair value - execution price (for sells)
            let cost_micro = if is_buy {
                trade.price - microprice
            } else {
                microprice - trade.price
            };

            let cost_mid = if is_buy {
                trade.price - mid_price
            } else {
                mid_price - trade.price
            };

            // Only count adverse (positive cost)
            if cost_micro > dec!(0) {
                total_cost_micro += cost_micro * trade.quantity;
            }
            if cost_mid > dec!(0) {
                total_cost_mid += cost_mid * trade.quantity;
            }

            count += trade.quantity;
        }

        if count == dec!(0) {
            return (None, None);
        }

        (
            Some(total_cost_micro / count),
            Some(total_cost_mid / count),
        )
    }

    /// Compute trade arrival asymmetry
    /// Normalized difference between buy and sell rates
    fn compute_arrival_asymmetry(&self, trades: &[crate::data::tradeslog::Trade]) -> Option<Decimal> {
        if trades.is_empty() {
            return None;
        }

        let mut buy_count = dec!(0);
        let mut sell_count = dec!(0);

        for trade in trades {
            if trade.is_buyer_maker {
                sell_count += dec!(1);
            } else {
                buy_count += dec!(1);
            }
        }

        let total = buy_count + sell_count;
        if total == dec!(0) {
            return None;
        }

        // Normalized asymmetry: (buys - sells) / total
        Some((buy_count - sell_count) / total)
    }

    /// Compute size-weighted toxicity
    /// Compares toxicity of large trades vs small trades
    fn compute_size_toxicity(
        &self,
        trades: &[crate::data::tradeslog::Trade],
        microprice: Decimal,
    ) -> Option<Decimal> {
        if trades.len() < 10 {
            return None;
        }

        // Calculate median trade size
        let mut sizes: Vec<Decimal> = trades.iter().map(|t| t.quantity).collect();
        sizes.sort();
        let median_size = sizes[sizes.len() / 2];

        if median_size == dec!(0) {
            return None;
        }

        let mut large_toxic = dec!(0);
        let mut large_total = dec!(0);
        let mut small_toxic = dec!(0);
        let mut small_total = dec!(0);

        for trade in trades {
            let is_buy = !trade.is_buyer_maker;
            let is_toxic = (is_buy && trade.price > microprice) ||
                          (!is_buy && trade.price < microprice);

            if trade.quantity >= median_size {
                large_total += trade.quantity;
                if is_toxic {
                    large_toxic += trade.quantity;
                }
            } else {
                small_total += trade.quantity;
                if is_toxic {
                    small_toxic += trade.quantity;
                }
            }
        }

        if small_total == dec!(0) || large_total == dec!(0) {
            return None;
        }

        let large_toxic_ratio = large_toxic / large_total;
        let small_toxic_ratio = small_toxic / small_total;

        if small_toxic_ratio == dec!(0) {
            return Some(dec!(1));
        }

        // Ratio > 1 means large trades are more toxic
        Some(large_toxic_ratio / small_toxic_ratio)
    }

    /// Compute composite toxicity index
    /// Weighted average of normalized toxicity measures
    fn compute_toxicity_index(
        &self,
        toxic_flow: Option<Decimal>,
        adverse_selection: Option<Decimal>,
        arrival_asymmetry: Option<Decimal>,
        size_toxicity: Option<Decimal>,
    ) -> Option<Decimal> {
        let mut sum = dec!(0);
        let mut weight = dec!(0);

        // Toxic flow ratio (already 0-1)
        if let Some(tf) = toxic_flow {
            sum += tf * dec!(0.3);
            weight += dec!(0.3);
        }

        // Adverse selection (normalize by typical spread, cap at 1)
        if let Some(adv) = adverse_selection {
            let normalized = (adv * dec!(10000)).min(dec!(1)); // Assume 1bp = 0.0001
            sum += normalized * dec!(0.3);
            weight += dec!(0.3);
        }

        // Arrival asymmetry (absolute value, already normalized)
        if let Some(asym) = arrival_asymmetry {
            let normalized = asym.abs().min(dec!(1));
            sum += normalized * dec!(0.2);
            weight += dec!(0.2);
        }

        // Size toxicity (normalize around 1.0)
        if let Some(st) = size_toxicity {
            let normalized = ((st - dec!(1)).abs() / dec!(2)).min(dec!(1));
            sum += normalized * dec!(0.2);
            weight += dec!(0.2);
        }

        if weight == dec!(0) {
            return None;
        }

        Some(sum / weight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toxicity_metrics_default() {
        let metrics = ToxicityMetrics::default();
        assert!(metrics.toxic_flow_ratio_micro.is_none());
        assert!(metrics.toxicity_index.is_none());
    }
}

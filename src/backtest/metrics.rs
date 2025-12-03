//! Performance Metrics
//!
//! Calculate trading performance metrics from backtest results.

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

/// A single trade record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub timestamp_ms: i64,
    pub side: TradeSide,
    pub price: Decimal,
    pub size: Decimal,
    pub fee: Decimal,
    pub pnl: Option<Decimal>, // Realized PnL if closing
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Equity curve point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPoint {
    pub timestamp_ms: i64,
    pub equity: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub inventory: Decimal,
}

/// Equity curve over time
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EquityCurve {
    pub points: Vec<EquityPoint>,
}

impl EquityCurve {
    pub fn new() -> Self {
        Self { points: Vec::new() }
    }

    pub fn add(&mut self, point: EquityPoint) {
        self.points.push(point);
    }

    /// Get returns as a vector
    pub fn returns(&self) -> Vec<f64> {
        if self.points.len() < 2 {
            return vec![];
        }

        self.points
            .windows(2)
            .map(|w| {
                let prev = w[0].equity.to_f64().unwrap_or(1.0);
                let curr = w[1].equity.to_f64().unwrap_or(1.0);
                if prev == 0.0 {
                    0.0
                } else {
                    (curr - prev) / prev
                }
            })
            .collect()
    }

    /// Get log returns
    pub fn log_returns(&self) -> Vec<f64> {
        if self.points.len() < 2 {
            return vec![];
        }

        self.points
            .windows(2)
            .map(|w| {
                let prev = w[0].equity.to_f64().unwrap_or(1.0);
                let curr = w[1].equity.to_f64().unwrap_or(1.0);
                if prev <= 0.0 || curr <= 0.0 {
                    0.0
                } else {
                    (curr / prev).ln()
                }
            })
            .collect()
    }

    /// Get final equity
    pub fn final_equity(&self) -> Decimal {
        self.points.last().map(|p| p.equity).unwrap_or(dec!(0))
    }

    /// Get peak equity
    pub fn peak_equity(&self) -> Decimal {
        self.points.iter().map(|p| p.equity).max().unwrap_or(dec!(0))
    }
}

/// Trade log
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TradeLog {
    pub trades: Vec<TradeRecord>,
}

impl TradeLog {
    pub fn new() -> Self {
        Self { trades: Vec::new() }
    }

    pub fn add(&mut self, trade: TradeRecord) {
        self.trades.push(trade);
    }

    pub fn len(&self) -> usize {
        self.trades.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trades.is_empty()
    }

    /// Total realized PnL
    pub fn total_pnl(&self) -> Decimal {
        self.trades
            .iter()
            .filter_map(|t| t.pnl)
            .sum()
    }

    /// Total fees paid
    pub fn total_fees(&self) -> Decimal {
        self.trades.iter().map(|t| t.fee).sum()
    }

    /// Total volume traded
    pub fn total_volume(&self) -> Decimal {
        self.trades.iter().map(|t| t.size).sum()
    }

    /// Number of buys
    pub fn num_buys(&self) -> usize {
        self.trades.iter().filter(|t| t.side == TradeSide::Buy).count()
    }

    /// Number of sells
    pub fn num_sells(&self) -> usize {
        self.trades.iter().filter(|t| t.side == TradeSide::Sell).count()
    }

    /// Win rate (trades with positive PnL)
    pub fn win_rate(&self) -> f64 {
        let with_pnl: Vec<_> = self.trades.iter().filter_map(|t| t.pnl).collect();
        if with_pnl.is_empty() {
            return 0.0;
        }
        let wins = with_pnl.iter().filter(|&&p| p > dec!(0)).count();
        wins as f64 / with_pnl.len() as f64
    }

    /// Average trade PnL
    pub fn avg_pnl(&self) -> Decimal {
        let with_pnl: Vec<_> = self.trades.iter().filter_map(|t| t.pnl).collect();
        if with_pnl.is_empty() {
            return dec!(0);
        }
        let sum: Decimal = with_pnl.iter().sum();
        sum / Decimal::from(with_pnl.len())
    }

    /// Average winning trade
    pub fn avg_win(&self) -> Decimal {
        let wins: Vec<_> = self.trades
            .iter()
            .filter_map(|t| t.pnl)
            .filter(|&p| p > dec!(0))
            .collect();
        if wins.is_empty() {
            return dec!(0);
        }
        let sum: Decimal = wins.iter().sum();
        sum / Decimal::from(wins.len())
    }

    /// Average losing trade
    pub fn avg_loss(&self) -> Decimal {
        let losses: Vec<_> = self.trades
            .iter()
            .filter_map(|t| t.pnl)
            .filter(|&p| p < dec!(0))
            .collect();
        if losses.is_empty() {
            return dec!(0);
        }
        let sum: Decimal = losses.iter().sum();
        sum / Decimal::from(losses.len())
    }

    /// Profit factor (gross profit / gross loss)
    pub fn profit_factor(&self) -> f64 {
        let gross_profit: Decimal = self.trades
            .iter()
            .filter_map(|t| t.pnl)
            .filter(|&p| p > dec!(0))
            .sum();
        let gross_loss: Decimal = self.trades
            .iter()
            .filter_map(|t| t.pnl)
            .filter(|&p| p < dec!(0))
            .map(|p| p.abs())
            .sum();

        if gross_loss == dec!(0) {
            if gross_profit > dec!(0) {
                f64::INFINITY
            } else {
                0.0
            }
        } else {
            gross_profit.to_f64().unwrap_or(0.0) / gross_loss.to_f64().unwrap_or(1.0)
        }
    }
}

/// Complete performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    // Returns
    pub total_return: f64,
    pub annualized_return: f64,

    // Risk
    pub volatility: f64,
    pub annualized_volatility: f64,
    pub max_drawdown: f64,
    pub max_drawdown_duration_ms: i64,

    // Risk-adjusted
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,

    // Trading
    pub num_trades: usize,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub avg_trade_pnl: Decimal,

    // Inventory
    pub avg_inventory: f64,
    pub max_inventory: f64,
    pub inventory_turnover: f64,

    // Execution
    pub total_fees: Decimal,
    pub total_volume: Decimal,
    pub pnl_per_volume: f64,

    // Time
    pub duration_ms: i64,
    pub start_time: i64,
    pub end_time: i64,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            total_return: 0.0,
            annualized_return: 0.0,
            volatility: 0.0,
            annualized_volatility: 0.0,
            max_drawdown: 0.0,
            max_drawdown_duration_ms: 0,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
            num_trades: 0,
            win_rate: 0.0,
            profit_factor: 0.0,
            avg_trade_pnl: dec!(0),
            avg_inventory: 0.0,
            max_inventory: 0.0,
            inventory_turnover: 0.0,
            total_fees: dec!(0),
            total_volume: dec!(0),
            pnl_per_volume: 0.0,
            duration_ms: 0,
            start_time: 0,
            end_time: 0,
        }
    }
}

impl PerformanceMetrics {
    /// Calculate metrics from equity curve and trade log
    pub fn calculate(
        equity_curve: &EquityCurve,
        trade_log: &TradeLog,
        risk_free_rate: f64,
    ) -> Self {
        if equity_curve.points.is_empty() {
            return Self::default();
        }

        let returns = equity_curve.returns();
        let _log_returns = equity_curve.log_returns();

        // Time info
        let start_time = equity_curve.points.first().map(|p| p.timestamp_ms).unwrap_or(0);
        let end_time = equity_curve.points.last().map(|p| p.timestamp_ms).unwrap_or(0);
        let duration_ms = end_time - start_time;

        // Calculate basic returns
        let initial_equity = equity_curve.points.first()
            .map(|p| p.equity.to_f64().unwrap_or(1.0))
            .unwrap_or(1.0);
        let final_equity = equity_curve.points.last()
            .map(|p| p.equity.to_f64().unwrap_or(1.0))
            .unwrap_or(1.0);

        let total_return = if initial_equity > 0.0 {
            (final_equity - initial_equity) / initial_equity
        } else {
            0.0
        };

        // Annualization factor (assuming millisecond timestamps)
        let years = duration_ms as f64 / (365.25 * 24.0 * 60.0 * 60.0 * 1000.0);
        let annualization_factor = if years > 0.0 { 1.0 / years } else { 1.0 };

        let annualized_return = if years > 0.0 {
            (1.0 + total_return).powf(1.0 / years) - 1.0
        } else {
            total_return
        };

        // Volatility
        let volatility = std_dev(&returns);
        let annualized_volatility = volatility * (annualization_factor * returns.len() as f64).sqrt();

        // Max drawdown
        let (max_drawdown, max_dd_duration) = calculate_max_drawdown(&equity_curve.points);

        // Sharpe ratio (assuming daily-ish snapshots)
        let mean_return = mean(&returns);
        let excess_return = mean_return - risk_free_rate / 252.0; // Daily risk-free
        let sharpe_ratio = if volatility > 0.0 {
            excess_return / volatility * (252.0_f64).sqrt() // Annualized
        } else {
            0.0
        };

        // Sortino ratio (downside deviation)
        let downside_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        let downside_dev = std_dev(&downside_returns);
        let sortino_ratio = if downside_dev > 0.0 {
            excess_return / downside_dev * (252.0_f64).sqrt()
        } else {
            0.0
        };

        // Calmar ratio
        let calmar_ratio = if max_drawdown > 0.0 {
            annualized_return / max_drawdown
        } else {
            0.0
        };

        // Inventory metrics
        let inventories: Vec<f64> = equity_curve.points
            .iter()
            .map(|p| p.inventory.to_f64().unwrap_or(0.0).abs())
            .collect();
        let avg_inventory = mean(&inventories);
        let max_inventory = inventories.iter().cloned().fold(0.0_f64, f64::max);

        let total_volume = trade_log.total_volume();
        let inventory_turnover = if avg_inventory > 0.0 {
            total_volume.to_f64().unwrap_or(0.0) / (avg_inventory * years.max(0.001) * 365.25)
        } else {
            0.0
        };

        // PnL per volume
        let total_pnl = trade_log.total_pnl();
        let pnl_per_volume = if total_volume > dec!(0) {
            total_pnl.to_f64().unwrap_or(0.0) / total_volume.to_f64().unwrap_or(1.0)
        } else {
            0.0
        };

        Self {
            total_return,
            annualized_return,
            volatility,
            annualized_volatility,
            max_drawdown,
            max_drawdown_duration_ms: max_dd_duration,
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
            num_trades: trade_log.len(),
            win_rate: trade_log.win_rate(),
            profit_factor: trade_log.profit_factor(),
            avg_trade_pnl: trade_log.avg_pnl(),
            avg_inventory,
            max_inventory,
            inventory_turnover,
            total_fees: trade_log.total_fees(),
            total_volume,
            pnl_per_volume,
            duration_ms,
            start_time,
            end_time,
        }
    }

    /// Print a summary report
    pub fn print_report(&self) {
        println!("═══════════════════════════════════════════════════════");
        println!("                 BACKTEST RESULTS                       ");
        println!("═══════════════════════════════════════════════════════");
        println!();
        println!("RETURNS");
        println!("  Total Return:        {:>10.2}%", self.total_return * 100.0);
        println!("  Annualized Return:   {:>10.2}%", self.annualized_return * 100.0);
        println!();
        println!("RISK");
        println!("  Volatility (ann.):   {:>10.2}%", self.annualized_volatility * 100.0);
        println!("  Max Drawdown:        {:>10.2}%", self.max_drawdown * 100.0);
        println!();
        println!("RISK-ADJUSTED");
        println!("  Sharpe Ratio:        {:>10.2}", self.sharpe_ratio);
        println!("  Sortino Ratio:       {:>10.2}", self.sortino_ratio);
        println!("  Calmar Ratio:        {:>10.2}", self.calmar_ratio);
        println!();
        println!("TRADING");
        println!("  Num Trades:          {:>10}", self.num_trades);
        println!("  Win Rate:            {:>10.1}%", self.win_rate * 100.0);
        println!("  Profit Factor:       {:>10.2}", self.profit_factor);
        println!("  Avg Trade PnL:       {:>10.6}", self.avg_trade_pnl);
        println!();
        println!("EXECUTION");
        println!("  Total Volume:        {:>10.4}", self.total_volume);
        println!("  Total Fees:          {:>10.6}", self.total_fees);
        println!("  PnL/Volume:          {:>10.6}", self.pnl_per_volume);
        println!();
        println!("INVENTORY");
        println!("  Avg Inventory:       {:>10.6}", self.avg_inventory);
        println!("  Max Inventory:       {:>10.6}", self.max_inventory);
        println!("═══════════════════════════════════════════════════════");
    }
}

// Helper functions

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn std_dev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let m = mean(values);
    let variance = values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    variance.sqrt()
}

fn calculate_max_drawdown(points: &[EquityPoint]) -> (f64, i64) {
    if points.is_empty() {
        return (0.0, 0);
    }

    let mut peak = points[0].equity.to_f64().unwrap_or(0.0);
    let mut peak_time = points[0].timestamp_ms;
    let mut max_dd = 0.0;
    let mut max_dd_duration = 0_i64;

    for point in points {
        let equity = point.equity.to_f64().unwrap_or(0.0);

        if equity > peak {
            peak = equity;
            peak_time = point.timestamp_ms;
        }

        let dd = if peak > 0.0 { (peak - equity) / peak } else { 0.0 };

        if dd > max_dd {
            max_dd = dd;
            max_dd_duration = point.timestamp_ms - peak_time;
        }
    }

    (max_dd, max_dd_duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean() {
        assert_eq!(mean(&[1.0, 2.0, 3.0]), 2.0);
        assert_eq!(mean(&[]), 0.0);
    }

    #[test]
    fn test_std_dev() {
        let vals = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let sd = std_dev(&vals);
        // Sample std dev for this data is ~2.138, not 2.0 (which is population std dev)
        assert!((sd - 2.138).abs() < 0.1, "Expected ~2.138 but got {}", sd);
    }

    #[test]
    fn test_trade_log() {
        let mut log = TradeLog::new();
        log.add(TradeRecord {
            timestamp_ms: 1000,
            side: TradeSide::Buy,
            price: dec!(100),
            size: dec!(1),
            fee: dec!(0.1),
            pnl: None,
        });
        log.add(TradeRecord {
            timestamp_ms: 2000,
            side: TradeSide::Sell,
            price: dec!(101),
            size: dec!(1),
            fee: dec!(0.1),
            pnl: Some(dec!(1)),
        });

        assert_eq!(log.len(), 2);
        assert_eq!(log.num_buys(), 1);
        assert_eq!(log.num_sells(), 1);
        assert_eq!(log.total_fees(), dec!(0.2));
    }

    #[test]
    fn test_equity_curve_returns() {
        let mut curve = EquityCurve::new();
        curve.add(EquityPoint {
            timestamp_ms: 0,
            equity: dec!(100),
            unrealized_pnl: dec!(0),
            realized_pnl: dec!(0),
            inventory: dec!(0),
        });
        curve.add(EquityPoint {
            timestamp_ms: 1000,
            equity: dec!(110),
            unrealized_pnl: dec!(0),
            realized_pnl: dec!(10),
            inventory: dec!(0),
        });

        let returns = curve.returns();
        assert_eq!(returns.len(), 1);
        assert!((returns[0] - 0.1).abs() < 0.001);
    }
}

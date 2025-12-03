//! Forward Testing Infrastructure
//!
//! Logs paper trading activity and compares against backtest predictions.
//!
//! # Features
//!
//! - Trade logging to JSON/Parquet files
//! - Real-time session metrics (Sharpe, PnL, drawdown)
//! - Backtest vs live comparison reports
//! - Execution quality tracking (latency, fill rates)
//!
//! # Usage
//!
//! ```ignore
//! let mut session = ForwardTestSession::new(ForwardTestConfig::default());
//! session.start();
//!
//! // On each trade...
//! session.log_trade(&trade);
//!
//! // On each quote update...
//! session.log_quote(&quotes, mid_price);
//!
//! // Get live metrics
//! let metrics = session.metrics();
//!
//! // End session and save
//! session.end()?;
//! ```

use std::path::PathBuf;
use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use crate::market_maker::{Fill, QuoteSide, MMState};

/// Configuration for forward testing session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardTestConfig {
    /// Directory to save session logs
    pub log_dir: PathBuf,
    /// Save trades to JSON file
    pub log_trades: bool,
    /// Save quotes to file (can be large)
    pub log_quotes: bool,
    /// Rolling window for Sharpe calculation (in trades)
    pub sharpe_window: usize,
    /// Session name/identifier
    pub session_name: Option<String>,
}

impl Default for ForwardTestConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("./data/sessions"),
            log_trades: true,
            log_quotes: false, // Can be very large
            sharpe_window: 100,
            session_name: None,
        }
    }
}

/// A logged trade record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    /// Unique trade ID within session
    pub id: u64,
    /// Timestamp when trade occurred
    pub timestamp: DateTime<Utc>,
    /// Timestamp in milliseconds
    pub timestamp_ms: u64,
    /// Side (buy/sell)
    pub side: String,
    /// Fill price
    pub price: Decimal,
    /// Fill size
    pub size: Decimal,
    /// Fee paid
    pub fee: Decimal,
    /// PnL from this trade (if closing position)
    pub pnl: Option<Decimal>,
    /// Inventory after this trade
    pub inventory_after: Decimal,
    /// Total PnL after this trade
    pub total_pnl_after: Decimal,
    /// Mid price at time of fill (for slippage calculation)
    pub mid_price: Decimal,
    /// Slippage in bps (positive = unfavorable)
    pub slippage_bps: f64,
}

/// A logged quote record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteRecord {
    pub timestamp: DateTime<Utc>,
    pub timestamp_ms: u64,
    pub bid_price: Option<Decimal>,
    pub bid_size: Option<Decimal>,
    pub ask_price: Option<Decimal>,
    pub ask_size: Option<Decimal>,
    pub mid_price: Decimal,
    pub spread_bps: f64,
    pub inventory: Decimal,
    pub regime: String,
}

/// Real-time session metrics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionMetrics {
    /// Session start time
    pub start_time: Option<DateTime<Utc>>,
    /// Session duration in seconds
    pub duration_secs: f64,
    /// Total trades executed
    pub total_trades: u64,
    /// Buy trades
    pub buy_trades: u64,
    /// Sell trades
    pub sell_trades: u64,
    /// Total volume traded
    pub total_volume: Decimal,
    /// Gross PnL (before fees)
    pub gross_pnl: Decimal,
    /// Total fees paid
    pub total_fees: Decimal,
    /// Net PnL (after fees)
    pub net_pnl: Decimal,
    /// Realized PnL (from closed positions)
    pub realized_pnl: Decimal,
    /// Unrealized PnL (from open inventory)
    pub unrealized_pnl: Decimal,
    /// Current inventory
    pub inventory: Decimal,
    /// Peak inventory (max absolute)
    pub peak_inventory: Decimal,
    /// Maximum drawdown
    pub max_drawdown: f64,
    /// Peak equity (for drawdown calculation)
    pub peak_equity: Decimal,
    /// Win rate (% of profitable trades)
    pub win_rate: f64,
    /// Winning trades
    pub winning_trades: u64,
    /// Losing trades
    pub losing_trades: u64,
    /// Average trade PnL
    pub avg_trade_pnl: Decimal,
    /// Sharpe ratio (rolling window)
    pub sharpe_ratio: f64,
    /// Profit factor (gross profit / gross loss)
    pub profit_factor: f64,
    /// Average slippage in bps
    pub avg_slippage_bps: f64,
    /// Bid fill rate
    pub bid_fill_rate: f64,
    /// Ask fill rate
    pub ask_fill_rate: f64,
    /// Quotes generated
    pub quotes_generated: u64,
    /// Bid touches (price reached our bid)
    pub bid_touches: u64,
    /// Ask touches
    pub ask_touches: u64,
}

impl SessionMetrics {
    /// Print a formatted summary
    pub fn print_summary(&self) {
        println!();
        println!("════════════════════════════════════════════════════════");
        println!("           FORWARD TEST SESSION SUMMARY");
        println!("════════════════════════════════════════════════════════");
        println!();

        if let Some(start) = self.start_time {
            println!("Session Start: {}", start.format("%Y-%m-%d %H:%M:%S UTC"));
        }
        println!("Duration:      {:.1} minutes", self.duration_secs / 60.0);
        println!();

        println!("TRADING ACTIVITY:");
        println!("  Total Trades:    {}", self.total_trades);
        println!("  Buy / Sell:      {} / {}", self.buy_trades, self.sell_trades);
        println!("  Total Volume:    {:.4}", self.total_volume);
        println!("  Current Inv:     {:+.4}", self.inventory);
        println!("  Peak Inventory:  {:.4}", self.peak_inventory);
        println!();

        println!("PERFORMANCE:");
        println!("  Net PnL:         {:+.4}", self.net_pnl);
        println!("  Realized PnL:    {:+.4}", self.realized_pnl);
        println!("  Unrealized PnL:  {:+.4}", self.unrealized_pnl);
        println!("  Total Fees:      {:.4}", self.total_fees);
        println!("  Max Drawdown:    {:.2}%", self.max_drawdown * 100.0);
        println!();

        println!("QUALITY METRICS:");
        println!("  Sharpe Ratio:    {:+.2}", self.sharpe_ratio);
        println!("  Win Rate:        {:.1}%", self.win_rate * 100.0);
        println!("  Profit Factor:   {:.2}", self.profit_factor);
        println!("  Avg Slippage:    {:.2} bps", self.avg_slippage_bps);
        println!();

        println!("FILL RATES:");
        println!("  Bid Fill Rate:   {:.1}%", self.bid_fill_rate * 100.0);
        println!("  Ask Fill Rate:   {:.1}%", self.ask_fill_rate * 100.0);
        println!("  Quotes Gen:      {}", self.quotes_generated);
        println!("════════════════════════════════════════════════════════");
    }
}

/// Forward test session manager
pub struct ForwardTestSession {
    config: ForwardTestConfig,
    /// Session ID (timestamp-based)
    session_id: String,
    /// Trade log
    trades: Vec<TradeRecord>,
    /// Quote log (optional, can be large)
    quotes: Vec<QuoteRecord>,
    /// Rolling PnL window for Sharpe calculation
    pnl_window: VecDeque<f64>,
    /// Current metrics
    metrics: SessionMetrics,
    /// Trade counter
    trade_counter: u64,
    /// Last mid price
    last_mid_price: Decimal,
    /// Cumulative slippage for averaging
    total_slippage_bps: f64,
    /// Gross profit (for profit factor)
    gross_profit: Decimal,
    /// Gross loss (for profit factor)
    gross_loss: Decimal,
    /// Is session active
    is_active: bool,
}

impl ForwardTestSession {
    /// Create a new session
    pub fn new(config: ForwardTestConfig) -> Self {
        let session_id = Utc::now().format("%Y%m%d_%H%M%S").to_string();

        Self {
            config,
            session_id,
            trades: Vec::new(),
            quotes: Vec::new(),
            pnl_window: VecDeque::new(),
            metrics: SessionMetrics::default(),
            trade_counter: 0,
            last_mid_price: dec!(0),
            total_slippage_bps: 0.0,
            gross_profit: dec!(0),
            gross_loss: dec!(0),
            is_active: false,
        }
    }

    /// Start the session
    pub fn start(&mut self) {
        self.metrics.start_time = Some(Utc::now());
        self.is_active = true;

        // Create log directory if needed
        if self.config.log_trades || self.config.log_quotes {
            let _ = std::fs::create_dir_all(&self.config.log_dir);
        }
    }

    /// Log a trade/fill
    pub fn log_trade(
        &mut self,
        fill: &Fill,
        mm_state: &MMState,
        mid_price: Decimal,
        fee: Decimal,
    ) {
        self.trade_counter += 1;

        // Calculate slippage (difference from mid price)
        let slippage_bps = if mid_price > dec!(0) {
            match fill.side {
                QuoteSide::Bid => {
                    // Buying: paying more than mid is bad
                    ((fill.price - mid_price) / mid_price * dec!(10000))
                        .to_f64()
                        .unwrap_or(0.0)
                }
                QuoteSide::Ask => {
                    // Selling: receiving less than mid is bad
                    ((mid_price - fill.price) / mid_price * dec!(10000))
                        .to_f64()
                        .unwrap_or(0.0)
                }
            }
        } else {
            0.0
        };

        // Calculate trade PnL (if closing position)
        let trade_pnl = self.calculate_trade_pnl(fill, mm_state);

        let record = TradeRecord {
            id: self.trade_counter,
            timestamp: Utc::now(),
            timestamp_ms: fill.timestamp_ms,
            side: match fill.side {
                QuoteSide::Bid => "BUY".to_string(),
                QuoteSide::Ask => "SELL".to_string(),
            },
            price: fill.price,
            size: fill.size,
            fee,
            pnl: trade_pnl,
            inventory_after: mm_state.inventory,
            total_pnl_after: mm_state.pnl.total_pnl,
            mid_price,
            slippage_bps,
        };

        // Update metrics
        self.update_metrics_on_trade(&record, mm_state);

        // Store trade
        self.trades.push(record);
    }

    /// Calculate PnL for this trade (if closing position)
    fn calculate_trade_pnl(&self, fill: &Fill, mm_state: &MMState) -> Option<Decimal> {
        let prev_inventory = mm_state.inventory - match fill.side {
            QuoteSide::Bid => fill.size,
            QuoteSide::Ask => -fill.size,
        };

        // Check if this trade is closing (partially or fully)
        match fill.side {
            QuoteSide::Ask if prev_inventory > dec!(0) => {
                // Selling while long = closing
                let close_size = fill.size.min(prev_inventory);
                if close_size > dec!(0) && mm_state.avg_entry_price > dec!(0) {
                    Some((fill.price - mm_state.avg_entry_price) * close_size)
                } else {
                    None
                }
            }
            QuoteSide::Bid if prev_inventory < dec!(0) => {
                // Buying while short = closing
                let close_size = fill.size.min(prev_inventory.abs());
                if close_size > dec!(0) && mm_state.avg_entry_price > dec!(0) {
                    Some((mm_state.avg_entry_price - fill.price) * close_size)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Update metrics after a trade
    fn update_metrics_on_trade(&mut self, record: &TradeRecord, mm_state: &MMState) {
        self.metrics.total_trades += 1;

        match record.side.as_str() {
            "BUY" => self.metrics.buy_trades += 1,
            "SELL" => self.metrics.sell_trades += 1,
            _ => {}
        }

        self.metrics.total_volume += record.size;
        self.metrics.total_fees += record.fee;
        self.metrics.inventory = record.inventory_after;
        self.metrics.net_pnl = mm_state.pnl.total_pnl;
        self.metrics.realized_pnl = mm_state.pnl.realized_pnl;
        self.metrics.unrealized_pnl = mm_state.pnl.unrealized_pnl;

        // Peak inventory
        if record.inventory_after.abs() > self.metrics.peak_inventory {
            self.metrics.peak_inventory = record.inventory_after.abs();
        }

        // Drawdown
        if mm_state.pnl.total_pnl > self.metrics.peak_equity {
            self.metrics.peak_equity = mm_state.pnl.total_pnl;
        }
        if self.metrics.peak_equity > dec!(0) {
            let dd = ((self.metrics.peak_equity - mm_state.pnl.total_pnl) / self.metrics.peak_equity)
                .to_f64()
                .unwrap_or(0.0);
            if dd > self.metrics.max_drawdown {
                self.metrics.max_drawdown = dd;
            }
        }

        // Win/loss tracking
        if let Some(pnl) = record.pnl {
            if pnl > dec!(0) {
                self.metrics.winning_trades += 1;
                self.gross_profit += pnl;
            } else if pnl < dec!(0) {
                self.metrics.losing_trades += 1;
                self.gross_loss += pnl.abs();
            }
        }

        // Win rate
        let total_closed = self.metrics.winning_trades + self.metrics.losing_trades;
        if total_closed > 0 {
            self.metrics.win_rate = self.metrics.winning_trades as f64 / total_closed as f64;
        }

        // Profit factor
        if self.gross_loss > dec!(0) {
            self.metrics.profit_factor = (self.gross_profit / self.gross_loss)
                .to_f64()
                .unwrap_or(0.0);
        }

        // Average trade PnL
        if self.metrics.total_trades > 0 {
            self.metrics.avg_trade_pnl = self.metrics.net_pnl
                / Decimal::from(self.metrics.total_trades);
        }

        // Slippage tracking
        self.total_slippage_bps += record.slippage_bps;
        self.metrics.avg_slippage_bps = self.total_slippage_bps / self.metrics.total_trades as f64;

        // Sharpe calculation (rolling window)
        if let Some(pnl) = record.pnl {
            let pnl_f64 = pnl.to_f64().unwrap_or(0.0);
            self.pnl_window.push_back(pnl_f64);
            if self.pnl_window.len() > self.config.sharpe_window {
                self.pnl_window.pop_front();
            }
            self.metrics.sharpe_ratio = self.calculate_sharpe();
        }

        // Duration
        if let Some(start) = self.metrics.start_time {
            self.metrics.duration_secs = (Utc::now() - start).num_milliseconds() as f64 / 1000.0;
        }
    }

    /// Calculate rolling Sharpe ratio
    fn calculate_sharpe(&self) -> f64 {
        if self.pnl_window.len() < 2 {
            return 0.0;
        }

        let n = self.pnl_window.len() as f64;
        let mean: f64 = self.pnl_window.iter().sum::<f64>() / n;
        let variance: f64 = self.pnl_window.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / (n - 1.0);
        let std_dev = variance.sqrt();

        if std_dev > 0.0 {
            mean / std_dev * (252.0_f64).sqrt() // Annualized
        } else {
            0.0
        }
    }

    /// Log a quote update
    ///
    /// Uses primitive values to avoid crate duplication issues between binary and library.
    pub fn log_quote(
        &mut self,
        timestamp_ms: u64,
        bid_price: Option<Decimal>,
        bid_size: Option<Decimal>,
        ask_price: Option<Decimal>,
        ask_size: Option<Decimal>,
        mid_price: Decimal,
        inventory: Decimal,
        regime: &str,
    ) {
        self.metrics.quotes_generated += 1;
        self.last_mid_price = mid_price;

        if !self.config.log_quotes {
            return;
        }

        let spread_bps = if let (Some(bid), Some(ask)) = (bid_price, ask_price) {
            ((ask - bid) / mid_price * dec!(10000))
                .to_f64()
                .unwrap_or(0.0)
        } else {
            0.0
        };

        let record = QuoteRecord {
            timestamp: Utc::now(),
            timestamp_ms,
            bid_price,
            bid_size,
            ask_price,
            ask_size,
            mid_price,
            spread_bps,
            inventory,
            regime: regime.to_string(),
        };

        self.quotes.push(record);
    }

    /// Record a quote touch (price reached our level but may not have filled)
    pub fn record_touch(&mut self, is_bid: bool) {
        if is_bid {
            self.metrics.bid_touches += 1;
        } else {
            self.metrics.ask_touches += 1;
        }

        // Update fill rates
        if self.metrics.bid_touches > 0 {
            self.metrics.bid_fill_rate =
                self.metrics.buy_trades as f64 / self.metrics.bid_touches as f64;
        }
        if self.metrics.ask_touches > 0 {
            self.metrics.ask_fill_rate =
                self.metrics.sell_trades as f64 / self.metrics.ask_touches as f64;
        }
    }

    /// Get current metrics
    pub fn metrics(&self) -> &SessionMetrics {
        &self.metrics
    }

    /// Get all trades
    pub fn trades(&self) -> &[TradeRecord] {
        &self.trades
    }

    /// End session and save logs
    pub fn end(&mut self) -> Result<SessionSummary> {
        self.is_active = false;

        // Update final duration
        if let Some(start) = self.metrics.start_time {
            self.metrics.duration_secs = (Utc::now() - start).num_milliseconds() as f64 / 1000.0;
        }

        // Save trades to JSON
        if self.config.log_trades && !self.trades.is_empty() {
            let filename = format!(
                "{}/trades_{}.json",
                self.config.log_dir.display(),
                self.session_id
            );
            let json = serde_json::to_string_pretty(&self.trades)?;
            std::fs::write(&filename, json)?;
        }

        // Save session summary
        let summary = SessionSummary {
            session_id: self.session_id.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            trade_count: self.trades.len(),
        };

        let summary_filename = format!(
            "{}/summary_{}.json",
            self.config.log_dir.display(),
            self.session_id
        );
        let json = serde_json::to_string_pretty(&summary)?;
        std::fs::write(&summary_filename, json)?;

        Ok(summary)
    }

    /// Get session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Check if session is active
    pub fn is_active(&self) -> bool {
        self.is_active
    }
}

/// Session summary for comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub config: ForwardTestConfig,
    pub metrics: SessionMetrics,
    pub trade_count: usize,
}

/// Comparison between backtest and forward test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestComparison {
    /// Backtest metrics
    pub backtest: ComparisonMetrics,
    /// Forward test metrics
    pub forward: ComparisonMetrics,
    /// Difference (forward - backtest)
    pub diff: ComparisonDiff,
    /// Overall assessment
    pub assessment: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ComparisonMetrics {
    pub sharpe_ratio: f64,
    pub total_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub trades_per_hour: f64,
    pub avg_slippage_bps: f64,
    pub bid_fill_rate: f64,
    pub ask_fill_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ComparisonDiff {
    pub sharpe_diff: f64,
    pub return_diff_pct: f64,
    pub drawdown_diff_pct: f64,
    pub win_rate_diff: f64,
    pub fill_rate_diff: f64,
}

impl BacktestComparison {
    /// Create a comparison from backtest results and session metrics
    pub fn new(
        backtest_metrics: &crate::backtest::PerformanceMetrics,
        backtest_fill_stats: &crate::backtest::harness::FillStats,
        backtest_duration_hours: f64,
        forward_metrics: &SessionMetrics,
    ) -> Self {
        let backtest = ComparisonMetrics {
            sharpe_ratio: backtest_metrics.sharpe_ratio,
            total_return_pct: backtest_metrics.total_return * 100.0,
            max_drawdown_pct: backtest_metrics.max_drawdown * 100.0,
            win_rate: backtest_metrics.win_rate,
            profit_factor: backtest_metrics.profit_factor,
            trades_per_hour: if backtest_duration_hours > 0.0 {
                backtest_metrics.num_trades as f64 / backtest_duration_hours
            } else {
                0.0
            },
            avg_slippage_bps: 0.0, // Not tracked in backtest
            bid_fill_rate: backtest_fill_stats.bid_fill_rate,
            ask_fill_rate: backtest_fill_stats.ask_fill_rate,
        };

        let forward_duration_hours = forward_metrics.duration_secs / 3600.0;
        let forward = ComparisonMetrics {
            sharpe_ratio: forward_metrics.sharpe_ratio,
            total_return_pct: forward_metrics.net_pnl.to_f64().unwrap_or(0.0) * 100.0,
            max_drawdown_pct: forward_metrics.max_drawdown * 100.0,
            win_rate: forward_metrics.win_rate,
            profit_factor: forward_metrics.profit_factor,
            trades_per_hour: if forward_duration_hours > 0.0 {
                forward_metrics.total_trades as f64 / forward_duration_hours
            } else {
                0.0
            },
            avg_slippage_bps: forward_metrics.avg_slippage_bps,
            bid_fill_rate: forward_metrics.bid_fill_rate,
            ask_fill_rate: forward_metrics.ask_fill_rate,
        };

        let diff = ComparisonDiff {
            sharpe_diff: forward.sharpe_ratio - backtest.sharpe_ratio,
            return_diff_pct: forward.total_return_pct - backtest.total_return_pct,
            drawdown_diff_pct: forward.max_drawdown_pct - backtest.max_drawdown_pct,
            win_rate_diff: forward.win_rate - backtest.win_rate,
            fill_rate_diff: ((forward.bid_fill_rate + forward.ask_fill_rate) / 2.0)
                - ((backtest.bid_fill_rate + backtest.ask_fill_rate) / 2.0),
        };

        // Generate assessment
        let assessment = Self::generate_assessment(&diff);

        Self {
            backtest,
            forward,
            diff,
            assessment,
        }
    }

    fn generate_assessment(diff: &ComparisonDiff) -> String {
        let mut issues = Vec::new();

        if diff.sharpe_diff < -0.5 {
            issues.push("Sharpe significantly worse than backtest (possible overfit)");
        }
        if diff.return_diff_pct < -5.0 {
            issues.push("Returns much lower than expected");
        }
        if diff.drawdown_diff_pct > 5.0 {
            issues.push("Drawdowns larger than backtested");
        }
        if diff.fill_rate_diff < -0.2 {
            issues.push("Fill rates much lower than simulated");
        }

        if issues.is_empty() {
            "GOOD: Forward test results align with backtest predictions".to_string()
        } else {
            format!("ISSUES: {}", issues.join("; "))
        }
    }

    /// Print comparison report
    pub fn print_report(&self) {
        println!();
        println!("════════════════════════════════════════════════════════════════");
        println!("           BACKTEST vs FORWARD TEST COMPARISON");
        println!("════════════════════════════════════════════════════════════════");
        println!();
        println!("{:<25} {:>12} {:>12} {:>12}", "Metric", "Backtest", "Forward", "Diff");
        println!("{}", "-".repeat(65));
        println!("{:<25} {:>+12.2} {:>+12.2} {:>+12.2}",
            "Sharpe Ratio",
            self.backtest.sharpe_ratio,
            self.forward.sharpe_ratio,
            self.diff.sharpe_diff);
        println!("{:<25} {:>11.2}% {:>11.2}% {:>+11.2}%",
            "Total Return",
            self.backtest.total_return_pct,
            self.forward.total_return_pct,
            self.diff.return_diff_pct);
        println!("{:<25} {:>11.2}% {:>11.2}% {:>+11.2}%",
            "Max Drawdown",
            self.backtest.max_drawdown_pct,
            self.forward.max_drawdown_pct,
            self.diff.drawdown_diff_pct);
        println!("{:<25} {:>11.1}% {:>11.1}% {:>+11.1}%",
            "Win Rate",
            self.backtest.win_rate * 100.0,
            self.forward.win_rate * 100.0,
            self.diff.win_rate_diff * 100.0);
        println!("{:<25} {:>12.2} {:>12.2}",
            "Profit Factor",
            self.backtest.profit_factor,
            self.forward.profit_factor);
        println!("{:<25} {:>12.1} {:>12.1}",
            "Trades/Hour",
            self.backtest.trades_per_hour,
            self.forward.trades_per_hour);
        println!("{:<25} {:>11.1}% {:>11.1}%",
            "Bid Fill Rate",
            self.backtest.bid_fill_rate * 100.0,
            self.forward.bid_fill_rate * 100.0);
        println!("{:<25} {:>11.1}% {:>11.1}%",
            "Ask Fill Rate",
            self.backtest.ask_fill_rate * 100.0,
            self.forward.ask_fill_rate * 100.0);
        println!("{:<25} {:>12} {:>11.2} bps",
            "Avg Slippage",
            "N/A",
            self.forward.avg_slippage_bps);
        println!();
        println!("ASSESSMENT: {}", self.assessment);
        println!("════════════════════════════════════════════════════════════════");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let config = ForwardTestConfig::default();
        let session = ForwardTestSession::new(config);
        assert!(!session.is_active());
        assert_eq!(session.metrics().total_trades, 0);
    }

    #[test]
    fn test_session_start() {
        let config = ForwardTestConfig::default();
        let mut session = ForwardTestSession::new(config);
        session.start();
        assert!(session.is_active());
        assert!(session.metrics().start_time.is_some());
    }

    #[test]
    fn test_sharpe_calculation() {
        let config = ForwardTestConfig {
            sharpe_window: 5,
            ..Default::default()
        };
        let mut session = ForwardTestSession::new(config);
        session.start();

        // Add some PnL values
        session.pnl_window.push_back(0.01);
        session.pnl_window.push_back(0.02);
        session.pnl_window.push_back(-0.01);
        session.pnl_window.push_back(0.015);
        session.pnl_window.push_back(0.005);

        let sharpe = session.calculate_sharpe();
        // Should be positive (more gains than losses)
        assert!(sharpe > 0.0);
    }

    #[test]
    fn test_session_metrics_default() {
        let metrics = SessionMetrics::default();
        assert_eq!(metrics.total_trades, 0);
        assert_eq!(metrics.net_pnl, dec!(0));
    }

    #[test]
    fn test_comparison_diff() {
        let diff = ComparisonDiff {
            sharpe_diff: -0.8,
            return_diff_pct: -10.0,
            drawdown_diff_pct: 8.0,
            win_rate_diff: -0.1,
            fill_rate_diff: -0.3,
        };

        let assessment = BacktestComparison::generate_assessment(&diff);
        assert!(assessment.contains("ISSUES"));
    }
}

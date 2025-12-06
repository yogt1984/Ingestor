//! Forward Testing Infrastructure
//!
//! Logs paper trading activity and compares against backtest predictions.
//!
//! # Algorithm Background
//!
//! This module supports the **Avellaneda-Stoikov (2008)** market making algorithm
//! with entropy-based regime detection. The strategy places bid/ask quotes around
//! a fair value, adjusting spreads based on:
//!
//! - **Inventory risk**: Skew quotes to reduce position (Avellaneda-Stoikov)
//! - **Regime detection**: Widen/pull quotes in low-entropy (trending) markets
//! - **Volatility**: Widen spreads in high-vol environments
//!
//! # Degrees of Freedom (Tunable Parameters)
//!
//! ## Market Making Parameters (`MMConfig`)
//!
//! | Parameter | Default | Range | Description |
//! |-----------|---------|-------|-------------|
//! | `base_spread_bps` | 2.0 | 0.5-10.0 | Base half-spread per side |
//! | `inventory_skew_factor` | 0.5 | 0.1-2.0 | Skew per unit inventory |
//! | `max_inventory` | 0.1 | 0.01-1.0 | Position limit (BTC) |
//! | `quote_size` | 0.001 | 0.0001-0.01 | Order size (BTC) |
//! | `risk_aversion` | 0.1 | 0.01-1.0 | A-S gamma parameter |
//! | `high_entropy_threshold` | 0.7 | 0.5-0.9 | High entropy cutoff |
//! | `low_entropy_threshold` | 0.4 | 0.2-0.6 | Low entropy cutoff |
//! | `pull_quotes_in_low_entropy` | false | bool | Full entropy gating |
//!
//! ## Fill Simulation Parameters (`FillSimulatorConfig`)
//!
//! | Parameter | Default | Range | Description |
//! |-----------|---------|-------|-------------|
//! | `base_fill_probability` | 0.10 | 0.01-0.30 | Fill rate on touch |
//! | `queue_position` | 0.5 | 0.0-1.0 | Queue position (0=front) |
//! | `adverse_selection_factor` | 0.3 | 0.0-1.0 | Post-fill adverse move |
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
//! session.log_quote(timestamp_ms, bid_price, bid_size, ask_price, ask_size, mid_price, inventory, "HighEntropy");
//!
//! // Get live metrics
//! let metrics = session.metrics();
//!
//! // End session and save
//! session.end()?;
//! ```
//!
//! # References
//!
//! - Avellaneda, M. & Stoikov, S. (2008). High-frequency trading in a limit order book
//! - Cont, R., Kukanov, A. & Stoikov, S. (2014). The price impact of order book events
//! - Moallemi, C.C. & Yuan, K. (2017). The value of queue position in a limit order book
//! - Easley, D., López de Prado, M. & O'Hara, M. (2012). Flow toxicity and liquidity

use std::path::PathBuf;
use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use crate::algorithms::AlgorithmType;
use crate::market_maker::{Fill, QuoteSide, MMState};
use crate::presets::ParameterPreset;

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
    /// Preset name being used (for comparison reports)
    #[serde(default)]
    pub preset_name: Option<String>,
    /// Algorithm type being used
    #[serde(default)]
    pub algorithm_type: AlgorithmType,
}

impl Default for ForwardTestConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("./data/sessions"),
            log_trades: true,
            log_quotes: false, // Can be very large
            sharpe_window: 100,
            session_name: None,
            preset_name: None,
            algorithm_type: AlgorithmType::AvellanedaStoikov,
        }
    }
}

impl ForwardTestConfig {
    /// Create config for a specific preset
    pub fn for_preset(preset: &ParameterPreset) -> Self {
        Self {
            log_dir: PathBuf::from("./data/sessions"),
            log_trades: true,
            log_quotes: false,
            sharpe_window: 100,
            session_name: Some(preset.name.clone()),
            preset_name: Some(preset.name.clone()),
            algorithm_type: preset.algorithm_type.clone(),
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

/// Comparison between preset expectations and live session results
///
/// This is simpler than BacktestComparison and uses the preset's stored
/// expected values rather than running a full backtest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresetComparison {
    /// Preset name
    pub preset_name: String,
    /// Algorithm type
    pub algorithm_type: AlgorithmType,
    /// Expected metrics from preset (from backtest optimization)
    pub expected: PresetExpectations,
    /// Actual metrics from live session
    pub actual: SessionMetrics,
    /// Normalized metrics for comparison (same scale)
    pub normalized: NormalizedComparison,
    /// Assessment verdict
    pub assessment: ComparisonVerdict,
}

/// Expected metrics stored in preset
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PresetExpectations {
    /// Expected return (as decimal, e.g., 0.05 = 5%)
    pub expected_return: f64,
    /// Expected Sharpe ratio
    pub expected_sharpe: f64,
    /// Expected win rate (as decimal)
    pub expected_win_rate: f64,
    /// Expected number of trades (total in backtest period)
    pub expected_trades: usize,
    /// Backtest duration in hours (for normalization)
    pub backtest_duration_hours: f64,
    /// Fill probability assumption used
    pub fill_prob_assumption: f64,
}

/// Normalized comparison (per-hour metrics for fair comparison)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NormalizedComparison {
    /// Expected trades per hour
    pub expected_trades_per_hour: f64,
    /// Actual trades per hour
    pub actual_trades_per_hour: f64,
    /// Trade rate ratio (actual / expected)
    pub trade_rate_ratio: f64,
    /// Win rate difference (actual - expected)
    pub win_rate_diff: f64,
    /// Sharpe difference (actual - expected)
    pub sharpe_diff: f64,
    /// Fill rate vs assumption
    pub fill_rate_vs_assumption: f64,
}

/// Assessment verdict for the comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonVerdict {
    /// Overall status
    pub status: VerdictStatus,
    /// Summary message
    pub summary: String,
    /// Detailed issues (if any)
    pub issues: Vec<String>,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Verdict status levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VerdictStatus {
    /// Results match or exceed expectations
    Good,
    /// Minor discrepancies
    Warning,
    /// Significant underperformance
    Poor,
    /// Insufficient data for comparison
    InsufficientData,
}

impl PresetComparison {
    /// Create a comparison from preset and session metrics
    pub fn new(preset: &ParameterPreset, session_metrics: &SessionMetrics) -> Self {
        // Estimate backtest duration from number of events
        // Assuming ~1500 events per hour on average
        let estimated_backtest_hours = preset.num_events as f64 / 1500.0;

        let expected = PresetExpectations {
            expected_return: preset.expected_return,
            expected_sharpe: preset.expected_sharpe,
            expected_win_rate: preset.expected_win_rate,
            expected_trades: preset.expected_trades,
            backtest_duration_hours: estimated_backtest_hours.max(1.0),
            fill_prob_assumption: preset.fill_prob_assumption,
        };

        let session_hours = session_metrics.duration_secs / 3600.0;

        let normalized = Self::calculate_normalized(&expected, session_metrics, session_hours);
        let assessment = Self::generate_verdict(&expected, session_metrics, &normalized, session_hours);

        Self {
            preset_name: preset.name.clone(),
            algorithm_type: preset.algorithm_type.clone(),
            expected,
            actual: session_metrics.clone(),
            normalized,
            assessment,
        }
    }

    fn calculate_normalized(
        expected: &PresetExpectations,
        actual: &SessionMetrics,
        session_hours: f64,
    ) -> NormalizedComparison {
        let expected_trades_per_hour = if expected.backtest_duration_hours > 0.0 {
            expected.expected_trades as f64 / expected.backtest_duration_hours
        } else {
            0.0
        };

        let actual_trades_per_hour = if session_hours > 0.0 {
            actual.total_trades as f64 / session_hours
        } else {
            0.0
        };

        let trade_rate_ratio = if expected_trades_per_hour > 0.0 {
            actual_trades_per_hour / expected_trades_per_hour
        } else {
            0.0
        };

        // Calculate actual fill rate from session
        let actual_fill_rate = if actual.quotes_generated > 0 {
            actual.total_trades as f64 / actual.quotes_generated as f64
        } else {
            0.0
        };

        NormalizedComparison {
            expected_trades_per_hour,
            actual_trades_per_hour,
            trade_rate_ratio,
            win_rate_diff: actual.win_rate - expected.expected_win_rate,
            sharpe_diff: actual.sharpe_ratio - expected.expected_sharpe,
            fill_rate_vs_assumption: actual_fill_rate - expected.fill_prob_assumption,
        }
    }

    fn generate_verdict(
        expected: &PresetExpectations,
        actual: &SessionMetrics,
        normalized: &NormalizedComparison,
        session_hours: f64,
    ) -> ComparisonVerdict {
        let mut issues = Vec::new();
        let mut recommendations = Vec::new();

        // Check if we have enough data
        if session_hours < 0.5 || actual.total_trades < 5 {
            return ComparisonVerdict {
                status: VerdictStatus::InsufficientData,
                summary: "Insufficient data for meaningful comparison".to_string(),
                issues: vec![format!(
                    "Session duration: {:.1} hours, trades: {}. Need at least 0.5 hours and 5 trades.",
                    session_hours, actual.total_trades
                )],
                recommendations: vec!["Continue running the session to collect more data.".to_string()],
            };
        }

        // Check trade rate
        if normalized.trade_rate_ratio < 0.5 {
            issues.push(format!(
                "Trade rate {:.1}x lower than expected ({:.1}/hr vs {:.1}/hr expected)",
                1.0 / normalized.trade_rate_ratio.max(0.01),
                normalized.actual_trades_per_hour,
                normalized.expected_trades_per_hour
            ));
            recommendations.push("Fill rate may be lower than assumed in backtest.".to_string());
        } else if normalized.trade_rate_ratio > 2.0 {
            issues.push(format!(
                "Trade rate {:.1}x higher than expected - verify fill simulation",
                normalized.trade_rate_ratio
            ));
        }

        // Check win rate
        if normalized.win_rate_diff < -0.1 {
            issues.push(format!(
                "Win rate {:.1}% vs {:.1}% expected ({:+.1}% diff)",
                actual.win_rate * 100.0,
                expected.expected_win_rate * 100.0,
                normalized.win_rate_diff * 100.0
            ));
            recommendations.push("Market conditions may differ from backtest period.".to_string());
        }

        // Check Sharpe (only if we have meaningful data)
        if actual.total_trades >= 20 && normalized.sharpe_diff < -1.0 {
            issues.push(format!(
                "Sharpe {:.2} vs {:.2} expected ({:+.2} diff)",
                actual.sharpe_ratio,
                expected.expected_sharpe,
                normalized.sharpe_diff
            ));
            recommendations.push("Consider reviewing market regime detection.".to_string());
        }

        // Check fill rate vs assumption
        if normalized.fill_rate_vs_assumption < -0.05 {
            issues.push(format!(
                "Fill rate {:.1}% below assumption ({:.1}%)",
                -normalized.fill_rate_vs_assumption * 100.0,
                expected.fill_prob_assumption * 100.0
            ));
            recommendations.push("Backtest may be too optimistic about fills.".to_string());
        }

        // Determine overall status
        let status = if issues.is_empty() {
            VerdictStatus::Good
        } else if issues.len() <= 2 && !issues.iter().any(|i| i.contains("lower than expected")) {
            VerdictStatus::Warning
        } else {
            VerdictStatus::Poor
        };

        let summary = match status {
            VerdictStatus::Good => "Live results align with backtest expectations.".to_string(),
            VerdictStatus::Warning => format!("Minor discrepancies detected: {} issue(s).", issues.len()),
            VerdictStatus::Poor => format!("Significant underperformance: {} issue(s).", issues.len()),
            VerdictStatus::InsufficientData => "Need more data.".to_string(),
        };

        if recommendations.is_empty() && status == VerdictStatus::Good {
            recommendations.push("Continue monitoring. Consider extending session duration.".to_string());
        }

        ComparisonVerdict {
            status,
            summary,
            issues,
            recommendations,
        }
    }

    /// Print a formatted comparison report
    pub fn print_report(&self) {
        let algo_label = match self.algorithm_type {
            AlgorithmType::AvellanedaStoikov => "A-S",
            AlgorithmType::MLSpreadSkew => "ML",
        };

        println!();
        println!("════════════════════════════════════════════════════════════════");
        println!("       PRESET vs LIVE SESSION COMPARISON");
        println!("════════════════════════════════════════════════════════════════");
        println!();
        println!("Preset: {} [{}]", self.preset_name, algo_label);
        println!("Session Duration: {:.1} hours", self.actual.duration_secs / 3600.0);
        println!();

        println!("{:<25} {:>15} {:>15}", "Metric", "Expected", "Actual");
        println!("{}", "-".repeat(57));

        println!("{:<25} {:>14.2}% {:>14.2}%",
            "Return",
            self.expected.expected_return * 100.0,
            self.actual.net_pnl.to_f64().unwrap_or(0.0) * 100.0);

        println!("{:<25} {:>15.2} {:>15.2}",
            "Sharpe Ratio",
            self.expected.expected_sharpe,
            self.actual.sharpe_ratio);

        println!("{:<25} {:>14.1}% {:>14.1}%",
            "Win Rate",
            self.expected.expected_win_rate * 100.0,
            self.actual.win_rate * 100.0);

        println!("{:<25} {:>15.1} {:>15.1}",
            "Trades/Hour",
            self.normalized.expected_trades_per_hour,
            self.normalized.actual_trades_per_hour);

        println!("{:<25} {:>14.1}% {:>14.1}%",
            "Fill Rate (assumption)",
            self.expected.fill_prob_assumption * 100.0,
            if self.actual.quotes_generated > 0 {
                self.actual.total_trades as f64 / self.actual.quotes_generated as f64 * 100.0
            } else {
                0.0
            });

        println!();

        // Status with color indication
        let status_str = match self.assessment.status {
            VerdictStatus::Good => "GOOD",
            VerdictStatus::Warning => "WARNING",
            VerdictStatus::Poor => "POOR",
            VerdictStatus::InsufficientData => "INSUFFICIENT DATA",
        };

        println!("STATUS: {} - {}", status_str, self.assessment.summary);

        if !self.assessment.issues.is_empty() {
            println!();
            println!("Issues:");
            for issue in &self.assessment.issues {
                println!("  - {}", issue);
            }
        }

        if !self.assessment.recommendations.is_empty() {
            println!();
            println!("Recommendations:");
            for rec in &self.assessment.recommendations {
                println!("  - {}", rec);
            }
        }

        println!("════════════════════════════════════════════════════════════════");
    }

    /// Save comparison to JSON file
    pub fn save(&self, path: &std::path::Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

/// Load a session summary from file
pub fn load_session_summary(path: &std::path::Path) -> Result<SessionSummary> {
    let content = std::fs::read_to_string(path)?;
    let summary: SessionSummary = serde_json::from_str(&content)?;
    Ok(summary)
}

/// List all session summaries in a directory
pub fn list_sessions(dir: &std::path::Path) -> Result<Vec<SessionSummary>> {
    let mut sessions = Vec::new();

    if !dir.exists() {
        return Ok(sessions);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("summary_") && name.ends_with(".json") {
                    if let Ok(summary) = load_session_summary(&path) {
                        sessions.push(summary);
                    }
                }
            }
        }
    }

    // Sort by session ID (which is timestamp-based)
    sessions.sort_by(|a, b| b.session_id.cmp(&a.session_id));

    Ok(sessions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithms::AlgorithmType;

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

    #[test]
    fn test_config_for_preset() {
        let mut preset = ParameterPreset::new("TestPreset", "manual", 2.0, 0.5, 0.7, 0.10);
        preset.algorithm_type = AlgorithmType::MLSpreadSkew;

        let config = ForwardTestConfig::for_preset(&preset);

        assert_eq!(config.preset_name, Some("TestPreset".to_string()));
        assert_eq!(config.algorithm_type, AlgorithmType::MLSpreadSkew);
        assert_eq!(config.session_name, Some("TestPreset".to_string()));
    }

    #[test]
    fn test_preset_comparison_insufficient_data() {
        let preset = ParameterPreset::new("Test", "manual", 2.0, 0.5, 0.7, 0.10);

        // Session with very little data
        let metrics = SessionMetrics {
            duration_secs: 60.0, // 1 minute
            total_trades: 2,
            ..Default::default()
        };

        let comparison = PresetComparison::new(&preset, &metrics);

        assert_eq!(comparison.assessment.status, VerdictStatus::InsufficientData);
        assert!(comparison.assessment.summary.contains("Insufficient"));
    }

    #[test]
    fn test_preset_comparison_good_performance() {
        let mut preset = ParameterPreset::new("Test", "manual", 2.0, 0.5, 0.7, 0.10);
        preset.expected_return = 0.05;
        preset.expected_sharpe = -1.0;
        preset.expected_win_rate = 0.55;
        preset.expected_trades = 100;
        preset.num_events = 75000; // ~50 hours of data

        // Session with good performance matching expectations
        // Trade rate: 100 trades / 50 hours = 2 trades/hour expected
        // We need ~4 trades in 2 hours to match expectations
        let metrics = SessionMetrics {
            duration_secs: 3600.0 * 2.0, // 2 hours
            total_trades: 6,             // ~3 trades/hour, within 2x of expected
            winning_trades: 4,
            losing_trades: 2,
            win_rate: 0.60,
            sharpe_ratio: -0.8,
            quotes_generated: 60,        // 10% fill rate matches assumption
            ..Default::default()
        };

        let comparison = PresetComparison::new(&preset, &metrics);

        // Should be Good or Warning (not Poor or InsufficientData)
        assert!(comparison.assessment.status == VerdictStatus::Good
            || comparison.assessment.status == VerdictStatus::Warning);
    }

    #[test]
    fn test_preset_comparison_poor_performance() {
        let mut preset = ParameterPreset::new("Test", "manual", 2.0, 0.5, 0.7, 0.10);
        preset.expected_return = 0.05;
        preset.expected_sharpe = 1.5;
        preset.expected_win_rate = 0.65;
        preset.expected_trades = 200;
        preset.num_events = 75000;

        // Session with much worse performance
        let metrics = SessionMetrics {
            duration_secs: 3600.0 * 2.0,
            total_trades: 20,
            winning_trades: 8,
            losing_trades: 12,
            win_rate: 0.40, // Much worse than expected 0.65
            sharpe_ratio: -2.0,
            quotes_generated: 500,
            ..Default::default()
        };

        let comparison = PresetComparison::new(&preset, &metrics);

        // Should detect issues
        assert!(!comparison.assessment.issues.is_empty());
        assert!(comparison.assessment.status == VerdictStatus::Warning
            || comparison.assessment.status == VerdictStatus::Poor);
    }

    #[test]
    fn test_normalized_comparison_trade_rate() {
        let expected = PresetExpectations {
            expected_trades: 100,
            backtest_duration_hours: 50.0, // 2 trades/hour expected
            ..Default::default()
        };

        let actual = SessionMetrics {
            total_trades: 20,
            duration_secs: 3600.0 * 5.0, // 5 hours -> 4 trades/hour
            ..Default::default()
        };

        let normalized = PresetComparison::calculate_normalized(&expected, &actual, 5.0);

        assert!((normalized.expected_trades_per_hour - 2.0).abs() < 0.01);
        assert!((normalized.actual_trades_per_hour - 4.0).abs() < 0.01);
        assert!((normalized.trade_rate_ratio - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_verdict_status_serialization() {
        let status = VerdictStatus::Good;
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("Good"));

        let deserialized: VerdictStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, VerdictStatus::Good);
    }

    #[test]
    fn test_quote_logging() {
        let mut config = ForwardTestConfig::default();
        config.log_quotes = true;

        let mut session = ForwardTestSession::new(config);
        session.start();

        session.log_quote(
            1000,
            Some(dec!(50000)),
            Some(dec!(0.1)),
            Some(dec!(50010)),
            Some(dec!(0.1)),
            dec!(50005),
            dec!(0.05),
            "HighEntropy",
        );

        assert_eq!(session.metrics().quotes_generated, 1);
        assert_eq!(session.quotes.len(), 1);
        assert_eq!(session.quotes[0].regime, "HighEntropy");
    }

    #[test]
    fn test_touch_recording() {
        let config = ForwardTestConfig::default();
        let mut session = ForwardTestSession::new(config);
        session.start();

        // Simulate some touches
        session.record_touch(true); // bid touch
        session.record_touch(true); // bid touch
        session.record_touch(false); // ask touch

        assert_eq!(session.metrics().bid_touches, 2);
        assert_eq!(session.metrics().ask_touches, 1);
    }

    #[test]
    fn test_session_id_format() {
        let config = ForwardTestConfig::default();
        let session = ForwardTestSession::new(config);

        // Session ID should be in YYYYMMDD_HHMMSS format
        let id = session.session_id();
        assert_eq!(id.len(), 15); // YYYYMMDD_HHMMSS
        assert!(id.contains('_'));
    }

    #[test]
    fn test_preset_expectations_default() {
        let expectations = PresetExpectations::default();
        assert_eq!(expectations.expected_return, 0.0);
        assert_eq!(expectations.expected_trades, 0);
    }

    #[test]
    fn test_comparison_verdict_creation() {
        let verdict = ComparisonVerdict {
            status: VerdictStatus::Warning,
            summary: "Test warning".to_string(),
            issues: vec!["Issue 1".to_string()],
            recommendations: vec!["Fix it".to_string()],
        };

        assert_eq!(verdict.status, VerdictStatus::Warning);
        assert_eq!(verdict.issues.len(), 1);
        assert_eq!(verdict.recommendations.len(), 1);
    }
}

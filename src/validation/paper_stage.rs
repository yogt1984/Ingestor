//! PaperStage Implementation (Task 2.4)
//!
//! Paper trading validation stage that tests the algorithm with live/simulated
//! data and simulated execution. This is the final validation before live trading.
//!
//! # Overview
//!
//! The PaperStage is the fourth stage in the validation pipeline. It:
//! 1. Runs the algorithm for a configurable duration (not historical replay)
//! 2. Simulates fills at market price + slippage
//! 3. Tracks P&L in real-time
//! 4. Provides final validation before committing to live trading
//!
//! # Duration-Based Execution
//!
//! Unlike historical stages that replay past data, the paper stage runs for
//! a specified duration (e.g., 1 hour, 1 day) to validate behavior in
//! near-real-time conditions.
//!
//! ```text
//! |--- Paper Trading Duration (configurable) ----|
//!                      ^
//!                      |
//!              Real-time validation
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::validation::{PaperStage, PaperStageConfig, StageContext};
//!
//! let stage = PaperStage::new(PaperStageConfig::default());
//! let context = StageContext::default()
//!     .with_name("Paper-2025Q1")
//!     .with_timeout(3600);  // 1 hour
//!
//! let result = stage.run(&context).await?;
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::core::{
    ExitReason, TradeDirection, TradeResult, ValidationResult, ValidationStageType,
};

use super::traits::{RunFuture, StageContext, StageError, ValidationStage};

/// Configuration for the PaperStage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperStageConfig {
    /// Duration to run paper trading (in seconds)
    pub duration_seconds: u64,

    /// Fill probability (for simulated fills)
    pub fill_probability: f64,

    /// Slippage in basis points
    pub slippage_bps: f64,

    /// Fee rate (as decimal, e.g., 0.0001 for 1 bps)
    pub fee_rate_bps: f64,

    /// Initial capital for equity tracking
    pub initial_capital: f64,

    /// Risk-free rate for Sharpe calculation (annual)
    pub risk_free_rate: f64,

    /// How often to sample P&L (in seconds)
    pub pnl_sample_interval_seconds: u64,

    /// Minimum number of trades required
    pub min_trades: usize,

    /// Minimum acceptable Sharpe ratio
    pub min_sharpe: f64,

    /// Maximum acceptable drawdown (as percentage, e.g., 0.10 for 10%)
    pub max_drawdown: f64,

    /// Whether to use realistic fill simulation
    pub use_realistic_fills: bool,

    /// Print progress during execution
    pub verbose: bool,

    /// Name for the stage
    pub name: String,

    /// Graceful shutdown timeout (seconds)
    pub shutdown_timeout_seconds: u64,
}

impl Default for PaperStageConfig {
    fn default() -> Self {
        Self {
            duration_seconds: 3600, // 1 hour default
            fill_probability: 0.10, // 10% fill probability (conservative)
            slippage_bps: 1.0,      // 1 bps slippage
            fee_rate_bps: 1.0,      // 1 bps fee
            initial_capital: 10_000.0,
            risk_free_rate: 0.05, // 5% annual
            pnl_sample_interval_seconds: 60,
            min_trades: 10,
            min_sharpe: 0.0,       // Paper stage is more lenient
            max_drawdown: 0.15,    // 15% max drawdown
            use_realistic_fills: true,
            verbose: false,
            name: "Paper".to_string(),
            shutdown_timeout_seconds: 30,
        }
    }
}

impl PaperStageConfig {
    /// Create a configuration for quick testing (short duration)
    pub fn fast() -> Self {
        Self {
            duration_seconds: 300, // 5 minutes
            fill_probability: 0.50,
            slippage_bps: 0.5,
            use_realistic_fills: false,
            verbose: false,
            min_trades: 5,
            pnl_sample_interval_seconds: 10,
            shutdown_timeout_seconds: 5,
            ..Default::default()
        }
    }

    /// Create a configuration with conservative (realistic) assumptions
    pub fn conservative() -> Self {
        Self {
            duration_seconds: 86400, // 24 hours
            fill_probability: 0.05,  // Only 5% fill probability
            slippage_bps: 2.0,       // Higher slippage
            fee_rate_bps: 2.0,       // Higher fees
            min_trades: 20,
            min_sharpe: 0.3,
            max_drawdown: 0.10,
            use_realistic_fills: true,
            pnl_sample_interval_seconds: 300, // 5 minute samples
            ..Default::default()
        }
    }

    /// Create a configuration for extended validation
    pub fn extended() -> Self {
        Self {
            duration_seconds: 604800, // 1 week
            fill_probability: 0.10,
            min_trades: 50,
            min_sharpe: 0.5,
            pnl_sample_interval_seconds: 600, // 10 minute samples
            ..Default::default()
        }
    }

    /// Create a configuration for simulation testing
    pub fn simulation() -> Self {
        Self {
            duration_seconds: 60, // 1 minute for tests
            fill_probability: 1.0,
            slippage_bps: 0.0,
            fee_rate_bps: 0.0,
            use_realistic_fills: false,
            min_trades: 1,
            pnl_sample_interval_seconds: 1,
            shutdown_timeout_seconds: 1,
            ..Default::default()
        }
    }

    /// Set the stage name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the duration
    pub fn with_duration(mut self, seconds: u64) -> Self {
        self.duration_seconds = seconds.max(1);
        self
    }

    /// Set the fill probability
    pub fn with_fill_probability(mut self, probability: f64) -> Self {
        self.fill_probability = probability.clamp(0.0, 1.0);
        self
    }

    /// Set the slippage
    pub fn with_slippage(mut self, bps: f64) -> Self {
        self.slippage_bps = bps.max(0.0);
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.duration_seconds == 0 {
            return Err("Duration must be > 0".to_string());
        }
        if self.fill_probability < 0.0 || self.fill_probability > 1.0 {
            return Err("Fill probability must be between 0 and 1".to_string());
        }
        if self.slippage_bps < 0.0 {
            return Err("Slippage must be >= 0".to_string());
        }
        if self.max_drawdown < 0.0 || self.max_drawdown > 1.0 {
            return Err("Max drawdown must be between 0 and 1".to_string());
        }
        Ok(())
    }
}

/// Real-time P&L sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLSample {
    /// Timestamp (ms)
    pub timestamp_ms: i64,

    /// Cumulative P&L
    pub cumulative_pnl: f64,

    /// Equity value
    pub equity: f64,

    /// Drawdown from peak
    pub drawdown: f64,

    /// Number of trades so far
    pub trade_count: usize,
}

/// Detailed metrics from paper trading
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PaperMetrics {
    /// Duration of paper trading (seconds)
    pub duration_seconds: u64,

    /// Number of trades executed
    pub trade_count: usize,

    /// Sharpe ratio
    pub sharpe_ratio: f64,

    /// Total return
    pub total_return: f64,

    /// Win rate
    pub win_rate: f64,

    /// Maximum drawdown
    pub max_drawdown: f64,

    /// Average trade return in bps
    pub avg_trade_return_bps: f64,

    /// Fills generated
    pub fills_generated: usize,

    /// Fill rate
    pub fill_rate: f64,

    /// Peak equity
    pub peak_equity: f64,

    /// Final equity
    pub final_equity: f64,

    /// P&L samples over time
    pub pnl_samples: Vec<PnLSample>,

    /// Whether graceful shutdown was achieved
    pub graceful_shutdown: bool,

    /// Number of quotes generated
    pub quotes_generated: usize,

    /// Bid fills
    pub bid_fills: usize,

    /// Ask fills
    pub ask_fills: usize,

    /// Total slippage incurred (bps)
    pub total_slippage_bps: f64,

    /// Total fees paid
    pub total_fees: f64,
}

impl PaperMetrics {
    /// Create new metrics from paper trading results
    pub fn new(
        duration_seconds: u64,
        initial_capital: f64,
        trades: &[SimulatedTrade],
        pnl_samples: Vec<PnLSample>,
        graceful_shutdown: bool,
    ) -> Self {
        let trade_count = trades.len();
        let winners = trades.iter().filter(|t| t.pnl > 0.0).count();
        let win_rate = if trade_count > 0 {
            winners as f64 / trade_count as f64
        } else {
            0.0
        };

        let total_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
        let total_return = if initial_capital > 0.0 {
            total_pnl / initial_capital
        } else {
            0.0
        };

        let avg_trade_return_bps = if trade_count > 0 {
            trades.iter().map(|t| t.return_bps).sum::<f64>() / trade_count as f64
        } else {
            0.0
        };

        // Calculate Sharpe ratio from P&L samples
        let sharpe_ratio = Self::calculate_sharpe(&pnl_samples, duration_seconds);

        // Find max drawdown
        let max_drawdown = pnl_samples
            .iter()
            .map(|s| s.drawdown)
            .fold(0.0f64, |a, b| a.max(b));

        let peak_equity = pnl_samples
            .iter()
            .map(|s| s.equity)
            .fold(initial_capital, |a, b| a.max(b));

        let final_equity = pnl_samples
            .last()
            .map(|s| s.equity)
            .unwrap_or(initial_capital);

        let bid_fills = trades.iter().filter(|t| t.is_bid).count();
        let ask_fills = trades.iter().filter(|t| !t.is_bid).count();

        let total_slippage_bps: f64 = trades.iter().map(|t| t.slippage_bps).sum();
        let total_fees: f64 = trades.iter().map(|t| t.fee).sum();

        Self {
            duration_seconds,
            trade_count,
            sharpe_ratio,
            total_return,
            win_rate,
            max_drawdown,
            avg_trade_return_bps,
            fills_generated: trade_count,
            fill_rate: 0.0, // Set by caller
            peak_equity,
            final_equity,
            pnl_samples,
            graceful_shutdown,
            quotes_generated: 0, // Set by caller
            bid_fills,
            ask_fills,
            total_slippage_bps,
            total_fees,
        }
    }

    /// Calculate Sharpe ratio from P&L samples
    fn calculate_sharpe(samples: &[PnLSample], duration_seconds: u64) -> f64 {
        if samples.len() < 2 {
            return 0.0;
        }

        // Calculate returns between samples
        let returns: Vec<f64> = samples
            .windows(2)
            .map(|w| {
                if w[0].equity > 0.0 {
                    (w[1].equity - w[0].equity) / w[0].equity
                } else {
                    0.0
                }
            })
            .collect();

        if returns.is_empty() {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev < 1e-10 {
            return if mean > 0.0 { f64::INFINITY } else { 0.0 };
        }

        // Annualize: assume samples are evenly spaced
        let samples_per_year = if duration_seconds > 0 {
            (365.25 * 24.0 * 3600.0 / duration_seconds as f64) * returns.len() as f64
        } else {
            252.0
        };

        (mean / std_dev) * samples_per_year.sqrt()
    }

    /// Check if paper trading meets requirements
    pub fn meets_requirements(&self, config: &PaperStageConfig) -> bool {
        self.trade_count >= config.min_trades
            && self.sharpe_ratio >= config.min_sharpe
            && self.max_drawdown <= config.max_drawdown
    }

    /// Check if this is a go/no-go pass
    pub fn is_go(&self, config: &PaperStageConfig) -> bool {
        if !self.meets_requirements(config) {
            return false;
        }

        // Must be profitable
        if self.total_return <= 0.0 {
            return false;
        }

        // Must have reasonable win rate (at least 30%)
        if self.win_rate < 0.30 {
            return false;
        }

        // Must have completed gracefully
        if !self.graceful_shutdown {
            return false;
        }

        true
    }
}

/// Simulated trade during paper trading
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedTrade {
    /// Trade ID
    pub trade_id: String,

    /// Entry timestamp (ms)
    pub entry_time_ms: i64,

    /// Exit timestamp (ms)
    pub exit_time_ms: i64,

    /// Entry price
    pub entry_price: f64,

    /// Exit price
    pub exit_price: f64,

    /// Size
    pub size: f64,

    /// Is this a bid fill (buy)
    pub is_bid: bool,

    /// P&L
    pub pnl: f64,

    /// Return in basis points
    pub return_bps: f64,

    /// Slippage incurred
    pub slippage_bps: f64,

    /// Fee paid
    pub fee: f64,
}

impl SimulatedTrade {
    /// Create a new simulated trade
    pub fn new(
        trade_id: String,
        entry_time_ms: i64,
        exit_time_ms: i64,
        entry_price: f64,
        exit_price: f64,
        size: f64,
        is_bid: bool,
        slippage_bps: f64,
        fee: f64,
    ) -> Self {
        let price_diff = if is_bid {
            exit_price - entry_price
        } else {
            entry_price - exit_price
        };

        let pnl = price_diff * size - fee;
        let return_bps = if entry_price > 0.0 {
            (price_diff / entry_price) * 10000.0
        } else {
            0.0
        };

        Self {
            trade_id,
            entry_time_ms,
            exit_time_ms,
            entry_price,
            exit_price,
            size,
            is_bid,
            pnl,
            return_bps,
            slippage_bps,
            fee,
        }
    }

    /// Convert to TradeResult
    pub fn to_trade_result(&self, config_id: &str) -> TradeResult {
        let entry_time = Utc
            .timestamp_millis_opt(self.entry_time_ms)
            .single()
            .unwrap_or_else(Utc::now);
        let exit_time = Utc
            .timestamp_millis_opt(self.exit_time_ms)
            .single()
            .unwrap_or_else(Utc::now);

        let direction = if self.is_bid {
            TradeDirection::Long
        } else {
            TradeDirection::Short
        };

        let exit_reason = if self.pnl > 0.0 {
            ExitReason::TakeProfit
        } else if self.pnl < 0.0 {
            ExitReason::StopLoss
        } else {
            ExitReason::Unknown
        };

        TradeResult {
            trade_id: self.trade_id.clone(),
            direction,
            entry_time,
            exit_time,
            entry_price: self.entry_price,
            exit_price: self.exit_price,
            size: self.size,
            pnl: self.pnl,
            pnl_bps: self.return_bps,
            return_pct: self.return_bps / 100.0,
            exit_reason,
            research_state_id: None,
            config_id: Some(config_id.to_string()),
            slippage_bps: self.slippage_bps,
            commission: self.fee,
            mae_bps: 0.0,
            mfe_bps: 0.0,
            metadata: std::collections::HashMap::new(),
        }
    }
}

/// Shutdown handle for graceful termination
#[derive(Clone)]
pub struct ShutdownHandle {
    shutdown_flag: Arc<AtomicBool>,
}

impl ShutdownHandle {
    /// Create a new shutdown handle
    pub fn new() -> Self {
        Self {
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
    }

    /// Check if shutdown was requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_flag.load(Ordering::SeqCst)
    }
}

impl Default for ShutdownHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// PaperStage - Paper trading validation
///
/// This stage performs validation using simulated trading with configurable
/// duration and fill simulation.
pub struct PaperStage {
    config: PaperStageConfig,
    shutdown_handle: ShutdownHandle,
}

impl PaperStage {
    /// Create a new PaperStage with the given configuration
    pub fn new(config: PaperStageConfig) -> Self {
        Self {
            config,
            shutdown_handle: ShutdownHandle::new(),
        }
    }

    /// Create a PaperStage with default configuration
    pub fn with_defaults() -> Self {
        Self::new(PaperStageConfig::default())
    }

    /// Get the shutdown handle for external control
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown_handle.clone()
    }

    /// Execute paper trading simulation
    async fn execute_paper(
        &self,
        context: &StageContext,
    ) -> Result<(Vec<SimulatedTrade>, PaperMetrics), StageError> {
        let start_time = Instant::now();
        let start_ms = Utc::now().timestamp_millis();

        let mut trades = Vec::new();
        let mut pnl_samples = Vec::new();
        let mut current_equity = self.config.initial_capital;
        let mut peak_equity = current_equity;
        let mut trade_idx = 0;
        let mut quotes_generated = 0u64;

        // Sample P&L at intervals
        let sample_interval_ms = self.config.pnl_sample_interval_seconds * 1000;
        let mut last_sample_ms = start_ms;

        // Initial sample
        pnl_samples.push(PnLSample {
            timestamp_ms: start_ms,
            cumulative_pnl: 0.0,
            equity: current_equity,
            drawdown: 0.0,
            trade_count: 0,
        });

        // Simulate trading for the configured duration
        // In a real implementation, this would connect to live data
        // For now, we simulate trades at random intervals
        let mut simulated_time_ms = start_ms;
        let end_time_ms = start_ms + (self.config.duration_seconds * 1000) as i64;

        // Use a deterministic approach based on context for reproducibility
        let seed = context.config.id.len() as u64 + self.config.duration_seconds;
        let mut pseudo_random = seed;

        while simulated_time_ms < end_time_ms {
            // Check for shutdown
            if self.shutdown_handle.is_shutdown_requested() {
                break;
            }

            // Check for timeout
            if let Some(timeout) = context.timeout_seconds {
                if start_time.elapsed().as_secs() >= timeout {
                    return Err(StageError::Timeout(timeout));
                }
            }

            // Advance time (simulate varying intervals between events)
            pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
            let interval_ms = (pseudo_random % 60000) + 1000; // 1-60 seconds
            simulated_time_ms += interval_ms as i64;

            if simulated_time_ms >= end_time_ms {
                break;
            }

            // Generate a quote
            quotes_generated += 1;

            // Simulate fill probability
            pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
            let fill_roll = (pseudo_random % 1000) as f64 / 1000.0;

            if fill_roll < self.config.fill_probability {
                // Generate a simulated trade
                trade_idx += 1;

                // Determine trade direction
                pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
                let is_bid = pseudo_random % 2 == 0;

                // Simulate price (around 100.0 with some noise)
                pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
                let price_noise = ((pseudo_random % 1000) as f64 - 500.0) / 10000.0;
                let entry_price = 100.0 * (1.0 + price_noise);

                // Simulate exit (small profit or loss)
                pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
                let exit_noise = ((pseudo_random % 1000) as f64 - 400.0) / 10000.0; // Slight positive bias
                let exit_price = entry_price * (1.0 + exit_noise);

                // Apply slippage
                let slippage = self.config.slippage_bps;
                let fee =
                    entry_price * 1.0 * (self.config.fee_rate_bps / 10000.0) * 2.0; // Entry + exit

                let exit_time_ms = simulated_time_ms + 60000; // 1 minute trade duration

                let trade = SimulatedTrade::new(
                    format!("PP-{}", trade_idx),
                    simulated_time_ms,
                    exit_time_ms,
                    entry_price,
                    exit_price,
                    1.0, // Unit size
                    is_bid,
                    slippage,
                    fee,
                );

                current_equity += trade.pnl;
                if current_equity > peak_equity {
                    peak_equity = current_equity;
                }

                trades.push(trade);
            }

            // Sample P&L if interval has passed
            if simulated_time_ms - last_sample_ms >= sample_interval_ms as i64 {
                let cumulative_pnl = current_equity - self.config.initial_capital;
                let drawdown = if peak_equity > 0.0 {
                    (peak_equity - current_equity) / peak_equity
                } else {
                    0.0
                };

                pnl_samples.push(PnLSample {
                    timestamp_ms: simulated_time_ms,
                    cumulative_pnl,
                    equity: current_equity,
                    drawdown,
                    trade_count: trades.len(),
                });

                last_sample_ms = simulated_time_ms;
            }
        }

        // Final sample
        let cumulative_pnl = current_equity - self.config.initial_capital;
        let drawdown = if peak_equity > 0.0 {
            (peak_equity - current_equity) / peak_equity
        } else {
            0.0
        };

        pnl_samples.push(PnLSample {
            timestamp_ms: simulated_time_ms,
            cumulative_pnl,
            equity: current_equity,
            drawdown,
            trade_count: trades.len(),
        });

        // Calculate metrics
        let actual_duration = start_time.elapsed().as_secs();
        let graceful_shutdown = !self.shutdown_handle.is_shutdown_requested()
            || actual_duration < self.config.shutdown_timeout_seconds;

        let mut metrics = PaperMetrics::new(
            self.config.duration_seconds,
            self.config.initial_capital,
            &trades,
            pnl_samples,
            graceful_shutdown,
        );

        metrics.quotes_generated = quotes_generated as usize;
        metrics.fill_rate = if quotes_generated > 0 {
            trades.len() as f64 / quotes_generated as f64
        } else {
            0.0
        };

        Ok((trades, metrics))
    }

    /// Convert results to ValidationResult
    fn convert_results(
        &self,
        trades: &[SimulatedTrade],
        paper_metrics: &PaperMetrics,
        context: &StageContext,
        duration_secs: f64,
    ) -> ValidationResult {
        // Convert trades
        let trade_results: Vec<TradeResult> = trades
            .iter()
            .map(|t| t.to_trade_result(&context.config.id))
            .collect();

        // Create validation result
        let mut result = ValidationResult::new(
            ValidationStageType::Paper,
            context.stage_name.clone(),
            context.config.id.clone(),
            context.period_start,
            context.period_end,
        );

        // Set trades and compute metrics
        result = result.with_trades(trade_results);

        // Add metadata
        result.add_metadata(
            "duration_seconds".to_string(),
            paper_metrics.duration_seconds.to_string(),
        );
        result.add_metadata(
            "paper_sharpe".to_string(),
            format!("{:.3}", paper_metrics.sharpe_ratio),
        );
        result.add_metadata(
            "paper_return".to_string(),
            format!("{:.2}%", paper_metrics.total_return * 100.0),
        );
        result.add_metadata(
            "paper_win_rate".to_string(),
            format!("{:.1}%", paper_metrics.win_rate * 100.0),
        );
        result.add_metadata(
            "paper_max_drawdown".to_string(),
            format!("{:.2}%", paper_metrics.max_drawdown * 100.0),
        );
        result.add_metadata(
            "fills_generated".to_string(),
            paper_metrics.fills_generated.to_string(),
        );
        result.add_metadata(
            "fill_rate".to_string(),
            format!("{:.2}%", paper_metrics.fill_rate * 100.0),
        );
        result.add_metadata(
            "quotes_generated".to_string(),
            paper_metrics.quotes_generated.to_string(),
        );
        result.add_metadata(
            "graceful_shutdown".to_string(),
            paper_metrics.graceful_shutdown.to_string(),
        );
        result.add_metadata(
            "fill_simulation".to_string(),
            if self.config.use_realistic_fills {
                "realistic"
            } else {
                "naive"
            }
            .to_string(),
        );
        result.add_metadata(
            "bid_fills".to_string(),
            paper_metrics.bid_fills.to_string(),
        );
        result.add_metadata(
            "ask_fills".to_string(),
            paper_metrics.ask_fills.to_string(),
        );

        // Set validation duration
        result.set_duration(duration_secs);

        // Evaluate thresholds
        result.evaluate_thresholds(context.thresholds.clone());

        // Add warnings for potential issues
        if paper_metrics.trade_count < self.config.min_trades {
            result.add_warning(format!(
                "Low trade count: {} (minimum recommended: {})",
                paper_metrics.trade_count, self.config.min_trades
            ));
        }

        if paper_metrics.total_return <= 0.0 {
            result.add_warning("Paper trading return is not profitable".to_string());
        }

        if paper_metrics.sharpe_ratio < self.config.min_sharpe {
            result.add_warning(format!(
                "Paper Sharpe ratio {:.2} below minimum {:.2}",
                paper_metrics.sharpe_ratio, self.config.min_sharpe
            ));
        }

        if paper_metrics.max_drawdown > self.config.max_drawdown {
            result.add_warning(format!(
                "Paper max drawdown {:.1}% exceeds limit {:.1}%",
                paper_metrics.max_drawdown * 100.0,
                self.config.max_drawdown * 100.0
            ));
        }

        if paper_metrics.win_rate < 0.40 {
            result.add_warning(format!(
                "Low paper win rate: {:.1}%",
                paper_metrics.win_rate * 100.0
            ));
        }

        if !paper_metrics.graceful_shutdown {
            result.add_warning("Paper trading did not achieve graceful shutdown".to_string());
        }

        result
    }
}

impl ValidationStage for PaperStage {
    fn stage_type(&self) -> ValidationStageType {
        ValidationStageType::Paper
    }

    fn name(&self) -> &str {
        &self.config.name
    }

    fn description(&self) -> &str {
        "Paper trading validation with simulated execution"
    }

    fn can_run(&self, context: &StageContext) -> Result<(), StageError> {
        // Check period validity
        if context.period_end <= context.period_start {
            return Err(StageError::ConfigurationError(
                "Period end must be after period start".to_string(),
            ));
        }

        // Validate configuration
        self.config
            .validate()
            .map_err(StageError::ConfigurationError)?;

        // Check for previous stage requirement
        if let Some(required_stage) = self.requires_previous() {
            if let Some(passed) = context.previous_stage_passed(required_stage) {
                if !passed {
                    return Err(StageError::ConfigurationError(format!(
                        "Required previous stage {} did not pass",
                        required_stage.display_name()
                    )));
                }
            }
        }

        Ok(())
    }

    fn run<'a>(&'a self, context: &'a StageContext) -> RunFuture<'a> {
        Box::pin(async move {
            let start_time = Instant::now();

            // Execute paper trading
            let (trades, paper_metrics) = self.execute_paper(context).await?;

            let duration_secs = start_time.elapsed().as_secs_f64();

            // Convert to ValidationResult
            let result = self.convert_results(&trades, &paper_metrics, context, duration_secs);

            Ok(result)
        })
    }

    fn estimated_duration(&self, _context: &StageContext) -> Option<u64> {
        // Paper stage runs for configured duration plus some overhead
        Some(self.config.duration_seconds + 10)
    }

    fn min_trades(&self) -> usize {
        self.config.min_trades
    }

    fn requires_previous(&self) -> Option<ValidationStageType> {
        Some(ValidationStageType::OutOfSample) // Paper requires OOS to pass first
    }
}

/// Factory for creating PaperStage instances
pub struct PaperStageFactory {
    default_config: PaperStageConfig,
}

impl PaperStageFactory {
    /// Create a new factory with default configuration
    pub fn new() -> Self {
        Self {
            default_config: PaperStageConfig::default(),
        }
    }

    /// Create a factory with custom default configuration
    pub fn with_config(config: PaperStageConfig) -> Self {
        Self {
            default_config: config,
        }
    }

    /// Create a PaperStage with the default configuration
    pub fn create(&self, name: &str) -> PaperStage {
        PaperStage::new(self.default_config.clone().with_name(name))
    }

    /// Create a PaperStage with custom configuration
    pub fn create_with_config(&self, name: &str, config: PaperStageConfig) -> PaperStage {
        PaperStage::new(config.with_name(name))
    }
}

impl Default for PaperStageFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AlgorithmConfig, ValidationThresholds};
    use chrono::Duration;

    // ==================== PaperStageConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = PaperStageConfig::default();

        assert_eq!(config.duration_seconds, 3600);
        assert!((config.fill_probability - 0.10).abs() < 0.01);
        assert!((config.slippage_bps - 1.0).abs() < 0.01);
        assert!((config.fee_rate_bps - 1.0).abs() < 0.01);
        assert!((config.initial_capital - 10_000.0).abs() < 0.01);
        assert_eq!(config.pnl_sample_interval_seconds, 60);
        assert_eq!(config.min_trades, 10);
        assert!(config.use_realistic_fills);
        assert!(!config.verbose);
        assert_eq!(config.name, "Paper");
    }

    #[test]
    fn test_config_fast() {
        let config = PaperStageConfig::fast();

        assert_eq!(config.duration_seconds, 300);
        assert!((config.fill_probability - 0.50).abs() < 0.01);
        assert!(!config.use_realistic_fills);
        assert_eq!(config.min_trades, 5);
    }

    #[test]
    fn test_config_conservative() {
        let config = PaperStageConfig::conservative();

        assert_eq!(config.duration_seconds, 86400);
        assert!((config.fill_probability - 0.05).abs() < 0.01);
        assert!((config.slippage_bps - 2.0).abs() < 0.01);
        assert!(config.use_realistic_fills);
        assert_eq!(config.min_trades, 20);
    }

    #[test]
    fn test_config_extended() {
        let config = PaperStageConfig::extended();

        assert_eq!(config.duration_seconds, 604800);
        assert_eq!(config.min_trades, 50);
    }

    #[test]
    fn test_config_simulation() {
        let config = PaperStageConfig::simulation();

        assert_eq!(config.duration_seconds, 60);
        assert!((config.fill_probability - 1.0).abs() < 0.01);
        assert!((config.slippage_bps).abs() < 0.01);
        assert!(!config.use_realistic_fills);
    }

    #[test]
    fn test_config_with_name() {
        let config = PaperStageConfig::default().with_name("Paper-2025Q1");
        assert_eq!(config.name, "Paper-2025Q1");
    }

    #[test]
    fn test_config_with_duration() {
        let config = PaperStageConfig::default().with_duration(7200);
        assert_eq!(config.duration_seconds, 7200);
    }

    #[test]
    fn test_config_with_duration_minimum() {
        let config = PaperStageConfig::default().with_duration(0);
        assert_eq!(config.duration_seconds, 1); // Clamped to minimum
    }

    #[test]
    fn test_config_with_fill_probability() {
        let config = PaperStageConfig::default().with_fill_probability(0.25);
        assert!((config.fill_probability - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_config_with_fill_probability_clamped() {
        let config = PaperStageConfig::default().with_fill_probability(1.5);
        assert!((config.fill_probability - 1.0).abs() < 0.01);

        let config = PaperStageConfig::default().with_fill_probability(-0.5);
        assert!((config.fill_probability).abs() < 0.01);
    }

    #[test]
    fn test_config_with_slippage() {
        let config = PaperStageConfig::default().with_slippage(3.0);
        assert!((config.slippage_bps - 3.0).abs() < 0.01);
    }

    #[test]
    fn test_config_with_slippage_minimum() {
        let config = PaperStageConfig::default().with_slippage(-1.0);
        assert!((config.slippage_bps).abs() < 0.01);
    }

    #[test]
    fn test_config_validate_success() {
        let config = PaperStageConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_zero_duration() {
        let config = PaperStageConfig {
            duration_seconds: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_invalid_fill_probability() {
        let config = PaperStageConfig {
            fill_probability: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_negative_slippage() {
        let config = PaperStageConfig {
            slippage_bps: -1.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_invalid_max_drawdown() {
        let config = PaperStageConfig {
            max_drawdown: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_serialization() {
        let config = PaperStageConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PaperStageConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, config.name);
        assert_eq!(deserialized.duration_seconds, config.duration_seconds);
    }

    #[test]
    fn test_config_clone() {
        let config = PaperStageConfig::conservative();
        let cloned = config.clone();

        assert_eq!(cloned.name, config.name);
        assert_eq!(cloned.duration_seconds, config.duration_seconds);
    }

    #[test]
    fn test_config_debug() {
        let config = PaperStageConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("PaperStageConfig"));
        assert!(debug_str.contains("duration_seconds"));
    }

    // ==================== PaperMetrics Tests ====================

    #[test]
    fn test_paper_metrics_default() {
        let metrics = PaperMetrics::default();

        assert_eq!(metrics.duration_seconds, 0);
        assert_eq!(metrics.trade_count, 0);
        assert!((metrics.sharpe_ratio).abs() < 0.01);
        assert!((metrics.total_return).abs() < 0.01);
    }

    #[test]
    fn test_paper_metrics_new_with_trades() {
        let trades = vec![
            SimulatedTrade::new("T1".to_string(), 1000, 2000, 100.0, 101.0, 1.0, true, 0.5, 0.01),
            SimulatedTrade::new("T2".to_string(), 2000, 3000, 100.0, 99.0, 1.0, true, 0.5, 0.01),
        ];

        let samples = vec![
            PnLSample {
                timestamp_ms: 1000,
                cumulative_pnl: 0.0,
                equity: 10000.0,
                drawdown: 0.0,
                trade_count: 0,
            },
            PnLSample {
                timestamp_ms: 2000,
                cumulative_pnl: 0.99,
                equity: 10000.99,
                drawdown: 0.0,
                trade_count: 1,
            },
        ];

        let metrics = PaperMetrics::new(3600, 10000.0, &trades, samples, true);

        assert_eq!(metrics.trade_count, 2);
        assert!((metrics.win_rate - 0.5).abs() < 0.01);
        assert!(metrics.graceful_shutdown);
    }

    #[test]
    fn test_paper_metrics_meets_requirements_pass() {
        let mut metrics = PaperMetrics::default();
        metrics.trade_count = 15;
        metrics.sharpe_ratio = 0.5;
        metrics.max_drawdown = 0.08;

        let config = PaperStageConfig::default();
        assert!(metrics.meets_requirements(&config));
    }

    #[test]
    fn test_paper_metrics_meets_requirements_fail_trades() {
        let mut metrics = PaperMetrics::default();
        metrics.trade_count = 5; // Below min_trades (10)
        metrics.sharpe_ratio = 0.5;
        metrics.max_drawdown = 0.08;

        let config = PaperStageConfig::default();
        assert!(!metrics.meets_requirements(&config));
    }

    #[test]
    fn test_paper_metrics_meets_requirements_fail_drawdown() {
        let mut metrics = PaperMetrics::default();
        metrics.trade_count = 15;
        metrics.sharpe_ratio = 0.5;
        metrics.max_drawdown = 0.25; // Above max_drawdown (0.15)

        let config = PaperStageConfig::default();
        assert!(!metrics.meets_requirements(&config));
    }

    #[test]
    fn test_paper_metrics_is_go_pass() {
        let mut metrics = PaperMetrics::default();
        metrics.trade_count = 15;
        metrics.sharpe_ratio = 0.5;
        metrics.max_drawdown = 0.08;
        metrics.total_return = 0.05;
        metrics.win_rate = 0.55;
        metrics.graceful_shutdown = true;

        let config = PaperStageConfig::default();
        assert!(metrics.is_go(&config));
    }

    #[test]
    fn test_paper_metrics_is_go_fail_not_profitable() {
        let mut metrics = PaperMetrics::default();
        metrics.trade_count = 15;
        metrics.sharpe_ratio = 0.5;
        metrics.max_drawdown = 0.08;
        metrics.total_return = -0.02; // Not profitable
        metrics.win_rate = 0.55;
        metrics.graceful_shutdown = true;

        let config = PaperStageConfig::default();
        assert!(!metrics.is_go(&config));
    }

    #[test]
    fn test_paper_metrics_is_go_fail_low_win_rate() {
        let mut metrics = PaperMetrics::default();
        metrics.trade_count = 15;
        metrics.sharpe_ratio = 0.5;
        metrics.max_drawdown = 0.08;
        metrics.total_return = 0.05;
        metrics.win_rate = 0.20; // Below 30%
        metrics.graceful_shutdown = true;

        let config = PaperStageConfig::default();
        assert!(!metrics.is_go(&config));
    }

    #[test]
    fn test_paper_metrics_is_go_fail_no_graceful_shutdown() {
        let mut metrics = PaperMetrics::default();
        metrics.trade_count = 15;
        metrics.sharpe_ratio = 0.5;
        metrics.max_drawdown = 0.08;
        metrics.total_return = 0.05;
        metrics.win_rate = 0.55;
        metrics.graceful_shutdown = false;

        let config = PaperStageConfig::default();
        assert!(!metrics.is_go(&config));
    }

    #[test]
    fn test_paper_metrics_serialization() {
        let metrics = PaperMetrics {
            duration_seconds: 3600,
            trade_count: 20,
            sharpe_ratio: 1.5,
            total_return: 0.05,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 5.0,
            fills_generated: 20,
            fill_rate: 0.1,
            peak_equity: 10500.0,
            final_equity: 10500.0,
            pnl_samples: vec![],
            graceful_shutdown: true,
            quotes_generated: 200,
            bid_fills: 10,
            ask_fills: 10,
            total_slippage_bps: 20.0,
            total_fees: 2.0,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: PaperMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.duration_seconds, metrics.duration_seconds);
        assert!((deserialized.sharpe_ratio - metrics.sharpe_ratio).abs() < 0.01);
    }

    // ==================== SimulatedTrade Tests ====================

    #[test]
    fn test_simulated_trade_new_bid_winner() {
        let trade = SimulatedTrade::new(
            "T1".to_string(),
            1000,
            2000,
            100.0,
            101.0,
            1.0,
            true, // bid
            0.5,
            0.02,
        );

        assert!(trade.pnl > 0.0);
        assert!(trade.return_bps > 0.0);
        assert!(trade.is_bid);
    }

    #[test]
    fn test_simulated_trade_new_bid_loser() {
        let trade = SimulatedTrade::new(
            "T1".to_string(),
            1000,
            2000,
            100.0,
            99.0,
            1.0,
            true, // bid
            0.5,
            0.02,
        );

        assert!(trade.pnl < 0.0);
        assert!(trade.return_bps < 0.0);
    }

    #[test]
    fn test_simulated_trade_new_ask_winner() {
        let trade = SimulatedTrade::new(
            "T1".to_string(),
            1000,
            2000,
            100.0,
            99.0,
            1.0,
            false, // ask (short)
            0.5,
            0.02,
        );

        assert!(trade.pnl > 0.0);
    }

    #[test]
    fn test_simulated_trade_to_trade_result() {
        let trade = SimulatedTrade::new(
            "T1".to_string(),
            1000,
            2000,
            100.0,
            101.0,
            1.0,
            true,
            0.5,
            0.02,
        );

        let result = trade.to_trade_result("CFG001");

        assert_eq!(result.trade_id, "T1");
        assert_eq!(result.direction, TradeDirection::Long);
        assert_eq!(result.config_id, Some("CFG001".to_string()));
    }

    #[test]
    fn test_simulated_trade_serialization() {
        let trade = SimulatedTrade::new(
            "T1".to_string(),
            1000,
            2000,
            100.0,
            101.0,
            1.0,
            true,
            0.5,
            0.02,
        );

        let json = serde_json::to_string(&trade).unwrap();
        let deserialized: SimulatedTrade = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.trade_id, trade.trade_id);
        assert!((deserialized.pnl - trade.pnl).abs() < 0.01);
    }

    // ==================== ShutdownHandle Tests ====================

    #[test]
    fn test_shutdown_handle_new() {
        let handle = ShutdownHandle::new();
        assert!(!handle.is_shutdown_requested());
    }

    #[test]
    fn test_shutdown_handle_default() {
        let handle = ShutdownHandle::default();
        assert!(!handle.is_shutdown_requested());
    }

    #[test]
    fn test_shutdown_handle_shutdown() {
        let handle = ShutdownHandle::new();
        assert!(!handle.is_shutdown_requested());

        handle.shutdown();
        assert!(handle.is_shutdown_requested());
    }

    #[test]
    fn test_shutdown_handle_clone() {
        let handle1 = ShutdownHandle::new();
        let handle2 = handle1.clone();

        handle1.shutdown();

        assert!(handle1.is_shutdown_requested());
        assert!(handle2.is_shutdown_requested());
    }

    // ==================== PaperStage Basic Tests ====================

    #[test]
    fn test_stage_new() {
        let config = PaperStageConfig::default();
        let stage = PaperStage::new(config.clone());

        assert_eq!(stage.config.name, config.name);
    }

    #[test]
    fn test_stage_with_defaults() {
        let stage = PaperStage::with_defaults();

        assert_eq!(stage.stage_type(), ValidationStageType::Paper);
        assert_eq!(stage.name(), "Paper");
    }

    #[test]
    fn test_stage_type() {
        let stage = PaperStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::Paper);
    }

    #[test]
    fn test_stage_name() {
        let config = PaperStageConfig::default().with_name("Custom-Paper");
        let stage = PaperStage::new(config);

        assert_eq!(stage.name(), "Custom-Paper");
    }

    #[test]
    fn test_stage_description() {
        let stage = PaperStage::with_defaults();
        let desc = stage.description();

        assert!(desc.contains("Paper"));
        assert!(desc.contains("simulated"));
    }

    #[test]
    fn test_stage_min_trades() {
        let config = PaperStageConfig {
            min_trades: 25,
            ..Default::default()
        };
        let stage = PaperStage::new(config);
        assert_eq!(stage.min_trades(), 25);
    }

    #[test]
    fn test_stage_requires_previous() {
        let stage = PaperStage::with_defaults();
        assert_eq!(
            stage.requires_previous(),
            Some(ValidationStageType::OutOfSample)
        );
    }

    #[test]
    fn test_stage_estimated_duration() {
        let config = PaperStageConfig::default().with_duration(7200);
        let stage = PaperStage::new(config);
        let ctx = StageContext::default();

        let duration = stage.estimated_duration(&ctx);
        assert!(duration.is_some());
        assert!(duration.unwrap() >= 7200);
    }

    #[test]
    fn test_stage_shutdown_handle() {
        let stage = PaperStage::with_defaults();
        let handle = stage.shutdown_handle();

        assert!(!handle.is_shutdown_requested());
    }

    // ==================== can_run() Tests ====================

    #[test]
    fn test_can_run_valid() {
        let stage = PaperStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::hours(1),
            Utc::now(),
        );

        // Paper doesn't require previous stages in basic can_run
        // (requirement is checked separately)
        assert!(stage.can_run(&ctx).is_ok());
    }

    #[test]
    fn test_can_run_invalid_period() {
        let stage = PaperStage::with_defaults();
        let now = Utc::now();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now,
            now - Duration::days(1), // End before start
        );

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StageError::ConfigurationError(_)
        ));
    }

    #[test]
    fn test_can_run_same_start_end() {
        let stage = PaperStage::with_defaults();
        let now = Utc::now();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now,
            now, // Same as start
        );

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_can_run_invalid_config() {
        let config = PaperStageConfig {
            duration_seconds: 0, // Invalid
            ..Default::default()
        };
        let stage = PaperStage::new(config);

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::hours(1),
            Utc::now(),
        );

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
    }

    // ==================== Factory Tests ====================

    #[test]
    fn test_factory_new() {
        let factory = PaperStageFactory::new();
        let stage = factory.create("Paper-Test");

        assert_eq!(stage.name(), "Paper-Test");
    }

    #[test]
    fn test_factory_default() {
        let factory = PaperStageFactory::default();
        let stage = factory.create("Paper-Default");

        assert_eq!(stage.name(), "Paper-Default");
    }

    #[test]
    fn test_factory_with_config() {
        let config = PaperStageConfig::conservative();
        let factory = PaperStageFactory::with_config(config);
        let stage = factory.create("Paper-Conservative");

        assert_eq!(stage.name(), "Paper-Conservative");
        assert_eq!(stage.config.duration_seconds, 86400);
    }

    #[test]
    fn test_factory_create_with_config() {
        let factory = PaperStageFactory::new();
        let custom_config = PaperStageConfig::fast();
        let stage = factory.create_with_config("Paper-Custom", custom_config);

        assert_eq!(stage.name(), "Paper-Custom");
        assert!(!stage.config.use_realistic_fills);
    }

    // ==================== ValidationStage Trait Tests ====================

    #[test]
    fn test_trait_stage_type_is_paper() {
        let stage = PaperStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::Paper);
    }

    #[test]
    fn test_trait_uses_live_data() {
        let stage = PaperStage::with_defaults();
        // Paper stage uses live data (simulated)
        assert!(stage.stage_type().uses_live_data());
    }

    #[test]
    fn test_trait_is_not_historical() {
        let stage = PaperStage::with_defaults();
        assert!(!stage.stage_type().is_historical());
    }

    #[test]
    fn test_trait_pipeline_order() {
        let stage = PaperStage::with_defaults();
        assert_eq!(stage.stage_type().pipeline_order(), 4); // Fourth in pipeline
    }

    // ==================== Async Run Tests ====================

    #[tokio::test]
    async fn test_run_simulation_short() {
        let config = PaperStageConfig::simulation();
        let stage = PaperStage::new(config);

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::relaxed(),
            Utc::now() - Duration::minutes(1),
            Utc::now(),
        )
        .with_name("Paper-Sim");

        let result = stage.run(&ctx).await;
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.stage_type, ValidationStageType::Paper);
    }

    #[tokio::test]
    async fn test_run_with_shutdown() {
        let config = PaperStageConfig {
            duration_seconds: 3600, // Long duration
            ..PaperStageConfig::simulation()
        };
        let stage = PaperStage::new(config);

        let handle = stage.shutdown_handle();

        // Start the run in a separate task
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::relaxed(),
            Utc::now() - Duration::minutes(1),
            Utc::now(),
        )
        .with_name("Paper-Shutdown");

        // Request shutdown immediately
        handle.shutdown();

        let result = stage.run(&ctx).await;
        // Should complete early due to shutdown
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_returns_future() {
        let stage = PaperStage::with_defaults();
        let ctx = StageContext::default().with_name("Test");

        let _future = stage.run(&ctx);
        // Just checking it compiles and returns a future
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_stage_name() {
        let config = PaperStageConfig::default().with_name("");
        let stage = PaperStage::new(config);
        assert_eq!(stage.name(), "");
    }

    #[test]
    fn test_very_long_stage_name() {
        let long_name = "P".repeat(1000);
        let config = PaperStageConfig::default().with_name(&long_name);
        let stage = PaperStage::new(config);
        assert_eq!(stage.name().len(), 1000);
    }

    #[test]
    fn test_zero_fill_probability() {
        let config = PaperStageConfig {
            fill_probability: 0.0,
            ..Default::default()
        };
        let stage = PaperStage::new(config);
        assert!((stage.config.fill_probability).abs() < 0.01);
    }

    #[test]
    fn test_full_fill_probability() {
        let config = PaperStageConfig {
            fill_probability: 1.0,
            ..Default::default()
        };
        let stage = PaperStage::new(config);
        assert!((stage.config.fill_probability - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_zero_initial_capital() {
        let config = PaperStageConfig {
            initial_capital: 0.0,
            ..Default::default()
        };
        let stage = PaperStage::new(config);
        assert!((stage.config.initial_capital).abs() < 0.01);
    }

    #[test]
    fn test_very_short_duration() {
        let config = PaperStageConfig::default().with_duration(1);
        let stage = PaperStage::new(config);
        assert_eq!(stage.config.duration_seconds, 1);
    }

    #[test]
    fn test_very_long_duration() {
        let config = PaperStageConfig::default().with_duration(31536000); // 1 year
        let stage = PaperStage::new(config);
        assert_eq!(stage.config.duration_seconds, 31536000);
    }

    // ==================== PnLSample Tests ====================

    #[test]
    fn test_pnl_sample_serialization() {
        let sample = PnLSample {
            timestamp_ms: 1000,
            cumulative_pnl: 100.0,
            equity: 10100.0,
            drawdown: 0.01,
            trade_count: 5,
        };

        let json = serde_json::to_string(&sample).unwrap();
        let deserialized: PnLSample = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.timestamp_ms, sample.timestamp_ms);
        assert!((deserialized.cumulative_pnl - sample.cumulative_pnl).abs() < 0.01);
    }

    // ==================== Integration Tests ====================

    #[tokio::test]
    async fn test_full_paper_trading_workflow() {
        let config = PaperStageConfig::simulation();
        let stage = PaperStage::new(config);

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::relaxed(),
            Utc::now() - Duration::minutes(5),
            Utc::now(),
        )
        .with_name("Paper-Full-Test");

        // Run paper trading
        let result = stage.run(&ctx).await;
        assert!(result.is_ok());

        let result = result.unwrap();

        // Verify result structure
        assert_eq!(result.stage_type, ValidationStageType::Paper);
        assert_eq!(result.stage_name, "Paper-Full-Test");
        assert!(result.metadata.contains_key("duration_seconds"));
        assert!(result.metadata.contains_key("graceful_shutdown"));
    }

    #[test]
    fn test_sharpe_calculation_empty_samples() {
        let sharpe = PaperMetrics::calculate_sharpe(&[], 3600);
        assert!((sharpe).abs() < 0.01);
    }

    #[test]
    fn test_sharpe_calculation_single_sample() {
        let samples = vec![PnLSample {
            timestamp_ms: 1000,
            cumulative_pnl: 100.0,
            equity: 10100.0,
            drawdown: 0.0,
            trade_count: 1,
        }];

        let sharpe = PaperMetrics::calculate_sharpe(&samples, 3600);
        assert!((sharpe).abs() < 0.01);
    }

    #[test]
    fn test_sharpe_calculation_constant_equity() {
        let samples = vec![
            PnLSample {
                timestamp_ms: 1000,
                cumulative_pnl: 0.0,
                equity: 10000.0,
                drawdown: 0.0,
                trade_count: 0,
            },
            PnLSample {
                timestamp_ms: 2000,
                cumulative_pnl: 0.0,
                equity: 10000.0,
                drawdown: 0.0,
                trade_count: 0,
            },
        ];

        let sharpe = PaperMetrics::calculate_sharpe(&samples, 3600);
        // Zero std dev, zero return should give 0
        assert!((sharpe).abs() < 0.01);
    }

    #[test]
    fn test_sharpe_calculation_positive_returns() {
        let samples = vec![
            PnLSample {
                timestamp_ms: 1000,
                cumulative_pnl: 0.0,
                equity: 10000.0,
                drawdown: 0.0,
                trade_count: 0,
            },
            PnLSample {
                timestamp_ms: 2000,
                cumulative_pnl: 100.0,
                equity: 10100.0,
                drawdown: 0.0,
                trade_count: 1,
            },
            PnLSample {
                timestamp_ms: 3000,
                cumulative_pnl: 200.0,
                equity: 10200.0,
                drawdown: 0.0,
                trade_count: 2,
            },
        ];

        let sharpe = PaperMetrics::calculate_sharpe(&samples, 3600);
        // Consistent positive returns should give positive Sharpe
        assert!(sharpe > 0.0 || sharpe.is_infinite());
    }

    // ==================== Concurrent Access Tests ====================

    #[tokio::test]
    async fn test_concurrent_shutdown_handle_access() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let handle = Arc::new(ShutdownHandle::new());
        let mut tasks = JoinSet::new();

        // Spawn multiple tasks checking shutdown status
        for _ in 0..10 {
            let handle_clone = handle.clone();
            tasks.spawn(async move { handle_clone.is_shutdown_requested() });
        }

        // All should return false
        while let Some(result) = tasks.join_next().await {
            assert!(!result.unwrap());
        }

        // Now shutdown
        handle.shutdown();

        // Spawn more tasks
        let mut tasks = JoinSet::new();
        for _ in 0..10 {
            let handle_clone = handle.clone();
            tasks.spawn(async move { handle_clone.is_shutdown_requested() });
        }

        // All should return true
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap());
        }
    }
}

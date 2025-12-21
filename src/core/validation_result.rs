//! Validation Result - Task 0.2
//!
//! Unified result structure that all validation stages produce.
//! This enables consistent evaluation across Backtest, Forward, OOS, Paper, and Live stages.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Type of validation stage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValidationStageType {
    /// Historical replay on past data
    Backtest,
    /// Walk-forward validation with rolling windows
    Forward,
    /// Out-of-sample validation on held-out data
    OutOfSample,
    /// Paper trading with live data, simulated execution
    Paper,
    /// Live trading with real execution
    Live,
}

impl ValidationStageType {
    /// Get the display name for this stage type
    pub fn display_name(&self) -> &'static str {
        match self {
            ValidationStageType::Backtest => "Backtest",
            ValidationStageType::Forward => "Forward",
            ValidationStageType::OutOfSample => "Out-of-Sample",
            ValidationStageType::Paper => "Paper",
            ValidationStageType::Live => "Live",
        }
    }

    /// Get short code for this stage type
    pub fn code(&self) -> &'static str {
        match self {
            ValidationStageType::Backtest => "BT",
            ValidationStageType::Forward => "FW",
            ValidationStageType::OutOfSample => "OOS",
            ValidationStageType::Paper => "PP",
            ValidationStageType::Live => "LV",
        }
    }

    /// Check if this is a historical (non-live) stage
    pub fn is_historical(&self) -> bool {
        matches!(
            self,
            ValidationStageType::Backtest
                | ValidationStageType::Forward
                | ValidationStageType::OutOfSample
        )
    }

    /// Check if this involves real market data
    pub fn uses_live_data(&self) -> bool {
        matches!(self, ValidationStageType::Paper | ValidationStageType::Live)
    }

    /// Check if this involves real execution
    pub fn uses_real_execution(&self) -> bool {
        matches!(self, ValidationStageType::Live)
    }

    /// Get the recommended order for pipeline progression
    pub fn pipeline_order(&self) -> u8 {
        match self {
            ValidationStageType::Backtest => 1,
            ValidationStageType::Forward => 2,
            ValidationStageType::OutOfSample => 3,
            ValidationStageType::Paper => 4,
            ValidationStageType::Live => 5,
        }
    }
}

/// Trade direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradeDirection {
    Long,
    Short,
}

impl TradeDirection {
    /// Get the opposite direction
    pub fn opposite(&self) -> Self {
        match self {
            TradeDirection::Long => TradeDirection::Short,
            TradeDirection::Short => TradeDirection::Long,
        }
    }

    /// Get the sign multiplier for P&L calculations
    pub fn sign(&self) -> f64 {
        match self {
            TradeDirection::Long => 1.0,
            TradeDirection::Short => -1.0,
        }
    }
}

/// Exit reason for a trade
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExitReason {
    /// Take profit target hit
    TakeProfit,
    /// Stop loss triggered
    StopLoss,
    /// Signal reversed
    SignalReversal,
    /// Time-based exit
    TimeExpiry,
    /// Manual exit
    Manual,
    /// Circuit breaker triggered
    CircuitBreaker,
    /// End of validation period
    EndOfPeriod,
    /// Position sizing reduced to zero
    PositionClosed,
    /// Unknown or unspecified
    Unknown,
}

impl ExitReason {
    /// Check if this was a winning exit
    pub fn is_winner(&self) -> bool {
        matches!(self, ExitReason::TakeProfit)
    }

    /// Check if this was a losing exit
    pub fn is_loser(&self) -> bool {
        matches!(self, ExitReason::StopLoss | ExitReason::CircuitBreaker)
    }

    /// Check if this was a controlled exit (not forced)
    pub fn is_controlled(&self) -> bool {
        matches!(
            self,
            ExitReason::TakeProfit
                | ExitReason::SignalReversal
                | ExitReason::TimeExpiry
                | ExitReason::Manual
                | ExitReason::PositionClosed
        )
    }
}

/// Result of a single trade
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeResult {
    /// Unique trade identifier
    pub trade_id: String,

    /// Trade direction
    pub direction: TradeDirection,

    /// Entry timestamp
    pub entry_time: DateTime<Utc>,

    /// Exit timestamp
    pub exit_time: DateTime<Utc>,

    /// Entry price
    pub entry_price: f64,

    /// Exit price
    pub exit_price: f64,

    /// Position size (quantity)
    pub size: f64,

    /// Realized P&L in quote currency
    pub pnl: f64,

    /// Realized P&L in basis points
    pub pnl_bps: f64,

    /// Return percentage
    pub return_pct: f64,

    /// Exit reason
    pub exit_reason: ExitReason,

    /// Research state ID at entry (for traceability)
    pub research_state_id: Option<String>,

    /// Algorithm config ID that generated this trade
    pub config_id: Option<String>,

    /// Slippage in basis points (actual vs expected)
    pub slippage_bps: f64,

    /// Commission/fees paid
    pub commission: f64,

    /// Maximum adverse excursion (worst drawdown during trade)
    pub mae_bps: f64,

    /// Maximum favorable excursion (best profit during trade)
    pub mfe_bps: f64,

    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl TradeResult {
    /// Create a new trade result
    pub fn new(
        trade_id: String,
        direction: TradeDirection,
        entry_time: DateTime<Utc>,
        exit_time: DateTime<Utc>,
        entry_price: f64,
        exit_price: f64,
        size: f64,
    ) -> Self {
        let price_diff = exit_price - entry_price;
        let return_pct = (price_diff / entry_price) * 100.0 * direction.sign();
        let pnl_bps = return_pct * 100.0;
        let pnl = price_diff * size * direction.sign();

        Self {
            trade_id,
            direction,
            entry_time,
            exit_time,
            entry_price,
            exit_price,
            size,
            pnl,
            pnl_bps,
            return_pct,
            exit_reason: ExitReason::Unknown,
            research_state_id: None,
            config_id: None,
            slippage_bps: 0.0,
            commission: 0.0,
            mae_bps: 0.0,
            mfe_bps: 0.0,
            metadata: HashMap::new(),
        }
    }

    /// Check if this trade was profitable
    pub fn is_winner(&self) -> bool {
        self.pnl > 0.0
    }

    /// Check if this trade was a loss
    pub fn is_loser(&self) -> bool {
        self.pnl < 0.0
    }

    /// Get the trade duration
    pub fn duration(&self) -> Duration {
        self.exit_time - self.entry_time
    }

    /// Get duration in seconds
    pub fn duration_seconds(&self) -> i64 {
        self.duration().num_seconds()
    }

    /// Get net P&L after commission
    pub fn net_pnl(&self) -> f64 {
        self.pnl - self.commission
    }

    /// Get the risk-reward ratio (MFE / MAE)
    pub fn risk_reward_ratio(&self) -> f64 {
        if self.mae_bps.abs() < 1e-10 {
            if self.mfe_bps > 0.0 {
                f64::INFINITY
            } else {
                0.0
            }
        } else {
            self.mfe_bps / self.mae_bps.abs()
        }
    }

    /// Set the exit reason
    pub fn with_exit_reason(mut self, reason: ExitReason) -> Self {
        self.exit_reason = reason;
        self
    }

    /// Set the research state ID
    pub fn with_research_state(mut self, state_id: String) -> Self {
        self.research_state_id = Some(state_id);
        self
    }

    /// Set the config ID
    pub fn with_config(mut self, config_id: String) -> Self {
        self.config_id = Some(config_id);
        self
    }

    /// Set slippage
    pub fn with_slippage(mut self, slippage_bps: f64) -> Self {
        self.slippage_bps = slippage_bps;
        self
    }

    /// Set commission
    pub fn with_commission(mut self, commission: f64) -> Self {
        self.commission = commission;
        self
    }

    /// Set MAE/MFE
    pub fn with_excursions(mut self, mae_bps: f64, mfe_bps: f64) -> Self {
        self.mae_bps = mae_bps;
        self.mfe_bps = mfe_bps;
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

impl Default for TradeResult {
    fn default() -> Self {
        Self {
            trade_id: String::new(),
            direction: TradeDirection::Long,
            entry_time: Utc::now(),
            exit_time: Utc::now(),
            entry_price: 0.0,
            exit_price: 0.0,
            size: 0.0,
            pnl: 0.0,
            pnl_bps: 0.0,
            return_pct: 0.0,
            exit_reason: ExitReason::Unknown,
            research_state_id: None,
            config_id: None,
            slippage_bps: 0.0,
            commission: 0.0,
            mae_bps: 0.0,
            mfe_bps: 0.0,
            metadata: HashMap::new(),
        }
    }
}

/// Threshold configuration for pass/fail evaluation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationThresholds {
    /// Minimum Sharpe ratio required
    pub min_sharpe: f64,

    /// Maximum drawdown allowed (as positive percentage, e.g., 10.0 for 10%)
    pub max_drawdown_pct: f64,

    /// Minimum win rate required (0.0 to 1.0)
    pub min_win_rate: f64,

    /// Minimum number of trades required
    pub min_trade_count: usize,

    /// Minimum profit factor required (gross profit / gross loss)
    pub min_profit_factor: f64,

    /// Maximum average slippage allowed in bps
    pub max_avg_slippage_bps: f64,

    /// Minimum average trade P&L in bps
    pub min_avg_trade_bps: f64,

    /// Maximum consecutive losses allowed
    pub max_consecutive_losses: usize,
}

impl Default for ValidationThresholds {
    fn default() -> Self {
        Self {
            min_sharpe: 0.5,
            max_drawdown_pct: 20.0,
            min_win_rate: 0.4,
            min_trade_count: 30,
            min_profit_factor: 1.0,
            max_avg_slippage_bps: 5.0,
            min_avg_trade_bps: 0.0,
            max_consecutive_losses: 10,
        }
    }
}

impl ValidationThresholds {
    /// Create strict thresholds for production
    pub fn strict() -> Self {
        Self {
            min_sharpe: 1.0,
            max_drawdown_pct: 10.0,
            min_win_rate: 0.5,
            min_trade_count: 100,
            min_profit_factor: 1.5,
            max_avg_slippage_bps: 2.0,
            min_avg_trade_bps: 1.0,
            max_consecutive_losses: 5,
        }
    }

    /// Create relaxed thresholds for research
    pub fn relaxed() -> Self {
        Self {
            min_sharpe: 0.0,
            max_drawdown_pct: 50.0,
            min_win_rate: 0.3,
            min_trade_count: 10,
            min_profit_factor: 0.8,
            max_avg_slippage_bps: 10.0,
            min_avg_trade_bps: -5.0,
            max_consecutive_losses: 20,
        }
    }
}

/// Detailed metrics from validation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationMetrics {
    /// Total number of trades
    pub trade_count: usize,

    /// Number of winning trades
    pub winners: usize,

    /// Number of losing trades
    pub losers: usize,

    /// Win rate (0.0 to 1.0)
    pub win_rate: f64,

    /// Total P&L
    pub total_pnl: f64,

    /// Gross profit (sum of winning trades)
    pub gross_profit: f64,

    /// Gross loss (sum of losing trades, positive number)
    pub gross_loss: f64,

    /// Profit factor (gross_profit / gross_loss)
    pub profit_factor: f64,

    /// Average P&L per trade
    pub avg_pnl: f64,

    /// Average P&L in basis points
    pub avg_pnl_bps: f64,

    /// Average winning trade P&L
    pub avg_winner: f64,

    /// Average losing trade P&L
    pub avg_loser: f64,

    /// Largest winning trade
    pub max_winner: f64,

    /// Largest losing trade (as negative number)
    pub max_loser: f64,

    /// Sharpe ratio (annualized)
    pub sharpe_ratio: f64,

    /// Sortino ratio (annualized)
    pub sortino_ratio: f64,

    /// Calmar ratio (annualized return / max drawdown)
    pub calmar_ratio: f64,

    /// Maximum drawdown percentage
    pub max_drawdown_pct: f64,

    /// Maximum drawdown duration in seconds
    pub max_drawdown_duration_seconds: i64,

    /// Average trade duration in seconds
    pub avg_trade_duration_seconds: f64,

    /// Maximum consecutive wins
    pub max_consecutive_wins: usize,

    /// Maximum consecutive losses
    pub max_consecutive_losses: usize,

    /// Total commission paid
    pub total_commission: f64,

    /// Average slippage in bps
    pub avg_slippage_bps: f64,

    /// Expectancy (avg_winner * win_rate - avg_loser * (1 - win_rate))
    pub expectancy: f64,

    /// Annualized return percentage
    pub annualized_return_pct: f64,

    /// Annualized volatility percentage
    pub annualized_volatility_pct: f64,

    /// Long trade count
    pub long_trades: usize,

    /// Short trade count
    pub short_trades: usize,

    /// Long win rate
    pub long_win_rate: f64,

    /// Short win rate
    pub short_win_rate: f64,
}

impl Default for ValidationMetrics {
    fn default() -> Self {
        Self {
            trade_count: 0,
            winners: 0,
            losers: 0,
            win_rate: 0.0,
            total_pnl: 0.0,
            gross_profit: 0.0,
            gross_loss: 0.0,
            profit_factor: 0.0,
            avg_pnl: 0.0,
            avg_pnl_bps: 0.0,
            avg_winner: 0.0,
            avg_loser: 0.0,
            max_winner: 0.0,
            max_loser: 0.0,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
            max_drawdown_pct: 0.0,
            max_drawdown_duration_seconds: 0,
            avg_trade_duration_seconds: 0.0,
            max_consecutive_wins: 0,
            max_consecutive_losses: 0,
            total_commission: 0.0,
            avg_slippage_bps: 0.0,
            expectancy: 0.0,
            annualized_return_pct: 0.0,
            annualized_volatility_pct: 0.0,
            long_trades: 0,
            short_trades: 0,
            long_win_rate: 0.0,
            short_win_rate: 0.0,
        }
    }
}

impl ValidationMetrics {
    /// Compute metrics from a list of trades
    pub fn from_trades(trades: &[TradeResult], period_days: f64) -> Self {
        if trades.is_empty() {
            return Self::default();
        }

        let trade_count = trades.len();

        // Basic counts
        let winners: Vec<_> = trades.iter().filter(|t| t.is_winner()).collect();
        let losers: Vec<_> = trades.iter().filter(|t| t.is_loser()).collect();
        let winner_count = winners.len();
        let loser_count = losers.len();

        // Win rate
        let win_rate = winner_count as f64 / trade_count as f64;

        // P&L calculations
        let total_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
        let gross_profit: f64 = winners.iter().map(|t| t.pnl).sum();
        let gross_loss: f64 = losers.iter().map(|t| t.pnl.abs()).sum();

        // Profit factor
        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Averages
        let avg_pnl = total_pnl / trade_count as f64;
        let avg_pnl_bps: f64 = trades.iter().map(|t| t.pnl_bps).sum::<f64>() / trade_count as f64;

        let avg_winner = if winner_count > 0 {
            gross_profit / winner_count as f64
        } else {
            0.0
        };

        let avg_loser = if loser_count > 0 {
            -gross_loss / loser_count as f64
        } else {
            0.0
        };

        // Max winner/loser
        let max_winner = trades.iter().map(|t| t.pnl).fold(0.0, f64::max);
        let max_loser = trades.iter().map(|t| t.pnl).fold(0.0, f64::min);

        // Returns for Sharpe/Sortino calculation
        let returns: Vec<f64> = trades.iter().map(|t| t.return_pct).collect();
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;

        let variance: f64 = returns.iter().map(|r| (r - mean_return).powi(2)).sum::<f64>()
            / returns.len() as f64;
        let std_dev = variance.sqrt();

        // Downside deviation for Sortino
        let downside_returns: Vec<f64> = returns.iter().filter(|&&r| r < 0.0).copied().collect();
        let downside_variance = if !downside_returns.is_empty() {
            downside_returns.iter().map(|r| r.powi(2)).sum::<f64>() / downside_returns.len() as f64
        } else {
            0.0
        };
        let downside_dev = downside_variance.sqrt();

        // Annualization factor (assuming ~252 trading days)
        let trades_per_year = if period_days > 0.0 {
            (trade_count as f64 / period_days) * 252.0
        } else {
            trade_count as f64
        };
        let annualization_factor = trades_per_year.sqrt();

        // Sharpe ratio (annualized)
        let sharpe_ratio = if std_dev > 0.0 {
            (mean_return / std_dev) * annualization_factor
        } else if mean_return > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Sortino ratio (annualized)
        let sortino_ratio = if downside_dev > 0.0 {
            (mean_return / downside_dev) * annualization_factor
        } else if mean_return > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Drawdown calculation
        let (max_drawdown_pct, max_drawdown_duration) = Self::calculate_drawdown(trades);

        // Calmar ratio
        let annualized_return = mean_return * trades_per_year;
        let calmar_ratio = if max_drawdown_pct > 0.0 {
            annualized_return / max_drawdown_pct
        } else if annualized_return > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Consecutive wins/losses
        let (max_consecutive_wins, max_consecutive_losses) = Self::calculate_streaks(trades);

        // Average duration
        let avg_trade_duration_seconds =
            trades.iter().map(|t| t.duration_seconds() as f64).sum::<f64>() / trade_count as f64;

        // Commission and slippage
        let total_commission: f64 = trades.iter().map(|t| t.commission).sum();
        let avg_slippage_bps: f64 =
            trades.iter().map(|t| t.slippage_bps).sum::<f64>() / trade_count as f64;

        // Expectancy
        let expectancy = avg_winner * win_rate + avg_loser * (1.0 - win_rate);

        // Direction breakdown
        let long_trades: Vec<_> = trades
            .iter()
            .filter(|t| t.direction == TradeDirection::Long)
            .collect();
        let short_trades: Vec<_> = trades
            .iter()
            .filter(|t| t.direction == TradeDirection::Short)
            .collect();

        let long_win_rate = if !long_trades.is_empty() {
            long_trades.iter().filter(|t| t.is_winner()).count() as f64 / long_trades.len() as f64
        } else {
            0.0
        };

        let short_win_rate = if !short_trades.is_empty() {
            short_trades.iter().filter(|t| t.is_winner()).count() as f64 / short_trades.len() as f64
        } else {
            0.0
        };

        // Annualized volatility
        let annualized_volatility_pct = std_dev * annualization_factor;

        Self {
            trade_count,
            winners: winner_count,
            losers: loser_count,
            win_rate,
            total_pnl,
            gross_profit,
            gross_loss,
            profit_factor,
            avg_pnl,
            avg_pnl_bps,
            avg_winner,
            avg_loser,
            max_winner,
            max_loser,
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
            max_drawdown_pct,
            max_drawdown_duration_seconds: max_drawdown_duration,
            avg_trade_duration_seconds,
            max_consecutive_wins,
            max_consecutive_losses,
            total_commission,
            avg_slippage_bps,
            expectancy,
            annualized_return_pct: annualized_return,
            annualized_volatility_pct,
            long_trades: long_trades.len(),
            short_trades: short_trades.len(),
            long_win_rate,
            short_win_rate,
        }
    }

    /// Calculate max drawdown and duration from trades
    fn calculate_drawdown(trades: &[TradeResult]) -> (f64, i64) {
        if trades.is_empty() {
            return (0.0, 0);
        }

        let mut cumulative_pnl = 0.0;
        let mut peak_pnl = 0.0;
        let mut max_drawdown = 0.0;
        let mut max_drawdown_duration: i64 = 0;

        let mut drawdown_start: Option<DateTime<Utc>> = None;

        for trade in trades {
            cumulative_pnl += trade.pnl;

            if cumulative_pnl > peak_pnl {
                peak_pnl = cumulative_pnl;
                drawdown_start = None;
            } else if peak_pnl > 0.0 {
                let current_drawdown = (peak_pnl - cumulative_pnl) / peak_pnl * 100.0;
                if current_drawdown > max_drawdown {
                    max_drawdown = current_drawdown;
                }

                if drawdown_start.is_none() {
                    drawdown_start = Some(trade.entry_time);
                }

                if let Some(start) = drawdown_start {
                    let duration = (trade.exit_time - start).num_seconds();
                    if duration > max_drawdown_duration {
                        max_drawdown_duration = duration;
                    }
                }
            }
        }

        (max_drawdown, max_drawdown_duration)
    }

    /// Calculate consecutive win/loss streaks
    fn calculate_streaks(trades: &[TradeResult]) -> (usize, usize) {
        let mut max_wins = 0;
        let mut max_losses = 0;
        let mut current_wins = 0;
        let mut current_losses = 0;

        for trade in trades {
            if trade.is_winner() {
                current_wins += 1;
                current_losses = 0;
                if current_wins > max_wins {
                    max_wins = current_wins;
                }
            } else if trade.is_loser() {
                current_losses += 1;
                current_wins = 0;
                if current_losses > max_losses {
                    max_losses = current_losses;
                }
            }
        }

        (max_wins, max_losses)
    }
}

/// Complete validation result from a stage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Unique identifier for this result
    pub id: String,

    /// Stage type
    pub stage_type: ValidationStageType,

    /// Stage name (e.g., "Backtest-2025Q1")
    pub stage_name: String,

    /// Algorithm config ID that was validated
    pub config_id: String,

    /// Research state ID used for this validation
    pub research_state_id: Option<String>,

    /// Start of validation period
    pub period_start: DateTime<Utc>,

    /// End of validation period
    pub period_end: DateTime<Utc>,

    /// Computed metrics
    pub metrics: ValidationMetrics,

    /// Individual trade results
    pub trades: Vec<TradeResult>,

    /// Whether this result passed the thresholds
    pub passed: bool,

    /// Thresholds used for evaluation
    pub thresholds: ValidationThresholds,

    /// Detailed pass/fail breakdown
    pub threshold_results: HashMap<String, ThresholdResult>,

    /// Timestamp when validation was run
    pub validated_at: DateTime<Utc>,

    /// Duration of validation run in seconds
    pub validation_duration_seconds: f64,

    /// Any warnings or notes
    pub warnings: Vec<String>,

    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Result of a single threshold check
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThresholdResult {
    /// Threshold name
    pub name: String,

    /// Whether this threshold passed
    pub passed: bool,

    /// Actual value achieved
    pub actual: f64,

    /// Required threshold value
    pub required: f64,

    /// Comparison type (">", "<", ">=", "<=")
    pub comparison: String,
}

impl ValidationResult {
    /// Create a new validation result
    pub fn new(
        stage_type: ValidationStageType,
        stage_name: String,
        config_id: String,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            stage_type,
            stage_name,
            config_id,
            research_state_id: None,
            period_start,
            period_end,
            metrics: ValidationMetrics::default(),
            trades: Vec::new(),
            passed: false,
            thresholds: ValidationThresholds::default(),
            threshold_results: HashMap::new(),
            validated_at: Utc::now(),
            validation_duration_seconds: 0.0,
            warnings: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Set trades and compute metrics
    pub fn with_trades(mut self, trades: Vec<TradeResult>) -> Self {
        let period_days = (self.period_end - self.period_start).num_days() as f64;
        self.metrics = ValidationMetrics::from_trades(&trades, period_days);
        self.trades = trades;
        self
    }

    /// Set the research state ID
    pub fn with_research_state(mut self, state_id: String) -> Self {
        self.research_state_id = Some(state_id);
        self
    }

    /// Evaluate against thresholds
    pub fn evaluate_thresholds(&mut self, thresholds: ValidationThresholds) {
        self.thresholds = thresholds.clone();
        self.threshold_results.clear();

        // Sharpe ratio
        let sharpe_passed = self.metrics.sharpe_ratio >= thresholds.min_sharpe;
        self.threshold_results.insert(
            "sharpe_ratio".to_string(),
            ThresholdResult {
                name: "Sharpe Ratio".to_string(),
                passed: sharpe_passed,
                actual: self.metrics.sharpe_ratio,
                required: thresholds.min_sharpe,
                comparison: ">=".to_string(),
            },
        );

        // Max drawdown
        let drawdown_passed = self.metrics.max_drawdown_pct <= thresholds.max_drawdown_pct;
        self.threshold_results.insert(
            "max_drawdown".to_string(),
            ThresholdResult {
                name: "Max Drawdown".to_string(),
                passed: drawdown_passed,
                actual: self.metrics.max_drawdown_pct,
                required: thresholds.max_drawdown_pct,
                comparison: "<=".to_string(),
            },
        );

        // Win rate
        let win_rate_passed = self.metrics.win_rate >= thresholds.min_win_rate;
        self.threshold_results.insert(
            "win_rate".to_string(),
            ThresholdResult {
                name: "Win Rate".to_string(),
                passed: win_rate_passed,
                actual: self.metrics.win_rate,
                required: thresholds.min_win_rate,
                comparison: ">=".to_string(),
            },
        );

        // Trade count
        let trade_count_passed = self.metrics.trade_count >= thresholds.min_trade_count;
        self.threshold_results.insert(
            "trade_count".to_string(),
            ThresholdResult {
                name: "Trade Count".to_string(),
                passed: trade_count_passed,
                actual: self.metrics.trade_count as f64,
                required: thresholds.min_trade_count as f64,
                comparison: ">=".to_string(),
            },
        );

        // Profit factor
        let profit_factor_passed = self.metrics.profit_factor >= thresholds.min_profit_factor;
        self.threshold_results.insert(
            "profit_factor".to_string(),
            ThresholdResult {
                name: "Profit Factor".to_string(),
                passed: profit_factor_passed,
                actual: self.metrics.profit_factor,
                required: thresholds.min_profit_factor,
                comparison: ">=".to_string(),
            },
        );

        // Slippage
        let slippage_passed = self.metrics.avg_slippage_bps <= thresholds.max_avg_slippage_bps;
        self.threshold_results.insert(
            "avg_slippage".to_string(),
            ThresholdResult {
                name: "Avg Slippage".to_string(),
                passed: slippage_passed,
                actual: self.metrics.avg_slippage_bps,
                required: thresholds.max_avg_slippage_bps,
                comparison: "<=".to_string(),
            },
        );

        // Average trade P&L
        let avg_trade_passed = self.metrics.avg_pnl_bps >= thresholds.min_avg_trade_bps;
        self.threshold_results.insert(
            "avg_trade_bps".to_string(),
            ThresholdResult {
                name: "Avg Trade P&L".to_string(),
                passed: avg_trade_passed,
                actual: self.metrics.avg_pnl_bps,
                required: thresholds.min_avg_trade_bps,
                comparison: ">=".to_string(),
            },
        );

        // Consecutive losses
        let consec_losses_passed =
            self.metrics.max_consecutive_losses <= thresholds.max_consecutive_losses;
        self.threshold_results.insert(
            "consecutive_losses".to_string(),
            ThresholdResult {
                name: "Max Consecutive Losses".to_string(),
                passed: consec_losses_passed,
                actual: self.metrics.max_consecutive_losses as f64,
                required: thresholds.max_consecutive_losses as f64,
                comparison: "<=".to_string(),
            },
        );

        // Overall pass
        self.passed = sharpe_passed
            && drawdown_passed
            && win_rate_passed
            && trade_count_passed
            && profit_factor_passed
            && slippage_passed
            && avg_trade_passed
            && consec_losses_passed;
    }

    /// Check if result passed all thresholds
    pub fn passed_threshold(&self, thresholds: &ValidationThresholds) -> bool {
        self.metrics.sharpe_ratio >= thresholds.min_sharpe
            && self.metrics.max_drawdown_pct <= thresholds.max_drawdown_pct
            && self.metrics.win_rate >= thresholds.min_win_rate
            && self.metrics.trade_count >= thresholds.min_trade_count
            && self.metrics.profit_factor >= thresholds.min_profit_factor
            && self.metrics.avg_slippage_bps <= thresholds.max_avg_slippage_bps
            && self.metrics.avg_pnl_bps >= thresholds.min_avg_trade_bps
            && self.metrics.max_consecutive_losses <= thresholds.max_consecutive_losses
    }

    /// Get the period duration in days
    pub fn period_days(&self) -> f64 {
        (self.period_end - self.period_start).num_days() as f64
    }

    /// Get failed thresholds
    pub fn failed_thresholds(&self) -> Vec<&ThresholdResult> {
        self.threshold_results
            .values()
            .filter(|r| !r.passed)
            .collect()
    }

    /// Get passed thresholds
    pub fn passed_thresholds(&self) -> Vec<&ThresholdResult> {
        self.threshold_results
            .values()
            .filter(|r| r.passed)
            .collect()
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    /// Set validation duration
    pub fn set_duration(&mut self, duration_seconds: f64) {
        self.validation_duration_seconds = duration_seconds;
    }

    /// Add metadata
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Get a summary string
    pub fn summary(&self) -> String {
        format!(
            "{} [{}]: {} trades, {:.1}% win rate, Sharpe {:.2}, DD {:.1}% - {}",
            self.stage_name,
            self.stage_type.code(),
            self.metrics.trade_count,
            self.metrics.win_rate * 100.0,
            self.metrics.sharpe_ratio,
            self.metrics.max_drawdown_pct,
            if self.passed { "PASSED" } else { "FAILED" }
        )
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new(
            ValidationStageType::Backtest,
            "Default".to_string(),
            String::new(),
            Utc::now(),
            Utc::now(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Helper Functions ====================

    fn create_winning_trade(id: &str, pnl_bps: f64) -> TradeResult {
        let entry_price = 100.0;
        let exit_price = entry_price * (1.0 + pnl_bps / 10000.0);
        let entry_time = Utc::now();
        let exit_time = entry_time + Duration::minutes(30);

        TradeResult::new(
            id.to_string(),
            TradeDirection::Long,
            entry_time,
            exit_time,
            entry_price,
            exit_price,
            1.0,
        )
        .with_exit_reason(ExitReason::TakeProfit)
    }

    fn create_losing_trade(id: &str, pnl_bps: f64) -> TradeResult {
        let entry_price = 100.0;
        let exit_price = entry_price * (1.0 - pnl_bps / 10000.0);
        let entry_time = Utc::now();
        let exit_time = entry_time + Duration::minutes(30);

        TradeResult::new(
            id.to_string(),
            TradeDirection::Long,
            entry_time,
            exit_time,
            entry_price,
            exit_price,
            1.0,
        )
        .with_exit_reason(ExitReason::StopLoss)
    }

    fn create_mixed_trades(win_count: usize, loss_count: usize) -> Vec<TradeResult> {
        let mut trades = Vec::new();

        for i in 0..win_count {
            trades.push(create_winning_trade(&format!("W{}", i), 50.0));
        }

        for i in 0..loss_count {
            trades.push(create_losing_trade(&format!("L{}", i), 30.0));
        }

        trades
    }

    fn create_short_trade(id: &str, entry: f64, exit: f64) -> TradeResult {
        let entry_time = Utc::now();
        let exit_time = entry_time + Duration::minutes(15);

        TradeResult::new(
            id.to_string(),
            TradeDirection::Short,
            entry_time,
            exit_time,
            entry,
            exit,
            1.0,
        )
    }

    // ==================== ValidationStageType Tests ====================

    #[test]
    fn test_stage_type_display_name() {
        assert_eq!(ValidationStageType::Backtest.display_name(), "Backtest");
        assert_eq!(ValidationStageType::Forward.display_name(), "Forward");
        assert_eq!(
            ValidationStageType::OutOfSample.display_name(),
            "Out-of-Sample"
        );
        assert_eq!(ValidationStageType::Paper.display_name(), "Paper");
        assert_eq!(ValidationStageType::Live.display_name(), "Live");
    }

    #[test]
    fn test_stage_type_code() {
        assert_eq!(ValidationStageType::Backtest.code(), "BT");
        assert_eq!(ValidationStageType::Forward.code(), "FW");
        assert_eq!(ValidationStageType::OutOfSample.code(), "OOS");
        assert_eq!(ValidationStageType::Paper.code(), "PP");
        assert_eq!(ValidationStageType::Live.code(), "LV");
    }

    #[test]
    fn test_stage_type_is_historical() {
        assert!(ValidationStageType::Backtest.is_historical());
        assert!(ValidationStageType::Forward.is_historical());
        assert!(ValidationStageType::OutOfSample.is_historical());
        assert!(!ValidationStageType::Paper.is_historical());
        assert!(!ValidationStageType::Live.is_historical());
    }

    #[test]
    fn test_stage_type_uses_live_data() {
        assert!(!ValidationStageType::Backtest.uses_live_data());
        assert!(!ValidationStageType::Forward.uses_live_data());
        assert!(!ValidationStageType::OutOfSample.uses_live_data());
        assert!(ValidationStageType::Paper.uses_live_data());
        assert!(ValidationStageType::Live.uses_live_data());
    }

    #[test]
    fn test_stage_type_uses_real_execution() {
        assert!(!ValidationStageType::Backtest.uses_real_execution());
        assert!(!ValidationStageType::Forward.uses_real_execution());
        assert!(!ValidationStageType::OutOfSample.uses_real_execution());
        assert!(!ValidationStageType::Paper.uses_real_execution());
        assert!(ValidationStageType::Live.uses_real_execution());
    }

    #[test]
    fn test_stage_type_pipeline_order() {
        assert_eq!(ValidationStageType::Backtest.pipeline_order(), 1);
        assert_eq!(ValidationStageType::Forward.pipeline_order(), 2);
        assert_eq!(ValidationStageType::OutOfSample.pipeline_order(), 3);
        assert_eq!(ValidationStageType::Paper.pipeline_order(), 4);
        assert_eq!(ValidationStageType::Live.pipeline_order(), 5);
    }

    #[test]
    fn test_stage_type_serialization() {
        for stage in [
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
            ValidationStageType::Paper,
            ValidationStageType::Live,
        ] {
            let json = serde_json::to_string(&stage).unwrap();
            let deserialized: ValidationStageType = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, stage);
        }
    }

    // ==================== TradeDirection Tests ====================

    #[test]
    fn test_trade_direction_opposite() {
        assert_eq!(TradeDirection::Long.opposite(), TradeDirection::Short);
        assert_eq!(TradeDirection::Short.opposite(), TradeDirection::Long);
    }

    #[test]
    fn test_trade_direction_sign() {
        assert_eq!(TradeDirection::Long.sign(), 1.0);
        assert_eq!(TradeDirection::Short.sign(), -1.0);
    }

    #[test]
    fn test_trade_direction_serialization() {
        for dir in [TradeDirection::Long, TradeDirection::Short] {
            let json = serde_json::to_string(&dir).unwrap();
            let deserialized: TradeDirection = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, dir);
        }
    }

    // ==================== ExitReason Tests ====================

    #[test]
    fn test_exit_reason_is_winner() {
        assert!(ExitReason::TakeProfit.is_winner());
        assert!(!ExitReason::StopLoss.is_winner());
        assert!(!ExitReason::SignalReversal.is_winner());
        assert!(!ExitReason::CircuitBreaker.is_winner());
    }

    #[test]
    fn test_exit_reason_is_loser() {
        assert!(!ExitReason::TakeProfit.is_loser());
        assert!(ExitReason::StopLoss.is_loser());
        assert!(!ExitReason::SignalReversal.is_loser());
        assert!(ExitReason::CircuitBreaker.is_loser());
    }

    #[test]
    fn test_exit_reason_is_controlled() {
        assert!(ExitReason::TakeProfit.is_controlled());
        assert!(!ExitReason::StopLoss.is_controlled());
        assert!(ExitReason::SignalReversal.is_controlled());
        assert!(ExitReason::TimeExpiry.is_controlled());
        assert!(ExitReason::Manual.is_controlled());
        assert!(!ExitReason::CircuitBreaker.is_controlled());
        assert!(!ExitReason::EndOfPeriod.is_controlled());
        assert!(ExitReason::PositionClosed.is_controlled());
        assert!(!ExitReason::Unknown.is_controlled());
    }

    #[test]
    fn test_exit_reason_serialization() {
        for reason in [
            ExitReason::TakeProfit,
            ExitReason::StopLoss,
            ExitReason::SignalReversal,
            ExitReason::TimeExpiry,
            ExitReason::Manual,
            ExitReason::CircuitBreaker,
            ExitReason::EndOfPeriod,
            ExitReason::PositionClosed,
            ExitReason::Unknown,
        ] {
            let json = serde_json::to_string(&reason).unwrap();
            let deserialized: ExitReason = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, reason);
        }
    }

    // ==================== TradeResult Tests ====================

    #[test]
    fn test_trade_result_new_long_winning() {
        let trade = TradeResult::new(
            "T1".to_string(),
            TradeDirection::Long,
            Utc::now(),
            Utc::now() + Duration::hours(1),
            100.0,
            105.0,
            10.0,
        );

        assert!(trade.pnl > 0.0);
        assert!(trade.is_winner());
        assert!(!trade.is_loser());
        assert!((trade.return_pct - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_trade_result_new_long_losing() {
        let trade = TradeResult::new(
            "T1".to_string(),
            TradeDirection::Long,
            Utc::now(),
            Utc::now() + Duration::hours(1),
            100.0,
            95.0,
            10.0,
        );

        assert!(trade.pnl < 0.0);
        assert!(!trade.is_winner());
        assert!(trade.is_loser());
        assert!((trade.return_pct - (-5.0)).abs() < 0.01);
    }

    #[test]
    fn test_trade_result_new_short_winning() {
        let trade = create_short_trade("T1", 100.0, 95.0);

        assert!(trade.pnl > 0.0);
        assert!(trade.is_winner());
        assert!((trade.return_pct - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_trade_result_new_short_losing() {
        let trade = create_short_trade("T1", 100.0, 105.0);

        assert!(trade.pnl < 0.0);
        assert!(trade.is_loser());
        assert!((trade.return_pct - (-5.0)).abs() < 0.01);
    }

    #[test]
    fn test_trade_result_breakeven() {
        let trade = TradeResult::new(
            "T1".to_string(),
            TradeDirection::Long,
            Utc::now(),
            Utc::now() + Duration::hours(1),
            100.0,
            100.0,
            10.0,
        );

        assert!((trade.pnl).abs() < 0.01);
        assert!(!trade.is_winner());
        assert!(!trade.is_loser());
    }

    #[test]
    fn test_trade_result_duration() {
        let entry = Utc::now();
        let exit = entry + Duration::hours(2) + Duration::minutes(30);

        let trade = TradeResult::new(
            "T1".to_string(),
            TradeDirection::Long,
            entry,
            exit,
            100.0,
            101.0,
            1.0,
        );

        assert_eq!(trade.duration_seconds(), 2 * 3600 + 30 * 60);
    }

    #[test]
    fn test_trade_result_net_pnl() {
        let trade = TradeResult::new(
            "T1".to_string(),
            TradeDirection::Long,
            Utc::now(),
            Utc::now() + Duration::hours(1),
            100.0,
            105.0,
            1.0,
        )
        .with_commission(1.0);

        assert!((trade.net_pnl() - (trade.pnl - 1.0)).abs() < 0.01);
    }

    #[test]
    fn test_trade_result_risk_reward_ratio() {
        let trade = TradeResult::new(
            "T1".to_string(),
            TradeDirection::Long,
            Utc::now(),
            Utc::now() + Duration::hours(1),
            100.0,
            105.0,
            1.0,
        )
        .with_excursions(20.0, 60.0);

        assert!((trade.risk_reward_ratio() - 3.0).abs() < 0.01);
    }

    #[test]
    fn test_trade_result_risk_reward_ratio_no_mae() {
        let trade = TradeResult::new(
            "T1".to_string(),
            TradeDirection::Long,
            Utc::now(),
            Utc::now() + Duration::hours(1),
            100.0,
            105.0,
            1.0,
        )
        .with_excursions(0.0, 60.0);

        assert!(trade.risk_reward_ratio().is_infinite());
    }

    #[test]
    fn test_trade_result_builders() {
        let trade = TradeResult::new(
            "T1".to_string(),
            TradeDirection::Long,
            Utc::now(),
            Utc::now() + Duration::hours(1),
            100.0,
            105.0,
            1.0,
        )
        .with_exit_reason(ExitReason::TakeProfit)
        .with_research_state("RS123".to_string())
        .with_config("CFG456".to_string())
        .with_slippage(2.5)
        .with_commission(0.5)
        .with_excursions(10.0, 60.0)
        .with_metadata("key".to_string(), "value".to_string());

        assert_eq!(trade.exit_reason, ExitReason::TakeProfit);
        assert_eq!(trade.research_state_id, Some("RS123".to_string()));
        assert_eq!(trade.config_id, Some("CFG456".to_string()));
        assert!((trade.slippage_bps - 2.5).abs() < 0.01);
        assert!((trade.commission - 0.5).abs() < 0.01);
        assert!((trade.mae_bps - 10.0).abs() < 0.01);
        assert!((trade.mfe_bps - 60.0).abs() < 0.01);
        assert_eq!(trade.metadata.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_trade_result_default() {
        let trade = TradeResult::default();

        assert!(trade.trade_id.is_empty());
        assert_eq!(trade.direction, TradeDirection::Long);
        assert!((trade.pnl).abs() < 0.01);
    }

    #[test]
    fn test_trade_result_serialization() {
        let trade = create_winning_trade("T1", 50.0);

        let json = serde_json::to_string(&trade).unwrap();
        let deserialized: TradeResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.trade_id, trade.trade_id);
        assert!((deserialized.pnl - trade.pnl).abs() < 0.01);
    }

    #[test]
    fn test_trade_result_pnl_bps_calculation() {
        let trade = create_winning_trade("T1", 50.0);

        // 50 bps = 0.5% return
        assert!((trade.pnl_bps - 50.0).abs() < 1.0);
        assert!((trade.return_pct - 0.5).abs() < 0.01);
    }

    // ==================== ValidationThresholds Tests ====================

    #[test]
    fn test_thresholds_default() {
        let thresholds = ValidationThresholds::default();

        assert!((thresholds.min_sharpe - 0.5).abs() < 0.01);
        assert!((thresholds.max_drawdown_pct - 20.0).abs() < 0.01);
        assert!((thresholds.min_win_rate - 0.4).abs() < 0.01);
        assert_eq!(thresholds.min_trade_count, 30);
    }

    #[test]
    fn test_thresholds_strict() {
        let thresholds = ValidationThresholds::strict();

        assert!((thresholds.min_sharpe - 1.0).abs() < 0.01);
        assert!((thresholds.max_drawdown_pct - 10.0).abs() < 0.01);
        assert!((thresholds.min_win_rate - 0.5).abs() < 0.01);
        assert_eq!(thresholds.min_trade_count, 100);
    }

    #[test]
    fn test_thresholds_relaxed() {
        let thresholds = ValidationThresholds::relaxed();

        assert!((thresholds.min_sharpe - 0.0).abs() < 0.01);
        assert!((thresholds.max_drawdown_pct - 50.0).abs() < 0.01);
        assert!((thresholds.min_win_rate - 0.3).abs() < 0.01);
        assert_eq!(thresholds.min_trade_count, 10);
    }

    #[test]
    fn test_thresholds_serialization() {
        let thresholds = ValidationThresholds::default();

        let json = serde_json::to_string(&thresholds).unwrap();
        let deserialized: ValidationThresholds = serde_json::from_str(&json).unwrap();

        assert!((deserialized.min_sharpe - thresholds.min_sharpe).abs() < 0.01);
        assert_eq!(deserialized.min_trade_count, thresholds.min_trade_count);
    }

    // ==================== ValidationMetrics Tests ====================

    #[test]
    fn test_metrics_empty_trades() {
        let metrics = ValidationMetrics::from_trades(&[], 30.0);

        assert_eq!(metrics.trade_count, 0);
        assert!((metrics.win_rate).abs() < 0.01);
        assert!((metrics.total_pnl).abs() < 0.01);
    }

    #[test]
    fn test_metrics_all_winners() {
        let trades = vec![
            create_winning_trade("T1", 50.0),
            create_winning_trade("T2", 60.0),
            create_winning_trade("T3", 40.0),
        ];

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert_eq!(metrics.trade_count, 3);
        assert_eq!(metrics.winners, 3);
        assert_eq!(metrics.losers, 0);
        assert!((metrics.win_rate - 1.0).abs() < 0.01);
        assert!(metrics.total_pnl > 0.0);
    }

    #[test]
    fn test_metrics_all_losers() {
        let trades = vec![
            create_losing_trade("T1", 30.0),
            create_losing_trade("T2", 40.0),
            create_losing_trade("T3", 50.0),
        ];

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert_eq!(metrics.trade_count, 3);
        assert_eq!(metrics.winners, 0);
        assert_eq!(metrics.losers, 3);
        assert!((metrics.win_rate).abs() < 0.01);
        assert!(metrics.total_pnl < 0.0);
    }

    #[test]
    fn test_metrics_mixed_trades() {
        let trades = create_mixed_trades(6, 4);

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert_eq!(metrics.trade_count, 10);
        assert_eq!(metrics.winners, 6);
        assert_eq!(metrics.losers, 4);
        assert!((metrics.win_rate - 0.6).abs() < 0.01);
    }

    #[test]
    fn test_metrics_profit_factor() {
        let trades = create_mixed_trades(6, 4);

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        // Profit factor = gross_profit / gross_loss
        assert!(metrics.profit_factor > 0.0);
        let expected_pf = metrics.gross_profit / metrics.gross_loss;
        assert!((metrics.profit_factor - expected_pf).abs() < 0.01);
    }

    #[test]
    fn test_metrics_profit_factor_no_losses() {
        let trades = vec![
            create_winning_trade("T1", 50.0),
            create_winning_trade("T2", 60.0),
        ];

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert!(metrics.profit_factor.is_infinite());
    }

    #[test]
    fn test_metrics_avg_winner_loser() {
        let trades = create_mixed_trades(5, 5);

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert!(metrics.avg_winner > 0.0);
        assert!(metrics.avg_loser < 0.0);
    }

    #[test]
    fn test_metrics_max_winner_loser() {
        let trades = vec![
            create_winning_trade("T1", 50.0),
            create_winning_trade("T2", 100.0),
            create_losing_trade("T3", 30.0),
            create_losing_trade("T4", 60.0),
        ];

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert!(metrics.max_winner > 0.0);
        assert!(metrics.max_loser < 0.0);
    }

    #[test]
    fn test_metrics_consecutive_streaks() {
        // Create specific pattern: WWWLLWW
        let trades = vec![
            create_winning_trade("T1", 50.0),
            create_winning_trade("T2", 50.0),
            create_winning_trade("T3", 50.0),
            create_losing_trade("T4", 30.0),
            create_losing_trade("T5", 30.0),
            create_winning_trade("T6", 50.0),
            create_winning_trade("T7", 50.0),
        ];

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert_eq!(metrics.max_consecutive_wins, 3);
        assert_eq!(metrics.max_consecutive_losses, 2);
    }

    #[test]
    fn test_metrics_long_short_breakdown() {
        let mut trades = Vec::new();

        // 3 long winners, 1 long loser
        trades.push(create_winning_trade("L1", 50.0));
        trades.push(create_winning_trade("L2", 50.0));
        trades.push(create_winning_trade("L3", 50.0));
        trades.push(create_losing_trade("L4", 30.0));

        // 2 short winners, 2 short losers
        trades.push(create_short_trade("S1", 100.0, 95.0)); // winner
        trades.push(create_short_trade("S2", 100.0, 95.0)); // winner
        trades.push(create_short_trade("S3", 100.0, 105.0)); // loser
        trades.push(create_short_trade("S4", 100.0, 105.0)); // loser

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert_eq!(metrics.long_trades, 4);
        assert_eq!(metrics.short_trades, 4);
        assert!((metrics.long_win_rate - 0.75).abs() < 0.01);
        assert!((metrics.short_win_rate - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_metrics_expectancy() {
        let trades = create_mixed_trades(6, 4);

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        let expected = metrics.avg_winner * metrics.win_rate
            + metrics.avg_loser * (1.0 - metrics.win_rate);
        assert!((metrics.expectancy - expected).abs() < 0.01);
    }

    #[test]
    fn test_metrics_sharpe_positive() {
        let trades = create_mixed_trades(8, 2);

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert!(metrics.sharpe_ratio > 0.0);
    }

    #[test]
    fn test_metrics_sharpe_negative() {
        let trades = create_mixed_trades(2, 8);

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        // Sharpe should be negative or low
        assert!(metrics.sharpe_ratio < 1.0);
    }

    #[test]
    fn test_metrics_sortino() {
        let trades = create_mixed_trades(6, 4);

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        // Sortino should typically be >= Sharpe when there are losses
        assert!(metrics.sortino_ratio >= 0.0 || metrics.sortino_ratio.is_infinite());
    }

    #[test]
    fn test_metrics_avg_duration() {
        let trades = vec![
            create_winning_trade("T1", 50.0),
            create_winning_trade("T2", 50.0),
        ];

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        // Duration should be around 30 minutes = 1800 seconds
        assert!(metrics.avg_trade_duration_seconds > 0.0);
    }

    #[test]
    fn test_metrics_commission_slippage() {
        let trades = vec![
            create_winning_trade("T1", 50.0)
                .with_commission(1.0)
                .with_slippage(2.0),
            create_winning_trade("T2", 50.0)
                .with_commission(1.5)
                .with_slippage(3.0),
        ];

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert!((metrics.total_commission - 2.5).abs() < 0.01);
        assert!((metrics.avg_slippage_bps - 2.5).abs() < 0.01);
    }

    #[test]
    fn test_metrics_default() {
        let metrics = ValidationMetrics::default();

        assert_eq!(metrics.trade_count, 0);
        assert!((metrics.sharpe_ratio).abs() < 0.01);
    }

    #[test]
    fn test_metrics_serialization() {
        let trades = create_mixed_trades(5, 5);
        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: ValidationMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.trade_count, metrics.trade_count);
        assert!((deserialized.win_rate - metrics.win_rate).abs() < 0.01);
    }

    // ==================== ValidationResult Tests ====================

    #[test]
    fn test_validation_result_new() {
        let result = ValidationResult::new(
            ValidationStageType::Backtest,
            "BT-2025Q1".to_string(),
            "CFG001".to_string(),
            Utc::now() - Duration::days(90),
            Utc::now(),
        );

        assert_eq!(result.stage_type, ValidationStageType::Backtest);
        assert_eq!(result.stage_name, "BT-2025Q1");
        assert_eq!(result.config_id, "CFG001");
        assert!(!result.id.is_empty());
    }

    #[test]
    fn test_validation_result_with_trades() {
        let trades = create_mixed_trades(6, 4);
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        assert_eq!(result.metrics.trade_count, 10);
        assert_eq!(result.trades.len(), 10);
    }

    #[test]
    fn test_validation_result_period_days() {
        let start = Utc::now() - Duration::days(90);
        let end = Utc::now();

        let result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        );

        assert!((result.period_days() - 90.0).abs() < 1.0);
    }

    #[test]
    fn test_validation_result_evaluate_thresholds_pass() {
        let trades = create_mixed_trades(40, 10); // 80% win rate
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        let thresholds = ValidationThresholds::relaxed();
        result.evaluate_thresholds(thresholds);

        assert!(result.passed);
    }

    #[test]
    fn test_validation_result_evaluate_thresholds_fail() {
        let trades = create_mixed_trades(2, 8); // 20% win rate
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        let thresholds = ValidationThresholds::strict();
        result.evaluate_thresholds(thresholds);

        assert!(!result.passed);
    }

    #[test]
    fn test_validation_result_passed_threshold() {
        let trades = create_mixed_trades(40, 10);
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        let relaxed = ValidationThresholds::relaxed();
        let strict = ValidationThresholds::strict();

        assert!(result.passed_threshold(&relaxed));
        // May or may not pass strict depending on metrics
    }

    #[test]
    fn test_validation_result_threshold_results() {
        let trades = create_mixed_trades(6, 4);
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        result.evaluate_thresholds(ValidationThresholds::default());

        // Should have all threshold results
        assert!(result.threshold_results.contains_key("sharpe_ratio"));
        assert!(result.threshold_results.contains_key("max_drawdown"));
        assert!(result.threshold_results.contains_key("win_rate"));
        assert!(result.threshold_results.contains_key("trade_count"));
        assert!(result.threshold_results.contains_key("profit_factor"));
    }

    #[test]
    fn test_validation_result_failed_thresholds() {
        let trades = create_mixed_trades(2, 8);
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        result.evaluate_thresholds(ValidationThresholds::strict());

        let failed = result.failed_thresholds();
        assert!(!failed.is_empty());
    }

    #[test]
    fn test_validation_result_passed_thresholds() {
        let trades = create_mixed_trades(40, 10);
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        result.evaluate_thresholds(ValidationThresholds::relaxed());

        let passed = result.passed_thresholds();
        assert!(!passed.is_empty());
    }

    #[test]
    fn test_validation_result_warnings() {
        let mut result = ValidationResult::default();

        result.add_warning("Warning 1".to_string());
        result.add_warning("Warning 2".to_string());

        assert_eq!(result.warnings.len(), 2);
        assert_eq!(result.warnings[0], "Warning 1");
    }

    #[test]
    fn test_validation_result_duration() {
        let mut result = ValidationResult::default();

        result.set_duration(123.5);

        assert!((result.validation_duration_seconds - 123.5).abs() < 0.01);
    }

    #[test]
    fn test_validation_result_metadata() {
        let mut result = ValidationResult::default();

        result.add_metadata("key1".to_string(), "value1".to_string());
        result.add_metadata("key2".to_string(), "value2".to_string());

        assert_eq!(result.metadata.get("key1"), Some(&"value1".to_string()));
        assert_eq!(result.metadata.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_validation_result_summary() {
        let trades = create_mixed_trades(6, 4);
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            "BT-Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        result.evaluate_thresholds(ValidationThresholds::relaxed());

        let summary = result.summary();

        assert!(summary.contains("BT-Test"));
        assert!(summary.contains("[BT]"));
        assert!(summary.contains("10 trades"));
    }

    #[test]
    fn test_validation_result_with_research_state() {
        let result = ValidationResult::default().with_research_state("RS123".to_string());

        assert_eq!(result.research_state_id, Some("RS123".to_string()));
    }

    #[test]
    fn test_validation_result_default() {
        let result = ValidationResult::default();

        assert_eq!(result.stage_type, ValidationStageType::Backtest);
        assert!(!result.passed);
        assert!(result.trades.is_empty());
    }

    #[test]
    fn test_validation_result_serialization() {
        let trades = create_mixed_trades(5, 5);
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let mut result = ValidationResult::new(
            ValidationStageType::Forward,
            "FW-Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        result.evaluate_thresholds(ValidationThresholds::default());

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ValidationResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.stage_type, result.stage_type);
        assert_eq!(deserialized.stage_name, result.stage_name);
        assert_eq!(deserialized.metrics.trade_count, result.metrics.trade_count);
        assert_eq!(deserialized.passed, result.passed);
    }

    #[test]
    fn test_validation_result_serialization_empty() {
        let result = ValidationResult::default();

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ValidationResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.trades.len(), 0);
    }

    // ==================== ThresholdResult Tests ====================

    #[test]
    fn test_threshold_result_serialization() {
        let result = ThresholdResult {
            name: "Sharpe Ratio".to_string(),
            passed: true,
            actual: 1.5,
            required: 1.0,
            comparison: ">=".to_string(),
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ThresholdResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, result.name);
        assert_eq!(deserialized.passed, result.passed);
        assert!((deserialized.actual - result.actual).abs() < 0.01);
    }

    // ==================== Integration Tests ====================

    #[test]
    fn test_full_validation_workflow() {
        // Create a realistic set of trades
        let mut trades = Vec::new();
        let base_time = Utc::now() - Duration::days(30);

        for i in 0..50 {
            let is_winner = i % 3 != 0; // 66% win rate
            let pnl_bps = if is_winner { 40.0 + (i % 10) as f64 } else { 25.0 + (i % 5) as f64 };
            let is_short = i % 4 == 0;

            let entry_time = base_time + Duration::hours(i * 2);
            let exit_time = entry_time + Duration::minutes(30 + (i % 60) as i64);
            let entry_price = 100.0;

            // For longs: winner = price goes up, loser = price goes down
            // For shorts: winner = price goes down, loser = price goes up
            let exit_price = if is_short {
                if is_winner {
                    entry_price * (1.0 - pnl_bps / 10000.0) // Short wins when price drops
                } else {
                    entry_price * (1.0 + pnl_bps / 10000.0) // Short loses when price rises
                }
            } else {
                if is_winner {
                    entry_price * (1.0 + pnl_bps / 10000.0) // Long wins when price rises
                } else {
                    entry_price * (1.0 - pnl_bps / 10000.0) // Long loses when price drops
                }
            };

            let trade = TradeResult::new(
                format!("T{}", i),
                if is_short {
                    TradeDirection::Short
                } else {
                    TradeDirection::Long
                },
                entry_time,
                exit_time,
                entry_price,
                exit_price,
                1.0,
            )
            .with_exit_reason(if is_winner {
                ExitReason::TakeProfit
            } else {
                ExitReason::StopLoss
            })
            .with_slippage(0.5)
            .with_commission(0.1)
            .with_research_state("RS001".to_string())
            .with_config("CFG001".to_string());

            trades.push(trade);
        }

        // Create validation result
        let start = base_time;
        let end = Utc::now();

        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            "BT-2025Q1".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades)
        .with_research_state("RS001".to_string());

        result.add_metadata("symbol".to_string(), "BTCUSDT".to_string());
        result.add_warning("Test warning".to_string());
        result.set_duration(5.5);

        // Evaluate thresholds
        result.evaluate_thresholds(ValidationThresholds::relaxed());

        // Verify results
        assert_eq!(result.metrics.trade_count, 50);
        assert!(result.metrics.win_rate > 0.5);
        assert!(result.metrics.long_trades > 0);
        assert!(result.metrics.short_trades > 0);
        assert!(result.passed);

        // Serialize and deserialize
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ValidationResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, result.id);
        assert_eq!(deserialized.metrics.trade_count, 50);
    }

    #[test]
    fn test_validation_across_all_stages() {
        for stage in [
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
            ValidationStageType::Paper,
            ValidationStageType::Live,
        ] {
            let trades = create_mixed_trades(10, 5);
            let start = Utc::now() - Duration::days(7);
            let end = Utc::now();

            let mut result = ValidationResult::new(
                stage,
                format!("{}-Test", stage.code()),
                "CFG001".to_string(),
                start,
                end,
            )
            .with_trades(trades);

            result.evaluate_thresholds(ValidationThresholds::relaxed());

            assert_eq!(result.stage_type, stage);
            assert_eq!(result.metrics.trade_count, 15);
        }
    }

    #[test]
    fn test_threshold_comparison_edge_cases() {
        let trades = create_mixed_trades(30, 0); // All winners
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "CFG001".to_string(),
            start,
            end,
        )
        .with_trades(trades);

        // Edge case: win_rate = 1.0, profit_factor = infinity
        assert!((result.metrics.win_rate - 1.0).abs() < 0.01);
        assert!(result.metrics.profit_factor.is_infinite());
    }

    #[test]
    fn test_metrics_with_zero_pnl_trade() {
        let entry_time = Utc::now();
        let exit_time = entry_time + Duration::hours(1);

        let trades = vec![
            TradeResult::new(
                "T1".to_string(),
                TradeDirection::Long,
                entry_time,
                exit_time,
                100.0,
                100.0, // Zero P&L
                1.0,
            ),
            create_winning_trade("T2", 50.0),
        ];

        let metrics = ValidationMetrics::from_trades(&trades, 30.0);

        assert_eq!(metrics.trade_count, 2);
        assert_eq!(metrics.winners, 1);
        assert_eq!(metrics.losers, 0); // Zero P&L is neither winner nor loser
    }

    #[test]
    fn test_large_trade_count() {
        let mut trades = Vec::new();

        for i in 0..1000 {
            if i % 2 == 0 {
                trades.push(create_winning_trade(&format!("W{}", i), 20.0));
            } else {
                trades.push(create_losing_trade(&format!("L{}", i), 15.0));
            }
        }

        let metrics = ValidationMetrics::from_trades(&trades, 365.0);

        assert_eq!(metrics.trade_count, 1000);
        assert_eq!(metrics.winners, 500);
        assert_eq!(metrics.losers, 500);
        assert!((metrics.win_rate - 0.5).abs() < 0.01);
    }
}

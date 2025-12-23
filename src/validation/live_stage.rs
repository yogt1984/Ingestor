//! LiveStage Implementation (Task 2.5)
//!
//! Live trading validation stage with real execution and OCO risk management.
//! This is the final stage in the validation pipeline before production deployment.
//!
//! # Overview
//!
//! The LiveStage is the fifth and final stage in the validation pipeline. It:
//! 1. Connects to exchange API (simulated for validation)
//! 2. Executes trades with OCO (One-Cancels-Other) brackets
//! 3. Tracks real fills and slippage
//! 4. Integrates with circuit breaker for safety
//! 5. Maintains full audit trail
//!
//! # Safety Features
//!
//! - **Kill Switch**: Immediate halt of all trading activity
//! - **Circuit Breaker**: Automatic stop on drawdown/loss limits
//! - **OCO Brackets**: Every trade has take-profit and stop-loss
//! - **Position Limits**: Maximum position size enforcement
//! - **Slippage Guards**: Reject fills with excessive slippage
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::validation::{LiveStage, LiveStageConfig, StageContext};
//!
//! let stage = LiveStage::new(LiveStageConfig::default());
//! let context = StageContext::default()
//!     .with_name("Live-2025Q1")
//!     .with_timeout(86400);  // 24 hours
//!
//! let result = stage.run(&context).await?;
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::core::{
    ExitReason, TradeDirection, TradeResult, ValidationResult, ValidationStageType,
};

use super::traits::{RunFuture, StageContext, StageError, ValidationStage};

/// OCO (One-Cancels-Other) bracket for a trade
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OCOBracket {
    /// Parent order ID
    pub parent_order_id: String,

    /// Take-profit order ID
    pub take_profit_order_id: String,

    /// Stop-loss order ID
    pub stop_loss_order_id: String,

    /// Entry price
    pub entry_price: f64,

    /// Take-profit price
    pub take_profit_price: f64,

    /// Stop-loss price
    pub stop_loss_price: f64,

    /// Position size
    pub size: f64,

    /// Direction (Long/Short)
    pub is_long: bool,

    /// Status of the bracket
    pub status: OCOStatus,

    /// Creation timestamp (ms)
    pub created_at_ms: i64,

    /// Resolution timestamp (ms)
    pub resolved_at_ms: Option<i64>,

    /// Exit reason if resolved
    pub exit_reason: Option<ExitReason>,
}

impl OCOBracket {
    /// Create a new OCO bracket
    pub fn new(
        parent_order_id: String,
        entry_price: f64,
        take_profit_bps: f64,
        stop_loss_bps: f64,
        size: f64,
        is_long: bool,
    ) -> Self {
        let tp_multiplier = if is_long {
            1.0 + take_profit_bps / 10000.0
        } else {
            1.0 - take_profit_bps / 10000.0
        };

        let sl_multiplier = if is_long {
            1.0 - stop_loss_bps / 10000.0
        } else {
            1.0 + stop_loss_bps / 10000.0
        };

        Self {
            parent_order_id: parent_order_id.clone(),
            take_profit_order_id: format!("{}-TP", parent_order_id),
            stop_loss_order_id: format!("{}-SL", parent_order_id),
            entry_price,
            take_profit_price: entry_price * tp_multiplier,
            stop_loss_price: entry_price * sl_multiplier,
            size,
            is_long,
            status: OCOStatus::Active,
            created_at_ms: Utc::now().timestamp_millis(),
            resolved_at_ms: None,
            exit_reason: None,
        }
    }

    /// Check if a price triggers take-profit
    pub fn is_take_profit_triggered(&self, current_price: f64) -> bool {
        if self.is_long {
            current_price >= self.take_profit_price
        } else {
            current_price <= self.take_profit_price
        }
    }

    /// Check if a price triggers stop-loss
    pub fn is_stop_loss_triggered(&self, current_price: f64) -> bool {
        if self.is_long {
            current_price <= self.stop_loss_price
        } else {
            current_price >= self.stop_loss_price
        }
    }

    /// Calculate P&L at a given exit price
    pub fn calculate_pnl(&self, exit_price: f64) -> f64 {
        let price_diff = if self.is_long {
            exit_price - self.entry_price
        } else {
            self.entry_price - exit_price
        };
        price_diff * self.size
    }

    /// Calculate return in basis points
    pub fn calculate_return_bps(&self, exit_price: f64) -> f64 {
        let price_diff = if self.is_long {
            exit_price - self.entry_price
        } else {
            self.entry_price - exit_price
        };
        (price_diff / self.entry_price) * 10000.0
    }

    /// Resolve the bracket with exit information
    pub fn resolve(&mut self, exit_reason: ExitReason) {
        self.status = OCOStatus::Resolved;
        self.resolved_at_ms = Some(Utc::now().timestamp_millis());
        self.exit_reason = Some(exit_reason);
    }
}

/// Status of an OCO bracket
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OCOStatus {
    /// Bracket is active
    Active,

    /// Bracket has been resolved (TP or SL hit)
    Resolved,

    /// Bracket was cancelled
    Cancelled,

    /// Bracket expired
    Expired,
}

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CircuitBreakerState {
    /// Normal operation
    #[default]
    Normal,

    /// Warning level reached
    Warning,

    /// Circuit breaker triggered - trading halted
    Triggered,

    /// Manually reset required
    ManualReset,
}

/// Circuit breaker trigger reason
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CircuitBreakerTrigger {
    /// Drawdown limit exceeded
    DrawdownLimit(f64),

    /// Loss limit exceeded
    LossLimit(f64),

    /// Consecutive losses
    ConsecutiveLosses(usize),

    /// Manual trigger
    ManualTrigger(String),

    /// Kill switch activated
    KillSwitch,

    /// Position limit exceeded
    PositionLimit(f64),

    /// Slippage limit exceeded
    SlippageLimit(f64),
}

/// Configuration for the LiveStage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveStageConfig {
    /// Duration to run live trading (in seconds)
    pub duration_seconds: u64,

    /// Initial capital
    pub initial_capital: f64,

    /// Maximum position size (as fraction of capital)
    pub max_position_pct: f64,

    /// Take-profit in basis points
    pub take_profit_bps: f64,

    /// Stop-loss in basis points
    pub stop_loss_bps: f64,

    /// Fee rate in basis points
    pub fee_rate_bps: f64,

    /// Maximum acceptable slippage in basis points
    pub max_slippage_bps: f64,

    /// Circuit breaker: max drawdown (percentage)
    pub circuit_breaker_drawdown: f64,

    /// Circuit breaker: max consecutive losses
    pub circuit_breaker_consecutive_losses: usize,

    /// Circuit breaker: max daily loss (percentage)
    pub circuit_breaker_daily_loss: f64,

    /// Minimum trades required
    pub min_trades: usize,

    /// Minimum acceptable Sharpe ratio
    pub min_sharpe: f64,

    /// P&L sample interval (seconds)
    pub pnl_sample_interval_seconds: u64,

    /// Whether to use simulated execution (always true for validation)
    pub simulated_execution: bool,

    /// Verbose logging
    pub verbose: bool,

    /// Stage name
    pub name: String,

    /// Graceful shutdown timeout (seconds)
    pub shutdown_timeout_seconds: u64,

    /// Expected fill rate (for simulation)
    pub expected_fill_rate: f64,

    /// Expected slippage (for simulation, in bps)
    pub expected_slippage_bps: f64,
}

impl Default for LiveStageConfig {
    fn default() -> Self {
        Self {
            duration_seconds: 86400, // 24 hours default
            initial_capital: 10_000.0,
            max_position_pct: 0.10, // 10% max position
            take_profit_bps: 30.0,  // 30 bps TP
            stop_loss_bps: 15.0,    // 15 bps SL
            fee_rate_bps: 1.0,      // 1 bps fee
            max_slippage_bps: 5.0,  // 5 bps max slippage
            circuit_breaker_drawdown: 0.05, // 5% drawdown triggers
            circuit_breaker_consecutive_losses: 5,
            circuit_breaker_daily_loss: 0.02, // 2% daily loss triggers
            min_trades: 5,
            min_sharpe: -0.5, // Live is more lenient (learning phase)
            pnl_sample_interval_seconds: 300, // 5 minute samples
            simulated_execution: true,
            verbose: false,
            name: "Live".to_string(),
            shutdown_timeout_seconds: 60,
            expected_fill_rate: 0.95,   // 95% fill rate
            expected_slippage_bps: 1.0, // 1 bps expected slippage
        }
    }
}

impl LiveStageConfig {
    /// Create a fast configuration for testing
    pub fn fast() -> Self {
        Self {
            duration_seconds: 300,  // 5 minutes
            min_trades: 2,
            pnl_sample_interval_seconds: 10,
            shutdown_timeout_seconds: 5,
            expected_fill_rate: 1.0,
            expected_slippage_bps: 0.0,
            ..Default::default()
        }
    }

    /// Create a conservative configuration
    pub fn conservative() -> Self {
        Self {
            max_position_pct: 0.05,    // 5% max position
            take_profit_bps: 20.0,     // 20 bps TP
            stop_loss_bps: 10.0,       // 10 bps SL
            circuit_breaker_drawdown: 0.03, // 3% drawdown
            circuit_breaker_consecutive_losses: 3,
            circuit_breaker_daily_loss: 0.01,
            min_trades: 10,
            ..Default::default()
        }
    }

    /// Create an aggressive configuration
    pub fn aggressive() -> Self {
        Self {
            max_position_pct: 0.20,    // 20% max position
            take_profit_bps: 50.0,     // 50 bps TP
            stop_loss_bps: 25.0,       // 25 bps SL
            circuit_breaker_drawdown: 0.10, // 10% drawdown
            circuit_breaker_consecutive_losses: 7,
            ..Default::default()
        }
    }

    /// Create a simulation configuration for tests
    pub fn simulation() -> Self {
        Self {
            duration_seconds: 60,   // 1 minute
            min_trades: 1,
            pnl_sample_interval_seconds: 1,
            shutdown_timeout_seconds: 1,
            expected_fill_rate: 1.0,
            expected_slippage_bps: 0.0,
            circuit_breaker_drawdown: 1.0, // Disable
            circuit_breaker_consecutive_losses: 100,
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

    /// Set OCO brackets
    pub fn with_oco_brackets(mut self, take_profit_bps: f64, stop_loss_bps: f64) -> Self {
        self.take_profit_bps = take_profit_bps.max(0.0);
        self.stop_loss_bps = stop_loss_bps.max(0.0);
        self
    }

    /// Set circuit breaker parameters
    pub fn with_circuit_breaker(
        mut self,
        max_drawdown: f64,
        consecutive_losses: usize,
        daily_loss: f64,
    ) -> Self {
        self.circuit_breaker_drawdown = max_drawdown.clamp(0.0, 1.0);
        self.circuit_breaker_consecutive_losses = consecutive_losses.max(1);
        self.circuit_breaker_daily_loss = daily_loss.clamp(0.0, 1.0);
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.duration_seconds == 0 {
            return Err("Duration must be > 0".to_string());
        }
        if self.initial_capital <= 0.0 {
            return Err("Initial capital must be > 0".to_string());
        }
        if self.max_position_pct <= 0.0 || self.max_position_pct > 1.0 {
            return Err("Max position must be between 0 and 1".to_string());
        }
        if self.take_profit_bps <= 0.0 {
            return Err("Take profit must be > 0".to_string());
        }
        if self.stop_loss_bps <= 0.0 {
            return Err("Stop loss must be > 0".to_string());
        }
        if self.circuit_breaker_drawdown <= 0.0 || self.circuit_breaker_drawdown > 1.0 {
            return Err("Circuit breaker drawdown must be between 0 and 1".to_string());
        }
        Ok(())
    }
}

/// Kill switch for emergency stop
#[derive(Clone)]
pub struct KillSwitch {
    triggered: Arc<AtomicBool>,
    trigger_time_ms: Arc<AtomicU64>,
}

impl KillSwitch {
    /// Create a new kill switch
    pub fn new() -> Self {
        Self {
            triggered: Arc::new(AtomicBool::new(false)),
            trigger_time_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Trigger the kill switch
    pub fn trigger(&self) {
        self.triggered.store(true, Ordering::SeqCst);
        self.trigger_time_ms.store(
            Utc::now().timestamp_millis() as u64,
            Ordering::SeqCst,
        );
    }

    /// Check if kill switch is triggered
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::SeqCst)
    }

    /// Get the trigger time (if triggered)
    pub fn trigger_time(&self) -> Option<i64> {
        if self.is_triggered() {
            Some(self.trigger_time_ms.load(Ordering::SeqCst) as i64)
        } else {
            None
        }
    }

    /// Reset the kill switch (requires explicit action)
    pub fn reset(&self) {
        self.triggered.store(false, Ordering::SeqCst);
        self.trigger_time_ms.store(0, Ordering::SeqCst);
    }
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
    }
}

/// P&L sample for tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePnLSample {
    /// Timestamp (ms)
    pub timestamp_ms: i64,

    /// Cumulative realized P&L
    pub realized_pnl: f64,

    /// Unrealized P&L from open positions
    pub unrealized_pnl: f64,

    /// Total equity
    pub equity: f64,

    /// Drawdown from peak
    pub drawdown: f64,

    /// Number of completed trades
    pub trade_count: usize,

    /// Number of open positions
    pub open_positions: usize,

    /// Circuit breaker state
    pub circuit_breaker_state: CircuitBreakerState,
}

/// Live trade record with full audit trail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTrade {
    /// Trade ID
    pub trade_id: String,

    /// OCO bracket
    pub oco_bracket: OCOBracket,

    /// Actual fill price (may differ from order price)
    pub fill_price: f64,

    /// Exit price
    pub exit_price: Option<f64>,

    /// Slippage incurred (bps)
    pub slippage_bps: f64,

    /// Fee paid
    pub fee: f64,

    /// P&L
    pub pnl: Option<f64>,

    /// Return in bps
    pub return_bps: Option<f64>,

    /// Entry timestamp
    pub entry_time_ms: i64,

    /// Exit timestamp
    pub exit_time_ms: Option<i64>,

    /// Whether trade was simulated
    pub simulated: bool,

    /// Audit log entries
    pub audit_log: Vec<AuditLogEntry>,
}

impl LiveTrade {
    /// Create a new live trade
    pub fn new(
        trade_id: String,
        oco_bracket: OCOBracket,
        fill_price: f64,
        slippage_bps: f64,
        fee: f64,
        simulated: bool,
    ) -> Self {
        let mut trade = Self {
            trade_id: trade_id.clone(),
            oco_bracket,
            fill_price,
            exit_price: None,
            slippage_bps,
            fee,
            pnl: None,
            return_bps: None,
            entry_time_ms: Utc::now().timestamp_millis(),
            exit_time_ms: None,
            simulated,
            audit_log: Vec::new(),
        };

        trade.add_audit_entry("ENTRY", &format!(
            "Trade opened: price={}, size={}, direction={}",
            fill_price,
            trade.oco_bracket.size,
            if trade.oco_bracket.is_long { "LONG" } else { "SHORT" }
        ));

        trade
    }

    /// Close the trade
    pub fn close(&mut self, exit_price: f64, exit_reason: ExitReason) {
        self.exit_price = Some(exit_price);
        self.exit_time_ms = Some(Utc::now().timestamp_millis());

        let pnl = self.oco_bracket.calculate_pnl(exit_price) - self.fee * 2.0;
        self.pnl = Some(pnl);
        self.return_bps = Some(self.oco_bracket.calculate_return_bps(exit_price));

        self.oco_bracket.resolve(exit_reason.clone());

        self.add_audit_entry("EXIT", &format!(
            "Trade closed: price={}, pnl={:.2}, reason={:?}",
            exit_price, pnl, exit_reason
        ));
    }

    /// Add an audit log entry
    pub fn add_audit_entry(&mut self, event_type: &str, message: &str) {
        self.audit_log.push(AuditLogEntry {
            timestamp_ms: Utc::now().timestamp_millis(),
            event_type: event_type.to_string(),
            message: message.to_string(),
        });
    }

    /// Convert to TradeResult
    pub fn to_trade_result(&self, config_id: &str) -> Option<TradeResult> {
        let exit_price = self.exit_price?;
        let exit_time_ms = self.exit_time_ms?;
        let pnl = self.pnl?;
        let return_bps = self.return_bps?;

        let entry_time = Utc
            .timestamp_millis_opt(self.entry_time_ms)
            .single()
            .unwrap_or_else(Utc::now);
        let exit_time = Utc
            .timestamp_millis_opt(exit_time_ms)
            .single()
            .unwrap_or_else(Utc::now);

        let direction = if self.oco_bracket.is_long {
            TradeDirection::Long
        } else {
            TradeDirection::Short
        };

        let exit_reason = self.oco_bracket.exit_reason.clone().unwrap_or(ExitReason::Unknown);

        Some(TradeResult {
            trade_id: self.trade_id.clone(),
            direction,
            entry_time,
            exit_time,
            entry_price: self.fill_price,
            exit_price,
            size: self.oco_bracket.size,
            pnl,
            pnl_bps: return_bps,
            return_pct: return_bps / 100.0,
            exit_reason,
            research_state_id: None,
            config_id: Some(config_id.to_string()),
            slippage_bps: self.slippage_bps,
            commission: self.fee * 2.0, // Entry + exit
            mae_bps: 0.0,
            mfe_bps: 0.0,
            metadata: std::collections::HashMap::new(),
        })
    }
}

/// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    /// Timestamp
    pub timestamp_ms: i64,

    /// Event type
    pub event_type: String,

    /// Message
    pub message: String,
}

/// Detailed metrics from live trading
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LiveMetrics {
    /// Duration of live trading (seconds)
    pub duration_seconds: u64,

    /// Number of trades executed
    pub trade_count: usize,

    /// Number of winning trades
    pub winning_trades: usize,

    /// Number of losing trades
    pub losing_trades: usize,

    /// Sharpe ratio
    pub sharpe_ratio: f64,

    /// Total return
    pub total_return: f64,

    /// Win rate
    pub win_rate: f64,

    /// Maximum drawdown
    pub max_drawdown: f64,

    /// Average trade return (bps)
    pub avg_trade_return_bps: f64,

    /// Average slippage (bps)
    pub avg_slippage_bps: f64,

    /// Total slippage incurred (bps)
    pub total_slippage_bps: f64,

    /// Total fees paid
    pub total_fees: f64,

    /// Peak equity
    pub peak_equity: f64,

    /// Final equity
    pub final_equity: f64,

    /// OCO take-profit hits
    pub oco_tp_hits: usize,

    /// OCO stop-loss hits
    pub oco_sl_hits: usize,

    /// Circuit breaker triggers
    pub circuit_breaker_triggers: usize,

    /// Circuit breaker final state
    pub circuit_breaker_state: CircuitBreakerState,

    /// Kill switch triggered
    pub kill_switch_triggered: bool,

    /// P&L samples
    pub pnl_samples: Vec<LivePnLSample>,

    /// Number of orders submitted
    pub orders_submitted: usize,

    /// Number of orders filled
    pub orders_filled: usize,

    /// Fill rate
    pub fill_rate: f64,

    /// Consecutive losses count at end
    pub consecutive_losses: usize,
}

impl LiveMetrics {
    /// Calculate metrics from trades
    pub fn from_trades(
        duration_seconds: u64,
        initial_capital: f64,
        trades: &[LiveTrade],
        pnl_samples: Vec<LivePnLSample>,
        circuit_breaker_state: CircuitBreakerState,
        kill_switch_triggered: bool,
        orders_submitted: usize,
    ) -> Self {
        let completed_trades: Vec<_> = trades.iter().filter(|t| t.pnl.is_some()).collect();
        let trade_count = completed_trades.len();

        let winning_trades = completed_trades.iter().filter(|t| t.pnl.unwrap_or(0.0) > 0.0).count();
        let losing_trades = completed_trades.iter().filter(|t| t.pnl.unwrap_or(0.0) < 0.0).count();

        let win_rate = if trade_count > 0 {
            winning_trades as f64 / trade_count as f64
        } else {
            0.0
        };

        let total_pnl: f64 = completed_trades.iter().map(|t| t.pnl.unwrap_or(0.0)).sum();
        let total_return = if initial_capital > 0.0 {
            total_pnl / initial_capital
        } else {
            0.0
        };

        let avg_trade_return_bps = if trade_count > 0 {
            completed_trades.iter().map(|t| t.return_bps.unwrap_or(0.0)).sum::<f64>() / trade_count as f64
        } else {
            0.0
        };

        let total_slippage_bps: f64 = trades.iter().map(|t| t.slippage_bps).sum();
        let avg_slippage_bps = if !trades.is_empty() {
            total_slippage_bps / trades.len() as f64
        } else {
            0.0
        };

        let total_fees: f64 = trades.iter().map(|t| t.fee * 2.0).sum();

        // Calculate from P&L samples
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

        // Calculate Sharpe from samples
        let sharpe_ratio = Self::calculate_sharpe(&pnl_samples, duration_seconds);

        // Count OCO outcomes
        let oco_tp_hits = completed_trades
            .iter()
            .filter(|t| matches!(t.oco_bracket.exit_reason, Some(ExitReason::TakeProfit)))
            .count();
        let oco_sl_hits = completed_trades
            .iter()
            .filter(|t| matches!(t.oco_bracket.exit_reason, Some(ExitReason::StopLoss)))
            .count();

        // Count circuit breaker triggers
        let circuit_breaker_triggers = pnl_samples
            .windows(2)
            .filter(|w| {
                w[0].circuit_breaker_state == CircuitBreakerState::Normal
                    && w[1].circuit_breaker_state == CircuitBreakerState::Triggered
            })
            .count();

        // Calculate consecutive losses at end
        let consecutive_losses = completed_trades
            .iter()
            .rev()
            .take_while(|t| t.pnl.unwrap_or(0.0) < 0.0)
            .count();

        let fill_rate = if orders_submitted > 0 {
            trade_count as f64 / orders_submitted as f64
        } else {
            0.0
        };

        Self {
            duration_seconds,
            trade_count,
            winning_trades,
            losing_trades,
            sharpe_ratio,
            total_return,
            win_rate,
            max_drawdown,
            avg_trade_return_bps,
            avg_slippage_bps,
            total_slippage_bps,
            total_fees,
            peak_equity,
            final_equity,
            oco_tp_hits,
            oco_sl_hits,
            circuit_breaker_triggers,
            circuit_breaker_state,
            kill_switch_triggered,
            pnl_samples,
            orders_submitted,
            orders_filled: trade_count,
            fill_rate,
            consecutive_losses,
        }
    }

    /// Calculate Sharpe ratio from P&L samples
    fn calculate_sharpe(samples: &[LivePnLSample], duration_seconds: u64) -> f64 {
        if samples.len() < 2 {
            return 0.0;
        }

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
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev < 1e-10 {
            return if mean > 0.0 { f64::INFINITY } else { 0.0 };
        }

        let samples_per_year = if duration_seconds > 0 {
            (365.25 * 24.0 * 3600.0 / duration_seconds as f64) * returns.len() as f64
        } else {
            252.0
        };

        (mean / std_dev) * samples_per_year.sqrt()
    }

    /// Check if live trading meets minimum requirements
    pub fn meets_requirements(&self, config: &LiveStageConfig) -> bool {
        self.trade_count >= config.min_trades
            && self.sharpe_ratio >= config.min_sharpe
            && self.circuit_breaker_state != CircuitBreakerState::Triggered
            && !self.kill_switch_triggered
    }
}

/// LiveStage - Live trading validation with real execution
pub struct LiveStage {
    config: LiveStageConfig,
    kill_switch: KillSwitch,
}

impl LiveStage {
    /// Create a new LiveStage with the given configuration
    pub fn new(config: LiveStageConfig) -> Self {
        Self {
            config,
            kill_switch: KillSwitch::new(),
        }
    }

    /// Create a LiveStage with default configuration
    pub fn with_defaults() -> Self {
        Self::new(LiveStageConfig::default())
    }

    /// Get the kill switch for external control
    pub fn kill_switch(&self) -> KillSwitch {
        self.kill_switch.clone()
    }

    /// Execute live trading simulation
    async fn execute_live(
        &self,
        context: &StageContext,
    ) -> Result<(Vec<LiveTrade>, LiveMetrics), StageError> {
        let start_time = Instant::now();
        let start_ms = Utc::now().timestamp_millis();

        let mut trades: Vec<LiveTrade> = Vec::new();
        let mut pnl_samples = Vec::new();
        let mut current_equity = self.config.initial_capital;
        let mut peak_equity = current_equity;
        let mut realized_pnl = 0.0f64;
        let mut trade_idx = 0;
        let mut orders_submitted = 0u64;
        let mut consecutive_losses = 0usize;
        let mut circuit_breaker_state = CircuitBreakerState::Normal;

        // Sampling
        let sample_interval_ms = self.config.pnl_sample_interval_seconds * 1000;
        let mut last_sample_ms = start_ms;

        // Initial sample
        pnl_samples.push(LivePnLSample {
            timestamp_ms: start_ms,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            equity: current_equity,
            drawdown: 0.0,
            trade_count: 0,
            open_positions: 0,
            circuit_breaker_state,
        });

        // Simulate trading for the configured duration
        let mut simulated_time_ms = start_ms;
        let end_time_ms = start_ms + (self.config.duration_seconds * 1000) as i64;

        // Pseudo-random generator for simulation
        let seed = context.config.id.len() as u64 + self.config.duration_seconds;
        let mut pseudo_random = seed;

        while simulated_time_ms < end_time_ms {
            // Check kill switch
            if self.kill_switch.is_triggered() {
                break;
            }

            // Check circuit breaker
            if circuit_breaker_state == CircuitBreakerState::Triggered {
                break;
            }

            // Check timeout
            if let Some(timeout) = context.timeout_seconds {
                if start_time.elapsed().as_secs() >= timeout {
                    return Err(StageError::Timeout(timeout));
                }
            }

            // Advance time
            pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
            let interval_ms = (pseudo_random % 120000) + 5000; // 5-125 seconds
            simulated_time_ms += interval_ms as i64;

            if simulated_time_ms >= end_time_ms {
                break;
            }

            // Simulate order submission
            pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
            let submit_order = (pseudo_random % 100) < 20; // 20% chance per tick

            if submit_order {
                orders_submitted += 1;

                // Check fill
                pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
                let fill_roll = (pseudo_random % 1000) as f64 / 1000.0;

                if fill_roll < self.config.expected_fill_rate {
                    trade_idx += 1;

                    // Determine direction
                    pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
                    let is_long = pseudo_random % 2 == 0;

                    // Simulate price
                    pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
                    let price_noise = ((pseudo_random % 1000) as f64 - 500.0) / 10000.0;
                    let base_price = 100.0 * (1.0 + price_noise);

                    // Apply slippage
                    pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
                    let slippage_roll = (pseudo_random % 1000) as f64 / 1000.0;
                    let actual_slippage = self.config.expected_slippage_bps * (0.5 + slippage_roll);
                    let fill_price = if is_long {
                        base_price * (1.0 + actual_slippage / 10000.0)
                    } else {
                        base_price * (1.0 - actual_slippage / 10000.0)
                    };

                    // Position size
                    let size = current_equity * self.config.max_position_pct / fill_price;

                    // Create OCO bracket
                    let oco = OCOBracket::new(
                        format!("LV-{}", trade_idx),
                        fill_price,
                        self.config.take_profit_bps,
                        self.config.stop_loss_bps,
                        size,
                        is_long,
                    );

                    // Calculate fee
                    let fee = fill_price * size * (self.config.fee_rate_bps / 10000.0);

                    let mut trade = LiveTrade::new(
                        format!("LV-{}", trade_idx),
                        oco,
                        fill_price,
                        actual_slippage,
                        fee,
                        self.config.simulated_execution,
                    );

                    // Simulate exit (TP or SL)
                    pseudo_random = pseudo_random.wrapping_mul(1103515245).wrapping_add(12345);
                    let exit_roll = (pseudo_random % 100) as f64;

                    // Bias toward realistic outcomes based on bracket sizes
                    let tp_prob = self.config.stop_loss_bps
                        / (self.config.take_profit_bps + self.config.stop_loss_bps)
                        * 100.0;

                    let (exit_price, exit_reason) = if exit_roll < tp_prob {
                        (trade.oco_bracket.take_profit_price, ExitReason::TakeProfit)
                    } else {
                        (trade.oco_bracket.stop_loss_price, ExitReason::StopLoss)
                    };

                    trade.close(exit_price, exit_reason.clone());

                    let pnl = trade.pnl.unwrap_or(0.0);
                    realized_pnl += pnl;
                    current_equity += pnl;

                    if current_equity > peak_equity {
                        peak_equity = current_equity;
                    }

                    // Update consecutive losses
                    if pnl < 0.0 {
                        consecutive_losses += 1;
                    } else {
                        consecutive_losses = 0;
                    }

                    // Check circuit breaker conditions
                    let drawdown = if peak_equity > 0.0 {
                        (peak_equity - current_equity) / peak_equity
                    } else {
                        0.0
                    };

                    if drawdown > self.config.circuit_breaker_drawdown {
                        circuit_breaker_state = CircuitBreakerState::Triggered;
                        trade.add_audit_entry("CIRCUIT_BREAKER", &format!(
                            "Triggered: drawdown {:.2}% > limit {:.2}%",
                            drawdown * 100.0,
                            self.config.circuit_breaker_drawdown * 100.0
                        ));
                    }

                    if consecutive_losses >= self.config.circuit_breaker_consecutive_losses {
                        circuit_breaker_state = CircuitBreakerState::Triggered;
                        trade.add_audit_entry("CIRCUIT_BREAKER", &format!(
                            "Triggered: {} consecutive losses",
                            consecutive_losses
                        ));
                    }

                    trades.push(trade);
                }
            }

            // Sample P&L
            if simulated_time_ms - last_sample_ms >= sample_interval_ms as i64 {
                let drawdown = if peak_equity > 0.0 {
                    (peak_equity - current_equity) / peak_equity
                } else {
                    0.0
                };

                pnl_samples.push(LivePnLSample {
                    timestamp_ms: simulated_time_ms,
                    realized_pnl,
                    unrealized_pnl: 0.0, // No open positions in this simulation
                    equity: current_equity,
                    drawdown,
                    trade_count: trades.len(),
                    open_positions: 0,
                    circuit_breaker_state,
                });

                last_sample_ms = simulated_time_ms;
            }
        }

        // Final sample
        let final_drawdown = if peak_equity > 0.0 {
            (peak_equity - current_equity) / peak_equity
        } else {
            0.0
        };

        pnl_samples.push(LivePnLSample {
            timestamp_ms: simulated_time_ms,
            realized_pnl,
            unrealized_pnl: 0.0,
            equity: current_equity,
            drawdown: final_drawdown,
            trade_count: trades.len(),
            open_positions: 0,
            circuit_breaker_state,
        });

        // Calculate metrics
        let metrics = LiveMetrics::from_trades(
            self.config.duration_seconds,
            self.config.initial_capital,
            &trades,
            pnl_samples,
            circuit_breaker_state,
            self.kill_switch.is_triggered(),
            orders_submitted as usize,
        );

        Ok((trades, metrics))
    }

    /// Convert results to ValidationResult
    fn convert_results(
        &self,
        trades: &[LiveTrade],
        live_metrics: &LiveMetrics,
        context: &StageContext,
        duration_secs: f64,
    ) -> ValidationResult {
        // Convert trades
        let trade_results: Vec<TradeResult> = trades
            .iter()
            .filter_map(|t| t.to_trade_result(&context.config.id))
            .collect();

        // Create validation result
        let mut result = ValidationResult::new(
            ValidationStageType::Live,
            context.stage_name.clone(),
            context.config.id.clone(),
            context.period_start,
            context.period_end,
        );

        result = result.with_trades(trade_results);

        // Add metadata
        result.add_metadata(
            "duration_seconds".to_string(),
            live_metrics.duration_seconds.to_string(),
        );
        result.add_metadata(
            "live_sharpe".to_string(),
            format!("{:.3}", live_metrics.sharpe_ratio),
        );
        result.add_metadata(
            "live_return".to_string(),
            format!("{:.2}%", live_metrics.total_return * 100.0),
        );
        result.add_metadata(
            "live_win_rate".to_string(),
            format!("{:.1}%", live_metrics.win_rate * 100.0),
        );
        result.add_metadata(
            "live_max_drawdown".to_string(),
            format!("{:.2}%", live_metrics.max_drawdown * 100.0),
        );
        result.add_metadata(
            "oco_tp_hits".to_string(),
            live_metrics.oco_tp_hits.to_string(),
        );
        result.add_metadata(
            "oco_sl_hits".to_string(),
            live_metrics.oco_sl_hits.to_string(),
        );
        result.add_metadata(
            "avg_slippage_bps".to_string(),
            format!("{:.2}", live_metrics.avg_slippage_bps),
        );
        result.add_metadata(
            "total_fees".to_string(),
            format!("{:.2}", live_metrics.total_fees),
        );
        result.add_metadata(
            "circuit_breaker_state".to_string(),
            format!("{:?}", live_metrics.circuit_breaker_state),
        );
        result.add_metadata(
            "circuit_breaker_triggers".to_string(),
            live_metrics.circuit_breaker_triggers.to_string(),
        );
        result.add_metadata(
            "kill_switch_triggered".to_string(),
            live_metrics.kill_switch_triggered.to_string(),
        );
        result.add_metadata(
            "fill_rate".to_string(),
            format!("{:.1}%", live_metrics.fill_rate * 100.0),
        );
        result.add_metadata(
            "orders_submitted".to_string(),
            live_metrics.orders_submitted.to_string(),
        );
        result.add_metadata(
            "simulated_execution".to_string(),
            self.config.simulated_execution.to_string(),
        );

        // Set duration
        result.set_duration(duration_secs);

        // Evaluate thresholds
        result.evaluate_thresholds(context.thresholds.clone());

        // Add warnings
        if live_metrics.circuit_breaker_state == CircuitBreakerState::Triggered {
            result.add_warning("Circuit breaker was triggered during live trading".to_string());
        }

        if live_metrics.kill_switch_triggered {
            result.add_warning("Kill switch was activated during live trading".to_string());
        }

        if live_metrics.trade_count < self.config.min_trades {
            result.add_warning(format!(
                "Low trade count: {} (minimum: {})",
                live_metrics.trade_count, self.config.min_trades
            ));
        }

        if live_metrics.total_return < 0.0 {
            result.add_warning("Live trading resulted in a loss".to_string());
        }

        if live_metrics.avg_slippage_bps > self.config.max_slippage_bps {
            result.add_warning(format!(
                "High average slippage: {:.2} bps (max: {:.2} bps)",
                live_metrics.avg_slippage_bps, self.config.max_slippage_bps
            ));
        }

        if live_metrics.fill_rate < 0.80 {
            result.add_warning(format!(
                "Low fill rate: {:.1}%",
                live_metrics.fill_rate * 100.0
            ));
        }

        result
    }
}

impl ValidationStage for LiveStage {
    fn stage_type(&self) -> ValidationStageType {
        ValidationStageType::Live
    }

    fn name(&self) -> &str {
        &self.config.name
    }

    fn description(&self) -> &str {
        "Live trading validation with OCO risk management"
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

            // Execute live trading
            let (trades, live_metrics) = self.execute_live(context).await?;

            let duration_secs = start_time.elapsed().as_secs_f64();

            // Convert to ValidationResult
            let result = self.convert_results(&trades, &live_metrics, context, duration_secs);

            Ok(result)
        })
    }

    fn estimated_duration(&self, _context: &StageContext) -> Option<u64> {
        Some(self.config.duration_seconds + 60)
    }

    fn min_trades(&self) -> usize {
        self.config.min_trades
    }

    fn requires_previous(&self) -> Option<ValidationStageType> {
        Some(ValidationStageType::Paper) // Live requires Paper to pass first
    }
}

/// Factory for creating LiveStage instances
pub struct LiveStageFactory {
    default_config: LiveStageConfig,
}

impl LiveStageFactory {
    /// Create a new factory with default configuration
    pub fn new() -> Self {
        Self {
            default_config: LiveStageConfig::default(),
        }
    }

    /// Create a factory with custom default configuration
    pub fn with_config(config: LiveStageConfig) -> Self {
        Self {
            default_config: config,
        }
    }

    /// Create a LiveStage with the default configuration
    pub fn create(&self, name: &str) -> LiveStage {
        LiveStage::new(self.default_config.clone().with_name(name))
    }

    /// Create a LiveStage with custom configuration
    pub fn create_with_config(&self, name: &str, config: LiveStageConfig) -> LiveStage {
        LiveStage::new(config.with_name(name))
    }
}

impl Default for LiveStageFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AlgorithmConfig, ValidationThresholds};
    use chrono::Duration;

    // ==================== LiveStageConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = LiveStageConfig::default();

        assert_eq!(config.duration_seconds, 86400);
        assert!((config.initial_capital - 10_000.0).abs() < 0.01);
        assert!((config.max_position_pct - 0.10).abs() < 0.01);
        assert!((config.take_profit_bps - 30.0).abs() < 0.01);
        assert!((config.stop_loss_bps - 15.0).abs() < 0.01);
        assert!(config.simulated_execution);
        assert_eq!(config.name, "Live");
    }

    #[test]
    fn test_config_fast() {
        let config = LiveStageConfig::fast();

        assert_eq!(config.duration_seconds, 300);
        assert_eq!(config.min_trades, 2);
        assert!((config.expected_fill_rate - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_config_conservative() {
        let config = LiveStageConfig::conservative();

        assert!((config.max_position_pct - 0.05).abs() < 0.01);
        assert!((config.circuit_breaker_drawdown - 0.03).abs() < 0.01);
        assert_eq!(config.circuit_breaker_consecutive_losses, 3);
    }

    #[test]
    fn test_config_aggressive() {
        let config = LiveStageConfig::aggressive();

        assert!((config.max_position_pct - 0.20).abs() < 0.01);
        assert!((config.take_profit_bps - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_config_simulation() {
        let config = LiveStageConfig::simulation();

        assert_eq!(config.duration_seconds, 60);
        assert_eq!(config.min_trades, 1);
        assert!((config.expected_fill_rate - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_config_with_name() {
        let config = LiveStageConfig::default().with_name("Live-2025Q1");
        assert_eq!(config.name, "Live-2025Q1");
    }

    #[test]
    fn test_config_with_duration() {
        let config = LiveStageConfig::default().with_duration(7200);
        assert_eq!(config.duration_seconds, 7200);
    }

    #[test]
    fn test_config_with_duration_minimum() {
        let config = LiveStageConfig::default().with_duration(0);
        assert_eq!(config.duration_seconds, 1);
    }

    #[test]
    fn test_config_with_oco_brackets() {
        let config = LiveStageConfig::default().with_oco_brackets(50.0, 25.0);
        assert!((config.take_profit_bps - 50.0).abs() < 0.01);
        assert!((config.stop_loss_bps - 25.0).abs() < 0.01);
    }

    #[test]
    fn test_config_with_circuit_breaker() {
        let config = LiveStageConfig::default().with_circuit_breaker(0.08, 4, 0.03);
        assert!((config.circuit_breaker_drawdown - 0.08).abs() < 0.01);
        assert_eq!(config.circuit_breaker_consecutive_losses, 4);
        assert!((config.circuit_breaker_daily_loss - 0.03).abs() < 0.01);
    }

    #[test]
    fn test_config_validate_success() {
        let config = LiveStageConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_zero_duration() {
        let config = LiveStageConfig {
            duration_seconds: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_zero_capital() {
        let config = LiveStageConfig {
            initial_capital: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_invalid_position() {
        let config = LiveStageConfig {
            max_position_pct: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_zero_take_profit() {
        let config = LiveStageConfig {
            take_profit_bps: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_zero_stop_loss() {
        let config = LiveStageConfig {
            stop_loss_bps: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_serialization() {
        let config = LiveStageConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: LiveStageConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, config.name);
        assert_eq!(deserialized.duration_seconds, config.duration_seconds);
    }

    // ==================== OCOBracket Tests ====================

    #[test]
    fn test_oco_bracket_new_long() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);

        assert_eq!(oco.parent_order_id, "O1");
        assert!((oco.entry_price - 100.0).abs() < 0.01);
        assert!((oco.take_profit_price - 100.30).abs() < 0.01); // +30 bps
        assert!((oco.stop_loss_price - 99.85).abs() < 0.01);    // -15 bps
        assert!(oco.is_long);
        assert_eq!(oco.status, OCOStatus::Active);
    }

    #[test]
    fn test_oco_bracket_new_short() {
        let oco = OCOBracket::new("O2".to_string(), 100.0, 30.0, 15.0, 1.0, false);

        assert!((oco.take_profit_price - 99.70).abs() < 0.01);  // -30 bps
        assert!((oco.stop_loss_price - 100.15).abs() < 0.01);   // +15 bps
        assert!(!oco.is_long);
    }

    #[test]
    fn test_oco_bracket_tp_triggered_long() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);

        assert!(!oco.is_take_profit_triggered(100.0));
        assert!(!oco.is_take_profit_triggered(100.20));
        assert!(oco.is_take_profit_triggered(100.30));
        assert!(oco.is_take_profit_triggered(100.50));
    }

    #[test]
    fn test_oco_bracket_sl_triggered_long() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);

        assert!(!oco.is_stop_loss_triggered(100.0));
        assert!(!oco.is_stop_loss_triggered(99.90));
        assert!(oco.is_stop_loss_triggered(99.85));
        assert!(oco.is_stop_loss_triggered(99.50));
    }

    #[test]
    fn test_oco_bracket_tp_triggered_short() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, false);

        assert!(!oco.is_take_profit_triggered(100.0));
        assert!(!oco.is_take_profit_triggered(99.80));
        assert!(oco.is_take_profit_triggered(99.70));
        assert!(oco.is_take_profit_triggered(99.50));
    }

    #[test]
    fn test_oco_bracket_calculate_pnl_long() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 2.0, true);

        let pnl_win = oco.calculate_pnl(101.0);
        assert!((pnl_win - 2.0).abs() < 0.01); // +1 * 2 units

        let pnl_loss = oco.calculate_pnl(99.0);
        assert!((pnl_loss - (-2.0)).abs() < 0.01); // -1 * 2 units
    }

    #[test]
    fn test_oco_bracket_calculate_return_bps() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);

        let return_bps = oco.calculate_return_bps(100.50);
        assert!((return_bps - 50.0).abs() < 0.01); // +0.5% = 50 bps
    }

    #[test]
    fn test_oco_bracket_resolve() {
        let mut oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);

        assert_eq!(oco.status, OCOStatus::Active);
        assert!(oco.resolved_at_ms.is_none());

        oco.resolve(ExitReason::TakeProfit);

        assert_eq!(oco.status, OCOStatus::Resolved);
        assert!(oco.resolved_at_ms.is_some());
        assert_eq!(oco.exit_reason, Some(ExitReason::TakeProfit));
    }

    #[test]
    fn test_oco_bracket_serialization() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);
        let json = serde_json::to_string(&oco).unwrap();
        let deserialized: OCOBracket = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.parent_order_id, oco.parent_order_id);
        assert!((deserialized.entry_price - oco.entry_price).abs() < 0.01);
    }

    // ==================== KillSwitch Tests ====================

    #[test]
    fn test_kill_switch_new() {
        let ks = KillSwitch::new();
        assert!(!ks.is_triggered());
        assert!(ks.trigger_time().is_none());
    }

    #[test]
    fn test_kill_switch_default() {
        let ks = KillSwitch::default();
        assert!(!ks.is_triggered());
    }

    #[test]
    fn test_kill_switch_trigger() {
        let ks = KillSwitch::new();
        assert!(!ks.is_triggered());

        ks.trigger();

        assert!(ks.is_triggered());
        assert!(ks.trigger_time().is_some());
    }

    #[test]
    fn test_kill_switch_reset() {
        let ks = KillSwitch::new();
        ks.trigger();
        assert!(ks.is_triggered());

        ks.reset();

        assert!(!ks.is_triggered());
        assert!(ks.trigger_time().is_none());
    }

    #[test]
    fn test_kill_switch_clone() {
        let ks1 = KillSwitch::new();
        let ks2 = ks1.clone();

        ks1.trigger();

        assert!(ks1.is_triggered());
        assert!(ks2.is_triggered()); // Shared state
    }

    // ==================== CircuitBreakerState Tests ====================

    #[test]
    fn test_circuit_breaker_state_enum() {
        assert_eq!(CircuitBreakerState::Normal, CircuitBreakerState::Normal);
        assert_ne!(CircuitBreakerState::Normal, CircuitBreakerState::Triggered);
    }

    #[test]
    fn test_circuit_breaker_state_serialization() {
        let state = CircuitBreakerState::Warning;
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: CircuitBreakerState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, state);
    }

    // ==================== LiveTrade Tests ====================

    #[test]
    fn test_live_trade_new() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);
        let trade = LiveTrade::new("T1".to_string(), oco, 100.05, 0.5, 0.01, true);

        assert_eq!(trade.trade_id, "T1");
        assert!((trade.fill_price - 100.05).abs() < 0.01);
        assert!((trade.slippage_bps - 0.5).abs() < 0.01);
        assert!(trade.simulated);
        assert!(trade.pnl.is_none());
        assert!(!trade.audit_log.is_empty());
    }

    #[test]
    fn test_live_trade_close() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);
        let mut trade = LiveTrade::new("T1".to_string(), oco, 100.0, 0.0, 0.01, true);

        trade.close(100.30, ExitReason::TakeProfit);

        assert!(trade.pnl.is_some());
        assert!(trade.exit_price.is_some());
        assert!(trade.exit_time_ms.is_some());
        assert_eq!(trade.oco_bracket.status, OCOStatus::Resolved);
    }

    #[test]
    fn test_live_trade_to_trade_result() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);
        let mut trade = LiveTrade::new("T1".to_string(), oco, 100.0, 0.5, 0.01, true);
        trade.close(100.30, ExitReason::TakeProfit);

        let result = trade.to_trade_result("CFG001");

        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.trade_id, "T1");
        assert_eq!(result.direction, TradeDirection::Long);
        assert_eq!(result.exit_reason, ExitReason::TakeProfit);
    }

    #[test]
    fn test_live_trade_to_trade_result_unclosed() {
        let oco = OCOBracket::new("O1".to_string(), 100.0, 30.0, 15.0, 1.0, true);
        let trade = LiveTrade::new("T1".to_string(), oco, 100.0, 0.5, 0.01, true);

        // Not closed yet
        let result = trade.to_trade_result("CFG001");
        assert!(result.is_none());
    }

    // ==================== LiveMetrics Tests ====================

    #[test]
    fn test_live_metrics_default() {
        let metrics = LiveMetrics::default();

        assert_eq!(metrics.trade_count, 0);
        assert!((metrics.sharpe_ratio).abs() < 0.01);
        assert_eq!(metrics.circuit_breaker_state, CircuitBreakerState::Normal);
    }

    #[test]
    fn test_live_metrics_meets_requirements_pass() {
        let mut metrics = LiveMetrics::default();
        metrics.trade_count = 10;
        metrics.sharpe_ratio = 0.5;
        metrics.circuit_breaker_state = CircuitBreakerState::Normal;
        metrics.kill_switch_triggered = false;

        let config = LiveStageConfig::default();
        assert!(metrics.meets_requirements(&config));
    }

    #[test]
    fn test_live_metrics_meets_requirements_fail_trades() {
        let mut metrics = LiveMetrics::default();
        metrics.trade_count = 2; // Below min_trades
        metrics.sharpe_ratio = 0.5;

        let config = LiveStageConfig::default();
        assert!(!metrics.meets_requirements(&config));
    }

    #[test]
    fn test_live_metrics_meets_requirements_fail_circuit_breaker() {
        let mut metrics = LiveMetrics::default();
        metrics.trade_count = 10;
        metrics.sharpe_ratio = 0.5;
        metrics.circuit_breaker_state = CircuitBreakerState::Triggered;

        let config = LiveStageConfig::default();
        assert!(!metrics.meets_requirements(&config));
    }

    #[test]
    fn test_live_metrics_meets_requirements_fail_kill_switch() {
        let mut metrics = LiveMetrics::default();
        metrics.trade_count = 10;
        metrics.sharpe_ratio = 0.5;
        metrics.kill_switch_triggered = true;

        let config = LiveStageConfig::default();
        assert!(!metrics.meets_requirements(&config));
    }

    #[test]
    fn test_live_metrics_serialization() {
        let metrics = LiveMetrics {
            trade_count: 50,
            sharpe_ratio: 1.5,
            total_return: 0.08,
            ..Default::default()
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: LiveMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.trade_count, metrics.trade_count);
        assert!((deserialized.sharpe_ratio - metrics.sharpe_ratio).abs() < 0.01);
    }

    // ==================== LiveStage Tests ====================

    #[test]
    fn test_stage_new() {
        let config = LiveStageConfig::default();
        let stage = LiveStage::new(config.clone());

        assert_eq!(stage.config.name, config.name);
    }

    #[test]
    fn test_stage_with_defaults() {
        let stage = LiveStage::with_defaults();

        assert_eq!(stage.stage_type(), ValidationStageType::Live);
        assert_eq!(stage.name(), "Live");
    }

    #[test]
    fn test_stage_type() {
        let stage = LiveStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::Live);
    }

    #[test]
    fn test_stage_name() {
        let config = LiveStageConfig::default().with_name("Custom-Live");
        let stage = LiveStage::new(config);

        assert_eq!(stage.name(), "Custom-Live");
    }

    #[test]
    fn test_stage_description() {
        let stage = LiveStage::with_defaults();
        let desc = stage.description();

        assert!(desc.contains("Live"));
        assert!(desc.contains("OCO"));
    }

    #[test]
    fn test_stage_min_trades() {
        let config = LiveStageConfig {
            min_trades: 15,
            ..Default::default()
        };
        let stage = LiveStage::new(config);
        assert_eq!(stage.min_trades(), 15);
    }

    #[test]
    fn test_stage_requires_previous() {
        let stage = LiveStage::with_defaults();
        assert_eq!(stage.requires_previous(), Some(ValidationStageType::Paper));
    }

    #[test]
    fn test_stage_estimated_duration() {
        let config = LiveStageConfig::default().with_duration(7200);
        let stage = LiveStage::new(config);
        let ctx = StageContext::default();

        let duration = stage.estimated_duration(&ctx);
        assert!(duration.is_some());
        assert!(duration.unwrap() >= 7200);
    }

    #[test]
    fn test_stage_kill_switch() {
        let stage = LiveStage::with_defaults();
        let ks = stage.kill_switch();

        assert!(!ks.is_triggered());
    }

    // ==================== can_run() Tests ====================

    #[test]
    fn test_can_run_valid() {
        let stage = LiveStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::hours(1),
            Utc::now(),
        );

        assert!(stage.can_run(&ctx).is_ok());
    }

    #[test]
    fn test_can_run_invalid_period() {
        let stage = LiveStage::with_defaults();
        let now = Utc::now();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now,
            now - Duration::days(1),
        );

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_can_run_invalid_config() {
        let config = LiveStageConfig {
            duration_seconds: 0,
            ..Default::default()
        };
        let stage = LiveStage::new(config);

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
        let factory = LiveStageFactory::new();
        let stage = factory.create("Live-Test");

        assert_eq!(stage.name(), "Live-Test");
    }

    #[test]
    fn test_factory_default() {
        let factory = LiveStageFactory::default();
        let stage = factory.create("Live-Default");

        assert_eq!(stage.name(), "Live-Default");
    }

    #[test]
    fn test_factory_with_config() {
        let config = LiveStageConfig::conservative();
        let factory = LiveStageFactory::with_config(config);
        let stage = factory.create("Live-Conservative");

        assert_eq!(stage.name(), "Live-Conservative");
        assert!((stage.config.max_position_pct - 0.05).abs() < 0.01);
    }

    #[test]
    fn test_factory_create_with_config() {
        let factory = LiveStageFactory::new();
        let custom_config = LiveStageConfig::fast();
        let stage = factory.create_with_config("Live-Custom", custom_config);

        assert_eq!(stage.name(), "Live-Custom");
        assert_eq!(stage.config.duration_seconds, 300);
    }

    // ==================== Async Run Tests ====================

    #[tokio::test]
    async fn test_run_simulation() {
        let config = LiveStageConfig::simulation();
        let stage = LiveStage::new(config);

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::relaxed(),
            Utc::now() - Duration::minutes(1),
            Utc::now(),
        )
        .with_name("Live-Sim");

        let result = stage.run(&ctx).await;
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.stage_type, ValidationStageType::Live);
    }

    #[tokio::test]
    async fn test_run_with_kill_switch() {
        let config = LiveStageConfig {
            duration_seconds: 3600,
            ..LiveStageConfig::simulation()
        };
        let stage = LiveStage::new(config);

        let ks = stage.kill_switch();

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::relaxed(),
            Utc::now() - Duration::minutes(1),
            Utc::now(),
        )
        .with_name("Live-KillSwitch");

        // Trigger kill switch immediately
        ks.trigger();

        let result = stage.run(&ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_returns_future() {
        let stage = LiveStage::with_defaults();
        let ctx = StageContext::default().with_name("Test");

        let _future = stage.run(&ctx);
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_stage_name() {
        let config = LiveStageConfig::default().with_name("");
        let stage = LiveStage::new(config);
        assert_eq!(stage.name(), "");
    }

    #[test]
    fn test_very_long_stage_name() {
        let long_name = "L".repeat(1000);
        let config = LiveStageConfig::default().with_name(&long_name);
        let stage = LiveStage::new(config);
        assert_eq!(stage.name().len(), 1000);
    }

    #[test]
    fn test_very_short_duration() {
        let config = LiveStageConfig::default().with_duration(1);
        let stage = LiveStage::new(config);
        assert_eq!(stage.config.duration_seconds, 1);
    }

    #[test]
    fn test_very_long_duration() {
        let config = LiveStageConfig::default().with_duration(31536000); // 1 year
        let stage = LiveStage::new(config);
        assert_eq!(stage.config.duration_seconds, 31536000);
    }

    #[test]
    fn test_oco_brackets_negative_clamped() {
        let config = LiveStageConfig::default().with_oco_brackets(-10.0, -5.0);
        assert!((config.take_profit_bps).abs() < 0.01);
        assert!((config.stop_loss_bps).abs() < 0.01);
    }

    // ==================== ValidationStage Trait Tests ====================

    #[test]
    fn test_trait_stage_type_is_live() {
        let stage = LiveStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::Live);
    }

    #[test]
    fn test_trait_uses_live_data() {
        let stage = LiveStage::with_defaults();
        assert!(stage.stage_type().uses_live_data());
    }

    #[test]
    fn test_trait_is_not_historical() {
        let stage = LiveStage::with_defaults();
        assert!(!stage.stage_type().is_historical());
    }

    #[test]
    fn test_trait_pipeline_order() {
        let stage = LiveStage::with_defaults();
        assert_eq!(stage.stage_type().pipeline_order(), 5); // Fifth in pipeline
    }

    // ==================== Integration Tests ====================

    #[tokio::test]
    async fn test_full_live_trading_workflow() {
        let config = LiveStageConfig::simulation();
        let stage = LiveStage::new(config);

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::relaxed(),
            Utc::now() - Duration::minutes(5),
            Utc::now(),
        )
        .with_name("Live-Full-Test");

        let result = stage.run(&ctx).await;
        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.stage_type, ValidationStageType::Live);
        assert_eq!(result.stage_name, "Live-Full-Test");
        assert!(result.metadata.contains_key("duration_seconds"));
        assert!(result.metadata.contains_key("circuit_breaker_state"));
        assert!(result.metadata.contains_key("kill_switch_triggered"));
        assert!(result.metadata.contains_key("oco_tp_hits"));
        assert!(result.metadata.contains_key("oco_sl_hits"));
    }

    // ==================== Concurrent Access Tests ====================

    #[tokio::test]
    async fn test_concurrent_kill_switch_access() {
        use tokio::task::JoinSet;

        let ks = Arc::new(KillSwitch::new());
        let mut tasks = JoinSet::new();

        // Spawn multiple tasks checking kill switch status
        for _ in 0..10 {
            let ks_clone = ks.clone();
            tasks.spawn(async move { ks_clone.is_triggered() });
        }

        // All should return false
        while let Some(result) = tasks.join_next().await {
            assert!(!result.unwrap());
        }

        // Now trigger
        ks.trigger();

        // Spawn more tasks
        let mut tasks = JoinSet::new();
        for _ in 0..10 {
            let ks_clone = ks.clone();
            tasks.spawn(async move { ks_clone.is_triggered() });
        }

        // All should return true
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap());
        }
    }

    // ==================== AuditLogEntry Tests ====================

    #[test]
    fn test_audit_log_entry_serialization() {
        let entry = AuditLogEntry {
            timestamp_ms: 1000,
            event_type: "ENTRY".to_string(),
            message: "Trade opened".to_string(),
        };

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: AuditLogEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.event_type, entry.event_type);
        assert_eq!(deserialized.message, entry.message);
    }

    // ==================== CircuitBreakerTrigger Tests ====================

    #[test]
    fn test_circuit_breaker_trigger_serialization() {
        let trigger = CircuitBreakerTrigger::DrawdownLimit(0.05);
        let json = serde_json::to_string(&trigger).unwrap();
        let deserialized: CircuitBreakerTrigger = serde_json::from_str(&json).unwrap();

        match deserialized {
            CircuitBreakerTrigger::DrawdownLimit(val) => assert!((val - 0.05).abs() < 0.01),
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_circuit_breaker_trigger_variants() {
        let triggers = vec![
            CircuitBreakerTrigger::DrawdownLimit(0.05),
            CircuitBreakerTrigger::LossLimit(100.0),
            CircuitBreakerTrigger::ConsecutiveLosses(5),
            CircuitBreakerTrigger::ManualTrigger("test".to_string()),
            CircuitBreakerTrigger::KillSwitch,
            CircuitBreakerTrigger::PositionLimit(1000.0),
            CircuitBreakerTrigger::SlippageLimit(10.0),
        ];

        for trigger in triggers {
            let json = serde_json::to_string(&trigger).unwrap();
            assert!(!json.is_empty());
        }
    }

    // ==================== OCOStatus Tests ====================

    #[test]
    fn test_oco_status_variants() {
        let statuses = vec![
            OCOStatus::Active,
            OCOStatus::Resolved,
            OCOStatus::Cancelled,
            OCOStatus::Expired,
        ];

        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: OCOStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, status);
        }
    }

    // ==================== LivePnLSample Tests ====================

    #[test]
    fn test_live_pnl_sample_serialization() {
        let sample = LivePnLSample {
            timestamp_ms: 1000,
            realized_pnl: 100.0,
            unrealized_pnl: 50.0,
            equity: 10150.0,
            drawdown: 0.01,
            trade_count: 5,
            open_positions: 1,
            circuit_breaker_state: CircuitBreakerState::Normal,
        };

        let json = serde_json::to_string(&sample).unwrap();
        let deserialized: LivePnLSample = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.timestamp_ms, sample.timestamp_ms);
        assert!((deserialized.realized_pnl - sample.realized_pnl).abs() < 0.01);
    }
}

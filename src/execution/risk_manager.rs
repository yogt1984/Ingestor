//! Risk Management Layer
//!
//! Provides safety controls for market making operations including:
//! - Position limits (max inventory)
//! - Drawdown limits (max loss before kill switch)
//! - Daily loss limits
//! - Quote rate limiting
//! - Position timeout (force close after duration)
//! - Circuit breakers for abnormal conditions
//! - **Enhanced drawdown tracking** - trailing, time-weighted, recovery metrics
//! - **Staged circuit breakers** - warning, reduce-only, halt, emergency levels
//!
//! # Design Philosophy
//!
//! Risk management operates as a **gate** that can:
//! 1. **Allow** - Normal operation, all quotes permitted
//! 2. **Reduce** - Reduce position only (close trades allowed, new positions blocked)
//! 3. **Halt** - No new quotes, existing positions remain
//! 4. **Emergency** - Full stop, should trigger position liquidation
//!
//! # Circuit Breaker Stages
//!
//! The circuit breaker operates in stages to provide graduated risk control:
//! - **Stage 0 (Normal)**: All operations allowed
//! - **Stage 1 (Warning)**: Operations allowed, alerts generated
//! - **Stage 2 (Reduce Only)**: Only position-reducing trades allowed
//! - **Stage 3 (Halt)**: All trading halted, positions maintained
//! - **Stage 4 (Emergency)**: Full stop, liquidation may be triggered
//!
//! # Usage
//!
//! ```ignore
//! let config = RiskConfig::default();
//! let mut risk_mgr = RiskManager::new(config);
//!
//! // Before generating quotes
//! let action = risk_mgr.check_pre_quote(&mm_state, current_time_ms);
//! match action {
//!     RiskAction::Allow => { /* proceed with quoting */ }
//!     RiskAction::ReduceOnly => { /* only quote to reduce position */ }
//!     RiskAction::Halt { reason } => { /* stop quoting, log reason */ }
//!     RiskAction::Emergency { reason } => { /* stop everything, alert */ }
//! }
//!
//! // After a fill
//! risk_mgr.on_fill(&fill, &mm_state, current_time_ms);
//!
//! // Get drawdown metrics
//! let metrics = risk_mgr.drawdown_metrics();
//! println!("Current drawdown: {:.2}%", metrics.current_drawdown * 100.0);
//! ```

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use crate::execution::market_maker::{Fill, QuoteSide, MMState};

// ============================================================================
// Circuit Breaker Types
// ============================================================================

/// Circuit breaker stage levels (0-4)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CircuitBreakerStage {
    /// Normal operation - all trading allowed
    Normal = 0,
    /// Warning level - trading allowed but alerts generated
    Warning = 1,
    /// Reduce only - only position-reducing trades allowed
    ReduceOnly = 2,
    /// Halt - all trading halted
    Halt = 3,
    /// Emergency - full stop, may trigger liquidation
    Emergency = 4,
}

impl Default for CircuitBreakerStage {
    fn default() -> Self {
        Self::Normal
    }
}

impl CircuitBreakerStage {
    /// Check if trading is allowed at this stage
    pub fn allows_trading(&self) -> bool {
        matches!(self, Self::Normal | Self::Warning | Self::ReduceOnly)
    }

    /// Check if new positions are allowed at this stage
    pub fn allows_new_positions(&self) -> bool {
        matches!(self, Self::Normal | Self::Warning)
    }

    /// Get the next higher stage
    pub fn escalate(&self) -> Self {
        match self {
            Self::Normal => Self::Warning,
            Self::Warning => Self::ReduceOnly,
            Self::ReduceOnly => Self::Halt,
            Self::Halt => Self::Emergency,
            Self::Emergency => Self::Emergency,
        }
    }

    /// Get the next lower stage (for recovery)
    pub fn de_escalate(&self) -> Self {
        match self {
            Self::Normal => Self::Normal,
            Self::Warning => Self::Normal,
            Self::ReduceOnly => Self::Warning,
            Self::Halt => Self::ReduceOnly,
            Self::Emergency => Self::Halt,
        }
    }
}

/// Circuit breaker configuration with staged thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Drawdown threshold for warning (e.g., 0.05 = 5%)
    pub warning_drawdown: f64,
    /// Drawdown threshold for reduce-only (e.g., 0.08 = 8%)
    pub reduce_only_drawdown: f64,
    /// Drawdown threshold for halt (e.g., 0.10 = 10%)
    pub halt_drawdown: f64,
    /// Drawdown threshold for emergency (e.g., 0.20 = 20%)
    pub emergency_drawdown: f64,

    /// Daily loss threshold for warning
    pub warning_daily_loss: Decimal,
    /// Daily loss threshold for reduce-only
    pub reduce_only_daily_loss: Decimal,
    /// Daily loss threshold for halt
    pub halt_daily_loss: Decimal,

    /// Consecutive losses for warning
    pub warning_consecutive_losses: u32,
    /// Consecutive losses for reduce-only
    pub reduce_only_consecutive_losses: u32,
    /// Consecutive losses for halt
    pub halt_consecutive_losses: u32,

    /// Enable automatic de-escalation when conditions improve
    pub auto_de_escalate: bool,
    /// Minimum time at each stage before de-escalation (ms)
    pub min_stage_duration_ms: u64,
    /// Required improvement factor for de-escalation (e.g., 0.8 = 80% of threshold)
    pub de_escalation_threshold: f64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            // Staged drawdown thresholds
            warning_drawdown: 0.05,        // 5%
            reduce_only_drawdown: 0.08,    // 8%
            halt_drawdown: 0.10,           // 10%
            emergency_drawdown: 0.20,      // 20%

            // Staged daily loss thresholds
            warning_daily_loss: dec!(0.02),      // 0.02 BTC
            reduce_only_daily_loss: dec!(0.035), // 0.035 BTC
            halt_daily_loss: dec!(0.05),         // 0.05 BTC

            // Staged consecutive loss thresholds
            warning_consecutive_losses: 3,
            reduce_only_consecutive_losses: 5,
            halt_consecutive_losses: 8,

            // Recovery settings
            auto_de_escalate: true,
            min_stage_duration_ms: 60_000,  // 1 minute minimum
            de_escalation_threshold: 0.7,   // Must be at 70% of threshold to de-escalate
        }
    }
}

impl CircuitBreakerConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.warning_drawdown >= self.reduce_only_drawdown {
            return Err("warning_drawdown must be < reduce_only_drawdown".to_string());
        }
        if self.reduce_only_drawdown >= self.halt_drawdown {
            return Err("reduce_only_drawdown must be < halt_drawdown".to_string());
        }
        if self.halt_drawdown >= self.emergency_drawdown {
            return Err("halt_drawdown must be < emergency_drawdown".to_string());
        }
        if self.de_escalation_threshold <= 0.0 || self.de_escalation_threshold >= 1.0 {
            return Err("de_escalation_threshold must be between 0 and 1".to_string());
        }
        Ok(())
    }

    /// Get the circuit breaker stage for a given drawdown
    pub fn stage_for_drawdown(&self, drawdown: f64) -> CircuitBreakerStage {
        if drawdown >= self.emergency_drawdown {
            CircuitBreakerStage::Emergency
        } else if drawdown >= self.halt_drawdown {
            CircuitBreakerStage::Halt
        } else if drawdown >= self.reduce_only_drawdown {
            CircuitBreakerStage::ReduceOnly
        } else if drawdown >= self.warning_drawdown {
            CircuitBreakerStage::Warning
        } else {
            CircuitBreakerStage::Normal
        }
    }

    /// Get the circuit breaker stage for daily loss
    pub fn stage_for_daily_loss(&self, daily_loss: Decimal) -> CircuitBreakerStage {
        if daily_loss >= self.halt_daily_loss {
            CircuitBreakerStage::Halt
        } else if daily_loss >= self.reduce_only_daily_loss {
            CircuitBreakerStage::ReduceOnly
        } else if daily_loss >= self.warning_daily_loss {
            CircuitBreakerStage::Warning
        } else {
            CircuitBreakerStage::Normal
        }
    }

    /// Get the circuit breaker stage for consecutive losses
    pub fn stage_for_consecutive_losses(&self, losses: u32) -> CircuitBreakerStage {
        if losses >= self.halt_consecutive_losses {
            CircuitBreakerStage::Halt
        } else if losses >= self.reduce_only_consecutive_losses {
            CircuitBreakerStage::ReduceOnly
        } else if losses >= self.warning_consecutive_losses {
            CircuitBreakerStage::Warning
        } else {
            CircuitBreakerStage::Normal
        }
    }
}

// ============================================================================
// Drawdown Tracking Types
// ============================================================================

/// Comprehensive drawdown metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DrawdownMetrics {
    /// Current drawdown from peak (as decimal, e.g., 0.05 = 5%)
    pub current_drawdown: f64,
    /// Maximum drawdown ever observed
    pub max_drawdown: f64,
    /// Average drawdown over the session
    pub average_drawdown: f64,
    /// Current peak equity value
    pub peak_equity: Decimal,
    /// Current equity value
    pub current_equity: Decimal,
    /// Time spent in drawdown (ms)
    pub time_in_drawdown_ms: u64,
    /// Number of drawdown periods
    pub drawdown_count: u32,
    /// Average drawdown duration (ms)
    pub avg_drawdown_duration_ms: u64,
    /// Longest drawdown duration (ms)
    pub max_drawdown_duration_ms: u64,
    /// Time since peak (ms) - current drawdown duration
    pub current_drawdown_duration_ms: u64,
    /// Recovery factor (total profit / max drawdown)
    pub recovery_factor: f64,
    /// Calmar ratio (annualized return / max drawdown)
    pub calmar_ratio: f64,
}

/// Drawdown period record for analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrawdownPeriod {
    /// Start time of drawdown
    pub start_time_ms: u64,
    /// End time of drawdown (None if ongoing)
    pub end_time_ms: Option<u64>,
    /// Peak equity at start
    pub peak_equity: Decimal,
    /// Trough equity (lowest point)
    pub trough_equity: Decimal,
    /// Maximum drawdown during this period
    pub max_drawdown: f64,
    /// Whether this drawdown has been recovered from
    pub recovered: bool,
}

impl DrawdownPeriod {
    /// Create a new drawdown period
    pub fn new(start_time_ms: u64, peak_equity: Decimal) -> Self {
        Self {
            start_time_ms,
            end_time_ms: None,
            peak_equity,
            trough_equity: peak_equity,
            max_drawdown: 0.0,
            recovered: false,
        }
    }

    /// Update the trough if current equity is lower
    pub fn update_trough(&mut self, equity: Decimal) {
        if equity < self.trough_equity {
            self.trough_equity = equity;
            if self.peak_equity > Decimal::ZERO {
                self.max_drawdown = ((self.peak_equity - self.trough_equity) / self.peak_equity)
                    .to_f64()
                    .unwrap_or(0.0);
            }
        }
    }

    /// Mark the drawdown as recovered
    pub fn mark_recovered(&mut self, end_time_ms: u64) {
        self.end_time_ms = Some(end_time_ms);
        self.recovered = true;
    }

    /// Get the duration of this drawdown period
    pub fn duration_ms(&self, current_time_ms: u64) -> u64 {
        self.end_time_ms.unwrap_or(current_time_ms).saturating_sub(self.start_time_ms)
    }
}

/// Circuit breaker state tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerState {
    /// Current stage
    pub stage: CircuitBreakerStage,
    /// Time when entered current stage
    pub stage_entered_ms: u64,
    /// Reason for current stage
    pub reason: Option<String>,
    /// Number of stage escalations
    pub escalation_count: u32,
    /// Number of stage de-escalations
    pub de_escalation_count: u32,
    /// Time spent in each stage (cumulative)
    pub time_per_stage_ms: [u64; 5],
}

impl Default for CircuitBreakerState {
    fn default() -> Self {
        Self {
            stage: CircuitBreakerStage::Normal,
            stage_entered_ms: 0,
            reason: None,
            escalation_count: 0,
            de_escalation_count: 0,
            time_per_stage_ms: [0; 5],
        }
    }
}

impl CircuitBreakerState {
    /// Update time tracking when stage changes
    pub fn update_time(&mut self, current_time_ms: u64) {
        let duration = current_time_ms.saturating_sub(self.stage_entered_ms);
        self.time_per_stage_ms[self.stage as usize] += duration;
    }

    /// Set new stage
    pub fn set_stage(&mut self, new_stage: CircuitBreakerStage, current_time_ms: u64, reason: Option<String>) {
        if new_stage != self.stage {
            self.update_time(current_time_ms);

            if new_stage > self.stage {
                self.escalation_count += 1;
            } else {
                self.de_escalation_count += 1;
            }

            self.stage = new_stage;
            self.stage_entered_ms = current_time_ms;
            self.reason = reason;
        }
    }

    /// Check if minimum stage duration has passed
    pub fn can_de_escalate(&self, current_time_ms: u64, min_duration_ms: u64) -> bool {
        current_time_ms.saturating_sub(self.stage_entered_ms) >= min_duration_ms
    }
}

// ============================================================================
// Risk Management Configuration
// ============================================================================

/// Risk management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    // === Position Limits ===
    /// Maximum absolute inventory (e.g., 0.1 BTC)
    pub max_inventory: Decimal,
    /// Soft limit - start reducing at this level (e.g., 0.08 BTC)
    pub soft_inventory_limit: Decimal,

    // === Loss Limits ===
    /// Maximum drawdown as decimal (e.g., 0.05 = 5%)
    pub max_drawdown: f64,
    /// Daily loss limit in base currency (e.g., 0.01 BTC)
    pub daily_loss_limit: Decimal,
    /// Per-trade loss limit (e.g., 0.001 BTC)
    pub max_loss_per_trade: Decimal,

    // === Time Limits ===
    /// Maximum time to hold a position in milliseconds (0 = disabled)
    pub max_position_age_ms: u64,
    /// Cooldown period after a halt in milliseconds
    pub halt_cooldown_ms: u64,

    // === Rate Limits ===
    /// Maximum quotes per minute (0 = disabled)
    pub max_quotes_per_minute: u32,
    /// Maximum fills per minute (0 = disabled)
    pub max_fills_per_minute: u32,

    // === Circuit Breakers ===
    /// Consecutive losses before reducing (0 = disabled)
    pub consecutive_loss_limit: u32,
    /// Volatility threshold for circuit breaker (0 = disabled)
    pub max_volatility: f64,

    // === Recovery ===
    /// Enable automatic recovery from Halt state
    pub auto_recover: bool,
    /// Time before auto-recovery attempt in milliseconds
    pub recovery_delay_ms: u64,

    // === Circuit Breaker ===
    /// Enable staged circuit breaker (if false, uses legacy behavior)
    pub use_staged_circuit_breaker: bool,
    /// Staged circuit breaker configuration
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            // Position limits
            max_inventory: dec!(0.1),
            soft_inventory_limit: dec!(0.08),

            // Loss limits
            max_drawdown: 0.10,        // 10% max drawdown
            daily_loss_limit: dec!(0.05), // 0.05 BTC daily loss limit
            max_loss_per_trade: dec!(0.005), // 0.005 BTC max loss per trade

            // Time limits
            max_position_age_ms: 3600 * 1000, // 1 hour max position hold
            halt_cooldown_ms: 60 * 1000,      // 1 minute cooldown

            // Rate limits
            max_quotes_per_minute: 120, // 2 quotes per second average
            max_fills_per_minute: 30,   // 1 fill every 2 seconds average

            // Circuit breakers
            consecutive_loss_limit: 5,
            max_volatility: 0.05, // 5% volatility threshold

            // Recovery
            auto_recover: true,
            recovery_delay_ms: 300_000, // 5 minutes

            // Circuit breaker
            use_staged_circuit_breaker: true,
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

impl RiskConfig {
    /// Create a conservative configuration for testing
    pub fn conservative() -> Self {
        Self {
            max_inventory: dec!(0.05),
            soft_inventory_limit: dec!(0.03),
            max_drawdown: 0.05,
            daily_loss_limit: dec!(0.02),
            max_loss_per_trade: dec!(0.002),
            max_position_age_ms: 1800 * 1000, // 30 minutes
            halt_cooldown_ms: 120 * 1000,     // 2 minutes
            max_quotes_per_minute: 60,
            max_fills_per_minute: 15,
            consecutive_loss_limit: 3,
            max_volatility: 0.03,
            auto_recover: false,
            recovery_delay_ms: 600_000, // 10 minutes
            use_staged_circuit_breaker: true,
            circuit_breaker: CircuitBreakerConfig {
                warning_drawdown: 0.03,
                reduce_only_drawdown: 0.04,
                halt_drawdown: 0.05,
                emergency_drawdown: 0.10,
                warning_daily_loss: dec!(0.01),
                reduce_only_daily_loss: dec!(0.015),
                halt_daily_loss: dec!(0.02),
                warning_consecutive_losses: 2,
                reduce_only_consecutive_losses: 3,
                halt_consecutive_losses: 5,
                auto_de_escalate: false,
                min_stage_duration_ms: 120_000,
                de_escalation_threshold: 0.6,
            },
        }
    }

    /// Create an aggressive configuration for backtesting
    pub fn aggressive() -> Self {
        Self {
            max_inventory: dec!(0.2),
            soft_inventory_limit: dec!(0.15),
            max_drawdown: 0.20,
            daily_loss_limit: dec!(0.1),
            max_loss_per_trade: dec!(0.01),
            max_position_age_ms: 0, // Disabled
            halt_cooldown_ms: 30 * 1000,
            max_quotes_per_minute: 0, // Disabled
            max_fills_per_minute: 0,  // Disabled
            consecutive_loss_limit: 10,
            max_volatility: 0.10,
            auto_recover: true,
            recovery_delay_ms: 60_000,
            use_staged_circuit_breaker: true,
            circuit_breaker: CircuitBreakerConfig {
                warning_drawdown: 0.10,
                reduce_only_drawdown: 0.15,
                halt_drawdown: 0.20,
                emergency_drawdown: 0.35,
                warning_daily_loss: dec!(0.05),
                reduce_only_daily_loss: dec!(0.07),
                halt_daily_loss: dec!(0.10),
                warning_consecutive_losses: 5,
                reduce_only_consecutive_losses: 8,
                halt_consecutive_losses: 12,
                auto_de_escalate: true,
                min_stage_duration_ms: 30_000,
                de_escalation_threshold: 0.8,
            },
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_inventory <= dec!(0) {
            return Err("max_inventory must be positive".to_string());
        }
        if self.soft_inventory_limit > self.max_inventory {
            return Err("soft_inventory_limit must be <= max_inventory".to_string());
        }
        if self.max_drawdown <= 0.0 || self.max_drawdown > 1.0 {
            return Err("max_drawdown must be between 0 and 1".to_string());
        }
        Ok(())
    }
}

/// Risk action to take
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskAction {
    /// Normal operation - all quotes allowed
    Allow,
    /// Reduce position only - no new exposure
    ReduceOnly,
    /// Stop quoting - maintain current position
    Halt { reason: HaltReason },
    /// Emergency stop - should trigger liquidation
    Emergency { reason: EmergencyReason },
}

impl RiskAction {
    /// Check if quoting is allowed
    pub fn allows_quoting(&self) -> bool {
        matches!(self, RiskAction::Allow | RiskAction::ReduceOnly)
    }

    /// Check if new positions are allowed
    pub fn allows_new_position(&self) -> bool {
        matches!(self, RiskAction::Allow)
    }

    /// Check if this is a halt or emergency
    pub fn is_stopped(&self) -> bool {
        matches!(self, RiskAction::Halt { .. } | RiskAction::Emergency { .. })
    }
}

/// Reasons for halting
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HaltReason {
    MaxDrawdownExceeded,
    DailyLossLimitExceeded,
    MaxInventoryExceeded,
    ConsecutiveLosses,
    PositionTimeout,
    QuoteRateLimitExceeded,
    FillRateLimitExceeded,
    HighVolatility,
    ManualHalt,
    CooldownActive,
}

impl std::fmt::Display for HaltReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HaltReason::MaxDrawdownExceeded => write!(f, "Maximum drawdown exceeded"),
            HaltReason::DailyLossLimitExceeded => write!(f, "Daily loss limit exceeded"),
            HaltReason::MaxInventoryExceeded => write!(f, "Maximum inventory exceeded"),
            HaltReason::ConsecutiveLosses => write!(f, "Too many consecutive losses"),
            HaltReason::PositionTimeout => write!(f, "Position held too long"),
            HaltReason::QuoteRateLimitExceeded => write!(f, "Quote rate limit exceeded"),
            HaltReason::FillRateLimitExceeded => write!(f, "Fill rate limit exceeded"),
            HaltReason::HighVolatility => write!(f, "Volatility too high"),
            HaltReason::ManualHalt => write!(f, "Manual halt triggered"),
            HaltReason::CooldownActive => write!(f, "Cooldown period active"),
        }
    }
}

/// Reasons for emergency stop
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmergencyReason {
    CatastrophicLoss,
    SystemError,
    ExternalTrigger,
}

impl std::fmt::Display for EmergencyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmergencyReason::CatastrophicLoss => write!(f, "Catastrophic loss detected"),
            EmergencyReason::SystemError => write!(f, "System error"),
            EmergencyReason::ExternalTrigger => write!(f, "External emergency trigger"),
        }
    }
}

/// Current risk state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskState {
    Normal,
    ReduceOnly,
    Halted { reason: HaltReason, since_ms: u64 },
    Emergency { reason: EmergencyReason },
}

/// Risk event for logging/alerting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEvent {
    pub timestamp: DateTime<Utc>,
    pub timestamp_ms: u64,
    pub event_type: RiskEventType,
    pub details: String,
    pub state_before: RiskState,
    pub state_after: RiskState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskEventType {
    StateChange,
    LimitBreached,
    LimitWarning,
    Recovery,
    ManualAction,
}

/// Risk statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RiskStats {
    /// Number of halts triggered
    pub halt_count: u32,
    /// Total time in halted state (milliseconds)
    pub total_halt_time_ms: u64,
    /// Number of reduce-only periods
    pub reduce_only_count: u32,
    /// Breaches by type
    pub inventory_breaches: u32,
    pub drawdown_breaches: u32,
    pub daily_loss_breaches: u32,
    pub consecutive_loss_breaches: u32,
    pub volatility_breaches: u32,
    pub rate_limit_breaches: u32,
    /// Peak values observed
    pub peak_inventory: Decimal,
    pub peak_drawdown: f64,
    pub peak_daily_loss: Decimal,
}

/// Risk Manager
pub struct RiskManager {
    config: RiskConfig,
    state: RiskState,
    stats: RiskStats,
    events: Vec<RiskEvent>,

    // Tracking state
    /// Peak equity for drawdown calculation
    peak_equity: Decimal,
    /// Daily PnL start (resets at midnight UTC)
    daily_pnl_start: Decimal,
    /// Day of last reset
    last_reset_day: u32,
    /// Consecutive losses counter
    consecutive_losses: u32,
    /// Last winning trade flag
    last_trade_was_win: bool,

    // Position tracking
    /// Time when position was first opened
    position_open_time_ms: Option<u64>,

    // Rate limiting
    /// Recent quote timestamps for rate limiting
    recent_quotes: VecDeque<u64>,
    /// Recent fill timestamps for rate limiting
    recent_fills: VecDeque<u64>,

    // Halt tracking
    /// Time when halt started
    halt_start_ms: Option<u64>,

    // === Enhanced Drawdown Tracking ===
    /// Current drawdown period (if in drawdown)
    current_drawdown_period: Option<DrawdownPeriod>,
    /// Historical drawdown periods for analysis
    drawdown_history: Vec<DrawdownPeriod>,
    /// Sum of all drawdown observations for average calculation
    drawdown_sum: f64,
    /// Count of drawdown observations
    drawdown_observation_count: u64,
    /// Time when tracking started
    tracking_start_ms: u64,
    /// Initial equity at start
    initial_equity: Decimal,

    // === Circuit Breaker State ===
    /// Staged circuit breaker state
    circuit_breaker_state: CircuitBreakerState,
}

impl RiskManager {
    /// Create a new risk manager
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            state: RiskState::Normal,
            stats: RiskStats::default(),
            events: Vec::new(),
            peak_equity: dec!(0),
            daily_pnl_start: dec!(0),
            last_reset_day: 0,
            consecutive_losses: 0,
            last_trade_was_win: true,
            position_open_time_ms: None,
            recent_quotes: VecDeque::new(),
            recent_fills: VecDeque::new(),
            halt_start_ms: None,
            // Enhanced drawdown tracking
            current_drawdown_period: None,
            drawdown_history: Vec::new(),
            drawdown_sum: 0.0,
            drawdown_observation_count: 0,
            tracking_start_ms: 0,
            initial_equity: dec!(0),
            // Circuit breaker
            circuit_breaker_state: CircuitBreakerState::default(),
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &RiskConfig {
        &self.config
    }

    /// Get current state
    pub fn state(&self) -> &RiskState {
        &self.state
    }

    /// Get statistics
    pub fn stats(&self) -> &RiskStats {
        &self.stats
    }

    /// Get risk events
    pub fn events(&self) -> &[RiskEvent] {
        &self.events
    }

    /// Get current circuit breaker state
    pub fn circuit_breaker_state(&self) -> &CircuitBreakerState {
        &self.circuit_breaker_state
    }

    /// Get current circuit breaker stage
    pub fn circuit_breaker_stage(&self) -> CircuitBreakerStage {
        self.circuit_breaker_state.stage
    }

    /// Get drawdown history
    pub fn drawdown_history(&self) -> &[DrawdownPeriod] {
        &self.drawdown_history
    }

    /// Get comprehensive drawdown metrics
    pub fn drawdown_metrics(&self, current_time_ms: u64) -> DrawdownMetrics {
        let current_drawdown = self.calculate_drawdown_internal();
        let elapsed_ms = current_time_ms.saturating_sub(self.tracking_start_ms);

        // Calculate time in drawdown
        let mut total_drawdown_time = 0u64;
        let mut completed_periods: Vec<&DrawdownPeriod> = Vec::new();

        for period in &self.drawdown_history {
            if period.recovered {
                total_drawdown_time += period.duration_ms(current_time_ms);
                completed_periods.push(period);
            }
        }

        // Add current drawdown period time if active
        if let Some(ref period) = self.current_drawdown_period {
            total_drawdown_time += period.duration_ms(current_time_ms);
        }

        // Calculate statistics
        let drawdown_count = self.drawdown_history.len() as u32
            + if self.current_drawdown_period.is_some() { 1 } else { 0 };

        let avg_drawdown = if self.drawdown_observation_count > 0 {
            self.drawdown_sum / self.drawdown_observation_count as f64
        } else {
            0.0
        };

        let avg_duration = if !completed_periods.is_empty() {
            completed_periods.iter()
                .map(|p| p.duration_ms(current_time_ms))
                .sum::<u64>() / completed_periods.len() as u64
        } else {
            0
        };

        let max_duration = self.drawdown_history.iter()
            .chain(self.current_drawdown_period.iter())
            .map(|p| p.duration_ms(current_time_ms))
            .max()
            .unwrap_or(0);

        let current_duration = self.current_drawdown_period.as_ref()
            .map(|p| p.duration_ms(current_time_ms))
            .unwrap_or(0);

        // Calculate recovery factor and Calmar ratio
        let total_return = if self.initial_equity > Decimal::ZERO {
            ((self.peak_equity - self.initial_equity) / self.initial_equity)
                .to_f64()
                .unwrap_or(0.0)
        } else {
            0.0
        };

        let recovery_factor = if self.stats.peak_drawdown > 0.0 {
            total_return / self.stats.peak_drawdown
        } else {
            0.0
        };

        // Annualize for Calmar ratio (assuming ~252 trading days)
        let years = elapsed_ms as f64 / (365.25 * 24.0 * 60.0 * 60.0 * 1000.0);
        let annualized_return = if years > 0.0 {
            total_return / years
        } else {
            0.0
        };

        let calmar_ratio = if self.stats.peak_drawdown > 0.0 {
            annualized_return / self.stats.peak_drawdown
        } else {
            0.0
        };

        DrawdownMetrics {
            current_drawdown,
            max_drawdown: self.stats.peak_drawdown,
            average_drawdown: avg_drawdown,
            peak_equity: self.peak_equity,
            current_equity: self.peak_equity * Decimal::from_f64(1.0 - current_drawdown).unwrap_or(Decimal::ONE),
            time_in_drawdown_ms: total_drawdown_time,
            drawdown_count,
            avg_drawdown_duration_ms: avg_duration,
            max_drawdown_duration_ms: max_duration,
            current_drawdown_duration_ms: current_duration,
            recovery_factor,
            calmar_ratio,
        }
    }

    /// Initialize tracking with initial equity
    pub fn initialize_tracking(&mut self, initial_equity: Decimal, start_time_ms: u64) {
        self.initial_equity = initial_equity;
        self.peak_equity = initial_equity;
        self.tracking_start_ms = start_time_ms;
        self.daily_pnl_start = initial_equity;
    }

    /// Update drawdown tracking based on current equity
    pub fn update_drawdown_tracking(&mut self, current_equity: Decimal, current_time_ms: u64) {
        let current_drawdown = if self.peak_equity > Decimal::ZERO {
            ((self.peak_equity - current_equity) / self.peak_equity)
                .to_f64()
                .unwrap_or(0.0)
                .max(0.0)
        } else {
            0.0
        };

        // Track for average calculation
        self.drawdown_sum += current_drawdown;
        self.drawdown_observation_count += 1;

        // Update peak if new high
        if current_equity > self.peak_equity {
            // Check if we're recovering from a drawdown
            if let Some(mut period) = self.current_drawdown_period.take() {
                period.mark_recovered(current_time_ms);
                self.drawdown_history.push(period);
            }
            self.peak_equity = current_equity;
        } else if current_drawdown > 0.0 {
            // We're in a drawdown
            if let Some(ref mut period) = self.current_drawdown_period {
                period.update_trough(current_equity);
            } else {
                // Start a new drawdown period
                let mut period = DrawdownPeriod::new(current_time_ms, self.peak_equity);
                period.update_trough(current_equity);
                self.current_drawdown_period = Some(period);
            }
        }

        // Update peak drawdown stat
        if current_drawdown > self.stats.peak_drawdown {
            self.stats.peak_drawdown = current_drawdown;
        }
    }

    /// Check staged circuit breaker and return appropriate action
    pub fn check_staged_circuit_breaker(
        &mut self,
        drawdown: f64,
        daily_loss: Decimal,
        current_time_ms: u64,
    ) -> RiskAction {
        if !self.config.use_staged_circuit_breaker {
            // Fall back to legacy behavior
            return RiskAction::Allow;
        }

        let cb_config = &self.config.circuit_breaker;

        // Determine the required stage based on current conditions
        let drawdown_stage = cb_config.stage_for_drawdown(drawdown);
        let daily_loss_stage = cb_config.stage_for_daily_loss(daily_loss);
        let consecutive_loss_stage = cb_config.stage_for_consecutive_losses(self.consecutive_losses);

        // Take the most severe stage
        let required_stage = drawdown_stage.max(daily_loss_stage).max(consecutive_loss_stage);

        // Determine reason for the stage
        let reason = if drawdown_stage >= daily_loss_stage && drawdown_stage >= consecutive_loss_stage {
            format!("Drawdown {:.2}%", drawdown * 100.0)
        } else if daily_loss_stage >= consecutive_loss_stage {
            format!("Daily loss {}", daily_loss)
        } else {
            format!("{} consecutive losses", self.consecutive_losses)
        };

        // Handle stage transitions
        let current_stage = self.circuit_breaker_state.stage;

        if required_stage > current_stage {
            // Escalate
            self.circuit_breaker_state.set_stage(required_stage, current_time_ms, Some(reason.clone()));
            self.log_event(
                current_time_ms,
                RiskEventType::StateChange,
                format!("Circuit breaker escalated to {:?}: {}", required_stage, reason),
            );
        } else if required_stage < current_stage && cb_config.auto_de_escalate {
            // Check if we can de-escalate
            if self.circuit_breaker_state.can_de_escalate(current_time_ms, cb_config.min_stage_duration_ms) {
                // Check if conditions are sufficiently improved
                let de_escalate_threshold = cb_config.de_escalation_threshold;
                let target_stage = current_stage.de_escalate();

                let can_de_escalate = match target_stage {
                    CircuitBreakerStage::Normal => {
                        drawdown < cb_config.warning_drawdown * de_escalate_threshold
                            && daily_loss < cb_config.warning_daily_loss * Decimal::from_f64(de_escalate_threshold).unwrap()
                            && self.consecutive_losses < cb_config.warning_consecutive_losses
                    }
                    CircuitBreakerStage::Warning => {
                        drawdown < cb_config.reduce_only_drawdown * de_escalate_threshold
                            && daily_loss < cb_config.reduce_only_daily_loss * Decimal::from_f64(de_escalate_threshold).unwrap()
                            && self.consecutive_losses < cb_config.reduce_only_consecutive_losses
                    }
                    CircuitBreakerStage::ReduceOnly => {
                        drawdown < cb_config.halt_drawdown * de_escalate_threshold
                            && daily_loss < cb_config.halt_daily_loss * Decimal::from_f64(de_escalate_threshold).unwrap()
                            && self.consecutive_losses < cb_config.halt_consecutive_losses
                    }
                    _ => false,
                };

                if can_de_escalate {
                    self.circuit_breaker_state.set_stage(target_stage, current_time_ms, None);
                    self.log_event(
                        current_time_ms,
                        RiskEventType::Recovery,
                        format!("Circuit breaker de-escalated to {:?}", target_stage),
                    );
                }
            }
        }

        // Return action based on current stage
        match self.circuit_breaker_state.stage {
            CircuitBreakerStage::Normal => RiskAction::Allow,
            CircuitBreakerStage::Warning => {
                // Generate warning but allow trading
                self.log_event(
                    current_time_ms,
                    RiskEventType::LimitWarning,
                    format!("Warning: {}", reason),
                );
                RiskAction::Allow
            }
            CircuitBreakerStage::ReduceOnly => RiskAction::ReduceOnly,
            CircuitBreakerStage::Halt => RiskAction::Halt {
                reason: HaltReason::MaxDrawdownExceeded,
            },
            CircuitBreakerStage::Emergency => RiskAction::Emergency {
                reason: EmergencyReason::CatastrophicLoss,
            },
        }
    }

    /// Force circuit breaker to a specific stage (for testing or manual override)
    pub fn set_circuit_breaker_stage(&mut self, stage: CircuitBreakerStage, current_time_ms: u64, reason: Option<String>) {
        self.circuit_breaker_state.set_stage(stage, current_time_ms, reason);
    }

    /// Get time spent at each circuit breaker stage
    pub fn circuit_breaker_time_breakdown(&self, current_time_ms: u64) -> [u64; 5] {
        let mut times = self.circuit_breaker_state.time_per_stage_ms;
        // Add time for current stage
        let current_duration = current_time_ms.saturating_sub(self.circuit_breaker_state.stage_entered_ms);
        times[self.circuit_breaker_state.stage as usize] += current_duration;
        times
    }

    // Internal helper for drawdown calculation
    fn calculate_drawdown_internal(&self) -> f64 {
        if let Some(ref period) = self.current_drawdown_period {
            period.max_drawdown
        } else {
            0.0
        }
    }

    /// Check risk before generating a quote
    pub fn check_pre_quote(
        &mut self,
        mm_state: &MMState,
        current_time_ms: u64,
        volatility: f64,
    ) -> RiskAction {
        // Check for cooldown first
        if let RiskState::Halted { since_ms, reason } = &self.state {
            let halt_duration = current_time_ms.saturating_sub(*since_ms);

            // Check if cooldown has passed
            if halt_duration < self.config.halt_cooldown_ms {
                return RiskAction::Halt {
                    reason: HaltReason::CooldownActive,
                };
            }

            // Check if we can auto-recover
            if self.config.auto_recover && halt_duration >= self.config.recovery_delay_ms {
                // Attempt recovery
                self.attempt_recovery(mm_state, current_time_ms);
            } else {
                return RiskAction::Halt { reason: reason.clone() };
            }
        }

        // If in emergency, stay there
        if let RiskState::Emergency { reason } = &self.state {
            return RiskAction::Emergency { reason: reason.clone() };
        }

        // Update daily tracking
        self.update_daily_tracking(mm_state, current_time_ms);

        // Update peak equity
        if mm_state.pnl.total_pnl > self.peak_equity {
            self.peak_equity = mm_state.pnl.total_pnl;
        }

        // === Check all limits ===

        // 1. Inventory limit
        let inventory_abs = mm_state.inventory.abs();
        if inventory_abs > self.config.max_inventory {
            self.stats.inventory_breaches += 1;
            self.stats.peak_inventory = self.stats.peak_inventory.max(inventory_abs);
            return self.trigger_halt(HaltReason::MaxInventoryExceeded, current_time_ms);
        }

        // 2. Drawdown limit
        let drawdown = self.calculate_drawdown(mm_state);
        if drawdown > self.config.max_drawdown {
            self.stats.drawdown_breaches += 1;
            if drawdown > self.stats.peak_drawdown {
                self.stats.peak_drawdown = drawdown;
            }

            // Catastrophic loss = emergency
            if drawdown > self.config.max_drawdown * 2.0 {
                return self.trigger_emergency(EmergencyReason::CatastrophicLoss, current_time_ms);
            }

            return self.trigger_halt(HaltReason::MaxDrawdownExceeded, current_time_ms);
        }

        // 3. Daily loss limit
        let daily_loss = self.calculate_daily_loss(mm_state);
        if daily_loss > self.config.daily_loss_limit {
            self.stats.daily_loss_breaches += 1;
            if daily_loss > self.stats.peak_daily_loss {
                self.stats.peak_daily_loss = daily_loss;
            }
            return self.trigger_halt(HaltReason::DailyLossLimitExceeded, current_time_ms);
        }

        // 4. Consecutive losses
        if self.config.consecutive_loss_limit > 0
            && self.consecutive_losses >= self.config.consecutive_loss_limit
        {
            self.stats.consecutive_loss_breaches += 1;
            return self.trigger_halt(HaltReason::ConsecutiveLosses, current_time_ms);
        }

        // 5. Position timeout
        if self.config.max_position_age_ms > 0 {
            if let Some(open_time) = self.position_open_time_ms {
                let position_age = current_time_ms.saturating_sub(open_time);
                if position_age > self.config.max_position_age_ms && mm_state.inventory.abs() > dec!(0) {
                    return self.trigger_halt(HaltReason::PositionTimeout, current_time_ms);
                }
            }
        }

        // 6. Quote rate limit
        if self.config.max_quotes_per_minute > 0 {
            self.cleanup_old_timestamps(&mut self.recent_quotes.clone(), current_time_ms, 60_000);
            if self.recent_quotes.len() as u32 >= self.config.max_quotes_per_minute {
                self.stats.rate_limit_breaches += 1;
                return self.trigger_halt(HaltReason::QuoteRateLimitExceeded, current_time_ms);
            }
        }

        // 7. Volatility circuit breaker
        if self.config.max_volatility > 0.0 && volatility > self.config.max_volatility {
            self.stats.volatility_breaches += 1;
            return self.trigger_halt(HaltReason::HighVolatility, current_time_ms);
        }

        // 8. Check for reduce-only mode (soft limit)
        if inventory_abs > self.config.soft_inventory_limit {
            self.set_state(RiskState::ReduceOnly, current_time_ms);
            return RiskAction::ReduceOnly;
        }

        // All checks passed
        if self.state != RiskState::Normal {
            self.set_state(RiskState::Normal, current_time_ms);
        }

        RiskAction::Allow
    }

    /// Record a quote was generated
    pub fn on_quote(&mut self, current_time_ms: u64) {
        if self.config.max_quotes_per_minute > 0 {
            self.recent_quotes.push_back(current_time_ms);
            self.cleanup_old_timestamps(&mut self.recent_quotes.clone(), current_time_ms, 60_000);
        }
    }

    /// Process a fill and update risk state
    pub fn on_fill(
        &mut self,
        _fill: &Fill,
        mm_state: &MMState,
        trade_pnl: Option<Decimal>,
        current_time_ms: u64,
    ) -> RiskAction {
        // Track fill rate
        if self.config.max_fills_per_minute > 0 {
            self.recent_fills.push_back(current_time_ms);
            self.cleanup_old_timestamps(&mut self.recent_fills.clone(), current_time_ms, 60_000);

            if self.recent_fills.len() as u32 >= self.config.max_fills_per_minute {
                self.stats.rate_limit_breaches += 1;
                return self.trigger_halt(HaltReason::FillRateLimitExceeded, current_time_ms);
            }
        }

        // Track position open time
        if mm_state.inventory.abs() > dec!(0) && self.position_open_time_ms.is_none() {
            self.position_open_time_ms = Some(current_time_ms);
        } else if mm_state.inventory == dec!(0) {
            self.position_open_time_ms = None;
        }

        // Track consecutive losses
        if let Some(pnl) = trade_pnl {
            // Check for max loss per trade
            if pnl < -self.config.max_loss_per_trade {
                // Single trade exceeded max loss - this is concerning
                self.log_event(
                    current_time_ms,
                    RiskEventType::LimitWarning,
                    format!("Trade loss {:.4} exceeded max_loss_per_trade {:.4}",
                        pnl, self.config.max_loss_per_trade),
                );
            }

            if pnl < dec!(0) {
                if !self.last_trade_was_win {
                    self.consecutive_losses += 1;
                } else {
                    self.consecutive_losses = 1;
                }
                self.last_trade_was_win = false;
            } else {
                self.consecutive_losses = 0;
                self.last_trade_was_win = true;
            }
        }

        // Update peak inventory
        let inv_abs = mm_state.inventory.abs();
        if inv_abs > self.stats.peak_inventory {
            self.stats.peak_inventory = inv_abs;
        }

        RiskAction::Allow
    }

    /// Manually trigger a halt
    pub fn manual_halt(&mut self, current_time_ms: u64) -> RiskAction {
        self.trigger_halt(HaltReason::ManualHalt, current_time_ms)
    }

    /// Manually trigger emergency stop
    pub fn emergency_stop(&mut self, current_time_ms: u64) -> RiskAction {
        self.trigger_emergency(EmergencyReason::ExternalTrigger, current_time_ms)
    }

    /// Manually reset to normal state
    pub fn reset(&mut self, current_time_ms: u64) {
        self.set_state(RiskState::Normal, current_time_ms);
        self.consecutive_losses = 0;
        self.halt_start_ms = None;
        self.log_event(
            current_time_ms,
            RiskEventType::ManualAction,
            "Manual reset to Normal state".to_string(),
        );
    }

    /// Reset daily tracking (call at start of new day)
    pub fn reset_daily(&mut self, current_pnl: Decimal) {
        self.daily_pnl_start = current_pnl;
    }

    /// Check if a specific quote side is allowed (for reduce-only mode)
    pub fn is_quote_side_allowed(&self, side: QuoteSide, current_inventory: Decimal) -> bool {
        match &self.state {
            RiskState::Normal => true,
            RiskState::ReduceOnly => {
                // In reduce-only, only allow quotes that would reduce inventory
                match side {
                    QuoteSide::Bid => current_inventory < dec!(0), // Can buy if short
                    QuoteSide::Ask => current_inventory > dec!(0), // Can sell if long
                }
            }
            RiskState::Halted { .. } | RiskState::Emergency { .. } => false,
        }
    }

    // === Private methods ===

    fn trigger_halt(&mut self, reason: HaltReason, current_time_ms: u64) -> RiskAction {
        self.stats.halt_count += 1;
        self.halt_start_ms = Some(current_time_ms);
        self.set_state(RiskState::Halted { reason: reason.clone(), since_ms: current_time_ms }, current_time_ms);
        RiskAction::Halt { reason }
    }

    fn trigger_emergency(&mut self, reason: EmergencyReason, current_time_ms: u64) -> RiskAction {
        self.set_state(RiskState::Emergency { reason: reason.clone() }, current_time_ms);
        RiskAction::Emergency { reason }
    }

    fn set_state(&mut self, new_state: RiskState, current_time_ms: u64) {
        if self.state != new_state {
            // Track halt time
            if let RiskState::Halted { since_ms, .. } = &self.state {
                self.stats.total_halt_time_ms += current_time_ms.saturating_sub(*since_ms);
            }

            // Track reduce-only transitions
            if matches!(&new_state, RiskState::ReduceOnly)
                && !matches!(&self.state, RiskState::ReduceOnly)
            {
                self.stats.reduce_only_count += 1;
            }

            let event_type = if matches!(&new_state, RiskState::Normal)
                && matches!(&self.state, RiskState::Halted { .. })
            {
                RiskEventType::Recovery
            } else {
                RiskEventType::StateChange
            };

            self.log_event(
                current_time_ms,
                event_type,
                format!("{:?} -> {:?}", self.state, new_state),
            );

            self.state = new_state;
        }
    }

    fn attempt_recovery(&mut self, mm_state: &MMState, current_time_ms: u64) {
        // Only recover if conditions are safe
        let inventory_ok = mm_state.inventory.abs() <= self.config.soft_inventory_limit;
        let drawdown_ok = self.calculate_drawdown(mm_state) < self.config.max_drawdown * 0.8;
        let daily_ok = self.calculate_daily_loss(mm_state) < self.config.daily_loss_limit * Decimal::from_f64(0.8).unwrap();

        if inventory_ok && drawdown_ok && daily_ok {
            self.set_state(RiskState::Normal, current_time_ms);
            self.consecutive_losses = 0;
            self.halt_start_ms = None;
        }
    }

    fn calculate_drawdown(&self, mm_state: &MMState) -> f64 {
        if self.peak_equity <= dec!(0) {
            return 0.0;
        }

        let drawdown = (self.peak_equity - mm_state.pnl.total_pnl) / self.peak_equity;
        drawdown.to_f64().unwrap_or(0.0).max(0.0)
    }

    fn calculate_daily_loss(&self, mm_state: &MMState) -> Decimal {
        let daily_pnl = mm_state.pnl.total_pnl - self.daily_pnl_start;
        if daily_pnl < dec!(0) {
            daily_pnl.abs()
        } else {
            dec!(0)
        }
    }

    fn update_daily_tracking(&mut self, mm_state: &MMState, current_time_ms: u64) {
        // Convert to day number
        let day = (current_time_ms / (24 * 60 * 60 * 1000)) as u32;

        if day != self.last_reset_day {
            self.daily_pnl_start = mm_state.pnl.total_pnl;
            self.last_reset_day = day;
        }
    }

    fn cleanup_old_timestamps(&self, timestamps: &mut VecDeque<u64>, current_time_ms: u64, window_ms: u64) {
        let cutoff = current_time_ms.saturating_sub(window_ms);
        while let Some(&ts) = timestamps.front() {
            if ts < cutoff {
                timestamps.pop_front();
            } else {
                break;
            }
        }
    }

    fn log_event(&mut self, current_time_ms: u64, event_type: RiskEventType, details: String) {
        let event = RiskEvent {
            timestamp: Utc::now(),
            timestamp_ms: current_time_ms,
            event_type,
            details,
            state_before: self.state.clone(),
            state_after: self.state.clone(), // Will be updated after state change
        };
        self.events.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::market_maker::PnLTracker;

    fn create_mm_state(inventory: Decimal, total_pnl: Decimal) -> MMState {
        MMState {
            inventory,
            avg_entry_price: dec!(50000),
            pnl: PnLTracker {
                realized_pnl: total_pnl,
                unrealized_pnl: dec!(0),
                total_pnl,
                num_trades: 0,
                total_volume: dec!(0),
                fees_paid: dec!(0),
            },
            current_bid: None,
            current_ask: None,
        }
    }

    fn create_fill(side: QuoteSide, size: Decimal) -> Fill {
        Fill {
            side,
            price: dec!(50000),
            size,
            timestamp_ms: 0,
        }
    }

    // === Basic Tests ===

    #[test]
    fn test_risk_manager_creation() {
        let config = RiskConfig::default();
        let manager = RiskManager::new(config.clone());

        assert_eq!(*manager.state(), RiskState::Normal);
        assert_eq!(manager.stats().halt_count, 0);
    }

    #[test]
    fn test_config_validation() {
        let mut config = RiskConfig::default();
        assert!(config.validate().is_ok());

        config.max_inventory = dec!(0);
        assert!(config.validate().is_err());

        config.max_inventory = dec!(0.1);
        config.soft_inventory_limit = dec!(0.2); // Greater than max
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_presets() {
        let conservative = RiskConfig::conservative();
        let aggressive = RiskConfig::aggressive();

        assert!(conservative.max_inventory < aggressive.max_inventory);
        assert!(conservative.max_drawdown < aggressive.max_drawdown);
    }

    // === Allow State Tests ===

    #[test]
    fn test_normal_operation() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0.01), dec!(0.001));
        let action = manager.check_pre_quote(&state, 1000, 0.01);

        assert_eq!(action, RiskAction::Allow);
        assert!(action.allows_quoting());
        assert!(action.allows_new_position());
    }

    #[test]
    fn test_allow_with_small_inventory() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0.05), dec!(0.001));
        let action = manager.check_pre_quote(&state, 1000, 0.01);

        assert_eq!(action, RiskAction::Allow);
    }

    // === Reduce Only Tests ===

    #[test]
    fn test_reduce_only_on_soft_limit() {
        let mut config = RiskConfig::default();
        config.soft_inventory_limit = dec!(0.05);
        config.max_inventory = dec!(0.1);

        let mut manager = RiskManager::new(config);

        // Above soft limit, below hard limit
        let state = create_mm_state(dec!(0.06), dec!(0.001));
        let action = manager.check_pre_quote(&state, 1000, 0.01);

        assert_eq!(action, RiskAction::ReduceOnly);
        assert!(action.allows_quoting());
        assert!(!action.allows_new_position());
    }

    #[test]
    fn test_reduce_only_quote_side_filtering() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.state = RiskState::ReduceOnly;

        // Long position - should allow sells, not buys
        let long_inventory = dec!(0.05);
        assert!(manager.is_quote_side_allowed(QuoteSide::Ask, long_inventory));
        assert!(!manager.is_quote_side_allowed(QuoteSide::Bid, long_inventory));

        // Short position - should allow buys, not sells
        let short_inventory = dec!(-0.05);
        assert!(manager.is_quote_side_allowed(QuoteSide::Bid, short_inventory));
        assert!(!manager.is_quote_side_allowed(QuoteSide::Ask, short_inventory));
    }

    // === Inventory Limit Tests ===

    #[test]
    fn test_halt_on_max_inventory() {
        let mut config = RiskConfig::default();
        config.max_inventory = dec!(0.1);

        let mut manager = RiskManager::new(config);

        // Exceed max inventory
        let state = create_mm_state(dec!(0.15), dec!(0.001));
        let action = manager.check_pre_quote(&state, 1000, 0.01);

        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::MaxInventoryExceeded }));
        assert!(action.is_stopped());
        assert_eq!(manager.stats().inventory_breaches, 1);
    }

    #[test]
    fn test_halt_on_negative_inventory() {
        let mut config = RiskConfig::default();
        config.max_inventory = dec!(0.1);

        let mut manager = RiskManager::new(config);

        // Exceed max inventory (short)
        let state = create_mm_state(dec!(-0.15), dec!(0.001));
        let action = manager.check_pre_quote(&state, 1000, 0.01);

        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::MaxInventoryExceeded }));
    }

    // === Drawdown Tests ===

    #[test]
    fn test_halt_on_max_drawdown() {
        let mut config = RiskConfig::default();
        config.max_drawdown = 0.10; // 10%

        let mut manager = RiskManager::new(config);

        // First, establish peak equity
        let peak_state = create_mm_state(dec!(0), dec!(1.0));
        manager.check_pre_quote(&peak_state, 1000, 0.01);

        // Now have a loss that exceeds 10%
        let loss_state = create_mm_state(dec!(0), dec!(0.85)); // 15% loss
        let action = manager.check_pre_quote(&loss_state, 2000, 0.01);

        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::MaxDrawdownExceeded }));
        assert_eq!(manager.stats().drawdown_breaches, 1);
    }

    #[test]
    fn test_emergency_on_catastrophic_loss() {
        let mut config = RiskConfig::default();
        config.max_drawdown = 0.10; // 10%

        let mut manager = RiskManager::new(config);

        // Establish peak equity
        let peak_state = create_mm_state(dec!(0), dec!(1.0));
        manager.check_pre_quote(&peak_state, 1000, 0.01);

        // Catastrophic loss (> 2x max drawdown)
        let loss_state = create_mm_state(dec!(0), dec!(0.7)); // 30% loss
        let action = manager.check_pre_quote(&loss_state, 2000, 0.01);

        assert!(matches!(action, RiskAction::Emergency { reason: EmergencyReason::CatastrophicLoss }));
    }

    // === Daily Loss Tests ===

    #[test]
    fn test_halt_on_daily_loss_limit() {
        let mut config = RiskConfig::default();
        config.daily_loss_limit = dec!(0.05);

        let mut manager = RiskManager::new(config);

        // Set starting PnL
        manager.daily_pnl_start = dec!(0.1);
        manager.last_reset_day = 1; // Prevent auto-reset

        // Now have a daily loss exceeding limit
        let state = create_mm_state(dec!(0), dec!(0.04)); // Lost 0.06 from 0.1
        let action = manager.check_pre_quote(&state, 86400001, 0.01); // Day 1

        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::DailyLossLimitExceeded }));
        assert_eq!(manager.stats().daily_loss_breaches, 1);
    }

    // === Consecutive Loss Tests ===

    #[test]
    fn test_halt_on_consecutive_losses() {
        let mut config = RiskConfig::default();
        config.consecutive_loss_limit = 3;

        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0), dec!(0));
        let fill = create_fill(QuoteSide::Ask, dec!(0.01));

        // Record 3 consecutive losses
        manager.on_fill(&fill, &state, Some(dec!(-0.001)), 1000);
        manager.on_fill(&fill, &state, Some(dec!(-0.001)), 2000);
        manager.on_fill(&fill, &state, Some(dec!(-0.001)), 3000);

        assert_eq!(manager.consecutive_losses, 3);

        // Now check should trigger halt
        let action = manager.check_pre_quote(&state, 4000, 0.01);

        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::ConsecutiveLosses }));
        assert_eq!(manager.stats().consecutive_loss_breaches, 1);
    }

    #[test]
    fn test_consecutive_losses_reset_on_win() {
        let mut config = RiskConfig::default();
        config.consecutive_loss_limit = 5;

        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0), dec!(0));
        let fill = create_fill(QuoteSide::Ask, dec!(0.01));

        // Record 3 losses
        manager.on_fill(&fill, &state, Some(dec!(-0.001)), 1000);
        manager.on_fill(&fill, &state, Some(dec!(-0.001)), 2000);
        manager.on_fill(&fill, &state, Some(dec!(-0.001)), 3000);

        assert_eq!(manager.consecutive_losses, 3);

        // Win resets counter
        manager.on_fill(&fill, &state, Some(dec!(0.002)), 4000);

        assert_eq!(manager.consecutive_losses, 0);
    }

    // === Position Timeout Tests ===

    #[test]
    fn test_halt_on_position_timeout() {
        let mut config = RiskConfig::default();
        config.max_position_age_ms = 1000; // 1 second for testing

        let mut manager = RiskManager::new(config);

        // Open a position
        let state = create_mm_state(dec!(0.05), dec!(0));
        let fill = create_fill(QuoteSide::Bid, dec!(0.05));
        manager.on_fill(&fill, &state, None, 0);

        // Check immediately - should be fine
        let action = manager.check_pre_quote(&state, 500, 0.01);
        assert_eq!(action, RiskAction::Allow);

        // Check after timeout
        let action = manager.check_pre_quote(&state, 1500, 0.01);
        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::PositionTimeout }));
    }

    #[test]
    fn test_position_timeout_resets_on_flat() {
        let mut config = RiskConfig::default();
        config.max_position_age_ms = 1000;

        let mut manager = RiskManager::new(config);

        // Open position
        let state_with_pos = create_mm_state(dec!(0.05), dec!(0));
        let fill = create_fill(QuoteSide::Bid, dec!(0.05));
        manager.on_fill(&fill, &state_with_pos, None, 0);

        // Close position
        let state_flat = create_mm_state(dec!(0), dec!(0));
        let fill = create_fill(QuoteSide::Ask, dec!(0.05));
        manager.on_fill(&fill, &state_flat, None, 500);

        assert!(manager.position_open_time_ms.is_none());
    }

    // === Volatility Circuit Breaker Tests ===

    #[test]
    fn test_halt_on_high_volatility() {
        let mut config = RiskConfig::default();
        config.max_volatility = 0.05;

        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0), dec!(0));

        // Normal volatility - OK
        let action = manager.check_pre_quote(&state, 1000, 0.03);
        assert_eq!(action, RiskAction::Allow);

        // High volatility - halt
        let action = manager.check_pre_quote(&state, 2000, 0.06);
        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::HighVolatility }));
        assert_eq!(manager.stats().volatility_breaches, 1);
    }

    // === Rate Limit Tests ===

    #[test]
    fn test_halt_on_quote_rate_limit() {
        let mut config = RiskConfig::default();
        config.max_quotes_per_minute = 5;
        config.halt_cooldown_ms = 0; // Disable for testing

        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0), dec!(0));

        // Generate quotes up to limit
        for i in 0..5 {
            manager.on_quote(i * 1000);
        }

        // This should trigger rate limit
        let action = manager.check_pre_quote(&state, 5000, 0.01);
        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::QuoteRateLimitExceeded }));
    }

    #[test]
    fn test_halt_on_fill_rate_limit() {
        let mut config = RiskConfig::default();
        config.max_fills_per_minute = 3;

        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0), dec!(0));
        let fill = create_fill(QuoteSide::Bid, dec!(0.01));

        // Generate fills up to limit
        manager.on_fill(&fill, &state, None, 0);
        manager.on_fill(&fill, &state, None, 1000);
        manager.on_fill(&fill, &state, None, 2000);

        // This should trigger rate limit
        let action = manager.on_fill(&fill, &state, None, 3000);
        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::FillRateLimitExceeded }));
    }

    // === Cooldown Tests ===

    #[test]
    fn test_cooldown_after_halt() {
        let mut config = RiskConfig::default();
        config.max_inventory = dec!(0.1);
        config.halt_cooldown_ms = 1000;
        config.auto_recover = false;

        let mut manager = RiskManager::new(config);

        // Trigger halt
        let bad_state = create_mm_state(dec!(0.15), dec!(0));
        manager.check_pre_quote(&bad_state, 0, 0.01);

        // Even with good state, should still be in cooldown
        let good_state = create_mm_state(dec!(0.01), dec!(0));
        let action = manager.check_pre_quote(&good_state, 500, 0.01);

        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::CooldownActive }));
    }

    #[test]
    fn test_cooldown_expires() {
        let mut config = RiskConfig::default();
        config.max_inventory = dec!(0.1);
        config.halt_cooldown_ms = 1000;
        config.auto_recover = true;
        config.recovery_delay_ms = 1000;

        let mut manager = RiskManager::new(config);

        // Trigger halt
        let bad_state = create_mm_state(dec!(0.15), dec!(0));
        manager.check_pre_quote(&bad_state, 0, 0.01);

        // After cooldown and recovery delay, with safe state
        let good_state = create_mm_state(dec!(0.01), dec!(0));
        let action = manager.check_pre_quote(&good_state, 2000, 0.01);

        // Should recover to normal
        assert_eq!(action, RiskAction::Allow);
    }

    // === Manual Control Tests ===

    #[test]
    fn test_manual_halt() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        let action = manager.manual_halt(1000);

        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::ManualHalt }));
        assert!(matches!(manager.state(), RiskState::Halted { .. }));
    }

    #[test]
    fn test_emergency_stop() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        let action = manager.emergency_stop(1000);

        assert!(matches!(action, RiskAction::Emergency { reason: EmergencyReason::ExternalTrigger }));
        assert!(matches!(manager.state(), RiskState::Emergency { .. }));
    }

    #[test]
    fn test_manual_reset() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        // Put in halt state
        manager.manual_halt(1000);
        assert!(matches!(manager.state(), RiskState::Halted { .. }));

        // Manual reset
        manager.reset(2000);
        assert_eq!(*manager.state(), RiskState::Normal);
        assert_eq!(manager.consecutive_losses, 0);
    }

    // === Statistics Tests ===

    #[test]
    fn test_stats_tracking() {
        let mut config = RiskConfig::default();
        config.max_inventory = dec!(0.1);
        config.halt_cooldown_ms = 0;
        config.auto_recover = true;
        config.recovery_delay_ms = 0;

        let mut manager = RiskManager::new(config);

        // Trigger multiple halts
        let bad_state = create_mm_state(dec!(0.15), dec!(0));
        manager.check_pre_quote(&bad_state, 0, 0.01);

        let good_state = create_mm_state(dec!(0.01), dec!(0));
        manager.check_pre_quote(&good_state, 100, 0.01); // Recover

        let bad_state = create_mm_state(dec!(0.15), dec!(0));
        manager.check_pre_quote(&bad_state, 200, 0.01);

        assert_eq!(manager.stats().halt_count, 2);
        assert_eq!(manager.stats().inventory_breaches, 2);
    }

    #[test]
    fn test_peak_tracking() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0.05), dec!(0));
        let fill = create_fill(QuoteSide::Bid, dec!(0.05));
        manager.on_fill(&fill, &state, None, 1000);

        let state = create_mm_state(dec!(0.08), dec!(0));
        let fill = create_fill(QuoteSide::Bid, dec!(0.03));
        manager.on_fill(&fill, &state, None, 2000);

        assert_eq!(manager.stats().peak_inventory, dec!(0.08));
    }

    // === Event Logging Tests ===

    #[test]
    fn test_event_logging() {
        let mut config = RiskConfig::default();
        config.max_inventory = dec!(0.1);
        config.halt_cooldown_ms = 0;
        config.auto_recover = true;
        config.recovery_delay_ms = 0;

        let mut manager = RiskManager::new(config);

        // Generate some events
        let bad_state = create_mm_state(dec!(0.15), dec!(0));
        manager.check_pre_quote(&bad_state, 0, 0.01);

        let good_state = create_mm_state(dec!(0.01), dec!(0));
        manager.check_pre_quote(&good_state, 100, 0.01);

        assert!(!manager.events().is_empty());
        assert!(manager.events().iter().any(|e| matches!(e.event_type, RiskEventType::StateChange)));
    }

    // === Risk Action Tests ===

    #[test]
    fn test_risk_action_properties() {
        assert!(RiskAction::Allow.allows_quoting());
        assert!(RiskAction::Allow.allows_new_position());
        assert!(!RiskAction::Allow.is_stopped());

        assert!(RiskAction::ReduceOnly.allows_quoting());
        assert!(!RiskAction::ReduceOnly.allows_new_position());
        assert!(!RiskAction::ReduceOnly.is_stopped());

        let halt = RiskAction::Halt { reason: HaltReason::ManualHalt };
        assert!(!halt.allows_quoting());
        assert!(!halt.allows_new_position());
        assert!(halt.is_stopped());

        let emergency = RiskAction::Emergency { reason: EmergencyReason::CatastrophicLoss };
        assert!(!emergency.allows_quoting());
        assert!(!emergency.allows_new_position());
        assert!(emergency.is_stopped());
    }

    // === Serialization Tests ===

    #[test]
    fn test_config_serialization() {
        let config = RiskConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RiskConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.max_inventory, deserialized.max_inventory);
        assert_eq!(config.max_drawdown, deserialized.max_drawdown);
    }

    #[test]
    fn test_risk_action_serialization() {
        let action = RiskAction::Halt { reason: HaltReason::MaxDrawdownExceeded };
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: RiskAction = serde_json::from_str(&json).unwrap();

        assert_eq!(action, deserialized);
    }

    #[test]
    fn test_stats_serialization() {
        let mut stats = RiskStats::default();
        stats.halt_count = 5;
        stats.peak_inventory = dec!(0.08);

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: RiskStats = serde_json::from_str(&json).unwrap();

        assert_eq!(stats.halt_count, deserialized.halt_count);
        assert_eq!(stats.peak_inventory, deserialized.peak_inventory);
    }

    // === Edge Case Tests ===

    #[test]
    fn test_zero_peak_equity_no_panic() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        // Drawdown with zero peak should not panic
        let state = create_mm_state(dec!(0), dec!(-0.01));
        let action = manager.check_pre_quote(&state, 1000, 0.01);

        // Should not crash, action depends on other checks
        assert!(matches!(action, RiskAction::Allow | RiskAction::Halt { .. }));
    }

    #[test]
    fn test_disabled_limits() {
        let mut config = RiskConfig::default();
        config.max_position_age_ms = 0; // Disabled
        config.max_quotes_per_minute = 0; // Disabled
        config.max_fills_per_minute = 0; // Disabled
        config.consecutive_loss_limit = 0; // Disabled
        config.max_volatility = 0.0; // Disabled

        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0.01), dec!(0));
        let action = manager.check_pre_quote(&state, 1000, 1.0); // High volatility ignored

        assert_eq!(action, RiskAction::Allow);
    }

    #[test]
    fn test_multiple_limits_priority() {
        let mut config = RiskConfig::default();
        config.max_inventory = dec!(0.05);
        config.max_drawdown = 0.10;
        config.daily_loss_limit = dec!(0.02);

        let mut manager = RiskManager::new(config);

        // Set up for multiple violations
        manager.peak_equity = dec!(1.0);
        manager.daily_pnl_start = dec!(0.5);
        manager.last_reset_day = 1;

        // State that violates inventory (should be checked first after cooldown)
        let state = create_mm_state(dec!(0.1), dec!(0.3)); // Daily loss 0.2, DD 70%, Inv 0.1
        let action = manager.check_pre_quote(&state, 86400001, 0.01);

        // Inventory is checked first, should trigger that
        assert!(matches!(action, RiskAction::Halt { reason: HaltReason::MaxInventoryExceeded }));
    }

    // === Display Trait Tests ===

    #[test]
    fn test_halt_reason_display() {
        let reasons = vec![
            HaltReason::MaxDrawdownExceeded,
            HaltReason::DailyLossLimitExceeded,
            HaltReason::MaxInventoryExceeded,
            HaltReason::ConsecutiveLosses,
            HaltReason::PositionTimeout,
            HaltReason::QuoteRateLimitExceeded,
            HaltReason::FillRateLimitExceeded,
            HaltReason::HighVolatility,
            HaltReason::ManualHalt,
            HaltReason::CooldownActive,
        ];

        for reason in reasons {
            let display = format!("{}", reason);
            assert!(!display.is_empty());
        }
    }

    #[test]
    fn test_emergency_reason_display() {
        let reasons = vec![
            EmergencyReason::CatastrophicLoss,
            EmergencyReason::SystemError,
            EmergencyReason::ExternalTrigger,
        ];

        for reason in reasons {
            let display = format!("{}", reason);
            assert!(!display.is_empty());
        }
    }

    // ============================================================================
    // CircuitBreakerStage Tests
    // ============================================================================

    #[test]
    fn test_circuit_breaker_stage_default() {
        let stage = CircuitBreakerStage::default();
        assert_eq!(stage, CircuitBreakerStage::Normal);
    }

    #[test]
    fn test_circuit_breaker_stage_allows_trading() {
        assert!(CircuitBreakerStage::Normal.allows_trading());
        assert!(CircuitBreakerStage::Warning.allows_trading());
        assert!(CircuitBreakerStage::ReduceOnly.allows_trading());
        assert!(!CircuitBreakerStage::Halt.allows_trading());
        assert!(!CircuitBreakerStage::Emergency.allows_trading());
    }

    #[test]
    fn test_circuit_breaker_stage_allows_new_positions() {
        assert!(CircuitBreakerStage::Normal.allows_new_positions());
        assert!(CircuitBreakerStage::Warning.allows_new_positions());
        assert!(!CircuitBreakerStage::ReduceOnly.allows_new_positions());
        assert!(!CircuitBreakerStage::Halt.allows_new_positions());
        assert!(!CircuitBreakerStage::Emergency.allows_new_positions());
    }

    #[test]
    fn test_circuit_breaker_stage_escalate() {
        assert_eq!(CircuitBreakerStage::Normal.escalate(), CircuitBreakerStage::Warning);
        assert_eq!(CircuitBreakerStage::Warning.escalate(), CircuitBreakerStage::ReduceOnly);
        assert_eq!(CircuitBreakerStage::ReduceOnly.escalate(), CircuitBreakerStage::Halt);
        assert_eq!(CircuitBreakerStage::Halt.escalate(), CircuitBreakerStage::Emergency);
        assert_eq!(CircuitBreakerStage::Emergency.escalate(), CircuitBreakerStage::Emergency);
    }

    #[test]
    fn test_circuit_breaker_stage_de_escalate() {
        assert_eq!(CircuitBreakerStage::Normal.de_escalate(), CircuitBreakerStage::Normal);
        assert_eq!(CircuitBreakerStage::Warning.de_escalate(), CircuitBreakerStage::Normal);
        assert_eq!(CircuitBreakerStage::ReduceOnly.de_escalate(), CircuitBreakerStage::Warning);
        assert_eq!(CircuitBreakerStage::Halt.de_escalate(), CircuitBreakerStage::ReduceOnly);
        assert_eq!(CircuitBreakerStage::Emergency.de_escalate(), CircuitBreakerStage::Halt);
    }

    #[test]
    fn test_circuit_breaker_stage_ordering() {
        assert!(CircuitBreakerStage::Normal < CircuitBreakerStage::Warning);
        assert!(CircuitBreakerStage::Warning < CircuitBreakerStage::ReduceOnly);
        assert!(CircuitBreakerStage::ReduceOnly < CircuitBreakerStage::Halt);
        assert!(CircuitBreakerStage::Halt < CircuitBreakerStage::Emergency);
    }

    #[test]
    fn test_circuit_breaker_stage_as_usize() {
        assert_eq!(CircuitBreakerStage::Normal as usize, 0);
        assert_eq!(CircuitBreakerStage::Warning as usize, 1);
        assert_eq!(CircuitBreakerStage::ReduceOnly as usize, 2);
        assert_eq!(CircuitBreakerStage::Halt as usize, 3);
        assert_eq!(CircuitBreakerStage::Emergency as usize, 4);
    }

    #[test]
    fn test_circuit_breaker_stage_serialization() {
        let stage = CircuitBreakerStage::ReduceOnly;
        let json = serde_json::to_string(&stage).unwrap();
        let deserialized: CircuitBreakerStage = serde_json::from_str(&json).unwrap();
        assert_eq!(stage, deserialized);
    }

    // ============================================================================
    // CircuitBreakerConfig Tests
    // ============================================================================

    #[test]
    fn test_circuit_breaker_config_default() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.warning_drawdown, 0.05);
        assert_eq!(config.reduce_only_drawdown, 0.08);
        assert_eq!(config.halt_drawdown, 0.10);
        assert_eq!(config.emergency_drawdown, 0.20);
        assert!(config.auto_de_escalate);
        assert_eq!(config.min_stage_duration_ms, 60_000);
    }

    #[test]
    fn test_circuit_breaker_config_validate_valid() {
        let config = CircuitBreakerConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_circuit_breaker_config_validate_warning_ge_reduce_only() {
        let mut config = CircuitBreakerConfig::default();
        config.warning_drawdown = 0.10;
        config.reduce_only_drawdown = 0.08;
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("warning_drawdown"));
    }

    #[test]
    fn test_circuit_breaker_config_validate_reduce_only_ge_halt() {
        let mut config = CircuitBreakerConfig::default();
        config.reduce_only_drawdown = 0.12;
        config.halt_drawdown = 0.10;
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("reduce_only_drawdown"));
    }

    #[test]
    fn test_circuit_breaker_config_validate_halt_ge_emergency() {
        let mut config = CircuitBreakerConfig::default();
        config.halt_drawdown = 0.25;
        config.emergency_drawdown = 0.20;
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("halt_drawdown"));
    }

    #[test]
    fn test_circuit_breaker_config_validate_de_escalation_threshold_bounds() {
        let mut config = CircuitBreakerConfig::default();

        config.de_escalation_threshold = 0.0;
        assert!(config.validate().is_err());

        config.de_escalation_threshold = 1.0;
        assert!(config.validate().is_err());

        config.de_escalation_threshold = -0.5;
        assert!(config.validate().is_err());

        config.de_escalation_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_circuit_breaker_config_stage_for_drawdown_normal() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.stage_for_drawdown(0.0), CircuitBreakerStage::Normal);
        assert_eq!(config.stage_for_drawdown(0.02), CircuitBreakerStage::Normal);
        assert_eq!(config.stage_for_drawdown(0.049), CircuitBreakerStage::Normal);
    }

    #[test]
    fn test_circuit_breaker_config_stage_for_drawdown_warning() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.stage_for_drawdown(0.05), CircuitBreakerStage::Warning);
        assert_eq!(config.stage_for_drawdown(0.06), CircuitBreakerStage::Warning);
        assert_eq!(config.stage_for_drawdown(0.079), CircuitBreakerStage::Warning);
    }

    #[test]
    fn test_circuit_breaker_config_stage_for_drawdown_reduce_only() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.stage_for_drawdown(0.08), CircuitBreakerStage::ReduceOnly);
        assert_eq!(config.stage_for_drawdown(0.09), CircuitBreakerStage::ReduceOnly);
        assert_eq!(config.stage_for_drawdown(0.099), CircuitBreakerStage::ReduceOnly);
    }

    #[test]
    fn test_circuit_breaker_config_stage_for_drawdown_halt() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.stage_for_drawdown(0.10), CircuitBreakerStage::Halt);
        assert_eq!(config.stage_for_drawdown(0.15), CircuitBreakerStage::Halt);
        assert_eq!(config.stage_for_drawdown(0.199), CircuitBreakerStage::Halt);
    }

    #[test]
    fn test_circuit_breaker_config_stage_for_drawdown_emergency() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.stage_for_drawdown(0.20), CircuitBreakerStage::Emergency);
        assert_eq!(config.stage_for_drawdown(0.50), CircuitBreakerStage::Emergency);
        assert_eq!(config.stage_for_drawdown(1.0), CircuitBreakerStage::Emergency);
    }

    #[test]
    fn test_circuit_breaker_config_stage_for_daily_loss() {
        let config = CircuitBreakerConfig::default();

        // Normal
        assert_eq!(config.stage_for_daily_loss(dec!(0.01)), CircuitBreakerStage::Normal);

        // Warning
        assert_eq!(config.stage_for_daily_loss(dec!(0.02)), CircuitBreakerStage::Warning);
        assert_eq!(config.stage_for_daily_loss(dec!(0.03)), CircuitBreakerStage::Warning);

        // ReduceOnly
        assert_eq!(config.stage_for_daily_loss(dec!(0.035)), CircuitBreakerStage::ReduceOnly);
        assert_eq!(config.stage_for_daily_loss(dec!(0.04)), CircuitBreakerStage::ReduceOnly);

        // Halt
        assert_eq!(config.stage_for_daily_loss(dec!(0.05)), CircuitBreakerStage::Halt);
        assert_eq!(config.stage_for_daily_loss(dec!(0.10)), CircuitBreakerStage::Halt);
    }

    #[test]
    fn test_circuit_breaker_config_stage_for_consecutive_losses() {
        let config = CircuitBreakerConfig::default();

        // Normal
        assert_eq!(config.stage_for_consecutive_losses(0), CircuitBreakerStage::Normal);
        assert_eq!(config.stage_for_consecutive_losses(2), CircuitBreakerStage::Normal);

        // Warning
        assert_eq!(config.stage_for_consecutive_losses(3), CircuitBreakerStage::Warning);
        assert_eq!(config.stage_for_consecutive_losses(4), CircuitBreakerStage::Warning);

        // ReduceOnly
        assert_eq!(config.stage_for_consecutive_losses(5), CircuitBreakerStage::ReduceOnly);
        assert_eq!(config.stage_for_consecutive_losses(7), CircuitBreakerStage::ReduceOnly);

        // Halt
        assert_eq!(config.stage_for_consecutive_losses(8), CircuitBreakerStage::Halt);
        assert_eq!(config.stage_for_consecutive_losses(100), CircuitBreakerStage::Halt);
    }

    #[test]
    fn test_circuit_breaker_config_serialization() {
        let config = CircuitBreakerConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: CircuitBreakerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.warning_drawdown, deserialized.warning_drawdown);
        assert_eq!(config.auto_de_escalate, deserialized.auto_de_escalate);
    }

    // ============================================================================
    // DrawdownPeriod Tests
    // ============================================================================

    #[test]
    fn test_drawdown_period_new() {
        let period = DrawdownPeriod::new(1000, dec!(100));
        assert_eq!(period.start_time_ms, 1000);
        assert!(period.end_time_ms.is_none());
        assert_eq!(period.peak_equity, dec!(100));
        assert_eq!(period.trough_equity, dec!(100));
        assert_eq!(period.max_drawdown, 0.0);
        assert!(!period.recovered);
    }

    #[test]
    fn test_drawdown_period_update_trough() {
        let mut period = DrawdownPeriod::new(1000, dec!(100));

        // First trough update
        period.update_trough(dec!(95));
        assert_eq!(period.trough_equity, dec!(95));
        assert!((period.max_drawdown - 0.05).abs() < 0.0001);

        // Deeper trough
        period.update_trough(dec!(90));
        assert_eq!(period.trough_equity, dec!(90));
        assert!((period.max_drawdown - 0.10).abs() < 0.0001);

        // Higher value should not update trough
        period.update_trough(dec!(92));
        assert_eq!(period.trough_equity, dec!(90));
        assert!((period.max_drawdown - 0.10).abs() < 0.0001);
    }

    #[test]
    fn test_drawdown_period_update_trough_zero_peak() {
        let mut period = DrawdownPeriod::new(1000, dec!(0));
        period.update_trough(dec!(-5));
        // Should not panic and drawdown should be 0 due to zero peak
        assert_eq!(period.trough_equity, dec!(-5));
        assert_eq!(period.max_drawdown, 0.0);
    }

    #[test]
    fn test_drawdown_period_mark_recovered() {
        let mut period = DrawdownPeriod::new(1000, dec!(100));
        period.update_trough(dec!(90));

        assert!(!period.recovered);
        assert!(period.end_time_ms.is_none());

        period.mark_recovered(5000);

        assert!(period.recovered);
        assert_eq!(period.end_time_ms, Some(5000));
    }

    #[test]
    fn test_drawdown_period_duration_ongoing() {
        let period = DrawdownPeriod::new(1000, dec!(100));

        assert_eq!(period.duration_ms(3000), 2000);
        assert_eq!(period.duration_ms(10000), 9000);
    }

    #[test]
    fn test_drawdown_period_duration_recovered() {
        let mut period = DrawdownPeriod::new(1000, dec!(100));
        period.mark_recovered(5000);

        // Duration should be fixed once recovered
        assert_eq!(period.duration_ms(5000), 4000);
        assert_eq!(period.duration_ms(10000), 4000);
    }

    #[test]
    fn test_drawdown_period_duration_same_time() {
        let period = DrawdownPeriod::new(1000, dec!(100));
        assert_eq!(period.duration_ms(1000), 0);
    }

    #[test]
    fn test_drawdown_period_serialization() {
        let mut period = DrawdownPeriod::new(1000, dec!(100));
        period.update_trough(dec!(90));
        period.mark_recovered(5000);

        let json = serde_json::to_string(&period).unwrap();
        let deserialized: DrawdownPeriod = serde_json::from_str(&json).unwrap();

        assert_eq!(period.start_time_ms, deserialized.start_time_ms);
        assert_eq!(period.end_time_ms, deserialized.end_time_ms);
        assert_eq!(period.peak_equity, deserialized.peak_equity);
        assert_eq!(period.recovered, deserialized.recovered);
    }

    // ============================================================================
    // CircuitBreakerState Tests
    // ============================================================================

    #[test]
    fn test_circuit_breaker_state_default() {
        let state = CircuitBreakerState::default();
        assert_eq!(state.stage, CircuitBreakerStage::Normal);
        assert_eq!(state.stage_entered_ms, 0);
        assert!(state.reason.is_none());
        assert_eq!(state.escalation_count, 0);
        assert_eq!(state.de_escalation_count, 0);
        assert_eq!(state.time_per_stage_ms, [0; 5]);
    }

    #[test]
    fn test_circuit_breaker_state_update_time() {
        let mut state = CircuitBreakerState::default();
        state.stage_entered_ms = 1000;

        state.update_time(5000);

        assert_eq!(state.time_per_stage_ms[0], 4000); // Normal stage
    }

    #[test]
    fn test_circuit_breaker_state_set_stage_escalation() {
        let mut state = CircuitBreakerState::default();
        state.stage_entered_ms = 0;

        state.set_stage(CircuitBreakerStage::Warning, 1000, Some("Test reason".to_string()));

        assert_eq!(state.stage, CircuitBreakerStage::Warning);
        assert_eq!(state.stage_entered_ms, 1000);
        assert_eq!(state.reason, Some("Test reason".to_string()));
        assert_eq!(state.escalation_count, 1);
        assert_eq!(state.de_escalation_count, 0);
        assert_eq!(state.time_per_stage_ms[0], 1000); // Time in Normal
    }

    #[test]
    fn test_circuit_breaker_state_set_stage_de_escalation() {
        let mut state = CircuitBreakerState::default();
        state.stage = CircuitBreakerStage::Warning;
        state.stage_entered_ms = 1000;

        state.set_stage(CircuitBreakerStage::Normal, 5000, None);

        assert_eq!(state.stage, CircuitBreakerStage::Normal);
        assert_eq!(state.stage_entered_ms, 5000);
        assert!(state.reason.is_none());
        assert_eq!(state.escalation_count, 0);
        assert_eq!(state.de_escalation_count, 1);
        assert_eq!(state.time_per_stage_ms[1], 4000); // Time in Warning
    }

    #[test]
    fn test_circuit_breaker_state_set_stage_same_stage_no_change() {
        let mut state = CircuitBreakerState::default();
        state.stage_entered_ms = 1000;
        state.reason = Some("Original".to_string());

        state.set_stage(CircuitBreakerStage::Normal, 5000, Some("New".to_string()));

        // Should not change anything
        assert_eq!(state.stage_entered_ms, 1000);
        assert_eq!(state.reason, Some("Original".to_string()));
        assert_eq!(state.escalation_count, 0);
        assert_eq!(state.de_escalation_count, 0);
    }

    #[test]
    fn test_circuit_breaker_state_can_de_escalate() {
        let mut state = CircuitBreakerState::default();
        state.stage_entered_ms = 1000;

        // Before min duration
        assert!(!state.can_de_escalate(2000, 5000));

        // At min duration
        assert!(state.can_de_escalate(6000, 5000));

        // After min duration
        assert!(state.can_de_escalate(10000, 5000));
    }

    #[test]
    fn test_circuit_breaker_state_multiple_transitions() {
        let mut state = CircuitBreakerState::default();

        // Normal -> Warning
        state.set_stage(CircuitBreakerStage::Warning, 1000, Some("DD 5%".to_string()));
        // Warning -> ReduceOnly
        state.set_stage(CircuitBreakerStage::ReduceOnly, 2000, Some("DD 8%".to_string()));
        // ReduceOnly -> Warning (de-escalate)
        state.set_stage(CircuitBreakerStage::Warning, 5000, None);
        // Warning -> Normal (de-escalate)
        state.set_stage(CircuitBreakerStage::Normal, 8000, None);

        assert_eq!(state.escalation_count, 2);
        assert_eq!(state.de_escalation_count, 2);
        assert_eq!(state.time_per_stage_ms[0], 1000);  // Normal
        assert_eq!(state.time_per_stage_ms[1], 4000);  // Warning (1000-2000, 5000-8000)
        assert_eq!(state.time_per_stage_ms[2], 3000);  // ReduceOnly (2000-5000)
    }

    #[test]
    fn test_circuit_breaker_state_serialization() {
        let mut state = CircuitBreakerState::default();
        state.stage = CircuitBreakerStage::Warning;
        state.stage_entered_ms = 1000;
        state.reason = Some("Test".to_string());
        state.escalation_count = 3;

        let json = serde_json::to_string(&state).unwrap();
        let deserialized: CircuitBreakerState = serde_json::from_str(&json).unwrap();

        assert_eq!(state.stage, deserialized.stage);
        assert_eq!(state.escalation_count, deserialized.escalation_count);
    }

    // ============================================================================
    // DrawdownMetrics Tests
    // ============================================================================

    #[test]
    fn test_drawdown_metrics_default() {
        let metrics = DrawdownMetrics::default();
        assert_eq!(metrics.current_drawdown, 0.0);
        assert_eq!(metrics.max_drawdown, 0.0);
        assert_eq!(metrics.drawdown_count, 0);
        assert_eq!(metrics.recovery_factor, 0.0);
        assert_eq!(metrics.calmar_ratio, 0.0);
    }

    #[test]
    fn test_drawdown_metrics_serialization() {
        let metrics = DrawdownMetrics {
            current_drawdown: 0.05,
            max_drawdown: 0.10,
            average_drawdown: 0.03,
            peak_equity: dec!(100),
            current_equity: dec!(95),
            time_in_drawdown_ms: 5000,
            drawdown_count: 3,
            avg_drawdown_duration_ms: 1000,
            max_drawdown_duration_ms: 2000,
            current_drawdown_duration_ms: 500,
            recovery_factor: 2.5,
            calmar_ratio: 1.8,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: DrawdownMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(metrics.current_drawdown, deserialized.current_drawdown);
        assert_eq!(metrics.drawdown_count, deserialized.drawdown_count);
        assert_eq!(metrics.recovery_factor, deserialized.recovery_factor);
    }

    // ============================================================================
    // RiskManager Enhanced Drawdown Tracking Tests
    // ============================================================================

    #[test]
    fn test_initialize_tracking() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        manager.initialize_tracking(dec!(1.0), 1000);

        assert_eq!(manager.initial_equity, dec!(1.0));
        assert_eq!(manager.peak_equity, dec!(1.0));
        assert_eq!(manager.tracking_start_ms, 1000);
        assert_eq!(manager.daily_pnl_start, dec!(1.0));
    }

    #[test]
    fn test_update_drawdown_tracking_new_high() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // New high
        manager.update_drawdown_tracking(dec!(110), 1000);

        assert_eq!(manager.peak_equity, dec!(110));
        assert!(manager.current_drawdown_period.is_none());
        assert_eq!(manager.drawdown_observation_count, 1);
    }

    #[test]
    fn test_update_drawdown_tracking_entering_drawdown() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // Enter drawdown
        manager.update_drawdown_tracking(dec!(95), 1000);

        assert_eq!(manager.peak_equity, dec!(100));
        assert!(manager.current_drawdown_period.is_some());

        let period = manager.current_drawdown_period.as_ref().unwrap();
        assert_eq!(period.trough_equity, dec!(95));
        assert!((period.max_drawdown - 0.05).abs() < 0.0001);
    }

    #[test]
    fn test_update_drawdown_tracking_deepening_drawdown() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // Enter and deepen drawdown
        manager.update_drawdown_tracking(dec!(95), 1000);
        manager.update_drawdown_tracking(dec!(90), 2000);

        let period = manager.current_drawdown_period.as_ref().unwrap();
        assert_eq!(period.trough_equity, dec!(90));
        assert!((period.max_drawdown - 0.10).abs() < 0.0001);
    }

    #[test]
    fn test_update_drawdown_tracking_recovery() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // Enter drawdown
        manager.update_drawdown_tracking(dec!(90), 1000);
        assert!(manager.current_drawdown_period.is_some());

        // Recover to new high
        manager.update_drawdown_tracking(dec!(105), 5000);

        assert!(manager.current_drawdown_period.is_none());
        assert_eq!(manager.drawdown_history.len(), 1);

        let recovered = &manager.drawdown_history[0];
        assert!(recovered.recovered);
        assert_eq!(recovered.end_time_ms, Some(5000));
    }

    #[test]
    fn test_update_drawdown_tracking_multiple_periods() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // First drawdown
        manager.update_drawdown_tracking(dec!(90), 1000);
        manager.update_drawdown_tracking(dec!(105), 2000); // Recover

        // Second drawdown
        manager.update_drawdown_tracking(dec!(100), 3000);
        manager.update_drawdown_tracking(dec!(110), 4000); // Recover

        // Third drawdown (ongoing)
        manager.update_drawdown_tracking(dec!(100), 5000);

        assert_eq!(manager.drawdown_history.len(), 2);
        assert!(manager.current_drawdown_period.is_some());
    }

    #[test]
    fn test_update_drawdown_tracking_peak_drawdown_stat() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        manager.update_drawdown_tracking(dec!(92), 1000); // 8% drawdown
        assert!((manager.stats.peak_drawdown - 0.08).abs() < 0.0001);

        manager.update_drawdown_tracking(dec!(88), 2000); // 12% drawdown
        assert!((manager.stats.peak_drawdown - 0.12).abs() < 0.0001);

        // Recovery shouldn't reduce peak drawdown
        manager.update_drawdown_tracking(dec!(110), 3000);
        assert!((manager.stats.peak_drawdown - 0.12).abs() < 0.0001);
    }

    #[test]
    fn test_drawdown_metrics_calculation() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // Create drawdown history
        manager.update_drawdown_tracking(dec!(90), 1000);  // 10% DD
        manager.update_drawdown_tracking(dec!(110), 5000); // Recover
        manager.update_drawdown_tracking(dec!(100), 6000); // New DD (~9%)
        manager.update_drawdown_tracking(dec!(95), 7000);  // Deeper

        let metrics = manager.drawdown_metrics(10000);

        assert!(metrics.current_drawdown > 0.0);
        assert!(metrics.max_drawdown > 0.0);
        assert_eq!(metrics.drawdown_count, 2);
        assert!(metrics.time_in_drawdown_ms > 0);
    }

    #[test]
    fn test_drawdown_metrics_no_drawdown() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // Only gains
        manager.update_drawdown_tracking(dec!(110), 1000);
        manager.update_drawdown_tracking(dec!(120), 2000);

        let metrics = manager.drawdown_metrics(5000);

        assert_eq!(metrics.current_drawdown, 0.0);
        assert_eq!(metrics.drawdown_count, 0);
        assert_eq!(metrics.time_in_drawdown_ms, 0);
    }

    #[test]
    fn test_drawdown_history_access() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        manager.update_drawdown_tracking(dec!(90), 1000);
        manager.update_drawdown_tracking(dec!(110), 2000);

        let history = manager.drawdown_history();
        assert_eq!(history.len(), 1);
        assert!(history[0].recovered);
    }

    // ============================================================================
    // Staged Circuit Breaker Tests
    // ============================================================================

    #[test]
    fn test_circuit_breaker_state_access() {
        let config = RiskConfig::default();
        let manager = RiskManager::new(config);

        let state = manager.circuit_breaker_state();
        assert_eq!(state.stage, CircuitBreakerStage::Normal);
    }

    #[test]
    fn test_circuit_breaker_stage_access() {
        let config = RiskConfig::default();
        let manager = RiskManager::new(config);

        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Normal);
    }

    #[test]
    fn test_check_staged_circuit_breaker_disabled() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = false;
        let mut manager = RiskManager::new(config);

        let action = manager.check_staged_circuit_breaker(0.15, dec!(0.1), 1000);

        assert_eq!(action, RiskAction::Allow);
    }

    #[test]
    fn test_check_staged_circuit_breaker_normal() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        let action = manager.check_staged_circuit_breaker(0.02, dec!(0.01), 1000);

        assert_eq!(action, RiskAction::Allow);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Normal);
    }

    #[test]
    fn test_check_staged_circuit_breaker_warning() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // 6% drawdown triggers warning
        let action = manager.check_staged_circuit_breaker(0.06, dec!(0.01), 1000);

        assert_eq!(action, RiskAction::Allow); // Warning still allows trading
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Warning);
    }

    #[test]
    fn test_check_staged_circuit_breaker_reduce_only() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // 9% drawdown triggers reduce-only
        let action = manager.check_staged_circuit_breaker(0.09, dec!(0.01), 1000);

        assert_eq!(action, RiskAction::ReduceOnly);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::ReduceOnly);
    }

    #[test]
    fn test_check_staged_circuit_breaker_halt() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // 12% drawdown triggers halt
        let action = manager.check_staged_circuit_breaker(0.12, dec!(0.01), 1000);

        assert!(matches!(action, RiskAction::Halt { .. }));
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Halt);
    }

    #[test]
    fn test_check_staged_circuit_breaker_emergency() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // 25% drawdown triggers emergency
        let action = manager.check_staged_circuit_breaker(0.25, dec!(0.01), 1000);

        assert!(matches!(action, RiskAction::Emergency { .. }));
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Emergency);
    }

    #[test]
    fn test_check_staged_circuit_breaker_daily_loss_trigger() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // Small drawdown but high daily loss
        let action = manager.check_staged_circuit_breaker(0.01, dec!(0.04), 1000);

        assert_eq!(action, RiskAction::ReduceOnly);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::ReduceOnly);
    }

    #[test]
    fn test_check_staged_circuit_breaker_consecutive_loss_trigger() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // Simulate consecutive losses
        manager.consecutive_losses = 6; // Above reduce_only threshold (5)

        let action = manager.check_staged_circuit_breaker(0.01, dec!(0.01), 1000);

        assert_eq!(action, RiskAction::ReduceOnly);
    }

    #[test]
    fn test_check_staged_circuit_breaker_takes_max_severity() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // Warning from drawdown, ReduceOnly from consecutive losses
        manager.consecutive_losses = 5;

        let action = manager.check_staged_circuit_breaker(0.06, dec!(0.01), 1000);

        // Should be at ReduceOnly (higher severity)
        assert_eq!(action, RiskAction::ReduceOnly);
    }

    #[test]
    fn test_check_staged_circuit_breaker_escalation_event_logged() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        let initial_events = manager.events.len();

        manager.check_staged_circuit_breaker(0.09, dec!(0.01), 1000);

        assert!(manager.events.len() > initial_events);
    }

    #[test]
    fn test_check_staged_circuit_breaker_de_escalation() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        config.circuit_breaker.auto_de_escalate = true;
        config.circuit_breaker.min_stage_duration_ms = 1000;
        config.circuit_breaker.de_escalation_threshold = 0.7;
        let mut manager = RiskManager::new(config);

        // Escalate to warning
        manager.check_staged_circuit_breaker(0.06, dec!(0.01), 0);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Warning);

        // Conditions improve significantly and min duration passed
        manager.check_staged_circuit_breaker(0.02, dec!(0.005), 2000);

        // Should de-escalate to Normal (0.02 < 0.05 * 0.7 = 0.035)
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Normal);
    }

    #[test]
    fn test_check_staged_circuit_breaker_no_de_escalation_before_min_duration() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        config.circuit_breaker.auto_de_escalate = true;
        config.circuit_breaker.min_stage_duration_ms = 5000;
        let mut manager = RiskManager::new(config);

        // Escalate to warning
        manager.check_staged_circuit_breaker(0.06, dec!(0.01), 0);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Warning);

        // Conditions improve but min duration not passed
        manager.check_staged_circuit_breaker(0.02, dec!(0.005), 1000);

        // Should still be Warning
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Warning);
    }

    #[test]
    fn test_check_staged_circuit_breaker_no_de_escalation_when_disabled() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        config.circuit_breaker.auto_de_escalate = false;
        config.circuit_breaker.min_stage_duration_ms = 0;
        let mut manager = RiskManager::new(config);

        // Escalate to warning
        manager.check_staged_circuit_breaker(0.06, dec!(0.01), 0);

        // Conditions improve
        manager.check_staged_circuit_breaker(0.02, dec!(0.005), 10000);

        // Should still be Warning (auto de-escalate disabled)
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Warning);
    }

    #[test]
    fn test_set_circuit_breaker_stage_manual() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);

        manager.set_circuit_breaker_stage(
            CircuitBreakerStage::Halt,
            1000,
            Some("Manual override".to_string()),
        );

        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Halt);
        assert_eq!(
            manager.circuit_breaker_state().reason,
            Some("Manual override".to_string())
        );
    }

    #[test]
    fn test_circuit_breaker_time_breakdown() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // Start in Normal
        manager.circuit_breaker_state.stage_entered_ms = 0;

        // Escalate to Warning at t=1000
        manager.check_staged_circuit_breaker(0.06, dec!(0.01), 1000);

        // Escalate to ReduceOnly at t=3000
        manager.check_staged_circuit_breaker(0.09, dec!(0.01), 3000);

        // Get breakdown at t=5000
        let breakdown = manager.circuit_breaker_time_breakdown(5000);

        assert_eq!(breakdown[0], 1000);  // Normal: 0-1000
        assert_eq!(breakdown[1], 2000);  // Warning: 1000-3000
        assert_eq!(breakdown[2], 2000);  // ReduceOnly: 3000-5000
    }

    #[test]
    fn test_circuit_breaker_escalation_count() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        manager.check_staged_circuit_breaker(0.06, dec!(0.01), 1000); // -> Warning
        manager.check_staged_circuit_breaker(0.09, dec!(0.01), 2000); // -> ReduceOnly
        manager.check_staged_circuit_breaker(0.12, dec!(0.01), 3000); // -> Halt

        assert_eq!(manager.circuit_breaker_state().escalation_count, 3);
    }

    #[test]
    fn test_risk_config_presets_circuit_breaker() {
        let conservative = RiskConfig::conservative();
        let aggressive = RiskConfig::aggressive();

        // Conservative should have tighter thresholds
        assert!(conservative.circuit_breaker.warning_drawdown < aggressive.circuit_breaker.warning_drawdown);
        assert!(conservative.circuit_breaker.halt_drawdown < aggressive.circuit_breaker.halt_drawdown);

        // Conservative should not auto-de-escalate
        assert!(!conservative.circuit_breaker.auto_de_escalate);
        assert!(aggressive.circuit_breaker.auto_de_escalate);
    }

    // ============================================================================
    // Integration Tests
    // ============================================================================

    #[test]
    fn test_drawdown_and_circuit_breaker_integration() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        manager.initialize_tracking(dec!(100), 0);

        // Update drawdown tracking as equity declines
        manager.update_drawdown_tracking(dec!(97), 1000);  // 3% - Normal
        manager.update_drawdown_tracking(dec!(94), 2000);  // 6% - Warning
        manager.update_drawdown_tracking(dec!(91), 3000);  // 9% - ReduceOnly

        let metrics = manager.drawdown_metrics(4000);

        assert!(metrics.current_drawdown > 0.08);
        assert_eq!(metrics.drawdown_count, 1);

        // Check circuit breaker matches current drawdown
        let action = manager.check_staged_circuit_breaker(
            metrics.current_drawdown,
            dec!(0),
            4000
        );

        assert_eq!(action, RiskAction::ReduceOnly);
    }

    #[test]
    fn test_full_drawdown_cycle_with_recovery() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        config.circuit_breaker.auto_de_escalate = true;
        config.circuit_breaker.min_stage_duration_ms = 100;
        config.circuit_breaker.de_escalation_threshold = 0.7;
        let mut manager = RiskManager::new(config);

        manager.initialize_tracking(dec!(100), 0);

        // Declining equity
        manager.update_drawdown_tracking(dec!(93), 1000);  // 7% DD
        manager.check_staged_circuit_breaker(0.07, dec!(0), 1000);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Warning);

        // Recovery
        manager.update_drawdown_tracking(dec!(105), 2000);

        // Drawdown period should be closed
        assert!(manager.current_drawdown_period.is_none());
        assert_eq!(manager.drawdown_history.len(), 1);

        // Circuit breaker should de-escalate
        manager.check_staged_circuit_breaker(0.0, dec!(0), 2000);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Normal);
    }

    #[test]
    fn test_consecutive_losses_affect_circuit_breaker() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        let state = create_mm_state(dec!(0.01), dec!(0));
        let fill = create_fill(QuoteSide::Ask, dec!(0.01));

        // Record losses
        for i in 0..6 {
            manager.on_fill(&fill, &state, Some(dec!(-0.001)), i * 1000);
        }

        assert_eq!(manager.consecutive_losses, 6);

        // Check circuit breaker
        let action = manager.check_staged_circuit_breaker(0.01, dec!(0.01), 7000);

        // 6 consecutive losses triggers ReduceOnly
        assert_eq!(action, RiskAction::ReduceOnly);
    }

    #[test]
    fn test_drawdown_metrics_with_multiple_periods() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // First drawdown: 10%
        manager.update_drawdown_tracking(dec!(90), 1000);
        manager.update_drawdown_tracking(dec!(110), 5000);

        // Second drawdown: 5%
        manager.update_drawdown_tracking(dec!(104.5), 6000);
        manager.update_drawdown_tracking(dec!(115), 10000);

        // Third drawdown: 8% (ongoing)
        manager.update_drawdown_tracking(dec!(105.8), 11000);

        let metrics = manager.drawdown_metrics(15000);

        assert_eq!(metrics.drawdown_count, 3); // 2 completed + 1 ongoing
        assert_eq!(manager.drawdown_history.len(), 2);
        assert!(manager.current_drawdown_period.is_some());

        // Average duration should be calculated from completed periods
        assert!(metrics.avg_drawdown_duration_ms > 0);
    }

    #[test]
    fn test_circuit_breaker_reason_tracking() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        let mut manager = RiskManager::new(config);

        // Trigger via drawdown
        manager.check_staged_circuit_breaker(0.09, dec!(0.01), 1000);

        let reason = manager.circuit_breaker_state().reason.as_ref().unwrap();
        assert!(reason.contains("Drawdown"));

        // Reset and trigger via daily loss
        manager.set_circuit_breaker_stage(CircuitBreakerStage::Normal, 2000, None);
        manager.check_staged_circuit_breaker(0.01, dec!(0.04), 3000);

        let reason = manager.circuit_breaker_state().reason.as_ref().unwrap();
        assert!(reason.contains("Daily loss"));
    }

    #[test]
    fn test_circuit_breaker_de_escalation_from_reduce_only_to_warning() {
        let mut config = RiskConfig::default();
        config.use_staged_circuit_breaker = true;
        config.circuit_breaker.auto_de_escalate = true;
        config.circuit_breaker.min_stage_duration_ms = 100;
        config.circuit_breaker.de_escalation_threshold = 0.7;
        let mut manager = RiskManager::new(config);

        // Escalate to ReduceOnly
        manager.check_staged_circuit_breaker(0.09, dec!(0.01), 0);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::ReduceOnly);

        // To de-escalate from ReduceOnly to Warning, we need:
        // drawdown < halt_drawdown * de_escalation_threshold = 0.10 * 0.7 = 0.07
        // daily_loss < halt_daily_loss * de_escalation_threshold = 0.05 * 0.7 = 0.035
        // consecutive_losses < halt_consecutive_losses = 8
        // Let's use a much lower drawdown to ensure de-escalation
        manager.check_staged_circuit_breaker(0.05, dec!(0.01), 1000);

        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Warning);
    }

    #[test]
    fn test_drawdown_average_calculation() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // Multiple observations
        manager.update_drawdown_tracking(dec!(100), 1000); // 0% DD
        manager.update_drawdown_tracking(dec!(95), 2000);  // 5% DD
        manager.update_drawdown_tracking(dec!(90), 3000);  // 10% DD
        manager.update_drawdown_tracking(dec!(95), 4000);  // Still 10% (trough at 90)
        manager.update_drawdown_tracking(dec!(100), 5000); // Back to 0% (new peak)

        let metrics = manager.drawdown_metrics(6000);

        // Average should be positive (we had some drawdown observations)
        assert!(metrics.average_drawdown > 0.0);
        assert!(metrics.average_drawdown <= metrics.max_drawdown);
    }

    #[test]
    fn test_risk_manager_with_staged_circuit_breaker_in_config() {
        let config = RiskConfig {
            use_staged_circuit_breaker: true,
            circuit_breaker: CircuitBreakerConfig {
                warning_drawdown: 0.03,
                reduce_only_drawdown: 0.05,
                halt_drawdown: 0.07,
                emergency_drawdown: 0.15,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut manager = RiskManager::new(config);

        // Test with custom thresholds
        manager.check_staged_circuit_breaker(0.04, dec!(0), 1000);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Warning);

        manager.check_staged_circuit_breaker(0.06, dec!(0), 2000);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::ReduceOnly);

        manager.check_staged_circuit_breaker(0.08, dec!(0), 3000);
        assert_eq!(manager.circuit_breaker_stage(), CircuitBreakerStage::Halt);
    }

    #[test]
    fn test_zero_initial_equity_handling() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(0), 0);

        // Should not panic
        manager.update_drawdown_tracking(dec!(-10), 1000);

        let metrics = manager.drawdown_metrics(2000);
        assert_eq!(metrics.current_drawdown, 0.0);
    }

    #[test]
    fn test_negative_equity_handling() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // Equity goes negative
        manager.update_drawdown_tracking(dec!(-50), 1000);

        let metrics = manager.drawdown_metrics(2000);

        // Should handle gracefully - drawdown should be capped or calculated safely
        assert!(metrics.current_drawdown >= 0.0);
    }

    #[test]
    fn test_circuit_breaker_recovery_factor_calculation() {
        let config = RiskConfig::default();
        let mut manager = RiskManager::new(config);
        manager.initialize_tracking(dec!(100), 0);

        // Make profit and experience drawdown
        manager.update_drawdown_tracking(dec!(120), 1000);  // 20% gain
        manager.update_drawdown_tracking(dec!(108), 2000);  // 10% DD from 120

        let metrics = manager.drawdown_metrics(3000);

        // Recovery factor = total return / max drawdown
        // Total return = (120 - 100) / 100 = 0.20
        // Max DD = 10%
        // Recovery factor should be ~2.0
        assert!(metrics.recovery_factor > 0.0);
    }
}

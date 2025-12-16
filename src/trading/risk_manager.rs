//! Risk Management Layer
//!
//! Provides safety controls for market making operations including:
//! - Position limits (max inventory)
//! - Drawdown limits (max loss before kill switch)
//! - Daily loss limits
//! - Quote rate limiting
//! - Position timeout (force close after duration)
//! - Circuit breakers for abnormal conditions
//!
//! # Design Philosophy
//!
//! Risk management operates as a **gate** that can:
//! 1. **Allow** - Normal operation, all quotes permitted
//! 2. **Reduce** - Reduce position only (close trades allowed, new positions blocked)
//! 3. **Halt** - No new quotes, existing positions remain
//! 4. **Emergency** - Full stop, should trigger position liquidation
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
//! ```

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use crate::trading::market_maker::{Fill, QuoteSide, MMState};

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
    use crate::trading::market_maker::PnLTracker;

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
}

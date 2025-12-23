//! TradingAlgorithm Trait Module (Task 3.0)
//!
//! Defines the unified trait for all trading algorithms (MM and Momentum).
//! This trait provides a common interface for:
//! - Receiving features + research assessment
//! - Producing trading decisions
//! - Tracking internal state (position, P&L)
//! - Serializable state for checkpointing
//!
//! # Trait Hierarchy
//!
//! ```text
//! TradingAlgorithm  - Unified trait for all trading algorithms
//!       │
//!       ├── MomentumAlgorithm      - Directional trading (Task 3.1)
//!       ├── MarketMakingAlgorithm  - Quote-based trading (Task 3.2)
//!       └── HybridAlgorithm        - Combines both approaches
//! ```
//!
//! # Key Design Decisions
//!
//! 1. **Strategy Agnostic**: Works with both directional (momentum) and
//!    symmetric (market making) strategies.
//!
//! 2. **Research Integration**: Receives `TradeableAssessment` from
//!    research engine to inform decisions.
//!
//! 3. **Checkpointable**: State can be serialized for persistence and
//!    recovery across restarts.
//!
//! 4. **Config-Driven**: All algorithms are parameterized by `AlgorithmConfig`
//!    generated from research state.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use crate::core::{
    AlgorithmConfig, StrategyType, TradeableAssessment,
};
use crate::features::FeaturesSnapshot;

// ============================================================================
// Trading Decision Types
// ============================================================================

/// Direction of a trading position
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionDirection {
    /// Long position (buy)
    Long,
    /// Short position (sell)
    Short,
    /// No position
    Flat,
}

impl PositionDirection {
    /// Returns the opposite direction
    pub fn opposite(&self) -> Self {
        match self {
            PositionDirection::Long => PositionDirection::Short,
            PositionDirection::Short => PositionDirection::Long,
            PositionDirection::Flat => PositionDirection::Flat,
        }
    }

    /// Returns true if position has a direction
    pub fn is_directional(&self) -> bool {
        !matches!(self, PositionDirection::Flat)
    }

    /// Returns the sign multiplier for P&L calculation
    pub fn sign(&self) -> f64 {
        match self {
            PositionDirection::Long => 1.0,
            PositionDirection::Short => -1.0,
            PositionDirection::Flat => 0.0,
        }
    }
}

impl fmt::Display for PositionDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PositionDirection::Long => write!(f, "Long"),
            PositionDirection::Short => write!(f, "Short"),
            PositionDirection::Flat => write!(f, "Flat"),
        }
    }
}

impl Default for PositionDirection {
    fn default() -> Self {
        PositionDirection::Flat
    }
}

/// Trading action to take
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TradingAction {
    /// Enter a new position
    Enter {
        /// Direction to enter
        direction: PositionDirection,
        /// Suggested size as fraction of max position
        size_fraction: f64,
        /// Reason for entry
        reason: String,
    },
    /// Exit current position
    Exit {
        /// Reason for exit
        reason: String,
    },
    /// Adjust existing position
    Adjust {
        /// New target direction
        target_direction: PositionDirection,
        /// New target size as fraction of max position
        target_size_fraction: f64,
        /// Reason for adjustment
        reason: String,
    },
    /// Place market making quotes
    Quote {
        /// Bid price
        bid_price: Decimal,
        /// Ask price
        ask_price: Decimal,
        /// Bid size
        bid_size: Decimal,
        /// Ask size
        ask_size: Decimal,
    },
    /// Do nothing (wait for better conditions)
    Hold {
        /// Reason for holding
        reason: String,
    },
}

impl TradingAction {
    /// Create an entry action
    pub fn enter(direction: PositionDirection, size_fraction: f64, reason: impl Into<String>) -> Self {
        TradingAction::Enter {
            direction,
            size_fraction,
            reason: reason.into(),
        }
    }

    /// Create an exit action
    pub fn exit(reason: impl Into<String>) -> Self {
        TradingAction::Exit {
            reason: reason.into(),
        }
    }

    /// Create an adjustment action
    pub fn adjust(
        target_direction: PositionDirection,
        target_size_fraction: f64,
        reason: impl Into<String>,
    ) -> Self {
        TradingAction::Adjust {
            target_direction,
            target_size_fraction,
            reason: reason.into(),
        }
    }

    /// Create a quote action
    pub fn quote(
        bid_price: Decimal,
        ask_price: Decimal,
        bid_size: Decimal,
        ask_size: Decimal,
    ) -> Self {
        TradingAction::Quote {
            bid_price,
            ask_price,
            bid_size,
            ask_size,
        }
    }

    /// Create a hold action
    pub fn hold(reason: impl Into<String>) -> Self {
        TradingAction::Hold {
            reason: reason.into(),
        }
    }

    /// Returns true if this is an entry action
    pub fn is_entry(&self) -> bool {
        matches!(self, TradingAction::Enter { .. })
    }

    /// Returns true if this is an exit action
    pub fn is_exit(&self) -> bool {
        matches!(self, TradingAction::Exit { .. })
    }

    /// Returns true if this is a quote action
    pub fn is_quote(&self) -> bool {
        matches!(self, TradingAction::Quote { .. })
    }

    /// Returns true if this is a hold action
    pub fn is_hold(&self) -> bool {
        matches!(self, TradingAction::Hold { .. })
    }

    /// Returns the reason/description for this action
    pub fn reason(&self) -> &str {
        match self {
            TradingAction::Enter { reason, .. } => reason,
            TradingAction::Exit { reason, .. } => reason,
            TradingAction::Adjust { reason, .. } => reason,
            TradingAction::Quote { .. } => "Market making quotes",
            TradingAction::Hold { reason, .. } => reason,
        }
    }
}

impl fmt::Display for TradingAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TradingAction::Enter { direction, size_fraction, reason } => {
                write!(f, "Enter {} (size: {:.1}%): {}", direction, size_fraction * 100.0, reason)
            }
            TradingAction::Exit { reason } => {
                write!(f, "Exit: {}", reason)
            }
            TradingAction::Adjust { target_direction, target_size_fraction, reason } => {
                write!(f, "Adjust to {} (size: {:.1}%): {}", target_direction, target_size_fraction * 100.0, reason)
            }
            TradingAction::Quote { bid_price, ask_price, bid_size, ask_size } => {
                write!(f, "Quote: bid {}@{}, ask {}@{}", bid_size, bid_price, ask_size, ask_price)
            }
            TradingAction::Hold { reason } => {
                write!(f, "Hold: {}", reason)
            }
        }
    }
}

/// Complete trading decision with confidence and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingDecision {
    /// The action to take
    pub action: TradingAction,
    /// Confidence in the decision (0.0 to 1.0)
    pub confidence: f64,
    /// Expected edge/alpha (as fraction, e.g., 0.001 = 0.1%)
    pub expected_edge: f64,
    /// Timestamp of decision
    pub timestamp: DateTime<Utc>,
    /// Take profit target in bps (if applicable)
    pub take_profit_bps: Option<f64>,
    /// Stop loss in bps (if applicable)
    pub stop_loss_bps: Option<f64>,
    /// Maximum hold time in seconds (if applicable)
    pub max_hold_secs: Option<u64>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl TradingDecision {
    /// Create a new trading decision
    pub fn new(action: TradingAction, confidence: f64, expected_edge: f64) -> Self {
        Self {
            action,
            confidence: confidence.clamp(0.0, 1.0),
            expected_edge,
            timestamp: Utc::now(),
            take_profit_bps: None,
            stop_loss_bps: None,
            max_hold_secs: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a hold decision
    pub fn hold(reason: impl Into<String>) -> Self {
        Self::new(TradingAction::hold(reason), 1.0, 0.0)
    }

    /// Set take profit target
    pub fn with_take_profit(mut self, bps: f64) -> Self {
        self.take_profit_bps = Some(bps);
        self
    }

    /// Set stop loss
    pub fn with_stop_loss(mut self, bps: f64) -> Self {
        self.stop_loss_bps = Some(bps);
        self
    }

    /// Set maximum hold time
    pub fn with_max_hold(mut self, secs: u64) -> Self {
        self.max_hold_secs = Some(secs);
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Returns true if decision has high confidence (>= 0.7)
    pub fn is_high_confidence(&self) -> bool {
        self.confidence >= 0.7
    }

    /// Returns true if expected edge is positive
    pub fn has_positive_edge(&self) -> bool {
        self.expected_edge > 0.0
    }

    /// Returns true if decision has OCO brackets set
    pub fn has_oco_brackets(&self) -> bool {
        self.take_profit_bps.is_some() && self.stop_loss_bps.is_some()
    }
}

impl Default for TradingDecision {
    fn default() -> Self {
        Self::hold("No signal")
    }
}

// ============================================================================
// Algorithm State
// ============================================================================

/// Serializable algorithm state for checkpointing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmState {
    /// Unique algorithm instance ID
    pub instance_id: String,
    /// Algorithm config ID
    pub config_id: String,
    /// Strategy type
    pub strategy_type: StrategyType,
    /// Current position direction
    pub position_direction: PositionDirection,
    /// Current position size (absolute value)
    pub position_size: Decimal,
    /// Average entry price (if in position)
    pub entry_price: Option<Decimal>,
    /// Entry timestamp (if in position)
    pub entry_time: Option<DateTime<Utc>>,
    /// Realized P&L (total)
    pub realized_pnl: Decimal,
    /// Unrealized P&L (current position)
    pub unrealized_pnl: Decimal,
    /// Total trade count
    pub trade_count: u64,
    /// Winning trade count
    pub win_count: u64,
    /// Last decision timestamp
    pub last_decision_time: Option<DateTime<Utc>>,
    /// Last decision
    pub last_decision: Option<TradingDecision>,
    /// Current take profit price (if set)
    pub take_profit_price: Option<Decimal>,
    /// Current stop loss price (if set)
    pub stop_loss_price: Option<Decimal>,
    /// Highest price seen since entry (for trailing stops)
    pub high_water_mark: Option<Decimal>,
    /// Lowest price seen since entry (for trailing stops)
    pub low_water_mark: Option<Decimal>,
    /// State creation timestamp
    pub created_at: DateTime<Utc>,
    /// State last update timestamp
    pub updated_at: DateTime<Utc>,
    /// Custom state data (algorithm-specific)
    pub custom_data: HashMap<String, serde_json::Value>,
}

impl AlgorithmState {
    /// Create new algorithm state
    pub fn new(instance_id: impl Into<String>, config_id: impl Into<String>, strategy_type: StrategyType) -> Self {
        let now = Utc::now();
        Self {
            instance_id: instance_id.into(),
            config_id: config_id.into(),
            strategy_type,
            position_direction: PositionDirection::Flat,
            position_size: Decimal::ZERO,
            entry_price: None,
            entry_time: None,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            trade_count: 0,
            win_count: 0,
            last_decision_time: None,
            last_decision: None,
            take_profit_price: None,
            stop_loss_price: None,
            high_water_mark: None,
            low_water_mark: None,
            created_at: now,
            updated_at: now,
            custom_data: HashMap::new(),
        }
    }

    /// Returns true if currently in a position
    pub fn is_in_position(&self) -> bool {
        self.position_direction.is_directional() && self.position_size > Decimal::ZERO
    }

    /// Returns true if position is long
    pub fn is_long(&self) -> bool {
        self.position_direction == PositionDirection::Long && self.is_in_position()
    }

    /// Returns true if position is short
    pub fn is_short(&self) -> bool {
        self.position_direction == PositionDirection::Short && self.is_in_position()
    }

    /// Returns true if flat (no position)
    pub fn is_flat(&self) -> bool {
        !self.is_in_position()
    }

    /// Calculate win rate
    pub fn win_rate(&self) -> f64 {
        if self.trade_count == 0 {
            0.0
        } else {
            self.win_count as f64 / self.trade_count as f64
        }
    }

    /// Calculate total P&L (realized + unrealized)
    pub fn total_pnl(&self) -> Decimal {
        self.realized_pnl + self.unrealized_pnl
    }

    /// Update unrealized P&L based on current price
    pub fn update_unrealized_pnl(&mut self, current_price: Decimal) {
        if let Some(entry_price) = self.entry_price {
            let price_change = current_price - entry_price;
            let direction_sign = match self.position_direction {
                PositionDirection::Long => Decimal::ONE,
                PositionDirection::Short => -Decimal::ONE,
                PositionDirection::Flat => Decimal::ZERO,
            };
            self.unrealized_pnl = price_change * self.position_size * direction_sign;

            // Update high/low water marks
            if self.is_long() {
                match self.high_water_mark {
                    Some(hwm) if current_price > hwm => self.high_water_mark = Some(current_price),
                    None => self.high_water_mark = Some(current_price),
                    _ => {}
                }
            } else if self.is_short() {
                match self.low_water_mark {
                    Some(lwm) if current_price < lwm => self.low_water_mark = Some(current_price),
                    None => self.low_water_mark = Some(current_price),
                    _ => {}
                }
            }
        }
        self.updated_at = Utc::now();
    }

    /// Enter a new position
    pub fn enter_position(
        &mut self,
        direction: PositionDirection,
        size: Decimal,
        price: Decimal,
        take_profit_price: Option<Decimal>,
        stop_loss_price: Option<Decimal>,
    ) {
        self.position_direction = direction;
        self.position_size = size;
        self.entry_price = Some(price);
        self.entry_time = Some(Utc::now());
        self.take_profit_price = take_profit_price;
        self.stop_loss_price = stop_loss_price;
        self.high_water_mark = Some(price);
        self.low_water_mark = Some(price);
        self.unrealized_pnl = Decimal::ZERO;
        self.updated_at = Utc::now();
    }

    /// Exit current position
    pub fn exit_position(&mut self, exit_price: Decimal) {
        // Calculate P&L
        if let Some(entry_price) = self.entry_price {
            let price_change = exit_price - entry_price;
            let direction_sign = match self.position_direction {
                PositionDirection::Long => Decimal::ONE,
                PositionDirection::Short => -Decimal::ONE,
                PositionDirection::Flat => Decimal::ZERO,
            };
            let trade_pnl = price_change * self.position_size * direction_sign;
            self.realized_pnl += trade_pnl;
            self.trade_count += 1;
            if trade_pnl > Decimal::ZERO {
                self.win_count += 1;
            }
        }

        // Reset position state
        self.position_direction = PositionDirection::Flat;
        self.position_size = Decimal::ZERO;
        self.entry_price = None;
        self.entry_time = None;
        self.take_profit_price = None;
        self.stop_loss_price = None;
        self.high_water_mark = None;
        self.low_water_mark = None;
        self.unrealized_pnl = Decimal::ZERO;
        self.updated_at = Utc::now();
    }

    /// Reset state (for new session)
    pub fn reset(&mut self) {
        let instance_id = self.instance_id.clone();
        let config_id = self.config_id.clone();
        let strategy_type = self.strategy_type;
        *self = Self::new(instance_id, config_id, strategy_type);
    }

    /// Set custom data
    pub fn set_custom<T: Serialize>(&mut self, key: impl Into<String>, value: &T) {
        if let Ok(json_value) = serde_json::to_value(value) {
            self.custom_data.insert(key.into(), json_value);
            self.updated_at = Utc::now();
        }
    }

    /// Get custom data
    pub fn get_custom<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Option<T> {
        self.custom_data.get(key).and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

impl Default for AlgorithmState {
    fn default() -> Self {
        Self::new(uuid::Uuid::new_v4().to_string(), "default", StrategyType::Hybrid)
    }
}

// ============================================================================
// Trading Input
// ============================================================================

/// Input data for algorithm decision making
#[derive(Debug, Clone)]
pub struct TradingInput {
    /// Market features snapshot
    pub features: FeaturesSnapshot,
    /// Research assessment (optional, may not be available)
    pub assessment: Option<TradeableAssessment>,
    /// Current timestamp
    pub timestamp: DateTime<Utc>,
}

impl TradingInput {
    /// Create new trading input with features
    pub fn new(features: FeaturesSnapshot) -> Self {
        Self {
            features,
            assessment: None,
            timestamp: Utc::now(),
        }
    }

    /// Create trading input with features and assessment
    pub fn with_assessment(features: FeaturesSnapshot, assessment: TradeableAssessment) -> Self {
        Self {
            features,
            assessment: Some(assessment),
            timestamp: Utc::now(),
        }
    }

    /// Get mid price from features
    pub fn mid_price(&self) -> Option<Decimal> {
        self.features.mid_price
    }

    /// Get best bid from features
    pub fn best_bid(&self) -> Option<Decimal> {
        self.features.best_bid
    }

    /// Get best ask from features
    pub fn best_ask(&self) -> Option<Decimal> {
        self.features.best_ask
    }

    /// Get spread in decimal
    pub fn spread(&self) -> Option<Decimal> {
        self.features.spread
    }

    /// Check if assessment indicates tradeable
    pub fn is_tradeable(&self) -> bool {
        self.assessment.as_ref().map_or(false, |a| a.is_tradeable)
    }

    /// Get recommended position scale from assessment
    pub fn position_scale(&self) -> f64 {
        self.assessment.as_ref().map_or(0.5, |a| a.position_scale)
    }
}

// ============================================================================
// Trading Algorithm Error
// ============================================================================

/// Errors that can occur in trading algorithm operations
#[derive(Debug, Clone)]
pub enum TradingAlgorithmError {
    /// Invalid configuration
    InvalidConfig(String),
    /// Algorithm is in invalid state
    InvalidState(String),
    /// Feature data is missing or invalid
    InvalidInput(String),
    /// Serialization/deserialization error
    SerializationError(String),
    /// IO error
    IoError(String),
}

impl fmt::Display for TradingAlgorithmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TradingAlgorithmError::InvalidConfig(s) => write!(f, "Invalid config: {}", s),
            TradingAlgorithmError::InvalidState(s) => write!(f, "Invalid state: {}", s),
            TradingAlgorithmError::InvalidInput(s) => write!(f, "Invalid input: {}", s),
            TradingAlgorithmError::SerializationError(s) => write!(f, "Serialization error: {}", s),
            TradingAlgorithmError::IoError(s) => write!(f, "IO error: {}", s),
        }
    }
}

impl std::error::Error for TradingAlgorithmError {}

// ============================================================================
// Trading Algorithm Trait
// ============================================================================

/// Unified trait for all trading algorithms (MM and Momentum).
///
/// This trait provides a common interface for:
/// - Receiving features + research assessment
/// - Producing trading decisions
/// - Tracking internal state (position, P&L)
/// - Serializable state for checkpointing
///
/// # Example Implementation
///
/// ```ignore
/// struct MyAlgorithm {
///     config: AlgorithmConfig,
///     state: AlgorithmState,
/// }
///
/// impl TradingAlgorithm for MyAlgorithm {
///     fn strategy_type(&self) -> StrategyType {
///         StrategyType::Momentum
///     }
///
///     fn decide(&mut self, input: &TradingInput) -> TradingDecision {
///         // Analyze features and assessment
///         // Return trading decision
///     }
///
///     fn state(&self) -> &AlgorithmState {
///         &self.state
///     }
///
///     // ... other methods
/// }
/// ```
pub trait TradingAlgorithm: Send + Sync {
    // ========================================================================
    // Identity Methods
    // ========================================================================

    /// Returns the strategy type of this algorithm
    fn strategy_type(&self) -> StrategyType;

    /// Returns a human-readable name for this algorithm
    fn name(&self) -> &str;

    /// Returns algorithm version
    fn version(&self) -> &str {
        "1.0.0"
    }

    /// Returns the config ID this algorithm was created from
    fn config_id(&self) -> &str;

    /// Returns the unique instance ID
    fn instance_id(&self) -> &str;

    // ========================================================================
    // Core Decision Making
    // ========================================================================

    /// Make a trading decision based on current input.
    ///
    /// This is the core method where each algorithm implements its
    /// unique decision-making logic.
    ///
    /// # Arguments
    /// * `input` - Current market features and research assessment
    ///
    /// # Returns
    /// A `TradingDecision` containing the action to take
    fn decide(&mut self, input: &TradingInput) -> TradingDecision;

    /// Process a fill/execution notification.
    ///
    /// Called when an order is filled to update internal state.
    ///
    /// # Arguments
    /// * `price` - Fill price
    /// * `size` - Fill size
    /// * `direction` - Fill direction
    /// * `fee` - Fee paid
    fn on_fill(
        &mut self,
        price: Decimal,
        size: Decimal,
        direction: PositionDirection,
        fee: Decimal,
    );

    /// Process price update for mark-to-market.
    ///
    /// Called on each price update to track unrealized P&L.
    fn on_price_update(&mut self, price: Decimal);

    // ========================================================================
    // State Management
    // ========================================================================

    /// Get current algorithm state (immutable reference)
    fn state(&self) -> &AlgorithmState;

    /// Get current algorithm state (mutable reference)
    fn state_mut(&mut self) -> &mut AlgorithmState;

    /// Reset algorithm to initial state
    fn reset(&mut self);

    /// Check if algorithm should stop (circuit breaker, max drawdown, etc.)
    fn should_stop(&self) -> bool {
        false
    }

    /// Get stop reason if `should_stop()` returns true
    fn stop_reason(&self) -> Option<String> {
        None
    }

    // ========================================================================
    // Checkpointing
    // ========================================================================

    /// Serialize state for checkpointing
    fn checkpoint(&self) -> Result<Vec<u8>, TradingAlgorithmError> {
        serde_json::to_vec(self.state())
            .map_err(|e| TradingAlgorithmError::SerializationError(e.to_string()))
    }

    /// Restore state from checkpoint
    fn restore(&mut self, data: &[u8]) -> Result<(), TradingAlgorithmError> {
        let state: AlgorithmState = serde_json::from_slice(data)
            .map_err(|e| TradingAlgorithmError::SerializationError(e.to_string()))?;
        *self.state_mut() = state;
        Ok(())
    }

    // ========================================================================
    // Configuration
    // ========================================================================

    /// Get the algorithm configuration
    fn config(&self) -> &AlgorithmConfig;

    /// Update configuration (for hot-reloading)
    fn update_config(&mut self, config: AlgorithmConfig) -> Result<(), TradingAlgorithmError>;

    // ========================================================================
    // Convenience Methods (Default Implementations)
    // ========================================================================

    /// Check if currently in a position
    fn is_in_position(&self) -> bool {
        self.state().is_in_position()
    }

    /// Get current position direction
    fn position_direction(&self) -> PositionDirection {
        self.state().position_direction
    }

    /// Get current position size
    fn position_size(&self) -> Decimal {
        self.state().position_size
    }

    /// Get total P&L
    fn total_pnl(&self) -> Decimal {
        self.state().total_pnl()
    }

    /// Get realized P&L
    fn realized_pnl(&self) -> Decimal {
        self.state().realized_pnl
    }

    /// Get unrealized P&L
    fn unrealized_pnl(&self) -> Decimal {
        self.state().unrealized_pnl
    }

    /// Get trade count
    fn trade_count(&self) -> u64 {
        self.state().trade_count
    }

    /// Get win rate
    fn win_rate(&self) -> f64 {
        self.state().win_rate()
    }

    /// Returns a JSON-serializable summary of current state
    fn state_json(&self) -> serde_json::Value {
        serde_json::json!({
            "instance_id": self.instance_id(),
            "config_id": self.config_id(),
            "strategy_type": format!("{}", self.strategy_type()),
            "position_direction": format!("{}", self.position_direction()),
            "position_size": self.position_size().to_string(),
            "realized_pnl": self.realized_pnl().to_string(),
            "unrealized_pnl": self.unrealized_pnl().to_string(),
            "total_pnl": self.total_pnl().to_string(),
            "trade_count": self.trade_count(),
            "win_rate": self.win_rate(),
        })
    }
}

// ============================================================================
// Factory Trait
// ============================================================================

/// Factory trait for creating trading algorithms from config
pub trait TradingAlgorithmFactory: Send + Sync {
    /// Create a new algorithm instance from configuration
    fn create(&self, config: &AlgorithmConfig) -> Result<Box<dyn TradingAlgorithm>, TradingAlgorithmError>;

    /// Returns the strategy type this factory creates
    fn strategy_type(&self) -> StrategyType;

    /// Returns a human-readable name for algorithms created by this factory
    fn algorithm_name(&self) -> &str;
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // ========================================================================
    // PositionDirection Tests
    // ========================================================================

    #[test]
    fn test_position_direction_opposite() {
        assert_eq!(PositionDirection::Long.opposite(), PositionDirection::Short);
        assert_eq!(PositionDirection::Short.opposite(), PositionDirection::Long);
        assert_eq!(PositionDirection::Flat.opposite(), PositionDirection::Flat);
    }

    #[test]
    fn test_position_direction_is_directional() {
        assert!(PositionDirection::Long.is_directional());
        assert!(PositionDirection::Short.is_directional());
        assert!(!PositionDirection::Flat.is_directional());
    }

    #[test]
    fn test_position_direction_sign() {
        assert_eq!(PositionDirection::Long.sign(), 1.0);
        assert_eq!(PositionDirection::Short.sign(), -1.0);
        assert_eq!(PositionDirection::Flat.sign(), 0.0);
    }

    #[test]
    fn test_position_direction_display() {
        assert_eq!(format!("{}", PositionDirection::Long), "Long");
        assert_eq!(format!("{}", PositionDirection::Short), "Short");
        assert_eq!(format!("{}", PositionDirection::Flat), "Flat");
    }

    #[test]
    fn test_position_direction_default() {
        assert_eq!(PositionDirection::default(), PositionDirection::Flat);
    }

    #[test]
    fn test_position_direction_serde() {
        let dir = PositionDirection::Long;
        let json = serde_json::to_string(&dir).unwrap();
        let parsed: PositionDirection = serde_json::from_str(&json).unwrap();
        assert_eq!(dir, parsed);
    }

    // ========================================================================
    // TradingAction Tests
    // ========================================================================

    #[test]
    fn test_trading_action_enter() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Signal detected");
        assert!(action.is_entry());
        assert!(!action.is_exit());
        assert!(!action.is_quote());
        assert!(!action.is_hold());
        assert_eq!(action.reason(), "Signal detected");
    }

    #[test]
    fn test_trading_action_exit() {
        let action = TradingAction::exit("Take profit hit");
        assert!(!action.is_entry());
        assert!(action.is_exit());
        assert_eq!(action.reason(), "Take profit hit");
    }

    #[test]
    fn test_trading_action_quote() {
        let action = TradingAction::quote(dec!(100), dec!(101), dec!(1), dec!(1));
        assert!(action.is_quote());
        assert!(!action.is_entry());
    }

    #[test]
    fn test_trading_action_hold() {
        let action = TradingAction::hold("Low confidence");
        assert!(action.is_hold());
        assert_eq!(action.reason(), "Low confidence");
    }

    #[test]
    fn test_trading_action_adjust() {
        let action = TradingAction::adjust(PositionDirection::Long, 0.75, "Increasing position");
        match &action {
            TradingAction::Adjust { target_direction, target_size_fraction, reason } => {
                assert_eq!(*target_direction, PositionDirection::Long);
                assert!((target_size_fraction - 0.75).abs() < 0.001);
                assert_eq!(reason, "Increasing position");
            }
            _ => panic!("Expected Adjust action"),
        }
    }

    #[test]
    fn test_trading_action_display() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Test");
        let display = format!("{}", action);
        assert!(display.contains("Long"));
        assert!(display.contains("50.0%"));
        assert!(display.contains("Test"));
    }

    #[test]
    fn test_trading_action_serde() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Test");
        let json = serde_json::to_string(&action).unwrap();
        let parsed: TradingAction = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_entry());
    }

    // ========================================================================
    // TradingDecision Tests
    // ========================================================================

    #[test]
    fn test_trading_decision_new() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Signal");
        let decision = TradingDecision::new(action, 0.8, 0.001);

        assert!(decision.is_high_confidence());
        assert!(decision.has_positive_edge());
        assert!(!decision.has_oco_brackets());
    }

    #[test]
    fn test_trading_decision_hold() {
        let decision = TradingDecision::hold("No signal");
        assert!(decision.action.is_hold());
        assert_eq!(decision.confidence, 1.0);
        assert_eq!(decision.expected_edge, 0.0);
    }

    #[test]
    fn test_trading_decision_with_oco() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Signal");
        let decision = TradingDecision::new(action, 0.8, 0.001)
            .with_take_profit(20.0)
            .with_stop_loss(10.0);

        assert!(decision.has_oco_brackets());
        assert_eq!(decision.take_profit_bps, Some(20.0));
        assert_eq!(decision.stop_loss_bps, Some(10.0));
    }

    #[test]
    fn test_trading_decision_with_max_hold() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Signal");
        let decision = TradingDecision::new(action, 0.8, 0.001)
            .with_max_hold(300);

        assert_eq!(decision.max_hold_secs, Some(300));
    }

    #[test]
    fn test_trading_decision_with_metadata() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Signal");
        let decision = TradingDecision::new(action, 0.8, 0.001)
            .with_metadata("signal_strength", "0.85");

        assert_eq!(decision.metadata.get("signal_strength"), Some(&"0.85".to_string()));
    }

    #[test]
    fn test_trading_decision_confidence_clamped() {
        let action = TradingAction::hold("Test");
        let decision1 = TradingDecision::new(action.clone(), 1.5, 0.0);
        assert_eq!(decision1.confidence, 1.0);

        let decision2 = TradingDecision::new(action, -0.5, 0.0);
        assert_eq!(decision2.confidence, 0.0);
    }

    #[test]
    fn test_trading_decision_low_confidence() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Signal");
        let decision = TradingDecision::new(action, 0.5, 0.001);
        assert!(!decision.is_high_confidence());
    }

    #[test]
    fn test_trading_decision_negative_edge() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Signal");
        let decision = TradingDecision::new(action, 0.8, -0.001);
        assert!(!decision.has_positive_edge());
    }

    #[test]
    fn test_trading_decision_serde() {
        let action = TradingAction::enter(PositionDirection::Long, 0.5, "Signal");
        let decision = TradingDecision::new(action, 0.8, 0.001)
            .with_take_profit(20.0)
            .with_stop_loss(10.0);

        let json = serde_json::to_string(&decision).unwrap();
        let parsed: TradingDecision = serde_json::from_str(&json).unwrap();

        assert!(parsed.action.is_entry());
        assert_eq!(parsed.confidence, 0.8);
        assert!(parsed.has_oco_brackets());
    }

    #[test]
    fn test_trading_decision_default() {
        let decision = TradingDecision::default();
        assert!(decision.action.is_hold());
    }

    // ========================================================================
    // AlgorithmState Tests
    // ========================================================================

    #[test]
    fn test_algorithm_state_new() {
        let state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        assert_eq!(state.instance_id, "inst-1");
        assert_eq!(state.config_id, "config-1");
        assert_eq!(state.strategy_type, StrategyType::Momentum);
        assert!(state.is_flat());
        assert!(!state.is_in_position());
    }

    #[test]
    fn test_algorithm_state_enter_position() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        state.enter_position(
            PositionDirection::Long,
            dec!(100),
            dec!(50000),
            Some(dec!(50100)),
            Some(dec!(49900)),
        );

        assert!(state.is_in_position());
        assert!(state.is_long());
        assert!(!state.is_short());
        assert!(!state.is_flat());
        assert_eq!(state.position_size, dec!(100));
        assert_eq!(state.entry_price, Some(dec!(50000)));
        assert_eq!(state.take_profit_price, Some(dec!(50100)));
        assert_eq!(state.stop_loss_price, Some(dec!(49900)));
    }

    #[test]
    fn test_algorithm_state_exit_position_profit() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        // Enter long at 50000
        state.enter_position(
            PositionDirection::Long,
            dec!(100),
            dec!(50000),
            None,
            None,
        );

        // Exit at 50100 (profit)
        state.exit_position(dec!(50100));

        assert!(state.is_flat());
        assert_eq!(state.trade_count, 1);
        assert_eq!(state.win_count, 1);
        assert_eq!(state.win_rate(), 1.0);
        // P&L = (50100 - 50000) * 100 = 10000
        assert_eq!(state.realized_pnl, dec!(10000));
    }

    #[test]
    fn test_algorithm_state_exit_position_loss() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        // Enter long at 50000
        state.enter_position(
            PositionDirection::Long,
            dec!(100),
            dec!(50000),
            None,
            None,
        );

        // Exit at 49900 (loss)
        state.exit_position(dec!(49900));

        assert_eq!(state.trade_count, 1);
        assert_eq!(state.win_count, 0);
        assert_eq!(state.win_rate(), 0.0);
        // P&L = (49900 - 50000) * 100 = -10000
        assert_eq!(state.realized_pnl, dec!(-10000));
    }

    #[test]
    fn test_algorithm_state_short_position() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        // Enter short at 50000
        state.enter_position(
            PositionDirection::Short,
            dec!(100),
            dec!(50000),
            None,
            None,
        );

        assert!(state.is_short());

        // Exit at 49900 (profit for short)
        state.exit_position(dec!(49900));

        assert_eq!(state.win_count, 1);
        // P&L = -(49900 - 50000) * 100 = 10000
        assert_eq!(state.realized_pnl, dec!(10000));
    }

    #[test]
    fn test_algorithm_state_update_unrealized_pnl_long() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        state.enter_position(
            PositionDirection::Long,
            dec!(100),
            dec!(50000),
            None,
            None,
        );

        // Price moves up
        state.update_unrealized_pnl(dec!(50100));
        assert_eq!(state.unrealized_pnl, dec!(10000));
        assert_eq!(state.high_water_mark, Some(dec!(50100)));

        // Price moves down
        state.update_unrealized_pnl(dec!(49900));
        assert_eq!(state.unrealized_pnl, dec!(-10000));
        // High water mark should not decrease
        assert_eq!(state.high_water_mark, Some(dec!(50100)));
    }

    #[test]
    fn test_algorithm_state_update_unrealized_pnl_short() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        state.enter_position(
            PositionDirection::Short,
            dec!(100),
            dec!(50000),
            None,
            None,
        );

        // Price moves down (profit for short)
        state.update_unrealized_pnl(dec!(49900));
        assert_eq!(state.unrealized_pnl, dec!(10000));
        assert_eq!(state.low_water_mark, Some(dec!(49900)));
    }

    #[test]
    fn test_algorithm_state_total_pnl() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        // Complete a winning trade
        state.enter_position(PositionDirection::Long, dec!(100), dec!(50000), None, None);
        state.exit_position(dec!(50100));

        // Enter a new position
        state.enter_position(PositionDirection::Long, dec!(100), dec!(50100), None, None);
        state.update_unrealized_pnl(dec!(50200));

        // Realized = 10000, Unrealized = 10000
        assert_eq!(state.total_pnl(), dec!(20000));
    }

    #[test]
    fn test_algorithm_state_reset() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        state.enter_position(PositionDirection::Long, dec!(100), dec!(50000), None, None);
        state.exit_position(dec!(50100));

        state.reset();

        assert!(state.is_flat());
        assert_eq!(state.realized_pnl, Decimal::ZERO);
        assert_eq!(state.trade_count, 0);
        assert_eq!(state.instance_id, "inst-1");
        assert_eq!(state.config_id, "config-1");
    }

    #[test]
    fn test_algorithm_state_custom_data() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        state.set_custom("signal_strength", &0.85f64);
        state.set_custom("regime", &"trending".to_string());

        let strength: f64 = state.get_custom("signal_strength").unwrap();
        let regime: String = state.get_custom("regime").unwrap();

        assert!((strength - 0.85).abs() < 0.001);
        assert_eq!(regime, "trending");
    }

    #[test]
    fn test_algorithm_state_custom_data_missing() {
        let state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);
        let result: Option<f64> = state.get_custom("nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_algorithm_state_serde() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);
        state.enter_position(PositionDirection::Long, dec!(100), dec!(50000), None, None);
        state.set_custom("test_key", &"test_value".to_string());

        let json = serde_json::to_string(&state).unwrap();
        let parsed: AlgorithmState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.instance_id, "inst-1");
        assert_eq!(parsed.position_direction, PositionDirection::Long);
        assert_eq!(parsed.position_size, dec!(100));
        let test_val: String = parsed.get_custom("test_key").unwrap();
        assert_eq!(test_val, "test_value");
    }

    #[test]
    fn test_algorithm_state_default() {
        let state = AlgorithmState::default();
        assert!(state.is_flat());
        assert_eq!(state.strategy_type, StrategyType::Hybrid);
    }

    #[test]
    fn test_algorithm_state_multiple_trades() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        // Trade 1: Win
        state.enter_position(PositionDirection::Long, dec!(100), dec!(50000), None, None);
        state.exit_position(dec!(50100));

        // Trade 2: Loss
        state.enter_position(PositionDirection::Long, dec!(100), dec!(50100), None, None);
        state.exit_position(dec!(50000));

        // Trade 3: Win
        state.enter_position(PositionDirection::Short, dec!(100), dec!(50000), None, None);
        state.exit_position(dec!(49900));

        assert_eq!(state.trade_count, 3);
        assert_eq!(state.win_count, 2);
        assert!((state.win_rate() - 0.666666).abs() < 0.001);
        // 10000 - 10000 + 10000 = 10000
        assert_eq!(state.realized_pnl, dec!(10000));
    }

    // ========================================================================
    // TradingAlgorithmError Tests
    // ========================================================================

    #[test]
    fn test_trading_algorithm_error_display() {
        let err = TradingAlgorithmError::InvalidConfig("bad param".to_string());
        assert_eq!(format!("{}", err), "Invalid config: bad param");

        let err = TradingAlgorithmError::InvalidState("no position".to_string());
        assert_eq!(format!("{}", err), "Invalid state: no position");

        let err = TradingAlgorithmError::InvalidInput("missing price".to_string());
        assert_eq!(format!("{}", err), "Invalid input: missing price");

        let err = TradingAlgorithmError::SerializationError("json error".to_string());
        assert_eq!(format!("{}", err), "Serialization error: json error");

        let err = TradingAlgorithmError::IoError("file not found".to_string());
        assert_eq!(format!("{}", err), "IO error: file not found");
    }

    // ========================================================================
    // TradingInput Tests
    // ========================================================================

    #[test]
    fn test_trading_input_new() {
        let features = FeaturesSnapshot::default();
        let input = TradingInput::new(features);

        assert!(input.assessment.is_none());
        assert!(!input.is_tradeable());
        assert_eq!(input.position_scale(), 0.5); // Default
    }

    #[test]
    fn test_trading_input_with_assessment() {
        let features = FeaturesSnapshot::default();
        let assessment = TradeableAssessment {
            is_tradeable: true,
            position_scale: 0.8,
            ..Default::default()
        };

        let input = TradingInput::with_assessment(features, assessment);

        assert!(input.is_tradeable());
        assert_eq!(input.position_scale(), 0.8);
    }

    #[test]
    fn test_trading_input_price_accessors() {
        let mut features = FeaturesSnapshot::default();
        features.mid_price = Some(dec!(50000));
        features.best_bid = Some(dec!(49990));
        features.best_ask = Some(dec!(50010));
        features.spread = Some(dec!(20));

        let input = TradingInput::new(features);

        assert_eq!(input.mid_price(), Some(dec!(50000)));
        assert_eq!(input.best_bid(), Some(dec!(49990)));
        assert_eq!(input.best_ask(), Some(dec!(50010)));
        assert_eq!(input.spread(), Some(dec!(20)));
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_algorithm_state_zero_size_position() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        state.enter_position(
            PositionDirection::Long,
            Decimal::ZERO,
            dec!(50000),
            None,
            None,
        );

        // Zero size means not really in position
        assert!(!state.is_in_position());
    }

    #[test]
    fn test_algorithm_state_flat_with_size() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        // Manually set inconsistent state (shouldn't happen in practice)
        state.position_direction = PositionDirection::Flat;
        state.position_size = dec!(100);

        // Should be treated as not in position because direction is Flat
        assert!(!state.is_in_position());
    }

    #[test]
    fn test_trading_decision_boundary_confidence() {
        let action = TradingAction::hold("Test");

        // Exactly at boundary
        let decision = TradingDecision::new(action.clone(), 0.7, 0.0);
        assert!(decision.is_high_confidence());

        // Just below boundary
        let decision = TradingDecision::new(action, 0.699, 0.0);
        assert!(!decision.is_high_confidence());
    }

    #[test]
    fn test_algorithm_state_win_rate_no_trades() {
        let state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);
        assert_eq!(state.win_rate(), 0.0);
    }

    #[test]
    fn test_exit_without_entry() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        // Exit without entry - should handle gracefully
        state.exit_position(dec!(50000));

        // Should remain unchanged
        assert_eq!(state.trade_count, 0);
        assert!(state.is_flat());
    }

    #[test]
    fn test_algorithm_state_high_water_mark_updates() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        state.enter_position(PositionDirection::Long, dec!(100), dec!(50000), None, None);

        // Initial high water mark
        assert_eq!(state.high_water_mark, Some(dec!(50000)));

        // Price goes up - should update
        state.update_unrealized_pnl(dec!(50100));
        assert_eq!(state.high_water_mark, Some(dec!(50100)));

        // Price goes up more - should update
        state.update_unrealized_pnl(dec!(50200));
        assert_eq!(state.high_water_mark, Some(dec!(50200)));

        // Price goes down - should NOT update (high water mark)
        state.update_unrealized_pnl(dec!(50150));
        assert_eq!(state.high_water_mark, Some(dec!(50200)));
    }

    #[test]
    fn test_algorithm_state_low_water_mark_updates() {
        let mut state = AlgorithmState::new("inst-1", "config-1", StrategyType::Momentum);

        state.enter_position(PositionDirection::Short, dec!(100), dec!(50000), None, None);

        // Initial low water mark
        assert_eq!(state.low_water_mark, Some(dec!(50000)));

        // Price goes down - should update
        state.update_unrealized_pnl(dec!(49900));
        assert_eq!(state.low_water_mark, Some(dec!(49900)));

        // Price goes up - should NOT update (low water mark)
        state.update_unrealized_pnl(dec!(49950));
        assert_eq!(state.low_water_mark, Some(dec!(49900)));
    }
}

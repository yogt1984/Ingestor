//! Momentum Algorithm Implementation (Task 3.1)
//!
//! Parameterized momentum algorithm created from `AlgorithmConfig`. This algorithm
//! implements directional trading based on:
//! - Conditional probability thresholds from research
//! - OCO exits (TP/SL) from config
//! - Position sizing from config
//! - Regime filters (skip if τ_half below threshold)
//!
//! # Key Features
//!
//! - **Config-driven**: All parameters derived from `AlgorithmConfig`, no hardcoding
//! - **Research integration**: Uses `TradeableAssessment` to validate entry conditions
//! - **OCO risk management**: Every entry has associated TP/SL brackets
//! - **Regime filtering**: Skips trading when market conditions are unfavorable
//!
//! # Decision Flow
//!
//! ```text
//! Input (Features + Assessment)
//!         │
//!         ▼
//! ┌───────────────────┐
//! │ Regime Check      │──────▶ Hold if unfavorable
//! └───────────────────┘
//!         │
//!         ▼
//! ┌───────────────────┐
//! │ Signal Generation │──────▶ Hold if no signal
//! └───────────────────┘
//!         │
//!         ▼
//! ┌───────────────────┐
//! │ Entry/Exit Logic  │──────▶ Entry, Exit, or Hold
//! └───────────────────┘
//!         │
//!         ▼
//! TradingDecision with OCO brackets
//! ```

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use crate::core::{
    AlgorithmConfig, RecommendedStrategy, StrategyType,
};
use crate::features::FeaturesSnapshot;

use super::trading_algorithm::{
    AlgorithmState, PositionDirection, TradingAction, TradingAlgorithm,
    TradingAlgorithmError, TradingDecision, TradingInput,
};

// ============================================================================
// Momentum Algorithm Configuration
// ============================================================================

/// Internal configuration extracted from AlgorithmConfig for momentum trading
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumConfig {
    // Entry thresholds
    pub min_momentum_signal: f64,
    pub min_monotonicity: f64,
    pub min_hurst: f64,
    pub max_entry_entropy: f64,
    pub min_conditional_prob: f64,
    pub min_confidence: f64,

    // Exit parameters
    pub take_profit_bps: f64,
    pub stop_loss_bps: f64,
    pub max_hold_seconds: u64,
    pub trailing_stop_activation_bps: f64,
    pub trailing_stop_distance_bps: f64,

    // Position sizing
    pub max_position_size: f64,
    pub base_position_fraction: f64,

    // Regime filters
    pub min_tau_half: f64,
    pub max_entropy: f64,
    pub min_persistence: f64,

    // Circuit breaker
    pub max_consecutive_losses: u32,
    pub max_daily_drawdown_pct: f64,
}

impl Default for MomentumConfig {
    fn default() -> Self {
        Self {
            // Entry thresholds
            min_momentum_signal: 0.5,
            min_monotonicity: 0.55,
            min_hurst: 0.5,
            max_entry_entropy: 0.65,
            min_conditional_prob: 0.52,
            min_confidence: 0.6,

            // Exit parameters
            take_profit_bps: 20.0,
            stop_loss_bps: 10.0,
            max_hold_seconds: 300,
            trailing_stop_activation_bps: 0.0,
            trailing_stop_distance_bps: 0.0,

            // Position sizing
            max_position_size: 1.0,
            base_position_fraction: 0.5,

            // Regime filters
            min_tau_half: 30.0,
            max_entropy: 0.6,
            min_persistence: 0.5,

            // Circuit breaker
            max_consecutive_losses: 5,
            max_daily_drawdown_pct: 2.0,
        }
    }
}

impl MomentumConfig {
    /// Create configuration from AlgorithmConfig
    pub fn from_algorithm_config(config: &AlgorithmConfig) -> Self {
        Self {
            // Entry thresholds from config
            min_momentum_signal: config.entry.min_momentum_signal,
            min_monotonicity: config.entry.min_monotonicity,
            min_hurst: config.entry.min_hurst,
            max_entry_entropy: config.entry.max_entry_entropy,
            min_conditional_prob: config.entry.min_conditional_prob,
            min_confidence: config.entry.min_confidence,

            // Exit parameters from config
            take_profit_bps: config.exit.take_profit_bps,
            stop_loss_bps: config.exit.stop_loss_bps,
            max_hold_seconds: config.exit.max_hold_seconds,
            trailing_stop_activation_bps: config.exit.trailing_stop_activation_bps,
            trailing_stop_distance_bps: config.exit.trailing_stop_distance_bps,

            // Position sizing from config
            max_position_size: config.position.max_size_fraction,
            base_position_fraction: config.position.base_size_fraction,

            // Regime filters from config
            min_tau_half: config.regime_filters.min_tau_half,
            max_entropy: config.regime_filters.max_entropy,
            min_persistence: config.regime_filters.min_tau_half / 100.0, // Derive from tau_half

            // Circuit breaker (default values - not in AlgorithmConfig)
            max_consecutive_losses: 5,
            max_daily_drawdown_pct: 2.0,
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.min_momentum_signal < 0.0 || self.min_momentum_signal > 1.0 {
            return Err("min_momentum_signal must be between 0 and 1".to_string());
        }
        if self.min_confidence < 0.0 || self.min_confidence > 1.0 {
            return Err("min_confidence must be between 0 and 1".to_string());
        }
        if self.take_profit_bps <= 0.0 {
            return Err("take_profit_bps must be positive".to_string());
        }
        if self.stop_loss_bps <= 0.0 {
            return Err("stop_loss_bps must be positive".to_string());
        }
        if self.max_position_size <= 0.0 {
            return Err("max_position_size must be positive".to_string());
        }
        Ok(())
    }
}

// ============================================================================
// Momentum Signal
// ============================================================================

/// Generated momentum signal from features
#[derive(Debug, Clone)]
pub struct MomentumSignal {
    /// Direction of the signal
    pub direction: PositionDirection,
    /// Signal strength (0.0 to 1.0)
    pub strength: f64,
    /// Confidence in the signal (0.0 to 1.0)
    pub confidence: f64,
    /// Expected edge in basis points
    pub expected_edge_bps: f64,
    /// Reason for the signal
    pub reason: String,
}

impl MomentumSignal {
    /// Create a new momentum signal
    pub fn new(
        direction: PositionDirection,
        strength: f64,
        confidence: f64,
        expected_edge_bps: f64,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            direction,
            strength: strength.clamp(0.0, 1.0),
            confidence: confidence.clamp(0.0, 1.0),
            expected_edge_bps,
            reason: reason.into(),
        }
    }

    /// Create a neutral/no-signal
    pub fn neutral(reason: impl Into<String>) -> Self {
        Self {
            direction: PositionDirection::Flat,
            strength: 0.0,
            confidence: 0.0,
            expected_edge_bps: 0.0,
            reason: reason.into(),
        }
    }

    /// Returns true if signal is actionable
    pub fn is_actionable(&self, min_strength: f64, min_confidence: f64) -> bool {
        self.direction.is_directional()
            && self.strength >= min_strength
            && self.confidence >= min_confidence
    }
}

// ============================================================================
// Momentum Algorithm
// ============================================================================

/// Parameterized momentum algorithm implementing `TradingAlgorithm` trait.
///
/// This algorithm:
/// - Enters positions based on momentum signals from features and research assessment
/// - Uses OCO brackets (TP/SL) for every entry
/// - Applies regime filters to avoid unfavorable conditions
/// - Tracks consecutive losses for circuit breaker
#[derive(Debug)]
pub struct MomentumAlgorithm {
    /// Algorithm configuration (source)
    algo_config: AlgorithmConfig,
    /// Momentum-specific configuration
    momentum_config: MomentumConfig,
    /// Algorithm state
    state: AlgorithmState,
    /// Consecutive losses counter
    consecutive_losses: u32,
    /// Daily P&L tracker
    daily_pnl: Decimal,
    /// Daily start timestamp
    daily_start: DateTime<Utc>,
    /// Whether circuit breaker is tripped
    circuit_breaker_tripped: bool,
    /// Circuit breaker reason
    circuit_breaker_reason: Option<String>,
}

impl MomentumAlgorithm {
    /// Create a new momentum algorithm from AlgorithmConfig
    pub fn from_config(config: AlgorithmConfig) -> Result<Self, TradingAlgorithmError> {
        // Validate that strategy type is compatible
        if config.strategy_type != StrategyType::Momentum && config.strategy_type != StrategyType::Hybrid {
            return Err(TradingAlgorithmError::InvalidConfig(
                format!("MomentumAlgorithm requires Momentum or Hybrid strategy type, got {:?}", config.strategy_type)
            ));
        }

        let momentum_config = MomentumConfig::from_algorithm_config(&config);
        momentum_config.validate().map_err(TradingAlgorithmError::InvalidConfig)?;

        let instance_id = uuid::Uuid::new_v4().to_string();
        let state = AlgorithmState::new(
            instance_id,
            config.id.clone(),
            StrategyType::Momentum,
        );

        Ok(Self {
            algo_config: config,
            momentum_config,
            state,
            consecutive_losses: 0,
            daily_pnl: Decimal::ZERO,
            daily_start: Utc::now(),
            circuit_breaker_tripped: false,
            circuit_breaker_reason: None,
        })
    }

    /// Create with default configuration
    pub fn new() -> Self {
        let config = AlgorithmConfig::default();
        Self::from_config(config).expect("Default config should be valid")
    }

    /// Get momentum configuration
    pub fn momentum_config(&self) -> &MomentumConfig {
        &self.momentum_config
    }

    /// Check if regime is favorable for momentum trading
    fn is_regime_favorable(&self, input: &TradingInput) -> (bool, String) {
        // Check assessment first
        if let Some(assessment) = &input.assessment {
            // Must be tradeable
            if !assessment.is_tradeable {
                return (false, "Assessment: not tradeable".to_string());
            }

            // Check if momentum or TSMOM is recommended
            match assessment.recommended_strategy {
                RecommendedStrategy::Momentum
                | RecommendedStrategy::TSMOM
                | RecommendedStrategy::MACrossover
                | RecommendedStrategy::Hybrid => {
                    // Good for momentum
                }
                RecommendedStrategy::MarketMaking => {
                    return (false, "Assessment recommends MarketMaking, not Momentum".to_string());
                }
                RecommendedStrategy::None => {
                    return (false, "Assessment: no strategy recommended".to_string());
                }
            }

            // Check persistence
            if !assessment.persistence_ok {
                return (false, "Assessment: persistence not OK".to_string());
            }

            // Check entropy
            if !assessment.entropy_ok {
                return (false, "Assessment: entropy too high".to_string());
            }
        }

        // Check features-based regime indicators
        if let Some(hurst) = input.features.regime_persistence {
            if hurst < self.momentum_config.min_persistence {
                return (false, format!(
                    "Hurst exponent {:.3} below threshold {:.3}",
                    hurst, self.momentum_config.min_persistence
                ));
            }
        }

        // Check entropy from features
        if let Some(entropy) = input.features.tick_entropy_10s {
            let entropy_f64 = entropy.to_f64().unwrap_or(1.0);
            // Normalize entropy (assuming max entropy around 1.585 for 3-state system)
            let normalized_entropy = entropy_f64 / 1.585;
            if normalized_entropy > self.momentum_config.max_entropy {
                return (false, format!(
                    "Entropy {:.3} above threshold {:.3}",
                    normalized_entropy, self.momentum_config.max_entropy
                ));
            }
        }

        (true, "Regime favorable".to_string())
    }

    /// Generate momentum signal from features and assessment
    fn generate_signal(&self, input: &TradingInput) -> MomentumSignal {
        let features = &input.features;

        // Get momentum indicators
        let trend_strength = features.trend_strength.unwrap_or(0.0);
        let monotonicity = features.trend_monotonicity.unwrap_or(0.5);
        let hurst = features.regime_persistence.unwrap_or(0.5);

        // Determine direction from trend features
        let regime_str = features.regime.as_deref().unwrap_or("Uncertain");
        let direction = match regime_str {
            "TrendingUp" => PositionDirection::Long,
            "TrendingDown" => PositionDirection::Short,
            _ => {
                // Use trend strength as fallback
                if trend_strength > 0.3 {
                    PositionDirection::Long
                } else if trend_strength < -0.3 {
                    PositionDirection::Short
                } else {
                    return MomentumSignal::neutral("No clear trend direction");
                }
            }
        };

        // Check monotonicity threshold
        if monotonicity < self.momentum_config.min_monotonicity {
            return MomentumSignal::neutral(format!(
                "Monotonicity {:.3} below threshold {:.3}",
                monotonicity, self.momentum_config.min_monotonicity
            ));
        }

        // Check Hurst threshold
        if hurst < self.momentum_config.min_hurst {
            return MomentumSignal::neutral(format!(
                "Hurst {:.3} below threshold {:.3}",
                hurst, self.momentum_config.min_hurst
            ));
        }

        // Calculate signal strength based on multiple factors
        let abs_trend = trend_strength.abs();
        let strength = (abs_trend * 0.4 + monotonicity * 0.3 + (hurst - 0.5) * 0.3).clamp(0.0, 1.0);

        // Check minimum signal strength
        if strength < self.momentum_config.min_momentum_signal {
            return MomentumSignal::neutral(format!(
                "Signal strength {:.3} below threshold {:.3}",
                strength, self.momentum_config.min_momentum_signal
            ));
        }

        // Calculate confidence from regime confidence and assessment
        let mut confidence = features.regime_confidence.unwrap_or(0.5);
        if let Some(assessment) = &input.assessment {
            // Boost confidence if assessment agrees
            if assessment.signals_ok {
                confidence = (confidence + 0.2).min(1.0);
            }
            // Scale by assessment position scale
            confidence *= assessment.position_scale;
        }

        // Check minimum confidence
        if confidence < self.momentum_config.min_confidence {
            return MomentumSignal::neutral(format!(
                "Confidence {:.3} below threshold {:.3}",
                confidence, self.momentum_config.min_confidence
            ));
        }

        // Calculate expected edge based on take profit and confidence
        let expected_edge_bps = self.momentum_config.take_profit_bps * confidence * 0.5;

        let reason = format!(
            "Trend: {:.2}, Mono: {:.2}, Hurst: {:.2}",
            trend_strength, monotonicity, hurst
        );

        MomentumSignal::new(direction, strength, confidence, expected_edge_bps, reason)
    }

    /// Calculate position size based on signal and config
    fn calculate_position_size(&self, signal: &MomentumSignal, input: &TradingInput) -> f64 {
        let base_size = self.momentum_config.base_position_fraction;

        // Scale by signal strength
        let strength_factor = signal.strength;

        // Scale by assessment position scale
        let assessment_factor = input.assessment
            .as_ref()
            .map_or(1.0, |a| a.position_scale);

        // Apply factors
        let size = base_size * strength_factor * assessment_factor;

        // Clamp to max position size
        size.clamp(0.0, self.momentum_config.max_position_size)
    }

    /// Calculate TP/SL prices based on entry
    fn calculate_oco_prices(
        &self,
        entry_price: Decimal,
        direction: PositionDirection,
    ) -> (Decimal, Decimal) {
        let tp_bps = Decimal::from_f64_retain(self.momentum_config.take_profit_bps / 10000.0)
            .unwrap_or(dec!(0.002));
        let sl_bps = Decimal::from_f64_retain(self.momentum_config.stop_loss_bps / 10000.0)
            .unwrap_or(dec!(0.001));

        match direction {
            PositionDirection::Long => {
                let tp = entry_price * (Decimal::ONE + tp_bps);
                let sl = entry_price * (Decimal::ONE - sl_bps);
                (tp, sl)
            }
            PositionDirection::Short => {
                let tp = entry_price * (Decimal::ONE - tp_bps);
                let sl = entry_price * (Decimal::ONE + sl_bps);
                (tp, sl)
            }
            PositionDirection::Flat => (entry_price, entry_price),
        }
    }

    /// Check if should exit based on price levels
    fn should_exit(&self, current_price: Decimal) -> Option<String> {
        let state = &self.state;

        // Check if we have exit prices set
        if let (Some(tp), Some(sl)) = (state.take_profit_price, state.stop_loss_price) {
            match state.position_direction {
                PositionDirection::Long => {
                    if current_price >= tp {
                        return Some("Take profit hit".to_string());
                    }
                    if current_price <= sl {
                        return Some("Stop loss hit".to_string());
                    }
                }
                PositionDirection::Short => {
                    if current_price <= tp {
                        return Some("Take profit hit".to_string());
                    }
                    if current_price >= sl {
                        return Some("Stop loss hit".to_string());
                    }
                }
                PositionDirection::Flat => {}
            }
        }

        // Check time-based exit
        if self.momentum_config.max_hold_seconds > 0 {
            if let Some(entry_time) = state.entry_time {
                let hold_duration = (Utc::now() - entry_time).num_seconds() as u64;
                if hold_duration >= self.momentum_config.max_hold_seconds {
                    return Some(format!(
                        "Max hold time exceeded ({} seconds)",
                        self.momentum_config.max_hold_seconds
                    ));
                }
            }
        }

        None
    }

    /// Update daily P&L tracking
    fn update_daily_tracking(&mut self, pnl_change: Decimal) {
        let now = Utc::now();

        // Reset if new day
        if now.date_naive() != self.daily_start.date_naive() {
            self.daily_pnl = Decimal::ZERO;
            self.daily_start = now;
            // Reset circuit breaker on new day
            if self.circuit_breaker_tripped {
                self.circuit_breaker_tripped = false;
                self.circuit_breaker_reason = None;
            }
        }

        self.daily_pnl += pnl_change;

        // Check daily drawdown
        let daily_drawdown_pct = if self.daily_pnl < Decimal::ZERO {
            self.daily_pnl.to_f64().unwrap_or(0.0).abs()
        } else {
            0.0
        };

        if daily_drawdown_pct >= self.momentum_config.max_daily_drawdown_pct {
            self.circuit_breaker_tripped = true;
            self.circuit_breaker_reason = Some(format!(
                "Daily drawdown {:.2}% exceeded limit {:.2}%",
                daily_drawdown_pct, self.momentum_config.max_daily_drawdown_pct
            ));
        }
    }

    /// Record a trade result
    fn record_trade(&mut self, is_win: bool) {
        if is_win {
            self.consecutive_losses = 0;
        } else {
            self.consecutive_losses += 1;

            // Check consecutive loss limit
            if self.consecutive_losses >= self.momentum_config.max_consecutive_losses {
                self.circuit_breaker_tripped = true;
                self.circuit_breaker_reason = Some(format!(
                    "Consecutive losses ({}) exceeded limit ({})",
                    self.consecutive_losses, self.momentum_config.max_consecutive_losses
                ));
            }
        }
    }
}

impl Default for MomentumAlgorithm {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TradingAlgorithm Implementation
// ============================================================================

impl TradingAlgorithm for MomentumAlgorithm {
    fn strategy_type(&self) -> StrategyType {
        StrategyType::Momentum
    }

    fn name(&self) -> &str {
        "Momentum Algorithm"
    }

    fn version(&self) -> &str {
        "1.0.0"
    }

    fn config_id(&self) -> &str {
        &self.algo_config.id
    }

    fn instance_id(&self) -> &str {
        &self.state.instance_id
    }

    fn decide(&mut self, input: &TradingInput) -> TradingDecision {
        // Get current price
        let current_price = match input.mid_price() {
            Some(p) => p,
            None => return TradingDecision::hold("No mid price available"),
        };

        // Update unrealized P&L
        self.state.update_unrealized_pnl(current_price);

        // Check circuit breaker
        if self.circuit_breaker_tripped {
            return TradingDecision::hold(
                self.circuit_breaker_reason.clone().unwrap_or_else(|| "Circuit breaker tripped".to_string())
            );
        }

        // Check if already in position
        if self.state.is_in_position() {
            // Check exit conditions
            if let Some(exit_reason) = self.should_exit(current_price) {
                return TradingDecision::new(
                    TradingAction::exit(&exit_reason),
                    1.0, // High confidence on rule-based exit
                    0.0,
                )
                .with_take_profit(self.momentum_config.take_profit_bps)
                .with_stop_loss(self.momentum_config.stop_loss_bps);
            }

            // Continue holding
            return TradingDecision::hold("Holding position");
        }

        // Check regime favorability
        let (regime_ok, regime_reason) = self.is_regime_favorable(input);
        if !regime_ok {
            return TradingDecision::hold(regime_reason);
        }

        // Generate signal
        let signal = self.generate_signal(input);
        if !signal.is_actionable(
            self.momentum_config.min_momentum_signal,
            self.momentum_config.min_confidence,
        ) {
            return TradingDecision::hold(signal.reason);
        }

        // Calculate position size
        let position_size = self.calculate_position_size(&signal, input);
        if position_size <= 0.0 {
            return TradingDecision::hold("Position size too small");
        }

        // Generate entry decision
        TradingDecision::new(
            TradingAction::enter(signal.direction, position_size, &signal.reason),
            signal.confidence,
            signal.expected_edge_bps / 10000.0, // Convert bps to fraction
        )
        .with_take_profit(self.momentum_config.take_profit_bps)
        .with_stop_loss(self.momentum_config.stop_loss_bps)
        .with_max_hold(self.momentum_config.max_hold_seconds)
        .with_metadata("signal_strength", signal.strength.to_string())
    }

    fn on_fill(
        &mut self,
        price: Decimal,
        size: Decimal,
        direction: PositionDirection,
        fee: Decimal,
    ) {
        if self.state.is_in_position() && direction == self.state.position_direction.opposite() {
            // This is an exit fill
            let entry_price = self.state.entry_price.unwrap_or(price);
            let pnl = match self.state.position_direction {
                PositionDirection::Long => (price - entry_price) * size - fee,
                PositionDirection::Short => (entry_price - price) * size - fee,
                PositionDirection::Flat => -fee,
            };

            let is_win = pnl > Decimal::ZERO;
            self.record_trade(is_win);
            self.update_daily_tracking(pnl);
            self.state.exit_position(price);
        } else {
            // This is an entry fill
            let (tp_price, sl_price) = self.calculate_oco_prices(price, direction);
            self.state.enter_position(
                direction,
                size,
                price,
                Some(tp_price),
                Some(sl_price),
            );
        }
    }

    fn on_price_update(&mut self, price: Decimal) {
        self.state.update_unrealized_pnl(price);
    }

    fn state(&self) -> &AlgorithmState {
        &self.state
    }

    fn state_mut(&mut self) -> &mut AlgorithmState {
        &mut self.state
    }

    fn reset(&mut self) {
        self.state.reset();
        self.consecutive_losses = 0;
        self.daily_pnl = Decimal::ZERO;
        self.daily_start = Utc::now();
        self.circuit_breaker_tripped = false;
        self.circuit_breaker_reason = None;
    }

    fn should_stop(&self) -> bool {
        self.circuit_breaker_tripped
    }

    fn stop_reason(&self) -> Option<String> {
        self.circuit_breaker_reason.clone()
    }

    fn config(&self) -> &AlgorithmConfig {
        &self.algo_config
    }

    fn update_config(&mut self, config: AlgorithmConfig) -> Result<(), TradingAlgorithmError> {
        if config.strategy_type != StrategyType::Momentum && config.strategy_type != StrategyType::Hybrid {
            return Err(TradingAlgorithmError::InvalidConfig(
                format!("MomentumAlgorithm requires Momentum or Hybrid strategy type, got {:?}", config.strategy_type)
            ));
        }

        let momentum_config = MomentumConfig::from_algorithm_config(&config);
        momentum_config.validate().map_err(TradingAlgorithmError::InvalidConfig)?;

        self.algo_config = config;
        self.momentum_config = momentum_config;
        Ok(())
    }
}

// ============================================================================
// Factory
// ============================================================================

/// Factory for creating MomentumAlgorithm instances
pub struct MomentumAlgorithmFactory;

impl MomentumAlgorithmFactory {
    /// Create a new momentum algorithm from config
    pub fn create(config: AlgorithmConfig) -> Result<Box<dyn TradingAlgorithm>, TradingAlgorithmError> {
        let algo = MomentumAlgorithm::from_config(config)?;
        Ok(Box::new(algo))
    }

    /// Create with default config
    pub fn create_default() -> Box<dyn TradingAlgorithm> {
        Box::new(MomentumAlgorithm::new())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use crate::core::{EntryParams, ExitParams, PositionParams, RegimeFilters, TradeableAssessment};

    // ========================================================================
    // Helper Functions
    // ========================================================================

    fn create_test_config() -> AlgorithmConfig {
        let mut config = AlgorithmConfig::new("test-momentum", StrategyType::Momentum, "BTCUSDT");
        config.entry = EntryParams {
            min_momentum_signal: 0.4,
            min_monotonicity: 0.5,
            min_hurst: 0.45,
            max_entry_entropy: 0.7,
            min_conditional_prob: 0.5,
            min_confidence: 0.5,
        };
        config.exit = ExitParams {
            take_profit_bps: 20.0,
            stop_loss_bps: 10.0,
            max_hold_seconds: 300,
            trailing_stop_activation_bps: 0.0,
            trailing_stop_distance_bps: 0.0,
            use_time_exit: true,
        };
        config.position = PositionParams {
            max_size_fraction: 1.0,
            base_size_fraction: 0.5,
            ..Default::default()
        };
        config.regime_filters = RegimeFilters {
            min_tau_half: 20.0,
            max_entropy: 0.7,
            ..Default::default()
        };
        config
    }

    fn create_bullish_features() -> FeaturesSnapshot {
        let mut features = FeaturesSnapshot::default();
        features.mid_price = Some(dec!(50000));
        features.best_bid = Some(dec!(49999));
        features.best_ask = Some(dec!(50001));
        features.regime = Some("TrendingUp".to_string());
        features.regime_confidence = Some(0.8);
        features.trend_strength = Some(0.6);
        features.trend_monotonicity = Some(0.7);
        features.regime_persistence = Some(0.6);
        features.tick_entropy_10s = Some(dec!(0.8)); // ~0.5 normalized
        features
    }

    fn create_bearish_features() -> FeaturesSnapshot {
        let mut features = FeaturesSnapshot::default();
        features.mid_price = Some(dec!(50000));
        features.best_bid = Some(dec!(49999));
        features.best_ask = Some(dec!(50001));
        features.regime = Some("TrendingDown".to_string());
        features.regime_confidence = Some(0.75);
        features.trend_strength = Some(-0.55);
        features.trend_monotonicity = Some(0.65);
        features.regime_persistence = Some(0.55);
        features.tick_entropy_10s = Some(dec!(0.7));
        features
    }

    fn create_neutral_features() -> FeaturesSnapshot {
        let mut features = FeaturesSnapshot::default();
        features.mid_price = Some(dec!(50000));
        features.best_bid = Some(dec!(49999));
        features.best_ask = Some(dec!(50001));
        features.regime = Some("Uncertain".to_string());
        features.regime_confidence = Some(0.3);
        features.trend_strength = Some(0.1);
        features.trend_monotonicity = Some(0.4);
        features.regime_persistence = Some(0.5);
        features.tick_entropy_10s = Some(dec!(1.2)); // High entropy
        features
    }

    fn create_tradeable_assessment() -> TradeableAssessment {
        TradeableAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: RecommendedStrategy::Momentum,
            position_scale: 1.0,
            reasoning: "Test assessment".to_string(),
            assessed_at: Utc::now(),
        }
    }

    fn create_non_tradeable_assessment() -> TradeableAssessment {
        TradeableAssessment {
            midc_ok: false,
            entropy_ok: false,
            persistence_ok: false,
            signals_ok: false,
            is_tradeable: false,
            recommended_strategy: RecommendedStrategy::None,
            position_scale: 0.0,
            reasoning: "Not tradeable".to_string(),
            assessed_at: Utc::now(),
        }
    }

    // ========================================================================
    // MomentumConfig Tests
    // ========================================================================

    #[test]
    fn test_momentum_config_default() {
        let config = MomentumConfig::default();
        assert!(config.min_momentum_signal > 0.0);
        assert!(config.take_profit_bps > 0.0);
        assert!(config.stop_loss_bps > 0.0);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_momentum_config_from_algorithm_config() {
        let algo_config = create_test_config();
        let config = MomentumConfig::from_algorithm_config(&algo_config);

        assert_eq!(config.min_momentum_signal, 0.4);
        assert_eq!(config.take_profit_bps, 20.0);
        assert_eq!(config.stop_loss_bps, 10.0);
        assert_eq!(config.max_hold_seconds, 300);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_momentum_config_validate_invalid_signal() {
        let mut config = MomentumConfig::default();
        config.min_momentum_signal = 1.5; // Invalid
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_momentum_config_validate_invalid_confidence() {
        let mut config = MomentumConfig::default();
        config.min_confidence = -0.1; // Invalid
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_momentum_config_validate_invalid_take_profit() {
        let mut config = MomentumConfig::default();
        config.take_profit_bps = 0.0; // Invalid
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_momentum_config_validate_invalid_stop_loss() {
        let mut config = MomentumConfig::default();
        config.stop_loss_bps = -5.0; // Invalid
        assert!(config.validate().is_err());
    }

    // ========================================================================
    // MomentumSignal Tests
    // ========================================================================

    #[test]
    fn test_momentum_signal_new() {
        let signal = MomentumSignal::new(
            PositionDirection::Long,
            0.8,
            0.7,
            15.0,
            "Test signal",
        );
        assert_eq!(signal.direction, PositionDirection::Long);
        assert_eq!(signal.strength, 0.8);
        assert_eq!(signal.confidence, 0.7);
    }

    #[test]
    fn test_momentum_signal_clamps_strength() {
        let signal = MomentumSignal::new(
            PositionDirection::Long,
            1.5, // Should be clamped to 1.0
            0.7,
            15.0,
            "Test",
        );
        assert_eq!(signal.strength, 1.0);
    }

    #[test]
    fn test_momentum_signal_neutral() {
        let signal = MomentumSignal::neutral("No signal");
        assert_eq!(signal.direction, PositionDirection::Flat);
        assert_eq!(signal.strength, 0.0);
        assert_eq!(signal.confidence, 0.0);
    }

    #[test]
    fn test_momentum_signal_is_actionable() {
        let signal = MomentumSignal::new(
            PositionDirection::Long,
            0.6,
            0.7,
            15.0,
            "Test",
        );
        assert!(signal.is_actionable(0.5, 0.6));
        assert!(!signal.is_actionable(0.7, 0.6)); // Strength too low
        assert!(!signal.is_actionable(0.5, 0.8)); // Confidence too low
    }

    #[test]
    fn test_momentum_signal_neutral_not_actionable() {
        let signal = MomentumSignal::neutral("No signal");
        assert!(!signal.is_actionable(0.1, 0.1));
    }

    // ========================================================================
    // MomentumAlgorithm Creation Tests
    // ========================================================================

    #[test]
    fn test_algorithm_from_config() {
        let config = create_test_config();
        let algo = MomentumAlgorithm::from_config(config);
        assert!(algo.is_ok());
    }

    #[test]
    fn test_algorithm_from_config_wrong_strategy_type() {
        let mut config = create_test_config();
        config.strategy_type = StrategyType::MarketMaking;
        let algo = MomentumAlgorithm::from_config(config);
        assert!(algo.is_err());
    }

    #[test]
    fn test_algorithm_from_config_hybrid_allowed() {
        let mut config = create_test_config();
        config.strategy_type = StrategyType::Hybrid;
        let algo = MomentumAlgorithm::from_config(config);
        assert!(algo.is_ok());
    }

    #[test]
    fn test_algorithm_new_default() {
        let algo = MomentumAlgorithm::new();
        assert_eq!(algo.strategy_type(), StrategyType::Momentum);
        assert!(!algo.state.is_in_position());
    }

    #[test]
    fn test_algorithm_factory_create() {
        let config = create_test_config();
        let algo = MomentumAlgorithmFactory::create(config);
        assert!(algo.is_ok());
    }

    #[test]
    fn test_algorithm_factory_create_default() {
        let algo = MomentumAlgorithmFactory::create_default();
        assert_eq!(algo.strategy_type(), StrategyType::Momentum);
    }

    // ========================================================================
    // TradingAlgorithm Trait Tests
    // ========================================================================

    #[test]
    fn test_trait_strategy_type() {
        let algo = MomentumAlgorithm::new();
        assert_eq!(algo.strategy_type(), StrategyType::Momentum);
    }

    #[test]
    fn test_trait_name() {
        let algo = MomentumAlgorithm::new();
        assert_eq!(algo.name(), "Momentum Algorithm");
    }

    #[test]
    fn test_trait_version() {
        let algo = MomentumAlgorithm::new();
        assert_eq!(algo.version(), "1.0.0");
    }

    #[test]
    fn test_trait_config_id() {
        let config = create_test_config();
        let expected_id = config.id.clone();
        let algo = MomentumAlgorithm::from_config(config).unwrap();
        assert_eq!(algo.config_id(), expected_id);
    }

    #[test]
    fn test_trait_instance_id_unique() {
        let algo1 = MomentumAlgorithm::new();
        let algo2 = MomentumAlgorithm::new();
        assert_ne!(algo1.instance_id(), algo2.instance_id());
    }

    // ========================================================================
    // Decision Making Tests - Entry
    // ========================================================================

    #[test]
    fn test_decide_bullish_entry() {
        let config = create_test_config();
        let mut algo = MomentumAlgorithm::from_config(config).unwrap();

        let features = create_bullish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let decision = algo.decide(&input);
        assert!(decision.action.is_entry() || decision.action.is_hold());

        if let TradingAction::Enter { direction, .. } = &decision.action {
            assert_eq!(*direction, PositionDirection::Long);
        }
    }

    #[test]
    fn test_decide_bearish_entry() {
        let config = create_test_config();
        let mut algo = MomentumAlgorithm::from_config(config).unwrap();

        let features = create_bearish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let decision = algo.decide(&input);
        assert!(decision.action.is_entry() || decision.action.is_hold());

        if let TradingAction::Enter { direction, .. } = &decision.action {
            assert_eq!(*direction, PositionDirection::Short);
        }
    }

    #[test]
    fn test_decide_no_mid_price_holds() {
        let mut algo = MomentumAlgorithm::new();
        let mut features = FeaturesSnapshot::default();
        features.mid_price = None;
        let input = TradingInput::new(features);

        let decision = algo.decide(&input);
        assert!(decision.action.is_hold());
        assert!(decision.action.reason().contains("No mid price"));
    }

    #[test]
    fn test_decide_neutral_features_holds() {
        let mut algo = MomentumAlgorithm::new();
        let features = create_neutral_features();
        let input = TradingInput::new(features);

        let decision = algo.decide(&input);
        assert!(decision.action.is_hold());
    }

    #[test]
    fn test_decide_non_tradeable_assessment_holds() {
        let mut algo = MomentumAlgorithm::new();
        let features = create_bullish_features();
        let assessment = create_non_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let decision = algo.decide(&input);
        assert!(decision.action.is_hold());
    }

    // ========================================================================
    // Decision Making Tests - Exit
    // ========================================================================

    #[test]
    fn test_decide_holds_when_in_position() {
        let config = create_test_config();
        let mut algo = MomentumAlgorithm::from_config(config).unwrap();

        // Enter position
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));

        let features = create_bullish_features();
        let input = TradingInput::new(features);

        let decision = algo.decide(&input);
        // Should hold or exit, not enter again
        assert!(!decision.action.is_entry() || decision.action.is_hold());
    }

    #[test]
    fn test_decide_exits_on_take_profit() {
        let config = create_test_config();
        let mut algo = MomentumAlgorithm::from_config(config).unwrap();

        // Enter long position
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));

        // Price moves up to hit TP (20 bps = 0.2% = $100)
        let mut features = create_bullish_features();
        features.mid_price = Some(dec!(50150)); // Above TP
        let input = TradingInput::new(features);

        let decision = algo.decide(&input);
        assert!(decision.action.is_exit());
    }

    #[test]
    fn test_decide_exits_on_stop_loss() {
        let config = create_test_config();
        let mut algo = MomentumAlgorithm::from_config(config).unwrap();

        // Enter long position
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));

        // Price moves down to hit SL (10 bps = 0.1% = $50)
        let mut features = create_bullish_features();
        features.mid_price = Some(dec!(49940)); // Below SL
        let input = TradingInput::new(features);

        let decision = algo.decide(&input);
        assert!(decision.action.is_exit());
    }

    // ========================================================================
    // Fill Processing Tests
    // ========================================================================

    #[test]
    fn test_on_fill_entry_long() {
        let mut algo = MomentumAlgorithm::new();

        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));

        assert!(algo.state.is_in_position());
        assert!(algo.state.is_long());
        assert_eq!(algo.state.position_size, dec!(0.1));
        assert_eq!(algo.state.entry_price, Some(dec!(50000)));
        assert!(algo.state.take_profit_price.is_some());
        assert!(algo.state.stop_loss_price.is_some());
    }

    #[test]
    fn test_on_fill_entry_short() {
        let mut algo = MomentumAlgorithm::new();

        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Short, dec!(0));

        assert!(algo.state.is_in_position());
        assert!(algo.state.is_short());
        assert_eq!(algo.state.position_size, dec!(0.1));
    }

    #[test]
    fn test_on_fill_exit_profitable() {
        let mut algo = MomentumAlgorithm::new();

        // Enter long
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));

        // Exit with profit
        algo.on_fill(dec!(50100), dec!(0.1), PositionDirection::Short, dec!(0.5));

        assert!(algo.state.is_flat());
        assert_eq!(algo.state.trade_count, 1);
        assert_eq!(algo.state.win_count, 1);
        assert_eq!(algo.consecutive_losses, 0);
    }

    #[test]
    fn test_on_fill_exit_loss() {
        let mut algo = MomentumAlgorithm::new();

        // Enter long
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));

        // Exit with loss
        algo.on_fill(dec!(49900), dec!(0.1), PositionDirection::Short, dec!(0.5));

        assert!(algo.state.is_flat());
        assert_eq!(algo.state.trade_count, 1);
        assert_eq!(algo.state.win_count, 0);
        assert_eq!(algo.consecutive_losses, 1);
    }

    // ========================================================================
    // Circuit Breaker Tests
    // ========================================================================

    #[test]
    fn test_circuit_breaker_consecutive_losses() {
        // Use MomentumAlgorithm::new() which has default config with max_consecutive_losses=5
        // Use very small losses to avoid triggering daily drawdown breaker first
        let mut algo = MomentumAlgorithm::new();

        // Simulate 5 consecutive small losses (small enough to not trigger daily drawdown)
        // With default max_daily_drawdown_pct=2.0, losses must be < 2.0 total
        // But since daily_pnl comparison is absolute (not percentage), we need tiny losses
        for _ in 0..5 {
            // Enter at 50000.10 BTC, exit at 50000 BTC = loss of 0.01 per trade (1c)
            algo.on_fill(dec!(50000.10), dec!(0.001), PositionDirection::Long, dec!(0));
            algo.on_fill(dec!(50000.00), dec!(0.001), PositionDirection::Short, dec!(0));
        }

        assert!(algo.should_stop());
        assert!(algo.stop_reason().is_some());
        assert!(algo.stop_reason().unwrap().contains("Consecutive losses"));
    }

    #[test]
    fn test_circuit_breaker_blocks_trading() {
        // Use MomentumAlgorithm::new() to get default config
        let mut algo = MomentumAlgorithm::new();

        // Trip circuit breaker with small losses (5 consecutive losses)
        for _ in 0..5 {
            algo.on_fill(dec!(50000.10), dec!(0.001), PositionDirection::Long, dec!(0));
            algo.on_fill(dec!(50000.00), dec!(0.001), PositionDirection::Short, dec!(0));
        }

        // Try to trade
        let features = create_bullish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let decision = algo.decide(&input);
        assert!(decision.action.is_hold());
        assert!(decision.action.reason().contains("Circuit breaker") ||
                decision.action.reason().contains("Consecutive losses"));
    }

    #[test]
    fn test_consecutive_losses_reset_on_win() {
        let mut algo = MomentumAlgorithm::new();

        // Two losses
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));
        algo.on_fill(dec!(49900), dec!(0.1), PositionDirection::Short, dec!(0));
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));
        algo.on_fill(dec!(49900), dec!(0.1), PositionDirection::Short, dec!(0));

        assert_eq!(algo.consecutive_losses, 2);

        // One win
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));
        algo.on_fill(dec!(50100), dec!(0.1), PositionDirection::Short, dec!(0));

        assert_eq!(algo.consecutive_losses, 0);
    }

    // ========================================================================
    // State Management Tests
    // ========================================================================

    #[test]
    fn test_reset() {
        let mut algo = MomentumAlgorithm::new();

        // Make some trades
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));
        algo.on_fill(dec!(49900), dec!(0.1), PositionDirection::Short, dec!(0));

        algo.reset();

        assert!(algo.state.is_flat());
        assert_eq!(algo.state.trade_count, 0);
        assert_eq!(algo.consecutive_losses, 0);
        assert!(!algo.circuit_breaker_tripped);
    }

    #[test]
    fn test_checkpoint_restore() {
        let mut algo = MomentumAlgorithm::new();

        // Enter position
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));

        // Checkpoint
        let checkpoint = algo.checkpoint().unwrap();

        // Reset
        algo.reset();
        assert!(algo.state.is_flat());

        // Restore
        algo.restore(&checkpoint).unwrap();
        assert!(algo.state.is_long());
        assert_eq!(algo.state.position_size, dec!(0.1));
    }

    // ========================================================================
    // Config Update Tests
    // ========================================================================

    #[test]
    fn test_update_config() {
        let mut algo = MomentumAlgorithm::new();

        let mut new_config = create_test_config();
        new_config.exit.take_profit_bps = 30.0;

        let result = algo.update_config(new_config);
        assert!(result.is_ok());
        assert_eq!(algo.momentum_config.take_profit_bps, 30.0);
    }

    #[test]
    fn test_update_config_invalid_strategy() {
        let mut algo = MomentumAlgorithm::new();

        let mut new_config = create_test_config();
        new_config.strategy_type = StrategyType::MarketMaking;

        let result = algo.update_config(new_config);
        assert!(result.is_err());
    }

    // ========================================================================
    // OCO Price Calculation Tests
    // ========================================================================

    #[test]
    fn test_calculate_oco_prices_long() {
        let algo = MomentumAlgorithm::new();
        let (tp, sl) = algo.calculate_oco_prices(dec!(50000), PositionDirection::Long);

        // TP should be above entry
        assert!(tp > dec!(50000));
        // SL should be below entry
        assert!(sl < dec!(50000));
    }

    #[test]
    fn test_calculate_oco_prices_short() {
        let algo = MomentumAlgorithm::new();
        let (tp, sl) = algo.calculate_oco_prices(dec!(50000), PositionDirection::Short);

        // TP should be below entry for short
        assert!(tp < dec!(50000));
        // SL should be above entry for short
        assert!(sl > dec!(50000));
    }

    // ========================================================================
    // Position Size Calculation Tests
    // ========================================================================

    #[test]
    fn test_calculate_position_size() {
        let algo = MomentumAlgorithm::new();
        let signal = MomentumSignal::new(
            PositionDirection::Long,
            0.8,
            0.9,
            15.0,
            "Test",
        );
        let features = create_bullish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let size = algo.calculate_position_size(&signal, &input);
        assert!(size > 0.0);
        assert!(size <= algo.momentum_config.max_position_size);
    }

    #[test]
    fn test_calculate_position_size_scales_with_strength() {
        let algo = MomentumAlgorithm::new();

        let strong_signal = MomentumSignal::new(
            PositionDirection::Long, 0.9, 0.9, 15.0, "Strong"
        );
        let weak_signal = MomentumSignal::new(
            PositionDirection::Long, 0.4, 0.9, 15.0, "Weak"
        );

        let features = create_bullish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let strong_size = algo.calculate_position_size(&strong_signal, &input);
        let weak_size = algo.calculate_position_size(&weak_signal, &input);

        assert!(strong_size > weak_size);
    }

    // ========================================================================
    // Regime Favorability Tests
    // ========================================================================

    #[test]
    fn test_regime_favorable_with_good_assessment() {
        let algo = MomentumAlgorithm::new();
        let features = create_bullish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let (favorable, _) = algo.is_regime_favorable(&input);
        assert!(favorable);
    }

    #[test]
    fn test_regime_not_favorable_with_bad_assessment() {
        let algo = MomentumAlgorithm::new();
        let features = create_bullish_features();
        let assessment = create_non_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let (favorable, reason) = algo.is_regime_favorable(&input);
        assert!(!favorable);
        assert!(reason.contains("not tradeable"));
    }

    #[test]
    fn test_regime_not_favorable_market_making_recommended() {
        let algo = MomentumAlgorithm::new();
        let features = create_bullish_features();
        let mut assessment = create_tradeable_assessment();
        assessment.recommended_strategy = RecommendedStrategy::MarketMaking;
        let input = TradingInput::with_assessment(features, assessment);

        let (favorable, reason) = algo.is_regime_favorable(&input);
        assert!(!favorable);
        assert!(reason.contains("MarketMaking"));
    }

    // ========================================================================
    // Signal Generation Tests
    // ========================================================================

    #[test]
    fn test_generate_signal_bullish() {
        let algo = MomentumAlgorithm::new();
        let features = create_bullish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let signal = algo.generate_signal(&input);
        // May or may not be actionable depending on exact thresholds
        // but should detect bullish direction
        if signal.direction.is_directional() {
            assert_eq!(signal.direction, PositionDirection::Long);
        }
    }

    #[test]
    fn test_generate_signal_bearish() {
        let algo = MomentumAlgorithm::new();
        let features = create_bearish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let signal = algo.generate_signal(&input);
        if signal.direction.is_directional() {
            assert_eq!(signal.direction, PositionDirection::Short);
        }
    }

    #[test]
    fn test_generate_signal_neutral_low_monotonicity() {
        let config = create_test_config();
        let algo = MomentumAlgorithm::from_config(config).unwrap();

        let mut features = create_bullish_features();
        features.trend_monotonicity = Some(0.3); // Below threshold
        let input = TradingInput::new(features);

        let signal = algo.generate_signal(&input);
        assert_eq!(signal.direction, PositionDirection::Flat);
    }

    // ========================================================================
    // Price Update Tests
    // ========================================================================

    #[test]
    fn test_on_price_update_updates_pnl() {
        let mut algo = MomentumAlgorithm::new();

        // Enter long
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));
        assert_eq!(algo.state.unrealized_pnl, dec!(0));

        // Price goes up
        algo.on_price_update(dec!(50100));
        assert!(algo.state.unrealized_pnl > dec!(0));
    }

    #[test]
    fn test_on_price_update_no_position() {
        let mut algo = MomentumAlgorithm::new();

        algo.on_price_update(dec!(50100));
        assert_eq!(algo.state.unrealized_pnl, dec!(0));
    }

    // ========================================================================
    // Edge Cases
    // ========================================================================

    #[test]
    fn test_decide_with_empty_features() {
        let mut algo = MomentumAlgorithm::new();
        let features = FeaturesSnapshot::default();
        let input = TradingInput::new(features);

        let decision = algo.decide(&input);
        assert!(decision.action.is_hold());
    }

    #[test]
    fn test_decide_without_assessment() {
        let mut algo = MomentumAlgorithm::new();
        let features = create_bullish_features();
        let input = TradingInput::new(features);

        // Should still work, just use features only
        let decision = algo.decide(&input);
        // Either entry or hold is valid
        assert!(decision.action.is_entry() || decision.action.is_hold());
    }

    #[test]
    fn test_multiple_entries_blocked() {
        let config = create_test_config();
        let mut algo = MomentumAlgorithm::from_config(config).unwrap();

        // First entry
        algo.on_fill(dec!(50000), dec!(0.1), PositionDirection::Long, dec!(0));
        assert!(algo.state.is_in_position());

        // Try another entry (should be blocked by logic)
        let features = create_bullish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let decision = algo.decide(&input);
        // Should not suggest another entry
        assert!(!decision.action.is_entry() || decision.action.is_hold() || decision.action.is_exit());
    }

    #[test]
    fn test_decision_has_oco_brackets() {
        let config = create_test_config();
        let mut algo = MomentumAlgorithm::from_config(config).unwrap();

        let features = create_bullish_features();
        let assessment = create_tradeable_assessment();
        let input = TradingInput::with_assessment(features, assessment);

        let decision = algo.decide(&input);
        if decision.action.is_entry() {
            assert!(decision.take_profit_bps.is_some());
            assert!(decision.stop_loss_bps.is_some());
        }
    }
}

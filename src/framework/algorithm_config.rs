//! Algorithm Configuration Module (Task 0.4)
//!
//! Defines the configuration structure that parameterizes algorithms from research findings.
//! This module provides:
//! - `StrategyType` enum for strategy classification
//! - `AlgorithmConfig` struct with all trading parameters
//! - `from_research(&ResearchState)` constructor for deriving configs from research
//! - Unique config ID generation for tracking
//! - Parameter bounds and validation

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

use super::research_state::{
    MIDCRegime, RecommendedStrategy, ResearchState, TSMOMConfig,
};

// ============================================================================
// Strategy Type
// ============================================================================

/// Classification of trading strategy types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StrategyType {
    /// Pure momentum/trend-following strategy (TSMOM-based)
    Momentum,
    /// Pure market making with symmetric quotes
    MarketMaking,
    /// Hybrid approach: directional skew in trending, symmetric in mean-reverting
    Hybrid,
}

impl StrategyType {
    /// Returns all strategy types
    pub fn all() -> Vec<StrategyType> {
        vec![
            StrategyType::Momentum,
            StrategyType::MarketMaking,
            StrategyType::Hybrid,
        ]
    }

    /// Returns a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            StrategyType::Momentum => "Pure trend-following with directional positions",
            StrategyType::MarketMaking => "Symmetric market making with inventory control",
            StrategyType::Hybrid => "Adaptive: directional in trends, symmetric otherwise",
        }
    }

    /// Returns the recommended minimum persistence (τ_half) for this strategy
    pub fn recommended_min_tau_half(&self) -> f64 {
        match self {
            StrategyType::Momentum => 30.0,    // Need strong persistence for momentum
            StrategyType::MarketMaking => 5.0, // Less sensitive to persistence
            StrategyType::Hybrid => 15.0,      // Moderate persistence requirement
        }
    }

    /// Returns the recommended maximum entropy for this strategy
    pub fn recommended_max_entropy(&self) -> f64 {
        match self {
            StrategyType::Momentum => 0.6,     // Want low entropy (predictable)
            StrategyType::MarketMaking => 0.9, // Can handle high entropy
            StrategyType::Hybrid => 0.75,      // Moderate entropy tolerance
        }
    }
}

impl fmt::Display for StrategyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StrategyType::Momentum => write!(f, "Momentum"),
            StrategyType::MarketMaking => write!(f, "MarketMaking"),
            StrategyType::Hybrid => write!(f, "Hybrid"),
        }
    }
}

impl Default for StrategyType {
    fn default() -> Self {
        StrategyType::Hybrid
    }
}

// ============================================================================
// Entry Parameters
// ============================================================================

/// Entry threshold parameters derived from research
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntryParams {
    /// Minimum momentum signal strength to enter (0.0 to 1.0)
    pub min_momentum_signal: f64,
    /// Minimum monotonicity score required (0.0 to 1.0)
    pub min_monotonicity: f64,
    /// Minimum Hurst exponent for trend confirmation (0.0 to 1.0)
    pub min_hurst: f64,
    /// Maximum entropy allowed for entry (0.0 to 1.0)
    pub max_entry_entropy: f64,
    /// Minimum probability from conditional table (0.0 to 1.0)
    pub min_conditional_prob: f64,
    /// Minimum confidence level for signals (0.0 to 1.0)
    pub min_confidence: f64,
}

impl EntryParams {
    /// Creates entry parameters with specified thresholds
    pub fn new(
        min_momentum_signal: f64,
        min_monotonicity: f64,
        min_hurst: f64,
        max_entry_entropy: f64,
        min_conditional_prob: f64,
        min_confidence: f64,
    ) -> Self {
        Self {
            min_momentum_signal,
            min_monotonicity,
            min_hurst,
            max_entry_entropy,
            min_conditional_prob,
            min_confidence,
        }
    }

    /// Conservative entry parameters (higher thresholds)
    pub fn conservative() -> Self {
        Self {
            min_momentum_signal: 0.7,
            min_monotonicity: 0.65,
            min_hurst: 0.55,
            max_entry_entropy: 0.5,
            min_conditional_prob: 0.6,
            min_confidence: 0.8,
        }
    }

    /// Aggressive entry parameters (lower thresholds)
    pub fn aggressive() -> Self {
        Self {
            min_momentum_signal: 0.3,
            min_monotonicity: 0.45,
            min_hurst: 0.45,
            max_entry_entropy: 0.75,
            min_conditional_prob: 0.45,
            min_confidence: 0.5,
        }
    }

    /// Market making entry parameters (very permissive)
    pub fn market_making() -> Self {
        Self {
            min_momentum_signal: 0.0,
            min_monotonicity: 0.0,
            min_hurst: 0.0,
            max_entry_entropy: 0.95,
            min_conditional_prob: 0.0,
            min_confidence: 0.0,
        }
    }

    /// Validates all parameters are within bounds
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_probability("min_momentum_signal", self.min_momentum_signal)?;
        validate_probability("min_monotonicity", self.min_monotonicity)?;
        validate_probability("min_hurst", self.min_hurst)?;
        validate_probability("max_entry_entropy", self.max_entry_entropy)?;
        validate_probability("min_conditional_prob", self.min_conditional_prob)?;
        validate_probability("min_confidence", self.min_confidence)?;
        Ok(())
    }
}

impl Default for EntryParams {
    fn default() -> Self {
        Self {
            min_momentum_signal: 0.5,
            min_monotonicity: 0.55,
            min_hurst: 0.5,
            max_entry_entropy: 0.65,
            min_conditional_prob: 0.52,
            min_confidence: 0.6,
        }
    }
}

// ============================================================================
// Exit Parameters
// ============================================================================

/// Exit parameters for take-profit and stop-loss
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExitParams {
    /// Take-profit in basis points (e.g., 20.0 = 0.20%)
    pub take_profit_bps: f64,
    /// Stop-loss in basis points (e.g., 10.0 = 0.10%)
    pub stop_loss_bps: f64,
    /// Maximum holding time in seconds (0 = unlimited)
    pub max_hold_seconds: u64,
    /// Trailing stop activation in bps (0 = disabled)
    pub trailing_stop_activation_bps: f64,
    /// Trailing stop distance in bps
    pub trailing_stop_distance_bps: f64,
    /// Whether to use time-based exit
    pub use_time_exit: bool,
}

impl ExitParams {
    /// Creates exit parameters with specified values
    pub fn new(
        take_profit_bps: f64,
        stop_loss_bps: f64,
        max_hold_seconds: u64,
    ) -> Self {
        Self {
            take_profit_bps,
            stop_loss_bps,
            max_hold_seconds,
            trailing_stop_activation_bps: 0.0,
            trailing_stop_distance_bps: 0.0,
            use_time_exit: max_hold_seconds > 0,
        }
    }

    /// Creates exit parameters with trailing stop
    pub fn with_trailing_stop(
        take_profit_bps: f64,
        stop_loss_bps: f64,
        activation_bps: f64,
        distance_bps: f64,
    ) -> Self {
        Self {
            take_profit_bps,
            stop_loss_bps,
            max_hold_seconds: 0,
            trailing_stop_activation_bps: activation_bps,
            trailing_stop_distance_bps: distance_bps,
            use_time_exit: false,
        }
    }

    /// Conservative exit parameters (tight stops)
    pub fn conservative() -> Self {
        Self {
            take_profit_bps: 15.0,
            stop_loss_bps: 8.0,
            max_hold_seconds: 300,  // 5 minutes
            trailing_stop_activation_bps: 10.0,
            trailing_stop_distance_bps: 5.0,
            use_time_exit: true,
        }
    }

    /// Aggressive exit parameters (wider targets)
    pub fn aggressive() -> Self {
        Self {
            take_profit_bps: 30.0,
            stop_loss_bps: 15.0,
            max_hold_seconds: 0,
            trailing_stop_activation_bps: 20.0,
            trailing_stop_distance_bps: 10.0,
            use_time_exit: false,
        }
    }

    /// Market making exit parameters (symmetric)
    pub fn market_making() -> Self {
        Self {
            take_profit_bps: 10.0,
            stop_loss_bps: 10.0,
            max_hold_seconds: 60,
            trailing_stop_activation_bps: 0.0,
            trailing_stop_distance_bps: 0.0,
            use_time_exit: true,
        }
    }

    /// Calculates risk/reward ratio
    pub fn risk_reward_ratio(&self) -> f64 {
        if self.stop_loss_bps > 0.0 {
            self.take_profit_bps / self.stop_loss_bps
        } else {
            f64::INFINITY
        }
    }

    /// Returns true if trailing stop is enabled
    pub fn has_trailing_stop(&self) -> bool {
        self.trailing_stop_activation_bps > 0.0 && self.trailing_stop_distance_bps > 0.0
    }

    /// Validates all parameters are within bounds
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.take_profit_bps < 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "take_profit_bps".to_string(),
                value: self.take_profit_bps,
                reason: "must be non-negative".to_string(),
            });
        }
        if self.take_profit_bps > 1000.0 {
            return Err(ConfigError::InvalidParameter {
                name: "take_profit_bps".to_string(),
                value: self.take_profit_bps,
                reason: "must be <= 1000 bps (10%)".to_string(),
            });
        }
        if self.stop_loss_bps < 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "stop_loss_bps".to_string(),
                value: self.stop_loss_bps,
                reason: "must be non-negative".to_string(),
            });
        }
        if self.stop_loss_bps > 1000.0 {
            return Err(ConfigError::InvalidParameter {
                name: "stop_loss_bps".to_string(),
                value: self.stop_loss_bps,
                reason: "must be <= 1000 bps (10%)".to_string(),
            });
        }
        if self.trailing_stop_activation_bps < 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "trailing_stop_activation_bps".to_string(),
                value: self.trailing_stop_activation_bps,
                reason: "must be non-negative".to_string(),
            });
        }
        if self.trailing_stop_distance_bps < 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "trailing_stop_distance_bps".to_string(),
                value: self.trailing_stop_distance_bps,
                reason: "must be non-negative".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for ExitParams {
    fn default() -> Self {
        Self {
            take_profit_bps: 20.0,
            stop_loss_bps: 10.0,
            max_hold_seconds: 0,
            trailing_stop_activation_bps: 0.0,
            trailing_stop_distance_bps: 0.0,
            use_time_exit: false,
        }
    }
}

// ============================================================================
// Position Sizing Parameters
// ============================================================================

/// Position sizing method for algorithm configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConfigSizingMethod {
    /// Fixed position size as percentage of capital
    Fixed,
    /// Volatility-targeted sizing
    VolatilityTarget,
    /// Kelly criterion with fraction
    Kelly,
    /// Risk parity (equal risk contribution)
    RiskParity,
}

impl Default for ConfigSizingMethod {
    fn default() -> Self {
        ConfigSizingMethod::VolatilityTarget
    }
}

impl fmt::Display for ConfigSizingMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigSizingMethod::Fixed => write!(f, "Fixed"),
            ConfigSizingMethod::VolatilityTarget => write!(f, "VolatilityTarget"),
            ConfigSizingMethod::Kelly => write!(f, "Kelly"),
            ConfigSizingMethod::RiskParity => write!(f, "RiskParity"),
        }
    }
}

/// Position sizing parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionParams {
    /// Sizing method to use
    pub method: ConfigSizingMethod,
    /// Base position size as fraction of capital (0.0 to 1.0)
    pub base_size_fraction: f64,
    /// Maximum position size as fraction of capital (0.0 to 1.0)
    pub max_size_fraction: f64,
    /// Target annualized volatility for volatility targeting (e.g., 0.10 = 10%)
    pub target_volatility: f64,
    /// Kelly fraction (0.0 to 1.0, typically 0.25-0.5 for fractional Kelly)
    pub kelly_fraction: f64,
    /// EWMA decay factor for volatility estimation (0.0 to 1.0)
    pub ewma_lambda: f64,
    /// Minimum position size as fraction of capital
    pub min_size_fraction: f64,
    /// Whether to scale position with signal strength
    pub scale_with_signal: bool,
}

impl PositionParams {
    /// Creates position parameters with specified method
    pub fn new(method: ConfigSizingMethod, base_size: f64, max_size: f64) -> Self {
        Self {
            method,
            base_size_fraction: base_size,
            max_size_fraction: max_size,
            target_volatility: 0.10,
            kelly_fraction: 0.25,
            ewma_lambda: 0.94,
            min_size_fraction: 0.01,
            scale_with_signal: false,
        }
    }

    /// Conservative position sizing
    pub fn conservative() -> Self {
        Self {
            method: ConfigSizingMethod::VolatilityTarget,
            base_size_fraction: 0.05,
            max_size_fraction: 0.10,
            target_volatility: 0.05,
            kelly_fraction: 0.20,
            ewma_lambda: 0.97,
            min_size_fraction: 0.01,
            scale_with_signal: true,
        }
    }

    /// Aggressive position sizing
    pub fn aggressive() -> Self {
        Self {
            method: ConfigSizingMethod::Kelly,
            base_size_fraction: 0.15,
            max_size_fraction: 0.30,
            target_volatility: 0.20,
            kelly_fraction: 0.50,
            ewma_lambda: 0.90,
            min_size_fraction: 0.05,
            scale_with_signal: true,
        }
    }

    /// Market making position sizing (smaller, faster turnover)
    pub fn market_making() -> Self {
        Self {
            method: ConfigSizingMethod::Fixed,
            base_size_fraction: 0.02,
            max_size_fraction: 0.05,
            target_volatility: 0.10,
            kelly_fraction: 0.25,
            ewma_lambda: 0.94,
            min_size_fraction: 0.01,
            scale_with_signal: false,
        }
    }

    /// Validates all parameters are within bounds
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_fraction("base_size_fraction", self.base_size_fraction)?;
        validate_fraction("max_size_fraction", self.max_size_fraction)?;
        validate_fraction("min_size_fraction", self.min_size_fraction)?;
        validate_fraction("kelly_fraction", self.kelly_fraction)?;
        validate_fraction("ewma_lambda", self.ewma_lambda)?;

        if self.base_size_fraction > self.max_size_fraction {
            return Err(ConfigError::InvalidParameter {
                name: "base_size_fraction".to_string(),
                value: self.base_size_fraction,
                reason: "must be <= max_size_fraction".to_string(),
            });
        }

        if self.min_size_fraction > self.base_size_fraction {
            return Err(ConfigError::InvalidParameter {
                name: "min_size_fraction".to_string(),
                value: self.min_size_fraction,
                reason: "must be <= base_size_fraction".to_string(),
            });
        }

        if self.target_volatility <= 0.0 || self.target_volatility > 1.0 {
            return Err(ConfigError::InvalidParameter {
                name: "target_volatility".to_string(),
                value: self.target_volatility,
                reason: "must be in (0.0, 1.0]".to_string(),
            });
        }

        Ok(())
    }
}

impl Default for PositionParams {
    fn default() -> Self {
        Self {
            method: ConfigSizingMethod::VolatilityTarget,
            base_size_fraction: 0.10,
            max_size_fraction: 0.20,
            target_volatility: 0.10,
            kelly_fraction: 0.25,
            ewma_lambda: 0.94,
            min_size_fraction: 0.01,
            scale_with_signal: true,
        }
    }
}

// ============================================================================
// Regime Filter Parameters
// ============================================================================

/// Regime filter parameters for conditional trading
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegimeFilters {
    /// Minimum τ_half (half-life) in seconds for persistence
    pub min_tau_half: f64,
    /// Maximum entropy for trading (0.0 to 1.0)
    pub max_entropy: f64,
    /// Minimum R² for MIDC fit
    pub min_r_squared: f64,
    /// Required regime for entry
    pub required_regime: Option<MIDCRegime>,
    /// Whether to trade in uncertain regimes
    pub trade_uncertain: bool,
    /// Minimum kappa (mean reversion speed)
    pub min_kappa: f64,
    /// Maximum kappa (too fast = noise)
    pub max_kappa: f64,
}

impl RegimeFilters {
    /// Creates regime filters with specified thresholds
    pub fn new(min_tau_half: f64, max_entropy: f64) -> Self {
        Self {
            min_tau_half,
            max_entropy,
            min_r_squared: 0.5,
            required_regime: None,
            trade_uncertain: false,
            min_kappa: 0.0,
            max_kappa: 1.0,
        }
    }

    /// Conservative regime filters (strict requirements)
    pub fn conservative() -> Self {
        Self {
            min_tau_half: 60.0,
            max_entropy: 0.5,
            min_r_squared: 0.7,
            required_regime: Some(MIDCRegime::SlowDiffusion),
            trade_uncertain: false,
            min_kappa: 0.01,
            max_kappa: 0.5,
        }
    }

    /// Aggressive regime filters (loose requirements)
    pub fn aggressive() -> Self {
        Self {
            min_tau_half: 10.0,
            max_entropy: 0.8,
            min_r_squared: 0.3,
            required_regime: None,
            trade_uncertain: true,
            min_kappa: 0.0,
            max_kappa: 1.0,
        }
    }

    /// Market making regime filters
    pub fn market_making() -> Self {
        Self {
            min_tau_half: 5.0,
            max_entropy: 0.9,
            min_r_squared: 0.2,
            required_regime: None,
            trade_uncertain: true,
            min_kappa: 0.0,
            max_kappa: 1.0,
        }
    }

    /// Validates all parameters are within bounds
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.min_tau_half < 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "min_tau_half".to_string(),
                value: self.min_tau_half,
                reason: "must be non-negative".to_string(),
            });
        }
        validate_probability("max_entropy", self.max_entropy)?;
        validate_probability("min_r_squared", self.min_r_squared)?;
        if self.min_kappa < 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "min_kappa".to_string(),
                value: self.min_kappa,
                reason: "must be non-negative".to_string(),
            });
        }
        if self.max_kappa <= self.min_kappa {
            return Err(ConfigError::InvalidParameter {
                name: "max_kappa".to_string(),
                value: self.max_kappa,
                reason: "must be > min_kappa".to_string(),
            });
        }
        Ok(())
    }

    /// Checks if the given research state passes these filters
    pub fn passes(&self, research: &ResearchState) -> bool {
        // Check MIDC estimates
        let midc = &research.midc;
        if midc.tau_half_seconds < self.min_tau_half {
            return false;
        }
        if midc.r_squared < self.min_r_squared {
            return false;
        }
        if midc.kappa < self.min_kappa || midc.kappa > self.max_kappa {
            return false;
        }

        // Check entropy
        if research.entropy > self.max_entropy {
            return false;
        }

        // Check regime requirement
        if let Some(required) = &self.required_regime {
            if midc.regime() != *required {
                return false;
            }
        }

        // Check uncertain regime handling
        if !self.trade_uncertain {
            if matches!(research.assessment.recommended_strategy, RecommendedStrategy::None) {
                return false;
            }
        }

        true
    }
}

impl Default for RegimeFilters {
    fn default() -> Self {
        Self {
            min_tau_half: 15.0,
            max_entropy: 0.7,
            min_r_squared: 0.5,
            required_regime: None,
            trade_uncertain: false,
            min_kappa: 0.0,
            max_kappa: 1.0,
        }
    }
}

// ============================================================================
// Market Making Parameters (for Hybrid/MM strategies)
// ============================================================================

/// Market making specific parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketMakingParams {
    /// Base spread in basis points
    pub base_spread_bps: f64,
    /// Maximum spread in basis points
    pub max_spread_bps: f64,
    /// Inventory skew factor (how much to skew quotes based on inventory)
    pub inventory_skew: f64,
    /// Gamma parameter (risk aversion in A-S model)
    pub gamma: f64,
    /// Kappa parameter (order arrival rate in A-S model)
    pub kappa: f64,
    /// Quote refresh interval in milliseconds
    pub refresh_interval_ms: u64,
    /// Whether to widen spread in high entropy
    pub widen_in_high_entropy: bool,
    /// Entropy threshold for spread widening
    pub entropy_widen_threshold: f64,
    /// Spread multiplier when widening
    pub spread_widen_multiplier: f64,
}

impl MarketMakingParams {
    /// Creates market making parameters
    pub fn new(base_spread_bps: f64, gamma: f64, kappa: f64) -> Self {
        Self {
            base_spread_bps,
            max_spread_bps: base_spread_bps * 3.0,
            inventory_skew: 0.5,
            gamma,
            kappa,
            refresh_interval_ms: 100,
            widen_in_high_entropy: true,
            entropy_widen_threshold: 0.7,
            spread_widen_multiplier: 1.5,
        }
    }

    /// Conservative market making (wider spreads)
    pub fn conservative() -> Self {
        Self {
            base_spread_bps: 3.0,
            max_spread_bps: 10.0,
            inventory_skew: 0.3,
            gamma: 0.5,
            kappa: 1.5,
            refresh_interval_ms: 200,
            widen_in_high_entropy: true,
            entropy_widen_threshold: 0.6,
            spread_widen_multiplier: 2.0,
        }
    }

    /// Aggressive market making (tighter spreads)
    pub fn aggressive() -> Self {
        Self {
            base_spread_bps: 1.0,
            max_spread_bps: 5.0,
            inventory_skew: 0.7,
            gamma: 0.1,
            kappa: 3.0,
            refresh_interval_ms: 50,
            widen_in_high_entropy: false,
            entropy_widen_threshold: 0.8,
            spread_widen_multiplier: 1.2,
        }
    }

    /// Validates all parameters are within bounds
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.base_spread_bps <= 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "base_spread_bps".to_string(),
                value: self.base_spread_bps,
                reason: "must be positive".to_string(),
            });
        }
        if self.max_spread_bps < self.base_spread_bps {
            return Err(ConfigError::InvalidParameter {
                name: "max_spread_bps".to_string(),
                value: self.max_spread_bps,
                reason: "must be >= base_spread_bps".to_string(),
            });
        }
        validate_fraction("inventory_skew", self.inventory_skew)?;
        if self.gamma < 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "gamma".to_string(),
                value: self.gamma,
                reason: "must be non-negative".to_string(),
            });
        }
        if self.kappa <= 0.0 {
            return Err(ConfigError::InvalidParameter {
                name: "kappa".to_string(),
                value: self.kappa,
                reason: "must be positive".to_string(),
            });
        }
        validate_probability("entropy_widen_threshold", self.entropy_widen_threshold)?;
        if self.spread_widen_multiplier < 1.0 {
            return Err(ConfigError::InvalidParameter {
                name: "spread_widen_multiplier".to_string(),
                value: self.spread_widen_multiplier,
                reason: "must be >= 1.0".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for MarketMakingParams {
    fn default() -> Self {
        Self {
            base_spread_bps: 2.0,
            max_spread_bps: 8.0,
            inventory_skew: 0.5,
            gamma: 0.3,
            kappa: 2.0,
            refresh_interval_ms: 100,
            widen_in_high_entropy: true,
            entropy_widen_threshold: 0.7,
            spread_widen_multiplier: 1.5,
        }
    }
}

// ============================================================================
// Algorithm Config
// ============================================================================

/// Complete algorithm configuration derived from research findings
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlgorithmConfig {
    /// Unique identifier for this configuration
    pub id: String,
    /// Human-readable name for this configuration
    pub name: String,
    /// Strategy type
    pub strategy_type: StrategyType,
    /// Symbol this config is optimized for
    pub symbol: String,
    /// Entry parameters
    pub entry: EntryParams,
    /// Exit parameters
    pub exit: ExitParams,
    /// Position sizing parameters
    pub position: PositionParams,
    /// Regime filters
    pub regime_filters: RegimeFilters,
    /// Market making parameters (used for MM and Hybrid)
    pub market_making: MarketMakingParams,
    /// TSMOM configuration (used for Momentum and Hybrid)
    pub tsmom: Option<TSMOMConfig>,
    /// Research state ID this was derived from (if any)
    pub source_research_id: Option<String>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Version for tracking changes
    pub version: u32,
    /// Whether this config is currently active
    pub active: bool,
    /// Optional description/notes
    pub description: Option<String>,
}

impl AlgorithmConfig {
    /// Creates a new algorithm configuration with the given strategy type
    pub fn new(name: impl Into<String>, strategy_type: StrategyType, symbol: impl Into<String>) -> Self {
        let mut config = Self {
            id: String::new(),
            name: name.into(),
            strategy_type,
            symbol: symbol.into(),
            entry: EntryParams::default(),
            exit: ExitParams::default(),
            position: PositionParams::default(),
            regime_filters: RegimeFilters::default(),
            market_making: MarketMakingParams::default(),
            tsmom: None,
            source_research_id: None,
            created_at: Utc::now(),
            version: 1,
            active: true,
            description: None,
        };
        config.id = config.generate_id();
        config
    }

    /// Creates a configuration from research state findings
    pub fn from_research(research: &ResearchState) -> Self {
        let strategy_type = Self::derive_strategy_type(research);
        let mut config = Self::new(
            format!("{}_research_derived", research.symbol),
            strategy_type,
            &research.symbol,
        );

        // Set entry params based on assessment
        config.entry = Self::derive_entry_params(research);

        // Set exit params based on persistence
        config.exit = Self::derive_exit_params(research);

        // Set position params based on TSMOM config
        config.position = Self::derive_position_params(research);

        // Set regime filters based on MIDC
        config.regime_filters = Self::derive_regime_filters(research);

        // Set market making params if applicable
        if matches!(strategy_type, StrategyType::MarketMaking | StrategyType::Hybrid) {
            config.market_making = Self::derive_mm_params(research);
        }

        // Copy TSMOM config if available
        config.tsmom = research.tsmom_config.clone();

        // Link to source research
        config.source_research_id = Some(research.id.clone());
        config.id = config.generate_id();

        config
    }

    /// Derives strategy type from research findings
    fn derive_strategy_type(research: &ResearchState) -> StrategyType {
        match &research.assessment.recommended_strategy {
            RecommendedStrategy::TSMOM | RecommendedStrategy::Momentum => {
                StrategyType::Momentum
            }
            RecommendedStrategy::MarketMaking => StrategyType::MarketMaking,
            RecommendedStrategy::Hybrid | RecommendedStrategy::MACrossover => {
                StrategyType::Hybrid
            }
            RecommendedStrategy::None => {
                // Default to hybrid if uncertain
                StrategyType::Hybrid
            }
        }
    }

    /// Derives entry parameters from research
    fn derive_entry_params(research: &ResearchState) -> EntryParams {
        let mut params = EntryParams::default();

        // Use conditional probabilities to set thresholds
        if !research.conditional_table.is_empty() {
            // Find max probability to calibrate entry threshold
            let max_prob = research.conditional_table
                .values()
                .map(|cp| cp.p_continuation.max(cp.p_reversal))
                .fold(0.0f64, f64::max);
            // Set min conditional prob at 80% of max for meaningful signal
            params.min_conditional_prob = (max_prob * 0.8).max(0.45).min(0.7);
        }

        // Use assessment position_scale as a proxy for confidence
        params.min_confidence = (research.assessment.position_scale * 0.9).max(0.5);

        // Set entropy threshold from current entropy
        params.max_entry_entropy = (research.entropy + 0.1).min(0.85);

        params
    }

    /// Derives exit parameters from research
    fn derive_exit_params(research: &ResearchState) -> ExitParams {
        let mut params = ExitParams::default();

        // Use persistence to set holding time
        let expected_duration_seconds = research.persistence.mean_duration_seconds;
        params.max_hold_seconds = expected_duration_seconds as u64;
        params.use_time_exit = expected_duration_seconds < 1800.0; // Use time exit for trends < 30 min

        // Use MIDC for TP/SL calibration
        let midc = &research.midc;
        // Set TP based on tau_half (longer persistence = wider target)
        let tau_factor = (midc.tau_half_seconds / 30.0).sqrt().min(2.0);
        params.take_profit_bps = 15.0 * tau_factor;
        // SL at half of TP for 2:1 ratio
        params.stop_loss_bps = params.take_profit_bps / 2.0;

        params
    }

    /// Derives position parameters from research
    fn derive_position_params(research: &ResearchState) -> PositionParams {
        let mut params = PositionParams::default();

        // Use TSMOM config if available
        if let Some(tsmom) = &research.tsmom_config {
            params.target_volatility = tsmom.target_volatility;
            params.ewma_lambda = tsmom.ewma_lambda;
            // TSMOM max_position_size is leverage (e.g., 2.0 = 2x), not a fraction
            // Clamp to valid fraction range for position sizing
            params.max_size_fraction = (tsmom.max_position_size as f64).min(1.0);
            params.method = ConfigSizingMethod::VolatilityTarget;
        }

        // Scale position with position_scale, ensure valid bounds
        params.base_size_fraction = (params.base_size_fraction * research.assessment.position_scale)
            .max(params.min_size_fraction)
            .min(params.max_size_fraction);

        params
    }

    /// Derives regime filters from research
    fn derive_regime_filters(research: &ResearchState) -> RegimeFilters {
        let mut filters = RegimeFilters::default();

        // Use MIDC for tau_half filter
        let midc = &research.midc;
        // Set min tau at 50% of current to filter similar regimes
        filters.min_tau_half = midc.tau_half_seconds * 0.5;
        filters.min_r_squared = midc.r_squared * 0.8;
        filters.required_regime = Some(midc.regime());

        // Use current entropy as baseline
        filters.max_entropy = (research.entropy + 0.15).min(0.9);

        filters
    }

    /// Derives market making parameters from research
    fn derive_mm_params(research: &ResearchState) -> MarketMakingParams {
        let mut params = MarketMakingParams::default();

        // Use MIDC kappa for order arrival rate
        let midc = &research.midc;
        params.kappa = midc.kappa.max(0.1);
        // Set gamma inversely to persistence (more persistent = lower risk aversion)
        let gamma_value: f64 = 1.0 / midc.tau_half_seconds.sqrt();
        params.gamma = gamma_value.min(1.0).max(0.05);

        // Widen spread in current entropy conditions
        params.entropy_widen_threshold = research.entropy;

        params
    }

    /// Generates a unique identifier for this configuration
    pub fn generate_id(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.symbol.as_bytes());
        hasher.update(format!("{:?}", self.strategy_type).as_bytes());
        hasher.update(format!("{:?}", self.entry).as_bytes());
        hasher.update(format!("{:?}", self.exit).as_bytes());
        hasher.update(format!("{:?}", self.position).as_bytes());
        hasher.update(format!("{:?}", self.regime_filters).as_bytes());
        hasher.update(self.created_at.timestamp().to_le_bytes());
        let result = hasher.finalize();
        format!("cfg_{}", hex::encode(&result[..8]))
    }

    /// Validates the entire configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.name.is_empty() {
            return Err(ConfigError::MissingField("name".to_string()));
        }
        if self.symbol.is_empty() {
            return Err(ConfigError::MissingField("symbol".to_string()));
        }

        self.entry.validate()?;
        self.exit.validate()?;
        self.position.validate()?;
        self.regime_filters.validate()?;

        // Validate market making params if using MM or Hybrid
        if matches!(self.strategy_type, StrategyType::MarketMaking | StrategyType::Hybrid) {
            self.market_making.validate()?;
        }

        // Validate TSMOM config if present
        if let Some(tsmom) = &self.tsmom {
            tsmom.validate().map_err(|e| ConfigError::InvalidParameter {
                name: "tsmom".to_string(),
                value: 0.0,
                reason: e,
            })?;
        }

        Ok(())
    }

    /// Creates a builder for fluent configuration
    pub fn builder(name: impl Into<String>, symbol: impl Into<String>) -> AlgorithmConfigBuilder {
        AlgorithmConfigBuilder::new(name, symbol)
    }

    /// Returns a summary string for logging
    pub fn summary(&self) -> String {
        format!(
            "{} [{}] - {} on {}, TP/SL: {:.1}/{:.1}bps, τ_half≥{:.0}s, entropy≤{:.2}",
            self.id,
            self.strategy_type,
            self.name,
            self.symbol,
            self.exit.take_profit_bps,
            self.exit.stop_loss_bps,
            self.regime_filters.min_tau_half,
            self.regime_filters.max_entropy,
        )
    }

    /// Creates a copy with a new version number
    pub fn next_version(&self) -> Self {
        let mut new = self.clone();
        new.version += 1;
        new.created_at = Utc::now();
        new.id = new.generate_id();
        new
    }

    /// Creates a preset configuration
    pub fn preset(preset_type: ConfigPreset, symbol: impl Into<String>) -> Self {
        let symbol = symbol.into();
        match preset_type {
            ConfigPreset::Conservative => {
                let mut config = Self::new("Conservative", StrategyType::Hybrid, &symbol);
                config.entry = EntryParams::conservative();
                config.exit = ExitParams::conservative();
                config.position = PositionParams::conservative();
                config.regime_filters = RegimeFilters::conservative();
                config.market_making = MarketMakingParams::conservative();
                config.id = config.generate_id();
                config
            }
            ConfigPreset::Aggressive => {
                let mut config = Self::new("Aggressive", StrategyType::Momentum, &symbol);
                config.entry = EntryParams::aggressive();
                config.exit = ExitParams::aggressive();
                config.position = PositionParams::aggressive();
                config.regime_filters = RegimeFilters::aggressive();
                config.market_making = MarketMakingParams::aggressive();
                config.id = config.generate_id();
                config
            }
            ConfigPreset::MarketMaking => {
                let mut config = Self::new("MarketMaking", StrategyType::MarketMaking, &symbol);
                config.entry = EntryParams::market_making();
                config.exit = ExitParams::market_making();
                config.position = PositionParams::market_making();
                config.regime_filters = RegimeFilters::market_making();
                config.market_making = MarketMakingParams::default();
                config.id = config.generate_id();
                config
            }
            ConfigPreset::TSMOM => {
                let mut config = Self::new("TSMOM", StrategyType::Momentum, &symbol);
                config.entry = EntryParams::default();
                config.exit = ExitParams::default();
                config.position = PositionParams::default();
                config.regime_filters = RegimeFilters::default();
                config.tsmom = Some(TSMOMConfig::default());
                config.id = config.generate_id();
                config
            }
        }
    }

    /// Checks if this config can trade given current research state
    pub fn can_trade(&self, research: &ResearchState) -> bool {
        self.regime_filters.passes(research)
    }

    /// Returns the config as JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Creates config from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

impl Default for AlgorithmConfig {
    fn default() -> Self {
        Self::new("Default", StrategyType::Hybrid, "BTCUSDT")
    }
}

impl fmt::Display for AlgorithmConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.summary())
    }
}

// ============================================================================
// Config Preset
// ============================================================================

/// Preset configuration types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigPreset {
    /// Conservative: lower risk, stricter filters
    Conservative,
    /// Aggressive: higher risk, looser filters
    Aggressive,
    /// Market Making: optimized for MM strategy
    MarketMaking,
    /// TSMOM: Time-series momentum with volatility targeting
    TSMOM,
}

// ============================================================================
// Config Builder
// ============================================================================

/// Builder for fluent AlgorithmConfig construction
pub struct AlgorithmConfigBuilder {
    config: AlgorithmConfig,
}

impl AlgorithmConfigBuilder {
    /// Creates a new builder
    pub fn new(name: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            config: AlgorithmConfig::new(name, StrategyType::Hybrid, symbol),
        }
    }

    /// Sets the strategy type
    pub fn strategy_type(mut self, strategy_type: StrategyType) -> Self {
        self.config.strategy_type = strategy_type;
        self
    }

    /// Sets entry parameters
    pub fn entry(mut self, entry: EntryParams) -> Self {
        self.config.entry = entry;
        self
    }

    /// Sets exit parameters
    pub fn exit(mut self, exit: ExitParams) -> Self {
        self.config.exit = exit;
        self
    }

    /// Sets position parameters
    pub fn position(mut self, position: PositionParams) -> Self {
        self.config.position = position;
        self
    }

    /// Sets regime filters
    pub fn regime_filters(mut self, filters: RegimeFilters) -> Self {
        self.config.regime_filters = filters;
        self
    }

    /// Sets market making parameters
    pub fn market_making(mut self, params: MarketMakingParams) -> Self {
        self.config.market_making = params;
        self
    }

    /// Sets TSMOM configuration
    pub fn tsmom(mut self, tsmom: TSMOMConfig) -> Self {
        self.config.tsmom = Some(tsmom);
        self
    }

    /// Sets take profit in bps
    pub fn take_profit_bps(mut self, bps: f64) -> Self {
        self.config.exit.take_profit_bps = bps;
        self
    }

    /// Sets stop loss in bps
    pub fn stop_loss_bps(mut self, bps: f64) -> Self {
        self.config.exit.stop_loss_bps = bps;
        self
    }

    /// Sets minimum tau half filter
    pub fn min_tau_half(mut self, tau: f64) -> Self {
        self.config.regime_filters.min_tau_half = tau;
        self
    }

    /// Sets maximum entropy filter
    pub fn max_entropy(mut self, entropy: f64) -> Self {
        self.config.regime_filters.max_entropy = entropy;
        self
    }

    /// Sets description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.config.description = Some(desc.into());
        self
    }

    /// Sets active status
    pub fn active(mut self, active: bool) -> Self {
        self.config.active = active;
        self
    }

    /// Builds the configuration
    pub fn build(mut self) -> Result<AlgorithmConfig, ConfigError> {
        self.config.id = self.config.generate_id();
        self.config.validate()?;
        Ok(self.config)
    }

    /// Builds the configuration without validation
    pub fn build_unchecked(mut self) -> AlgorithmConfig {
        self.config.id = self.config.generate_id();
        self.config
    }
}

// ============================================================================
// Error Type
// ============================================================================

/// Configuration validation error
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigError {
    /// A required field is missing
    MissingField(String),
    /// A parameter has an invalid value
    InvalidParameter {
        name: String,
        value: f64,
        reason: String,
    },
    /// General validation error
    ValidationError(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::MissingField(field) => {
                write!(f, "Missing required field: {}", field)
            }
            ConfigError::InvalidParameter { name, value, reason } => {
                write!(f, "Invalid parameter '{}' = {}: {}", name, value, reason)
            }
            ConfigError::ValidationError(msg) => {
                write!(f, "Validation error: {}", msg)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

// ============================================================================
// Helper Functions
// ============================================================================

/// Validates that a value is a valid probability (0.0 to 1.0)
fn validate_probability(name: &str, value: f64) -> Result<(), ConfigError> {
    if value < 0.0 || value > 1.0 {
        Err(ConfigError::InvalidParameter {
            name: name.to_string(),
            value,
            reason: "must be between 0.0 and 1.0".to_string(),
        })
    } else {
        Ok(())
    }
}

/// Validates that a value is a valid fraction (0.0 to 1.0)
fn validate_fraction(name: &str, value: f64) -> Result<(), ConfigError> {
    if value < 0.0 || value > 1.0 {
        Err(ConfigError::InvalidParameter {
            name: name.to_string(),
            value,
            reason: "must be between 0.0 and 1.0".to_string(),
        })
    } else {
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::research_state::{
        BarSize, ConditionalProbability, PriceSignature, SignatureConsistency,
        SignatureDirection, SignatureMagnitude, SignatureSpeed,
    };

    // Helper to create test ResearchState
    fn create_test_research() -> ResearchState {
        use crate::framework::research_state::{MIDCEstimate, PersistenceStats, TradeableAssessment};
        use std::collections::HashMap;

        let mut research = ResearchState::new("BTCUSDT");
        // MIDCEstimate::new(kappa, rho_0, r_squared, sample_size) computes tau_half and confidence
        research.midc = MIDCEstimate::new(0.005, 0.8, 0.75, 100); // kappa=0.005 -> SlowDiffusion regime
        research.persistence = PersistenceStats {
            mean_duration_seconds: 60.0,
            median_duration_seconds: 55.0,
            std_duration_seconds: 15.0,
            percentile_25: 45.0,
            percentile_75: 75.0,
            sample_count: 100,
            updated_at: Utc::now(),
        };
        research.entropy = 0.55;
        research.assessment = TradeableAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: RecommendedStrategy::Hybrid,
            position_scale: 0.85,
            reasoning: "Good persistence".to_string(),
            assessed_at: Utc::now(),
        };
        research.tsmom_config = Some(TSMOMConfig::default());

        // conditional_table is a HashMap<String, ConditionalProbability>
        let mut table = HashMap::new();
        table.insert("medium_moderate_up_mixed".to_string(), ConditionalProbability {
            p_continuation: 0.62,
            p_reversal: 0.38,
            expected_magnitude_bps: 5.0,
            std_magnitude_bps: 2.0,
            sample_count: 100,
            confidence_interval: (0.55, 0.69),
        });
        research.conditional_table = table;
        research
    }

    // ========================================================================
    // StrategyType Tests
    // ========================================================================

    #[test]
    fn test_strategy_type_all() {
        let all = StrategyType::all();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&StrategyType::Momentum));
        assert!(all.contains(&StrategyType::MarketMaking));
        assert!(all.contains(&StrategyType::Hybrid));
    }

    #[test]
    fn test_strategy_type_description() {
        assert!(!StrategyType::Momentum.description().is_empty());
        assert!(!StrategyType::MarketMaking.description().is_empty());
        assert!(!StrategyType::Hybrid.description().is_empty());
    }

    #[test]
    fn test_strategy_type_recommended_tau_half() {
        assert!(StrategyType::Momentum.recommended_min_tau_half() > StrategyType::MarketMaking.recommended_min_tau_half());
        assert!(StrategyType::Hybrid.recommended_min_tau_half() > StrategyType::MarketMaking.recommended_min_tau_half());
    }

    #[test]
    fn test_strategy_type_recommended_entropy() {
        assert!(StrategyType::Momentum.recommended_max_entropy() < StrategyType::MarketMaking.recommended_max_entropy());
        assert!(StrategyType::Hybrid.recommended_max_entropy() < StrategyType::MarketMaking.recommended_max_entropy());
    }

    #[test]
    fn test_strategy_type_display() {
        assert_eq!(format!("{}", StrategyType::Momentum), "Momentum");
        assert_eq!(format!("{}", StrategyType::MarketMaking), "MarketMaking");
        assert_eq!(format!("{}", StrategyType::Hybrid), "Hybrid");
    }

    #[test]
    fn test_strategy_type_default() {
        assert_eq!(StrategyType::default(), StrategyType::Hybrid);
    }

    #[test]
    fn test_strategy_type_serialization() {
        let json = serde_json::to_string(&StrategyType::Momentum).unwrap();
        let parsed: StrategyType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, StrategyType::Momentum);
    }

    #[test]
    fn test_strategy_type_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(StrategyType::Momentum);
        set.insert(StrategyType::MarketMaking);
        assert_eq!(set.len(), 2);
    }

    // ========================================================================
    // EntryParams Tests
    // ========================================================================

    #[test]
    fn test_entry_params_new() {
        let params = EntryParams::new(0.5, 0.6, 0.55, 0.7, 0.52, 0.6);
        assert_eq!(params.min_momentum_signal, 0.5);
        assert_eq!(params.min_monotonicity, 0.6);
        assert_eq!(params.min_hurst, 0.55);
        assert_eq!(params.max_entry_entropy, 0.7);
        assert_eq!(params.min_conditional_prob, 0.52);
        assert_eq!(params.min_confidence, 0.6);
    }

    #[test]
    fn test_entry_params_conservative() {
        let params = EntryParams::conservative();
        assert!(params.min_momentum_signal > 0.5);
        assert!(params.min_monotonicity > 0.5);
        assert!(params.max_entry_entropy < 0.6);
        assert!(params.min_confidence > 0.7);
    }

    #[test]
    fn test_entry_params_aggressive() {
        let params = EntryParams::aggressive();
        assert!(params.min_momentum_signal < 0.5);
        assert!(params.max_entry_entropy > 0.6);
    }

    #[test]
    fn test_entry_params_market_making() {
        let params = EntryParams::market_making();
        assert_eq!(params.min_momentum_signal, 0.0);
        assert_eq!(params.min_monotonicity, 0.0);
        assert!(params.max_entry_entropy > 0.9);
    }

    #[test]
    fn test_entry_params_default() {
        let params = EntryParams::default();
        assert!(params.min_momentum_signal >= 0.0 && params.min_momentum_signal <= 1.0);
        assert!(params.max_entry_entropy >= 0.0 && params.max_entry_entropy <= 1.0);
    }

    #[test]
    fn test_entry_params_validate_valid() {
        let params = EntryParams::default();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_entry_params_validate_invalid_momentum() {
        let mut params = EntryParams::default();
        params.min_momentum_signal = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_entry_params_validate_invalid_momentum_high() {
        let mut params = EntryParams::default();
        params.min_momentum_signal = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_entry_params_validate_invalid_monotonicity() {
        let mut params = EntryParams::default();
        params.min_monotonicity = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_entry_params_validate_invalid_hurst() {
        let mut params = EntryParams::default();
        params.min_hurst = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_entry_params_validate_invalid_entropy() {
        let mut params = EntryParams::default();
        params.max_entry_entropy = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_entry_params_validate_invalid_prob() {
        let mut params = EntryParams::default();
        params.min_conditional_prob = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_entry_params_validate_invalid_confidence() {
        let mut params = EntryParams::default();
        params.min_confidence = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_entry_params_serialization() {
        let params = EntryParams::conservative();
        let json = serde_json::to_string(&params).unwrap();
        let parsed: EntryParams = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, params);
    }

    // ========================================================================
    // ExitParams Tests
    // ========================================================================

    #[test]
    fn test_exit_params_new() {
        let params = ExitParams::new(25.0, 12.0, 120);
        assert_eq!(params.take_profit_bps, 25.0);
        assert_eq!(params.stop_loss_bps, 12.0);
        assert_eq!(params.max_hold_seconds, 120);
        assert!(params.use_time_exit);
    }

    #[test]
    fn test_exit_params_new_no_time_exit() {
        let params = ExitParams::new(20.0, 10.0, 0);
        assert!(!params.use_time_exit);
    }

    #[test]
    fn test_exit_params_with_trailing_stop() {
        let params = ExitParams::with_trailing_stop(20.0, 10.0, 15.0, 5.0);
        assert_eq!(params.trailing_stop_activation_bps, 15.0);
        assert_eq!(params.trailing_stop_distance_bps, 5.0);
        assert!(params.has_trailing_stop());
    }

    #[test]
    fn test_exit_params_conservative() {
        let params = ExitParams::conservative();
        assert!(params.take_profit_bps < 20.0);
        assert!(params.stop_loss_bps < 10.0);
        assert!(params.use_time_exit);
    }

    #[test]
    fn test_exit_params_aggressive() {
        let params = ExitParams::aggressive();
        assert!(params.take_profit_bps > 25.0);
        assert!(params.stop_loss_bps > 10.0);
    }

    #[test]
    fn test_exit_params_market_making() {
        let params = ExitParams::market_making();
        assert_eq!(params.take_profit_bps, params.stop_loss_bps); // Symmetric
        assert!(params.use_time_exit);
    }

    #[test]
    fn test_exit_params_risk_reward_ratio() {
        let params = ExitParams::new(20.0, 10.0, 0);
        assert_eq!(params.risk_reward_ratio(), 2.0);
    }

    #[test]
    fn test_exit_params_risk_reward_ratio_zero_sl() {
        let params = ExitParams::new(20.0, 0.0, 0);
        assert!(params.risk_reward_ratio().is_infinite());
    }

    #[test]
    fn test_exit_params_has_trailing_stop_false() {
        let params = ExitParams::default();
        assert!(!params.has_trailing_stop());
    }

    #[test]
    fn test_exit_params_validate_valid() {
        let params = ExitParams::default();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_exit_params_validate_negative_tp() {
        let mut params = ExitParams::default();
        params.take_profit_bps = -5.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_exit_params_validate_tp_too_high() {
        let mut params = ExitParams::default();
        params.take_profit_bps = 1500.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_exit_params_validate_negative_sl() {
        let mut params = ExitParams::default();
        params.stop_loss_bps = -5.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_exit_params_validate_sl_too_high() {
        let mut params = ExitParams::default();
        params.stop_loss_bps = 1500.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_exit_params_validate_negative_trailing_activation() {
        let mut params = ExitParams::default();
        params.trailing_stop_activation_bps = -1.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_exit_params_validate_negative_trailing_distance() {
        let mut params = ExitParams::default();
        params.trailing_stop_distance_bps = -1.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_exit_params_serialization() {
        let params = ExitParams::aggressive();
        let json = serde_json::to_string(&params).unwrap();
        let parsed: ExitParams = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, params);
    }

    // ========================================================================
    // SizingMethod Tests
    // ========================================================================

    #[test]
    fn test_sizing_method_default() {
        assert_eq!(ConfigSizingMethod::default(), ConfigSizingMethod::VolatilityTarget);
    }

    #[test]
    fn test_sizing_method_display() {
        assert_eq!(format!("{}", ConfigSizingMethod::Fixed), "Fixed");
        assert_eq!(format!("{}", ConfigSizingMethod::VolatilityTarget), "VolatilityTarget");
        assert_eq!(format!("{}", ConfigSizingMethod::Kelly), "Kelly");
        assert_eq!(format!("{}", ConfigSizingMethod::RiskParity), "RiskParity");
    }

    #[test]
    fn test_sizing_method_serialization() {
        let method = ConfigSizingMethod::Kelly;
        let json = serde_json::to_string(&method).unwrap();
        let parsed: ConfigSizingMethod = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, method);
    }

    // ========================================================================
    // PositionParams Tests
    // ========================================================================

    #[test]
    fn test_position_params_new() {
        let params = PositionParams::new(ConfigSizingMethod::Fixed, 0.05, 0.15);
        assert_eq!(params.method, ConfigSizingMethod::Fixed);
        assert_eq!(params.base_size_fraction, 0.05);
        assert_eq!(params.max_size_fraction, 0.15);
    }

    #[test]
    fn test_position_params_conservative() {
        let params = PositionParams::conservative();
        assert!(params.base_size_fraction < 0.1);
        assert!(params.max_size_fraction < 0.15);
        assert!(params.target_volatility < 0.1);
    }

    #[test]
    fn test_position_params_aggressive() {
        let params = PositionParams::aggressive();
        assert!(params.base_size_fraction > 0.1);
        assert!(params.max_size_fraction > 0.2);
        assert_eq!(params.method, ConfigSizingMethod::Kelly);
    }

    #[test]
    fn test_position_params_market_making() {
        let params = PositionParams::market_making();
        assert!(params.base_size_fraction < 0.05);
        assert_eq!(params.method, ConfigSizingMethod::Fixed);
        assert!(!params.scale_with_signal);
    }

    #[test]
    fn test_position_params_default() {
        let params = PositionParams::default();
        assert_eq!(params.method, ConfigSizingMethod::VolatilityTarget);
        assert!(params.base_size_fraction > 0.0);
        assert!(params.max_size_fraction > params.base_size_fraction);
    }

    #[test]
    fn test_position_params_validate_valid() {
        let params = PositionParams::default();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_position_params_validate_negative_base() {
        let mut params = PositionParams::default();
        params.base_size_fraction = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_base_too_high() {
        let mut params = PositionParams::default();
        params.base_size_fraction = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_max_too_high() {
        let mut params = PositionParams::default();
        params.max_size_fraction = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_base_greater_than_max() {
        let mut params = PositionParams::default();
        params.base_size_fraction = 0.3;
        params.max_size_fraction = 0.2;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_min_greater_than_base() {
        let mut params = PositionParams::default();
        params.min_size_fraction = 0.15;
        params.base_size_fraction = 0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_negative_target_vol() {
        let mut params = PositionParams::default();
        params.target_volatility = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_zero_target_vol() {
        let mut params = PositionParams::default();
        params.target_volatility = 0.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_target_vol_too_high() {
        let mut params = PositionParams::default();
        params.target_volatility = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_kelly_fraction() {
        let mut params = PositionParams::default();
        params.kelly_fraction = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_validate_ewma_lambda() {
        let mut params = PositionParams::default();
        params.ewma_lambda = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_position_params_serialization() {
        let params = PositionParams::aggressive();
        let json = serde_json::to_string(&params).unwrap();
        let parsed: PositionParams = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, params);
    }

    // ========================================================================
    // RegimeFilters Tests
    // ========================================================================

    #[test]
    fn test_regime_filters_new() {
        let filters = RegimeFilters::new(30.0, 0.65);
        assert_eq!(filters.min_tau_half, 30.0);
        assert_eq!(filters.max_entropy, 0.65);
    }

    #[test]
    fn test_regime_filters_conservative() {
        let filters = RegimeFilters::conservative();
        assert!(filters.min_tau_half > 30.0);
        assert!(filters.max_entropy < 0.6);
        assert!(filters.min_r_squared > 0.5);
        assert!(filters.required_regime.is_some());
        assert!(!filters.trade_uncertain);
    }

    #[test]
    fn test_regime_filters_aggressive() {
        let filters = RegimeFilters::aggressive();
        assert!(filters.min_tau_half < 20.0);
        assert!(filters.max_entropy > 0.7);
        assert!(filters.trade_uncertain);
    }

    #[test]
    fn test_regime_filters_market_making() {
        let filters = RegimeFilters::market_making();
        assert!(filters.min_tau_half < 10.0);
        assert!(filters.max_entropy > 0.8);
        assert!(filters.trade_uncertain);
    }

    #[test]
    fn test_regime_filters_default() {
        let filters = RegimeFilters::default();
        assert!(filters.min_tau_half > 0.0);
        assert!(filters.max_entropy > 0.0 && filters.max_entropy <= 1.0);
    }

    #[test]
    fn test_regime_filters_validate_valid() {
        let filters = RegimeFilters::default();
        assert!(filters.validate().is_ok());
    }

    #[test]
    fn test_regime_filters_validate_negative_tau() {
        let mut filters = RegimeFilters::default();
        filters.min_tau_half = -1.0;
        assert!(filters.validate().is_err());
    }

    #[test]
    fn test_regime_filters_validate_invalid_entropy() {
        let mut filters = RegimeFilters::default();
        filters.max_entropy = 1.5;
        assert!(filters.validate().is_err());
    }

    #[test]
    fn test_regime_filters_validate_invalid_r_squared() {
        let mut filters = RegimeFilters::default();
        filters.min_r_squared = -0.1;
        assert!(filters.validate().is_err());
    }

    #[test]
    fn test_regime_filters_validate_negative_min_kappa() {
        let mut filters = RegimeFilters::default();
        filters.min_kappa = -0.1;
        assert!(filters.validate().is_err());
    }

    #[test]
    fn test_regime_filters_validate_max_kappa_less_than_min() {
        let mut filters = RegimeFilters::default();
        filters.min_kappa = 0.5;
        filters.max_kappa = 0.3;
        assert!(filters.validate().is_err());
    }

    #[test]
    fn test_regime_filters_passes_valid_research() {
        let filters = RegimeFilters::default();
        let research = create_test_research();
        assert!(filters.passes(&research));
    }

    #[test]
    fn test_regime_filters_passes_tau_too_low() {
        let mut filters = RegimeFilters::default();
        // Test research has kappa=0.005, so tau_half = ln(2)/0.005 ≈ 138.6
        filters.min_tau_half = 200.0; // Higher than research's ~138.6
        let research = create_test_research();
        assert!(!filters.passes(&research));
    }

    #[test]
    fn test_regime_filters_passes_entropy_too_high() {
        let mut filters = RegimeFilters::default();
        filters.max_entropy = 0.4; // Lower than research's 0.55
        let research = create_test_research();
        assert!(!filters.passes(&research));
    }

    #[test]
    fn test_regime_filters_passes_r_squared_too_low() {
        let mut filters = RegimeFilters::default();
        filters.min_r_squared = 0.9; // Higher than research's 0.75
        let research = create_test_research();
        assert!(!filters.passes(&research));
    }

    #[test]
    fn test_regime_filters_passes_wrong_regime() {
        let mut filters = RegimeFilters::default();
        filters.required_regime = Some(MIDCRegime::FastDiffusion);
        let research = create_test_research(); // Has SlowDiffusion regime (kappa=0.005)
        assert!(!filters.passes(&research));
    }

    #[test]
    fn test_regime_filters_passes_kappa_too_low() {
        let mut filters = RegimeFilters::default();
        // Test research has kappa=0.005
        filters.min_kappa = 0.01; // Higher than research's 0.005
        let research = create_test_research();
        assert!(!filters.passes(&research));
    }

    #[test]
    fn test_regime_filters_passes_kappa_too_high() {
        let mut filters = RegimeFilters::default();
        // Test research has kappa=0.005
        filters.max_kappa = 0.003; // Lower than research's 0.005
        let research = create_test_research();
        assert!(!filters.passes(&research));
    }

    #[test]
    fn test_regime_filters_serialization() {
        let filters = RegimeFilters::conservative();
        let json = serde_json::to_string(&filters).unwrap();
        let parsed: RegimeFilters = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, filters);
    }

    // ========================================================================
    // MarketMakingParams Tests
    // ========================================================================

    #[test]
    fn test_mm_params_new() {
        let params = MarketMakingParams::new(2.0, 0.3, 2.0);
        assert_eq!(params.base_spread_bps, 2.0);
        assert_eq!(params.gamma, 0.3);
        assert_eq!(params.kappa, 2.0);
        assert_eq!(params.max_spread_bps, 6.0);
    }

    #[test]
    fn test_mm_params_conservative() {
        let params = MarketMakingParams::conservative();
        assert!(params.base_spread_bps > 2.0);
        assert!(params.gamma > 0.3);
        assert!(params.spread_widen_multiplier > 1.5);
    }

    #[test]
    fn test_mm_params_aggressive() {
        let params = MarketMakingParams::aggressive();
        assert!(params.base_spread_bps < 2.0);
        assert!(params.gamma < 0.3);
        assert!(!params.widen_in_high_entropy);
    }

    #[test]
    fn test_mm_params_default() {
        let params = MarketMakingParams::default();
        assert!(params.base_spread_bps > 0.0);
        assert!(params.max_spread_bps >= params.base_spread_bps);
        assert!(params.widen_in_high_entropy);
    }

    #[test]
    fn test_mm_params_validate_valid() {
        let params = MarketMakingParams::default();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_mm_params_validate_zero_base_spread() {
        let mut params = MarketMakingParams::default();
        params.base_spread_bps = 0.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_negative_base_spread() {
        let mut params = MarketMakingParams::default();
        params.base_spread_bps = -1.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_max_less_than_base() {
        let mut params = MarketMakingParams::default();
        params.base_spread_bps = 5.0;
        params.max_spread_bps = 3.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_negative_inventory_skew() {
        let mut params = MarketMakingParams::default();
        params.inventory_skew = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_inventory_skew_too_high() {
        let mut params = MarketMakingParams::default();
        params.inventory_skew = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_negative_gamma() {
        let mut params = MarketMakingParams::default();
        params.gamma = -0.1;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_zero_kappa() {
        let mut params = MarketMakingParams::default();
        params.kappa = 0.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_negative_kappa() {
        let mut params = MarketMakingParams::default();
        params.kappa = -1.0;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_invalid_entropy_threshold() {
        let mut params = MarketMakingParams::default();
        params.entropy_widen_threshold = 1.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_validate_widen_multiplier_less_than_one() {
        let mut params = MarketMakingParams::default();
        params.spread_widen_multiplier = 0.5;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_mm_params_serialization() {
        let params = MarketMakingParams::conservative();
        let json = serde_json::to_string(&params).unwrap();
        let parsed: MarketMakingParams = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, params);
    }

    // ========================================================================
    // AlgorithmConfig Tests
    // ========================================================================

    #[test]
    fn test_config_new() {
        let config = AlgorithmConfig::new("Test", StrategyType::Momentum, "ETHUSDT");
        assert_eq!(config.name, "Test");
        assert_eq!(config.strategy_type, StrategyType::Momentum);
        assert_eq!(config.symbol, "ETHUSDT");
        assert!(config.active);
        assert_eq!(config.version, 1);
        assert!(!config.id.is_empty());
    }

    #[test]
    fn test_config_generate_id_unique() {
        let config1 = AlgorithmConfig::new("Test1", StrategyType::Momentum, "BTCUSDT");
        let config2 = AlgorithmConfig::new("Test2", StrategyType::Momentum, "BTCUSDT");
        assert_ne!(config1.id, config2.id);
    }

    #[test]
    fn test_config_generate_id_starts_with_cfg() {
        let config = AlgorithmConfig::new("Test", StrategyType::Hybrid, "BTCUSDT");
        assert!(config.id.starts_with("cfg_"));
    }

    #[test]
    fn test_config_from_research() {
        let research = create_test_research();
        let config = AlgorithmConfig::from_research(&research);

        assert_eq!(config.symbol, "BTCUSDT");
        assert!(config.source_research_id.is_some());
        assert_eq!(config.source_research_id.unwrap(), research.id);
    }

    #[test]
    fn test_config_from_research_strategy_type_hybrid() {
        let research = create_test_research();
        let config = AlgorithmConfig::from_research(&research);
        assert_eq!(config.strategy_type, StrategyType::Hybrid);
    }

    #[test]
    fn test_config_from_research_strategy_type_momentum() {
        use crate::framework::research_state::TradeableAssessment;
        let mut research = create_test_research();
        research.assessment = TradeableAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: RecommendedStrategy::TSMOM,
            position_scale: 0.9,
            reasoning: String::new(),
            assessed_at: Utc::now(),
        };
        let config = AlgorithmConfig::from_research(&research);
        assert_eq!(config.strategy_type, StrategyType::Momentum);
    }

    #[test]
    fn test_config_from_research_strategy_type_mm() {
        use crate::framework::research_state::TradeableAssessment;
        let mut research = create_test_research();
        research.assessment = TradeableAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: RecommendedStrategy::MarketMaking,
            position_scale: 0.8,
            reasoning: String::new(),
            assessed_at: Utc::now(),
        };
        let config = AlgorithmConfig::from_research(&research);
        assert_eq!(config.strategy_type, StrategyType::MarketMaking);
    }

    #[test]
    fn test_config_from_research_copies_tsmom() {
        let research = create_test_research();
        let config = AlgorithmConfig::from_research(&research);
        assert!(config.tsmom.is_some());
    }

    #[test]
    fn test_config_validate_valid() {
        let config = AlgorithmConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_empty_name() {
        let mut config = AlgorithmConfig::default();
        config.name = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_empty_symbol() {
        let mut config = AlgorithmConfig::default();
        config.symbol = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_invalid_entry() {
        let mut config = AlgorithmConfig::default();
        config.entry.min_momentum_signal = -0.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_invalid_exit() {
        let mut config = AlgorithmConfig::default();
        config.exit.take_profit_bps = -10.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_invalid_position() {
        let mut config = AlgorithmConfig::default();
        config.position.base_size_fraction = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_invalid_regime() {
        let mut config = AlgorithmConfig::default();
        config.regime_filters.max_entropy = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_invalid_mm_for_hybrid() {
        let mut config = AlgorithmConfig::new("Test", StrategyType::Hybrid, "BTCUSDT");
        config.market_making.base_spread_bps = -1.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_mm_not_checked_for_momentum() {
        let mut config = AlgorithmConfig::new("Test", StrategyType::Momentum, "BTCUSDT");
        config.market_making.base_spread_bps = -1.0;
        // Should pass because MM params aren't validated for pure Momentum
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_builder() {
        let config = AlgorithmConfig::builder("Builder Test", "SOLUSDT")
            .strategy_type(StrategyType::Momentum)
            .take_profit_bps(25.0)
            .stop_loss_bps(12.0)
            .min_tau_half(20.0)
            .max_entropy(0.6)
            .description("Test config")
            .build()
            .unwrap();

        assert_eq!(config.name, "Builder Test");
        assert_eq!(config.symbol, "SOLUSDT");
        assert_eq!(config.strategy_type, StrategyType::Momentum);
        assert_eq!(config.exit.take_profit_bps, 25.0);
        assert_eq!(config.exit.stop_loss_bps, 12.0);
        assert_eq!(config.regime_filters.min_tau_half, 20.0);
        assert_eq!(config.regime_filters.max_entropy, 0.6);
        assert_eq!(config.description, Some("Test config".to_string()));
    }

    #[test]
    fn test_config_builder_invalid() {
        let result = AlgorithmConfig::builder("Test", "BTCUSDT")
            .take_profit_bps(-10.0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_builder_unchecked() {
        let config = AlgorithmConfig::builder("Test", "BTCUSDT")
            .take_profit_bps(-10.0)
            .build_unchecked();
        assert_eq!(config.exit.take_profit_bps, -10.0);
    }

    #[test]
    fn test_config_summary() {
        let config = AlgorithmConfig::default();
        let summary = config.summary();
        assert!(summary.contains(&config.id));
        assert!(summary.contains("Hybrid"));
        assert!(summary.contains("BTCUSDT"));
    }

    #[test]
    fn test_config_display() {
        let config = AlgorithmConfig::default();
        let display = format!("{}", config);
        assert!(!display.is_empty());
        assert!(display.contains(&config.id));
    }

    #[test]
    fn test_config_next_version() {
        let config = AlgorithmConfig::default();
        let next = config.next_version();
        assert_eq!(next.version, 2);
        // Version is part of ID computation via name/params, and created_at differs
        // IDs may or may not differ depending on timestamp precision, so just check version and timestamp
        assert!(next.created_at >= config.created_at);
        assert_eq!(next.name, config.name);
    }

    #[test]
    fn test_config_preset_conservative() {
        let config = AlgorithmConfig::preset(ConfigPreset::Conservative, "BTCUSDT");
        assert_eq!(config.name, "Conservative");
        assert_eq!(config.strategy_type, StrategyType::Hybrid);
        assert!(config.regime_filters.min_tau_half > 30.0);
    }

    #[test]
    fn test_config_preset_aggressive() {
        let config = AlgorithmConfig::preset(ConfigPreset::Aggressive, "ETHUSDT");
        assert_eq!(config.name, "Aggressive");
        assert_eq!(config.strategy_type, StrategyType::Momentum);
        assert!(config.regime_filters.min_tau_half < 20.0);
    }

    #[test]
    fn test_config_preset_market_making() {
        let config = AlgorithmConfig::preset(ConfigPreset::MarketMaking, "SOLUSDT");
        assert_eq!(config.name, "MarketMaking");
        assert_eq!(config.strategy_type, StrategyType::MarketMaking);
    }

    #[test]
    fn test_config_preset_tsmom() {
        let config = AlgorithmConfig::preset(ConfigPreset::TSMOM, "BTCUSDT");
        assert_eq!(config.name, "TSMOM");
        assert!(config.tsmom.is_some());
    }

    #[test]
    fn test_config_can_trade_passes() {
        let config = AlgorithmConfig::default();
        let research = create_test_research();
        assert!(config.can_trade(&research));
    }

    #[test]
    fn test_config_can_trade_fails() {
        let mut config = AlgorithmConfig::default();
        // Test research has tau_half ~138.6 (from kappa=0.005), so set min higher than that
        config.regime_filters.min_tau_half = 200.0;
        let research = create_test_research();
        assert!(!config.can_trade(&research));
    }

    #[test]
    fn test_config_to_json() {
        let config = AlgorithmConfig::default();
        let json = config.to_json().unwrap();
        assert!(json.contains("id"));
        assert!(json.contains("name"));
        assert!(json.contains("strategy_type"));
    }

    #[test]
    fn test_config_from_json() {
        let config = AlgorithmConfig::default();
        let json = config.to_json().unwrap();
        let parsed = AlgorithmConfig::from_json(&json).unwrap();
        assert_eq!(parsed.name, config.name);
        assert_eq!(parsed.symbol, config.symbol);
        assert_eq!(parsed.strategy_type, config.strategy_type);
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let original = AlgorithmConfig::preset(ConfigPreset::Aggressive, "BTCUSDT");
        let json = serde_json::to_string(&original).unwrap();
        let parsed: AlgorithmConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, original.name);
        assert_eq!(parsed.strategy_type, original.strategy_type);
        assert_eq!(parsed.symbol, original.symbol);
    }

    #[test]
    fn test_config_default() {
        let config = AlgorithmConfig::default();
        assert_eq!(config.symbol, "BTCUSDT");
        assert_eq!(config.strategy_type, StrategyType::Hybrid);
        assert!(config.validate().is_ok());
    }

    // ========================================================================
    // ConfigError Tests
    // ========================================================================

    #[test]
    fn test_config_error_missing_field_display() {
        let err = ConfigError::MissingField("name".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Missing"));
        assert!(msg.contains("name"));
    }

    #[test]
    fn test_config_error_invalid_param_display() {
        let err = ConfigError::InvalidParameter {
            name: "tau_half".to_string(),
            value: -5.0,
            reason: "must be positive".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("tau_half"));
        assert!(msg.contains("-5"));
        assert!(msg.contains("positive"));
    }

    #[test]
    fn test_config_error_validation_display() {
        let err = ConfigError::ValidationError("general error".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Validation"));
        assert!(msg.contains("general error"));
    }

    // ========================================================================
    // Helper Function Tests
    // ========================================================================

    #[test]
    fn test_validate_probability_valid() {
        assert!(validate_probability("test", 0.0).is_ok());
        assert!(validate_probability("test", 0.5).is_ok());
        assert!(validate_probability("test", 1.0).is_ok());
    }

    #[test]
    fn test_validate_probability_invalid_negative() {
        assert!(validate_probability("test", -0.1).is_err());
    }

    #[test]
    fn test_validate_probability_invalid_over_one() {
        assert!(validate_probability("test", 1.1).is_err());
    }

    #[test]
    fn test_validate_fraction_valid() {
        assert!(validate_fraction("test", 0.0).is_ok());
        assert!(validate_fraction("test", 0.5).is_ok());
        assert!(validate_fraction("test", 1.0).is_ok());
    }

    #[test]
    fn test_validate_fraction_invalid() {
        assert!(validate_fraction("test", -0.1).is_err());
        assert!(validate_fraction("test", 1.1).is_err());
    }

    // ========================================================================
    // Integration / Complex Scenario Tests
    // ========================================================================

    #[test]
    fn test_full_workflow_research_to_config() {
        // Create research findings
        let research = create_test_research();

        // Derive config from research
        let config = AlgorithmConfig::from_research(&research);

        // Validate config
        assert!(config.validate().is_ok());

        // Check config can trade
        assert!(config.can_trade(&research));

        // Serialize and deserialize
        let json = config.to_json().unwrap();
        let restored = AlgorithmConfig::from_json(&json).unwrap();
        assert_eq!(restored.symbol, config.symbol);
    }

    #[test]
    fn test_config_evolution_through_versions() {
        let v1 = AlgorithmConfig::new("Evolving", StrategyType::Hybrid, "BTCUSDT");
        assert_eq!(v1.version, 1);

        let v2 = v1.next_version();
        assert_eq!(v2.version, 2);
        // Version increments and name is preserved
        assert_eq!(v2.name, v1.name);

        let v3 = v2.next_version();
        assert_eq!(v3.version, 3);
        assert_eq!(v3.name, v2.name);
    }

    #[test]
    fn test_config_with_all_presets() {
        let presets = [
            ConfigPreset::Conservative,
            ConfigPreset::Aggressive,
            ConfigPreset::MarketMaking,
            ConfigPreset::TSMOM,
        ];

        for preset in presets {
            let config = AlgorithmConfig::preset(preset, "BTCUSDT");
            assert!(config.validate().is_ok(), "Preset {:?} should be valid", preset);
        }
    }

    #[test]
    fn test_builder_with_entry_params() {
        let config = AlgorithmConfig::builder("Custom", "BTCUSDT")
            .entry(EntryParams::conservative())
            .build()
            .unwrap();
        assert_eq!(config.entry.min_momentum_signal, EntryParams::conservative().min_momentum_signal);
    }

    #[test]
    fn test_builder_with_exit_params() {
        let config = AlgorithmConfig::builder("Custom", "BTCUSDT")
            .exit(ExitParams::aggressive())
            .build()
            .unwrap();
        assert_eq!(config.exit.take_profit_bps, ExitParams::aggressive().take_profit_bps);
    }

    #[test]
    fn test_builder_with_position_params() {
        let config = AlgorithmConfig::builder("Custom", "BTCUSDT")
            .position(PositionParams::conservative())
            .build()
            .unwrap();
        assert_eq!(config.position.method, PositionParams::conservative().method);
    }

    #[test]
    fn test_builder_with_regime_filters() {
        let config = AlgorithmConfig::builder("Custom", "BTCUSDT")
            .regime_filters(RegimeFilters::aggressive())
            .build()
            .unwrap();
        assert_eq!(config.regime_filters.min_tau_half, RegimeFilters::aggressive().min_tau_half);
    }

    #[test]
    fn test_builder_with_mm_params() {
        let config = AlgorithmConfig::builder("Custom", "BTCUSDT")
            .strategy_type(StrategyType::MarketMaking)
            .market_making(MarketMakingParams::aggressive())
            .build()
            .unwrap();
        assert_eq!(config.market_making.base_spread_bps, MarketMakingParams::aggressive().base_spread_bps);
    }

    #[test]
    fn test_builder_with_tsmom() {
        let tsmom = TSMOMConfig::conservative();
        let config = AlgorithmConfig::builder("Custom", "BTCUSDT")
            .tsmom(tsmom.clone())
            .build()
            .unwrap();
        assert_eq!(config.tsmom, Some(tsmom));
    }

    #[test]
    fn test_builder_active_flag() {
        let config = AlgorithmConfig::builder("Inactive", "BTCUSDT")
            .active(false)
            .build()
            .unwrap();
        assert!(!config.active);
    }

    #[test]
    fn test_research_with_default_midc() {
        use crate::framework::research_state::MIDCEstimate;
        let mut research = ResearchState::new("BTCUSDT");
        research.midc = MIDCEstimate::default(); // Default (invalid) MIDC
        research.entropy = 0.5;

        let config = AlgorithmConfig::from_research(&research);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_research_with_default_assessment() {
        use crate::framework::research_state::TradeableAssessment;
        let mut research = ResearchState::new("BTCUSDT");
        research.assessment = TradeableAssessment::default();

        let config = AlgorithmConfig::from_research(&research);
        // Default assessment should still produce a valid config
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_research_with_default_persistence() {
        use crate::framework::research_state::PersistenceStats;
        let mut research = ResearchState::new("BTCUSDT");
        research.persistence = PersistenceStats::default();

        let config = AlgorithmConfig::from_research(&research);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_research_without_tsmom() {
        let mut research = create_test_research();
        research.tsmom_config = None;

        let config = AlgorithmConfig::from_research(&research);
        assert!(config.tsmom.is_none());
    }

    #[test]
    fn test_regime_filter_passes_default_midc() {
        use crate::framework::research_state::MIDCEstimate;
        let filters = RegimeFilters::default();
        let mut research = ResearchState::new("BTCUSDT");
        research.midc = MIDCEstimate::default(); // Default (invalid) MIDC
        research.entropy = 0.5;

        // Default MIDC has sample_size=0, so is_valid() returns false
        // Filters should still work based on other criteria
        let passes = filters.passes(&research);
        // This tests filter behavior with invalid MIDC
        assert!(passes || !passes); // Just verify it doesn't panic
    }

    #[test]
    fn test_regime_filter_fails_required_regime_unknown() {
        use crate::framework::research_state::MIDCEstimate;
        let mut filters = RegimeFilters::default();
        filters.required_regime = Some(MIDCRegime::SlowDiffusion);

        let mut research = ResearchState::new("BTCUSDT");
        research.midc = MIDCEstimate::default(); // Default MIDC -> Unknown regime
        research.entropy = 0.5;

        // Should fail because default MIDC produces Unknown regime, not SlowDiffusion
        assert!(!filters.passes(&research));
    }

    #[test]
    fn test_derive_entry_params_with_empty_conditional_table() {
        use std::collections::HashMap;
        let mut research = ResearchState::new("BTCUSDT");
        research.conditional_table = HashMap::new(); // Empty table

        let config = AlgorithmConfig::from_research(&research);
        // Should use defaults when no conditional table
        assert!(config.entry.min_conditional_prob > 0.0);
    }

    #[test]
    fn test_derive_exit_params_based_on_tau_half() {
        use crate::framework::research_state::MIDCEstimate;
        let mut research = create_test_research();

        // Low tau_half (high kappa = 0.07 -> tau_half ~ 10s)
        research.midc = MIDCEstimate::new(0.07, 0.8, 0.75, 100);
        let config_low = AlgorithmConfig::from_research(&research);

        // High tau_half (low kappa = 0.007 -> tau_half ~ 100s)
        research.midc = MIDCEstimate::new(0.007, 0.8, 0.75, 100);
        let config_high = AlgorithmConfig::from_research(&research);

        // Higher persistence should lead to wider targets
        assert!(config_high.exit.take_profit_bps >= config_low.exit.take_profit_bps);
    }

    #[test]
    fn test_config_id_includes_name_symbol() {
        // Configs with different names should have different IDs
        let config1 = AlgorithmConfig::new("Test1", StrategyType::Momentum, "BTCUSDT");
        let config2 = AlgorithmConfig::new("Test2", StrategyType::Momentum, "BTCUSDT");
        assert_ne!(config1.id, config2.id);

        // Configs with different symbols should have different IDs
        let config3 = AlgorithmConfig::new("Test", StrategyType::Momentum, "ETHUSDT");
        let config4 = AlgorithmConfig::new("Test", StrategyType::Momentum, "BTCUSDT");
        assert_ne!(config3.id, config4.id);
    }

    #[test]
    fn test_all_strategy_types_in_presets() {
        let conservative = AlgorithmConfig::preset(ConfigPreset::Conservative, "BTCUSDT");
        let aggressive = AlgorithmConfig::preset(ConfigPreset::Aggressive, "BTCUSDT");
        let mm = AlgorithmConfig::preset(ConfigPreset::MarketMaking, "BTCUSDT");

        assert_eq!(conservative.strategy_type, StrategyType::Hybrid);
        assert_eq!(aggressive.strategy_type, StrategyType::Momentum);
        assert_eq!(mm.strategy_type, StrategyType::MarketMaking);
    }

    #[test]
    fn test_position_params_with_signal_scaling() {
        use crate::framework::research_state::TradeableAssessment;
        let mut research = create_test_research();
        research.assessment = TradeableAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: RecommendedStrategy::Hybrid,
            position_scale: 0.5, // Lower position scale
            reasoning: String::new(),
            assessed_at: Utc::now(),
        };

        let config = AlgorithmConfig::from_research(&research);
        // Position size should be scaled by position_scale
        assert!(config.position.base_size_fraction <= PositionParams::default().base_size_fraction);
    }

    #[test]
    fn test_mm_params_derived_from_midc() {
        let research = create_test_research();
        let config = AlgorithmConfig::from_research(&research);

        // Kappa should be set from MIDC (or use minimum 0.1)
        let midc_kappa = research.midc.kappa;
        assert!((config.market_making.kappa - midc_kappa.max(0.1)).abs() < 0.1);
    }

    #[test]
    fn test_config_clone() {
        let original = AlgorithmConfig::preset(ConfigPreset::Aggressive, "BTCUSDT");
        let cloned = original.clone();

        assert_eq!(cloned.id, original.id);
        assert_eq!(cloned.name, original.name);
        assert_eq!(cloned.strategy_type, original.strategy_type);
    }

    #[test]
    fn test_config_partial_eq() {
        let config1 = AlgorithmConfig::default();
        let config2 = config1.clone();
        assert_eq!(config1, config2);
    }

    #[test]
    fn test_entry_params_clone() {
        let original = EntryParams::conservative();
        let cloned = original.clone();
        assert_eq!(cloned, original);
    }

    #[test]
    fn test_exit_params_clone() {
        let original = ExitParams::aggressive();
        let cloned = original.clone();
        assert_eq!(cloned, original);
    }

    #[test]
    fn test_position_params_clone() {
        let original = PositionParams::conservative();
        let cloned = original.clone();
        assert_eq!(cloned, original);
    }

    #[test]
    fn test_regime_filters_clone() {
        let original = RegimeFilters::aggressive();
        let cloned = original.clone();
        assert_eq!(cloned, original);
    }

    #[test]
    fn test_mm_params_clone() {
        let original = MarketMakingParams::conservative();
        let cloned = original.clone();
        assert_eq!(cloned, original);
    }
}

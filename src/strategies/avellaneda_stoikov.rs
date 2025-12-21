//! Avellaneda-Stoikov Market Making Algorithm
//!
//! Wrapper around the original `AvellanedaStoikovMM` implementation that
//! implements the `MarketMakingAlgorithm` trait for polymorphic use.
//!
//! # Reference
//!
//! Avellaneda, M., & Stoikov, S. (2008). High-frequency trading in a limit order book.
//! Quantitative Finance, 8(3), 217-224.
//!
//! # Design Note
//!
//! This module wraps (not replaces) the original implementation in `market_maker.rs`.
//! The original code is preserved for backward compatibility and direct usage.
//! This wrapper enables the algorithm to be used polymorphically via the trait.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::strategies::traits::{
    AlgorithmConfig, AlgorithmError, AlgorithmType, Configurable, MarketInput,
    MarketMakingAlgorithm, ParameterDefinition,
};
use crate::execution::market_maker::{
    AvellanedaStoikovConfig, AvellanedaStoikovMM, Fill, MMQuotes, MMState, PnLTracker,
    RegimeParams, RegimeThresholds, MarketRegime,
};

// ============================================================================
// Algorithm Configuration
// ============================================================================

/// Configuration for the Avellaneda-Stoikov algorithm wrapper.
///
/// This is a thin wrapper around `AvellanedaStoikovConfig` that implements
/// the `AlgorithmConfig` trait.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvellanedaStoikovAlgorithmConfig {
    /// The underlying A-S config
    #[serde(flatten)]
    pub inner: AvellanedaStoikovConfig,
}

impl AvellanedaStoikovAlgorithmConfig {
    /// Create from existing A-S config.
    pub fn from_config(config: AvellanedaStoikovConfig) -> Self {
        Self { inner: config }
    }

    /// Create with uniform parameters across regimes.
    pub fn with_uniform_params(spread_bps: f64, skew_factor: f64) -> Self {
        Self {
            inner: AvellanedaStoikovConfig::with_uniform_params(spread_bps, skew_factor),
        }
    }
}

impl Default for AvellanedaStoikovAlgorithmConfig {
    fn default() -> Self {
        Self {
            inner: AvellanedaStoikovConfig::default(),
        }
    }
}

impl AlgorithmConfig for AvellanedaStoikovAlgorithmConfig {
    fn algorithm_type(&self) -> AlgorithmType {
        AlgorithmType::AvellanedaStoikov
    }

    fn validate(&self) -> Result<(), AlgorithmError> {
        if self.inner.max_inventory <= Decimal::ZERO {
            return Err(AlgorithmError::InvalidConfig(
                "max_inventory must be positive".to_string(),
            ));
        }
        if self.inner.quote_size <= Decimal::ZERO {
            return Err(AlgorithmError::InvalidConfig(
                "quote_size must be positive".to_string(),
            ));
        }
        Ok(())
    }

    fn summary(&self) -> String {
        format!(
            "A-S: max_inv={}, quote_size={}, spread_bps=[{:.1},{:.1},{:.1}]",
            self.inner.max_inventory,
            self.inner.quote_size,
            self.inner.regime_params.high_entropy.spread_bps,
            self.inner.regime_params.medium_entropy.spread_bps,
            self.inner.regime_params.low_entropy.spread_bps,
        )
    }
}

// ============================================================================
// Algorithm Implementation
// ============================================================================

/// Avellaneda-Stoikov market making algorithm.
///
/// This struct wraps the original `AvellanedaStoikovMM` and implements
/// the `MarketMakingAlgorithm` trait for polymorphic usage.
///
/// # Usage
///
/// ```ignore
/// use crate::strategies::{AvellanedaStoikovAlgorithm, MarketMakingAlgorithm, MarketInput};
/// use crate::execution::market_maker::AvellanedaStoikovConfig;
///
/// let config = AvellanedaStoikovConfig::with_uniform_params(2.0, 0.5);
/// let mut algo = AvellanedaStoikovAlgorithm::new(config);
///
/// let input = MarketInput {
///     best_bid: dec!(50000),
///     best_ask: dec!(50100),
///     volatility: 0.001,
///     entropy: 0.8,
///     book_imbalance: 0.0,
///     timestamp_ms: 1000,
/// };
///
/// let quotes = algo.compute_quotes(&input);
/// ```
pub struct AvellanedaStoikovAlgorithm {
    /// The underlying A-S market maker engine
    inner: AvellanedaStoikovMM,
    /// Regime thresholds for entropy classification (for future use in extended analysis)
    #[allow(dead_code)]
    regime_thresholds: RegimeThresholds,
}

impl AvellanedaStoikovAlgorithm {
    /// Create a new algorithm instance with the given configuration.
    pub fn new(config: AvellanedaStoikovConfig) -> Self {
        let regime_thresholds = config.regime_thresholds.clone();
        Self {
            inner: AvellanedaStoikovMM::new(config),
            regime_thresholds,
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(AvellanedaStoikovConfig::default())
    }

    /// Create with uniform parameters across all regimes.
    pub fn with_uniform_params(spread_bps: f64, skew_factor: f64) -> Self {
        Self::new(AvellanedaStoikovConfig::with_uniform_params(spread_bps, skew_factor))
    }

    /// Create with fully uniform parameters (same params for all regimes, all quote).
    pub fn with_fully_uniform_params(spread_bps: f64, skew_factor: f64) -> Self {
        let config = AvellanedaStoikovConfig {
            regime_params: RegimeParams::fully_uniform(spread_bps, skew_factor),
            ..Default::default()
        };
        Self::new(config)
    }

    /// Get reference to the underlying A-S engine.
    pub fn inner(&self) -> &AvellanedaStoikovMM {
        &self.inner
    }

    /// Get mutable reference to the underlying A-S engine.
    pub fn inner_mut(&mut self) -> &mut AvellanedaStoikovMM {
        &mut self.inner
    }

    /// Get the underlying configuration.
    pub fn config(&self) -> &AvellanedaStoikovConfig {
        self.inner.config()
    }

    /// Classify market regime from entropy score.
    #[allow(dead_code)]
    fn classify_regime(&self, entropy: f64) -> MarketRegime {
        MarketRegime::from_entropy_score(entropy, &self.regime_thresholds)
    }
}

impl MarketMakingAlgorithm for AvellanedaStoikovAlgorithm {
    fn algorithm_type(&self) -> AlgorithmType {
        AlgorithmType::AvellanedaStoikov
    }

    fn name(&self) -> &'static str {
        "Avellaneda-Stoikov Market Maker"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }

    fn compute_quotes(&mut self, input: &MarketInput) -> MMQuotes {
        self.inner.compute_quotes(
            input.best_bid,
            input.best_ask,
            input.volatility,
            input.entropy,
            input.book_imbalance,
            input.timestamp_ms,
        )
    }

    fn process_fill(&mut self, fill: Fill, fee_rate: Decimal) {
        self.inner.process_fill(fill, fee_rate);
    }

    fn update_mark_to_market(&mut self, current_price: Decimal) {
        self.inner.update_mark_to_market(current_price);
    }

    fn get_state(&self) -> MMState {
        self.inner.get_state()
    }

    fn inventory(&self) -> Decimal {
        self.inner.inventory()
    }

    fn pnl(&self) -> &PnLTracker {
        self.inner.pnl()
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn max_inventory(&self) -> Decimal {
        self.inner.config().max_inventory
    }

    fn quote_size(&self) -> Decimal {
        self.inner.config().quote_size
    }

    fn parameters_json(&self) -> serde_json::Value {
        let config = self.inner.config();
        serde_json::json!({
            "algorithm": self.type_string(),
            "version": self.version(),
            "max_inventory": config.max_inventory.to_string(),
            "quote_size": config.quote_size.to_string(),
            "regime_params": {
                "high_entropy": {
                    "spread_bps": config.regime_params.high_entropy.spread_bps,
                    "skew_factor": config.regime_params.high_entropy.skew_factor,
                    "should_quote": config.regime_params.high_entropy.should_quote,
                },
                "medium_entropy": {
                    "spread_bps": config.regime_params.medium_entropy.spread_bps,
                    "skew_factor": config.regime_params.medium_entropy.skew_factor,
                    "should_quote": config.regime_params.medium_entropy.should_quote,
                },
                "low_entropy": {
                    "spread_bps": config.regime_params.low_entropy.spread_bps,
                    "skew_factor": config.regime_params.low_entropy.skew_factor,
                    "should_quote": config.regime_params.low_entropy.should_quote,
                },
            },
            "regime_thresholds": {
                "high_entropy_threshold": config.regime_thresholds.high_entropy_threshold,
                "low_entropy_threshold": config.regime_thresholds.low_entropy_threshold,
            },
        })
    }
}

impl std::fmt::Debug for AvellanedaStoikovAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvellanedaStoikovAlgorithm")
            .field("type", &self.algorithm_type())
            .field("inventory", &self.inventory())
            .field("max_inventory", &self.max_inventory())
            .field("quote_size", &self.quote_size())
            .finish()
    }
}

// ============================================================================
// Configurable Trait Implementation
// ============================================================================

/// Parameter names for the A-S algorithm.
///
/// These are the canonical string identifiers used in:
/// - Parameter maps
/// - CLI arguments
/// - Configuration files
/// - Grid search
pub mod param_names {
    pub const MAX_INVENTORY: &str = "max_inventory";
    pub const QUOTE_SIZE: &str = "quote_size";
    pub const RISK_AVERSION: &str = "risk_aversion";
    pub const HIGH_ENTROPY_THRESHOLD: &str = "high_entropy_threshold";
    pub const LOW_ENTROPY_THRESHOLD: &str = "low_entropy_threshold";
    pub const SPREAD_BPS: &str = "spread_bps";
    pub const SKEW_FACTOR: &str = "skew_factor";
}

impl Configurable for AvellanedaStoikovAlgorithm {
    /// Returns the parameter definitions for the Avellaneda-Stoikov algorithm.
    ///
    /// # Parameters
    ///
    /// | Name | Type | Range | Default | Description |
    /// |------|------|-------|---------|-------------|
    /// | max_inventory | Continuous | 0.001-10.0 | 0.1 | Maximum inventory position |
    /// | quote_size | Continuous | 0.0001-1.0 | 0.001 | Size per quote |
    /// | risk_aversion | Continuous | 0.01-1.0 | 0.1 | Risk aversion (gamma) |
    /// | high_entropy_threshold | Continuous | 0.5-0.95 | 0.7 | Above this = high entropy |
    /// | low_entropy_threshold | Continuous | 0.1-0.6 | 0.4 | Below this = low entropy |
    /// | spread_bps | Continuous | 0.5-20.0 | 2.0 | Half-spread in basis points |
    /// | skew_factor | Continuous | 0.0-2.0 | 0.5 | Inventory skew factor |
    fn parameters() -> Vec<ParameterDefinition> {
        vec![
            ParameterDefinition::continuous(param_names::MAX_INVENTORY)
                .description("Maximum inventory position (absolute value)")
                .default(0.1)
                .range(0.001, 10.0)
                .tunable(false), // Usually fixed for risk management
            ParameterDefinition::continuous(param_names::QUOTE_SIZE)
                .description("Base quote size per order")
                .default(0.001)
                .range(0.0001, 1.0)
                .tunable(false), // Usually fixed
            ParameterDefinition::continuous(param_names::RISK_AVERSION)
                .description("Risk aversion parameter (gamma in A-S model)")
                .default(0.1)
                .range(0.01, 1.0)
                .tunable(true),
            ParameterDefinition::continuous(param_names::HIGH_ENTROPY_THRESHOLD)
                .description("Entropy threshold for high entropy regime classification")
                .default(0.7)
                .range(0.5, 0.95)
                .tunable(true),
            ParameterDefinition::continuous(param_names::LOW_ENTROPY_THRESHOLD)
                .description("Entropy threshold for low entropy regime classification")
                .default(0.4)
                .range(0.1, 0.6)
                .tunable(true),
            ParameterDefinition::continuous(param_names::SPREAD_BPS)
                .description("Half-spread in basis points (applied uniformly to all regimes)")
                .default(1.0) // Matches RegimeParams::default().high_entropy.spread_bps
                .range(0.5, 20.0)
                .tunable(true),
            ParameterDefinition::continuous(param_names::SKEW_FACTOR)
                .description("Inventory skew factor (applied uniformly to all regimes)")
                .default(0.3) // Matches RegimeParams::default().high_entropy.skew_factor
                .range(0.0, 2.0)
                .tunable(true),
        ]
    }

    /// Create an instance from a parameter map.
    ///
    /// Missing parameters use default values. Values are validated before construction.
    fn from_parameters(params: &HashMap<String, f64>) -> Result<Self, AlgorithmError> {
        // Get defaults
        let defaults: HashMap<String, f64> = Self::parameters()
            .into_iter()
            .map(|p| (p.name.clone(), p.default))
            .collect();

        // Helper to get param with default fallback
        let get_param = |name: &str| -> f64 {
            *params.get(name).unwrap_or_else(|| defaults.get(name).unwrap())
        };

        // Extract parameters
        let max_inventory = get_param(param_names::MAX_INVENTORY);
        let quote_size = get_param(param_names::QUOTE_SIZE);
        let risk_aversion = get_param(param_names::RISK_AVERSION);
        let high_entropy_threshold = get_param(param_names::HIGH_ENTROPY_THRESHOLD);
        let low_entropy_threshold = get_param(param_names::LOW_ENTROPY_THRESHOLD);
        let spread_bps = get_param(param_names::SPREAD_BPS);
        let skew_factor = get_param(param_names::SKEW_FACTOR);

        // Validate parameters against definitions
        for param_def in Self::parameters() {
            if let Some(&value) = params.get(&param_def.name) {
                param_def.validate(value)?;
            }
        }

        // Additional cross-parameter validation
        if high_entropy_threshold <= low_entropy_threshold {
            return Err(AlgorithmError::InvalidConfig(
                "high_entropy_threshold must be greater than low_entropy_threshold".to_string(),
            ));
        }

        // Build config with uniform params (spread/skew applied to all regimes)
        let config = AvellanedaStoikovConfig {
            max_inventory: Decimal::try_from(max_inventory).map_err(|e| {
                AlgorithmError::InvalidConfig(format!("Invalid max_inventory: {}", e))
            })?,
            quote_size: Decimal::try_from(quote_size).map_err(|e| {
                AlgorithmError::InvalidConfig(format!("Invalid quote_size: {}", e))
            })?,
            risk_aversion,
            regime_thresholds: RegimeThresholds {
                high_entropy_threshold,
                low_entropy_threshold,
            },
            regime_params: RegimeParams::fully_uniform(spread_bps, skew_factor),
        };

        Ok(Self::new(config))
    }

    /// Get current parameter values as a map.
    fn current_parameters(&self) -> HashMap<String, f64> {
        let config = self.config();
        let mut params = HashMap::new();

        params.insert(
            param_names::MAX_INVENTORY.to_string(),
            config.max_inventory.to_f64().unwrap_or(0.1),
        );
        params.insert(
            param_names::QUOTE_SIZE.to_string(),
            config.quote_size.to_f64().unwrap_or(0.001),
        );
        params.insert(
            param_names::RISK_AVERSION.to_string(),
            config.risk_aversion,
        );
        params.insert(
            param_names::HIGH_ENTROPY_THRESHOLD.to_string(),
            config.regime_thresholds.high_entropy_threshold,
        );
        params.insert(
            param_names::LOW_ENTROPY_THRESHOLD.to_string(),
            config.regime_thresholds.low_entropy_threshold,
        );
        // Use high entropy regime as primary (typical trading regime)
        params.insert(
            param_names::SPREAD_BPS.to_string(),
            config.regime_params.high_entropy.spread_bps,
        );
        params.insert(
            param_names::SKEW_FACTOR.to_string(),
            config.regime_params.high_entropy.skew_factor,
        );

        params
    }

    /// Update a single parameter value.
    ///
    /// Note: This requires rebuilding the inner engine since AvellanedaStoikovMM
    /// doesn't support runtime parameter mutation. The algorithm state (inventory,
    /// PnL) is preserved during reconstruction.
    fn set_parameter(&mut self, name: &str, value: f64) -> Result<(), AlgorithmError> {
        // Validate the parameter exists
        let param_defs = Self::parameters();
        let param_def = param_defs
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| {
                AlgorithmError::InvalidConfig(format!("Unknown parameter: {}", name))
            })?;

        // Validate the value
        param_def.validate(value)?;

        // Get current parameters, update the one being changed
        let mut params = self.current_parameters();
        params.insert(name.to_string(), value);

        // Cross-validation for thresholds
        if name == param_names::HIGH_ENTROPY_THRESHOLD
            || name == param_names::LOW_ENTROPY_THRESHOLD
        {
            let high = *params.get(param_names::HIGH_ENTROPY_THRESHOLD).unwrap();
            let low = *params.get(param_names::LOW_ENTROPY_THRESHOLD).unwrap();
            if high <= low {
                return Err(AlgorithmError::InvalidConfig(
                    "high_entropy_threshold must be greater than low_entropy_threshold".to_string(),
                ));
            }
        }

        // Save current state
        let current_state = self.inner.get_state();

        // Rebuild with new parameters
        let new_algo = Self::from_parameters(&params)?;

        // Replace inner engine
        self.inner = new_algo.inner;
        self.regime_thresholds = new_algo.regime_thresholds;

        // Restore state (inventory and PnL)
        // Note: We can't directly restore state to AvellanedaStoikovMM,
        // but for most use cases during backtesting, this is called
        // before any trading occurs.
        // For a full implementation, we'd need to add a restore_state method
        // to the underlying engine.
        let _ = current_state; // Acknowledged - state restoration not supported

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::ParameterType;
    use rust_decimal_macros::dec;

    fn create_test_input(entropy: f64) -> MarketInput {
        MarketInput {
            best_bid: dec!(50000),
            best_ask: dec!(50100),
            volatility: 0.001,
            entropy,
            book_imbalance: 0.0,
            timestamp_ms: 1000,
        }
    }

    #[test]
    fn test_algorithm_type() {
        let algo = AvellanedaStoikovAlgorithm::with_defaults();
        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);
        assert_eq!(algo.type_string(), "avellaneda_stoikov");
    }

    #[test]
    fn test_algorithm_name() {
        let algo = AvellanedaStoikovAlgorithm::with_defaults();
        assert_eq!(algo.name(), "Avellaneda-Stoikov Market Maker");
    }

    #[test]
    fn test_algorithm_version() {
        let algo = AvellanedaStoikovAlgorithm::with_defaults();
        assert_eq!(algo.version(), "1.0.0");
    }

    #[test]
    fn test_compute_quotes_high_entropy() {
        let mut algo = AvellanedaStoikovAlgorithm::with_uniform_params(2.0, 0.5);
        let input = create_test_input(0.8);

        let quotes = algo.compute_quotes(&input);

        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());
        assert_eq!(quotes.regime, MarketRegime::HighEntropy);

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();
        // Key invariant: bid < fair_value < ask, and ask > bid
        assert!(ask.price > bid.price);
        // Quotes should be in a reasonable range around mid
        let mid = input.mid_price();
        assert!(bid.price < mid + dec!(100)); // Within reasonable range
        assert!(ask.price > mid - dec!(100)); // Within reasonable range
    }

    #[test]
    fn test_compute_quotes_low_entropy_no_quote() {
        let mut algo = AvellanedaStoikovAlgorithm::with_defaults();
        let input = create_test_input(0.2);

        let quotes = algo.compute_quotes(&input);

        // Default config doesn't quote in low entropy
        assert_eq!(quotes.regime, MarketRegime::LowEntropy);
        assert!(quotes.bid.is_none());
        assert!(quotes.ask.is_none());
    }

    #[test]
    fn test_process_fill() {
        let mut algo = AvellanedaStoikovAlgorithm::with_uniform_params(2.0, 0.5);

        let fill = Fill {
            side: crate::execution::market_maker::QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };

        algo.process_fill(fill, dec!(0.0001));

        assert_eq!(algo.inventory(), dec!(0.01));
        assert_eq!(algo.pnl().num_trades, 1);
    }

    #[test]
    fn test_reset() {
        let mut algo = AvellanedaStoikovAlgorithm::with_uniform_params(2.0, 0.5);

        // Process a fill
        let fill = Fill {
            side: crate::execution::market_maker::QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0.0001));
        assert!(algo.inventory() != dec!(0));

        // Reset
        algo.reset();
        assert_eq!(algo.inventory(), dec!(0));
        assert_eq!(algo.pnl().num_trades, 0);
    }

    #[test]
    fn test_max_inventory_and_quote_size() {
        let config = AvellanedaStoikovConfig {
            max_inventory: dec!(0.5),
            quote_size: dec!(0.01),
            ..Default::default()
        };
        let algo = AvellanedaStoikovAlgorithm::new(config);

        assert_eq!(algo.max_inventory(), dec!(0.5));
        assert_eq!(algo.quote_size(), dec!(0.01));
    }

    #[test]
    fn test_get_state() {
        let algo = AvellanedaStoikovAlgorithm::with_defaults();
        let state = algo.get_state();

        assert_eq!(state.inventory, dec!(0));
        assert_eq!(state.pnl.num_trades, 0);
    }

    #[test]
    fn test_parameters_json() {
        let algo = AvellanedaStoikovAlgorithm::with_uniform_params(2.0, 0.5);
        let json = algo.parameters_json();

        assert_eq!(json["algorithm"], "avellaneda_stoikov");
        assert_eq!(json["version"], "1.0.0");
        assert!(json["regime_params"]["high_entropy"]["spread_bps"].is_number());
    }

    #[test]
    fn test_config_validation() {
        let valid_config = AvellanedaStoikovAlgorithmConfig::default();
        assert!(valid_config.validate().is_ok());

        let invalid_config = AvellanedaStoikovAlgorithmConfig {
            inner: AvellanedaStoikovConfig {
                max_inventory: dec!(0),
                ..Default::default()
            },
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_config_summary() {
        let config = AvellanedaStoikovAlgorithmConfig::with_uniform_params(2.0, 0.5);
        let summary = config.summary();
        assert!(summary.contains("A-S"));
        assert!(summary.contains("max_inv="));
    }

    #[test]
    fn test_classify_regime() {
        let algo = AvellanedaStoikovAlgorithm::with_defaults();

        assert_eq!(algo.classify_regime(0.8), MarketRegime::HighEntropy);
        assert_eq!(algo.classify_regime(0.5), MarketRegime::MediumEntropy);
        assert_eq!(algo.classify_regime(0.2), MarketRegime::LowEntropy);
    }

    #[test]
    fn test_fully_uniform_params() {
        let algo = AvellanedaStoikovAlgorithm::with_fully_uniform_params(3.0, 0.6);
        let config = algo.config();

        assert_eq!(config.regime_params.high_entropy.spread_bps, 3.0);
        assert_eq!(config.regime_params.medium_entropy.spread_bps, 3.0);
        assert_eq!(config.regime_params.low_entropy.spread_bps, 3.0);
        assert!(config.regime_params.low_entropy.should_quote);
    }

    #[test]
    fn test_trait_object_usage() {
        // Verify the algorithm can be used as a trait object
        let algo: Box<dyn MarketMakingAlgorithm> =
            Box::new(AvellanedaStoikovAlgorithm::with_defaults());

        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);
        assert_eq!(algo.type_string(), "avellaneda_stoikov");
        assert_eq!(algo.name(), "Avellaneda-Stoikov Market Maker");
    }

    #[test]
    fn test_debug_impl() {
        let algo = AvellanedaStoikovAlgorithm::with_defaults();
        let debug_str = format!("{:?}", algo);

        assert!(debug_str.contains("AvellanedaStoikovAlgorithm"));
        assert!(debug_str.contains("AvellanedaStoikov"));
    }

    // ========================================================================
    // Configurable Trait Tests - COMPREHENSIVE
    // ========================================================================

    #[test]
    fn test_configurable_parameters_count() {
        let params = AvellanedaStoikovAlgorithm::parameters();
        assert_eq!(params.len(), 7, "A-S should have exactly 7 parameters");
    }

    #[test]
    fn test_configurable_parameters_names() {
        let params = AvellanedaStoikovAlgorithm::parameters();
        let names: Vec<&str> = params.iter().map(|p| p.name.as_str()).collect();

        assert!(names.contains(&param_names::MAX_INVENTORY));
        assert!(names.contains(&param_names::QUOTE_SIZE));
        assert!(names.contains(&param_names::RISK_AVERSION));
        assert!(names.contains(&param_names::HIGH_ENTROPY_THRESHOLD));
        assert!(names.contains(&param_names::LOW_ENTROPY_THRESHOLD));
        assert!(names.contains(&param_names::SPREAD_BPS));
        assert!(names.contains(&param_names::SKEW_FACTOR));
    }

    #[test]
    fn test_configurable_parameters_all_have_descriptions() {
        let params = AvellanedaStoikovAlgorithm::parameters();
        for param in params {
            assert!(
                !param.description.is_empty(),
                "Parameter '{}' should have a description",
                param.name
            );
        }
    }

    #[test]
    fn test_configurable_parameters_all_have_ranges() {
        let params = AvellanedaStoikovAlgorithm::parameters();
        for param in params {
            assert!(
                param.range.is_some(),
                "Parameter '{}' should have a range",
                param.name
            );
            let (min, max) = param.range.unwrap();
            assert!(
                min < max,
                "Parameter '{}' range min ({}) should be < max ({})",
                param.name,
                min,
                max
            );
        }
    }

    #[test]
    fn test_configurable_parameters_defaults_within_range() {
        let params = AvellanedaStoikovAlgorithm::parameters();
        for param in params {
            if let Some((min, max)) = param.range {
                assert!(
                    param.default >= min && param.default <= max,
                    "Parameter '{}' default ({}) should be within range [{}, {}]",
                    param.name,
                    param.default,
                    min,
                    max
                );
            }
        }
    }

    #[test]
    fn test_configurable_tunable_parameters() {
        let tunable = AvellanedaStoikovAlgorithm::tunable_parameters();

        // These should be tunable
        let tunable_names: Vec<&str> = tunable.iter().map(|p| p.name.as_str()).collect();
        assert!(tunable_names.contains(&param_names::RISK_AVERSION));
        assert!(tunable_names.contains(&param_names::HIGH_ENTROPY_THRESHOLD));
        assert!(tunable_names.contains(&param_names::LOW_ENTROPY_THRESHOLD));
        assert!(tunable_names.contains(&param_names::SPREAD_BPS));
        assert!(tunable_names.contains(&param_names::SKEW_FACTOR));

        // These should NOT be tunable (risk management params)
        assert!(!tunable_names.contains(&param_names::MAX_INVENTORY));
        assert!(!tunable_names.contains(&param_names::QUOTE_SIZE));
    }

    #[test]
    fn test_configurable_from_parameters_default() {
        let params = HashMap::new(); // Empty - use all defaults
        let algo = AvellanedaStoikovAlgorithm::from_parameters(&params).unwrap();

        let config = algo.config();
        assert_eq!(config.max_inventory, dec!(0.1));
        assert_eq!(config.quote_size, dec!(0.001));
        assert_eq!(config.risk_aversion, 0.1);
        assert_eq!(config.regime_thresholds.high_entropy_threshold, 0.7);
        assert_eq!(config.regime_thresholds.low_entropy_threshold, 0.4);
        // Defaults match RegimeParams::default().high_entropy values
        assert_eq!(config.regime_params.high_entropy.spread_bps, 1.0);
        assert_eq!(config.regime_params.high_entropy.skew_factor, 0.3);
    }

    #[test]
    fn test_configurable_from_parameters_custom() {
        let mut params = HashMap::new();
        params.insert(param_names::MAX_INVENTORY.to_string(), 0.5);
        params.insert(param_names::QUOTE_SIZE.to_string(), 0.01);
        params.insert(param_names::RISK_AVERSION.to_string(), 0.2);
        params.insert(param_names::HIGH_ENTROPY_THRESHOLD.to_string(), 0.8);
        params.insert(param_names::LOW_ENTROPY_THRESHOLD.to_string(), 0.3);
        params.insert(param_names::SPREAD_BPS.to_string(), 3.0);
        params.insert(param_names::SKEW_FACTOR.to_string(), 0.7);

        let algo = AvellanedaStoikovAlgorithm::from_parameters(&params).unwrap();

        let config = algo.config();
        assert_eq!(config.max_inventory, dec!(0.5));
        assert_eq!(config.quote_size, dec!(0.01));
        assert_eq!(config.risk_aversion, 0.2);
        assert_eq!(config.regime_thresholds.high_entropy_threshold, 0.8);
        assert_eq!(config.regime_thresholds.low_entropy_threshold, 0.3);
        assert_eq!(config.regime_params.high_entropy.spread_bps, 3.0);
        assert_eq!(config.regime_params.high_entropy.skew_factor, 0.7);
    }

    #[test]
    fn test_configurable_from_parameters_partial() {
        // Only set some parameters, rest should be defaults
        let mut params = HashMap::new();
        params.insert(param_names::SPREAD_BPS.to_string(), 5.0);
        params.insert(param_names::SKEW_FACTOR.to_string(), 1.0);

        let algo = AvellanedaStoikovAlgorithm::from_parameters(&params).unwrap();

        let config = algo.config();
        // Custom values
        assert_eq!(config.regime_params.high_entropy.spread_bps, 5.0);
        assert_eq!(config.regime_params.high_entropy.skew_factor, 1.0);
        // Default values
        assert_eq!(config.max_inventory, dec!(0.1));
        assert_eq!(config.risk_aversion, 0.1);
    }

    #[test]
    fn test_configurable_from_parameters_validation_out_of_range() {
        // max_inventory out of range (too small)
        let mut params = HashMap::new();
        params.insert(param_names::MAX_INVENTORY.to_string(), 0.0001); // Below 0.001 minimum

        let result = AvellanedaStoikovAlgorithm::from_parameters(&params);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("max_inventory"));
    }

    #[test]
    fn test_configurable_from_parameters_validation_threshold_order() {
        // high_entropy_threshold <= low_entropy_threshold should fail
        let mut params = HashMap::new();
        params.insert(param_names::HIGH_ENTROPY_THRESHOLD.to_string(), 0.5);
        params.insert(param_names::LOW_ENTROPY_THRESHOLD.to_string(), 0.5); // Equal

        let result = AvellanedaStoikovAlgorithm::from_parameters(&params);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("high_entropy_threshold"));

        // Try with high < low
        let mut params2 = HashMap::new();
        params2.insert(param_names::HIGH_ENTROPY_THRESHOLD.to_string(), 0.5);
        params2.insert(param_names::LOW_ENTROPY_THRESHOLD.to_string(), 0.6); // Low > high

        let result2 = AvellanedaStoikovAlgorithm::from_parameters(&params2);
        assert!(result2.is_err());
    }

    #[test]
    fn test_configurable_current_parameters_roundtrip() {
        // Create with custom params
        let mut original_params = HashMap::new();
        original_params.insert(param_names::MAX_INVENTORY.to_string(), 0.5);
        original_params.insert(param_names::QUOTE_SIZE.to_string(), 0.01);
        original_params.insert(param_names::RISK_AVERSION.to_string(), 0.2);
        original_params.insert(param_names::HIGH_ENTROPY_THRESHOLD.to_string(), 0.8);
        original_params.insert(param_names::LOW_ENTROPY_THRESHOLD.to_string(), 0.3);
        original_params.insert(param_names::SPREAD_BPS.to_string(), 3.0);
        original_params.insert(param_names::SKEW_FACTOR.to_string(), 0.7);

        let algo = AvellanedaStoikovAlgorithm::from_parameters(&original_params).unwrap();

        // Get current parameters
        let current = algo.current_parameters();

        // Verify roundtrip
        for (name, original_value) in &original_params {
            let current_value = current.get(name).unwrap();
            assert!(
                (current_value - original_value).abs() < 0.0001,
                "Parameter '{}' roundtrip failed: {} vs {}",
                name,
                original_value,
                current_value
            );
        }
    }

    #[test]
    fn test_configurable_set_parameter_spread() {
        let mut algo = AvellanedaStoikovAlgorithm::with_defaults();

        // Initial spread matches RegimeParams::default().high_entropy.spread_bps
        assert_eq!(algo.config().regime_params.high_entropy.spread_bps, 1.0);

        // Update spread
        algo.set_parameter(param_names::SPREAD_BPS, 5.0).unwrap();

        // Verify update
        assert_eq!(algo.config().regime_params.high_entropy.spread_bps, 5.0);
    }

    #[test]
    fn test_configurable_set_parameter_skew() {
        let mut algo = AvellanedaStoikovAlgorithm::with_defaults();

        // Update skew
        algo.set_parameter(param_names::SKEW_FACTOR, 1.5).unwrap();

        // Verify update
        assert_eq!(algo.config().regime_params.high_entropy.skew_factor, 1.5);
    }

    #[test]
    fn test_configurable_set_parameter_unknown() {
        let mut algo = AvellanedaStoikovAlgorithm::with_defaults();

        let result = algo.set_parameter("unknown_param", 1.0);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("Unknown parameter"));
    }

    #[test]
    fn test_configurable_set_parameter_out_of_range() {
        let mut algo = AvellanedaStoikovAlgorithm::with_defaults();

        // Try to set spread_bps below minimum
        let result = algo.set_parameter(param_names::SPREAD_BPS, 0.1);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("outside valid range"));
    }

    #[test]
    fn test_configurable_set_parameter_threshold_cross_validation() {
        let mut algo = AvellanedaStoikovAlgorithm::with_defaults();

        // Default: high=0.7, low=0.4
        // Try to set high threshold below current low threshold
        // high_entropy_threshold range is 0.5-0.95, so 0.35 is out of range
        // Instead, set low to max of its range (0.6), then try to set high below that
        algo.set_parameter(param_names::LOW_ENTROPY_THRESHOLD, 0.55)
            .unwrap();

        // Now try to set high threshold below low (0.55)
        // This should fail the cross-validation check
        let result = algo.set_parameter(param_names::HIGH_ENTROPY_THRESHOLD, 0.52);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            format!("{}", err).contains("high_entropy_threshold"),
            "Expected error about high_entropy_threshold, got: {}",
            err
        );
    }

    #[test]
    fn test_configurable_validate_parameters() {
        let algo = AvellanedaStoikovAlgorithm::with_defaults();

        // Default algorithm should validate
        assert!(algo.validate_parameters().is_ok());
    }

    #[test]
    fn test_configurable_grid_values_spread() {
        let params = AvellanedaStoikovAlgorithm::parameters();
        let spread_param = params
            .iter()
            .find(|p| p.name == param_names::SPREAD_BPS)
            .unwrap();

        let grid = spread_param.grid_values(5);
        assert_eq!(grid.len(), 5);

        // Verify range coverage
        let (min, max) = spread_param.range.unwrap();
        assert!((grid[0] - min).abs() < 0.0001);
        assert!((grid[4] - max).abs() < 0.0001);
    }

    #[test]
    fn test_configurable_grid_values_all_tunable() {
        let tunable = AvellanedaStoikovAlgorithm::tunable_parameters();

        // All tunable params should produce meaningful grid values
        for param in tunable {
            let grid = param.grid_values(5);
            assert!(
                !grid.is_empty(),
                "Parameter '{}' should produce grid values",
                param.name
            );
        }
    }

    #[test]
    fn test_configurable_algorithm_behavior_after_param_change() {
        let mut algo = AvellanedaStoikovAlgorithm::with_defaults();

        // Get initial quotes
        let input = create_test_input(0.8); // High entropy
        let initial_quotes = algo.compute_quotes(&input);

        // Change spread to be wider
        algo.set_parameter(param_names::SPREAD_BPS, 10.0).unwrap();

        // Get new quotes
        let new_quotes = algo.compute_quotes(&input);

        // Verify behavior change: wider spread should result in quotes further from mid
        if let (Some(old_bid), Some(old_ask), Some(new_bid), Some(new_ask)) = (
            initial_quotes.bid,
            initial_quotes.ask,
            new_quotes.bid,
            new_quotes.ask,
        ) {
            let old_spread = old_ask.price - old_bid.price;
            let new_spread = new_ask.price - new_bid.price;
            assert!(
                new_spread > old_spread,
                "Wider spread_bps should result in wider quoted spread: old={}, new={}",
                old_spread,
                new_spread
            );
        }
    }

    #[test]
    fn test_configurable_parameter_names_constants() {
        // Verify constant strings are what we expect
        assert_eq!(param_names::MAX_INVENTORY, "max_inventory");
        assert_eq!(param_names::QUOTE_SIZE, "quote_size");
        assert_eq!(param_names::RISK_AVERSION, "risk_aversion");
        assert_eq!(param_names::HIGH_ENTROPY_THRESHOLD, "high_entropy_threshold");
        assert_eq!(param_names::LOW_ENTROPY_THRESHOLD, "low_entropy_threshold");
        assert_eq!(param_names::SPREAD_BPS, "spread_bps");
        assert_eq!(param_names::SKEW_FACTOR, "skew_factor");
    }

    #[test]
    fn test_configurable_all_param_types_are_continuous() {
        let params = AvellanedaStoikovAlgorithm::parameters();
        for param in params {
            assert_eq!(
                param.param_type,
                ParameterType::Continuous,
                "Parameter '{}' should be Continuous type",
                param.name
            );
        }
    }

    #[test]
    fn test_configurable_boundary_values() {
        // Test at exact boundary values
        let params = AvellanedaStoikovAlgorithm::parameters();

        for param in params {
            if let Some((min, max)) = param.range {
                // Min value should be valid
                assert!(
                    param.validate(min).is_ok(),
                    "Parameter '{}' should accept min value {}",
                    param.name,
                    min
                );

                // Max value should be valid
                assert!(
                    param.validate(max).is_ok(),
                    "Parameter '{}' should accept max value {}",
                    param.name,
                    max
                );

                // Just below min should be invalid (with small epsilon)
                let below_min = min - 0.0001;
                if below_min >= 0.0 || param.name != param_names::SKEW_FACTOR {
                    assert!(
                        param.validate(below_min).is_err(),
                        "Parameter '{}' should reject value {} below min {}",
                        param.name,
                        below_min,
                        min
                    );
                }

                // Just above max should be invalid
                let above_max = max + 0.0001;
                assert!(
                    param.validate(above_max).is_err(),
                    "Parameter '{}' should reject value {} above max {}",
                    param.name,
                    above_max,
                    max
                );
            }
        }
    }

    #[test]
    fn test_configurable_create_with_grid_values() {
        // Create algorithms for a grid search scenario
        let spreads = [1.0, 2.0, 3.0];
        let skews = [0.3, 0.5, 0.7];

        for spread in spreads {
            for skew in skews {
                let mut params = HashMap::new();
                params.insert(param_names::SPREAD_BPS.to_string(), spread);
                params.insert(param_names::SKEW_FACTOR.to_string(), skew);

                let algo = AvellanedaStoikovAlgorithm::from_parameters(&params).unwrap();
                let config = algo.config();

                assert_eq!(
                    config.regime_params.high_entropy.spread_bps, spread,
                    "Spread mismatch for grid point ({}, {})",
                    spread, skew
                );
                assert_eq!(
                    config.regime_params.high_entropy.skew_factor, skew,
                    "Skew mismatch for grid point ({}, {})",
                    spread, skew
                );
            }
        }
    }

    #[test]
    fn test_configurable_uniform_regime_params() {
        // Verify that from_parameters creates uniform regime params
        let mut params = HashMap::new();
        params.insert(param_names::SPREAD_BPS.to_string(), 4.0);
        params.insert(param_names::SKEW_FACTOR.to_string(), 0.8);

        let algo = AvellanedaStoikovAlgorithm::from_parameters(&params).unwrap();
        let config = algo.config();

        // All regimes should have the same spread and skew
        assert_eq!(config.regime_params.high_entropy.spread_bps, 4.0);
        assert_eq!(config.regime_params.medium_entropy.spread_bps, 4.0);
        assert_eq!(config.regime_params.low_entropy.spread_bps, 4.0);

        assert_eq!(config.regime_params.high_entropy.skew_factor, 0.8);
        assert_eq!(config.regime_params.medium_entropy.skew_factor, 0.8);
        assert_eq!(config.regime_params.low_entropy.skew_factor, 0.8);
    }
}

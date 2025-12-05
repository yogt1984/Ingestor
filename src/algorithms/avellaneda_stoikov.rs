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
use serde::{Deserialize, Serialize};

use crate::algorithms::traits::{AlgorithmConfig, AlgorithmError, AlgorithmType, MarketInput, MarketMakingAlgorithm};
use crate::market_maker::{
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
/// use ingestor::algorithms::{AvellanedaStoikovAlgorithm, MarketMakingAlgorithm, MarketInput};
/// use ingestor::market_maker::AvellanedaStoikovConfig;
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
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
            side: crate::market_maker::QuoteSide::Bid,
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
            side: crate::market_maker::QuoteSide::Bid,
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
}

//! Fixed Spread Market Making Algorithm
//!
//! A simple baseline algorithm that uses constant spread and skew parameters.
//! This serves as a comparison baseline for evaluating more sophisticated algorithms.
//!
//! # Design Philosophy
//!
//! This algorithm intentionally ignores market conditions (volatility, entropy, etc.)
//! and applies fixed parameters regardless of the market state. This makes it useful for:
//!
//! - **Baseline comparison**: Comparing more complex algorithms against a simple baseline
//! - **Parameter sensitivity**: Understanding the impact of spread/skew on performance
//! - **Backtesting validation**: A deterministic algorithm for testing infrastructure
//!
//! # Parameters
//!
//! - `spread_bps`: Fixed half-spread in basis points (applied symmetrically to mid price)
//! - `skew_factor`: Fixed inventory skew factor (adjusts quotes based on current inventory)
//! - `max_inventory`: Maximum inventory position allowed
//! - `quote_size`: Size per order
//!
//! # Quote Computation
//!
//! ```text
//! mid_price = (best_bid + best_ask) / 2
//! spread_offset = mid_price * spread_bps / 10000
//! inventory_skew = skew_factor * (inventory / max_inventory) * spread_offset
//!
//! bid_price = mid_price - spread_offset - inventory_skew
//! ask_price = mid_price + spread_offset - inventory_skew
//! ```
//!
//! When inventory is positive (long), quotes are shifted down to encourage selling.
//! When inventory is negative (short), quotes are shifted up to encourage buying.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::algorithms::traits::{
    AlgorithmConfig, AlgorithmError, AlgorithmType, Configurable, MarketInput,
    MarketMakingAlgorithm, ParameterDefinition,
};
use crate::trading::market_maker::{Fill, MMQuotes, MMState, MarketRegime, PnLTracker, Quote, QuoteSide};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the Fixed Spread algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixedSpreadConfig {
    /// Maximum inventory position (absolute value)
    pub max_inventory: Decimal,
    /// Size per quote order
    pub quote_size: Decimal,
    /// Fixed half-spread in basis points
    pub spread_bps: f64,
    /// Fixed inventory skew factor
    pub skew_factor: f64,
}

impl Default for FixedSpreadConfig {
    fn default() -> Self {
        Self {
            max_inventory: dec!(0.1),
            quote_size: dec!(0.001),
            spread_bps: 1.0,  // 1 bps half-spread
            skew_factor: 0.3, // 30% skew factor
        }
    }
}

impl AlgorithmConfig for FixedSpreadConfig {
    fn algorithm_type(&self) -> AlgorithmType {
        AlgorithmType::FixedSpread
    }

    fn validate(&self) -> Result<(), AlgorithmError> {
        if self.max_inventory <= Decimal::ZERO {
            return Err(AlgorithmError::InvalidConfig(
                "max_inventory must be positive".to_string(),
            ));
        }
        if self.quote_size <= Decimal::ZERO {
            return Err(AlgorithmError::InvalidConfig(
                "quote_size must be positive".to_string(),
            ));
        }
        if self.spread_bps < 0.0 {
            return Err(AlgorithmError::InvalidConfig(
                "spread_bps must be non-negative".to_string(),
            ));
        }
        if self.skew_factor < 0.0 {
            return Err(AlgorithmError::InvalidConfig(
                "skew_factor must be non-negative".to_string(),
            ));
        }
        Ok(())
    }

    fn summary(&self) -> String {
        format!(
            "FixedSpread: spread_bps={:.2}, skew={:.2}, max_inv={}, quote_size={}",
            self.spread_bps, self.skew_factor, self.max_inventory, self.quote_size
        )
    }
}

// ============================================================================
// Algorithm Implementation
// ============================================================================

/// Fixed Spread market making algorithm.
///
/// A simple baseline algorithm that applies constant spread and skew parameters
/// regardless of market conditions. This is useful for:
/// - Baseline comparison against more sophisticated algorithms
/// - Testing infrastructure correctness
/// - Understanding the impact of spread/skew parameters
///
/// # Example
///
/// ```ignore
/// use crate::algorithms::FixedSpreadAlgorithm;
/// use rust_decimal_macros::dec;
///
/// let mut algo = FixedSpreadAlgorithm::new(FixedSpreadConfig {
///     spread_bps: 2.0,
///     skew_factor: 0.5,
///     ..Default::default()
/// });
///
/// let input = MarketInput {
///     best_bid: dec!(50000),
///     best_ask: dec!(50100),
///     volatility: 0.001,  // Ignored by this algorithm
///     entropy: 0.8,       // Ignored by this algorithm
///     book_imbalance: 0.1, // Ignored by this algorithm
///     timestamp_ms: 1000,
/// };
///
/// let quotes = algo.compute_quotes(&input);
/// ```
pub struct FixedSpreadAlgorithm {
    /// Algorithm configuration
    config: FixedSpreadConfig,
    /// Current inventory position
    inventory: Decimal,
    /// Average entry price for inventory
    avg_entry_price: Decimal,
    /// PnL tracker
    pnl: PnLTracker,
    /// Last quote timestamp
    last_quote_timestamp: u64,
}

impl FixedSpreadAlgorithm {
    /// Create a new Fixed Spread algorithm with the given configuration.
    pub fn new(config: FixedSpreadConfig) -> Self {
        Self {
            config,
            inventory: Decimal::ZERO,
            avg_entry_price: Decimal::ZERO,
            pnl: PnLTracker::default(),
            last_quote_timestamp: 0,
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(FixedSpreadConfig::default())
    }

    /// Create with specified spread and skew parameters.
    pub fn with_params(spread_bps: f64, skew_factor: f64) -> Self {
        Self::new(FixedSpreadConfig {
            spread_bps,
            skew_factor,
            ..Default::default()
        })
    }

    /// Create with fully specified parameters.
    pub fn with_full_params(
        max_inventory: Decimal,
        quote_size: Decimal,
        spread_bps: f64,
        skew_factor: f64,
    ) -> Self {
        Self::new(FixedSpreadConfig {
            max_inventory,
            quote_size,
            spread_bps,
            skew_factor,
        })
    }

    /// Get the algorithm configuration.
    pub fn config(&self) -> &FixedSpreadConfig {
        &self.config
    }

    /// Compute the spread offset from mid price.
    fn compute_spread_offset(&self, mid_price: Decimal) -> Decimal {
        // spread_offset = mid_price * spread_bps / 10000
        let spread_bps_decimal = Decimal::try_from(self.config.spread_bps).unwrap_or(dec!(1.0));
        mid_price * spread_bps_decimal / dec!(10000)
    }

    /// Compute the inventory skew adjustment.
    fn compute_inventory_skew(&self, spread_offset: Decimal) -> Decimal {
        if self.config.max_inventory.is_zero() {
            return Decimal::ZERO;
        }

        // inventory_skew = skew_factor * (inventory / max_inventory) * spread_offset
        let inventory_ratio = self.inventory / self.config.max_inventory;
        let skew_factor_decimal =
            Decimal::try_from(self.config.skew_factor).unwrap_or(dec!(0.3));

        skew_factor_decimal * inventory_ratio * spread_offset
    }

    /// Check if we can place a bid (not at max long inventory).
    fn can_bid(&self) -> bool {
        self.inventory < self.config.max_inventory
    }

    /// Check if we can place an ask (not at max short inventory).
    fn can_ask(&self) -> bool {
        self.inventory > -self.config.max_inventory
    }
}

impl MarketMakingAlgorithm for FixedSpreadAlgorithm {
    fn algorithm_type(&self) -> AlgorithmType {
        AlgorithmType::FixedSpread
    }

    fn name(&self) -> &'static str {
        "Fixed Spread Market Maker"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }

    fn compute_quotes(&mut self, input: &MarketInput) -> MMQuotes {
        self.last_quote_timestamp = input.timestamp_ms;

        let mid_price = input.mid_price();
        let spread_offset = self.compute_spread_offset(mid_price);
        let inventory_skew = self.compute_inventory_skew(spread_offset);

        // Compute bid and ask prices
        // When long (positive inventory), inventory_skew is positive, shifting quotes down
        // When short (negative inventory), inventory_skew is negative, shifting quotes up
        let bid_price = mid_price - spread_offset - inventory_skew;
        let ask_price = mid_price + spread_offset - inventory_skew;

        // Build quotes respecting inventory limits
        let bid = if self.can_bid() {
            Some(Quote {
                price: bid_price,
                size: self.config.quote_size,
                side: QuoteSide::Bid,
                timestamp_ms: input.timestamp_ms,
            })
        } else {
            None
        };

        let ask = if self.can_ask() {
            Some(Quote {
                price: ask_price,
                size: self.config.quote_size,
                side: QuoteSide::Ask,
                timestamp_ms: input.timestamp_ms,
            })
        } else {
            None
        };

        MMQuotes {
            bid,
            ask,
            // Fixed spread ignores entropy, but we report it as MediumEntropy
            // since it always quotes (doesn't differentiate by regime)
            regime: MarketRegime::MediumEntropy,
            fair_value: mid_price,
            half_spread: spread_offset,
            skew: inventory_skew,
        }
    }

    fn process_fill(&mut self, fill: Fill, fee_rate: Decimal) {
        let fill_value = fill.price * fill.size;

        match fill.side {
            QuoteSide::Bid => {
                // Bought: increase inventory
                let old_inventory = self.inventory;
                self.inventory += fill.size;

                // Update average entry price
                if old_inventory >= Decimal::ZERO {
                    // Adding to long or flat position
                    let old_value = old_inventory * self.avg_entry_price;
                    self.avg_entry_price = (old_value + fill_value)
                        / self.inventory.max(Decimal::ONE / dec!(1000000));
                }

                // Track PnL
                let fee = fill_value * fee_rate;
                self.pnl.realized_pnl -= fee;
                self.pnl.fees_paid += fee;
                self.pnl.num_trades += 1;
                self.pnl.total_volume += fill_value;
            }
            QuoteSide::Ask => {
                // Sold: decrease inventory
                let old_inventory = self.inventory;
                self.inventory -= fill.size;

                // Calculate realized PnL if closing a long position
                if old_inventory > Decimal::ZERO {
                    let close_size = fill.size.min(old_inventory);
                    let realized_pnl = (fill.price - self.avg_entry_price) * close_size;
                    self.pnl.realized_pnl += realized_pnl;

                    // If going short, reset average entry to fill price
                    if self.inventory < Decimal::ZERO {
                        self.avg_entry_price = fill.price;
                    }
                } else {
                    // Adding to short position
                    let old_value = old_inventory.abs() * self.avg_entry_price;
                    let new_value = old_value + fill_value;
                    self.avg_entry_price =
                        new_value / self.inventory.abs().max(Decimal::ONE / dec!(1000000));
                }

                // Track fees
                let fee = fill_value * fee_rate;
                self.pnl.realized_pnl -= fee;
                self.pnl.fees_paid += fee;
                self.pnl.num_trades += 1;
                self.pnl.total_volume += fill_value;
            }
        }
    }

    fn update_mark_to_market(&mut self, current_price: Decimal) {
        self.pnl.update(self.inventory, self.avg_entry_price, current_price);
    }

    fn get_state(&self) -> MMState {
        MMState {
            inventory: self.inventory,
            avg_entry_price: self.avg_entry_price,
            pnl: self.pnl.clone(),
            current_bid: None,
            current_ask: None,
        }
    }

    fn inventory(&self) -> Decimal {
        self.inventory
    }

    fn pnl(&self) -> &PnLTracker {
        &self.pnl
    }

    fn reset(&mut self) {
        self.inventory = Decimal::ZERO;
        self.avg_entry_price = Decimal::ZERO;
        self.pnl = PnLTracker::default();
        self.last_quote_timestamp = 0;
    }

    fn max_inventory(&self) -> Decimal {
        self.config.max_inventory
    }

    fn quote_size(&self) -> Decimal {
        self.config.quote_size
    }

    fn parameters_json(&self) -> serde_json::Value {
        serde_json::json!({
            "algorithm": self.type_string(),
            "version": self.version(),
            "max_inventory": self.config.max_inventory.to_string(),
            "quote_size": self.config.quote_size.to_string(),
            "spread_bps": self.config.spread_bps,
            "skew_factor": self.config.skew_factor,
        })
    }
}

impl std::fmt::Debug for FixedSpreadAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedSpreadAlgorithm")
            .field("type", &self.algorithm_type())
            .field("spread_bps", &self.config.spread_bps)
            .field("skew_factor", &self.config.skew_factor)
            .field("inventory", &self.inventory)
            .field("max_inventory", &self.max_inventory())
            .field("quote_size", &self.quote_size())
            .finish()
    }
}

// ============================================================================
// Configurable Trait Implementation
// ============================================================================

/// Parameter names for the Fixed Spread algorithm.
pub mod param_names {
    pub const MAX_INVENTORY: &str = "max_inventory";
    pub const QUOTE_SIZE: &str = "quote_size";
    pub const SPREAD_BPS: &str = "spread_bps";
    pub const SKEW_FACTOR: &str = "skew_factor";
}

impl Configurable for FixedSpreadAlgorithm {
    /// Returns the parameter definitions for the Fixed Spread algorithm.
    ///
    /// # Parameters
    ///
    /// | Name | Type | Range | Default | Description |
    /// |------|------|-------|---------|-------------|
    /// | max_inventory | Continuous | 0.001-10.0 | 0.1 | Maximum inventory position |
    /// | quote_size | Continuous | 0.0001-1.0 | 0.001 | Size per quote |
    /// | spread_bps | Continuous | 0.5-20.0 | 1.0 | Half-spread in basis points |
    /// | skew_factor | Continuous | 0.0-2.0 | 0.3 | Inventory skew factor |
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
            ParameterDefinition::continuous(param_names::SPREAD_BPS)
                .description("Fixed half-spread in basis points")
                .default(1.0)
                .range(0.5, 20.0)
                .tunable(true),
            ParameterDefinition::continuous(param_names::SKEW_FACTOR)
                .description("Fixed inventory skew factor")
                .default(0.3)
                .range(0.0, 2.0)
                .tunable(true),
        ]
    }

    fn from_parameters(params: &HashMap<String, f64>) -> Result<Self, AlgorithmError> {
        // Get defaults
        let defaults: HashMap<String, f64> = Self::parameters()
            .into_iter()
            .map(|p| (p.name.clone(), p.default))
            .collect();

        // Helper to get param with default fallback
        let get_param = |name: &str| -> f64 {
            *params
                .get(name)
                .unwrap_or_else(|| defaults.get(name).unwrap())
        };

        // Extract parameters
        let max_inventory = get_param(param_names::MAX_INVENTORY);
        let quote_size = get_param(param_names::QUOTE_SIZE);
        let spread_bps = get_param(param_names::SPREAD_BPS);
        let skew_factor = get_param(param_names::SKEW_FACTOR);

        // Validate parameters against definitions
        for param_def in Self::parameters() {
            if let Some(&value) = params.get(&param_def.name) {
                param_def.validate(value)?;
            }
        }

        // Build config
        let config = FixedSpreadConfig {
            max_inventory: Decimal::try_from(max_inventory).map_err(|e| {
                AlgorithmError::InvalidConfig(format!("Invalid max_inventory: {}", e))
            })?,
            quote_size: Decimal::try_from(quote_size).map_err(|e| {
                AlgorithmError::InvalidConfig(format!("Invalid quote_size: {}", e))
            })?,
            spread_bps,
            skew_factor,
        };

        config.validate()?;
        Ok(Self::new(config))
    }

    fn current_parameters(&self) -> HashMap<String, f64> {
        let mut params = HashMap::new();

        params.insert(
            param_names::MAX_INVENTORY.to_string(),
            self.config.max_inventory.to_f64().unwrap_or(0.1),
        );
        params.insert(
            param_names::QUOTE_SIZE.to_string(),
            self.config.quote_size.to_f64().unwrap_or(0.001),
        );
        params.insert(param_names::SPREAD_BPS.to_string(), self.config.spread_bps);
        params.insert(
            param_names::SKEW_FACTOR.to_string(),
            self.config.skew_factor,
        );

        params
    }

    fn set_parameter(&mut self, name: &str, value: f64) -> Result<(), AlgorithmError> {
        // Validate the parameter exists
        let param_defs = Self::parameters();
        let param_def = param_defs.iter().find(|p| p.name == name).ok_or_else(|| {
            AlgorithmError::InvalidConfig(format!("Unknown parameter: {}", name))
        })?;

        // Validate the value
        param_def.validate(value)?;

        // Get current parameters, update the one being changed
        let mut params = self.current_parameters();
        params.insert(name.to_string(), value);

        // Rebuild with new parameters
        let new_algo = Self::from_parameters(&params)?;

        // Replace config but preserve state
        let old_inventory = self.inventory;
        let old_avg_entry = self.avg_entry_price;
        let old_pnl = self.pnl.clone();
        let old_timestamp = self.last_quote_timestamp;

        *self = new_algo;

        // Restore state
        self.inventory = old_inventory;
        self.avg_entry_price = old_avg_entry;
        self.pnl = old_pnl;
        self.last_quote_timestamp = old_timestamp;

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithms::ParameterType;

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

    // ========================================================================
    // Basic Identity Tests
    // ========================================================================

    #[test]
    fn test_algorithm_type() {
        let algo = FixedSpreadAlgorithm::with_defaults();
        assert_eq!(algo.algorithm_type(), AlgorithmType::FixedSpread);
        assert_eq!(algo.type_string(), "fixed_spread");
    }

    #[test]
    fn test_algorithm_name() {
        let algo = FixedSpreadAlgorithm::with_defaults();
        assert_eq!(algo.name(), "Fixed Spread Market Maker");
    }

    #[test]
    fn test_algorithm_version() {
        let algo = FixedSpreadAlgorithm::with_defaults();
        assert_eq!(algo.version(), "1.0.0");
    }

    // ========================================================================
    // Construction Tests
    // ========================================================================

    #[test]
    fn test_new_with_config() {
        let config = FixedSpreadConfig {
            max_inventory: dec!(0.5),
            quote_size: dec!(0.01),
            spread_bps: 3.0,
            skew_factor: 0.7,
        };
        let algo = FixedSpreadAlgorithm::new(config);

        assert_eq!(algo.max_inventory(), dec!(0.5));
        assert_eq!(algo.quote_size(), dec!(0.01));
        assert_eq!(algo.config().spread_bps, 3.0);
        assert_eq!(algo.config().skew_factor, 0.7);
    }

    #[test]
    fn test_with_defaults() {
        let algo = FixedSpreadAlgorithm::with_defaults();

        assert_eq!(algo.max_inventory(), dec!(0.1));
        assert_eq!(algo.quote_size(), dec!(0.001));
        assert_eq!(algo.config().spread_bps, 1.0);
        assert_eq!(algo.config().skew_factor, 0.3);
    }

    #[test]
    fn test_with_params() {
        let algo = FixedSpreadAlgorithm::with_params(5.0, 0.8);

        assert_eq!(algo.config().spread_bps, 5.0);
        assert_eq!(algo.config().skew_factor, 0.8);
        // Should use defaults for other params
        assert_eq!(algo.max_inventory(), dec!(0.1));
        assert_eq!(algo.quote_size(), dec!(0.001));
    }

    #[test]
    fn test_with_full_params() {
        let algo =
            FixedSpreadAlgorithm::with_full_params(dec!(0.2), dec!(0.002), 2.5, 0.4);

        assert_eq!(algo.max_inventory(), dec!(0.2));
        assert_eq!(algo.quote_size(), dec!(0.002));
        assert_eq!(algo.config().spread_bps, 2.5);
        assert_eq!(algo.config().skew_factor, 0.4);
    }

    // ========================================================================
    // Config Validation Tests
    // ========================================================================

    #[test]
    fn test_config_validation_valid() {
        let config = FixedSpreadConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_zero_max_inventory() {
        let config = FixedSpreadConfig {
            max_inventory: Decimal::ZERO,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("max_inventory"));
    }

    #[test]
    fn test_config_validation_negative_max_inventory() {
        let config = FixedSpreadConfig {
            max_inventory: dec!(-0.1),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_quote_size() {
        let config = FixedSpreadConfig {
            quote_size: Decimal::ZERO,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_negative_spread() {
        let config = FixedSpreadConfig {
            spread_bps: -1.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_negative_skew() {
        let config = FixedSpreadConfig {
            skew_factor: -0.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_summary() {
        let config = FixedSpreadConfig::default();
        let summary = config.summary();
        assert!(summary.contains("FixedSpread"));
        assert!(summary.contains("spread_bps"));
        assert!(summary.contains("skew"));
    }

    // ========================================================================
    // Quote Computation Tests - Core Logic
    // ========================================================================

    #[test]
    fn test_compute_quotes_basic() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();
        let input = create_test_input(0.8);

        let quotes = algo.compute_quotes(&input);

        // Should produce both bid and ask
        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());
        // Verify the quotes have the correct timestamp from input
        assert_eq!(quotes.bid.as_ref().unwrap().timestamp_ms, 1000);
        assert_eq!(quotes.ask.as_ref().unwrap().timestamp_ms, 1000);
    }

    #[test]
    fn test_compute_quotes_spread_calculation() {
        let mut algo = FixedSpreadAlgorithm::with_params(1.0, 0.0); // 1 bps, no skew
        let input = create_test_input(0.8);

        let quotes = algo.compute_quotes(&input);
        let mid = input.mid_price(); // 50050

        // Expected spread offset: 50050 * 1 / 10000 = 5.005
        let expected_offset = mid * dec!(1) / dec!(10000);

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();

        // With no inventory and no skew:
        // bid = mid - offset = 50050 - 5.005 = 50044.995
        // ask = mid + offset = 50050 + 5.005 = 50055.005
        assert_eq!(bid.price, mid - expected_offset);
        assert_eq!(ask.price, mid + expected_offset);
    }

    #[test]
    fn test_compute_quotes_wider_spread() {
        let mut algo = FixedSpreadAlgorithm::with_params(5.0, 0.0); // 5 bps, no skew
        let input = create_test_input(0.8);

        let quotes = algo.compute_quotes(&input);
        let mid = input.mid_price();

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();

        // Wider spread = more distance from mid
        let expected_offset = mid * dec!(5) / dec!(10000);
        assert_eq!(bid.price, mid - expected_offset);
        assert_eq!(ask.price, mid + expected_offset);
    }

    #[test]
    fn test_compute_quotes_zero_spread() {
        let mut algo = FixedSpreadAlgorithm::with_params(0.5, 0.0); // 0.5 bps minimum, no skew
        let input = create_test_input(0.8);

        let quotes = algo.compute_quotes(&input);

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();

        // Even with tiny spread, bid < ask
        assert!(bid.price < ask.price);
    }

    #[test]
    fn test_compute_quotes_quote_sizes() {
        let config = FixedSpreadConfig {
            quote_size: dec!(0.05),
            ..Default::default()
        };
        let mut algo = FixedSpreadAlgorithm::new(config);
        let input = create_test_input(0.8);

        let quotes = algo.compute_quotes(&input);

        assert_eq!(quotes.bid.unwrap().size, dec!(0.05));
        assert_eq!(quotes.ask.unwrap().size, dec!(0.05));
    }

    // ========================================================================
    // Inventory Skew Tests
    // ========================================================================

    #[test]
    fn test_skew_with_zero_inventory() {
        let mut algo = FixedSpreadAlgorithm::with_params(2.0, 0.5);
        let input = create_test_input(0.8);

        // With zero inventory, skew should be zero
        let quotes = algo.compute_quotes(&input);
        let mid = input.mid_price();
        let spread_offset = mid * dec!(2) / dec!(10000);

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();

        // Quotes should be symmetric around mid
        assert_eq!(bid.price, mid - spread_offset);
        assert_eq!(ask.price, mid + spread_offset);
    }

    #[test]
    fn test_skew_with_long_inventory() {
        let mut algo = FixedSpreadAlgorithm::with_full_params(dec!(0.1), dec!(0.01), 2.0, 0.5);
        let input = create_test_input(0.8);

        // Simulate having long inventory
        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.05), // 50% of max inventory
            timestamp_ms: 500,
        };
        algo.process_fill(fill, dec!(0.0001));

        assert_eq!(algo.inventory(), dec!(0.05));

        let quotes = algo.compute_quotes(&input);
        let mid = input.mid_price();
        let spread_offset = mid * dec!(2) / dec!(10000);

        // inventory_skew = 0.5 * (0.05 / 0.1) * spread_offset = 0.25 * spread_offset
        let inventory_skew = dec!(0.5) * (dec!(0.05) / dec!(0.1)) * spread_offset;

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();

        // Long inventory shifts quotes down (to encourage selling)
        assert_eq!(bid.price, mid - spread_offset - inventory_skew);
        assert_eq!(ask.price, mid + spread_offset - inventory_skew);

        // Ask should be closer to mid (lower) than symmetric
        let symmetric_ask = mid + spread_offset;
        assert!(ask.price < symmetric_ask);
    }

    #[test]
    fn test_skew_with_short_inventory() {
        let mut algo = FixedSpreadAlgorithm::with_full_params(dec!(0.1), dec!(0.01), 2.0, 0.5);
        let input = create_test_input(0.8);

        // Simulate having short inventory
        let fill = Fill {
            side: QuoteSide::Ask,
            price: dec!(50100),
            size: dec!(0.05), // 50% of max inventory (short)
            timestamp_ms: 500,
        };
        algo.process_fill(fill, dec!(0.0001));

        assert_eq!(algo.inventory(), dec!(-0.05));

        let quotes = algo.compute_quotes(&input);
        let mid = input.mid_price();
        let spread_offset = mid * dec!(2) / dec!(10000);

        // inventory_skew = 0.5 * (-0.05 / 0.1) * spread_offset = -0.25 * spread_offset
        let inventory_skew = dec!(0.5) * (dec!(-0.05) / dec!(0.1)) * spread_offset;

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();

        // Short inventory shifts quotes up (to encourage buying)
        assert_eq!(bid.price, mid - spread_offset - inventory_skew);
        assert_eq!(ask.price, mid + spread_offset - inventory_skew);

        // Bid should be higher than symmetric (closer to mid)
        let symmetric_bid = mid - spread_offset;
        assert!(bid.price > symmetric_bid);
    }

    #[test]
    fn test_skew_at_max_inventory() {
        let mut algo = FixedSpreadAlgorithm::with_full_params(dec!(0.1), dec!(0.01), 2.0, 1.0);
        let input = create_test_input(0.8);

        // Fill to max inventory
        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.1),
            timestamp_ms: 500,
        };
        algo.process_fill(fill, dec!(0.0001));

        assert_eq!(algo.inventory(), dec!(0.1)); // At max

        let quotes = algo.compute_quotes(&input);

        // At max inventory, should not be able to bid
        assert!(quotes.bid.is_none());
        assert!(quotes.ask.is_some());

        // Ask should be shifted down significantly (skew = 1.0 * 1.0 * spread_offset)
        let mid = input.mid_price();
        let spread_offset = mid * dec!(2) / dec!(10000);
        let max_skew = spread_offset; // skew_factor * (1.0) * spread_offset

        let ask = quotes.ask.unwrap();
        assert_eq!(ask.price, mid + spread_offset - max_skew);
    }

    #[test]
    fn test_skew_zero_skew_factor() {
        let mut algo = FixedSpreadAlgorithm::with_full_params(dec!(0.1), dec!(0.01), 2.0, 0.0);
        let input = create_test_input(0.8);

        // Get inventory
        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.05),
            timestamp_ms: 500,
        };
        algo.process_fill(fill, dec!(0.0001));

        let quotes = algo.compute_quotes(&input);
        let mid = input.mid_price();
        let spread_offset = mid * dec!(2) / dec!(10000);

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();

        // With zero skew factor, quotes should be symmetric regardless of inventory
        assert_eq!(bid.price, mid - spread_offset);
        assert_eq!(ask.price, mid + spread_offset);
    }

    // ========================================================================
    // Inventory Limit Tests
    // ========================================================================

    #[test]
    fn test_no_bid_at_max_long() {
        let mut algo = FixedSpreadAlgorithm::with_full_params(dec!(0.1), dec!(0.01), 2.0, 0.5);

        // Fill to max inventory
        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.1),
            timestamp_ms: 500,
        };
        algo.process_fill(fill, dec!(0.0001));

        let input = create_test_input(0.8);
        let quotes = algo.compute_quotes(&input);

        assert!(quotes.bid.is_none(), "Should not bid when at max inventory");
        assert!(quotes.ask.is_some());
    }

    #[test]
    fn test_no_ask_at_max_short() {
        let mut algo = FixedSpreadAlgorithm::with_full_params(dec!(0.1), dec!(0.01), 2.0, 0.5);

        // Sell to max short inventory
        let fill = Fill {
            side: QuoteSide::Ask,
            price: dec!(50100),
            size: dec!(0.1),
            timestamp_ms: 500,
        };
        algo.process_fill(fill, dec!(0.0001));

        let input = create_test_input(0.8);
        let quotes = algo.compute_quotes(&input);

        assert!(quotes.bid.is_some());
        assert!(
            quotes.ask.is_none(),
            "Should not ask when at max short inventory"
        );
    }

    #[test]
    fn test_partial_inventory_both_sides() {
        let mut algo = FixedSpreadAlgorithm::with_full_params(dec!(0.1), dec!(0.01), 2.0, 0.5);

        // Partial inventory (50% of max)
        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.05),
            timestamp_ms: 500,
        };
        algo.process_fill(fill, dec!(0.0001));

        let input = create_test_input(0.8);
        let quotes = algo.compute_quotes(&input);

        // Should still quote both sides
        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());
    }

    // ========================================================================
    // Fill Processing Tests
    // ========================================================================

    #[test]
    fn test_process_bid_fill_inventory_increase() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0.0001));

        assert_eq!(algo.inventory(), dec!(0.01));
        assert_eq!(algo.pnl().num_trades, 1);
    }

    #[test]
    fn test_process_ask_fill_inventory_decrease() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Ask,
            price: dec!(50100),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0.0001));

        assert_eq!(algo.inventory(), dec!(-0.01));
        assert_eq!(algo.pnl().num_trades, 1);
    }

    #[test]
    fn test_process_fill_multiple_buys() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        for i in 0..3 {
            let fill = Fill {
                side: QuoteSide::Bid,
                price: dec!(50000) + Decimal::from(i * 10),
                size: dec!(0.01),
                timestamp_ms: 1000 + i as u64,
            };
            algo.process_fill(fill, dec!(0.0001));
        }

        assert_eq!(algo.inventory(), dec!(0.03));
        assert_eq!(algo.pnl().num_trades, 3);
    }

    #[test]
    fn test_process_fill_buy_then_sell() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        // Buy
        let buy = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(buy, dec!(0.0001));

        // Sell at profit
        let sell = Fill {
            side: QuoteSide::Ask,
            price: dec!(50100),
            size: dec!(0.01),
            timestamp_ms: 2000,
        };
        algo.process_fill(sell, dec!(0.0001));

        assert_eq!(algo.inventory(), dec!(0));
        assert_eq!(algo.pnl().num_trades, 2);
        // Realized PnL should be positive (100 per unit * 0.01 - fees)
        assert!(algo.pnl().realized_pnl > dec!(0));
    }

    #[test]
    fn test_process_fill_fees_deducted() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0.001)); // 0.1% fee

        // Fee = 50000 * 0.01 * 0.001 = 0.5
        assert!(algo.pnl().realized_pnl < dec!(0));
        assert!((algo.pnl().realized_pnl + dec!(0.5)).abs() < dec!(0.001));
    }

    // ========================================================================
    // Mark to Market Tests
    // ========================================================================

    #[test]
    fn test_mark_to_market_zero_inventory() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();
        algo.update_mark_to_market(dec!(50000));

        assert_eq!(algo.pnl().unrealized_pnl, dec!(0));
    }

    #[test]
    fn test_mark_to_market_long_profit() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0));

        // Price went up
        algo.update_mark_to_market(dec!(50100));

        // Unrealized: (50100 - 50000) * 0.01 = 1.0
        assert_eq!(algo.pnl().unrealized_pnl, dec!(1));
    }

    #[test]
    fn test_mark_to_market_long_loss() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0));

        // Price went down
        algo.update_mark_to_market(dec!(49900));

        // Unrealized: (49900 - 50000) * 0.01 = -1.0
        assert_eq!(algo.pnl().unrealized_pnl, dec!(-1));
    }

    #[test]
    fn test_mark_to_market_short_profit() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Ask,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0));

        // Price went down (profit for short)
        algo.update_mark_to_market(dec!(49900));

        // Unrealized: (50000 - 49900) * 0.01 = 1.0
        assert_eq!(algo.pnl().unrealized_pnl, dec!(1));
    }

    // ========================================================================
    // Reset Tests
    // ========================================================================

    #[test]
    fn test_reset_clears_inventory() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0.0001));

        assert_ne!(algo.inventory(), dec!(0));

        algo.reset();

        assert_eq!(algo.inventory(), dec!(0));
    }

    #[test]
    fn test_reset_clears_pnl() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0.0001));
        algo.update_mark_to_market(dec!(50100));

        assert!(algo.pnl().num_trades > 0);

        algo.reset();

        assert_eq!(algo.pnl().num_trades, 0);
        assert_eq!(algo.pnl().realized_pnl, dec!(0));
        assert_eq!(algo.pnl().unrealized_pnl, dec!(0));
    }

    #[test]
    fn test_reset_preserves_config() {
        let algo = FixedSpreadAlgorithm::with_params(5.0, 0.7);
        let spread_before = algo.config().spread_bps;
        let skew_before = algo.config().skew_factor;

        let mut algo = algo;
        algo.reset();

        assert_eq!(algo.config().spread_bps, spread_before);
        assert_eq!(algo.config().skew_factor, skew_before);
    }

    // ========================================================================
    // State Tests
    // ========================================================================

    #[test]
    fn test_get_state() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0.0001));

        let state = algo.get_state();

        assert_eq!(state.inventory, dec!(0.01));
        assert_eq!(state.pnl.num_trades, 1);
    }

    #[test]
    fn test_parameters_json() {
        let algo = FixedSpreadAlgorithm::with_params(3.0, 0.6);
        let json = algo.parameters_json();

        assert_eq!(json["algorithm"], "fixed_spread");
        assert_eq!(json["version"], "1.0.0");
        assert_eq!(json["spread_bps"], 3.0);
        assert_eq!(json["skew_factor"], 0.6);
    }

    // ========================================================================
    // Entropy Invariance Tests
    // ========================================================================

    #[test]
    fn test_ignores_entropy_low() {
        let mut algo = FixedSpreadAlgorithm::with_params(2.0, 0.0);
        let input = create_test_input(0.1); // Low entropy

        let quotes = algo.compute_quotes(&input);

        // Should still quote (doesn't care about entropy)
        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());
    }

    #[test]
    fn test_ignores_entropy_high() {
        let mut algo = FixedSpreadAlgorithm::with_params(2.0, 0.0);
        let input = create_test_input(0.9); // High entropy

        let quotes = algo.compute_quotes(&input);

        // Should still quote same as low entropy
        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());
    }

    #[test]
    fn test_same_quotes_regardless_of_entropy() {
        let mut algo = FixedSpreadAlgorithm::with_params(2.0, 0.0);

        let low_entropy_input = create_test_input(0.2);
        let high_entropy_input = create_test_input(0.8);

        let low_quotes = algo.compute_quotes(&low_entropy_input);
        algo.reset(); // Reset between tests
        let high_quotes = algo.compute_quotes(&high_entropy_input);

        // Quotes should be identical (entropy is ignored)
        assert_eq!(
            low_quotes.bid.unwrap().price,
            high_quotes.bid.unwrap().price
        );
        assert_eq!(
            low_quotes.ask.unwrap().price,
            high_quotes.ask.unwrap().price
        );
    }

    #[test]
    fn test_ignores_volatility() {
        let mut algo = FixedSpreadAlgorithm::with_params(2.0, 0.0);

        let low_vol = MarketInput {
            best_bid: dec!(50000),
            best_ask: dec!(50100),
            volatility: 0.0001, // Very low
            entropy: 0.5,
            book_imbalance: 0.0,
            timestamp_ms: 1000,
        };

        let high_vol = MarketInput {
            best_bid: dec!(50000),
            best_ask: dec!(50100),
            volatility: 0.1, // Very high
            entropy: 0.5,
            book_imbalance: 0.0,
            timestamp_ms: 1000,
        };

        let low_quotes = algo.compute_quotes(&low_vol);
        algo.reset();
        let high_quotes = algo.compute_quotes(&high_vol);

        assert_eq!(
            low_quotes.bid.unwrap().price,
            high_quotes.bid.unwrap().price
        );
        assert_eq!(
            low_quotes.ask.unwrap().price,
            high_quotes.ask.unwrap().price
        );
    }

    #[test]
    fn test_ignores_book_imbalance() {
        let mut algo = FixedSpreadAlgorithm::with_params(2.0, 0.0);

        let buy_pressure = MarketInput {
            best_bid: dec!(50000),
            best_ask: dec!(50100),
            volatility: 0.001,
            entropy: 0.5,
            book_imbalance: 0.9, // Strong buy pressure
            timestamp_ms: 1000,
        };

        let sell_pressure = MarketInput {
            best_bid: dec!(50000),
            best_ask: dec!(50100),
            volatility: 0.001,
            entropy: 0.5,
            book_imbalance: -0.9, // Strong sell pressure
            timestamp_ms: 1000,
        };

        let buy_quotes = algo.compute_quotes(&buy_pressure);
        algo.reset();
        let sell_quotes = algo.compute_quotes(&sell_pressure);

        // FixedSpread ignores book imbalance
        assert_eq!(
            buy_quotes.bid.unwrap().price,
            sell_quotes.bid.unwrap().price
        );
    }

    // ========================================================================
    // Trait Object Tests
    // ========================================================================

    #[test]
    fn test_trait_object_usage() {
        let algo: Box<dyn MarketMakingAlgorithm> =
            Box::new(FixedSpreadAlgorithm::with_defaults());

        assert_eq!(algo.algorithm_type(), AlgorithmType::FixedSpread);
        assert_eq!(algo.type_string(), "fixed_spread");
        assert_eq!(algo.name(), "Fixed Spread Market Maker");
    }

    #[test]
    fn test_debug_impl() {
        let algo = FixedSpreadAlgorithm::with_params(3.0, 0.5);
        let debug_str = format!("{:?}", algo);

        assert!(debug_str.contains("FixedSpreadAlgorithm"));
        assert!(debug_str.contains("spread_bps"));
        assert!(debug_str.contains("3"));
    }

    // ========================================================================
    // Configurable Trait Tests
    // ========================================================================

    #[test]
    fn test_configurable_parameters_count() {
        let params = FixedSpreadAlgorithm::parameters();
        assert_eq!(params.len(), 4, "FixedSpread should have exactly 4 parameters");
    }

    #[test]
    fn test_configurable_parameters_names() {
        let params = FixedSpreadAlgorithm::parameters();
        let names: Vec<&str> = params.iter().map(|p| p.name.as_str()).collect();

        assert!(names.contains(&param_names::MAX_INVENTORY));
        assert!(names.contains(&param_names::QUOTE_SIZE));
        assert!(names.contains(&param_names::SPREAD_BPS));
        assert!(names.contains(&param_names::SKEW_FACTOR));
    }

    #[test]
    fn test_configurable_parameters_all_have_descriptions() {
        let params = FixedSpreadAlgorithm::parameters();
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
        let params = FixedSpreadAlgorithm::parameters();
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
        let params = FixedSpreadAlgorithm::parameters();
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
        let tunable = FixedSpreadAlgorithm::tunable_parameters();
        let tunable_names: Vec<&str> = tunable.iter().map(|p| p.name.as_str()).collect();

        // These should be tunable
        assert!(tunable_names.contains(&param_names::SPREAD_BPS));
        assert!(tunable_names.contains(&param_names::SKEW_FACTOR));

        // These should NOT be tunable
        assert!(!tunable_names.contains(&param_names::MAX_INVENTORY));
        assert!(!tunable_names.contains(&param_names::QUOTE_SIZE));
    }

    #[test]
    fn test_configurable_from_parameters_default() {
        let params = HashMap::new();
        let algo = FixedSpreadAlgorithm::from_parameters(&params).unwrap();

        let config = algo.config();
        assert_eq!(config.max_inventory, dec!(0.1));
        assert_eq!(config.quote_size, dec!(0.001));
        assert_eq!(config.spread_bps, 1.0);
        assert_eq!(config.skew_factor, 0.3);
    }

    #[test]
    fn test_configurable_from_parameters_custom() {
        let mut params = HashMap::new();
        params.insert(param_names::MAX_INVENTORY.to_string(), 0.5);
        params.insert(param_names::QUOTE_SIZE.to_string(), 0.01);
        params.insert(param_names::SPREAD_BPS.to_string(), 3.0);
        params.insert(param_names::SKEW_FACTOR.to_string(), 0.7);

        let algo = FixedSpreadAlgorithm::from_parameters(&params).unwrap();

        let config = algo.config();
        assert_eq!(config.max_inventory, dec!(0.5));
        assert_eq!(config.quote_size, dec!(0.01));
        assert_eq!(config.spread_bps, 3.0);
        assert_eq!(config.skew_factor, 0.7);
    }

    #[test]
    fn test_configurable_from_parameters_partial() {
        let mut params = HashMap::new();
        params.insert(param_names::SPREAD_BPS.to_string(), 5.0);

        let algo = FixedSpreadAlgorithm::from_parameters(&params).unwrap();

        let config = algo.config();
        assert_eq!(config.spread_bps, 5.0);
        // Other params should be default
        assert_eq!(config.max_inventory, dec!(0.1));
        assert_eq!(config.skew_factor, 0.3);
    }

    #[test]
    fn test_configurable_from_parameters_validation() {
        let mut params = HashMap::new();
        params.insert(param_names::MAX_INVENTORY.to_string(), 0.0001); // Below min

        let result = FixedSpreadAlgorithm::from_parameters(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_configurable_current_parameters_roundtrip() {
        let mut original_params = HashMap::new();
        original_params.insert(param_names::MAX_INVENTORY.to_string(), 0.5);
        original_params.insert(param_names::QUOTE_SIZE.to_string(), 0.01);
        original_params.insert(param_names::SPREAD_BPS.to_string(), 3.0);
        original_params.insert(param_names::SKEW_FACTOR.to_string(), 0.7);

        let algo = FixedSpreadAlgorithm::from_parameters(&original_params).unwrap();
        let current = algo.current_parameters();

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
        let mut algo = FixedSpreadAlgorithm::with_defaults();
        assert_eq!(algo.config().spread_bps, 1.0);

        algo.set_parameter(param_names::SPREAD_BPS, 5.0).unwrap();

        assert_eq!(algo.config().spread_bps, 5.0);
    }

    #[test]
    fn test_configurable_set_parameter_preserves_state() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        // Build up some state
        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(fill, dec!(0.0001));

        let inventory_before = algo.inventory();
        let num_trades_before = algo.pnl().num_trades;

        // Update a parameter
        algo.set_parameter(param_names::SPREAD_BPS, 5.0).unwrap();

        // State should be preserved
        assert_eq!(algo.inventory(), inventory_before);
        assert_eq!(algo.pnl().num_trades, num_trades_before);
    }

    #[test]
    fn test_configurable_set_parameter_unknown() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let result = algo.set_parameter("unknown_param", 1.0);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Unknown parameter"));
    }

    #[test]
    fn test_configurable_set_parameter_out_of_range() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        let result = algo.set_parameter(param_names::SPREAD_BPS, 0.1); // Below min 0.5
        assert!(result.is_err());
    }

    #[test]
    fn test_configurable_all_param_types_are_continuous() {
        let params = FixedSpreadAlgorithm::parameters();
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
        let params = FixedSpreadAlgorithm::parameters();

        for param in params {
            if let Some((min, max)) = param.range {
                assert!(
                    param.validate(min).is_ok(),
                    "Parameter '{}' should accept min value {}",
                    param.name,
                    min
                );
                assert!(
                    param.validate(max).is_ok(),
                    "Parameter '{}' should accept max value {}",
                    param.name,
                    max
                );

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
    fn test_configurable_grid_values() {
        let params = FixedSpreadAlgorithm::parameters();
        let spread_param = params
            .iter()
            .find(|p| p.name == param_names::SPREAD_BPS)
            .unwrap();

        let grid = spread_param.grid_values(5);
        assert_eq!(grid.len(), 5);

        let (min, max) = spread_param.range.unwrap();
        assert!((grid[0] - min).abs() < 0.0001);
        assert!((grid[4] - max).abs() < 0.0001);
    }

    #[test]
    fn test_configurable_create_with_grid_values() {
        let spreads = [1.0, 2.0, 3.0];
        let skews = [0.3, 0.5, 0.7];

        for spread in spreads {
            for skew in skews {
                let mut params = HashMap::new();
                params.insert(param_names::SPREAD_BPS.to_string(), spread);
                params.insert(param_names::SKEW_FACTOR.to_string(), skew);

                let algo = FixedSpreadAlgorithm::from_parameters(&params).unwrap();
                let config = algo.config();

                assert_eq!(config.spread_bps, spread);
                assert_eq!(config.skew_factor, skew);
            }
        }
    }

    // ========================================================================
    // Numerical Precision Tests
    // ========================================================================

    #[test]
    fn test_spread_precision_small_price() {
        let mut algo = FixedSpreadAlgorithm::with_params(1.0, 0.0);

        let input = MarketInput {
            best_bid: dec!(0.00001),
            best_ask: dec!(0.00002),
            volatility: 0.001,
            entropy: 0.5,
            book_imbalance: 0.0,
            timestamp_ms: 1000,
        };

        let quotes = algo.compute_quotes(&input);

        // Should still produce valid quotes
        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());
        assert!(quotes.bid.unwrap().price < quotes.ask.unwrap().price);
    }

    #[test]
    fn test_spread_precision_large_price() {
        let mut algo = FixedSpreadAlgorithm::with_params(1.0, 0.0);

        let input = MarketInput {
            best_bid: dec!(100000000),
            best_ask: dec!(100000100),
            volatility: 0.001,
            entropy: 0.5,
            book_imbalance: 0.0,
            timestamp_ms: 1000,
        };

        let quotes = algo.compute_quotes(&input);

        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());
        assert!(quotes.bid.unwrap().price < quotes.ask.unwrap().price);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_zero_skew_factor_boundary() {
        let algo = FixedSpreadAlgorithm::with_params(2.0, 0.0);
        assert_eq!(algo.config().skew_factor, 0.0);
    }

    #[test]
    fn test_max_skew_factor_boundary() {
        let mut params = HashMap::new();
        params.insert(param_names::SKEW_FACTOR.to_string(), 2.0); // Max allowed

        let algo = FixedSpreadAlgorithm::from_parameters(&params).unwrap();
        assert_eq!(algo.config().skew_factor, 2.0);
    }

    #[test]
    fn test_min_spread_boundary() {
        let mut params = HashMap::new();
        params.insert(param_names::SPREAD_BPS.to_string(), 0.5); // Min allowed

        let algo = FixedSpreadAlgorithm::from_parameters(&params).unwrap();
        assert_eq!(algo.config().spread_bps, 0.5);
    }

    #[test]
    fn test_max_spread_boundary() {
        let mut params = HashMap::new();
        params.insert(param_names::SPREAD_BPS.to_string(), 20.0); // Max allowed

        let algo = FixedSpreadAlgorithm::from_parameters(&params).unwrap();
        assert_eq!(algo.config().spread_bps, 20.0);
    }

    // ========================================================================
    // Consistency Tests (Paranoid Testing)
    // ========================================================================

    #[test]
    fn test_quotes_consistent_over_multiple_calls() {
        let mut algo = FixedSpreadAlgorithm::with_params(2.0, 0.0);
        let input = create_test_input(0.5);

        let quotes1 = algo.compute_quotes(&input);
        let quotes2 = algo.compute_quotes(&input);
        let quotes3 = algo.compute_quotes(&input);

        // All quotes should be identical (deterministic)
        assert_eq!(quotes1.bid.as_ref().unwrap().price, quotes2.bid.as_ref().unwrap().price);
        assert_eq!(quotes2.bid.as_ref().unwrap().price, quotes3.bid.as_ref().unwrap().price);
        assert_eq!(quotes1.ask.as_ref().unwrap().price, quotes2.ask.as_ref().unwrap().price);
        assert_eq!(quotes2.ask.as_ref().unwrap().price, quotes3.ask.as_ref().unwrap().price);
    }

    #[test]
    fn test_bid_always_less_than_ask() {
        for spread in [0.5, 1.0, 5.0, 10.0, 20.0] {
            for skew in [0.0, 0.3, 0.5, 1.0, 2.0] {
                let mut algo = FixedSpreadAlgorithm::with_params(spread, skew);
                let input = create_test_input(0.5);

                // Test at various inventory levels
                for inv_pct in [-100i32, -50, 0, 50, 100] {
                    algo.reset();
                    if inv_pct != 0 {
                        let size = dec!(0.1) * Decimal::from(inv_pct.abs()) / dec!(100);
                        let fill = Fill {
                            side: if inv_pct > 0 {
                                QuoteSide::Bid
                            } else {
                                QuoteSide::Ask
                            },
                            price: dec!(50000),
                            size,
                            timestamp_ms: 500,
                        };
                        algo.process_fill(fill, dec!(0));
                    }

                    let quotes = algo.compute_quotes(&input);

                    // If we have both quotes, bid must be < ask
                    if let (Some(bid), Some(ask)) = (quotes.bid, quotes.ask) {
                        assert!(
                            bid.price < ask.price,
                            "Bid >= Ask at spread={}, skew={}, inv_pct={}: bid={}, ask={}",
                            spread,
                            skew,
                            inv_pct,
                            bid.price,
                            ask.price
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_inventory_limits_always_respected() {
        let mut algo = FixedSpreadAlgorithm::with_full_params(dec!(0.05), dec!(0.01), 2.0, 0.5);
        let input = create_test_input(0.5);

        // Try to exceed max inventory through many fills
        for _ in 0..20 {
            let quotes = algo.compute_quotes(&input);
            if quotes.bid.is_some() {
                let fill = Fill {
                    side: QuoteSide::Bid,
                    price: dec!(50000),
                    size: dec!(0.01),
                    timestamp_ms: 1000,
                };
                algo.process_fill(fill, dec!(0));
            }
        }

        // Inventory should never exceed max
        assert!(
            algo.inventory() <= algo.max_inventory(),
            "Inventory {} exceeded max {}",
            algo.inventory(),
            algo.max_inventory()
        );

        // No more bids should be allowed
        let quotes = algo.compute_quotes(&input);
        assert!(quotes.bid.is_none(), "Should not bid at max inventory");
    }

    #[test]
    fn test_pnl_accounting_symmetry() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        // Buy at 50000
        let buy = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };
        algo.process_fill(buy, dec!(0)); // No fee for clarity

        // Sell at same price
        let sell = Fill {
            side: QuoteSide::Ask,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 2000,
        };
        algo.process_fill(sell, dec!(0));

        // Should have zero inventory and zero PnL
        assert_eq!(algo.inventory(), dec!(0));
        assert_eq!(algo.pnl().realized_pnl, dec!(0));
    }

    #[test]
    fn test_quote_size_consistency() {
        let config = FixedSpreadConfig {
            quote_size: dec!(0.05),
            ..Default::default()
        };
        let mut algo = FixedSpreadAlgorithm::new(config);

        // All quotes at all inventory levels should have same size
        for _ in 0..5 {
            let input = create_test_input(0.5);
            let quotes = algo.compute_quotes(&input);

            let has_bid = quotes.bid.is_some();
            if let Some(ref bid) = quotes.bid {
                assert_eq!(bid.size, dec!(0.05));
            }
            if let Some(ref ask) = quotes.ask {
                assert_eq!(ask.size, dec!(0.05));
            }

            // Add some inventory
            if has_bid {
                let fill = Fill {
                    side: QuoteSide::Bid,
                    price: dec!(50000),
                    size: dec!(0.02),
                    timestamp_ms: 1000,
                };
                algo.process_fill(fill, dec!(0));
            }
        }
    }

    #[test]
    fn test_regime_always_medium_entropy() {
        let mut algo = FixedSpreadAlgorithm::with_defaults();

        // Fixed spread doesn't distinguish regimes
        for entropy in [0.1, 0.5, 0.9] {
            let input = create_test_input(entropy);
            let quotes = algo.compute_quotes(&input);
            assert_eq!(
                quotes.regime,
                MarketRegime::MediumEntropy,
                "FixedSpread should always report MediumEntropy"
            );
        }
    }
}

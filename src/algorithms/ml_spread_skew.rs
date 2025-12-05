//! ML-Based Spread/Skew Predictor Algorithm
//!
//! Uses a learned model to predict optimal spread and skew parameters
//! based on market features (entropy, volatility, imbalance).
//!
//! # Model Architecture
//!
//! Phase 1 uses a simple linear model:
//! ```text
//! spread_bps = w0 + w1*entropy + w2*volatility + w3*imbalance + w4*entropy*volatility
//! skew_factor = v0 + v1*entropy + v2*volatility + v3*imbalance + v4*inventory_ratio
//! ```
//!
//! # Training
//!
//! Model weights can be:
//! 1. Loaded from a JSON config file
//! 2. Set programmatically via `MLModelWeights`
//! 3. Trained offline using the backtesting infrastructure
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::algorithms::{MLSpreadSkewAlgorithm, MLModelWeights};
//!
//! // Create with default weights (baseline from A-S optimization)
//! let algo = MLSpreadSkewAlgorithm::with_defaults();
//!
//! // Or with custom trained weights
//! let weights = MLModelWeights::load_from_file("model.json")?;
//! let algo = MLSpreadSkewAlgorithm::new(config, weights);
//! ```

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::Path;

use crate::algorithms::traits::{AlgorithmType, MarketInput, MarketMakingAlgorithm};
use crate::market_maker::{Fill, MMQuotes, MMState, PnLTracker, Quote, QuoteSide, MarketRegime, RegimeThresholds};

// ============================================================================
// Model Weights
// ============================================================================

/// Weights for the spread prediction linear model.
///
/// spread_bps = intercept + entropy*w_entropy + volatility*w_volatility
///            + imbalance*w_imbalance + entropy*volatility*w_interaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadWeights {
    /// Intercept (base spread in bps)
    pub intercept: f64,
    /// Weight for entropy (higher entropy -> can tighten spread)
    pub w_entropy: f64,
    /// Weight for volatility (higher vol -> widen spread)
    pub w_volatility: f64,
    /// Weight for absolute imbalance (higher imbalance -> widen spread)
    pub w_imbalance: f64,
    /// Interaction term: entropy * volatility
    pub w_interaction: f64,
}

impl Default for SpreadWeights {
    fn default() -> Self {
        // Default weights derived from A-S regime optimization:
        // - Base spread ~2.5 bps (medium regime)
        // - Tighter in high entropy (-1.5 bps when entropy=1.0)
        // - Wider in low entropy / high volatility
        Self {
            intercept: 3.0,        // Base spread 3 bps
            w_entropy: -2.0,       // High entropy -> tighter spread
            w_volatility: 500.0,   // Volatility (in raw units) widens spread
            w_imbalance: 1.0,      // High imbalance -> slightly wider
            w_interaction: -100.0, // High entropy dampens volatility effect
        }
    }
}

/// Weights for the skew prediction linear model.
///
/// skew_factor = intercept + entropy*w_entropy + volatility*w_volatility
///             + imbalance*w_imbalance + inventory_ratio*w_inventory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkewWeights {
    /// Intercept (base skew factor)
    pub intercept: f64,
    /// Weight for entropy
    pub w_entropy: f64,
    /// Weight for volatility
    pub w_volatility: f64,
    /// Weight for imbalance (positive imbalance = buy pressure -> skew up)
    pub w_imbalance: f64,
    /// Weight for inventory ratio (long inventory -> skew asks down)
    pub w_inventory: f64,
}

impl Default for SkewWeights {
    fn default() -> Self {
        // Default weights for skew:
        // - Base skew ~0.5
        // - Lower skew in high entropy (more mean-reverting)
        // - Inventory is the dominant factor
        Self {
            intercept: 0.5,
            w_entropy: -0.2,      // Less aggressive skew in high entropy
            w_volatility: 50.0,   // Higher vol -> more aggressive skew
            w_imbalance: 0.1,     // Follow the flow slightly
            w_inventory: -0.8,    // Main driver: reduce inventory exposure
        }
    }
}

/// Combined model weights for spread and skew prediction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLModelWeights {
    pub spread: SpreadWeights,
    pub skew: SkewWeights,
    /// Model version for tracking
    pub version: String,
    /// Training metadata (optional)
    pub training_info: Option<TrainingInfo>,
}

/// Information about model training.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingInfo {
    pub trained_on: String,
    pub num_samples: usize,
    pub train_sharpe: f64,
    pub validation_sharpe: Option<f64>,
}

impl Default for MLModelWeights {
    fn default() -> Self {
        Self {
            spread: SpreadWeights::default(),
            skew: SkewWeights::default(),
            version: "1.0.0-baseline".to_string(),
            training_info: None,
        }
    }
}

impl MLModelWeights {
    /// Load weights from a JSON file.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let content = std::fs::read_to_string(path)?;
        let weights: Self = serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(weights)
    }

    /// Save weights to a JSON file.
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), std::io::Error> {
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(path, content)
    }

    /// Create weights with custom spread/skew parameters.
    pub fn with_params(
        spread_intercept: f64,
        spread_entropy: f64,
        skew_intercept: f64,
        skew_inventory: f64,
    ) -> Self {
        Self {
            spread: SpreadWeights {
                intercept: spread_intercept,
                w_entropy: spread_entropy,
                ..Default::default()
            },
            skew: SkewWeights {
                intercept: skew_intercept,
                w_inventory: skew_inventory,
                ..Default::default()
            },
            version: "custom".to_string(),
            training_info: None,
        }
    }
}

// ============================================================================
// Algorithm Configuration
// ============================================================================

/// Configuration for the ML Spread/Skew algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLSpreadSkewConfig {
    /// Maximum inventory position
    pub max_inventory: Decimal,
    /// Quote size per order
    pub quote_size: Decimal,
    /// Minimum spread in bps (floor)
    pub min_spread_bps: f64,
    /// Maximum spread in bps (ceiling)
    pub max_spread_bps: f64,
    /// Minimum skew factor
    pub min_skew: f64,
    /// Maximum skew factor
    pub max_skew: f64,
    /// Entropy threshold below which we don't quote
    pub no_quote_entropy_threshold: f64,
    /// Whether to enable the no-quote gate
    pub enable_no_quote_gate: bool,
    /// Regime thresholds for classification (for logging)
    pub regime_thresholds: RegimeThresholds,
}

impl Default for MLSpreadSkewConfig {
    fn default() -> Self {
        Self {
            max_inventory: dec!(0.1),
            quote_size: dec!(0.001),
            min_spread_bps: 0.5,   // Floor at 0.5 bps
            max_spread_bps: 10.0,  // Ceiling at 10 bps
            min_skew: 0.1,
            max_skew: 1.5,
            no_quote_entropy_threshold: 0.3,
            enable_no_quote_gate: false, // Disabled by default (learned from grid search)
            regime_thresholds: RegimeThresholds::default(),
        }
    }
}

// ============================================================================
// Algorithm Implementation
// ============================================================================

/// ML-based spread/skew predictor algorithm.
///
/// Uses a linear model to predict optimal spread and skew parameters
/// from market features, replacing the fixed regime-based parameters
/// of Avellaneda-Stoikov.
pub struct MLSpreadSkewAlgorithm {
    config: MLSpreadSkewConfig,
    weights: MLModelWeights,

    // State
    inventory: Decimal,
    avg_entry_price: Decimal,
    pnl: PnLTracker,
    current_bid: Option<Quote>,
    current_ask: Option<Quote>,
    recent_fills: VecDeque<Fill>,
    max_fill_history: usize,
}

impl MLSpreadSkewAlgorithm {
    /// Create a new algorithm with the given config and weights.
    pub fn new(config: MLSpreadSkewConfig, weights: MLModelWeights) -> Self {
        Self {
            config,
            weights,
            inventory: dec!(0),
            avg_entry_price: dec!(0),
            pnl: PnLTracker::default(),
            current_bid: None,
            current_ask: None,
            recent_fills: VecDeque::new(),
            max_fill_history: 100,
        }
    }

    /// Create with default configuration and weights.
    pub fn with_defaults() -> Self {
        Self::new(MLSpreadSkewConfig::default(), MLModelWeights::default())
    }

    /// Create with custom weights but default config.
    pub fn with_weights(weights: MLModelWeights) -> Self {
        Self::new(MLSpreadSkewConfig::default(), weights)
    }

    /// Load weights from file.
    pub fn from_weights_file<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let weights = MLModelWeights::load_from_file(path)?;
        Ok(Self::new(MLSpreadSkewConfig::default(), weights))
    }

    /// Get the model weights.
    pub fn weights(&self) -> &MLModelWeights {
        &self.weights
    }

    /// Get the configuration.
    pub fn config(&self) -> &MLSpreadSkewConfig {
        &self.config
    }

    /// Predict spread in basis points.
    fn predict_spread(&self, input: &MarketInput) -> f64 {
        let w = &self.weights.spread;

        let spread = w.intercept
            + w.w_entropy * input.entropy
            + w.w_volatility * input.volatility
            + w.w_imbalance * input.book_imbalance.abs()
            + w.w_interaction * input.entropy * input.volatility;

        // Clamp to configured bounds
        spread.clamp(self.config.min_spread_bps, self.config.max_spread_bps)
    }

    /// Predict skew factor.
    fn predict_skew(&self, input: &MarketInput) -> f64 {
        let w = &self.weights.skew;

        // Calculate inventory ratio: -1 (max short) to +1 (max long)
        let inventory_ratio = if self.config.max_inventory > dec!(0) {
            (self.inventory / self.config.max_inventory)
                .to_f64()
                .unwrap_or(0.0)
                .clamp(-1.0, 1.0)
        } else {
            0.0
        };

        let skew = w.intercept
            + w.w_entropy * input.entropy
            + w.w_volatility * input.volatility
            + w.w_imbalance * input.book_imbalance
            + w.w_inventory * inventory_ratio;

        // Clamp to configured bounds
        skew.clamp(self.config.min_skew, self.config.max_skew)
    }

    /// Determine if we should quote based on entropy.
    fn should_quote(&self, entropy: f64) -> bool {
        if !self.config.enable_no_quote_gate {
            return true;
        }
        entropy >= self.config.no_quote_entropy_threshold
    }

    /// Classify regime for logging purposes.
    fn classify_regime(&self, entropy: f64) -> MarketRegime {
        MarketRegime::from_entropy_score(entropy, &self.config.regime_thresholds)
    }
}

impl MarketMakingAlgorithm for MLSpreadSkewAlgorithm {
    fn algorithm_type(&self) -> AlgorithmType {
        AlgorithmType::MLSpreadSkew
    }

    fn name(&self) -> &'static str {
        "ML Spread/Skew Predictor"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }

    fn compute_quotes(&mut self, input: &MarketInput) -> MMQuotes {
        let regime = self.classify_regime(input.entropy);
        let fair_value = (input.best_bid + input.best_ask) / Decimal::TWO;

        // Check if we should quote
        if !self.should_quote(input.entropy) {
            self.current_bid = None;
            self.current_ask = None;
            return MMQuotes {
                bid: None,
                ask: None,
                regime,
                fair_value,
                half_spread: dec!(0),
                skew: dec!(0),
            };
        }

        // Predict spread and skew using ML model
        let spread_bps = self.predict_spread(input);
        let skew_factor = self.predict_skew(input);

        // Calculate half spread in price units
        let half_spread_decimal = Decimal::from_f64_retain(spread_bps / 10000.0 / 2.0)
            .unwrap_or(dec!(0.0001));
        let half_spread = fair_value * half_spread_decimal;

        // Calculate inventory skew
        let inventory_ratio = if self.config.max_inventory > dec!(0) {
            self.inventory / self.config.max_inventory
        } else {
            dec!(0)
        };

        let skew_decimal = Decimal::from_f64_retain(skew_factor).unwrap_or(dec!(0.5));
        let inventory_skew = half_spread * inventory_ratio * skew_decimal;
        let total_skew = inventory_skew;

        // Calculate bid and ask prices
        let bid_price = fair_value - half_spread - total_skew;
        let ask_price = fair_value + half_spread - total_skew;

        // Determine quote sizes
        let bid_size = self.config.quote_size;
        let ask_size = self.config.quote_size;

        // Check inventory limits
        let at_max_inventory = self.inventory.abs() >= self.config.max_inventory;

        let bid = if at_max_inventory && self.inventory > dec!(0) {
            None // Don't buy more when max long
        } else {
            Some(Quote {
                price: bid_price.round_dp(2),
                size: bid_size,
                side: QuoteSide::Bid,
                timestamp_ms: input.timestamp_ms,
            })
        };

        let ask = if at_max_inventory && self.inventory < dec!(0) {
            None // Don't sell more when max short
        } else {
            Some(Quote {
                price: ask_price.round_dp(2),
                size: ask_size,
                side: QuoteSide::Ask,
                timestamp_ms: input.timestamp_ms,
            })
        };

        self.current_bid = bid.clone();
        self.current_ask = ask.clone();

        MMQuotes {
            bid,
            ask,
            regime,
            fair_value,
            half_spread,
            skew: total_skew,
        }
    }

    fn process_fill(&mut self, fill: Fill, fee_rate: Decimal) {
        let fill_value = fill.price * fill.size;
        let fee = fill_value * fee_rate;

        match fill.side {
            QuoteSide::Bid => {
                let old_value = self.inventory * self.avg_entry_price;
                let new_value = fill.price * fill.size;
                self.inventory += fill.size;

                if self.inventory != dec!(0) {
                    self.avg_entry_price = (old_value + new_value) / self.inventory;
                }
            }
            QuoteSide::Ask => {
                if self.inventory > dec!(0) {
                    let pnl = (fill.price - self.avg_entry_price) * fill.size;
                    self.pnl.realized_pnl += pnl;
                }
                self.inventory -= fill.size;

                if self.inventory < dec!(0) {
                    self.avg_entry_price = fill.price;
                }
            }
        }

        self.pnl.fees_paid += fee;
        self.pnl.realized_pnl -= fee;
        self.pnl.num_trades += 1;
        self.pnl.total_volume += fill.size;

        self.recent_fills.push_back(fill);
        if self.recent_fills.len() > self.max_fill_history {
            self.recent_fills.pop_front();
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
            current_bid: self.current_bid.clone(),
            current_ask: self.current_ask.clone(),
        }
    }

    fn inventory(&self) -> Decimal {
        self.inventory
    }

    fn pnl(&self) -> &PnLTracker {
        &self.pnl
    }

    fn reset(&mut self) {
        self.inventory = dec!(0);
        self.avg_entry_price = dec!(0);
        self.pnl = PnLTracker::default();
        self.current_bid = None;
        self.current_ask = None;
        self.recent_fills.clear();
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
            "model_version": self.weights.version,
            "max_inventory": self.config.max_inventory.to_string(),
            "quote_size": self.config.quote_size.to_string(),
            "spread_bounds": {
                "min_bps": self.config.min_spread_bps,
                "max_bps": self.config.max_spread_bps,
            },
            "skew_bounds": {
                "min": self.config.min_skew,
                "max": self.config.max_skew,
            },
            "no_quote_gate": {
                "enabled": self.config.enable_no_quote_gate,
                "threshold": self.config.no_quote_entropy_threshold,
            },
            "weights": {
                "spread": {
                    "intercept": self.weights.spread.intercept,
                    "w_entropy": self.weights.spread.w_entropy,
                    "w_volatility": self.weights.spread.w_volatility,
                    "w_imbalance": self.weights.spread.w_imbalance,
                    "w_interaction": self.weights.spread.w_interaction,
                },
                "skew": {
                    "intercept": self.weights.skew.intercept,
                    "w_entropy": self.weights.skew.w_entropy,
                    "w_volatility": self.weights.skew.w_volatility,
                    "w_imbalance": self.weights.skew.w_imbalance,
                    "w_inventory": self.weights.skew.w_inventory,
                },
            },
        })
    }
}

impl std::fmt::Debug for MLSpreadSkewAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MLSpreadSkewAlgorithm")
            .field("type", &self.algorithm_type())
            .field("model_version", &self.weights.version)
            .field("inventory", &self.inventory)
            .field("max_inventory", &self.config.max_inventory)
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_input(entropy: f64, volatility: f64, imbalance: f64) -> MarketInput {
        MarketInput {
            best_bid: dec!(50000),
            best_ask: dec!(50100),
            volatility,
            entropy,
            book_imbalance: imbalance,
            timestamp_ms: 1000,
        }
    }

    #[test]
    fn test_algorithm_type() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
        assert_eq!(algo.type_string(), "ml_spread_skew");
        assert_eq!(algo.name(), "ML Spread/Skew Predictor");
    }

    #[test]
    fn test_default_weights() {
        let weights = MLModelWeights::default();
        assert_eq!(weights.version, "1.0.0-baseline");
        assert!(weights.spread.intercept > 0.0);
        assert!(weights.skew.intercept > 0.0);
    }

    #[test]
    fn test_predict_spread_high_entropy() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        let input = create_test_input(0.9, 0.001, 0.0);

        let spread = algo.predict_spread(&input);

        // High entropy should result in tighter spread
        assert!(spread < 3.0); // Less than base
        assert!(spread >= algo.config.min_spread_bps);
    }

    #[test]
    fn test_predict_spread_low_entropy() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        let input = create_test_input(0.2, 0.001, 0.0);

        let spread = algo.predict_spread(&input);

        // Low entropy should result in wider spread
        assert!(spread > 2.0);
    }

    #[test]
    fn test_predict_spread_high_volatility() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        let input_low_vol = create_test_input(0.5, 0.001, 0.0);
        let input_high_vol = create_test_input(0.5, 0.01, 0.0);

        let spread_low = algo.predict_spread(&input_low_vol);
        let spread_high = algo.predict_spread(&input_high_vol);

        // Higher volatility should widen spread
        assert!(spread_high > spread_low);
    }

    #[test]
    fn test_predict_skew_with_inventory() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        let input = create_test_input(0.5, 0.001, 0.0);

        // No inventory
        let skew_neutral = algo.predict_skew(&input);

        // Long inventory
        algo.inventory = dec!(0.05); // 50% of max
        let skew_long = algo.predict_skew(&input);

        // Short inventory
        algo.inventory = dec!(-0.05);
        let skew_short = algo.predict_skew(&input);

        // Long inventory should reduce skew (encourage selling)
        assert!(skew_long < skew_neutral);
        // Short inventory should increase skew (encourage buying)
        assert!(skew_short > skew_neutral);
    }

    #[test]
    fn test_compute_quotes_generates_quotes() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        let input = create_test_input(0.8, 0.001, 0.0);

        let quotes = algo.compute_quotes(&input);

        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();
        assert!(ask.price > bid.price);
    }

    #[test]
    fn test_no_quote_gate() {
        let mut config = MLSpreadSkewConfig::default();
        config.enable_no_quote_gate = true;
        config.no_quote_entropy_threshold = 0.4;

        let mut algo = MLSpreadSkewAlgorithm::new(config, MLModelWeights::default());

        // Low entropy - should not quote
        let input_low = create_test_input(0.2, 0.001, 0.0);
        let quotes_low = algo.compute_quotes(&input_low);
        assert!(quotes_low.bid.is_none());
        assert!(quotes_low.ask.is_none());

        // High entropy - should quote
        let input_high = create_test_input(0.8, 0.001, 0.0);
        let quotes_high = algo.compute_quotes(&input_high);
        assert!(quotes_high.bid.is_some());
        assert!(quotes_high.ask.is_some());
    }

    #[test]
    fn test_inventory_limits() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        algo.inventory = algo.config.max_inventory; // Max long

        let input = create_test_input(0.8, 0.001, 0.0);
        let quotes = algo.compute_quotes(&input);

        // Should not have bid (can't buy more)
        assert!(quotes.bid.is_none());
        // Should have ask (can sell)
        assert!(quotes.ask.is_some());
    }

    #[test]
    fn test_process_fill() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        let fill = Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        };

        algo.process_fill(fill, dec!(0.0001));

        assert_eq!(algo.inventory, dec!(0.01));
        assert_eq!(algo.pnl.num_trades, 1);
    }

    #[test]
    fn test_reset() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Make some trades
        algo.inventory = dec!(0.05);
        algo.pnl.num_trades = 10;

        algo.reset();

        assert_eq!(algo.inventory, dec!(0));
        assert_eq!(algo.pnl.num_trades, 0);
    }

    #[test]
    fn test_weights_serialization() {
        let weights = MLModelWeights::default();
        let json = serde_json::to_string(&weights).unwrap();
        let parsed: MLModelWeights = serde_json::from_str(&json).unwrap();

        assert_eq!(weights.version, parsed.version);
        assert_eq!(weights.spread.intercept, parsed.spread.intercept);
    }

    #[test]
    fn test_parameters_json() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        let json = algo.parameters_json();

        assert_eq!(json["algorithm"], "ml_spread_skew");
        assert!(json["weights"]["spread"]["intercept"].is_number());
    }

    #[test]
    fn test_spread_bounds_clamping() {
        let mut config = MLSpreadSkewConfig::default();
        config.min_spread_bps = 1.0;
        config.max_spread_bps = 5.0;

        // Create weights that would predict extreme values
        let mut weights = MLModelWeights::default();
        weights.spread.intercept = 100.0; // Very high

        let algo = MLSpreadSkewAlgorithm::new(config.clone(), weights);
        let input = create_test_input(0.5, 0.001, 0.0);

        let spread = algo.predict_spread(&input);
        assert!(spread <= config.max_spread_bps);
        assert!(spread >= config.min_spread_bps);
    }

    #[test]
    fn test_trait_object_usage() {
        let algo: Box<dyn MarketMakingAlgorithm> =
            Box::new(MLSpreadSkewAlgorithm::with_defaults());

        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
        assert_eq!(algo.type_string(), "ml_spread_skew");
    }
}

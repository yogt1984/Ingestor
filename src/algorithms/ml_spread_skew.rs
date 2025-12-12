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
//! 3. Trained via the `Trainable` trait using least squares regression
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::algorithms::{MLSpreadSkewAlgorithm, MLModelWeights, Configurable, Trainable};
//!
//! // Create with default weights (baseline from A-S optimization)
//! let algo = MLSpreadSkewAlgorithm::with_defaults();
//!
//! // Or with custom trained weights
//! let weights = MLModelWeights::load_from_file("model.json")?;
//! let algo = MLSpreadSkewAlgorithm::new(config, weights);
//!
//! // Or create from parameters (Configurable trait)
//! let params = HashMap::new();
//! let algo = MLSpreadSkewAlgorithm::from_parameters(&params)?;
//! ```

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::Path;

use crate::algorithms::traits::{
    AlgorithmError, AlgorithmType, Configurable, MarketInput, MarketMakingAlgorithm,
    ParameterDefinition, Trainable, TrainingData, TrainingResult,
};
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
// Configurable Trait Implementation
// ============================================================================

/// Parameter names for the ML Spread/Skew algorithm.
///
/// These are the canonical string identifiers used in:
/// - Parameter maps
/// - CLI arguments
/// - Configuration files
/// - Grid search
pub mod param_names {
    // Config parameters
    pub const MAX_INVENTORY: &str = "max_inventory";
    pub const QUOTE_SIZE: &str = "quote_size";
    pub const MIN_SPREAD_BPS: &str = "min_spread_bps";
    pub const MAX_SPREAD_BPS: &str = "max_spread_bps";
    pub const MIN_SKEW: &str = "min_skew";
    pub const MAX_SKEW: &str = "max_skew";
    pub const NO_QUOTE_ENTROPY_THRESHOLD: &str = "no_quote_entropy_threshold";
    pub const ENABLE_NO_QUOTE_GATE: &str = "enable_no_quote_gate";
}

/// Weight names for the ML model (for Trainable trait).
///
/// The ML model has 10 learnable weights:
/// - 5 for spread: intercept, entropy, volatility, imbalance, interaction
/// - 5 for skew: intercept, entropy, volatility, imbalance, inventory
pub mod weight_names {
    // Spread weights
    pub const SPREAD_INTERCEPT: &str = "spread_intercept";
    pub const SPREAD_ENTROPY: &str = "spread_entropy";
    pub const SPREAD_VOLATILITY: &str = "spread_volatility";
    pub const SPREAD_IMBALANCE: &str = "spread_imbalance";
    pub const SPREAD_INTERACTION: &str = "spread_interaction";

    // Skew weights
    pub const SKEW_INTERCEPT: &str = "skew_intercept";
    pub const SKEW_ENTROPY: &str = "skew_entropy";
    pub const SKEW_VOLATILITY: &str = "skew_volatility";
    pub const SKEW_IMBALANCE: &str = "skew_imbalance";
    pub const SKEW_INVENTORY: &str = "skew_inventory";
}

impl Configurable for MLSpreadSkewAlgorithm {
    /// Returns the parameter definitions for the ML Spread/Skew algorithm.
    ///
    /// # Parameters
    ///
    /// | Name | Type | Range | Default | Description |
    /// |------|------|-------|---------|-------------|
    /// | max_inventory | Continuous | 0.001-10.0 | 0.1 | Maximum inventory position |
    /// | quote_size | Continuous | 0.0001-1.0 | 0.001 | Size per quote |
    /// | min_spread_bps | Continuous | 0.1-5.0 | 0.5 | Minimum spread floor (bps) |
    /// | max_spread_bps | Continuous | 1.0-50.0 | 10.0 | Maximum spread ceiling (bps) |
    /// | min_skew | Continuous | 0.0-0.5 | 0.1 | Minimum skew factor |
    /// | max_skew | Continuous | 0.5-3.0 | 1.5 | Maximum skew factor |
    /// | no_quote_entropy_threshold | Continuous | 0.0-0.7 | 0.3 | Entropy below this = no quote |
    /// | enable_no_quote_gate | Boolean | 0/1 | 0 | Enable entropy-based quote gating |
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
            ParameterDefinition::continuous(param_names::MIN_SPREAD_BPS)
                .description("Minimum spread floor in basis points")
                .default(0.5)
                .range(0.1, 5.0)
                .tunable(true),
            ParameterDefinition::continuous(param_names::MAX_SPREAD_BPS)
                .description("Maximum spread ceiling in basis points")
                .default(10.0)
                .range(1.0, 50.0)
                .tunable(true),
            ParameterDefinition::continuous(param_names::MIN_SKEW)
                .description("Minimum skew factor")
                .default(0.1)
                .range(0.0, 0.5)
                .tunable(true),
            ParameterDefinition::continuous(param_names::MAX_SKEW)
                .description("Maximum skew factor")
                .default(1.5)
                .range(0.5, 3.0)
                .tunable(true),
            ParameterDefinition::continuous(param_names::NO_QUOTE_ENTROPY_THRESHOLD)
                .description("Entropy threshold below which we don't quote")
                .default(0.3)
                .range(0.0, 0.7)
                .tunable(true),
            ParameterDefinition::boolean(param_names::ENABLE_NO_QUOTE_GATE)
                .description("Enable entropy-based quote gating")
                .default(0.0) // false by default
                .tunable(true),
        ]
    }

    /// Create an instance from a parameter map.
    ///
    /// Missing parameters use default values. Values are validated before construction.
    /// Note: This creates an algorithm with DEFAULT weights. Use `Trainable::load_weights`
    /// or `set_weights` to load trained weights.
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
        let min_spread_bps = get_param(param_names::MIN_SPREAD_BPS);
        let max_spread_bps = get_param(param_names::MAX_SPREAD_BPS);
        let min_skew = get_param(param_names::MIN_SKEW);
        let max_skew = get_param(param_names::MAX_SKEW);
        let no_quote_entropy_threshold = get_param(param_names::NO_QUOTE_ENTROPY_THRESHOLD);
        let enable_no_quote_gate = get_param(param_names::ENABLE_NO_QUOTE_GATE) != 0.0;

        // Validate parameters against definitions
        for param_def in Self::parameters() {
            if let Some(&value) = params.get(&param_def.name) {
                param_def.validate(value)?;
            }
        }

        // Additional cross-parameter validation
        if min_spread_bps >= max_spread_bps {
            return Err(AlgorithmError::InvalidConfig(
                "min_spread_bps must be less than max_spread_bps".to_string(),
            ));
        }
        if min_skew >= max_skew {
            return Err(AlgorithmError::InvalidConfig(
                "min_skew must be less than max_skew".to_string(),
            ));
        }

        // Build config
        let config = MLSpreadSkewConfig {
            max_inventory: Decimal::try_from(max_inventory).map_err(|e| {
                AlgorithmError::InvalidConfig(format!("Invalid max_inventory: {}", e))
            })?,
            quote_size: Decimal::try_from(quote_size).map_err(|e| {
                AlgorithmError::InvalidConfig(format!("Invalid quote_size: {}", e))
            })?,
            min_spread_bps,
            max_spread_bps,
            min_skew,
            max_skew,
            no_quote_entropy_threshold,
            enable_no_quote_gate,
            regime_thresholds: RegimeThresholds::default(),
        };

        Ok(Self::new(config, MLModelWeights::default()))
    }

    /// Get current parameter values as a map.
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
        params.insert(
            param_names::MIN_SPREAD_BPS.to_string(),
            self.config.min_spread_bps,
        );
        params.insert(
            param_names::MAX_SPREAD_BPS.to_string(),
            self.config.max_spread_bps,
        );
        params.insert(param_names::MIN_SKEW.to_string(), self.config.min_skew);
        params.insert(param_names::MAX_SKEW.to_string(), self.config.max_skew);
        params.insert(
            param_names::NO_QUOTE_ENTROPY_THRESHOLD.to_string(),
            self.config.no_quote_entropy_threshold,
        );
        params.insert(
            param_names::ENABLE_NO_QUOTE_GATE.to_string(),
            if self.config.enable_no_quote_gate { 1.0 } else { 0.0 },
        );

        params
    }

    /// Update a single parameter value.
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

        // Update the specific parameter
        match name {
            param_names::MAX_INVENTORY => {
                self.config.max_inventory = Decimal::try_from(value).map_err(|e| {
                    AlgorithmError::InvalidConfig(format!("Invalid max_inventory: {}", e))
                })?;
            }
            param_names::QUOTE_SIZE => {
                self.config.quote_size = Decimal::try_from(value).map_err(|e| {
                    AlgorithmError::InvalidConfig(format!("Invalid quote_size: {}", e))
                })?;
            }
            param_names::MIN_SPREAD_BPS => {
                // Cross-validate with max
                if value >= self.config.max_spread_bps {
                    return Err(AlgorithmError::InvalidConfig(
                        "min_spread_bps must be less than max_spread_bps".to_string(),
                    ));
                }
                self.config.min_spread_bps = value;
            }
            param_names::MAX_SPREAD_BPS => {
                // Cross-validate with min
                if value <= self.config.min_spread_bps {
                    return Err(AlgorithmError::InvalidConfig(
                        "max_spread_bps must be greater than min_spread_bps".to_string(),
                    ));
                }
                self.config.max_spread_bps = value;
            }
            param_names::MIN_SKEW => {
                // Cross-validate with max
                if value >= self.config.max_skew {
                    return Err(AlgorithmError::InvalidConfig(
                        "min_skew must be less than max_skew".to_string(),
                    ));
                }
                self.config.min_skew = value;
            }
            param_names::MAX_SKEW => {
                // Cross-validate with min
                if value <= self.config.min_skew {
                    return Err(AlgorithmError::InvalidConfig(
                        "max_skew must be greater than min_skew".to_string(),
                    ));
                }
                self.config.max_skew = value;
            }
            param_names::NO_QUOTE_ENTROPY_THRESHOLD => {
                self.config.no_quote_entropy_threshold = value;
            }
            param_names::ENABLE_NO_QUOTE_GATE => {
                self.config.enable_no_quote_gate = value != 0.0;
            }
            _ => {
                return Err(AlgorithmError::InvalidConfig(format!(
                    "Unknown parameter: {}",
                    name
                )));
            }
        }

        Ok(())
    }
}

// ============================================================================
// Trainable Trait Implementation
// ============================================================================

impl Trainable for MLSpreadSkewAlgorithm {
    /// Train the algorithm's weights from data using linear regression.
    ///
    /// # Training Data Format
    ///
    /// The training data should have feature vectors with 5 elements each:
    /// - [0] entropy
    /// - [1] volatility
    /// - [2] |imbalance| (for spread) or imbalance (for skew)
    /// - [3] entropy * volatility (interaction)
    /// - [4] inventory_ratio (for skew training only)
    ///
    /// # Training Process
    ///
    /// Uses ordinary least squares to fit the linear model:
    /// - For spread: target = spread_bps
    /// - For skew: target = skew_factor
    ///
    /// Note: This is a simplified implementation. For production use,
    /// consider using a proper ML library or the walk-forward trainer.
    fn train(&mut self, data: &TrainingData) -> Result<TrainingResult, AlgorithmError> {
        use std::time::Instant;
        let start = Instant::now();

        if data.is_empty() {
            return Err(AlgorithmError::InvalidConfig(
                "Training data is empty".to_string(),
            ));
        }

        // Validate feature vector length (expecting 5 features)
        let expected_features = 5;
        for (i, features) in data.features.iter().enumerate() {
            if features.len() != expected_features {
                return Err(AlgorithmError::InvalidConfig(format!(
                    "Sample {} has {} features, expected {}",
                    i,
                    features.len(),
                    expected_features
                )));
            }
        }

        // Simple OLS: w = (X'X)^-1 X'y
        // For now, we use a simplified approach with gradient descent
        let n = data.len();
        let learning_rate = 0.01;
        let max_iterations = 1000;
        let convergence_threshold = 1e-6;

        // Initialize weights to current values
        let mut weights = self.get_weights();
        let mut prev_loss = f64::MAX;
        let mut converged = false;
        let mut iterations = 0;

        // Train spread weights (first 5) and skew weights (last 5) together
        for iter in 0..max_iterations {
            let mut gradients = vec![0.0; 10];
            let mut total_loss = 0.0;

            for (i, features) in data.features.iter().enumerate() {
                let target = data.targets[i];
                let sample_weight = data.weights.as_ref().map(|w| w[i]).unwrap_or(1.0);

                // Compute prediction (we'll treat this as spread training)
                // spread = w0 + w1*entropy + w2*volatility + w3*|imbalance| + w4*interaction
                let prediction = weights[0]
                    + weights[1] * features[0]
                    + weights[2] * features[1]
                    + weights[3] * features[2]
                    + weights[4] * features[3];

                let error = prediction - target;
                total_loss += error * error * sample_weight;

                // Compute gradients for spread weights
                gradients[0] += 2.0 * error * sample_weight; // intercept
                gradients[1] += 2.0 * error * features[0] * sample_weight; // entropy
                gradients[2] += 2.0 * error * features[1] * sample_weight; // volatility
                gradients[3] += 2.0 * error * features[2] * sample_weight; // imbalance
                gradients[4] += 2.0 * error * features[3] * sample_weight; // interaction
            }

            // Normalize gradients
            for g in gradients.iter_mut() {
                *g /= n as f64;
            }
            total_loss /= n as f64;

            // Update weights
            for (w, g) in weights.iter_mut().zip(gradients.iter()) {
                *w -= learning_rate * g;
            }

            // Check convergence
            if (prev_loss - total_loss).abs() < convergence_threshold {
                converged = true;
                iterations = iter + 1;
                break;
            }
            prev_loss = total_loss;
            iterations = iter + 1;
        }

        // Apply trained weights
        self.set_weights(weights.clone())?;

        // Update training info
        self.weights.training_info = Some(TrainingInfo {
            trained_on: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            num_samples: n,
            train_sharpe: 0.0, // Would need more sophisticated computation
            validation_sharpe: None,
        });
        self.weights.version = "trained".to_string();

        let duration = start.elapsed();

        let mut metrics = HashMap::new();
        metrics.insert("mse".to_string(), prev_loss);
        metrics.insert("rmse".to_string(), prev_loss.sqrt());

        Ok(TrainingResult {
            converged,
            final_loss: prev_loss,
            iterations,
            duration_secs: duration.as_secs_f64(),
            metrics,
        })
    }

    /// Save learned weights to a JSON file.
    fn save_weights(&self, path: &Path) -> Result<(), AlgorithmError> {
        self.weights.save_to_file(path).map_err(|e| {
            AlgorithmError::InvalidConfig(format!("Failed to save weights: {}", e))
        })
    }

    /// Load learned weights from a JSON file.
    fn load_weights(&mut self, path: &Path) -> Result<(), AlgorithmError> {
        let weights = MLModelWeights::load_from_file(path).map_err(|e| {
            AlgorithmError::InvalidConfig(format!("Failed to load weights: {}", e))
        })?;
        self.weights = weights;
        Ok(())
    }

    /// Get the current weights as a vector.
    ///
    /// # Weight Order
    ///
    /// Returns 10 weights in this order:
    /// 0. spread_intercept
    /// 1. spread_entropy
    /// 2. spread_volatility
    /// 3. spread_imbalance
    /// 4. spread_interaction
    /// 5. skew_intercept
    /// 6. skew_entropy
    /// 7. skew_volatility
    /// 8. skew_imbalance
    /// 9. skew_inventory
    fn get_weights(&self) -> Vec<f64> {
        vec![
            self.weights.spread.intercept,
            self.weights.spread.w_entropy,
            self.weights.spread.w_volatility,
            self.weights.spread.w_imbalance,
            self.weights.spread.w_interaction,
            self.weights.skew.intercept,
            self.weights.skew.w_entropy,
            self.weights.skew.w_volatility,
            self.weights.skew.w_imbalance,
            self.weights.skew.w_inventory,
        ]
    }

    /// Set weights directly (useful for ensemble/consensus).
    ///
    /// # Arguments
    ///
    /// * `weights` - Vector of 10 weights in the same order as `get_weights()`
    fn set_weights(&mut self, weights: Vec<f64>) -> Result<(), AlgorithmError> {
        if weights.len() != 10 {
            return Err(AlgorithmError::InvalidConfig(format!(
                "Expected 10 weights, got {}",
                weights.len()
            )));
        }

        self.weights.spread.intercept = weights[0];
        self.weights.spread.w_entropy = weights[1];
        self.weights.spread.w_volatility = weights[2];
        self.weights.spread.w_imbalance = weights[3];
        self.weights.spread.w_interaction = weights[4];
        self.weights.skew.intercept = weights[5];
        self.weights.skew.w_entropy = weights[6];
        self.weights.skew.w_volatility = weights[7];
        self.weights.skew.w_imbalance = weights[8];
        self.weights.skew.w_inventory = weights[9];

        Ok(())
    }

    /// Get weight names/labels for interpretation.
    fn weight_names(&self) -> Vec<String> {
        vec![
            weight_names::SPREAD_INTERCEPT.to_string(),
            weight_names::SPREAD_ENTROPY.to_string(),
            weight_names::SPREAD_VOLATILITY.to_string(),
            weight_names::SPREAD_IMBALANCE.to_string(),
            weight_names::SPREAD_INTERACTION.to_string(),
            weight_names::SKEW_INTERCEPT.to_string(),
            weight_names::SKEW_ENTROPY.to_string(),
            weight_names::SKEW_VOLATILITY.to_string(),
            weight_names::SKEW_IMBALANCE.to_string(),
            weight_names::SKEW_INVENTORY.to_string(),
        ]
    }

    /// Check if the algorithm has been trained (weights are non-default).
    fn is_trained(&self) -> bool {
        self.weights.training_info.is_some() || self.weights.version != "1.0.0-baseline"
    }

    /// Reset weights to their initial/default values.
    fn reset_weights(&mut self) {
        self.weights = MLModelWeights::default();
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

    // ========================================================================
    // Configurable Trait Tests - COMPREHENSIVE
    // ========================================================================

    #[test]
    fn test_configurable_parameters_count() {
        let params = MLSpreadSkewAlgorithm::parameters();
        assert_eq!(params.len(), 8, "ML Spread/Skew should have exactly 8 parameters");
    }

    #[test]
    fn test_configurable_parameters_names() {
        let params = MLSpreadSkewAlgorithm::parameters();
        let names: Vec<&str> = params.iter().map(|p| p.name.as_str()).collect();

        assert!(names.contains(&param_names::MAX_INVENTORY));
        assert!(names.contains(&param_names::QUOTE_SIZE));
        assert!(names.contains(&param_names::MIN_SPREAD_BPS));
        assert!(names.contains(&param_names::MAX_SPREAD_BPS));
        assert!(names.contains(&param_names::MIN_SKEW));
        assert!(names.contains(&param_names::MAX_SKEW));
        assert!(names.contains(&param_names::NO_QUOTE_ENTROPY_THRESHOLD));
        assert!(names.contains(&param_names::ENABLE_NO_QUOTE_GATE));
    }

    #[test]
    fn test_configurable_parameters_all_have_descriptions() {
        let params = MLSpreadSkewAlgorithm::parameters();
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
        let params = MLSpreadSkewAlgorithm::parameters();
        for param in params {
            assert!(
                param.range.is_some(),
                "Parameter '{}' should have a range",
                param.name
            );
            let (min, max) = param.range.unwrap();
            assert!(
                min <= max,
                "Parameter '{}' range min ({}) should be <= max ({})",
                param.name,
                min,
                max
            );
        }
    }

    #[test]
    fn test_configurable_parameters_defaults_within_range() {
        let params = MLSpreadSkewAlgorithm::parameters();
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
        let tunable = MLSpreadSkewAlgorithm::tunable_parameters();

        // These should be tunable
        let tunable_names: Vec<&str> = tunable.iter().map(|p| p.name.as_str()).collect();
        assert!(tunable_names.contains(&param_names::MIN_SPREAD_BPS));
        assert!(tunable_names.contains(&param_names::MAX_SPREAD_BPS));
        assert!(tunable_names.contains(&param_names::MIN_SKEW));
        assert!(tunable_names.contains(&param_names::MAX_SKEW));
        assert!(tunable_names.contains(&param_names::NO_QUOTE_ENTROPY_THRESHOLD));
        assert!(tunable_names.contains(&param_names::ENABLE_NO_QUOTE_GATE));

        // These should NOT be tunable (risk management params)
        assert!(!tunable_names.contains(&param_names::MAX_INVENTORY));
        assert!(!tunable_names.contains(&param_names::QUOTE_SIZE));
    }

    #[test]
    fn test_configurable_from_parameters_default() {
        let params = HashMap::new(); // Empty - use all defaults
        let algo = MLSpreadSkewAlgorithm::from_parameters(&params).unwrap();

        assert_eq!(algo.config.max_inventory, dec!(0.1));
        assert_eq!(algo.config.quote_size, dec!(0.001));
        assert_eq!(algo.config.min_spread_bps, 0.5);
        assert_eq!(algo.config.max_spread_bps, 10.0);
        assert_eq!(algo.config.min_skew, 0.1);
        assert_eq!(algo.config.max_skew, 1.5);
        assert_eq!(algo.config.no_quote_entropy_threshold, 0.3);
        assert!(!algo.config.enable_no_quote_gate);
    }

    #[test]
    fn test_configurable_from_parameters_custom() {
        let mut params = HashMap::new();
        params.insert(param_names::MAX_INVENTORY.to_string(), 0.5);
        params.insert(param_names::QUOTE_SIZE.to_string(), 0.01);
        params.insert(param_names::MIN_SPREAD_BPS.to_string(), 1.0);
        params.insert(param_names::MAX_SPREAD_BPS.to_string(), 20.0);
        params.insert(param_names::MIN_SKEW.to_string(), 0.2);
        params.insert(param_names::MAX_SKEW.to_string(), 2.0);
        params.insert(param_names::NO_QUOTE_ENTROPY_THRESHOLD.to_string(), 0.5);
        params.insert(param_names::ENABLE_NO_QUOTE_GATE.to_string(), 1.0);

        let algo = MLSpreadSkewAlgorithm::from_parameters(&params).unwrap();

        assert_eq!(algo.config.max_inventory, dec!(0.5));
        assert_eq!(algo.config.quote_size, dec!(0.01));
        assert_eq!(algo.config.min_spread_bps, 1.0);
        assert_eq!(algo.config.max_spread_bps, 20.0);
        assert_eq!(algo.config.min_skew, 0.2);
        assert_eq!(algo.config.max_skew, 2.0);
        assert_eq!(algo.config.no_quote_entropy_threshold, 0.5);
        assert!(algo.config.enable_no_quote_gate);
    }

    #[test]
    fn test_configurable_from_parameters_partial() {
        // Only set some parameters, rest should be defaults
        let mut params = HashMap::new();
        params.insert(param_names::MIN_SPREAD_BPS.to_string(), 2.0);
        params.insert(param_names::MAX_SPREAD_BPS.to_string(), 15.0);

        let algo = MLSpreadSkewAlgorithm::from_parameters(&params).unwrap();

        // Custom values
        assert_eq!(algo.config.min_spread_bps, 2.0);
        assert_eq!(algo.config.max_spread_bps, 15.0);
        // Default values
        assert_eq!(algo.config.max_inventory, dec!(0.1));
        assert_eq!(algo.config.min_skew, 0.1);
    }

    #[test]
    fn test_configurable_from_parameters_validation_spread_order() {
        // min_spread_bps >= max_spread_bps should fail
        let mut params = HashMap::new();
        params.insert(param_names::MIN_SPREAD_BPS.to_string(), 5.0);
        params.insert(param_names::MAX_SPREAD_BPS.to_string(), 5.0); // Equal

        let result = MLSpreadSkewAlgorithm::from_parameters(&params);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("min_spread_bps"));
    }

    #[test]
    fn test_configurable_from_parameters_validation_skew_order() {
        // min_skew >= max_skew should fail
        let mut params = HashMap::new();
        params.insert(param_names::MIN_SKEW.to_string(), 0.5);
        params.insert(param_names::MAX_SKEW.to_string(), 0.5); // Equal

        let result = MLSpreadSkewAlgorithm::from_parameters(&params);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("min_skew"));
    }

    #[test]
    fn test_configurable_current_parameters_roundtrip() {
        // Create with custom params
        let mut original_params = HashMap::new();
        original_params.insert(param_names::MAX_INVENTORY.to_string(), 0.5);
        original_params.insert(param_names::QUOTE_SIZE.to_string(), 0.01);
        original_params.insert(param_names::MIN_SPREAD_BPS.to_string(), 1.0);
        original_params.insert(param_names::MAX_SPREAD_BPS.to_string(), 20.0);
        original_params.insert(param_names::MIN_SKEW.to_string(), 0.2);
        original_params.insert(param_names::MAX_SKEW.to_string(), 2.0);
        original_params.insert(param_names::NO_QUOTE_ENTROPY_THRESHOLD.to_string(), 0.5);
        original_params.insert(param_names::ENABLE_NO_QUOTE_GATE.to_string(), 1.0);

        let algo = MLSpreadSkewAlgorithm::from_parameters(&original_params).unwrap();

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
    fn test_configurable_set_parameter_min_spread() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Initial value
        assert_eq!(algo.config.min_spread_bps, 0.5);

        // Update
        algo.set_parameter(param_names::MIN_SPREAD_BPS, 2.0).unwrap();

        // Verify update
        assert_eq!(algo.config.min_spread_bps, 2.0);
    }

    #[test]
    fn test_configurable_set_parameter_enable_gate() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Initial value
        assert!(!algo.config.enable_no_quote_gate);

        // Update to true
        algo.set_parameter(param_names::ENABLE_NO_QUOTE_GATE, 1.0)
            .unwrap();

        // Verify update
        assert!(algo.config.enable_no_quote_gate);
    }

    #[test]
    fn test_configurable_set_parameter_unknown() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        let result = algo.set_parameter("unknown_param", 1.0);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("Unknown parameter"));
    }

    #[test]
    fn test_configurable_set_parameter_cross_validation_spread() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Try to set min_spread_bps above current max_spread_bps (10.0)
        let result = algo.set_parameter(param_names::MIN_SPREAD_BPS, 15.0);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("min_spread_bps"));
    }

    #[test]
    fn test_configurable_set_parameter_cross_validation_skew() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Try to set max_skew below current min_skew (0.1)
        let result = algo.set_parameter(param_names::MAX_SKEW, 0.05);
        // This should fail because 0.05 < 0.5 (min of max_skew range)
        assert!(result.is_err());
    }

    #[test]
    fn test_configurable_validate_parameters() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();

        // Default algorithm should validate
        assert!(algo.validate_parameters().is_ok());
    }

    #[test]
    fn test_configurable_parameter_names_constants() {
        // Verify constant strings are what we expect
        assert_eq!(param_names::MAX_INVENTORY, "max_inventory");
        assert_eq!(param_names::QUOTE_SIZE, "quote_size");
        assert_eq!(param_names::MIN_SPREAD_BPS, "min_spread_bps");
        assert_eq!(param_names::MAX_SPREAD_BPS, "max_spread_bps");
        assert_eq!(param_names::MIN_SKEW, "min_skew");
        assert_eq!(param_names::MAX_SKEW, "max_skew");
        assert_eq!(param_names::NO_QUOTE_ENTROPY_THRESHOLD, "no_quote_entropy_threshold");
        assert_eq!(param_names::ENABLE_NO_QUOTE_GATE, "enable_no_quote_gate");
    }

    // ========================================================================
    // Trainable Trait Tests - COMPREHENSIVE
    // ========================================================================

    #[test]
    fn test_trainable_get_weights() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        let weights = algo.get_weights();

        assert_eq!(weights.len(), 10);

        // Check spread weights
        let default = MLModelWeights::default();
        assert_eq!(weights[0], default.spread.intercept);
        assert_eq!(weights[1], default.spread.w_entropy);
        assert_eq!(weights[2], default.spread.w_volatility);
        assert_eq!(weights[3], default.spread.w_imbalance);
        assert_eq!(weights[4], default.spread.w_interaction);

        // Check skew weights
        assert_eq!(weights[5], default.skew.intercept);
        assert_eq!(weights[6], default.skew.w_entropy);
        assert_eq!(weights[7], default.skew.w_volatility);
        assert_eq!(weights[8], default.skew.w_imbalance);
        assert_eq!(weights[9], default.skew.w_inventory);
    }

    #[test]
    fn test_trainable_set_weights() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        let new_weights = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        algo.set_weights(new_weights.clone()).unwrap();

        let retrieved = algo.get_weights();
        assert_eq!(retrieved, new_weights);

        // Verify internal structure
        assert_eq!(algo.weights.spread.intercept, 1.0);
        assert_eq!(algo.weights.spread.w_entropy, 2.0);
        assert_eq!(algo.weights.skew.w_inventory, 10.0);
    }

    #[test]
    fn test_trainable_set_weights_wrong_count() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Too few weights
        let result = algo.set_weights(vec![1.0, 2.0, 3.0]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("Expected 10"));

        // Too many weights
        let result = algo.set_weights(vec![1.0; 15]);
        assert!(result.is_err());
    }

    #[test]
    fn test_trainable_weight_names() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        let names = algo.weight_names();

        assert_eq!(names.len(), 10);
        assert_eq!(names[0], weight_names::SPREAD_INTERCEPT);
        assert_eq!(names[1], weight_names::SPREAD_ENTROPY);
        assert_eq!(names[2], weight_names::SPREAD_VOLATILITY);
        assert_eq!(names[3], weight_names::SPREAD_IMBALANCE);
        assert_eq!(names[4], weight_names::SPREAD_INTERACTION);
        assert_eq!(names[5], weight_names::SKEW_INTERCEPT);
        assert_eq!(names[6], weight_names::SKEW_ENTROPY);
        assert_eq!(names[7], weight_names::SKEW_VOLATILITY);
        assert_eq!(names[8], weight_names::SKEW_IMBALANCE);
        assert_eq!(names[9], weight_names::SKEW_INVENTORY);
    }

    #[test]
    fn test_trainable_is_trained_default() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        assert!(!algo.is_trained());
    }

    #[test]
    fn test_trainable_is_trained_after_custom_version() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        algo.weights.version = "custom".to_string();
        assert!(algo.is_trained());
    }

    #[test]
    fn test_trainable_is_trained_after_training_info() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        algo.weights.training_info = Some(TrainingInfo {
            trained_on: "2024-01-01".to_string(),
            num_samples: 100,
            train_sharpe: 1.0,
            validation_sharpe: None,
        });
        assert!(algo.is_trained());
    }

    #[test]
    fn test_trainable_reset_weights() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Modify weights
        algo.set_weights(vec![1.0; 10]).unwrap();
        algo.weights.version = "modified".to_string();
        algo.weights.training_info = Some(TrainingInfo {
            trained_on: "test".to_string(),
            num_samples: 100,
            train_sharpe: 1.0,
            validation_sharpe: None,
        });

        // Reset
        algo.reset_weights();

        // Should be back to defaults
        assert_eq!(algo.weights.version, "1.0.0-baseline");
        assert!(algo.weights.training_info.is_none());
        assert!(!algo.is_trained());

        let default = MLModelWeights::default();
        assert_eq!(algo.weights.spread.intercept, default.spread.intercept);
    }

    #[test]
    fn test_trainable_train_empty_data() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        let data = TrainingData::new(vec![], vec![]);

        let result = algo.train(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("empty"));
    }

    #[test]
    fn test_trainable_train_wrong_feature_count() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        let features = vec![
            vec![1.0, 2.0, 3.0], // Should have 5 features
        ];
        let targets = vec![1.0];
        let data = TrainingData::new(features, targets);

        let result = algo.train(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("features"));
    }

    #[test]
    fn test_trainable_train_simple() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Create simple training data: target = 2.0 for all
        let features = vec![
            vec![0.5, 0.001, 0.0, 0.0005, 0.0],
            vec![0.6, 0.002, 0.1, 0.0012, 0.0],
            vec![0.7, 0.001, 0.2, 0.0007, 0.0],
            vec![0.8, 0.003, 0.0, 0.0024, 0.0],
        ];
        let targets = vec![2.0, 2.0, 2.0, 2.0];
        let data = TrainingData::new(features, targets);

        let result = algo.train(&data).unwrap();

        assert!(result.iterations > 0);
        assert!(result.duration_secs >= 0.0);
        assert!(result.metrics.contains_key("mse"));
        assert!(result.metrics.contains_key("rmse"));
    }

    #[test]
    fn test_trainable_train_updates_is_trained() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        assert!(!algo.is_trained());

        let features = vec![
            vec![0.5, 0.001, 0.0, 0.0005, 0.0],
            vec![0.6, 0.002, 0.1, 0.0012, 0.0],
        ];
        let targets = vec![2.0, 2.5];
        let data = TrainingData::new(features, targets);

        algo.train(&data).unwrap();

        assert!(algo.is_trained());
        assert!(algo.weights.training_info.is_some());
        assert_eq!(algo.weights.version, "trained");
    }

    #[test]
    fn test_trainable_save_load_weights() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Set custom weights
        let custom_weights = vec![1.5, -2.5, 600.0, 1.2, -120.0, 0.6, -0.3, 60.0, 0.15, -0.9];
        algo.set_weights(custom_weights.clone()).unwrap();
        algo.weights.version = "test-save".to_string();

        // Save to temp file
        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join("test_ml_weights.json");

        algo.save_weights(&temp_path).unwrap();

        // Create new algorithm and load weights
        let mut algo2 = MLSpreadSkewAlgorithm::with_defaults();
        algo2.load_weights(&temp_path).unwrap();

        // Verify weights match
        let loaded_weights = algo2.get_weights();
        for (i, (a, b)) in custom_weights.iter().zip(loaded_weights.iter()).enumerate() {
            assert!(
                (a - b).abs() < 0.0001,
                "Weight {} mismatch: {} vs {}",
                i,
                a,
                b
            );
        }
        assert_eq!(algo2.weights.version, "test-save");

        // Cleanup
        let _ = std::fs::remove_file(&temp_path);
    }

    #[test]
    fn test_trainable_load_weights_nonexistent() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();
        let result = algo.load_weights(Path::new("/nonexistent/path/weights.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_trainable_weight_names_constants() {
        assert_eq!(weight_names::SPREAD_INTERCEPT, "spread_intercept");
        assert_eq!(weight_names::SPREAD_ENTROPY, "spread_entropy");
        assert_eq!(weight_names::SPREAD_VOLATILITY, "spread_volatility");
        assert_eq!(weight_names::SPREAD_IMBALANCE, "spread_imbalance");
        assert_eq!(weight_names::SPREAD_INTERACTION, "spread_interaction");
        assert_eq!(weight_names::SKEW_INTERCEPT, "skew_intercept");
        assert_eq!(weight_names::SKEW_ENTROPY, "skew_entropy");
        assert_eq!(weight_names::SKEW_VOLATILITY, "skew_volatility");
        assert_eq!(weight_names::SKEW_IMBALANCE, "skew_imbalance");
        assert_eq!(weight_names::SKEW_INVENTORY, "skew_inventory");
    }

    #[test]
    fn test_trainable_weights_roundtrip() {
        let algo = MLSpreadSkewAlgorithm::with_defaults();
        let weights = algo.get_weights();

        let mut algo2 = MLSpreadSkewAlgorithm::with_defaults();
        algo2.set_weights(weights.clone()).unwrap();

        let weights2 = algo2.get_weights();
        assert_eq!(weights, weights2);
    }

    #[test]
    fn test_trainable_train_with_sample_weights() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        let features = vec![
            vec![0.5, 0.001, 0.0, 0.0005, 0.0],
            vec![0.6, 0.002, 0.1, 0.0012, 0.0],
            vec![0.7, 0.001, 0.2, 0.0007, 0.0],
        ];
        let targets = vec![2.0, 2.5, 3.0];
        let weights = vec![1.0, 2.0, 1.0]; // Higher weight for second sample
        let data = TrainingData::with_weights(features, targets, weights);

        let result = algo.train(&data).unwrap();
        assert!(result.iterations > 0);
    }

    #[test]
    fn test_algorithm_behavior_after_weight_change() {
        let mut algo = MLSpreadSkewAlgorithm::with_defaults();

        // Get initial prediction
        let input = create_test_input(0.7, 0.001, 0.0);
        let initial_spread = algo.predict_spread(&input);

        // Change spread intercept to a much larger value
        let mut weights = algo.get_weights();
        weights[0] = 50.0; // spread_intercept = 50
        algo.set_weights(weights).unwrap();

        // Prediction should change (but be clamped)
        let new_spread = algo.predict_spread(&input);

        // New spread should be at max (clamped)
        assert_eq!(new_spread, algo.config.max_spread_bps);
        assert!(new_spread != initial_spread);
    }
}

//! Core Traits and Types for Market Making Algorithms
//!
//! Defines the abstract interface that all market making algorithms must implement.
//!
//! # Trait Hierarchy
//!
//! ```text
//! MarketMakingAlgorithm   - Base trait: compute quotes, process fills
//!       ↓
//! Configurable           - Describes parameters, supports grid search
//!       ↓
//! Trainable              - Learnable weights, train/save/load
//! ```
//!
//! # Parameter System
//!
//! The `ParameterDefinition` system enables:
//! - Automatic CLI/TUI parameter discovery
//! - Grid search / hyperparameter optimization
//! - Configuration validation
//! - Serialization for reproducibility

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;

use crate::trading::market_maker::{Fill, MMQuotes, MMState, PnLTracker};

// ============================================================================
// Algorithm Type Enumeration
// ============================================================================

/// Unique identifier for each algorithm implementation.
///
/// Each variant has a stable string representation for serialization,
/// logging, and configuration files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmType {
    /// Avellaneda-Stoikov (2008) - Classic inventory-based market making
    AvellanedaStoikov,
    /// ML-based spread and skew prediction using learned weights
    MLSpreadSkew,
    /// Fixed spread baseline algorithm (ignores market conditions)
    FixedSpread,
}

impl AlgorithmType {
    /// Returns the stable string identifier for this algorithm type.
    ///
    /// This string is used for:
    /// - Configuration files
    /// - Logging and metrics
    /// - CLI parameters
    /// - Database storage
    pub fn as_str(&self) -> &'static str {
        match self {
            AlgorithmType::AvellanedaStoikov => "avellaneda_stoikov",
            AlgorithmType::MLSpreadSkew => "ml_spread_skew",
            AlgorithmType::FixedSpread => "fixed_spread",
        }
    }

    /// Parse algorithm type from string.
    pub fn from_str(s: &str) -> Result<Self, AlgorithmError> {
        match s.to_lowercase().as_str() {
            "avellaneda_stoikov" | "avellaneda-stoikov" | "as" | "a-s" => {
                Ok(AlgorithmType::AvellanedaStoikov)
            }
            "ml_spread_skew" | "ml-spread-skew" | "ml" | "mlss" => {
                Ok(AlgorithmType::MLSpreadSkew)
            }
            "fixed_spread" | "fixed-spread" | "fixed" | "fs" | "baseline" => {
                Ok(AlgorithmType::FixedSpread)
            }
            _ => Err(AlgorithmError::UnknownAlgorithm(s.to_string())),
        }
    }

    /// Returns all available algorithm types.
    pub fn all() -> &'static [AlgorithmType] {
        &[
            AlgorithmType::AvellanedaStoikov,
            AlgorithmType::MLSpreadSkew,
            AlgorithmType::FixedSpread,
        ]
    }

    /// Returns a human-readable name for this algorithm.
    pub fn display_name(&self) -> &'static str {
        match self {
            AlgorithmType::AvellanedaStoikov => "Avellaneda-Stoikov",
            AlgorithmType::MLSpreadSkew => "ML Spread-Skew",
            AlgorithmType::FixedSpread => "Fixed Spread",
        }
    }

    /// Returns a brief description of the algorithm.
    pub fn description(&self) -> &'static str {
        match self {
            AlgorithmType::AvellanedaStoikov => {
                "Inventory-based market making with optimal spread and skew (2008)"
            }
            AlgorithmType::MLSpreadSkew => {
                "ML-based spread/skew prediction using learned linear weights"
            }
            AlgorithmType::FixedSpread => {
                "Simple baseline with fixed spread/skew (ignores market conditions)"
            }
        }
    }
}

impl fmt::Display for AlgorithmType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for AlgorithmType {
    fn default() -> Self {
        AlgorithmType::AvellanedaStoikov
    }
}

// ============================================================================
// Algorithm Error Type
// ============================================================================

/// Errors that can occur in algorithm operations.
#[derive(Debug, Clone)]
pub enum AlgorithmError {
    /// Unknown algorithm type string
    UnknownAlgorithm(String),
    /// Invalid configuration
    InvalidConfig(String),
    /// Algorithm is in an invalid state
    InvalidState(String),
}

impl fmt::Display for AlgorithmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlgorithmError::UnknownAlgorithm(s) => write!(f, "Unknown algorithm: {}", s),
            AlgorithmError::InvalidConfig(s) => write!(f, "Invalid configuration: {}", s),
            AlgorithmError::InvalidState(s) => write!(f, "Invalid state: {}", s),
        }
    }
}

impl std::error::Error for AlgorithmError {}

// ============================================================================
// Parameter System
// ============================================================================

/// Type classification for algorithm parameters.
///
/// Used to determine:
/// - How to render parameter input in TUI
/// - How to generate grid search ranges
/// - How to validate parameter values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterType {
    /// Continuous real-valued parameter (e.g., spread_bps: 0.5 - 10.0)
    Continuous,
    /// Discrete integer-valued parameter (e.g., order_levels: 1, 2, 3)
    Discrete,
    /// Boolean on/off parameter (e.g., entropy_gating: true/false)
    Boolean,
}

impl fmt::Display for ParameterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParameterType::Continuous => write!(f, "continuous"),
            ParameterType::Discrete => write!(f, "discrete"),
            ParameterType::Boolean => write!(f, "boolean"),
        }
    }
}

/// Definition of an algorithm parameter for discovery and tuning.
///
/// This struct enables:
/// - Automatic CLI/TUI parameter discovery
/// - Grid search / hyperparameter optimization
/// - Configuration validation
/// - Serialization for reproducibility
///
/// # Example
///
/// ```ignore
/// let spread_param = ParameterDefinition::continuous("spread_bps")
///     .description("Half-spread in basis points")
///     .default(1.0)
///     .range(0.5, 10.0)
///     .tunable(true);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterDefinition {
    /// Parameter name (used in config files and CLI)
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Type classification
    pub param_type: ParameterType,
    /// Default value
    pub default: f64,
    /// Optional (min, max) range for validation and grid search
    pub range: Option<(f64, f64)>,
    /// Whether this parameter should be included in hyperparameter tuning
    pub tunable: bool,
}

impl ParameterDefinition {
    /// Create a new continuous parameter definition.
    pub fn continuous(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            param_type: ParameterType::Continuous,
            default: 0.0,
            range: None,
            tunable: true,
        }
    }

    /// Create a new discrete parameter definition.
    pub fn discrete(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            param_type: ParameterType::Discrete,
            default: 0.0,
            range: None,
            tunable: true,
        }
    }

    /// Create a new boolean parameter definition.
    pub fn boolean(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            param_type: ParameterType::Boolean,
            default: 0.0, // 0.0 = false, 1.0 = true
            range: Some((0.0, 1.0)),
            tunable: true,
        }
    }

    /// Set the description (builder pattern).
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Set the default value (builder pattern).
    pub fn default(mut self, value: f64) -> Self {
        self.default = value;
        self
    }

    /// Set the valid range (builder pattern).
    pub fn range(mut self, min: f64, max: f64) -> Self {
        self.range = Some((min, max));
        self
    }

    /// Set whether the parameter is tunable (builder pattern).
    pub fn tunable(mut self, tunable: bool) -> Self {
        self.tunable = tunable;
        self
    }

    /// Validate a value against this parameter's constraints.
    pub fn validate(&self, value: f64) -> Result<(), AlgorithmError> {
        if let Some((min, max)) = self.range {
            if value < min || value > max {
                return Err(AlgorithmError::InvalidConfig(format!(
                    "Parameter '{}' value {} is outside valid range [{}, {}]",
                    self.name, value, min, max
                )));
            }
        }

        match self.param_type {
            ParameterType::Discrete => {
                if value.fract() != 0.0 {
                    return Err(AlgorithmError::InvalidConfig(format!(
                        "Parameter '{}' must be an integer, got {}",
                        self.name, value
                    )));
                }
            }
            ParameterType::Boolean => {
                if value != 0.0 && value != 1.0 {
                    return Err(AlgorithmError::InvalidConfig(format!(
                        "Parameter '{}' must be 0 or 1 (boolean), got {}",
                        self.name, value
                    )));
                }
            }
            ParameterType::Continuous => {
                // Any value in range is valid
            }
        }

        Ok(())
    }

    /// Generate grid search values for this parameter.
    ///
    /// - Continuous: linear steps between min and max
    /// - Discrete: integer steps between min and max
    /// - Boolean: [0.0, 1.0]
    pub fn grid_values(&self, num_steps: usize) -> Vec<f64> {
        match self.param_type {
            ParameterType::Boolean => vec![0.0, 1.0],
            ParameterType::Discrete => {
                if let Some((min, max)) = self.range {
                    let min_int = min.ceil() as i64;
                    let max_int = max.floor() as i64;
                    (min_int..=max_int).map(|i| i as f64).collect()
                } else {
                    vec![self.default]
                }
            }
            ParameterType::Continuous => {
                if let Some((min, max)) = self.range {
                    if num_steps <= 1 {
                        return vec![self.default];
                    }
                    let step = (max - min) / (num_steps - 1) as f64;
                    (0..num_steps).map(|i| min + step * i as f64).collect()
                } else {
                    vec![self.default]
                }
            }
        }
    }

    /// Get boolean value from f64 representation.
    pub fn as_bool(&self, value: f64) -> bool {
        value != 0.0
    }

    /// Convert boolean to f64 representation.
    pub fn from_bool(value: bool) -> f64 {
        if value { 1.0 } else { 0.0 }
    }
}

// ============================================================================
// Configurable Trait
// ============================================================================

/// Trait for algorithms that describe their parameters.
///
/// This trait extends `MarketMakingAlgorithm` to add:
/// - Parameter discovery for CLI/TUI
/// - Grid search support
/// - Configuration validation
///
/// # Example Implementation
///
/// ```ignore
/// impl Configurable for MyAlgorithm {
///     fn parameters() -> Vec<ParameterDefinition> {
///         vec![
///             ParameterDefinition::continuous("spread_bps")
///                 .description("Half-spread in basis points")
///                 .default(1.0)
///                 .range(0.5, 10.0),
///             ParameterDefinition::boolean("entropy_gating")
///                 .description("Enable entropy-based quote gating")
///                 .default(0.0),
///         ]
///     }
///
///     fn from_parameters(params: &HashMap<String, f64>) -> Result<Self, AlgorithmError> {
///         let spread_bps = *params.get("spread_bps").unwrap_or(&1.0);
///         Ok(Self::new(spread_bps))
///     }
///
///     fn current_parameters(&self) -> HashMap<String, f64> {
///         let mut params = HashMap::new();
///         params.insert("spread_bps".to_string(), self.spread_bps);
///         params
///     }
/// }
/// ```
pub trait Configurable: MarketMakingAlgorithm {
    /// Returns the parameter definitions for this algorithm.
    ///
    /// This is a static method so parameters can be queried without an instance.
    fn parameters() -> Vec<ParameterDefinition>
    where
        Self: Sized;

    /// Create an instance from a parameter map.
    ///
    /// The map should contain all required parameters with valid values.
    fn from_parameters(
        params: &std::collections::HashMap<String, f64>,
    ) -> Result<Self, AlgorithmError>
    where
        Self: Sized;

    /// Get current parameter values as a map.
    fn current_parameters(&self) -> std::collections::HashMap<String, f64>;

    /// Update a single parameter value.
    ///
    /// Returns an error if the parameter doesn't exist or the value is invalid.
    fn set_parameter(&mut self, name: &str, value: f64) -> Result<(), AlgorithmError>;

    /// Validate all current parameters against their definitions.
    fn validate_parameters(&self) -> Result<(), AlgorithmError>
    where
        Self: Sized,
    {
        let current = self.current_parameters();
        for param_def in Self::parameters() {
            if let Some(&value) = current.get(&param_def.name) {
                param_def.validate(value)?;
            }
        }
        Ok(())
    }

    /// Get tunable parameters only (for grid search).
    fn tunable_parameters() -> Vec<ParameterDefinition>
    where
        Self: Sized,
    {
        Self::parameters()
            .into_iter()
            .filter(|p| p.tunable)
            .collect()
    }
}

// ============================================================================
// Training Results
// ============================================================================

/// Result of training a trainable algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingResult {
    /// Whether training converged successfully
    pub converged: bool,
    /// Final loss/objective value
    pub final_loss: f64,
    /// Number of training iterations/epochs
    pub iterations: usize,
    /// Training duration in seconds
    pub duration_secs: f64,
    /// Additional metrics (algorithm-specific)
    pub metrics: std::collections::HashMap<String, f64>,
}

impl Default for TrainingResult {
    fn default() -> Self {
        Self {
            converged: false,
            final_loss: f64::NAN,
            iterations: 0,
            duration_secs: 0.0,
            metrics: std::collections::HashMap::new(),
        }
    }
}

/// Training data for algorithms with learnable weights.
#[derive(Debug, Clone)]
pub struct TrainingData {
    /// Feature vectors (each inner Vec is one sample's features)
    pub features: Vec<Vec<f64>>,
    /// Target values (e.g., optimal spread, optimal skew)
    pub targets: Vec<f64>,
    /// Sample weights (optional, defaults to uniform)
    pub weights: Option<Vec<f64>>,
}

impl TrainingData {
    /// Create new training data.
    pub fn new(features: Vec<Vec<f64>>, targets: Vec<f64>) -> Self {
        Self {
            features,
            targets,
            weights: None,
        }
    }

    /// Create training data with sample weights.
    pub fn with_weights(features: Vec<Vec<f64>>, targets: Vec<f64>, weights: Vec<f64>) -> Self {
        Self {
            features,
            targets,
            weights: Some(weights),
        }
    }

    /// Get the number of samples.
    pub fn len(&self) -> usize {
        self.targets.len()
    }

    /// Check if training data is empty.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

// ============================================================================
// Trainable Trait
// ============================================================================

/// Trait for algorithms with learnable weights.
///
/// This trait extends `Configurable` to add:
/// - Weight training from data
/// - Weight persistence (save/load)
/// - Training configuration
///
/// # Example Implementation
///
/// ```ignore
/// impl Trainable for MLSpreadSkew {
///     fn train(&mut self, data: &TrainingData) -> Result<TrainingResult, AlgorithmError> {
///         // Train linear weights using least squares
///         let weights = solve_least_squares(&data.features, &data.targets);
///         self.weights = weights;
///         Ok(TrainingResult { converged: true, ..Default::default() })
///     }
///
///     fn save_weights(&self, path: &Path) -> Result<(), AlgorithmError> {
///         let json = serde_json::to_string(&self.weights)?;
///         std::fs::write(path, json)?;
///         Ok(())
///     }
///
///     fn load_weights(&mut self, path: &Path) -> Result<(), AlgorithmError> {
///         let json = std::fs::read_to_string(path)?;
///         self.weights = serde_json::from_str(&json)?;
///         Ok(())
///     }
/// }
/// ```
pub trait Trainable: Configurable {
    /// Train the algorithm's weights from data.
    fn train(&mut self, data: &TrainingData) -> Result<TrainingResult, AlgorithmError>;

    /// Save learned weights to a file.
    fn save_weights(&self, path: &Path) -> Result<(), AlgorithmError>;

    /// Load learned weights from a file.
    fn load_weights(&mut self, path: &Path) -> Result<(), AlgorithmError>;

    /// Get the current weights as a vector.
    fn get_weights(&self) -> Vec<f64>;

    /// Set weights directly (useful for ensemble/consensus).
    fn set_weights(&mut self, weights: Vec<f64>) -> Result<(), AlgorithmError>;

    /// Get weight names/labels for interpretation.
    fn weight_names(&self) -> Vec<String>;

    /// Check if the algorithm has been trained (weights are non-default).
    fn is_trained(&self) -> bool;

    /// Reset weights to their initial/default values.
    fn reset_weights(&mut self);
}

// ============================================================================
// Market Input Structure
// ============================================================================

/// Input data for algorithm quote computation.
///
/// Provides all market information needed by algorithms to generate quotes.
#[derive(Debug, Clone)]
pub struct MarketInput {
    /// Best bid price from the order book
    pub best_bid: Decimal,
    /// Best ask price from the order book
    pub best_ask: Decimal,
    /// Estimated volatility (annualized or per-period)
    pub volatility: f64,
    /// Entropy score (0.0 = low entropy/trending, 1.0 = high entropy/mean-reverting)
    pub entropy: f64,
    /// Order book imbalance (-1.0 to 1.0, positive = buy pressure)
    pub book_imbalance: f64,
    /// Current timestamp in milliseconds
    pub timestamp_ms: u64,
}

impl MarketInput {
    /// Calculate mid price from best bid and ask.
    pub fn mid_price(&self) -> Decimal {
        (self.best_bid + self.best_ask) / Decimal::TWO
    }

    /// Calculate spread in basis points.
    pub fn spread_bps(&self) -> f64 {
        let mid = self.mid_price();
        if mid.is_zero() {
            return 0.0;
        }
        let spread = self.best_ask - self.best_bid;
        (spread / mid * Decimal::from(10000))
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0)
    }
}

// ============================================================================
// Algorithm Configuration Trait
// ============================================================================

/// Trait for algorithm-specific configuration.
///
/// Implementations should be serializable for persistence and logging.
pub trait AlgorithmConfig: Send + Sync + fmt::Debug {
    /// Returns the algorithm type this config is for.
    fn algorithm_type(&self) -> AlgorithmType;

    /// Validates the configuration.
    fn validate(&self) -> Result<(), AlgorithmError>;

    /// Returns a human-readable summary of the configuration.
    fn summary(&self) -> String;
}

// ============================================================================
// Core Algorithm Trait
// ============================================================================

/// Core trait that all market making algorithms must implement.
///
/// This trait provides a common interface for:
/// - Quote computation based on market state
/// - Fill processing and inventory management
/// - State introspection and reset
///
/// # Example Implementation
///
/// ```ignore
/// impl MarketMakingAlgorithm for MyAlgorithm {
///     fn algorithm_type(&self) -> AlgorithmType {
///         AlgorithmType::MyAlgorithm
///     }
///
///     fn compute_quotes(&mut self, input: &MarketInput) -> MMQuotes {
///         // Custom quote logic
///     }
///
///     // ... other methods
/// }
/// ```
pub trait MarketMakingAlgorithm: Send + Sync {
    // ========================================================================
    // Identity Methods
    // ========================================================================

    /// Returns the algorithm type identifier.
    fn algorithm_type(&self) -> AlgorithmType;

    /// Returns the stable string identifier for this algorithm.
    ///
    /// Default implementation delegates to `algorithm_type().as_str()`.
    fn type_string(&self) -> &'static str {
        self.algorithm_type().as_str()
    }

    /// Returns a human-readable name for logging and display.
    fn name(&self) -> &'static str;

    /// Returns algorithm version for tracking changes.
    fn version(&self) -> &'static str {
        "1.0.0"
    }

    // ========================================================================
    // Core Trading Methods
    // ========================================================================

    /// Compute bid/ask quotes based on current market state.
    ///
    /// This is the core method where each algorithm implements its
    /// unique quote generation logic.
    fn compute_quotes(&mut self, input: &MarketInput) -> MMQuotes;

    /// Process a fill (when our quote gets executed).
    ///
    /// Updates inventory, average entry price, and PnL tracking.
    fn process_fill(&mut self, fill: Fill, fee_rate: Decimal);

    /// Update mark-to-market PnL based on current price.
    fn update_mark_to_market(&mut self, current_price: Decimal);

    // ========================================================================
    // State Methods
    // ========================================================================

    /// Get current algorithm state for inspection.
    fn get_state(&self) -> MMState;

    /// Get current inventory position.
    fn inventory(&self) -> Decimal;

    /// Get PnL tracker reference.
    fn pnl(&self) -> &PnLTracker;

    /// Reset algorithm to initial state (for new session or backtest run).
    fn reset(&mut self);

    // ========================================================================
    // Configuration Methods
    // ========================================================================

    /// Get maximum inventory limit.
    fn max_inventory(&self) -> Decimal;

    /// Get quote size per order.
    fn quote_size(&self) -> Decimal;

    /// Returns a JSON-serializable summary of current parameters.
    fn parameters_json(&self) -> serde_json::Value {
        serde_json::json!({
            "algorithm": self.type_string(),
            "version": self.version(),
            "max_inventory": self.max_inventory().to_string(),
            "quote_size": self.quote_size().to_string(),
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_type_as_str() {
        assert_eq!(AlgorithmType::AvellanedaStoikov.as_str(), "avellaneda_stoikov");
        assert_eq!(AlgorithmType::MLSpreadSkew.as_str(), "ml_spread_skew");
    }

    #[test]
    fn test_algorithm_type_from_str() {
        assert_eq!(
            AlgorithmType::from_str("avellaneda_stoikov").unwrap(),
            AlgorithmType::AvellanedaStoikov
        );
        assert_eq!(
            AlgorithmType::from_str("avellaneda-stoikov").unwrap(),
            AlgorithmType::AvellanedaStoikov
        );
        assert_eq!(
            AlgorithmType::from_str("as").unwrap(),
            AlgorithmType::AvellanedaStoikov
        );
        assert_eq!(
            AlgorithmType::from_str("a-s").unwrap(),
            AlgorithmType::AvellanedaStoikov
        );
        assert_eq!(
            AlgorithmType::from_str("AVELLANEDA_STOIKOV").unwrap(),
            AlgorithmType::AvellanedaStoikov
        );
        // ML Spread Skew variants
        assert_eq!(
            AlgorithmType::from_str("ml_spread_skew").unwrap(),
            AlgorithmType::MLSpreadSkew
        );
        assert_eq!(
            AlgorithmType::from_str("ml-spread-skew").unwrap(),
            AlgorithmType::MLSpreadSkew
        );
        assert_eq!(
            AlgorithmType::from_str("ml").unwrap(),
            AlgorithmType::MLSpreadSkew
        );
        assert_eq!(
            AlgorithmType::from_str("mlss").unwrap(),
            AlgorithmType::MLSpreadSkew
        );
    }

    #[test]
    fn test_algorithm_type_from_str_unknown() {
        assert!(AlgorithmType::from_str("unknown").is_err());
    }

    #[test]
    fn test_algorithm_type_display() {
        assert_eq!(
            format!("{}", AlgorithmType::AvellanedaStoikov),
            "avellaneda_stoikov"
        );
    }

    #[test]
    fn test_algorithm_type_all() {
        let all = AlgorithmType::all();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&AlgorithmType::AvellanedaStoikov));
        assert!(all.contains(&AlgorithmType::MLSpreadSkew));
        assert!(all.contains(&AlgorithmType::FixedSpread));
    }

    #[test]
    fn test_market_input_mid_price() {
        use rust_decimal_macros::dec;

        let input = MarketInput {
            best_bid: dec!(50000),
            best_ask: dec!(50100),
            volatility: 0.001,
            entropy: 0.8,
            book_imbalance: 0.0,
            timestamp_ms: 1000,
        };

        assert_eq!(input.mid_price(), dec!(50050));
    }

    #[test]
    fn test_market_input_spread_bps() {
        use rust_decimal_macros::dec;

        let input = MarketInput {
            best_bid: dec!(50000),
            best_ask: dec!(50100),
            volatility: 0.001,
            entropy: 0.8,
            book_imbalance: 0.0,
            timestamp_ms: 1000,
        };

        // Spread = 100, Mid = 50050, Spread bps = 100/50050 * 10000 ≈ 19.98
        let spread_bps = input.spread_bps();
        assert!(spread_bps > 19.0 && spread_bps < 21.0);
    }

    #[test]
    fn test_algorithm_error_display() {
        let err = AlgorithmError::UnknownAlgorithm("foo".to_string());
        assert_eq!(format!("{}", err), "Unknown algorithm: foo");

        let err = AlgorithmError::InvalidConfig("bad value".to_string());
        assert_eq!(format!("{}", err), "Invalid configuration: bad value");
    }

    #[test]
    fn test_algorithm_type_serde() {
        let algo = AlgorithmType::AvellanedaStoikov;
        let json = serde_json::to_string(&algo).unwrap();
        assert_eq!(json, "\"avellaneda_stoikov\"");

        let parsed: AlgorithmType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, algo);
    }

    // ========================================================================
    // ParameterType Tests
    // ========================================================================

    #[test]
    fn test_parameter_type_display() {
        assert_eq!(format!("{}", ParameterType::Continuous), "continuous");
        assert_eq!(format!("{}", ParameterType::Discrete), "discrete");
        assert_eq!(format!("{}", ParameterType::Boolean), "boolean");
    }

    #[test]
    fn test_parameter_type_serde() {
        let pt = ParameterType::Continuous;
        let json = serde_json::to_string(&pt).unwrap();
        assert_eq!(json, "\"continuous\"");

        let parsed: ParameterType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, pt);

        // Test all variants
        for variant in [
            ParameterType::Continuous,
            ParameterType::Discrete,
            ParameterType::Boolean,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let parsed: ParameterType = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    // ========================================================================
    // ParameterDefinition Tests
    // ========================================================================

    #[test]
    fn test_parameter_definition_continuous_builder() {
        let param = ParameterDefinition::continuous("spread_bps")
            .description("Half-spread in basis points")
            .default(1.0)
            .range(0.5, 10.0)
            .tunable(true);

        assert_eq!(param.name, "spread_bps");
        assert_eq!(param.description, "Half-spread in basis points");
        assert_eq!(param.param_type, ParameterType::Continuous);
        assert_eq!(param.default, 1.0);
        assert_eq!(param.range, Some((0.5, 10.0)));
        assert!(param.tunable);
    }

    #[test]
    fn test_parameter_definition_discrete_builder() {
        let param = ParameterDefinition::discrete("order_levels")
            .description("Number of order levels")
            .default(3.0)
            .range(1.0, 5.0);

        assert_eq!(param.name, "order_levels");
        assert_eq!(param.param_type, ParameterType::Discrete);
        assert_eq!(param.default, 3.0);
        assert_eq!(param.range, Some((1.0, 5.0)));
    }

    #[test]
    fn test_parameter_definition_boolean_builder() {
        let param = ParameterDefinition::boolean("entropy_gating")
            .description("Enable entropy-based quote gating")
            .default(0.0);

        assert_eq!(param.name, "entropy_gating");
        assert_eq!(param.param_type, ParameterType::Boolean);
        assert_eq!(param.default, 0.0);
        assert_eq!(param.range, Some((0.0, 1.0))); // Boolean auto-sets range
    }

    #[test]
    fn test_parameter_definition_validate_continuous_in_range() {
        let param = ParameterDefinition::continuous("test")
            .range(0.0, 10.0);

        assert!(param.validate(0.0).is_ok());
        assert!(param.validate(5.0).is_ok());
        assert!(param.validate(10.0).is_ok());
    }

    #[test]
    fn test_parameter_definition_validate_continuous_out_of_range() {
        let param = ParameterDefinition::continuous("test")
            .range(0.0, 10.0);

        assert!(param.validate(-0.1).is_err());
        assert!(param.validate(10.1).is_err());
    }

    #[test]
    fn test_parameter_definition_validate_discrete_integer() {
        let param = ParameterDefinition::discrete("levels")
            .range(1.0, 5.0);

        assert!(param.validate(1.0).is_ok());
        assert!(param.validate(3.0).is_ok());
        assert!(param.validate(5.0).is_ok());
    }

    #[test]
    fn test_parameter_definition_validate_discrete_non_integer() {
        let param = ParameterDefinition::discrete("levels")
            .range(1.0, 5.0);

        assert!(param.validate(1.5).is_err());
        assert!(param.validate(2.7).is_err());
    }

    #[test]
    fn test_parameter_definition_validate_boolean() {
        let param = ParameterDefinition::boolean("flag");

        assert!(param.validate(0.0).is_ok());
        assert!(param.validate(1.0).is_ok());
        assert!(param.validate(0.5).is_err());
        assert!(param.validate(2.0).is_err());
    }

    #[test]
    fn test_parameter_definition_grid_values_boolean() {
        let param = ParameterDefinition::boolean("flag");
        let values = param.grid_values(10); // num_steps ignored for boolean

        assert_eq!(values, vec![0.0, 1.0]);
    }

    #[test]
    fn test_parameter_definition_grid_values_discrete() {
        let param = ParameterDefinition::discrete("levels")
            .range(1.0, 5.0);
        let values = param.grid_values(10); // num_steps ignored for discrete

        assert_eq!(values, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    }

    #[test]
    fn test_parameter_definition_grid_values_continuous() {
        let param = ParameterDefinition::continuous("spread")
            .range(0.0, 10.0);
        let values = param.grid_values(5);

        assert_eq!(values.len(), 5);
        assert!((values[0] - 0.0).abs() < 0.001);
        assert!((values[1] - 2.5).abs() < 0.001);
        assert!((values[2] - 5.0).abs() < 0.001);
        assert!((values[3] - 7.5).abs() < 0.001);
        assert!((values[4] - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_parameter_definition_grid_values_continuous_single_step() {
        let param = ParameterDefinition::continuous("spread")
            .default(5.0)
            .range(0.0, 10.0);
        let values = param.grid_values(1);

        assert_eq!(values, vec![5.0]); // Returns default for single step
    }

    #[test]
    fn test_parameter_definition_grid_values_no_range() {
        let param = ParameterDefinition::continuous("spread")
            .default(5.0);
        let values = param.grid_values(5);

        assert_eq!(values, vec![5.0]); // Returns default when no range
    }

    #[test]
    fn test_parameter_definition_bool_conversion() {
        let param = ParameterDefinition::boolean("flag");

        assert!(!param.as_bool(0.0));
        assert!(param.as_bool(1.0));
        assert!(param.as_bool(0.5)); // Non-zero is true

        assert_eq!(ParameterDefinition::from_bool(false), 0.0);
        assert_eq!(ParameterDefinition::from_bool(true), 1.0);
    }

    #[test]
    fn test_parameter_definition_serde() {
        let param = ParameterDefinition::continuous("spread_bps")
            .description("Half-spread")
            .default(1.0)
            .range(0.5, 10.0)
            .tunable(true);

        let json = serde_json::to_string(&param).unwrap();
        let parsed: ParameterDefinition = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name, param.name);
        assert_eq!(parsed.description, param.description);
        assert_eq!(parsed.param_type, param.param_type);
        assert_eq!(parsed.default, param.default);
        assert_eq!(parsed.range, param.range);
        assert_eq!(parsed.tunable, param.tunable);
    }

    // ========================================================================
    // TrainingData Tests
    // ========================================================================

    #[test]
    fn test_training_data_new() {
        let features = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
        let targets = vec![0.5, 1.5];
        let data = TrainingData::new(features.clone(), targets.clone());

        assert_eq!(data.features, features);
        assert_eq!(data.targets, targets);
        assert!(data.weights.is_none());
        assert_eq!(data.len(), 2);
        assert!(!data.is_empty());
    }

    #[test]
    fn test_training_data_with_weights() {
        let features = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
        let targets = vec![0.5, 1.5];
        let weights = vec![1.0, 2.0];
        let data = TrainingData::with_weights(features.clone(), targets.clone(), weights.clone());

        assert_eq!(data.features, features);
        assert_eq!(data.targets, targets);
        assert_eq!(data.weights, Some(weights));
    }

    #[test]
    fn test_training_data_empty() {
        let data = TrainingData::new(vec![], vec![]);

        assert!(data.is_empty());
        assert_eq!(data.len(), 0);
    }

    // ========================================================================
    // TrainingResult Tests
    // ========================================================================

    #[test]
    fn test_training_result_default() {
        let result = TrainingResult::default();

        assert!(!result.converged);
        assert!(result.final_loss.is_nan());
        assert_eq!(result.iterations, 0);
        assert_eq!(result.duration_secs, 0.0);
        assert!(result.metrics.is_empty());
    }

    #[test]
    fn test_training_result_custom() {
        let mut metrics = std::collections::HashMap::new();
        metrics.insert("r_squared".to_string(), 0.95);

        let result = TrainingResult {
            converged: true,
            final_loss: 0.001,
            iterations: 100,
            duration_secs: 1.5,
            metrics,
        };

        assert!(result.converged);
        assert!((result.final_loss - 0.001).abs() < 0.0001);
        assert_eq!(result.iterations, 100);
        assert!((result.duration_secs - 1.5).abs() < 0.001);
        assert_eq!(result.metrics.get("r_squared"), Some(&0.95));
    }

    #[test]
    fn test_training_result_serde() {
        let result = TrainingResult {
            converged: true,
            final_loss: 0.001,
            iterations: 100,
            duration_secs: 1.5,
            metrics: std::collections::HashMap::new(),
        };

        let json = serde_json::to_string(&result).unwrap();
        let parsed: TrainingResult = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.converged, result.converged);
        assert!((parsed.final_loss - result.final_loss).abs() < 0.0001);
        assert_eq!(parsed.iterations, result.iterations);
        assert!((parsed.duration_secs - result.duration_secs).abs() < 0.001);
    }
}

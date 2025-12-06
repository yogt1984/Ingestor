//! Core Traits and Types for Market Making Algorithms
//!
//! Defines the abstract interface that all market making algorithms must implement.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::market_maker::{Fill, MMQuotes, MMState, PnLTracker};

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
            _ => Err(AlgorithmError::UnknownAlgorithm(s.to_string())),
        }
    }

    /// Returns all available algorithm types.
    pub fn all() -> &'static [AlgorithmType] {
        &[AlgorithmType::AvellanedaStoikov, AlgorithmType::MLSpreadSkew]
    }

    /// Returns a human-readable name for this algorithm.
    pub fn display_name(&self) -> &'static str {
        match self {
            AlgorithmType::AvellanedaStoikov => "Avellaneda-Stoikov",
            AlgorithmType::MLSpreadSkew => "ML Spread-Skew",
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
        assert_eq!(all.len(), 2);
        assert!(all.contains(&AlgorithmType::AvellanedaStoikov));
        assert!(all.contains(&AlgorithmType::MLSpreadSkew));
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
}

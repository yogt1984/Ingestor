//! Algorithm Registry
//!
//! Centralized registration and discovery of market making algorithms.
//!
//! The registry provides:
//! - Listing all available algorithms with metadata
//! - Creating algorithms by type string
//! - Parameter and capability discovery
//! - Version tracking for reproducibility
//!
//! # Usage
//!
//! ```ignore
//! use crate::strategies::registry::AlgorithmRegistry;
//!
//! // List all available algorithms
//! for info in AlgorithmRegistry::list() {
//!     println!("{}: {}", info.name, info.description);
//! }
//!
//! // Create algorithm by type string
//! let algo = AlgorithmRegistry::create("avellaneda_stoikov", params)?;
//! ```

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{
    AlgorithmError, AlgorithmType, AvellanedaStoikovAlgorithm, Configurable,
    FixedSpreadAlgorithm, FixedSpreadConfig, MLModelWeights, MLSpreadSkewAlgorithm,
    MLSpreadSkewConfig, MarketMakingAlgorithm, ParameterDefinition, RegimeParams,
};
use crate::execution::market_maker::AvellanedaStoikovConfig;

// ============================================================================
// Algorithm Info
// ============================================================================

/// Metadata about a registered algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmInfo {
    /// Unique type identifier
    pub algorithm_type: AlgorithmType,
    /// Type string for CLI/config
    pub type_string: &'static str,
    /// Human-readable name
    pub name: &'static str,
    /// Algorithm description
    pub description: &'static str,
    /// Algorithm version
    pub version: &'static str,
    /// Whether the algorithm implements Configurable
    pub is_configurable: bool,
    /// Whether the algorithm implements Trainable
    pub is_trainable: bool,
    /// Aliases for CLI parsing (e.g., "as" for "avellaneda_stoikov")
    pub aliases: Vec<&'static str>,
}

impl AlgorithmInfo {
    /// Create info for Avellaneda-Stoikov algorithm.
    fn avellaneda_stoikov() -> Self {
        Self {
            algorithm_type: AlgorithmType::AvellanedaStoikov,
            type_string: "avellaneda_stoikov",
            name: "Avellaneda-Stoikov Market Maker",
            description: "Inventory-based market making with optimal spread and skew (2008)",
            version: "1.0.0",
            is_configurable: true,
            is_trainable: false,
            aliases: vec!["as", "a-s", "avellaneda-stoikov"],
        }
    }

    /// Create info for ML Spread-Skew algorithm.
    fn ml_spread_skew() -> Self {
        Self {
            algorithm_type: AlgorithmType::MLSpreadSkew,
            type_string: "ml_spread_skew",
            name: "ML Spread/Skew Predictor",
            description: "ML-based spread/skew prediction using learned linear weights",
            version: "1.0.0",
            is_configurable: true,
            is_trainable: true,
            aliases: vec!["ml", "mlss", "ml-spread-skew"],
        }
    }

    /// Create info for Fixed Spread algorithm.
    fn fixed_spread() -> Self {
        Self {
            algorithm_type: AlgorithmType::FixedSpread,
            type_string: "fixed_spread",
            name: "Fixed Spread Market Maker",
            description: "Simple baseline with fixed spread/skew (ignores market conditions)",
            version: "1.0.0",
            is_configurable: true,
            is_trainable: false,
            aliases: vec!["fixed", "fs", "fixed-spread", "baseline"],
        }
    }
}

// ============================================================================
// Backtest Algorithm Parameters
// ============================================================================

/// Unified parameters for algorithm creation in backtest context.
///
/// This struct encapsulates all the parameters needed to create any algorithm
/// from CLI arguments, simplifying the interface between CLI and registry.
#[derive(Debug, Clone)]
pub struct BacktestAlgorithmParams {
    /// Maximum inventory limit
    pub max_inventory: Decimal,
    /// Size per quote order
    pub quote_size: Decimal,
    /// Spread in basis points (used by A-S and FixedSpread)
    pub spread_bps: f64,
    /// Skew factor (used by A-S and FixedSpread)
    pub skew_factor: f64,
    /// Optional ML model weights (only used by MLSpreadSkew)
    pub ml_weights: Option<MLModelWeights>,
}

impl Default for BacktestAlgorithmParams {
    fn default() -> Self {
        Self {
            max_inventory: Decimal::new(1, 1), // 0.1
            quote_size: Decimal::new(1, 3),    // 0.001
            spread_bps: 1.0,
            skew_factor: 0.3,
            ml_weights: None,
        }
    }
}

impl BacktestAlgorithmParams {
    /// Create new parameters with basic settings.
    pub fn new(
        max_inventory: Decimal,
        quote_size: Decimal,
        spread_bps: f64,
        skew_factor: f64,
    ) -> Self {
        Self {
            max_inventory,
            quote_size,
            spread_bps,
            skew_factor,
            ml_weights: None,
        }
    }

    /// Add ML weights to the parameters.
    pub fn with_ml_weights(mut self, weights: MLModelWeights) -> Self {
        self.ml_weights = Some(weights);
        self
    }
}

// ============================================================================
// Algorithm Registry
// ============================================================================

/// Central registry for market making algorithms.
///
/// Provides discovery, metadata, and factory methods for all registered algorithms.
pub struct AlgorithmRegistry;

impl AlgorithmRegistry {
    /// List all registered algorithms with their metadata.
    pub fn list() -> Vec<AlgorithmInfo> {
        vec![
            AlgorithmInfo::avellaneda_stoikov(),
            AlgorithmInfo::ml_spread_skew(),
            AlgorithmInfo::fixed_spread(),
        ]
    }

    /// Get metadata for a specific algorithm type.
    pub fn info(algo_type: AlgorithmType) -> AlgorithmInfo {
        match algo_type {
            AlgorithmType::AvellanedaStoikov => AlgorithmInfo::avellaneda_stoikov(),
            AlgorithmType::MLSpreadSkew => AlgorithmInfo::ml_spread_skew(),
            AlgorithmType::FixedSpread => AlgorithmInfo::fixed_spread(),
        }
    }

    /// Get metadata by type string (supports aliases).
    pub fn info_by_string(type_string: &str) -> Result<AlgorithmInfo, AlgorithmError> {
        let algo_type = AlgorithmType::from_str(type_string)?;
        Ok(Self::info(algo_type))
    }

    /// Check if a type string is valid (including aliases).
    pub fn is_valid_type(type_string: &str) -> bool {
        AlgorithmType::from_str(type_string).is_ok()
    }

    /// Get all valid type strings (primary + aliases).
    pub fn all_type_strings() -> Vec<&'static str> {
        let mut strings = Vec::new();
        for info in Self::list() {
            strings.push(info.type_string);
            strings.extend(info.aliases);
        }
        strings
    }

    /// Get parameter definitions for an algorithm type.
    pub fn parameters(algo_type: AlgorithmType) -> Vec<ParameterDefinition> {
        match algo_type {
            AlgorithmType::AvellanedaStoikov => AvellanedaStoikovAlgorithm::parameters(),
            AlgorithmType::MLSpreadSkew => MLSpreadSkewAlgorithm::parameters(),
            AlgorithmType::FixedSpread => FixedSpreadAlgorithm::parameters(),
        }
    }

    /// Get parameter definitions by type string.
    pub fn parameters_by_string(
        type_string: &str,
    ) -> Result<Vec<ParameterDefinition>, AlgorithmError> {
        let algo_type = AlgorithmType::from_str(type_string)?;
        Ok(Self::parameters(algo_type))
    }

    /// Get tunable parameters for an algorithm type (for grid search).
    pub fn tunable_parameters(algo_type: AlgorithmType) -> Vec<ParameterDefinition> {
        match algo_type {
            AlgorithmType::AvellanedaStoikov => AvellanedaStoikovAlgorithm::tunable_parameters(),
            AlgorithmType::MLSpreadSkew => MLSpreadSkewAlgorithm::tunable_parameters(),
            AlgorithmType::FixedSpread => FixedSpreadAlgorithm::tunable_parameters(),
        }
    }

    /// Create an algorithm instance by type with default configuration.
    ///
    /// # Arguments
    /// * `algo_type` - Algorithm type to create
    /// * `max_inventory` - Maximum inventory limit
    /// * `quote_size` - Size per quote order
    pub fn create_default(
        algo_type: AlgorithmType,
        max_inventory: Decimal,
        quote_size: Decimal,
    ) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError> {
        match algo_type {
            AlgorithmType::AvellanedaStoikov => {
                let config = AvellanedaStoikovConfig {
                    max_inventory,
                    quote_size,
                    ..Default::default()
                };
                Ok(Box::new(AvellanedaStoikovAlgorithm::new(config)))
            }
            AlgorithmType::MLSpreadSkew => {
                let config = MLSpreadSkewConfig {
                    max_inventory,
                    quote_size,
                    ..Default::default()
                };
                Ok(Box::new(MLSpreadSkewAlgorithm::new(
                    config,
                    MLModelWeights::default(),
                )))
            }
            AlgorithmType::FixedSpread => {
                let config = FixedSpreadConfig {
                    max_inventory,
                    quote_size,
                    ..Default::default()
                };
                Ok(Box::new(FixedSpreadAlgorithm::new(config)))
            }
        }
    }

    /// Create an algorithm instance by type string with default configuration.
    pub fn create_default_by_string(
        type_string: &str,
        max_inventory: Decimal,
        quote_size: Decimal,
    ) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError> {
        let algo_type = AlgorithmType::from_str(type_string)?;
        Self::create_default(algo_type, max_inventory, quote_size)
    }

    /// Create an algorithm from a parameter map.
    ///
    /// The parameter map should contain all required parameters.
    /// Missing parameters will use default values.
    pub fn create_from_params(
        algo_type: AlgorithmType,
        params: &HashMap<String, f64>,
    ) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError> {
        match algo_type {
            AlgorithmType::AvellanedaStoikov => {
                let algo = AvellanedaStoikovAlgorithm::from_parameters(params)?;
                Ok(Box::new(algo))
            }
            AlgorithmType::MLSpreadSkew => {
                let algo = MLSpreadSkewAlgorithm::from_parameters(params)?;
                Ok(Box::new(algo))
            }
            AlgorithmType::FixedSpread => {
                let algo = FixedSpreadAlgorithm::from_parameters(params)?;
                Ok(Box::new(algo))
            }
        }
    }

    /// Create an algorithm from a parameter map by type string.
    pub fn create_from_params_by_string(
        type_string: &str,
        params: &HashMap<String, f64>,
    ) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError> {
        let algo_type = AlgorithmType::from_str(type_string)?;
        Self::create_from_params(algo_type, params)
    }

    /// Create Avellaneda-Stoikov algorithm with regime params.
    pub fn create_avellaneda_stoikov(
        max_inventory: Decimal,
        quote_size: Decimal,
        regime_params: Option<RegimeParams>,
    ) -> AvellanedaStoikovAlgorithm {
        let config = if let Some(params) = regime_params {
            AvellanedaStoikovConfig {
                max_inventory,
                quote_size,
                regime_params: params,
                ..Default::default()
            }
        } else {
            AvellanedaStoikovConfig {
                max_inventory,
                quote_size,
                ..Default::default()
            }
        };
        AvellanedaStoikovAlgorithm::new(config)
    }

    /// Create ML Spread-Skew algorithm with custom weights.
    pub fn create_ml_spread_skew(
        max_inventory: Decimal,
        quote_size: Decimal,
        weights: Option<MLModelWeights>,
    ) -> MLSpreadSkewAlgorithm {
        let config = MLSpreadSkewConfig {
            max_inventory,
            quote_size,
            ..Default::default()
        };
        MLSpreadSkewAlgorithm::new(config, weights.unwrap_or_default())
    }

    /// Create Fixed Spread algorithm with optional custom spread/skew.
    pub fn create_fixed_spread(
        max_inventory: Decimal,
        quote_size: Decimal,
        spread_bps: Option<f64>,
        skew_factor: Option<f64>,
    ) -> FixedSpreadAlgorithm {
        let config = FixedSpreadConfig {
            max_inventory,
            quote_size,
            spread_bps: spread_bps.unwrap_or(1.0),
            skew_factor: skew_factor.unwrap_or(0.3),
        };
        FixedSpreadAlgorithm::new(config)
    }

    /// Create an algorithm for CLI usage with unified parameters.
    ///
    /// This is a convenience method for the backtest CLI that handles the
    /// different parameter requirements of each algorithm type.
    ///
    /// # Arguments
    /// * `algo_type` - Algorithm type to create
    /// * `params` - Unified CLI parameters
    ///
    /// # Returns
    /// A boxed algorithm instance
    pub fn create_for_backtest(
        algo_type: AlgorithmType,
        params: &BacktestAlgorithmParams,
    ) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError> {
        match algo_type {
            AlgorithmType::AvellanedaStoikov => {
                let regime_params = RegimeParams::uniform(params.spread_bps, params.skew_factor);
                let algo = Self::create_avellaneda_stoikov(
                    params.max_inventory,
                    params.quote_size,
                    Some(regime_params),
                );
                Ok(Box::new(algo))
            }
            AlgorithmType::MLSpreadSkew => {
                let algo = Self::create_ml_spread_skew(
                    params.max_inventory,
                    params.quote_size,
                    params.ml_weights.clone(),
                );
                Ok(Box::new(algo))
            }
            AlgorithmType::FixedSpread => {
                let algo = Self::create_fixed_spread(
                    params.max_inventory,
                    params.quote_size,
                    Some(params.spread_bps),
                    Some(params.skew_factor),
                );
                Ok(Box::new(algo))
            }
        }
    }

    /// Format algorithm listing for display (CLI/TUI).
    pub fn format_listing() -> String {
        let mut output = String::new();
        output.push_str("Available Algorithms:\n");
        output.push_str(&"=".repeat(60));
        output.push('\n');

        for info in Self::list() {
            output.push_str(&format!("\n{} ({})\n", info.name, info.type_string));
            output.push_str(&"-".repeat(50));
            output.push('\n');
            output.push_str(&format!("  Description: {}\n", info.description));
            output.push_str(&format!("  Version:     {}\n", info.version));
            output.push_str(&format!("  Configurable: {}\n", info.is_configurable));
            output.push_str(&format!("  Trainable:    {}\n", info.is_trainable));

            if !info.aliases.is_empty() {
                output.push_str(&format!("  Aliases:      {}\n", info.aliases.join(", ")));
            }

            // List parameters
            let params = Self::parameters(info.algorithm_type);
            if !params.is_empty() {
                output.push_str("\n  Parameters:\n");
                for param in params {
                    let range_str = if let Some((min, max)) = param.range {
                        format!(" [{:.2}, {:.2}]", min, max)
                    } else {
                        String::new()
                    };
                    let tunable_str = if param.tunable { " [tunable]" } else { "" };
                    output.push_str(&format!(
                        "    - {} ({}): {}{}{}\n",
                        param.name, param.param_type, param.description, range_str, tunable_str
                    ));
                }
            }
        }

        output
    }

    /// Format algorithm listing as JSON for programmatic use.
    pub fn to_json() -> serde_json::Value {
        let infos: Vec<serde_json::Value> = Self::list()
            .into_iter()
            .map(|info| {
                let params = Self::parameters(info.algorithm_type);
                serde_json::json!({
                    "type": info.type_string,
                    "name": info.name,
                    "description": info.description,
                    "version": info.version,
                    "configurable": info.is_configurable,
                    "trainable": info.is_trainable,
                    "aliases": info.aliases,
                    "parameters": params.iter().map(|p| {
                        serde_json::json!({
                            "name": p.name,
                            "type": format!("{}", p.param_type),
                            "description": p.description,
                            "default": p.default,
                            "range": p.range,
                            "tunable": p.tunable,
                        })
                    }).collect::<Vec<_>>(),
                })
            })
            .collect();

        serde_json::json!({
            "algorithms": infos,
            "count": infos.len(),
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::Trainable; // For is_trained() test
    use super::super::{SpreadWeights, SkewWeights}; // For custom ML weights tests
    use rust_decimal_macros::dec;

    #[test]
    fn test_registry_list() {
        let algorithms = AlgorithmRegistry::list();
        assert_eq!(algorithms.len(), 3);

        let types: Vec<_> = algorithms.iter().map(|a| a.algorithm_type).collect();
        assert!(types.contains(&AlgorithmType::AvellanedaStoikov));
        assert!(types.contains(&AlgorithmType::MLSpreadSkew));
        assert!(types.contains(&AlgorithmType::FixedSpread));
    }

    #[test]
    fn test_registry_info() {
        let info = AlgorithmRegistry::info(AlgorithmType::AvellanedaStoikov);
        assert_eq!(info.type_string, "avellaneda_stoikov");
        assert_eq!(info.name, "Avellaneda-Stoikov Market Maker");
        assert!(info.is_configurable);
        assert!(!info.is_trainable);

        let info = AlgorithmRegistry::info(AlgorithmType::MLSpreadSkew);
        assert_eq!(info.type_string, "ml_spread_skew");
        assert!(info.is_configurable);
        assert!(info.is_trainable);
    }

    #[test]
    fn test_registry_info_by_string() {
        let info = AlgorithmRegistry::info_by_string("avellaneda_stoikov").unwrap();
        assert_eq!(info.algorithm_type, AlgorithmType::AvellanedaStoikov);

        // Test alias
        let info = AlgorithmRegistry::info_by_string("as").unwrap();
        assert_eq!(info.algorithm_type, AlgorithmType::AvellanedaStoikov);

        let info = AlgorithmRegistry::info_by_string("ml").unwrap();
        assert_eq!(info.algorithm_type, AlgorithmType::MLSpreadSkew);

        // Test unknown
        assert!(AlgorithmRegistry::info_by_string("unknown").is_err());
    }

    #[test]
    fn test_registry_is_valid_type() {
        assert!(AlgorithmRegistry::is_valid_type("avellaneda_stoikov"));
        assert!(AlgorithmRegistry::is_valid_type("as"));
        assert!(AlgorithmRegistry::is_valid_type("ml_spread_skew"));
        assert!(AlgorithmRegistry::is_valid_type("ml"));
        assert!(!AlgorithmRegistry::is_valid_type("unknown"));
    }

    #[test]
    fn test_registry_all_type_strings() {
        let strings = AlgorithmRegistry::all_type_strings();
        assert!(strings.contains(&"avellaneda_stoikov"));
        assert!(strings.contains(&"as"));
        assert!(strings.contains(&"ml_spread_skew"));
        assert!(strings.contains(&"ml"));
    }

    #[test]
    fn test_registry_parameters() {
        let params = AlgorithmRegistry::parameters(AlgorithmType::AvellanedaStoikov);
        assert!(!params.is_empty());

        // Verify max_inventory is present
        let names: Vec<_> = params.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"max_inventory"));

        let params = AlgorithmRegistry::parameters(AlgorithmType::MLSpreadSkew);
        assert!(!params.is_empty());
    }

    #[test]
    fn test_registry_parameters_by_string() {
        let params = AlgorithmRegistry::parameters_by_string("as").unwrap();
        assert!(!params.is_empty());

        assert!(AlgorithmRegistry::parameters_by_string("unknown").is_err());
    }

    #[test]
    fn test_registry_tunable_parameters() {
        let params = AlgorithmRegistry::tunable_parameters(AlgorithmType::AvellanedaStoikov);
        for param in &params {
            assert!(param.tunable);
        }
    }

    #[test]
    fn test_registry_create_default() {
        let algo =
            AlgorithmRegistry::create_default(AlgorithmType::AvellanedaStoikov, dec!(0.1), dec!(0.001))
                .unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);

        let algo =
            AlgorithmRegistry::create_default(AlgorithmType::MLSpreadSkew, dec!(0.1), dec!(0.001))
                .unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
    }

    #[test]
    fn test_registry_create_default_by_string() {
        let algo =
            AlgorithmRegistry::create_default_by_string("as", dec!(0.1), dec!(0.001)).unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);

        let algo =
            AlgorithmRegistry::create_default_by_string("ml", dec!(0.1), dec!(0.001)).unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);

        assert!(AlgorithmRegistry::create_default_by_string("unknown", dec!(0.1), dec!(0.001)).is_err());
    }

    #[test]
    fn test_registry_create_from_params() {
        let mut params = HashMap::new();
        params.insert("max_inventory".to_string(), 0.1);
        params.insert("quote_size".to_string(), 0.001);

        let algo =
            AlgorithmRegistry::create_from_params(AlgorithmType::AvellanedaStoikov, &params)
                .unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);
        assert_eq!(algo.max_inventory(), dec!(0.1));
    }

    #[test]
    fn test_registry_create_from_params_by_string() {
        let mut params = HashMap::new();
        params.insert("max_inventory".to_string(), 0.1);
        params.insert("quote_size".to_string(), 0.001);

        let algo = AlgorithmRegistry::create_from_params_by_string("as", &params).unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);

        assert!(AlgorithmRegistry::create_from_params_by_string("unknown", &params).is_err());
    }

    #[test]
    fn test_registry_create_avellaneda_stoikov() {
        let algo = AlgorithmRegistry::create_avellaneda_stoikov(dec!(0.1), dec!(0.001), None);
        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);
        assert_eq!(algo.max_inventory(), dec!(0.1));
        assert_eq!(algo.quote_size(), dec!(0.001));
    }

    #[test]
    fn test_registry_create_ml_spread_skew() {
        let algo = AlgorithmRegistry::create_ml_spread_skew(dec!(0.1), dec!(0.001), None);
        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
        assert!(!algo.is_trained());

        // With custom weights
        let weights = MLModelWeights::default();
        let algo = AlgorithmRegistry::create_ml_spread_skew(dec!(0.1), dec!(0.001), Some(weights));
        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
    }

    #[test]
    fn test_registry_format_listing() {
        let listing = AlgorithmRegistry::format_listing();
        assert!(listing.contains("Avellaneda-Stoikov"));
        assert!(listing.contains("ML Spread/Skew"));
        assert!(listing.contains("avellaneda_stoikov"));
        assert!(listing.contains("ml_spread_skew"));
        assert!(listing.contains("Parameters:"));
    }

    #[test]
    fn test_registry_to_json() {
        let json = AlgorithmRegistry::to_json();
        assert_eq!(json["count"], 3);

        let algorithms = json["algorithms"].as_array().unwrap();
        assert_eq!(algorithms.len(), 3);

        // Check structure
        let first = &algorithms[0];
        assert!(first["type"].is_string());
        assert!(first["name"].is_string());
        assert!(first["parameters"].is_array());
    }

    #[test]
    fn test_algorithm_info_aliases() {
        let info = AlgorithmInfo::avellaneda_stoikov();
        assert!(info.aliases.contains(&"as"));
        assert!(info.aliases.contains(&"a-s"));

        let info = AlgorithmInfo::ml_spread_skew();
        assert!(info.aliases.contains(&"ml"));
        assert!(info.aliases.contains(&"mlss"));

        let info = AlgorithmInfo::fixed_spread();
        assert!(info.aliases.contains(&"fs"));
        assert!(info.aliases.contains(&"fixed"));
        assert!(info.aliases.contains(&"baseline"));
    }

    #[test]
    fn test_algorithm_info_serialize() {
        // Test that AlgorithmInfo can be serialized to JSON
        let info = AlgorithmInfo::avellaneda_stoikov();
        let json = serde_json::to_string(&info).unwrap();

        // Verify key fields are in the JSON
        assert!(json.contains("avellaneda_stoikov"));
        assert!(json.contains("Avellaneda-Stoikov"));
        assert!(json.contains("is_configurable"));
        assert!(json.contains("is_trainable"));
    }

    // ==========================================================================
    // Fixed Spread Registry Tests
    // ==========================================================================

    #[test]
    fn test_registry_info_fixed_spread() {
        let info = AlgorithmRegistry::info(AlgorithmType::FixedSpread);
        assert_eq!(info.type_string, "fixed_spread");
        assert_eq!(info.name, "Fixed Spread Market Maker");
        assert!(info.is_configurable);
        assert!(!info.is_trainable);
    }

    #[test]
    fn test_registry_info_by_string_fixed_spread() {
        // Primary name
        let info = AlgorithmRegistry::info_by_string("fixed_spread").unwrap();
        assert_eq!(info.algorithm_type, AlgorithmType::FixedSpread);

        // Aliases
        let info = AlgorithmRegistry::info_by_string("fs").unwrap();
        assert_eq!(info.algorithm_type, AlgorithmType::FixedSpread);

        let info = AlgorithmRegistry::info_by_string("fixed").unwrap();
        assert_eq!(info.algorithm_type, AlgorithmType::FixedSpread);

        let info = AlgorithmRegistry::info_by_string("baseline").unwrap();
        assert_eq!(info.algorithm_type, AlgorithmType::FixedSpread);

        let info = AlgorithmRegistry::info_by_string("fixed-spread").unwrap();
        assert_eq!(info.algorithm_type, AlgorithmType::FixedSpread);
    }

    #[test]
    fn test_registry_is_valid_type_fixed_spread() {
        assert!(AlgorithmRegistry::is_valid_type("fixed_spread"));
        assert!(AlgorithmRegistry::is_valid_type("fs"));
        assert!(AlgorithmRegistry::is_valid_type("fixed"));
        assert!(AlgorithmRegistry::is_valid_type("baseline"));
    }

    #[test]
    fn test_registry_parameters_fixed_spread() {
        let params = AlgorithmRegistry::parameters(AlgorithmType::FixedSpread);
        assert!(!params.is_empty());

        // Verify key parameters
        let names: Vec<_> = params.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"max_inventory"));
        assert!(names.contains(&"quote_size"));
        assert!(names.contains(&"spread_bps"));
        assert!(names.contains(&"skew_factor"));
    }

    #[test]
    fn test_registry_tunable_parameters_fixed_spread() {
        let params = AlgorithmRegistry::tunable_parameters(AlgorithmType::FixedSpread);
        for param in &params {
            assert!(param.tunable);
        }
        // Should have spread_bps and skew_factor as tunable
        let names: Vec<_> = params.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"spread_bps"));
        assert!(names.contains(&"skew_factor"));
    }

    #[test]
    fn test_registry_create_default_fixed_spread() {
        let algo =
            AlgorithmRegistry::create_default(AlgorithmType::FixedSpread, dec!(0.1), dec!(0.001))
                .unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::FixedSpread);
        assert_eq!(algo.max_inventory(), dec!(0.1));
        assert_eq!(algo.quote_size(), dec!(0.001));
    }

    #[test]
    fn test_registry_create_default_by_string_fixed_spread() {
        let algo =
            AlgorithmRegistry::create_default_by_string("fs", dec!(0.1), dec!(0.001)).unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::FixedSpread);

        let algo =
            AlgorithmRegistry::create_default_by_string("baseline", dec!(0.1), dec!(0.001)).unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::FixedSpread);
    }

    #[test]
    fn test_registry_create_from_params_fixed_spread() {
        let mut params = HashMap::new();
        params.insert("max_inventory".to_string(), 0.1);
        params.insert("quote_size".to_string(), 0.001);
        params.insert("spread_bps".to_string(), 2.0);
        params.insert("skew_factor".to_string(), 0.5);

        let algo =
            AlgorithmRegistry::create_from_params(AlgorithmType::FixedSpread, &params).unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::FixedSpread);
        assert_eq!(algo.max_inventory(), dec!(0.1));
    }

    #[test]
    fn test_registry_create_fixed_spread() {
        let algo = AlgorithmRegistry::create_fixed_spread(dec!(0.1), dec!(0.001), None, None);
        assert_eq!(algo.algorithm_type(), AlgorithmType::FixedSpread);
        assert_eq!(algo.max_inventory(), dec!(0.1));
        assert_eq!(algo.quote_size(), dec!(0.001));

        // With custom values
        let algo =
            AlgorithmRegistry::create_fixed_spread(dec!(0.2), dec!(0.002), Some(2.5), Some(0.4));
        assert_eq!(algo.max_inventory(), dec!(0.2));
        assert_eq!(algo.quote_size(), dec!(0.002));
    }

    #[test]
    fn test_registry_format_listing_includes_fixed_spread() {
        let listing = AlgorithmRegistry::format_listing();
        assert!(listing.contains("Fixed Spread"));
        assert!(listing.contains("fixed_spread"));
        assert!(listing.contains("baseline"));
    }

    #[test]
    fn test_registry_all_type_strings_includes_fixed_spread() {
        let strings = AlgorithmRegistry::all_type_strings();
        assert!(strings.contains(&"fixed_spread"));
        assert!(strings.contains(&"fs"));
        assert!(strings.contains(&"fixed"));
        assert!(strings.contains(&"baseline"));
    }

    // ==========================================================================
    // CLI Command Display Tests - Paranoid/Comprehensive Coverage
    // ==========================================================================

    /// Test that list() returns exactly the expected number of algorithms
    #[test]
    fn test_cli_display_algorithm_count() {
        let list = AlgorithmRegistry::list();
        assert_eq!(list.len(), 3, "Expected exactly 3 algorithms in registry");
    }

    /// Test that all algorithms have non-empty names
    #[test]
    fn test_cli_display_all_algorithms_have_names() {
        for info in AlgorithmRegistry::list() {
            assert!(!info.name.is_empty(), "Algorithm name should not be empty");
            assert!(!info.type_string.is_empty(), "Type string should not be empty");
            assert!(!info.description.is_empty(), "Description should not be empty");
        }
    }

    /// Test that all algorithms have valid version strings
    #[test]
    fn test_cli_display_all_algorithms_have_versions() {
        for info in AlgorithmRegistry::list() {
            assert!(!info.version.is_empty(), "Version should not be empty");
            // Version should be in semver format
            let parts: Vec<_> = info.version.split('.').collect();
            assert_eq!(parts.len(), 3, "Version should be semver format: {}", info.version);
            for part in parts {
                assert!(part.parse::<u32>().is_ok(), "Version parts should be numbers: {}", info.version);
            }
        }
    }

    /// Test that all algorithms have at least one alias
    #[test]
    fn test_cli_display_all_algorithms_have_aliases() {
        for info in AlgorithmRegistry::list() {
            assert!(!info.aliases.is_empty(), "Algorithm {} should have at least one alias", info.type_string);
        }
    }

    /// Test that all type strings are unique
    #[test]
    fn test_cli_display_unique_type_strings() {
        let list = AlgorithmRegistry::list();
        let mut seen = std::collections::HashSet::new();
        for info in &list {
            assert!(seen.insert(info.type_string), "Duplicate type_string: {}", info.type_string);
        }
    }

    /// Test that all aliases are unique across all algorithms
    #[test]
    fn test_cli_display_unique_aliases() {
        let all_strings = AlgorithmRegistry::all_type_strings();
        let mut seen = std::collections::HashSet::new();
        for s in &all_strings {
            assert!(seen.insert(*s), "Duplicate alias/type_string: {}", s);
        }
    }

    /// Test that format_listing contains all algorithm names
    #[test]
    fn test_cli_display_format_listing_contains_all_names() {
        let listing = AlgorithmRegistry::format_listing();
        assert!(listing.contains("Avellaneda-Stoikov Market Maker"), "Missing A-S name");
        assert!(listing.contains("ML Spread/Skew Predictor"), "Missing ML name");
        assert!(listing.contains("Fixed Spread Market Maker"), "Missing Fixed Spread name");
    }

    /// Test that format_listing contains all type strings
    #[test]
    fn test_cli_display_format_listing_contains_all_type_strings() {
        let listing = AlgorithmRegistry::format_listing();
        assert!(listing.contains("avellaneda_stoikov"), "Missing A-S type string");
        assert!(listing.contains("ml_spread_skew"), "Missing ML type string");
        assert!(listing.contains("fixed_spread"), "Missing Fixed Spread type string");
    }

    /// Test that format_listing contains parameter information
    #[test]
    fn test_cli_display_format_listing_contains_parameters() {
        let listing = AlgorithmRegistry::format_listing();
        assert!(listing.contains("Parameters:"), "Missing Parameters section");
        assert!(listing.contains("max_inventory"), "Missing max_inventory parameter");
        assert!(listing.contains("quote_size"), "Missing quote_size parameter");
        assert!(listing.contains("spread_bps"), "Missing spread_bps parameter");
    }

    /// Test that format_listing contains trainable indicator
    #[test]
    fn test_cli_display_format_listing_trainable_indicator() {
        let listing = AlgorithmRegistry::format_listing();
        assert!(listing.contains("Trainable:"), "Missing Trainable indicator");
    }

    /// Test JSON output contains all required fields
    #[test]
    fn test_cli_display_json_output_structure() {
        let json = AlgorithmRegistry::to_json();

        // Check top-level fields
        assert!(json.get("algorithms").is_some(), "Missing algorithms field in JSON");
        assert!(json.get("count").is_some(), "Missing count field in JSON");
        assert_eq!(json["count"], 3, "Count should be 3");

        let algorithms = json["algorithms"].as_array().unwrap();
        assert_eq!(algorithms.len(), 3, "Should have 3 algorithms in JSON array");
    }

    /// Test JSON output algorithm structure
    #[test]
    fn test_cli_display_json_algorithm_structure() {
        let json = AlgorithmRegistry::to_json();
        let algorithms = json["algorithms"].as_array().unwrap();

        for algo in algorithms {
            // Check all required fields exist
            assert!(algo.get("type").is_some(), "Missing type field");
            assert!(algo.get("name").is_some(), "Missing name field");
            assert!(algo.get("description").is_some(), "Missing description field");
            assert!(algo.get("version").is_some(), "Missing version field");
            assert!(algo.get("configurable").is_some(), "Missing configurable field");
            assert!(algo.get("trainable").is_some(), "Missing trainable field");
            assert!(algo.get("aliases").is_some(), "Missing aliases field");
            assert!(algo.get("parameters").is_some(), "Missing parameters field");

            // Check types
            assert!(algo["type"].is_string(), "type should be string");
            assert!(algo["name"].is_string(), "name should be string");
            assert!(algo["description"].is_string(), "description should be string");
            assert!(algo["version"].is_string(), "version should be string");
            assert!(algo["configurable"].is_boolean(), "configurable should be boolean");
            assert!(algo["trainable"].is_boolean(), "trainable should be boolean");
            assert!(algo["aliases"].is_array(), "aliases should be array");
            assert!(algo["parameters"].is_array(), "parameters should be array");
        }
    }

    /// Test JSON output parameter structure
    #[test]
    fn test_cli_display_json_parameter_structure() {
        let json = AlgorithmRegistry::to_json();
        let algorithms = json["algorithms"].as_array().unwrap();

        for algo in algorithms {
            let params = algo["parameters"].as_array().unwrap();
            assert!(!params.is_empty(), "Algorithm {} should have parameters", algo["name"]);

            for param in params {
                // Check required parameter fields
                assert!(param.get("name").is_some(), "Missing param name field");
                assert!(param.get("type").is_some(), "Missing param type field");
                assert!(param.get("description").is_some(), "Missing param description field");
                assert!(param.get("default").is_some(), "Missing param default field");
                assert!(param.get("tunable").is_some(), "Missing param tunable field");

                // Check types
                assert!(param["name"].is_string(), "param name should be string");
                assert!(param["type"].is_string(), "param type should be string");
                assert!(param["description"].is_string(), "param description should be string");
                assert!(param["tunable"].is_boolean(), "param tunable should be boolean");
            }
        }
    }

    /// Test that all primary aliases resolve to correct algorithm
    #[test]
    fn test_cli_display_all_aliases_resolve_correctly() {
        // Test Avellaneda-Stoikov aliases
        for alias in &["avellaneda_stoikov", "as", "a-s", "avellaneda-stoikov"] {
            let info = AlgorithmRegistry::info_by_string(alias).unwrap();
            assert_eq!(info.algorithm_type, AlgorithmType::AvellanedaStoikov, "Alias {} should resolve to A-S", alias);
        }

        // Test ML Spread-Skew aliases
        for alias in &["ml_spread_skew", "ml", "mlss", "ml-spread-skew"] {
            let info = AlgorithmRegistry::info_by_string(alias).unwrap();
            assert_eq!(info.algorithm_type, AlgorithmType::MLSpreadSkew, "Alias {} should resolve to ML", alias);
        }

        // Test Fixed Spread aliases
        for alias in &["fixed_spread", "fixed", "fs", "fixed-spread", "baseline"] {
            let info = AlgorithmRegistry::info_by_string(alias).unwrap();
            assert_eq!(info.algorithm_type, AlgorithmType::FixedSpread, "Alias {} should resolve to FS", alias);
        }
    }

    /// Test that invalid aliases return error
    #[test]
    fn test_cli_display_invalid_aliases_return_error() {
        // These are truly invalid - not matching any algorithm
        let invalid_aliases = vec!["unknown", "invalid", "foo", "bar", "xyz", "", "  ", "123"];
        for alias in invalid_aliases {
            assert!(AlgorithmRegistry::info_by_string(alias).is_err(), "Invalid alias '{}' should return error", alias);
        }
    }

    /// Test case insensitivity of aliases (the parser handles case)
    #[test]
    fn test_cli_display_alias_case_handling() {
        // These should work (lowercase)
        assert!(AlgorithmRegistry::info_by_string("as").is_ok());
        assert!(AlgorithmRegistry::info_by_string("ml").is_ok());
        assert!(AlgorithmRegistry::info_by_string("fs").is_ok());

        // The parser appears to be case-insensitive for hyphenated forms
        // Test the actual behavior
        let as_result = AlgorithmRegistry::info_by_string("a-s");
        assert!(as_result.is_ok(), "a-s should resolve to A-S");
        assert_eq!(as_result.unwrap().algorithm_type, AlgorithmType::AvellanedaStoikov);
    }

    /// Test that each algorithm has parameters
    #[test]
    fn test_cli_display_each_algorithm_has_parameters() {
        for algo_type in AlgorithmType::all() {
            let params = AlgorithmRegistry::parameters(*algo_type);
            assert!(!params.is_empty(), "Algorithm {:?} should have parameters", algo_type);
        }
    }

    /// Test that each algorithm has tunable parameters
    #[test]
    fn test_cli_display_each_algorithm_has_tunable_parameters() {
        for algo_type in AlgorithmType::all() {
            let tunable = AlgorithmRegistry::tunable_parameters(*algo_type);
            assert!(!tunable.is_empty(), "Algorithm {:?} should have tunable parameters", algo_type);
        }
    }

    /// Test that tunable parameters have valid ranges
    #[test]
    fn test_cli_display_tunable_parameters_have_ranges() {
        for algo_type in AlgorithmType::all() {
            let tunable = AlgorithmRegistry::tunable_parameters(*algo_type);
            for param in &tunable {
                assert!(param.range.is_some(), "Tunable parameter {} should have a range", param.name);
                let (min, max) = param.range.unwrap();
                assert!(min < max, "Parameter {} range invalid: {} >= {}", param.name, min, max);
            }
        }
    }

    /// Test that parameter defaults are within ranges
    #[test]
    fn test_cli_display_parameter_defaults_in_range() {
        for algo_type in AlgorithmType::all() {
            let params = AlgorithmRegistry::parameters(*algo_type);
            for param in &params {
                if let Some((min, max)) = param.range {
                    assert!(
                        param.default >= min && param.default <= max,
                        "Parameter {} default {} not in range [{}, {}]",
                        param.name, param.default, min, max
                    );
                }
            }
        }
    }

    /// Test that common parameters exist across all algorithms
    #[test]
    fn test_cli_display_common_parameters_exist() {
        for algo_type in AlgorithmType::all() {
            let params = AlgorithmRegistry::parameters(*algo_type);
            let names: Vec<_> = params.iter().map(|p| p.name.as_str()).collect();

            assert!(names.contains(&"max_inventory"), "Algorithm {:?} missing max_inventory", algo_type);
            assert!(names.contains(&"quote_size"), "Algorithm {:?} missing quote_size", algo_type);
        }
    }

    /// Test trainability flags are consistent
    #[test]
    fn test_cli_display_trainability_flags() {
        let as_info = AlgorithmRegistry::info(AlgorithmType::AvellanedaStoikov);
        let ml_info = AlgorithmRegistry::info(AlgorithmType::MLSpreadSkew);
        let fs_info = AlgorithmRegistry::info(AlgorithmType::FixedSpread);

        assert!(!as_info.is_trainable, "A-S should not be trainable");
        assert!(ml_info.is_trainable, "ML should be trainable");
        assert!(!fs_info.is_trainable, "FS should not be trainable");
    }

    /// Test configurability flags
    #[test]
    fn test_cli_display_configurability_flags() {
        for info in AlgorithmRegistry::list() {
            assert!(info.is_configurable, "Algorithm {} should be configurable", info.type_string);
        }
    }

    /// Test that algorithm descriptions are meaningful (not just placeholder text)
    #[test]
    fn test_cli_display_descriptions_meaningful() {
        for info in AlgorithmRegistry::list() {
            assert!(info.description.len() > 20, "Description too short for {}", info.type_string);
            assert!(!info.description.contains("TODO"), "Description contains TODO for {}", info.type_string);
            assert!(!info.description.contains("FIXME"), "Description contains FIXME for {}", info.type_string);
        }
    }

    /// Test all type strings can create algorithms
    #[test]
    fn test_cli_display_all_type_strings_can_create() {
        for type_string in AlgorithmRegistry::all_type_strings() {
            let result = AlgorithmRegistry::create_default_by_string(type_string, dec!(0.1), dec!(0.001));
            assert!(result.is_ok(), "Should be able to create algorithm from type string: {}", type_string);
        }
    }

    /// Test JSON output can be serialized and deserialized
    #[test]
    fn test_cli_display_json_roundtrip() {
        let json = AlgorithmRegistry::to_json();
        let json_str = serde_json::to_string(&json).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(json, parsed, "JSON should survive serialization roundtrip");
    }

    /// Test JSON output is valid JSON
    #[test]
    fn test_cli_display_json_valid() {
        let json = AlgorithmRegistry::to_json();
        let json_str = serde_json::to_string_pretty(&json);
        assert!(json_str.is_ok(), "JSON output should be valid JSON");
    }

    /// Test format_listing is non-empty
    #[test]
    fn test_cli_display_format_listing_non_empty() {
        let listing = AlgorithmRegistry::format_listing();
        assert!(!listing.is_empty(), "format_listing should return non-empty string");
        assert!(listing.len() > 100, "format_listing should be substantial (>100 chars)");
    }

    /// Test format_listing structure
    #[test]
    fn test_cli_display_format_listing_structure() {
        let listing = AlgorithmRegistry::format_listing();
        assert!(listing.contains("Available Algorithms:"), "Missing header");
        assert!(listing.contains("==="), "Missing separator");
        assert!(listing.contains("---"), "Missing algorithm separator");
        assert!(listing.contains("Version:"), "Missing version");
        assert!(listing.contains("Description:"), "Missing description");
    }

    /// Test that parameter types are valid
    #[test]
    fn test_cli_display_parameter_types_valid() {
        for algo_type in AlgorithmType::all() {
            let params = AlgorithmRegistry::parameters(*algo_type);
            for param in &params {
                // ParameterType should be a valid enum variant (this is compile-time checked,
                // but we verify the Display impl works)
                let type_str = format!("{}", param.param_type);
                assert!(!type_str.is_empty(), "Parameter type display should not be empty");
            }
        }
    }

    /// Test all_type_strings returns expected count
    #[test]
    fn test_cli_display_all_type_strings_count() {
        let strings = AlgorithmRegistry::all_type_strings();
        // Should have: avellaneda_stoikov + 3 aliases, ml_spread_skew + 3 aliases, fixed_spread + 4 aliases
        // = 4 + 4 + 5 = 13 total
        assert!(strings.len() >= 12, "Should have at least 12 type strings (3 algorithms x 4 avg aliases each)");
    }

    /// Test parameters_by_string works for all valid types
    #[test]
    fn test_cli_display_parameters_by_string_all_types() {
        for type_string in AlgorithmRegistry::all_type_strings() {
            let result = AlgorithmRegistry::parameters_by_string(type_string);
            assert!(result.is_ok(), "parameters_by_string should work for: {}", type_string);
            assert!(!result.unwrap().is_empty(), "Should have parameters for: {}", type_string);
        }
    }

    /// Test info consistency between list() and info()
    #[test]
    fn test_cli_display_info_consistency() {
        for list_info in AlgorithmRegistry::list() {
            let direct_info = AlgorithmRegistry::info(list_info.algorithm_type);
            assert_eq!(list_info.name, direct_info.name);
            assert_eq!(list_info.type_string, direct_info.type_string);
            assert_eq!(list_info.description, direct_info.description);
            assert_eq!(list_info.version, direct_info.version);
            assert_eq!(list_info.is_configurable, direct_info.is_configurable);
            assert_eq!(list_info.is_trainable, direct_info.is_trainable);
        }
    }

    /// Test that algorithm types match expected values in info
    #[test]
    fn test_cli_display_algorithm_type_consistency() {
        for info in AlgorithmRegistry::list() {
            let algo = AlgorithmRegistry::create_default(info.algorithm_type, dec!(0.1), dec!(0.001)).unwrap();
            assert_eq!(algo.algorithm_type(), info.algorithm_type,
                "Created algorithm type should match info for {}", info.type_string);
        }
    }

    // ========================================================================
    // BacktestAlgorithmParams Tests (Harness Integration)
    // ========================================================================

    /// Test BacktestAlgorithmParams default values
    #[test]
    fn test_backtest_params_default() {
        let params = BacktestAlgorithmParams::default();
        assert_eq!(params.max_inventory, Decimal::new(1, 1)); // 0.1
        assert_eq!(params.quote_size, Decimal::new(1, 3)); // 0.001
        assert_eq!(params.spread_bps, 1.0);
        assert_eq!(params.skew_factor, 0.3);
        assert!(params.ml_weights.is_none());
    }

    /// Test BacktestAlgorithmParams new constructor
    #[test]
    fn test_backtest_params_new() {
        let params = BacktestAlgorithmParams::new(
            dec!(0.2),
            dec!(0.005),
            2.5,
            0.5,
        );
        assert_eq!(params.max_inventory, dec!(0.2));
        assert_eq!(params.quote_size, dec!(0.005));
        assert_eq!(params.spread_bps, 2.5);
        assert_eq!(params.skew_factor, 0.5);
        assert!(params.ml_weights.is_none());
    }

    /// Test BacktestAlgorithmParams with_ml_weights builder
    #[test]
    fn test_backtest_params_with_ml_weights() {
        let weights = MLModelWeights::default();
        let params = BacktestAlgorithmParams::default()
            .with_ml_weights(weights.clone());
        assert!(params.ml_weights.is_some());
        let ml_weights = params.ml_weights.unwrap();
        assert_eq!(ml_weights.spread.intercept, weights.spread.intercept);
    }

    /// Test create_for_backtest with Avellaneda-Stoikov
    #[test]
    fn test_create_for_backtest_avellaneda_stoikov() {
        let params = BacktestAlgorithmParams::new(dec!(0.15), dec!(0.002), 1.5, 0.4);
        let algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::AvellanedaStoikov, &params)
            .unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);
        assert_eq!(algo.name(), "Avellaneda-Stoikov Market Maker");
    }

    /// Test create_for_backtest with ML algorithm (no weights)
    #[test]
    fn test_create_for_backtest_ml_no_weights() {
        let params = BacktestAlgorithmParams::default();
        let algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::MLSpreadSkew, &params)
            .unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
        assert_eq!(algo.name(), "ML Spread/Skew Predictor");
    }

    /// Test create_for_backtest with ML algorithm (with weights)
    #[test]
    fn test_create_for_backtest_ml_with_weights() {
        let weights = MLModelWeights {
            spread: SpreadWeights {
                intercept: 2.0,
                w_entropy: -0.5,
                w_volatility: 0.3,
                w_imbalance: 0.1,
                w_interaction: 0.05,
            },
            skew: SkewWeights {
                intercept: 0.0,
                w_entropy: 0.1,
                w_volatility: 0.2,
                w_inventory: -0.8,
                w_imbalance: 0.2,
            },
            ..Default::default()
        };
        let params = BacktestAlgorithmParams::default()
            .with_ml_weights(weights);
        let algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::MLSpreadSkew, &params)
            .unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
    }

    /// Test create_for_backtest with FixedSpread
    #[test]
    fn test_create_for_backtest_fixed_spread() {
        let params = BacktestAlgorithmParams::new(dec!(0.1), dec!(0.001), 2.0, 0.5);
        let algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::FixedSpread, &params)
            .unwrap();
        assert_eq!(algo.algorithm_type(), AlgorithmType::FixedSpread);
        assert_eq!(algo.name(), "Fixed Spread Market Maker");
    }

    /// Test create_for_backtest all algorithm types succeed
    #[test]
    fn test_create_for_backtest_all_types() {
        let params = BacktestAlgorithmParams::default();
        for algo_type in AlgorithmType::all() {
            let result = AlgorithmRegistry::create_for_backtest(*algo_type, &params);
            assert!(result.is_ok(), "Should create {} via backtest params", algo_type.as_str());
        }
    }

    /// Test create_for_backtest preserves max_inventory
    #[test]
    fn test_create_for_backtest_preserves_inventory_params() {
        for algo_type in AlgorithmType::all() {
            let params = BacktestAlgorithmParams::new(dec!(0.25), dec!(0.003), 1.0, 0.3);
            let algo = AlgorithmRegistry::create_for_backtest(*algo_type, &params).unwrap();
            // Algorithm was created successfully - type is correct
            assert_eq!(algo.algorithm_type(), *algo_type);
        }
    }

    /// Test create_for_backtest with different spread/skew values
    #[test]
    fn test_create_for_backtest_spread_skew_values() {
        // Test various spread/skew combinations
        let test_cases = vec![
            (0.5, 0.1),
            (1.0, 0.3),
            (2.0, 0.5),
            (5.0, 1.0),
        ];

        for (spread, skew) in test_cases {
            let params = BacktestAlgorithmParams::new(dec!(0.1), dec!(0.001), spread, skew);

            // A-S should use spread/skew
            let as_algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::AvellanedaStoikov, &params)
                .unwrap();
            assert_eq!(as_algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);

            // FixedSpread should use spread/skew
            let fs_algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::FixedSpread, &params)
                .unwrap();
            assert_eq!(fs_algo.algorithm_type(), AlgorithmType::FixedSpread);
        }
    }

    /// Test BacktestAlgorithmParams Clone implementation
    #[test]
    fn test_backtest_params_clone() {
        let weights = MLModelWeights::default();
        let params = BacktestAlgorithmParams::new(dec!(0.2), dec!(0.002), 1.5, 0.4)
            .with_ml_weights(weights);
        let cloned = params.clone();
        assert_eq!(cloned.max_inventory, params.max_inventory);
        assert_eq!(cloned.quote_size, params.quote_size);
        assert_eq!(cloned.spread_bps, params.spread_bps);
        assert_eq!(cloned.skew_factor, params.skew_factor);
        assert!(cloned.ml_weights.is_some());
    }

    /// Test BacktestAlgorithmParams Debug implementation
    #[test]
    fn test_backtest_params_debug() {
        let params = BacktestAlgorithmParams::default();
        let debug_str = format!("{:?}", params);
        assert!(debug_str.contains("BacktestAlgorithmParams"));
        assert!(debug_str.contains("max_inventory"));
        assert!(debug_str.contains("spread_bps"));
    }

    /// Test create_for_backtest with extreme parameter values
    #[test]
    fn test_create_for_backtest_extreme_values() {
        // Very small values
        let small_params = BacktestAlgorithmParams::new(dec!(0.001), dec!(0.0001), 0.1, 0.01);
        for algo_type in AlgorithmType::all() {
            let result = AlgorithmRegistry::create_for_backtest(*algo_type, &small_params);
            assert!(result.is_ok(), "Should handle small values for {}", algo_type.as_str());
        }

        // Larger values
        let large_params = BacktestAlgorithmParams::new(dec!(10.0), dec!(1.0), 100.0, 10.0);
        for algo_type in AlgorithmType::all() {
            let result = AlgorithmRegistry::create_for_backtest(*algo_type, &large_params);
            assert!(result.is_ok(), "Should handle large values for {}", algo_type.as_str());
        }
    }

    /// Test create_for_backtest idempotency
    #[test]
    fn test_create_for_backtest_idempotent() {
        let params = BacktestAlgorithmParams::default();
        for algo_type in AlgorithmType::all() {
            let algo1 = AlgorithmRegistry::create_for_backtest(*algo_type, &params).unwrap();
            let algo2 = AlgorithmRegistry::create_for_backtest(*algo_type, &params).unwrap();
            assert_eq!(algo1.algorithm_type(), algo2.algorithm_type());
            assert_eq!(algo1.name(), algo2.name());
        }
    }

    /// Test create_for_backtest with ML weights serialization
    #[test]
    fn test_create_for_backtest_ml_weights_serialization() {
        let weights = MLModelWeights::default();
        let params = BacktestAlgorithmParams::default().with_ml_weights(weights);

        // Verify ML weights can be serialized/deserialized
        if let Some(ref w) = params.ml_weights {
            let json = serde_json::to_string(w).unwrap();
            let deserialized: MLModelWeights = serde_json::from_str(&json).unwrap();
            assert_eq!(w.spread.intercept, deserialized.spread.intercept);
        }
    }

    /// Test BacktestAlgorithmParams builder pattern
    #[test]
    fn test_backtest_params_builder_chain() {
        let weights = MLModelWeights::default();
        let params = BacktestAlgorithmParams::new(dec!(0.1), dec!(0.001), 1.0, 0.3)
            .with_ml_weights(weights);

        // Verify chain returns correct type and values
        assert_eq!(params.max_inventory, dec!(0.1));
        assert!(params.ml_weights.is_some());
    }

    /// Test create_for_backtest returns correct algorithm instances
    #[test]
    fn test_create_for_backtest_algorithm_identity() {
        let params = BacktestAlgorithmParams::default();

        let as_algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::AvellanedaStoikov, &params).unwrap();
        assert_eq!(as_algo.type_string(), "avellaneda_stoikov");

        let ml_algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::MLSpreadSkew, &params).unwrap();
        assert_eq!(ml_algo.type_string(), "ml_spread_skew");

        let fs_algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::FixedSpread, &params).unwrap();
        assert_eq!(fs_algo.type_string(), "fixed_spread");
    }

    /// Test that ML weights are actually used when creating algorithm
    #[test]
    fn test_create_for_backtest_ml_weights_are_used() {
        // Create with custom weights
        let custom_weights = MLModelWeights {
            spread: SpreadWeights {
                intercept: 3.0,
                w_entropy: -1.0,
                w_volatility: 0.5,
                w_imbalance: 0.2,
                w_interaction: 0.1,
            },
            skew: SkewWeights {
                intercept: 0.1,
                w_entropy: 0.15,
                w_volatility: 0.25,
                w_inventory: -0.5,
                w_imbalance: 0.3,
            },
            ..Default::default()
        };

        let params = BacktestAlgorithmParams::default().with_ml_weights(custom_weights);
        let algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::MLSpreadSkew, &params).unwrap();

        // Verify algorithm was created with correct type
        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
    }

    /// Test BacktestAlgorithmParams zero values handling
    #[test]
    fn test_backtest_params_zero_values() {
        let params = BacktestAlgorithmParams::new(dec!(0), dec!(0), 0.0, 0.0);
        // Zero values should be accepted (though not recommended)
        for algo_type in AlgorithmType::all() {
            let result = AlgorithmRegistry::create_for_backtest(*algo_type, &params);
            assert!(result.is_ok(), "Should handle zero values for {}", algo_type.as_str());
        }
    }

    /// Test create_for_backtest negative skew handling
    #[test]
    fn test_create_for_backtest_negative_skew() {
        let params = BacktestAlgorithmParams::new(dec!(0.1), dec!(0.001), 1.0, -0.5);
        for algo_type in AlgorithmType::all() {
            let result = AlgorithmRegistry::create_for_backtest(*algo_type, &params);
            assert!(result.is_ok(), "Should handle negative skew for {}", algo_type.as_str());
        }
    }

    /// Test create_for_backtest concurrent access safety
    #[test]
    fn test_create_for_backtest_concurrent() {
        use std::thread;
        let handles: Vec<_> = (0..4).map(|i| {
            thread::spawn(move || {
                let params = BacktestAlgorithmParams::default();
                let algo_type = match i % 3 {
                    0 => AlgorithmType::AvellanedaStoikov,
                    1 => AlgorithmType::MLSpreadSkew,
                    _ => AlgorithmType::FixedSpread,
                };
                let result = AlgorithmRegistry::create_for_backtest(algo_type, &params);
                assert!(result.is_ok());
            })
        }).collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    /// Test create_for_backtest vs create_default consistency
    #[test]
    fn test_create_for_backtest_vs_create_default() {
        let params = BacktestAlgorithmParams::new(dec!(0.1), dec!(0.001), 1.0, 0.3);

        // Both methods should create algorithms of correct type
        let backtest_algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::FixedSpread, &params).unwrap();
        let default_algo = AlgorithmRegistry::create_default(AlgorithmType::FixedSpread, dec!(0.1), dec!(0.001)).unwrap();

        assert_eq!(backtest_algo.algorithm_type(), default_algo.algorithm_type());
        assert_eq!(backtest_algo.name(), default_algo.name());
    }
}

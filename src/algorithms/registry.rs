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
//! use ingestor::algorithms::registry::AlgorithmRegistry;
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
use crate::market_maker::AvellanedaStoikovConfig;

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
}

//! Algorithm Abstractions and Implementations
//!
//! This module provides a trait-based architecture for market making algorithms,
//! enabling easy comparison, backtesting, and extension of different strategies.
//!
//! # Design Principles
//!
//! 1. **Trait-based polymorphism**: All algorithms implement `MarketMakingAlgorithm`
//! 2. **Type identification**: Each algorithm has a unique `AlgorithmType` for serialization
//! 3. **Preserve primitives**: Original implementations are kept intact for comparison
//! 4. **Composability**: Algorithms can be swapped at runtime via trait objects
//!
//! # Available Algorithms
//!
//! - `AvellanedaStoikovMM`: Classic Avellaneda-Stoikov market making (2008)
//! - `MLSpreadSkew`: ML-based spread/skew predictor using learned linear weights
//!
//! # Usage
//!
//! ```ignore
//! use crate::strategies::{MarketMakingAlgorithm, AlgorithmType, create_algorithm};
//!
//! // Create algorithm by type
//! let algo = create_algorithm(AlgorithmType::AvellanedaStoikov, config)?;
//!
//! // Use polymorphically
//! let quotes = algo.compute_quotes(&market_state);
//! ```

pub mod traits;
pub mod trading_algorithm;
pub mod avellaneda_stoikov;
pub mod ml_spread_skew;
pub mod fixed_spread;
pub mod momentum;
pub mod market_making_trading;
pub mod registry;
pub mod factory;

pub use traits::{
    MarketMakingAlgorithm,
    AlgorithmType,
    AlgorithmConfig,
    MarketInput,
    AlgorithmError,
    // Parameter system
    ParameterType,
    ParameterDefinition,
    // Configurable trait
    Configurable,
    // Trainable trait and supporting types
    Trainable,
    TrainingData,
    TrainingResult,
};
pub use avellaneda_stoikov::AvellanedaStoikovAlgorithm;
pub use ml_spread_skew::{
    MLSpreadSkewAlgorithm,
    MLSpreadSkewConfig,
    MLModelWeights,
    SpreadWeights,
    SkewWeights,
    TrainingInfo,
    param_names as ml_param_names,
    weight_names as ml_weight_names,
};
pub use fixed_spread::{
    FixedSpreadAlgorithm,
    FixedSpreadConfig,
    param_names as fixed_spread_param_names,
};
pub use momentum::{
    MomentumAlgorithm,
    MomentumAlgorithmFactory,
    MomentumConfig,
    MomentumSignal,
};
pub use market_making_trading::{
    MarketMakingTradingAlgorithm,
    MarketMakingTradingAlgorithmFactory,
    MMTradingConfig,
};
pub use registry::{AlgorithmRegistry, AlgorithmInfo, BacktestAlgorithmParams};
pub use factory::{AlgorithmFactory, AlgorithmFactoryError, AlgorithmPreview};

// TradingAlgorithm trait and types (Task 3.0)
pub use trading_algorithm::{
    TradingAlgorithm, TradingAlgorithmFactory, TradingAlgorithmError,
    TradingDecision, TradingAction, TradingInput,
    PositionDirection, AlgorithmState,
};

// Re-export common types from market_maker for convenience
pub use crate::execution::market_maker::{
    MMQuotes,
    Quote,
    QuoteSide,
    Fill,
    PnLTracker,
    MMState,
    MarketRegime,
    RegimeThresholds,
    RegimeParams,
    RegimeConfig,
};

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;

// ============================================================================
// Feature Computation Utilities
// ============================================================================

/// Compute normalized entropy score from tick entropy features.
///
/// Combines multi-timeframe entropy readings into a single 0-1 score.
/// Higher score = more random/mean-reverting market (good for MM).
/// Lower score = trending market (dangerous for MM).
///
/// # Arguments
/// * `tick_entropy_1s` - 1-second tick entropy
/// * `tick_entropy_5s` - 5-second tick entropy
/// * `tick_entropy_10s` - 10-second tick entropy
///
/// # Returns
/// Normalized entropy score between 0.0 and 1.0
pub fn compute_entropy_score(
    tick_entropy_1s: Option<Decimal>,
    tick_entropy_5s: Option<Decimal>,
    tick_entropy_10s: Option<Decimal>,
) -> f64 {
    const MAX_ENTROPY: f64 = 1.585; // log2(3) for 3-state system

    let mut sum = 0.0;
    let mut count = 0;

    if let Some(e) = tick_entropy_1s {
        sum += e.to_f64().unwrap_or(0.0);
        count += 1;
    }
    if let Some(e) = tick_entropy_5s {
        sum += e.to_f64().unwrap_or(0.0);
        count += 1;
    }
    if let Some(e) = tick_entropy_10s {
        sum += e.to_f64().unwrap_or(0.0);
        count += 1;
    }

    if count == 0 {
        return 0.5; // Default to medium entropy
    }

    let avg_entropy = sum / count as f64;
    (avg_entropy / MAX_ENTROPY).clamp(0.0, 1.0)
}

/// Compute flow imbalance from buy/sell volumes.
///
/// # Arguments
/// * `aggr_buy_vol` - Aggregated buy volume
/// * `aggr_sell_vol` - Aggregated sell volume
///
/// # Returns
/// Imbalance ratio between -1.0 (all sells) and 1.0 (all buys)
pub fn compute_flow_imbalance(aggr_buy_vol: Decimal, aggr_sell_vol: Decimal) -> f64 {
    let total = aggr_buy_vol + aggr_sell_vol;
    if total == dec!(0) {
        return 0.0;
    }

    let imbalance = (aggr_buy_vol - aggr_sell_vol) / total;
    imbalance.to_f64().unwrap_or(0.0)
}

// ============================================================================
// Algorithm Factory
// ============================================================================

/// Factory function to create algorithms by type
pub fn create_algorithm(
    algo_type: AlgorithmType,
    max_inventory: Decimal,
    quote_size: Decimal,
    regime_params: Option<RegimeParams>,
) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError> {
    match algo_type {
        AlgorithmType::AvellanedaStoikov => {
            let config = if let Some(params) = regime_params {
                crate::execution::market_maker::AvellanedaStoikovConfig {
                    max_inventory,
                    quote_size,
                    regime_params: params,
                    ..Default::default()
                }
            } else {
                crate::execution::market_maker::AvellanedaStoikovConfig {
                    max_inventory,
                    quote_size,
                    ..Default::default()
                }
            };
            Ok(Box::new(AvellanedaStoikovAlgorithm::new(config)))
        }
        AlgorithmType::MLSpreadSkew => {
            // Create ML algorithm with default weights
            // Use regime params for base values if available
            let config = MLSpreadSkewConfig {
                max_inventory,
                quote_size,
                ..Default::default()
            };
            Ok(Box::new(MLSpreadSkewAlgorithm::new(config, MLModelWeights::default())))
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

/// Factory function to create ML algorithm with custom weights
pub fn create_ml_algorithm(
    max_inventory: Decimal,
    quote_size: Decimal,
    weights: MLModelWeights,
) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError> {
    let config = MLSpreadSkewConfig {
        max_inventory,
        quote_size,
        ..Default::default()
    };
    Ok(Box::new(MLSpreadSkewAlgorithm::new(config, weights)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_create_algorithm_avellaneda_stoikov() {
        let algo = create_algorithm(
            AlgorithmType::AvellanedaStoikov,
            dec!(0.1),
            dec!(0.001),
            None,
        ).unwrap();

        assert_eq!(algo.algorithm_type(), AlgorithmType::AvellanedaStoikov);
        assert_eq!(algo.type_string(), "avellaneda_stoikov");
        assert_eq!(algo.name(), "Avellaneda-Stoikov Market Maker");
    }

    #[test]
    fn test_create_algorithm_ml_spread_skew() {
        let algo = create_algorithm(
            AlgorithmType::MLSpreadSkew,
            dec!(0.1),
            dec!(0.001),
            None,
        ).unwrap();

        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
        assert_eq!(algo.type_string(), "ml_spread_skew");
        assert_eq!(algo.name(), "ML Spread/Skew Predictor");
    }

    #[test]
    fn test_create_ml_algorithm_with_weights() {
        let weights = MLModelWeights::default();
        let algo = create_ml_algorithm(dec!(0.1), dec!(0.001), weights).unwrap();

        assert_eq!(algo.algorithm_type(), AlgorithmType::MLSpreadSkew);
    }

    #[test]
    fn test_algorithm_type_string_roundtrip() {
        let original = AlgorithmType::AvellanedaStoikov;
        let type_str = original.as_str();
        let parsed = AlgorithmType::from_str(type_str).unwrap();
        assert_eq!(original, parsed);

        // Also test MLSpreadSkew roundtrip
        let ml_original = AlgorithmType::MLSpreadSkew;
        let ml_type_str = ml_original.as_str();
        let ml_parsed = AlgorithmType::from_str(ml_type_str).unwrap();
        assert_eq!(ml_original, ml_parsed);
    }

    #[test]
    fn test_compute_entropy_score_with_values() {
        let score = compute_entropy_score(
            Some(dec!(1.0)),
            Some(dec!(1.2)),
            Some(dec!(1.4)),
        );
        // Average = 1.2, normalized = 1.2 / 1.585 ≈ 0.757
        assert!(score > 0.7 && score < 0.8);
    }

    #[test]
    fn test_compute_entropy_score_none_values() {
        let score = compute_entropy_score(None, None, None);
        assert_eq!(score, 0.5); // Default to medium entropy
    }

    #[test]
    fn test_compute_entropy_score_partial_values() {
        let score = compute_entropy_score(Some(dec!(0.8)), None, None);
        // 0.8 / 1.585 ≈ 0.505
        assert!(score > 0.45 && score < 0.55);
    }

    #[test]
    fn test_compute_flow_imbalance_balanced() {
        let imbalance = compute_flow_imbalance(dec!(100), dec!(100));
        assert_eq!(imbalance, 0.0);
    }

    #[test]
    fn test_compute_flow_imbalance_buy_heavy() {
        let imbalance = compute_flow_imbalance(dec!(100), dec!(0));
        assert_eq!(imbalance, 1.0);
    }

    #[test]
    fn test_compute_flow_imbalance_sell_heavy() {
        let imbalance = compute_flow_imbalance(dec!(0), dec!(100));
        assert_eq!(imbalance, -1.0);
    }

    #[test]
    fn test_compute_flow_imbalance_zero_volume() {
        let imbalance = compute_flow_imbalance(dec!(0), dec!(0));
        assert_eq!(imbalance, 0.0);
    }
}

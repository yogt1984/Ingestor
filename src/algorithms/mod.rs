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
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::algorithms::{MarketMakingAlgorithm, AlgorithmType, create_algorithm};
//!
//! // Create algorithm by type
//! let algo = create_algorithm(AlgorithmType::AvellanedaStoikov, config)?;
//!
//! // Use polymorphically
//! let quotes = algo.compute_quotes(&market_state);
//! ```

pub mod traits;
pub mod avellaneda_stoikov;

pub use traits::{
    MarketMakingAlgorithm,
    AlgorithmType,
    AlgorithmConfig,
    MarketInput,
    AlgorithmError,
};
pub use avellaneda_stoikov::AvellanedaStoikovAlgorithm;

// Re-export common types from market_maker for convenience
pub use crate::market_maker::{
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
                crate::market_maker::AvellanedaStoikovConfig {
                    max_inventory,
                    quote_size,
                    regime_params: params,
                    ..Default::default()
                }
            } else {
                crate::market_maker::AvellanedaStoikovConfig {
                    max_inventory,
                    quote_size,
                    ..Default::default()
                }
            };
            Ok(Box::new(AvellanedaStoikovAlgorithm::new(config)))
        }
    }
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
    fn test_algorithm_type_string_roundtrip() {
        let original = AlgorithmType::AvellanedaStoikov;
        let type_str = original.as_str();
        let parsed = AlgorithmType::from_str(type_str).unwrap();
        assert_eq!(original, parsed);
    }
}

//! Feature Engineering Module
//!
//! This module provides feature extraction engines for market microstructure analysis:
//!
//! - **Feature Fusion**: Central snapshot combining all feature types
//! - **Illiquidity**: Roll spread, Amihud's lambda, Kyle's lambda, VPIN
//! - **Entropy**: Tick entropy at multiple timescales
//! - **Volatility**: Realized volatility, bipower variation, jump detection
//! - **Toxicity**: Adverse selection metrics, toxic flow ratios
//! - **Trend Features**: Momentum, monotonicity, Hurst exponent, MA crossover
//! - **Signal Processing**: Kalman filter for velocity/acceleration estimation

pub mod feature_fusion;
pub mod illiquidity;
pub mod entropy;
pub mod volatility;
pub mod toxicity;
pub mod trend_features;
pub mod signal_processing;

// Feature fusion (central snapshot)
pub use feature_fusion::{FeatureFusionEngine, FeaturesSnapshot};

// Illiquidity metrics
pub use illiquidity::{IlliquidityEngine, IlliquidityMetrics, IlliquidityConfig};

// Entropy metrics
pub use entropy::{EntropyEngine, EntropyMetrics, EntropyConfig};

// Volatility metrics
pub use volatility::{VolatilityEngine, VolatilityMetrics, VolatilityConfig};

// Toxicity metrics
pub use toxicity::{ToxicityEngine, ToxicityMetrics, ToxicityConfig};

// Trend features
pub use trend_features::{
    TrendFeatureEngine,
    TrendFeatures,
    TrendFeatureConfig,
};

// Signal processing
pub use signal_processing::{
    KalmanFilter,
    KalmanConfig,
    KalmanState,
    MultiSymbolKalman,
};

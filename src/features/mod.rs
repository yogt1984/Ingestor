//! Feature Engineering Module for MARS (Momentum Adaptive Regime Strategy)
//!
//! This module provides enhanced feature extraction for trend-following and
//! regime detection. It extends the base feature set with:
//!
//! - **Trend Features**: Momentum, monotonicity, Hurst exponent, MA crossover
//! - **Signal Processing**: Kalman filter for velocity/acceleration (planned)
//! - **Cross-Asset Features**: Correlation, joint momentum (planned)
//!
//! # Architecture
//!
//! ```text
//! Price Series → TrendFeatureEngine → TrendFeatures
//!                      │
//!                      ├─→ Momentum (linear regression slope)
//!                      ├─→ Monotonicity (directional consistency)
//!                      ├─→ Hurst Exponent (trend vs mean-reversion)
//!                      └─→ MA Crossover (EMA difference)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::features::{TrendFeatureEngine, TrendFeatures};
//!
//! let mut engine = TrendFeatureEngine::new(60); // 60-tick window
//! engine.update(100.0);
//! engine.update(101.0);
//! // ... more updates
//! let features = engine.compute();
//! println!("Momentum: {:?}", features.momentum);
//! ```

pub mod trend_features;

pub use trend_features::{
    TrendFeatureEngine,
    TrendFeatures,
    TrendFeatureConfig,
};

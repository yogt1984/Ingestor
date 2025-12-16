//! Feature Engineering Module for MARS (Momentum Adaptive Regime Strategy)
//!
//! This module provides enhanced feature extraction for trend-following and
//! regime detection. It extends the base feature set with:
//!
//! - **Trend Features**: Momentum, monotonicity, Hurst exponent, MA crossover
//! - **Signal Processing**: Kalman filter for velocity/acceleration estimation
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
//!
//! Price Series → KalmanFilter → KalmanState
//!                      │
//!                      ├─→ Position (smoothed price)
//!                      ├─→ Velocity (rate of change / momentum)
//!                      └─→ Acceleration (momentum change / reversal indicator)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::features::{TrendFeatureEngine, TrendFeatures};
//! use ingestor::features::{KalmanFilter, KalmanConfig};
//!
//! // Trend features
//! let mut engine = TrendFeatureEngine::new(60); // 60-tick window
//! engine.update(100.0);
//! engine.update(101.0);
//! let features = engine.compute();
//! println!("Momentum: {:?}", features.momentum);
//!
//! // Kalman filter
//! let mut kalman = KalmanFilter::new(KalmanConfig::default());
//! let state = kalman.update(100.0);
//! println!("Velocity: {}", state.velocity);
//! ```

pub mod trend_features;
pub mod signal_processing;

pub use trend_features::{
    TrendFeatureEngine,
    TrendFeatures,
    TrendFeatureConfig,
};

pub use signal_processing::{
    KalmanFilter,
    KalmanConfig,
    KalmanState,
    MultiSymbolKalman,
};

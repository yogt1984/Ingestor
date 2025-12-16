//! Enhanced Forward Testing Infrastructure
//!
//! Provides comprehensive tools for validating backtested strategies against live data:
//!
//! - **A/B Testing**: Compare multiple algorithms simultaneously with statistical rigor
//! - **Statistical Significance**: T-tests and bootstrap confidence intervals
//! - **Regime Monitoring**: Track performance across market regimes
//! - **Drift Detection**: Alert when live performance diverges from backtest expectations
//!
//! # Usage
//!
//! ```ignore
//! use crate::forward_testing::{ABTestManager, StatisticalTest, RegimeMonitor};
//!
//! // Set up A/B test between two strategies
//! let mut ab_test = ABTestManager::new(ABTestConfig::default());
//! ab_test.add_variant("control", preset_a);
//! ab_test.add_variant("treatment", preset_b);
//!
//! // Process events and get statistical comparison
//! let result = ab_test.analyze();
//! if result.is_significant() {
//!     println!("Winner: {}", result.winner());
//! }
//! ```

mod ab_testing;
mod core;
mod drift_detection;
mod regime_monitor;
mod statistical;

pub use ab_testing::*;
pub use core::*;
pub use drift_detection::*;
pub use regime_monitor::*;
pub use statistical::*;

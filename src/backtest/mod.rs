//! Backtesting Infrastructure
//!
//! Provides historical replay and strategy evaluation capabilities.
//!
//! # Components
//!
//! - `replay`: Parquet file reader with time-ordered event streaming
//! - `harness`: Run strategies on historical data
//! - `metrics`: Performance measurement (Sharpe, drawdown, etc.)
//! - `fill_simulator`: Realistic fill simulation with queue position modeling
//! - `walk_forward`: Walk-forward validation to prevent overfitting
//! - `data_quality`: Data validation and quality checks
//! - `statistics`: Statistical significance testing (PSR, DSR, bootstrap CI)
//! - `oos_validation`: Out-of-sample validation and overfitting detection
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::backtest::{BacktestEngine, BacktestConfig};
//!
//! let config = BacktestConfig::default();
//! let mut engine = BacktestEngine::new(config)?;
//!
//! // Load historical data
//! engine.load_data("./data/features")?;
//!
//! // Run backtest
//! let results = engine.run()?;
//!
//! // Analyze results
//! println!("Sharpe: {:.2}", results.sharpe_ratio());
//! println!("Max Drawdown: {:.2}%", results.max_drawdown() * 100.0);
//! ```

pub mod replay;
pub mod harness;
pub mod metrics;
pub mod fill_simulator;
pub mod walk_forward;
pub mod data_quality;
pub mod statistics;
pub mod oos_validation;
pub mod multi_objective;

pub use replay::{ParquetReplay, ReplayEvent, ReplayConfig};
pub use harness::{BacktestEngine, BacktestConfig, BacktestResults};
pub use metrics::{PerformanceMetrics, TradeLog, EquityCurve};
pub use fill_simulator::{FillSimulator, FillSimulatorConfig, FillEvent, MarketState};
pub use walk_forward::{WalkForwardEngine, WalkForwardConfig, WalkForwardResults};
pub use data_quality::{DataQualityReport, DataValidator};
pub use statistics::{StatisticalReport, SignificanceVerdict, compute_statistics};
pub use oos_validation::{OOSValidator, OOSConfig, ValidationReport, OverfitVerdict};
pub use multi_objective::{MultiObjectiveOptimizer, MOConfig, MOResults, Objective};

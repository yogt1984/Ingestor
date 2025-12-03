//! Backtesting Infrastructure
//!
//! Provides historical replay and strategy evaluation capabilities.
//!
//! # Components
//!
//! - `replay`: Parquet file reader with time-ordered event streaming
//! - `harness`: Run strategies on historical data
//! - `metrics`: Performance measurement (Sharpe, drawdown, etc.)
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

pub use replay::{ParquetReplay, ReplayEvent, ReplayConfig};
pub use harness::{BacktestEngine, BacktestConfig, BacktestResults};
pub use metrics::{PerformanceMetrics, TradeLog, EquityCurve};
pub use fill_simulator::{FillSimulator, FillSimulatorConfig, FillEvent, MarketState};

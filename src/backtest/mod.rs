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
//! - `regime_optimizer`: Regime-specific parameter optimization
//! - `ml_trainer`: ML weight training with grid search optimization
//! - `validation_campaign`: 4-week validation campaign infrastructure
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
pub mod regime_optimizer;
pub mod ml_trainer;
pub mod walk_forward_ml;
pub mod paper_validation;
pub mod session_runner;
pub mod validation_campaign;

pub use replay::{ParquetReplay, ReplayEvent, ReplayConfig};
pub use harness::{BacktestEngine, BacktestConfig, BacktestResults};
pub use metrics::{PerformanceMetrics, TradeLog, EquityCurve};
pub use fill_simulator::{FillSimulator, FillSimulatorConfig, FillEvent, MarketState};
pub use walk_forward::{WalkForwardEngine, WalkForwardConfig, WalkForwardResults};
pub use data_quality::{DataQualityReport, DataValidator};
pub use statistics::{StatisticalReport, SignificanceVerdict, compute_statistics};
pub use oos_validation::{OOSValidator, OOSConfig, ValidationReport, OverfitVerdict};
pub use multi_objective::{MultiObjectiveOptimizer, MOConfig, MOResults, Objective};
pub use regime_optimizer::{RegimeOptimizer, RegimeOptimizerConfig, RegimeOptimizationResults};
pub use ml_trainer::{MLTrainer, MLTrainerConfig, MLTrainingResults};
pub use walk_forward_ml::{WalkForwardMLTrainer, WalkForwardMLConfig, WalkForwardMLResults};
pub use paper_validation::{
    SessionValidator, ValidationConfig, SessionValidationReport,
    AggregatedValidationReport, Verdict, ValidationCheck,
};
pub use session_runner::{
    SessionRunner, SessionRunnerConfig, SessionState, SessionProgress,
    SessionResult, FillRateStats, SimulatedEvent, SimulatedTrade,
};
pub use validation_campaign::{
    ValidationCampaign, CampaignConfig, CampaignStatus, CampaignProgress,
    CampaignReport, CampaignMetrics, WeeklyMetrics, DailyMetrics,
    ValidationGates, GateResult, ValidationVerdict,
};

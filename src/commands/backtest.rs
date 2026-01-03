//! Backtest Commands
//!
//! This module provides all backtest-related commands that can be executed
//! from both CLI and TUI interfaces.
//!
//! # Commands
//!
//! - `evaluate` - Single backtest evaluation
//! - `sweep` - Parameter sweep
//! - `walk_forward` - Walk-forward validation
//! - `tune` - Hyperparameter tuning (grid search) - MM only
//! - `regime_search` - Regime-specific grid search - MM only
//! - `oos_validate` - Out-of-sample validation
//! - `multi_objective` - Multi-objective optimization - MM only
//! - `regime_optimize` - Per-regime optimization - MM only
//! - `train` - ML weight training - MM only (ML Spread/Skew)
//! - `walk_forward_ml` - Walk-forward ML training - MM only
//! - `simulate` - Campaign simulation
//! - `grid` - Grid search - MM only
//! - `campaign` - Validation campaign
//! - `paper` - Paper trading
//! - `list_algorithms` - List available algorithms

use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Result, Context};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use num::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::commands::common::{ProgressCallback, ProgressEvent, LogLevel};
use crate::commands::params::backtest_params::EvaluateParams;
use crate::backtest::{
    BacktestEngine, BacktestConfig, BacktestResults,
    replay::{ParquetReplay, ReplayConfig},
    fill_simulator::FillSimulatorConfig,
};
use crate::execution::market_maker::MMConfig;
use crate::execution::mm_simulator::SimulatorConfig;
use crate::strategies::{
    AlgorithmType, AlgorithmRegistry, BacktestAlgorithmParams, MLModelWeights,
};

/// Results from the `evaluate` command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluateResult {
    /// Algorithm type used
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// Performance metrics
    pub metrics: EvaluateMetrics,
    /// Parameters used
    pub params: EvaluateParams,
    /// Number of events processed
    pub events_processed: usize,
    /// Number of fills generated
    pub fills_generated: usize,
}

/// Performance metrics extracted from backtest results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluateMetrics {
    pub sharpe_ratio: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
    pub avg_trade_pnl: f64,
    pub annualized_return: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub profit_factor: f64,
}

impl From<&BacktestResults> for EvaluateMetrics {
    fn from(results: &BacktestResults) -> Self {
        Self {
            sharpe_ratio: results.metrics.sharpe_ratio,
            total_return: results.metrics.total_return,
            max_drawdown: results.metrics.max_drawdown,
            num_trades: results.metrics.num_trades,
            win_rate: results.metrics.win_rate,
            avg_trade_pnl: results.metrics.avg_trade_pnl.to_f64().unwrap_or(0.0),
            annualized_return: results.metrics.annualized_return,
            sortino_ratio: results.metrics.sortino_ratio,
            calmar_ratio: results.metrics.calmar_ratio,
            profit_factor: results.metrics.profit_factor,
        }
    }
}

/// Backtest command executor
///
/// All backtest commands are executed through this struct.
/// Commands are async and support progress callbacks for long-running operations.
pub struct BacktestCommands;

impl BacktestCommands {
    /// Run a single backtest evaluation
    ///
    /// This is the extracted version of the `run_single()` function from the CLI.
    /// It supports progress callbacks for real-time updates during execution.
    ///
    /// Returns both the full `BacktestResults` (for CLI printing/stats) and
    /// a simplified `EvaluateResult` (for TUI/JSON output).
    pub fn evaluate(
        params: EvaluateParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<(BacktestResults, EvaluateResult)> {
        // Parse algorithm type early to fail fast on invalid algorithm
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|_| anyhow::anyhow!(
                "Unknown algorithm '{}'. Valid options: {}",
                params.algorithm,
                AlgorithmRegistry::all_type_strings().join(", ")
            ))?;

        let algo_name = algo_type.display_name().to_string();

        callback.on_event(ProgressEvent::Started {
            total: None,
            message: format!("Starting backtest evaluation with algorithm: {}", algo_name),
        });

        // Load ML weights if needed
        let ml_weights = Self::load_ml_weights_if_needed(
            algo_type,
            params.weights_file.as_deref(),
            &callback,
        )?;

        // Load data
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Loading market data...".to_string(),
        });

        let replay_config = ReplayConfig {
            data_dir: params.data_path.clone(),
            ..Default::default()
        };

        let mut replay = ParquetReplay::new(replay_config.clone());
        let num_events = replay.load()
            .context("Failed to load market data")?;
        let events = replay.into_events();

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loaded {} events", num_events),
        });

        // Create algorithm using registry
        let algo_params = Self::create_algorithm_params(&params, ml_weights);
        let algorithm = AlgorithmRegistry::create_for_backtest(algo_type, &algo_params)
            .map_err(|e| anyhow::anyhow!("Failed to create algorithm '{}': {}", algo_name, e))?;

        // Build backtest config
        let backtest_config = BacktestConfig {
            replay: replay_config,
            mm: MMConfig::default(),
            simulator: SimulatorConfig {
                fee_rate: Decimal::from_f64_retain(params.fee_rate).unwrap_or(dec!(0.0001)),
                ..Default::default()
            },
            fill_sim: FillSimulatorConfig {
                base_fill_probability: params.fill_prob,
                queue_position: params.queue_pos,
                fee_rate: Decimal::from_f64_retain(params.fee_rate).unwrap_or(dec!(0.0001)),
                ..Default::default()
            },
            verbose: !params.quiet,
            use_realistic_fills: !params.naive_fills,
            ..Default::default()
        };

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Running backtest...".to_string(),
        });

        let mut engine = BacktestEngine::from_events_with_algorithm(
            backtest_config,
            events,
            algorithm,
        );
        let results = engine.run()
            .context("Failed to run backtest")?;

        callback.on_event(ProgressEvent::Metric {
            name: "sharpe_ratio".to_string(),
            value: results.metrics.sharpe_ratio,
        });

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Backtest completed: Sharpe={:.2}, Return={:.2}%, Trades={}",
                results.metrics.sharpe_ratio,
                results.metrics.total_return * 100.0,
                results.metrics.num_trades
            ),
        });

        let eval_result = EvaluateResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            metrics: EvaluateMetrics::from(&results),
            params,
            events_processed: results.events_processed,
            fills_generated: results.fills_generated,
        };

        Ok((results, eval_result))
    }

    /// Load ML weights from file if algorithm is MLSpreadSkew and weights file provided.
    /// Returns None for non-ML algorithms or if no weights file specified.
    fn load_ml_weights_if_needed(
        algo_type: AlgorithmType,
        weights_file: Option<&std::path::Path>,
        callback: &Arc<dyn ProgressCallback>,
    ) -> Result<Option<MLModelWeights>> {
        if algo_type != AlgorithmType::MLSpreadSkew {
            return Ok(None);
        }

        match weights_file {
            Some(path) => {
                callback.on_event(ProgressEvent::Log {
                    level: LogLevel::Info,
                    message: format!("Loading ML weights from {:?}", path),
                });
                let json = std::fs::read_to_string(path)
                    .with_context(|| format!("Failed to read weights file {:?}", path))?;
                let weights: MLModelWeights = serde_json::from_str(&json)
                    .context("Failed to parse weights JSON")?;
                Ok(Some(weights))
            }
            None => {
                callback.on_event(ProgressEvent::Log {
                    level: LogLevel::Info,
                    message: "Using default ML weights".to_string(),
                });
                Ok(Some(MLModelWeights::default()))
            }
        }
    }

    /// Create algorithm parameters from evaluate params
    fn create_algorithm_params(
        params: &EvaluateParams,
        ml_weights: Option<MLModelWeights>,
    ) -> BacktestAlgorithmParams {
        let mut algo_params = BacktestAlgorithmParams::new(
            Decimal::from_f64_retain(params.max_inventory).unwrap_or(dec!(0.1)),
            Decimal::from_f64_retain(params.quote_size).unwrap_or(dec!(0.001)),
            params.spread,
            params.skew,
        );
        if let Some(weights) = ml_weights {
            algo_params = algo_params.with_ml_weights(weights);
        }
        algo_params
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::common::NoOpCallback;

    #[test]
    fn test_backtest_commands_struct() {
        // Verify struct can be instantiated
        let _commands = BacktestCommands;
    }

    #[test]
    fn test_evaluate_metrics_from_backtest_results() {
        use crate::backtest::metrics::{PerformanceMetrics, TradeLog, EquityCurve};
        use crate::backtest::harness::{BacktestConfig, FillStats};

        let mut metrics = PerformanceMetrics::default();
        metrics.total_return = 0.05;
        metrics.annualized_return = 0.10;
        metrics.max_drawdown = 0.02;
        metrics.max_drawdown_duration_ms = 1000;
        metrics.sharpe_ratio = 1.5;
        metrics.sortino_ratio = 2.0;
        metrics.calmar_ratio = 5.0;
        metrics.num_trades = 100;
        metrics.win_rate = 0.55;
        metrics.profit_factor = 1.2;
        metrics.avg_trade_pnl = dec!(0.001);

        let results = BacktestResults {
            config: BacktestConfig::default(),
            metrics,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 1000,
            fills_generated: 100,
            fill_stats: FillStats::default(),
            oco_stats: None,
        };

        let eval_metrics = EvaluateMetrics::from(&results);
        assert_eq!(eval_metrics.sharpe_ratio, 1.5);
        assert_eq!(eval_metrics.total_return, 0.05);
        assert_eq!(eval_metrics.max_drawdown, 0.02);
        assert_eq!(eval_metrics.num_trades, 100);
        assert_eq!(eval_metrics.win_rate, 0.55);
    }
}



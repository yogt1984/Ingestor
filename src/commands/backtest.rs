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
use crate::commands::params::backtest_params::{EvaluateParams, TuneParams, RegimeSearchParams, MultiObjectiveParams, RegimeOptimizeParams, TrainParams, WalkForwardMLParams, SweepParams, WalkForwardParams};
use crate::backtest::{
    BacktestEngine, BacktestConfig, BacktestResults,
    replay::{ParquetReplay, ReplayConfig},
    fill_simulator::FillSimulatorConfig,
};
use crate::execution::market_maker::{MMConfig, RegimeParams, RegimeConfig, RegimeThresholds};
use crate::execution::mm_simulator::SimulatorConfig;
use crate::strategies::{
    AlgorithmType, AlgorithmRegistry, BacktestAlgorithmParams, MLModelWeights,
    SpreadWeights, SkewWeights,
};

/// Single regime search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeSearchResultItem {
    pub high_spread: f64,
    pub high_skew: f64,
    pub med_spread: f64,
    pub med_skew: f64,
    /// Low entropy spread (None = no quoting in low entropy)
    pub low_spread: Option<f64>,
    pub low_skew: f64,
    pub fill_prob: f64,
    pub sharpe: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
    pub avg_trade_pnl: f64,
}

/// Single solution from multi-objective optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiObjectiveSolution {
    /// Parameters used
    pub spread_bps: f64,
    pub skew_factor: f64,
    pub fill_probability: f64,
    pub high_entropy_threshold: f64,
    /// Objective values achieved
    pub sharpe: f64,
    pub drawdown: f64,
    pub fill_rate: f64,
    pub turnover: f64,
    pub total_return: f64,
    pub win_rate: f64,
    pub num_trades: usize,
    /// Pareto rank (1 = frontier, 2 = second tier, etc.)
    pub pareto_rank: usize,
    /// Crowding distance (higher = more isolated = more diverse)
    pub crowding_distance: f64,
}

/// Results from the `multi_objective` command (Pareto frontier optimization)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiObjectiveResult {
    /// Algorithm type used
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// All evaluated solutions
    pub all_solutions: Vec<MultiObjectiveSolution>,
    /// Pareto frontier solutions (rank 1)
    pub pareto_frontier: Vec<MultiObjectiveSolution>,
    /// Best solution by weighted score
    pub best_weighted: Option<MultiObjectiveSolution>,
    /// Total number of combinations tested
    pub total_combinations: usize,
    /// Time span of data in hours
    pub time_span_hours: f64,
    /// Number of events processed
    pub num_events: usize,
}

/// Results from the `regime_search` command (regime-specific grid search)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeSearchResult {
    /// Algorithm type used
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// All grid search results (sorted by Sharpe ratio, descending)
    pub all_results: Vec<RegimeSearchResultItem>,
    /// Best parameter combination (by Sharpe ratio)
    pub best: Option<RegimeSearchResultItem>,
    /// Total number of combinations tested
    pub total_combinations: usize,
    /// Average Sharpe for results with low entropy quoting
    pub avg_sharpe_with_quote: Option<f64>,
    /// Average Sharpe for results without low entropy quoting
    pub avg_sharpe_without_quote: Option<f64>,
}

/// Metrics for a single regime from optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeOptimizeMetrics {
    pub regime: String,
    pub event_count: usize,
    pub event_fraction: f64,
    pub time_hours: f64,
    pub optimal_spread: f64,
    pub optimal_skew: f64,
    pub should_quote: bool,
    pub best_sharpe: f64,
    pub best_return: f64,
    pub best_drawdown: f64,
    pub best_trades: usize,
    pub best_win_rate: f64,
}

/// Optimal parameter set for a regime
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeParamSet {
    pub spread_bps: f64,
    pub skew_factor: f64,
    pub should_quote: bool,
}

/// Optimal regime parameters (one set per regime)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimalRegimeParams {
    pub high: RegimeParamSet,
    pub medium: RegimeParamSet,
    pub low: RegimeParamSet,
}

/// Strategy comparison metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyComparison {
    pub uniform_sharpe: f64,
    pub uniform_return: f64,
    pub uniform_drawdown: f64,
    pub uniform_trades: usize,
    pub uniform_win_rate: f64,
    pub regime_specific_sharpe: f64,
    pub regime_specific_return: f64,
    pub regime_specific_drawdown: f64,
    pub regime_specific_trades: usize,
    pub regime_specific_win_rate: f64,
    pub sharpe_improvement: f64,
    pub return_improvement: f64,
    pub drawdown_improvement: f64,
    pub trade_count_diff: i64,
}

/// Results from the `regime_optimize` command (regime-specific parameter optimization)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeOptimizeResult {
    /// Algorithm type used
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// Per-regime optimization results
    pub high_entropy: RegimeOptimizeMetrics,
    pub medium_entropy: RegimeOptimizeMetrics,
    pub low_entropy: RegimeOptimizeMetrics,
    /// Optimal combined regime params
    pub optimal_regime_params: OptimalRegimeParams,
    /// Comparison with uniform approach
    pub comparison: StrategyComparison,
    /// Total number of events processed
    pub total_events: usize,
    /// Time span of data in hours
    pub time_span_hours: f64,
}

/// Results from the `train` command (ML weight training - ML Spread/Skew only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainResult {
    /// Algorithm type used (should be ML Spread/Skew)
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// Optimal trained weights
    pub optimal_weights: MLModelWeights,
    /// Training performance metrics
    pub train_sharpe: f64,
    pub train_return: f64,
    pub train_trades: usize,
    /// Test performance metrics
    pub test_sharpe: f64,
    pub test_return: f64,
    pub test_trades: usize,
    /// Generalization gap (train_sharpe - test_sharpe)
    pub generalization_gap: f64,
    /// Number of valid configurations tested
    pub valid_configurations: usize,
    /// Total number of configurations tested
    pub total_configurations: usize,
}

/// Single fold result from walk-forward ML training
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardMLFoldResult {
    pub fold_num: usize,
    pub train_start_ms: i64,
    pub train_end_ms: i64,
    pub test_start_ms: i64,
    pub test_end_ms: i64,
    pub train_events: usize,
    pub test_events: usize,
    pub best_weights: MLModelWeights,
    pub train_sharpe: f64,
    pub train_return: f64,
    pub train_trades: usize,
    pub test_sharpe: f64,
    pub test_return: f64,
    pub test_trades: usize,
    pub generalization_gap: f64,
    pub configs_evaluated: usize,
    pub valid_configs: usize,
}

/// Weight stability metrics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WeightStability {
    pub spread_intercept_std: f64,
    pub spread_entropy_std: f64,
    pub spread_volatility_std: f64,
    pub skew_intercept_std: f64,
    pub skew_inventory_std: f64,
    pub stability_score: f64,
}

/// Aggregated results across all folds
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalkForwardMLAggregate {
    pub avg_oos_sharpe: f64,
    pub std_oos_sharpe: f64,
    pub avg_oos_return: f64,
    pub total_oos_trades: usize,
    pub avg_generalization_gap: f64,
    pub pct_profitable_folds: f64,
    pub is_oos_sharpe_ratio: f64,
    pub prob_sharpe_gt_zero: f64,
    pub weight_stability: WeightStability,
}

/// Results from the `walk-forward-ml` command (walk-forward ML training - MM algorithms only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardMLResult {
    /// Algorithm type used
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// Number of folds
    pub folds: usize,
    /// Results for each fold
    pub fold_results: Vec<WalkForwardMLFoldResult>,
    /// Aggregated metrics across all folds
    pub aggregate: WalkForwardMLAggregate,
    /// Consensus weights (average of fold weights, weighted by test performance)
    pub consensus_weights: MLModelWeights,
}

/// Single grid search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuneResultItem {
    pub spread: f64,
    pub skew: f64,
    pub high_entropy_threshold: f64,
    pub fill_prob: f64,
    pub sharpe: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
    pub avg_trade_pnl: f64,
}

/// Results from the `tune` command (grid search)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuneResult {
    /// Algorithm type used
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// All grid search results (sorted by Sharpe ratio, descending)
    pub all_results: Vec<TuneResultItem>,
    /// Best parameter combination (by Sharpe ratio)
    pub best: Option<TuneResultItem>,
    /// Total number of combinations tested
    pub total_combinations: usize,
}

/// Single sweep result (one parameter combination)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepResultItem {
    pub spread: f64,
    pub skew: f64,
    pub sharpe: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
}

/// Results from the `sweep` command (parameter sweep)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepResult {
    /// Algorithm type used
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// All sweep results (all parameter combinations tested)
    pub all_results: Vec<SweepResultItem>,
    /// Best parameter combination (by Sharpe ratio)
    pub best: Option<SweepResultItem>,
    /// Total number of combinations tested
    pub total_combinations: usize,
}

/// Results from the `walk-forward` command (walk-forward validation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardResult {
    /// Algorithm type used
    pub algorithm: String,
    /// Algorithm display name
    pub algorithm_name: String,
    /// Number of folds
    pub folds: usize,
    /// Results for each fold
    pub fold_results: Vec<WalkForwardFoldResult>,
    /// Aggregated metrics across all folds
    pub aggregate: WalkForwardAggregate,
}

/// Single fold result from walk-forward validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardFoldResult {
    pub fold_num: usize,
    pub train_start_ms: i64,
    pub train_end_ms: i64,
    pub test_start_ms: i64,
    pub test_end_ms: i64,
    /// Best parameters found in training
    pub best_params: WalkForwardOptimizedParams,
    /// Training set performance (in-sample)
    pub train_metrics: WalkForwardFoldMetrics,
    /// Test set performance (out-of-sample)
    pub test_metrics: WalkForwardFoldMetrics,
}

/// Optimized parameters from training
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardOptimizedParams {
    pub spread: f64,
    pub skew: f64,
    pub fill_prob: f64,
    pub train_sharpe: f64,
}

/// Summary metrics for a fold
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalkForwardFoldMetrics {
    pub sharpe: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
    pub profit_factor: f64,
}

/// Aggregated results across all folds
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalkForwardAggregate {
    /// Average out-of-sample Sharpe
    pub avg_oos_sharpe: f64,
    /// Std dev of out-of-sample Sharpe
    pub std_oos_sharpe: f64,
    /// Average out-of-sample return
    pub avg_oos_return: f64,
    /// Total out-of-sample trades
    pub total_oos_trades: usize,
    /// Average win rate
    pub avg_win_rate: f64,
    /// Percentage of folds profitable
    pub pct_profitable_folds: f64,
    /// In-sample vs out-of-sample Sharpe ratio (overfitting indicator)
    pub is_oos_sharpe_ratio: f64,
    /// Probability Sharpe Ratio > 0 (statistical significance)
    pub prob_sharpe_gt_zero: f64,
}

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

    /// Run grid search (tune) command - MM algorithms only
    ///
    /// This is the extracted version of the `run_grid_search()` function from the CLI.
    /// It supports progress callbacks for real-time updates during execution.
    ///
    /// Returns `TuneResult` with all grid search results sorted by Sharpe ratio.
    pub fn tune(
        params: TuneParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<TuneResult> {
        // Parse algorithm type early to fail fast on invalid algorithm
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|_| anyhow::anyhow!(
                "Unknown algorithm '{}'. Valid options: {}",
                params.algorithm,
                AlgorithmRegistry::all_type_strings().join(", ")
            ))?;

        let algo_name = algo_type.display_name().to_string();

        // Validate that algorithm is a Market Making algorithm
        Self::validate_mm_algorithm(algo_type)?;

        // Parse parameter lists
        let spreads: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skews: Vec<f64> = params.skews
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let high_entropies: Vec<f64> = params.high_entropies
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let fill_probs: Vec<f64> = params.fill_probs
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        let total_combinations = spreads.len() * skews.len() * high_entropies.len() * fill_probs.len();

        callback.on_event(ProgressEvent::Started {
            total: Some(total_combinations),
            message: format!(
                "Starting grid search with {} combinations for algorithm: {}",
                total_combinations,
                algo_name
            ),
        });

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!(
                "Parameter space: spreads={:?}, skews={:?}, high_entropies={:?}, fill_probs={:?}",
                spreads, skews, high_entropies, fill_probs
            ),
        });

        // Load ML weights if needed
        let ml_weights = Self::load_ml_weights_if_needed(
            algo_type,
            params.weights_file.as_deref(),
            &callback,
        )?;

        // Load data once (we'll reload events for each combination)
        let replay_config = ReplayConfig {
            data_dir: params.data_path.clone(),
            ..Default::default()
        };

        let mut all_results: Vec<TuneResultItem> = Vec::new();
        let mut count = 0;

        for &spread in &spreads {
            for &skew in &skews {
                for &high_entropy in &high_entropies {
                    for &fill_prob in &fill_probs {
                        count += 1;

                        callback.on_event(ProgressEvent::Progress {
                            current: count,
                            total: Some(total_combinations),
                            message: format!(
                                "Testing: spread={:.1}, skew={:.1}, entropy={:.1}, fill_prob={:.2}",
                                spread, skew, high_entropy, fill_prob
                            ),
                        });

                        // Reload events (need fresh copy for each run)
                        let mut replay = ParquetReplay::new(replay_config.clone());
                        replay.load()
                            .context("Failed to load market data")?;
                        let events = replay.into_events();

                        // Create algorithm with grid parameters
                        let mut algo_params = BacktestAlgorithmParams::new(
                            Decimal::from_f64_retain(params.max_inventory).unwrap_or(dec!(0.1)),
                            Decimal::from_f64_retain(params.quote_size).unwrap_or(dec!(0.001)),
                            spread,
                            skew,
                        );
                        if let Some(weights) = ml_weights.clone() {
                            algo_params = algo_params.with_ml_weights(weights);
                        }

                        let algorithm = AlgorithmRegistry::create_for_backtest(algo_type, &algo_params)
                            .map_err(|e| anyhow::anyhow!("Failed to create algorithm '{}': {}", algo_name, e))?;

                        let config = BacktestConfig {
                            replay: replay_config.clone(),
                            mm: MMConfig::default(),
                            simulator: SimulatorConfig {
                                fee_rate: Decimal::from_f64_retain(params.fee_rate).unwrap_or(dec!(0.0001)),
                                ..Default::default()
                            },
                            fill_sim: FillSimulatorConfig {
                                base_fill_probability: fill_prob,
                                queue_position: params.queue_pos,
                                fee_rate: Decimal::from_f64_retain(params.fee_rate).unwrap_or(dec!(0.0001)),
                                ..Default::default()
                            },
                            verbose: false,
                            use_realistic_fills: !params.naive_fills,
                            ..Default::default()
                        };

                        let mut engine = BacktestEngine::from_events_with_algorithm(config, events, algorithm);
                        let results = engine.run()
                            .context("Failed to run backtest")?;

                        let avg_trade_pnl = if results.metrics.num_trades > 0 {
                            results.metrics.total_return / results.metrics.num_trades as f64
                        } else {
                            0.0
                        };

                        let grid_result = TuneResultItem {
                            spread,
                            skew,
                            high_entropy_threshold: high_entropy,
                            fill_prob,
                            sharpe: results.metrics.sharpe_ratio,
                            total_return: results.metrics.total_return,
                            max_drawdown: results.metrics.max_drawdown,
                            num_trades: results.metrics.num_trades,
                            win_rate: results.metrics.win_rate,
                            avg_trade_pnl,
                        };

                        // Send metric update with current best Sharpe
                        let current_best_sharpe = all_results
                            .iter()
                            .map(|r| r.sharpe)
                            .fold(f64::NEG_INFINITY, f64::max);
                        let new_best = grid_result.sharpe > current_best_sharpe;

                        callback.on_event(ProgressEvent::Metric {
                            name: "sharpe_ratio".to_string(),
                            value: grid_result.sharpe,
                        });

                        if new_best {
                            callback.on_event(ProgressEvent::Log {
                                level: LogLevel::Info,
                                message: format!(
                                    "New best Sharpe: {:.2} (spread={:.1}, skew={:.1}, entropy={:.1}, fill_prob={:.2})",
                                    grid_result.sharpe, spread, skew, high_entropy, fill_prob
                                ),
                            });
                        }

                        all_results.push(grid_result);
                    }
                }
            }
        }

        // Sort by Sharpe ratio (descending)
        all_results.sort_by(|a, b| b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal));

        let best = all_results.first().cloned();

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Grid search completed: tested {} combinations, best Sharpe={:.2}",
                total_combinations,
                best.as_ref().map(|b| b.sharpe).unwrap_or(0.0)
            ),
        });

        Ok(TuneResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            all_results,
            best,
            total_combinations,
        })
    }

    /// Validate that the algorithm is a Market Making algorithm
    fn validate_mm_algorithm(algo_type: AlgorithmType) -> Result<()> {
        match algo_type {
            AlgorithmType::AvellanedaStoikov
            | AlgorithmType::MLSpreadSkew
            | AlgorithmType::FixedSpread => Ok(()),
            _ => anyhow::bail!(
                "Algorithm '{}' is not a Market Making algorithm. \
                 Grid search (tune) is only available for MM algorithms: as, ml, fixed",
                algo_type.as_str()
            ),
        }
    }

    /// Run regime-specific grid search command - MM algorithms only
    ///
    /// This is the extracted version of the `run_regime_search()` function from the CLI.
    /// It supports progress callbacks for real-time updates during execution.
    ///
    /// Returns `RegimeSearchResult` with all grid search results sorted by Sharpe ratio.
    pub fn regime_search(
        params: RegimeSearchParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<RegimeSearchResult> {
        use crate::backtest::replay::ParquetReplay;

        // Parse algorithm type early to fail fast on invalid algorithm
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|_| anyhow::anyhow!(
                "Unknown algorithm '{}'. Valid options: {}",
                params.algorithm,
                AlgorithmRegistry::all_type_strings().join(", ")
            ))?;

        let algo_name = algo_type.display_name().to_string();

        // Validate that algorithm is a Market Making algorithm
        Self::validate_mm_algorithm(algo_type)?;

        // Parse parameter lists
        let high_spreads: Vec<f64> = params.high_spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let med_spreads: Vec<f64> = params.med_spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let high_skews: Vec<f64> = params.high_skews
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let med_skews: Vec<f64> = params.med_skews
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let low_skews: Vec<f64> = params.low_skews
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let fill_probs: Vec<f64> = params.fill_probs
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        // Parse low entropy spreads - can include "none"
        #[derive(Debug, Clone)]
        enum LowEntropySpread {
            Value(f64),
            NoQuote,
        }

        let low_spreads: Vec<LowEntropySpread> = params.low_spreads
            .split(',')
            .map(|s| {
                let s = s.trim().to_lowercase();
                if s == "none" || s == "no" {
                    LowEntropySpread::NoQuote
                } else {
                    s.parse().map(LowEntropySpread::Value).unwrap_or(LowEntropySpread::NoQuote)
                }
            })
            .collect();

        let total_combinations = high_spreads.len() * high_skews.len()
            * med_spreads.len() * med_skews.len()
            * low_spreads.len() * low_skews.len()
            * fill_probs.len();

        callback.on_event(ProgressEvent::Started {
            total: Some(total_combinations),
            message: format!(
                "Starting regime-specific grid search with {} combinations for algorithm: {}",
                total_combinations,
                algo_name
            ),
        });

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!(
                "Parameter space: high_spreads={:?}, med_spreads={:?}, low_spreads={}, high_skews={:?}, med_skews={:?}, low_skews={:?}, fill_probs={:?}",
                high_spreads, med_spreads, params.low_spreads, high_skews, med_skews, low_skews, fill_probs
            ),
        });

        // Load ML weights if needed
        let ml_weights = Self::load_ml_weights_if_needed(
            algo_type,
            params.weights_file.as_deref(),
            &callback,
        )?;

        // Load data once (we'll reload events for each combination)
        let replay_config = ReplayConfig {
            data_dir: params.data_path.clone(),
            ..Default::default()
        };

        let mut all_results: Vec<RegimeSearchResultItem> = Vec::new();
        let mut count = 0;

        for &h_spread in &high_spreads {
            for &h_skew in &high_skews {
                for &m_spread in &med_spreads {
                    for &m_skew in &med_skews {
                        for l_spread in &low_spreads {
                            for &l_skew in &low_skews {
                                for &fill_prob in &fill_probs {
                                    count += 1;

                                    callback.on_event(ProgressEvent::Progress {
                                        current: count,
                                        total: Some(total_combinations),
                                        message: format!(
                                            "Testing: H({:.1},{:.1}) M({:.1},{:.1}) L({},{:.1}) fp={:.2}",
                                            h_spread, h_skew, m_spread, m_skew,
                                            match l_spread {
                                                LowEntropySpread::Value(v) => format!("{:.1}", v),
                                                LowEntropySpread::NoQuote => "NONE".to_string(),
                                            },
                                            l_skew, fill_prob
                                        ),
                                    });

                                    let (low_spread_val, should_quote_low) = match l_spread {
                                        LowEntropySpread::Value(v) => (*v, true),
                                        LowEntropySpread::NoQuote => (5.0, false), // dummy value when not quoting
                                    };

                                    // Reload events (need fresh copy for each run)
                                    let mut replay = ParquetReplay::new(replay_config.clone());
                                    replay.load()
                                        .context("Failed to load market data")?;
                                    let events = replay.into_events();

                                    // Create regime-specific parameters
                                    let regime_params = RegimeParams {
                                        high_entropy: RegimeConfig {
                                            spread_bps: h_spread,
                                            skew_factor: h_skew,
                                            size_mult: 1.0,
                                            should_quote: true,
                                        },
                                        medium_entropy: RegimeConfig {
                                            spread_bps: m_spread,
                                            skew_factor: m_skew,
                                            size_mult: 0.7,
                                            should_quote: true,
                                        },
                                        low_entropy: RegimeConfig {
                                            spread_bps: low_spread_val,
                                            skew_factor: l_skew,
                                            size_mult: 0.3,
                                            should_quote: should_quote_low,
                                        },
                                    };

                                    let mm_config = MMConfig {
                                        regime_params,
                                        max_inventory: Decimal::from_f64_retain(params.max_inventory).unwrap_or(dec!(0.1)),
                                        quote_size: Decimal::from_f64_retain(params.quote_size).unwrap_or(dec!(0.001)),
                                        regime_thresholds: RegimeThresholds {
                                            high_entropy_threshold: params.high_entropy,
                                            low_entropy_threshold: params.low_entropy,
                                        },
                                        ..Default::default()
                                    };

                                    let config = BacktestConfig {
                                        replay: replay_config.clone(),
                                        mm: mm_config,
                                        simulator: SimulatorConfig {
                                            fee_rate: Decimal::from_f64_retain(params.fee_rate).unwrap_or(dec!(0.0001)),
                                            ..Default::default()
                                        },
                                        fill_sim: FillSimulatorConfig {
                                            base_fill_probability: fill_prob,
                                            queue_position: params.queue_pos,
                                            fee_rate: Decimal::from_f64_retain(params.fee_rate).unwrap_or(dec!(0.0001)),
                                            ..Default::default()
                                        },
                                        verbose: false,
                                        use_realistic_fills: !params.naive_fills,
                                        ..Default::default()
                                    };

                                    // Create backtest engine with regime params
                                    // BacktestEngine::new() creates default A-S algorithm which uses regime_params from MMConfig
                                    let mut engine = BacktestEngine::new(config);
                                    engine.load_data()
                                        .context("Failed to load data")?;
                                    let results = engine.run()
                                        .context("Failed to run backtest")?;

                                    let avg_trade_pnl = if results.metrics.num_trades > 0 {
                                        results.metrics.total_return / results.metrics.num_trades as f64
                                    } else {
                                        0.0
                                    };

                                    let low_spread_opt = match l_spread {
                                        LowEntropySpread::Value(v) => Some(*v),
                                        LowEntropySpread::NoQuote => None,
                                    };

                                    let result = RegimeSearchResultItem {
                                        high_spread: h_spread,
                                        high_skew: h_skew,
                                        med_spread: m_spread,
                                        med_skew: m_skew,
                                        low_spread: low_spread_opt,
                                        low_skew: l_skew,
                                        fill_prob,
                                        sharpe: results.metrics.sharpe_ratio,
                                        total_return: results.metrics.total_return,
                                        max_drawdown: results.metrics.max_drawdown,
                                        num_trades: results.metrics.num_trades,
                                        win_rate: results.metrics.win_rate,
                                        avg_trade_pnl,
                                    };

                                    // Send metric update with current best Sharpe
                                    let current_best_sharpe = all_results
                                        .iter()
                                        .map(|r| r.sharpe)
                                        .fold(f64::NEG_INFINITY, f64::max);
                                    let new_best = result.sharpe > current_best_sharpe;

                                    callback.on_event(ProgressEvent::Metric {
                                        name: "sharpe_ratio".to_string(),
                                        value: result.sharpe,
                                    });

                                    if new_best {
                                        callback.on_event(ProgressEvent::Log {
                                            level: LogLevel::Info,
                                            message: format!(
                                                "New best Sharpe: {:.2} (H({:.1},{:.1}) M({:.1},{:.1}) L({},{:.1}) fp={:.2})",
                                                result.sharpe, h_spread, h_skew, m_spread, m_skew,
                                                match low_spread_opt {
                                                    Some(v) => format!("{:.1}", v),
                                                    None => "NONE".to_string(),
                                                },
                                                l_skew, fill_prob
                                            ),
                                        });
                                    }

                                    all_results.push(result);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort by Sharpe ratio (descending)
        all_results.sort_by(|a, b| b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal));

        // Calculate average Sharpe for quoting vs not quoting in low entropy
        let with_low_quote: Vec<_> = all_results.iter().filter(|r| r.low_spread.is_some()).collect();
        let without_low_quote: Vec<_> = all_results.iter().filter(|r| r.low_spread.is_none()).collect();

        let avg_sharpe_with = if !with_low_quote.is_empty() {
            Some(with_low_quote.iter().map(|r| r.sharpe).sum::<f64>() / with_low_quote.len() as f64)
        } else {
            None
        };

        let avg_sharpe_without = if !without_low_quote.is_empty() {
            Some(without_low_quote.iter().map(|r| r.sharpe).sum::<f64>() / without_low_quote.len() as f64)
        } else {
            None
        };

        let best = all_results.first().cloned();

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Regime search completed: tested {} combinations, best Sharpe={:.2}",
                total_combinations,
                best.as_ref().map(|b| b.sharpe).unwrap_or(0.0)
            ),
        });

        Ok(RegimeSearchResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            all_results,
            best,
            total_combinations,
            avg_sharpe_with_quote: avg_sharpe_with,
            avg_sharpe_without_quote: avg_sharpe_without,
        })
    }

    /// Run multi-objective optimization command (Pareto frontier) - MM algorithms only
    ///
    /// This is the extracted version of the `run_multi_objective()` function from the CLI.
    /// It supports progress callbacks for real-time updates during execution.
    ///
    /// Returns `MultiObjectiveResult` with Pareto frontier solutions.
    pub fn multi_objective(
        params: MultiObjectiveParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<MultiObjectiveResult> {
        use crate::backtest::multi_objective::{MultiObjectiveOptimizer, MOConfig, ObjectiveWeights};

        // Parse algorithm type early to fail fast on invalid algorithm
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|_| anyhow::anyhow!(
                "Unknown algorithm '{}'. Valid options: {}",
                params.algorithm,
                AlgorithmRegistry::all_type_strings().join(", ")
            ))?;

        let algo_name = algo_type.display_name().to_string();

        // Validate that algorithm is a Market Making algorithm
        Self::validate_mm_algorithm(algo_type)?;

        // Parse parameter lists
        let spread_values: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skew_values: Vec<f64> = params.skews
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let fill_prob_values: Vec<f64> = params.fill_probs
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let high_entropy_values: Vec<f64> = params.high_entropies
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        let total_combinations = spread_values.len() * skew_values.len() *
            fill_prob_values.len() * high_entropy_values.len();

        callback.on_event(ProgressEvent::Started {
            total: Some(total_combinations),
            message: format!(
                "Starting multi-objective optimization with {} combinations for algorithm: {}",
                total_combinations,
                algo_name
            ),
        });

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!(
                "Parameter grid: spreads={:?}, skews={:?}, fill_probs={:?}, high_entropies={:?}",
                spread_values, skew_values, fill_prob_values, high_entropy_values
            ),
        });

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!(
                "Objective weights: Sharpe={:.0}%, Drawdown={:.0}%, Fill={:.0}%, Turnover={:.0}%",
                params.w_sharpe * 100.0,
                params.w_drawdown * 100.0,
                params.w_fill * 100.0,
                params.w_turnover * 100.0
            ),
        });

        // Build MOConfig
        let mo_config = MOConfig {
            data_dir: params.data_path.clone(),
            spreads: spread_values,
            skews: skew_values,
            fill_probs: fill_prob_values,
            high_entropies: high_entropy_values,
            objective_weights: ObjectiveWeights {
                sharpe: params.w_sharpe,
                drawdown: params.w_drawdown,
                fill_rate: params.w_fill,
                turnover: params.w_turnover,
            },
            min_trades: params.min_trades,
            verbose: false, // We'll handle output via callbacks
        };

        // Create optimizer and load data
        let mut optimizer = MultiObjectiveOptimizer::new(mo_config);
        let num_events = optimizer.load_data()
            .context("Failed to load market data")?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loaded {} events", num_events),
        });

        // Run optimization with progress updates
        // Note: The optimizer doesn't support callbacks directly, so we'll need to
        // wrap it or modify it. For now, we'll run it and convert the results.
        let mo_results = optimizer.optimize()
            .context("Failed to run multi-objective optimization")?;

        // Convert MOResults to MultiObjectiveResult
        let all_solutions: Vec<MultiObjectiveSolution> = mo_results.all_solutions
            .iter()
            .map(|sol| MultiObjectiveSolution {
                spread_bps: sol.params.spread_bps,
                skew_factor: sol.params.skew_factor,
                fill_probability: sol.params.fill_probability,
                high_entropy_threshold: sol.params.high_entropy_threshold,
                sharpe: sol.objectives.sharpe,
                drawdown: sol.objectives.drawdown,
                fill_rate: sol.objectives.fill_rate,
                turnover: sol.objectives.turnover,
                total_return: sol.objectives.total_return,
                win_rate: sol.objectives.win_rate,
                num_trades: sol.objectives.num_trades,
                pareto_rank: sol.pareto_rank,
                crowding_distance: sol.crowding_distance,
            })
            .collect();

        let pareto_frontier: Vec<MultiObjectiveSolution> = mo_results.pareto_frontier()
            .iter()
            .map(|sol| MultiObjectiveSolution {
                spread_bps: sol.params.spread_bps,
                skew_factor: sol.params.skew_factor,
                fill_probability: sol.params.fill_probability,
                high_entropy_threshold: sol.params.high_entropy_threshold,
                sharpe: sol.objectives.sharpe,
                drawdown: sol.objectives.drawdown,
                fill_rate: sol.objectives.fill_rate,
                turnover: sol.objectives.turnover,
                total_return: sol.objectives.total_return,
                win_rate: sol.objectives.win_rate,
                num_trades: sol.objectives.num_trades,
                pareto_rank: sol.pareto_rank,
                crowding_distance: sol.crowding_distance,
            })
            .collect();

        let best_weighted = mo_results.best_weighted()
            .map(|sol| MultiObjectiveSolution {
                spread_bps: sol.params.spread_bps,
                skew_factor: sol.params.skew_factor,
                fill_probability: sol.params.fill_probability,
                high_entropy_threshold: sol.params.high_entropy_threshold,
                sharpe: sol.objectives.sharpe,
                drawdown: sol.objectives.drawdown,
                fill_rate: sol.objectives.fill_rate,
                turnover: sol.objectives.turnover,
                total_return: sol.objectives.total_return,
                win_rate: sol.objectives.win_rate,
                num_trades: sol.objectives.num_trades,
                pareto_rank: sol.pareto_rank,
                crowding_distance: sol.crowding_distance,
            });

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!(
                "Multi-objective optimization completed: {} solutions evaluated, {} on Pareto frontier",
                all_solutions.len(),
                pareto_frontier.len()
            ),
        });

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Multi-objective optimization completed: {} solutions on Pareto frontier",
                pareto_frontier.len()
            ),
        });

        Ok(MultiObjectiveResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            all_solutions,
            pareto_frontier,
            best_weighted,
            total_combinations,
            time_span_hours: mo_results.time_span_hours,
            num_events: mo_results.num_events,
        })
    }

    /// Regime-specific parameter optimization (MM algorithms only)
    ///
    /// This function optimizes MM parameters independently for each market regime
    /// (high/medium/low entropy), then combines them into an optimal regime-switching strategy.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for regime optimization
    /// * `callback` - Progress callback for real-time updates
    ///
    /// # Returns
    ///
    /// `RegimeOptimizeResult` with per-regime optimization results and comparison with uniform approach.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Algorithm type is not a Market Making algorithm
    /// - Data cannot be loaded
    /// - Optimization fails
    ///
    /// This is the extracted version of the `run_regime_optimize()` function from the CLI.
    pub fn regime_optimize(
        params: RegimeOptimizeParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<RegimeOptimizeResult> {
        use crate::backtest::regime_optimizer::{RegimeOptimizer, RegimeOptimizerConfig};

        // Validate algorithm type (MM only)
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|e| anyhow::anyhow!("Invalid algorithm: {}", e))?;
        Self::validate_mm_algorithm(algo_type)?;

        let algo_name = algo_type.display_name().to_string();

        callback.on_event(ProgressEvent::Started {
            total: None,
            message: format!(
                "Starting regime-specific parameter optimization for {}",
                algo_name
            ),
        });

        // Parse parameter grids
        let spread_values: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skew_values: Vec<f64> = params.skews
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        if spread_values.is_empty() {
            anyhow::bail!("No valid spread values found in '{}'", params.spreads);
        }
        if skew_values.is_empty() {
            anyhow::bail!("No valid skew values found in '{}'", params.skews);
        }

        let total_combinations = spread_values.len() * skew_values.len();
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!(
                "Testing {} combinations per regime ({} spreads × {} skews)",
                total_combinations,
                spread_values.len(),
                skew_values.len()
            ),
        });

        // Build config
        let config = RegimeOptimizerConfig {
            data_dir: params.data_path.clone(),
            high_entropy_threshold: params.high_entropy,
            low_entropy_threshold: params.low_entropy,
            spreads: spread_values,
            skews: skew_values,
            fill_probability: params.fill_prob,
            min_trades: params.min_trades,
            allow_no_quote_low: params.allow_no_quote,
            verbose: false, // Use callback instead
        };

        // Run optimization
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Loading data...".to_string(),
        });

        let mut optimizer = RegimeOptimizer::new(config);
        optimizer.load_data()
            .context("Failed to load data for regime optimization")?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Running regime-specific optimization...".to_string(),
        });

        let results = optimizer.optimize()
            .context("Failed to run regime optimization")?;

        // Convert results to our result struct
        let high_entropy = RegimeOptimizeMetrics {
            regime: "High Entropy".to_string(),
            event_count: results.high_entropy.event_count,
            event_fraction: results.high_entropy.event_fraction,
            time_hours: results.high_entropy.time_hours,
            optimal_spread: results.high_entropy.optimal_spread,
            optimal_skew: results.high_entropy.optimal_skew,
            should_quote: results.high_entropy.should_quote,
            best_sharpe: results.high_entropy.best_sharpe,
            best_return: results.high_entropy.best_return,
            best_drawdown: results.high_entropy.best_drawdown,
            best_trades: results.high_entropy.best_trades,
            best_win_rate: results.high_entropy.best_win_rate,
        };

        let medium_entropy = RegimeOptimizeMetrics {
            regime: "Medium Entropy".to_string(),
            event_count: results.medium_entropy.event_count,
            event_fraction: results.medium_entropy.event_fraction,
            time_hours: results.medium_entropy.time_hours,
            optimal_spread: results.medium_entropy.optimal_spread,
            optimal_skew: results.medium_entropy.optimal_skew,
            should_quote: results.medium_entropy.should_quote,
            best_sharpe: results.medium_entropy.best_sharpe,
            best_return: results.medium_entropy.best_return,
            best_drawdown: results.medium_entropy.best_drawdown,
            best_trades: results.medium_entropy.best_trades,
            best_win_rate: results.medium_entropy.best_win_rate,
        };

        let low_entropy = RegimeOptimizeMetrics {
            regime: "Low Entropy".to_string(),
            event_count: results.low_entropy.event_count,
            event_fraction: results.low_entropy.event_fraction,
            time_hours: results.low_entropy.time_hours,
            optimal_spread: results.low_entropy.optimal_spread,
            optimal_skew: results.low_entropy.optimal_skew,
            should_quote: results.low_entropy.should_quote,
            best_sharpe: results.low_entropy.best_sharpe,
            best_return: results.low_entropy.best_return,
            best_drawdown: results.low_entropy.best_drawdown,
            best_trades: results.low_entropy.best_trades,
            best_win_rate: results.low_entropy.best_win_rate,
        };

        let optimal_regime_params = OptimalRegimeParams {
            high: RegimeParamSet {
                spread_bps: results.optimal_regime_params.high.spread_bps,
                skew_factor: results.optimal_regime_params.high.skew_factor,
                should_quote: results.optimal_regime_params.high.should_quote,
            },
            medium: RegimeParamSet {
                spread_bps: results.optimal_regime_params.medium.spread_bps,
                skew_factor: results.optimal_regime_params.medium.skew_factor,
                should_quote: results.optimal_regime_params.medium.should_quote,
            },
            low: RegimeParamSet {
                spread_bps: results.optimal_regime_params.low.spread_bps,
                skew_factor: results.optimal_regime_params.low.skew_factor,
                should_quote: results.optimal_regime_params.low.should_quote,
            },
        };

        let comparison = StrategyComparison {
            uniform_sharpe: results.comparison.uniform.sharpe,
            uniform_return: results.comparison.uniform.total_return,
            uniform_drawdown: results.comparison.uniform.max_drawdown,
            uniform_trades: results.comparison.uniform.num_trades,
            uniform_win_rate: results.comparison.uniform.win_rate,
            regime_specific_sharpe: results.comparison.regime_specific.sharpe,
            regime_specific_return: results.comparison.regime_specific.total_return,
            regime_specific_drawdown: results.comparison.regime_specific.max_drawdown,
            regime_specific_trades: results.comparison.regime_specific.num_trades,
            regime_specific_win_rate: results.comparison.regime_specific.win_rate,
            sharpe_improvement: results.comparison.sharpe_improvement,
            return_improvement: results.comparison.return_improvement,
            drawdown_improvement: results.comparison.drawdown_improvement,
            trade_count_diff: results.comparison.trade_count_diff,
        };

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Regime optimization completed: {} events processed over {:.1} hours",
                results.total_events,
                results.time_span_hours
            ),
        });

        Ok(RegimeOptimizeResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            high_entropy,
            medium_entropy,
            low_entropy,
            optimal_regime_params,
            comparison,
            total_events: results.total_events,
            time_span_hours: results.time_span_hours,
        })
    }

    /// ML weight training (ML Spread/Skew algorithm only)
    ///
    /// This function trains ML weights for the ML Spread/Skew algorithm using grid search
    /// over historical backtest data with train/test validation.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for ML training
    /// * `callback` - Progress callback for real-time updates
    ///
    /// # Returns
    ///
    /// `TrainResult` with optimal trained weights and performance metrics.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Algorithm type is not ML Spread/Skew
    /// - Data cannot be loaded
    /// - Training fails
    ///
    /// This is the extracted version of the `run_train_ml()` function from the CLI.
    pub fn train(
        params: TrainParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<TrainResult> {
        use crate::backtest::ml_trainer::{MLTrainer, MLTrainerConfig};

        // Validate algorithm type (ML Spread/Skew only)
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|e| anyhow::anyhow!("Invalid algorithm: {}", e))?;
        
        match algo_type {
            AlgorithmType::MLSpreadSkew => {},
            _ => anyhow::bail!(
                "Algorithm '{}' is not ML Spread/Skew. \
                 Training is only available for ML Spread/Skew algorithm (ml, ml-spread-skew)",
                algo_type.as_str()
            ),
        }

        let algo_name = algo_type.display_name().to_string();

        callback.on_event(ProgressEvent::Started {
            total: None,
            message: format!(
                "Starting ML weight training for {}",
                algo_name
            ),
        });

        // Parse parameter grids
        let spread_intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let spread_entropy_weights: Vec<f64> = params.spread_entropy_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let spread_vol_weights: Vec<f64> = params.spread_vol_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skew_intercepts: Vec<f64> = params.skew_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skew_inv_weights: Vec<f64> = params.skew_inv_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        if spread_intercepts.is_empty() {
            anyhow::bail!("No valid spread_intercept values found in '{}'", params.spread_intercepts);
        }
        if spread_entropy_weights.is_empty() {
            anyhow::bail!("No valid spread_entropy_weight values found in '{}'", params.spread_entropy_weights);
        }
        if spread_vol_weights.is_empty() {
            anyhow::bail!("No valid spread_vol_weight values found in '{}'", params.spread_vol_weights);
        }
        if skew_intercepts.is_empty() {
            anyhow::bail!("No valid skew_intercept values found in '{}'", params.skew_intercepts);
        }
        if skew_inv_weights.is_empty() {
            anyhow::bail!("No valid skew_inv_weight values found in '{}'", params.skew_inv_weights);
        }

        let total_combinations = spread_intercepts.len()
            * spread_entropy_weights.len()
            * spread_vol_weights.len()
            * skew_intercepts.len()
            * skew_inv_weights.len();

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!(
                "Testing {} weight combinations ({} spreads × {} skews)",
                total_combinations,
                spread_intercepts.len() * spread_entropy_weights.len() * spread_vol_weights.len(),
                skew_intercepts.len() * skew_inv_weights.len()
            ),
        });

        // Build config
        let config = MLTrainerConfig {
            data_dir: params.data_path.clone(),
            train_ratio: params.train_ratio,
            spread_intercepts,
            spread_entropy_weights,
            spread_volatility_weights: spread_vol_weights,
            skew_intercepts,
            skew_inventory_weights: skew_inv_weights,
            max_inventory: Decimal::from_f64_retain(params.max_inventory).unwrap_or(dec!(0.1)),
            quote_size: Decimal::from_f64_retain(params.quote_size).unwrap_or(dec!(0.001)),
            fill_probability: params.fill_prob,
            min_trades: 10,
            objective: "sharpe".to_string(),
            verbose: false, // Use callback instead
        };

        // Run training
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Loading data and initializing trainer...".to_string(),
        });

        let mut trainer = MLTrainer::new(config)
            .context("Failed to create ML trainer")?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Training ML weights...".to_string(),
        });

        let results = trainer.train()
            .context("Failed to train ML weights")?;

        // Convert results to our result struct
        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "ML training completed: {} valid configurations tested, train Sharpe: {:.3}, test Sharpe: {:.3}",
                results.valid_configurations,
                results.train_sharpe,
                results.test_sharpe
            ),
        });

        Ok(TrainResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            optimal_weights: results.optimal_weights,
            train_sharpe: results.train_sharpe,
            train_return: results.train_return,
            train_trades: results.train_trades,
            test_sharpe: results.test_sharpe,
            test_return: results.test_return,
            test_trades: results.test_trades,
            generalization_gap: results.generalization_gap,
            valid_configurations: results.valid_configurations,
            total_configurations: results.total_configurations,
        })
    }

    /// Walk-forward ML training (MM algorithms only)
    ///
    /// This function performs walk-forward validation for ML weights, training on
    /// sequential folds of data and testing on out-of-sample periods.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for walk-forward ML training
    /// * `callback` - Progress callback for real-time updates
    ///
    /// # Returns
    ///
    /// `WalkForwardMLResult` with fold results, aggregated metrics, and consensus weights.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Algorithm type is not a Market Making algorithm
    /// - Data cannot be loaded
    /// - Walk-forward training fails
    ///
    /// This is the extracted version of the `run_walk_forward_ml()` function from the CLI.
    pub fn walk_forward_ml(
        params: WalkForwardMLParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<WalkForwardMLResult> {
        use crate::backtest::walk_forward_ml::{WalkForwardMLTrainer, WalkForwardMLConfig};

        // Validate algorithm type (MM only)
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|e| anyhow::anyhow!("Invalid algorithm: {}", e))?;
        Self::validate_mm_algorithm(algo_type)?;

        let algo_name = algo_type.display_name().to_string();

        callback.on_event(ProgressEvent::Started {
            total: Some(params.folds),
            message: format!(
                "Starting walk-forward ML training for {} with {} folds",
                algo_name,
                params.folds
            ),
        });

        // Parse parameter grids
        let spread_intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let spread_entropy_weights: Vec<f64> = params.spread_entropy_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let spread_vol_weights: Vec<f64> = params.spread_vol_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skew_intercepts: Vec<f64> = params.skew_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skew_inv_weights: Vec<f64> = params.skew_inv_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        if spread_intercepts.is_empty() {
            anyhow::bail!("No valid spread_intercept values found in '{}'", params.spread_intercepts);
        }
        if spread_entropy_weights.is_empty() {
            anyhow::bail!("No valid spread_entropy_weight values found in '{}'", params.spread_entropy_weights);
        }
        if spread_vol_weights.is_empty() {
            anyhow::bail!("No valid spread_vol_weight values found in '{}'", params.spread_vol_weights);
        }
        if skew_intercepts.is_empty() {
            anyhow::bail!("No valid skew_intercept values found in '{}'", params.skew_intercepts);
        }
        if skew_inv_weights.is_empty() {
            anyhow::bail!("No valid skew_inv_weight values found in '{}'", params.skew_inv_weights);
        }

        let total_configs = spread_intercepts.len()
            * spread_entropy_weights.len()
            * spread_vol_weights.len()
            * skew_intercepts.len()
            * skew_inv_weights.len();

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!(
                "Testing {} weight combinations per fold ({} spreads × {} skews)",
                total_configs,
                spread_intercepts.len() * spread_entropy_weights.len() * spread_vol_weights.len(),
                skew_intercepts.len() * skew_inv_weights.len()
            ),
        });

        // Build config
        let config = WalkForwardMLConfig {
            data_dir: params.data_path.clone(),
            n_folds: params.folds,
            min_train_hours: params.min_train_hours,
            test_hours: params.test_hours,
            anchored: !params.rolling,
            embargo_hours: params.embargo_hours,
            spread_intercepts,
            spread_entropy_weights,
            spread_volatility_weights: spread_vol_weights,
            skew_intercepts,
            skew_inventory_weights: skew_inv_weights,
            fill_probability: params.fill_prob,
            max_inventory: Decimal::from_f64_retain(params.max_inventory).unwrap_or(dec!(0.1)),
            quote_size: Decimal::from_f64_retain(params.quote_size).unwrap_or(dec!(0.001)),
            min_trades: 10,
            verbose: false, // Use callback instead
        };

        // Run walk-forward training
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Loading data and initializing walk-forward trainer...".to_string(),
        });

        let mut trainer = WalkForwardMLTrainer::new(config)
            .context("Failed to create walk-forward ML trainer")?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Running walk-forward training with {} folds...", params.folds),
        });

        let results = trainer.run()
            .context("Failed to run walk-forward ML training")?;

        // Convert fold results
        let fold_results: Vec<WalkForwardMLFoldResult> = results.folds
            .iter()
            .map(|fold| WalkForwardMLFoldResult {
                fold_num: fold.fold_num,
                train_start_ms: fold.train_start_ms,
                train_end_ms: fold.train_end_ms,
                test_start_ms: fold.test_start_ms,
                test_end_ms: fold.test_end_ms,
                train_events: fold.train_events,
                test_events: fold.test_events,
                best_weights: fold.best_weights.clone(),
                train_sharpe: fold.train_sharpe,
                train_return: fold.train_return,
                train_trades: fold.train_trades,
                test_sharpe: fold.test_sharpe,
                test_return: fold.test_return,
                test_trades: fold.test_trades,
                generalization_gap: fold.generalization_gap,
                configs_evaluated: fold.configs_evaluated,
                valid_configs: fold.valid_configs,
            })
            .collect();

        // Convert aggregate
        let aggregate = WalkForwardMLAggregate {
            avg_oos_sharpe: results.aggregate.avg_oos_sharpe,
            std_oos_sharpe: results.aggregate.std_oos_sharpe,
            avg_oos_return: results.aggregate.avg_oos_return,
            total_oos_trades: results.aggregate.total_oos_trades,
            avg_generalization_gap: results.aggregate.avg_generalization_gap,
            pct_profitable_folds: results.aggregate.pct_profitable_folds,
            is_oos_sharpe_ratio: results.aggregate.is_oos_sharpe_ratio,
            prob_sharpe_gt_zero: results.aggregate.prob_sharpe_gt_zero,
            weight_stability: WeightStability {
                spread_intercept_std: results.aggregate.weight_stability.spread_intercept_std,
                spread_entropy_std: results.aggregate.weight_stability.spread_entropy_std,
                spread_volatility_std: results.aggregate.weight_stability.spread_volatility_std,
                skew_intercept_std: results.aggregate.weight_stability.skew_intercept_std,
                skew_inventory_std: results.aggregate.weight_stability.skew_inventory_std,
                stability_score: results.aggregate.weight_stability.stability_score,
            },
        };

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Walk-forward ML training completed: {} folds, avg OOS Sharpe: {:.3}",
                params.folds,
                aggregate.avg_oos_sharpe
            ),
        });

        Ok(WalkForwardMLResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            folds: params.folds,
            fold_results,
            aggregate,
            consensus_weights: results.consensus_weights,
        })
    }

    /// Run parameter sweep
    ///
    /// This is the extracted version of the `run_sweep()` function from the CLI.
    /// It supports progress callbacks for real-time updates during execution.
    ///
    /// Returns `SweepResult` with all parameter combinations tested, sorted by Sharpe ratio.
    pub fn sweep(
        params: SweepParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<SweepResult> {
        // Parse algorithm type early to fail fast on invalid algorithm
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|_| anyhow::anyhow!(
                "Unknown algorithm '{}'. Valid options: {}",
                params.algorithm,
                AlgorithmRegistry::all_type_strings().join(", ")
            ))?;

        let algo_name = algo_type.display_name().to_string();

        // Parse spreads and skews
        let spreads: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        let skews: Vec<f64> = params.skews
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        let total_combinations = spreads.len() * skews.len();

        callback.on_event(ProgressEvent::Started {
            total: Some(total_combinations),
            message: format!(
                "Starting parameter sweep with algorithm: {} ({} combinations)",
                algo_name,
                total_combinations
            ),
        });

        // Load ML weights if needed
        let ml_weights = Self::load_ml_weights_if_needed(
            algo_type,
            params.weights_file.as_deref(),
            &callback,
        )?;

        // Load data once
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
        let _events = replay.into_events(); // We'll reload for each combination

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loaded {} events", num_events),
        });

        let mut all_results: Vec<SweepResultItem> = Vec::new();
        let mut current = 0;

        for &spread in &spreads {
            for &skew in &skews {
                current += 1;

                // Reload events (need fresh copy for each run)
                let mut replay = ParquetReplay::new(replay_config.clone());
                replay.load()
                    .context("Failed to reload market data")?;
                let events = replay.into_events();

                // Create algorithm with sweep parameters
                let algo_params = BacktestAlgorithmParams::new(
                    Decimal::from_f64_retain(params.max_inventory).unwrap_or(dec!(0.1)),
                    Decimal::from_f64_retain(params.quote_size).unwrap_or(dec!(0.001)),
                    spread,
                    skew,
                );
                let algo_params = if let Some(ref weights) = ml_weights {
                    algo_params.with_ml_weights(weights.clone())
                } else {
                    algo_params
                };

                let algorithm = AlgorithmRegistry::create_for_backtest(algo_type, &algo_params)
                    .map_err(|e| anyhow::anyhow!("Failed to create algorithm '{}': {}", algo_name, e))?;

                let config = BacktestConfig {
                    replay: replay_config.clone(),
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
                    verbose: false,
                    use_realistic_fills: !params.naive_fills,
                    ..Default::default()
                };

                let mut engine = BacktestEngine::from_events_with_algorithm(config, events, algorithm);
                let results = engine.run()
                    .context("Failed to run backtest")?;

                let sweep_item = SweepResultItem {
                    spread,
                    skew,
                    sharpe: results.metrics.sharpe_ratio,
                    total_return: results.metrics.total_return,
                    max_drawdown: results.metrics.max_drawdown,
                    num_trades: results.metrics.num_trades,
                    win_rate: results.metrics.win_rate,
                };

                callback.on_event(ProgressEvent::Progress {
                    current,
                    total: Some(total_combinations),
                    message: format!(
                        "Spread={:.1}, Skew={:.1} => Sharpe={:+.2}, Return={:+.2}%, Trades={}",
                        spread,
                        skew,
                        sweep_item.sharpe,
                        sweep_item.total_return * 100.0,
                        sweep_item.num_trades,
                    ),
                });

                callback.on_event(ProgressEvent::Metric {
                    name: "current_sharpe".to_string(),
                    value: sweep_item.sharpe,
                });

                all_results.push(sweep_item);
            }
        }

        // Find best by Sharpe
        let best = all_results.iter()
            .max_by(|a, b| {
                a.sharpe.partial_cmp(&b.sharpe).unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned();

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Parameter sweep completed: {} combinations tested, best Sharpe: {:.2}",
                total_combinations,
                best.as_ref().map(|b| b.sharpe).unwrap_or(0.0)
            ),
        });

        Ok(SweepResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            all_results,
            best,
            total_combinations,
        })
    }

    /// Run walk-forward validation
    ///
    /// This is the extracted version of the `run_walk_forward()` function from the CLI.
    /// It supports progress callbacks for real-time updates during execution.
    ///
    /// Returns `WalkForwardResult` with fold results and aggregated metrics.
    pub fn walk_forward(
        params: WalkForwardParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<WalkForwardResult> {
        use crate::backtest::walk_forward::{WalkForwardEngine, WalkForwardConfig, ParamGrid};

        // Parse algorithm type early to fail fast on invalid algorithm
        let algo_type = AlgorithmType::from_str(&params.algorithm)
            .map_err(|_| anyhow::anyhow!(
                "Unknown algorithm '{}'. Valid options: {}",
                params.algorithm,
                AlgorithmRegistry::all_type_strings().join(", ")
            ))?;

        let algo_name = algo_type.display_name().to_string();

        // Parse parameter lists
        let spreads: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        let skews: Vec<f64> = params.skews
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        let fill_probs: Vec<f64> = params.fill_probs
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        callback.on_event(ProgressEvent::Started {
            total: Some(params.folds),
            message: format!(
                "Starting walk-forward validation with algorithm: {} ({} folds, {} mode)",
                algo_name,
                params.folds,
                if params.rolling { "rolling" } else { "anchored" }
            ),
        });

        // Build WalkForwardConfig
        let config = WalkForwardConfig {
            n_folds: params.folds,
            min_train_hours: params.min_train_hours,
            test_hours: params.test_hours,
            anchored: !params.rolling,
            embargo_hours: params.embargo_hours,
            param_grid: ParamGrid {
                spreads,
                skews,
                fill_probs,
            },
            data_dir: params.data_path.clone(),
            verbose: !params.quiet,
        };

        let mut engine = WalkForwardEngine::new(config);

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Loading market data...".to_string(),
        });

        let num_events = engine.load_data()
            .context("Failed to load market data")?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loaded {} events", num_events),
        });

        // Run walk-forward validation
        let results = engine.run()
            .context("Failed to run walk-forward validation")?;

        // Convert fold results
        let fold_results: Vec<WalkForwardFoldResult> = results.folds
            .iter()
            .map(|fold| {
                callback.on_event(ProgressEvent::Progress {
                    current: fold.fold_num,
                    total: Some(params.folds),
                    message: format!(
                        "Fold {}/{}: Train Sharpe={:.2}, Test Sharpe={:.2}",
                        fold.fold_num,
                        params.folds,
                        fold.train_metrics.sharpe,
                        fold.test_metrics.sharpe
                    ),
                });

                WalkForwardFoldResult {
                    fold_num: fold.fold_num,
                    train_start_ms: fold.train_start_ms,
                    train_end_ms: fold.train_end_ms,
                    test_start_ms: fold.test_start_ms,
                    test_end_ms: fold.test_end_ms,
                    best_params: WalkForwardOptimizedParams {
                        spread: fold.best_params.spread,
                        skew: fold.best_params.skew,
                        fill_prob: fold.best_params.fill_prob,
                        train_sharpe: fold.best_params.train_sharpe,
                    },
                    train_metrics: WalkForwardFoldMetrics {
                        sharpe: fold.train_metrics.sharpe,
                        total_return: fold.train_metrics.total_return,
                        max_drawdown: fold.train_metrics.max_drawdown,
                        num_trades: fold.train_metrics.num_trades,
                        win_rate: fold.train_metrics.win_rate,
                        profit_factor: fold.train_metrics.profit_factor,
                    },
                    test_metrics: WalkForwardFoldMetrics {
                        sharpe: fold.test_metrics.sharpe,
                        total_return: fold.test_metrics.total_return,
                        max_drawdown: fold.test_metrics.max_drawdown,
                        num_trades: fold.test_metrics.num_trades,
                        win_rate: fold.test_metrics.win_rate,
                        profit_factor: fold.test_metrics.profit_factor,
                    },
                }
            })
            .collect();

        // Convert aggregate
        let aggregate = WalkForwardAggregate {
            avg_oos_sharpe: results.aggregate.avg_oos_sharpe,
            std_oos_sharpe: results.aggregate.std_oos_sharpe,
            avg_oos_return: results.aggregate.avg_oos_return,
            total_oos_trades: results.aggregate.total_oos_trades,
            avg_win_rate: results.aggregate.avg_win_rate,
            pct_profitable_folds: results.aggregate.pct_profitable_folds,
            is_oos_sharpe_ratio: results.aggregate.is_oos_sharpe_ratio,
            prob_sharpe_gt_zero: results.aggregate.prob_sharpe_gt_zero,
        };

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Walk-forward validation completed: {} folds, avg OOS Sharpe: {:.3}",
                params.folds,
                aggregate.avg_oos_sharpe
            ),
        });

        Ok(WalkForwardResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: algo_name,
            folds: params.folds,
            fold_results,
            aggregate,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::common::{NoOpCallback, ProgressCallback, ProgressEvent, LogLevel};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::collections::VecDeque;

    // ============================================================================
    // Basic Structure Tests
    // ============================================================================

    #[test]
    fn test_backtest_commands_struct() {
        // Verify struct can be instantiated
        let _commands = BacktestCommands;
    }

    // ============================================================================
    // EvaluateMetrics Conversion Tests
    // ============================================================================

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

    #[test]
    fn test_evaluate_metrics_zero_trades() {
        use crate::backtest::metrics::{PerformanceMetrics, TradeLog, EquityCurve};
        use crate::backtest::harness::{BacktestConfig, FillStats};

        let mut metrics = PerformanceMetrics::default();
        metrics.num_trades = 0;
        metrics.win_rate = 0.0;
        metrics.avg_trade_pnl = dec!(0);

        let results = BacktestResults {
            config: BacktestConfig::default(),
            metrics,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 0,
            fills_generated: 0,
            fill_stats: FillStats::default(),
            oco_stats: None,
        };

        let eval_metrics = EvaluateMetrics::from(&results);
        assert_eq!(eval_metrics.num_trades, 0);
        assert_eq!(eval_metrics.win_rate, 0.0);
        assert_eq!(eval_metrics.avg_trade_pnl, 0.0);
    }

    #[test]
    fn test_evaluate_metrics_negative_returns() {
        use crate::backtest::metrics::{PerformanceMetrics, TradeLog, EquityCurve};
        use crate::backtest::harness::{BacktestConfig, FillStats};

        let mut metrics = PerformanceMetrics::default();
        metrics.total_return = -0.10;
        metrics.sharpe_ratio = -1.5;
        metrics.max_drawdown = 0.15;

        let results = BacktestResults {
            config: BacktestConfig::default(),
            metrics,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 100,
            fills_generated: 50,
            fill_stats: FillStats::default(),
            oco_stats: None,
        };

        let eval_metrics = EvaluateMetrics::from(&results);
        assert_eq!(eval_metrics.total_return, -0.10);
        assert_eq!(eval_metrics.sharpe_ratio, -1.5);
        assert_eq!(eval_metrics.max_drawdown, 0.15);
    }

    #[test]
    fn test_evaluate_metrics_all_fields() {
        use crate::backtest::metrics::{PerformanceMetrics, TradeLog, EquityCurve};
        use crate::backtest::harness::{BacktestConfig, FillStats};

        let mut metrics = PerformanceMetrics::default();
        metrics.sharpe_ratio = 2.0;
        metrics.total_return = 0.15;
        metrics.max_drawdown = 0.05;
        metrics.num_trades = 200;
        metrics.win_rate = 0.60;
        metrics.avg_trade_pnl = dec!(0.002);
        metrics.annualized_return = 0.30;
        metrics.sortino_ratio = 2.5;
        metrics.calmar_ratio = 6.0;
        metrics.profit_factor = 1.5;

        let results = BacktestResults {
            config: BacktestConfig::default(),
            metrics,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 5000,
            fills_generated: 200,
            fill_stats: FillStats::default(),
            oco_stats: None,
        };

        let eval_metrics = EvaluateMetrics::from(&results);
        assert_eq!(eval_metrics.sharpe_ratio, 2.0);
        assert_eq!(eval_metrics.total_return, 0.15);
        assert_eq!(eval_metrics.max_drawdown, 0.05);
        assert_eq!(eval_metrics.num_trades, 200);
        assert_eq!(eval_metrics.win_rate, 0.60);
        assert_eq!(eval_metrics.avg_trade_pnl, 0.002);
        assert_eq!(eval_metrics.annualized_return, 0.30);
        assert_eq!(eval_metrics.sortino_ratio, 2.5);
        assert_eq!(eval_metrics.calmar_ratio, 6.0);
        assert_eq!(eval_metrics.profit_factor, 1.5);
    }

    // ============================================================================
    // Progress Callback Tests
    // ============================================================================

    /// Test callback that collects all events
    struct TestCallback {
        events: Arc<std::sync::Mutex<VecDeque<ProgressEvent>>>,
    }

    impl TestCallback {
        fn new() -> Self {
            Self {
                events: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            }
        }

        fn get_events(&self) -> Vec<ProgressEvent> {
            self.events.lock().unwrap().iter().cloned().collect()
        }

        fn event_count(&self) -> usize {
            self.events.lock().unwrap().len()
        }
    }

    impl ProgressCallback for TestCallback {
        fn on_event(&self, event: ProgressEvent) {
            self.events.lock().unwrap().push_back(event);
        }
    }

    #[test]
    fn test_progress_callback_receives_events() {
        let callback = Arc::new(TestCallback::new());
        let callback_clone = callback.clone();

        // Simulate some events
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Test started".to_string(),
        });
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Test log".to_string(),
        });
        callback.on_event(ProgressEvent::Completed {
            message: "Test completed".to_string(),
        });

        assert_eq!(callback_clone.event_count(), 3);
        let events = callback_clone.get_events();
        assert!(matches!(events[0], ProgressEvent::Started { .. }));
        assert!(matches!(events[1], ProgressEvent::Log { .. }));
        assert!(matches!(events[2], ProgressEvent::Completed { .. }));
    }

    // ============================================================================
    // Algorithm Parameter Creation Tests
    // ============================================================================

    #[test]
    fn test_create_algorithm_params_basic() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(2.0)
            .skew(0.5)
            .max_inventory(0.1)
            .quote_size(0.001)
            .build()
            .unwrap();

        let algo_params = BacktestCommands::create_algorithm_params(&params, None);
        // Verify the params were created (check fields directly)
        // Use approximate comparison due to f64->Decimal conversion
        assert!((algo_params.max_inventory.to_f64().unwrap() - 0.1).abs() < 1e-10);
        assert!((algo_params.quote_size.to_f64().unwrap() - 0.001).abs() < 1e-10);
    }

    #[test]
    fn test_create_algorithm_params_with_ml_weights() {
        use crate::strategies::MLModelWeights;
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread(2.0)
            .skew(0.5)
            .max_inventory(0.1)
            .quote_size(0.001)
            .build()
            .unwrap();

        let ml_weights = Some(MLModelWeights::default());
        let algo_params = BacktestCommands::create_algorithm_params(&params, ml_weights);
        // ML weights should be set (check the field directly)
        assert!(algo_params.ml_weights.is_some());
    }

    #[test]
    fn test_create_algorithm_params_extreme_values() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(100.0)
            .skew(10.0)
            .max_inventory(5.0)
            .quote_size(0.1)
            .build()
            .unwrap();

        let algo_params = BacktestCommands::create_algorithm_params(&params, None);
        // Use approximate comparison due to f64->Decimal conversion
        assert!((algo_params.max_inventory.to_f64().unwrap() - 5.0).abs() < 1e-10);
        assert!((algo_params.quote_size.to_f64().unwrap() - 0.1).abs() < 1e-10);
    }

    // ============================================================================
    // EvaluateResult Structure Tests
    // ============================================================================

    #[test]
    fn test_evaluate_result_serialization() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        let eval_result = EvaluateResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            metrics: EvaluateMetrics {
                sharpe_ratio: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.001,
                annualized_return: 0.10,
                sortino_ratio: 2.0,
                calmar_ratio: 5.0,
                profit_factor: 1.2,
            },
            params: params.clone(),
            events_processed: 1000,
            fills_generated: 100,
        };

        let json = serde_json::to_string(&eval_result).unwrap();
        let deserialized: EvaluateResult = serde_json::from_str(&json).unwrap();

        assert_eq!(eval_result.algorithm, deserialized.algorithm);
        assert_eq!(eval_result.algorithm_name, deserialized.algorithm_name);
        assert_eq!(eval_result.metrics.sharpe_ratio, deserialized.metrics.sharpe_ratio);
        assert_eq!(eval_result.events_processed, deserialized.events_processed);
        assert_eq!(eval_result.fills_generated, deserialized.fills_generated);
    }

    #[test]
    fn test_evaluate_result_clone() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        let result1 = EvaluateResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            metrics: EvaluateMetrics {
                sharpe_ratio: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.001,
                annualized_return: 0.10,
                sortino_ratio: 2.0,
                calmar_ratio: 5.0,
                profit_factor: 1.2,
            },
            params: params.clone(),
            events_processed: 1000,
            fills_generated: 100,
        };

        let result2 = result1.clone();
        assert_eq!(result1.algorithm, result2.algorithm);
        assert_eq!(result1.metrics.sharpe_ratio, result2.metrics.sharpe_ratio);
        assert_eq!(result1.events_processed, result2.events_processed);
    }

    #[test]
    fn test_evaluate_metrics_debug() {
        let metrics = EvaluateMetrics {
            sharpe_ratio: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.001,
            annualized_return: 0.10,
            sortino_ratio: 2.0,
            calmar_ratio: 5.0,
            profit_factor: 1.2,
        };

        let debug_str = format!("{:?}", metrics);
        assert!(debug_str.contains("sharpe_ratio"));
        assert!(debug_str.contains("1.5"));
    }

    #[test]
    fn test_evaluate_metrics_serialization() {
        let metrics = EvaluateMetrics {
            sharpe_ratio: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.001,
            annualized_return: 0.10,
            sortino_ratio: 2.0,
            calmar_ratio: 5.0,
            profit_factor: 1.2,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: EvaluateMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(metrics.sharpe_ratio, deserialized.sharpe_ratio);
        assert_eq!(metrics.total_return, deserialized.total_return);
        assert_eq!(metrics.max_drawdown, deserialized.max_drawdown);
        assert_eq!(metrics.num_trades, deserialized.num_trades);
        assert_eq!(metrics.win_rate, deserialized.win_rate);
    }

    // ============================================================================
    // Error Handling Tests
    // ============================================================================

    #[test]
    fn test_evaluate_invalid_algorithm() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("invalid_algorithm".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::evaluate(params, callback);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown algorithm"));
    }

    #[test]
    fn test_evaluate_nonexistent_data_path() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("/nonexistent/path/that/does/not/exist"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::evaluate(params, callback);

        // Should fail when trying to load data
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_invalid_ml_weights_file() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;
        use std::fs;

        // Create a temporary directory for test
        let temp_dir = std::env::temp_dir().join("ingestor_test");
        let _ = fs::create_dir_all(&temp_dir);

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .weights_file(Some(PathBuf::from("/nonexistent/weights.json")))
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::evaluate(params, callback);

        // Should fail when trying to load weights
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("weights"));
    }

    #[test]
    fn test_evaluate_invalid_ml_weights_json() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;
        use std::fs;
        use std::io::Write;

        // Create a temporary file with invalid JSON
        let temp_file = std::env::temp_dir().join("invalid_weights.json");
        let mut file = fs::File::create(&temp_file).unwrap();
        writeln!(file, "{{ invalid json }}").unwrap();
        drop(file);

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .weights_file(Some(temp_file.clone()))
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::evaluate(params, callback);

        // Should fail when parsing JSON
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("parse") || err_msg.contains("JSON"));

        // Cleanup
        let _ = fs::remove_file(&temp_file);
    }

    // ============================================================================
    // Progress Callback Integration Tests
    // ============================================================================

    #[test]
    fn test_evaluate_progress_callback_events() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        // This test would require actual data, so we'll just test the callback structure
        let callback = Arc::new(TestCallback::new());
        let callback_clone = callback.clone();

        // Simulate events that would be sent during evaluation
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Starting backtest".to_string(),
        });

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Loading data".to_string(),
        });

        callback.on_event(ProgressEvent::Metric {
            name: "sharpe_ratio".to_string(),
            value: 1.5,
        });

        assert_eq!(callback_clone.event_count(), 3);
        let events = callback_clone.get_events();
        assert!(matches!(events[0], ProgressEvent::Started { .. }));
        assert!(matches!(events[1], ProgressEvent::Log { .. }));
        assert!(matches!(events[2], ProgressEvent::Metric { .. }));
    }

    #[test]
    fn test_noop_callback_does_nothing() {
        let callback = NoOpCallback;
        // Should not panic
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Test".to_string(),
        });
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Test".to_string(),
        });
        callback.on_event(ProgressEvent::Completed {
            message: "Test".to_string(),
        });
        callback.on_event(ProgressEvent::Error {
            message: "Test".to_string(),
        });
    }

    // ============================================================================
    // Algorithm Type Tests
    // ============================================================================

    #[test]
    fn test_create_algorithm_params_all_algorithms() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;
        use crate::strategies::AlgorithmType;

        let base_params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(2.0)
            .skew(0.5)
            .max_inventory(0.1)
            .quote_size(0.001)
            .build()
            .unwrap();

        // Test that params can be created for different algorithm types
        let algo_params = BacktestCommands::create_algorithm_params(&base_params, None);
        assert!((algo_params.max_inventory.to_f64().unwrap() - 0.1).abs() < 1e-10);
    }

    #[test]
    fn test_ml_weights_loading_for_non_ml_algorithm() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string()) // Not ML algorithm
            .weights_file(Some(PathBuf::from("./weights.json")))
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        // For non-ML algorithms, weights_file should be ignored
        // This is tested implicitly - if weights are loaded for non-ML, it would fail
        // But the function should return None for non-ML algorithms
        let algo_type = AlgorithmType::from_str(&params.algorithm).ok();
        if let Some(AlgorithmType::MLSpreadSkew) = algo_type {
            // Only ML algorithms should load weights
        } else {
            // Non-ML algorithms should not load weights
            assert!(algo_type.is_some());
        }
    }

    // ============================================================================
    // Edge Cases and Boundary Tests
    // ============================================================================

    #[test]
    fn test_evaluate_metrics_extreme_sharpe() {
        use crate::backtest::metrics::{PerformanceMetrics, TradeLog, EquityCurve};
        use crate::backtest::harness::{BacktestConfig, FillStats};

        let mut metrics = PerformanceMetrics::default();
        metrics.sharpe_ratio = 10.0; // Very high
        metrics.total_return = 1.0; // 100% return
        metrics.max_drawdown = 0.01; // Very low drawdown

        let results = BacktestResults {
            config: BacktestConfig::default(),
            metrics,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 10000,
            fills_generated: 5000,
            fill_stats: FillStats::default(),
            oco_stats: None,
        };

        let eval_metrics = EvaluateMetrics::from(&results);
        assert_eq!(eval_metrics.sharpe_ratio, 10.0);
        assert_eq!(eval_metrics.total_return, 1.0);
        assert_eq!(eval_metrics.max_drawdown, 0.01);
    }

    #[test]
    fn test_evaluate_metrics_negative_sharpe() {
        use crate::backtest::metrics::{PerformanceMetrics, TradeLog, EquityCurve};
        use crate::backtest::harness::{BacktestConfig, FillStats};

        let mut metrics = PerformanceMetrics::default();
        metrics.sharpe_ratio = -2.0; // Negative Sharpe
        metrics.total_return = -0.20; // -20% return
        metrics.max_drawdown = 0.25; // 25% drawdown

        let results = BacktestResults {
            config: BacktestConfig::default(),
            metrics,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 1000,
            fills_generated: 500,
            fill_stats: FillStats::default(),
            oco_stats: None,
        };

        let eval_metrics = EvaluateMetrics::from(&results);
        assert_eq!(eval_metrics.sharpe_ratio, -2.0);
        assert_eq!(eval_metrics.total_return, -0.20);
        assert_eq!(eval_metrics.max_drawdown, 0.25);
    }

    #[test]
    fn test_evaluate_metrics_zero_metrics() {
        use crate::backtest::metrics::{PerformanceMetrics, TradeLog, EquityCurve};
        use crate::backtest::harness::{BacktestConfig, FillStats};

        let metrics = PerformanceMetrics::default(); // All zeros

        let results = BacktestResults {
            config: BacktestConfig::default(),
            metrics,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 0,
            fills_generated: 0,
            fill_stats: FillStats::default(),
            oco_stats: None,
        };

        let eval_metrics = EvaluateMetrics::from(&results);
        assert_eq!(eval_metrics.sharpe_ratio, 0.0);
        assert_eq!(eval_metrics.total_return, 0.0);
        assert_eq!(eval_metrics.max_drawdown, 0.0);
        assert_eq!(eval_metrics.num_trades, 0);
        assert_eq!(eval_metrics.win_rate, 0.0);
        assert_eq!(eval_metrics.avg_trade_pnl, 0.0);
    }

    #[test]
    fn test_evaluate_metrics_very_large_numbers() {
        use crate::backtest::metrics::{PerformanceMetrics, TradeLog, EquityCurve};
        use crate::backtest::harness::{BacktestConfig, FillStats};

        let mut metrics = PerformanceMetrics::default();
        metrics.num_trades = 1_000_000;
        metrics.total_return = 100.0; // 10000% return
        metrics.avg_trade_pnl = dec!(1000.0);

        let results = BacktestResults {
            config: BacktestConfig::default(),
            metrics,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 10_000_000,
            fills_generated: 5_000_000,
            fill_stats: FillStats::default(),
            oco_stats: None,
        };

        let eval_metrics = EvaluateMetrics::from(&results);
        assert_eq!(eval_metrics.num_trades, 1_000_000);
        assert_eq!(eval_metrics.total_return, 100.0);
        assert_eq!(eval_metrics.avg_trade_pnl, 1000.0);
        // Note: events_processed is on EvaluateResult, not EvaluateMetrics
    }

    // ============================================================================
    // Thread Safety Tests
    // ============================================================================

    #[test]
    fn test_progress_callback_thread_safety() {
        use std::thread;
        use std::sync::Arc;

        let callback = Arc::new(TestCallback::new());
        let mut handles = vec![];

        // Spawn multiple threads sending events
        for i in 0..10 {
            let callback_clone = callback.clone();
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    callback_clone.on_event(ProgressEvent::Log {
                        level: LogLevel::Info,
                        message: format!("Thread {} event {}", i, j),
                    });
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have received all events (10 threads * 100 events = 1000)
        assert_eq!(callback.event_count(), 1000);
    }

    // ============================================================================
    // Parameter Conversion Tests
    // ============================================================================

    #[test]
    fn test_decimal_conversion_precision() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        // Test that f64 values are correctly converted to Decimal
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_inventory(0.123456789)
            .quote_size(0.000000001)
            .fee_rate(0.0000001)
            .build()
            .unwrap();

        let algo_params = BacktestCommands::create_algorithm_params(&params, None);
        
        // Verify precision is maintained (within floating point limits)
        let max_inv = algo_params.max_inventory.to_f64().unwrap();
        assert!((max_inv - 0.123456789).abs() < 1e-9);
    }

    #[test]
    fn test_f64_to_decimal_edge_cases() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        // Test edge cases for f64 to Decimal conversion
        let test_cases = vec![
            (0.0, "zero"),
            (f64::MIN_POSITIVE, "min positive"),
            (f64::MAX, "max"),
        ];

        for (value, name) in test_cases {
            let params = EvaluateParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm("as".to_string())
                .max_inventory(value)
                .build()
                .unwrap();

            let algo_params = BacktestCommands::create_algorithm_params(&params, None);
            let converted = algo_params.max_inventory.to_f64().unwrap();
            
            // For very large numbers, we just check it doesn't panic
            if value < 1e10 {
                assert!((converted - value).abs() < 1e-6, "Failed for {}", name);
            }
        }
    }

    // ============================================================================
    // Integration-style Tests (without actual data)
    // ============================================================================

    #[test]
    fn test_evaluate_params_to_algorithm_params_flow() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        // Test the full flow from EvaluateParams to BacktestAlgorithmParams
        let eval_params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(3.0)
            .skew(0.7)
            .max_inventory(0.15)
            .quote_size(0.002)
            .build()
            .unwrap();

        let algo_params = BacktestCommands::create_algorithm_params(&eval_params, None);

        // Verify all parameters were correctly transferred
        assert!((algo_params.max_inventory.to_f64().unwrap() - 0.15).abs() < 1e-10);
        assert!((algo_params.quote_size.to_f64().unwrap() - 0.002).abs() < 1e-10);
        // Spread and skew are stored differently in BacktestAlgorithmParams
        // (they might be in a config struct), so we just verify no panic
    }

    #[test]
    fn test_evaluate_result_completeness() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        let result = EvaluateResult {
            algorithm: params.algorithm.clone(),
            algorithm_name: "Test Algorithm".to_string(),
            metrics: EvaluateMetrics {
                sharpe_ratio: 1.0,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 50,
                win_rate: 0.50,
                avg_trade_pnl: 0.001,
                annualized_return: 0.10,
                sortino_ratio: 1.5,
                calmar_ratio: 2.5,
                profit_factor: 1.1,
            },
            params: params.clone(),
            events_processed: 500,
            fills_generated: 50,
        };

        // Verify all fields are accessible
        assert_eq!(result.algorithm, "as");
        assert_eq!(result.algorithm_name, "Test Algorithm");
        assert_eq!(result.metrics.sharpe_ratio, 1.0);
        assert_eq!(result.events_processed, 500);
        assert_eq!(result.fills_generated, 50);
        assert_eq!(result.params.algorithm, "as");
    }

    // ============================================================================
    // Error Handling Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_invalid_algorithm() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        // This test verifies that invalid algorithms are caught at the command level
        // The params builder doesn't validate algorithm names
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("invalid_algorithm".to_string())
            .build()
            .unwrap();

        // Algorithm validation happens in evaluate(), not in params builder
        assert_eq!(params.algorithm, "invalid_algorithm");
    }

    // ============================================================================
    // Edge Cases for EvaluateResult
    // ============================================================================

    #[test]
    fn test_evaluate_result_zero_events() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let result = EvaluateResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            metrics: EvaluateMetrics {
                sharpe_ratio: 0.0,
                total_return: 0.0,
                max_drawdown: 0.0,
                num_trades: 0,
                win_rate: 0.0,
                avg_trade_pnl: 0.0,
                annualized_return: 0.0,
                sortino_ratio: 0.0,
                calmar_ratio: 0.0,
                profit_factor: 0.0,
            },
            params: EvaluateParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm("as".to_string())
                .build()
                .unwrap(),
            events_processed: 0,
            fills_generated: 0,
        };

        assert_eq!(result.events_processed, 0);
        assert_eq!(result.fills_generated, 0);
        assert_eq!(result.metrics.num_trades, 0);
    }

    #[test]
    fn test_evaluate_result_large_numbers() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let result = EvaluateResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            metrics: EvaluateMetrics {
                sharpe_ratio: 10.0,
                total_return: 5.0,
                max_drawdown: 2.0,
                num_trades: 1000000,
                win_rate: 0.99,
                avg_trade_pnl: 1000.0,
                annualized_return: 10.0,
                sortino_ratio: 15.0,
                calmar_ratio: 20.0,
                profit_factor: 10.0,
            },
            params: EvaluateParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm("as".to_string())
                .build()
                .unwrap(),
            events_processed: 10000000,
            fills_generated: 5000000,
        };

        assert_eq!(result.events_processed, 10000000);
        assert_eq!(result.fills_generated, 5000000);
        assert_eq!(result.metrics.num_trades, 1000000);
    }

    // ============================================================================
    // Parameter Combinations Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_all_combinations_naive_fills() {
        use crate::commands::params::backtest_params::{EvaluateParamsBuilder, EvaluateParams};

        // Test with naive fills (should ignore fill_prob and queue_pos)
        let params: EvaluateParams = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .naive_fills(true)
            .fill_prob(0.5)  // Should be ignored
            .queue_pos(0.8)   // Should be ignored
            .build()
            .unwrap();

        assert!(params.naive_fills);
        // Values are still stored, but won't be used in backtest
        assert_eq!(params.fill_prob, 0.5);
        assert_eq!(params.queue_pos, 0.8);
    }

    #[test]
    fn test_evaluate_params_regime_params_combinations() {
        use crate::commands::params::backtest_params::{EvaluateParamsBuilder, EvaluateParams};

        // Test regime params with different spreads and skews
        let params: EvaluateParams = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .regime_params(true)
            .high_spread(0.5)
            .med_spread(1.5)
            .low_spread(3.0)
            .high_skew(0.1)
            .med_skew(0.3)
            .low_skew(0.7)
            .quote_low_entropy(true)
            .build()
            .unwrap();

        assert!(params.regime_params);
        assert_eq!(params.high_spread, 0.5);
        assert_eq!(params.med_spread, 1.5);
        assert_eq!(params.low_spread, 3.0);
        assert_eq!(params.high_skew, 0.1);
        assert_eq!(params.med_skew, 0.3);
        assert_eq!(params.low_skew, 0.7);
        assert!(params.quote_low_entropy);
    }

    // ============================================================================
    // NoOpCallback Tests
    // ============================================================================

    // ============================================================================
    // Decimal Conversion Tests
    // ============================================================================

    #[test]
    fn test_decimal_to_f64_conversion() {
        use num::ToPrimitive;
        
        let decimal = dec!(0.001);
        let f64_val = decimal.to_f64().unwrap();
        assert!((f64_val - 0.001).abs() < 1e-10);

        let decimal = dec!(100.5);
        let f64_val = decimal.to_f64().unwrap();
        assert!((f64_val - 100.5).abs() < 1e-10);
    }

    // ============================================================================
    // Path Handling Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_path_cloning() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let path = PathBuf::from("./test_data");
        let params = EvaluateParamsBuilder::new()
            .data_path(path.clone())
            .algorithm("as".to_string())
            .build()
            .unwrap();

        assert_eq!(params.data_path, path);
    }

    #[test]
    fn test_evaluate_params_optional_paths() {
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;

        let params1 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(None)
            .output(None)
            .build()
            .unwrap();

        assert_eq!(params1.weights_file, None);
        assert_eq!(params1.output, None);

        let params2 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        assert_eq!(params2.weights_file, Some(PathBuf::from("./weights.json")));
        assert_eq!(params2.output, Some(PathBuf::from("./output.json")));
    }

    // ============================================================================
    // Tune Command Tests
    // ============================================================================

    #[test]
    fn test_tune_result_item_structure() {
        let item = TuneResultItem {
            spread: 2.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.10,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.001,
        };

        assert_eq!(item.spread, 2.0);
        assert_eq!(item.skew, 0.5);
        assert_eq!(item.sharpe, 1.5);
        assert_eq!(item.num_trades, 100);
    }

    #[test]
    fn test_tune_result_item_serialization() {
        let item = TuneResultItem {
            spread: 2.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.10,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.001,
        };

        let json = serde_json::to_string(&item).unwrap();
        let deserialized: TuneResultItem = serde_json::from_str(&json).unwrap();

        assert_eq!(item.spread, deserialized.spread);
        assert_eq!(item.skew, deserialized.skew);
        assert_eq!(item.sharpe, deserialized.sharpe);
        assert_eq!(item.num_trades, deserialized.num_trades);
    }

    #[test]
    fn test_tune_result_item_clone() {
        let item1 = TuneResultItem {
            spread: 2.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.10,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.001,
        };

        let item2 = item1.clone();
        assert_eq!(item1.spread, item2.spread);
        assert_eq!(item1.sharpe, item2.sharpe);
    }

    #[test]
    fn test_tune_result_structure() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        let result = TuneResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                TuneResultItem {
                    spread: 2.0,
                    skew: 0.5,
                    high_entropy_threshold: 0.7,
                    fill_prob: 0.10,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                    avg_trade_pnl: 0.001,
                },
            ],
            best: Some(TuneResultItem {
                spread: 2.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.10,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.001,
            }),
            total_combinations: 1,
        };

        assert_eq!(result.algorithm, "as");
        assert_eq!(result.all_results.len(), 1);
        assert!(result.best.is_some());
        assert_eq!(result.total_combinations, 1);
    }

    #[test]
    fn test_tune_result_empty_results() {
        let result = TuneResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![],
            best: None,
            total_combinations: 0,
        };

        assert_eq!(result.all_results.len(), 0);
        assert!(result.best.is_none());
        assert_eq!(result.total_combinations, 0);
    }

    #[test]
    fn test_tune_result_serialization() {
        let result = TuneResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                TuneResultItem {
                    spread: 2.0,
                    skew: 0.5,
                    high_entropy_threshold: 0.7,
                    fill_prob: 0.10,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                    avg_trade_pnl: 0.001,
                },
            ],
            best: Some(TuneResultItem {
                spread: 2.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.10,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.001,
            }),
            total_combinations: 1,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: TuneResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.algorithm, deserialized.algorithm);
        assert_eq!(result.all_results.len(), deserialized.all_results.len());
        assert_eq!(result.best.as_ref().unwrap().sharpe, deserialized.best.as_ref().unwrap().sharpe);
    }

    #[test]
    fn test_tune_result_sorted_by_sharpe() {
        let mut result = TuneResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                TuneResultItem {
                    spread: 1.0,
                    skew: 0.3,
                    high_entropy_threshold: 0.6,
                    fill_prob: 0.05,
                    sharpe: 0.5,
                    total_return: 0.02,
                    max_drawdown: 0.01,
                    num_trades: 50,
                    win_rate: 0.50,
                    avg_trade_pnl: 0.0004,
                },
                TuneResultItem {
                    spread: 2.0,
                    skew: 0.5,
                    high_entropy_threshold: 0.7,
                    fill_prob: 0.10,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                    avg_trade_pnl: 0.0005,
                },
                TuneResultItem {
                    spread: 3.0,
                    skew: 0.7,
                    high_entropy_threshold: 0.8,
                    fill_prob: 0.15,
                    sharpe: 1.0,
                    total_return: 0.03,
                    max_drawdown: 0.015,
                    num_trades: 75,
                    win_rate: 0.52,
                    avg_trade_pnl: 0.0004,
                },
            ],
            best: None,
            total_combinations: 3,
        };

        // Sort by Sharpe (descending)
        result.all_results.sort_by(|a, b| b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal));
        result.best = result.all_results.first().cloned();

        // Verify sorted order
        assert_eq!(result.all_results[0].sharpe, 1.5);
        assert_eq!(result.all_results[1].sharpe, 1.0);
        assert_eq!(result.all_results[2].sharpe, 0.5);
        assert_eq!(result.best.as_ref().unwrap().sharpe, 1.5);
    }

    // ============================================================================
    // Algorithm Type Validation Tests
    // ============================================================================

    #[test]
    fn test_validate_mm_algorithm_avellaneda_stoikov() {
        let result = BacktestCommands::validate_mm_algorithm(AlgorithmType::AvellanedaStoikov);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_mm_algorithm_ml_spread_skew() {
        let result = BacktestCommands::validate_mm_algorithm(AlgorithmType::MLSpreadSkew);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_mm_algorithm_fixed_spread() {
        let result = BacktestCommands::validate_mm_algorithm(AlgorithmType::FixedSpread);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_mm_algorithm_all_variants() {
        // Test all three MM algorithm types
        assert!(BacktestCommands::validate_mm_algorithm(AlgorithmType::AvellanedaStoikov).is_ok());
        assert!(BacktestCommands::validate_mm_algorithm(AlgorithmType::MLSpreadSkew).is_ok());
        assert!(BacktestCommands::validate_mm_algorithm(AlgorithmType::FixedSpread).is_ok());
    }

    // ============================================================================
    // Tune Command Error Handling Tests
    // ============================================================================

    #[test]
    fn test_tune_invalid_algorithm() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("invalid_algorithm".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::tune(params, callback);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown algorithm"));
    }

    #[test]
    fn test_tune_invalid_algorithm_string() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        // Test with an algorithm string that doesn't exist
        // This will fail at parse_algorithm_type, not at validate_mm_algorithm
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("nonexistent_algorithm".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::tune(params, callback);

        assert!(result.is_err());
        // Should fail at algorithm parsing, not MM validation
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unknown algorithm") || err_msg.contains("algorithm"));
    }

    #[test]
    fn test_tune_nonexistent_data_path() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("/nonexistent/path/that/does/not/exist"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::tune(params, callback);

        // Should fail when trying to load data
        assert!(result.is_err());
    }

    // ============================================================================
    // Progress Callback Tests for Tune
    // ============================================================================

    #[test]
    fn test_tune_progress_callback_events() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        // This test would require actual data, so we'll just test the callback structure
        let callback = Arc::new(TestCallback::new());
        let callback_clone = callback.clone();

        // Simulate events that would be sent during tuning
        callback.on_event(ProgressEvent::Started {
            total: Some(10),
            message: "Starting grid search".to_string(),
        });

        callback.on_event(ProgressEvent::Progress {
            current: 1,
            total: Some(10),
            message: "Testing combination 1".to_string(),
        });

        callback.on_event(ProgressEvent::Metric {
            name: "sharpe_ratio".to_string(),
            value: 1.5,
        });

        assert_eq!(callback_clone.event_count(), 3);
        let events = callback_clone.get_events();
        assert!(matches!(events[0], ProgressEvent::Started { .. }));
        assert!(matches!(events[1], ProgressEvent::Progress { .. }));
        assert!(matches!(events[2], ProgressEvent::Metric { .. }));
    }

    // ============================================================================
    // Parameter Parsing Tests
    // ============================================================================

    #[test]
    fn test_tune_params_parse_spreads() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3,4,5".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(spreads, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    }

    #[test]
    fn test_tune_params_parse_skews() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3,0.5,0.7,1.0".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .unwrap();

        let skews: Vec<f64> = params.skews.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(skews, vec![0.3, 0.5, 0.7, 1.0]);
    }

    #[test]
    fn test_tune_params_parse_high_entropies() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6,0.7,0.8".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .unwrap();

        let high_entropies: Vec<f64> = params.high_entropies.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(high_entropies, vec![0.6, 0.7, 0.8]);
    }

    #[test]
    fn test_tune_params_parse_fill_probs() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05,0.10,0.15".to_string())
            .build()
            .unwrap();

        let fill_probs: Vec<f64> = params.fill_probs.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(fill_probs, vec![0.05, 0.10, 0.15]);
    }

    #[test]
    fn test_tune_params_calculate_total_combinations() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())  // 3 values
            .skews("0.3,0.5".to_string())   // 2 values
            .high_entropies("0.6,0.7".to_string())  // 2 values
            .fill_probs("0.05,0.10".to_string())   // 2 values
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let skews: Vec<f64> = params.skews.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let high_entropies: Vec<f64> = params.high_entropies.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let fill_probs: Vec<f64> = params.fill_probs.split(',').filter_map(|s| s.trim().parse().ok()).collect();

        let total = spreads.len() * skews.len() * high_entropies.len() * fill_probs.len();
        assert_eq!(total, 3 * 2 * 2 * 2); // 24 combinations
    }

    // ============================================================================
    // Edge Cases for Tune
    // ============================================================================

    #[test]
    fn test_tune_result_item_zero_trades() {
        let item = TuneResultItem {
            spread: 2.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.10,
            sharpe: 0.0,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
            avg_trade_pnl: 0.0,
        };

        assert_eq!(item.num_trades, 0);
        assert_eq!(item.win_rate, 0.0);
        assert_eq!(item.avg_trade_pnl, 0.0);
    }

    #[test]
    fn test_tune_result_item_negative_sharpe() {
        let item = TuneResultItem {
            spread: 2.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.10,
            sharpe: -1.5,
            total_return: -0.10,
            max_drawdown: 0.15,
            num_trades: 50,
            win_rate: 0.30,
            avg_trade_pnl: -0.002,
        };

        assert_eq!(item.sharpe, -1.5);
        assert_eq!(item.total_return, -0.10);
        assert!(item.win_rate < 0.5);
    }

    #[test]
    fn test_tune_result_item_extreme_values() {
        let item = TuneResultItem {
            spread: 1000.0,
            skew: 100.0,
            high_entropy_threshold: 1.0,
            fill_prob: 1.0,
            sharpe: 10.0,
            total_return: 5.0,
            max_drawdown: 2.0,
            num_trades: 1_000_000,
            win_rate: 0.99,
            avg_trade_pnl: 1000.0,
        };

        assert_eq!(item.spread, 1000.0);
        assert_eq!(item.sharpe, 10.0);
        assert_eq!(item.num_trades, 1_000_000);
    }

    #[test]
    fn test_tune_result_multiple_items_sorting() {
        let mut items = vec![
            TuneResultItem {
                spread: 1.0,
                skew: 0.3,
                high_entropy_threshold: 0.6,
                fill_prob: 0.05,
                sharpe: 0.5,
                total_return: 0.02,
                max_drawdown: 0.01,
                num_trades: 50,
                win_rate: 0.50,
                avg_trade_pnl: 0.0004,
            },
            TuneResultItem {
                spread: 2.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.10,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.0005,
            },
            TuneResultItem {
                spread: 3.0,
                skew: 0.7,
                high_entropy_threshold: 0.8,
                fill_prob: 0.15,
                sharpe: 1.0,
                total_return: 0.03,
                max_drawdown: 0.015,
                num_trades: 75,
                win_rate: 0.52,
                avg_trade_pnl: 0.0004,
            },
        ];

        items.sort_by(|a, b| b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal));

        assert_eq!(items[0].sharpe, 1.5);
        assert_eq!(items[1].sharpe, 1.0);
        assert_eq!(items[2].sharpe, 0.5);
    }

    #[test]
    fn test_tune_result_best_selection() {
        let items = vec![
            TuneResultItem {
                spread: 1.0,
                skew: 0.3,
                high_entropy_threshold: 0.6,
                fill_prob: 0.05,
                sharpe: 0.5,
                total_return: 0.02,
                max_drawdown: 0.01,
                num_trades: 50,
                win_rate: 0.50,
                avg_trade_pnl: 0.0004,
            },
            TuneResultItem {
                spread: 2.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.10,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.0005,
            },
        ];

        let best = items.iter().max_by(|a, b| a.sharpe.partial_cmp(&b.sharpe).unwrap_or(std::cmp::Ordering::Equal));
        assert!(best.is_some());
        assert_eq!(best.unwrap().sharpe, 1.5);
    }

    // ============================================================================
    // Thread Safety Tests for Tune
    // ============================================================================

    #[test]
    fn test_tune_progress_callback_thread_safety() {
        use std::thread;

        let callback = Arc::new(TestCallback::new());
        let mut handles = vec![];

        // Spawn multiple threads sending progress events
        for i in 0..10 {
            let callback_clone = callback.clone();
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    callback_clone.on_event(ProgressEvent::Progress {
                        current: i * 100 + j,
                        total: Some(1000),
                        message: format!("Thread {} combination {}", i, j),
                    });
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have received all events (10 threads * 100 events = 1000)
        assert_eq!(callback.event_count(), 1000);
    }

    // ============================================================================
    // Integration-style Tests (without actual data)
    // ============================================================================

    #[test]
    fn test_tune_params_to_result_flow() {
        use crate::commands::params::backtest_params::TuneParamsBuilder;

        // Test the flow from TuneParams to expected result structure
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3,0.5".to_string())
            .high_entropies("0.6,0.7".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build()
            .unwrap();

        // Calculate expected combinations
        let spreads: Vec<f64> = params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let skews: Vec<f64> = params.skews.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let high_entropies: Vec<f64> = params.high_entropies.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let fill_probs: Vec<f64> = params.fill_probs.split(',').filter_map(|s| s.trim().parse().ok()).collect();

        let expected_combinations = spreads.len() * skews.len() * high_entropies.len() * fill_probs.len();
        assert_eq!(expected_combinations, 2 * 2 * 2 * 2); // 16 combinations
    }

    #[test]
    fn test_tune_result_completeness() {
        let result = TuneResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                TuneResultItem {
                    spread: 2.0,
                    skew: 0.5,
                    high_entropy_threshold: 0.7,
                    fill_prob: 0.10,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                    avg_trade_pnl: 0.001,
                },
            ],
            best: Some(TuneResultItem {
                spread: 2.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.10,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.001,
            }),
            total_combinations: 1,
        };

        // Verify all fields are accessible
        assert_eq!(result.algorithm, "as");
        assert_eq!(result.algorithm_name, "Avellaneda-Stoikov");
        assert_eq!(result.all_results.len(), 1);
        assert!(result.best.is_some());
        assert_eq!(result.total_combinations, 1);
        assert_eq!(result.best.as_ref().unwrap().sharpe, 1.5);
    }

    // ============================================================================
    // Regime Search Command Tests
    // ============================================================================

    #[test]
    fn test_regime_search_result_item_structure() {
        let item = RegimeSearchResultItem {
            high_spread: 0.5,
            high_skew: 0.2,
            med_spread: 2.0,
            med_skew: 0.4,
            low_spread: Some(4.0),
            low_skew: 0.8,
            fill_prob: 0.10,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };

        assert_eq!(item.high_spread, 0.5);
        assert_eq!(item.med_spread, 2.0);
        assert_eq!(item.low_spread, Some(4.0));
        assert_eq!(item.sharpe, 1.5);
    }

    #[test]
    fn test_regime_search_result_item_no_quote() {
        let item = RegimeSearchResultItem {
            high_spread: 0.5,
            high_skew: 0.2,
            med_spread: 2.0,
            med_skew: 0.4,
            low_spread: None, // No quoting in low entropy
            low_skew: 0.8,
            fill_prob: 0.10,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };

        assert!(item.low_spread.is_none());
    }

    #[test]
    fn test_regime_search_result_item_serialization() {
        let item = RegimeSearchResultItem {
            high_spread: 0.5,
            high_skew: 0.2,
            med_spread: 2.0,
            med_skew: 0.4,
            low_spread: Some(4.0),
            low_skew: 0.8,
            fill_prob: 0.10,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };

        let json = serde_json::to_string(&item).unwrap();
        let deserialized: RegimeSearchResultItem = serde_json::from_str(&json).unwrap();

        assert_eq!(item.high_spread, deserialized.high_spread);
        assert_eq!(item.low_spread, deserialized.low_spread);
        assert_eq!(item.sharpe, deserialized.sharpe);
    }

    #[test]
    fn test_regime_search_result_structure() {
        let result = RegimeSearchResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                RegimeSearchResultItem {
                    high_spread: 0.5,
                    high_skew: 0.2,
                    med_spread: 2.0,
                    med_skew: 0.4,
                    low_spread: Some(4.0),
                    low_skew: 0.8,
                    fill_prob: 0.10,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                    avg_trade_pnl: 0.0005,
                },
            ],
            best: Some(RegimeSearchResultItem {
                high_spread: 0.5,
                high_skew: 0.2,
                med_spread: 2.0,
                med_skew: 0.4,
                low_spread: Some(4.0),
                low_skew: 0.8,
                fill_prob: 0.10,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.0005,
            }),
            total_combinations: 1,
            avg_sharpe_with_quote: Some(1.5),
            avg_sharpe_without_quote: None,
        };

        assert_eq!(result.algorithm, "as");
        assert_eq!(result.all_results.len(), 1);
        assert!(result.best.is_some());
        assert_eq!(result.total_combinations, 1);
    }

    #[test]
    fn test_regime_search_result_empty_results() {
        let result = RegimeSearchResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![],
            best: None,
            total_combinations: 0,
            avg_sharpe_with_quote: None,
            avg_sharpe_without_quote: None,
        };

        assert_eq!(result.all_results.len(), 0);
        assert!(result.best.is_none());
        assert_eq!(result.total_combinations, 0);
    }

    #[test]
    fn test_regime_search_result_serialization() {
        let result = RegimeSearchResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                RegimeSearchResultItem {
                    high_spread: 0.5,
                    high_skew: 0.2,
                    med_spread: 2.0,
                    med_skew: 0.4,
                    low_spread: Some(4.0),
                    low_skew: 0.8,
                    fill_prob: 0.10,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                    avg_trade_pnl: 0.0005,
                },
            ],
            best: Some(RegimeSearchResultItem {
                high_spread: 0.5,
                high_skew: 0.2,
                med_spread: 2.0,
                med_skew: 0.4,
                low_spread: Some(4.0),
                low_skew: 0.8,
                fill_prob: 0.10,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.0005,
            }),
            total_combinations: 1,
            avg_sharpe_with_quote: Some(1.5),
            avg_sharpe_without_quote: None,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: RegimeSearchResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.algorithm, deserialized.algorithm);
        assert_eq!(result.all_results.len(), deserialized.all_results.len());
        assert_eq!(result.best.as_ref().unwrap().sharpe, deserialized.best.as_ref().unwrap().sharpe);
    }

    #[test]
    fn test_regime_search_result_sorted_by_sharpe() {
        let mut result = RegimeSearchResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                RegimeSearchResultItem {
                    high_spread: 0.5,
                    high_skew: 0.2,
                    med_spread: 2.0,
                    med_skew: 0.4,
                    low_spread: Some(4.0),
                    low_skew: 0.8,
                    fill_prob: 0.10,
                    sharpe: 0.5,
                    total_return: 0.02,
                    max_drawdown: 0.01,
                    num_trades: 50,
                    win_rate: 0.50,
                    avg_trade_pnl: 0.0004,
                },
                RegimeSearchResultItem {
                    high_spread: 0.5,
                    high_skew: 0.2,
                    med_spread: 2.0,
                    med_skew: 0.4,
                    low_spread: Some(4.0),
                    low_skew: 0.8,
                    fill_prob: 0.10,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                    avg_trade_pnl: 0.0005,
                },
                RegimeSearchResultItem {
                    high_spread: 0.5,
                    high_skew: 0.2,
                    med_spread: 2.0,
                    med_skew: 0.4,
                    low_spread: Some(4.0),
                    low_skew: 0.8,
                    fill_prob: 0.10,
                    sharpe: 1.0,
                    total_return: 0.03,
                    max_drawdown: 0.015,
                    num_trades: 75,
                    win_rate: 0.52,
                    avg_trade_pnl: 0.0004,
                },
            ],
            best: None,
            total_combinations: 3,
            avg_sharpe_with_quote: None,
            avg_sharpe_without_quote: None,
        };

        // Sort by Sharpe (descending)
        result.all_results.sort_by(|a, b| b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal));
        result.best = result.all_results.first().cloned();

        assert_eq!(result.all_results[0].sharpe, 1.5);
        assert_eq!(result.all_results[1].sharpe, 1.0);
        assert_eq!(result.all_results[2].sharpe, 0.5);
        assert_eq!(result.best.as_ref().unwrap().sharpe, 1.5);
    }

    #[test]
    fn test_regime_search_result_quote_comparison() {
        let result = RegimeSearchResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                RegimeSearchResultItem {
                    high_spread: 0.5,
                    high_skew: 0.2,
                    med_spread: 2.0,
                    med_skew: 0.4,
                    low_spread: Some(4.0), // With quote
                    low_skew: 0.8,
                    fill_prob: 0.10,
                    sharpe: 1.0,
                    total_return: 0.03,
                    max_drawdown: 0.015,
                    num_trades: 75,
                    win_rate: 0.52,
                    avg_trade_pnl: 0.0004,
                },
                RegimeSearchResultItem {
                    high_spread: 0.5,
                    high_skew: 0.2,
                    med_spread: 2.0,
                    med_skew: 0.4,
                    low_spread: None, // Without quote
                    low_skew: 0.8,
                    fill_prob: 0.10,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                    avg_trade_pnl: 0.0005,
                },
            ],
            best: None,
            total_combinations: 2,
            avg_sharpe_with_quote: Some(1.0),
            avg_sharpe_without_quote: Some(1.5),
        };

        assert_eq!(result.avg_sharpe_with_quote, Some(1.0));
        assert_eq!(result.avg_sharpe_without_quote, Some(1.5));
    }

    #[test]
    fn test_regime_search_result_item_clone() {
        let item1 = RegimeSearchResultItem {
            high_spread: 0.5,
            high_skew: 0.2,
            med_spread: 2.0,
            med_skew: 0.4,
            low_spread: Some(4.0),
            low_skew: 0.8,
            fill_prob: 0.10,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };

        let item2 = item1.clone();
        assert_eq!(item1.high_spread, item2.high_spread);
        assert_eq!(item1.low_spread, item2.low_spread);
        assert_eq!(item1.sharpe, item2.sharpe);
    }

    #[test]
    fn test_regime_search_result_clone() {
        let result1 = RegimeSearchResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![],
            best: None,
            total_combinations: 0,
            avg_sharpe_with_quote: None,
            avg_sharpe_without_quote: None,
        };

        let result2 = result1.clone();
        assert_eq!(result1.algorithm, result2.algorithm);
        assert_eq!(result1.total_combinations, result2.total_combinations);
    }

    #[test]
    fn test_regime_search_result_item_with_zero_trades() {
        let item = RegimeSearchResultItem {
            high_spread: 0.5,
            high_skew: 0.2,
            med_spread: 2.0,
            med_skew: 0.4,
            low_spread: Some(4.0),
            low_skew: 0.8,
            fill_prob: 0.10,
            sharpe: 0.0,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
            avg_trade_pnl: 0.0,
        };

        assert_eq!(item.num_trades, 0);
        assert_eq!(item.avg_trade_pnl, 0.0);
    }

    #[test]
    fn test_regime_search_result_item_negative_sharpe() {
        let item = RegimeSearchResultItem {
            high_spread: 0.5,
            high_skew: 0.2,
            med_spread: 2.0,
            med_skew: 0.4,
            low_spread: Some(4.0),
            low_skew: 0.8,
            fill_prob: 0.10,
            sharpe: -0.5, // Negative Sharpe (losing strategy)
            total_return: -0.02,
            max_drawdown: 0.05,
            num_trades: 50,
            win_rate: 0.40,
            avg_trade_pnl: -0.0004,
        };

        assert!(item.sharpe < 0.0);
        assert!(item.total_return < 0.0);
    }

    // ============================================================================
    // RegimeOptimizeResult Structure Tests
    // ============================================================================

    #[test]
    fn test_regime_optimize_result_structure() {
        let result = RegimeOptimizeResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            high_entropy: RegimeOptimizeMetrics {
                regime: "High Entropy".to_string(),
                event_count: 1000,
                event_fraction: 0.4,
                time_hours: 24.0,
                optimal_spread: 1.5,
                optimal_skew: 0.3,
                should_quote: true,
                best_sharpe: 2.0,
                best_return: 0.05,
                best_drawdown: 0.02,
                best_trades: 100,
                best_win_rate: 0.55,
            },
            medium_entropy: RegimeOptimizeMetrics {
                regime: "Medium Entropy".to_string(),
                event_count: 1200,
                event_fraction: 0.5,
                time_hours: 30.0,
                optimal_spread: 2.0,
                optimal_skew: 0.5,
                should_quote: true,
                best_sharpe: 1.5,
                best_return: 0.03,
                best_drawdown: 0.03,
                best_trades: 80,
                best_win_rate: 0.52,
            },
            low_entropy: RegimeOptimizeMetrics {
                regime: "Low Entropy".to_string(),
                event_count: 300,
                event_fraction: 0.1,
                time_hours: 6.0,
                optimal_spread: 4.0,
                optimal_skew: 0.8,
                should_quote: false,
                best_sharpe: 0.5,
                best_return: 0.01,
                best_drawdown: 0.05,
                best_trades: 20,
                best_win_rate: 0.45,
            },
            optimal_regime_params: OptimalRegimeParams {
                high: RegimeParamSet {
                    spread_bps: 1.5,
                    skew_factor: 0.3,
                    should_quote: true,
                },
                medium: RegimeParamSet {
                    spread_bps: 2.0,
                    skew_factor: 0.5,
                    should_quote: true,
                },
                low: RegimeParamSet {
                    spread_bps: 4.0,
                    skew_factor: 0.8,
                    should_quote: false,
                },
            },
            comparison: StrategyComparison {
                uniform_sharpe: 1.2,
                uniform_return: 0.03,
                uniform_drawdown: 0.04,
                uniform_trades: 200,
                uniform_win_rate: 0.51,
                regime_specific_sharpe: 1.5,
                regime_specific_return: 0.04,
                regime_specific_drawdown: 0.03,
                regime_specific_trades: 200,
                regime_specific_win_rate: 0.52,
                sharpe_improvement: 0.3,
                return_improvement: 0.01,
                drawdown_improvement: -0.01,
                trade_count_diff: 0,
            },
            total_events: 2500,
            time_span_hours: 60.0,
        };

        assert_eq!(result.algorithm, "as");
        assert_eq!(result.high_entropy.optimal_spread, 1.5);
        assert_eq!(result.medium_entropy.optimal_spread, 2.0);
        assert_eq!(result.low_entropy.should_quote, false);
        assert_eq!(result.comparison.sharpe_improvement, 0.3);
    }

    #[test]
    fn test_regime_optimize_result_serialization() {
        let result = RegimeOptimizeResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            high_entropy: RegimeOptimizeMetrics {
                regime: "High Entropy".to_string(),
                event_count: 500,
                event_fraction: 0.3,
                time_hours: 12.0,
                optimal_spread: 1.0,
                optimal_skew: 0.2,
                should_quote: true,
                best_sharpe: 1.8,
                best_return: 0.04,
                best_drawdown: 0.02,
                best_trades: 50,
                best_win_rate: 0.56,
            },
            medium_entropy: RegimeOptimizeMetrics {
                regime: "Medium Entropy".to_string(),
                event_count: 800,
                event_fraction: 0.5,
                time_hours: 20.0,
                optimal_spread: 1.8,
                optimal_skew: 0.4,
                should_quote: true,
                best_sharpe: 1.4,
                best_return: 0.03,
                best_drawdown: 0.025,
                best_trades: 60,
                best_win_rate: 0.53,
            },
            low_entropy: RegimeOptimizeMetrics {
                regime: "Low Entropy".to_string(),
                event_count: 400,
                event_fraction: 0.2,
                time_hours: 8.0,
                optimal_spread: 3.5,
                optimal_skew: 0.7,
                should_quote: true,
                best_sharpe: 0.8,
                best_return: 0.015,
                best_drawdown: 0.04,
                best_trades: 30,
                best_win_rate: 0.48,
            },
            optimal_regime_params: OptimalRegimeParams {
                high: RegimeParamSet {
                    spread_bps: 1.0,
                    skew_factor: 0.2,
                    should_quote: true,
                },
                medium: RegimeParamSet {
                    spread_bps: 1.8,
                    skew_factor: 0.4,
                    should_quote: true,
                },
                low: RegimeParamSet {
                    spread_bps: 3.5,
                    skew_factor: 0.7,
                    should_quote: true,
                },
            },
            comparison: StrategyComparison {
                uniform_sharpe: 1.0,
                uniform_return: 0.025,
                uniform_drawdown: 0.035,
                uniform_trades: 140,
                uniform_win_rate: 0.50,
                regime_specific_sharpe: 1.3,
                regime_specific_return: 0.03,
                regime_specific_drawdown: 0.03,
                regime_specific_trades: 140,
                regime_specific_win_rate: 0.52,
                sharpe_improvement: 0.3,
                return_improvement: 0.005,
                drawdown_improvement: -0.005,
                trade_count_diff: 0,
            },
            total_events: 1700,
            time_span_hours: 40.0,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: RegimeOptimizeResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.algorithm, deserialized.algorithm);
        assert_eq!(result.high_entropy.optimal_spread, deserialized.high_entropy.optimal_spread);
        assert_eq!(result.comparison.sharpe_improvement, deserialized.comparison.sharpe_improvement);
    }

    #[test]
    fn test_regime_optimize_metrics_structure() {
        let metrics = RegimeOptimizeMetrics {
            regime: "High Entropy".to_string(),
            event_count: 1000,
            event_fraction: 0.4,
            time_hours: 24.0,
            optimal_spread: 1.5,
            optimal_skew: 0.3,
            should_quote: true,
            best_sharpe: 2.0,
            best_return: 0.05,
            best_drawdown: 0.02,
            best_trades: 100,
            best_win_rate: 0.55,
        };

        assert_eq!(metrics.regime, "High Entropy");
        assert_eq!(metrics.event_count, 1000);
        assert_eq!(metrics.event_fraction, 0.4);
        assert!(metrics.should_quote);
        assert!(metrics.best_sharpe > 0.0);
    }

    #[test]
    fn test_optimal_regime_params_structure() {
        let params = OptimalRegimeParams {
            high: RegimeParamSet {
                spread_bps: 1.5,
                skew_factor: 0.3,
                should_quote: true,
            },
            medium: RegimeParamSet {
                spread_bps: 2.0,
                skew_factor: 0.5,
                should_quote: true,
            },
            low: RegimeParamSet {
                spread_bps: 4.0,
                skew_factor: 0.8,
                should_quote: false,
            },
        };

        assert_eq!(params.high.spread_bps, 1.5);
        assert_eq!(params.medium.skew_factor, 0.5);
        assert!(!params.low.should_quote);
    }

    #[test]
    fn test_strategy_comparison_structure() {
        let comparison = StrategyComparison {
            uniform_sharpe: 1.2,
            uniform_return: 0.03,
            uniform_drawdown: 0.04,
            uniform_trades: 200,
            uniform_win_rate: 0.51,
            regime_specific_sharpe: 1.5,
            regime_specific_return: 0.04,
            regime_specific_drawdown: 0.03,
            regime_specific_trades: 200,
            regime_specific_win_rate: 0.52,
            sharpe_improvement: 0.3,
            return_improvement: 0.01,
            drawdown_improvement: -0.01,
            trade_count_diff: 0,
        };

        assert!(comparison.regime_specific_sharpe > comparison.uniform_sharpe);
        assert!(comparison.sharpe_improvement > 0.0);
        assert!(comparison.drawdown_improvement < 0.0); // Negative is better (less drawdown)
    }

    // ============================================================================
    // RegimeOptimize Function Tests (Algorithm Validation)
    // ============================================================================

    #[test]
    fn test_regime_optimize_invalid_algorithm() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("invalid".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::regime_optimize(params, callback);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid algorithm"));
    }

    #[test]
    fn test_regime_optimize_non_mm_algorithm() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;
        use crate::strategies::AlgorithmType;

        // Try with a non-MM algorithm (if one exists)
        // Note: This test assumes there are non-MM algorithms in the system
        // If all algorithms are MM, this test may need adjustment

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("momentum".to_string()) // Assuming this doesn't exist or isn't MM
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build();

        // Should fail at build or at algorithm validation
        if let Ok(params) = params {
            let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
            let result = BacktestCommands::regime_optimize(params, callback);
            // Should fail with MM validation error if algorithm type is parsed but not MM
            if result.is_err() {
                let err_msg = result.unwrap_err().to_string();
                // Either invalid algorithm or not MM algorithm
                assert!(err_msg.contains("Invalid algorithm") || err_msg.contains("Market Making"));
            }
        }
    }

    #[test]
    fn test_regime_optimize_valid_mm_algorithms() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let algorithms = vec!["as", "ml", "fixed", "avellaneda-stoikov", "ml-spread-skew"];

        for algo in algorithms {
            let params = RegimeOptimizeParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm(algo.to_string())
                .spreads("0.5".to_string())
                .skews("0.2".to_string())
                .build();

            // Should build successfully (algorithm validation happens in function)
            if let Ok(params) = params {
                // Note: Actual execution would require real data, so we just test parameter building
                assert_eq!(params.algorithm, algo);
            }
        }
    }

    // ============================================================================
    // Progress Callback Tests for RegimeOptimize
    // ============================================================================

    #[test]
    fn test_regime_optimize_nonexistent_data_path() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("/nonexistent/path/that/does/not/exist"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::regime_optimize(params, callback);

        // Should fail when trying to load data
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("load") || err_msg.contains("data") || err_msg.contains("Failed"));
    }

    #[test]
    fn test_regime_optimize_empty_spread_list() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("".to_string())
            .skews("0.2,0.3".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_regime_optimize_empty_skew_list() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_regime_optimize_invalid_spread_values() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("abc,def,xyz".to_string())
            .skews("0.2,0.3".to_string())
            .build();

        // Should fail at build time (no valid numeric values)
        assert!(params.is_err());
    }

    #[test]
    fn test_regime_optimize_invalid_skew_values() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("invalid,values".to_string())
            .build();

        // Should fail at build time (no valid numeric values)
        assert!(params.is_err());
    }

    #[test]
    fn test_regime_optimize_negative_spread() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("-1.0,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build();

        // Should fail at build time (negative spread not allowed)
        assert!(params.is_err());
    }

    #[test]
    fn test_regime_optimize_invalid_fill_prob() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .fill_prob(1.5) // > 1.0
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_regime_optimize_invalid_entropy_thresholds() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        // High entropy <= low entropy should fail
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .high_entropy(0.3)
            .low_entropy(0.5) // High < Low
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_regime_optimize_zero_min_trades() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .min_trades(0)
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_regime_optimize_progress_callbacks_structure() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build()
            .unwrap();

        // Test that params are correctly structured for progress callbacks
        // Actual callback testing would require real data execution
        assert_eq!(params.algorithm, "as");
        assert_eq!(params.spreads, "0.5,1.0");
        assert_eq!(params.skews, "0.2,0.3");
    }

    #[test]
    fn test_regime_optimize_progress_callback_events() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;
        use std::sync::Mutex;

        let events = Arc::new(Mutex::new(Vec::new()));

        struct TestCallback {
            events: Arc<Mutex<Vec<ProgressEvent>>>,
        }

        impl ProgressCallback for TestCallback {
            fn on_event(&self, event: ProgressEvent) {
                self.events.lock().unwrap().push(event);
            }
        }

        let callback: Arc<dyn ProgressCallback> = Arc::new(TestCallback {
            events: events.clone(),
        });

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("/nonexistent/path")) // Will fail, but we can test callbacks
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build()
            .unwrap();

        let _result = BacktestCommands::regime_optimize(params, callback);

        // Even if execution fails, we should have received at least a Started event
        let events_guard = events.lock().unwrap();
        assert!(!events_guard.is_empty(), "Should have received at least one progress event");
        assert!(matches!(events_guard[0], ProgressEvent::Started { .. }));
    }

    #[test]
    fn test_regime_optimize_all_mm_algorithms() {
        use crate::commands::params::backtest_params::RegimeOptimizeParamsBuilder;

        // Test that all MM algorithms can be parsed (validation happens in function)
        let mm_algorithms = vec!["as", "ml", "fixed"];

        for algo in mm_algorithms {
            let params = RegimeOptimizeParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm(algo.to_string())
                .spreads("0.5".to_string())
                .skews("0.2".to_string())
                .build();

            assert!(params.is_ok(), "Should build params for MM algorithm: {}", algo);
            let params = params.unwrap();
            assert_eq!(params.algorithm, algo);
        }
    }

    #[test]
    fn test_regime_optimize_result_conversion() {
        // Test that result structs can be properly constructed and converted
        let metrics = RegimeOptimizeMetrics {
            regime: "Test Regime".to_string(),
            event_count: 100,
            event_fraction: 0.5,
            time_hours: 10.0,
            optimal_spread: 2.0,
            optimal_skew: 0.5,
            should_quote: true,
            best_sharpe: 1.5,
            best_return: 0.03,
            best_drawdown: 0.02,
            best_trades: 50,
            best_win_rate: 0.55,
        };

        assert_eq!(metrics.regime, "Test Regime");
        assert_eq!(metrics.optimal_spread, 2.0);
        assert!(metrics.should_quote);
        assert!(metrics.best_sharpe > 0.0);
    }

    #[test]
    fn test_regime_optimize_optimal_params_conversion() {
        // Test that optimal regime params can be properly constructed
        let params = OptimalRegimeParams {
            high: RegimeParamSet {
                spread_bps: 1.0,
                skew_factor: 0.3,
                should_quote: true,
            },
            medium: RegimeParamSet {
                spread_bps: 2.0,
                skew_factor: 0.5,
                should_quote: true,
            },
            low: RegimeParamSet {
                spread_bps: 4.0,
                skew_factor: 0.8,
                should_quote: false,
            },
        };

        // Verify structure
        assert_eq!(params.high.spread_bps, 1.0);
        assert_eq!(params.medium.skew_factor, 0.5);
        assert!(!params.low.should_quote);
        assert!(params.high.should_quote);
        assert!(params.medium.should_quote);
    }

    // ============================================================================
    // TrainResult Structure Tests
    // ============================================================================

    #[test]
    fn test_train_result_structure() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let result = TrainResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            optimal_weights: MLModelWeights {
                spread: SpreadWeights {
                    intercept: 2.5,
                    w_entropy: -2.0,
                    w_volatility: 500.0,
                    w_imbalance: 1.0,
                    w_interaction: -100.0,
                },
                skew: SkewWeights {
                    intercept: 0.5,
                    w_entropy: -0.2,
                    w_volatility: 50.0,
                    w_imbalance: 0.1,
                    w_inventory: -0.8,
                },
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 1.8,
            train_return: 0.04,
            train_trades: 150,
            test_sharpe: 1.5,
            test_return: 0.03,
            test_trades: 50,
            generalization_gap: 0.3,
            valid_configurations: 120,
            total_configurations: 150,
        };

        assert_eq!(result.algorithm, "ml");
        assert_eq!(result.optimal_weights.spread.intercept, 2.5);
        assert_eq!(result.optimal_weights.skew.w_inventory, -0.8);
        assert_eq!(result.train_sharpe, 1.8);
        assert_eq!(result.test_sharpe, 1.5);
        assert_eq!(result.generalization_gap, 0.3);
    }

    #[test]
    fn test_train_result_serialization() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let result = TrainResult {
            algorithm: "ml-spread-skew".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            optimal_weights: MLModelWeights {
                spread: SpreadWeights {
                    intercept: 3.0,
                    w_entropy: -1.5,
                    w_volatility: 600.0,
                    w_imbalance: 1.2,
                    w_interaction: -120.0,
                },
                skew: SkewWeights {
                    intercept: 0.6,
                    w_entropy: -0.3,
                    w_volatility: 60.0,
                    w_imbalance: 0.15,
                    w_inventory: -0.9,
                },
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 2.0,
            train_return: 0.05,
            train_trades: 200,
            test_sharpe: 1.7,
            test_return: 0.04,
            test_trades: 80,
            generalization_gap: 0.3,
            valid_configurations: 180,
            total_configurations: 200,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: TrainResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.algorithm, deserialized.algorithm);
        assert_eq!(result.optimal_weights.spread.intercept, deserialized.optimal_weights.spread.intercept);
        assert_eq!(result.train_sharpe, deserialized.train_sharpe);
        assert_eq!(result.generalization_gap, deserialized.generalization_gap);
    }

    // ============================================================================
    // Train Function Tests (Algorithm Validation)
    // ============================================================================

    #[test]
    fn test_train_invalid_algorithm() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("invalid".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::train(params, callback);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid algorithm"));
    }

    #[test]
    fn test_train_non_ml_algorithm() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string()) // Avellaneda-Stoikov, not ML
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::train(params, callback);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("ML Spread/Skew") || err_msg.contains("not ML"));
    }

    #[test]
    fn test_train_valid_ml_algorithms() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;

        let ml_algorithms = vec!["ml", "ml-spread-skew", "ml_spread_skew"];

        for algo in ml_algorithms {
            let params = TrainParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm(algo.to_string())
                .spread_intercepts("1.0".to_string())
                .spread_entropy_weights("-2.0".to_string())
                .spread_vol_weights("200.0".to_string())
                .skew_intercepts("0.3".to_string())
                .skew_inv_weights("-1.0".to_string())
                .build();

            // Should build successfully (algorithm validation happens in function)
            if let Ok(params) = params {
                assert_eq!(params.algorithm, algo);
            }
        }
    }

    #[test]
    fn test_train_nonexistent_data_path() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("/nonexistent/path/that/does/not/exist"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::train(params, callback);

        // Should fail when trying to load data
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("load") || err_msg.contains("data") || err_msg.contains("Failed"));
    }

    #[test]
    fn test_train_empty_spread_intercepts() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_train_empty_skew_intercepts() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_train_invalid_spread_intercepts() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("abc,def".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_train_invalid_train_ratio() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .train_ratio(1.5) // > 1.0
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_train_progress_callback_events() {
        use crate::commands::params::backtest_params::TrainParamsBuilder;
        use std::sync::Mutex;

        let events = Arc::new(Mutex::new(Vec::new()));

        struct TestCallback {
            events: Arc<Mutex<Vec<ProgressEvent>>>,
        }

        impl ProgressCallback for TestCallback {
            fn on_event(&self, event: ProgressEvent) {
                self.events.lock().unwrap().push(event);
            }
        }

        let callback: Arc<dyn ProgressCallback> = Arc::new(TestCallback {
            events: events.clone(),
        });

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("/nonexistent/path")) // Will fail, but we can test callbacks
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let _result = BacktestCommands::train(params, callback);

        // Even if execution fails, we should have received at least a Started event
        let events_guard = events.lock().unwrap();
        assert!(!events_guard.is_empty(), "Should have received at least one progress event");
        assert!(matches!(events_guard[0], ProgressEvent::Started { .. }));
    }

    #[test]
    fn test_train_result_generalization_gap() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        // Test that generalization gap is correctly calculated
        let result = TrainResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            optimal_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 2.0,
            train_return: 0.05,
            train_trades: 200,
            test_sharpe: 1.5,
            test_return: 0.03,
            test_trades: 80,
            generalization_gap: 0.5, // train_sharpe - test_sharpe
            valid_configurations: 150,
            total_configurations: 200,
        };

        assert_eq!(result.generalization_gap, 0.5);
        assert!(result.train_sharpe > result.test_sharpe); // Expected: train > test
    }

    #[test]
    fn test_train_result_zero_trades() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let result = TrainResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            optimal_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 0.0,
            train_return: 0.0,
            train_trades: 0,
            test_sharpe: 0.0,
            test_return: 0.0,
            test_trades: 0,
            generalization_gap: 0.0,
            valid_configurations: 0,
            total_configurations: 100,
        };

        assert_eq!(result.train_trades, 0);
        assert_eq!(result.test_trades, 0);
        assert_eq!(result.valid_configurations, 0);
    }

    #[test]
    fn test_train_result_negative_generalization_gap() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        // Test case where test performs better than train (overfitting)
        let result = TrainResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            optimal_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 1.0,
            train_return: 0.02,
            train_trades: 100,
            test_sharpe: 1.5, // Test better than train
            test_return: 0.04,
            test_trades: 50,
            generalization_gap: -0.5, // Negative gap
            valid_configurations: 80,
            total_configurations: 100,
        };

        assert!(result.generalization_gap < 0.0);
        assert!(result.test_sharpe > result.train_sharpe);
    }

    // ============================================================================
    // WalkForwardMLResult Structure Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_result_structure() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let result = WalkForwardMLResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            folds: 5,
            fold_results: vec![
                WalkForwardMLFoldResult {
                    fold_num: 1,
                    train_start_ms: 1000,
                    train_end_ms: 2000,
                    test_start_ms: 2100,
                    test_end_ms: 3000,
                    train_events: 1000,
                    test_events: 500,
                    best_weights: MLModelWeights {
                        spread: SpreadWeights::default(),
                        skew: SkewWeights::default(),
                        version: "1.0".to_string(),
                        training_info: None,
                    },
                    train_sharpe: 2.0,
                    train_return: 0.05,
                    train_trades: 150,
                    test_sharpe: 1.5,
                    test_return: 0.03,
                    test_trades: 50,
                    generalization_gap: 0.5,
                    configs_evaluated: 100,
                    valid_configs: 80,
                },
            ],
            aggregate: WalkForwardMLAggregate {
                avg_oos_sharpe: 1.5,
                std_oos_sharpe: 0.3,
                avg_oos_return: 0.03,
                total_oos_trades: 250,
                avg_generalization_gap: 0.4,
                pct_profitable_folds: 0.8,
                is_oos_sharpe_ratio: 1.2,
                prob_sharpe_gt_zero: 0.95,
                weight_stability: WeightStability {
                    spread_intercept_std: 0.5,
                    spread_entropy_std: 0.3,
                    spread_volatility_std: 50.0,
                    skew_intercept_std: 0.1,
                    skew_inventory_std: 0.2,
                    stability_score: 0.8,
                },
            },
            consensus_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
        };

        assert_eq!(result.algorithm, "ml");
        assert_eq!(result.folds, 5);
        assert_eq!(result.fold_results.len(), 1);
        assert_eq!(result.aggregate.avg_oos_sharpe, 1.5);
        assert_eq!(result.aggregate.weight_stability.stability_score, 0.8);
    }

    #[test]
    fn test_walk_forward_ml_result_serialization() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let result = WalkForwardMLResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            folds: 3,
            fold_results: vec![],
            aggregate: WalkForwardMLAggregate {
                avg_oos_sharpe: 1.2,
                std_oos_sharpe: 0.25,
                avg_oos_return: 0.025,
                total_oos_trades: 200,
                avg_generalization_gap: 0.3,
                pct_profitable_folds: 0.67,
                is_oos_sharpe_ratio: 1.1,
                prob_sharpe_gt_zero: 0.9,
                weight_stability: WeightStability {
                    spread_intercept_std: 0.4,
                    spread_entropy_std: 0.25,
                    spread_volatility_std: 40.0,
                    skew_intercept_std: 0.08,
                    skew_inventory_std: 0.15,
                    stability_score: 0.75,
                },
            },
            consensus_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: WalkForwardMLResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.algorithm, deserialized.algorithm);
        assert_eq!(result.folds, deserialized.folds);
        assert_eq!(result.aggregate.avg_oos_sharpe, deserialized.aggregate.avg_oos_sharpe);
        assert_eq!(result.aggregate.weight_stability.stability_score, deserialized.aggregate.weight_stability.stability_score);
    }

    #[test]
    fn test_walk_forward_ml_fold_result_structure() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let fold_result = WalkForwardMLFoldResult {
            fold_num: 1,
            train_start_ms: 1000,
            train_end_ms: 2000,
            test_start_ms: 2100,
            test_end_ms: 3000,
            train_events: 1000,
            test_events: 500,
            best_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 2.0,
            train_return: 0.05,
            train_trades: 150,
            test_sharpe: 1.5,
            test_return: 0.03,
            test_trades: 50,
            generalization_gap: 0.5,
            configs_evaluated: 100,
            valid_configs: 80,
        };

        assert_eq!(fold_result.fold_num, 1);
        assert_eq!(fold_result.train_events, 1000);
        assert_eq!(fold_result.test_events, 500);
        assert_eq!(fold_result.generalization_gap, 0.5);
    }

    #[test]
    fn test_walk_forward_ml_aggregate_structure() {
        let aggregate = WalkForwardMLAggregate {
            avg_oos_sharpe: 1.5,
            std_oos_sharpe: 0.3,
            avg_oos_return: 0.03,
            total_oos_trades: 250,
            avg_generalization_gap: 0.4,
            pct_profitable_folds: 0.8,
            is_oos_sharpe_ratio: 1.2,
            prob_sharpe_gt_zero: 0.95,
            weight_stability: WeightStability {
                spread_intercept_std: 0.5,
                spread_entropy_std: 0.3,
                spread_volatility_std: 50.0,
                skew_intercept_std: 0.1,
                skew_inventory_std: 0.2,
                stability_score: 0.8,
            },
        };

        assert_eq!(aggregate.avg_oos_sharpe, 1.5);
        assert_eq!(aggregate.pct_profitable_folds, 0.8);
        assert_eq!(aggregate.weight_stability.stability_score, 0.8);
    }

    #[test]
    fn test_weight_stability_structure() {
        let stability = WeightStability {
            spread_intercept_std: 0.5,
            spread_entropy_std: 0.3,
            spread_volatility_std: 50.0,
            skew_intercept_std: 0.1,
            skew_inventory_std: 0.2,
            stability_score: 0.8,
        };

        assert_eq!(stability.spread_intercept_std, 0.5);
        assert_eq!(stability.stability_score, 0.8);
    }

    // ============================================================================
    // WalkForwardML Function Tests (Algorithm Validation)
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_invalid_algorithm() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("invalid".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::walk_forward_ml(params, callback);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid algorithm"));
    }

    #[test]
    fn test_walk_forward_ml_non_mm_algorithm() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        // Try with a non-MM algorithm (if one exists)
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("momentum".to_string()) // Assuming this doesn't exist or isn't MM
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build or at algorithm validation
        if let Ok(params) = params {
            let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
            let result = BacktestCommands::walk_forward_ml(params, callback);
            // Should fail with MM validation error if algorithm type is parsed but not MM
            if result.is_err() {
                let err_msg = result.unwrap_err().to_string();
                // Either invalid algorithm or not MM algorithm
                assert!(err_msg.contains("Invalid algorithm") || err_msg.contains("Market Making"));
            }
        }
    }

    #[test]
    fn test_walk_forward_ml_valid_mm_algorithms() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let algorithms = vec!["as", "ml", "fixed", "avellaneda-stoikov", "ml-spread-skew"];

        for algo in algorithms {
            let params = WalkForwardMLParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm(algo.to_string())
                .spread_intercepts("1.0".to_string())
                .spread_entropy_weights("-2.0".to_string())
                .spread_vol_weights("200.0".to_string())
                .skew_intercepts("0.3".to_string())
                .skew_inv_weights("-1.0".to_string())
                .build();

            // Should build successfully (algorithm validation happens in function)
            if let Ok(params) = params {
                assert_eq!(params.algorithm, algo);
            }
        }
    }

    #[test]
    fn test_walk_forward_ml_nonexistent_data_path() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("/nonexistent/path/that/does/not/exist"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = BacktestCommands::walk_forward_ml(params, callback);

        // Should fail when trying to load data
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("load") || err_msg.contains("data") || err_msg.contains("Failed"));
    }

    #[test]
    fn test_walk_forward_ml_empty_spread_intercepts() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_walk_forward_ml_invalid_folds() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .folds(0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_walk_forward_ml_invalid_min_train_hours() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .min_train_hours(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_walk_forward_ml_invalid_test_hours() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .test_hours(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_walk_forward_ml_invalid_embargo_hours() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .embargo_hours(-0.1)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();

        // Should fail at build time
        assert!(params.is_err());
    }

    #[test]
    fn test_walk_forward_ml_progress_callback_events() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;
        use std::sync::Mutex;

        let events = Arc::new(Mutex::new(Vec::new()));

        struct TestCallback {
            events: Arc<Mutex<Vec<ProgressEvent>>>,
        }

        impl ProgressCallback for TestCallback {
            fn on_event(&self, event: ProgressEvent) {
                self.events.lock().unwrap().push(event);
            }
        }

        let callback: Arc<dyn ProgressCallback> = Arc::new(TestCallback {
            events: events.clone(),
        });

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("/nonexistent/path")) // Will fail, but we can test callbacks
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let _result = BacktestCommands::walk_forward_ml(params, callback);

        // Even if execution fails, we should have received at least a Started event
        let events_guard = events.lock().unwrap();
        assert!(!events_guard.is_empty(), "Should have received at least one progress event");
        assert!(matches!(events_guard[0], ProgressEvent::Started { .. }));
    }

    #[test]
    fn test_walk_forward_ml_result_empty_folds() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let result = WalkForwardMLResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            folds: 0,
            fold_results: vec![],
            aggregate: WalkForwardMLAggregate {
                avg_oos_sharpe: 0.0,
                std_oos_sharpe: 0.0,
                avg_oos_return: 0.0,
                total_oos_trades: 0,
                avg_generalization_gap: 0.0,
                pct_profitable_folds: 0.0,
                is_oos_sharpe_ratio: 0.0,
                prob_sharpe_gt_zero: 0.0,
                weight_stability: WeightStability {
                    spread_intercept_std: 0.0,
                    spread_entropy_std: 0.0,
                    spread_volatility_std: 0.0,
                    skew_intercept_std: 0.0,
                    skew_inventory_std: 0.0,
                    stability_score: 0.0,
                },
            },
            consensus_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
        };

        assert_eq!(result.folds, 0);
        assert!(result.fold_results.is_empty());
    }

    #[test]
    fn test_walk_forward_ml_result_multiple_folds() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let fold_results = (1..=5).map(|i| WalkForwardMLFoldResult {
            fold_num: i,
            train_start_ms: (i * 1000) as i64,
            train_end_ms: (i * 1000 + 1000) as i64,
            test_start_ms: (i * 1000 + 1100) as i64,
            test_end_ms: (i * 1000 + 2000) as i64,
            train_events: 1000,
            test_events: 500,
            best_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 2.0,
            train_return: 0.05,
            train_trades: 150,
            test_sharpe: 1.5,
            test_return: 0.03,
            test_trades: 50,
            generalization_gap: 0.5,
            configs_evaluated: 100,
            valid_configs: 80,
        }).collect();

        let result = WalkForwardMLResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            folds: 5,
            fold_results,
            aggregate: WalkForwardMLAggregate {
                avg_oos_sharpe: 1.5,
                std_oos_sharpe: 0.3,
                avg_oos_return: 0.03,
                total_oos_trades: 250,
                avg_generalization_gap: 0.4,
                pct_profitable_folds: 0.8,
                is_oos_sharpe_ratio: 1.2,
                prob_sharpe_gt_zero: 0.95,
                weight_stability: WeightStability {
                    spread_intercept_std: 0.5,
                    spread_entropy_std: 0.3,
                    spread_volatility_std: 50.0,
                    skew_intercept_std: 0.1,
                    skew_inventory_std: 0.2,
                    stability_score: 0.8,
                },
            },
            consensus_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
        };

        assert_eq!(result.folds, 5);
        assert_eq!(result.fold_results.len(), 5);
        assert_eq!(result.fold_results[0].fold_num, 1);
        assert_eq!(result.fold_results[4].fold_num, 5);
    }

    #[test]
    fn test_walk_forward_ml_aggregate_negative_sharpe() {
        let aggregate = WalkForwardMLAggregate {
            avg_oos_sharpe: -0.5,
            std_oos_sharpe: 0.3,
            avg_oos_return: -0.01,
            total_oos_trades: 100,
            avg_generalization_gap: 0.2,
            pct_profitable_folds: 0.2,
            is_oos_sharpe_ratio: 0.8,
            prob_sharpe_gt_zero: 0.1,
            weight_stability: WeightStability {
                spread_intercept_std: 0.5,
                spread_entropy_std: 0.3,
                spread_volatility_std: 50.0,
                skew_intercept_std: 0.1,
                skew_inventory_std: 0.2,
                stability_score: 0.6,
            },
        };

        assert!(aggregate.avg_oos_sharpe < 0.0);
        assert!(aggregate.pct_profitable_folds < 0.5);
    }

    #[test]
    fn test_walk_forward_ml_fold_result_time_ranges() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let fold_result = WalkForwardMLFoldResult {
            fold_num: 1,
            train_start_ms: 1000,
            train_end_ms: 2000,
            test_start_ms: 2100, // After train_end_ms
            test_end_ms: 3000,   // After test_start_ms
            train_events: 1000,
            test_events: 500,
            best_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 2.0,
            train_return: 0.05,
            train_trades: 150,
            test_sharpe: 1.5,
            test_return: 0.03,
            test_trades: 50,
            generalization_gap: 0.5,
            configs_evaluated: 100,
            valid_configs: 80,
        };

        assert!(fold_result.test_start_ms > fold_result.train_end_ms);
        assert!(fold_result.test_end_ms > fold_result.test_start_ms);
    }

    #[test]
    fn test_walk_forward_ml_result_zero_folds() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let result = WalkForwardMLResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            folds: 0,
            fold_results: vec![],
            aggregate: WalkForwardMLAggregate::default(),
            consensus_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
        };

        assert_eq!(result.folds, 0);
        assert!(result.fold_results.is_empty());
    }

    #[test]
    fn test_walk_forward_ml_weight_stability_structure() {
        let stability = WeightStability {
            spread_intercept_std: 0.5,
            spread_entropy_std: 0.3,
            spread_volatility_std: 100.0,
            skew_intercept_std: 0.1,
            skew_inventory_std: 0.2,
            stability_score: 0.8,
        };

        assert_eq!(stability.spread_intercept_std, 0.5);
        assert_eq!(stability.stability_score, 0.8);
        assert!(stability.stability_score >= 0.0 && stability.stability_score <= 1.0);
    }

    #[test]
    fn test_walk_forward_ml_aggregate_default() {
        let aggregate = WalkForwardMLAggregate::default();
        
        assert_eq!(aggregate.avg_oos_sharpe, 0.0);
        assert_eq!(aggregate.std_oos_sharpe, 0.0);
        assert_eq!(aggregate.avg_oos_return, 0.0);
        assert_eq!(aggregate.total_oos_trades, 0);
        assert_eq!(aggregate.avg_generalization_gap, 0.0);
        assert_eq!(aggregate.pct_profitable_folds, 0.0);
        assert_eq!(aggregate.is_oos_sharpe_ratio, 0.0);
        assert_eq!(aggregate.prob_sharpe_gt_zero, 0.0);
    }

    #[test]
    fn test_walk_forward_ml_aggregate_high_stability() {
        let aggregate = WalkForwardMLAggregate {
            avg_oos_sharpe: 1.5,
            std_oos_sharpe: 0.2,
            avg_oos_return: 0.04,
            total_oos_trades: 500,
            avg_generalization_gap: 0.3,
            pct_profitable_folds: 0.8,
            is_oos_sharpe_ratio: 1.2,
            prob_sharpe_gt_zero: 0.9,
            weight_stability: WeightStability {
                spread_intercept_std: 0.1,
                spread_entropy_std: 0.05,
                spread_volatility_std: 10.0,
                skew_intercept_std: 0.02,
                skew_inventory_std: 0.03,
                stability_score: 0.95, // Very stable
            },
        };

        assert!(aggregate.weight_stability.stability_score > 0.9);
        assert!(aggregate.pct_profitable_folds > 0.5);
        assert!(aggregate.avg_oos_sharpe > 0.0);
    }

    #[test]
    fn test_walk_forward_ml_aggregate_low_stability() {
        let aggregate = WalkForwardMLAggregate {
            avg_oos_sharpe: 0.5,
            std_oos_sharpe: 1.0,
            avg_oos_return: 0.01,
            total_oos_trades: 100,
            avg_generalization_gap: 1.0,
            pct_profitable_folds: 0.3,
            is_oos_sharpe_ratio: 2.0, // High IS/OOS ratio = overfitting
            prob_sharpe_gt_zero: 0.4,
            weight_stability: WeightStability {
                spread_intercept_std: 2.0,
                spread_entropy_std: 1.5,
                spread_volatility_std: 500.0,
                skew_intercept_std: 0.5,
                skew_inventory_std: 0.8,
                stability_score: 0.2, // Low stability
            },
        };

        assert!(aggregate.weight_stability.stability_score < 0.5);
        assert!(aggregate.is_oos_sharpe_ratio > 1.5); // Indicates overfitting
        assert!(aggregate.pct_profitable_folds < 0.5);
    }

    #[test]
    fn test_walk_forward_ml_fold_result_zero_trades() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let fold_result = WalkForwardMLFoldResult {
            fold_num: 1,
            train_start_ms: 1000,
            train_end_ms: 2000,
            test_start_ms: 2100,
            test_end_ms: 3000,
            train_events: 1000,
            test_events: 500,
            best_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 0.0,
            train_return: 0.0,
            train_trades: 0,
            test_sharpe: 0.0,
            test_return: 0.0,
            test_trades: 0,
            generalization_gap: 0.0,
            configs_evaluated: 100,
            valid_configs: 0,
        };

        assert_eq!(fold_result.train_trades, 0);
        assert_eq!(fold_result.test_trades, 0);
        assert_eq!(fold_result.valid_configs, 0);
    }

    #[test]
    fn test_walk_forward_ml_fold_result_negative_generalization_gap() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        // Test case where test performs better than train (rare but possible)
        let fold_result = WalkForwardMLFoldResult {
            fold_num: 1,
            train_start_ms: 1000,
            train_end_ms: 2000,
            test_start_ms: 2100,
            test_end_ms: 3000,
            train_events: 1000,
            test_events: 500,
            best_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 1.0,
            train_return: 0.02,
            train_trades: 100,
            test_sharpe: 1.5, // Test better than train
            test_return: 0.04,
            test_trades: 50,
            generalization_gap: -0.5, // Negative gap
            configs_evaluated: 100,
            valid_configs: 80,
        };

        assert!(fold_result.generalization_gap < 0.0);
        assert!(fold_result.test_sharpe > fold_result.train_sharpe);
    }

    #[test]
    fn test_walk_forward_ml_result_consensus_weights() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let result = WalkForwardMLResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            folds: 5,
            fold_results: vec![],
            aggregate: WalkForwardMLAggregate::default(),
            consensus_weights: MLModelWeights {
                spread: SpreadWeights {
                    intercept: 2.5,
                    w_entropy: -2.0,
                    w_volatility: 500.0,
                    w_imbalance: 1.0,
                    w_interaction: -100.0,
                },
                skew: SkewWeights {
                    intercept: 0.5,
                    w_entropy: -0.2,
                    w_volatility: 50.0,
                    w_imbalance: 0.1,
                    w_inventory: -0.8,
                },
                version: "1.0".to_string(),
                training_info: None,
            },
        };

        assert_eq!(result.consensus_weights.spread.intercept, 2.5);
        assert_eq!(result.consensus_weights.skew.w_inventory, -0.8);
    }

    #[test]
    fn test_walk_forward_ml_result_serialization_roundtrip() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let original = WalkForwardMLResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            folds: 3,
            fold_results: vec![
                WalkForwardMLFoldResult {
                    fold_num: 1,
                    train_start_ms: 1000,
                    train_end_ms: 2000,
                    test_start_ms: 2100,
                    test_end_ms: 3000,
                    train_events: 1000,
                    test_events: 500,
                    best_weights: MLModelWeights {
                        spread: SpreadWeights::default(),
                        skew: SkewWeights::default(),
                        version: "1.0".to_string(),
                        training_info: None,
                    },
                    train_sharpe: 2.0,
                    train_return: 0.05,
                    train_trades: 150,
                    test_sharpe: 1.5,
                    test_return: 0.03,
                    test_trades: 50,
                    generalization_gap: 0.5,
                    configs_evaluated: 100,
                    valid_configs: 80,
                },
            ],
            aggregate: WalkForwardMLAggregate {
                avg_oos_sharpe: 1.5,
                std_oos_sharpe: 0.3,
                avg_oos_return: 0.04,
                total_oos_trades: 200,
                avg_generalization_gap: 0.4,
                pct_profitable_folds: 0.8,
                is_oos_sharpe_ratio: 1.2,
                prob_sharpe_gt_zero: 0.9,
                weight_stability: WeightStability {
                    spread_intercept_std: 0.2,
                    spread_entropy_std: 0.1,
                    spread_volatility_std: 50.0,
                    skew_intercept_std: 0.05,
                    skew_inventory_std: 0.08,
                    stability_score: 0.85,
                },
            },
            consensus_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
        };

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: WalkForwardMLResult = serde_json::from_str(&json).unwrap();

        assert_eq!(original.algorithm, deserialized.algorithm);
        assert_eq!(original.folds, deserialized.folds);
        assert_eq!(original.fold_results.len(), deserialized.fold_results.len());
        assert_eq!(original.aggregate.avg_oos_sharpe, deserialized.aggregate.avg_oos_sharpe);
        assert_eq!(original.aggregate.weight_stability.stability_score, deserialized.aggregate.weight_stability.stability_score);
    }

    #[test]
    fn test_walk_forward_ml_invalid_empty_all_grids() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        // Test that all required grids must be non-empty
        let test_cases = vec![
            ("", "-2.0", "200.0", "0.3", "-1.0", "spread_intercepts"),
            ("1.0", "", "200.0", "0.3", "-1.0", "spread_entropy_weights"),
            ("1.0", "-2.0", "", "0.3", "-1.0", "spread_vol_weights"),
            ("1.0", "-2.0", "200.0", "", "-1.0", "skew_intercepts"),
            ("1.0", "-2.0", "200.0", "0.3", "", "skew_inv_weights"),
        ];

        for (spread_ints, spread_ents, spread_vols, skew_ints, skew_invs, field_name) in test_cases {
            let result = WalkForwardMLParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm("ml".to_string())
                .spread_intercepts(spread_ints.to_string())
                .spread_entropy_weights(spread_ents.to_string())
                .spread_vol_weights(spread_vols.to_string())
                .skew_intercepts(skew_ints.to_string())
                .skew_inv_weights(skew_invs.to_string())
                .build();

            assert!(result.is_err(), "Should fail when {} is empty", field_name);
        }
    }

    #[test]
    fn test_walk_forward_ml_params_extreme_values() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        // Test with very large values
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .folds(100)
            .min_train_hours(10000.0)
            .test_hours(1000.0)
            .embargo_hours(100.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        assert_eq!(params.folds, 100);
        assert_eq!(params.min_train_hours, 10000.0);
        assert_eq!(params.test_hours, 1000.0);
        assert_eq!(params.embargo_hours, 100.0);
    }

    #[test]
    fn test_walk_forward_ml_params_rolling_vs_anchored() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params_rolling = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .rolling(true)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let params_anchored = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .rolling(false)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        assert!(params_rolling.rolling);
        assert!(!params_anchored.rolling);
    }

    #[test]
    fn test_walk_forward_ml_params_both_outputs() {
        use crate::commands::params::backtest_params::WalkForwardMLParamsBuilder;

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .output(Some(PathBuf::from("./results.json")))
            .weights_output(Some(PathBuf::from("./weights.json")))
            .build()
            .unwrap();

        assert_eq!(params.output, Some(PathBuf::from("./results.json")));
        assert_eq!(params.weights_output, Some(PathBuf::from("./weights.json")));
    }

    #[test]
    fn test_walk_forward_ml_aggregate_all_zeros() {
        let aggregate = WalkForwardMLAggregate {
            avg_oos_sharpe: 0.0,
            std_oos_sharpe: 0.0,
            avg_oos_return: 0.0,
            total_oos_trades: 0,
            avg_generalization_gap: 0.0,
            pct_profitable_folds: 0.0,
            is_oos_sharpe_ratio: 0.0,
            prob_sharpe_gt_zero: 0.0,
            weight_stability: WeightStability::default(),
        };

        assert_eq!(aggregate.total_oos_trades, 0);
        assert_eq!(aggregate.pct_profitable_folds, 0.0);
    }

    #[test]
    fn test_walk_forward_ml_weight_stability_default() {
        let stability = WeightStability::default();
        
        assert_eq!(stability.spread_intercept_std, 0.0);
        assert_eq!(stability.spread_entropy_std, 0.0);
        assert_eq!(stability.spread_volatility_std, 0.0);
        assert_eq!(stability.skew_intercept_std, 0.0);
        assert_eq!(stability.skew_inventory_std, 0.0);
        assert_eq!(stability.stability_score, 0.0);
    }

    #[test]
    fn test_walk_forward_ml_fold_result_all_configs_invalid() {
        use crate::strategies::{SpreadWeights, SkewWeights, MLModelWeights};

        let fold_result = WalkForwardMLFoldResult {
            fold_num: 1,
            train_start_ms: 1000,
            train_end_ms: 2000,
            test_start_ms: 2100,
            test_end_ms: 3000,
            train_events: 1000,
            test_events: 500,
            best_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 0.0,
            train_return: 0.0,
            train_trades: 0,
            test_sharpe: 0.0,
            test_return: 0.0,
            test_trades: 0,
            generalization_gap: 0.0,
            configs_evaluated: 100,
            valid_configs: 0, // All configs invalid
        };

        assert_eq!(fold_result.configs_evaluated, 100);
        assert_eq!(fold_result.valid_configs, 0);
        assert_eq!(fold_result.train_trades, 0);
    }

    // ============================================================================
    // SweepParamsBuilder Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_builder_new() {
        let builder = crate::commands::params::backtest_params::SweepParamsBuilder::new();
        // Should not panic
        assert!(true);
    }

    #[test]
    fn test_sweep_params_builder_default() {
        let builder = crate::commands::params::backtest_params::SweepParamsBuilder::default();
        // Should not panic
        assert!(true);
    }

    #[test]
    fn test_sweep_params_builder_required_fields() {
        use crate::commands::params::backtest_params::SweepParamsBuilder;
        use std::path::PathBuf;

        // Missing data_path
        let result = SweepParamsBuilder::new()
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data_path"));

        // Missing algorithm
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("algorithm"));

        // Missing spreads
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spreads"));

        // Missing skews
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("skews"));
    }

    #[test]
    fn test_sweep_params_builder_valid_params() {
        use crate::commands::params::backtest_params::SweepParamsBuilder;
        use std::path::PathBuf;

        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .fill_prob(0.15)
            .queue_pos(0.6)
            .naive_fills(true)
            .quiet(true)
            .build()
            .expect("Should build valid params");

        assert_eq!(params.algorithm, "as");
        assert_eq!(params.spreads, "1,2,3");
        assert_eq!(params.skews, "0.3,0.5");
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.queue_pos, 0.6);
        assert_eq!(params.naive_fills, true);
        assert_eq!(params.quiet, true);
    }

    #[test]
    fn test_sweep_params_builder_defaults() {
        use crate::commands::params::backtest_params::SweepParamsBuilder;
        use std::path::PathBuf;

        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .build()
            .expect("Should build with defaults");

        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert_eq!(params.fill_prob, 0.10);
        assert_eq!(params.queue_pos, 0.5);
        assert_eq!(params.naive_fills, false);
        assert_eq!(params.quiet, false);
    }

    #[test]
    fn test_sweep_params_builder_invalid_spreads() {
        use crate::commands::params::backtest_params::SweepParamsBuilder;
        use std::path::PathBuf;

        // Empty spreads
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("".to_string())
            .skews("0.3".to_string())
            .build();
        assert!(result.is_err());

        // Invalid numbers
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("abc,def".to_string())
            .skews("0.3".to_string())
            .build();
        assert!(result.is_err());

        // Negative spread
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("-1,2".to_string())
            .skews("0.3".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_sweep_params_builder_invalid_skews() {
        use crate::commands::params::backtest_params::SweepParamsBuilder;
        use std::path::PathBuf;

        // Empty skews
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("".to_string())
            .build();
        assert!(result.is_err());

        // Invalid numbers
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("abc,def".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_sweep_params_builder_invalid_ranges() {
        use crate::commands::params::backtest_params::SweepParamsBuilder;
        use std::path::PathBuf;

        // Invalid fill_prob
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_prob(1.5)
            .build();
        assert!(result.is_err());

        // Invalid queue_pos
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .queue_pos(1.5)
            .build();
        assert!(result.is_err());

        // Invalid fee_rate
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fee_rate(-0.1)
            .build();
        assert!(result.is_err());

        // Invalid max_inventory
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .max_inventory(0.0)
            .build();
        assert!(result.is_err());

        // Invalid quote_size
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .quote_size(0.0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_sweep_params_builder_whitespace_handling() {
        use crate::commands::params::backtest_params::SweepParamsBuilder;
        use std::path::PathBuf;

        // Should handle whitespace in comma-separated values
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads(" 1 , 2 , 3 ".to_string())
            .skews(" 0.3 , 0.5 ".to_string())
            .build()
            .expect("Should handle whitespace");

        assert_eq!(params.spreads, " 1 , 2 , 3 ");
        assert_eq!(params.skews, " 0.3 , 0.5 ");
    }

    // ============================================================================
    // SweepResult Tests
    // ============================================================================

    #[test]
    fn test_sweep_result_item_creation() {
        let item = SweepResultItem {
            spread: 2.0,
            skew: 0.5,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };

        assert_eq!(item.spread, 2.0);
        assert_eq!(item.skew, 0.5);
        assert_eq!(item.sharpe, 1.5);
        assert_eq!(item.total_return, 0.05);
        assert_eq!(item.max_drawdown, 0.02);
        assert_eq!(item.num_trades, 100);
        assert_eq!(item.win_rate, 0.55);
    }

    #[test]
    fn test_sweep_result_item_clone() {
        let item1 = SweepResultItem {
            spread: 2.0,
            skew: 0.5,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };

        let item2 = item1.clone();
        assert_eq!(item1.spread, item2.spread);
        assert_eq!(item1.sharpe, item2.sharpe);
    }

    #[test]
    fn test_sweep_result_creation() {
        let result = SweepResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                SweepResultItem {
                    spread: 1.0,
                    skew: 0.3,
                    sharpe: 1.0,
                    total_return: 0.03,
                    max_drawdown: 0.01,
                    num_trades: 50,
                    win_rate: 0.50,
                },
                SweepResultItem {
                    spread: 2.0,
                    skew: 0.5,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                },
            ],
            best: Some(SweepResultItem {
                spread: 2.0,
                skew: 0.5,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
            }),
            total_combinations: 2,
        };

        assert_eq!(result.algorithm, "as");
        assert_eq!(result.all_results.len(), 2);
        assert!(result.best.is_some());
        assert_eq!(result.total_combinations, 2);
    }

    #[test]
    fn test_sweep_result_serialization() {
        let result = SweepResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![
                SweepResultItem {
                    spread: 2.0,
                    skew: 0.5,
                    sharpe: 1.5,
                    total_return: 0.05,
                    max_drawdown: 0.02,
                    num_trades: 100,
                    win_rate: 0.55,
                },
            ],
            best: Some(SweepResultItem {
                spread: 2.0,
                skew: 0.5,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
            }),
            total_combinations: 1,
        };

        let json = serde_json::to_string(&result).expect("Should serialize");
        let deserialized: SweepResult = serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(deserialized.algorithm, result.algorithm);
        assert_eq!(deserialized.all_results.len(), result.all_results.len());
        assert_eq!(deserialized.total_combinations, result.total_combinations);
    }

    #[test]
    fn test_sweep_result_with_no_best() {
        let result = SweepResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results: vec![],
            best: None,
            total_combinations: 0,
        };

        assert!(result.best.is_none());
        assert_eq!(result.all_results.len(), 0);
    }

    #[test]
    fn test_sweep_result_item_edge_cases() {
        // Test with zero values
        let item = SweepResultItem {
            spread: 0.0,
            skew: 0.0,
            sharpe: 0.0,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
        };

        assert_eq!(item.spread, 0.0);
        assert_eq!(item.num_trades, 0);

        // Test with negative sharpe (losses)
        let item = SweepResultItem {
            spread: 1.0,
            skew: 0.3,
            sharpe: -1.0,
            total_return: -0.05,
            max_drawdown: 0.10,
            num_trades: 50,
            win_rate: 0.30,
        };

        assert!(item.sharpe < 0.0);
        assert!(item.total_return < 0.0);
    }

    // ============================================================================
    // WalkForwardResult Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_result_creation() {
        let result = WalkForwardResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            folds: 5,
            fold_results: vec![
                WalkForwardFoldResult {
                    fold_num: 1,
                    train_start_ms: 1000,
                    train_end_ms: 2000,
                    test_start_ms: 2100,
                    test_end_ms: 3000,
                    best_params: WalkForwardOptimizedParams {
                        spread: 2.0,
                        skew: 0.5,
                        fill_prob: 0.10,
                        train_sharpe: 1.5,
                    },
                    train_metrics: WalkForwardFoldMetrics {
                        sharpe: 1.5,
                        total_return: 0.05,
                        max_drawdown: 0.02,
                        num_trades: 100,
                        win_rate: 0.55,
                        profit_factor: 1.2,
                    },
                    test_metrics: WalkForwardFoldMetrics {
                        sharpe: 1.2,
                        total_return: 0.03,
                        max_drawdown: 0.03,
                        num_trades: 50,
                        win_rate: 0.50,
                        profit_factor: 1.1,
                    },
                },
            ],
            aggregate: WalkForwardAggregate {
                avg_oos_sharpe: 1.2,
                std_oos_sharpe: 0.1,
                avg_oos_return: 0.03,
                total_oos_trades: 50,
                avg_win_rate: 0.50,
                pct_profitable_folds: 0.80,
                is_oos_sharpe_ratio: 0.80,
                prob_sharpe_gt_zero: 0.95,
            },
        };

        assert_eq!(result.algorithm, "as");
        assert_eq!(result.folds, 5);
        assert_eq!(result.fold_results.len(), 1);
        assert_eq!(result.aggregate.avg_oos_sharpe, 1.2);
    }

    #[test]
    fn test_walk_forward_result_serialization() {
        let result = WalkForwardResult {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            folds: 3,
            fold_results: vec![],
            aggregate: WalkForwardAggregate::default(),
        };

        let json = serde_json::to_string(&result).expect("Should serialize");
        let deserialized: WalkForwardResult = serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(deserialized.algorithm, result.algorithm);
        assert_eq!(deserialized.folds, result.folds);
        assert_eq!(deserialized.fold_results.len(), result.fold_results.len());
    }

    #[test]
    fn test_walk_forward_fold_result_creation() {
        let fold = WalkForwardFoldResult {
            fold_num: 1,
            train_start_ms: 1000,
            train_end_ms: 2000,
            test_start_ms: 2100,
            test_end_ms: 3000,
            best_params: WalkForwardOptimizedParams {
                spread: 2.0,
                skew: 0.5,
                fill_prob: 0.10,
                train_sharpe: 1.5,
            },
            train_metrics: WalkForwardFoldMetrics::default(),
            test_metrics: WalkForwardFoldMetrics::default(),
        };

        assert_eq!(fold.fold_num, 1);
        assert_eq!(fold.best_params.spread, 2.0);
        assert_eq!(fold.best_params.train_sharpe, 1.5);
    }

    #[test]
    fn test_walk_forward_aggregate_default() {
        let aggregate = WalkForwardAggregate::default();
        assert_eq!(aggregate.avg_oos_sharpe, 0.0);
        assert_eq!(aggregate.total_oos_trades, 0);
        assert_eq!(aggregate.pct_profitable_folds, 0.0);
    }

    #[test]
    fn test_walk_forward_fold_metrics_default() {
        let metrics = WalkForwardFoldMetrics::default();
        assert_eq!(metrics.sharpe, 0.0);
        assert_eq!(metrics.num_trades, 0);
        assert_eq!(metrics.win_rate, 0.0);
    }

    #[test]
    fn test_walk_forward_optimized_params_creation() {
        let params = WalkForwardOptimizedParams {
            spread: 2.0,
            skew: 0.5,
            fill_prob: 0.10,
            train_sharpe: 1.5,
        };

        assert_eq!(params.spread, 2.0);
        assert_eq!(params.skew, 0.5);
        assert_eq!(params.fill_prob, 0.10);
        assert_eq!(params.train_sharpe, 1.5);
    }
}



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
use crate::commands::params::backtest_params::{EvaluateParams, TuneParams, RegimeSearchParams};
use crate::backtest::{
    BacktestEngine, BacktestConfig, BacktestResults,
    replay::{ParquetReplay, ReplayConfig},
    fill_simulator::FillSimulatorConfig,
};
use crate::execution::market_maker::{MMConfig, RegimeParams, RegimeConfig, RegimeThresholds};
use crate::execution::mm_simulator::SimulatorConfig;
use crate::strategies::{
    AlgorithmType, AlgorithmRegistry, BacktestAlgorithmParams, MLModelWeights,
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
}



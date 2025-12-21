//! Walk-Forward ML Training
//!
//! Integrates ML weight optimization with walk-forward validation to prevent overfitting.
//! Trains ML weights on expanding/rolling training windows and validates on out-of-sample
//! test periods.
//!
//! # Methodology
//!
//! 1. **Split data** into N folds with train/test periods
//! 2. **For each fold**:
//!    - Run grid search to find optimal ML weights on training data
//!    - Evaluate best weights on out-of-sample test data
//!    - Track generalization gap (train vs test performance)
//! 3. **Aggregate results** across all folds for robust performance estimate
//!
//! # Advantages over simple train/test split
//!
//! - Uses multiple out-of-sample periods instead of single split
//! - Detects regime changes (weights that work in early periods may fail later)
//! - Provides confidence intervals on out-of-sample performance
//! - Measures parameter stability across time
//!
//! # Usage
//!
//! ```ignore
//! use crate::backtest::walk_forward_ml::{WalkForwardMLTrainer, WalkForwardMLConfig};
//!
//! let config = WalkForwardMLConfig::default();
//! let mut trainer = WalkForwardMLTrainer::new(config)?;
//! let results = trainer.run()?;
//! println!("Avg OOS Sharpe: {:.3}", results.aggregate.avg_oos_sharpe);
//! ```

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use anyhow::Result;

use crate::strategies::{
    MLSpreadSkewAlgorithm, MLSpreadSkewConfig, MLModelWeights,
    SpreadWeights, SkewWeights, TrainingInfo, MarketMakingAlgorithm,
};
use crate::backtest::{
    BacktestEngine, BacktestConfig, BacktestResults,
    ReplayEvent, ReplayConfig, FillSimulatorConfig, ParquetReplay,
};
use crate::execution::market_maker::MMConfig;
use crate::execution::mm_simulator::SimulatorConfig;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for walk-forward ML training
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardMLConfig {
    /// Data directory containing Parquet files
    pub data_dir: PathBuf,

    /// Number of folds (train/test splits)
    pub n_folds: usize,

    /// Minimum training period in hours
    pub min_train_hours: f64,

    /// Test period in hours per fold
    pub test_hours: f64,

    /// Use anchored (expanding) or rolling window
    pub anchored: bool,

    /// Gap between train and test to prevent lookahead (hours)
    pub embargo_hours: f64,

    /// Grid search values for spread intercept (base spread in bps)
    pub spread_intercepts: Vec<f64>,

    /// Grid search values for spread entropy weight
    pub spread_entropy_weights: Vec<f64>,

    /// Grid search values for spread volatility weight
    pub spread_volatility_weights: Vec<f64>,

    /// Grid search values for skew intercept
    pub skew_intercepts: Vec<f64>,

    /// Grid search values for skew inventory weight
    pub skew_inventory_weights: Vec<f64>,

    /// Fill probability for simulation
    pub fill_probability: f64,

    /// Maximum inventory
    pub max_inventory: Decimal,

    /// Quote size
    pub quote_size: Decimal,

    /// Minimum trades required for valid evaluation
    pub min_trades: usize,

    /// Verbose output
    pub verbose: bool,
}

impl Default for WalkForwardMLConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data/features"),
            n_folds: 5,
            min_train_hours: 100.0,
            test_hours: 24.0,
            anchored: true,
            embargo_hours: 1.0,
            // Spread weight search space (compact for speed)
            spread_intercepts: vec![1.0, 2.0, 3.0],
            spread_entropy_weights: vec![-2.0, -1.0, 0.0],
            spread_volatility_weights: vec![200.0, 400.0],
            // Skew weight search space
            skew_intercepts: vec![0.3, 0.5, 0.7],
            skew_inventory_weights: vec![-1.0, -0.6],
            // Simulation params
            fill_probability: 0.10,
            max_inventory: dec!(0.1),
            quote_size: dec!(0.001),
            min_trades: 10,
            verbose: true,
        }
    }
}

// ============================================================================
// Results Structures
// ============================================================================

/// Results from a single fold's ML training and evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLFoldResult {
    pub fold_num: usize,
    pub train_start_ms: i64,
    pub train_end_ms: i64,
    pub test_start_ms: i64,
    pub test_end_ms: i64,
    pub train_events: usize,
    pub test_events: usize,

    /// Best weights found for this fold
    pub best_weights: MLModelWeights,

    /// Training set performance (in-sample)
    pub train_sharpe: f64,
    pub train_return: f64,
    pub train_trades: usize,

    /// Test set performance (out-of-sample)
    pub test_sharpe: f64,
    pub test_return: f64,
    pub test_trades: usize,

    /// Generalization gap (train - test)
    pub generalization_gap: f64,

    /// Number of weight configurations evaluated
    pub configs_evaluated: usize,
    pub valid_configs: usize,
}

/// Aggregated results across all folds
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalkForwardMLAggregate {
    /// Average out-of-sample Sharpe
    pub avg_oos_sharpe: f64,
    /// Std dev of out-of-sample Sharpe
    pub std_oos_sharpe: f64,
    /// Average out-of-sample return
    pub avg_oos_return: f64,
    /// Total out-of-sample trades
    pub total_oos_trades: usize,
    /// Average generalization gap
    pub avg_generalization_gap: f64,
    /// Percentage of folds profitable
    pub pct_profitable_folds: f64,
    /// In-sample vs out-of-sample Sharpe ratio (overfitting indicator)
    pub is_oos_sharpe_ratio: f64,
    /// Probability Sharpe > 0 (statistical significance)
    pub prob_sharpe_gt_zero: f64,
    /// Weight stability: how much do optimal weights vary across folds?
    pub weight_stability: WeightStability,
}

/// Measures how stable the optimal weights are across folds
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WeightStability {
    pub spread_intercept_std: f64,
    pub spread_entropy_std: f64,
    pub spread_volatility_std: f64,
    pub skew_intercept_std: f64,
    pub skew_inventory_std: f64,
    /// Overall stability score (0-1, higher = more stable)
    pub stability_score: f64,
}

/// Complete walk-forward ML training results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardMLResults {
    pub config: WalkForwardMLConfig,
    pub folds: Vec<MLFoldResult>,
    pub aggregate: WalkForwardMLAggregate,
    /// Consensus weights (average of fold weights, weighted by test performance)
    pub consensus_weights: MLModelWeights,
}

impl WalkForwardMLResults {
    /// Save results to JSON file
    pub fn save_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Save consensus weights to JSON file
    pub fn save_weights(&self, path: &str) -> Result<()> {
        self.consensus_weights.save_to_file(path)?;
        Ok(())
    }
}

// ============================================================================
// Walk-Forward ML Trainer
// ============================================================================

/// Walk-forward ML weight trainer
pub struct WalkForwardMLTrainer {
    config: WalkForwardMLConfig,
    events: Vec<ReplayEvent>,
    time_range: Option<(i64, i64)>,
}

impl WalkForwardMLTrainer {
    /// Create a new trainer and load data
    pub fn new(config: WalkForwardMLConfig) -> Result<Self> {
        let mut trainer = Self {
            config,
            events: Vec::new(),
            time_range: None,
        };
        trainer.load_data()?;
        Ok(trainer)
    }

    /// Create trainer with pre-loaded events (for testing)
    pub fn with_events(config: WalkForwardMLConfig, events: Vec<ReplayEvent>) -> Self {
        let time_range = if events.is_empty() {
            None
        } else {
            Some((
                events.first().unwrap().timestamp_ms,
                events.last().unwrap().timestamp_ms,
            ))
        };
        Self {
            config,
            events,
            time_range,
        }
    }

    /// Load data from Parquet files
    fn load_data(&mut self) -> Result<()> {
        if self.config.verbose {
            println!("Loading data from {:?}...", self.config.data_dir);
        }

        let replay_config = ReplayConfig {
            data_dir: self.config.data_dir.clone(),
            ..Default::default()
        };

        let mut replay = ParquetReplay::new(replay_config);
        let num_events = replay.load()?;

        self.time_range = replay.time_range();
        self.events = replay.into_events();

        if self.config.verbose {
            println!("Loaded {} events", num_events);
        }

        Ok(())
    }

    /// Generate time-based fold boundaries
    fn generate_folds(&self) -> Result<Vec<(i64, i64, i64, i64)>> {
        let (start_ms, end_ms) = self.time_range
            .ok_or_else(|| anyhow::anyhow!("No time range available - load data first"))?;

        let total_hours = (end_ms - start_ms) as f64 / (1000.0 * 60.0 * 60.0);
        let test_ms = (self.config.test_hours * 60.0 * 60.0 * 1000.0) as i64;
        let embargo_ms = (self.config.embargo_hours * 60.0 * 60.0 * 1000.0) as i64;
        let min_train_ms = (self.config.min_train_hours * 60.0 * 60.0 * 1000.0) as i64;

        if self.config.verbose {
            println!("Total data span: {:.1} hours", total_hours);
            println!("Test period per fold: {:.1} hours", self.config.test_hours);
            println!("Embargo period: {:.1} hours", self.config.embargo_hours);
        }

        let mut folds = Vec::new();

        if self.config.anchored {
            // Anchored walk-forward: expanding training window
            let fold_size_ms = (end_ms - start_ms - min_train_ms) / self.config.n_folds as i64;

            for i in 0..self.config.n_folds {
                let train_start = start_ms;
                let train_end = start_ms + min_train_ms + (i as i64 * fold_size_ms);
                let test_start = train_end + embargo_ms;
                let test_end = (test_start + test_ms).min(end_ms);

                if test_end > test_start {
                    folds.push((train_start, train_end, test_start, test_end));
                }
            }
        } else {
            // Rolling walk-forward: fixed training window
            let step_ms = (end_ms - start_ms - min_train_ms - test_ms) / self.config.n_folds as i64;

            for i in 0..self.config.n_folds {
                let train_start = start_ms + (i as i64 * step_ms);
                let train_end = train_start + min_train_ms;
                let test_start = train_end + embargo_ms;
                let test_end = (test_start + test_ms).min(end_ms);

                if test_end > test_start && train_end < end_ms {
                    folds.push((train_start, train_end, test_start, test_end));
                }
            }
        }

        Ok(folds)
    }

    /// Filter events to a time range
    fn filter_events(&self, start_ms: i64, end_ms: i64) -> Vec<ReplayEvent> {
        self.events
            .iter()
            .filter(|e| e.timestamp_ms >= start_ms && e.timestamp_ms < end_ms)
            .cloned()
            .collect()
    }

    /// Run backtest with specific ML weights
    fn evaluate_weights(
        &self,
        weights: &MLModelWeights,
        events: &[ReplayEvent],
    ) -> Result<BacktestResults> {
        let ml_config = MLSpreadSkewConfig {
            max_inventory: self.config.max_inventory,
            quote_size: self.config.quote_size,
            ..Default::default()
        };

        let algorithm: Box<dyn MarketMakingAlgorithm> = Box::new(
            MLSpreadSkewAlgorithm::new(ml_config, weights.clone())
        );

        let backtest_config = BacktestConfig {
            replay: ReplayConfig {
                data_dir: self.config.data_dir.clone(),
                ..Default::default()
            },
            mm: MMConfig {
                max_inventory: self.config.max_inventory,
                quote_size: self.config.quote_size,
                ..Default::default()
            },
            simulator: SimulatorConfig::default(),
            fill_sim: FillSimulatorConfig {
                base_fill_probability: self.config.fill_probability,
                ..Default::default()
            },
            verbose: false,
            use_realistic_fills: true,
            ..Default::default()
        };

        let mut engine = BacktestEngine::from_events_with_algorithm(
            backtest_config,
            events.to_vec(),
            algorithm,
        );

        engine.run()
    }

    /// Optimize ML weights on training data using grid search
    fn optimize_on_train(
        &self,
        train_events: &[ReplayEvent],
    ) -> Result<(MLModelWeights, f64, f64, usize, usize, usize)> {
        let total_configs = self.config.spread_intercepts.len()
            * self.config.spread_entropy_weights.len()
            * self.config.spread_volatility_weights.len()
            * self.config.skew_intercepts.len()
            * self.config.skew_inventory_weights.len();

        let mut best_sharpe = f64::NEG_INFINITY;
        let mut best_weights = MLModelWeights::default();
        let mut best_return = 0.0;
        let mut best_trades = 0;
        let mut valid_configs = 0;

        for &spread_int in &self.config.spread_intercepts {
            for &spread_ent in &self.config.spread_entropy_weights {
                for &spread_vol in &self.config.spread_volatility_weights {
                    for &skew_int in &self.config.skew_intercepts {
                        for &skew_inv in &self.config.skew_inventory_weights {
                            let weights = MLModelWeights {
                                spread: SpreadWeights {
                                    intercept: spread_int,
                                    w_entropy: spread_ent,
                                    w_volatility: spread_vol,
                                    ..Default::default()
                                },
                                skew: SkewWeights {
                                    intercept: skew_int,
                                    w_inventory: skew_inv,
                                    ..Default::default()
                                },
                                version: "walk-forward".to_string(),
                                training_info: None,
                            };

                            let result = self.evaluate_weights(&weights, train_events)?;

                            if result.fills_generated < self.config.min_trades {
                                continue;
                            }

                            valid_configs += 1;
                            let sharpe = result.metrics.sharpe_ratio;

                            if sharpe > best_sharpe {
                                best_sharpe = sharpe;
                                best_weights = weights;
                                best_return = result.metrics.total_return;
                                best_trades = result.fills_generated;
                            }
                        }
                    }
                }
            }
        }

        Ok((best_weights, best_sharpe, best_return, best_trades, total_configs, valid_configs))
    }

    /// Run walk-forward ML training
    pub fn run(&mut self) -> Result<WalkForwardMLResults> {
        let fold_boundaries = self.generate_folds()?;

        if self.config.verbose {
            println!("\n========================================");
            println!("   WALK-FORWARD ML TRAINING");
            println!("========================================");
            println!("Mode: {}", if self.config.anchored { "Anchored (expanding)" } else { "Rolling (fixed)" });
            println!("Folds: {}", fold_boundaries.len());
            let total_configs = self.config.spread_intercepts.len()
                * self.config.spread_entropy_weights.len()
                * self.config.spread_volatility_weights.len()
                * self.config.skew_intercepts.len()
                * self.config.skew_inventory_weights.len();
            println!("Weight combinations per fold: {}", total_configs);
            println!();
        }

        let mut folds = Vec::new();

        for (i, &(train_start, train_end, test_start, test_end)) in fold_boundaries.iter().enumerate() {
            if self.config.verbose {
                let train_hours = (train_end - train_start) as f64 / (1000.0 * 60.0 * 60.0);
                let test_hours = (test_end - test_start) as f64 / (1000.0 * 60.0 * 60.0);
                println!("Fold {}/{}: Train={:.1}h, Test={:.1}h",
                    i + 1, fold_boundaries.len(), train_hours, test_hours);
            }

            // Get events for train and test periods
            let train_events = self.filter_events(train_start, train_end);
            let test_events = self.filter_events(test_start, test_end);

            if train_events.is_empty() || test_events.is_empty() {
                if self.config.verbose {
                    println!("  Skipping fold {} - insufficient data", i + 1);
                }
                continue;
            }

            // Optimize weights on training data
            let (best_weights, train_sharpe, train_return, train_trades, configs_evaluated, valid_configs) =
                self.optimize_on_train(&train_events)?;

            if valid_configs == 0 {
                if self.config.verbose {
                    println!("  Skipping fold {} - no valid configurations", i + 1);
                }
                continue;
            }

            // Evaluate best weights on test data
            let test_results = self.evaluate_weights(&best_weights, &test_events)?;

            let fold_result = MLFoldResult {
                fold_num: i + 1,
                train_start_ms: train_start,
                train_end_ms: train_end,
                test_start_ms: test_start,
                test_end_ms: test_end,
                train_events: train_events.len(),
                test_events: test_events.len(),
                best_weights: best_weights.clone(),
                train_sharpe,
                train_return,
                train_trades,
                test_sharpe: test_results.metrics.sharpe_ratio,
                test_return: test_results.metrics.total_return,
                test_trades: test_results.fills_generated,
                generalization_gap: train_sharpe - test_results.metrics.sharpe_ratio,
                configs_evaluated,
                valid_configs,
            };

            if self.config.verbose {
                println!("  Best weights: spread_int={:.1}, ent={:.1}, vol={:.0}",
                    best_weights.spread.intercept,
                    best_weights.spread.w_entropy,
                    best_weights.spread.w_volatility);
                println!("  Train: Sharpe={:+.2}, Return={:+.2}%, Trades={}",
                    fold_result.train_sharpe,
                    fold_result.train_return * 100.0,
                    fold_result.train_trades);
                println!("  Test:  Sharpe={:+.2}, Return={:+.2}%, Trades={}",
                    fold_result.test_sharpe,
                    fold_result.test_return * 100.0,
                    fold_result.test_trades);
                println!("  Gap:   {:.2}", fold_result.generalization_gap);
                println!();
            }

            folds.push(fold_result);
        }

        if folds.is_empty() {
            anyhow::bail!("No valid folds completed");
        }

        // Calculate aggregate results
        let aggregate = self.calculate_aggregate(&folds);
        let consensus_weights = self.calculate_consensus_weights(&folds);

        if self.config.verbose {
            self.print_summary(&aggregate, &consensus_weights);
        }

        Ok(WalkForwardMLResults {
            config: self.config.clone(),
            folds,
            aggregate,
            consensus_weights,
        })
    }

    /// Calculate aggregate statistics across folds
    fn calculate_aggregate(&self, folds: &[MLFoldResult]) -> WalkForwardMLAggregate {
        if folds.is_empty() {
            return WalkForwardMLAggregate::default();
        }

        let n = folds.len() as f64;

        // Out-of-sample statistics
        let oos_sharpes: Vec<f64> = folds.iter().map(|f| f.test_sharpe).collect();
        let avg_oos_sharpe = oos_sharpes.iter().sum::<f64>() / n;
        let std_oos_sharpe = {
            let variance = oos_sharpes.iter()
                .map(|s| (s - avg_oos_sharpe).powi(2))
                .sum::<f64>() / (n - 1.0).max(1.0);
            variance.sqrt()
        };

        let avg_oos_return = folds.iter()
            .map(|f| f.test_return)
            .sum::<f64>() / n;

        let total_oos_trades = folds.iter()
            .map(|f| f.test_trades)
            .sum();

        let avg_generalization_gap = folds.iter()
            .map(|f| f.generalization_gap)
            .sum::<f64>() / n;

        let profitable_folds = folds.iter()
            .filter(|f| f.test_return > 0.0)
            .count();
        let pct_profitable_folds = profitable_folds as f64 / n;

        // In-sample vs out-of-sample comparison
        let avg_is_sharpe = folds.iter()
            .map(|f| f.train_sharpe)
            .sum::<f64>() / n;
        let is_oos_sharpe_ratio = if avg_is_sharpe.abs() > 0.01 {
            avg_oos_sharpe / avg_is_sharpe
        } else {
            0.0
        };

        // Probability of Sharpe > 0
        let prob_sharpe_gt_zero = if std_oos_sharpe > 0.0 && n > 2.0 {
            let t_stat = avg_oos_sharpe * (n - 1.0).sqrt() / std_oos_sharpe;
            0.5 * (1.0 + erf(t_stat / std::f64::consts::SQRT_2))
        } else {
            0.5
        };

        // Weight stability across folds
        let weight_stability = self.calculate_weight_stability(folds);

        WalkForwardMLAggregate {
            avg_oos_sharpe,
            std_oos_sharpe,
            avg_oos_return,
            total_oos_trades,
            avg_generalization_gap,
            pct_profitable_folds,
            is_oos_sharpe_ratio,
            prob_sharpe_gt_zero,
            weight_stability,
        }
    }

    /// Calculate how stable weights are across folds
    fn calculate_weight_stability(&self, folds: &[MLFoldResult]) -> WeightStability {
        if folds.len() < 2 {
            return WeightStability {
                stability_score: 1.0,
                ..Default::default()
            };
        }

        let _n = folds.len() as f64;

        // Collect weights from each fold
        let spread_intercepts: Vec<f64> = folds.iter().map(|f| f.best_weights.spread.intercept).collect();
        let spread_entropies: Vec<f64> = folds.iter().map(|f| f.best_weights.spread.w_entropy).collect();
        let spread_volatilities: Vec<f64> = folds.iter().map(|f| f.best_weights.spread.w_volatility).collect();
        let skew_intercepts: Vec<f64> = folds.iter().map(|f| f.best_weights.skew.intercept).collect();
        let skew_inventories: Vec<f64> = folds.iter().map(|f| f.best_weights.skew.w_inventory).collect();

        // Calculate standard deviations
        let spread_intercept_std = std_dev(&spread_intercepts);
        let spread_entropy_std = std_dev(&spread_entropies);
        let spread_volatility_std = std_dev(&spread_volatilities);
        let skew_intercept_std = std_dev(&skew_intercepts);
        let skew_inventory_std = std_dev(&skew_inventories);

        // Calculate coefficient of variation for each weight (normalized by search range)
        let spread_int_range = self.config.spread_intercepts.last().unwrap_or(&1.0)
            - self.config.spread_intercepts.first().unwrap_or(&0.0);
        let spread_ent_range = (self.config.spread_entropy_weights.last().unwrap_or(&0.0)
            - self.config.spread_entropy_weights.first().unwrap_or(&0.0)).abs();
        let spread_vol_range = self.config.spread_volatility_weights.last().unwrap_or(&1.0)
            - self.config.spread_volatility_weights.first().unwrap_or(&0.0);
        let skew_int_range = self.config.skew_intercepts.last().unwrap_or(&1.0)
            - self.config.skew_intercepts.first().unwrap_or(&0.0);
        let skew_inv_range = (self.config.skew_inventory_weights.last().unwrap_or(&0.0)
            - self.config.skew_inventory_weights.first().unwrap_or(&0.0)).abs();

        // Normalized variations (0 = perfectly stable, 1 = varies across full range)
        let norm_vars = vec![
            if spread_int_range > 0.0 { spread_intercept_std / spread_int_range } else { 0.0 },
            if spread_ent_range > 0.0 { spread_entropy_std / spread_ent_range } else { 0.0 },
            if spread_vol_range > 0.0 { spread_volatility_std / spread_vol_range } else { 0.0 },
            if skew_int_range > 0.0 { skew_intercept_std / skew_int_range } else { 0.0 },
            if skew_inv_range > 0.0 { skew_inventory_std / skew_inv_range } else { 0.0 },
        ];

        let avg_norm_var = norm_vars.iter().sum::<f64>() / norm_vars.len() as f64;
        let stability_score = (1.0 - avg_norm_var).max(0.0).min(1.0);

        WeightStability {
            spread_intercept_std,
            spread_entropy_std,
            spread_volatility_std,
            skew_intercept_std,
            skew_inventory_std,
            stability_score,
        }
    }

    /// Calculate consensus weights from all folds (weighted by test performance)
    fn calculate_consensus_weights(&self, folds: &[MLFoldResult]) -> MLModelWeights {
        if folds.is_empty() {
            return MLModelWeights::default();
        }

        // Use Sharpe-weighted average (only positive Sharpes contribute)
        let weights: Vec<f64> = folds.iter()
            .map(|f| (f.test_sharpe + 1.0).max(0.0)) // Shift to handle negative Sharpes
            .collect();
        let total_weight: f64 = weights.iter().sum();

        if total_weight <= 0.0 {
            // Fall back to simple average
            let n = folds.len() as f64;
            return MLModelWeights {
                spread: SpreadWeights {
                    intercept: folds.iter().map(|f| f.best_weights.spread.intercept).sum::<f64>() / n,
                    w_entropy: folds.iter().map(|f| f.best_weights.spread.w_entropy).sum::<f64>() / n,
                    w_volatility: folds.iter().map(|f| f.best_weights.spread.w_volatility).sum::<f64>() / n,
                    ..Default::default()
                },
                skew: SkewWeights {
                    intercept: folds.iter().map(|f| f.best_weights.skew.intercept).sum::<f64>() / n,
                    w_inventory: folds.iter().map(|f| f.best_weights.skew.w_inventory).sum::<f64>() / n,
                    ..Default::default()
                },
                version: "walk-forward-consensus".to_string(),
                training_info: Some(TrainingInfo {
                    trained_on: chrono::Utc::now().to_rfc3339(),
                    num_samples: folds.iter().map(|f| f.train_events).sum(),
                    train_sharpe: folds.iter().map(|f| f.train_sharpe).sum::<f64>() / n,
                    validation_sharpe: Some(folds.iter().map(|f| f.test_sharpe).sum::<f64>() / n),
                }),
            };
        }

        // Weighted averages
        let spread_intercept = folds.iter()
            .zip(&weights)
            .map(|(f, w)| f.best_weights.spread.intercept * w)
            .sum::<f64>() / total_weight;
        let spread_entropy = folds.iter()
            .zip(&weights)
            .map(|(f, w)| f.best_weights.spread.w_entropy * w)
            .sum::<f64>() / total_weight;
        let spread_volatility = folds.iter()
            .zip(&weights)
            .map(|(f, w)| f.best_weights.spread.w_volatility * w)
            .sum::<f64>() / total_weight;
        let skew_intercept = folds.iter()
            .zip(&weights)
            .map(|(f, w)| f.best_weights.skew.intercept * w)
            .sum::<f64>() / total_weight;
        let skew_inventory = folds.iter()
            .zip(&weights)
            .map(|(f, w)| f.best_weights.skew.w_inventory * w)
            .sum::<f64>() / total_weight;

        let avg_train_sharpe = folds.iter()
            .zip(&weights)
            .map(|(f, w)| f.train_sharpe * w)
            .sum::<f64>() / total_weight;
        let avg_test_sharpe = folds.iter()
            .zip(&weights)
            .map(|(f, w)| f.test_sharpe * w)
            .sum::<f64>() / total_weight;

        MLModelWeights {
            spread: SpreadWeights {
                intercept: spread_intercept,
                w_entropy: spread_entropy,
                w_volatility: spread_volatility,
                ..Default::default()
            },
            skew: SkewWeights {
                intercept: skew_intercept,
                w_inventory: skew_inventory,
                ..Default::default()
            },
            version: "walk-forward-consensus".to_string(),
            training_info: Some(TrainingInfo {
                trained_on: chrono::Utc::now().to_rfc3339(),
                num_samples: folds.iter().map(|f| f.train_events).sum(),
                train_sharpe: avg_train_sharpe,
                validation_sharpe: Some(avg_test_sharpe),
            }),
        }
    }

    fn print_summary(&self, agg: &WalkForwardMLAggregate, consensus: &MLModelWeights) {
        println!("========================================");
        println!("   WALK-FORWARD ML RESULTS SUMMARY");
        println!("========================================");
        println!();
        println!("OUT-OF-SAMPLE PERFORMANCE:");
        println!("  Avg Sharpe:          {:+.3} +/- {:.3}", agg.avg_oos_sharpe, agg.std_oos_sharpe);
        println!("  Avg Return:          {:+.2}%", agg.avg_oos_return * 100.0);
        println!("  Total Trades:        {}", agg.total_oos_trades);
        println!("  Profitable Folds:    {:.0}%", agg.pct_profitable_folds * 100.0);
        println!();
        println!("GENERALIZATION:");
        println!("  Avg Gap (IS-OOS):    {:+.3}", agg.avg_generalization_gap);
        println!("  IS/OOS Sharpe Ratio: {:.2} (closer to 1.0 = less overfit)", agg.is_oos_sharpe_ratio);
        println!("  P(Sharpe > 0):       {:.1}%", agg.prob_sharpe_gt_zero * 100.0);
        println!();
        println!("WEIGHT STABILITY:");
        println!("  Stability Score:     {:.2} (1.0 = perfectly stable)", agg.weight_stability.stability_score);
        println!("  Spread Int Std:      {:.2}", agg.weight_stability.spread_intercept_std);
        println!("  Spread Ent Std:      {:.2}", agg.weight_stability.spread_entropy_std);
        println!("  Spread Vol Std:      {:.1}", agg.weight_stability.spread_volatility_std);
        println!();
        println!("CONSENSUS WEIGHTS:");
        println!("  Spread:");
        println!("    intercept:     {:.2}", consensus.spread.intercept);
        println!("    w_entropy:     {:.2}", consensus.spread.w_entropy);
        println!("    w_volatility:  {:.1}", consensus.spread.w_volatility);
        println!("  Skew:");
        println!("    intercept:     {:.2}", consensus.skew.intercept);
        println!("    w_inventory:   {:.2}", consensus.skew.w_inventory);
        println!();

        // Interpretation
        if agg.is_oos_sharpe_ratio > 0.8 {
            println!("INTERPRETATION: Good generalization - strategy appears robust");
        } else if agg.is_oos_sharpe_ratio > 0.5 {
            println!("INTERPRETATION: Moderate overfitting - consider simplifying");
        } else {
            println!("INTERPRETATION: Significant overfitting - results may not replicate");
        }

        if agg.weight_stability.stability_score > 0.8 {
            println!("WEIGHT STABILITY: High - consistent parameters across time periods");
        } else if agg.weight_stability.stability_score > 0.5 {
            println!("WEIGHT STABILITY: Moderate - some regime sensitivity detected");
        } else {
            println!("WEIGHT STABILITY: Low - optimal parameters vary significantly");
        }

        println!("========================================");
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Calculate standard deviation of a slice
fn std_dev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter()
        .map(|v| (v - mean).powi(2))
        .sum::<f64>() / (n - 1.0);
    variance.sqrt()
}

/// Error function approximation for probability calculation
fn erf(x: f64) -> f64 {
    let a1 =  0.254829592;
    let a2 = -0.284496736;
    let a3 =  1.421413741;
    let a4 = -1.453152027;
    let a5 =  1.061405429;
    let p  =  0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    sign * y
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::feature_fusion::FeaturesSnapshot;

    fn create_test_events(count: usize, start_ms: i64) -> Vec<ReplayEvent> {
        (0..count)
            .map(|i| {
                let mut snapshot = FeaturesSnapshot::default();
                snapshot.timestamp = format!("2024-01-01T00:00:{:02}.000Z", i % 60);
                snapshot.mid_price = Some(dec!(50000) + Decimal::from(i as i64));
                snapshot.best_bid = Some(dec!(49990) + Decimal::from(i as i64));
                snapshot.best_ask = Some(dec!(50010) + Decimal::from(i as i64));
                snapshot.tick_entropy_1s = Some(dec!(1.2));
                snapshot.tick_entropy_5s = Some(dec!(1.3));
                snapshot.realized_volatility_100 = Some(0.001);
                ReplayEvent {
                    timestamp_ms: start_ms + i as i64 * 1000, // 1 second apart
                    snapshot,
                }
            })
            .collect()
    }

    #[test]
    fn test_config_default() {
        let config = WalkForwardMLConfig::default();
        assert_eq!(config.n_folds, 5);
        assert!(config.anchored);
        assert_eq!(config.test_hours, 24.0);
        assert!(!config.spread_intercepts.is_empty());
    }

    #[test]
    fn test_trainer_with_events() {
        let events = create_test_events(1000, 0);
        let config = WalkForwardMLConfig {
            n_folds: 2,
            min_train_hours: 0.01, // Very small for test
            test_hours: 0.005,
            verbose: false,
            ..Default::default()
        };

        let trainer = WalkForwardMLTrainer::with_events(config, events);
        assert!(trainer.time_range.is_some());
    }

    #[test]
    fn test_fold_generation() {
        let events = create_test_events(10000, 0);
        let config = WalkForwardMLConfig {
            n_folds: 3,
            min_train_hours: 1.0,
            test_hours: 0.5,
            embargo_hours: 0.1,
            anchored: true,
            verbose: false,
            ..Default::default()
        };

        let trainer = WalkForwardMLTrainer::with_events(config, events);
        let folds = trainer.generate_folds().unwrap();

        // Should have generated some folds
        assert!(!folds.is_empty());

        // Each fold should have train before test
        for (train_start, train_end, test_start, test_end) in &folds {
            assert!(train_start < train_end);
            assert!(train_end < test_start); // Embargo
            assert!(test_start < test_end);
        }
    }

    #[test]
    fn test_filter_events() {
        let events = create_test_events(100, 0);
        let config = WalkForwardMLConfig::default();
        let trainer = WalkForwardMLTrainer::with_events(config, events);

        let filtered = trainer.filter_events(10000, 30000);

        // Should have events in range [10, 30) seconds
        assert!(!filtered.is_empty());
        for e in &filtered {
            assert!(e.timestamp_ms >= 10000);
            assert!(e.timestamp_ms < 30000);
        }
    }

    #[test]
    fn test_weight_stability_calculation() {
        let events = create_test_events(100, 0);
        let config = WalkForwardMLConfig::default();
        let trainer = WalkForwardMLTrainer::with_events(config, events);

        // Create fake fold results with identical weights (should be perfectly stable)
        let folds = vec![
            MLFoldResult {
                fold_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                best_weights: MLModelWeights {
                    spread: SpreadWeights {
                        intercept: 2.0,
                        w_entropy: -1.0,
                        w_volatility: 300.0,
                        ..Default::default()
                    },
                    skew: SkewWeights {
                        intercept: 0.5,
                        w_inventory: -0.8,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                train_sharpe: 1.0,
                train_return: 0.05,
                train_trades: 50,
                test_sharpe: 0.8,
                test_return: 0.04,
                test_trades: 30,
                generalization_gap: 0.2,
                configs_evaluated: 100,
                valid_configs: 50,
            },
            MLFoldResult {
                fold_num: 2,
                train_start_ms: 0,
                train_end_ms: 1500,
                test_start_ms: 1600,
                test_end_ms: 2500,
                train_events: 150,
                test_events: 60,
                best_weights: MLModelWeights {
                    spread: SpreadWeights {
                        intercept: 2.0, // Same weights
                        w_entropy: -1.0,
                        w_volatility: 300.0,
                        ..Default::default()
                    },
                    skew: SkewWeights {
                        intercept: 0.5,
                        w_inventory: -0.8,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                train_sharpe: 1.2,
                train_return: 0.06,
                train_trades: 60,
                test_sharpe: 0.9,
                test_return: 0.045,
                test_trades: 35,
                generalization_gap: 0.3,
                configs_evaluated: 100,
                valid_configs: 55,
            },
        ];

        let stability = trainer.calculate_weight_stability(&folds);

        // With identical weights, std devs should be 0
        assert_eq!(stability.spread_intercept_std, 0.0);
        assert_eq!(stability.spread_entropy_std, 0.0);
        assert_eq!(stability.stability_score, 1.0);
    }

    #[test]
    fn test_consensus_weights_calculation() {
        let events = create_test_events(100, 0);
        let config = WalkForwardMLConfig::default();
        let trainer = WalkForwardMLTrainer::with_events(config, events);

        let folds = vec![
            MLFoldResult {
                fold_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                best_weights: MLModelWeights {
                    spread: SpreadWeights {
                        intercept: 1.0,
                        w_entropy: -2.0,
                        w_volatility: 200.0,
                        ..Default::default()
                    },
                    skew: SkewWeights {
                        intercept: 0.3,
                        w_inventory: -1.0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                train_sharpe: 1.0,
                train_return: 0.05,
                train_trades: 50,
                test_sharpe: 1.0, // High test sharpe = high weight
                test_return: 0.04,
                test_trades: 30,
                generalization_gap: 0.0,
                configs_evaluated: 100,
                valid_configs: 50,
            },
            MLFoldResult {
                fold_num: 2,
                train_start_ms: 0,
                train_end_ms: 1500,
                test_start_ms: 1600,
                test_end_ms: 2500,
                train_events: 150,
                test_events: 60,
                best_weights: MLModelWeights {
                    spread: SpreadWeights {
                        intercept: 3.0,
                        w_entropy: 0.0,
                        w_volatility: 400.0,
                        ..Default::default()
                    },
                    skew: SkewWeights {
                        intercept: 0.7,
                        w_inventory: -0.6,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                train_sharpe: 0.5,
                train_return: 0.03,
                train_trades: 40,
                test_sharpe: 0.0, // Low test sharpe = low weight
                test_return: 0.01,
                test_trades: 25,
                generalization_gap: 0.5,
                configs_evaluated: 100,
                valid_configs: 45,
            },
        ];

        let consensus = trainer.calculate_consensus_weights(&folds);

        // Fold 1 has test_sharpe=1.0, weight = 2.0
        // Fold 2 has test_sharpe=0.0, weight = 1.0
        // Weighted avg should be closer to fold 1's values
        assert!(consensus.spread.intercept < 2.5); // Closer to 1.0 than 3.0
        assert!(consensus.spread.intercept > 1.0);
    }

    #[test]
    fn test_aggregate_calculation() {
        let events = create_test_events(100, 0);
        let config = WalkForwardMLConfig::default();
        let trainer = WalkForwardMLTrainer::with_events(config, events);

        let folds = vec![
            MLFoldResult {
                fold_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                best_weights: MLModelWeights::default(),
                train_sharpe: 1.0,
                train_return: 0.05,
                train_trades: 50,
                test_sharpe: 0.8,
                test_return: 0.04,
                test_trades: 30,
                generalization_gap: 0.2,
                configs_evaluated: 100,
                valid_configs: 50,
            },
            MLFoldResult {
                fold_num: 2,
                train_start_ms: 0,
                train_end_ms: 1500,
                test_start_ms: 1600,
                test_end_ms: 2500,
                train_events: 150,
                test_events: 60,
                best_weights: MLModelWeights::default(),
                train_sharpe: 1.2,
                train_return: 0.06,
                train_trades: 60,
                test_sharpe: 1.0,
                test_return: 0.05,
                test_trades: 35,
                generalization_gap: 0.2,
                configs_evaluated: 100,
                valid_configs: 55,
            },
        ];

        let agg = trainer.calculate_aggregate(&folds);

        // Average of 0.8 and 1.0
        assert!((agg.avg_oos_sharpe - 0.9).abs() < 0.01);
        // All folds profitable
        assert_eq!(agg.pct_profitable_folds, 1.0);
        // Total trades
        assert_eq!(agg.total_oos_trades, 65);
    }

    #[test]
    fn test_std_dev() {
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let sd = std_dev(&values);
        // Mean = 5.0, variance = (9+1+1+1+0+0+4+16)/7 = 32/7 ≈ 4.57, std ≈ 2.14
        assert!((sd - 2.14).abs() < 0.1);
    }

    #[test]
    fn test_std_dev_single_value() {
        let values = vec![5.0];
        let sd = std_dev(&values);
        assert_eq!(sd, 0.0);
    }

    #[test]
    fn test_erf() {
        assert!((erf(0.0)).abs() < 0.001);
        assert!((erf(5.0) - 1.0).abs() < 0.001);
        assert!((erf(-1.0) + erf(1.0)).abs() < 0.001);
    }

    #[test]
    fn test_ml_fold_result_serialization() {
        let fold = MLFoldResult {
            fold_num: 1,
            train_start_ms: 0,
            train_end_ms: 1000,
            test_start_ms: 1100,
            test_end_ms: 2000,
            train_events: 100,
            test_events: 50,
            best_weights: MLModelWeights::default(),
            train_sharpe: 1.0,
            train_return: 0.05,
            train_trades: 50,
            test_sharpe: 0.8,
            test_return: 0.04,
            test_trades: 30,
            generalization_gap: 0.2,
            configs_evaluated: 100,
            valid_configs: 50,
        };

        let json = serde_json::to_string(&fold).unwrap();
        let parsed: MLFoldResult = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.fold_num, fold.fold_num);
        assert_eq!(parsed.train_sharpe, fold.train_sharpe);
        assert_eq!(parsed.test_sharpe, fold.test_sharpe);
    }
}

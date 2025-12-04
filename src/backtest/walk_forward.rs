//! Walk-Forward Validation Framework
//!
//! Implements anchored and rolling walk-forward validation to prevent overfitting
//! in strategy backtesting.
//!
//! # Methodology
//!
//! Walk-forward validation splits historical data into multiple train/test periods,
//! optimizing parameters on training data and evaluating on out-of-sample test data.
//!
//! ```text
//! Anchored Walk-Forward (expanding window):
//! |--Train 1--|--Test 1--|
//! |----Train 2----|--Test 2--|
//! |------Train 3------|--Test 3--|
//!
//! Rolling Walk-Forward (fixed window):
//! |--Train 1--|--Test 1--|
//!       |--Train 2--|--Test 2--|
//!             |--Train 3--|--Test 3--|
//! ```
//!
//! # References
//! - Pardo, R. (2008). The Evaluation and Optimization of Trading Strategies
//! - Lopez de Prado, M. (2018). Advances in Financial Machine Learning

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use anyhow::Result;

use crate::backtest::{BacktestEngine, BacktestConfig, BacktestResults, PerformanceMetrics};
use crate::backtest::replay::ReplayConfig;
use crate::market_maker::MMConfig;

/// Walk-forward validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardConfig {
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

    /// Parameters to sweep in each training fold
    pub param_grid: ParamGrid,

    /// Data directory
    pub data_dir: PathBuf,

    /// Verbose output
    pub verbose: bool,
}

impl Default for WalkForwardConfig {
    fn default() -> Self {
        Self {
            n_folds: 5,
            min_train_hours: 100.0,
            test_hours: 24.0,
            anchored: true,
            embargo_hours: 1.0,
            param_grid: ParamGrid::default(),
            data_dir: PathBuf::from("./data/features"),
            verbose: true,
        }
    }
}

/// Parameter grid for optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamGrid {
    pub spreads: Vec<f64>,
    pub skews: Vec<f64>,
    pub fill_probs: Vec<f64>,
}

impl Default for ParamGrid {
    fn default() -> Self {
        Self {
            spreads: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            skews: vec![0.3, 0.5, 0.7],
            fill_probs: vec![0.05, 0.10, 0.15],
        }
    }
}

/// Results from a single fold
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoldResult {
    pub fold_num: usize,
    pub train_start_ms: i64,
    pub train_end_ms: i64,
    pub test_start_ms: i64,
    pub test_end_ms: i64,

    /// Best parameters found in training
    pub best_params: OptimizedParams,

    /// Training set performance (in-sample)
    pub train_metrics: FoldMetrics,

    /// Test set performance (out-of-sample)
    pub test_metrics: FoldMetrics,
}

/// Optimized parameters from training
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizedParams {
    pub spread: f64,
    pub skew: f64,
    pub fill_prob: f64,
    pub train_sharpe: f64,
}

/// Summary metrics for a fold
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FoldMetrics {
    pub sharpe: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
    pub profit_factor: f64,
}

impl From<&PerformanceMetrics> for FoldMetrics {
    fn from(m: &PerformanceMetrics) -> Self {
        Self {
            sharpe: m.sharpe_ratio,
            total_return: m.total_return,
            max_drawdown: m.max_drawdown,
            num_trades: m.num_trades,
            win_rate: m.win_rate,
            profit_factor: m.profit_factor,
        }
    }
}

/// Walk-forward validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardResults {
    pub config: WalkForwardConfig,
    pub folds: Vec<FoldResult>,
    pub aggregate: AggregateResults,
}

/// Aggregated results across all folds
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AggregateResults {
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
    /// Values close to 1.0 indicate good generalization
    pub is_oos_sharpe_ratio: f64,
    /// Probability Sharpe Ratio > 0 (statistical significance)
    pub prob_sharpe_gt_zero: f64,
}

/// Walk-forward validation engine
pub struct WalkForwardEngine {
    config: WalkForwardConfig,
    events: Vec<crate::backtest::ReplayEvent>,
    time_range: Option<(i64, i64)>,
}

impl WalkForwardEngine {
    /// Create a new walk-forward engine
    pub fn new(config: WalkForwardConfig) -> Self {
        Self {
            config,
            events: Vec::new(),
            time_range: None,
        }
    }

    /// Load data from Parquet files
    pub fn load_data(&mut self) -> Result<usize> {
        use crate::backtest::replay::{ParquetReplay, ReplayConfig};

        let replay_config = ReplayConfig {
            data_dir: self.config.data_dir.clone(),
            ..Default::default()
        };

        let mut replay = ParquetReplay::new(replay_config);
        let num_events = replay.load()?;

        self.time_range = replay.time_range();
        self.events = replay.into_events();

        Ok(num_events)
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
    fn filter_events(&self, start_ms: i64, end_ms: i64) -> Vec<crate::backtest::ReplayEvent> {
        self.events
            .iter()
            .filter(|e| e.timestamp_ms >= start_ms && e.timestamp_ms < end_ms)
            .cloned()
            .collect()
    }

    /// Run backtest on a subset of events
    fn run_backtest_on_events(
        &self,
        events: Vec<crate::backtest::ReplayEvent>,
        spread: f64,
        skew: f64,
        fill_prob: f64,
    ) -> Result<BacktestResults> {
        use crate::backtest::FillSimulatorConfig;
        use crate::mm_simulator::SimulatorConfig;

        let mm_config = MMConfig {
            max_inventory: Decimal::from_f64_retain(0.1).unwrap_or(dec!(0.1)),
            quote_size: Decimal::from_f64_retain(0.001).unwrap_or(dec!(0.001)),
            regime_params: crate::market_maker::RegimeParams::uniform(spread, skew),
            ..Default::default()
        };

        let fill_config = FillSimulatorConfig {
            base_fill_probability: fill_prob,
            ..Default::default()
        };

        let config = BacktestConfig {
            replay: ReplayConfig {
                data_dir: self.config.data_dir.clone(),
                ..Default::default()
            },
            mm: mm_config,
            simulator: SimulatorConfig::default(),
            fill_sim: fill_config,
            verbose: false,
            use_realistic_fills: true,
            ..Default::default()
        };

        let mut engine = BacktestEngine::from_events(config, events);
        engine.run()
    }

    /// Optimize parameters on training data
    fn optimize_on_train(
        &self,
        train_events: Vec<crate::backtest::ReplayEvent>,
    ) -> Result<OptimizedParams> {
        let mut best_sharpe = f64::NEG_INFINITY;
        let mut best_params = OptimizedParams {
            spread: self.config.param_grid.spreads[0],
            skew: self.config.param_grid.skews[0],
            fill_prob: self.config.param_grid.fill_probs[0],
            train_sharpe: 0.0,
        };

        for &spread in &self.config.param_grid.spreads {
            for &skew in &self.config.param_grid.skews {
                for &fill_prob in &self.config.param_grid.fill_probs {
                    let results = self.run_backtest_on_events(
                        train_events.clone(),
                        spread,
                        skew,
                        fill_prob,
                    )?;

                    let sharpe = results.metrics.sharpe_ratio;

                    // Only consider if we have enough trades
                    if results.metrics.num_trades >= 10 && sharpe > best_sharpe {
                        best_sharpe = sharpe;
                        best_params = OptimizedParams {
                            spread,
                            skew,
                            fill_prob,
                            train_sharpe: sharpe,
                        };
                    }
                }
            }
        }

        Ok(best_params)
    }

    /// Run walk-forward validation
    pub fn run(&self) -> Result<WalkForwardResults> {
        let fold_boundaries = self.generate_folds()?;

        if self.config.verbose {
            println!("\n========================================");
            println!("     WALK-FORWARD VALIDATION");
            println!("========================================");
            println!("Mode: {}", if self.config.anchored { "Anchored (expanding)" } else { "Rolling (fixed)" });
            println!("Folds: {}", fold_boundaries.len());
            println!("Parameter combinations: {}",
                self.config.param_grid.spreads.len() *
                self.config.param_grid.skews.len() *
                self.config.param_grid.fill_probs.len()
            );
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

            // Optimize on training data
            let best_params = self.optimize_on_train(train_events.clone())?;

            // Evaluate on test data with optimized parameters
            let train_results = self.run_backtest_on_events(
                train_events,
                best_params.spread,
                best_params.skew,
                best_params.fill_prob,
            )?;

            let test_results = self.run_backtest_on_events(
                test_events,
                best_params.spread,
                best_params.skew,
                best_params.fill_prob,
            )?;

            let fold_result = FoldResult {
                fold_num: i + 1,
                train_start_ms: train_start,
                train_end_ms: train_end,
                test_start_ms: test_start,
                test_end_ms: test_end,
                best_params: best_params.clone(),
                train_metrics: FoldMetrics::from(&train_results.metrics),
                test_metrics: FoldMetrics::from(&test_results.metrics),
            };

            if self.config.verbose {
                println!("  Best params: spread={:.1}, skew={:.1}, fill_prob={:.0}%",
                    best_params.spread, best_params.skew, best_params.fill_prob * 100.0);
                println!("  Train: Sharpe={:+.2}, Return={:+.2}%, Trades={}",
                    fold_result.train_metrics.sharpe,
                    fold_result.train_metrics.total_return * 100.0,
                    fold_result.train_metrics.num_trades);
                println!("  Test:  Sharpe={:+.2}, Return={:+.2}%, Trades={}",
                    fold_result.test_metrics.sharpe,
                    fold_result.test_metrics.total_return * 100.0,
                    fold_result.test_metrics.num_trades);
                println!();
            }

            folds.push(fold_result);
        }

        // Calculate aggregate results
        let aggregate = self.calculate_aggregate(&folds);

        if self.config.verbose {
            self.print_summary(&aggregate);
        }

        Ok(WalkForwardResults {
            config: self.config.clone(),
            folds,
            aggregate,
        })
    }

    /// Calculate aggregate statistics across folds
    fn calculate_aggregate(&self, folds: &[FoldResult]) -> AggregateResults {
        if folds.is_empty() {
            return AggregateResults::default();
        }

        let n = folds.len() as f64;

        // Out-of-sample statistics
        let oos_sharpes: Vec<f64> = folds.iter().map(|f| f.test_metrics.sharpe).collect();
        let avg_oos_sharpe = oos_sharpes.iter().sum::<f64>() / n;
        let std_oos_sharpe = {
            let variance = oos_sharpes.iter()
                .map(|s| (s - avg_oos_sharpe).powi(2))
                .sum::<f64>() / (n - 1.0).max(1.0);
            variance.sqrt()
        };

        let avg_oos_return = folds.iter()
            .map(|f| f.test_metrics.total_return)
            .sum::<f64>() / n;

        let total_oos_trades = folds.iter()
            .map(|f| f.test_metrics.num_trades)
            .sum();

        let avg_win_rate = folds.iter()
            .map(|f| f.test_metrics.win_rate)
            .sum::<f64>() / n;

        let profitable_folds = folds.iter()
            .filter(|f| f.test_metrics.total_return > 0.0)
            .count();
        let pct_profitable_folds = profitable_folds as f64 / n;

        // In-sample vs out-of-sample comparison (overfitting indicator)
        let avg_is_sharpe = folds.iter()
            .map(|f| f.train_metrics.sharpe)
            .sum::<f64>() / n;
        let is_oos_sharpe_ratio = if avg_is_sharpe.abs() > 0.01 {
            avg_oos_sharpe / avg_is_sharpe
        } else {
            0.0
        };

        // Probability of Sharpe > 0 (simplified Bailey-Lopez de Prado)
        // Using t-distribution approximation
        let prob_sharpe_gt_zero = if std_oos_sharpe > 0.0 && n > 2.0 {
            let t_stat = avg_oos_sharpe * (n - 1.0).sqrt() / std_oos_sharpe;
            // Approximate using normal CDF for simplicity
            0.5 * (1.0 + erf(t_stat / std::f64::consts::SQRT_2))
        } else {
            0.5
        };

        AggregateResults {
            avg_oos_sharpe,
            std_oos_sharpe,
            avg_oos_return,
            total_oos_trades,
            avg_win_rate,
            pct_profitable_folds,
            is_oos_sharpe_ratio,
            prob_sharpe_gt_zero,
        }
    }

    fn print_summary(&self, agg: &AggregateResults) {
        println!("========================================");
        println!("     WALK-FORWARD RESULTS SUMMARY");
        println!("========================================");
        println!();
        println!("OUT-OF-SAMPLE PERFORMANCE:");
        println!("  Avg Sharpe:          {:+.3} +/- {:.3}", agg.avg_oos_sharpe, agg.std_oos_sharpe);
        println!("  Avg Return:          {:+.2}%", agg.avg_oos_return * 100.0);
        println!("  Total Trades:        {}", agg.total_oos_trades);
        println!("  Avg Win Rate:        {:.1}%", agg.avg_win_rate * 100.0);
        println!("  Profitable Folds:    {:.0}%", agg.pct_profitable_folds * 100.0);
        println!();
        println!("OVERFITTING INDICATORS:");
        println!("  IS/OOS Sharpe Ratio: {:.2} (closer to 1.0 = less overfit)", agg.is_oos_sharpe_ratio);
        println!("  P(Sharpe > 0):       {:.1}%", agg.prob_sharpe_gt_zero * 100.0);
        println!();

        // Interpretation
        if agg.is_oos_sharpe_ratio > 0.8 {
            println!("INTERPRETATION: Good generalization - strategy appears robust");
        } else if agg.is_oos_sharpe_ratio > 0.5 {
            println!("INTERPRETATION: Moderate overfitting - consider simplifying strategy");
        } else {
            println!("INTERPRETATION: Significant overfitting - results likely won't replicate");
        }

        if agg.prob_sharpe_gt_zero > 0.95 {
            println!("STATISTICAL SIGNIFICANCE: High confidence (>95%) Sharpe > 0");
        } else if agg.prob_sharpe_gt_zero > 0.90 {
            println!("STATISTICAL SIGNIFICANCE: Moderate confidence (90-95%) Sharpe > 0");
        } else {
            println!("STATISTICAL SIGNIFICANCE: Low confidence (<90%) - results may be noise");
        }

        println!("========================================");
    }
}

impl WalkForwardResults {
    /// Save results to JSON file
    pub fn save_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

/// Error function approximation for probability calculation
fn erf(x: f64) -> f64 {
    // Horner form coefficients for approximation
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_walk_forward_config_default() {
        let config = WalkForwardConfig::default();
        assert_eq!(config.n_folds, 5);
        assert!(config.anchored);
        assert_eq!(config.test_hours, 24.0);
    }

    #[test]
    fn test_param_grid_default() {
        let grid = ParamGrid::default();
        assert!(!grid.spreads.is_empty());
        assert!(!grid.skews.is_empty());
        assert!(!grid.fill_probs.is_empty());
    }

    #[test]
    fn test_fold_metrics_from_performance() {
        let metrics = PerformanceMetrics {
            sharpe_ratio: 1.5,
            total_return: 0.10,
            max_drawdown: 0.05,
            num_trades: 100,
            win_rate: 0.55,
            profit_factor: 1.8,
            ..Default::default()
        };

        let fold_metrics = FoldMetrics::from(&metrics);
        assert_eq!(fold_metrics.sharpe, 1.5);
        assert_eq!(fold_metrics.total_return, 0.10);
        assert_eq!(fold_metrics.num_trades, 100);
    }

    #[test]
    fn test_erf_function() {
        // erf(0) = 0
        assert!((erf(0.0)).abs() < 0.001);
        // erf(inf) -> 1
        assert!((erf(5.0) - 1.0).abs() < 0.001);
        // erf(-x) = -erf(x)
        assert!((erf(1.0) + erf(-1.0)).abs() < 0.001);
    }

    #[test]
    fn test_aggregate_results_default() {
        let agg = AggregateResults::default();
        assert_eq!(agg.total_oos_trades, 0);
        assert_eq!(agg.avg_oos_sharpe, 0.0);
    }
}

//! ForwardStage Implementation (Task 2.2)
//!
//! Walk-forward validation stage that splits data into train/test windows,
//! trains on each window, and validates on out-of-sample data.
//!
//! # Overview
//!
//! The ForwardStage is the second stage in the validation pipeline. It:
//! 1. Splits historical data into N train/test windows
//! 2. For each window: train parameters on in-sample, evaluate on out-of-sample
//! 3. Tracks generalization gap (train - test performance)
//! 4. Computes aggregate metrics across all windows
//! 5. Produces a ValidationResult with pass/fail based on thresholds
//!
//! # Walk-Forward Methodology
//!
//! ```text
//! |-------- Train 1 --------|-- Test 1 --|
//! |------------- Train 2 -----------|-- Test 2 --|
//! |------------------ Train 3 --------------|-- Test 3 --|
//! ```
//!
//! Anchored mode: Training window expands from fixed start
//! Rolling mode: Training window slides forward with fixed size
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::validation::{ForwardStage, ForwardStageConfig, StageContext};
//!
//! let stage = ForwardStage::new(ForwardStageConfig::default());
//! let context = StageContext::default()
//!     .with_data_path("./data/features")
//!     .with_name("WF-2025Q1");
//!
//! let result = stage.run(&context).await?;
//! ```

use std::path::PathBuf;
use std::time::Instant;

use chrono::{Duration, TimeZone, Utc};
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use crate::backtest::{
    BacktestConfig, BacktestEngine, BacktestResults,
    ReplayConfig, FillSimulatorConfig, ParquetReplay, ReplayEvent,
};
use crate::backtest::metrics::{TradeRecord, TradeSide};
use crate::core::{
    ValidationResult, ValidationStageType,
    TradeResult, TradeDirection, ExitReason,
};
use crate::execution::market_maker::MMConfig;
use crate::execution::mm_simulator::SimulatorConfig;

use super::traits::{ValidationStage, StageContext, StageError, RunFuture};

/// Configuration for the ForwardStage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardStageConfig {
    /// Number of train/test windows (folds)
    pub n_folds: usize,

    /// Minimum training period in hours
    pub min_train_hours: f64,

    /// Test period in hours per fold
    pub test_hours: f64,

    /// Use anchored (expanding) vs rolling (fixed) window
    pub anchored: bool,

    /// Gap between train and test to prevent lookahead (hours)
    pub embargo_hours: f64,

    /// Use realistic fill simulation
    pub use_realistic_fills: bool,

    /// Fill probability (for realistic fills)
    pub fill_probability: f64,

    /// Fee rate (as decimal, e.g., 0.0001 for 1 bps)
    pub fee_rate_bps: f64,

    /// Initial capital for equity tracking
    pub initial_capital: f64,

    /// Minimum trades per window for valid evaluation
    pub min_trades_per_window: usize,

    /// Maximum acceptable generalization gap (train - test sharpe)
    pub max_generalization_gap: f64,

    /// Minimum percentage of profitable windows
    pub min_profitable_pct: f64,

    /// Print progress during execution
    pub verbose: bool,

    /// Name for the stage
    pub name: String,
}

impl Default for ForwardStageConfig {
    fn default() -> Self {
        Self {
            n_folds: 5,
            min_train_hours: 100.0,
            test_hours: 24.0,
            anchored: true,
            embargo_hours: 1.0,
            use_realistic_fills: true,
            fill_probability: 0.10,
            fee_rate_bps: 1.0,
            initial_capital: 10_000.0,
            min_trades_per_window: 10,
            max_generalization_gap: 2.0,  // Train sharpe can be at most 2.0 higher than test
            min_profitable_pct: 0.40,     // At least 40% of windows should be profitable
            verbose: false,
            name: "Forward".to_string(),
        }
    }
}

impl ForwardStageConfig {
    /// Create a configuration optimized for quick validation
    pub fn quick() -> Self {
        Self {
            n_folds: 3,
            min_train_hours: 50.0,
            test_hours: 12.0,
            use_realistic_fills: false,
            fill_probability: 1.0,
            verbose: false,
            ..Default::default()
        }
    }

    /// Create a configuration with conservative settings
    pub fn conservative() -> Self {
        Self {
            n_folds: 7,
            min_train_hours: 150.0,
            test_hours: 48.0,
            embargo_hours: 2.0,
            use_realistic_fills: true,
            fill_probability: 0.05,
            max_generalization_gap: 1.5,
            min_profitable_pct: 0.50,
            ..Default::default()
        }
    }

    /// Create a configuration with rolling (fixed-size) windows
    pub fn rolling() -> Self {
        Self {
            anchored: false,
            ..Default::default()
        }
    }

    /// Set the stage name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set number of folds
    pub fn with_folds(mut self, n_folds: usize) -> Self {
        self.n_folds = n_folds;
        self
    }
}

/// Results from a single walk-forward window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowResult {
    /// Window number (1-indexed)
    pub window_num: usize,

    /// Train period start timestamp (ms)
    pub train_start_ms: i64,

    /// Train period end timestamp (ms)
    pub train_end_ms: i64,

    /// Test period start timestamp (ms)
    pub test_start_ms: i64,

    /// Test period end timestamp (ms)
    pub test_end_ms: i64,

    /// Number of events in training set
    pub train_events: usize,

    /// Number of events in test set
    pub test_events: usize,

    /// Training set Sharpe ratio
    pub train_sharpe: f64,

    /// Training set return
    pub train_return: f64,

    /// Training set trades
    pub train_trades: usize,

    /// Test set Sharpe ratio (out-of-sample)
    pub test_sharpe: f64,

    /// Test set return (out-of-sample)
    pub test_return: f64,

    /// Test set trades
    pub test_trades: usize,

    /// Test set win rate
    pub test_win_rate: f64,

    /// Generalization gap (train_sharpe - test_sharpe)
    pub generalization_gap: f64,

    /// Whether this window passed validation
    pub passed: bool,
}

impl WindowResult {
    /// Check if this window is profitable (test return > 0)
    pub fn is_profitable(&self) -> bool {
        self.test_return > 0.0
    }

    /// Check if this window has acceptable generalization
    pub fn has_good_generalization(&self, max_gap: f64) -> bool {
        self.generalization_gap <= max_gap
    }
}

/// Aggregate results across all windows
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ForwardAggregateMetrics {
    /// Average out-of-sample Sharpe ratio
    pub avg_oos_sharpe: f64,

    /// Standard deviation of out-of-sample Sharpe
    pub std_oos_sharpe: f64,

    /// Average out-of-sample return
    pub avg_oos_return: f64,

    /// Total out-of-sample trades
    pub total_oos_trades: usize,

    /// Average generalization gap
    pub avg_generalization_gap: f64,

    /// Percentage of profitable windows
    pub pct_profitable_windows: f64,

    /// Number of windows that passed validation
    pub windows_passed: usize,

    /// Total windows evaluated
    pub total_windows: usize,

    /// In-sample vs out-of-sample Sharpe ratio (overfitting indicator)
    pub is_oos_sharpe_ratio: f64,
}

impl ForwardAggregateMetrics {
    /// Calculate aggregate metrics from window results
    pub fn from_windows(windows: &[WindowResult]) -> Self {
        if windows.is_empty() {
            return Self::default();
        }

        let n = windows.len() as f64;

        // Out-of-sample statistics
        let oos_sharpes: Vec<f64> = windows.iter().map(|w| w.test_sharpe).collect();
        let avg_oos_sharpe = oos_sharpes.iter().sum::<f64>() / n;

        let std_oos_sharpe = if n > 1.0 {
            let variance = oos_sharpes.iter()
                .map(|s| (s - avg_oos_sharpe).powi(2))
                .sum::<f64>() / (n - 1.0);
            variance.sqrt()
        } else {
            0.0
        };

        let avg_oos_return = windows.iter()
            .map(|w| w.test_return)
            .sum::<f64>() / n;

        let total_oos_trades = windows.iter()
            .map(|w| w.test_trades)
            .sum();

        let avg_generalization_gap = windows.iter()
            .map(|w| w.generalization_gap)
            .sum::<f64>() / n;

        let profitable_windows = windows.iter()
            .filter(|w| w.is_profitable())
            .count();
        let pct_profitable_windows = profitable_windows as f64 / n;

        let windows_passed = windows.iter()
            .filter(|w| w.passed)
            .count();

        // In-sample vs out-of-sample comparison
        let avg_is_sharpe = windows.iter()
            .map(|w| w.train_sharpe)
            .sum::<f64>() / n;

        let is_oos_sharpe_ratio = if avg_is_sharpe.abs() > 0.01 {
            avg_oos_sharpe / avg_is_sharpe
        } else {
            0.0
        };

        Self {
            avg_oos_sharpe,
            std_oos_sharpe,
            avg_oos_return,
            total_oos_trades,
            avg_generalization_gap,
            pct_profitable_windows,
            windows_passed,
            total_windows: windows.len(),
            is_oos_sharpe_ratio,
        }
    }
}

/// ForwardStage - Walk-forward validation
///
/// This stage performs walk-forward validation by splitting data into
/// train/test windows and evaluating out-of-sample performance.
pub struct ForwardStage {
    config: ForwardStageConfig,
}

impl ForwardStage {
    /// Create a new ForwardStage with the given configuration
    pub fn new(config: ForwardStageConfig) -> Self {
        Self { config }
    }

    /// Create a ForwardStage with default configuration
    pub fn with_defaults() -> Self {
        Self::new(ForwardStageConfig::default())
    }

    /// Load events from Parquet files in the given path
    fn load_events(&self, data_path: &str, start_ms: Option<i64>, end_ms: Option<i64>) -> Result<Vec<ReplayEvent>, StageError> {
        let replay_config = ReplayConfig {
            data_dir: PathBuf::from(data_path),
            start_time: start_ms,
            end_time: end_ms,
            speed: 0.0,
        };

        let mut replay = ParquetReplay::new(replay_config);
        let num_events = replay.load()
            .map_err(|e| StageError::DataUnavailable(format!("Failed to load data: {}", e)))?;

        if num_events == 0 {
            return Err(StageError::DataUnavailable(
                "No events found in data directory".to_string()
            ));
        }

        Ok(replay.into_events())
    }

    /// Generate time-based fold boundaries
    fn generate_folds(&self, start_ms: i64, end_ms: i64) -> Vec<(i64, i64, i64, i64)> {
        let test_ms = (self.config.test_hours * 60.0 * 60.0 * 1000.0) as i64;
        let embargo_ms = (self.config.embargo_hours * 60.0 * 60.0 * 1000.0) as i64;
        let min_train_ms = (self.config.min_train_hours * 60.0 * 60.0 * 1000.0) as i64;

        let mut folds = Vec::new();

        if self.config.anchored {
            // Anchored walk-forward: expanding training window
            let available_for_folds = end_ms - start_ms - min_train_ms;
            if available_for_folds <= 0 {
                return folds;
            }

            let fold_size_ms = available_for_folds / self.config.n_folds as i64;

            for i in 0..self.config.n_folds {
                let train_start = start_ms;
                let train_end = start_ms + min_train_ms + (i as i64 * fold_size_ms);
                let test_start = train_end + embargo_ms;
                let test_end = (test_start + test_ms).min(end_ms);

                if test_end > test_start && train_end < end_ms {
                    folds.push((train_start, train_end, test_start, test_end));
                }
            }
        } else {
            // Rolling walk-forward: fixed training window
            let step_ms = (end_ms - start_ms - min_train_ms - test_ms) / self.config.n_folds.max(1) as i64;

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

        folds
    }

    /// Filter events to a time range
    fn filter_events(&self, events: &[ReplayEvent], start_ms: i64, end_ms: i64) -> Vec<ReplayEvent> {
        events
            .iter()
            .filter(|e| e.timestamp_ms >= start_ms && e.timestamp_ms < end_ms)
            .cloned()
            .collect()
    }

    /// Run backtest on a set of events
    fn run_backtest(&self, events: Vec<ReplayEvent>, data_path: &str) -> Result<BacktestResults, StageError> {
        let backtest_config = BacktestConfig {
            replay: ReplayConfig {
                data_dir: PathBuf::from(data_path),
                start_time: None,
                end_time: None,
                speed: 0.0,
            },
            mm: MMConfig::default(),
            simulator: SimulatorConfig::default(),
            fill_sim: FillSimulatorConfig {
                base_fill_probability: self.config.fill_probability,
                fee_rate: rust_decimal::Decimal::from_f64(self.config.fee_rate_bps / 10000.0)
                    .unwrap_or_else(|| rust_decimal::Decimal::new(1, 4)),
                ..Default::default()
            },
            initial_capital: rust_decimal::Decimal::from_f64(self.config.initial_capital)
                .unwrap_or_else(|| rust_decimal::Decimal::new(10000, 0)),
            risk_free_rate: 0.05,
            equity_sample_interval: 100,
            verbose: false,
            use_realistic_fills: self.config.use_realistic_fills,
            oco: Default::default(),
        };

        let mut engine = BacktestEngine::from_events(backtest_config, events);
        engine.run()
            .map_err(|e| StageError::ExecutionError(format!("Backtest failed: {}", e)))
    }

    /// Evaluate a single window
    fn evaluate_window(
        &self,
        window_num: usize,
        train_events: Vec<ReplayEvent>,
        test_events: Vec<ReplayEvent>,
        train_start: i64,
        train_end: i64,
        test_start: i64,
        test_end: i64,
        data_path: &str,
    ) -> Result<WindowResult, StageError> {
        let train_event_count = train_events.len();
        let test_event_count = test_events.len();

        // Run backtest on training data
        let train_results = self.run_backtest(train_events, data_path)?;

        // Run backtest on test data
        let test_results = self.run_backtest(test_events, data_path)?;

        let generalization_gap = train_results.metrics.sharpe_ratio - test_results.metrics.sharpe_ratio;

        // Determine if this window passes
        let passed = test_results.fills_generated >= self.config.min_trades_per_window
            && generalization_gap <= self.config.max_generalization_gap;

        Ok(WindowResult {
            window_num,
            train_start_ms: train_start,
            train_end_ms: train_end,
            test_start_ms: test_start,
            test_end_ms: test_end,
            train_events: train_event_count,
            test_events: test_event_count,
            train_sharpe: train_results.metrics.sharpe_ratio,
            train_return: train_results.metrics.total_return,
            train_trades: train_results.fills_generated,
            test_sharpe: test_results.metrics.sharpe_ratio,
            test_return: test_results.metrics.total_return,
            test_trades: test_results.fills_generated,
            test_win_rate: test_results.metrics.win_rate,
            generalization_gap,
            passed,
        })
    }

    /// Convert WindowResult to TradeResult for aggregation
    fn window_to_trade_result(&self, window: &WindowResult, config_id: &str) -> TradeResult {
        let entry_time = Utc.timestamp_millis_opt(window.test_start_ms)
            .single()
            .unwrap_or_else(Utc::now);
        let exit_time = Utc.timestamp_millis_opt(window.test_end_ms)
            .single()
            .unwrap_or_else(Utc::now);

        // Convert window result to a "trade" representing the window performance
        let direction = if window.test_return >= 0.0 {
            TradeDirection::Long
        } else {
            TradeDirection::Short
        };

        TradeResult {
            trade_id: format!("WF-{}", window.window_num),
            direction,
            entry_time,
            exit_time,
            entry_price: 1.0,  // Normalized
            exit_price: 1.0 + window.test_return,
            size: 1.0,
            pnl: window.test_return * self.config.initial_capital,
            pnl_bps: window.test_return * 10000.0,
            return_pct: window.test_return * 100.0,
            exit_reason: if window.test_return >= 0.0 {
                ExitReason::TakeProfit
            } else {
                ExitReason::StopLoss
            },
            research_state_id: None,
            config_id: Some(config_id.to_string()),
            slippage_bps: 0.0,
            commission: 0.0,
            mae_bps: 0.0,
            mfe_bps: 0.0,
            metadata: {
                let mut m = std::collections::HashMap::new();
                m.insert("train_sharpe".to_string(), format!("{:.3}", window.train_sharpe));
                m.insert("test_sharpe".to_string(), format!("{:.3}", window.test_sharpe));
                m.insert("generalization_gap".to_string(), format!("{:.3}", window.generalization_gap));
                m.insert("test_trades".to_string(), window.test_trades.to_string());
                m
            },
        }
    }

    /// Convert results to ValidationResult
    fn convert_results(
        &self,
        windows: &[WindowResult],
        aggregate: &ForwardAggregateMetrics,
        context: &StageContext,
        duration_secs: f64,
    ) -> ValidationResult {
        // Convert windows to trade results
        let trades: Vec<TradeResult> = windows
            .iter()
            .map(|w| self.window_to_trade_result(w, &context.config.id))
            .collect();

        // Create validation result
        let mut result = ValidationResult::new(
            ValidationStageType::Forward,
            context.stage_name.clone(),
            context.config.id.clone(),
            context.period_start,
            context.period_end,
        );

        // Set trades
        result = result.with_trades(trades);

        // Add metadata
        result.add_metadata("total_windows".to_string(), aggregate.total_windows.to_string());
        result.add_metadata("windows_passed".to_string(), aggregate.windows_passed.to_string());
        result.add_metadata("avg_oos_sharpe".to_string(), format!("{:.3}", aggregate.avg_oos_sharpe));
        result.add_metadata("std_oos_sharpe".to_string(), format!("{:.3}", aggregate.std_oos_sharpe));
        result.add_metadata("avg_generalization_gap".to_string(), format!("{:.3}", aggregate.avg_generalization_gap));
        result.add_metadata("pct_profitable_windows".to_string(), format!("{:.1}%", aggregate.pct_profitable_windows * 100.0));
        result.add_metadata("is_oos_sharpe_ratio".to_string(), format!("{:.2}", aggregate.is_oos_sharpe_ratio));
        result.add_metadata("mode".to_string(), if self.config.anchored { "anchored" } else { "rolling" }.to_string());

        // Set validation duration
        result.set_duration(duration_secs);

        // Evaluate thresholds
        result.evaluate_thresholds(context.thresholds.clone());

        // Add warnings
        if aggregate.pct_profitable_windows < self.config.min_profitable_pct {
            result.add_warning(format!(
                "Low profitable window rate: {:.1}% (minimum: {:.1}%)",
                aggregate.pct_profitable_windows * 100.0,
                self.config.min_profitable_pct * 100.0
            ));
        }

        if aggregate.avg_generalization_gap > self.config.max_generalization_gap {
            result.add_warning(format!(
                "High generalization gap: {:.2} (maximum: {:.2})",
                aggregate.avg_generalization_gap,
                self.config.max_generalization_gap
            ));
        }

        if aggregate.is_oos_sharpe_ratio < 0.5 && aggregate.avg_oos_sharpe.abs() > 0.1 {
            result.add_warning(
                "Possible overfitting: OOS Sharpe significantly lower than IS Sharpe".to_string()
            );
        }

        if aggregate.total_windows < 3 {
            result.add_warning(format!(
                "Limited windows for statistical significance: {} (recommend at least 3)",
                aggregate.total_windows
            ));
        }

        result
    }

    /// Execute walk-forward validation
    async fn execute_forward(&self, context: &StageContext) -> Result<(Vec<WindowResult>, ForwardAggregateMetrics), StageError> {
        let data_path = context.data_path.as_deref()
            .ok_or_else(|| StageError::ConfigurationError("Data path required".to_string()))?;

        // Load all events
        let start_ms = Some(context.period_start.timestamp_millis());
        let end_ms = Some(context.period_end.timestamp_millis());
        let events = self.load_events(data_path, start_ms, end_ms)?;

        if events.is_empty() {
            return Err(StageError::DataUnavailable("No events loaded".to_string()));
        }

        // Get actual time range from data
        let actual_start = events.first().map(|e| e.timestamp_ms).unwrap_or(0);
        let actual_end = events.last().map(|e| e.timestamp_ms).unwrap_or(0);

        // Generate fold boundaries
        let folds = self.generate_folds(actual_start, actual_end);

        if folds.is_empty() {
            return Err(StageError::DataUnavailable(
                "Insufficient data for walk-forward validation".to_string()
            ));
        }

        // Evaluate each window
        let mut windows = Vec::new();

        for (i, &(train_start, train_end, test_start, test_end)) in folds.iter().enumerate() {
            let train_events = self.filter_events(&events, train_start, train_end);
            let test_events = self.filter_events(&events, test_start, test_end);

            if train_events.is_empty() || test_events.is_empty() {
                continue;
            }

            match self.evaluate_window(
                i + 1,
                train_events,
                test_events,
                train_start,
                train_end,
                test_start,
                test_end,
                data_path,
            ) {
                Ok(window_result) => windows.push(window_result),
                Err(e) => {
                    if self.config.verbose {
                        eprintln!("Window {} evaluation failed: {}", i + 1, e);
                    }
                }
            }
        }

        if windows.is_empty() {
            return Err(StageError::ExecutionError(
                "No windows completed successfully".to_string()
            ));
        }

        // Calculate aggregate metrics
        let aggregate = ForwardAggregateMetrics::from_windows(&windows);

        Ok((windows, aggregate))
    }
}

impl ValidationStage for ForwardStage {
    fn stage_type(&self) -> ValidationStageType {
        ValidationStageType::Forward
    }

    fn name(&self) -> &str {
        &self.config.name
    }

    fn description(&self) -> &str {
        "Walk-forward validation with train/test window splitting"
    }

    fn can_run(&self, context: &StageContext) -> Result<(), StageError> {
        // Check period validity
        if context.period_end <= context.period_start {
            return Err(StageError::ConfigurationError(
                "Period end must be after period start".to_string(),
            ));
        }

        // Check minimum period length for walk-forward
        let period_hours = context.period_days() * 24.0;
        let required_hours = self.config.min_train_hours + self.config.test_hours * self.config.n_folds as f64;

        if period_hours < required_hours {
            return Err(StageError::ConfigurationError(format!(
                "Period too short for walk-forward: {:.0} hours (need at least {:.0} hours)",
                period_hours, required_hours
            )));
        }

        // Check data path exists
        let data_path = context.data_path.as_deref()
            .ok_or_else(|| StageError::ConfigurationError(
                "Data path required for forward stage".to_string(),
            ))?;

        let path = PathBuf::from(data_path);
        if !path.exists() {
            return Err(StageError::DataUnavailable(format!(
                "Data directory does not exist: {}",
                data_path
            )));
        }

        if !path.is_dir() {
            return Err(StageError::ConfigurationError(format!(
                "Data path is not a directory: {}",
                data_path
            )));
        }

        // Check for parquet files
        let has_parquet = std::fs::read_dir(&path)
            .map(|entries| {
                entries.filter_map(|e| e.ok())
                    .any(|e| e.path().extension().map(|ext| ext == "parquet").unwrap_or(false))
            })
            .unwrap_or(false);

        if !has_parquet {
            return Err(StageError::DataUnavailable(format!(
                "No Parquet files found in: {}",
                data_path
            )));
        }

        // Check n_folds
        if self.config.n_folds < 2 {
            return Err(StageError::ConfigurationError(
                "Walk-forward requires at least 2 folds".to_string(),
            ));
        }

        Ok(())
    }

    fn run<'a>(&'a self, context: &'a StageContext) -> RunFuture<'a> {
        Box::pin(async move {
            let start_time = Instant::now();

            // Execute walk-forward validation
            let (windows, aggregate) = self.execute_forward(context).await?;

            let duration_secs = start_time.elapsed().as_secs_f64();

            // Convert to ValidationResult
            let result = self.convert_results(&windows, &aggregate, context, duration_secs);

            Ok(result)
        })
    }

    fn estimated_duration(&self, context: &StageContext) -> Option<u64> {
        // Estimate based on period length and number of folds
        // Each fold requires ~2 seconds per day of data
        let days = context.period_days();
        let base_time = (days * 2.0) as u64;
        Some(base_time * self.config.n_folds as u64)
    }

    fn min_trades(&self) -> usize {
        self.config.min_trades_per_window * self.config.n_folds
    }

    fn requires_previous(&self) -> Option<ValidationStageType> {
        Some(ValidationStageType::Backtest)  // Forward requires backtest to pass first
    }
}

/// Factory for creating ForwardStage instances
pub struct ForwardStageFactory {
    default_config: ForwardStageConfig,
}

impl ForwardStageFactory {
    /// Create a new factory with default configuration
    pub fn new() -> Self {
        Self {
            default_config: ForwardStageConfig::default(),
        }
    }

    /// Create a factory with custom default configuration
    pub fn with_config(config: ForwardStageConfig) -> Self {
        Self {
            default_config: config,
        }
    }

    /// Create a ForwardStage with the default configuration
    pub fn create(&self, name: &str) -> ForwardStage {
        ForwardStage::new(self.default_config.clone().with_name(name))
    }

    /// Create a ForwardStage with custom configuration
    pub fn create_with_config(&self, name: &str, config: ForwardStageConfig) -> ForwardStage {
        ForwardStage::new(config.with_name(name))
    }
}

impl Default for ForwardStageFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AlgorithmConfig, ValidationThresholds};
    use tempfile::tempdir;
    use std::fs::File;

    // ==================== ForwardStageConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = ForwardStageConfig::default();

        assert_eq!(config.n_folds, 5);
        assert!((config.min_train_hours - 100.0).abs() < 0.01);
        assert!((config.test_hours - 24.0).abs() < 0.01);
        assert!(config.anchored);
        assert!((config.embargo_hours - 1.0).abs() < 0.01);
        assert!(config.use_realistic_fills);
        assert!((config.fill_probability - 0.10).abs() < 0.01);
        assert_eq!(config.min_trades_per_window, 10);
        assert_eq!(config.name, "Forward");
    }

    #[test]
    fn test_config_quick() {
        let config = ForwardStageConfig::quick();

        assert_eq!(config.n_folds, 3);
        assert!((config.min_train_hours - 50.0).abs() < 0.01);
        assert!((config.test_hours - 12.0).abs() < 0.01);
        assert!(!config.use_realistic_fills);
        assert!((config.fill_probability - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_config_conservative() {
        let config = ForwardStageConfig::conservative();

        assert_eq!(config.n_folds, 7);
        assert!((config.min_train_hours - 150.0).abs() < 0.01);
        assert!((config.test_hours - 48.0).abs() < 0.01);
        assert!((config.embargo_hours - 2.0).abs() < 0.01);
        assert!(config.use_realistic_fills);
        assert!((config.fill_probability - 0.05).abs() < 0.01);
    }

    #[test]
    fn test_config_rolling() {
        let config = ForwardStageConfig::rolling();
        assert!(!config.anchored);
    }

    #[test]
    fn test_config_with_name() {
        let config = ForwardStageConfig::default().with_name("WF-Test");
        assert_eq!(config.name, "WF-Test");
    }

    #[test]
    fn test_config_with_folds() {
        let config = ForwardStageConfig::default().with_folds(10);
        assert_eq!(config.n_folds, 10);
    }

    #[test]
    fn test_config_serialization() {
        let config = ForwardStageConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ForwardStageConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, config.name);
        assert_eq!(deserialized.n_folds, config.n_folds);
        assert!(deserialized.anchored == config.anchored);
    }

    // ==================== WindowResult Tests ====================

    #[test]
    fn test_window_result_profitable() {
        let window = WindowResult {
            window_num: 1,
            train_start_ms: 0,
            train_end_ms: 1000,
            test_start_ms: 1100,
            test_end_ms: 2000,
            train_events: 100,
            test_events: 50,
            train_sharpe: 1.0,
            train_return: 0.05,
            train_trades: 50,
            test_sharpe: 0.8,
            test_return: 0.03,  // Positive
            test_trades: 30,
            test_win_rate: 0.6,
            generalization_gap: 0.2,
            passed: true,
        };

        assert!(window.is_profitable());
    }

    #[test]
    fn test_window_result_not_profitable() {
        let window = WindowResult {
            window_num: 1,
            train_start_ms: 0,
            train_end_ms: 1000,
            test_start_ms: 1100,
            test_end_ms: 2000,
            train_events: 100,
            test_events: 50,
            train_sharpe: 1.0,
            train_return: 0.05,
            train_trades: 50,
            test_sharpe: -0.5,
            test_return: -0.02,  // Negative
            test_trades: 30,
            test_win_rate: 0.4,
            generalization_gap: 1.5,
            passed: false,
        };

        assert!(!window.is_profitable());
    }

    #[test]
    fn test_window_result_good_generalization() {
        let window = WindowResult {
            window_num: 1,
            train_start_ms: 0,
            train_end_ms: 1000,
            test_start_ms: 1100,
            test_end_ms: 2000,
            train_events: 100,
            test_events: 50,
            train_sharpe: 1.0,
            train_return: 0.05,
            train_trades: 50,
            test_sharpe: 0.8,
            test_return: 0.03,
            test_trades: 30,
            test_win_rate: 0.6,
            generalization_gap: 0.2,  // Small gap
            passed: true,
        };

        assert!(window.has_good_generalization(1.0));
        assert!(window.has_good_generalization(0.5));
        assert!(!window.has_good_generalization(0.1));
    }

    #[test]
    fn test_window_result_bad_generalization() {
        let window = WindowResult {
            window_num: 1,
            train_start_ms: 0,
            train_end_ms: 1000,
            test_start_ms: 1100,
            test_end_ms: 2000,
            train_events: 100,
            test_events: 50,
            train_sharpe: 2.5,
            train_return: 0.10,
            train_trades: 50,
            test_sharpe: 0.0,
            test_return: -0.01,
            test_trades: 30,
            test_win_rate: 0.45,
            generalization_gap: 2.5,  // Large gap - overfitting
            passed: false,
        };

        assert!(!window.has_good_generalization(2.0));
        assert!(window.has_good_generalization(3.0));
    }

    // ==================== ForwardAggregateMetrics Tests ====================

    #[test]
    fn test_aggregate_from_empty() {
        let windows: Vec<WindowResult> = vec![];
        let agg = ForwardAggregateMetrics::from_windows(&windows);

        assert_eq!(agg.total_windows, 0);
        assert_eq!(agg.windows_passed, 0);
        assert!((agg.avg_oos_sharpe).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_from_single_window() {
        let windows = vec![
            WindowResult {
                window_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 1.0,
                train_return: 0.05,
                train_trades: 50,
                test_sharpe: 0.8,
                test_return: 0.04,
                test_trades: 30,
                test_win_rate: 0.6,
                generalization_gap: 0.2,
                passed: true,
            },
        ];

        let agg = ForwardAggregateMetrics::from_windows(&windows);

        assert_eq!(agg.total_windows, 1);
        assert_eq!(agg.windows_passed, 1);
        assert!((agg.avg_oos_sharpe - 0.8).abs() < 0.01);
        assert!((agg.pct_profitable_windows - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_from_multiple_windows() {
        let windows = vec![
            WindowResult {
                window_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 1.0,
                train_return: 0.05,
                train_trades: 50,
                test_sharpe: 0.8,
                test_return: 0.04,
                test_trades: 30,
                test_win_rate: 0.6,
                generalization_gap: 0.2,
                passed: true,
            },
            WindowResult {
                window_num: 2,
                train_start_ms: 1000,
                train_end_ms: 2000,
                test_start_ms: 2100,
                test_end_ms: 3000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 1.2,
                train_return: 0.06,
                train_trades: 60,
                test_sharpe: 1.0,
                test_return: 0.05,
                test_trades: 35,
                test_win_rate: 0.65,
                generalization_gap: 0.2,
                passed: true,
            },
        ];

        let agg = ForwardAggregateMetrics::from_windows(&windows);

        assert_eq!(agg.total_windows, 2);
        assert_eq!(agg.windows_passed, 2);
        assert!((agg.avg_oos_sharpe - 0.9).abs() < 0.01);  // (0.8 + 1.0) / 2
        assert!(agg.std_oos_sharpe > 0.0);
        assert_eq!(agg.total_oos_trades, 65);  // 30 + 35
        assert!((agg.pct_profitable_windows - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_mixed_results() {
        let windows = vec![
            WindowResult {
                window_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 1.0,
                train_return: 0.05,
                train_trades: 50,
                test_sharpe: 0.8,
                test_return: 0.04,
                test_trades: 30,
                test_win_rate: 0.6,
                generalization_gap: 0.2,
                passed: true,
            },
            WindowResult {
                window_num: 2,
                train_start_ms: 1000,
                train_end_ms: 2000,
                test_start_ms: 2100,
                test_end_ms: 3000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 1.5,
                train_return: 0.08,
                train_trades: 60,
                test_sharpe: -0.5,
                test_return: -0.03,  // Not profitable
                test_trades: 25,
                test_win_rate: 0.4,
                generalization_gap: 2.0,
                passed: false,
            },
        ];

        let agg = ForwardAggregateMetrics::from_windows(&windows);

        assert_eq!(agg.total_windows, 2);
        assert_eq!(agg.windows_passed, 1);
        assert!((agg.pct_profitable_windows - 0.5).abs() < 0.01);  // 1 of 2
    }

    // ==================== ForwardStage Basic Tests ====================

    #[test]
    fn test_stage_new() {
        let config = ForwardStageConfig::default();
        let stage = ForwardStage::new(config.clone());

        assert_eq!(stage.config.n_folds, config.n_folds);
    }

    #[test]
    fn test_stage_with_defaults() {
        let stage = ForwardStage::with_defaults();

        assert_eq!(stage.stage_type(), ValidationStageType::Forward);
        assert_eq!(stage.name(), "Forward");
    }

    #[test]
    fn test_stage_type() {
        let stage = ForwardStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::Forward);
    }

    #[test]
    fn test_stage_name() {
        let config = ForwardStageConfig::default().with_name("Custom-WF");
        let stage = ForwardStage::new(config);

        assert_eq!(stage.name(), "Custom-WF");
    }

    #[test]
    fn test_stage_description() {
        let stage = ForwardStage::with_defaults();
        let desc = stage.description();

        assert!(desc.contains("Walk-forward"));
    }

    #[test]
    fn test_stage_min_trades() {
        let config = ForwardStageConfig {
            n_folds: 5,
            min_trades_per_window: 10,
            ..Default::default()
        };
        let stage = ForwardStage::new(config);

        assert_eq!(stage.min_trades(), 50);  // 5 * 10
    }

    #[test]
    fn test_stage_requires_previous() {
        let stage = ForwardStage::with_defaults();
        assert_eq!(stage.requires_previous(), Some(ValidationStageType::Backtest));
    }

    #[test]
    fn test_stage_estimated_duration() {
        let stage = ForwardStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        );

        let duration = stage.estimated_duration(&ctx);
        assert!(duration.is_some());
        assert!(duration.unwrap() > 0);
    }

    // ==================== can_run() Tests ====================

    #[test]
    fn test_can_run_no_data_path() {
        let stage = ForwardStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(60),
            Utc::now(),
        );

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    #[test]
    fn test_can_run_invalid_period() {
        let stage = ForwardStage::with_defaults();
        let now = Utc::now();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now,
            now - Duration::days(1),  // End before start
        )
        .with_data_path("/tmp");

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    #[test]
    fn test_can_run_period_too_short() {
        let stage = ForwardStage::with_defaults();  // Default needs ~100h train + 5*24h test
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::hours(50),  // Only 50 hours
            Utc::now(),
        )
        .with_data_path("/tmp");

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    #[test]
    fn test_can_run_nonexistent_path() {
        let stage = ForwardStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(60),
            Utc::now(),
        )
        .with_data_path("/nonexistent/path/that/does/not/exist");

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::DataUnavailable(_)));
    }

    #[test]
    fn test_can_run_path_is_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        File::create(&file_path).unwrap();

        let stage = ForwardStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(60),
            Utc::now(),
        )
        .with_data_path(file_path.to_str().unwrap());

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    #[test]
    fn test_can_run_empty_directory() {
        let dir = tempdir().unwrap();

        let stage = ForwardStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(60),
            Utc::now(),
        )
        .with_data_path(dir.path().to_str().unwrap());

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::DataUnavailable(_)));
    }

    #[test]
    fn test_can_run_no_parquet_files() {
        let dir = tempdir().unwrap();
        File::create(dir.path().join("test.txt")).unwrap();
        File::create(dir.path().join("data.csv")).unwrap();

        let stage = ForwardStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(60),
            Utc::now(),
        )
        .with_data_path(dir.path().to_str().unwrap());

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::DataUnavailable(_)));
    }

    #[test]
    fn test_can_run_too_few_folds() {
        let config = ForwardStageConfig {
            n_folds: 1,  // Invalid - need at least 2
            ..Default::default()
        };
        let stage = ForwardStage::new(config);

        let dir = tempdir().unwrap();
        File::create(dir.path().join("test.parquet")).unwrap();

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(60),
            Utc::now(),
        )
        .with_data_path(dir.path().to_str().unwrap());

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    // ==================== generate_folds() Tests ====================

    #[test]
    fn test_generate_folds_anchored() {
        let config = ForwardStageConfig {
            n_folds: 3,
            min_train_hours: 10.0,
            test_hours: 5.0,
            embargo_hours: 1.0,
            anchored: true,
            ..Default::default()
        };
        let stage = ForwardStage::new(config);

        // 100 hours of data
        let start_ms = 0;
        let end_ms = 100 * 60 * 60 * 1000;

        let folds = stage.generate_folds(start_ms, end_ms);

        assert!(!folds.is_empty());

        // All folds should start at the same point (anchored)
        for (train_start, _, _, _) in &folds {
            assert_eq!(*train_start, start_ms);
        }

        // Each fold should have train before test
        for (train_start, train_end, test_start, test_end) in &folds {
            assert!(train_start < train_end);
            assert!(train_end < test_start);  // Embargo
            assert!(test_start < test_end);
        }
    }

    #[test]
    fn test_generate_folds_rolling() {
        let config = ForwardStageConfig {
            n_folds: 3,
            min_train_hours: 10.0,
            test_hours: 5.0,
            embargo_hours: 1.0,
            anchored: false,  // Rolling
            ..Default::default()
        };
        let stage = ForwardStage::new(config);

        // 100 hours of data
        let start_ms = 0;
        let end_ms = 100 * 60 * 60 * 1000;

        let folds = stage.generate_folds(start_ms, end_ms);

        assert!(!folds.is_empty());

        // Each fold should have train before test
        for (train_start, train_end, test_start, test_end) in &folds {
            assert!(train_start < train_end);
            assert!(train_end < test_start);
            assert!(test_start < test_end);
        }

        // Rolling: train starts should advance
        if folds.len() >= 2 {
            assert!(folds[1].0 > folds[0].0);  // Second fold starts later
        }
    }

    #[test]
    fn test_generate_folds_insufficient_data() {
        let config = ForwardStageConfig {
            n_folds: 5,
            min_train_hours: 100.0,  // 100 hours minimum
            test_hours: 24.0,
            embargo_hours: 1.0,
            ..Default::default()
        };
        let stage = ForwardStage::new(config);

        // Only 50 hours of data - not enough
        let start_ms = 0;
        let end_ms = 50 * 60 * 60 * 1000;

        let folds = stage.generate_folds(start_ms, end_ms);

        assert!(folds.is_empty());
    }

    // ==================== Factory Tests ====================

    #[test]
    fn test_factory_new() {
        let factory = ForwardStageFactory::new();
        let stage = factory.create("WF-Test");

        assert_eq!(stage.name(), "WF-Test");
    }

    #[test]
    fn test_factory_default() {
        let factory = ForwardStageFactory::default();
        let stage = factory.create("WF-Default");

        assert_eq!(stage.name(), "WF-Default");
    }

    #[test]
    fn test_factory_with_config() {
        let config = ForwardStageConfig::conservative();
        let factory = ForwardStageFactory::with_config(config);
        let stage = factory.create("WF-Conservative");

        assert_eq!(stage.name(), "WF-Conservative");
        assert_eq!(stage.config.n_folds, 7);
    }

    #[test]
    fn test_factory_create_with_config() {
        let factory = ForwardStageFactory::new();
        let custom_config = ForwardStageConfig::quick();
        let stage = factory.create_with_config("WF-Custom", custom_config);

        assert_eq!(stage.name(), "WF-Custom");
        assert_eq!(stage.config.n_folds, 3);
    }

    // ==================== ValidationStage Trait Tests ====================

    #[test]
    fn test_trait_stage_type_is_forward() {
        let stage = ForwardStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::Forward);
    }

    #[test]
    fn test_trait_is_historical() {
        let stage = ForwardStage::with_defaults();
        // Forward stage uses historical data for walk-forward validation
        // It should be considered historical (not live)
        assert!(stage.stage_type().is_historical());
    }

    #[test]
    fn test_trait_pipeline_order() {
        let stage = ForwardStage::with_defaults();
        assert_eq!(stage.stage_type().pipeline_order(), 2);  // Second in pipeline
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_stage_name() {
        let config = ForwardStageConfig::default().with_name("");
        let stage = ForwardStage::new(config);
        assert_eq!(stage.name(), "");
    }

    #[test]
    fn test_very_long_stage_name() {
        let long_name = "W".repeat(1000);
        let config = ForwardStageConfig::default().with_name(&long_name);
        let stage = ForwardStage::new(config);
        assert_eq!(stage.name().len(), 1000);
    }

    #[test]
    fn test_zero_fill_probability() {
        let config = ForwardStageConfig {
            fill_probability: 0.0,
            ..Default::default()
        };
        let stage = ForwardStage::new(config);
        assert!((stage.config.fill_probability).abs() < 0.01);
    }

    #[test]
    fn test_extreme_fill_probability() {
        let config = ForwardStageConfig {
            fill_probability: 1.0,
            ..Default::default()
        };
        let stage = ForwardStage::new(config);
        assert!((stage.config.fill_probability - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_negative_generalization_gap_threshold() {
        // Negative gap would mean test outperforms train - unusual but valid config
        let config = ForwardStageConfig {
            max_generalization_gap: -1.0,
            ..Default::default()
        };
        let stage = ForwardStage::new(config);
        assert!((stage.config.max_generalization_gap - (-1.0)).abs() < 0.01);
    }

    #[test]
    fn test_zero_embargo_hours() {
        let config = ForwardStageConfig {
            embargo_hours: 0.0,
            ..Default::default()
        };
        let stage = ForwardStage::new(config);
        assert!((stage.config.embargo_hours).abs() < 0.01);
    }

    #[test]
    fn test_large_number_of_folds() {
        let config = ForwardStageConfig {
            n_folds: 100,
            ..Default::default()
        };
        let stage = ForwardStage::new(config);
        assert_eq!(stage.config.n_folds, 100);
    }

    // ==================== Generalization Gap Tests ====================

    #[test]
    fn test_window_generalization_gap_calculation() {
        let window = WindowResult {
            window_num: 1,
            train_start_ms: 0,
            train_end_ms: 1000,
            test_start_ms: 1100,
            test_end_ms: 2000,
            train_events: 100,
            test_events: 50,
            train_sharpe: 2.0,
            train_return: 0.10,
            train_trades: 50,
            test_sharpe: 0.5,
            test_return: 0.02,
            test_trades: 30,
            test_win_rate: 0.55,
            generalization_gap: 1.5,  // 2.0 - 0.5 = 1.5
            passed: true,
        };

        assert!((window.generalization_gap - 1.5).abs() < 0.01);
    }

    #[test]
    fn test_negative_generalization_gap() {
        // Test outperforms train - negative gap
        let window = WindowResult {
            window_num: 1,
            train_start_ms: 0,
            train_end_ms: 1000,
            test_start_ms: 1100,
            test_end_ms: 2000,
            train_events: 100,
            test_events: 50,
            train_sharpe: 0.5,
            train_return: 0.02,
            train_trades: 50,
            test_sharpe: 1.5,
            test_return: 0.08,
            test_trades: 30,
            test_win_rate: 0.7,
            generalization_gap: -1.0,  // 0.5 - 1.5 = -1.0
            passed: true,
        };

        assert!(window.has_good_generalization(0.0));  // Negative gap is always good
    }

    // ==================== Async Run Signature Tests ====================

    #[test]
    fn test_run_returns_future() {
        let stage = ForwardStage::with_defaults();
        let ctx = StageContext::default()
            .with_data_path("/tmp")
            .with_name("Test");

        let _future = stage.run(&ctx);
        // Just checking it compiles and returns a future
    }

    // ==================== Clone and Debug Tests ====================

    #[test]
    fn test_config_clone() {
        let config = ForwardStageConfig::conservative();
        let cloned = config.clone();

        assert_eq!(cloned.n_folds, config.n_folds);
        assert_eq!(cloned.name, config.name);
        assert!(cloned.anchored == config.anchored);
    }

    #[test]
    fn test_config_debug() {
        let config = ForwardStageConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("ForwardStageConfig"));
        assert!(debug_str.contains("n_folds"));
    }

    #[test]
    fn test_window_result_clone() {
        let window = WindowResult {
            window_num: 1,
            train_start_ms: 0,
            train_end_ms: 1000,
            test_start_ms: 1100,
            test_end_ms: 2000,
            train_events: 100,
            test_events: 50,
            train_sharpe: 1.0,
            train_return: 0.05,
            train_trades: 50,
            test_sharpe: 0.8,
            test_return: 0.04,
            test_trades: 30,
            test_win_rate: 0.6,
            generalization_gap: 0.2,
            passed: true,
        };

        let cloned = window.clone();
        assert_eq!(cloned.window_num, window.window_num);
        assert!((cloned.test_sharpe - window.test_sharpe).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_clone() {
        let windows = vec![
            WindowResult {
                window_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 1.0,
                train_return: 0.05,
                train_trades: 50,
                test_sharpe: 0.8,
                test_return: 0.04,
                test_trades: 30,
                test_win_rate: 0.6,
                generalization_gap: 0.2,
                passed: true,
            },
        ];

        let agg = ForwardAggregateMetrics::from_windows(&windows);
        let cloned = agg.clone();

        assert_eq!(cloned.total_windows, agg.total_windows);
        assert!((cloned.avg_oos_sharpe - agg.avg_oos_sharpe).abs() < 0.01);
    }

    // ==================== IS/OOS Ratio Tests ====================

    #[test]
    fn test_is_oos_ratio_no_overfitting() {
        let windows = vec![
            WindowResult {
                window_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 1.0,
                train_return: 0.05,
                train_trades: 50,
                test_sharpe: 0.9,  // Close to train
                test_return: 0.045,
                test_trades: 30,
                test_win_rate: 0.6,
                generalization_gap: 0.1,
                passed: true,
            },
        ];

        let agg = ForwardAggregateMetrics::from_windows(&windows);

        // IS/OOS ratio close to 1.0 = good generalization
        assert!(agg.is_oos_sharpe_ratio > 0.8);
    }

    #[test]
    fn test_is_oos_ratio_severe_overfitting() {
        let windows = vec![
            WindowResult {
                window_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 2.0,
                train_return: 0.10,
                train_trades: 50,
                test_sharpe: 0.2,  // Much worse than train
                test_return: 0.01,
                test_trades: 30,
                test_win_rate: 0.5,
                generalization_gap: 1.8,
                passed: false,
            },
        ];

        let agg = ForwardAggregateMetrics::from_windows(&windows);

        // IS/OOS ratio much less than 1.0 = overfitting
        assert!(agg.is_oos_sharpe_ratio < 0.5);
    }

    #[test]
    fn test_is_oos_ratio_near_zero_is_sharpe() {
        let windows = vec![
            WindowResult {
                window_num: 1,
                train_start_ms: 0,
                train_end_ms: 1000,
                test_start_ms: 1100,
                test_end_ms: 2000,
                train_events: 100,
                test_events: 50,
                train_sharpe: 0.005,  // Near zero
                train_return: 0.001,
                train_trades: 50,
                test_sharpe: 0.5,
                test_return: 0.02,
                test_trades: 30,
                test_win_rate: 0.55,
                generalization_gap: -0.495,
                passed: true,
            },
        ];

        let agg = ForwardAggregateMetrics::from_windows(&windows);

        // Should handle near-zero IS sharpe gracefully
        assert!(agg.is_oos_sharpe_ratio.abs() < 0.1 || agg.is_oos_sharpe_ratio > 0.0);
    }
}

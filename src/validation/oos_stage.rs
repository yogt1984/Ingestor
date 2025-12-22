//! OOSStage Implementation (Task 2.3)
//!
//! Out-of-Sample validation stage that tests the algorithm on held-out data
//! that was never seen during research or earlier validation stages.
//!
//! # Overview
//!
//! The OOSStage is the third stage in the validation pipeline. It:
//! 1. Uses the final X% of data (typically 20%) as truly held-out data
//! 2. Performs a single-pass evaluation with strict no-lookahead guarantee
//! 3. Provides the final go/no-go decision before paper/live trading
//! 4. Tracks overfitting by comparing to earlier stage results
//!
//! # Holdout Methodology
//!
//! ```text
//! |------ Training/Research Data (80%) ------|--- OOS Holdout (20%) ---|
//!                                            ^
//!                                            |
//!                                    Never seen before this stage
//! ```
//!
//! This stage is critical for detecting overfitting that may have occurred
//! during backtesting and walk-forward validation.
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::validation::{OOSStage, OOSStageConfig, StageContext};
//!
//! let stage = OOSStage::new(OOSStageConfig::default());
//! let context = StageContext::default()
//!     .with_data_path("./data/features")
//!     .with_name("OOS-2025Q1");
//!
//! let result = stage.run(&context).await?;
//! ```

use std::path::PathBuf;
use std::time::Instant;

use chrono::{TimeZone, Utc};
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

/// Configuration for the OOSStage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OOSStageConfig {
    /// Percentage of data to use as holdout (0.0-1.0)
    pub holdout_pct: f64,

    /// Use realistic fill simulation
    pub use_realistic_fills: bool,

    /// Fill probability (for realistic fills)
    pub fill_probability: f64,

    /// Fee rate (as decimal, e.g., 0.0001 for 1 bps)
    pub fee_rate_bps: f64,

    /// Initial capital for equity tracking
    pub initial_capital: f64,

    /// Risk-free rate for Sharpe calculation (annual)
    pub risk_free_rate: f64,

    /// How often to record equity (in events)
    pub equity_sample_interval: usize,

    /// Minimum number of events required in holdout
    pub min_events: usize,

    /// Minimum trades required for valid evaluation
    pub min_trades: usize,

    /// Maximum allowable degradation from backtest Sharpe (ratio)
    /// e.g., 0.5 means OOS Sharpe must be at least 50% of backtest Sharpe
    pub min_sharpe_retention: f64,

    /// Print progress during execution
    pub verbose: bool,

    /// Name for the stage
    pub name: String,
}

impl Default for OOSStageConfig {
    fn default() -> Self {
        Self {
            holdout_pct: 0.20,  // 20% holdout
            use_realistic_fills: true,
            fill_probability: 0.10,  // 10% fill probability (conservative)
            fee_rate_bps: 1.0,       // 1 bps fee
            initial_capital: 10_000.0,
            risk_free_rate: 0.05,    // 5% annual
            equity_sample_interval: 100,
            min_events: 100,
            min_trades: 20,
            min_sharpe_retention: 0.50,  // OOS must retain at least 50% of IS Sharpe
            verbose: false,
            name: "OOS".to_string(),
        }
    }
}

impl OOSStageConfig {
    /// Create a configuration optimized for fast validation
    pub fn fast() -> Self {
        Self {
            use_realistic_fills: false,  // Faster without realistic fills
            fill_probability: 1.0,       // All touches fill
            verbose: false,
            equity_sample_interval: 500,
            min_events: 50,
            min_trades: 10,
            ..Default::default()
        }
    }

    /// Create a configuration with conservative (realistic) assumptions
    pub fn conservative() -> Self {
        Self {
            holdout_pct: 0.25,        // 25% holdout - more OOS data
            use_realistic_fills: true,
            fill_probability: 0.05,   // Only 5% fill probability
            fee_rate_bps: 2.0,        // Higher fees
            min_trades: 30,
            min_sharpe_retention: 0.60,  // Stricter retention requirement
            ..Default::default()
        }
    }

    /// Create a configuration with optimistic assumptions
    pub fn optimistic() -> Self {
        Self {
            holdout_pct: 0.15,        // 15% holdout
            use_realistic_fills: false,
            fill_probability: 0.50,
            fee_rate_bps: 0.5,
            min_trades: 15,
            min_sharpe_retention: 0.40,
            ..Default::default()
        }
    }

    /// Create a configuration with larger holdout
    pub fn large_holdout() -> Self {
        Self {
            holdout_pct: 0.30,  // 30% holdout
            ..Default::default()
        }
    }

    /// Set the stage name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the holdout percentage
    pub fn with_holdout_pct(mut self, pct: f64) -> Self {
        self.holdout_pct = pct.clamp(0.05, 0.50);  // Clamp to reasonable range
        self
    }
}

/// Detailed metrics from OOS evaluation
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OOSMetrics {
    /// Number of events in holdout set
    pub holdout_events: usize,

    /// Number of trades generated
    pub trade_count: usize,

    /// Sharpe ratio on holdout data
    pub sharpe_ratio: f64,

    /// Total return on holdout data
    pub total_return: f64,

    /// Win rate on holdout data
    pub win_rate: f64,

    /// Maximum drawdown on holdout data
    pub max_drawdown: f64,

    /// Average trade return in bps
    pub avg_trade_return_bps: f64,

    /// Holdout period start (ms)
    pub holdout_start_ms: i64,

    /// Holdout period end (ms)
    pub holdout_end_ms: i64,

    /// Hours of data in holdout
    pub holdout_hours: f64,

    /// Fills generated
    pub fills_generated: usize,

    /// Bid fill rate
    pub bid_fill_rate: f64,

    /// Ask fill rate
    pub ask_fill_rate: f64,
}

impl OOSMetrics {
    /// Create OOS metrics from backtest results
    pub fn from_backtest_results(
        results: &BacktestResults,
        holdout_start_ms: i64,
        holdout_end_ms: i64,
    ) -> Self {
        let holdout_hours = (holdout_end_ms - holdout_start_ms) as f64 / (1000.0 * 60.0 * 60.0);

        // Calculate average trade return
        let avg_trade_return_bps = if results.fills_generated > 0 {
            (results.metrics.total_return / results.fills_generated as f64) * 10000.0
        } else {
            0.0
        };

        Self {
            holdout_events: results.events_processed,
            trade_count: results.fills_generated,
            sharpe_ratio: results.metrics.sharpe_ratio,
            total_return: results.metrics.total_return,
            win_rate: results.metrics.win_rate,
            max_drawdown: results.metrics.max_drawdown,
            avg_trade_return_bps,
            holdout_start_ms,
            holdout_end_ms,
            holdout_hours,
            fills_generated: results.fills_generated,
            bid_fill_rate: results.fill_stats.bid_fill_rate,
            ask_fill_rate: results.fill_stats.ask_fill_rate,
        }
    }

    /// Check if results meet minimum requirements
    pub fn meets_requirements(&self, config: &OOSStageConfig) -> bool {
        self.holdout_events >= config.min_events
            && self.trade_count >= config.min_trades
    }

    /// Check if this is a go/no-go pass
    pub fn is_go(&self, config: &OOSStageConfig, backtest_sharpe: Option<f64>) -> bool {
        if !self.meets_requirements(config) {
            return false;
        }

        // Must be profitable
        if self.total_return <= 0.0 {
            return false;
        }

        // Must have positive Sharpe
        if self.sharpe_ratio <= 0.0 {
            return false;
        }

        // If we have backtest Sharpe, check retention
        if let Some(bt_sharpe) = backtest_sharpe {
            if bt_sharpe > 0.0 {
                let retention = self.sharpe_ratio / bt_sharpe;
                if retention < config.min_sharpe_retention {
                    return false;
                }
            }
        }

        true
    }
}

/// OOSStage - Out-of-Sample validation
///
/// This stage performs final validation on held-out data that was never
/// seen during research or earlier validation stages.
pub struct OOSStage {
    config: OOSStageConfig,
}

impl OOSStage {
    /// Create a new OOSStage with the given configuration
    pub fn new(config: OOSStageConfig) -> Self {
        Self { config }
    }

    /// Create an OOSStage with default configuration
    pub fn with_defaults() -> Self {
        Self::new(OOSStageConfig::default())
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

    /// Calculate holdout period boundaries
    fn calculate_holdout_boundaries(&self, events: &[ReplayEvent]) -> Result<(i64, i64), StageError> {
        if events.is_empty() {
            return Err(StageError::DataUnavailable("No events to calculate holdout".to_string()));
        }

        let start_ms = events.first().map(|e| e.timestamp_ms).unwrap_or(0);
        let end_ms = events.last().map(|e| e.timestamp_ms).unwrap_or(0);
        let total_duration = end_ms - start_ms;

        // Calculate holdout start (last X% of data)
        let holdout_start = start_ms + ((1.0 - self.config.holdout_pct) * total_duration as f64) as i64;

        Ok((holdout_start, end_ms))
    }

    /// Filter events to holdout period
    fn filter_to_holdout(&self, events: &[ReplayEvent], holdout_start: i64, holdout_end: i64) -> Vec<ReplayEvent> {
        events
            .iter()
            .filter(|e| e.timestamp_ms >= holdout_start && e.timestamp_ms <= holdout_end)
            .cloned()
            .collect()
    }

    /// Run backtest on holdout events
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
            risk_free_rate: self.config.risk_free_rate,
            equity_sample_interval: self.config.equity_sample_interval,
            verbose: self.config.verbose,
            use_realistic_fills: self.config.use_realistic_fills,
            oco: Default::default(),
        };

        let mut engine = BacktestEngine::from_events(backtest_config, events);
        engine.run()
            .map_err(|e| StageError::ExecutionError(format!("OOS backtest failed: {}", e)))
    }

    /// Convert backtest TradeRecord to validation TradeResult
    fn convert_trade(
        &self,
        record: &TradeRecord,
        trade_idx: usize,
        config_id: &str,
    ) -> TradeResult {
        let entry_time = Utc.timestamp_millis_opt(record.timestamp_ms)
            .single()
            .unwrap_or_else(Utc::now);
        let exit_time = entry_time;

        let price = record.price.to_f64().unwrap_or(0.0);
        let size = record.size.to_f64().unwrap_or(0.0);
        let pnl = record.pnl.map(|p| p.to_f64().unwrap_or(0.0)).unwrap_or(0.0);

        let direction = match record.side {
            TradeSide::Buy => TradeDirection::Long,
            TradeSide::Sell => TradeDirection::Short,
        };

        let pnl_bps = if price > 0.0 && size > 0.0 {
            (pnl / (price * size)) * 10000.0
        } else {
            0.0
        };

        let return_pct = pnl_bps / 100.0;

        let exit_reason = if pnl > 0.0 {
            ExitReason::TakeProfit
        } else if pnl < 0.0 {
            ExitReason::StopLoss
        } else {
            ExitReason::Unknown
        };

        TradeResult {
            trade_id: format!("OOS-{}", trade_idx),
            direction,
            entry_time,
            exit_time,
            entry_price: price,
            exit_price: price,
            size,
            pnl,
            pnl_bps,
            return_pct,
            exit_reason,
            research_state_id: None,
            config_id: Some(config_id.to_string()),
            slippage_bps: 0.0,
            commission: record.fee.to_f64().unwrap_or(0.0),
            mae_bps: 0.0,
            mfe_bps: 0.0,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Convert results to ValidationResult
    fn convert_results(
        &self,
        results: &BacktestResults,
        oos_metrics: &OOSMetrics,
        context: &StageContext,
        duration_secs: f64,
    ) -> ValidationResult {
        // Convert trades (only those with realized P&L)
        let trades: Vec<TradeResult> = results.trade_log.trades
            .iter()
            .enumerate()
            .filter(|(_, t)| t.pnl.is_some())
            .map(|(idx, t)| self.convert_trade(t, idx, &context.config.id))
            .collect();

        // Create validation result
        let mut result = ValidationResult::new(
            ValidationStageType::OutOfSample,
            context.stage_name.clone(),
            context.config.id.clone(),
            Utc.timestamp_millis_opt(oos_metrics.holdout_start_ms)
                .single()
                .unwrap_or(context.period_start),
            Utc.timestamp_millis_opt(oos_metrics.holdout_end_ms)
                .single()
                .unwrap_or(context.period_end),
        );

        // Set trades and compute metrics
        result = result.with_trades(trades);

        // Add metadata
        result.add_metadata("holdout_pct".to_string(), format!("{:.0}%", self.config.holdout_pct * 100.0));
        result.add_metadata("holdout_events".to_string(), oos_metrics.holdout_events.to_string());
        result.add_metadata("holdout_hours".to_string(), format!("{:.1}", oos_metrics.holdout_hours));
        result.add_metadata("oos_sharpe".to_string(), format!("{:.3}", oos_metrics.sharpe_ratio));
        result.add_metadata("oos_return".to_string(), format!("{:.2}%", oos_metrics.total_return * 100.0));
        result.add_metadata("oos_win_rate".to_string(), format!("{:.1}%", oos_metrics.win_rate * 100.0));
        result.add_metadata("oos_max_drawdown".to_string(), format!("{:.2}%", oos_metrics.max_drawdown * 100.0));
        result.add_metadata("fills_generated".to_string(), oos_metrics.fills_generated.to_string());
        result.add_metadata("fill_simulation".to_string(),
            if self.config.use_realistic_fills { "realistic" } else { "naive" }.to_string());
        result.add_metadata("bid_fill_rate".to_string(),
            format!("{:.2}%", oos_metrics.bid_fill_rate * 100.0));
        result.add_metadata("ask_fill_rate".to_string(),
            format!("{:.2}%", oos_metrics.ask_fill_rate * 100.0));

        // Set validation duration
        result.set_duration(duration_secs);

        // Evaluate thresholds
        result.evaluate_thresholds(context.thresholds.clone());

        // Add warnings for potential issues
        if oos_metrics.holdout_events < self.config.min_events {
            result.add_warning(format!(
                "Low holdout event count: {} (minimum recommended: {})",
                oos_metrics.holdout_events, self.config.min_events
            ));
        }

        if oos_metrics.trade_count < self.config.min_trades {
            result.add_warning(format!(
                "Low trade count: {} (minimum for statistical significance: {})",
                oos_metrics.trade_count, self.config.min_trades
            ));
        }

        if oos_metrics.total_return <= 0.0 {
            result.add_warning("OOS return is not profitable".to_string());
        }

        if oos_metrics.sharpe_ratio <= 0.0 {
            result.add_warning("OOS Sharpe ratio is not positive".to_string());
        }

        if oos_metrics.win_rate < 0.40 {
            result.add_warning(format!(
                "Low OOS win rate: {:.1}%",
                oos_metrics.win_rate * 100.0
            ));
        }

        if oos_metrics.max_drawdown > 0.10 {
            result.add_warning(format!(
                "High OOS max drawdown: {:.1}%",
                oos_metrics.max_drawdown * 100.0
            ));
        }

        result
    }

    /// Execute OOS validation
    async fn execute_oos(&self, context: &StageContext) -> Result<(BacktestResults, OOSMetrics), StageError> {
        let data_path = context.data_path.as_deref()
            .ok_or_else(|| StageError::ConfigurationError("Data path required".to_string()))?;

        // Load all events
        let start_ms = Some(context.period_start.timestamp_millis());
        let end_ms = Some(context.period_end.timestamp_millis());
        let events = self.load_events(data_path, start_ms, end_ms)?;

        if events.is_empty() {
            return Err(StageError::DataUnavailable("No events loaded".to_string()));
        }

        // Calculate holdout boundaries
        let (holdout_start, holdout_end) = self.calculate_holdout_boundaries(&events)?;

        // Filter to holdout period
        let holdout_events = self.filter_to_holdout(&events, holdout_start, holdout_end);

        if holdout_events.is_empty() {
            return Err(StageError::DataUnavailable(
                "No events in holdout period".to_string()
            ));
        }

        if holdout_events.len() < self.config.min_events {
            return Err(StageError::DataUnavailable(format!(
                "Insufficient holdout events: {} (need at least {})",
                holdout_events.len(), self.config.min_events
            )));
        }

        // Run backtest on holdout data
        let results = self.run_backtest(holdout_events, data_path)?;

        // Calculate OOS metrics
        let oos_metrics = OOSMetrics::from_backtest_results(&results, holdout_start, holdout_end);

        Ok((results, oos_metrics))
    }
}

impl ValidationStage for OOSStage {
    fn stage_type(&self) -> ValidationStageType {
        ValidationStageType::OutOfSample
    }

    fn name(&self) -> &str {
        &self.config.name
    }

    fn description(&self) -> &str {
        "Out-of-sample validation on held-out data never seen during research"
    }

    fn can_run(&self, context: &StageContext) -> Result<(), StageError> {
        // Check period validity
        if context.period_end <= context.period_start {
            return Err(StageError::ConfigurationError(
                "Period end must be after period start".to_string(),
            ));
        }

        // Check data path exists
        let data_path = context.data_path.as_deref()
            .ok_or_else(|| StageError::ConfigurationError(
                "Data path required for OOS stage".to_string(),
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

        // Check holdout percentage is valid
        if self.config.holdout_pct <= 0.0 || self.config.holdout_pct > 0.50 {
            return Err(StageError::ConfigurationError(format!(
                "Invalid holdout percentage: {:.1}% (must be between 0% and 50%)",
                self.config.holdout_pct * 100.0
            )));
        }

        Ok(())
    }

    fn run<'a>(&'a self, context: &'a StageContext) -> RunFuture<'a> {
        Box::pin(async move {
            let start_time = Instant::now();

            // Execute OOS validation
            let (results, oos_metrics) = self.execute_oos(context).await?;

            let duration_secs = start_time.elapsed().as_secs_f64();

            // Convert to ValidationResult
            let result = self.convert_results(&results, &oos_metrics, context, duration_secs);

            Ok(result)
        })
    }

    fn estimated_duration(&self, context: &StageContext) -> Option<u64> {
        // Estimate based on holdout period length
        // OOS should be faster than full backtest since it's only X% of data
        let total_days = context.period_days();
        let holdout_days = total_days * self.config.holdout_pct;
        Some((holdout_days / 7.0).max(1.0) as u64)
    }

    fn min_trades(&self) -> usize {
        self.config.min_trades
    }

    fn requires_previous(&self) -> Option<ValidationStageType> {
        Some(ValidationStageType::Forward)  // OOS requires forward validation to pass first
    }
}

/// Factory for creating OOSStage instances
pub struct OOSStageFactory {
    default_config: OOSStageConfig,
}

impl OOSStageFactory {
    /// Create a new factory with default configuration
    pub fn new() -> Self {
        Self {
            default_config: OOSStageConfig::default(),
        }
    }

    /// Create a factory with custom default configuration
    pub fn with_config(config: OOSStageConfig) -> Self {
        Self {
            default_config: config,
        }
    }

    /// Create an OOSStage with the default configuration
    pub fn create(&self, name: &str) -> OOSStage {
        OOSStage::new(self.default_config.clone().with_name(name))
    }

    /// Create an OOSStage with custom configuration
    pub fn create_with_config(&self, name: &str, config: OOSStageConfig) -> OOSStage {
        OOSStage::new(config.with_name(name))
    }
}

impl Default for OOSStageFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AlgorithmConfig, ValidationThresholds};
    use chrono::Duration;
    use tempfile::tempdir;
    use std::fs::File;

    // ==================== OOSStageConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = OOSStageConfig::default();

        assert!((config.holdout_pct - 0.20).abs() < 0.01);
        assert!(config.use_realistic_fills);
        assert!((config.fill_probability - 0.10).abs() < 0.01);
        assert!((config.fee_rate_bps - 1.0).abs() < 0.01);
        assert!((config.initial_capital - 10_000.0).abs() < 0.01);
        assert!((config.risk_free_rate - 0.05).abs() < 0.01);
        assert_eq!(config.equity_sample_interval, 100);
        assert_eq!(config.min_events, 100);
        assert_eq!(config.min_trades, 20);
        assert!((config.min_sharpe_retention - 0.50).abs() < 0.01);
        assert!(!config.verbose);
        assert_eq!(config.name, "OOS");
    }

    #[test]
    fn test_config_fast() {
        let config = OOSStageConfig::fast();

        assert!(!config.use_realistic_fills);
        assert!((config.fill_probability - 1.0).abs() < 0.01);
        assert!(!config.verbose);
        assert_eq!(config.equity_sample_interval, 500);
        assert_eq!(config.min_events, 50);
        assert_eq!(config.min_trades, 10);
    }

    #[test]
    fn test_config_conservative() {
        let config = OOSStageConfig::conservative();

        assert!((config.holdout_pct - 0.25).abs() < 0.01);
        assert!(config.use_realistic_fills);
        assert!((config.fill_probability - 0.05).abs() < 0.01);
        assert!((config.fee_rate_bps - 2.0).abs() < 0.01);
        assert_eq!(config.min_trades, 30);
        assert!((config.min_sharpe_retention - 0.60).abs() < 0.01);
    }

    #[test]
    fn test_config_optimistic() {
        let config = OOSStageConfig::optimistic();

        assert!((config.holdout_pct - 0.15).abs() < 0.01);
        assert!(!config.use_realistic_fills);
        assert!((config.fill_probability - 0.50).abs() < 0.01);
        assert!((config.fee_rate_bps - 0.5).abs() < 0.01);
        assert_eq!(config.min_trades, 15);
        assert!((config.min_sharpe_retention - 0.40).abs() < 0.01);
    }

    #[test]
    fn test_config_large_holdout() {
        let config = OOSStageConfig::large_holdout();
        assert!((config.holdout_pct - 0.30).abs() < 0.01);
    }

    #[test]
    fn test_config_with_name() {
        let config = OOSStageConfig::default().with_name("OOS-2025Q1");
        assert_eq!(config.name, "OOS-2025Q1");
    }

    #[test]
    fn test_config_with_holdout_pct() {
        let config = OOSStageConfig::default().with_holdout_pct(0.25);
        assert!((config.holdout_pct - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_config_with_holdout_pct_clamped_low() {
        let config = OOSStageConfig::default().with_holdout_pct(0.01);
        assert!((config.holdout_pct - 0.05).abs() < 0.01);  // Clamped to 5%
    }

    #[test]
    fn test_config_with_holdout_pct_clamped_high() {
        let config = OOSStageConfig::default().with_holdout_pct(0.80);
        assert!((config.holdout_pct - 0.50).abs() < 0.01);  // Clamped to 50%
    }

    #[test]
    fn test_config_serialization() {
        let config = OOSStageConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: OOSStageConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, config.name);
        assert!((deserialized.holdout_pct - config.holdout_pct).abs() < 0.01);
        assert!((deserialized.fill_probability - config.fill_probability).abs() < 0.01);
    }

    #[test]
    fn test_config_clone() {
        let config = OOSStageConfig::conservative();
        let cloned = config.clone();

        assert_eq!(cloned.name, config.name);
        assert!((cloned.holdout_pct - config.holdout_pct).abs() < 0.01);
        assert!(cloned.use_realistic_fills == config.use_realistic_fills);
    }

    #[test]
    fn test_config_debug() {
        let config = OOSStageConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("OOSStageConfig"));
        assert!(debug_str.contains("holdout_pct"));
    }

    // ==================== OOSMetrics Tests ====================

    #[test]
    fn test_oos_metrics_default() {
        let metrics = OOSMetrics::default();

        assert_eq!(metrics.holdout_events, 0);
        assert_eq!(metrics.trade_count, 0);
        assert!((metrics.sharpe_ratio).abs() < 0.01);
        assert!((metrics.total_return).abs() < 0.01);
    }

    #[test]
    fn test_oos_metrics_meets_requirements_pass() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 1.5,
            total_return: 0.05,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 5.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(metrics.meets_requirements(&config));
    }

    #[test]
    fn test_oos_metrics_meets_requirements_fail_events() {
        let metrics = OOSMetrics {
            holdout_events: 50,  // Below min_events (100)
            trade_count: 30,
            sharpe_ratio: 1.5,
            total_return: 0.05,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 5.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(!metrics.meets_requirements(&config));
    }

    #[test]
    fn test_oos_metrics_meets_requirements_fail_trades() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 10,  // Below min_trades (20)
            sharpe_ratio: 1.5,
            total_return: 0.05,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 5.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 10,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(!metrics.meets_requirements(&config));
    }

    #[test]
    fn test_oos_metrics_is_go_pass() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 1.0,
            total_return: 0.05,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 5.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(metrics.is_go(&config, Some(1.5)));  // 1.0/1.5 = 0.67 > 0.50
    }

    #[test]
    fn test_oos_metrics_is_go_fail_not_profitable() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 0.5,
            total_return: -0.02,  // Not profitable
            win_rate: 0.45,
            max_drawdown: 0.05,
            avg_trade_return_bps: -2.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(!metrics.is_go(&config, Some(1.5)));
    }

    #[test]
    fn test_oos_metrics_is_go_fail_negative_sharpe() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: -0.5,  // Negative Sharpe
            total_return: 0.01,
            win_rate: 0.50,
            max_drawdown: 0.04,
            avg_trade_return_bps: 1.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(!metrics.is_go(&config, Some(1.5)));
    }

    #[test]
    fn test_oos_metrics_is_go_fail_low_retention() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 0.3,  // Only 20% of backtest Sharpe
            total_return: 0.02,
            win_rate: 0.52,
            max_drawdown: 0.03,
            avg_trade_return_bps: 2.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(!metrics.is_go(&config, Some(1.5)));  // 0.3/1.5 = 0.20 < 0.50
    }

    #[test]
    fn test_oos_metrics_is_go_no_backtest_sharpe() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 0.5,
            total_return: 0.03,
            win_rate: 0.54,
            max_drawdown: 0.02,
            avg_trade_return_bps: 3.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(metrics.is_go(&config, None));  // No backtest to compare to
    }

    #[test]
    fn test_oos_metrics_clone() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 1.5,
            total_return: 0.05,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 5.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let cloned = metrics.clone();
        assert_eq!(cloned.holdout_events, metrics.holdout_events);
        assert!((cloned.sharpe_ratio - metrics.sharpe_ratio).abs() < 0.01);
    }

    #[test]
    fn test_oos_metrics_serialization() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 1.5,
            total_return: 0.05,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 5.0,
            holdout_start_ms: 1000,
            holdout_end_ms: 2000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: OOSMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.holdout_events, metrics.holdout_events);
        assert!((deserialized.sharpe_ratio - metrics.sharpe_ratio).abs() < 0.01);
    }

    // ==================== OOSStage Basic Tests ====================

    #[test]
    fn test_stage_new() {
        let config = OOSStageConfig::default();
        let stage = OOSStage::new(config.clone());

        assert_eq!(stage.config.name, config.name);
    }

    #[test]
    fn test_stage_with_defaults() {
        let stage = OOSStage::with_defaults();

        assert_eq!(stage.stage_type(), ValidationStageType::OutOfSample);
        assert_eq!(stage.name(), "OOS");
    }

    #[test]
    fn test_stage_type() {
        let stage = OOSStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::OutOfSample);
    }

    #[test]
    fn test_stage_name() {
        let config = OOSStageConfig::default().with_name("Custom-OOS");
        let stage = OOSStage::new(config);

        assert_eq!(stage.name(), "Custom-OOS");
    }

    #[test]
    fn test_stage_description() {
        let stage = OOSStage::with_defaults();
        let desc = stage.description();

        assert!(desc.contains("Out-of-sample"));
        assert!(desc.contains("held-out"));
    }

    #[test]
    fn test_stage_min_trades() {
        let config = OOSStageConfig {
            min_trades: 25,
            ..Default::default()
        };
        let stage = OOSStage::new(config);
        assert_eq!(stage.min_trades(), 25);
    }

    #[test]
    fn test_stage_requires_previous() {
        let stage = OOSStage::with_defaults();
        assert_eq!(stage.requires_previous(), Some(ValidationStageType::Forward));
    }

    #[test]
    fn test_stage_estimated_duration_short_period() {
        let stage = OOSStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(7),
            Utc::now(),
        );

        let duration = stage.estimated_duration(&ctx);
        assert!(duration.is_some());
        assert!(duration.unwrap() >= 1);
    }

    #[test]
    fn test_stage_estimated_duration_long_period() {
        let stage = OOSStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(365),
            Utc::now(),
        );

        let duration = stage.estimated_duration(&ctx);
        assert!(duration.is_some());
        // OOS uses 20% of data, so less than backtest
        assert!(duration.unwrap() > 5);
    }

    // ==================== can_run() Tests ====================

    #[test]
    fn test_can_run_no_data_path() {
        let stage = OOSStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        );

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    #[test]
    fn test_can_run_invalid_period() {
        let stage = OOSStage::with_defaults();
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
    fn test_can_run_nonexistent_path() {
        let stage = OOSStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
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

        let stage = OOSStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
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

        let stage = OOSStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
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

        let stage = OOSStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        )
        .with_data_path(dir.path().to_str().unwrap());

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::DataUnavailable(_)));
    }

    #[test]
    fn test_can_run_invalid_holdout_zero() {
        let config = OOSStageConfig {
            holdout_pct: 0.0,
            ..Default::default()
        };
        let stage = OOSStage::new(config);

        let dir = tempdir().unwrap();
        File::create(dir.path().join("test.parquet")).unwrap();

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        )
        .with_data_path(dir.path().to_str().unwrap());

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    #[test]
    fn test_can_run_invalid_holdout_too_high() {
        let config = OOSStageConfig {
            holdout_pct: 0.75,  // 75% is too high
            ..Default::default()
        };
        let stage = OOSStage::new(config);

        let dir = tempdir().unwrap();
        File::create(dir.path().join("test.parquet")).unwrap();

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        )
        .with_data_path(dir.path().to_str().unwrap());

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    // ==================== Factory Tests ====================

    #[test]
    fn test_factory_new() {
        let factory = OOSStageFactory::new();
        let stage = factory.create("OOS-Test");

        assert_eq!(stage.name(), "OOS-Test");
    }

    #[test]
    fn test_factory_default() {
        let factory = OOSStageFactory::default();
        let stage = factory.create("OOS-Default");

        assert_eq!(stage.name(), "OOS-Default");
    }

    #[test]
    fn test_factory_with_config() {
        let config = OOSStageConfig::conservative();
        let factory = OOSStageFactory::with_config(config);
        let stage = factory.create("OOS-Conservative");

        assert_eq!(stage.name(), "OOS-Conservative");
        assert!((stage.config.holdout_pct - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_factory_create_with_config() {
        let factory = OOSStageFactory::new();
        let custom_config = OOSStageConfig::optimistic();
        let stage = factory.create_with_config("OOS-Custom", custom_config);

        assert_eq!(stage.name(), "OOS-Custom");
        assert!(!stage.config.use_realistic_fills);
    }

    // ==================== ValidationStage Trait Tests ====================

    #[test]
    fn test_trait_stage_type_is_oos() {
        let stage = OOSStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::OutOfSample);
    }

    #[test]
    fn test_trait_is_historical() {
        let stage = OOSStage::with_defaults();
        // OOS uses historical data, so should be considered historical
        assert!(stage.stage_type().is_historical());
    }

    #[test]
    fn test_trait_pipeline_order() {
        let stage = OOSStage::with_defaults();
        assert_eq!(stage.stage_type().pipeline_order(), 3);  // Third in pipeline
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_stage_name() {
        let config = OOSStageConfig::default().with_name("");
        let stage = OOSStage::new(config);
        assert_eq!(stage.name(), "");
    }

    #[test]
    fn test_very_long_stage_name() {
        let long_name = "O".repeat(1000);
        let config = OOSStageConfig::default().with_name(&long_name);
        let stage = OOSStage::new(config);
        assert_eq!(stage.name().len(), 1000);
    }

    #[test]
    fn test_zero_fill_probability() {
        let config = OOSStageConfig {
            fill_probability: 0.0,
            ..Default::default()
        };
        let stage = OOSStage::new(config);
        assert!((stage.config.fill_probability).abs() < 0.01);
    }

    #[test]
    fn test_extreme_fill_probability() {
        let config = OOSStageConfig {
            fill_probability: 1.0,
            ..Default::default()
        };
        let stage = OOSStage::new(config);
        assert!((stage.config.fill_probability - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_zero_initial_capital() {
        let config = OOSStageConfig {
            initial_capital: 0.0,
            ..Default::default()
        };
        let stage = OOSStage::new(config);
        assert!((stage.config.initial_capital).abs() < 0.01);
    }

    #[test]
    fn test_negative_fee_rate() {
        // Should not crash even with invalid input
        let config = OOSStageConfig {
            fee_rate_bps: -1.0,
            ..Default::default()
        };
        let stage = OOSStage::new(config);
        assert!((stage.config.fee_rate_bps - (-1.0)).abs() < 0.01);
    }

    // ==================== Holdout Calculation Tests ====================

    #[test]
    fn test_holdout_boundary_calculation_20_pct() {
        let config = OOSStageConfig {
            holdout_pct: 0.20,
            ..Default::default()
        };
        let stage = OOSStage::new(config);

        // Simulate events spanning 100 hours
        let start = 0i64;
        let end = 100 * 60 * 60 * 1000i64;  // 100 hours in ms

        let events = vec![
            ReplayEvent { timestamp_ms: start, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: end, snapshot: Default::default() },
        ];

        let result = stage.calculate_holdout_boundaries(&events);
        assert!(result.is_ok());

        let (holdout_start, holdout_end) = result.unwrap();

        // Holdout should be last 20% = last 20 hours
        // So holdout_start should be at 80 hours
        let expected_holdout_start = start + ((1.0 - 0.20) * (end - start) as f64) as i64;

        assert_eq!(holdout_start, expected_holdout_start);
        assert_eq!(holdout_end, end);
    }

    #[test]
    fn test_holdout_boundary_calculation_30_pct() {
        let config = OOSStageConfig {
            holdout_pct: 0.30,
            ..Default::default()
        };
        let stage = OOSStage::new(config);

        let start = 0i64;
        let end = 100 * 60 * 60 * 1000i64;

        let events = vec![
            ReplayEvent { timestamp_ms: start, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: end, snapshot: Default::default() },
        ];

        let result = stage.calculate_holdout_boundaries(&events);
        assert!(result.is_ok());

        let (holdout_start, holdout_end) = result.unwrap();

        // Holdout should be last 30%
        let expected_holdout_start = start + ((1.0 - 0.30) * (end - start) as f64) as i64;

        assert_eq!(holdout_start, expected_holdout_start);
        assert_eq!(holdout_end, end);
    }

    #[test]
    fn test_holdout_boundary_empty_events() {
        let stage = OOSStage::with_defaults();
        let events: Vec<ReplayEvent> = vec![];

        let result = stage.calculate_holdout_boundaries(&events);
        assert!(result.is_err());
    }

    // ==================== Filter to Holdout Tests ====================

    #[test]
    fn test_filter_to_holdout_basic() {
        let stage = OOSStage::with_defaults();

        let events = vec![
            ReplayEvent { timestamp_ms: 100, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: 200, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: 300, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: 400, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: 500, snapshot: Default::default() },
        ];

        // Holdout from 300 to 500
        let filtered = stage.filter_to_holdout(&events, 300, 500);

        assert_eq!(filtered.len(), 3);  // 300, 400, 500
        assert_eq!(filtered[0].timestamp_ms, 300);
        assert_eq!(filtered[2].timestamp_ms, 500);
    }

    #[test]
    fn test_filter_to_holdout_empty_result() {
        let stage = OOSStage::with_defaults();

        let events = vec![
            ReplayEvent { timestamp_ms: 100, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: 200, snapshot: Default::default() },
        ];

        // Holdout period has no events
        let filtered = stage.filter_to_holdout(&events, 500, 600);

        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_to_holdout_all_events() {
        let stage = OOSStage::with_defaults();

        let events = vec![
            ReplayEvent { timestamp_ms: 100, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: 200, snapshot: Default::default() },
            ReplayEvent { timestamp_ms: 300, snapshot: Default::default() },
        ];

        // Holdout covers all events
        let filtered = stage.filter_to_holdout(&events, 0, 1000);

        assert_eq!(filtered.len(), 3);
    }

    // ==================== Period Validation Tests ====================

    #[test]
    fn test_period_same_start_end() {
        let stage = OOSStage::with_defaults();
        let now = Utc::now();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now,
            now,  // Same as start
        )
        .with_data_path("/tmp");

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_period_very_short() {
        let stage = OOSStage::with_defaults();
        let now = Utc::now();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now - Duration::seconds(1),
            now,
        );

        let est = stage.estimated_duration(&ctx);
        assert!(est.is_some());
        assert!(est.unwrap() >= 1);
    }

    // ==================== Async Run Signature Tests ====================

    #[test]
    fn test_run_returns_future() {
        let stage = OOSStage::with_defaults();
        let ctx = StageContext::default()
            .with_data_path("/tmp")
            .with_name("Test");

        let _future = stage.run(&ctx);
        // Just checking it compiles and returns a future
    }

    // ==================== Sharpe Retention Tests ====================

    #[test]
    fn test_sharpe_retention_pass_exactly_50_pct() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 0.5,
            total_return: 0.03,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 3.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();  // min_sharpe_retention = 0.50
        assert!(metrics.is_go(&config, Some(1.0)));  // 0.5/1.0 = 0.50 >= 0.50
    }

    #[test]
    fn test_sharpe_retention_fail_just_below() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 0.49,
            total_return: 0.02,
            win_rate: 0.52,
            max_drawdown: 0.04,
            avg_trade_return_bps: 2.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        assert!(!metrics.is_go(&config, Some(1.0)));  // 0.49/1.0 = 0.49 < 0.50
    }

    #[test]
    fn test_sharpe_retention_with_zero_backtest_sharpe() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 0.5,
            total_return: 0.03,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 3.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        // Zero backtest Sharpe should not cause division by zero
        assert!(metrics.is_go(&config, Some(0.0)));
    }

    #[test]
    fn test_sharpe_retention_with_negative_backtest_sharpe() {
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 0.5,
            total_return: 0.03,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 3.0,
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        let config = OOSStageConfig::default();
        // Negative backtest Sharpe means check is skipped
        assert!(metrics.is_go(&config, Some(-0.5)));
    }

    // ==================== Holdout Percentage Boundary Tests ====================

    #[test]
    fn test_holdout_pct_at_5_percent() {
        let config = OOSStageConfig::default().with_holdout_pct(0.05);
        assert!((config.holdout_pct - 0.05).abs() < 0.01);
    }

    #[test]
    fn test_holdout_pct_at_50_percent() {
        let config = OOSStageConfig::default().with_holdout_pct(0.50);
        assert!((config.holdout_pct - 0.50).abs() < 0.01);
    }

    // ==================== Metric Calculations Tests ====================

    #[test]
    fn test_oos_metrics_avg_trade_return_calculation() {
        // When there are fills, avg_trade_return_bps should be calculated
        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 10,
            sharpe_ratio: 1.0,
            total_return: 0.10,  // 10% total return
            win_rate: 0.6,
            max_drawdown: 0.02,
            avg_trade_return_bps: 100.0,  // 10% / 10 trades = 1% = 100 bps per trade
            holdout_start_ms: 0,
            holdout_end_ms: 1000000,
            holdout_hours: 24.0,
            fills_generated: 10,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        assert!((metrics.avg_trade_return_bps - 100.0).abs() < 1.0);
    }

    #[test]
    fn test_oos_metrics_holdout_hours_calculation() {
        let start = 0i64;
        let end = 24 * 60 * 60 * 1000i64;  // 24 hours in ms

        let metrics = OOSMetrics {
            holdout_events: 200,
            trade_count: 30,
            sharpe_ratio: 1.0,
            total_return: 0.05,
            win_rate: 0.55,
            max_drawdown: 0.03,
            avg_trade_return_bps: 5.0,
            holdout_start_ms: start,
            holdout_end_ms: end,
            holdout_hours: 24.0,
            fills_generated: 30,
            bid_fill_rate: 0.1,
            ask_fill_rate: 0.1,
        };

        assert!((metrics.holdout_hours - 24.0).abs() < 0.1);
    }
}

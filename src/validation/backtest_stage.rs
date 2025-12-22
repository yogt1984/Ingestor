//! BacktestStage Implementation (Task 2.1)
//!
//! Historical replay validation stage that loads features from Parquet,
//! runs the algorithm through the data, and produces a ValidationResult.
//!
//! # Overview
//!
//! The BacktestStage is the first stage in the validation pipeline. It:
//! 1. Loads historical feature data from Parquet files
//! 2. Replays data through the market making algorithm
//! 3. Tracks all trades and outcomes
//! 4. Computes performance metrics (Sharpe, drawdown, win rate, etc.)
//! 5. Produces a ValidationResult with pass/fail based on thresholds
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::validation::{BacktestStage, BacktestStageConfig, StageContext};
//!
//! let stage = BacktestStage::new(BacktestStageConfig::default());
//! let context = StageContext::default()
//!     .with_data_path("./data/features")
//!     .with_name("BT-2025Q1");
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
    ReplayConfig, FillSimulatorConfig,
};
use crate::backtest::metrics::{TradeRecord, TradeSide};
use crate::core::{
    ValidationResult, ValidationStageType,
    TradeResult, TradeDirection, ExitReason,
};
use crate::execution::market_maker::MMConfig;
use crate::execution::mm_simulator::SimulatorConfig;

use super::traits::{ValidationStage, StageContext, StageError, RunFuture};

/// Configuration for the BacktestStage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestStageConfig {
    /// Use realistic fill simulation (vs naive)
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

    /// Print progress during execution
    pub verbose: bool,

    /// Minimum number of events required
    pub min_events: usize,

    /// Name for the stage
    pub name: String,
}

impl Default for BacktestStageConfig {
    fn default() -> Self {
        Self {
            use_realistic_fills: true,
            fill_probability: 0.10,  // 10% fill probability (conservative)
            fee_rate_bps: 1.0,       // 1 bps fee
            initial_capital: 10_000.0,
            risk_free_rate: 0.05,    // 5% annual
            equity_sample_interval: 100,
            verbose: false,
            min_events: 100,
            name: "Backtest".to_string(),
        }
    }
}

impl BacktestStageConfig {
    /// Create a configuration optimized for fast validation
    pub fn fast() -> Self {
        Self {
            use_realistic_fills: false,  // Faster without realistic fills
            fill_probability: 1.0,       // All touches fill
            verbose: false,
            equity_sample_interval: 500,
            ..Default::default()
        }
    }

    /// Create a configuration with conservative (realistic) assumptions
    pub fn conservative() -> Self {
        Self {
            use_realistic_fills: true,
            fill_probability: 0.05,  // Only 5% fill probability
            fee_rate_bps: 2.0,       // Higher fees
            ..Default::default()
        }
    }

    /// Create a configuration with optimistic assumptions (for upper bound)
    pub fn optimistic() -> Self {
        Self {
            use_realistic_fills: false,
            fill_probability: 0.50,  // 50% fill probability
            fee_rate_bps: 0.5,       // Lower fees
            ..Default::default()
        }
    }

    /// Set the stage name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
}

/// BacktestStage - Historical replay validation
///
/// This stage loads historical feature data from Parquet files, replays
/// the algorithm through the data, and produces a ValidationResult.
pub struct BacktestStage {
    config: BacktestStageConfig,
}

impl BacktestStage {
    /// Create a new BacktestStage with the given configuration
    pub fn new(config: BacktestStageConfig) -> Self {
        Self { config }
    }

    /// Create a BacktestStage with default configuration
    pub fn with_defaults() -> Self {
        Self::new(BacktestStageConfig::default())
    }

    /// Build the backtest configuration from context and stage config
    fn build_backtest_config(&self, context: &StageContext) -> BacktestConfig {
        let data_path = context.data_path.clone().unwrap_or_else(|| "./data/features".to_string());

        // Convert period timestamps to i64 milliseconds
        let start_time = Some(context.period_start.timestamp_millis());
        let end_time = Some(context.period_end.timestamp_millis());

        BacktestConfig {
            replay: ReplayConfig {
                data_dir: PathBuf::from(data_path),
                start_time,
                end_time,
                speed: 0.0,  // As fast as possible
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
        }
    }

    /// Convert backtest TradeRecord to validation TradeResult
    fn convert_trade(
        &self,
        record: &TradeRecord,
        trade_idx: usize,
        config_id: &str,
    ) -> TradeResult {
        // We don't have entry/exit pairs in the backtest trade log,
        // so we treat each record as a single trade
        let entry_time = Utc.timestamp_millis_opt(record.timestamp_ms)
            .single()
            .unwrap_or_else(Utc::now);
        let exit_time = entry_time;  // Same timestamp for single trades

        let price = record.price.to_f64().unwrap_or(0.0);
        let size = record.size.to_f64().unwrap_or(0.0);
        let pnl = record.pnl.map(|p| p.to_f64().unwrap_or(0.0)).unwrap_or(0.0);

        // Determine direction
        let direction = match record.side {
            TradeSide::Buy => TradeDirection::Long,
            TradeSide::Sell => TradeDirection::Short,
        };

        // Calculate P&L in bps
        let pnl_bps = if price > 0.0 && size > 0.0 {
            (pnl / (price * size)) * 10000.0
        } else {
            0.0
        };

        let return_pct = pnl_bps / 100.0;

        // Determine exit reason based on P&L
        let exit_reason = if pnl > 0.0 {
            ExitReason::TakeProfit
        } else if pnl < 0.0 {
            ExitReason::StopLoss
        } else {
            ExitReason::Unknown
        };

        TradeResult {
            trade_id: format!("BT-{}", trade_idx),
            direction,
            entry_time,
            exit_time,
            entry_price: price,
            exit_price: price,  // Same for single-point trades
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

    /// Convert backtest results to ValidationResult
    fn convert_results(
        &self,
        results: &BacktestResults,
        context: &StageContext,
        duration_secs: f64,
    ) -> ValidationResult {
        // Convert trades (only those with realized P&L)
        let trades: Vec<TradeResult> = results.trade_log.trades
            .iter()
            .enumerate()
            .filter(|(_, t)| t.pnl.is_some())  // Only closed trades
            .map(|(idx, t)| self.convert_trade(t, idx, &context.config.id))
            .collect();

        // Create validation result
        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            context.stage_name.clone(),
            context.config.id.clone(),
            context.period_start,
            context.period_end,
        );

        // Set trades and compute metrics
        result = result.with_trades(trades);

        // Add metadata
        result.add_metadata("events_processed".to_string(), results.events_processed.to_string());
        result.add_metadata("fills_generated".to_string(), results.fills_generated.to_string());
        result.add_metadata("fill_simulation".to_string(),
            if results.config.use_realistic_fills { "realistic" } else { "naive" }.to_string());

        // Add fill statistics
        result.add_metadata("bid_fill_rate".to_string(),
            format!("{:.2}%", results.fill_stats.bid_fill_rate * 100.0));
        result.add_metadata("ask_fill_rate".to_string(),
            format!("{:.2}%", results.fill_stats.ask_fill_rate * 100.0));

        // Set validation duration
        result.set_duration(duration_secs);

        // Evaluate thresholds
        result.evaluate_thresholds(context.thresholds.clone());

        // Add warnings for potential issues
        if results.events_processed < self.config.min_events {
            result.add_warning(format!(
                "Low event count: {} (minimum recommended: {})",
                results.events_processed, self.config.min_events
            ));
        }

        if result.metrics.trade_count < self.min_trades() {
            result.add_warning(format!(
                "Low trade count: {} (minimum for statistical significance: {})",
                result.metrics.trade_count, self.min_trades()
            ));
        }

        result
    }

    /// Run the backtest and return results
    async fn execute_backtest(&self, context: &StageContext) -> Result<BacktestResults, StageError> {
        let backtest_config = self.build_backtest_config(context);

        // Create and run backtest engine
        let mut engine = BacktestEngine::new(backtest_config);

        // Load data
        let data_path = context.data_path.as_deref().unwrap_or("./data/features");
        let event_count = engine.load_from(data_path)
            .map_err(|e| StageError::DataUnavailable(format!("Failed to load data: {}", e)))?;

        if event_count == 0 {
            return Err(StageError::DataUnavailable(
                "No events found in data directory".to_string()
            ));
        }

        // Run backtest
        let results = engine.run()
            .map_err(|e| StageError::ExecutionError(format!("Backtest failed: {}", e)))?;

        Ok(results)
    }
}

impl ValidationStage for BacktestStage {
    fn stage_type(&self) -> ValidationStageType {
        ValidationStageType::Backtest
    }

    fn name(&self) -> &str {
        &self.config.name
    }

    fn description(&self) -> &str {
        "Historical replay validation using Parquet feature data"
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
                "Data path required for backtest stage".to_string(),
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

        Ok(())
    }

    fn run<'a>(&'a self, context: &'a StageContext) -> RunFuture<'a> {
        Box::pin(async move {
            let start_time = Instant::now();

            // Execute the backtest
            let results = self.execute_backtest(context).await?;

            let duration_secs = start_time.elapsed().as_secs_f64();

            // Convert to ValidationResult
            let result = self.convert_results(&results, context, duration_secs);

            Ok(result)
        })
    }

    fn estimated_duration(&self, context: &StageContext) -> Option<u64> {
        // Estimate based on period length
        // Assume ~1 second per 7 days of data
        let days = context.period_days();
        Some((days / 7.0).max(1.0) as u64)
    }

    fn min_trades(&self) -> usize {
        30  // Minimum for statistical significance
    }

    fn requires_previous(&self) -> Option<ValidationStageType> {
        None  // Backtest is first stage, no prerequisites
    }
}

/// Factory for creating BacktestStage instances
pub struct BacktestStageFactory {
    default_config: BacktestStageConfig,
}

impl BacktestStageFactory {
    /// Create a new factory with default configuration
    pub fn new() -> Self {
        Self {
            default_config: BacktestStageConfig::default(),
        }
    }

    /// Create a factory with custom default configuration
    pub fn with_config(config: BacktestStageConfig) -> Self {
        Self {
            default_config: config,
        }
    }

    /// Create a BacktestStage with the default configuration
    pub fn create(&self, name: &str) -> BacktestStage {
        BacktestStage::new(self.default_config.clone().with_name(name))
    }

    /// Create a BacktestStage with custom configuration
    pub fn create_with_config(&self, name: &str, config: BacktestStageConfig) -> BacktestStage {
        BacktestStage::new(config.with_name(name))
    }
}

impl Default for BacktestStageFactory {
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

    // ==================== BacktestStageConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = BacktestStageConfig::default();

        assert!(config.use_realistic_fills);
        assert!((config.fill_probability - 0.10).abs() < 0.01);
        assert!((config.fee_rate_bps - 1.0).abs() < 0.01);
        assert!((config.initial_capital - 10_000.0).abs() < 0.01);
        assert!((config.risk_free_rate - 0.05).abs() < 0.01);
        assert_eq!(config.equity_sample_interval, 100);
        assert!(!config.verbose);
        assert_eq!(config.min_events, 100);
        assert_eq!(config.name, "Backtest");
    }

    #[test]
    fn test_config_fast() {
        let config = BacktestStageConfig::fast();

        assert!(!config.use_realistic_fills);
        assert!((config.fill_probability - 1.0).abs() < 0.01);
        assert!(!config.verbose);
        assert_eq!(config.equity_sample_interval, 500);
    }

    #[test]
    fn test_config_conservative() {
        let config = BacktestStageConfig::conservative();

        assert!(config.use_realistic_fills);
        assert!((config.fill_probability - 0.05).abs() < 0.01);
        assert!((config.fee_rate_bps - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_config_optimistic() {
        let config = BacktestStageConfig::optimistic();

        assert!(!config.use_realistic_fills);
        assert!((config.fill_probability - 0.50).abs() < 0.01);
        assert!((config.fee_rate_bps - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_config_with_name() {
        let config = BacktestStageConfig::default().with_name("BT-2025Q1");
        assert_eq!(config.name, "BT-2025Q1");
    }

    #[test]
    fn test_config_serialization() {
        let config = BacktestStageConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: BacktestStageConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, config.name);
        assert!((deserialized.fill_probability - config.fill_probability).abs() < 0.01);
    }

    // ==================== BacktestStage Basic Tests ====================

    #[test]
    fn test_stage_new() {
        let config = BacktestStageConfig::default();
        let stage = BacktestStage::new(config.clone());

        assert_eq!(stage.config.name, config.name);
    }

    #[test]
    fn test_stage_with_defaults() {
        let stage = BacktestStage::with_defaults();

        assert_eq!(stage.stage_type(), ValidationStageType::Backtest);
        assert_eq!(stage.name(), "Backtest");
    }

    #[test]
    fn test_stage_type() {
        let stage = BacktestStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::Backtest);
    }

    #[test]
    fn test_stage_name() {
        let config = BacktestStageConfig::default().with_name("Custom-BT");
        let stage = BacktestStage::new(config);

        assert_eq!(stage.name(), "Custom-BT");
    }

    #[test]
    fn test_stage_description() {
        let stage = BacktestStage::with_defaults();
        let desc = stage.description();

        assert!(desc.contains("Historical"));
        assert!(desc.contains("replay"));
    }

    #[test]
    fn test_stage_min_trades() {
        let stage = BacktestStage::with_defaults();
        assert_eq!(stage.min_trades(), 30);
    }

    #[test]
    fn test_stage_requires_previous() {
        let stage = BacktestStage::with_defaults();
        assert!(stage.requires_previous().is_none());
    }

    #[test]
    fn test_stage_estimated_duration_short_period() {
        let stage = BacktestStage::with_defaults();
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
        let stage = BacktestStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(365),
            Utc::now(),
        );

        let duration = stage.estimated_duration(&ctx);
        assert!(duration.is_some());
        assert!(duration.unwrap() > 30);  // More than 30 seconds for a year
    }

    // ==================== can_run() Tests ====================

    #[test]
    fn test_can_run_no_data_path() {
        let stage = BacktestStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        );
        // No data_path set

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    #[test]
    fn test_can_run_invalid_period() {
        let stage = BacktestStage::with_defaults();
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
        let stage = BacktestStage::with_defaults();
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

        let stage = BacktestStage::with_defaults();
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

        let stage = BacktestStage::with_defaults();
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

        let stage = BacktestStage::with_defaults();
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

    // ==================== convert_trade() Tests ====================

    #[test]
    fn test_convert_trade_buy_with_profit() {
        let stage = BacktestStage::with_defaults();
        let record = TradeRecord {
            timestamp_ms: 1700000000000,
            side: TradeSide::Buy,
            price: rust_decimal::Decimal::new(50000, 0),
            size: rust_decimal::Decimal::new(1, 1),  // 0.1
            fee: rust_decimal::Decimal::new(5, 2),   // 0.05
            pnl: Some(rust_decimal::Decimal::new(50, 0)),  // +50
        };

        let trade = stage.convert_trade(&record, 0, "CFG001");

        assert_eq!(trade.trade_id, "BT-0");
        assert_eq!(trade.direction, TradeDirection::Long);
        assert!((trade.pnl - 50.0).abs() < 0.01);
        assert!((trade.commission - 0.05).abs() < 0.01);
        assert_eq!(trade.exit_reason, ExitReason::TakeProfit);
        assert_eq!(trade.config_id, Some("CFG001".to_string()));
    }

    #[test]
    fn test_convert_trade_sell_with_loss() {
        let stage = BacktestStage::with_defaults();
        let record = TradeRecord {
            timestamp_ms: 1700000000000,
            side: TradeSide::Sell,
            price: rust_decimal::Decimal::new(50000, 0),
            size: rust_decimal::Decimal::new(1, 1),
            fee: rust_decimal::Decimal::new(5, 2),
            pnl: Some(rust_decimal::Decimal::new(-30, 0)),  // -30
        };

        let trade = stage.convert_trade(&record, 1, "CFG002");

        assert_eq!(trade.trade_id, "BT-1");
        assert_eq!(trade.direction, TradeDirection::Short);
        assert!((trade.pnl - (-30.0)).abs() < 0.01);
        assert_eq!(trade.exit_reason, ExitReason::StopLoss);
    }

    #[test]
    fn test_convert_trade_breakeven() {
        let stage = BacktestStage::with_defaults();
        let record = TradeRecord {
            timestamp_ms: 1700000000000,
            side: TradeSide::Buy,
            price: rust_decimal::Decimal::new(50000, 0),
            size: rust_decimal::Decimal::new(1, 1),
            fee: rust_decimal::Decimal::new(5, 2),
            pnl: Some(rust_decimal::Decimal::ZERO),
        };

        let trade = stage.convert_trade(&record, 2, "CFG003");

        assert_eq!(trade.exit_reason, ExitReason::Unknown);
        assert!((trade.pnl).abs() < 0.01);
    }

    #[test]
    fn test_convert_trade_pnl_bps_calculation() {
        let stage = BacktestStage::with_defaults();
        let record = TradeRecord {
            timestamp_ms: 1700000000000,
            side: TradeSide::Buy,
            price: rust_decimal::Decimal::new(100, 0),  // 100
            size: rust_decimal::Decimal::new(1, 0),      // 1.0
            fee: rust_decimal::Decimal::ZERO,
            pnl: Some(rust_decimal::Decimal::new(1, 0)),  // 1.0 (1% of notional)
        };

        let trade = stage.convert_trade(&record, 0, "CFG");

        // 1% return = 100 bps
        assert!((trade.pnl_bps - 100.0).abs() < 1.0);
        assert!((trade.return_pct - 1.0).abs() < 0.1);
    }

    #[test]
    fn test_convert_trade_zero_price() {
        let stage = BacktestStage::with_defaults();
        let record = TradeRecord {
            timestamp_ms: 1700000000000,
            side: TradeSide::Buy,
            price: rust_decimal::Decimal::ZERO,
            size: rust_decimal::Decimal::new(1, 0),
            fee: rust_decimal::Decimal::ZERO,
            pnl: Some(rust_decimal::Decimal::new(10, 0)),
        };

        let trade = stage.convert_trade(&record, 0, "CFG");

        // Should handle gracefully
        assert!((trade.pnl_bps).abs() < 0.01);  // Can't calculate bps with zero price
    }

    #[test]
    fn test_convert_trade_zero_size() {
        let stage = BacktestStage::with_defaults();
        let record = TradeRecord {
            timestamp_ms: 1700000000000,
            side: TradeSide::Sell,
            price: rust_decimal::Decimal::new(100, 0),
            size: rust_decimal::Decimal::ZERO,
            fee: rust_decimal::Decimal::ZERO,
            pnl: Some(rust_decimal::Decimal::new(10, 0)),
        };

        let trade = stage.convert_trade(&record, 0, "CFG");

        assert!((trade.pnl_bps).abs() < 0.01);
    }

    // ==================== build_backtest_config() Tests ====================

    #[test]
    fn test_build_backtest_config_basic() {
        let stage = BacktestStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        )
        .with_data_path("/tmp/data");

        let config = stage.build_backtest_config(&ctx);

        assert_eq!(config.replay.data_dir, PathBuf::from("/tmp/data"));
        assert!(config.replay.start_time.is_some());
        assert!(config.replay.end_time.is_some());
        assert!(!config.verbose);
    }

    #[test]
    fn test_build_backtest_config_verbose() {
        let stage_config = BacktestStageConfig {
            verbose: true,
            ..Default::default()
        };
        let stage = BacktestStage::new(stage_config);
        let ctx = StageContext::default().with_data_path("/tmp");

        let config = stage.build_backtest_config(&ctx);
        assert!(config.verbose);
    }

    #[test]
    fn test_build_backtest_config_realistic_fills() {
        let stage_config = BacktestStageConfig {
            use_realistic_fills: true,
            fill_probability: 0.15,
            ..Default::default()
        };
        let stage = BacktestStage::new(stage_config);
        let ctx = StageContext::default().with_data_path("/tmp");

        let config = stage.build_backtest_config(&ctx);

        assert!(config.use_realistic_fills);
        assert!((config.fill_sim.base_fill_probability - 0.15).abs() < 0.01);
    }

    #[test]
    fn test_build_backtest_config_default_data_path() {
        let stage = BacktestStage::with_defaults();
        let ctx = StageContext::default();  // No data path

        let config = stage.build_backtest_config(&ctx);

        assert_eq!(config.replay.data_dir, PathBuf::from("./data/features"));
    }

    #[test]
    fn test_build_backtest_config_time_range() {
        let stage = BacktestStage::with_defaults();
        let start = Utc::now() - Duration::days(90);
        let end = Utc::now();

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            start,
            end,
        )
        .with_data_path("/tmp");

        let config = stage.build_backtest_config(&ctx);

        assert_eq!(config.replay.start_time, Some(start.timestamp_millis()));
        assert_eq!(config.replay.end_time, Some(end.timestamp_millis()));
    }

    // ==================== Factory Tests ====================

    #[test]
    fn test_factory_new() {
        let factory = BacktestStageFactory::new();
        let stage = factory.create("BT-Test");

        assert_eq!(stage.name(), "BT-Test");
    }

    #[test]
    fn test_factory_default() {
        let factory = BacktestStageFactory::default();
        let stage = factory.create("BT-Default");

        assert_eq!(stage.name(), "BT-Default");
    }

    #[test]
    fn test_factory_with_config() {
        let config = BacktestStageConfig::conservative();
        let factory = BacktestStageFactory::with_config(config);
        let stage = factory.create("BT-Conservative");

        assert_eq!(stage.name(), "BT-Conservative");
        assert!((stage.config.fill_probability - 0.05).abs() < 0.01);
    }

    #[test]
    fn test_factory_create_with_config() {
        let factory = BacktestStageFactory::new();
        let custom_config = BacktestStageConfig::optimistic();
        let stage = factory.create_with_config("BT-Custom", custom_config);

        assert_eq!(stage.name(), "BT-Custom");
        assert!(!stage.config.use_realistic_fills);
    }

    // ==================== ValidationStage Trait Tests ====================

    #[test]
    fn test_trait_stage_type_is_backtest() {
        let stage = BacktestStage::with_defaults();
        assert_eq!(stage.stage_type(), ValidationStageType::Backtest);
    }

    #[test]
    fn test_trait_is_historical() {
        let stage = BacktestStage::with_defaults();
        assert!(stage.stage_type().is_historical());
    }

    #[test]
    fn test_trait_pipeline_order() {
        let stage = BacktestStage::with_defaults();
        assert_eq!(stage.stage_type().pipeline_order(), 1);  // First in pipeline
    }

    // ==================== Error Handling Tests ====================

    #[test]
    fn test_error_recoverable_data_unavailable() {
        let err = StageError::DataUnavailable("test".to_string());
        assert!(err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    #[test]
    fn test_error_configuration_halts_pipeline() {
        let err = StageError::ConfigurationError("test".to_string());
        assert!(!err.is_recoverable());
        assert!(err.should_halt_pipeline());
    }

    #[test]
    fn test_error_execution_not_recoverable() {
        let err = StageError::ExecutionError("test".to_string());
        assert!(!err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_stage_name() {
        let config = BacktestStageConfig::default().with_name("");
        let stage = BacktestStage::new(config);
        assert_eq!(stage.name(), "");
    }

    #[test]
    fn test_very_long_stage_name() {
        let long_name = "A".repeat(1000);
        let config = BacktestStageConfig::default().with_name(&long_name);
        let stage = BacktestStage::new(config);
        assert_eq!(stage.name().len(), 1000);
    }

    #[test]
    fn test_zero_fill_probability() {
        let config = BacktestStageConfig {
            fill_probability: 0.0,
            ..Default::default()
        };
        let stage = BacktestStage::new(config);
        let ctx = StageContext::default().with_data_path("/tmp");

        let backtest_config = stage.build_backtest_config(&ctx);
        assert!((backtest_config.fill_sim.base_fill_probability).abs() < 0.01);
    }

    #[test]
    fn test_extreme_fill_probability() {
        let config = BacktestStageConfig {
            fill_probability: 1.0,
            ..Default::default()
        };
        let stage = BacktestStage::new(config);
        let ctx = StageContext::default().with_data_path("/tmp");

        let backtest_config = stage.build_backtest_config(&ctx);
        assert!((backtest_config.fill_sim.base_fill_probability - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_negative_fee_rate_handled() {
        // Should not crash even with invalid input
        let config = BacktestStageConfig {
            fee_rate_bps: -1.0,
            ..Default::default()
        };
        let stage = BacktestStage::new(config);
        let ctx = StageContext::default().with_data_path("/tmp");

        // Should still build config without crashing
        let _ = stage.build_backtest_config(&ctx);
    }

    #[test]
    fn test_zero_initial_capital() {
        let config = BacktestStageConfig {
            initial_capital: 0.0,
            ..Default::default()
        };
        let stage = BacktestStage::new(config);
        let ctx = StageContext::default().with_data_path("/tmp");

        let backtest_config = stage.build_backtest_config(&ctx);
        // Should still create config
        assert!(backtest_config.initial_capital.is_zero());
    }

    // ==================== Period Validation Tests ====================

    #[test]
    fn test_period_same_start_end() {
        let stage = BacktestStage::with_defaults();
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
        let stage = BacktestStage::with_defaults();
        let now = Utc::now();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now - Duration::seconds(1),
            now,
        );

        // Very short period should still estimate some duration
        let est = stage.estimated_duration(&ctx);
        assert!(est.is_some());
        assert!(est.unwrap() >= 1);
    }

    #[test]
    fn test_period_very_long() {
        let stage = BacktestStage::with_defaults();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(3650),  // 10 years
            Utc::now(),
        );

        let est = stage.estimated_duration(&ctx);
        assert!(est.is_some());
        assert!(est.unwrap() > 500);  // Should estimate significant time
    }

    // ==================== Config Combination Tests ====================

    #[test]
    fn test_config_all_options() {
        let config = BacktestStageConfig {
            use_realistic_fills: true,
            fill_probability: 0.25,
            fee_rate_bps: 1.5,
            initial_capital: 50_000.0,
            risk_free_rate: 0.03,
            equity_sample_interval: 200,
            verbose: true,
            min_events: 500,
            name: "Full-Config-Test".to_string(),
        };

        let stage = BacktestStage::new(config.clone());

        assert_eq!(stage.name(), "Full-Config-Test");
        assert_eq!(stage.config.min_events, 500);
        assert!((stage.config.initial_capital - 50_000.0).abs() < 0.01);
    }

    // ==================== Async Run Signature Tests ====================

    #[test]
    fn test_run_returns_future() {
        // Verify the run method returns a future (compile-time check)
        let stage = BacktestStage::with_defaults();
        let ctx = StageContext::default()
            .with_data_path("/tmp")
            .with_name("Test");

        let _future = stage.run(&ctx);
        // Just checking it compiles and returns a future
    }

    // ==================== Clone and Debug Tests ====================

    #[test]
    fn test_config_clone() {
        let config = BacktestStageConfig::conservative();
        let cloned = config.clone();

        assert_eq!(cloned.fill_probability, config.fill_probability);
        assert_eq!(cloned.name, config.name);
    }

    #[test]
    fn test_config_debug() {
        let config = BacktestStageConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("BacktestStageConfig"));
        assert!(debug_str.contains("fill_probability"));
    }
}

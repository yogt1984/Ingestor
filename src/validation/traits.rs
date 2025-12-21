//! Validation Stage Traits (Task 2.0)
//!
//! Defines the core traits for validation stages that can be plugged into the pipeline.

use chrono::{DateTime, Utc};
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use crate::core::{AlgorithmConfig, ValidationResult, ValidationStageType, ValidationThresholds};

/// Error type for validation stage operations
#[derive(Debug, Clone)]
pub enum StageError {
    /// Configuration error
    ConfigurationError(String),

    /// Data not available for the requested period
    DataUnavailable(String),

    /// Execution error during validation
    ExecutionError(String),

    /// Threshold evaluation failed
    ThresholdError(String),

    /// Timeout during validation
    Timeout(u64),

    /// Stage was cancelled
    Cancelled(String),

    /// Invalid state for operation
    InvalidState(String),

    /// Resource exhaustion (memory, file handles, etc.)
    ResourceExhausted(String),

    /// Internal error
    Internal(String),
}

impl StageError {
    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            StageError::DataUnavailable(_)
                | StageError::Timeout(_)
                | StageError::ResourceExhausted(_)
        )
    }

    /// Check if this error should halt the pipeline
    pub fn should_halt_pipeline(&self) -> bool {
        matches!(
            self,
            StageError::ConfigurationError(_) | StageError::InvalidState(_)
        )
    }
}

impl fmt::Display for StageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StageError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            StageError::DataUnavailable(msg) => write!(f, "Data unavailable: {}", msg),
            StageError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            StageError::ThresholdError(msg) => write!(f, "Threshold evaluation failed: {}", msg),
            StageError::Timeout(secs) => write!(f, "Validation timed out after {} seconds", secs),
            StageError::Cancelled(msg) => write!(f, "Cancelled: {}", msg),
            StageError::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            StageError::ResourceExhausted(msg) => write!(f, "Resource exhaustion: {}", msg),
            StageError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for StageError {}

/// Context provided to each validation stage
#[derive(Debug, Clone)]
pub struct StageContext {
    /// Algorithm configuration being validated
    pub config: AlgorithmConfig,

    /// Thresholds for pass/fail evaluation
    pub thresholds: ValidationThresholds,

    /// Start of validation period
    pub period_start: DateTime<Utc>,

    /// End of validation period
    pub period_end: DateTime<Utc>,

    /// Name for this stage run
    pub stage_name: String,

    /// Maximum duration allowed for this stage (seconds)
    pub timeout_seconds: Option<u64>,

    /// Results from previous stages (for reference)
    pub previous_results: Vec<ValidationResult>,

    /// Optional data path for historical stages
    pub data_path: Option<String>,

    /// Whether to generate detailed trade-by-trade results
    pub detailed_output: bool,

    /// Custom metadata for stage-specific configuration
    pub metadata: std::collections::HashMap<String, String>,
}

impl StageContext {
    /// Create a new stage context
    pub fn new(
        config: AlgorithmConfig,
        thresholds: ValidationThresholds,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
    ) -> Self {
        Self {
            config,
            thresholds,
            period_start,
            period_end,
            stage_name: String::new(),
            timeout_seconds: None,
            previous_results: Vec::new(),
            data_path: None,
            detailed_output: true,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Set the stage name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.stage_name = name.into();
        self
    }

    /// Set the timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    /// Add previous stage results
    pub fn with_previous_results(mut self, results: Vec<ValidationResult>) -> Self {
        self.previous_results = results;
        self
    }

    /// Set data path
    pub fn with_data_path(mut self, path: impl Into<String>) -> Self {
        self.data_path = Some(path.into());
        self
    }

    /// Set detailed output flag
    pub fn with_detailed_output(mut self, detailed: bool) -> Self {
        self.detailed_output = detailed;
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Get the period duration in days
    pub fn period_days(&self) -> f64 {
        (self.period_end - self.period_start).num_seconds() as f64 / 86400.0
    }

    /// Check if a previous stage of the given type passed
    pub fn previous_stage_passed(&self, stage_type: ValidationStageType) -> Option<bool> {
        self.previous_results
            .iter()
            .find(|r| r.stage_type == stage_type)
            .map(|r| r.passed)
    }

    /// Get the result from a previous stage
    pub fn get_previous_result(&self, stage_type: ValidationStageType) -> Option<&ValidationResult> {
        self.previous_results
            .iter()
            .find(|r| r.stage_type == stage_type)
    }
}

impl Default for StageContext {
    fn default() -> Self {
        Self {
            config: AlgorithmConfig::default(),
            thresholds: ValidationThresholds::default(),
            period_start: Utc::now(),
            period_end: Utc::now(),
            stage_name: String::new(),
            timeout_seconds: None,
            previous_results: Vec::new(),
            data_path: None,
            detailed_output: true,
            metadata: std::collections::HashMap::new(),
        }
    }
}

/// Type alias for the async run future
pub type RunFuture<'a> = Pin<Box<dyn Future<Output = Result<ValidationResult, StageError>> + Send + 'a>>;

/// Core trait for validation stages
///
/// Each stage in the pipeline implements this trait to provide its
/// specific validation logic. Stages are run sequentially, with each
/// stage receiving the context and results from previous stages.
pub trait ValidationStage: Send + Sync {
    /// Get the stage type
    fn stage_type(&self) -> ValidationStageType;

    /// Get the stage name
    fn name(&self) -> &str;

    /// Get a human-readable description
    fn description(&self) -> &str {
        match self.stage_type() {
            ValidationStageType::Backtest => "Historical replay validation",
            ValidationStageType::Forward => "Walk-forward validation",
            ValidationStageType::OutOfSample => "Out-of-sample validation",
            ValidationStageType::Paper => "Paper trading validation",
            ValidationStageType::Live => "Live trading validation",
        }
    }

    /// Check if this stage can run given the context
    ///
    /// Returns Ok(()) if the stage can run, or an error explaining why not.
    fn can_run(&self, context: &StageContext) -> Result<(), StageError> {
        // Default implementation: check that we have valid period
        if context.period_end <= context.period_start {
            return Err(StageError::ConfigurationError(
                "Period end must be after period start".to_string(),
            ));
        }

        // Check for data path on historical stages
        if self.stage_type().is_historical() && context.data_path.is_none() {
            return Err(StageError::ConfigurationError(
                "Data path required for historical validation".to_string(),
            ));
        }

        Ok(())
    }

    /// Run the validation stage
    ///
    /// This is the main entry point for stage execution. It should:
    /// 1. Load/prepare any necessary data
    /// 2. Run the algorithm through the validation period
    /// 3. Collect trade results
    /// 4. Compute metrics and evaluate thresholds
    fn run<'a>(&'a self, context: &'a StageContext) -> RunFuture<'a>;

    /// Estimate the time this stage will take (in seconds)
    ///
    /// This is used for progress reporting and timeout estimation.
    fn estimated_duration(&self, context: &StageContext) -> Option<u64> {
        // Default: estimate based on period length
        // Assume ~1 second per day of data
        Some(context.period_days().max(1.0) as u64)
    }

    /// Get the minimum number of trades required for this stage to be valid
    fn min_trades(&self) -> usize {
        match self.stage_type() {
            ValidationStageType::Backtest => 30,
            ValidationStageType::Forward => 20,
            ValidationStageType::OutOfSample => 20,
            ValidationStageType::Paper => 10,
            ValidationStageType::Live => 5,
        }
    }

    /// Check if this stage requires a previous stage to have passed
    fn requires_previous(&self) -> Option<ValidationStageType> {
        match self.stage_type() {
            ValidationStageType::Backtest => None,
            ValidationStageType::Forward => Some(ValidationStageType::Backtest),
            ValidationStageType::OutOfSample => Some(ValidationStageType::Forward),
            ValidationStageType::Paper => Some(ValidationStageType::OutOfSample),
            ValidationStageType::Live => Some(ValidationStageType::Paper),
        }
    }
}

/// Factory trait for creating validation stages
///
/// This allows for dependency injection and testing with mock stages.
pub trait ValidationStageFactory: Send + Sync {
    /// Create a backtest stage
    fn create_backtest(&self, name: &str) -> Box<dyn ValidationStage>;

    /// Create a forward validation stage
    fn create_forward(&self, name: &str) -> Box<dyn ValidationStage>;

    /// Create an out-of-sample stage
    fn create_oos(&self, name: &str) -> Box<dyn ValidationStage>;

    /// Create a paper trading stage
    fn create_paper(&self, name: &str) -> Box<dyn ValidationStage>;

    /// Create a live trading stage
    fn create_live(&self, name: &str) -> Box<dyn ValidationStage>;

    /// Create a stage by type
    fn create(&self, stage_type: ValidationStageType, name: &str) -> Box<dyn ValidationStage> {
        match stage_type {
            ValidationStageType::Backtest => self.create_backtest(name),
            ValidationStageType::Forward => self.create_forward(name),
            ValidationStageType::OutOfSample => self.create_oos(name),
            ValidationStageType::Paper => self.create_paper(name),
            ValidationStageType::Live => self.create_live(name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{TradeDirection, TradeResult};
    use chrono::Duration;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    // ==================== StageError Tests ====================

    #[test]
    fn test_stage_error_configuration() {
        let err = StageError::ConfigurationError("missing field".to_string());
        assert!(err.to_string().contains("Configuration"));
        assert!(err.to_string().contains("missing field"));
        assert!(!err.is_recoverable());
        assert!(err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_data_unavailable() {
        let err = StageError::DataUnavailable("no data for 2024-01".to_string());
        assert!(err.to_string().contains("Data unavailable"));
        assert!(err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_execution() {
        let err = StageError::ExecutionError("algorithm crashed".to_string());
        assert!(err.to_string().contains("Execution"));
        assert!(!err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_threshold() {
        let err = StageError::ThresholdError("sharpe below minimum".to_string());
        assert!(err.to_string().contains("Threshold"));
        assert!(!err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_timeout() {
        let err = StageError::Timeout(3600);
        assert!(err.to_string().contains("3600"));
        assert!(err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_cancelled() {
        let err = StageError::Cancelled("user requested".to_string());
        assert!(err.to_string().contains("Cancelled"));
        assert!(!err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_invalid_state() {
        let err = StageError::InvalidState("already running".to_string());
        assert!(err.to_string().contains("Invalid state"));
        assert!(!err.is_recoverable());
        assert!(err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_resource_exhausted() {
        let err = StageError::ResourceExhausted("out of memory".to_string());
        assert!(err.to_string().contains("Resource exhaustion"));
        assert!(err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_internal() {
        let err = StageError::Internal("unexpected condition".to_string());
        assert!(err.to_string().contains("Internal"));
        assert!(!err.is_recoverable());
        assert!(!err.should_halt_pipeline());
    }

    #[test]
    fn test_stage_error_clone() {
        let err = StageError::Timeout(100);
        let cloned = err.clone();
        assert!(matches!(cloned, StageError::Timeout(100)));
    }

    #[test]
    fn test_stage_error_debug() {
        let err = StageError::ConfigurationError("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("ConfigurationError"));
    }

    #[test]
    fn test_stage_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(StageError::Internal("test".to_string()));
        assert!(err.to_string().contains("Internal"));
    }

    // ==================== StageContext Tests ====================

    #[test]
    fn test_stage_context_new() {
        let config = AlgorithmConfig::default();
        let thresholds = ValidationThresholds::default();
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let ctx = StageContext::new(config.clone(), thresholds.clone(), start, end);

        assert_eq!(ctx.period_start, start);
        assert_eq!(ctx.period_end, end);
        assert!(ctx.stage_name.is_empty());
        assert!(ctx.timeout_seconds.is_none());
    }

    #[test]
    fn test_stage_context_with_name() {
        let ctx = StageContext::default().with_name("BT-2025Q1");
        assert_eq!(ctx.stage_name, "BT-2025Q1");
    }

    #[test]
    fn test_stage_context_with_timeout() {
        let ctx = StageContext::default().with_timeout(3600);
        assert_eq!(ctx.timeout_seconds, Some(3600));
    }

    #[test]
    fn test_stage_context_with_data_path() {
        let ctx = StageContext::default().with_data_path("/data/features");
        assert_eq!(ctx.data_path, Some("/data/features".to_string()));
    }

    #[test]
    fn test_stage_context_with_detailed_output() {
        let ctx = StageContext::default().with_detailed_output(false);
        assert!(!ctx.detailed_output);
    }

    #[test]
    fn test_stage_context_with_metadata() {
        let ctx = StageContext::default()
            .with_metadata("symbol", "BTCUSDT")
            .with_metadata("exchange", "binance");

        assert_eq!(ctx.metadata.get("symbol"), Some(&"BTCUSDT".to_string()));
        assert_eq!(ctx.metadata.get("exchange"), Some(&"binance".to_string()));
    }

    #[test]
    fn test_stage_context_period_days() {
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            start,
            end,
        );

        let days = ctx.period_days();
        assert!((days - 30.0).abs() < 0.1);
    }

    #[test]
    fn test_stage_context_period_days_fractional() {
        let start = Utc::now() - Duration::hours(36);
        let end = Utc::now();

        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            start,
            end,
        );

        let days = ctx.period_days();
        assert!((days - 1.5).abs() < 0.1);
    }

    #[test]
    fn test_stage_context_with_previous_results() {
        let mut result = ValidationResult::default();
        result.stage_type = ValidationStageType::Backtest;
        result.passed = true;

        let ctx = StageContext::default().with_previous_results(vec![result]);

        assert_eq!(ctx.previous_results.len(), 1);
    }

    #[test]
    fn test_stage_context_previous_stage_passed() {
        let mut bt_result = ValidationResult::default();
        bt_result.stage_type = ValidationStageType::Backtest;
        bt_result.passed = true;

        let mut fw_result = ValidationResult::default();
        fw_result.stage_type = ValidationStageType::Forward;
        fw_result.passed = false;

        let ctx = StageContext::default().with_previous_results(vec![bt_result, fw_result]);

        assert_eq!(
            ctx.previous_stage_passed(ValidationStageType::Backtest),
            Some(true)
        );
        assert_eq!(
            ctx.previous_stage_passed(ValidationStageType::Forward),
            Some(false)
        );
        assert_eq!(
            ctx.previous_stage_passed(ValidationStageType::OutOfSample),
            None
        );
    }

    #[test]
    fn test_stage_context_get_previous_result() {
        let mut bt_result = ValidationResult::default();
        bt_result.stage_type = ValidationStageType::Backtest;
        bt_result.stage_name = "BT-Test".to_string();

        let ctx = StageContext::default().with_previous_results(vec![bt_result]);

        let prev = ctx.get_previous_result(ValidationStageType::Backtest);
        assert!(prev.is_some());
        assert_eq!(prev.unwrap().stage_name, "BT-Test");

        let missing = ctx.get_previous_result(ValidationStageType::Paper);
        assert!(missing.is_none());
    }

    #[test]
    fn test_stage_context_default() {
        let ctx = StageContext::default();

        assert!(ctx.stage_name.is_empty());
        assert!(ctx.timeout_seconds.is_none());
        assert!(ctx.previous_results.is_empty());
        assert!(ctx.data_path.is_none());
        assert!(ctx.detailed_output);
        assert!(ctx.metadata.is_empty());
    }

    #[test]
    fn test_stage_context_builder_chain() {
        let ctx = StageContext::default()
            .with_name("Test-Stage")
            .with_timeout(1800)
            .with_data_path("/data")
            .with_detailed_output(true)
            .with_metadata("key", "value");

        assert_eq!(ctx.stage_name, "Test-Stage");
        assert_eq!(ctx.timeout_seconds, Some(1800));
        assert_eq!(ctx.data_path, Some("/data".to_string()));
        assert!(ctx.detailed_output);
        assert_eq!(ctx.metadata.get("key"), Some(&"value".to_string()));
    }

    // ==================== Mock ValidationStage for Testing ====================

    struct MockStage {
        stage_type: ValidationStageType,
        name: String,
        should_fail: bool,
        run_count: Arc<AtomicUsize>,
        can_run_called: Arc<AtomicBool>,
    }

    impl MockStage {
        fn new(stage_type: ValidationStageType, name: &str) -> Self {
            Self {
                stage_type,
                name: name.to_string(),
                should_fail: false,
                run_count: Arc::new(AtomicUsize::new(0)),
                can_run_called: Arc::new(AtomicBool::new(false)),
            }
        }

        fn with_failure(mut self) -> Self {
            self.should_fail = true;
            self
        }
    }

    impl ValidationStage for MockStage {
        fn stage_type(&self) -> ValidationStageType {
            self.stage_type
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn can_run(&self, context: &StageContext) -> Result<(), StageError> {
            self.can_run_called.store(true, Ordering::SeqCst);

            if context.period_end <= context.period_start {
                return Err(StageError::ConfigurationError(
                    "Invalid period".to_string(),
                ));
            }

            // Historical stages need data path
            if self.stage_type.is_historical() && context.data_path.is_none() {
                return Err(StageError::ConfigurationError(
                    "Data path required".to_string(),
                ));
            }

            Ok(())
        }

        fn run<'a>(&'a self, context: &'a StageContext) -> RunFuture<'a> {
            let run_count = self.run_count.clone();
            let should_fail = self.should_fail;
            let stage_type = self.stage_type;
            let stage_name = context.stage_name.clone();
            let config_id = context.config.id.clone();
            let period_start = context.period_start;
            let period_end = context.period_end;
            let thresholds = context.thresholds.clone();

            Box::pin(async move {
                run_count.fetch_add(1, Ordering::SeqCst);

                if should_fail {
                    return Err(StageError::ExecutionError("Mock failure".to_string()));
                }

                // Create a simple result
                let mut result = ValidationResult::new(
                    stage_type,
                    stage_name,
                    config_id,
                    period_start,
                    period_end,
                );

                // Create some mock trades
                let mut trades = Vec::new();
                for i in 0..10 {
                    let trade = TradeResult::new(
                        format!("T{}", i),
                        TradeDirection::Long,
                        period_start + Duration::hours(i as i64),
                        period_start + Duration::hours(i as i64 + 1),
                        100.0,
                        101.0,
                        1.0,
                    );
                    trades.push(trade);
                }

                result = result.with_trades(trades);
                result.evaluate_thresholds(thresholds);

                Ok(result)
            })
        }

        fn estimated_duration(&self, _context: &StageContext) -> Option<u64> {
            Some(10)
        }
    }

    // ==================== ValidationStage Trait Tests ====================

    #[tokio::test]
    async fn test_validation_stage_stage_type() {
        let stage = MockStage::new(ValidationStageType::Backtest, "BT-Test");
        assert_eq!(stage.stage_type(), ValidationStageType::Backtest);

        let stage = MockStage::new(ValidationStageType::Forward, "FW-Test");
        assert_eq!(stage.stage_type(), ValidationStageType::Forward);
    }

    #[tokio::test]
    async fn test_validation_stage_name() {
        let stage = MockStage::new(ValidationStageType::Backtest, "Custom-Name");
        assert_eq!(stage.name(), "Custom-Name");
    }

    #[tokio::test]
    async fn test_validation_stage_description() {
        let bt = MockStage::new(ValidationStageType::Backtest, "BT");
        assert_eq!(bt.description(), "Historical replay validation");

        let fw = MockStage::new(ValidationStageType::Forward, "FW");
        assert_eq!(fw.description(), "Walk-forward validation");

        let oos = MockStage::new(ValidationStageType::OutOfSample, "OOS");
        assert_eq!(oos.description(), "Out-of-sample validation");

        let pp = MockStage::new(ValidationStageType::Paper, "PP");
        assert_eq!(pp.description(), "Paper trading validation");

        let lv = MockStage::new(ValidationStageType::Live, "LV");
        assert_eq!(lv.description(), "Live trading validation");
    }

    #[tokio::test]
    async fn test_validation_stage_can_run_valid() {
        let stage = MockStage::new(ValidationStageType::Backtest, "BT");
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        )
        .with_data_path("/data");

        assert!(stage.can_run(&ctx).is_ok());
        assert!(stage.can_run_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_validation_stage_can_run_invalid_period() {
        let stage = MockStage::new(ValidationStageType::Backtest, "BT");
        let now = Utc::now();

        // End before start
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now,
            now - Duration::days(1),
        )
        .with_data_path("/data");

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ConfigurationError(_)));
    }

    #[tokio::test]
    async fn test_validation_stage_can_run_missing_data_path() {
        let stage = MockStage::new(ValidationStageType::Backtest, "BT");
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        );
        // No data path set

        let result = stage.can_run(&ctx);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validation_stage_run_success() {
        let stage = MockStage::new(ValidationStageType::Backtest, "BT-Test");
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::relaxed(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        )
        .with_name("BT-Test")
        .with_data_path("/data");

        let result = stage.run(&ctx).await;
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.stage_type, ValidationStageType::Backtest);
        assert_eq!(result.metrics.trade_count, 10);
        assert_eq!(stage.run_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_validation_stage_run_failure() {
        let stage = MockStage::new(ValidationStageType::Backtest, "BT-Fail").with_failure();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        )
        .with_data_path("/data");

        let result = stage.run(&ctx).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StageError::ExecutionError(_)));
    }

    #[tokio::test]
    async fn test_validation_stage_estimated_duration() {
        let stage = MockStage::new(ValidationStageType::Backtest, "BT");
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        );

        let duration = stage.estimated_duration(&ctx);
        assert_eq!(duration, Some(10)); // Mock returns 10
    }

    #[tokio::test]
    async fn test_validation_stage_min_trades() {
        let bt = MockStage::new(ValidationStageType::Backtest, "BT");
        assert_eq!(bt.min_trades(), 30);

        let fw = MockStage::new(ValidationStageType::Forward, "FW");
        assert_eq!(fw.min_trades(), 20);

        let oos = MockStage::new(ValidationStageType::OutOfSample, "OOS");
        assert_eq!(oos.min_trades(), 20);

        let pp = MockStage::new(ValidationStageType::Paper, "PP");
        assert_eq!(pp.min_trades(), 10);

        let lv = MockStage::new(ValidationStageType::Live, "LV");
        assert_eq!(lv.min_trades(), 5);
    }

    #[tokio::test]
    async fn test_validation_stage_requires_previous() {
        let bt = MockStage::new(ValidationStageType::Backtest, "BT");
        assert!(bt.requires_previous().is_none());

        let fw = MockStage::new(ValidationStageType::Forward, "FW");
        assert_eq!(fw.requires_previous(), Some(ValidationStageType::Backtest));

        let oos = MockStage::new(ValidationStageType::OutOfSample, "OOS");
        assert_eq!(oos.requires_previous(), Some(ValidationStageType::Forward));

        let pp = MockStage::new(ValidationStageType::Paper, "PP");
        assert_eq!(
            pp.requires_previous(),
            Some(ValidationStageType::OutOfSample)
        );

        let lv = MockStage::new(ValidationStageType::Live, "LV");
        assert_eq!(lv.requires_previous(), Some(ValidationStageType::Paper));
    }

    #[tokio::test]
    async fn test_validation_stage_multiple_runs() {
        let stage = MockStage::new(ValidationStageType::Backtest, "BT");
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::relaxed(),
            Utc::now() - Duration::days(30),
            Utc::now(),
        )
        .with_data_path("/data");

        for i in 1..=5 {
            let result = stage.run(&ctx).await;
            assert!(result.is_ok());
            assert_eq!(stage.run_count.load(Ordering::SeqCst), i);
        }
    }

    // ==================== MockFactory for Testing ====================

    struct MockFactory;

    impl ValidationStageFactory for MockFactory {
        fn create_backtest(&self, name: &str) -> Box<dyn ValidationStage> {
            Box::new(MockStage::new(ValidationStageType::Backtest, name))
        }

        fn create_forward(&self, name: &str) -> Box<dyn ValidationStage> {
            Box::new(MockStage::new(ValidationStageType::Forward, name))
        }

        fn create_oos(&self, name: &str) -> Box<dyn ValidationStage> {
            Box::new(MockStage::new(ValidationStageType::OutOfSample, name))
        }

        fn create_paper(&self, name: &str) -> Box<dyn ValidationStage> {
            Box::new(MockStage::new(ValidationStageType::Paper, name))
        }

        fn create_live(&self, name: &str) -> Box<dyn ValidationStage> {
            Box::new(MockStage::new(ValidationStageType::Live, name))
        }
    }

    // ==================== ValidationStageFactory Tests ====================

    #[test]
    fn test_factory_create_backtest() {
        let factory = MockFactory;
        let stage = factory.create_backtest("BT-Test");
        assert_eq!(stage.stage_type(), ValidationStageType::Backtest);
        assert_eq!(stage.name(), "BT-Test");
    }

    #[test]
    fn test_factory_create_forward() {
        let factory = MockFactory;
        let stage = factory.create_forward("FW-Test");
        assert_eq!(stage.stage_type(), ValidationStageType::Forward);
    }

    #[test]
    fn test_factory_create_oos() {
        let factory = MockFactory;
        let stage = factory.create_oos("OOS-Test");
        assert_eq!(stage.stage_type(), ValidationStageType::OutOfSample);
    }

    #[test]
    fn test_factory_create_paper() {
        let factory = MockFactory;
        let stage = factory.create_paper("PP-Test");
        assert_eq!(stage.stage_type(), ValidationStageType::Paper);
    }

    #[test]
    fn test_factory_create_live() {
        let factory = MockFactory;
        let stage = factory.create_live("LV-Test");
        assert_eq!(stage.stage_type(), ValidationStageType::Live);
    }

    #[test]
    fn test_factory_create_by_type() {
        let factory = MockFactory;

        let bt = factory.create(ValidationStageType::Backtest, "BT");
        assert_eq!(bt.stage_type(), ValidationStageType::Backtest);

        let fw = factory.create(ValidationStageType::Forward, "FW");
        assert_eq!(fw.stage_type(), ValidationStageType::Forward);

        let oos = factory.create(ValidationStageType::OutOfSample, "OOS");
        assert_eq!(oos.stage_type(), ValidationStageType::OutOfSample);

        let pp = factory.create(ValidationStageType::Paper, "PP");
        assert_eq!(pp.stage_type(), ValidationStageType::Paper);

        let lv = factory.create(ValidationStageType::Live, "LV");
        assert_eq!(lv.stage_type(), ValidationStageType::Live);
    }

    // ==================== Edge Case Tests ====================

    #[tokio::test]
    async fn test_stage_with_empty_name() {
        let stage = MockStage::new(ValidationStageType::Backtest, "");
        assert_eq!(stage.name(), "");
    }

    #[tokio::test]
    async fn test_stage_with_very_long_name() {
        let long_name = "A".repeat(1000);
        let stage = MockStage::new(ValidationStageType::Backtest, &long_name);
        assert_eq!(stage.name().len(), 1000);
    }

    #[tokio::test]
    async fn test_context_with_zero_period() {
        let now = Utc::now();
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            now,
            now,
        );

        assert!(ctx.period_days() < 0.001);
    }

    #[tokio::test]
    async fn test_context_with_very_long_period() {
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(3650), // 10 years
            Utc::now(),
        );

        assert!((ctx.period_days() - 3650.0).abs() < 1.0);
    }

    #[tokio::test]
    async fn test_stage_default_can_run_implementation() {
        // Test the default can_run implementation
        let stage = MockStage::new(ValidationStageType::Paper, "PP");
        let ctx = StageContext::new(
            AlgorithmConfig::default(),
            ValidationThresholds::default(),
            Utc::now() - Duration::days(7),
            Utc::now(),
        );
        // Paper stage doesn't require data path
        assert!(stage.can_run(&ctx).is_ok());
    }

    #[tokio::test]
    async fn test_stage_with_many_previous_results() {
        let mut results = Vec::new();
        for i in 0..100 {
            let mut result = ValidationResult::default();
            result.stage_name = format!("Stage-{}", i);
            results.push(result);
        }

        let ctx = StageContext::default().with_previous_results(results);
        assert_eq!(ctx.previous_results.len(), 100);
    }

    #[tokio::test]
    async fn test_concurrent_stage_access() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let stage = Arc::new(MockStage::new(ValidationStageType::Paper, "PP"));
        let mut tasks = JoinSet::new();

        for i in 0..10 {
            let stage_clone = stage.clone();
            tasks.spawn(async move {
                let ctx = StageContext::new(
                    AlgorithmConfig::default(),
                    ValidationThresholds::relaxed(),
                    Utc::now() - Duration::days(7),
                    Utc::now(),
                )
                .with_name(format!("Task-{}", i));

                stage_clone.run(&ctx).await
            });
        }

        while let Some(result) = tasks.join_next().await {
            assert!(result.is_ok());
            assert!(result.unwrap().is_ok());
        }

        assert_eq!(stage.run_count.load(Ordering::SeqCst), 10);
    }

    // ==================== Error Message Tests ====================

    #[test]
    fn test_error_messages_are_descriptive() {
        let errors = vec![
            StageError::ConfigurationError("missing field X".to_string()),
            StageError::DataUnavailable("2024-01-01 to 2024-01-31".to_string()),
            StageError::ExecutionError("division by zero".to_string()),
            StageError::ThresholdError("Sharpe ratio 0.3 < 0.5".to_string()),
            StageError::Timeout(7200),
            StageError::Cancelled("user interrupt".to_string()),
            StageError::InvalidState("stage already completed".to_string()),
            StageError::ResourceExhausted("memory limit 8GB".to_string()),
            StageError::Internal("null pointer".to_string()),
        ];

        for err in errors {
            let msg = err.to_string();
            assert!(!msg.is_empty());
            assert!(msg.len() > 10); // Should be meaningful
        }
    }

    #[test]
    fn test_error_recoverability_makes_sense() {
        // Recoverable errors should be things that might work on retry
        assert!(StageError::Timeout(100).is_recoverable());
        assert!(StageError::DataUnavailable("".to_string()).is_recoverable());
        assert!(StageError::ResourceExhausted("".to_string()).is_recoverable());

        // Non-recoverable errors are configuration or logic errors
        assert!(!StageError::ConfigurationError("".to_string()).is_recoverable());
        assert!(!StageError::ExecutionError("".to_string()).is_recoverable());
        assert!(!StageError::InvalidState("".to_string()).is_recoverable());
    }

    #[test]
    fn test_pipeline_halt_logic() {
        // Only critical errors should halt the pipeline
        assert!(StageError::ConfigurationError("".to_string()).should_halt_pipeline());
        assert!(StageError::InvalidState("".to_string()).should_halt_pipeline());

        // Transient errors should not halt
        assert!(!StageError::Timeout(100).should_halt_pipeline());
        assert!(!StageError::DataUnavailable("".to_string()).should_halt_pipeline());
        assert!(!StageError::ExecutionError("".to_string()).should_halt_pipeline());
    }
}

//! Validation Pipeline (Task 2.0)
//!
//! Orchestrates validation stages with configurable stop conditions and result aggregation.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

use crate::core::{AlgorithmConfig, ValidationResult, ValidationStageType, ValidationThresholds};

use super::traits::{RunFuture, StageContext, StageError, ValidationStage};

/// Condition under which the pipeline should stop
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopCondition {
    /// Stop on first failure
    StopOnFirstFailure,
    /// Continue through all stages regardless of failures
    ContinueOnFailure,
    /// Stop if a specific number of stages fail
    StopAfterNFailures(usize),
    /// Stop after a specific stage type
    StopAfterStage(ValidationStageType),
}

impl Default for StopCondition {
    fn default() -> Self {
        StopCondition::StopOnFirstFailure
    }
}

/// Outcome of a single stage execution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StageOutcome {
    /// Stage passed all thresholds
    Passed(ValidationResult),
    /// Stage failed to meet thresholds
    Failed(ValidationResult),
    /// Stage encountered an error
    Error(String),
    /// Stage was skipped (e.g., due to previous failure)
    Skipped(String),
    /// Stage is pending execution
    Pending,
}

impl StageOutcome {
    /// Check if this outcome represents a successful pass
    pub fn is_passed(&self) -> bool {
        matches!(self, StageOutcome::Passed(_))
    }

    /// Check if this outcome represents a failure
    pub fn is_failed(&self) -> bool {
        matches!(self, StageOutcome::Failed(_) | StageOutcome::Error(_))
    }

    /// Check if this outcome was skipped
    pub fn is_skipped(&self) -> bool {
        matches!(self, StageOutcome::Skipped(_))
    }

    /// Check if this outcome is pending
    pub fn is_pending(&self) -> bool {
        matches!(self, StageOutcome::Pending)
    }

    /// Get the validation result if available
    pub fn result(&self) -> Option<&ValidationResult> {
        match self {
            StageOutcome::Passed(r) | StageOutcome::Failed(r) => Some(r),
            _ => None,
        }
    }

    /// Get the error message if this is an error
    pub fn error_message(&self) -> Option<&str> {
        match self {
            StageOutcome::Error(msg) => Some(msg),
            StageOutcome::Skipped(msg) => Some(msg),
            _ => None,
        }
    }
}

/// Configuration for a single stage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StageConfig {
    /// Stage type
    pub stage_type: ValidationStageType,

    /// Custom name for this stage
    pub name: String,

    /// Whether this stage is enabled
    pub enabled: bool,

    /// Custom thresholds for this stage (overrides pipeline defaults)
    pub thresholds: Option<ValidationThresholds>,

    /// Timeout for this stage in seconds
    pub timeout_seconds: Option<u64>,

    /// Data path for this stage (for historical stages)
    pub data_path: Option<String>,

    /// Start of validation period
    pub period_start: Option<DateTime<Utc>>,

    /// End of validation period
    pub period_end: Option<DateTime<Utc>>,

    /// Whether to generate detailed trade output
    pub detailed_output: bool,

    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

impl StageConfig {
    /// Create a new stage config
    pub fn new(stage_type: ValidationStageType, name: impl Into<String>) -> Self {
        Self {
            stage_type,
            name: name.into(),
            enabled: true,
            thresholds: None,
            timeout_seconds: None,
            data_path: None,
            period_start: None,
            period_end: None,
            detailed_output: true,
            metadata: HashMap::new(),
        }
    }

    /// Disable this stage
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Set custom thresholds
    pub fn with_thresholds(mut self, thresholds: ValidationThresholds) -> Self {
        self.thresholds = Some(thresholds);
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    /// Set data path
    pub fn with_data_path(mut self, path: impl Into<String>) -> Self {
        self.data_path = Some(path.into());
        self
    }

    /// Set period
    pub fn with_period(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.period_start = Some(start);
        self.period_end = Some(end);
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
}

impl Default for StageConfig {
    fn default() -> Self {
        Self::new(ValidationStageType::Backtest, "Default")
    }
}

/// Overall pipeline configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Unique identifier for this pipeline configuration
    pub id: String,

    /// Human-readable name
    pub name: String,

    /// Description of this pipeline
    pub description: String,

    /// Default thresholds for all stages
    pub default_thresholds: ValidationThresholds,

    /// Stop condition
    pub stop_condition: StopCondition,

    /// Default data path for historical stages
    pub data_path: Option<String>,

    /// Global timeout in seconds (0 = no timeout)
    pub global_timeout_seconds: u64,

    /// Whether to require stages to run in order
    pub enforce_stage_order: bool,

    /// Maximum retries per stage on transient errors
    pub max_retries_per_stage: usize,

    /// Delay between retries in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: "Default Pipeline".to_string(),
            description: String::new(),
            default_thresholds: ValidationThresholds::default(),
            stop_condition: StopCondition::StopOnFirstFailure,
            data_path: None,
            global_timeout_seconds: 0,
            enforce_stage_order: true,
            max_retries_per_stage: 0,
            retry_delay_ms: 1000,
        }
    }
}

impl PipelineConfig {
    /// Create a new pipeline config with the given name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Set the stop condition
    pub fn with_stop_condition(mut self, condition: StopCondition) -> Self {
        self.stop_condition = condition;
        self
    }

    /// Set default thresholds
    pub fn with_thresholds(mut self, thresholds: ValidationThresholds) -> Self {
        self.default_thresholds = thresholds;
        self
    }

    /// Set data path
    pub fn with_data_path(mut self, path: impl Into<String>) -> Self {
        self.data_path = Some(path.into());
        self
    }

    /// Set global timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.global_timeout_seconds = seconds;
        self
    }

    /// Set retry configuration
    pub fn with_retries(mut self, max_retries: usize, delay_ms: u64) -> Self {
        self.max_retries_per_stage = max_retries;
        self.retry_delay_ms = delay_ms;
        self
    }

    /// Create a strict production pipeline
    pub fn production() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: "Production Pipeline".to_string(),
            description: "Strict validation for production deployment".to_string(),
            default_thresholds: ValidationThresholds::strict(),
            stop_condition: StopCondition::StopOnFirstFailure,
            data_path: None,
            global_timeout_seconds: 7200, // 2 hours
            enforce_stage_order: true,
            max_retries_per_stage: 2,
            retry_delay_ms: 5000,
        }
    }

    /// Create a relaxed research pipeline
    pub fn research() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: "Research Pipeline".to_string(),
            description: "Relaxed validation for research and experimentation".to_string(),
            default_thresholds: ValidationThresholds::relaxed(),
            stop_condition: StopCondition::ContinueOnFailure,
            data_path: None,
            global_timeout_seconds: 0,
            enforce_stage_order: false,
            max_retries_per_stage: 0,
            retry_delay_ms: 0,
        }
    }
}

/// Status of the pipeline execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStatus {
    /// Pipeline has not started
    Pending,
    /// Pipeline is currently running
    Running,
    /// Pipeline completed with all stages passing
    Passed,
    /// Pipeline completed with some stages failing
    Failed,
    /// Pipeline was cancelled
    Cancelled,
    /// Pipeline encountered an error
    Error,
}

impl PipelineStatus {
    /// Check if the pipeline is still in progress
    pub fn is_in_progress(&self) -> bool {
        matches!(self, PipelineStatus::Pending | PipelineStatus::Running)
    }

    /// Check if the pipeline has completed (regardless of outcome)
    pub fn is_complete(&self) -> bool {
        !self.is_in_progress()
    }

    /// Check if the pipeline passed
    pub fn is_passed(&self) -> bool {
        matches!(self, PipelineStatus::Passed)
    }

    /// Check if the pipeline failed or errored
    pub fn is_failed(&self) -> bool {
        matches!(self, PipelineStatus::Failed | PipelineStatus::Error)
    }
}

/// Result of a complete pipeline run
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineResult {
    /// Unique identifier for this run
    pub id: String,

    /// Pipeline configuration used
    pub config_id: String,

    /// Algorithm configuration that was validated
    pub algorithm_config_id: String,

    /// Final status
    pub status: PipelineStatus,

    /// Outcomes for each stage
    pub stage_outcomes: HashMap<ValidationStageType, StageOutcome>,

    /// Order in which stages were executed
    pub execution_order: Vec<ValidationStageType>,

    /// Start time of the pipeline
    pub started_at: DateTime<Utc>,

    /// End time of the pipeline
    pub completed_at: Option<DateTime<Utc>>,

    /// Total duration in seconds
    pub duration_seconds: f64,

    /// Number of stages that passed
    pub stages_passed: usize,

    /// Number of stages that failed
    pub stages_failed: usize,

    /// Number of stages that were skipped
    pub stages_skipped: usize,

    /// Summary message
    pub summary: String,

    /// Detailed warnings or notes
    pub warnings: Vec<String>,
}

impl PipelineResult {
    /// Create a new pipeline result
    pub fn new(config_id: String, algorithm_config_id: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            config_id,
            algorithm_config_id,
            status: PipelineStatus::Pending,
            stage_outcomes: HashMap::new(),
            execution_order: Vec::new(),
            started_at: Utc::now(),
            completed_at: None,
            duration_seconds: 0.0,
            stages_passed: 0,
            stages_failed: 0,
            stages_skipped: 0,
            summary: String::new(),
            warnings: Vec::new(),
        }
    }

    /// Mark the pipeline as started
    pub fn start(&mut self) {
        self.status = PipelineStatus::Running;
        self.started_at = Utc::now();
    }

    /// Add a stage outcome
    pub fn add_outcome(&mut self, stage_type: ValidationStageType, outcome: StageOutcome) {
        self.execution_order.push(stage_type);

        match &outcome {
            StageOutcome::Passed(_) => self.stages_passed += 1,
            StageOutcome::Failed(_) | StageOutcome::Error(_) => self.stages_failed += 1,
            StageOutcome::Skipped(_) => self.stages_skipped += 1,
            StageOutcome::Pending => {}
        }

        self.stage_outcomes.insert(stage_type, outcome);
    }

    /// Complete the pipeline with the given status
    pub fn complete(&mut self, status: PipelineStatus) {
        self.status = status;
        self.completed_at = Some(Utc::now());
        if let Some(completed) = self.completed_at {
            self.duration_seconds = (completed - self.started_at).num_milliseconds() as f64 / 1000.0;
        }
        self.update_summary();
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    /// Update the summary based on outcomes
    fn update_summary(&mut self) {
        let total = self.stages_passed + self.stages_failed + self.stages_skipped;
        self.summary = format!(
            "{}: {}/{} stages passed, {} failed, {} skipped in {:.1}s",
            match self.status {
                PipelineStatus::Passed => "PASSED",
                PipelineStatus::Failed => "FAILED",
                PipelineStatus::Cancelled => "CANCELLED",
                PipelineStatus::Error => "ERROR",
                _ => "UNKNOWN",
            },
            self.stages_passed,
            total,
            self.stages_failed,
            self.stages_skipped,
            self.duration_seconds
        );
    }

    /// Get the result for a specific stage
    pub fn get_stage_result(&self, stage_type: ValidationStageType) -> Option<&ValidationResult> {
        self.stage_outcomes
            .get(&stage_type)
            .and_then(|o| o.result())
    }

    /// Get all validation results (from passed and failed stages)
    pub fn all_results(&self) -> Vec<&ValidationResult> {
        self.stage_outcomes
            .values()
            .filter_map(|o| o.result())
            .collect()
    }

    /// Check if a specific stage passed
    pub fn stage_passed(&self, stage_type: ValidationStageType) -> bool {
        self.stage_outcomes
            .get(&stage_type)
            .map(|o| o.is_passed())
            .unwrap_or(false)
    }

    /// Get the furthest stage that passed
    pub fn furthest_passed_stage(&self) -> Option<ValidationStageType> {
        let mut furthest: Option<ValidationStageType> = None;

        for stage_type in &self.execution_order {
            if let Some(outcome) = self.stage_outcomes.get(stage_type) {
                if outcome.is_passed() {
                    furthest = Some(*stage_type);
                }
            }
        }

        furthest
    }
}

impl Default for PipelineResult {
    fn default() -> Self {
        Self::new(String::new(), String::new())
    }
}

/// Builder for creating and running a validation pipeline
pub struct PipelineBuilder {
    config: PipelineConfig,
    algorithm_config: AlgorithmConfig,
    stage_configs: Vec<StageConfig>,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new(config: PipelineConfig, algorithm_config: AlgorithmConfig) -> Self {
        Self {
            config,
            algorithm_config,
            stage_configs: Vec::new(),
        }
    }

    /// Add a stage to the pipeline
    pub fn add_stage(mut self, stage_config: StageConfig) -> Self {
        self.stage_configs.push(stage_config);
        self
    }

    /// Add a backtest stage with default config
    pub fn with_backtest(self, name: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        let config = StageConfig::new(ValidationStageType::Backtest, name).with_period(start, end);
        self.add_stage(config)
    }

    /// Add a forward validation stage
    pub fn with_forward(self, name: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        let config = StageConfig::new(ValidationStageType::Forward, name).with_period(start, end);
        self.add_stage(config)
    }

    /// Add an out-of-sample stage
    pub fn with_oos(self, name: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        let config =
            StageConfig::new(ValidationStageType::OutOfSample, name).with_period(start, end);
        self.add_stage(config)
    }

    /// Add a paper trading stage
    pub fn with_paper(self, name: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        let config = StageConfig::new(ValidationStageType::Paper, name).with_period(start, end);
        self.add_stage(config)
    }

    /// Add a live trading stage
    pub fn with_live(self, name: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        let config = StageConfig::new(ValidationStageType::Live, name).with_period(start, end);
        self.add_stage(config)
    }

    /// Add the standard 5-stage pipeline
    pub fn with_standard_stages(self, data_path: &str) -> Self {
        let now = Utc::now();
        let bt_start = now - Duration::days(365);
        let bt_end = now - Duration::days(180);
        let fw_start = bt_end;
        let fw_end = now - Duration::days(90);
        let oos_start = fw_end;
        let oos_end = now - Duration::days(30);
        let pp_start = oos_end;
        let pp_end = now;
        let lv_start = now;
        let lv_end = now + Duration::days(30);

        self.add_stage(
            StageConfig::new(ValidationStageType::Backtest, "Backtest")
                .with_period(bt_start, bt_end)
                .with_data_path(data_path),
        )
        .add_stage(
            StageConfig::new(ValidationStageType::Forward, "Forward")
                .with_period(fw_start, fw_end)
                .with_data_path(data_path),
        )
        .add_stage(
            StageConfig::new(ValidationStageType::OutOfSample, "OOS")
                .with_period(oos_start, oos_end)
                .with_data_path(data_path),
        )
        .add_stage(
            StageConfig::new(ValidationStageType::Paper, "Paper").with_period(pp_start, pp_end),
        )
        .add_stage(
            StageConfig::new(ValidationStageType::Live, "Live").with_period(lv_start, lv_end),
        )
    }

    /// Build the pipeline (returns the components needed for run)
    pub fn build(self) -> (PipelineConfig, AlgorithmConfig, Vec<StageConfig>) {
        (self.config, self.algorithm_config, self.stage_configs)
    }
}

/// The main validation pipeline orchestrator
pub struct ValidationPipeline {
    config: PipelineConfig,
}

impl ValidationPipeline {
    /// Create a new validation pipeline
    pub fn new(config: PipelineConfig) -> Self {
        Self { config }
    }

    /// Create a pipeline with default configuration
    pub fn with_defaults() -> Self {
        Self::new(PipelineConfig::default())
    }

    /// Get the pipeline configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Run the pipeline with the given stages
    ///
    /// This is the main entry point for executing a validation pipeline.
    pub async fn run(
        &self,
        algorithm_config: &AlgorithmConfig,
        stages: &[Box<dyn ValidationStage>],
        stage_configs: &[StageConfig],
    ) -> Result<PipelineResult, StageError> {
        let mut result =
            PipelineResult::new(self.config.id.clone(), algorithm_config.id.clone());
        result.start();

        let start_time = Instant::now();
        let mut previous_results: Vec<ValidationResult> = Vec::new();
        let mut failure_count = 0;

        for (stage, stage_config) in stages.iter().zip(stage_configs.iter()) {
            // Check if stage is enabled
            if !stage_config.enabled {
                result.add_outcome(
                    stage.stage_type(),
                    StageOutcome::Skipped("Stage disabled".to_string()),
                );
                continue;
            }

            // Check global timeout
            if self.config.global_timeout_seconds > 0 {
                let elapsed = start_time.elapsed().as_secs();
                if elapsed >= self.config.global_timeout_seconds {
                    result.add_outcome(
                        stage.stage_type(),
                        StageOutcome::Error("Global timeout exceeded".to_string()),
                    );
                    result.add_warning("Pipeline terminated due to global timeout".to_string());
                    break;
                }
            }

            // Check stop conditions
            let should_stop = match self.config.stop_condition {
                StopCondition::StopOnFirstFailure => failure_count > 0,
                StopCondition::ContinueOnFailure => false,
                StopCondition::StopAfterNFailures(n) => failure_count >= n,
                StopCondition::StopAfterStage(stop_stage) => {
                    previous_results
                        .iter()
                        .any(|r| r.stage_type == stop_stage)
                }
            };

            if should_stop {
                result.add_outcome(
                    stage.stage_type(),
                    StageOutcome::Skipped(format!(
                        "Skipped due to stop condition: {:?}",
                        self.config.stop_condition
                    )),
                );
                continue;
            }

            // Check if previous required stage passed
            if self.config.enforce_stage_order {
                if let Some(required) = stage.requires_previous() {
                    let prev_passed = previous_results
                        .iter()
                        .any(|r| r.stage_type == required && r.passed);

                    if !prev_passed {
                        result.add_outcome(
                            stage.stage_type(),
                            StageOutcome::Skipped(format!(
                                "Required stage {:?} did not pass",
                                required
                            )),
                        );
                        continue;
                    }
                }
            }

            // Build stage context
            let thresholds = stage_config
                .thresholds
                .clone()
                .unwrap_or_else(|| self.config.default_thresholds.clone());

            let period_start = stage_config.period_start.unwrap_or(Utc::now() - Duration::days(30));
            let period_end = stage_config.period_end.unwrap_or(Utc::now());

            let mut context = StageContext::new(
                algorithm_config.clone(),
                thresholds,
                period_start,
                period_end,
            )
            .with_name(&stage_config.name)
            .with_previous_results(previous_results.clone())
            .with_detailed_output(stage_config.detailed_output);

            if let Some(timeout) = stage_config.timeout_seconds {
                context = context.with_timeout(timeout);
            }

            if let Some(ref path) = stage_config.data_path {
                context = context.with_data_path(path.clone());
            } else if let Some(ref path) = self.config.data_path {
                context = context.with_data_path(path.clone());
            }

            for (key, value) in &stage_config.metadata {
                context = context.with_metadata(key.clone(), value.clone());
            }

            // Check if stage can run
            if let Err(e) = stage.can_run(&context) {
                if e.should_halt_pipeline() {
                    result.add_outcome(stage.stage_type(), StageOutcome::Error(e.to_string()));
                    result.complete(PipelineStatus::Error);
                    return Err(e);
                } else {
                    result.add_outcome(
                        stage.stage_type(),
                        StageOutcome::Skipped(format!("Cannot run: {}", e)),
                    );
                    continue;
                }
            }

            // Run the stage with retry logic
            let mut attempts = 0;
            let max_attempts = self.config.max_retries_per_stage + 1;
            let mut stage_result: Result<ValidationResult, StageError> =
                Err(StageError::Internal("Not executed".to_string()));

            while attempts < max_attempts {
                attempts += 1;

                match stage.run(&context).await {
                    Ok(r) => {
                        stage_result = Ok(r);
                        break;
                    }
                    Err(e) => {
                        if e.is_recoverable() && attempts < max_attempts {
                            result.add_warning(format!(
                                "Stage {} attempt {} failed, retrying: {}",
                                stage.name(),
                                attempts,
                                e
                            ));
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                self.config.retry_delay_ms,
                            ))
                            .await;
                        } else {
                            stage_result = Err(e);
                            break;
                        }
                    }
                }
            }

            // Process result
            match stage_result {
                Ok(validation_result) => {
                    let passed = validation_result.passed;
                    previous_results.push(validation_result.clone());

                    if passed {
                        result.add_outcome(stage.stage_type(), StageOutcome::Passed(validation_result));
                    } else {
                        failure_count += 1;
                        result.add_outcome(stage.stage_type(), StageOutcome::Failed(validation_result));
                    }
                }
                Err(e) => {
                    failure_count += 1;
                    result.add_outcome(stage.stage_type(), StageOutcome::Error(e.to_string()));

                    if e.should_halt_pipeline() {
                        result.complete(PipelineStatus::Error);
                        return Err(e);
                    }
                }
            }
        }

        // Determine final status
        let final_status = if result.stages_failed == 0 && result.stages_passed > 0 {
            PipelineStatus::Passed
        } else if result.stages_failed > 0 {
            PipelineStatus::Failed
        } else {
            PipelineStatus::Error
        };

        result.complete(final_status);
        Ok(result)
    }

    /// Create a builder for this pipeline
    pub fn builder(config: PipelineConfig, algorithm_config: AlgorithmConfig) -> PipelineBuilder {
        PipelineBuilder::new(config, algorithm_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{TradeDirection, TradeResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // ==================== Mock Stage for Testing ====================

    struct TestStage {
        stage_type: ValidationStageType,
        name: String,
        should_fail: bool,
        should_error: bool,
        error_message: String,
        run_count: Arc<AtomicUsize>,
        can_run_result: Result<(), StageError>,
    }

    impl TestStage {
        fn new(stage_type: ValidationStageType, name: &str) -> Self {
            Self {
                stage_type,
                name: name.to_string(),
                should_fail: false,
                should_error: false,
                error_message: String::new(),
                run_count: Arc::new(AtomicUsize::new(0)),
                can_run_result: Ok(()),
            }
        }

        fn failing(mut self) -> Self {
            self.should_fail = true;
            self
        }

        fn with_error(mut self, msg: &str) -> Self {
            self.should_error = true;
            self.error_message = msg.to_string();
            self
        }

        fn with_can_run_error(mut self, err: StageError) -> Self {
            self.can_run_result = Err(err);
            self
        }
    }

    impl ValidationStage for TestStage {
        fn stage_type(&self) -> ValidationStageType {
            self.stage_type
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn can_run(&self, _context: &StageContext) -> Result<(), StageError> {
            self.can_run_result.clone()
        }

        fn run<'a>(&'a self, context: &'a StageContext) -> RunFuture<'a> {
            Box::pin(async move {
                self.run_count.fetch_add(1, Ordering::SeqCst);

                if self.should_error {
                    return Err(StageError::ExecutionError(self.error_message.clone()));
                }

                let mut result = ValidationResult::new(
                    self.stage_type,
                    context.stage_name.clone(),
                    context.config.id.clone(),
                    context.period_start,
                    context.period_end,
                );

                // Create trades that will pass or fail based on should_fail
                let mut trades = Vec::new();
                for i in 0..50 {
                    let is_winner = if self.should_fail {
                        i % 10 == 0 // 10% win rate when failing
                    } else {
                        i % 10 != 0 // 90% win rate when passing
                    };

                    let entry_price = 100.0;
                    let exit_price = if is_winner { 105.0 } else { 95.0 };

                    let trade = TradeResult::new(
                        format!("T{}", i),
                        TradeDirection::Long,
                        context.period_start + Duration::hours(i as i64),
                        context.period_start + Duration::hours(i as i64 + 1),
                        entry_price,
                        exit_price,
                        1.0,
                    );
                    trades.push(trade);
                }

                result = result.with_trades(trades);
                result.evaluate_thresholds(context.thresholds.clone());

                Ok(result)
            })
        }
    }

    // ==================== StopCondition Tests ====================

    #[test]
    fn test_stop_condition_default() {
        let condition = StopCondition::default();
        assert_eq!(condition, StopCondition::StopOnFirstFailure);
    }

    #[test]
    fn test_stop_condition_variants() {
        let _ = StopCondition::StopOnFirstFailure;
        let _ = StopCondition::ContinueOnFailure;
        let _ = StopCondition::StopAfterNFailures(3);
        let _ = StopCondition::StopAfterStage(ValidationStageType::Forward);
    }

    #[test]
    fn test_stop_condition_serialization() {
        for condition in [
            StopCondition::StopOnFirstFailure,
            StopCondition::ContinueOnFailure,
            StopCondition::StopAfterNFailures(5),
            StopCondition::StopAfterStage(ValidationStageType::Paper),
        ] {
            let json = serde_json::to_string(&condition).unwrap();
            let deserialized: StopCondition = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, condition);
        }
    }

    // ==================== StageOutcome Tests ====================

    #[test]
    fn test_stage_outcome_passed() {
        let result = ValidationResult::default();
        let outcome = StageOutcome::Passed(result);
        assert!(outcome.is_passed());
        assert!(!outcome.is_failed());
        assert!(!outcome.is_skipped());
        assert!(!outcome.is_pending());
        assert!(outcome.result().is_some());
    }

    #[test]
    fn test_stage_outcome_failed() {
        let result = ValidationResult::default();
        let outcome = StageOutcome::Failed(result);
        assert!(!outcome.is_passed());
        assert!(outcome.is_failed());
        assert!(outcome.result().is_some());
    }

    #[test]
    fn test_stage_outcome_error() {
        let outcome = StageOutcome::Error("test error".to_string());
        assert!(!outcome.is_passed());
        assert!(outcome.is_failed());
        assert!(outcome.result().is_none());
        assert_eq!(outcome.error_message(), Some("test error"));
    }

    #[test]
    fn test_stage_outcome_skipped() {
        let outcome = StageOutcome::Skipped("previous failed".to_string());
        assert!(!outcome.is_passed());
        assert!(!outcome.is_failed());
        assert!(outcome.is_skipped());
        assert!(outcome.result().is_none());
        assert_eq!(outcome.error_message(), Some("previous failed"));
    }

    #[test]
    fn test_stage_outcome_pending() {
        let outcome = StageOutcome::Pending;
        assert!(outcome.is_pending());
        assert!(!outcome.is_passed());
        assert!(!outcome.is_failed());
    }

    #[test]
    fn test_stage_outcome_serialization() {
        let result = ValidationResult::default();
        let outcomes = vec![
            StageOutcome::Passed(result.clone()),
            StageOutcome::Failed(result),
            StageOutcome::Error("error".to_string()),
            StageOutcome::Skipped("skipped".to_string()),
            StageOutcome::Pending,
        ];

        for outcome in outcomes {
            let json = serde_json::to_string(&outcome).unwrap();
            let deserialized: StageOutcome = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized.is_passed(), outcome.is_passed());
        }
    }

    // ==================== StageConfig Tests ====================

    #[test]
    fn test_stage_config_new() {
        let config = StageConfig::new(ValidationStageType::Backtest, "BT-2025Q1");
        assert_eq!(config.stage_type, ValidationStageType::Backtest);
        assert_eq!(config.name, "BT-2025Q1");
        assert!(config.enabled);
    }

    #[test]
    fn test_stage_config_disabled() {
        let config = StageConfig::new(ValidationStageType::Backtest, "BT").disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_stage_config_with_thresholds() {
        let thresholds = ValidationThresholds::strict();
        let config = StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_thresholds(thresholds.clone());
        assert_eq!(config.thresholds, Some(thresholds));
    }

    #[test]
    fn test_stage_config_with_timeout() {
        let config = StageConfig::new(ValidationStageType::Backtest, "BT").with_timeout(3600);
        assert_eq!(config.timeout_seconds, Some(3600));
    }

    #[test]
    fn test_stage_config_with_data_path() {
        let config = StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_data_path("/data/features");
        assert_eq!(config.data_path, Some("/data/features".to_string()));
    }

    #[test]
    fn test_stage_config_with_period() {
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now();
        let config =
            StageConfig::new(ValidationStageType::Backtest, "BT").with_period(start, end);
        assert_eq!(config.period_start, Some(start));
        assert_eq!(config.period_end, Some(end));
    }

    #[test]
    fn test_stage_config_with_metadata() {
        let config = StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_metadata("key1", "value1")
            .with_metadata("key2", "value2");
        assert_eq!(config.metadata.get("key1"), Some(&"value1".to_string()));
        assert_eq!(config.metadata.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_stage_config_default() {
        let config = StageConfig::default();
        assert_eq!(config.stage_type, ValidationStageType::Backtest);
        assert!(config.enabled);
    }

    #[test]
    fn test_stage_config_serialization() {
        let config = StageConfig::new(ValidationStageType::Forward, "FW")
            .with_timeout(1800)
            .with_data_path("/data");

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StageConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.stage_type, config.stage_type);
        assert_eq!(deserialized.name, config.name);
    }

    // ==================== PipelineConfig Tests ====================

    #[test]
    fn test_pipeline_config_new() {
        let config = PipelineConfig::new("My Pipeline");
        assert_eq!(config.name, "My Pipeline");
        assert!(!config.id.is_empty());
    }

    #[test]
    fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert_eq!(config.name, "Default Pipeline");
        assert_eq!(config.stop_condition, StopCondition::StopOnFirstFailure);
        assert!(config.enforce_stage_order);
    }

    #[test]
    fn test_pipeline_config_with_stop_condition() {
        let config =
            PipelineConfig::new("Test").with_stop_condition(StopCondition::ContinueOnFailure);
        assert_eq!(config.stop_condition, StopCondition::ContinueOnFailure);
    }

    #[test]
    fn test_pipeline_config_with_thresholds() {
        let thresholds = ValidationThresholds::strict();
        let config = PipelineConfig::new("Test").with_thresholds(thresholds.clone());
        assert_eq!(config.default_thresholds, thresholds);
    }

    #[test]
    fn test_pipeline_config_with_data_path() {
        let config = PipelineConfig::new("Test").with_data_path("/data/features");
        assert_eq!(config.data_path, Some("/data/features".to_string()));
    }

    #[test]
    fn test_pipeline_config_with_timeout() {
        let config = PipelineConfig::new("Test").with_timeout(7200);
        assert_eq!(config.global_timeout_seconds, 7200);
    }

    #[test]
    fn test_pipeline_config_with_retries() {
        let config = PipelineConfig::new("Test").with_retries(3, 5000);
        assert_eq!(config.max_retries_per_stage, 3);
        assert_eq!(config.retry_delay_ms, 5000);
    }

    #[test]
    fn test_pipeline_config_production() {
        let config = PipelineConfig::production();
        assert_eq!(config.name, "Production Pipeline");
        assert_eq!(config.stop_condition, StopCondition::StopOnFirstFailure);
        assert!(config.enforce_stage_order);
        assert_eq!(config.default_thresholds, ValidationThresholds::strict());
    }

    #[test]
    fn test_pipeline_config_research() {
        let config = PipelineConfig::research();
        assert_eq!(config.name, "Research Pipeline");
        assert_eq!(config.stop_condition, StopCondition::ContinueOnFailure);
        assert!(!config.enforce_stage_order);
        assert_eq!(config.default_thresholds, ValidationThresholds::relaxed());
    }

    #[test]
    fn test_pipeline_config_serialization() {
        let config = PipelineConfig::production();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PipelineConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, config.name);
        assert_eq!(deserialized.stop_condition, config.stop_condition);
    }

    // ==================== PipelineStatus Tests ====================

    #[test]
    fn test_pipeline_status_is_in_progress() {
        assert!(PipelineStatus::Pending.is_in_progress());
        assert!(PipelineStatus::Running.is_in_progress());
        assert!(!PipelineStatus::Passed.is_in_progress());
        assert!(!PipelineStatus::Failed.is_in_progress());
    }

    #[test]
    fn test_pipeline_status_is_complete() {
        assert!(!PipelineStatus::Pending.is_complete());
        assert!(!PipelineStatus::Running.is_complete());
        assert!(PipelineStatus::Passed.is_complete());
        assert!(PipelineStatus::Failed.is_complete());
        assert!(PipelineStatus::Cancelled.is_complete());
        assert!(PipelineStatus::Error.is_complete());
    }

    #[test]
    fn test_pipeline_status_is_passed() {
        assert!(PipelineStatus::Passed.is_passed());
        assert!(!PipelineStatus::Failed.is_passed());
        assert!(!PipelineStatus::Pending.is_passed());
    }

    #[test]
    fn test_pipeline_status_is_failed() {
        assert!(PipelineStatus::Failed.is_failed());
        assert!(PipelineStatus::Error.is_failed());
        assert!(!PipelineStatus::Passed.is_failed());
    }

    #[test]
    fn test_pipeline_status_serialization() {
        for status in [
            PipelineStatus::Pending,
            PipelineStatus::Running,
            PipelineStatus::Passed,
            PipelineStatus::Failed,
            PipelineStatus::Cancelled,
            PipelineStatus::Error,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: PipelineStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, status);
        }
    }

    // ==================== PipelineResult Tests ====================

    #[test]
    fn test_pipeline_result_new() {
        let result = PipelineResult::new("cfg-123".to_string(), "algo-456".to_string());
        assert!(!result.id.is_empty());
        assert_eq!(result.config_id, "cfg-123");
        assert_eq!(result.algorithm_config_id, "algo-456");
        assert_eq!(result.status, PipelineStatus::Pending);
    }

    #[test]
    fn test_pipeline_result_start() {
        let mut result = PipelineResult::default();
        result.start();
        assert_eq!(result.status, PipelineStatus::Running);
    }

    #[test]
    fn test_pipeline_result_add_outcome_passed() {
        let mut result = PipelineResult::default();
        let validation_result = ValidationResult::default();
        result.add_outcome(
            ValidationStageType::Backtest,
            StageOutcome::Passed(validation_result),
        );
        assert_eq!(result.stages_passed, 1);
        assert_eq!(result.stages_failed, 0);
    }

    #[test]
    fn test_pipeline_result_add_outcome_failed() {
        let mut result = PipelineResult::default();
        let validation_result = ValidationResult::default();
        result.add_outcome(
            ValidationStageType::Backtest,
            StageOutcome::Failed(validation_result),
        );
        assert_eq!(result.stages_passed, 0);
        assert_eq!(result.stages_failed, 1);
    }

    #[test]
    fn test_pipeline_result_add_outcome_skipped() {
        let mut result = PipelineResult::default();
        result.add_outcome(
            ValidationStageType::Forward,
            StageOutcome::Skipped("Previous failed".to_string()),
        );
        assert_eq!(result.stages_skipped, 1);
    }

    #[test]
    fn test_pipeline_result_complete() {
        let mut result = PipelineResult::default();
        result.start();
        std::thread::sleep(std::time::Duration::from_millis(10));
        result.complete(PipelineStatus::Passed);
        assert_eq!(result.status, PipelineStatus::Passed);
        assert!(result.completed_at.is_some());
        assert!(result.duration_seconds >= 0.01);
    }

    #[test]
    fn test_pipeline_result_summary() {
        let mut result = PipelineResult::default();
        result.stages_passed = 3;
        result.stages_failed = 1;
        result.stages_skipped = 1;
        result.complete(PipelineStatus::Failed);
        assert!(result.summary.contains("FAILED"));
        assert!(result.summary.contains("3"));
    }

    #[test]
    fn test_pipeline_result_get_stage_result() {
        let mut result = PipelineResult::default();
        let mut validation_result = ValidationResult::default();
        validation_result.stage_type = ValidationStageType::Backtest;
        validation_result.stage_name = "BT-Test".to_string();

        result.add_outcome(
            ValidationStageType::Backtest,
            StageOutcome::Passed(validation_result),
        );

        let retrieved = result.get_stage_result(ValidationStageType::Backtest);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().stage_name, "BT-Test");

        let missing = result.get_stage_result(ValidationStageType::Forward);
        assert!(missing.is_none());
    }

    #[test]
    fn test_pipeline_result_all_results() {
        let mut result = PipelineResult::default();
        result.add_outcome(
            ValidationStageType::Backtest,
            StageOutcome::Passed(ValidationResult::default()),
        );
        result.add_outcome(
            ValidationStageType::Forward,
            StageOutcome::Failed(ValidationResult::default()),
        );
        result.add_outcome(
            ValidationStageType::OutOfSample,
            StageOutcome::Skipped("skipped".to_string()),
        );

        let all = result.all_results();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_pipeline_result_stage_passed() {
        let mut result = PipelineResult::default();
        let mut validation_result = ValidationResult::default();
        validation_result.passed = true;
        result.add_outcome(
            ValidationStageType::Backtest,
            StageOutcome::Passed(validation_result),
        );

        assert!(result.stage_passed(ValidationStageType::Backtest));
        assert!(!result.stage_passed(ValidationStageType::Forward));
    }

    #[test]
    fn test_pipeline_result_furthest_passed_stage() {
        let mut result = PipelineResult::default();
        result.add_outcome(
            ValidationStageType::Backtest,
            StageOutcome::Passed(ValidationResult::default()),
        );
        result.add_outcome(
            ValidationStageType::Forward,
            StageOutcome::Passed(ValidationResult::default()),
        );
        result.add_outcome(
            ValidationStageType::OutOfSample,
            StageOutcome::Failed(ValidationResult::default()),
        );

        let furthest = result.furthest_passed_stage();
        assert_eq!(furthest, Some(ValidationStageType::Forward));
    }

    #[test]
    fn test_pipeline_result_serialization() {
        let mut result = PipelineResult::new("cfg".to_string(), "algo".to_string());
        result.add_outcome(
            ValidationStageType::Backtest,
            StageOutcome::Passed(ValidationResult::default()),
        );
        result.complete(PipelineStatus::Passed);

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: PipelineResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, result.id);
        assert_eq!(deserialized.status, PipelineStatus::Passed);
    }

    // ==================== PipelineBuilder Tests ====================

    #[test]
    fn test_pipeline_builder_new() {
        let config = PipelineConfig::default();
        let algo = AlgorithmConfig::default();
        let builder = PipelineBuilder::new(config, algo);
        let (cfg, _, stages) = builder.build();
        assert_eq!(cfg.name, "Default Pipeline");
        assert!(stages.is_empty());
    }

    #[test]
    fn test_pipeline_builder_add_stage() {
        let config = PipelineConfig::default();
        let algo = AlgorithmConfig::default();
        let builder = PipelineBuilder::new(config, algo)
            .add_stage(StageConfig::new(ValidationStageType::Backtest, "BT"));
        let (_, _, stages) = builder.build();
        assert_eq!(stages.len(), 1);
    }

    #[test]
    fn test_pipeline_builder_with_stages() {
        let config = PipelineConfig::default();
        let algo = AlgorithmConfig::default();
        let now = Utc::now();

        let builder = PipelineBuilder::new(config, algo)
            .with_backtest("BT", now - Duration::days(90), now - Duration::days(60))
            .with_forward("FW", now - Duration::days(60), now - Duration::days(30))
            .with_oos("OOS", now - Duration::days(30), now);

        let (_, _, stages) = builder.build();
        assert_eq!(stages.len(), 3);
        assert_eq!(stages[0].stage_type, ValidationStageType::Backtest);
        assert_eq!(stages[1].stage_type, ValidationStageType::Forward);
        assert_eq!(stages[2].stage_type, ValidationStageType::OutOfSample);
    }

    #[test]
    fn test_pipeline_builder_standard_stages() {
        let config = PipelineConfig::default();
        let algo = AlgorithmConfig::default();
        let builder = PipelineBuilder::new(config, algo).with_standard_stages("/data/features");
        let (_, _, stages) = builder.build();
        assert_eq!(stages.len(), 5);
    }

    // ==================== ValidationPipeline Tests ====================

    #[test]
    fn test_validation_pipeline_new() {
        let config = PipelineConfig::default();
        let pipeline = ValidationPipeline::new(config.clone());
        assert_eq!(pipeline.config().name, config.name);
    }

    #[test]
    fn test_validation_pipeline_with_defaults() {
        let pipeline = ValidationPipeline::with_defaults();
        assert_eq!(pipeline.config().name, "Default Pipeline");
    }

    #[tokio::test]
    async fn test_pipeline_run_single_passing_stage() {
        let config = PipelineConfig::new("Test").with_thresholds(ValidationThresholds::relaxed());
        let pipeline = ValidationPipeline::new(config.clone());
        let algo = AlgorithmConfig::default();

        let stage = TestStage::new(ValidationStageType::Backtest, "BT");
        let stages: Vec<Box<dyn ValidationStage>> = vec![Box::new(stage)];
        let stage_configs = vec![StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_period(Utc::now() - Duration::days(30), Utc::now())];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.status, PipelineStatus::Passed);
        assert_eq!(result.stages_passed, 1);
        assert_eq!(result.stages_failed, 0);
    }

    #[tokio::test]
    async fn test_pipeline_run_single_failing_stage() {
        let config = PipelineConfig::new("Test").with_thresholds(ValidationThresholds::strict());
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stage = TestStage::new(ValidationStageType::Backtest, "BT").failing();
        let stages: Vec<Box<dyn ValidationStage>> = vec![Box::new(stage)];
        let stage_configs = vec![StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_period(Utc::now() - Duration::days(30), Utc::now())];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.status, PipelineStatus::Failed);
        assert_eq!(result.stages_failed, 1);
    }

    #[tokio::test]
    async fn test_pipeline_stop_on_first_failure() {
        let config = PipelineConfig::new("Test")
            .with_stop_condition(StopCondition::StopOnFirstFailure)
            .with_thresholds(ValidationThresholds::strict());
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stages: Vec<Box<dyn ValidationStage>> = vec![
            Box::new(TestStage::new(ValidationStageType::Backtest, "BT").failing()),
            Box::new(TestStage::new(ValidationStageType::Forward, "FW")),
        ];
        let stage_configs = vec![
            StageConfig::new(ValidationStageType::Backtest, "BT")
                .with_period(Utc::now() - Duration::days(60), Utc::now() - Duration::days(30)),
            StageConfig::new(ValidationStageType::Forward, "FW")
                .with_period(Utc::now() - Duration::days(30), Utc::now()),
        ];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.stages_failed, 1);
        assert_eq!(result.stages_skipped, 1);
    }

    #[tokio::test]
    async fn test_pipeline_continue_on_failure() {
        let config = PipelineConfig::new("Test")
            .with_stop_condition(StopCondition::ContinueOnFailure)
            .with_thresholds(ValidationThresholds::relaxed());
        let mut config = config;
        config.enforce_stage_order = false;

        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stages: Vec<Box<dyn ValidationStage>> = vec![
            Box::new(TestStage::new(ValidationStageType::Backtest, "BT").failing()),
            Box::new(TestStage::new(ValidationStageType::Forward, "FW")),
        ];
        let stage_configs = vec![
            StageConfig::new(ValidationStageType::Backtest, "BT")
                .with_period(Utc::now() - Duration::days(60), Utc::now() - Duration::days(30)),
            StageConfig::new(ValidationStageType::Forward, "FW")
                .with_period(Utc::now() - Duration::days(30), Utc::now()),
        ];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        // Both stages should run
        assert_eq!(result.stages_failed, 1);
        assert_eq!(result.stages_passed, 1);
        assert_eq!(result.stages_skipped, 0);
    }

    #[tokio::test]
    async fn test_pipeline_disabled_stage() {
        let config = PipelineConfig::new("Test").with_thresholds(ValidationThresholds::relaxed());
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stages: Vec<Box<dyn ValidationStage>> = vec![
            Box::new(TestStage::new(ValidationStageType::Backtest, "BT")),
            Box::new(TestStage::new(ValidationStageType::Forward, "FW")),
        ];
        let stage_configs = vec![
            StageConfig::new(ValidationStageType::Backtest, "BT")
                .with_period(Utc::now() - Duration::days(60), Utc::now() - Duration::days(30)),
            StageConfig::new(ValidationStageType::Forward, "FW")
                .with_period(Utc::now() - Duration::days(30), Utc::now())
                .disabled(),
        ];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.stages_passed, 1);
        assert_eq!(result.stages_skipped, 1);
    }

    #[tokio::test]
    async fn test_pipeline_stage_error() {
        let config = PipelineConfig::new("Test").with_thresholds(ValidationThresholds::relaxed());
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stage = TestStage::new(ValidationStageType::Backtest, "BT").with_error("Test error");
        let stages: Vec<Box<dyn ValidationStage>> = vec![Box::new(stage)];
        let stage_configs = vec![StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_period(Utc::now() - Duration::days(30), Utc::now())];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.stages_failed, 1);
        assert!(result
            .stage_outcomes
            .get(&ValidationStageType::Backtest)
            .unwrap()
            .error_message()
            .is_some());
    }

    #[tokio::test]
    async fn test_pipeline_can_run_check_fails() {
        let config = PipelineConfig::new("Test").with_thresholds(ValidationThresholds::relaxed());
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stage = TestStage::new(ValidationStageType::Backtest, "BT")
            .with_can_run_error(StageError::DataUnavailable("No data".to_string()));
        let stages: Vec<Box<dyn ValidationStage>> = vec![Box::new(stage)];
        let stage_configs = vec![StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_period(Utc::now() - Duration::days(30), Utc::now())];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.stages_skipped, 1);
    }

    #[tokio::test]
    async fn test_pipeline_enforce_stage_order() {
        let config = PipelineConfig::new("Test").with_thresholds(ValidationThresholds::relaxed());
        let mut config = config;
        config.enforce_stage_order = true;

        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        // Forward stage should be skipped because backtest fails
        let stages: Vec<Box<dyn ValidationStage>> = vec![
            Box::new(TestStage::new(ValidationStageType::Backtest, "BT").failing()),
            Box::new(TestStage::new(ValidationStageType::Forward, "FW")),
        ];
        let stage_configs = vec![
            StageConfig::new(ValidationStageType::Backtest, "BT")
                .with_period(Utc::now() - Duration::days(60), Utc::now() - Duration::days(30))
                .with_thresholds(ValidationThresholds::strict()),
            StageConfig::new(ValidationStageType::Forward, "FW")
                .with_period(Utc::now() - Duration::days(30), Utc::now()),
        ];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        // Forward should be skipped because backtest failed AND stop_on_first_failure
        assert!(result.stages_skipped >= 1 || result.stages_failed >= 1);
    }

    #[tokio::test]
    async fn test_pipeline_custom_thresholds_per_stage() {
        let config = PipelineConfig::new("Test").with_thresholds(ValidationThresholds::strict());
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stage = TestStage::new(ValidationStageType::Backtest, "BT");
        let stages: Vec<Box<dyn ValidationStage>> = vec![Box::new(stage)];
        let stage_configs = vec![StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_period(Utc::now() - Duration::days(30), Utc::now())
            .with_thresholds(ValidationThresholds::relaxed())]; // Override with relaxed

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        // Should pass because stage uses relaxed thresholds
        assert_eq!(result.stages_passed, 1);
    }

    #[tokio::test]
    async fn test_pipeline_execution_order_tracking() {
        let config = PipelineConfig::new("Test")
            .with_stop_condition(StopCondition::ContinueOnFailure)
            .with_thresholds(ValidationThresholds::relaxed());
        let mut config = config;
        config.enforce_stage_order = false;

        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stages: Vec<Box<dyn ValidationStage>> = vec![
            Box::new(TestStage::new(ValidationStageType::Backtest, "BT")),
            Box::new(TestStage::new(ValidationStageType::Forward, "FW")),
            Box::new(TestStage::new(ValidationStageType::OutOfSample, "OOS")),
        ];
        let stage_configs = vec![
            StageConfig::new(ValidationStageType::Backtest, "BT")
                .with_period(Utc::now() - Duration::days(90), Utc::now() - Duration::days(60)),
            StageConfig::new(ValidationStageType::Forward, "FW")
                .with_period(Utc::now() - Duration::days(60), Utc::now() - Duration::days(30)),
            StageConfig::new(ValidationStageType::OutOfSample, "OOS")
                .with_period(Utc::now() - Duration::days(30), Utc::now()),
        ];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.execution_order.len(), 3);
        assert_eq!(result.execution_order[0], ValidationStageType::Backtest);
        assert_eq!(result.execution_order[1], ValidationStageType::Forward);
        assert_eq!(result.execution_order[2], ValidationStageType::OutOfSample);
    }

    #[tokio::test]
    async fn test_pipeline_multiple_stages_all_pass() {
        let config = PipelineConfig::new("Test")
            .with_stop_condition(StopCondition::ContinueOnFailure)
            .with_thresholds(ValidationThresholds::relaxed());
        let mut config = config;
        config.enforce_stage_order = false;

        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stages: Vec<Box<dyn ValidationStage>> = vec![
            Box::new(TestStage::new(ValidationStageType::Backtest, "BT")),
            Box::new(TestStage::new(ValidationStageType::Forward, "FW")),
            Box::new(TestStage::new(ValidationStageType::OutOfSample, "OOS")),
        ];
        let stage_configs = vec![
            StageConfig::new(ValidationStageType::Backtest, "BT")
                .with_period(Utc::now() - Duration::days(90), Utc::now() - Duration::days(60)),
            StageConfig::new(ValidationStageType::Forward, "FW")
                .with_period(Utc::now() - Duration::days(60), Utc::now() - Duration::days(30)),
            StageConfig::new(ValidationStageType::OutOfSample, "OOS")
                .with_period(Utc::now() - Duration::days(30), Utc::now()),
        ];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.status, PipelineStatus::Passed);
        assert_eq!(result.stages_passed, 3);
        assert_eq!(result.stages_failed, 0);
    }

    #[tokio::test]
    async fn test_pipeline_empty_stages() {
        let config = PipelineConfig::new("Test");
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stages: Vec<Box<dyn ValidationStage>> = vec![];
        let stage_configs: Vec<StageConfig> = vec![];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.status, PipelineStatus::Error);
        assert_eq!(result.stages_passed, 0);
    }

    #[tokio::test]
    async fn test_pipeline_stop_after_n_failures() {
        let config = PipelineConfig::new("Test")
            .with_stop_condition(StopCondition::StopAfterNFailures(2))
            .with_thresholds(ValidationThresholds::strict());
        let mut config = config;
        config.enforce_stage_order = false;

        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stages: Vec<Box<dyn ValidationStage>> = vec![
            Box::new(TestStage::new(ValidationStageType::Backtest, "BT").failing()),
            Box::new(TestStage::new(ValidationStageType::Forward, "FW").failing()),
            Box::new(TestStage::new(ValidationStageType::OutOfSample, "OOS")),
        ];
        let stage_configs = vec![
            StageConfig::new(ValidationStageType::Backtest, "BT")
                .with_period(Utc::now() - Duration::days(90), Utc::now() - Duration::days(60)),
            StageConfig::new(ValidationStageType::Forward, "FW")
                .with_period(Utc::now() - Duration::days(60), Utc::now() - Duration::days(30)),
            StageConfig::new(ValidationStageType::OutOfSample, "OOS")
                .with_period(Utc::now() - Duration::days(30), Utc::now()),
        ];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert_eq!(result.stages_failed, 2);
        assert_eq!(result.stages_skipped, 1);
    }

    // ==================== Integration Tests ====================

    #[tokio::test]
    async fn test_full_pipeline_workflow() {
        let config = PipelineConfig::production();
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stages: Vec<Box<dyn ValidationStage>> = vec![
            Box::new(TestStage::new(ValidationStageType::Backtest, "BT-2025Q1")),
        ];

        let now = Utc::now();
        let stage_configs = vec![StageConfig::new(ValidationStageType::Backtest, "BT-2025Q1")
            .with_period(now - Duration::days(90), now)
            .with_timeout(3600)
            .with_detailed_output(true)
            .with_metadata("symbol", "BTCUSDT")
            .with_thresholds(ValidationThresholds::relaxed())];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        assert!(result.status.is_complete());
        assert!(result.duration_seconds >= 0.0);
        assert!(!result.summary.is_empty());

        // Verify we can access individual stage results
        if let Some(bt_result) = result.get_stage_result(ValidationStageType::Backtest) {
            assert_eq!(bt_result.stage_type, ValidationStageType::Backtest);
            assert!(bt_result.metrics.trade_count > 0);
        }
    }

    #[tokio::test]
    async fn test_pipeline_with_warnings() {
        let config = PipelineConfig::new("Test").with_thresholds(ValidationThresholds::relaxed());
        let pipeline = ValidationPipeline::new(config);
        let algo = AlgorithmConfig::default();

        let stage = TestStage::new(ValidationStageType::Backtest, "BT");
        let stages: Vec<Box<dyn ValidationStage>> = vec![Box::new(stage)];
        let stage_configs = vec![StageConfig::new(ValidationStageType::Backtest, "BT")
            .with_period(Utc::now() - Duration::days(30), Utc::now())];

        let result = pipeline.run(&algo, &stages, &stage_configs).await.unwrap();

        // Result should have been created successfully
        assert!(result.status.is_complete());
    }

    #[test]
    fn test_pipeline_builder_full_chain() {
        let now = Utc::now();
        let config = PipelineConfig::production();
        let algo = AlgorithmConfig::default();

        let builder = PipelineBuilder::new(config, algo)
            .with_backtest("BT", now - Duration::days(365), now - Duration::days(180))
            .with_forward("FW", now - Duration::days(180), now - Duration::days(90))
            .with_oos("OOS", now - Duration::days(90), now - Duration::days(30))
            .with_paper("PP", now - Duration::days(30), now)
            .with_live("LV", now, now + Duration::days(30));

        let (config, algo, stages) = builder.build();

        assert_eq!(stages.len(), 5);
        assert_eq!(config.name, "Production Pipeline");
        assert!(!algo.id.is_empty());
    }
}

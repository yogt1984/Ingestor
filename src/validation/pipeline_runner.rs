//! PipelineRunner Implementation (Task 2.6)
//!
//! Orchestrates all validation stages with stop conditions, stage-by-stage
//! result persistence, and support for partial runs.
//!
//! # Overview
//!
//! The `PipelineRunner` is a high-level orchestrator that:
//! 1. Creates and registers validation stages (Backtest, Forward, OOS, Paper, Live)
//! 2. Runs stages in sequence with configurable stop conditions
//! 3. Persists results at each stage to `ResultsStore`
//! 4. Supports partial runs (start from a specific stage)
//! 5. Produces a final `PipelineResult`
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::validation::{PipelineRunner, RunnerConfig};
//! use ingestor::core::{AlgorithmConfig, ResultsStore};
//!
//! let config = RunnerConfig::default();
//! let results_store = ResultsStore::new("/path/to/results");
//! let runner = PipelineRunner::new(config, results_store);
//!
//! // Run all stages
//! let result = runner.run_all(&algorithm_config).await?;
//!
//! // Or run from a specific stage
//! let result = runner.run_from(ValidationStageType::Forward, &algorithm_config).await?;
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::core::{
    AlgorithmConfig, ResultsStore, ResultsStoreConfig, ValidationResult, ValidationStageType,
    ValidationThresholds,
};

use super::backtest_stage::{BacktestStage, BacktestStageConfig};
use super::forward_stage::{ForwardStage, ForwardStageConfig};
use super::live_stage::{LiveStage, LiveStageConfig};
use super::oos_stage::{OOSStage, OOSStageConfig};
use super::paper_stage::{PaperStage, PaperStageConfig};
use super::pipeline::{
    PipelineConfig, PipelineResult, PipelineStatus, StageConfig, StageOutcome, StopCondition,
};
use super::traits::{StageContext, StageError, ValidationStage};

/// Configuration for the pipeline runner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerConfig {
    /// Pipeline configuration
    pub pipeline_config: PipelineConfig,

    /// Configuration for each stage type
    pub stage_configs: HashMap<ValidationStageType, StageTypeConfig>,

    /// Whether to persist results after each stage
    pub persist_results: bool,

    /// Whether to load previous results for partial runs
    pub load_previous_results: bool,

    /// Data path for historical stages
    pub data_path: String,

    /// Results path for persistence
    pub results_path: Option<String>,

    /// Custom name prefix for this run
    pub run_name_prefix: String,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        let mut stage_configs = HashMap::new();
        stage_configs.insert(
            ValidationStageType::Backtest,
            StageTypeConfig::default_for(ValidationStageType::Backtest),
        );
        stage_configs.insert(
            ValidationStageType::Forward,
            StageTypeConfig::default_for(ValidationStageType::Forward),
        );
        stage_configs.insert(
            ValidationStageType::OutOfSample,
            StageTypeConfig::default_for(ValidationStageType::OutOfSample),
        );
        stage_configs.insert(
            ValidationStageType::Paper,
            StageTypeConfig::default_for(ValidationStageType::Paper),
        );
        stage_configs.insert(
            ValidationStageType::Live,
            StageTypeConfig::default_for(ValidationStageType::Live),
        );

        Self {
            pipeline_config: PipelineConfig::default(),
            stage_configs,
            persist_results: true,
            load_previous_results: true,
            data_path: "./data/features".to_string(),
            results_path: None,
            run_name_prefix: "validation".to_string(),
        }
    }
}

impl RunnerConfig {
    /// Create a new runner configuration
    pub fn new(data_path: impl Into<String>) -> Self {
        Self {
            data_path: data_path.into(),
            ..Default::default()
        }
    }

    /// Set pipeline configuration
    pub fn with_pipeline_config(mut self, config: PipelineConfig) -> Self {
        self.pipeline_config = config;
        self
    }

    /// Set whether to persist results
    pub fn with_persistence(mut self, persist: bool) -> Self {
        self.persist_results = persist;
        self
    }

    /// Set results path
    pub fn with_results_path(mut self, path: impl Into<String>) -> Self {
        self.results_path = Some(path.into());
        self
    }

    /// Set run name prefix
    pub fn with_run_name(mut self, name: impl Into<String>) -> Self {
        self.run_name_prefix = name.into();
        self
    }

    /// Configure a specific stage type
    pub fn with_stage_config(
        mut self,
        stage_type: ValidationStageType,
        config: StageTypeConfig,
    ) -> Self {
        self.stage_configs.insert(stage_type, config);
        self
    }

    /// Disable a specific stage
    pub fn without_stage(mut self, stage_type: ValidationStageType) -> Self {
        if let Some(config) = self.stage_configs.get_mut(&stage_type) {
            config.enabled = false;
        }
        self
    }

    /// Create a production configuration
    pub fn production(data_path: impl Into<String>) -> Self {
        Self {
            pipeline_config: PipelineConfig::production(),
            data_path: data_path.into(),
            persist_results: true,
            load_previous_results: true,
            run_name_prefix: "prod".to_string(),
            ..Default::default()
        }
    }

    /// Create a research/testing configuration
    pub fn research(data_path: impl Into<String>) -> Self {
        Self {
            pipeline_config: PipelineConfig::research(),
            data_path: data_path.into(),
            persist_results: true,
            load_previous_results: false,
            run_name_prefix: "research".to_string(),
            ..Default::default()
        }
    }

    /// Create a fast validation configuration (backtest only)
    pub fn fast(data_path: impl Into<String>) -> Self {
        let mut config = Self::new(data_path);
        config.run_name_prefix = "fast".to_string();
        config
            .without_stage(ValidationStageType::Forward)
            .without_stage(ValidationStageType::OutOfSample)
            .without_stage(ValidationStageType::Paper)
            .without_stage(ValidationStageType::Live)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), RunnerError> {
        if self.data_path.is_empty() {
            return Err(RunnerError::Configuration(
                "Data path cannot be empty".to_string(),
            ));
        }

        // Check that at least one stage is enabled
        let enabled_count = self
            .stage_configs
            .values()
            .filter(|c| c.enabled)
            .count();

        if enabled_count == 0 {
            return Err(RunnerError::Configuration(
                "At least one stage must be enabled".to_string(),
            ));
        }

        Ok(())
    }
}

/// Configuration for a specific stage type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTypeConfig {
    /// Whether this stage is enabled
    pub enabled: bool,

    /// Custom thresholds (overrides pipeline defaults)
    pub thresholds: Option<ValidationThresholds>,

    /// Timeout in seconds
    pub timeout_seconds: Option<u64>,

    /// Period duration in days (for historical stages)
    pub period_days: Option<u64>,

    /// Duration in seconds (for live stages)
    pub duration_seconds: Option<u64>,

    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

impl Default for StageTypeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            thresholds: None,
            timeout_seconds: None,
            period_days: None,
            duration_seconds: None,
            metadata: HashMap::new(),
        }
    }
}

impl StageTypeConfig {
    /// Create default config for a specific stage type
    pub fn default_for(stage_type: ValidationStageType) -> Self {
        match stage_type {
            ValidationStageType::Backtest => Self {
                enabled: true,
                period_days: Some(180),
                timeout_seconds: Some(3600),
                ..Default::default()
            },
            ValidationStageType::Forward => Self {
                enabled: true,
                period_days: Some(90),
                timeout_seconds: Some(3600),
                ..Default::default()
            },
            ValidationStageType::OutOfSample => Self {
                enabled: true,
                period_days: Some(30),
                timeout_seconds: Some(1800),
                ..Default::default()
            },
            ValidationStageType::Paper => Self {
                enabled: true,
                duration_seconds: Some(86400), // 24 hours
                timeout_seconds: Some(90000),
                ..Default::default()
            },
            ValidationStageType::Live => Self {
                enabled: true,
                duration_seconds: Some(86400), // 24 hours
                timeout_seconds: Some(90000),
                ..Default::default()
            },
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

    /// Set period duration
    pub fn with_period_days(mut self, days: u64) -> Self {
        self.period_days = Some(days);
        self
    }

    /// Set duration
    pub fn with_duration(mut self, seconds: u64) -> Self {
        self.duration_seconds = Some(seconds);
        self
    }
}

/// Errors that can occur during pipeline execution
#[derive(Debug, Clone)]
pub enum RunnerError {
    /// Configuration error
    Configuration(String),

    /// Stage creation error
    StageCreation(String),

    /// Stage execution error
    StageExecution(ValidationStageType, String),

    /// Persistence error
    Persistence(String),

    /// Invalid stage order
    InvalidStageOrder(String),

    /// Previous results not found
    PreviousResultsNotFound(ValidationStageType),

    /// Pipeline cancelled
    Cancelled(String),
}

impl std::fmt::Display for RunnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunnerError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            RunnerError::StageCreation(msg) => write!(f, "Stage creation error: {}", msg),
            RunnerError::StageExecution(stage, msg) => {
                write!(f, "Stage {:?} execution error: {}", stage, msg)
            }
            RunnerError::Persistence(msg) => write!(f, "Persistence error: {}", msg),
            RunnerError::InvalidStageOrder(msg) => write!(f, "Invalid stage order: {}", msg),
            RunnerError::PreviousResultsNotFound(stage) => {
                write!(f, "Previous results not found for stage {:?}", stage)
            }
            RunnerError::Cancelled(msg) => write!(f, "Pipeline cancelled: {}", msg),
        }
    }
}

impl std::error::Error for RunnerError {}

impl From<StageError> for RunnerError {
    fn from(err: StageError) -> Self {
        RunnerError::StageExecution(ValidationStageType::Backtest, err.to_string())
    }
}

/// Result of a stage execution with persistence info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageExecutionResult {
    /// Stage type that was executed
    pub stage_type: ValidationStageType,

    /// The stage outcome
    pub outcome: StageOutcome,

    /// Path where result was persisted (if any)
    pub persisted_path: Option<PathBuf>,

    /// Execution duration in seconds
    pub duration_seconds: f64,

    /// Timestamp of execution
    pub executed_at: DateTime<Utc>,
}

/// The main pipeline runner that orchestrates validation stages
pub struct PipelineRunner {
    /// Runner configuration
    config: RunnerConfig,

    /// Results store for persistence
    results_store: Option<ResultsStore>,

    /// Execution history
    execution_history: Vec<StageExecutionResult>,
}

impl PipelineRunner {
    /// Create a new pipeline runner
    pub fn new(config: RunnerConfig) -> Self {
        let results_store = config.results_path.as_ref().and_then(|path| {
            ResultsStore::new(ResultsStoreConfig::with_path(path))
                .or_else(|_| ResultsStore::new(ResultsStoreConfig::with_path(".")))
                .ok()
        });

        Self {
            config,
            results_store,
            execution_history: Vec::new(),
        }
    }

    /// Create a runner with an existing results store
    pub fn with_results_store(config: RunnerConfig, results_store: ResultsStore) -> Self {
        Self {
            config,
            results_store: Some(results_store),
            execution_history: Vec::new(),
        }
    }

    /// Get the runner configuration
    pub fn config(&self) -> &RunnerConfig {
        &self.config
    }

    /// Get the execution history
    pub fn execution_history(&self) -> &[StageExecutionResult] {
        &self.execution_history
    }

    /// Clear the execution history
    pub fn clear_history(&mut self) {
        self.execution_history.clear();
    }

    /// Run all validation stages
    pub async fn run_all(
        &mut self,
        algorithm_config: &AlgorithmConfig,
    ) -> Result<PipelineResult, RunnerError> {
        self.config.validate()?;
        self.run_from_internal(None, algorithm_config).await
    }

    /// Run validation starting from a specific stage
    pub async fn run_from(
        &mut self,
        start_stage: ValidationStageType,
        algorithm_config: &AlgorithmConfig,
    ) -> Result<PipelineResult, RunnerError> {
        self.config.validate()?;
        self.run_from_internal(Some(start_stage), algorithm_config)
            .await
    }

    /// Internal method to run the pipeline
    async fn run_from_internal(
        &mut self,
        start_stage: Option<ValidationStageType>,
        algorithm_config: &AlgorithmConfig,
    ) -> Result<PipelineResult, RunnerError> {
        // Determine which stages to run
        let stage_order = Self::get_stage_order();
        let start_index = if let Some(start) = start_stage {
            stage_order
                .iter()
                .position(|s| *s == start)
                .ok_or_else(|| {
                    RunnerError::InvalidStageOrder(format!("Invalid start stage: {:?}", start))
                })?
        } else {
            0
        };

        // Load previous results if needed
        let mut previous_results: Vec<ValidationResult> = Vec::new();
        if self.config.load_previous_results && start_index > 0 {
            previous_results = self.load_previous_results(algorithm_config, start_index)?;
        }

        // Create stages
        let (stages, stage_configs) = self.create_stages(&stage_order[start_index..])?;

        // Build contexts with previous results
        let mut result =
            PipelineResult::new(self.config.pipeline_config.id.clone(), algorithm_config.id.clone());
        result.start();

        // Run stages manually to support persistence after each stage
        let mut current_previous_results = previous_results;
        let _start_time = Instant::now();

        for (_i, (stage, stage_config)) in stages.iter().zip(stage_configs.iter()).enumerate() {
            if !stage_config.enabled {
                let exec_result = StageExecutionResult {
                    stage_type: stage.stage_type(),
                    outcome: StageOutcome::Skipped("Stage disabled".to_string()),
                    persisted_path: None,
                    duration_seconds: 0.0,
                    executed_at: Utc::now(),
                };
                self.execution_history.push(exec_result);
                result.add_outcome(
                    stage.stage_type(),
                    StageOutcome::Skipped("Stage disabled".to_string()),
                );
                continue;
            }

            // Check stop conditions
            let should_stop = self.check_stop_condition(&result);
            if should_stop {
                let exec_result = StageExecutionResult {
                    stage_type: stage.stage_type(),
                    outcome: StageOutcome::Skipped("Stop condition met".to_string()),
                    persisted_path: None,
                    duration_seconds: 0.0,
                    executed_at: Utc::now(),
                };
                self.execution_history.push(exec_result);
                result.add_outcome(
                    stage.stage_type(),
                    StageOutcome::Skipped("Stop condition met".to_string()),
                );
                continue;
            }

            // Check if required previous stage passed
            if self.config.pipeline_config.enforce_stage_order {
                if let Some(required) = stage.requires_previous() {
                    let prev_passed = current_previous_results
                        .iter()
                        .any(|r| r.stage_type == required && r.passed);

                    if !prev_passed {
                        let exec_result = StageExecutionResult {
                            stage_type: stage.stage_type(),
                            outcome: StageOutcome::Skipped(format!(
                                "Required stage {:?} did not pass",
                                required
                            )),
                            persisted_path: None,
                            duration_seconds: 0.0,
                            executed_at: Utc::now(),
                        };
                        self.execution_history.push(exec_result);
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

            // Build context
            let context = self.build_context(
                algorithm_config,
                stage_config,
                &current_previous_results,
            );

            // Check if stage can run
            if let Err(e) = stage.can_run(&context) {
                let exec_result = StageExecutionResult {
                    stage_type: stage.stage_type(),
                    outcome: StageOutcome::Error(e.to_string()),
                    persisted_path: None,
                    duration_seconds: 0.0,
                    executed_at: Utc::now(),
                };
                self.execution_history.push(exec_result);

                if e.should_halt_pipeline() {
                    result.add_outcome(stage.stage_type(), StageOutcome::Error(e.to_string()));
                    result.complete(PipelineStatus::Error);
                    return Err(RunnerError::StageExecution(stage.stage_type(), e.to_string()));
                } else {
                    result.add_outcome(
                        stage.stage_type(),
                        StageOutcome::Skipped(format!("Cannot run: {}", e)),
                    );
                    continue;
                }
            }

            // Run the stage
            let stage_start = Instant::now();
            let stage_result = stage.run(&context).await;
            let stage_duration = stage_start.elapsed().as_secs_f64();

            // Process result
            match stage_result {
                Ok(validation_result) => {
                    let passed = validation_result.passed;

                    // Persist result if configured
                    let persisted_path = if self.config.persist_results {
                        self.persist_result(&validation_result).ok()
                    } else {
                        None
                    };

                    // Add to previous results for next stage
                    current_previous_results.push(validation_result.clone());

                    let outcome = if passed {
                        StageOutcome::Passed(validation_result)
                    } else {
                        StageOutcome::Failed(validation_result)
                    };

                    let exec_result = StageExecutionResult {
                        stage_type: stage.stage_type(),
                        outcome: outcome.clone(),
                        persisted_path,
                        duration_seconds: stage_duration,
                        executed_at: Utc::now(),
                    };
                    self.execution_history.push(exec_result);
                    result.add_outcome(stage.stage_type(), outcome);
                }
                Err(e) => {
                    let exec_result = StageExecutionResult {
                        stage_type: stage.stage_type(),
                        outcome: StageOutcome::Error(e.to_string()),
                        persisted_path: None,
                        duration_seconds: stage_duration,
                        executed_at: Utc::now(),
                    };
                    self.execution_history.push(exec_result);
                    result.add_outcome(stage.stage_type(), StageOutcome::Error(e.to_string()));

                    if e.should_halt_pipeline() {
                        result.complete(PipelineStatus::Error);
                        return Err(RunnerError::StageExecution(stage.stage_type(), e.to_string()));
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

    /// Get the standard stage order
    fn get_stage_order() -> Vec<ValidationStageType> {
        vec![
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
            ValidationStageType::Paper,
            ValidationStageType::Live,
        ]
    }

    /// Check if the pipeline should stop based on current results
    fn check_stop_condition(&self, result: &PipelineResult) -> bool {
        match self.config.pipeline_config.stop_condition {
            StopCondition::StopOnFirstFailure => result.stages_failed > 0,
            StopCondition::ContinueOnFailure => false,
            StopCondition::StopAfterNFailures(n) => result.stages_failed >= n,
            StopCondition::StopAfterStage(stage) => result
                .stage_outcomes
                .contains_key(&stage),
        }
    }

    /// Load previous results for partial runs
    fn load_previous_results(
        &mut self,
        algorithm_config: &AlgorithmConfig,
        start_index: usize,
    ) -> Result<Vec<ValidationResult>, RunnerError> {
        let mut results = Vec::new();
        let stage_order = Self::get_stage_order();

        if let Some(ref mut store) = self.results_store {
            for i in 0..start_index {
                let stage_type = stage_order[i];

                // Try to load from store
                if let Ok(stage_results) = store.load_by_config(&algorithm_config.id) {
                    if let Some(result) = stage_results
                        .into_iter()
                        .find(|r| r.stage_type == stage_type)
                    {
                        results.push(result);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Create validation stages based on configuration
    fn create_stages(
        &self,
        stage_types: &[ValidationStageType],
    ) -> Result<(Vec<Box<dyn ValidationStage>>, Vec<StageConfig>), RunnerError> {
        let mut stages: Vec<Box<dyn ValidationStage>> = Vec::new();
        let mut configs: Vec<StageConfig> = Vec::new();
        let now = Utc::now();

        for stage_type in stage_types {
            let type_config = self
                .config
                .stage_configs
                .get(stage_type)
                .cloned()
                .unwrap_or_else(|| StageTypeConfig::default_for(*stage_type));

            let (stage, config) = self.create_stage(*stage_type, &type_config, now)?;
            stages.push(stage);
            configs.push(config);
        }

        Ok((stages, configs))
    }

    /// Create a single stage with its configuration
    fn create_stage(
        &self,
        stage_type: ValidationStageType,
        type_config: &StageTypeConfig,
        now: DateTime<Utc>,
    ) -> Result<(Box<dyn ValidationStage>, StageConfig), RunnerError> {
        let stage_name = format!(
            "{}-{}-{}",
            self.config.run_name_prefix,
            stage_type_name(stage_type),
            now.format("%Y%m%d")
        );

        let (period_start, period_end) = self.calculate_period(stage_type, type_config, now);

        let mut stage_config = StageConfig::new(stage_type, &stage_name)
            .with_period(period_start, period_end)
            .with_data_path(&self.config.data_path)
            .with_detailed_output(true);

        if !type_config.enabled {
            stage_config = stage_config.disabled();
        }

        if let Some(timeout) = type_config.timeout_seconds {
            stage_config = stage_config.with_timeout(timeout);
        }

        if let Some(ref thresholds) = type_config.thresholds {
            stage_config = stage_config.with_thresholds(thresholds.clone());
        }

        let stage: Box<dyn ValidationStage> = match stage_type {
            ValidationStageType::Backtest => {
                let bt_config = BacktestStageConfig::default();
                Box::new(BacktestStage::new(bt_config))
            }
            ValidationStageType::Forward => {
                let fw_config = ForwardStageConfig::default();
                Box::new(ForwardStage::new(fw_config))
            }
            ValidationStageType::OutOfSample => {
                let oos_config = OOSStageConfig::default();
                Box::new(OOSStage::new(oos_config))
            }
            ValidationStageType::Paper => {
                let duration = type_config.duration_seconds.unwrap_or(86400);
                let pp_config = PaperStageConfig::default().with_duration(duration);
                Box::new(PaperStage::new(pp_config))
            }
            ValidationStageType::Live => {
                let duration = type_config.duration_seconds.unwrap_or(86400);
                let lv_config = LiveStageConfig::default().with_duration(duration);
                Box::new(LiveStage::new(lv_config))
            }
        };

        Ok((stage, stage_config))
    }

    /// Calculate period for a stage
    fn calculate_period(
        &self,
        stage_type: ValidationStageType,
        type_config: &StageTypeConfig,
        now: DateTime<Utc>,
    ) -> (DateTime<Utc>, DateTime<Utc>) {
        match stage_type {
            ValidationStageType::Backtest => {
                let days = type_config.period_days.unwrap_or(180) as i64;
                (now - Duration::days(days), now - Duration::days(days / 2))
            }
            ValidationStageType::Forward => {
                let days = type_config.period_days.unwrap_or(90) as i64;
                (now - Duration::days(days), now - Duration::days(days / 3))
            }
            ValidationStageType::OutOfSample => {
                let days = type_config.period_days.unwrap_or(30) as i64;
                (now - Duration::days(days), now)
            }
            ValidationStageType::Paper => {
                let duration = type_config.duration_seconds.unwrap_or(86400) as i64;
                (now, now + Duration::seconds(duration))
            }
            ValidationStageType::Live => {
                let duration = type_config.duration_seconds.unwrap_or(86400) as i64;
                (now, now + Duration::seconds(duration))
            }
        }
    }

    /// Build stage context
    fn build_context(
        &self,
        algorithm_config: &AlgorithmConfig,
        stage_config: &StageConfig,
        previous_results: &[ValidationResult],
    ) -> StageContext {
        let thresholds = stage_config
            .thresholds
            .clone()
            .unwrap_or_else(|| self.config.pipeline_config.default_thresholds.clone());

        let period_start = stage_config
            .period_start
            .unwrap_or_else(|| Utc::now() - Duration::days(30));
        let period_end = stage_config.period_end.unwrap_or_else(Utc::now);

        let mut context = StageContext::new(
            algorithm_config.clone(),
            thresholds,
            period_start,
            period_end,
        )
        .with_name(&stage_config.name)
        .with_previous_results(previous_results.to_vec())
        .with_detailed_output(stage_config.detailed_output);

        if let Some(timeout) = stage_config.timeout_seconds {
            context = context.with_timeout(timeout);
        }

        if let Some(ref path) = stage_config.data_path {
            context = context.with_data_path(path.clone());
        }

        context
    }

    /// Persist a validation result
    fn persist_result(&mut self, result: &ValidationResult) -> Result<PathBuf, RunnerError> {
        if let Some(ref mut store) = self.results_store {
            store
                .save(result)
                .map_err(|e| RunnerError::Persistence(e.to_string()))
        } else {
            Err(RunnerError::Persistence(
                "No results store configured".to_string(),
            ))
        }
    }

    /// Get stages that are enabled
    pub fn enabled_stages(&self) -> Vec<ValidationStageType> {
        Self::get_stage_order()
            .into_iter()
            .filter(|s| {
                self.config
                    .stage_configs
                    .get(s)
                    .map(|c| c.enabled)
                    .unwrap_or(true)
            })
            .collect()
    }

    /// Check if a stage is enabled
    pub fn is_stage_enabled(&self, stage_type: ValidationStageType) -> bool {
        self.config
            .stage_configs
            .get(&stage_type)
            .map(|c| c.enabled)
            .unwrap_or(true)
    }
}

/// Factory for creating pipeline runners
pub struct PipelineRunnerFactory;

impl PipelineRunnerFactory {
    /// Create a new runner with default configuration
    pub fn create(data_path: impl Into<String>) -> PipelineRunner {
        PipelineRunner::new(RunnerConfig::new(data_path))
    }

    /// Create a production runner
    pub fn create_production(data_path: impl Into<String>) -> PipelineRunner {
        PipelineRunner::new(RunnerConfig::production(data_path))
    }

    /// Create a research runner
    pub fn create_research(data_path: impl Into<String>) -> PipelineRunner {
        PipelineRunner::new(RunnerConfig::research(data_path))
    }

    /// Create a fast validation runner (backtest only)
    pub fn create_fast(data_path: impl Into<String>) -> PipelineRunner {
        PipelineRunner::new(RunnerConfig::fast(data_path))
    }
}

/// Helper function to get stage type name
fn stage_type_name(stage_type: ValidationStageType) -> &'static str {
    match stage_type {
        ValidationStageType::Backtest => "backtest",
        ValidationStageType::Forward => "forward",
        ValidationStageType::OutOfSample => "oos",
        ValidationStageType::Paper => "paper",
        ValidationStageType::Live => "live",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== RunnerConfig Tests ====================

    #[test]
    fn test_runner_config_default() {
        let config = RunnerConfig::default();
        assert!(config.persist_results);
        assert!(config.load_previous_results);
        assert_eq!(config.data_path, "./data/features");
        assert_eq!(config.stage_configs.len(), 5);
    }

    #[test]
    fn test_runner_config_new() {
        let config = RunnerConfig::new("/custom/path");
        assert_eq!(config.data_path, "/custom/path");
    }

    #[test]
    fn test_runner_config_with_pipeline_config() {
        let pipeline_config = PipelineConfig::production();
        let config = RunnerConfig::default().with_pipeline_config(pipeline_config.clone());
        assert_eq!(config.pipeline_config.name, pipeline_config.name);
    }

    #[test]
    fn test_runner_config_with_persistence() {
        let config = RunnerConfig::default().with_persistence(false);
        assert!(!config.persist_results);
    }

    #[test]
    fn test_runner_config_with_results_path() {
        let config = RunnerConfig::default().with_results_path("/results");
        assert_eq!(config.results_path, Some("/results".to_string()));
    }

    #[test]
    fn test_runner_config_with_run_name() {
        let config = RunnerConfig::default().with_run_name("test-run");
        assert_eq!(config.run_name_prefix, "test-run");
    }

    #[test]
    fn test_runner_config_with_stage_config() {
        let stage_config = StageTypeConfig::default().with_timeout(7200);
        let config = RunnerConfig::default()
            .with_stage_config(ValidationStageType::Backtest, stage_config);
        assert_eq!(
            config
                .stage_configs
                .get(&ValidationStageType::Backtest)
                .unwrap()
                .timeout_seconds,
            Some(7200)
        );
    }

    #[test]
    fn test_runner_config_without_stage() {
        let config = RunnerConfig::default().without_stage(ValidationStageType::Live);
        assert!(!config
            .stage_configs
            .get(&ValidationStageType::Live)
            .unwrap()
            .enabled);
    }

    #[test]
    fn test_runner_config_production() {
        let config = RunnerConfig::production("/data");
        assert_eq!(config.run_name_prefix, "prod");
        assert_eq!(
            config.pipeline_config.stop_condition,
            StopCondition::StopOnFirstFailure
        );
    }

    #[test]
    fn test_runner_config_research() {
        let config = RunnerConfig::research("/data");
        assert_eq!(config.run_name_prefix, "research");
        assert!(!config.load_previous_results);
    }

    #[test]
    fn test_runner_config_fast() {
        let config = RunnerConfig::fast("/data");
        assert_eq!(config.run_name_prefix, "fast");
        assert!(config
            .stage_configs
            .get(&ValidationStageType::Backtest)
            .unwrap()
            .enabled);
        assert!(!config
            .stage_configs
            .get(&ValidationStageType::Forward)
            .unwrap()
            .enabled);
        assert!(!config
            .stage_configs
            .get(&ValidationStageType::Live)
            .unwrap()
            .enabled);
    }

    #[test]
    fn test_runner_config_validate_success() {
        let config = RunnerConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_runner_config_validate_empty_path() {
        let mut config = RunnerConfig::default();
        config.data_path = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_runner_config_validate_no_stages() {
        let config = RunnerConfig::default()
            .without_stage(ValidationStageType::Backtest)
            .without_stage(ValidationStageType::Forward)
            .without_stage(ValidationStageType::OutOfSample)
            .without_stage(ValidationStageType::Paper)
            .without_stage(ValidationStageType::Live);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_runner_config_serialization() {
        let config = RunnerConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RunnerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.data_path, config.data_path);
        assert_eq!(deserialized.persist_results, config.persist_results);
    }

    // ==================== StageTypeConfig Tests ====================

    #[test]
    fn test_stage_type_config_default() {
        let config = StageTypeConfig::default();
        assert!(config.enabled);
        assert!(config.thresholds.is_none());
    }

    #[test]
    fn test_stage_type_config_default_for_backtest() {
        let config = StageTypeConfig::default_for(ValidationStageType::Backtest);
        assert!(config.enabled);
        assert_eq!(config.period_days, Some(180));
        assert_eq!(config.timeout_seconds, Some(3600));
    }

    #[test]
    fn test_stage_type_config_default_for_forward() {
        let config = StageTypeConfig::default_for(ValidationStageType::Forward);
        assert_eq!(config.period_days, Some(90));
    }

    #[test]
    fn test_stage_type_config_default_for_oos() {
        let config = StageTypeConfig::default_for(ValidationStageType::OutOfSample);
        assert_eq!(config.period_days, Some(30));
    }

    #[test]
    fn test_stage_type_config_default_for_paper() {
        let config = StageTypeConfig::default_for(ValidationStageType::Paper);
        assert_eq!(config.duration_seconds, Some(86400));
    }

    #[test]
    fn test_stage_type_config_default_for_live() {
        let config = StageTypeConfig::default_for(ValidationStageType::Live);
        assert_eq!(config.duration_seconds, Some(86400));
    }

    #[test]
    fn test_stage_type_config_disabled() {
        let config = StageTypeConfig::default().disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_stage_type_config_with_thresholds() {
        let thresholds = ValidationThresholds::strict();
        let config = StageTypeConfig::default().with_thresholds(thresholds.clone());
        assert_eq!(config.thresholds, Some(thresholds));
    }

    #[test]
    fn test_stage_type_config_with_timeout() {
        let config = StageTypeConfig::default().with_timeout(7200);
        assert_eq!(config.timeout_seconds, Some(7200));
    }

    #[test]
    fn test_stage_type_config_with_period_days() {
        let config = StageTypeConfig::default().with_period_days(365);
        assert_eq!(config.period_days, Some(365));
    }

    #[test]
    fn test_stage_type_config_with_duration() {
        let config = StageTypeConfig::default().with_duration(3600);
        assert_eq!(config.duration_seconds, Some(3600));
    }

    #[test]
    fn test_stage_type_config_serialization() {
        let config = StageTypeConfig::default_for(ValidationStageType::Backtest);
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StageTypeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.period_days, config.period_days);
    }

    // ==================== RunnerError Tests ====================

    #[test]
    fn test_runner_error_display() {
        let errors = vec![
            RunnerError::Configuration("test".to_string()),
            RunnerError::StageCreation("test".to_string()),
            RunnerError::StageExecution(ValidationStageType::Backtest, "test".to_string()),
            RunnerError::Persistence("test".to_string()),
            RunnerError::InvalidStageOrder("test".to_string()),
            RunnerError::PreviousResultsNotFound(ValidationStageType::Forward),
            RunnerError::Cancelled("test".to_string()),
        ];

        for error in errors {
            let display = format!("{}", error);
            assert!(!display.is_empty());
        }
    }

    #[test]
    fn test_runner_error_from_stage_error() {
        let stage_error = StageError::ExecutionError("test".to_string());
        let runner_error: RunnerError = stage_error.into();
        match runner_error {
            RunnerError::StageExecution(_, msg) => assert!(msg.contains("test")),
            _ => panic!("Wrong error type"),
        }
    }

    // ==================== StageExecutionResult Tests ====================

    #[test]
    fn test_stage_execution_result_serialization() {
        let result = StageExecutionResult {
            stage_type: ValidationStageType::Backtest,
            outcome: StageOutcome::Pending,
            persisted_path: Some(PathBuf::from("/test")),
            duration_seconds: 1.5,
            executed_at: Utc::now(),
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: StageExecutionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.stage_type, ValidationStageType::Backtest);
        assert_eq!(deserialized.duration_seconds, 1.5);
    }

    // ==================== PipelineRunner Tests ====================

    #[test]
    fn test_pipeline_runner_new() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        assert!(runner.execution_history().is_empty());
    }

    #[test]
    fn test_pipeline_runner_config() {
        let config = RunnerConfig::new("/custom/path");
        let runner = PipelineRunner::new(config);
        assert_eq!(runner.config().data_path, "/custom/path");
    }

    #[test]
    fn test_pipeline_runner_clear_history() {
        let config = RunnerConfig::default();
        let mut runner = PipelineRunner::new(config);
        runner.clear_history();
        assert!(runner.execution_history().is_empty());
    }

    #[test]
    fn test_pipeline_runner_enabled_stages() {
        let config = RunnerConfig::default()
            .without_stage(ValidationStageType::Paper)
            .without_stage(ValidationStageType::Live);
        let runner = PipelineRunner::new(config);
        let enabled = runner.enabled_stages();
        assert_eq!(enabled.len(), 3);
        assert!(enabled.contains(&ValidationStageType::Backtest));
        assert!(!enabled.contains(&ValidationStageType::Paper));
    }

    #[test]
    fn test_pipeline_runner_is_stage_enabled() {
        let config = RunnerConfig::default().without_stage(ValidationStageType::Live);
        let runner = PipelineRunner::new(config);
        assert!(runner.is_stage_enabled(ValidationStageType::Backtest));
        assert!(!runner.is_stage_enabled(ValidationStageType::Live));
    }

    #[test]
    fn test_get_stage_order() {
        let order = PipelineRunner::get_stage_order();
        assert_eq!(order.len(), 5);
        assert_eq!(order[0], ValidationStageType::Backtest);
        assert_eq!(order[1], ValidationStageType::Forward);
        assert_eq!(order[2], ValidationStageType::OutOfSample);
        assert_eq!(order[3], ValidationStageType::Paper);
        assert_eq!(order[4], ValidationStageType::Live);
    }

    #[test]
    fn test_stage_type_name() {
        assert_eq!(stage_type_name(ValidationStageType::Backtest), "backtest");
        assert_eq!(stage_type_name(ValidationStageType::Forward), "forward");
        assert_eq!(stage_type_name(ValidationStageType::OutOfSample), "oos");
        assert_eq!(stage_type_name(ValidationStageType::Paper), "paper");
        assert_eq!(stage_type_name(ValidationStageType::Live), "live");
    }

    // ==================== PipelineRunnerFactory Tests ====================

    #[test]
    fn test_factory_create() {
        let runner = PipelineRunnerFactory::create("/data");
        assert_eq!(runner.config().data_path, "/data");
    }

    #[test]
    fn test_factory_create_production() {
        let runner = PipelineRunnerFactory::create_production("/data");
        assert_eq!(runner.config().run_name_prefix, "prod");
    }

    #[test]
    fn test_factory_create_research() {
        let runner = PipelineRunnerFactory::create_research("/data");
        assert_eq!(runner.config().run_name_prefix, "research");
    }

    #[test]
    fn test_factory_create_fast() {
        let runner = PipelineRunnerFactory::create_fast("/data");
        assert_eq!(runner.config().run_name_prefix, "fast");
        assert!(!runner.is_stage_enabled(ValidationStageType::Forward));
    }

    // ==================== Stop Condition Tests ====================

    #[test]
    fn test_check_stop_condition_first_failure() {
        let config = RunnerConfig::default()
            .with_pipeline_config(
                PipelineConfig::default().with_stop_condition(StopCondition::StopOnFirstFailure),
            );
        let runner = PipelineRunner::new(config);

        let mut result = PipelineResult::default();
        assert!(!runner.check_stop_condition(&result));

        result.stages_failed = 1;
        assert!(runner.check_stop_condition(&result));
    }

    #[test]
    fn test_check_stop_condition_continue() {
        let config = RunnerConfig::default()
            .with_pipeline_config(
                PipelineConfig::default().with_stop_condition(StopCondition::ContinueOnFailure),
            );
        let runner = PipelineRunner::new(config);

        let mut result = PipelineResult::default();
        result.stages_failed = 5;
        assert!(!runner.check_stop_condition(&result));
    }

    #[test]
    fn test_check_stop_condition_n_failures() {
        let config = RunnerConfig::default()
            .with_pipeline_config(
                PipelineConfig::default().with_stop_condition(StopCondition::StopAfterNFailures(3)),
            );
        let runner = PipelineRunner::new(config);

        let mut result = PipelineResult::default();
        result.stages_failed = 2;
        assert!(!runner.check_stop_condition(&result));

        result.stages_failed = 3;
        assert!(runner.check_stop_condition(&result));
    }

    #[test]
    fn test_check_stop_condition_after_stage() {
        let config = RunnerConfig::default().with_pipeline_config(
            PipelineConfig::default()
                .with_stop_condition(StopCondition::StopAfterStage(ValidationStageType::Forward)),
        );
        let runner = PipelineRunner::new(config);

        let mut result = PipelineResult::default();
        assert!(!runner.check_stop_condition(&result));

        result.add_outcome(
            ValidationStageType::Forward,
            StageOutcome::Passed(ValidationResult::default()),
        );
        assert!(runner.check_stop_condition(&result));
    }

    // ==================== Period Calculation Tests ====================

    #[test]
    fn test_calculate_period_backtest() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        let type_config = StageTypeConfig::default_for(ValidationStageType::Backtest);
        let now = Utc::now();

        let (start, end) = runner.calculate_period(ValidationStageType::Backtest, &type_config, now);
        assert!(start < end);
        assert!(end < now);
    }

    #[test]
    fn test_calculate_period_forward() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        let type_config = StageTypeConfig::default_for(ValidationStageType::Forward);
        let now = Utc::now();

        let (start, end) = runner.calculate_period(ValidationStageType::Forward, &type_config, now);
        assert!(start < end);
        assert!(end < now);
    }

    #[test]
    fn test_calculate_period_oos() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        let type_config = StageTypeConfig::default_for(ValidationStageType::OutOfSample);
        let now = Utc::now();

        let (start, end) =
            runner.calculate_period(ValidationStageType::OutOfSample, &type_config, now);
        assert!(start < end);
        assert_eq!(end, now);
    }

    #[test]
    fn test_calculate_period_paper() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        let type_config = StageTypeConfig::default_for(ValidationStageType::Paper);
        let now = Utc::now();

        let (start, end) = runner.calculate_period(ValidationStageType::Paper, &type_config, now);
        assert_eq!(start, now);
        assert!(end > now);
    }

    #[test]
    fn test_calculate_period_live() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        let type_config = StageTypeConfig::default_for(ValidationStageType::Live);
        let now = Utc::now();

        let (start, end) = runner.calculate_period(ValidationStageType::Live, &type_config, now);
        assert_eq!(start, now);
        assert!(end > now);
    }

    // ==================== Context Building Tests ====================

    #[test]
    fn test_build_context() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);

        let algo_config = AlgorithmConfig::default();
        let stage_config = StageConfig::new(ValidationStageType::Backtest, "test")
            .with_period(Utc::now() - Duration::days(30), Utc::now())
            .with_timeout(3600);

        let context = runner.build_context(&algo_config, &stage_config, &[]);

        assert_eq!(context.stage_name, "test");
        assert_eq!(context.timeout_seconds, Some(3600));
        assert!(context.previous_results.is_empty());
    }

    #[test]
    fn test_build_context_with_previous_results() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);

        let algo_config = AlgorithmConfig::default();
        let stage_config = StageConfig::new(ValidationStageType::Forward, "test")
            .with_period(Utc::now() - Duration::days(30), Utc::now());

        let previous = vec![ValidationResult::default()];
        let context = runner.build_context(&algo_config, &stage_config, &previous);

        assert_eq!(context.previous_results.len(), 1);
    }

    // ==================== Integration Tests ====================

    #[tokio::test]
    async fn test_runner_validates_config_on_run_all() {
        let mut config = RunnerConfig::default();
        config.data_path = String::new();
        let mut runner = PipelineRunner::new(config);

        let algo_config = AlgorithmConfig::default();
        let result = runner.run_all(&algo_config).await;

        assert!(result.is_err());
        match result {
            Err(RunnerError::Configuration(_)) => {}
            _ => panic!("Expected configuration error"),
        }
    }

    #[tokio::test]
    async fn test_runner_validates_config_on_run_from() {
        let mut config = RunnerConfig::default();
        config.data_path = String::new();
        let mut runner = PipelineRunner::new(config);

        let algo_config = AlgorithmConfig::default();
        let result = runner
            .run_from(ValidationStageType::Backtest, &algo_config)
            .await;

        assert!(result.is_err());
    }

    #[test]
    fn test_create_stages() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);

        let stage_types = vec![ValidationStageType::Backtest, ValidationStageType::Forward];
        let result = runner.create_stages(&stage_types);

        assert!(result.is_ok());
        let (stages, configs) = result.unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(configs.len(), 2);
    }

    #[test]
    fn test_create_stage_backtest() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        let type_config = StageTypeConfig::default_for(ValidationStageType::Backtest);

        let result = runner.create_stage(ValidationStageType::Backtest, &type_config, Utc::now());
        assert!(result.is_ok());

        let (stage, stage_config) = result.unwrap();
        assert_eq!(stage.stage_type(), ValidationStageType::Backtest);
        assert!(stage_config.name.contains("backtest"));
    }

    #[test]
    fn test_create_stage_paper() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        let type_config = StageTypeConfig::default_for(ValidationStageType::Paper);

        let result = runner.create_stage(ValidationStageType::Paper, &type_config, Utc::now());
        assert!(result.is_ok());

        let (stage, _) = result.unwrap();
        assert_eq!(stage.stage_type(), ValidationStageType::Paper);
    }

    #[test]
    fn test_create_stage_live() {
        let config = RunnerConfig::default();
        let runner = PipelineRunner::new(config);
        let type_config = StageTypeConfig::default_for(ValidationStageType::Live);

        let result = runner.create_stage(ValidationStageType::Live, &type_config, Utc::now());
        assert!(result.is_ok());

        let (stage, _) = result.unwrap();
        assert_eq!(stage.stage_type(), ValidationStageType::Live);
    }

    // ==================== Additional Edge Case Tests ====================

    #[test]
    fn test_runner_config_all_builders() {
        let config = RunnerConfig::new("/data")
            .with_pipeline_config(PipelineConfig::production())
            .with_persistence(true)
            .with_results_path("/results")
            .with_run_name("full-test")
            .with_stage_config(
                ValidationStageType::Backtest,
                StageTypeConfig::default_for(ValidationStageType::Backtest),
            )
            .without_stage(ValidationStageType::Live);

        assert_eq!(config.data_path, "/data");
        assert!(config.persist_results);
        assert_eq!(config.results_path, Some("/results".to_string()));
        assert_eq!(config.run_name_prefix, "full-test");
    }

    #[test]
    fn test_stage_type_config_all_builders() {
        let config = StageTypeConfig::default()
            .disabled()
            .with_thresholds(ValidationThresholds::relaxed())
            .with_timeout(1800)
            .with_period_days(60)
            .with_duration(7200);

        assert!(!config.enabled);
        assert!(config.thresholds.is_some());
        assert_eq!(config.timeout_seconds, Some(1800));
        assert_eq!(config.period_days, Some(60));
        assert_eq!(config.duration_seconds, Some(7200));
    }

    #[test]
    fn test_empty_stage_configs() {
        let mut config = RunnerConfig::default();
        config.stage_configs.clear();
        let runner = PipelineRunner::new(config);

        // All stages should report as enabled (default behavior)
        assert!(runner.is_stage_enabled(ValidationStageType::Backtest));
    }

    #[test]
    fn test_execution_result_with_path() {
        let result = StageExecutionResult {
            stage_type: ValidationStageType::Backtest,
            outcome: StageOutcome::Passed(ValidationResult::default()),
            persisted_path: Some(PathBuf::from("/test/result.json")),
            duration_seconds: 10.5,
            executed_at: Utc::now(),
        };

        assert!(result.persisted_path.is_some());
        assert_eq!(
            result.persisted_path.unwrap().to_str().unwrap(),
            "/test/result.json"
        );
    }

    #[test]
    fn test_execution_result_without_path() {
        let result = StageExecutionResult {
            stage_type: ValidationStageType::Forward,
            outcome: StageOutcome::Skipped("Test".to_string()),
            persisted_path: None,
            duration_seconds: 0.0,
            executed_at: Utc::now(),
        };

        assert!(result.persisted_path.is_none());
    }
}

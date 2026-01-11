//! Validate Commands
//!
//! This module provides all validation-related commands that can be executed
//! from both CLI and TUI interfaces.
//!
//! # Commands
//!
//! - `run` - Run validation pipeline
//! - `presets` - List available pipeline presets
//! - `stages` - List available validation stages
//! - `status` - Show validation status
//! - `show` - Show detailed validation info

use std::path::PathBuf;
use std::sync::Arc;
use std::fs;
use std::time::Instant;
use anyhow::{Result, Context, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::commands::common::{ProgressCallback, ProgressEvent, LogLevel};
use crate::commands::params::validate_params::{
    RunParams as ValidateRunParams,
    PresetsParams,
    StagesParams,
    StatusParams as ValidateStatusParams,
    ShowParams,
};
use crate::core::{
    AlgorithmConfig, ValidationResult, ValidationStageType,
    ResearchStore, ResearchStoreConfig,
    ResultsStore, ResultsStoreConfig,
};
use crate::validation::{
    PipelineResult, PipelineRunner, PipelineStatus, RunnerConfig, StageOutcome,
};

/// Result of a validation pipeline run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    /// Pipeline result
    pub pipeline_result: PipelineResult,
    /// Algorithm configuration ID that was validated
    pub algorithm_config_id: String,
    /// Algorithm name
    pub algorithm_name: String,
    /// Duration in seconds
    pub duration_seconds: f64,
}

/// Result of a presets query (info only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresetsResult {
    /// Available presets with descriptions
    pub presets: Vec<PresetInfo>,
}

/// Information about a preset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresetInfo {
    /// Preset name
    pub name: String,
    /// Preset description
    pub description: String,
}

/// Result of a stages query (info only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagesResult {
    /// Available stages with descriptions
    pub stages: Vec<StageInfo>,
}

/// Information about a stage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageInfo {
    /// Stage name (lowercase)
    pub name: String,
    /// Stage type
    pub stage_type: String,
    /// Stage description
    pub description: String,
}

/// Result of a status query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResult {
    /// List of validation run summaries
    pub runs: Vec<RunSummary>,
    /// Total number of runs in store
    pub total_runs: usize,
}

/// Summary of a validation run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    /// Run ID
    pub id: String,
    /// Stage type
    pub stage_type: String,
    /// Whether the run passed
    pub passed: bool,
    /// Number of trades
    pub trade_count: u64,
    /// Timestamp when validated
    pub timestamp: String,
}

/// Result of a show command (detailed run info)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShowResult {
    /// Validation result details
    pub result: ValidationResult,
    /// Run ID
    pub run_id: String,
    /// Stage type
    pub stage_type: String,
    /// Algorithm config ID
    pub config_id: String,
    /// Timestamp when validated
    pub timestamp: String,
    /// Whether the run passed
    pub passed: bool,
}

/// Validate command executor
///
/// All validate commands are executed through this struct.
/// Commands support progress callbacks for long-running operations.
pub struct ValidateCommands;

impl ValidateCommands {
    /// Run the validation pipeline
    ///
    /// This command runs the full validation pipeline on an algorithm configuration,
    /// executing all enabled stages in sequence.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for the pipeline run
    /// * `callback` - Progress callback for updates during execution
    ///
    /// # Returns
    ///
    /// Pipeline run result containing all stage outcomes
    pub async fn run(
        params: ValidateRunParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<RunResult> {
        let start_time = Instant::now();

        callback.on_event(ProgressEvent::Started {
            total: Some(5), // Maximum 5 stages
            message: "Starting validation pipeline".to_string(),
        });

        // Load or generate algorithm config
        let algorithm_config = Self::load_algorithm_config(&params).await?;

        if !params.quiet && !params.json {
            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: format!("Algorithm: {} ({})", algorithm_config.name, algorithm_config.id),
            });
            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: format!("Strategy: {:?}", algorithm_config.strategy_type),
            });
        }

        // Build runner config
        let runner_config = Self::build_runner_config(&params)?;

        // Create results store
        let results_store = if params.no_persist {
            None
        } else {
            Some(
                ResultsStore::new(ResultsStoreConfig::with_path(&params.results))
                    .context("Failed to create results store")?
            )
        };

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Data path: {:?}", params.data),
        });

        // Create pipeline runner
        let mut runner = if let Some(store) = results_store {
            PipelineRunner::with_results_store(runner_config, store)
        } else {
            PipelineRunner::new(runner_config)
        };

        // Run the pipeline with progress updates
        callback.on_event(ProgressEvent::Progress {
            current: 0,
            total: Some(5),
            message: "Running validation stages...".to_string(),
        });

        let pipeline_result = if let Some(from_stage) = params.from_stage {
            if !params.quiet && !params.json {
                callback.on_event(ProgressEvent::Log {
                    level: LogLevel::Info,
                    message: format!("Starting from stage: {:?}", from_stage),
                });
            }
            runner
                .run_from(from_stage, &algorithm_config)
                .await
                .context("Failed to run pipeline from stage")?
        } else {
            runner.run_all(&algorithm_config).await
                .context("Failed to run pipeline")?
        };

        let duration_seconds = start_time.elapsed().as_secs_f64();

        // Send progress updates for each completed stage
        for (stage_type, outcome) in &pipeline_result.stage_outcomes {
            let stage_name = format!("{:?}", stage_type);
            match outcome {
                StageOutcome::Passed(_) => {
                    callback.on_event(ProgressEvent::Progress {
                        current: pipeline_result.stages_passed,
                        total: Some(5),
                        message: format!("Stage {}: PASSED", stage_name),
                    });
                }
                StageOutcome::Failed(_) => {
                    callback.on_event(ProgressEvent::Progress {
                        current: pipeline_result.stages_passed,
                        total: Some(5),
                        message: format!("Stage {}: FAILED", stage_name),
                    });
                    callback.on_event(ProgressEvent::Log {
                        level: LogLevel::Warn,
                        message: format!("Stage {} failed validation", stage_name),
                    });
                }
                StageOutcome::Error(e) => {
                    callback.on_event(ProgressEvent::Log {
                        level: LogLevel::Error,
                        message: format!("Stage {} error: {}", stage_name, e),
                    });
                }
                StageOutcome::Skipped(reason) => {
                    callback.on_event(ProgressEvent::Log {
                        level: LogLevel::Info,
                        message: format!("Stage {} skipped: {}", stage_name, reason),
                    });
                }
                StageOutcome::Pending => {}
            }
        }

        // Send completion event
        let status_message = match pipeline_result.status {
            PipelineStatus::Passed => "Pipeline PASSED - all stages completed successfully".to_string(),
            PipelineStatus::Failed => "Pipeline FAILED - algorithm did not pass validation".to_string(),
            PipelineStatus::Error => "Pipeline ERROR - execution encountered errors".to_string(),
            _ => format!("Pipeline completed with status: {:?}", pipeline_result.status),
        };

        callback.on_event(ProgressEvent::Completed {
            message: status_message.clone(),
        });

        let result = RunResult {
            pipeline_result,
            algorithm_config_id: algorithm_config.id.clone(),
            algorithm_name: algorithm_config.name.clone(),
            duration_seconds,
        };

        Ok(result)
    }

    /// List available pipeline presets
    ///
    /// This command displays information about available validation pipeline presets.
    ///
    /// # Arguments
    ///
    /// * `_params` - Parameters (currently unused, info only)
    /// * `callback` - Progress callback (minimal usage for this quick query)
    ///
    /// # Returns
    ///
    /// Presets result containing all available presets
    pub fn presets(
        _params: PresetsParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<PresetsResult> {
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Listing available presets".to_string(),
        });

        let presets = vec![
            PresetInfo {
                name: "default".to_string(),
                description: "Standard configuration for general use. Runs all stages, stops on first failure.".to_string(),
            },
            PresetInfo {
                name: "production".to_string(),
                description: "Conservative settings for live deployment. Strict thresholds, full audit trail.".to_string(),
            },
            PresetInfo {
                name: "research".to_string(),
                description: "Relaxed settings for exploration. Continues on failures, lower thresholds.".to_string(),
            },
            PresetInfo {
                name: "fast".to_string(),
                description: "Quick validation (backtest only). Skips forward, OOS, paper, live stages.".to_string(),
            },
        ];

        callback.on_event(ProgressEvent::Completed {
            message: format!("Found {} presets", presets.len()),
        });

        Ok(PresetsResult { presets })
    }

    /// List available validation stages
    ///
    /// This command displays information about available validation stages.
    ///
    /// # Arguments
    ///
    /// * `_params` - Parameters (currently unused, info only)
    /// * `callback` - Progress callback (minimal usage for this quick query)
    ///
    /// # Returns
    ///
    /// Stages result containing all available stages
    pub fn stages(
        _params: StagesParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<StagesResult> {
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Listing available stages".to_string(),
        });

        let stages = vec![
            StageInfo {
                name: "backtest".to_string(),
                stage_type: format!("{:?}", ValidationStageType::Backtest),
                description: "Historical replay validation. Replays algorithm on historical data.".to_string(),
            },
            StageInfo {
                name: "forward".to_string(),
                stage_type: format!("{:?}", ValidationStageType::Forward),
                description: "Walk-forward validation. Splits data into train/test windows.".to_string(),
            },
            StageInfo {
                name: "oos".to_string(),
                stage_type: format!("{:?}", ValidationStageType::OutOfSample),
                description: "Out-of-sample validation. Final holdout validation (default 20%).".to_string(),
            },
            StageInfo {
                name: "paper".to_string(),
                stage_type: format!("{:?}", ValidationStageType::Paper),
                description: "Paper trading validation. Live data, simulated execution.".to_string(),
            },
            StageInfo {
                name: "live".to_string(),
                stage_type: format!("{:?}", ValidationStageType::Live),
                description: "Live trading validation. Real execution with OCO risk management.".to_string(),
            },
        ];

        callback.on_event(ProgressEvent::Completed {
            message: format!("Found {} stages", stages.len()),
        });

        Ok(StagesResult { stages })
    }

    /// Show status of previous validation runs
    ///
    /// This command displays a summary of recent validation runs from the results store.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for the status query
    /// * `callback` - Progress callback (minimal usage for this quick query)
    ///
    /// # Returns
    ///
    /// Status result containing recent run summaries
    pub fn status(
        params: ValidateStatusParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<StatusResult> {
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: format!("Loading status for last {} runs", params.last),
        });

        // Validate parameters
        Self::validate_status_params(&params)?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Opening results store: {:?}", params.results),
        });

        // Open results store
        let mut results_store = ResultsStore::new(ResultsStoreConfig::with_path(&params.results))
            .context("Failed to open results store")?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Loading run IDs...".to_string(),
        });

        // Load run IDs
        let ids = results_store.list_ids()
            .context("Failed to list run IDs")?;

        let total_runs = ids.len();

        // Build run summaries
        let mut runs = Vec::new();
        for id in ids.iter().take(params.last) {
            if let Some(result) = results_store.load_by_id(id)
                .context(format!("Failed to load run: {}", id))? {
                let stage_type_str = match result.stage_type {
                    ValidationStageType::Backtest => "Backtest",
                    ValidationStageType::Forward => "Forward",
                    ValidationStageType::OutOfSample => "OOS",
                    ValidationStageType::Paper => "Paper",
                    ValidationStageType::Live => "Live",
                };

                runs.push(RunSummary {
                    id: result.id.clone(),
                    stage_type: stage_type_str.to_string(),
                    passed: result.passed,
                    trade_count: result.metrics.trade_count as u64,
                    timestamp: result.validated_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                });
            }
        }

        callback.on_event(ProgressEvent::Completed {
            message: format!("Loaded {} runs (showing last {})", total_runs, params.last),
        });

        Ok(StatusResult {
            runs,
            total_runs,
        })
    }

    /// Show detailed information about a specific validation run
    ///
    /// This command displays detailed metrics and information for a specific validation run.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for the show query
    /// * `callback` - Progress callback (minimal usage for this quick query)
    ///
    /// # Returns
    ///
    /// Show result containing detailed run information
    pub fn show(
        params: ShowParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<ShowResult> {
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: format!("Loading run details: {}", params.run_id),
        });

        // Validate parameters
        Self::validate_show_params(&params)?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Opening results store: {:?}", params.results),
        });

        // Open results store
        let mut results_store = ResultsStore::new(ResultsStoreConfig::with_path(&params.results))
            .context("Failed to open results store")?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loading run: {}", params.run_id),
        });

        // Load the specific run
        let result = results_store
            .load_by_id(&params.run_id)
            .context(format!("Failed to load run: {}", params.run_id))?
            .ok_or_else(|| anyhow!("Run not found: {}", params.run_id))?;

        let stage_type_str = format!("{:?}", result.stage_type);

        callback.on_event(ProgressEvent::Completed {
            message: format!("Loaded run: {} ({})", params.run_id, stage_type_str),
        });

        Ok(ShowResult {
            result: result.clone(),
            run_id: result.id.clone(),
            stage_type: stage_type_str,
            config_id: result.config_id.clone(),
            timestamp: result.validated_at.to_rfc3339(),
            passed: result.passed,
        })
    }

    // ==================== Private Helper Functions ====================

    /// Load algorithm configuration from file, research state, or default
    async fn load_algorithm_config(params: &ValidateRunParams) -> Result<AlgorithmConfig> {
        if let Some(ref config_path) = params.config {
            // Load from JSON file
            let content = fs::read_to_string(config_path)
                .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;

            let config: AlgorithmConfig = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {}", config_path.display()))?;

            Ok(config)
        } else if let Some(ref research_path) = params.from_research {
            // Generate from research state
            let mut store = ResearchStore::new(ResearchStoreConfig::with_path(research_path))
                .context("Failed to open research store")?;

            // Use "default" as the symbol for research state
            let research_state = store
                .load("default")?
                .ok_or_else(|| anyhow!("No research state found at: {}", research_path.display()))?;

            let config = AlgorithmConfig::from_research(&research_state);
            Ok(config)
        } else {
            // Use default config
            Ok(AlgorithmConfig::default())
        }
    }

    /// Build runner configuration from parameters
    fn build_runner_config(params: &ValidateRunParams) -> Result<RunnerConfig> {
        let data_path = params.data.to_string_lossy().to_string();

        let mut config = match params.preset.as_deref() {
            Some("production") => RunnerConfig::production(&data_path),
            Some("research") => RunnerConfig::research(&data_path),
            Some("fast") => RunnerConfig::fast(&data_path),
            Some("default") | None => RunnerConfig::new(&data_path),
            _ => unreachable!(), // Validation in builder ensures only valid presets
        };

        // Apply command-line overrides
        config.run_name_prefix = params.name.clone();
        config.results_path = Some(params.results.to_string_lossy().to_string());
        config.persist_results = !params.no_persist;

        // Handle continue-on-failure
        if params.continue_on_failure {
            config.pipeline_config.stop_condition =
                crate::validation::StopCondition::ContinueOnFailure;
        }

        // Handle stage selection
        if let Some(ref stages) = params.stages {
            // Disable all stages first
            for stage_type in [
                ValidationStageType::Backtest,
                ValidationStageType::Forward,
                ValidationStageType::OutOfSample,
                ValidationStageType::Paper,
                ValidationStageType::Live,
            ] {
                if let Some(stage_config) = config.stage_configs.get_mut(&stage_type) {
                    stage_config.enabled = stages.contains(&stage_type);
                }
            }
        }

        config.validate().map_err(|e| anyhow!("{}", e))?;
        Ok(config)
    }

    /// Validate status parameters
    fn validate_status_params(params: &ValidateStatusParams) -> Result<()> {
        // Check results directory exists (or can be created)
        if !params.results.exists() {
            // Try to create it
            fs::create_dir_all(&params.results)
                .context(format!("Failed to create results directory: {:?}", params.results))?;
        }

        // Check last is reasonable (already validated in builder, but double-check)
        if params.last == 0 {
            anyhow::bail!("last must be greater than 0");
        }
        if params.last > 1000 {
            anyhow::bail!("last too large (max 1000): {}", params.last);
        }

        Ok(())
    }

    /// Validate show parameters
    fn validate_show_params(params: &ShowParams) -> Result<()> {
        // Check results directory exists
        if !params.results.exists() {
            anyhow::bail!("Results directory does not exist: {:?}", params.results);
        }

        // Check run_id is valid (already validated in builder, but double-check)
        if params.run_id.is_empty() {
            anyhow::bail!("run_id cannot be empty");
        }
        if params.run_id.len() > 200 {
            anyhow::bail!("run_id too long (max 200 characters): {}", params.run_id.len());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::commands::common::NoOpCallback;

    // ==================== Parameter Validation Tests ====================

    #[test]
    fn test_validate_status_params_success() {
        let temp_dir = TempDir::new().unwrap();
        let results_path = temp_dir.path().to_path_buf();

        let params = ValidateStatusParams {
            results: results_path,
            last: 10,
        };

        let callback = Arc::new(NoOpCallback);
        // Should not panic for valid params
        let _ = ValidateCommands::validate_status_params(&params);
    }

    #[test]
    fn test_validate_status_params_invalid_last_zero() {
        let temp_dir = TempDir::new().unwrap();
        let params = ValidateStatusParams {
            results: temp_dir.path().to_path_buf(),
            last: 0,
        };

        let result = ValidateCommands::validate_status_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("last must be greater than 0"));
    }

    #[test]
    fn test_validate_status_params_invalid_last_too_large() {
        let temp_dir = TempDir::new().unwrap();
        let params = ValidateStatusParams {
            results: temp_dir.path().to_path_buf(),
            last: 1001,
        };

        let result = ValidateCommands::validate_status_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("last too large"));
    }

    #[test]
    fn test_validate_show_params_success() {
        let temp_dir = TempDir::new().unwrap();
        let results_path = temp_dir.path().to_path_buf();

        let params = ShowParams {
            results: results_path,
            run_id: "test-run-id".to_string(),
            json: false,
            verbose: false,
        };

        // Should not panic for valid params (though will fail on missing run)
        let _ = ValidateCommands::validate_show_params(&params);
    }

    #[test]
    fn test_validate_show_params_missing_results_dir() {
        let params = ShowParams {
            results: PathBuf::from("/nonexistent/results"),
            run_id: "test-run-id".to_string(),
            json: false,
            verbose: false,
        };

        let result = ValidateCommands::validate_show_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Results directory does not exist"));
    }

    #[test]
    fn test_validate_show_params_empty_run_id() {
        let temp_dir = TempDir::new().unwrap();
        let params = ShowParams {
            results: temp_dir.path().to_path_buf(),
            run_id: "".to_string(),
            json: false,
            verbose: false,
        };

        let result = ValidateCommands::validate_show_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("run_id cannot be empty"));
    }

    // ==================== Presets Command Tests ====================

    #[test]
    fn test_presets_command() {
        let params = PresetsParams {};
        let callback = Arc::new(NoOpCallback);

        let result = ValidateCommands::presets(params, callback).unwrap();

        assert_eq!(result.presets.len(), 4);
        assert!(result.presets.iter().any(|p| p.name == "default"));
        assert!(result.presets.iter().any(|p| p.name == "production"));
        assert!(result.presets.iter().any(|p| p.name == "research"));
        assert!(result.presets.iter().any(|p| p.name == "fast"));
    }

    #[test]
    fn test_presets_command_contains_descriptions() {
        let params = PresetsParams {};
        let callback = Arc::new(NoOpCallback);

        let result = ValidateCommands::presets(params, callback).unwrap();

        for preset in &result.presets {
            assert!(!preset.description.is_empty());
        }
    }

    // ==================== Stages Command Tests ====================

    #[test]
    fn test_stages_command() {
        let params = StagesParams {};
        let callback = Arc::new(NoOpCallback);

        let result = ValidateCommands::stages(params, callback).unwrap();

        assert_eq!(result.stages.len(), 5);
        assert!(result.stages.iter().any(|s| s.name == "backtest"));
        assert!(result.stages.iter().any(|s| s.name == "forward"));
        assert!(result.stages.iter().any(|s| s.name == "oos"));
        assert!(result.stages.iter().any(|s| s.name == "paper"));
        assert!(result.stages.iter().any(|s| s.name == "live"));
    }

    #[test]
    fn test_stages_command_contains_descriptions() {
        let params = StagesParams {};
        let callback = Arc::new(NoOpCallback);

        let result = ValidateCommands::stages(params, callback).unwrap();

        for stage in &result.stages {
            assert!(!stage.description.is_empty());
            assert!(!stage.stage_type.is_empty());
        }
    }

    // ==================== Build Runner Config Tests ====================

    #[test]
    fn test_build_runner_config_default() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().to_path_buf();

        let params = ValidateRunParams {
            config: None,
            from_research: None,
            stages: None,
            from_stage: None,
            data: data_path.clone(),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = ValidateCommands::build_runner_config(&params).unwrap();
        assert_eq!(config.data_path, data_path.to_string_lossy());
        assert_eq!(config.run_name_prefix, "test");
    }

    #[test]
    fn test_build_runner_config_production_preset() {
        let temp_dir = TempDir::new().unwrap();
        let params = ValidateRunParams {
            config: None,
            from_research: None,
            stages: None,
            from_stage: None,
            data: temp_dir.path().to_path_buf(),
            results: PathBuf::from("./results"),
            preset: Some("production".to_string()),
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = ValidateCommands::build_runner_config(&params).unwrap();
        assert_eq!(config.run_name_prefix, "test"); // Name override still works
    }

    #[test]
    fn test_build_runner_config_with_stages() {
        let temp_dir = TempDir::new().unwrap();
        let stages = vec![
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
        ];

        let params = ValidateRunParams {
            config: None,
            from_research: None,
            stages: Some(stages.clone()),
            from_stage: None,
            data: temp_dir.path().to_path_buf(),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = ValidateCommands::build_runner_config(&params).unwrap();
        
        // Check that only specified stages are enabled
        assert!(config.stage_configs.get(&ValidationStageType::Backtest)
            .map(|c| c.enabled).unwrap_or(false));
        assert!(config.stage_configs.get(&ValidationStageType::Forward)
            .map(|c| c.enabled).unwrap_or(false));
    }

    #[test]
    fn test_build_runner_config_continue_on_failure() {
        let temp_dir = TempDir::new().unwrap();
        let params = ValidateRunParams {
            config: None,
            from_research: None,
            stages: None,
            from_stage: None,
            data: temp_dir.path().to_path_buf(),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: true,
            no_persist: false,
        };

        let config = ValidateCommands::build_runner_config(&params).unwrap();
        assert_eq!(
            config.pipeline_config.stop_condition,
            crate::validation::StopCondition::ContinueOnFailure
        );
    }

    #[test]
    fn test_build_runner_config_no_persist() {
        let temp_dir = TempDir::new().unwrap();
        let params = ValidateRunParams {
            config: None,
            from_research: None,
            stages: None,
            from_stage: None,
            data: temp_dir.path().to_path_buf(),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: true,
        };

        let config = ValidateCommands::build_runner_config(&params).unwrap();
        assert!(!config.persist_results);
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn test_presets_result_serialize() {
        let result = PresetsResult {
            presets: vec![
                PresetInfo {
                    name: "test".to_string(),
                    description: "Test preset".to_string(),
                },
            ],
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: PresetsResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.presets.len(), deserialized.presets.len());
        assert_eq!(result.presets[0].name, deserialized.presets[0].name);
    }

    #[test]
    fn test_stages_result_serialize() {
        let result = StagesResult {
            stages: vec![
                StageInfo {
                    name: "backtest".to_string(),
                    stage_type: "Backtest".to_string(),
                    description: "Test stage".to_string(),
                },
            ],
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: StagesResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.stages.len(), deserialized.stages.len());
        assert_eq!(result.stages[0].name, deserialized.stages[0].name);
    }

    #[test]
    fn test_status_result_serialize() {
        let result = StatusResult {
            runs: vec![
                RunSummary {
                    id: "test-id".to_string(),
                    stage_type: "Backtest".to_string(),
                    passed: true,
                    trade_count: 100,
                    timestamp: "2024-01-01 00:00:00".to_string(),
                },
            ],
            total_runs: 1,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: StatusResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.total_runs, deserialized.total_runs);
        assert_eq!(result.runs.len(), deserialized.runs.len());
    }

    #[test]
    fn test_show_result_serialize() {
        let temp_dir = TempDir::new().unwrap();
        let result = ShowResult {
            result: ValidationResult::default(),
            run_id: "test-id".to_string(),
            stage_type: "Backtest".to_string(),
            config_id: "config-id".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            passed: true,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ShowResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.run_id, deserialized.run_id);
        assert_eq!(result.stage_type, deserialized.stage_type);
        assert_eq!(result.passed, deserialized.passed);
    }
}

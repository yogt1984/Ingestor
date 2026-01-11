//! Validate Command Parameters
//!
//! This module defines parameter structs and builders for all validate commands.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use crate::core::ValidationStageType;

/// Parameters for the `run` command (run validation pipeline)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunParams {
    /// Path to algorithm config file (JSON)
    pub config: Option<PathBuf>,
    /// Generate config from research state at this path
    pub from_research: Option<PathBuf>,
    /// Comma-separated list of stages to run
    pub stages: Option<Vec<ValidationStageType>>,
    /// Start from this stage (for partial runs)
    pub from_stage: Option<ValidationStageType>,
    /// Path to data directory containing Parquet files
    pub data: PathBuf,
    /// Path to results directory for persistence
    pub results: PathBuf,
    /// Runner preset to use
    pub preset: Option<String>,
    /// Quiet mode (minimal output)
    pub quiet: bool,
    /// Output results as JSON
    pub json: bool,
    /// Save results to file
    pub output: Option<PathBuf>,
    /// Run name prefix for identification
    pub name: String,
    /// Continue on failure (don't stop on first failed stage)
    pub continue_on_failure: bool,
    /// Disable persistence (don't save results)
    pub no_persist: bool,
}

/// Builder for `RunParams` with validation
pub struct RunParamsBuilder {
    config: Option<PathBuf>,
    from_research: Option<PathBuf>,
    stages: Option<Vec<ValidationStageType>>,
    from_stage: Option<ValidationStageType>,
    data: Option<PathBuf>,
    results: Option<PathBuf>,
    preset: Option<String>,
    quiet: Option<bool>,
    json: Option<bool>,
    output: Option<PathBuf>,
    name: Option<String>,
    continue_on_failure: Option<bool>,
    no_persist: Option<bool>,
}

impl RunParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            config: None,
            from_research: None,
            stages: None,
            from_stage: None,
            data: None,
            results: None,
            preset: None,
            quiet: None,
            json: None,
            output: None,
            name: None,
            continue_on_failure: None,
            no_persist: None,
        }
    }

    /// Set config file path
    pub fn with_config(mut self, config: Option<PathBuf>) -> Self {
        self.config = config;
        self
    }

    /// Set research path for config generation
    pub fn with_from_research(mut self, from_research: Option<PathBuf>) -> Self {
        self.from_research = from_research;
        self
    }

    /// Set stages to run
    pub fn with_stages(mut self, stages: Option<Vec<ValidationStageType>>) -> Self {
        self.stages = stages;
        self
    }

    /// Set starting stage
    pub fn with_from_stage(mut self, from_stage: Option<ValidationStageType>) -> Self {
        self.from_stage = from_stage;
        self
    }

    /// Set data directory path
    pub fn with_data(mut self, data: PathBuf) -> Self {
        self.data = Some(data);
        self
    }

    /// Set results directory path
    pub fn with_results(mut self, results: PathBuf) -> Self {
        self.results = Some(results);
        self
    }

    /// Set preset name
    pub fn with_preset(mut self, preset: Option<String>) -> Self {
        self.preset = preset;
        self
    }

    /// Set quiet mode
    pub fn with_quiet(mut self, quiet: bool) -> Self {
        self.quiet = Some(quiet);
        self
    }

    /// Set JSON output flag
    pub fn with_json(mut self, json: bool) -> Self {
        self.json = Some(json);
        self
    }

    /// Set output file path
    pub fn with_output(mut self, output: Option<PathBuf>) -> Self {
        self.output = output;
        self
    }

    /// Set run name prefix
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Set continue on failure flag
    pub fn with_continue_on_failure(mut self, continue_on_failure: bool) -> Self {
        self.continue_on_failure = Some(continue_on_failure);
        self
    }

    /// Set no persist flag
    pub fn with_no_persist(mut self, no_persist: bool) -> Self {
        self.no_persist = Some(no_persist);
        self
    }

    /// Build `RunParams` with validation
    pub fn build(self) -> Result<RunParams> {
        let data = self.data
            .unwrap_or_else(|| PathBuf::from("./data/features"));
        
        let results = self.results
            .unwrap_or_else(|| PathBuf::from("./results"));

        // Validate that either config or from_research is provided (but not both)
        if self.config.is_some() && self.from_research.is_some() {
            anyhow::bail!("Cannot specify both --config and --from-research");
        }

        // Validate preset if provided
        if let Some(ref preset) = self.preset {
            let valid_presets = ["default", "production", "research", "fast"];
            if !valid_presets.contains(&preset.as_str()) {
                anyhow::bail!("Invalid preset '{}'. Valid options: {}", preset, valid_presets.join(", "));
            }
        }

        Ok(RunParams {
            config: self.config,
            from_research: self.from_research,
            stages: self.stages,
            from_stage: self.from_stage,
            data,
            results,
            preset: self.preset,
            quiet: self.quiet.unwrap_or(false),
            json: self.json.unwrap_or(false),
            output: self.output,
            name: self.name.unwrap_or_else(|| "validate".to_string()),
            continue_on_failure: self.continue_on_failure.unwrap_or(false),
            no_persist: self.no_persist.unwrap_or(false),
        })
    }
}

impl Default for RunParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `presets` command (info only - list available presets)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresetsParams {
    // No parameters needed - this is just informational
}

/// Builder for `PresetsParams`
pub struct PresetsParamsBuilder;

impl PresetsParamsBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self
    }

    /// Build `PresetsParams`
    pub fn build(self) -> Result<PresetsParams> {
        Ok(PresetsParams {})
    }
}

impl Default for PresetsParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `stages` command (info only - list available stages)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagesParams {
    // No parameters needed - this is just informational
}

/// Builder for `StagesParams`
pub struct StagesParamsBuilder;

impl StagesParamsBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self
    }

    /// Build `StagesParams`
    pub fn build(self) -> Result<StagesParams> {
        Ok(StagesParams {})
    }
}

impl Default for StagesParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `status` command (show status of previous runs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusParams {
    /// Path to results directory
    pub results: PathBuf,
    /// Show last N runs
    pub last: usize,
}

/// Builder for `StatusParams` with validation
pub struct StatusParamsBuilder {
    results: Option<PathBuf>,
    last: Option<usize>,
}

impl StatusParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            results: None,
            last: None,
        }
    }

    /// Set results directory path
    pub fn with_results(mut self, results: PathBuf) -> Self {
        self.results = Some(results);
        self
    }

    /// Set number of runs to show
    pub fn with_last(mut self, last: usize) -> Self {
        self.last = Some(last);
        self
    }

    /// Build `StatusParams` with validation
    pub fn build(self) -> Result<StatusParams> {
        let results = self.results
            .unwrap_or_else(|| PathBuf::from("./results"));

        let last = self.last.unwrap_or(10);
        if last == 0 {
            anyhow::bail!("last must be greater than 0");
        }
        if last > 1000 {
            anyhow::bail!("last too large (max 1000): {}", last);
        }

        Ok(StatusParams {
            results,
            last,
        })
    }
}

impl Default for StatusParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `show` command (show detailed info about a specific run)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShowParams {
    /// Path to results directory
    pub results: PathBuf,
    /// Run ID to show
    pub run_id: String,
    /// Output results as JSON
    pub json: bool,
    /// Show verbose output
    pub verbose: bool,
}

/// Builder for `ShowParams` with validation
pub struct ShowParamsBuilder {
    results: Option<PathBuf>,
    run_id: Option<String>,
    json: Option<bool>,
    verbose: Option<bool>,
}

impl ShowParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            results: None,
            run_id: None,
            json: None,
            verbose: None,
        }
    }

    /// Set results directory path
    pub fn with_results(mut self, results: PathBuf) -> Self {
        self.results = Some(results);
        self
    }

    /// Set run ID
    pub fn with_run_id(mut self, run_id: String) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Set JSON output flag
    pub fn with_json(mut self, json: bool) -> Self {
        self.json = Some(json);
        self
    }

    /// Set verbose flag
    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = Some(verbose);
        self
    }

    /// Build `ShowParams` with validation
    pub fn build(self) -> Result<ShowParams> {
        let results = self.results
            .unwrap_or_else(|| PathBuf::from("./results"));

        let run_id = self.run_id
            .ok_or_else(|| anyhow::anyhow!("run_id is required"))?;

        if run_id.is_empty() {
            anyhow::bail!("run_id cannot be empty");
        }
        if run_id.len() > 200 {
            anyhow::bail!("run_id too long (max 200 characters): {}", run_id.len());
        }

        Ok(ShowParams {
            results,
            run_id,
            json: self.json.unwrap_or(false),
            verbose: self.verbose.unwrap_or(false),
        })
    }
}

impl Default for ShowParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ==================== RunParams Tests ====================

    #[test]
    fn test_run_params_builder_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().to_path_buf();

        let params = RunParamsBuilder::new()
            .with_data(data_path.clone())
            .build()
            .unwrap();

        assert_eq!(params.data, data_path);
        assert_eq!(params.results, PathBuf::from("./results"));
        assert_eq!(params.name, "validate");
        assert!(!params.quiet);
        assert!(!params.json);
        assert!(!params.continue_on_failure);
        assert!(!params.no_persist);
    }

    #[test]
    fn test_run_params_builder_all_fields() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().to_path_buf();
        let results_path = temp_dir.path().join("results").to_path_buf();

        let params = RunParamsBuilder::new()
            .with_config(Some(temp_dir.path().join("config.json")))
            .with_data(data_path.clone())
            .with_results(results_path.clone())
            .with_preset(Some("production".to_string()))
            .with_name("test-run".to_string())
            .with_quiet(true)
            .with_json(true)
            .with_output(Some(temp_dir.path().join("output.json")))
            .with_continue_on_failure(true)
            .with_no_persist(true)
            .build()
            .unwrap();

        assert_eq!(params.data, data_path);
        assert_eq!(params.results, results_path);
        assert_eq!(params.name, "test-run");
        assert_eq!(params.preset, Some("production".to_string()));
        assert!(params.quiet);
        assert!(params.json);
        assert!(params.continue_on_failure);
        assert!(params.no_persist);
    }

    #[test]
    fn test_run_params_both_config_and_from_research() {
        let temp_dir = TempDir::new().unwrap();
        let result = RunParamsBuilder::new()
            .with_data(temp_dir.path().to_path_buf())
            .with_config(Some(temp_dir.path().join("config.json")))
            .with_from_research(Some(temp_dir.path().join("research")))
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot specify both"));
    }

    #[test]
    fn test_run_params_invalid_preset() {
        let temp_dir = TempDir::new().unwrap();
        let result = RunParamsBuilder::new()
            .with_data(temp_dir.path().to_path_buf())
            .with_preset(Some("invalid".to_string()))
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid preset"));
    }

    #[test]
    fn test_run_params_valid_presets() {
        let temp_dir = TempDir::new().unwrap();
        let valid_presets = ["default", "production", "research", "fast"];

        for preset in valid_presets {
            let params = RunParamsBuilder::new()
                .with_data(temp_dir.path().to_path_buf())
                .with_preset(Some(preset.to_string()))
                .build()
                .unwrap();

            assert_eq!(params.preset, Some(preset.to_string()));
        }
    }

    // ==================== PresetsParams Tests ====================

    #[test]
    fn test_presets_params_builder() {
        let params = PresetsParamsBuilder::new().build().unwrap();
        // PresetsParams has no fields, just verify it builds
        let _ = params;
    }

    // ==================== StagesParams Tests ====================

    #[test]
    fn test_stages_params_builder() {
        let params = StagesParamsBuilder::new().build().unwrap();
        // StagesParams has no fields, just verify it builds
        let _ = params;
    }

    // ==================== StatusParams Tests ====================

    #[test]
    fn test_status_params_builder_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let results_path = temp_dir.path().to_path_buf();

        let params = StatusParamsBuilder::new()
            .with_results(results_path.clone())
            .build()
            .unwrap();

        assert_eq!(params.results, results_path);
        assert_eq!(params.last, 10);
    }

    #[test]
    fn test_status_params_builder_all_fields() {
        let temp_dir = TempDir::new().unwrap();
        let results_path = temp_dir.path().to_path_buf();

        let params = StatusParamsBuilder::new()
            .with_results(results_path.clone())
            .with_last(20)
            .build()
            .unwrap();

        assert_eq!(params.results, results_path);
        assert_eq!(params.last, 20);
    }

    #[test]
    fn test_status_params_invalid_last_zero() {
        let temp_dir = TempDir::new().unwrap();
        let result = StatusParamsBuilder::new()
            .with_results(temp_dir.path().to_path_buf())
            .with_last(0)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("last must be greater than 0"));
    }

    #[test]
    fn test_status_params_invalid_last_too_large() {
        let temp_dir = TempDir::new().unwrap();
        let result = StatusParamsBuilder::new()
            .with_results(temp_dir.path().to_path_buf())
            .with_last(1001)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("last too large"));
    }

    // ==================== ShowParams Tests ====================

    #[test]
    fn test_show_params_builder_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let results_path = temp_dir.path().to_path_buf();

        let params = ShowParamsBuilder::new()
            .with_results(results_path.clone())
            .with_run_id("test-run-id".to_string())
            .build()
            .unwrap();

        assert_eq!(params.results, results_path);
        assert_eq!(params.run_id, "test-run-id");
        assert!(!params.json);
        assert!(!params.verbose);
    }

    #[test]
    fn test_show_params_builder_all_fields() {
        let temp_dir = TempDir::new().unwrap();
        let results_path = temp_dir.path().to_path_buf();

        let params = ShowParamsBuilder::new()
            .with_results(results_path.clone())
            .with_run_id("test-run-id".to_string())
            .with_json(true)
            .with_verbose(true)
            .build()
            .unwrap();

        assert_eq!(params.results, results_path);
        assert_eq!(params.run_id, "test-run-id");
        assert!(params.json);
        assert!(params.verbose);
    }

    #[test]
    fn test_show_params_missing_run_id() {
        let temp_dir = TempDir::new().unwrap();
        let result = ShowParamsBuilder::new()
            .with_results(temp_dir.path().to_path_buf())
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("run_id is required"));
    }

    #[test]
    fn test_show_params_empty_run_id() {
        let temp_dir = TempDir::new().unwrap();
        let result = ShowParamsBuilder::new()
            .with_results(temp_dir.path().to_path_buf())
            .with_run_id("".to_string())
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("run_id cannot be empty"));
    }

    #[test]
    fn test_show_params_run_id_too_long() {
        let temp_dir = TempDir::new().unwrap();
        let long_id = "a".repeat(201);
        let result = ShowParamsBuilder::new()
            .with_results(temp_dir.path().to_path_buf())
            .with_run_id(long_id)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("run_id too long"));
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn test_run_params_serialize() {
        let temp_dir = TempDir::new().unwrap();
        let params = RunParams {
            config: Some(temp_dir.path().join("config.json")),
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

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: RunParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.name, deserialized.name);
        assert_eq!(params.preset, deserialized.preset);
    }

    #[test]
    fn test_status_params_serialize() {
        let temp_dir = TempDir::new().unwrap();
        let params = StatusParams {
            results: temp_dir.path().to_path_buf(),
            last: 20,
        };

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: StatusParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.last, deserialized.last);
    }

    #[test]
    fn test_show_params_serialize() {
        let temp_dir = TempDir::new().unwrap();
        let params = ShowParams {
            results: temp_dir.path().to_path_buf(),
            run_id: "test-id".to_string(),
            json: true,
            verbose: true,
        };

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: ShowParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.run_id, deserialized.run_id);
        assert_eq!(params.json, deserialized.json);
        assert_eq!(params.verbose, deserialized.verbose);
    }
}

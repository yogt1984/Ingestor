//! Research Command Parameters
//!
//! This module defines parameter structs and builders for all research commands.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};

/// Parameters for the `run` command (research analysis on historical data)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunParams {
    /// Path to data directory containing Parquet feature files
    pub data: PathBuf,
    /// Path to output directory for research state
    pub output: PathBuf,
    /// Trading symbol (e.g., BTCUSDT)
    pub symbol: String,
    /// Start date for filtering (YYYY-MM-DD)
    pub start: Option<String>,
    /// End date for filtering (YYYY-MM-DD)
    pub end: Option<String>,
    /// Minimum samples before engine is considered ready
    pub min_samples: usize,
    /// Checkpoint interval (number of samples between saves)
    pub checkpoint_interval: usize,
    /// Resume from previous state if available
    pub resume: bool,
    /// Quiet mode (disable progress bar)
    pub quiet: bool,
    /// Output results as JSON
    pub json: bool,
}

/// Builder for `RunParams` with validation
pub struct RunParamsBuilder {
    data: Option<PathBuf>,
    output: Option<PathBuf>,
    symbol: Option<String>,
    start: Option<String>,
    end: Option<String>,
    min_samples: Option<usize>,
    checkpoint_interval: Option<usize>,
    resume: Option<bool>,
    quiet: Option<bool>,
    json: Option<bool>,
}

impl RunParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data: None,
            output: None,
            symbol: None,
            start: None,
            end: None,
            min_samples: None,
            checkpoint_interval: None,
            resume: None,
            quiet: None,
            json: None,
        }
    }

    /// Set data directory path
    pub fn with_data(mut self, data: PathBuf) -> Self {
        self.data = Some(data);
        self
    }

    /// Set output directory path
    pub fn with_output(mut self, output: PathBuf) -> Self {
        self.output = Some(output);
        self
    }

    /// Set trading symbol
    pub fn with_symbol(mut self, symbol: String) -> Self {
        self.symbol = Some(symbol);
        self
    }

    /// Set start date (YYYY-MM-DD)
    pub fn with_start(mut self, start: Option<String>) -> Self {
        self.start = start;
        self
    }

    /// Set end date (YYYY-MM-DD)
    pub fn with_end(mut self, end: Option<String>) -> Self {
        self.end = end;
        self
    }

    /// Set minimum samples
    pub fn with_min_samples(mut self, min_samples: usize) -> Self {
        self.min_samples = Some(min_samples);
        self
    }

    /// Set checkpoint interval
    pub fn with_checkpoint_interval(mut self, checkpoint_interval: usize) -> Self {
        self.checkpoint_interval = Some(checkpoint_interval);
        self
    }

    /// Set resume flag
    pub fn with_resume(mut self, resume: bool) -> Self {
        self.resume = Some(resume);
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

    /// Build `RunParams` with validation
    pub fn build(self) -> Result<RunParams> {
        let data = self.data
            .ok_or_else(|| anyhow::anyhow!("data directory is required"))?;
        
        let output = self.output
            .unwrap_or_else(|| PathBuf::from("./research"));
        
        let symbol = self.symbol
            .unwrap_or_else(|| "BTCUSDT".to_string());

        // Validate symbol
        if symbol.is_empty() {
            anyhow::bail!("Symbol cannot be empty");
        }
        if symbol.len() > 20 {
            anyhow::bail!("Symbol too long: {} (max 20 characters)", symbol);
        }

        // Validate dates if provided
        if let (Some(ref start), Some(ref end)) = (&self.start, &self.end) {
            if let (Ok(start_date), Ok(end_date)) = (
                chrono::NaiveDate::parse_from_str(start, "%Y-%m-%d"),
                chrono::NaiveDate::parse_from_str(end, "%Y-%m-%d"),
            ) {
                if start_date > end_date {
                    anyhow::bail!("Start date must be before end date: {} > {}", start, end);
                }
            }
        }

        let min_samples = self.min_samples.unwrap_or(100);
        if min_samples == 0 {
            anyhow::bail!("min_samples must be greater than 0");
        }

        let checkpoint_interval = self.checkpoint_interval.unwrap_or(10000);
        if checkpoint_interval == 0 {
            anyhow::bail!("checkpoint_interval must be greater than 0");
        }

        Ok(RunParams {
            data,
            output,
            symbol,
            start: self.start,
            end: self.end,
            min_samples,
            checkpoint_interval,
            resume: self.resume.unwrap_or(false),
            quiet: self.quiet.unwrap_or(false),
            json: self.json.unwrap_or(false),
        })
    }
}

impl Default for RunParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `status` command (show current research status)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusParams {
    /// Path to research store directory
    pub store: PathBuf,
    /// Trading symbol to query (e.g., BTCUSDT)
    pub symbol: String,
    /// Output results as JSON
    pub json: bool,
    /// Show verbose output with all details
    pub verbose: bool,
    /// Number of top signals to display
    pub top_signals: usize,
}

/// Builder for `StatusParams` with validation
pub struct StatusParamsBuilder {
    store: Option<PathBuf>,
    symbol: Option<String>,
    json: Option<bool>,
    verbose: Option<bool>,
    top_signals: Option<usize>,
}

impl StatusParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            store: None,
            symbol: None,
            json: None,
            verbose: None,
            top_signals: None,
        }
    }

    /// Set store directory path
    pub fn with_store(mut self, store: PathBuf) -> Self {
        self.store = Some(store);
        self
    }

    /// Set trading symbol
    pub fn with_symbol(mut self, symbol: String) -> Self {
        self.symbol = Some(symbol);
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

    /// Set top signals count
    pub fn with_top_signals(mut self, top_signals: usize) -> Self {
        self.top_signals = Some(top_signals);
        self
    }

    /// Build `StatusParams` with validation
    pub fn build(self) -> Result<StatusParams> {
        let store = self.store
            .unwrap_or_else(|| PathBuf::from("./research"));
        
        let symbol = self.symbol
            .unwrap_or_else(|| "BTCUSDT".to_string());

        // Validate symbol
        if symbol.is_empty() {
            anyhow::bail!("Symbol cannot be empty");
        }
        if symbol.len() > 20 {
            anyhow::bail!("Symbol too long: {} (max 20 characters)", symbol);
        }

        let top_signals = self.top_signals.unwrap_or(5);
        if top_signals == 0 {
            anyhow::bail!("top_signals must be greater than 0");
        }
        if top_signals > 100 {
            anyhow::bail!("top_signals too large (max 100): {}", top_signals);
        }

        Ok(StatusParams {
            store,
            symbol,
            json: self.json.unwrap_or(false),
            verbose: self.verbose.unwrap_or(false),
            top_signals,
        })
    }
}

impl Default for StatusParamsBuilder {
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
        assert_eq!(params.output, PathBuf::from("./research"));
        assert_eq!(params.symbol, "BTCUSDT");
        assert_eq!(params.min_samples, 100);
        assert_eq!(params.checkpoint_interval, 10000);
        assert!(!params.resume);
        assert!(!params.quiet);
        assert!(!params.json);
    }

    #[test]
    fn test_run_params_builder_all_fields() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().to_path_buf();
        let output_path = temp_dir.path().join("output").to_path_buf();

        let params = RunParamsBuilder::new()
            .with_data(data_path.clone())
            .with_output(output_path.clone())
            .with_symbol("ETHUSDT".to_string())
            .with_start(Some("2024-01-01".to_string()))
            .with_end(Some("2024-01-31".to_string()))
            .with_min_samples(200)
            .with_checkpoint_interval(5000)
            .with_resume(true)
            .with_quiet(true)
            .with_json(true)
            .build()
            .unwrap();

        assert_eq!(params.data, data_path);
        assert_eq!(params.output, output_path);
        assert_eq!(params.symbol, "ETHUSDT");
        assert_eq!(params.start, Some("2024-01-01".to_string()));
        assert_eq!(params.end, Some("2024-01-31".to_string()));
        assert_eq!(params.min_samples, 200);
        assert_eq!(params.checkpoint_interval, 5000);
        assert!(params.resume);
        assert!(params.quiet);
        assert!(params.json);
    }

    #[test]
    fn test_run_params_missing_data() {
        let result = RunParamsBuilder::new().build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data directory is required"));
    }

    #[test]
    fn test_run_params_invalid_symbol_empty() {
        let temp_dir = TempDir::new().unwrap();
        let result = RunParamsBuilder::new()
            .with_data(temp_dir.path().to_path_buf())
            .with_symbol("".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Symbol cannot be empty"));
    }

    #[test]
    fn test_run_params_invalid_symbol_too_long() {
        let temp_dir = TempDir::new().unwrap();
        let result = RunParamsBuilder::new()
            .with_data(temp_dir.path().to_path_buf())
            .with_symbol("A".repeat(21))
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Symbol too long"));
    }

    #[test]
    fn test_run_params_invalid_date_range() {
        let temp_dir = TempDir::new().unwrap();
        let result = RunParamsBuilder::new()
            .with_data(temp_dir.path().to_path_buf())
            .with_start(Some("2024-01-31".to_string()))
            .with_end(Some("2024-01-01".to_string()))
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Start date must be before end date"));
    }

    #[test]
    fn test_run_params_invalid_min_samples_zero() {
        let temp_dir = TempDir::new().unwrap();
        let result = RunParamsBuilder::new()
            .with_data(temp_dir.path().to_path_buf())
            .with_min_samples(0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("min_samples must be greater than 0"));
    }

    #[test]
    fn test_run_params_invalid_checkpoint_interval_zero() {
        let temp_dir = TempDir::new().unwrap();
        let result = RunParamsBuilder::new()
            .with_data(temp_dir.path().to_path_buf())
            .with_checkpoint_interval(0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checkpoint_interval must be greater than 0"));
    }

    #[test]
    fn test_run_params_valid_date_range() {
        let temp_dir = TempDir::new().unwrap();
        let params = RunParamsBuilder::new()
            .with_data(temp_dir.path().to_path_buf())
            .with_start(Some("2024-01-01".to_string()))
            .with_end(Some("2024-01-31".to_string()))
            .build()
            .unwrap();

        assert_eq!(params.start, Some("2024-01-01".to_string()));
        assert_eq!(params.end, Some("2024-01-31".to_string()));
    }

    // ==================== StatusParams Tests ====================

    #[test]
    fn test_status_params_builder_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let store_path = temp_dir.path().to_path_buf();

        let params = StatusParamsBuilder::new()
            .with_store(store_path.clone())
            .build()
            .unwrap();

        assert_eq!(params.store, store_path);
        assert_eq!(params.symbol, "BTCUSDT");
        assert!(!params.json);
        assert!(!params.verbose);
        assert_eq!(params.top_signals, 5);
    }

    #[test]
    fn test_status_params_builder_all_fields() {
        let temp_dir = TempDir::new().unwrap();
        let store_path = temp_dir.path().to_path_buf();

        let params = StatusParamsBuilder::new()
            .with_store(store_path.clone())
            .with_symbol("ETHUSDT".to_string())
            .with_json(true)
            .with_verbose(true)
            .with_top_signals(10)
            .build()
            .unwrap();

        assert_eq!(params.store, store_path);
        assert_eq!(params.symbol, "ETHUSDT");
        assert!(params.json);
        assert!(params.verbose);
        assert_eq!(params.top_signals, 10);
    }

    #[test]
    fn test_status_params_default_store() {
        let params = StatusParamsBuilder::new()
            .build()
            .unwrap();

        assert_eq!(params.store, PathBuf::from("./research"));
    }

    #[test]
    fn test_status_params_invalid_symbol_empty() {
        let result = StatusParamsBuilder::new()
            .with_symbol("".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Symbol cannot be empty"));
    }

    #[test]
    fn test_status_params_invalid_symbol_too_long() {
        let result = StatusParamsBuilder::new()
            .with_symbol("A".repeat(21))
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Symbol too long"));
    }

    #[test]
    fn test_status_params_invalid_top_signals_zero() {
        let result = StatusParamsBuilder::new()
            .with_top_signals(0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("top_signals must be greater than 0"));
    }

    #[test]
    fn test_status_params_invalid_top_signals_too_large() {
        let result = StatusParamsBuilder::new()
            .with_top_signals(101)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("top_signals too large"));
    }

    #[test]
    fn test_status_params_valid_top_signals_max() {
        let params = StatusParamsBuilder::new()
            .with_top_signals(100)
            .build()
            .unwrap();

        assert_eq!(params.top_signals, 100);
    }

    #[test]
    fn test_run_params_serialize() {
        let temp_dir = TempDir::new().unwrap();
        let params = RunParams {
            data: temp_dir.path().to_path_buf(),
            output: PathBuf::from("./research"),
            symbol: "BTCUSDT".to_string(),
            start: Some("2024-01-01".to_string()),
            end: Some("2024-01-31".to_string()),
            min_samples: 100,
            checkpoint_interval: 10000,
            resume: false,
            quiet: false,
            json: false,
        };

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: RunParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.symbol, deserialized.symbol);
        assert_eq!(params.min_samples, deserialized.min_samples);
    }

    #[test]
    fn test_status_params_serialize() {
        let temp_dir = TempDir::new().unwrap();
        let params = StatusParams {
            store: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: true,
            verbose: true,
            top_signals: 10,
        };

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: StatusParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.symbol, deserialized.symbol);
        assert_eq!(params.top_signals, deserialized.top_signals);
        assert_eq!(params.json, deserialized.json);
        assert_eq!(params.verbose, deserialized.verbose);
    }
}

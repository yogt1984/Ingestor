//! Research Commands
//!
//! This module provides all research-related commands that can be executed
//! from both CLI and TUI interfaces.
//!
//! # Commands
//!
//! - `run` - Run research analysis on historical data
//! - `status` - Show current research status

use std::sync::Arc;
use std::time::Instant;
use anyhow::{Result, Context};
use chrono::{NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::commands::common::{ProgressCallback, ProgressEvent, LogLevel};
use crate::commands::params::research_params::{RunParams, StatusParams};
use crate::backtest::replay::{ParquetReplay, ReplayConfig};
use crate::core::{
    ResearchState, ResearchStore, ResearchStoreConfig, TradeableAssessment,
};
use crate::edge_detection::{
    DefaultResearchEngine, ResearchEngine, ResearchEngineConfig,
};
use crate::edge_detection::traits::SignificantSignal;

/// Result of a research run command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    /// Number of samples processed
    pub samples_processed: usize,
    /// Duration in seconds
    pub duration_seconds: f64,
    /// MIDC kappa value (diffusion rate)
    pub midc_kappa: f64,
    /// MIDC confidence level (0.0 to 1.0)
    pub midc_confidence: f64,
    /// MIDC regime classification
    pub midc_regime: String,
    /// Mean persistence duration in seconds
    pub persistence_mean_seconds: f64,
    /// Number of persistence samples observed
    pub persistence_sample_count: usize,
    /// Top conditional signals
    pub top_signals: Vec<SignalSummary>,
    /// Whether market is tradeable
    pub is_tradeable: bool,
    /// Reason for tradeable assessment
    pub tradeable_reason: String,
    /// Number of checkpoints saved
    pub checkpoints_saved: usize,
}

/// Summary of a conditional signal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalSummary {
    /// Signal signature (feature combination)
    pub signature: String,
    /// Probability of continuation
    pub p_continuation: f64,
    /// Number of samples observed
    pub sample_count: usize,
    /// Lower bound of confidence interval
    pub confidence_lower: f64,
    /// Upper bound of confidence interval
    pub confidence_upper: f64,
}

impl From<&SignificantSignal> for SignalSummary {
    fn from(sig: &SignificantSignal) -> Self {
        Self {
            signature: sig.signature_key.clone(),
            p_continuation: sig.probability.p_continuation,
            sample_count: sig.probability.sample_count,
            confidence_lower: sig.probability.confidence_interval.0,
            confidence_upper: sig.probability.confidence_interval.1,
        }
    }
}

/// Result of a research status command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResult {
    /// Trading symbol
    pub symbol: String,
    /// State ID
    pub state_id: String,
    /// Timestamp when state was created
    pub timestamp: String,
    /// Data start timestamp (if available)
    pub data_start: Option<String>,
    /// Data end timestamp (if available)
    pub data_end: Option<String>,
    /// MIDC kappa value
    pub midc_kappa: f64,
    /// MIDC confidence level
    pub midc_confidence: f64,
    /// MIDC tau-half in seconds
    pub midc_tau_half_seconds: f64,
    /// MIDC regime classification
    pub midc_regime: String,
    /// MIDC interpretation text
    pub midc_interpretation: String,
    /// Mean persistence duration in seconds
    pub persistence_mean_seconds: f64,
    /// Median persistence duration in seconds
    pub persistence_median_seconds: f64,
    /// Number of persistence samples
    pub persistence_sample_count: usize,
    /// Whether persistence data is reliable
    pub persistence_reliable: bool,
    /// Current entropy value
    pub entropy: f64,
    /// Top conditional signals
    pub top_signals: Vec<StatusSignal>,
    /// Total number of signals in table
    pub total_signals: usize,
    /// Tradeable assessment
    pub assessment: StatusAssessment,
}

/// Signal information in status result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSignal {
    /// Signal signature
    pub signature: String,
    /// Probability of continuation
    pub p_continuation: f64,
    /// Number of samples
    pub sample_count: usize,
    /// Edge (p_continuation - 0.5)
    pub edge: f64,
    /// Lower bound of confidence interval
    pub confidence_lower: f64,
    /// Upper bound of confidence interval
    pub confidence_upper: f64,
}

/// Tradeable assessment information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusAssessment {
    /// MIDC condition met
    pub midc_ok: bool,
    /// Entropy condition met
    pub entropy_ok: bool,
    /// Persistence condition met
    pub persistence_ok: bool,
    /// Signals condition met
    pub signals_ok: bool,
    /// Overall tradeable status
    pub is_tradeable: bool,
    /// Recommended strategy
    pub recommended_strategy: String,
    /// Position scale factor (0.0 to 1.0)
    pub position_scale: f64,
    /// Reasoning for assessment
    pub reasoning: String,
}

impl From<&TradeableAssessment> for StatusAssessment {
    fn from(a: &TradeableAssessment) -> Self {
        Self {
            midc_ok: a.midc_ok,
            entropy_ok: a.entropy_ok,
            persistence_ok: a.persistence_ok,
            signals_ok: a.signals_ok,
            is_tradeable: a.is_tradeable,
            recommended_strategy: format!("{:?}", a.recommended_strategy),
            position_scale: a.position_scale,
            reasoning: a.reasoning.clone(),
        }
    }
}

/// Research command executor
///
/// All research commands are executed through this struct.
/// Commands support progress callbacks for long-running operations.
pub struct ResearchCommands;

impl ResearchCommands {
    /// Run research analysis on historical data
    ///
    /// This command processes historical feature data to build a research state
    /// containing MIDC estimates, persistence statistics, and conditional signals.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for the research run
    /// * `callback` - Progress callback for updates during execution
    ///
    /// # Returns
    ///
    /// Research run result containing analysis results
    pub fn run(
        params: RunParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<RunResult> {
        let start_time = Instant::now();

        callback.on_event(ProgressEvent::Started {
            total: None,
            message: format!("Starting research analysis for symbol: {}", params.symbol),
        });

        // Validate inputs
        Self::validate_run_params(&params)?;

        // Parse date range if provided
        let start_time_ms = params.start.as_ref()
            .and_then(|s| Self::parse_date_to_millis(s));
        let end_time_ms = params.end.as_ref()
            .and_then(|s| Self::parse_date_to_millis(s));

        // Validate date range
        if let (Some(start), Some(end)) = (start_time_ms, end_time_ms) {
            if start > end {
                anyhow::bail!("Start date must be before end date");
            }
        }

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loading data from: {:?}", params.data),
        });

        // Setup replay engine
        let replay_config = ReplayConfig {
            data_dir: params.data.clone(),
            start_time: start_time_ms,
            end_time: end_time_ms,
            speed: 0.0, // As fast as possible
        };

        let mut replay = ParquetReplay::new(replay_config);
        let event_count = replay.load().context("Failed to load Parquet data")?;

        if event_count == 0 {
            anyhow::bail!("No events found in data directory");
        }

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loaded {} events", event_count),
        });

        // Setup research store
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Initializing research store: {:?}", params.output),
        });

        let store_config = ResearchStoreConfig::with_path(&params.output);
        let store = ResearchStore::new(store_config)
            .context("Failed to create research store")?;

        // Setup research engine
        let engine_config = ResearchEngineConfig::new(&params.symbol)
            .with_min_samples(params.min_samples)
            .with_checkpoint_interval(params.checkpoint_interval);

        callback.on_event(ProgressEvent::Progress {
            current: 0,
            total: Some(event_count),
            message: format!("Initializing research engine (resume={})", params.resume),
        });

        let mut engine = if params.resume {
            DefaultResearchEngine::load_or_init(engine_config, store)
                .context("Failed to load or init research engine")?
        } else {
            DefaultResearchEngine::new(engine_config, Some(store))
                .context("Failed to create research engine")?
        };

        // Process events with progress updates
        let mut processed = 0;
        while let Some(event) = replay.next() {
            if let Err(e) = engine.on_features(&event.snapshot) {
                callback.on_event(ProgressEvent::Log {
                    level: LogLevel::Warn,
                    message: format!("Error processing snapshot: {}", e),
                });
            }
            processed += 1;

            // Update progress every 1000 events
            if processed % 1000 == 0 || processed == event_count {
                let stats = engine.stats();
                callback.on_event(ProgressEvent::Progress {
                    current: processed,
                    total: Some(event_count),
                    message: format!(
                        "MIDC: {:.4}, Signals: {}",
                        engine.state().midc.kappa,
                        stats.conditional_updates
                    ),
                });

                callback.on_event(ProgressEvent::Metric {
                    name: "midc_kappa".to_string(),
                    value: engine.state().midc.kappa,
                });
            }
        }

        // Final checkpoint
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Saving final checkpoint...".to_string(),
        });

        engine.checkpoint().context("Failed to save final checkpoint")?;

        // Gather results
        let state = engine.state();
        let stats = engine.stats();
        let assessment = engine.assess();

        let top_signals: Vec<SignalSummary> = engine
            .significant_signals()
            .iter()
            .take(10)
            .map(SignalSummary::from)
            .collect();

        let tradeable_reason = if assessment.is_tradeable {
            "All conditions met".to_string()
        } else {
            let mut reasons = Vec::new();
            if !assessment.midc_ok {
                reasons.push("MIDC out of range");
            }
            if !assessment.persistence_ok {
                reasons.push("Insufficient persistence data");
            }
            if !assessment.entropy_ok {
                reasons.push("Entropy too high");
            }
            if !assessment.signals_ok {
                reasons.push("Low signal confidence");
            }
            reasons.join(", ")
        };

        let duration_seconds = start_time.elapsed().as_secs_f64();

        let result = RunResult {
            samples_processed: stats.samples_processed,
            duration_seconds,
            midc_kappa: state.midc.kappa,
            midc_confidence: state.midc.confidence,
            midc_regime: format!("{:?}", state.midc.regime()),
            persistence_mean_seconds: state.persistence.mean_duration_seconds,
            persistence_sample_count: state.persistence.sample_count,
            top_signals,
            is_tradeable: assessment.is_tradeable,
            tradeable_reason,
            checkpoints_saved: stats.checkpoints,
        };

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Research analysis completed: {} samples processed in {:.2}s, Tradeable: {}",
                result.samples_processed,
                result.duration_seconds,
                result.is_tradeable
            ),
        });

        Ok(result)
    }

    /// Show current research status
    ///
    /// This command displays the current research state for a given symbol,
    /// including MIDC estimates, persistence statistics, and top signals.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for the status query
    /// * `callback` - Progress callback (not heavily used for this quick query)
    ///
    /// # Returns
    ///
    /// Status result containing current research state
    pub fn status(
        params: StatusParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<StatusResult> {
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: format!("Loading research status for symbol: {}", params.symbol),
        });

        // Validate configuration
        Self::validate_status_params(&params)?;

        // Open research store
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Opening research store: {:?}", params.store),
        });

        let store_config = ResearchStoreConfig::with_path(&params.store);
        let mut store = ResearchStore::new(store_config)
            .context("Failed to open research store")?;

        // Load latest state for symbol
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loading state for symbol: {}", params.symbol),
        });

        let state = store
            .load(&params.symbol)
            .context("Failed to load research state")?
            .ok_or_else(|| anyhow::anyhow!("No research state found for symbol: {}", params.symbol))?;

        // Build result
        let result = Self::build_status_result(&state, params.top_signals)?;

        callback.on_event(ProgressEvent::Completed {
            message: format!(
                "Status loaded: {} signals found, Tradeable: {}",
                result.total_signals,
                result.assessment.is_tradeable
            ),
        });

        Ok(result)
    }

    // ==================== Private Helper Functions ====================

    /// Validate run parameters
    fn validate_run_params(params: &RunParams) -> Result<()> {
        // Check data directory exists
        if !params.data.exists() {
            anyhow::bail!("Data directory does not exist: {:?}", params.data);
        }

        // Check symbol is valid (already validated in builder, but double-check)
        if params.symbol.is_empty() {
            anyhow::bail!("Symbol cannot be empty");
        }
        if params.symbol.len() > 20 {
            anyhow::bail!("Symbol too long: {}", params.symbol);
        }

        // Check min_samples is reasonable
        if params.min_samples == 0 {
            anyhow::bail!("min_samples must be greater than 0");
        }

        // Check checkpoint_interval is reasonable
        if params.checkpoint_interval == 0 {
            anyhow::bail!("checkpoint_interval must be greater than 0");
        }

        Ok(())
    }

    /// Validate status parameters
    fn validate_status_params(params: &StatusParams) -> Result<()> {
        // Check store directory exists
        if !params.store.exists() {
            anyhow::bail!("Research store directory does not exist: {:?}", params.store);
        }

        // Check symbol is valid (already validated in builder, but double-check)
        if params.symbol.is_empty() {
            anyhow::bail!("Symbol cannot be empty");
        }
        if params.symbol.len() > 20 {
            anyhow::bail!("Symbol too long: {}", params.symbol);
        }

        // Check top_signals is reasonable
        if params.top_signals == 0 {
            anyhow::bail!("top_signals must be greater than 0");
        }
        if params.top_signals > 100 {
            anyhow::bail!("top_signals too large (max 100): {}", params.top_signals);
        }

        Ok(())
    }

    /// Parse a date string (YYYY-MM-DD) to milliseconds since epoch
    fn parse_date_to_millis(date_str: &str) -> Option<i64> {
        NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
            .ok()
            .map(|d| {
                let dt = d.and_hms_opt(0, 0, 0).unwrap_or_default();
                Utc.from_utc_datetime(&dt).timestamp_millis()
            })
    }

    /// Build status result from research state
    fn build_status_result(state: &ResearchState, top_n: usize) -> Result<StatusResult> {
        let midc = &state.midc;
        let persistence = &state.persistence;
        let assessment = &state.assessment;

        // Get top signals sorted by edge magnitude
        let mut signals: Vec<_> = state
            .conditional_table
            .iter()
            .filter(|(_, p)| p.sample_count >= 10)
            .map(|(key, p)| {
                StatusSignal {
                    signature: key.clone(),
                    p_continuation: p.p_continuation,
                    sample_count: p.sample_count,
                    edge: p.p_continuation - 0.5,
                    confidence_lower: p.confidence_interval.0,
                    confidence_upper: p.confidence_interval.1,
                }
            })
            .collect();

        // Sort by absolute edge (highest first)
        signals.sort_by(|a, b| {
            b.edge.abs().partial_cmp(&a.edge.abs()).unwrap_or(std::cmp::Ordering::Equal)
        });

        let total_signals = signals.len();
        let top_signals: Vec<_> = signals.into_iter().take(top_n).collect();

        Ok(StatusResult {
            symbol: state.symbol.clone(),
            state_id: state.id.clone(),
            timestamp: state.timestamp.to_rfc3339(),
            data_start: state.data_start.map(|dt| dt.to_rfc3339()),
            data_end: state.data_end.map(|dt| dt.to_rfc3339()),
            midc_kappa: midc.kappa,
            midc_confidence: midc.confidence,
            midc_tau_half_seconds: midc.tau_half_seconds,
            midc_regime: format!("{:?}", midc.regime()),
            midc_interpretation: Self::interpret_midc(midc.kappa).to_string(),
            persistence_mean_seconds: persistence.mean_duration_seconds,
            persistence_median_seconds: persistence.median_duration_seconds,
            persistence_sample_count: persistence.sample_count,
            persistence_reliable: persistence.is_reliable(),
            entropy: state.entropy,
            top_signals,
            total_signals,
            assessment: StatusAssessment::from(assessment),
        })
    }

    /// Interpret MIDC kappa value
    fn interpret_midc(kappa: f64) -> &'static str {
        if kappa < 0.01 {
            "Very efficient (strong mean-reversion)"
        } else if kappa < 0.05 {
            "Efficient (moderate mean-reversion)"
        } else if kappa < 0.15 {
            "Semi-efficient (weak trends possible)"
        } else if kappa < 0.30 {
            "Inefficient (trending markets)"
        } else {
            "Highly inefficient (strong trends)"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::commands::common::NoOpCallback;

    // ==================== Parameter Validation Tests ====================

    #[test]
    fn test_validate_run_params_success() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().to_path_buf();

        let params = RunParams {
            data: data_path,
            output: std::path::PathBuf::from("./research"),
            symbol: "BTCUSDT".to_string(),
            start: None,
            end: None,
            min_samples: 100,
            checkpoint_interval: 10000,
            resume: false,
            quiet: false,
            json: false,
        };

        // Should not panic for valid params (though will fail on missing data files)
        let _ = ResearchCommands::validate_run_params(&params);
    }

    #[test]
    fn test_validate_run_params_missing_data_dir() {
        let params = RunParams {
            data: std::path::PathBuf::from("/nonexistent/path"),
            output: std::path::PathBuf::from("./research"),
            symbol: "BTCUSDT".to_string(),
            start: None,
            end: None,
            min_samples: 100,
            checkpoint_interval: 10000,
            resume: false,
            quiet: false,
            json: false,
        };

        let result = ResearchCommands::validate_run_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Data directory does not exist"));
    }

    #[test]
    fn test_validate_run_params_invalid_symbol_empty() {
        let temp_dir = TempDir::new().unwrap();
        let params = RunParams {
            data: temp_dir.path().to_path_buf(),
            output: std::path::PathBuf::from("./research"),
            symbol: "".to_string(),
            start: None,
            end: None,
            min_samples: 100,
            checkpoint_interval: 10000,
            resume: false,
            quiet: false,
            json: false,
        };

        let result = ResearchCommands::validate_run_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Symbol cannot be empty"));
    }

    #[test]
    fn test_validate_run_params_invalid_min_samples() {
        let temp_dir = TempDir::new().unwrap();
        let params = RunParams {
            data: temp_dir.path().to_path_buf(),
            output: std::path::PathBuf::from("./research"),
            symbol: "BTCUSDT".to_string(),
            start: None,
            end: None,
            min_samples: 0,
            checkpoint_interval: 10000,
            resume: false,
            quiet: false,
            json: false,
        };

        let result = ResearchCommands::validate_run_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("min_samples must be greater than 0"));
    }

    #[test]
    fn test_validate_status_params_success() {
        let temp_dir = TempDir::new().unwrap();
        let store_path = temp_dir.path().to_path_buf();

        let params = StatusParams {
            store: store_path,
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 5,
        };

        // Should not panic for valid params (though will fail on missing store)
        let _ = ResearchCommands::validate_status_params(&params);
    }

    #[test]
    fn test_validate_status_params_missing_store() {
        let params = StatusParams {
            store: std::path::PathBuf::from("/nonexistent/store"),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 5,
        };

        let result = ResearchCommands::validate_status_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Research store directory does not exist"));
    }

    #[test]
    fn test_validate_status_params_invalid_top_signals() {
        let temp_dir = TempDir::new().unwrap();
        let params = StatusParams {
            store: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 0,
        };

        let result = ResearchCommands::validate_status_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("top_signals must be greater than 0"));
    }

    #[test]
    fn test_validate_status_params_top_signals_too_large() {
        let temp_dir = TempDir::new().unwrap();
        let params = StatusParams {
            store: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 101,
        };

        let result = ResearchCommands::validate_status_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("top_signals too large"));
    }

    // ==================== Date Parsing Tests ====================

    #[test]
    fn test_parse_date_to_millis_valid() {
        let millis = ResearchCommands::parse_date_to_millis("2024-01-15");
        assert!(millis.is_some());
        assert!(millis.unwrap() > 0);
    }

    #[test]
    fn test_parse_date_to_millis_invalid_format() {
        let millis = ResearchCommands::parse_date_to_millis("2024/01/15");
        assert!(millis.is_none());
    }

    #[test]
    fn test_parse_date_to_millis_invalid_date() {
        let millis = ResearchCommands::parse_date_to_millis("2024-13-45");
        assert!(millis.is_none());
    }

    // ==================== MIDC Interpretation Tests ====================

    #[test]
    fn test_interpret_midc_very_efficient() {
        assert_eq!(
            ResearchCommands::interpret_midc(0.005),
            "Very efficient (strong mean-reversion)"
        );
    }

    #[test]
    fn test_interpret_midc_efficient() {
        assert_eq!(
            ResearchCommands::interpret_midc(0.03),
            "Efficient (moderate mean-reversion)"
        );
    }

    #[test]
    fn test_interpret_midc_semi_efficient() {
        assert_eq!(
            ResearchCommands::interpret_midc(0.10),
            "Semi-efficient (weak trends possible)"
        );
    }

    #[test]
    fn test_interpret_midc_inefficient() {
        assert_eq!(
            ResearchCommands::interpret_midc(0.20),
            "Inefficient (trending markets)"
        );
    }

    #[test]
    fn test_interpret_midc_highly_inefficient() {
        assert_eq!(
            ResearchCommands::interpret_midc(0.50),
            "Highly inefficient (strong trends)"
        );
    }

    // ==================== SignalSummary Tests ====================

    #[test]
    fn test_signal_summary_from_significant_signal() {
        use crate::core::ConditionalProbability;
        use crate::edge_detection::traits::SignificantSignal;
        
        let prob = ConditionalProbability {
            p_continuation: 0.65,
            p_reversal: 0.35,
            expected_magnitude_bps: 5.0,
            std_magnitude_bps: 2.0,
            sample_count: 100,
            confidence_interval: (0.55, 0.75),
        };

        let sig = SignificantSignal {
            signature_key: "test_signal".to_string(),
            probability: prob,
            edge: 0.15,
        };

        let summary = SignalSummary::from(&sig);
        assert_eq!(summary.signature, "test_signal");
        assert_eq!(summary.p_continuation, 0.65);
        assert_eq!(summary.sample_count, 100);
        assert_eq!(summary.confidence_lower, 0.55);
        assert_eq!(summary.confidence_upper, 0.75);
    }

    // ==================== StatusAssessment Tests ====================

    #[test]
    fn test_status_assessment_from_tradeable_assessment() {
        use crate::core::{RecommendedStrategy, TradeableAssessment};
        use chrono::Utc;
        
        let assessment = TradeableAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: RecommendedStrategy::MarketMaking,
            position_scale: 0.8,
            reasoning: "All conditions met".to_string(),
            assessed_at: Utc::now(),
        };

        let status = StatusAssessment::from(&assessment);
        assert!(status.midc_ok);
        assert!(status.is_tradeable);
        assert_eq!(status.position_scale, 0.8);
        assert_eq!(status.reasoning, "All conditions met");
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn test_run_result_serialize() {
        let result = RunResult {
            samples_processed: 1000,
            duration_seconds: 10.5,
            midc_kappa: 0.05,
            midc_confidence: 0.95,
            midc_regime: "ModerateDiffusion".to_string(),
            persistence_mean_seconds: 5.2,
            persistence_sample_count: 50,
            top_signals: vec![],
            is_tradeable: true,
            tradeable_reason: "All conditions met".to_string(),
            checkpoints_saved: 2,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: RunResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.samples_processed, deserialized.samples_processed);
        assert_eq!(result.midc_kappa, deserialized.midc_kappa);
        assert_eq!(result.is_tradeable, deserialized.is_tradeable);
    }

    #[test]
    fn test_status_result_serialize() {
        let result = StatusResult {
            symbol: "BTCUSDT".to_string(),
            state_id: "test-id".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            data_start: None,
            data_end: None,
            midc_kappa: 0.05,
            midc_confidence: 0.95,
            midc_tau_half_seconds: 13.86,
            midc_regime: "ModerateDiffusion".to_string(),
            midc_interpretation: "Efficient".to_string(),
            persistence_mean_seconds: 5.2,
            persistence_median_seconds: 4.8,
            persistence_sample_count: 50,
            persistence_reliable: true,
            entropy: 0.45,
            top_signals: vec![],
            total_signals: 10,
            assessment: StatusAssessment {
                midc_ok: true,
                entropy_ok: true,
                persistence_ok: true,
                signals_ok: true,
                is_tradeable: true,
                recommended_strategy: "MarketMaking".to_string(),
                position_scale: 0.8,
                reasoning: "All conditions met".to_string(),
            },
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: StatusResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.symbol, deserialized.symbol);
        assert_eq!(result.midc_kappa, deserialized.midc_kappa);
        assert_eq!(result.assessment.is_tradeable, deserialized.assessment.is_tradeable);
    }
}

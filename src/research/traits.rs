//! Research Engine Traits - Task 1.0
//!
//! Core trait definitions for pluggable research implementations.
//! The ResearchEngine trait defines the interface for continuous research
//! processes that detect mutual information between past features and future price.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                        RESEARCH ENGINE TRAIT                                 │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                             │
//! │  FeaturesSnapshot ──────────────────────────────────────────────────────┐   │
//! │  └── Input: Real-time market microstructure features                    │   │
//! │                           │                                             │   │
//! │                           ▼ on_features()                               │   │
//! │  ResearchEngine ────────────────────────────────────────────────────────┤   │
//! │  └── Processes features, updates internal state                         │   │
//! │                           │                                             │   │
//! │                           ▼ assess()                                    │   │
//! │  TradeableAssessment ───────────────────────────────────────────────────┘   │
//! │  └── Output: Is I(Past; Future) > 0 right now?                              │
//! │                                                                             │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example Implementation
//!
//! ```rust,ignore
//! use ingestor::research::{ResearchEngine, ResearchEngineConfig, ResearchError};
//! use ingestor::features::FeaturesSnapshot;
//! use ingestor::framework::{ResearchState, TradeableAssessment, AlgorithmConfig};
//!
//! struct MyResearchEngine {
//!     state: ResearchState,
//!     config: ResearchEngineConfig,
//! }
//!
//! impl ResearchEngine for MyResearchEngine {
//!     fn on_features(&mut self, snapshot: &FeaturesSnapshot) -> Result<(), ResearchError> {
//!         // Update internal state with new features
//!         Ok(())
//!     }
//!
//!     fn assess(&self) -> TradeableAssessment {
//!         self.state.assessment.clone()
//!     }
//!
//!     fn generate_config(&self) -> Option<AlgorithmConfig> {
//!         if self.state.assessment.is_tradeable {
//!             Some(AlgorithmConfig::from_research(&self.state))
//!         } else {
//!             None
//!         }
//!     }
//!
//!     fn state(&self) -> &ResearchState {
//!         &self.state
//!     }
//!
//!     fn checkpoint(&mut self) -> Result<(), ResearchError> {
//!         // Save state to ResearchStore
//!         Ok(())
//!     }
//! }
//! ```

use crate::features::FeaturesSnapshot;
use crate::framework::{
    AlgorithmConfig, ResearchState, ResearchStore, TradeableAssessment,
    MIDCEstimate, PersistenceStats, ConditionalProbability,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during research operations
#[derive(Debug, Error)]
pub enum ResearchError {
    /// Error during state persistence
    #[error("Persistence error: {0}")]
    Persistence(String),

    /// Error during feature processing
    #[error("Feature processing error: {0}")]
    FeatureProcessing(String),

    /// Error during MIDC estimation
    #[error("MIDC estimation error: {0}")]
    MIDCEstimation(String),

    /// Error during persistence analysis
    #[error("Persistence analysis error: {0}")]
    PersistenceAnalysis(String),

    /// Error during conditional model update
    #[error("Conditional model error: {0}")]
    ConditionalModel(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Insufficient data for analysis
    #[error("Insufficient data: {message}, required: {required}, available: {available}")]
    InsufficientData {
        message: String,
        required: usize,
        available: usize,
    },

    /// Store not available
    #[error("Store not available: {0}")]
    StoreUnavailable(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the research engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchEngineConfig {
    /// Symbol being researched
    pub symbol: String,

    /// Minimum samples required before producing assessments
    pub min_samples: usize,

    /// Checkpoint interval in number of features processed
    pub checkpoint_interval: usize,

    /// MIDC configuration
    pub midc_config: MIDCConfig,

    /// Persistence analyzer configuration
    pub persistence_config: PersistenceConfig,

    /// Conditional model configuration
    pub conditional_config: ConditionalConfig,

    /// Assessment thresholds
    pub assessment_thresholds: AssessmentThresholds,

    /// Whether to enable auto-checkpointing
    pub auto_checkpoint: bool,

    /// Engine version for compatibility tracking
    pub engine_version: String,
}

impl Default for ResearchEngineConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            min_samples: 100,
            checkpoint_interval: 1000,
            midc_config: MIDCConfig::default(),
            persistence_config: PersistenceConfig::default(),
            conditional_config: ConditionalConfig::default(),
            assessment_thresholds: AssessmentThresholds::default(),
            auto_checkpoint: true,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

impl ResearchEngineConfig {
    /// Create a new config for a specific symbol
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            ..Default::default()
        }
    }

    /// Set minimum samples required
    pub fn with_min_samples(mut self, min_samples: usize) -> Self {
        self.min_samples = min_samples;
        self
    }

    /// Set checkpoint interval
    pub fn with_checkpoint_interval(mut self, interval: usize) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Disable auto-checkpointing
    pub fn without_auto_checkpoint(mut self) -> Self {
        self.auto_checkpoint = false;
        self
    }

    /// Set MIDC configuration
    pub fn with_midc_config(mut self, config: MIDCConfig) -> Self {
        self.midc_config = config;
        self
    }

    /// Set persistence configuration
    pub fn with_persistence_config(mut self, config: PersistenceConfig) -> Self {
        self.persistence_config = config;
        self
    }

    /// Set conditional model configuration
    pub fn with_conditional_config(mut self, config: ConditionalConfig) -> Self {
        self.conditional_config = config;
        self
    }

    /// Set assessment thresholds
    pub fn with_assessment_thresholds(mut self, thresholds: AssessmentThresholds) -> Self {
        self.assessment_thresholds = thresholds;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.symbol.is_empty() {
            return Err(ResearchError::Configuration("Symbol cannot be empty".to_string()));
        }
        if self.min_samples == 0 {
            return Err(ResearchError::Configuration("min_samples must be > 0".to_string()));
        }
        if self.checkpoint_interval == 0 {
            return Err(ResearchError::Configuration("checkpoint_interval must be > 0".to_string()));
        }
        self.midc_config.validate()?;
        self.persistence_config.validate()?;
        self.conditional_config.validate()?;
        self.assessment_thresholds.validate()?;
        Ok(())
    }
}

/// MIDC estimation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MIDCConfig {
    /// Rolling window size for return calculation (in data points)
    pub rolling_window: usize,

    /// Time scales for autocorrelation calculation (in seconds)
    pub time_scales: Vec<f64>,

    /// Minimum R-squared for valid fit
    pub min_r_squared: f64,

    /// Maximum kappa for valid estimate
    pub max_kappa: f64,

    /// Update frequency (every N samples)
    pub update_frequency: usize,
}

impl Default for MIDCConfig {
    fn default() -> Self {
        Self {
            rolling_window: 1000,
            time_scales: vec![1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0],
            min_r_squared: 0.5,
            max_kappa: 1.0,
            update_frequency: 100,
        }
    }
}

impl MIDCConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.rolling_window < 10 {
            return Err(ResearchError::Configuration(
                "rolling_window must be >= 10".to_string(),
            ));
        }
        if self.time_scales.is_empty() {
            return Err(ResearchError::Configuration(
                "time_scales cannot be empty".to_string(),
            ));
        }
        if self.min_r_squared < 0.0 || self.min_r_squared > 1.0 {
            return Err(ResearchError::Configuration(
                "min_r_squared must be in [0, 1]".to_string(),
            ));
        }
        if self.max_kappa <= 0.0 {
            return Err(ResearchError::Configuration(
                "max_kappa must be > 0".to_string(),
            ));
        }
        if self.update_frequency == 0 {
            return Err(ResearchError::Configuration(
                "update_frequency must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Persistence analyzer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// Minimum move magnitude to count as trend start (in basis points)
    pub min_move_bps: f64,

    /// Reversal threshold to end trend (in basis points)
    pub reversal_threshold_bps: f64,

    /// Maximum trend duration to track (in seconds)
    pub max_duration_seconds: f64,

    /// Rolling window for statistics
    pub stats_window: usize,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            min_move_bps: 5.0,
            reversal_threshold_bps: 10.0,
            max_duration_seconds: 3600.0,
            stats_window: 500,
        }
    }
}

impl PersistenceConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.min_move_bps <= 0.0 {
            return Err(ResearchError::Configuration(
                "min_move_bps must be > 0".to_string(),
            ));
        }
        if self.reversal_threshold_bps <= 0.0 {
            return Err(ResearchError::Configuration(
                "reversal_threshold_bps must be > 0".to_string(),
            ));
        }
        if self.max_duration_seconds <= 0.0 {
            return Err(ResearchError::Configuration(
                "max_duration_seconds must be > 0".to_string(),
            ));
        }
        if self.stats_window < 10 {
            return Err(ResearchError::Configuration(
                "stats_window must be >= 10".to_string(),
            ));
        }
        Ok(())
    }
}

/// Conditional model configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalConfig {
    /// Minimum samples required for a signature to be considered
    pub min_signature_samples: usize,

    /// Minimum edge over random (0.5) to be considered significant
    pub min_edge: f64,

    /// Maximum number of signatures to track
    pub max_signatures: usize,

    /// Decay factor for old observations (0 = no decay, 1 = full decay)
    pub observation_decay: f64,

    /// Lookback window for outcome tracking (in seconds)
    pub outcome_window_seconds: f64,
}

impl Default for ConditionalConfig {
    fn default() -> Self {
        Self {
            min_signature_samples: 30,
            min_edge: 0.05,
            max_signatures: 1000,
            observation_decay: 0.0,
            outcome_window_seconds: 60.0,
        }
    }
}

impl ConditionalConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.min_signature_samples < 5 {
            return Err(ResearchError::Configuration(
                "min_signature_samples must be >= 5".to_string(),
            ));
        }
        if self.min_edge < 0.0 || self.min_edge > 0.5 {
            return Err(ResearchError::Configuration(
                "min_edge must be in [0, 0.5]".to_string(),
            ));
        }
        if self.max_signatures < 10 {
            return Err(ResearchError::Configuration(
                "max_signatures must be >= 10".to_string(),
            ));
        }
        if self.observation_decay < 0.0 || self.observation_decay > 1.0 {
            return Err(ResearchError::Configuration(
                "observation_decay must be in [0, 1]".to_string(),
            ));
        }
        if self.outcome_window_seconds <= 0.0 {
            return Err(ResearchError::Configuration(
                "outcome_window_seconds must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Thresholds for tradeable assessment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssessmentThresholds {
    /// Maximum kappa for MIDC to be considered favorable
    pub max_kappa: f64,

    /// Maximum entropy for market to be considered predictable
    pub max_entropy: f64,

    /// Minimum mean persistence duration (in seconds) for trends to be tradeable
    pub min_persistence_seconds: f64,

    /// Minimum number of significant signals required
    pub min_significant_signals: usize,

    /// Minimum edge required for a signal to be considered significant
    pub signal_min_edge: f64,
}

impl Default for AssessmentThresholds {
    fn default() -> Self {
        Self {
            max_kappa: 0.1,
            max_entropy: 0.7,
            min_persistence_seconds: 30.0,
            min_significant_signals: 3,
            signal_min_edge: 0.05,
        }
    }
}

impl AssessmentThresholds {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.max_kappa <= 0.0 {
            return Err(ResearchError::Configuration(
                "max_kappa must be > 0".to_string(),
            ));
        }
        if self.max_entropy <= 0.0 || self.max_entropy > 1.0 {
            return Err(ResearchError::Configuration(
                "max_entropy must be in (0, 1]".to_string(),
            ));
        }
        if self.min_persistence_seconds < 0.0 {
            return Err(ResearchError::Configuration(
                "min_persistence_seconds must be >= 0".to_string(),
            ));
        }
        if self.signal_min_edge < 0.0 || self.signal_min_edge > 0.5 {
            return Err(ResearchError::Configuration(
                "signal_min_edge must be in [0, 0.5]".to_string(),
            ));
        }
        Ok(())
    }
}

// ============================================================================
// Core Trait: ResearchEngine
// ============================================================================

/// Core trait for research engine implementations.
///
/// A ResearchEngine continuously processes market features and maintains
/// research state. It answers the fundamental question: "Is I(Past; Future) > 0 right now?"
///
/// # Lifecycle
///
/// 1. **Initialization**: Create with config and optionally load previous state
/// 2. **Processing**: Call `on_features()` for each incoming feature snapshot
/// 3. **Assessment**: Call `assess()` to get current tradeable assessment
/// 4. **Config Generation**: Call `generate_config()` when edge is detected
/// 5. **Checkpointing**: Periodic `checkpoint()` calls persist state
///
/// # Thread Safety
///
/// Implementations should be designed for single-threaded use within an async context.
/// For concurrent access, wrap in appropriate synchronization primitives.
pub trait ResearchEngine: Send {
    /// Process a new features snapshot.
    ///
    /// This is the primary data ingestion method. Each snapshot should be
    /// processed to update internal MIDC, persistence, and conditional estimates.
    ///
    /// # Arguments
    /// * `snapshot` - The feature snapshot to process
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(ResearchError)` on failure
    fn on_features(&mut self, snapshot: &FeaturesSnapshot) -> Result<(), ResearchError>;

    /// Get the current tradeable assessment.
    ///
    /// Returns the current assessment of market conditions and whether
    /// trading is viable. This should be a cheap operation that doesn't
    /// perform heavy computation.
    fn assess(&self) -> TradeableAssessment;

    /// Generate an algorithm configuration from current research state.
    ///
    /// Returns `Some(config)` if edge is detected and trading is viable,
    /// `None` if conditions are not favorable for trading.
    fn generate_config(&self) -> Option<AlgorithmConfig>;

    /// Get a reference to the current research state.
    fn state(&self) -> &ResearchState;

    /// Get a mutable reference to the current research state.
    fn state_mut(&mut self) -> &mut ResearchState;

    /// Checkpoint the current state to persistent storage.
    ///
    /// This saves the current ResearchState to the ResearchStore.
    /// Called periodically or on significant state changes.
    fn checkpoint(&mut self) -> Result<(), ResearchError>;

    /// Reset the engine to initial state.
    ///
    /// Clears all accumulated research data and starts fresh.
    /// The configuration remains unchanged.
    fn reset(&mut self);

    /// Get the engine configuration.
    fn config(&self) -> &ResearchEngineConfig;

    /// Get statistics about the engine's processing.
    fn stats(&self) -> ResearchEngineStats;

    /// Check if the engine has enough data to produce valid assessments.
    fn is_ready(&self) -> bool {
        self.stats().samples_processed >= self.config().min_samples
    }

    /// Get the number of samples processed.
    fn samples_processed(&self) -> usize {
        self.stats().samples_processed
    }

    /// Get the current MIDC estimate.
    fn midc(&self) -> &MIDCEstimate {
        &self.state().midc
    }

    /// Get the current persistence statistics.
    fn persistence(&self) -> &PersistenceStats {
        &self.state().persistence
    }

    /// Get significant signals from the conditional model.
    fn significant_signals(&self) -> Vec<SignificantSignal> {
        let config = self.config();
        let state = self.state();

        state
            .conditional_table
            .iter()
            .filter_map(|(key, prob)| {
                if prob.is_significant(
                    config.assessment_thresholds.signal_min_edge,
                    config.conditional_config.min_signature_samples,
                ) {
                    Some(SignificantSignal {
                        signature_key: key.clone(),
                        probability: prob.clone(),
                        edge: prob.edge(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Statistics about research engine processing
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResearchEngineStats {
    /// Total number of samples processed
    pub samples_processed: usize,

    /// Number of MIDC updates performed
    pub midc_updates: usize,

    /// Number of persistence updates performed
    pub persistence_updates: usize,

    /// Number of conditional model updates
    pub conditional_updates: usize,

    /// Number of checkpoints performed
    pub checkpoints: usize,

    /// Timestamp of last checkpoint
    pub last_checkpoint: Option<DateTime<Utc>>,

    /// Timestamp of first sample
    pub first_sample_at: Option<DateTime<Utc>>,

    /// Timestamp of last sample
    pub last_sample_at: Option<DateTime<Utc>>,

    /// Number of assessment changes
    pub assessment_changes: usize,

    /// Current assessment state (for tracking changes)
    pub current_assessment_tradeable: bool,
}

impl ResearchEngineStats {
    /// Create a new stats instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a sample processed
    pub fn record_sample(&mut self, timestamp: DateTime<Utc>) {
        self.samples_processed += 1;
        if self.first_sample_at.is_none() {
            self.first_sample_at = Some(timestamp);
        }
        self.last_sample_at = Some(timestamp);
    }

    /// Record an MIDC update
    pub fn record_midc_update(&mut self) {
        self.midc_updates += 1;
    }

    /// Record a persistence update
    pub fn record_persistence_update(&mut self) {
        self.persistence_updates += 1;
    }

    /// Record a conditional model update
    pub fn record_conditional_update(&mut self) {
        self.conditional_updates += 1;
    }

    /// Record a checkpoint
    pub fn record_checkpoint(&mut self) {
        self.checkpoints += 1;
        self.last_checkpoint = Some(Utc::now());
    }

    /// Record an assessment change
    pub fn record_assessment_change(&mut self, is_tradeable: bool) {
        if is_tradeable != self.current_assessment_tradeable {
            self.assessment_changes += 1;
            self.current_assessment_tradeable = is_tradeable;
        }
    }

    /// Get processing duration
    pub fn processing_duration(&self) -> Option<chrono::Duration> {
        match (self.first_sample_at, self.last_sample_at) {
            (Some(first), Some(last)) => Some(last - first),
            _ => None,
        }
    }

    /// Get average samples per second
    pub fn samples_per_second(&self) -> Option<f64> {
        self.processing_duration().map(|d| {
            let seconds = d.num_milliseconds() as f64 / 1000.0;
            if seconds > 0.0 {
                self.samples_processed as f64 / seconds
            } else {
                0.0
            }
        })
    }
}

impl fmt::Display for ResearchEngineStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Research Engine Statistics:")?;
        writeln!(f, "  Samples processed: {}", self.samples_processed)?;
        writeln!(f, "  MIDC updates: {}", self.midc_updates)?;
        writeln!(f, "  Persistence updates: {}", self.persistence_updates)?;
        writeln!(f, "  Conditional updates: {}", self.conditional_updates)?;
        writeln!(f, "  Checkpoints: {}", self.checkpoints)?;
        writeln!(f, "  Assessment changes: {}", self.assessment_changes)?;
        if let Some(rate) = self.samples_per_second() {
            writeln!(f, "  Processing rate: {:.2} samples/sec", rate)?;
        }
        Ok(())
    }
}

/// A significant signal from the conditional model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignificantSignal {
    /// The signature key
    pub signature_key: String,

    /// The conditional probability
    pub probability: ConditionalProbability,

    /// Edge over random (p_continuation - 0.5)
    pub edge: f64,
}

impl SignificantSignal {
    /// Get the expected value of this signal in basis points
    pub fn expected_value_bps(&self) -> f64 {
        self.probability.expected_magnitude_bps * self.edge * 2.0
    }

    /// Get the signal quality score (edge * sqrt(samples))
    pub fn quality_score(&self) -> f64 {
        self.edge.abs() * (self.probability.sample_count as f64).sqrt()
    }
}

// ============================================================================
// ResearchEngineFactory Trait
// ============================================================================

/// Factory trait for creating research engine instances
pub trait ResearchEngineFactory: Send + Sync {
    /// Create a new research engine from configuration
    fn create(&self, config: ResearchEngineConfig) -> Result<Box<dyn ResearchEngine>, ResearchError>;

    /// Create a research engine with a connected store for persistence
    fn create_with_store(
        &self,
        config: ResearchEngineConfig,
        store: ResearchStore,
    ) -> Result<Box<dyn ResearchEngine>, ResearchError>;

    /// Load or initialize a research engine from persistent storage
    fn load_or_init(
        &self,
        config: ResearchEngineConfig,
        store: ResearchStore,
    ) -> Result<Box<dyn ResearchEngine>, ResearchError>;

    /// Get the factory name/version
    fn name(&self) -> &str;
}

// ============================================================================
// Utility Types
// ============================================================================

/// A price point for research calculations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PricePoint {
    /// Timestamp of the price
    pub timestamp: DateTime<Utc>,

    /// Price value
    pub price: f64,

    /// Volume (optional)
    pub volume: Option<f64>,
}

impl PricePoint {
    /// Create a new price point
    pub fn new(timestamp: DateTime<Utc>, price: f64) -> Self {
        Self {
            timestamp,
            price,
            volume: None,
        }
    }

    /// Create with volume
    pub fn with_volume(timestamp: DateTime<Utc>, price: f64, volume: f64) -> Self {
        Self {
            timestamp,
            price,
            volume: Some(volume),
        }
    }

    /// Calculate return from this point to another
    pub fn return_to(&self, other: &PricePoint) -> f64 {
        if self.price > 0.0 {
            (other.price - self.price) / self.price
        } else {
            0.0
        }
    }

    /// Calculate return in basis points
    pub fn return_bps_to(&self, other: &PricePoint) -> f64 {
        self.return_to(other) * 10000.0
    }

    /// Time difference in seconds
    pub fn seconds_to(&self, other: &PricePoint) -> f64 {
        (other.timestamp - self.timestamp).num_milliseconds() as f64 / 1000.0
    }
}

/// Outcome of a trade opportunity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// Price continued in the predicted direction
    Continuation,
    /// Price reversed from the predicted direction
    Reversal,
    /// Price stayed within threshold (no clear outcome)
    Neutral,
}

impl Outcome {
    /// Create outcome from price movement
    pub fn from_movement(predicted_direction: bool, actual_return: f64, threshold_bps: f64) -> Self {
        let actual_bps = actual_return * 10000.0;
        let threshold = threshold_bps.abs();

        if actual_bps.abs() < threshold {
            Outcome::Neutral
        } else if (predicted_direction && actual_bps > 0.0) || (!predicted_direction && actual_bps < 0.0) {
            Outcome::Continuation
        } else {
            Outcome::Reversal
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== ResearchEngineConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = ResearchEngineConfig::default();
        assert_eq!(config.symbol, "BTCUSDT");
        assert_eq!(config.min_samples, 100);
        assert_eq!(config.checkpoint_interval, 1000);
        assert!(config.auto_checkpoint);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_new() {
        let config = ResearchEngineConfig::new("ETHUSDT");
        assert_eq!(config.symbol, "ETHUSDT");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_builder_pattern() {
        let config = ResearchEngineConfig::new("SOLUSDT")
            .with_min_samples(500)
            .with_checkpoint_interval(2000)
            .without_auto_checkpoint();

        assert_eq!(config.symbol, "SOLUSDT");
        assert_eq!(config.min_samples, 500);
        assert_eq!(config.checkpoint_interval, 2000);
        assert!(!config.auto_checkpoint);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_empty_symbol_validation() {
        let config = ResearchEngineConfig {
            symbol: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_zero_min_samples_validation() {
        let config = ResearchEngineConfig::default().with_min_samples(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_zero_checkpoint_interval_validation() {
        let config = ResearchEngineConfig::default().with_checkpoint_interval(0);
        assert!(config.validate().is_err());
    }

    // ==================== MIDCConfig Tests ====================

    #[test]
    fn test_midc_config_default() {
        let config = MIDCConfig::default();
        assert_eq!(config.rolling_window, 1000);
        assert!(!config.time_scales.is_empty());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_midc_config_invalid_rolling_window() {
        let config = MIDCConfig {
            rolling_window: 5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_midc_config_empty_time_scales() {
        let config = MIDCConfig {
            time_scales: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_midc_config_invalid_r_squared() {
        let config = MIDCConfig {
            min_r_squared: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_midc_config_negative_r_squared() {
        let config = MIDCConfig {
            min_r_squared: -0.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_midc_config_invalid_max_kappa() {
        let config = MIDCConfig {
            max_kappa: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_midc_config_zero_update_frequency() {
        let config = MIDCConfig {
            update_frequency: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // ==================== PersistenceConfig Tests ====================

    #[test]
    fn test_persistence_config_default() {
        let config = PersistenceConfig::default();
        assert!(config.min_move_bps > 0.0);
        assert!(config.reversal_threshold_bps > 0.0);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_persistence_config_invalid_min_move() {
        let config = PersistenceConfig {
            min_move_bps: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_persistence_config_invalid_reversal_threshold() {
        let config = PersistenceConfig {
            reversal_threshold_bps: -5.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_persistence_config_invalid_max_duration() {
        let config = PersistenceConfig {
            max_duration_seconds: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_persistence_config_invalid_stats_window() {
        let config = PersistenceConfig {
            stats_window: 5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // ==================== ConditionalConfig Tests ====================

    #[test]
    fn test_conditional_config_default() {
        let config = ConditionalConfig::default();
        assert!(config.min_signature_samples >= 5);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_conditional_config_invalid_min_samples() {
        let config = ConditionalConfig {
            min_signature_samples: 2,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_conditional_config_invalid_min_edge() {
        let config = ConditionalConfig {
            min_edge: 0.6,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_conditional_config_negative_min_edge() {
        let config = ConditionalConfig {
            min_edge: -0.1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_conditional_config_invalid_max_signatures() {
        let config = ConditionalConfig {
            max_signatures: 5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_conditional_config_invalid_decay() {
        let config = ConditionalConfig {
            observation_decay: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_conditional_config_negative_decay() {
        let config = ConditionalConfig {
            observation_decay: -0.1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_conditional_config_invalid_outcome_window() {
        let config = ConditionalConfig {
            outcome_window_seconds: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // ==================== AssessmentThresholds Tests ====================

    #[test]
    fn test_assessment_thresholds_default() {
        let thresholds = AssessmentThresholds::default();
        assert!(thresholds.max_kappa > 0.0);
        assert!(thresholds.max_entropy > 0.0);
        assert!(thresholds.validate().is_ok());
    }

    #[test]
    fn test_assessment_thresholds_invalid_max_kappa() {
        let thresholds = AssessmentThresholds {
            max_kappa: 0.0,
            ..Default::default()
        };
        assert!(thresholds.validate().is_err());
    }

    #[test]
    fn test_assessment_thresholds_invalid_max_entropy() {
        let thresholds = AssessmentThresholds {
            max_entropy: 1.5,
            ..Default::default()
        };
        assert!(thresholds.validate().is_err());
    }

    #[test]
    fn test_assessment_thresholds_invalid_min_persistence() {
        let thresholds = AssessmentThresholds {
            min_persistence_seconds: -10.0,
            ..Default::default()
        };
        assert!(thresholds.validate().is_err());
    }

    #[test]
    fn test_assessment_thresholds_invalid_signal_edge() {
        let thresholds = AssessmentThresholds {
            signal_min_edge: 0.6,
            ..Default::default()
        };
        assert!(thresholds.validate().is_err());
    }

    // ==================== ResearchEngineStats Tests ====================

    #[test]
    fn test_stats_new() {
        let stats = ResearchEngineStats::new();
        assert_eq!(stats.samples_processed, 0);
        assert_eq!(stats.midc_updates, 0);
        assert!(stats.first_sample_at.is_none());
    }

    #[test]
    fn test_stats_record_sample() {
        let mut stats = ResearchEngineStats::new();
        let now = Utc::now();

        stats.record_sample(now);
        assert_eq!(stats.samples_processed, 1);
        assert_eq!(stats.first_sample_at, Some(now));
        assert_eq!(stats.last_sample_at, Some(now));

        let later = now + chrono::Duration::seconds(10);
        stats.record_sample(later);
        assert_eq!(stats.samples_processed, 2);
        assert_eq!(stats.first_sample_at, Some(now));
        assert_eq!(stats.last_sample_at, Some(later));
    }

    #[test]
    fn test_stats_record_updates() {
        let mut stats = ResearchEngineStats::new();

        stats.record_midc_update();
        stats.record_midc_update();
        assert_eq!(stats.midc_updates, 2);

        stats.record_persistence_update();
        assert_eq!(stats.persistence_updates, 1);

        stats.record_conditional_update();
        stats.record_conditional_update();
        stats.record_conditional_update();
        assert_eq!(stats.conditional_updates, 3);
    }

    #[test]
    fn test_stats_record_checkpoint() {
        let mut stats = ResearchEngineStats::new();

        assert!(stats.last_checkpoint.is_none());
        stats.record_checkpoint();
        assert_eq!(stats.checkpoints, 1);
        assert!(stats.last_checkpoint.is_some());
    }

    #[test]
    fn test_stats_record_assessment_change() {
        let mut stats = ResearchEngineStats::new();
        assert_eq!(stats.assessment_changes, 0);

        // First change from false to true
        stats.record_assessment_change(true);
        assert_eq!(stats.assessment_changes, 1);
        assert!(stats.current_assessment_tradeable);

        // Same value, no change recorded
        stats.record_assessment_change(true);
        assert_eq!(stats.assessment_changes, 1);

        // Change back to false
        stats.record_assessment_change(false);
        assert_eq!(stats.assessment_changes, 2);
        assert!(!stats.current_assessment_tradeable);
    }

    #[test]
    fn test_stats_processing_duration() {
        let mut stats = ResearchEngineStats::new();
        assert!(stats.processing_duration().is_none());

        let start = Utc::now();
        stats.record_sample(start);

        let end = start + chrono::Duration::seconds(60);
        stats.record_sample(end);

        let duration = stats.processing_duration().unwrap();
        assert_eq!(duration.num_seconds(), 60);
    }

    #[test]
    fn test_stats_samples_per_second() {
        let mut stats = ResearchEngineStats::new();
        assert!(stats.samples_per_second().is_none());

        let start = Utc::now();
        for i in 0..100 {
            let ts = start + chrono::Duration::milliseconds(i * 100);
            stats.record_sample(ts);
        }

        let rate = stats.samples_per_second().unwrap();
        // 100 samples over ~10 seconds = ~10 samples/sec
        assert!(rate > 9.0 && rate < 11.0);
    }

    #[test]
    fn test_stats_display() {
        let mut stats = ResearchEngineStats::new();
        stats.samples_processed = 1000;
        stats.midc_updates = 50;

        let display = format!("{}", stats);
        assert!(display.contains("Samples processed: 1000"));
        assert!(display.contains("MIDC updates: 50"));
    }

    // ==================== SignificantSignal Tests ====================

    #[test]
    fn test_significant_signal_expected_value() {
        let signal = SignificantSignal {
            signature_key: "test".to_string(),
            probability: ConditionalProbability {
                p_continuation: 0.6,
                p_reversal: 0.4,
                expected_magnitude_bps: 10.0,
                std_magnitude_bps: 5.0,
                sample_count: 100,
                confidence_interval: (0.55, 0.65),
            },
            edge: 0.1, // 60% - 50% = 10%
        };

        // Expected value = 10 bps * 0.1 * 2 = 2 bps
        assert!((signal.expected_value_bps() - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_significant_signal_quality_score() {
        let signal = SignificantSignal {
            signature_key: "test".to_string(),
            probability: ConditionalProbability {
                p_continuation: 0.6,
                p_reversal: 0.4,
                expected_magnitude_bps: 10.0,
                std_magnitude_bps: 5.0,
                sample_count: 100,
                confidence_interval: (0.55, 0.65),
            },
            edge: 0.1,
        };

        // Quality = 0.1 * sqrt(100) = 0.1 * 10 = 1.0
        assert!((signal.quality_score() - 1.0).abs() < 0.001);
    }

    // ==================== PricePoint Tests ====================

    #[test]
    fn test_price_point_new() {
        let now = Utc::now();
        let pp = PricePoint::new(now, 100.0);
        assert_eq!(pp.timestamp, now);
        assert_eq!(pp.price, 100.0);
        assert!(pp.volume.is_none());
    }

    #[test]
    fn test_price_point_with_volume() {
        let now = Utc::now();
        let pp = PricePoint::with_volume(now, 100.0, 1000.0);
        assert_eq!(pp.price, 100.0);
        assert_eq!(pp.volume, Some(1000.0));
    }

    #[test]
    fn test_price_point_return_to() {
        let now = Utc::now();
        let p1 = PricePoint::new(now, 100.0);
        let p2 = PricePoint::new(now, 105.0);

        let ret = p1.return_to(&p2);
        assert!((ret - 0.05).abs() < 0.0001);
    }

    #[test]
    fn test_price_point_return_bps() {
        let now = Utc::now();
        let p1 = PricePoint::new(now, 100.0);
        let p2 = PricePoint::new(now, 101.0);

        let ret_bps = p1.return_bps_to(&p2);
        assert!((ret_bps - 100.0).abs() < 0.1); // 1% = 100 bps
    }

    #[test]
    fn test_price_point_return_zero_price() {
        let now = Utc::now();
        let p1 = PricePoint::new(now, 0.0);
        let p2 = PricePoint::new(now, 100.0);

        assert_eq!(p1.return_to(&p2), 0.0);
    }

    #[test]
    fn test_price_point_seconds_to() {
        let now = Utc::now();
        let p1 = PricePoint::new(now, 100.0);
        let p2 = PricePoint::new(now + chrono::Duration::seconds(30), 105.0);

        let seconds = p1.seconds_to(&p2);
        assert!((seconds - 30.0).abs() < 0.001);
    }

    #[test]
    fn test_price_point_negative_return() {
        let now = Utc::now();
        let p1 = PricePoint::new(now, 100.0);
        let p2 = PricePoint::new(now, 95.0);

        let ret = p1.return_to(&p2);
        assert!((ret - (-0.05)).abs() < 0.0001);
    }

    // ==================== Outcome Tests ====================

    #[test]
    fn test_outcome_continuation_up() {
        let outcome = Outcome::from_movement(true, 0.01, 5.0);
        assert_eq!(outcome, Outcome::Continuation);
    }

    #[test]
    fn test_outcome_continuation_down() {
        let outcome = Outcome::from_movement(false, -0.01, 5.0);
        assert_eq!(outcome, Outcome::Continuation);
    }

    #[test]
    fn test_outcome_reversal_up() {
        let outcome = Outcome::from_movement(false, 0.01, 5.0);
        assert_eq!(outcome, Outcome::Reversal);
    }

    #[test]
    fn test_outcome_reversal_down() {
        let outcome = Outcome::from_movement(true, -0.01, 5.0);
        assert_eq!(outcome, Outcome::Reversal);
    }

    #[test]
    fn test_outcome_neutral() {
        let outcome = Outcome::from_movement(true, 0.0001, 5.0);
        assert_eq!(outcome, Outcome::Neutral);
    }

    #[test]
    fn test_outcome_at_threshold() {
        // 0.0005 = 5 bps, threshold = 5.0 bps
        // At exact threshold, it's not neutral (< threshold), it's continuation
        let outcome = Outcome::from_movement(true, 0.0005, 5.0);
        assert_eq!(outcome, Outcome::Continuation);
    }

    #[test]
    fn test_outcome_just_above_threshold() {
        let outcome = Outcome::from_movement(true, 0.0006, 5.0);
        assert_eq!(outcome, Outcome::Continuation);
    }

    // ==================== ResearchError Tests ====================

    #[test]
    fn test_research_error_display() {
        let err = ResearchError::Persistence("test error".to_string());
        assert!(format!("{}", err).contains("test error"));
    }

    #[test]
    fn test_research_error_insufficient_data() {
        let err = ResearchError::InsufficientData {
            message: "need more samples".to_string(),
            required: 100,
            available: 50,
        };
        let display = format!("{}", err);
        assert!(display.contains("100"));
        assert!(display.contains("50"));
    }

    #[test]
    fn test_research_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let research_err: ResearchError = io_err.into();
        assert!(matches!(research_err, ResearchError::Io(_)));
    }

    // ==================== Configuration Integration Tests ====================

    #[test]
    fn test_full_config_with_custom_components() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(200)
            .with_checkpoint_interval(500)
            .with_midc_config(MIDCConfig {
                rolling_window: 500,
                time_scales: vec![1.0, 5.0, 15.0],
                min_r_squared: 0.6,
                max_kappa: 0.5,
                update_frequency: 50,
            })
            .with_persistence_config(PersistenceConfig {
                min_move_bps: 10.0,
                reversal_threshold_bps: 20.0,
                max_duration_seconds: 1800.0,
                stats_window: 200,
            })
            .with_conditional_config(ConditionalConfig {
                min_signature_samples: 50,
                min_edge: 0.1,
                max_signatures: 500,
                observation_decay: 0.1,
                outcome_window_seconds: 120.0,
            })
            .with_assessment_thresholds(AssessmentThresholds {
                max_kappa: 0.05,
                max_entropy: 0.6,
                min_persistence_seconds: 60.0,
                min_significant_signals: 5,
                signal_min_edge: 0.1,
            });

        assert!(config.validate().is_ok());
        assert_eq!(config.symbol, "BTCUSDT");
        assert_eq!(config.min_samples, 200);
        assert_eq!(config.midc_config.rolling_window, 500);
        assert_eq!(config.persistence_config.min_move_bps, 10.0);
        assert_eq!(config.conditional_config.min_signature_samples, 50);
        assert_eq!(config.assessment_thresholds.max_kappa, 0.05);
    }

    #[test]
    fn test_config_serialization() {
        let config = ResearchEngineConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ResearchEngineConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.symbol, deserialized.symbol);
        assert_eq!(config.min_samples, deserialized.min_samples);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_price_point_very_small_price() {
        let now = Utc::now();
        let p1 = PricePoint::new(now, 0.00000001);
        let p2 = PricePoint::new(now, 0.00000002);

        let ret = p1.return_to(&p2);
        assert!((ret - 1.0).abs() < 0.0001); // 100% increase
    }

    #[test]
    fn test_price_point_negative_time_diff() {
        let now = Utc::now();
        let p1 = PricePoint::new(now, 100.0);
        let p2 = PricePoint::new(now - chrono::Duration::seconds(30), 105.0);

        let seconds = p1.seconds_to(&p2);
        assert!((seconds - (-30.0)).abs() < 0.001);
    }

    #[test]
    fn test_significant_signal_negative_edge() {
        let signal = SignificantSignal {
            signature_key: "test".to_string(),
            probability: ConditionalProbability {
                p_continuation: 0.4,
                p_reversal: 0.6,
                expected_magnitude_bps: 10.0,
                std_magnitude_bps: 5.0,
                sample_count: 100,
                confidence_interval: (0.35, 0.45),
            },
            edge: -0.1, // 40% - 50% = -10%
        };

        // Quality score uses absolute edge
        assert!((signal.quality_score() - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_stats_no_samples_rate() {
        let stats = ResearchEngineStats::new();
        assert!(stats.samples_per_second().is_none());
    }

    #[test]
    fn test_stats_single_sample_rate() {
        let mut stats = ResearchEngineStats::new();
        stats.record_sample(Utc::now());

        // With only one sample, duration is 0 seconds
        let rate = stats.samples_per_second();
        assert!(rate.is_some());
        assert_eq!(rate.unwrap(), 0.0);
    }
}

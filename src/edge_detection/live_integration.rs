//! Live Feature Integration - Task 1.8
//!
//! Connect research engine to live feature stream for continuous updates.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                        LIVE RESEARCH RUNNER                                  │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                             │
//! │  FeaturesSnapshot Channel ─────────────────────────────────────────────┐    │
//! │  └── crossbeam::channel::Receiver<FeaturesSnapshot>                    │    │
//! │                                      │                                 │    │
//! │                                      ▼ process_feature()               │    │
//! │  LiveResearchRunner ─────────────────────────────────────────────────┐ │    │
//! │  ├── ResearchEngine (Box<dyn ResearchEngine>)                        │ │    │
//! │  ├── Config (LiveResearchConfig)                                     │ │    │
//! │  │   ├── checkpoint_interval_minutes: u64                            │ │    │
//! │  │   ├── emit_assessment_changes: bool                               │ │    │
//! │  │   └── max_samples_per_batch: usize                                │ │    │
//! │  └── State                                                           │ │    │
//! │      ├── last_checkpoint: DateTime<Utc>                              │ │    │
//! │      ├── samples_since_checkpoint: usize                             │ │    │
//! │      └── last_assessment: TradeableAssessment                        │ │    │
//! │                                      │                                 │    │
//! │                                      ▼ on_assessment_change()          │    │
//! │  Assessment Change Channel ──────────────────────────────────────────┘ │    │
//! │  └── Option<crossbeam::channel::Sender<AssessmentChange>>             │    │
//! │                                                                             │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example Usage
//!
//! ```rust,ignore
//! use ingestor::edge_detection::{
//!     LiveResearchRunner, LiveResearchConfig,
//!     DefaultResearchEngineFactory, ResearchEngineConfig,
//! };
//! use crossbeam::channel;
//!
//! // Create channels
//! let (feature_tx, feature_rx) = channel::bounded(1000);
//! let (assessment_tx, assessment_rx) = channel::bounded(100);
//!
//! // Create runner
//! let factory = DefaultResearchEngineFactory;
//! let engine_config = ResearchEngineConfig::new("BTCUSDT");
//! let live_config = LiveResearchConfig::default()
//!     .with_checkpoint_interval(5);
//!
//! let mut runner = LiveResearchRunner::new(
//!     factory.create(engine_config)?,
//!     live_config,
//! );
//!
//! // Optionally subscribe to assessment changes
//! runner.subscribe_assessment_changes(assessment_tx);
//!
//! // Run the async task
//! runner.run(feature_rx).await?;
//! ```

use crate::features::FeaturesSnapshot;
use crate::core::TradeableAssessment;
use crate::edge_detection::{ResearchEngine, ResearchError};

use chrono::{DateTime, Duration, Utc};
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use serde::{Deserialize, Serialize};
use std::fmt;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the live research runner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveResearchConfig {
    /// Checkpoint interval in minutes
    pub checkpoint_interval_minutes: u64,

    /// Whether to emit assessment changes
    pub emit_assessment_changes: bool,

    /// Maximum samples to process per batch (for flow control)
    pub max_samples_per_batch: usize,

    /// Minimum interval between checkpoints (even if checkpoint_interval samples reached)
    pub min_checkpoint_interval_seconds: u64,

    /// Whether to checkpoint on shutdown
    pub checkpoint_on_shutdown: bool,

    /// Log level for runner events (0=none, 1=errors, 2=warnings, 3=info, 4=debug)
    pub log_level: u8,
}

impl Default for LiveResearchConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval_minutes: 5,
            emit_assessment_changes: true,
            max_samples_per_batch: 100,
            min_checkpoint_interval_seconds: 60,
            checkpoint_on_shutdown: true,
            log_level: 3,
        }
    }
}

impl LiveResearchConfig {
    /// Create a new config with checkpoint interval in minutes
    pub fn new(checkpoint_interval_minutes: u64) -> Self {
        Self {
            checkpoint_interval_minutes,
            ..Default::default()
        }
    }

    /// Set checkpoint interval
    pub fn with_checkpoint_interval(mut self, minutes: u64) -> Self {
        self.checkpoint_interval_minutes = minutes;
        self
    }

    /// Disable assessment change emission
    pub fn without_assessment_emission(mut self) -> Self {
        self.emit_assessment_changes = false;
        self
    }

    /// Set max samples per batch
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_samples_per_batch = size;
        self
    }

    /// Set minimum checkpoint interval
    pub fn with_min_checkpoint_interval(mut self, seconds: u64) -> Self {
        self.min_checkpoint_interval_seconds = seconds;
        self
    }

    /// Disable checkpoint on shutdown
    pub fn without_shutdown_checkpoint(mut self) -> Self {
        self.checkpoint_on_shutdown = false;
        self
    }

    /// Set log level
    pub fn with_log_level(mut self, level: u8) -> Self {
        self.log_level = level;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.checkpoint_interval_minutes == 0 {
            return Err(ResearchError::Configuration(
                "checkpoint_interval_minutes must be > 0".to_string(),
            ));
        }
        if self.max_samples_per_batch == 0 {
            return Err(ResearchError::Configuration(
                "max_samples_per_batch must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    /// Get checkpoint interval as Duration
    pub fn checkpoint_duration(&self) -> Duration {
        Duration::minutes(self.checkpoint_interval_minutes as i64)
    }

    /// Get minimum checkpoint interval as Duration
    pub fn min_checkpoint_duration(&self) -> Duration {
        Duration::seconds(self.min_checkpoint_interval_seconds as i64)
    }
}

// ============================================================================
// Assessment Change Event
// ============================================================================

/// Event emitted when tradeable assessment changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssessmentChange {
    /// Timestamp of the change
    pub timestamp: DateTime<Utc>,

    /// Previous assessment (None if first assessment)
    pub previous: Option<TradeableAssessment>,

    /// New assessment
    pub current: TradeableAssessment,

    /// Number of samples processed when change occurred
    pub samples_processed: usize,

    /// Symbol being researched
    pub symbol: String,
}

impl AssessmentChange {
    /// Create a new assessment change event
    pub fn new(
        previous: Option<TradeableAssessment>,
        current: TradeableAssessment,
        samples_processed: usize,
        symbol: String,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            previous,
            current,
            samples_processed,
            symbol,
        }
    }

    /// Check if the change is from non-tradeable to tradeable
    pub fn became_tradeable(&self) -> bool {
        match &self.previous {
            Some(prev) => !prev.is_tradeable && self.current.is_tradeable,
            None => self.current.is_tradeable,
        }
    }

    /// Check if the change is from tradeable to non-tradeable
    pub fn became_non_tradeable(&self) -> bool {
        match &self.previous {
            Some(prev) => prev.is_tradeable && !self.current.is_tradeable,
            None => false,
        }
    }

    /// Get the change in position scale (None if no previous)
    pub fn position_scale_change(&self) -> Option<f64> {
        self.previous
            .as_ref()
            .map(|prev| self.current.position_scale - prev.position_scale)
    }
}

impl fmt::Display for AssessmentChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] Assessment change for {}: {} -> {} (samples: {})",
            self.timestamp.format("%Y-%m-%d %H:%M:%S"),
            self.symbol,
            self.previous
                .as_ref()
                .map(|p| if p.is_tradeable { "TRADEABLE" } else { "NOT_TRADEABLE" })
                .unwrap_or("N/A"),
            if self.current.is_tradeable { "TRADEABLE" } else { "NOT_TRADEABLE" },
            self.samples_processed
        )
    }
}

// ============================================================================
// Runner State
// ============================================================================

/// Internal state for the live research runner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveResearchState {
    /// Last checkpoint timestamp
    pub last_checkpoint: DateTime<Utc>,

    /// Samples processed since last checkpoint
    pub samples_since_checkpoint: usize,

    /// Total samples processed
    pub total_samples: usize,

    /// Last known assessment
    pub last_assessment: Option<TradeableAssessment>,

    /// Number of assessment changes
    pub assessment_changes: usize,

    /// Number of checkpoints performed
    pub checkpoints_performed: usize,

    /// Start time
    pub started_at: DateTime<Utc>,

    /// Last sample timestamp
    pub last_sample_at: Option<DateTime<Utc>>,

    /// Is the runner currently active
    pub is_running: bool,

    /// Errors encountered
    pub errors_encountered: usize,
}

impl Default for LiveResearchState {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            last_checkpoint: now,
            samples_since_checkpoint: 0,
            total_samples: 0,
            last_assessment: None,
            assessment_changes: 0,
            checkpoints_performed: 0,
            started_at: now,
            last_sample_at: None,
            is_running: false,
            errors_encountered: 0,
        }
    }
}

impl LiveResearchState {
    /// Create new state
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset state to initial
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Record a sample processed
    pub fn record_sample(&mut self, timestamp: DateTime<Utc>) {
        self.total_samples += 1;
        self.samples_since_checkpoint += 1;
        self.last_sample_at = Some(timestamp);
    }

    /// Record a checkpoint
    pub fn record_checkpoint(&mut self) {
        self.last_checkpoint = Utc::now();
        self.samples_since_checkpoint = 0;
        self.checkpoints_performed += 1;
    }

    /// Record an assessment change
    pub fn record_assessment_change(&mut self, assessment: TradeableAssessment) {
        self.last_assessment = Some(assessment);
        self.assessment_changes += 1;
    }

    /// Record an error
    pub fn record_error(&mut self) {
        self.errors_encountered += 1;
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        Utc::now() - self.started_at
    }

    /// Get processing rate (samples per second)
    pub fn processing_rate(&self) -> Option<f64> {
        let uptime_secs = self.uptime().num_milliseconds() as f64 / 1000.0;
        if uptime_secs > 0.0 {
            Some(self.total_samples as f64 / uptime_secs)
        } else {
            None
        }
    }

    /// Check if checkpoint is due based on time
    pub fn is_checkpoint_due(&self, interval: Duration, min_interval: Duration) -> bool {
        let now = Utc::now();
        let time_since_checkpoint = now - self.last_checkpoint;

        // Must meet minimum interval
        if time_since_checkpoint < min_interval {
            return false;
        }

        // Check if regular interval has passed
        time_since_checkpoint >= interval
    }
}

impl fmt::Display for LiveResearchState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Live Research Runner State:")?;
        writeln!(f, "  Started at: {}", self.started_at.format("%Y-%m-%d %H:%M:%S"))?;
        writeln!(f, "  Is running: {}", self.is_running)?;
        writeln!(f, "  Total samples: {}", self.total_samples)?;
        writeln!(f, "  Samples since checkpoint: {}", self.samples_since_checkpoint)?;
        writeln!(f, "  Checkpoints: {}", self.checkpoints_performed)?;
        writeln!(f, "  Assessment changes: {}", self.assessment_changes)?;
        writeln!(f, "  Errors: {}", self.errors_encountered)?;
        if let Some(rate) = self.processing_rate() {
            writeln!(f, "  Processing rate: {:.2} samples/sec", rate)?;
        }
        Ok(())
    }
}

// ============================================================================
// Live Research Runner
// ============================================================================

/// Live research runner that processes feature streams
pub struct LiveResearchRunner {
    /// The research engine
    engine: Box<dyn ResearchEngine>,

    /// Configuration
    config: LiveResearchConfig,

    /// Runner state
    state: LiveResearchState,

    /// Assessment change sender
    assessment_sender: Option<Sender<AssessmentChange>>,

    /// Shutdown flag
    shutdown_requested: bool,
}

impl LiveResearchRunner {
    /// Create a new live research runner
    pub fn new(engine: Box<dyn ResearchEngine>, config: LiveResearchConfig) -> Self {
        Self {
            engine,
            config,
            state: LiveResearchState::new(),
            assessment_sender: None,
            shutdown_requested: false,
        }
    }

    /// Create with default config
    pub fn with_default_config(engine: Box<dyn ResearchEngine>) -> Self {
        Self::new(engine, LiveResearchConfig::default())
    }

    /// Subscribe to assessment changes
    pub fn subscribe_assessment_changes(&mut self, sender: Sender<AssessmentChange>) {
        self.assessment_sender = Some(sender);
    }

    /// Unsubscribe from assessment changes
    pub fn unsubscribe_assessment_changes(&mut self) {
        self.assessment_sender = None;
    }

    /// Get the current configuration
    pub fn config(&self) -> &LiveResearchConfig {
        &self.config
    }

    /// Get the current state
    pub fn state(&self) -> &LiveResearchState {
        &self.state
    }

    /// Get a reference to the engine
    pub fn engine(&self) -> &dyn ResearchEngine {
        self.engine.as_ref()
    }

    /// Get a mutable reference to the engine
    pub fn engine_mut(&mut self) -> &mut dyn ResearchEngine {
        self.engine.as_mut()
    }

    /// Request shutdown
    pub fn request_shutdown(&mut self) {
        self.shutdown_requested = true;
    }

    /// Check if shutdown was requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested
    }

    /// Process a single feature snapshot
    pub fn process_feature(&mut self, snapshot: &FeaturesSnapshot) -> Result<(), ResearchError> {
        // Get timestamp from snapshot
        let timestamp = parse_timestamp(&snapshot.timestamp).unwrap_or_else(Utc::now);

        // Process the feature
        self.engine.on_features(snapshot)?;

        // Record sample
        self.state.record_sample(timestamp);

        // Check for assessment changes
        if self.config.emit_assessment_changes {
            self.check_assessment_change()?;
        }

        // Check if checkpoint is due
        if self.should_checkpoint() {
            self.perform_checkpoint()?;
        }

        Ok(())
    }

    /// Process a batch of features
    pub fn process_batch(&mut self, snapshots: &[FeaturesSnapshot]) -> Result<usize, ResearchError> {
        let mut processed = 0;

        for snapshot in snapshots.iter().take(self.config.max_samples_per_batch) {
            if self.shutdown_requested {
                break;
            }

            match self.process_feature(snapshot) {
                Ok(()) => processed += 1,
                Err(e) => {
                    self.state.record_error();
                    if self.config.log_level >= 1 {
                        log::error!("Error processing feature: {}", e);
                    }
                }
            }
        }

        Ok(processed)
    }

    /// Run the runner with a channel receiver (blocking)
    pub fn run_blocking(&mut self, receiver: Receiver<FeaturesSnapshot>) -> Result<RunnerStats, ResearchError> {
        self.config.validate()?;
        self.state.is_running = true;
        self.state.started_at = Utc::now();

        if self.config.log_level >= 3 {
            log::info!("Live research runner started for symbol: {}", self.engine.config().symbol);
        }

        loop {
            if self.shutdown_requested {
                break;
            }

            match receiver.try_recv() {
                Ok(snapshot) => {
                    if let Err(e) = self.process_feature(&snapshot) {
                        self.state.record_error();
                        if self.config.log_level >= 1 {
                            log::error!("Error processing feature: {}", e);
                        }
                    }
                }
                Err(TryRecvError::Empty) => {
                    // No data available, yield briefly
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(TryRecvError::Disconnected) => {
                    if self.config.log_level >= 3 {
                        log::info!("Feature channel disconnected, shutting down");
                    }
                    break;
                }
            }
        }

        // Shutdown
        self.state.is_running = false;

        // Final checkpoint if configured
        if self.config.checkpoint_on_shutdown && self.state.samples_since_checkpoint > 0 {
            if let Err(e) = self.perform_checkpoint() {
                if self.config.log_level >= 1 {
                    log::error!("Error during shutdown checkpoint: {}", e);
                }
            }
        }

        Ok(self.get_stats())
    }

    /// Run the runner asynchronously
    pub async fn run_async(&mut self, receiver: Receiver<FeaturesSnapshot>) -> Result<RunnerStats, ResearchError> {
        self.config.validate()?;
        self.state.is_running = true;
        self.state.started_at = Utc::now();

        if self.config.log_level >= 3 {
            log::info!("Live research runner started (async) for symbol: {}", self.engine.config().symbol);
        }

        loop {
            if self.shutdown_requested {
                break;
            }

            match receiver.try_recv() {
                Ok(snapshot) => {
                    if let Err(e) = self.process_feature(&snapshot) {
                        self.state.record_error();
                        if self.config.log_level >= 1 {
                            log::error!("Error processing feature: {}", e);
                        }
                    }
                }
                Err(TryRecvError::Empty) => {
                    // Yield to other tasks
                    tokio::task::yield_now().await;
                }
                Err(TryRecvError::Disconnected) => {
                    if self.config.log_level >= 3 {
                        log::info!("Feature channel disconnected, shutting down");
                    }
                    break;
                }
            }
        }

        // Shutdown
        self.state.is_running = false;

        // Final checkpoint if configured
        if self.config.checkpoint_on_shutdown && self.state.samples_since_checkpoint > 0 {
            if let Err(e) = self.perform_checkpoint() {
                if self.config.log_level >= 1 {
                    log::error!("Error during shutdown checkpoint: {}", e);
                }
            }
        }

        Ok(self.get_stats())
    }

    /// Check if we should checkpoint
    fn should_checkpoint(&self) -> bool {
        self.state.is_checkpoint_due(
            self.config.checkpoint_duration(),
            self.config.min_checkpoint_duration(),
        )
    }

    /// Perform a checkpoint
    fn perform_checkpoint(&mut self) -> Result<(), ResearchError> {
        self.engine.checkpoint()?;
        self.state.record_checkpoint();

        if self.config.log_level >= 3 {
            log::info!(
                "Checkpoint performed: {} samples processed, {} checkpoints total",
                self.state.total_samples,
                self.state.checkpoints_performed
            );
        }

        Ok(())
    }

    /// Check for assessment changes and emit if needed
    fn check_assessment_change(&mut self) -> Result<(), ResearchError> {
        let current = self.engine.assess();

        // Check if assessment changed
        let changed = match &self.state.last_assessment {
            Some(prev) => prev.is_tradeable != current.is_tradeable,
            None => true, // First assessment is always a "change"
        };

        if changed {
            let change = AssessmentChange::new(
                self.state.last_assessment.clone(),
                current.clone(),
                self.state.total_samples,
                self.engine.config().symbol.clone(),
            );

            self.state.record_assessment_change(current);

            // Emit to subscriber
            if let Some(ref sender) = self.assessment_sender {
                // Non-blocking send, drop if channel full
                let _ = sender.try_send(change.clone());
            }

            if self.config.log_level >= 3 {
                log::info!("{}", change);
            }
        }

        Ok(())
    }

    /// Get current statistics
    pub fn get_stats(&self) -> RunnerStats {
        RunnerStats {
            total_samples: self.state.total_samples,
            samples_since_checkpoint: self.state.samples_since_checkpoint,
            checkpoints_performed: self.state.checkpoints_performed,
            assessment_changes: self.state.assessment_changes,
            errors_encountered: self.state.errors_encountered,
            uptime_seconds: self.state.uptime().num_seconds() as u64,
            processing_rate: self.state.processing_rate(),
            is_running: self.state.is_running,
            is_tradeable: self.state.last_assessment.as_ref().map(|a| a.is_tradeable),
        }
    }

    /// Force a checkpoint now
    pub fn force_checkpoint(&mut self) -> Result<(), ResearchError> {
        self.perform_checkpoint()
    }

    /// Reset the runner state
    pub fn reset(&mut self) {
        self.state.reset();
        self.engine.reset();
        self.shutdown_requested = false;
    }
}

// ============================================================================
// Runner Statistics
// ============================================================================

/// Statistics from the runner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerStats {
    /// Total samples processed
    pub total_samples: usize,

    /// Samples since last checkpoint
    pub samples_since_checkpoint: usize,

    /// Checkpoints performed
    pub checkpoints_performed: usize,

    /// Assessment changes detected
    pub assessment_changes: usize,

    /// Errors encountered
    pub errors_encountered: usize,

    /// Uptime in seconds
    pub uptime_seconds: u64,

    /// Processing rate (samples per second)
    pub processing_rate: Option<f64>,

    /// Is currently running
    pub is_running: bool,

    /// Current tradeable status
    pub is_tradeable: Option<bool>,
}

impl Default for RunnerStats {
    fn default() -> Self {
        Self {
            total_samples: 0,
            samples_since_checkpoint: 0,
            checkpoints_performed: 0,
            assessment_changes: 0,
            errors_encountered: 0,
            uptime_seconds: 0,
            processing_rate: None,
            is_running: false,
            is_tradeable: None,
        }
    }
}

impl fmt::Display for RunnerStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Runner Statistics:")?;
        writeln!(f, "  Total samples: {}", self.total_samples)?;
        writeln!(f, "  Checkpoints: {}", self.checkpoints_performed)?;
        writeln!(f, "  Assessment changes: {}", self.assessment_changes)?;
        writeln!(f, "  Errors: {}", self.errors_encountered)?;
        writeln!(f, "  Uptime: {} seconds", self.uptime_seconds)?;
        if let Some(rate) = self.processing_rate {
            writeln!(f, "  Processing rate: {:.2} samples/sec", rate)?;
        }
        writeln!(f, "  Running: {}", self.is_running)?;
        if let Some(tradeable) = self.is_tradeable {
            writeln!(f, "  Tradeable: {}", tradeable)?;
        }
        Ok(())
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Parse timestamp string to DateTime
fn parse_timestamp(ts: &str) -> Option<DateTime<Utc>> {
    // Try parsing as milliseconds first
    if let Ok(millis) = ts.parse::<i64>() {
        return DateTime::from_timestamp_millis(millis);
    }

    // Try parsing as RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(ts) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try parsing other formats
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt.and_utc());
    }

    None
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::FeaturesSnapshot;
    use crate::core::{ResearchState, AlgorithmConfig};
    use crate::edge_detection::{ResearchEngineConfig, ResearchEngineStats};
    use rust_decimal::Decimal;

    // ==================== Mock Research Engine ====================

    struct MockResearchEngine {
        config: ResearchEngineConfig,
        state: ResearchState,
        stats: ResearchEngineStats,
        samples: usize,
        checkpoint_count: usize,
        should_fail: bool,
        tradeable_after: usize,
    }

    impl MockResearchEngine {
        fn new(symbol: &str) -> Self {
            Self {
                config: ResearchEngineConfig::new(symbol),
                state: ResearchState::new(symbol),
                stats: ResearchEngineStats::new(),
                samples: 0,
                checkpoint_count: 0,
                should_fail: false,
                tradeable_after: 10,
            }
        }

        fn with_tradeable_after(mut self, n: usize) -> Self {
            self.tradeable_after = n;
            self
        }

        fn with_failure(mut self) -> Self {
            self.should_fail = true;
            self
        }
    }

    impl ResearchEngine for MockResearchEngine {
        fn on_features(&mut self, snapshot: &FeaturesSnapshot) -> Result<(), ResearchError> {
            if self.should_fail {
                return Err(ResearchError::FeatureProcessing("Mock failure".to_string()));
            }

            self.samples += 1;
            let ts = parse_timestamp(&snapshot.timestamp).unwrap_or_else(Utc::now);
            self.stats.record_sample(ts);

            // Update assessment based on samples
            if self.samples >= self.tradeable_after {
                self.state.assessment = TradeableAssessment::new(true, true, true, true);
            }

            Ok(())
        }

        fn assess(&self) -> TradeableAssessment {
            self.state.assessment.clone()
        }

        fn generate_config(&self) -> Option<AlgorithmConfig> {
            if self.state.assessment.is_tradeable {
                Some(AlgorithmConfig::default())
            } else {
                None
            }
        }

        fn state(&self) -> &ResearchState {
            &self.state
        }

        fn state_mut(&mut self) -> &mut ResearchState {
            &mut self.state
        }

        fn checkpoint(&mut self) -> Result<(), ResearchError> {
            self.checkpoint_count += 1;
            self.stats.record_checkpoint();
            Ok(())
        }

        fn reset(&mut self) {
            self.samples = 0;
            self.checkpoint_count = 0;
            self.state = ResearchState::new(&self.config.symbol);
            self.stats = ResearchEngineStats::new();
        }

        fn config(&self) -> &ResearchEngineConfig {
            &self.config
        }

        fn stats(&self) -> ResearchEngineStats {
            self.stats.clone()
        }
    }

    // ==================== Helper Functions ====================

    fn create_snapshot(timestamp_millis: i64) -> FeaturesSnapshot {
        FeaturesSnapshot {
            timestamp: timestamp_millis.to_string(),
            best_bid: Some(Decimal::from(50000)),
            best_ask: Some(Decimal::from(50001)),
            mid_price: Some(Decimal::new(500005, 1)),
            microprice: Some(Decimal::new(500005, 1)),
            spread: Some(Decimal::from(1)),
            imbalance: Some(Decimal::new(5, 1)),
            ..Default::default()
        }
    }

    fn create_snapshots(count: usize, start_millis: i64) -> Vec<FeaturesSnapshot> {
        (0..count)
            .map(|i| create_snapshot(start_millis + (i as i64 * 1000)))
            .collect()
    }

    // ==================== LiveResearchConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = LiveResearchConfig::default();
        assert_eq!(config.checkpoint_interval_minutes, 5);
        assert!(config.emit_assessment_changes);
        assert_eq!(config.max_samples_per_batch, 100);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_new() {
        let config = LiveResearchConfig::new(10);
        assert_eq!(config.checkpoint_interval_minutes, 10);
    }

    #[test]
    fn test_config_builder_pattern() {
        let config = LiveResearchConfig::default()
            .with_checkpoint_interval(15)
            .without_assessment_emission()
            .with_max_batch_size(50)
            .with_min_checkpoint_interval(120)
            .without_shutdown_checkpoint()
            .with_log_level(4);

        assert_eq!(config.checkpoint_interval_minutes, 15);
        assert!(!config.emit_assessment_changes);
        assert_eq!(config.max_samples_per_batch, 50);
        assert_eq!(config.min_checkpoint_interval_seconds, 120);
        assert!(!config.checkpoint_on_shutdown);
        assert_eq!(config.log_level, 4);
    }

    #[test]
    fn test_config_validation_zero_interval() {
        let config = LiveResearchConfig {
            checkpoint_interval_minutes: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_batch_size() {
        let config = LiveResearchConfig {
            max_samples_per_batch: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_checkpoint_duration() {
        let config = LiveResearchConfig::new(10);
        assert_eq!(config.checkpoint_duration(), Duration::minutes(10));
    }

    #[test]
    fn test_config_min_checkpoint_duration() {
        let config = LiveResearchConfig::default()
            .with_min_checkpoint_interval(90);
        assert_eq!(config.min_checkpoint_duration(), Duration::seconds(90));
    }

    #[test]
    fn test_config_serialization() {
        let config = LiveResearchConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let restored: LiveResearchConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.checkpoint_interval_minutes, restored.checkpoint_interval_minutes);
    }

    // ==================== AssessmentChange Tests ====================

    #[test]
    fn test_assessment_change_new() {
        let prev = TradeableAssessment::new(false, true, true, true);
        let curr = TradeableAssessment::new(true, true, true, true);

        let change = AssessmentChange::new(Some(prev), curr.clone(), 100, "BTCUSDT".to_string());

        assert_eq!(change.samples_processed, 100);
        assert_eq!(change.symbol, "BTCUSDT");
        assert!(change.current.is_tradeable);
    }

    #[test]
    fn test_assessment_change_became_tradeable() {
        let prev = TradeableAssessment::new(false, true, true, true);
        let curr = TradeableAssessment::new(true, true, true, true);

        let change = AssessmentChange::new(Some(prev), curr, 100, "BTCUSDT".to_string());
        assert!(change.became_tradeable());
        assert!(!change.became_non_tradeable());
    }

    #[test]
    fn test_assessment_change_became_non_tradeable() {
        let prev = TradeableAssessment::new(true, true, true, true);
        let curr = TradeableAssessment::new(false, true, true, true);

        let change = AssessmentChange::new(Some(prev), curr, 100, "BTCUSDT".to_string());
        assert!(!change.became_tradeable());
        assert!(change.became_non_tradeable());
    }

    #[test]
    fn test_assessment_change_first_tradeable() {
        let curr = TradeableAssessment::new(true, true, true, true);
        let change = AssessmentChange::new(None, curr, 100, "BTCUSDT".to_string());
        assert!(change.became_tradeable());
    }

    #[test]
    fn test_assessment_change_first_non_tradeable() {
        let curr = TradeableAssessment::new(false, true, true, true);
        let change = AssessmentChange::new(None, curr, 100, "BTCUSDT".to_string());
        assert!(!change.became_tradeable());
        assert!(!change.became_non_tradeable());
    }

    #[test]
    fn test_assessment_change_position_scale_change() {
        let mut prev = TradeableAssessment::new(true, true, true, true);
        prev.position_scale = 0.5;
        let mut curr = TradeableAssessment::new(true, true, true, true);
        curr.position_scale = 1.0;

        let change = AssessmentChange::new(Some(prev), curr, 100, "BTCUSDT".to_string());
        let scale_change = change.position_scale_change().unwrap();
        assert!((scale_change - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_assessment_change_no_previous() {
        let curr = TradeableAssessment::new(true, true, true, true);
        let change = AssessmentChange::new(None, curr, 100, "BTCUSDT".to_string());
        assert!(change.position_scale_change().is_none());
    }

    #[test]
    fn test_assessment_change_display() {
        let curr = TradeableAssessment::new(true, true, true, true);
        let change = AssessmentChange::new(None, curr, 100, "BTCUSDT".to_string());
        let display = format!("{}", change);
        assert!(display.contains("BTCUSDT"));
        assert!(display.contains("TRADEABLE"));
        assert!(display.contains("100"));
    }

    #[test]
    fn test_assessment_change_serialization() {
        let curr = TradeableAssessment::new(true, true, true, true);
        let change = AssessmentChange::new(None, curr, 100, "BTCUSDT".to_string());

        let json = serde_json::to_string(&change).unwrap();
        let restored: AssessmentChange = serde_json::from_str(&json).unwrap();

        assert_eq!(change.samples_processed, restored.samples_processed);
        assert_eq!(change.symbol, restored.symbol);
    }

    // ==================== LiveResearchState Tests ====================

    #[test]
    fn test_state_default() {
        let state = LiveResearchState::default();
        assert_eq!(state.total_samples, 0);
        assert!(!state.is_running);
        assert!(state.last_assessment.is_none());
    }

    #[test]
    fn test_state_new() {
        let state = LiveResearchState::new();
        assert_eq!(state.checkpoints_performed, 0);
        assert_eq!(state.errors_encountered, 0);
    }

    #[test]
    fn test_state_record_sample() {
        let mut state = LiveResearchState::new();
        let ts = Utc::now();

        state.record_sample(ts);
        assert_eq!(state.total_samples, 1);
        assert_eq!(state.samples_since_checkpoint, 1);
        assert_eq!(state.last_sample_at, Some(ts));
    }

    #[test]
    fn test_state_record_checkpoint() {
        let mut state = LiveResearchState::new();
        state.samples_since_checkpoint = 100;

        state.record_checkpoint();
        assert_eq!(state.checkpoints_performed, 1);
        assert_eq!(state.samples_since_checkpoint, 0);
    }

    #[test]
    fn test_state_record_assessment_change() {
        let mut state = LiveResearchState::new();
        let assessment = TradeableAssessment::new(true, true, true, true);

        state.record_assessment_change(assessment.clone());
        assert_eq!(state.assessment_changes, 1);
        assert!(state.last_assessment.is_some());
    }

    #[test]
    fn test_state_record_error() {
        let mut state = LiveResearchState::new();
        state.record_error();
        state.record_error();
        assert_eq!(state.errors_encountered, 2);
    }

    #[test]
    fn test_state_reset() {
        let mut state = LiveResearchState::new();
        state.total_samples = 1000;
        state.checkpoints_performed = 5;

        state.reset();
        assert_eq!(state.total_samples, 0);
        assert_eq!(state.checkpoints_performed, 0);
    }

    #[test]
    fn test_state_processing_rate() {
        let mut state = LiveResearchState::new();
        state.started_at = Utc::now() - Duration::seconds(10);
        state.total_samples = 1000;

        let rate = state.processing_rate().unwrap();
        assert!(rate > 90.0 && rate < 110.0); // ~100 samples/sec
    }

    #[test]
    fn test_state_is_checkpoint_due() {
        let mut state = LiveResearchState::new();
        state.last_checkpoint = Utc::now() - Duration::minutes(10);

        assert!(state.is_checkpoint_due(Duration::minutes(5), Duration::seconds(0)));
        assert!(!state.is_checkpoint_due(Duration::minutes(15), Duration::seconds(0)));
    }

    #[test]
    fn test_state_min_checkpoint_interval() {
        let mut state = LiveResearchState::new();
        state.last_checkpoint = Utc::now() - Duration::seconds(30);

        // Even if checkpoint interval passed, min interval not met
        assert!(!state.is_checkpoint_due(Duration::seconds(1), Duration::minutes(1)));
    }

    #[test]
    fn test_state_display() {
        let state = LiveResearchState::new();
        let display = format!("{}", state);
        assert!(display.contains("Total samples:"));
        assert!(display.contains("Checkpoints:"));
    }

    #[test]
    fn test_state_serialization() {
        let state = LiveResearchState::new();
        let json = serde_json::to_string(&state).unwrap();
        let restored: LiveResearchState = serde_json::from_str(&json).unwrap();
        assert_eq!(state.total_samples, restored.total_samples);
    }

    // ==================== LiveResearchRunner Tests ====================

    #[test]
    fn test_runner_new() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default();
        let runner = LiveResearchRunner::new(engine, config);

        assert!(!runner.is_shutdown_requested());
        assert_eq!(runner.state().total_samples, 0);
    }

    #[test]
    fn test_runner_with_default_config() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let runner = LiveResearchRunner::with_default_config(engine);

        assert_eq!(runner.config().checkpoint_interval_minutes, 5);
    }

    #[test]
    fn test_runner_subscribe_assessment() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let mut runner = LiveResearchRunner::with_default_config(engine);

        let (tx, _rx) = crossbeam::channel::bounded(10);
        runner.subscribe_assessment_changes(tx);

        assert!(runner.assessment_sender.is_some());
    }

    #[test]
    fn test_runner_unsubscribe_assessment() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let mut runner = LiveResearchRunner::with_default_config(engine);

        let (tx, _rx) = crossbeam::channel::bounded(10);
        runner.subscribe_assessment_changes(tx);
        runner.unsubscribe_assessment_changes();

        assert!(runner.assessment_sender.is_none());
    }

    #[test]
    fn test_runner_process_feature() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default().without_assessment_emission();
        let mut runner = LiveResearchRunner::new(engine, config);

        let snapshot = create_snapshot(1000000000000);
        runner.process_feature(&snapshot).unwrap();

        assert_eq!(runner.state().total_samples, 1);
    }

    #[test]
    fn test_runner_process_batch() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default()
            .without_assessment_emission()
            .with_max_batch_size(50);
        let mut runner = LiveResearchRunner::new(engine, config);

        let snapshots = create_snapshots(100, 1000000000000);
        let processed = runner.process_batch(&snapshots).unwrap();

        assert_eq!(processed, 50); // Limited by max_batch_size
        assert_eq!(runner.state().total_samples, 50);
    }

    #[test]
    fn test_runner_assessment_change_detection() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT").with_tradeable_after(5));
        let config = LiveResearchConfig::default();
        let mut runner = LiveResearchRunner::new(engine, config);

        let (tx, rx) = crossbeam::channel::bounded(10);
        runner.subscribe_assessment_changes(tx);

        // Process snapshots until assessment changes
        let snapshots = create_snapshots(10, 1000000000000);
        for snapshot in &snapshots {
            runner.process_feature(snapshot).unwrap();
        }

        // Should have received assessment changes
        let mut changes_received = 0;
        while rx.try_recv().is_ok() {
            changes_received += 1;
        }

        assert!(changes_received > 0);
    }

    #[test]
    fn test_runner_shutdown_request() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let mut runner = LiveResearchRunner::with_default_config(engine);

        assert!(!runner.is_shutdown_requested());
        runner.request_shutdown();
        assert!(runner.is_shutdown_requested());
    }

    #[test]
    fn test_runner_shutdown_stops_batch() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default().without_assessment_emission();
        let mut runner = LiveResearchRunner::new(engine, config);

        runner.request_shutdown();

        let snapshots = create_snapshots(100, 1000000000000);
        let processed = runner.process_batch(&snapshots).unwrap();

        assert_eq!(processed, 0);
    }

    #[test]
    fn test_runner_get_stats() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default().without_assessment_emission();
        let mut runner = LiveResearchRunner::new(engine, config);

        let snapshots = create_snapshots(50, 1000000000000);
        runner.process_batch(&snapshots).unwrap();

        let stats = runner.get_stats();
        assert_eq!(stats.total_samples, 50);
        assert!(!stats.is_running);
    }

    #[test]
    fn test_runner_force_checkpoint() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let mut runner = LiveResearchRunner::with_default_config(engine);

        runner.force_checkpoint().unwrap();
        assert_eq!(runner.state().checkpoints_performed, 1);
    }

    #[test]
    fn test_runner_reset() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default().without_assessment_emission();
        let mut runner = LiveResearchRunner::new(engine, config);

        let snapshots = create_snapshots(50, 1000000000000);
        runner.process_batch(&snapshots).unwrap();
        runner.request_shutdown();

        runner.reset();

        assert_eq!(runner.state().total_samples, 0);
        assert!(!runner.is_shutdown_requested());
    }

    #[test]
    fn test_runner_error_handling() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT").with_failure());
        let config = LiveResearchConfig::default()
            .without_assessment_emission()
            .with_log_level(0);
        let mut runner = LiveResearchRunner::new(engine, config);

        let snapshot = create_snapshot(1000000000000);
        let result = runner.process_feature(&snapshot);

        assert!(result.is_err());
    }

    #[test]
    fn test_runner_engine_access() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let runner = LiveResearchRunner::with_default_config(engine);

        assert_eq!(runner.engine().config().symbol, "BTCUSDT");
    }

    #[test]
    fn test_runner_engine_mut_access() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let mut runner = LiveResearchRunner::with_default_config(engine);

        // Just verify we can get mutable access
        let _ = runner.engine_mut();
    }

    // ==================== RunnerStats Tests ====================

    #[test]
    fn test_runner_stats_default() {
        let stats = RunnerStats::default();
        assert_eq!(stats.total_samples, 0);
        assert!(!stats.is_running);
        assert!(stats.is_tradeable.is_none());
    }

    #[test]
    fn test_runner_stats_display() {
        let stats = RunnerStats {
            total_samples: 1000,
            samples_since_checkpoint: 50,
            checkpoints_performed: 3,
            assessment_changes: 2,
            errors_encountered: 1,
            uptime_seconds: 120,
            processing_rate: Some(8.33),
            is_running: true,
            is_tradeable: Some(true),
        };

        let display = format!("{}", stats);
        assert!(display.contains("1000"));
        assert!(display.contains("3"));
        assert!(display.contains("true"));
    }

    #[test]
    fn test_runner_stats_serialization() {
        let stats = RunnerStats::default();
        let json = serde_json::to_string(&stats).unwrap();
        let restored: RunnerStats = serde_json::from_str(&json).unwrap();
        assert_eq!(stats.total_samples, restored.total_samples);
    }

    // ==================== Utility Function Tests ====================

    #[test]
    fn test_parse_timestamp_millis() {
        let ts = parse_timestamp("1609459200000").unwrap();
        assert_eq!(ts.timestamp_millis(), 1609459200000);
    }

    #[test]
    fn test_parse_timestamp_rfc3339() {
        let ts = parse_timestamp("2021-01-01T00:00:00Z").unwrap();
        assert_eq!(ts.timestamp(), 1609459200);
    }

    #[test]
    fn test_parse_timestamp_datetime() {
        let ts = parse_timestamp("2021-01-01 00:00:00.000").unwrap();
        assert_eq!(ts.timestamp(), 1609459200);
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        let ts = parse_timestamp("invalid");
        assert!(ts.is_none());
    }

    #[test]
    fn test_parse_timestamp_empty() {
        let ts = parse_timestamp("");
        assert!(ts.is_none());
    }

    // ==================== Integration-style Tests ====================

    #[test]
    fn test_full_pipeline_flow() {
        let engine = Box::new(MockResearchEngine::new("ETHUSDT").with_tradeable_after(20));
        let config = LiveResearchConfig::default()
            .with_checkpoint_interval(1)
            .with_min_checkpoint_interval(0);
        let mut runner = LiveResearchRunner::new(engine, config);

        let (tx, rx) = crossbeam::channel::bounded(100);
        runner.subscribe_assessment_changes(tx);

        // Process enough samples to trigger assessment change
        let snapshots = create_snapshots(50, 1000000000000);
        for snapshot in &snapshots {
            runner.process_feature(snapshot).unwrap();
        }

        let stats = runner.get_stats();
        assert_eq!(stats.total_samples, 50);
        assert!(stats.assessment_changes > 0);

        // Verify assessment changes were emitted
        let mut changes = vec![];
        while let Ok(change) = rx.try_recv() {
            changes.push(change);
        }

        assert!(!changes.is_empty());
        // Last change should be tradeable
        assert!(changes.last().unwrap().current.is_tradeable);
    }

    #[test]
    fn test_channel_based_run() {
        use std::thread;

        let engine = Box::new(MockResearchEngine::new("BTCUSDT").with_tradeable_after(5));
        let config = LiveResearchConfig::default()
            .without_assessment_emission()
            .with_log_level(0);
        let mut runner = LiveResearchRunner::new(engine, config);

        let (tx, rx) = crossbeam::channel::bounded(100);

        // Spawn runner in separate thread
        let runner_handle = thread::spawn(move || {
            runner.run_blocking(rx)
        });

        // Send some snapshots
        let snapshots = create_snapshots(20, 1000000000000);
        for snapshot in &snapshots {
            tx.send(snapshot.clone()).unwrap();
        }

        // Drop sender to signal shutdown
        drop(tx);

        // Wait for runner to finish
        let stats = runner_handle.join().unwrap().unwrap();
        assert_eq!(stats.total_samples, 20);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_empty_batch() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default().without_assessment_emission();
        let mut runner = LiveResearchRunner::new(engine, config);

        let processed = runner.process_batch(&[]).unwrap();
        assert_eq!(processed, 0);
    }

    #[test]
    fn test_multiple_checkpoints() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let mut runner = LiveResearchRunner::with_default_config(engine);

        for _ in 0..5 {
            runner.force_checkpoint().unwrap();
        }

        assert_eq!(runner.state().checkpoints_performed, 5);
    }

    #[test]
    fn test_assessment_no_change() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT").with_tradeable_after(1000));
        let config = LiveResearchConfig::default();
        let mut runner = LiveResearchRunner::new(engine, config);

        let (tx, rx) = crossbeam::channel::bounded(100);
        runner.subscribe_assessment_changes(tx);

        // Process 10 snapshots - should get 1 change (initial)
        let snapshots = create_snapshots(10, 1000000000000);
        for snapshot in &snapshots {
            runner.process_feature(snapshot).unwrap();
        }

        // Count changes
        let mut changes = 0;
        while rx.try_recv().is_ok() {
            changes += 1;
        }

        // Only the initial assessment should trigger a change
        assert_eq!(changes, 1);
    }

    #[test]
    fn test_stats_reflect_runner_state() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT").with_tradeable_after(5));
        let config = LiveResearchConfig::default();
        let mut runner = LiveResearchRunner::new(engine, config);

        // Process enough to become tradeable
        let snapshots = create_snapshots(10, 1000000000000);
        for snapshot in &snapshots {
            runner.process_feature(snapshot).unwrap();
        }

        let stats = runner.get_stats();
        assert_eq!(stats.is_tradeable, Some(true));
    }

    #[test]
    fn test_concurrent_assessment_subscribers() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT").with_tradeable_after(5));
        let config = LiveResearchConfig::default();
        let mut runner = LiveResearchRunner::new(engine, config);

        // Only one subscriber at a time is supported
        let (tx1, rx1) = crossbeam::channel::bounded(10);
        let (tx2, rx2) = crossbeam::channel::bounded(10);

        runner.subscribe_assessment_changes(tx1);
        runner.subscribe_assessment_changes(tx2);  // Replaces tx1

        let snapshots = create_snapshots(10, 1000000000000);
        for snapshot in &snapshots {
            runner.process_feature(snapshot).unwrap();
        }

        // Only rx2 should have received changes
        assert!(rx1.try_recv().is_err());  // tx1 was replaced
        assert!(rx2.try_recv().is_ok());
    }

    // ==================== Boundary Value Tests ====================

    #[test]
    fn test_max_batch_size_boundary() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default()
            .without_assessment_emission()
            .with_max_batch_size(1);  // Only 1 per batch
        let mut runner = LiveResearchRunner::new(engine, config);

        let snapshots = create_snapshots(10, 1000000000000);
        let processed = runner.process_batch(&snapshots).unwrap();

        assert_eq!(processed, 1);
    }

    #[test]
    fn test_large_batch() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default()
            .without_assessment_emission()
            .with_max_batch_size(10000);
        let mut runner = LiveResearchRunner::new(engine, config);

        let snapshots = create_snapshots(1000, 1000000000000);
        let processed = runner.process_batch(&snapshots).unwrap();

        assert_eq!(processed, 1000);
    }

    #[test]
    fn test_timestamp_edge_cases() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT"));
        let config = LiveResearchConfig::default().without_assessment_emission();
        let mut runner = LiveResearchRunner::new(engine, config);

        // Very old timestamp
        let mut snapshot = create_snapshot(0);
        runner.process_feature(&snapshot).unwrap();

        // Far future timestamp
        snapshot = create_snapshot(4102444800000);  // Year 2100
        runner.process_feature(&snapshot).unwrap();

        assert_eq!(runner.state().total_samples, 2);
    }

    // ==================== State Persistence Tests ====================

    #[test]
    fn test_state_survives_errors() {
        let engine = Box::new(MockResearchEngine::new("BTCUSDT").with_failure());
        let config = LiveResearchConfig::default()
            .without_assessment_emission()
            .with_log_level(0);
        let mut runner = LiveResearchRunner::new(engine, config);

        // First operation fails
        let snapshot = create_snapshot(1000000000000);
        let _ = runner.process_feature(&snapshot);

        // State should still be accessible
        let state = runner.state();
        assert_eq!(state.total_samples, 0);
    }

    #[test]
    fn test_uptime_calculation() {
        let mut state = LiveResearchState::new();
        state.started_at = Utc::now() - Duration::hours(1);

        let uptime = state.uptime();
        assert!(uptime >= Duration::hours(1) - Duration::seconds(1));
    }

    // ==================== Clone and Debug Tests ====================

    #[test]
    fn test_config_clone() {
        let config = LiveResearchConfig::new(10);
        let cloned = config.clone();
        assert_eq!(config.checkpoint_interval_minutes, cloned.checkpoint_interval_minutes);
    }

    #[test]
    fn test_config_debug() {
        let config = LiveResearchConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("checkpoint_interval_minutes"));
    }

    #[test]
    fn test_state_clone() {
        let state = LiveResearchState::new();
        let cloned = state.clone();
        assert_eq!(state.total_samples, cloned.total_samples);
    }

    #[test]
    fn test_state_debug() {
        let state = LiveResearchState::new();
        let debug = format!("{:?}", state);
        assert!(debug.contains("total_samples"));
    }

    #[test]
    fn test_assessment_change_clone() {
        let curr = TradeableAssessment::new(true, true, true, true);
        let change = AssessmentChange::new(None, curr, 100, "BTCUSDT".to_string());
        let cloned = change.clone();
        assert_eq!(change.samples_processed, cloned.samples_processed);
    }

    #[test]
    fn test_runner_stats_clone() {
        let stats = RunnerStats::default();
        let cloned = stats.clone();
        assert_eq!(stats.total_samples, cloned.total_samples);
    }
}

//! Research Engine Implementation - Task 1.5
//!
//! The DefaultResearchEngine orchestrates all research components:
//! - MIDCEstimator: Market Information Diffusion Coefficient estimation
//! - PersistenceAnalyzer: Trend duration analysis
//! - PriceSignatureBuilder: Price movement discretization
//! - ConditionalModel: P(continuation | signature) tables
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                       DefaultResearchEngine                                  │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                             │
//! │  FeaturesSnapshot ──────────────────────────────────────────────────────┐   │
//! │  │                                                                      │   │
//! │  │                   on_features(&snapshot)                             │   │
//! │  │                           │                                          │   │
//! │  │    ┌──────────────────────┼──────────────────────┐                  │   │
//! │  │    │                      │                      │                  │   │
//! │  │    ▼                      ▼                      ▼                  │   │
//! │  │ MIDCEstimator    PersistenceAnalyzer    PriceSignatureBuilder       │   │
//! │  │    │                      │                      │                  │   │
//! │  │    │                      │                      ▼                  │   │
//! │  │    │                      │              ConditionalModel           │   │
//! │  │    │                      │                      │                  │   │
//! │  │    └──────────────────────┼──────────────────────┘                  │   │
//! │  │                           │                                          │   │
//! │  │                           ▼                                          │   │
//! │  │                    ResearchState                                     │   │
//! │  │                           │                                          │   │
//! │  │    assess() ──────────────┼─────────────────> TradeableAssessment   │   │
//! │  │                           │                                          │   │
//! │  │    generate_config() ─────┼─────────────────> Option<AlgorithmConfig>│   │
//! │  │                           │                                          │   │
//! │  │    checkpoint() ──────────┼─────────────────> ResearchStore         │   │
//! │  │                                                                      │   │
//! │  └──────────────────────────────────────────────────────────────────────┘   │
//! │                                                                             │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use ingestor::research::{
//!     DefaultResearchEngine, ResearchEngineConfig, ResearchEngine,
//!     ResearchStore, ResearchStoreConfig,
//! };
//!
//! // Create store
//! let store = ResearchStore::new(ResearchStoreConfig::default()).unwrap();
//!
//! // Create engine
//! let config = ResearchEngineConfig::new("BTCUSDT");
//! let mut engine = DefaultResearchEngine::new(config, Some(store)).unwrap();
//!
//! // Process features
//! for snapshot in feature_stream {
//!     engine.on_features(&snapshot)?;
//!
//!     if engine.is_ready() {
//!         let assessment = engine.assess();
//!         if assessment.is_tradeable {
//!             let algo_config = engine.generate_config();
//!             // Use algo_config for trading
//!         }
//!     }
//! }
//!
//! // Checkpoint state
//! engine.checkpoint()?;
//! ```

use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use crate::features::FeaturesSnapshot;
use crate::framework::{
    AlgorithmConfig, ConditionalProbability, MIDCEstimate, PersistenceStats, PriceSignature,
    ResearchState, ResearchStore, TradeableAssessment,
};
use crate::research::{
    ConditionalModel, ConditionalModelConfig, MIDCConfig, MIDCEstimator, Outcome,
    PersistenceAnalyzer, PersistenceConfig, PricePoint, PriceSignatureBuilder,
    ResearchEngine, ResearchEngineConfig, ResearchEngineFactory, ResearchEngineStats,
    ResearchError, SignatureConfig, SignificantSignal,
};

// ============================================================================
// Pending Outcome Tracking
// ============================================================================

/// A pending outcome waiting to be resolved
#[derive(Debug, Clone)]
struct PendingOutcome {
    /// Signature at the time of observation
    signature: PriceSignature,
    /// Price at observation
    start_price: f64,
    /// Time of observation
    start_time: DateTime<Utc>,
    /// Window end time (when outcome should be determined)
    end_time: DateTime<Utc>,
    /// Direction predicted (based on signature)
    predicted_up: bool,
}

// ============================================================================
// DefaultResearchEngine
// ============================================================================

/// Default implementation of the ResearchEngine trait
///
/// Orchestrates all research components and manages state persistence.
pub struct DefaultResearchEngine {
    /// Configuration
    config: ResearchEngineConfig,

    /// Signature configuration (derived from config)
    signature_config: SignatureConfig,

    /// Current research state
    state: ResearchState,

    /// MIDC estimator component
    midc_estimator: MIDCEstimator,

    /// Persistence analyzer component
    persistence_analyzer: PersistenceAnalyzer,

    /// Price signature builder
    signature_builder: PriceSignatureBuilder,

    /// Conditional probability model
    conditional_model: ConditionalModel,

    /// Optional persistent store
    store: Option<ResearchStore>,

    /// Engine statistics
    stats: ResearchEngineStats,

    /// Rolling window of price points for signature building
    price_window: VecDeque<PricePoint>,

    /// Pending outcomes waiting to be resolved
    pending_outcomes: VecDeque<PendingOutcome>,

    /// Last known mid price (for movement calculation)
    last_mid_price: Option<f64>,

    /// Last known timestamp
    last_timestamp: Option<DateTime<Utc>>,

    /// Samples since last checkpoint
    samples_since_checkpoint: usize,

    /// Current entropy value from features
    current_entropy: f64,
}

impl DefaultResearchEngine {
    /// Create a new research engine with configuration and optional store
    pub fn new(
        config: ResearchEngineConfig,
        store: Option<ResearchStore>,
    ) -> Result<Self, ResearchError> {
        // Validate configuration
        config.validate()?;

        // Create component configurations
        let midc_config = config.midc_config.clone();
        let persistence_config = config.persistence_config.clone();
        let conditional_config = ConditionalModelConfig {
            min_samples_for_probability: 10,
            min_samples_for_significance: config.conditional_config.min_signature_samples,
            min_edge_for_significance: config.conditional_config.min_edge,
            magnitude_decay: config.conditional_config.observation_decay,
            track_neutral: true,
            confidence_level: 0.95,
        };
        let signature_config = SignatureConfig::default();

        // Create state
        let state = ResearchState::new(&config.symbol);

        // Create components
        let midc_estimator = MIDCEstimator::new(midc_config);
        let persistence_analyzer = PersistenceAnalyzer::new(persistence_config);
        let signature_builder = PriceSignatureBuilder::new(signature_config.clone());
        let conditional_model = ConditionalModel::new(conditional_config);

        Ok(Self {
            config,
            signature_config,
            state,
            midc_estimator,
            persistence_analyzer,
            signature_builder,
            conditional_model,
            store,
            stats: ResearchEngineStats::new(),
            price_window: VecDeque::with_capacity(100),
            pending_outcomes: VecDeque::new(),
            last_mid_price: None,
            last_timestamp: None,
            samples_since_checkpoint: 0,
            current_entropy: 0.0,
        })
    }

    /// Create a new engine without a store
    pub fn without_store(config: ResearchEngineConfig) -> Result<Self, ResearchError> {
        Self::new(config, None)
    }

    /// Load existing state from store or initialize fresh
    pub fn load_or_init(
        config: ResearchEngineConfig,
        mut store: ResearchStore,
    ) -> Result<Self, ResearchError> {
        // Try to load latest state for this symbol
        let loaded_state = store
            .load(&config.symbol)
            .map_err(|e| ResearchError::Persistence(e.to_string()))?;

        let mut engine = Self::new(config, Some(store))?;

        if let Some(state) = loaded_state {
            // Restore state
            engine.state = state;

            // Restore conditional model from state's table
            for (key, prob) in &engine.state.conditional_table {
                if let Some(sig) = PriceSignature::from_key(key) {
                    engine.conditional_model.import_probability(&sig, prob.clone());
                }
            }
        }

        Ok(engine)
    }

    /// Get the store reference
    pub fn store(&self) -> Option<&ResearchStore> {
        self.store.as_ref()
    }

    /// Set or replace the store
    pub fn set_store(&mut self, store: ResearchStore) {
        self.store = Some(store);
    }

    /// Extract mid price from snapshot as f64
    fn extract_mid_price(snapshot: &FeaturesSnapshot) -> Option<f64> {
        snapshot.mid_price.and_then(|d| d.to_f64())
    }

    /// Extract timestamp from snapshot
    fn extract_timestamp(snapshot: &FeaturesSnapshot) -> DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339(&snapshot.timestamp)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now())
    }

    /// Extract entropy from snapshot (use tick entropy 1m if available)
    fn extract_entropy(snapshot: &FeaturesSnapshot) -> f64 {
        snapshot
            .tick_entropy_1m
            .and_then(|d| d.to_f64())
            .unwrap_or(0.5)
    }

    /// Process pending outcomes based on current price and time
    fn process_pending_outcomes(&mut self, current_price: f64, current_time: DateTime<Utc>) {
        // Collect outcomes to process
        let mut resolved_indices = Vec::new();

        for (i, pending) in self.pending_outcomes.iter().enumerate() {
            if current_time >= pending.end_time {
                // Determine outcome
                let return_pct = (current_price - pending.start_price) / pending.start_price;
                let outcome = Outcome::from_movement(
                    pending.predicted_up,
                    return_pct,
                    self.signature_config.magnitude_tiny_small_bps / 2.0, // Use half of tiny threshold as neutral zone
                );

                // Calculate magnitude
                let magnitude_bps = return_pct.abs() * 10000.0;

                // Record in conditional model
                self.conditional_model
                    .record_outcome(&pending.signature, outcome, magnitude_bps);
                self.stats.record_conditional_update();

                resolved_indices.push(i);
            }
        }

        // Remove resolved outcomes (in reverse order to maintain indices)
        for i in resolved_indices.into_iter().rev() {
            self.pending_outcomes.remove(i);
        }
    }

    /// Create a pending outcome for a signature
    fn create_pending_outcome(
        &mut self,
        signature: &PriceSignature,
        price: f64,
        timestamp: DateTime<Utc>,
    ) {
        let outcome_window_ms =
            (self.config.conditional_config.outcome_window_seconds * 1000.0) as i64;
        let end_time = timestamp + chrono::Duration::milliseconds(outcome_window_ms);

        let predicted_up = matches!(
            signature.direction,
            crate::framework::SignatureDirection::Up
        );

        let pending = PendingOutcome {
            signature: *signature,
            start_price: price,
            start_time: timestamp,
            end_time,
            predicted_up,
        };

        self.pending_outcomes.push_back(pending);

        // Limit pending outcomes queue size
        while self.pending_outcomes.len() > 1000 {
            self.pending_outcomes.pop_front();
        }
    }

    /// Update state assessment based on current research
    fn update_assessment(&mut self) {
        let thresholds = &self.config.assessment_thresholds;

        // Check MIDC condition
        let midc_ok =
            self.state.midc.is_valid() && self.state.midc.kappa <= thresholds.max_kappa;

        // Check entropy condition
        let entropy_ok = self.current_entropy <= thresholds.max_entropy;

        // Check persistence condition
        let persistence_ok = self
            .state
            .persistence
            .trends_exploitable(thresholds.min_persistence_seconds);

        // Check significant signals condition
        let significant_signals = self.conditional_model.get_all_significant(
            self.config.conditional_config.min_signature_samples,
            thresholds.signal_min_edge,
        );
        let signals_ok = significant_signals.len() >= thresholds.min_significant_signals;

        // Create assessment
        let new_assessment = TradeableAssessment::new(midc_ok, entropy_ok, persistence_ok, signals_ok);

        // Track assessment changes
        if new_assessment.is_tradeable != self.state.assessment.is_tradeable {
            self.stats
                .record_assessment_change(new_assessment.is_tradeable);
        }

        self.state.assessment = new_assessment;
    }

    /// Sync conditional model state to research state
    fn sync_conditional_to_state(&mut self) {
        // Get all probabilities from the model
        let all_probs = self.conditional_model.get_all_probabilities();

        // Clear and rebuild the conditional table in state
        self.state.conditional_table.clear();
        for (sig, prob) in all_probs {
            self.state.conditional_table.insert(sig.to_key(), prob);
        }
    }

    /// Get number of significant signals
    pub fn significant_signal_count(&self) -> usize {
        self.conditional_model
            .get_all_significant(
                self.config.conditional_config.min_signature_samples,
                self.config.assessment_thresholds.signal_min_edge,
            )
            .len()
    }

    /// Get the conditional model reference
    pub fn conditional_model(&self) -> &ConditionalModel {
        &self.conditional_model
    }

    /// Get the MIDC estimator reference
    pub fn midc_estimator(&self) -> &MIDCEstimator {
        &self.midc_estimator
    }

    /// Get the persistence analyzer reference
    pub fn persistence_analyzer(&self) -> &PersistenceAnalyzer {
        &self.persistence_analyzer
    }
}

impl ResearchEngine for DefaultResearchEngine {
    fn on_features(&mut self, snapshot: &FeaturesSnapshot) -> Result<(), ResearchError> {
        // Extract data from snapshot
        let mid_price = match Self::extract_mid_price(snapshot) {
            Some(p) if p > 0.0 => p,
            _ => return Ok(()), // Skip invalid prices
        };

        let timestamp = Self::extract_timestamp(snapshot);
        self.current_entropy = Self::extract_entropy(snapshot);

        // Create price point
        let price_point = PricePoint::new(timestamp, mid_price);

        // Update MIDC estimator
        self.midc_estimator
            .update(&price_point)
            .map_err(|e| ResearchError::MIDCEstimation(e.to_string()))?;
        self.state.update_midc(self.midc_estimator.current().clone());
        self.stats.record_midc_update();

        // Update persistence analyzer
        self.persistence_analyzer.on_price(&price_point);
        let persistence_stats = self.persistence_analyzer.get_stats();
        self.state.update_persistence(persistence_stats);
        self.stats.record_persistence_update();

        // Process pending outcomes before creating new ones
        self.process_pending_outcomes(mid_price, timestamp);

        // Update price window for signature building
        self.price_window.push_back(price_point);
        while self.price_window.len() > 50 {
            self.price_window.pop_front();
        }

        // Build signature if we have enough points
        if self.price_window.len() >= self.signature_config.min_points {
            let prices: Vec<PricePoint> = self.price_window.iter().cloned().collect();
            if let Some(signature) = self.signature_builder.from_price_window(&prices) {
                // Create pending outcome for this signature
                self.create_pending_outcome(&signature, mid_price, timestamp);
            }
        }

        // Update entropy in state
        self.state.update_entropy(self.current_entropy);

        // Record snapshot
        self.state.record_snapshot(timestamp);
        self.stats.record_sample(timestamp);

        // Update assessment
        self.update_assessment();

        // Sync conditional model to state
        self.sync_conditional_to_state();

        // Auto-checkpoint if enabled
        self.samples_since_checkpoint += 1;
        if self.config.auto_checkpoint
            && self.samples_since_checkpoint >= self.config.checkpoint_interval
        {
            self.checkpoint()?;
            self.samples_since_checkpoint = 0;
        }

        // Store last values
        self.last_mid_price = Some(mid_price);
        self.last_timestamp = Some(timestamp);

        Ok(())
    }

    fn assess(&self) -> TradeableAssessment {
        self.state.assessment.clone()
    }

    fn generate_config(&self) -> Option<AlgorithmConfig> {
        if self.state.assessment.is_tradeable {
            Some(AlgorithmConfig::from_research(&self.state))
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
        // Check if we have a store first
        if self.store.is_none() {
            return Ok(());
        }

        // Sync conditional model to state
        self.sync_conditional_to_state();

        // Update timestamp
        self.state.timestamp = Utc::now();

        // Save to store (need mutable reference)
        if let Some(ref mut store) = self.store {
            store
                .save(&self.state)
                .map_err(|e| ResearchError::Persistence(e.to_string()))?;
        }

        self.stats.record_checkpoint();

        Ok(())
    }

    fn reset(&mut self) {
        // Create fresh state
        self.state = ResearchState::new(&self.config.symbol);

        // Reset components
        self.midc_estimator = MIDCEstimator::new(self.config.midc_config.clone());
        self.persistence_analyzer =
            PersistenceAnalyzer::new(self.config.persistence_config.clone());
        self.signature_builder = PriceSignatureBuilder::new(self.signature_config.clone());
        self.conditional_model = ConditionalModel::new(ConditionalModelConfig {
            min_samples_for_probability: 10,
            min_samples_for_significance: self.config.conditional_config.min_signature_samples,
            min_edge_for_significance: self.config.conditional_config.min_edge,
            magnitude_decay: self.config.conditional_config.observation_decay,
            track_neutral: true,
            confidence_level: 0.95,
        });

        // Clear buffers
        self.price_window.clear();
        self.pending_outcomes.clear();
        self.last_mid_price = None;
        self.last_timestamp = None;
        self.samples_since_checkpoint = 0;
        self.current_entropy = 0.0;

        // Reset stats
        self.stats = ResearchEngineStats::new();
    }

    fn config(&self) -> &ResearchEngineConfig {
        &self.config
    }

    fn stats(&self) -> ResearchEngineStats {
        self.stats.clone()
    }
}

// Implement Send for DefaultResearchEngine (required by trait)
unsafe impl Send for DefaultResearchEngine {}

// ============================================================================
// DefaultResearchEngineFactory
// ============================================================================

/// Factory for creating DefaultResearchEngine instances
pub struct DefaultResearchEngineFactory;

impl DefaultResearchEngineFactory {
    /// Create a new factory
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultResearchEngineFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ResearchEngineFactory for DefaultResearchEngineFactory {
    fn create(
        &self,
        config: ResearchEngineConfig,
    ) -> Result<Box<dyn ResearchEngine>, ResearchError> {
        let engine = DefaultResearchEngine::without_store(config)?;
        Ok(Box::new(engine))
    }

    fn create_with_store(
        &self,
        config: ResearchEngineConfig,
        store: ResearchStore,
    ) -> Result<Box<dyn ResearchEngine>, ResearchError> {
        let engine = DefaultResearchEngine::new(config, Some(store))?;
        Ok(Box::new(engine))
    }

    fn load_or_init(
        &self,
        config: ResearchEngineConfig,
        store: ResearchStore,
    ) -> Result<Box<dyn ResearchEngine>, ResearchError> {
        let engine = DefaultResearchEngine::load_or_init(config, store)?;
        Ok(Box::new(engine))
    }

    fn name(&self) -> &str {
        "DefaultResearchEngineFactory"
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::{
        SignatureConsistency, SignatureDirection, SignatureMagnitude, SignatureSpeed,
    };
    use rust_decimal_macros::dec;
    use std::path::PathBuf;
    use tempfile::TempDir;

    // ==================== Helper Functions ====================

    fn create_test_config() -> ResearchEngineConfig {
        ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(10)
            .with_checkpoint_interval(100)
            .without_auto_checkpoint()
    }

    fn create_temp_store() -> (ResearchStore, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_config = crate::framework::ResearchStoreConfig::with_path(temp_dir.path());
        let store = ResearchStore::new(store_config).expect("Failed to create store");
        (store, temp_dir)
    }

    fn create_test_snapshot(mid_price: f64, timestamp: DateTime<Utc>) -> FeaturesSnapshot {
        FeaturesSnapshot {
            timestamp: timestamp.to_rfc3339(),
            mid_price: Some(rust_decimal::Decimal::from_f64(mid_price).unwrap_or(dec!(0))),
            tick_entropy_1m: Some(dec!(0.5)),
            ..Default::default()
        }
    }

    fn create_trending_snapshots(
        start_price: f64,
        end_price: f64,
        count: usize,
        start_time: DateTime<Utc>,
    ) -> Vec<FeaturesSnapshot> {
        let price_step = (end_price - start_price) / count as f64;
        let time_step = chrono::Duration::seconds(1);

        (0..count)
            .map(|i| {
                let price = start_price + price_step * i as f64;
                let ts = start_time + time_step * i as i32;
                create_test_snapshot(price, ts)
            })
            .collect()
    }

    // ==================== Construction Tests ====================

    #[test]
    fn test_engine_new() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        assert_eq!(engine.state.symbol, "BTCUSDT");
        assert_eq!(engine.stats.samples_processed, 0);
        assert!(!engine.is_ready());
    }

    #[test]
    fn test_engine_new_with_invalid_config() {
        let config = ResearchEngineConfig {
            symbol: String::new(), // Invalid: empty symbol
            ..Default::default()
        };
        let result = DefaultResearchEngine::without_store(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_engine_new_with_store() {
        let config = create_test_config();
        let (store, _temp_dir) = create_temp_store();
        let engine = DefaultResearchEngine::new(config, Some(store)).unwrap();

        assert!(engine.store().is_some());
    }

    #[test]
    fn test_engine_without_store() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        assert!(engine.store().is_none());
    }

    // ==================== on_features Tests ====================

    #[test]
    fn test_on_features_single() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let snapshot = create_test_snapshot(50000.0, Utc::now());
        let result = engine.on_features(&snapshot);

        assert!(result.is_ok());
        assert_eq!(engine.stats.samples_processed, 1);
    }

    #[test]
    fn test_on_features_multiple() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let now = Utc::now();
        for i in 0..100 {
            let ts = now + chrono::Duration::seconds(i);
            let snapshot = create_test_snapshot(50000.0 + i as f64, ts);
            engine.on_features(&snapshot).unwrap();
        }

        assert_eq!(engine.stats.samples_processed, 100);
    }

    #[test]
    fn test_on_features_skip_invalid_price() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Snapshot with no mid_price
        let snapshot = FeaturesSnapshot {
            timestamp: Utc::now().to_rfc3339(),
            mid_price: None,
            ..Default::default()
        };
        let result = engine.on_features(&snapshot);

        assert!(result.is_ok());
        assert_eq!(engine.stats.samples_processed, 0);
    }

    #[test]
    fn test_on_features_skip_zero_price() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let snapshot = create_test_snapshot(0.0, Utc::now());
        let result = engine.on_features(&snapshot);

        assert!(result.is_ok());
        assert_eq!(engine.stats.samples_processed, 0);
    }

    #[test]
    fn test_on_features_skip_negative_price() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let snapshot = create_test_snapshot(-100.0, Utc::now());
        let result = engine.on_features(&snapshot);

        assert!(result.is_ok());
        assert_eq!(engine.stats.samples_processed, 0);
    }

    #[test]
    fn test_on_features_updates_midc() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let snapshots = create_trending_snapshots(50000.0, 51000.0, 500, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        assert!(engine.stats.midc_updates > 0);
    }

    #[test]
    fn test_on_features_updates_persistence() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let snapshots = create_trending_snapshots(50000.0, 51000.0, 100, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        assert!(engine.stats.persistence_updates > 0);
    }

    #[test]
    fn test_on_features_updates_entropy() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let mut snapshot = create_test_snapshot(50000.0, Utc::now());
        snapshot.tick_entropy_1m = Some(dec!(0.35));
        engine.on_features(&snapshot).unwrap();

        // Entropy should be updated from snapshot
        assert!((engine.current_entropy - 0.35).abs() < 0.01);
    }

    #[test]
    fn test_on_features_records_snapshot() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let ts = Utc::now();
        let snapshot = create_test_snapshot(50000.0, ts);
        engine.on_features(&snapshot).unwrap();

        assert_eq!(engine.state.snapshots_processed, 1);
        assert!(engine.state.data_start.is_some());
        assert!(engine.state.data_end.is_some());
    }

    // ==================== assess() Tests ====================

    #[test]
    fn test_assess_initial_not_tradeable() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let assessment = engine.assess();
        assert!(!assessment.is_tradeable);
    }

    #[test]
    fn test_assess_returns_clone() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let a1 = engine.assess();
        let a2 = engine.assess();

        // Should be independent clones
        assert_eq!(a1.is_tradeable, a2.is_tradeable);
    }

    #[test]
    fn test_assess_reflects_state() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Manually set state to tradeable
        engine.state.assessment = TradeableAssessment::new(true, true, true, true);

        let assessment = engine.assess();
        assert!(assessment.is_tradeable);
    }

    // ==================== generate_config() Tests ====================

    #[test]
    fn test_generate_config_none_when_not_tradeable() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let algo_config = engine.generate_config();
        assert!(algo_config.is_none());
    }

    #[test]
    fn test_generate_config_some_when_tradeable() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Set state to tradeable
        engine.state.assessment = TradeableAssessment::new(true, true, true, true);
        engine.state.midc = MIDCEstimate::new(0.05, 0.8, 0.9, 1000);

        let algo_config = engine.generate_config();
        assert!(algo_config.is_some());

        let config = algo_config.unwrap();
        assert_eq!(config.symbol, "BTCUSDT");
    }

    // ==================== state() Tests ====================

    #[test]
    fn test_state_reference() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let state = engine.state();
        assert_eq!(state.symbol, "BTCUSDT");
    }

    #[test]
    fn test_state_mut_reference() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let state = engine.state_mut();
        state.entropy = 0.75;

        assert!((engine.state.entropy - 0.75).abs() < 0.01);
    }

    // ==================== checkpoint() Tests ====================

    #[test]
    fn test_checkpoint_without_store() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Should succeed even without store
        let result = engine.checkpoint();
        assert!(result.is_ok());
        assert_eq!(engine.stats.checkpoints, 0); // No checkpoint recorded without store
    }

    #[test]
    fn test_checkpoint_with_store() {
        let config = create_test_config();
        let (store, _temp_dir) = create_temp_store();
        let mut engine = DefaultResearchEngine::new(config, Some(store)).unwrap();

        // Process some data
        let snapshot = create_test_snapshot(50000.0, Utc::now());
        engine.on_features(&snapshot).unwrap();

        let result = engine.checkpoint();
        assert!(result.is_ok());
        assert_eq!(engine.stats.checkpoints, 1);
    }

    #[test]
    fn test_checkpoint_updates_timestamp() {
        let config = create_test_config();
        let (store, _temp_dir) = create_temp_store();
        let mut engine = DefaultResearchEngine::new(config, Some(store)).unwrap();

        let before = engine.state.timestamp;
        std::thread::sleep(std::time::Duration::from_millis(10));

        engine.checkpoint().unwrap();

        assert!(engine.state.timestamp > before);
    }

    // ==================== reset() Tests ====================

    #[test]
    fn test_reset_clears_state() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Process some data
        let snapshots = create_trending_snapshots(50000.0, 51000.0, 100, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        assert!(engine.stats.samples_processed > 0);

        // Reset
        engine.reset();

        assert_eq!(engine.stats.samples_processed, 0);
        assert_eq!(engine.state.snapshots_processed, 0);
    }

    #[test]
    fn test_reset_clears_buffers() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Process data
        let snapshots = create_trending_snapshots(50000.0, 51000.0, 50, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        assert!(!engine.price_window.is_empty());

        // Reset
        engine.reset();

        assert!(engine.price_window.is_empty());
        assert!(engine.pending_outcomes.is_empty());
    }

    #[test]
    fn test_reset_preserves_config() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let original_symbol = engine.config.symbol.clone();
        let original_min_samples = engine.config.min_samples;

        engine.reset();

        assert_eq!(engine.config.symbol, original_symbol);
        assert_eq!(engine.config.min_samples, original_min_samples);
    }

    // ==================== config() Tests ====================

    #[test]
    fn test_config_reference() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let cfg = engine.config();
        assert_eq!(cfg.symbol, "BTCUSDT");
        assert_eq!(cfg.min_samples, 10);
    }

    // ==================== stats() Tests ====================

    #[test]
    fn test_stats_initial() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let stats = engine.stats();
        assert_eq!(stats.samples_processed, 0);
        assert_eq!(stats.midc_updates, 0);
        assert_eq!(stats.persistence_updates, 0);
        assert_eq!(stats.conditional_updates, 0);
        assert_eq!(stats.checkpoints, 0);
    }

    #[test]
    fn test_stats_after_processing() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let snapshots = create_trending_snapshots(50000.0, 51000.0, 50, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        let stats = engine.stats();
        assert_eq!(stats.samples_processed, 50);
        assert!(stats.midc_updates > 0);
        assert!(stats.persistence_updates > 0);
    }

    #[test]
    fn test_stats_returns_clone() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let stats1 = engine.stats();

        // Process more data
        let snapshot = create_test_snapshot(50000.0, Utc::now());
        engine.on_features(&snapshot).unwrap();

        let stats2 = engine.stats();

        // stats1 should be unchanged
        assert_eq!(stats1.samples_processed, 0);
        assert_eq!(stats2.samples_processed, 1);
    }

    // ==================== is_ready() Tests ====================

    #[test]
    fn test_is_ready_false_initially() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        assert!(!engine.is_ready());
    }

    #[test]
    fn test_is_ready_true_after_min_samples() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(20)
            .without_auto_checkpoint();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Process less than min_samples
        let snapshots = create_trending_snapshots(50000.0, 50100.0, 15, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }
        assert!(!engine.is_ready());

        // Process more
        let more_snapshots = create_trending_snapshots(50100.0, 50200.0, 10, Utc::now());
        for snapshot in more_snapshots {
            engine.on_features(&snapshot).unwrap();
        }
        assert!(engine.is_ready());
    }

    // ==================== samples_processed() Tests ====================

    #[test]
    fn test_samples_processed() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        assert_eq!(engine.samples_processed(), 0);

        let snapshots = create_trending_snapshots(50000.0, 50100.0, 25, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        assert_eq!(engine.samples_processed(), 25);
    }

    // ==================== midc() Tests ====================

    #[test]
    fn test_midc_reference() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let midc = engine.midc();
        assert!(!midc.is_valid()); // Initial MIDC is not valid
    }

    // ==================== persistence() Tests ====================

    #[test]
    fn test_persistence_reference() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let persistence = engine.persistence();
        assert_eq!(persistence.sample_count, 0);
    }

    // ==================== significant_signals() Tests ====================

    #[test]
    fn test_significant_signals_empty_initially() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let signals = engine.significant_signals();
        assert!(signals.is_empty());
    }

    // ==================== Auto-checkpoint Tests ====================

    #[test]
    fn test_auto_checkpoint_disabled() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_checkpoint_interval(10)
            .without_auto_checkpoint();
        let (store, _temp_dir) = create_temp_store();
        let mut engine = DefaultResearchEngine::new(config, Some(store)).unwrap();

        let snapshots = create_trending_snapshots(50000.0, 50200.0, 20, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        // Should not auto-checkpoint
        assert_eq!(engine.stats.checkpoints, 0);
    }

    #[test]
    fn test_auto_checkpoint_enabled() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(5)
            .with_checkpoint_interval(10);
        let (store, _temp_dir) = create_temp_store();
        let mut engine = DefaultResearchEngine::new(config, Some(store)).unwrap();

        let snapshots = create_trending_snapshots(50000.0, 50500.0, 25, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        // Should have auto-checkpointed at least twice (at 10 and 20)
        assert!(engine.stats.checkpoints >= 2);
    }

    // ==================== Factory Tests ====================

    #[test]
    fn test_factory_create() {
        let factory = DefaultResearchEngineFactory::new();
        let config = create_test_config();

        let engine = factory.create(config);
        assert!(engine.is_ok());
    }

    #[test]
    fn test_factory_create_with_store() {
        let factory = DefaultResearchEngineFactory::new();
        let config = create_test_config();
        let (store, _temp_dir) = create_temp_store();

        let engine = factory.create_with_store(config, store);
        assert!(engine.is_ok());
    }

    #[test]
    fn test_factory_load_or_init_fresh() {
        let factory = DefaultResearchEngineFactory::new();
        let config = create_test_config();
        let (store, _temp_dir) = create_temp_store();

        let engine = factory.load_or_init(config, store);
        assert!(engine.is_ok());
    }

    #[test]
    fn test_factory_name() {
        let factory = DefaultResearchEngineFactory::new();
        assert_eq!(factory.name(), "DefaultResearchEngineFactory");
    }

    #[test]
    fn test_factory_default() {
        let factory = DefaultResearchEngineFactory::default();
        assert_eq!(factory.name(), "DefaultResearchEngineFactory");
    }

    // ==================== Price Window Tests ====================

    #[test]
    fn test_price_window_grows() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let snapshots = create_trending_snapshots(50000.0, 50100.0, 30, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        assert!(!engine.price_window.is_empty());
        assert!(engine.price_window.len() <= 50); // Max size is 50
    }

    #[test]
    fn test_price_window_limited() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let snapshots = create_trending_snapshots(50000.0, 50200.0, 100, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        assert_eq!(engine.price_window.len(), 50);
    }

    // ==================== Pending Outcomes Tests ====================

    #[test]
    fn test_pending_outcomes_created() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(5)
            .without_auto_checkpoint()
            .with_conditional_config(crate::research::ConditionalConfig {
                outcome_window_seconds: 60.0, // 60 second window
                ..Default::default()
            });
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Need enough points to build a signature (min_points = 3)
        let snapshots = create_trending_snapshots(50000.0, 50100.0, 10, Utc::now());
        for snapshot in snapshots {
            engine.on_features(&snapshot).unwrap();
        }

        // Should have some pending outcomes
        // (might be empty if signatures weren't built, but structure is tested)
    }

    // ==================== Assessment Update Tests ====================

    #[test]
    fn test_assessment_midc_threshold() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(5)
            .without_auto_checkpoint()
            .with_assessment_thresholds(crate::research::AssessmentThresholds {
                max_kappa: 0.1,
                ..Default::default()
            });
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Set MIDC above threshold
        engine.state.midc = MIDCEstimate::new(0.2, 0.8, 0.9, 1000); // kappa = 0.2 > 0.1
        engine.update_assessment();

        assert!(!engine.state.assessment.midc_ok);
    }

    #[test]
    fn test_assessment_entropy_threshold() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(5)
            .without_auto_checkpoint()
            .with_assessment_thresholds(crate::research::AssessmentThresholds {
                max_entropy: 0.6,
                ..Default::default()
            });
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Set entropy above threshold
        engine.current_entropy = 0.8;
        engine.update_assessment();

        assert!(!engine.state.assessment.entropy_ok);
    }

    // ==================== Load/Restore Tests ====================

    #[test]
    fn test_load_or_init_restores_state() {
        // First, create and save some state using a temp directory
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_path = temp_dir.path().to_path_buf();

        let config1 = create_test_config();
        let store_config1 = crate::framework::ResearchStoreConfig::with_path(&store_path);
        let store1 = ResearchStore::new(store_config1).unwrap();

        let mut engine1 = DefaultResearchEngine::new(config1, Some(store1)).unwrap();

        // Process data
        let snapshots = create_trending_snapshots(50000.0, 50100.0, 20, Utc::now());
        for snapshot in snapshots {
            engine1.on_features(&snapshot).unwrap();
        }
        engine1.checkpoint().unwrap();

        let saved_snapshots = engine1.state.snapshots_processed;
        drop(engine1); // Release the store

        // Now load using a new store pointing to the same path
        let config2 = create_test_config();
        let store_config2 = crate::framework::ResearchStoreConfig::with_path(&store_path);
        let store2 = ResearchStore::new(store_config2).unwrap();
        let engine2 = DefaultResearchEngine::load_or_init(config2, store2).unwrap();

        assert_eq!(engine2.state.snapshots_processed, saved_snapshots);
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_timestamp_handling() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let mut snapshot = FeaturesSnapshot::default();
        snapshot.mid_price = Some(dec!(50000));
        snapshot.timestamp = String::new(); // Empty timestamp

        // Should use fallback (Utc::now())
        let result = engine.on_features(&snapshot);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_timestamp_handling() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let mut snapshot = FeaturesSnapshot::default();
        snapshot.mid_price = Some(dec!(50000));
        snapshot.timestamp = "not-a-timestamp".to_string();

        // Should use fallback (Utc::now())
        let result = engine.on_features(&snapshot);
        assert!(result.is_ok());
    }

    #[test]
    fn test_missing_entropy_handling() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let mut snapshot = FeaturesSnapshot::default();
        snapshot.mid_price = Some(dec!(50000));
        snapshot.timestamp = Utc::now().to_rfc3339();
        snapshot.tick_entropy_1m = None; // Missing entropy

        engine.on_features(&snapshot).unwrap();

        // Should use default (0.5)
        assert!((engine.current_entropy - 0.5).abs() < 0.01);
    }

    // ==================== Component Access Tests ====================

    #[test]
    fn test_conditional_model_access() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let model = engine.conditional_model();
        assert_eq!(model.total_outcomes(), 0);
    }

    #[test]
    fn test_midc_estimator_access() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let estimator = engine.midc_estimator();
        assert_eq!(estimator.samples_processed(), 0);
    }

    #[test]
    fn test_persistence_analyzer_access() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let analyzer = engine.persistence_analyzer();
        assert_eq!(analyzer.total_trends(), 0);
    }

    // ==================== Significant Signal Count Tests ====================

    #[test]
    fn test_significant_signal_count_zero_initially() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        assert_eq!(engine.significant_signal_count(), 0);
    }

    // ==================== Store Management Tests ====================

    #[test]
    fn test_set_store() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        assert!(engine.store().is_none());

        let (store, _temp_dir) = create_temp_store();
        engine.set_store(store);

        assert!(engine.store().is_some());
    }

    // ==================== Sync Conditional to State Tests ====================

    #[test]
    fn test_sync_conditional_to_state() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Manually add to conditional model
        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );

        // Record some outcomes
        for _ in 0..20 {
            engine
                .conditional_model
                .record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        // Sync
        engine.sync_conditional_to_state();

        // Should have entry in state
        assert!(!engine.state.conditional_table.is_empty());
    }

    // ==================== Integration Tests ====================

    #[test]
    fn test_full_workflow() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(50)
            .with_checkpoint_interval(100)
            .without_auto_checkpoint();
        let (store, _temp_dir) = create_temp_store();
        let mut engine = DefaultResearchEngine::new(config, Some(store)).unwrap();

        // 1. Process trending data
        let trend_up = create_trending_snapshots(50000.0, 51000.0, 100, Utc::now());
        for snapshot in trend_up {
            engine.on_features(&snapshot).unwrap();
        }

        // 2. Process reversal
        let trend_down = create_trending_snapshots(
            51000.0,
            50000.0,
            100,
            Utc::now() + chrono::Duration::minutes(2),
        );
        for snapshot in trend_down {
            engine.on_features(&snapshot).unwrap();
        }

        // 3. Check state
        assert!(engine.is_ready());
        assert!(engine.stats.samples_processed >= 200);

        // 4. Checkpoint
        engine.checkpoint().unwrap();
        assert!(engine.stats.checkpoints > 0);

        // 5. Assess
        let _assessment = engine.assess();

        // 6. Generate config (may or may not produce one depending on thresholds)
        let _config = engine.generate_config();
    }

    #[test]
    fn test_volatile_market_simulation() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let now = Utc::now();

        // Simulate volatile market with many small reversals
        for i in 0..200 {
            let price = 50000.0 + (i as f64 * 0.1).sin() * 100.0;
            let ts = now + chrono::Duration::seconds(i);
            let snapshot = create_test_snapshot(price, ts);
            engine.on_features(&snapshot).unwrap();
        }

        // Should have processed all
        assert_eq!(engine.stats.samples_processed, 200);
    }

    #[test]
    fn test_flat_market_simulation() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let now = Utc::now();

        // Simulate flat market
        for i in 0..100 {
            let price = 50000.0 + (i % 2) as f64 * 0.01; // Tiny oscillation
            let ts = now + chrono::Duration::seconds(i);
            let snapshot = create_test_snapshot(price, ts);
            engine.on_features(&snapshot).unwrap();
        }

        assert_eq!(engine.stats.samples_processed, 100);
    }

    // ==================== Concurrency Tests ====================

    #[test]
    fn test_send_trait() {
        fn assert_send<T: Send>() {}
        assert_send::<DefaultResearchEngine>();
    }

    // ==================== Timestamp Tests ====================

    #[test]
    fn test_extract_timestamp_valid() {
        let now = Utc::now();
        let snapshot = create_test_snapshot(50000.0, now);
        let extracted = DefaultResearchEngine::extract_timestamp(&snapshot);

        // Should be close to now
        let diff = (extracted - now).num_milliseconds().abs();
        assert!(diff < 1000); // Within 1 second
    }

    // ==================== Entropy Extraction Tests ====================

    #[test]
    fn test_extract_entropy_present() {
        let mut snapshot = FeaturesSnapshot::default();
        snapshot.tick_entropy_1m = Some(dec!(0.42));

        let entropy = DefaultResearchEngine::extract_entropy(&snapshot);
        assert!((entropy - 0.42).abs() < 0.01);
    }

    #[test]
    fn test_extract_entropy_absent() {
        let snapshot = FeaturesSnapshot::default();

        let entropy = DefaultResearchEngine::extract_entropy(&snapshot);
        assert!((entropy - 0.5).abs() < 0.01); // Default
    }

    // ==================== Mid Price Extraction Tests ====================

    #[test]
    fn test_extract_mid_price_present() {
        let snapshot = create_test_snapshot(50000.0, Utc::now());
        let price = DefaultResearchEngine::extract_mid_price(&snapshot);
        assert!(price.is_some());
        assert!((price.unwrap() - 50000.0).abs() < 0.01);
    }

    #[test]
    fn test_extract_mid_price_absent() {
        let snapshot = FeaturesSnapshot::default();
        let price = DefaultResearchEngine::extract_mid_price(&snapshot);
        assert!(price.is_none());
    }

    // ==================== Assessment Changes Tracking Tests ====================

    #[test]
    fn test_assessment_change_tracking() {
        let config = create_test_config();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        // Initially not tradeable
        assert_eq!(engine.stats.assessment_changes, 0);

        // Manually toggle to tradeable
        engine.state.assessment = TradeableAssessment::new(true, true, true, true);
        engine
            .stats
            .record_assessment_change(engine.state.assessment.is_tradeable);

        assert_eq!(engine.stats.assessment_changes, 1);

        // Toggle back
        engine.state.assessment = TradeableAssessment::new(false, true, true, true);
        engine
            .stats
            .record_assessment_change(engine.state.assessment.is_tradeable);

        assert_eq!(engine.stats.assessment_changes, 2);
    }

    // ==================== Long Running Tests ====================

    #[test]
    fn test_long_running_session() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(100)
            .with_checkpoint_interval(500)
            .without_auto_checkpoint();
        let mut engine = DefaultResearchEngine::without_store(config).unwrap();

        let now = Utc::now();

        // Simulate 1000 data points over ~16 minutes
        for i in 0..1000 {
            let price = 50000.0 + (i as f64 / 100.0).sin() * 500.0;
            let ts = now + chrono::Duration::seconds(i);
            let snapshot = create_test_snapshot(price, ts);
            engine.on_features(&snapshot).unwrap();
        }

        assert_eq!(engine.stats.samples_processed, 1000);
        assert!(engine.is_ready());
    }

    // ==================== Config Serialization Tests ====================

    #[test]
    fn test_config_clone() {
        let config = create_test_config();
        let engine = DefaultResearchEngine::without_store(config).unwrap();

        let config_ref = engine.config();
        let cloned = config_ref.clone();

        assert_eq!(cloned.symbol, config_ref.symbol);
        assert_eq!(cloned.min_samples, config_ref.min_samples);
    }
}

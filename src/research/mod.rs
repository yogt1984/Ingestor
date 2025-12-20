//! Research Module - Task 1.0
//!
//! The Research Module provides the continuous research process that detects
//! mutual information between past features and future price movements.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                        RESEARCH ENGINE                                       │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                             │
//! │  FeaturesSnapshot (from data layer)                                         │
//! │  └── Real-time market microstructure features                               │
//! │                           │                                                 │
//! │                           ▼                                                 │
//! │  ┌─────────────────────────────────────────────────────────────────────┐   │
//! │  │                    ResearchEngine Trait                              │   │
//! │  ├─────────────────────────────────────────────────────────────────────┤   │
//! │  │  on_features() ────────────────────────────────────────────────┐    │   │
//! │  │  │                                                              │    │   │
//! │  │  ├── MIDCEstimator (Task 1.1)                                  │    │   │
//! │  │  │   └── Estimates Market Information Diffusion Coefficient     │    │   │
//! │  │  │                                                              │    │   │
//! │  │  ├── PersistenceAnalyzer (Task 1.2)                            │    │   │
//! │  │  │   └── Tracks trend duration distribution                     │    │   │
//! │  │  │                                                              │    │   │
//! │  │  ├── PriceSignature (Task 1.3)                                 │    │   │
//! │  │  │   └── Discretizes price movements                            │    │   │
//! │  │  │                                                              │    │   │
//! │  │  └── ConditionalModel (Task 1.4)                               │    │   │
//! │  │      └── P(continuation | signature) tables                     │    │   │
//! │  │                                                              │    │   │
//! │  │                                                              ▼    │   │
//! │  │  assess() ─────────────────────────────────────────────────────>  │   │
//! │  │      └── TradeableAssessment                                      │   │
//! │  │          ├── midc_ok: Is κ in favorable range?                    │   │
//! │  │          ├── entropy_ok: Is market predictable?                   │   │
//! │  │          ├── persistence_ok: Do trends last?                      │   │
//! │  │          └── signals_ok: Are there high-edge signals?             │   │
//! │  │                                                                   │   │
//! │  │  generate_config() ────────────────────────────────────────────>  │   │
//! │  │      └── Option<AlgorithmConfig>                                  │   │
//! │  │          └── Parameterized algorithm from research                │   │
//! │  │                                                                   │   │
//! │  │  checkpoint() ─────────────────────────────────────────────────>  │   │
//! │  │      └── ResearchStore persistence                                │   │
//! │  └─────────────────────────────────────────────────────────────────────┘   │
//! │                                                                             │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Components
//!
//! ## Traits (`traits.rs`)
//! - `ResearchEngine`: Core trait for research implementations
//! - `ResearchEngineFactory`: Factory for creating research engines
//! - Configuration structs for all research components
//!
//! ## Planned Components (Tasks 1.1-1.4)
//! - `MIDCEstimator`: Estimates Market Information Diffusion Coefficient
//! - `PersistenceAnalyzer`: Analyzes trend duration distributions
//! - `PriceSignature`: Discretizes price movements for conditional modeling
//! - `ConditionalModel`: Builds P(continuation | signature) tables
//!
//! # Usage Example
//!
//! ```rust,ignore
//! use ingestor::research::{
//!     ResearchEngine, ResearchEngineConfig,
//!     MIDCConfig, PersistenceConfig, ConditionalConfig,
//! };
//! use ingestor::features::FeaturesSnapshot;
//!
//! // Create configuration
//! let config = ResearchEngineConfig::new("BTCUSDT")
//!     .with_min_samples(500)
//!     .with_checkpoint_interval(1000);
//!
//! // Create engine (implementation-specific)
//! // let engine = MyResearchEngine::new(config);
//!
//! // Process features
//! // for snapshot in feature_stream {
//! //     engine.on_features(&snapshot)?;
//! //     if engine.is_ready() {
//! //         let assessment = engine.assess();
//! //         if assessment.is_tradeable {
//! //             let config = engine.generate_config();
//! //             // Create algorithm from config
//! //         }
//! //     }
//! // }
//! ```
//!
//! # Integration with Framework
//!
//! The research module integrates with the framework module for persistence:
//!
//! - `ResearchState` (from framework) is the persistent state object
//! - `ResearchStore` (from framework) provides storage
//! - `AlgorithmConfig` (from framework) is generated from research findings
//! - `TradeableAssessment` (from framework) is the assessment output
//!
//! # Academic Foundation
//!
//! The research module implements concepts from:
//!
//! - **MIDC Estimation**: Based on market microstructure theory
//!   - Information diffuses through prices over time
//!   - κ (kappa) measures diffusion rate
//!   - τ_half = ln(2)/κ is the predictability horizon
//!
//! - **Trend Persistence**: Duration analysis of price trends
//!   - Empirical distribution of trend durations
//!   - Regime-segmented analysis
//!
//! - **Conditional Probability**: P(continuation | signature)
//!   - Price signature discretization (magnitude, speed, direction, consistency)
//!   - Empirical conditional probability tables
//!   - Edge detection over random (50%)
//!
//! References:
//! - Moskowitz, Ooi, Pedersen (2012): "Time Series Momentum"
//! - Baltas & Kosowski (2013): "Momentum Strategies in Futures Markets"

pub mod traits;
pub mod midc_estimator;
pub mod persistence_analyzer;
pub mod price_signature;

// Re-export core trait and types
pub use traits::{
    // Core trait
    ResearchEngine,
    ResearchEngineFactory,

    // Configuration
    ResearchEngineConfig,
    MIDCConfig,
    PersistenceConfig,
    ConditionalConfig,
    AssessmentThresholds,

    // Statistics and signals
    ResearchEngineStats,
    SignificantSignal,

    // Utility types
    PricePoint,
    Outcome,

    // Error type
    ResearchError,
};

// Re-export MIDC estimator (Task 1.1)
pub use midc_estimator::{MIDCEstimator, MIDCEstimatorBuilder, MIDCEstimatorStats};

// Re-export Persistence analyzer (Task 1.2)
pub use persistence_analyzer::{
    PersistenceAnalyzer, PersistenceAnalyzerBuilder, PersistenceAnalyzerStats,
    TrendDirection, CompletedTrend,
};

// Re-export Price signature builder (Task 1.3)
pub use price_signature::{
    SignatureConfig, PriceSignatureBuilder, SignatureWithMetrics, PriceSignatureBuilderStats,
};

// Re-export framework types used by research module
// These are the data structures that research produces/consumes
pub use crate::framework::{
    // State types (produced by research)
    ResearchState,
    MIDCEstimate,
    MIDCRegime,
    PersistenceStats,
    PriceSignature,
    SignatureMagnitude,
    SignatureSpeed,
    SignatureDirection,
    SignatureConsistency,
    ConditionalProbability,
    TradeableAssessment,
    RecommendedStrategy,

    // Store types (for persistence)
    ResearchStore,
    ResearchStoreConfig,

    // Config types (output of research)
    AlgorithmConfig,
    AlgorithmConfigBuilder,
};

// ============================================================================
// Module-level Integration Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    // ==================== Module Structure Tests ====================

    #[test]
    fn test_module_exports_research_engine_trait() {
        // Verify that the ResearchEngine trait is accessible
        fn _takes_research_engine<T: ResearchEngine>(_: T) {}
    }

    #[test]
    fn test_module_exports_config_types() {
        let config = ResearchEngineConfig::default();
        assert!(!config.symbol.is_empty());
    }

    #[test]
    fn test_module_exports_midc_config() {
        let config = MIDCConfig::default();
        assert!(config.rolling_window > 0);
    }

    #[test]
    fn test_module_exports_persistence_config() {
        let config = PersistenceConfig::default();
        assert!(config.min_move_bps > 0.0);
    }

    #[test]
    fn test_module_exports_conditional_config() {
        let config = ConditionalConfig::default();
        assert!(config.min_signature_samples > 0);
    }

    #[test]
    fn test_module_exports_assessment_thresholds() {
        let thresholds = AssessmentThresholds::default();
        assert!(thresholds.max_kappa > 0.0);
    }

    #[test]
    fn test_module_exports_stats() {
        let stats = ResearchEngineStats::new();
        assert_eq!(stats.samples_processed, 0);
    }

    #[test]
    fn test_module_exports_price_point() {
        let pp = PricePoint::new(Utc::now(), 100.0);
        assert_eq!(pp.price, 100.0);
    }

    #[test]
    fn test_module_exports_outcome() {
        let outcome = Outcome::Continuation;
        assert_eq!(outcome, Outcome::Continuation);
    }

    #[test]
    fn test_module_exports_research_error() {
        let err = ResearchError::Persistence("test".to_string());
        assert!(format!("{}", err).contains("test"));
    }

    // ==================== Framework Integration Tests ====================

    #[test]
    fn test_module_exports_research_state() {
        let state = ResearchState::new("BTCUSDT");
        assert_eq!(state.symbol, "BTCUSDT");
    }

    #[test]
    fn test_module_exports_midc_estimate() {
        let midc = MIDCEstimate::new(0.05, 0.8, 0.9, 1000);
        assert!(midc.is_valid());
    }

    #[test]
    fn test_module_exports_midc_regime() {
        let regime = MIDCRegime::SlowDiffusion;
        assert!(regime.momentum_viable());
    }

    #[test]
    fn test_module_exports_persistence_stats() {
        let stats = PersistenceStats::default();
        assert_eq!(stats.sample_count, 0);
    }

    #[test]
    fn test_module_exports_price_signature() {
        // PriceSignature requires proper construction - returns Option
        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );
        // PriceSignature::new returns PriceSignature directly, not Option
        assert_eq!(sig.magnitude, SignatureMagnitude::Medium);
    }

    #[test]
    fn test_module_exports_conditional_probability() {
        let prob = ConditionalProbability::default();
        assert_eq!(prob.p_continuation, 0.5);
    }

    #[test]
    fn test_module_exports_tradeable_assessment() {
        let assessment = TradeableAssessment::new(true, true, true, true);
        assert!(assessment.is_tradeable);
    }

    #[test]
    fn test_module_exports_recommended_strategy() {
        let strategy = RecommendedStrategy::Momentum;
        assert_eq!(strategy, RecommendedStrategy::Momentum);
    }

    #[test]
    fn test_module_exports_research_store_config() {
        let config = ResearchStoreConfig::default();
        // Just verify it compiles and has expected behavior
        let _ = config;
    }

    #[test]
    fn test_module_exports_algorithm_config() {
        let config = AlgorithmConfig::default();
        assert!(!config.id.is_empty());
    }

    #[test]
    fn test_module_exports_algorithm_config_builder() {
        let builder = AlgorithmConfigBuilder::new("TestAlgo", "BTCUSDT");
        let config = builder.build();
        assert!(config.is_ok());
    }

    // ==================== Type Interaction Tests ====================

    #[test]
    fn test_research_state_with_midc_estimate() {
        let mut state = ResearchState::new("BTCUSDT");
        let midc = MIDCEstimate::new(0.05, 0.8, 0.9, 1000);
        state.update_midc(midc);
        assert!(state.midc.is_valid());
    }

    #[test]
    fn test_research_state_with_persistence_stats() {
        let mut state = ResearchState::new("BTCUSDT");
        let stats = PersistenceStats {
            mean_duration_seconds: 60.0,
            median_duration_seconds: 45.0,
            std_duration_seconds: 20.0,
            percentile_25: 30.0,
            percentile_75: 90.0,
            sample_count: 100,
            updated_at: Utc::now(),
        };
        state.update_persistence(stats);
        assert_eq!(state.persistence.sample_count, 100);
    }

    #[test]
    fn test_research_state_with_conditional_probability() {
        let mut state = ResearchState::new("BTCUSDT");
        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );
        let prob = ConditionalProbability {
            p_continuation: 0.65,
            p_reversal: 0.35,
            expected_magnitude_bps: 15.0,
            std_magnitude_bps: 8.0,
            sample_count: 50,
            confidence_interval: (0.55, 0.75),
        };
        state.update_conditional(&sig, prob);
        assert!(!state.conditional_table.is_empty());
    }

    #[test]
    fn test_algorithm_config_from_research_state() {
        let mut state = ResearchState::new("BTCUSDT");
        state.midc = MIDCEstimate::new(0.05, 0.8, 0.9, 1000);
        state.persistence = PersistenceStats {
            mean_duration_seconds: 60.0,
            median_duration_seconds: 45.0,
            std_duration_seconds: 20.0,
            percentile_25: 30.0,
            percentile_75: 90.0,
            sample_count: 100,
            updated_at: Utc::now(),
        };
        state.assessment = TradeableAssessment::new(true, true, true, true);

        let config = AlgorithmConfig::from_research(&state);
        assert_eq!(config.symbol, "BTCUSDT");
    }

    #[test]
    fn test_price_point_calculations() {
        let now = Utc::now();
        let p1 = PricePoint::new(now, 100.0);
        let p2 = PricePoint::new(now + chrono::Duration::seconds(60), 105.0);

        let return_pct = p1.return_to(&p2);
        assert!((return_pct - 0.05).abs() < 0.0001);

        let return_bps = p1.return_bps_to(&p2);
        assert!((return_bps - 500.0).abs() < 0.1);

        let seconds = p1.seconds_to(&p2);
        assert!((seconds - 60.0).abs() < 0.001);
    }

    #[test]
    fn test_outcome_from_movement() {
        // Continuation: predicted up, actual up
        let outcome = Outcome::from_movement(true, 0.01, 5.0);
        assert_eq!(outcome, Outcome::Continuation);

        // Reversal: predicted up, actual down
        let outcome = Outcome::from_movement(true, -0.01, 5.0);
        assert_eq!(outcome, Outcome::Reversal);

        // Neutral: within threshold
        let outcome = Outcome::from_movement(true, 0.0001, 5.0);
        assert_eq!(outcome, Outcome::Neutral);
    }

    // ==================== Config Validation Tests ====================

    #[test]
    fn test_full_config_validation_chain() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(100)
            .with_checkpoint_interval(500)
            .with_midc_config(MIDCConfig::default())
            .with_persistence_config(PersistenceConfig::default())
            .with_conditional_config(ConditionalConfig::default())
            .with_assessment_thresholds(AssessmentThresholds::default());

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_fails_with_invalid_midc() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_midc_config(MIDCConfig {
                rolling_window: 5, // Invalid: too small
                ..Default::default()
            });

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_fails_with_invalid_persistence() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_persistence_config(PersistenceConfig {
                min_move_bps: 0.0, // Invalid: must be > 0
                ..Default::default()
            });

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_fails_with_invalid_conditional() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_conditional_config(ConditionalConfig {
                min_edge: 0.6, // Invalid: > 0.5
                ..Default::default()
            });

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_fails_with_invalid_thresholds() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_assessment_thresholds(AssessmentThresholds {
                max_entropy: 1.5, // Invalid: > 1.0
                ..Default::default()
            });

        assert!(config.validate().is_err());
    }

    // ==================== Stats Tracking Tests ====================

    #[test]
    fn test_stats_comprehensive_tracking() {
        let mut stats = ResearchEngineStats::new();
        let start = Utc::now();

        // Record samples
        for i in 0..100 {
            let ts = start + chrono::Duration::milliseconds(i * 10);
            stats.record_sample(ts);
        }

        assert_eq!(stats.samples_processed, 100);
        assert_eq!(stats.first_sample_at, Some(start));

        // Record updates
        for _ in 0..10 {
            stats.record_midc_update();
        }
        assert_eq!(stats.midc_updates, 10);

        for _ in 0..5 {
            stats.record_persistence_update();
        }
        assert_eq!(stats.persistence_updates, 5);

        for _ in 0..20 {
            stats.record_conditional_update();
        }
        assert_eq!(stats.conditional_updates, 20);

        // Record checkpoints
        stats.record_checkpoint();
        assert_eq!(stats.checkpoints, 1);
        assert!(stats.last_checkpoint.is_some());

        // Record assessment changes
        stats.record_assessment_change(true);
        stats.record_assessment_change(true); // No change
        stats.record_assessment_change(false);
        assert_eq!(stats.assessment_changes, 2);
    }

    #[test]
    fn test_stats_processing_rate() {
        let mut stats = ResearchEngineStats::new();
        let start = Utc::now();

        // 1000 samples over 10 seconds = 100 samples/sec
        for i in 0..1000 {
            let ts = start + chrono::Duration::milliseconds(i * 10);
            stats.record_sample(ts);
        }

        let rate = stats.samples_per_second().unwrap();
        assert!(rate > 99.0 && rate < 101.0);
    }

    // ==================== Significant Signal Tests ====================

    #[test]
    fn test_significant_signal_metrics() {
        let signal = SignificantSignal {
            signature_key: "Medium_Normal_Up_Smooth".to_string(),
            probability: ConditionalProbability {
                p_continuation: 0.65,
                p_reversal: 0.35,
                expected_magnitude_bps: 20.0,
                std_magnitude_bps: 10.0,
                sample_count: 100,
                confidence_interval: (0.55, 0.75),
            },
            edge: 0.15,
        };

        // Expected value = 20 * 0.15 * 2 = 6 bps
        assert!((signal.expected_value_bps() - 6.0).abs() < 0.001);

        // Quality = 0.15 * sqrt(100) = 1.5
        assert!((signal.quality_score() - 1.5).abs() < 0.001);
    }

    // ==================== Error Type Tests ====================

    #[test]
    fn test_research_error_variants() {
        let errors = vec![
            ResearchError::Persistence("persist failed".to_string()),
            ResearchError::FeatureProcessing("feature failed".to_string()),
            ResearchError::MIDCEstimation("midc failed".to_string()),
            ResearchError::PersistenceAnalysis("analysis failed".to_string()),
            ResearchError::ConditionalModel("model failed".to_string()),
            ResearchError::Configuration("config failed".to_string()),
            ResearchError::InsufficientData {
                message: "need more".to_string(),
                required: 100,
                available: 50,
            },
            ResearchError::StoreUnavailable("store down".to_string()),
        ];

        for err in errors {
            let display = format!("{}", err);
            assert!(!display.is_empty());
        }
    }

    // ==================== Cross-Module Integration ====================

    #[test]
    fn test_research_to_framework_flow() {
        // Simulate the research → framework flow

        // 1. Create research state
        let mut state = ResearchState::new("BTCUSDT");

        // 2. Update with MIDC (slow diffusion = momentum viable)
        state.midc = MIDCEstimate::new(0.005, 0.8, 0.9, 1000);

        // 3. Update persistence stats
        state.persistence = PersistenceStats {
            mean_duration_seconds: 60.0,
            median_duration_seconds: 45.0,
            std_duration_seconds: 20.0,
            percentile_25: 30.0,
            percentile_75: 90.0,
            sample_count: 100,
            updated_at: Utc::now(),
        };

        // 4. Update conditional table
        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );
        let prob = ConditionalProbability {
            p_continuation: 0.65,
            p_reversal: 0.35,
            expected_magnitude_bps: 15.0,
            std_magnitude_bps: 8.0,
            sample_count: 50,
            confidence_interval: (0.55, 0.75),
        };
        state.update_conditional(&sig, prob);

        // 5. Update assessment
        state.entropy = 0.4;
        state.assessment = TradeableAssessment::new(true, true, true, true);

        // 6. Generate algorithm config
        let config = AlgorithmConfig::from_research(&state);

        // Verify the flow worked
        assert_eq!(config.symbol, "BTCUSDT");
        assert!(state.assessment.is_tradeable);
        assert!(state.midc.regime().momentum_viable());
    }

    #[test]
    fn test_price_signature_to_key_roundtrip() {
        let sig = PriceSignature::new(
            SignatureMagnitude::Large,
            SignatureSpeed::Fast,
            SignatureDirection::Down,
            SignatureConsistency::Choppy,
        );

        let key = sig.to_key();
        assert!(!key.is_empty());
        // Key should contain identifiable components
        assert!(key.contains("Large") || key.contains("Fast") || key.contains("Down"));
    }

    #[test]
    fn test_conditional_probability_edge_calculation() {
        let prob = ConditionalProbability {
            p_continuation: 0.65,
            p_reversal: 0.35,
            expected_magnitude_bps: 15.0,
            std_magnitude_bps: 8.0,
            sample_count: 50,
            confidence_interval: (0.55, 0.75),
        };

        let edge = prob.edge();
        assert!((edge - 0.15).abs() < 0.0001);

        assert!(prob.is_significant(0.1, 30)); // Edge > 0.1, samples > 30
        assert!(!prob.is_significant(0.2, 30)); // Edge < 0.2
        assert!(!prob.is_significant(0.1, 100)); // Samples < 100
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn test_config_json_roundtrip() {
        let config = ResearchEngineConfig::new("BTCUSDT")
            .with_min_samples(200)
            .with_checkpoint_interval(500);

        let json = serde_json::to_string(&config).unwrap();
        let restored: ResearchEngineConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.symbol, restored.symbol);
        assert_eq!(config.min_samples, restored.min_samples);
        assert_eq!(config.checkpoint_interval, restored.checkpoint_interval);
    }

    #[test]
    fn test_stats_json_roundtrip() {
        let mut stats = ResearchEngineStats::new();
        stats.samples_processed = 1000;
        stats.midc_updates = 50;
        stats.record_checkpoint();

        let json = serde_json::to_string(&stats).unwrap();
        let restored: ResearchEngineStats = serde_json::from_str(&json).unwrap();

        assert_eq!(stats.samples_processed, restored.samples_processed);
        assert_eq!(stats.midc_updates, restored.midc_updates);
    }

    #[test]
    fn test_significant_signal_json_roundtrip() {
        let signal = SignificantSignal {
            signature_key: "test_key".to_string(),
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

        let json = serde_json::to_string(&signal).unwrap();
        let restored: SignificantSignal = serde_json::from_str(&json).unwrap();

        assert_eq!(signal.signature_key, restored.signature_key);
        assert_eq!(signal.edge, restored.edge);
    }

    #[test]
    fn test_price_point_json_roundtrip() {
        let pp = PricePoint::with_volume(Utc::now(), 50000.0, 1.5);

        let json = serde_json::to_string(&pp).unwrap();
        let restored: PricePoint = serde_json::from_str(&json).unwrap();

        assert_eq!(pp.price, restored.price);
        assert_eq!(pp.volume, restored.volume);
    }

    #[test]
    fn test_outcome_json_roundtrip() {
        for outcome in [Outcome::Continuation, Outcome::Reversal, Outcome::Neutral] {
            let json = serde_json::to_string(&outcome).unwrap();
            let restored: Outcome = serde_json::from_str(&json).unwrap();
            assert_eq!(outcome, restored);
        }
    }
}

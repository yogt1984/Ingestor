//! Framework Module
//!
//! Core persistent infrastructure for the MARS trading system.
//! This module provides:
//! - ResearchState: Unified research findings (Task 0.0)
//! - ResearchStore: Persistence layer for research state (Task 0.1)
//! - ValidationResult: Unified validation outcomes (Task 0.2)
//! - ResultsStore: Persistence layer for validation results (Task 0.3)
//! - AlgorithmConfig: Parameterized algorithm configuration (Task 0.4)
//! - ConfigStore: Persistence layer for algorithm configs (Task 0.5)
//! - Integration Tests: Cross-store interaction tests (Task 0.6)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                     FRAMEWORK PERSISTENCE LAYER                      │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │                                                                     │
//! │  ResearchStore ─────────────────────────────────────────────────┐   │
//! │  └── Persists ResearchState (MIDC, signatures, assessments)     │   │
//! │                           │                                     │   │
//! │                           ▼ source_research_id                  │   │
//! │  ConfigStore ──────────────────────────────────────────────────┤   │
//! │  └── Persists AlgorithmConfig (params derived from research)   │   │
//! │                           │                                     │   │
//! │                           ▼ config_id                           │   │
//! │  ResultsStore ─────────────────────────────────────────────────┘   │
//! │  └── Persists ValidationResult (metrics, trades, thresholds)        │
//! │                                                                     │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage Example
//!
//! ```rust,ignore
//! use ingestor::core::{
//!     ResearchState, ResearchStore, ResearchStoreConfig,
//!     AlgorithmConfig, ConfigStore, ConfigStoreConfig,
//!     ValidationResult, ResultsStore, ResultsStoreConfig,
//! };
//!
//! // 1. Save research findings
//! let mut research_store = ResearchStore::new(ResearchStoreConfig::default())?;
//! let state = ResearchState::new("BTCUSDT");
//! research_store.save(&state)?;
//!
//! // 2. Generate and save algorithm config
//! let mut config_store = ConfigStore::new(ConfigStoreConfig::default())?;
//! let config = AlgorithmConfig::from_research(&state);
//! config_store.save(&config)?;
//!
//! // 3. Save validation results
//! let mut results_store = ResultsStore::new(ResultsStoreConfig::default())?;
//! let result = ValidationResult::new(ValidationStageType::Backtest, &config.id);
//! results_store.save(&result)?;
//! ```

pub mod algorithm_config;
pub mod config_store;
pub mod research_state;
pub mod research_store;
pub mod results_store;
pub mod validation_result;

#[cfg(test)]
mod integration_tests;

pub use research_state::{
    ConditionalProbability, MIDCEstimate, MIDCRegime, PersistenceStats, PriceSignature,
    RecommendedStrategy, ResearchState, SignatureConsistency, SignatureDirection,
    SignatureMagnitude, SignatureSpeed, TradeableAssessment,
    // TSMOM Framework (Moskowitz et al. 2012)
    BarSize, TSMOMConfig, TSMOMSignal, TSMOMSignalType, TSMOMStats,
};

pub use research_store::{
    AuditLogEntry, AuditOperation, ResearchStore, ResearchStoreConfig,
};

pub use results_store::{
    AggregatedMetrics, ResultsAuditEntry, ResultsAuditOperation, ResultsQuery,
    ResultsStore, ResultsStoreConfig, SortField,
};

pub use validation_result::{
    ExitReason, ThresholdResult, TradeDirection, TradeResult, ValidationMetrics,
    ValidationResult, ValidationStageType, ValidationThresholds,
};

pub use algorithm_config::{
    AlgorithmConfig, AlgorithmConfigBuilder, ConfigError, ConfigPreset,
    EntryParams, ExitParams, MarketMakingParams, PositionParams, RegimeFilters,
    ConfigSizingMethod, StrategyType,
};

pub use config_store::{
    ConfigStore, ConfigStoreConfig, ConfigAuditEntry, ConfigAuditOperation,
    ConfigQuery, ConfigSortField, ConfigDiff, ConfigDifference, ConfigSummary,
    ConfigStoreStats,
};

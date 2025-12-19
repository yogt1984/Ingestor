//! Framework Module
//!
//! Core persistent infrastructure for the MARS trading system.
//! This module provides:
//! - ResearchState: Unified research findings (Task 0.0)
//! - ResearchStore: Persistence layer for research state (Task 0.1)
//! - ValidationResult: Unified validation outcomes (Task 0.2)
//! - ResultsStore: Persistence layer for validation results (Task 0.3)
//! - AlgorithmConfig: Parameterized algorithm configuration (Task 0.4)
//! - Persistence stores for all state (Tasks 0.3, 0.5)

pub mod algorithm_config;
pub mod research_state;
pub mod research_store;
pub mod results_store;
pub mod validation_result;

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

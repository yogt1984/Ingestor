//! Framework Module
//!
//! Core persistent infrastructure for the MARS trading system.
//! This module provides:
//! - ResearchState: Unified research findings (Task 0.0)
//! - ResearchStore: Persistence layer for research state (Task 0.1)
//! - ValidationResult: Unified validation outcomes (Task 0.2)
//! - AlgorithmConfig: Parameterized algorithm configuration (Task 0.4)
//! - Persistence stores for all state (Tasks 0.3, 0.5)

pub mod research_state;
pub mod research_store;

pub use research_state::{
    ConditionalProbability, MIDCEstimate, MIDCRegime, PersistenceStats, PriceSignature,
    RecommendedStrategy, ResearchState, SignatureConsistency, SignatureDirection,
    SignatureMagnitude, SignatureSpeed, TradeableAssessment,
};

pub use research_store::{
    AuditLogEntry, AuditOperation, ResearchStore, ResearchStoreConfig,
};

//! Validation Pipeline Module (Task 2.0)
//!
//! Provides a unified validation pipeline that takes any algorithm through sequential
//! stages: Backtest → Forward → OOS → Paper → Live.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                      VALIDATION PIPELINE                                 │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌────────┐│
//! │  │ Backtest │ → │ Forward  │ → │   OOS    │ → │  Paper   │ → │  Live  ││
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └────────┘│
//! │       │              │              │              │              │     │
//! │       ▼              ▼              ▼              ▼              ▼     │
//! │  [ValidationResult] for each stage, stop on failure                    │
//! │                                                                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Components
//!
//! - [`ValidationStage`]: Trait for individual validation stages
//! - [`ValidationPipeline`]: Orchestrates stages with stop conditions
//! - [`StageConfig`]: Configuration for individual stages
//! - [`PipelineConfig`]: Configuration for the entire pipeline
//! - [`PipelineResult`]: Final result containing all stage results
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::validation::{
//!     ValidationPipeline, PipelineConfig, StageConfig,
//! };
//! use ingestor::core::{AlgorithmConfig, ValidationThresholds};
//!
//! // Create pipeline with configuration
//! let config = PipelineConfig::default();
//! let pipeline = ValidationPipeline::new(config);
//!
//! // Run validation (implementations would be provided)
//! // let result = pipeline.run(&algorithm_config, stages).await;
//! ```

pub mod traits;
pub mod pipeline;

// Re-export main types
pub use traits::{
    ValidationStage, ValidationStageFactory, StageContext, StageError, RunFuture,
};
pub use pipeline::{
    ValidationPipeline, PipelineConfig, PipelineResult, PipelineStatus,
    StageConfig, StageOutcome, StopCondition, PipelineBuilder,
};

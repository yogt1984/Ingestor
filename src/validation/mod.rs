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
//! - [`BacktestStage`]: Historical replay validation (Task 2.1)
//! - [`ForwardStage`]: Walk-forward validation (Task 2.2)
//! - [`OOSStage`]: Out-of-sample holdout validation (Task 2.3)
//! - [`StageConfig`]: Configuration for individual stages
//! - [`PipelineConfig`]: Configuration for the entire pipeline
//! - [`PipelineResult`]: Final result containing all stage results
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::validation::{
//!     ValidationPipeline, PipelineConfig, BacktestStage, BacktestStageConfig,
//! };
//! use ingestor::core::{AlgorithmConfig, ValidationThresholds};
//!
//! // Create a backtest stage
//! let stage = BacktestStage::new(BacktestStageConfig::default());
//!
//! // Create pipeline with configuration
//! let config = PipelineConfig::default();
//! let pipeline = ValidationPipeline::new(config);
//!
//! // Run validation
//! // let result = pipeline.run(&algorithm_config, stages).await;
//! ```

pub mod traits;
pub mod pipeline;
pub mod backtest_stage;
pub mod forward_stage;
pub mod oos_stage;
pub mod paper_stage;
pub mod live_stage;
pub mod pipeline_runner;

// Re-export main types
pub use traits::{
    ValidationStage, ValidationStageFactory, StageContext, StageError, RunFuture,
};
pub use pipeline::{
    ValidationPipeline, PipelineConfig, PipelineResult, PipelineStatus,
    StageConfig, StageOutcome, StopCondition, PipelineBuilder,
};
pub use backtest_stage::{
    BacktestStage, BacktestStageConfig, BacktestStageFactory,
};
pub use forward_stage::{
    ForwardStage, ForwardStageConfig, ForwardStageFactory,
    WindowResult, ForwardAggregateMetrics,
};
pub use oos_stage::{
    OOSStage, OOSStageConfig, OOSStageFactory, OOSMetrics,
};
pub use paper_stage::{
    PaperStage, PaperStageConfig, PaperStageFactory, PaperMetrics,
    PnLSample, SimulatedTrade, ShutdownHandle,
};
pub use live_stage::{
    LiveStage, LiveStageConfig, LiveStageFactory, LiveMetrics,
    LiveTrade, LivePnLSample, OCOBracket, OCOStatus, KillSwitch,
    CircuitBreakerState, CircuitBreakerTrigger, AuditLogEntry,
};
pub use pipeline_runner::{
    PipelineRunner, PipelineRunnerFactory, RunnerConfig, RunnerError,
    StageTypeConfig, StageExecutionResult,
};

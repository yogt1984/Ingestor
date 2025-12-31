//! Ingestor - Real-time market microstructure feature extraction platform
//!
//! This library provides:
//! - **Data Layer**: Order book and trades log ingestion, feed management, persistence
//! - **Features**: Entropy, illiquidity, volatility, toxicity, trend features, signal processing
//! - **Execution**: Market making engine, paper trading simulator, risk management
//! - **Backtesting**: Replay, harness, walk-forward validation
//! - **Forward Testing**: A/B testing, drift detection, regime monitoring
//! - **Strategies**: Fixed spread, Avellaneda-Stoikov, ML-based spread/skew
//!
//! ## Module Organization (v0.2)
//!
//! - `core/` - Persistent infrastructure: stores, configs, validation
//! - `edge_detection/` - Signal research: MIDC, persistence, conditional probability
//! - `execution/` - Order execution: market maker, simulator, risk
//! - `strategies/` - Trading strategies: MM algorithms, momentum
//! - `features/` - Feature extraction: entropy, volatility, toxicity
//! - `data/` - Data layer: orderbook, trades, persistence
//! - `regime/` - Regime detection
//! - `backtest/` - Backtesting infrastructure

// Core modules
pub mod data;
pub mod features;
pub mod ui;

// Execution module (order execution, risk management)
pub mod execution;

// Core module (persistent infrastructure - was: framework)
pub mod core;

// Edge Detection module (signal research - was: research)
pub mod edge_detection;

// Strategy modules (was: algorithms)
pub mod strategies;
pub mod regime;
pub mod backtest;
pub mod forward_testing;

// Validation Pipeline (Task 2.0)
pub mod validation;

// Unified Command Execution Layer (TUI-0.26, Task T-1.1)
pub mod commands;

// Re-export commonly used types for convenience
pub use data::{
    ConcurrentOrderBook, OrderBook, OrderBookEngine, OrderBookFeatures, OrderBookEngineConfig,
    ConcurrentTradesLog, TradesLog, TradesLogEngine, TradesLogFeatures, TradesLogEngineConfig,
    LobFeedManager, LogFeedManager,
    PersistenceEngine, save_feature_as_parquet_path,
};

pub use features::{
    FeatureFusionEngine, FeaturesSnapshot,
    IlliquidityEngine, IlliquidityMetrics, IlliquidityConfig,
    EntropyEngine, EntropyMetrics, EntropyConfig,
    VolatilityEngine, VolatilityMetrics, VolatilityConfig,
    ToxicityEngine, ToxicityMetrics, ToxicityConfig,
    TrendFeatureEngine, TrendFeatures, TrendFeatureConfig,
    KalmanFilter, KalmanConfig, KalmanState, MultiSymbolKalman,
};

pub use execution::{
    MarketMakerEngine, MMConfig, Quote,
    RiskManagedPaperTradingEngine, SimulatorConfig,
    RiskManager, RiskConfig,
    OCOManager, OCOOrder, OCOStats, OCOTrigger, OCOError, Side, TriggerType,
    PositionManager, PositionConfig, Position, PositionSide, PositionSizeRequest, PositionSizeResult, SizingMethod, PositionError, PortfolioStats,
    ParameterPreset, PresetStore,
};

pub use regime::{RegimeEngine, RegimeEngineConfig};

pub use core::{
    ResearchState, MIDCEstimate, MIDCRegime, PersistenceStats,
    PriceSignature, SignatureMagnitude, SignatureSpeed, SignatureDirection, SignatureConsistency,
    ConditionalProbability, TradeableAssessment, RecommendedStrategy,
    ResearchStore, ResearchStoreConfig, AuditLogEntry, AuditOperation,
    ValidationResult, ValidationStageType, ValidationMetrics, ValidationThresholds,
    TradeResult, TradeDirection, ExitReason, ThresholdResult,
    // TSMOM Framework (Moskowitz et al. 2012)
    BarSize, TSMOMConfig, TSMOMSignal, TSMOMSignalType, TSMOMStats,
    // Results Store (Task 0.3)
    ResultsStore, ResultsStoreConfig, ResultsQuery, ResultsAuditEntry, ResultsAuditOperation,
    AggregatedMetrics, SortField,
    // Algorithm Config (Task 0.4)
    AlgorithmConfig, AlgorithmConfigBuilder, ConfigError, ConfigPreset,
    EntryParams, ExitParams as AlgoExitParams, MarketMakingParams, PositionParams, RegimeFilters,
    ConfigSizingMethod, StrategyType,
    // Config Store (Task 0.5)
    ConfigStore, ConfigStoreConfig, ConfigAuditEntry, ConfigAuditOperation,
    ConfigQuery, ConfigSortField, ConfigDiff, ConfigDifference, ConfigSummary,
    ConfigStoreStats,
};

// Validation Pipeline re-exports (Task 2.0, 2.1, 2.2, 2.3)
pub use validation::{
    ValidationStage, ValidationStageFactory, StageContext, StageError,
    ValidationPipeline, PipelineConfig, PipelineResult, PipelineStatus,
    StageConfig, StageOutcome, StopCondition, PipelineBuilder,
    // BacktestStage (Task 2.1)
    BacktestStage, BacktestStageConfig, BacktestStageFactory,
    // ForwardStage (Task 2.2)
    ForwardStage, ForwardStageConfig, ForwardStageFactory,
    WindowResult, ForwardAggregateMetrics,
    // OOSStage (Task 2.3)
    OOSStage, OOSStageConfig, OOSStageFactory, OOSMetrics,
};

// Edge Detection module re-exports (was: research, Task 1.0)
pub use edge_detection::{
    ResearchEngine, ResearchEngineFactory, ResearchEngineConfig,
    MIDCConfig, PersistenceConfig, ConditionalConfig, AssessmentThresholds,
    ResearchEngineStats, SignificantSignal, PricePoint, Outcome, ResearchError,
    // MIDC Estimator (Task 1.1)
    MIDCEstimator, MIDCEstimatorBuilder, MIDCEstimatorStats,
    // Persistence Analyzer (Task 1.2)
    PersistenceAnalyzer, PersistenceAnalyzerBuilder, PersistenceAnalyzerStats,
    TrendDirection, CompletedTrend,
    // Price Signature Builder (Task 1.3)
    SignatureConfig, PriceSignatureBuilder, SignatureWithMetrics, PriceSignatureBuilderStats,
    // Conditional Model (Task 1.4)
    ConditionalModel, ConditionalModelConfig, ConditionalModelStats, ConditionalModelBuilder,
    // Research Engine (Task 1.5)
    DefaultResearchEngine, DefaultResearchEngineFactory,
    // Live Integration (Task 1.8)
    LiveResearchConfig, LiveResearchRunner, LiveResearchState,
    AssessmentChange, RunnerStats,
};

//! Ingestor - Real-time market microstructure feature extraction platform
//!
//! This library provides:
//! - **Data Layer**: Order book and trades log ingestion, feed management, persistence
//! - **Features**: Entropy, illiquidity, volatility, toxicity, trend features, signal processing
//! - **Trading**: Market making engine, paper trading simulator, risk management
//! - **Backtesting**: Replay, harness, walk-forward validation
//! - **Forward Testing**: A/B testing, drift detection, regime monitoring
//! - **Algorithms**: Fixed spread, Avellaneda-Stoikov, ML-based spread/skew

// Core modules
pub mod data;
pub mod features;
pub mod trading;
pub mod ui;

// Framework module (persistent infrastructure)
pub mod framework;

// Research module (continuous research engine - Task 1.0)
pub mod research;

// Analysis modules
pub mod algorithms;
pub mod regime;
pub mod backtest;
pub mod forward_testing;

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

pub use trading::{
    MarketMakerEngine, MMConfig, Quote,
    RiskManagedPaperTradingEngine, SimulatorConfig,
    RiskManager, RiskConfig,
    OCOManager, OCOOrder, OCOStats, OCOTrigger, OCOError, Side, TriggerType,
    PositionManager, PositionConfig, Position, PositionSide, PositionSizeRequest, PositionSizeResult, SizingMethod, PositionError, PortfolioStats,
    ParameterPreset, PresetStore,
};

pub use regime::{RegimeEngine, RegimeEngineConfig};

pub use framework::{
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

// Research module re-exports (Task 1.0)
pub use research::{
    ResearchEngine, ResearchEngineFactory, ResearchEngineConfig,
    MIDCConfig, PersistenceConfig, ConditionalConfig, AssessmentThresholds,
    ResearchEngineStats, SignificantSignal, PricePoint, Outcome, ResearchError,
    // MIDC Estimator (Task 1.1)
    MIDCEstimator, MIDCEstimatorBuilder, MIDCEstimatorStats,
    // Persistence Analyzer (Task 1.2)
    PersistenceAnalyzer, PersistenceAnalyzerBuilder, PersistenceAnalyzerStats,
    TrendDirection, CompletedTrend,
};

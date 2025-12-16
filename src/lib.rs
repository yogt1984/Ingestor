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
    ParameterPreset, PresetStore,
};

pub use regime::{RegimeEngine, RegimeEngineConfig};

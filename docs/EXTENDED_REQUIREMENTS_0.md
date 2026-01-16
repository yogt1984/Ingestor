# Extended Requirements: Information-Guided Adaptive Trading System

**Document Type:** Architecture Requirements & Implementation Specification
**Version:** 0.2
**Date:** 2026-01-15
**Status:** Approved for MVP Implementation

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Core Philosophy](#2-core-philosophy)
3. [Strategic Design Decisions](#3-strategic-design-decisions)
4. [System Architecture](#4-system-architecture)
5. [Module Specifications](#5-module-specifications)
6. [AMD Phase Detection Subsystem](#6-amd-phase-detection-subsystem)
7. [Implementation Requirements](#7-implementation-requirements)
8. [Validation & Testing Requirements](#8-validation--testing-requirements)
9. [Future Extensions](#9-future-extensions)

---

## 1. Executive Summary

### 1.1 System Purpose

The Information-Guided Adaptive Trading System is a regime-aware algorithmic trading platform that gates all trading decisions by **measured predictive information** rather than heuristic signals. The system uses information-theoretic measures (Mutual Information, Entropy) to:

- Identify which features contain predictive power about future returns
- Detect market regimes (trending, mean-reverting, noise)
- Adapt position sizing based on information availability
- Preserve capital when predictive information is absent

### 1.2 Core Principle

```
THE SYSTEM DOES NOT CHASE SIGNALS.
IT MEASURES INFORMATION.
```

All trading decisions are gated by:
- **Measured Predictive Information** (Mutual Information via KSG estimator)
- **Regime Awareness** (Entropy-based regime detection)
- **Confidence-Weighted Execution** (Position sizing proportional to certainty)

### 1.3 MVP Scope

The MVP deliberately minimizes complexity while maximizing signal quality:

| Included in MVP | Deferred to v2+ |
|-----------------|-----------------|
| Entropy-based regime detection | MRMR redundancy pruning |
| KSG MI feature ranking | Latent space construction (PCA/PLS) |
| Confidence-weighted OMS | Unsupervised regime clustering |
| Walk-forward validation | Neural MI estimation (MINE) |
| Multi-horizon target labeling | Cross-asset regime networks |
| Order flow direction features | Full HMM Wyckoff detection |
| Basic AMD phase labeling | AMD-aware OMS integration |

---

## 2. Core Philosophy

### 2.1 Information-Theoretic Foundation

Markets are treated as **non-stationary information processes**. The system's approach:

1. **Features are hypotheses** about what predicts future returns
2. **MI measures truth** - how much uncertainty reduction does each feature provide?
3. **Regimes modulate informativeness** - features that work in trends may fail in noise
4. **Adaptation is mandatory** - information content changes; the system must track this

### 2.2 Capital Preservation Principle

```
IF INFORMATION DISAPPEARS → EXPOSURE DISAPPEARS
```

Trading in the absence of measured predictive information is considered a **bug**, not a feature. The system treats "going flat" in noise regimes as a **first-class outcome**, not a failure mode.

### 2.3 Robustness Over Optimization

The architecture prioritizes:
- **Statistical defensibility** over maximum backtest performance
- **Compute awareness** over brute-force approaches
- **Overfitting resistance** over curve-fitting
- **Interpretability** over black-box complexity

---

## 3. Strategic Design Decisions

### 3.1 Target Definition (Y)

**Decision:** Use volatility-adjusted future return as primary target.

```
Y_t = (p_{t+H} - p_t) / (σ_{t,t+H} + ε)

where:
  p_t       = mid price at time t
  H         = forecast horizon (configurable)
  σ_{t,t+H} = realized volatility over [t, t+H]
  ε         = small constant to prevent division by zero
```

**Rationale:**
- Encodes direction, magnitude, AND tradability
- Normalizes by volatility → comparable across regimes
- Avoids microstructure noise and bid-ask artifacts
- Risk-adjusted target leads to risk-adjusted strategies

**Alternative targets (for comparison/diagnostics):**
- Triple-barrier labels (cost-aware classification)
- Monotonicity score (regime diagnostic)
- Binary direction (baseline)

### 3.2 Mutual Information Estimator

**Decision:** Use Kraskov-Stögbauer-Grassberger (KSG) estimator.

**Rationale:**
- Non-parametric (no distribution assumptions)
- Captures non-linear dependencies
- Well-established, extensively validated
- O(n log n) complexity with KD-trees

**Usage Pattern:**
- Strictly **offline/periodic** computation
- Recomputed **weekly or monthly**
- NOT used for real-time signal generation

**Key Insight:** MI is a **selection pressure**, not a signal generator.

### 3.3 Regime Detection Method

**Decision:** Use entropy-based regime detection (existing `entropy.rs` module).

**Regime Categories:**
| Regime | Characteristics | Trading Approach |
|--------|----------------|------------------|
| TREND | Low entropy, high monotonicity | Momentum, trend-following |
| MEAN_REVERT | Moderate entropy, oscillating | Market-making, fade moves |
| NOISE | High entropy, unpredictable | Minimal/zero exposure |

### 3.4 MVP Simplifications

**Explicitly deferred:**
- MRMR redundancy pruning → use simple correlation filter if needed
- PCA/PLS latent spaces → work in original feature space
- GMM clustering → use threshold-based regime classification
- Neural MI (MINE) → KSG is sufficient for MVP scale

---

## 4. System Architecture

### 4.1 Processing Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                      RAW MARKET DATA                             │
│              (Orderbook, Trades, Features @ 10Hz)                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 1: FEATURE EXTRACTION                      │
│                                                                  │
│  Existing modules:                                               │
│  • orderbook.rs → OB features, imbalance, depth                 │
│  • entropy.rs → sample entropy, permutation entropy             │
│  • (new) Additional entropy/microstructure features             │
│                                                                  │
│  Output: X_t ∈ ℝ^d (d features per timestamp)                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 2: TARGET LABELING                         │
│                                                                  │
│  New module: labeler.rs                                          │
│                                                                  │
│  • Compute Y_t for multiple horizons H ∈ {10, 30, 100, 300}     │
│  • Align (X_t, Y_{t+H}) pairs                                   │
│  • Strict no-lookahead time indexing                            │
│                                                                  │
│  Output: Labeled dataset {(X_t, Y_t^H)} for each horizon        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 3: MI COMPUTATION                          │
│                                                                  │
│  New module: mi.rs                                               │
│                                                                  │
│  • Batch KSG MI estimation: I(X_i; Y) for each feature          │
│  • Shuffle-based significance testing                           │
│  • Regime-conditioned MI (different rankings per regime)        │
│                                                                  │
│  Output: MI scores with confidence intervals                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 4: FEATURE RANKING                         │
│                                                                  │
│  New module: feature_rank.rs                                     │
│                                                                  │
│  • Rank features by MI score                                    │
│  • Track stability across time windows                          │
│  • Index by regime and horizon                                  │
│                                                                  │
│  Output: Feature importance tables                              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 5: MODEL TRAINING                          │
│                                                                  │
│  New module: model.rs                                            │
│                                                                  │
│  • Lightweight supervised model (logistic/linear/SVM)           │
│  • Walk-forward training with temporal splits                   │
│  • Output: direction probability + confidence score             │
│                                                                  │
│  Note: Model uses only top-k MI-ranked features                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 6: REGIME DETECTION                        │
│                                                                  │
│  Existing module: entropy.rs (enhanced)                          │
│                                                                  │
│  • Real-time entropy computation                                │
│  • Regime classification: TREND / MEAN_REVERT / NOISE           │
│  • Regime probability estimation                                │
│                                                                  │
│  Output: regime_t, P(regime_t)                                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 6B: AMD PHASE DETECTION                    │
│                                                                  │
│  New module: amd_detector.rs                                     │
│                                                                  │
│  • Wyckoff phase classification using order flow features       │
│  • Hidden Markov Model for phase sequence                       │
│  • ACCUMULATION / MARKUP / DISTRIBUTION / MARKDOWN detection    │
│  • Manipulation event flagging                                  │
│                                                                  │
│  Output: phase_t, P(phase_t), trading_signal                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 7: ORDER MANAGEMENT (OMS)                  │
│                                                                  │
│  Enhanced module: market_maker.rs / mm_simulator.rs              │
│                                                                  │
│  Decision Logic:                                                 │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ if regime == NOISE:                                        │ │
│  │     position_size ≈ 0                                      │ │
│  │ else if regime == TREND:                                   │ │
│  │     use momentum strategy                                  │ │
│  │     size ∝ confidence × regime_probability                 │ │
│  │ else if regime == MEAN_REVERT:                             │ │
│  │     use market-making / fade logic                         │ │
│  │     size ∝ distance_from_mean × confidence                 │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Output: Trading signals with position sizes                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 8: CONTINUOUS ADAPTATION                   │
│                                                                  │
│  New module: adaptation.rs                                       │
│                                                                  │
│  Periodic recalibration:                                         │
│  • Recompute MI rankings (detect feature decay)                 │
│  • Retrain models on recent data                                │
│  • Detect regime distribution shifts                            │
│                                                                  │
│  Frequency: hourly → daily → weekly (configurable)              │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Data Flow Diagram

```
                    ┌──────────────┐
                    │ Market Data  │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │Orderbook│  │ Trades  │  │ Features│
        │ Engine  │  │ Engine  │  │Snapshot │
        └────┬────┘  └────┬────┘  └────┬────┘
              │            │            │
              └────────────┼────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   Labeler    │◄──── Offline
                    │  (Y target)  │      Batch
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │   MI    │  │ Feature │  │  Model  │
        │Compute  │  │  Rank   │  │Training │
        └────┬────┘  └────┬────┘  └────┬────┘
              │            │            │
              └────────────┼────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
        ┌──────────────┐         ┌──────────────┐
        │   Regime     │◄────────│     AMD      │◄──── Real-time
        │  Detector    │         │   Detector   │
        │ (Entropy)    │         │  (Wyckoff)   │
        └──────┬───────┘         └──────┬───────┘
              │                         │
              └────────────┬────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │     OMS      │
                    │ (Combined    │
                    │  Regime+AMD) │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  Validator   │◄──── Continuous
                    │  (Monitor)   │      Feedback
                    └──────────────┘
```

---

## 5. Module Specifications

### 5.1 `labeler.rs` - Target Computation

**Purpose:** Compute forward-looking target variables Y for supervised learning.

**Interface:**
```rust
pub struct LabelConfig {
    /// Forecast horizons in ticks
    pub horizons: Vec<usize>,  // e.g., [10, 30, 100, 300]
    /// Minimum volatility floor (epsilon)
    pub vol_floor: f64,        // e.g., 1e-8
    /// Label type
    pub label_type: LabelType,
}

pub enum LabelType {
    /// Y = (p_{t+H} - p_t) / (σ + ε)
    VolatilityAdjustedReturn,
    /// Y ∈ {-1, 0, 1} based on triple barrier
    TripleBarrier { take_profit: f64, stop_loss: f64 },
    /// Y = monotonicity score ∈ [0, 1]
    Monotonicity,
    /// Y = sign(p_{t+H} - p_t)
    BinaryDirection,
}

pub struct LabeledSample {
    pub timestamp: i64,
    pub features: Vec<f64>,    // X_t
    pub labels: HashMap<usize, f64>,  // horizon -> Y_{t+H}
}

pub trait Labeler {
    fn compute_labels(&self, prices: &[f64], config: &LabelConfig) -> Vec<LabeledSample>;
    fn align_features_labels(&self, features: &[FeatureSnapshot], labels: &[f64], horizon: usize) -> Vec<(Vec<f64>, f64)>;
}
```

**Requirements:**
- [REQ-LAB-001] Compute volatility-adjusted returns for configurable horizons
- [REQ-LAB-002] Implement strict temporal alignment (no lookahead)
- [REQ-LAB-003] Handle missing data and gaps gracefully
- [REQ-LAB-004] Support multiple label types for comparison
- [REQ-LAB-005] Output aligned (X_t, Y_{t+H}) pairs ready for MI computation

---

### 5.2 `mi.rs` - Mutual Information Estimation

**Purpose:** Compute KSG mutual information between features and targets.

**Interface:**
```rust
pub struct MIConfig {
    /// Number of nearest neighbors for KSG
    pub k_neighbors: usize,    // default: 5
    /// Number of permutations for significance testing
    pub n_permutations: usize, // default: 100
    /// Significance level
    pub alpha: f64,            // default: 0.05
}

pub struct MIResult {
    pub feature_name: String,
    pub mi_score: f64,         // in nats
    pub mi_bits: f64,          // in bits (mi_score / ln(2))
    pub p_value: f64,          // from permutation test
    pub significant: bool,     // p_value < alpha
    pub confidence_interval: (f64, f64),
}

pub trait MutualInformationEstimator {
    /// Compute MI between single feature and target
    fn compute_mi(&self, x: &[f64], y: &[f64], config: &MIConfig) -> MIResult;

    /// Compute MI for all features
    fn compute_mi_batch(&self, features: &[Vec<f64>], target: &[f64],
                        feature_names: &[String], config: &MIConfig) -> Vec<MIResult>;

    /// Compute MI conditioned on regime
    fn compute_mi_by_regime(&self, features: &[Vec<f64>], target: &[f64],
                            regimes: &[Regime], config: &MIConfig) -> HashMap<Regime, Vec<MIResult>>;
}
```

**Requirements:**
- [REQ-MI-001] Implement KSG estimator with configurable k
- [REQ-MI-002] Use KD-tree for O(n log n) neighbor search
- [REQ-MI-003] Implement permutation-based significance testing
- [REQ-MI-004] Handle edge cases (constant features, zero variance)
- [REQ-MI-005] Support regime-conditioned MI computation
- [REQ-MI-006] Compute confidence intervals via bootstrap

---

### 5.3 `feature_rank.rs` - Feature Importance Tracking

**Purpose:** Maintain MI-based feature rankings across time and regimes.

**Interface:**
```rust
pub struct FeatureRankTable {
    /// MI scores indexed by (regime, horizon, feature)
    pub scores: HashMap<(Regime, usize, String), MIResult>,
    /// Stability metrics
    pub stability: HashMap<String, StabilityMetrics>,
    /// Last update timestamp
    pub last_updated: i64,
}

pub struct StabilityMetrics {
    pub mean_mi: f64,
    pub std_mi: f64,
    pub coefficient_of_variation: f64,
    pub rank_correlation: f64,  // Spearman correlation across time windows
}

pub trait FeatureRanker {
    /// Get top-k features for given regime and horizon
    fn get_top_features(&self, regime: Regime, horizon: usize, k: usize) -> Vec<String>;

    /// Update rankings with new MI computation
    fn update_rankings(&mut self, mi_results: &[MIResult], regime: Regime, horizon: usize);

    /// Get stability report
    fn stability_report(&self) -> Vec<(String, StabilityMetrics)>;

    /// Detect features with decaying informativeness
    fn detect_decay(&self, threshold: f64) -> Vec<String>;
}
```

**Requirements:**
- [REQ-RANK-001] Maintain MI rankings indexed by regime and horizon
- [REQ-RANK-002] Track temporal stability of rankings
- [REQ-RANK-003] Alert on significant rank changes (feature decay)
- [REQ-RANK-004] Support efficient lookup for real-time use
- [REQ-RANK-005] Persist rankings for audit and analysis

---

### 5.4 `selector.rs` - Feature Selection (Optional v1)

**Purpose:** MRMR-based feature selection for dimensionality reduction.

**Interface:**
```rust
pub struct SelectorConfig {
    /// Maximum features to select
    pub max_features: usize,
    /// Minimum MI threshold
    pub mi_threshold: f64,
    /// Redundancy measure
    pub redundancy_measure: RedundancyMeasure,
}

pub enum RedundancyMeasure {
    /// Use KSG MI (accurate but slow)
    MutualInformation,
    /// Use Spearman correlation (fast approximation)
    SpearmanCorrelation,
    /// Use distance correlation (captures nonlinear)
    DistanceCorrelation,
}

pub trait FeatureSelector {
    /// Select features using MRMR criterion
    fn select_mrmr(&self, features: &[Vec<f64>], target: &[f64],
                   feature_names: &[String], config: &SelectorConfig) -> Vec<String>;

    /// Get MRMR scores for all features
    fn get_mrmr_scores(&self, features: &[Vec<f64>], target: &[f64],
                       feature_names: &[String]) -> Vec<(String, f64)>;
}
```

**Requirements:**
- [REQ-SEL-001] Implement MRMR criterion: score = relevance - redundancy
- [REQ-SEL-002] Support fast redundancy proxies (Spearman, dCor)
- [REQ-SEL-003] Greedy forward selection with early stopping
- [REQ-SEL-004] Cache pairwise redundancy computations

---

### 5.5 `model.rs` - Supervised Learning

**Purpose:** Train lightweight models on MI-selected features.

**Interface:**
```rust
pub struct ModelConfig {
    /// Model type
    pub model_type: ModelType,
    /// Features to use (from selector)
    pub feature_names: Vec<String>,
    /// Training parameters
    pub train_params: TrainParams,
}

pub enum ModelType {
    /// Logistic regression for classification
    LogisticRegression { regularization: f64 },
    /// Linear regression for continuous target
    LinearRegression { regularization: f64 },
    /// Support vector machine
    SVM { kernel: Kernel, c: f64 },
    /// Gradient boosted trees (lightweight)
    GradientBoosting { n_trees: usize, max_depth: usize },
}

pub struct Prediction {
    pub direction: f64,      // -1 to +1
    pub confidence: f64,     // 0 to 1
    pub raw_score: f64,      // model output before calibration
}

pub trait TradingModel {
    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<(), ModelError>;
    fn predict(&self, features: &[f64]) -> Prediction;
    fn feature_importance(&self) -> Vec<(String, f64)>;
}
```

**Requirements:**
- [REQ-MOD-001] Support multiple model types (logistic, linear, SVM)
- [REQ-MOD-002] Walk-forward training with purged cross-validation
- [REQ-MOD-003] Output calibrated confidence scores
- [REQ-MOD-004] Track feature importance within model
- [REQ-MOD-005] Support model persistence and loading

---

### 5.6 `oms.rs` - Order Management System

**Purpose:** Execute regime-aware, confidence-weighted trading decisions.

**Interface:**
```rust
pub struct OMSConfig {
    /// Base position size (notional)
    pub base_size: f64,
    /// Maximum position size
    pub max_size: f64,
    /// Minimum confidence to trade
    pub min_confidence: f64,
    /// Regime-specific parameters
    pub regime_params: HashMap<Regime, RegimeParams>,
}

pub struct RegimeParams {
    /// Position size multiplier for this regime
    pub size_multiplier: f64,
    /// Strategy type
    pub strategy: Strategy,
    /// Stop loss multiplier
    pub stop_loss: f64,
    /// Take profit multiplier
    pub take_profit: f64,
}

pub enum Strategy {
    Momentum,
    MeanReversion,
    MarketMaking { spread_bps: f64, skew: f64 },
    Flat,  // No trading
}

pub struct TradingDecision {
    pub action: Action,
    pub size: f64,
    pub reason: String,
    pub confidence: f64,
    pub regime: Regime,
}

pub enum Action {
    Buy,
    Sell,
    Hold,
    Flatten,
}

pub trait OrderManagement {
    fn decide(&self, prediction: &Prediction, regime: Regime,
              regime_prob: f64, current_position: f64) -> TradingDecision;
    fn apply_risk_limits(&self, decision: &TradingDecision) -> TradingDecision;
}
```

**Requirements:**
- [REQ-OMS-001] Implement regime-aware position sizing
- [REQ-OMS-002] Zero/minimal exposure in NOISE regime
- [REQ-OMS-003] Confidence-weighted sizing: size ∝ confidence × regime_prob
- [REQ-OMS-004] Respect maximum position limits
- [REQ-OMS-005] Log all decisions with full reasoning

---

### 5.7 `validator.rs` - Performance Validation

**Purpose:** Walk-forward evaluation with regime-conditioned metrics.

**Interface:**
```rust
pub struct ValidationConfig {
    /// Number of walk-forward folds
    pub n_folds: usize,
    /// Training window size
    pub train_window: usize,
    /// Test window size
    pub test_window: usize,
    /// Purge gap between train and test
    pub purge_gap: usize,
    /// Transaction cost in bps
    pub transaction_cost_bps: f64,
}

pub struct ValidationResult {
    pub total_return: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub regime_performance: HashMap<Regime, RegimePerformance>,
}

pub struct RegimePerformance {
    pub return_in_regime: f64,
    pub time_in_regime: f64,
    pub trades_in_regime: usize,
    pub accuracy_in_regime: f64,
}

pub trait Validator {
    fn walk_forward_validate(&self, data: &ValidationData,
                             config: &ValidationConfig) -> ValidationResult;
    fn regime_breakdown(&self, trades: &[Trade], regimes: &[Regime]) -> HashMap<Regime, RegimePerformance>;
}
```

**Requirements:**
- [REQ-VAL-001] Walk-forward validation with purged train/test split
- [REQ-VAL-002] Transaction-cost-aware PnL computation
- [REQ-VAL-003] Regime-conditioned performance breakdown
- [REQ-VAL-004] Statistical significance testing of results
- [REQ-VAL-005] Detect overfitting via train/test performance gap

---

### 5.8 `adaptation.rs` - Continuous Recalibration

**Purpose:** Detect drift and trigger model/ranking updates.

**Interface:**
```rust
pub struct AdaptationConfig {
    /// Recalibration frequency
    pub frequency: RecalibrationFrequency,
    /// Drift detection threshold
    pub drift_threshold: f64,
    /// Minimum samples for recalibration
    pub min_samples: usize,
}

pub enum RecalibrationFrequency {
    Hourly,
    Daily,
    Weekly,
    OnDrift,  // Only when drift detected
}

pub struct DriftReport {
    pub mi_drift_detected: bool,
    pub regime_drift_detected: bool,
    pub features_with_decay: Vec<String>,
    pub recommended_action: AdaptationAction,
}

pub enum AdaptationAction {
    NoAction,
    RefreshMIRankings,
    RetrainModel,
    FullRecalibration,
}

pub trait AdaptationManager {
    fn check_drift(&self, recent_data: &[Sample], historical_stats: &Stats) -> DriftReport;
    fn trigger_recalibration(&mut self, action: AdaptationAction) -> Result<(), Error>;
    fn schedule_next_update(&self) -> i64;
}
```

**Requirements:**
- [REQ-ADP-001] Periodic MI ranking refresh
- [REQ-ADP-002] KS-test based drift detection
- [REQ-ADP-003] Feature decay alerting
- [REQ-ADP-004] Automated recalibration triggers
- [REQ-ADP-005] Audit log of all adaptations

---

## 6. AMD Phase Detection Subsystem

### 6.1 Overview: Wyckoff Market Cycle Detection

The AMD (Accumulation, Manipulation, Distribution) Phase Detection subsystem implements information-theoretic detection of Wyckoff market cycle phases. This is a **potential alpha source** that complements the entropy-based regime detection by identifying **institutional activity patterns**.

**Core Thesis:** Large market participants (smart money) cannot buy or sell instantly. Their activity leaves detectable footprints in order flow, volume, and market microstructure that can be identified using MI-based feature analysis.

```
THE WYCKOFF MARKET CYCLE

    Price
      │
      │                          ┌─────────────────┐
      │                         ╱│   DISTRIBUTION  │╲
      │                        ╱ │  (Smart money   │ ╲
      │                       ╱  │   selling)      │  ╲
      │        ┌─────────────┤   └─────────────────┘   │
      │       ╱│   MARKUP    │                         │  MARKDOWN
      │      ╱ │  (Trend up) │                         │  (Trend down)
      │     ╱  │             │                         │ ╲
      │    ╱   └─────────────┘                         │  ╲
      │   │                                            │   ╲
      │┌──┴────────────┐                          ┌────┴─────────┐
      ││ ACCUMULATION  │                          │ New cycle    │
      ││ (Smart money  │                          │ begins...    │
      ││  buying)      │                          │              │
      │└───────────────┘                          └──────────────┘
      │
      └─────────────────────────────────────────────────────────────► Time
```

**Why This Creates Edge:**
- Detecting ACCUMULATION before MARKUP → position early for trend
- Detecting DISTRIBUTION before MARKDOWN → exit/short before decline
- Detecting MANIPULATION → avoid stop hunts, recognize false breakouts

---

### 6.2 Theoretical Foundation

#### 6.2.1 The Information Leakage Model

AMD phases are **hidden states** representing institutional intent. They leak information through observable market microstructure:

```
HIDDEN REALITY                    OBSERVABLE MANIFESTATIONS
══════════════                    ═════════════════════════

┌─────────────────┐               ┌─────────────────────────┐
│  ACCUMULATION   │               │ • Price: Range-bound    │
│                 │ ──leaks───►   │ • Volume: Elevated      │
│  Smart money    │               │ • Order flow: NET BUY   │
│  quietly buying │               │ • OB: Bid absorption    │
│                 │               │ • Entropy: LOW          │
└─────────────────┘               └─────────────────────────┘

┌─────────────────┐               ┌─────────────────────────┐
│  DISTRIBUTION   │               │ • Price: Range-bound    │
│                 │ ──leaks───►   │ • Volume: Elevated      │
│  Smart money    │               │ • Order flow: NET SELL  │
│  quietly selling│               │ • OB: Ask absorption    │
│                 │               │ • Entropy: LOW          │
└─────────────────┘               └─────────────────────────┘

KEY INSIGHT: Accumulation and Distribution look IDENTICAL
in price, but OPPOSITE in order flow. Order book features
are critical for discrimination.
```

#### 6.2.2 Phase Characteristics

| Phase | Price Action | Volume | Order Flow | Entropy | Duration |
|-------|-------------|--------|------------|---------|----------|
| **ACCUMULATION** | Range-bound | Elevated, hidden | Net buying (absorbed) | Low | Days-weeks |
| **MARKUP** | Trending up | Increasing | Strong buy imbalance | Low-moderate | Days-weeks |
| **DISTRIBUTION** | Range-bound at top | Elevated, hidden | Net selling (absorbed) | Low | Days-weeks |
| **MARKDOWN** | Trending down | Panic spikes | Strong sell imbalance | High | Days |
| **MANIPULATION** | Sharp spike + reversal | Extreme spike | Sudden imbalance shift | Spike | Minutes-hours |

#### 6.2.3 The Discrimination Problem

**Critical Insight:** ACCUMULATION and DISTRIBUTION appear identical in price-based features. Discrimination requires **order flow direction**:

```
ACCUMULATION:
  Sellers hit bids → Bids refill → Price stable
  Smart money ABSORBS selling pressure

  Order Book Signature:
  ┌────────────────────────────────┐
  │  Asks: ████████ (being sold)   │
  │  Mid:  ────────                │
  │  Bids: ████████████ (absorbing)│ ← Bids keep refilling
  └────────────────────────────────┘

DISTRIBUTION:
  Buyers lift asks → Asks refill → Price stable
  Smart money ABSORBS buying pressure

  Order Book Signature:
  ┌────────────────────────────────┐
  │  Asks: ████████████ (absorbing)│ ← Asks keep refilling
  │  Mid:  ────────                │
  │  Bids: ████████ (being bought) │
  └────────────────────────────────┘
```

---

### 6.3 MI-Based Phase Feature Analysis

#### 6.3.1 Hypothesis: Feature Informativeness by Phase

Based on Wyckoff theory, the following features should have HIGH mutual information with each phase:

```
PREDICTED MI RANKINGS (to be validated empirically)

┌─────────────────────────────────────────────────────────────────────┐
│                         ACCUMULATION                                 │
├─────────────────────────────────────────────────────────────────────┤
│ HIGH MI expected:                                                    │
│   • order_flow_imbalance (POSITIVE - net buying)                    │
│   • bid_depth_change (bids absorbing, constantly refilling)         │
│   • volume_at_bid / volume_at_ask ratio (> 1)                       │
│   • sample_entropy (LOW - range-bound, predictable)                 │
│   • volatility (LOW)                                                │
│   • trade_size_asymmetry (larger hidden buys)                       │
│                                                                      │
│ LOW MI expected:                                                     │
│   • raw returns (≈ 0 during accumulation)                           │
│   • momentum indicators (flat)                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                         DISTRIBUTION                                 │
├─────────────────────────────────────────────────────────────────────┤
│ HIGH MI expected:                                                    │
│   • order_flow_imbalance (NEGATIVE - net selling)                   │
│   • ask_depth_change (asks absorbing, constantly refilling)         │
│   • volume_at_ask / volume_at_bid ratio (> 1)                       │
│   • sample_entropy (LOW - range-bound)                              │
│   • price_percentile_in_range (HIGH - distribution at tops)         │
│                                                                      │
│ DISTINGUISHING from accumulation:                                    │
│   • order_flow_imbalance SIGN is opposite                           │
│   • bid/ask absorption ratio is inverted                            │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                         MANIPULATION                                 │
├─────────────────────────────────────────────────────────────────────┤
│ HIGH MI expected:                                                    │
│   • return_reversal_score (sharp move followed by reversal)         │
│   • volume_spike_ratio (unusual volume on manipulation bar)         │
│   • distance_from_range_boundary (manipulations at edges)           │
│   • order_book_imbalance_change_rate (sudden shifts)                │
│   • trade_aggression (market orders hitting stops)                  │
│                                                                      │
│ NOTE: Manipulation events are TRANSIENT - hard to predict,          │
│       but valuable to detect for avoiding false signals             │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                         MARKUP / MARKDOWN                            │
├─────────────────────────────────────────────────────────────────────┤
│ HIGH MI expected:                                                    │
│   • momentum (strong directional signal)                            │
│   • returns (positive for markup, negative for markdown)            │
│   • volume_trend (increasing during markup)                         │
│   • sample_entropy (LOW-MODERATE - directional)                     │
│   • order_book_slope (thin in direction of move)                    │
└─────────────────────────────────────────────────────────────────────┘
```

#### 6.3.2 Required New Features for AMD Detection

The current feature set must be extended with **order flow direction** features:

| Feature | Description | Critical For |
|---------|-------------|--------------|
| `order_flow_imbalance` | (buy_volume - sell_volume) / total_volume | ACCUM vs DISTRIB |
| `bid_depth_change` | Rate of bid level refilling after hits | ACCUM detection |
| `ask_depth_change` | Rate of ask level refilling after lifts | DISTRIB detection |
| `volume_at_bid` | Volume of trades at bid price | ACCUM detection |
| `volume_at_ask` | Volume of trades at ask price | DISTRIB detection |
| `large_trade_ratio` | Fraction of volume in large trades | Hidden activity |
| `absorption_ratio` | Bid absorption / Ask absorption | ACCUM vs DISTRIB |
| `price_range_percentile` | Current price position in N-bar range | Phase context |
| `return_reversal_score` | Magnitude of reversal after sharp move | MANIPULATION |

---

### 6.4 Module Specification: `amd_detector.rs`

**Purpose:** Detect Wyckoff market cycle phases using MI-based feature analysis and Hidden Markov Models.

#### 6.4.1 Data Structures

```rust
/// Wyckoff market cycle phases
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WyckoffPhase {
    /// Smart money quietly accumulating - precedes markup
    Accumulation,
    /// Uptrend phase - after accumulation complete
    Markup,
    /// Smart money quietly distributing - precedes markdown
    Distribution,
    /// Downtrend phase - after distribution complete
    Markdown,
    /// False breakout/breakdown designed to trap traders
    Manipulation,
    /// Cannot determine phase with confidence
    Unknown,
}

/// Configuration for AMD phase detection
pub struct AMDConfig {
    /// Lookback window for phase classification (in samples)
    pub lookback_window: usize,           // default: 500
    /// Forward window for label validation (in samples)
    pub forward_validation_window: usize, // default: 100
    /// Minimum confidence to report a phase
    pub min_confidence: f64,              // default: 0.6
    /// Order flow imbalance threshold for ACCUM/DISTRIB
    pub ofi_threshold: f64,               // default: 0.1
    /// Number of HMM states (typically 4-5)
    pub hmm_states: usize,                // default: 4
    /// MI significance threshold
    pub mi_significance: f64,             // default: 0.05
}

/// Result of phase detection
pub struct PhaseDetectionResult {
    /// Most likely current phase
    pub phase: WyckoffPhase,
    /// Confidence in the classification (0-1)
    pub confidence: f64,
    /// Probability distribution over all phases
    pub phase_probabilities: HashMap<WyckoffPhase, f64>,
    /// Features that most support this classification
    pub supporting_features: Vec<(String, f64)>,
    /// Recommended trading action
    pub trading_signal: AMDTradingSignal,
}

/// Trading signal derived from AMD phase
pub struct AMDTradingSignal {
    /// Position bias (-1 to +1)
    pub bias: f64,
    /// Suggested strategy
    pub strategy: AMDStrategy,
    /// Risk level (0-1, higher = more caution)
    pub risk_level: f64,
}

pub enum AMDStrategy {
    /// Accumulate long positions gradually
    AccumulateLong,
    /// Hold/add to longs, trail stops
    RideTrend,
    /// Reduce longs, prepare for reversal
    DistributeLong,
    /// Short or flat, wait for accumulation
    AvoidOrShort,
    /// Ignore current signal, likely manipulation
    WaitForClarity,
}

/// MI analysis results for phase features
pub struct PhaseMIAnalysis {
    /// MI scores indexed by (phase, feature)
    pub mi_by_phase: HashMap<WyckoffPhase, Vec<MIResult>>,
    /// Features ranked by discrimination power
    pub discriminating_features: Vec<DiscriminatingFeature>,
    /// Phase signatures (mean, std of top features per phase)
    pub phase_signatures: HashMap<WyckoffPhase, PhaseSignature>,
}

pub struct DiscriminatingFeature {
    pub feature_name: String,
    /// How well this feature discriminates between phases
    pub discrimination_score: f64,
    /// Which phase this feature best identifies
    pub best_phase: WyckoffPhase,
    /// MI with that phase
    pub max_mi: f64,
}

pub struct PhaseSignature {
    /// Feature -> (mean, std) during this phase
    pub feature_stats: HashMap<String, (f64, f64)>,
    /// Number of samples used to compute signature
    pub sample_count: usize,
}
```

#### 6.4.2 Core Interface

```rust
/// Main trait for AMD phase detection
pub trait AMDPhaseDetector {
    /// Create proxy labels for phases from historical data
    fn create_phase_labels(
        &self,
        prices: &[f64],
        volumes: &[f64],
        order_flow_imbalance: &[f64],
        config: &AMDConfig,
    ) -> Vec<Option<WyckoffPhase>>;

    /// Compute MI between features and phase labels
    fn compute_phase_mi(
        &self,
        features: &[Vec<f64>],
        feature_names: &[String],
        phase_labels: &[Option<WyckoffPhase>],
    ) -> PhaseMIAnalysis;

    /// Learn phase signatures from labeled data
    fn learn_phase_signatures(
        &mut self,
        features: &[Vec<f64>],
        feature_names: &[String],
        phase_labels: &[Option<WyckoffPhase>],
        top_k_features: usize,
    );

    /// Detect current phase from feature values
    fn detect_phase(
        &self,
        current_features: &HashMap<String, f64>,
    ) -> PhaseDetectionResult;

    /// Get features ranked by phase discrimination ability
    fn get_discriminating_features(&self) -> Vec<DiscriminatingFeature>;
}

/// Hidden Markov Model for phase sequence modeling
pub trait WyckoffHMM {
    /// Fit HMM to observation sequence
    fn fit(&mut self, observations: &[Vec<f64>], n_iterations: usize);

    /// Predict most likely phase sequence (Viterbi)
    fn predict_sequence(&self, observations: &[Vec<f64>]) -> Vec<WyckoffPhase>;

    /// Get smoothed phase probabilities (Forward-Backward)
    fn predict_proba(&self, observations: &[Vec<f64>]) -> Vec<HashMap<WyckoffPhase, f64>>;

    /// Get transition matrix
    fn get_transition_matrix(&self) -> [[f64; 4]; 4];

    /// Get emission parameters per state
    fn get_emission_params(&self) -> HashMap<WyckoffPhase, EmissionParams>;
}

pub struct EmissionParams {
    pub mean: Vec<f64>,
    pub covariance: Vec<Vec<f64>>,
}
```

#### 6.4.3 Integration with OMS

```rust
/// Extended OMS decision incorporating AMD phase
pub struct AMDAwareDecision {
    /// Base decision from regime detection
    pub base_decision: TradingDecision,
    /// AMD phase overlay
    pub amd_phase: WyckoffPhase,
    /// AMD confidence
    pub amd_confidence: f64,
    /// Final adjusted decision
    pub final_decision: TradingDecision,
    /// Adjustment reasoning
    pub adjustment_reason: String,
}

/// AMD-aware order management
pub trait AMDAwareOMS {
    /// Make decision incorporating both regime and AMD phase
    fn decide_with_amd(
        &self,
        prediction: &Prediction,
        regime: Regime,
        regime_prob: f64,
        amd_result: &PhaseDetectionResult,
        current_position: f64,
    ) -> AMDAwareDecision;
}

impl AMDAwareOMS for OrderManagementSystem {
    fn decide_with_amd(
        &self,
        prediction: &Prediction,
        regime: Regime,
        regime_prob: f64,
        amd_result: &PhaseDetectionResult,
        current_position: f64,
    ) -> AMDAwareDecision {
        // Base decision from regime
        let base_decision = self.decide(prediction, regime, regime_prob, current_position);

        // AMD overlay logic
        let final_decision = match (regime, amd_result.phase) {
            // ACCUMULATION detected - bias long even in neutral regime
            (Regime::MeanRevert, WyckoffPhase::Accumulation) if amd_result.confidence > 0.7 => {
                TradingDecision {
                    action: Action::Buy,
                    size: base_decision.size * 0.5 * amd_result.confidence,
                    reason: "Accumulation detected in range - early long entry".into(),
                    ..base_decision
                }
            },

            // DISTRIBUTION detected - reduce longs even in trend
            (Regime::Trend, WyckoffPhase::Distribution) if amd_result.confidence > 0.7 => {
                TradingDecision {
                    action: Action::Sell,
                    size: current_position * 0.3,
                    reason: "Distribution detected at highs - reducing exposure".into(),
                    ..base_decision
                }
            },

            // MANIPULATION detected - wait for clarity
            (_, WyckoffPhase::Manipulation) if amd_result.confidence > 0.6 => {
                TradingDecision {
                    action: Action::Hold,
                    size: 0.0,
                    reason: "Manipulation detected - waiting for clarity".into(),
                    ..base_decision
                }
            },

            // Default: use base decision
            _ => base_decision.clone(),
        };

        AMDAwareDecision {
            base_decision,
            amd_phase: amd_result.phase,
            amd_confidence: amd_result.confidence,
            final_decision,
            adjustment_reason: "AMD overlay applied".into(),
        }
    }
}
```

---

### 6.5 Hidden Markov Model for Phase Transitions

#### 6.5.1 HMM Structure

The Wyckoff cycle has a natural HMM representation:

```
HMM PARAMETERS FOR WYCKOFF CYCLE

States: S = {ACCUMULATION, MARKUP, DISTRIBUTION, MARKDOWN}

Initial Distribution π:
  π = [0.25, 0.25, 0.25, 0.25]  (uniform)

Transition Matrix A (encodes cycle structure):
                      To:
                      ACCUM   MARKUP  DISTRIB  MARKDOWN
  From: ACCUM      [  0.85    0.15    0.00     0.00  ]
        MARKUP     [  0.00    0.80    0.20     0.00  ]
        DISTRIB    [  0.00    0.00    0.85     0.15  ]
        MARKDOWN   [  0.15    0.00    0.00     0.85  ]

Key Insights from A:
  • ACCUM → MARKUP (15%): Accumulation complete, markup begins
  • MARKUP → DISTRIB (20%): Trend exhaustion, distribution starts
  • DISTRIB → MARKDOWN (15%): Distribution complete, markdown begins
  • MARKDOWN → ACCUM (15%): Capitulation, new accumulation cycle
  • Diagonal dominance: Phases persist (sticky states)

Emission Distributions B (observation likelihoods):
  Each state emits observations (features) according to:

  ACCUMULATION:
    returns ~ N(0, σ_low)
    order_flow_imbalance ~ N(+0.15, 0.10)  ← KEY: positive
    entropy ~ N(0.3, 0.1)

  MARKUP:
    returns ~ N(+μ, σ_med)
    order_flow_imbalance ~ N(+0.25, 0.15)
    entropy ~ N(0.4, 0.1)

  DISTRIBUTION:
    returns ~ N(0, σ_low)
    order_flow_imbalance ~ N(-0.15, 0.10)  ← KEY: negative
    entropy ~ N(0.3, 0.1)

  MARKDOWN:
    returns ~ N(-μ, σ_high)
    order_flow_imbalance ~ N(-0.30, 0.20)
    entropy ~ N(0.6, 0.15)
```

#### 6.5.2 Learning and Inference

```rust
/// HMM implementation for Wyckoff phase detection
pub struct WyckoffHMMImpl {
    /// Number of states (4 for standard Wyckoff)
    n_states: usize,
    /// Initial state distribution
    pi: Vec<f64>,
    /// Transition matrix A[i][j] = P(S_{t+1}=j | S_t=i)
    transition_matrix: Vec<Vec<f64>>,
    /// Emission parameters per state
    emission_means: Vec<Vec<f64>>,
    emission_covars: Vec<Vec<Vec<f64>>>,
    /// State-to-phase mapping
    state_to_phase: Vec<WyckoffPhase>,
}

impl WyckoffHMMImpl {
    /// Initialize with Wyckoff cycle structure
    pub fn new_wyckoff() -> Self {
        Self {
            n_states: 4,
            pi: vec![0.25, 0.25, 0.25, 0.25],
            transition_matrix: vec![
                vec![0.85, 0.15, 0.00, 0.00],  // ACCUM
                vec![0.00, 0.80, 0.20, 0.00],  // MARKUP
                vec![0.00, 0.00, 0.85, 0.15],  // DISTRIB
                vec![0.15, 0.00, 0.00, 0.85],  // MARKDOWN
            ],
            emission_means: vec![],  // Learned from data
            emission_covars: vec![], // Learned from data
            state_to_phase: vec![
                WyckoffPhase::Accumulation,
                WyckoffPhase::Markup,
                WyckoffPhase::Distribution,
                WyckoffPhase::Markdown,
            ],
        }
    }

    /// Baum-Welch algorithm for parameter learning
    pub fn fit(&mut self, observations: &[Vec<f64>], max_iterations: usize) {
        // E-step: Forward-backward to get state expectations
        // M-step: Update parameters to maximize expected log-likelihood
        // Iterate until convergence
    }

    /// Viterbi algorithm for most likely state sequence
    pub fn viterbi(&self, observations: &[Vec<f64>]) -> Vec<WyckoffPhase> {
        // Dynamic programming to find argmax P(S | O)
    }

    /// Forward-backward for smoothed state probabilities
    pub fn forward_backward(&self, observations: &[Vec<f64>]) -> Vec<Vec<f64>> {
        // P(S_t = k | O_{1:T}) for all t, k
    }
}
```

---

### 6.6 Profit Mechanism

#### 6.6.1 The Edge from Early Detection

```
THE PROFIT MODEL

If you detect ACCUMULATION before MARKUP:
  → Enter long before the crowd
  → Capture full trend move
  → Higher Sharpe from better entry

If you detect DISTRIBUTION before MARKDOWN:
  → Exit longs before decline
  → Optionally short
  → Avoid drawdown, capture downside

If you detect MANIPULATION:
  → Avoid false breakout traps
  → Don't get stopped out
  → Preserve capital


QUANTIFIED EDGE (hypothetical):

Assume 4 phases, each ~50 periods average:
  MARKUP:   E[return] = +0.2% per period
  MARKDOWN: E[return] = -0.3% per period
  ACCUM/DISTRIB: E[return] = 0%

Strategy A: Buy and Hold
  Per cycle (200 periods):
  = 50×0% + 50×0.2% + 50×0% + 50×(-0.3%)
  = -5%

Strategy B: Perfect Phase Detection
  Long only during MARKUP, Short during MARKDOWN:
  = 50×0.2% + 50×0.3%
  = +25%

Strategy C: 70% Accurate Phase Detection
  ≈ 0.7 × 25% + 0.3 × (-5%)
  = +16%

Edge from detection = 16% - (-5%) = 21% per cycle
```

#### 6.6.2 Integration with Existing System

```
COMBINED SYSTEM: REGIME + AMD

┌─────────────────────────────────────────────────────────────────────┐
│                      DECISION MATRIX                                 │
├─────────────────┬───────────────────────────────────────────────────┤
│                 │              AMD PHASE                             │
│    REGIME       │ ACCUM    MARKUP    DISTRIB   MARKDOWN   MANIP    │
├─────────────────┼───────────────────────────────────────────────────┤
│ TREND           │ Long++   Long+     Reduce    Short      Wait     │
│ MEAN_REVERT     │ Long+    Hold      Short+    Hold       Wait     │
│ NOISE           │ Small    Flat      Small     Flat       Flat     │
│                 │ Long               Short                          │
└─────────────────┴───────────────────────────────────────────────────┘

Legend:
  ++  = Strong signal, full position
  +   = Moderate signal, partial position
  Wait = Suspected manipulation, no action
  Flat = No position

Key Insight: AMD provides DIRECTIONAL overlay on regime detection.
  - Regime tells you HOW to trade (trend-follow vs mean-revert)
  - AMD tells you WHICH DIRECTION to bias
```

---

### 6.7 AMD Implementation Requirements

| ID | Requirement | Priority | Complexity |
|----|-------------|----------|------------|
| REQ-AMD-001 | Implement proxy phase labeling from price/volume/OFI | P1 | Medium |
| REQ-AMD-002 | Compute MI between features and phase labels | P1 | Medium |
| REQ-AMD-003 | Identify discriminating features for ACCUM vs DISTRIB | P0 | High |
| REQ-AMD-004 | Implement phase signature learning | P1 | Medium |
| REQ-AMD-005 | Real-time phase detection from current features | P1 | Medium |
| REQ-AMD-006 | HMM implementation for phase sequence modeling | P2 | High |
| REQ-AMD-007 | Viterbi decoding for most likely phase path | P2 | Medium |
| REQ-AMD-008 | Forward-backward for smoothed probabilities | P2 | Medium |
| REQ-AMD-009 | Integration with OMS (AMD-aware decisions) | P1 | Medium |
| REQ-AMD-010 | Add order flow direction features to orderbook.rs | P0 | Medium |
| REQ-AMD-011 | Add bid/ask absorption rate features | P1 | Medium |
| REQ-AMD-012 | Manipulation event detection | P2 | High |
| REQ-AMD-013 | Walk-forward validation of phase detection | P1 | Medium |
| REQ-AMD-014 | Phase-conditioned performance reporting | P1 | Low |

---

### 6.8 Required Feature Extensions

To enable AMD detection, `orderbook.rs` must be extended with:

```rust
/// New features required for AMD phase detection
pub struct OrderFlowFeatures {
    /// (buy_volume - sell_volume) / total_volume
    /// Positive = net buying, Negative = net selling
    /// CRITICAL for ACCUM vs DISTRIB discrimination
    pub order_flow_imbalance: f64,

    /// Volume of trades executed at bid price
    pub volume_at_bid: f64,

    /// Volume of trades executed at ask price
    pub volume_at_ask: f64,

    /// Rate at which bid levels refill after being hit
    /// High during ACCUMULATION (bids absorbing)
    pub bid_absorption_rate: f64,

    /// Rate at which ask levels refill after being lifted
    /// High during DISTRIBUTION (asks absorbing)
    pub ask_absorption_rate: f64,

    /// bid_absorption_rate / ask_absorption_rate
    /// > 1 suggests ACCUMULATION, < 1 suggests DISTRIBUTION
    pub absorption_ratio: f64,

    /// Fraction of volume in trades > threshold size
    /// High during ACCUM/DISTRIB (hidden large orders)
    pub large_trade_ratio: f64,

    /// Current price as percentile of recent range [0, 1]
    /// High during DISTRIBUTION (at top of range)
    /// Low during ACCUMULATION (at bottom of range)
    pub price_range_percentile: f64,

    /// Magnitude of price reversal after sharp move
    /// High indicates MANIPULATION event
    pub reversal_score: f64,
}
```

---

## 7. Implementation Requirements

### 7.1 Phase 1: Foundation (labeler.rs, mi.rs)

| ID | Requirement | Priority | Complexity |
|----|-------------|----------|------------|
| REQ-LAB-001 | Implement volatility-adjusted return labeler | P0 | Medium |
| REQ-LAB-002 | Strict temporal alignment with no lookahead | P0 | Low |
| REQ-LAB-003 | Handle missing data gracefully | P1 | Low |
| REQ-LAB-004 | Support multiple label types | P1 | Medium |
| REQ-LAB-005 | Output aligned (X, Y) pairs | P0 | Low |
| REQ-MI-001 | Implement KSG estimator | P0 | High |
| REQ-MI-002 | KD-tree neighbor search | P0 | Medium |
| REQ-MI-003 | Permutation significance testing | P1 | Medium |
| REQ-MI-004 | Edge case handling | P1 | Low |
| REQ-MI-005 | Regime-conditioned MI | P1 | Medium |
| REQ-MI-006 | Bootstrap confidence intervals | P2 | Medium |

### 7.2 Phase 2: Ranking & Selection (feature_rank.rs, selector.rs)

| ID | Requirement | Priority | Complexity |
|----|-------------|----------|------------|
| REQ-RANK-001 | MI rankings by regime/horizon | P0 | Medium |
| REQ-RANK-002 | Temporal stability tracking | P1 | Medium |
| REQ-RANK-003 | Feature decay alerting | P1 | Low |
| REQ-RANK-004 | Efficient lookup | P0 | Low |
| REQ-RANK-005 | Persistence | P1 | Low |
| REQ-SEL-001 | MRMR implementation | P2 | High |
| REQ-SEL-002 | Fast redundancy proxies | P2 | Medium |
| REQ-SEL-003 | Greedy selection | P2 | Medium |
| REQ-SEL-004 | Redundancy caching | P2 | Low |

### 7.3 Phase 3: Model & OMS (model.rs, oms.rs)

| ID | Requirement | Priority | Complexity |
|----|-------------|----------|------------|
| REQ-MOD-001 | Multiple model types | P1 | High |
| REQ-MOD-002 | Walk-forward training | P0 | Medium |
| REQ-MOD-003 | Calibrated confidence | P1 | Medium |
| REQ-MOD-004 | Feature importance | P1 | Low |
| REQ-MOD-005 | Model persistence | P1 | Low |
| REQ-OMS-001 | Regime-aware sizing | P0 | Medium |
| REQ-OMS-002 | Zero exposure in NOISE | P0 | Low |
| REQ-OMS-003 | Confidence weighting | P0 | Low |
| REQ-OMS-004 | Position limits | P0 | Low |
| REQ-OMS-005 | Decision logging | P1 | Low |

### 7.4 Phase 4: Validation & Adaptation (validator.rs, adaptation.rs)

| ID | Requirement | Priority | Complexity |
|----|-------------|----------|------------|
| REQ-VAL-001 | Walk-forward validation | P0 | Medium |
| REQ-VAL-002 | Transaction cost PnL | P0 | Low |
| REQ-VAL-003 | Regime breakdown | P1 | Medium |
| REQ-VAL-004 | Statistical significance | P2 | Medium |
| REQ-VAL-005 | Overfitting detection | P1 | Medium |
| REQ-ADP-001 | Periodic MI refresh | P1 | Low |
| REQ-ADP-002 | Drift detection | P1 | Medium |
| REQ-ADP-003 | Decay alerting | P1 | Low |
| REQ-ADP-004 | Auto recalibration | P2 | Medium |
| REQ-ADP-005 | Audit logging | P1 | Low |

### 7.5 Phase 5: AMD Phase Detection (amd_detector.rs)

| ID | Requirement | Priority | Complexity |
|----|-------------|----------|------------|
| REQ-AMD-001 | Proxy phase labeling from price/volume/OFI | P1 | Medium |
| REQ-AMD-002 | MI computation for phase features | P1 | Medium |
| REQ-AMD-003 | ACCUM vs DISTRIB discrimination features | P0 | High |
| REQ-AMD-004 | Phase signature learning | P1 | Medium |
| REQ-AMD-005 | Real-time phase detection | P1 | Medium |
| REQ-AMD-006 | HMM implementation (Baum-Welch) | P2 | High |
| REQ-AMD-007 | Viterbi decoding | P2 | Medium |
| REQ-AMD-008 | Forward-backward smoothing | P2 | Medium |
| REQ-AMD-009 | OMS integration (AMD-aware decisions) | P1 | Medium |
| REQ-AMD-010 | Order flow direction features | P0 | Medium |
| REQ-AMD-011 | Bid/ask absorption rate features | P1 | Medium |
| REQ-AMD-012 | Manipulation event detection | P2 | High |
| REQ-AMD-013 | Walk-forward phase validation | P1 | Medium |
| REQ-AMD-014 | Phase-conditioned reporting | P1 | Low |

---

## 8. Validation & Testing Requirements

### 8.1 Unit Testing

- Each module must have >80% code coverage
- All edge cases documented and tested
- Performance benchmarks for critical paths

### 8.2 Integration Testing

- End-to-end pipeline tests with synthetic data
- Regression tests on historical data
- Lookahead contamination detection tests

### 8.3 Statistical Validation

- Out-of-sample performance must exceed baseline
- Statistical significance (p < 0.05) for key metrics
- Regime-conditioned performance analysis

### 8.4 Production Validation

- Paper trading for minimum 2 weeks before live
- Real-time latency monitoring
- Capital preservation verification in NOISE regimes

### 8.5 AMD-Specific Validation

- Phase detection accuracy vs proxy labels
- Walk-forward validation of phase transitions
- Comparison: phase-aware trading vs baseline
- Accumulation/Distribution discrimination accuracy
- Manipulation false positive rate monitoring

---

## 9. Future Extensions

### 9.1 Version 2 Candidates

| Feature | Description | Prerequisite |
|---------|-------------|--------------|
| MRMR Selection | Full redundancy-aware feature selection | Stable MI computation |
| PLS Reduction | Supervised dimensionality reduction | Feature ranking stable |
| GMM Clustering | Unsupervised regime discovery | Baseline regime detection working |
| Neural MI (MINE) | Higher-dimensional MI estimation | GPU infrastructure |
| Cross-Asset | Regime signals across correlated assets | Single-asset validation |

### 9.2 Research Extensions

- Transfer entropy for causal feature analysis
- Information bottleneck for optimal compression
- Multi-timeframe regime hierarchies
- Cross-asset AMD phase correlation
- Deep learning for phase transition prediction
- Reinforcement learning for AMD-aware position sizing

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **KSG** | Kraskov-Stögbauer-Grassberger mutual information estimator |
| **MI** | Mutual Information - measures statistical dependence between variables |
| **MRMR** | Minimum Redundancy Maximum Relevance - feature selection criterion |
| **PLS** | Partial Least Squares - supervised dimensionality reduction |
| **OMS** | Order Management System - executes trading decisions |
| **Regime** | Market state (TREND, MEAN_REVERT, NOISE) |
| **Walk-Forward** | Validation method respecting temporal ordering |
| **AMD** | Accumulation, Manipulation, Distribution - Wyckoff market phases |
| **Wyckoff** | Richard Wyckoff's method for analyzing market cycles via institutional activity |
| **HMM** | Hidden Markov Model - probabilistic model for sequences with hidden states |
| **Accumulation** | Phase where smart money quietly buys, price range-bound |
| **Distribution** | Phase where smart money quietly sells, price range-bound at top |
| **Markup** | Uptrend phase following accumulation |
| **Markdown** | Downtrend phase following distribution |
| **OFI** | Order Flow Imbalance - (buy_vol - sell_vol) / total_vol |
| **Absorption** | When one side of the order book absorbs pressure without price movement |
| **Viterbi** | Algorithm for finding most likely hidden state sequence in HMM |
| **Baum-Welch** | EM algorithm for learning HMM parameters |
| **Forward-Backward** | Algorithm for computing smoothed state probabilities in HMM |

---

## Appendix B: References

### Information Theory & MI Estimation
1. Kraskov, Stögbauer, Grassberger (2004) - "Estimating mutual information" - *Physical Review E*
2. Peng, Long, Ding (2005) - "Feature selection based on mutual information" - *IEEE TPAMI*
3. Cover & Thomas (2006) - "Elements of Information Theory" - Wiley

### Market Microstructure
4. Cont et al. (2014) - "Price impact and queue position" - *Quantitative Finance*
5. Avellaneda & Stoikov (2008) - "High-frequency trading in a limit order book" - *Quantitative Finance*

### Regime Detection & HMM
6. Hamilton (1989) - "A new approach to the economic analysis of nonstationary time series" - *Econometrica*
7. Rabiner (1989) - "A tutorial on hidden Markov models" - *Proceedings of the IEEE*
8. Ang & Timmermann (2012) - "Regime changes and financial markets" - *Annual Review of Financial Economics*

### Wyckoff Method & Market Phases
9. Wyckoff, R. (1931) - "The Richard D. Wyckoff Method of Trading in Stocks"
10. Pruden, H. (2007) - "The Three Skills of Top Trading" - Wiley
11. Schroeder, J. (2015) - "Detecting institutional order flow" - *Journal of Trading*

### Information Theory in Finance
12. Dionisio et al. (2004) - "Mutual information: a measure of dependency for nonlinear time series" - *Physica A*
13. Diks & Panchenko (2006) - "A new statistic for nonparametric Granger causality testing" - *Journal of Economic Dynamics*
14. Schreiber (2000) - "Measuring information transfer" - *Physical Review Letters*

---

**Document Control:**
- Created: 2026-01-15
- Updated: 2026-01-15 (Added AMD Phase Detection Subsystem)
- Author: System Architecture
- Status: Approved for MVP Implementation
- Next Review: After Phase 1 completion

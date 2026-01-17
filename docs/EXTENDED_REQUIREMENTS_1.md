# Extended Requirements: Information-Guided Adaptive Trading System

**Document Type:** Architecture Requirements & Implementation Specification
**Version:** 1.0
**Date:** 2026-01-17
**Status:** Draft - Pending Empirical Validation
**Predecessor:** EXTENDED_REQUIREMENTS_0.md v0.2

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Core Philosophy](#2-core-philosophy)
3. [Strategic Design Decisions](#3-strategic-design-decisions)
4. [Empirical Foundation](#4-empirical-foundation)
5. [System Architecture](#5-system-architecture)
6. [Module Specifications](#6-module-specifications)
7. [AMD Phase Detection Subsystem](#7-amd-phase-detection-subsystem)
8. [Temporal Structure Analysis](#8-temporal-structure-analysis)
9. [Data Infrastructure](#9-data-infrastructure)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [Implementation Requirements](#11-implementation-requirements)
12. [Validation & Testing Requirements](#12-validation--testing-requirements)
13. [Implementation Roadmap](#13-implementation-roadmap)
14. [Future Extensions](#14-future-extensions)
15. [Appendix A: Glossary](#appendix-a-glossary)
16. [Appendix B: References](#appendix-b-references)
17. [Appendix C: Empirical Results Template](#appendix-c-empirical-results-template)

---

## Document Changes from v0.2

| Section | Change Type | Description |
|---------|-------------|-------------|
| Section 4 | **NEW** | Empirical Foundation - validation before implementation |
| Section 8 | **NEW** | Temporal Structure Analysis - HMM as hypothesis |
| Section 9 | **NEW** | Data Infrastructure specification |
| Section 10 | **NEW** | Monitoring & Observability |
| Section 13 | **NEW** | Detailed Implementation Roadmap with milestones |
| Section 7 | **REVISED** | AMD repositioned as data-driven discovery |
| Appendix B | **EXPANDED** | 30+ academic references (was 14) |
| Appendix C | **NEW** | Empirical results template |
| All REQs | **REVISED** | Added acceptance criteria |

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
IT DOES NOT ASSUME STRUCTURE.
IT DISCOVERS AND VALIDATES STRUCTURE.
```

All trading decisions are gated by:
- **Measured Predictive Information** (Mutual Information via KSG estimator)
- **Regime Awareness** (Entropy-based regime detection)
- **Confidence-Weighted Execution** (Position sizing proportional to certainty)
- **Empirical Validation** (All hypotheses tested before deployment)

### 1.3 Key Differentiator: Hypothesis-Driven Development

This system treats theoretical concepts (Wyckoff cycles, regime persistence, feature informativeness) as **testable hypotheses**, not assumptions:

| Traditional Approach | Our Approach |
|---------------------|--------------|
| "Wyckoff says accumulation precedes markup" | "Test: Does OFI predict future breakouts?" |
| "Use HMM for phase detection" | "Test: Does temporal structure exist in phases?" |
| "These features should predict returns" | "Measure: Which features have significant MI?" |

### 1.4 MVP Scope

| Included in MVP | Deferred to v2+ |
|-----------------|-----------------|
| Empirical MI analysis of existing features | Neural MI estimation (MINE) |
| Entropy-based regime detection | Unsupervised regime clustering (GMM) |
| KSG MI feature ranking | MRMR redundancy pruning |
| Temporal structure hypothesis testing | Full HMM (only if validated) |
| Walk-forward validation framework | Cross-asset regime networks |
| Data-driven AMD phase labeling | Deep learning phase detection |
| Progress visualization dashboard | Reinforcement learning OMS |

---

## 2. Core Philosophy

### 2.1 Information-Theoretic Foundation

Markets are treated as **non-stationary information processes**. The system's approach:

1. **Features are hypotheses** about what predicts future returns
2. **MI measures truth** - how much uncertainty reduction does each feature provide?
3. **Regimes modulate informativeness** - features that work in trends may fail in noise
4. **Structure is discovered, not assumed** - temporal patterns must be validated empirically
5. **Adaptation is mandatory** - information content changes; the system must track this

### 2.2 Capital Preservation Principle

```
IF INFORMATION DISAPPEARS → EXPOSURE DISAPPEARS
IF HYPOTHESIS FAILS VALIDATION → HYPOTHESIS IS REJECTED
```

Trading in the absence of measured predictive information is considered a **bug**, not a feature. The system treats "going flat" in noise regimes as a **first-class outcome**, not a failure mode.

### 2.3 Robustness Over Optimization

The architecture prioritizes:
- **Statistical defensibility** over maximum backtest performance
- **Empirical validation** over theoretical elegance
- **Compute awareness** over brute-force approaches
- **Overfitting resistance** over curve-fitting
- **Interpretability** over black-box complexity

### 2.4 The Validation-First Principle

```
EVERY CLAIM REQUIRES EVIDENCE

Before implementing any component, answer:
1. What hypothesis does this component encode?
2. How do we measure if the hypothesis is true?
3. What is the null hypothesis (simpler alternative)?
4. What evidence would cause us to reject this component?
```

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
  ε         = small constant to prevent division by zero (1e-8)
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

**Acceptance Criteria:**
- Target computation matches reference implementation within 1e-6
- No lookahead bias (verified via temporal shuffle test)
- Handles missing prices gracefully (interpolation or exclusion)

### 3.2 Mutual Information Estimator

**Decision:** Use Kraskov-Stögbauer-Grassberger (KSG) estimator with bias corrections.

**Rationale:**
- Non-parametric (no distribution assumptions)
- Captures non-linear dependencies
- Well-established, extensively validated
- O(n log n) complexity with KD-trees

**Known Limitations (from literature):**
- Overestimates MI on autocorrelated data (Gao et al., 2015)
- Requires sufficient samples for convergence
- Sensitive to k parameter choice

**Mitigations:**
- Use block bootstrap for significance testing (preserves autocorrelation)
- Minimum 5000 samples per MI computation
- Cross-validate k parameter (default k=5, test k ∈ {3, 5, 7, 10})

**Usage Pattern:**
- Strictly **offline/periodic** computation
- Recomputed **weekly or monthly**
- NOT used for real-time signal generation

**Key Insight:** MI is a **selection pressure**, not a signal generator.

**Acceptance Criteria:**
- Matches sklearn.feature_selection.mutual_info_regression within 10% on IID Gaussian test data
- Correctly returns ~0 MI for independent variables
- Correctly returns ~log(σ_x/σ_{x|y}) for linear Gaussian case
- Processes 100k samples in <30 seconds

### 3.3 Regime Detection Method

**Decision:** Use entropy-based regime detection (existing `entropy.rs` module).

**Regime Categories:**
| Regime | Characteristics | Trading Approach |
|--------|----------------|------------------|
| TREND | Low entropy, high monotonicity | Momentum, trend-following |
| MEAN_REVERT | Moderate entropy, oscillating | Market-making, fade moves |
| NOISE | High entropy, unpredictable | Minimal/zero exposure |

**Validation Required:**
- Empirically measure time spent in each regime
- Validate regime persistence (not random flickering)
- Measure return distribution conditioned on regime
- Compare regime-conditional trading vs unconditional baseline

### 3.4 Temporal Structure Approach

**Decision:** Treat temporal structure (HMM) as a hypothesis, not an assumption.

**Hypothesis H1:** Phase transitions exhibit temporal dependencies
```
I(S_t; S_{t-1} | X_t) > 0
```

**Null Hypothesis H0:** Phases are memoryless given features
```
P(S_t | X_t, S_{t-1}) = P(S_t | X_t)
```

**Protocol:**
1. First implement memoryless classifier
2. Test H1 using MI gain from adding previous state
3. Only implement HMM if H1 is validated (p < 0.05)
4. If HMM implemented, learn all parameters from data

### 3.5 AMD Phase Detection Approach

**Decision:** Data-driven phase discovery, not Wyckoff pattern matching.

**Protocol:**
1. Create proxy labels based on **future outcomes** (not patterns)
2. Use MI to discover which features discriminate phases
3. Train classifier on discovered features
4. Compare learned transitions to Wyckoff theory (validation, not assumption)

---

## 4. Empirical Foundation

**This section must be completed BEFORE implementation of dependent modules.**

### 4.1 Data Summary

```
DATA INVENTORY

Source:           Binance WebSocket (BTCUSDT perpetual)
Collection Period: [TO BE FILLED]
Total Events:     ~73,000 (as of 2025-12-02)
Total Days:       ~47 days
Parquet Files:    ~97 files in ./data/features/

Feature Count:    90+ features in FeaturesSnapshot
Update Frequency: ~10 Hz (100ms)
```

### 4.2 Feature-Target MI Analysis

**Status:** PENDING

**Protocol:**
```python
for horizon in [10, 30, 100, 300]:  # ticks
    for feature in all_features:
        mi = compute_ksg_mi(feature_values, future_returns[horizon])
        p_value = block_permutation_test(mi, n_permutations=1000)
        record(feature, horizon, mi, p_value)
```

**Results Template:**

| Feature | Horizon | MI (bits) | p-value | Significant | Stable Across Time |
|---------|---------|-----------|---------|-------------|-------------------|
| order_flow_imbalance | 30 | ___ | ___ | ___ | ___ |
| tick_entropy_1s | 30 | ___ | ___ | ___ | ___ |
| tick_entropy_15m | 30 | ___ | ___ | ___ | ___ |
| vpin | 30 | ___ | ___ | ___ | ___ |
| ... | ... | ... | ... | ... | ... |

**Acceptance Criteria for Feature:**
- MI p-value < 0.05 (significant)
- MI > 0.01 bits (meaningful magnitude)
- Stable across 3+ non-overlapping time windows (robust)

### 4.3 Regime Detection Validation

**Status:** PENDING

**Metrics to Compute:**

| Metric | Value | Notes |
|--------|-------|-------|
| % time in TREND | ___% | |
| % time in MEAN_REVERT | ___% | |
| % time in NOISE | ___% | |
| Mean regime duration | ___ samples | Should be >> 1 |
| Regime transition entropy | ___ | Lower = more predictable |
| Return in TREND (mean ± std) | ___ | |
| Return in MEAN_REVERT (mean ± std) | ___ | |
| Return in NOISE (mean ± std) | ___ | Should be ~0 |

**Validation Checks:**
- [ ] Regimes are not random (persistence test)
- [ ] Return distribution differs by regime (KS test)
- [ ] Regime-conditional strategy outperforms unconditional

### 4.4 Baseline Strategy Performance

**Status:** PENDING

| Strategy | Sharpe | Max DD | Win Rate | Profit Factor |
|----------|--------|--------|----------|---------------|
| Buy & Hold | ___ | ___% | - | - |
| Random Entry | ___ | ___% | ___% | ___ |
| Entropy-gated (NOISE=flat) | ___ | ___% | ___% | ___ |
| MI top-5 features only | ___ | ___% | ___% | ___ |
| Full system | ___ | ___% | ___% | ___ |

**Success Criterion:** Full system Sharpe > Entropy-gated Sharpe > Random

### 4.5 Temporal Structure Test

**Status:** PENDING

**Hypothesis:** H1: I(S_t; S_{t-1} | X_t) > 0

| Test | Statistic | p-value | Conclusion |
|------|-----------|---------|------------|
| MI gain from S_{t-1} | ___ bits | ___ | ___ |
| Likelihood ratio (HMM vs memoryless) | ___ | ___ | ___ |
| Transition matrix entropy | ___ | ___ | ___ |

**Decision Rule:**
- If p < 0.05: Implement HMM with learned parameters
- If p >= 0.05: Use memoryless classifier (simpler)

### 4.6 AMD Phase Discriminability Test

**Status:** PENDING

**Question:** Can Accumulation and Distribution be distinguished using available features?

| Feature | MI with Phase Label | ACCUM vs DISTRIB AUC |
|---------|--------------------|-----------------------|
| order_flow_imbalance | ___ | ___ |
| aggr_ratio_50 | ___ | ___ |
| trade_imbalance | ___ | ___ |
| bid_depth_change | ___ | ___ |
| ... | ... | ... |

**Success Criterion:** At least one feature achieves AUC > 0.6 for ACCUM vs DISTRIB

---

## 5. System Architecture

### 5.1 Processing Pipeline

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
│  • tradeslog.rs → aggressor ratio, trade imbalance              │
│  • entropy.rs → tick entropy at multiple timescales             │
│  • toxicity.rs → VPIN, toxicity index                           │
│  • volatility.rs → realized volatility, vol of vol              │
│                                                                  │
│  Output: X_t ∈ ℝ^d (90+ features per timestamp)                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 2: TARGET LABELING                         │
│                                                                  │
│  Module: labeler.rs                                              │
│                                                                  │
│  • Compute Y_t for multiple horizons H ∈ {10, 30, 100, 300}     │
│  • Align (X_t, Y_{t+H}) pairs                                   │
│  • Strict no-lookahead time indexing                            │
│  • Create AMD proxy labels from future outcomes                  │
│                                                                  │
│  Output: Labeled dataset {(X_t, Y_t^H, Phase_t)}                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 3: MI COMPUTATION                          │
│                                                                  │
│  Module: mi.rs                                                   │
│                                                                  │
│  • Batch KSG MI estimation: I(X_i; Y) for each feature          │
│  • Block-bootstrap significance testing                         │
│  • Regime-conditioned MI (different rankings per regime)        │
│  • Phase-conditioned MI (for AMD feature discovery)             │
│                                                                  │
│  Output: MI scores with confidence intervals and p-values       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 4: FEATURE RANKING & SELECTION             │
│                                                                  │
│  Module: feature_rank.rs                                         │
│                                                                  │
│  • Rank features by MI score (per regime, per horizon)          │
│  • Track stability across time windows                          │
│  • Select top-k significant features for model                  │
│  • Alert on feature decay                                       │
│                                                                  │
│  Output: Feature importance tables, selected feature sets       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 5: MODEL TRAINING                          │
│                                                                  │
│  Module: model.rs                                                │
│                                                                  │
│  • Lightweight supervised model (logistic/MLP)                  │
│  • Walk-forward training with purged cross-validation           │
│  • Output: direction probability + calibrated confidence        │
│                                                                  │
│  Note: Model uses only MI-validated features                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 6: REGIME DETECTION                        │
│                                                                  │
│  Module: entropy.rs (existing, enhanced)                         │
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
│                 STAGE 7: AMD PHASE DETECTION                     │
│                                                                  │
│  Module: amd_detector.rs                                         │
│                                                                  │
│  • Phase classification using MI-discovered features            │
│  • Memoryless classifier (primary)                              │
│  • Optional: Temporal smoother (if H1 validated)                │
│  • Output confidence and supporting features                    │
│                                                                  │
│  Output: phase_t, P(phase_t), confidence, evidence              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 8: ORDER MANAGEMENT (OMS)                  │
│                                                                  │
│  Module: oms.rs (enhanced market_maker.rs)                       │
│                                                                  │
│  Decision Logic:                                                 │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ confidence = model_confidence × regime_probability          │ │
│  │                                                             │ │
│  │ if regime == NOISE or confidence < threshold:               │ │
│  │     position_size = 0  // Capital preservation              │ │
│  │ else:                                                       │ │
│  │     direction = model_prediction × amd_bias                 │ │
│  │     size = base_size × confidence × regime_multiplier       │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Output: Trading signals with position sizes and reasoning      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 9: CONTINUOUS MONITORING                   │
│                                                                  │
│  Module: monitor.rs                                              │
│                                                                  │
│  • Real-time performance metrics                                │
│  • MI decay detection                                           │
│  • Regime distribution shift detection                          │
│  • Automatic alerts on degradation                              │
│                                                                  │
│  Output: Metrics stream, alerts, adaptation triggers            │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Data Flow Diagram

```
                    ┌──────────────┐
                    │ Market Data  │
                    │  (Binance)   │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │Orderbook│  │ Trades  │  │ Funding │
        │ Engine  │  │ Engine  │  │  Rate   │
        └────┬────┘  └────┬────┘  └────┬────┘
              │            │            │
              └────────────┼────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  Features    │
                    │  Snapshot    │───────────────┐
                    └──────┬───────┘               │
                           │                       │
              ┌────────────┴────────────┐          │
              │                         │          │
              ▼                         ▼          ▼
        ┌──────────┐              ┌──────────┐  ┌──────────┐
        │ Labeler  │              │ Regime   │  │ Metrics  │
        │ (Offline)│              │ Detector │  │ Exporter │
        └────┬─────┘              │(Realtime)│  │ (WS/REST)│
             │                    └────┬─────┘  └──────────┘
             ▼                         │              │
        ┌──────────┐                   │              │
        │   MI     │                   │              │
        │ Compute  │                   │              │
        └────┬─────┘                   │              │
             │                         │              │
             ▼                         │              │
        ┌──────────┐                   │              │
        │ Feature  │                   │              │
        │  Rank    │                   │              │
        └────┬─────┘                   │              │
             │                         │              │
             ▼                         │              │
        ┌──────────┐                   │              │
        │  Model   │                   │              │
        │ Training │                   │              │
        └────┬─────┘                   │              │
             │                         │              │
             └─────────┬───────────────┘              │
                       │                              │
                       ▼                              │
                 ┌──────────┐                         │
                 │   OMS    │                         │
                 │(Combined)│                         │
                 └────┬─────┘                         │
                      │                               │
                      ▼                               │
                 ┌──────────┐                         │
                 │  Order   │                         │
                 │ Executor │                         │
                 └────┬─────┘                         │
                      │                               │
                      ▼                               ▼
                 ┌──────────┐                  ┌──────────┐
                 │  Trade   │                  │Dashboard │
                 │  Log     │                  │(Website) │
                 └──────────┘                  └──────────┘
```

---

## 6. Module Specifications

### 6.1 `labeler.rs` - Target Computation

**Purpose:** Compute forward-looking target variables Y for supervised learning.

**Interface:**
```rust
pub struct LabelConfig {
    /// Forecast horizons in ticks
    pub horizons: Vec<usize>,  // e.g., [10, 30, 100, 300]
    /// Minimum volatility floor (epsilon)
    pub vol_floor: f64,        // default: 1e-8
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
    pub features: HashMap<String, f64>,
    pub labels: HashMap<usize, f64>,  // horizon -> Y_{t+H}
    pub phase_label: Option<Phase>,   // For AMD training
}

pub trait Labeler {
    fn compute_labels(&self, prices: &[f64], config: &LabelConfig) -> Vec<LabeledSample>;
    fn create_phase_labels(&self, prices: &[f64], config: &PhaseLabelConfig) -> Vec<Option<Phase>>;
    fn align_features_labels(&self, features: &[FeatureSnapshot], labels: &[f64], horizon: usize)
        -> Vec<(HashMap<String, f64>, f64)>;
}
```

**Requirements:**

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-LAB-001 | Compute volatility-adjusted returns | P0 | Matches reference impl within 1e-6 |
| REQ-LAB-002 | Strict temporal alignment (no lookahead) | P0 | Passes temporal shuffle test |
| REQ-LAB-003 | Handle missing data gracefully | P1 | No panics on NaN/gaps |
| REQ-LAB-004 | Support multiple label types | P1 | All 4 types produce valid output |
| REQ-LAB-005 | Create AMD proxy labels from future returns | P1 | Labels correlate with actual outcomes |

---

### 6.2 `mi.rs` - Mutual Information Estimation

**Purpose:** Compute KSG mutual information between features and targets.

**Interface:**
```rust
pub struct MIConfig {
    /// Number of nearest neighbors for KSG
    pub k_neighbors: usize,        // default: 5
    /// Number of permutations for significance testing
    pub n_permutations: usize,     // default: 1000
    /// Block size for block bootstrap (preserves autocorrelation)
    pub block_size: usize,         // default: 50
    /// Significance level
    pub alpha: f64,                // default: 0.05
    /// Minimum samples required
    pub min_samples: usize,        // default: 5000
}

pub struct MIResult {
    pub feature_name: String,
    pub mi_nats: f64,              // MI in nats
    pub mi_bits: f64,              // MI in bits (mi_nats / ln(2))
    pub p_value: f64,              // from block permutation test
    pub significant: bool,         // p_value < alpha
    pub confidence_interval: (f64, f64),  // 95% CI
    pub n_samples: usize,
    pub k_used: usize,
}

pub trait MutualInformationEstimator {
    /// Compute MI between single feature and target
    fn compute_mi(&self, x: &[f64], y: &[f64], config: &MIConfig) -> MIResult;

    /// Compute MI for all features (parallelized)
    fn compute_mi_batch(&self, features: &HashMap<String, Vec<f64>>, target: &[f64],
                        config: &MIConfig) -> Vec<MIResult>;

    /// Compute MI conditioned on regime
    fn compute_mi_by_regime(&self, features: &HashMap<String, Vec<f64>>, target: &[f64],
                            regimes: &[Regime], config: &MIConfig)
        -> HashMap<Regime, Vec<MIResult>>;

    /// Compute MI gain from adding previous state (temporal structure test)
    fn compute_mi_gain_from_history(&self, features: &HashMap<String, Vec<f64>>,
                                     phase_labels: &[Phase], config: &MIConfig)
        -> TemporalStructureTestResult;
}

pub struct TemporalStructureTestResult {
    pub mi_without_history: f64,
    pub mi_with_history: f64,
    pub mi_gain: f64,
    pub p_value: f64,
    pub temporal_structure_significant: bool,
    pub recommended_model: TemporalModelType,
}

pub enum TemporalModelType {
    Memoryless,
    FirstOrderMarkov,
    HigherOrderMarkov,
}
```

**Requirements:**

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-MI-001 | Implement KSG estimator | P0 | Within 10% of sklearn on IID Gaussian |
| REQ-MI-002 | KD-tree neighbor search | P0 | O(n log n) complexity verified |
| REQ-MI-003 | Block permutation testing | P0 | Preserves autocorrelation structure |
| REQ-MI-004 | Handle edge cases | P1 | No crash on constant/NaN features |
| REQ-MI-005 | Regime-conditioned MI | P1 | Produces per-regime rankings |
| REQ-MI-006 | Bootstrap confidence intervals | P1 | 95% CI computed correctly |
| REQ-MI-007 | Temporal structure test | P1 | Correctly identifies MI gain |

---

### 6.3 `feature_rank.rs` - Feature Importance Tracking

**Purpose:** Maintain MI-based feature rankings across time and regimes.

**Interface:**
```rust
pub struct FeatureRankTable {
    /// MI scores indexed by (regime, horizon, feature)
    scores: HashMap<(Option<Regime>, usize, String), MIResult>,
    /// Temporal stability metrics
    stability: HashMap<String, StabilityMetrics>,
    /// Selected feature sets per regime/horizon
    selected_features: HashMap<(Option<Regime>, usize), Vec<String>>,
    /// Last update timestamp
    last_updated: i64,
}

pub struct StabilityMetrics {
    pub mean_mi: f64,
    pub std_mi: f64,
    pub coefficient_of_variation: f64,
    pub rank_correlation_over_time: f64,  // Spearman
    pub n_windows_significant: usize,
    pub n_windows_total: usize,
}

pub struct FeatureSelectionConfig {
    pub max_features: usize,      // default: 10
    pub min_mi_bits: f64,         // default: 0.01
    pub min_stability: f64,       // default: 0.5 (50% of windows significant)
    pub correlation_threshold: f64, // default: 0.7 (remove correlated)
}

pub trait FeatureRanker {
    fn get_top_features(&self, regime: Option<Regime>, horizon: usize, k: usize) -> Vec<String>;
    fn select_features(&self, regime: Option<Regime>, horizon: usize,
                       config: &FeatureSelectionConfig) -> Vec<String>;
    fn update_rankings(&mut self, mi_results: &[MIResult], regime: Option<Regime>,
                       horizon: usize, window_id: usize);
    fn stability_report(&self) -> Vec<(String, StabilityMetrics)>;
    fn detect_decay(&self, threshold: f64) -> Vec<String>;
}
```

**Requirements:**

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-RANK-001 | MI rankings by regime/horizon | P0 | Correct indexing verified |
| REQ-RANK-002 | Temporal stability tracking | P1 | Spearman computed correctly |
| REQ-RANK-003 | Feature decay alerting | P1 | Alert when stability < threshold |
| REQ-RANK-004 | Efficient lookup | P0 | O(1) for top-k query |
| REQ-RANK-005 | Persistence to disk | P1 | JSON/Parquet serialization |
| REQ-RANK-006 | Correlation-based pruning | P1 | Removes redundant features |

---

### 6.4 `model.rs` - Supervised Learning

**Purpose:** Train lightweight models on MI-selected features.

**Interface:**
```rust
pub struct ModelConfig {
    pub model_type: ModelType,
    pub feature_names: Vec<String>,
    pub regularization: f64,
    pub calibrate_confidence: bool,
}

pub enum ModelType {
    LogisticRegression,
    LinearRegression,
    MLP { hidden_sizes: Vec<usize>, dropout: f64 },
}

pub struct Prediction {
    pub direction: f64,           // -1 to +1
    pub confidence: f64,          // 0 to 1 (calibrated if configured)
    pub raw_score: f64,           // model output before calibration
    pub feature_contributions: HashMap<String, f64>,  // interpretability
}

pub struct WalkForwardConfig {
    pub n_folds: usize,
    pub train_window: usize,
    pub test_window: usize,
    pub purge_gap: usize,         // gap between train and test
    pub embargo: usize,           // additional buffer
}

pub trait TradingModel {
    fn train(&mut self, features: &[HashMap<String, f64>], targets: &[f64]) -> Result<(), ModelError>;
    fn predict(&self, features: &HashMap<String, f64>) -> Prediction;
    fn feature_importance(&self) -> Vec<(String, f64)>;
    fn save(&self, path: &Path) -> Result<(), IOError>;
    fn load(path: &Path) -> Result<Self, IOError> where Self: Sized;
}

pub trait WalkForwardValidator {
    fn validate(&self, data: &LabeledDataset, model_config: &ModelConfig,
                wf_config: &WalkForwardConfig) -> WalkForwardResult;
}

pub struct WalkForwardResult {
    pub fold_results: Vec<FoldResult>,
    pub aggregate_sharpe: f64,
    pub aggregate_accuracy: f64,
    pub train_test_gap: f64,  // overfitting indicator
}
```

**Requirements:**

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-MOD-001 | Logistic regression implementation | P0 | Matches sklearn within 1% |
| REQ-MOD-002 | Walk-forward validation | P0 | Correct temporal splits |
| REQ-MOD-003 | Calibrated confidence (Platt scaling) | P1 | Calibration plot is diagonal |
| REQ-MOD-004 | Feature importance extraction | P1 | Coefficients accessible |
| REQ-MOD-005 | Model persistence (ONNX export) | P1 | Load/save round-trip works |
| REQ-MOD-006 | MLP support | P2 | 2-layer MLP trainable |

---

### 6.5 `oms.rs` - Order Management System

**Purpose:** Execute regime-aware, confidence-weighted trading decisions.

**Interface:**
```rust
pub struct OMSConfig {
    pub base_size: f64,
    pub max_size: f64,
    pub min_confidence: f64,       // default: 0.6
    pub noise_regime_exposure: f64, // default: 0.0 (flat in noise)
    pub regime_params: HashMap<Regime, RegimeParams>,
}

pub struct RegimeParams {
    pub size_multiplier: f64,
    pub strategy: Strategy,
    pub stop_loss_atr_mult: f64,
    pub take_profit_atr_mult: f64,
}

pub enum Strategy {
    Momentum,
    MeanReversion,
    MarketMaking { spread_bps: f64, skew_factor: f64 },
    Flat,
}

pub struct TradingDecision {
    pub action: Action,
    pub size: f64,
    pub confidence: f64,
    pub regime: Regime,
    pub phase: Option<Phase>,
    pub reasoning: DecisionReasoning,
}

pub struct DecisionReasoning {
    pub model_prediction: f64,
    pub model_confidence: f64,
    pub regime_probability: f64,
    pub phase_probability: Option<f64>,
    pub top_features: Vec<(String, f64)>,
    pub risk_adjustments: Vec<String>,
}

pub enum Action {
    Buy,
    Sell,
    Hold,
    Flatten,
    ReduceExposure { target_fraction: f64 },
}

pub trait OrderManagement {
    fn decide(&self, prediction: &Prediction, regime: Regime, regime_prob: f64,
              phase: Option<Phase>, phase_prob: Option<f64>,
              current_position: f64, current_pnl: f64) -> TradingDecision;
    fn apply_risk_limits(&self, decision: TradingDecision,
                         portfolio_state: &PortfolioState) -> TradingDecision;
}
```

**Requirements:**

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-OMS-001 | Regime-aware sizing | P0 | Different sizes per regime |
| REQ-OMS-002 | Zero exposure in NOISE | P0 | position=0 when regime=NOISE |
| REQ-OMS-003 | Confidence weighting | P0 | size ∝ confidence verified |
| REQ-OMS-004 | Position limits | P0 | Never exceeds max_size |
| REQ-OMS-005 | Decision logging | P1 | All decisions logged with reasoning |
| REQ-OMS-006 | AMD phase integration | P1 | Phase affects direction bias |

---

## 7. AMD Phase Detection Subsystem

### 7.1 Philosophical Approach

**Key Principle:** AMD phase detection is a **data-driven discovery** process, not pattern matching against Wyckoff theory.

```
WRONG APPROACH:
"Wyckoff says accumulation looks like X, so detect X"

CORRECT APPROACH:
"What feature patterns precede upward breakouts?
 If consistent, call that pattern 'accumulation' and test if it matches Wyckoff"
```

### 7.2 Phase Definition (Outcome-Based)

Phases are defined by **future outcomes**, not current patterns:

```rust
/// Phase labels are created from FUTURE information (training only)
pub fn create_phase_label(
    current_idx: usize,
    prices: &[f64],
    volatility: &[f64],
    forward_window: usize,  // e.g., 300 ticks
    breakout_threshold: f64, // e.g., 2.0 standard deviations
) -> Option<Phase> {
    let future_return = (prices[current_idx + forward_window] - prices[current_idx])
                        / prices[current_idx];
    let future_vol = volatility[current_idx..current_idx + forward_window].mean();
    let current_vol = volatility[current_idx - forward_window..current_idx].mean();

    let normalized_return = future_return / future_vol;
    let vol_ratio = current_vol / future_vol;

    // Low current volatility + positive future breakout = Accumulation
    if vol_ratio < 0.8 && normalized_return > breakout_threshold {
        return Some(Phase::Accumulation);
    }

    // Low current volatility + negative future breakout = Distribution
    if vol_ratio < 0.8 && normalized_return < -breakout_threshold {
        return Some(Phase::Distribution);
    }

    // High volatility + positive return = Markup
    if vol_ratio >= 0.8 && normalized_return > breakout_threshold / 2.0 {
        return Some(Phase::Markup);
    }

    // High volatility + negative return = Markdown
    if vol_ratio >= 0.8 && normalized_return < -breakout_threshold / 2.0 {
        return Some(Phase::Markdown);
    }

    // Ambiguous - don't use for training
    None
}
```

### 7.3 Feature Discovery Protocol

```
STEP 1: Create Proxy Labels
        └── Label historical data using future outcomes
        └── This is allowed because labels are for TRAINING only

STEP 2: MI Feature Analysis
        └── For each feature, compute I(feature; phase_label)
        └── Identify features that discriminate phases
        └── Key question: Can ACCUM vs DISTRIB be separated?

STEP 3: Train Classifier
        └── Use MI-selected features
        └── Walk-forward validation
        └── Classifier predicts phase from CURRENT features only

STEP 4: Validate Predictive Power
        └── Does predicted phase correlate with future returns?
        └── Is phase-aware trading better than phase-unaware?

STEP 5: Compare to Wyckoff Theory
        └── Do learned patterns match Wyckoff descriptions?
        └── This is VALIDATION, not assumption
```

### 7.4 Module Specification: `amd_detector.rs`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    Accumulation,
    Markup,
    Distribution,
    Markdown,
    Unknown,
}

pub struct AMDConfig {
    /// Features to use (from MI analysis)
    pub selected_features: Vec<String>,
    /// Minimum confidence to report phase
    pub min_confidence: f64,
    /// Whether to use temporal smoothing
    pub use_temporal_smoothing: bool,
    /// Smoothing parameter (if enabled)
    pub smoothing_alpha: f64,
}

pub struct PhaseDetectionResult {
    pub phase: Phase,
    pub confidence: f64,
    pub phase_probabilities: HashMap<Phase, f64>,
    pub supporting_features: Vec<(String, f64)>,
    pub raw_classifier_output: HashMap<Phase, f64>,
    pub smoothed: bool,
}

pub trait AMDPhaseDetector {
    /// Train detector on labeled data
    fn train(&mut self, features: &[HashMap<String, f64>],
             phase_labels: &[Phase]) -> Result<(), TrainError>;

    /// Detect phase from current features
    fn detect(&self, features: &HashMap<String, f64>) -> PhaseDetectionResult;

    /// Get feature importance for phase discrimination
    fn feature_importance(&self) -> HashMap<Phase, Vec<(String, f64)>>;

    /// Compare learned patterns to Wyckoff theory
    fn wyckoff_comparison(&self) -> WyckoffComparisonReport;
}

pub struct WyckoffComparisonReport {
    /// Does OFI > 0 correlate with Accumulation?
    pub ofi_accumulation_correlation: f64,
    /// Does OFI < 0 correlate with Distribution?
    pub ofi_distribution_correlation: f64,
    /// Do phases follow expected sequence?
    pub sequence_adherence: f64,
    /// Detailed constraint tests
    pub constraint_tests: Vec<ConstraintTest>,
}

pub struct ConstraintTest {
    pub name: String,
    pub description: String,
    pub expected_by_wyckoff: String,
    pub observed_in_data: String,
    pub agrees: bool,
    pub p_value: f64,
}
```

### 7.5 Required Features for AMD

| Feature | Source | Critical For | Status |
|---------|--------|--------------|--------|
| `order_flow_imbalance` | orderbook.rs | ACCUM vs DISTRIB | Exists |
| `aggr_ratio_50` | tradeslog.rs | Aggressor direction | Exists |
| `trade_imbalance` | tradeslog.rs | Flow direction | Exists |
| `tick_entropy_1s` | entropy.rs | Short-term noise | Exists |
| `tick_entropy_15m` | entropy.rs | Long-term direction | Exists |
| `realized_volatility_100` | volatility.rs | Consolidation detection | Exists |
| `vpin` | toxicity.rs | Informed flow | Exists |
| `ofi_persistence` | **NEW** | Flow consistency | To implement |
| `entropy_ratio` | **NEW** | Timescale divergence | To implement |
| `price_range_percentile` | **NEW** | Range position | To implement |
| `absorption_rate` | **NEW** | Order absorption | To implement |

### 7.6 AMD Requirements

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-AMD-001 | Proxy phase labeling | P1 | Labels correlate with future returns |
| REQ-AMD-002 | MI feature discovery | P1 | Top features identified |
| REQ-AMD-003 | ACCUM vs DISTRIB discrimination | P0 | AUC > 0.6 required |
| REQ-AMD-004 | Phase classifier training | P1 | Walk-forward accuracy > 50% |
| REQ-AMD-005 | Real-time phase detection | P1 | <10ms latency |
| REQ-AMD-006 | Wyckoff comparison report | P2 | Report generated |
| REQ-AMD-007 | New derived features | P1 | 4 new features implemented |

---

## 8. Temporal Structure Analysis

### 8.1 The Hypothesis Framework

Temporal structure (HMM) is treated as a **testable hypothesis**, not an assumption.

```
HYPOTHESIS HIERARCHY

H0 (Null): Phases are memoryless given features
    P(S_t | X_t, S_{t-1}) = P(S_t | X_t)
    → Use simple classifier

H1 (First-order): Previous phase provides information
    I(S_t; S_{t-1} | X_t) > 0
    → Use HMM or EMA smoothing

H2 (Higher-order): Longer history matters
    I(S_t; S_{t-1}, S_{t-2}, ... | X_t) > I(S_t; S_{t-1} | X_t)
    → Use LSTM or Transformer
```

### 8.2 Temporal Structure Test Protocol

```python
def test_temporal_structure(features, phase_labels, config):
    """
    Test whether temporal structure exists in phase transitions.

    Returns recommendation for which model to use.
    """

    # Step 1: Train memoryless classifier
    classifier_h0 = train_classifier(features, phase_labels)
    preds_h0 = classifier_h0.predict_proba(features)
    mi_h0 = mutual_information(preds_h0, phase_labels)

    # Step 2: Add previous phase as feature
    features_with_lag = add_lagged_labels(features, phase_labels, lag=1)
    classifier_h1 = train_classifier(features_with_lag, phase_labels)
    preds_h1 = classifier_h1.predict_proba(features_with_lag)
    mi_h1 = mutual_information(preds_h1, phase_labels)

    # Step 3: Statistical test for MI gain
    mi_gain = mi_h1 - mi_h0
    p_value = block_permutation_test_mi_gain(
        features, phase_labels,
        n_permutations=1000,
        block_size=config.block_size
    )

    # Step 4: Decision
    if p_value >= 0.05:
        return TemporalModelRecommendation(
            model_type=TemporalModelType.Memoryless,
            mi_gain=mi_gain,
            p_value=p_value,
            reasoning="No significant temporal structure detected"
        )
    elif mi_gain < 0.05:  # bits
        return TemporalModelRecommendation(
            model_type=TemporalModelType.EMASmoother,
            mi_gain=mi_gain,
            p_value=p_value,
            reasoning="Weak temporal structure - simple smoothing sufficient"
        )
    else:
        return TemporalModelRecommendation(
            model_type=TemporalModelType.LearnedHMM,
            mi_gain=mi_gain,
            p_value=p_value,
            reasoning="Strong temporal structure - HMM recommended"
        )
```

### 8.3 Learned HMM Specification

**If and only if** temporal structure is validated (p < 0.05), implement HMM with **learned parameters**:

```rust
pub struct LearnedHMM {
    n_states: usize,
    /// ALL parameters learned from data
    transition_matrix: Vec<Vec<f64>>,
    initial_distribution: Vec<f64>,
    /// No hard-coded Wyckoff assumptions
}

impl LearnedHMM {
    /// Initialize with uniform prior (no assumptions)
    pub fn new(n_states: usize) -> Self {
        let uniform = 1.0 / n_states as f64;
        Self {
            n_states,
            transition_matrix: vec![vec![uniform; n_states]; n_states],
            initial_distribution: vec![uniform; n_states],
        }
    }

    /// Learn from classifier probability outputs
    pub fn fit(&mut self, classifier_outputs: &[Vec<f64>], max_iterations: usize) {
        for _ in 0..max_iterations {
            // E-step: Forward-backward
            let (alpha, beta, gamma, xi) = self.forward_backward(classifier_outputs);

            // M-step: Update parameters
            self.update_initial(&gamma);
            self.update_transitions(&xi);

            // Check convergence
            if self.converged() {
                break;
            }
        }
    }

    /// Smooth classifier outputs
    pub fn smooth(&self, classifier_outputs: &[Vec<f64>]) -> Vec<Vec<f64>> {
        let (_, _, gamma, _) = self.forward_backward(classifier_outputs);
        gamma
    }

    /// Compare learned transitions to Wyckoff theory
    pub fn compare_to_wyckoff(&self) -> TransitionComparisonReport {
        let wyckoff_expected = Self::wyckoff_theoretical_matrix();

        TransitionComparisonReport {
            learned: self.transition_matrix.clone(),
            theoretical: wyckoff_expected,
            kl_divergence: kl_div(&self.transition_matrix, &wyckoff_expected),
            element_wise_comparison: self.compare_elements(&wyckoff_expected),
        }
    }

    fn wyckoff_theoretical_matrix() -> Vec<Vec<f64>> {
        // Wyckoff theory suggests:
        // ACCUM -> MARKUP (likely), ACCUM -> DISTRIB (unlikely)
        // This is for COMPARISON, not initialization
        vec![
            vec![0.85, 0.15, 0.00, 0.00],  // From ACCUM
            vec![0.00, 0.80, 0.20, 0.00],  // From MARKUP
            vec![0.00, 0.00, 0.85, 0.15],  // From DISTRIB
            vec![0.15, 0.00, 0.00, 0.85],  // From MARKDOWN
        ]
    }
}
```

### 8.4 Temporal Structure Requirements

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-TEMP-001 | Temporal structure test | P1 | Correct p-value computation |
| REQ-TEMP-002 | Memoryless baseline | P0 | Working classifier without history |
| REQ-TEMP-003 | EMA smoother | P1 | Configurable alpha parameter |
| REQ-TEMP-004 | Learned HMM | P2 | Baum-Welch converges correctly |
| REQ-TEMP-005 | Wyckoff comparison | P2 | Report compares learned vs theory |
| REQ-TEMP-006 | Model selection | P1 | Automatic based on test result |

---

## 9. Data Infrastructure

### 9.1 Data Schema

**Feature Parquet Schema:**
```
features_YYYYMMDD_HHMMSS.parquet
├── timestamp: INT64 (Unix ms)
├── mid_price: DOUBLE
├── spread: DOUBLE
├── imbalance: DOUBLE
├── order_flow_imbalance: DOUBLE
├── order_flow_pressure: DOUBLE
├── tick_entropy_1s: DOUBLE
├── tick_entropy_5s: DOUBLE
├── tick_entropy_15s: DOUBLE
├── tick_entropy_1m: DOUBLE
├── tick_entropy_5m: DOUBLE
├── tick_entropy_15m: DOUBLE
├── tick_entropy_1h: DOUBLE
├── vpin: DOUBLE
├── toxicity_index: DOUBLE
├── realized_volatility_10: DOUBLE
├── realized_volatility_100: DOUBLE
├── ... (90+ columns)
└── regime: STRING (optional)
```

**MI Results Schema:**
```json
{
  "computation_id": "uuid",
  "timestamp": "ISO8601",
  "config": {
    "k_neighbors": 5,
    "n_permutations": 1000,
    "block_size": 50
  },
  "horizon": 30,
  "regime": "ALL",
  "results": [
    {
      "feature": "order_flow_imbalance",
      "mi_bits": 0.023,
      "p_value": 0.003,
      "significant": true,
      "ci_lower": 0.018,
      "ci_upper": 0.028
    }
  ]
}
```

### 9.2 Data Versioning

**Using DVC (Data Version Control):**

```yaml
# dvc.yaml
stages:
  extract_features:
    cmd: cargo run --release --bin feature_extractor -- --output data/features/
    deps:
      - src/features/
    outs:
      - data/features/

  compute_labels:
    cmd: cargo run --release --bin labeler -- --input data/features/ --output data/labeled/
    deps:
      - data/features/
      - src/labeler.rs
    params:
      - labeling.horizons
      - labeling.vol_floor
    outs:
      - data/labeled/

  compute_mi:
    cmd: cargo run --release --bin mi_compute -- --input data/labeled/ --output data/mi/
    deps:
      - data/labeled/
      - src/mi.rs
    params:
      - mi.k_neighbors
      - mi.n_permutations
    outs:
      - data/mi/
    metrics:
      - data/mi/summary.json:
          cache: false

  train_model:
    cmd: cargo run --release --bin train -- --features data/mi/selected.json --output models/
    deps:
      - data/mi/
      - data/labeled/
    params:
      - model.type
      - model.regularization
    outs:
      - models/
    metrics:
      - models/metrics.json:
          cache: false
```

### 9.3 Feature Registry

| Feature Name | Module | Update Freq | Dependencies | MI Validated |
|--------------|--------|-------------|--------------|--------------|
| `mid_price` | orderbook.rs | 100ms | orderbook | N/A (target) |
| `spread` | orderbook.rs | 100ms | orderbook | PENDING |
| `imbalance` | orderbook.rs | 100ms | orderbook | PENDING |
| `order_flow_imbalance` | orderbook.rs | 100ms | trades | PENDING |
| `tick_entropy_1s` | entropy.rs | 100ms | tick_buffer | PENDING |
| `tick_entropy_15m` | entropy.rs | 100ms | tick_buffer | PENDING |
| `vpin` | toxicity.rs | 100ms | trades | PENDING |
| `aggr_ratio_50` | tradeslog.rs | 100ms | trades | PENDING |

### 9.4 Data Quality Checks

```rust
pub struct DataQualityReport {
    pub total_samples: usize,
    pub missing_values: HashMap<String, usize>,
    pub nan_values: HashMap<String, usize>,
    pub inf_values: HashMap<String, usize>,
    pub constant_features: Vec<String>,
    pub high_correlation_pairs: Vec<(String, String, f64)>,
    pub timestamp_gaps: Vec<(i64, i64, Duration)>,
    pub quality_score: f64,  // 0-1
}

pub trait DataQualityChecker {
    fn check_quality(&self, data: &DataFrame) -> DataQualityReport;
    fn filter_invalid(&self, data: DataFrame) -> DataFrame;
}
```

### 9.5 Data Infrastructure Requirements

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-DATA-001 | Parquet schema definition | P0 | Schema documented and validated |
| REQ-DATA-002 | DVC pipeline setup | P1 | Reproducible data pipeline |
| REQ-DATA-003 | Feature registry | P1 | All features catalogued |
| REQ-DATA-004 | Quality checks | P1 | Report generated for each dataset |
| REQ-DATA-005 | Gap handling | P1 | Gaps detected and handled |

---

## 10. Monitoring & Observability

### 10.1 Metrics Specification

**System Metrics (Prometheus format):**
```
# HELP ingestor_features_processed_total Total feature snapshots processed
# TYPE ingestor_features_processed_total counter
ingestor_features_processed_total{symbol="BTCUSDT"} 1234567

# HELP ingestor_regime_current Current detected regime
# TYPE ingestor_regime_current gauge
ingestor_regime_current{regime="TREND"} 1
ingestor_regime_current{regime="MEAN_REVERT"} 0
ingestor_regime_current{regime="NOISE"} 0

# HELP ingestor_phase_current Current detected AMD phase
# TYPE ingestor_phase_current gauge
ingestor_phase_current{phase="ACCUMULATION"} 1
ingestor_phase_current{phase="MARKUP"} 0

# HELP ingestor_model_confidence Current model confidence
# TYPE ingestor_model_confidence gauge
ingestor_model_confidence 0.73

# HELP ingestor_position_size Current position size
# TYPE ingestor_position_size gauge
ingestor_position_size 0.5

# HELP ingestor_pnl_unrealized Unrealized PnL
# TYPE ingestor_pnl_unrealized gauge
ingestor_pnl_unrealized 123.45
```

**Trading Metrics:**
```
# HELP trading_sharpe_rolling Rolling Sharpe ratio
# TYPE trading_sharpe_rolling gauge
trading_sharpe_rolling{window="1d"} 1.23
trading_sharpe_rolling{window="7d"} 0.89

# HELP trading_drawdown_current Current drawdown
# TYPE trading_drawdown_current gauge
trading_drawdown_current -0.05

# HELP trading_trades_total Total trades executed
# TYPE trading_trades_total counter
trading_trades_total{direction="buy"} 234
trading_trades_total{direction="sell"} 218
```

### 10.2 Dashboard Specification

**Page 1: Implementation Progress**
```
┌─────────────────────────────────────────────────────────────────────────┐
│                    IMPLEMENTATION PROGRESS                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Overall Progress: [████████████░░░░░░░░] 58%                           │
│                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │ Milestone 1     │  │ Milestone 2     │  │ Milestone 3     │         │
│  │ Empirical       │  │ Core System     │  │ AMD Detection   │         │
│  │ Foundation      │  │                 │  │                 │         │
│  │ ✅ COMPLETE     │  │ 🔄 IN PROGRESS  │  │ ⏳ PENDING      │         │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘         │
│                                                                          │
│  Recent Activity:                                                        │
│  • 2026-01-17: REQ-MI-001 completed                                     │
│  • 2026-01-16: REQ-LAB-002 completed                                    │
│  • 2026-01-15: REQ-DATA-001 completed                                   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Page 2: Empirical Results**
```
┌─────────────────────────────────────────────────────────────────────────┐
│                    EMPIRICAL RESULTS                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Feature MI Rankings (Horizon: 30 ticks)                                │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ Feature                    MI (bits)   Significant   Stable    │    │
│  │ ─────────────────────────────────────────────────────────────  │    │
│  │ order_flow_imbalance       0.023       ✅            ✅        │    │
│  │ tick_entropy_15m           0.018       ✅            ✅        │    │
│  │ vpin                       0.015       ✅            ⚠️        │    │
│  │ aggr_ratio_50              0.012       ✅            ✅        │    │
│  │ spread                     0.003       ❌            -         │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  Regime Distribution            Baseline Performance                     │
│  ┌──────────────────┐          ┌──────────────────────────────┐        │
│  │ TREND: 35%       │          │ Strategy        Sharpe  MaxDD │        │
│  │ MEAN_REVERT: 40% │          │ ─────────────────────────────│        │
│  │ NOISE: 25%       │          │ Buy & Hold      0.12   -42%  │        │
│  └──────────────────┘          │ Entropy-gated   0.45   -18%  │        │
│                                │ Full system     0.78   -12%  │        │
│                                └──────────────────────────────┘        │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Page 3: Live Monitor**
```
┌─────────────────────────────────────────────────────────────────────────┐
│                    LIVE SYSTEM MONITOR                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Current State                  Position & PnL                          │
│  ┌──────────────────┐          ┌──────────────────────────────┐        │
│  │ Regime: TREND    │          │ Position:    0.5 BTC          │        │
│  │ Phase: MARKUP    │          │ Entry:       $42,150          │        │
│  │ Confidence: 73%  │          │ Current:     $42,380          │        │
│  │ Updated: 0.1s    │          │ Unrealized:  +$115.00         │        │
│  └──────────────────┘          │ Today:       +$342.50         │        │
│                                └──────────────────────────────┘        │
│                                                                          │
│  Feature Heatmap (Top 10 by MI)                                         │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ order_flow_imbalance  [████████░░]  +0.32 (bullish)            │    │
│  │ tick_entropy_15m      [███░░░░░░░]  0.28 (low, directional)    │    │
│  │ vpin                  [██████░░░░]  0.58 (elevated)            │    │
│  │ aggr_ratio_50         [███████░░░]  0.67 (buy-heavy)           │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  Equity Curve (7 days)                                                  │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │    ╱╲    ╱╲                                                    │    │
│  │   ╱  ╲  ╱  ╲   ╱╲  ╱                                          │    │
│  │  ╱    ╲╱    ╲ ╱  ╲╱                                           │    │
│  │ ╱              ╲                                               │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 10.3 Alerting Thresholds

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| MI Decay | Top feature MI drops >50% | Warning | Review features |
| Regime Stuck | Same regime >24h | Info | Manual review |
| Model Degradation | Rolling Sharpe <0 for 7d | Critical | Halt trading |
| Data Gap | No data >5 minutes | Critical | Check connection |
| High Drawdown | DD >15% | Warning | Reduce exposure |

### 10.4 Monitoring Requirements

| ID | Requirement | Priority | Acceptance Criteria |
|----|-------------|----------|---------------------|
| REQ-MON-001 | Prometheus metrics export | P1 | All metrics exposed |
| REQ-MON-002 | WebSocket live feed | P1 | <100ms latency |
| REQ-MON-003 | Progress dashboard | P1 | Shows milestone status |
| REQ-MON-004 | Results dashboard | P1 | Shows MI rankings |
| REQ-MON-005 | Live monitor | P1 | Real-time updates |
| REQ-MON-006 | Alerting | P2 | Alerts sent to Slack/email |

---

## 11. Implementation Requirements

### 11.1 Phase 1: Empirical Foundation

| ID | Requirement | Priority | Complexity | Acceptance Criteria |
|----|-------------|----------|------------|---------------------|
| REQ-EMP-001 | Data inventory and quality report | P0 | Low | Report generated |
| REQ-EMP-002 | Feature-target MI analysis | P0 | High | All features scored |
| REQ-EMP-003 | Regime validation metrics | P0 | Medium | Metrics computed |
| REQ-EMP-004 | Baseline strategy comparison | P0 | Medium | 3 baselines compared |
| REQ-EMP-005 | Temporal structure test | P0 | Medium | H0/H1 decision made |
| REQ-EMP-006 | AMD discriminability test | P0 | Medium | AUC computed |

### 11.2 Phase 2: Core Infrastructure

| ID | Requirement | Priority | Complexity | Acceptance Criteria |
|----|-------------|----------|------------|---------------------|
| REQ-LAB-001 | Volatility-adjusted labeler | P0 | Medium | Matches reference |
| REQ-LAB-002 | No-lookahead verification | P0 | Low | Passes shuffle test |
| REQ-MI-001 | KSG estimator | P0 | High | Within 10% of sklearn |
| REQ-MI-003 | Block permutation test | P0 | Medium | Preserves autocorrelation |
| REQ-RANK-001 | Feature rankings | P0 | Medium | Correct indexing |
| REQ-MOD-001 | Logistic regression | P0 | Medium | Matches sklearn |
| REQ-MOD-002 | Walk-forward validation | P0 | Medium | Correct splits |

### 11.3 Phase 3: Trading System

| ID | Requirement | Priority | Complexity | Acceptance Criteria |
|----|-------------|----------|------------|---------------------|
| REQ-OMS-001 | Regime-aware sizing | P0 | Medium | Different per regime |
| REQ-OMS-002 | Zero in NOISE | P0 | Low | Verified flat |
| REQ-OMS-005 | Decision logging | P1 | Low | JSON logs |
| REQ-VAL-001 | Walk-forward backtest | P0 | Medium | Metrics computed |
| REQ-VAL-002 | Transaction costs | P0 | Low | Costs included |

### 11.4 Phase 4: AMD Detection

| ID | Requirement | Priority | Complexity | Acceptance Criteria |
|----|-------------|----------|------------|---------------------|
| REQ-AMD-001 | Proxy phase labeling | P1 | Medium | Labels correlate |
| REQ-AMD-002 | MI feature discovery | P1 | Medium | Top features found |
| REQ-AMD-003 | ACCUM vs DISTRIB | P0 | High | AUC >0.6 |
| REQ-AMD-004 | Phase classifier | P1 | Medium | Accuracy >50% |
| REQ-AMD-007 | Derived features | P1 | Medium | 4 features added |

### 11.5 Phase 5: Monitoring & Dashboard

| ID | Requirement | Priority | Complexity | Acceptance Criteria |
|----|-------------|----------|------------|---------------------|
| REQ-MON-001 | Prometheus metrics | P1 | Low | Metrics exposed |
| REQ-MON-002 | WebSocket feed | P1 | Medium | <100ms latency |
| REQ-MON-003 | Progress dashboard | P1 | Medium | Milestones visible |
| REQ-MON-004 | Results dashboard | P1 | Medium | MI rankings shown |
| REQ-MON-005 | Live monitor | P1 | Medium | Real-time updates |

---

## 12. Validation & Testing Requirements

### 12.1 Unit Testing

- Each module must have >80% code coverage
- All edge cases documented and tested
- Performance benchmarks for critical paths (MI computation, predictions)

### 12.2 Integration Testing

- End-to-end pipeline tests with synthetic data
- Regression tests on historical data
- Lookahead contamination detection tests (temporal shuffle)

### 12.3 Statistical Validation

- Out-of-sample performance must exceed baseline
- Statistical significance (p < 0.05) for key metrics
- Multiple testing correction (Bonferroni/FDR) for feature selection

### 12.4 Hypothesis Validation Checklist

- [ ] Feature MI significance (p < 0.05 with block bootstrap)
- [ ] Regime persistence (not random flickering)
- [ ] Regime-conditional returns differ (KS test)
- [ ] Temporal structure test completed
- [ ] AMD discriminability verified (AUC > 0.6)
- [ ] Walk-forward Sharpe > baseline Sharpe
- [ ] Learned patterns compared to Wyckoff theory

### 12.5 Production Validation

- Paper trading for minimum 2 weeks before live
- Real-time latency monitoring (<100ms decision)
- Capital preservation verification in NOISE regimes
- Drawdown limits enforced

---

## 13. Implementation Roadmap

### 13.1 Milestone Overview

```
TIMELINE (Effort-based, not calendar-based)

┌─────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  M1: Empirical Foundation                                               │
│  ├── Data inventory & quality                                           │
│  ├── MI analysis of all features                                        │
│  ├── Regime validation                                                  │
│  ├── Baseline comparisons                                               │
│  └── Temporal structure test                                            │
│      │                                                                   │
│      ▼ GATE: Do features have significant MI? Proceed only if yes.     │
│                                                                          │
│  M2: Core Infrastructure                                                │
│  ├── Labeler module                                                     │
│  ├── MI computation module                                              │
│  ├── Feature ranking module                                             │
│  └── Model training module                                              │
│      │                                                                   │
│      ▼ GATE: Does model beat baseline? Proceed only if yes.            │
│                                                                          │
│  M3: Trading System                                                     │
│  ├── OMS with regime awareness                                          │
│  ├── Walk-forward backtesting                                           │
│  ├── Risk management                                                    │
│  └── Decision logging                                                   │
│      │                                                                   │
│      ▼ GATE: Positive Sharpe in backtest? Proceed only if yes.         │
│                                                                          │
│  M4: AMD Detection                                                      │
│  ├── Proxy labeling                                                     │
│  ├── Feature discovery for phases                                       │
│  ├── Phase classifier                                                   │
│  ├── Temporal smoother (if validated)                                   │
│  └── Wyckoff comparison                                                 │
│      │                                                                   │
│      ▼ GATE: Does AMD improve performance? Proceed only if yes.        │
│                                                                          │
│  M5: Monitoring & Production                                            │
│  ├── Metrics export                                                     │
│  ├── Dashboard (progress, results, live)                                │
│  ├── Alerting                                                           │
│  └── Paper trading                                                      │
│      │                                                                   │
│      ▼ GATE: 2 weeks profitable paper trading? Deploy.                 │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 13.2 Milestone 1: Empirical Foundation

**Objective:** Validate that the core hypotheses hold before building infrastructure.

**Deliverables:**
1. Data quality report
2. Feature MI ranking table (all 90+ features, 4 horizons)
3. Regime distribution and persistence metrics
4. Baseline strategy comparison table
5. Temporal structure test result (H0 accept/reject)
6. AMD discriminability assessment

**Tasks:**

| Task | Description | Dependencies | Deliverable |
|------|-------------|--------------|-------------|
| M1.1 | Inventory existing Parquet data | None | Data summary doc |
| M1.2 | Implement data quality checker | M1.1 | Quality report |
| M1.3 | Implement KSG MI estimator | None | mi.rs module |
| M1.4 | Compute MI for all features × horizons | M1.2, M1.3 | MI table JSON |
| M1.5 | Implement block permutation test | M1.3 | p-values added |
| M1.6 | Compute regime metrics | M1.2 | Regime report |
| M1.7 | Implement baseline strategies | M1.2 | Baseline Sharpes |
| M1.8 | Implement temporal structure test | M1.3 | H0/H1 decision |
| M1.9 | Compute AMD discriminability | M1.4 | AUC scores |
| M1.10 | Write empirical findings report | All above | Section 4 filled |

**Gate Criteria:**
- [ ] At least 5 features with MI p-value < 0.05
- [ ] Regime persistence > 10 samples on average
- [ ] Temporal structure test completed
- [ ] AMD ACCUM vs DISTRIB has at least one feature with AUC > 0.55

### 13.3 Milestone 2: Core Infrastructure

**Objective:** Build the foundational modules with validated components.

**Deliverables:**
1. labeler.rs with tests
2. mi.rs with tests (reuse from M1)
3. feature_rank.rs with tests
4. model.rs with walk-forward validation

**Tasks:**

| Task | Description | Dependencies | Deliverable |
|------|-------------|--------------|-------------|
| M2.1 | Implement LabelConfig and LabelType | None | labeler.rs types |
| M2.2 | Implement volatility-adjusted labeler | M2.1 | compute_labels() |
| M2.3 | Implement no-lookahead verification test | M2.2 | Test passes |
| M2.4 | Refactor MI module from M1 | M1.3 | Production mi.rs |
| M2.5 | Implement FeatureRankTable | M2.4 | feature_rank.rs |
| M2.6 | Implement stability tracking | M2.5 | StabilityMetrics |
| M2.7 | Implement logistic regression | None | model.rs |
| M2.8 | Implement walk-forward validator | M2.7 | WalkForwardResult |
| M2.9 | Integration test: full pipeline | All above | E2E test passes |

**Gate Criteria:**
- [ ] Walk-forward Sharpe > entropy-gated baseline Sharpe
- [ ] Model accuracy > 52% (better than random)
- [ ] All tests passing

### 13.4 Milestone 3: Trading System

**Objective:** Build production-ready trading infrastructure.

**Deliverables:**
1. oms.rs with regime-aware logic
2. Backtest harness with transaction costs
3. Risk management module
4. Decision logging

**Tasks:**

| Task | Description | Dependencies | Deliverable |
|------|-------------|--------------|-------------|
| M3.1 | Implement OMSConfig and RegimeParams | None | oms.rs types |
| M3.2 | Implement regime-aware decide() | M3.1, M2 | TradingDecision |
| M3.3 | Implement position limits | M3.2 | Risk checks |
| M3.4 | Implement decision logging | M3.2 | JSON logs |
| M3.5 | Implement backtest harness | M3.2 | BacktestResult |
| M3.6 | Add transaction costs | M3.5 | Cost-aware PnL |
| M3.7 | Compute regime-conditional metrics | M3.5 | RegimePerformance |
| M3.8 | Full system backtest | All above | Sharpe, DD, etc. |

**Gate Criteria:**
- [ ] Backtest Sharpe > 0.5
- [ ] Max drawdown < 20%
- [ ] Zero exposure verified in NOISE periods

### 13.5 Milestone 4: AMD Detection

**Objective:** Add phase detection if it improves performance.

**Deliverables:**
1. Proxy phase labeling
2. Phase feature discovery (MI-based)
3. Phase classifier
4. Temporal smoother (conditional)
5. Wyckoff comparison report

**Tasks:**

| Task | Description | Dependencies | Deliverable |
|------|-------------|--------------|-------------|
| M4.1 | Implement proxy phase labeler | M2.2 | create_phase_labels() |
| M4.2 | Compute MI(features; phase) | M2.4, M4.1 | Phase MI table |
| M4.3 | Select top features for phases | M4.2 | Selected features |
| M4.4 | Implement Phase enum and structs | None | amd_detector.rs types |
| M4.5 | Train phase classifier | M4.3, M2.7 | PhaseDetector |
| M4.6 | Implement confidence smoothing (EMA) | M4.5 | Smoothed output |
| M4.7 | Implement learned HMM (if H1 validated) | M4.5, M1.8 | LearnedHMM |
| M4.8 | Implement Wyckoff comparison | M4.5 or M4.7 | Comparison report |
| M4.9 | Integrate with OMS | M4.5, M3.2 | AMD-aware OMS |
| M4.10 | Backtest with AMD | M4.9 | Comparison metrics |

**Gate Criteria:**
- [ ] Phase detection accuracy > 50%
- [ ] AMD-aware system Sharpe > non-AMD system Sharpe
- [ ] Wyckoff comparison report generated

### 13.6 Milestone 5: Monitoring & Production

**Objective:** Production-ready system with observability.

**Deliverables:**
1. Prometheus metrics export
2. WebSocket metrics server
3. Progress dashboard
4. Results dashboard
5. Live monitor dashboard
6. Alerting integration

**Tasks:**

| Task | Description | Dependencies | Deliverable |
|------|-------------|--------------|-------------|
| M5.1 | Define Prometheus metrics | None | Metrics spec |
| M5.2 | Implement metrics export in Rust | M5.1 | /metrics endpoint |
| M5.3 | Implement WebSocket server | M5.2 | /ws endpoint |
| M5.4 | Create Next.js dashboard project | None | Project scaffold |
| M5.5 | Implement progress page | M5.4 | Progress dashboard |
| M5.6 | Implement results page | M5.4, M1 | Results dashboard |
| M5.7 | Implement live monitor page | M5.4, M5.3 | Live dashboard |
| M5.8 | Implement alerting | M5.2 | Alerts to Slack |
| M5.9 | Paper trading setup | All above | Paper trading live |
| M5.10 | 2-week paper trading validation | M5.9 | Performance report |

**Gate Criteria:**
- [ ] All dashboards functional
- [ ] Alerts firing correctly
- [ ] 2 weeks paper trading with positive Sharpe

---

## 14. Future Extensions

### 14.1 Version 2 Candidates

| Feature | Description | Prerequisite |
|---------|-------------|--------------|
| MRMR Selection | Full redundancy-aware feature selection | Stable MI computation |
| PLS Reduction | Supervised dimensionality reduction | Feature ranking stable |
| GMM Clustering | Unsupervised regime discovery | Baseline regime working |
| Neural MI (MINE) | Higher-dimensional MI estimation | GPU infrastructure |
| Cross-Asset | Regime signals across correlated assets | Single-asset validation |
| Deep Learning | LSTM/Transformer for sequences | Large dataset accumulated |

### 14.2 Research Extensions

- Transfer entropy for causal feature analysis
- Information bottleneck for optimal compression
- Multi-timeframe regime hierarchies
- Cross-asset AMD phase correlation
- Reinforcement learning for position sizing
- Attention mechanisms for feature importance

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
| **Wyckoff** | Richard Wyckoff's method for analyzing market cycles |
| **HMM** | Hidden Markov Model - probabilistic model for sequences |
| **Phase** | Market cycle stage (Accumulation, Markup, Distribution, Markdown) |
| **OFI** | Order Flow Imbalance - (buy_vol - sell_vol) / total_vol |
| **Block Bootstrap** | Resampling method preserving autocorrelation |
| **Proxy Label** | Label created from future information for training |

---

## Appendix B: References

### Information Theory & MI Estimation

1. Kraskov, A., Stögbauer, H., & Grassberger, P. (2004). "Estimating mutual information." *Physical Review E*, 69(6), 066138.

2. Peng, H., Long, F., & Ding, C. (2005). "Feature selection based on mutual information: criteria of max-dependency, max-relevance, and min-redundancy." *IEEE Transactions on Pattern Analysis and Machine Intelligence*, 27(8), 1226-1238.

3. Cover, T. M., & Thomas, J. A. (2006). *Elements of Information Theory* (2nd ed.). Wiley.

4. **Gao, S., Ver Steeg, G., & Galstyan, A. (2015). "Efficient estimation of mutual information for strongly dependent variables." *Artificial Intelligence and Statistics*, 277-286.** [Critical for autocorrelated data]

5. **Reshef, D. N., et al. (2011). "Detecting novel associations in large data sets." *Science*, 334(6062), 1518-1524.** [MIC as alternative to MI]

6. **Kinney, J. B., & Atwal, G. S. (2014). "Equitability, mutual information, and the maximal information coefficient." *Proceedings of the National Academy of Sciences*, 111(9), 3354-3359.**

7. Moon, Y. I., Rajagopalan, B., & Lall, U. (1995). "Estimation of mutual information using kernel density estimators." *Physical Review E*, 52(3), 2318.

### Market Microstructure

8. **Cont, R. (2001). "Empirical properties of asset returns: stylized facts and statistical issues." *Quantitative Finance*, 1(2), 223-236.** [Baseline properties]

9. Cont, R., Kukanov, A., & Stoikov, S. (2014). "The price impact of order book events." *Journal of Financial Econometrics*, 12(1), 47-88.

10. Avellaneda, M., & Stoikov, S. (2008). "High-frequency trading in a limit order book." *Quantitative Finance*, 8(3), 217-224.

11. **Lillo, F., & Farmer, J. D. (2004). "The long memory of the efficient market." *Studies in Nonlinear Dynamics & Econometrics*, 8(3).** [Order flow autocorrelation]

12. **Bouchaud, J. P., Gefen, Y., Potters, M., & Wyart, M. (2004). "Fluctuations and response in financial markets: the subtle nature of 'random' price changes." *Quantitative Finance*, 4(2), 176-190.** [Price impact]

13. **Toth, B., et al. (2015). "Anomalous price impact and the critical nature of liquidity in financial markets." *Physical Review X*, 5(4), 041025.**

### Regime Detection & Hidden Markov Models

14. Hamilton, J. D. (1989). "A new approach to the economic analysis of nonstationary time series and the business cycle." *Econometrica*, 57(2), 357-384.

15. Rabiner, L. R. (1989). "A tutorial on hidden Markov models and selected applications in speech recognition." *Proceedings of the IEEE*, 77(2), 257-286.

16. Ang, A., & Timmermann, A. (2012). "Regime changes and financial markets." *Annual Review of Financial Economics*, 4(1), 313-337.

17. **Guidolin, M., & Timmermann, A. (2007). "Asset allocation under multivariate regime switching." *Journal of Economic Dynamics and Control*, 31(11), 3503-3544.** [How to trade on regimes]

18. **Bulla, J., & Bulla, I. (2006). "Stylized facts of financial time series and hidden semi-Markov models." *Computational Statistics & Data Analysis*, 51(4), 2192-2209.** [HSMM alternative]

19. **Nystrup, P., Madsen, H., & Lindström, E. (2017). "Dynamic portfolio optimization across hidden market regimes." *Quantitative Finance*, 17(12), 1781-1797.**

### Wyckoff Method & Market Phases

20. Wyckoff, R. D. (1931). *The Richard D. Wyckoff Method of Trading in Stocks*. (Original publication)

21. Pruden, H. M. (2007). *The Three Skills of Top Trading: Behavioral Systems Building, Pattern Recognition, and Mental State Management*. Wiley.

22. Schroeder, J. (2015). "Detecting institutional order flow." *Journal of Trading*, 10(4), 27-39.

### Information Theory in Finance

23. **Dionisio, A., Menezes, R., & Mendes, D. A. (2004). "Mutual information: a measure of dependency for nonlinear time series." *Physica A*, 344(1-2), 326-329.**

24. **Diks, C., & Panchenko, V. (2006). "A new statistic and practical guidelines for nonparametric Granger causality testing." *Journal of Economic Dynamics and Control*, 30(9-10), 1647-1669.**

25. **Schreiber, T. (2000). "Measuring information transfer." *Physical Review Letters*, 85(2), 461.** [Transfer entropy]

26. **Fiedor, P. (2014). "Information-theoretic approach to lead-lag effect on financial markets." *The European Physical Journal B*, 87(8), 168.**

### Walk-Forward Validation & Backtesting

27. **de Prado, M. L. (2018). *Advances in Financial Machine Learning*. Wiley.** [Walk-forward, purging, embargo]

28. Bailey, D. H., & de Prado, M. L. (2014). "The deflated Sharpe ratio: correcting for selection bias, backtest overfitting, and non-normality." *Journal of Portfolio Management*, 40(5), 94-107.

### Deep Learning for Sequences

29. Hochreiter, S., & Schmidhuber, J. (1997). "Long short-term memory." *Neural Computation*, 9(8), 1735-1780.

30. **Vaswani, A., et al. (2017). "Attention is all you need." *Advances in Neural Information Processing Systems*, 30.** [Transformers]

---

## Appendix C: Empirical Results Template

**This appendix will be populated with actual results as Milestone 1 progresses.**

### C.1 Feature MI Rankings

| Rank | Feature | Horizon 10 MI | Horizon 30 MI | Horizon 100 MI | Horizon 300 MI | p-value | Stable |
|------|---------|---------------|---------------|----------------|----------------|---------|--------|
| 1 | ___ | ___ | ___ | ___ | ___ | ___ | ___ |
| 2 | ___ | ___ | ___ | ___ | ___ | ___ | ___ |
| ... | ... | ... | ... | ... | ... | ... | ... |

### C.2 Regime Distribution

| Regime | % Time | Mean Duration | Mean Return | Std Return |
|--------|--------|---------------|-------------|------------|
| TREND | ___% | ___ samples | ___% | ___% |
| MEAN_REVERT | ___% | ___ samples | ___% | ___% |
| NOISE | ___% | ___ samples | ___% | ___% |

### C.3 Temporal Structure Test

| Metric | Value |
|--------|-------|
| MI without history | ___ bits |
| MI with history | ___ bits |
| MI gain | ___ bits |
| p-value | ___ |
| Decision | H0 / H1 |
| Recommended model | Memoryless / EMA / HMM |

### C.4 AMD Discriminability

| Feature | ACCUM vs DISTRIB AUC | ACCUM vs MARKUP AUC | All-phase Accuracy |
|---------|---------------------|---------------------|-------------------|
| order_flow_imbalance | ___ | ___ | ___% |
| aggr_ratio_50 | ___ | ___ | ___% |
| ... | ... | ... | ... |

### C.5 Baseline Comparison

| Strategy | Sharpe | Max DD | Win Rate | Profit Factor | Annual Return |
|----------|--------|--------|----------|---------------|---------------|
| Buy & Hold | ___ | ___% | - | - | ___% |
| Random (mean) | ___ | ___% | ___% | ___ | ___% |
| Entropy-gated | ___ | ___% | ___% | ___ | ___% |
| MI top-5 | ___ | ___% | ___% | ___ | ___% |
| Full system | ___ | ___% | ___% | ___ | ___% |
| With AMD | ___ | ___% | ___% | ___ | ___% |

---

**Document Control:**
- Created: 2026-01-17
- Version: 1.0
- Author: System Architecture
- Status: Draft - Pending Empirical Validation
- Predecessor: EXTENDED_REQUIREMENTS_0.md v0.2
- Next Review: After Milestone 1 completion

**Change Log:**
| Version | Date | Changes |
|---------|------|---------|
| 0.2 | 2026-01-15 | Initial AMD subsystem (EXTENDED_REQUIREMENTS_0.md) |
| 1.0 | 2026-01-17 | Major revision: empirical foundation, temporal hypothesis framework, expanded references, implementation roadmap |

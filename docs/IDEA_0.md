# IDEA_0: Systematic Trend-Following Algorithm Development Process

**Version:** 0.1
**Created:** 2026-01-20
**Status:** Conceptual Framework

---

## Executive Summary

A systematic process for developing consistently profitable trend-following algorithms by integrating five foundational papers:

1. **Lo (1991)** - Trendability detection via Hurst exponent
2. **Hamilton (1989)** - Regime estimation via Hidden Markov Models
3. **Schreiber (2000)** - Causal discovery via Transfer Entropy
4. **Granger (1969)** - Causal validation via Granger Causality
5. **Cont et al. (2014)** - Signal confirmation via Order Flow Imbalance

The framework accepts that errors will occur and builds in continuous adaptation mechanisms.

---

## Core Philosophy

**The goal is not to find a permanently profitable algorithm, but to:**

1. Systematically discover when profit is available
2. Exploit it while it lasts
3. Detect when edge disappears
4. Re-adapt to new conditions

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    TREND FOLLOWING RESEARCH PIPELINE                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   PHASE 1    │───▶│   PHASE 2    │───▶│   PHASE 3    │              │
│  │  Trendability│    │    Regime    │    │   Causal     │              │
│  │  Detection   │    │  Estimation  │    │  Discovery   │              │
│  │   (Lo 1991)  │    │(Hamilton '89)│    │(Granger/TE)  │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│         │                   │                   │                       │
│         ▼                   ▼                   ▼                       │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │ H > 0.5?     │    │ P(trending)  │    │ Features that│              │
│  │ VR > 1?      │    │ P(reverting) │    │ Granger-cause│              │
│  │ → Trendable  │    │ P(random)    │    │ returns      │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│                              │                   │                      │
│                              ▼                   ▼                      │
│                       ┌─────────────────────────────────┐              │
│                       │          PHASE 4                │              │
│                       │    Signal Construction          │              │
│                       │  (Regime-Conditional Weights)   │              │
│                       └─────────────────────────────────┘              │
│                                      │                                  │
│                                      ▼                                  │
│                       ┌─────────────────────────────────┐              │
│                       │          PHASE 5                │              │
│                       │   Walk-Forward Validation       │              │
│                       │    (Cont OFI Confirmation)      │              │
│                       └─────────────────────────────────┘              │
│                                      │                                  │
│                              ┌───────┴───────┐                         │
│                              ▼               ▼                          │
│                       ┌──────────┐    ┌──────────┐                     │
│                       │  ACCEPT  │    │  REJECT  │                     │
│                       │  Deploy  │    │  Iterate │                     │
│                       └──────────┘    └──────────┘                     │
│                              │                                          │
│                              ▼                                          │
│                       ┌─────────────────────────────────┐              │
│                       │          PHASE 6                │              │
│                       │   Continuous Monitoring         │              │
│                       │    & Re-optimization            │              │
│                       └─────────────────────────────────┘              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Trendability Detection (Lo 1991)

### Objective

Determine if the asset/timeframe exhibits exploitable trending behavior before attempting to build a trend-following strategy.

### Key Metrics

**Hurst Exponent (H):**
```
H = 0.5  →  Random walk (no predictability)
H > 0.5  →  Persistent/trending (today's direction predicts tomorrow's)
H < 0.5  →  Anti-persistent/mean-reverting
```

**Variance Ratio (VR):**
```
VR(k) = Var(k-period returns) / (k × Var(1-period returns))

VR > 1  →  Positive autocorrelation (trending)
VR = 1  →  Random walk
VR < 1  →  Negative autocorrelation (mean-reverting)
```

### R/S Analysis for Hurst Estimation

For a series of returns r₁, r₂, ..., rₙ:

```
R/S = (1/σ) × [max(W₁,...,Wₙ) - min(W₁,...,Wₙ)]

where Wₖ = Σᵢ₌₁ᵏ (rᵢ - r̄)  (cumulative deviation from mean)
      σ = standard deviation of returns
```

The scaling relationship:
```
E[R/S] ~ c × n^H  as n → ∞
```

Estimate H as slope of log(R/S) vs log(n).

### Decision Rule

| Condition | Regime | Action |
|-----------|--------|--------|
| H > 0.5 + 1.96×SE and VR > 1 | Trending | Proceed to Phase 2 |
| H < 0.5 - 1.96×SE and VR < 1 | Mean-Reverting | Use reversal strategy |
| H ≈ 0.5 and VR ≈ 1 | Random Walk | Change timeframe/asset |

### Implementation

```python
def assess_trendability(returns: np.ndarray, timestamp: int) -> TrendabilityResult:
    H, H_stderr = compute_hurst_rs(returns)
    VR, VR_z = compute_variance_ratio(returns, k=5)

    # Classification with statistical significance
    H_trending = H > 0.5 + 1.96 * H_stderr
    H_reverting = H < 0.5 - 1.96 * H_stderr
    VR_trending = VR_z > 1.96
    VR_reverting = VR_z < -1.96

    # Consensus regime
    if H_trending and VR_trending:
        regime = 'trending'
        tradeable = True
    elif H_reverting and VR_reverting:
        regime = 'mean_reverting'
        tradeable = True
    else:
        regime = 'random_walk'
        tradeable = False

    return TrendabilityResult(regime=regime, tradeable=tradeable, H=H, VR=VR)
```

---

## Phase 2: Regime Estimation (Hamilton 1989)

### Objective

Given trendability exists, estimate the current regime probability to condition strategy behavior.

### Hidden Markov Model Structure

**States:**
- State 0: Trending (H > 0.5, positive autocorrelation)
- State 1: Mean-reverting (H < 0.5, negative autocorrelation)
- State 2: Random walk (H ≈ 0.5, no autocorrelation)

**Observations:** (return_autocorrelation, entropy, volatility_ratio, OFI_persistence)

### Transition Matrix

Regimes tend to persist:
```
A = [
    [0.95, 0.025, 0.025],  # Trending persists
    [0.025, 0.95, 0.025],  # Reverting persists
    [0.05, 0.05, 0.90],    # Random is less stable
]
```

### Key Algorithms

1. **Forward Algorithm:** Compute P(state_t | observations_1:t)
2. **Viterbi Algorithm:** Find most likely state sequence
3. **Baum-Welch:** Estimate parameters from data

### Regime State Output

```python
@dataclass
class RegimeState:
    timestamp: int
    prob_trending: float
    prob_reverting: float
    prob_random: float
    current_regime: int
    regime_duration: int      # How long in current regime
    transition_prob: float    # P(regime change next period)
```

### Feature Engineering for HMM

Transform existing features into HMM observations:
- Return autocorrelation (lag-1)
- Tick entropy (existing feature)
- Volatility ratio (short/long window)
- OFI persistence

---

## Phase 3: Causal Discovery (Granger/Schreiber)

### Objective

Identify which features genuinely predict returns (not just correlate).

### Granger Causality Test

**Hypothesis:**
- H₀: Feature does NOT Granger-cause returns
- H₁: Feature DOES Granger-cause returns

**Models:**
```
Restricted:   returns_t = a + Σ bᵢ × returns_{t-i}
Unrestricted: returns_t = a + Σ bᵢ × returns_{t-i} + Σ cᵢ × feature_{t-i}
```

**F-statistic:**
```
F = [(RSS_r - RSS_u) / df1] / [RSS_u / df2]

where df1 = max_lag (additional parameters)
      df2 = n - 2×max_lag - 1 (residual degrees of freedom)
```

Reject H₀ if p-value < 0.05 → Feature Granger-causes returns.

### Transfer Entropy (Non-Linear Causality)

```
TE(X→Y) = I(Y_future; X_past | Y_past)
```

Measures how much knowing X's past reduces uncertainty about Y's future, beyond what Y's own past provides.

**KSG Estimation:**
```
TE ≈ ψ(k) + <ψ(n_z)> - <ψ(n_xz)> - <ψ(n_yz)>
```

### Bidirectional Analysis

Compute TE in both directions:
- TE(feature → returns) > TE(returns → feature) → Feature leads (useful)
- TE(feature → returns) < TE(returns → feature) → Returns lead (not useful)

### Combined Causal Score

```python
if granger.is_causal:
    combined = 0.6 × granger_score + 0.4 × te_score
else:
    combined = 0.3 × granger_score + 0.7 × te_score

if te['feature_leads']:
    combined *= 1.2  # Boost for genuine leading relationship
```

### Feature Selection Output

```python
@dataclass
class CausalFeatureRank:
    feature_name: str
    granger_pvalue: float
    granger_direction: str   # 'positive', 'negative', 'mixed'
    transfer_entropy: float
    net_info_flow: float
    combined_score: float
    selected: bool           # combined_score >= threshold
```

---

## Phase 4: Signal Construction

### Objective

Combine regime state with causal features into actionable trading signals.

### Regime-Conditional Weights

**Key Insight:** Feature weights should differ by regime.

```python
regime_weights = {
    'trending': {
        'ofi': +0.8,           # Follow order flow
        'entropy': -0.3,       # Low entropy = trend continues
        'volatility': +0.2,    # Higher vol = stronger moves
    },
    'mean_reverting': {
        'ofi': -0.4,           # Fade order flow
        'entropy': +0.3,       # High entropy = reversal coming
        'volatility': -0.2,    # Lower vol preferred
    },
    'random_walk': {
        'ofi': +0.1,           # Minimal weight
        'entropy': +0.1,
        'volatility': +0.1,
    }
}
```

### Signal Generation

```python
def compute_signal(regime_state, feature_values, trendability) -> TrendSignal:
    # Select active regime
    if regime_state.prob_trending > 0.6:
        active_regime = 'trending'
    elif regime_state.prob_reverting > 0.6:
        active_regime = 'mean_reverting'
    else:
        active_regime = 'random_walk'

    # Weighted sum of features
    weights = regime_weights[active_regime]
    raw_signal = sum(weights[f] * feature_values[f] for f in weights)

    # Normalize to [-1, 1]
    direction_signal = tanh(raw_signal)

    # Direction
    if direction_signal > 0.2:
        direction = +1  # Long
    elif direction_signal < -0.2:
        direction = -1  # Short
    else:
        direction = 0   # Flat

    # Confidence
    confidence = 0.4 × regime_prob + 0.3 × trendability.confidence + 0.3 × |direction_signal|

    # Position sizing with regime adjustments
    position_size = confidence
    if active_regime == 'random_walk':
        position_size *= 0.2
    if regime_state.transition_prob > 0.3:
        position_size *= 0.5  # Reduce near regime changes
    if regime_state.regime_duration < 5:
        position_size *= 0.7  # New regime, less confident

    return TrendSignal(direction, confidence, position_size, active_regime)
```

### OFI Confirmation (Cont et al. 2014)

Use Order Flow Imbalance for real-time microstructure confirmation:

```python
def confirm_with_ofi(signal, ofi_current, ofi_momentum) -> TrendSignal:
    # Check alignment
    signal_direction = signal.direction
    ofi_direction = +1 if ofi > threshold else (-1 if ofi < -threshold else 0)
    ofi_mom_direction = +1 if ofi_momentum > 0 else -1

    direction_aligned = (signal_direction == ofi_direction)
    momentum_aligned = (signal_direction == ofi_mom_direction)

    confirmation_score = 0.5 × direction_aligned + 0.5 × momentum_aligned

    if confirmation_score >= 0.5:
        # Confirmed: boost confidence
        signal.confidence *= 1.2
        signal.position_size *= 1.1
    elif confirmation_score == 0:
        # Contradicted: reduce or reject
        signal.confidence *= 0.5
        signal.position_size *= 0.3

    return signal
```

---

## Phase 5: Walk-Forward Validation

### Objective

Validate the complete pipeline on out-of-sample data with realistic constraints.

### Critical Rules

1. **NEVER look ahead** - All parameters estimated on train only
2. **Include transaction costs** - Realistic cost model
3. **Track regime at time of signal** - Not retrospectively
4. **Multiple folds** - Assess stability across time

### Fold Structure (Anchored Expanding Window)

```
Fold 1: Train [0, 500]      Test [500, 600]
Fold 2: Train [0, 600]      Test [600, 700]
Fold 3: Train [0, 700]      Test [700, 800]
Fold 4: Train [0, 800]      Test [800, 900]
Fold 5: Train [0, 900]      Test [900, 1000]
```

### Validation Metrics

| Metric | Minimum Threshold |
|--------|-------------------|
| Aggregate Sharpe | > 0.3 |
| Win Rate | > 45% |
| Sharpe Stability (std across folds) | < 1.5 |
| Weight Stability | > 0.5 |

### Validation Decision

```python
is_valid = (
    aggregate_sharpe >= min_sharpe and
    avg_win_rate >= min_win_rate and
    sharpe_stability <= max_sharpe_std
)

is_deployable = (
    is_valid and
    len(causal_features) >= 2 and
    trendability.hurst > 0.45
)
```

---

## Phase 6: Continuous Monitoring & Adaptation

### Objective

Detect performance degradation and trigger re-optimization.

### Alert Types

| Alert | Detection | Severity | Action |
|-------|-----------|----------|--------|
| Sharpe Decay | Rolling Sharpe < 50% of baseline | Critical | Re-optimize pipeline |
| Drawdown | Current DD > 15% | Critical | Reduce positions |
| Regime Instability | >30% regime changes in lookback | Warning | Re-fit HMM |
| Feature Degradation | Signal-return correlation < 0.3 | Warning | Re-run causal discovery |

### Re-optimization Triggers

```python
def should_reoptimize(alerts, observations_since_last) -> bool:
    critical_alerts = [a for a in alerts if a.severity == 'critical']
    return (
        len(critical_alerts) > 0 or
        len(alerts) >= 2 or
        observations_since_last >= reoptimize_interval
    )
```

### Adaptation Loop

```
1. Deploy validated model
2. Monitor performance in real-time
3. Detect alerts
4. If re-optimization triggered:
   a. Gather recent data (rolling window)
   b. Re-run full pipeline
   c. Compare new model to current
   d. Deploy if improvement, keep current otherwise
5. Repeat
```

---

## Expected Failure Modes

| Error | Detection | Response |
|-------|-----------|----------|
| **No trendability** | H ≈ 0.5, VR ≈ 1 | Change timeframe or asset |
| **No causal features** | All Granger p > 0.05 | Add more features, check data quality |
| **Regime instability** | HMM switches rapidly | Widen confidence bands, reduce position |
| **Validation failure** | Sharpe < threshold | Adjust signal thresholds, add features |
| **Live performance decay** | Rolling Sharpe drops | Trigger re-optimization |
| **Feature degradation** | Signal-return correlation drops | Re-run causal discovery |
| **Overfitting** | Train >> Test performance | More regularization, fewer features |

---

## Configuration Parameters

```python
@dataclass
class PipelineConfig:
    # Phase 1: Trendability
    hurst_window: int = 500
    variance_ratio_k: int = 5

    # Phase 2: Regime
    hmm_states: int = 3

    # Phase 3: Causality
    granger_max_lag: int = 5
    te_lag: int = 1
    min_causal_score: float = 0.4

    # Phase 4: Signal
    ofi_threshold: float = 0.3
    entry_threshold: float = 0.2

    # Phase 5: Validation
    min_train_periods: int = 500
    test_periods: int = 100
    n_folds: int = 5
    transaction_cost_bps: float = 2.0
    min_sharpe: float = 0.3
    min_win_rate: float = 0.45

    # Phase 6: Monitoring
    reoptimize_interval: int = 500
    sharpe_decay_threshold: float = 0.5
    max_drawdown_threshold: float = 0.15
```

---

## Integration with Existing Infrastructure

### Existing Features (from Ingestor)

| Feature | Phase Usage |
|---------|-------------|
| tick_entropy | HMM observation, signal weight |
| ofi (order flow imbalance) | Signal confirmation |
| bid_ask_spread | Transaction cost adjustment |
| volatility | Signal weight, position sizing |
| mid_price | Return computation |
| depth_imbalance | Causal discovery candidate |

### New Features Required

| Feature | Purpose | Implementation |
|---------|---------|----------------|
| return_autocorr | HMM observation | Rolling lag-1 autocorrelation |
| hurst_exponent | Trendability | Rolling R/S analysis |
| variance_ratio | Trendability | Rolling VR(5) |
| ofi_momentum | Signal confirmation | Δ(OFI) over window |

### Rust Integration Points

```rust
// New feature computations in src/features/
pub struct TrendabilityFeatures {
    pub hurst: f64,
    pub variance_ratio: f64,
    pub return_autocorr: f64,
}

// Regime state from HMM (Python → Rust via FFI or file)
pub struct RegimeState {
    pub prob_trending: f64,
    pub prob_reverting: f64,
    pub prob_random: f64,
    pub current_regime: u8,
}

// Signal generation
pub fn generate_trend_signal(
    regime: &RegimeState,
    features: &FeaturesSnapshot,
    config: &SignalConfig,
) -> TrendSignal;
```

---

## Implementation Roadmap

### Phase A: Foundation (Week 1-2)

1. Implement Hurst exponent computation in Rust
2. Implement variance ratio test in Rust
3. Add return_autocorr to feature pipeline
4. Create TrendabilityResult output

### Phase B: Regime Model (Week 3-4)

1. Implement HMM in Python (or use hmmlearn)
2. Create feature preparation for HMM observations
3. Implement forward/Viterbi algorithms
4. Bridge Python HMM to Rust via file or FFI

### Phase C: Causal Discovery (Week 5-6)

1. Implement Granger causality test in Python
2. Implement transfer entropy with KSG estimator
3. Create feature ranking system
4. Integrate with existing feature set

### Phase D: Signal & Validation (Week 7-8)

1. Implement regime-conditional signal generator
2. Add OFI confirmation logic
3. Extend walk-forward validation for new pipeline
4. Run validation on historical data

### Phase E: Monitoring & Deployment (Week 9-10)

1. Implement continuous monitoring
2. Create alert system
3. Build re-optimization triggers
4. Deploy with paper trading

---

## Success Criteria

| Metric | Target |
|--------|--------|
| Out-of-sample Sharpe | > 0.5 |
| Win rate | > 50% |
| Max drawdown | < 15% |
| Sharpe stability (fold std) | < 1.0 |
| Feature weight stability | > 0.6 |
| Re-optimization frequency | < 1 per month |

---

## References

1. Lo, A. W. (1991). Long-term memory in stock market prices. *Econometrica*, 59(5), 1279-1313.
2. Hamilton, J. D. (1989). A new approach to economic analysis of nonstationary time series. *Econometrica*, 57(2), 357-384.
3. Schreiber, T. (2000). Measuring information transfer. *Physical Review Letters*, 85(2), 461.
4. Granger, C. W. (1969). Investigating causal relations by econometric models. *Econometrica*, 37(3), 424-438.
5. Cont, R., Kukanov, A., & Stoikov, S. (2014). The price impact of order book events. *Journal of Financial Economics*, 21(1), 21-49.

---

*This document defines a systematic process for trend-following algorithm development. The framework is designed to be iterative, accepting that failures will occur and building in mechanisms for continuous adaptation.*

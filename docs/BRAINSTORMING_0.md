# MARS Brainstorming Session 0

**Date:** 2026-01-11
**Participants:** Onat + Claude (AI)
**Status:** Design Phase

---

## 1. Project Vision: MARS

**MARS = Metaheuristic Algorithm Research System**

A system that:
1. Ingests market data (Binance, Hyperliquid)
2. Extracts features
3. Defines trading algorithms as **genotypes** (parameter configurations)
4. Evolves algorithms via **metaheuristic optimization** (PSO, GA)
5. Validates via paper trading
6. Optionally stores results on-chain (Walrus/Sui)

---

## 2. Key Architectural Decisions

### 2.1 Genotype Must Be Tautologically Complete

The genotype should represent ALL degrees of freedom in the system. Every possible valid strategy configuration must be expressible as a point in the genotype space.

**Problem:** Unconstrained genotype space is astronomically large (~10^82 combinations), causing PSO to diverge.

**Solution:** Use **Algorithm Templates** that constrain the search space while maintaining expressiveness.

### 2.2 Template-Based Architecture

Instead of searching all possible strategies, define templates with:
- **Fixed structure** (computation graph)
- **Variable parameters** (evolvable values)

This reduces search space from ~10^82 to ~10^10-10^15 (tractable).

### 2.3 No Supervised Learning for V1

**Key insight from discussion:**

```
Labels not necessary → Labeled data not necessary → Supervised ML not necessary
```

| Component | Needed? | Rationale |
|-----------|---------|-----------|
| Triple-barrier labeling | No | Rules + PSO optimize directly on Sharpe |
| SVM, XGBoost, Neural Nets | No | No labels to train on |
| PSO | Yes | Optimizes rule parameters |
| Rule-based modules | Yes | Human-designed structure, PSO-tuned params |

**Why skip supervised ML:**
- Limited data (47 days, 73k events)
- Structure is known (regime → strategy mapping is explicit)
- PSO optimizes end-to-end on true objective (Sharpe)
- Simpler, fewer overfitting risks

---

## 3. The Minimal Modular Architecture

### 3.1 Five Core Modules

```
┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
│ OBSERVE │──►│ CLASSIFY│──►│ PREDICT │──►│  SIZE   │──►│ EXECUTE │
│         │   │         │   │         │   │         │   │         │
│Features │   │ Regime  │   │ Signal  │   │Position │   │ Orders  │
└─────────┘   └─────────┘   └─────────┘   └─────────┘   └─────────┘
    M1            M2            M3            M4            M5
```

### 3.2 Module Specifications

#### M1: Observer (Feature Extraction)
- **Purpose:** Transform raw market data into normalized features
- **Input:** Raw prices, volumes, order book, trades
- **Output:** Feature vector F ∈ ℝⁿ
- **Evolvable params:** 0-4 (mostly fixed)

Core features:
- mid_price, returns[short/medium/long]
- volatility[short/long]
- tick_entropy (regime indicator)
- order_book_imbalance
- spread_bps, volume_ratio

#### M2: Classifier (Regime Detection)
- **Purpose:** Determine current market regime
- **Input:** Feature vector F
- **Output:** Regime ∈ {TREND, REVERT, AVOID} + confidence
- **Implementation:** Rule-based thresholds (not ML)
- **Evolvable params:** 4

```
IF entropy < θ_trend AND autocorr > θ_autocorr:
    regime = TREND
ELIF entropy > θ_avoid:
    regime = AVOID
ELSE:
    regime = REVERT
```

Parameters:
- entropy_trend_max: [0.3, 0.6]
- entropy_avoid_min: [0.7, 0.95]
- volatility_avoid_mult: [1.5, 4.0]
- min_confidence: [0.5, 0.9]

#### M3: Predictor (Signal Generation)
- **Purpose:** Generate trading signal given regime
- **Input:** Feature vector F, Regime R
- **Output:** Signal ∈ [-1, +1]
- **Key insight:** Different predictor per regime
- **Evolvable params:** 9 (5 momentum + 4 reversion)

**Momentum Predictor (for TREND regime):**
```
momentum = w_short × returns[short] + w_medium × returns[medium] + w_long × returns[long]
signal = sign(momentum) × min(1, |momentum| / scale)
```

Parameters:
- weight_short, weight_medium, weight_long: [0, 1]
- entry_threshold: [0.001, 0.02]
- signal_scale: [0.01, 0.1]

**Mean Reversion Predictor (for REVERT regime):**
```
zscore = (price - moving_avg) / moving_std
signal = -sign(zscore) × min(1, |zscore| / scale) if |zscore| > threshold
```

Parameters:
- lookback_window: [300, 7200]
- entry_zscore: [1.0, 3.0]
- exit_zscore: [0, 0.5]
- signal_scale: [1, 4]

#### M4: Sizer (Position Management)
- **Purpose:** Convert signal to position size with risk constraints
- **Input:** Signal, Current position, Portfolio state
- **Output:** Target position
- **Evolvable params:** 6

```
base_size = signal × base_position × capital
vol_adjusted = base_size × (target_vol / current_vol)  // optional
final_size = clamp(vol_adjusted, -max_position, +max_position)
```

Parameters:
- base_position_pct: [0.05, 0.5]
- max_position_pct: [0.1, 1.0]
- use_vol_scaling: {true, false}
- target_volatility: [0.1, 0.5]
- drawdown_threshold: [0.05, 0.15]
- max_drawdown: [0.10, 0.30]

#### M5: Executor (Order Management)
- **Purpose:** Convert target position to orders, manage exits
- **Input:** Target position, Current position, Market state
- **Output:** Orders
- **Evolvable params:** 4

Parameters:
- stop_loss_pct: [0.01, 0.10]
- take_profit_pct: [0.02, 0.20]
- max_holding_periods: [10, 500]
- slippage_bps: [1, 20]

### 3.3 Complete Genotype: 23 Parameters

```rust
struct MinimalGenotype {
    // M2: Classifier (4 params)
    entropy_trend_max: f64,
    entropy_avoid_min: f64,
    volatility_avoid_mult: f64,
    min_confidence: f64,

    // M3a: Momentum Predictor (5 params)
    mom_weight_short: f64,
    mom_weight_medium: f64,
    mom_weight_long: f64,
    mom_entry_threshold: f64,
    mom_signal_scale: f64,

    // M3b: Mean Reversion Predictor (4 params)
    rev_lookback_window: f64,
    rev_entry_zscore: f64,
    rev_exit_zscore: f64,
    rev_signal_scale: f64,

    // M4: Sizer (6 params)
    base_position_pct: f64,
    max_position_pct: f64,
    use_vol_scaling: bool,
    target_volatility: f64,
    drawdown_threshold: f64,
    max_drawdown: f64,

    // M5: Executor (4 params)
    stop_loss_pct: f64,
    take_profit_pct: f64,
    max_holding_periods: u32,
    slippage_bps: f64,
}
```

**Search space:** ~10^23 (tractable with PSO)

---

## 4. The Regime Classifier as Gating Module

### 4.1 Core Insight

The regime classifier is the **key innovation**:
- Most traders use one strategy for all conditions
- MARS adapts strategy to regime
- This conditional selection is the edge

```
MARKET STATE ──► REGIME CLASSIFIER ──► ACTION GATE
                        │
                        ▼
                 ┌──────────────┐
                 │   OUTPUT:    │
                 │  TRENDING    │──► Enable Momentum Strategy
                 │  REVERTING   │──► Enable Mean Reversion Strategy
                 │  CHAOTIC     │──► No Trade (sit out)
                 └──────────────┘
```

### 4.2 Regime Definitions

**TRENDING:**
- Low entropy (orderly price movement)
- Positive autocorrelation (momentum persists)
- Hurst exponent > 0.5
- Volume increasing with trend

**REVERTING:**
- Medium entropy (bounded movement)
- Negative autocorrelation
- Hurst exponent < 0.5
- Price oscillating around level

**CHAOTIC:**
- High entropy (random/unpredictable)
- Zero autocorrelation
- High volatility, no direction
- Often during news, liquidations

---

## 5. Optimization Strategy

### 5.1 PSO (Particle Swarm Optimization)

- Population: ~100 particles
- Iterations: ~500-1000
- Each particle = one genotype configuration
- Fitness = Sharpe ratio from backtest

### 5.2 Fitness Function

```
fitness = sharpe_ratio
        - α × max_drawdown              // Penalize large DD
        - β × (IS_sharpe - OOS_sharpe)  // Penalize overfitting
        - γ × (1 / num_trades)          // Penalize too few trades

where:
  α ≈ 2.0 (drawdown penalty)
  β ≈ 1.0 (overfitting penalty)
  γ ≈ 0.01 (activity penalty)
```

### 5.3 Validation Protocol

1. Split data: 60% train, 20% validate, 20% test
2. PSO optimizes on train
3. Early stopping based on validate
4. Final evaluation on test (never seen)
5. Reject if test_sharpe < 0.5 × train_sharpe

---

## 6. Why This Design Can Work

1. **Regime gating prevents trading in noise**
   - AVOID regime = sit out chaotic markets
   - Reduces adverse selection

2. **Strategy-regime matching**
   - Momentum in trending: exploit persistence
   - Reversion in ranging: exploit mean-seeking

3. **Adaptive position sizing**
   - Volatility scaling: consistent risk
   - Drawdown reduction: survive losing streaks

4. **Exit discipline**
   - Stop loss: cut losers
   - Take profit: lock gains
   - Time stop: no stale positions

5. **Evolution finds robust parameters**
   - Walk-forward prevents overfitting
   - Multi-objective prevents gaming

---

## 7. Hyperliquid Integration Value

### 7.1 Unique Data from Hyperliquid

| Data | Binance | Hyperliquid | Winner |
|------|---------|-------------|--------|
| Liquidation data | 1/sec limit | Full on-chain | Hyperliquid |
| Funding rates | Basic | Predicted + cross-venue | Hyperliquid |
| HLP vault positions | N/A | Real-time visible | Hyperliquid |
| Open interest caps | No | Yes | Hyperliquid |

### 7.2 Integration Effort

~5-8 days for core data streams

### 7.3 Value for Strategies

- **Funding arbitrage:** Predicted funding, cross-venue comparison
- **Market making:** HLP position transparency
- **Risk management:** Full liquidation data

---

## 8. Walrus/Sui Integration

### 8.1 What to Store On-Chain

**Store on Walrus (high-value, immutable):**
- Genotype definitions
- Optimized strategy configs
- Evolution lineage
- Strategy NFT metadata

**Store off-chain (performance-sensitive):**
- Raw market data
- Feature computations
- Backtest details
- Real-time state

### 8.2 Commercialization Model

Strategy NFTs with:
- Genotype hash
- Performance proof (Sharpe, period, merkle root)
- Access type (exclusive/shared)
- Royalty structure

---

## 9. What We're NOT Building (V1)

| Component | Status | Rationale |
|-----------|--------|-----------|
| Triple-barrier labeling | Skip | Rules + PSO sufficient |
| SVM/XGBoost/Neural nets | Skip | No labels needed |
| Meta-labeling | Skip | Add in V2 if needed |
| Deep learning encoders | Skip | Limited data |
| Full NAS | Skip | Too expensive |

---

## 10. Implementation Roadmap

### Phase 1: Core Infrastructure (Weeks 1-2)
- [ ] Define trait interfaces (Observer, Classifier, Predictor, Sizer, Executor)
- [ ] Implement MinimalGenotype struct
- [ ] Implement PSO optimizer
- [ ] Wire genotype → backtest → fitness

### Phase 2: Module Implementation (Weeks 3-4)
- [ ] M1: Observer (use existing feature extraction)
- [ ] M2: Rule-based regime classifier
- [ ] M3a: Momentum predictor
- [ ] M3b: Mean reversion predictor
- [ ] M4: Position sizer with risk controls
- [ ] M5: Executor with stops

### Phase 3: Validation (Weeks 5-6)
- [ ] Walk-forward validation framework
- [ ] Overfitting detection metrics
- [ ] Parameter sensitivity analysis
- [ ] Out-of-sample testing

### Phase 4: Hyperliquid (Week 7)
- [ ] WebSocket adapter
- [ ] Funding rate ingestion
- [ ] HLP position tracking

### Phase 5: Evolution Experiments (Weeks 8+)
- [ ] Run PSO optimization
- [ ] Analyze Pareto fronts
- [ ] Paper trading validation

---

## 11. Open Questions

1. **Regime transition handling:** How to handle position when regime changes?
2. **Multi-asset extension:** How to scale to cross-asset scanning?
3. **Execution realism:** How to model queue position for limit orders?
4. **Compute scaling:** How many backtests per hour can we run?
5. **Walrus integration timing:** After V1 proven or during?

---

## 12. Key Insights from Discussion

### 12.1 On Genotype Design
> "If I don't limit the class of algorithm, it might diverge very fast."

Templates constrain search space while maintaining expressiveness.

### 12.2 On Supervised Learning
> "Labels not necessary → Labeled data not necessary → SVM not necessary"

Rule-based + PSO is simpler and sufficient for V1.

### 12.3 On Regime Classification
> "We do require a module which estimates current entropy such that we decide on mean reversion bets or momentum bets or we don't perform anything."

Regime classifier is the gating decision that enables conditional strategy selection.

### 12.4 On Commercial Value
> "MARS could ingest higher-level features, run labeling + ML + paper trading, all parametrized via config (genotype), then evaluate via metaheuristic algorithms."

This positions MARS as a strategy research automation platform.

---

## 13. References

- Marcos Lopez de Prado - "Advances in Financial Machine Learning"
- Avellaneda & Stoikov (2008) - Market making
- Particle Swarm Optimization literature
- NSGA-II for multi-objective optimization

---

## Document History

| Version | Date | Changes |
|---------|------|---------|
| 0.1 | 2026-01-11 | Initial brainstorming capture |

---

*This document captures the design discussion. Implementation details will be in separate task documents.*

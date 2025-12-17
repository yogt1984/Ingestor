# INGESTOR v0.2 Requirements & Roadmap

**Document Version:** 1.2
**Created:** December 13, 2025
**Last Updated:** December 17, 2025
**Philosophy:** Pivot from latency-dependent MM to prediction-dependent trend-following.

---

## Executive Summary

v0.2 represents a **strategic pivot** from pure market making to a hybrid trend-following system.

**Key Insight:** Competing on speed against HFT firms is unwinnable. Competing on prediction accuracy during identifiable regimes is viable.

**Core Strategy:**
```
Regime Detector → "Trending Up" → A-S with Heavy Bid Skew → OCO Risk Management
```

**Timeline:** 4-6 weeks
**Success Metric:** Positive Sharpe (>0.5) on 30+ days out-of-sample with bounded drawdown (<10%)

---

## Why Skip v0.1 Validation?

### v0.1 Limitations (Pure MM)

| Issue | Impact |
|-------|--------|
| Latency disadvantage | 200ms+ vs microseconds for HFT |
| Fill model uncertainty | 10% assumption is arbitrary |
| Adverse selection | Get filled when wrong |
| Negative Sharpe | All realistic backtests show losses |

### v0.2 Advantages (Trend + MM Hybrid)

| Advantage | Rationale |
|-----------|-----------|
| Latency tolerance | 5-minute trends >> 200ms delay |
| Simpler fills | Market orders fill at spread |
| Bounded risk | OCO = known max loss |
| Prediction game | Math skills apply directly |

**Conclusion:** Continuing v0.1 pure MM validation wastes time on a fundamentally disadvantaged approach. Pivot now.

---

## The Hybrid Strategy

### Core Concept

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Regime Detector │ ──► │ A-S Quote Engine │ ──► │ OCO Risk Mgmt   │
│                 │     │ (Skewed for      │     │                 │
│ "Trending Up"   │     │  trend direction)│     │ TP: +X bps      │
│ "Trending Down" │     │                  │     │ SL: -Y bps      │
│ "Mean Reverting"│     │ Wide spread when │     │                 │
│ "Uncertain"     │     │ uncertain        │     │ Max 1 position  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

### Regime States

| Regime | Detection | Action |
|--------|-----------|--------|
| **Trending Up** | Monotonic price increase + low entropy + cross-asset confirmation | Heavy bid skew, tight spread, buy bias |
| **Trending Down** | Monotonic price decrease + low entropy + cross-asset confirmation | Heavy ask skew, tight spread, sell bias |
| **Mean Reverting** | High entropy, range-bound | Classic A-S symmetric quotes |
| **Uncertain** | Conflicting signals, high volatility | Wide spread or no quotes |

### Why This Can Work

1. **Trend persistence in crypto**: BTC trends often persist 5-60 minutes
2. **Cross-asset confirmation**: When BTC, ETH, alts move together, signal is stronger
3. **Entropy filtering**: High entropy = random = don't trade. Low entropy = predictable = trade
4. **OCO bounds losses**: Even if wrong, loss is capped

---

## Feature Augmentation Plan

### Current Features (v0.1)
- Order book imbalance, depth, spread
- Trade flow (aggressor volumes)
- Tick entropy (1s, 5s, 10s)
- Volatility measures
- Mid-price dynamics

### New Features (v0.2)

#### Trend Detection Features
| Feature | Description | Implementation |
|---------|-------------|----------------|
| Price momentum | Slope of mid-price over N seconds | Linear regression coefficient |
| Monotonicity score | % of ticks in same direction | Count(up)/Count(total) |
| Hurst exponent | Trending vs mean-reverting | Rescaled range analysis |
| MA crossover | Short MA vs Long MA | EMA(10) - EMA(50) |

#### Cross-Asset Features
| Feature | Description | Implementation |
|---------|-------------|----------------|
| BTC-ETH correlation | Rolling correlation | Pearson over 60s window |
| Alt momentum | Average momentum across top alts | Mean of individual momentums |
| Cross-asset entropy | Joint entropy of price directions | Multi-symbol entropy |

#### Signal Processing Features
| Feature | Description | Implementation |
|---------|-------------|----------------|
| Wavelet trend | Low-frequency component | DWT decomposition |
| Spectral peak | Dominant cycle period | FFT of price series |
| Kalman velocity | Filtered price velocity | Kalman filter state |
| Kalman acceleration | Filtered price acceleration | Kalman filter state |

---

## Architecture Changes

### New Modules

```
src/
├── strategies/
│   ├── mod.rs
│   ├── traits.rs              # TradingStrategy trait
│   ├── trend_following.rs     # Trend detection + entry
│   └── hybrid_mm.rs           # A-S with regime-based skew
│
├── regime/
│   ├── mod.rs
│   ├── detector.rs            # RegimeDetector trait + impls
│   ├── trend_regime.rs        # Monotonic trend detection
│   └── entropy_regime.rs      # Entropy-based classification
│
├── risk/
│   ├── mod.rs
│   ├── oco_manager.rs         # OCO order management
│   └── position_manager.rs    # Position sizing, limits
│
├── features/
│   ├── mod.rs
│   ├── trend_features.rs      # Momentum, Hurst, MA crossover
│   ├── cross_asset.rs         # Multi-symbol features
│   └── signal_processing.rs   # Wavelet, FFT, Kalman
```

### Trait Design

```rust
/// Regime classification
pub trait RegimeDetector: Send + Sync {
    fn classify(&self, features: &FeaturesSnapshot) -> Regime;
    fn confidence(&self) -> f64;
}

/// Trading strategy (replaces MM-only approach)
pub trait TradingStrategy: Send + Sync {
    fn on_features(&mut self, features: &FeaturesSnapshot) -> StrategyDecision;
    fn on_fill(&mut self, fill: &Fill);
    fn current_position(&self) -> Position;
}

/// Strategy decisions
pub enum StrategyDecision {
    Hold,
    Enter { side: Side, size: Decimal, tp: Decimal, sl: Decimal },
    Exit { reason: ExitReason },
    AdjustQuotes { bid: Option<Quote>, ask: Option<Quote> },
}
```

---

## Roadmap: 4-6 Weeks

### Phase 0: Foundation (Week 1) - COMPLETE

**Goal:** Feature augmentation and regime detection primitives

| Task | Description | Effort | Priority | Status |
|------|-------------|--------|----------|--------|
| 0.1 | Implement `trend_features.rs` (momentum, monotonicity, Hurst) | 4h | HIGH | DONE |
| 0.2 | Implement `signal_processing.rs` (Kalman filter) | 4h | HIGH | DONE |
| 0.3 | Add multi-symbol data ingestion (ETH, SOL minimum) | 4h | HIGH | TODO |
| 0.4 | Implement `cross_asset.rs` (correlation, joint momentum) | 3h | MEDIUM | TODO |
| 0.5 | Create `RegimeDetector` trait and basic implementation | 3h | HIGH | DONE |
| 0.6 | Add regime labels to `FeaturesSnapshot` | 2h | HIGH | DONE |

**Deliverable:** Enhanced feature set with regime classification

**Implementation Notes (Dec 16, 2025):**
- `src/features/trend_features.rs`: TrendFeatureEngine with momentum, monotonicity, Hurst exponent, MA crossover
- `src/features/signal_processing.rs`: KalmanFilter with position/velocity/acceleration state estimation
- `src/regime/mod.rs`: MarketRegime enum, RegimeState, RegimeFeatures, ThresholdRegimeDetector, CompositeRegimeDetector

### Phase 1: Hybrid Strategy (Week 2) - NOT STARTED

**Goal:** Implement trend-following A-S hybrid

| Task | Description | Effort | Priority | Status |
|------|-------------|--------|----------|--------|
| 1.1 | Create `TradingStrategy` trait | 2h | HIGH | TODO |
| 1.2 | Implement `TrendRegimeDetector` (monotonic + entropy) | 4h | HIGH | TODO |
| 1.3 | Implement `HybridMMStrategy` (A-S with regime skew) | 6h | HIGH | TODO |
| 1.4 | Add regime-based spread/skew adjustment logic | 4h | HIGH | TODO |
| 1.5 | Implement position tracking for directional trades | 3h | HIGH | TODO |

**Deliverable:** Working hybrid strategy that adapts to detected regimes

### Phase 2: Risk Management (Week 3) - COMPLETE

**Goal:** OCO and position management

| Task | Description | Effort | Priority | Status |
|------|-------------|--------|----------|--------|
| 2.1 | Implement `OCOManager` for take-profit/stop-loss | 4h | HIGH | DONE |
| 2.2 | Implement `PositionManager` (size limits, exposure) | 3h | HIGH | DONE |
| 2.3 | Add drawdown tracking and circuit breaker | 3h | MEDIUM | DONE |
| 2.4 | Integrate OCO with backtest harness | 4h | HIGH | DONE |
| 2.5 | Add position P&L tracking in real-time | 2h | MEDIUM | DONE |

**Deliverable:** Complete risk management layer

**Implementation Notes (Dec 17, 2025):**
- `src/trading/oco_manager.rs`: OCOManager with basis points and absolute price support, comprehensive stats, 49 unit tests (1285 LOC)
- `src/trading/position_manager.rs`: PositionManager with volatility-based sizing, Kelly criterion, exposure limits, 35 unit tests
- `src/trading/risk_manager.rs`: RiskManager with staged circuit breaker (Normal/Warning/ReduceOnly/Halt/Emergency), drawdown tracking, recovery metrics
- `src/backtest/harness.rs`: OCO integration with BacktestEngine - enter_position_with_oco(), OCO trigger processing, OCOBacktestStats, 17 new integration tests
- `src/trading/pnl_tracker.rs`: RealTimePnLTracker with FIFO cost basis, P&L attribution by source, equity/drawdown curves, Sharpe ratio, 56 unit tests

### Phase 3: Backtesting & Validation (Week 4) - NOT STARTED

**Goal:** Validate hybrid strategy on historical data

| Task | Description | Effort | Priority | Status |
|------|-------------|--------|----------|--------|
| 3.1 | Update backtest harness for `TradingStrategy` trait | 4h | HIGH | TODO |
| 3.2 | Add trend-specific metrics (win rate, avg win/loss) | 3h | HIGH | TODO |
| 3.3 | Run walk-forward validation on 47 days | 2h | HIGH | TODO |
| 3.4 | Parameter sweep: regime thresholds, TP/SL ratios | 4h | HIGH | TODO |
| 3.5 | Out-of-sample test on held-out data | 2h | HIGH | TODO |
| 3.6 | Document findings and regime persistence analysis | 3h | MEDIUM | TODO |

**Deliverable:** Validated understanding of hybrid strategy performance

### Phase 4: Paper Trading (Week 5-6) - NOT STARTED

**Goal:** Live validation with real market data

| Task | Description | Effort | Priority | Status |
|------|-------------|--------|----------|--------|
| 4.1 | Integrate hybrid strategy with TUI paper trading | 4h | HIGH | TODO |
| 4.2 | Add OCO order simulation in paper trader | 4h | HIGH | TODO |
| 4.3 | Run paper trading for 2+ weeks | 2 weeks | HIGH | TODO |
| 4.4 | Compare paper vs backtest results | 4h | HIGH | TODO |
| 4.5 | Analyze regime detection accuracy in live data | 4h | MEDIUM | TODO |

**Deliverable:** Real-world validation of hybrid approach

---

## Success Criteria for v0.2

### MUST HAVE

| Criterion | Metric | Target |
|-----------|--------|--------|
| Positive Sharpe | Out-of-sample Sharpe ratio | > 0.5 |
| Bounded drawdown | Maximum drawdown | < 10% |
| Regime accuracy | Correct trend identification | > 60% |
| Win rate | Profitable trades / total trades | > 45% |
| Risk/reward | Average win / average loss | > 1.5 |

### NICE TO HAVE

- Wavelet-based trend extraction
- FFT cycle detection
- Multi-timeframe regime consensus
- Adaptive TP/SL based on volatility

### OUT OF SCOPE (v0.3+)

- Deep learning regime classifier
- Reinforcement learning position sizing
- Multi-exchange arbitrage
- Options/derivatives integration

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| No trend persistence | Medium | High | Validate regime duration empirically |
| Whipsaw losses | High | Medium | OCO limits max loss per trade |
| Overfitting regime params | Medium | High | Walk-forward validation |
| Cross-asset data quality | Low | Medium | Validate data before use |
| Regime detection lag | Medium | Medium | Use leading indicators |

---

## Key Experiments

### Experiment 1: Trend Persistence

**Question:** How long do monotonic trends persist in BTC?

**Method:**
1. Define monotonic trend: 70%+ ticks in same direction over 60s
2. Measure duration of trend continuation after detection
3. Compare to latency budget (200ms)

**Success:** Mean persistence > 5 minutes

### Experiment 2: Cross-Asset Confirmation

**Question:** Does BTC-ETH correlation predict trend continuation?

**Method:**
1. Compute rolling 60s correlation
2. Filter trades to high-correlation periods (>0.8)
3. Measure win rate vs unfiltered

**Success:** Win rate improvement > 5%

### Experiment 3: Entropy Gating

**Question:** Does low entropy predict profitable trends?

**Method:**
1. Split data by entropy quartiles
2. Run hybrid strategy on each quartile
3. Compare Sharpe ratios

**Success:** Low entropy Sharpe > 2x high entropy Sharpe

---

## Comparison: v0.1 vs v0.2

| Aspect | v0.1 (Pure MM) | v0.2 (Hybrid) |
|--------|----------------|---------------|
| Edge source | Speed (disadvantaged) | Prediction (viable) |
| Latency sensitivity | Critical | Tolerable |
| Fill modeling | Complex, uncertain | Simple, reliable |
| Risk profile | Unbounded inventory | Bounded by OCO |
| Success probability | 5-10% | 30-40% |
| Scalability | Limited | Higher |

---

## File Structure After v0.2

```
src/
├── algorithms/           # Keep existing MM algorithms
│   ├── mod.rs
│   ├── traits.rs
│   ├── avellaneda_stoikov.rs
│   ├── ml_spread_skew.rs
│   ├── fixed_spread.rs
│   └── registry.rs
│
├── strategies/           # NEW: Trading strategies
│   ├── mod.rs
│   ├── traits.rs
│   ├── trend_following.rs
│   └── hybrid_mm.rs
│
├── regime/               # NEW: Regime detection
│   ├── mod.rs
│   ├── detector.rs
│   ├── trend_regime.rs
│   └── entropy_regime.rs
│
├── features/             # NEW: Enhanced features
│   ├── mod.rs
│   ├── trend_features.rs
│   ├── cross_asset.rs
│   └── signal_processing.rs
│
├── risk/                 # NEW: Risk management
│   ├── mod.rs
│   ├── oco_manager.rs
│   └── position_manager.rs
│
├── backtest/             # Existing, extended
│   ├── ...
│   └── trend_harness.rs  # NEW: Trend strategy backtester
│
└── bin/
    ├── backtest.rs       # Extended for strategies
    └── ingestor          # Main app
```

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-12-13 | Pivot from pure MM to hybrid trend | MM requires latency we don't have |
| 2025-12-13 | Skip v0.1 paper validation | Negative Sharpe in backtests, no point validating |
| 2025-12-13 | Use A-S as execution layer, not strategy | A-S is good for quoting, not direction |
| 2025-12-13 | OCO mandatory for all trades | Bound risk, learn from losses |
| 2025-12-13 | Multi-symbol features required | Cross-asset confirmation improves signal |
| 2025-12-16 | Phase 0 foundation complete | trend_features, signal_processing, regime modules implemented |
| 2025-12-17 | Phase 2 risk management complete | OCO, position, risk managers + backtest integration |

---

## Summary

**v0.2 is a strategic pivot from "be faster" to "be smarter."**

The hybrid approach:
1. **Detects regimes** using entropy, momentum, cross-asset signals
2. **Executes with A-S** using regime-appropriate skew
3. **Manages risk with OCO** bounding every trade

This leverages your mathematical background (regime detection, signal processing) while avoiding the latency game you cannot win.

**Probability of viable trading system: 30-40%**

This is excellent odds for a solo effort building foundational quant infrastructure.

---

*Document maintained by: Development Team*
*Last updated: December 17, 2025*

# MARS MVP Implementation Tasks - Entropy Feature Suite

**Version:** 1.26
**Date:** 2026-01-12
**Status:** Planning
**Goal:** Implement entropy-based MVP for regime detection and filtered trading strategies

This document defines all tasks required to implement the MARS MVP focused on entropy features as the core differentiator, as specified in BRAINSTORMING_0.md.

---

## Task Organization

Tasks are organized by:
- **Phase** (1-5): Implementation phases
- **Task ID** (E-XX.XX): Unique task identifier (E = Entropy MVP)
- **Priority**: 🔴 Critical, 🟡 High, 🟢 Medium
- **Dependencies**: Tasks that must be completed first

---

## Phase 1: Core Entropy Features (Week 1)

### E-1.1: Implement Entropy Derivatives Module
**Priority:** 🔴 Critical
**Dependencies:** None (builds on existing `tick_entropy`)
**Estimated Time:** 4 hours

**Description:** Create module for entropy derivative calculations.

**Tasks:**
- [ ] Create `src/entropy/derivatives.rs`
- [ ] Implement `EntropySMA` struct (configurable window)
- [ ] Implement `entropy_sma(window: usize)` function
- [ ] Implement `entropy_roc(window: usize)` function (rate of change)
- [ ] Implement `entropy_zscore(window: usize)` function
- [ ] Implement `entropy_acceleration(window: usize)` function
- [ ] Add unit tests for each derivative
- [ ] Add integration tests with existing entropy calculation
- [ ] Update `src/entropy/mod.rs` exports

**Acceptance Criteria:**
- [ ] All derivatives compile without errors
- [ ] Unit tests pass with >95% coverage
- [ ] Derivatives integrate with existing FeaturesSnapshot
- [ ] Performance: <1ms per derivative calculation

---

### E-1.2: Implement Multi-Scale Entropy
**Priority:** 🔴 Critical
**Dependencies:** E-1.1
**Estimated Time:** 4 hours

**Description:** Create multi-scale entropy calculations at different time horizons.

**Tasks:**
- [ ] Create `src/entropy/multiscale.rs`
- [ ] Implement `MultiScaleEntropy` struct
- [ ] Implement `entropy_fast(window: 50)` - ~5 second scale
- [ ] Implement `entropy_medium(window: 300)` - ~30 second scale
- [ ] Implement `entropy_slow(window: 1800)` - ~3 minute scale
- [ ] Implement `entropy_divergence()` - fast minus slow
- [ ] Implement efficient rolling window calculation
- [ ] Add unit tests for each scale
- [ ] Add tests for divergence calculation
- [ ] Update module exports

**Acceptance Criteria:**
- [ ] All scales compile without errors
- [ ] Rolling window is memory-efficient
- [ ] Divergence correctly identifies regime transitions
- [ ] Unit tests pass

---

### E-1.3: Integrate Entropy Features into FeaturesSnapshot
**Priority:** 🔴 Critical
**Dependencies:** E-1.1, E-1.2
**Estimated Time:** 3 hours

**Description:** Add new entropy features to the main FeaturesSnapshot struct.

**Tasks:**
- [ ] Update `FeaturesSnapshot` struct in `src/orderbook.rs`
- [ ] Add `entropy_sma_20: Option<f64>`
- [ ] Add `entropy_sma_100: Option<f64>`
- [ ] Add `entropy_roc: Option<f64>`
- [ ] Add `entropy_zscore: Option<f64>`
- [ ] Add `entropy_acceleration: Option<f64>`
- [ ] Add `entropy_fast: Option<f64>`
- [ ] Add `entropy_medium: Option<f64>`
- [ ] Add `entropy_slow: Option<f64>`
- [ ] Add `entropy_divergence: Option<f64>`
- [ ] Update Parquet schema for new fields
- [ ] Update feature extraction pipeline
- [ ] Add backward compatibility for existing Parquet files
- [ ] Write integration tests

**Acceptance Criteria:**
- [ ] FeaturesSnapshot compiles with new fields
- [ ] Parquet serialization works
- [ ] Backward compatibility maintained
- [ ] Feature extraction pipeline produces valid values

---

### E-1.4: Implement Entropy Feature Calculator
**Priority:** 🔴 Critical
**Dependencies:** E-1.1, E-1.2, E-1.3
**Estimated Time:** 4 hours

**Description:** Create unified calculator that produces all entropy features from tick stream.

**Tasks:**
- [ ] Create `src/entropy/calculator.rs`
- [ ] Implement `EntropyCalculator` struct
- [ ] Implement `EntropyCalculatorConfig` for window sizes
- [ ] Implement `update(tick_direction: i8)` method
- [ ] Implement `get_features() -> EntropyFeatures` method
- [ ] Implement efficient state management (circular buffers)
- [ ] Add warmup detection (not enough data yet)
- [ ] Write unit tests
- [ ] Write performance benchmarks
- [ ] Document API

**Acceptance Criteria:**
- [ ] Calculator produces all 10 entropy features
- [ ] State management is memory-efficient
- [ ] Warmup handling works correctly
- [ ] Performance: <100μs per update

---

## Phase 2: Regime Classification (Week 2)

### E-2.1: Define Regime Types and Thresholds
**Priority:** 🔴 Critical
**Dependencies:** Phase 1 complete
**Estimated Time:** 2 hours

**Description:** Define regime enumeration and configurable thresholds.

**Tasks:**
- [ ] Create `src/regime/mod.rs`
- [ ] Define `Regime` enum: `{ Trend, Revert, Avoid }`
- [ ] Define `RegimeConfidence` struct (regime + confidence: f64)
- [ ] Define `RegimeThresholds` struct with defaults:
  - `entropy_trend_max: 0.45`
  - `entropy_avoid_min: 0.70`
  - `entropy_zscore_trend: -0.5`
  - `entropy_zscore_avoid: 1.5`
  - `vol_zscore_avoid: 2.0`
- [ ] Implement `Default` for `RegimeThresholds`
- [ ] Implement serialization (serde)
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Regime types compile
- [ ] Thresholds are configurable
- [ ] Serialization works (for config files)
- [ ] Unit tests pass

---

### E-2.2: Implement Regime Classifier
**Priority:** 🔴 Critical
**Dependencies:** E-2.1, Phase 1
**Estimated Time:** 6 hours

**Description:** Implement rule-based regime classifier.

**Tasks:**
- [ ] Create `src/regime/classifier.rs`
- [ ] Implement `RegimeClassifier` struct
- [ ] Implement `new(thresholds: RegimeThresholds)` constructor
- [ ] Implement `classify(features: &EntropyFeatures) -> RegimeConfidence`
- [ ] Implement TREND detection logic:
  ```
  entropy_medium < threshold AND entropy_zscore < zscore_threshold
  ```
- [ ] Implement AVOID detection logic:
  ```
  entropy_medium > threshold OR entropy_zscore > zscore_threshold
  OR vol_zscore > vol_threshold
  ```
- [ ] Implement REVERT detection (default when not TREND or AVOID)
- [ ] Implement confidence calculation
- [ ] Add hysteresis to prevent rapid regime flipping
- [ ] Write unit tests for each regime
- [ ] Write integration tests with real data samples

**Acceptance Criteria:**
- [ ] Classifier correctly identifies all 3 regimes
- [ ] Confidence scores are in [0, 1]
- [ ] Hysteresis prevents flipping
- [ ] Unit tests pass
- [ ] Integration tests pass

---

### E-2.3: Add Regime to FeaturesSnapshot
**Priority:** 🟡 High
**Dependencies:** E-2.2
**Estimated Time:** 2 hours

**Description:** Add classified regime to feature snapshots.

**Tasks:**
- [ ] Add `regime: Option<Regime>` to `FeaturesSnapshot`
- [ ] Add `regime_confidence: Option<f64>` to `FeaturesSnapshot`
- [ ] Update Parquet schema
- [ ] Update feature extraction to include regime
- [ ] Add regime to TUI display (if applicable)
- [ ] Write tests

**Acceptance Criteria:**
- [ ] Regime in FeaturesSnapshot
- [ ] Parquet persistence works
- [ ] Tests pass

---

### E-2.4: Implement Regime Persistence Metrics
**Priority:** 🟢 Medium
**Dependencies:** E-2.2
**Estimated Time:** 3 hours

**Description:** Track regime duration and transition statistics.

**Tasks:**
- [ ] Create `src/regime/metrics.rs`
- [ ] Implement `RegimeMetrics` struct
- [ ] Track `current_regime_duration` (ticks/time)
- [ ] Track `regime_transition_count`
- [ ] Track `avg_regime_duration` per regime type
- [ ] Track `regime_distribution` (% time in each)
- [ ] Implement `update(regime: Regime)` method
- [ ] Implement `get_metrics() -> RegimeStats`
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Metrics track regime persistence
- [ ] Statistics are accurate
- [ ] Unit tests pass

---

## Phase 3: Helper Features (Week 2-3)

### E-3.1: Implement Autocorrelation Feature
**Priority:** 🟡 High
**Dependencies:** None
**Estimated Time:** 3 hours

**Description:** Add return autocorrelation feature for trend confirmation.

**Tasks:**
- [ ] Create `src/features/autocorr.rs`
- [ ] Implement `autocorrelation(returns: &[f64], lag: usize) -> f64`
- [ ] Implement rolling autocorrelation calculator
- [ ] Add `autocorr_20: Option<f64>` to FeaturesSnapshot
- [ ] Update Parquet schema
- [ ] Write unit tests
- [ ] Validate against known values

**Acceptance Criteria:**
- [ ] Autocorrelation calculation correct
- [ ] Rolling version is efficient
- [ ] Integration with FeaturesSnapshot
- [ ] Tests pass

---

### E-3.2: Implement Volatility Z-Score
**Priority:** 🟡 High
**Dependencies:** None
**Estimated Time:** 2 hours

**Description:** Add normalized volatility feature.

**Tasks:**
- [ ] Create `src/features/vol_zscore.rs`
- [ ] Implement `vol_zscore(current_vol, vol_sma, vol_std) -> f64`
- [ ] Implement rolling calculation
- [ ] Add `vol_zscore: Option<f64>` to FeaturesSnapshot
- [ ] Update Parquet schema
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Vol z-score calculation correct
- [ ] Integration with FeaturesSnapshot
- [ ] Tests pass

---

### E-3.3: Implement Price Path Smoothness (R²)
**Priority:** 🟢 Medium
**Dependencies:** None
**Estimated Time:** 3 hours

**Description:** Add R² of linear regression as path smoothness measure.

**Tasks:**
- [ ] Create `src/features/path_smoothness.rs`
- [ ] Implement `linear_regression_r2(prices: &[f64]) -> f64`
- [ ] Implement rolling R² calculator
- [ ] Add `price_r2_50: Option<f64>` to FeaturesSnapshot
- [ ] Update Parquet schema
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] R² calculation correct (0 = no fit, 1 = perfect line)
- [ ] Rolling version efficient
- [ ] Integration with FeaturesSnapshot
- [ ] Tests pass

---

### E-3.4: Implement Composite Signals
**Priority:** 🟡 High
**Dependencies:** E-1.3, E-3.1, E-3.2
**Estimated Time:** 4 hours

**Description:** Create composite signals combining entropy with other features.

**Tasks:**
- [ ] Create `src/features/composites.rs`
- [ ] Implement `trend_quality(momentum, entropy) -> f64`
  ```
  trend_quality = momentum × (1 - entropy_medium)
  ```
- [ ] Implement `reversion_risk(zscore, entropy) -> f64`
  ```
  reversion_risk = abs(zscore) × entropy_medium
  ```
- [ ] Implement `flow_clarity(imbalance, entropy) -> f64`
  ```
  flow_clarity = imbalance × (1 - entropy_medium)
  ```
- [ ] Implement `regime_stability(divergence) -> f64`
  ```
  regime_stability = 1 - abs(entropy_divergence)
  ```
- [ ] Add all composites to FeaturesSnapshot
- [ ] Update Parquet schema
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] All composites calculate correctly
- [ ] Values are in expected ranges
- [ ] Integration with FeaturesSnapshot
- [ ] Tests pass

---

## Phase 4: Visual Validation (Week 3)

### E-4.1: Create Python Validation Notebook
**Priority:** 🔴 Critical
**Dependencies:** Phase 1, Phase 2
**Estimated Time:** 4 hours

**Description:** Create Jupyter notebook for visual hypothesis validation.

**Tasks:**
- [ ] Create `notebooks/entropy_validation.ipynb`
- [ ] Load Parquet data with new entropy features
- [ ] Create Plot 1: Price + Entropy time series overlay
- [ ] Create Plot 2: Entropy vs forward returns scatter
- [ ] Create Plot 3: Regime heatmap over time
- [ ] Create Plot 4: Return distribution by regime
- [ ] Create Plot 5: Regime persistence histogram
- [ ] Add statistical tests (correlation, t-tests)
- [ ] Document findings

**Acceptance Criteria:**
- [ ] Notebook runs without errors
- [ ] All 5 plots render correctly
- [ ] Statistical tests produce results
- [ ] Findings documented

---

### E-4.2: Create Validation Report
**Priority:** 🟡 High
**Dependencies:** E-4.1
**Estimated Time:** 2 hours

**Description:** Document validation results and go/no-go decision.

**Tasks:**
- [ ] Create `docs/ENTROPY_VALIDATION_REPORT.md`
- [ ] Document hypothesis: "Low entropy predicts regime quality"
- [ ] Document data: period, samples, symbols
- [ ] Document methodology
- [ ] Document results with screenshots
- [ ] Document statistical significance
- [ ] Document conclusions
- [ ] Make go/no-go recommendation

**Acceptance Criteria:**
- [ ] Report is complete
- [ ] Evidence supports conclusions
- [ ] Clear recommendation made

---

## Phase 5: Strategy Integration (Week 4)

### E-5.1: Implement Regime-Filtered Momentum Signal
**Priority:** 🟡 High
**Dependencies:** Phase 2 complete, validation positive
**Estimated Time:** 6 hours

**Description:** Create momentum signal that only fires in TREND regime.

**Tasks:**
- [ ] Create `src/strategies/momentum_filtered.rs`
- [ ] Implement `FilteredMomentumConfig` struct
- [ ] Implement `FilteredMomentumStrategy` struct
- [ ] Implement signal generation:
  ```
  if regime == TREND:
      momentum_raw = w1×ret_short + w2×ret_medium + w3×ret_long
      signal = sign(momentum_raw) × min(1, |momentum_raw| / scale)
      return signal × trend_quality
  else:
      return 0.0
  ```
- [ ] Add entry threshold
- [ ] Add signal scaling
- [ ] Integrate with existing algorithm framework
- [ ] Write unit tests
- [ ] Write backtest integration tests

**Acceptance Criteria:**
- [ ] Strategy only signals in TREND regime
- [ ] Trend quality weighting works
- [ ] Integration with backtest harness
- [ ] Tests pass

---

### E-5.2: Implement Regime-Filtered Mean Reversion Signal
**Priority:** 🟡 High
**Dependencies:** Phase 2 complete, validation positive
**Estimated Time:** 6 hours

**Description:** Create mean reversion signal that only fires in REVERT regime.

**Tasks:**
- [ ] Create `src/strategies/reversion_filtered.rs`
- [ ] Implement `FilteredReversionConfig` struct
- [ ] Implement `FilteredReversionStrategy` struct
- [ ] Implement signal generation:
  ```
  if regime == REVERT:
      zscore = (price - sma) / std
      if abs(zscore) > entry_threshold:
          signal = -sign(zscore) × min(1, |zscore| / scale)
          return signal × (1 - reversion_risk)
      return 0.0
  else:
      return 0.0
  ```
- [ ] Add entry/exit thresholds
- [ ] Add signal scaling
- [ ] Integrate with existing algorithm framework
- [ ] Write unit tests
- [ ] Write backtest integration tests

**Acceptance Criteria:**
- [ ] Strategy only signals in REVERT regime
- [ ] Reversion risk weighting works
- [ ] Integration with backtest harness
- [ ] Tests pass

---

### E-5.3: Implement Combined Regime Strategy
**Priority:** 🟡 High
**Dependencies:** E-5.1, E-5.2
**Estimated Time:** 4 hours

**Description:** Create meta-strategy that dispatches to appropriate sub-strategy based on regime.

**Tasks:**
- [ ] Create `src/strategies/regime_combined.rs`
- [ ] Implement `CombinedRegimeConfig` struct
- [ ] Implement `CombinedRegimeStrategy` struct
- [ ] Implement regime dispatch:
  ```
  match regime:
      TREND -> momentum_strategy.generate_signal()
      REVERT -> reversion_strategy.generate_signal()
      AVOID -> 0.0 (no trade)
  ```
- [ ] Add position management for regime transitions
- [ ] Integrate with existing algorithm framework
- [ ] Write unit tests
- [ ] Write backtest integration tests

**Acceptance Criteria:**
- [ ] Strategy dispatches correctly by regime
- [ ] AVOID regime produces no signals
- [ ] Position management handles transitions
- [ ] Tests pass

---

### E-5.4: Backtest Regime Strategies
**Priority:** 🔴 Critical
**Dependencies:** E-5.1, E-5.2, E-5.3
**Estimated Time:** 8 hours

**Description:** Run comprehensive backtests on regime-filtered strategies.

**Tasks:**
- [ ] Configure backtest harness for regime strategies
- [ ] Run momentum-only backtest (unfiltered baseline)
- [ ] Run filtered momentum backtest (TREND only)
- [ ] Run reversion-only backtest (unfiltered baseline)
- [ ] Run filtered reversion backtest (REVERT only)
- [ ] Run combined regime strategy backtest
- [ ] Compare Sharpe ratios: filtered vs unfiltered
- [ ] Compare drawdowns: filtered vs unfiltered
- [ ] Calculate AVOID regime saves (trades avoided that would lose)
- [ ] Document results

**Acceptance Criteria:**
- [ ] All backtests complete successfully
- [ ] Filtered strategies outperform unfiltered
- [ ] AVOID regime demonstrably saves bad trades
- [ ] Results documented

---

### E-5.5: Walk-Forward Validation
**Priority:** 🔴 Critical
**Dependencies:** E-5.4
**Estimated Time:** 6 hours

**Description:** Run walk-forward validation to test out-of-sample performance.

**Tasks:**
- [ ] Configure walk-forward harness
- [ ] Run 5-fold walk-forward on combined regime strategy
- [ ] Calculate IS Sharpe per fold
- [ ] Calculate OOS Sharpe per fold
- [ ] Calculate Sharpe retention (OOS/IS)
- [ ] Calculate generalization gap
- [ ] Test parameter stability across folds
- [ ] Document results

**Acceptance Criteria:**
- [ ] Walk-forward completes
- [ ] OOS Sharpe retention > 50%
- [ ] Parameters stable across folds
- [ ] Results documented

---

## Summary

### Total Tasks: 22
- **Phase 1:** 4 tasks (Core Entropy Features)
- **Phase 2:** 4 tasks (Regime Classification)
- **Phase 3:** 4 tasks (Helper Features)
- **Phase 4:** 2 tasks (Visual Validation)
- **Phase 5:** 8 tasks (Strategy Integration)

### Estimated Time: ~80 hours (~2 weeks @ 40 hours/week)

### Critical Path:
1. Phase 1 (Entropy Features) - Must complete first
2. Phase 2 (Regime Classification) - Depends on Phase 1
3. Phase 4 (Validation) - Can start after Phase 2, CRITICAL GATE
4. Phase 3 (Helper Features) - Can run parallel to Phase 2
5. Phase 5 (Strategy Integration) - Only after validation positive

### Go/No-Go Gate:
**After Phase 4 (E-4.2)**: If validation shows entropy does NOT predict regime quality:
- Stop development
- Document findings
- Pivot to alternative approach

If validation is positive:
- Proceed to Phase 5
- Build strategies on validated foundation

---

## Task Status Tracking

Use this section to track task completion:

### Phase 1: Core Entropy Features
- [ ] E-1.1: Implement Entropy Derivatives Module
- [ ] E-1.2: Implement Multi-Scale Entropy
- [ ] E-1.3: Integrate Entropy Features into FeaturesSnapshot
- [ ] E-1.4: Implement Entropy Feature Calculator

### Phase 2: Regime Classification
- [ ] E-2.1: Define Regime Types and Thresholds
- [ ] E-2.2: Implement Regime Classifier
- [ ] E-2.3: Add Regime to FeaturesSnapshot
- [ ] E-2.4: Implement Regime Persistence Metrics

### Phase 3: Helper Features
- [ ] E-3.1: Implement Autocorrelation Feature
- [ ] E-3.2: Implement Volatility Z-Score
- [ ] E-3.3: Implement Price Path Smoothness (R²)
- [ ] E-3.4: Implement Composite Signals

### Phase 4: Visual Validation ⚠️ CRITICAL GATE
- [ ] E-4.1: Create Python Validation Notebook
- [ ] E-4.2: Create Validation Report (GO/NO-GO DECISION)

### Phase 5: Strategy Integration (Only if validation positive)
- [ ] E-5.1: Implement Regime-Filtered Momentum Signal
- [ ] E-5.2: Implement Regime-Filtered Mean Reversion Signal
- [ ] E-5.3: Implement Combined Regime Strategy
- [ ] E-5.4: Backtest Regime Strategies
- [ ] E-5.5: Walk-Forward Validation

---

## Success Metrics

| Metric | Target | Measured At |
|--------|--------|-------------|
| Regime persistence | >5 min average | E-2.4 |
| TREND regime win rate | >55% | E-5.4 |
| AVOID regime saves | >60% avoided = losers | E-5.4 |
| Filtered vs unfiltered Sharpe | >20% improvement | E-5.4 |
| OOS Sharpe retention | >50% | E-5.5 |
| Parameter stability | <20% variation | E-5.5 |

---

## Feature Summary

### New Features (16 total)

**Entropy Core (10 features):**
1. `tick_entropy` - EXISTS
2. `entropy_sma_20` - ADD
3. `entropy_sma_100` - ADD
4. `entropy_roc` - ADD
5. `entropy_zscore` - ADD
6. `entropy_acceleration` - ADD
7. `entropy_fast` - ADD
8. `entropy_medium` - ADD
9. `entropy_slow` - ADD
10. `entropy_divergence` - ADD

**Helper Features (3 features):**
11. `autocorr_20` - ADD
12. `vol_zscore` - ADD
13. `price_r2_50` - ADD

**Composite Signals (4 features):**
14. `trend_quality` - ADD
15. `reversion_risk` - ADD
16. `flow_clarity` - ADD
17. `regime_stability` - ADD

**Classification (2 fields):**
18. `regime` - ADD
19. `regime_confidence` - ADD

---

*Document Version: 1.26*
*Last Updated: 2026-01-12*
*Status: Ready for Implementation*

# Advanced Entropy Features - Implementation Plan

**Version:** 1.0
**Date:** 2026-01-14
**Status:** Analysis & Planning

## Executive Summary

This document analyzes **10 advanced entropy features** not currently in TASKS_1_26.md, prioritizes them by implementation complexity and impact, and provides a clear action plan for integration into the MARS platform.

---

## Gap Analysis

### Current Implementation (TASKS_1_26.md)
- Shannon Entropy at multiple timeframes ✅
- Entropy derivatives (SMA, ROC, z-score, acceleration) ✅
- Multi-scale approach (simplified) ✅
- Regime classification (trend/revert/avoid) ✅

### Missing Advanced Features

| Feature | Category | Complexity | Impact | Priority |
|---------|----------|------------|--------|----------|
| **Permutation Entropy** | Pattern | Low | High | P0 |
| **Sample Entropy** | Regularity | Low | High | P0 |
| **Order Book Entropy** | Structure | Low | High | P0 |
| **KL Divergence** | Transition | Low | High | P0 |
| **Spectral Entropy** | Frequency | Medium | Medium | P1 |
| **Approximate Entropy** | Regularity | Medium | Medium | P1 |
| **Transfer Entropy** | Causality | High | High | P2 |
| **Weighted Permutation** | Pattern | Medium | Medium | P2 |
| **Tsallis Entropy** | Robustness | High | Low | P3 |
| **Rényi Entropy** | Theory | High | Low | P3 |

---

## Priority 0 Features (Quick Wins - High Impact, Low Complexity)

### 1. Permutation Entropy

**What it measures:** Complexity of ordinal patterns in time series

**Why it matters:**
- Robust to noise and outliers (uses ranks, not raw values)
- Captures temporal structure without magnitude sensitivity
- Very fast O(N) computation
- Proven in financial applications

**Mathematical formulation:**
```
For embedding dimension m, map sequence to ordinal patterns:
  [x₁, x₂, ..., xₘ] → permutation π

Example (m=3):
  [100.5, 100.3, 100.8] → (2, 1, 3) meaning "middle < low < high"

Count frequencies p(π) for all m! possible patterns:
  H_perm = -∑ p(π) log₂(p(π))

Normalized:
  H_norm = H_perm / log₂(m!)
```

**Regime interpretation:**
- `H_norm → 1.0`: All patterns equally likely → Random/efficient market
- `H_norm → 0.0`: Few dominant patterns → Structured (trend or mean-revert)
- `H_norm ∈ [0.6, 0.8]`: Typical for liquid markets
- `H_norm < 0.5`: Strong structure, exploitable

**Implementation complexity:** ~60 LOC
- Circular buffer for rolling window
- Pattern counting with HashMap
- Normalize by max entropy

**Integration:**
- Add `permutation_entropy_10s`, `permutation_entropy_1m`, `permutation_entropy_5m`
- Use m=3 (6 patterns), m=4 (24 patterns), or m=5 (120 patterns)
- Recommended: m=3 for speed, m=4 for accuracy

**Reference:** Bandt & Pompe (2002), Physical Review Letters

---

### 2. Sample Entropy (SampEn)

**What it measures:** Regularity and predictability of time series

**Why it matters:**
- Improved version of Approximate Entropy (removes self-matching bias)
- More consistent for short time series
- Lower variance than ApEn
- Detects when markets become predictable

**Mathematical formulation:**
```
For embedding dimension m, tolerance r, and N data points:

1. Create template vectors of length m:
   X_m(i) = [x_i, x_{i+1}, ..., x_{i+m-1}]

2. Count matches within tolerance r:
   B^m(r) = (N-m)⁻¹ ∑ᵢ [count of j where d(X_m(i), X_m(j)) ≤ r, i≠j]
   A^m(r) = same for m+1 dimension

3. Sample entropy:
   SampEn(m, r, N) = -ln(A^m(r) / B^m(r))

Parameters:
  m = 2 (embedding dimension)
  r = 0.15 × std(data) (tolerance)
```

**Regime interpretation:**
- `SampEn → 0`: Highly regular, predictable → Trending regime
- `SampEn → 2+`: Irregular, complex → Random/mean-reverting regime
- `SampEn < 0.5`: Strong trend, high predictability
- `SampEn > 1.5`: Noisy, low predictability

**Implementation complexity:** ~80 LOC
- Compute pairwise distances (O(N²) naive, O(N log N) with k-d tree)
- Count matches within tolerance
- Handle edge cases (A=0)

**Integration:**
- Add `sample_entropy_30s`, `sample_entropy_1m`, `sample_entropy_5m`
- Use m=2, r=0.15×std as defaults
- Combine with Shannon entropy for robust regime detection

**Reference:** Richman & Moorman (2000), American Journal of Physiology

---

### 3. Order Book Entropy

**What it measures:** Distribution of liquidity across price levels

**Why it matters:**
- Direct measure of market depth and resilience
- Detects thin vs deep book regimes
- Identifies concentrated vs dispersed liquidity
- Complements price-based entropy measures

**Mathematical formulation:**

**3a. Price Level Entropy (Volume Distribution)**
```
For N price levels with volumes [v₁, v₂, ..., vₙ]:
  V = ∑vᵢ (total volume)
  pᵢ = vᵢ / V (probability)

  H_levels = -∑ pᵢ log₂(pᵢ)

Normalized:
  H_norm = H_levels / log₂(N)
```

**3b. Bid-Ask Entropy Asymmetry**
```
  H_bid = entropy of bid side
  H_ask = entropy of ask side

  ΔH = H_bid - H_ask
```

**3c. Order Flow Entropy (Trade Size Distribution)**
```
Discretize trades into size buckets:
  [small: <10%, medium: 10-50%, large: >50% of avg]

For each (size, side) combination:
  p(size, side) = count / total

  H_flow = -∑∑ p(size, side) log₂(p(size, side))
```

**Regime interpretation:**

**Price Level Entropy:**
- `H_levels → 1.0`: Volume spread across many levels → Deep, resilient book
- `H_levels → 0.0`: Volume concentrated at few levels → Thin, fragile book
- `H_levels > 0.8`: Good for market making (deep liquidity)
- `H_levels < 0.5`: Risky for market making (thin liquidity)

**Bid-Ask Asymmetry:**
- `ΔH > 0`: Bid side more spread out → Selling pressure
- `ΔH < 0`: Ask side more spread out → Buying pressure
- `|ΔH| > 0.1`: Directional imbalance

**Order Flow Entropy:**
- `H_flow → low`: Uniform trade sizes → Algorithmic/informed trading
- `H_flow → high`: Variable sizes → Retail/uninformed mix

**Implementation complexity:** ~120 LOC
- Extract top N levels from OrderBook (already available)
- Compute entropy over volume distribution
- Add rolling window for order flow entropy
- Separate bid/ask calculations

**Integration:**
- Add `orderbook_entropy`, `orderbook_bid_entropy`, `orderbook_ask_entropy`
- Add `orderbook_entropy_asymmetry`
- Add `orderflow_entropy_1m`, `orderflow_entropy_5m`
- Use top 10 levels for price level entropy

**No external reference needed** (standard information theory application)

---

### 4. KL Divergence (Relative Entropy)

**What it measures:** How much current distribution diverges from reference baseline

**Why it matters:**
- Detects regime **transitions** (not just static regimes)
- Quantifies "surprise" or "abnormality" of current state
- Early warning system for regime changes
- Complements static entropy measures

**Mathematical formulation:**
```
Kullback-Leibler divergence from Q to P:
  D_KL(P || Q) = ∑ P(x) log₂(P(x) / Q(x))

Where:
  P = current distribution (rolling window)
  Q = reference distribution (baseline)

Symmetric version (Jensen-Shannon divergence):
  M = (P + Q) / 2
  JSD(P, Q) = [D_KL(P || M) + D_KL(Q || M)] / 2
```

**Application variants:**

**4a. Tick Distribution Divergence**
```
P = current tick distribution (up/down/unch)
Q = 1-hour historical baseline
D_KL(P || Q) → measures deviation from typical behavior
```

**4b. Volatility Distribution Divergence**
```
P = current return distribution (histogram)
Q = 1-day historical baseline
D_KL(P || Q) → detects volatility regime changes
```

**4c. Order Flow Distribution Divergence**
```
P = current trade size distribution
Q = session-start baseline
D_KL(P || Q) → detects informed flow arrival
```

**Regime interpretation:**
- `D_KL → 0.0`: Same regime as reference (stable)
- `D_KL > 0.5`: Moderate divergence (regime shift starting)
- `D_KL > 1.0`: Strong divergence (regime transition)
- `D_KL > 2.0`: Extreme divergence (avoid trading)

**Spike in D_KL** = regime transition in progress

**Implementation complexity:** ~60 LOC
- Maintain reference distribution (sliding window)
- Compute current distribution (rolling window)
- Handle zero probabilities (add smoothing: +1e-10)
- Compute KL or JSD

**Integration:**
- Add `kl_div_tick_1m`, `kl_div_tick_5m` (tick distribution vs baseline)
- Add `kl_div_volatility_1m`, `kl_div_volatility_5m` (return distribution vs baseline)
- Add `regime_transition_score` (composite of KL divergences)

**Reference:** Kullback & Leibler (1951), Lin (1991) for JSD

---

## Priority 1 Features (Medium Complexity, Medium-High Impact)

### 5. Spectral Entropy

**What it measures:** Flatness of power spectral density (frequency domain)

**Why it matters:**
- Detects cyclical vs random dynamics
- Identifies dominant frequencies (if any)
- Complements time-domain entropy
- Useful for detecting algorithmic trading patterns

**Mathematical formulation:**
```
1. Compute FFT of return series:
   R(f) = FFT(returns)

2. Compute power spectral density:
   P(f) = |R(f)|²

3. Normalize:
   p(f) = P(f) / ∑P(f)

4. Shannon entropy in frequency domain:
   H_spectral = -∑ p(f) log₂(p(f))

5. Normalize:
   H_norm = H_spectral / log₂(N)
```

**Regime interpretation:**
- `H_norm → 1.0`: White noise (flat spectrum) → Random walk
- `H_norm → 0.0`: Dominated by few frequencies → Cyclical/trending
- `H_norm > 0.9`: High-frequency noise regime
- `H_norm < 0.6`: Structured, low-frequency dominated

**Implementation complexity:** ~90 LOC
- Use `rustfft` crate for FFT
- Compute power spectrum
- Handle window functions (Hann, Hamming)
- Normalize entropy

**Integration:**
- Add `spectral_entropy_5m`, `spectral_entropy_15m`
- Add `dominant_frequency` (frequency with max power)
- Use 256 or 512 point FFT

**Reference:** Inouye et al. (1991), IEEE Transactions

---

### 6. Approximate Entropy (ApEn)

**What it measures:** Regularity and predictability (predecessor to SampEn)

**Why it matters:**
- Well-established in biomedical signal processing
- Simpler than SampEn but similar insights
- Good for sanity-checking SampEn results
- Widely cited (easier to compare with literature)

**Mathematical formulation:**
```
For embedding dimension m, tolerance r:

1. Create patterns of length m:
   x_m(i) = [x_i, ..., x_{i+m-1}]

2. Count matches within tolerance r:
   C_i^m(r) = (count of j where d(x_m(i), x_m(j)) ≤ r) / (N-m+1)

3. Average log frequency:
   Φ^m(r) = (N-m+1)⁻¹ ∑ᵢ log(C_i^m(r))

4. Approximate entropy:
   ApEn(m, r, N) = Φ^m(r) - Φ^{m+1}(r)

Parameters:
  m = 2
  r = 0.2 × std(data)
```

**Regime interpretation:**
- `ApEn → 2.0`: Irregular, unpredictable → Mean-reverting
- `ApEn → 0.0`: Regular, predictable → Trending
- Similar to SampEn interpretation

**Implementation complexity:** ~100 LOC
- Similar to SampEn but includes self-matches
- O(N²) complexity (can optimize with spatial index)

**Integration:**
- Add `approx_entropy_30s`, `approx_entropy_1m`
- Use alongside SampEn for validation
- If ApEn ≈ SampEn → reliable signal

**Reference:** Pincus (1991), PNAS

---

## Priority 2 Features (High Complexity, High Impact)

### 7. Transfer Entropy

**What it measures:** Directional information flow between time series (causality)

**Why it matters:**
- Detects when one variable **causes** another
- Identifies informed trading (order flow → price)
- Measures market efficiency (how fast info is priced)
- Critical for detecting exploitable edges

**Mathematical formulation:**
```
Transfer entropy from X to Y:

T_{X→Y} = ∑ p(y_{t+1}, y_t^k, x_t^l) log₂[
    p(y_{t+1} | y_t^k, x_t^l) / p(y_{t+1} | y_t^k)
]

Where:
  y_t^k = [y_t, y_{t-1}, ..., y_{t-k+1}] (history of Y)
  x_t^l = [x_t, x_{t-1}, ..., x_{t-l+1}] (history of X)
  k, l = embedding dimensions (typically 1-3)
```

**Use cases:**

**7a. Order Flow → Price**
```
X = signed order flow (buyer - seller initiated)
Y = mid price returns
T_{flow→price} → high = order flow predicts price (informed regime)
```

**7b. Volatility → Spread**
```
X = realized volatility
Y = bid-ask spread
T_{vol→spread} → high = MMs reacting to vol (risk regime)
```

**7c. BTC → ETH** (cross-asset)
```
X = BTC returns
Y = ETH returns
T_{BTC→ETH} → high = contagion/correlation regime
```

**Regime interpretation:**
- `T_{X→Y} → 0`: No causal information flow (efficient/random)
- `T_{X→Y} > 0.1`: Weak causality (some predictability)
- `T_{X→Y} > 0.5`: Strong causality (exploitable edge)
- `T_{X→Y} > 1.0`: Very strong causality (informed flow regime)

**If T_{flow→price} is high**: Informed trading, avoid market making

**Implementation complexity:** ~200 LOC
- Requires probability density estimation (histograms or KDE)
- High dimensional joint probabilities
- Computationally expensive (O(N²) or worse)
- Needs careful binning strategy

**Integration:**
- Add `transfer_entropy_flow_to_price_1m`
- Add `transfer_entropy_vol_to_spread_1m`
- Add `informed_flow_score` (composite of transfer entropies)
- Compute every 10-30 seconds (not every tick)

**Reference:** Schreiber (2000), Physical Review Letters

---

### 8. Weighted Permutation Entropy

**What it measures:** Pattern complexity with magnitude awareness

**Why it matters:**
- Permutation entropy ignores whether move is 1 tick or 100 ticks
- WPE weights patterns by their importance (variance)
- Better for financial data with heteroskedasticity
- Distinguishes "quiet trend" from "volatile trend"

**Mathematical formulation:**
```
Standard permutation entropy:
  H_perm = -∑ p(π) log₂(p(π))

Weighted version:
  1. For each pattern π occurrence, compute weight:
     w_i = σ²(x_i, x_{i+1}, ..., x_{i+m-1})

  2. Weighted probability:
     p_w(π) = ∑(weights for pattern π) / ∑(all weights)

  3. Weighted permutation entropy:
     H_WPE = -∑ p_w(π) log₂(p_w(π))
```

**Regime interpretation:**
- `H_WPE < H_perm`: Large moves are more structured
- `H_WPE > H_perm`: Large moves are more random (risky)
- `H_WPE → 0` with large weights: Volatile trend (strong signal)
- `H_WPE → 1` with large weights: Volatile noise (avoid)

**Implementation complexity:** ~100 LOC
- Build on permutation entropy implementation
- Add variance weighting
- Normalize by total weight

**Integration:**
- Add `weighted_permutation_entropy_10s`, `weighted_permutation_entropy_1m`
- Add `wpe_vs_pe_ratio` (H_WPE / H_perm)
- Ratio > 1.1: Large moves are noisy (avoid)
- Ratio < 0.9: Large moves are structured (trade)

**Reference:** Fadlallah et al. (2013), Physical Review E

---

## Priority 3 Features (High Complexity, Lower Priority)

### 9. Tsallis Entropy

**What it measures:** Generalized entropy for fat-tailed distributions

**Mathematical formulation:**
```
S_q = (1 - ∑ p_i^q) / (q - 1)

Where:
  q → 1: Shannon entropy (limit case)
  q < 1: Emphasizes rare events (tail sensitivity)
  q > 1: Emphasizes common events (core sensitivity)
```

**Why it matters:**
- Financial returns have fat tails
- Shannon entropy may miss tail risk
- Tsallis with q≠1 captures tail behavior
- Theoretical interest for regime detection in extreme events

**Implementation complexity:** ~80 LOC
- Simple formula but needs parameter tuning
- Must experiment with q values (try q=0.5, 1.5, 2.0)

**Integration:**
- Add `tsallis_entropy_q05`, `tsallis_entropy_q15`
- Experimental - evaluate in validation phase

**Reference:** Tsallis (1988), Borland (2002) for finance application

---

### 10. Rényi Entropy

**What it measures:** Generalized entropy family (includes Shannon, min-entropy, collision entropy)

**Mathematical formulation:**
```
H_α = (1 / (1-α)) log₂(∑ p_i^α)

Special cases:
  α = 0: Hartley entropy (log₂(support size))
  α → 1: Shannon entropy (limit)
  α = 2: Collision entropy
  α → ∞: Min-entropy
```

**Why it matters:**
- Provides entropy spectrum
- α=2 (collision entropy) useful for concentration
- Min-entropy useful for worst-case analysis
- Theoretical completeness

**Implementation complexity:** ~60 LOC
- Simple formula
- Compute for α ∈ {0, 1, 2, ∞}

**Integration:**
- Add `collision_entropy` (α=2)
- Add `min_entropy` (α=∞)
- Use for orderbook concentration analysis

**Reference:** Rényi (1961)

---

## Recommended Implementation Sequence

### Phase 0: Quick Wins (Week 1)
**Effort:** ~300 LOC, ~10 hours

1. **Permutation Entropy** (60 LOC, 2 hours)
   - `src/features/permutation_entropy.rs`
   - Add to FeaturesSnapshot: `perm_entropy_10s`, `perm_entropy_1m`, `perm_entropy_5m`

2. **Sample Entropy** (80 LOC, 3 hours)
   - `src/features/sample_entropy.rs`
   - Add to FeaturesSnapshot: `sample_entropy_30s`, `sample_entropy_1m`, `sample_entropy_5m`

3. **Order Book Entropy** (120 LOC, 3 hours)
   - `src/features/orderbook_entropy.rs`
   - Add to FeaturesSnapshot: `ob_entropy`, `ob_bid_entropy`, `ob_ask_entropy`, `ob_entropy_asymmetry`

4. **KL Divergence** (60 LOC, 2 hours)
   - `src/features/kl_divergence.rs`
   - Add to FeaturesSnapshot: `kl_div_tick_1m`, `kl_div_volatility_1m`

**Impact:** High - these four features provide:
- Pattern complexity (Permutation)
- Sequential regularity (Sample)
- Structural liquidity (Order Book)
- Transition detection (KL Divergence)

**Validation:**
- Run validation notebook after implementation
- Check if regime classification improves
- Measure correlation with forward returns

---

### Phase 1: Medium Complexity (Week 2)
**Effort:** ~190 LOC, ~7 hours

5. **Spectral Entropy** (90 LOC, 4 hours)
   - `src/features/spectral_entropy.rs`
   - Requires `rustfft` dependency
   - Add to FeaturesSnapshot: `spectral_entropy_5m`, `dominant_frequency`

6. **Approximate Entropy** (100 LOC, 3 hours)
   - `src/features/approx_entropy.rs`
   - Add to FeaturesSnapshot: `approx_entropy_30s`, `approx_entropy_1m`
   - Use for validation against Sample Entropy

**Impact:** Medium - provides frequency domain analysis and validation

---

### Phase 2: Advanced Features (Week 3-4)
**Effort:** ~300 LOC, ~12 hours

7. **Transfer Entropy** (200 LOC, 8 hours)
   - `src/features/transfer_entropy.rs`
   - Complex probability estimation
   - Add to FeaturesSnapshot: `te_flow_to_price_1m`, `te_vol_to_spread_1m`, `informed_flow_score`

8. **Weighted Permutation Entropy** (100 LOC, 4 hours)
   - `src/features/weighted_perm_entropy.rs`
   - Build on permutation entropy
   - Add to FeaturesSnapshot: `wpe_10s`, `wpe_1m`, `wpe_vs_pe_ratio`

**Impact:** High (Transfer Entropy especially) - detects causal relationships and informed flow

---

### Phase 3: Research Extensions (Optional)
**Effort:** ~140 LOC, ~6 hours

9. **Tsallis Entropy** (80 LOC, 3 hours)
   - `src/features/tsallis_entropy.rs`
   - Experimental
   - Add to FeaturesSnapshot: `tsallis_q05`, `tsallis_q15`

10. **Rényi Entropy** (60 LOC, 3 hours)
    - `src/features/renyi_entropy.rs`
    - Theoretical completeness
    - Add to FeaturesSnapshot: `collision_entropy`, `min_entropy`

**Impact:** Low - primarily for academic completeness and edge cases

---

## Integration with Existing TASKS_1_26.md

### Proposed Changes

1. **Add new Phase 1.5: Advanced Entropy Features** (between current Phase 1 and Phase 2)
   - Insert P0 features (Permutation, Sample, Order Book, KL Divergence)
   - ~10 hours effort, 4 new tasks

2. **Add new Phase 3.5: Complex Entropy Features** (after validation)
   - Insert P1-P2 features (Spectral, ApEn, Transfer, WPE)
   - ~19 hours effort, 4 new tasks

3. **Add new Phase 6: Research Extensions** (optional, post-MVP)
   - Insert P3 features (Tsallis, Rényi)
   - ~6 hours effort, 2 new tasks

4. **Update Phase 2 Regime Classifier** to incorporate new features:
   - Use Permutation + Sample Entropy for trend detection
   - Use Order Book Entropy for liquidity regime
   - Use KL Divergence for transition detection
   - Use Transfer Entropy for informed flow regime

### Updated Feature Count
- Current plan: 19 features (18 new + regime)
- With P0 additions: **30 features** (+11)
- With P1 additions: **34 features** (+4)
- With P2 additions: **38 features** (+4)
- With P3 additions: **42 features** (+4)

---

## Success Metrics

| Feature | Validation Metric | Target |
|---------|-------------------|--------|
| **Permutation Entropy** | Correlation with forward returns | \|r\| > 0.15 |
| **Sample Entropy** | Trend regime win rate improvement | +5% |
| **Order Book Entropy** | MM fill rate prediction accuracy | R² > 0.3 |
| **KL Divergence** | Regime transition early detection | >50% lead time |
| **Spectral Entropy** | Cycle detection accuracy | >70% |
| **Transfer Entropy** | Informed flow detection precision | >60% |

---

## Dependencies

### Rust Crates
```toml
[dependencies]
# Existing
rust_decimal = "1.33"
tokio = { version = "1", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }

# New for advanced entropy
rustfft = "6.1"  # For Spectral Entropy (FFT)
ndarray = "0.15"  # For efficient matrix operations (Transfer Entropy)
statrs = "0.16"  # For statistical distributions
```

### Module Structure
```
src/features/
├── mod.rs
├── entropy.rs                    # Existing: Shannon entropy
├── permutation_entropy.rs        # New: P0
├── sample_entropy.rs             # New: P0
├── orderbook_entropy.rs          # New: P0
├── kl_divergence.rs              # New: P0
├── spectral_entropy.rs           # New: P1
├── approx_entropy.rs             # New: P1
├── transfer_entropy.rs           # New: P2
├── weighted_perm_entropy.rs      # New: P2
├── tsallis_entropy.rs            # New: P3
└── renyi_entropy.rs              # New: P3
```

---

## Validation Strategy

### Phase 0 Validation (After P0 features)
1. Run entropy_validation.ipynb with new features
2. Check correlation matrix (feature redundancy)
3. Test regime classifier with new inputs
4. Measure improvement in Sharpe ratio
5. **Gate decision:** If no improvement, halt P1-P3

### Phase 1 Validation (After P1 features)
1. Test spectral entropy for cycle detection
2. Validate ApEn vs SampEn consistency
3. Update regime classifier if beneficial

### Phase 2 Validation (After P2 features)
1. Test transfer entropy for informed flow detection
2. Backtest with informed flow filter
3. Measure win rate improvement in filtered trades

---

## Risk Mitigation

### Computational Performance
- **Risk:** Complex entropies (Transfer, Sample) are O(N²)
- **Mitigation:**
  - Use spatial indexing (k-d trees) for nearest neighbor search
  - Compute at lower frequency (10-30s vs every tick)
  - Parallelize with Rayon

### Feature Redundancy
- **Risk:** New features may be correlated with existing
- **Mitigation:**
  - Compute correlation matrix during validation
  - Use PCA or feature selection
  - Only keep features with |corr| < 0.8

### Overfitting
- **Risk:** More features = more overfitting potential
- **Mitigation:**
  - Strict walk-forward validation
  - OOS testing on held-out data
  - Regularization in ML models
  - Feature selection based on stability

---

## Conclusion

This analysis identifies **10 advanced entropy features** not in the current TASKS_1_26.md document. Prioritized by implementation complexity and impact, the recommended sequence is:

1. **Phase 0 (P0):** Permutation, Sample, Order Book, KL Divergence (~10 hours, high impact)
2. **Phase 1 (P1):** Spectral, Approximate (~7 hours, medium impact)
3. **Phase 2 (P2):** Transfer, Weighted Permutation (~12 hours, high impact)
4. **Phase 3 (P3):** Tsallis, Rényi (~6 hours, low priority - research only)

**Total effort:** ~35 hours for P0-P2, ~41 hours including P3

**Expected outcome:** Significantly improved regime classification with robust, academically-validated entropy measures, leading to better risk-adjusted returns in the MARS trading system.

---

*Document Version: 1.0*
*Created: 2026-01-14*
*Next Review: After Phase 0 implementation*

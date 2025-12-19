# Conditional Momentum Research Framework

**Document Version:** 1.0
**Created:** December 18, 2025
**Purpose:** Define research methodology for testing conditional momentum hypotheses

---

## Core Research Question

> **Given that price moved X% in T minutes, what is the probability of a subsequent Y% move in the same direction within the next T' minutes?**

Notation: `P(Y | X, T, T')` = Probability of Y% continuation given X% move over T minutes, measured over next T' minutes

---

## Mathematical Framework

### 1. Price Move Definition

```
Initial observation window: [t₀, t₀ + T]
Continuation window:        [t₀ + T, t₀ + T + T']

X = (P(t₀ + T) - P(t₀)) / P(t₀) × 100    # Percentage move in observation window
Y = (P(t₀ + T + T') - P(t₀ + T)) / P(t₀ + T) × 100  # Percentage move in continuation window
```

### 2. Conditional Probability Table Structure

```
P(continuation | trigger) = P(Y ≥ threshold | X ∈ bucket, T, T')

Example table for T=5min, T'=5min:
┌─────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│ X (trigger) │ P(Y > 0.1%)  │ P(Y > 0.2%)  │ P(Y > 0.3%)  │ P(Y > 0.5%)  │
├─────────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│ 0.1% - 0.2% │     ???      │     ???      │     ???      │     ???      │
│ 0.2% - 0.3% │     ???      │     ???      │     ???      │     ???      │
│ 0.3% - 0.5% │     ???      │     ???      │     ???      │     ???      │
│ 0.5% - 1.0% │     ???      │     ???      │     ???      │     ???      │
│ > 1.0%      │     ???      │     ???      │     ???      │     ???      │
└─────────────┴──────────────┴──────────────┴──────────────┴──────────────┘
```

### 3. Key Metrics Per Cell

For each (X_bucket, Y_threshold, T, T') combination:

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| `n_triggers` | Count of X events | Sample size |
| `n_continuations` | Count where Y > threshold | Success count |
| `p_continuation` | n_continuations / n_triggers | Raw probability |
| `p_reversal` | Count where Y < -threshold / n_triggers | Reversal probability |
| `expected_Y` | Mean(Y \| X) | Expected continuation magnitude |
| `std_Y` | Std(Y \| X) | Uncertainty in continuation |
| `sharpe_Y` | expected_Y / std_Y | Risk-adjusted continuation |
| `edge` | p_continuation - 0.5 | Edge over random |

---

## Research Dimensions

### Dimension 1: Trigger Magnitude (X)

Discretize initial move into buckets:

```rust
pub enum TriggerMagnitude {
    Tiny,      // 0.05% - 0.10%
    Small,     // 0.10% - 0.20%
    Medium,    // 0.20% - 0.50%
    Large,     // 0.50% - 1.00%
    VeryLarge, // > 1.00%
}
```

**Hypothesis:** Larger triggers have higher continuation probability but lower frequency.

### Dimension 2: Trigger Duration (T)

Test multiple observation windows:

```rust
pub enum TriggerDuration {
    T1m,   // 1 minute
    T2m,   // 2 minutes
    T5m,   // 5 minutes
    T10m,  // 10 minutes
    T15m,  // 15 minutes
    T30m,  // 30 minutes
}
```

**Hypothesis:** Slower moves (larger T for same X) indicate more persistent trends.

### Dimension 3: Continuation Window (T')

Test multiple prediction horizons:

```rust
pub enum ContinuationWindow {
    T1m,   // Next 1 minute
    T2m,   // Next 2 minutes
    T5m,   // Next 5 minutes
    T10m,  // Next 10 minutes
}
```

**Hypothesis:** Shorter T' has higher predictability but smaller expected Y.

### Dimension 4: Continuation Threshold (Y)

Define what counts as "continuation":

```rust
pub enum ContinuationThreshold {
    Y5bps,   // 0.05%
    Y10bps,  // 0.10%
    Y20bps,  // 0.20%
    Y30bps,  // 0.30%
    Y50bps,  // 0.50%
}
```

**Hypothesis:** Higher thresholds have lower probability but better risk/reward.

### Dimension 5: Move Quality (Additional Filters)

Not just magnitude, but HOW the move happened:

```rust
pub struct TriggerQuality {
    pub monotonicity: f64,     // % of ticks in direction (0.5 = random, 1.0 = perfect trend)
    pub volume_profile: VolumeProfile,  // Increasing, Decreasing, Flat
    pub entropy: f64,          // Tick entropy during move
    pub speed_consistency: f64, // Was move steady or spikey?
}

pub enum VolumeProfile {
    Increasing,  // Volume grew during move
    Decreasing,  // Volume faded during move
    Flat,        // Consistent volume
}
```

**Hypothesis:** High monotonicity + increasing volume = higher continuation probability.

---

## Data Structures

### TriggerEvent

```rust
/// A detected price movement that may predict continuation
#[derive(Debug, Clone)]
pub struct TriggerEvent {
    // Identification
    pub id: u64,
    pub symbol: String,
    pub trigger_start: DateTime<Utc>,
    pub trigger_end: DateTime<Utc>,

    // Trigger characteristics
    pub x_pct: f64,              // Percentage move (signed)
    pub t_minutes: f64,          // Duration of trigger window
    pub direction: Direction,    // Up or Down

    // Quality metrics
    pub monotonicity: f64,       // 0.0 to 1.0
    pub tick_count: usize,       // Number of ticks in window
    pub volume: f64,             // Total volume during trigger
    pub entropy: f64,            // Tick entropy

    // Price levels
    pub price_start: f64,
    pub price_end: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}
```

### ContinuationResult

```rust
/// Observed outcome after a trigger event
#[derive(Debug, Clone)]
pub struct ContinuationResult {
    pub trigger_id: u64,
    pub continuation_window_minutes: f64,

    // Outcome
    pub y_pct: f64,              // Actual percentage move (signed)
    pub continued: bool,         // Did it continue in same direction?
    pub reversed: bool,          // Did it reverse significantly?

    // Additional context
    pub max_favorable: f64,      // Best point during window
    pub max_adverse: f64,        // Worst point during window
    pub time_to_target: Option<f64>,  // Minutes to reach threshold (if any)
}
```

### ConditionalProbabilityTable

```rust
/// Aggregated statistics for a specific (X, T, T', Y) combination
#[derive(Debug, Clone)]
pub struct ConditionalStats {
    // Bucket definition
    pub x_min_pct: f64,
    pub x_max_pct: f64,
    pub t_minutes: f64,
    pub t_prime_minutes: f64,
    pub y_threshold_pct: f64,

    // Sample statistics
    pub n_triggers: usize,
    pub n_continuations: usize,
    pub n_reversals: usize,

    // Probability estimates
    pub p_continuation: f64,     // P(Y > threshold | X, T, T')
    pub p_reversal: f64,         // P(Y < -threshold | X, T, T')
    pub p_neutral: f64,          // P(-threshold < Y < threshold)

    // Magnitude statistics
    pub expected_y: f64,         // E[Y | X, T, T']
    pub std_y: f64,              // Std[Y | X, T, T']
    pub median_y: f64,
    pub percentile_25: f64,
    pub percentile_75: f64,

    // Risk metrics
    pub sharpe: f64,             // expected_y / std_y
    pub edge: f64,               // p_continuation - 0.5
    pub expected_pnl_bps: f64,   // Expected P&L if trading this signal

    // Confidence
    pub confidence_interval_95: (f64, f64),  // 95% CI for p_continuation
}

/// Full probability table across all dimensions
pub struct ConditionalProbabilityTable {
    pub symbol: String,
    pub data_start: DateTime<Utc>,
    pub data_end: DateTime<Utc>,
    pub total_triggers: usize,

    // Nested map: X_bucket -> T -> T' -> Y_threshold -> Stats
    pub stats: HashMap<TriggerBucket, HashMap<Duration, HashMap<Duration, HashMap<ThresholdBucket, ConditionalStats>>>>,
}
```

---

## Research Pipeline

### Stage 1: Data Preparation

```
Parquet Files → Load Features → Extract Mid-Price Series → Normalize
```

```rust
pub struct PriceSeriesLoader {
    pub fn load_from_parquet(path: &Path) -> Result<Vec<PricePoint>>;
    pub fn merge_files(paths: &[Path]) -> Result<Vec<PricePoint>>;
    pub fn validate_continuity(series: &[PricePoint]) -> ValidationResult;
}

pub struct PricePoint {
    pub timestamp: DateTime<Utc>,
    pub mid_price: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: Option<f64>,
}
```

### Stage 2: Trigger Detection

```
Price Series → Sliding Window → Detect X% Moves → Create TriggerEvents
```

```rust
pub struct TriggerDetector {
    config: TriggerConfig,
}

pub struct TriggerConfig {
    pub observation_windows: Vec<Duration>,     // T values to test
    pub magnitude_thresholds: Vec<f64>,         // Minimum X to count as trigger
    pub min_monotonicity: Option<f64>,          // Quality filter
    pub max_entropy: Option<f64>,               // Quality filter
}

impl TriggerDetector {
    pub fn detect_triggers(&self, prices: &[PricePoint]) -> Vec<TriggerEvent>;
}
```

### Stage 3: Continuation Measurement

```
TriggerEvents → For Each Trigger → Measure Outcome Over T' → Create ContinuationResults
```

```rust
pub struct ContinuationMeasurer {
    config: ContinuationConfig,
}

pub struct ContinuationConfig {
    pub continuation_windows: Vec<Duration>,    // T' values to test
    pub thresholds: Vec<f64>,                   // Y thresholds to check
}

impl ContinuationMeasurer {
    pub fn measure_continuations(
        &self,
        triggers: &[TriggerEvent],
        prices: &[PricePoint]
    ) -> Vec<(TriggerEvent, ContinuationResult)>;
}
```

### Stage 4: Statistical Aggregation

```
(Trigger, Continuation) Pairs → Group by Buckets → Compute Statistics → Build Table
```

```rust
pub struct StatisticalAggregator;

impl StatisticalAggregator {
    pub fn build_probability_table(
        pairs: &[(TriggerEvent, ContinuationResult)],
        config: &AggregationConfig,
    ) -> ConditionalProbabilityTable;

    pub fn compute_confidence_intervals(stats: &mut ConditionalStats);
    pub fn apply_multiple_testing_correction(table: &mut ConditionalProbabilityTable);
}
```

### Stage 5: Analysis & Visualization

```
Probability Table → Identify Significant Edges → Visualize → Report
```

```rust
pub struct ResearchAnalyzer;

impl ResearchAnalyzer {
    /// Find statistically significant edges
    pub fn find_significant_signals(
        table: &ConditionalProbabilityTable,
        min_edge: f64,           // Minimum edge over 0.5
        min_samples: usize,      // Minimum sample size
        max_p_value: f64,        // Statistical significance
    ) -> Vec<SignificantSignal>;

    /// Generate report
    pub fn generate_report(
        table: &ConditionalProbabilityTable,
        signals: &[SignificantSignal],
    ) -> ResearchReport;
}

pub struct SignificantSignal {
    pub trigger_bucket: TriggerBucket,
    pub t_minutes: f64,
    pub t_prime_minutes: f64,
    pub y_threshold: f64,
    pub p_continuation: f64,
    pub edge: f64,
    pub n_samples: usize,
    pub p_value: f64,
    pub expected_pnl_bps: f64,
}
```

---

## Key Hypotheses to Test

### H1: Momentum Persistence Exists

```
H0: P(Y > 0 | X > 0) = 0.5  (no momentum)
H1: P(Y > 0 | X > 0) > 0.5  (momentum exists)

Test: One-sided binomial test for each (X, T, T') combination
```

### H2: Larger Moves Have Higher Continuation

```
H0: P(continuation | X_large) = P(continuation | X_small)
H1: P(continuation | X_large) > P(continuation | X_small)

Test: Chi-squared test comparing buckets
```

### H3: Slower Moves Are More Persistent

```
H0: P(continuation | X, T_slow) = P(continuation | X, T_fast)
H1: P(continuation | X, T_slow) > P(continuation | X, T_fast)

Test: Compare same X% move over different T windows
```

### H4: High Quality Moves Are More Predictive

```
H0: P(continuation | high_monotonicity) = P(continuation | low_monotonicity)
H1: P(continuation | high_monotonicity) > P(continuation | low_monotonicity)

Test: Compare within same X bucket, split by quality
```

### H5: Edge Decays with Time

```
H0: Edge is constant across T' values
H1: Edge decreases as T' increases

Test: Regression of edge vs T' for fixed (X, T)
```

---

## Expected Output Format

### Summary Statistics

```
CONDITIONAL MOMENTUM RESEARCH REPORT
====================================
Symbol: BTCUSDT
Period: 2025-10-16 to 2025-12-17 (62 days)
Total Price Points: 5,234,567
Total Triggers Detected: 12,456

SIGNIFICANT SIGNALS FOUND: 7

Signal 1: [HIGH CONFIDENCE]
  Trigger:  0.20% - 0.30% move over 5 minutes
  Predict:  0.10% continuation in next 5 minutes
  P(cont):  0.583 ± 0.024 (95% CI)
  Edge:     +8.3%
  Samples:  1,234
  p-value:  0.0003
  Expected: +3.2 bps per trade (after spread)

Signal 2: [MEDIUM CONFIDENCE]
  ...
```

### Probability Heatmap

```
P(Y > 0.1% | X, T=5min, T'=5min)

           │ Tiny  │ Small │ Medium │ Large │ V.Large │
───────────┼───────┼───────┼────────┼───────┼─────────┤
 Up Move   │ 0.51  │ 0.53  │ 0.58*  │ 0.62* │  0.67*  │
 Down Move │ 0.51  │ 0.54  │ 0.57*  │ 0.61* │  0.64*  │

* = statistically significant (p < 0.05)
```

### Edge Decay Curve

```
Edge vs Continuation Window (for X=0.3%, T=5min)

Edge │
0.10 │    *
0.08 │      *
0.06 │        *
0.04 │          *
0.02 │            *
0.00 │──────────────────
     │  1m  2m  5m  10m   T' (continuation window)
```

---

## Integration with Trading

### From Research to Strategy

```rust
/// Use research results to make trading decisions
pub struct MOM_ConditionalStrategy {
    probability_table: ConditionalProbabilityTable,
    min_edge: f64,
    min_samples: usize,

    // Current state
    active_trigger: Option<TriggerEvent>,
}

impl TradingStrategy for MOM_ConditionalStrategy {
    fn on_features(&mut self, features: &FeaturesSnapshot) -> StrategyDecision {
        // 1. Check if we're in a trigger condition
        if let Some(trigger) = self.detect_trigger(features) {
            // 2. Look up continuation probability
            let stats = self.probability_table.lookup(&trigger);

            // 3. Check if edge is sufficient
            if stats.edge > self.min_edge && stats.n_triggers > self.min_samples {
                // 4. Calculate position size based on edge
                let kelly_fraction = self.calculate_kelly(stats);

                return StrategyDecision::Enter {
                    side: trigger.direction.into(),
                    size: kelly_fraction * self.max_position,
                    tp_bps: stats.percentile_75,  // Take profit at 75th percentile
                    sl_bps: stats.percentile_25.abs(),  // Stop at 25th percentile
                };
            }
        }

        StrategyDecision::Hold
    }
}
```

### Kelly Criterion for Position Sizing

```rust
/// Calculate optimal position size given edge
fn calculate_kelly(stats: &ConditionalStats) -> f64 {
    let p = stats.p_continuation;
    let q = 1.0 - p;
    let b = stats.expected_y.abs() / stats.std_y;  // Win/loss ratio proxy

    // Kelly: f* = (bp - q) / b
    let kelly = (b * p - q) / b;

    // Half-Kelly for safety
    (kelly * 0.5).max(0.0).min(0.25)
}
```

---

## Implementation Checklist

### Phase R.4: Conditional Model (from REQUIREMENTS_V0.2.md)

| Task | Description | Status |
|------|-------------|--------|
| R.4.1 | Define TriggerEvent, ContinuationResult structs | TODO |
| R.4.2 | Implement TriggerDetector | TODO |
| R.4.3 | Implement ContinuationMeasurer | TODO |
| R.4.4 | Implement StatisticalAggregator | TODO |
| R.4.5 | Implement ConditionalProbabilityTable | TODO |
| R.4.6 | Add statistical tests (binomial, chi-squared) | TODO |
| R.4.7 | Implement ResearchAnalyzer | TODO |
| R.4.8 | Create CLI tool for running analysis | TODO |
| R.4.9 | Add visualization/reporting | TODO |
| R.4.10 | Integrate with MOM_* strategies | TODO |

---

## Configuration Defaults

```rust
pub const DEFAULT_TRIGGER_CONFIG: TriggerConfig = TriggerConfig {
    observation_windows: vec![
        Duration::minutes(1),
        Duration::minutes(2),
        Duration::minutes(5),
        Duration::minutes(10),
    ],
    magnitude_thresholds: vec![0.05, 0.10, 0.20, 0.30, 0.50, 1.00],
    min_monotonicity: Some(0.6),
    max_entropy: None,
};

pub const DEFAULT_CONTINUATION_CONFIG: ContinuationConfig = ContinuationConfig {
    continuation_windows: vec![
        Duration::minutes(1),
        Duration::minutes(2),
        Duration::minutes(5),
        Duration::minutes(10),
    ],
    thresholds: vec![0.05, 0.10, 0.20, 0.30, 0.50],
};

pub const DEFAULT_SIGNIFICANCE: SignificanceConfig = SignificanceConfig {
    min_edge: 0.05,          // 5% edge over random
    min_samples: 100,        // At least 100 observations
    max_p_value: 0.05,       // 95% confidence
    apply_bonferroni: true,  // Correct for multiple testing
};
```

---

## Summary

This research framework answers the question:

> **"If price moved X% over T minutes, what happens next?"**

By systematically:
1. **Detecting** trigger events (significant price moves)
2. **Measuring** what happens after each trigger
3. **Aggregating** results into probability tables
4. **Testing** for statistical significance
5. **Integrating** findings into trading strategies

The key insight is that not all price moves are equal - **magnitude, duration, and quality** all affect continuation probability. By building empirical probability tables, we can identify which signals have genuine predictive power and size positions accordingly.

---

*Document maintained by: Development Team*
*Last updated: December 18, 2025*

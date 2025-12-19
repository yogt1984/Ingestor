# Breakout Detector: Basis Pattern Matching Framework

**Document Version:** 1.0
**Created:** December 18, 2025
**Purpose:** Define a pattern-based breakout detection system using convolution/dot product with learned basis patterns

---

## Core Concept

> **Breakout Score = Σ (wᵢ · ⟨pattern_i, market_window⟩)**
>
> Where ⟨·,·⟩ is inner product (dot product) between a learned basis pattern and the current market window

The idea: Historical breakout patterns leave "signatures" in price/volume data. By learning these signatures as basis vectors, we can detect similar setups in real-time.

---

## Mathematical Framework

### 1. Market State Vector

At each point in time, we construct a **market state vector** from recent candles:

```
For N candles (e.g., 15 x 5-minute = 75 minutes of history):

Price features per candle:
  - returns[i] = (close[i] - close[i-1]) / close[i-1]
  - range[i] = (high[i] - low[i]) / close[i]
  - body[i] = (close[i] - open[i]) / close[i]
  - upper_wick[i] = (high[i] - max(open[i], close[i])) / close[i]
  - lower_wick[i] = (min(open[i], close[i]) - low[i]) / close[i]

Volume features per candle:
  - vol_normalized[i] = volume[i] / mean(volume[i-20:i])
  - vol_delta[i] = (volume[i] - volume[i-1]) / volume[i-1]

Derived features:
  - momentum[i] = sum(returns[i-k:i]) for k in [3, 5, 10]
  - volatility[i] = std(returns[i-10:i])
```

**Market State Vector:**
```
x(t) ∈ ℝᵈ where d = N × features_per_candle

Example: 15 candles × 10 features = 150-dimensional vector
```

### 2. Basis Patterns (Templates)

Each basis pattern represents a "canonical" breakout setup:

```
Basis Pattern b_k ∈ ℝᵈ (same dimension as market state)

Pattern types:
  - b_bullish_breakout: Strong up move after consolidation
  - b_bearish_breakdown: Strong down move after consolidation
  - b_volume_spike_up: Volume surge with price increase
  - b_volume_spike_down: Volume surge with price decrease
  - b_squeeze_release: Low volatility → high volatility expansion
  - b_continuation: Trend continuation after pullback
  - b_reversal: Trend exhaustion and reversal
```

### 3. Breakout Score Computation

```
For basis set B = {b₁, b₂, ..., bₖ} with weights W = {w₁, w₂, ..., wₖ}:

score(t) = Σᵢ wᵢ · similarity(x(t), bᵢ)

Where similarity can be:
  - Dot product: ⟨x, b⟩ = xᵀb
  - Cosine similarity: ⟨x, b⟩ / (||x|| · ||b||)
  - Normalized correlation: corr(x, b)
```

### 4. Continuous Tracing

The system maintains a **rolling breakout score vector**:

```
S(t) = [score_bullish(t), score_bearish(t), score_squeeze(t), ...]

At each new candle:
  1. Update market state vector x(t)
  2. Compute S(t) = B · x(t)  (matrix-vector multiply)
  3. Apply thresholds to generate signals
```

---

## Basis Learning Methods

### Method 1: Empirical Averaging (Simple)

Extract patterns from historical breakouts and average them:

```rust
pub struct EmpiricalBasisLearner {
    lookback_candles: usize,      // N candles for pattern
    lookahead_candles: usize,     // How far ahead to measure outcome
    breakout_threshold_pct: f64,  // What counts as "breakout"
}

impl EmpiricalBasisLearner {
    /// Learn basis from historical data
    pub fn learn(&self, candles: &[Candle]) -> Vec<BasisPattern> {
        // 1. Find all breakout events (price moved > threshold in lookahead)
        let breakouts = self.find_breakout_events(candles);

        // 2. Extract market state before each breakout
        let patterns: Vec<MarketState> = breakouts.iter()
            .map(|b| self.extract_state_before(candles, b))
            .collect();

        // 3. Average to get canonical pattern
        let bullish_basis = self.average_patterns(&patterns.filter(bullish));
        let bearish_basis = self.average_patterns(&patterns.filter(bearish));

        vec![bullish_basis, bearish_basis, ...]
    }
}
```

### Method 2: PCA-Based Basis (Orthogonal)

Use Principal Component Analysis to find orthogonal patterns:

```rust
pub struct PCABasisLearner {
    n_components: usize,  // Number of principal components to keep
}

impl PCABasisLearner {
    pub fn learn(&self, successful_setups: &[MarketState]) -> Vec<BasisPattern> {
        // 1. Stack all successful setup patterns into matrix X
        let X = self.stack_patterns(successful_setups);

        // 2. Compute PCA
        let (eigenvalues, eigenvectors) = pca(&X);

        // 3. Top-k eigenvectors become basis patterns
        eigenvectors.iter()
            .take(self.n_components)
            .map(|v| BasisPattern::from_vector(v))
            .collect()
    }
}
```

### Method 3: Clustering-Based Basis (Diverse)

Find distinct pattern clusters:

```rust
pub struct ClusterBasisLearner {
    n_clusters: usize,
    algorithm: ClusterAlgorithm,  // KMeans, DBSCAN, etc.
}

impl ClusterBasisLearner {
    pub fn learn(&self, successful_setups: &[MarketState]) -> Vec<BasisPattern> {
        // 1. Cluster similar patterns together
        let clusters = self.algorithm.fit(successful_setups);

        // 2. Centroid of each cluster becomes a basis pattern
        clusters.iter()
            .map(|c| c.centroid())
            .collect()
    }
}
```

### Method 4: Discriminative Learning (Supervised)

Learn patterns that distinguish breakouts from non-breakouts:

```rust
pub struct DiscriminativeBasisLearner {
    regularization: f64,
}

impl DiscriminativeBasisLearner {
    pub fn learn(
        &self,
        breakout_setups: &[MarketState],    // Positive examples
        non_breakout_setups: &[MarketState], // Negative examples
    ) -> Vec<BasisPattern> {
        // Use Fisher's Linear Discriminant or similar
        // Find directions that maximize separation between classes

        let mean_breakout = mean(breakout_setups);
        let mean_non_breakout = mean(non_breakout_setups);

        // Direction of maximum separation
        let discriminant = (mean_breakout - mean_non_breakout).normalize();

        vec![BasisPattern::from_vector(&discriminant)]
    }
}
```

---

## Data Structures

### Candle

```rust
#[derive(Debug, Clone, Copy)]
pub struct Candle {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl Candle {
    /// Convert to feature vector (single candle)
    pub fn to_features(&self, prev_candle: Option<&Candle>, vol_mean: f64) -> CandleFeatures {
        CandleFeatures {
            returns: prev_candle.map(|p| (self.close - p.close) / p.close).unwrap_or(0.0),
            range: (self.high - self.low) / self.close,
            body: (self.close - self.open) / self.close,
            upper_wick: (self.high - self.close.max(self.open)) / self.close,
            lower_wick: (self.open.min(self.close) - self.low) / self.close,
            vol_normalized: self.volume / vol_mean,
            vol_delta: prev_candle.map(|p| (self.volume - p.volume) / p.volume).unwrap_or(0.0),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CandleFeatures {
    pub returns: f64,
    pub range: f64,
    pub body: f64,
    pub upper_wick: f64,
    pub lower_wick: f64,
    pub vol_normalized: f64,
    pub vol_delta: f64,
}

impl CandleFeatures {
    pub const DIM: usize = 7;

    pub fn to_array(&self) -> [f64; Self::DIM] {
        [self.returns, self.range, self.body, self.upper_wick,
         self.lower_wick, self.vol_normalized, self.vol_delta]
    }
}
```

### MarketState (Window of Candles)

```rust
/// Market state vector constructed from N candles
#[derive(Debug, Clone)]
pub struct MarketState {
    pub timestamp: DateTime<Utc>,
    pub candle_features: Vec<CandleFeatures>,  // N candles
    pub derived_features: DerivedFeatures,

    // Flattened vector representation
    vector: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct DerivedFeatures {
    pub momentum_3: f64,
    pub momentum_5: f64,
    pub momentum_10: f64,
    pub volatility_10: f64,
    pub volume_trend: f64,  // Linear regression slope of volume
    pub price_trend: f64,   // Linear regression slope of price
}

impl MarketState {
    pub const CANDLE_WINDOW: usize = 15;  // 15 candles
    pub const FEATURE_DIM: usize = CandleFeatures::DIM * Self::CANDLE_WINDOW + 6;  // + derived

    /// Construct market state from candle window
    pub fn from_candles(candles: &[Candle]) -> Self {
        assert!(candles.len() >= Self::CANDLE_WINDOW);

        let recent = &candles[candles.len() - Self::CANDLE_WINDOW..];
        let vol_mean = recent.iter().map(|c| c.volume).sum::<f64>() / recent.len() as f64;

        let candle_features: Vec<CandleFeatures> = recent.windows(2)
            .map(|w| w[1].to_features(Some(&w[0]), vol_mean))
            .collect();

        let derived = Self::compute_derived(&candle_features);
        let vector = Self::flatten(&candle_features, &derived);

        Self {
            timestamp: recent.last().unwrap().timestamp,
            candle_features,
            derived_features: derived,
            vector,
        }
    }

    /// Get as normalized vector for dot product
    pub fn as_vector(&self) -> &[f64] {
        &self.vector
    }

    /// Normalized vector (unit length)
    pub fn as_unit_vector(&self) -> Vec<f64> {
        let norm: f64 = self.vector.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 1e-10 {
            self.vector.iter().map(|x| x / norm).collect()
        } else {
            self.vector.clone()
        }
    }

    fn compute_derived(features: &[CandleFeatures]) -> DerivedFeatures {
        let returns: Vec<f64> = features.iter().map(|f| f.returns).collect();

        DerivedFeatures {
            momentum_3: returns.iter().rev().take(3).sum(),
            momentum_5: returns.iter().rev().take(5).sum(),
            momentum_10: returns.iter().rev().take(10).sum(),
            volatility_10: std_dev(&returns[returns.len().saturating_sub(10)..]),
            volume_trend: linear_slope(&features.iter().map(|f| f.vol_normalized).collect::<Vec<_>>()),
            price_trend: linear_slope(&returns),
        }
    }

    fn flatten(candle_features: &[CandleFeatures], derived: &DerivedFeatures) -> Vec<f64> {
        let mut v = Vec::with_capacity(Self::FEATURE_DIM);

        for f in candle_features {
            v.extend_from_slice(&f.to_array());
        }

        v.push(derived.momentum_3);
        v.push(derived.momentum_5);
        v.push(derived.momentum_10);
        v.push(derived.volatility_10);
        v.push(derived.volume_trend);
        v.push(derived.price_trend);

        v
    }
}
```

### BasisPattern

```rust
/// A learned breakout pattern template
#[derive(Debug, Clone)]
pub struct BasisPattern {
    pub name: String,
    pub pattern_type: PatternType,

    // The pattern vector (same dimension as MarketState)
    vector: Vec<f64>,

    // Metadata
    pub learned_from_n_samples: usize,
    pub expected_continuation_pct: f64,
    pub historical_accuracy: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternType {
    BullishBreakout,
    BearishBreakdown,
    VolumeSpikeUp,
    VolumeSpikeDown,
    SqueezeRelease,
    TrendContinuation,
    Reversal,
    Custom(u32),
}

impl BasisPattern {
    /// Create from learned vector
    pub fn from_vector(name: &str, pattern_type: PatternType, vector: Vec<f64>) -> Self {
        Self {
            name: name.to_string(),
            pattern_type,
            vector,
            learned_from_n_samples: 0,
            expected_continuation_pct: 0.0,
            historical_accuracy: 0.0,
        }
    }

    /// Normalize to unit length
    pub fn normalize(&mut self) {
        let norm: f64 = self.vector.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 1e-10 {
            for x in &mut self.vector {
                *x /= norm;
            }
        }
    }

    /// Dot product with market state
    pub fn dot(&self, state: &MarketState) -> f64 {
        self.vector.iter()
            .zip(state.as_vector())
            .map(|(a, b)| a * b)
            .sum()
    }

    /// Cosine similarity with market state
    pub fn cosine_similarity(&self, state: &MarketState) -> f64 {
        let dot = self.dot(state);
        let norm_self: f64 = self.vector.iter().map(|x| x * x).sum::<f64>().sqrt();
        let norm_state: f64 = state.as_vector().iter().map(|x| x * x).sum::<f64>().sqrt();

        if norm_self > 1e-10 && norm_state > 1e-10 {
            dot / (norm_self * norm_state)
        } else {
            0.0
        }
    }
}
```

### BasisSet (Collection of Patterns)

```rust
/// A set of basis patterns for breakout detection
#[derive(Debug, Clone)]
pub struct BasisSet {
    pub name: String,
    pub patterns: Vec<BasisPattern>,
    pub weights: Vec<f64>,  // Weight for each pattern in combined score

    // Configuration
    pub similarity_method: SimilarityMethod,
}

#[derive(Debug, Clone, Copy)]
pub enum SimilarityMethod {
    DotProduct,
    CosineSimilarity,
    Correlation,
}

impl BasisSet {
    /// Create new basis set
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            patterns: Vec::new(),
            weights: Vec::new(),
            similarity_method: SimilarityMethod::CosineSimilarity,
        }
    }

    /// Add a pattern with weight
    pub fn add_pattern(&mut self, pattern: BasisPattern, weight: f64) {
        self.patterns.push(pattern);
        self.weights.push(weight);
    }

    /// Compute all pattern scores for a market state
    pub fn compute_scores(&self, state: &MarketState) -> BreakoutScores {
        let individual_scores: Vec<f64> = self.patterns.iter()
            .map(|p| self.compute_similarity(p, state))
            .collect();

        let weighted_sum: f64 = individual_scores.iter()
            .zip(&self.weights)
            .map(|(s, w)| s * w)
            .sum();

        let bullish_score = self.aggregate_by_type(&individual_scores, PatternType::BullishBreakout);
        let bearish_score = self.aggregate_by_type(&individual_scores, PatternType::BearishBreakdown);

        BreakoutScores {
            timestamp: state.timestamp,
            individual_scores,
            weighted_composite: weighted_sum,
            bullish_score,
            bearish_score,
            strongest_pattern: self.find_strongest(&individual_scores),
        }
    }

    fn compute_similarity(&self, pattern: &BasisPattern, state: &MarketState) -> f64 {
        match self.similarity_method {
            SimilarityMethod::DotProduct => pattern.dot(state),
            SimilarityMethod::CosineSimilarity => pattern.cosine_similarity(state),
            SimilarityMethod::Correlation => self.correlation(pattern, state),
        }
    }

    fn correlation(&self, pattern: &BasisPattern, state: &MarketState) -> f64 {
        let p = &pattern.vector;
        let s = state.as_vector();

        let mean_p: f64 = p.iter().sum::<f64>() / p.len() as f64;
        let mean_s: f64 = s.iter().sum::<f64>() / s.len() as f64;

        let cov: f64 = p.iter().zip(s).map(|(a, b)| (a - mean_p) * (b - mean_s)).sum();
        let std_p: f64 = p.iter().map(|x| (x - mean_p).powi(2)).sum::<f64>().sqrt();
        let std_s: f64 = s.iter().map(|x| (x - mean_s).powi(2)).sum::<f64>().sqrt();

        if std_p > 1e-10 && std_s > 1e-10 {
            cov / (std_p * std_s)
        } else {
            0.0
        }
    }

    fn aggregate_by_type(&self, scores: &[f64], pattern_type: PatternType) -> f64 {
        scores.iter()
            .zip(&self.patterns)
            .filter(|(_, p)| p.pattern_type == pattern_type)
            .map(|(s, _)| *s)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0)
    }

    fn find_strongest(&self, scores: &[f64]) -> Option<(usize, f64)> {
        scores.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(i, s)| (i, *s))
    }
}
```

### BreakoutScores

```rust
/// Output of breakout detection at a point in time
#[derive(Debug, Clone)]
pub struct BreakoutScores {
    pub timestamp: DateTime<Utc>,

    // Individual pattern scores
    pub individual_scores: Vec<f64>,

    // Aggregated scores
    pub weighted_composite: f64,
    pub bullish_score: f64,
    pub bearish_score: f64,

    // Best match
    pub strongest_pattern: Option<(usize, f64)>,  // (pattern_index, score)
}

impl BreakoutScores {
    /// Check if any score exceeds threshold
    pub fn has_signal(&self, threshold: f64) -> bool {
        self.bullish_score > threshold || self.bearish_score > threshold
    }

    /// Get signal direction if above threshold
    pub fn get_signal(&self, threshold: f64) -> Option<BreakoutSignal> {
        if self.bullish_score > threshold && self.bullish_score > self.bearish_score {
            Some(BreakoutSignal::Bullish(self.bullish_score))
        } else if self.bearish_score > threshold {
            Some(BreakoutSignal::Bearish(self.bearish_score))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BreakoutSignal {
    Bullish(f64),
    Bearish(f64),
}
```

---

## Continuous Tracing System

### BreakoutDetector (Real-Time)

```rust
/// Real-time breakout detector that continuously traces pattern scores
pub struct BreakoutDetector {
    // Configuration
    config: BreakoutConfig,

    // Learned basis patterns
    basis_set: BasisSet,

    // State
    candle_buffer: VecDeque<Candle>,
    score_history: VecDeque<BreakoutScores>,

    // Statistics
    stats: BreakoutStats,
}

#[derive(Debug, Clone)]
pub struct BreakoutConfig {
    pub candle_window: usize,       // How many candles for market state (default: 15)
    pub score_history_len: usize,   // How many scores to keep (default: 100)
    pub signal_threshold: f64,      // Score threshold for signal (default: 0.7)
    pub confirmation_candles: usize, // How many candles above threshold (default: 2)
}

impl Default for BreakoutConfig {
    fn default() -> Self {
        Self {
            candle_window: 15,
            score_history_len: 100,
            signal_threshold: 0.7,
            confirmation_candles: 2,
        }
    }
}

impl BreakoutDetector {
    /// Create new detector with learned basis
    pub fn new(config: BreakoutConfig, basis_set: BasisSet) -> Self {
        Self {
            config,
            basis_set,
            candle_buffer: VecDeque::with_capacity(config.candle_window + 10),
            score_history: VecDeque::with_capacity(config.score_history_len),
            stats: BreakoutStats::default(),
        }
    }

    /// Process new candle and return updated scores
    pub fn on_candle(&mut self, candle: Candle) -> Option<BreakoutScores> {
        // 1. Add candle to buffer
        self.candle_buffer.push_back(candle);
        if self.candle_buffer.len() > self.config.candle_window + 10 {
            self.candle_buffer.pop_front();
        }

        // 2. Check if we have enough candles
        if self.candle_buffer.len() < self.config.candle_window {
            return None;
        }

        // 3. Build market state
        let candles: Vec<Candle> = self.candle_buffer.iter().cloned().collect();
        let state = MarketState::from_candles(&candles);

        // 4. Compute scores
        let scores = self.basis_set.compute_scores(&state);

        // 5. Update history
        self.score_history.push_back(scores.clone());
        if self.score_history.len() > self.config.score_history_len {
            self.score_history.pop_front();
        }

        // 6. Update stats
        self.stats.update(&scores);

        Some(scores)
    }

    /// Check if we have a confirmed signal
    pub fn check_signal(&self) -> Option<ConfirmedBreakout> {
        if self.score_history.len() < self.config.confirmation_candles {
            return None;
        }

        // Check last N scores for consistent signal
        let recent: Vec<&BreakoutScores> = self.score_history.iter()
            .rev()
            .take(self.config.confirmation_candles)
            .collect();

        let bullish_confirmed = recent.iter()
            .all(|s| s.bullish_score > self.config.signal_threshold);

        let bearish_confirmed = recent.iter()
            .all(|s| s.bearish_score > self.config.signal_threshold);

        if bullish_confirmed {
            let avg_score = recent.iter().map(|s| s.bullish_score).sum::<f64>()
                / recent.len() as f64;
            Some(ConfirmedBreakout {
                direction: Direction::Up,
                score: avg_score,
                confirmation_candles: self.config.confirmation_candles,
                timestamp: recent[0].timestamp,
            })
        } else if bearish_confirmed {
            let avg_score = recent.iter().map(|s| s.bearish_score).sum::<f64>()
                / recent.len() as f64;
            Some(ConfirmedBreakout {
                direction: Direction::Down,
                score: avg_score,
                confirmation_candles: self.config.confirmation_candles,
                timestamp: recent[0].timestamp,
            })
        } else {
            None
        }
    }

    /// Get current score vector (for visualization/tracing)
    pub fn current_scores(&self) -> Option<&BreakoutScores> {
        self.score_history.back()
    }

    /// Get score history (for plotting)
    pub fn score_history(&self) -> &VecDeque<BreakoutScores> {
        &self.score_history
    }

    /// Get statistics
    pub fn stats(&self) -> &BreakoutStats {
        &self.stats
    }
}

#[derive(Debug, Clone)]
pub struct ConfirmedBreakout {
    pub direction: Direction,
    pub score: f64,
    pub confirmation_candles: usize,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}
```

### BreakoutStats

```rust
/// Statistics tracked by the breakout detector
#[derive(Debug, Clone, Default)]
pub struct BreakoutStats {
    pub candles_processed: usize,
    pub signals_generated: usize,
    pub bullish_signals: usize,
    pub bearish_signals: usize,

    // Score distribution
    pub max_bullish_score: f64,
    pub max_bearish_score: f64,
    pub mean_bullish_score: f64,
    pub mean_bearish_score: f64,

    // Running sums for mean calculation
    sum_bullish: f64,
    sum_bearish: f64,
}

impl BreakoutStats {
    pub fn update(&mut self, scores: &BreakoutScores) {
        self.candles_processed += 1;

        self.max_bullish_score = self.max_bullish_score.max(scores.bullish_score);
        self.max_bearish_score = self.max_bearish_score.max(scores.bearish_score);

        self.sum_bullish += scores.bullish_score;
        self.sum_bearish += scores.bearish_score;

        self.mean_bullish_score = self.sum_bullish / self.candles_processed as f64;
        self.mean_bearish_score = self.sum_bearish / self.candles_processed as f64;
    }

    pub fn record_signal(&mut self, direction: Direction) {
        self.signals_generated += 1;
        match direction {
            Direction::Up => self.bullish_signals += 1,
            Direction::Down => self.bearish_signals += 1,
        }
    }
}
```

---

## Basis Learning Pipeline

### BasisLearner

```rust
/// Main entry point for learning basis patterns from historical data
pub struct BasisLearner {
    config: LearningConfig,
}

#[derive(Debug, Clone)]
pub struct LearningConfig {
    pub candle_window: usize,           // N candles for pattern
    pub lookahead_candles: usize,       // How far ahead to measure outcome
    pub breakout_threshold_pct: f64,    // What % move counts as breakout
    pub min_samples_per_pattern: usize, // Minimum samples to learn pattern
    pub learning_method: LearningMethod,
}

#[derive(Debug, Clone, Copy)]
pub enum LearningMethod {
    Empirical,       // Average successful patterns
    PCA { n_components: usize },
    Clustering { n_clusters: usize },
    Discriminative,
}

impl BasisLearner {
    pub fn new(config: LearningConfig) -> Self {
        Self { config }
    }

    /// Learn basis patterns from historical candles
    pub fn learn(&self, candles: &[Candle]) -> Result<BasisSet, LearningError> {
        // 1. Find breakout events
        let breakouts = self.find_breakouts(candles)?;
        println!("Found {} breakout events", breakouts.len());

        // 2. Extract market states before each breakout
        let bullish_states: Vec<MarketState> = breakouts.iter()
            .filter(|b| b.direction == Direction::Up)
            .filter_map(|b| self.extract_state_before(candles, b))
            .collect();

        let bearish_states: Vec<MarketState> = breakouts.iter()
            .filter(|b| b.direction == Direction::Down)
            .filter_map(|b| self.extract_state_before(candles, b))
            .collect();

        println!("Bullish patterns: {}, Bearish patterns: {}",
                 bullish_states.len(), bearish_states.len());

        // 3. Learn patterns based on method
        let mut basis_set = BasisSet::new("learned_breakout_basis");

        match self.config.learning_method {
            LearningMethod::Empirical => {
                if bullish_states.len() >= self.config.min_samples_per_pattern {
                    let pattern = self.learn_empirical(&bullish_states, "bullish_breakout", PatternType::BullishBreakout);
                    basis_set.add_pattern(pattern, 1.0);
                }
                if bearish_states.len() >= self.config.min_samples_per_pattern {
                    let pattern = self.learn_empirical(&bearish_states, "bearish_breakdown", PatternType::BearishBreakdown);
                    basis_set.add_pattern(pattern, 1.0);
                }
            }
            LearningMethod::PCA { n_components } => {
                let all_states: Vec<&MarketState> = bullish_states.iter()
                    .chain(bearish_states.iter())
                    .collect();
                let patterns = self.learn_pca(&all_states, n_components);
                for (i, p) in patterns.into_iter().enumerate() {
                    basis_set.add_pattern(p, 1.0 / (i + 1) as f64);
                }
            }
            LearningMethod::Clustering { n_clusters } => {
                // Learn separate clusters for bullish and bearish
                let bullish_patterns = self.learn_clustering(&bullish_states, n_clusters / 2, PatternType::BullishBreakout);
                let bearish_patterns = self.learn_clustering(&bearish_states, n_clusters / 2, PatternType::BearishBreakdown);

                for p in bullish_patterns {
                    basis_set.add_pattern(p, 1.0);
                }
                for p in bearish_patterns {
                    basis_set.add_pattern(p, 1.0);
                }
            }
            LearningMethod::Discriminative => {
                // Need non-breakout samples too
                let non_breakouts = self.find_non_breakouts(candles, &breakouts)?;
                let pattern = self.learn_discriminative(&bullish_states, &non_breakouts);
                basis_set.add_pattern(pattern, 1.0);
            }
        }

        Ok(basis_set)
    }

    fn find_breakouts(&self, candles: &[Candle]) -> Result<Vec<BreakoutEvent>, LearningError> {
        let mut breakouts = Vec::new();

        for i in self.config.candle_window..candles.len() - self.config.lookahead_candles {
            let price_now = candles[i].close;
            let price_future = candles[i + self.config.lookahead_candles].close;
            let move_pct = (price_future - price_now) / price_now * 100.0;

            if move_pct > self.config.breakout_threshold_pct {
                breakouts.push(BreakoutEvent {
                    index: i,
                    direction: Direction::Up,
                    magnitude_pct: move_pct,
                    timestamp: candles[i].timestamp,
                });
            } else if move_pct < -self.config.breakout_threshold_pct {
                breakouts.push(BreakoutEvent {
                    index: i,
                    direction: Direction::Down,
                    magnitude_pct: move_pct.abs(),
                    timestamp: candles[i].timestamp,
                });
            }
        }

        Ok(breakouts)
    }

    fn extract_state_before(&self, candles: &[Candle], breakout: &BreakoutEvent) -> Option<MarketState> {
        if breakout.index < self.config.candle_window {
            return None;
        }

        let start = breakout.index - self.config.candle_window;
        let window = &candles[start..=breakout.index];

        Some(MarketState::from_candles(window))
    }

    fn learn_empirical(&self, states: &[MarketState], name: &str, pattern_type: PatternType) -> BasisPattern {
        // Average all state vectors
        let dim = states[0].as_vector().len();
        let mut avg = vec![0.0; dim];

        for state in states {
            for (i, v) in state.as_vector().iter().enumerate() {
                avg[i] += v;
            }
        }

        for v in &mut avg {
            *v /= states.len() as f64;
        }

        let mut pattern = BasisPattern::from_vector(name, pattern_type, avg);
        pattern.learned_from_n_samples = states.len();
        pattern.normalize();
        pattern
    }

    fn learn_pca(&self, states: &[&MarketState], n_components: usize) -> Vec<BasisPattern> {
        // Simplified PCA implementation
        // In production, use nalgebra or ndarray-linalg

        // For now, return empirical average as single component
        let dim = states[0].as_vector().len();
        let mut avg = vec![0.0; dim];

        for state in states {
            for (i, v) in state.as_vector().iter().enumerate() {
                avg[i] += v;
            }
        }

        for v in &mut avg {
            *v /= states.len() as f64;
        }

        let mut pattern = BasisPattern::from_vector("pca_component_0", PatternType::Custom(0), avg);
        pattern.normalize();

        vec![pattern]  // TODO: Implement full PCA
    }

    fn learn_clustering(&self, states: &[MarketState], n_clusters: usize, pattern_type: PatternType) -> Vec<BasisPattern> {
        // Simplified: just return empirical average for now
        // TODO: Implement k-means clustering

        if states.is_empty() {
            return vec![];
        }

        vec![self.learn_empirical(states, &format!("{:?}_cluster_0", pattern_type), pattern_type)]
    }

    fn learn_discriminative(&self, positive: &[MarketState], negative: &[MarketState]) -> BasisPattern {
        // Fisher's Linear Discriminant
        let dim = positive[0].as_vector().len();

        let mean_pos = Self::compute_mean(positive);
        let mean_neg = Self::compute_mean(negative);

        // Direction = mean_pos - mean_neg (simplified)
        let mut direction: Vec<f64> = mean_pos.iter()
            .zip(&mean_neg)
            .map(|(p, n)| p - n)
            .collect();

        // Normalize
        let norm: f64 = direction.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 1e-10 {
            for v in &mut direction {
                *v /= norm;
            }
        }

        BasisPattern::from_vector("discriminative", PatternType::BullishBreakout, direction)
    }

    fn compute_mean(states: &[MarketState]) -> Vec<f64> {
        let dim = states[0].as_vector().len();
        let mut avg = vec![0.0; dim];

        for state in states {
            for (i, v) in state.as_vector().iter().enumerate() {
                avg[i] += v;
            }
        }

        for v in &mut avg {
            *v /= states.len() as f64;
        }

        avg
    }

    fn find_non_breakouts(&self, candles: &[Candle], breakouts: &[BreakoutEvent]) -> Result<Vec<MarketState>, LearningError> {
        // Find periods that were NOT followed by breakouts
        let breakout_indices: std::collections::HashSet<usize> = breakouts.iter()
            .map(|b| b.index)
            .collect();

        let mut non_breakouts = Vec::new();

        for i in self.config.candle_window..candles.len() - self.config.lookahead_candles {
            if !breakout_indices.contains(&i) {
                if let Some(state) = self.extract_state_before(candles, &BreakoutEvent {
                    index: i,
                    direction: Direction::Up,
                    magnitude_pct: 0.0,
                    timestamp: candles[i].timestamp,
                }) {
                    non_breakouts.push(state);
                }
            }
        }

        Ok(non_breakouts)
    }
}

#[derive(Debug, Clone)]
struct BreakoutEvent {
    index: usize,
    direction: Direction,
    magnitude_pct: f64,
    timestamp: DateTime<Utc>,
}

#[derive(Debug)]
pub enum LearningError {
    InsufficientData(String),
    NoBreakoutsFound,
    ComputationError(String),
}
```

---

## Integration with Trading System

### MOM_BreakoutStrategy

```rust
/// Momentum strategy based on breakout pattern detection
pub struct MOM_BreakoutStrategy {
    detector: BreakoutDetector,

    // Configuration
    config: BreakoutStrategyConfig,

    // State
    position: Option<Position>,
    last_signal: Option<ConfirmedBreakout>,
}

#[derive(Debug, Clone)]
pub struct BreakoutStrategyConfig {
    pub signal_threshold: f64,
    pub confirmation_candles: usize,
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    pub max_position_size: f64,
}

impl TradingStrategy for MOM_BreakoutStrategy {
    fn name(&self) -> &str {
        "MOM_Breakout"
    }

    fn on_candle(&mut self, candle: &Candle) -> StrategyDecision {
        // 1. Update detector
        self.detector.on_candle(*candle);

        // 2. Check for confirmed signal
        if let Some(signal) = self.detector.check_signal() {
            // 3. If no position, consider entry
            if self.position.is_none() {
                let side = match signal.direction {
                    Direction::Up => Side::Buy,
                    Direction::Down => Side::Sell,
                };

                return StrategyDecision::Enter {
                    side,
                    size: self.calculate_size(signal.score),
                    tp_bps: self.config.take_profit_pct * 100.0,
                    sl_bps: self.config.stop_loss_pct * 100.0,
                };
            }
        }

        StrategyDecision::Hold
    }

    fn current_position(&self) -> &Option<Position> {
        &self.position
    }
}
```

---

## CLI Tool for Research

```rust
/// Command-line tool for breakout pattern research
///
/// Usage:
///   cargo run --bin breakout_research -- learn --data ./data/candles --output ./models/basis.json
///   cargo run --bin breakout_research -- backtest --basis ./models/basis.json --data ./data/candles
///   cargo run --bin breakout_research -- analyze --data ./data/candles

fn main() {
    let args = Args::parse();

    match args.command {
        Command::Learn { data, output, method, threshold } => {
            // Load candle data
            let candles = load_candles(&data)?;

            // Configure learner
            let config = LearningConfig {
                candle_window: 15,
                lookahead_candles: 10,
                breakout_threshold_pct: threshold,
                min_samples_per_pattern: 50,
                learning_method: method,
            };

            // Learn basis
            let learner = BasisLearner::new(config);
            let basis_set = learner.learn(&candles)?;

            // Save
            save_basis_set(&basis_set, &output)?;
            println!("Learned {} patterns, saved to {:?}", basis_set.patterns.len(), output);
        }

        Command::Backtest { basis, data, threshold } => {
            // Load basis and data
            let basis_set = load_basis_set(&basis)?;
            let candles = load_candles(&data)?;

            // Create detector
            let config = BreakoutConfig {
                signal_threshold: threshold,
                ..Default::default()
            };
            let mut detector = BreakoutDetector::new(config, basis_set);

            // Run through candles
            let mut signals = Vec::new();
            for candle in &candles {
                if let Some(scores) = detector.on_candle(*candle) {
                    if let Some(signal) = detector.check_signal() {
                        signals.push((candle.timestamp, signal));
                    }
                }
            }

            // Evaluate signals
            evaluate_signals(&signals, &candles);
        }

        Command::Analyze { data } => {
            // Statistical analysis of breakout patterns
            let candles = load_candles(&data)?;
            analyze_breakout_statistics(&candles);
        }
    }
}
```

---

## Implementation Checklist

| Task | Description | Status |
|------|-------------|--------|
| 1 | Define Candle, CandleFeatures structs | TODO |
| 2 | Implement MarketState from candle window | TODO |
| 3 | Implement BasisPattern with dot/cosine | TODO |
| 4 | Implement BasisSet with score computation | TODO |
| 5 | Implement BreakoutDetector (real-time) | TODO |
| 6 | Implement BasisLearner (empirical method) | TODO |
| 7 | Add PCA-based learning | TODO |
| 8 | Add clustering-based learning | TODO |
| 9 | Add discriminative learning | TODO |
| 10 | Create CLI tool for research | TODO |
| 11 | Integrate with MOM_* strategy framework | TODO |
| 12 | Add TUI visualization of scores | TODO |
| 13 | Add persistence (save/load basis sets) | TODO |
| 14 | Backtest validation | TODO |

---

## Summary

This framework provides:

1. **Market State Vector**: Standardized representation of recent price/volume action
2. **Basis Patterns**: Learned templates representing canonical breakout setups
3. **Continuous Tracing**: Real-time dot product scores updated on each candle
4. **Multiple Learning Methods**: Empirical, PCA, clustering, discriminative
5. **Trading Integration**: Direct feed into MOM_* strategy decisions

The key insight is that **breakouts have signatures** - characteristic patterns in price and volume that precede significant moves. By learning these signatures from historical data and matching against them in real-time, we can detect breakout setups before they fully develop.

---

*Document maintained by: Development Team*
*Last updated: December 18, 2025*

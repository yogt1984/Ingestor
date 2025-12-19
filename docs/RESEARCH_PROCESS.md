# Research Process: Pattern-Based Breakout Prediction

**Document Version:** 1.0
**Created:** December 18, 2025
**Purpose:** Define a systematic research process to answer: "When do past patterns predict breakouts?"

---

## The Core Question

> **Given a feature vector F(t) representing market state at time t, can we predict whether a breakout will occur in the next T minutes?**

This research process uses multiple analysis methods to:
1. **Discover** patterns that precede breakouts
2. **Validate** that these patterns have predictive power
3. **Quantify** the probability and expected magnitude of breakouts
4. **Operationalize** findings into trading signals

---

## Research Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         RESEARCH PIPELINE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STAGE 1: DATA PREPARATION                                                  │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                     │
│  │ Load Parquet│ -> │ Build       │ -> │ Label       │                     │
│  │ Features    │    │ Feature     │    │ Breakouts   │                     │
│  │             │    │ Vectors     │    │ (Y/N)       │                     │
│  └─────────────┘    └─────────────┘    └─────────────┘                     │
│                                              │                              │
│  STAGE 2: EXPLORATORY ANALYSIS              ▼                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                     │
│  │ Statistical │    │ Feature     │    │ Temporal    │                     │
│  │ Summary     │    │ Correlation │    │ Patterns    │                     │
│  └─────────────┘    └─────────────┘    └─────────────┘                     │
│                                              │                              │
│  STAGE 3: PATTERN DISCOVERY                 ▼                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│  │ Empirical   │    │ PCA         │    │ Clustering  │    │Discriminant │ │
│  │ Averaging   │    │ Analysis    │    │ (K-Means)   │    │ Analysis    │ │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘ │
│                                              │                              │
│  STAGE 4: VALIDATION                        ▼                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                     │
│  │ Statistical │    │ Walk-Forward│    │ Out-of-     │                     │
│  │ Tests       │    │ Validation  │    │ Sample Test │                     │
│  └─────────────┘    └─────────────┘    └─────────────┘                     │
│                                              │                              │
│  STAGE 5: OPERATIONALIZATION               ▼                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                     │
│  │ Build Basis │    │ Define      │    │ Integrate   │                     │
│  │ Patterns    │    │ Thresholds  │    │ w/ Strategy │                     │
│  └─────────────┘    └─────────────┘    └─────────────┘                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Stage 1: Data Preparation

### 1.1 Feature Vector Definition

The feature vector F(t) captures market state from multiple sources:

```rust
/// Complete feature vector for research
#[derive(Debug, Clone)]
pub struct ResearchFeatureVector {
    pub timestamp: DateTime<Utc>,

    // Source: FeaturesSnapshot (existing)
    pub orderbook_features: OrderBookFeatures,    // ~20 features
    pub trade_flow_features: TradeFlowFeatures,   // ~15 features
    pub entropy_features: EntropyFeatures,        // ~6 features
    pub volatility_features: VolatilityFeatures,  // ~8 features
    pub trend_features: TrendFeatures,            // ~8 features

    // Source: Candle aggregation (new)
    pub candle_features: CandleWindowFeatures,    // ~15 candles × 7 = 105 features

    // Derived: Signal processing
    pub kalman_state: KalmanState,                // ~4 features

    // Total: ~166 features
}

impl ResearchFeatureVector {
    pub const DIMENSION: usize = 166;

    /// Convert to flat vector for analysis
    pub fn to_vector(&self) -> Vec<f64> {
        let mut v = Vec::with_capacity(Self::DIMENSION);
        v.extend(self.orderbook_features.to_array());
        v.extend(self.trade_flow_features.to_array());
        v.extend(self.entropy_features.to_array());
        v.extend(self.volatility_features.to_array());
        v.extend(self.trend_features.to_array());
        v.extend(self.candle_features.to_array());
        v.extend(self.kalman_state.to_array());
        v
    }

    /// Normalize vector (z-score)
    pub fn normalize(&self, mean: &[f64], std: &[f64]) -> Vec<f64> {
        self.to_vector()
            .iter()
            .zip(mean.iter().zip(std.iter()))
            .map(|(x, (m, s))| if *s > 1e-10 { (x - m) / s } else { 0.0 })
            .collect()
    }
}
```

### 1.2 Breakout Labeling

```rust
/// Label each feature vector with whether a breakout followed
#[derive(Debug, Clone)]
pub struct LabeledSample {
    pub features: ResearchFeatureVector,
    pub label: BreakoutLabel,
    pub outcome: BreakoutOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakoutLabel {
    NoBreakout,
    BullishBreakout,
    BearishBreakout,
}

#[derive(Debug, Clone)]
pub struct BreakoutOutcome {
    pub max_up_pct: f64,      // Maximum upward move in lookahead
    pub max_down_pct: f64,    // Maximum downward move in lookahead
    pub final_pct: f64,       // Final price change
    pub time_to_max: f64,     // Time to reach maximum
}

pub struct DataLabeler {
    pub lookahead_minutes: f64,
    pub breakout_threshold_pct: f64,
}

impl DataLabeler {
    /// Label all samples in dataset
    pub fn label_dataset(&self, features: &[ResearchFeatureVector], prices: &[PricePoint]) -> Vec<LabeledSample> {
        features.iter()
            .filter_map(|f| {
                let outcome = self.compute_outcome(f.timestamp, prices)?;
                let label = self.classify_outcome(&outcome);
                Some(LabeledSample { features: f.clone(), label, outcome })
            })
            .collect()
    }

    fn compute_outcome(&self, t: DateTime<Utc>, prices: &[PricePoint]) -> Option<BreakoutOutcome> {
        let start_idx = prices.iter().position(|p| p.timestamp >= t)?;
        let end_time = t + Duration::minutes(self.lookahead_minutes as i64);
        let end_idx = prices.iter().position(|p| p.timestamp >= end_time)?;

        let window = &prices[start_idx..=end_idx];
        let start_price = window[0].mid_price;

        let max_up = window.iter().map(|p| (p.mid_price - start_price) / start_price * 100.0).fold(0.0, f64::max);
        let max_down = window.iter().map(|p| (start_price - p.mid_price) / start_price * 100.0).fold(0.0, f64::max);
        let final_pct = (window.last()?.mid_price - start_price) / start_price * 100.0;

        Some(BreakoutOutcome {
            max_up_pct: max_up,
            max_down_pct: max_down,
            final_pct,
            time_to_max: 0.0, // TODO: compute
        })
    }

    fn classify_outcome(&self, outcome: &BreakoutOutcome) -> BreakoutLabel {
        if outcome.max_up_pct > self.breakout_threshold_pct && outcome.max_up_pct > outcome.max_down_pct {
            BreakoutLabel::BullishBreakout
        } else if outcome.max_down_pct > self.breakout_threshold_pct {
            BreakoutLabel::BearishBreakout
        } else {
            BreakoutLabel::NoBreakout
        }
    }
}
```

### 1.3 Dataset Split

```rust
pub struct DatasetSplit {
    pub train: Vec<LabeledSample>,      // 60% - for learning
    pub validation: Vec<LabeledSample>,  // 20% - for tuning
    pub test: Vec<LabeledSample>,        // 20% - for final evaluation
}

impl DatasetSplit {
    /// Time-based split (no future leakage)
    pub fn from_chronological(samples: Vec<LabeledSample>) -> Self {
        let n = samples.len();
        let train_end = (n as f64 * 0.6) as usize;
        let val_end = (n as f64 * 0.8) as usize;

        Self {
            train: samples[..train_end].to_vec(),
            validation: samples[train_end..val_end].to_vec(),
            test: samples[val_end..].to_vec(),
        }
    }
}
```

---

## Stage 2: Exploratory Analysis

### 2.1 Statistical Summary

```rust
pub struct StatisticalSummary {
    pub n_samples: usize,
    pub n_breakouts: usize,
    pub n_bullish: usize,
    pub n_bearish: usize,
    pub breakout_rate: f64,

    pub feature_stats: Vec<FeatureStats>,
}

pub struct FeatureStats {
    pub name: String,
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub max: f64,
    pub skewness: f64,
    pub kurtosis: f64,

    // Conditional stats
    pub mean_given_breakout: f64,
    pub mean_given_no_breakout: f64,
    pub t_statistic: f64,
    pub p_value: f64,
}

impl StatisticalSummary {
    pub fn compute(samples: &[LabeledSample]) -> Self {
        // ... compute all statistics
    }

    /// Find features with significant difference between breakout/no-breakout
    pub fn significant_features(&self, p_threshold: f64) -> Vec<&FeatureStats> {
        self.feature_stats.iter()
            .filter(|f| f.p_value < p_threshold)
            .collect()
    }
}
```

### 2.2 Feature Correlation Analysis

```rust
pub struct CorrelationAnalysis {
    /// Correlation matrix between features
    pub feature_correlation: Vec<Vec<f64>>,

    /// Correlation of each feature with breakout outcome
    pub outcome_correlation: Vec<f64>,

    /// Highly correlated feature pairs (for removal)
    pub redundant_pairs: Vec<(usize, usize, f64)>,
}

impl CorrelationAnalysis {
    pub fn compute(samples: &[LabeledSample]) -> Self {
        // ... compute correlation matrix
    }

    /// Get features most correlated with breakouts
    pub fn top_predictive_features(&self, n: usize) -> Vec<(usize, f64)> {
        let mut indexed: Vec<(usize, f64)> = self.outcome_correlation.iter()
            .enumerate()
            .map(|(i, &c)| (i, c.abs()))
            .collect();
        indexed.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        indexed.into_iter().take(n).collect()
    }
}
```

### 2.3 Temporal Pattern Analysis

```rust
pub struct TemporalAnalysis {
    /// Breakout rate by hour of day
    pub hourly_breakout_rate: [f64; 24],

    /// Breakout rate by day of week
    pub daily_breakout_rate: [f64; 7],

    /// Average time between breakouts
    pub mean_inter_breakout_minutes: f64,

    /// Autocorrelation of breakout events
    pub breakout_autocorrelation: Vec<f64>,
}
```

---

## Stage 3: Pattern Discovery

### 3.1 Empirical Pattern Extraction

**Goal:** Find the "average" feature vector that precedes breakouts.

```rust
pub struct EmpiricalAnalysis {
    /// Average feature vector before bullish breakouts
    pub bullish_centroid: Vec<f64>,

    /// Average feature vector before bearish breakouts
    pub bearish_centroid: Vec<f64>,

    /// Average feature vector before no-breakout periods
    pub neutral_centroid: Vec<f64>,

    /// Standard deviation within each class
    pub bullish_std: Vec<f64>,
    pub bearish_std: Vec<f64>,
    pub neutral_std: Vec<f64>,
}

impl EmpiricalAnalysis {
    pub fn compute(samples: &[LabeledSample]) -> Self {
        let bullish: Vec<_> = samples.iter()
            .filter(|s| s.label == BreakoutLabel::BullishBreakout)
            .collect();
        let bearish: Vec<_> = samples.iter()
            .filter(|s| s.label == BreakoutLabel::BearishBreakout)
            .collect();
        let neutral: Vec<_> = samples.iter()
            .filter(|s| s.label == BreakoutLabel::NoBreakout)
            .collect();

        Self {
            bullish_centroid: Self::compute_centroid(&bullish),
            bearish_centroid: Self::compute_centroid(&bearish),
            neutral_centroid: Self::compute_centroid(&neutral),
            bullish_std: Self::compute_std(&bullish),
            bearish_std: Self::compute_std(&bearish),
            neutral_std: Self::compute_std(&neutral),
        }
    }

    /// Create basis patterns from centroids
    pub fn to_basis_patterns(&self) -> Vec<BasisPattern> {
        vec![
            BasisPattern::from_vector("empirical_bullish", PatternType::BullishBreakout, self.bullish_centroid.clone()),
            BasisPattern::from_vector("empirical_bearish", PatternType::BearishBreakdown, self.bearish_centroid.clone()),
        ]
    }

    /// Score a new sample against empirical patterns
    pub fn score(&self, features: &ResearchFeatureVector) -> EmpiricalScores {
        let v = features.to_vector();
        EmpiricalScores {
            bullish_similarity: cosine_similarity(&v, &self.bullish_centroid),
            bearish_similarity: cosine_similarity(&v, &self.bearish_centroid),
            neutral_similarity: cosine_similarity(&v, &self.neutral_centroid),
        }
    }
}

pub struct EmpiricalScores {
    pub bullish_similarity: f64,
    pub bearish_similarity: f64,
    pub neutral_similarity: f64,
}
```

### 3.2 PCA Analysis

**Goal:** Find orthogonal directions of maximum variance in breakout patterns.

```rust
pub struct PCAAnalysis {
    /// Principal components (eigenvectors)
    pub components: Vec<Vec<f64>>,

    /// Explained variance ratio for each component
    pub explained_variance_ratio: Vec<f64>,

    /// Cumulative explained variance
    pub cumulative_variance: Vec<f64>,

    /// Number of components to explain 95% variance
    pub n_components_95: usize,

    /// Mean vector (for centering)
    pub mean: Vec<f64>,
}

impl PCAAnalysis {
    pub fn compute(samples: &[LabeledSample], max_components: usize) -> Self {
        // 1. Extract feature vectors
        let vectors: Vec<Vec<f64>> = samples.iter()
            .map(|s| s.features.to_vector())
            .collect();

        // 2. Center data
        let mean = Self::compute_mean(&vectors);
        let centered: Vec<Vec<f64>> = vectors.iter()
            .map(|v| v.iter().zip(&mean).map(|(x, m)| x - m).collect())
            .collect();

        // 3. Compute covariance matrix
        let cov = Self::compute_covariance(&centered);

        // 4. Eigendecomposition (use nalgebra or similar)
        let (eigenvalues, eigenvectors) = Self::eigen_decomposition(&cov);

        // 5. Sort by eigenvalue and take top components
        let total_var: f64 = eigenvalues.iter().sum();
        let explained_variance_ratio: Vec<f64> = eigenvalues.iter()
            .map(|e| e / total_var)
            .take(max_components)
            .collect();

        let cumulative: Vec<f64> = explained_variance_ratio.iter()
            .scan(0.0, |acc, &x| { *acc += x; Some(*acc) })
            .collect();

        let n_95 = cumulative.iter().position(|&c| c >= 0.95).unwrap_or(max_components);

        Self {
            components: eigenvectors.into_iter().take(max_components).collect(),
            explained_variance_ratio,
            cumulative_variance: cumulative,
            n_components_95: n_95,
            mean,
        }
    }

    /// Project sample onto principal components
    pub fn transform(&self, features: &ResearchFeatureVector) -> Vec<f64> {
        let v = features.to_vector();
        let centered: Vec<f64> = v.iter().zip(&self.mean).map(|(x, m)| x - m).collect();

        self.components.iter()
            .map(|pc| centered.iter().zip(pc).map(|(x, p)| x * p).sum())
            .collect()
    }

    /// Create basis patterns from top PCs
    pub fn to_basis_patterns(&self, n: usize) -> Vec<BasisPattern> {
        self.components.iter()
            .take(n)
            .enumerate()
            .map(|(i, pc)| BasisPattern::from_vector(
                &format!("pca_component_{}", i),
                PatternType::Custom(i as u32),
                pc.clone()
            ))
            .collect()
    }

    /// Analyze which original features load onto each PC
    pub fn feature_loadings(&self, feature_names: &[String]) -> Vec<PCLoadings> {
        self.components.iter()
            .enumerate()
            .map(|(pc_idx, pc)| {
                let mut loadings: Vec<(String, f64)> = feature_names.iter()
                    .zip(pc)
                    .map(|(name, &loading)| (name.clone(), loading))
                    .collect();
                loadings.sort_by(|a, b| b.1.abs().partial_cmp(&a.1.abs()).unwrap());

                PCLoadings {
                    component_index: pc_idx,
                    explained_variance: self.explained_variance_ratio[pc_idx],
                    top_loadings: loadings.into_iter().take(10).collect(),
                }
            })
            .collect()
    }
}

pub struct PCLoadings {
    pub component_index: usize,
    pub explained_variance: f64,
    pub top_loadings: Vec<(String, f64)>,
}
```

### 3.3 Clustering Analysis

**Goal:** Find distinct pattern types within breakout events.

```rust
pub struct ClusteringAnalysis {
    /// Cluster centroids
    pub centroids: Vec<Vec<f64>>,

    /// Cluster assignments for each sample
    pub assignments: Vec<usize>,

    /// Samples per cluster
    pub cluster_sizes: Vec<usize>,

    /// Within-cluster variance
    pub inertia: f64,

    /// Silhouette score (cluster quality)
    pub silhouette_score: f64,

    /// Breakout rate per cluster
    pub cluster_breakout_rates: Vec<ClusterStats>,
}

pub struct ClusterStats {
    pub cluster_id: usize,
    pub n_samples: usize,
    pub n_bullish: usize,
    pub n_bearish: usize,
    pub n_neutral: usize,
    pub bullish_rate: f64,
    pub bearish_rate: f64,
}

impl ClusteringAnalysis {
    pub fn compute_kmeans(samples: &[LabeledSample], k: usize, max_iter: usize) -> Self {
        let vectors: Vec<Vec<f64>> = samples.iter()
            .map(|s| s.features.to_vector())
            .collect();

        // K-Means algorithm
        let (centroids, assignments) = Self::kmeans(&vectors, k, max_iter);

        // Compute cluster statistics
        let cluster_sizes: Vec<usize> = (0..k)
            .map(|c| assignments.iter().filter(|&&a| a == c).count())
            .collect();

        let cluster_breakout_rates: Vec<ClusterStats> = (0..k)
            .map(|c| {
                let cluster_samples: Vec<_> = samples.iter()
                    .zip(&assignments)
                    .filter(|(_, &a)| a == c)
                    .map(|(s, _)| s)
                    .collect();

                let n = cluster_samples.len();
                let n_bullish = cluster_samples.iter().filter(|s| s.label == BreakoutLabel::BullishBreakout).count();
                let n_bearish = cluster_samples.iter().filter(|s| s.label == BreakoutLabel::BearishBreakout).count();

                ClusterStats {
                    cluster_id: c,
                    n_samples: n,
                    n_bullish,
                    n_bearish,
                    n_neutral: n - n_bullish - n_bearish,
                    bullish_rate: n_bullish as f64 / n as f64,
                    bearish_rate: n_bearish as f64 / n as f64,
                }
            })
            .collect();

        Self {
            centroids,
            assignments,
            cluster_sizes,
            inertia: 0.0, // TODO
            silhouette_score: 0.0, // TODO
            cluster_breakout_rates,
        }
    }

    /// Find optimal k using elbow method
    pub fn find_optimal_k(samples: &[LabeledSample], k_range: std::ops::Range<usize>) -> Vec<(usize, f64)> {
        k_range.map(|k| {
            let analysis = Self::compute_kmeans(samples, k, 100);
            (k, analysis.inertia)
        }).collect()
    }

    /// Identify "breakout-prone" clusters
    pub fn breakout_clusters(&self, min_rate: f64) -> Vec<&ClusterStats> {
        self.cluster_breakout_rates.iter()
            .filter(|c| c.bullish_rate > min_rate || c.bearish_rate > min_rate)
            .collect()
    }

    /// Create basis patterns from breakout-prone cluster centroids
    pub fn to_basis_patterns(&self, min_breakout_rate: f64) -> Vec<BasisPattern> {
        self.cluster_breakout_rates.iter()
            .filter(|c| c.bullish_rate > min_breakout_rate || c.bearish_rate > min_breakout_rate)
            .map(|c| {
                let pattern_type = if c.bullish_rate > c.bearish_rate {
                    PatternType::BullishBreakout
                } else {
                    PatternType::BearishBreakdown
                };
                BasisPattern::from_vector(
                    &format!("cluster_{}", c.cluster_id),
                    pattern_type,
                    self.centroids[c.cluster_id].clone()
                )
            })
            .collect()
    }

    /// Assign new sample to cluster
    pub fn predict_cluster(&self, features: &ResearchFeatureVector) -> usize {
        let v = features.to_vector();
        self.centroids.iter()
            .enumerate()
            .min_by(|(_, c1), (_, c2)| {
                let d1 = euclidean_distance(&v, c1);
                let d2 = euclidean_distance(&v, c2);
                d1.partial_cmp(&d2).unwrap()
            })
            .map(|(i, _)| i)
            .unwrap_or(0)
    }
}
```

### 3.4 Discriminant Analysis (LDA/QDA)

**Goal:** Find directions that maximize separation between breakout and no-breakout classes.

```rust
pub struct DiscriminantAnalysis {
    /// Linear discriminant direction (LDA)
    pub lda_direction: Vec<f64>,

    /// Class means
    pub mean_breakout: Vec<f64>,
    pub mean_no_breakout: Vec<f64>,

    /// Within-class scatter matrix
    pub within_scatter: Vec<Vec<f64>>,

    /// Between-class scatter matrix
    pub between_scatter: Vec<Vec<f64>>,

    /// Fisher criterion value (separation quality)
    pub fisher_criterion: f64,

    /// Classification threshold
    pub threshold: f64,

    /// Training accuracy
    pub train_accuracy: f64,
}

impl DiscriminantAnalysis {
    pub fn compute(samples: &[LabeledSample]) -> Self {
        // Separate classes
        let breakouts: Vec<Vec<f64>> = samples.iter()
            .filter(|s| s.label != BreakoutLabel::NoBreakout)
            .map(|s| s.features.to_vector())
            .collect();

        let no_breakouts: Vec<Vec<f64>> = samples.iter()
            .filter(|s| s.label == BreakoutLabel::NoBreakout)
            .map(|s| s.features.to_vector())
            .collect();

        // Compute class means
        let mean_breakout = Self::compute_mean(&breakouts);
        let mean_no_breakout = Self::compute_mean(&no_breakouts);

        // Compute within-class scatter
        let sw_breakout = Self::compute_scatter(&breakouts, &mean_breakout);
        let sw_no_breakout = Self::compute_scatter(&no_breakouts, &mean_no_breakout);
        let within_scatter = Self::add_matrices(&sw_breakout, &sw_no_breakout);

        // Compute between-class scatter
        let mean_diff: Vec<f64> = mean_breakout.iter()
            .zip(&mean_no_breakout)
            .map(|(a, b)| a - b)
            .collect();

        // LDA direction = Sw^(-1) * (m1 - m2)
        // Simplified: use mean difference as direction
        let lda_direction = Self::normalize(&mean_diff);

        // Compute threshold (midpoint of projected means)
        let proj_breakout: f64 = mean_breakout.iter().zip(&lda_direction).map(|(x, w)| x * w).sum();
        let proj_no_breakout: f64 = mean_no_breakout.iter().zip(&lda_direction).map(|(x, w)| x * w).sum();
        let threshold = (proj_breakout + proj_no_breakout) / 2.0;

        // Compute training accuracy
        let mut correct = 0;
        for sample in samples {
            let proj: f64 = sample.features.to_vector().iter()
                .zip(&lda_direction)
                .map(|(x, w)| x * w)
                .sum();
            let predicted_breakout = proj > threshold;
            let actual_breakout = sample.label != BreakoutLabel::NoBreakout;
            if predicted_breakout == actual_breakout {
                correct += 1;
            }
        }
        let train_accuracy = correct as f64 / samples.len() as f64;

        Self {
            lda_direction,
            mean_breakout,
            mean_no_breakout,
            within_scatter,
            between_scatter: vec![], // TODO
            fisher_criterion: 0.0, // TODO
            threshold,
            train_accuracy,
        }
    }

    /// Score a sample (distance along discriminant axis)
    pub fn score(&self, features: &ResearchFeatureVector) -> f64 {
        features.to_vector().iter()
            .zip(&self.lda_direction)
            .map(|(x, w)| x * w)
            .sum()
    }

    /// Predict breakout probability
    pub fn predict_probability(&self, features: &ResearchFeatureVector) -> f64 {
        let score = self.score(features);
        // Convert to probability using sigmoid
        1.0 / (1.0 + (-2.0 * (score - self.threshold)).exp())
    }

    /// Create basis pattern from LDA direction
    pub fn to_basis_pattern(&self) -> BasisPattern {
        BasisPattern::from_vector(
            "lda_discriminant",
            PatternType::BullishBreakout,
            self.lda_direction.clone()
        )
    }

    /// Feature importance (absolute LDA weights)
    pub fn feature_importance(&self, feature_names: &[String]) -> Vec<(String, f64)> {
        let mut importance: Vec<(String, f64)> = feature_names.iter()
            .zip(&self.lda_direction)
            .map(|(name, &w)| (name.clone(), w.abs()))
            .collect();
        importance.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        importance
    }
}
```

---

## Stage 4: Validation

### 4.1 Statistical Significance Testing

```rust
pub struct StatisticalTests {
    /// Is the breakout pattern significantly different from random?
    pub pattern_significance: SignificanceResult,

    /// Is discriminant analysis better than chance?
    pub discriminant_significance: SignificanceResult,

    /// Are clusters statistically distinct?
    pub cluster_significance: Vec<SignificanceResult>,
}

pub struct SignificanceResult {
    pub test_name: String,
    pub test_statistic: f64,
    pub p_value: f64,
    pub is_significant: bool,  // p < 0.05
    pub effect_size: f64,
}

impl StatisticalTests {
    /// Test if pattern similarity predicts breakouts
    pub fn test_pattern_significance(
        empirical: &EmpiricalAnalysis,
        samples: &[LabeledSample]
    ) -> SignificanceResult {
        // Compare pattern scores for breakout vs no-breakout samples
        let breakout_scores: Vec<f64> = samples.iter()
            .filter(|s| s.label != BreakoutLabel::NoBreakout)
            .map(|s| empirical.score(&s.features).bullish_similarity)
            .collect();

        let no_breakout_scores: Vec<f64> = samples.iter()
            .filter(|s| s.label == BreakoutLabel::NoBreakout)
            .map(|s| empirical.score(&s.features).bullish_similarity)
            .collect();

        // Two-sample t-test
        let (t_stat, p_value) = t_test(&breakout_scores, &no_breakout_scores);
        let effect_size = cohens_d(&breakout_scores, &no_breakout_scores);

        SignificanceResult {
            test_name: "Pattern Similarity T-Test".to_string(),
            test_statistic: t_stat,
            p_value,
            is_significant: p_value < 0.05,
            effect_size,
        }
    }
}
```

### 4.2 Walk-Forward Validation

```rust
pub struct WalkForwardValidation {
    /// Results from each fold
    pub fold_results: Vec<FoldResult>,

    /// Aggregated metrics
    pub mean_accuracy: f64,
    pub std_accuracy: f64,
    pub mean_precision: f64,
    pub mean_recall: f64,
    pub mean_f1: f64,
}

pub struct FoldResult {
    pub fold_index: usize,
    pub train_start: DateTime<Utc>,
    pub train_end: DateTime<Utc>,
    pub test_start: DateTime<Utc>,
    pub test_end: DateTime<Utc>,

    pub accuracy: f64,
    pub precision: f64,
    pub recall: f64,
    pub f1_score: f64,

    pub confusion_matrix: [[usize; 2]; 2],  // [[TN, FP], [FN, TP]]
}

impl WalkForwardValidation {
    /// Run walk-forward validation
    pub fn run<M: PatternModel>(
        samples: &[LabeledSample],
        n_folds: usize,
        train_ratio: f64,
    ) -> Self {
        let fold_size = samples.len() / n_folds;
        let train_size = (fold_size as f64 * train_ratio) as usize;

        let fold_results: Vec<FoldResult> = (0..n_folds)
            .map(|fold| {
                let start = fold * fold_size;
                let train_end = start + train_size;
                let test_end = start + fold_size;

                let train = &samples[start..train_end];
                let test = &samples[train_end..test_end];

                // Train model on train set
                let model = M::train(train);

                // Evaluate on test set
                let predictions: Vec<bool> = test.iter()
                    .map(|s| model.predict(&s.features))
                    .collect();

                let actuals: Vec<bool> = test.iter()
                    .map(|s| s.label != BreakoutLabel::NoBreakout)
                    .collect();

                Self::compute_fold_result(fold, train, test, &predictions, &actuals)
            })
            .collect();

        let mean_accuracy = fold_results.iter().map(|f| f.accuracy).sum::<f64>() / n_folds as f64;
        let std_accuracy = (fold_results.iter()
            .map(|f| (f.accuracy - mean_accuracy).powi(2))
            .sum::<f64>() / n_folds as f64).sqrt();

        Self {
            fold_results,
            mean_accuracy,
            std_accuracy,
            mean_precision: 0.0, // TODO
            mean_recall: 0.0,
            mean_f1: 0.0,
        }
    }
}
```

### 4.3 Out-of-Sample Testing

```rust
pub struct OutOfSampleTest {
    pub test_period_start: DateTime<Utc>,
    pub test_period_end: DateTime<Utc>,
    pub n_samples: usize,

    // Classification metrics
    pub accuracy: f64,
    pub precision: f64,
    pub recall: f64,
    pub f1_score: f64,

    // Trading metrics
    pub n_signals: usize,
    pub n_correct_signals: usize,
    pub signal_accuracy: f64,
    pub expected_pnl_per_signal_bps: f64,
    pub sharpe_ratio: f64,
}

impl OutOfSampleTest {
    pub fn run<M: PatternModel>(
        model: &M,
        test_samples: &[LabeledSample],
        signal_threshold: f64,
    ) -> Self {
        // Generate predictions
        let predictions: Vec<(f64, bool)> = test_samples.iter()
            .map(|s| {
                let score = model.score(&s.features);
                let predicted = score > signal_threshold;
                (score, predicted)
            })
            .collect();

        // Compute metrics
        let signals: Vec<(&LabeledSample, f64)> = test_samples.iter()
            .zip(&predictions)
            .filter(|(_, (score, _))| *score > signal_threshold)
            .map(|(s, (score, _))| (s, *score))
            .collect();

        let correct_signals = signals.iter()
            .filter(|(s, _)| s.label != BreakoutLabel::NoBreakout)
            .count();

        Self {
            test_period_start: test_samples.first().unwrap().features.timestamp,
            test_period_end: test_samples.last().unwrap().features.timestamp,
            n_samples: test_samples.len(),
            accuracy: 0.0, // TODO
            precision: 0.0,
            recall: 0.0,
            f1_score: 0.0,
            n_signals: signals.len(),
            n_correct_signals: correct_signals,
            signal_accuracy: correct_signals as f64 / signals.len() as f64,
            expected_pnl_per_signal_bps: 0.0, // TODO
            sharpe_ratio: 0.0,
        }
    }
}
```

---

## Stage 5: Operationalization

### 5.1 Combined Pattern Model

```rust
/// Unified model combining all analysis methods
pub struct CombinedPatternModel {
    /// Empirical patterns
    pub empirical: EmpiricalAnalysis,

    /// PCA components
    pub pca: PCAAnalysis,

    /// Cluster centroids
    pub clustering: ClusteringAnalysis,

    /// Discriminant direction
    pub discriminant: DiscriminantAnalysis,

    /// Combined basis set
    pub basis_set: BasisSet,

    /// Weights for each method
    pub method_weights: MethodWeights,

    /// Normalization parameters
    pub feature_mean: Vec<f64>,
    pub feature_std: Vec<f64>,
}

pub struct MethodWeights {
    pub empirical: f64,
    pub pca: f64,
    pub clustering: f64,
    pub discriminant: f64,
}

impl CombinedPatternModel {
    /// Train combined model
    pub fn train(samples: &[LabeledSample]) -> Self {
        // Run all analyses
        let empirical = EmpiricalAnalysis::compute(samples);
        let pca = PCAAnalysis::compute(samples, 20);
        let clustering = ClusteringAnalysis::compute_kmeans(samples, 8, 100);
        let discriminant = DiscriminantAnalysis::compute(samples);

        // Build combined basis set
        let mut basis_set = BasisSet::new("combined");

        // Add empirical patterns
        for p in empirical.to_basis_patterns() {
            basis_set.add_pattern(p, 1.0);
        }

        // Add top PCA components
        for p in pca.to_basis_patterns(5) {
            basis_set.add_pattern(p, 0.5);
        }

        // Add breakout-prone cluster centroids
        for p in clustering.to_basis_patterns(0.3) {
            basis_set.add_pattern(p, 0.7);
        }

        // Add LDA direction
        basis_set.add_pattern(discriminant.to_basis_pattern(), 1.0);

        // Compute normalization parameters
        let vectors: Vec<Vec<f64>> = samples.iter()
            .map(|s| s.features.to_vector())
            .collect();
        let feature_mean = Self::compute_mean(&vectors);
        let feature_std = Self::compute_std(&vectors, &feature_mean);

        Self {
            empirical,
            pca,
            clustering,
            discriminant,
            basis_set,
            method_weights: MethodWeights {
                empirical: 0.3,
                pca: 0.2,
                clustering: 0.2,
                discriminant: 0.3,
            },
            feature_mean,
            feature_std,
        }
    }

    /// Score new sample
    pub fn score(&self, features: &ResearchFeatureVector) -> CombinedScore {
        let normalized = features.normalize(&self.feature_mean, &self.feature_std);
        let normalized_features = ResearchFeatureVector::from_vector(normalized, features.timestamp);

        CombinedScore {
            empirical_bullish: self.empirical.score(&normalized_features).bullish_similarity,
            empirical_bearish: self.empirical.score(&normalized_features).bearish_similarity,
            pca_projection: self.pca.transform(&normalized_features),
            cluster_id: self.clustering.predict_cluster(&normalized_features),
            discriminant_score: self.discriminant.score(&normalized_features),
            breakout_probability: self.discriminant.predict_probability(&normalized_features),
            basis_scores: self.basis_set.compute_scores(&MarketState::from_research_features(&normalized_features)),
        }
    }

    /// Generate trading signal
    pub fn generate_signal(&self, features: &ResearchFeatureVector, threshold: f64) -> Option<TradingSignal> {
        let score = self.score(features);

        if score.breakout_probability > threshold {
            let direction = if score.empirical_bullish > score.empirical_bearish {
                Direction::Up
            } else {
                Direction::Down
            };

            Some(TradingSignal {
                direction,
                confidence: score.breakout_probability,
                method_contributions: MethodContributions {
                    empirical: score.empirical_bullish.max(score.empirical_bearish),
                    pca: score.pca_projection[0],
                    clustering: self.clustering.cluster_breakout_rates[score.cluster_id].bullish_rate.max(
                        self.clustering.cluster_breakout_rates[score.cluster_id].bearish_rate
                    ),
                    discriminant: score.discriminant_score,
                },
            })
        } else {
            None
        }
    }

    /// Save model to file
    pub fn save(&self, path: &Path) -> Result<(), std::io::Error> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)
    }

    /// Load model from file
    pub fn load(path: &Path) -> Result<Self, std::io::Error> {
        let json = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&json)?)
    }
}

pub struct CombinedScore {
    pub empirical_bullish: f64,
    pub empirical_bearish: f64,
    pub pca_projection: Vec<f64>,
    pub cluster_id: usize,
    pub discriminant_score: f64,
    pub breakout_probability: f64,
    pub basis_scores: BreakoutScores,
}

pub struct TradingSignal {
    pub direction: Direction,
    pub confidence: f64,
    pub method_contributions: MethodContributions,
}

pub struct MethodContributions {
    pub empirical: f64,
    pub pca: f64,
    pub clustering: f64,
    pub discriminant: f64,
}
```

### 5.2 Research CLI Tool

```rust
/// Research command-line interface
///
/// Usage:
///   cargo run --bin research -- prepare --data ./data/features --output ./research/dataset.parquet
///   cargo run --bin research -- explore --dataset ./research/dataset.parquet
///   cargo run --bin research -- train --dataset ./research/dataset.parquet --output ./models/pattern_model.json
///   cargo run --bin research -- validate --model ./models/pattern_model.json --dataset ./research/dataset.parquet
///   cargo run --bin research -- report --model ./models/pattern_model.json

#[derive(Parser)]
pub struct ResearchCli {
    #[command(subcommand)]
    command: ResearchCommand,
}

#[derive(Subcommand)]
pub enum ResearchCommand {
    /// Stage 1: Prepare labeled dataset
    Prepare {
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "10")]
        lookahead_minutes: u64,
        #[arg(long, default_value = "0.3")]
        breakout_threshold_pct: f64,
    },

    /// Stage 2: Exploratory analysis
    Explore {
        #[arg(long)]
        dataset: PathBuf,
    },

    /// Stage 3 & 4: Train and validate model
    Train {
        #[arg(long)]
        dataset: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "5")]
        n_folds: usize,
    },

    /// Stage 4: Validate existing model
    Validate {
        #[arg(long)]
        model: PathBuf,
        #[arg(long)]
        dataset: PathBuf,
    },

    /// Generate research report
    Report {
        #[arg(long)]
        model: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}
```

---

## Research Report Format

```
================================================================================
                    PATTERN-BASED BREAKOUT RESEARCH REPORT
================================================================================

Dataset Summary
---------------
Period: 2025-10-16 to 2025-12-17 (62 days)
Total Samples: 45,234
Breakouts: 3,456 (7.6%)
  - Bullish: 1,823 (52.7%)
  - Bearish: 1,633 (47.3%)
Breakout Threshold: 0.30%
Lookahead Window: 10 minutes

Exploratory Analysis
--------------------
Top 10 Predictive Features (by correlation with breakout):
  1. momentum_5min         r = 0.34  ***
  2. tick_entropy_10s      r = -0.28 ***
  3. trade_imbalance       r = 0.25  ***
  4. volatility_10min      r = 0.22  ***
  5. orderbook_imbalance   r = 0.19  ***
  ...

Empirical Analysis
------------------
Bullish Pattern Centroid (top features):
  - momentum_5min: +0.15% (vs -0.02% for no-breakout)
  - tick_entropy: 0.42 (vs 0.68 for no-breakout)
  - trade_imbalance: +12.3% (vs +1.2% for no-breakout)

Pattern Similarity Test:
  - T-statistic: 8.34
  - P-value: < 0.0001 ***
  - Effect size (Cohen's d): 0.52 (medium)

PCA Analysis
------------
Components needed for 95% variance: 23 (of 166)

Top Principal Component Loadings:
  PC1 (18.3% variance): momentum_5min (0.45), price_trend (0.38), ...
  PC2 (12.1% variance): tick_entropy (-0.52), volatility (0.31), ...
  PC3 (8.7% variance): volume_normalized (0.61), trade_count (0.33), ...

Clustering Analysis
-------------------
Optimal K: 8 clusters (elbow method)
Silhouette Score: 0.42

Breakout-Prone Clusters:
  Cluster 3: 234 samples, 45.2% bullish breakout rate (5.9x baseline)
  Cluster 7: 189 samples, 38.1% bearish breakout rate (5.0x baseline)

Cluster 3 Characteristics (bullish-prone):
  - High momentum (0.18% vs 0.02% avg)
  - Low entropy (0.38 vs 0.65 avg)
  - Increasing volume trend

Discriminant Analysis
---------------------
Training Accuracy: 71.3%
Fisher Criterion: 2.34

Top Discriminating Features:
  1. momentum_5min (|w| = 0.45)
  2. tick_entropy (|w| = 0.38)
  3. trade_imbalance (|w| = 0.29)

Walk-Forward Validation (5 folds)
---------------------------------
Mean Accuracy: 68.2% ± 3.1%
Mean Precision: 42.3% ± 4.2%
Mean Recall: 55.1% ± 5.8%
Mean F1 Score: 0.48 ± 0.04

Fold Results:
  Fold 1: Acc=71.2%, Prec=45.1%, Rec=52.3%, F1=0.48
  Fold 2: Acc=65.8%, Prec=38.9%, Rec=58.2%, F1=0.47
  ...

Out-of-Sample Test (held-out 20%)
---------------------------------
Test Period: 2025-12-03 to 2025-12-17
Samples: 9,047

Classification Metrics:
  Accuracy: 66.8%
  Precision: 39.2%
  Recall: 51.3%
  F1 Score: 0.44

Trading Metrics (threshold = 0.7):
  Signals Generated: 423
  Correct Signals: 187 (44.2%)
  Expected P&L per Signal: +4.2 bps
  Estimated Sharpe: 0.82

Conclusions
-----------
1. PATTERN SIGNIFICANCE: Yes - empirical patterns are significantly
   different from random (p < 0.0001, d = 0.52)

2. PREDICTIVE FEATURES: momentum_5min, tick_entropy, and trade_imbalance
   are the strongest predictors of breakouts

3. CLUSTER PATTERNS: Clusters 3 and 7 show 5-6x elevated breakout rates,
   suggesting distinct pre-breakout market states

4. MODEL PERFORMANCE: Combined model achieves 44% signal accuracy,
   which translates to positive expected P&L given typical breakout magnitudes

5. RECOMMENDATION: Deploy MOM_PatternBreakout strategy with:
   - Signal threshold: 0.7
   - Take profit: 30 bps (based on mean breakout magnitude)
   - Stop loss: 15 bps
   - Expected Sharpe: 0.8 (needs live validation)

================================================================================
```

---

## Implementation Checklist

| Stage | Task | Status |
|-------|------|--------|
| 1 | ResearchFeatureVector struct | TODO |
| 1 | DataLabeler (breakout labeling) | TODO |
| 1 | DatasetSplit (train/val/test) | TODO |
| 2 | StatisticalSummary | TODO |
| 2 | CorrelationAnalysis | TODO |
| 2 | TemporalAnalysis | TODO |
| 3 | EmpiricalAnalysis | TODO |
| 3 | PCAAnalysis | TODO |
| 3 | ClusteringAnalysis (K-Means) | TODO |
| 3 | DiscriminantAnalysis (LDA) | TODO |
| 4 | StatisticalTests | TODO |
| 4 | WalkForwardValidation | TODO |
| 4 | OutOfSampleTest | TODO |
| 5 | CombinedPatternModel | TODO |
| 5 | Research CLI tool | TODO |
| 5 | Report generator | TODO |

---

## Summary

This research process answers the question **"When do past patterns predict breakouts?"** through:

1. **Data Preparation**: Build labeled dataset from feature vectors + breakout outcomes
2. **Exploratory Analysis**: Find which features correlate with breakouts
3. **Pattern Discovery**:
   - Empirical: Average pattern before breakouts
   - PCA: Orthogonal directions of variance
   - Clustering: Distinct pattern types
   - Discriminant: Maximum separation direction
4. **Validation**: Statistical tests + walk-forward + out-of-sample
5. **Operationalization**: Combined model for real-time signal generation

The key insight is that **multiple methods provide complementary views** of the same underlying question. By combining them, we get more robust pattern detection than any single method alone.

---

*Document maintained by: Development Team*
*Last updated: December 18, 2025*

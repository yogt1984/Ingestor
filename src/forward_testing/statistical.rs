//! Statistical Testing for Forward Testing
//!
//! Provides statistical tools for comparing strategy performance:
//! - Welch's t-test for comparing means with unequal variances
//! - Bootstrap confidence intervals for robust estimation
//! - Effect size calculation (Cohen's d)
//! - Multiple testing corrections (Bonferroni, Holm)

use std::collections::VecDeque;
use serde::{Deserialize, Serialize};

/// Result of a statistical hypothesis test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HypothesisTestResult {
    /// Test statistic (e.g., t-value)
    pub statistic: f64,
    /// Two-tailed p-value
    pub p_value: f64,
    /// Degrees of freedom (for t-test)
    pub degrees_of_freedom: f64,
    /// Effect size (Cohen's d)
    pub effect_size: f64,
    /// 95% confidence interval for the difference
    pub ci_95: (f64, f64),
    /// Whether the result is statistically significant at alpha=0.05
    pub is_significant: bool,
    /// Interpretation of the result
    pub interpretation: String,
}

impl Default for HypothesisTestResult {
    fn default() -> Self {
        Self {
            statistic: 0.0,
            p_value: 1.0,
            degrees_of_freedom: 0.0,
            effect_size: 0.0,
            ci_95: (0.0, 0.0),
            is_significant: false,
            interpretation: "Insufficient data".to_string(),
        }
    }
}

/// Bootstrap confidence interval result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapCI {
    /// Point estimate (mean of sample)
    pub estimate: f64,
    /// Lower bound of confidence interval
    pub lower: f64,
    /// Upper bound of confidence interval
    pub upper: f64,
    /// Confidence level (e.g., 0.95)
    pub confidence_level: f64,
    /// Number of bootstrap samples used
    pub n_bootstrap: usize,
    /// Standard error of the estimate
    pub std_error: f64,
}

impl Default for BootstrapCI {
    fn default() -> Self {
        Self {
            estimate: 0.0,
            lower: 0.0,
            upper: 0.0,
            confidence_level: 0.95,
            n_bootstrap: 0,
            std_error: 0.0,
        }
    }
}

/// Statistical comparison between two samples
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwoSampleComparison {
    /// Sample A statistics
    pub sample_a: SampleStats,
    /// Sample B statistics
    pub sample_b: SampleStats,
    /// Hypothesis test result
    pub test_result: HypothesisTestResult,
    /// Bootstrap CI for the difference
    pub difference_ci: BootstrapCI,
    /// Practical significance assessment
    pub practical_significance: PracticalSignificance,
}

/// Summary statistics for a sample
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SampleStats {
    /// Sample size
    pub n: usize,
    /// Sample mean
    pub mean: f64,
    /// Sample standard deviation
    pub std_dev: f64,
    /// Sample median
    pub median: f64,
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Skewness
    pub skewness: f64,
    /// Sum of all values
    pub sum: f64,
}

impl SampleStats {
    /// Calculate statistics from a slice of values
    pub fn from_slice(data: &[f64]) -> Self {
        if data.is_empty() {
            return Self::default();
        }

        let n = data.len();
        let sum: f64 = data.iter().sum();
        let mean = sum / n as f64;

        let variance = if n > 1 {
            data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64
        } else {
            0.0
        };
        let std_dev = variance.sqrt();

        let mut sorted = data.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let median = if n % 2 == 0 {
            (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
        } else {
            sorted[n / 2]
        };

        let min = sorted[0];
        let max = sorted[n - 1];

        // Calculate skewness
        let skewness = if std_dev > 0.0 && n > 2 {
            let m3: f64 = data.iter().map(|x| ((x - mean) / std_dev).powi(3)).sum();
            m3 / n as f64
        } else {
            0.0
        };

        Self {
            n,
            mean,
            std_dev,
            median,
            min,
            max,
            skewness,
            sum,
        }
    }
}

/// Practical significance assessment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PracticalSignificance {
    /// Effect size category (negligible, small, medium, large)
    pub effect_category: EffectCategory,
    /// Is the effect practically meaningful?
    pub is_meaningful: bool,
    /// Human-readable interpretation
    pub interpretation: String,
}

/// Effect size categories based on Cohen's d
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum EffectCategory {
    Negligible,
    Small,
    Medium,
    Large,
}

impl EffectCategory {
    /// Categorize Cohen's d effect size
    pub fn from_cohens_d(d: f64) -> Self {
        let abs_d = d.abs();
        if abs_d < 0.2 {
            Self::Negligible
        } else if abs_d < 0.5 {
            Self::Small
        } else if abs_d < 0.8 {
            Self::Medium
        } else {
            Self::Large
        }
    }
}

/// Statistical test engine
pub struct StatisticalTester {
    /// Significance level (alpha)
    alpha: f64,
    /// Number of bootstrap samples
    n_bootstrap: usize,
    /// Random seed for reproducibility
    seed: u64,
}

impl Default for StatisticalTester {
    fn default() -> Self {
        Self {
            alpha: 0.05,
            n_bootstrap: 10_000,
            seed: 42,
        }
    }
}

impl StatisticalTester {
    /// Create a new tester with custom parameters
    pub fn new(alpha: f64, n_bootstrap: usize, seed: u64) -> Self {
        Self {
            alpha,
            n_bootstrap,
            seed,
        }
    }

    /// Perform Welch's t-test (unequal variances t-test)
    ///
    /// Tests H0: mean_a = mean_b vs H1: mean_a != mean_b
    pub fn welch_t_test(&self, sample_a: &[f64], sample_b: &[f64]) -> HypothesisTestResult {
        let stats_a = SampleStats::from_slice(sample_a);
        let stats_b = SampleStats::from_slice(sample_b);

        if stats_a.n < 2 || stats_b.n < 2 {
            return HypothesisTestResult {
                interpretation: format!(
                    "Insufficient sample sizes: n_a={}, n_b={}. Need at least 2 in each group.",
                    stats_a.n, stats_b.n
                ),
                ..Default::default()
            };
        }

        let n1 = stats_a.n as f64;
        let n2 = stats_b.n as f64;
        let var1 = stats_a.std_dev.powi(2);
        let var2 = stats_b.std_dev.powi(2);

        // Welch's t-statistic
        let se = (var1 / n1 + var2 / n2).sqrt();
        if se == 0.0 {
            return HypothesisTestResult {
                interpretation: "Zero variance in both samples - cannot compute t-test".to_string(),
                ..Default::default()
            };
        }

        let t_stat = (stats_a.mean - stats_b.mean) / se;

        // Welch-Satterthwaite degrees of freedom
        let df_num = (var1 / n1 + var2 / n2).powi(2);
        let df_denom = (var1 / n1).powi(2) / (n1 - 1.0) + (var2 / n2).powi(2) / (n2 - 1.0);
        let df = if df_denom > 0.0 {
            df_num / df_denom
        } else {
            n1 + n2 - 2.0
        };

        // Calculate p-value using t-distribution approximation
        let p_value = self.t_cdf_two_tailed(t_stat, df);

        // Cohen's d effect size
        let pooled_std = ((var1 * (n1 - 1.0) + var2 * (n2 - 1.0)) / (n1 + n2 - 2.0)).sqrt();
        let effect_size = if pooled_std > 0.0 {
            (stats_a.mean - stats_b.mean) / pooled_std
        } else {
            0.0
        };

        // 95% CI for the difference
        let t_crit = self.t_critical(df, self.alpha);
        let diff = stats_a.mean - stats_b.mean;
        let ci_95 = (diff - t_crit * se, diff + t_crit * se);

        let is_significant = p_value < self.alpha;

        let interpretation = if is_significant {
            format!(
                "Statistically significant difference (p={:.4}). Sample A mean ({:.4}) {} sample B mean ({:.4}).",
                p_value,
                stats_a.mean,
                if stats_a.mean > stats_b.mean { ">" } else { "<" },
                stats_b.mean
            )
        } else {
            format!(
                "No statistically significant difference (p={:.4}). Cannot reject H0 at alpha={:.2}.",
                p_value, self.alpha
            )
        };

        HypothesisTestResult {
            statistic: t_stat,
            p_value,
            degrees_of_freedom: df,
            effect_size,
            ci_95,
            is_significant,
            interpretation,
        }
    }

    /// Perform a paired t-test for dependent samples
    ///
    /// Tests H0: mean_diff = 0 vs H1: mean_diff != 0
    pub fn paired_t_test(&self, sample_a: &[f64], sample_b: &[f64]) -> HypothesisTestResult {
        if sample_a.len() != sample_b.len() {
            return HypothesisTestResult {
                interpretation: "Paired t-test requires equal sample sizes".to_string(),
                ..Default::default()
            };
        }

        let differences: Vec<f64> = sample_a
            .iter()
            .zip(sample_b.iter())
            .map(|(a, b)| a - b)
            .collect();

        let stats = SampleStats::from_slice(&differences);

        if stats.n < 2 {
            return HypothesisTestResult {
                interpretation: "Insufficient sample size for paired t-test".to_string(),
                ..Default::default()
            };
        }

        let se = stats.std_dev / (stats.n as f64).sqrt();
        if se == 0.0 {
            return HypothesisTestResult {
                interpretation: "Zero variance in differences".to_string(),
                ..Default::default()
            };
        }

        let t_stat = stats.mean / se;
        let df = (stats.n - 1) as f64;
        let p_value = self.t_cdf_two_tailed(t_stat, df);

        let effect_size = if stats.std_dev > 0.0 {
            stats.mean / stats.std_dev
        } else {
            0.0
        };

        let t_crit = self.t_critical(df, self.alpha);
        let ci_95 = (stats.mean - t_crit * se, stats.mean + t_crit * se);

        let is_significant = p_value < self.alpha;

        let interpretation = if is_significant {
            format!(
                "Significant difference in paired samples (p={:.4}). Mean difference: {:.4}",
                p_value, stats.mean
            )
        } else {
            format!(
                "No significant difference in paired samples (p={:.4})",
                p_value
            )
        };

        HypothesisTestResult {
            statistic: t_stat,
            p_value,
            degrees_of_freedom: df,
            effect_size,
            ci_95,
            is_significant,
            interpretation,
        }
    }

    /// Calculate bootstrap confidence interval for the mean
    pub fn bootstrap_ci(&self, data: &[f64], confidence_level: f64) -> BootstrapCI {
        if data.is_empty() {
            return BootstrapCI::default();
        }

        let n = data.len();
        let stats = SampleStats::from_slice(data);

        // Generate bootstrap samples
        let mut bootstrap_means = Vec::with_capacity(self.n_bootstrap);
        let mut rng = SimpleRng::new(self.seed);

        for _ in 0..self.n_bootstrap {
            let mut sum = 0.0;
            for _ in 0..n {
                let idx = rng.next_usize() % n;
                sum += data[idx];
            }
            bootstrap_means.push(sum / n as f64);
        }

        // Sort bootstrap means
        bootstrap_means.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Calculate percentile CI
        let alpha = 1.0 - confidence_level;
        let lower_idx = ((alpha / 2.0) * self.n_bootstrap as f64).floor() as usize;
        let upper_idx = ((1.0 - alpha / 2.0) * self.n_bootstrap as f64).ceil() as usize;

        let lower = bootstrap_means.get(lower_idx).copied().unwrap_or(stats.mean);
        let upper = bootstrap_means
            .get(upper_idx.min(self.n_bootstrap - 1))
            .copied()
            .unwrap_or(stats.mean);

        // Standard error from bootstrap
        let bootstrap_stats = SampleStats::from_slice(&bootstrap_means);

        BootstrapCI {
            estimate: stats.mean,
            lower,
            upper,
            confidence_level,
            n_bootstrap: self.n_bootstrap,
            std_error: bootstrap_stats.std_dev,
        }
    }

    /// Bootstrap CI for the difference between two samples
    pub fn bootstrap_difference_ci(
        &self,
        sample_a: &[f64],
        sample_b: &[f64],
        confidence_level: f64,
    ) -> BootstrapCI {
        if sample_a.is_empty() || sample_b.is_empty() {
            return BootstrapCI::default();
        }

        let n_a = sample_a.len();
        let n_b = sample_b.len();

        let stats_a = SampleStats::from_slice(sample_a);
        let stats_b = SampleStats::from_slice(sample_b);
        let observed_diff = stats_a.mean - stats_b.mean;

        // Generate bootstrap samples
        let mut bootstrap_diffs = Vec::with_capacity(self.n_bootstrap);
        let mut rng = SimpleRng::new(self.seed);

        for _ in 0..self.n_bootstrap {
            // Resample A
            let mut sum_a = 0.0;
            for _ in 0..n_a {
                let idx = rng.next_usize() % n_a;
                sum_a += sample_a[idx];
            }
            let mean_a = sum_a / n_a as f64;

            // Resample B
            let mut sum_b = 0.0;
            for _ in 0..n_b {
                let idx = rng.next_usize() % n_b;
                sum_b += sample_b[idx];
            }
            let mean_b = sum_b / n_b as f64;

            bootstrap_diffs.push(mean_a - mean_b);
        }

        // Sort bootstrap differences
        bootstrap_diffs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Calculate percentile CI
        let alpha = 1.0 - confidence_level;
        let lower_idx = ((alpha / 2.0) * self.n_bootstrap as f64).floor() as usize;
        let upper_idx = ((1.0 - alpha / 2.0) * self.n_bootstrap as f64).ceil() as usize;

        let lower = bootstrap_diffs
            .get(lower_idx)
            .copied()
            .unwrap_or(observed_diff);
        let upper = bootstrap_diffs
            .get(upper_idx.min(self.n_bootstrap - 1))
            .copied()
            .unwrap_or(observed_diff);

        let bootstrap_stats = SampleStats::from_slice(&bootstrap_diffs);

        BootstrapCI {
            estimate: observed_diff,
            lower,
            upper,
            confidence_level,
            n_bootstrap: self.n_bootstrap,
            std_error: bootstrap_stats.std_dev,
        }
    }

    /// Full two-sample comparison with all statistics
    pub fn compare_samples(&self, sample_a: &[f64], sample_b: &[f64]) -> TwoSampleComparison {
        let stats_a = SampleStats::from_slice(sample_a);
        let stats_b = SampleStats::from_slice(sample_b);

        let test_result = self.welch_t_test(sample_a, sample_b);
        let difference_ci = self.bootstrap_difference_ci(sample_a, sample_b, 0.95);

        let effect_category = EffectCategory::from_cohens_d(test_result.effect_size);

        let practical_significance = PracticalSignificance {
            effect_category,
            is_meaningful: matches!(effect_category, EffectCategory::Medium | EffectCategory::Large),
            interpretation: match effect_category {
                EffectCategory::Negligible => {
                    "Effect size is negligible - no practical difference".to_string()
                }
                EffectCategory::Small => {
                    "Small effect size - difference may not be practically meaningful".to_string()
                }
                EffectCategory::Medium => {
                    "Medium effect size - difference is likely practically meaningful".to_string()
                }
                EffectCategory::Large => {
                    "Large effect size - substantial practical difference".to_string()
                }
            },
        };

        TwoSampleComparison {
            sample_a: stats_a,
            sample_b: stats_b,
            test_result,
            difference_ci,
            practical_significance,
        }
    }

    /// Apply Bonferroni correction for multiple comparisons
    pub fn bonferroni_correction(&self, p_values: &[f64]) -> Vec<f64> {
        let n = p_values.len() as f64;
        p_values.iter().map(|p| (p * n).min(1.0)).collect()
    }

    /// Apply Holm-Bonferroni correction (step-down procedure)
    pub fn holm_correction(&self, p_values: &[f64]) -> Vec<f64> {
        let n = p_values.len();
        if n == 0 {
            return Vec::new();
        }

        // Sort p-values while keeping track of original indices
        let mut indexed: Vec<(usize, f64)> = p_values.iter().copied().enumerate().collect();
        indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Apply Holm correction
        let mut adjusted = vec![0.0; n];
        let mut prev_adjusted = 0.0;

        for (rank, (orig_idx, p)) in indexed.into_iter().enumerate() {
            let multiplier = (n - rank) as f64;
            let adj = (p * multiplier).min(1.0).max(prev_adjusted);
            adjusted[orig_idx] = adj;
            prev_adjusted = adj;
        }

        adjusted
    }

    // Helper: Approximate t-distribution CDF (two-tailed p-value)
    fn t_cdf_two_tailed(&self, t: f64, df: f64) -> f64 {
        // Use approximation for t-distribution
        // Based on Abramowitz and Stegun approximation
        let x = df / (df + t * t);
        let beta = self.incomplete_beta(df / 2.0, 0.5, x);
        beta // This gives two-tailed p-value directly
    }

    // Helper: Incomplete beta function approximation
    fn incomplete_beta(&self, a: f64, b: f64, x: f64) -> f64 {
        // Simple continued fraction approximation
        if x == 0.0 {
            return 0.0;
        }
        if x == 1.0 {
            return 1.0;
        }

        // Use regularized incomplete beta function approximation
        // This is a simplified version - production would use a proper library
        let bt = if x == 0.0 || x == 1.0 {
            0.0
        } else {
            (self.ln_gamma(a + b) - self.ln_gamma(a) - self.ln_gamma(b)
                + a * x.ln()
                + b * (1.0 - x).ln())
            .exp()
        };

        // Continued fraction approximation
        if x < (a + 1.0) / (a + b + 2.0) {
            bt * self.beta_cf(a, b, x) / a
        } else {
            1.0 - bt * self.beta_cf(b, a, 1.0 - x) / b
        }
    }

    // Helper: Continued fraction for incomplete beta
    fn beta_cf(&self, a: f64, b: f64, x: f64) -> f64 {
        let max_iter = 100;
        let eps = 1e-10;

        let mut c = 1.0;
        let mut d = 1.0 / (1.0 - (a + b) * x / (a + 1.0)).max(eps);
        let mut h = d;

        for m in 1..=max_iter {
            let m = m as f64;

            // Even step
            let aa = m * (b - m) * x / ((a + 2.0 * m - 1.0) * (a + 2.0 * m));
            d = 1.0 / (1.0 + aa * d).max(eps);
            c = (1.0 + aa / c).max(eps);
            h *= d * c;

            // Odd step
            let aa = -(a + m) * (a + b + m) * x / ((a + 2.0 * m) * (a + 2.0 * m + 1.0));
            d = 1.0 / (1.0 + aa * d).max(eps);
            c = (1.0 + aa / c).max(eps);
            let delta = d * c;
            h *= delta;

            if (delta - 1.0).abs() < eps {
                break;
            }
        }

        h
    }

    // Helper: Log gamma function approximation (Stirling)
    fn ln_gamma(&self, x: f64) -> f64 {
        // Lanczos approximation coefficients
        let g = 7.0;
        let c = [
            0.99999999999980993,
            676.5203681218851,
            -1259.1392167224028,
            771.32342877765313,
            -176.61502916214059,
            12.507343278686905,
            -0.13857109526572012,
            9.9843695780195716e-6,
            1.5056327351493116e-7,
        ];

        if x < 0.5 {
            std::f64::consts::PI.ln() - (std::f64::consts::PI * x).sin().ln() - self.ln_gamma(1.0 - x)
        } else {
            let x = x - 1.0;
            let mut a = c[0];
            for (i, &coef) in c.iter().enumerate().skip(1) {
                a += coef / (x + i as f64);
            }
            let t = x + g + 0.5;
            0.5 * (2.0 * std::f64::consts::PI).ln() + (t.ln() * (x + 0.5)) - t + a.ln()
        }
    }

    // Helper: Critical t-value for confidence interval
    fn t_critical(&self, df: f64, alpha: f64) -> f64 {
        // Approximation for t critical value
        // Uses Hill's approximation
        let a = 1.0 / (df - 0.5);
        let b = 48.0 / (a * a);
        let c = ((20700.0 * a / b - 98.0) * a - 16.0) * a + 96.36;
        let d = ((94.5 / (b + c) - 3.0) / b + 1.0) * (1.0 / (1.0 - alpha / 2.0)).ln().sqrt() * df.sqrt();

        d.min(10.0) // Cap at reasonable value
    }
}

/// Simple RNG for bootstrap sampling (LCG)
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // LCG parameters
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }

    fn next_usize(&mut self) -> usize {
        self.next_u64() as usize
    }
}

/// Rolling statistics tracker for online updates
#[derive(Debug, Clone, Default)]
pub struct RollingStats {
    values: VecDeque<f64>,
    window_size: usize,
    sum: f64,
    sum_sq: f64,
}

impl RollingStats {
    /// Create a new rolling statistics tracker with given window size
    pub fn new(window_size: usize) -> Self {
        Self {
            values: VecDeque::with_capacity(window_size + 1),
            window_size,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    /// Add a new value
    pub fn push(&mut self, value: f64) {
        self.values.push_back(value);
        self.sum += value;
        self.sum_sq += value * value;

        // Remove old value if window is full
        if self.values.len() > self.window_size {
            if let Some(old) = self.values.pop_front() {
                self.sum -= old;
                self.sum_sq -= old * old;
            }
        }
    }

    /// Get current count
    pub fn count(&self) -> usize {
        self.values.len()
    }

    /// Get mean
    pub fn mean(&self) -> f64 {
        if self.values.is_empty() {
            0.0
        } else {
            self.sum / self.values.len() as f64
        }
    }

    /// Get variance
    pub fn variance(&self) -> f64 {
        let n = self.values.len() as f64;
        if n < 2.0 {
            0.0
        } else {
            let mean = self.mean();
            (self.sum_sq - n * mean * mean) / (n - 1.0)
        }
    }

    /// Get standard deviation
    pub fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }

    /// Get all values as a slice
    pub fn values(&self) -> Vec<f64> {
        self.values.iter().copied().collect()
    }

    /// Clear all values
    pub fn clear(&mut self) {
        self.values.clear();
        self.sum = 0.0;
        self.sum_sq = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== SampleStats Tests ====================

    #[test]
    fn test_sample_stats_empty() {
        let stats = SampleStats::from_slice(&[]);
        assert_eq!(stats.n, 0);
        assert_eq!(stats.mean, 0.0);
        assert_eq!(stats.std_dev, 0.0);
    }

    #[test]
    fn test_sample_stats_single_value() {
        let stats = SampleStats::from_slice(&[5.0]);
        assert_eq!(stats.n, 1);
        assert_eq!(stats.mean, 5.0);
        assert_eq!(stats.std_dev, 0.0); // Can't compute std with n=1
        assert_eq!(stats.median, 5.0);
        assert_eq!(stats.min, 5.0);
        assert_eq!(stats.max, 5.0);
    }

    #[test]
    fn test_sample_stats_two_values() {
        let stats = SampleStats::from_slice(&[0.0, 10.0]);
        assert_eq!(stats.n, 2);
        assert_eq!(stats.mean, 5.0);
        assert_eq!(stats.median, 5.0);
        assert_eq!(stats.min, 0.0);
        assert_eq!(stats.max, 10.0);
    }

    #[test]
    fn test_sample_stats_known_values() {
        // Known dataset: [1, 2, 3, 4, 5]
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let stats = SampleStats::from_slice(&data);

        assert_eq!(stats.n, 5);
        assert!((stats.mean - 3.0).abs() < 1e-10);
        assert!((stats.median - 3.0).abs() < 1e-10);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 5.0);
        assert_eq!(stats.sum, 15.0);

        // Sample std dev = sqrt(sum((x - mean)^2) / (n-1))
        // = sqrt((4+1+0+1+4)/4) = sqrt(10/4) = sqrt(2.5) ≈ 1.5811
        assert!((stats.std_dev - 1.5811388300841898).abs() < 1e-10);
    }

    #[test]
    fn test_sample_stats_even_median() {
        // Even number of elements
        let data = vec![1.0, 2.0, 3.0, 4.0];
        let stats = SampleStats::from_slice(&data);
        assert!((stats.median - 2.5).abs() < 1e-10);
    }

    #[test]
    fn test_sample_stats_negative_values() {
        let data = vec![-5.0, -3.0, -1.0, 1.0, 3.0, 5.0];
        let stats = SampleStats::from_slice(&data);

        assert_eq!(stats.n, 6);
        assert!((stats.mean - 0.0).abs() < 1e-10);
        assert_eq!(stats.min, -5.0);
        assert_eq!(stats.max, 5.0);
    }

    #[test]
    fn test_sample_stats_constant_values() {
        let data = vec![7.0, 7.0, 7.0, 7.0];
        let stats = SampleStats::from_slice(&data);

        assert_eq!(stats.mean, 7.0);
        assert_eq!(stats.std_dev, 0.0);
        assert_eq!(stats.median, 7.0);
    }

    // ==================== T-Test Tests ====================

    #[test]
    fn test_welch_t_test_insufficient_data() {
        let tester = StatisticalTester::default();

        // Only one value in each sample
        let result = tester.welch_t_test(&[1.0], &[2.0]);
        assert!(!result.is_significant);
        assert!(result.interpretation.contains("Insufficient"));
    }

    #[test]
    fn test_welch_t_test_identical_samples() {
        let tester = StatisticalTester::default();

        let sample = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = tester.welch_t_test(&sample, &sample);

        // t-statistic should be 0, p-value should be 1
        assert!((result.statistic - 0.0).abs() < 1e-10);
        assert!(!result.is_significant);
    }

    #[test]
    fn test_welch_t_test_clearly_different() {
        let tester = StatisticalTester::default();

        // Two clearly different distributions
        let sample_a: Vec<f64> = (0..50).map(|x| x as f64).collect();
        let sample_b: Vec<f64> = (100..150).map(|x| x as f64).collect();

        let result = tester.welch_t_test(&sample_a, &sample_b);

        assert!(result.is_significant);
        assert!(result.p_value < 0.001);
        assert!(result.statistic < 0.0); // A is less than B
    }

    #[test]
    fn test_welch_t_test_effect_size() {
        let tester = StatisticalTester::default();

        // Large effect size
        let sample_a = vec![0.0, 1.0, 2.0, 3.0, 4.0];
        let sample_b = vec![10.0, 11.0, 12.0, 13.0, 14.0];

        let result = tester.welch_t_test(&sample_a, &sample_b);

        // Effect size should be large
        assert!(result.effect_size.abs() > 2.0); // Very large effect
    }

    #[test]
    fn test_welch_t_test_confidence_interval() {
        let tester = StatisticalTester::default();

        let sample_a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let sample_b = vec![2.0, 3.0, 4.0, 5.0, 6.0];

        let result = tester.welch_t_test(&sample_a, &sample_b);

        // CI should contain the true difference (-1.0)
        assert!(result.ci_95.0 <= -1.0);
        assert!(result.ci_95.1 >= -1.0);
    }

    #[test]
    fn test_welch_t_test_unequal_variances() {
        let tester = StatisticalTester::default();

        // Different variances
        let sample_a = vec![1.0, 1.1, 1.0, 0.9, 1.0]; // Low variance
        let sample_b = vec![0.0, 2.0, 1.0, -1.0, 3.0]; // High variance

        let result = tester.welch_t_test(&sample_a, &sample_b);

        // Should still compute valid result
        assert!(result.degrees_of_freedom > 0.0);
        assert!(result.p_value >= 0.0 && result.p_value <= 1.0);
    }

    // ==================== Paired T-Test Tests ====================

    #[test]
    fn test_paired_t_test_unequal_lengths() {
        let tester = StatisticalTester::default();

        let result = tester.paired_t_test(&[1.0, 2.0, 3.0], &[1.0, 2.0]);
        assert!(result.interpretation.contains("equal sample sizes"));
    }

    #[test]
    fn test_paired_t_test_no_difference() {
        let tester = StatisticalTester::default();

        let sample_a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let sample_b = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        let result = tester.paired_t_test(&sample_a, &sample_b);

        // No difference
        assert!((result.statistic - 0.0).abs() < 1e-10 || result.interpretation.contains("Zero variance"));
    }

    #[test]
    fn test_paired_t_test_systematic_difference() {
        let tester = StatisticalTester::default();

        // B is consistently higher than A with some variation in differences
        // Differences: -9, -10, -11, -10, -10 (varying slightly to have non-zero variance)
        let sample_a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let sample_b = vec![10.0, 12.0, 14.0, 14.0, 15.0];

        let result = tester.paired_t_test(&sample_a, &sample_b);

        // With non-zero variance in differences, this should be significant
        assert!(result.is_significant);
        // Effect size should be large
        assert!(result.effect_size.abs() > 1.0);
    }

    // ==================== Bootstrap CI Tests ====================

    #[test]
    fn test_bootstrap_ci_empty() {
        let tester = StatisticalTester::default();
        let result = tester.bootstrap_ci(&[], 0.95);
        assert_eq!(result.n_bootstrap, 0);
    }

    #[test]
    fn test_bootstrap_ci_single_value() {
        let tester = StatisticalTester::new(0.05, 1000, 42);
        let result = tester.bootstrap_ci(&[5.0], 0.95);

        // Single value: CI should be very narrow around 5.0
        assert!((result.estimate - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_bootstrap_ci_known_distribution() {
        let tester = StatisticalTester::new(0.05, 10_000, 42);

        // Large sample from known distribution
        let data: Vec<f64> = (0..100).map(|x| x as f64).collect();
        let result = tester.bootstrap_ci(&data, 0.95);

        // Mean should be 49.5
        assert!((result.estimate - 49.5).abs() < 0.1);

        // CI should contain the true mean
        assert!(result.lower < 49.5);
        assert!(result.upper > 49.5);
    }

    #[test]
    fn test_bootstrap_ci_confidence_level() {
        let tester = StatisticalTester::new(0.05, 5000, 42);

        let data: Vec<f64> = (0..50).map(|x| x as f64).collect();

        let ci_90 = tester.bootstrap_ci(&data, 0.90);
        let ci_95 = tester.bootstrap_ci(&data, 0.95);
        let ci_99 = tester.bootstrap_ci(&data, 0.99);

        // Higher confidence = wider interval
        let width_90 = ci_90.upper - ci_90.lower;
        let width_95 = ci_95.upper - ci_95.lower;
        let width_99 = ci_99.upper - ci_99.lower;

        assert!(width_90 <= width_95);
        assert!(width_95 <= width_99);
    }

    #[test]
    fn test_bootstrap_difference_ci() {
        let tester = StatisticalTester::new(0.05, 5000, 42);

        let sample_a: Vec<f64> = (0..30).map(|x| x as f64).collect();
        let sample_b: Vec<f64> = (10..40).map(|x| x as f64).collect();

        let result = tester.bootstrap_difference_ci(&sample_a, &sample_b, 0.95);

        // True difference is -10
        assert!(result.lower < -10.0);
        assert!(result.upper > -10.0);
    }

    // ==================== Effect Size Tests ====================

    #[test]
    fn test_effect_category_negligible() {
        assert_eq!(EffectCategory::from_cohens_d(0.0), EffectCategory::Negligible);
        assert_eq!(EffectCategory::from_cohens_d(0.1), EffectCategory::Negligible);
        assert_eq!(EffectCategory::from_cohens_d(-0.15), EffectCategory::Negligible);
    }

    #[test]
    fn test_effect_category_small() {
        assert_eq!(EffectCategory::from_cohens_d(0.2), EffectCategory::Small);
        assert_eq!(EffectCategory::from_cohens_d(0.4), EffectCategory::Small);
        assert_eq!(EffectCategory::from_cohens_d(-0.3), EffectCategory::Small);
    }

    #[test]
    fn test_effect_category_medium() {
        assert_eq!(EffectCategory::from_cohens_d(0.5), EffectCategory::Medium);
        assert_eq!(EffectCategory::from_cohens_d(0.7), EffectCategory::Medium);
        assert_eq!(EffectCategory::from_cohens_d(-0.6), EffectCategory::Medium);
    }

    #[test]
    fn test_effect_category_large() {
        assert_eq!(EffectCategory::from_cohens_d(0.8), EffectCategory::Large);
        assert_eq!(EffectCategory::from_cohens_d(1.5), EffectCategory::Large);
        assert_eq!(EffectCategory::from_cohens_d(-1.0), EffectCategory::Large);
    }

    // ==================== Multiple Testing Correction Tests ====================

    #[test]
    fn test_bonferroni_correction() {
        let tester = StatisticalTester::default();

        let p_values = vec![0.01, 0.02, 0.03, 0.04, 0.05];
        let adjusted = tester.bonferroni_correction(&p_values);

        // Should be multiplied by 5
        assert!((adjusted[0] - 0.05).abs() < 1e-10);
        assert!((adjusted[1] - 0.10).abs() < 1e-10);
        assert!((adjusted[4] - 0.25).abs() < 1e-10);
    }

    #[test]
    fn test_bonferroni_caps_at_one() {
        let tester = StatisticalTester::default();

        let p_values = vec![0.5, 0.6, 0.7];
        let adjusted = tester.bonferroni_correction(&p_values);

        // All should be capped at 1.0
        for p in adjusted {
            assert!(p <= 1.0);
        }
    }

    #[test]
    fn test_holm_correction() {
        let tester = StatisticalTester::default();

        let p_values = vec![0.01, 0.04, 0.03, 0.02]; // Out of order
        let adjusted = tester.holm_correction(&p_values);

        // Holm procedure should maintain ordering
        assert!(adjusted.len() == 4);

        // All adjusted p-values should be valid
        for p in &adjusted {
            assert!(*p >= 0.0 && *p <= 1.0);
        }
    }

    #[test]
    fn test_holm_correction_empty() {
        let tester = StatisticalTester::default();
        let adjusted = tester.holm_correction(&[]);
        assert!(adjusted.is_empty());
    }

    #[test]
    fn test_holm_less_conservative_than_bonferroni() {
        let tester = StatisticalTester::default();

        let p_values = vec![0.01, 0.02, 0.03, 0.04, 0.05];
        let bonferroni = tester.bonferroni_correction(&p_values);
        let holm = tester.holm_correction(&p_values);

        // Holm should be less conservative (smaller adjusted p-values on average)
        let bonf_sum: f64 = bonferroni.iter().sum();
        let holm_sum: f64 = holm.iter().sum();

        assert!(holm_sum <= bonf_sum);
    }

    // ==================== Rolling Stats Tests ====================

    #[test]
    fn test_rolling_stats_empty() {
        let stats = RollingStats::new(10);
        assert_eq!(stats.count(), 0);
        assert_eq!(stats.mean(), 0.0);
        assert_eq!(stats.variance(), 0.0);
    }

    #[test]
    fn test_rolling_stats_basic() {
        let mut stats = RollingStats::new(5);

        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);

        assert_eq!(stats.count(), 3);
        assert!((stats.mean() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_rolling_stats_window_overflow() {
        let mut stats = RollingStats::new(3);

        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);
        stats.push(4.0); // Should remove 1.0
        stats.push(5.0); // Should remove 2.0

        assert_eq!(stats.count(), 3);
        // Window should now be [3, 4, 5]
        assert!((stats.mean() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_rolling_stats_variance() {
        let mut stats = RollingStats::new(5);

        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);
        stats.push(4.0);
        stats.push(5.0);

        // Variance of [1,2,3,4,5] with sample variance = 2.5
        assert!((stats.variance() - 2.5).abs() < 1e-10);
        assert!((stats.std_dev() - 2.5_f64.sqrt()).abs() < 1e-10);
    }

    #[test]
    fn test_rolling_stats_clear() {
        let mut stats = RollingStats::new(10);

        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);

        stats.clear();

        assert_eq!(stats.count(), 0);
        assert_eq!(stats.mean(), 0.0);
    }

    #[test]
    fn test_rolling_stats_values() {
        let mut stats = RollingStats::new(5);

        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);

        let values = stats.values();
        assert_eq!(values, vec![1.0, 2.0, 3.0]);
    }

    // ==================== Two-Sample Comparison Tests ====================

    #[test]
    fn test_compare_samples_comprehensive() {
        let tester = StatisticalTester::new(0.05, 1000, 42);

        let sample_a: Vec<f64> = (0..30).map(|x| x as f64).collect();
        let sample_b: Vec<f64> = (20..50).map(|x| x as f64).collect();

        let result = tester.compare_samples(&sample_a, &sample_b);

        assert_eq!(result.sample_a.n, 30);
        assert_eq!(result.sample_b.n, 30);
        assert!(result.test_result.is_significant);
    }

    #[test]
    fn test_compare_samples_practical_significance() {
        let tester = StatisticalTester::default();

        // Negligible effect
        let sample_a = vec![1.0, 1.1, 0.9, 1.0, 1.05];
        let sample_b = vec![1.0, 1.05, 0.95, 1.0, 1.02];

        let result = tester.compare_samples(&sample_a, &sample_b);

        // Effect should be negligible or small
        assert!(matches!(
            result.practical_significance.effect_category,
            EffectCategory::Negligible | EffectCategory::Small
        ));
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_t_test_zero_variance_in_one_sample() {
        let tester = StatisticalTester::default();

        let sample_a = vec![5.0, 5.0, 5.0, 5.0, 5.0]; // Zero variance
        let sample_b = vec![1.0, 2.0, 3.0, 4.0, 5.0]; // Non-zero variance

        let result = tester.welch_t_test(&sample_a, &sample_b);

        // Should handle gracefully
        assert!(result.p_value >= 0.0 && result.p_value <= 1.0);
    }

    #[test]
    fn test_t_test_very_small_sample() {
        let tester = StatisticalTester::default();

        let sample_a = vec![1.0, 2.0];
        let sample_b = vec![3.0, 4.0];

        let result = tester.welch_t_test(&sample_a, &sample_b);

        // Should compute but with low power
        assert!(result.degrees_of_freedom > 0.0);
    }

    #[test]
    fn test_bootstrap_reproducibility() {
        let tester1 = StatisticalTester::new(0.05, 1000, 42);
        let tester2 = StatisticalTester::new(0.05, 1000, 42);

        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        let result1 = tester1.bootstrap_ci(&data, 0.95);
        let result2 = tester2.bootstrap_ci(&data, 0.95);

        // Same seed should give same result
        assert!((result1.lower - result2.lower).abs() < 1e-10);
        assert!((result1.upper - result2.upper).abs() < 1e-10);
    }

    #[test]
    fn test_hypothesis_test_result_default() {
        let result = HypothesisTestResult::default();
        assert_eq!(result.p_value, 1.0);
        assert!(!result.is_significant);
    }

    #[test]
    fn test_sample_stats_skewness() {
        // Positively skewed distribution
        let data = vec![1.0, 1.5, 2.0, 2.5, 10.0];
        let stats = SampleStats::from_slice(&data);

        assert!(stats.skewness > 0.0); // Should be positively skewed
    }

    #[test]
    fn test_rolling_stats_single_value_variance() {
        let mut stats = RollingStats::new(10);
        stats.push(5.0);

        // With n=1, variance should be 0
        assert_eq!(stats.variance(), 0.0);
    }

    #[test]
    fn test_practical_significance_meaningful() {
        let ps = PracticalSignificance {
            effect_category: EffectCategory::Large,
            is_meaningful: true,
            interpretation: "Large effect".to_string(),
        };

        assert!(ps.is_meaningful);
    }
}

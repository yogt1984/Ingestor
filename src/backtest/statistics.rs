//! Statistical Significance Testing for Backtest Results
//!
//! Provides rigorous statistical analysis to determine if strategy performance
//! is genuine or due to random chance.
//!
//! # Metrics Implemented
//!
//! - **Probabilistic Sharpe Ratio (PSR)**: Probability that the true Sharpe ratio > 0
//! - **Deflated Sharpe Ratio (DSR)**: Adjusts for multiple hypothesis testing
//! - **Minimum Track Record Length (minTRL)**: Required trades to trust the Sharpe
//! - **Bootstrap Confidence Intervals**: Non-parametric error bounds
//!
//! # References
//!
//! - Bailey, D.H. & Lopez de Prado, M. (2012). "The Sharpe Ratio Efficient Frontier"
//! - Bailey, D.H. & Lopez de Prado, M. (2014). "The Deflated Sharpe Ratio:
//!   Correcting for Selection Bias, Backtest Overfitting and Non-Normality"
//! - Lopez de Prado, M. (2018). "Advances in Financial Machine Learning", Chapter 14
//!
//! # Usage
//!
//! ```ignore
//! use crate::backtest::statistics::{StatisticalReport, compute_statistics};
//!
//! let report = compute_statistics(&backtest_results, num_trials);
//! report.print();
//! ```

use serde::{Deserialize, Serialize};

/// Statistical significance report for backtest results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalReport {
    /// Observed Sharpe ratio from backtest
    pub sharpe_ratio: f64,
    /// Standard error of Sharpe ratio estimate
    pub sharpe_std_error: f64,
    /// Number of trades/observations
    pub num_trades: usize,
    /// Skewness of returns
    pub skewness: f64,
    /// Excess kurtosis of returns
    pub kurtosis: f64,

    // Core metrics
    /// Probabilistic Sharpe Ratio: P(true SR > 0)
    pub probabilistic_sharpe: f64,
    /// Deflated Sharpe Ratio (adjusted for multiple testing)
    pub deflated_sharpe: f64,
    /// Number of independent trials/backtests conducted
    pub num_trials: usize,
    /// Minimum track record length (trades needed)
    pub min_track_record_length: usize,
    /// Whether we have sufficient data
    pub has_sufficient_data: bool,

    // Bootstrap confidence intervals
    pub bootstrap_sharpe_lower: f64,
    pub bootstrap_sharpe_upper: f64,
    pub bootstrap_return_lower: f64,
    pub bootstrap_return_upper: f64,
    pub bootstrap_drawdown_lower: f64,
    pub bootstrap_drawdown_upper: f64,

    // Overall verdict
    pub verdict: SignificanceVerdict,
}

/// Verdict on statistical significance
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SignificanceVerdict {
    /// Strong evidence of positive edge (PSR > 0.95)
    SignificantPositive,
    /// Weak evidence of positive edge (PSR 0.75-0.95)
    WeakPositive,
    /// Insufficient evidence either way (PSR 0.25-0.75)
    Inconclusive,
    /// Weak evidence of negative edge (PSR 0.05-0.25)
    WeakNegative,
    /// Strong evidence of negative edge (PSR < 0.05)
    SignificantNegative,
    /// Not enough data to make determination
    InsufficientData,
}

impl std::fmt::Display for SignificanceVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignificanceVerdict::SignificantPositive => write!(f, "SIGNIFICANT POSITIVE EDGE"),
            SignificanceVerdict::WeakPositive => write!(f, "WEAK POSITIVE EVIDENCE"),
            SignificanceVerdict::Inconclusive => write!(f, "INCONCLUSIVE"),
            SignificanceVerdict::WeakNegative => write!(f, "WEAK NEGATIVE EVIDENCE"),
            SignificanceVerdict::SignificantNegative => write!(f, "SIGNIFICANT NEGATIVE"),
            SignificanceVerdict::InsufficientData => write!(f, "INSUFFICIENT DATA"),
        }
    }
}

impl StatisticalReport {
    /// Print formatted statistical significance report
    pub fn print(&self) {
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("           STATISTICAL SIGNIFICANCE REPORT             ");
        println!("═══════════════════════════════════════════════════════");
        println!();
        println!("OBSERVED METRICS");
        println!("  Sharpe Ratio:        {:+.3}", self.sharpe_ratio);
        println!("  Standard Error:      ±{:.3}", self.sharpe_std_error);
        println!("  Number of Trades:    {}", self.num_trades);
        println!("  Skewness:            {:+.3}", self.skewness);
        println!("  Excess Kurtosis:     {:+.3}", self.kurtosis);
        println!();
        println!("SIGNIFICANCE TESTS");
        println!("  Probabilistic Sharpe Ratio (PSR): {:.1}%",
            self.probabilistic_sharpe * 100.0);
        println!("    → Probability that true Sharpe > 0");
        println!();
        if self.num_trials > 1 {
            println!("  Deflated Sharpe Ratio (DSR):      {:+.3}", self.deflated_sharpe);
            println!("    → Adjusted for {} independent trials", self.num_trials);
            println!();
        }
        println!("  Minimum Track Record Length:      {} trades", self.min_track_record_length);
        println!("    → Have {} trades: {}",
            self.num_trades,
            if self.has_sufficient_data { "SUFFICIENT" } else { "INSUFFICIENT" });
        println!();
        println!("BOOTSTRAP 95% CONFIDENCE INTERVALS");
        println!("  Sharpe Ratio:   [{:+.3}, {:+.3}]",
            self.bootstrap_sharpe_lower, self.bootstrap_sharpe_upper);
        println!("  Total Return:   [{:+.2}%, {:+.2}%]",
            self.bootstrap_return_lower * 100.0, self.bootstrap_return_upper * 100.0);
        println!("  Max Drawdown:   [{:.2}%, {:.2}%]",
            self.bootstrap_drawdown_lower * 100.0, self.bootstrap_drawdown_upper * 100.0);
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("VERDICT: {}", self.verdict);
        println!("═══════════════════════════════════════════════════════");
        println!();
        println!("References:");
        println!("  - Bailey & Lopez de Prado (2012) \"The Sharpe Ratio Efficient Frontier\"");
        println!("  - Bailey & Lopez de Prado (2014) \"The Deflated Sharpe Ratio\"");
        println!("  - Lopez de Prado (2018) \"Advances in Financial Machine Learning\" Ch.14");
        println!();
    }
}

/// Compute statistical significance metrics from trade returns
pub fn compute_statistics(
    trade_returns: &[f64],
    total_return: f64,
    max_drawdown: f64,
    sharpe_ratio: f64,
    num_trials: usize,
) -> StatisticalReport {
    let n = trade_returns.len();

    if n < 10 {
        return StatisticalReport {
            sharpe_ratio,
            sharpe_std_error: f64::NAN,
            num_trades: n,
            skewness: 0.0,
            kurtosis: 0.0,
            probabilistic_sharpe: 0.5,
            deflated_sharpe: sharpe_ratio,
            num_trials,
            min_track_record_length: usize::MAX,
            has_sufficient_data: false,
            bootstrap_sharpe_lower: sharpe_ratio,
            bootstrap_sharpe_upper: sharpe_ratio,
            bootstrap_return_lower: total_return,
            bootstrap_return_upper: total_return,
            bootstrap_drawdown_lower: max_drawdown,
            bootstrap_drawdown_upper: max_drawdown,
            verdict: SignificanceVerdict::InsufficientData,
        };
    }

    // Compute moments of return distribution
    let (mean, std_dev) = mean_and_std(trade_returns);
    let skewness = compute_skewness(trade_returns, mean, std_dev);
    let kurtosis = compute_kurtosis(trade_returns, mean, std_dev);

    // Standard error of Sharpe ratio (Lo, 2002)
    // SE(SR) = sqrt((1 + 0.5*SR^2 - skew*SR + (kurt-3)/4 * SR^2) / n)
    let sr_squared = sharpe_ratio * sharpe_ratio;
    let se_variance = (1.0 + 0.5 * sr_squared
        - skewness * sharpe_ratio
        + (kurtosis - 3.0) / 4.0 * sr_squared) / n as f64;
    let sharpe_std_error = se_variance.max(0.0).sqrt();

    // Probabilistic Sharpe Ratio: P(SR* > 0 | SR)
    // PSR = Φ((SR - 0) / SE(SR)) = Φ(SR / SE(SR))
    let psr = if sharpe_std_error > 0.0 {
        normal_cdf(sharpe_ratio / sharpe_std_error)
    } else {
        0.5
    };

    // Deflated Sharpe Ratio (adjusted for multiple testing)
    // DSR adjusts for the expected maximum Sharpe from N independent trials
    let dsr = compute_deflated_sharpe(sharpe_ratio, sharpe_std_error, n, num_trials);

    // Minimum Track Record Length
    // minTRL = (1 + (1 - skew*SR + (kurt-1)/4 * SR^2)) * (z_α / SR)^2
    let min_trl = compute_min_track_record_length(sharpe_ratio, skewness, kurtosis, 0.05);
    let has_sufficient_data = n >= min_trl;

    // Bootstrap confidence intervals
    let (sharpe_lo, sharpe_hi) = bootstrap_ci(trade_returns, 1000, 0.05, |r| {
        let (m, s) = mean_and_std(r);
        if s > 0.0 { m / s * (252.0_f64).sqrt() } else { 0.0 }
    });

    let (return_lo, return_hi) = bootstrap_ci(trade_returns, 1000, 0.05, |r| {
        r.iter().sum::<f64>()
    });

    // For drawdown, we need to simulate equity curves
    let (dd_lo, dd_hi) = bootstrap_drawdown_ci(trade_returns, 1000, 0.05);

    // Determine verdict
    let verdict = if !has_sufficient_data {
        SignificanceVerdict::InsufficientData
    } else if psr > 0.95 {
        SignificanceVerdict::SignificantPositive
    } else if psr > 0.75 {
        SignificanceVerdict::WeakPositive
    } else if psr > 0.25 {
        SignificanceVerdict::Inconclusive
    } else if psr > 0.05 {
        SignificanceVerdict::WeakNegative
    } else {
        SignificanceVerdict::SignificantNegative
    };

    StatisticalReport {
        sharpe_ratio,
        sharpe_std_error,
        num_trades: n,
        skewness,
        kurtosis,
        probabilistic_sharpe: psr,
        deflated_sharpe: dsr,
        num_trials,
        min_track_record_length: min_trl,
        has_sufficient_data,
        bootstrap_sharpe_lower: sharpe_lo,
        bootstrap_sharpe_upper: sharpe_hi,
        bootstrap_return_lower: return_lo,
        bootstrap_return_upper: return_hi,
        bootstrap_drawdown_lower: dd_lo,
        bootstrap_drawdown_upper: dd_hi,
        verdict,
    }
}

/// Compute mean and standard deviation
fn mean_and_std(data: &[f64]) -> (f64, f64) {
    if data.is_empty() {
        return (0.0, 0.0);
    }
    let n = data.len() as f64;
    let mean = data.iter().sum::<f64>() / n;
    let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0);
    (mean, variance.sqrt())
}

/// Compute skewness
fn compute_skewness(data: &[f64], mean: f64, std: f64) -> f64 {
    if data.len() < 3 || std == 0.0 {
        return 0.0;
    }
    let n = data.len() as f64;
    let m3 = data.iter().map(|x| ((x - mean) / std).powi(3)).sum::<f64>() / n;
    // Adjusted Fisher-Pearson standardized moment coefficient
    let adjustment = ((n * (n - 1.0)).sqrt()) / (n - 2.0);
    m3 * adjustment
}

/// Compute excess kurtosis
fn compute_kurtosis(data: &[f64], mean: f64, std: f64) -> f64 {
    if data.len() < 4 || std == 0.0 {
        return 0.0;
    }
    let n = data.len() as f64;
    let m4 = data.iter().map(|x| ((x - mean) / std).powi(4)).sum::<f64>() / n;
    // Excess kurtosis (normal = 0)
    let adjustment = (n - 1.0) / ((n - 2.0) * (n - 3.0));
    ((n + 1.0) * m4 - 3.0 * (n - 1.0)) * adjustment
}

/// Standard normal CDF approximation (Abramowitz and Stegun)
fn normal_cdf(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs() / (2.0_f64).sqrt();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    0.5 * (1.0 + sign * y)
}

/// Inverse normal CDF (approximation)
/// Using Abramowitz and Stegun approximation 26.2.23
fn normal_quantile(p: f64) -> f64 {
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }
    if (p - 0.5).abs() < 1e-10 {
        return 0.0;
    }

    let sign = if p > 0.5 { 1.0 } else { -1.0 };
    let p_adj = if p > 0.5 { 1.0 - p } else { p };

    let t = (-2.0 * p_adj.ln()).sqrt();

    // Coefficients for rational approximation
    let c0 = 2.515517;
    let c1 = 0.802853;
    let c2 = 0.010328;
    let d1 = 1.432788;
    let d2 = 0.189269;
    let d3 = 0.001308;

    let x = t - (c0 + c1 * t + c2 * t * t) / (1.0 + d1 * t + d2 * t * t + d3 * t * t * t);

    sign * x
}

/// Compute Deflated Sharpe Ratio
/// Adjusts for expected maximum from multiple independent trials
fn compute_deflated_sharpe(
    sharpe: f64,
    sharpe_se: f64,
    _n_obs: usize,
    n_trials: usize,
) -> f64 {
    if n_trials <= 1 || sharpe_se <= 0.0 {
        return sharpe;
    }

    // Expected maximum Sharpe from N independent trials
    // E[max(SR_1, ..., SR_N)] ≈ (1 - γ) * Φ^{-1}(1 - 1/N) + γ * Φ^{-1}(1 - 1/(N*e))
    // where γ ≈ 0.5772 is Euler-Mascheroni constant
    let gamma = 0.5772156649;
    let n = n_trials as f64;

    let q1 = normal_quantile(1.0 - 1.0 / n);
    let q2 = normal_quantile(1.0 - 1.0 / (n * std::f64::consts::E));
    let expected_max_sr = (1.0 - gamma) * q1 + gamma * q2;

    // Scale by SE to get expected max in SR units
    let expected_max = expected_max_sr * sharpe_se;

    // DSR = (SR - E[max]) / SE
    // This gives the z-score relative to the expected maximum
    (sharpe - expected_max) / sharpe_se
}

/// Compute Minimum Track Record Length
/// Based on Bailey & Lopez de Prado (2012)
fn compute_min_track_record_length(
    sharpe: f64,
    skewness: f64,
    kurtosis: f64,
    alpha: f64,
) -> usize {
    if sharpe.abs() < 1e-10 {
        return usize::MAX; // Infinite if SR = 0
    }

    let z_alpha = normal_quantile(1.0 - alpha / 2.0).abs();
    let sr_sq = sharpe * sharpe;

    // minTRL = (1 + (1 - skew*SR + (kurt-1)/4 * SR^2)) * (z / SR)^2
    let adjustment = 1.0 + (1.0 - skewness * sharpe + (kurtosis - 1.0) / 4.0 * sr_sq);
    let min_trl = adjustment * (z_alpha / sharpe).powi(2);

    min_trl.ceil().max(10.0) as usize
}

/// Bootstrap confidence interval
fn bootstrap_ci<F>(data: &[f64], n_samples: usize, alpha: f64, statistic: F) -> (f64, f64)
where
    F: Fn(&[f64]) -> f64,
{
    if data.len() < 10 {
        let stat = statistic(data);
        return (stat, stat);
    }

    let mut stats: Vec<f64> = Vec::with_capacity(n_samples);
    let n = data.len();

    // Simple deterministic bootstrap (for reproducibility)
    // Uses a simple LCG for index selection
    let mut seed: u64 = 12345;

    for _ in 0..n_samples {
        let mut sample: Vec<f64> = Vec::with_capacity(n);
        for _ in 0..n {
            seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            let idx = (seed as usize) % n;
            sample.push(data[idx]);
        }
        stats.push(statistic(&sample));
    }

    stats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let lower_idx = ((alpha / 2.0) * n_samples as f64) as usize;
    let upper_idx = ((1.0 - alpha / 2.0) * n_samples as f64) as usize;

    let lower_idx = lower_idx.min(stats.len() - 1);
    let upper_idx = upper_idx.min(stats.len() - 1);

    (stats[lower_idx], stats[upper_idx])
}

/// Bootstrap confidence interval for maximum drawdown
fn bootstrap_drawdown_ci(data: &[f64], n_samples: usize, alpha: f64) -> (f64, f64) {
    if data.len() < 10 {
        return (0.0, 0.0);
    }

    let compute_drawdown = |returns: &[f64]| -> f64 {
        let mut equity: f64 = 1.0;
        let mut peak: f64 = 1.0;
        let mut max_dd: f64 = 0.0;

        for &r in returns {
            equity *= 1.0 + r;
            peak = peak.max(equity);
            let dd = (peak - equity) / peak;
            max_dd = max_dd.max(dd);
        }
        max_dd
    };

    bootstrap_ci(data, n_samples, alpha, compute_drawdown)
}

/// Extract per-trade returns from trade log
pub fn extract_trade_returns(trade_pnls: &[f64], trade_notionals: &[f64]) -> Vec<f64> {
    trade_pnls.iter()
        .zip(trade_notionals.iter())
        .filter(|(_, n)| **n > 0.0)
        .map(|(p, n)| p / n)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean_and_std() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let (mean, std) = mean_and_std(&data);
        assert!((mean - 3.0).abs() < 1e-10);
        assert!((std - 1.5811388).abs() < 1e-5);
    }

    #[test]
    fn test_normal_cdf() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((normal_cdf(1.96) - 0.975).abs() < 0.01);
        assert!((normal_cdf(-1.96) - 0.025).abs() < 0.01);
    }

    #[test]
    fn test_normal_quantile() {
        assert!((normal_quantile(0.5) - 0.0).abs() < 1e-6);
        // The approximation gives ~1.96 for 0.975
        let q975 = normal_quantile(0.975);
        assert!(q975 > 1.5 && q975 < 2.5, "Expected ~1.96, got {}", q975);
        let q025 = normal_quantile(0.025);
        assert!(q025 < -1.5 && q025 > -2.5, "Expected ~-1.96, got {}", q025);
    }

    #[test]
    fn test_compute_skewness() {
        // Symmetric distribution should have ~0 skewness
        let data: Vec<f64> = (-10..=10).map(|x| x as f64).collect();
        let (mean, std) = mean_and_std(&data);
        let skew = compute_skewness(&data, mean, std);
        assert!(skew.abs() < 0.1);
    }

    #[test]
    fn test_compute_statistics_insufficient_data() {
        let returns = vec![0.01, 0.02, -0.01];
        let report = compute_statistics(&returns, 0.02, 0.01, 0.5, 1);
        assert_eq!(report.verdict, SignificanceVerdict::InsufficientData);
    }

    #[test]
    fn test_compute_statistics_positive() {
        // Positive returns with some variance (realistic trading returns)
        let returns: Vec<f64> = (0..100).map(|i| {
            // Oscillating returns with positive bias
            let noise = ((i as f64) * 0.7).sin() * 0.002;
            0.003 + noise
        }).collect();
        let sharpe = 2.0;
        let report = compute_statistics(&returns, 0.15, 0.02, sharpe, 1);

        // Report should be generated successfully
        assert!(report.num_trades == 100);
        // PSR with high positive Sharpe should be at least 0.5
        assert!(report.probabilistic_sharpe >= 0.5,
            "Expected PSR >= 0.5, got {}", report.probabilistic_sharpe);
    }

    #[test]
    fn test_compute_statistics_negative() {
        // Negative returns with some variance
        let returns: Vec<f64> = (0..100).map(|i| {
            let noise = ((i as f64) * 0.7).sin() * 0.001;
            -0.003 + noise
        }).collect();
        let sharpe = -2.0;
        let report = compute_statistics(&returns, -0.20, 0.30, sharpe, 1);

        // PSR with negative Sharpe should be at most 0.5
        assert!(report.probabilistic_sharpe <= 0.5,
            "Expected PSR <= 0.5, got {}", report.probabilistic_sharpe);
    }

    #[test]
    fn test_min_track_record_length() {
        // High Sharpe needs fewer observations
        let min_trl_high = compute_min_track_record_length(2.0, 0.0, 3.0, 0.05);
        let min_trl_low = compute_min_track_record_length(0.5, 0.0, 3.0, 0.05);

        assert!(min_trl_high < min_trl_low);
    }

    #[test]
    fn test_deflated_sharpe_single_trial() {
        // With single trial, DSR should equal SR
        let dsr = compute_deflated_sharpe(1.5, 0.3, 100, 1);
        assert!((dsr - 1.5).abs() < 1e-10);
    }

    #[test]
    fn test_deflated_sharpe_multiple_trials() {
        // With multiple trials, DSR should be different from SR
        // (typically lower due to expected max adjustment)
        let dsr = compute_deflated_sharpe(1.5, 0.3, 100, 100);
        // The DSR adjusts for multiple testing bias
        assert!(dsr != 1.5, "DSR should differ from SR with multiple trials");
    }

    #[test]
    fn test_bootstrap_ci() {
        let data: Vec<f64> = (0..100).map(|x| x as f64 / 100.0).collect();
        let (lo, hi) = bootstrap_ci(&data, 100, 0.05, |d| {
            d.iter().sum::<f64>() / d.len() as f64
        });

        // Mean is ~0.5, CI should contain it
        assert!(lo < 0.5);
        assert!(hi > 0.5);
    }

    #[test]
    fn test_verdict_display() {
        assert_eq!(
            format!("{}", SignificanceVerdict::SignificantPositive),
            "SIGNIFICANT POSITIVE EDGE"
        );
        assert_eq!(
            format!("{}", SignificanceVerdict::InsufficientData),
            "INSUFFICIENT DATA"
        );
    }
}

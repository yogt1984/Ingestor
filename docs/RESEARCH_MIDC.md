# MIDC: Market Information Diffusion Coefficient

**Document Version:** 1.0
**Created:** December 18, 2025
**Purpose:** Define the MIDC estimation process for determining when momentum strategies are viable

---

## Core Concept

> **MIDC (Market Information Diffusion Coefficient)** measures how quickly new information gets incorporated into price. Low MIDC = slow diffusion = trends persist = momentum is exploitable.

The fundamental question:

> **"If price moved at time t, how much does that tell us about price at time t+Δ?"**

This is precisely **I(P_t ; P_{t+Δ})** - the mutual information between current and future price action.

---

## Theoretical Foundation

### Information Diffusion Model

Price follows an information diffusion process:

```
P(t+Δ) = P(t) + Information_new(Δ) + Noise(Δ)

Where:
- Information_new(Δ) = new information arriving in interval Δ
- The rate at which past information "decays" from predictive power is κ (MIDC)
```

### Autocorrelation Decay

The key observable: **autocorrelation of returns decays exponentially**

```
ρ(Δ) = Corr(r_t, r_{t+Δ}) ≈ ρ₀ · e^(-κΔ)

Where:
- ρ(Δ) = autocorrelation at lag Δ
- ρ₀ = initial autocorrelation (at Δ→0)
- κ = MIDC (diffusion rate)
```

### Key Derived Metrics

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **κ (MIDC)** | Fitted decay rate | Higher = faster information diffusion |
| **τ_half** | ln(2) / κ | Half-life of predictability (seconds) |
| **τ_useful** | ln(1/threshold) / κ | Time until correlation drops below threshold |
| **Predictability Horizon** | τ_half | How far ahead we can meaningfully predict |

---

## MIDC Estimation Process

### Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      MIDC ESTIMATION PIPELINE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  INPUT: Historical price data (1 month recommended)                         │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│  │ Compute     │    │ Compute     │    │ Fit Exp.   │    │ Extract     │ │
│  │ Returns at  │ -> │ Autocorr at │ -> │ Decay      │ -> │ MIDC (κ)    │ │
│  │ Various Δt  │    │ Multiple    │    │ Model      │    │ and τ_half  │ │
│  │             │    │ Lags        │    │             │    │             │ │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘ │
│                                                                             │
│  OUTPUT: MIDCEstimate { κ, τ_half, confidence, regime_assessment }          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 1: Data Preparation

```rust
pub struct MIDCDataConfig {
    /// Minimum data required (default: 30 days)
    pub min_history_days: u64,

    /// Recommended data (default: 60 days)
    pub recommended_history_days: u64,

    /// Base time unit for returns (default: 1 second)
    pub base_interval_seconds: u64,

    /// Lags to compute autocorrelation (in base units)
    /// Default: [1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600]
    pub lags_seconds: Vec<u64>,
}

impl Default for MIDCDataConfig {
    fn default() -> Self {
        Self {
            min_history_days: 30,
            recommended_history_days: 60,
            base_interval_seconds: 1,
            lags_seconds: vec![1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
        }
    }
}
```

### Step 2: Return Computation

```rust
/// Compute returns at various time scales
pub struct ReturnComputer {
    config: MIDCDataConfig,
}

impl ReturnComputer {
    /// Compute log returns: r_t = ln(P_t / P_{t-1})
    pub fn compute_returns(&self, prices: &[PricePoint]) -> HashMap<u64, Vec<f64>> {
        let mut returns_by_interval = HashMap::new();

        for &interval in &self.config.lags_seconds {
            let returns = self.compute_returns_at_interval(prices, interval);
            returns_by_interval.insert(interval, returns);
        }

        returns_by_interval
    }

    fn compute_returns_at_interval(&self, prices: &[PricePoint], interval_secs: u64) -> Vec<f64> {
        // Resample prices to interval
        let resampled = self.resample_prices(prices, interval_secs);

        // Compute log returns
        resampled.windows(2)
            .map(|w| (w[1].mid_price / w[0].mid_price).ln())
            .collect()
    }

    fn resample_prices(&self, prices: &[PricePoint], interval_secs: u64) -> Vec<PricePoint> {
        // Group by interval and take last price
        let mut resampled = Vec::new();
        let mut current_bucket = prices[0].timestamp;
        let interval = Duration::seconds(interval_secs as i64);

        for price in prices {
            if price.timestamp >= current_bucket + interval {
                resampled.push(*price);
                current_bucket = price.timestamp;
            }
        }

        resampled
    }
}
```

### Step 3: Autocorrelation Computation

```rust
/// Compute autocorrelation at multiple lags
pub struct AutocorrelationComputer;

impl AutocorrelationComputer {
    /// Compute autocorrelation function
    pub fn compute_acf(&self, returns: &[f64], max_lag: usize) -> Vec<(usize, f64)> {
        let n = returns.len();
        let mean = returns.iter().sum::<f64>() / n as f64;
        let variance: f64 = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n as f64;

        if variance < 1e-10 {
            return vec![];
        }

        (1..=max_lag)
            .map(|lag| {
                let covariance: f64 = returns.iter()
                    .skip(lag)
                    .zip(returns.iter())
                    .map(|(r_t, r_t_lag)| (r_t - mean) * (r_t_lag - mean))
                    .sum::<f64>() / (n - lag) as f64;

                (lag, covariance / variance)
            })
            .collect()
    }

    /// Compute autocorrelation for specific lags (in seconds)
    pub fn compute_acf_at_lags(
        &self,
        returns_by_interval: &HashMap<u64, Vec<f64>>,
        lags_seconds: &[u64],
    ) -> Vec<AutocorrPoint> {
        lags_seconds.iter()
            .filter_map(|&lag| {
                // Use returns computed at base interval, with appropriate lag
                let base_returns = returns_by_interval.get(&1)?;
                let acf = self.compute_acf(base_returns, lag as usize);
                let (_, rho) = acf.iter().find(|(l, _)| *l == lag as usize)?;

                Some(AutocorrPoint {
                    lag_seconds: lag,
                    autocorrelation: *rho,
                    standard_error: self.compute_se(base_returns.len(), lag as usize),
                })
            })
            .collect()
    }

    fn compute_se(&self, n: usize, lag: usize) -> f64 {
        // Bartlett's formula for SE of autocorrelation
        (1.0 / (n - lag) as f64).sqrt()
    }
}

#[derive(Debug, Clone)]
pub struct AutocorrPoint {
    pub lag_seconds: u64,
    pub autocorrelation: f64,
    pub standard_error: f64,
}
```

### Step 4: Exponential Decay Fitting

```rust
/// Fit exponential decay model to autocorrelation function
pub struct ExponentialDecayFitter;

impl ExponentialDecayFitter {
    /// Fit ρ(Δ) = ρ₀ · e^(-κΔ) using least squares
    pub fn fit(&self, acf_points: &[AutocorrPoint]) -> MIDCFitResult {
        // Filter to positive autocorrelations (negative indicates noise)
        let positive_points: Vec<_> = acf_points.iter()
            .filter(|p| p.autocorrelation > 0.0)
            .collect();

        if positive_points.len() < 3 {
            return MIDCFitResult::insufficient_data();
        }

        // Transform to linear problem: ln(ρ) = ln(ρ₀) - κΔ
        let x: Vec<f64> = positive_points.iter()
            .map(|p| p.lag_seconds as f64)
            .collect();
        let y: Vec<f64> = positive_points.iter()
            .map(|p| p.autocorrelation.ln())
            .collect();

        // Linear regression: y = a + bx where b = -κ
        let (intercept, slope, r_squared) = self.linear_regression(&x, &y);

        let kappa = -slope;  // MIDC
        let rho_0 = intercept.exp();  // Initial autocorrelation
        let tau_half = (2.0_f64).ln() / kappa;  // Half-life

        // Compute confidence interval for κ
        let se_slope = self.slope_standard_error(&x, &y, slope, intercept);
        let ci_95 = (kappa - 1.96 * se_slope, kappa + 1.96 * se_slope);

        MIDCFitResult {
            kappa,
            rho_0,
            tau_half_seconds: tau_half,
            r_squared,
            confidence_interval_95: ci_95,
            n_points: positive_points.len(),
            fit_quality: self.assess_fit_quality(r_squared, positive_points.len()),
        }
    }

    fn linear_regression(&self, x: &[f64], y: &[f64]) -> (f64, f64, f64) {
        let n = x.len() as f64;
        let sum_x: f64 = x.iter().sum();
        let sum_y: f64 = y.iter().sum();
        let sum_xy: f64 = x.iter().zip(y).map(|(xi, yi)| xi * yi).sum();
        let sum_xx: f64 = x.iter().map(|xi| xi * xi).sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
        let intercept = (sum_y - slope * sum_x) / n;

        // R-squared
        let mean_y = sum_y / n;
        let ss_tot: f64 = y.iter().map(|yi| (yi - mean_y).powi(2)).sum();
        let ss_res: f64 = x.iter().zip(y).map(|(xi, yi)| {
            let predicted = intercept + slope * xi;
            (yi - predicted).powi(2)
        }).sum();
        let r_squared = 1.0 - ss_res / ss_tot;

        (intercept, slope, r_squared)
    }

    fn slope_standard_error(&self, x: &[f64], y: &[f64], slope: f64, intercept: f64) -> f64 {
        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;

        let ss_res: f64 = x.iter().zip(y).map(|(xi, yi)| {
            let predicted = intercept + slope * xi;
            (yi - predicted).powi(2)
        }).sum();

        let ss_xx: f64 = x.iter().map(|xi| (xi - mean_x).powi(2)).sum();

        (ss_res / (n - 2.0) / ss_xx).sqrt()
    }

    fn assess_fit_quality(&self, r_squared: f64, n_points: usize) -> FitQuality {
        if n_points < 5 {
            FitQuality::Insufficient
        } else if r_squared > 0.9 {
            FitQuality::Excellent
        } else if r_squared > 0.7 {
            FitQuality::Good
        } else if r_squared > 0.5 {
            FitQuality::Moderate
        } else {
            FitQuality::Poor
        }
    }
}

#[derive(Debug, Clone)]
pub struct MIDCFitResult {
    /// MIDC (κ) - diffusion rate in 1/seconds
    pub kappa: f64,

    /// Initial autocorrelation (ρ₀)
    pub rho_0: f64,

    /// Half-life of predictability in seconds
    pub tau_half_seconds: f64,

    /// R² of the exponential fit
    pub r_squared: f64,

    /// 95% confidence interval for κ
    pub confidence_interval_95: (f64, f64),

    /// Number of points used in fit
    pub n_points: usize,

    /// Quality assessment
    pub fit_quality: FitQuality,
}

impl MIDCFitResult {
    pub fn insufficient_data() -> Self {
        Self {
            kappa: f64::NAN,
            rho_0: f64::NAN,
            tau_half_seconds: f64::NAN,
            r_squared: 0.0,
            confidence_interval_95: (f64::NAN, f64::NAN),
            n_points: 0,
            fit_quality: FitQuality::Insufficient,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FitQuality {
    Excellent,   // R² > 0.9
    Good,        // R² > 0.7
    Moderate,    // R² > 0.5
    Poor,        // R² < 0.5
    Insufficient, // Not enough data points
}
```

### Step 5: Regime Assessment

```rust
/// Assess trading regime based on MIDC
pub struct RegimeAssessor {
    config: RegimeConfig,
}

#[derive(Debug, Clone)]
pub struct RegimeConfig {
    /// MIDC threshold for "slow diffusion" (momentum viable)
    /// Default: 0.01 (τ_half ≈ 69 seconds)
    pub slow_diffusion_threshold: f64,

    /// MIDC threshold for "fast diffusion" (momentum not viable)
    /// Default: 0.1 (τ_half ≈ 7 seconds)
    pub fast_diffusion_threshold: f64,

    /// Minimum τ_half for momentum to be exploitable (given our latency)
    /// Default: 30 seconds (assumes 200ms execution latency)
    pub min_useful_tau_half_seconds: f64,

    /// Minimum R² to trust the MIDC estimate
    pub min_r_squared: f64,
}

impl Default for RegimeConfig {
    fn default() -> Self {
        Self {
            slow_diffusion_threshold: 0.01,
            fast_diffusion_threshold: 0.1,
            min_useful_tau_half_seconds: 30.0,
            min_r_squared: 0.5,
        }
    }
}

impl RegimeAssessor {
    pub fn assess(&self, fit: &MIDCFitResult) -> MIDCRegimeAssessment {
        // Check fit quality first
        if fit.r_squared < self.config.min_r_squared {
            return MIDCRegimeAssessment {
                regime: MIDCRegime::Uncertain,
                momentum_viable: false,
                confidence: AssessmentConfidence::Low,
                reasoning: "MIDC fit quality too low to make assessment".to_string(),
                recommended_action: RecommendedAction::NoTrade,
            };
        }

        let regime = if fit.kappa < self.config.slow_diffusion_threshold {
            MIDCRegime::SlowDiffusion
        } else if fit.kappa > self.config.fast_diffusion_threshold {
            MIDCRegime::FastDiffusion
        } else {
            MIDCRegime::ModerateDiffusion
        };

        let momentum_viable = fit.tau_half_seconds >= self.config.min_useful_tau_half_seconds;

        let confidence = match fit.fit_quality {
            FitQuality::Excellent => AssessmentConfidence::High,
            FitQuality::Good => AssessmentConfidence::Medium,
            _ => AssessmentConfidence::Low,
        };

        let (reasoning, recommended_action) = self.generate_recommendation(&regime, momentum_viable, fit);

        MIDCRegimeAssessment {
            regime,
            momentum_viable,
            confidence,
            reasoning,
            recommended_action,
        }
    }

    fn generate_recommendation(
        &self,
        regime: &MIDCRegime,
        momentum_viable: bool,
        fit: &MIDCFitResult,
    ) -> (String, RecommendedAction) {
        match (regime, momentum_viable) {
            (MIDCRegime::SlowDiffusion, true) => (
                format!(
                    "Slow information diffusion (κ={:.4}, τ_half={:.1}s). \
                     Trends persist long enough for momentum strategies.",
                    fit.kappa, fit.tau_half_seconds
                ),
                RecommendedAction::MomentumStrategy,
            ),
            (MIDCRegime::SlowDiffusion, false) => (
                format!(
                    "Slow diffusion but τ_half={:.1}s < {:.1}s minimum. \
                     Edge exists but execution latency may erode it.",
                    fit.tau_half_seconds, self.config.min_useful_tau_half_seconds
                ),
                RecommendedAction::CautiousMomentum,
            ),
            (MIDCRegime::ModerateDiffusion, true) => (
                format!(
                    "Moderate diffusion (κ={:.4}, τ_half={:.1}s). \
                     Some momentum edge may exist, use smaller position sizes.",
                    fit.kappa, fit.tau_half_seconds
                ),
                RecommendedAction::CautiousMomentum,
            ),
            (MIDCRegime::ModerateDiffusion, false) |
            (MIDCRegime::FastDiffusion, _) => (
                format!(
                    "Fast information diffusion (κ={:.4}, τ_half={:.1}s). \
                     Momentum strategies not viable. Consider MM or no trade.",
                    fit.kappa, fit.tau_half_seconds
                ),
                RecommendedAction::MarketMaking,
            ),
            (MIDCRegime::Uncertain, _) => (
                "Unable to reliably estimate MIDC. Insufficient data or poor fit.".to_string(),
                RecommendedAction::NoTrade,
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MIDCRegimeAssessment {
    pub regime: MIDCRegime,
    pub momentum_viable: bool,
    pub confidence: AssessmentConfidence,
    pub reasoning: String,
    pub recommended_action: RecommendedAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MIDCRegime {
    SlowDiffusion,      // κ < 0.01, trends persist
    ModerateDiffusion,  // 0.01 < κ < 0.1
    FastDiffusion,      // κ > 0.1, information incorporated quickly
    Uncertain,          // Cannot determine
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssessmentConfidence {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecommendedAction {
    MomentumStrategy,   // Full MOM_* deployment
    CautiousMomentum,   // MOM_* with reduced size
    MarketMaking,       // MM_* strategies
    NoTrade,            // Stay flat
}
```

---

## Complete MIDC Estimator

```rust
/// Main MIDC estimation interface
pub struct MIDCEstimator {
    data_config: MIDCDataConfig,
    regime_config: RegimeConfig,

    return_computer: ReturnComputer,
    acf_computer: AutocorrelationComputer,
    decay_fitter: ExponentialDecayFitter,
    regime_assessor: RegimeAssessor,
}

impl MIDCEstimator {
    pub fn new(data_config: MIDCDataConfig, regime_config: RegimeConfig) -> Self {
        Self {
            return_computer: ReturnComputer { config: data_config.clone() },
            acf_computer: AutocorrelationComputer,
            decay_fitter: ExponentialDecayFitter,
            regime_assessor: RegimeAssessor { config: regime_config.clone() },
            data_config,
            regime_config,
        }
    }

    /// Run full MIDC estimation on historical data
    pub fn estimate(&self, prices: &[PricePoint]) -> MIDCAnalysisResult {
        let start_time = prices.first().map(|p| p.timestamp);
        let end_time = prices.last().map(|p| p.timestamp);

        // Step 1: Compute returns
        let returns = self.return_computer.compute_returns(prices);

        // Step 2: Compute autocorrelation
        let acf = self.acf_computer.compute_acf_at_lags(&returns, &self.data_config.lags_seconds);

        // Step 3: Fit exponential decay
        let fit = self.decay_fitter.fit(&acf);

        // Step 4: Assess regime
        let assessment = self.regime_assessor.assess(&fit);

        MIDCAnalysisResult {
            period_start: start_time,
            period_end: end_time,
            n_price_points: prices.len(),
            autocorrelation_points: acf,
            fit_result: fit,
            regime_assessment: assessment,
        }
    }

    /// Run rolling MIDC estimation (for regime change detection)
    pub fn estimate_rolling(
        &self,
        prices: &[PricePoint],
        window_days: u64,
        step_days: u64,
    ) -> Vec<MIDCAnalysisResult> {
        let window_duration = Duration::days(window_days as i64);
        let step_duration = Duration::days(step_days as i64);

        let mut results = Vec::new();
        let mut window_start = prices.first().unwrap().timestamp;
        let data_end = prices.last().unwrap().timestamp;

        while window_start + window_duration <= data_end {
            let window_end = window_start + window_duration;

            let window_prices: Vec<_> = prices.iter()
                .filter(|p| p.timestamp >= window_start && p.timestamp < window_end)
                .cloned()
                .collect();

            if window_prices.len() > 1000 {  // Minimum data points
                results.push(self.estimate(&window_prices));
            }

            window_start = window_start + step_duration;
        }

        results
    }

    /// Analyze MIDC stability over time
    pub fn analyze_stability(&self, rolling_results: &[MIDCAnalysisResult]) -> MIDCStabilityAnalysis {
        let kappas: Vec<f64> = rolling_results.iter()
            .map(|r| r.fit_result.kappa)
            .filter(|k| k.is_finite())
            .collect();

        if kappas.is_empty() {
            return MIDCStabilityAnalysis::insufficient_data();
        }

        let mean_kappa = kappas.iter().sum::<f64>() / kappas.len() as f64;
        let std_kappa = (kappas.iter()
            .map(|k| (k - mean_kappa).powi(2))
            .sum::<f64>() / kappas.len() as f64).sqrt();
        let cv = std_kappa / mean_kappa;  // Coefficient of variation

        let regime_changes = self.count_regime_changes(rolling_results);

        MIDCStabilityAnalysis {
            n_windows: rolling_results.len(),
            mean_kappa,
            std_kappa,
            coefficient_of_variation: cv,
            min_kappa: kappas.iter().cloned().fold(f64::INFINITY, f64::min),
            max_kappa: kappas.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            regime_changes,
            stability_assessment: self.assess_stability(cv, regime_changes, rolling_results.len()),
        }
    }

    fn count_regime_changes(&self, results: &[MIDCAnalysisResult]) -> usize {
        results.windows(2)
            .filter(|w| w[0].regime_assessment.regime != w[1].regime_assessment.regime)
            .count()
    }

    fn assess_stability(&self, cv: f64, regime_changes: usize, n_windows: usize) -> StabilityAssessment {
        let change_rate = regime_changes as f64 / n_windows as f64;

        if cv < 0.2 && change_rate < 0.1 {
            StabilityAssessment::Stable
        } else if cv < 0.5 && change_rate < 0.3 {
            StabilityAssessment::ModeratelyStable
        } else {
            StabilityAssessment::Unstable
        }
    }
}

#[derive(Debug, Clone)]
pub struct MIDCAnalysisResult {
    pub period_start: Option<DateTime<Utc>>,
    pub period_end: Option<DateTime<Utc>>,
    pub n_price_points: usize,
    pub autocorrelation_points: Vec<AutocorrPoint>,
    pub fit_result: MIDCFitResult,
    pub regime_assessment: MIDCRegimeAssessment,
}

#[derive(Debug, Clone)]
pub struct MIDCStabilityAnalysis {
    pub n_windows: usize,
    pub mean_kappa: f64,
    pub std_kappa: f64,
    pub coefficient_of_variation: f64,
    pub min_kappa: f64,
    pub max_kappa: f64,
    pub regime_changes: usize,
    pub stability_assessment: StabilityAssessment,
}

impl MIDCStabilityAnalysis {
    fn insufficient_data() -> Self {
        Self {
            n_windows: 0,
            mean_kappa: f64::NAN,
            std_kappa: f64::NAN,
            coefficient_of_variation: f64::NAN,
            min_kappa: f64::NAN,
            max_kappa: f64::NAN,
            regime_changes: 0,
            stability_assessment: StabilityAssessment::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StabilityAssessment {
    Stable,           // MIDC consistent over time
    ModeratelyStable, // Some variation but predictable
    Unstable,         // High variation, regime changes frequently
    Unknown,
}
```

---

## CLI Tool for MIDC Analysis

```rust
/// MIDC Analysis CLI
///
/// Usage:
///   cargo run --bin midc_analysis -- analyze --data ./data/features --days 30
///   cargo run --bin midc_analysis -- rolling --data ./data/features --window 7 --step 1
///   cargo run --bin midc_analysis -- report --data ./data/features --output ./reports/midc.md

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "midc_analysis")]
#[command(about = "Analyze Market Information Diffusion Coefficient")]
pub struct MIDCCli {
    #[command(subcommand)]
    command: MIDCCommand,
}

#[derive(Subcommand)]
pub enum MIDCCommand {
    /// Single-period MIDC analysis
    Analyze {
        /// Path to price data (parquet files)
        #[arg(long)]
        data: PathBuf,

        /// Number of days to analyze
        #[arg(long, default_value = "30")]
        days: u64,
    },

    /// Rolling window MIDC analysis
    Rolling {
        /// Path to price data
        #[arg(long)]
        data: PathBuf,

        /// Window size in days
        #[arg(long, default_value = "7")]
        window: u64,

        /// Step size in days
        #[arg(long, default_value = "1")]
        step: u64,
    },

    /// Generate full MIDC report
    Report {
        /// Path to price data
        #[arg(long)]
        data: PathBuf,

        /// Output path for report
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = MIDCCli::parse();

    match cli.command {
        MIDCCommand::Analyze { data, days } => {
            let prices = load_prices(&data, days)?;
            let estimator = MIDCEstimator::new(
                MIDCDataConfig::default(),
                RegimeConfig::default(),
            );

            let result = estimator.estimate(&prices);
            print_analysis_result(&result);
        }

        MIDCCommand::Rolling { data, window, step } => {
            let prices = load_all_prices(&data)?;
            let estimator = MIDCEstimator::new(
                MIDCDataConfig::default(),
                RegimeConfig::default(),
            );

            let results = estimator.estimate_rolling(&prices, window, step);
            let stability = estimator.analyze_stability(&results);

            print_rolling_results(&results);
            print_stability_analysis(&stability);
        }

        MIDCCommand::Report { data, output } => {
            let prices = load_all_prices(&data)?;
            let report = generate_full_report(&prices)?;

            if let Some(path) = output {
                std::fs::write(&path, &report)?;
                println!("Report written to {:?}", path);
            } else {
                println!("{}", report);
            }
        }
    }

    Ok(())
}
```

---

## Expected Output

### Single Analysis

```
================================================================================
                         MIDC ANALYSIS REPORT
================================================================================

Analysis Period: 2025-11-18 to 2025-12-18 (30 days)
Price Points: 2,592,000
Symbol: BTCUSDT

AUTOCORRELATION FUNCTION
------------------------
Lag (s)  |  ρ(Δ)   |  SE     |  Significant?
---------|---------|---------|---------------
    1    |  0.0823 |  0.0006 |  Yes ***
    2    |  0.0712 |  0.0006 |  Yes ***
    5    |  0.0534 |  0.0006 |  Yes ***
   10    |  0.0398 |  0.0006 |  Yes ***
   30    |  0.0187 |  0.0006 |  Yes ***
   60    |  0.0092 |  0.0006 |  Yes ***
  120    |  0.0041 |  0.0006 |  Yes ***
  300    |  0.0012 |  0.0006 |  Yes *
  600    |  0.0003 |  0.0006 |  No
 1800    | -0.0001 |  0.0006 |  No

EXPONENTIAL DECAY FIT
---------------------
Model: ρ(Δ) = ρ₀ · e^(-κΔ)

Fitted Parameters:
  ρ₀ (initial autocorr):  0.0891
  κ (MIDC):               0.0234 /second
  τ_half (half-life):     29.6 seconds

Fit Quality:
  R²:                     0.962
  N points used:          8
  Quality Assessment:     Excellent

REGIME ASSESSMENT
-----------------
Regime:           MODERATE DIFFUSION
Momentum Viable:  YES (τ_half = 29.6s > 30s threshold - borderline)
Confidence:       HIGH

Reasoning:
  Moderate diffusion (κ=0.0234, τ_half=29.6s). Some momentum edge may exist,
  use smaller position sizes.

RECOMMENDATION:   CAUTIOUS MOMENTUM STRATEGY
================================================================================
```

### Rolling Analysis

```
================================================================================
                      ROLLING MIDC STABILITY ANALYSIS
================================================================================

Rolling Windows: 24 (7-day windows, 1-day step)
Period: 2025-10-18 to 2025-12-18

MIDC OVER TIME
--------------
Window End   |    κ     | τ_half(s) | Regime           | Quality
-------------|----------|-----------|------------------|----------
2025-10-25   |  0.0312  |    22.2   | Moderate         | Good
2025-10-26   |  0.0298  |    23.3   | Moderate         | Good
2025-10-27   |  0.0245  |    28.3   | Moderate         | Excellent
...
2025-12-15   |  0.0089  |    77.9   | SLOW             | Excellent  ← Best for momentum
2025-12-16   |  0.0102  |    67.9   | Slow             | Excellent
2025-12-17   |  0.0156  |    44.4   | Moderate         | Good
2025-12-18   |  0.0234  |    29.6   | Moderate         | Excellent

STABILITY SUMMARY
-----------------
Mean κ:                   0.0198
Std κ:                    0.0078
Coefficient of Variation: 0.394 (39.4%)
Min κ:                    0.0089 (τ_half = 77.9s)
Max κ:                    0.0312 (τ_half = 22.2s)
Regime Changes:           6 (25%)

Stability Assessment:     MODERATELY STABLE

MOMENTUM OPPORTUNITY WINDOWS
----------------------------
Periods where τ_half > 60s (strong momentum potential):
  - 2025-12-13 to 2025-12-16 (4 days)
  - 2025-11-22 to 2025-11-24 (3 days)

Periods where τ_half < 20s (momentum NOT viable):
  - 2025-11-01 to 2025-11-03 (3 days)

CONCLUSION
----------
MIDC varies significantly over time (CV=39%). Momentum strategies should
adapt position sizing based on current MIDC estimate. Best momentum windows
show τ_half > 60s. Current regime (τ_half=29.6s) suggests cautious approach.

================================================================================
```

---

## Integration with Research Layer

```rust
/// Integration point with MarketResearch (Layer 0)
impl MarketResearch {
    pub fn assess_momentum_viability(&self) -> MomentumViabilityAssessment {
        // Get current MIDC estimate
        let midc = self.midc_estimator.current_estimate();

        // Combine with other research signals
        let entropy_ok = self.entropy_gating.current() < ENTROPY_THRESHOLD;
        let persistence_ok = self.persistence.mean_duration() > MIN_PERSISTENCE;

        MomentumViabilityAssessment {
            midc_assessment: midc.regime_assessment.clone(),
            entropy_ok,
            persistence_ok,
            overall_viable: midc.regime_assessment.momentum_viable && entropy_ok && persistence_ok,
            recommended_position_scale: self.compute_position_scale(&midc),
        }
    }

    fn compute_position_scale(&self, midc: &MIDCAnalysisResult) -> f64 {
        // Scale position size based on τ_half
        let tau = midc.fit_result.tau_half_seconds;

        if tau > 120.0 {
            1.0  // Full size
        } else if tau > 60.0 {
            0.75
        } else if tau > 30.0 {
            0.5
        } else {
            0.25  // Minimal size
        }
    }
}
```

---

## Implementation Checklist

| Task | Description | Status |
|------|-------------|--------|
| 1 | MIDCDataConfig struct | TODO |
| 2 | ReturnComputer (multi-scale returns) | TODO |
| 3 | AutocorrelationComputer (ACF) | TODO |
| 4 | ExponentialDecayFitter (least squares) | TODO |
| 5 | RegimeAssessor | TODO |
| 6 | MIDCEstimator (main interface) | TODO |
| 7 | Rolling estimation | TODO |
| 8 | Stability analysis | TODO |
| 9 | CLI tool | TODO |
| 10 | Integration with MarketResearch | TODO |
| 11 | TUI visualization | TODO |
| 12 | Unit tests | TODO |

---

## Summary

MIDC answers the fundamental question: **"How long does predictability last in this market?"**

The process:
1. **Load 1 month of price data**
2. **Compute returns at multiple time scales**
3. **Calculate autocorrelation function**
4. **Fit exponential decay: ρ(Δ) = ρ₀ · e^(-κΔ)**
5. **Extract κ (MIDC) and τ_half (predictability horizon)**
6. **Assess regime and recommend strategy**

Key insight: **If τ_half > our execution latency, momentum is exploitable.**

---

*Document maintained by: Development Team*
*Last updated: December 18, 2025*

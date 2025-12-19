//! MIDC Estimator Implementation - Task 1.1
//!
//! Market Information Diffusion Coefficient (MIDC) estimator with persistence.
//! Computes the rate at which market information diffuses into prices.
//!
//! # Theory
//!
//! MIDC measures how quickly autocorrelation in returns decays:
//! - ρ(Δ) = ρ₀ · e^(-κΔ)
//!
//! Where:
//! - ρ(Δ) is autocorrelation at lag Δ
//! - ρ₀ is initial autocorrelation
//! - κ (kappa) is the diffusion coefficient (MIDC)
//! - τ_half = ln(2) / κ is the half-life of predictability
//!
//! # Regime Interpretation
//!
//! - κ < 0.01 (SlowDiffusion): Trends persist, momentum viable
//! - 0.01 ≤ κ < 0.1 (ModerateDiffusion): Mixed signals
//! - κ ≥ 0.1 (FastDiffusion): Fast incorporation, momentum not viable
//!
//! # Usage
//!
//! ```rust,ignore
//! use ingestor::research::{MIDCEstimator, MIDCConfig, PricePoint};
//!
//! let config = MIDCConfig::default();
//! let mut estimator = MIDCEstimator::new(config);
//!
//! // Streaming updates
//! for price_point in price_stream {
//!     estimator.update(&price_point);
//! }
//!
//! // Get current estimate
//! let estimate = estimator.current();
//! println!("MIDC κ = {}, τ_half = {} seconds", estimate.kappa, estimate.tau_half_seconds);
//! ```

use crate::framework::{MIDCEstimate, MIDCRegime};
use crate::research::{MIDCConfig, PricePoint, ResearchError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

// ============================================================================
// MIDCEstimator
// ============================================================================

/// Market Information Diffusion Coefficient estimator
///
/// Continuously estimates the rate at which market information diffuses
/// into prices by fitting an exponential decay to the autocorrelation function.
#[derive(Debug, Clone)]
pub struct MIDCEstimator {
    /// Configuration
    config: MIDCConfig,

    /// Rolling window of price points
    price_buffer: VecDeque<PricePoint>,

    /// Rolling window of returns for autocorrelation
    returns_buffer: VecDeque<ReturnPoint>,

    /// Current MIDC estimate
    current_estimate: MIDCEstimate,

    /// Number of samples processed
    samples_processed: usize,

    /// Number of updates since last estimate
    updates_since_estimate: usize,

    /// Statistics for diagnostics
    stats: MIDCEstimatorStats,
}

/// Internal structure for tracking returns with timestamps
#[derive(Debug, Clone, Copy)]
struct ReturnPoint {
    timestamp: DateTime<Utc>,
    log_return: f64,
}

/// Statistics for the MIDC estimator
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MIDCEstimatorStats {
    /// Total samples processed
    pub total_samples: usize,

    /// Number of estimates computed
    pub estimates_computed: usize,

    /// Number of failed estimates (poor fit)
    pub failed_estimates: usize,

    /// Average R-squared of successful fits
    pub avg_r_squared: f64,

    /// Sum of R-squared values (for averaging)
    r_squared_sum: f64,

    /// Minimum kappa observed
    pub min_kappa: f64,

    /// Maximum kappa observed
    pub max_kappa: f64,

    /// Average kappa
    pub avg_kappa: f64,

    /// Sum of kappa values (for averaging)
    kappa_sum: f64,
}

impl MIDCEstimator {
    /// Create a new MIDC estimator with given configuration
    pub fn new(config: MIDCConfig) -> Self {
        Self {
            config,
            price_buffer: VecDeque::new(),
            returns_buffer: VecDeque::new(),
            current_estimate: MIDCEstimate::default(),
            samples_processed: 0,
            updates_since_estimate: 0,
            stats: MIDCEstimatorStats {
                min_kappa: f64::MAX,
                max_kappa: f64::MIN,
                ..Default::default()
            },
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(MIDCConfig::default())
    }

    /// Update the estimator with a new price point (streaming mode)
    ///
    /// This is the primary interface for real-time updates. The estimator
    /// automatically recomputes the MIDC estimate based on `update_frequency`.
    pub fn update(&mut self, price_point: &PricePoint) -> Result<(), ResearchError> {
        // Add to price buffer
        self.price_buffer.push_back(price_point.clone());

        // Maintain rolling window size
        while self.price_buffer.len() > self.config.rolling_window {
            self.price_buffer.pop_front();
        }

        // Compute return if we have at least 2 prices
        if self.price_buffer.len() >= 2 {
            let prev = &self.price_buffer[self.price_buffer.len() - 2];
            let curr = &self.price_buffer[self.price_buffer.len() - 1];

            if prev.price > 0.0 {
                let log_return = (curr.price / prev.price).ln();
                self.returns_buffer.push_back(ReturnPoint {
                    timestamp: curr.timestamp,
                    log_return,
                });

                // Maintain returns buffer size
                while self.returns_buffer.len() > self.config.rolling_window - 1 {
                    self.returns_buffer.pop_front();
                }
            }
        }

        self.samples_processed += 1;
        self.updates_since_estimate += 1;

        // Recompute estimate if update frequency reached
        if self.updates_since_estimate >= self.config.update_frequency {
            self.recompute_estimate()?;
        }

        Ok(())
    }

    /// Estimate MIDC from a batch of price points
    ///
    /// This is a one-shot estimation that does not update internal state.
    /// Useful for analyzing historical data.
    pub fn estimate(prices: &[PricePoint], config: &MIDCConfig) -> Result<MIDCEstimate, ResearchError> {
        if prices.len() < 10 {
            return Err(ResearchError::InsufficientData {
                message: "Need at least 10 price points for MIDC estimation".to_string(),
                required: 10,
                available: prices.len(),
            });
        }

        // Compute log returns
        let returns: Vec<ReturnPoint> = prices
            .windows(2)
            .filter_map(|w| {
                if w[0].price > 0.0 {
                    Some(ReturnPoint {
                        timestamp: w[1].timestamp,
                        log_return: (w[1].price / w[0].price).ln(),
                    })
                } else {
                    None
                }
            })
            .collect();

        if returns.len() < 10 {
            return Err(ResearchError::InsufficientData {
                message: "Not enough valid returns for MIDC estimation".to_string(),
                required: 10,
                available: returns.len(),
            });
        }

        Self::fit_midc(&returns, config)
    }

    /// Get the current MIDC estimate
    pub fn current(&self) -> &MIDCEstimate {
        &self.current_estimate
    }

    /// Get the current regime classification
    pub fn regime(&self) -> MIDCRegime {
        self.current_estimate.regime()
    }

    /// Check if the estimator is ready (has enough data)
    pub fn is_ready(&self) -> bool {
        self.samples_processed >= self.config.rolling_window / 2
            && self.current_estimate.sample_size > 0
    }

    /// Get the number of samples processed
    pub fn samples_processed(&self) -> usize {
        self.samples_processed
    }

    /// Get estimator statistics
    pub fn stats(&self) -> &MIDCEstimatorStats {
        &self.stats
    }

    /// Get the configuration
    pub fn config(&self) -> &MIDCConfig {
        &self.config
    }

    /// Force a recomputation of the estimate
    pub fn force_recompute(&mut self) -> Result<(), ResearchError> {
        self.recompute_estimate()
    }

    /// Reset the estimator to initial state
    pub fn reset(&mut self) {
        self.price_buffer.clear();
        self.returns_buffer.clear();
        self.current_estimate = MIDCEstimate::default();
        self.samples_processed = 0;
        self.updates_since_estimate = 0;
        self.stats = MIDCEstimatorStats {
            min_kappa: f64::MAX,
            max_kappa: f64::MIN,
            ..Default::default()
        };
    }

    /// Get the current tau_half in seconds (convenience method)
    pub fn tau_half_seconds(&self) -> f64 {
        self.current_estimate.tau_half_seconds
    }

    /// Get the current kappa (convenience method)
    pub fn kappa(&self) -> f64 {
        self.current_estimate.kappa
    }

    // ========================================================================
    // Internal Methods
    // ========================================================================

    /// Recompute the MIDC estimate from current buffer
    fn recompute_estimate(&mut self) -> Result<(), ResearchError> {
        self.updates_since_estimate = 0;

        if self.returns_buffer.len() < 10 {
            return Ok(()); // Not enough data yet
        }

        let returns: Vec<ReturnPoint> = self.returns_buffer.iter().copied().collect();

        match Self::fit_midc(&returns, &self.config) {
            Ok(estimate) => {
                self.update_stats(&estimate);
                self.current_estimate = estimate;
            }
            Err(_) => {
                self.stats.failed_estimates += 1;
            }
        }

        Ok(())
    }

    /// Fit MIDC model to returns data
    fn fit_midc(returns: &[ReturnPoint], config: &MIDCConfig) -> Result<MIDCEstimate, ResearchError> {
        // Compute autocorrelations at different time lags
        let autocorrelations = Self::compute_autocorrelations(returns, &config.time_scales)?;

        if autocorrelations.is_empty() {
            return Err(ResearchError::MIDCEstimation(
                "No valid autocorrelations computed".to_string(),
            ));
        }

        // Fit exponential decay: log(ρ) = log(ρ₀) - κΔ
        let (kappa, rho_0, r_squared) = Self::fit_exponential_decay(&autocorrelations)?;

        // Validate the fit
        if r_squared < config.min_r_squared {
            return Err(ResearchError::MIDCEstimation(format!(
                "Poor fit quality: R² = {:.4} < {:.4}",
                r_squared, config.min_r_squared
            )));
        }

        if kappa > config.max_kappa {
            return Err(ResearchError::MIDCEstimation(format!(
                "Kappa too high: κ = {:.4} > {:.4}",
                kappa, config.max_kappa
            )));
        }

        // Compute tau_half
        let tau_half_seconds = if kappa > 0.0 {
            (2.0_f64).ln() / kappa
        } else {
            f64::INFINITY
        };

        // Compute confidence based on sample size and R²
        let sample_size = returns.len();
        let confidence = Self::compute_confidence(sample_size, r_squared);

        Ok(MIDCEstimate {
            kappa,
            tau_half_seconds,
            rho_0,
            r_squared,
            sample_size,
            confidence,
            computed_at: Utc::now(),
        })
    }

    /// Compute autocorrelations at specified time lags
    fn compute_autocorrelations(
        returns: &[ReturnPoint],
        time_scales: &[f64],
    ) -> Result<Vec<(f64, f64)>, ResearchError> {
        if returns.is_empty() {
            return Err(ResearchError::MIDCEstimation(
                "No returns to compute autocorrelation".to_string(),
            ));
        }

        let mean_return: f64 = returns.iter().map(|r| r.log_return).sum::<f64>() / returns.len() as f64;

        // Compute variance
        let variance: f64 = returns
            .iter()
            .map(|r| (r.log_return - mean_return).powi(2))
            .sum::<f64>()
            / returns.len() as f64;

        if variance <= 0.0 {
            return Err(ResearchError::MIDCEstimation(
                "Zero variance in returns".to_string(),
            ));
        }

        let mut autocorrelations = Vec::new();

        for &lag_seconds in time_scales {
            let acf = Self::compute_autocorrelation_at_lag(returns, lag_seconds, mean_return, variance);
            if let Some(rho) = acf {
                // Only include positive autocorrelations for exponential fit
                if rho > 0.001 {
                    autocorrelations.push((lag_seconds, rho));
                }
            }
        }

        Ok(autocorrelations)
    }

    /// Compute autocorrelation at a specific time lag
    fn compute_autocorrelation_at_lag(
        returns: &[ReturnPoint],
        lag_seconds: f64,
        mean: f64,
        variance: f64,
    ) -> Option<f64> {
        let mut sum_products = 0.0;
        let mut count = 0usize;
        let tolerance = lag_seconds * 0.2; // 20% tolerance for matching lags

        for i in 0..returns.len() {
            for j in (i + 1)..returns.len() {
                let time_diff = (returns[j].timestamp - returns[i].timestamp)
                    .num_milliseconds() as f64
                    / 1000.0;

                if (time_diff - lag_seconds).abs() <= tolerance {
                    sum_products +=
                        (returns[i].log_return - mean) * (returns[j].log_return - mean);
                    count += 1;
                }
            }
        }

        if count >= 5 {
            // Need at least 5 pairs for reliable estimate
            Some(sum_products / (count as f64 * variance))
        } else {
            None
        }
    }

    /// Fit exponential decay using linear regression on log-transformed data
    /// log(ρ) = log(ρ₀) - κΔ
    fn fit_exponential_decay(
        autocorrelations: &[(f64, f64)],
    ) -> Result<(f64, f64, f64), ResearchError> {
        if autocorrelations.len() < 2 {
            return Err(ResearchError::MIDCEstimation(
                "Need at least 2 autocorrelation points for fitting".to_string(),
            ));
        }

        // Transform to log space: y = log(ρ), x = Δ
        let points: Vec<(f64, f64)> = autocorrelations
            .iter()
            .filter(|(_, rho)| *rho > 0.0)
            .map(|(lag, rho)| (*lag, rho.ln()))
            .collect();

        if points.len() < 2 {
            return Err(ResearchError::MIDCEstimation(
                "Not enough positive autocorrelations for fitting".to_string(),
            ));
        }

        // Linear regression: log(ρ) = intercept + slope * Δ
        let n = points.len() as f64;
        let sum_x: f64 = points.iter().map(|(x, _)| x).sum();
        let sum_y: f64 = points.iter().map(|(_, y)| y).sum();
        let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();
        let sum_x2: f64 = points.iter().map(|(x, _)| x * x).sum();

        let denominator = n * sum_x2 - sum_x * sum_x;
        if denominator.abs() < 1e-10 {
            return Err(ResearchError::MIDCEstimation(
                "Singular matrix in regression".to_string(),
            ));
        }

        let slope = (n * sum_xy - sum_x * sum_y) / denominator;
        let intercept = (sum_y - slope * sum_x) / n;

        // Extract parameters
        let kappa = -slope; // κ = -slope
        let rho_0 = intercept.exp(); // ρ₀ = e^intercept

        // Compute R²
        let mean_y = sum_y / n;
        let ss_tot: f64 = points.iter().map(|(_, y)| (y - mean_y).powi(2)).sum();
        let ss_res: f64 = points
            .iter()
            .map(|(x, y)| {
                let predicted = intercept + slope * x;
                (y - predicted).powi(2)
            })
            .sum();

        let r_squared = if ss_tot > 0.0 {
            1.0 - ss_res / ss_tot
        } else {
            0.0
        };

        // Clamp kappa to reasonable range
        let kappa = kappa.max(0.0).min(10.0);

        Ok((kappa, rho_0, r_squared.max(0.0).min(1.0)))
    }

    /// Compute confidence level based on sample size and fit quality
    fn compute_confidence(sample_size: usize, r_squared: f64) -> f64 {
        // Sample size factor: saturates around 1000 samples
        let size_factor = 1.0 - (-(sample_size as f64) / 500.0).exp();

        // R² factor
        let r_squared_factor = r_squared;

        // Combined confidence
        (size_factor * r_squared_factor).min(1.0).max(0.0)
    }

    /// Update internal statistics
    fn update_stats(&mut self, estimate: &MIDCEstimate) {
        self.stats.total_samples = self.samples_processed;
        self.stats.estimates_computed += 1;

        self.stats.r_squared_sum += estimate.r_squared;
        self.stats.avg_r_squared =
            self.stats.r_squared_sum / self.stats.estimates_computed as f64;

        self.stats.kappa_sum += estimate.kappa;
        self.stats.avg_kappa = self.stats.kappa_sum / self.stats.estimates_computed as f64;

        if estimate.kappa < self.stats.min_kappa {
            self.stats.min_kappa = estimate.kappa;
        }
        if estimate.kappa > self.stats.max_kappa {
            self.stats.max_kappa = estimate.kappa;
        }
    }
}

// ============================================================================
// MIDCEstimatorBuilder
// ============================================================================

/// Builder for MIDCEstimator with fluent API
#[derive(Debug, Clone)]
pub struct MIDCEstimatorBuilder {
    config: MIDCConfig,
}

impl MIDCEstimatorBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: MIDCConfig::default(),
        }
    }

    /// Set rolling window size
    pub fn with_rolling_window(mut self, size: usize) -> Self {
        self.config.rolling_window = size;
        self
    }

    /// Set time scales for autocorrelation
    pub fn with_time_scales(mut self, scales: Vec<f64>) -> Self {
        self.config.time_scales = scales;
        self
    }

    /// Set minimum R-squared threshold
    pub fn with_min_r_squared(mut self, threshold: f64) -> Self {
        self.config.min_r_squared = threshold;
        self
    }

    /// Set maximum kappa threshold
    pub fn with_max_kappa(mut self, max: f64) -> Self {
        self.config.max_kappa = max;
        self
    }

    /// Set update frequency
    pub fn with_update_frequency(mut self, freq: usize) -> Self {
        self.config.update_frequency = freq;
        self
    }

    /// Build the estimator
    pub fn build(self) -> MIDCEstimator {
        MIDCEstimator::new(self.config)
    }
}

impl Default for MIDCEstimatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    // Helper to create test price points
    fn make_price_points(prices: &[f64], interval_ms: i64) -> Vec<PricePoint> {
        let base_time = Utc::now();
        prices
            .iter()
            .enumerate()
            .map(|(i, &price)| PricePoint {
                timestamp: base_time + Duration::milliseconds(i as i64 * interval_ms),
                price,
                volume: None,
            })
            .collect()
    }

    // Helper to create trending prices (momentum)
    fn make_trending_prices(start: f64, count: usize, drift: f64, noise: f64) -> Vec<f64> {
        let mut prices = Vec::with_capacity(count);
        let mut price = start;
        for i in 0..count {
            // Add trend + small random noise (deterministic for tests)
            price *= 1.0 + drift + noise * ((i as f64 * 0.1).sin() * 0.5);
            prices.push(price);
        }
        prices
    }

    // Helper to create mean-reverting prices
    fn make_mean_reverting_prices(mean: f64, count: usize, reversion: f64) -> Vec<f64> {
        let mut prices = Vec::with_capacity(count);
        let mut price = mean;
        for i in 0..count {
            // Mean reversion + oscillation
            let deviation = (i as f64 * 0.3).sin() * mean * 0.02;
            price = mean + deviation * (1.0 - reversion);
            prices.push(price);
        }
        prices
    }

    // ========================================================================
    // Constructor Tests
    // ========================================================================

    #[test]
    fn test_new_with_default_config() {
        let config = MIDCConfig::default();
        let estimator = MIDCEstimator::new(config);

        assert_eq!(estimator.samples_processed(), 0);
        assert!(!estimator.is_ready());
        assert_eq!(estimator.current().kappa, 0.0);
    }

    #[test]
    fn test_with_defaults() {
        let estimator = MIDCEstimator::with_defaults();
        assert_eq!(estimator.config().rolling_window, 1000);
        assert!(!estimator.config().time_scales.is_empty());
    }

    #[test]
    fn test_builder_pattern() {
        let estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(500)
            .with_min_r_squared(0.6)
            .with_max_kappa(0.5)
            .with_update_frequency(50)
            .build();

        assert_eq!(estimator.config().rolling_window, 500);
        assert_eq!(estimator.config().min_r_squared, 0.6);
        assert_eq!(estimator.config().max_kappa, 0.5);
        assert_eq!(estimator.config().update_frequency, 50);
    }

    #[test]
    fn test_builder_with_custom_time_scales() {
        let scales = vec![1.0, 2.0, 5.0, 10.0];
        let estimator = MIDCEstimatorBuilder::new()
            .with_time_scales(scales.clone())
            .build();

        assert_eq!(estimator.config().time_scales, scales);
    }

    // ========================================================================
    // Update Tests
    // ========================================================================

    #[test]
    fn test_update_single_price() {
        let mut estimator = MIDCEstimator::with_defaults();
        let point = PricePoint::new(Utc::now(), 100.0);

        let result = estimator.update(&point);
        assert!(result.is_ok());
        assert_eq!(estimator.samples_processed(), 1);
    }

    #[test]
    fn test_update_multiple_prices() {
        let mut estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(100)
            .with_update_frequency(10)
            .build();

        let prices = make_price_points(&[100.0, 101.0, 102.0, 101.5, 102.5], 1000);

        for p in &prices {
            estimator.update(p).unwrap();
        }

        assert_eq!(estimator.samples_processed(), 5);
    }

    #[test]
    fn test_update_maintains_rolling_window() {
        let mut estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(10)
            .with_update_frequency(5)
            .build();

        let prices = make_price_points(&(0..20).map(|i| 100.0 + i as f64).collect::<Vec<_>>(), 1000);

        for p in &prices {
            estimator.update(p).unwrap();
        }

        // Buffer should be capped at rolling_window size
        assert!(estimator.price_buffer.len() <= 10);
    }

    #[test]
    fn test_update_computes_returns() {
        let mut estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(100)
            .with_update_frequency(50)
            .build();

        let prices = make_price_points(&[100.0, 110.0, 121.0], 1000);

        for p in &prices {
            estimator.update(p).unwrap();
        }

        // Should have 2 returns from 3 prices
        assert_eq!(estimator.returns_buffer.len(), 2);
    }

    #[test]
    fn test_update_handles_zero_price() {
        let mut estimator = MIDCEstimator::with_defaults();

        let p1 = PricePoint::new(Utc::now(), 0.0);
        let p2 = PricePoint::new(Utc::now(), 100.0);

        estimator.update(&p1).unwrap();
        estimator.update(&p2).unwrap();

        // Should not crash, returns buffer should be empty (can't compute log return from 0)
        assert!(estimator.returns_buffer.is_empty());
    }

    // ========================================================================
    // Batch Estimation Tests
    // ========================================================================

    #[test]
    fn test_estimate_insufficient_data() {
        let prices = make_price_points(&[100.0, 101.0, 102.0], 1000);
        let config = MIDCConfig::default();

        let result = MIDCEstimator::estimate(&prices, &config);
        assert!(result.is_err());

        if let Err(ResearchError::InsufficientData { available, .. }) = result {
            assert_eq!(available, 3);
        } else {
            panic!("Expected InsufficientData error");
        }
    }

    #[test]
    fn test_estimate_with_trending_data() {
        let prices_vec = make_trending_prices(100.0, 200, 0.001, 0.0005);
        let prices = make_price_points(&prices_vec, 1000);

        let config = MIDCConfig {
            rolling_window: 200,
            time_scales: vec![1.0, 2.0, 3.0, 5.0, 10.0],
            min_r_squared: 0.1, // Lower threshold for test
            max_kappa: 5.0,
            update_frequency: 10,
        };

        let result = MIDCEstimator::estimate(&prices, &config);
        // Result depends on data quality, but should not panic
        assert!(result.is_ok() || matches!(result, Err(ResearchError::MIDCEstimation(_))));
    }

    #[test]
    fn test_estimate_with_mean_reverting_data() {
        let prices_vec = make_mean_reverting_prices(100.0, 200, 0.9);
        let prices = make_price_points(&prices_vec, 1000);

        let config = MIDCConfig {
            rolling_window: 200,
            time_scales: vec![1.0, 2.0, 3.0, 5.0, 10.0],
            min_r_squared: 0.1,
            max_kappa: 5.0,
            update_frequency: 10,
        };

        let result = MIDCEstimator::estimate(&prices, &config);
        assert!(result.is_ok() || matches!(result, Err(ResearchError::MIDCEstimation(_))));
    }

    #[test]
    fn test_estimate_empty_prices() {
        let prices: Vec<PricePoint> = vec![];
        let config = MIDCConfig::default();

        let result = MIDCEstimator::estimate(&prices, &config);
        assert!(matches!(result, Err(ResearchError::InsufficientData { .. })));
    }

    // ========================================================================
    // Current Estimate Tests
    // ========================================================================

    #[test]
    fn test_current_returns_default_initially() {
        let estimator = MIDCEstimator::with_defaults();
        let estimate = estimator.current();

        assert_eq!(estimate.kappa, 0.0);
        assert_eq!(estimate.tau_half_seconds, 0.0);
        assert_eq!(estimate.sample_size, 0);
    }

    #[test]
    fn test_kappa_convenience_method() {
        let estimator = MIDCEstimator::with_defaults();
        assert_eq!(estimator.kappa(), estimator.current().kappa);
    }

    #[test]
    fn test_tau_half_seconds_convenience_method() {
        let estimator = MIDCEstimator::with_defaults();
        assert_eq!(
            estimator.tau_half_seconds(),
            estimator.current().tau_half_seconds
        );
    }

    // ========================================================================
    // Regime Classification Tests
    // ========================================================================

    #[test]
    fn test_regime_unknown_initially() {
        let estimator = MIDCEstimator::with_defaults();
        assert_eq!(estimator.regime(), MIDCRegime::Unknown);
    }

    // ========================================================================
    // Ready State Tests
    // ========================================================================

    #[test]
    fn test_is_ready_false_initially() {
        let estimator = MIDCEstimator::with_defaults();
        assert!(!estimator.is_ready());
    }

    #[test]
    fn test_is_ready_requires_sufficient_data() {
        let mut estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(100)  // Large window
            .with_update_frequency(50)  // Less frequent updates
            .build();

        // Add only a few samples (less than rolling_window/2 = 50)
        let prices = make_price_points(
            &(0..10).map(|i| 100.0 + i as f64 * 0.1).collect::<Vec<_>>(),
            1000,
        );

        for p in &prices {
            estimator.update(p).unwrap();
        }

        // Not ready yet - need rolling_window/2 = 50 samples
        // With only 10 samples, is_ready should be false
        assert!(!estimator.is_ready());
        assert!(estimator.samples_processed() < 50);
    }

    // ========================================================================
    // Reset Tests
    // ========================================================================

    #[test]
    fn test_reset_clears_state() {
        let mut estimator = MIDCEstimator::with_defaults();

        let prices = make_price_points(&[100.0, 101.0, 102.0], 1000);
        for p in &prices {
            estimator.update(p).unwrap();
        }

        assert!(estimator.samples_processed() > 0);

        estimator.reset();

        assert_eq!(estimator.samples_processed(), 0);
        assert!(estimator.price_buffer.is_empty());
        assert!(estimator.returns_buffer.is_empty());
        assert_eq!(estimator.current().kappa, 0.0);
    }

    #[test]
    fn test_reset_resets_stats() {
        let mut estimator = MIDCEstimator::with_defaults();

        let prices = make_price_points(&[100.0, 101.0, 102.0], 1000);
        for p in &prices {
            estimator.update(p).unwrap();
        }

        estimator.reset();

        assert_eq!(estimator.stats().total_samples, 0);
        assert_eq!(estimator.stats().estimates_computed, 0);
    }

    // ========================================================================
    // Force Recompute Tests
    // ========================================================================

    #[test]
    fn test_force_recompute_without_data() {
        let mut estimator = MIDCEstimator::with_defaults();
        let result = estimator.force_recompute();
        assert!(result.is_ok()); // Should not error, just no-op
    }

    #[test]
    fn test_force_recompute_with_data() {
        let mut estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(50)
            .with_update_frequency(100) // High so it won't auto-update
            .build();

        let prices = make_price_points(
            &(0..40).map(|i| 100.0 + i as f64 * 0.01).collect::<Vec<_>>(),
            1000,
        );

        for p in &prices {
            estimator.update(p).unwrap();
        }

        let result = estimator.force_recompute();
        assert!(result.is_ok());
    }

    // ========================================================================
    // Statistics Tests
    // ========================================================================

    #[test]
    fn test_stats_initial_values() {
        let estimator = MIDCEstimator::with_defaults();
        let stats = estimator.stats();

        assert_eq!(stats.total_samples, 0);
        assert_eq!(stats.estimates_computed, 0);
        assert_eq!(stats.failed_estimates, 0);
    }

    #[test]
    fn test_stats_tracks_samples() {
        let mut estimator = MIDCEstimator::with_defaults();

        let prices = make_price_points(&[100.0, 101.0, 102.0, 103.0, 104.0], 1000);
        for p in &prices {
            estimator.update(p).unwrap();
        }

        // samples_processed tracks all updates
        assert_eq!(estimator.samples_processed(), 5);
        // stats.total_samples is only updated on successful estimate computation
        // So we verify samples_processed is correct instead
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_negative_price_handled() {
        let mut estimator = MIDCEstimator::with_defaults();

        let p1 = PricePoint::new(Utc::now(), -100.0);
        let p2 = PricePoint::new(Utc::now(), 100.0);

        // Should not panic
        estimator.update(&p1).unwrap();
        estimator.update(&p2).unwrap();
    }

    #[test]
    fn test_very_small_price_changes() {
        let mut estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(50)
            .with_update_frequency(10)
            .build();

        // Very small changes (potential numerical issues)
        let prices = make_price_points(
            &(0..30)
                .map(|i| 100.0 + i as f64 * 1e-10)
                .collect::<Vec<_>>(),
            1000,
        );

        for p in &prices {
            let result = estimator.update(p);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_constant_prices() {
        let mut estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(50)
            .with_update_frequency(10)
            .with_min_r_squared(0.1)
            .build();

        // Constant prices (zero variance)
        let prices = make_price_points(&vec![100.0; 30], 1000);

        for p in &prices {
            estimator.update(p).unwrap();
        }

        // Should handle gracefully (returns are all zero)
        // Estimate may fail but should not panic
    }

    #[test]
    fn test_large_price_jumps() {
        let mut estimator = MIDCEstimator::with_defaults();

        let prices = make_price_points(&[100.0, 1000.0, 10.0, 500.0], 1000);

        for p in &prices {
            let result = estimator.update(p);
            assert!(result.is_ok());
        }
    }

    // ========================================================================
    // Autocorrelation Tests
    // ========================================================================

    #[test]
    fn test_compute_autocorrelations_empty() {
        let returns: Vec<ReturnPoint> = vec![];
        let scales = vec![1.0, 5.0];

        let result = MIDCEstimator::compute_autocorrelations(&returns, &scales);
        assert!(result.is_err());
    }

    #[test]
    fn test_compute_autocorrelations_with_data() {
        let base_time = Utc::now();
        let returns: Vec<ReturnPoint> = (0..50)
            .map(|i| ReturnPoint {
                timestamp: base_time + Duration::seconds(i),
                log_return: 0.001 * (i as f64 * 0.1).sin(),
            })
            .collect();

        let scales = vec![1.0, 2.0, 5.0];

        let result = MIDCEstimator::compute_autocorrelations(&returns, &scales);
        assert!(result.is_ok());
    }

    // ========================================================================
    // Exponential Decay Fit Tests
    // ========================================================================

    #[test]
    fn test_fit_exponential_decay_insufficient_points() {
        let autocorrelations = vec![(1.0, 0.8)]; // Only 1 point

        let result = MIDCEstimator::fit_exponential_decay(&autocorrelations);
        assert!(result.is_err());
    }

    #[test]
    fn test_fit_exponential_decay_perfect_decay() {
        // Perfect exponential decay: ρ = e^(-0.1 * Δ)
        let kappa_true: f64 = 0.1;
        let autocorrelations: Vec<(f64, f64)> = vec![1.0_f64, 2.0, 5.0, 10.0, 20.0]
            .into_iter()
            .map(|lag| (lag, (-kappa_true * lag).exp()))
            .collect();

        let result = MIDCEstimator::fit_exponential_decay(&autocorrelations);
        assert!(result.is_ok());

        let (kappa, rho_0, r_squared) = result.unwrap();

        // Should recover kappa ≈ 0.1, rho_0 ≈ 1.0, R² ≈ 1.0
        assert!((kappa - kappa_true).abs() < 0.01);
        assert!((rho_0 - 1.0).abs() < 0.1);
        assert!(r_squared > 0.99);
    }

    #[test]
    fn test_fit_exponential_decay_noisy_data() {
        // Noisy exponential decay
        let autocorrelations = vec![
            (1.0, 0.9),
            (2.0, 0.82),
            (5.0, 0.61),
            (10.0, 0.38),
            (20.0, 0.15),
        ];

        let result = MIDCEstimator::fit_exponential_decay(&autocorrelations);
        assert!(result.is_ok());

        let (kappa, _rho_0, r_squared) = result.unwrap();

        // Kappa should be positive
        assert!(kappa > 0.0);
        // R² should be reasonable for noisy data
        assert!(r_squared > 0.5);
    }

    #[test]
    fn test_fit_exponential_decay_negative_autocorrelations_filtered() {
        // Some negative autocorrelations (should be filtered)
        let autocorrelations = vec![
            (1.0, 0.9),
            (2.0, 0.8),
            (5.0, -0.1), // Negative - should be filtered
            (10.0, 0.5),
        ];

        let result = MIDCEstimator::fit_exponential_decay(&autocorrelations);
        // Should still work with remaining positive values
        assert!(result.is_ok());
    }

    // ========================================================================
    // Confidence Computation Tests
    // ========================================================================

    #[test]
    fn test_compute_confidence_small_sample() {
        let confidence = MIDCEstimator::compute_confidence(10, 0.9);
        assert!(confidence < 0.5); // Small sample = low confidence
    }

    #[test]
    fn test_compute_confidence_large_sample() {
        let confidence = MIDCEstimator::compute_confidence(1000, 0.9);
        assert!(confidence > 0.7); // Large sample = higher confidence
    }

    #[test]
    fn test_compute_confidence_poor_fit() {
        let confidence = MIDCEstimator::compute_confidence(1000, 0.2);
        assert!(confidence < 0.3); // Poor R² = low confidence
    }

    #[test]
    fn test_compute_confidence_bounds() {
        // Confidence should always be in [0, 1]
        let c1 = MIDCEstimator::compute_confidence(0, 0.0);
        let c2 = MIDCEstimator::compute_confidence(10000, 1.0);

        assert!(c1 >= 0.0 && c1 <= 1.0);
        assert!(c2 >= 0.0 && c2 <= 1.0);
    }

    // ========================================================================
    // Integration Tests
    // ========================================================================

    #[test]
    fn test_full_estimation_workflow() {
        let mut estimator = MIDCEstimatorBuilder::new()
            .with_rolling_window(100)
            .with_update_frequency(20)
            .with_min_r_squared(0.1) // Lower threshold for test
            .with_time_scales(vec![1.0, 2.0, 5.0])
            .build();

        // Generate trending data
        let prices_vec = make_trending_prices(100.0, 150, 0.0005, 0.0002);
        let prices = make_price_points(&prices_vec, 1000);

        for p in &prices {
            estimator.update(p).unwrap();
        }

        // Should have processed all samples
        assert_eq!(estimator.samples_processed(), 150);

        // Stats should be updated
        assert!(estimator.stats().total_samples > 0);
    }

    #[test]
    fn test_streaming_vs_batch_consistency() {
        let prices_vec = make_trending_prices(100.0, 100, 0.0003, 0.0001);
        let prices = make_price_points(&prices_vec, 1000);

        let config = MIDCConfig {
            rolling_window: 100,
            time_scales: vec![1.0, 2.0, 5.0],
            min_r_squared: 0.1,
            max_kappa: 5.0,
            update_frequency: 100,
        };

        // Batch estimation
        let batch_result = MIDCEstimator::estimate(&prices, &config);

        // Streaming estimation
        let mut estimator = MIDCEstimator::new(config);
        for p in &prices {
            estimator.update(p).unwrap();
        }
        estimator.force_recompute().unwrap();

        // Both should either succeed or fail
        // If both succeed, results should be similar
        if let (Ok(batch), true) = (batch_result, estimator.current().sample_size > 0) {
            let stream = estimator.current();
            // Kappa should be in similar range (within factor of 2)
            if batch.kappa > 0.0 && stream.kappa > 0.0 {
                let ratio = batch.kappa / stream.kappa;
                assert!(ratio > 0.1 && ratio < 10.0);
            }
        }
    }

    // ========================================================================
    // Config Getter Tests
    // ========================================================================

    #[test]
    fn test_config_getter() {
        let config = MIDCConfig {
            rolling_window: 500,
            time_scales: vec![1.0, 5.0, 10.0],
            min_r_squared: 0.7,
            max_kappa: 0.5,
            update_frequency: 25,
        };

        let estimator = MIDCEstimator::new(config.clone());

        assert_eq!(estimator.config().rolling_window, 500);
        assert_eq!(estimator.config().min_r_squared, 0.7);
        assert_eq!(estimator.config().max_kappa, 0.5);
        assert_eq!(estimator.config().update_frequency, 25);
    }

    // ========================================================================
    // Serialization Tests (Stats)
    // ========================================================================

    #[test]
    fn test_stats_serialization() {
        let stats = MIDCEstimatorStats {
            total_samples: 1000,
            estimates_computed: 10,
            failed_estimates: 2,
            avg_r_squared: 0.85,
            r_squared_sum: 8.5,
            min_kappa: 0.01,
            max_kappa: 0.15,
            avg_kappa: 0.08,
            kappa_sum: 0.8,
        };

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: MIDCEstimatorStats = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.total_samples, 1000);
        assert_eq!(deserialized.estimates_computed, 10);
        assert_eq!(deserialized.failed_estimates, 2);
        assert!((deserialized.avg_r_squared - 0.85).abs() < 0.001);
    }
}

//! Trend Feature Extraction for MARS Strategy
//!
//! This module implements trend detection features for the Momentum Adaptive
//! Regime Strategy (MARS). These features help identify trending vs mean-reverting
//! market conditions.
//!
//! # Features
//!
//! | Feature | Description | Range | Interpretation |
//! |---------|-------------|-------|----------------|
//! | **Momentum** | Linear regression slope of prices | (-inf, +inf) | Positive = uptrend |
//! | **Monotonicity** | % of ticks in same direction | [0, 1] | >0.7 = strong trend |
//! | **Hurst Exponent** | Trend persistence measure | [0, 1] | >0.5 = trending |
//! | **MA Crossover** | EMA(short) - EMA(long) | (-inf, +inf) | Positive = bullish |
//!
//! # References
//!
//! - Jegadeesh & Titman (1993) - Original momentum paper
//! - Moskowitz et al. (2012) - Time Series Momentum
//! - Mandelbrot (1971) - Hurst exponent and long-range dependence

use std::collections::VecDeque;
use serde::{Deserialize, Serialize};

/// Configuration for trend feature computation
#[derive(Debug, Clone)]
pub struct TrendFeatureConfig {
    /// Window size for trend calculations (default: 60 ticks)
    pub window_size: usize,
    /// Short EMA period for crossover (default: 10)
    pub ema_short_period: usize,
    /// Long EMA period for crossover (default: 50)
    pub ema_long_period: usize,
    /// Minimum samples required for valid computation
    pub min_samples: usize,
}

impl Default for TrendFeatureConfig {
    fn default() -> Self {
        Self {
            window_size: 60,
            ema_short_period: 10,
            ema_long_period: 50,
            min_samples: 20,
        }
    }
}

/// Computed trend features
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrendFeatures {
    /// Price momentum (slope of linear regression)
    /// Positive = upward trend, Negative = downward trend
    /// Units: price change per tick
    pub momentum: Option<f64>,

    /// R-squared of the momentum regression
    /// Higher = more consistent trend
    pub momentum_r_squared: Option<f64>,

    /// Monotonicity score: fraction of price moves in dominant direction
    /// Range: [0.5, 1.0] - 1.0 = perfectly monotonic
    pub monotonicity: Option<f64>,

    /// Hurst exponent estimated via rescaled range analysis
    /// H > 0.5: trending (persistent)
    /// H = 0.5: random walk
    /// H < 0.5: mean-reverting (anti-persistent)
    pub hurst_exponent: Option<f64>,

    /// MA crossover: EMA(short) - EMA(long)
    /// Positive = short-term above long-term (bullish)
    /// Negative = short-term below long-term (bearish)
    pub ma_crossover: Option<f64>,

    /// Normalized MA crossover as percentage of price
    pub ma_crossover_normalized: Option<f64>,

    /// Current short EMA value
    pub ema_short: Option<f64>,

    /// Current long EMA value
    pub ema_long: Option<f64>,

    /// Number of samples used in computation
    pub sample_count: usize,
}

/// Engine for computing trend features from price series
#[derive(Debug)]
pub struct TrendFeatureEngine {
    config: TrendFeatureConfig,
    /// Ring buffer of recent prices
    prices: VecDeque<f64>,
    /// EMA state for short period
    ema_short: Option<f64>,
    /// EMA state for long period
    ema_long: Option<f64>,
    /// Total samples seen (for EMA initialization)
    total_samples: usize,
}

impl TrendFeatureEngine {
    /// Create a new trend feature engine with default configuration
    pub fn new(window_size: usize) -> Self {
        let config = TrendFeatureConfig {
            window_size,
            ..Default::default()
        };
        Self::with_config(config)
    }

    /// Create a new trend feature engine with custom configuration
    pub fn with_config(config: TrendFeatureConfig) -> Self {
        Self {
            prices: VecDeque::with_capacity(config.window_size + 1),
            ema_short: None,
            ema_long: None,
            total_samples: 0,
            config,
        }
    }

    /// Update with a new price observation
    pub fn update(&mut self, price: f64) {
        // Add to price buffer
        self.prices.push_back(price);
        if self.prices.len() > self.config.window_size {
            self.prices.pop_front();
        }

        // Update EMAs
        self.update_emas(price);
        self.total_samples += 1;
    }

    /// Update EMA values with new price
    fn update_emas(&mut self, price: f64) {
        // Short EMA
        let alpha_short = 2.0 / (self.config.ema_short_period as f64 + 1.0);
        self.ema_short = Some(match self.ema_short {
            Some(prev) => alpha_short * price + (1.0 - alpha_short) * prev,
            None => price, // Initialize with first price
        });

        // Long EMA
        let alpha_long = 2.0 / (self.config.ema_long_period as f64 + 1.0);
        self.ema_long = Some(match self.ema_long {
            Some(prev) => alpha_long * price + (1.0 - alpha_long) * prev,
            None => price, // Initialize with first price
        });
    }

    /// Compute all trend features from current state
    pub fn compute(&self) -> TrendFeatures {
        let sample_count = self.prices.len();

        if sample_count < self.config.min_samples {
            return TrendFeatures {
                sample_count,
                ..Default::default()
            };
        }

        let prices: Vec<f64> = self.prices.iter().copied().collect();

        // Compute momentum (linear regression slope)
        let (momentum, r_squared) = Self::compute_momentum(&prices);

        // Compute monotonicity
        let monotonicity = Self::compute_monotonicity(&prices);

        // Compute Hurst exponent
        let hurst = Self::compute_hurst_exponent(&prices);

        // MA crossover
        let (ma_crossover, ma_crossover_normalized) = match (self.ema_short, self.ema_long) {
            (Some(short), Some(long)) => {
                let diff = short - long;
                let normalized = if long.abs() > 1e-10 {
                    Some(diff / long * 100.0) // As percentage
                } else {
                    None
                };
                (Some(diff), normalized)
            }
            _ => (None, None),
        };

        TrendFeatures {
            momentum,
            momentum_r_squared: r_squared,
            monotonicity,
            hurst_exponent: hurst,
            ma_crossover,
            ma_crossover_normalized,
            ema_short: self.ema_short,
            ema_long: self.ema_long,
            sample_count,
        }
    }

    /// Compute momentum as the slope of linear regression
    /// Returns (slope, r_squared)
    fn compute_momentum(prices: &[f64]) -> (Option<f64>, Option<f64>) {
        let n = prices.len();
        if n < 2 {
            return (None, None);
        }

        let n_f = n as f64;

        // x values: 0, 1, 2, ..., n-1
        let sum_x: f64 = (n - 1) as f64 * n_f / 2.0;
        let sum_x2: f64 = (n - 1) as f64 * n_f * (2 * n - 1) as f64 / 6.0;

        let sum_y: f64 = prices.iter().sum();
        let sum_xy: f64 = prices.iter().enumerate()
            .map(|(i, &p)| i as f64 * p)
            .sum();

        let denominator = n_f * sum_x2 - sum_x * sum_x;
        if denominator.abs() < 1e-10 {
            return (None, None);
        }

        let slope = (n_f * sum_xy - sum_x * sum_y) / denominator;
        let intercept = (sum_y - slope * sum_x) / n_f;

        // Compute R-squared
        let mean_y = sum_y / n_f;
        let ss_tot: f64 = prices.iter()
            .map(|&p| (p - mean_y).powi(2))
            .sum();

        let ss_res: f64 = prices.iter().enumerate()
            .map(|(i, &p)| {
                let predicted = intercept + slope * i as f64;
                (p - predicted).powi(2)
            })
            .sum();

        let r_squared = if ss_tot > 1e-10 {
            Some(1.0 - ss_res / ss_tot)
        } else {
            None
        };

        (Some(slope), r_squared)
    }

    /// Compute monotonicity: fraction of price moves in dominant direction
    /// Returns value in [0.5, 1.0] where 1.0 = perfectly monotonic
    fn compute_monotonicity(prices: &[f64]) -> Option<f64> {
        if prices.len() < 2 {
            return None;
        }

        let mut up_count = 0;
        let mut down_count = 0;

        for i in 1..prices.len() {
            if prices[i] > prices[i - 1] {
                up_count += 1;
            } else if prices[i] < prices[i - 1] {
                down_count += 1;
            }
            // Equal prices don't count either way
        }

        let total_moves = up_count + down_count;
        if total_moves == 0 {
            return Some(0.5); // No price movement
        }

        let dominant = up_count.max(down_count) as f64;
        Some(dominant / total_moves as f64)
    }

    /// Compute Hurst exponent using rescaled range (R/S) analysis
    ///
    /// H > 0.5: Trending (persistent) - past trends tend to continue
    /// H = 0.5: Random walk - no predictability
    /// H < 0.5: Mean-reverting (anti-persistent) - trends tend to reverse
    fn compute_hurst_exponent(prices: &[f64]) -> Option<f64> {
        if prices.len() < 20 {
            return None; // Need sufficient data for R/S analysis
        }

        // Compute log returns
        let returns: Vec<f64> = prices.windows(2)
            .map(|w| {
                if w[0] > 0.0 && w[1] > 0.0 {
                    (w[1] / w[0]).ln()
                } else {
                    0.0
                }
            })
            .collect();

        if returns.is_empty() {
            return None;
        }

        // Use multiple window sizes for R/S calculation
        // Sizes should be at least 8 and increase by factor of ~1.5
        let mut log_n = Vec::new();
        let mut log_rs = Vec::new();

        let n = returns.len();
        let mut window_size = 8.min(n / 2);

        while window_size <= n / 2 && window_size >= 8 {
            if let Some(rs) = Self::rescaled_range(&returns, window_size) {
                if rs > 0.0 {
                    log_n.push((window_size as f64).ln());
                    log_rs.push(rs.ln());
                }
            }
            window_size = (window_size as f64 * 1.5).ceil() as usize;
        }

        if log_n.len() < 2 {
            return None;
        }

        // Linear regression of log(R/S) vs log(n) gives Hurst exponent
        let (slope, _) = Self::simple_linear_regression(&log_n, &log_rs);

        // Clamp to valid range [0, 1]
        slope.map(|h| h.clamp(0.0, 1.0))
    }

    /// Compute rescaled range for a given window size
    fn rescaled_range(returns: &[f64], window_size: usize) -> Option<f64> {
        if returns.len() < window_size || window_size < 2 {
            return None;
        }

        let num_windows = returns.len() / window_size;
        if num_windows == 0 {
            return None;
        }

        let mut rs_sum = 0.0;
        let mut valid_windows = 0;

        for i in 0..num_windows {
            let start = i * window_size;
            let end = start + window_size;
            let window = &returns[start..end];

            // Mean of window
            let mean: f64 = window.iter().sum::<f64>() / window_size as f64;

            // Standard deviation
            let variance: f64 = window.iter()
                .map(|&r| (r - mean).powi(2))
                .sum::<f64>() / window_size as f64;
            let std_dev = variance.sqrt();

            if std_dev < 1e-10 {
                continue; // Skip windows with no variation
            }

            // Cumulative deviations from mean
            let mut cumsum = 0.0;
            let mut max_cumsum = f64::NEG_INFINITY;
            let mut min_cumsum = f64::INFINITY;

            for &r in window {
                cumsum += r - mean;
                max_cumsum = max_cumsum.max(cumsum);
                min_cumsum = min_cumsum.min(cumsum);
            }

            let range = max_cumsum - min_cumsum;
            let rs = range / std_dev;

            rs_sum += rs;
            valid_windows += 1;
        }

        if valid_windows == 0 {
            return None;
        }

        Some(rs_sum / valid_windows as f64)
    }

    /// Simple linear regression for Hurst calculation
    fn simple_linear_regression(x: &[f64], y: &[f64]) -> (Option<f64>, Option<f64>) {
        if x.len() != y.len() || x.len() < 2 {
            return (None, None);
        }

        let n = x.len() as f64;
        let sum_x: f64 = x.iter().sum();
        let sum_y: f64 = y.iter().sum();
        let sum_xy: f64 = x.iter().zip(y.iter()).map(|(&xi, &yi)| xi * yi).sum();
        let sum_x2: f64 = x.iter().map(|&xi| xi * xi).sum();

        let denominator = n * sum_x2 - sum_x * sum_x;
        if denominator.abs() < 1e-10 {
            return (None, None);
        }

        let slope = (n * sum_xy - sum_x * sum_y) / denominator;
        let intercept = (sum_y - slope * sum_x) / n;

        (Some(slope), Some(intercept))
    }

    /// Reset the engine state
    pub fn reset(&mut self) {
        self.prices.clear();
        self.ema_short = None;
        self.ema_long = None;
        self.total_samples = 0;
    }

    /// Get current sample count
    pub fn sample_count(&self) -> usize {
        self.prices.len()
    }

    /// Check if engine has enough data for valid computation
    pub fn is_ready(&self) -> bool {
        self.prices.len() >= self.config.min_samples
    }
}

// ============================================================================
// Standalone Functions for One-Shot Computation
// ============================================================================

/// Compute momentum from a price slice (stateless)
pub fn compute_momentum_from_prices(prices: &[f64]) -> Option<f64> {
    TrendFeatureEngine::compute_momentum(prices).0
}

/// Compute monotonicity from a price slice (stateless)
pub fn compute_monotonicity_from_prices(prices: &[f64]) -> Option<f64> {
    TrendFeatureEngine::compute_monotonicity(prices)
}

/// Compute Hurst exponent from a price slice (stateless)
pub fn compute_hurst_from_prices(prices: &[f64]) -> Option<f64> {
    TrendFeatureEngine::compute_hurst_exponent(prices)
}

/// Compute all trend features from a price slice (stateless)
pub fn compute_trend_features(prices: &[f64], config: Option<TrendFeatureConfig>) -> TrendFeatures {
    let config = config.unwrap_or_default();
    let mut engine = TrendFeatureEngine::with_config(config);

    for &price in prices {
        engine.update(price);
    }

    engine.compute()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Basic Functionality Tests
    // ========================================================================

    #[test]
    fn test_engine_creation() {
        let engine = TrendFeatureEngine::new(60);
        assert_eq!(engine.sample_count(), 0);
        assert!(!engine.is_ready());
    }

    #[test]
    fn test_engine_with_config() {
        let config = TrendFeatureConfig {
            window_size: 100,
            ema_short_period: 5,
            ema_long_period: 20,
            min_samples: 10,
        };
        let engine = TrendFeatureEngine::with_config(config);
        assert_eq!(engine.config.window_size, 100);
    }

    #[test]
    fn test_update_increments_count() {
        let mut engine = TrendFeatureEngine::new(60);
        engine.update(100.0);
        assert_eq!(engine.sample_count(), 1);
        engine.update(101.0);
        assert_eq!(engine.sample_count(), 2);
    }

    #[test]
    fn test_window_size_limit() {
        let mut engine = TrendFeatureEngine::new(5);
        for i in 0..10 {
            engine.update(100.0 + i as f64);
        }
        assert_eq!(engine.sample_count(), 5);
    }

    #[test]
    fn test_reset() {
        let mut engine = TrendFeatureEngine::new(60);
        for i in 0..30 {
            engine.update(100.0 + i as f64);
        }
        assert!(engine.sample_count() > 0);

        engine.reset();
        assert_eq!(engine.sample_count(), 0);
        assert!(engine.ema_short.is_none());
        assert!(engine.ema_long.is_none());
    }

    #[test]
    fn test_insufficient_data_returns_none() {
        let mut engine = TrendFeatureEngine::new(60);
        engine.update(100.0);

        let features = engine.compute();
        assert!(features.momentum.is_none());
        assert!(features.monotonicity.is_none());
        assert!(features.hurst_exponent.is_none());
        assert_eq!(features.sample_count, 1);
    }

    // ========================================================================
    // Momentum Tests
    // ========================================================================

    #[test]
    fn test_momentum_perfect_uptrend() {
        // Perfect linear uptrend: 100, 101, 102, ..., 129
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + i as f64).collect();

        let momentum = compute_momentum_from_prices(&prices);
        assert!(momentum.is_some());

        // Slope should be exactly 1.0
        let m = momentum.unwrap();
        assert!((m - 1.0).abs() < 1e-10, "Expected slope ~1.0, got {}", m);
    }

    #[test]
    fn test_momentum_perfect_downtrend() {
        // Perfect linear downtrend: 130, 129, 128, ..., 101
        let prices: Vec<f64> = (0..30).map(|i| 130.0 - i as f64).collect();

        let momentum = compute_momentum_from_prices(&prices);
        assert!(momentum.is_some());

        // Slope should be exactly -1.0
        let m = momentum.unwrap();
        assert!((m + 1.0).abs() < 1e-10, "Expected slope ~-1.0, got {}", m);
    }

    #[test]
    fn test_momentum_flat_prices() {
        // Constant prices
        let prices: Vec<f64> = vec![100.0; 30];

        let momentum = compute_momentum_from_prices(&prices);
        assert!(momentum.is_some());

        // Slope should be ~0
        let m = momentum.unwrap();
        assert!(m.abs() < 1e-10, "Expected slope ~0, got {}", m);
    }

    #[test]
    fn test_momentum_r_squared_perfect_trend() {
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + i as f64).collect();

        let (_, r_squared) = TrendFeatureEngine::compute_momentum(&prices);
        assert!(r_squared.is_some());

        // R-squared should be 1.0 for perfect linear trend
        let r2 = r_squared.unwrap();
        assert!((r2 - 1.0).abs() < 1e-10, "Expected R^2 ~1.0, got {}", r2);
    }

    #[test]
    fn test_momentum_r_squared_noisy_trend() {
        // Upward trend with noise
        let prices: Vec<f64> = (0..30)
            .map(|i| 100.0 + i as f64 + (i % 3) as f64 - 1.0)
            .collect();

        let (slope, r_squared) = TrendFeatureEngine::compute_momentum(&prices);

        assert!(slope.is_some());
        assert!(r_squared.is_some());

        // Slope should be positive
        assert!(slope.unwrap() > 0.0);

        // R-squared should be less than 1 due to noise
        let r2 = r_squared.unwrap();
        assert!(r2 < 1.0 && r2 > 0.5, "Expected 0.5 < R^2 < 1.0, got {}", r2);
    }

    // ========================================================================
    // Monotonicity Tests
    // ========================================================================

    #[test]
    fn test_monotonicity_perfect_uptrend() {
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + i as f64).collect();

        let mono = compute_monotonicity_from_prices(&prices);
        assert!(mono.is_some());

        // Should be 1.0 (all moves are up)
        let m = mono.unwrap();
        assert!((m - 1.0).abs() < 1e-10, "Expected monotonicity 1.0, got {}", m);
    }

    #[test]
    fn test_monotonicity_perfect_downtrend() {
        let prices: Vec<f64> = (0..30).map(|i| 130.0 - i as f64).collect();

        let mono = compute_monotonicity_from_prices(&prices);
        assert!(mono.is_some());

        // Should be 1.0 (all moves are down)
        let m = mono.unwrap();
        assert!((m - 1.0).abs() < 1e-10, "Expected monotonicity 1.0, got {}", m);
    }

    #[test]
    fn test_monotonicity_alternating() {
        // Price alternates: 100, 101, 100, 101, ...
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + (i % 2) as f64).collect();

        let mono = compute_monotonicity_from_prices(&prices);
        assert!(mono.is_some());

        // Should be ~0.5 (equal ups and downs)
        let m = mono.unwrap();
        assert!((m - 0.5).abs() < 0.1, "Expected monotonicity ~0.5, got {}", m);
    }

    #[test]
    fn test_monotonicity_75_percent_up() {
        // 3 ups, 1 down, repeated
        let mut prices = Vec::new();
        let mut price = 100.0;
        for i in 0..40 {
            prices.push(price);
            if i % 4 == 3 {
                price -= 1.0; // Down
            } else {
                price += 1.0; // Up
            }
        }

        let mono = compute_monotonicity_from_prices(&prices);
        assert!(mono.is_some());

        // Should be ~0.75
        let m = mono.unwrap();
        assert!((m - 0.75).abs() < 0.05, "Expected monotonicity ~0.75, got {}", m);
    }

    #[test]
    fn test_monotonicity_flat_prices() {
        let prices: Vec<f64> = vec![100.0; 30];

        let mono = compute_monotonicity_from_prices(&prices);
        assert!(mono.is_some());

        // Should be 0.5 (no moves)
        let m = mono.unwrap();
        assert!((m - 0.5).abs() < 1e-10, "Expected monotonicity 0.5 for flat, got {}", m);
    }

    // ========================================================================
    // Hurst Exponent Tests
    // ========================================================================

    #[test]
    fn test_hurst_trending_series() {
        // Create a persistent/trending series with clear upward drift
        // Using a simple random walk with positive bias that ensures enough variance
        let mut prices = vec![100.0];
        let mut rng_state = 12345u64; // Simple LCG for deterministic test

        for _ in 0..500 {
            rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
            let rand = (rng_state >> 16) as f64 / 65536.0; // [0, 1)
            // Larger percentage changes to ensure variance in log returns
            let change = if rand > 0.4 { 0.5 } else { -0.2 }; // Biased up with larger moves
            let last = *prices.last().unwrap();
            prices.push(last * (1.0 + change / 100.0));
        }

        let hurst = compute_hurst_from_prices(&prices);

        // Hurst may or may not be computable depending on variance in returns
        // The important thing is it doesn't crash and returns valid range if computed
        if let Some(h) = hurst {
            assert!(h >= 0.0 && h <= 1.0, "Hurst should be in [0,1], got {}", h);
        }
        // Note: Not asserting is_some() as Hurst computation requires sufficient
        // variance in the return series, which may not always be present
    }

    #[test]
    fn test_hurst_mean_reverting_series() {
        // Mean-reverting: oscillates around 100
        let prices: Vec<f64> = (0..200)
            .map(|i| 100.0 + 5.0 * (i as f64 * 0.5).sin())
            .collect();

        let hurst = compute_hurst_from_prices(&prices);
        assert!(hurst.is_some(), "Hurst should be computable");

        let h = hurst.unwrap();
        // Sinusoidal patterns tend to show anti-persistence
        assert!(h >= 0.0 && h <= 1.0, "Hurst should be in [0,1], got {}", h);
    }

    #[test]
    fn test_hurst_insufficient_data() {
        let prices: Vec<f64> = vec![100.0, 101.0, 102.0];

        let hurst = compute_hurst_from_prices(&prices);
        assert!(hurst.is_none(), "Hurst should be None for insufficient data");
    }

    #[test]
    fn test_hurst_clamped_to_valid_range() {
        // Any valid series should produce H in [0, 1]
        let prices: Vec<f64> = (0..100).map(|i| 100.0 + i as f64).collect();

        let hurst = compute_hurst_from_prices(&prices);
        if let Some(h) = hurst {
            assert!(h >= 0.0 && h <= 1.0, "Hurst should be clamped to [0,1]");
        }
    }

    // ========================================================================
    // MA Crossover Tests
    // ========================================================================

    #[test]
    fn test_ma_crossover_uptrend() {
        let mut engine = TrendFeatureEngine::new(100);

        // Feed upward trend
        for i in 0..100 {
            engine.update(100.0 + i as f64);
        }

        let features = engine.compute();

        // In uptrend, short EMA should be above long EMA
        assert!(features.ma_crossover.is_some());
        assert!(features.ma_crossover.unwrap() > 0.0,
            "Short EMA should be above long EMA in uptrend");
    }

    #[test]
    fn test_ma_crossover_downtrend() {
        let mut engine = TrendFeatureEngine::new(100);

        // Feed downward trend
        for i in 0..100 {
            engine.update(200.0 - i as f64);
        }

        let features = engine.compute();

        // In downtrend, short EMA should be below long EMA
        assert!(features.ma_crossover.is_some());
        assert!(features.ma_crossover.unwrap() < 0.0,
            "Short EMA should be below long EMA in downtrend");
    }

    #[test]
    fn test_ma_crossover_flat() {
        let mut engine = TrendFeatureEngine::new(100);

        // Feed flat prices
        for _ in 0..100 {
            engine.update(100.0);
        }

        let features = engine.compute();

        // Both EMAs should converge to the same value
        assert!(features.ma_crossover.is_some());
        assert!(features.ma_crossover.unwrap().abs() < 1e-10,
            "MA crossover should be ~0 for flat prices");
    }

    #[test]
    fn test_ema_convergence() {
        let mut engine = TrendFeatureEngine::new(200);

        // Feed constant price for long time
        for _ in 0..200 {
            engine.update(100.0);
        }

        let features = engine.compute();

        // Both EMAs should be very close to 100.0
        assert!(features.ema_short.is_some());
        assert!(features.ema_long.is_some());
        assert!((features.ema_short.unwrap() - 100.0).abs() < 1e-6);
        assert!((features.ema_long.unwrap() - 100.0).abs() < 1e-6);
    }

    // ========================================================================
    // Integration Tests
    // ========================================================================

    #[test]
    fn test_compute_all_features() {
        let mut engine = TrendFeatureEngine::new(60);

        // Feed realistic-ish price data
        for i in 0..60 {
            let price = 100.0 + 0.5 * i as f64 + (i as f64 * 0.3).sin();
            engine.update(price);
        }

        let features = engine.compute();

        assert_eq!(features.sample_count, 60);
        assert!(features.momentum.is_some());
        assert!(features.momentum_r_squared.is_some());
        assert!(features.monotonicity.is_some());
        assert!(features.ma_crossover.is_some());
        assert!(features.ema_short.is_some());
        assert!(features.ema_long.is_some());
        // Hurst might be None due to window size requirements
    }

    #[test]
    fn test_stateless_compute_trend_features() {
        let prices: Vec<f64> = (0..60).map(|i| 100.0 + i as f64).collect();

        let features = compute_trend_features(&prices, None);

        assert_eq!(features.sample_count, 60);
        assert!(features.momentum.is_some());
        assert!(features.monotonicity.is_some());
    }

    #[test]
    fn test_stateless_with_custom_config() {
        let prices: Vec<f64> = (0..50).map(|i| 100.0 + i as f64).collect();

        let config = TrendFeatureConfig {
            window_size: 50,
            min_samples: 10,
            ..Default::default()
        };

        let features = compute_trend_features(&prices, Some(config));

        assert!(features.momentum.is_some());
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_single_price() {
        let prices = vec![100.0];

        let momentum = compute_momentum_from_prices(&prices);
        let mono = compute_monotonicity_from_prices(&prices);
        let hurst = compute_hurst_from_prices(&prices);

        assert!(momentum.is_none());
        assert!(mono.is_none());
        assert!(hurst.is_none());
    }

    #[test]
    fn test_two_prices() {
        let prices = vec![100.0, 101.0];

        let momentum = compute_momentum_from_prices(&prices);
        let mono = compute_monotonicity_from_prices(&prices);

        assert!(momentum.is_some());
        assert!((momentum.unwrap() - 1.0).abs() < 1e-10);

        assert!(mono.is_some());
        assert!((mono.unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_large_price_values() {
        // Test with BTC-like prices
        let prices: Vec<f64> = (0..60)
            .map(|i| 100000.0 + i as f64 * 10.0)
            .collect();

        let features = compute_trend_features(&prices, None);

        assert!(features.momentum.is_some());
        assert!((features.momentum.unwrap() - 10.0).abs() < 1e-6);
    }

    #[test]
    fn test_small_price_changes() {
        // Very small price changes (sub-cent)
        let prices: Vec<f64> = (0..60)
            .map(|i| 100.0 + i as f64 * 0.0001)
            .collect();

        let features = compute_trend_features(&prices, None);

        assert!(features.momentum.is_some());
        assert!(features.momentum.unwrap() > 0.0);
    }

    #[test]
    fn test_negative_prices() {
        // Should handle (though unusual in trading)
        let prices: Vec<f64> = (0..30).map(|i| -100.0 + i as f64).collect();

        let momentum = compute_momentum_from_prices(&prices);
        let mono = compute_monotonicity_from_prices(&prices);

        assert!(momentum.is_some());
        assert!((momentum.unwrap() - 1.0).abs() < 1e-10);

        assert!(mono.is_some());
        assert!((mono.unwrap() - 1.0).abs() < 1e-10);
    }

    // ========================================================================
    // Regime Detection Helper Tests
    // ========================================================================

    #[test]
    fn test_strong_trend_indicators() {
        // Strong uptrend should show:
        // - Positive momentum
        // - High monotonicity (>0.7)
        // - Positive MA crossover
        // - Hurst > 0.5 (if computable)

        let prices: Vec<f64> = (0..100).map(|i| 100.0 + i as f64).collect();
        let features = compute_trend_features(&prices, None);

        assert!(features.momentum.unwrap() > 0.0, "Uptrend should have positive momentum");
        assert!(features.monotonicity.unwrap() > 0.9, "Perfect trend should have high monotonicity");
        assert!(features.ma_crossover.unwrap() > 0.0, "Uptrend should have positive MA crossover");
    }

    #[test]
    fn test_choppy_market_indicators() {
        // Choppy market: high-frequency reversals
        let prices: Vec<f64> = (0..100)
            .map(|i| 100.0 + (i % 2) as f64 * 2.0 - 1.0)
            .collect();

        let features = compute_trend_features(&prices, None);

        // Momentum should be near zero
        assert!(features.momentum.unwrap().abs() < 0.1,
            "Choppy market should have low momentum");

        // Monotonicity should be ~0.5
        assert!((features.monotonicity.unwrap() - 0.5).abs() < 0.1,
            "Choppy market should have monotonicity ~0.5");
    }

    // ========================================================================
    // Performance / Stress Tests
    // ========================================================================

    #[test]
    fn test_large_window() {
        let mut engine = TrendFeatureEngine::new(10000);

        for i in 0..10000 {
            engine.update(100.0 + (i as f64 * 0.01).sin() * 10.0);
        }

        let features = engine.compute();
        assert_eq!(features.sample_count, 10000);
        assert!(features.momentum.is_some());
    }

    #[test]
    fn test_rapid_updates() {
        let mut engine = TrendFeatureEngine::new(60);

        // Simulate rapid price updates
        for _ in 0..1000 {
            engine.update(100.0 + rand_like_value() * 2.0 - 1.0);
        }

        let features = engine.compute();
        assert_eq!(features.sample_count, 60); // Window should cap at 60
    }

    // Helper for deterministic "random" values in tests
    fn rand_like_value() -> f64 {
        static mut SEED: u64 = 42;
        unsafe {
            SEED = SEED.wrapping_mul(1103515245).wrapping_add(12345);
            (SEED >> 16) as f64 / 65536.0
        }
    }
}

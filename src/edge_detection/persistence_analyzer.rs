//! Persistence Analyzer Implementation - Task 1.2
//!
//! Analyzes how long trends persist after detection.
//! Builds distribution of trend durations segmented by regime.
//!
//! # Theory
//!
//! Trend persistence measures how long price movements continue in one direction
//! before reversing. This is critical for:
//! - Determining optimal holding periods
//! - Setting realistic take-profit/stop-loss levels
//! - Understanding regime-dependent behavior
//!
//! # Algorithm
//!
//! 1. Detect trend start when price moves > min_move_bps from anchor
//! 2. Track trend until reversal > reversal_threshold_bps from peak
//! 3. Record trend duration and magnitude
//! 4. Build rolling distribution of durations
//! 5. Segment by regime (MIDC quartiles)
//!
//! # Usage
//!
//! ```rust,ignore
//! use ingestor::edge_detection::{PersistenceAnalyzer, PersistenceConfig, PricePoint};
//!
//! let config = PersistenceConfig::default();
//! let mut analyzer = PersistenceAnalyzer::new(config);
//!
//! // Streaming updates
//! for price_point in price_stream {
//!     analyzer.on_price(&price_point);
//! }
//!
//! // Get statistics
//! let stats = analyzer.get_stats();
//! println!("Mean trend duration: {:.2}s", stats.mean_duration_seconds);
//! ```

use crate::core::{MIDCRegime, PersistenceStats};
use crate::edge_detection::{PersistenceConfig, PricePoint, ResearchError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

// ============================================================================
// Trend State
// ============================================================================

/// Current trend state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrendDirection {
    /// Upward trend (price rising)
    Up,
    /// Downward trend (price falling)
    Down,
    /// No active trend
    None,
}

impl Default for TrendDirection {
    fn default() -> Self {
        TrendDirection::None
    }
}

/// Active trend being tracked
#[derive(Debug, Clone)]
struct ActiveTrend {
    /// Direction of the trend
    direction: TrendDirection,
    /// Starting price (anchor)
    start_price: f64,
    /// Starting timestamp
    start_time: DateTime<Utc>,
    /// Peak price reached (for reversals)
    peak_price: f64,
    /// Peak timestamp
    peak_time: DateTime<Utc>,
    /// Current regime when trend started
    start_regime: MIDCRegime,
}

/// Completed trend record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedTrend {
    /// Direction of the trend
    pub direction: TrendDirection,
    /// Duration in seconds
    pub duration_seconds: f64,
    /// Magnitude in basis points
    pub magnitude_bps: f64,
    /// Peak magnitude before reversal (bps)
    pub peak_magnitude_bps: f64,
    /// Regime when trend started
    pub regime: MIDCRegime,
    /// Timestamp when trend completed
    pub completed_at: DateTime<Utc>,
}

// ============================================================================
// PersistenceAnalyzer
// ============================================================================

/// Analyzes how long trends persist after detection
///
/// Tracks trend durations and builds distributions segmented by regime.
#[derive(Debug, Clone)]
pub struct PersistenceAnalyzer {
    /// Configuration
    config: PersistenceConfig,

    /// Currently active trend (if any)
    active_trend: Option<ActiveTrend>,

    /// Previous price point (for movement calculation)
    prev_price: Option<PricePoint>,

    /// Rolling window of completed trends
    completed_trends: VecDeque<CompletedTrend>,

    /// Completed trends by regime
    trends_by_regime: HashMap<MIDCRegime, VecDeque<CompletedTrend>>,

    /// Current MIDC regime (external input)
    current_regime: MIDCRegime,

    /// Total trends observed
    total_trends: usize,

    /// Statistics cache
    stats_cache: Option<PersistenceStats>,

    /// Cache validity (number of trends when cache was built)
    cache_valid_at: usize,
}

/// Statistics for the persistence analyzer
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistenceAnalyzerStats {
    /// Total trends completed
    pub total_trends: usize,

    /// Trends by direction
    pub up_trends: usize,
    pub down_trends: usize,

    /// Active trend duration so far (if any)
    pub active_trend_duration_seconds: Option<f64>,

    /// Active trend direction
    pub active_trend_direction: TrendDirection,

    /// Trends per regime
    pub trends_by_regime: HashMap<MIDCRegime, usize>,

    /// Average magnitude (bps)
    pub avg_magnitude_bps: f64,
}

impl PersistenceAnalyzer {
    /// Create a new persistence analyzer with given configuration
    pub fn new(config: PersistenceConfig) -> Self {
        Self {
            config,
            active_trend: None,
            prev_price: None,
            completed_trends: VecDeque::new(),
            trends_by_regime: HashMap::new(),
            current_regime: MIDCRegime::Unknown,
            total_trends: 0,
            stats_cache: None,
            cache_valid_at: 0,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(PersistenceConfig::default())
    }

    /// Process a new price point (streaming mode)
    ///
    /// This is the primary interface for real-time updates.
    pub fn on_price(&mut self, price_point: &PricePoint) {
        if let Some(prev) = self.prev_price.take() {
            self.process_price_update(&prev, price_point);
        }
        self.prev_price = Some(price_point.clone());
    }

    /// Set the current MIDC regime
    ///
    /// Called externally when regime is updated by MIDCEstimator.
    pub fn set_regime(&mut self, regime: MIDCRegime) {
        self.current_regime = regime;
    }

    /// Get current regime
    pub fn regime(&self) -> MIDCRegime {
        self.current_regime
    }

    /// Get current trend direction
    pub fn current_trend(&self) -> TrendDirection {
        self.active_trend
            .as_ref()
            .map(|t| t.direction)
            .unwrap_or(TrendDirection::None)
    }

    /// Check if a trend is currently active
    pub fn has_active_trend(&self) -> bool {
        self.active_trend.is_some()
    }

    /// Get statistics for all trends
    pub fn get_stats(&mut self) -> PersistenceStats {
        // Return cached if valid
        if self.cache_valid_at == self.total_trends {
            if let Some(ref stats) = self.stats_cache {
                return stats.clone();
            }
        }

        let stats = self.compute_stats(&self.completed_trends);
        self.stats_cache = Some(stats.clone());
        self.cache_valid_at = self.total_trends;
        stats
    }

    /// Get statistics for a specific regime
    pub fn get_stats_by_regime(&self, regime: MIDCRegime) -> PersistenceStats {
        if let Some(trends) = self.trends_by_regime.get(&regime) {
            self.compute_stats(trends)
        } else {
            PersistenceStats::default()
        }
    }

    /// Get analyzer-specific statistics
    pub fn analyzer_stats(&self) -> PersistenceAnalyzerStats {
        let mut up_trends = 0;
        let mut down_trends = 0;
        let mut total_magnitude = 0.0;

        for trend in &self.completed_trends {
            match trend.direction {
                TrendDirection::Up => up_trends += 1,
                TrendDirection::Down => down_trends += 1,
                TrendDirection::None => {}
            }
            total_magnitude += trend.magnitude_bps.abs();
        }

        let avg_magnitude_bps = if self.total_trends > 0 {
            total_magnitude / self.total_trends as f64
        } else {
            0.0
        };

        let mut trends_by_regime = HashMap::new();
        for (regime, trends) in &self.trends_by_regime {
            trends_by_regime.insert(*regime, trends.len());
        }

        let (active_duration, active_direction) = if let Some(ref trend) = self.active_trend {
            let duration = if let Some(ref prev) = self.prev_price {
                (prev.timestamp - trend.start_time).num_milliseconds() as f64 / 1000.0
            } else {
                0.0
            };
            (Some(duration), trend.direction)
        } else {
            (None, TrendDirection::None)
        };

        PersistenceAnalyzerStats {
            total_trends: self.total_trends,
            up_trends,
            down_trends,
            active_trend_duration_seconds: active_duration,
            active_trend_direction: active_direction,
            trends_by_regime,
            avg_magnitude_bps,
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &PersistenceConfig {
        &self.config
    }

    /// Get recent completed trends
    pub fn recent_trends(&self, count: usize) -> Vec<CompletedTrend> {
        self.completed_trends
            .iter()
            .rev()
            .take(count)
            .cloned()
            .collect()
    }

    /// Get total number of completed trends
    pub fn total_trends(&self) -> usize {
        self.total_trends
    }

    /// Check if analyzer has enough data
    pub fn is_ready(&self) -> bool {
        self.total_trends >= 10
    }

    /// Reset the analyzer
    pub fn reset(&mut self) {
        self.active_trend = None;
        self.prev_price = None;
        self.completed_trends.clear();
        self.trends_by_regime.clear();
        self.current_regime = MIDCRegime::Unknown;
        self.total_trends = 0;
        self.stats_cache = None;
        self.cache_valid_at = 0;
    }

    // ========================================================================
    // Internal Methods
    // ========================================================================

    /// Process a price update
    fn process_price_update(&mut self, prev: &PricePoint, curr: &PricePoint) {
        let move_bps = self.compute_move_bps(prev.price, curr.price);

        if self.active_trend.is_some() {
            // Update active trend - need to check for reversal or continuation
            self.update_active_trend_inline(curr);
        } else {
            // Check for new trend start
            if move_bps.abs() >= self.config.min_move_bps {
                self.start_new_trend(prev, curr, move_bps);
            }
        }
    }

    /// Compute move in basis points
    fn compute_move_bps(&self, prev_price: f64, curr_price: f64) -> f64 {
        if prev_price <= 0.0 {
            return 0.0;
        }
        ((curr_price - prev_price) / prev_price) * 10000.0
    }

    /// Start a new trend
    fn start_new_trend(&mut self, prev: &PricePoint, curr: &PricePoint, _move_bps: f64) {
        let direction = if curr.price > prev.price {
            TrendDirection::Up
        } else {
            TrendDirection::Down
        };

        self.active_trend = Some(ActiveTrend {
            direction,
            start_price: prev.price,
            start_time: prev.timestamp,
            peak_price: curr.price,
            peak_time: curr.timestamp,
            start_regime: self.current_regime,
        });
    }

    /// Update an active trend (inlined to avoid borrow conflicts)
    fn update_active_trend_inline(&mut self, curr: &PricePoint) {
        // Extract needed values to avoid borrow conflicts
        let (direction, peak_price) = if let Some(ref trend) = self.active_trend {
            (trend.direction, trend.peak_price)
        } else {
            return;
        };

        // Calculate movement from peak
        let from_peak_bps = self.compute_move_bps(peak_price, curr.price);
        let reversal_threshold = self.config.reversal_threshold_bps;

        // Check for trend continuation or reversal
        let mut should_complete = false;
        match direction {
            TrendDirection::Up => {
                if curr.price > peak_price {
                    // New peak - update in place
                    if let Some(ref mut trend) = self.active_trend {
                        trend.peak_price = curr.price;
                        trend.peak_time = curr.timestamp;
                    }
                } else if from_peak_bps <= -reversal_threshold {
                    // Reversal detected
                    should_complete = true;
                }
            }
            TrendDirection::Down => {
                if curr.price < peak_price {
                    // New trough (peak in downtrend)
                    if let Some(ref mut trend) = self.active_trend {
                        trend.peak_price = curr.price;
                        trend.peak_time = curr.timestamp;
                    }
                } else if from_peak_bps >= reversal_threshold {
                    // Reversal detected
                    should_complete = true;
                }
            }
            TrendDirection::None => {}
        }

        // Check for max duration
        if !should_complete {
            if let Some(ref trend) = self.active_trend {
                let duration = (curr.timestamp - trend.start_time).num_milliseconds() as f64 / 1000.0;
                if duration >= self.config.max_duration_seconds {
                    should_complete = true;
                }
            }
        }

        if should_complete {
            self.complete_trend(curr);
        }
    }

    /// Complete the current trend
    fn complete_trend(&mut self, curr: &PricePoint) {
        if let Some(trend) = self.active_trend.take() {
            let duration = (curr.timestamp - trend.start_time).num_milliseconds() as f64 / 1000.0;
            let magnitude_bps = self.compute_move_bps(trend.start_price, curr.price);
            let peak_magnitude_bps = self.compute_move_bps(trend.start_price, trend.peak_price);

            let completed = CompletedTrend {
                direction: trend.direction,
                duration_seconds: duration,
                magnitude_bps,
                peak_magnitude_bps,
                regime: trend.start_regime,
                completed_at: curr.timestamp,
            };

            // Add to rolling window
            self.completed_trends.push_back(completed.clone());
            while self.completed_trends.len() > self.config.stats_window {
                self.completed_trends.pop_front();
            }

            // Add to regime-specific window
            let regime_trends = self
                .trends_by_regime
                .entry(trend.start_regime)
                .or_insert_with(VecDeque::new);
            regime_trends.push_back(completed);
            while regime_trends.len() > self.config.stats_window {
                regime_trends.pop_front();
            }

            self.total_trends += 1;
        }
    }

    /// Compute statistics from a collection of trends
    fn compute_stats(&self, trends: &VecDeque<CompletedTrend>) -> PersistenceStats {
        if trends.is_empty() {
            return PersistenceStats::default();
        }

        let mut durations: Vec<f64> = trends.iter().map(|t| t.duration_seconds).collect();
        durations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = durations.len();
        let mean = durations.iter().sum::<f64>() / n as f64;
        let median = if n % 2 == 0 {
            (durations[n / 2 - 1] + durations[n / 2]) / 2.0
        } else {
            durations[n / 2]
        };

        let variance = durations.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / n as f64;
        let std = variance.sqrt();

        let percentile_25 = self.percentile(&durations, 25.0);
        let percentile_75 = self.percentile(&durations, 75.0);

        PersistenceStats {
            mean_duration_seconds: mean,
            median_duration_seconds: median,
            std_duration_seconds: std,
            percentile_25,
            percentile_75,
            sample_count: n,
            updated_at: Utc::now(),
        }
    }

    /// Compute percentile from sorted data
    fn percentile(&self, sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

// ============================================================================
// PersistenceAnalyzerBuilder
// ============================================================================

/// Builder for PersistenceAnalyzer with fluent API
#[derive(Debug, Clone)]
pub struct PersistenceAnalyzerBuilder {
    config: PersistenceConfig,
}

impl PersistenceAnalyzerBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: PersistenceConfig::default(),
        }
    }

    /// Set minimum move threshold (bps)
    pub fn with_min_move_bps(mut self, bps: f64) -> Self {
        self.config.min_move_bps = bps;
        self
    }

    /// Set reversal threshold (bps)
    pub fn with_reversal_threshold_bps(mut self, bps: f64) -> Self {
        self.config.reversal_threshold_bps = bps;
        self
    }

    /// Set maximum duration (seconds)
    pub fn with_max_duration_seconds(mut self, seconds: f64) -> Self {
        self.config.max_duration_seconds = seconds;
        self
    }

    /// Set stats window size
    pub fn with_stats_window(mut self, size: usize) -> Self {
        self.config.stats_window = size;
        self
    }

    /// Build the analyzer
    pub fn build(self) -> PersistenceAnalyzer {
        PersistenceAnalyzer::new(self.config)
    }
}

impl Default for PersistenceAnalyzerBuilder {
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

    // Helper to create uptrend followed by reversal
    fn make_uptrend_reversal(
        start: f64,
        peak_move_bps: f64,
        reversal_bps: f64,
        points: usize,
    ) -> Vec<f64> {
        let mut prices = Vec::with_capacity(points);
        let peak = start * (1.0 + peak_move_bps / 10000.0);
        let end = peak * (1.0 - reversal_bps / 10000.0);

        // Upward leg
        let up_points = points / 2;
        for i in 0..up_points {
            let p = start + (peak - start) * (i as f64 / up_points as f64);
            prices.push(p);
        }

        // Downward leg (reversal)
        let down_points = points - up_points;
        for i in 0..down_points {
            let p = peak - (peak - end) * (i as f64 / down_points as f64);
            prices.push(p);
        }

        prices
    }

    // ========================================================================
    // Constructor Tests
    // ========================================================================

    #[test]
    fn test_new_with_default_config() {
        let config = PersistenceConfig::default();
        let analyzer = PersistenceAnalyzer::new(config);

        assert_eq!(analyzer.total_trends(), 0);
        assert!(!analyzer.is_ready());
        assert!(!analyzer.has_active_trend());
    }

    #[test]
    fn test_with_defaults() {
        let analyzer = PersistenceAnalyzer::with_defaults();
        assert_eq!(analyzer.config().min_move_bps, 5.0);
        assert_eq!(analyzer.config().reversal_threshold_bps, 10.0);
    }

    #[test]
    fn test_builder_pattern() {
        let analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(10.0)
            .with_reversal_threshold_bps(20.0)
            .with_max_duration_seconds(1800.0)
            .with_stats_window(100)
            .build();

        assert_eq!(analyzer.config().min_move_bps, 10.0);
        assert_eq!(analyzer.config().reversal_threshold_bps, 20.0);
        assert_eq!(analyzer.config().max_duration_seconds, 1800.0);
        assert_eq!(analyzer.config().stats_window, 100);
    }

    // ========================================================================
    // Trend Detection Tests
    // ========================================================================

    #[test]
    fn test_no_trend_on_small_move() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(10.0)
            .build();

        // Small move (less than 10 bps)
        let prices = make_price_points(&[100.0, 100.05, 100.08], 1000);

        for p in &prices {
            analyzer.on_price(p);
        }

        assert!(!analyzer.has_active_trend());
        assert_eq!(analyzer.total_trends(), 0);
    }

    #[test]
    fn test_uptrend_detection() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(10.0)
            .with_reversal_threshold_bps(20.0)
            .build();

        // Move up by 20 bps (0.2%)
        let prices = make_price_points(&[100.0, 100.20], 1000);

        for p in &prices {
            analyzer.on_price(p);
        }

        assert!(analyzer.has_active_trend());
        assert_eq!(analyzer.current_trend(), TrendDirection::Up);
    }

    #[test]
    fn test_downtrend_detection() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(10.0)
            .with_reversal_threshold_bps(20.0)
            .build();

        // Move down by 20 bps (0.2%)
        let prices = make_price_points(&[100.0, 99.80], 1000);

        for p in &prices {
            analyzer.on_price(p);
        }

        assert!(analyzer.has_active_trend());
        assert_eq!(analyzer.current_trend(), TrendDirection::Down);
    }

    #[test]
    fn test_uptrend_peak_update() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        // Steady uptrend
        let prices = make_price_points(&[100.0, 100.10, 100.20, 100.30, 100.40], 1000);

        for p in &prices {
            analyzer.on_price(p);
        }

        // Should still be in uptrend (no reversal yet)
        assert!(analyzer.has_active_trend());
        assert_eq!(analyzer.current_trend(), TrendDirection::Up);
        assert_eq!(analyzer.total_trends(), 0); // Not completed
    }

    #[test]
    fn test_uptrend_reversal_completion() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0) // 10 bps reversal
            .build();

        // Uptrend then reversal: start at 100, peak at 100.30 (+30 bps), reverse to 99.80 (-50 bps from peak)
        let prices = make_price_points(&[100.0, 100.10, 100.20, 100.30, 100.00, 99.80], 1000);

        for p in &prices {
            analyzer.on_price(p);
        }

        // Should have completed one trend
        assert_eq!(analyzer.total_trends(), 1);

        let recent = analyzer.recent_trends(1);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].direction, TrendDirection::Up);
    }

    #[test]
    fn test_downtrend_reversal_completion() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        // Downtrend then reversal
        let prices = make_price_points(&[100.0, 99.90, 99.80, 99.70, 100.00, 100.20], 1000);

        for p in &prices {
            analyzer.on_price(p);
        }

        assert_eq!(analyzer.total_trends(), 1);
        let recent = analyzer.recent_trends(1);
        assert_eq!(recent[0].direction, TrendDirection::Down);
    }

    #[test]
    fn test_multiple_trends() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        // First uptrend then reversal
        let prices1 = make_price_points(&[100.0, 100.20, 100.30, 99.90, 99.80], 1000);
        for p in &prices1 {
            analyzer.on_price(p);
        }

        // Second trend (down then reverse)
        let base_time = Utc::now() + Duration::seconds(10);
        let prices2: Vec<PricePoint> = [99.80, 99.60, 99.50, 99.80, 100.00]
            .iter()
            .enumerate()
            .map(|(i, &price)| PricePoint {
                timestamp: base_time + Duration::milliseconds(i as i64 * 1000),
                price,
                volume: None,
            })
            .collect();

        for p in &prices2 {
            analyzer.on_price(p);
        }

        assert!(analyzer.total_trends() >= 2);
    }

    // ========================================================================
    // Duration Calculation Tests
    // ========================================================================

    #[test]
    fn test_trend_duration_calculation() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        let base_time = Utc::now();
        let prices = vec![
            PricePoint::new(base_time, 100.0),
            PricePoint::new(base_time + Duration::seconds(1), 100.10),
            PricePoint::new(base_time + Duration::seconds(2), 100.20),
            PricePoint::new(base_time + Duration::seconds(3), 100.30),
            PricePoint::new(base_time + Duration::seconds(4), 99.80), // Reversal
        ];

        for p in &prices {
            analyzer.on_price(p);
        }

        assert_eq!(analyzer.total_trends(), 1);
        let recent = analyzer.recent_trends(1);
        // Duration should be ~4 seconds (from t=0 to t=4)
        assert!(recent[0].duration_seconds > 3.0);
        assert!(recent[0].duration_seconds < 5.0);
    }

    #[test]
    fn test_max_duration_completion() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(100.0) // Very high threshold
            .with_max_duration_seconds(2.0)      // Short max duration
            .build();

        let base_time = Utc::now();
        let prices = vec![
            PricePoint::new(base_time, 100.0),
            PricePoint::new(base_time + Duration::seconds(1), 100.10),
            PricePoint::new(base_time + Duration::seconds(2), 100.20),
            PricePoint::new(base_time + Duration::seconds(3), 100.30), // Exceeds max duration
        ];

        for p in &prices {
            analyzer.on_price(p);
        }

        // Should have completed due to max duration
        assert_eq!(analyzer.total_trends(), 1);
    }

    // ========================================================================
    // Statistics Tests
    // ========================================================================

    #[test]
    fn test_empty_stats() {
        let mut analyzer = PersistenceAnalyzer::with_defaults();
        let stats = analyzer.get_stats();

        assert_eq!(stats.sample_count, 0);
        assert_eq!(stats.mean_duration_seconds, 0.0);
    }

    #[test]
    fn test_stats_with_single_trend() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        let base_time = Utc::now();
        let prices = vec![
            PricePoint::new(base_time, 100.0),
            PricePoint::new(base_time + Duration::seconds(5), 100.20),
            PricePoint::new(base_time + Duration::seconds(10), 99.80), // Reversal
        ];

        for p in &prices {
            analyzer.on_price(p);
        }

        let stats = analyzer.get_stats();
        assert_eq!(stats.sample_count, 1);
        assert!(stats.mean_duration_seconds > 0.0);
        assert_eq!(stats.mean_duration_seconds, stats.median_duration_seconds);
    }

    #[test]
    fn test_stats_with_multiple_trends() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        // Generate multiple trends with different durations
        let base_time = Utc::now();
        let mut all_prices = Vec::new();
        let durations = [2.0, 4.0, 6.0, 8.0, 10.0]; // seconds

        let mut current_time = base_time;
        let mut price = 100.0;

        for &dur in &durations {
            // Start trend
            all_prices.push(PricePoint::new(current_time, price));
            current_time = current_time + Duration::milliseconds((dur * 500.0) as i64);
            price *= 1.002; // 20 bps up
            all_prices.push(PricePoint::new(current_time, price));
            current_time = current_time + Duration::milliseconds((dur * 500.0) as i64);
            // Reversal
            price *= 0.998; // 20 bps down
            all_prices.push(PricePoint::new(current_time, price));
            current_time = current_time + Duration::seconds(1);
        }

        for p in &all_prices {
            analyzer.on_price(p);
        }

        let stats = analyzer.get_stats();
        assert!(stats.sample_count >= 3);
        assert!(stats.std_duration_seconds >= 0.0);
    }

    #[test]
    fn test_stats_percentiles() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(3.0)
            .with_reversal_threshold_bps(8.0)
            .build();

        // Generate trends with varying durations
        let base_time = Utc::now();
        let mut all_prices = Vec::new();
        let mut current_time = base_time;
        let mut price = 100.0;

        for i in 0..20 {
            let duration_secs = (i % 5 + 1) as f64;
            all_prices.push(PricePoint::new(current_time, price));
            current_time = current_time + Duration::milliseconds((duration_secs * 1000.0) as i64);
            price *= 1.001;
            all_prices.push(PricePoint::new(current_time, price));
            price *= 0.999;
            all_prices.push(PricePoint::new(current_time + Duration::milliseconds(100), price));
            current_time = current_time + Duration::seconds(1);
        }

        for p in &all_prices {
            analyzer.on_price(p);
        }

        let stats = analyzer.get_stats();
        assert!(stats.percentile_25 <= stats.median_duration_seconds);
        assert!(stats.median_duration_seconds <= stats.percentile_75);
    }

    // ========================================================================
    // Regime Segmentation Tests
    // ========================================================================

    #[test]
    fn test_regime_tracking() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        // Set regime before trend
        analyzer.set_regime(MIDCRegime::SlowDiffusion);

        let prices = make_price_points(&[100.0, 100.20, 100.30, 99.80], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        assert_eq!(analyzer.total_trends(), 1);
        let recent = analyzer.recent_trends(1);
        assert_eq!(recent[0].regime, MIDCRegime::SlowDiffusion);
    }

    #[test]
    fn test_stats_by_regime() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        // Trend in SlowDiffusion regime
        analyzer.set_regime(MIDCRegime::SlowDiffusion);
        let prices1 = make_price_points(&[100.0, 100.20, 100.30, 99.80], 1000);
        for p in &prices1 {
            analyzer.on_price(p);
        }

        // Trend in FastDiffusion regime
        analyzer.set_regime(MIDCRegime::FastDiffusion);
        let base_time = Utc::now() + Duration::seconds(10);
        let prices2: Vec<PricePoint> = [99.80, 100.10, 100.30, 99.90, 99.70]
            .iter()
            .enumerate()
            .map(|(i, &price)| PricePoint {
                timestamp: base_time + Duration::milliseconds(i as i64 * 1000),
                price,
                volume: None,
            })
            .collect();

        for p in &prices2 {
            analyzer.on_price(p);
        }

        let slow_stats = analyzer.get_stats_by_regime(MIDCRegime::SlowDiffusion);
        let fast_stats = analyzer.get_stats_by_regime(MIDCRegime::FastDiffusion);

        assert!(slow_stats.sample_count >= 1);
        assert!(fast_stats.sample_count >= 1);
    }

    #[test]
    fn test_stats_by_regime_empty() {
        let analyzer = PersistenceAnalyzer::with_defaults();
        let stats = analyzer.get_stats_by_regime(MIDCRegime::ModerateDiffusion);
        assert_eq!(stats.sample_count, 0);
    }

    // ========================================================================
    // Rolling Window Tests
    // ========================================================================

    #[test]
    fn test_rolling_window() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(2.0)
            .with_reversal_threshold_bps(5.0)
            .with_stats_window(5) // Small window
            .build();

        let base_time = Utc::now();
        let mut current_time = base_time;
        let mut price = 100.0;

        // Generate 10 trends
        for _ in 0..10 {
            let p1 = PricePoint::new(current_time, price);
            current_time = current_time + Duration::seconds(1);
            price *= 1.001;
            let p2 = PricePoint::new(current_time, price);
            current_time = current_time + Duration::milliseconds(100);
            price *= 0.999;
            let p3 = PricePoint::new(current_time, price);

            analyzer.on_price(&p1);
            analyzer.on_price(&p2);
            analyzer.on_price(&p3);

            current_time = current_time + Duration::seconds(1);
        }

        // Total should be high, but stats window should be limited
        assert!(analyzer.total_trends() >= 5);
        let stats = analyzer.get_stats();
        assert!(stats.sample_count <= 5); // Limited by stats_window
    }

    // ========================================================================
    // Reset Tests
    // ========================================================================

    #[test]
    fn test_reset() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        analyzer.set_regime(MIDCRegime::SlowDiffusion);
        let prices = make_price_points(&[100.0, 100.20, 100.30, 99.80], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        assert!(analyzer.total_trends() > 0);

        analyzer.reset();

        assert_eq!(analyzer.total_trends(), 0);
        assert!(!analyzer.has_active_trend());
        assert_eq!(analyzer.regime(), MIDCRegime::Unknown);
        assert!(!analyzer.is_ready());
    }

    // ========================================================================
    // Analyzer Stats Tests
    // ========================================================================

    #[test]
    fn test_analyzer_stats() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        // One uptrend
        let prices = make_price_points(&[100.0, 100.20, 100.30, 99.80], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        let stats = analyzer.analyzer_stats();
        assert_eq!(stats.total_trends, 1);
        assert_eq!(stats.up_trends, 1);
        assert_eq!(stats.down_trends, 0);
    }

    #[test]
    fn test_analyzer_stats_active_trend() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(50.0) // High threshold
            .build();

        // Start trend but don't complete
        let base_time = Utc::now();
        let prices = vec![
            PricePoint::new(base_time, 100.0),
            PricePoint::new(base_time + Duration::seconds(2), 100.20),
        ];

        for p in &prices {
            analyzer.on_price(p);
        }

        let stats = analyzer.analyzer_stats();
        assert!(stats.active_trend_duration_seconds.is_some());
        assert_eq!(stats.active_trend_direction, TrendDirection::Up);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_zero_price() {
        let mut analyzer = PersistenceAnalyzer::with_defaults();

        let prices = make_price_points(&[0.0, 100.0, 101.0], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        // Should handle gracefully (no crash)
    }

    #[test]
    fn test_negative_price() {
        let mut analyzer = PersistenceAnalyzer::with_defaults();

        let prices = make_price_points(&[-100.0, -99.0, -98.0], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        // Should handle gracefully
    }

    #[test]
    fn test_constant_prices() {
        let mut analyzer = PersistenceAnalyzer::with_defaults();

        let prices = make_price_points(&[100.0, 100.0, 100.0, 100.0], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        assert!(!analyzer.has_active_trend());
        assert_eq!(analyzer.total_trends(), 0);
    }

    #[test]
    fn test_single_price() {
        let mut analyzer = PersistenceAnalyzer::with_defaults();

        let prices = make_price_points(&[100.0], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        assert!(!analyzer.has_active_trend());
    }

    #[test]
    fn test_large_price_jump() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        let prices = make_price_points(&[100.0, 200.0, 50.0], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        // Should detect trend and complete
        assert_eq!(analyzer.total_trends(), 1);
    }

    // ========================================================================
    // Recent Trends Tests
    // ========================================================================

    #[test]
    fn test_recent_trends_empty() {
        let analyzer = PersistenceAnalyzer::with_defaults();
        let recent = analyzer.recent_trends(5);
        assert!(recent.is_empty());
    }

    #[test]
    fn test_recent_trends_limit() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(2.0)
            .with_reversal_threshold_bps(5.0)
            .build();

        let base_time = Utc::now();
        let mut current_time = base_time;
        let mut price = 100.0;

        // Generate 5 trends
        for _ in 0..5 {
            let p1 = PricePoint::new(current_time, price);
            current_time = current_time + Duration::seconds(1);
            price *= 1.001;
            let p2 = PricePoint::new(current_time, price);
            current_time = current_time + Duration::milliseconds(100);
            price *= 0.999;
            let p3 = PricePoint::new(current_time, price);

            analyzer.on_price(&p1);
            analyzer.on_price(&p2);
            analyzer.on_price(&p3);

            current_time = current_time + Duration::seconds(1);
        }

        let recent = analyzer.recent_trends(3);
        assert!(recent.len() <= 3);
    }

    // ========================================================================
    // Ready State Tests
    // ========================================================================

    #[test]
    fn test_is_ready() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(1.0)
            .with_reversal_threshold_bps(2.0)
            .build();

        assert!(!analyzer.is_ready());

        let base_time = Utc::now();
        let mut current_time = base_time;
        let mut price = 100.0;

        // Generate 15 trends
        for _ in 0..15 {
            let p1 = PricePoint::new(current_time, price);
            current_time = current_time + Duration::seconds(1);
            price *= 1.0005;
            let p2 = PricePoint::new(current_time, price);
            current_time = current_time + Duration::milliseconds(100);
            price *= 0.9995;
            let p3 = PricePoint::new(current_time, price);

            analyzer.on_price(&p1);
            analyzer.on_price(&p2);
            analyzer.on_price(&p3);

            current_time = current_time + Duration::seconds(1);
        }

        assert!(analyzer.is_ready());
    }

    // ========================================================================
    // Config Getter Tests
    // ========================================================================

    #[test]
    fn test_config_getter() {
        let config = PersistenceConfig {
            min_move_bps: 15.0,
            reversal_threshold_bps: 25.0,
            max_duration_seconds: 1200.0,
            stats_window: 200,
        };

        let analyzer = PersistenceAnalyzer::new(config);

        assert_eq!(analyzer.config().min_move_bps, 15.0);
        assert_eq!(analyzer.config().reversal_threshold_bps, 25.0);
        assert_eq!(analyzer.config().max_duration_seconds, 1200.0);
        assert_eq!(analyzer.config().stats_window, 200);
    }

    // ========================================================================
    // Serialization Tests
    // ========================================================================

    #[test]
    fn test_trend_direction_serialization() {
        let directions = vec![TrendDirection::Up, TrendDirection::Down, TrendDirection::None];

        for dir in directions {
            let json = serde_json::to_string(&dir).unwrap();
            let deserialized: TrendDirection = serde_json::from_str(&json).unwrap();
            assert_eq!(dir, deserialized);
        }
    }

    #[test]
    fn test_completed_trend_serialization() {
        let trend = CompletedTrend {
            direction: TrendDirection::Up,
            duration_seconds: 10.5,
            magnitude_bps: 25.0,
            peak_magnitude_bps: 30.0,
            regime: MIDCRegime::SlowDiffusion,
            completed_at: Utc::now(),
        };

        let json = serde_json::to_string(&trend).unwrap();
        let deserialized: CompletedTrend = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.direction, TrendDirection::Up);
        assert_eq!(deserialized.duration_seconds, 10.5);
        assert_eq!(deserialized.magnitude_bps, 25.0);
    }

    #[test]
    fn test_analyzer_stats_serialization() {
        let mut stats = PersistenceAnalyzerStats::default();
        stats.total_trends = 50;
        stats.up_trends = 30;
        stats.down_trends = 20;
        stats.avg_magnitude_bps = 15.5;

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: PersistenceAnalyzerStats = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.total_trends, 50);
        assert_eq!(deserialized.up_trends, 30);
        assert_eq!(deserialized.down_trends, 20);
    }

    // ========================================================================
    // Cache Tests
    // ========================================================================

    #[test]
    fn test_stats_caching() {
        let mut analyzer = PersistenceAnalyzerBuilder::new()
            .with_min_move_bps(5.0)
            .with_reversal_threshold_bps(10.0)
            .build();

        let prices = make_price_points(&[100.0, 100.20, 100.30, 99.80], 1000);
        for p in &prices {
            analyzer.on_price(p);
        }

        // First call computes stats
        let stats1 = analyzer.get_stats();
        // Second call should use cache
        let stats2 = analyzer.get_stats();

        assert_eq!(stats1.sample_count, stats2.sample_count);
    }
}

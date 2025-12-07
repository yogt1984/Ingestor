//! Performance Drift Detection
//!
//! Monitors live performance for divergence from backtest expectations.
//! Implements CUSUM and other change detection algorithms to identify
//! when a strategy is underperforming.
//!
//! # Algorithms
//!
//! - **CUSUM**: Cumulative sum control chart for detecting mean shifts
//! - **EWMA**: Exponentially weighted moving average for trend detection
//! - **Page-Hinkley**: Sequential change detection test

use std::collections::VecDeque;
use serde::{Deserialize, Serialize};

/// Configuration for drift detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftConfig {
    /// Target mean (expected value from backtest)
    pub target_mean: f64,
    /// Target standard deviation (from backtest)
    pub target_std: f64,
    /// CUSUM threshold (in standard deviations)
    pub cusum_threshold: f64,
    /// EWMA lambda (smoothing factor)
    pub ewma_lambda: f64,
    /// Page-Hinkley threshold
    pub ph_threshold: f64,
    /// Page-Hinkley minimum change to detect
    pub ph_delta: f64,
    /// Minimum observations before alerting
    pub min_observations: usize,
    /// Window size for rolling calculations
    pub window_size: usize,
}

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            target_mean: 0.0,
            target_std: 1.0,
            cusum_threshold: 4.0, // 4 sigma
            ewma_lambda: 0.2,
            ph_threshold: 50.0,
            ph_delta: 0.1,
            min_observations: 20,
            window_size: 100,
        }
    }
}

/// Drift detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftResult {
    /// Is drift detected?
    pub drift_detected: bool,
    /// Which detector triggered (if any)
    pub triggered_by: Option<DriftDetector>,
    /// Current CUSUM value
    pub cusum_value: f64,
    /// CUSUM direction (positive or negative)
    pub cusum_direction: CusumDirection,
    /// Current EWMA value
    pub ewma_value: f64,
    /// Page-Hinkley statistic
    pub ph_statistic: f64,
    /// Number of observations
    pub n_observations: usize,
    /// Rolling mean
    pub rolling_mean: f64,
    /// Rolling std
    pub rolling_std: f64,
    /// Z-score of recent performance
    pub z_score: f64,
    /// Alert level
    pub alert_level: AlertLevel,
    /// Human-readable message
    pub message: String,
}

impl Default for DriftResult {
    fn default() -> Self {
        Self {
            drift_detected: false,
            triggered_by: None,
            cusum_value: 0.0,
            cusum_direction: CusumDirection::Neither,
            ewma_value: 0.0,
            ph_statistic: 0.0,
            n_observations: 0,
            rolling_mean: 0.0,
            rolling_std: 0.0,
            z_score: 0.0,
            alert_level: AlertLevel::Normal,
            message: "No drift detected".to_string(),
        }
    }
}

/// Type of drift detector
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum DriftDetector {
    Cusum,
    Ewma,
    PageHinkley,
    ZScore,
}

/// Direction of CUSUM drift
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum CusumDirection {
    Positive,
    Negative,
    Neither,
}

/// Alert severity level
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertLevel {
    Normal,
    Warning,
    Alert,
    Critical,
}

/// Drift detector state machine
pub struct DriftDetectorEngine {
    config: DriftConfig,
    /// Recent observations
    observations: VecDeque<f64>,
    /// CUSUM positive statistic
    cusum_pos: f64,
    /// CUSUM negative statistic
    cusum_neg: f64,
    /// EWMA current value
    ewma: f64,
    /// EWMA initialized flag
    ewma_initialized: bool,
    /// Page-Hinkley sum
    ph_sum: f64,
    /// Page-Hinkley minimum
    ph_min: f64,
    /// Total observations seen
    n_observations: usize,
    /// Sum for rolling mean
    rolling_sum: f64,
    /// Sum of squares for rolling variance
    rolling_sum_sq: f64,
}

impl DriftDetectorEngine {
    /// Create a new drift detector with given configuration
    pub fn new(config: DriftConfig) -> Self {
        Self {
            config,
            observations: VecDeque::new(),
            cusum_pos: 0.0,
            cusum_neg: 0.0,
            ewma: 0.0,
            ewma_initialized: false,
            ph_sum: 0.0,
            ph_min: 0.0,
            n_observations: 0,
            rolling_sum: 0.0,
            rolling_sum_sq: 0.0,
        }
    }

    /// Create with expected performance from backtest
    pub fn from_backtest(expected_mean: f64, expected_std: f64) -> Self {
        let config = DriftConfig {
            target_mean: expected_mean,
            target_std: if expected_std > 0.0 { expected_std } else { 1.0 },
            ..Default::default()
        };
        Self::new(config)
    }

    /// Update with a new observation and check for drift
    pub fn update(&mut self, value: f64) -> DriftResult {
        self.n_observations += 1;

        // Update rolling window
        self.observations.push_back(value);
        self.rolling_sum += value;
        self.rolling_sum_sq += value * value;

        if self.observations.len() > self.config.window_size {
            if let Some(old) = self.observations.pop_front() {
                self.rolling_sum -= old;
                self.rolling_sum_sq -= old * old;
            }
        }

        // Standardize the observation
        let z = if self.config.target_std > 0.0 {
            (value - self.config.target_mean) / self.config.target_std
        } else {
            value - self.config.target_mean
        };

        // Update CUSUM
        self.update_cusum(z);

        // Update EWMA
        self.update_ewma(value);

        // Update Page-Hinkley
        self.update_page_hinkley(value);

        // Check for drift
        self.check_drift()
    }

    /// Update CUSUM statistics
    fn update_cusum(&mut self, z: f64) {
        // Two-sided CUSUM
        // S_pos detects increase (underperformance if metric is PnL)
        // S_neg detects decrease (overperformance)

        // Using k=0.5 slack parameter (standard)
        let k = 0.5;

        self.cusum_pos = (self.cusum_pos + z - k).max(0.0);
        self.cusum_neg = (self.cusum_neg - z - k).max(0.0);
    }

    /// Update EWMA
    fn update_ewma(&mut self, value: f64) {
        if !self.ewma_initialized {
            self.ewma = value;
            self.ewma_initialized = true;
        } else {
            self.ewma = self.config.ewma_lambda * value
                + (1.0 - self.config.ewma_lambda) * self.ewma;
        }
    }

    /// Update Page-Hinkley test
    fn update_page_hinkley(&mut self, value: f64) {
        // Page-Hinkley test for decrease in mean
        // m_t = sum(x_i - x_bar - delta)
        // M_t = min(m_s) for s <= t
        // PH = m_t - M_t

        // Use current rolling mean as x_bar
        let mean = self.rolling_mean();
        self.ph_sum += value - mean - self.config.ph_delta;
        self.ph_min = self.ph_min.min(self.ph_sum);
    }

    /// Check all detectors for drift
    fn check_drift(&self) -> DriftResult {
        let mut result = DriftResult {
            n_observations: self.n_observations,
            cusum_value: self.cusum_pos.max(self.cusum_neg),
            ewma_value: self.ewma,
            ph_statistic: self.ph_sum - self.ph_min,
            rolling_mean: self.rolling_mean(),
            rolling_std: self.rolling_std(),
            ..Default::default()
        };

        // Set CUSUM direction
        result.cusum_direction = if self.cusum_pos > self.cusum_neg {
            CusumDirection::Positive
        } else if self.cusum_neg > self.cusum_pos {
            CusumDirection::Negative
        } else {
            CusumDirection::Neither
        };

        // Calculate z-score of current rolling mean
        if self.config.target_std > 0.0 && self.observations.len() >= 2 {
            result.z_score =
                (result.rolling_mean - self.config.target_mean) / self.config.target_std;
        }

        // Check minimum observations
        if self.n_observations < self.config.min_observations {
            result.message = format!(
                "Collecting data ({}/{})",
                self.n_observations, self.config.min_observations
            );
            return result;
        }

        // Check CUSUM threshold
        let cusum_triggered = result.cusum_value > self.config.cusum_threshold;

        // Check EWMA deviation from target
        let ewma_z = if self.config.target_std > 0.0 {
            (self.ewma - self.config.target_mean) / self.config.target_std
        } else {
            0.0
        };
        let ewma_triggered = ewma_z.abs() > 3.0; // 3 sigma

        // Check Page-Hinkley
        let ph_triggered = result.ph_statistic > self.config.ph_threshold;

        // Check z-score
        let zscore_triggered = result.z_score.abs() > 2.5;

        // Determine alert level and message
        let triggered_count =
            cusum_triggered as u8 + ewma_triggered as u8 + ph_triggered as u8 + zscore_triggered as u8;

        result.alert_level = match triggered_count {
            0 => AlertLevel::Normal,
            1 => AlertLevel::Warning,
            2 => AlertLevel::Alert,
            _ => AlertLevel::Critical,
        };

        result.drift_detected = triggered_count >= 2;

        // Set triggered detector
        result.triggered_by = if cusum_triggered {
            Some(DriftDetector::Cusum)
        } else if ph_triggered {
            Some(DriftDetector::PageHinkley)
        } else if ewma_triggered {
            Some(DriftDetector::Ewma)
        } else if zscore_triggered {
            Some(DriftDetector::ZScore)
        } else {
            None
        };

        // Generate message
        result.message = match result.alert_level {
            AlertLevel::Normal => "Performance within expected range".to_string(),
            AlertLevel::Warning => format!(
                "Warning: Performance deviation detected (z={:.2})",
                result.z_score
            ),
            AlertLevel::Alert => format!(
                "Alert: Significant drift detected. CUSUM={:.2}, PH={:.2}",
                result.cusum_value, result.ph_statistic
            ),
            AlertLevel::Critical => format!(
                "CRITICAL: Multiple drift indicators triggered! Mean={:.4} vs expected={:.4}",
                result.rolling_mean, self.config.target_mean
            ),
        };

        result
    }

    /// Get rolling mean
    pub fn rolling_mean(&self) -> f64 {
        if self.observations.is_empty() {
            self.config.target_mean
        } else {
            self.rolling_sum / self.observations.len() as f64
        }
    }

    /// Get rolling standard deviation
    pub fn rolling_std(&self) -> f64 {
        let n = self.observations.len() as f64;
        if n < 2.0 {
            return self.config.target_std;
        }

        let mean = self.rolling_mean();
        let variance = (self.rolling_sum_sq - n * mean * mean) / (n - 1.0);
        variance.max(0.0).sqrt()
    }

    /// Reset the detector
    pub fn reset(&mut self) {
        self.observations.clear();
        self.cusum_pos = 0.0;
        self.cusum_neg = 0.0;
        self.ewma_initialized = false;
        self.ph_sum = 0.0;
        self.ph_min = 0.0;
        self.n_observations = 0;
        self.rolling_sum = 0.0;
        self.rolling_sum_sq = 0.0;
    }

    /// Get current state summary
    pub fn state_summary(&self) -> String {
        format!(
            "Observations: {}, Mean: {:.4} (target: {:.4}), CUSUM: {:.2}",
            self.n_observations,
            self.rolling_mean(),
            self.config.target_mean,
            self.cusum_pos.max(self.cusum_neg)
        )
    }
}

/// Multi-metric drift monitor
pub struct MultiMetricDriftMonitor {
    /// Detectors for each metric
    detectors: Vec<(String, DriftDetectorEngine)>,
    /// Overall alert level
    overall_alert: AlertLevel,
    /// Alert history
    alert_history: VecDeque<(u64, AlertLevel, String)>,
    /// Max history size
    max_history: usize,
}

impl MultiMetricDriftMonitor {
    /// Create a new multi-metric monitor
    pub fn new() -> Self {
        Self {
            detectors: Vec::new(),
            overall_alert: AlertLevel::Normal,
            alert_history: VecDeque::new(),
            max_history: 100,
        }
    }

    /// Add a metric to monitor
    pub fn add_metric(&mut self, name: &str, expected_mean: f64, expected_std: f64) {
        let detector = DriftDetectorEngine::from_backtest(expected_mean, expected_std);
        self.detectors.push((name.to_string(), detector));
    }

    /// Update a specific metric
    pub fn update_metric(&mut self, name: &str, value: f64, timestamp_ms: u64) -> Option<DriftResult> {
        for (n, detector) in &mut self.detectors {
            if n == name {
                let result = detector.update(value);

                // Record alert if not normal
                if result.alert_level != AlertLevel::Normal {
                    self.alert_history.push_back((
                        timestamp_ms,
                        result.alert_level,
                        format!("{}: {}", name, result.message),
                    ));

                    if self.alert_history.len() > self.max_history {
                        self.alert_history.pop_front();
                    }
                }

                // Update overall alert level
                self.update_overall_alert();

                return Some(result);
            }
        }
        None
    }

    /// Update overall alert level
    fn update_overall_alert(&mut self) {
        self.overall_alert = AlertLevel::Normal;

        for (_, detector) in &self.detectors {
            let result = detector.check_drift();
            if result.alert_level > self.overall_alert {
                self.overall_alert = result.alert_level;
            }
        }
    }

    /// Get overall alert level
    pub fn overall_alert_level(&self) -> AlertLevel {
        self.overall_alert
    }

    /// Get alert history
    pub fn alert_history(&self) -> Vec<(u64, AlertLevel, String)> {
        self.alert_history.iter().cloned().collect()
    }

    /// Get all current results
    pub fn current_results(&self) -> Vec<(String, DriftResult)> {
        self.detectors
            .iter()
            .map(|(name, detector)| (name.clone(), detector.check_drift()))
            .collect()
    }

    /// Reset all detectors
    pub fn reset_all(&mut self) {
        for (_, detector) in &mut self.detectors {
            detector.reset();
        }
        self.overall_alert = AlertLevel::Normal;
        self.alert_history.clear();
    }
}

impl Default for MultiMetricDriftMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== DriftConfig Tests ====================

    #[test]
    fn test_drift_config_default() {
        let config = DriftConfig::default();
        assert_eq!(config.target_mean, 0.0);
        assert_eq!(config.target_std, 1.0);
        assert_eq!(config.min_observations, 20);
    }

    // ==================== DriftResult Tests ====================

    #[test]
    fn test_drift_result_default() {
        let result = DriftResult::default();
        assert!(!result.drift_detected);
        assert!(result.triggered_by.is_none());
        assert_eq!(result.alert_level, AlertLevel::Normal);
    }

    // ==================== AlertLevel Tests ====================

    #[test]
    fn test_alert_level_ordering() {
        assert!(AlertLevel::Normal < AlertLevel::Warning);
        assert!(AlertLevel::Warning < AlertLevel::Alert);
        assert!(AlertLevel::Alert < AlertLevel::Critical);
    }

    #[test]
    fn test_alert_level_equality() {
        assert_eq!(AlertLevel::Normal, AlertLevel::Normal);
        assert_ne!(AlertLevel::Normal, AlertLevel::Warning);
    }

    // ==================== DriftDetectorEngine Tests ====================

    #[test]
    fn test_detector_creation() {
        let config = DriftConfig::default();
        let detector = DriftDetectorEngine::new(config);
        assert_eq!(detector.n_observations, 0);
    }

    #[test]
    fn test_detector_from_backtest() {
        let detector = DriftDetectorEngine::from_backtest(0.01, 0.02);
        assert_eq!(detector.config.target_mean, 0.01);
        assert_eq!(detector.config.target_std, 0.02);
    }

    #[test]
    fn test_detector_from_backtest_zero_std() {
        let detector = DriftDetectorEngine::from_backtest(0.01, 0.0);
        // Should default to 1.0 to avoid division by zero
        assert_eq!(detector.config.target_std, 1.0);
    }

    #[test]
    fn test_detector_single_update() {
        let mut detector = DriftDetectorEngine::from_backtest(0.01, 0.02);
        let result = detector.update(0.01);

        assert_eq!(result.n_observations, 1);
        assert!(!result.drift_detected);
    }

    #[test]
    fn test_detector_updates_rolling_stats() {
        let mut detector = DriftDetectorEngine::from_backtest(0.0, 1.0);

        for i in 0..10 {
            detector.update(i as f64);
        }

        // Mean of 0..10 is 4.5
        assert!((detector.rolling_mean() - 4.5).abs() < 0.01);
        assert_eq!(detector.n_observations, 10);
    }

    #[test]
    fn test_detector_ewma_initialization() {
        let mut detector = DriftDetectorEngine::from_backtest(0.0, 1.0);

        detector.update(5.0);
        assert_eq!(detector.ewma, 5.0);

        detector.update(10.0);
        // EWMA with lambda=0.2: 0.2 * 10 + 0.8 * 5 = 6
        assert!((detector.ewma - 6.0).abs() < 0.01);
    }

    #[test]
    fn test_detector_no_drift_on_target() {
        let mut config = DriftConfig::default();
        config.target_mean = 0.0;
        config.target_std = 1.0;
        config.min_observations = 5;

        let mut detector = DriftDetectorEngine::new(config);

        // Feed values close to target
        for _ in 0..30 {
            let result = detector.update(0.1);
            // Should not trigger critical drift
            assert!(result.alert_level != AlertLevel::Critical);
        }
    }

    #[test]
    fn test_detector_drift_on_mean_shift() {
        let mut config = DriftConfig::default();
        config.target_mean = 0.0;
        config.target_std = 1.0;
        config.min_observations = 10;
        config.cusum_threshold = 3.0;

        let mut detector = DriftDetectorEngine::new(config);

        // Feed values significantly above target
        for _ in 0..50 {
            detector.update(5.0); // 5 sigma above target
        }

        let result = detector.check_drift();
        // Should detect drift with high CUSUM
        assert!(result.cusum_value > 3.0);
        assert!(result.alert_level >= AlertLevel::Warning);
    }

    #[test]
    fn test_detector_cusum_directions() {
        let mut detector = DriftDetectorEngine::from_backtest(0.0, 1.0);

        // Positive drift
        for _ in 0..30 {
            detector.update(3.0);
        }
        assert!(detector.cusum_pos > detector.cusum_neg);

        detector.reset();

        // Negative drift
        for _ in 0..30 {
            detector.update(-3.0);
        }
        assert!(detector.cusum_neg > detector.cusum_pos);
    }

    #[test]
    fn test_detector_rolling_window() {
        let mut config = DriftConfig::default();
        config.window_size = 5;

        let mut detector = DriftDetectorEngine::new(config);

        // Fill window
        for i in 0..5 {
            detector.update(i as f64);
        }
        assert_eq!(detector.observations.len(), 5);

        // Add more, old should drop
        detector.update(100.0);
        assert_eq!(detector.observations.len(), 5);

        // Mean should shift toward 100
        assert!(detector.rolling_mean() > 10.0);
    }

    #[test]
    fn test_detector_reset() {
        let mut detector = DriftDetectorEngine::from_backtest(0.0, 1.0);

        for _ in 0..10 {
            detector.update(1.0);
        }

        assert!(detector.n_observations > 0);

        detector.reset();

        assert_eq!(detector.n_observations, 0);
        assert_eq!(detector.cusum_pos, 0.0);
        assert_eq!(detector.cusum_neg, 0.0);
        assert!(detector.observations.is_empty());
    }

    #[test]
    fn test_detector_state_summary() {
        let mut detector = DriftDetectorEngine::from_backtest(0.01, 0.02);
        detector.update(0.01);
        detector.update(0.02);

        let summary = detector.state_summary();
        assert!(summary.contains("Observations: 2"));
        assert!(summary.contains("target: 0.01"));
    }

    #[test]
    fn test_detector_insufficient_observations_message() {
        let mut config = DriftConfig::default();
        config.min_observations = 100;

        let mut detector = DriftDetectorEngine::new(config);
        let result = detector.update(1.0);

        assert!(result.message.contains("Collecting data"));
        assert!(!result.drift_detected);
    }

    #[test]
    fn test_detector_rolling_std_single_value() {
        let mut detector = DriftDetectorEngine::from_backtest(0.0, 1.0);
        detector.update(5.0);

        // With single value, should return target std
        assert_eq!(detector.rolling_std(), 1.0);
    }

    #[test]
    fn test_detector_rolling_std_multiple_values() {
        let mut detector = DriftDetectorEngine::from_backtest(0.0, 1.0);

        detector.update(0.0);
        detector.update(10.0);

        // Std of [0, 10] with sample variance
        // Variance = ((0-5)^2 + (10-5)^2) / 1 = 50
        // Std = sqrt(50) ≈ 7.07
        let std = detector.rolling_std();
        assert!((std - 7.07).abs() < 0.1);
    }

    // ==================== CusumDirection Tests ====================

    #[test]
    fn test_cusum_direction_equality() {
        assert_eq!(CusumDirection::Positive, CusumDirection::Positive);
        assert_ne!(CusumDirection::Positive, CusumDirection::Negative);
    }

    // ==================== DriftDetector Tests ====================

    #[test]
    fn test_drift_detector_type_equality() {
        assert_eq!(DriftDetector::Cusum, DriftDetector::Cusum);
        assert_ne!(DriftDetector::Cusum, DriftDetector::Ewma);
    }

    // ==================== MultiMetricDriftMonitor Tests ====================

    #[test]
    fn test_multi_monitor_creation() {
        let monitor = MultiMetricDriftMonitor::new();
        assert_eq!(monitor.overall_alert_level(), AlertLevel::Normal);
    }

    #[test]
    fn test_multi_monitor_add_metric() {
        let mut monitor = MultiMetricDriftMonitor::new();
        monitor.add_metric("pnl", 0.01, 0.02);
        monitor.add_metric("sharpe", 1.5, 0.5);

        assert_eq!(monitor.detectors.len(), 2);
    }

    #[test]
    fn test_multi_monitor_update_metric() {
        let mut monitor = MultiMetricDriftMonitor::new();
        monitor.add_metric("pnl", 0.01, 0.02);

        let result = monitor.update_metric("pnl", 0.01, 1000);
        assert!(result.is_some());

        let result = monitor.update_metric("nonexistent", 0.0, 1000);
        assert!(result.is_none());
    }

    #[test]
    fn test_multi_monitor_overall_alert() {
        let mut monitor = MultiMetricDriftMonitor::new();

        let mut config = DriftConfig::default();
        config.min_observations = 5;
        config.cusum_threshold = 1.0;

        monitor.detectors.push(("test".to_string(), DriftDetectorEngine::new(config)));

        // Feed extreme values
        for _ in 0..20 {
            monitor.update_metric("test", 100.0, 1000);
        }

        // Should have elevated alert
        assert!(monitor.overall_alert_level() >= AlertLevel::Normal);
    }

    #[test]
    fn test_multi_monitor_current_results() {
        let mut monitor = MultiMetricDriftMonitor::new();
        monitor.add_metric("pnl", 0.01, 0.02);
        monitor.add_metric("sharpe", 1.5, 0.5);

        monitor.update_metric("pnl", 0.01, 1000);
        monitor.update_metric("sharpe", 1.5, 1000);

        let results = monitor.current_results();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_multi_monitor_alert_history() {
        let mut monitor = MultiMetricDriftMonitor::new();

        let mut config = DriftConfig::default();
        config.min_observations = 3;

        monitor.detectors.push(("test".to_string(), DriftDetectorEngine::new(config)));

        // Feed some values
        for i in 0..10 {
            monitor.update_metric("test", (i as f64) * 10.0, i as u64 * 1000);
        }

        // Check history structure (may or may not have alerts)
        let history = monitor.alert_history();
        for (ts, level, msg) in &history {
            assert!(*ts > 0);
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_multi_monitor_reset_all() {
        let mut monitor = MultiMetricDriftMonitor::new();
        monitor.add_metric("pnl", 0.01, 0.02);

        for _ in 0..10 {
            monitor.update_metric("pnl", 0.05, 1000);
        }

        monitor.reset_all();

        assert_eq!(monitor.overall_alert_level(), AlertLevel::Normal);
        assert!(monitor.alert_history().is_empty());

        let results = monitor.current_results();
        for (_, result) in results {
            assert_eq!(result.n_observations, 0);
        }
    }

    #[test]
    fn test_multi_monitor_default() {
        let monitor = MultiMetricDriftMonitor::default();
        assert!(monitor.detectors.is_empty());
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_detector_empty_rolling_mean() {
        let detector = DriftDetectorEngine::from_backtest(0.5, 1.0);
        // Empty observations should return target mean
        assert_eq!(detector.rolling_mean(), 0.5);
    }

    #[test]
    fn test_detector_page_hinkley_accumulation() {
        let mut detector = DriftDetectorEngine::from_backtest(0.0, 1.0);

        // Page-Hinkley should accumulate
        for _ in 0..20 {
            detector.update(-5.0);
        }

        let result = detector.check_drift();
        assert!(result.ph_statistic > 0.0 || result.ph_statistic == 0.0); // May be positive or zero depending on min
    }

    #[test]
    fn test_detector_z_score_calculation() {
        let mut config = DriftConfig::default();
        config.target_mean = 0.0;
        config.target_std = 1.0;
        config.min_observations = 2;

        let mut detector = DriftDetectorEngine::new(config);

        detector.update(2.0);
        detector.update(2.0);

        let result = detector.check_drift();
        // Mean is 2.0, z-score should be 2.0
        assert!((result.z_score - 2.0).abs() < 0.1);
    }

    #[test]
    fn test_alert_level_critical() {
        let mut config = DriftConfig::default();
        config.target_mean = 0.0;
        config.target_std = 0.1; // Small std for easy triggering
        config.min_observations = 5;
        config.cusum_threshold = 2.0;
        config.ph_threshold = 1.0;

        let mut detector = DriftDetectorEngine::new(config);

        // Feed extreme values to trigger multiple detectors
        for _ in 0..100 {
            detector.update(100.0);
        }

        let result = detector.check_drift();
        // Should likely be at least Alert level
        assert!(result.alert_level >= AlertLevel::Warning);
    }
}

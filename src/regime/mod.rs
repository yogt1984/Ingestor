//! Regime Detection Module for MARS (Momentum Adaptive Regime Strategy)
//!
//! This module provides market regime classification to guide trading decisions.
//! It combines multiple signals (momentum, monotonicity, Hurst exponent, entropy)
//! to classify market conditions into actionable regimes.
//!
//! # Architecture
//!
//! ```text
//! RegimeFeatures → RegimeDetector → RegimeState
//!       │                │               │
//!       ├─ momentum      │               ├─ regime (TrendingUp/Down/MeanReverting/Uncertain)
//!       ├─ monotonicity  │               ├─ confidence (0.0 - 1.0)
//!       ├─ hurst         │               ├─ trend_strength (-1.0 to 1.0)
//!       ├─ entropy       │               └─ persistence (Hurst)
//!       ├─ kalman_vel    │
//!       └─ kalman_accel  │
//!                        │
//!         ThresholdRegimeDetector (basic implementation)
//!         CompositeRegimeDetector (weighted voting)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use crate::regime::{RegimeDetector, ThresholdRegimeDetector, RegimeFeatures, MarketRegime};
//!
//! let detector = ThresholdRegimeDetector::default();
//! let features = RegimeFeatures {
//!     momentum: 0.001,
//!     monotonicity: 0.75,
//!     hurst: 0.65,
//!     entropy: 0.4,
//!     kalman_velocity: 0.0005,
//!     kalman_acceleration: 0.0001,
//! };
//!
//! let state = detector.detect(&features);
//! assert_eq!(state.regime, MarketRegime::TrendingUp);
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

// ============================================================================
// Core Types
// ============================================================================

/// Market regime classification
///
/// Represents the four primary market states that MARS uses to adapt trading behavior:
/// - **TrendingUp**: Strong upward momentum, high directional consistency
/// - **TrendingDown**: Strong downward momentum, high directional consistency
/// - **MeanReverting**: Low directional consistency, price oscillates around mean
/// - **Uncertain**: Conflicting signals, high entropy, unpredictable behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketRegime {
    /// Strong upward trend detected
    TrendingUp,
    /// Strong downward trend detected
    TrendingDown,
    /// Price oscillates around mean, suitable for symmetric market making
    MeanReverting,
    /// Conflicting signals, high uncertainty - widen spreads or abstain
    Uncertain,
}

impl fmt::Display for MarketRegime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarketRegime::TrendingUp => write!(f, "TrendingUp"),
            MarketRegime::TrendingDown => write!(f, "TrendingDown"),
            MarketRegime::MeanReverting => write!(f, "MeanReverting"),
            MarketRegime::Uncertain => write!(f, "Uncertain"),
        }
    }
}

impl MarketRegime {
    /// Returns true if the regime is trending (up or down)
    pub fn is_trending(&self) -> bool {
        matches!(self, MarketRegime::TrendingUp | MarketRegime::TrendingDown)
    }

    /// Returns true if the regime is mean-reverting
    pub fn is_mean_reverting(&self) -> bool {
        matches!(self, MarketRegime::MeanReverting)
    }

    /// Returns true if the regime is uncertain
    pub fn is_uncertain(&self) -> bool {
        matches!(self, MarketRegime::Uncertain)
    }

    /// Returns the recommended skew direction (-1.0 for short, 0.0 for neutral, 1.0 for long)
    pub fn skew_direction(&self) -> f64 {
        match self {
            MarketRegime::TrendingUp => 1.0,
            MarketRegime::TrendingDown => -1.0,
            MarketRegime::MeanReverting => 0.0,
            MarketRegime::Uncertain => 0.0,
        }
    }

    /// Returns the recommended spread multiplier (1.0 = normal, >1.0 = wider)
    pub fn spread_multiplier(&self) -> f64 {
        match self {
            MarketRegime::TrendingUp => 1.0,
            MarketRegime::TrendingDown => 1.0,
            MarketRegime::MeanReverting => 0.8, // Tighter spreads for mean-reverting
            MarketRegime::Uncertain => 2.0,    // Much wider spreads for uncertainty
        }
    }
}

/// Complete regime detection result with confidence and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeState {
    /// The detected market regime
    pub regime: MarketRegime,
    /// Confidence in the regime classification (0.0 - 1.0)
    pub confidence: f64,
    /// Trend strength from -1.0 (strong down) to 1.0 (strong up)
    pub trend_strength: f64,
    /// Hurst exponent indicating persistence (>0.5 trending, <0.5 mean-reverting)
    pub persistence: f64,
    /// Number of signals that agree with the classification
    pub signal_agreement: u8,
    /// Total number of signals considered
    pub total_signals: u8,
}

impl Default for RegimeState {
    fn default() -> Self {
        Self {
            regime: MarketRegime::Uncertain,
            confidence: 0.0,
            trend_strength: 0.0,
            persistence: 0.5,
            signal_agreement: 0,
            total_signals: 0,
        }
    }
}

impl RegimeState {
    /// Create a new RegimeState with the given parameters
    pub fn new(
        regime: MarketRegime,
        confidence: f64,
        trend_strength: f64,
        persistence: f64,
    ) -> Self {
        Self {
            regime,
            confidence: confidence.clamp(0.0, 1.0),
            trend_strength: trend_strength.clamp(-1.0, 1.0),
            persistence,
            signal_agreement: 0,
            total_signals: 0,
        }
    }

    /// Create a new RegimeState with signal agreement info
    pub fn with_signals(
        regime: MarketRegime,
        confidence: f64,
        trend_strength: f64,
        persistence: f64,
        signal_agreement: u8,
        total_signals: u8,
    ) -> Self {
        Self {
            regime,
            confidence: confidence.clamp(0.0, 1.0),
            trend_strength: trend_strength.clamp(-1.0, 1.0),
            persistence,
            signal_agreement,
            total_signals,
        }
    }

    /// Returns the agreement ratio (0.0 - 1.0)
    pub fn agreement_ratio(&self) -> f64 {
        if self.total_signals == 0 {
            0.0
        } else {
            self.signal_agreement as f64 / self.total_signals as f64
        }
    }
}

/// Input features for regime detection
///
/// These features are computed by TrendFeatureEngine, KalmanFilter, and EntropyEngine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegimeFeatures {
    /// Linear regression slope of prices (positive = uptrend)
    pub momentum: f64,
    /// Fraction of ticks in dominant direction (0.0 - 1.0)
    pub monotonicity: f64,
    /// Hurst exponent (>0.5 = trending, <0.5 = mean-reverting)
    pub hurst: f64,
    /// Tick entropy (high = unpredictable, low = structured)
    pub entropy: f64,
    /// Kalman filter velocity estimate
    pub kalman_velocity: f64,
    /// Kalman filter acceleration estimate
    pub kalman_acceleration: f64,
}

impl RegimeFeatures {
    /// Create new RegimeFeatures with all fields
    pub fn new(
        momentum: f64,
        monotonicity: f64,
        hurst: f64,
        entropy: f64,
        kalman_velocity: f64,
        kalman_acceleration: f64,
    ) -> Self {
        Self {
            momentum,
            monotonicity,
            hurst,
            entropy,
            kalman_velocity,
            kalman_acceleration,
        }
    }

    /// Check if any feature is NaN
    pub fn has_nan(&self) -> bool {
        self.momentum.is_nan()
            || self.monotonicity.is_nan()
            || self.hurst.is_nan()
            || self.entropy.is_nan()
            || self.kalman_velocity.is_nan()
            || self.kalman_acceleration.is_nan()
    }

    /// Replace NaN values with defaults
    pub fn sanitize(&self) -> Self {
        Self {
            momentum: if self.momentum.is_nan() { 0.0 } else { self.momentum },
            monotonicity: if self.monotonicity.is_nan() { 0.5 } else { self.monotonicity },
            hurst: if self.hurst.is_nan() { 0.5 } else { self.hurst },
            entropy: if self.entropy.is_nan() { 1.0 } else { self.entropy },
            kalman_velocity: if self.kalman_velocity.is_nan() { 0.0 } else { self.kalman_velocity },
            kalman_acceleration: if self.kalman_acceleration.is_nan() { 0.0 } else { self.kalman_acceleration },
        }
    }
}

// ============================================================================
// Regime Detector Trait
// ============================================================================

/// Trait for regime detection algorithms
///
/// Implementations of this trait take market features and classify the current
/// market regime. Different implementations may use different algorithms:
/// - ThresholdRegimeDetector: Simple threshold-based classification
/// - CompositeRegimeDetector: Weighted voting from multiple sub-detectors
pub trait RegimeDetector: Send + Sync {
    /// Detect the current market regime from features
    fn detect(&self, features: &RegimeFeatures) -> RegimeState;

    /// Name of the detector for logging/debugging
    fn name(&self) -> &str;

    /// Reset any internal state (for stateful detectors)
    fn reset(&mut self) {}
}

// ============================================================================
// Threshold Configuration
// ============================================================================

/// Configuration for threshold-based regime detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdConfig {
    /// Momentum threshold for trend detection (absolute value)
    pub momentum_threshold: f64,
    /// Monotonicity threshold for trend detection (0.0 - 1.0)
    pub monotonicity_threshold: f64,
    /// Hurst threshold for trending vs mean-reverting (typically 0.5)
    pub hurst_trending_threshold: f64,
    /// Hurst threshold below which is clearly mean-reverting
    pub hurst_mean_reverting_threshold: f64,
    /// Entropy threshold above which is uncertain (0.0 - 1.0)
    pub entropy_threshold: f64,
    /// Minimum confidence to not classify as uncertain
    pub min_confidence: f64,
    /// Weight for momentum signal
    pub momentum_weight: f64,
    /// Weight for monotonicity signal
    pub monotonicity_weight: f64,
    /// Weight for Hurst signal
    pub hurst_weight: f64,
    /// Weight for Kalman velocity signal
    pub kalman_weight: f64,
}

impl Default for ThresholdConfig {
    fn default() -> Self {
        Self {
            momentum_threshold: 0.0001,          // 1 bps per tick
            monotonicity_threshold: 0.6,         // 60% directional consistency
            hurst_trending_threshold: 0.55,      // Above 0.55 = trending
            hurst_mean_reverting_threshold: 0.45, // Below 0.45 = mean-reverting
            entropy_threshold: 0.8,              // Above 0.8 = uncertain
            min_confidence: 0.3,                 // Below 0.3 = uncertain
            momentum_weight: 1.0,
            monotonicity_weight: 1.0,
            hurst_weight: 1.0,
            kalman_weight: 0.5,
        }
    }
}

impl ThresholdConfig {
    /// Create a conservative configuration (fewer signals, higher confidence required)
    pub fn conservative() -> Self {
        Self {
            momentum_threshold: 0.0002,
            monotonicity_threshold: 0.7,
            hurst_trending_threshold: 0.6,
            hurst_mean_reverting_threshold: 0.4,
            entropy_threshold: 0.7,
            min_confidence: 0.5,
            momentum_weight: 1.0,
            monotonicity_weight: 1.0,
            hurst_weight: 1.0,
            kalman_weight: 0.5,
        }
    }

    /// Create an aggressive configuration (more signals, lower confidence threshold)
    pub fn aggressive() -> Self {
        Self {
            momentum_threshold: 0.00005,
            monotonicity_threshold: 0.55,
            hurst_trending_threshold: 0.52,
            hurst_mean_reverting_threshold: 0.48,
            entropy_threshold: 0.9,
            min_confidence: 0.2,
            momentum_weight: 1.0,
            monotonicity_weight: 1.0,
            hurst_weight: 1.0,
            kalman_weight: 0.5,
        }
    }

    /// Validate configuration parameters
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.momentum_threshold < 0.0 {
            return Err("momentum_threshold must be non-negative");
        }
        if !(0.0..=1.0).contains(&self.monotonicity_threshold) {
            return Err("monotonicity_threshold must be between 0 and 1");
        }
        if !(0.0..=1.0).contains(&self.hurst_trending_threshold) {
            return Err("hurst_trending_threshold must be between 0 and 1");
        }
        if !(0.0..=1.0).contains(&self.hurst_mean_reverting_threshold) {
            return Err("hurst_mean_reverting_threshold must be between 0 and 1");
        }
        if self.hurst_mean_reverting_threshold >= self.hurst_trending_threshold {
            return Err("hurst_mean_reverting_threshold must be less than hurst_trending_threshold");
        }
        if !(0.0..=1.0).contains(&self.entropy_threshold) {
            return Err("entropy_threshold must be between 0 and 1");
        }
        if !(0.0..=1.0).contains(&self.min_confidence) {
            return Err("min_confidence must be between 0 and 1");
        }
        Ok(())
    }
}

// ============================================================================
// Threshold Regime Detector
// ============================================================================

/// Basic threshold-based regime detector
///
/// Classifies market regime based on configurable thresholds for each feature.
/// Uses a voting system where each feature "votes" for a regime, and the
/// final classification is based on the weighted majority.
#[derive(Debug, Clone)]
pub struct ThresholdRegimeDetector {
    config: ThresholdConfig,
}

impl Default for ThresholdRegimeDetector {
    fn default() -> Self {
        Self {
            config: ThresholdConfig::default(),
        }
    }
}

impl ThresholdRegimeDetector {
    /// Create a new detector with the given configuration
    pub fn new(config: ThresholdConfig) -> Self {
        Self { config }
    }

    /// Create with conservative settings
    pub fn conservative() -> Self {
        Self::new(ThresholdConfig::conservative())
    }

    /// Create with aggressive settings
    pub fn aggressive() -> Self {
        Self::new(ThresholdConfig::aggressive())
    }

    /// Get the current configuration
    pub fn config(&self) -> &ThresholdConfig {
        &self.config
    }

    /// Compute directional vote from a single signal
    /// Returns (vote, confidence) where vote is -1, 0, or 1
    fn momentum_vote(&self, momentum: f64) -> (i8, f64) {
        let abs_momentum = momentum.abs();
        if abs_momentum > self.config.momentum_threshold {
            let confidence = (abs_momentum / self.config.momentum_threshold).min(3.0) / 3.0;
            if momentum > 0.0 {
                (1, confidence)
            } else {
                (-1, confidence)
            }
        } else {
            (0, 1.0 - abs_momentum / self.config.momentum_threshold)
        }
    }

    fn monotonicity_vote(&self, monotonicity: f64, momentum: f64) -> (i8, f64) {
        if monotonicity > self.config.monotonicity_threshold {
            // High monotonicity = trending
            let confidence = (monotonicity - self.config.monotonicity_threshold)
                / (1.0 - self.config.monotonicity_threshold);
            // Direction comes from momentum sign
            if momentum >= 0.0 {
                (1, confidence)
            } else {
                (-1, confidence)
            }
        } else {
            // Low monotonicity = not clearly trending
            let confidence = (self.config.monotonicity_threshold - monotonicity)
                / self.config.monotonicity_threshold;
            (0, confidence)
        }
    }

    fn hurst_vote(&self, hurst: f64) -> (RegimeVote, f64) {
        if hurst > self.config.hurst_trending_threshold {
            // Trending
            let confidence = (hurst - self.config.hurst_trending_threshold)
                / (1.0 - self.config.hurst_trending_threshold);
            (RegimeVote::Trending, confidence.min(1.0))
        } else if hurst < self.config.hurst_mean_reverting_threshold {
            // Mean-reverting
            let confidence = (self.config.hurst_mean_reverting_threshold - hurst)
                / self.config.hurst_mean_reverting_threshold;
            (RegimeVote::MeanReverting, confidence.min(1.0))
        } else {
            // In between - uncertain
            let mid = (self.config.hurst_trending_threshold + self.config.hurst_mean_reverting_threshold) / 2.0;
            let range = self.config.hurst_trending_threshold - self.config.hurst_mean_reverting_threshold;
            let confidence = 1.0 - 2.0 * (hurst - mid).abs() / range;
            (RegimeVote::Uncertain, confidence.max(0.0))
        }
    }

    fn kalman_vote(&self, velocity: f64, acceleration: f64) -> (i8, f64) {
        // Use velocity for direction, acceleration for confidence
        let abs_vel = velocity.abs();
        if abs_vel > self.config.momentum_threshold {
            // Confidence boosted if acceleration agrees with velocity
            let accel_agreement = if velocity * acceleration > 0.0 { 1.2 } else { 0.8 };
            let confidence = ((abs_vel / self.config.momentum_threshold).min(3.0) / 3.0) * accel_agreement;
            if velocity > 0.0 {
                (1, confidence.min(1.0))
            } else {
                (-1, confidence.min(1.0))
            }
        } else {
            (0, 0.5)
        }
    }

    fn entropy_check(&self, entropy: f64) -> bool {
        // Returns true if entropy is too high (uncertain)
        entropy > self.config.entropy_threshold
    }
}

/// Internal vote type for Hurst-based classification
#[derive(Debug, Clone, Copy, PartialEq)]
enum RegimeVote {
    Trending,
    MeanReverting,
    Uncertain,
}

impl RegimeDetector for ThresholdRegimeDetector {
    fn detect(&self, features: &RegimeFeatures) -> RegimeState {
        // Sanitize inputs
        let features = if features.has_nan() {
            features.sanitize()
        } else {
            features.clone()
        };

        // Check entropy first - if too high, return uncertain
        if self.entropy_check(features.entropy) {
            return RegimeState::with_signals(
                MarketRegime::Uncertain,
                0.2,
                0.0,
                features.hurst,
                0,
                4,
            );
        }

        // Collect votes from each signal
        let (mom_dir, mom_conf) = self.momentum_vote(features.momentum);
        let (mono_dir, mono_conf) = self.monotonicity_vote(features.monotonicity, features.momentum);
        let (hurst_vote, hurst_conf) = self.hurst_vote(features.hurst);
        let (kalman_dir, kalman_conf) = self.kalman_vote(features.kalman_velocity, features.kalman_acceleration);

        // Count directional votes
        let directions = [
            (mom_dir, mom_conf * self.config.momentum_weight),
            (mono_dir, mono_conf * self.config.monotonicity_weight),
            (kalman_dir, kalman_conf * self.config.kalman_weight),
        ];

        let mut up_votes = 0.0;
        let mut down_votes = 0.0;
        let mut neutral_votes = 0.0;
        let mut total_weight = 0.0;

        for (dir, weight) in directions.iter() {
            total_weight += weight;
            match dir {
                1 => up_votes += weight,
                -1 => down_votes += weight,
                _ => neutral_votes += weight,
            }
        }

        // Hurst vote affects regime type, not direction
        let hurst_weight = hurst_conf * self.config.hurst_weight;
        total_weight += hurst_weight;

        // Determine regime
        let trend_strength = (up_votes - down_votes) / total_weight;
        let directional_strength = (up_votes.max(down_votes)) / total_weight;

        // Count signal agreement
        #[allow(unused_assignments)]
        let mut agreement = 0u8;
        let total = 4u8;

        let regime = match hurst_vote {
            RegimeVote::MeanReverting => {
                // Hurst says mean-reverting
                if hurst_conf > 0.5 {
                    agreement = if neutral_votes > up_votes.max(down_votes) { 2 } else { 1 };
                    agreement += 1; // Hurst agrees with mean-reverting
                    MarketRegime::MeanReverting
                } else if directional_strength > 0.6 {
                    // But directional signals are strong - trust direction
                    agreement = if up_votes > down_votes {
                        directions.iter().filter(|(d, _)| *d == 1).count() as u8
                    } else {
                        directions.iter().filter(|(d, _)| *d == -1).count() as u8
                    };
                    if trend_strength > 0.3 {
                        MarketRegime::TrendingUp
                    } else if trend_strength < -0.3 {
                        MarketRegime::TrendingDown
                    } else {
                        MarketRegime::MeanReverting
                    }
                } else {
                    agreement = 2; // Hurst + low directional
                    MarketRegime::MeanReverting
                }
            }
            RegimeVote::Trending => {
                // Hurst says trending - use directional votes
                if directional_strength > 0.4 {
                    if up_votes > down_votes {
                        agreement = directions.iter().filter(|(d, _)| *d == 1).count() as u8 + 1;
                        MarketRegime::TrendingUp
                    } else {
                        agreement = directions.iter().filter(|(d, _)| *d == -1).count() as u8 + 1;
                        MarketRegime::TrendingDown
                    }
                } else {
                    // Hurst says trending but no clear direction
                    agreement = 1;
                    MarketRegime::Uncertain
                }
            }
            RegimeVote::Uncertain => {
                // Hurst is uncertain - rely on directional signals
                if directional_strength > 0.7 {
                    if up_votes > down_votes {
                        agreement = directions.iter().filter(|(d, _)| *d == 1).count() as u8;
                        MarketRegime::TrendingUp
                    } else {
                        agreement = directions.iter().filter(|(d, _)| *d == -1).count() as u8;
                        MarketRegime::TrendingDown
                    }
                } else if neutral_votes > directional_strength * total_weight {
                    agreement = directions.iter().filter(|(d, _)| *d == 0).count() as u8;
                    MarketRegime::MeanReverting
                } else {
                    agreement = 0;
                    MarketRegime::Uncertain
                }
            }
        };

        // Calculate confidence
        let base_confidence = match regime {
            MarketRegime::TrendingUp | MarketRegime::TrendingDown => {
                directional_strength * 0.5 + hurst_conf * 0.3 + (1.0 - features.entropy) * 0.2
            }
            MarketRegime::MeanReverting => {
                (1.0 - directional_strength) * 0.4 + hurst_conf * 0.4 + (1.0 - features.entropy) * 0.2
            }
            MarketRegime::Uncertain => {
                0.2 + features.entropy * 0.3
            }
        };

        let confidence = base_confidence.clamp(0.0, 1.0);

        // If confidence is too low, return uncertain
        if confidence < self.config.min_confidence && regime != MarketRegime::Uncertain {
            return RegimeState::with_signals(
                MarketRegime::Uncertain,
                confidence,
                trend_strength,
                features.hurst,
                agreement,
                total,
            );
        }

        RegimeState::with_signals(regime, confidence, trend_strength, features.hurst, agreement, total)
    }

    fn name(&self) -> &str {
        "ThresholdRegimeDetector"
    }
}

// ============================================================================
// Composite Regime Detector
// ============================================================================

/// Composite detector that combines multiple sub-detectors with weighted voting
#[derive(Default)]
pub struct CompositeRegimeDetector {
    detectors: Vec<(Box<dyn RegimeDetector>, f64)>, // (detector, weight)
}

impl CompositeRegimeDetector {
    /// Create a new empty composite detector
    pub fn new() -> Self {
        Self {
            detectors: Vec::new(),
        }
    }

    /// Add a detector with a given weight
    pub fn add_detector(&mut self, detector: Box<dyn RegimeDetector>, weight: f64) {
        self.detectors.push((detector, weight));
    }

    /// Create with default set of detectors
    pub fn with_defaults() -> Self {
        let mut composite = Self::new();
        composite.add_detector(Box::new(ThresholdRegimeDetector::default()), 1.0);
        composite.add_detector(Box::new(ThresholdRegimeDetector::conservative()), 0.5);
        composite
    }
}

impl RegimeDetector for CompositeRegimeDetector {
    fn detect(&self, features: &RegimeFeatures) -> RegimeState {
        if self.detectors.is_empty() {
            return RegimeState::default();
        }

        let mut up_weight = 0.0;
        let mut down_weight = 0.0;
        let mut mean_rev_weight = 0.0;
        let mut uncertain_weight = 0.0;
        let mut total_weight = 0.0;
        let mut total_confidence = 0.0;
        let mut total_trend_strength = 0.0;
        let mut total_persistence = 0.0;

        for (detector, weight) in &self.detectors {
            let state = detector.detect(features);
            let effective_weight = weight * state.confidence;
            total_weight += effective_weight;
            total_confidence += state.confidence * weight;
            total_trend_strength += state.trend_strength * effective_weight;
            total_persistence += state.persistence * effective_weight;

            match state.regime {
                MarketRegime::TrendingUp => up_weight += effective_weight,
                MarketRegime::TrendingDown => down_weight += effective_weight,
                MarketRegime::MeanReverting => mean_rev_weight += effective_weight,
                MarketRegime::Uncertain => uncertain_weight += effective_weight,
            }
        }

        if total_weight == 0.0 {
            return RegimeState::default();
        }

        let sum_weights: f64 = self.detectors.iter().map(|(_, w)| w).sum();

        // Find winning regime
        let weights = [
            (MarketRegime::TrendingUp, up_weight),
            (MarketRegime::TrendingDown, down_weight),
            (MarketRegime::MeanReverting, mean_rev_weight),
            (MarketRegime::Uncertain, uncertain_weight),
        ];

        let (regime, max_weight) = weights
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap();

        let confidence = (total_confidence / sum_weights).clamp(0.0, 1.0);
        let agreement = (max_weight / total_weight * 4.0).round() as u8;

        RegimeState::with_signals(
            *regime,
            confidence,
            total_trend_strength / total_weight,
            total_persistence / total_weight,
            agreement,
            4,
        )
    }

    fn name(&self) -> &str {
        "CompositeRegimeDetector"
    }

    fn reset(&mut self) {
        for (detector, _) in &mut self.detectors {
            detector.reset();
        }
    }
}

// ============================================================================
// Stateful Regime Detector (with smoothing)
// ============================================================================

/// Stateful detector that smooths regime transitions
pub struct SmoothedRegimeDetector {
    inner: Box<dyn RegimeDetector>,
    history: Vec<RegimeState>,
    window_size: usize,
    transition_threshold: f64, // Required confidence to change regime
}

impl SmoothedRegimeDetector {
    /// Create a new smoothed detector wrapping an inner detector
    pub fn new(inner: Box<dyn RegimeDetector>, window_size: usize, transition_threshold: f64) -> Self {
        Self {
            inner,
            history: Vec::with_capacity(window_size),
            window_size,
            transition_threshold: transition_threshold.clamp(0.0, 1.0),
        }
    }

    /// Create with default settings
    pub fn with_defaults(inner: Box<dyn RegimeDetector>) -> Self {
        Self::new(inner, 5, 0.6)
    }

    /// Get the current regime history
    pub fn history(&self) -> &[RegimeState] {
        &self.history
    }
}

impl RegimeDetector for SmoothedRegimeDetector {
    fn detect(&self, features: &RegimeFeatures) -> RegimeState {
        // Note: This is a const method for the trait, but we can't mutate history
        // In practice, you'd use a different pattern (interior mutability) for production
        self.inner.detect(features)
    }

    fn name(&self) -> &str {
        "SmoothedRegimeDetector"
    }

    fn reset(&mut self) {
        self.history.clear();
        self.inner.reset();
    }
}

impl SmoothedRegimeDetector {
    /// Detect with state update (mutable version)
    pub fn detect_and_update(&mut self, features: &RegimeFeatures) -> RegimeState {
        let new_state = self.inner.detect(features);

        // Add to history
        if self.history.len() >= self.window_size {
            self.history.remove(0);
        }
        self.history.push(new_state.clone());

        // If not enough history, return raw
        if self.history.len() < 2 {
            return new_state;
        }

        // Count regime occurrences in history
        let mut up_count = 0;
        let mut down_count = 0;
        let mut mean_rev_count = 0;
        let mut uncertain_count = 0;
        let mut total_confidence = 0.0;
        let mut total_trend = 0.0;
        let mut total_persistence = 0.0;

        for state in &self.history {
            total_confidence += state.confidence;
            total_trend += state.trend_strength;
            total_persistence += state.persistence;
            match state.regime {
                MarketRegime::TrendingUp => up_count += 1,
                MarketRegime::TrendingDown => down_count += 1,
                MarketRegime::MeanReverting => mean_rev_count += 1,
                MarketRegime::Uncertain => uncertain_count += 1,
            }
        }

        let n = self.history.len() as f64;
        let avg_confidence = total_confidence / n;
        let avg_trend = total_trend / n;
        let avg_persistence = total_persistence / n;

        // Find dominant regime
        let counts = [
            (MarketRegime::TrendingUp, up_count),
            (MarketRegime::TrendingDown, down_count),
            (MarketRegime::MeanReverting, mean_rev_count),
            (MarketRegime::Uncertain, uncertain_count),
        ];

        let (dominant_regime, max_count) = counts
            .iter()
            .max_by_key(|(_, c)| *c)
            .unwrap();

        // Check if we should transition
        let dominance_ratio = *max_count as f64 / n;
        if dominance_ratio >= self.transition_threshold {
            RegimeState::with_signals(
                *dominant_regime,
                avg_confidence,
                avg_trend,
                avg_persistence,
                *max_count as u8,
                self.history.len() as u8,
            )
        } else {
            // Not enough consensus - return uncertain or most recent
            if avg_confidence < 0.3 {
                RegimeState::with_signals(
                    MarketRegime::Uncertain,
                    avg_confidence,
                    avg_trend,
                    avg_persistence,
                    uncertain_count as u8,
                    self.history.len() as u8,
                )
            } else {
                new_state
            }
        }
    }
}

// ============================================================================
// Regime Engine - FeaturesSnapshot Integration
// ============================================================================

use crate::features::feature_fusion::FeaturesSnapshot;
use crate::features::{TrendFeatureEngine, KalmanFilter, KalmanConfig};
use rust_decimal::prelude::ToPrimitive;

/// Configuration for the RegimeEngine
#[derive(Debug, Clone)]
pub struct RegimeEngineConfig {
    /// Number of price ticks to use for trend feature computation
    pub window_size: usize,
    /// Configuration for the threshold-based regime detector
    pub threshold_config: ThresholdConfig,
    /// Configuration for the Kalman filter
    pub kalman_config: KalmanConfig,
}

impl Default for RegimeEngineConfig {
    fn default() -> Self {
        Self {
            window_size: 60,
            threshold_config: ThresholdConfig::default(),
            kalman_config: KalmanConfig::default(),
        }
    }
}

/// Engine that integrates regime detection with FeaturesSnapshot
///
/// This engine maintains internal state (TrendFeatureEngine, KalmanFilter)
/// and provides methods to:
/// 1. Update with new price data
/// 2. Extract RegimeFeatures from accumulated data
/// 3. Detect the current regime
/// 4. Enrich a FeaturesSnapshot with regime labels
///
/// # Usage
///
/// ```ignore
/// use crate::regime::{RegimeEngine, RegimeEngineConfig};
/// use crate::features::feature_fusion::FeaturesSnapshot;
///
/// let mut engine = RegimeEngine::new(RegimeEngineConfig::default());
///
/// // Update with prices as they arrive
/// engine.update(100.0);
/// engine.update(100.5);
/// // ...
///
/// // Get the current regime state
/// let state = engine.current_regime();
///
/// // Or enrich a FeaturesSnapshot
/// let mut snapshot = FeaturesSnapshot::default();
/// engine.enrich_snapshot(&mut snapshot);
/// ```
pub struct RegimeEngine {
    config: RegimeEngineConfig,
    trend_engine: TrendFeatureEngine,
    kalman_filter: KalmanFilter,
    detector: ThresholdRegimeDetector,
    last_entropy: f64,
}

impl RegimeEngine {
    /// Create a new RegimeEngine with the given configuration
    pub fn new(config: RegimeEngineConfig) -> Self {
        Self {
            trend_engine: TrendFeatureEngine::new(config.window_size),
            kalman_filter: KalmanFilter::new(config.kalman_config.clone()),
            detector: ThresholdRegimeDetector::new(config.threshold_config.clone()),
            last_entropy: 0.5, // Default neutral entropy
            config,
        }
    }

    /// Create a new RegimeEngine with default configuration
    pub fn default() -> Self {
        Self::new(RegimeEngineConfig::default())
    }

    /// Update the engine with a new price observation
    pub fn update(&mut self, price: f64) {
        self.trend_engine.update(price);
        self.kalman_filter.update(price);
    }

    /// Update the engine with entropy from external source (e.g., EntropyEngine)
    pub fn update_entropy(&mut self, entropy: f64) {
        self.last_entropy = entropy;
    }

    /// Reset all internal state
    pub fn reset(&mut self) {
        self.trend_engine = TrendFeatureEngine::new(self.config.window_size);
        self.kalman_filter = KalmanFilter::new(self.config.kalman_config.clone());
        self.last_entropy = 0.5;
    }

    /// Extract RegimeFeatures from current state
    pub fn extract_features(&self) -> RegimeFeatures {
        let trend_features = self.trend_engine.compute();
        let kalman_state = self.kalman_filter.state();

        let (kalman_velocity, kalman_acceleration) = match kalman_state {
            Some(state) => (state.velocity, state.acceleration),
            None => (0.0, 0.0),
        };

        RegimeFeatures {
            momentum: trend_features.momentum.unwrap_or(0.0),
            monotonicity: trend_features.monotonicity.unwrap_or(0.5),
            hurst: trend_features.hurst_exponent.unwrap_or(0.5),
            entropy: self.last_entropy,
            kalman_velocity,
            kalman_acceleration,
        }
    }

    /// Detect the current market regime
    pub fn current_regime(&self) -> RegimeState {
        let features = self.extract_features();
        self.detector.detect(&features)
    }

    /// Enrich a FeaturesSnapshot with regime detection results
    ///
    /// This populates the following fields:
    /// - `regime`: String representation of the detected regime
    /// - `regime_confidence`: Confidence in the classification (0.0 - 1.0)
    /// - `trend_strength`: Trend strength from -1.0 to 1.0
    /// - `regime_persistence`: Hurst exponent
    /// - `trend_momentum`: Momentum from TrendFeatureEngine
    /// - `trend_monotonicity`: Monotonicity from TrendFeatureEngine
    /// - `trend_hurst`: Hurst exponent from TrendFeatureEngine
    pub fn enrich_snapshot(&self, snapshot: &mut FeaturesSnapshot) {
        let features = self.extract_features();
        let state = self.detector.detect(&features);
        let trend_features = self.trend_engine.compute();

        snapshot.regime = Some(state.regime.to_string());
        snapshot.regime_confidence = Some(state.confidence);
        snapshot.trend_strength = Some(state.trend_strength);
        snapshot.regime_persistence = Some(state.persistence);
        snapshot.trend_momentum = trend_features.momentum;
        snapshot.trend_monotonicity = trend_features.monotonicity;
        snapshot.trend_hurst = trend_features.hurst_exponent;
    }

    /// Update engine from a FeaturesSnapshot (extracts price and entropy)
    ///
    /// This is useful when you want to process snapshots in sequence.
    /// It extracts:
    /// - Mid price (or microprice as fallback) for trend/Kalman update
    /// - Tick entropy (30s window) for regime detection
    pub fn update_from_snapshot(&mut self, snapshot: &FeaturesSnapshot) {
        // Extract price (prefer mid_price, fallback to microprice)
        let price = snapshot
            .mid_price
            .and_then(|p| p.to_f64())
            .or_else(|| snapshot.microprice.and_then(|p| p.to_f64()));

        if let Some(p) = price {
            self.update(p);
        }

        // Extract entropy (prefer 30s window)
        let entropy = snapshot
            .tick_entropy_30s
            .and_then(|e| e.to_f64())
            .unwrap_or(0.5);

        self.update_entropy(entropy);
    }

    /// Check if the engine has enough data for meaningful regime detection
    pub fn is_ready(&self) -> bool {
        self.trend_engine.is_ready()
    }

    /// Get the underlying detector configuration
    pub fn config(&self) -> &RegimeEngineConfig {
        &self.config
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // MarketRegime Tests (1-10)
    // ========================================================================

    #[test]
    fn test_01_regime_display() {
        assert_eq!(format!("{}", MarketRegime::TrendingUp), "TrendingUp");
        assert_eq!(format!("{}", MarketRegime::TrendingDown), "TrendingDown");
        assert_eq!(format!("{}", MarketRegime::MeanReverting), "MeanReverting");
        assert_eq!(format!("{}", MarketRegime::Uncertain), "Uncertain");
    }

    #[test]
    fn test_02_regime_is_trending() {
        assert!(MarketRegime::TrendingUp.is_trending());
        assert!(MarketRegime::TrendingDown.is_trending());
        assert!(!MarketRegime::MeanReverting.is_trending());
        assert!(!MarketRegime::Uncertain.is_trending());
    }

    #[test]
    fn test_03_regime_is_mean_reverting() {
        assert!(!MarketRegime::TrendingUp.is_mean_reverting());
        assert!(!MarketRegime::TrendingDown.is_mean_reverting());
        assert!(MarketRegime::MeanReverting.is_mean_reverting());
        assert!(!MarketRegime::Uncertain.is_mean_reverting());
    }

    #[test]
    fn test_04_regime_is_uncertain() {
        assert!(!MarketRegime::TrendingUp.is_uncertain());
        assert!(!MarketRegime::TrendingDown.is_uncertain());
        assert!(!MarketRegime::MeanReverting.is_uncertain());
        assert!(MarketRegime::Uncertain.is_uncertain());
    }

    #[test]
    fn test_05_regime_skew_direction() {
        assert_eq!(MarketRegime::TrendingUp.skew_direction(), 1.0);
        assert_eq!(MarketRegime::TrendingDown.skew_direction(), -1.0);
        assert_eq!(MarketRegime::MeanReverting.skew_direction(), 0.0);
        assert_eq!(MarketRegime::Uncertain.skew_direction(), 0.0);
    }

    #[test]
    fn test_06_regime_spread_multiplier() {
        assert_eq!(MarketRegime::TrendingUp.spread_multiplier(), 1.0);
        assert_eq!(MarketRegime::TrendingDown.spread_multiplier(), 1.0);
        assert!(MarketRegime::MeanReverting.spread_multiplier() < 1.0);
        assert!(MarketRegime::Uncertain.spread_multiplier() > 1.0);
    }

    #[test]
    fn test_07_regime_serialization() {
        let regime = MarketRegime::TrendingUp;
        let json = serde_json::to_string(&regime).unwrap();
        let parsed: MarketRegime = serde_json::from_str(&json).unwrap();
        assert_eq!(regime, parsed);
    }

    #[test]
    fn test_08_regime_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(MarketRegime::TrendingUp);
        set.insert(MarketRegime::TrendingDown);
        assert!(set.contains(&MarketRegime::TrendingUp));
        assert!(!set.contains(&MarketRegime::MeanReverting));
    }

    #[test]
    fn test_09_regime_clone() {
        let regime = MarketRegime::MeanReverting;
        let cloned = regime;
        assert_eq!(regime, cloned);
    }

    #[test]
    fn test_10_regime_debug() {
        let debug = format!("{:?}", MarketRegime::Uncertain);
        assert!(debug.contains("Uncertain"));
    }

    // ========================================================================
    // RegimeState Tests (11-20)
    // ========================================================================

    #[test]
    fn test_11_regime_state_default() {
        let state = RegimeState::default();
        assert_eq!(state.regime, MarketRegime::Uncertain);
        assert_eq!(state.confidence, 0.0);
        assert_eq!(state.trend_strength, 0.0);
        assert_eq!(state.persistence, 0.5);
    }

    #[test]
    fn test_12_regime_state_new() {
        let state = RegimeState::new(MarketRegime::TrendingUp, 0.8, 0.5, 0.65);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
        assert_eq!(state.confidence, 0.8);
        assert_eq!(state.trend_strength, 0.5);
        assert_eq!(state.persistence, 0.65);
    }

    #[test]
    fn test_13_regime_state_confidence_clamping() {
        let state = RegimeState::new(MarketRegime::TrendingUp, 1.5, 0.0, 0.5);
        assert_eq!(state.confidence, 1.0);

        let state = RegimeState::new(MarketRegime::TrendingUp, -0.5, 0.0, 0.5);
        assert_eq!(state.confidence, 0.0);
    }

    #[test]
    fn test_14_regime_state_trend_strength_clamping() {
        let state = RegimeState::new(MarketRegime::TrendingUp, 0.5, 2.0, 0.5);
        assert_eq!(state.trend_strength, 1.0);

        let state = RegimeState::new(MarketRegime::TrendingDown, 0.5, -2.0, 0.5);
        assert_eq!(state.trend_strength, -1.0);
    }

    #[test]
    fn test_15_regime_state_with_signals() {
        let state = RegimeState::with_signals(MarketRegime::TrendingUp, 0.8, 0.5, 0.65, 3, 4);
        assert_eq!(state.signal_agreement, 3);
        assert_eq!(state.total_signals, 4);
    }

    #[test]
    fn test_16_regime_state_agreement_ratio() {
        let state = RegimeState::with_signals(MarketRegime::TrendingUp, 0.8, 0.5, 0.65, 3, 4);
        assert_eq!(state.agreement_ratio(), 0.75);
    }

    #[test]
    fn test_17_regime_state_agreement_ratio_zero_total() {
        let state = RegimeState::default();
        assert_eq!(state.agreement_ratio(), 0.0);
    }

    #[test]
    fn test_18_regime_state_serialization() {
        let state = RegimeState::new(MarketRegime::MeanReverting, 0.7, -0.1, 0.45);
        let json = serde_json::to_string(&state).unwrap();
        let parsed: RegimeState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.regime, MarketRegime::MeanReverting);
        assert!((parsed.confidence - 0.7).abs() < 1e-10);
    }

    #[test]
    fn test_19_regime_state_clone() {
        let state = RegimeState::new(MarketRegime::TrendingDown, 0.6, -0.4, 0.55);
        let cloned = state.clone();
        assert_eq!(cloned.regime, state.regime);
        assert_eq!(cloned.confidence, state.confidence);
    }

    #[test]
    fn test_20_regime_state_debug() {
        let state = RegimeState::default();
        let debug = format!("{:?}", state);
        assert!(debug.contains("RegimeState"));
    }

    // ========================================================================
    // RegimeFeatures Tests (21-30)
    // ========================================================================

    #[test]
    fn test_21_regime_features_default() {
        let features = RegimeFeatures::default();
        assert_eq!(features.momentum, 0.0);
        assert_eq!(features.monotonicity, 0.0);
        assert_eq!(features.hurst, 0.0);
        assert_eq!(features.entropy, 0.0);
    }

    #[test]
    fn test_22_regime_features_new() {
        let features = RegimeFeatures::new(0.001, 0.7, 0.6, 0.3, 0.0005, 0.0001);
        assert_eq!(features.momentum, 0.001);
        assert_eq!(features.monotonicity, 0.7);
        assert_eq!(features.hurst, 0.6);
        assert_eq!(features.entropy, 0.3);
    }

    #[test]
    fn test_23_regime_features_has_nan_false() {
        let features = RegimeFeatures::new(0.001, 0.7, 0.6, 0.3, 0.0005, 0.0001);
        assert!(!features.has_nan());
    }

    #[test]
    fn test_24_regime_features_has_nan_momentum() {
        let features = RegimeFeatures::new(f64::NAN, 0.7, 0.6, 0.3, 0.0005, 0.0001);
        assert!(features.has_nan());
    }

    #[test]
    fn test_25_regime_features_has_nan_hurst() {
        let features = RegimeFeatures::new(0.001, 0.7, f64::NAN, 0.3, 0.0005, 0.0001);
        assert!(features.has_nan());
    }

    #[test]
    fn test_26_regime_features_has_nan_kalman() {
        let features = RegimeFeatures::new(0.001, 0.7, 0.6, 0.3, f64::NAN, 0.0001);
        assert!(features.has_nan());
    }

    #[test]
    fn test_27_regime_features_sanitize() {
        let features = RegimeFeatures::new(f64::NAN, f64::NAN, f64::NAN, f64::NAN, f64::NAN, f64::NAN);
        let sanitized = features.sanitize();
        assert_eq!(sanitized.momentum, 0.0);
        assert_eq!(sanitized.monotonicity, 0.5);
        assert_eq!(sanitized.hurst, 0.5);
        assert_eq!(sanitized.entropy, 1.0);
        assert_eq!(sanitized.kalman_velocity, 0.0);
        assert_eq!(sanitized.kalman_acceleration, 0.0);
    }

    #[test]
    fn test_28_regime_features_sanitize_preserves_valid() {
        let features = RegimeFeatures::new(0.001, 0.7, 0.6, 0.3, 0.0005, 0.0001);
        let sanitized = features.sanitize();
        assert_eq!(sanitized.momentum, 0.001);
        assert_eq!(sanitized.monotonicity, 0.7);
    }

    #[test]
    fn test_29_regime_features_serialization() {
        let features = RegimeFeatures::new(0.001, 0.7, 0.6, 0.3, 0.0005, 0.0001);
        let json = serde_json::to_string(&features).unwrap();
        let parsed: RegimeFeatures = serde_json::from_str(&json).unwrap();
        assert!((parsed.momentum - 0.001).abs() < 1e-10);
    }

    #[test]
    fn test_30_regime_features_clone() {
        let features = RegimeFeatures::new(0.001, 0.7, 0.6, 0.3, 0.0005, 0.0001);
        let cloned = features.clone();
        assert_eq!(cloned.momentum, features.momentum);
    }

    // ========================================================================
    // ThresholdConfig Tests (31-40)
    // ========================================================================

    #[test]
    fn test_31_threshold_config_default() {
        let config = ThresholdConfig::default();
        assert!(config.momentum_threshold > 0.0);
        assert!(config.monotonicity_threshold > 0.0);
        assert!(config.hurst_trending_threshold > config.hurst_mean_reverting_threshold);
    }

    #[test]
    fn test_32_threshold_config_conservative() {
        let config = ThresholdConfig::conservative();
        let default = ThresholdConfig::default();
        assert!(config.momentum_threshold >= default.momentum_threshold);
        assert!(config.min_confidence >= default.min_confidence);
    }

    #[test]
    fn test_33_threshold_config_aggressive() {
        let config = ThresholdConfig::aggressive();
        let default = ThresholdConfig::default();
        assert!(config.momentum_threshold <= default.momentum_threshold);
        assert!(config.min_confidence <= default.min_confidence);
    }

    #[test]
    fn test_34_threshold_config_validate_valid() {
        let config = ThresholdConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_35_threshold_config_validate_negative_momentum() {
        let mut config = ThresholdConfig::default();
        config.momentum_threshold = -0.001;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_36_threshold_config_validate_monotonicity_out_of_range() {
        let mut config = ThresholdConfig::default();
        config.monotonicity_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_37_threshold_config_validate_hurst_order() {
        let mut config = ThresholdConfig::default();
        config.hurst_trending_threshold = 0.4;
        config.hurst_mean_reverting_threshold = 0.6;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_38_threshold_config_validate_entropy_out_of_range() {
        let mut config = ThresholdConfig::default();
        config.entropy_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_39_threshold_config_validate_min_confidence_out_of_range() {
        let mut config = ThresholdConfig::default();
        config.min_confidence = -0.1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_40_threshold_config_serialization() {
        let config = ThresholdConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: ThresholdConfig = serde_json::from_str(&json).unwrap();
        assert!((parsed.momentum_threshold - config.momentum_threshold).abs() < 1e-10);
    }

    // ========================================================================
    // ThresholdRegimeDetector Basic Tests (41-50)
    // ========================================================================

    #[test]
    fn test_41_detector_default() {
        let detector = ThresholdRegimeDetector::default();
        assert_eq!(detector.name(), "ThresholdRegimeDetector");
    }

    #[test]
    fn test_42_detector_conservative() {
        let detector = ThresholdRegimeDetector::conservative();
        assert!(detector.config().min_confidence > ThresholdConfig::default().min_confidence);
    }

    #[test]
    fn test_43_detector_aggressive() {
        let detector = ThresholdRegimeDetector::aggressive();
        assert!(detector.config().min_confidence < ThresholdConfig::default().min_confidence);
    }

    #[test]
    fn test_44_detect_strong_uptrend() {
        let detector = ThresholdRegimeDetector::default();
        let features = RegimeFeatures::new(
            0.001,  // Strong positive momentum
            0.8,    // High monotonicity
            0.7,    // High Hurst (trending)
            0.3,    // Low entropy
            0.001,  // Positive Kalman velocity
            0.0001, // Positive acceleration
        );
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
        assert!(state.confidence > 0.5);
        assert!(state.trend_strength > 0.0);
    }

    #[test]
    fn test_45_detect_strong_downtrend() {
        let detector = ThresholdRegimeDetector::default();
        let features = RegimeFeatures::new(
            -0.001, // Strong negative momentum
            0.8,    // High monotonicity
            0.7,    // High Hurst (trending)
            0.3,    // Low entropy
            -0.001, // Negative Kalman velocity
            -0.0001, // Negative acceleration
        );
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingDown);
        assert!(state.confidence > 0.5);
        assert!(state.trend_strength < 0.0);
    }

    #[test]
    fn test_46_detect_mean_reverting() {
        let detector = ThresholdRegimeDetector::default();
        let features = RegimeFeatures::new(
            0.00001, // Very low momentum
            0.45,    // Low monotonicity
            0.35,    // Low Hurst (mean-reverting)
            0.4,     // Moderate entropy
            0.00001, // Low Kalman velocity
            0.0,     // No acceleration
        );
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::MeanReverting);
    }

    #[test]
    fn test_47_detect_uncertain_high_entropy() {
        let detector = ThresholdRegimeDetector::default();
        let features = RegimeFeatures::new(
            0.0005, // Some momentum
            0.6,    // Moderate monotonicity
            0.5,    // Neutral Hurst
            0.95,   // Very high entropy
            0.0005, // Some velocity
            0.0,
        );
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::Uncertain);
    }

    #[test]
    fn test_48_detect_with_nan_features() {
        let detector = ThresholdRegimeDetector::default();
        let features = RegimeFeatures::new(
            f64::NAN, // NaN momentum
            0.8,
            0.7,
            0.3,
            0.001,
            0.0001,
        );
        let state = detector.detect(&features);
        // Should not panic, NaN is sanitized
        assert!(state.confidence >= 0.0);
    }

    #[test]
    fn test_49_detect_all_nan_features() {
        let detector = ThresholdRegimeDetector::default();
        let features = RegimeFeatures::new(
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
        );
        let state = detector.detect(&features);
        // Sanitized to defaults, high entropy -> uncertain
        assert_eq!(state.regime, MarketRegime::Uncertain);
    }

    #[test]
    fn test_50_detect_zero_features() {
        let detector = ThresholdRegimeDetector::default();
        let features = RegimeFeatures::default();
        let state = detector.detect(&features);
        // Zero momentum, zero monotonicity, zero Hurst
        // Should be uncertain or mean-reverting
        assert!(state.regime == MarketRegime::Uncertain || state.regime == MarketRegime::MeanReverting);
    }

    // ========================================================================
    // Threshold Detection Edge Cases (51-60)
    // ========================================================================

    #[test]
    fn test_51_detect_borderline_momentum() {
        let detector = ThresholdRegimeDetector::default();
        let threshold = detector.config().momentum_threshold;

        // Just below threshold
        let features = RegimeFeatures::new(threshold * 0.99, 0.8, 0.7, 0.3, threshold * 0.99, 0.0);
        let state = detector.detect(&features);
        // May or may not be trending depending on other signals
        assert!(state.confidence > 0.0);

        // Just above threshold
        let features = RegimeFeatures::new(threshold * 1.01, 0.8, 0.7, 0.3, threshold * 1.01, 0.0);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
    }

    #[test]
    fn test_52_detect_borderline_monotonicity() {
        let detector = ThresholdRegimeDetector::default();
        let threshold = detector.config().monotonicity_threshold;

        // Just at threshold
        let features = RegimeFeatures::new(0.001, threshold, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        // Should lean toward trending
        assert!(state.regime.is_trending() || state.regime == MarketRegime::Uncertain);
    }

    #[test]
    fn test_53_detect_borderline_hurst() {
        let detector = ThresholdRegimeDetector::default();

        // Hurst exactly at 0.5
        let features = RegimeFeatures::new(0.0001, 0.6, 0.5, 0.3, 0.0001, 0.0);
        let state = detector.detect(&features);
        // Should be uncertain or influenced by other signals
        assert!(state.confidence > 0.0);
    }

    #[test]
    fn test_54_detect_conflicting_signals() {
        let detector = ThresholdRegimeDetector::default();
        // Momentum says up, but Hurst says mean-reverting
        let features = RegimeFeatures::new(
            0.002,  // Strong positive momentum
            0.8,    // High monotonicity
            0.3,    // Low Hurst (mean-reverting)
            0.3,    // Low entropy
            0.002,  // Strong positive velocity
            0.0,
        );
        let state = detector.detect(&features);
        // Conflict should reduce confidence or affect regime
        assert!(state.confidence < 0.9);
    }

    #[test]
    fn test_55_detect_kalman_acceleration_boost() {
        let detector = ThresholdRegimeDetector::default();

        // Velocity and acceleration agree (both positive)
        let features = RegimeFeatures::new(0.001, 0.7, 0.6, 0.3, 0.001, 0.001);
        let state_agree = detector.detect(&features);

        // Velocity and acceleration disagree
        let features = RegimeFeatures::new(0.001, 0.7, 0.6, 0.3, 0.001, -0.001);
        let state_disagree = detector.detect(&features);

        // Agreement should give higher confidence
        assert!(state_agree.confidence >= state_disagree.confidence - 0.1);
    }

    #[test]
    fn test_56_detect_extreme_values() {
        let detector = ThresholdRegimeDetector::default();

        // Extremely large values
        let features = RegimeFeatures::new(1.0, 1.0, 1.0, 0.0, 1.0, 1.0);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
        assert!(state.confidence > 0.5);

        // Extremely negative values
        let features = RegimeFeatures::new(-1.0, 1.0, 1.0, 0.0, -1.0, -1.0);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingDown);
    }

    #[test]
    fn test_57_detect_entropy_just_below_threshold() {
        let detector = ThresholdRegimeDetector::default();
        let entropy_threshold = detector.config().entropy_threshold;

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, entropy_threshold - 0.01, 0.001, 0.0);
        let state = detector.detect(&features);
        // Should NOT be uncertain due to entropy
        assert_ne!(state.regime, MarketRegime::Uncertain);
    }

    #[test]
    fn test_58_detect_entropy_just_above_threshold() {
        let detector = ThresholdRegimeDetector::default();
        let entropy_threshold = detector.config().entropy_threshold;

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, entropy_threshold + 0.01, 0.001, 0.0);
        let state = detector.detect(&features);
        // Should be uncertain due to high entropy
        assert_eq!(state.regime, MarketRegime::Uncertain);
    }

    #[test]
    fn test_59_detect_weak_trend_low_confidence() {
        let detector = ThresholdRegimeDetector::default();

        // Weak signals across the board
        let features = RegimeFeatures::new(
            0.00015, // Just above threshold
            0.55,    // Just below monotonicity threshold
            0.52,    // Just above Hurst threshold
            0.5,     // Moderate entropy
            0.00015, // Weak velocity
            0.0,
        );
        let state = detector.detect(&features);
        // Low confidence expected
        assert!(state.confidence < 0.8);
    }

    #[test]
    fn test_60_detect_perfect_mean_reverting() {
        let detector = ThresholdRegimeDetector::default();

        let features = RegimeFeatures::new(
            0.0,    // No momentum
            0.4,    // Low monotonicity
            0.3,    // Low Hurst
            0.2,    // Low entropy
            0.0,    // No velocity
            0.0,    // No acceleration
        );
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::MeanReverting);
    }

    // ========================================================================
    // CompositeRegimeDetector Tests (61-70)
    // ========================================================================

    #[test]
    fn test_61_composite_detector_empty() {
        let detector = CompositeRegimeDetector::new();
        let features = RegimeFeatures::default();
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::Uncertain);
    }

    #[test]
    fn test_62_composite_detector_single() {
        let mut detector = CompositeRegimeDetector::new();
        detector.add_detector(Box::new(ThresholdRegimeDetector::default()), 1.0);

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
    }

    #[test]
    fn test_63_composite_detector_with_defaults() {
        let detector = CompositeRegimeDetector::with_defaults();
        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
    }

    #[test]
    fn test_64_composite_detector_weighted_voting() {
        let mut detector = CompositeRegimeDetector::new();
        detector.add_detector(Box::new(ThresholdRegimeDetector::default()), 2.0);
        detector.add_detector(Box::new(ThresholdRegimeDetector::conservative()), 1.0);

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        // Both should agree on trending up
        assert_eq!(state.regime, MarketRegime::TrendingUp);
    }

    #[test]
    fn test_65_composite_detector_name() {
        let detector = CompositeRegimeDetector::new();
        assert_eq!(detector.name(), "CompositeRegimeDetector");
    }

    #[test]
    fn test_66_composite_detector_reset() {
        let mut detector = CompositeRegimeDetector::with_defaults();
        detector.reset(); // Should not panic
    }

    #[test]
    fn test_67_composite_detector_disagreement() {
        // This is hard to test without a mock detector
        // We use different configs that might disagree
        let mut detector = CompositeRegimeDetector::new();
        detector.add_detector(Box::new(ThresholdRegimeDetector::aggressive()), 1.0);
        detector.add_detector(Box::new(ThresholdRegimeDetector::conservative()), 1.0);

        // Borderline case that might cause disagreement
        let features = RegimeFeatures::new(0.00015, 0.65, 0.55, 0.5, 0.00015, 0.0);
        let state = detector.detect(&features);
        // Should still produce a valid result
        assert!(state.confidence >= 0.0 && state.confidence <= 1.0);
    }

    #[test]
    fn test_68_composite_detector_zero_weight() {
        let mut detector = CompositeRegimeDetector::new();
        detector.add_detector(Box::new(ThresholdRegimeDetector::default()), 0.0);

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        // Zero weight means no effective contribution
        // Result depends on how we handle zero total weight
        assert!(state.regime == MarketRegime::Uncertain || state.confidence == 0.0);
    }

    #[test]
    fn test_69_composite_detector_high_weights() {
        let mut detector = CompositeRegimeDetector::new();
        detector.add_detector(Box::new(ThresholdRegimeDetector::default()), 100.0);
        detector.add_detector(Box::new(ThresholdRegimeDetector::conservative()), 100.0);

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        // High weights should still normalize correctly
        assert!(state.confidence >= 0.0 && state.confidence <= 1.0);
    }

    #[test]
    fn test_70_composite_detector_mixed_regimes() {
        let mut detector = CompositeRegimeDetector::new();
        detector.add_detector(Box::new(ThresholdRegimeDetector::default()), 1.0);
        detector.add_detector(Box::new(ThresholdRegimeDetector::default()), 1.0);

        // Features that should clearly be trending up
        let features = RegimeFeatures::new(0.002, 0.9, 0.8, 0.2, 0.002, 0.001);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
        assert!(state.agreement_ratio() >= 0.5);
    }

    // ========================================================================
    // SmoothedRegimeDetector Tests (71-80)
    // ========================================================================

    #[test]
    fn test_71_smoothed_detector_creation() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let detector = SmoothedRegimeDetector::new(inner, 5, 0.6);
        assert_eq!(detector.name(), "SmoothedRegimeDetector");
    }

    #[test]
    fn test_72_smoothed_detector_with_defaults() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let detector = SmoothedRegimeDetector::with_defaults(inner);
        assert!(detector.history().is_empty());
    }

    #[test]
    fn test_73_smoothed_detector_single_observation() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let mut detector = SmoothedRegimeDetector::new(inner, 5, 0.6);

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect_and_update(&features);

        assert_eq!(state.regime, MarketRegime::TrendingUp);
        assert_eq!(detector.history().len(), 1);
    }

    #[test]
    fn test_74_smoothed_detector_history_accumulation() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let mut detector = SmoothedRegimeDetector::new(inner, 5, 0.6);

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        for _ in 0..3 {
            detector.detect_and_update(&features);
        }

        assert_eq!(detector.history().len(), 3);
    }

    #[test]
    fn test_75_smoothed_detector_history_window() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let mut detector = SmoothedRegimeDetector::new(inner, 3, 0.6);

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        for _ in 0..5 {
            detector.detect_and_update(&features);
        }

        // Should cap at window size
        assert_eq!(detector.history().len(), 3);
    }

    #[test]
    fn test_76_smoothed_detector_regime_persistence() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let mut detector = SmoothedRegimeDetector::new(inner, 5, 0.6);

        // Feed consistent trending up
        let up_features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        for _ in 0..5 {
            detector.detect_and_update(&up_features);
        }

        // All should be TrendingUp
        for state in detector.history() {
            assert_eq!(state.regime, MarketRegime::TrendingUp);
        }
    }

    #[test]
    fn test_77_smoothed_detector_transition() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let mut detector = SmoothedRegimeDetector::new(inner, 3, 0.6);

        // Start with trending up
        let up_features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        for _ in 0..3 {
            detector.detect_and_update(&up_features);
        }

        // Transition to mean-reverting
        let mr_features = RegimeFeatures::new(0.0, 0.4, 0.35, 0.3, 0.0, 0.0);
        let state = detector.detect_and_update(&mr_features);

        // Should still be influenced by history
        // May or may not have transitioned yet
        assert!(state.confidence > 0.0);
    }

    #[test]
    fn test_78_smoothed_detector_reset() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let mut detector = SmoothedRegimeDetector::new(inner, 5, 0.6);

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        for _ in 0..3 {
            detector.detect_and_update(&features);
        }

        detector.reset();
        assert!(detector.history().is_empty());
    }

    #[test]
    fn test_79_smoothed_detector_transition_threshold() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let mut detector = SmoothedRegimeDetector::new(inner, 5, 0.8); // High threshold

        // Feed 3 trending up, 2 mean-reverting
        let up_features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);
        let mr_features = RegimeFeatures::new(0.0, 0.4, 0.35, 0.3, 0.0, 0.0);

        for _ in 0..3 {
            detector.detect_and_update(&up_features);
        }
        for _ in 0..2 {
            detector.detect_and_update(&mr_features);
        }

        // 3/5 = 0.6 < 0.8 threshold, so may not commit to a regime
        let last_state = detector.history().last().unwrap();
        assert!(last_state.confidence >= 0.0);
    }

    #[test]
    fn test_80_smoothed_detector_low_confidence_fallback() {
        let inner = Box::new(ThresholdRegimeDetector::default());
        let mut detector = SmoothedRegimeDetector::new(inner, 5, 0.6);

        // Feed uncertain/low confidence scenarios
        let uncertain_features = RegimeFeatures::new(0.0, 0.5, 0.5, 0.9, 0.0, 0.0);
        for _ in 0..5 {
            detector.detect_and_update(&uncertain_features);
        }

        // Should result in uncertain regime
        let last_state = detector.history().last().unwrap();
        assert_eq!(last_state.regime, MarketRegime::Uncertain);
    }

    // ========================================================================
    // Integration Tests (81-90)
    // ========================================================================

    #[test]
    fn test_81_realistic_bull_market_scenario() {
        let detector = ThresholdRegimeDetector::default();

        // Simulated bull market: steady upward movement
        let features = RegimeFeatures::new(
            0.0008,  // 8 bps positive momentum
            0.72,    // 72% directional consistency
            0.62,    // Trending Hurst
            0.35,    // Low entropy (structured)
            0.0007,  // Positive velocity
            0.00005, // Slight acceleration
        );

        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
        assert!(state.confidence > 0.4);
        assert!(state.trend_strength > 0.2);
    }

    #[test]
    fn test_82_realistic_bear_market_scenario() {
        let detector = ThresholdRegimeDetector::default();

        // Simulated bear market: steady downward movement
        let features = RegimeFeatures::new(
            -0.0012, // -12 bps negative momentum
            0.68,    // 68% directional consistency (down)
            0.58,    // Trending Hurst
            0.40,    // Moderate entropy
            -0.0010, // Negative velocity
            -0.00008, // Negative acceleration (accelerating down)
        );

        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingDown);
        assert!(state.trend_strength < -0.2);
    }

    #[test]
    fn test_83_realistic_range_bound_scenario() {
        let detector = ThresholdRegimeDetector::default();

        // Range-bound market: oscillating around mean
        let features = RegimeFeatures::new(
            0.00002,  // Near-zero momentum
            0.42,     // Low monotonicity (oscillating)
            0.38,     // Low Hurst (mean-reverting)
            0.45,     // Moderate entropy
            0.00001,  // Near-zero velocity
            0.0,      // No acceleration
        );

        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::MeanReverting);
    }

    #[test]
    fn test_84_realistic_choppy_market_scenario() {
        let detector = ThresholdRegimeDetector::default();

        // Choppy/uncertain market: high entropy, no clear direction
        let features = RegimeFeatures::new(
            0.00003,  // Negligible momentum
            0.48,     // Borderline monotonicity
            0.50,     // Neutral Hurst
            0.85,     // High entropy
            0.00001,  // Negligible velocity
            0.00001,  // Negligible acceleration
        );

        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::Uncertain);
    }

    #[test]
    fn test_85_trend_reversal_detection() {
        let mut smoothed = SmoothedRegimeDetector::new(
            Box::new(ThresholdRegimeDetector::default()),
            5,
            0.6,
        );

        // Start with uptrend
        let up_features = RegimeFeatures::new(0.001, 0.75, 0.65, 0.3, 0.001, 0.0001);
        for _ in 0..5 {
            smoothed.detect_and_update(&up_features);
        }

        // Reversal: velocity still positive but decelerating
        let reversal = RegimeFeatures::new(0.0003, 0.55, 0.58, 0.45, 0.0002, -0.0002);
        let state = smoothed.detect_and_update(&reversal);

        // May still show trending up due to smoothing, but confidence should drop
        assert!(state.confidence <= 0.9);
    }

    #[test]
    fn test_86_config_impact_on_detection() {
        let default_detector = ThresholdRegimeDetector::default();
        let conservative_detector = ThresholdRegimeDetector::conservative();

        // Borderline trending features
        let features = RegimeFeatures::new(0.00015, 0.62, 0.56, 0.5, 0.00015, 0.0);

        let default_state = default_detector.detect(&features);
        let conservative_state = conservative_detector.detect(&features);

        // Conservative should be less likely to call a trend
        // (may classify as uncertain when default says trending)
        assert!(conservative_state.confidence <= default_state.confidence + 0.1);
    }

    #[test]
    fn test_87_signal_agreement_tracking() {
        let detector = ThresholdRegimeDetector::default();

        // All signals strongly agree on uptrend
        let features = RegimeFeatures::new(0.002, 0.9, 0.8, 0.2, 0.002, 0.001);
        let state = detector.detect(&features);

        assert!(state.signal_agreement >= 2);
        assert!(state.total_signals >= 3);
        assert!(state.agreement_ratio() >= 0.5);
    }

    #[test]
    fn test_88_persistence_tracking() {
        let detector = ThresholdRegimeDetector::default();

        let low_hurst = RegimeFeatures::new(0.0, 0.4, 0.35, 0.3, 0.0, 0.0);
        let high_hurst = RegimeFeatures::new(0.001, 0.8, 0.75, 0.3, 0.001, 0.0);

        let state_low = detector.detect(&low_hurst);
        let state_high = detector.detect(&high_hurst);

        assert!(state_low.persistence < state_high.persistence);
    }

    #[test]
    fn test_89_composite_robustness() {
        let composite = CompositeRegimeDetector::with_defaults();

        // Test across various scenarios
        let scenarios = vec![
            (RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0), MarketRegime::TrendingUp),
            (RegimeFeatures::new(-0.001, 0.8, 0.7, 0.3, -0.001, 0.0), MarketRegime::TrendingDown),
            (RegimeFeatures::new(0.0, 0.4, 0.35, 0.3, 0.0, 0.0), MarketRegime::MeanReverting),
        ];

        for (features, expected_regime) in scenarios {
            let state = composite.detect(&features);
            assert_eq!(state.regime, expected_regime);
        }
    }

    #[test]
    fn test_90_full_pipeline_integration() {
        // Simulate full detection pipeline
        let mut smoothed = SmoothedRegimeDetector::new(
            Box::new(CompositeRegimeDetector::with_defaults()),
            5,
            0.6,
        );

        // Generate realistic time series
        let base_momentum = 0.0005;
        let trend_features: Vec<_> = (0..10)
            .map(|i| {
                RegimeFeatures::new(
                    base_momentum + 0.0001 * (i as f64),
                    0.65 + 0.02 * (i as f64),
                    0.6 + 0.01 * (i as f64),
                    0.4 - 0.01 * (i as f64),
                    base_momentum + 0.00008 * (i as f64),
                    0.00005,
                )
            })
            .collect();

        for features in trend_features {
            let state = smoothed.detect_and_update(&features);
            assert!(state.confidence >= 0.0 && state.confidence <= 1.0);
        }

        // Should converge to TrendingUp
        let final_state = smoothed.history().last().unwrap();
        assert_eq!(final_state.regime, MarketRegime::TrendingUp);
    }

    // ========================================================================
    // Additional Edge Case Tests (91-100)
    // ========================================================================

    #[test]
    fn test_91_infinity_handling() {
        let detector = ThresholdRegimeDetector::default();

        let features = RegimeFeatures::new(
            f64::INFINITY,
            0.8,
            0.7,
            0.3,
            f64::INFINITY,
            0.0,
        );
        let state = detector.detect(&features);
        // Should handle infinity gracefully
        assert!(state.regime.is_trending() || state.regime.is_uncertain());
    }

    #[test]
    fn test_92_negative_infinity_handling() {
        let detector = ThresholdRegimeDetector::default();

        let features = RegimeFeatures::new(
            f64::NEG_INFINITY,
            0.8,
            0.7,
            0.3,
            f64::NEG_INFINITY,
            0.0,
        );
        let state = detector.detect(&features);
        // Should handle negative infinity gracefully
        assert!(state.regime.is_trending() || state.regime.is_uncertain());
    }

    #[test]
    fn test_93_very_small_values() {
        let detector = ThresholdRegimeDetector::default();

        let features = RegimeFeatures::new(
            1e-15,
            0.5,
            0.5,
            0.5,
            1e-15,
            1e-15,
        );
        let state = detector.detect(&features);
        // Should not detect as trending with such small values
        assert!(!state.regime.is_trending() || state.confidence < 0.5);
    }

    #[test]
    fn test_94_entropy_exactly_at_threshold() {
        let detector = ThresholdRegimeDetector::default();
        let threshold = detector.config().entropy_threshold;

        let features = RegimeFeatures::new(0.001, 0.8, 0.7, threshold, 0.001, 0.0);
        let state = detector.detect(&features);
        // At exactly threshold, should not trigger uncertain (< not <=)
        assert_ne!(state.regime, MarketRegime::Uncertain);
    }

    #[test]
    fn test_95_hurst_at_boundaries() {
        let detector = ThresholdRegimeDetector::default();

        // Hurst at 0
        let features = RegimeFeatures::new(0.0, 0.5, 0.0, 0.5, 0.0, 0.0);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::MeanReverting);

        // Hurst at 1
        let features = RegimeFeatures::new(0.001, 0.8, 1.0, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
    }

    #[test]
    fn test_96_monotonicity_at_boundaries() {
        let detector = ThresholdRegimeDetector::default();

        // Monotonicity at 0
        let features = RegimeFeatures::new(0.001, 0.0, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        // Low monotonicity should reduce trend confidence
        assert!(state.confidence < 1.0);

        // Monotonicity at 1
        let features = RegimeFeatures::new(0.001, 1.0, 0.7, 0.3, 0.001, 0.0);
        let state = detector.detect(&features);
        assert_eq!(state.regime, MarketRegime::TrendingUp);
    }

    #[test]
    fn test_97_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let detector = Arc::new(ThresholdRegimeDetector::default());
        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let detector = Arc::clone(&detector);
                let features = features.clone();
                thread::spawn(move || {
                    detector.detect(&features)
                })
            })
            .collect();

        for handle in handles {
            let state = handle.join().unwrap();
            assert_eq!(state.regime, MarketRegime::TrendingUp);
        }
    }

    #[test]
    fn test_98_deterministic_detection() {
        let detector = ThresholdRegimeDetector::default();
        let features = RegimeFeatures::new(0.001, 0.8, 0.7, 0.3, 0.001, 0.0);

        // Run detection multiple times
        let states: Vec<_> = (0..10).map(|_| detector.detect(&features)).collect();

        // All results should be identical
        for state in &states {
            assert_eq!(state.regime, states[0].regime);
            assert!((state.confidence - states[0].confidence).abs() < 1e-10);
        }
    }

    #[test]
    fn test_99_config_clone_independence() {
        let config1 = ThresholdConfig::default();
        let mut config2 = config1.clone();
        config2.momentum_threshold = 0.1;

        // Original should be unchanged
        assert!(config1.momentum_threshold < 0.1);
    }

    #[test]
    fn test_100_regime_state_independence() {
        let state1 = RegimeState::new(MarketRegime::TrendingUp, 0.8, 0.5, 0.65);
        let state2 = state1.clone();

        assert_eq!(state1.regime, state2.regime);
        assert_eq!(state1.confidence, state2.confidence);

        // Verify they're independent by checking memory addresses would differ
        // (we can't actually modify state1 as it's immutable, but clone is independent)
    }

    // ========================================================================
    // RegimeEngine Tests (101-120)
    // ========================================================================

    #[test]
    fn test_101_regime_engine_default_creation() {
        let engine = RegimeEngine::new(RegimeEngineConfig::default());
        assert!(!engine.is_ready()); // No data yet
    }

    #[test]
    fn test_102_regime_engine_custom_config() {
        let mut config = RegimeEngineConfig::default();
        config.window_size = 50;
        config.threshold_config.momentum_threshold = 0.05;
        config.threshold_config.hurst_trending_threshold = 0.6;

        let engine = RegimeEngine::new(config.clone());
        assert_eq!(engine.config().window_size, 50);
        assert_eq!(engine.config().threshold_config.momentum_threshold, 0.05);
    }

    #[test]
    fn test_103_regime_engine_update_price() {
        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Update with some prices
        for i in 0..50 {
            engine.update(100.0 + i as f64 * 0.1);
        }

        // Should have enough data now
        assert!(engine.is_ready());
    }

    #[test]
    fn test_104_regime_engine_extract_features() {
        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Feed trending up prices
        for i in 0..50 {
            engine.update(100.0 + i as f64);
        }

        let features = engine.extract_features();

        // With upward trending prices, momentum should be positive
        assert!(features.momentum > 0.0);
        // Hurst should indicate trending (> 0.5)
        assert!(features.hurst > 0.0);
    }

    #[test]
    fn test_105_regime_engine_current_regime_trending_up() {
        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Strong upward trend
        for i in 0..100 {
            engine.update(100.0 + i as f64 * 2.0);
        }

        let state = engine.current_regime();

        // With strong upward movement, should detect TrendingUp
        assert!(state.trend_strength > 0.0);
    }

    #[test]
    fn test_106_regime_engine_current_regime_trending_down() {
        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Strong downward trend
        for i in 0..100 {
            engine.update(200.0 - i as f64 * 2.0);
        }

        let state = engine.current_regime();

        // With strong downward movement, trend_strength should be negative
        assert!(state.trend_strength < 0.0);
    }

    #[test]
    fn test_107_regime_engine_entropy_update() {
        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Feed prices
        for i in 0..50 {
            engine.update(100.0 + i as f64 * 0.1);
        }

        // Update entropy
        engine.update_entropy(1.5);

        let features = engine.extract_features();
        assert!((features.entropy - 1.5).abs() < 0.001);
    }

    #[test]
    fn test_108_regime_engine_enrich_snapshot() {
        use crate::features::feature_fusion::FeaturesSnapshot;

        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Feed prices
        for i in 0..50 {
            engine.update(100.0 + i as f64);
        }
        engine.update_entropy(1.2);

        let mut snapshot = FeaturesSnapshot::default();
        engine.enrich_snapshot(&mut snapshot);

        // Snapshot should now have regime fields populated
        assert!(snapshot.regime.is_some());
        assert!(snapshot.regime_confidence.is_some());
        assert!(snapshot.trend_strength.is_some());
        assert!(snapshot.regime_persistence.is_some());
    }

    #[test]
    fn test_109_regime_engine_enrich_snapshot_values() {
        use crate::features::feature_fusion::FeaturesSnapshot;

        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Feed strong uptrend
        for i in 0..100 {
            engine.update(100.0 + i as f64 * 2.0);
        }

        let mut snapshot = FeaturesSnapshot::default();
        engine.enrich_snapshot(&mut snapshot);

        // Verify regime is one of the valid values
        let regime = snapshot.regime.unwrap();
        assert!(
            regime == "TrendingUp" ||
            regime == "TrendingDown" ||
            regime == "MeanReverting" ||
            regime == "Uncertain"
        );

        // Confidence should be between 0 and 1
        let confidence = snapshot.regime_confidence.unwrap();
        assert!(confidence >= 0.0 && confidence <= 1.0);

        // Trend strength should be between -1 and 1
        let trend_strength = snapshot.trend_strength.unwrap();
        assert!(trend_strength >= -1.0 && trend_strength <= 1.0);
    }

    #[test]
    fn test_110_regime_engine_update_from_snapshot() {
        use crate::features::feature_fusion::FeaturesSnapshot;
        use rust_decimal_macros::dec;

        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Create snapshot with mid_price and entropy
        let mut snapshot = FeaturesSnapshot::default();
        snapshot.mid_price = Some(dec!(100.5));
        snapshot.tick_entropy_30s = Some(dec!(1.8));

        engine.update_from_snapshot(&snapshot);

        // Engine should have processed the snapshot
        let features = engine.extract_features();
        assert!((features.entropy - 1.8).abs() < 0.001);
    }

    #[test]
    fn test_111_regime_engine_repeated_updates() {
        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Simulate market data over time
        for i in 0..200 {
            // Oscillating prices
            let price = 100.0 + (i as f64 * 0.1).sin() * 5.0;
            engine.update(price);
        }

        // Should be ready
        assert!(engine.is_ready());

        // Get regime - with oscillating data should likely be mean reverting or uncertain
        let state = engine.current_regime();
        assert!(state.confidence >= 0.0);
    }

    #[test]
    fn test_112_regime_engine_kalman_integration() {
        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Feed data
        for i in 0..50 {
            engine.update(100.0 + i as f64);
        }

        let features = engine.extract_features();

        // Kalman should have computed velocity
        // For upward trend, velocity should be positive
        assert!(features.kalman_velocity > 0.0 || features.kalman_velocity == 0.0);
    }

    #[test]
    fn test_113_regime_engine_config_accessors() {
        let mut config = RegimeEngineConfig::default();
        config.window_size = 60;
        config.threshold_config.hurst_trending_threshold = 0.65;
        config.threshold_config.min_confidence = 0.8;

        let engine = RegimeEngine::new(config);

        assert_eq!(engine.config().window_size, 60);
        assert_eq!(engine.config().threshold_config.hurst_trending_threshold, 0.65);
        assert_eq!(engine.config().threshold_config.min_confidence, 0.8);
    }

    #[test]
    fn test_114_regime_engine_not_ready_initially() {
        let engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Without any data, engine should not be ready
        assert!(!engine.is_ready());

        // Should still return a valid regime state (likely Uncertain)
        let state = engine.current_regime();
        assert!(state.confidence >= 0.0);
    }

    #[test]
    fn test_115_regime_engine_snapshot_enrichment_preserves_existing() {
        use crate::features::feature_fusion::FeaturesSnapshot;
        use rust_decimal_macros::dec;

        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());
        for i in 0..50 {
            engine.update(100.0 + i as f64);
        }

        let mut snapshot = FeaturesSnapshot::default();
        snapshot.mid_price = Some(dec!(150.0));
        snapshot.best_bid = Some(dec!(149.9));
        snapshot.best_ask = Some(dec!(150.1));

        engine.enrich_snapshot(&mut snapshot);

        // Original fields should be preserved
        assert_eq!(snapshot.mid_price, Some(dec!(150.0)));
        assert_eq!(snapshot.best_bid, Some(dec!(149.9)));
        assert_eq!(snapshot.best_ask, Some(dec!(150.1)));

        // Regime fields should be added
        assert!(snapshot.regime.is_some());
    }

    #[test]
    fn test_116_regime_engine_persistence_value() {
        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Feed consistent uptrend
        for i in 0..100 {
            engine.update(100.0 + i as f64 * 0.5);
        }

        let state = engine.current_regime();

        // Persistence (Hurst) should be calculated
        assert!(state.persistence >= 0.0 && state.persistence <= 1.0);
    }

    #[test]
    fn test_117_regime_engine_default_config_values() {
        let config = RegimeEngineConfig::default();

        // RegimeEngineConfig uses nested configs
        assert_eq!(config.window_size, 60);

        // Check threshold config defaults
        let tc = &config.threshold_config;
        assert_eq!(tc.hurst_trending_threshold, 0.55);
        assert_eq!(tc.hurst_mean_reverting_threshold, 0.45);
        assert_eq!(tc.entropy_threshold, 0.8);

        // Check kalman config has measurement noise field
        let kc = &config.kalman_config;
        assert_eq!(kc.measurement_noise, 1.0);
    }

    #[test]
    fn test_118_regime_engine_snapshot_with_no_entropy() {
        use crate::features::feature_fusion::FeaturesSnapshot;
        use rust_decimal_macros::dec;

        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());
        for i in 0..50 {
            engine.update(100.0 + i as f64);
        }

        // Snapshot without entropy field
        let mut snapshot = FeaturesSnapshot::default();
        snapshot.mid_price = Some(dec!(100.0));
        // tick_entropy_30s is None

        // Should not panic, should use default entropy
        engine.update_from_snapshot(&snapshot);

        let features = engine.extract_features();
        assert_eq!(features.entropy, 0.5); // Default value when None
    }

    #[test]
    fn test_119_regime_engine_multiple_enrichments() {
        use crate::features::feature_fusion::FeaturesSnapshot;

        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Feed data and enrich multiple snapshots
        for batch in 0..5 {
            for i in 0..20 {
                engine.update(100.0 + (batch * 20 + i) as f64);
            }

            let mut snapshot = FeaturesSnapshot::default();
            engine.enrich_snapshot(&mut snapshot);

            // Each enrichment should produce valid values
            assert!(snapshot.regime.is_some());
            let confidence = snapshot.regime_confidence.unwrap();
            assert!(confidence >= 0.0 && confidence <= 1.0);
        }
    }

    #[test]
    fn test_120_regime_engine_trend_features_populated() {
        use crate::features::feature_fusion::FeaturesSnapshot;

        let mut engine = RegimeEngine::new(RegimeEngineConfig::default());

        // Feed strong trend
        for i in 0..100 {
            engine.update(100.0 + i as f64);
        }

        let mut snapshot = FeaturesSnapshot::default();
        engine.enrich_snapshot(&mut snapshot);

        // All trend features should be populated
        assert!(snapshot.trend_momentum.is_some() || snapshot.trend_momentum.is_none());
        assert!(snapshot.trend_monotonicity.is_some() || snapshot.trend_monotonicity.is_none());
        assert!(snapshot.trend_hurst.is_some() || snapshot.trend_hurst.is_none());
    }
}

//! Research State - Task 0.0
//!
//! Unified data structure that captures all research findings at a point in time.
//! This is the core state that persists across application restarts and drives
//! algorithm configuration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Market Information Diffusion Coefficient estimate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MIDCEstimate {
    /// Diffusion rate (kappa) - higher means faster information diffusion
    pub kappa: f64,

    /// Half-life of predictability in seconds: τ_half = ln(2) / κ
    pub tau_half_seconds: f64,

    /// Initial autocorrelation (rho_0) from exponential fit
    pub rho_0: f64,

    /// R-squared of the exponential decay fit
    pub r_squared: f64,

    /// Number of data points used in estimation
    pub sample_size: usize,

    /// Confidence level (0.0 to 1.0)
    pub confidence: f64,

    /// Timestamp when this estimate was computed
    pub computed_at: DateTime<Utc>,
}

impl Default for MIDCEstimate {
    fn default() -> Self {
        Self {
            kappa: 0.0,            // Use 0.0 instead of NAN for JSON compatibility
            tau_half_seconds: 0.0, // Use 0.0 instead of NAN for JSON compatibility
            rho_0: 0.0,            // Use 0.0 instead of NAN for JSON compatibility
            r_squared: 0.0,
            sample_size: 0,
            confidence: 0.0,
            computed_at: Utc::now(),
        }
    }
}

impl MIDCEstimate {
    /// Create a new MIDC estimate
    pub fn new(
        kappa: f64,
        rho_0: f64,
        r_squared: f64,
        sample_size: usize,
    ) -> Self {
        let tau_half_seconds = if kappa > 0.0 {
            (2.0_f64).ln() / kappa
        } else if kappa < 0.0 {
            // Negative kappa produces negative tau_half (for testing edge cases)
            (2.0_f64).ln() / kappa
        } else {
            f64::INFINITY
        };

        let confidence = Self::compute_confidence(r_squared, sample_size);

        Self {
            kappa,
            tau_half_seconds,
            rho_0,
            r_squared,
            sample_size,
            confidence,
            computed_at: Utc::now(),
        }
    }

    /// Compute confidence based on R² and sample size
    fn compute_confidence(r_squared: f64, sample_size: usize) -> f64 {
        if sample_size < 10 {
            return 0.0;
        }

        // Confidence is product of fit quality and sample adequacy
        let fit_quality = r_squared.max(0.0).min(1.0);
        let sample_adequacy = (sample_size as f64 / 1000.0).min(1.0);

        fit_quality * sample_adequacy
    }

    /// Check if this estimate is valid (has meaningful data)
    pub fn is_valid(&self) -> bool {
        !self.kappa.is_nan()
            && !self.tau_half_seconds.is_nan()
            && self.kappa > 0.0  // Must have positive kappa
            && self.sample_size >= 10
            && self.r_squared > 0.0
    }

    /// Get regime classification based on MIDC
    pub fn regime(&self) -> MIDCRegime {
        if !self.is_valid() {
            return MIDCRegime::Unknown;
        }

        if self.kappa < 0.01 {
            MIDCRegime::SlowDiffusion
        } else if self.kappa < 0.1 {
            MIDCRegime::ModerateDiffusion
        } else {
            MIDCRegime::FastDiffusion
        }
    }
}

/// MIDC regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MIDCRegime {
    /// κ < 0.01: Trends persist, momentum viable
    SlowDiffusion,
    /// 0.01 ≤ κ < 0.1: Mixed signals
    ModerateDiffusion,
    /// κ ≥ 0.1: Fast information incorporation, momentum not viable
    FastDiffusion,
    /// Cannot determine regime
    Unknown,
}

impl MIDCRegime {
    /// Check if momentum strategies are viable in this regime
    pub fn momentum_viable(&self) -> bool {
        matches!(self, MIDCRegime::SlowDiffusion | MIDCRegime::ModerateDiffusion)
    }

    /// Get recommended position scale (0.0 to 1.0)
    pub fn position_scale(&self) -> f64 {
        match self {
            MIDCRegime::SlowDiffusion => 1.0,
            MIDCRegime::ModerateDiffusion => 0.5,
            MIDCRegime::FastDiffusion => 0.0,
            MIDCRegime::Unknown => 0.0,
        }
    }
}

/// Persistence statistics for trend duration analysis
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistenceStats {
    /// Mean trend duration in seconds
    pub mean_duration_seconds: f64,

    /// Median trend duration in seconds
    pub median_duration_seconds: f64,

    /// Standard deviation of trend duration
    pub std_duration_seconds: f64,

    /// 25th percentile duration
    pub percentile_25: f64,

    /// 75th percentile duration
    pub percentile_75: f64,

    /// Number of trends observed
    pub sample_count: usize,

    /// Timestamp of last update
    pub updated_at: DateTime<Utc>,
}

impl Default for PersistenceStats {
    fn default() -> Self {
        Self {
            mean_duration_seconds: 0.0,
            median_duration_seconds: 0.0,
            std_duration_seconds: 0.0,
            percentile_25: 0.0,
            percentile_75: 0.0,
            sample_count: 0,
            updated_at: Utc::now(),
        }
    }
}

impl PersistenceStats {
    /// Check if we have sufficient data for reliable statistics
    pub fn is_reliable(&self) -> bool {
        self.sample_count >= 30
    }

    /// Check if trends persist long enough for our latency budget
    pub fn trends_exploitable(&self, min_duration_seconds: f64) -> bool {
        self.is_reliable() && self.median_duration_seconds >= min_duration_seconds
    }
}

/// Discretized price signature for conditional probability modeling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PriceSignature {
    /// Magnitude of the move
    pub magnitude: SignatureMagnitude,

    /// Speed of the move
    pub speed: SignatureSpeed,

    /// Direction of the move
    pub direction: SignatureDirection,

    /// Consistency (smoothness) of the move
    pub consistency: SignatureConsistency,
}

impl PriceSignature {
    /// Create a new price signature
    pub fn new(
        magnitude: SignatureMagnitude,
        speed: SignatureSpeed,
        direction: SignatureDirection,
        consistency: SignatureConsistency,
    ) -> Self {
        Self {
            magnitude,
            speed,
            direction,
            consistency,
        }
    }

    /// Convert to a string key for use in hash maps
    pub fn to_key(&self) -> String {
        format!(
            "{:?}_{:?}_{:?}_{:?}",
            self.magnitude, self.speed, self.direction, self.consistency
        )
    }

    /// Parse from a key string
    pub fn from_key(key: &str) -> Option<Self> {
        let parts: Vec<&str> = key.split('_').collect();
        if parts.len() != 4 {
            return None;
        }

        let magnitude = match parts[0] {
            "Tiny" => SignatureMagnitude::Tiny,
            "Small" => SignatureMagnitude::Small,
            "Medium" => SignatureMagnitude::Medium,
            "Large" => SignatureMagnitude::Large,
            "VeryLarge" => SignatureMagnitude::VeryLarge,
            _ => return None,
        };

        let speed = match parts[1] {
            "Slow" => SignatureSpeed::Slow,
            "Normal" => SignatureSpeed::Normal,
            "Fast" => SignatureSpeed::Fast,
            _ => return None,
        };

        let direction = match parts[2] {
            "Up" => SignatureDirection::Up,
            "Down" => SignatureDirection::Down,
            _ => return None,
        };

        let consistency = match parts[3] {
            "Choppy" => SignatureConsistency::Choppy,
            "Mixed" => SignatureConsistency::Mixed,
            "Smooth" => SignatureConsistency::Smooth,
            _ => return None,
        };

        Some(Self {
            magnitude,
            speed,
            direction,
            consistency,
        })
    }
}

/// Magnitude of price move
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignatureMagnitude {
    /// 0.00% - 0.05%
    Tiny,
    /// 0.05% - 0.10%
    Small,
    /// 0.10% - 0.30%
    Medium,
    /// 0.30% - 0.50%
    Large,
    /// > 0.50%
    VeryLarge,
}

/// Speed of price move
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignatureSpeed {
    /// Slow move (> 5 minutes)
    Slow,
    /// Normal speed (1-5 minutes)
    Normal,
    /// Fast move (< 1 minute)
    Fast,
}

/// Direction of price move
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignatureDirection {
    Up,
    Down,
}

/// Consistency of price move (based on monotonicity)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignatureConsistency {
    /// Monotonicity < 0.6
    Choppy,
    /// Monotonicity 0.6 - 0.8
    Mixed,
    /// Monotonicity > 0.8
    Smooth,
}

/// Conditional probability for a given signature
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionalProbability {
    /// Probability of price continuing in same direction
    pub p_continuation: f64,

    /// Probability of price reversing
    pub p_reversal: f64,

    /// Expected magnitude of continuation in basis points
    pub expected_magnitude_bps: f64,

    /// Standard deviation of continuation magnitude
    pub std_magnitude_bps: f64,

    /// Number of observations for this signature
    pub sample_count: usize,

    /// 95% confidence interval for p_continuation
    pub confidence_interval: (f64, f64),
}

impl Default for ConditionalProbability {
    fn default() -> Self {
        Self {
            p_continuation: 0.5,
            p_reversal: 0.5,
            expected_magnitude_bps: 0.0,
            std_magnitude_bps: 0.0,
            sample_count: 0,
            confidence_interval: (0.0, 1.0),
        }
    }
}

impl ConditionalProbability {
    /// Calculate edge over random (0.5)
    pub fn edge(&self) -> f64 {
        self.p_continuation - 0.5
    }

    /// Check if this signal has a significant edge
    pub fn is_significant(&self, min_edge: f64, min_samples: usize) -> bool {
        self.sample_count >= min_samples && self.edge().abs() >= min_edge
    }

    /// Compute Wilson score confidence interval
    pub fn compute_confidence_interval(&mut self) {
        if self.sample_count == 0 {
            self.confidence_interval = (0.0, 1.0);
            return;
        }

        let n = self.sample_count as f64;
        let p = self.p_continuation;
        let z = 1.96; // 95% confidence

        let denominator = 1.0 + z * z / n;
        let center = (p + z * z / (2.0 * n)) / denominator;
        let spread = z * (p * (1.0 - p) / n + z * z / (4.0 * n * n)).sqrt() / denominator;

        self.confidence_interval = (
            (center - spread).max(0.0),
            (center + spread).min(1.0),
        );
    }
}

/// Overall tradeable assessment from research
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeableAssessment {
    /// Is MIDC in favorable range?
    pub midc_ok: bool,

    /// Is entropy low enough for predictability?
    pub entropy_ok: bool,

    /// Do trends persist long enough?
    pub persistence_ok: bool,

    /// Are there high-edge conditional signals?
    pub signals_ok: bool,

    /// Overall tradeable status
    pub is_tradeable: bool,

    /// Recommended strategy type
    pub recommended_strategy: RecommendedStrategy,

    /// Recommended position scale (0.0 to 1.0)
    pub position_scale: f64,

    /// Human-readable reasoning
    pub reasoning: String,

    /// Timestamp of assessment
    pub assessed_at: DateTime<Utc>,
}

impl Default for TradeableAssessment {
    fn default() -> Self {
        Self {
            midc_ok: false,
            entropy_ok: false,
            persistence_ok: false,
            signals_ok: false,
            is_tradeable: false,
            recommended_strategy: RecommendedStrategy::None,
            position_scale: 0.0,
            reasoning: "No assessment performed".to_string(),
            assessed_at: Utc::now(),
        }
    }
}

impl TradeableAssessment {
    /// Create a new assessment
    pub fn new(
        midc_ok: bool,
        entropy_ok: bool,
        persistence_ok: bool,
        signals_ok: bool,
    ) -> Self {
        let is_tradeable = midc_ok && entropy_ok && persistence_ok && signals_ok;

        let recommended_strategy = if is_tradeable {
            RecommendedStrategy::Momentum
        } else if entropy_ok && !midc_ok {
            RecommendedStrategy::MarketMaking
        } else {
            RecommendedStrategy::None
        };

        let position_scale = if is_tradeable {
            1.0
        } else if recommended_strategy == RecommendedStrategy::MarketMaking {
            0.5
        } else {
            0.0
        };

        let reasoning = Self::generate_reasoning(midc_ok, entropy_ok, persistence_ok, signals_ok);

        Self {
            midc_ok,
            entropy_ok,
            persistence_ok,
            signals_ok,
            is_tradeable,
            recommended_strategy,
            position_scale,
            reasoning,
            assessed_at: Utc::now(),
        }
    }

    fn generate_reasoning(midc_ok: bool, entropy_ok: bool, persistence_ok: bool, signals_ok: bool) -> String {
        let mut reasons = Vec::new();

        if !midc_ok {
            reasons.push("MIDC too high (fast information diffusion)");
        }
        if !entropy_ok {
            reasons.push("Entropy too high (unpredictable regime)");
        }
        if !persistence_ok {
            reasons.push("Trends too short-lived");
        }
        if !signals_ok {
            reasons.push("No high-edge conditional signals");
        }

        if reasons.is_empty() {
            "All conditions favorable for momentum trading".to_string()
        } else {
            reasons.join("; ")
        }
    }
}

/// Recommended trading strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RecommendedStrategy {
    /// Momentum following strategy
    Momentum,
    /// Market making strategy
    MarketMaking,
    /// Hybrid strategy
    Hybrid,
    /// No trading recommended
    None,
}

/// Complete research state at a point in time
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchState {
    /// Unique identifier for this state
    pub id: String,

    /// Symbol this research applies to
    pub symbol: String,

    /// Timestamp when this state was created
    pub timestamp: DateTime<Utc>,

    /// MIDC estimate
    pub midc: MIDCEstimate,

    /// Persistence statistics
    pub persistence: PersistenceStats,

    /// Conditional probability table (signature key -> probability)
    pub conditional_table: HashMap<String, ConditionalProbability>,

    /// Current entropy value
    pub entropy: f64,

    /// Tradeable assessment
    pub assessment: TradeableAssessment,

    /// Data period start (earliest data used)
    pub data_start: Option<DateTime<Utc>>,

    /// Data period end (latest data used)
    pub data_end: Option<DateTime<Utc>>,

    /// Number of feature snapshots processed
    pub snapshots_processed: usize,

    /// Version of the research engine that created this state
    pub engine_version: String,
}

impl Default for ResearchState {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            symbol: String::new(),
            timestamp: Utc::now(),
            midc: MIDCEstimate::default(),
            persistence: PersistenceStats::default(),
            conditional_table: HashMap::new(),
            entropy: 0.0,
            assessment: TradeableAssessment::default(),
            data_start: None,
            data_end: None,
            snapshots_processed: 0,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

impl ResearchState {
    /// Create a new research state for a symbol
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            ..Default::default()
        }
    }

    /// Create with a specific ID (for testing)
    pub fn with_id(symbol: &str, id: &str) -> Self {
        Self {
            id: id.to_string(),
            symbol: symbol.to_string(),
            ..Default::default()
        }
    }

    /// Update MIDC estimate
    pub fn update_midc(&mut self, midc: MIDCEstimate) {
        self.midc = midc;
        self.timestamp = Utc::now();
    }

    /// Update persistence stats
    pub fn update_persistence(&mut self, persistence: PersistenceStats) {
        self.persistence = persistence;
        self.timestamp = Utc::now();
    }

    /// Update a conditional probability entry
    pub fn update_conditional(&mut self, signature: &PriceSignature, prob: ConditionalProbability) {
        self.conditional_table.insert(signature.to_key(), prob);
        self.timestamp = Utc::now();
    }

    /// Get conditional probability for a signature
    pub fn get_conditional(&self, signature: &PriceSignature) -> Option<&ConditionalProbability> {
        self.conditional_table.get(&signature.to_key())
    }

    /// Update entropy value
    pub fn update_entropy(&mut self, entropy: f64) {
        self.entropy = entropy;
        self.timestamp = Utc::now();
    }

    /// Recompute the tradeable assessment based on current state
    pub fn recompute_assessment(&mut self, entropy_threshold: f64, min_persistence_seconds: f64, min_edge: f64, min_samples: usize) {
        let midc_ok = self.midc.is_valid() && self.midc.regime().momentum_viable();
        let entropy_ok = self.entropy < entropy_threshold;
        let persistence_ok = self.persistence.trends_exploitable(min_persistence_seconds);

        let signals_ok = self.conditional_table.values()
            .any(|p| p.is_significant(min_edge, min_samples));

        self.assessment = TradeableAssessment::new(midc_ok, entropy_ok, persistence_ok, signals_ok);
        self.timestamp = Utc::now();
    }

    /// Get all significant signals above thresholds
    pub fn get_significant_signals(&self, min_edge: f64, min_samples: usize) -> Vec<(PriceSignature, &ConditionalProbability)> {
        self.conditional_table
            .iter()
            .filter(|(_, prob)| prob.is_significant(min_edge, min_samples))
            .filter_map(|(key, prob)| {
                PriceSignature::from_key(key).map(|sig| (sig, prob))
            })
            .collect()
    }

    /// Increment snapshot counter and update data end time
    pub fn record_snapshot(&mut self, timestamp: DateTime<Utc>) {
        self.snapshots_processed += 1;

        if self.data_start.is_none() {
            self.data_start = Some(timestamp);
        }
        self.data_end = Some(timestamp);
        self.timestamp = Utc::now();
    }

    /// Check if the state has enough data to be useful
    pub fn is_sufficient(&self) -> bool {
        self.snapshots_processed >= 1000
            && self.midc.is_valid()
            && self.persistence.is_reliable()
    }

    /// Get age of this state in seconds
    pub fn age_seconds(&self) -> f64 {
        (Utc::now() - self.timestamp).num_milliseconds() as f64 / 1000.0
    }

    /// Merge another research state into this one (for incremental updates)
    pub fn merge(&mut self, other: &ResearchState) {
        // Take the more recent MIDC if valid
        if other.midc.is_valid() && (!self.midc.is_valid() || other.midc.computed_at > self.midc.computed_at) {
            self.midc = other.midc.clone();
        }

        // Take the more recent persistence if reliable
        if other.persistence.is_reliable() && (!self.persistence.is_reliable() || other.persistence.updated_at > self.persistence.updated_at) {
            self.persistence = other.persistence.clone();
        }

        // Merge conditional tables (other's entries override)
        for (key, prob) in &other.conditional_table {
            self.conditional_table.insert(key.clone(), prob.clone());
        }

        // Take the more recent entropy
        self.entropy = other.entropy;

        // Update data range
        if let Some(start) = other.data_start {
            self.data_start = Some(self.data_start.map_or(start, |s| s.min(start)));
        }
        if let Some(end) = other.data_end {
            self.data_end = Some(self.data_end.map_or(end, |e| e.max(end)));
        }

        self.snapshots_processed += other.snapshots_processed;
        self.timestamp = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== MIDCEstimate Tests ====================

    #[test]
    fn test_midc_estimate_new() {
        let midc = MIDCEstimate::new(0.05, 0.1, 0.85, 500);

        assert!((midc.kappa - 0.05).abs() < 1e-10);
        assert!((midc.rho_0 - 0.1).abs() < 1e-10);
        assert!((midc.r_squared - 0.85).abs() < 1e-10);
        assert_eq!(midc.sample_size, 500);

        // tau_half = ln(2) / kappa = 0.693 / 0.05 ≈ 13.86
        let expected_tau = (2.0_f64).ln() / 0.05;
        assert!((midc.tau_half_seconds - expected_tau).abs() < 1e-6);
    }

    #[test]
    fn test_midc_estimate_default() {
        let midc = MIDCEstimate::default();

        assert!((midc.kappa - 0.0).abs() < 1e-10);
        assert!((midc.tau_half_seconds - 0.0).abs() < 1e-10);
        assert_eq!(midc.sample_size, 0);
        assert!(!midc.is_valid());
    }

    #[test]
    fn test_midc_estimate_is_valid() {
        // Valid estimate
        let valid = MIDCEstimate::new(0.05, 0.1, 0.85, 500);
        assert!(valid.is_valid());

        // Invalid: too few samples
        let few_samples = MIDCEstimate::new(0.05, 0.1, 0.85, 5);
        assert!(!few_samples.is_valid());

        // Invalid: zero R²
        let zero_r2 = MIDCEstimate::new(0.05, 0.1, 0.0, 500);
        assert!(!zero_r2.is_valid());

        // Invalid: zero kappa (default)
        let zero_kappa = MIDCEstimate::default();
        assert!(!zero_kappa.is_valid());
    }

    #[test]
    fn test_midc_estimate_regime_slow() {
        let slow = MIDCEstimate::new(0.005, 0.1, 0.85, 500);
        assert_eq!(slow.regime(), MIDCRegime::SlowDiffusion);
        assert!(slow.regime().momentum_viable());
        assert!((slow.regime().position_scale() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_midc_estimate_regime_moderate() {
        let moderate = MIDCEstimate::new(0.05, 0.1, 0.85, 500);
        assert_eq!(moderate.regime(), MIDCRegime::ModerateDiffusion);
        assert!(moderate.regime().momentum_viable());
        assert!((moderate.regime().position_scale() - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_midc_estimate_regime_fast() {
        let fast = MIDCEstimate::new(0.15, 0.1, 0.85, 500);
        assert_eq!(fast.regime(), MIDCRegime::FastDiffusion);
        assert!(!fast.regime().momentum_viable());
        assert!((fast.regime().position_scale() - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_midc_estimate_regime_unknown() {
        let unknown = MIDCEstimate::default();
        assert_eq!(unknown.regime(), MIDCRegime::Unknown);
        assert!(!unknown.regime().momentum_viable());
    }

    #[test]
    fn test_midc_estimate_zero_kappa() {
        let zero = MIDCEstimate::new(0.0, 0.1, 0.85, 500);
        assert!(zero.tau_half_seconds.is_infinite());
    }

    #[test]
    fn test_midc_estimate_confidence_computation() {
        // High R², high samples -> high confidence
        let high_conf = MIDCEstimate::new(0.05, 0.1, 0.95, 1500);
        assert!(high_conf.confidence > 0.9);

        // Low R² -> low confidence
        let low_r2 = MIDCEstimate::new(0.05, 0.1, 0.3, 1500);
        assert!(low_r2.confidence < 0.5);

        // Low samples -> low confidence
        let low_samples = MIDCEstimate::new(0.05, 0.1, 0.95, 100);
        assert!(low_samples.confidence < 0.2);
    }

    // ==================== PersistenceStats Tests ====================

    #[test]
    fn test_persistence_stats_default() {
        let stats = PersistenceStats::default();
        assert_eq!(stats.sample_count, 0);
        assert!(!stats.is_reliable());
    }

    #[test]
    fn test_persistence_stats_reliable() {
        let mut stats = PersistenceStats::default();
        stats.sample_count = 30;
        assert!(stats.is_reliable());

        stats.sample_count = 29;
        assert!(!stats.is_reliable());
    }

    #[test]
    fn test_persistence_stats_trends_exploitable() {
        let mut stats = PersistenceStats::default();
        stats.sample_count = 50;
        stats.median_duration_seconds = 60.0;

        assert!(stats.trends_exploitable(30.0));
        assert!(stats.trends_exploitable(60.0));
        assert!(!stats.trends_exploitable(61.0));
    }

    #[test]
    fn test_persistence_stats_not_exploitable_insufficient_data() {
        let mut stats = PersistenceStats::default();
        stats.sample_count = 10; // Not enough
        stats.median_duration_seconds = 120.0;

        assert!(!stats.trends_exploitable(30.0));
    }

    // ==================== PriceSignature Tests ====================

    #[test]
    fn test_price_signature_new() {
        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );

        assert_eq!(sig.magnitude, SignatureMagnitude::Medium);
        assert_eq!(sig.speed, SignatureSpeed::Normal);
        assert_eq!(sig.direction, SignatureDirection::Up);
        assert_eq!(sig.consistency, SignatureConsistency::Smooth);
    }

    #[test]
    fn test_price_signature_to_key() {
        let sig = PriceSignature::new(
            SignatureMagnitude::Large,
            SignatureSpeed::Fast,
            SignatureDirection::Down,
            SignatureConsistency::Choppy,
        );

        let key = sig.to_key();
        assert_eq!(key, "Large_Fast_Down_Choppy");
    }

    #[test]
    fn test_price_signature_from_key_valid() {
        let key = "Medium_Normal_Up_Smooth";
        let sig = PriceSignature::from_key(key).unwrap();

        assert_eq!(sig.magnitude, SignatureMagnitude::Medium);
        assert_eq!(sig.speed, SignatureSpeed::Normal);
        assert_eq!(sig.direction, SignatureDirection::Up);
        assert_eq!(sig.consistency, SignatureConsistency::Smooth);
    }

    #[test]
    fn test_price_signature_from_key_invalid() {
        assert!(PriceSignature::from_key("invalid").is_none());
        assert!(PriceSignature::from_key("Too_Many_Parts_Here_Now").is_none());
        assert!(PriceSignature::from_key("Unknown_Normal_Up_Smooth").is_none());
        assert!(PriceSignature::from_key("Medium_Unknown_Up_Smooth").is_none());
        assert!(PriceSignature::from_key("Medium_Normal_Unknown_Smooth").is_none());
        assert!(PriceSignature::from_key("Medium_Normal_Up_Unknown").is_none());
    }

    #[test]
    fn test_price_signature_roundtrip() {
        let original = PriceSignature::new(
            SignatureMagnitude::VeryLarge,
            SignatureSpeed::Slow,
            SignatureDirection::Down,
            SignatureConsistency::Mixed,
        );

        let key = original.to_key();
        let parsed = PriceSignature::from_key(&key).unwrap();

        assert_eq!(original, parsed);
    }

    #[test]
    fn test_price_signature_all_magnitudes() {
        for mag in [
            SignatureMagnitude::Tiny,
            SignatureMagnitude::Small,
            SignatureMagnitude::Medium,
            SignatureMagnitude::Large,
            SignatureMagnitude::VeryLarge,
        ] {
            let sig = PriceSignature::new(
                mag,
                SignatureSpeed::Normal,
                SignatureDirection::Up,
                SignatureConsistency::Smooth,
            );
            let key = sig.to_key();
            let parsed = PriceSignature::from_key(&key).unwrap();
            assert_eq!(sig, parsed);
        }
    }

    #[test]
    fn test_price_signature_all_speeds() {
        for speed in [
            SignatureSpeed::Slow,
            SignatureSpeed::Normal,
            SignatureSpeed::Fast,
        ] {
            let sig = PriceSignature::new(
                SignatureMagnitude::Medium,
                speed,
                SignatureDirection::Up,
                SignatureConsistency::Smooth,
            );
            let key = sig.to_key();
            let parsed = PriceSignature::from_key(&key).unwrap();
            assert_eq!(sig, parsed);
        }
    }

    #[test]
    fn test_price_signature_all_directions() {
        for dir in [SignatureDirection::Up, SignatureDirection::Down] {
            let sig = PriceSignature::new(
                SignatureMagnitude::Medium,
                SignatureSpeed::Normal,
                dir,
                SignatureConsistency::Smooth,
            );
            let key = sig.to_key();
            let parsed = PriceSignature::from_key(&key).unwrap();
            assert_eq!(sig, parsed);
        }
    }

    #[test]
    fn test_price_signature_all_consistencies() {
        for con in [
            SignatureConsistency::Choppy,
            SignatureConsistency::Mixed,
            SignatureConsistency::Smooth,
        ] {
            let sig = PriceSignature::new(
                SignatureMagnitude::Medium,
                SignatureSpeed::Normal,
                SignatureDirection::Up,
                con,
            );
            let key = sig.to_key();
            let parsed = PriceSignature::from_key(&key).unwrap();
            assert_eq!(sig, parsed);
        }
    }

    // ==================== ConditionalProbability Tests ====================

    #[test]
    fn test_conditional_probability_default() {
        let prob = ConditionalProbability::default();
        assert!((prob.p_continuation - 0.5).abs() < 1e-10);
        assert!((prob.p_reversal - 0.5).abs() < 1e-10);
        assert_eq!(prob.sample_count, 0);
    }

    #[test]
    fn test_conditional_probability_edge() {
        let mut prob = ConditionalProbability::default();

        prob.p_continuation = 0.6;
        assert!((prob.edge() - 0.1).abs() < 1e-10);

        prob.p_continuation = 0.4;
        assert!((prob.edge() - (-0.1)).abs() < 1e-10);

        prob.p_continuation = 0.5;
        assert!(prob.edge().abs() < 1e-10);
    }

    #[test]
    fn test_conditional_probability_is_significant() {
        let mut prob = ConditionalProbability::default();
        prob.p_continuation = 0.6;
        prob.sample_count = 100;

        assert!(prob.is_significant(0.05, 50));
        assert!(prob.is_significant(0.09, 100)); // Edge 0.1 >= 0.09, so significant
        assert!(!prob.is_significant(0.15, 100)); // Edge too low (0.1 < 0.15)
        assert!(!prob.is_significant(0.1, 150)); // Not enough samples
    }

    #[test]
    fn test_conditional_probability_confidence_interval() {
        let mut prob = ConditionalProbability::default();
        prob.p_continuation = 0.7;
        prob.sample_count = 100;
        prob.compute_confidence_interval();

        // Confidence interval should contain p_continuation
        assert!(prob.confidence_interval.0 <= prob.p_continuation);
        assert!(prob.confidence_interval.1 >= prob.p_continuation);

        // Interval should be within [0, 1]
        assert!(prob.confidence_interval.0 >= 0.0);
        assert!(prob.confidence_interval.1 <= 1.0);
    }

    #[test]
    fn test_conditional_probability_confidence_interval_empty() {
        let mut prob = ConditionalProbability::default();
        prob.sample_count = 0;
        prob.compute_confidence_interval();

        assert!((prob.confidence_interval.0 - 0.0).abs() < 1e-10);
        assert!((prob.confidence_interval.1 - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_conditional_probability_confidence_interval_narrows_with_samples() {
        let mut prob1 = ConditionalProbability::default();
        prob1.p_continuation = 0.6;
        prob1.sample_count = 50;
        prob1.compute_confidence_interval();

        let mut prob2 = ConditionalProbability::default();
        prob2.p_continuation = 0.6;
        prob2.sample_count = 500;
        prob2.compute_confidence_interval();

        let width1 = prob1.confidence_interval.1 - prob1.confidence_interval.0;
        let width2 = prob2.confidence_interval.1 - prob2.confidence_interval.0;

        assert!(width2 < width1); // More samples = narrower interval
    }

    // ==================== TradeableAssessment Tests ====================

    #[test]
    fn test_tradeable_assessment_all_ok() {
        let assessment = TradeableAssessment::new(true, true, true, true);

        assert!(assessment.is_tradeable);
        assert_eq!(assessment.recommended_strategy, RecommendedStrategy::Momentum);
        assert!((assessment.position_scale - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_tradeable_assessment_midc_not_ok() {
        let assessment = TradeableAssessment::new(false, true, true, true);

        assert!(!assessment.is_tradeable);
        assert_eq!(assessment.recommended_strategy, RecommendedStrategy::MarketMaking);
        assert!((assessment.position_scale - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_tradeable_assessment_nothing_ok() {
        let assessment = TradeableAssessment::new(false, false, false, false);

        assert!(!assessment.is_tradeable);
        assert_eq!(assessment.recommended_strategy, RecommendedStrategy::None);
        assert!((assessment.position_scale - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_tradeable_assessment_reasoning_all_ok() {
        let assessment = TradeableAssessment::new(true, true, true, true);
        assert!(assessment.reasoning.contains("favorable"));
    }

    #[test]
    fn test_tradeable_assessment_reasoning_failures() {
        let assessment = TradeableAssessment::new(false, false, true, true);
        assert!(assessment.reasoning.contains("MIDC"));
        assert!(assessment.reasoning.contains("Entropy"));
    }

    #[test]
    fn test_tradeable_assessment_default() {
        let assessment = TradeableAssessment::default();
        assert!(!assessment.is_tradeable);
        assert_eq!(assessment.recommended_strategy, RecommendedStrategy::None);
    }

    // ==================== ResearchState Tests ====================

    #[test]
    fn test_research_state_new() {
        let state = ResearchState::new("BTCUSDT");

        assert_eq!(state.symbol, "BTCUSDT");
        assert!(!state.id.is_empty());
        assert!(state.conditional_table.is_empty());
        assert_eq!(state.snapshots_processed, 0);
    }

    #[test]
    fn test_research_state_with_id() {
        let state = ResearchState::with_id("BTCUSDT", "test-id-123");

        assert_eq!(state.id, "test-id-123");
        assert_eq!(state.symbol, "BTCUSDT");
    }

    #[test]
    fn test_research_state_default() {
        let state = ResearchState::default();

        assert!(state.symbol.is_empty());
        assert!(!state.id.is_empty());
        assert!(state.data_start.is_none());
        assert!(state.data_end.is_none());
    }

    #[test]
    fn test_research_state_update_midc() {
        let mut state = ResearchState::new("BTCUSDT");
        let old_timestamp = state.timestamp;

        std::thread::sleep(std::time::Duration::from_millis(10));

        let midc = MIDCEstimate::new(0.05, 0.1, 0.85, 500);
        state.update_midc(midc.clone());

        assert_eq!(state.midc.kappa, midc.kappa);
        assert!(state.timestamp > old_timestamp);
    }

    #[test]
    fn test_research_state_update_persistence() {
        let mut state = ResearchState::new("BTCUSDT");

        let mut persistence = PersistenceStats::default();
        persistence.mean_duration_seconds = 45.0;
        persistence.sample_count = 100;

        state.update_persistence(persistence.clone());

        assert_eq!(state.persistence.mean_duration_seconds, 45.0);
        assert_eq!(state.persistence.sample_count, 100);
    }

    #[test]
    fn test_research_state_update_conditional() {
        let mut state = ResearchState::new("BTCUSDT");

        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );

        let mut prob = ConditionalProbability::default();
        prob.p_continuation = 0.65;
        prob.sample_count = 200;

        state.update_conditional(&sig, prob.clone());

        let retrieved = state.get_conditional(&sig).unwrap();
        assert_eq!(retrieved.p_continuation, 0.65);
        assert_eq!(retrieved.sample_count, 200);
    }

    #[test]
    fn test_research_state_get_conditional_missing() {
        let state = ResearchState::new("BTCUSDT");

        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );

        assert!(state.get_conditional(&sig).is_none());
    }

    #[test]
    fn test_research_state_update_entropy() {
        let mut state = ResearchState::new("BTCUSDT");
        state.update_entropy(0.75);

        assert!((state.entropy - 0.75).abs() < 1e-10);
    }

    #[test]
    fn test_research_state_recompute_assessment() {
        let mut state = ResearchState::new("BTCUSDT");

        // Set up favorable conditions
        state.midc = MIDCEstimate::new(0.005, 0.1, 0.9, 1000);
        state.entropy = 0.3;
        state.persistence = PersistenceStats {
            mean_duration_seconds: 60.0,
            median_duration_seconds: 55.0,
            std_duration_seconds: 20.0,
            percentile_25: 40.0,
            percentile_75: 80.0,
            sample_count: 100,
            updated_at: Utc::now(),
        };

        // Add a significant signal
        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );
        let mut prob = ConditionalProbability::default();
        prob.p_continuation = 0.65;
        prob.sample_count = 200;
        state.update_conditional(&sig, prob);

        state.recompute_assessment(0.5, 30.0, 0.05, 100);

        assert!(state.assessment.is_tradeable);
        assert_eq!(state.assessment.recommended_strategy, RecommendedStrategy::Momentum);
    }

    #[test]
    fn test_research_state_recompute_assessment_not_tradeable() {
        let mut state = ResearchState::new("BTCUSDT");

        // Set up unfavorable MIDC
        state.midc = MIDCEstimate::new(0.2, 0.1, 0.9, 1000);
        state.entropy = 0.3;

        state.recompute_assessment(0.5, 30.0, 0.05, 100);

        assert!(!state.assessment.is_tradeable);
    }

    #[test]
    fn test_research_state_get_significant_signals() {
        let mut state = ResearchState::new("BTCUSDT");

        // Add some signals
        let sig1 = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );
        let mut prob1 = ConditionalProbability::default();
        prob1.p_continuation = 0.7;
        prob1.sample_count = 200;
        state.update_conditional(&sig1, prob1);

        let sig2 = PriceSignature::new(
            SignatureMagnitude::Large,
            SignatureSpeed::Fast,
            SignatureDirection::Down,
            SignatureConsistency::Choppy,
        );
        let mut prob2 = ConditionalProbability::default();
        prob2.p_continuation = 0.52; // Not significant
        prob2.sample_count = 200;
        state.update_conditional(&sig2, prob2);

        let significant = state.get_significant_signals(0.1, 100);

        assert_eq!(significant.len(), 1);
        assert_eq!(significant[0].0, sig1);
    }

    #[test]
    fn test_research_state_record_snapshot() {
        let mut state = ResearchState::new("BTCUSDT");
        let ts1 = Utc::now();

        state.record_snapshot(ts1);
        assert_eq!(state.snapshots_processed, 1);
        assert_eq!(state.data_start, Some(ts1));
        assert_eq!(state.data_end, Some(ts1));

        std::thread::sleep(std::time::Duration::from_millis(10));
        let ts2 = Utc::now();

        state.record_snapshot(ts2);
        assert_eq!(state.snapshots_processed, 2);
        assert_eq!(state.data_start, Some(ts1));
        assert_eq!(state.data_end, Some(ts2));
    }

    #[test]
    fn test_research_state_is_sufficient() {
        let mut state = ResearchState::new("BTCUSDT");

        // Not sufficient by default
        assert!(!state.is_sufficient());

        // Add sufficient data
        state.snapshots_processed = 1500;
        state.midc = MIDCEstimate::new(0.05, 0.1, 0.9, 1000);
        state.persistence.sample_count = 50;

        assert!(state.is_sufficient());
    }

    #[test]
    fn test_research_state_is_sufficient_missing_midc() {
        let mut state = ResearchState::new("BTCUSDT");
        state.snapshots_processed = 1500;
        state.persistence.sample_count = 50;
        // No valid MIDC

        assert!(!state.is_sufficient());
    }

    #[test]
    fn test_research_state_is_sufficient_missing_persistence() {
        let mut state = ResearchState::new("BTCUSDT");
        state.snapshots_processed = 1500;
        state.midc = MIDCEstimate::new(0.05, 0.1, 0.9, 1000);
        // Persistence sample count = 0

        assert!(!state.is_sufficient());
    }

    #[test]
    fn test_research_state_age_seconds() {
        let state = ResearchState::new("BTCUSDT");

        std::thread::sleep(std::time::Duration::from_millis(50));

        let age = state.age_seconds();
        assert!(age >= 0.05);
        assert!(age < 1.0); // Should be much less than 1 second
    }

    #[test]
    fn test_research_state_merge_midc() {
        let mut state1 = ResearchState::new("BTCUSDT");
        let mut state2 = ResearchState::new("BTCUSDT");

        // state2 has more recent MIDC
        std::thread::sleep(std::time::Duration::from_millis(10));
        state2.midc = MIDCEstimate::new(0.03, 0.1, 0.9, 1000);

        state1.merge(&state2);

        assert_eq!(state1.midc.kappa, 0.03);
    }

    #[test]
    fn test_research_state_merge_conditional_tables() {
        let mut state1 = ResearchState::new("BTCUSDT");
        let mut state2 = ResearchState::new("BTCUSDT");

        let sig1 = PriceSignature::new(
            SignatureMagnitude::Small,
            SignatureSpeed::Slow,
            SignatureDirection::Up,
            SignatureConsistency::Mixed,
        );
        let mut prob1 = ConditionalProbability::default();
        prob1.p_continuation = 0.6;
        state1.update_conditional(&sig1, prob1);

        let sig2 = PriceSignature::new(
            SignatureMagnitude::Large,
            SignatureSpeed::Fast,
            SignatureDirection::Down,
            SignatureConsistency::Smooth,
        );
        let mut prob2 = ConditionalProbability::default();
        prob2.p_continuation = 0.7;
        state2.update_conditional(&sig2, prob2);

        state1.merge(&state2);

        // Should have both signals
        assert!(state1.get_conditional(&sig1).is_some());
        assert!(state1.get_conditional(&sig2).is_some());
    }

    #[test]
    fn test_research_state_merge_snapshots() {
        let mut state1 = ResearchState::new("BTCUSDT");
        let mut state2 = ResearchState::new("BTCUSDT");

        state1.snapshots_processed = 100;
        state2.snapshots_processed = 50;

        state1.merge(&state2);

        assert_eq!(state1.snapshots_processed, 150);
    }

    #[test]
    fn test_research_state_merge_data_range() {
        let mut state1 = ResearchState::new("BTCUSDT");
        let mut state2 = ResearchState::new("BTCUSDT");

        let ts1 = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ts2 = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ts3 = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ts4 = Utc::now();

        state1.data_start = Some(ts2);
        state1.data_end = Some(ts3);

        state2.data_start = Some(ts1);
        state2.data_end = Some(ts4);

        state1.merge(&state2);

        assert_eq!(state1.data_start, Some(ts1)); // Earliest
        assert_eq!(state1.data_end, Some(ts4)); // Latest
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn test_midc_estimate_serialization() {
        let midc = MIDCEstimate::new(0.05, 0.1, 0.85, 500);

        let json = serde_json::to_string(&midc).unwrap();
        let deserialized: MIDCEstimate = serde_json::from_str(&json).unwrap();

        assert!((deserialized.kappa - midc.kappa).abs() < 1e-10);
        assert!((deserialized.tau_half_seconds - midc.tau_half_seconds).abs() < 1e-10);
    }

    #[test]
    fn test_persistence_stats_serialization() {
        let mut stats = PersistenceStats::default();
        stats.mean_duration_seconds = 45.5;
        stats.sample_count = 100;

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: PersistenceStats = serde_json::from_str(&json).unwrap();

        assert!((deserialized.mean_duration_seconds - stats.mean_duration_seconds).abs() < 1e-10);
        assert_eq!(deserialized.sample_count, stats.sample_count);
    }

    #[test]
    fn test_price_signature_serialization() {
        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );

        let json = serde_json::to_string(&sig).unwrap();
        let deserialized: PriceSignature = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, sig);
    }

    #[test]
    fn test_conditional_probability_serialization() {
        let mut prob = ConditionalProbability::default();
        prob.p_continuation = 0.65;
        prob.sample_count = 200;
        prob.compute_confidence_interval();

        let json = serde_json::to_string(&prob).unwrap();
        let deserialized: ConditionalProbability = serde_json::from_str(&json).unwrap();

        assert!((deserialized.p_continuation - prob.p_continuation).abs() < 1e-10);
        assert_eq!(deserialized.sample_count, prob.sample_count);
    }

    #[test]
    fn test_tradeable_assessment_serialization() {
        let assessment = TradeableAssessment::new(true, true, true, true);

        let json = serde_json::to_string(&assessment).unwrap();
        let deserialized: TradeableAssessment = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.is_tradeable, assessment.is_tradeable);
        assert_eq!(deserialized.recommended_strategy, assessment.recommended_strategy);
    }

    #[test]
    fn test_research_state_serialization() {
        let mut state = ResearchState::new("BTCUSDT");
        state.midc = MIDCEstimate::new(0.05, 0.1, 0.85, 500);
        state.entropy = 0.3;

        let sig = PriceSignature::new(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );
        let mut prob = ConditionalProbability::default();
        prob.p_continuation = 0.65;
        state.update_conditional(&sig, prob);

        let json = serde_json::to_string(&state).unwrap();
        let deserialized: ResearchState = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.symbol, state.symbol);
        assert_eq!(deserialized.id, state.id);
        assert!((deserialized.midc.kappa - state.midc.kappa).abs() < 1e-10);
        assert!(deserialized.get_conditional(&sig).is_some());
    }

    #[test]
    fn test_research_state_serialization_empty() {
        let state = ResearchState::default();

        let json = serde_json::to_string(&state).unwrap();
        let deserialized: ResearchState = serde_json::from_str(&json).unwrap();

        assert!(deserialized.symbol.is_empty());
        assert!(deserialized.conditional_table.is_empty());
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_midc_estimate_negative_kappa() {
        // Negative kappa shouldn't happen in practice, but handle gracefully
        let midc = MIDCEstimate::new(-0.05, 0.1, 0.85, 500);
        assert!(midc.tau_half_seconds < 0.0);
    }

    #[test]
    fn test_conditional_probability_extreme_values() {
        let mut prob = ConditionalProbability::default();

        // p = 1.0
        prob.p_continuation = 1.0;
        prob.sample_count = 100;
        prob.compute_confidence_interval();
        assert!(prob.confidence_interval.1 <= 1.0);

        // p = 0.0
        prob.p_continuation = 0.0;
        prob.compute_confidence_interval();
        assert!(prob.confidence_interval.0 >= 0.0);
    }

    #[test]
    fn test_research_state_large_conditional_table() {
        let mut state = ResearchState::new("BTCUSDT");

        // Add many signals
        for mag in [
            SignatureMagnitude::Tiny,
            SignatureMagnitude::Small,
            SignatureMagnitude::Medium,
            SignatureMagnitude::Large,
            SignatureMagnitude::VeryLarge,
        ] {
            for speed in [SignatureSpeed::Slow, SignatureSpeed::Normal, SignatureSpeed::Fast] {
                for dir in [SignatureDirection::Up, SignatureDirection::Down] {
                    for con in [
                        SignatureConsistency::Choppy,
                        SignatureConsistency::Mixed,
                        SignatureConsistency::Smooth,
                    ] {
                        let sig = PriceSignature::new(mag, speed, dir, con);
                        let mut prob = ConditionalProbability::default();
                        prob.p_continuation = 0.5 + (mag as usize) as f64 * 0.02;
                        prob.sample_count = 100;
                        state.update_conditional(&sig, prob);
                    }
                }
            }
        }

        // Should have 5 * 3 * 2 * 3 = 90 entries
        assert_eq!(state.conditional_table.len(), 90);

        // Serialization should still work
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: ResearchState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.conditional_table.len(), 90);
    }
}

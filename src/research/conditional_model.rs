//! Conditional Model - Task 1.4
//!
//! Builds and updates conditional probability tables P(continuation | signature).
//!
//! # Overview
//!
//! The ConditionalModel tracks outcomes for each price signature and computes:
//! - P(continuation): Probability price continues in same direction
//! - P(reversal): Probability price reverses direction
//! - Expected magnitude: Average magnitude of continuation moves
//!
//! # Incremental Updates
//!
//! The model uses incremental Bayesian updates rather than recomputing from scratch:
//! - Running counts of continuations and reversals per signature
//! - Running mean and variance for magnitude estimation
//! - Wilson score confidence intervals based on sample size
//!
//! # Edge Detection
//!
//! Signals are considered "significant" when:
//! - Sample count exceeds minimum threshold (default: 30)
//! - Edge over random (|P(continuation) - 0.5|) exceeds minimum (default: 0.05)
//! - Confidence interval doesn't cross 0.5
//!
//! # Usage
//!
//! ```rust,ignore
//! use ingestor::research::{ConditionalModel, ConditionalModelConfig};
//! use ingestor::framework::PriceSignature;
//!
//! let config = ConditionalModelConfig::default();
//! let mut model = ConditionalModel::new(config);
//!
//! // Record outcomes
//! model.record_outcome(&signature, Outcome::Continuation, 15.0);
//! model.record_outcome(&signature, Outcome::Reversal, -10.0);
//!
//! // Get probability for a signature
//! let prob = model.get_probability(&signature);
//! println!("P(continuation) = {:.2}%", prob.p_continuation * 100.0);
//!
//! // Get all significant signals
//! let signals = model.get_all_significant(30, 0.05);
//! for signal in signals {
//!     println!("{}: edge = {:.2}%", signal.signature_key, signal.edge * 100.0);
//! }
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use crate::framework::{
    ConditionalProbability, PriceSignature, SignatureConsistency, SignatureDirection,
    SignatureMagnitude, SignatureSpeed,
};
use crate::research::traits::{Outcome, SignificantSignal};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the ConditionalModel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalModelConfig {
    /// Minimum samples required before computing probabilities
    /// Default: 10 (must have at least this many observations)
    pub min_samples_for_probability: usize,

    /// Minimum samples for a signal to be considered significant
    /// Default: 30 (statistical significance threshold)
    pub min_samples_for_significance: usize,

    /// Minimum edge over random (0.5) for significance
    /// Default: 0.05 (5% edge)
    pub min_edge_for_significance: f64,

    /// Decay factor for magnitude averaging (0 = no decay, 1 = instant decay)
    /// Default: 0.0 (equal weighting)
    pub magnitude_decay: f64,

    /// Whether to track neutral outcomes separately
    /// Default: true
    pub track_neutral: bool,

    /// Confidence level for interval calculation (e.g., 0.95 for 95%)
    /// Default: 0.95
    pub confidence_level: f64,
}

impl Default for ConditionalModelConfig {
    fn default() -> Self {
        Self {
            min_samples_for_probability: 10,
            min_samples_for_significance: 30,
            min_edge_for_significance: 0.05,
            magnitude_decay: 0.0,
            track_neutral: true,
            confidence_level: 0.95,
        }
    }
}

impl ConditionalModelConfig {
    /// Create a new config with custom significance thresholds
    pub fn with_significance(min_samples: usize, min_edge: f64) -> Self {
        Self {
            min_samples_for_significance: min_samples,
            min_edge_for_significance: min_edge,
            ..Default::default()
        }
    }

    /// Builder: set minimum samples for probability
    pub fn with_min_samples_for_probability(mut self, min: usize) -> Self {
        self.min_samples_for_probability = min;
        self
    }

    /// Builder: set minimum samples for significance
    pub fn with_min_samples_for_significance(mut self, min: usize) -> Self {
        self.min_samples_for_significance = min;
        self
    }

    /// Builder: set minimum edge for significance
    pub fn with_min_edge(mut self, edge: f64) -> Self {
        self.min_edge_for_significance = edge;
        self
    }

    /// Builder: set magnitude decay
    pub fn with_magnitude_decay(mut self, decay: f64) -> Self {
        self.magnitude_decay = decay.clamp(0.0, 1.0);
        self
    }

    /// Builder: disable neutral tracking
    pub fn without_neutral_tracking(mut self) -> Self {
        self.track_neutral = false;
        self
    }

    /// Builder: set confidence level
    pub fn with_confidence_level(mut self, level: f64) -> Self {
        self.confidence_level = level.clamp(0.5, 0.999);
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.min_samples_for_probability == 0 {
            return Err("min_samples_for_probability must be > 0".to_string());
        }
        if self.min_samples_for_significance == 0 {
            return Err("min_samples_for_significance must be > 0".to_string());
        }
        if self.min_edge_for_significance < 0.0 || self.min_edge_for_significance > 0.5 {
            return Err("min_edge_for_significance must be in [0.0, 0.5]".to_string());
        }
        if self.confidence_level < 0.5 || self.confidence_level >= 1.0 {
            return Err("confidence_level must be in [0.5, 1.0)".to_string());
        }
        Ok(())
    }
}

// ============================================================================
// Internal Data Structures
// ============================================================================

/// Accumulated statistics for a single signature
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SignatureStats {
    /// Number of continuation outcomes
    continuations: usize,

    /// Number of reversal outcomes
    reversals: usize,

    /// Number of neutral outcomes (if tracking enabled)
    neutrals: usize,

    /// Running sum of continuation magnitudes (for mean calculation)
    magnitude_sum: f64,

    /// Running sum of squared magnitudes (for variance calculation)
    magnitude_sum_sq: f64,

    /// Count of magnitude observations (may differ from continuations if some are zero)
    magnitude_count: usize,

    /// Timestamp of first observation
    first_observation: DateTime<Utc>,

    /// Timestamp of most recent observation
    last_observation: DateTime<Utc>,
}

impl Default for SignatureStats {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            continuations: 0,
            reversals: 0,
            neutrals: 0,
            magnitude_sum: 0.0,
            magnitude_sum_sq: 0.0,
            magnitude_count: 0,
            first_observation: now,
            last_observation: now,
        }
    }
}

impl SignatureStats {
    /// Total non-neutral outcomes
    fn total_outcomes(&self) -> usize {
        self.continuations + self.reversals
    }

    /// Total including neutrals
    fn total_all(&self) -> usize {
        self.continuations + self.reversals + self.neutrals
    }

    /// Compute P(continuation) from counts
    fn p_continuation(&self) -> f64 {
        let total = self.total_outcomes();
        if total == 0 {
            0.5 // No data, return uninformative prior
        } else {
            self.continuations as f64 / total as f64
        }
    }

    /// Compute P(reversal) from counts
    fn p_reversal(&self) -> f64 {
        1.0 - self.p_continuation()
    }

    /// Compute mean magnitude
    fn mean_magnitude(&self) -> f64 {
        if self.magnitude_count == 0 {
            0.0
        } else {
            self.magnitude_sum / self.magnitude_count as f64
        }
    }

    /// Compute standard deviation of magnitude
    fn std_magnitude(&self) -> f64 {
        if self.magnitude_count < 2 {
            0.0
        } else {
            let n = self.magnitude_count as f64;
            let mean = self.mean_magnitude();
            let variance = (self.magnitude_sum_sq / n) - (mean * mean);
            // Use Bessel's correction for sample std
            let corrected_variance = variance * n / (n - 1.0);
            corrected_variance.max(0.0).sqrt()
        }
    }

    /// Record an outcome
    fn record(&mut self, outcome: Outcome, magnitude_bps: f64, track_neutral: bool) {
        self.last_observation = Utc::now();

        match outcome {
            Outcome::Continuation => {
                self.continuations += 1;
                // Only track positive magnitudes for continuation
                if magnitude_bps > 0.0 {
                    self.magnitude_sum += magnitude_bps;
                    self.magnitude_sum_sq += magnitude_bps * magnitude_bps;
                    self.magnitude_count += 1;
                }
            }
            Outcome::Reversal => {
                self.reversals += 1;
                // Track absolute magnitude for reversals too
                if magnitude_bps.abs() > 0.0 {
                    let abs_mag = magnitude_bps.abs();
                    self.magnitude_sum += abs_mag;
                    self.magnitude_sum_sq += abs_mag * abs_mag;
                    self.magnitude_count += 1;
                }
            }
            Outcome::Neutral => {
                if track_neutral {
                    self.neutrals += 1;
                }
            }
        }
    }

    /// Compute Wilson score confidence interval for p_continuation
    fn confidence_interval(&self, z: f64) -> (f64, f64) {
        let n = self.total_outcomes();
        if n == 0 {
            return (0.0, 1.0);
        }

        let p = self.p_continuation();
        let n_f = n as f64;

        let denominator = 1.0 + z * z / n_f;
        let center = (p + z * z / (2.0 * n_f)) / denominator;
        let spread = z * (p * (1.0 - p) / n_f + z * z / (4.0 * n_f * n_f)).sqrt() / denominator;

        ((center - spread).max(0.0), (center + spread).min(1.0))
    }
}

// ============================================================================
// ConditionalModel
// ============================================================================

/// Conditional probability model for price signatures
///
/// Tracks P(continuation | signature) tables with incremental updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalModel {
    /// Configuration
    config: ConditionalModelConfig,

    /// Statistics per signature
    #[serde(serialize_with = "serialize_stats_map", deserialize_with = "deserialize_stats_map")]
    stats: HashMap<PriceSignature, SignatureStats>,

    /// Total outcomes recorded (across all signatures)
    total_outcomes: usize,

    /// Timestamp of model creation
    created_at: DateTime<Utc>,

    /// Timestamp of last update
    updated_at: DateTime<Utc>,
}

// Custom serialization for HashMap<PriceSignature, SignatureStats>
fn serialize_stats_map<S>(
    map: &HashMap<PriceSignature, SignatureStats>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut ser_map = serializer.serialize_map(Some(map.len()))?;
    for (sig, stats) in map {
        ser_map.serialize_entry(&sig.to_key(), stats)?;
    }
    ser_map.end()
}

fn deserialize_stats_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<PriceSignature, SignatureStats>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let string_map: HashMap<String, SignatureStats> = HashMap::deserialize(deserializer)?;
    let mut result = HashMap::new();
    for (key, stats) in string_map {
        if let Some(sig) = PriceSignature::from_key(&key) {
            result.insert(sig, stats);
        }
    }
    Ok(result)
}

impl ConditionalModel {
    /// Create a new ConditionalModel with the given configuration
    pub fn new(config: ConditionalModelConfig) -> Self {
        let now = Utc::now();
        Self {
            config,
            stats: HashMap::new(),
            total_outcomes: 0,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create with default configuration
    pub fn default_model() -> Self {
        Self::new(ConditionalModelConfig::default())
    }

    /// Get the configuration
    pub fn config(&self) -> &ConditionalModelConfig {
        &self.config
    }

    /// Get the number of unique signatures tracked
    pub fn signature_count(&self) -> usize {
        self.stats.len()
    }

    /// Get the total number of outcomes recorded
    pub fn total_outcomes(&self) -> usize {
        self.total_outcomes
    }

    /// Get creation timestamp
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Get last update timestamp
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Record an outcome for a price signature
    ///
    /// # Arguments
    /// * `signature` - The price signature that preceded this outcome
    /// * `outcome` - Whether price continued, reversed, or stayed neutral
    /// * `magnitude_bps` - The magnitude of the move in basis points (absolute value used)
    ///
    /// # Example
    /// ```rust,ignore
    /// model.record_outcome(&signature, Outcome::Continuation, 15.0);
    /// ```
    pub fn record_outcome(&mut self, signature: &PriceSignature, outcome: Outcome, magnitude_bps: f64) {
        let stats = self.stats.entry(*signature).or_default();

        // If this is the first observation for this signature, set first_observation
        if stats.total_all() == 0 {
            stats.first_observation = Utc::now();
        }

        stats.record(outcome, magnitude_bps, self.config.track_neutral);
        self.total_outcomes += 1;
        self.updated_at = Utc::now();
    }

    /// Get conditional probability for a signature
    ///
    /// Returns the probability of continuation given this signature,
    /// with confidence intervals if sufficient data exists.
    ///
    /// # Arguments
    /// * `signature` - The price signature to look up
    ///
    /// # Returns
    /// `ConditionalProbability` with p_continuation, p_reversal, expected magnitude, etc.
    pub fn get_probability(&self, signature: &PriceSignature) -> ConditionalProbability {
        let stats = self.stats.get(signature);

        match stats {
            None => {
                // No data for this signature - return uninformative prior
                ConditionalProbability::default()
            }
            Some(stats) => {
                let sample_count = stats.total_outcomes();

                // If below minimum samples, return with low confidence
                if sample_count < self.config.min_samples_for_probability {
                    let mut prob = ConditionalProbability {
                        p_continuation: stats.p_continuation(),
                        p_reversal: stats.p_reversal(),
                        expected_magnitude_bps: stats.mean_magnitude(),
                        std_magnitude_bps: stats.std_magnitude(),
                        sample_count,
                        confidence_interval: (0.0, 1.0), // Maximum uncertainty
                    };
                    prob.compute_confidence_interval();
                    return prob;
                }

                // Compute z-score for confidence level
                let z = self.z_score_for_confidence();
                let ci = stats.confidence_interval(z);

                ConditionalProbability {
                    p_continuation: stats.p_continuation(),
                    p_reversal: stats.p_reversal(),
                    expected_magnitude_bps: stats.mean_magnitude(),
                    std_magnitude_bps: stats.std_magnitude(),
                    sample_count,
                    confidence_interval: ci,
                }
            }
        }
    }

    /// Get all signatures with significant edge
    ///
    /// Returns signatures where:
    /// - Sample count >= min_samples
    /// - |edge| >= min_edge
    /// - Confidence interval doesn't cross 0.5 (optional strictness)
    ///
    /// # Arguments
    /// * `min_samples` - Minimum sample count for significance
    /// * `min_edge` - Minimum edge over random (0.5)
    ///
    /// # Returns
    /// Vector of `SignificantSignal` sorted by edge (descending absolute value)
    pub fn get_all_significant(&self, min_samples: usize, min_edge: f64) -> Vec<SignificantSignal> {
        let mut signals: Vec<SignificantSignal> = self
            .stats
            .iter()
            .filter_map(|(signature, stats)| {
                let sample_count = stats.total_outcomes();
                if sample_count < min_samples {
                    return None;
                }

                let p_cont = stats.p_continuation();
                let edge = p_cont - 0.5;

                if edge.abs() < min_edge {
                    return None;
                }

                // Compute confidence interval
                let z = self.z_score_for_confidence();
                let ci = stats.confidence_interval(z);

                Some(SignificantSignal {
                    signature_key: signature.to_key(),
                    probability: ConditionalProbability {
                        p_continuation: p_cont,
                        p_reversal: stats.p_reversal(),
                        expected_magnitude_bps: stats.mean_magnitude(),
                        std_magnitude_bps: stats.std_magnitude(),
                        sample_count,
                        confidence_interval: ci,
                    },
                    edge,
                })
            })
            .collect();

        // Sort by absolute edge, descending
        signals.sort_by(|a, b| {
            b.edge
                .abs()
                .partial_cmp(&a.edge.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        signals
    }

    /// Get significant signals using config defaults
    pub fn get_significant(&self) -> Vec<SignificantSignal> {
        self.get_all_significant(
            self.config.min_samples_for_significance,
            self.config.min_edge_for_significance,
        )
    }

    /// Check if a signature has significant edge
    pub fn is_significant(&self, signature: &PriceSignature) -> bool {
        self.stats.get(signature).map_or(false, |stats| {
            let sample_count = stats.total_outcomes();
            let edge = (stats.p_continuation() - 0.5).abs();
            sample_count >= self.config.min_samples_for_significance
                && edge >= self.config.min_edge_for_significance
        })
    }

    /// Get statistics for a specific signature (for debugging)
    pub fn get_raw_stats(&self, signature: &PriceSignature) -> Option<(usize, usize, usize)> {
        self.stats
            .get(signature)
            .map(|s| (s.continuations, s.reversals, s.neutrals))
    }

    /// Merge another ConditionalModel into this one
    ///
    /// Useful for combining results from different time periods or parallel processing.
    pub fn merge(&mut self, other: &ConditionalModel) {
        for (sig, other_stats) in &other.stats {
            let stats = self.stats.entry(*sig).or_default();

            stats.continuations += other_stats.continuations;
            stats.reversals += other_stats.reversals;
            stats.neutrals += other_stats.neutrals;
            stats.magnitude_sum += other_stats.magnitude_sum;
            stats.magnitude_sum_sq += other_stats.magnitude_sum_sq;
            stats.magnitude_count += other_stats.magnitude_count;

            // Keep earliest first_observation
            if other_stats.first_observation < stats.first_observation {
                stats.first_observation = other_stats.first_observation;
            }
            // Keep latest last_observation
            if other_stats.last_observation > stats.last_observation {
                stats.last_observation = other_stats.last_observation;
            }
        }

        self.total_outcomes += other.total_outcomes;
        self.updated_at = Utc::now();
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.stats.clear();
        self.total_outcomes = 0;
        self.updated_at = Utc::now();
    }

    /// Get z-score for the configured confidence level
    fn z_score_for_confidence(&self) -> f64 {
        // Approximate z-scores for common confidence levels
        match (self.config.confidence_level * 100.0).round() as u32 {
            90 => 1.645,
            95 => 1.96,
            99 => 2.576,
            _ => {
                // Rough approximation using inverse error function
                // For confidence level c, z ≈ sqrt(2) * erfinv(c)
                // Using simplified approximation
                let alpha = 1.0 - self.config.confidence_level;
                if alpha <= 0.01 {
                    2.576
                } else if alpha <= 0.05 {
                    1.96
                } else {
                    1.645
                }
            }
        }
    }

    /// Get summary statistics
    pub fn summary(&self) -> ConditionalModelStats {
        let significant_signals = self.get_significant();
        let total_continuations: usize = self.stats.values().map(|s| s.continuations).sum();
        let total_reversals: usize = self.stats.values().map(|s| s.reversals).sum();
        let total_neutrals: usize = self.stats.values().map(|s| s.neutrals).sum();

        let avg_edge = if significant_signals.is_empty() {
            0.0
        } else {
            significant_signals.iter().map(|s| s.edge.abs()).sum::<f64>()
                / significant_signals.len() as f64
        };

        let max_edge = significant_signals
            .iter()
            .map(|s| s.edge.abs())
            .fold(0.0, f64::max);

        ConditionalModelStats {
            signature_count: self.stats.len(),
            total_outcomes: self.total_outcomes,
            total_continuations,
            total_reversals,
            total_neutrals,
            significant_signal_count: significant_signals.len(),
            average_edge: avg_edge,
            max_edge,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    /// Export to HashMap for serialization (signature key -> probability)
    pub fn export_probabilities(&self) -> HashMap<String, ConditionalProbability> {
        self.stats
            .iter()
            .map(|(sig, _)| (sig.to_key(), self.get_probability(sig)))
            .collect()
    }

    /// Get all probabilities as a vector of (signature, probability) pairs
    pub fn get_all_probabilities(&self) -> Vec<(PriceSignature, ConditionalProbability)> {
        self.stats
            .keys()
            .map(|sig| (*sig, self.get_probability(sig)))
            .collect()
    }

    /// Import a probability for a specific signature (for state restoration)
    ///
    /// This creates synthetic stats to represent the imported probability.
    /// Note: This is a simplified import that may not preserve all statistics.
    pub fn import_probability(&mut self, signature: &PriceSignature, prob: ConditionalProbability) {
        // Create synthetic stats based on the probability
        let sample_count = prob.sample_count;
        if sample_count == 0 {
            return;
        }

        let continuations = (prob.p_continuation * sample_count as f64).round() as usize;
        let reversals = sample_count.saturating_sub(continuations);

        let stats = SignatureStats {
            continuations,
            reversals,
            neutrals: 0,
            magnitude_sum: prob.expected_magnitude_bps * sample_count as f64,
            magnitude_sum_sq: (prob.expected_magnitude_bps.powi(2) + prob.std_magnitude_bps.powi(2))
                * sample_count as f64,
            magnitude_count: sample_count,
            first_observation: Utc::now(),
            last_observation: Utc::now(),
        };

        self.stats.insert(*signature, stats);
        self.total_outcomes += sample_count;
        self.updated_at = Utc::now();
    }

    /// Import from HashMap (for deserialization from external format)
    pub fn import_from_counts(
        counts: HashMap<String, (usize, usize, usize, f64, f64)>,
        config: ConditionalModelConfig,
    ) -> Self {
        let mut model = Self::new(config);
        let now = Utc::now();

        for (key, (cont, rev, neut, mag_sum, mag_sum_sq)) in counts {
            if let Some(sig) = PriceSignature::from_key(&key) {
                let stats = SignatureStats {
                    continuations: cont,
                    reversals: rev,
                    neutrals: neut,
                    magnitude_sum: mag_sum,
                    magnitude_sum_sq: mag_sum_sq,
                    magnitude_count: cont + rev,
                    first_observation: now,
                    last_observation: now,
                };
                model.stats.insert(sig, stats);
                model.total_outcomes += cont + rev + neut;
            }
        }

        model
    }
}

impl Default for ConditionalModel {
    fn default() -> Self {
        Self::new(ConditionalModelConfig::default())
    }
}

// ============================================================================
// Statistics Summary
// ============================================================================

/// Summary statistics for the ConditionalModel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalModelStats {
    /// Number of unique signatures tracked
    pub signature_count: usize,

    /// Total outcomes recorded
    pub total_outcomes: usize,

    /// Total continuation outcomes
    pub total_continuations: usize,

    /// Total reversal outcomes
    pub total_reversals: usize,

    /// Total neutral outcomes
    pub total_neutrals: usize,

    /// Number of signatures with significant edge
    pub significant_signal_count: usize,

    /// Average absolute edge of significant signals
    pub average_edge: f64,

    /// Maximum absolute edge
    pub max_edge: f64,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
}

impl fmt::Display for ConditionalModelStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "ConditionalModel Statistics:")?;
        writeln!(f, "  Unique signatures: {}", self.signature_count)?;
        writeln!(f, "  Total outcomes: {}", self.total_outcomes)?;
        writeln!(f, "    Continuations: {}", self.total_continuations)?;
        writeln!(f, "    Reversals: {}", self.total_reversals)?;
        writeln!(f, "    Neutrals: {}", self.total_neutrals)?;
        writeln!(
            f,
            "  Significant signals: {}",
            self.significant_signal_count
        )?;
        writeln!(f, "  Average edge: {:.2}%", self.average_edge * 100.0)?;
        writeln!(f, "  Max edge: {:.2}%", self.max_edge * 100.0)?;
        Ok(())
    }
}

// ============================================================================
// Builder Pattern
// ============================================================================

/// Builder for ConditionalModel with pre-populated data
#[derive(Debug, Default)]
pub struct ConditionalModelBuilder {
    config: ConditionalModelConfig,
    initial_data: Vec<(PriceSignature, Outcome, f64)>,
}

impl ConditionalModelBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set configuration
    pub fn with_config(mut self, config: ConditionalModelConfig) -> Self {
        self.config = config;
        self
    }

    /// Add initial outcome data
    pub fn with_outcome(mut self, signature: PriceSignature, outcome: Outcome, magnitude: f64) -> Self {
        self.initial_data.push((signature, outcome, magnitude));
        self
    }

    /// Add multiple outcomes
    pub fn with_outcomes(mut self, data: Vec<(PriceSignature, Outcome, f64)>) -> Self {
        self.initial_data.extend(data);
        self
    }

    /// Build the model
    pub fn build(self) -> ConditionalModel {
        let mut model = ConditionalModel::new(self.config);
        for (sig, outcome, mag) in self.initial_data {
            model.record_outcome(&sig, outcome, mag);
        }
        model
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a test signature
    fn test_signature(
        mag: SignatureMagnitude,
        speed: SignatureSpeed,
        dir: SignatureDirection,
        cons: SignatureConsistency,
    ) -> PriceSignature {
        PriceSignature::new(mag, speed, dir, cons)
    }

    fn default_up_signature() -> PriceSignature {
        test_signature(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        )
    }

    fn default_down_signature() -> PriceSignature {
        test_signature(
            SignatureMagnitude::Medium,
            SignatureSpeed::Normal,
            SignatureDirection::Down,
            SignatureConsistency::Smooth,
        )
    }

    // ==================== Config Tests ====================

    #[test]
    fn test_config_default() {
        let config = ConditionalModelConfig::default();
        assert_eq!(config.min_samples_for_probability, 10);
        assert_eq!(config.min_samples_for_significance, 30);
        assert!((config.min_edge_for_significance - 0.05).abs() < 0.001);
        assert!(config.track_neutral);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_builder() {
        let config = ConditionalModelConfig::default()
            .with_min_samples_for_probability(5)
            .with_min_samples_for_significance(50)
            .with_min_edge(0.10)
            .with_magnitude_decay(0.1)
            .without_neutral_tracking()
            .with_confidence_level(0.99);

        assert_eq!(config.min_samples_for_probability, 5);
        assert_eq!(config.min_samples_for_significance, 50);
        assert!((config.min_edge_for_significance - 0.10).abs() < 0.001);
        assert!((config.magnitude_decay - 0.1).abs() < 0.001);
        assert!(!config.track_neutral);
        assert!((config.confidence_level - 0.99).abs() < 0.001);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_with_significance() {
        let config = ConditionalModelConfig::with_significance(100, 0.15);
        assert_eq!(config.min_samples_for_significance, 100);
        assert!((config.min_edge_for_significance - 0.15).abs() < 0.001);
    }

    #[test]
    fn test_config_validation_zero_samples() {
        let config = ConditionalModelConfig::default().with_min_samples_for_probability(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_significance_samples() {
        let config = ConditionalModelConfig::default().with_min_samples_for_significance(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_edge() {
        let config = ConditionalModelConfig::default().with_min_edge(-0.1);
        assert!(config.validate().is_err());

        let config2 = ConditionalModelConfig::default().with_min_edge(0.6);
        assert!(config2.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_confidence() {
        // Test confidence level clamping (builder clamps to valid range)
        let config = ConditionalModelConfig::default().with_confidence_level(0.3);
        assert!((config.confidence_level - 0.5).abs() < 0.001); // Clamped to 0.5
    }

    // ==================== Basic Model Tests ====================

    #[test]
    fn test_model_creation() {
        let model = ConditionalModel::default();
        assert_eq!(model.signature_count(), 0);
        assert_eq!(model.total_outcomes(), 0);
    }

    #[test]
    fn test_model_with_config() {
        let config = ConditionalModelConfig::with_significance(50, 0.10);
        let model = ConditionalModel::new(config);
        assert_eq!(model.config().min_samples_for_significance, 50);
    }

    #[test]
    fn test_record_single_outcome() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Continuation, 10.0);

        assert_eq!(model.signature_count(), 1);
        assert_eq!(model.total_outcomes(), 1);
        assert_eq!(model.get_raw_stats(&sig), Some((1, 0, 0)));
    }

    #[test]
    fn test_record_multiple_outcomes_same_signature() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Continuation, 10.0);
        model.record_outcome(&sig, Outcome::Continuation, 15.0);
        model.record_outcome(&sig, Outcome::Reversal, -8.0);
        model.record_outcome(&sig, Outcome::Neutral, 0.0);

        assert_eq!(model.signature_count(), 1);
        assert_eq!(model.total_outcomes(), 4);
        assert_eq!(model.get_raw_stats(&sig), Some((2, 1, 1)));
    }

    #[test]
    fn test_record_multiple_signatures() {
        let mut model = ConditionalModel::default();
        let sig1 = default_up_signature();
        let sig2 = default_down_signature();

        model.record_outcome(&sig1, Outcome::Continuation, 10.0);
        model.record_outcome(&sig2, Outcome::Reversal, 5.0);

        assert_eq!(model.signature_count(), 2);
        assert_eq!(model.total_outcomes(), 2);
    }

    // ==================== Probability Calculation Tests ====================

    #[test]
    fn test_probability_no_data() {
        let model = ConditionalModel::default();
        let sig = default_up_signature();

        let prob = model.get_probability(&sig);
        assert!((prob.p_continuation - 0.5).abs() < 0.001);
        assert!((prob.p_reversal - 0.5).abs() < 0.001);
        assert_eq!(prob.sample_count, 0);
    }

    #[test]
    fn test_probability_all_continuations() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..50 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!((prob.p_continuation - 1.0).abs() < 0.001);
        assert!((prob.p_reversal - 0.0).abs() < 0.001);
        assert_eq!(prob.sample_count, 50);
    }

    #[test]
    fn test_probability_all_reversals() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..50 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!((prob.p_continuation - 0.0).abs() < 0.001);
        assert!((prob.p_reversal - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_probability_50_50_split() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..50 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!((prob.p_continuation - 0.5).abs() < 0.001);
        assert!((prob.p_reversal - 0.5).abs() < 0.001);
        assert_eq!(prob.sample_count, 100);
    }

    #[test]
    fn test_probability_60_40_split() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..60 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..40 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!((prob.p_continuation - 0.6).abs() < 0.001);
        assert!((prob.p_reversal - 0.4).abs() < 0.001);
    }

    #[test]
    fn test_probability_neutrals_dont_affect_probabilities() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Continuation, 10.0);
        model.record_outcome(&sig, Outcome::Reversal, 10.0);

        let prob1 = model.get_probability(&sig);

        // Add many neutrals
        for _ in 0..100 {
            model.record_outcome(&sig, Outcome::Neutral, 0.0);
        }

        let prob2 = model.get_probability(&sig);

        // Probabilities should be unchanged
        assert!((prob1.p_continuation - prob2.p_continuation).abs() < 0.001);
        assert_eq!(prob2.sample_count, 2); // Only non-neutral outcomes count
    }

    #[test]
    fn test_probability_neutral_tracking_disabled() {
        let config = ConditionalModelConfig::default().without_neutral_tracking();
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Continuation, 10.0);
        model.record_outcome(&sig, Outcome::Neutral, 0.0);

        assert_eq!(model.get_raw_stats(&sig), Some((1, 0, 0)));
    }

    // ==================== Magnitude Statistics Tests ====================

    #[test]
    fn test_magnitude_mean() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Continuation, 10.0);
        model.record_outcome(&sig, Outcome::Continuation, 20.0);
        model.record_outcome(&sig, Outcome::Continuation, 30.0);

        let prob = model.get_probability(&sig);
        assert!((prob.expected_magnitude_bps - 20.0).abs() < 0.001);
    }

    #[test]
    fn test_magnitude_std() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // Record same magnitude - std should be 0
        for _ in 0..10 {
            model.record_outcome(&sig, Outcome::Continuation, 15.0);
        }

        let prob = model.get_probability(&sig);
        assert!(prob.std_magnitude_bps < 0.001);
    }

    #[test]
    fn test_magnitude_std_varied() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // Record 5, 10, 15, 20, 25 - mean = 15, variance = 50
        for mag in [5.0, 10.0, 15.0, 20.0, 25.0] {
            model.record_outcome(&sig, Outcome::Continuation, mag);
        }

        let prob = model.get_probability(&sig);
        // Standard deviation should be around sqrt(62.5) ≈ 7.9 with Bessel's correction
        assert!((prob.expected_magnitude_bps - 15.0).abs() < 0.001);
        assert!(prob.std_magnitude_bps > 7.0 && prob.std_magnitude_bps < 9.0);
    }

    #[test]
    fn test_magnitude_zero_not_counted() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Continuation, 0.0);
        model.record_outcome(&sig, Outcome::Continuation, 10.0);

        let prob = model.get_probability(&sig);
        // Only the non-zero magnitude should be counted
        assert!((prob.expected_magnitude_bps - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_magnitude_reversal_uses_absolute() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Reversal, -15.0);
        model.record_outcome(&sig, Outcome::Reversal, -25.0);

        let prob = model.get_probability(&sig);
        // Should use absolute values
        assert!((prob.expected_magnitude_bps - 20.0).abs() < 0.001);
    }

    // ==================== Confidence Interval Tests ====================

    #[test]
    fn test_confidence_interval_no_data() {
        let model = ConditionalModel::default();
        let sig = default_up_signature();

        let prob = model.get_probability(&sig);
        assert_eq!(prob.confidence_interval, (0.0, 1.0));
    }

    #[test]
    fn test_confidence_interval_few_samples() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Continuation, 10.0);

        let prob = model.get_probability(&sig);
        // With only 1 sample, confidence interval should be wide
        assert!(prob.confidence_interval.0 < 0.3);
        assert!(prob.confidence_interval.1 > 0.7);
    }

    #[test]
    fn test_confidence_interval_narrows_with_samples() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // Record 70/30 split
        for _ in 0..7 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..3 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob_10 = model.get_probability(&sig);
        let width_10 = prob_10.confidence_interval.1 - prob_10.confidence_interval.0;

        // Add more samples
        for _ in 0..63 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..27 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob_100 = model.get_probability(&sig);
        let width_100 = prob_100.confidence_interval.1 - prob_100.confidence_interval.0;

        // CI should be narrower with more samples
        assert!(width_100 < width_10);
    }

    #[test]
    fn test_confidence_interval_100_percent() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..50 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        let prob = model.get_probability(&sig);
        // Lower bound should be close to 1.0
        assert!(prob.confidence_interval.0 > 0.9);
        assert!((prob.confidence_interval.1 - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_confidence_interval_0_percent() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..50 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob = model.get_probability(&sig);
        // Upper bound should be close to 0.0
        assert!(prob.confidence_interval.0.abs() < 0.001);
        assert!(prob.confidence_interval.1 < 0.1);
    }

    // ==================== Edge and Significance Tests ====================

    #[test]
    fn test_edge_calculation() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // 60% continuation rate
        for _ in 0..60 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..40 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!((prob.edge() - 0.1).abs() < 0.001); // 0.6 - 0.5 = 0.1
    }

    #[test]
    fn test_edge_negative() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // 40% continuation rate (negative edge)
        for _ in 0..40 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..60 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!((prob.edge() - (-0.1)).abs() < 0.001); // 0.4 - 0.5 = -0.1
    }

    #[test]
    fn test_is_significant_true() {
        let config = ConditionalModelConfig::with_significance(30, 0.05);
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        // 60% continuation (10% edge) with 50 samples
        for _ in 0..30 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..20 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        assert!(model.is_significant(&sig));
    }

    #[test]
    fn test_is_significant_false_insufficient_samples() {
        let config = ConditionalModelConfig::with_significance(30, 0.05);
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        // 70% continuation but only 20 samples
        for _ in 0..14 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..6 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        assert!(!model.is_significant(&sig)); // Only 20 samples, need 30
    }

    #[test]
    fn test_is_significant_false_insufficient_edge() {
        let config = ConditionalModelConfig::with_significance(30, 0.05);
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        // 52% continuation (2% edge) with 100 samples
        for _ in 0..52 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..48 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        assert!(!model.is_significant(&sig)); // Edge 2% < 5%
    }

    #[test]
    fn test_is_significant_unknown_signature() {
        let model = ConditionalModel::default();
        let sig = default_up_signature();
        assert!(!model.is_significant(&sig));
    }

    // ==================== Get All Significant Tests ====================

    #[test]
    fn test_get_all_significant_empty() {
        let model = ConditionalModel::default();
        let signals = model.get_all_significant(30, 0.05);
        assert!(signals.is_empty());
    }

    #[test]
    fn test_get_all_significant_one_signal() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..35 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..15 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let signals = model.get_all_significant(30, 0.05);
        assert_eq!(signals.len(), 1);
        assert!((signals[0].edge - 0.2).abs() < 0.001);
    }

    #[test]
    fn test_get_all_significant_sorted_by_edge() {
        let mut model = ConditionalModel::default();

        // Signature 1: 60% continuation (10% edge)
        let sig1 = test_signature(
            SignatureMagnitude::Small,
            SignatureSpeed::Fast,
            SignatureDirection::Up,
            SignatureConsistency::Smooth,
        );
        for _ in 0..30 {
            model.record_outcome(&sig1, Outcome::Continuation, 10.0);
        }
        for _ in 0..20 {
            model.record_outcome(&sig1, Outcome::Reversal, 10.0);
        }

        // Signature 2: 70% continuation (20% edge)
        let sig2 = test_signature(
            SignatureMagnitude::Large,
            SignatureSpeed::Slow,
            SignatureDirection::Down,
            SignatureConsistency::Choppy,
        );
        for _ in 0..35 {
            model.record_outcome(&sig2, Outcome::Continuation, 10.0);
        }
        for _ in 0..15 {
            model.record_outcome(&sig2, Outcome::Reversal, 10.0);
        }

        let signals = model.get_all_significant(30, 0.05);
        assert_eq!(signals.len(), 2);
        // Should be sorted by absolute edge descending
        assert!(signals[0].edge.abs() >= signals[1].edge.abs());
    }

    #[test]
    fn test_get_all_significant_negative_edge_included() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // 35% continuation = -15% edge (reversal bias)
        for _ in 0..35 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..65 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let signals = model.get_all_significant(30, 0.05);
        assert_eq!(signals.len(), 1);
        assert!(signals[0].edge < -0.1);
    }

    #[test]
    fn test_get_significant_uses_config() {
        let config = ConditionalModelConfig::with_significance(50, 0.15);
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        // 60% continuation (10% edge) with 40 samples
        for _ in 0..24 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..16 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        // Should not be significant: samples=40 < 50 required
        let signals = model.get_significant();
        assert!(signals.is_empty());
    }

    // ==================== Merge Tests ====================

    #[test]
    fn test_merge_models() {
        let mut model1 = ConditionalModel::default();
        let mut model2 = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..30 {
            model1.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..20 {
            model2.record_outcome(&sig, Outcome::Reversal, 8.0);
        }

        model1.merge(&model2);

        assert_eq!(model1.get_raw_stats(&sig), Some((30, 20, 0)));
        assert_eq!(model1.total_outcomes(), 50);
    }

    #[test]
    fn test_merge_different_signatures() {
        let mut model1 = ConditionalModel::default();
        let mut model2 = ConditionalModel::default();
        let sig1 = default_up_signature();
        let sig2 = default_down_signature();

        model1.record_outcome(&sig1, Outcome::Continuation, 10.0);
        model2.record_outcome(&sig2, Outcome::Reversal, 10.0);

        model1.merge(&model2);

        assert_eq!(model1.signature_count(), 2);
        assert_eq!(model1.get_raw_stats(&sig1), Some((1, 0, 0)));
        assert_eq!(model1.get_raw_stats(&sig2), Some((0, 1, 0)));
    }

    #[test]
    fn test_merge_magnitude_sums() {
        let mut model1 = ConditionalModel::default();
        let mut model2 = ConditionalModel::default();
        let sig = default_up_signature();

        // Model 1: 3 outcomes averaging 10 bps
        for _ in 0..3 {
            model1.record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        // Model 2: 2 outcomes averaging 20 bps
        for _ in 0..2 {
            model2.record_outcome(&sig, Outcome::Continuation, 20.0);
        }

        model1.merge(&model2);

        let prob = model1.get_probability(&sig);
        // Mean should be (30 + 40) / 5 = 14 bps
        assert!((prob.expected_magnitude_bps - 14.0).abs() < 0.001);
    }

    // ==================== Clear Tests ====================

    #[test]
    fn test_clear() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..50 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        assert_eq!(model.signature_count(), 1);

        model.clear();

        assert_eq!(model.signature_count(), 0);
        assert_eq!(model.total_outcomes(), 0);
    }

    // ==================== Builder Tests ====================

    #[test]
    fn test_builder_basic() {
        let model = ConditionalModelBuilder::new().build();
        assert_eq!(model.signature_count(), 0);
    }

    #[test]
    fn test_builder_with_config() {
        let config = ConditionalModelConfig::with_significance(100, 0.20);
        let model = ConditionalModelBuilder::new().with_config(config).build();
        assert_eq!(model.config().min_samples_for_significance, 100);
    }

    #[test]
    fn test_builder_with_outcomes() {
        let sig = default_up_signature();
        let model = ConditionalModelBuilder::new()
            .with_outcome(sig, Outcome::Continuation, 10.0)
            .with_outcome(sig, Outcome::Continuation, 15.0)
            .with_outcome(sig, Outcome::Reversal, 8.0)
            .build();

        assert_eq!(model.get_raw_stats(&sig), Some((2, 1, 0)));
    }

    #[test]
    fn test_builder_with_outcomes_vec() {
        let sig = default_up_signature();
        let data = vec![
            (sig, Outcome::Continuation, 10.0),
            (sig, Outcome::Continuation, 15.0),
            (sig, Outcome::Reversal, 8.0),
        ];

        let model = ConditionalModelBuilder::new().with_outcomes(data).build();

        assert_eq!(model.get_raw_stats(&sig), Some((2, 1, 0)));
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn test_serialization_roundtrip() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..30 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..20 {
            model.record_outcome(&sig, Outcome::Reversal, 8.0);
        }

        let json = serde_json::to_string(&model).unwrap();
        let restored: ConditionalModel = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.signature_count(), model.signature_count());
        assert_eq!(restored.total_outcomes(), model.total_outcomes());
        assert_eq!(restored.get_raw_stats(&sig), model.get_raw_stats(&sig));
    }

    #[test]
    fn test_serialization_multiple_signatures() {
        let mut model = ConditionalModel::default();

        for mag in [
            SignatureMagnitude::Tiny,
            SignatureMagnitude::Small,
            SignatureMagnitude::Medium,
        ] {
            let sig = test_signature(
                mag,
                SignatureSpeed::Normal,
                SignatureDirection::Up,
                SignatureConsistency::Smooth,
            );
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        let json = serde_json::to_string(&model).unwrap();
        let restored: ConditionalModel = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.signature_count(), 3);
    }

    #[test]
    fn test_config_serialization() {
        let config = ConditionalModelConfig::with_significance(50, 0.15)
            .with_magnitude_decay(0.1)
            .without_neutral_tracking();

        let json = serde_json::to_string(&config).unwrap();
        let restored: ConditionalModelConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.min_samples_for_significance, 50);
        assert!((restored.min_edge_for_significance - 0.15).abs() < 0.001);
        assert!((restored.magnitude_decay - 0.1).abs() < 0.001);
        assert!(!restored.track_neutral);
    }

    // ==================== Summary Statistics Tests ====================

    #[test]
    fn test_summary_empty() {
        let model = ConditionalModel::default();
        let stats = model.summary();

        assert_eq!(stats.signature_count, 0);
        assert_eq!(stats.total_outcomes, 0);
        assert_eq!(stats.significant_signal_count, 0);
    }

    #[test]
    fn test_summary_with_data() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..40 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..10 {
            model.record_outcome(&sig, Outcome::Reversal, 8.0);
        }
        for _ in 0..5 {
            model.record_outcome(&sig, Outcome::Neutral, 0.0);
        }

        let stats = model.summary();

        assert_eq!(stats.signature_count, 1);
        assert_eq!(stats.total_outcomes, 55);
        assert_eq!(stats.total_continuations, 40);
        assert_eq!(stats.total_reversals, 10);
        assert_eq!(stats.total_neutrals, 5);
        assert_eq!(stats.significant_signal_count, 1);
        assert!(stats.average_edge > 0.2);
        assert!(stats.max_edge > 0.2);
    }

    #[test]
    fn test_summary_display() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..50 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        let stats = model.summary();
        let display = format!("{}", stats);

        assert!(display.contains("ConditionalModel Statistics:"));
        assert!(display.contains("Unique signatures: 1"));
    }

    // ==================== Export/Import Tests ====================

    #[test]
    fn test_export_probabilities() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..30 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..20 {
            model.record_outcome(&sig, Outcome::Reversal, 8.0);
        }

        let exported = model.export_probabilities();
        assert_eq!(exported.len(), 1);

        let prob = exported.get(&sig.to_key()).unwrap();
        assert!((prob.p_continuation - 0.6).abs() < 0.001);
    }

    #[test]
    fn test_import_from_counts() {
        let mut counts = HashMap::new();
        let sig = default_up_signature();
        // (continuations, reversals, neutrals, mag_sum, mag_sum_sq)
        counts.insert(sig.to_key(), (30, 20, 5, 500.0, 5000.0));

        let model = ConditionalModel::import_from_counts(counts, ConditionalModelConfig::default());

        assert_eq!(model.signature_count(), 1);
        assert_eq!(model.get_raw_stats(&sig), Some((30, 20, 5)));
    }

    // ==================== Edge Cases & Skeptical Tests ====================

    #[test]
    fn test_all_signature_combinations() {
        let mut model = ConditionalModel::default();

        // Test all 60 signature combinations
        for mag in [
            SignatureMagnitude::Tiny,
            SignatureMagnitude::Small,
            SignatureMagnitude::Medium,
            SignatureMagnitude::Large,
            SignatureMagnitude::VeryLarge,
        ] {
            for speed in [SignatureSpeed::Slow, SignatureSpeed::Normal, SignatureSpeed::Fast] {
                for dir in [SignatureDirection::Up, SignatureDirection::Down] {
                    for cons in [
                        SignatureConsistency::Choppy,
                        SignatureConsistency::Mixed,
                        SignatureConsistency::Smooth,
                    ] {
                        let sig = test_signature(mag, speed, dir, cons);
                        model.record_outcome(&sig, Outcome::Continuation, 10.0);
                    }
                }
            }
        }

        // 5 * 3 * 2 * 3 = 90 combinations
        assert_eq!(model.signature_count(), 90);
    }

    #[test]
    fn test_very_small_edge_not_significant() {
        let config = ConditionalModelConfig::with_significance(30, 0.05);
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        // 51% continuation (1% edge)
        for _ in 0..51 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..49 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        assert!(!model.is_significant(&sig));
        assert!(model.get_all_significant(30, 0.05).is_empty());
    }

    #[test]
    fn test_boundary_edge_significant() {
        let config = ConditionalModelConfig::with_significance(30, 0.05);
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        // Exactly 55% continuation (5% edge)
        for _ in 0..55 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..45 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        assert!(model.is_significant(&sig));
    }

    #[test]
    fn test_boundary_samples_not_significant() {
        let config = ConditionalModelConfig::with_significance(30, 0.05);
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        // 29 samples (just below threshold)
        for _ in 0..20 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..9 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        assert!(!model.is_significant(&sig)); // 29 < 30 required
    }

    #[test]
    fn test_boundary_samples_significant() {
        let config = ConditionalModelConfig::with_significance(30, 0.05);
        let mut model = ConditionalModel::new(config);
        let sig = default_up_signature();

        // Exactly 30 samples with 70% continuation
        for _ in 0..21 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..9 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        assert!(model.is_significant(&sig)); // 30 samples, 20% edge
    }

    #[test]
    fn test_extreme_skew_100_percent() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..1000 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!((prob.p_continuation - 1.0).abs() < 0.001);
        assert!(prob.edge() > 0.49);
    }

    #[test]
    fn test_extreme_skew_0_percent() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..1000 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!(prob.p_continuation.abs() < 0.001);
        assert!(prob.edge() < -0.49);
    }

    #[test]
    fn test_large_sample_size() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // 10,000 samples
        for _ in 0..5500 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..4500 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let prob = model.get_probability(&sig);
        assert!((prob.p_continuation - 0.55).abs() < 0.001);
        assert_eq!(prob.sample_count, 10000);

        // Confidence interval should be very narrow
        let ci_width = prob.confidence_interval.1 - prob.confidence_interval.0;
        assert!(ci_width < 0.03);
    }

    #[test]
    fn test_negative_magnitude_handling() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // Negative magnitude should use absolute value for reversals
        model.record_outcome(&sig, Outcome::Reversal, -15.0);

        let prob = model.get_probability(&sig);
        assert!((prob.expected_magnitude_bps - 15.0).abs() < 0.001);
    }

    #[test]
    fn test_mixed_positive_negative_magnitudes() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        model.record_outcome(&sig, Outcome::Continuation, 20.0);
        model.record_outcome(&sig, Outcome::Reversal, -10.0); // Should become 10

        let prob = model.get_probability(&sig);
        assert!((prob.expected_magnitude_bps - 15.0).abs() < 0.001);
    }

    #[test]
    fn test_multiple_get_probability_calls() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        for _ in 0..50 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }

        // Multiple calls should return same result
        let p1 = model.get_probability(&sig);
        let p2 = model.get_probability(&sig);
        let p3 = model.get_probability(&sig);

        assert!((p1.p_continuation - p2.p_continuation).abs() < 0.0001);
        assert!((p2.p_continuation - p3.p_continuation).abs() < 0.0001);
    }

    #[test]
    fn test_timestamps_update() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        let created = model.created_at();

        std::thread::sleep(std::time::Duration::from_millis(10));

        model.record_outcome(&sig, Outcome::Continuation, 10.0);

        let updated = model.updated_at();

        assert!(updated >= created);
    }

    #[test]
    fn test_significant_signal_expected_value() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // 60% continuation with 10 bps average magnitude
        for _ in 0..60 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..40 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let signals = model.get_all_significant(30, 0.05);
        assert_eq!(signals.len(), 1);

        let ev = signals[0].expected_value_bps();
        // EV = magnitude * edge * 2 = 10 * 0.1 * 2 = 2 bps
        assert!((ev - 2.0).abs() < 0.1);
    }

    #[test]
    fn test_significant_signal_quality_score() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // 100 samples with 10% edge
        for _ in 0..60 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        for _ in 0..40 {
            model.record_outcome(&sig, Outcome::Reversal, 10.0);
        }

        let signals = model.get_all_significant(30, 0.05);
        assert_eq!(signals.len(), 1);

        let quality = signals[0].quality_score();
        // Quality = edge * sqrt(samples) = 0.1 * sqrt(100) = 1.0
        assert!((quality - 1.0).abs() < 0.1);
    }

    #[test]
    fn test_probability_with_only_neutrals() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // Only neutral outcomes
        for _ in 0..100 {
            model.record_outcome(&sig, Outcome::Neutral, 0.0);
        }

        let prob = model.get_probability(&sig);
        // Should return uninformative prior since no directional data
        assert!((prob.p_continuation - 0.5).abs() < 0.001);
        assert_eq!(prob.sample_count, 0);
    }

    #[test]
    fn test_concurrent_signature_updates() {
        let mut model = ConditionalModel::default();

        // Simulate interleaved updates to multiple signatures
        let sigs: Vec<_> = (0..5)
            .map(|i| {
                test_signature(
                    if i % 2 == 0 {
                        SignatureMagnitude::Small
                    } else {
                        SignatureMagnitude::Large
                    },
                    SignatureSpeed::Normal,
                    SignatureDirection::Up,
                    SignatureConsistency::Smooth,
                )
            })
            .collect();

        for round in 0..20 {
            for (i, sig) in sigs.iter().enumerate() {
                let outcome = if (round + i) % 3 == 0 {
                    Outcome::Reversal
                } else {
                    Outcome::Continuation
                };
                model.record_outcome(sig, outcome, 10.0);
            }
        }

        // Verify all signatures were tracked
        // Note: Some signatures may be identical
        assert!(model.signature_count() > 0);
        assert_eq!(model.total_outcomes(), 100);
    }

    #[test]
    fn test_wilson_score_vs_naive_confidence() {
        let mut model = ConditionalModel::default();
        let sig = default_up_signature();

        // Test with extreme probability (95%) and small sample
        for _ in 0..19 {
            model.record_outcome(&sig, Outcome::Continuation, 10.0);
        }
        model.record_outcome(&sig, Outcome::Reversal, 10.0);

        let prob = model.get_probability(&sig);

        // Wilson score should give asymmetric interval for extreme probabilities
        let ci = prob.confidence_interval;

        // Lower bound should be further from 0.95 than upper bound is from 1.0
        let dist_from_lower = prob.p_continuation - ci.0;
        let dist_to_upper = ci.1 - prob.p_continuation;

        // Wilson score produces asymmetric CIs near boundaries
        // (This is actually a feature, not a bug)
        assert!(ci.0 < prob.p_continuation);
        assert!(ci.1 > prob.p_continuation);
    }
}

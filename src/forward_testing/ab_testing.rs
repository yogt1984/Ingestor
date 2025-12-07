//! A/B Testing Framework for Forward Testing
//!
//! Enables comparing multiple market making strategies simultaneously
//! with proper statistical analysis and traffic splitting.
//!
//! # Features
//!
//! - Run multiple algorithm variants on the same live data
//! - Statistical comparison with significance testing
//! - Automatic winner detection
//! - Support for multi-armed bandit exploration
//! - Detailed performance breakdown by variant

use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::algorithms::AlgorithmType;
use crate::presets::ParameterPreset;

use super::statistical::{
    BootstrapCI, EffectCategory, HypothesisTestResult, RollingStats, SampleStats,
    StatisticalTester, TwoSampleComparison,
};

/// Configuration for A/B test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABTestConfig {
    /// Name of the test
    pub test_name: String,
    /// Minimum number of trades before analysis
    pub min_trades_per_variant: usize,
    /// Minimum hours before declaring winner
    pub min_hours: f64,
    /// Significance level (alpha)
    pub alpha: f64,
    /// Minimum practical effect size to care about (Cohen's d)
    pub min_effect_size: f64,
    /// Number of bootstrap samples for CI
    pub n_bootstrap: usize,
    /// Primary metric for comparison
    pub primary_metric: ComparisonMetric,
    /// Whether to use sequential analysis (early stopping)
    pub use_sequential_analysis: bool,
    /// Spending function alpha for sequential analysis
    pub sequential_alpha_spent: f64,
}

impl Default for ABTestConfig {
    fn default() -> Self {
        Self {
            test_name: "AB_Test".to_string(),
            min_trades_per_variant: 30,
            min_hours: 1.0,
            alpha: 0.05,
            min_effect_size: 0.3, // Small-medium effect
            n_bootstrap: 10_000,
            primary_metric: ComparisonMetric::NetPnl,
            use_sequential_analysis: true,
            sequential_alpha_spent: 0.0,
        }
    }
}

/// Metric to use for comparison
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ComparisonMetric {
    NetPnl,
    SharpeRatio,
    WinRate,
    ProfitFactor,
    MaxDrawdown,
}

/// A variant in the A/B test
#[derive(Debug, Clone)]
pub struct ABVariant {
    /// Variant name
    pub name: String,
    /// Preset configuration
    pub preset: ParameterPreset,
    /// Rolling PnL per trade
    pnl_per_trade: RollingStats,
    /// Trade outcomes (1 = win, 0 = loss)
    trade_outcomes: Vec<f64>,
    /// Cumulative metrics
    pub metrics: VariantMetrics,
    /// Start time
    start_time: Option<Instant>,
}

/// Metrics tracked for each variant
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VariantMetrics {
    /// Total trades
    pub total_trades: usize,
    /// Winning trades
    pub winning_trades: usize,
    /// Losing trades
    pub losing_trades: usize,
    /// Total PnL
    pub total_pnl: f64,
    /// Peak PnL (for drawdown)
    pub peak_pnl: f64,
    /// Max drawdown
    pub max_drawdown: f64,
    /// Sum of squared returns (for Sharpe)
    pub sum_sq_returns: f64,
    /// Duration in seconds
    pub duration_secs: f64,
    /// Current drawdown
    pub current_drawdown: f64,
    /// Gross profit
    pub gross_profit: f64,
    /// Gross loss
    pub gross_loss: f64,
    /// All individual trade PnLs
    pub trade_pnls: Vec<f64>,
}

impl VariantMetrics {
    /// Calculate Sharpe ratio
    pub fn sharpe_ratio(&self) -> f64 {
        if self.total_trades < 2 || self.trade_pnls.is_empty() {
            return 0.0;
        }

        let stats = SampleStats::from_slice(&self.trade_pnls);
        if stats.std_dev == 0.0 {
            return 0.0;
        }

        // Annualized Sharpe (assuming ~1000 trades/year for HFT)
        stats.mean / stats.std_dev * (252.0_f64).sqrt()
    }

    /// Calculate win rate
    pub fn win_rate(&self) -> f64 {
        let total = self.winning_trades + self.losing_trades;
        if total == 0 {
            return 0.0;
        }
        self.winning_trades as f64 / total as f64
    }

    /// Calculate profit factor
    pub fn profit_factor(&self) -> f64 {
        if self.gross_loss == 0.0 {
            return if self.gross_profit > 0.0 {
                f64::INFINITY
            } else {
                0.0
            };
        }
        self.gross_profit / self.gross_loss
    }
}

impl ABVariant {
    /// Create a new variant from a preset
    pub fn new(name: &str, preset: ParameterPreset) -> Self {
        Self {
            name: name.to_string(),
            preset,
            pnl_per_trade: RollingStats::new(1000),
            trade_outcomes: Vec::new(),
            metrics: VariantMetrics::default(),
            start_time: None,
        }
    }

    /// Start tracking this variant
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Record a trade for this variant
    pub fn record_trade(&mut self, pnl: f64) {
        self.metrics.total_trades += 1;
        self.metrics.total_pnl += pnl;
        self.metrics.trade_pnls.push(pnl);

        // Rolling stats
        self.pnl_per_trade.push(pnl);

        // Win/loss
        if pnl > 0.0 {
            self.metrics.winning_trades += 1;
            self.metrics.gross_profit += pnl;
            self.trade_outcomes.push(1.0);
        } else if pnl < 0.0 {
            self.metrics.losing_trades += 1;
            self.metrics.gross_loss += pnl.abs();
            self.trade_outcomes.push(0.0);
        }

        // Drawdown tracking
        if self.metrics.total_pnl > self.metrics.peak_pnl {
            self.metrics.peak_pnl = self.metrics.total_pnl;
        }

        self.metrics.current_drawdown = if self.metrics.peak_pnl > 0.0 {
            (self.metrics.peak_pnl - self.metrics.total_pnl) / self.metrics.peak_pnl
        } else {
            0.0
        };

        if self.metrics.current_drawdown > self.metrics.max_drawdown {
            self.metrics.max_drawdown = self.metrics.current_drawdown;
        }

        // Update duration
        if let Some(start) = self.start_time {
            self.metrics.duration_secs = start.elapsed().as_secs_f64();
        }
    }

    /// Get the primary metric value based on metric type
    pub fn get_metric(&self, metric: ComparisonMetric) -> f64 {
        match metric {
            ComparisonMetric::NetPnl => self.metrics.total_pnl,
            ComparisonMetric::SharpeRatio => self.metrics.sharpe_ratio(),
            ComparisonMetric::WinRate => self.metrics.win_rate(),
            ComparisonMetric::ProfitFactor => self.metrics.profit_factor(),
            ComparisonMetric::MaxDrawdown => -self.metrics.max_drawdown, // Negative so lower is worse
        }
    }
}

/// Result of A/B test analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABTestResult {
    /// Test configuration
    pub config: ABTestConfig,
    /// Results per variant
    pub variant_results: HashMap<String, VariantResult>,
    /// Pairwise comparisons
    pub comparisons: Vec<PairwiseComparison>,
    /// Overall winner (if any)
    pub winner: Option<String>,
    /// Test status
    pub status: ABTestStatus,
    /// Recommendations
    pub recommendations: Vec<String>,
    /// Total duration
    pub total_duration_secs: f64,
}

/// Result for a single variant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantResult {
    /// Variant name
    pub name: String,
    /// Algorithm type
    pub algorithm_type: AlgorithmType,
    /// Metrics
    pub metrics: VariantMetrics,
    /// Bootstrap CI for primary metric
    pub primary_metric_ci: BootstrapCI,
    /// Rank (1 = best)
    pub rank: usize,
}

/// Pairwise comparison between two variants
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairwiseComparison {
    /// Variant A name
    pub variant_a: String,
    /// Variant B name
    pub variant_b: String,
    /// Statistical comparison
    pub comparison: TwoSampleComparison,
    /// Winner (A, B, or None if not significant)
    pub winner: Option<String>,
    /// Confidence in the result
    pub confidence: ComparisonConfidence,
}

/// Confidence level for comparison result
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ComparisonConfidence {
    /// High confidence - statistically significant with large effect
    High,
    /// Medium confidence - significant but small effect
    Medium,
    /// Low confidence - not significant or too few samples
    Low,
    /// Insufficient data
    InsufficientData,
}

/// Status of the A/B test
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ABTestStatus {
    /// Still collecting data
    Running,
    /// Have enough data, analysis ready
    AnalysisReady,
    /// Clear winner found
    WinnerDeclared,
    /// No significant difference found
    NoDifference,
    /// Test stopped early due to sequential analysis
    EarlyStopped,
}

/// A/B Test Manager
pub struct ABTestManager {
    config: ABTestConfig,
    variants: HashMap<String, ABVariant>,
    start_time: Option<Instant>,
    tester: StatisticalTester,
    /// Number of analyses performed (for sequential analysis)
    analyses_performed: usize,
}

impl ABTestManager {
    /// Create a new A/B test manager
    pub fn new(config: ABTestConfig) -> Self {
        let tester = StatisticalTester::new(config.alpha, config.n_bootstrap, 42);
        Self {
            config,
            variants: HashMap::new(),
            start_time: None,
            tester,
            analyses_performed: 0,
        }
    }

    /// Add a variant to the test
    pub fn add_variant(&mut self, name: &str, preset: ParameterPreset) {
        let variant = ABVariant::new(name, preset);
        self.variants.insert(name.to_string(), variant);
    }

    /// Start the test
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
        for variant in self.variants.values_mut() {
            variant.start();
        }
    }

    /// Record a trade for a specific variant
    pub fn record_trade(&mut self, variant_name: &str, pnl: f64) {
        if let Some(variant) = self.variants.get_mut(variant_name) {
            variant.record_trade(pnl);
        }
    }

    /// Get a variant by name
    pub fn get_variant(&self, name: &str) -> Option<&ABVariant> {
        self.variants.get(name)
    }

    /// Get all variant names
    pub fn variant_names(&self) -> Vec<String> {
        self.variants.keys().cloned().collect()
    }

    /// Check if minimum data requirements are met
    pub fn has_sufficient_data(&self) -> bool {
        let min_trades = self.config.min_trades_per_variant;
        let min_hours = self.config.min_hours;

        // Check all variants have minimum trades
        let all_have_trades = self.variants.values().all(|v| v.metrics.total_trades >= min_trades);

        // Check minimum duration
        let has_duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64() / 3600.0 >= min_hours)
            .unwrap_or(false);

        all_have_trades && has_duration
    }

    /// Analyze the test results
    pub fn analyze(&mut self) -> ABTestResult {
        self.analyses_performed += 1;

        let total_duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        // Check if we have sufficient data
        let status = if !self.has_sufficient_data() {
            ABTestStatus::Running
        } else {
            ABTestStatus::AnalysisReady
        };

        // Build variant results
        let mut variant_results: HashMap<String, VariantResult> = HashMap::new();
        let mut metric_values: Vec<(String, f64)> = Vec::new();

        for (name, variant) in &self.variants {
            let metric_data: Vec<f64> = variant.metrics.trade_pnls.clone();
            let primary_metric_ci = if metric_data.len() >= 2 {
                self.tester.bootstrap_ci(&metric_data, 0.95)
            } else {
                BootstrapCI::default()
            };

            let result = VariantResult {
                name: name.clone(),
                algorithm_type: variant.preset.algorithm_type.clone(),
                metrics: variant.metrics.clone(),
                primary_metric_ci,
                rank: 0, // Will be set later
            };

            let metric_value = variant.get_metric(self.config.primary_metric);
            metric_values.push((name.clone(), metric_value));
            variant_results.insert(name.clone(), result);
        }

        // Rank variants
        metric_values.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        for (rank, (name, _)) in metric_values.iter().enumerate() {
            if let Some(result) = variant_results.get_mut(name) {
                result.rank = rank + 1;
            }
        }

        // Pairwise comparisons
        let comparisons = self.compute_pairwise_comparisons();

        // Determine winner
        let (winner, final_status) = self.determine_winner(&comparisons, status);

        // Generate recommendations
        let recommendations = self.generate_recommendations(&variant_results, &comparisons, &final_status);

        ABTestResult {
            config: self.config.clone(),
            variant_results,
            comparisons,
            winner,
            status: final_status,
            recommendations,
            total_duration_secs: total_duration,
        }
    }

    /// Compute all pairwise comparisons
    fn compute_pairwise_comparisons(&self) -> Vec<PairwiseComparison> {
        let mut comparisons = Vec::new();
        let names: Vec<_> = self.variants.keys().collect();

        for i in 0..names.len() {
            for j in (i + 1)..names.len() {
                let name_a = names[i];
                let name_b = names[j];

                let variant_a = &self.variants[name_a];
                let variant_b = &self.variants[name_b];

                let sample_a: Vec<f64> = variant_a.metrics.trade_pnls.clone();
                let sample_b: Vec<f64> = variant_b.metrics.trade_pnls.clone();

                // Determine confidence
                let confidence = if sample_a.len() < 5 || sample_b.len() < 5 {
                    ComparisonConfidence::InsufficientData
                } else if sample_a.len() < 20 || sample_b.len() < 20 {
                    ComparisonConfidence::Low
                } else {
                    ComparisonConfidence::Medium // Will be upgraded to High if significant
                };

                let comparison = if confidence == ComparisonConfidence::InsufficientData {
                    TwoSampleComparison {
                        sample_a: SampleStats::from_slice(&sample_a),
                        sample_b: SampleStats::from_slice(&sample_b),
                        test_result: HypothesisTestResult::default(),
                        difference_ci: BootstrapCI::default(),
                        practical_significance: super::statistical::PracticalSignificance {
                            effect_category: EffectCategory::Negligible,
                            is_meaningful: false,
                            interpretation: "Insufficient data".to_string(),
                        },
                    }
                } else {
                    self.tester.compare_samples(&sample_a, &sample_b)
                };

                // Determine winner
                let (winner, final_confidence) = if confidence == ComparisonConfidence::InsufficientData {
                    (None, ComparisonConfidence::InsufficientData)
                } else if !comparison.test_result.is_significant {
                    (None, ComparisonConfidence::Low)
                } else {
                    let effect = comparison.test_result.effect_size;
                    let w = if effect > 0.0 {
                        Some(name_a.clone())
                    } else {
                        Some(name_b.clone())
                    };

                    let conf = if comparison.practical_significance.is_meaningful {
                        ComparisonConfidence::High
                    } else {
                        ComparisonConfidence::Medium
                    };

                    (w, conf)
                };

                comparisons.push(PairwiseComparison {
                    variant_a: name_a.clone(),
                    variant_b: name_b.clone(),
                    comparison,
                    winner,
                    confidence: final_confidence,
                });
            }
        }

        comparisons
    }

    /// Determine overall winner
    fn determine_winner(
        &self,
        comparisons: &[PairwiseComparison],
        current_status: ABTestStatus,
    ) -> (Option<String>, ABTestStatus) {
        if current_status == ABTestStatus::Running {
            return (None, current_status);
        }

        // Count wins for each variant
        let mut wins: HashMap<String, usize> = HashMap::new();
        let mut high_confidence_wins: HashMap<String, usize> = HashMap::new();

        for name in self.variants.keys() {
            wins.insert(name.clone(), 0);
            high_confidence_wins.insert(name.clone(), 0);
        }

        for comp in comparisons {
            if let Some(ref winner) = comp.winner {
                *wins.get_mut(winner).unwrap() += 1;
                if comp.confidence == ComparisonConfidence::High {
                    *high_confidence_wins.get_mut(winner).unwrap() += 1;
                }
            }
        }

        let num_variants = self.variants.len();
        let _total_comparisons = comparisons.len();

        // A variant is the winner if it beats all others
        let required_wins = num_variants - 1;

        let potential_winners: Vec<_> = wins
            .iter()
            .filter(|(_, &w)| w >= required_wins)
            .map(|(name, _)| name.clone())
            .collect();

        if potential_winners.len() == 1 {
            let winner = potential_winners[0].clone();
            let high_conf = high_confidence_wins.get(&winner).copied().unwrap_or(0);

            if high_conf >= required_wins {
                return (Some(winner), ABTestStatus::WinnerDeclared);
            } else {
                return (Some(winner), ABTestStatus::AnalysisReady);
            }
        }

        // No clear winner
        let any_significant = comparisons.iter().any(|c| c.winner.is_some());
        let status = if any_significant {
            ABTestStatus::AnalysisReady
        } else {
            ABTestStatus::NoDifference
        };

        (None, status)
    }

    /// Generate recommendations
    fn generate_recommendations(
        &self,
        results: &HashMap<String, VariantResult>,
        comparisons: &[PairwiseComparison],
        status: &ABTestStatus,
    ) -> Vec<String> {
        let mut recs = Vec::new();

        match status {
            ABTestStatus::Running => {
                // Calculate how much more data needed
                let min_trades = self.config.min_trades_per_variant;
                let min_hours = self.config.min_hours;

                let trades_needed: usize = self
                    .variants
                    .values()
                    .map(|v| min_trades.saturating_sub(v.metrics.total_trades))
                    .max()
                    .unwrap_or(0);

                if trades_needed > 0 {
                    recs.push(format!(
                        "Need {} more trades per variant (minimum: {})",
                        trades_needed, min_trades
                    ));
                }

                let hours_elapsed = self
                    .start_time
                    .map(|t| t.elapsed().as_secs_f64() / 3600.0)
                    .unwrap_or(0.0);

                if hours_elapsed < min_hours {
                    recs.push(format!(
                        "Need {:.1} more hours of data (minimum: {:.1}h)",
                        min_hours - hours_elapsed,
                        min_hours
                    ));
                }
            }
            ABTestStatus::WinnerDeclared => {
                recs.push("Consider deploying the winning variant.".to_string());
                recs.push("Continue monitoring for regime changes.".to_string());
            }
            ABTestStatus::NoDifference => {
                recs.push("No significant difference detected between variants.".to_string());
                recs.push("Consider: (1) running longer, (2) using variant with lower risk, or (3) testing different parameters.".to_string());
            }
            ABTestStatus::AnalysisReady => {
                // Check for concerning patterns
                for result in results.values() {
                    if result.metrics.max_drawdown > 0.1 {
                        recs.push(format!(
                            "Warning: {} has high drawdown ({:.1}%)",
                            result.name,
                            result.metrics.max_drawdown * 100.0
                        ));
                    }
                }

                let any_low_confidence = comparisons
                    .iter()
                    .any(|c| c.confidence == ComparisonConfidence::Low);

                if any_low_confidence {
                    recs.push("Some comparisons have low confidence. Consider running longer.".to_string());
                }
            }
            ABTestStatus::EarlyStopped => {
                recs.push("Test stopped early via sequential analysis.".to_string());
                recs.push("Result may have inflated Type I error rate.".to_string());
            }
        }

        recs
    }

    /// Get summary statistics for all variants
    pub fn summary(&self) -> String {
        let mut s = format!("A/B Test: {}\n", self.config.test_name);
        s.push_str(&format!("Primary metric: {:?}\n", self.config.primary_metric));
        s.push_str(&format!("Variants: {}\n", self.variants.len()));
        s.push_str(&"-".repeat(50));
        s.push('\n');

        for (name, variant) in &self.variants {
            s.push_str(&format!(
                "{}: {} trades, PnL={:.4}, Sharpe={:.2}, WR={:.1}%\n",
                name,
                variant.metrics.total_trades,
                variant.metrics.total_pnl,
                variant.metrics.sharpe_ratio(),
                variant.metrics.win_rate() * 100.0
            ));
        }

        s
    }
}

impl ABTestResult {
    /// Print a formatted report
    pub fn print_report(&self) {
        println!();
        println!("════════════════════════════════════════════════════════════════");
        println!("                      A/B TEST REPORT");
        println!("════════════════════════════════════════════════════════════════");
        println!();

        println!("Test: {}", self.config.test_name);
        println!("Duration: {:.1} hours", self.total_duration_secs / 3600.0);
        println!("Status: {:?}", self.status);

        if let Some(ref winner) = self.winner {
            println!("WINNER: {}", winner);
        }

        println!();
        println!("VARIANT RESULTS:");
        println!("{}", "-".repeat(60));

        let mut results: Vec<_> = self.variant_results.values().collect();
        results.sort_by_key(|r| r.rank);

        for result in &results {
            println!(
                "#{} {} [{}]",
                result.rank,
                result.name,
                match result.algorithm_type {
                    AlgorithmType::AvellanedaStoikov => "A-S",
                    AlgorithmType::MLSpreadSkew => "ML",
                }
            );
            println!(
                "   Trades: {}, PnL: {:.4}, Sharpe: {:.2}",
                result.metrics.total_trades,
                result.metrics.total_pnl,
                result.metrics.sharpe_ratio()
            );
            println!(
                "   Win Rate: {:.1}%, Max DD: {:.1}%",
                result.metrics.win_rate() * 100.0,
                result.metrics.max_drawdown * 100.0
            );
            println!(
                "   Primary Metric 95% CI: [{:.4}, {:.4}]",
                result.primary_metric_ci.lower, result.primary_metric_ci.upper
            );
            println!();
        }

        if !self.comparisons.is_empty() {
            println!("PAIRWISE COMPARISONS:");
            println!("{}", "-".repeat(60));

            for comp in &self.comparisons {
                println!("{} vs {}", comp.variant_a, comp.variant_b);
                println!(
                    "   p-value: {:.4}, Effect: {:.2} ({:?})",
                    comp.comparison.test_result.p_value,
                    comp.comparison.test_result.effect_size,
                    comp.comparison.practical_significance.effect_category
                );
                println!(
                    "   Confidence: {:?}, Winner: {}",
                    comp.confidence,
                    comp.winner.as_deref().unwrap_or("None")
                );
            }
            println!();
        }

        if !self.recommendations.is_empty() {
            println!("RECOMMENDATIONS:");
            for rec in &self.recommendations {
                println!("  - {}", rec);
            }
        }

        println!("════════════════════════════════════════════════════════════════");
    }

    /// Check if there's a clear winner
    pub fn has_winner(&self) -> bool {
        self.winner.is_some() && self.status == ABTestStatus::WinnerDeclared
    }

    /// Get the winning variant name
    pub fn get_winner(&self) -> Option<&str> {
        self.winner.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_preset(name: &str) -> ParameterPreset {
        ParameterPreset::new(name, "test", 2.0, 0.5, 0.7, 0.10)
    }

    // ==================== ABTestConfig Tests ====================

    #[test]
    fn test_ab_config_default() {
        let config = ABTestConfig::default();
        assert_eq!(config.min_trades_per_variant, 30);
        assert_eq!(config.alpha, 0.05);
        assert_eq!(config.primary_metric, ComparisonMetric::NetPnl);
    }

    // ==================== VariantMetrics Tests ====================

    #[test]
    fn test_variant_metrics_default() {
        let metrics = VariantMetrics::default();
        assert_eq!(metrics.total_trades, 0);
        assert_eq!(metrics.sharpe_ratio(), 0.0);
        assert_eq!(metrics.win_rate(), 0.0);
    }

    #[test]
    fn test_variant_metrics_win_rate() {
        let metrics = VariantMetrics {
            winning_trades: 6,
            losing_trades: 4,
            ..Default::default()
        };
        assert!((metrics.win_rate() - 0.6).abs() < 1e-10);
    }

    #[test]
    fn test_variant_metrics_profit_factor() {
        let metrics = VariantMetrics {
            gross_profit: 100.0,
            gross_loss: 50.0,
            ..Default::default()
        };
        assert!((metrics.profit_factor() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_variant_metrics_profit_factor_no_loss() {
        let metrics = VariantMetrics {
            gross_profit: 100.0,
            gross_loss: 0.0,
            ..Default::default()
        };
        assert!(metrics.profit_factor().is_infinite());
    }

    #[test]
    fn test_variant_metrics_sharpe() {
        let metrics = VariantMetrics {
            total_trades: 5,
            trade_pnls: vec![0.01, 0.02, -0.01, 0.015, 0.005],
            ..Default::default()
        };
        let sharpe = metrics.sharpe_ratio();
        // Should be positive (more gains than losses)
        assert!(sharpe > 0.0);
    }

    // ==================== ABVariant Tests ====================

    #[test]
    fn test_ab_variant_creation() {
        let preset = create_test_preset("Test");
        let variant = ABVariant::new("control", preset);
        assert_eq!(variant.name, "control");
        assert_eq!(variant.metrics.total_trades, 0);
    }

    #[test]
    fn test_ab_variant_record_trade_win() {
        let preset = create_test_preset("Test");
        let mut variant = ABVariant::new("test", preset);
        variant.start();

        variant.record_trade(0.05);

        assert_eq!(variant.metrics.total_trades, 1);
        assert_eq!(variant.metrics.winning_trades, 1);
        assert_eq!(variant.metrics.losing_trades, 0);
        assert!((variant.metrics.total_pnl - 0.05).abs() < 1e-10);
    }

    #[test]
    fn test_ab_variant_record_trade_loss() {
        let preset = create_test_preset("Test");
        let mut variant = ABVariant::new("test", preset);

        variant.record_trade(-0.03);

        assert_eq!(variant.metrics.total_trades, 1);
        assert_eq!(variant.metrics.winning_trades, 0);
        assert_eq!(variant.metrics.losing_trades, 1);
        assert!((variant.metrics.gross_loss - 0.03).abs() < 1e-10);
    }

    #[test]
    fn test_ab_variant_drawdown_tracking() {
        let preset = create_test_preset("Test");
        let mut variant = ABVariant::new("test", preset);

        variant.record_trade(0.10);  // PnL: 0.10, Peak: 0.10
        variant.record_trade(0.05);  // PnL: 0.15, Peak: 0.15
        variant.record_trade(-0.06); // PnL: 0.09, DD: 6/15 = 40%

        assert!((variant.metrics.max_drawdown - 0.4).abs() < 1e-10);
    }

    #[test]
    fn test_ab_variant_get_metric() {
        let preset = create_test_preset("Test");
        let mut variant = ABVariant::new("test", preset);

        variant.record_trade(0.05);
        variant.record_trade(0.03);
        variant.record_trade(-0.02);

        let pnl = variant.get_metric(ComparisonMetric::NetPnl);
        assert!((pnl - 0.06).abs() < 1e-10);

        let wr = variant.get_metric(ComparisonMetric::WinRate);
        assert!((wr - 2.0 / 3.0).abs() < 1e-10);
    }

    // ==================== ABTestManager Tests ====================

    #[test]
    fn test_ab_manager_creation() {
        let config = ABTestConfig::default();
        let manager = ABTestManager::new(config);
        assert!(manager.variants.is_empty());
    }

    #[test]
    fn test_ab_manager_add_variant() {
        let config = ABTestConfig::default();
        let mut manager = ABTestManager::new(config);

        let preset_a = create_test_preset("A");
        let preset_b = create_test_preset("B");

        manager.add_variant("control", preset_a);
        manager.add_variant("treatment", preset_b);

        assert_eq!(manager.variants.len(), 2);
        assert!(manager.get_variant("control").is_some());
        assert!(manager.get_variant("treatment").is_some());
        assert!(manager.get_variant("nonexistent").is_none());
    }

    #[test]
    fn test_ab_manager_record_trade() {
        let config = ABTestConfig::default();
        let mut manager = ABTestManager::new(config);

        manager.add_variant("control", create_test_preset("A"));
        manager.start();

        manager.record_trade("control", 0.05);
        manager.record_trade("control", -0.02);

        let variant = manager.get_variant("control").unwrap();
        assert_eq!(variant.metrics.total_trades, 2);
    }

    #[test]
    fn test_ab_manager_has_sufficient_data_false() {
        let mut config = ABTestConfig::default();
        config.min_trades_per_variant = 10;
        config.min_hours = 0.001; // Very short

        let mut manager = ABTestManager::new(config);
        manager.add_variant("control", create_test_preset("A"));
        manager.start();

        // Only 5 trades, need 10
        for _ in 0..5 {
            manager.record_trade("control", 0.01);
        }

        assert!(!manager.has_sufficient_data());
    }

    #[test]
    fn test_ab_manager_has_sufficient_data_true() {
        let mut config = ABTestConfig::default();
        config.min_trades_per_variant = 5;
        config.min_hours = 0.0; // No time requirement

        let mut manager = ABTestManager::new(config);
        manager.add_variant("control", create_test_preset("A"));
        manager.add_variant("treatment", create_test_preset("B"));
        manager.start();

        for _ in 0..10 {
            manager.record_trade("control", 0.01);
            manager.record_trade("treatment", 0.01);
        }

        // Now has enough trades
        assert!(manager.has_sufficient_data());
    }

    #[test]
    fn test_ab_manager_analyze_insufficient_data() {
        let mut config = ABTestConfig::default();
        config.min_trades_per_variant = 100;

        let mut manager = ABTestManager::new(config);
        manager.add_variant("control", create_test_preset("A"));
        manager.start();

        // Only a few trades
        manager.record_trade("control", 0.01);
        manager.record_trade("control", 0.02);

        let result = manager.analyze();
        assert_eq!(result.status, ABTestStatus::Running);
        assert!(result.winner.is_none());
    }

    #[test]
    fn test_ab_manager_analyze_with_clear_winner() {
        let mut config = ABTestConfig::default();
        config.min_trades_per_variant = 5;
        config.min_hours = 0.0;

        let mut manager = ABTestManager::new(config);
        manager.add_variant("winner", create_test_preset("A"));
        manager.add_variant("loser", create_test_preset("B"));
        manager.start();

        // Winner has much better results
        for _ in 0..30 {
            manager.record_trade("winner", 0.05);  // Consistent wins
            manager.record_trade("loser", -0.03); // Consistent losses
        }

        let result = manager.analyze();

        // Should detect winner
        assert_eq!(result.variant_results.len(), 2);

        let winner_result = &result.variant_results["winner"];
        let loser_result = &result.variant_results["loser"];

        assert!(winner_result.rank < loser_result.rank);
    }

    #[test]
    fn test_ab_manager_variant_names() {
        let config = ABTestConfig::default();
        let mut manager = ABTestManager::new(config);

        manager.add_variant("alpha", create_test_preset("A"));
        manager.add_variant("beta", create_test_preset("B"));
        manager.add_variant("gamma", create_test_preset("C"));

        let names = manager.variant_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"alpha".to_string()));
        assert!(names.contains(&"beta".to_string()));
        assert!(names.contains(&"gamma".to_string()));
    }

    #[test]
    fn test_ab_manager_summary() {
        let config = ABTestConfig::default();
        let mut manager = ABTestManager::new(config);

        manager.add_variant("control", create_test_preset("A"));
        manager.start();

        for _ in 0..5 {
            manager.record_trade("control", 0.01);
        }

        let summary = manager.summary();
        assert!(summary.contains("control"));
        assert!(summary.contains("5 trades"));
    }

    // ==================== ABTestResult Tests ====================

    #[test]
    fn test_ab_result_has_winner() {
        let config = ABTestConfig::default();
        let result = ABTestResult {
            config,
            variant_results: HashMap::new(),
            comparisons: Vec::new(),
            winner: Some("best".to_string()),
            status: ABTestStatus::WinnerDeclared,
            recommendations: Vec::new(),
            total_duration_secs: 0.0,
        };

        assert!(result.has_winner());
        assert_eq!(result.get_winner(), Some("best"));
    }

    #[test]
    fn test_ab_result_no_winner() {
        let config = ABTestConfig::default();
        let result = ABTestResult {
            config,
            variant_results: HashMap::new(),
            comparisons: Vec::new(),
            winner: None,
            status: ABTestStatus::NoDifference,
            recommendations: Vec::new(),
            total_duration_secs: 0.0,
        };

        assert!(!result.has_winner());
        assert_eq!(result.get_winner(), None);
    }

    // ==================== ComparisonMetric Tests ====================

    #[test]
    fn test_comparison_metric_equality() {
        assert_eq!(ComparisonMetric::NetPnl, ComparisonMetric::NetPnl);
        assert_ne!(ComparisonMetric::NetPnl, ComparisonMetric::SharpeRatio);
    }

    // ==================== ComparisonConfidence Tests ====================

    #[test]
    fn test_comparison_confidence_equality() {
        assert_eq!(ComparisonConfidence::High, ComparisonConfidence::High);
        assert_ne!(ComparisonConfidence::High, ComparisonConfidence::Low);
    }

    // ==================== ABTestStatus Tests ====================

    #[test]
    fn test_ab_test_status_equality() {
        assert_eq!(ABTestStatus::Running, ABTestStatus::Running);
        assert_ne!(ABTestStatus::Running, ABTestStatus::WinnerDeclared);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_ab_manager_record_nonexistent_variant() {
        let config = ABTestConfig::default();
        let mut manager = ABTestManager::new(config);

        // This should not panic
        manager.record_trade("nonexistent", 0.05);
    }

    #[test]
    fn test_ab_manager_single_variant() {
        let mut config = ABTestConfig::default();
        config.min_trades_per_variant = 3;
        config.min_hours = 0.0;

        let mut manager = ABTestManager::new(config);
        manager.add_variant("solo", create_test_preset("A"));
        manager.start();

        for _ in 0..5 {
            manager.record_trade("solo", 0.02);
        }

        let result = manager.analyze();

        // With single variant, no comparisons possible
        assert!(result.comparisons.is_empty());
    }

    #[test]
    fn test_pairwise_comparison_creation() {
        let comparison = PairwiseComparison {
            variant_a: "A".to_string(),
            variant_b: "B".to_string(),
            comparison: TwoSampleComparison {
                sample_a: SampleStats::default(),
                sample_b: SampleStats::default(),
                test_result: HypothesisTestResult::default(),
                difference_ci: BootstrapCI::default(),
                practical_significance: super::super::statistical::PracticalSignificance {
                    effect_category: EffectCategory::Negligible,
                    is_meaningful: false,
                    interpretation: "Test".to_string(),
                },
            },
            winner: None,
            confidence: ComparisonConfidence::InsufficientData,
        };

        assert_eq!(comparison.variant_a, "A");
        assert_eq!(comparison.variant_b, "B");
        assert!(comparison.winner.is_none());
    }

    #[test]
    fn test_variant_result_creation() {
        let result = VariantResult {
            name: "test".to_string(),
            algorithm_type: AlgorithmType::AvellanedaStoikov,
            metrics: VariantMetrics::default(),
            primary_metric_ci: BootstrapCI::default(),
            rank: 1,
        };

        assert_eq!(result.name, "test");
        assert_eq!(result.rank, 1);
    }
}

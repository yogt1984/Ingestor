//! Out-of-Sample Validation Framework
//!
//! Provides systematic comparison between in-sample (backtest) and out-of-sample
//! (paper trading or hold-out) performance to detect overfitting and validate
//! strategy robustness.
//!
//! # Key Features
//!
//! - **Hold-out Test Set**: Reserve most recent data for final validation
//! - **Performance Degradation Analysis**: Compare IS vs OOS metrics
//! - **Overfitting Detection**: Statistical tests for strategy robustness
//! - **Paper vs Backtest Comparison**: Compare expected vs realized performance
//!
//! # Methodology
//!
//! The framework implements the validation approach from:
//! - Bailey, D.H. & Lopez de Prado, M. (2014). "The Deflated Sharpe Ratio"
//! - Pardo, R. (2008). "The Evaluation and Optimization of Trading Strategies"
//! - Lopez de Prado, M. (2018). "Advances in Financial Machine Learning" Ch.11-12
//!
//! # Usage
//!
//! ```ignore
//! use crate::backtest::oos_validation::{OOSValidator, OOSConfig, ValidationReport};
//!
//! // Create validator with 20% hold-out
//! let config = OOSConfig {
//!     holdout_fraction: 0.20,
//!     ..Default::default()
//! };
//!
//! let mut validator = OOSValidator::new(config);
//! validator.load_data("./data/features")?;
//!
//! // Run validation
//! let report = validator.validate(mm_config, fill_prob)?;
//!
//! // Check for overfitting
//! if report.is_overfit() {
//!     println!("WARNING: Strategy shows signs of overfitting");
//! }
//! ```

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use rust_decimal_macros::dec;
use rust_decimal::prelude::*;
use anyhow::Result;

use crate::backtest::{
    BacktestEngine, BacktestConfig, BacktestResults, PerformanceMetrics,
    ReplayEvent, ReplayConfig, FillSimulatorConfig,
};
use crate::backtest::statistics::StatisticalReport;
use crate::execution::market_maker::MMConfig;
use crate::execution::mm_simulator::SimulatorConfig;

/// Configuration for out-of-sample validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OOSConfig {
    /// Fraction of data to reserve for final hold-out test (0.0-0.5)
    /// Default: 0.20 (20%)
    pub holdout_fraction: f64,

    /// Gap between train and test to prevent lookahead (hours)
    /// Default: 1.0 hour
    pub embargo_hours: f64,

    /// Minimum number of events required in each split
    pub min_events_per_split: usize,

    /// Data directory
    pub data_dir: PathBuf,

    /// Verbose output
    pub verbose: bool,

    /// Overfitting threshold: OOS/IS Sharpe ratio below this is concerning
    /// Default: 0.5 (OOS Sharpe < 50% of IS Sharpe indicates overfitting)
    pub overfit_threshold: f64,

    /// Significance threshold for declaring results "real"
    /// Default: 0.05 (95% confidence)
    pub significance_level: f64,
}

impl Default for OOSConfig {
    fn default() -> Self {
        Self {
            holdout_fraction: 0.20,
            embargo_hours: 1.0,
            min_events_per_split: 1000,
            data_dir: PathBuf::from("./data/features"),
            verbose: true,
            overfit_threshold: 0.5,
            significance_level: 0.05,
        }
    }
}

/// Performance metrics for a single sample (IS or OOS)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleMetrics {
    pub sharpe_ratio: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub avg_trade_pnl: f64,
    /// Time span in hours
    pub time_span_hours: f64,
    /// Number of events
    pub num_events: usize,
}

impl From<&PerformanceMetrics> for SampleMetrics {
    fn from(m: &PerformanceMetrics) -> Self {
        Self {
            sharpe_ratio: m.sharpe_ratio,
            total_return: m.total_return,
            max_drawdown: m.max_drawdown,
            num_trades: m.num_trades,
            win_rate: m.win_rate,
            profit_factor: m.profit_factor,
            avg_trade_pnl: m.avg_trade_pnl.to_f64().unwrap_or(0.0),
            time_span_hours: 0.0,
            num_events: 0,
        }
    }
}

/// Comparison between in-sample and out-of-sample performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceComparison {
    /// In-sample metrics
    pub in_sample: SampleMetrics,

    /// Out-of-sample metrics
    pub out_of_sample: SampleMetrics,

    /// Ratio of OOS/IS Sharpe (closer to 1.0 = less overfit)
    pub sharpe_degradation: f64,

    /// Ratio of OOS/IS return (closer to 1.0 = less overfit)
    pub return_degradation: f64,

    /// Difference in win rate (IS - OOS)
    pub win_rate_drop: f64,

    /// Ratio of OOS/IS trades (measures consistency)
    pub trade_frequency_ratio: f64,
}

impl PerformanceComparison {
    pub fn new(is: SampleMetrics, oos: SampleMetrics) -> Self {
        let sharpe_degradation = if is.sharpe_ratio.abs() > 0.01 {
            oos.sharpe_ratio / is.sharpe_ratio
        } else {
            0.0
        };

        let return_degradation = if is.total_return.abs() > 0.0001 {
            oos.total_return / is.total_return
        } else {
            0.0
        };

        let win_rate_drop = is.win_rate - oos.win_rate;

        // Normalize by time to get trades per hour ratio
        let is_trades_per_hour = if is.time_span_hours > 0.0 {
            is.num_trades as f64 / is.time_span_hours
        } else {
            0.0
        };
        let oos_trades_per_hour = if oos.time_span_hours > 0.0 {
            oos.num_trades as f64 / oos.time_span_hours
        } else {
            0.0
        };
        let trade_frequency_ratio = if is_trades_per_hour > 0.0 {
            oos_trades_per_hour / is_trades_per_hour
        } else {
            0.0
        };

        Self {
            in_sample: is,
            out_of_sample: oos,
            sharpe_degradation,
            return_degradation,
            win_rate_drop,
            trade_frequency_ratio,
        }
    }
}

/// Overfitting verdict based on IS/OOS comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverfitVerdict {
    /// OOS performance is comparable to IS (ratio > 0.8)
    Robust,
    /// OOS performance shows some degradation (ratio 0.5-0.8)
    MildOverfit,
    /// OOS performance shows significant degradation (ratio 0.2-0.5)
    ModerateOverfit,
    /// OOS performance is much worse than IS (ratio < 0.2)
    SevereOverfit,
    /// Not enough data to determine
    Inconclusive,
}

impl std::fmt::Display for OverfitVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OverfitVerdict::Robust => write!(f, "ROBUST (good generalization)"),
            OverfitVerdict::MildOverfit => write!(f, "MILD OVERFITTING (some degradation)"),
            OverfitVerdict::ModerateOverfit => write!(f, "MODERATE OVERFITTING (significant degradation)"),
            OverfitVerdict::SevereOverfit => write!(f, "SEVERE OVERFITTING (poor generalization)"),
            OverfitVerdict::Inconclusive => write!(f, "INCONCLUSIVE (insufficient data)"),
        }
    }
}

/// Full validation report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Configuration used
    pub config: OOSConfig,

    /// Parameters tested
    pub params_tested: TestedParams,

    /// Performance comparison
    pub comparison: PerformanceComparison,

    /// Statistical significance of OOS results
    pub oos_statistics: StatisticalReport,

    /// Overfitting verdict
    pub overfit_verdict: OverfitVerdict,

    /// Recommendation for next steps
    pub recommendation: ValidationRecommendation,
}

/// Parameters that were tested
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestedParams {
    pub spread_bps: f64,
    pub skew_factor: f64,
    pub fill_probability: f64,
    pub high_entropy_threshold: f64,
}

/// Recommendation based on validation results
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationRecommendation {
    /// Strategy is ready for paper trading
    ReadyForPaperTrading,
    /// Strategy needs more data for validation
    NeedsMoreData,
    /// Strategy is overfit - simplify parameters
    SimplifyStrategy,
    /// Strategy shows no edge - reconsider approach
    ReconsiderApproach,
    /// Results are statistically insignificant
    StatisticallyInsignificant,
}

impl std::fmt::Display for ValidationRecommendation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationRecommendation::ReadyForPaperTrading =>
                write!(f, "Ready for paper trading - proceed with caution"),
            ValidationRecommendation::NeedsMoreData =>
                write!(f, "Needs more data - collect at least 2 more weeks"),
            ValidationRecommendation::SimplifyStrategy =>
                write!(f, "Simplify strategy - reduce degrees of freedom"),
            ValidationRecommendation::ReconsiderApproach =>
                write!(f, "Reconsider approach - no evidence of edge"),
            ValidationRecommendation::StatisticallyInsignificant =>
                write!(f, "Statistically insignificant - insufficient trades"),
        }
    }
}

impl ValidationReport {
    /// Check if the strategy shows signs of overfitting
    pub fn is_overfit(&self) -> bool {
        matches!(
            self.overfit_verdict,
            OverfitVerdict::ModerateOverfit | OverfitVerdict::SevereOverfit
        )
    }

    /// Check if results are statistically significant
    pub fn is_significant(&self) -> bool {
        self.oos_statistics.probabilistic_sharpe > 0.95
    }

    /// Check if ready for paper trading
    pub fn is_ready_for_paper(&self) -> bool {
        matches!(self.recommendation, ValidationRecommendation::ReadyForPaperTrading)
    }

    /// Save report to JSON file
    pub fn save_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Print detailed report
    pub fn print(&self) {
        println!();
        println!("════════════════════════════════════════════════════════════");
        println!("              OUT-OF-SAMPLE VALIDATION REPORT                ");
        println!("════════════════════════════════════════════════════════════");
        println!();

        // Parameters tested
        println!("PARAMETERS TESTED:");
        println!("  Spread:          {:.1} bps", self.params_tested.spread_bps);
        println!("  Skew:            {:.2}", self.params_tested.skew_factor);
        println!("  Fill Prob:       {:.0}%", self.params_tested.fill_probability * 100.0);
        println!("  High Entropy:    {:.2}", self.params_tested.high_entropy_threshold);
        println!();

        // Split information
        println!("DATA SPLIT:");
        println!("  In-Sample:       {:.1} hours ({} events, {} trades)",
            self.comparison.in_sample.time_span_hours,
            self.comparison.in_sample.num_events,
            self.comparison.in_sample.num_trades);
        println!("  Out-of-Sample:   {:.1} hours ({} events, {} trades)",
            self.comparison.out_of_sample.time_span_hours,
            self.comparison.out_of_sample.num_events,
            self.comparison.out_of_sample.num_trades);
        println!("  Holdout:         {:.0}%", self.config.holdout_fraction * 100.0);
        println!();

        // Performance comparison table
        println!("PERFORMANCE COMPARISON:");
        println!("┌─────────────────────┬──────────────┬──────────────┬─────────────┐");
        println!("│ Metric              │  In-Sample   │Out-of-Sample │ Degradation │");
        println!("├─────────────────────┼──────────────┼──────────────┼─────────────┤");
        println!("│ Sharpe Ratio        │ {:+10.3}  │ {:+10.3}  │   {:+.0}%      │",
            self.comparison.in_sample.sharpe_ratio,
            self.comparison.out_of_sample.sharpe_ratio,
            (1.0 - self.comparison.sharpe_degradation) * 100.0);
        println!("│ Total Return        │ {:+10.2}% │ {:+10.2}% │   {:+.0}%      │",
            self.comparison.in_sample.total_return * 100.0,
            self.comparison.out_of_sample.total_return * 100.0,
            (1.0 - self.comparison.return_degradation) * 100.0);
        println!("│ Max Drawdown        │ {:10.2}% │ {:10.2}% │             │",
            self.comparison.in_sample.max_drawdown * 100.0,
            self.comparison.out_of_sample.max_drawdown * 100.0);
        println!("│ Win Rate            │ {:10.1}% │ {:10.1}% │   {:+.1}pp    │",
            self.comparison.in_sample.win_rate * 100.0,
            self.comparison.out_of_sample.win_rate * 100.0,
            -self.comparison.win_rate_drop * 100.0);
        println!("│ Profit Factor       │ {:10.2}  │ {:10.2}  │             │",
            self.comparison.in_sample.profit_factor,
            self.comparison.out_of_sample.profit_factor);
        println!("│ Trades/Hour         │ {:10.2}  │ {:10.2}  │   {:.0}x       │",
            if self.comparison.in_sample.time_span_hours > 0.0 {
                self.comparison.in_sample.num_trades as f64 / self.comparison.in_sample.time_span_hours
            } else { 0.0 },
            if self.comparison.out_of_sample.time_span_hours > 0.0 {
                self.comparison.out_of_sample.num_trades as f64 / self.comparison.out_of_sample.time_span_hours
            } else { 0.0 },
            self.comparison.trade_frequency_ratio);
        println!("└─────────────────────┴──────────────┴──────────────┴─────────────┘");
        println!();

        // Overfitting analysis
        println!("OVERFITTING ANALYSIS:");
        println!("  Sharpe Degradation: {:.2} (OOS/IS ratio)", self.comparison.sharpe_degradation);
        println!("  Verdict: {}", self.overfit_verdict);
        println!();

        // Statistical significance
        println!("STATISTICAL SIGNIFICANCE (OOS):");
        println!("  P(Sharpe > 0):    {:.1}%", self.oos_statistics.probabilistic_sharpe * 100.0);
        println!("  Deflated Sharpe:  {:.3}", self.oos_statistics.deflated_sharpe);
        println!("  Min Track Record: {} trades (have {})",
            self.oos_statistics.min_track_record_length,
            self.oos_statistics.num_trades);
        println!("  Sufficient Data:  {}",
            if self.oos_statistics.has_sufficient_data { "YES" } else { "NO" });
        println!();

        // Final recommendation
        println!("════════════════════════════════════════════════════════════");
        println!("RECOMMENDATION: {}", self.recommendation);
        println!("════════════════════════════════════════════════════════════");
        println!();

        // Academic references
        println!("References:");
        println!("  - Bailey & Lopez de Prado (2014) \"The Deflated Sharpe Ratio\"");
        println!("  - Pardo (2008) \"The Evaluation and Optimization of Trading Strategies\"");
        println!("  - Lopez de Prado (2018) \"Advances in Financial Machine Learning\" Ch.11-12");
        println!();
    }
}

/// Out-of-sample validator engine
pub struct OOSValidator {
    config: OOSConfig,
    events: Vec<ReplayEvent>,
    time_range: Option<(i64, i64)>,
}

impl OOSValidator {
    /// Create a new OOS validator
    pub fn new(config: OOSConfig) -> Self {
        Self {
            config,
            events: Vec::new(),
            time_range: None,
        }
    }

    /// Load data from Parquet files
    pub fn load_data(&mut self) -> Result<usize> {
        use crate::backtest::replay::ParquetReplay;

        let replay_config = ReplayConfig {
            data_dir: self.config.data_dir.clone(),
            ..Default::default()
        };

        let mut replay = ParquetReplay::new(replay_config);
        let num_events = replay.load()?;

        self.time_range = replay.time_range();
        self.events = replay.into_events();

        if self.config.verbose {
            println!("Loaded {} events for OOS validation", num_events);
            if let Some((start, end)) = self.time_range {
                let hours = (end - start) as f64 / (1000.0 * 60.0 * 60.0);
                println!("Time span: {:.1} hours ({:.1} days)", hours, hours / 24.0);
            }
        }

        Ok(num_events)
    }

    /// Split data into in-sample and out-of-sample sets
    fn split_data(&self) -> Result<(Vec<ReplayEvent>, Vec<ReplayEvent>)> {
        let (start_ms, end_ms) = self.time_range
            .ok_or_else(|| anyhow::anyhow!("No time range - load data first"))?;

        let total_duration = end_ms - start_ms;
        let embargo_ms = (self.config.embargo_hours * 60.0 * 60.0 * 1000.0) as i64;

        // Calculate split point
        let oos_duration = (total_duration as f64 * self.config.holdout_fraction) as i64;
        let split_point = end_ms - oos_duration - embargo_ms;

        if self.config.verbose {
            let is_hours = (split_point - start_ms) as f64 / (1000.0 * 60.0 * 60.0);
            let oos_hours = (end_ms - split_point - embargo_ms) as f64 / (1000.0 * 60.0 * 60.0);
            println!("Split: IS={:.1}h, Embargo={:.1}h, OOS={:.1}h",
                is_hours, self.config.embargo_hours, oos_hours);
        }

        // Split events
        let is_events: Vec<ReplayEvent> = self.events
            .iter()
            .filter(|e| e.timestamp_ms < split_point)
            .cloned()
            .collect();

        let oos_events: Vec<ReplayEvent> = self.events
            .iter()
            .filter(|e| e.timestamp_ms >= split_point + embargo_ms)
            .cloned()
            .collect();

        // Validate minimum events
        if is_events.len() < self.config.min_events_per_split {
            anyhow::bail!(
                "In-sample has {} events, minimum required is {}",
                is_events.len(),
                self.config.min_events_per_split
            );
        }

        if oos_events.len() < self.config.min_events_per_split {
            anyhow::bail!(
                "Out-of-sample has {} events, minimum required is {}",
                oos_events.len(),
                self.config.min_events_per_split
            );
        }

        Ok((is_events, oos_events))
    }

    /// Run backtest on a subset of events
    fn run_backtest(
        &self,
        events: Vec<ReplayEvent>,
        mm_config: MMConfig,
        fill_prob: f64,
    ) -> Result<BacktestResults> {
        let config = BacktestConfig {
            replay: ReplayConfig {
                data_dir: self.config.data_dir.clone(),
                ..Default::default()
            },
            mm: mm_config,
            simulator: SimulatorConfig::default(),
            fill_sim: FillSimulatorConfig {
                base_fill_probability: fill_prob,
                ..Default::default()
            },
            verbose: false,
            use_realistic_fills: true,
            ..Default::default()
        };

        let mut engine = BacktestEngine::from_events(config, events);
        engine.run()
    }

    /// Validate a strategy with hold-out test
    pub fn validate(
        &self,
        mm_config: MMConfig,
        fill_prob: f64,
    ) -> Result<ValidationReport> {
        if self.events.is_empty() {
            anyhow::bail!("No data loaded. Call load_data() first.");
        }

        if self.config.verbose {
            println!();
            println!("Running out-of-sample validation...");
        }

        // Split data
        let (is_events, oos_events) = self.split_data()?;
        let is_num_events = is_events.len();
        let oos_num_events = oos_events.len();

        // Calculate time spans
        let is_time_span = if !is_events.is_empty() {
            let start = is_events.first().unwrap().timestamp_ms;
            let end = is_events.last().unwrap().timestamp_ms;
            (end - start) as f64 / (1000.0 * 60.0 * 60.0)
        } else {
            0.0
        };

        let oos_time_span = if !oos_events.is_empty() {
            let start = oos_events.first().unwrap().timestamp_ms;
            let end = oos_events.last().unwrap().timestamp_ms;
            (end - start) as f64 / (1000.0 * 60.0 * 60.0)
        } else {
            0.0
        };

        // Run backtests
        if self.config.verbose {
            println!("Running in-sample backtest ({} events)...", is_num_events);
        }
        let is_results = self.run_backtest(is_events, mm_config.clone(), fill_prob)?;

        if self.config.verbose {
            println!("Running out-of-sample backtest ({} events)...", oos_num_events);
        }
        let oos_results = self.run_backtest(oos_events, mm_config.clone(), fill_prob)?;

        // Build sample metrics
        let mut is_metrics = SampleMetrics::from(&is_results.metrics);
        is_metrics.time_span_hours = is_time_span;
        is_metrics.num_events = is_num_events;

        let mut oos_metrics = SampleMetrics::from(&oos_results.metrics);
        oos_metrics.time_span_hours = oos_time_span;
        oos_metrics.num_events = oos_num_events;

        // Create comparison
        let comparison = PerformanceComparison::new(is_metrics, oos_metrics);

        // Compute OOS statistics
        let oos_statistics = oos_results.compute_statistics(1);

        // Determine overfitting verdict
        let overfit_verdict = self.determine_overfit_verdict(&comparison, &oos_statistics);

        // Determine recommendation
        let recommendation = self.determine_recommendation(&comparison, &oos_statistics, &overfit_verdict);

        // Extract tested params
        let params_tested = TestedParams {
            spread_bps: mm_config.regime_params.high_entropy.spread_bps,
            skew_factor: mm_config.regime_params.high_entropy.skew_factor,
            fill_probability: fill_prob,
            high_entropy_threshold: mm_config.regime_thresholds.high_entropy_threshold,
        };

        let report = ValidationReport {
            config: self.config.clone(),
            params_tested,
            comparison,
            oos_statistics,
            overfit_verdict,
            recommendation,
        };

        if self.config.verbose {
            report.print();
        }

        Ok(report)
    }

    /// Determine overfitting verdict based on comparison
    fn determine_overfit_verdict(
        &self,
        comparison: &PerformanceComparison,
        _stats: &StatisticalReport,
    ) -> OverfitVerdict {
        // Need minimum trades in OOS
        if comparison.out_of_sample.num_trades < 20 {
            return OverfitVerdict::Inconclusive;
        }

        // Check if IS performance is too low to compare
        if comparison.in_sample.num_trades < 20 {
            return OverfitVerdict::Inconclusive;
        }

        // Use Sharpe degradation as primary metric
        let degradation = comparison.sharpe_degradation;

        // Handle case where IS and OOS have opposite signs
        if comparison.in_sample.sharpe_ratio > 0.0 && comparison.out_of_sample.sharpe_ratio < 0.0 {
            return OverfitVerdict::SevereOverfit;
        }

        if degradation > 0.8 {
            OverfitVerdict::Robust
        } else if degradation > 0.5 {
            OverfitVerdict::MildOverfit
        } else if degradation > 0.2 {
            OverfitVerdict::ModerateOverfit
        } else if degradation >= 0.0 {
            OverfitVerdict::SevereOverfit
        } else {
            // Negative degradation means OOS is actually better (rare)
            OverfitVerdict::Robust
        }
    }

    /// Determine recommendation based on all factors
    fn determine_recommendation(
        &self,
        comparison: &PerformanceComparison,
        stats: &StatisticalReport,
        verdict: &OverfitVerdict,
    ) -> ValidationRecommendation {
        // Check for insufficient data first
        if !stats.has_sufficient_data || comparison.out_of_sample.num_trades < 30 {
            return ValidationRecommendation::NeedsMoreData;
        }

        // Check statistical significance
        if stats.probabilistic_sharpe < 0.75 {
            return ValidationRecommendation::StatisticallyInsignificant;
        }

        // Check for severe overfitting
        if matches!(verdict, OverfitVerdict::SevereOverfit) {
            return ValidationRecommendation::SimplifyStrategy;
        }

        // Check for moderate overfitting
        if matches!(verdict, OverfitVerdict::ModerateOverfit) {
            return ValidationRecommendation::SimplifyStrategy;
        }

        // Check if OOS actually shows profit potential
        if comparison.out_of_sample.total_return <= 0.0
            || comparison.out_of_sample.sharpe_ratio < 0.0 {
            return ValidationRecommendation::ReconsiderApproach;
        }

        // All checks passed
        ValidationRecommendation::ReadyForPaperTrading
    }

    /// Run validation with multiple parameter sets (grid)
    pub fn validate_grid(
        &self,
        spreads: &[f64],
        skews: &[f64],
        fill_probs: &[f64],
    ) -> Result<Vec<ValidationReport>> {
        use crate::execution::market_maker::RegimeParams;

        let mut reports = Vec::new();

        for &spread in spreads {
            for &skew in skews {
                for &fill_prob in fill_probs {
                    let mm_config = MMConfig {
                        regime_params: RegimeParams::uniform(spread, skew),
                        max_inventory: dec!(0.1),
                        quote_size: dec!(0.001),
                        ..Default::default()
                    };

                    match self.validate(mm_config, fill_prob) {
                        Ok(report) => reports.push(report),
                        Err(e) => {
                            if self.config.verbose {
                                println!("  Skip spread={}, skew={}: {}", spread, skew, e);
                            }
                        }
                    }
                }
            }
        }

        // Sort by OOS Sharpe
        reports.sort_by(|a, b| {
            b.comparison.out_of_sample.sharpe_ratio
                .partial_cmp(&a.comparison.out_of_sample.sharpe_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(reports)
    }
}

/// Paper trading session record (for comparing backtest to paper)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperSession {
    /// Session start timestamp (ms)
    pub start_ms: i64,
    /// Session end timestamp (ms)
    pub end_ms: i64,
    /// Parameters used
    pub params: TestedParams,
    /// Session metrics
    pub metrics: SampleMetrics,
    /// Number of quotes generated
    pub num_quotes: usize,
    /// Fill rate observed
    pub fill_rate: f64,
}

/// Comparison between backtest expectations and paper trading reality
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestVsPaperReport {
    /// Backtest results (expected)
    pub backtest: SampleMetrics,

    /// Paper trading results (actual)
    pub paper: SampleMetrics,

    /// Return ratio (paper/backtest)
    pub return_ratio: f64,

    /// Sharpe ratio (paper/backtest)
    pub sharpe_ratio: f64,

    /// Fill rate ratio (paper/backtest_assumption)
    pub fill_rate_ratio: f64,

    /// Is paper trading meeting expectations?
    pub meets_expectations: bool,

    /// Recommendations
    pub recommendations: Vec<String>,
}

impl BacktestVsPaperReport {
    /// Create comparison report
    pub fn new(
        backtest: SampleMetrics,
        paper: SampleMetrics,
        backtest_fill_rate: f64,
        paper_fill_rate: f64,
    ) -> Self {
        let return_ratio = if backtest.total_return.abs() > 0.0001 {
            paper.total_return / backtest.total_return
        } else {
            0.0
        };

        let sharpe_ratio = if backtest.sharpe_ratio.abs() > 0.01 {
            paper.sharpe_ratio / backtest.sharpe_ratio
        } else {
            0.0
        };

        let fill_rate_ratio = if backtest_fill_rate > 0.0 {
            paper_fill_rate / backtest_fill_rate
        } else {
            0.0
        };

        // Determine if meeting expectations (within 50% of backtest)
        let meets_expectations = return_ratio > 0.5 && sharpe_ratio > 0.5;

        // Generate recommendations
        let mut recommendations = Vec::new();

        if fill_rate_ratio < 0.5 {
            recommendations.push(
                "Fill rate significantly lower than expected - consider tighter spreads or front-of-queue strategies".to_string()
            );
        }

        if return_ratio < 0.3 {
            recommendations.push(
                "Returns significantly below backtest - likely overfitting or adverse selection".to_string()
            );
        }

        if paper.win_rate < backtest.win_rate * 0.8 {
            recommendations.push(
                "Win rate degraded - check for increased adverse selection in live conditions".to_string()
            );
        }

        if paper.max_drawdown > backtest.max_drawdown * 2.0 {
            recommendations.push(
                "Drawdown much larger than expected - reduce position size".to_string()
            );
        }

        if meets_expectations {
            recommendations.push(
                "Performance within acceptable range - consider gradually increasing size".to_string()
            );
        }

        Self {
            backtest,
            paper,
            return_ratio,
            sharpe_ratio,
            fill_rate_ratio,
            meets_expectations,
            recommendations,
        }
    }

    /// Print comparison report
    pub fn print(&self) {
        println!();
        println!("════════════════════════════════════════════════════════════");
        println!("          BACKTEST vs PAPER TRADING COMPARISON               ");
        println!("════════════════════════════════════════════════════════════");
        println!();

        println!("┌─────────────────────┬──────────────┬──────────────┬─────────┐");
        println!("│ Metric              │   Backtest   │    Paper     │  Ratio  │");
        println!("├─────────────────────┼──────────────┼──────────────┼─────────┤");
        println!("│ Total Return        │ {:+10.2}% │ {:+10.2}% │  {:.0}%   │",
            self.backtest.total_return * 100.0,
            self.paper.total_return * 100.0,
            self.return_ratio * 100.0);
        println!("│ Sharpe Ratio        │ {:+10.3}  │ {:+10.3}  │  {:.0}%   │",
            self.backtest.sharpe_ratio,
            self.paper.sharpe_ratio,
            self.sharpe_ratio * 100.0);
        println!("│ Win Rate            │ {:10.1}% │ {:10.1}% │         │",
            self.backtest.win_rate * 100.0,
            self.paper.win_rate * 100.0);
        println!("│ Trades              │ {:10}  │ {:10}  │         │",
            self.backtest.num_trades,
            self.paper.num_trades);
        println!("│ Max Drawdown        │ {:10.2}% │ {:10.2}% │         │",
            self.backtest.max_drawdown * 100.0,
            self.paper.max_drawdown * 100.0);
        println!("└─────────────────────┴──────────────┴──────────────┴─────────┘");
        println!();

        println!("VERDICT: {}",
            if self.meets_expectations { "MEETING EXPECTATIONS" } else { "BELOW EXPECTATIONS" });
        println!();

        if !self.recommendations.is_empty() {
            println!("RECOMMENDATIONS:");
            for rec in &self.recommendations {
                println!("  - {}", rec);
            }
            println!();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oos_config_default() {
        let config = OOSConfig::default();
        assert_eq!(config.holdout_fraction, 0.20);
        assert_eq!(config.embargo_hours, 1.0);
        assert_eq!(config.min_events_per_split, 1000);
    }

    #[test]
    fn test_sample_metrics_from_performance() {
        let perf = PerformanceMetrics {
            sharpe_ratio: 1.5,
            total_return: 0.10,
            max_drawdown: 0.05,
            num_trades: 100,
            win_rate: 0.55,
            profit_factor: 1.8,
            avg_trade_pnl: dec!(0.001),
            ..Default::default()
        };

        let sample = SampleMetrics::from(&perf);
        assert_eq!(sample.sharpe_ratio, 1.5);
        assert_eq!(sample.total_return, 0.10);
        assert_eq!(sample.num_trades, 100);
    }

    #[test]
    fn test_performance_comparison() {
        let is = SampleMetrics {
            sharpe_ratio: 2.0,
            total_return: 0.10,
            max_drawdown: 0.05,
            num_trades: 100,
            win_rate: 0.60,
            profit_factor: 2.0,
            avg_trade_pnl: 0.001,
            time_span_hours: 100.0,
            num_events: 10000,
        };

        let oos = SampleMetrics {
            sharpe_ratio: 1.5,  // 75% of IS
            total_return: 0.08, // 80% of IS
            max_drawdown: 0.06,
            num_trades: 80,
            win_rate: 0.55,
            profit_factor: 1.8,
            avg_trade_pnl: 0.001,
            time_span_hours: 50.0,
            num_events: 5000,
        };

        let comparison = PerformanceComparison::new(is, oos);

        // Check degradation calculations
        assert!((comparison.sharpe_degradation - 0.75).abs() < 0.01);
        assert!((comparison.return_degradation - 0.80).abs() < 0.01);
        assert!((comparison.win_rate_drop - 0.05).abs() < 0.01);
    }

    #[test]
    fn test_overfit_verdict_display() {
        assert_eq!(
            format!("{}", OverfitVerdict::Robust),
            "ROBUST (good generalization)"
        );
        assert_eq!(
            format!("{}", OverfitVerdict::SevereOverfit),
            "SEVERE OVERFITTING (poor generalization)"
        );
    }

    #[test]
    fn test_recommendation_display() {
        assert!(format!("{}", ValidationRecommendation::ReadyForPaperTrading)
            .contains("paper trading"));
        assert!(format!("{}", ValidationRecommendation::SimplifyStrategy)
            .contains("Simplify"));
    }

    #[test]
    fn test_backtest_vs_paper_report() {
        let backtest = SampleMetrics {
            sharpe_ratio: 2.0,
            total_return: 0.10,
            max_drawdown: 0.03,
            num_trades: 100,
            win_rate: 0.60,
            profit_factor: 2.0,
            avg_trade_pnl: 0.001,
            time_span_hours: 100.0,
            num_events: 10000,
        };

        let paper = SampleMetrics {
            sharpe_ratio: 1.5,
            total_return: 0.06,
            max_drawdown: 0.04,
            num_trades: 50,
            win_rate: 0.54,
            profit_factor: 1.5,
            avg_trade_pnl: 0.0012,
            time_span_hours: 48.0,
            num_events: 5000,
        };

        let report = BacktestVsPaperReport::new(
            backtest, paper,
            0.10,  // backtest fill rate assumption
            0.05,  // paper actual fill rate
        );

        // Check ratios
        assert!((report.return_ratio - 0.60).abs() < 0.01);
        assert!((report.sharpe_ratio - 0.75).abs() < 0.01);
        assert!((report.fill_rate_ratio - 0.50).abs() < 0.01);

        // Should meet expectations (ratios > 0.5)
        assert!(report.meets_expectations);
    }

    #[test]
    fn test_backtest_vs_paper_below_expectations() {
        let backtest = SampleMetrics {
            sharpe_ratio: 2.0,
            total_return: 0.10,
            max_drawdown: 0.03,
            num_trades: 100,
            win_rate: 0.60,
            profit_factor: 2.0,
            avg_trade_pnl: 0.001,
            time_span_hours: 100.0,
            num_events: 10000,
        };

        let paper = SampleMetrics {
            sharpe_ratio: 0.3,  // Much worse
            total_return: 0.01, // Much worse
            max_drawdown: 0.08, // Larger
            num_trades: 30,
            win_rate: 0.45,
            profit_factor: 1.1,
            avg_trade_pnl: 0.0003,
            time_span_hours: 48.0,
            num_events: 5000,
        };

        let report = BacktestVsPaperReport::new(
            backtest, paper,
            0.10, 0.03,
        );

        // Should NOT meet expectations
        assert!(!report.meets_expectations);
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    fn test_tested_params() {
        let params = TestedParams {
            spread_bps: 1.0,
            skew_factor: 0.3,
            fill_probability: 0.10,
            high_entropy_threshold: 0.7,
        };

        assert_eq!(params.spread_bps, 1.0);
        assert_eq!(params.high_entropy_threshold, 0.7);
    }
}

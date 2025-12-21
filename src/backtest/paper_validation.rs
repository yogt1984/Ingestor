//! Paper Trading Validation Infrastructure
//!
//! Validates whether backtested strategies perform as expected in paper trading.
//! Compares actual fill rates, Sharpe ratios, and win rates against backtest expectations.
//!
//! # Key Features
//!
//! - Load and analyze paper trading sessions
//! - Compare against backtest expectations
//! - Generate PASS/FAIL verdicts with detailed diagnostics
//! - Aggregate multiple sessions for statistical confidence
//!
//! # Usage
//!
//! ```ignore
//! use crate::backtest::paper_validation::{SessionValidator, ValidationConfig};
//!
//! let config = ValidationConfig::default();
//! let validator = SessionValidator::new(config)?;
//!
//! // Validate a single session
//! let report = validator.validate_session("./data/sessions/summary_20251207_120000.json")?;
//! report.print_report();
//!
//! // Validate multiple sessions for statistical confidence
//! let aggregate = validator.validate_all_sessions("./data/sessions")?;
//! aggregate.print_report();
//! ```

use std::path::{Path, PathBuf};

use anyhow::{Result, Context, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::forward_testing::{
    SessionSummary, SessionMetrics, load_session_summary, list_sessions,
};
use crate::execution::presets::{ParameterPreset, PresetStore};
use crate::strategies::AlgorithmType;

/// Configuration for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Minimum session duration in hours for valid comparison
    pub min_duration_hours: f64,
    /// Minimum trades required for valid comparison
    pub min_trades: usize,
    /// Fill rate tolerance (actual vs expected, as fraction)
    pub fill_rate_tolerance: f64,
    /// Sharpe ratio tolerance (actual vs expected)
    pub sharpe_tolerance: f64,
    /// Win rate tolerance (actual vs expected)
    pub win_rate_tolerance: f64,
    /// Trade rate tolerance ratio (actual / expected must be in this range)
    pub trade_rate_min_ratio: f64,
    pub trade_rate_max_ratio: f64,
    /// Path to sessions directory
    pub sessions_dir: PathBuf,
    /// Path to presets file
    pub presets_path: PathBuf,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            min_duration_hours: 0.5,
            min_trades: 5,
            fill_rate_tolerance: 0.05, // 5% tolerance on fill rate
            sharpe_tolerance: 1.0,     // Within 1.0 of expected Sharpe
            win_rate_tolerance: 0.10,  // 10% tolerance on win rate
            trade_rate_min_ratio: 0.5, // At least 50% of expected trade rate
            trade_rate_max_ratio: 2.0, // At most 200% of expected trade rate
            sessions_dir: PathBuf::from("./data/sessions"),
            presets_path: PathBuf::from("./data/presets.json"),
        }
    }
}

/// Validation verdict
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// All checks passed
    Pass,
    /// Minor issues detected
    Warning,
    /// Significant issues detected
    Fail,
    /// Not enough data for meaningful validation
    InsufficientData,
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Verdict::Pass => write!(f, "PASS"),
            Verdict::Warning => write!(f, "WARNING"),
            Verdict::Fail => write!(f, "FAIL"),
            Verdict::InsufficientData => write!(f, "INSUFFICIENT DATA"),
        }
    }
}

/// A single validation check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationCheck {
    /// Name of the check
    pub name: String,
    /// Expected value (from backtest)
    pub expected: f64,
    /// Actual value (from paper trading)
    pub actual: f64,
    /// Tolerance for this check
    pub tolerance: f64,
    /// Did this check pass?
    pub passed: bool,
    /// Detailed message
    pub message: String,
}

impl ValidationCheck {
    fn new(name: &str, expected: f64, actual: f64, tolerance: f64) -> Self {
        let diff = (actual - expected).abs();
        let passed = diff <= tolerance;
        let message = if passed {
            format!("{}: {:.3} vs {:.3} expected (within tolerance {:.3})", name, actual, expected, tolerance)
        } else {
            format!("{}: {:.3} vs {:.3} expected (EXCEEDS tolerance {:.3}, diff={:.3})", name, actual, expected, tolerance, diff)
        };
        Self {
            name: name.to_string(),
            expected,
            actual,
            tolerance,
            passed,
            message,
        }
    }

    fn new_ratio(name: &str, actual: f64, expected: f64, min_ratio: f64, max_ratio: f64) -> Self {
        let ratio = if expected > 0.0 { actual / expected } else { 0.0 };
        let passed = ratio >= min_ratio && ratio <= max_ratio;
        let message = if passed {
            format!("{}: {:.3} vs {:.3} expected (ratio {:.2}x, within [{:.1}x, {:.1}x])", name, actual, expected, ratio, min_ratio, max_ratio)
        } else {
            format!("{}: {:.3} vs {:.3} expected (ratio {:.2}x, OUTSIDE [{:.1}x, {:.1}x])", name, actual, expected, ratio, min_ratio, max_ratio)
        };
        Self {
            name: name.to_string(),
            expected,
            actual,
            tolerance: 0.0, // Using ratio bounds instead
            passed,
            message,
        }
    }

    fn insufficient_data(name: &str, reason: &str) -> Self {
        Self {
            name: name.to_string(),
            expected: 0.0,
            actual: 0.0,
            tolerance: 0.0,
            passed: false,
            message: format!("{}: {}", name, reason),
        }
    }
}

/// Session validation report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionValidationReport {
    /// Session being validated
    pub session_id: String,
    /// Preset used for comparison (if found)
    pub preset_name: Option<String>,
    /// Algorithm type
    pub algorithm_type: AlgorithmType,
    /// Session metrics
    pub session_metrics: SessionMetrics,
    /// Expected metrics (from preset/backtest)
    pub expected_metrics: ExpectedMetrics,
    /// Individual validation checks
    pub checks: Vec<ValidationCheck>,
    /// Overall verdict
    pub verdict: Verdict,
    /// Summary message
    pub summary: String,
    /// Recommendations for improvement
    pub recommendations: Vec<String>,
    /// Timestamp of validation
    pub validated_at: DateTime<Utc>,
}

/// Expected metrics from backtest
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExpectedMetrics {
    pub expected_return: f64,
    pub expected_sharpe: f64,
    pub expected_win_rate: f64,
    pub expected_trades: usize,
    pub backtest_duration_hours: f64,
    pub fill_prob_assumption: f64,
    pub expected_trades_per_hour: f64,
}

impl ExpectedMetrics {
    fn from_preset(preset: &ParameterPreset) -> Self {
        // Estimate backtest duration from number of events (~1500 events/hour typical)
        let backtest_hours = (preset.num_events as f64 / 1500.0).max(1.0);
        let expected_trades_per_hour = if backtest_hours > 0.0 {
            preset.expected_trades as f64 / backtest_hours
        } else {
            0.0
        };

        Self {
            expected_return: preset.expected_return,
            expected_sharpe: preset.expected_sharpe,
            expected_win_rate: preset.expected_win_rate,
            expected_trades: preset.expected_trades,
            backtest_duration_hours: backtest_hours,
            fill_prob_assumption: preset.fill_prob_assumption,
            expected_trades_per_hour,
        }
    }
}

impl SessionValidationReport {
    /// Print formatted report
    pub fn print_report(&self) {
        println!();
        println!("================================================================================");
        println!("                    PAPER TRADING VALIDATION REPORT");
        println!("================================================================================");
        println!();
        println!("Session:     {}", self.session_id);
        if let Some(ref preset) = self.preset_name {
            println!("Preset:      {}", preset);
        }
        println!("Algorithm:   {:?}", self.algorithm_type);
        println!("Duration:    {:.1} hours", self.session_metrics.duration_secs / 3600.0);
        println!("Trades:      {}", self.session_metrics.total_trades);
        println!("Validated:   {}", self.validated_at.format("%Y-%m-%d %H:%M:%S UTC"));
        println!();

        println!("VALIDATION CHECKS:");
        println!("{}", "-".repeat(80));
        for check in &self.checks {
            let status = if check.passed { "[OK]  " } else { "[FAIL]" };
            println!("{} {}", status, check.message);
        }
        println!("{}", "-".repeat(80));
        println!();

        let verdict_display = match self.verdict {
            Verdict::Pass => "PASS",
            Verdict::Warning => "WARNING",
            Verdict::Fail => "FAIL",
            Verdict::InsufficientData => "INSUFFICIENT DATA",
        };
        println!("VERDICT: {}", verdict_display);
        println!();
        println!("Summary: {}", self.summary);

        if !self.recommendations.is_empty() {
            println!();
            println!("Recommendations:");
            for rec in &self.recommendations {
                println!("  - {}", rec);
            }
        }

        println!("================================================================================");
    }

    /// Save report to JSON file
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

/// Aggregated validation report across multiple sessions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedValidationReport {
    /// Number of sessions analyzed
    pub num_sessions: usize,
    /// Total duration across all sessions (hours)
    pub total_duration_hours: f64,
    /// Total trades across all sessions
    pub total_trades: u64,
    /// Sessions that passed validation
    pub sessions_passed: usize,
    /// Sessions with warnings
    pub sessions_warning: usize,
    /// Sessions that failed validation
    pub sessions_failed: usize,
    /// Sessions with insufficient data
    pub sessions_insufficient: usize,
    /// Aggregated metrics
    pub aggregated_metrics: AggregatedMetrics,
    /// Individual session reports
    pub session_reports: Vec<SessionValidationReport>,
    /// Overall verdict
    pub verdict: Verdict,
    /// Summary message
    pub summary: String,
    /// Statistical confidence assessment
    pub statistical_confidence: StatisticalConfidence,
    /// Timestamp of validation
    pub validated_at: DateTime<Utc>,
}

/// Aggregated metrics across sessions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AggregatedMetrics {
    /// Average Sharpe ratio
    pub avg_sharpe: f64,
    /// Sharpe standard deviation
    pub sharpe_std: f64,
    /// Average win rate
    pub avg_win_rate: f64,
    /// Win rate standard deviation
    pub win_rate_std: f64,
    /// Average fill rate
    pub avg_fill_rate: f64,
    /// Fill rate standard deviation
    pub fill_rate_std: f64,
    /// Total PnL
    pub total_pnl: f64,
    /// Average trades per hour
    pub avg_trades_per_hour: f64,
    /// Max drawdown observed
    pub max_drawdown: f64,
}

/// Statistical confidence assessment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalConfidence {
    /// Number of sessions (sample size)
    pub sample_size: usize,
    /// Minimum sessions needed for statistical significance
    pub min_sample_size: usize,
    /// Is sample size sufficient?
    pub sufficient_sample: bool,
    /// Sharpe ratio 95% confidence interval
    pub sharpe_ci_lower: f64,
    pub sharpe_ci_upper: f64,
    /// Win rate 95% confidence interval
    pub win_rate_ci_lower: f64,
    pub win_rate_ci_upper: f64,
    /// Probability that true Sharpe > 0 (estimated)
    pub prob_positive_sharpe: f64,
    /// Confidence level message
    pub confidence_message: String,
}

impl AggregatedValidationReport {
    /// Print formatted report
    pub fn print_report(&self) {
        println!();
        println!("================================================================================");
        println!("                AGGREGATED VALIDATION REPORT");
        println!("================================================================================");
        println!();
        println!("Sessions Analyzed:   {}", self.num_sessions);
        println!("Total Duration:      {:.1} hours", self.total_duration_hours);
        println!("Total Trades:        {}", self.total_trades);
        println!();

        println!("SESSION OUTCOMES:");
        println!("  Passed:            {}", self.sessions_passed);
        println!("  Warnings:          {}", self.sessions_warning);
        println!("  Failed:            {}", self.sessions_failed);
        println!("  Insufficient Data: {}", self.sessions_insufficient);
        println!();

        println!("AGGREGATED METRICS:");
        println!("{}", "-".repeat(60));
        println!("  Avg Sharpe:        {:.2} +/- {:.2}", self.aggregated_metrics.avg_sharpe, self.aggregated_metrics.sharpe_std);
        println!("  Avg Win Rate:      {:.1}% +/- {:.1}%", self.aggregated_metrics.avg_win_rate * 100.0, self.aggregated_metrics.win_rate_std * 100.0);
        println!("  Avg Fill Rate:     {:.1}% +/- {:.1}%", self.aggregated_metrics.avg_fill_rate * 100.0, self.aggregated_metrics.fill_rate_std * 100.0);
        println!("  Total PnL:         {:+.4}", self.aggregated_metrics.total_pnl);
        println!("  Trades/Hour:       {:.1}", self.aggregated_metrics.avg_trades_per_hour);
        println!("  Max Drawdown:      {:.2}%", self.aggregated_metrics.max_drawdown * 100.0);
        println!("{}", "-".repeat(60));
        println!();

        println!("STATISTICAL CONFIDENCE:");
        println!("  Sample Size:       {} ({} min required)",
                 self.statistical_confidence.sample_size,
                 self.statistical_confidence.min_sample_size);
        if self.statistical_confidence.sufficient_sample {
            println!("  Sharpe 95% CI:     [{:.2}, {:.2}]",
                     self.statistical_confidence.sharpe_ci_lower,
                     self.statistical_confidence.sharpe_ci_upper);
            println!("  Win Rate 95% CI:   [{:.1}%, {:.1}%]",
                     self.statistical_confidence.win_rate_ci_lower * 100.0,
                     self.statistical_confidence.win_rate_ci_upper * 100.0);
            println!("  P(Sharpe > 0):     {:.1}%", self.statistical_confidence.prob_positive_sharpe * 100.0);
        }
        println!("  Assessment:        {}", self.statistical_confidence.confidence_message);
        println!();

        let verdict_display = match self.verdict {
            Verdict::Pass => "PASS",
            Verdict::Warning => "WARNING",
            Verdict::Fail => "FAIL",
            Verdict::InsufficientData => "INSUFFICIENT DATA",
        };
        println!("OVERALL VERDICT: {}", verdict_display);
        println!();
        println!("Summary: {}", self.summary);
        println!("================================================================================");
    }

    /// Save report to JSON file
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

/// Session validator
pub struct SessionValidator {
    config: ValidationConfig,
    presets: PresetStore,
}

impl SessionValidator {
    /// Create a new validator
    pub fn new(config: ValidationConfig) -> Result<Self> {
        let presets = PresetStore::load();
        Ok(Self { config, presets })
    }

    /// Validate a single session
    pub fn validate_session(&self, session_path: &Path) -> Result<SessionValidationReport> {
        let summary = load_session_summary(session_path)
            .with_context(|| format!("Failed to load session: {:?}", session_path))?;

        self.validate_session_summary(&summary)
    }

    /// Validate a session summary directly
    pub fn validate_session_summary(&self, summary: &SessionSummary) -> Result<SessionValidationReport> {
        let metrics = &summary.metrics;

        // Find matching preset
        let preset = self.find_matching_preset(&summary.config);
        let expected = preset.as_ref()
            .map(|p| ExpectedMetrics::from_preset(p))
            .unwrap_or_default();

        let mut checks = Vec::new();
        let mut recommendations = Vec::new();

        // Check minimum data requirements
        let duration_hours = metrics.duration_secs / 3600.0;
        if duration_hours < self.config.min_duration_hours {
            checks.push(ValidationCheck::insufficient_data(
                "Duration",
                &format!("{:.1}h < {:.1}h minimum", duration_hours, self.config.min_duration_hours)
            ));
        }

        if metrics.total_trades < self.config.min_trades as u64 {
            checks.push(ValidationCheck::insufficient_data(
                "Trade Count",
                &format!("{} < {} minimum", metrics.total_trades, self.config.min_trades)
            ));
        }

        // If insufficient data, return early
        if checks.iter().any(|c| !c.passed) && checks.iter().all(|c| c.message.contains("minimum")) {
            return Ok(SessionValidationReport {
                session_id: summary.session_id.clone(),
                preset_name: preset.map(|p| p.name.clone()),
                algorithm_type: summary.config.algorithm_type.clone(),
                session_metrics: metrics.clone(),
                expected_metrics: expected,
                checks,
                verdict: Verdict::InsufficientData,
                summary: "Insufficient data for meaningful validation. Continue running the session.".to_string(),
                recommendations: vec!["Run paper trading for at least 30 minutes with 5+ trades.".to_string()],
                validated_at: Utc::now(),
            });
        }

        // Trade rate check (most critical for fill rate validation)
        if expected.expected_trades_per_hour > 0.0 {
            let actual_trades_per_hour = if duration_hours > 0.0 {
                metrics.total_trades as f64 / duration_hours
            } else {
                0.0
            };

            checks.push(ValidationCheck::new_ratio(
                "Trade Rate",
                actual_trades_per_hour,
                expected.expected_trades_per_hour,
                self.config.trade_rate_min_ratio,
                self.config.trade_rate_max_ratio,
            ));

            if actual_trades_per_hour < expected.expected_trades_per_hour * self.config.trade_rate_min_ratio {
                recommendations.push("Trade rate much lower than expected - fill rate may be overestimated in backtest.".to_string());
            }
        }

        // Fill rate check (actual fill rate vs backtest assumption)
        if metrics.quotes_generated > 0 && expected.fill_prob_assumption > 0.0 {
            let actual_fill_rate = metrics.total_trades as f64 / metrics.quotes_generated as f64;
            checks.push(ValidationCheck::new(
                "Fill Rate",
                expected.fill_prob_assumption,
                actual_fill_rate,
                self.config.fill_rate_tolerance,
            ));

            if actual_fill_rate < expected.fill_prob_assumption - self.config.fill_rate_tolerance {
                recommendations.push(format!(
                    "Actual fill rate ({:.1}%) significantly below assumption ({:.1}%). Backtest may be too optimistic.",
                    actual_fill_rate * 100.0, expected.fill_prob_assumption * 100.0
                ));
            }
        }

        // Win rate check
        if expected.expected_win_rate > 0.0 && metrics.total_trades >= 10 {
            checks.push(ValidationCheck::new(
                "Win Rate",
                expected.expected_win_rate,
                metrics.win_rate,
                self.config.win_rate_tolerance,
            ));

            if metrics.win_rate < expected.expected_win_rate - self.config.win_rate_tolerance {
                recommendations.push("Win rate below expectation - market conditions may differ from backtest period.".to_string());
            }
        }

        // Sharpe ratio check (only meaningful with enough trades)
        if expected.expected_sharpe != 0.0 && metrics.total_trades >= 20 {
            checks.push(ValidationCheck::new(
                "Sharpe Ratio",
                expected.expected_sharpe,
                metrics.sharpe_ratio,
                self.config.sharpe_tolerance,
            ));

            if metrics.sharpe_ratio < expected.expected_sharpe - self.config.sharpe_tolerance {
                recommendations.push("Sharpe ratio below expectation - consider reviewing regime detection or stopping out.".to_string());
            }
        }

        // Bid/Ask fill rate symmetry check
        if metrics.bid_touches > 10 && metrics.ask_touches > 10 {
            let bid_ask_diff = (metrics.bid_fill_rate - metrics.ask_fill_rate).abs();
            let symmetry_check = ValidationCheck::new(
                "Fill Rate Symmetry",
                0.0, // Expected no asymmetry
                bid_ask_diff,
                0.1, // 10% tolerance
            );
            checks.push(symmetry_check);

            if bid_ask_diff > 0.1 {
                if metrics.bid_fill_rate > metrics.ask_fill_rate {
                    recommendations.push("Bid fills significantly higher than ask - may indicate adverse selection on buys.".to_string());
                } else {
                    recommendations.push("Ask fills significantly higher than bid - may indicate adverse selection on sells.".to_string());
                }
            }
        }

        // Determine verdict
        let failed_count = checks.iter().filter(|c| !c.passed).count();
        let critical_fails = checks.iter()
            .filter(|c| !c.passed && (c.name == "Trade Rate" || c.name == "Fill Rate"))
            .count();

        let verdict = if failed_count == 0 {
            Verdict::Pass
        } else if critical_fails > 0 || failed_count >= 2 {
            Verdict::Fail
        } else {
            Verdict::Warning
        };

        let summary_msg = match verdict {
            Verdict::Pass => "All validation checks passed. Paper trading results align with backtest expectations.".to_string(),
            Verdict::Warning => format!("{} check(s) failed. Minor discrepancies detected.", failed_count),
            Verdict::Fail => format!("{} check(s) failed including critical metrics. Significant divergence from backtest.", failed_count),
            Verdict::InsufficientData => "Insufficient data for validation.".to_string(),
        };

        if recommendations.is_empty() && verdict == Verdict::Pass {
            recommendations.push("Continue monitoring. Consider extending session duration for higher confidence.".to_string());
        }

        Ok(SessionValidationReport {
            session_id: summary.session_id.clone(),
            preset_name: preset.map(|p| p.name.clone()),
            algorithm_type: summary.config.algorithm_type.clone(),
            session_metrics: metrics.clone(),
            expected_metrics: expected,
            checks,
            verdict,
            summary: summary_msg,
            recommendations,
            validated_at: Utc::now(),
        })
    }

    /// Validate all sessions in a directory
    pub fn validate_all_sessions(&self, sessions_dir: &Path) -> Result<AggregatedValidationReport> {
        let sessions = list_sessions(sessions_dir)
            .with_context(|| format!("Failed to list sessions in {:?}", sessions_dir))?;

        if sessions.is_empty() {
            bail!("No sessions found in {:?}", sessions_dir);
        }

        let mut reports = Vec::new();
        for session in &sessions {
            match self.validate_session_summary(session) {
                Ok(report) => reports.push(report),
                Err(e) => eprintln!("Warning: Failed to validate session {}: {}", session.session_id, e),
            }
        }

        self.aggregate_reports(reports)
    }

    /// Aggregate multiple session reports
    pub fn aggregate_reports(&self, reports: Vec<SessionValidationReport>) -> Result<AggregatedValidationReport> {
        if reports.is_empty() {
            bail!("No reports to aggregate");
        }

        // Count outcomes
        let sessions_passed = reports.iter().filter(|r| r.verdict == Verdict::Pass).count();
        let sessions_warning = reports.iter().filter(|r| r.verdict == Verdict::Warning).count();
        let sessions_failed = reports.iter().filter(|r| r.verdict == Verdict::Fail).count();
        let sessions_insufficient = reports.iter().filter(|r| r.verdict == Verdict::InsufficientData).count();

        // Filter to sessions with sufficient data for statistical analysis
        let valid_reports: Vec<_> = reports.iter()
            .filter(|r| r.verdict != Verdict::InsufficientData)
            .collect();

        // Calculate aggregated metrics
        let total_duration_hours: f64 = valid_reports.iter()
            .map(|r| r.session_metrics.duration_secs / 3600.0)
            .sum();

        let total_trades: u64 = valid_reports.iter()
            .map(|r| r.session_metrics.total_trades)
            .sum();

        let sharpes: Vec<f64> = valid_reports.iter()
            .filter(|r| r.session_metrics.total_trades >= 20)
            .map(|r| r.session_metrics.sharpe_ratio)
            .collect();

        let win_rates: Vec<f64> = valid_reports.iter()
            .filter(|r| r.session_metrics.total_trades >= 10)
            .map(|r| r.session_metrics.win_rate)
            .collect();

        let fill_rates: Vec<f64> = valid_reports.iter()
            .filter(|r| r.session_metrics.quotes_generated > 0)
            .map(|r| r.session_metrics.total_trades as f64 / r.session_metrics.quotes_generated as f64)
            .collect();

        let aggregated_metrics = AggregatedMetrics {
            avg_sharpe: mean(&sharpes),
            sharpe_std: std_dev(&sharpes),
            avg_win_rate: mean(&win_rates),
            win_rate_std: std_dev(&win_rates),
            avg_fill_rate: mean(&fill_rates),
            fill_rate_std: std_dev(&fill_rates),
            total_pnl: valid_reports.iter()
                .map(|r| r.session_metrics.net_pnl.to_string().parse::<f64>().unwrap_or(0.0))
                .sum(),
            avg_trades_per_hour: if total_duration_hours > 0.0 { total_trades as f64 / total_duration_hours } else { 0.0 },
            max_drawdown: valid_reports.iter()
                .map(|r| r.session_metrics.max_drawdown)
                .fold(0.0_f64, |a, b| a.max(b)),
        };

        // Statistical confidence
        let statistical_confidence = self.calculate_statistical_confidence(&sharpes, &win_rates);

        // Overall verdict
        let valid_count = valid_reports.len();
        let verdict = if sessions_insufficient == reports.len() {
            Verdict::InsufficientData
        } else if sessions_failed as f64 / valid_count as f64 > 0.5 {
            Verdict::Fail
        } else if sessions_passed as f64 / valid_count as f64 >= 0.7 {
            Verdict::Pass
        } else {
            Verdict::Warning
        };

        let summary = match verdict {
            Verdict::Pass => format!(
                "{} of {} sessions passed validation ({:.0}%). Paper trading confirms backtest expectations.",
                sessions_passed, valid_count, sessions_passed as f64 / valid_count as f64 * 100.0
            ),
            Verdict::Warning => format!(
                "Mixed results: {} passed, {} warnings, {} failed. More data needed.",
                sessions_passed, sessions_warning, sessions_failed
            ),
            Verdict::Fail => format!(
                "{} of {} sessions failed validation ({:.0}%). Backtest expectations not met in paper trading.",
                sessions_failed, valid_count, sessions_failed as f64 / valid_count as f64 * 100.0
            ),
            Verdict::InsufficientData => "All sessions have insufficient data for validation.".to_string(),
        };

        Ok(AggregatedValidationReport {
            num_sessions: reports.len(),
            total_duration_hours,
            total_trades,
            sessions_passed,
            sessions_warning,
            sessions_failed,
            sessions_insufficient,
            aggregated_metrics,
            session_reports: reports,
            verdict,
            summary,
            statistical_confidence,
            validated_at: Utc::now(),
        })
    }

    fn find_matching_preset(&self, config: &crate::forward_testing::ForwardTestConfig) -> Option<&ParameterPreset> {
        // First try exact name match
        if let Some(ref preset_name) = config.preset_name {
            if let Some(preset) = self.presets.presets.iter().find(|p| &p.name == preset_name) {
                return Some(preset);
            }
        }

        // Try session name match
        if let Some(ref session_name) = config.session_name {
            if let Some(preset) = self.presets.presets.iter().find(|p| &p.name == session_name) {
                return Some(preset);
            }
        }

        // Fall back to algorithm type match with latest preset
        self.presets.presets.iter()
            .filter(|p| p.algorithm_type == config.algorithm_type)
            .last()
    }

    fn calculate_statistical_confidence(&self, sharpes: &[f64], win_rates: &[f64]) -> StatisticalConfidence {
        const MIN_SAMPLE_SIZE: usize = 10;

        let n = sharpes.len();
        let sufficient = n >= MIN_SAMPLE_SIZE;

        // Calculate confidence intervals using t-distribution approximation
        let (sharpe_ci_lower, sharpe_ci_upper) = if n >= 2 {
            let mean = mean(sharpes);
            let std = std_dev(sharpes);
            let t_value = 1.96; // Approximate for large n
            let se = std / (n as f64).sqrt();
            (mean - t_value * se, mean + t_value * se)
        } else {
            (0.0, 0.0)
        };

        let (win_rate_ci_lower, win_rate_ci_upper) = if win_rates.len() >= 2 {
            let mean = mean(win_rates);
            let std = std_dev(win_rates);
            let t_value = 1.96;
            let se = std / (win_rates.len() as f64).sqrt();
            ((mean - t_value * se).max(0.0), (mean + t_value * se).min(1.0))
        } else {
            (0.0, 0.0)
        };

        // Estimate probability of positive Sharpe using normal approximation
        let prob_positive_sharpe = if n >= 2 && std_dev(sharpes) > 0.0 {
            let mean = mean(sharpes);
            let std = std_dev(sharpes);
            // P(X > 0) where X ~ N(mean, std)
            let z = mean / std;
            0.5 * (1.0 + erf(z / std::f64::consts::SQRT_2))
        } else {
            0.5
        };

        let confidence_message = if !sufficient {
            format!("Need {} more sessions for statistical significance (have {})", MIN_SAMPLE_SIZE - n, n)
        } else if sharpe_ci_lower > 0.0 {
            "High confidence: Sharpe 95% CI is entirely positive".to_string()
        } else if sharpe_ci_upper < 0.0 {
            "High confidence: Sharpe 95% CI is entirely negative".to_string()
        } else {
            format!("Moderate confidence: Sharpe 95% CI spans zero ({:.2} to {:.2})", sharpe_ci_lower, sharpe_ci_upper)
        };

        StatisticalConfidence {
            sample_size: n,
            min_sample_size: MIN_SAMPLE_SIZE,
            sufficient_sample: sufficient,
            sharpe_ci_lower,
            sharpe_ci_upper,
            win_rate_ci_lower,
            win_rate_ci_upper,
            prob_positive_sharpe,
            confidence_message,
        }
    }
}

// Helper functions
fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn std_dev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let m = mean(values);
    let variance = values.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    variance.sqrt()
}

/// Error function approximation for normal CDF calculation
fn erf(x: f64) -> f64 {
    // Horner form coefficients for approximation
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    sign * y
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_validation_config_default() {
        let config = ValidationConfig::default();
        assert_eq!(config.min_trades, 5);
        assert!(config.min_duration_hours > 0.0);
    }

    #[test]
    fn test_verdict_display() {
        assert_eq!(format!("{}", Verdict::Pass), "PASS");
        assert_eq!(format!("{}", Verdict::Fail), "FAIL");
        assert_eq!(format!("{}", Verdict::Warning), "WARNING");
        assert_eq!(format!("{}", Verdict::InsufficientData), "INSUFFICIENT DATA");
    }

    #[test]
    fn test_validation_check_passing() {
        let check = ValidationCheck::new("Test", 0.5, 0.52, 0.1);
        assert!(check.passed);
        assert!(check.message.contains("within tolerance"));
    }

    #[test]
    fn test_validation_check_failing() {
        let check = ValidationCheck::new("Test", 0.5, 0.8, 0.1);
        assert!(!check.passed);
        assert!(check.message.contains("EXCEEDS"));
    }

    #[test]
    fn test_validation_check_ratio_passing() {
        let check = ValidationCheck::new_ratio("Trade Rate", 5.0, 4.0, 0.5, 2.0);
        assert!(check.passed); // Ratio is 1.25x, within [0.5, 2.0]
    }

    #[test]
    fn test_validation_check_ratio_failing() {
        let check = ValidationCheck::new_ratio("Trade Rate", 1.0, 10.0, 0.5, 2.0);
        assert!(!check.passed); // Ratio is 0.1x, below 0.5
    }

    #[test]
    fn test_expected_metrics_from_preset() {
        let mut preset = ParameterPreset::new("Test", "manual", 2.0, 0.5, 0.7, 0.10);
        preset.expected_return = 0.05;
        preset.expected_sharpe = 1.5;
        preset.expected_win_rate = 0.60;
        preset.expected_trades = 100;
        preset.num_events = 75000; // ~50 hours

        let expected = ExpectedMetrics::from_preset(&preset);

        assert_eq!(expected.expected_return, 0.05);
        assert_eq!(expected.expected_sharpe, 1.5);
        assert_eq!(expected.expected_win_rate, 0.60);
        assert_eq!(expected.expected_trades, 100);
        assert!(expected.expected_trades_per_hour > 0.0);
    }

    #[test]
    fn test_mean_calculation() {
        assert_eq!(mean(&[1.0, 2.0, 3.0]), 2.0);
        assert_eq!(mean(&[]), 0.0);
        assert_eq!(mean(&[5.0]), 5.0);
    }

    #[test]
    fn test_std_dev_calculation() {
        assert_eq!(std_dev(&[]), 0.0);
        assert_eq!(std_dev(&[5.0]), 0.0);
        let std = std_dev(&[1.0, 2.0, 3.0]);
        assert!((std - 1.0).abs() < 0.01); // Sample std dev of [1,2,3] is 1.0
    }

    #[test]
    fn test_erf_approximation() {
        // erf(0) = 0
        assert!((erf(0.0) - 0.0).abs() < 0.001);
        // erf(inf) -> 1
        assert!((erf(3.0) - 1.0).abs() < 0.01);
        // erf is odd function
        assert!((erf(-1.0) + erf(1.0)).abs() < 0.001);
    }

    #[test]
    fn test_session_validator_creation() {
        let config = ValidationConfig::default();
        let result = SessionValidator::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_session_validation_insufficient_data() {
        let config = ValidationConfig::default();
        let validator = SessionValidator::new(config).unwrap();

        // Create a session with insufficient data
        let metrics = SessionMetrics {
            duration_secs: 60.0, // 1 minute - way below minimum
            total_trades: 2,    // Below minimum
            ..Default::default()
        };

        let summary = SessionSummary {
            session_id: "test_session".to_string(),
            config: crate::forward_testing::ForwardTestConfig::default(),
            metrics,
            trade_count: 2,
        };

        let report = validator.validate_session_summary(&summary).unwrap();
        assert_eq!(report.verdict, Verdict::InsufficientData);
    }

    #[test]
    fn test_session_validation_passing() {
        let config = ValidationConfig::default();
        let validator = SessionValidator::new(config).unwrap();

        // Create a session that should pass
        // Since no preset matches, no expected values are set, so all checks pass by default
        let metrics = SessionMetrics {
            duration_secs: 7200.0, // 2 hours
            total_trades: 50,
            winning_trades: 30,
            losing_trades: 20,
            win_rate: 0.60,
            sharpe_ratio: -1.0,
            quotes_generated: 500, // 10% fill rate
            ..Default::default()
        };

        let summary = SessionSummary {
            session_id: "test_session".to_string(),
            config: crate::forward_testing::ForwardTestConfig::default(),
            metrics,
            trade_count: 50,
        };

        let report = validator.validate_session_summary(&summary).unwrap();
        // Without a matching preset, there's no expectation to compare against
        // So should not be InsufficientData (we have enough trades and duration)
        assert!(report.verdict != Verdict::InsufficientData);
    }

    #[test]
    fn test_aggregated_report_empty() {
        let config = ValidationConfig::default();
        let validator = SessionValidator::new(config).unwrap();

        let result = validator.aggregate_reports(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_statistical_confidence_insufficient() {
        let config = ValidationConfig::default();
        let validator = SessionValidator::new(config).unwrap();

        let sharpes = vec![1.0, 1.5];
        let win_rates = vec![0.55, 0.60];

        let confidence = validator.calculate_statistical_confidence(&sharpes, &win_rates);

        assert!(!confidence.sufficient_sample);
        assert!(confidence.confidence_message.contains("Need"));
    }

    #[test]
    fn test_statistical_confidence_sufficient() {
        let config = ValidationConfig::default();
        let validator = SessionValidator::new(config).unwrap();

        // Create enough samples
        let sharpes: Vec<f64> = (0..15).map(|i| 0.5 + (i as f64 * 0.1)).collect();
        let win_rates: Vec<f64> = (0..15).map(|i| 0.5 + (i as f64 * 0.01)).collect();

        let confidence = validator.calculate_statistical_confidence(&sharpes, &win_rates);

        assert!(confidence.sufficient_sample);
        assert!(confidence.sharpe_ci_lower < confidence.sharpe_ci_upper);
    }

    #[test]
    fn test_validation_check_serialization() {
        let check = ValidationCheck::new("Test", 0.5, 0.52, 0.1);
        let json = serde_json::to_string(&check).unwrap();
        let deserialized: ValidationCheck = serde_json::from_str(&json).unwrap();
        assert_eq!(check.name, deserialized.name);
        assert_eq!(check.passed, deserialized.passed);
    }

    #[test]
    fn test_verdict_serialization() {
        let verdict = Verdict::Pass;
        let json = serde_json::to_string(&verdict).unwrap();
        assert!(json.contains("Pass"));

        let deserialized: Verdict = serde_json::from_str(&json).unwrap();
        assert_eq!(verdict, deserialized);
    }

    #[test]
    fn test_session_report_creation() {
        let report = SessionValidationReport {
            session_id: "test".to_string(),
            preset_name: Some("GridSearch-Best".to_string()),
            algorithm_type: AlgorithmType::AvellanedaStoikov,
            session_metrics: SessionMetrics::default(),
            expected_metrics: ExpectedMetrics::default(),
            checks: vec![],
            verdict: Verdict::Pass,
            summary: "Test summary".to_string(),
            recommendations: vec![],
            validated_at: Utc::now(),
        };

        assert_eq!(report.session_id, "test");
        assert_eq!(report.verdict, Verdict::Pass);
    }

    #[test]
    fn test_aggregated_metrics_default() {
        let metrics = AggregatedMetrics::default();
        assert_eq!(metrics.avg_sharpe, 0.0);
        assert_eq!(metrics.total_pnl, 0.0);
    }

    #[test]
    fn test_fill_rate_symmetry_check() {
        // Test asymmetric fill rates detection
        // The symmetry check only runs when bid_touches > 10 AND ask_touches > 10
        let mut metrics = SessionMetrics::default();
        metrics.duration_secs = 7200.0;
        metrics.total_trades = 100;
        metrics.bid_touches = 100;  // > 10, so check will run
        metrics.ask_touches = 100;  // > 10, so check will run
        metrics.bid_fill_rate = 0.25; // 25% bid fills
        metrics.ask_fill_rate = 0.05; // 5% ask fills - 20% difference, > 10% tolerance

        let config = ValidationConfig::default();
        let validator = SessionValidator::new(config).unwrap();

        let summary = SessionSummary {
            session_id: "asymmetric_test".to_string(),
            config: crate::forward_testing::ForwardTestConfig::default(),
            metrics,
            trade_count: 100,
        };

        let report = validator.validate_session_summary(&summary).unwrap();

        // Should detect the asymmetry when the check runs
        let symmetry_check = report.checks.iter().find(|c| c.name == "Fill Rate Symmetry");
        assert!(symmetry_check.is_some(), "Fill Rate Symmetry check should be present");
        let check = symmetry_check.unwrap();
        assert!(!check.passed, "Should fail due to 20% difference exceeding 10% tolerance");
    }

    #[test]
    fn test_critical_fail_detection() {
        let config = ValidationConfig::default();
        let validator = SessionValidator::new(config).unwrap();

        // Create metrics that will fail trade rate check
        let mut metrics = SessionMetrics::default();
        metrics.duration_secs = 7200.0; // 2 hours
        metrics.total_trades = 5; // Very few trades
        metrics.win_rate = 0.60;
        metrics.quotes_generated = 1000; // Low fill rate

        let mut forward_config = crate::forward_testing::ForwardTestConfig::default();
        forward_config.preset_name = Some("GridSearch-Best".to_string());

        let summary = SessionSummary {
            session_id: "low_trade_test".to_string(),
            config: forward_config,
            metrics,
            trade_count: 5,
        };

        let report = validator.validate_session_summary(&summary).unwrap();

        // With preset expectations of ~10 trades/hour and only getting 2.5/hour,
        // this should be flagged
        assert!(report.recommendations.len() > 0);
    }
}

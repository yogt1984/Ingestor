//! 4-Week Validation Campaign Infrastructure
//!
//! Orchestrates multi-session paper trading over 4 weeks to validate
//! strategy performance before live deployment.
//!
//! # Purpose
//!
//! A Validation Campaign runs a strategy against live market data over 4 weeks to:
//! 1. **Calibrate fill rate** - Compare backtest assumption (e.g., 10%) vs actual
//! 2. **Verify edge persists** - Ensure backtest Sharpe/returns translate to live
//! 3. **Detect regime sensitivity** - See performance across market conditions
//! 4. **Build confidence** - Accumulate 400+ trades for statistical significance
//! 5. **Gate progression** - Provide go/no-go decision for live trading
//!
//! # Architecture
//!
//! ```text
//! ValidationCampaign
//!     │
//!     ├── Week 1 ──┬── Day 1: SessionResult
//!     │            ├── Day 2: SessionResult
//!     │            └── ...
//!     │            └── WeeklySummary (Gate 1)
//!     │
//!     ├── Week 2 ──┬── Day 8: SessionResult
//!     │            └── WeeklySummary (Gate 2)
//!     │
//!     ├── Week 3 ──┬── SessionResults...
//!     │            └── WeeklySummary (Gate 3)
//!     │
//!     └── Week 4 ──┬── SessionResults...
//!                  └── Final CampaignReport
//!                       ↓
//!                  ValidationVerdict (GoLive | Recalibrate | Reject)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! let config = CampaignConfig {
//!     preset_name: "GridSearch-Best".to_string(),
//!     target_weeks: 4,
//!     session_hours_per_day: 8.0,
//!     ..Default::default()
//! };
//!
//! let mut campaign = ValidationCampaign::new(config)?;
//! campaign.start()?;
//!
//! // Add session results as they complete
//! campaign.add_session(session_result)?;
//!
//! // Check weekly gates
//! let gate = campaign.check_weekly_gate()?;
//!
//! // Get final verdict
//! let report = campaign.finalize()?;
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail, Context};
use chrono::{DateTime, Utc, Duration as ChronoDuration, NaiveDate};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::backtest::session_runner::{SessionResult, SessionState};
use crate::presets::ParameterPreset;

// ============================================================================
// Configuration
// ============================================================================

/// Campaign configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignConfig {
    /// Preset name to validate
    pub preset_name: String,
    /// Target duration in weeks (default 4)
    pub target_weeks: u8,
    /// Session duration per day in hours
    pub session_hours_per_day: f64,
    /// Minimum sessions per week for valid week
    pub min_sessions_per_week: u8,
    /// Trading symbol
    pub symbol: String,
    /// Output directory for campaign data
    pub output_dir: PathBuf,
    /// Expected fill rate from backtest (for comparison)
    pub expected_fill_rate: f64,
    /// Expected Sharpe from backtest
    pub expected_sharpe: f64,
    /// Expected return from backtest
    pub expected_return: f64,
    /// Validation gates configuration
    pub gates: ValidationGates,
}

impl Default for CampaignConfig {
    fn default() -> Self {
        Self {
            preset_name: String::new(),
            target_weeks: 4,
            session_hours_per_day: 8.0,
            min_sessions_per_week: 5,
            symbol: "BTCUSDT".to_string(),
            output_dir: PathBuf::from("./data/campaigns"),
            expected_fill_rate: 0.10,
            expected_sharpe: 1.0,
            expected_return: 0.05,
            gates: ValidationGates::default(),
        }
    }
}

impl CampaignConfig {
    /// Create config from a preset
    pub fn from_preset(preset: &ParameterPreset) -> Self {
        Self {
            preset_name: preset.name.clone(),
            expected_fill_rate: preset.fill_prob_assumption,
            expected_sharpe: preset.expected_sharpe,
            expected_return: preset.expected_return,
            ..Default::default()
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if self.preset_name.is_empty() {
            bail!("Preset name cannot be empty");
        }
        if self.target_weeks == 0 || self.target_weeks > 12 {
            bail!("Target weeks must be between 1 and 12");
        }
        if self.session_hours_per_day <= 0.0 || self.session_hours_per_day > 24.0 {
            bail!("Session hours per day must be between 0 and 24");
        }
        if self.min_sessions_per_week == 0 || self.min_sessions_per_week > 7 {
            bail!("Min sessions per week must be between 1 and 7");
        }
        if self.expected_fill_rate <= 0.0 || self.expected_fill_rate > 1.0 {
            bail!("Expected fill rate must be between 0 and 1");
        }
        Ok(())
    }
}

/// Validation gate thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationGates {
    // Minimum thresholds to continue (FAIL if below)
    /// Minimum trades per week
    pub min_weekly_trades: usize,
    /// Minimum fill rate ratio vs expected (e.g., 0.5 = at least 50% of expected)
    pub min_fill_rate_ratio: f64,
    /// Maximum drawdown percentage
    pub max_drawdown_pct: f64,
    /// Minimum win rate
    pub min_win_rate: f64,

    // Warning thresholds (continue but flag)
    /// Fill rate warning ratio
    pub fill_rate_warning_ratio: f64,
    /// Sharpe warning threshold
    pub sharpe_warning: f64,
    /// PnL vs expected warning ratio
    pub pnl_warning_ratio: f64,
}

impl Default for ValidationGates {
    fn default() -> Self {
        Self {
            // Fail thresholds
            min_weekly_trades: 50,
            min_fill_rate_ratio: 0.5,
            max_drawdown_pct: 5.0,
            min_win_rate: 0.40,
            // Warning thresholds
            fill_rate_warning_ratio: 0.7,
            sharpe_warning: 0.5,
            pnl_warning_ratio: 0.6,
        }
    }
}

impl ValidationGates {
    /// Strict gates for high-confidence validation
    pub fn strict() -> Self {
        Self {
            min_weekly_trades: 100,
            min_fill_rate_ratio: 0.7,
            max_drawdown_pct: 3.0,
            min_win_rate: 0.50,
            fill_rate_warning_ratio: 0.85,
            sharpe_warning: 1.0,
            pnl_warning_ratio: 0.8,
        }
    }

    /// Relaxed gates for exploratory validation
    pub fn relaxed() -> Self {
        Self {
            min_weekly_trades: 25,
            min_fill_rate_ratio: 0.3,
            max_drawdown_pct: 10.0,
            min_win_rate: 0.35,
            fill_rate_warning_ratio: 0.5,
            sharpe_warning: 0.0,
            pnl_warning_ratio: 0.4,
        }
    }
}

// ============================================================================
// Campaign Status and State
// ============================================================================

/// Campaign status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CampaignStatus {
    /// Not yet started
    Pending,
    /// Actively running
    Running,
    /// Temporarily paused
    Paused,
    /// Completed all weeks
    Completed,
    /// Stopped early due to gate failure
    Failed,
    /// Manually stopped
    Stopped,
}

impl CampaignStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Pending | Self::Running | Self::Paused)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Stopped)
    }
}

/// Gate check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GateResult {
    /// All metrics acceptable
    Pass,
    /// Concerning metrics but continue
    Warning { reasons: Vec<String> },
    /// Stop campaign
    Fail { reasons: Vec<String> },
}

impl GateResult {
    pub fn is_pass(&self) -> bool {
        matches!(self, Self::Pass)
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, Self::Fail { .. })
    }

    pub fn reasons(&self) -> Vec<String> {
        match self {
            Self::Pass => vec![],
            Self::Warning { reasons } | Self::Fail { reasons } => reasons.clone(),
        }
    }
}

/// Final campaign verdict
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationVerdict {
    /// Ready for live trading (all gates passed)
    GoLive,
    /// Recalibrate fill assumptions and re-run
    Recalibrate,
    /// Strategy doesn't work in live conditions
    Reject,
    /// Campaign incomplete (not enough data)
    Incomplete,
}

impl ValidationVerdict {
    pub fn description(&self) -> &'static str {
        match self {
            Self::GoLive => "Ready for live trading at 0.1x target size",
            Self::Recalibrate => "Adjust fill rate assumptions and re-validate",
            Self::Reject => "Strategy does not translate to live conditions",
            Self::Incomplete => "Insufficient data to make a determination",
        }
    }
}

// ============================================================================
// Metrics Aggregation
// ============================================================================

/// Daily aggregated metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyMetrics {
    /// Date
    pub date: NaiveDate,
    /// Session IDs for this day
    pub session_ids: Vec<String>,
    /// Number of sessions
    pub session_count: usize,
    /// Total trades
    pub trades: usize,
    /// Total PnL
    pub pnl: f64,
    /// Win count
    pub wins: usize,
    /// Loss count
    pub losses: usize,
    /// Fill rate (fills / opportunities)
    pub fill_rate: f64,
    /// Total volume
    pub volume: f64,
    /// Max drawdown
    pub max_drawdown: f64,
    /// Duration in hours
    pub duration_hours: f64,
}

impl Default for DailyMetrics {
    fn default() -> Self {
        Self {
            date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            session_ids: vec![],
            session_count: 0,
            trades: 0,
            pnl: 0.0,
            wins: 0,
            losses: 0,
            fill_rate: 0.0,
            volume: 0.0,
            max_drawdown: 0.0,
            duration_hours: 0.0,
        }
    }
}

impl DailyMetrics {
    /// Calculate win rate
    pub fn win_rate(&self) -> f64 {
        let total = self.wins + self.losses;
        if total == 0 {
            0.0
        } else {
            self.wins as f64 / total as f64
        }
    }

    /// Create from a single session result
    pub fn from_session(date: NaiveDate, session: &SessionResult) -> Self {
        let metrics = &session.summary.metrics;
        let trades = metrics.total_trades as usize;
        let pnl = metrics.net_pnl.to_f64().unwrap_or(0.0);

        // Estimate wins/losses from PnL direction (simplified)
        // In production, we'd track individual trade outcomes
        let (wins, losses) = if trades > 0 {
            if pnl >= 0.0 {
                ((trades as f64 * 0.6) as usize, (trades as f64 * 0.4) as usize)
            } else {
                ((trades as f64 * 0.4) as usize, (trades as f64 * 0.6) as usize)
            }
        } else {
            (0, 0)
        };

        Self {
            date,
            session_ids: vec![session.summary.session_id.clone()],
            session_count: 1,
            trades,
            pnl,
            wins,
            losses,
            fill_rate: 0.0, // Will be set by caller with actual fill data
            volume: metrics.total_volume.to_f64().unwrap_or(0.0),
            max_drawdown: metrics.max_drawdown,
            duration_hours: metrics.duration_secs / 3600.0,
        }
    }

    /// Merge another day's metrics (for same date)
    pub fn merge(&mut self, other: &DailyMetrics) {
        self.session_ids.extend(other.session_ids.clone());
        self.session_count += other.session_count;
        self.trades += other.trades;
        self.pnl += other.pnl;
        self.wins += other.wins;
        self.losses += other.losses;
        self.volume += other.volume;
        self.max_drawdown = self.max_drawdown.max(other.max_drawdown);
        self.duration_hours += other.duration_hours;
        // Fill rate needs recalculation after merge
    }
}

/// Weekly aggregated metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeeklyMetrics {
    /// Week number (1-based)
    pub week_number: u8,
    /// Start date of week
    pub start_date: NaiveDate,
    /// End date of week
    pub end_date: NaiveDate,
    /// Daily metrics
    pub days: Vec<DailyMetrics>,
    /// Number of valid sessions
    pub session_count: usize,

    // Aggregated metrics
    /// Total trades
    pub total_trades: usize,
    /// Cumulative PnL
    pub cumulative_pnl: f64,
    /// Average fill rate
    pub avg_fill_rate: f64,
    /// Weekly Sharpe ratio (annualized)
    pub weekly_sharpe: f64,
    /// Win rate
    pub win_rate: f64,
    /// Max drawdown
    pub max_drawdown: f64,
    /// Total volume
    pub total_volume: f64,
    /// Total duration in hours
    pub total_hours: f64,

    // Comparison to expected
    /// PnL vs expected ratio
    pub pnl_vs_expected: f64,
    /// Fill rate vs expected ratio
    pub fill_rate_vs_expected: f64,

    // Gate result
    pub gate_result: GateResult,
}

impl Default for WeeklyMetrics {
    fn default() -> Self {
        Self {
            week_number: 0,
            start_date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2000, 1, 7).unwrap(),
            days: vec![],
            session_count: 0,
            total_trades: 0,
            cumulative_pnl: 0.0,
            avg_fill_rate: 0.0,
            weekly_sharpe: 0.0,
            win_rate: 0.0,
            max_drawdown: 0.0,
            total_volume: 0.0,
            total_hours: 0.0,
            pnl_vs_expected: 0.0,
            fill_rate_vs_expected: 0.0,
            gate_result: GateResult::Pass,
        }
    }
}

impl WeeklyMetrics {
    /// Aggregate from daily metrics
    pub fn from_days(
        week_number: u8,
        days: Vec<DailyMetrics>,
        expected_fill_rate: f64,
        expected_weekly_pnl: f64,
    ) -> Self {
        let mut weekly = Self {
            week_number,
            days: days.clone(),
            ..Default::default()
        };

        if days.is_empty() {
            return weekly;
        }

        // Set date range
        weekly.start_date = days.iter().map(|d| d.date).min().unwrap();
        weekly.end_date = days.iter().map(|d| d.date).max().unwrap();

        // Aggregate metrics
        let mut total_wins = 0usize;
        let mut total_losses = 0usize;
        let mut daily_returns: Vec<f64> = Vec::new();

        for day in &days {
            weekly.session_count += day.session_count;
            weekly.total_trades += day.trades;
            weekly.cumulative_pnl += day.pnl;
            total_wins += day.wins;
            total_losses += day.losses;
            weekly.total_volume += day.volume;
            weekly.max_drawdown = weekly.max_drawdown.max(day.max_drawdown);
            weekly.total_hours += day.duration_hours;

            // Track daily return for Sharpe calculation
            if day.duration_hours > 0.0 {
                daily_returns.push(day.pnl);
            }
        }

        // Calculate win rate
        let total_outcomes = total_wins + total_losses;
        weekly.win_rate = if total_outcomes > 0 {
            total_wins as f64 / total_outcomes as f64
        } else {
            0.0
        };

        // Calculate average fill rate
        let fill_rates: Vec<f64> = days.iter()
            .filter(|d| d.fill_rate > 0.0)
            .map(|d| d.fill_rate)
            .collect();
        weekly.avg_fill_rate = if !fill_rates.is_empty() {
            fill_rates.iter().sum::<f64>() / fill_rates.len() as f64
        } else {
            0.0
        };

        // Calculate weekly Sharpe (simplified)
        weekly.weekly_sharpe = calculate_sharpe(&daily_returns);

        // Calculate comparison ratios
        if expected_fill_rate > 0.0 {
            weekly.fill_rate_vs_expected = weekly.avg_fill_rate / expected_fill_rate;
        }
        if expected_weekly_pnl.abs() > 1e-10 {
            weekly.pnl_vs_expected = weekly.cumulative_pnl / expected_weekly_pnl;
        }

        weekly
    }

    /// Check weekly gate
    pub fn check_gate(&mut self, gates: &ValidationGates) {
        let mut fail_reasons = Vec::new();
        let mut warn_reasons = Vec::new();

        // Check fail conditions
        if self.total_trades < gates.min_weekly_trades {
            fail_reasons.push(format!(
                "Insufficient trades: {} < {} minimum",
                self.total_trades, gates.min_weekly_trades
            ));
        }

        if self.fill_rate_vs_expected > 0.0 && self.fill_rate_vs_expected < gates.min_fill_rate_ratio {
            fail_reasons.push(format!(
                "Fill rate too low: {:.1}% of expected (min {:.1}%)",
                self.fill_rate_vs_expected * 100.0,
                gates.min_fill_rate_ratio * 100.0
            ));
        }

        if self.max_drawdown > gates.max_drawdown_pct {
            fail_reasons.push(format!(
                "Max drawdown exceeded: {:.2}% > {:.2}% limit",
                self.max_drawdown, gates.max_drawdown_pct
            ));
        }

        if self.total_trades > 0 && self.win_rate < gates.min_win_rate {
            fail_reasons.push(format!(
                "Win rate too low: {:.1}% < {:.1}% minimum",
                self.win_rate * 100.0,
                gates.min_win_rate * 100.0
            ));
        }

        // Check warning conditions
        if self.fill_rate_vs_expected > 0.0
            && self.fill_rate_vs_expected < gates.fill_rate_warning_ratio
            && self.fill_rate_vs_expected >= gates.min_fill_rate_ratio
        {
            warn_reasons.push(format!(
                "Fill rate below warning threshold: {:.1}% of expected",
                self.fill_rate_vs_expected * 100.0
            ));
        }

        if self.weekly_sharpe < gates.sharpe_warning && self.total_trades >= gates.min_weekly_trades {
            warn_reasons.push(format!(
                "Sharpe ratio below warning: {:.2} < {:.2}",
                self.weekly_sharpe, gates.sharpe_warning
            ));
        }

        if self.pnl_vs_expected > 0.0 && self.pnl_vs_expected < gates.pnl_warning_ratio {
            warn_reasons.push(format!(
                "PnL below warning threshold: {:.1}% of expected",
                self.pnl_vs_expected * 100.0
            ));
        }

        // Set gate result
        self.gate_result = if !fail_reasons.is_empty() {
            GateResult::Fail { reasons: fail_reasons }
        } else if !warn_reasons.is_empty() {
            GateResult::Warning { reasons: warn_reasons }
        } else {
            GateResult::Pass
        };
    }
}

/// Campaign-level aggregated metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignMetrics {
    /// Total sessions completed
    pub total_sessions: usize,
    /// Total weeks completed
    pub weeks_completed: u8,
    /// Total trades
    pub total_trades: usize,
    /// Total PnL
    pub total_pnl: f64,
    /// Overall Sharpe ratio
    pub overall_sharpe: f64,
    /// Overall win rate
    pub overall_win_rate: f64,
    /// Overall fill rate
    pub overall_fill_rate: f64,
    /// Max drawdown across campaign
    pub max_drawdown: f64,
    /// Total volume
    pub total_volume: f64,
    /// Total hours traded
    pub total_hours: f64,

    // Fill rate calibration
    /// Fill rate vs expected
    pub fill_rate_calibration: f64,
    /// Fill rate 95% CI lower bound
    pub fill_rate_ci_lower: f64,
    /// Fill rate 95% CI upper bound
    pub fill_rate_ci_upper: f64,

    // Statistical significance
    /// Probabilistic Sharpe Ratio
    pub psr: f64,
    /// Sharpe 95% CI lower bound
    pub sharpe_ci_lower: f64,
    /// Sharpe 95% CI upper bound
    pub sharpe_ci_upper: f64,

    // Comparison to backtest
    /// PnL vs expected ratio
    pub pnl_vs_expected: f64,
    /// Sharpe vs expected ratio
    pub sharpe_vs_expected: f64,
}

impl Default for CampaignMetrics {
    fn default() -> Self {
        Self {
            total_sessions: 0,
            weeks_completed: 0,
            total_trades: 0,
            total_pnl: 0.0,
            overall_sharpe: 0.0,
            overall_win_rate: 0.0,
            overall_fill_rate: 0.0,
            max_drawdown: 0.0,
            total_volume: 0.0,
            total_hours: 0.0,
            fill_rate_calibration: 0.0,
            fill_rate_ci_lower: 0.0,
            fill_rate_ci_upper: 0.0,
            psr: 0.0,
            sharpe_ci_lower: 0.0,
            sharpe_ci_upper: 0.0,
            pnl_vs_expected: 0.0,
            sharpe_vs_expected: 0.0,
        }
    }
}

impl CampaignMetrics {
    /// Aggregate from weekly metrics
    pub fn from_weeks(weeks: &[WeeklyMetrics], config: &CampaignConfig) -> Self {
        let mut metrics = Self::default();

        if weeks.is_empty() {
            return metrics;
        }

        let mut all_daily_returns: Vec<f64> = Vec::new();
        let mut total_wins = 0usize;
        let mut total_losses = 0usize;
        let mut fill_rates: Vec<f64> = Vec::new();

        for week in weeks {
            metrics.total_sessions += week.session_count;
            metrics.total_trades += week.total_trades;
            metrics.total_pnl += week.cumulative_pnl;
            metrics.total_volume += week.total_volume;
            metrics.total_hours += week.total_hours;
            metrics.max_drawdown = metrics.max_drawdown.max(week.max_drawdown);

            // Collect daily returns for Sharpe calculation
            for day in &week.days {
                if day.duration_hours > 0.0 {
                    all_daily_returns.push(day.pnl);
                }
                total_wins += day.wins;
                total_losses += day.losses;
            }

            if week.avg_fill_rate > 0.0 {
                fill_rates.push(week.avg_fill_rate);
            }
        }

        metrics.weeks_completed = weeks.len() as u8;

        // Calculate overall win rate
        let total_outcomes = total_wins + total_losses;
        metrics.overall_win_rate = if total_outcomes > 0 {
            total_wins as f64 / total_outcomes as f64
        } else {
            0.0
        };

        // Calculate overall fill rate
        metrics.overall_fill_rate = if !fill_rates.is_empty() {
            fill_rates.iter().sum::<f64>() / fill_rates.len() as f64
        } else {
            0.0
        };

        // Calculate overall Sharpe
        metrics.overall_sharpe = calculate_sharpe(&all_daily_returns);

        // Calculate fill rate calibration with CI
        if config.expected_fill_rate > 0.0 {
            metrics.fill_rate_calibration = metrics.overall_fill_rate / config.expected_fill_rate;

            // Simple CI estimation (assuming normal distribution)
            let n = fill_rates.len() as f64;
            if n > 1.0 {
                let mean = metrics.overall_fill_rate;
                let variance: f64 = fill_rates.iter()
                    .map(|x| (x - mean).powi(2))
                    .sum::<f64>() / (n - 1.0);
                let std_err = (variance / n).sqrt();
                let z = 1.96; // 95% CI
                metrics.fill_rate_ci_lower = (mean - z * std_err).max(0.0);
                metrics.fill_rate_ci_upper = (mean + z * std_err).min(1.0);
            }
        }

        // Calculate Sharpe CI and PSR
        if all_daily_returns.len() > 1 {
            let (ci_lower, ci_upper) = sharpe_confidence_interval(&all_daily_returns);
            metrics.sharpe_ci_lower = ci_lower;
            metrics.sharpe_ci_upper = ci_upper;

            // PSR: probability that true Sharpe > 0
            metrics.psr = calculate_psr(&all_daily_returns, 0.0);
        }

        // Calculate comparison ratios
        let expected_campaign_pnl = config.expected_return * config.target_weeks as f64 / 4.0;
        if expected_campaign_pnl.abs() > 1e-10 {
            metrics.pnl_vs_expected = metrics.total_pnl / expected_campaign_pnl;
        }
        if config.expected_sharpe.abs() > 1e-10 {
            metrics.sharpe_vs_expected = metrics.overall_sharpe / config.expected_sharpe;
        }

        metrics
    }
}

// ============================================================================
// Campaign Report
// ============================================================================

/// Final campaign report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignReport {
    /// Campaign ID
    pub campaign_id: String,
    /// Configuration used
    pub config: CampaignConfig,
    /// Campaign status
    pub status: CampaignStatus,
    /// Start time
    pub start_time: DateTime<Utc>,
    /// End time
    pub end_time: Option<DateTime<Utc>>,
    /// Weekly summaries
    pub weekly_summaries: Vec<WeeklyMetrics>,
    /// Campaign-level metrics
    pub campaign_metrics: CampaignMetrics,
    /// Final verdict
    pub verdict: ValidationVerdict,
    /// Verdict reasoning
    pub verdict_reasons: Vec<String>,
    /// Recommendations
    pub recommendations: Vec<String>,
}

impl CampaignReport {
    /// Generate recommendations based on metrics
    pub fn generate_recommendations(&mut self) {
        self.recommendations.clear();

        let metrics = &self.campaign_metrics;

        // Fill rate recommendations
        if metrics.fill_rate_calibration < 0.7 {
            self.recommendations.push(format!(
                "Fill rate is {:.1}% of expected. Consider reducing assumed fill probability from {:.1}% to {:.1}%",
                metrics.fill_rate_calibration * 100.0,
                self.config.expected_fill_rate * 100.0,
                metrics.overall_fill_rate * 100.0
            ));
        }

        // Sharpe recommendations
        if metrics.overall_sharpe < 0.5 {
            self.recommendations.push(
                "Sharpe ratio is below 0.5. Strategy may not have sufficient edge.".to_string()
            );
        } else if metrics.psr < 0.95 {
            self.recommendations.push(format!(
                "PSR is {:.1}%. Consider extending validation to build confidence.",
                metrics.psr * 100.0
            ));
        }

        // Trade count recommendations
        if metrics.total_trades < 400 {
            self.recommendations.push(format!(
                "Only {} trades recorded. Minimum 400 recommended for statistical significance.",
                metrics.total_trades
            ));
        }

        // Drawdown recommendations
        if metrics.max_drawdown > 3.0 {
            self.recommendations.push(format!(
                "Max drawdown of {:.2}% is concerning. Consider tighter risk limits.",
                metrics.max_drawdown
            ));
        }

        // Win rate recommendations
        if metrics.overall_win_rate < 0.5 {
            self.recommendations.push(format!(
                "Win rate of {:.1}% is below 50%. Verify trade execution quality.",
                metrics.overall_win_rate * 100.0
            ));
        }
    }

    /// Determine final verdict
    pub fn determine_verdict(&mut self) {
        self.verdict_reasons.clear();
        let metrics = &self.campaign_metrics;

        // Check minimum requirements
        if metrics.weeks_completed < self.config.target_weeks {
            self.verdict = ValidationVerdict::Incomplete;
            self.verdict_reasons.push(format!(
                "Only {}/{} weeks completed",
                metrics.weeks_completed, self.config.target_weeks
            ));
            return;
        }

        if metrics.total_trades < 200 {
            self.verdict = ValidationVerdict::Incomplete;
            self.verdict_reasons.push(format!(
                "Insufficient trades: {} < 200 minimum",
                metrics.total_trades
            ));
            return;
        }

        // Check for any failed gates
        let failed_weeks: Vec<u8> = self.weekly_summaries.iter()
            .filter(|w| w.gate_result.is_fail())
            .map(|w| w.week_number)
            .collect();

        if !failed_weeks.is_empty() {
            self.verdict = ValidationVerdict::Reject;
            self.verdict_reasons.push(format!(
                "Gate failures in weeks: {:?}",
                failed_weeks
            ));
            return;
        }

        // Check fill rate calibration
        if metrics.fill_rate_calibration < 0.5 {
            self.verdict = ValidationVerdict::Recalibrate;
            self.verdict_reasons.push(format!(
                "Fill rate significantly below expected: {:.1}% vs {:.1}%",
                metrics.overall_fill_rate * 100.0,
                self.config.expected_fill_rate * 100.0
            ));
            return;
        }

        // Check profitability
        if metrics.total_pnl < 0.0 {
            self.verdict = ValidationVerdict::Reject;
            self.verdict_reasons.push(format!(
                "Negative total PnL: {:.4}",
                metrics.total_pnl
            ));
            return;
        }

        // Check Sharpe
        if metrics.overall_sharpe < 0.5 {
            self.verdict = ValidationVerdict::Recalibrate;
            self.verdict_reasons.push(format!(
                "Sharpe ratio below threshold: {:.2} < 0.5",
                metrics.overall_sharpe
            ));
            return;
        }

        // Check statistical significance
        if metrics.psr < 0.90 {
            self.verdict = ValidationVerdict::Recalibrate;
            self.verdict_reasons.push(format!(
                "PSR below 90%: {:.1}%. Edge not statistically confirmed.",
                metrics.psr * 100.0
            ));
            return;
        }

        // All checks passed
        self.verdict = ValidationVerdict::GoLive;
        self.verdict_reasons.push("All validation gates passed".to_string());
        self.verdict_reasons.push(format!(
            "Sharpe: {:.2}, PSR: {:.1}%, Trades: {}",
            metrics.overall_sharpe,
            metrics.psr * 100.0,
            metrics.total_trades
        ));
    }
}

// ============================================================================
// Validation Campaign
// ============================================================================

/// Validation Campaign orchestrator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationCampaign {
    /// Unique campaign ID
    pub campaign_id: String,
    /// Configuration
    pub config: CampaignConfig,
    /// Current status
    pub status: CampaignStatus,
    /// Start time
    pub start_time: Option<DateTime<Utc>>,
    /// End time
    pub end_time: Option<DateTime<Utc>>,
    /// Current week (1-based)
    pub current_week: u8,
    /// All session results
    pub sessions: Vec<SessionResult>,
    /// Daily metrics
    pub daily_metrics: HashMap<NaiveDate, DailyMetrics>,
    /// Weekly metrics
    pub weekly_metrics: Vec<WeeklyMetrics>,
    /// Campaign directory
    pub campaign_dir: PathBuf,
}

impl ValidationCampaign {
    /// Create a new campaign
    pub fn new(config: CampaignConfig) -> Result<Self> {
        config.validate()?;

        let campaign_id = generate_campaign_id();
        let campaign_dir = config.output_dir.join(&campaign_id);

        Ok(Self {
            campaign_id,
            config,
            status: CampaignStatus::Pending,
            start_time: None,
            end_time: None,
            current_week: 0,
            sessions: Vec::new(),
            daily_metrics: HashMap::new(),
            weekly_metrics: Vec::new(),
            campaign_dir,
        })
    }

    /// Start the campaign
    pub fn start(&mut self) -> Result<()> {
        if self.status != CampaignStatus::Pending {
            bail!("Campaign already started");
        }

        self.status = CampaignStatus::Running;
        self.start_time = Some(Utc::now());
        self.current_week = 1;

        // Create campaign directory
        fs::create_dir_all(&self.campaign_dir)
            .context("Failed to create campaign directory")?;

        // Save initial checkpoint
        self.save_checkpoint()?;

        Ok(())
    }

    /// Pause the campaign
    pub fn pause(&mut self) -> Result<()> {
        if self.status != CampaignStatus::Running {
            bail!("Campaign is not running");
        }
        self.status = CampaignStatus::Paused;
        self.save_checkpoint()?;
        Ok(())
    }

    /// Resume the campaign
    pub fn resume(&mut self) -> Result<()> {
        if self.status != CampaignStatus::Paused {
            bail!("Campaign is not paused");
        }
        self.status = CampaignStatus::Running;
        self.save_checkpoint()?;
        Ok(())
    }

    /// Stop the campaign early
    pub fn stop(&mut self) -> Result<()> {
        if !self.status.is_active() {
            bail!("Campaign is not active");
        }
        self.status = CampaignStatus::Stopped;
        self.end_time = Some(Utc::now());
        self.save_checkpoint()?;
        Ok(())
    }

    /// Add a session result
    pub fn add_session(&mut self, session: SessionResult) -> Result<()> {
        if !self.status.is_active() {
            bail!("Campaign is not active");
        }

        // Only count valid sessions
        if session.final_state != SessionState::Completed {
            return Ok(());
        }

        // Determine date from session
        let date = session.summary.metrics.start_time
            .map(|dt| dt.date_naive())
            .unwrap_or_else(|| Utc::now().date_naive());

        // Update daily metrics
        let daily = DailyMetrics::from_session(date, &session);
        self.daily_metrics
            .entry(date)
            .and_modify(|existing| existing.merge(&daily))
            .or_insert(daily);

        // Store session
        self.sessions.push(session);

        // Check if we need to update week
        self.update_week()?;

        // Save checkpoint
        self.save_checkpoint()?;

        Ok(())
    }

    /// Update current week based on sessions
    fn update_week(&mut self) -> Result<()> {
        let start = match self.start_time {
            Some(s) => s,
            None => return Ok(()),
        };

        let now = Utc::now();
        let days_elapsed = (now - start).num_days();
        let new_week = ((days_elapsed / 7) + 1).min(self.config.target_weeks as i64) as u8;

        // If week changed, aggregate previous week
        if new_week > self.current_week {
            self.finalize_week(self.current_week)?;
            self.current_week = new_week;
        }

        // Check if campaign should complete
        if self.current_week > self.config.target_weeks {
            self.complete()?;
        }

        Ok(())
    }

    /// Finalize a week's metrics
    fn finalize_week(&mut self, week: u8) -> Result<()> {
        let start = self.start_time.ok_or_else(|| anyhow::anyhow!("Campaign not started"))?;

        // Calculate date range for this week
        let week_start = start + ChronoDuration::days((week as i64 - 1) * 7);
        let week_end = week_start + ChronoDuration::days(6);

        let week_start_date = week_start.date_naive();
        let week_end_date = week_end.date_naive();

        // Collect daily metrics for this week
        let days: Vec<DailyMetrics> = self.daily_metrics.iter()
            .filter(|(date, _)| **date >= week_start_date && **date <= week_end_date)
            .map(|(_, m)| m.clone())
            .collect();

        // Calculate expected weekly PnL
        let expected_weekly_pnl = self.config.expected_return / (self.config.target_weeks as f64);

        // Create weekly metrics
        let mut weekly = WeeklyMetrics::from_days(
            week,
            days,
            self.config.expected_fill_rate,
            expected_weekly_pnl,
        );

        // Check gate
        weekly.check_gate(&self.config.gates);

        // If gate failed, fail campaign
        if weekly.gate_result.is_fail() {
            self.status = CampaignStatus::Failed;
            self.end_time = Some(Utc::now());
        }

        self.weekly_metrics.push(weekly);

        Ok(())
    }

    /// Check weekly gate for current week
    pub fn check_weekly_gate(&self) -> Option<&GateResult> {
        self.weekly_metrics.last().map(|w| &w.gate_result)
    }

    /// Complete the campaign
    fn complete(&mut self) -> Result<()> {
        if self.status == CampaignStatus::Running {
            // Finalize last week if not done
            if self.weekly_metrics.len() < self.config.target_weeks as usize {
                self.finalize_week(self.current_week)?;
            }

            self.status = CampaignStatus::Completed;
            self.end_time = Some(Utc::now());
            self.save_checkpoint()?;
        }
        Ok(())
    }

    /// Force complete the campaign (for testing or early finish)
    pub fn force_complete(&mut self) -> Result<()> {
        // Finalize any remaining weeks
        while self.current_week <= self.config.target_weeks
            && self.weekly_metrics.len() < self.current_week as usize
        {
            self.finalize_week(self.current_week)?;
            self.current_week += 1;
        }

        self.status = CampaignStatus::Completed;
        self.end_time = Some(Utc::now());
        self.save_checkpoint()?;
        Ok(())
    }

    /// Generate final report
    pub fn generate_report(&self) -> CampaignReport {
        let campaign_metrics = CampaignMetrics::from_weeks(&self.weekly_metrics, &self.config);

        let mut report = CampaignReport {
            campaign_id: self.campaign_id.clone(),
            config: self.config.clone(),
            status: self.status,
            start_time: self.start_time.unwrap_or_else(Utc::now),
            end_time: self.end_time,
            weekly_summaries: self.weekly_metrics.clone(),
            campaign_metrics,
            verdict: ValidationVerdict::Incomplete,
            verdict_reasons: Vec::new(),
            recommendations: Vec::new(),
        };

        report.determine_verdict();
        report.generate_recommendations();

        report
    }

    /// Save checkpoint for crash recovery
    pub fn save_checkpoint(&self) -> Result<()> {
        let checkpoint_path = self.campaign_dir.join("checkpoint.json");

        // Ensure directory exists
        if let Some(parent) = checkpoint_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = serde_json::to_string_pretty(self)
            .context("Failed to serialize campaign")?;
        fs::write(&checkpoint_path, content)
            .context("Failed to write checkpoint")?;

        Ok(())
    }

    /// Load campaign from checkpoint
    pub fn load_checkpoint(campaign_dir: &Path) -> Result<Self> {
        let checkpoint_path = campaign_dir.join("checkpoint.json");
        let content = fs::read_to_string(&checkpoint_path)
            .context("Failed to read checkpoint")?;
        let campaign: Self = serde_json::from_str(&content)
            .context("Failed to deserialize campaign")?;
        Ok(campaign)
    }

    /// List all campaigns in output directory
    pub fn list_campaigns(output_dir: &Path) -> Result<Vec<(String, CampaignStatus)>> {
        let mut campaigns = Vec::new();

        if !output_dir.exists() {
            return Ok(campaigns);
        }

        for entry in fs::read_dir(output_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if let Ok(campaign) = Self::load_checkpoint(&path) {
                    campaigns.push((campaign.campaign_id, campaign.status));
                }
            }
        }

        Ok(campaigns)
    }

    /// Get progress summary
    pub fn progress(&self) -> CampaignProgress {
        CampaignProgress {
            campaign_id: self.campaign_id.clone(),
            status: self.status,
            current_week: self.current_week,
            target_weeks: self.config.target_weeks,
            sessions_completed: self.sessions.len(),
            total_trades: self.sessions.iter()
                .map(|s| s.summary.metrics.total_trades as usize)
                .sum(),
            days_elapsed: self.start_time
                .map(|s| (Utc::now() - s).num_days() as usize)
                .unwrap_or(0),
            last_gate_result: self.check_weekly_gate().cloned(),
        }
    }
}

/// Campaign progress summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignProgress {
    pub campaign_id: String,
    pub status: CampaignStatus,
    pub current_week: u8,
    pub target_weeks: u8,
    pub sessions_completed: usize,
    pub total_trades: usize,
    pub days_elapsed: usize,
    pub last_gate_result: Option<GateResult>,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate a unique campaign ID based on timestamp and random suffix
fn generate_campaign_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    // Use timestamp + a portion for uniqueness
    format!("c{:08x}", (timestamp & 0xFFFFFFFF) as u32)
}

/// Calculate Sharpe ratio from returns
fn calculate_sharpe(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }

    let n = returns.len() as f64;
    let mean: f64 = returns.iter().sum::<f64>() / n;
    let variance: f64 = returns.iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>() / (n - 1.0);
    let std_dev = variance.sqrt();

    if std_dev < 1e-10 {
        return 0.0;
    }

    // Annualize (assuming daily returns)
    let annualization_factor = (252.0_f64).sqrt();
    (mean / std_dev) * annualization_factor
}

/// Calculate Sharpe confidence interval using bootstrap
fn sharpe_confidence_interval(returns: &[f64]) -> (f64, f64) {
    if returns.len() < 10 {
        let sharpe = calculate_sharpe(returns);
        return (sharpe - 1.0, sharpe + 1.0);
    }

    // Simple analytical CI (Bailey-Lopez de Prado)
    let n = returns.len() as f64;
    let sharpe = calculate_sharpe(returns);

    // Standard error of Sharpe
    let se = ((1.0 + 0.5 * sharpe.powi(2)) / n).sqrt();
    let z = 1.96; // 95% CI

    (sharpe - z * se, sharpe + z * se)
}

/// Calculate Probabilistic Sharpe Ratio
fn calculate_psr(returns: &[f64], benchmark_sharpe: f64) -> f64 {
    if returns.len() < 10 {
        return 0.5;
    }

    let n = returns.len() as f64;
    let sharpe = calculate_sharpe(returns);

    // Skewness and kurtosis (simplified)
    let mean: f64 = returns.iter().sum::<f64>() / n;
    let variance: f64 = returns.iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>() / (n - 1.0);
    let std_dev = variance.sqrt();

    if std_dev < 1e-10 {
        return 0.5;
    }

    let skewness: f64 = returns.iter()
        .map(|r| ((r - mean) / std_dev).powi(3))
        .sum::<f64>() / n;

    let kurtosis: f64 = returns.iter()
        .map(|r| ((r - mean) / std_dev).powi(4))
        .sum::<f64>() / n - 3.0;

    // Standard error with skewness/kurtosis correction
    let se_squared = (1.0 - skewness * sharpe + (kurtosis / 4.0) * sharpe.powi(2)) / (n - 1.0);

    // Handle edge case where se_squared could be negative or very small
    if se_squared <= 0.0 {
        return if sharpe > benchmark_sharpe { 1.0 } else { 0.0 };
    }

    let se = se_squared.sqrt();

    if se < 1e-10 {
        return if sharpe > benchmark_sharpe { 1.0 } else { 0.0 };
    }

    // Z-score
    let z = (sharpe - benchmark_sharpe) / se;

    // Handle potential NaN from extreme z-scores
    if z.is_nan() || z.is_infinite() {
        return if sharpe > benchmark_sharpe { 1.0 } else { 0.0 };
    }

    // Normal CDF approximation
    normal_cdf(z)
}

/// Normal CDF approximation
fn normal_cdf(x: f64) -> f64 {
    // Abramowitz and Stegun approximation
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs() / (2.0_f64).sqrt();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    0.5 * (1.0 + sign * y)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forward_testing_core::{SessionSummary, ForwardTestConfig, SessionMetrics};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    // Helper to create a mock session result
    fn mock_session_result(
        session_id: &str,
        trades: u64,
        pnl: f64,
        duration_secs: f64,
        date: Option<DateTime<Utc>>,
    ) -> SessionResult {
        let win_rate = if pnl > 0.0 { 0.55 } else { 0.45 };
        let winning = (trades as f64 * win_rate) as u64;
        let losing = trades - winning;

        let metrics = SessionMetrics {
            start_time: date,
            duration_secs,
            total_trades: trades,
            buy_trades: trades / 2,
            sell_trades: trades / 2,
            total_volume: Decimal::from(trades) * dec!(0.001),
            gross_pnl: Decimal::try_from(pnl * 1.1).unwrap_or_default(),
            total_fees: Decimal::try_from(pnl * 0.1).unwrap_or_default(),
            net_pnl: Decimal::try_from(pnl).unwrap_or_default(),
            realized_pnl: Decimal::try_from(pnl * 0.8).unwrap_or_default(),
            unrealized_pnl: Decimal::try_from(pnl * 0.2).unwrap_or_default(),
            inventory: dec!(0),
            peak_inventory: dec!(0.05),
            max_drawdown: 0.5,
            peak_equity: Decimal::try_from(1.0 + pnl).unwrap_or_default(),
            win_rate,
            winning_trades: winning,
            losing_trades: losing,
            avg_trade_pnl: if trades > 0 {
                Decimal::try_from(pnl / trades as f64).unwrap_or_default()
            } else {
                dec!(0)
            },
            sharpe_ratio: if pnl > 0.0 { 1.5 } else { -0.5 },
            profit_factor: if pnl > 0.0 { 1.8 } else { 0.6 },
            avg_slippage_bps: 0.5,
            bid_fill_rate: 0.12,
            ask_fill_rate: 0.11,
            quotes_generated: trades * 10,
            bid_touches: trades * 5,
            ask_touches: trades * 5,
        };

        let summary = SessionSummary {
            session_id: session_id.to_string(),
            config: ForwardTestConfig::default(),
            metrics,
            trade_count: trades as usize,
        };

        SessionResult {
            summary,
            final_state: SessionState::Completed,
            events_processed: trades * 100,
            summary_path: PathBuf::from(format!("./data/sessions/{}.json", session_id)),
            trades_path: None,
            warnings: vec![],
            is_valid_for_validation: true,
        }
    }

    // ========================================================================
    // Configuration Tests
    // ========================================================================

    #[test]
    fn test_campaign_config_default() {
        let config = CampaignConfig::default();
        assert_eq!(config.target_weeks, 4);
        assert_eq!(config.min_sessions_per_week, 5);
        assert_eq!(config.expected_fill_rate, 0.10);
    }

    #[test]
    fn test_campaign_config_validation_empty_preset() {
        let config = CampaignConfig::default();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_campaign_config_validation_valid() {
        let mut config = CampaignConfig::default();
        config.preset_name = "test".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_campaign_config_validation_invalid_weeks() {
        let mut config = CampaignConfig::default();
        config.preset_name = "test".to_string();
        config.target_weeks = 0;
        assert!(config.validate().is_err());

        config.target_weeks = 13;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_campaign_config_validation_invalid_hours() {
        let mut config = CampaignConfig::default();
        config.preset_name = "test".to_string();
        config.session_hours_per_day = 25.0;
        assert!(config.validate().is_err());

        config.session_hours_per_day = 0.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_campaign_config_validation_invalid_fill_rate() {
        let mut config = CampaignConfig::default();
        config.preset_name = "test".to_string();
        config.expected_fill_rate = 0.0;
        assert!(config.validate().is_err());

        config.expected_fill_rate = 1.5;
        assert!(config.validate().is_err());
    }

    // ========================================================================
    // Validation Gates Tests
    // ========================================================================

    #[test]
    fn test_validation_gates_default() {
        let gates = ValidationGates::default();
        assert_eq!(gates.min_weekly_trades, 50);
        assert_eq!(gates.max_drawdown_pct, 5.0);
        assert_eq!(gates.min_win_rate, 0.40);
    }

    #[test]
    fn test_validation_gates_strict() {
        let gates = ValidationGates::strict();
        assert!(gates.min_weekly_trades > ValidationGates::default().min_weekly_trades);
        assert!(gates.max_drawdown_pct < ValidationGates::default().max_drawdown_pct);
    }

    #[test]
    fn test_validation_gates_relaxed() {
        let gates = ValidationGates::relaxed();
        assert!(gates.min_weekly_trades < ValidationGates::default().min_weekly_trades);
        assert!(gates.max_drawdown_pct > ValidationGates::default().max_drawdown_pct);
    }

    // ========================================================================
    // Campaign Status Tests
    // ========================================================================

    #[test]
    fn test_campaign_status_is_active() {
        assert!(CampaignStatus::Pending.is_active());
        assert!(CampaignStatus::Running.is_active());
        assert!(CampaignStatus::Paused.is_active());
        assert!(!CampaignStatus::Completed.is_active());
        assert!(!CampaignStatus::Failed.is_active());
        assert!(!CampaignStatus::Stopped.is_active());
    }

    #[test]
    fn test_campaign_status_is_terminal() {
        assert!(!CampaignStatus::Pending.is_terminal());
        assert!(!CampaignStatus::Running.is_terminal());
        assert!(!CampaignStatus::Paused.is_terminal());
        assert!(CampaignStatus::Completed.is_terminal());
        assert!(CampaignStatus::Failed.is_terminal());
        assert!(CampaignStatus::Stopped.is_terminal());
    }

    // ========================================================================
    // Gate Result Tests
    // ========================================================================

    #[test]
    fn test_gate_result_pass() {
        let result = GateResult::Pass;
        assert!(result.is_pass());
        assert!(!result.is_fail());
        assert!(result.reasons().is_empty());
    }

    #[test]
    fn test_gate_result_warning() {
        let result = GateResult::Warning {
            reasons: vec!["Low fill rate".to_string()],
        };
        assert!(!result.is_pass());
        assert!(!result.is_fail());
        assert_eq!(result.reasons().len(), 1);
    }

    #[test]
    fn test_gate_result_fail() {
        let result = GateResult::Fail {
            reasons: vec!["Drawdown exceeded".to_string()],
        };
        assert!(!result.is_pass());
        assert!(result.is_fail());
        assert_eq!(result.reasons().len(), 1);
    }

    // ========================================================================
    // Validation Verdict Tests
    // ========================================================================

    #[test]
    fn test_validation_verdict_descriptions() {
        assert!(!ValidationVerdict::GoLive.description().is_empty());
        assert!(!ValidationVerdict::Recalibrate.description().is_empty());
        assert!(!ValidationVerdict::Reject.description().is_empty());
        assert!(!ValidationVerdict::Incomplete.description().is_empty());
    }

    // ========================================================================
    // Daily Metrics Tests
    // ========================================================================

    #[test]
    fn test_daily_metrics_default() {
        let daily = DailyMetrics::default();
        assert_eq!(daily.trades, 0);
        assert_eq!(daily.pnl, 0.0);
        assert_eq!(daily.win_rate(), 0.0);
    }

    #[test]
    fn test_daily_metrics_win_rate() {
        let mut daily = DailyMetrics::default();
        daily.wins = 6;
        daily.losses = 4;
        assert!((daily.win_rate() - 0.6).abs() < 1e-10);
    }

    #[test]
    fn test_daily_metrics_win_rate_no_trades() {
        let daily = DailyMetrics::default();
        assert_eq!(daily.win_rate(), 0.0);
    }

    #[test]
    fn test_daily_metrics_from_session() {
        let session = mock_session_result("test-1", 10, 0.01, 3600.0, Some(Utc::now()));
        let date = Utc::now().date_naive();
        let daily = DailyMetrics::from_session(date, &session);

        assert_eq!(daily.trades, 10);
        assert!(daily.pnl > 0.0);
        assert!(daily.duration_hours > 0.0);
    }

    #[test]
    fn test_daily_metrics_merge() {
        let mut daily1 = DailyMetrics {
            date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            session_ids: vec!["s1".to_string()],
            session_count: 1,
            trades: 10,
            pnl: 0.01,
            wins: 6,
            losses: 4,
            fill_rate: 0.08,
            volume: 0.1,
            max_drawdown: 0.5,
            duration_hours: 4.0,
        };

        let daily2 = DailyMetrics {
            date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            session_ids: vec!["s2".to_string()],
            session_count: 1,
            trades: 15,
            pnl: 0.02,
            wins: 9,
            losses: 6,
            fill_rate: 0.10,
            volume: 0.15,
            max_drawdown: 0.3,
            duration_hours: 4.0,
        };

        daily1.merge(&daily2);

        assert_eq!(daily1.session_count, 2);
        assert_eq!(daily1.trades, 25);
        assert!((daily1.pnl - 0.03).abs() < 1e-10);
        assert_eq!(daily1.wins, 15);
        assert_eq!(daily1.losses, 10);
        assert_eq!(daily1.max_drawdown, 0.5);
        assert_eq!(daily1.duration_hours, 8.0);
    }

    // ========================================================================
    // Weekly Metrics Tests
    // ========================================================================

    #[test]
    fn test_weekly_metrics_default() {
        let weekly = WeeklyMetrics::default();
        assert_eq!(weekly.week_number, 0);
        assert_eq!(weekly.total_trades, 0);
    }

    #[test]
    fn test_weekly_metrics_from_days_empty() {
        let weekly = WeeklyMetrics::from_days(1, vec![], 0.10, 0.01);
        assert_eq!(weekly.week_number, 1);
        assert_eq!(weekly.total_trades, 0);
        assert_eq!(weekly.cumulative_pnl, 0.0);
    }

    #[test]
    fn test_weekly_metrics_from_days() {
        let days = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                session_ids: vec!["s1".to_string()],
                session_count: 1,
                trades: 20,
                pnl: 0.01,
                wins: 12,
                losses: 8,
                fill_rate: 0.08,
                volume: 0.2,
                max_drawdown: 0.3,
                duration_hours: 8.0,
            },
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
                session_ids: vec!["s2".to_string()],
                session_count: 1,
                trades: 25,
                pnl: 0.015,
                wins: 15,
                losses: 10,
                fill_rate: 0.09,
                volume: 0.25,
                max_drawdown: 0.4,
                duration_hours: 8.0,
            },
        ];

        let weekly = WeeklyMetrics::from_days(1, days, 0.10, 0.01);

        assert_eq!(weekly.week_number, 1);
        assert_eq!(weekly.session_count, 2);
        assert_eq!(weekly.total_trades, 45);
        assert!((weekly.cumulative_pnl - 0.025).abs() < 1e-10);
        assert_eq!(weekly.max_drawdown, 0.4);
        assert_eq!(weekly.total_hours, 16.0);
    }

    #[test]
    fn test_weekly_metrics_check_gate_pass() {
        // Need multiple days for Sharpe calculation to work correctly
        let days = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                session_ids: vec!["s1".to_string()],
                session_count: 1,
                trades: 30,
                pnl: 0.02,
                wins: 18,
                losses: 12,
                fill_rate: 0.08,
                volume: 0.3,
                max_drawdown: 1.0,
                duration_hours: 8.0,
            },
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
                session_ids: vec!["s2".to_string()],
                session_count: 1,
                trades: 30,
                pnl: 0.015,
                wins: 18,
                losses: 12,
                fill_rate: 0.09,
                volume: 0.3,
                max_drawdown: 0.5,
                duration_hours: 8.0,
            },
        ];

        let mut weekly = WeeklyMetrics::from_days(1, days, 0.10, 0.01);
        weekly.check_gate(&ValidationGates::default());

        // With good metrics (60 trades, 0.6 win rate, 0.85 fill rate ratio, low drawdown),
        // should pass all gates
        assert!(weekly.gate_result.is_pass(),
            "Expected pass but got: {:?}", weekly.gate_result);
    }

    #[test]
    fn test_weekly_metrics_check_gate_fail_trades() {
        let days = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                session_ids: vec!["s1".to_string()],
                session_count: 1,
                trades: 10, // Below minimum
                pnl: 0.01,
                wins: 6,
                losses: 4,
                fill_rate: 0.08,
                volume: 0.1,
                max_drawdown: 1.0,
                duration_hours: 8.0,
            },
        ];

        let mut weekly = WeeklyMetrics::from_days(1, days, 0.10, 0.01);
        weekly.check_gate(&ValidationGates::default());

        assert!(weekly.gate_result.is_fail());
    }

    #[test]
    fn test_weekly_metrics_check_gate_fail_drawdown() {
        let days = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                session_ids: vec!["s1".to_string()],
                session_count: 1,
                trades: 60,
                pnl: -0.01,
                wins: 24,
                losses: 36,
                fill_rate: 0.08,
                volume: 0.6,
                max_drawdown: 10.0, // Exceeds max
                duration_hours: 8.0,
            },
        ];

        let mut weekly = WeeklyMetrics::from_days(1, days, 0.10, 0.01);
        weekly.check_gate(&ValidationGates::default());

        assert!(weekly.gate_result.is_fail());
    }

    #[test]
    fn test_weekly_metrics_check_gate_warning() {
        let days = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                session_ids: vec!["s1".to_string()],
                session_count: 1,
                trades: 60,
                pnl: 0.005, // Low PnL
                wins: 33,
                losses: 27,
                fill_rate: 0.06, // Below warning but above fail
                volume: 0.6,
                max_drawdown: 1.0,
                duration_hours: 8.0,
            },
        ];

        let mut weekly = WeeklyMetrics::from_days(1, days, 0.10, 0.01);
        weekly.fill_rate_vs_expected = 0.6; // Set explicitly for test
        weekly.check_gate(&ValidationGates::default());

        // Should be warning, not fail
        assert!(!weekly.gate_result.is_pass());
        assert!(!weekly.gate_result.is_fail());
    }

    // ========================================================================
    // Campaign Metrics Tests
    // ========================================================================

    #[test]
    fn test_campaign_metrics_default() {
        let metrics = CampaignMetrics::default();
        assert_eq!(metrics.total_sessions, 0);
        assert_eq!(metrics.total_trades, 0);
        assert_eq!(metrics.overall_sharpe, 0.0);
    }

    #[test]
    fn test_campaign_metrics_from_weeks_empty() {
        let config = CampaignConfig {
            preset_name: "test".to_string(),
            ..Default::default()
        };
        let metrics = CampaignMetrics::from_weeks(&[], &config);
        assert_eq!(metrics.weeks_completed, 0);
        assert_eq!(metrics.total_trades, 0);
    }

    #[test]
    fn test_campaign_metrics_from_weeks() {
        let mut week1 = WeeklyMetrics::default();
        week1.week_number = 1;
        week1.session_count = 5;
        week1.total_trades = 100;
        week1.cumulative_pnl = 0.02;
        week1.avg_fill_rate = 0.08;
        week1.days = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                trades: 20,
                pnl: 0.004,
                wins: 12,
                losses: 8,
                duration_hours: 8.0,
                ..Default::default()
            },
        ];

        let mut week2 = WeeklyMetrics::default();
        week2.week_number = 2;
        week2.session_count = 5;
        week2.total_trades = 120;
        week2.cumulative_pnl = 0.025;
        week2.avg_fill_rate = 0.09;
        week2.days = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 8).unwrap(),
                trades: 24,
                pnl: 0.005,
                wins: 15,
                losses: 9,
                duration_hours: 8.0,
                ..Default::default()
            },
        ];

        let config = CampaignConfig {
            preset_name: "test".to_string(),
            expected_fill_rate: 0.10,
            expected_return: 0.05,
            target_weeks: 4,
            ..Default::default()
        };

        let metrics = CampaignMetrics::from_weeks(&[week1, week2], &config);

        assert_eq!(metrics.weeks_completed, 2);
        assert_eq!(metrics.total_sessions, 10);
        assert_eq!(metrics.total_trades, 220);
        assert!((metrics.total_pnl - 0.045).abs() < 1e-10);
    }

    // ========================================================================
    // Campaign Creation and Lifecycle Tests
    // ========================================================================

    #[test]
    fn test_campaign_creation() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            ..Default::default()
        };

        let campaign = ValidationCampaign::new(config).unwrap();

        assert_eq!(campaign.status, CampaignStatus::Pending);
        assert!(campaign.start_time.is_none());
        assert_eq!(campaign.current_week, 0);
        assert!(campaign.sessions.is_empty());
    }

    #[test]
    fn test_campaign_creation_invalid_config() {
        let config = CampaignConfig::default(); // Empty preset name
        assert!(ValidationCampaign::new(config).is_err());
    }

    #[test]
    fn test_campaign_start() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();

        assert_eq!(campaign.status, CampaignStatus::Running);
        assert!(campaign.start_time.is_some());
        assert_eq!(campaign.current_week, 1);
    }

    #[test]
    fn test_campaign_double_start() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();
        assert!(campaign.start().is_err());
    }

    #[test]
    fn test_campaign_pause_resume() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();

        campaign.pause().unwrap();
        assert_eq!(campaign.status, CampaignStatus::Paused);

        campaign.resume().unwrap();
        assert_eq!(campaign.status, CampaignStatus::Running);
    }

    #[test]
    fn test_campaign_pause_not_running() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        assert!(campaign.pause().is_err());
    }

    #[test]
    fn test_campaign_stop() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();
        campaign.stop().unwrap();

        assert_eq!(campaign.status, CampaignStatus::Stopped);
        assert!(campaign.end_time.is_some());
    }

    #[test]
    fn test_campaign_add_session_not_active() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        // First start and then stop the campaign to make it not active
        campaign.start().unwrap();
        campaign.stop().unwrap();

        let session = mock_session_result("test-1", 10, 0.01, 3600.0, Some(Utc::now()));

        // Now it should fail because campaign is stopped (not active)
        assert!(campaign.add_session(session).is_err());
    }

    #[test]
    fn test_campaign_add_session() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();

        let session = mock_session_result("test-1", 10, 0.01, 3600.0, Some(Utc::now()));
        campaign.add_session(session).unwrap();

        assert_eq!(campaign.sessions.len(), 1);
        assert_eq!(campaign.daily_metrics.len(), 1);
    }

    #[test]
    fn test_campaign_progress() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();

        let session = mock_session_result("test-1", 15, 0.01, 3600.0, Some(Utc::now()));
        campaign.add_session(session).unwrap();

        let progress = campaign.progress();

        assert_eq!(progress.status, CampaignStatus::Running);
        assert_eq!(progress.current_week, 1);
        assert_eq!(progress.sessions_completed, 1);
        assert_eq!(progress.total_trades, 15);
    }

    // ========================================================================
    // Campaign Report Tests
    // ========================================================================

    #[test]
    fn test_campaign_report_incomplete() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let campaign = ValidationCampaign::new(config).unwrap();
        let report = campaign.generate_report();

        assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    }

    #[test]
    fn test_campaign_report_determination() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            target_weeks: 2,
            expected_fill_rate: 0.10,
            expected_sharpe: 1.0,
            expected_return: 0.05,
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();

        // Add enough sessions for 2 weeks
        for i in 0..14 {
            let date = Utc::now() - ChronoDuration::days(13 - i);
            let session = mock_session_result(
                &format!("test-{}", i),
                30,
                0.003,
                28800.0, // 8 hours
                Some(date),
            );
            campaign.add_session(session).unwrap();
        }

        // Force completion
        campaign.force_complete().unwrap();

        let report = campaign.generate_report();

        // Should have some verdict (not incomplete since we have data)
        assert!(!report.verdict_reasons.is_empty());
    }

    // ========================================================================
    // Helper Function Tests
    // ========================================================================

    #[test]
    fn test_calculate_sharpe_empty() {
        assert_eq!(calculate_sharpe(&[]), 0.0);
    }

    #[test]
    fn test_calculate_sharpe_single() {
        assert_eq!(calculate_sharpe(&[0.01]), 0.0);
    }

    #[test]
    fn test_calculate_sharpe_positive() {
        let returns = vec![0.01, 0.02, 0.015, 0.01, 0.025];
        let sharpe = calculate_sharpe(&returns);
        assert!(sharpe > 0.0);
    }

    #[test]
    fn test_calculate_sharpe_negative() {
        let returns = vec![-0.01, -0.02, -0.015, -0.01, -0.025];
        let sharpe = calculate_sharpe(&returns);
        assert!(sharpe < 0.0);
    }

    #[test]
    fn test_calculate_sharpe_zero_variance() {
        let returns = vec![0.01, 0.01, 0.01, 0.01, 0.01];
        let sharpe = calculate_sharpe(&returns);
        assert_eq!(sharpe, 0.0);
    }

    #[test]
    fn test_sharpe_confidence_interval_short() {
        let returns = vec![0.01, 0.02];
        let (lower, upper) = sharpe_confidence_interval(&returns);
        assert!(lower < upper);
    }

    #[test]
    fn test_sharpe_confidence_interval() {
        let returns = vec![0.01, 0.02, 0.015, 0.01, 0.025, 0.02, 0.015, 0.01, 0.02, 0.015];
        let (lower, upper) = sharpe_confidence_interval(&returns);
        let sharpe = calculate_sharpe(&returns);

        assert!(lower <= sharpe);
        assert!(upper >= sharpe);
    }

    #[test]
    fn test_calculate_psr_short() {
        let returns = vec![0.01, 0.02];
        let psr = calculate_psr(&returns, 0.0);
        assert_eq!(psr, 0.5);
    }

    #[test]
    fn test_calculate_psr_positive() {
        // Use strongly positive returns with larger sample size for reliable PSR calculation
        let returns = vec![
            0.05, 0.04, 0.06, 0.03, 0.05, 0.04, 0.05, 0.06, 0.04, 0.05,
            0.05, 0.04, 0.06, 0.03, 0.05, 0.04, 0.05, 0.06, 0.04, 0.05,
        ];
        let psr = calculate_psr(&returns, 0.0);
        // With consistent positive returns, PSR should be > 0.5
        // But it's a statistical measure that depends on sample properties
        // At minimum, verify it's computed and in valid range
        assert!(psr >= 0.0 && psr <= 1.0,
            "PSR should be in [0, 1], got {}", psr);
        // With strongly positive mean and low variance, should be high
        assert!(psr > 0.9, "Expected PSR > 0.9 for consistent positive returns, got {}", psr);
    }

    #[test]
    fn test_normal_cdf_zero() {
        let cdf = normal_cdf(0.0);
        assert!((cdf - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_normal_cdf_positive() {
        let cdf = normal_cdf(1.96);
        assert!((cdf - 0.975).abs() < 0.01);
    }

    #[test]
    fn test_normal_cdf_negative() {
        let cdf = normal_cdf(-1.96);
        assert!((cdf - 0.025).abs() < 0.01);
    }

    // ========================================================================
    // Persistence Tests
    // ========================================================================

    #[test]
    fn test_campaign_checkpoint_save_load() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns_checkpoint"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();

        // Add a session
        let session = mock_session_result("test-1", 10, 0.01, 3600.0, Some(Utc::now()));
        campaign.add_session(session).unwrap();

        // Save and reload
        let campaign_dir = campaign.campaign_dir.clone();
        let loaded = ValidationCampaign::load_checkpoint(&campaign_dir).unwrap();

        assert_eq!(loaded.campaign_id, campaign.campaign_id);
        assert_eq!(loaded.status, campaign.status);
        assert_eq!(loaded.sessions.len(), campaign.sessions.len());

        // Cleanup
        let _ = fs::remove_dir_all(&campaign_dir);
    }

    #[test]
    fn test_campaign_checkpoint_load_nonexistent() {
        let result = ValidationCampaign::load_checkpoint(Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_campaign_skip_invalid_session() {
        let config = CampaignConfig {
            preset_name: "test-preset".to_string(),
            output_dir: PathBuf::from("/tmp/test_campaigns"),
            ..Default::default()
        };

        let mut campaign = ValidationCampaign::new(config).unwrap();
        campaign.start().unwrap();

        // Create a failed session
        let mut session = mock_session_result("test-1", 10, 0.01, 3600.0, Some(Utc::now()));
        session.final_state = SessionState::Failed;
        campaign.add_session(session).unwrap();

        // Should not be counted
        assert_eq!(campaign.sessions.len(), 0);
    }

    #[test]
    fn test_weekly_metrics_win_rate_calculation() {
        let days = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                trades: 100,
                pnl: 0.05,
                wins: 55,
                losses: 45,
                ..Default::default()
            },
        ];

        let weekly = WeeklyMetrics::from_days(1, days, 0.10, 0.01);
        assert!((weekly.win_rate - 0.55).abs() < 1e-10);
    }

    #[test]
    fn test_campaign_metrics_fill_rate_ci() {
        let mut week1 = WeeklyMetrics::default();
        week1.avg_fill_rate = 0.08;
        week1.days = vec![DailyMetrics { duration_hours: 8.0, ..Default::default() }];

        let mut week2 = WeeklyMetrics::default();
        week2.avg_fill_rate = 0.09;
        week2.days = vec![DailyMetrics { duration_hours: 8.0, ..Default::default() }];

        let mut week3 = WeeklyMetrics::default();
        week3.avg_fill_rate = 0.07;
        week3.days = vec![DailyMetrics { duration_hours: 8.0, ..Default::default() }];

        let config = CampaignConfig {
            preset_name: "test".to_string(),
            expected_fill_rate: 0.10,
            ..Default::default()
        };

        let metrics = CampaignMetrics::from_weeks(&[week1, week2, week3], &config);

        // Should have calculated CI
        assert!(metrics.fill_rate_ci_lower <= metrics.overall_fill_rate);
        assert!(metrics.fill_rate_ci_upper >= metrics.overall_fill_rate);
    }

    // ========================================================================
    // Report Generation Tests
    // ========================================================================

    #[test]
    fn test_report_recommendations_low_fill_rate() {
        let config = CampaignConfig {
            preset_name: "test".to_string(),
            expected_fill_rate: 0.10,
            ..Default::default()
        };

        let mut report = CampaignReport {
            campaign_id: "test".to_string(),
            config,
            status: CampaignStatus::Completed,
            start_time: Utc::now(),
            end_time: Some(Utc::now()),
            weekly_summaries: vec![],
            campaign_metrics: CampaignMetrics {
                fill_rate_calibration: 0.5, // Low
                overall_fill_rate: 0.05,
                ..Default::default()
            },
            verdict: ValidationVerdict::Incomplete,
            verdict_reasons: vec![],
            recommendations: vec![],
        };

        report.generate_recommendations();
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    fn test_report_recommendations_low_sharpe() {
        let config = CampaignConfig {
            preset_name: "test".to_string(),
            ..Default::default()
        };

        let mut report = CampaignReport {
            campaign_id: "test".to_string(),
            config,
            status: CampaignStatus::Completed,
            start_time: Utc::now(),
            end_time: Some(Utc::now()),
            weekly_summaries: vec![],
            campaign_metrics: CampaignMetrics {
                overall_sharpe: 0.3, // Low
                ..Default::default()
            },
            verdict: ValidationVerdict::Incomplete,
            verdict_reasons: vec![],
            recommendations: vec![],
        };

        report.generate_recommendations();
        assert!(report.recommendations.iter().any(|r| r.contains("Sharpe")));
    }

    #[test]
    fn test_report_verdict_reject_negative_pnl() {
        let config = CampaignConfig {
            preset_name: "test".to_string(),
            target_weeks: 1,
            ..Default::default()
        };

        let mut report = CampaignReport {
            campaign_id: "test".to_string(),
            config,
            status: CampaignStatus::Completed,
            start_time: Utc::now(),
            end_time: Some(Utc::now()),
            weekly_summaries: vec![WeeklyMetrics {
                week_number: 1,
                gate_result: GateResult::Pass,
                ..Default::default()
            }],
            campaign_metrics: CampaignMetrics {
                weeks_completed: 1,
                total_trades: 500,
                total_pnl: -0.05, // Negative
                fill_rate_calibration: 0.8,
                overall_sharpe: 0.6,
                psr: 0.95,
                ..Default::default()
            },
            verdict: ValidationVerdict::Incomplete,
            verdict_reasons: vec![],
            recommendations: vec![],
        };

        report.determine_verdict();
        assert_eq!(report.verdict, ValidationVerdict::Reject);
    }

    #[test]
    fn test_report_verdict_go_live() {
        let config = CampaignConfig {
            preset_name: "test".to_string(),
            target_weeks: 1,
            ..Default::default()
        };

        let mut report = CampaignReport {
            campaign_id: "test".to_string(),
            config,
            status: CampaignStatus::Completed,
            start_time: Utc::now(),
            end_time: Some(Utc::now()),
            weekly_summaries: vec![WeeklyMetrics {
                week_number: 1,
                gate_result: GateResult::Pass,
                ..Default::default()
            }],
            campaign_metrics: CampaignMetrics {
                weeks_completed: 1,
                total_trades: 500,
                total_pnl: 0.05,
                fill_rate_calibration: 0.8,
                overall_sharpe: 1.5,
                psr: 0.95,
                ..Default::default()
            },
            verdict: ValidationVerdict::Incomplete,
            verdict_reasons: vec![],
            recommendations: vec![],
        };

        report.determine_verdict();
        assert_eq!(report.verdict, ValidationVerdict::GoLive);
    }
}

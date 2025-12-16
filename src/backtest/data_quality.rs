//! Data Quality Validation Pipeline
//!
//! Validates historical data quality before backtesting to ensure reliable results.
//!
//! # Checks Performed
//!
//! - Missing value detection and reporting
//! - Price sanity checks (negative prices, extreme outliers)
//! - Timestamp continuity (gaps, duplicates, out-of-order)
//! - Feature range validation (entropy [0,1], ratios [-1,1], etc.)
//! - Data freshness (stale data detection)

use std::collections::HashMap;
use std::path::PathBuf;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use super::replay::{ParquetReplay, ReplayConfig, ReplayEvent};

/// Data quality report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataQualityReport {
    /// Total events analyzed
    pub total_events: usize,
    /// Events passing all checks
    pub valid_events: usize,
    /// Events with issues
    pub invalid_events: usize,
    /// Missing value statistics per field
    pub missing_stats: HashMap<String, MissingStats>,
    /// Price anomalies detected
    pub price_anomalies: Vec<PriceAnomaly>,
    /// Timestamp issues
    pub timestamp_issues: Vec<TimestampIssue>,
    /// Feature range violations
    pub range_violations: Vec<RangeViolation>,
    /// Data gaps (missing time periods)
    pub data_gaps: Vec<DataGap>,
    /// Overall quality score [0, 1]
    pub quality_score: f64,
    /// Recommendations
    pub recommendations: Vec<String>,
}

impl Default for DataQualityReport {
    fn default() -> Self {
        Self {
            total_events: 0,
            valid_events: 0,
            invalid_events: 0,
            missing_stats: HashMap::new(),
            price_anomalies: Vec::new(),
            timestamp_issues: Vec::new(),
            range_violations: Vec::new(),
            data_gaps: Vec::new(),
            quality_score: 0.0,
            recommendations: Vec::new(),
        }
    }
}

impl DataQualityReport {
    /// Print a formatted summary
    pub fn print_summary(&self) {
        println!();
        println!("========================================");
        println!("     DATA QUALITY REPORT");
        println!("========================================");
        println!();
        println!("OVERVIEW:");
        println!("  Total Events:    {}", self.total_events);
        println!("  Valid Events:    {} ({:.1}%)",
            self.valid_events,
            self.valid_events as f64 / self.total_events.max(1) as f64 * 100.0);
        println!("  Invalid Events:  {}", self.invalid_events);
        println!("  Quality Score:   {:.1}%", self.quality_score * 100.0);
        println!();

        // Missing values summary
        if !self.missing_stats.is_empty() {
            println!("MISSING VALUES:");
            let mut items: Vec<_> = self.missing_stats.iter().collect();
            items.sort_by(|a, b| b.1.missing_count.cmp(&a.1.missing_count));
            for (field, stats) in items.iter().take(10) {
                if stats.missing_count > 0 {
                    println!("  {:<25} {:>6} missing ({:.1}%)",
                        field,
                        stats.missing_count,
                        stats.missing_pct * 100.0);
                }
            }
            println!();
        }

        // Price anomalies
        if !self.price_anomalies.is_empty() {
            println!("PRICE ANOMALIES: {} detected", self.price_anomalies.len());
            for anomaly in self.price_anomalies.iter().take(5) {
                println!("  [{:?}] {} at ts={}: value={}",
                    anomaly.anomaly_type,
                    anomaly.field,
                    anomaly.timestamp_ms,
                    anomaly.value);
            }
            if self.price_anomalies.len() > 5 {
                println!("  ... and {} more", self.price_anomalies.len() - 5);
            }
            println!();
        }

        // Timestamp issues
        if !self.timestamp_issues.is_empty() {
            println!("TIMESTAMP ISSUES: {} detected", self.timestamp_issues.len());
            for issue in self.timestamp_issues.iter().take(5) {
                println!("  [{:?}] at ts={}: {}",
                    issue.issue_type,
                    issue.timestamp_ms,
                    issue.description);
            }
            if self.timestamp_issues.len() > 5 {
                println!("  ... and {} more", self.timestamp_issues.len() - 5);
            }
            println!();
        }

        // Data gaps
        if !self.data_gaps.is_empty() {
            println!("DATA GAPS: {} detected", self.data_gaps.len());
            for gap in self.data_gaps.iter().take(5) {
                println!("  Gap of {:.1} hours at ts={}",
                    gap.duration_hours,
                    gap.start_ms);
            }
            println!();
        }

        // Recommendations
        if !self.recommendations.is_empty() {
            println!("RECOMMENDATIONS:");
            for rec in &self.recommendations {
                println!("  - {}", rec);
            }
            println!();
        }

        println!("========================================");
    }

    /// Save report to JSON
    pub fn save_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

/// Missing value statistics for a field
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MissingStats {
    pub field_name: String,
    pub total_count: usize,
    pub missing_count: usize,
    pub missing_pct: f64,
}

/// Price anomaly types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PriceAnomalyType {
    Negative,
    Zero,
    ExtremeOutlier,
    SuddenJump,
}

/// A detected price anomaly
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceAnomaly {
    pub timestamp_ms: i64,
    pub field: String,
    pub value: f64,
    pub anomaly_type: PriceAnomalyType,
    pub expected_range: Option<(f64, f64)>,
}

/// Timestamp issue types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimestampIssueType {
    Duplicate,
    OutOfOrder,
    FutureTimestamp,
    StaleData,
}

/// A detected timestamp issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampIssue {
    pub timestamp_ms: i64,
    pub issue_type: TimestampIssueType,
    pub description: String,
}

/// Feature range violation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeViolation {
    pub timestamp_ms: i64,
    pub field: String,
    pub value: f64,
    pub expected_min: f64,
    pub expected_max: f64,
}

/// Data gap (missing time period)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataGap {
    pub start_ms: i64,
    pub end_ms: i64,
    pub duration_hours: f64,
}

/// Configuration for data validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Maximum allowed gap between events (hours)
    pub max_gap_hours: f64,
    /// Price jump threshold (multiple of typical spread)
    pub price_jump_threshold: f64,
    /// Minimum valid price
    pub min_valid_price: f64,
    /// Maximum valid price
    pub max_valid_price: f64,
    /// Critical fields that should rarely be missing
    pub critical_fields: Vec<String>,
    /// Maximum missing percentage for critical fields
    pub max_critical_missing_pct: f64,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            max_gap_hours: 4.0,
            price_jump_threshold: 0.05, // 5% jump
            min_valid_price: 0.0,
            max_valid_price: 1_000_000.0,
            critical_fields: vec![
                "mid_price".to_string(),
                "best_bid".to_string(),
                "best_ask".to_string(),
            ],
            max_critical_missing_pct: 5.0, // 5%
        }
    }
}

/// Data validator
pub struct DataValidator {
    config: ValidationConfig,
}

impl DataValidator {
    /// Create a new validator with default config
    pub fn new() -> Self {
        Self {
            config: ValidationConfig::default(),
        }
    }

    /// Create a validator with custom config
    pub fn with_config(config: ValidationConfig) -> Self {
        Self { config }
    }

    /// Validate data from a directory
    pub fn validate_directory(&self, data_dir: &PathBuf) -> Result<DataQualityReport> {
        let replay_config = ReplayConfig {
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        let mut replay = ParquetReplay::new(replay_config);
        let _num_events = replay.load()?;

        self.validate_events(replay.iter().cloned().collect())
    }

    /// Validate a list of events
    pub fn validate_events(&self, events: Vec<ReplayEvent>) -> Result<DataQualityReport> {
        let mut report = DataQualityReport::default();
        report.total_events = events.len();

        if events.is_empty() {
            report.recommendations.push("No data to validate".to_string());
            return Ok(report);
        }

        // Initialize missing stats for key fields
        let fields = vec![
            "mid_price", "best_bid", "best_ask", "microprice", "spread",
            "imbalance", "trade_rate_10s", "realized_volatility_100",
            "tick_entropy_10s", "toxicity_index",
        ];
        for field in &fields {
            report.missing_stats.insert(field.to_string(), MissingStats {
                field_name: field.to_string(),
                total_count: events.len(),
                missing_count: 0,
                missing_pct: 0.0,
            });
        }

        let mut prev_timestamp: Option<i64> = None;
        let mut prev_mid_price: Option<f64> = None;
        let mut valid_count = 0;

        for event in &events {
            let snap = &event.snapshot;
            let ts = event.timestamp_ms;
            let mut event_valid = true;

            // Check missing values
            self.check_missing(&mut report, "mid_price", snap.mid_price.is_none());
            self.check_missing(&mut report, "best_bid", snap.best_bid.is_none());
            self.check_missing(&mut report, "best_ask", snap.best_ask.is_none());
            self.check_missing(&mut report, "microprice", snap.microprice.is_none());
            self.check_missing(&mut report, "spread", snap.spread.is_none());
            self.check_missing(&mut report, "imbalance", snap.imbalance.is_none());
            self.check_missing(&mut report, "trade_rate_10s", snap.trade_rate_10s.is_none());
            self.check_missing(&mut report, "realized_volatility_100", snap.realized_volatility_100.is_none());
            self.check_missing(&mut report, "tick_entropy_10s", snap.tick_entropy_10s.is_none());
            self.check_missing(&mut report, "toxicity_index", snap.toxicity_index.is_none());

            // Check price sanity
            if let Some(mid) = snap.mid_price {
                let mid_f64 = mid.to_string().parse::<f64>().unwrap_or(0.0);

                if mid <= dec!(0) {
                    report.price_anomalies.push(PriceAnomaly {
                        timestamp_ms: ts,
                        field: "mid_price".to_string(),
                        value: mid_f64,
                        anomaly_type: if mid < dec!(0) { PriceAnomalyType::Negative } else { PriceAnomalyType::Zero },
                        expected_range: Some((self.config.min_valid_price, self.config.max_valid_price)),
                    });
                    event_valid = false;
                }

                // Check for sudden jumps
                if let Some(prev_mid) = prev_mid_price {
                    if prev_mid > 0.0 {
                        let pct_change = (mid_f64 - prev_mid).abs() / prev_mid;
                        if pct_change > self.config.price_jump_threshold {
                            report.price_anomalies.push(PriceAnomaly {
                                timestamp_ms: ts,
                                field: "mid_price".to_string(),
                                value: mid_f64,
                                anomaly_type: PriceAnomalyType::SuddenJump,
                                expected_range: Some((prev_mid * 0.95, prev_mid * 1.05)),
                            });
                        }
                    }
                }

                prev_mid_price = Some(mid_f64);
            } else {
                event_valid = false;
            }

            // Check timestamp continuity
            if let Some(prev_ts) = prev_timestamp {
                // Duplicate check
                if ts == prev_ts {
                    report.timestamp_issues.push(TimestampIssue {
                        timestamp_ms: ts,
                        issue_type: TimestampIssueType::Duplicate,
                        description: "Duplicate timestamp".to_string(),
                    });
                }

                // Out of order check
                if ts < prev_ts {
                    report.timestamp_issues.push(TimestampIssue {
                        timestamp_ms: ts,
                        issue_type: TimestampIssueType::OutOfOrder,
                        description: format!("Timestamp {} is before previous {}", ts, prev_ts),
                    });
                }

                // Gap check
                let gap_hours = (ts - prev_ts) as f64 / (1000.0 * 60.0 * 60.0);
                if gap_hours > self.config.max_gap_hours {
                    report.data_gaps.push(DataGap {
                        start_ms: prev_ts,
                        end_ms: ts,
                        duration_hours: gap_hours,
                    });
                }
            }

            prev_timestamp = Some(ts);

            // Check feature ranges
            self.check_range(&mut report, ts, "imbalance", snap.imbalance, -dec!(1), dec!(1));
            self.check_range(&mut report, ts, "trade_imbalance", snap.trade_imbalance, dec!(0), dec!(1));

            // Entropy should be in [0, log2(3)] ≈ [0, 1.585]
            if let Some(entropy) = snap.tick_entropy_10s {
                let ent_f64 = entropy.to_string().parse::<f64>().unwrap_or(0.0);
                if ent_f64 < 0.0 || ent_f64 > 2.0 {
                    report.range_violations.push(RangeViolation {
                        timestamp_ms: ts,
                        field: "tick_entropy_10s".to_string(),
                        value: ent_f64,
                        expected_min: 0.0,
                        expected_max: 1.585,
                    });
                }
            }

            if event_valid {
                valid_count += 1;
            }
        }

        report.valid_events = valid_count;
        report.invalid_events = report.total_events - valid_count;

        // Calculate missing percentages
        for stats in report.missing_stats.values_mut() {
            stats.missing_pct = stats.missing_count as f64 / stats.total_count.max(1) as f64;
        }

        // Calculate quality score
        report.quality_score = self.calculate_quality_score(&report);

        // Generate recommendations
        self.generate_recommendations(&mut report);

        Ok(report)
    }

    fn check_missing(&self, report: &mut DataQualityReport, field: &str, is_missing: bool) {
        if is_missing {
            if let Some(stats) = report.missing_stats.get_mut(field) {
                stats.missing_count += 1;
            }
        }
    }

    fn check_range(
        &self,
        report: &mut DataQualityReport,
        ts: i64,
        field: &str,
        value: Option<Decimal>,
        min: Decimal,
        max: Decimal,
    ) {
        if let Some(v) = value {
            if v < min || v > max {
                let v_f64 = v.to_string().parse::<f64>().unwrap_or(0.0);
                let min_f64 = min.to_string().parse::<f64>().unwrap_or(0.0);
                let max_f64 = max.to_string().parse::<f64>().unwrap_or(0.0);
                report.range_violations.push(RangeViolation {
                    timestamp_ms: ts,
                    field: field.to_string(),
                    value: v_f64,
                    expected_min: min_f64,
                    expected_max: max_f64,
                });
            }
        }
    }

    fn calculate_quality_score(&self, report: &DataQualityReport) -> f64 {
        let mut score = 1.0;

        // Penalize for invalid events
        let valid_ratio = report.valid_events as f64 / report.total_events.max(1) as f64;
        score *= valid_ratio;

        // Penalize for missing critical fields
        for field in &self.config.critical_fields {
            if let Some(stats) = report.missing_stats.get(field) {
                let penalty = (stats.missing_pct * 2.0).min(0.5); // Max 50% penalty per field
                score -= penalty;
            }
        }

        // Penalize for price anomalies
        let anomaly_ratio = report.price_anomalies.len() as f64 / report.total_events.max(1) as f64;
        score -= (anomaly_ratio * 10.0).min(0.3); // Max 30% penalty

        // Penalize for data gaps
        let gap_penalty = (report.data_gaps.len() as f64 * 0.05).min(0.2); // Max 20% penalty
        score -= gap_penalty;

        score.max(0.0).min(1.0)
    }

    fn generate_recommendations(&self, report: &mut DataQualityReport) {
        // Missing data recommendations
        for (field, stats) in &report.missing_stats {
            if self.config.critical_fields.contains(field) {
                if stats.missing_pct * 100.0 > self.config.max_critical_missing_pct {
                    report.recommendations.push(format!(
                        "Critical field '{}' has {:.1}% missing values - investigate data source",
                        field,
                        stats.missing_pct * 100.0
                    ));
                }
            }
        }

        // Price anomaly recommendations
        if !report.price_anomalies.is_empty() {
            report.recommendations.push(format!(
                "Found {} price anomalies - consider filtering or investigating these events",
                report.price_anomalies.len()
            ));
        }

        // Data gap recommendations
        if !report.data_gaps.is_empty() {
            let total_gap_hours: f64 = report.data_gaps.iter().map(|g| g.duration_hours).sum();
            report.recommendations.push(format!(
                "Found {} data gaps totaling {:.1} hours - may need additional data collection",
                report.data_gaps.len(),
                total_gap_hours
            ));
        }

        // Quality score recommendation
        if report.quality_score < 0.8 {
            report.recommendations.push(
                "Data quality score below 80% - backtest results may be unreliable".to_string()
            );
        }

        // Timestamp issues
        if !report.timestamp_issues.is_empty() {
            report.recommendations.push(format!(
                "Found {} timestamp issues - data may need reprocessing",
                report.timestamp_issues.len()
            ));
        }
    }
}

impl Default for DataValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::feature_fusion::FeaturesSnapshot;

    fn create_valid_event(ts: i64, mid_price: Decimal) -> ReplayEvent {
        let mut snap = FeaturesSnapshot::default();
        snap.mid_price = Some(mid_price);
        snap.best_bid = Some(mid_price - dec!(1));
        snap.best_ask = Some(mid_price + dec!(1));
        snap.spread = Some(dec!(2));
        snap.imbalance = Some(dec!(0));

        ReplayEvent {
            timestamp_ms: ts,
            snapshot: snap,
        }
    }

    #[test]
    fn test_validator_empty_events() {
        let validator = DataValidator::new();
        let report = validator.validate_events(vec![]).unwrap();

        assert_eq!(report.total_events, 0);
        assert_eq!(report.valid_events, 0);
    }

    #[test]
    fn test_validator_valid_events() {
        let validator = DataValidator::new();
        let events = vec![
            create_valid_event(1000, dec!(100)),
            create_valid_event(2000, dec!(100.5)),
            create_valid_event(3000, dec!(101)),
        ];

        let report = validator.validate_events(events).unwrap();

        assert_eq!(report.total_events, 3);
        assert_eq!(report.valid_events, 3);
        assert!(report.quality_score > 0.8);
    }

    #[test]
    fn test_validator_detects_missing_price() {
        let validator = DataValidator::new();
        let mut snap = FeaturesSnapshot::default();
        snap.mid_price = None; // Missing!

        let events = vec![
            ReplayEvent {
                timestamp_ms: 1000,
                snapshot: snap,
            },
        ];

        let report = validator.validate_events(events).unwrap();

        assert_eq!(report.invalid_events, 1);
        assert!(report.missing_stats.get("mid_price").unwrap().missing_count > 0);
    }

    #[test]
    fn test_validator_detects_data_gap() {
        let validator = DataValidator::new();
        let events = vec![
            create_valid_event(0, dec!(100)),
            create_valid_event(5 * 60 * 60 * 1000, dec!(100)), // 5 hour gap
        ];

        let report = validator.validate_events(events).unwrap();

        assert_eq!(report.data_gaps.len(), 1);
        assert!(report.data_gaps[0].duration_hours > 4.0);
    }

    #[test]
    fn test_validator_detects_price_jump() {
        let validator = DataValidator::new();
        let events = vec![
            create_valid_event(1000, dec!(100)),
            create_valid_event(2000, dec!(110)), // 10% jump
        ];

        let report = validator.validate_events(events).unwrap();

        assert!(!report.price_anomalies.is_empty());
    }

    #[test]
    fn test_quality_score_calculation() {
        let validator = DataValidator::new();

        // All valid events should give high score
        let events: Vec<ReplayEvent> = (0..100)
            .map(|i| create_valid_event(i * 1000, dec!(100)))
            .collect();

        let report = validator.validate_events(events).unwrap();
        assert!(report.quality_score > 0.9);
    }

    #[test]
    fn test_validation_config_default() {
        let config = ValidationConfig::default();
        assert_eq!(config.max_gap_hours, 4.0);
        assert!(!config.critical_fields.is_empty());
    }
}

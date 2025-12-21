//! Results Store - Persistence layer for ValidationResult
//!
//! Task 0.3: ResultsStore Persistence
//!
//! This module provides:
//! - Save ValidationResult to disk (JSON + Parquet)
//! - Query results by stage, time period, algorithm config
//! - Support aggregation (average Sharpe across runs)
//! - Link results to research state that generated them
//! - Audit logging for all operations

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::validation_result::{ValidationResult, ValidationStageType, ValidationMetrics};

// ==================== Configuration ====================

/// Configuration for the results store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultsStoreConfig {
    /// Base path for storing results
    pub base_path: PathBuf,
    /// Maximum number of results to keep per config_id
    pub max_results_per_config: usize,
    /// Enable compression for JSON files
    pub compress: bool,
    /// Enable audit logging
    pub enable_audit_log: bool,
    /// Enable Parquet storage in addition to JSON
    pub enable_parquet: bool,
}

impl Default for ResultsStoreConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./data/results"),
            max_results_per_config: 1000,
            compress: true,
            enable_audit_log: true,
            enable_parquet: true,
        }
    }
}

impl ResultsStoreConfig {
    /// Create config with a specific path
    pub fn with_path(path: impl AsRef<Path>) -> Self {
        Self {
            base_path: path.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    /// Disable Parquet storage
    pub fn without_parquet(mut self) -> Self {
        self.enable_parquet = false;
        self
    }

    /// Disable audit logging
    pub fn without_audit(mut self) -> Self {
        self.enable_audit_log = false;
        self
    }
}

// ==================== Audit Log ====================

/// Types of operations that can be audited
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultsAuditOperation {
    Save,
    Load,
    Delete,
    Query,
    Aggregate,
}

/// A single audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultsAuditEntry {
    pub timestamp: DateTime<Utc>,
    pub operation: ResultsAuditOperation,
    pub result_id: String,
    pub stage_type: Option<ValidationStageType>,
    pub config_id: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl ResultsAuditEntry {
    pub fn new(operation: ResultsAuditOperation, result_id: &str) -> Self {
        Self {
            timestamp: Utc::now(),
            operation,
            result_id: result_id.to_string(),
            stage_type: None,
            config_id: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_stage(mut self, stage: ValidationStageType) -> Self {
        self.stage_type = Some(stage);
        self
    }

    pub fn with_config(mut self, config_id: &str) -> Self {
        self.config_id = Some(config_id.to_string());
        self
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

// ==================== Aggregated Metrics ====================

/// Aggregated metrics across multiple validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMetrics {
    /// Number of results aggregated
    pub count: usize,
    /// Average Sharpe ratio
    pub avg_sharpe: f64,
    /// Standard deviation of Sharpe ratio
    pub std_sharpe: f64,
    /// Minimum Sharpe ratio
    pub min_sharpe: f64,
    /// Maximum Sharpe ratio
    pub max_sharpe: f64,
    /// Average total return
    pub avg_total_return: f64,
    /// Average win rate
    pub avg_win_rate: f64,
    /// Average max drawdown
    pub avg_max_drawdown: f64,
    /// Average number of trades
    pub avg_trade_count: f64,
    /// Average profit factor
    pub avg_profit_factor: f64,
    /// Percentage of results that passed thresholds
    pub pass_rate: f64,
    /// Stage type for this aggregation
    pub stage_type: Option<ValidationStageType>,
    /// Config ID for this aggregation
    pub config_id: Option<String>,
    /// Time range start
    pub start_time: Option<DateTime<Utc>>,
    /// Time range end
    pub end_time: Option<DateTime<Utc>>,
}

impl Default for AggregatedMetrics {
    fn default() -> Self {
        Self {
            count: 0,
            avg_sharpe: 0.0,
            std_sharpe: 0.0,
            min_sharpe: f64::MAX,
            max_sharpe: f64::MIN,
            avg_total_return: 0.0,
            avg_win_rate: 0.0,
            avg_max_drawdown: 0.0,
            avg_trade_count: 0.0,
            avg_profit_factor: 0.0,
            pass_rate: 0.0,
            stage_type: None,
            config_id: None,
            start_time: None,
            end_time: None,
        }
    }
}

impl AggregatedMetrics {
    /// Create aggregated metrics from a set of validation results
    pub fn from_results(results: &[ValidationResult]) -> Self {
        if results.is_empty() {
            return Self::default();
        }

        let count = results.len();
        let mut sum_sharpe = 0.0;
        let mut sum_return = 0.0;
        let mut sum_win_rate = 0.0;
        let mut sum_drawdown = 0.0;
        let mut sum_trades = 0.0;
        let mut sum_profit_factor = 0.0;
        let mut passed = 0usize;
        let mut min_sharpe = f64::MAX;
        let mut max_sharpe = f64::MIN;
        let mut start_time: Option<DateTime<Utc>> = None;
        let mut end_time: Option<DateTime<Utc>> = None;

        for result in results {
            let sharpe = result.metrics.sharpe_ratio;
            sum_sharpe += sharpe;
            sum_return += result.metrics.total_pnl;
            sum_win_rate += result.metrics.win_rate;
            sum_drawdown += result.metrics.max_drawdown_pct;
            sum_trades += result.metrics.trade_count as f64;
            sum_profit_factor += result.metrics.profit_factor;

            if result.passed {
                passed += 1;
            }

            if sharpe < min_sharpe {
                min_sharpe = sharpe;
            }
            if sharpe > max_sharpe {
                max_sharpe = sharpe;
            }

            // Track time range
            if start_time.is_none() || result.period_start < start_time.unwrap() {
                start_time = Some(result.period_start);
            }
            if end_time.is_none() || result.period_end > end_time.unwrap() {
                end_time = Some(result.period_end);
            }
        }

        let avg_sharpe = sum_sharpe / count as f64;

        // Calculate standard deviation of Sharpe
        let variance = results.iter()
            .map(|r| {
                let diff = r.metrics.sharpe_ratio - avg_sharpe;
                diff * diff
            })
            .sum::<f64>() / count as f64;
        let std_sharpe = variance.sqrt();

        Self {
            count,
            avg_sharpe,
            std_sharpe,
            min_sharpe,
            max_sharpe,
            avg_total_return: sum_return / count as f64,
            avg_win_rate: sum_win_rate / count as f64,
            avg_max_drawdown: sum_drawdown / count as f64,
            avg_trade_count: sum_trades / count as f64,
            avg_profit_factor: sum_profit_factor / count as f64,
            pass_rate: passed as f64 / count as f64,
            stage_type: results.first().map(|r| r.stage_type),
            config_id: results.first().map(|r| r.config_id.clone()),
            start_time,
            end_time,
        }
    }

    /// Check if this aggregation represents positive performance
    pub fn is_positive(&self) -> bool {
        self.avg_sharpe > 0.0 && self.avg_total_return > 0.0
    }

    /// Check if this aggregation meets minimum quality thresholds
    pub fn meets_quality_threshold(&self, min_sharpe: f64, min_pass_rate: f64) -> bool {
        self.avg_sharpe >= min_sharpe && self.pass_rate >= min_pass_rate
    }
}

// ==================== Query Filters ====================

/// Filter criteria for querying results
#[derive(Debug, Clone, Default)]
pub struct ResultsQuery {
    /// Filter by stage type
    pub stage_type: Option<ValidationStageType>,
    /// Filter by config ID
    pub config_id: Option<String>,
    /// Filter by research state ID
    pub research_state_id: Option<String>,
    /// Filter by symbol (from metadata)
    pub symbol: Option<String>,
    /// Filter by time range start
    pub start_time: Option<DateTime<Utc>>,
    /// Filter by time range end
    pub end_time: Option<DateTime<Utc>>,
    /// Filter by minimum Sharpe ratio
    pub min_sharpe: Option<f64>,
    /// Filter by maximum drawdown threshold
    pub max_drawdown: Option<f64>,
    /// Only include passed results
    pub passed_only: bool,
    /// Limit number of results
    pub limit: Option<usize>,
    /// Sort by field (default: timestamp descending)
    pub sort_by: Option<SortField>,
    /// Sort ascending (default: false = descending)
    pub sort_ascending: bool,
}

/// Fields that can be sorted by
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortField {
    Timestamp,
    Sharpe,
    TotalReturn,
    WinRate,
    MaxDrawdown,
    TradeCount,
}

impl ResultsQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stage(mut self, stage: ValidationStageType) -> Self {
        self.stage_type = Some(stage);
        self
    }

    pub fn with_config(mut self, config_id: &str) -> Self {
        self.config_id = Some(config_id.to_string());
        self
    }

    pub fn with_research_state(mut self, state_id: &str) -> Self {
        self.research_state_id = Some(state_id.to_string());
        self
    }

    pub fn with_symbol(mut self, symbol: &str) -> Self {
        self.symbol = Some(symbol.to_string());
        self
    }

    pub fn with_time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    pub fn with_min_sharpe(mut self, min_sharpe: f64) -> Self {
        self.min_sharpe = Some(min_sharpe);
        self
    }

    pub fn with_max_drawdown(mut self, max_drawdown: f64) -> Self {
        self.max_drawdown = Some(max_drawdown);
        self
    }

    pub fn passed_only(mut self) -> Self {
        self.passed_only = true;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn sorted_by(mut self, field: SortField, ascending: bool) -> Self {
        self.sort_by = Some(field);
        self.sort_ascending = ascending;
        self
    }

    /// Check if a result matches this query
    pub fn matches(&self, result: &ValidationResult) -> bool {
        // Stage type filter
        if let Some(stage) = self.stage_type {
            if result.stage_type != stage {
                return false;
            }
        }

        // Config ID filter
        if let Some(ref config_id) = self.config_id {
            if &result.config_id != config_id {
                return false;
            }
        }

        // Research state ID filter
        if let Some(ref state_id) = self.research_state_id {
            if result.research_state_id.as_ref() != Some(state_id) {
                return false;
            }
        }

        // Symbol filter (from metadata)
        if let Some(ref symbol) = self.symbol {
            let result_symbol = result.metadata.get("symbol");
            if result_symbol != Some(symbol) {
                return false;
            }
        }

        // Time range filter
        if let Some(start) = self.start_time {
            if result.period_start < start {
                return false;
            }
        }
        if let Some(end) = self.end_time {
            if result.period_end > end {
                return false;
            }
        }

        // Sharpe filter
        if let Some(min_sharpe) = self.min_sharpe {
            if result.metrics.sharpe_ratio < min_sharpe {
                return false;
            }
        }

        // Drawdown filter
        if let Some(max_dd) = self.max_drawdown {
            if result.metrics.max_drawdown_pct > max_dd {
                return false;
            }
        }

        // Passed filter
        if self.passed_only && !result.passed {
            return false;
        }

        true
    }
}

// ==================== Results Store ====================

/// Persistence store for validation results
pub struct ResultsStore {
    config: ResultsStoreConfig,
    cache: HashMap<String, ValidationResult>,
    audit_log: Vec<ResultsAuditEntry>,
}

impl ResultsStore {
    /// Create a new results store with the given configuration
    pub fn new(config: ResultsStoreConfig) -> Result<Self> {
        // Create directory structure
        fs::create_dir_all(&config.base_path)?;
        fs::create_dir_all(config.base_path.join("results"))?;
        fs::create_dir_all(config.base_path.join("parquet"))?;
        fs::create_dir_all(config.base_path.join("audit"))?;
        fs::create_dir_all(config.base_path.join("aggregations"))?;

        Ok(Self {
            config,
            cache: HashMap::new(),
            audit_log: Vec::new(),
        })
    }

    /// Create a store at the given path with default config
    pub fn at_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::new(ResultsStoreConfig::with_path(path))
    }

    /// Get the store configuration
    pub fn config(&self) -> &ResultsStoreConfig {
        &self.config
    }

    // ==================== Save Operations ====================

    /// Save a validation result to disk
    pub fn save(&mut self, result: &ValidationResult) -> Result<PathBuf> {
        let results_dir = self.config.base_path.join("results");

        // Create filename with stage, config, timestamp, and unique ID
        let stage_str = format!("{:?}", result.stage_type).to_lowercase();
        let symbol = result.metadata.get("symbol").map(|s| s.as_str()).unwrap_or("unknown");
        let filename = format!(
            "{}_{}_{}_{}_{}.json",
            symbol,
            stage_str,
            result.config_id,
            result.period_start.format("%Y%m%d_%H%M%S%.3f"),
            &result.id[..8]  // Use first 8 chars of UUID for uniqueness
        );
        let path = results_dir.join(&filename);

        // Serialize and write
        let json = serde_json::to_string_pretty(result)?;
        fs::write(&path, json)?;

        // Update cache
        self.cache.insert(result.id.clone(), result.clone());

        // Also save to Parquet if enabled
        if self.config.enable_parquet {
            self.save_to_parquet(result)?;
        }

        // Audit log
        if self.config.enable_audit_log {
            let entry = ResultsAuditEntry::new(ResultsAuditOperation::Save, &result.id)
                .with_stage(result.stage_type)
                .with_config(&result.config_id)
                .with_metadata("path", path.to_string_lossy().as_ref());
            self.audit_log.push(entry);
        }

        Ok(path)
    }

    /// Save result to Parquet format for efficient querying
    pub fn save_to_parquet(&self, result: &ValidationResult) -> Result<PathBuf> {
        let parquet_dir = self.config.base_path.join("parquet");

        let stage_str = format!("{:?}", result.stage_type).to_lowercase();
        let symbol = result.metadata.get("symbol").map(|s| s.as_str()).unwrap_or("unknown");
        let filename = format!(
            "{}_{}_{}_{}_{}.parquet",
            symbol,
            stage_str,
            result.config_id,
            result.period_start.format("%Y%m%d_%H%M%S%.3f"),
            &result.id[..8]  // Use first 8 chars of UUID for uniqueness
        );
        let path = parquet_dir.join(&filename);

        // Create DataFrame with key metrics for efficient querying
        let json = serde_json::to_string(result)?;

        let df = df!(
            "id" => [result.id.as_str()],
            "symbol" => [symbol],
            "stage_type" => [format!("{:?}", result.stage_type).as_str()],
            "config_id" => [result.config_id.as_str()],
            "research_state_id" => [result.research_state_id.as_deref().unwrap_or("")],
            "period_start" => [result.period_start.to_rfc3339()],
            "period_end" => [result.period_end.to_rfc3339()],
            "sharpe_ratio" => [result.metrics.sharpe_ratio],
            "total_pnl" => [result.metrics.total_pnl],
            "win_rate" => [result.metrics.win_rate],
            "max_drawdown_pct" => [result.metrics.max_drawdown_pct],
            "trade_count" => [result.metrics.trade_count as i64],
            "profit_factor" => [result.metrics.profit_factor],
            "passed" => [result.passed],
            "full_result_json" => [json.as_str()]
        )?;

        let file = std::fs::File::create(&path)?;
        ParquetWriter::new(file).finish(&mut df.clone())?;

        Ok(path)
    }

    /// Save multiple results in a batch
    pub fn save_batch(&mut self, results: &[ValidationResult]) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::with_capacity(results.len());
        for result in results {
            paths.push(self.save(result)?);
        }
        Ok(paths)
    }

    // ==================== Load Operations ====================

    /// Load a result by ID
    pub fn load_by_id(&mut self, id: &str) -> Result<Option<ValidationResult>> {
        // Check cache first
        if let Some(result) = self.cache.get(id) {
            return Ok(Some(result.clone()));
        }

        // Search in results directory
        let results_dir = self.config.base_path.join("results");
        if !results_dir.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&results_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                let content = fs::read_to_string(&path)?;
                let result: ValidationResult = serde_json::from_str(&content)?;

                if result.id == id {
                    // Update cache
                    self.cache.insert(id.to_string(), result.clone());

                    // Audit log
                    if self.config.enable_audit_log {
                        self.audit_log.push(
                            ResultsAuditEntry::new(ResultsAuditOperation::Load, id)
                                .with_stage(result.stage_type)
                        );
                    }

                    return Ok(Some(result));
                }
            }
        }

        Ok(None)
    }

    /// Load results by stage type
    pub fn load_by_stage(&mut self, stage: ValidationStageType) -> Result<Vec<ValidationResult>> {
        self.query(ResultsQuery::new().with_stage(stage))
    }

    /// Load results by config ID
    pub fn load_by_config(&mut self, config_id: &str) -> Result<Vec<ValidationResult>> {
        self.query(ResultsQuery::new().with_config(config_id))
    }

    /// Load results by research state ID
    pub fn load_by_research_state(&mut self, state_id: &str) -> Result<Vec<ValidationResult>> {
        self.query(ResultsQuery::new().with_research_state(state_id))
    }

    /// Load results by symbol (from metadata)
    pub fn load_by_symbol(&mut self, symbol: &str) -> Result<Vec<ValidationResult>> {
        self.query(ResultsQuery::new().with_symbol(symbol))
    }

    /// Load results within a time range
    pub fn load_by_time_range(
        &mut self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<ValidationResult>> {
        self.query(ResultsQuery::new().with_time_range(start, end))
    }

    /// Load the latest result for a given config
    pub fn load_latest(&mut self, config_id: Option<&str>) -> Result<Option<ValidationResult>> {
        let mut query = ResultsQuery::new()
            .sorted_by(SortField::Timestamp, false)
            .with_limit(1);

        if let Some(config) = config_id {
            query = query.with_config(config);
        }

        let results = self.query(query)?;
        Ok(results.into_iter().next())
    }

    /// Load from Parquet file
    pub fn load_from_parquet(&self, path: &Path) -> Result<ValidationResult> {
        let file = std::fs::File::open(path)?;
        let df = ParquetReader::new(file).finish()?;

        let json_col = df.column("full_result_json")?;
        if let Some(json_str) = json_col.str()?.get(0) {
            let result: ValidationResult = serde_json::from_str(json_str)?;
            return Ok(result);
        }

        anyhow::bail!("Failed to read result from Parquet file")
    }

    // ==================== Query Operations ====================

    /// Query results with filters
    pub fn query(&mut self, query: ResultsQuery) -> Result<Vec<ValidationResult>> {
        let results_dir = self.config.base_path.join("results");
        if !results_dir.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        for entry in fs::read_dir(&results_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                let content = fs::read_to_string(&path)?;
                let result: ValidationResult = serde_json::from_str(&content)?;

                if query.matches(&result) {
                    results.push(result);
                }
            }
        }

        // Sort results
        if let Some(sort_field) = query.sort_by {
            results.sort_by(|a, b| {
                let cmp = match sort_field {
                    SortField::Timestamp => a.period_start.cmp(&b.period_start),
                    SortField::Sharpe => a.metrics.sharpe_ratio.partial_cmp(&b.metrics.sharpe_ratio).unwrap_or(std::cmp::Ordering::Equal),
                    SortField::TotalReturn => a.metrics.total_pnl.partial_cmp(&b.metrics.total_pnl).unwrap_or(std::cmp::Ordering::Equal),
                    SortField::WinRate => a.metrics.win_rate.partial_cmp(&b.metrics.win_rate).unwrap_or(std::cmp::Ordering::Equal),
                    SortField::MaxDrawdown => a.metrics.max_drawdown_pct.partial_cmp(&b.metrics.max_drawdown_pct).unwrap_or(std::cmp::Ordering::Equal),
                    SortField::TradeCount => a.metrics.trade_count.cmp(&b.metrics.trade_count),
                };
                if query.sort_ascending { cmp } else { cmp.reverse() }
            });
        } else {
            // Default: sort by timestamp descending
            results.sort_by(|a, b| b.period_start.cmp(&a.period_start));
        }

        // Apply limit
        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        // Audit log
        if self.config.enable_audit_log {
            let mut entry = ResultsAuditEntry::new(ResultsAuditOperation::Query, "query")
                .with_metadata("count", &results.len().to_string());
            if let Some(stage) = query.stage_type {
                entry = entry.with_stage(stage);
            }
            if let Some(ref config_id) = query.config_id {
                entry = entry.with_config(config_id);
            }
            self.audit_log.push(entry);
        }

        Ok(results)
    }

    /// Count results matching a query
    pub fn count(&self, query: &ResultsQuery) -> Result<usize> {
        let results_dir = self.config.base_path.join("results");
        if !results_dir.exists() {
            return Ok(0);
        }

        let mut count = 0;

        for entry in fs::read_dir(&results_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                let content = fs::read_to_string(&path)?;
                let result: ValidationResult = serde_json::from_str(&content)?;

                if query.matches(&result) {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    // ==================== Aggregation Operations ====================

    /// Aggregate metrics for results matching a query
    pub fn aggregate(&mut self, query: ResultsQuery) -> Result<AggregatedMetrics> {
        let results = self.query(query)?;

        let metrics = AggregatedMetrics::from_results(&results);

        // Audit log
        if self.config.enable_audit_log {
            self.audit_log.push(
                ResultsAuditEntry::new(ResultsAuditOperation::Aggregate, "aggregate")
                    .with_metadata("count", &metrics.count.to_string())
                    .with_metadata("avg_sharpe", &format!("{:.4}", metrics.avg_sharpe))
            );
        }

        Ok(metrics)
    }

    /// Aggregate by stage type
    pub fn aggregate_by_stage(&mut self, stage: ValidationStageType) -> Result<AggregatedMetrics> {
        self.aggregate(ResultsQuery::new().with_stage(stage))
    }

    /// Aggregate by config ID
    pub fn aggregate_by_config(&mut self, config_id: &str) -> Result<AggregatedMetrics> {
        self.aggregate(ResultsQuery::new().with_config(config_id))
    }

    /// Get aggregations for all stages
    pub fn aggregate_all_stages(&mut self) -> Result<HashMap<ValidationStageType, AggregatedMetrics>> {
        let stages = [
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
            ValidationStageType::Paper,
            ValidationStageType::Live,
        ];

        let mut aggregations = HashMap::new();

        for stage in stages {
            let metrics = self.aggregate_by_stage(stage)?;
            if metrics.count > 0 {
                aggregations.insert(stage, metrics);
            }
        }

        Ok(aggregations)
    }

    /// Save aggregation to disk
    pub fn save_aggregation(&self, name: &str, metrics: &AggregatedMetrics) -> Result<PathBuf> {
        let agg_dir = self.config.base_path.join("aggregations");
        let filename = format!("{}_{}.json", name, Utc::now().format("%Y%m%d_%H%M%S"));
        let path = agg_dir.join(&filename);

        let json = serde_json::to_string_pretty(metrics)?;
        fs::write(&path, json)?;

        Ok(path)
    }

    // ==================== Delete Operations ====================

    /// Delete a result by ID
    pub fn delete(&mut self, id: &str) -> Result<bool> {
        let results_dir = self.config.base_path.join("results");
        if !results_dir.exists() {
            return Ok(false);
        }

        let mut deleted = false;

        for entry in fs::read_dir(&results_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                let content = fs::read_to_string(&path)?;
                let result: ValidationResult = serde_json::from_str(&content)?;

                if result.id == id {
                    fs::remove_file(&path)?;
                    self.cache.remove(id);
                    deleted = true;

                    // Also delete Parquet file if exists
                    let parquet_path = self.config.base_path.join("parquet")
                        .join(path.file_stem().unwrap())
                        .with_extension("parquet");
                    if parquet_path.exists() {
                        fs::remove_file(&parquet_path)?;
                    }

                    // Audit log
                    if self.config.enable_audit_log {
                        self.audit_log.push(
                            ResultsAuditEntry::new(ResultsAuditOperation::Delete, id)
                                .with_stage(result.stage_type)
                        );
                    }

                    break;
                }
            }
        }

        Ok(deleted)
    }

    /// Delete results matching a query
    pub fn delete_matching(&mut self, query: ResultsQuery) -> Result<usize> {
        let results = self.query(query)?;
        let mut deleted = 0;

        for result in results {
            if self.delete(&result.id)? {
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    /// Delete results older than a given timestamp
    pub fn delete_before(&mut self, timestamp: DateTime<Utc>) -> Result<usize> {
        self.delete_matching(
            ResultsQuery::new().with_time_range(DateTime::<Utc>::MIN_UTC, timestamp)
        )
    }

    // ==================== List Operations ====================

    /// List all result IDs
    pub fn list_ids(&self) -> Result<Vec<String>> {
        let results_dir = self.config.base_path.join("results");
        if !results_dir.exists() {
            return Ok(Vec::new());
        }

        let mut ids = Vec::new();

        for entry in fs::read_dir(&results_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                let content = fs::read_to_string(&path)?;
                let result: ValidationResult = serde_json::from_str(&content)?;
                ids.push(result.id);
            }
        }

        ids.sort();
        Ok(ids)
    }

    /// List all unique config IDs
    pub fn list_configs(&self) -> Result<Vec<String>> {
        let results_dir = self.config.base_path.join("results");
        if !results_dir.exists() {
            return Ok(Vec::new());
        }

        let mut configs = std::collections::HashSet::new();

        for entry in fs::read_dir(&results_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                let content = fs::read_to_string(&path)?;
                let result: ValidationResult = serde_json::from_str(&content)?;
                configs.insert(result.config_id);
            }
        }

        let mut result: Vec<_> = configs.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// List all unique symbols (from metadata)
    pub fn list_symbols(&self) -> Result<Vec<String>> {
        let results_dir = self.config.base_path.join("results");
        if !results_dir.exists() {
            return Ok(Vec::new());
        }

        let mut symbols = std::collections::HashSet::new();

        for entry in fs::read_dir(&results_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                let content = fs::read_to_string(&path)?;
                let result: ValidationResult = serde_json::from_str(&content)?;
                if let Some(symbol) = result.metadata.get("symbol") {
                    symbols.insert(symbol.clone());
                }
            }
        }

        let mut result: Vec<_> = symbols.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Get total result count
    pub fn total_count(&self) -> Result<usize> {
        let results_dir = self.config.base_path.join("results");
        if !results_dir.exists() {
            return Ok(0);
        }

        Ok(fs::read_dir(&results_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
            .count())
    }

    /// Get disk usage in bytes
    pub fn disk_usage(&self) -> Result<u64> {
        let mut total = 0u64;

        for entry in walkdir::WalkDir::new(&self.config.base_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                total += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }

        Ok(total)
    }

    // ==================== Cache Operations ====================

    /// Clear the in-memory cache
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Get a cached result
    pub fn get_cached(&self, id: &str) -> Option<&ValidationResult> {
        self.cache.get(id)
    }

    /// Update cache without saving to disk
    pub fn update_cache(&mut self, result: ValidationResult) {
        self.cache.insert(result.id.clone(), result);
    }

    /// Get cache size
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    // ==================== Audit Log Operations ====================

    /// Get audit log entries
    pub fn audit_log(&self) -> &[ResultsAuditEntry] {
        &self.audit_log
    }

    /// Flush audit log to disk
    pub fn flush_audit_log(&mut self) -> Result<()> {
        if self.audit_log.is_empty() || !self.config.enable_audit_log {
            return Ok(());
        }

        let audit_dir = self.config.base_path.join("audit");
        let filename = format!("audit_{}.jsonl", Utc::now().format("%Y%m%d_%H%M%S"));
        let path = audit_dir.join(&filename);

        let mut content = String::new();
        for entry in &self.audit_log {
            content.push_str(&serde_json::to_string(entry)?);
            content.push('\n');
        }

        fs::write(&path, content)?;
        self.audit_log.clear();

        Ok(())
    }

    /// Load all audit log entries from disk
    pub fn load_audit_log(&self) -> Result<Vec<ResultsAuditEntry>> {
        let audit_dir = self.config.base_path.join("audit");
        if !audit_dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();

        for file_entry in fs::read_dir(&audit_dir)? {
            let file_entry = file_entry?;
            let path = file_entry.path();

            if path.extension().map_or(false, |e| e == "jsonl") {
                let content = fs::read_to_string(&path)?;
                for line in content.lines() {
                    if !line.trim().is_empty() {
                        let entry: ResultsAuditEntry = serde_json::from_str(line)?;
                        entries.push(entry);
                    }
                }
            }
        }

        entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(entries)
    }

    // ==================== Link to Research State ====================

    /// Get all results linked to a specific research state
    pub fn get_results_for_research_state(&mut self, state_id: &str) -> Result<Vec<ValidationResult>> {
        self.load_by_research_state(state_id)
    }

    /// Get aggregated metrics for results linked to a research state
    pub fn aggregate_for_research_state(&mut self, state_id: &str) -> Result<AggregatedMetrics> {
        self.aggregate(ResultsQuery::new().with_research_state(state_id))
    }

    /// Check if a research state has any validation results
    pub fn has_results_for_research_state(&self, state_id: &str) -> Result<bool> {
        Ok(self.count(&ResultsQuery::new().with_research_state(state_id))? > 0)
    }
}

// Need walkdir for disk usage calculation
mod walkdir {
    pub struct WalkDir {
        path: std::path::PathBuf,
    }

    impl WalkDir {
        pub fn new(path: impl AsRef<std::path::Path>) -> Self {
            Self { path: path.as_ref().to_path_buf() }
        }

        pub fn into_iter(self) -> impl Iterator<Item = Result<DirEntry, std::io::Error>> {
            WalkDirIter::new(self.path)
        }
    }

    pub struct DirEntry {
        path: std::path::PathBuf,
        metadata: std::fs::Metadata,
    }

    impl DirEntry {
        #[allow(dead_code)]
        pub fn path(&self) -> &std::path::Path {
            &self.path
        }

        pub fn file_type(&self) -> std::fs::FileType {
            self.metadata.file_type()
        }

        pub fn metadata(&self) -> Result<std::fs::Metadata, std::io::Error> {
            Ok(self.metadata.clone())
        }
    }

    struct WalkDirIter {
        stack: Vec<std::path::PathBuf>,
    }

    impl WalkDirIter {
        fn new(path: std::path::PathBuf) -> Self {
            Self { stack: vec![path] }
        }
    }

    impl Iterator for WalkDirIter {
        type Item = Result<DirEntry, std::io::Error>;

        fn next(&mut self) -> Option<Self::Item> {
            while let Some(path) = self.stack.pop() {
                match std::fs::metadata(&path) {
                    Ok(metadata) => {
                        if metadata.is_dir() {
                            if let Ok(entries) = std::fs::read_dir(&path) {
                                for entry in entries.filter_map(|e| e.ok()) {
                                    self.stack.push(entry.path());
                                }
                            }
                        }
                        return Some(Ok(DirEntry { path, metadata }));
                    }
                    Err(e) => return Some(Err(e)),
                }
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::validation_result::*;
    use tempfile::TempDir;

    // ==================== Helper Functions ====================

    fn create_test_store() -> (ResultsStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = ResultsStoreConfig::with_path(temp_dir.path());
        let store = ResultsStore::new(config).unwrap();
        (store, temp_dir)
    }

    fn create_test_result(symbol: &str, stage: ValidationStageType) -> ValidationResult {
        let now = Utc::now();
        let mut result = ValidationResult::new(
            stage,
            format!("{:?}-Test", stage),
            "test_config".to_string(),
            now - chrono::Duration::hours(1),
            now,
        );
        result.metrics = ValidationMetrics {
            trade_count: 100,
            winners: 55,
            losers: 45,
            win_rate: 0.55,
            total_pnl: 0.15,
            gross_profit: 2500.0,
            gross_loss: 1000.0,
            profit_factor: 1.53,
            avg_pnl: 0.0015,
            avg_pnl_bps: 0.15,
            avg_winner: 0.025,
            avg_loser: -0.018,
            max_winner: 0.05,
            max_loser: -0.03,
            sharpe_ratio: 1.2,
            sortino_ratio: 1.5,
            calmar_ratio: 1.0,
            max_drawdown_pct: 0.08,
            max_drawdown_duration_seconds: 3600,
            avg_trade_duration_seconds: 3600.0,
            max_consecutive_wins: 8,
            max_consecutive_losses: 5,
            total_commission: 50.0,
            avg_slippage_bps: 1.0,
            expectancy: 0.005,
            annualized_return_pct: 15.0,
            annualized_volatility_pct: 10.0,
            long_trades: 50,
            short_trades: 50,
            long_win_rate: 0.55,
            short_win_rate: 0.55,
        };
        result.passed = true;
        result.add_metadata("symbol".to_string(), symbol.to_string());
        result
    }

    fn create_test_result_with_config(symbol: &str, config_id: &str) -> ValidationResult {
        let now = Utc::now();
        let mut result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Backtest-Test".to_string(),
            config_id.to_string(),
            now - chrono::Duration::hours(1),
            now,
        );
        result.metrics = ValidationMetrics::default();
        result.passed = true;
        result.add_metadata("symbol".to_string(), symbol.to_string());
        result
    }

    fn create_test_result_with_state(symbol: &str, state_id: &str) -> ValidationResult {
        let mut result = create_test_result(symbol, ValidationStageType::Backtest);
        result.research_state_id = Some(state_id.to_string());
        result
    }

    fn create_varied_results(count: usize) -> Vec<ValidationResult> {
        let stages = [
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
            ValidationStageType::Paper,
        ];
        let symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"];

        (0..count).map(|i| {
            let now = Utc::now();
            let mut result = ValidationResult::new(
                stages[i % stages.len()],
                format!("{:?}-{}", stages[i % stages.len()], i),
                format!("config_{}", i % 3),
                now - chrono::Duration::hours(1),
                now,
            );
            result.metrics.sharpe_ratio = 0.5 + (i as f64 * 0.1);
            result.metrics.total_pnl = 0.05 + (i as f64 * 0.01);
            result.add_metadata("symbol".to_string(), symbols[i % symbols.len()].to_string());
            result
        }).collect()
    }

    // ==================== ResultsStoreConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = ResultsStoreConfig::default();

        assert_eq!(config.max_results_per_config, 1000);
        assert!(config.compress);
        assert!(config.enable_audit_log);
        assert!(config.enable_parquet);
    }

    #[test]
    fn test_config_with_path() {
        let config = ResultsStoreConfig::with_path("/custom/path");

        assert_eq!(config.base_path, PathBuf::from("/custom/path"));
    }

    #[test]
    fn test_config_without_parquet() {
        let config = ResultsStoreConfig::default().without_parquet();

        assert!(!config.enable_parquet);
    }

    #[test]
    fn test_config_without_audit() {
        let config = ResultsStoreConfig::default().without_audit();

        assert!(!config.enable_audit_log);
    }

    #[test]
    fn test_config_serialization() {
        let config = ResultsStoreConfig::default();

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ResultsStoreConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.max_results_per_config, config.max_results_per_config);
        assert_eq!(deserialized.enable_parquet, config.enable_parquet);
    }

    #[test]
    fn test_config_chaining() {
        let config = ResultsStoreConfig::with_path("/test")
            .without_parquet()
            .without_audit();

        assert_eq!(config.base_path, PathBuf::from("/test"));
        assert!(!config.enable_parquet);
        assert!(!config.enable_audit_log);
    }

    // ==================== Store Creation Tests ====================

    #[test]
    fn test_store_creation() {
        let (store, _temp_dir) = create_test_store();

        assert!(store.config.base_path.exists());
        assert!(store.config.base_path.join("results").exists());
        assert!(store.config.base_path.join("parquet").exists());
        assert!(store.config.base_path.join("audit").exists());
        assert!(store.config.base_path.join("aggregations").exists());
    }

    #[test]
    fn test_store_at_path() {
        let temp_dir = TempDir::new().unwrap();
        let store = ResultsStore::at_path(temp_dir.path()).unwrap();

        assert!(store.config.base_path.exists());
    }

    #[test]
    fn test_store_creates_nested_directories() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nested").join("results");

        let config = ResultsStoreConfig::with_path(&path);
        let _store = ResultsStore::new(config).unwrap();

        assert!(path.join("results").exists());
        assert!(path.join("parquet").exists());
    }

    #[test]
    fn test_store_config_accessor() {
        let (store, _temp_dir) = create_test_store();

        assert!(store.config().enable_audit_log);
    }

    // ==================== Save Tests ====================

    #[test]
    fn test_save_creates_file() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        let path = store.save(&result).unwrap();

        assert!(path.exists());
        assert!(path.extension().map_or(false, |e| e == "json"));
    }

    #[test]
    fn test_save_file_content() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        let path = store.save(&result).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        let loaded: ValidationResult = serde_json::from_str(&content).unwrap();

        assert_eq!(loaded.metadata.get("symbol"), Some(&"BTCUSDT".to_string()));
        assert_eq!(loaded.id, result.id);
    }

    #[test]
    fn test_save_updates_cache() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        store.save(&result).unwrap();

        assert!(store.get_cached(&result.id).is_some());
    }

    #[test]
    fn test_save_creates_parquet() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        store.save(&result).unwrap();

        let parquet_dir = store.config.base_path.join("parquet");
        let parquet_files: Vec<_> = fs::read_dir(&parquet_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();

        assert_eq!(parquet_files.len(), 1);
    }

    #[test]
    fn test_save_without_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResultsStoreConfig::with_path(temp_dir.path()).without_parquet();
        let mut store = ResultsStore::new(config).unwrap();

        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        store.save(&result).unwrap();

        let parquet_dir = store.config.base_path.join("parquet");
        let parquet_count = fs::read_dir(&parquet_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .count();

        assert_eq!(parquet_count, 0);
    }

    #[test]
    fn test_save_multiple_stages() {
        let (mut store, _temp_dir) = create_test_store();

        for stage in [
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
        ] {
            let result = create_test_result("BTCUSDT", stage);
            store.save(&result).unwrap();
        }

        assert_eq!(store.total_count().unwrap(), 3);
    }

    #[test]
    fn test_save_multiple_symbols() {
        let (mut store, _temp_dir) = create_test_store();

        for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] {
            let result = create_test_result(symbol, ValidationStageType::Backtest);
            store.save(&result).unwrap();
        }

        let symbols = store.list_symbols().unwrap();
        assert_eq!(symbols.len(), 3);
    }

    #[test]
    fn test_save_batch() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(10);

        let paths = store.save_batch(&results).unwrap();

        assert_eq!(paths.len(), 10);
        assert_eq!(store.total_count().unwrap(), 10);
    }

    #[test]
    fn test_save_with_config_id() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result_with_config("BTCUSDT", "my_config");

        store.save(&result).unwrap();

        let configs = store.list_configs().unwrap();
        assert!(configs.contains(&"my_config".to_string()));
    }

    #[test]
    fn test_save_to_parquet_directly() {
        let (store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        let path = store.save_to_parquet(&result).unwrap();

        assert!(path.exists());
        assert!(path.extension().map_or(false, |e| e == "parquet"));
    }

    #[test]
    fn test_save_generates_audit_log() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        store.save(&result).unwrap();

        assert_eq!(store.audit_log().len(), 1);
        assert_eq!(store.audit_log()[0].operation, ResultsAuditOperation::Save);
    }

    // ==================== Load Tests ====================

    #[test]
    fn test_load_by_id_nonexistent() {
        let (mut store, _temp_dir) = create_test_store();

        let result = store.load_by_id("nonexistent").unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_load_by_id_after_save() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        let id = result.id.clone();

        store.save(&result).unwrap();
        store.clear_cache();

        let loaded = store.load_by_id(&id).unwrap().unwrap();

        assert_eq!(loaded.id, id);
        assert_eq!(loaded.metadata.get("symbol"), Some(&"BTCUSDT".to_string()));
    }

    #[test]
    fn test_load_by_id_from_cache() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        let id = result.id.clone();

        store.save(&result).unwrap();

        // Should return from cache
        let loaded = store.load_by_id(&id).unwrap().unwrap();

        assert_eq!(loaded.id, id);
    }

    #[test]
    fn test_load_by_stage() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Forward)).unwrap();

        let backtests = store.load_by_stage(ValidationStageType::Backtest).unwrap();

        assert_eq!(backtests.len(), 2);
    }

    #[test]
    fn test_load_by_config() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result_with_config("BTCUSDT", "config_a")).unwrap();
        store.save(&create_test_result_with_config("BTCUSDT", "config_a")).unwrap();
        store.save(&create_test_result_with_config("BTCUSDT", "config_b")).unwrap();

        let config_a = store.load_by_config("config_a").unwrap();

        assert_eq!(config_a.len(), 2);
    }

    #[test]
    fn test_load_by_research_state() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result_with_state("BTCUSDT", "state_1")).unwrap();
        store.save(&create_test_result_with_state("BTCUSDT", "state_1")).unwrap();
        store.save(&create_test_result_with_state("BTCUSDT", "state_2")).unwrap();

        let state_1_results = store.load_by_research_state("state_1").unwrap();

        assert_eq!(state_1_results.len(), 2);
    }

    #[test]
    fn test_load_by_symbol() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();
        store.save(&create_test_result("ETHUSDT", ValidationStageType::Backtest)).unwrap();

        let btc_results = store.load_by_symbol("BTCUSDT").unwrap();

        assert_eq!(btc_results.len(), 2);
    }

    #[test]
    fn test_load_by_time_range() {
        let (mut store, _temp_dir) = create_test_store();

        let now = Utc::now();
        let mut result1 = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::hours(3),
            now - chrono::Duration::hours(2),
        );
        result1.add_metadata("symbol".to_string(), "BTCUSDT".to_string());

        let mut result2 = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::hours(1),
            now,
        );
        result2.add_metadata("symbol".to_string(), "BTCUSDT".to_string());

        store.save(&result1).unwrap();
        store.save(&result2).unwrap();

        let range_start = now - chrono::Duration::hours(4);
        let range_end = now - chrono::Duration::minutes(90);

        let results = store.load_by_time_range(range_start, range_end).unwrap();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_load_latest() {
        let (mut store, _temp_dir) = create_test_store();

        let now = Utc::now();
        let mut result1 = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::hours(3),
            now - chrono::Duration::hours(2),
        );
        result1.add_metadata("symbol".to_string(), "BTCUSDT".to_string());

        let mut result2 = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::hours(1),
            now,
        );
        result2.add_metadata("symbol".to_string(), "BTCUSDT".to_string());

        store.save(&result1).unwrap();
        store.save(&result2).unwrap();

        let latest = store.load_latest(None).unwrap().unwrap();

        assert_eq!(latest.id, result2.id);
    }

    #[test]
    fn test_load_latest_with_config() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result_with_config("BTCUSDT", "config_a")).unwrap();
        store.save(&create_test_result_with_config("BTCUSDT", "config_b")).unwrap();

        let latest = store.load_latest(Some("config_a")).unwrap().unwrap();

        assert_eq!(latest.config_id, "config_a".to_string());
    }

    #[test]
    fn test_load_from_parquet() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        let parquet_path = store.save_to_parquet(&result).unwrap();

        let loaded = store.load_from_parquet(&parquet_path).unwrap();

        assert_eq!(loaded.id, result.id);
        assert_eq!(loaded.metadata.get("symbol"), Some(&"BTCUSDT".to_string()));
    }

    // ==================== Query Tests ====================

    #[test]
    fn test_query_empty() {
        let (mut store, _temp_dir) = create_test_store();

        let results = store.query(ResultsQuery::new()).unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn test_query_all() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(5);
        store.save_batch(&results).unwrap();

        let queried = store.query(ResultsQuery::new()).unwrap();

        assert_eq!(queried.len(), 5);
    }

    #[test]
    fn test_query_by_stage() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(8);
        store.save_batch(&results).unwrap();

        let backtests = store.query(
            ResultsQuery::new().with_stage(ValidationStageType::Backtest)
        ).unwrap();

        assert!(backtests.iter().all(|r| r.stage_type == ValidationStageType::Backtest));
    }

    #[test]
    fn test_query_by_min_sharpe() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(10);
        store.save_batch(&results).unwrap();

        let high_sharpe = store.query(
            ResultsQuery::new().with_min_sharpe(1.0)
        ).unwrap();

        assert!(high_sharpe.iter().all(|r| r.metrics.sharpe_ratio >= 1.0));
    }

    #[test]
    fn test_query_by_max_drawdown() {
        let (mut store, _temp_dir) = create_test_store();

        let mut result1 = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result1.metrics.max_drawdown_pct = 0.05;

        let mut result2 = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result2.metrics.max_drawdown_pct = 0.15;

        store.save(&result1).unwrap();
        store.save(&result2).unwrap();

        let low_dd = store.query(
            ResultsQuery::new().with_max_drawdown(0.10)
        ).unwrap();

        assert_eq!(low_dd.len(), 1);
    }

    #[test]
    fn test_query_passed_only() {
        let (mut store, _temp_dir) = create_test_store();

        let mut result1 = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result1.passed = true;

        let mut result2 = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result2.passed = false;

        store.save(&result1).unwrap();
        store.save(&result2).unwrap();

        let passed = store.query(ResultsQuery::new().passed_only()).unwrap();

        assert_eq!(passed.len(), 1);
        assert!(passed[0].passed);
    }

    #[test]
    fn test_query_with_limit() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(10);
        store.save_batch(&results).unwrap();

        let limited = store.query(ResultsQuery::new().with_limit(3)).unwrap();

        assert_eq!(limited.len(), 3);
    }

    #[test]
    fn test_query_sorted_by_sharpe() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(5);
        store.save_batch(&results).unwrap();

        let sorted = store.query(
            ResultsQuery::new().sorted_by(SortField::Sharpe, false)
        ).unwrap();

        for i in 0..sorted.len() - 1 {
            assert!(sorted[i].metrics.sharpe_ratio >= sorted[i + 1].metrics.sharpe_ratio);
        }
    }

    #[test]
    fn test_query_sorted_ascending() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(5);
        store.save_batch(&results).unwrap();

        let sorted = store.query(
            ResultsQuery::new().sorted_by(SortField::Sharpe, true)
        ).unwrap();

        for i in 0..sorted.len() - 1 {
            assert!(sorted[i].metrics.sharpe_ratio <= sorted[i + 1].metrics.sharpe_ratio);
        }
    }

    #[test]
    fn test_query_combined_filters() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(20);
        store.save_batch(&results).unwrap();

        let filtered = store.query(
            ResultsQuery::new()
                .with_stage(ValidationStageType::Backtest)
                .with_min_sharpe(0.8)
                .with_limit(3)
        ).unwrap();

        assert!(filtered.len() <= 3);
        assert!(filtered.iter().all(|r| r.stage_type == ValidationStageType::Backtest));
        assert!(filtered.iter().all(|r| r.metrics.sharpe_ratio >= 0.8));
    }

    #[test]
    fn test_count() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(10);
        store.save_batch(&results).unwrap();

        let count = store.count(&ResultsQuery::new()).unwrap();

        assert_eq!(count, 10);
    }

    #[test]
    fn test_count_with_filter() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(10);
        store.save_batch(&results).unwrap();

        let count = store.count(
            &ResultsQuery::new().with_stage(ValidationStageType::Backtest)
        ).unwrap();

        assert!(count < 10);
    }

    // ==================== Aggregation Tests ====================

    #[test]
    fn test_aggregate_empty() {
        let (mut store, _temp_dir) = create_test_store();

        let metrics = store.aggregate(ResultsQuery::new()).unwrap();

        assert_eq!(metrics.count, 0);
    }

    #[test]
    fn test_aggregate_single() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        store.save(&result).unwrap();

        let metrics = store.aggregate(ResultsQuery::new()).unwrap();

        assert_eq!(metrics.count, 1);
        assert!((metrics.avg_sharpe - result.metrics.sharpe_ratio).abs() < 1e-10);
    }

    #[test]
    fn test_aggregate_multiple() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(10);
        store.save_batch(&results).unwrap();

        let metrics = store.aggregate(ResultsQuery::new()).unwrap();

        assert_eq!(metrics.count, 10);
        assert!(metrics.avg_sharpe > 0.0);
        assert!(metrics.min_sharpe <= metrics.avg_sharpe);
        assert!(metrics.max_sharpe >= metrics.avg_sharpe);
    }

    #[test]
    fn test_aggregate_by_stage() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(12);
        store.save_batch(&results).unwrap();

        let metrics = store.aggregate_by_stage(ValidationStageType::Backtest).unwrap();

        assert!(metrics.count > 0);
        assert_eq!(metrics.stage_type, Some(ValidationStageType::Backtest));
    }

    #[test]
    fn test_aggregate_by_config() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(12);
        store.save_batch(&results).unwrap();

        let metrics = store.aggregate_by_config("config_0").unwrap();

        assert!(metrics.count > 0);
    }

    #[test]
    fn test_aggregate_all_stages() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(20);
        store.save_batch(&results).unwrap();

        let aggregations = store.aggregate_all_stages().unwrap();

        assert!(aggregations.len() > 0);
    }

    #[test]
    fn test_aggregate_std_sharpe() {
        let (mut store, _temp_dir) = create_test_store();

        // Create results with different Sharpe ratios
        for sharpe in [0.5, 1.0, 1.5, 2.0] {
            let mut result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
            result.metrics.sharpe_ratio = sharpe;
            store.save(&result).unwrap();
        }

        let metrics = store.aggregate(ResultsQuery::new()).unwrap();

        assert!(metrics.std_sharpe > 0.0);
    }

    #[test]
    fn test_aggregate_pass_rate() {
        let (mut store, _temp_dir) = create_test_store();

        let mut result1 = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result1.passed = true;

        let mut result2 = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result2.passed = false;

        store.save(&result1).unwrap();
        store.save(&result2).unwrap();

        let metrics = store.aggregate(ResultsQuery::new()).unwrap();

        assert!((metrics.pass_rate - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_save_aggregation() {
        let (store, _temp_dir) = create_test_store();
        let metrics = AggregatedMetrics::default();

        let path = store.save_aggregation("test_agg", &metrics).unwrap();

        assert!(path.exists());
    }

    #[test]
    fn test_aggregated_metrics_is_positive() {
        let mut metrics = AggregatedMetrics::default();
        metrics.avg_sharpe = 1.0;
        metrics.avg_total_return = 0.1;

        assert!(metrics.is_positive());

        metrics.avg_sharpe = -0.5;
        assert!(!metrics.is_positive());
    }

    #[test]
    fn test_aggregated_metrics_meets_threshold() {
        let mut metrics = AggregatedMetrics::default();
        metrics.avg_sharpe = 1.0;
        metrics.pass_rate = 0.6;

        assert!(metrics.meets_quality_threshold(0.5, 0.5));
        assert!(!metrics.meets_quality_threshold(1.5, 0.5));
        assert!(!metrics.meets_quality_threshold(0.5, 0.8));
    }

    // ==================== Delete Tests ====================

    #[test]
    fn test_delete_nonexistent() {
        let (mut store, _temp_dir) = create_test_store();

        let deleted = store.delete("nonexistent").unwrap();

        assert!(!deleted);
    }

    #[test]
    fn test_delete_existing() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        let id = result.id.clone();

        store.save(&result).unwrap();

        let deleted = store.delete(&id).unwrap();

        assert!(deleted);
        assert!(store.load_by_id(&id).unwrap().is_none());
    }

    #[test]
    fn test_delete_clears_cache() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        let id = result.id.clone();

        store.save(&result).unwrap();
        assert!(store.get_cached(&id).is_some());

        store.delete(&id).unwrap();

        assert!(store.get_cached(&id).is_none());
    }

    #[test]
    fn test_delete_matching() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Forward)).unwrap();

        let deleted = store.delete_matching(
            ResultsQuery::new().with_stage(ValidationStageType::Backtest)
        ).unwrap();

        assert_eq!(deleted, 2);
        assert_eq!(store.total_count().unwrap(), 1);
    }

    #[test]
    fn test_delete_before() {
        let (mut store, _temp_dir) = create_test_store();

        let now = Utc::now();

        let mut old_result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::hours(3),
            now - chrono::Duration::hours(2),
        );
        old_result.add_metadata("symbol".to_string(), "BTCUSDT".to_string());

        let mut new_result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::minutes(30),
            now,
        );
        new_result.add_metadata("symbol".to_string(), "BTCUSDT".to_string());

        store.save(&old_result).unwrap();
        store.save(&new_result).unwrap();

        let deleted = store.delete_before(now - chrono::Duration::hours(1)).unwrap();

        assert_eq!(deleted, 1);
        assert_eq!(store.total_count().unwrap(), 1);
    }

    // ==================== List Tests ====================

    #[test]
    fn test_list_ids_empty() {
        let (store, _temp_dir) = create_test_store();

        let ids = store.list_ids().unwrap();

        assert!(ids.is_empty());
    }

    #[test]
    fn test_list_ids() {
        let (mut store, _temp_dir) = create_test_store();
        let results = create_varied_results(5);
        store.save_batch(&results).unwrap();

        let ids = store.list_ids().unwrap();

        assert_eq!(ids.len(), 5);
    }

    #[test]
    fn test_list_configs() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result_with_config("BTCUSDT", "config_a")).unwrap();
        store.save(&create_test_result_with_config("BTCUSDT", "config_b")).unwrap();
        store.save(&create_test_result_with_config("BTCUSDT", "config_a")).unwrap();

        let configs = store.list_configs().unwrap();

        assert_eq!(configs.len(), 2);
        assert!(configs.contains(&"config_a".to_string()));
        assert!(configs.contains(&"config_b".to_string()));
    }

    #[test]
    fn test_list_symbols() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();
        store.save(&create_test_result("ETHUSDT", ValidationStageType::Backtest)).unwrap();
        store.save(&create_test_result("SOLUSDT", ValidationStageType::Backtest)).unwrap();

        let symbols = store.list_symbols().unwrap();

        assert_eq!(symbols.len(), 3);
    }

    #[test]
    fn test_total_count() {
        let (mut store, _temp_dir) = create_test_store();

        assert_eq!(store.total_count().unwrap(), 0);

        let results = create_varied_results(7);
        store.save_batch(&results).unwrap();

        assert_eq!(store.total_count().unwrap(), 7);
    }

    #[test]
    fn test_disk_usage() {
        let (mut store, _temp_dir) = create_test_store();

        let initial_usage = store.disk_usage().unwrap();

        let results = create_varied_results(10);
        store.save_batch(&results).unwrap();

        let final_usage = store.disk_usage().unwrap();

        assert!(final_usage > initial_usage);
    }

    // ==================== Cache Tests ====================

    #[test]
    fn test_clear_cache() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        store.save(&result).unwrap();
        assert!(store.cache_size() > 0);

        store.clear_cache();

        assert_eq!(store.cache_size(), 0);
    }

    #[test]
    fn test_update_cache() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        let id = result.id.clone();

        store.update_cache(result);

        assert!(store.get_cached(&id).is_some());
        assert_eq!(store.total_count().unwrap(), 0); // Not saved to disk
    }

    #[test]
    fn test_cache_size() {
        let (mut store, _temp_dir) = create_test_store();

        assert_eq!(store.cache_size(), 0);

        let results = create_varied_results(5);
        store.save_batch(&results).unwrap();

        assert_eq!(store.cache_size(), 5);
    }

    // ==================== Audit Log Tests ====================

    #[test]
    fn test_audit_log_save() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        store.save(&result).unwrap();

        assert_eq!(store.audit_log().len(), 1);
        assert_eq!(store.audit_log()[0].operation, ResultsAuditOperation::Save);
    }

    #[test]
    fn test_audit_log_query() {
        let (mut store, _temp_dir) = create_test_store();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();

        store.query(ResultsQuery::new()).unwrap();

        assert!(store.audit_log().iter().any(|e| e.operation == ResultsAuditOperation::Query));
    }

    #[test]
    fn test_audit_log_aggregate() {
        let (mut store, _temp_dir) = create_test_store();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();

        store.aggregate(ResultsQuery::new()).unwrap();

        assert!(store.audit_log().iter().any(|e| e.operation == ResultsAuditOperation::Aggregate));
    }

    #[test]
    fn test_audit_log_disabled() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResultsStoreConfig::with_path(temp_dir.path()).without_audit();
        let mut store = ResultsStore::new(config).unwrap();

        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();

        assert!(store.audit_log().is_empty());
    }

    #[test]
    fn test_flush_audit_log() {
        let (mut store, _temp_dir) = create_test_store();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();

        store.flush_audit_log().unwrap();

        assert!(store.audit_log().is_empty());

        let audit_dir = store.config.base_path.join("audit");
        let audit_files: Vec<_> = fs::read_dir(&audit_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();

        assert_eq!(audit_files.len(), 1);
    }

    #[test]
    fn test_load_audit_log() {
        let (mut store, _temp_dir) = create_test_store();
        store.save(&create_test_result("BTCUSDT", ValidationStageType::Backtest)).unwrap();
        store.query(ResultsQuery::new()).unwrap();
        store.flush_audit_log().unwrap();

        let entries = store.load_audit_log().unwrap();

        assert_eq!(entries.len(), 2);
    }

    // ==================== Research State Link Tests ====================

    #[test]
    fn test_get_results_for_research_state() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result_with_state("BTCUSDT", "state_1")).unwrap();
        store.save(&create_test_result_with_state("BTCUSDT", "state_1")).unwrap();
        store.save(&create_test_result_with_state("BTCUSDT", "state_2")).unwrap();

        let results = store.get_results_for_research_state("state_1").unwrap();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_aggregate_for_research_state() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result_with_state("BTCUSDT", "state_1")).unwrap();
        store.save(&create_test_result_with_state("BTCUSDT", "state_1")).unwrap();

        let metrics = store.aggregate_for_research_state("state_1").unwrap();

        assert_eq!(metrics.count, 2);
    }

    #[test]
    fn test_has_results_for_research_state() {
        let (mut store, _temp_dir) = create_test_store();

        store.save(&create_test_result_with_state("BTCUSDT", "state_1")).unwrap();

        assert!(store.has_results_for_research_state("state_1").unwrap());
        assert!(!store.has_results_for_research_state("state_2").unwrap());
    }

    // ==================== ResultsQuery Tests ====================

    #[test]
    fn test_query_builder() {
        let query = ResultsQuery::new()
            .with_stage(ValidationStageType::Backtest)
            .with_config("my_config")
            .with_symbol("BTCUSDT")
            .with_min_sharpe(1.0)
            .with_max_drawdown(0.1)
            .passed_only()
            .with_limit(10)
            .sorted_by(SortField::Sharpe, false);

        assert_eq!(query.stage_type, Some(ValidationStageType::Backtest));
        assert_eq!(query.config_id, Some("my_config".to_string()));
        assert_eq!(query.symbol, Some("BTCUSDT".to_string()));
        assert_eq!(query.min_sharpe, Some(1.0));
        assert_eq!(query.max_drawdown, Some(0.1));
        assert!(query.passed_only);
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.sort_by, Some(SortField::Sharpe));
    }

    #[test]
    fn test_query_matches_stage() {
        let query = ResultsQuery::new().with_stage(ValidationStageType::Backtest);

        let backtest = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        let forward = create_test_result("BTCUSDT", ValidationStageType::Forward);

        assert!(query.matches(&backtest));
        assert!(!query.matches(&forward));
    }

    #[test]
    fn test_query_matches_config() {
        let query = ResultsQuery::new().with_config("config_a");

        let config_a = create_test_result_with_config("BTCUSDT", "config_a");
        let config_b = create_test_result_with_config("BTCUSDT", "config_b");

        assert!(query.matches(&config_a));
        assert!(!query.matches(&config_b));
    }

    #[test]
    fn test_query_matches_symbol() {
        let query = ResultsQuery::new().with_symbol("BTCUSDT");

        let btc = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        let eth = create_test_result("ETHUSDT", ValidationStageType::Backtest);

        assert!(query.matches(&btc));
        assert!(!query.matches(&eth));
    }

    #[test]
    fn test_query_matches_sharpe() {
        let query = ResultsQuery::new().with_min_sharpe(1.0);

        let mut high_sharpe = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        high_sharpe.metrics.sharpe_ratio = 1.5;

        let mut low_sharpe = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        low_sharpe.metrics.sharpe_ratio = 0.5;

        assert!(query.matches(&high_sharpe));
        assert!(!query.matches(&low_sharpe));
    }

    #[test]
    fn test_query_matches_passed() {
        let query = ResultsQuery::new().passed_only();

        let mut passed = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        passed.passed = true;

        let mut failed = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        failed.passed = false;

        assert!(query.matches(&passed));
        assert!(!query.matches(&failed));
    }

    // ==================== AuditEntry Tests ====================

    #[test]
    fn test_audit_entry_new() {
        let entry = ResultsAuditEntry::new(ResultsAuditOperation::Save, "test-id");

        assert_eq!(entry.result_id, "test-id");
        assert_eq!(entry.operation, ResultsAuditOperation::Save);
    }

    #[test]
    fn test_audit_entry_with_stage() {
        let entry = ResultsAuditEntry::new(ResultsAuditOperation::Save, "test-id")
            .with_stage(ValidationStageType::Backtest);

        assert_eq!(entry.stage_type, Some(ValidationStageType::Backtest));
    }

    #[test]
    fn test_audit_entry_with_config() {
        let entry = ResultsAuditEntry::new(ResultsAuditOperation::Save, "test-id")
            .with_config("my_config");

        assert_eq!(entry.config_id, Some("my_config".to_string()));
    }

    #[test]
    fn test_audit_entry_with_metadata() {
        let entry = ResultsAuditEntry::new(ResultsAuditOperation::Save, "test-id")
            .with_metadata("key", "value");

        assert_eq!(entry.metadata.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_audit_entry_serialization() {
        let entry = ResultsAuditEntry::new(ResultsAuditOperation::Save, "test-id")
            .with_stage(ValidationStageType::Backtest)
            .with_config("config")
            .with_metadata("key", "value");

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: ResultsAuditEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.result_id, entry.result_id);
        assert_eq!(deserialized.operation, entry.operation);
    }

    // ==================== AggregatedMetrics Tests ====================

    #[test]
    fn test_aggregated_metrics_default() {
        let metrics = AggregatedMetrics::default();

        assert_eq!(metrics.count, 0);
        assert_eq!(metrics.avg_sharpe, 0.0);
    }

    #[test]
    fn test_aggregated_metrics_from_empty() {
        let metrics = AggregatedMetrics::from_results(&[]);

        assert_eq!(metrics.count, 0);
    }

    #[test]
    fn test_aggregated_metrics_from_single() {
        let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);

        let metrics = AggregatedMetrics::from_results(&[result.clone()]);

        assert_eq!(metrics.count, 1);
        assert!((metrics.avg_sharpe - result.metrics.sharpe_ratio).abs() < 1e-10);
        assert_eq!(metrics.std_sharpe, 0.0); // Single value, no variance
    }

    #[test]
    fn test_aggregated_metrics_from_multiple() {
        let results = create_varied_results(5);

        let metrics = AggregatedMetrics::from_results(&results);

        assert_eq!(metrics.count, 5);
        assert!(metrics.min_sharpe <= metrics.avg_sharpe);
        assert!(metrics.max_sharpe >= metrics.avg_sharpe);
    }

    #[test]
    fn test_aggregated_metrics_time_range() {
        let now = Utc::now();

        let mut result1 = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::hours(3),
            now - chrono::Duration::hours(2),
        );
        result1.add_metadata("symbol".to_string(), "BTCUSDT".to_string());

        let mut result2 = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::hours(1),
            now,
        );
        result2.add_metadata("symbol".to_string(), "BTCUSDT".to_string());

        let metrics = AggregatedMetrics::from_results(&[result1.clone(), result2.clone()]);

        assert_eq!(metrics.start_time, Some(result1.period_start));
        assert_eq!(metrics.end_time, Some(result2.period_end));
    }

    #[test]
    fn test_aggregated_metrics_serialization() {
        let results = create_varied_results(3);
        let metrics = AggregatedMetrics::from_results(&results);

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: AggregatedMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.count, metrics.count);
        assert!((deserialized.avg_sharpe - metrics.avg_sharpe).abs() < 1e-10);
    }

    // ==================== Integration Tests ====================

    #[test]
    fn test_full_workflow() {
        let (mut store, _temp_dir) = create_test_store();

        // Save results
        let results = create_varied_results(10);
        store.save_batch(&results).unwrap();

        // Query results
        let backtests = store.load_by_stage(ValidationStageType::Backtest).unwrap();
        assert!(backtests.len() > 0);

        // Aggregate
        let metrics = store.aggregate(ResultsQuery::new()).unwrap();
        assert_eq!(metrics.count, 10);

        // Delete some
        let deleted = store.delete_matching(
            ResultsQuery::new().with_stage(ValidationStageType::Backtest)
        ).unwrap();
        assert!(deleted > 0);

        // Flush audit log
        store.flush_audit_log().unwrap();

        // Load audit log
        let audit = store.load_audit_log().unwrap();
        assert!(!audit.is_empty());
    }

    #[test]
    fn test_persistence_across_sessions() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();
        let result_id;

        // First session
        {
            let mut store = ResultsStore::at_path(&path).unwrap();
            let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
            result_id = result.id.clone();
            store.save(&result).unwrap();
        }

        // Second session
        {
            let mut store = ResultsStore::at_path(&path).unwrap();
            let loaded = store.load_by_id(&result_id).unwrap();
            assert!(loaded.is_some());
            assert_eq!(loaded.unwrap().metadata.get("symbol"), Some(&"BTCUSDT".to_string()));
        }
    }

    #[test]
    fn test_multiple_symbols_workflow() {
        let (mut store, _temp_dir) = create_test_store();

        let symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"];

        for symbol in symbols {
            for _ in 0..3 {
                store.save(&create_test_result(symbol, ValidationStageType::Backtest)).unwrap();
            }
        }

        let stored_symbols = store.list_symbols().unwrap();
        assert_eq!(stored_symbols.len(), 4);

        for symbol in symbols {
            let results = store.load_by_symbol(symbol).unwrap();
            assert_eq!(results.len(), 3);
        }
    }

    #[test]
    fn test_research_state_linking_workflow() {
        let (mut store, _temp_dir) = create_test_store();

        // Simulate multiple validation runs for same research state
        for i in 0..5 {
            let mut result = create_test_result_with_state("BTCUSDT", "research_state_abc");
            result.metrics.sharpe_ratio = 0.5 + (i as f64 * 0.2);
            store.save(&result).unwrap();
        }

        // Query all results for this research state
        let results = store.get_results_for_research_state("research_state_abc").unwrap();
        assert_eq!(results.len(), 5);

        // Aggregate metrics
        let metrics = store.aggregate_for_research_state("research_state_abc").unwrap();
        assert_eq!(metrics.count, 5);

        // Check if has results
        assert!(store.has_results_for_research_state("research_state_abc").unwrap());
        assert!(!store.has_results_for_research_state("nonexistent").unwrap());
    }

    #[test]
    fn test_complex_query_workflow() {
        let (mut store, _temp_dir) = create_test_store();

        // Create varied results
        for i in 0..20 {
            let now = Utc::now();
            let mut result = ValidationResult::new(
                if i % 3 == 0 { ValidationStageType::Backtest } else { ValidationStageType::Forward },
                format!("Test-{}", i),
                format!("config_{}", i % 3),
                now - chrono::Duration::hours(1),
                now,
            );
            result.metrics.sharpe_ratio = (i as f64 * 0.1) - 0.5;
            result.passed = i % 2 == 0;
            result.add_metadata("symbol".to_string(), if i % 2 == 0 { "BTCUSDT" } else { "ETHUSDT" }.to_string());
            store.save(&result).unwrap();
        }

        // Complex query
        let results = store.query(
            ResultsQuery::new()
                .with_symbol("BTCUSDT")
                .with_stage(ValidationStageType::Backtest)
                .with_min_sharpe(0.0)
                .passed_only()
                .sorted_by(SortField::Sharpe, false)
                .with_limit(5)
        ).unwrap();

        // Verify all filters applied
        assert!(results.len() <= 5);
        for result in &results {
            assert_eq!(result.metadata.get("symbol"), Some(&"BTCUSDT".to_string()));
            assert_eq!(result.stage_type, ValidationStageType::Backtest);
            assert!(result.metrics.sharpe_ratio >= 0.0);
            assert!(result.passed);
        }

        // Verify sorting
        for i in 0..results.len().saturating_sub(1) {
            assert!(results[i].metrics.sharpe_ratio >= results[i + 1].metrics.sharpe_ratio);
        }
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_empty_symbol_metadata() {
        let (mut store, _temp_dir) = create_test_store();
        let now = Utc::now();
        let result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Test".to_string(),
            "config".to_string(),
            now - chrono::Duration::hours(1),
            now,
        );
        // No symbol in metadata

        let path = store.save(&result).unwrap();

        assert!(path.exists());
    }

    #[test]
    fn test_special_characters_in_config() {
        let (mut store, _temp_dir) = create_test_store();
        let result = create_test_result_with_config("BTCUSDT", "config-with-dashes_and_underscores");

        store.save(&result).unwrap();

        let configs = store.list_configs().unwrap();
        assert!(configs.contains(&"config-with-dashes_and_underscores".to_string()));
    }

    #[test]
    fn test_very_long_config_id() {
        let (mut store, _temp_dir) = create_test_store();
        let long_config = "a".repeat(200);
        let result = create_test_result_with_config("BTCUSDT", &long_config);

        store.save(&result).unwrap();

        let loaded = store.load_by_config(&long_config).unwrap();
        assert_eq!(loaded.len(), 1);
    }

    #[test]
    fn test_extreme_sharpe_values() {
        let (mut store, _temp_dir) = create_test_store();

        let mut result1 = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result1.metrics.sharpe_ratio = 100.0;

        let mut result2 = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result2.metrics.sharpe_ratio = -100.0;

        store.save(&result1).unwrap();
        store.save(&result2).unwrap();

        let metrics = store.aggregate(ResultsQuery::new()).unwrap();

        assert!(!metrics.avg_sharpe.is_nan());
    }

    #[test]
    fn test_negative_metrics() {
        let (mut store, _temp_dir) = create_test_store();

        let mut result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
        result.metrics.sharpe_ratio = -2.0;
        result.metrics.total_pnl = -0.5;

        store.save(&result).unwrap();

        let loaded = store.load_by_id(&result.id).unwrap().unwrap();

        assert!((loaded.metrics.sharpe_ratio - (-2.0)).abs() < 1e-10);
    }

    #[test]
    fn test_concurrent_saves() {
        let (mut store, _temp_dir) = create_test_store();

        // Simulate rapid saves (basic test - real concurrency would need threads)
        for _ in 0..50 {
            let result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
            store.save(&result).unwrap();
        }

        assert_eq!(store.total_count().unwrap(), 50);
    }

    #[test]
    fn test_load_corrupted_file() {
        let (mut store, temp_dir) = create_test_store();

        // Create a corrupted JSON file
        let bad_path = temp_dir.path().join("results").join("BTCUSDT_backtest_default_20251219_120000.000.json");
        fs::write(&bad_path, "{ this is not valid json }").unwrap();

        // Query should fail or skip the corrupted file
        let result = store.query(ResultsQuery::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_query_on_nonexistent_directory() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResultsStoreConfig::with_path(temp_dir.path().join("nonexistent"));

        // This should not fail - it creates directories
        let result = ResultsStore::new(config);
        assert!(result.is_ok());
    }

    // ==================== Sort Field Tests ====================

    #[test]
    fn test_sort_by_total_return() {
        let (mut store, _temp_dir) = create_test_store();

        for i in 0..5 {
            let mut result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
            result.metrics.total_pnl = i as f64 * 0.1;
            store.save(&result).unwrap();
        }

        let sorted = store.query(
            ResultsQuery::new().sorted_by(SortField::TotalReturn, false)
        ).unwrap();

        for i in 0..sorted.len() - 1 {
            assert!(sorted[i].metrics.total_pnl >= sorted[i + 1].metrics.total_pnl);
        }
    }

    #[test]
    fn test_sort_by_win_rate() {
        let (mut store, _temp_dir) = create_test_store();

        for i in 0..5 {
            let mut result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
            result.metrics.win_rate = 0.3 + (i as f64 * 0.1);
            store.save(&result).unwrap();
        }

        let sorted = store.query(
            ResultsQuery::new().sorted_by(SortField::WinRate, true)
        ).unwrap();

        for i in 0..sorted.len() - 1 {
            assert!(sorted[i].metrics.win_rate <= sorted[i + 1].metrics.win_rate);
        }
    }

    #[test]
    fn test_sort_by_max_drawdown() {
        let (mut store, _temp_dir) = create_test_store();

        for i in 0..5 {
            let mut result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
            result.metrics.max_drawdown_pct = 0.05 + (i as f64 * 0.02);
            store.save(&result).unwrap();
        }

        let sorted = store.query(
            ResultsQuery::new().sorted_by(SortField::MaxDrawdown, true)
        ).unwrap();

        for i in 0..sorted.len() - 1 {
            assert!(sorted[i].metrics.max_drawdown_pct <= sorted[i + 1].metrics.max_drawdown_pct);
        }
    }

    #[test]
    fn test_sort_by_trade_count() {
        let (mut store, _temp_dir) = create_test_store();

        for i in 0..5 {
            let mut result = create_test_result("BTCUSDT", ValidationStageType::Backtest);
            result.metrics.trade_count = 10 + i * 20;
            store.save(&result).unwrap();
        }

        let sorted = store.query(
            ResultsQuery::new().sorted_by(SortField::TradeCount, false)
        ).unwrap();

        for i in 0..sorted.len() - 1 {
            assert!(sorted[i].metrics.trade_count >= sorted[i + 1].metrics.trade_count);
        }
    }

    #[test]
    fn test_sort_by_timestamp() {
        let (mut store, _temp_dir) = create_test_store();

        let now = Utc::now();
        for i in 0..5 {
            let mut result = ValidationResult::new(
                ValidationStageType::Backtest,
                format!("Test-{}", i),
                "config".to_string(),
                now - chrono::Duration::hours(5 - i as i64),
                now - chrono::Duration::hours(4 - i as i64),
            );
            result.add_metadata("symbol".to_string(), "BTCUSDT".to_string());
            store.save(&result).unwrap();
        }

        let sorted = store.query(
            ResultsQuery::new().sorted_by(SortField::Timestamp, true)
        ).unwrap();

        for i in 0..sorted.len() - 1 {
            assert!(sorted[i].period_start <= sorted[i + 1].period_start);
        }
    }
}

//! Config Store - Persistence layer for AlgorithmConfig (Task 0.5)
//!
//! This module provides:
//! - Save AlgorithmConfig to disk (JSON + optional Parquet)
//! - Load configs by ID
//! - Track config lineage (which research state generated it)
//! - Support config comparison and version tracking
//! - List all configs with filtering
//! - Audit logging for all operations

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::algorithm_config::{AlgorithmConfig, StrategyType};

// ==================== Configuration ====================

/// Configuration for the config store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigStoreConfig {
    /// Base path for storing configs
    pub base_path: PathBuf,
    /// Maximum number of config versions to keep per name
    pub max_versions_per_name: usize,
    /// Enable compression for JSON files
    pub compress: bool,
    /// Enable audit logging
    pub enable_audit_log: bool,
    /// Enable Parquet index for fast queries
    pub enable_parquet_index: bool,
    /// Cache configs in memory
    pub enable_cache: bool,
}

impl Default for ConfigStoreConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./data/configs"),
            max_versions_per_name: 100,
            compress: true,
            enable_audit_log: true,
            enable_parquet_index: true,
            enable_cache: true,
        }
    }
}

impl ConfigStoreConfig {
    /// Create config with a specific path
    pub fn with_path(path: impl AsRef<Path>) -> Self {
        Self {
            base_path: path.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    /// Disable Parquet index
    pub fn without_parquet(mut self) -> Self {
        self.enable_parquet_index = false;
        self
    }

    /// Disable audit logging
    pub fn without_audit(mut self) -> Self {
        self.enable_audit_log = false;
        self
    }

    /// Disable caching
    pub fn without_cache(mut self) -> Self {
        self.enable_cache = false;
        self
    }

    /// Set max versions per name
    pub fn with_max_versions(mut self, max: usize) -> Self {
        self.max_versions_per_name = max;
        self
    }
}

// ==================== Audit Log ====================

/// Types of operations that can be audited
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigAuditOperation {
    Save,
    Load,
    Delete,
    List,
    Compare,
    Archive,
}

impl std::fmt::Display for ConfigAuditOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigAuditOperation::Save => write!(f, "SAVE"),
            ConfigAuditOperation::Load => write!(f, "LOAD"),
            ConfigAuditOperation::Delete => write!(f, "DELETE"),
            ConfigAuditOperation::List => write!(f, "LIST"),
            ConfigAuditOperation::Compare => write!(f, "COMPARE"),
            ConfigAuditOperation::Archive => write!(f, "ARCHIVE"),
        }
    }
}

/// A single audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigAuditEntry {
    pub timestamp: DateTime<Utc>,
    pub operation: ConfigAuditOperation,
    pub config_id: String,
    pub config_name: Option<String>,
    pub version: Option<u32>,
    pub source_research_id: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl ConfigAuditEntry {
    pub fn new(operation: ConfigAuditOperation, config_id: &str) -> Self {
        Self {
            timestamp: Utc::now(),
            operation,
            config_id: config_id.to_string(),
            config_name: None,
            version: None,
            source_research_id: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.config_name = Some(name.to_string());
        self
    }

    pub fn with_version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    pub fn with_research_state(mut self, state_id: &str) -> Self {
        self.source_research_id = Some(state_id.to_string());
        self
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

// ==================== Query Filters ====================

/// Filter criteria for querying configs
#[derive(Debug, Clone, Default)]
pub struct ConfigQuery {
    /// Filter by config name (partial match)
    pub name_contains: Option<String>,
    /// Filter by exact config name
    pub name_exact: Option<String>,
    /// Filter by symbol
    pub symbol: Option<String>,
    /// Filter by strategy type
    pub strategy_type: Option<StrategyType>,
    /// Filter by research state ID
    pub source_research_id: Option<String>,
    /// Filter by active status
    pub active_only: bool,
    /// Filter by minimum version
    pub min_version: Option<u32>,
    /// Filter by maximum version
    pub max_version: Option<u32>,
    /// Filter by creation time start
    pub created_after: Option<DateTime<Utc>>,
    /// Filter by creation time end
    pub created_before: Option<DateTime<Utc>>,
    /// Limit number of results
    pub limit: Option<usize>,
    /// Sort by field
    pub sort_by: Option<ConfigSortField>,
    /// Sort ascending (default: false = descending)
    pub sort_ascending: bool,
}

/// Fields that can be sorted by
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSortField {
    Name,
    Symbol,
    Version,
    CreatedAt,
    StrategyType,
}

impl ConfigQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name_contains(mut self, name: &str) -> Self {
        self.name_contains = Some(name.to_string());
        self
    }

    pub fn with_name_exact(mut self, name: &str) -> Self {
        self.name_exact = Some(name.to_string());
        self
    }

    pub fn with_symbol(mut self, symbol: &str) -> Self {
        self.symbol = Some(symbol.to_string());
        self
    }

    pub fn with_strategy_type(mut self, strategy: StrategyType) -> Self {
        self.strategy_type = Some(strategy);
        self
    }

    pub fn with_research_state(mut self, state_id: &str) -> Self {
        self.source_research_id = Some(state_id.to_string());
        self
    }

    pub fn active_only(mut self) -> Self {
        self.active_only = true;
        self
    }

    pub fn with_version_range(mut self, min: Option<u32>, max: Option<u32>) -> Self {
        self.min_version = min;
        self.max_version = max;
        self
    }

    pub fn with_created_range(mut self, after: Option<DateTime<Utc>>, before: Option<DateTime<Utc>>) -> Self {
        self.created_after = after;
        self.created_before = before;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn sorted_by(mut self, field: ConfigSortField, ascending: bool) -> Self {
        self.sort_by = Some(field);
        self.sort_ascending = ascending;
        self
    }

    /// Check if a config matches this query
    pub fn matches(&self, config: &AlgorithmConfig) -> bool {
        // Name contains filter
        if let Some(ref name) = self.name_contains {
            if !config.name.to_lowercase().contains(&name.to_lowercase()) {
                return false;
            }
        }

        // Exact name filter
        if let Some(ref name) = self.name_exact {
            if &config.name != name {
                return false;
            }
        }

        // Symbol filter
        if let Some(ref symbol) = self.symbol {
            if &config.symbol != symbol {
                return false;
            }
        }

        // Strategy type filter
        if let Some(strategy) = self.strategy_type {
            if config.strategy_type != strategy {
                return false;
            }
        }

        // Research state ID filter
        if let Some(ref state_id) = self.source_research_id {
            if config.source_research_id.as_ref() != Some(state_id) {
                return false;
            }
        }

        // Active filter
        if self.active_only && !config.active {
            return false;
        }

        // Version range filter
        if let Some(min_ver) = self.min_version {
            if config.version < min_ver {
                return false;
            }
        }
        if let Some(max_ver) = self.max_version {
            if config.version > max_ver {
                return false;
            }
        }

        // Creation time filter
        if let Some(after) = self.created_after {
            if config.created_at < after {
                return false;
            }
        }
        if let Some(before) = self.created_before {
            if config.created_at > before {
                return false;
            }
        }

        true
    }
}

// ==================== Config Comparison ====================

/// Result of comparing two configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDiff {
    /// ID of the first config
    pub config_a_id: String,
    /// ID of the second config
    pub config_b_id: String,
    /// Whether the configs are identical
    pub identical: bool,
    /// List of differences
    pub differences: Vec<ConfigDifference>,
    /// Timestamp of comparison
    pub compared_at: DateTime<Utc>,
}

/// A single difference between two configs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDifference {
    /// Path to the differing field (e.g., "entry.min_momentum_signal")
    pub field_path: String,
    /// Value in config A
    pub value_a: String,
    /// Value in config B
    pub value_b: String,
    /// Description of the change
    pub description: String,
}

impl ConfigDiff {
    /// Create a new config diff
    pub fn new(config_a: &AlgorithmConfig, config_b: &AlgorithmConfig) -> Self {
        let mut differences = Vec::new();

        // Compare basic fields
        if config_a.name != config_b.name {
            differences.push(ConfigDifference {
                field_path: "name".to_string(),
                value_a: config_a.name.clone(),
                value_b: config_b.name.clone(),
                description: "Config name changed".to_string(),
            });
        }

        if config_a.symbol != config_b.symbol {
            differences.push(ConfigDifference {
                field_path: "symbol".to_string(),
                value_a: config_a.symbol.clone(),
                value_b: config_b.symbol.clone(),
                description: "Trading symbol changed".to_string(),
            });
        }

        if config_a.strategy_type != config_b.strategy_type {
            differences.push(ConfigDifference {
                field_path: "strategy_type".to_string(),
                value_a: format!("{:?}", config_a.strategy_type),
                value_b: format!("{:?}", config_b.strategy_type),
                description: "Strategy type changed".to_string(),
            });
        }

        if config_a.active != config_b.active {
            differences.push(ConfigDifference {
                field_path: "active".to_string(),
                value_a: config_a.active.to_string(),
                value_b: config_b.active.to_string(),
                description: "Active status changed".to_string(),
            });
        }

        // Compare entry params
        Self::compare_entry_params(&config_a.entry, &config_b.entry, &mut differences);

        // Compare exit params
        Self::compare_exit_params(&config_a.exit, &config_b.exit, &mut differences);

        // Compare position params
        Self::compare_position_params(&config_a.position, &config_b.position, &mut differences);

        // Compare regime filters
        Self::compare_regime_filters(&config_a.regime_filters, &config_b.regime_filters, &mut differences);

        Self {
            config_a_id: config_a.id.clone(),
            config_b_id: config_b.id.clone(),
            identical: differences.is_empty(),
            differences,
            compared_at: Utc::now(),
        }
    }

    fn compare_entry_params(
        a: &super::algorithm_config::EntryParams,
        b: &super::algorithm_config::EntryParams,
        diffs: &mut Vec<ConfigDifference>,
    ) {
        if (a.min_momentum_signal - b.min_momentum_signal).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "entry.min_momentum_signal".to_string(),
                value_a: format!("{:.4}", a.min_momentum_signal),
                value_b: format!("{:.4}", b.min_momentum_signal),
                description: "Entry momentum threshold changed".to_string(),
            });
        }
        if (a.min_monotonicity - b.min_monotonicity).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "entry.min_monotonicity".to_string(),
                value_a: format!("{:.4}", a.min_monotonicity),
                value_b: format!("{:.4}", b.min_monotonicity),
                description: "Entry monotonicity threshold changed".to_string(),
            });
        }
        if (a.min_hurst - b.min_hurst).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "entry.min_hurst".to_string(),
                value_a: format!("{:.4}", a.min_hurst),
                value_b: format!("{:.4}", b.min_hurst),
                description: "Entry Hurst threshold changed".to_string(),
            });
        }
        if (a.max_entry_entropy - b.max_entry_entropy).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "entry.max_entry_entropy".to_string(),
                value_a: format!("{:.4}", a.max_entry_entropy),
                value_b: format!("{:.4}", b.max_entry_entropy),
                description: "Entry max entropy changed".to_string(),
            });
        }
        if (a.min_conditional_prob - b.min_conditional_prob).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "entry.min_conditional_prob".to_string(),
                value_a: format!("{:.4}", a.min_conditional_prob),
                value_b: format!("{:.4}", b.min_conditional_prob),
                description: "Entry conditional probability threshold changed".to_string(),
            });
        }
        if (a.min_confidence - b.min_confidence).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "entry.min_confidence".to_string(),
                value_a: format!("{:.4}", a.min_confidence),
                value_b: format!("{:.4}", b.min_confidence),
                description: "Entry confidence threshold changed".to_string(),
            });
        }
    }

    fn compare_exit_params(
        a: &super::algorithm_config::ExitParams,
        b: &super::algorithm_config::ExitParams,
        diffs: &mut Vec<ConfigDifference>,
    ) {
        if (a.take_profit_bps - b.take_profit_bps).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "exit.take_profit_bps".to_string(),
                value_a: format!("{:.2}", a.take_profit_bps),
                value_b: format!("{:.2}", b.take_profit_bps),
                description: "Take profit changed".to_string(),
            });
        }
        if (a.stop_loss_bps - b.stop_loss_bps).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "exit.stop_loss_bps".to_string(),
                value_a: format!("{:.2}", a.stop_loss_bps),
                value_b: format!("{:.2}", b.stop_loss_bps),
                description: "Stop loss changed".to_string(),
            });
        }
        if a.max_hold_seconds != b.max_hold_seconds {
            diffs.push(ConfigDifference {
                field_path: "exit.max_hold_seconds".to_string(),
                value_a: format!("{}", a.max_hold_seconds),
                value_b: format!("{}", b.max_hold_seconds),
                description: "Max hold time changed".to_string(),
            });
        }
        if (a.trailing_stop_activation_bps - b.trailing_stop_activation_bps).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "exit.trailing_stop_activation_bps".to_string(),
                value_a: format!("{:.2}", a.trailing_stop_activation_bps),
                value_b: format!("{:.2}", b.trailing_stop_activation_bps),
                description: "Trailing stop activation changed".to_string(),
            });
        }
        if a.use_time_exit != b.use_time_exit {
            diffs.push(ConfigDifference {
                field_path: "exit.use_time_exit".to_string(),
                value_a: a.use_time_exit.to_string(),
                value_b: b.use_time_exit.to_string(),
                description: "Time exit setting changed".to_string(),
            });
        }
    }

    fn compare_position_params(
        a: &super::algorithm_config::PositionParams,
        b: &super::algorithm_config::PositionParams,
        diffs: &mut Vec<ConfigDifference>,
    ) {
        if a.method != b.method {
            diffs.push(ConfigDifference {
                field_path: "position.method".to_string(),
                value_a: format!("{:?}", a.method),
                value_b: format!("{:?}", b.method),
                description: "Position sizing method changed".to_string(),
            });
        }
        if (a.base_size_fraction - b.base_size_fraction).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "position.base_size_fraction".to_string(),
                value_a: format!("{:.4}", a.base_size_fraction),
                value_b: format!("{:.4}", b.base_size_fraction),
                description: "Base position size changed".to_string(),
            });
        }
        if (a.max_size_fraction - b.max_size_fraction).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "position.max_size_fraction".to_string(),
                value_a: format!("{:.4}", a.max_size_fraction),
                value_b: format!("{:.4}", b.max_size_fraction),
                description: "Max position size changed".to_string(),
            });
        }
        if (a.target_volatility - b.target_volatility).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "position.target_volatility".to_string(),
                value_a: format!("{:.4}", a.target_volatility),
                value_b: format!("{:.4}", b.target_volatility),
                description: "Target volatility changed".to_string(),
            });
        }
    }

    fn compare_regime_filters(
        a: &super::algorithm_config::RegimeFilters,
        b: &super::algorithm_config::RegimeFilters,
        diffs: &mut Vec<ConfigDifference>,
    ) {
        if (a.min_tau_half - b.min_tau_half).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "regime_filters.min_tau_half".to_string(),
                value_a: format!("{:.2}", a.min_tau_half),
                value_b: format!("{:.2}", b.min_tau_half),
                description: "Min tau half changed".to_string(),
            });
        }
        if (a.max_entropy - b.max_entropy).abs() > 1e-9 {
            diffs.push(ConfigDifference {
                field_path: "regime_filters.max_entropy".to_string(),
                value_a: format!("{:.4}", a.max_entropy),
                value_b: format!("{:.4}", b.max_entropy),
                description: "Max entropy changed".to_string(),
            });
        }
        if a.required_regime != b.required_regime {
            diffs.push(ConfigDifference {
                field_path: "regime_filters.required_regime".to_string(),
                value_a: format!("{:?}", a.required_regime),
                value_b: format!("{:?}", b.required_regime),
                description: "Required regime changed".to_string(),
            });
        }
    }

    /// Returns true if the configs are identical
    pub fn is_identical(&self) -> bool {
        self.identical
    }

    /// Returns the number of differences
    pub fn diff_count(&self) -> usize {
        self.differences.len()
    }

    /// Get a summary of the differences
    pub fn summary(&self) -> String {
        if self.identical {
            return "Configurations are identical".to_string();
        }
        let mut summary = format!("Found {} difference(s):\n", self.differences.len());
        for diff in &self.differences {
            summary.push_str(&format!("  - {}: {} -> {}\n", diff.field_path, diff.value_a, diff.value_b));
        }
        summary
    }
}

// ==================== Config Summary ====================

/// Summary information about a config (for listing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSummary {
    pub id: String,
    pub name: String,
    pub symbol: String,
    pub version: u32,
    pub strategy_type: StrategyType,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub source_research_id: Option<String>,
}

impl From<&AlgorithmConfig> for ConfigSummary {
    fn from(config: &AlgorithmConfig) -> Self {
        Self {
            id: config.id.clone(),
            name: config.name.clone(),
            symbol: config.symbol.clone(),
            version: config.version,
            strategy_type: config.strategy_type,
            active: config.active,
            created_at: config.created_at,
            source_research_id: config.source_research_id.clone(),
        }
    }
}

// ==================== Config Store Statistics ====================

/// Statistics about the config store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigStoreStats {
    /// Total number of configs
    pub total_configs: usize,
    /// Number of active configs
    pub active_configs: usize,
    /// Number of unique config names
    pub unique_names: usize,
    /// Number of unique symbols
    pub unique_symbols: usize,
    /// Configs by strategy type
    pub by_strategy: HashMap<String, usize>,
    /// Configs by symbol
    pub by_symbol: HashMap<String, usize>,
    /// Oldest config timestamp
    pub oldest_config: Option<DateTime<Utc>>,
    /// Newest config timestamp
    pub newest_config: Option<DateTime<Utc>>,
    /// Total disk size in bytes
    pub disk_size_bytes: u64,
    /// Timestamp of statistics calculation
    pub calculated_at: DateTime<Utc>,
}

impl Default for ConfigStoreStats {
    fn default() -> Self {
        Self {
            total_configs: 0,
            active_configs: 0,
            unique_names: 0,
            unique_symbols: 0,
            by_strategy: HashMap::new(),
            by_symbol: HashMap::new(),
            oldest_config: None,
            newest_config: None,
            disk_size_bytes: 0,
            calculated_at: Utc::now(),
        }
    }
}

// ==================== Config Store ====================

/// Persistence store for algorithm configurations
pub struct ConfigStore {
    config: ConfigStoreConfig,
    cache: HashMap<String, AlgorithmConfig>,
    audit_log: Vec<ConfigAuditEntry>,
}

impl ConfigStore {
    /// Create a new config store with the given configuration
    pub fn new(config: ConfigStoreConfig) -> Result<Self> {
        // Create directory structure
        fs::create_dir_all(&config.base_path)?;
        fs::create_dir_all(config.base_path.join("configs"))?;
        fs::create_dir_all(config.base_path.join("parquet"))?;
        fs::create_dir_all(config.base_path.join("audit"))?;
        fs::create_dir_all(config.base_path.join("archive"))?;

        Ok(Self {
            config,
            cache: HashMap::new(),
            audit_log: Vec::new(),
        })
    }

    /// Create a store at the given path with default config
    pub fn at_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::new(ConfigStoreConfig::with_path(path))
    }

    /// Get the store configuration
    pub fn config(&self) -> &ConfigStoreConfig {
        &self.config
    }

    // ==================== Save Operations ====================

    /// Save an algorithm config to disk
    pub fn save(&mut self, config: &AlgorithmConfig) -> Result<PathBuf> {
        let configs_dir = self.config.base_path.join("configs");

        // Create filename with name, symbol, version, and ID
        let filename = format!(
            "{}_{}_{}_v{}_{}.json",
            config.symbol.to_lowercase(),
            config.name.to_lowercase().replace(' ', "_"),
            config.strategy_type.to_string().to_lowercase(),
            config.version,
            &config.id[..8]  // Use first 8 chars of ID for uniqueness
        );
        let path = configs_dir.join(&filename);

        // Serialize and write
        let json = serde_json::to_string_pretty(config)?;
        fs::write(&path, &json)?;

        // Update cache if enabled
        if self.config.enable_cache {
            self.cache.insert(config.id.clone(), config.clone());
        }

        // Write Parquet index if enabled
        if self.config.enable_parquet_index {
            self.update_parquet_index(config)?;
        }

        // Log audit entry
        if self.config.enable_audit_log {
            let entry = ConfigAuditEntry::new(ConfigAuditOperation::Save, &config.id)
                .with_name(&config.name)
                .with_version(config.version)
                .with_metadata("symbol", &config.symbol)
                .with_metadata("strategy", &config.strategy_type.to_string());

            if let Some(ref state_id) = config.source_research_id {
                self.audit_log.push(entry.with_research_state(state_id));
            } else {
                self.audit_log.push(entry);
            }

            self.flush_audit_log()?;
        }

        Ok(path)
    }

    /// Save multiple configs at once
    pub fn save_batch(&mut self, configs: &[AlgorithmConfig]) -> Result<Vec<PathBuf>> {
        configs.iter().map(|c| self.save(c)).collect()
    }

    // ==================== Load Operations ====================

    /// Load a config by ID
    pub fn load(&mut self, config_id: &str) -> Result<Option<AlgorithmConfig>> {
        // Check cache first
        if self.config.enable_cache {
            if let Some(config) = self.cache.get(config_id) {
                // Log audit entry
                if self.config.enable_audit_log {
                    let entry = ConfigAuditEntry::new(ConfigAuditOperation::Load, config_id)
                        .with_metadata("source", "cache");
                    self.audit_log.push(entry);
                }
                return Ok(Some(config.clone()));
            }
        }

        // Search in configs directory
        let configs_dir = self.config.base_path.join("configs");
        if !configs_dir.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&configs_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                if let Ok(content) = fs::read_to_string(&path) {
                    if let Ok(config) = serde_json::from_str::<AlgorithmConfig>(&content) {
                        if config.id == config_id {
                            // Update cache
                            if self.config.enable_cache {
                                self.cache.insert(config_id.to_string(), config.clone());
                            }

                            // Log audit entry
                            if self.config.enable_audit_log {
                                let entry = ConfigAuditEntry::new(ConfigAuditOperation::Load, config_id)
                                    .with_name(&config.name)
                                    .with_version(config.version)
                                    .with_metadata("source", "disk");
                                self.audit_log.push(entry);
                            }

                            return Ok(Some(config));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Load the latest version of a config by name and symbol
    pub fn load_latest(&mut self, name: &str, symbol: &str) -> Result<Option<AlgorithmConfig>> {
        let configs = self.list_all()?;
        let matching: Vec<_> = configs
            .into_iter()
            .filter(|c| c.name == name && c.symbol == symbol)
            .collect();

        if matching.is_empty() {
            return Ok(None);
        }

        // Find the one with highest version
        let latest = matching.into_iter().max_by_key(|c| c.version);

        if let Some(config) = latest {
            // Log audit entry
            if self.config.enable_audit_log {
                let entry = ConfigAuditEntry::new(ConfigAuditOperation::Load, &config.id)
                    .with_name(&config.name)
                    .with_version(config.version)
                    .with_metadata("method", "load_latest");
                self.audit_log.push(entry);
            }
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    /// Load a specific version of a config by name, symbol, and version
    pub fn load_version(&mut self, name: &str, symbol: &str, version: u32) -> Result<Option<AlgorithmConfig>> {
        let configs = self.list_all()?;
        let matching = configs
            .into_iter()
            .find(|c| c.name == name && c.symbol == symbol && c.version == version);

        if let Some(ref config) = matching {
            // Log audit entry
            if self.config.enable_audit_log {
                let entry = ConfigAuditEntry::new(ConfigAuditOperation::Load, &config.id)
                    .with_name(&config.name)
                    .with_version(config.version)
                    .with_metadata("method", "load_version");
                self.audit_log.push(entry);
            }
        }

        Ok(matching)
    }

    // ==================== List Operations ====================

    /// List all configs
    pub fn list_all(&self) -> Result<Vec<AlgorithmConfig>> {
        let configs_dir = self.config.base_path.join("configs");
        if !configs_dir.exists() {
            return Ok(Vec::new());
        }

        let mut configs = Vec::new();
        for entry in fs::read_dir(&configs_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                if let Ok(content) = fs::read_to_string(&path) {
                    if let Ok(config) = serde_json::from_str::<AlgorithmConfig>(&content) {
                        configs.push(config);
                    }
                }
            }
        }

        // Sort by created_at descending
        configs.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        Ok(configs)
    }

    /// List configs matching a query
    pub fn query(&self, query: &ConfigQuery) -> Result<Vec<AlgorithmConfig>> {
        let all_configs = self.list_all()?;
        let mut matched: Vec<_> = all_configs.into_iter().filter(|c| query.matches(c)).collect();

        // Apply sorting
        if let Some(sort_field) = query.sort_by {
            matched.sort_by(|a, b| {
                let cmp = match sort_field {
                    ConfigSortField::Name => a.name.cmp(&b.name),
                    ConfigSortField::Symbol => a.symbol.cmp(&b.symbol),
                    ConfigSortField::Version => a.version.cmp(&b.version),
                    ConfigSortField::CreatedAt => a.created_at.cmp(&b.created_at),
                    ConfigSortField::StrategyType => format!("{:?}", a.strategy_type).cmp(&format!("{:?}", b.strategy_type)),
                };
                if query.sort_ascending { cmp } else { cmp.reverse() }
            });
        }

        // Apply limit
        if let Some(limit) = query.limit {
            matched.truncate(limit);
        }

        Ok(matched)
    }

    /// List configs as summaries (lighter weight)
    pub fn list_summaries(&self) -> Result<Vec<ConfigSummary>> {
        let configs = self.list_all()?;
        Ok(configs.iter().map(ConfigSummary::from).collect())
    }

    /// List all versions of a config by name and symbol
    pub fn list_versions(&self, name: &str, symbol: &str) -> Result<Vec<AlgorithmConfig>> {
        let all_configs = self.list_all()?;
        let mut matching: Vec<_> = all_configs
            .into_iter()
            .filter(|c| c.name == name && c.symbol == symbol)
            .collect();

        // Sort by version descending
        matching.sort_by(|a, b| b.version.cmp(&a.version));

        Ok(matching)
    }

    // ==================== Delete Operations ====================

    /// Delete a config by ID
    pub fn delete(&mut self, config_id: &str) -> Result<bool> {
        let configs_dir = self.config.base_path.join("configs");
        if !configs_dir.exists() {
            return Ok(false);
        }

        for entry in fs::read_dir(&configs_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                if let Ok(content) = fs::read_to_string(&path) {
                    if let Ok(config) = serde_json::from_str::<AlgorithmConfig>(&content) {
                        if config.id == config_id {
                            // Remove from disk
                            fs::remove_file(&path)?;

                            // Remove from cache
                            self.cache.remove(config_id);

                            // Log audit entry
                            if self.config.enable_audit_log {
                                let entry = ConfigAuditEntry::new(ConfigAuditOperation::Delete, config_id)
                                    .with_name(&config.name)
                                    .with_version(config.version);
                                self.audit_log.push(entry);
                                self.flush_audit_log()?;
                            }

                            return Ok(true);
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    /// Delete old versions, keeping only the N most recent
    pub fn cleanup_old_versions(&mut self, name: &str, symbol: &str, keep_count: usize) -> Result<usize> {
        let versions = self.list_versions(name, symbol)?;

        if versions.len() <= keep_count {
            return Ok(0);
        }

        let to_delete: Vec<_> = versions.into_iter().skip(keep_count).collect();
        let delete_count = to_delete.len();

        for config in to_delete {
            self.delete(&config.id)?;
        }

        Ok(delete_count)
    }

    /// Archive a config (move to archive directory instead of deleting)
    pub fn archive(&mut self, config_id: &str) -> Result<Option<PathBuf>> {
        let configs_dir = self.config.base_path.join("configs");
        let archive_dir = self.config.base_path.join("archive");

        if !configs_dir.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&configs_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                if let Ok(content) = fs::read_to_string(&path) {
                    if let Ok(config) = serde_json::from_str::<AlgorithmConfig>(&content) {
                        if config.id == config_id {
                            // Move to archive
                            let filename = path.file_name().unwrap();
                            let archive_path = archive_dir.join(filename);
                            fs::rename(&path, &archive_path)?;

                            // Remove from cache
                            self.cache.remove(config_id);

                            // Log audit entry
                            if self.config.enable_audit_log {
                                let entry = ConfigAuditEntry::new(ConfigAuditOperation::Archive, config_id)
                                    .with_name(&config.name)
                                    .with_version(config.version);
                                self.audit_log.push(entry);
                                self.flush_audit_log()?;
                            }

                            return Ok(Some(archive_path));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    // ==================== Comparison Operations ====================

    /// Compare two configs by ID
    pub fn compare(&mut self, config_id_a: &str, config_id_b: &str) -> Result<Option<ConfigDiff>> {
        let config_a = self.load(config_id_a)?;
        let config_b = self.load(config_id_b)?;

        match (config_a, config_b) {
            (Some(a), Some(b)) => {
                let diff = ConfigDiff::new(&a, &b);

                // Log audit entry
                if self.config.enable_audit_log {
                    let entry = ConfigAuditEntry::new(ConfigAuditOperation::Compare, config_id_a)
                        .with_metadata("compare_to", config_id_b)
                        .with_metadata("diff_count", &diff.diff_count().to_string());
                    self.audit_log.push(entry);
                }

                Ok(Some(diff))
            }
            _ => Ok(None),
        }
    }

    /// Compare a config with the previous version
    pub fn compare_with_previous(&mut self, config: &AlgorithmConfig) -> Result<Option<ConfigDiff>> {
        if config.version == 1 {
            return Ok(None);
        }

        let previous = self.load_version(&config.name, &config.symbol, config.version - 1)?;

        match previous {
            Some(prev) => Ok(Some(ConfigDiff::new(&prev, config))),
            None => Ok(None),
        }
    }

    // ==================== Statistics ====================

    /// Get statistics about the config store
    pub fn get_stats(&self) -> Result<ConfigStoreStats> {
        let configs = self.list_all()?;

        let mut stats = ConfigStoreStats::default();
        stats.total_configs = configs.len();

        let mut names = std::collections::HashSet::new();
        let mut symbols = std::collections::HashSet::new();

        for config in &configs {
            if config.active {
                stats.active_configs += 1;
            }

            names.insert(&config.name);
            symbols.insert(&config.symbol);

            // Count by strategy
            let strategy_key = config.strategy_type.to_string();
            *stats.by_strategy.entry(strategy_key).or_insert(0) += 1;

            // Count by symbol
            *stats.by_symbol.entry(config.symbol.clone()).or_insert(0) += 1;

            // Track time range
            if stats.oldest_config.is_none() || config.created_at < stats.oldest_config.unwrap() {
                stats.oldest_config = Some(config.created_at);
            }
            if stats.newest_config.is_none() || config.created_at > stats.newest_config.unwrap() {
                stats.newest_config = Some(config.created_at);
            }
        }

        stats.unique_names = names.len();
        stats.unique_symbols = symbols.len();

        // Calculate disk size
        let configs_dir = self.config.base_path.join("configs");
        if configs_dir.exists() {
            for entry in fs::read_dir(&configs_dir)? {
                if let Ok(entry) = entry {
                    if let Ok(metadata) = entry.metadata() {
                        stats.disk_size_bytes += metadata.len();
                    }
                }
            }
        }

        stats.calculated_at = Utc::now();

        Ok(stats)
    }

    // ==================== Cache Operations ====================

    /// Clear the in-memory cache
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Get the number of cached configs
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Preload all configs into cache
    pub fn preload_cache(&mut self) -> Result<usize> {
        if !self.config.enable_cache {
            return Ok(0);
        }

        let configs = self.list_all()?;
        let count = configs.len();

        for config in configs {
            self.cache.insert(config.id.clone(), config);
        }

        Ok(count)
    }

    // ==================== Audit Log ====================

    /// Get the audit log
    pub fn audit_log(&self) -> &[ConfigAuditEntry] {
        &self.audit_log
    }

    /// Flush the audit log to disk
    fn flush_audit_log(&self) -> Result<()> {
        if !self.config.enable_audit_log {
            return Ok(());
        }

        let audit_dir = self.config.base_path.join("audit");
        let filename = format!("config_audit_{}.jsonl", Utc::now().format("%Y%m%d"));
        let path = audit_dir.join(&filename);

        // Append mode - write only new entries
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        use std::io::Write;
        if let Some(entry) = self.audit_log.last() {
            let json = serde_json::to_string(entry)?;
            writeln!(file, "{}", json)?;
        }

        Ok(())
    }

    /// Clear the in-memory audit log
    pub fn clear_audit_log(&mut self) {
        self.audit_log.clear();
    }

    // ==================== Parquet Index ====================

    /// Update the Parquet index with a new config
    fn update_parquet_index(&self, config: &AlgorithmConfig) -> Result<()> {
        let parquet_dir = self.config.base_path.join("parquet");
        let index_path = parquet_dir.join("config_index.parquet");

        // Create or append to index DataFrame
        let new_row = df!(
            "id" => [config.id.as_str()],
            "name" => [config.name.as_str()],
            "symbol" => [config.symbol.as_str()],
            "version" => [config.version as i64],
            "strategy_type" => [config.strategy_type.to_string()],
            "active" => [config.active],
            "created_at" => [config.created_at.to_rfc3339()],
            "research_state_id" => [config.source_research_id.as_deref().unwrap_or("")]
        )?;

        if index_path.exists() {
            // Read existing, append, and rewrite
            let existing = LazyFrame::scan_parquet(&index_path, Default::default())?
                .collect()?;
            let combined = existing.vstack(&new_row)?;

            let mut file = fs::File::create(&index_path)?;
            ParquetWriter::new(&mut file).finish(&mut combined.clone())?;
        } else {
            // Create new index
            let mut file = fs::File::create(&index_path)?;
            ParquetWriter::new(&mut file).finish(&mut new_row.clone())?;
        }

        Ok(())
    }

    /// Check if a config with the given ID exists
    pub fn exists(&self, config_id: &str) -> Result<bool> {
        // Check cache first
        if self.config.enable_cache && self.cache.contains_key(config_id) {
            return Ok(true);
        }

        // Check disk
        let configs_dir = self.config.base_path.join("configs");
        if !configs_dir.exists() {
            return Ok(false);
        }

        for entry in fs::read_dir(&configs_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "json") {
                if let Ok(content) = fs::read_to_string(&path) {
                    if let Ok(config) = serde_json::from_str::<AlgorithmConfig>(&content) {
                        if config.id == config_id {
                            return Ok(true);
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    /// Count total configs
    pub fn count(&self) -> Result<usize> {
        Ok(self.list_all()?.len())
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.count()? == 0)
    }
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> (ConfigStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = ConfigStoreConfig::with_path(temp_dir.path())
            .without_parquet()  // Disable parquet for faster tests
            .without_audit();    // Disable audit for faster tests
        let store = ConfigStore::new(config).unwrap();
        (store, temp_dir)
    }

    fn create_test_config(name: &str, symbol: &str) -> AlgorithmConfig {
        AlgorithmConfig::new(name, StrategyType::Hybrid, symbol)
    }

    // ==================== ConfigStoreConfig Tests ====================

    #[test]
    fn test_config_store_config_default() {
        let config = ConfigStoreConfig::default();
        assert_eq!(config.base_path, PathBuf::from("./data/configs"));
        assert_eq!(config.max_versions_per_name, 100);
        assert!(config.compress);
        assert!(config.enable_audit_log);
        assert!(config.enable_parquet_index);
        assert!(config.enable_cache);
    }

    #[test]
    fn test_config_store_config_with_path() {
        let config = ConfigStoreConfig::with_path("/tmp/test");
        assert_eq!(config.base_path, PathBuf::from("/tmp/test"));
    }

    #[test]
    fn test_config_store_config_without_parquet() {
        let config = ConfigStoreConfig::default().without_parquet();
        assert!(!config.enable_parquet_index);
    }

    #[test]
    fn test_config_store_config_without_audit() {
        let config = ConfigStoreConfig::default().without_audit();
        assert!(!config.enable_audit_log);
    }

    #[test]
    fn test_config_store_config_without_cache() {
        let config = ConfigStoreConfig::default().without_cache();
        assert!(!config.enable_cache);
    }

    #[test]
    fn test_config_store_config_with_max_versions() {
        let config = ConfigStoreConfig::default().with_max_versions(50);
        assert_eq!(config.max_versions_per_name, 50);
    }

    // ==================== ConfigAuditEntry Tests ====================

    #[test]
    fn test_config_audit_entry_new() {
        let entry = ConfigAuditEntry::new(ConfigAuditOperation::Save, "config123");
        assert_eq!(entry.operation, ConfigAuditOperation::Save);
        assert_eq!(entry.config_id, "config123");
        assert!(entry.config_name.is_none());
        assert!(entry.version.is_none());
    }

    #[test]
    fn test_config_audit_entry_with_name() {
        let entry = ConfigAuditEntry::new(ConfigAuditOperation::Load, "id")
            .with_name("TestConfig");
        assert_eq!(entry.config_name, Some("TestConfig".to_string()));
    }

    #[test]
    fn test_config_audit_entry_with_version() {
        let entry = ConfigAuditEntry::new(ConfigAuditOperation::Save, "id")
            .with_version(3);
        assert_eq!(entry.version, Some(3));
    }

    #[test]
    fn test_config_audit_entry_with_research_state() {
        let entry = ConfigAuditEntry::new(ConfigAuditOperation::Save, "id")
            .with_research_state("state123");
        assert_eq!(entry.source_research_id, Some("state123".to_string()));
    }

    #[test]
    fn test_config_audit_entry_with_metadata() {
        let entry = ConfigAuditEntry::new(ConfigAuditOperation::Save, "id")
            .with_metadata("key", "value");
        assert_eq!(entry.metadata.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_config_audit_operation_display() {
        assert_eq!(format!("{}", ConfigAuditOperation::Save), "SAVE");
        assert_eq!(format!("{}", ConfigAuditOperation::Load), "LOAD");
        assert_eq!(format!("{}", ConfigAuditOperation::Delete), "DELETE");
        assert_eq!(format!("{}", ConfigAuditOperation::List), "LIST");
        assert_eq!(format!("{}", ConfigAuditOperation::Compare), "COMPARE");
        assert_eq!(format!("{}", ConfigAuditOperation::Archive), "ARCHIVE");
    }

    // ==================== ConfigQuery Tests ====================

    #[test]
    fn test_config_query_default() {
        let query = ConfigQuery::default();
        assert!(query.name_contains.is_none());
        assert!(!query.active_only);
        assert!(query.limit.is_none());
    }

    #[test]
    fn test_config_query_with_name_contains() {
        let query = ConfigQuery::new().with_name_contains("test");
        assert_eq!(query.name_contains, Some("test".to_string()));
    }

    #[test]
    fn test_config_query_with_name_exact() {
        let query = ConfigQuery::new().with_name_exact("TestConfig");
        assert_eq!(query.name_exact, Some("TestConfig".to_string()));
    }

    #[test]
    fn test_config_query_with_symbol() {
        let query = ConfigQuery::new().with_symbol("BTCUSDT");
        assert_eq!(query.symbol, Some("BTCUSDT".to_string()));
    }

    #[test]
    fn test_config_query_with_strategy_type() {
        let query = ConfigQuery::new().with_strategy_type(StrategyType::Momentum);
        assert_eq!(query.strategy_type, Some(StrategyType::Momentum));
    }

    #[test]
    fn test_config_query_active_only() {
        let query = ConfigQuery::new().active_only();
        assert!(query.active_only);
    }

    #[test]
    fn test_config_query_with_version_range() {
        let query = ConfigQuery::new().with_version_range(Some(1), Some(5));
        assert_eq!(query.min_version, Some(1));
        assert_eq!(query.max_version, Some(5));
    }

    #[test]
    fn test_config_query_with_limit() {
        let query = ConfigQuery::new().with_limit(10);
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_config_query_sorted_by() {
        let query = ConfigQuery::new().sorted_by(ConfigSortField::Version, true);
        assert_eq!(query.sort_by, Some(ConfigSortField::Version));
        assert!(query.sort_ascending);
    }

    #[test]
    fn test_config_query_matches_name_contains() {
        let query = ConfigQuery::new().with_name_contains("test");
        let config = create_test_config("TestConfig", "BTCUSDT");
        assert!(query.matches(&config));

        let config2 = create_test_config("Other", "BTCUSDT");
        assert!(!query.matches(&config2));
    }

    #[test]
    fn test_config_query_matches_symbol() {
        let query = ConfigQuery::new().with_symbol("BTCUSDT");
        let config = create_test_config("Test", "BTCUSDT");
        assert!(query.matches(&config));

        let config2 = create_test_config("Test", "ETHUSDT");
        assert!(!query.matches(&config2));
    }

    #[test]
    fn test_config_query_matches_strategy_type() {
        let query = ConfigQuery::new().with_strategy_type(StrategyType::Hybrid);
        let config = create_test_config("Test", "BTCUSDT");
        assert!(query.matches(&config)); // Default is Hybrid
    }

    #[test]
    fn test_config_query_matches_active() {
        let query = ConfigQuery::new().active_only();
        let config = create_test_config("Test", "BTCUSDT");
        assert!(query.matches(&config)); // Default is active

        let mut inactive = create_test_config("Test", "BTCUSDT");
        inactive.active = false;
        assert!(!query.matches(&inactive));
    }

    #[test]
    fn test_config_query_matches_version_range() {
        let query = ConfigQuery::new().with_version_range(Some(2), Some(5));

        let v1 = create_test_config("Test", "BTCUSDT");
        assert!(!query.matches(&v1)); // v1 < min 2

        let v3 = v1.next_version().next_version();
        assert!(query.matches(&v3)); // v3 in range
    }

    // ==================== ConfigDiff Tests ====================

    #[test]
    fn test_config_diff_identical() {
        let config = create_test_config("Test", "BTCUSDT");
        let diff = ConfigDiff::new(&config, &config);
        assert!(diff.is_identical());
        assert_eq!(diff.diff_count(), 0);
    }

    #[test]
    fn test_config_diff_different_name() {
        let config_a = create_test_config("TestA", "BTCUSDT");
        let config_b = create_test_config("TestB", "BTCUSDT");
        let diff = ConfigDiff::new(&config_a, &config_b);
        assert!(!diff.is_identical());
        assert!(diff.differences.iter().any(|d| d.field_path == "name"));
    }

    #[test]
    fn test_config_diff_different_symbol() {
        let config_a = create_test_config("Test", "BTCUSDT");
        let config_b = create_test_config("Test", "ETHUSDT");
        let diff = ConfigDiff::new(&config_a, &config_b);
        assert!(!diff.is_identical());
        assert!(diff.differences.iter().any(|d| d.field_path == "symbol"));
    }

    #[test]
    fn test_config_diff_different_strategy() {
        let config_a = AlgorithmConfig::new("Test", StrategyType::Momentum, "BTCUSDT");
        let config_b = AlgorithmConfig::new("Test", StrategyType::MarketMaking, "BTCUSDT");
        let diff = ConfigDiff::new(&config_a, &config_b);
        assert!(!diff.is_identical());
        assert!(diff.differences.iter().any(|d| d.field_path == "strategy_type"));
    }

    #[test]
    fn test_config_diff_summary_identical() {
        let config = create_test_config("Test", "BTCUSDT");
        let diff = ConfigDiff::new(&config, &config);
        assert!(diff.summary().contains("identical"));
    }

    #[test]
    fn test_config_diff_summary_different() {
        let config_a = create_test_config("TestA", "BTCUSDT");
        let config_b = create_test_config("TestB", "BTCUSDT");
        let diff = ConfigDiff::new(&config_a, &config_b);
        assert!(diff.summary().contains("difference"));
    }

    // ==================== ConfigSummary Tests ====================

    #[test]
    fn test_config_summary_from_config() {
        let config = create_test_config("Test", "BTCUSDT");
        let summary = ConfigSummary::from(&config);
        assert_eq!(summary.id, config.id);
        assert_eq!(summary.name, config.name);
        assert_eq!(summary.symbol, config.symbol);
        assert_eq!(summary.version, config.version);
        assert_eq!(summary.strategy_type, config.strategy_type);
        assert_eq!(summary.active, config.active);
    }

    // ==================== ConfigStoreStats Tests ====================

    #[test]
    fn test_config_store_stats_default() {
        let stats = ConfigStoreStats::default();
        assert_eq!(stats.total_configs, 0);
        assert_eq!(stats.active_configs, 0);
        assert!(stats.oldest_config.is_none());
    }

    // ==================== ConfigStore Basic Tests ====================

    #[test]
    fn test_config_store_new() {
        let (store, _temp) = create_test_store();
        assert!(store.is_empty().unwrap());
    }

    #[test]
    fn test_config_store_at_path() {
        let temp_dir = TempDir::new().unwrap();
        let store = ConfigStore::at_path(temp_dir.path()).unwrap();
        assert!(store.is_empty().unwrap());
    }

    #[test]
    fn test_config_store_directories_created() {
        let temp_dir = TempDir::new().unwrap();
        let _store = ConfigStore::new(ConfigStoreConfig::with_path(temp_dir.path())).unwrap();

        assert!(temp_dir.path().join("configs").exists());
        assert!(temp_dir.path().join("parquet").exists());
        assert!(temp_dir.path().join("audit").exists());
        assert!(temp_dir.path().join("archive").exists());
    }

    // ==================== ConfigStore Save/Load Tests ====================

    #[test]
    fn test_config_store_save_and_load() {
        let (mut store, _temp) = create_test_store();
        let config = create_test_config("SaveLoadTest", "BTCUSDT");
        let config_id = config.id.clone();

        let path = store.save(&config).unwrap();
        assert!(path.exists());

        let loaded = store.load(&config_id).unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, config_id);
        assert_eq!(loaded.name, "SaveLoadTest");
    }

    #[test]
    fn test_config_store_save_creates_file() {
        let (mut store, temp) = create_test_store();
        let config = create_test_config("FileTest", "ETHUSDT");

        let path = store.save(&config).unwrap();
        assert!(path.exists());
        assert!(path.extension().map_or(false, |e| e == "json"));
    }

    #[test]
    fn test_config_store_load_nonexistent() {
        let (mut store, _temp) = create_test_store();
        let result = store.load("nonexistent_id").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_config_store_save_batch() {
        let (mut store, _temp) = create_test_store();
        let configs = vec![
            create_test_config("Batch1", "BTCUSDT"),
            create_test_config("Batch2", "ETHUSDT"),
            create_test_config("Batch3", "SOLUSDT"),
        ];

        let paths = store.save_batch(&configs).unwrap();
        assert_eq!(paths.len(), 3);

        for path in paths {
            assert!(path.exists());
        }
    }

    #[test]
    fn test_config_store_load_latest() {
        let (mut store, _temp) = create_test_store();

        let v1 = create_test_config("Versioned", "BTCUSDT");
        let v2 = v1.next_version();
        let v3 = v2.next_version();

        store.save(&v1).unwrap();
        store.save(&v2).unwrap();
        store.save(&v3).unwrap();

        let latest = store.load_latest("Versioned", "BTCUSDT").unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().version, 3);
    }

    #[test]
    fn test_config_store_load_version() {
        let (mut store, _temp) = create_test_store();

        let v1 = create_test_config("Versioned", "BTCUSDT");
        let v2 = v1.next_version();

        store.save(&v1).unwrap();
        store.save(&v2).unwrap();

        let loaded_v1 = store.load_version("Versioned", "BTCUSDT", 1).unwrap();
        assert!(loaded_v1.is_some());
        assert_eq!(loaded_v1.unwrap().version, 1);

        let loaded_v2 = store.load_version("Versioned", "BTCUSDT", 2).unwrap();
        assert!(loaded_v2.is_some());
        assert_eq!(loaded_v2.unwrap().version, 2);
    }

    // ==================== ConfigStore List Tests ====================

    #[test]
    fn test_config_store_list_all_empty() {
        let (store, _temp) = create_test_store();
        let configs = store.list_all().unwrap();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_config_store_list_all() {
        let (mut store, _temp) = create_test_store();

        store.save(&create_test_config("A", "BTCUSDT")).unwrap();
        store.save(&create_test_config("B", "ETHUSDT")).unwrap();
        store.save(&create_test_config("C", "SOLUSDT")).unwrap();

        let configs = store.list_all().unwrap();
        assert_eq!(configs.len(), 3);
    }

    #[test]
    fn test_config_store_query_by_symbol() {
        let (mut store, _temp) = create_test_store();

        store.save(&create_test_config("A", "BTCUSDT")).unwrap();
        store.save(&create_test_config("B", "BTCUSDT")).unwrap();
        store.save(&create_test_config("C", "ETHUSDT")).unwrap();

        let query = ConfigQuery::new().with_symbol("BTCUSDT");
        let results = store.query(&query).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_config_store_query_by_name() {
        let (mut store, _temp) = create_test_store();

        store.save(&create_test_config("Momentum1", "BTCUSDT")).unwrap();
        store.save(&create_test_config("Momentum2", "ETHUSDT")).unwrap();
        store.save(&create_test_config("MarketMaker", "SOLUSDT")).unwrap();

        let query = ConfigQuery::new().with_name_contains("momentum");
        let results = store.query(&query).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_config_store_query_with_limit() {
        let (mut store, _temp) = create_test_store();

        for i in 0..10 {
            store.save(&create_test_config(&format!("Config{}", i), "BTCUSDT")).unwrap();
        }

        let query = ConfigQuery::new().with_limit(5);
        let results = store.query(&query).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_config_store_list_summaries() {
        let (mut store, _temp) = create_test_store();

        store.save(&create_test_config("A", "BTCUSDT")).unwrap();
        store.save(&create_test_config("B", "ETHUSDT")).unwrap();

        let summaries = store.list_summaries().unwrap();
        assert_eq!(summaries.len(), 2);
    }

    #[test]
    fn test_config_store_list_versions() {
        let (mut store, _temp) = create_test_store();

        let v1 = create_test_config("Multi", "BTCUSDT");
        let v2 = v1.next_version();
        let v3 = v2.next_version();

        store.save(&v1).unwrap();
        store.save(&v2).unwrap();
        store.save(&v3).unwrap();

        let versions = store.list_versions("Multi", "BTCUSDT").unwrap();
        assert_eq!(versions.len(), 3);
        // Should be sorted by version descending
        assert_eq!(versions[0].version, 3);
        assert_eq!(versions[1].version, 2);
        assert_eq!(versions[2].version, 1);
    }

    // ==================== ConfigStore Delete Tests ====================

    #[test]
    fn test_config_store_delete() {
        let (mut store, _temp) = create_test_store();
        let config = create_test_config("ToDelete", "BTCUSDT");
        let config_id = config.id.clone();

        store.save(&config).unwrap();
        assert!(store.exists(&config_id).unwrap());

        let deleted = store.delete(&config_id).unwrap();
        assert!(deleted);
        assert!(!store.exists(&config_id).unwrap());
    }

    #[test]
    fn test_config_store_delete_nonexistent() {
        let (mut store, _temp) = create_test_store();
        let deleted = store.delete("nonexistent").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_config_store_cleanup_old_versions() {
        let (mut store, _temp) = create_test_store();

        let v1 = create_test_config("Cleanup", "BTCUSDT");
        let v2 = v1.next_version();
        let v3 = v2.next_version();
        let v4 = v3.next_version();
        let v5 = v4.next_version();

        store.save(&v1).unwrap();
        store.save(&v2).unwrap();
        store.save(&v3).unwrap();
        store.save(&v4).unwrap();
        store.save(&v5).unwrap();

        let deleted = store.cleanup_old_versions("Cleanup", "BTCUSDT", 2).unwrap();
        assert_eq!(deleted, 3); // Should delete v1, v2, v3

        let remaining = store.list_versions("Cleanup", "BTCUSDT").unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].version, 5);
        assert_eq!(remaining[1].version, 4);
    }

    #[test]
    fn test_config_store_archive() {
        let (mut store, temp) = create_test_store();
        let config = create_test_config("ToArchive", "BTCUSDT");
        let config_id = config.id.clone();

        store.save(&config).unwrap();

        let archive_path = store.archive(&config_id).unwrap();
        assert!(archive_path.is_some());
        let archive_path = archive_path.unwrap();
        assert!(archive_path.exists());
        assert!(archive_path.starts_with(temp.path().join("archive")));

        // Should no longer exist in main configs
        assert!(!store.exists(&config_id).unwrap());
    }

    // ==================== ConfigStore Compare Tests ====================

    #[test]
    fn test_config_store_compare() {
        let (mut store, _temp) = create_test_store();

        let config_a = create_test_config("CompareA", "BTCUSDT");
        let config_b = create_test_config("CompareB", "BTCUSDT");

        store.save(&config_a).unwrap();
        store.save(&config_b).unwrap();

        let diff = store.compare(&config_a.id, &config_b.id).unwrap();
        assert!(diff.is_some());
        let diff = diff.unwrap();
        assert!(!diff.is_identical());
    }

    #[test]
    fn test_config_store_compare_with_previous() {
        let (mut store, _temp) = create_test_store();

        let v1 = create_test_config("Evolving", "BTCUSDT");
        let mut v2 = v1.next_version();
        v2.entry.min_momentum_signal = 0.9; // Change something

        store.save(&v1).unwrap();
        store.save(&v2).unwrap();

        let diff = store.compare_with_previous(&v2).unwrap();
        assert!(diff.is_some());
        let diff = diff.unwrap();
        assert!(!diff.is_identical());
    }

    #[test]
    fn test_config_store_compare_with_previous_v1() {
        let (mut store, _temp) = create_test_store();

        let v1 = create_test_config("NoHistory", "BTCUSDT");
        store.save(&v1).unwrap();

        let diff = store.compare_with_previous(&v1).unwrap();
        assert!(diff.is_none()); // v1 has no previous
    }

    // ==================== ConfigStore Statistics Tests ====================

    #[test]
    fn test_config_store_stats_empty() {
        let (store, _temp) = create_test_store();
        let stats = store.get_stats().unwrap();
        assert_eq!(stats.total_configs, 0);
        assert_eq!(stats.active_configs, 0);
    }

    #[test]
    fn test_config_store_stats() {
        let (mut store, _temp) = create_test_store();

        store.save(&create_test_config("A", "BTCUSDT")).unwrap();
        store.save(&create_test_config("B", "BTCUSDT")).unwrap();
        store.save(&create_test_config("C", "ETHUSDT")).unwrap();

        let mut inactive = create_test_config("D", "SOLUSDT");
        inactive.active = false;
        store.save(&inactive).unwrap();

        let stats = store.get_stats().unwrap();
        assert_eq!(stats.total_configs, 4);
        assert_eq!(stats.active_configs, 3);
        assert_eq!(stats.unique_symbols, 3);
        assert!(stats.oldest_config.is_some());
        assert!(stats.newest_config.is_some());
    }

    // ==================== ConfigStore Cache Tests ====================

    #[test]
    fn test_config_store_cache() {
        let (mut store, _temp) = create_test_store();
        let config = create_test_config("Cached", "BTCUSDT");
        let config_id = config.id.clone();

        store.save(&config).unwrap();
        assert_eq!(store.cache_size(), 1);

        // Second load should hit cache
        let _ = store.load(&config_id).unwrap();
        assert_eq!(store.cache_size(), 1);
    }

    #[test]
    fn test_config_store_clear_cache() {
        let (mut store, _temp) = create_test_store();

        store.save(&create_test_config("A", "BTCUSDT")).unwrap();
        store.save(&create_test_config("B", "ETHUSDT")).unwrap();
        assert_eq!(store.cache_size(), 2);

        store.clear_cache();
        assert_eq!(store.cache_size(), 0);
    }

    #[test]
    fn test_config_store_preload_cache() {
        let (mut store, _temp) = create_test_store();

        store.save(&create_test_config("A", "BTCUSDT")).unwrap();
        store.save(&create_test_config("B", "ETHUSDT")).unwrap();

        store.clear_cache();
        assert_eq!(store.cache_size(), 0);

        let preloaded = store.preload_cache().unwrap();
        assert_eq!(preloaded, 2);
        assert_eq!(store.cache_size(), 2);
    }

    // ==================== ConfigStore Utility Tests ====================

    #[test]
    fn test_config_store_exists() {
        let (mut store, _temp) = create_test_store();
        let config = create_test_config("Exists", "BTCUSDT");
        let config_id = config.id.clone();

        assert!(!store.exists(&config_id).unwrap());

        store.save(&config).unwrap();
        assert!(store.exists(&config_id).unwrap());
    }

    #[test]
    fn test_config_store_count() {
        let (mut store, _temp) = create_test_store();
        assert_eq!(store.count().unwrap(), 0);

        store.save(&create_test_config("A", "BTCUSDT")).unwrap();
        store.save(&create_test_config("B", "ETHUSDT")).unwrap();

        assert_eq!(store.count().unwrap(), 2);
    }

    #[test]
    fn test_config_store_is_empty() {
        let (mut store, _temp) = create_test_store();
        assert!(store.is_empty().unwrap());

        store.save(&create_test_config("A", "BTCUSDT")).unwrap();
        assert!(!store.is_empty().unwrap());
    }

    // ==================== Serialization Roundtrip Tests ====================

    #[test]
    fn test_config_roundtrip_preserves_all_fields() {
        let (mut store, _temp) = create_test_store();

        let mut config = AlgorithmConfig::new("RoundtripTest", StrategyType::Momentum, "BTCUSDT");
        config.entry.min_momentum_signal = 0.75;
        config.exit.take_profit_bps = 25.0;
        config.position.target_volatility = 0.02;
        config.regime_filters.max_entropy = 0.65;
        config.source_research_id = Some("research123".to_string());
        config.description = Some("Test notes".to_string());

        let config_id = config.id.clone();
        store.save(&config).unwrap();
        store.clear_cache(); // Force read from disk

        let loaded = store.load(&config_id).unwrap().unwrap();
        assert_eq!(loaded.name, config.name);
        assert_eq!(loaded.symbol, config.symbol);
        assert_eq!(loaded.strategy_type, config.strategy_type);
        assert_eq!(loaded.entry.min_momentum_signal, config.entry.min_momentum_signal);
        assert_eq!(loaded.exit.take_profit_bps, config.exit.take_profit_bps);
        assert_eq!(loaded.position.target_volatility, config.position.target_volatility);
        assert_eq!(loaded.regime_filters.max_entropy, config.regime_filters.max_entropy);
        assert_eq!(loaded.source_research_id, config.source_research_id);
        assert_eq!(loaded.description, config.description);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_config_store_special_characters_in_name() {
        let (mut store, _temp) = create_test_store();
        let config = create_test_config("Test Config With Spaces", "BTCUSDT");

        let path = store.save(&config).unwrap();
        assert!(path.exists());

        let loaded = store.load(&config.id).unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, "Test Config With Spaces");
    }

    #[test]
    fn test_config_store_many_configs() {
        let (mut store, _temp) = create_test_store();

        for i in 0..100 {
            store.save(&create_test_config(&format!("Config{}", i), "BTCUSDT")).unwrap();
        }

        assert_eq!(store.count().unwrap(), 100);

        let query = ConfigQuery::new().with_limit(10);
        let results = store.query(&query).unwrap();
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_config_store_concurrent_versions() {
        let (mut store, _temp) = create_test_store();

        // Save multiple configs with same name but different symbols
        store.save(&create_test_config("Universal", "BTCUSDT")).unwrap();
        store.save(&create_test_config("Universal", "ETHUSDT")).unwrap();
        store.save(&create_test_config("Universal", "SOLUSDT")).unwrap();

        let btc = store.load_latest("Universal", "BTCUSDT").unwrap();
        let eth = store.load_latest("Universal", "ETHUSDT").unwrap();

        assert!(btc.is_some());
        assert!(eth.is_some());
        assert_ne!(btc.unwrap().id, eth.unwrap().id);
    }

    // ==================== Query Sorting Tests ====================

    #[test]
    fn test_config_query_sort_by_version() {
        let (mut store, _temp) = create_test_store();

        let v1 = create_test_config("Sort", "BTCUSDT");
        let v2 = v1.next_version();
        let v3 = v2.next_version();

        // Save out of order
        store.save(&v2).unwrap();
        store.save(&v1).unwrap();
        store.save(&v3).unwrap();

        let query = ConfigQuery::new()
            .with_name_exact("Sort")
            .sorted_by(ConfigSortField::Version, true); // ascending
        let results = store.query(&query).unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].version, 1);
        assert_eq!(results[1].version, 2);
        assert_eq!(results[2].version, 3);
    }

    #[test]
    fn test_config_query_sort_by_name() {
        let (mut store, _temp) = create_test_store();

        store.save(&create_test_config("Charlie", "BTCUSDT")).unwrap();
        store.save(&create_test_config("Alpha", "BTCUSDT")).unwrap();
        store.save(&create_test_config("Bravo", "BTCUSDT")).unwrap();

        let query = ConfigQuery::new().sorted_by(ConfigSortField::Name, true);
        let results = store.query(&query).unwrap();

        assert_eq!(results[0].name, "Alpha");
        assert_eq!(results[1].name, "Bravo");
        assert_eq!(results[2].name, "Charlie");
    }

    // ==================== Research State Linkage Tests ====================

    #[test]
    fn test_config_with_research_state_id() {
        let (mut store, _temp) = create_test_store();

        let mut config = create_test_config("Linked", "BTCUSDT");
        config.source_research_id = Some("research_abc123".to_string());

        store.save(&config).unwrap();

        let query = ConfigQuery::new().with_research_state("research_abc123");
        let results = store.query(&query).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].source_research_id, Some("research_abc123".to_string()));
    }

    // ==================== Strategy Type Filter Tests ====================

    #[test]
    fn test_config_query_by_all_strategy_types() {
        let (mut store, _temp) = create_test_store();

        store.save(&AlgorithmConfig::new("Mom1", StrategyType::Momentum, "BTCUSDT")).unwrap();
        store.save(&AlgorithmConfig::new("Mom2", StrategyType::Momentum, "ETHUSDT")).unwrap();
        store.save(&AlgorithmConfig::new("MM1", StrategyType::MarketMaking, "BTCUSDT")).unwrap();
        store.save(&AlgorithmConfig::new("Hyb1", StrategyType::Hybrid, "BTCUSDT")).unwrap();

        let momentum = store.query(&ConfigQuery::new().with_strategy_type(StrategyType::Momentum)).unwrap();
        assert_eq!(momentum.len(), 2);

        let mm = store.query(&ConfigQuery::new().with_strategy_type(StrategyType::MarketMaking)).unwrap();
        assert_eq!(mm.len(), 1);

        let hybrid = store.query(&ConfigQuery::new().with_strategy_type(StrategyType::Hybrid)).unwrap();
        assert_eq!(hybrid.len(), 1);
    }
}

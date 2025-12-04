//! Parameter Presets
//!
//! Stores optimized MM configurations with metadata about when/how they were developed.
//! Used for paper trading validation of backtested strategies.

use chrono::{DateTime, Utc, Local};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::market_maker::{MMConfig, RegimeThresholds};

/// A saved parameter preset with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterPreset {
    /// Human-readable name
    pub name: String,
    /// When this preset was created
    pub created_at: DateTime<Utc>,
    /// How it was optimized (grid-search, optuna, manual)
    pub optimization_method: String,
    /// Data range used for optimization
    pub data_range: String,
    /// Number of events in optimization dataset
    pub num_events: usize,
    /// Expected performance from backtest
    pub expected_return: f64,
    pub expected_sharpe: f64,
    pub expected_trades: usize,
    pub expected_win_rate: f64,
    /// The actual parameters
    pub spread_bps: f64,
    pub skew: f64,
    pub high_entropy_threshold: f64,
    pub low_entropy_threshold: f64,
    pub entropy_gate: bool,
    pub fill_prob_assumption: f64,
    /// Notes
    pub notes: String,
}

impl ParameterPreset {
    /// Create a new preset with current timestamp
    pub fn new(
        name: &str,
        method: &str,
        spread: f64,
        skew: f64,
        high_entropy: f64,
        fill_prob: f64,
    ) -> Self {
        Self {
            name: name.to_string(),
            created_at: Utc::now(),
            optimization_method: method.to_string(),
            data_range: String::new(),
            num_events: 0,
            expected_return: 0.0,
            expected_sharpe: 0.0,
            expected_trades: 0,
            expected_win_rate: 0.0,
            spread_bps: spread,
            skew,
            high_entropy_threshold: high_entropy,
            low_entropy_threshold: 0.4,
            entropy_gate: false,
            fill_prob_assumption: fill_prob,
            notes: String::new(),
        }
    }

    /// Convert to MMConfig
    pub fn to_mm_config(&self) -> MMConfig {
        MMConfig {
            base_spread_bps: self.spread_bps,
            inventory_skew_factor: self.skew,
            regime_thresholds: RegimeThresholds {
                high_entropy_threshold: self.high_entropy_threshold,
                low_entropy_threshold: self.low_entropy_threshold,
            },
            pull_quotes_in_low_entropy: self.entropy_gate,
            ..Default::default()
        }
    }

    /// Human-readable timestamp
    pub fn created_at_local(&self) -> String {
        let local: DateTime<Local> = self.created_at.into();
        local.format("%Y-%m-%d %H:%M").to_string()
    }

    /// Short description for menu
    pub fn menu_description(&self) -> String {
        format!(
            "{} ({}): spread={:.1}bps, skew={:.1}, exp={:+.1}%",
            self.name,
            self.created_at_local(),
            self.spread_bps,
            self.skew,
            self.expected_return * 100.0
        )
    }
}

/// Collection of presets
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PresetStore {
    pub presets: Vec<ParameterPreset>,
}

impl PresetStore {
    const FILE_PATH: &'static str = "./data/presets.json";

    /// Load presets from file
    pub fn load() -> Self {
        if Path::new(Self::FILE_PATH).exists() {
            match fs::read_to_string(Self::FILE_PATH) {
                Ok(content) => {
                    serde_json::from_str(&content).unwrap_or_default()
                }
                Err(_) => Self::default(),
            }
        } else {
            Self::default_presets()
        }
    }

    /// Save presets to file
    pub fn save(&self) -> std::io::Result<()> {
        // Ensure data directory exists
        fs::create_dir_all("./data")?;
        let content = serde_json::to_string_pretty(self)?;
        fs::write(Self::FILE_PATH, content)
    }

    /// Add a preset and save
    pub fn add(&mut self, preset: ParameterPreset) -> std::io::Result<()> {
        self.presets.push(preset);
        self.save()
    }

    /// Get latest preset
    pub fn latest(&self) -> Option<&ParameterPreset> {
        self.presets.last()
    }

    /// Get preset by index
    pub fn get(&self, index: usize) -> Option<&ParameterPreset> {
        self.presets.get(index)
    }

    /// Default presets based on grid search results
    fn default_presets() -> Self {
        let mut store = Self::default();

        // Best from grid search (Dec 3, 2025)
        let mut best = ParameterPreset::new(
            "GridSearch-Best",
            "grid-search --test-gate",
            1.0,  // spread
            0.3,  // skew
            0.7,  // high entropy
            0.10, // fill prob assumption
        );
        best.data_range = "Oct 16 - Dec 2, 2025 (47 days)".to_string();
        best.num_events = 73000;
        best.expected_return = 0.0514;  // +5.14%
        best.expected_sharpe = -1.20;
        best.expected_trades = 452;
        best.expected_win_rate = 0.595;
        best.notes = "Best from 360-combination grid search. WIDE mode (no gating).".to_string();
        // Set created_at to Dec 3, 2025
        best.created_at = "2025-12-03T16:00:00Z".parse().unwrap_or(Utc::now());

        store.presets.push(best);

        // Conservative variant
        let mut conservative = ParameterPreset::new(
            "GridSearch-Conservative",
            "grid-search --test-gate",
            1.0,
            0.3,
            0.7,
            0.05, // lower fill prob assumption
        );
        conservative.data_range = "Oct 16 - Dec 2, 2025 (47 days)".to_string();
        conservative.num_events = 73000;
        conservative.expected_return = 0.0109;  // +1.09%
        conservative.expected_sharpe = -6.18;
        conservative.expected_trades = 202;
        conservative.expected_win_rate = 0.55;
        conservative.notes = "Same params but with conservative 5% fill rate assumption.".to_string();
        conservative.created_at = "2025-12-03T16:00:00Z".parse().unwrap_or(Utc::now());

        store.presets.push(conservative);

        store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preset_creation() {
        let preset = ParameterPreset::new("Test", "manual", 2.0, 0.5, 0.7, 0.10);
        assert_eq!(preset.spread_bps, 2.0);
        assert_eq!(preset.skew, 0.5);
    }

    #[test]
    fn test_preset_to_config() {
        let preset = ParameterPreset::new("Test", "manual", 1.5, 0.4, 0.8, 0.10);
        let config = preset.to_mm_config();
        assert_eq!(config.base_spread_bps, 1.5);
        assert_eq!(config.inventory_skew_factor, 0.4);
    }
}

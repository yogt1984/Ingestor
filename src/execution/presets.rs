//! Parameter Presets
//!
//! Stores optimized MM configurations with metadata about when/how they were developed.
//! Used for paper trading validation of backtested strategies.

use chrono::{DateTime, Utc, Local};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::strategies::{AlgorithmType, MLModelWeights};
use crate::execution::market_maker::{MMConfig, RegimeThresholds, RegimeParams};

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
    pub fill_prob_assumption: f64,
    /// Notes
    pub notes: String,
    /// Algorithm type (avellaneda_stoikov or ml_spread_skew)
    #[serde(default)]
    pub algorithm_type: AlgorithmType,
    /// ML model weights (only used if algorithm_type is MLSpreadSkew)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ml_weights: Option<MLModelWeights>,
    /// Path to ML weights file (alternative to embedding weights)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ml_weights_path: Option<String>,
}

impl ParameterPreset {
    /// Create a new preset with current timestamp (defaults to A-S algorithm)
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
            fill_prob_assumption: fill_prob,
            notes: String::new(),
            algorithm_type: AlgorithmType::AvellanedaStoikov,
            ml_weights: None,
            ml_weights_path: None,
        }
    }

    /// Create a new ML preset with embedded weights
    pub fn new_ml(
        name: &str,
        method: &str,
        fill_prob: f64,
        weights: MLModelWeights,
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
            spread_bps: 0.0, // Not used for ML
            skew: 0.0,       // Not used for ML
            high_entropy_threshold: 0.7,
            low_entropy_threshold: 0.4,
            fill_prob_assumption: fill_prob,
            notes: String::new(),
            algorithm_type: AlgorithmType::MLSpreadSkew,
            ml_weights: Some(weights),
            ml_weights_path: None,
        }
    }

    /// Create a new ML preset with weights loaded from file
    pub fn new_ml_from_file(
        name: &str,
        method: &str,
        fill_prob: f64,
        weights_path: &str,
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
            spread_bps: 0.0,
            skew: 0.0,
            high_entropy_threshold: 0.7,
            low_entropy_threshold: 0.4,
            fill_prob_assumption: fill_prob,
            notes: String::new(),
            algorithm_type: AlgorithmType::MLSpreadSkew,
            ml_weights: None,
            ml_weights_path: Some(weights_path.to_string()),
        }
    }

    /// Get ML weights, loading from file if necessary
    pub fn get_ml_weights(&self) -> Option<MLModelWeights> {
        // First check embedded weights
        if self.ml_weights.is_some() {
            return self.ml_weights.clone();
        }

        // Try loading from path
        if let Some(ref path) = self.ml_weights_path {
            if let Ok(weights) = MLModelWeights::load_from_file(path) {
                return Some(weights);
            }
        }

        // Fall back to default weights for ML algorithm
        if self.algorithm_type == AlgorithmType::MLSpreadSkew {
            return Some(MLModelWeights::default());
        }

        None
    }

    /// Create the appropriate algorithm from this preset
    pub fn create_algorithm(&self) -> Box<dyn crate::strategies::MarketMakingAlgorithm> {
        use crate::strategies::{
            AvellanedaStoikovAlgorithm, MLSpreadSkewAlgorithm, MLSpreadSkewConfig,
            FixedSpreadAlgorithm, FixedSpreadConfig,
        };
        use rust_decimal_macros::dec;

        match self.algorithm_type {
            AlgorithmType::AvellanedaStoikov => {
                let config = self.to_mm_config();
                Box::new(AvellanedaStoikovAlgorithm::new(config))
            }
            AlgorithmType::MLSpreadSkew => {
                let weights = self.get_ml_weights().unwrap_or_default();
                let config = MLSpreadSkewConfig {
                    max_inventory: dec!(0.1),
                    quote_size: dec!(0.001),
                    ..Default::default()
                };
                Box::new(MLSpreadSkewAlgorithm::new(config, weights))
            }
            AlgorithmType::FixedSpread => {
                let config = FixedSpreadConfig {
                    max_inventory: dec!(0.1),
                    quote_size: dec!(0.001),
                    spread_bps: self.spread_bps,
                    skew_factor: self.skew,
                };
                Box::new(FixedSpreadAlgorithm::new(config))
            }
        }
    }

    /// Convert to MMConfig
    pub fn to_mm_config(&self) -> MMConfig {
        let regime_params = RegimeParams::uniform(self.spread_bps, self.skew);

        MMConfig {
            regime_thresholds: RegimeThresholds {
                high_entropy_threshold: self.high_entropy_threshold,
                low_entropy_threshold: self.low_entropy_threshold,
            },
            regime_params,
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
        let algo_label = match self.algorithm_type {
            AlgorithmType::AvellanedaStoikov => "A-S",
            AlgorithmType::MLSpreadSkew => "ML",
            AlgorithmType::FixedSpread => "FS",
        };

        match self.algorithm_type {
            AlgorithmType::AvellanedaStoikov | AlgorithmType::FixedSpread => format!(
                "[{}] {} ({}): spread={:.1}bps, skew={:.1}, exp={:+.1}%",
                algo_label,
                self.name,
                self.created_at_local(),
                self.spread_bps,
                self.skew,
                self.expected_return * 100.0
            ),
            AlgorithmType::MLSpreadSkew => format!(
                "[{}] {} ({}): model={}, exp={:+.1}%",
                algo_label,
                self.name,
                self.created_at_local(),
                self.ml_weights.as_ref()
                    .map(|w| w.version.as_str())
                    .unwrap_or("default"),
                self.expected_return * 100.0
            ),
        }
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
            "grid-search",
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
            "grid-search",
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

        // ML algorithm with trained weights from walk-forward optimization (Dec 6, 2025)
        let trained_weights = MLModelWeights {
            spread: crate::strategies::SpreadWeights {
                intercept: 1.0,
                w_entropy: -2.0,      // Widen spread in low entropy (trending) markets
                w_volatility: 200.0,  // Widen spread in high volatility
                w_imbalance: 1.0,
                w_interaction: -100.0,
            },
            skew: crate::strategies::SkewWeights {
                intercept: 0.5,
                w_entropy: -0.2,
                w_volatility: 50.0,
                w_imbalance: 0.1,
                w_inventory: -1.0,    // Skew against inventory
            },
            version: "walk-forward-v1".to_string(),
            training_info: Some(crate::strategies::TrainingInfo {
                trained_on: "2025-12-06T16:29:04Z".to_string(),
                num_samples: 101254,
                train_sharpe: -1.49,
                validation_sharpe: Some(0.0),
            }),
        };
        let mut ml_trained = ParameterPreset::new_ml(
            "ML-Trained",
            "walk-forward-ml",
            0.10,
            trained_weights,
        );
        ml_trained.data_range = "Oct 16 - Dec 6, 2025 (50 days)".to_string();
        ml_trained.num_events = 101254;
        ml_trained.expected_return = 0.032; // +3.2% on training
        ml_trained.expected_sharpe = -1.49;
        ml_trained.expected_trades = 14;
        ml_trained.notes = "Walk-forward trained ML weights. Spread widens in low entropy/high vol. Skew adjusts for inventory.".to_string();
        ml_trained.created_at = "2025-12-06T16:30:00Z".parse().unwrap_or(Utc::now());

        store.presets.push(ml_trained);

        // ML algorithm with default/baseline weights for comparison
        let mut ml_default = ParameterPreset::new_ml(
            "ML-Baseline",
            "manual",
            0.10,
            MLModelWeights::default(),
        );
        ml_default.data_range = "Oct 16 - Dec 2, 2025 (47 days)".to_string();
        ml_default.num_events = 73000;
        ml_default.notes = "ML Spread/Skew predictor with baseline weights. Compare against ML-Trained.".to_string();
        ml_default.created_at = "2025-12-06T12:00:00Z".parse().unwrap_or(Utc::now());

        store.presets.push(ml_default);

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
        // Now using RegimeParams - high entropy spread is the base spread
        assert_eq!(config.regime_params.high_entropy.spread_bps, 1.5);
        assert_eq!(config.regime_params.high_entropy.skew_factor, 0.4);
    }
}

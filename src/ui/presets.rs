//! Preset Management (T-2.12)
//!
//! This module provides preset save/load functionality for parameter configuration screens.
//! Supports saving and loading parameter configurations as JSON presets, with validation
//! and built-in quick presets.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

// ============================================================================
// Types
// ============================================================================

/// Preset type identifier for different command types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PresetType {
    BacktestEvaluate,
    BacktestTune,
    BacktestMultiObjective,
    BacktestRegimeSearch,
    BacktestRegimeOptimize,
    BacktestTrain,
    BacktestWalkForwardML,
    BacktestSweep,
    BacktestWalkForward,
    BacktestOOSValidate,
    BacktestSimulate,
    BacktestGrid,
    BacktestCampaign,
    BacktestPaper,
    ResearchRun,
    ValidateRun,
    AlgorithmCreate,
}

impl PresetType {
    pub fn all() -> Vec<Self> {
        vec![
            Self::BacktestEvaluate,
            Self::BacktestTune,
            Self::BacktestMultiObjective,
            Self::BacktestRegimeSearch,
            Self::BacktestRegimeOptimize,
            Self::BacktestTrain,
            Self::BacktestWalkForwardML,
            Self::BacktestSweep,
            Self::BacktestWalkForward,
            Self::BacktestOOSValidate,
            Self::BacktestSimulate,
            Self::BacktestGrid,
            Self::BacktestCampaign,
            Self::BacktestPaper,
            Self::ResearchRun,
            Self::ValidateRun,
            Self::AlgorithmCreate,
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::BacktestEvaluate => "backtest-evaluate",
            Self::BacktestTune => "backtest-tune",
            Self::BacktestMultiObjective => "backtest-multi-objective",
            Self::BacktestRegimeSearch => "backtest-regime-search",
            Self::BacktestRegimeOptimize => "backtest-regime-optimize",
            Self::BacktestTrain => "backtest-train",
            Self::BacktestWalkForwardML => "backtest-walk-forward-ml",
            Self::BacktestSweep => "backtest-sweep",
            Self::BacktestWalkForward => "backtest-walk-forward",
            Self::BacktestOOSValidate => "backtest-oos-validate",
            Self::BacktestSimulate => "backtest-simulate",
            Self::BacktestGrid => "backtest-grid",
            Self::BacktestCampaign => "backtest-campaign",
            Self::BacktestPaper => "backtest-paper",
            Self::ResearchRun => "research-run",
            Self::ValidateRun => "validate-run",
            Self::AlgorithmCreate => "algorithm-create",
        }
    }
}

/// Preset metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresetMetadata {
    /// Preset name
    pub name: String,
    /// Preset type
    pub preset_type: PresetType,
    /// Description (optional)
    pub description: Option<String>,
    /// Created timestamp
    pub created_at: String,
    /// Last modified timestamp
    pub modified_at: String,
    /// Whether this is a built-in quick preset
    pub is_quick_preset: bool,
}

/// Preset data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Preset {
    /// Metadata
    pub metadata: PresetMetadata,
    /// Parameter values (screen-specific, stored as generic JSON)
    pub parameters: serde_json::Value,
}

/// Preset manager for saving/loading presets
#[derive(Debug, Clone)]
pub struct PresetManager {
    /// Base directory for preset storage
    presets_dir: PathBuf,
}

impl PresetManager {
    /// Create a new preset manager with default directory
    pub fn new() -> Result<Self> {
        Self::with_directory(Self::default_presets_dir()?)
    }

    /// Create a new preset manager with custom directory
    pub fn with_directory(presets_dir: PathBuf) -> Result<Self> {
        // Ensure directory exists
        fs::create_dir_all(&presets_dir)
            .with_context(|| format!("Failed to create presets directory: {:?}", presets_dir))?;

        Ok(Self { presets_dir })
    }

    /// Get default presets directory
    pub fn default_presets_dir() -> Result<PathBuf> {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .context("Could not determine home directory")?;
        Ok(PathBuf::from(home).join(".mars").join("presets"))
    }

    /// Get presets directory
    pub fn presets_dir(&self) -> &Path {
        &self.presets_dir
    }

    /// Save a preset
    pub fn save_preset(&self, preset: &Preset) -> Result<()> {
        // Validate preset
        self.validate_preset(preset)?;

        // Create type-specific directory
        let type_dir = self.presets_dir.join(preset.metadata.preset_type.label());
        fs::create_dir_all(&type_dir)
            .with_context(|| format!("Failed to create preset type directory: {:?}", type_dir))?;

        // Generate filename from preset name (sanitize)
        let filename = self.sanitize_filename(&preset.metadata.name);
        let filepath = type_dir.join(format!("{}.json", filename));

        // Serialize to JSON
        let json = serde_json::to_string_pretty(preset)
            .context("Failed to serialize preset to JSON")?;

        // Write to file
        fs::write(&filepath, json)
            .with_context(|| format!("Failed to write preset file: {:?}", filepath))?;

        Ok(())
    }

    /// Load a preset by name and type
    pub fn load_preset(&self, name: &str, preset_type: PresetType) -> Result<Preset> {
        let filename = self.sanitize_filename(name);
        let filepath = self
            .presets_dir
            .join(preset_type.label())
            .join(format!("{}.json", filename));

        if !filepath.exists() {
            anyhow::bail!("Preset not found: {} (type: {})", name, preset_type.label());
        }

        let json = fs::read_to_string(&filepath)
            .with_context(|| format!("Failed to read preset file: {:?}", filepath))?;

        let preset: Preset = serde_json::from_str(&json)
            .with_context(|| format!("Failed to parse preset JSON: {:?}", filepath))?;

        // Validate loaded preset
        self.validate_preset(&preset)?;

        Ok(preset)
    }

    /// List all presets of a given type
    pub fn list_presets(&self, preset_type: PresetType) -> Result<Vec<PresetMetadata>> {
        let type_dir = self.presets_dir.join(preset_type.label());

        if !type_dir.exists() {
            return Ok(Vec::new());
        }

        let mut presets = Vec::new();

        for entry in fs::read_dir(&type_dir)
            .with_context(|| format!("Failed to read preset directory: {:?}", type_dir))?
        {
            let entry = entry.context("Failed to read directory entry")?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                match self.load_preset_metadata(&path) {
                    Ok(metadata) => presets.push(metadata),
                    Err(e) => {
                        eprintln!("Warning: Failed to load preset metadata from {:?}: {}", path, e);
                    }
                }
            }
        }

        // Sort by name
        presets.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(presets)
    }

    /// List all presets across all types
    pub fn list_all_presets(&self) -> Result<HashMap<PresetType, Vec<PresetMetadata>>> {
        let mut all_presets = HashMap::new();

        for preset_type in PresetType::all() {
            match self.list_presets(preset_type) {
                Ok(presets) => {
                    if !presets.is_empty() {
                        all_presets.insert(preset_type, presets);
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Failed to list presets for {}: {}", preset_type.label(), e);
                }
            }
        }

        Ok(all_presets)
    }

    /// Get quick presets (built-in presets)
    pub fn quick_presets(&self, preset_type: PresetType) -> Vec<Preset> {
        match preset_type {
            PresetType::BacktestEvaluate => Self::quick_presets_backtest_evaluate(),
            PresetType::BacktestTune => Self::quick_presets_backtest_tune(),
            PresetType::BacktestMultiObjective => Self::quick_presets_backtest_multi_objective(),
            PresetType::BacktestRegimeSearch => Self::quick_presets_backtest_regime_search(),
            PresetType::BacktestRegimeOptimize => Self::quick_presets_backtest_regime_optimize(),
            PresetType::BacktestTrain => Self::quick_presets_backtest_train(),
            PresetType::BacktestWalkForwardML => Self::quick_presets_backtest_walk_forward_ml(),
            PresetType::BacktestSweep => Self::quick_presets_backtest_sweep(),
            PresetType::BacktestWalkForward => Self::quick_presets_backtest_walk_forward(),
            PresetType::BacktestOOSValidate => Self::quick_presets_backtest_oos_validate(),
            PresetType::BacktestSimulate => Self::quick_presets_backtest_simulate(),
            PresetType::BacktestGrid => Self::quick_presets_backtest_grid(),
            PresetType::BacktestCampaign => Self::quick_presets_backtest_campaign(),
            PresetType::BacktestPaper => Self::quick_presets_backtest_paper(),
            PresetType::ResearchRun => Self::quick_presets_research_run(),
            PresetType::ValidateRun => Self::quick_presets_validate_run(),
            PresetType::AlgorithmCreate => Self::quick_presets_algorithm_create(),
        }
    }

    /// Validate a preset
    pub fn validate_preset(&self, preset: &Preset) -> Result<()> {
        // Validate metadata
        if preset.metadata.name.is_empty() {
            anyhow::bail!("Preset name cannot be empty");
        }

        if preset.metadata.name.len() > 100 {
            anyhow::bail!("Preset name too long (max 100 characters)");
        }

        // Validate parameters is an object
        if !preset.parameters.is_object() {
            anyhow::bail!("Preset parameters must be a JSON object");
        }

        Ok(())
    }

    /// Delete a preset
    pub fn delete_preset(&self, name: &str, preset_type: PresetType) -> Result<()> {
        let filename = self.sanitize_filename(name);
        let filepath = self
            .presets_dir
            .join(preset_type.label())
            .join(format!("{}.json", filename));

        if !filepath.exists() {
            anyhow::bail!("Preset not found: {} (type: {})", name, preset_type.label());
        }

        // Don't allow deleting quick presets
        let preset = self.load_preset(name, preset_type)?;
        if preset.metadata.is_quick_preset {
            anyhow::bail!("Cannot delete built-in quick presets");
        }

        fs::remove_file(&filepath)
            .with_context(|| format!("Failed to delete preset file: {:?}", filepath))?;

        Ok(())
    }

    // ============================================================================
    // Private Helpers
    // ============================================================================

    fn load_preset_metadata(&self, filepath: &Path) -> Result<PresetMetadata> {
        let json = fs::read_to_string(filepath)
            .with_context(|| format!("Failed to read preset file: {:?}", filepath))?;

        let preset: Preset = serde_json::from_str(&json)
            .with_context(|| format!("Failed to parse preset JSON: {:?}", filepath))?;

        Ok(preset.metadata)
    }

    fn sanitize_filename(&self, name: &str) -> String {
        name.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
                _ => '_',
            })
            .collect()
    }

    // ============================================================================
    // Quick Presets
    // ============================================================================

    fn quick_presets_backtest_evaluate() -> Vec<Preset> {
        vec![
            Self::create_quick_preset(
                PresetType::BacktestEvaluate,
                "default",
                "Default conservative settings",
                serde_json::json!({
                    "data_path": "",
                    "algorithm": "as",
                    "spread": 1.0,
                    "skew": 0.5,
                    "max_inventory": 0.1,
                    "quote_size": 0.001,
                    "fee_rate": 0.0001,
                    "naive_fills": false,
                    "fill_prob": 0.1,
                    "queue_pos": 0.5,
                }),
            ),
            Self::create_quick_preset(
                PresetType::BacktestEvaluate,
                "aggressive",
                "Aggressive trading settings",
                serde_json::json!({
                    "data_path": "",
                    "algorithm": "as",
                    "spread": 0.5,
                    "skew": 0.3,
                    "max_inventory": 0.2,
                    "quote_size": 0.002,
                    "fee_rate": 0.0001,
                    "naive_fills": false,
                    "fill_prob": 0.15,
                    "queue_pos": 0.3,
                }),
            ),
        ]
    }

    fn quick_presets_backtest_tune() -> Vec<Preset> {
        vec![
            Self::create_quick_preset(
                PresetType::BacktestTune,
                "wide-grid",
                "Wide parameter grid for exploration",
                serde_json::json!({
                    "data_path": "",
                    "algorithm": "as",
                    "spreads": "0.5,1.0,1.5,2.0,2.5",
                    "skews": "0.3,0.5,0.7",
                    "high_entropies": "0.5,0.6,0.7",
                    "fill_probs": "0.05,0.10,0.15",
                }),
            ),
            Self::create_quick_preset(
                PresetType::BacktestTune,
                "fine-grid",
                "Fine parameter grid for refinement",
                serde_json::json!({
                    "data_path": "",
                    "algorithm": "as",
                    "spreads": "0.9,1.0,1.1",
                    "skews": "0.45,0.50,0.55",
                    "high_entropies": "0.55,0.60,0.65",
                    "fill_probs": "0.08,0.10,0.12",
                }),
            ),
        ]
    }

    fn quick_presets_backtest_multi_objective() -> Vec<Preset> {
        vec![
            Self::create_quick_preset(
                PresetType::BacktestMultiObjective,
                "balanced",
                "Balanced objective weights",
                serde_json::json!({
                    "data_path": "",
                    "algorithm": "as",
                    "w_sharpe": 0.25,
                    "w_drawdown": 0.25,
                    "w_fill": 0.25,
                    "w_turnover": 0.25,
                }),
            ),
            Self::create_quick_preset(
                PresetType::BacktestMultiObjective,
                "sharpe-focused",
                "Sharpe ratio focused",
                serde_json::json!({
                    "data_path": "",
                    "algorithm": "as",
                    "w_sharpe": 0.5,
                    "w_drawdown": 0.2,
                    "w_fill": 0.2,
                    "w_turnover": 0.1,
                }),
            ),
        ]
    }

    fn quick_presets_backtest_regime_search() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestRegimeSearch,
            "default",
            "Default regime search settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "high_spreads": "1.0,1.5,2.0",
                "med_spreads": "0.5,1.0",
                "low_spreads": "0.3,0.5",
            }),
        )]
    }

    fn quick_presets_backtest_regime_optimize() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestRegimeOptimize,
            "default",
            "Default regime optimize settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "spreads": "0.5,1.0,1.5",
                "skews": "0.3,0.5,0.7",
            }),
        )]
    }

    fn quick_presets_backtest_train() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestTrain,
            "default",
            "Default ML training settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "ml-spread-skew",
                "train_ratio": 0.8,
                "spread_intercepts": "0.001,0.002",
                "spread_entropy_weights": "0.1,0.2",
                "spread_vol_weights": "0.1,0.2",
            }),
        )]
    }

    fn quick_presets_backtest_walk_forward_ml() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestWalkForwardML,
            "default",
            "Default walk-forward ML settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "ml",
                "folds": 5,
                "test_hours": 168.0,
            }),
        )]
    }

    fn quick_presets_backtest_sweep() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestSweep,
            "default",
            "Default sweep settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "spreads": "0.5,1.0,1.5,2.0",
                "skews": "0.3,0.5,0.7",
            }),
        )]
    }

    fn quick_presets_backtest_walk_forward() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestWalkForward,
            "default",
            "Default walk-forward settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "folds": 5,
                "test_hours": 168.0,
                "rolling": true,
            }),
        )]
    }

    fn quick_presets_backtest_oos_validate() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestOOSValidate,
            "default",
            "Default OOS validation settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "holdout": 0.2,
                "embargo_hours": 24.0,
            }),
        )]
    }

    fn quick_presets_backtest_simulate() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestSimulate,
            "default",
            "Default simulation settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "weeks": 4,
                "session_hours": 8.0,
            }),
        )]
    }

    fn quick_presets_backtest_grid() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestGrid,
            "default",
            "Default grid search settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "spreads": "0.5,1.0,1.5",
                "skews": "0.3,0.5,0.7",
            }),
        )]
    }

    fn quick_presets_backtest_campaign() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestCampaign,
            "default",
            "Default campaign settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "weeks": 4,
                "session_hours": 8.0,
            }),
        )]
    }

    fn quick_presets_backtest_paper() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::BacktestPaper,
            "default",
            "Default paper trading settings",
            serde_json::json!({
                "data_path": "",
                "algorithm": "as",
                "duration": 8.0,
            }),
        )]
    }

    fn quick_presets_research_run() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::ResearchRun,
            "default",
            "Default research run settings",
            serde_json::json!({
                "data": "",
                "output": "",
                "symbol": "BTCUSDT",
                "min_samples": 1000,
            }),
        )]
    }

    fn quick_presets_validate_run() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::ValidateRun,
            "default",
            "Default validation run settings",
            serde_json::json!({
                "data": "",
                "results": "",
                "name": "validation-run",
            }),
        )]
    }

    fn quick_presets_algorithm_create() -> Vec<Preset> {
        vec![Self::create_quick_preset(
            PresetType::AlgorithmCreate,
            "default",
            "Default algorithm creation settings",
            serde_json::json!({
                "research": "",
                "output": "",
                "symbol": "BTCUSDT",
                "validate": false,
            }),
        )]
    }

    fn create_quick_preset(
        preset_type: PresetType,
        name: &str,
        description: &str,
        parameters: serde_json::Value,
    ) -> Preset {
        let now = chrono::Utc::now().to_rfc3339();
        Preset {
            metadata: PresetMetadata {
                name: name.to_string(),
                preset_type,
                description: Some(description.to_string()),
                created_at: now.clone(),
                modified_at: now,
                is_quick_preset: true,
            },
            parameters,
        }
    }
}

impl Default for PresetManager {
    fn default() -> Self {
        Self::new().expect("Failed to create default PresetManager")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_manager() -> (PresetManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let manager = PresetManager::with_directory(temp_dir.path().to_path_buf()).unwrap();
        (manager, temp_dir)
    }

    #[test]
    fn test_preset_manager_creation() {
        let (manager, _temp_dir) = create_test_manager();
        assert!(manager.presets_dir().exists());
    }

    #[test]
    fn test_save_and_load_preset() {
        let (manager, _temp_dir) = create_test_manager();

        let preset = Preset {
            metadata: PresetMetadata {
                name: "test-preset".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: Some("Test preset".to_string()),
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({
                "spread": 1.0,
                "skew": 0.5,
            }),
        };

        // Save
        manager.save_preset(&preset).unwrap();

        // Load
        let loaded = manager.load_preset("test-preset", PresetType::BacktestEvaluate).unwrap();
        assert_eq!(loaded.metadata.name, "test-preset");
        assert_eq!(loaded.parameters, preset.parameters);
    }

    #[test]
    fn test_list_presets() {
        let (manager, _temp_dir) = create_test_manager();

        // Save multiple presets
        for i in 0..3 {
            let preset = Preset {
                metadata: PresetMetadata {
                    name: format!("preset-{}", i),
                    preset_type: PresetType::BacktestEvaluate,
                    description: None,
                    created_at: chrono::Utc::now().to_rfc3339(),
                    modified_at: chrono::Utc::now().to_rfc3339(),
                    is_quick_preset: false,
                },
                parameters: serde_json::json!({}),
            };
            manager.save_preset(&preset).unwrap();
        }

        // List
        let presets = manager.list_presets(PresetType::BacktestEvaluate).unwrap();
        assert_eq!(presets.len(), 3);
        assert_eq!(presets[0].name, "preset-0");
    }

    #[test]
    fn test_quick_presets() {
        let (manager, _temp_dir) = create_test_manager();

        let quick = manager.quick_presets(PresetType::BacktestEvaluate);
        assert!(!quick.is_empty());
        assert!(quick[0].metadata.is_quick_preset);
    }

    #[test]
    fn test_validate_preset() {
        let (manager, _temp_dir) = create_test_manager();

        // Valid preset
        let valid = Preset {
            metadata: PresetMetadata {
                name: "valid".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({}),
        };
        assert!(manager.validate_preset(&valid).is_ok());

        // Invalid: empty name
        let invalid = Preset {
            metadata: PresetMetadata {
                name: "".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({}),
        };
        assert!(manager.validate_preset(&invalid).is_err());

        // Invalid: parameters not an object
        let invalid2 = Preset {
            metadata: PresetMetadata {
                name: "test".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!([]),
        };
        assert!(manager.validate_preset(&invalid2).is_err());
    }

    #[test]
    fn test_delete_preset() {
        let (manager, _temp_dir) = create_test_manager();

        let preset = Preset {
            metadata: PresetMetadata {
                name: "to-delete".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({}),
        };

        manager.save_preset(&preset).unwrap();
        assert!(manager.load_preset("to-delete", PresetType::BacktestEvaluate).is_ok());

        manager.delete_preset("to-delete", PresetType::BacktestEvaluate).unwrap();
        assert!(manager.load_preset("to-delete", PresetType::BacktestEvaluate).is_err());
    }

    #[test]
    fn test_sanitize_filename() {
        let (manager, _temp_dir) = create_test_manager();
        assert_eq!(manager.sanitize_filename("test-preset"), "test-preset");
        assert_eq!(manager.sanitize_filename("test preset"), "test_preset");
        assert_eq!(manager.sanitize_filename("test@preset#123"), "test_preset_123");
    }

    #[test]
    fn test_preset_type_all() {
        let all = PresetType::all();
        assert_eq!(all.len(), 17);
    }

    #[test]
    fn test_list_all_presets() {
        let (manager, _temp_dir) = create_test_manager();

        // Save presets of different types
        for preset_type in [PresetType::BacktestEvaluate, PresetType::BacktestTune] {
            let preset = Preset {
                metadata: PresetMetadata {
                    name: "test".to_string(),
                    preset_type,
                    description: None,
                    created_at: chrono::Utc::now().to_rfc3339(),
                    modified_at: chrono::Utc::now().to_rfc3339(),
                    is_quick_preset: false,
                },
                parameters: serde_json::json!({}),
            };
            manager.save_preset(&preset).unwrap();
        }

        let all = manager.list_all_presets().unwrap();
        assert!(all.len() >= 2);
    }

    #[test]
    fn test_load_nonexistent_preset() {
        let (manager, _temp_dir) = create_test_manager();
        assert!(manager.load_preset("nonexistent", PresetType::BacktestEvaluate).is_err());
    }

    #[test]
    fn test_delete_nonexistent_preset() {
        let (manager, _temp_dir) = create_test_manager();
        assert!(manager.delete_preset("nonexistent", PresetType::BacktestEvaluate).is_err());
    }

    #[test]
    fn test_quick_presets_all_types() {
        let (manager, _temp_dir) = create_test_manager();

        for preset_type in PresetType::all() {
            let quick = manager.quick_presets(preset_type);
            // All types should have at least one quick preset
            assert!(!quick.is_empty(), "No quick presets for {:?}", preset_type);
        }
    }

    #[test]
    fn test_save_preset_with_special_characters() {
        let (manager, _temp_dir) = create_test_manager();

        let preset = Preset {
            metadata: PresetMetadata {
                name: "test@preset#123".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({}),
        };

        manager.save_preset(&preset).unwrap();
        // Should be able to load with sanitized name
        let loaded = manager.load_preset("test@preset#123", PresetType::BacktestEvaluate).unwrap();
        assert_eq!(loaded.metadata.name, "test@preset#123");
    }

    #[test]
    fn test_save_multiple_presets_same_type() {
        let (manager, _temp_dir) = create_test_manager();

        for i in 0..5 {
            let preset = Preset {
                metadata: PresetMetadata {
                    name: format!("preset-{}", i),
                    preset_type: PresetType::BacktestTune,
                    description: Some(format!("Preset {}", i)),
                    created_at: chrono::Utc::now().to_rfc3339(),
                    modified_at: chrono::Utc::now().to_rfc3339(),
                    is_quick_preset: false,
                },
                parameters: serde_json::json!({
                    "spreads": format!("{}", i),
                }),
            };
            manager.save_preset(&preset).unwrap();
        }

        let presets = manager.list_presets(PresetType::BacktestTune).unwrap();
        assert_eq!(presets.len(), 5);
    }

    #[test]
    fn test_save_preset_overwrites_existing() {
        let (manager, _temp_dir) = create_test_manager();

        let preset1 = Preset {
            metadata: PresetMetadata {
                name: "same-name".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: Some("First version".to_string()),
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({"spread": 1.0}),
        };

        let preset2 = Preset {
            metadata: PresetMetadata {
                name: "same-name".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: Some("Second version".to_string()),
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({"spread": 2.0}),
        };

        manager.save_preset(&preset1).unwrap();
        manager.save_preset(&preset2).unwrap();

        let loaded = manager.load_preset("same-name", PresetType::BacktestEvaluate).unwrap();
        assert_eq!(loaded.metadata.description, Some("Second version".to_string()));
        assert_eq!(loaded.parameters["spread"], 2.0);
    }

    #[test]
    fn test_validate_preset_name_too_long() {
        let (manager, _temp_dir) = create_test_manager();

        let preset = Preset {
            metadata: PresetMetadata {
                name: "a".repeat(101),
                preset_type: PresetType::BacktestEvaluate,
                description: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({}),
        };

        assert!(manager.validate_preset(&preset).is_err());
    }

    #[test]
    fn test_validate_preset_parameters_array() {
        let (manager, _temp_dir) = create_test_manager();

        let preset = Preset {
            metadata: PresetMetadata {
                name: "test".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!([]),
        };

        assert!(manager.validate_preset(&preset).is_err());
    }

    #[test]
    fn test_validate_preset_parameters_string() {
        let (manager, _temp_dir) = create_test_manager();

        let preset = Preset {
            metadata: PresetMetadata {
                name: "test".to_string(),
                preset_type: PresetType::BacktestEvaluate,
                description: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!("invalid"),
        };

        assert!(manager.validate_preset(&preset).is_err());
    }

    #[test]
    fn test_list_presets_empty_directory() {
        let (manager, _temp_dir) = create_test_manager();

        let presets = manager.list_presets(PresetType::BacktestSimulate).unwrap();
        assert_eq!(presets.len(), 0);
    }

    #[test]
    fn test_list_all_presets_empty() {
        let (manager, _temp_dir) = create_test_manager();

        let all = manager.list_all_presets().unwrap();
        assert_eq!(all.len(), 0);
    }

    #[test]
    fn test_delete_quick_preset_fails() {
        let (manager, _temp_dir) = create_test_manager();

        // Try to delete a quick preset (they're not saved, but test the logic)
        let quick = manager.quick_presets(PresetType::BacktestEvaluate);
        if !quick.is_empty() {
            // Save a quick preset first
            let mut preset = quick[0].clone();
            preset.metadata.is_quick_preset = true;
            manager.save_preset(&preset).unwrap();

            // Try to delete - should fail
            assert!(manager.delete_preset(&preset.metadata.name, PresetType::BacktestEvaluate).is_err());
        }
    }

    #[test]
    fn test_preset_metadata_serialization() {
        let metadata = PresetMetadata {
            name: "test".to_string(),
            preset_type: PresetType::BacktestEvaluate,
            description: Some("Test".to_string()),
            created_at: chrono::Utc::now().to_rfc3339(),
            modified_at: chrono::Utc::now().to_rfc3339(),
            is_quick_preset: false,
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: PresetMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(metadata.name, deserialized.name);
        assert_eq!(metadata.preset_type, deserialized.preset_type);
    }

    #[test]
    fn test_preset_full_serialization() {
        let preset = Preset {
            metadata: PresetMetadata {
                name: "test".to_string(),
                preset_type: PresetType::BacktestTune,
                description: Some("Test preset".to_string()),
                created_at: chrono::Utc::now().to_rfc3339(),
                modified_at: chrono::Utc::now().to_rfc3339(),
                is_quick_preset: false,
            },
            parameters: serde_json::json!({
                "spreads": "1.0,2.0,3.0",
                "skews": "0.3,0.5",
                "nested": {
                    "value": 42,
                    "array": [1, 2, 3],
                },
            }),
        };

        let json = serde_json::to_string_pretty(&preset).unwrap();
        let deserialized: Preset = serde_json::from_str(&json).unwrap();
        assert_eq!(preset.metadata.name, deserialized.metadata.name);
        assert_eq!(preset.parameters, deserialized.parameters);
    }

    #[test]
    fn test_default_presets_dir() {
        let dir = PresetManager::default_presets_dir().unwrap();
        assert!(dir.to_string_lossy().contains(".mars"));
        assert!(dir.to_string_lossy().contains("presets"));
    }

    #[test]
    fn test_preset_type_label() {
        assert_eq!(PresetType::BacktestEvaluate.label(), "backtest-evaluate");
        assert_eq!(PresetType::BacktestTune.label(), "backtest-tune");
        assert_eq!(PresetType::ResearchRun.label(), "research-run");
    }

    #[test]
    fn test_quick_presets_have_valid_metadata() {
        let (manager, _temp_dir) = create_test_manager();

        for preset_type in PresetType::all() {
            let quick = manager.quick_presets(preset_type);
            for preset in quick {
                assert!(!preset.metadata.name.is_empty());
                assert_eq!(preset.metadata.preset_type, preset_type);
                assert!(preset.metadata.is_quick_preset);
                assert!(preset.metadata.description.is_some());
            }
        }
    }

    #[test]
    fn test_quick_presets_validate() {
        let (manager, _temp_dir) = create_test_manager();

        for preset_type in PresetType::all() {
            let quick = manager.quick_presets(preset_type);
            for preset in quick {
                assert!(manager.validate_preset(&preset).is_ok());
            }
        }
    }
}

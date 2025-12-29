//! Algorithm Selection Persistence (Task TUI-8.0)
//!
//! Provides persistence for the currently selected algorithm across TUI sessions.
//! Stores the selected algorithm ID in a simple JSON file that is loaded on startup.
//!
//! # Features
//! - Save selected algorithm ID to disk
//! - Load selected algorithm ID on startup
//! - Integration with GlobalState via load_selected_algorithm()
//! - Automatic directory creation
//! - Graceful handling of missing/corrupt files
//!
//! # File Location
//! Default: `./data/ui/selected_algorithm.json`

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::core::algorithm_config::{AlgorithmConfig, StrategyType};
use crate::core::config_store::{ConfigStore, ConfigStoreConfig};
use crate::ui::state::{AlgorithmConfigSummary, GlobalState};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for algorithm selection persistence
#[derive(Debug, Clone)]
pub struct AlgorithmPersistenceConfig {
    /// Path to the persistence file
    pub file_path: PathBuf,
    /// Path to the config store (for loading full algorithm details)
    pub config_store_path: PathBuf,
}

impl Default for AlgorithmPersistenceConfig {
    fn default() -> Self {
        Self {
            file_path: PathBuf::from("./data/ui/selected_algorithm.json"),
            config_store_path: PathBuf::from("./data/configs"),
        }
    }
}

impl AlgorithmPersistenceConfig {
    /// Create configuration with custom paths
    pub fn new(file_path: impl AsRef<Path>, config_store_path: impl AsRef<Path>) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            config_store_path: config_store_path.as_ref().to_path_buf(),
        }
    }

    /// Create configuration with custom file path, default config store
    pub fn with_file_path(file_path: impl AsRef<Path>) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            ..Default::default()
        }
    }
}

// ============================================================================
// Persisted Selection Data
// ============================================================================

/// Data structure persisted to disk
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedSelection {
    /// ID of the selected algorithm
    pub algorithm_id: String,
    /// Timestamp when selection was made
    pub selected_at: DateTime<Utc>,
    /// Symbol the algorithm is for (for validation)
    pub symbol: String,
    /// Strategy type (for display before full load)
    pub strategy_type: StrategyType,
    /// Human-readable name (for display before full load)
    pub name: String,
}

impl PersistedSelection {
    /// Create a new persisted selection from an AlgorithmConfig
    pub fn from_config(config: &AlgorithmConfig) -> Self {
        Self {
            algorithm_id: config.id.clone(),
            selected_at: Utc::now(),
            symbol: config.symbol.clone(),
            strategy_type: config.strategy_type,
            name: config.name.clone(),
        }
    }

    /// Create a new persisted selection from an AlgorithmConfigSummary
    pub fn from_summary(summary: &AlgorithmConfigSummary) -> Self {
        Self {
            algorithm_id: summary.id.clone(),
            selected_at: Utc::now(),
            symbol: String::new(), // Summary doesn't have symbol
            strategy_type: summary.strategy_type,
            name: summary.name.clone(),
        }
    }

    /// Convert to AlgorithmConfigSummary for GlobalState
    pub fn to_summary(&self) -> AlgorithmConfigSummary {
        AlgorithmConfigSummary {
            id: self.algorithm_id.clone(),
            name: self.name.clone(),
            strategy_type: self.strategy_type,
            created_at: self.selected_at, // Use selection time as fallback
        }
    }
}

// ============================================================================
// Algorithm Persistence Manager
// ============================================================================

/// Manages persistence of the selected algorithm
#[derive(Debug, Clone)]
pub struct AlgorithmPersistence {
    config: AlgorithmPersistenceConfig,
}

impl AlgorithmPersistence {
    /// Create a new persistence manager with default configuration
    pub fn new() -> Self {
        Self {
            config: AlgorithmPersistenceConfig::default(),
        }
    }

    /// Create a new persistence manager with custom configuration
    pub fn with_config(config: AlgorithmPersistenceConfig) -> Self {
        Self { config }
    }

    /// Get the file path
    pub fn file_path(&self) -> &Path {
        &self.config.file_path
    }

    /// Save the selected algorithm to disk
    ///
    /// Creates the parent directory if it doesn't exist.
    pub fn save_selection(&self, selection: &PersistedSelection) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = self.config.file_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        // Serialize to JSON
        let json = serde_json::to_string_pretty(selection)
            .context("Failed to serialize selection")?;

        // Write to file
        fs::write(&self.config.file_path, json)
            .with_context(|| format!("Failed to write selection file: {:?}", self.config.file_path))?;

        Ok(())
    }

    /// Save selection from an AlgorithmConfig
    pub fn save_from_config(&self, config: &AlgorithmConfig) -> Result<()> {
        let selection = PersistedSelection::from_config(config);
        self.save_selection(&selection)
    }

    /// Save selection from an AlgorithmConfigSummary
    pub fn save_from_summary(&self, summary: &AlgorithmConfigSummary) -> Result<()> {
        let selection = PersistedSelection::from_summary(summary);
        self.save_selection(&selection)
    }

    /// Load the persisted selection from disk
    ///
    /// Returns None if:
    /// - File doesn't exist
    /// - File is corrupted/invalid JSON
    /// - Any I/O error occurs
    pub fn load_selection(&self) -> Option<PersistedSelection> {
        // Check if file exists
        if !self.config.file_path.exists() {
            return None;
        }

        // Read file contents
        let contents = fs::read_to_string(&self.config.file_path).ok()?;

        // Parse JSON
        serde_json::from_str(&contents).ok()
    }

    /// Clear the persisted selection (delete the file)
    pub fn clear_selection(&self) -> Result<()> {
        if self.config.file_path.exists() {
            fs::remove_file(&self.config.file_path)
                .with_context(|| format!("Failed to remove selection file: {:?}", self.config.file_path))?;
        }
        Ok(())
    }

    /// Check if a selection exists
    pub fn has_selection(&self) -> bool {
        self.config.file_path.exists()
    }

    /// Load selection and convert to AlgorithmConfigSummary
    ///
    /// This is the primary method for loading into GlobalState.
    pub fn load_as_summary(&self) -> Option<AlgorithmConfigSummary> {
        self.load_selection().map(|s| s.to_summary())
    }

    /// Load and validate selection against ConfigStore
    ///
    /// Returns the full AlgorithmConfig if:
    /// - A selection exists
    /// - The config still exists in ConfigStore
    /// - The config is still active
    ///
    /// Clears the persisted selection if validation fails.
    pub fn load_and_validate(&self) -> Option<AlgorithmConfig> {
        let selection = self.load_selection()?;

        // Try to load from ConfigStore
        let store_config = ConfigStoreConfig::with_path(&self.config.config_store_path)
            .without_parquet()
            .without_audit();

        let mut store = ConfigStore::new(store_config).ok()?;
        let config = store.load(&selection.algorithm_id).ok()??;

        // Validate the config is still active
        if !config.active {
            // Config was deactivated, clear selection
            let _ = self.clear_selection();
            return None;
        }

        Some(config)
    }

    /// Load selection into GlobalState
    ///
    /// This updates the GlobalState's active_algorithm field with the
    /// persisted selection. Uses the lightweight summary for display.
    pub fn load_into_state(&self, state: &mut GlobalState) -> bool {
        if let Some(summary) = self.load_as_summary() {
            state.set_active_algorithm(Some(summary));
            true
        } else {
            false
        }
    }

    /// Save current GlobalState selection to disk
    pub fn save_from_state(&self, state: &GlobalState) -> Result<()> {
        if let Some(summary) = &state.active_algorithm {
            self.save_from_summary(summary)
        } else {
            self.clear_selection()
        }
    }
}

impl Default for AlgorithmPersistence {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Load the selected algorithm into GlobalState using default configuration
pub fn load_selected_algorithm(state: &mut GlobalState) -> bool {
    AlgorithmPersistence::new().load_into_state(state)
}

/// Save the current algorithm selection from GlobalState
pub fn save_selected_algorithm(state: &GlobalState) -> Result<()> {
    AlgorithmPersistence::new().save_from_state(state)
}

/// Clear the persisted algorithm selection
pub fn clear_selected_algorithm() -> Result<()> {
    AlgorithmPersistence::new().clear_selection()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // -------------------------------------------------------------------------
    // Test Helpers
    // -------------------------------------------------------------------------

    fn create_test_config() -> AlgorithmPersistenceConfig {
        let temp_dir = TempDir::new().unwrap();
        AlgorithmPersistenceConfig::new(
            temp_dir.path().join("selected_algorithm.json"),
            temp_dir.path().join("configs"),
        )
    }

    fn create_test_selection() -> PersistedSelection {
        PersistedSelection {
            algorithm_id: "momentum_btc_v1_20251229".to_string(),
            selected_at: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            strategy_type: StrategyType::Momentum,
            name: "Momentum BTC v1".to_string(),
        }
    }

    fn create_test_summary() -> AlgorithmConfigSummary {
        AlgorithmConfigSummary {
            id: "mm_eth_v2_20251229".to_string(),
            name: "Market Making ETH v2".to_string(),
            strategy_type: StrategyType::MarketMaking,
            created_at: Utc::now(),
        }
    }

    // -------------------------------------------------------------------------
    // Configuration Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_config_default() {
        let config = AlgorithmPersistenceConfig::default();
        assert!(config.file_path.to_string_lossy().contains("selected_algorithm.json"));
        assert!(config.config_store_path.to_string_lossy().contains("configs"));
    }

    #[test]
    fn test_config_with_file_path() {
        let config = AlgorithmPersistenceConfig::with_file_path("/tmp/test.json");
        assert_eq!(config.file_path, PathBuf::from("/tmp/test.json"));
    }

    #[test]
    fn test_config_new_custom() {
        let config = AlgorithmPersistenceConfig::new("/tmp/sel.json", "/tmp/cfgs");
        assert_eq!(config.file_path, PathBuf::from("/tmp/sel.json"));
        assert_eq!(config.config_store_path, PathBuf::from("/tmp/cfgs"));
    }

    // -------------------------------------------------------------------------
    // PersistedSelection Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_persisted_selection_from_summary() {
        let summary = create_test_summary();
        let selection = PersistedSelection::from_summary(&summary);

        assert_eq!(selection.algorithm_id, summary.id);
        assert_eq!(selection.name, summary.name);
        assert_eq!(selection.strategy_type, summary.strategy_type);
        assert!(selection.symbol.is_empty()); // Summary doesn't have symbol
    }

    #[test]
    fn test_persisted_selection_to_summary() {
        let selection = create_test_selection();
        let summary = selection.to_summary();

        assert_eq!(summary.id, selection.algorithm_id);
        assert_eq!(summary.name, selection.name);
        assert_eq!(summary.strategy_type, selection.strategy_type);
    }

    #[test]
    fn test_persisted_selection_roundtrip() {
        let selection = create_test_selection();

        // Serialize
        let json = serde_json::to_string(&selection).unwrap();

        // Deserialize
        let restored: PersistedSelection = serde_json::from_str(&json).unwrap();

        assert_eq!(selection.algorithm_id, restored.algorithm_id);
        assert_eq!(selection.symbol, restored.symbol);
        assert_eq!(selection.strategy_type, restored.strategy_type);
        assert_eq!(selection.name, restored.name);
    }

    #[test]
    fn test_persisted_selection_clone() {
        let selection = create_test_selection();
        let cloned = selection.clone();

        assert_eq!(selection.algorithm_id, cloned.algorithm_id);
        assert_eq!(selection.name, cloned.name);
    }

    #[test]
    fn test_persisted_selection_debug() {
        let selection = create_test_selection();
        let debug_str = format!("{:?}", selection);

        assert!(debug_str.contains("PersistedSelection"));
        assert!(debug_str.contains(&selection.algorithm_id));
    }

    // -------------------------------------------------------------------------
    // AlgorithmPersistence Construction Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_persistence_new() {
        let persistence = AlgorithmPersistence::new();
        assert!(persistence.file_path().to_string_lossy().contains("selected_algorithm.json"));
    }

    #[test]
    fn test_persistence_with_config() {
        let config = create_test_config();
        let persistence = AlgorithmPersistence::with_config(config.clone());
        assert_eq!(persistence.file_path(), &config.file_path);
    }

    #[test]
    fn test_persistence_default() {
        let persistence = AlgorithmPersistence::default();
        assert!(persistence.file_path().to_string_lossy().contains("selected_algorithm.json"));
    }

    // -------------------------------------------------------------------------
    // Save/Load Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_save_and_load_selection() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = create_test_selection();

        // Save
        persistence.save_selection(&selection).unwrap();

        // Verify file exists
        assert!(persistence.has_selection());

        // Load
        let loaded = persistence.load_selection().unwrap();

        assert_eq!(loaded.algorithm_id, selection.algorithm_id);
        assert_eq!(loaded.symbol, selection.symbol);
        assert_eq!(loaded.strategy_type, selection.strategy_type);
        assert_eq!(loaded.name, selection.name);
    }

    #[test]
    fn test_save_from_summary() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let summary = create_test_summary();

        // Save
        persistence.save_from_summary(&summary).unwrap();

        // Load
        let loaded = persistence.load_selection().unwrap();

        assert_eq!(loaded.algorithm_id, summary.id);
        assert_eq!(loaded.name, summary.name);
        assert_eq!(loaded.strategy_type, summary.strategy_type);
    }

    #[test]
    fn test_load_as_summary() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = create_test_selection();
        persistence.save_selection(&selection).unwrap();

        let summary = persistence.load_as_summary().unwrap();

        assert_eq!(summary.id, selection.algorithm_id);
        assert_eq!(summary.name, selection.name);
        assert_eq!(summary.strategy_type, selection.strategy_type);
    }

    #[test]
    fn test_load_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("nonexistent.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        assert!(!persistence.has_selection());
        assert!(persistence.load_selection().is_none());
        assert!(persistence.load_as_summary().is_none());
    }

    #[test]
    fn test_load_corrupt_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("corrupt.json");

        // Write corrupt JSON
        fs::write(&file_path, "not valid json {{{").unwrap();

        let config = AlgorithmPersistenceConfig::new(&file_path, temp_dir.path().join("configs"));
        let persistence = AlgorithmPersistence::with_config(config);

        // Should return None, not panic
        assert!(persistence.load_selection().is_none());
    }

    #[test]
    fn test_load_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.json");

        // Write empty file
        fs::write(&file_path, "").unwrap();

        let config = AlgorithmPersistenceConfig::new(&file_path, temp_dir.path().join("configs"));
        let persistence = AlgorithmPersistence::with_config(config);

        // Should return None, not panic
        assert!(persistence.load_selection().is_none());
    }

    // -------------------------------------------------------------------------
    // Clear Selection Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_clear_selection() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = create_test_selection();
        persistence.save_selection(&selection).unwrap();

        assert!(persistence.has_selection());

        persistence.clear_selection().unwrap();

        assert!(!persistence.has_selection());
        assert!(persistence.load_selection().is_none());
    }

    #[test]
    fn test_clear_nonexistent_selection() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("nonexistent.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        // Should not fail when file doesn't exist
        persistence.clear_selection().unwrap();
    }

    // -------------------------------------------------------------------------
    // Has Selection Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_has_selection_false_initially() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        assert!(!persistence.has_selection());
    }

    #[test]
    fn test_has_selection_true_after_save() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = create_test_selection();
        persistence.save_selection(&selection).unwrap();

        assert!(persistence.has_selection());
    }

    // -------------------------------------------------------------------------
    // GlobalState Integration Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_load_into_state_empty() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let mut state = GlobalState::default();

        let loaded = persistence.load_into_state(&mut state);

        assert!(!loaded);
        assert!(state.active_algorithm.is_none());
    }

    #[test]
    fn test_load_into_state_with_selection() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = create_test_selection();
        persistence.save_selection(&selection).unwrap();

        let mut state = GlobalState::default();

        let loaded = persistence.load_into_state(&mut state);

        assert!(loaded);
        assert!(state.active_algorithm.is_some());

        let algo = state.active_algorithm.unwrap();
        assert_eq!(algo.id, selection.algorithm_id);
        assert_eq!(algo.name, selection.name);
        assert_eq!(algo.strategy_type, selection.strategy_type);
    }

    #[test]
    fn test_save_from_state_with_algorithm() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let mut state = GlobalState::default();
        state.set_active_algorithm(Some(create_test_summary()));

        persistence.save_from_state(&state).unwrap();

        assert!(persistence.has_selection());

        let loaded = persistence.load_selection().unwrap();
        assert_eq!(loaded.algorithm_id, "mm_eth_v2_20251229");
    }

    #[test]
    fn test_save_from_state_no_algorithm_clears() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        // Save initial selection
        let selection = create_test_selection();
        persistence.save_selection(&selection).unwrap();
        assert!(persistence.has_selection());

        // Save from state with no algorithm
        let state = GlobalState::default();
        persistence.save_from_state(&state).unwrap();

        // Selection should be cleared
        assert!(!persistence.has_selection());
    }

    // -------------------------------------------------------------------------
    // Directory Creation Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_creates_parent_directory() {
        let temp_dir = TempDir::new().unwrap();
        let nested_path = temp_dir.path().join("deep").join("nested").join("sel.json");

        let config = AlgorithmPersistenceConfig::new(&nested_path, temp_dir.path().join("configs"));
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = create_test_selection();
        persistence.save_selection(&selection).unwrap();

        assert!(nested_path.exists());
    }

    // -------------------------------------------------------------------------
    // Convenience Function Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_load_selected_algorithm_no_file() {
        let mut state = GlobalState::default();
        // This will try to load from default path which likely doesn't exist
        // We just verify it doesn't panic
        let _ = load_selected_algorithm(&mut state);
    }

    // -------------------------------------------------------------------------
    // Strategy Type Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_all_strategy_types_serialize() {
        for strategy_type in [StrategyType::Momentum, StrategyType::MarketMaking, StrategyType::Hybrid] {
            let selection = PersistedSelection {
                algorithm_id: "test".to_string(),
                selected_at: Utc::now(),
                symbol: "BTCUSDT".to_string(),
                strategy_type,
                name: "Test".to_string(),
            };

            let json = serde_json::to_string(&selection).unwrap();
            let restored: PersistedSelection = serde_json::from_str(&json).unwrap();

            assert_eq!(restored.strategy_type, strategy_type);
        }
    }

    // -------------------------------------------------------------------------
    // Edge Case Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_save_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        // Save first selection
        let selection1 = PersistedSelection {
            algorithm_id: "first".to_string(),
            selected_at: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            strategy_type: StrategyType::Momentum,
            name: "First".to_string(),
        };
        persistence.save_selection(&selection1).unwrap();

        // Save second selection (overwrite)
        let selection2 = PersistedSelection {
            algorithm_id: "second".to_string(),
            selected_at: Utc::now(),
            symbol: "ETHUSDT".to_string(),
            strategy_type: StrategyType::MarketMaking,
            name: "Second".to_string(),
        };
        persistence.save_selection(&selection2).unwrap();

        // Load should return second selection
        let loaded = persistence.load_selection().unwrap();
        assert_eq!(loaded.algorithm_id, "second");
        assert_eq!(loaded.name, "Second");
    }

    #[test]
    fn test_special_characters_in_name() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = PersistedSelection {
            algorithm_id: "test_v1".to_string(),
            selected_at: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            strategy_type: StrategyType::Momentum,
            name: "Test \"quoted\" & <special> chars!".to_string(),
        };
        persistence.save_selection(&selection).unwrap();

        let loaded = persistence.load_selection().unwrap();
        assert_eq!(loaded.name, "Test \"quoted\" & <special> chars!");
    }

    #[test]
    fn test_unicode_in_name() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = PersistedSelection {
            algorithm_id: "test_unicode".to_string(),
            selected_at: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            strategy_type: StrategyType::Momentum,
            name: "Test with unicode".to_string(),
        };
        persistence.save_selection(&selection).unwrap();

        let loaded = persistence.load_selection().unwrap();
        assert_eq!(loaded.name, "Test with unicode");
    }

    #[test]
    fn test_empty_algorithm_id() {
        let temp_dir = TempDir::new().unwrap();
        let config = AlgorithmPersistenceConfig::new(
            temp_dir.path().join("sel.json"),
            temp_dir.path().join("configs"),
        );
        let persistence = AlgorithmPersistence::with_config(config);

        let selection = PersistedSelection {
            algorithm_id: String::new(),
            selected_at: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            strategy_type: StrategyType::Momentum,
            name: "Empty ID".to_string(),
        };
        persistence.save_selection(&selection).unwrap();

        let loaded = persistence.load_selection().unwrap();
        assert!(loaded.algorithm_id.is_empty());
    }

    #[test]
    fn test_clone_persistence() {
        let persistence = AlgorithmPersistence::new();
        let cloned = persistence.clone();

        assert_eq!(persistence.file_path(), cloned.file_path());
    }

    #[test]
    fn test_debug_persistence() {
        let persistence = AlgorithmPersistence::new();
        let debug_str = format!("{:?}", persistence);

        assert!(debug_str.contains("AlgorithmPersistence"));
    }
}

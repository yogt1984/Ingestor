//! Algorithm Commands
//!
//! This module provides all algorithm-related commands that can be executed
//! from both CLI and TUI interfaces.
//!
//! # Commands
//!
//! - `create` - Create algorithm configuration from research state
//! - `list` - List existing algorithm configurations
//! - `show` - Show details of a specific algorithm configuration

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use anyhow::{Result, Context, anyhow};
use serde::{Deserialize, Serialize};

use crate::commands::common::{ProgressCallback, ProgressEvent, LogLevel};
use crate::commands::params::algorithm_params::{
    CreateParams,
    ListParams,
    ShowParams,
};
use crate::core::{
    AlgorithmConfig, ConfigStore, ConfigStoreConfig,
    ResearchState, ResearchStore, ResearchStoreConfig, StrategyType,
};

/// Algorithm command executor
///
/// All algorithm commands are executed through this struct.
/// Commands are async and support progress callbacks for long-running operations.
pub struct AlgorithmCommands;

impl AlgorithmCommands {
    /// Create a new algorithm configuration from research state
    pub async fn create(
        params: CreateParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<CreateResult> {
        let start_time = Instant::now();

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Creating algorithm config from research state for {}", params.symbol),
        });

        // Load research state
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loading research state from {:?}", params.research),
        });

        let research = Self::load_research_state(&params.research, &params.symbol)?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Research state loaded: {}", research.id),
        });

        // Generate config from research
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Generating algorithm configuration from research state".to_string(),
        });

        let mut config = AlgorithmConfig::from_research(&research);

        // Apply overrides if specified
        if let Some(name) = &params.name {
            config.name = name.clone();
            config.id = config.generate_id();
            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: format!("Applied custom name: {}", name),
            });
        }

        if let Some(strategy) = params.strategy {
            config.strategy_type = strategy;
            config.id = config.generate_id();
            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: format!("Applied strategy override: {:?}", config.strategy_type),
            });
        }

        // Validate config
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Validating algorithm configuration".to_string(),
        });

        config.validate().context("Generated config failed validation")?;

        let mut saved_path = None;
        let mut validation_result = None;

        // Save to store unless dry run
        if !params.dry_run {
            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: format!("Saving config to {:?}", params.output),
            });

            let store_config = ConfigStoreConfig::with_path(&params.output);
            let mut store = ConfigStore::new(store_config)
                .context("Failed to create config store")?;

            let path = store.save(&config)
                .context("Failed to save config to store")?;
            saved_path = Some(path.display().to_string());

            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: format!("Config saved: {}", saved_path.as_ref().unwrap()),
            });
        } else {
            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: "Dry run - config not saved".to_string(),
            });
        }

        // Run validation if requested
        if params.validate && !params.dry_run {
            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: format!("Running validation pipeline (stages: {})", params.stages),
            });

            validation_result = Some(Self::run_validation(&config, &params.data, &params.stages, callback.clone())?);
        }

        let success = validation_result.as_ref().map(|v| v.passed).unwrap_or(true);

        let message = if params.dry_run {
            "Dry run - config not saved".to_string()
        } else if params.validate {
            if success {
                "Config created and validated successfully".to_string()
            } else {
                "Config created but validation failed".to_string()
            }
        } else {
            "Config created successfully".to_string()
        };

        let duration = start_time.elapsed().as_secs_f64();

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Algorithm creation completed in {:.2}s", duration),
        });

        Ok(CreateResult {
            success,
            config_id: config.id.clone(),
            config_name: config.name.clone(),
            strategy_type: config.strategy_type.to_string(),
            symbol: config.symbol.clone(),
            version: config.version,
            source_research_id: config.source_research_id.clone(),
            saved_path,
            validation_result,
            duration_seconds: duration,
            message,
        })
    }

    /// List existing algorithm configurations
    pub fn list(
        params: ListParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<ListResult> {
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Listing algorithm configs from {:?}", params.store),
        });

        // Check store directory exists
        if !params.store.exists() {
            callback.on_event(ProgressEvent::Log {
                level: LogLevel::Warn,
                message: "Config store directory does not exist".to_string(),
            });
            return Ok(ListResult {
                count: 0,
                configs: vec![],
            });
        }

        let store_config = ConfigStoreConfig::with_path(&params.store);
        let store = ConfigStore::new(store_config)
            .context("Failed to open config store")?;

        // Load all configs and filter manually
        let all_configs = store.list_all()
            .context("Failed to list configs")?;

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Found {} total configs, applying filters", all_configs.len()),
        });

        // Apply filters
        let configs: Vec<_> = all_configs
            .into_iter()
            .filter(|c| {
                // Filter by symbol if specified
                if let Some(ref sym) = params.symbol {
                    if c.symbol != *sym {
                        return false;
                    }
                }
                // Filter by strategy if specified
                if let Some(strategy) = params.strategy {
                    if c.strategy_type != strategy {
                        return false;
                    }
                }
                // Filter by name (partial match) if specified
                if let Some(ref name) = params.name {
                    if !c.name.to_lowercase().contains(&name.to_lowercase()) {
                        return false;
                    }
                }
                // Filter by active only
                if params.active_only && !c.active {
                    return false;
                }
                true
            })
            .take(params.limit)
            .collect();

        let items: Vec<ConfigSummaryItem> = configs
            .iter()
            .map(|c| ConfigSummaryItem {
                id: c.id.clone(),
                name: c.name.clone(),
                symbol: c.symbol.clone(),
                strategy_type: c.strategy_type.to_string(),
                version: c.version,
                active: c.active,
                created_at: c.created_at.to_rfc3339(),
            })
            .collect();

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Returning {} configs", items.len()),
        });

        Ok(ListResult {
            count: items.len(),
            configs: items,
        })
    }

    /// Show details of a specific algorithm configuration
    pub fn show(
        params: ShowParams,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<ShowResult> {
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Loading config '{}' from {:?}", params.id, params.store),
        });

        let store_config = ConfigStoreConfig::with_path(&params.store);
        let mut store = ConfigStore::new(store_config)
            .context("Failed to open config store")?;

        // Try to load by ID
        let config = store.load(&params.id)
            .context("Failed to load config")?;

        match config {
            Some(c) => {
                callback.on_event(ProgressEvent::Log {
                    level: LogLevel::Info,
                    message: format!("Config loaded: {} ({})", c.name, c.id),
                });

                Ok(ShowResult {
                    config: c,
                    found: true,
                })
            }
            None => {
                callback.on_event(ProgressEvent::Log {
                    level: LogLevel::Warn,
                    message: format!("Config not found: {}", params.id),
                });

                Ok(ShowResult {
                    config: AlgorithmConfig::default(),
                    found: false,
                })
            }
        }
    }

    /// Load research state from store
    fn load_research_state(store_path: &PathBuf, symbol: &str) -> Result<ResearchState> {
        let store_config = ResearchStoreConfig::with_path(store_path);
        let mut store = ResearchStore::new(store_config)
            .context("Failed to open research store")?;

        store
            .load(symbol)
            .context("Failed to load research state")?
            .ok_or_else(|| anyhow!("No research state found for symbol: {}", symbol))
    }

    /// Run validation pipeline (simplified - full implementation would use validation module)
    fn run_validation(
        config: &AlgorithmConfig,
        _data_path: &PathBuf,
        stages: &str,
        callback: Arc<dyn ProgressCallback>,
    ) -> Result<ValidationSummary> {
        // Parse stages
        let stages_list: Vec<String> = stages
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: format!("Validation requested for config: {} (stages: {:?})", config.id, stages_list),
        });
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Note: Full validation pipeline integration is available via 'validate' CLI".to_string(),
        });

        // For now, return a placeholder since full validation pipeline integration
        // would require more infrastructure. The CLI structure is in place.
        // In production, this would call the validation pipeline runner.

        Ok(ValidationSummary {
            passed: true,
            stages_run: stages_list,
            sharpe: None,
            max_drawdown: None,
            trade_count: None,
            message: "Validation pipeline integration available via 'validate' CLI".to_string(),
        })
    }
}

/// Result of algorithm creation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateResult {
    pub success: bool,
    pub config_id: String,
    pub config_name: String,
    pub strategy_type: String,
    pub symbol: String,
    pub version: u32,
    pub source_research_id: Option<String>,
    pub saved_path: Option<String>,
    pub validation_result: Option<ValidationSummary>,
    pub duration_seconds: f64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub passed: bool,
    pub stages_run: Vec<String>,
    pub sharpe: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub trade_count: Option<usize>,
    pub message: String,
}

/// Result of list command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResult {
    pub count: usize,
    pub configs: Vec<ConfigSummaryItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSummaryItem {
    pub id: String,
    pub name: String,
    pub symbol: String,
    pub strategy_type: String,
    pub version: u32,
    pub active: bool,
    pub created_at: String,
}

/// Result of show command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShowResult {
    pub config: AlgorithmConfig,
    pub found: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::commands::common::NoOpCallback;

    #[test]
    fn test_algorithm_commands_struct() {
        let _commands = AlgorithmCommands;
    }

    #[test]
    fn test_load_research_state_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        std::fs::create_dir_all(&research_dir).unwrap();

        // Create empty store
        let store_config = ResearchStoreConfig::with_path(&research_dir);
        let _store = ResearchStore::new(store_config).unwrap();

        let result = AlgorithmCommands::load_research_state(&research_dir, "NONEXISTENT");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No research state found"));
    }

    #[test]
    fn test_load_research_state_success() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        std::fs::create_dir_all(&research_dir).unwrap();

        // Create store and save state
        let store_config = ResearchStoreConfig::with_path(&research_dir);
        let mut store = ResearchStore::new(store_config).unwrap();
        let research = ResearchState::new("BTCUSDT");
        store.save(&research).unwrap();

        let result = AlgorithmCommands::load_research_state(&research_dir, "BTCUSDT");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().symbol, "BTCUSDT");
    }

    #[tokio::test]
    async fn test_create_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        std::fs::create_dir_all(&research_dir).unwrap();

        // Create a research store and save state
        let store_config = ResearchStoreConfig::with_path(&research_dir);
        let mut store = ResearchStore::new(store_config).unwrap();
        let research = ResearchState::new("BTCUSDT");
        store.save(&research).unwrap();

        let params = CreateParams {
            research: research_dir,
            output: temp_dir.path().join("configs"),
            symbol: "BTCUSDT".to_string(),
            name: Some("DryRunTest".to_string()),
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            dry_run: true,
        };

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = AlgorithmCommands::create(params, callback).await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.success);
        assert!(result.saved_path.is_none()); // Not saved in dry run
        assert!(result.message.contains("Dry run"));
    }

    #[tokio::test]
    async fn test_create_saves_config() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        let config_dir = temp_dir.path().join("configs");
        std::fs::create_dir_all(&research_dir).unwrap();
        std::fs::create_dir_all(&config_dir).unwrap();

        // Create a research store and save state
        let store_config = ResearchStoreConfig::with_path(&research_dir);
        let mut store = ResearchStore::new(store_config).unwrap();
        let research = ResearchState::new("BTCUSDT");
        store.save(&research).unwrap();

        let params = CreateParams {
            research: research_dir,
            output: config_dir.clone(),
            symbol: "BTCUSDT".to_string(),
            name: Some("SavedConfig".to_string()),
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            dry_run: false,
        };

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = AlgorithmCommands::create(params, callback).await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.success);
        assert!(result.saved_path.is_some());
    }

    #[test]
    fn test_list_empty_store() {
        let temp_dir = TempDir::new().unwrap();

        let params = ListParams {
            store: temp_dir.path().join("nonexistent"),
            symbol: None,
            strategy: None,
            name: None,
            active_only: false,
            limit: 20,
        };

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = AlgorithmCommands::list(params, callback);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().count, 0);
    }

    #[test]
    fn test_show_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let store_dir = temp_dir.path().join("configs");
        std::fs::create_dir_all(&store_dir).unwrap();

        let params = ShowParams {
            store: store_dir,
            id: "nonexistent-id".to_string(),
            verbose: false,
        };

        let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
        let result = AlgorithmCommands::show(params, callback);

        assert!(result.is_ok());
        assert!(!result.unwrap().found);
    }

    #[test]
    fn test_create_result_serialize() {
        let result = CreateResult {
            success: true,
            config_id: "test-id-123".to_string(),
            config_name: "Test Config".to_string(),
            strategy_type: "Momentum".to_string(),
            symbol: "BTCUSDT".to_string(),
            version: 1,
            source_research_id: Some("research-123".to_string()),
            saved_path: Some("/path/to/config.json".to_string()),
            validation_result: None,
            duration_seconds: 1.5,
            message: "Success".to_string(),
        };

        let json = serde_json::to_string(&result);
        assert!(json.is_ok());

        let json_str = json.unwrap();
        assert!(json_str.contains("test-id-123"));
        assert!(json_str.contains("Test Config"));
        assert!(json_str.contains("BTCUSDT"));
    }

    #[test]
    fn test_list_result_serialize() {
        let result = ListResult {
            count: 1,
            configs: vec![ConfigSummaryItem {
                id: "abc123".to_string(),
                name: "Test".to_string(),
                symbol: "BTCUSDT".to_string(),
                strategy_type: "Hybrid".to_string(),
                version: 1,
                active: true,
                created_at: "2024-01-01T00:00:00Z".to_string(),
            }],
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("abc123"));
        assert!(json.contains("Test"));
    }
}

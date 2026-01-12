//! Algorithm Command Parameters
//!
//! This module defines parameter structs and builders for all algorithm commands.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};

use crate::core::StrategyType;

/// Parameters for the `create` command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateParams {
    /// Path to research store directory
    pub research: PathBuf,
    /// Path to config store directory for saving
    pub output: PathBuf,
    /// Trading symbol to load research for
    pub symbol: String,
    /// Custom name for the algorithm config
    pub name: Option<String>,
    /// Override strategy type
    pub strategy: Option<StrategyType>,
    /// Run validation pipeline after creation
    pub validate: bool,
    /// Path to data directory (required if validate is true)
    pub data: PathBuf,
    /// Validation stages to run (comma-separated)
    pub stages: String,
    /// Dry run - show what would be created without saving
    pub dry_run: bool,
}

/// Parameters for the `list` command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListParams {
    /// Path to config store directory
    pub store: PathBuf,
    /// Filter by symbol
    pub symbol: Option<String>,
    /// Filter by strategy type
    pub strategy: Option<StrategyType>,
    /// Filter by name (partial match)
    pub name: Option<String>,
    /// Show only active configs
    pub active_only: bool,
    /// Maximum number of configs to show
    pub limit: usize,
}

/// Parameters for the `show` command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShowParams {
    /// Path to config store directory
    pub store: PathBuf,
    /// Config ID to show (partial match supported)
    pub id: String,
    /// Show verbose details
    pub verbose: bool,
}

/// Builder for CreateParams
pub struct CreateParamsBuilder {
    research: Option<PathBuf>,
    output: Option<PathBuf>,
    symbol: Option<String>,
    name: Option<String>,
    strategy: Option<StrategyType>,
    validate: bool,
    data: Option<PathBuf>,
    stages: Option<String>,
    dry_run: bool,
}

impl CreateParamsBuilder {
    pub fn new() -> Self {
        Self {
            research: None,
            output: None,
            symbol: None,
            name: None,
            strategy: None,
            validate: false,
            data: None,
            stages: None,
            dry_run: false,
        }
    }

    pub fn with_research(mut self, research: PathBuf) -> Self {
        self.research = Some(research);
        self
    }

    pub fn with_output(mut self, output: PathBuf) -> Self {
        self.output = Some(output);
        self
    }

    pub fn with_symbol(mut self, symbol: String) -> Self {
        self.symbol = Some(symbol);
        self
    }

    pub fn with_name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    pub fn with_strategy(mut self, strategy: Option<StrategyType>) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_validate(mut self, validate: bool) -> Self {
        self.validate = validate;
        self
    }

    pub fn with_data(mut self, data: PathBuf) -> Self {
        self.data = Some(data);
        self
    }

    pub fn with_stages(mut self, stages: String) -> Self {
        self.stages = Some(stages);
        self
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    pub fn build(self) -> Result<CreateParams> {
        let research = self.research.ok_or_else(|| anyhow::anyhow!("research path is required"))?;
        let output = self.output.ok_or_else(|| anyhow::anyhow!("output path is required"))?;
        let symbol = self.symbol.ok_or_else(|| anyhow::anyhow!("symbol is required"))?;
        let data = self.data.unwrap_or_else(|| PathBuf::from("./data/features"));
        let stages = self.stages.unwrap_or_else(|| "backtest".to_string());

        // Validate symbol
        if symbol.is_empty() {
            anyhow::bail!("Symbol cannot be empty");
        }
        if symbol.len() > 20 {
            anyhow::bail!("Symbol too long: {}", symbol);
        }

        // Validate name if provided
        if let Some(ref name) = self.name {
            if name.is_empty() {
                anyhow::bail!("Name cannot be empty");
            }
            if name.len() > 100 {
                anyhow::bail!("Name too long (max 100 chars)");
            }
        }

        // If validation requested, check data directory
        if self.validate && !data.exists() {
            anyhow::bail!(
                "Data directory does not exist (required for validation): {:?}",
                data
            );
        }

        // Check research directory exists
        if !research.exists() {
            anyhow::bail!("Research store directory does not exist: {:?}", research);
        }

        Ok(CreateParams {
            research,
            output,
            symbol,
            name: self.name,
            strategy: self.strategy,
            validate: self.validate,
            data,
            stages,
            dry_run: self.dry_run,
        })
    }
}

impl Default for CreateParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for ListParams
pub struct ListParamsBuilder {
    store: Option<PathBuf>,
    symbol: Option<String>,
    strategy: Option<StrategyType>,
    name: Option<String>,
    active_only: bool,
    limit: Option<usize>,
}

impl ListParamsBuilder {
    pub fn new() -> Self {
        Self {
            store: None,
            symbol: None,
            strategy: None,
            name: None,
            active_only: false,
            limit: None,
        }
    }

    pub fn with_store(mut self, store: PathBuf) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_symbol(mut self, symbol: Option<String>) -> Self {
        self.symbol = symbol;
        self
    }

    pub fn with_strategy(mut self, strategy: Option<StrategyType>) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    pub fn with_active_only(mut self, active_only: bool) -> Self {
        self.active_only = active_only;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn build(self) -> Result<ListParams> {
        let store = self.store.unwrap_or_else(|| PathBuf::from("./data/configs"));
        let limit = self.limit.unwrap_or(20);

        // Validate limit
        if limit == 0 {
            anyhow::bail!("Limit must be greater than 0");
        }
        if limit > 1000 {
            anyhow::bail!("Limit too large (max 1000)");
        }

        Ok(ListParams {
            store,
            symbol: self.symbol,
            strategy: self.strategy,
            name: self.name,
            active_only: self.active_only,
            limit,
        })
    }
}

impl Default for ListParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for ShowParams
pub struct ShowParamsBuilder {
    store: Option<PathBuf>,
    id: Option<String>,
    verbose: bool,
}

impl ShowParamsBuilder {
    pub fn new() -> Self {
        Self {
            store: None,
            id: None,
            verbose: false,
        }
    }

    pub fn with_store(mut self, store: PathBuf) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub fn build(self) -> Result<ShowParams> {
        let store = self.store.unwrap_or_else(|| PathBuf::from("./data/configs"));
        let id = self.id.ok_or_else(|| anyhow::anyhow!("id is required"))?;

        // Validate id
        if id.is_empty() {
            anyhow::bail!("ID cannot be empty");
        }

        // Check store directory exists
        if !store.exists() {
            anyhow::bail!("Config store directory does not exist: {:?}", store);
        }

        Ok(ShowParams {
            store,
            id,
            verbose: self.verbose,
        })
    }
}

impl Default for ShowParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_params_builder_success() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        std::fs::create_dir_all(&research_dir).unwrap();

        let params = CreateParamsBuilder::new()
            .with_research(research_dir)
            .with_output(temp_dir.path().join("configs"))
            .with_symbol("BTCUSDT".to_string())
            .build();

        assert!(params.is_ok());
    }

    #[test]
    fn test_create_params_builder_missing_research() {
        let params = CreateParamsBuilder::new()
            .with_output(PathBuf::from("./configs"))
            .with_symbol("BTCUSDT".to_string())
            .build();

        assert!(params.is_err());
    }

    #[test]
    fn test_create_params_builder_empty_symbol() {
        let temp_dir = TempDir::new().unwrap();
        let params = CreateParamsBuilder::new()
            .with_research(temp_dir.path().to_path_buf())
            .with_output(PathBuf::from("./configs"))
            .with_symbol("".to_string())
            .build();

        assert!(params.is_err());
    }

    #[test]
    fn test_list_params_builder_success() {
        let params = ListParamsBuilder::new()
            .with_store(PathBuf::from("./configs"))
            .with_limit(10)
            .build();

        assert!(params.is_ok());
    }

    #[test]
    fn test_list_params_builder_default_limit() {
        let params = ListParamsBuilder::new()
            .with_store(PathBuf::from("./configs"))
            .build();

        assert!(params.is_ok());
        assert_eq!(params.unwrap().limit, 20);
    }

    #[test]
    fn test_show_params_builder_success() {
        let temp_dir = TempDir::new().unwrap();
        let store_dir = temp_dir.path().join("configs");
        std::fs::create_dir_all(&store_dir).unwrap();

        let params = ShowParamsBuilder::new()
            .with_store(store_dir)
            .with_id("test-id".to_string())
            .build();

        assert!(params.is_ok());
    }

    #[test]
    fn test_show_params_builder_empty_id() {
        let temp_dir = TempDir::new().unwrap();
        let params = ShowParamsBuilder::new()
            .with_store(temp_dir.path().to_path_buf())
            .with_id("".to_string())
            .build();

        assert!(params.is_err());
    }
}

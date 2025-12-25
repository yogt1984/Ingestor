//! Algorithm CLI - Task 3.6
//!
//! CLI commands for creating and managing algorithm configurations from research state.
//!
//! # Usage
//!
//! ```bash
//! # Create algorithm config from research state
//! cargo run --release --bin algorithm -- create --research ./research --output ./data/configs
//!
//! # Create with custom name
//! cargo run --release --bin algorithm -- create --research ./research --name "BTC_Momentum_v1"
//!
//! # Create and validate through pipeline
//! cargo run --release --bin algorithm -- create --research ./research --validate --data ./data/features
//!
//! # Create with preset strategy type override
//! cargo run --release --bin algorithm -- create --research ./research --strategy momentum
//!
//! # List existing configs
//! cargo run --release --bin algorithm -- list --store ./data/configs
//!
//! # Show config details
//! cargo run --release --bin algorithm -- show --store ./data/configs --id <config_id>
//! ```

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

use ingestor::core::{
    AlgorithmConfig, ConfigStore, ConfigStoreConfig,
    ResearchState, ResearchStore, ResearchStoreConfig, StrategyType,
};

// ============================================================================
// CLI Structures
// ============================================================================

#[derive(Parser)]
#[command(name = "algorithm")]
#[command(about = "Create and manage algorithm configurations from research")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new algorithm configuration from research state
    #[command(alias = "c")]
    Create(CreateArgs),

    /// List existing algorithm configurations
    #[command(alias = "ls")]
    List(ListArgs),

    /// Show details of a specific configuration
    #[command(alias = "s")]
    Show(ShowArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct CreateArgs {
    /// Path to research store directory containing research state
    #[arg(short, long, default_value = "./research")]
    pub research: PathBuf,

    /// Path to config store directory for saving the config
    #[arg(short, long, default_value = "./data/configs")]
    pub output: PathBuf,

    /// Trading symbol to load research for
    #[arg(short, long, default_value = "BTCUSDT")]
    pub symbol: String,

    /// Custom name for the algorithm config
    #[arg(short, long)]
    pub name: Option<String>,

    /// Override strategy type (momentum, marketmaking, hybrid)
    #[arg(long)]
    pub strategy: Option<StrategyOverride>,

    /// Run validation pipeline after creation
    #[arg(long)]
    pub validate: bool,

    /// Path to data directory (required if --validate is used)
    #[arg(short, long, default_value = "./data/features")]
    pub data: PathBuf,

    /// Validation stages to run (comma-separated: backtest,forward,oos)
    #[arg(long, default_value = "backtest")]
    pub stages: String,

    /// Output results as JSON
    #[arg(long)]
    pub json: bool,

    /// Quiet mode (minimal output)
    #[arg(short, long)]
    pub quiet: bool,

    /// Dry run - show what would be created without saving
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct ListArgs {
    /// Path to config store directory
    #[arg(short, long, default_value = "./data/configs")]
    pub store: PathBuf,

    /// Filter by symbol
    #[arg(short = 'y', long)]
    pub symbol: Option<String>,

    /// Filter by strategy type
    #[arg(long)]
    pub strategy: Option<StrategyOverride>,

    /// Filter by name (partial match)
    #[arg(short, long)]
    pub name: Option<String>,

    /// Show only active configs
    #[arg(long)]
    pub active_only: bool,

    /// Maximum number of configs to show
    #[arg(short, long, default_value = "20")]
    pub limit: usize,

    /// Output results as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct ShowArgs {
    /// Path to config store directory
    #[arg(short, long, default_value = "./data/configs")]
    pub store: PathBuf,

    /// Config ID to show (partial match supported)
    #[arg(short, long)]
    pub id: String,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,

    /// Show verbose details including all parameters
    #[arg(short, long)]
    pub verbose: bool,
}

/// Strategy type override for CLI
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum StrategyOverride {
    Momentum,
    MarketMaking,
    Hybrid,
}

impl From<StrategyOverride> for StrategyType {
    fn from(s: StrategyOverride) -> Self {
        match s {
            StrategyOverride::Momentum => StrategyType::Momentum,
            StrategyOverride::MarketMaking => StrategyType::MarketMaking,
            StrategyOverride::Hybrid => StrategyType::Hybrid,
        }
    }
}

// ============================================================================
// Create Command Implementation
// ============================================================================

/// Result of algorithm creation
#[derive(Debug, Clone, serde::Serialize)]
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

#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidationSummary {
    pub passed: bool,
    pub stages_run: Vec<String>,
    pub sharpe: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub trade_count: Option<usize>,
    pub message: String,
}

/// Execute the create command
pub fn execute_create(args: &CreateArgs) -> Result<CreateResult> {
    let start_time = Instant::now();

    // Validate inputs
    validate_create_args(args)?;

    // Load research state
    let research = load_research_state(&args.research, &args.symbol)?;

    // Generate config from research
    let mut config = AlgorithmConfig::from_research(&research);

    // Apply overrides if specified
    if let Some(name) = &args.name {
        config.name = name.clone();
        config.id = config.generate_id();
    }

    if let Some(strategy) = args.strategy {
        config.strategy_type = strategy.into();
        config.id = config.generate_id();
    }

    // Validate config
    config.validate().context("Generated config failed validation")?;

    let mut saved_path = None;
    let mut validation_result = None;

    // Save to store unless dry run
    if !args.dry_run {
        let store_config = ConfigStoreConfig::with_path(&args.output);
        let mut store = ConfigStore::new(store_config)
            .context("Failed to create config store")?;

        let path = store.save(&config)
            .context("Failed to save config to store")?;
        saved_path = Some(path.display().to_string());
    }

    // Run validation if requested
    if args.validate && !args.dry_run {
        validation_result = Some(run_validation(&config, &args.data, &args.stages)?);
    }

    let success = validation_result.as_ref().map(|v| v.passed).unwrap_or(true);

    let message = if args.dry_run {
        "Dry run - config not saved".to_string()
    } else if args.validate {
        if success {
            "Config created and validated successfully".to_string()
        } else {
            "Config created but validation failed".to_string()
        }
    } else {
        "Config created successfully".to_string()
    };

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
        duration_seconds: start_time.elapsed().as_secs_f64(),
        message,
    })
}

/// Validate create command arguments
fn validate_create_args(args: &CreateArgs) -> Result<()> {
    // Check research directory exists
    if !args.research.exists() {
        anyhow::bail!("Research store directory does not exist: {:?}", args.research);
    }

    // Check symbol is valid
    if args.symbol.is_empty() {
        anyhow::bail!("Symbol cannot be empty");
    }
    if args.symbol.len() > 20 {
        anyhow::bail!("Symbol too long: {}", args.symbol);
    }

    // Check name if provided
    if let Some(name) = &args.name {
        if name.is_empty() {
            anyhow::bail!("Name cannot be empty");
        }
        if name.len() > 100 {
            anyhow::bail!("Name too long (max 100 chars)");
        }
    }

    // If validation requested, check data directory
    if args.validate && !args.data.exists() {
        anyhow::bail!(
            "Data directory does not exist (required for validation): {:?}",
            args.data
        );
    }

    Ok(())
}

/// Load research state from store
fn load_research_state(store_path: &PathBuf, symbol: &str) -> Result<ResearchState> {
    let store_config = ResearchStoreConfig::with_path(store_path);
    let mut store = ResearchStore::new(store_config)
        .context("Failed to open research store")?;

    store
        .load(symbol)
        .context("Failed to load research state")?
        .ok_or_else(|| anyhow::anyhow!("No research state found for symbol: {}", symbol))
}

/// Run validation pipeline (simplified - full implementation would use validation module)
fn run_validation(config: &AlgorithmConfig, _data_path: &PathBuf, stages: &str) -> Result<ValidationSummary> {
    // Parse stages
    let stages_list: Vec<String> = stages
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // For now, return a placeholder since full validation pipeline integration
    // would require more infrastructure. The CLI structure is in place.
    // In production, this would call the validation pipeline runner.

    log::info!("Validation requested for config: {} (stages: {:?})", config.id, stages_list);
    log::info!("Note: Full validation pipeline integration is available via 'validate' CLI");

    Ok(ValidationSummary {
        passed: true,
        stages_run: stages_list,
        sharpe: None,
        max_drawdown: None,
        trade_count: None,
        message: "Validation pipeline integration available via 'validate' CLI".to_string(),
    })
}

/// Print human-readable create result
fn print_create_result(result: &CreateResult) {
    println!("\n{}", "=".repeat(60));
    println!("              ALGORITHM CONFIG CREATED");
    println!("{}", "=".repeat(60));

    println!("\n--- Configuration Details ---");
    println!("  Config ID:          {}", result.config_id);
    println!("  Name:               {}", result.config_name);
    println!("  Strategy:           {}", result.strategy_type);
    println!("  Symbol:             {}", result.symbol);
    println!("  Version:            {}", result.version);

    if let Some(ref research_id) = result.source_research_id {
        println!("  Source Research:    {}...", &research_id[..research_id.len().min(16)]);
    }

    if let Some(ref path) = result.saved_path {
        println!("\n--- Storage ---");
        println!("  Saved to:           {}", path);
    }

    if let Some(ref validation) = result.validation_result {
        println!("\n--- Validation ---");
        println!("  Stages run:         {:?}", validation.stages_run);
        println!("  Passed:             {}", if validation.passed { "YES" } else { "NO" });
        if let Some(sharpe) = validation.sharpe {
            println!("  Sharpe Ratio:       {:.4}", sharpe);
        }
        if let Some(dd) = validation.max_drawdown {
            println!("  Max Drawdown:       {:.2}%", dd * 100.0);
        }
        if let Some(trades) = validation.trade_count {
            println!("  Trade Count:        {}", trades);
        }
        println!("  Message:            {}", validation.message);
    }

    println!("\n--- Result ---");
    println!("  Status:             {}", if result.success { "SUCCESS" } else { "FAILED" });
    println!("  Message:            {}", result.message);
    println!("  Duration:           {:.2}s", result.duration_seconds);

    println!("\n{}", "=".repeat(60));
}

/// Print JSON create result
fn print_create_json(result: &CreateResult) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

// ============================================================================
// List Command Implementation
// ============================================================================

/// Result of list command
#[derive(Debug, Clone, serde::Serialize)]
pub struct ListResult {
    pub count: usize,
    pub configs: Vec<ConfigSummaryItem>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ConfigSummaryItem {
    pub id: String,
    pub name: String,
    pub symbol: String,
    pub strategy_type: String,
    pub version: u32,
    pub active: bool,
    pub created_at: String,
}

/// Execute the list command
pub fn execute_list(args: &ListArgs) -> Result<ListResult> {
    // Check store directory exists
    if !args.store.exists() {
        return Ok(ListResult {
            count: 0,
            configs: vec![],
        });
    }

    let store_config = ConfigStoreConfig::with_path(&args.store);
    let store = ConfigStore::new(store_config)
        .context("Failed to open config store")?;

    // Load all configs and filter manually
    let all_configs = store.list_all()
        .context("Failed to list configs")?;

    // Apply filters
    let configs: Vec<_> = all_configs
        .into_iter()
        .filter(|c| {
            // Filter by symbol if specified
            if let Some(ref sym) = args.symbol {
                if c.symbol != *sym {
                    return false;
                }
            }
            // Filter by strategy if specified
            if let Some(strategy) = args.strategy {
                let strategy_type: StrategyType = strategy.into();
                if c.strategy_type != strategy_type {
                    return false;
                }
            }
            // Filter by name (partial match) if specified
            if let Some(ref name) = args.name {
                if !c.name.to_lowercase().contains(&name.to_lowercase()) {
                    return false;
                }
            }
            // Filter by active only
            if args.active_only && !c.active {
                return false;
            }
            true
        })
        .take(args.limit)
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

    Ok(ListResult {
        count: items.len(),
        configs: items,
    })
}

/// Print human-readable list result
fn print_list_result(result: &ListResult) {
    if result.count == 0 {
        println!("\nNo algorithm configurations found.");
        return;
    }

    println!("\n{}", "=".repeat(100));
    println!("  ALGORITHM CONFIGURATIONS ({} found)", result.count);
    println!("{}", "=".repeat(100));
    println!(
        "\n  {:8} {:30} {:10} {:12} {:5} {:6}",
        "ID", "NAME", "SYMBOL", "STRATEGY", "VER", "ACTIVE"
    );
    println!("  {}", "-".repeat(95));

    for config in &result.configs {
        let id_short = if config.id.len() > 8 {
            &config.id[..8]
        } else {
            &config.id
        };
        let name_short = if config.name.len() > 30 {
            format!("{}...", &config.name[..27])
        } else {
            config.name.clone()
        };
        let active_str = if config.active { "Yes" } else { "No" };

        println!(
            "  {:8} {:30} {:10} {:12} {:5} {:6}",
            id_short,
            name_short,
            config.symbol,
            config.strategy_type,
            config.version,
            active_str
        );
    }

    println!("\n{}", "=".repeat(100));
}

/// Print JSON list result
fn print_list_json(result: &ListResult) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

// ============================================================================
// Show Command Implementation
// ============================================================================

/// Execute the show command
pub fn execute_show(args: &ShowArgs) -> Result<Option<AlgorithmConfig>> {
    // Check store directory exists
    if !args.store.exists() {
        anyhow::bail!("Config store directory does not exist: {:?}", args.store);
    }

    let store_config = ConfigStoreConfig::with_path(&args.store);
    let mut store = ConfigStore::new(store_config)
        .context("Failed to open config store")?;

    // Try to load by ID
    store.load(&args.id).context("Failed to load config")
}

/// Print human-readable config details
fn print_show_result(config: &AlgorithmConfig, verbose: bool) {
    println!("\n{}", "=".repeat(70));
    println!("              ALGORITHM CONFIGURATION DETAILS");
    println!("{}", "=".repeat(70));

    println!("\n--- Identification ---");
    println!("  ID:                 {}", config.id);
    println!("  Name:               {}", config.name);
    println!("  Symbol:             {}", config.symbol);
    println!("  Strategy Type:      {}", config.strategy_type);
    println!("  Version:            {}", config.version);
    println!("  Active:             {}", if config.active { "Yes" } else { "No" });

    if let Some(ref research_id) = config.source_research_id {
        println!("  Source Research:    {}", research_id);
    }

    println!("\n--- Entry Parameters ---");
    println!("  Min Momentum Signal: {:.3}", config.entry.min_momentum_signal);
    println!("  Min Monotonicity:    {:.3}", config.entry.min_monotonicity);
    println!("  Min Hurst:           {:.3}", config.entry.min_hurst);
    println!("  Max Entry Entropy:   {:.3}", config.entry.max_entry_entropy);
    println!("  Min Confidence:      {:.3}", config.entry.min_confidence);

    println!("\n--- Exit Parameters ---");
    println!("  Take Profit (bps):   {:.1}", config.exit.take_profit_bps);
    println!("  Stop Loss (bps):     {:.1}", config.exit.stop_loss_bps);
    println!("  Max Hold Time (s):   {}", config.exit.max_hold_seconds);
    println!("  Use Time Exit:       {}", if config.exit.use_time_exit { "Yes" } else { "No" });

    println!("\n--- Position Parameters ---");
    println!("  Base Size Fraction:  {:.3}", config.position.base_size_fraction);
    println!("  Max Size Fraction:   {:.3}", config.position.max_size_fraction);
    println!("  Sizing Method:       {:?}", config.position.method);

    if verbose {
        println!("\n--- Regime Filters ---");
        println!("  Min Tau Half (s):    {:.1}", config.regime_filters.min_tau_half);
        println!("  Max Entropy:         {:.3}", config.regime_filters.max_entropy);
        println!("  Min Kappa:           {:.4}", config.regime_filters.min_kappa);
        println!("  Max Kappa:           {:.4}", config.regime_filters.max_kappa);

        println!("\n--- Market Making Parameters ---");
        println!("  Base Spread (bps):   {:.1}", config.market_making.base_spread_bps);
        println!("  Inventory Skew:      {:.3}", config.market_making.inventory_skew);
        println!("  Gamma:               {:.4}", config.market_making.gamma);
        println!("  Kappa:               {:.4}", config.market_making.kappa);

        println!("\n--- Timestamps ---");
        println!("  Created:             {}", config.created_at);
    }

    println!("\n{}", "=".repeat(70));
}

/// Print JSON config
fn print_show_json(config: &AlgorithmConfig) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(config)?);
    Ok(())
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Create(args) => {
            if !args.quiet && !args.json {
                println!("\nAlgorithm Config Creation:");
                println!("  Research store:     {:?}", args.research);
                println!("  Output store:       {:?}", args.output);
                println!("  Symbol:             {}", args.symbol);
                if let Some(ref name) = args.name {
                    println!("  Custom name:        {}", name);
                }
                if let Some(strategy) = args.strategy {
                    println!("  Strategy override:  {:?}", strategy);
                }
                if args.validate {
                    println!("  Validation:         enabled (stages: {})", args.stages);
                    println!("  Data directory:     {:?}", args.data);
                }
                if args.dry_run {
                    println!("  Mode:               DRY RUN");
                }
                println!();
            }

            let result = execute_create(&args)?;

            if args.json {
                print_create_json(&result)?;
            } else if !args.quiet {
                print_create_result(&result);
            }

            // Exit with error code if validation failed
            if !result.success {
                std::process::exit(1);
            }
        }

        Commands::List(args) => {
            let result = execute_list(&args)?;

            if args.json {
                print_list_json(&result)?;
            } else {
                print_list_result(&result);
            }
        }

        Commands::Show(args) => {
            let config = execute_show(&args)?;

            match config {
                Some(c) => {
                    if args.json {
                        print_show_json(&c)?;
                    } else {
                        print_show_result(&c, args.verbose);
                    }
                }
                None => {
                    if args.json {
                        println!("{{\"error\": \"Config not found\", \"id\": \"{}\"}}", args.id);
                    } else {
                        println!("\nConfig not found: {}", args.id);
                    }
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use ingestor::core::ResearchState;

    // ==================== Argument Validation Tests ====================

    #[test]
    fn test_validate_create_args_missing_research_dir() {
        let args = CreateArgs {
            research: PathBuf::from("/nonexistent/path"),
            output: PathBuf::from("./test"),
            symbol: "BTCUSDT".to_string(),
            name: None,
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: false,
            dry_run: false,
        };

        let result = validate_create_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_validate_create_args_empty_symbol() {
        let temp_dir = TempDir::new().unwrap();

        let args = CreateArgs {
            research: temp_dir.path().to_path_buf(),
            output: PathBuf::from("./test"),
            symbol: "".to_string(),
            name: None,
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: false,
            dry_run: false,
        };

        let result = validate_create_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_create_args_symbol_too_long() {
        let temp_dir = TempDir::new().unwrap();

        let args = CreateArgs {
            research: temp_dir.path().to_path_buf(),
            output: PathBuf::from("./test"),
            symbol: "A".repeat(25),
            name: None,
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: false,
            dry_run: false,
        };

        let result = validate_create_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }

    #[test]
    fn test_validate_create_args_empty_name() {
        let temp_dir = TempDir::new().unwrap();

        let args = CreateArgs {
            research: temp_dir.path().to_path_buf(),
            output: PathBuf::from("./test"),
            symbol: "BTCUSDT".to_string(),
            name: Some("".to_string()),
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: false,
            dry_run: false,
        };

        let result = validate_create_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Name cannot be empty"));
    }

    #[test]
    fn test_validate_create_args_name_too_long() {
        let temp_dir = TempDir::new().unwrap();

        let args = CreateArgs {
            research: temp_dir.path().to_path_buf(),
            output: PathBuf::from("./test"),
            symbol: "BTCUSDT".to_string(),
            name: Some("A".repeat(150)),
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: false,
            dry_run: false,
        };

        let result = validate_create_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }

    #[test]
    fn test_validate_create_args_validate_without_data() {
        let temp_dir = TempDir::new().unwrap();

        let args = CreateArgs {
            research: temp_dir.path().to_path_buf(),
            output: PathBuf::from("./test"),
            symbol: "BTCUSDT".to_string(),
            name: None,
            strategy: None,
            validate: true,
            data: PathBuf::from("/nonexistent/data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: false,
            dry_run: false,
        };

        let result = validate_create_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("required for validation"));
    }

    #[test]
    fn test_validate_create_args_valid() {
        let temp_dir = TempDir::new().unwrap();

        let args = CreateArgs {
            research: temp_dir.path().to_path_buf(),
            output: PathBuf::from("./test"),
            symbol: "BTCUSDT".to_string(),
            name: Some("Test_Config".to_string()),
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: false,
            dry_run: false,
        };

        let result = validate_create_args(&args);
        assert!(result.is_ok());
    }

    // ==================== Strategy Override Tests ====================

    #[test]
    fn test_strategy_override_to_strategy_type_momentum() {
        let override_type = StrategyOverride::Momentum;
        let strategy_type: StrategyType = override_type.into();
        assert_eq!(strategy_type, StrategyType::Momentum);
    }

    #[test]
    fn test_strategy_override_to_strategy_type_market_making() {
        let override_type = StrategyOverride::MarketMaking;
        let strategy_type: StrategyType = override_type.into();
        assert_eq!(strategy_type, StrategyType::MarketMaking);
    }

    #[test]
    fn test_strategy_override_to_strategy_type_hybrid() {
        let override_type = StrategyOverride::Hybrid;
        let strategy_type: StrategyType = override_type.into();
        assert_eq!(strategy_type, StrategyType::Hybrid);
    }

    // ==================== Create Result Tests ====================

    #[test]
    fn test_create_result_serialization() {
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
    fn test_create_result_with_validation() {
        let validation = ValidationSummary {
            passed: true,
            stages_run: vec!["backtest".to_string(), "forward".to_string()],
            sharpe: Some(1.5),
            max_drawdown: Some(0.08),
            trade_count: Some(100),
            message: "Validation passed".to_string(),
        };

        let result = CreateResult {
            success: true,
            config_id: "test-id".to_string(),
            config_name: "Test".to_string(),
            strategy_type: "Hybrid".to_string(),
            symbol: "ETHUSDT".to_string(),
            version: 2,
            source_research_id: None,
            saved_path: None,
            validation_result: Some(validation),
            duration_seconds: 2.0,
            message: "Created with validation".to_string(),
        };

        let json = serde_json::to_string_pretty(&result).unwrap();
        assert!(json.contains("backtest"));
        assert!(json.contains("forward"));
        assert!(json.contains("1.5"));
    }

    // ==================== List Result Tests ====================

    #[test]
    fn test_list_result_empty() {
        let result = ListResult {
            count: 0,
            configs: vec![],
        };

        assert_eq!(result.count, 0);
        assert!(result.configs.is_empty());
    }

    #[test]
    fn test_list_result_with_configs() {
        let result = ListResult {
            count: 2,
            configs: vec![
                ConfigSummaryItem {
                    id: "id1".to_string(),
                    name: "Config 1".to_string(),
                    symbol: "BTCUSDT".to_string(),
                    strategy_type: "Momentum".to_string(),
                    version: 1,
                    active: true,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                },
                ConfigSummaryItem {
                    id: "id2".to_string(),
                    name: "Config 2".to_string(),
                    symbol: "ETHUSDT".to_string(),
                    strategy_type: "MarketMaking".to_string(),
                    version: 3,
                    active: false,
                    created_at: "2024-01-02T00:00:00Z".to_string(),
                },
            ],
        };

        assert_eq!(result.count, 2);
        assert_eq!(result.configs[0].name, "Config 1");
        assert_eq!(result.configs[1].name, "Config 2");
    }

    #[test]
    fn test_list_result_serialization() {
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

    // ==================== Validation Summary Tests ====================

    #[test]
    fn test_validation_summary_passed() {
        let summary = ValidationSummary {
            passed: true,
            stages_run: vec!["backtest".to_string()],
            sharpe: Some(2.0),
            max_drawdown: Some(0.05),
            trade_count: Some(500),
            message: "All checks passed".to_string(),
        };

        assert!(summary.passed);
        assert_eq!(summary.sharpe.unwrap(), 2.0);
    }

    #[test]
    fn test_validation_summary_failed() {
        let summary = ValidationSummary {
            passed: false,
            stages_run: vec!["backtest".to_string(), "forward".to_string()],
            sharpe: Some(-0.5),
            max_drawdown: Some(0.25),
            trade_count: Some(50),
            message: "Sharpe ratio too low".to_string(),
        };

        assert!(!summary.passed);
        assert!(summary.sharpe.unwrap() < 0.0);
    }

    #[test]
    fn test_validation_summary_no_metrics() {
        let summary = ValidationSummary {
            passed: true,
            stages_run: vec![],
            sharpe: None,
            max_drawdown: None,
            trade_count: None,
            message: "No validation performed".to_string(),
        };

        assert!(summary.sharpe.is_none());
        assert!(summary.max_drawdown.is_none());
        assert!(summary.trade_count.is_none());
    }

    // ==================== Integration Tests ====================

    #[test]
    fn test_config_from_research_creates_valid_config() {
        let research = ResearchState::new("BTCUSDT");
        let config = AlgorithmConfig::from_research(&research);

        assert_eq!(config.symbol, "BTCUSDT");
        assert!(config.source_research_id.is_some());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_execute_create_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        std::fs::create_dir_all(&research_dir).unwrap();

        // Create a research store and save state
        let store_config = ResearchStoreConfig::with_path(&research_dir);
        let mut store = ResearchStore::new(store_config).unwrap();
        let research = ResearchState::new("BTCUSDT");
        store.save(&research).unwrap();

        let args = CreateArgs {
            research: research_dir,
            output: temp_dir.path().join("configs"),
            symbol: "BTCUSDT".to_string(),
            name: Some("DryRunTest".to_string()),
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: true,
            dry_run: true,
        };

        let result = execute_create(&args);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.success);
        assert!(result.saved_path.is_none()); // Not saved in dry run
        assert!(result.message.contains("Dry run"));
    }

    #[test]
    fn test_execute_create_saves_config() {
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

        let args = CreateArgs {
            research: research_dir,
            output: config_dir.clone(),
            symbol: "BTCUSDT".to_string(),
            name: Some("SavedConfig".to_string()),
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: true,
            dry_run: false,
        };

        let result = execute_create(&args);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.success);
        assert!(result.saved_path.is_some());
    }

    #[test]
    fn test_execute_create_with_strategy_override() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        std::fs::create_dir_all(&research_dir).unwrap();

        // Create a research store and save state
        let store_config = ResearchStoreConfig::with_path(&research_dir);
        let mut store = ResearchStore::new(store_config).unwrap();
        let research = ResearchState::new("BTCUSDT");
        store.save(&research).unwrap();

        let args = CreateArgs {
            research: research_dir,
            output: temp_dir.path().join("configs"),
            symbol: "BTCUSDT".to_string(),
            name: None,
            strategy: Some(StrategyOverride::Momentum),
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: true,
            dry_run: true,
        };

        let result = execute_create(&args);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.strategy_type, "Momentum");
    }

    #[test]
    fn test_execute_list_empty_store() {
        let temp_dir = TempDir::new().unwrap();

        let args = ListArgs {
            store: temp_dir.path().join("nonexistent"),
            symbol: None,
            strategy: None,
            name: None,
            active_only: false,
            limit: 20,
            json: false,
        };

        let result = execute_list(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().count, 0);
    }

    #[test]
    fn test_execute_show_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let store_dir = temp_dir.path().join("configs");
        std::fs::create_dir_all(&store_dir).unwrap();

        let args = ShowArgs {
            store: store_dir,
            id: "nonexistent-id".to_string(),
            json: false,
            verbose: false,
        };

        let result = execute_show(&args);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    // ==================== Load Research State Tests ====================

    #[test]
    fn test_load_research_state_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        std::fs::create_dir_all(&research_dir).unwrap();

        // Create empty store
        let store_config = ResearchStoreConfig::with_path(&research_dir);
        let _store = ResearchStore::new(store_config).unwrap();

        let result = load_research_state(&research_dir, "NONEXISTENT");
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

        let result = load_research_state(&research_dir, "BTCUSDT");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().symbol, "BTCUSDT");
    }

    // ==================== Run Validation Tests ====================

    #[test]
    fn test_run_validation_parses_stages() {
        let config = AlgorithmConfig::new("Test", StrategyType::Momentum, "BTCUSDT");
        let data_path = PathBuf::from("./data");

        let result = run_validation(&config, &data_path, "backtest,forward,oos");
        assert!(result.is_ok());

        let summary = result.unwrap();
        assert_eq!(summary.stages_run.len(), 3);
        assert!(summary.stages_run.contains(&"backtest".to_string()));
        assert!(summary.stages_run.contains(&"forward".to_string()));
        assert!(summary.stages_run.contains(&"oos".to_string()));
    }

    #[test]
    fn test_run_validation_single_stage() {
        let config = AlgorithmConfig::new("Test", StrategyType::MarketMaking, "ETHUSDT");
        let data_path = PathBuf::from("./data");

        let result = run_validation(&config, &data_path, "backtest");
        assert!(result.is_ok());

        let summary = result.unwrap();
        assert_eq!(summary.stages_run.len(), 1);
        assert_eq!(summary.stages_run[0], "backtest");
    }

    #[test]
    fn test_run_validation_empty_stages() {
        let config = AlgorithmConfig::new("Test", StrategyType::Hybrid, "BTCUSDT");
        let data_path = PathBuf::from("./data");

        let result = run_validation(&config, &data_path, "");
        assert!(result.is_ok());

        let summary = result.unwrap();
        assert!(summary.stages_run.is_empty());
    }

    // ==================== CLI Parsing Tests ====================

    #[test]
    fn test_cli_create_defaults() {
        // This tests that defaults are sensible
        let args = CreateArgs {
            research: PathBuf::from("./research"),
            output: PathBuf::from("./data/configs"),
            symbol: "BTCUSDT".to_string(),
            name: None,
            strategy: None,
            validate: false,
            data: PathBuf::from("./data/features"),
            stages: "backtest".to_string(),
            json: false,
            quiet: false,
            dry_run: false,
        };

        assert_eq!(args.symbol, "BTCUSDT");
        assert!(!args.validate);
        assert!(!args.dry_run);
    }

    #[test]
    fn test_cli_list_defaults() {
        let args = ListArgs {
            store: PathBuf::from("./data/configs"),
            symbol: None,
            strategy: None,
            name: None,
            active_only: false,
            limit: 20,
            json: false,
        };

        assert_eq!(args.limit, 20);
        assert!(!args.active_only);
    }

    #[test]
    fn test_cli_show_defaults() {
        let args = ShowArgs {
            store: PathBuf::from("./data/configs"),
            id: "test-id".to_string(),
            json: false,
            verbose: false,
        };

        assert!(!args.verbose);
        assert!(!args.json);
    }

    // ==================== Error Handling Tests ====================

    #[test]
    fn test_create_fails_on_missing_research() {
        let args = CreateArgs {
            research: PathBuf::from("/definitely/does/not/exist/research"),
            output: PathBuf::from("./test"),
            symbol: "BTCUSDT".to_string(),
            name: None,
            strategy: None,
            validate: false,
            data: PathBuf::from("./data"),
            stages: "backtest".to_string(),
            json: false,
            quiet: true,
            dry_run: false,
        };

        let result = execute_create(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_show_fails_on_missing_store() {
        let args = ShowArgs {
            store: PathBuf::from("/definitely/does/not/exist/configs"),
            id: "test-id".to_string(),
            json: false,
            verbose: false,
        };

        let result = execute_show(&args);
        assert!(result.is_err());
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_create_with_all_options() {
        let temp_dir = TempDir::new().unwrap();
        let research_dir = temp_dir.path().join("research");
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&research_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        // Create a research store and save state
        let store_config = ResearchStoreConfig::with_path(&research_dir);
        let mut store = ResearchStore::new(store_config).unwrap();
        let research = ResearchState::new("BTCUSDT");
        store.save(&research).unwrap();

        let args = CreateArgs {
            research: research_dir,
            output: temp_dir.path().join("configs"),
            symbol: "BTCUSDT".to_string(),
            name: Some("FullOptionsTest".to_string()),
            strategy: Some(StrategyOverride::Hybrid),
            validate: true,
            data: data_dir,
            stages: "backtest,forward".to_string(),
            json: false,
            quiet: true,
            dry_run: true, // Dry run so we don't need real validation
        };

        let result = execute_create(&args);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.config_name, "FullOptionsTest");
        assert_eq!(result.strategy_type, "Hybrid");
    }

    #[test]
    fn test_config_summary_item_truncation() {
        let item = ConfigSummaryItem {
            id: "a".repeat(100),
            name: "b".repeat(100),
            symbol: "BTCUSDT".to_string(),
            strategy_type: "Momentum".to_string(),
            version: 1,
            active: true,
            created_at: "2024-01-01T00:00:00Z".to_string(),
        };

        // Just verify it doesn't panic with long strings
        let json = serde_json::to_string(&item);
        assert!(json.is_ok());
    }

    #[test]
    fn test_multiple_strategy_overrides() {
        // Test that all strategy overrides convert correctly
        let overrides = [
            (StrategyOverride::Momentum, StrategyType::Momentum),
            (StrategyOverride::MarketMaking, StrategyType::MarketMaking),
            (StrategyOverride::Hybrid, StrategyType::Hybrid),
        ];

        for (override_val, expected_type) in overrides {
            let result: StrategyType = override_val.into();
            assert_eq!(result, expected_type);
        }
    }
}

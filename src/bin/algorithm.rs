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
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

use ingestor::commands::{
    AlgorithmCommands,
    common::NoOpCallback,
};
use ingestor::commands::params::algorithm_params::{
    CreateParamsBuilder,
    ListParamsBuilder,
    ShowParamsBuilder,
};
use ingestor::commands::algorithm::{
    CreateResult, ListResult, ShowResult,
};
use ingestor::core::StrategyType;

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
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
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

            let strategy_override = args.strategy.map(|s| s.into());
            let params = CreateParamsBuilder::new()
                .with_research(args.research.clone())
                .with_output(args.output.clone())
                .with_symbol(args.symbol.clone())
                .with_name(args.name.clone())
                .with_strategy(strategy_override)
                .with_validate(args.validate)
                .with_data(args.data.clone())
                .with_stages(args.stages.clone())
                .with_dry_run(args.dry_run)
                .build()?;

            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = AlgorithmCommands::create(params, callback).await?;

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
            let strategy_override = args.strategy.map(|s| s.into());
            let params = ListParamsBuilder::new()
                .with_store(args.store.clone())
                .with_symbol(args.symbol.clone())
                .with_strategy(strategy_override)
                .with_name(args.name.clone())
                .with_active_only(args.active_only)
                .with_limit(args.limit)
                .build()?;

            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = AlgorithmCommands::list(params, callback)?;

            if args.json {
                print_list_json(&result)?;
            } else {
                print_list_result(&result);
            }
        }

        Commands::Show(args) => {
            let params = ShowParamsBuilder::new()
                .with_store(args.store.clone())
                .with_id(args.id.clone())
                .with_verbose(args.verbose)
                .build()?;

            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = AlgorithmCommands::show(params, callback)?;

            if result.found {
                if args.json {
                    print_show_json(&result.config)?;
                } else {
                    print_show_result(&result.config, args.verbose);
                }
            } else {
                if args.json {
                    println!("{{\"error\": \"Config not found\", \"id\": \"{}\"}}", args.id);
                } else {
                    println!("\nConfig not found: {}", args.id);
                }
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

// ============================================================================
// Output Formatting Functions
// ============================================================================

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

fn print_create_json(result: &CreateResult) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

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

fn print_list_json(result: &ListResult) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

fn print_show_result(config: &ingestor::core::AlgorithmConfig, verbose: bool) {
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

fn print_show_json(config: &ingestor::core::AlgorithmConfig) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(config)?);
    Ok(())
}

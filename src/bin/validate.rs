//! Validate CLI (Task 2.7)
//!
//! CLI command to run the validation pipeline on algorithm configurations.
//!
//! # Usage
//!
//! ```bash
//! # Run full pipeline with config file
//! cargo run --release --bin validate -- --config ./configs/algo.json
//!
//! # Run specific stages
//! cargo run --release --bin validate -- --config ./configs/algo.json --stages backtest,forward,oos
//!
//! # Generate config from research and validate
//! cargo run --release --bin validate -- --from-research ./research/ --stages backtest,forward
//!
//! # Run from a specific stage (partial run)
//! cargo run --release --bin validate -- --config ./configs/algo.json --from forward
//!
//! # Show available presets
//! cargo run --release --bin validate -- presets
//!
//! # Run with a preset configuration
//! cargo run --release --bin validate -- --config ./configs/algo.json --preset research
//!
//! # Quiet mode (minimal output)
//! cargo run --release --bin validate -- --config ./configs/algo.json --quiet
//!
//! # Output results as JSON
//! cargo run --release --bin validate -- --config ./configs/algo.json --json
//! ```
//!
//! # Subcommands
//!
//! - `run` (default) - Run the validation pipeline
//! - `presets` - List available pipeline presets
//! - `stages` - List available validation stages
//! - `status` - Show status of previous runs

use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;

use ingestor::core::{
    AlgorithmConfig, ResearchStore, ResearchStoreConfig, ResultsStore, ResultsStoreConfig,
    ValidationStageType,
};
use ingestor::validation::{
    PipelineResult, PipelineRunner, PipelineStatus, RunnerConfig, StageOutcome,
};

// ============================================================================
// CLI Structure
// ============================================================================

#[derive(Parser)]
#[command(name = "validate")]
#[command(about = "Run validation pipeline on algorithm configurations")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to algorithm config file (JSON)
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Generate config from research state at this path
    #[arg(long)]
    from_research: Option<PathBuf>,

    /// Comma-separated list of stages to run (backtest,forward,oos,paper,live)
    #[arg(short, long, value_delimiter = ',')]
    stages: Option<Vec<StageArg>>,

    /// Start from this stage (for partial runs)
    #[arg(long)]
    from: Option<StageArg>,

    /// Path to data directory containing Parquet files
    #[arg(short, long, default_value = "./data/features")]
    data: PathBuf,

    /// Path to results directory for persistence
    #[arg(short, long, default_value = "./results")]
    results: PathBuf,

    /// Runner preset to use
    #[arg(long)]
    preset: Option<PresetArg>,

    /// Quiet mode (minimal output)
    #[arg(short, long)]
    quiet: bool,

    /// Output results as JSON
    #[arg(long)]
    json: bool,

    /// Save results to file
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Run name prefix for identification
    #[arg(long, default_value = "validate")]
    name: String,

    /// Continue on failure (don't stop on first failed stage)
    #[arg(long)]
    continue_on_failure: bool,

    /// Disable persistence (don't save results)
    #[arg(long)]
    no_persist: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the validation pipeline (default)
    Run,

    /// List available pipeline presets
    Presets,

    /// List available validation stages
    Stages,

    /// Show status of previous validation runs
    Status {
        /// Show last N runs
        #[arg(short, long, default_value = "10")]
        last: usize,
    },

    /// Show detailed info about a specific run
    Show {
        /// Run ID to show
        run_id: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StageArg {
    Backtest,
    Forward,
    Oos,
    Paper,
    Live,
}

impl From<StageArg> for ValidationStageType {
    fn from(arg: StageArg) -> Self {
        match arg {
            StageArg::Backtest => ValidationStageType::Backtest,
            StageArg::Forward => ValidationStageType::Forward,
            StageArg::Oos => ValidationStageType::OutOfSample,
            StageArg::Paper => ValidationStageType::Paper,
            StageArg::Live => ValidationStageType::Live,
        }
    }
}

impl std::fmt::Display for StageArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StageArg::Backtest => write!(f, "backtest"),
            StageArg::Forward => write!(f, "forward"),
            StageArg::Oos => write!(f, "oos"),
            StageArg::Paper => write!(f, "paper"),
            StageArg::Live => write!(f, "live"),
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PresetArg {
    Default,
    Production,
    Research,
    Fast,
}

// ============================================================================
// Output Structures
// ============================================================================

/// JSON output structure for pipeline results
#[derive(Debug, Serialize)]
struct JsonOutput {
    success: bool,
    pipeline_id: String,
    algorithm_id: String,
    status: String,
    stages_passed: usize,
    stages_failed: usize,
    stages_skipped: usize,
    duration_seconds: f64,
    timestamp: String,
    stage_results: Vec<StageResultJson>,
    recommendation: String,
}

#[derive(Debug, Serialize)]
struct StageResultJson {
    stage: String,
    outcome: String,
    passed: Option<bool>,
    sharpe: Option<f64>,
    max_drawdown: Option<f64>,
    win_rate: Option<f64>,
    trade_count: Option<u64>,
    duration_seconds: Option<f64>,
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Presets) => show_presets(),
        Some(Commands::Stages) => show_stages(),
        Some(Commands::Status { last }) => show_status(&cli, last).await,
        Some(Commands::Show { ref run_id }) => show_run(&cli, run_id).await,
        Some(Commands::Run) | None => run_pipeline(&cli).await,
    }
}

// ============================================================================
// Command Implementations
// ============================================================================

fn show_presets() -> Result<()> {
    println!("Available Pipeline Presets:");
    println!();
    println!("  default     - Standard configuration for general use");
    println!("                Runs all stages, stops on first failure");
    println!();
    println!("  production  - Conservative settings for live deployment");
    println!("                Strict thresholds, full audit trail");
    println!();
    println!("  research    - Relaxed settings for exploration");
    println!("                Continues on failures, lower thresholds");
    println!();
    println!("  fast        - Quick validation (backtest only)");
    println!("                Skips forward, OOS, paper, live stages");
    println!();
    println!("Usage: validate --config algo.json --preset research");
    Ok(())
}

fn show_stages() -> Result<()> {
    println!("Available Validation Stages:");
    println!();
    println!("  backtest  - Historical replay validation");
    println!("              Replays algorithm on historical data");
    println!();
    println!("  forward   - Walk-forward validation");
    println!("              Splits data into train/test windows");
    println!();
    println!("  oos       - Out-of-sample validation");
    println!("              Final holdout validation (default 20%)");
    println!();
    println!("  paper     - Paper trading validation");
    println!("              Live data, simulated execution");
    println!();
    println!("  live      - Live trading validation");
    println!("              Real execution with OCO risk management");
    println!();
    println!("Usage: validate --config algo.json --stages backtest,forward,oos");
    Ok(())
}

async fn show_status(cli: &Cli, last: usize) -> Result<()> {
    let mut results_store = ResultsStore::new(ResultsStoreConfig::with_path(&cli.results))
        .context("Failed to open results store")?;

    let ids = results_store.list_ids()?;

    if ids.is_empty() {
        println!("No validation runs found.");
        return Ok(());
    }

    println!("Recent Validation Runs (last {}):", last);
    println!();
    println!(
        "{:<36} {:<12} {:<8} {:<8} {:<20}",
        "ID", "Status", "Passed", "Trades", "Timestamp"
    );
    println!("{}", "-".repeat(90));

    for id in ids.iter().take(last) {
        if let Some(result) = results_store.load_by_id(id)? {
            let status_str = match result.stage_type {
                ValidationStageType::Backtest => "Backtest",
                ValidationStageType::Forward => "Forward",
                ValidationStageType::OutOfSample => "OOS",
                ValidationStageType::Paper => "Paper",
                ValidationStageType::Live => "Live",
            };

            let passed_str = if result.passed { "PASS" } else { "FAIL" };

            println!(
                "{:<36} {:<12} {:<8} {:<8} {:<20}",
                &result.id[..36.min(result.id.len())],
                status_str,
                passed_str,
                result.metrics.trade_count,
                result.validated_at.format("%Y-%m-%d %H:%M:%S"),
            );
        }
    }

    Ok(())
}

async fn show_run(cli: &Cli, run_id: &str) -> Result<()> {
    let mut results_store = ResultsStore::new(ResultsStoreConfig::with_path(&cli.results))
        .context("Failed to open results store")?;

    let result = results_store
        .load_by_id(run_id)?
        .ok_or_else(|| anyhow!("Run not found: {}", run_id))?;

    println!("Validation Run Details");
    println!("{}", "=".repeat(50));
    println!();
    println!("Run ID:      {}", result.id);
    println!("Stage:       {:?}", result.stage_type);
    println!("Algorithm:   {}", result.config_id);
    println!("Timestamp:   {}", result.validated_at);
    println!("Passed:      {}", if result.passed { "Yes" } else { "No" });
    println!();
    println!("Metrics:");
    println!("  Sharpe Ratio:  {:.4}", result.metrics.sharpe_ratio);
    println!("  Max Drawdown:  {:.2}%", result.metrics.max_drawdown_pct);
    println!("  Win Rate:      {:.2}%", result.metrics.win_rate * 100.0);
    println!("  Trade Count:   {}", result.metrics.trade_count);
    println!("  Ann. Return:   {:.4}%", result.metrics.annualized_return_pct);
    println!("  Profit Factor: {:.4}", result.metrics.profit_factor);

    Ok(())
}

async fn run_pipeline(cli: &Cli) -> Result<()> {
    // Load or generate algorithm config
    let algorithm_config = load_algorithm_config(cli).await?;

    if !cli.quiet && !cli.json {
        println!("Validation Pipeline");
        println!("{}", "=".repeat(50));
        println!();
        println!("Algorithm: {} ({})", algorithm_config.name, algorithm_config.id);
        println!("Strategy:  {:?}", algorithm_config.strategy_type);
        println!("Data:      {}", cli.data.display());
        println!();
    }

    // Build runner config
    let runner_config = build_runner_config(cli)?;

    // Create results store
    let results_store = if cli.no_persist {
        None
    } else {
        Some(
            ResultsStore::new(ResultsStoreConfig::with_path(&cli.results))
                .context("Failed to create results store")?,
        )
    };

    // Create pipeline runner
    let mut runner = if let Some(store) = results_store {
        PipelineRunner::with_results_store(runner_config, store)
    } else {
        PipelineRunner::new(runner_config)
    };

    // Set up progress display
    let progress = if !cli.quiet && !cli.json {
        let pb = ProgressBar::new(5);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
        Some(pb)
    } else {
        None
    };

    // Run the pipeline
    let start_time = std::time::Instant::now();

    let result = if let Some(from_stage) = cli.from {
        if !cli.quiet && !cli.json {
            println!("Starting from stage: {}", from_stage);
        }
        runner
            .run_from(from_stage.into(), &algorithm_config)
            .await
    } else {
        runner.run_all(&algorithm_config).await
    };

    let duration = start_time.elapsed().as_secs_f64();

    if let Some(pb) = progress {
        pb.finish_and_clear();
    }

    // Handle result
    match result {
        Ok(pipeline_result) => {
            if cli.json {
                output_json(&pipeline_result, &algorithm_config, duration)?;
            } else if !cli.quiet {
                output_summary(&pipeline_result, &algorithm_config, duration);
            }

            // Save to output file if specified
            if let Some(output_path) = &cli.output {
                save_output(&pipeline_result, &algorithm_config, duration, output_path)?;
            }

            // Exit with appropriate code
            match pipeline_result.status {
                PipelineStatus::Passed => Ok(()),
                PipelineStatus::Failed => {
                    if !cli.quiet && !cli.json {
                        println!("\nPipeline FAILED - algorithm did not pass validation");
                    }
                    std::process::exit(1);
                }
                PipelineStatus::Error => {
                    if !cli.quiet && !cli.json {
                        println!("\nPipeline ERROR - execution encountered errors");
                    }
                    std::process::exit(2);
                }
                _ => Ok(()),
            }
        }
        Err(e) => {
            if cli.json {
                let error_output = serde_json::json!({
                    "success": false,
                    "error": e.to_string(),
                    "timestamp": Utc::now().to_rfc3339(),
                });
                println!("{}", serde_json::to_string_pretty(&error_output)?);
            } else {
                eprintln!("Pipeline execution failed: {}", e);
            }
            std::process::exit(3);
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

async fn load_algorithm_config(cli: &Cli) -> Result<AlgorithmConfig> {
    if let Some(config_path) = &cli.config {
        // Load from JSON file
        let content = fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;

        let config: AlgorithmConfig = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", config_path.display()))?;

        Ok(config)
    } else if let Some(research_path) = &cli.from_research {
        // Generate from research state
        let mut store = ResearchStore::new(ResearchStoreConfig::with_path(research_path))
            .context("Failed to open research store")?;

        // Use "default" as the symbol for research state
        let research_state = store
            .load("default")?
            .ok_or_else(|| anyhow!("No research state found at: {}", research_path.display()))?;

        let config = AlgorithmConfig::from_research(&research_state);
        Ok(config)
    } else {
        // Use default config
        if !cli.quiet && !cli.json {
            println!("No config specified, using default algorithm configuration");
        }
        Ok(AlgorithmConfig::default())
    }
}

fn build_runner_config(cli: &Cli) -> Result<RunnerConfig> {
    let data_path = cli.data.to_string_lossy().to_string();

    let mut config = match cli.preset {
        Some(PresetArg::Production) => RunnerConfig::production(&data_path),
        Some(PresetArg::Research) => RunnerConfig::research(&data_path),
        Some(PresetArg::Fast) => RunnerConfig::fast(&data_path),
        Some(PresetArg::Default) | None => RunnerConfig::new(&data_path),
    };

    // Apply command-line overrides
    config.run_name_prefix = cli.name.clone();
    config.results_path = Some(cli.results.to_string_lossy().to_string());
    config.persist_results = !cli.no_persist;

    // Handle continue-on-failure
    if cli.continue_on_failure {
        config.pipeline_config.stop_condition =
            ingestor::validation::StopCondition::ContinueOnFailure;
    }

    // Handle stage selection
    if let Some(stages) = &cli.stages {
        let enabled_stages: Vec<ValidationStageType> =
            stages.iter().map(|s| (*s).into()).collect();

        // Disable all stages first
        for stage_type in [
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
            ValidationStageType::Paper,
            ValidationStageType::Live,
        ] {
            if let Some(stage_config) = config.stage_configs.get_mut(&stage_type) {
                stage_config.enabled = enabled_stages.contains(&stage_type);
            }
        }
    }

    config.validate().map_err(|e| anyhow!("{}", e))?;
    Ok(config)
}

fn output_json(result: &PipelineResult, algo_config: &AlgorithmConfig, duration: f64) -> Result<()> {
    let stage_results: Vec<StageResultJson> = result
        .stage_outcomes
        .iter()
        .map(|(stage_type, outcome)| {
            let (outcome_str, passed, sharpe, max_dd, win_rate, trades, dur) = match outcome {
                StageOutcome::Passed(r) => (
                    "passed".to_string(),
                    Some(true),
                    Some(r.metrics.sharpe_ratio),
                    Some(r.metrics.max_drawdown_pct),
                    Some(r.metrics.win_rate),
                    Some(r.metrics.trade_count as u64),
                    None,
                ),
                StageOutcome::Failed(r) => (
                    "failed".to_string(),
                    Some(false),
                    Some(r.metrics.sharpe_ratio),
                    Some(r.metrics.max_drawdown_pct),
                    Some(r.metrics.win_rate),
                    Some(r.metrics.trade_count as u64),
                    None,
                ),
                StageOutcome::Error(e) => (format!("error: {}", e), None, None, None, None, None, None),
                StageOutcome::Skipped(reason) => {
                    (format!("skipped: {}", reason), None, None, None, None, None, None)
                }
                StageOutcome::Pending => ("pending".to_string(), None, None, None, None, None, None),
            };

            StageResultJson {
                stage: format!("{:?}", stage_type),
                outcome: outcome_str,
                passed,
                sharpe,
                max_drawdown: max_dd,
                win_rate,
                trade_count: trades,
                duration_seconds: dur,
            }
        })
        .collect();

    let recommendation = generate_recommendation(result);

    let output = JsonOutput {
        success: matches!(result.status, PipelineStatus::Passed),
        pipeline_id: result.id.clone(),
        algorithm_id: algo_config.id.clone(),
        status: format!("{:?}", result.status),
        stages_passed: result.stages_passed,
        stages_failed: result.stages_failed,
        stages_skipped: result.stages_skipped,
        duration_seconds: duration,
        timestamp: Utc::now().to_rfc3339(),
        stage_results,
        recommendation,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn output_summary(result: &PipelineResult, algo_config: &AlgorithmConfig, duration: f64) {
    println!();
    println!("Pipeline Results");
    println!("{}", "=".repeat(50));
    println!();

    // Overall status
    let status_icon = match result.status {
        PipelineStatus::Passed => "[PASS]",
        PipelineStatus::Failed => "[FAIL]",
        PipelineStatus::Error => "[ERROR]",
        _ => "[...]",
    };

    println!("Status: {} {:?}", status_icon, result.status);
    println!("Algorithm: {}", algo_config.name);
    println!("Duration: {:.2}s", duration);
    println!();

    // Stage summary
    println!("Stage Results:");
    println!(
        "{:<15} {:<10} {:<10} {:<10} {:<10} {:<8}",
        "Stage", "Status", "Sharpe", "MaxDD", "WinRate", "Trades"
    );
    println!("{}", "-".repeat(65));

    for (stage_type, outcome) in &result.stage_outcomes {
        let stage_name = format!("{:?}", stage_type);
        match outcome {
            StageOutcome::Passed(r) => {
                println!(
                    "{:<15} {:<10} {:<10.4} {:<10.2}% {:<10.2}% {:<8}",
                    stage_name,
                    "PASS",
                    r.metrics.sharpe_ratio,
                    r.metrics.max_drawdown_pct * 100.0,
                    r.metrics.win_rate * 100.0,
                    r.metrics.trade_count,
                );
            }
            StageOutcome::Failed(r) => {
                println!(
                    "{:<15} {:<10} {:<10.4} {:<10.2}% {:<10.2}% {:<8}",
                    stage_name,
                    "FAIL",
                    r.metrics.sharpe_ratio,
                    r.metrics.max_drawdown_pct * 100.0,
                    r.metrics.win_rate * 100.0,
                    r.metrics.trade_count,
                );
            }
            StageOutcome::Error(e) => {
                println!("{:<15} {:<10} {}", stage_name, "ERROR", e);
            }
            StageOutcome::Skipped(reason) => {
                println!("{:<15} {:<10} {}", stage_name, "SKIP", reason);
            }
            StageOutcome::Pending => {
                println!("{:<15} {:<10}", stage_name, "PENDING");
            }
        }
    }

    println!();
    println!("Summary:");
    println!("  Passed:  {}", result.stages_passed);
    println!("  Failed:  {}", result.stages_failed);
    println!("  Skipped: {}", result.stages_skipped);

    // Recommendation
    let recommendation = generate_recommendation(result);
    println!();
    println!("Recommendation: {}", recommendation);
}

fn generate_recommendation(result: &PipelineResult) -> String {
    match result.status {
        PipelineStatus::Passed => {
            "Algorithm passed all validation stages. Ready for deployment.".to_string()
        }
        PipelineStatus::Failed => {
            let failed_stages: Vec<String> = result
                .stage_outcomes
                .iter()
                .filter_map(|(stage, outcome)| {
                    if matches!(outcome, StageOutcome::Failed(_)) {
                        Some(format!("{:?}", stage))
                    } else {
                        None
                    }
                })
                .collect();

            format!(
                "Algorithm failed validation at: {}. Review parameters and retry.",
                failed_stages.join(", ")
            )
        }
        PipelineStatus::Error => {
            "Pipeline encountered errors during execution. Check logs for details.".to_string()
        }
        PipelineStatus::Running => "Pipeline is still running.".to_string(),
        PipelineStatus::Pending => "Pipeline has not started.".to_string(),
        PipelineStatus::Cancelled => "Pipeline was cancelled.".to_string(),
    }
}

fn save_output(
    result: &PipelineResult,
    algo_config: &AlgorithmConfig,
    duration: f64,
    path: &PathBuf,
) -> Result<()> {
    let stage_results: Vec<StageResultJson> = result
        .stage_outcomes
        .iter()
        .map(|(stage_type, outcome)| {
            let (outcome_str, passed, sharpe, max_dd, win_rate, trades, dur) = match outcome {
                StageOutcome::Passed(r) => (
                    "passed".to_string(),
                    Some(true),
                    Some(r.metrics.sharpe_ratio),
                    Some(r.metrics.max_drawdown_pct),
                    Some(r.metrics.win_rate),
                    Some(r.metrics.trade_count as u64),
                    None,
                ),
                StageOutcome::Failed(r) => (
                    "failed".to_string(),
                    Some(false),
                    Some(r.metrics.sharpe_ratio),
                    Some(r.metrics.max_drawdown_pct),
                    Some(r.metrics.win_rate),
                    Some(r.metrics.trade_count as u64),
                    None,
                ),
                StageOutcome::Error(e) => (format!("error: {}", e), None, None, None, None, None, None),
                StageOutcome::Skipped(reason) => {
                    (format!("skipped: {}", reason), None, None, None, None, None, None)
                }
                StageOutcome::Pending => ("pending".to_string(), None, None, None, None, None, None),
            };

            StageResultJson {
                stage: format!("{:?}", stage_type),
                outcome: outcome_str,
                passed,
                sharpe,
                max_drawdown: max_dd,
                win_rate,
                trade_count: trades,
                duration_seconds: dur,
            }
        })
        .collect();

    let recommendation = generate_recommendation(result);

    let output = JsonOutput {
        success: matches!(result.status, PipelineStatus::Passed),
        pipeline_id: result.id.clone(),
        algorithm_id: algo_config.id.clone(),
        status: format!("{:?}", result.status),
        stages_passed: result.stages_passed,
        stages_failed: result.stages_failed,
        stages_skipped: result.stages_skipped,
        duration_seconds: duration,
        timestamp: Utc::now().to_rfc3339(),
        stage_results,
        recommendation,
    };

    let json = serde_json::to_string_pretty(&output)?;
    fs::write(path, json).with_context(|| format!("Failed to write output to: {}", path.display()))?;

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ========================================================================
    // CLI Argument Parsing Tests
    // ========================================================================

    #[test]
    fn test_stage_arg_conversion() {
        assert_eq!(
            ValidationStageType::from(StageArg::Backtest),
            ValidationStageType::Backtest
        );
        assert_eq!(
            ValidationStageType::from(StageArg::Forward),
            ValidationStageType::Forward
        );
        assert_eq!(
            ValidationStageType::from(StageArg::Oos),
            ValidationStageType::OutOfSample
        );
        assert_eq!(
            ValidationStageType::from(StageArg::Paper),
            ValidationStageType::Paper
        );
        assert_eq!(
            ValidationStageType::from(StageArg::Live),
            ValidationStageType::Live
        );
    }

    #[test]
    fn test_stage_arg_display() {
        assert_eq!(format!("{}", StageArg::Backtest), "backtest");
        assert_eq!(format!("{}", StageArg::Forward), "forward");
        assert_eq!(format!("{}", StageArg::Oos), "oos");
        assert_eq!(format!("{}", StageArg::Paper), "paper");
        assert_eq!(format!("{}", StageArg::Live), "live");
    }

    // ========================================================================
    // Runner Config Builder Tests
    // ========================================================================

    #[test]
    fn test_build_runner_config_default() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = build_runner_config(&cli).unwrap();
        assert_eq!(config.data_path, "./data");
        assert_eq!(config.run_name_prefix, "test");
        assert!(config.persist_results);
    }

    #[test]
    fn test_build_runner_config_production_preset() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: Some(PresetArg::Production),
            quiet: false,
            json: false,
            output: None,
            name: "prod-test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = build_runner_config(&cli).unwrap();
        assert_eq!(config.run_name_prefix, "prod-test");
    }

    #[test]
    fn test_build_runner_config_fast_preset() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: Some(PresetArg::Fast),
            quiet: false,
            json: false,
            output: None,
            name: "fast-test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = build_runner_config(&cli).unwrap();
        // Fast preset disables most stages
        let backtest_enabled = config
            .stage_configs
            .get(&ValidationStageType::Backtest)
            .map(|c| c.enabled)
            .unwrap_or(false);
        assert!(backtest_enabled);

        let forward_enabled = config
            .stage_configs
            .get(&ValidationStageType::Forward)
            .map(|c| c.enabled)
            .unwrap_or(true);
        assert!(!forward_enabled);
    }

    #[test]
    fn test_build_runner_config_continue_on_failure() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: true,
            no_persist: false,
        };

        let config = build_runner_config(&cli).unwrap();
        assert!(matches!(
            config.pipeline_config.stop_condition,
            ingestor::validation::StopCondition::ContinueOnFailure
        ));
    }

    #[test]
    fn test_build_runner_config_no_persist() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: true,
        };

        let config = build_runner_config(&cli).unwrap();
        assert!(!config.persist_results);
    }

    #[test]
    fn test_build_runner_config_stage_selection() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: Some(vec![StageArg::Backtest, StageArg::Forward]),
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = build_runner_config(&cli).unwrap();

        let backtest_enabled = config
            .stage_configs
            .get(&ValidationStageType::Backtest)
            .map(|c| c.enabled)
            .unwrap_or(false);
        assert!(backtest_enabled);

        let forward_enabled = config
            .stage_configs
            .get(&ValidationStageType::Forward)
            .map(|c| c.enabled)
            .unwrap_or(false);
        assert!(forward_enabled);

        let oos_enabled = config
            .stage_configs
            .get(&ValidationStageType::OutOfSample)
            .map(|c| c.enabled)
            .unwrap_or(true);
        assert!(!oos_enabled);

        let paper_enabled = config
            .stage_configs
            .get(&ValidationStageType::Paper)
            .map(|c| c.enabled)
            .unwrap_or(true);
        assert!(!paper_enabled);

        let live_enabled = config
            .stage_configs
            .get(&ValidationStageType::Live)
            .map(|c| c.enabled)
            .unwrap_or(true);
        assert!(!live_enabled);
    }

    // ========================================================================
    // Recommendation Generation Tests
    // ========================================================================

    #[test]
    fn test_generate_recommendation_passed() {
        let mut result = PipelineResult::new("test-pipeline".to_string(), "test-algo".to_string());
        result.status = PipelineStatus::Passed;

        let recommendation = generate_recommendation(&result);
        assert!(recommendation.contains("passed all validation stages"));
        assert!(recommendation.contains("Ready for deployment"));
    }

    #[test]
    fn test_generate_recommendation_failed() {
        use ingestor::core::ValidationResult;

        let mut result = PipelineResult::new("test-pipeline".to_string(), "test-algo".to_string());
        result.status = PipelineStatus::Failed;

        // Add a failed stage
        let validation_result = ValidationResult::new(
            ValidationStageType::Backtest,
            "backtest-stage".to_string(),
            "test-config".to_string(),
            chrono::Utc::now(),
            chrono::Utc::now(),
        );
        result.add_outcome(
            ValidationStageType::Backtest,
            StageOutcome::Failed(validation_result),
        );

        let recommendation = generate_recommendation(&result);
        assert!(recommendation.contains("failed validation"));
        assert!(recommendation.contains("Backtest"));
    }

    #[test]
    fn test_generate_recommendation_error() {
        let mut result = PipelineResult::new("test-pipeline".to_string(), "test-algo".to_string());
        result.status = PipelineStatus::Error;

        let recommendation = generate_recommendation(&result);
        assert!(recommendation.contains("encountered errors"));
    }

    // ========================================================================
    // JSON Output Tests
    // ========================================================================

    #[test]
    fn test_json_output_structure() {
        let result = PipelineResult::new("test-pipeline".to_string(), "test-algo".to_string());
        let algo_config = AlgorithmConfig::default();

        // Capture JSON output
        let stage_results: Vec<StageResultJson> = result
            .stage_outcomes
            .iter()
            .map(|(stage_type, outcome)| StageResultJson {
                stage: format!("{:?}", stage_type),
                outcome: "pending".to_string(),
                passed: None,
                sharpe: None,
                max_drawdown: None,
                win_rate: None,
                trade_count: None,
                duration_seconds: None,
            })
            .collect();

        let output = JsonOutput {
            success: false,
            pipeline_id: result.id.clone(),
            algorithm_id: algo_config.id.clone(),
            status: format!("{:?}", result.status),
            stages_passed: result.stages_passed,
            stages_failed: result.stages_failed,
            stages_skipped: result.stages_skipped,
            duration_seconds: 0.0,
            timestamp: Utc::now().to_rfc3339(),
            stage_results,
            recommendation: "Test".to_string(),
        };

        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("pipeline_id"));
        assert!(json.contains("algorithm_id"));
        assert!(json.contains("status"));
    }

    // ========================================================================
    // Config Loading Tests
    // ========================================================================

    #[tokio::test]
    async fn test_load_algorithm_config_default() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: true,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = load_algorithm_config(&cli).await.unwrap();
        assert!(!config.id.is_empty());
    }

    #[tokio::test]
    async fn test_load_algorithm_config_from_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("algo.json");

        let algo_config = AlgorithmConfig::default();
        let json = serde_json::to_string_pretty(&algo_config).unwrap();
        fs::write(&config_path, json).unwrap();

        let cli = Cli {
            command: None,
            config: Some(config_path),
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: true,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = load_algorithm_config(&cli).await.unwrap();
        assert_eq!(config.id, algo_config.id);
    }

    #[tokio::test]
    async fn test_load_algorithm_config_missing_file() {
        let cli = Cli {
            command: None,
            config: Some(PathBuf::from("/nonexistent/path/algo.json")),
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: true,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let result = load_algorithm_config(&cli).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Output File Tests
    // ========================================================================

    #[test]
    fn test_save_output() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("output.json");

        let result = PipelineResult::new("test-pipeline".to_string(), "test-algo".to_string());
        let algo_config = AlgorithmConfig::default();

        save_output(&result, &algo_config, 1.5, &output_path).unwrap();

        assert!(output_path.exists());

        let content = fs::read_to_string(&output_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        // PipelineResult::new() generates a UUID for id, so just verify it exists
        assert!(parsed["pipeline_id"].is_string());
        assert!(!parsed["pipeline_id"].as_str().unwrap().is_empty());
        assert_eq!(parsed["duration_seconds"], 1.5);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_empty_stages_selection() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: Some(vec![]),
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let result = build_runner_config(&cli);
        // Should fail because no stages are enabled
        assert!(result.is_err());
    }

    #[test]
    fn test_all_stages_selected() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: Some(vec![
                StageArg::Backtest,
                StageArg::Forward,
                StageArg::Oos,
                StageArg::Paper,
                StageArg::Live,
            ]),
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: None,
            quiet: false,
            json: false,
            output: None,
            name: "test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = build_runner_config(&cli).unwrap();

        for stage_type in [
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
            ValidationStageType::Paper,
            ValidationStageType::Live,
        ] {
            let enabled = config
                .stage_configs
                .get(&stage_type)
                .map(|c| c.enabled)
                .unwrap_or(false);
            assert!(enabled, "Stage {:?} should be enabled", stage_type);
        }
    }

    // ========================================================================
    // Research Preset Tests
    // ========================================================================

    #[test]
    fn test_research_preset_config() {
        let cli = Cli {
            command: None,
            config: None,
            from_research: None,
            stages: None,
            from: None,
            data: PathBuf::from("./data"),
            results: PathBuf::from("./results"),
            preset: Some(PresetArg::Research),
            quiet: false,
            json: false,
            output: None,
            name: "research-test".to_string(),
            continue_on_failure: false,
            no_persist: false,
        };

        let config = build_runner_config(&cli).unwrap();
        // Research preset should not load previous results
        assert!(!config.load_previous_results);
    }

    // ========================================================================
    // Integration-Style Tests
    // ========================================================================

    #[test]
    fn test_full_cli_workflow_simulation() {
        // Simulate what would happen with various CLI configurations
        let test_cases = vec![
            ("default", None, None, true),
            ("production", Some(PresetArg::Production), None, true),
            ("research", Some(PresetArg::Research), None, true),
            ("fast", Some(PresetArg::Fast), None, true),
            (
                "backtest-only",
                None,
                Some(vec![StageArg::Backtest]),
                true,
            ),
            (
                "multiple-stages",
                None,
                Some(vec![StageArg::Backtest, StageArg::Forward]),
                true,
            ),
        ];

        for (name, preset, stages, should_succeed) in test_cases {
            let cli = Cli {
                command: None,
                config: None,
                from_research: None,
                stages,
                from: None,
                data: PathBuf::from("./data"),
                results: PathBuf::from("./results"),
                preset,
                quiet: false,
                json: false,
                output: None,
                name: name.to_string(),
                continue_on_failure: false,
                no_persist: false,
            };

            let result = build_runner_config(&cli);
            assert_eq!(
                result.is_ok(),
                should_succeed,
                "Test case '{}' failed",
                name
            );
        }
    }

    // ========================================================================
    // StageOutcome Mapping Tests
    // ========================================================================

    #[test]
    fn test_stage_outcome_to_json_passed() {
        use ingestor::core::{ValidationMetrics, ValidationResult};

        let mut validation_result = ValidationResult::new(
            ValidationStageType::Backtest,
            "backtest-stage".to_string(),
            "test-config".to_string(),
            chrono::Utc::now(),
            chrono::Utc::now(),
        );
        validation_result.metrics = ValidationMetrics {
            sharpe_ratio: 1.5,
            max_drawdown_pct: 0.1,
            win_rate: 0.6,
            trade_count: 100,
            annualized_return_pct: 0.15,
            profit_factor: 1.8,
            ..Default::default()
        };
        validation_result.passed = true;

        let outcome = StageOutcome::Passed(validation_result);

        let stage_result = match &outcome {
            StageOutcome::Passed(r) => StageResultJson {
                stage: "Backtest".to_string(),
                outcome: "passed".to_string(),
                passed: Some(true),
                sharpe: Some(r.metrics.sharpe_ratio),
                max_drawdown: Some(r.metrics.max_drawdown_pct),
                win_rate: Some(r.metrics.win_rate),
                trade_count: Some(r.metrics.trade_count as u64),
                duration_seconds: None,
            },
            _ => panic!("Expected Passed outcome"),
        };

        assert_eq!(stage_result.outcome, "passed");
        assert_eq!(stage_result.passed, Some(true));
        assert_eq!(stage_result.sharpe, Some(1.5));
    }

    #[test]
    fn test_stage_outcome_to_json_error() {
        let outcome = StageOutcome::Error("Test error".to_string());

        let stage_result = match &outcome {
            StageOutcome::Error(e) => StageResultJson {
                stage: "Backtest".to_string(),
                outcome: format!("error: {}", e),
                passed: None,
                sharpe: None,
                max_drawdown: None,
                win_rate: None,
                trade_count: None,
                duration_seconds: None,
            },
            _ => panic!("Expected Error outcome"),
        };

        assert!(stage_result.outcome.contains("error:"));
        assert!(stage_result.outcome.contains("Test error"));
        assert_eq!(stage_result.passed, None);
    }

    #[test]
    fn test_stage_outcome_to_json_skipped() {
        let outcome = StageOutcome::Skipped("Stage disabled".to_string());

        let stage_result = match &outcome {
            StageOutcome::Skipped(reason) => StageResultJson {
                stage: "Forward".to_string(),
                outcome: format!("skipped: {}", reason),
                passed: None,
                sharpe: None,
                max_drawdown: None,
                win_rate: None,
                trade_count: None,
                duration_seconds: None,
            },
            _ => panic!("Expected Skipped outcome"),
        };

        assert!(stage_result.outcome.contains("skipped:"));
        assert!(stage_result.outcome.contains("Stage disabled"));
    }

    // ========================================================================
    // Path Handling Tests
    // ========================================================================

    #[test]
    fn test_config_with_various_paths() {
        let test_paths = vec![
            "./data/features",
            "/absolute/path/to/data",
            "relative/path",
            ".",
        ];

        for path in test_paths {
            let cli = Cli {
                command: None,
                config: None,
                from_research: None,
                stages: None,
                from: None,
                data: PathBuf::from(path),
                results: PathBuf::from("./results"),
                preset: None,
                quiet: false,
                json: false,
                output: None,
                name: "test".to_string(),
                continue_on_failure: false,
                no_persist: false,
            };

            let config = build_runner_config(&cli).unwrap();
            assert_eq!(config.data_path, path);
        }
    }

    // ========================================================================
    // Subcommand Tests
    // ========================================================================

    #[test]
    fn test_show_presets_runs_without_error() {
        // This just tests that the function doesn't panic
        let result = show_presets();
        assert!(result.is_ok());
    }

    #[test]
    fn test_show_stages_runs_without_error() {
        // This just tests that the function doesn't panic
        let result = show_stages();
        assert!(result.is_ok());
    }
}

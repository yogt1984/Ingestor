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

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

use ingestor::commands::{
    ValidateCommands,
    common::NoOpCallback,
};
use ingestor::commands::params::validate_params::{
    RunParamsBuilder as ValidateRunParamsBuilder,
    PresetsParamsBuilder,
    StagesParamsBuilder,
    StatusParamsBuilder as ValidateStatusParamsBuilder,
    ShowParamsBuilder,
};
use ingestor::commands::validate::{
    RunResult, PresetsResult, StagesResult, StatusResult, ShowResult,
};
use ingestor::core::ValidationStageType;

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


// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Presets) => {
            let params = PresetsParamsBuilder::new().build()?;
            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = ValidateCommands::presets(params, callback)?;
            print_presets(&result);
            Ok(())
        }
        Some(Commands::Stages) => {
            let params = StagesParamsBuilder::new().build()?;
            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = ValidateCommands::stages(params, callback)?;
            print_stages(&result);
            Ok(())
        }
        Some(Commands::Status { last }) => {
            let params = ValidateStatusParamsBuilder::new()
                .with_results(cli.results.clone())
                .with_last(last)
                .build()?;
            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = ValidateCommands::status(params, callback)?;
            print_status(&result);
            Ok(())
        }
        Some(Commands::Show { ref run_id }) => {
            let params = ShowParamsBuilder::new()
                .with_results(cli.results.clone())
                .with_run_id(run_id.clone())
                .with_json(cli.json)
                .with_verbose(false) // CLI doesn't have verbose flag for show
                .build()?;
            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = ValidateCommands::show(params, callback)?;
            if cli.json {
                print_show_json(&result)?;
            } else {
                print_show(&result);
            }
            Ok(())
        }
        Some(Commands::Run) | None => {
            // Convert CLI args to params
            let stages = cli.stages.as_ref().map(|s| {
                s.iter().map(|stage_arg| {
                    match stage_arg {
                        StageArg::Backtest => ValidationStageType::Backtest,
                        StageArg::Forward => ValidationStageType::Forward,
                        StageArg::Oos => ValidationStageType::OutOfSample,
                        StageArg::Paper => ValidationStageType::Paper,
                        StageArg::Live => ValidationStageType::Live,
                    }
                }).collect()
            });

            let from_stage = cli.from.map(|stage_arg| {
                match stage_arg {
                    StageArg::Backtest => ValidationStageType::Backtest,
                    StageArg::Forward => ValidationStageType::Forward,
                    StageArg::Oos => ValidationStageType::OutOfSample,
                    StageArg::Paper => ValidationStageType::Paper,
                    StageArg::Live => ValidationStageType::Live,
                }
            });

            let preset = cli.preset.map(|p| {
                match p {
                    PresetArg::Default => "default".to_string(),
                    PresetArg::Production => "production".to_string(),
                    PresetArg::Research => "research".to_string(),
                    PresetArg::Fast => "fast".to_string(),
                }
            });

            let params = ValidateRunParamsBuilder::new()
                .with_config(cli.config.clone())
                .with_from_research(cli.from_research.clone())
                .with_stages(stages)
                .with_from_stage(from_stage)
                .with_data(cli.data.clone())
                .with_results(cli.results.clone())
                .with_preset(preset)
                .with_quiet(cli.quiet)
                .with_json(cli.json)
                .with_output(cli.output.clone())
                .with_name(cli.name.clone())
                .with_continue_on_failure(cli.continue_on_failure)
                .with_no_persist(cli.no_persist)
                .build()?;

            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = ValidateCommands::run(params, callback).await?;

            // Handle output
            if cli.json {
                print_run_json(&result)?;
            } else if !cli.quiet {
                print_run_summary(&result);
            }

            // Save to output file if specified
            if let Some(output_path) = &cli.output {
                save_output(&result, output_path)?;
            }

            // Exit with appropriate code
            match result.pipeline_result.status {
                ingestor::validation::PipelineStatus::Passed => Ok(()),
                ingestor::validation::PipelineStatus::Failed => {
                    if !cli.quiet && !cli.json {
                        println!("\nPipeline FAILED - algorithm did not pass validation");
                    }
                    std::process::exit(1);
                }
                ingestor::validation::PipelineStatus::Error => {
                    if !cli.quiet && !cli.json {
                        println!("\nPipeline ERROR - execution encountered errors");
                    }
                    std::process::exit(2);
                }
                _ => Ok(()),
            }
        }
    }
}

// ============================================================================
// Output Formatting Functions
// ============================================================================

fn print_presets(result: &PresetsResult) {
    println!("Available Pipeline Presets:");
    println!();
    for preset in &result.presets {
        println!("  {:<12} - {}", preset.name, preset.description);
    println!();
    }
    println!("Usage: validate --config algo.json --preset research");
}

fn print_stages(result: &StagesResult) {
    println!("Available Validation Stages:");
    println!();
    for stage in &result.stages {
        println!("  {:<12} - {}", stage.name, stage.description);
    println!();
    }
    println!("Usage: validate --config algo.json --stages backtest,forward,oos");
}

fn print_status(result: &StatusResult) {
    if result.runs.is_empty() {
        println!("No validation runs found.");
        return;
    }

    println!("Recent Validation Runs (last {}):", result.runs.len());
    println!();
    println!(
        "{:<36} {:<12} {:<8} {:<8} {:<20}",
        "ID", "Status", "Passed", "Trades", "Timestamp"
    );
    println!("{}", "-".repeat(90));

    for run in &result.runs {
        let passed_str = if run.passed { "PASS" } else { "FAIL" };
            println!(
                "{:<36} {:<12} {:<8} {:<8} {:<20}",
            &run.id[..36.min(run.id.len())],
            run.stage_type,
                passed_str,
            run.trade_count,
            run.timestamp,
        );
    }
}

fn print_show(result: &ShowResult) {
    println!("Validation Run Details");
    println!("{}", "=".repeat(50));
    println!();
    println!("Run ID:      {}", result.run_id);
    println!("Stage:       {}", result.stage_type);
    println!("Algorithm:   {}", result.config_id);
    println!("Timestamp:   {}", result.timestamp);
    println!("Passed:      {}", if result.passed { "Yes" } else { "No" });
    println!();
    println!("Metrics:");
    println!("  Sharpe Ratio:  {:.4}", result.result.metrics.sharpe_ratio);
    println!("  Max Drawdown:  {:.2}%", result.result.metrics.max_drawdown_pct);
    println!("  Win Rate:      {:.2}%", result.result.metrics.win_rate * 100.0);
    println!("  Trade Count:   {}", result.result.metrics.trade_count);
    println!("  Ann. Return:   {:.4}%", result.result.metrics.annualized_return_pct);
    println!("  Profit Factor: {:.4}", result.result.metrics.profit_factor);
}

fn print_show_json(result: &ShowResult) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

fn print_run_json(result: &RunResult) -> Result<()> {
    use serde::Serialize;
    use chrono::Utc;

    #[derive(Serialize)]
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

    #[derive(Serialize)]
    struct JsonOutput {
        success: bool,
        pipeline_id: String,
        algorithm_id: String,
        algorithm_name: String,
        status: String,
        stages_passed: usize,
        stages_failed: usize,
        stages_skipped: usize,
        duration_seconds: f64,
        timestamp: String,
        stage_results: Vec<StageResultJson>,
        recommendation: String,
    }

    let stage_results: Vec<StageResultJson> = result
        .pipeline_result
        .stage_outcomes
        .iter()
        .map(|(stage_type, outcome)| {
            let (outcome_str, passed, sharpe, max_dd, win_rate, trades, dur) = match outcome {
                ingestor::validation::StageOutcome::Passed(r) => (
                    "passed".to_string(),
                    Some(true),
                    Some(r.metrics.sharpe_ratio),
                    Some(r.metrics.max_drawdown_pct),
                    Some(r.metrics.win_rate),
                    Some(r.metrics.trade_count as u64),
                    None,
                ),
                ingestor::validation::StageOutcome::Failed(r) => (
                    "failed".to_string(),
                    Some(false),
                    Some(r.metrics.sharpe_ratio),
                    Some(r.metrics.max_drawdown_pct),
                    Some(r.metrics.win_rate),
                    Some(r.metrics.trade_count as u64),
                    None,
                ),
                ingestor::validation::StageOutcome::Error(e) => {
                    (format!("error: {}", e), None, None, None, None, None, None)
                }
                ingestor::validation::StageOutcome::Skipped(reason) => {
                    (format!("skipped: {}", reason), None, None, None, None, None, None)
                }
                ingestor::validation::StageOutcome::Pending => {
                    ("pending".to_string(), None, None, None, None, None, None)
                }
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

    let recommendation = generate_recommendation(&result.pipeline_result);

    let output = JsonOutput {
        success: matches!(result.pipeline_result.status, ingestor::validation::PipelineStatus::Passed),
        pipeline_id: result.pipeline_result.id.clone(),
        algorithm_id: result.algorithm_config_id.clone(),
        algorithm_name: result.algorithm_name.clone(),
        status: format!("{:?}", result.pipeline_result.status),
        stages_passed: result.pipeline_result.stages_passed,
        stages_failed: result.pipeline_result.stages_failed,
        stages_skipped: result.pipeline_result.stages_skipped,
        duration_seconds: result.duration_seconds,
        timestamp: Utc::now().to_rfc3339(),
        stage_results,
        recommendation,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn print_run_summary(result: &RunResult) {
    println!();
    println!("Pipeline Results");
    println!("{}", "=".repeat(50));
    println!();

    // Overall status
    let status_icon = match result.pipeline_result.status {
        ingestor::validation::PipelineStatus::Passed => "[PASS]",
        ingestor::validation::PipelineStatus::Failed => "[FAIL]",
        ingestor::validation::PipelineStatus::Error => "[ERROR]",
        _ => "[...]",
    };

    println!("Status: {} {:?}", status_icon, result.pipeline_result.status);
    println!("Algorithm: {} ({})", result.algorithm_name, result.algorithm_config_id);
    println!("Duration: {:.2}s", result.duration_seconds);
    println!();

    // Stage summary
    println!("Stage Results:");
    println!(
        "{:<15} {:<10} {:<10} {:<10} {:<10} {:<8}",
        "Stage", "Status", "Sharpe", "MaxDD", "WinRate", "Trades"
    );
    println!("{}", "-".repeat(65));

    for (stage_type, outcome) in &result.pipeline_result.stage_outcomes {
        let stage_name = format!("{:?}", stage_type);
        match outcome {
            ingestor::validation::StageOutcome::Passed(r) => {
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
            ingestor::validation::StageOutcome::Failed(r) => {
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
            ingestor::validation::StageOutcome::Error(e) => {
                println!("{:<15} {:<10} {}", stage_name, "ERROR", e);
            }
            ingestor::validation::StageOutcome::Skipped(reason) => {
                println!("{:<15} {:<10} {}", stage_name, "SKIP", reason);
            }
            ingestor::validation::StageOutcome::Pending => {
                println!("{:<15} {:<10}", stage_name, "PENDING");
            }
        }
    }

    println!();
    println!("Summary:");
    println!("  Passed:  {}", result.pipeline_result.stages_passed);
    println!("  Failed:  {}", result.pipeline_result.stages_failed);
    println!("  Skipped: {}", result.pipeline_result.stages_skipped);

    // Recommendation
    let recommendation = generate_recommendation(&result.pipeline_result);
    println!();
    println!("Recommendation: {}", recommendation);
}

fn generate_recommendation(result: &ingestor::validation::PipelineResult) -> String {
    match result.status {
        ingestor::validation::PipelineStatus::Passed => {
            "Algorithm passed all validation stages. Ready for deployment.".to_string()
        }
        ingestor::validation::PipelineStatus::Failed => {
            let failed_stages: Vec<String> = result
                .stage_outcomes
                .iter()
                .filter_map(|(stage, outcome)| {
                    if matches!(outcome, ingestor::validation::StageOutcome::Failed(_)) {
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
        ingestor::validation::PipelineStatus::Error => {
            "Pipeline encountered errors during execution. Check logs for details.".to_string()
        }
        ingestor::validation::PipelineStatus::Running => "Pipeline is still running.".to_string(),
        ingestor::validation::PipelineStatus::Pending => "Pipeline has not started.".to_string(),
        ingestor::validation::PipelineStatus::Cancelled => "Pipeline was cancelled.".to_string(),
    }
}

fn save_output(result: &RunResult, path: &PathBuf) -> Result<()> {
    use std::fs;
    use serde::Serialize;
    use chrono::Utc;
    use anyhow::Context;

    #[derive(Serialize)]
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

    #[derive(Serialize)]
    struct JsonOutput {
        success: bool,
        pipeline_id: String,
        algorithm_id: String,
        algorithm_name: String,
        status: String,
        stages_passed: usize,
        stages_failed: usize,
        stages_skipped: usize,
        duration_seconds: f64,
        timestamp: String,
        stage_results: Vec<StageResultJson>,
        recommendation: String,
    }

    let stage_results: Vec<StageResultJson> = result
        .pipeline_result
        .stage_outcomes
        .iter()
        .map(|(stage_type, outcome)| {
            let (outcome_str, passed, sharpe, max_dd, win_rate, trades, dur) = match outcome {
                ingestor::validation::StageOutcome::Passed(r) => (
                    "passed".to_string(),
                    Some(true),
                    Some(r.metrics.sharpe_ratio),
                    Some(r.metrics.max_drawdown_pct),
                    Some(r.metrics.win_rate),
                    Some(r.metrics.trade_count as u64),
                    None,
                ),
                ingestor::validation::StageOutcome::Failed(r) => (
                    "failed".to_string(),
                    Some(false),
                    Some(r.metrics.sharpe_ratio),
                    Some(r.metrics.max_drawdown_pct),
                    Some(r.metrics.win_rate),
                    Some(r.metrics.trade_count as u64),
                    None,
                ),
                ingestor::validation::StageOutcome::Error(e) => {
                    (format!("error: {}", e), None, None, None, None, None, None)
                }
                ingestor::validation::StageOutcome::Skipped(reason) => {
                    (format!("skipped: {}", reason), None, None, None, None, None, None)
                }
                ingestor::validation::StageOutcome::Pending => {
                    ("pending".to_string(), None, None, None, None, None, None)
                }
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

    let recommendation = generate_recommendation(&result.pipeline_result);

    let output = JsonOutput {
        success: matches!(result.pipeline_result.status, ingestor::validation::PipelineStatus::Passed),
        pipeline_id: result.pipeline_result.id.clone(),
        algorithm_id: result.algorithm_config_id.clone(),
        algorithm_name: result.algorithm_name.clone(),
        status: format!("{:?}", result.pipeline_result.status),
        stages_passed: result.pipeline_result.stages_passed,
        stages_failed: result.pipeline_result.stages_failed,
        stages_skipped: result.pipeline_result.stages_skipped,
        duration_seconds: result.duration_seconds,
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
// Note: Tests for validate commands are in src/commands/validate.rs


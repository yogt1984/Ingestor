//! Research CLI - Tasks 1.6 & 1.7
//!
//! CLI commands for research analysis on historical market data and status display.
//!
//! # Usage
//!
//! ```bash
//! # Run research on historical data
//! cargo run --release --bin research -- run --data ./data/features --output ./research/
//!
//! # With date range filtering
//! cargo run --release --bin research -- run --data ./data/features \
//!     --start 2024-01-01 --end 2024-01-31 --output ./research/
//!
//! # With custom symbol
//! cargo run --release --bin research -- run --data ./data/features \
//!     --symbol ETHUSDT --output ./research/
//!
//! # Quiet mode (no progress bar)
//! cargo run --release --bin research -- run --data ./data/features -q
//!
//! # Resume from previous state
//! cargo run --release --bin research -- run --data ./data/features --resume
//!
//! # Display current research status (Task 1.7)
//! cargo run --release --bin research -- status --store ./research/
//!
//! # Status with JSON output
//! cargo run --release --bin research -- status --store ./research/ --json
//! ```
//!
//! # Output
//!
//! The research CLI produces:
//! - MIDC estimate (kappa) with interpretation
//! - Persistence statistics across regimes
//! - Top conditional probability signals
//! - Tradeable assessment with color-coded regime
//! - Recommendation for trading strategy

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};

use ingestor::commands::{
    ResearchCommands,
    common::NoOpCallback,
};
use ingestor::commands::params::research_params::{RunParamsBuilder, StatusParamsBuilder};
use ingestor::commands::research::{RunResult, StatusResult};

// ============================================================================
// CLI Structures
// ============================================================================

#[derive(Parser)]
#[command(name = "research")]
#[command(about = "Run research analysis on historical market data")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run research analysis on historical feature data
    #[command(alias = "r")]
    Run(RunArgs),

    /// Display current research status (Task 1.7)
    #[command(alias = "s")]
    Status(StatusArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct StatusArgs {
    /// Path to research store directory
    #[arg(short, long, default_value = "./research")]
    pub store: PathBuf,

    /// Trading symbol to query (e.g., BTCUSDT)
    #[arg(short = 'y', long, default_value = "BTCUSDT")]
    pub symbol: String,

    /// Output results as JSON
    #[arg(long)]
    pub json: bool,

    /// Show verbose output with all details
    #[arg(short, long)]
    pub verbose: bool,

    /// Number of top signals to display
    #[arg(long, default_value = "5")]
    pub top_signals: usize,
}

#[derive(Parser, Debug, Clone)]
pub struct RunArgs {
    /// Path to data directory containing Parquet feature files
    #[arg(short, long, default_value = "./data/features")]
    pub data: PathBuf,

    /// Path to output directory for research state
    #[arg(short, long, default_value = "./research")]
    pub output: PathBuf,

    /// Trading symbol (e.g., BTCUSDT)
    #[arg(short, long, default_value = "BTCUSDT")]
    pub symbol: String,

    /// Start date for filtering (YYYY-MM-DD)
    #[arg(long)]
    pub start: Option<String>,

    /// End date for filtering (YYYY-MM-DD)
    #[arg(long)]
    pub end: Option<String>,

    /// Minimum samples before engine is considered ready
    #[arg(long, default_value = "100")]
    pub min_samples: usize,

    /// Checkpoint interval (number of samples between saves)
    #[arg(long, default_value = "10000")]
    pub checkpoint_interval: usize,

    /// Resume from previous state if available
    #[arg(long)]
    pub resume: bool,

    /// Quiet mode (disable progress bar)
    #[arg(short, long)]
    pub quiet: bool,

    /// Output results as JSON
    #[arg(long)]
    pub json: bool,
}

// ============================================================================
// Output Formatting Functions
// ============================================================================

/// Print human-readable summary
fn print_summary(result: &RunResult) {
    println!("\n{}", "=".repeat(60));
    println!("                    RESEARCH SUMMARY");
    println!("{}", "=".repeat(60));

    // Processing stats
    println!("\n--- Processing Statistics ---");
    println!("  Samples processed:  {:>12}", result.samples_processed);
    println!("  Duration:           {:>12.2} seconds", result.duration_seconds);
    println!("  Checkpoints saved:  {:>12}", result.checkpoints_saved);
    println!(
        "  Throughput:         {:>12.0} samples/sec",
        result.samples_processed as f64 / result.duration_seconds.max(0.001)
    );

    // MIDC Analysis
    println!("\n--- MIDC Analysis ---");
    println!("  Kappa (diffusion):  {:>12.6}", result.midc_kappa);
    println!("  Confidence:         {:>12.2}%", result.midc_confidence * 100.0);
    println!("  Regime:             {:>12}", result.midc_regime);
    println!("  Interpretation:     {}", interpret_midc(result.midc_kappa));

    // Persistence Analysis
    println!("\n--- Persistence Analysis ---");
    println!(
        "  Mean trend duration: {:>11.2} seconds",
        result.persistence_mean_seconds
    );
    println!("  Trends observed:    {:>12}", result.persistence_sample_count);

    // Top Signals
    if !result.top_signals.is_empty() {
        println!("\n--- Top Conditional Signals ---");
        for (i, sig) in result.top_signals.iter().take(5).enumerate() {
            println!(
                "  {}. {} -> P(cont)={:.3} [n={}, CI=({:.3},{:.3})]",
                i + 1,
                sig.signature,
                sig.p_continuation,
                sig.sample_count,
                sig.confidence_lower,
                sig.confidence_upper
            );
        }
    }

    // Tradeable Assessment
    println!("\n--- Tradeable Assessment ---");
    let status = if result.is_tradeable { "YES" } else { "NO" };
    println!("  Is Tradeable:       {:>12}", status);
    println!("  Reason:             {}", result.tradeable_reason);

    println!("\n{}", "=".repeat(60));
}

/// Print JSON output
fn print_json(result: &RunResult) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

/// Interpret MIDC kappa value
fn interpret_midc(kappa: f64) -> &'static str {
    if kappa < 0.01 {
        "Very efficient (strong mean-reversion)"
    } else if kappa < 0.05 {
        "Efficient (moderate mean-reversion)"
    } else if kappa < 0.15 {
        "Semi-efficient (weak trends possible)"
    } else if kappa < 0.30 {
        "Inefficient (trending markets)"
    } else {
        "Highly inefficient (strong trends)"
    }
}

/// Print human-readable status output
fn print_status(result: &StatusResult, verbose: bool) {
    println!("\n{}", "=".repeat(70));
    println!("                     RESEARCH STATUS: {}", result.symbol);
    println!("{}", "=".repeat(70));

    // State metadata
    println!("\n--- State Information ---");
    println!("  State ID:           {}", result.state_id);
    println!("  Timestamp:          {}", result.timestamp);
    if let Some(ref start) = result.data_start {
        println!("  Data period:        {} to {}", start, result.data_end.as_ref().unwrap_or(&"N/A".to_string()));
    }

    // MIDC Analysis
    println!("\n--- MIDC Analysis (Market Information Diffusion) ---");
    println!("  Kappa (diffusion):  {:>12.6}", result.midc_kappa);
    println!("  Tau-half (seconds): {:>12.2}", result.midc_tau_half_seconds);
    println!("  Confidence:         {:>12.2}%", result.midc_confidence * 100.0);
    println!("  Regime:             {:>12} {}", result.midc_regime, regime_indicator(&result.midc_regime));
    println!("  Interpretation:     {}", result.midc_interpretation);

    // Persistence Analysis
    println!("\n--- Persistence Analysis (Trend Duration) ---");
    println!("  Mean duration:      {:>12.2} seconds", result.persistence_mean_seconds);
    println!("  Median duration:    {:>12.2} seconds", result.persistence_median_seconds);
    println!("  Trends observed:    {:>12}", result.persistence_sample_count);
    println!("  Data reliable:      {:>12}", if result.persistence_reliable { "YES" } else { "NO" });

    // Entropy
    println!("\n--- Market Entropy ---");
    println!("  Current entropy:    {:>12.4}", result.entropy);
    println!("  Interpretation:     {}", interpret_entropy(result.entropy));

    // Top Signals
    if !result.top_signals.is_empty() {
        println!("\n--- Top Conditional Signals ({} of {}) ---", result.top_signals.len(), result.total_signals);
        for (i, sig) in result.top_signals.iter().enumerate() {
            let edge_str = if sig.edge >= 0.0 {
                format!("+{:.1}%", sig.edge * 100.0)
            } else {
                format!("{:.1}%", sig.edge * 100.0)
            };
            println!(
                "  {}. {} -> P(cont)={:.3} edge={:>6} [n={}, CI=({:.3},{:.3})]",
                i + 1,
                sig.signature,
                sig.p_continuation,
                edge_str,
                sig.sample_count,
                sig.confidence_lower,
                sig.confidence_upper
            );
        }
    } else {
        println!("\n--- No significant conditional signals found ---");
    }

    // Assessment
    println!("\n--- Tradeable Assessment ---");
    print_assessment_line("MIDC", result.assessment.midc_ok);
    print_assessment_line("Entropy", result.assessment.entropy_ok);
    print_assessment_line("Persistence", result.assessment.persistence_ok);
    print_assessment_line("Signals", result.assessment.signals_ok);
    println!("  {:-<50}", "");

    let tradeable_status = if result.assessment.is_tradeable {
        "TRADEABLE"
    } else {
        "NOT TRADEABLE"
    };
    println!("  Overall Status:     {:>12} {}", tradeable_status, tradeable_indicator(result.assessment.is_tradeable));

    // Recommendation
    println!("\n--- Recommendation ---");
    println!("  Strategy:           {}", result.assessment.recommended_strategy);
    println!("  Position Scale:     {:.0}%", result.assessment.position_scale * 100.0);
    println!("  Reasoning:          {}", result.assessment.reasoning);

    // Verbose details
    if verbose {
        println!("\n--- Verbose Details ---");
        println!("  Total signals in table: {}", result.total_signals);
        println!("  Signals with edge > 5%: {}",
            result.top_signals.iter().filter(|s| s.edge.abs() > 0.05).count());
        println!("  Signals with edge > 10%: {}",
            result.top_signals.iter().filter(|s| s.edge.abs() > 0.10).count());
    }

    println!("\n{}", "=".repeat(70));
}

/// Print assessment line with status indicator
fn print_assessment_line(name: &str, ok: bool) {
    let status = if ok { "OK" } else { "FAIL" };
    let indicator = if ok { "[+]" } else { "[-]" };
    println!("  {:18} {:>8} {}", name, status, indicator);
}

/// Get regime indicator (color-coded in spirit)
fn regime_indicator(regime: &str) -> &'static str {
    match regime {
        "SlowDiffusion" => "[FAVORABLE]",
        "ModerateDiffusion" => "[MODERATE]",
        "FastDiffusion" => "[UNFAVORABLE]",
        "Unknown" => "[UNKNOWN]",
        _ => "",
    }
}

/// Get tradeable indicator
fn tradeable_indicator(is_tradeable: bool) -> &'static str {
    if is_tradeable {
        "[+++]"
    } else {
        "[---]"
    }
}

/// Interpret entropy value
fn interpret_entropy(entropy: f64) -> &'static str {
    if entropy < 0.3 {
        "Low entropy (highly predictable)"
    } else if entropy < 0.6 {
        "Moderate entropy (somewhat predictable)"
    } else if entropy < 0.8 {
        "High entropy (less predictable)"
    } else {
        "Very high entropy (unpredictable)"
    }
}

/// Print JSON status output
fn print_status_json(result: &StatusResult) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
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
        Commands::Run(args) => {
            // Convert CLI args to params using builder
            let params = RunParamsBuilder::new()
                .with_data(args.data.clone())
                .with_output(args.output.clone())
                .with_symbol(args.symbol.clone())
                .with_start(args.start.clone())
                .with_end(args.end.clone())
                .with_min_samples(args.min_samples)
                .with_checkpoint_interval(args.checkpoint_interval)
                .with_resume(args.resume)
                .with_quiet(args.quiet)
                .with_json(args.json)
                .build()?;

            if !args.quiet && !args.json {
                println!("\nResearch Run Configuration:");
                println!("  Data directory:     {:?}", params.data);
                println!("  Output directory:   {:?}", params.output);
                println!("  Symbol:             {}", params.symbol);
                println!("  Min samples:        {}", params.min_samples);
                println!("  Checkpoint interval:{}", params.checkpoint_interval);
                if let Some(ref start) = params.start {
                    println!("  Start date:         {}", start);
                }
                if let Some(ref end) = params.end {
                    println!("  End date:           {}", end);
                }
                println!("  Resume:             {}", params.resume);
                println!();
            }

            // Use extracted command with no-op callback (progress shown via logs)
            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = ResearchCommands::run(params, callback)?;

            if args.json {
                print_json(&result)?;
            } else {
                print_summary(&result);
            }

            if !result.is_tradeable && !args.json {
                log::warn!("Market conditions not yet tradeable: {}", result.tradeable_reason);
            }
        }

        Commands::Status(args) => {
            // Convert CLI args to params using builder
            let params = StatusParamsBuilder::new()
                .with_store(args.store.clone())
                .with_symbol(args.symbol.clone())
                .with_json(args.json)
                .with_verbose(args.verbose)
                .with_top_signals(args.top_signals)
                .build()?;

            // Use extracted command with no-op callback
            let callback: Arc<dyn ingestor::commands::common::ProgressCallback> = Arc::new(NoOpCallback);
            let result = ResearchCommands::status(params, callback)?;

            if args.json {
                print_status_json(&result)?;
            } else {
                print_status(&result, args.verbose);
            }
        }
    }

    Ok(())
}

// Tests for research commands are in src/commands/research.rs


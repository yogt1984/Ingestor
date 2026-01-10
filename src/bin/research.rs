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

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ==================== Configuration Tests ====================

    #[test]
    fn test_parse_date_to_millis_valid() {
        let millis = parse_date_to_millis("2024-01-15");
        assert!(millis.is_some());
        let ms = millis.unwrap();
        // 2024-01-15 00:00:00 UTC
        assert!(ms > 0);
        // Should be roughly 1705276800000
        assert!((ms - 1705276800000).abs() < 86400000); // Within 1 day
    }

    #[test]
    fn test_parse_date_to_millis_invalid() {
        assert!(parse_date_to_millis("invalid").is_none());
        assert!(parse_date_to_millis("2024/01/15").is_none());
        assert!(parse_date_to_millis("15-01-2024").is_none());
        assert!(parse_date_to_millis("").is_none());
    }

    #[test]
    fn test_parse_date_to_millis_edge_cases() {
        // Leap year
        let leap = parse_date_to_millis("2024-02-29");
        assert!(leap.is_some());

        // Year boundaries
        let new_year = parse_date_to_millis("2024-01-01");
        assert!(new_year.is_some());

        let year_end = parse_date_to_millis("2024-12-31");
        assert!(year_end.is_some());
    }

    #[test]
    fn test_validate_config_missing_data_dir() {
        let config = ResearchRunConfig {
            data_dir: PathBuf::from("/nonexistent/path"),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_empty_symbol() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_symbol_too_long() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "A".repeat(25),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_invalid_date_range() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: Some(1705363200000), // 2024-01-16
            end_time: Some(1705276800000),   // 2024-01-15 (before start)
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_zero_min_samples() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 0,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_zero_checkpoint_interval() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 0,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_valid() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: Some(1705276800000), // 2024-01-15
            end_time: Some(1705363200000),   // 2024-01-16 (after start)
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_ok());
    }

    // ==================== MIDC Interpretation Tests ====================

    #[test]
    fn test_interpret_midc_very_efficient() {
        assert!(interpret_midc(0.005).contains("Very efficient"));
        assert!(interpret_midc(0.0).contains("Very efficient"));
    }

    #[test]
    fn test_interpret_midc_efficient() {
        assert!(interpret_midc(0.02).contains("Efficient"));
        assert!(interpret_midc(0.04).contains("Efficient"));
    }

    #[test]
    fn test_interpret_midc_semi_efficient() {
        assert!(interpret_midc(0.08).contains("Semi-efficient"));
        assert!(interpret_midc(0.14).contains("Semi-efficient"));
    }

    #[test]
    fn test_interpret_midc_inefficient() {
        assert!(interpret_midc(0.20).contains("Inefficient"));
        assert!(interpret_midc(0.28).contains("Inefficient"));
    }

    #[test]
    fn test_interpret_midc_highly_inefficient() {
        assert!(interpret_midc(0.35).contains("Highly inefficient"));
        assert!(interpret_midc(0.50).contains("Highly inefficient"));
        assert!(interpret_midc(1.0).contains("Highly inefficient"));
    }

    // ==================== Config Conversion Tests ====================

    #[test]
    fn test_run_args_to_config() {
        let args = RunArgs {
            data: PathBuf::from("./data"),
            output: PathBuf::from("./out"),
            symbol: "ETHUSDT".to_string(),
            start: Some("2024-01-15".to_string()),
            end: Some("2024-01-20".to_string()),
            min_samples: 500,
            checkpoint_interval: 5000,
            resume: true,
            quiet: true,
            json: true,
        };

        let config = ResearchRunConfig::from(&args);

        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.output_dir, PathBuf::from("./out"));
        assert_eq!(config.symbol, "ETHUSDT");
        assert!(config.start_time.is_some());
        assert!(config.end_time.is_some());
        assert_eq!(config.min_samples, 500);
        assert_eq!(config.checkpoint_interval, 5000);
        assert!(config.resume);
        assert!(config.quiet);
        assert!(config.json);
    }

    #[test]
    fn test_run_args_to_config_defaults() {
        let args = RunArgs {
            data: PathBuf::from("./data/features"),
            output: PathBuf::from("./research"),
            symbol: "BTCUSDT".to_string(),
            start: None,
            end: None,
            min_samples: 100,
            checkpoint_interval: 10000,
            resume: false,
            quiet: false,
            json: false,
        };

        let config = ResearchRunConfig::from(&args);

        assert!(config.start_time.is_none());
        assert!(config.end_time.is_none());
        assert!(!config.resume);
        assert!(!config.quiet);
        assert!(!config.json);
    }

    // ==================== SignalSummary Tests ====================

    #[test]
    fn test_signal_summary_serialization() {
        let summary = SignalSummary {
            signature: "Medium/Normal/Up/Smooth".to_string(),
            p_continuation: 0.65,
            sample_count: 150,
            confidence_lower: 0.57,
            confidence_upper: 0.72,
        };

        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("Medium/Normal/Up/Smooth"));
        assert!(json.contains("0.65"));
        assert!(json.contains("150"));
    }

    // ==================== ResearchRunResult Tests ====================

    #[test]
    fn test_research_run_result_creation() {
        let result = ResearchRunResult {
            samples_processed: 10000,
            duration_seconds: 5.5,
            midc_kappa: 0.08,
            midc_confidence: 0.95,
            midc_regime: "Normal".to_string(),
            persistence_mean_seconds: 12.5,
            persistence_sample_count: 250,
            top_signals: vec![],
            is_tradeable: true,
            tradeable_reason: "All conditions met".to_string(),
            checkpoints_saved: 2,
        };

        assert_eq!(result.samples_processed, 10000);
        assert!((result.duration_seconds - 5.5).abs() < 0.01);
        assert!((result.midc_kappa - 0.08).abs() < 0.001);
        assert!(result.is_tradeable);
    }

    #[test]
    fn test_research_run_result_not_tradeable() {
        let result = ResearchRunResult {
            samples_processed: 50,
            duration_seconds: 0.1,
            midc_kappa: 0.5,
            midc_confidence: 0.3,
            midc_regime: "Trending".to_string(),
            persistence_mean_seconds: 0.0,
            persistence_sample_count: 0,
            top_signals: vec![],
            is_tradeable: false,
            tradeable_reason: "MIDC out of range, Insufficient persistence data".to_string(),
            checkpoints_saved: 0,
        };

        assert!(!result.is_tradeable);
        assert!(result.tradeable_reason.contains("MIDC"));
        assert!(result.tradeable_reason.contains("persistence"));
    }

    // ==================== Date Range Tests ====================

    #[test]
    fn test_date_range_ordering() {
        let start = parse_date_to_millis("2024-01-01").unwrap();
        let mid = parse_date_to_millis("2024-06-15").unwrap();
        let end = parse_date_to_millis("2024-12-31").unwrap();

        assert!(start < mid);
        assert!(mid < end);
        assert!(start < end);
    }

    #[test]
    fn test_date_parsing_consistency() {
        // Parse same date twice should give same result
        let a = parse_date_to_millis("2024-03-15").unwrap();
        let b = parse_date_to_millis("2024-03-15").unwrap();
        assert_eq!(a, b);
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_data_directory_validation() {
        let temp_dir = TempDir::new().unwrap();
        // Empty directory exists but has no parquet files
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Validation should pass (directory exists)
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_special_characters_in_symbol() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTC-PERP".to_string(), // Contains hyphen
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Should be valid
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_very_large_min_samples() {
        let temp_dir = TempDir::new().unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 1_000_000,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Should be valid (just a large number)
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_same_start_end_date() {
        let temp_dir = TempDir::new().unwrap();
        let date = parse_date_to_millis("2024-06-15").unwrap();
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: Some(date),
            end_time: Some(date),
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Same date should be valid (start <= end)
        assert!(validate_config(&config).is_ok());
    }

    // ==================== Symbol Validation Tests ====================

    #[test]
    fn test_various_valid_symbols() {
        let temp_dir = TempDir::new().unwrap();

        let symbols = vec![
            "BTCUSDT",
            "ETHUSDT",
            "BTC-PERP",
            "ETH_USD",
            "1000SHIBUSDT",
            "A",
        ];

        for symbol in symbols {
            let config = ResearchRunConfig {
                data_dir: temp_dir.path().to_path_buf(),
                output_dir: PathBuf::from("./output"),
                symbol: symbol.to_string(),
                start_time: None,
                end_time: None,
                min_samples: 100,
                checkpoint_interval: 1000,
                resume: false,
                quiet: false,
                json: false,
            };
            assert!(
                validate_config(&config).is_ok(),
                "Symbol '{}' should be valid",
                symbol
            );
        }
    }

    // ==================== Throughput Calculation Tests ====================

    #[test]
    fn test_throughput_calculation() {
        let result = ResearchRunResult {
            samples_processed: 100000,
            duration_seconds: 10.0,
            midc_kappa: 0.1,
            midc_confidence: 0.9,
            midc_regime: "Normal".to_string(),
            persistence_mean_seconds: 10.0,
            persistence_sample_count: 100,
            top_signals: vec![],
            is_tradeable: true,
            tradeable_reason: "All conditions met".to_string(),
            checkpoints_saved: 10,
        };

        let throughput = result.samples_processed as f64 / result.duration_seconds;
        assert!((throughput - 10000.0).abs() < 0.01);
    }

    #[test]
    fn test_throughput_zero_duration_protection() {
        let result = ResearchRunResult {
            samples_processed: 100,
            duration_seconds: 0.0,
            midc_kappa: 0.1,
            midc_confidence: 0.9,
            midc_regime: "Normal".to_string(),
            persistence_mean_seconds: 0.0,
            persistence_sample_count: 0,
            top_signals: vec![],
            is_tradeable: false,
            tradeable_reason: "Test".to_string(),
            checkpoints_saved: 0,
        };

        // Use max(0.001) to prevent division by zero
        let throughput = result.samples_processed as f64 / result.duration_seconds.max(0.001);
        assert!(throughput.is_finite());
        assert!(throughput > 0.0);
    }

    // ==================== JSON Output Tests ====================

    #[test]
    fn test_json_output_structure() {
        let result = ResearchRunResult {
            samples_processed: 5000,
            duration_seconds: 2.5,
            midc_kappa: 0.12,
            midc_confidence: 0.88,
            midc_regime: "SemiEfficient".to_string(),
            persistence_mean_seconds: 15.3,
            persistence_sample_count: 75,
            top_signals: vec![
                SignalSummary {
                    signature: "Small/Fast/Up/Choppy".to_string(),
                    p_continuation: 0.72,
                    sample_count: 45,
                    confidence_lower: 0.58,
                    confidence_upper: 0.83,
                },
            ],
            is_tradeable: false,
            tradeable_reason: "Insufficient samples".to_string(),
            checkpoints_saved: 1,
        };

        // Serialize
        let json_str = serde_json::to_string_pretty(&serde_json::json!({
            "samples_processed": result.samples_processed,
            "duration_seconds": result.duration_seconds,
            "midc_kappa": result.midc_kappa,
            "midc_confidence": result.midc_confidence,
            "midc_regime": result.midc_regime,
            "persistence_mean_seconds": result.persistence_mean_seconds,
            "persistence_sample_count": result.persistence_sample_count,
            "top_signals": result.top_signals,
            "is_tradeable": result.is_tradeable,
            "tradeable_reason": result.tradeable_reason,
            "checkpoints_saved": result.checkpoints_saved,
        }))
        .unwrap();

        // Verify key fields are present
        assert!(json_str.contains("samples_processed"));
        assert!(json_str.contains("midc_kappa"));
        assert!(json_str.contains("is_tradeable"));
        assert!(json_str.contains("top_signals"));
    }

    // ==================== Run Args Clap Tests ====================

    #[test]
    fn test_run_args_debug() {
        let args = RunArgs {
            data: PathBuf::from("./data"),
            output: PathBuf::from("./out"),
            symbol: "BTCUSDT".to_string(),
            start: None,
            end: None,
            min_samples: 100,
            checkpoint_interval: 10000,
            resume: false,
            quiet: false,
            json: false,
        };

        // Should implement Debug
        let debug_str = format!("{:?}", args);
        assert!(debug_str.contains("BTCUSDT"));
        assert!(debug_str.contains("data"));
    }

    #[test]
    fn test_run_args_clone() {
        let args = RunArgs {
            data: PathBuf::from("./data"),
            output: PathBuf::from("./out"),
            symbol: "BTCUSDT".to_string(),
            start: Some("2024-01-01".to_string()),
            end: Some("2024-12-31".to_string()),
            min_samples: 100,
            checkpoint_interval: 10000,
            resume: true,
            quiet: true,
            json: true,
        };

        let cloned = args.clone();
        assert_eq!(cloned.symbol, args.symbol);
        assert_eq!(cloned.resume, args.resume);
        assert_eq!(cloned.start, args.start);
    }

    // ==================== Config Clone Tests ====================

    #[test]
    fn test_research_run_config_clone() {
        let config = ResearchRunConfig {
            data_dir: PathBuf::from("./data"),
            output_dir: PathBuf::from("./out"),
            symbol: "BTCUSDT".to_string(),
            start_time: Some(1000),
            end_time: Some(2000),
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: true,
            quiet: false,
            json: true,
        };

        let cloned = config.clone();
        assert_eq!(cloned.symbol, config.symbol);
        assert_eq!(cloned.start_time, config.start_time);
        assert_eq!(cloned.resume, config.resume);
    }

    // ==================== Boundary Value Tests ====================

    #[test]
    fn test_min_samples_boundary() {
        let temp_dir = TempDir::new().unwrap();

        // min_samples = 1 should be valid
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 1,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_checkpoint_interval_boundary() {
        let temp_dir = TempDir::new().unwrap();

        // checkpoint_interval = 1 should be valid
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_symbol_max_length() {
        let temp_dir = TempDir::new().unwrap();

        // 20 characters should be valid
        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "A".repeat(20),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_ok());

        // 21 characters should be invalid
        let config_too_long = ResearchRunConfig {
            symbol: "A".repeat(21),
            ..config
        };
        assert!(validate_config(&config_too_long).is_err());
    }

    // ==================== Integration-style Tests (mocked) ====================

    #[test]
    fn test_full_config_pipeline() {
        // Test the full conversion from args to config
        let args = RunArgs {
            data: PathBuf::from("./my_data"),
            output: PathBuf::from("./my_output"),
            symbol: "SOLUSDT".to_string(),
            start: Some("2024-03-01".to_string()),
            end: Some("2024-03-31".to_string()),
            min_samples: 250,
            checkpoint_interval: 2500,
            resume: true,
            quiet: false,
            json: true,
        };

        let config = ResearchRunConfig::from(&args);

        assert_eq!(config.data_dir, PathBuf::from("./my_data"));
        assert_eq!(config.output_dir, PathBuf::from("./my_output"));
        assert_eq!(config.symbol, "SOLUSDT");
        assert!(config.start_time.is_some());
        assert!(config.end_time.is_some());

        // Verify date ordering
        let start = config.start_time.unwrap();
        let end = config.end_time.unwrap();
        assert!(start < end);

        assert_eq!(config.min_samples, 250);
        assert_eq!(config.checkpoint_interval, 2500);
        assert!(config.resume);
        assert!(!config.quiet);
        assert!(config.json);
    }

    #[test]
    fn test_result_with_signals() {
        let signals = vec![
            SignalSummary {
                signature: "Large/Slow/Down/Smooth".to_string(),
                p_continuation: 0.78,
                sample_count: 200,
                confidence_lower: 0.72,
                confidence_upper: 0.84,
            },
            SignalSummary {
                signature: "Medium/Normal/Up/Choppy".to_string(),
                p_continuation: 0.55,
                sample_count: 500,
                confidence_lower: 0.51,
                confidence_upper: 0.59,
            },
        ];

        let result = ResearchRunResult {
            samples_processed: 50000,
            duration_seconds: 25.0,
            midc_kappa: 0.06,
            midc_confidence: 0.92,
            midc_regime: "Efficient".to_string(),
            persistence_mean_seconds: 8.5,
            persistence_sample_count: 450,
            top_signals: signals,
            is_tradeable: true,
            tradeable_reason: "All conditions met".to_string(),
            checkpoints_saved: 5,
        };

        assert_eq!(result.top_signals.len(), 2);
        assert_eq!(result.top_signals[0].signature, "Large/Slow/Down/Smooth");
        assert!((result.top_signals[0].p_continuation - 0.78).abs() < 0.001);
    }

    // ==================== Additional Skeptical Tests ====================

    #[test]
    fn test_interpret_midc_boundary_very_efficient() {
        // Exactly at 0.01 boundary should NOT be very efficient
        let result = interpret_midc(0.01);
        assert!(!result.contains("Very efficient"));
        assert!(result.contains("Efficient"));
    }

    #[test]
    fn test_interpret_midc_boundary_efficient() {
        // Exactly at 0.05 boundary should NOT be efficient
        let result = interpret_midc(0.05);
        assert!(!result.contains("Efficient") || result.contains("Semi"));
    }

    #[test]
    fn test_interpret_midc_boundary_semi_efficient() {
        // Exactly at 0.15 boundary should NOT be semi-efficient
        let result = interpret_midc(0.15);
        assert!(result.contains("Inefficient"));
    }

    #[test]
    fn test_interpret_midc_boundary_inefficient() {
        // Exactly at 0.30 boundary should be highly inefficient
        let result = interpret_midc(0.30);
        assert!(result.contains("Highly inefficient"));
    }

    #[test]
    fn test_interpret_midc_negative_kappa() {
        // Negative kappa (invalid but should handle gracefully)
        let result = interpret_midc(-0.05);
        assert!(result.contains("Very efficient"));
    }

    #[test]
    fn test_interpret_midc_large_kappa() {
        // Very large kappa
        let result = interpret_midc(10.0);
        assert!(result.contains("Highly inefficient"));
        let result2 = interpret_midc(100.0);
        assert!(result2.contains("Highly inefficient"));
    }

    #[test]
    fn test_parse_date_invalid_formats() {
        // Various invalid formats
        // Note: NaiveDate::parse_from_str with %Y-%m-%d is lenient with leading zeros
        // and even short years, so we test only truly invalid formats
        assert!(parse_date_to_millis("2024-13-01").is_none()); // Invalid month
        assert!(parse_date_to_millis("2024-01-32").is_none()); // Invalid day
        assert!(parse_date_to_millis("2024-02-30").is_none()); // Invalid day for Feb
        assert!(parse_date_to_millis("2023-02-29").is_none()); // Non-leap year
        assert!(parse_date_to_millis("not-a-date").is_none()); // Not a date at all
        assert!(parse_date_to_millis("").is_none()); // Empty string
        assert!(parse_date_to_millis("2024/01/15").is_none()); // Wrong separator
        assert!(parse_date_to_millis("01-15-2024").is_none()); // US format
    }

    #[test]
    fn test_parse_date_valid_leap_year() {
        // Valid leap year date
        assert!(parse_date_to_millis("2024-02-29").is_some());
        assert!(parse_date_to_millis("2020-02-29").is_some());
    }

    #[test]
    fn test_parse_date_edge_of_year() {
        let start_of_year = parse_date_to_millis("2024-01-01").unwrap();
        let end_of_year = parse_date_to_millis("2024-12-31").unwrap();

        // Year should be 365 days (2024 is leap year = 366)
        let days = (end_of_year - start_of_year) / (24 * 60 * 60 * 1000);
        assert_eq!(days, 365); // 0-indexed, so 365 days difference
    }

    #[test]
    fn test_validate_config_data_dir_is_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        std::fs::write(&file_path, "test").unwrap();

        let config = ResearchRunConfig {
            data_dir: file_path, // This is a file, not a directory
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // It exists, so validation passes (even though it's a file)
        // The ParquetReplay will fail later when trying to read
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_date_range_same_millisecond() {
        let temp_dir = TempDir::new().unwrap();
        let same_time = 1705276800000;

        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: Some(same_time),
            end_time: Some(same_time),
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Same time should be valid (start <= end)
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_date_range_one_millisecond_difference() {
        let temp_dir = TempDir::new().unwrap();

        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: Some(1705276800001),
            end_time: Some(1705276800000), // 1ms before start
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_only_start_time() {
        let temp_dir = TempDir::new().unwrap();

        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: Some(1705276800000),
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Should be valid - end time is optional
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_only_end_time() {
        let temp_dir = TempDir::new().unwrap();

        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTCUSDT".to_string(),
            start_time: None,
            end_time: Some(1705276800000),
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Should be valid - start time is optional
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_signal_summary_edge_values() {
        let summary = SignalSummary {
            signature: String::new(), // Empty signature
            p_continuation: 0.0,      // Zero probability
            sample_count: 0,          // Zero samples
            confidence_lower: 0.0,
            confidence_upper: 0.0,
        };

        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("0"));
    }

    #[test]
    fn test_signal_summary_high_values() {
        let summary = SignalSummary {
            signature: "X".repeat(1000), // Very long signature
            p_continuation: 1.0,         // Maximum probability
            sample_count: usize::MAX,    // Maximum samples
            confidence_lower: 1.0,
            confidence_upper: 1.0,
        };

        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("1.0"));
    }

    #[test]
    fn test_research_run_result_zero_duration() {
        let result = ResearchRunResult {
            samples_processed: 0,
            duration_seconds: 0.0,
            midc_kappa: 0.0,
            midc_confidence: 0.0,
            midc_regime: String::new(),
            persistence_mean_seconds: 0.0,
            persistence_sample_count: 0,
            top_signals: vec![],
            is_tradeable: false,
            tradeable_reason: String::new(),
            checkpoints_saved: 0,
        };

        // Throughput calculation should handle zero duration
        let throughput = result.samples_processed as f64 / result.duration_seconds.max(0.001);
        assert!(throughput.is_finite());
    }

    #[test]
    fn test_research_run_result_max_values() {
        let result = ResearchRunResult {
            samples_processed: usize::MAX,
            duration_seconds: f64::MAX,
            midc_kappa: f64::MAX,
            midc_confidence: f64::MAX,
            midc_regime: "X".repeat(10000),
            persistence_mean_seconds: f64::MAX,
            persistence_sample_count: usize::MAX,
            top_signals: vec![],
            is_tradeable: true,
            tradeable_reason: "X".repeat(10000),
            checkpoints_saved: usize::MAX,
        };

        assert!(result.is_tradeable);
        assert_eq!(result.midc_regime.len(), 10000);
    }

    #[test]
    fn test_config_from_args_invalid_dates() {
        let args = RunArgs {
            data: PathBuf::from("./data"),
            output: PathBuf::from("./out"),
            symbol: "BTCUSDT".to_string(),
            start: Some("invalid-date".to_string()),
            end: Some("also-invalid".to_string()),
            min_samples: 100,
            checkpoint_interval: 10000,
            resume: false,
            quiet: false,
            json: false,
        };

        let config = ResearchRunConfig::from(&args);

        // Invalid dates should result in None
        assert!(config.start_time.is_none());
        assert!(config.end_time.is_none());
    }

    #[test]
    fn test_config_with_unicode_symbol() {
        let temp_dir = TempDir::new().unwrap();

        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTC\u{20AC}USD".to_string(), // Contains unicode euro sign
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Should be valid (length in chars is what matters)
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_config_with_whitespace_symbol() {
        let temp_dir = TempDir::new().unwrap();

        let config = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "BTC USDT".to_string(), // Contains space
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        // Should be valid (we don't validate symbol format beyond length)
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_date_milliseconds_precision() {
        let date1 = parse_date_to_millis("2024-01-01").unwrap();
        let date2 = parse_date_to_millis("2024-01-02").unwrap();

        // Difference should be exactly 24 hours in milliseconds
        let diff = date2 - date1;
        assert_eq!(diff, 24 * 60 * 60 * 1000);
    }

    #[test]
    fn test_run_args_all_flags_true() {
        let args = RunArgs {
            data: PathBuf::from("./d"),
            output: PathBuf::from("./o"),
            symbol: "X".to_string(),
            start: None,
            end: None,
            min_samples: 1,
            checkpoint_interval: 1,
            resume: true,
            quiet: true,
            json: true,
        };

        let config = ResearchRunConfig::from(&args);
        assert!(config.resume);
        assert!(config.quiet);
        assert!(config.json);
    }

    #[test]
    fn test_run_args_all_flags_false() {
        let args = RunArgs {
            data: PathBuf::from("./d"),
            output: PathBuf::from("./o"),
            symbol: "X".to_string(),
            start: None,
            end: None,
            min_samples: 1,
            checkpoint_interval: 1,
            resume: false,
            quiet: false,
            json: false,
        };

        let config = ResearchRunConfig::from(&args);
        assert!(!config.resume);
        assert!(!config.quiet);
        assert!(!config.json);
    }

    #[test]
    fn test_tradeable_reasons_combinations() {
        // Test various combinations of tradeable reason flags
        let combinations = vec![
            (true, true, true, true, true),   // All ok
            (false, true, true, true, false), // MIDC not ok
            (true, false, true, true, false), // Persistence not ok
            (true, true, false, true, false), // Entropy not ok
            (true, true, true, false, false), // Signals not ok
            (false, false, false, false, false), // None ok
        ];

        for (midc, pers, ent, sig, expected) in combinations {
            let is_tradeable = midc && pers && ent && sig;
            assert_eq!(is_tradeable, expected);
        }
    }

    #[test]
    fn test_midc_kappa_nan_handling() {
        // NaN should result in "Very efficient" due to comparison behavior
        let result = interpret_midc(f64::NAN);
        // NaN comparisons are always false, so it falls through to the last case
        assert!(result.contains("Highly inefficient"));
    }

    #[test]
    fn test_midc_kappa_infinity() {
        let result = interpret_midc(f64::INFINITY);
        assert!(result.contains("Highly inefficient"));

        let result_neg = interpret_midc(f64::NEG_INFINITY);
        assert!(result_neg.contains("Very efficient"));
    }

    #[test]
    fn test_symbol_validation_edge_cases() {
        let temp_dir = TempDir::new().unwrap();

        // Single character
        let config_one = ResearchRunConfig {
            data_dir: temp_dir.path().to_path_buf(),
            output_dir: PathBuf::from("./output"),
            symbol: "X".to_string(),
            start_time: None,
            end_time: None,
            min_samples: 100,
            checkpoint_interval: 1000,
            resume: false,
            quiet: false,
            json: false,
        };
        assert!(validate_config(&config_one).is_ok());

        // Exactly 20 characters (boundary)
        let config_20 = ResearchRunConfig {
            symbol: "A".repeat(20),
            ..config_one.clone()
        };
        assert!(validate_config(&config_20).is_ok());
    }

    // ==================== Status Command Tests (Task 1.7) ====================

    // --- StatusConfig Tests ---

    #[test]
    fn test_status_config_from_args() {
        let args = StatusArgs {
            store: PathBuf::from("./my_store"),
            symbol: "ETHUSDT".to_string(),
            json: true,
            verbose: true,
            top_signals: 10,
        };

        let config = StatusConfig::from(&args);
        assert_eq!(config.store_path, PathBuf::from("./my_store"));
        assert_eq!(config.symbol, "ETHUSDT");
        assert!(config.json);
        assert!(config.verbose);
        assert_eq!(config.top_signals, 10);
    }

    #[test]
    fn test_status_config_from_args_defaults() {
        let args = StatusArgs {
            store: PathBuf::from("./research"),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 5,
        };

        let config = StatusConfig::from(&args);
        assert!(!config.json);
        assert!(!config.verbose);
        assert_eq!(config.top_signals, 5);
    }

    #[test]
    fn test_status_config_clone() {
        let config = StatusConfig {
            store_path: PathBuf::from("./test"),
            symbol: "TEST".to_string(),
            json: true,
            verbose: false,
            top_signals: 3,
        };

        let cloned = config.clone();
        assert_eq!(cloned.store_path, config.store_path);
        assert_eq!(cloned.symbol, config.symbol);
        assert_eq!(cloned.json, config.json);
        assert_eq!(cloned.verbose, config.verbose);
        assert_eq!(cloned.top_signals, config.top_signals);
    }

    #[test]
    fn test_status_config_debug() {
        let config = StatusConfig {
            store_path: PathBuf::from("./debug_test"),
            symbol: "DEBUGUSDT".to_string(),
            json: false,
            verbose: true,
            top_signals: 7,
        };

        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("debug_test"));
        assert!(debug_str.contains("DEBUGUSDT"));
    }

    // --- Status Validation Tests ---

    #[test]
    fn test_validate_status_config_missing_store() {
        let config = StatusConfig {
            store_path: PathBuf::from("/nonexistent/path/to/store"),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 5,
        };
        assert!(validate_status_config(&config).is_err());
    }

    #[test]
    fn test_validate_status_config_empty_symbol() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "".to_string(),
            json: false,
            verbose: false,
            top_signals: 5,
        };
        assert!(validate_status_config(&config).is_err());
    }

    #[test]
    fn test_validate_status_config_symbol_too_long() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "A".repeat(25),
            json: false,
            verbose: false,
            top_signals: 5,
        };
        assert!(validate_status_config(&config).is_err());
    }

    #[test]
    fn test_validate_status_config_symbol_max_length() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "A".repeat(20),
            json: false,
            verbose: false,
            top_signals: 5,
        };
        assert!(validate_status_config(&config).is_ok());
    }

    #[test]
    fn test_validate_status_config_zero_top_signals() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 0,
        };
        assert!(validate_status_config(&config).is_err());
    }

    #[test]
    fn test_validate_status_config_top_signals_too_large() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 101,
        };
        assert!(validate_status_config(&config).is_err());
    }

    #[test]
    fn test_validate_status_config_top_signals_max() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 100,
        };
        assert!(validate_status_config(&config).is_ok());
    }

    #[test]
    fn test_validate_status_config_valid() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: true,
            verbose: true,
            top_signals: 10,
        };
        assert!(validate_status_config(&config).is_ok());
    }

    // --- StatusResult Tests ---

    #[test]
    fn test_status_result_serialization() {
        let result = StatusResult {
            symbol: "BTCUSDT".to_string(),
            state_id: "test-id-123".to_string(),
            timestamp: "2024-01-15T12:00:00Z".to_string(),
            data_start: Some("2024-01-01T00:00:00Z".to_string()),
            data_end: Some("2024-01-15T12:00:00Z".to_string()),
            midc_kappa: 0.05,
            midc_confidence: 0.85,
            midc_tau_half_seconds: 13.86,
            midc_regime: "ModerateDiffusion".to_string(),
            midc_interpretation: "Semi-efficient (weak trends possible)".to_string(),
            persistence_mean_seconds: 15.5,
            persistence_median_seconds: 12.0,
            persistence_sample_count: 150,
            persistence_reliable: true,
            entropy: 0.45,
            top_signals: vec![],
            total_signals: 0,
            assessment: StatusAssessment {
                midc_ok: true,
                entropy_ok: true,
                persistence_ok: true,
                signals_ok: false,
                is_tradeable: false,
                recommended_strategy: "MarketMaking".to_string(),
                position_scale: 0.5,
                reasoning: "No high-edge conditional signals".to_string(),
            },
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("BTCUSDT"));
        assert!(json.contains("test-id-123"));
        assert!(json.contains("ModerateDiffusion"));
        assert!(json.contains("MarketMaking"));
    }

    #[test]
    fn test_status_result_with_signals() {
        let result = StatusResult {
            symbol: "ETHUSDT".to_string(),
            state_id: "eth-123".to_string(),
            timestamp: "2024-06-15T18:30:00Z".to_string(),
            data_start: None,
            data_end: None,
            midc_kappa: 0.02,
            midc_confidence: 0.92,
            midc_tau_half_seconds: 34.66,
            midc_regime: "SlowDiffusion".to_string(),
            midc_interpretation: "Efficient (moderate mean-reversion)".to_string(),
            persistence_mean_seconds: 25.0,
            persistence_median_seconds: 20.0,
            persistence_sample_count: 500,
            persistence_reliable: true,
            entropy: 0.35,
            top_signals: vec![
                StatusSignal {
                    signature: "Medium_Normal_Up_Smooth".to_string(),
                    p_continuation: 0.72,
                    sample_count: 250,
                    edge: 0.22,
                    confidence_lower: 0.66,
                    confidence_upper: 0.78,
                },
                StatusSignal {
                    signature: "Large_Fast_Down_Choppy".to_string(),
                    p_continuation: 0.38,
                    sample_count: 100,
                    edge: -0.12,
                    confidence_lower: 0.29,
                    confidence_upper: 0.48,
                },
            ],
            total_signals: 50,
            assessment: StatusAssessment {
                midc_ok: true,
                entropy_ok: true,
                persistence_ok: true,
                signals_ok: true,
                is_tradeable: true,
                recommended_strategy: "Momentum".to_string(),
                position_scale: 1.0,
                reasoning: "All conditions favorable for momentum trading".to_string(),
            },
        };

        assert_eq!(result.top_signals.len(), 2);
        assert!((result.top_signals[0].edge - 0.22).abs() < 0.001);
        assert!(result.assessment.is_tradeable);
    }

    // --- StatusSignal Tests ---

    #[test]
    fn test_status_signal_positive_edge() {
        let signal = StatusSignal {
            signature: "Large_Slow_Up_Smooth".to_string(),
            p_continuation: 0.75,
            sample_count: 200,
            edge: 0.25,
            confidence_lower: 0.69,
            confidence_upper: 0.81,
        };

        assert!(signal.edge > 0.0);
        assert!((signal.p_continuation - 0.5 - signal.edge).abs() < 0.001);
    }

    #[test]
    fn test_status_signal_negative_edge() {
        let signal = StatusSignal {
            signature: "Tiny_Fast_Down_Choppy".to_string(),
            p_continuation: 0.35,
            sample_count: 150,
            edge: -0.15,
            confidence_lower: 0.28,
            confidence_upper: 0.42,
        };

        assert!(signal.edge < 0.0);
        assert!((signal.p_continuation - 0.5 - signal.edge).abs() < 0.001);
    }

    #[test]
    fn test_status_signal_zero_edge() {
        let signal = StatusSignal {
            signature: "Medium_Normal_Up_Mixed".to_string(),
            p_continuation: 0.50,
            sample_count: 1000,
            edge: 0.0,
            confidence_lower: 0.47,
            confidence_upper: 0.53,
        };

        assert!((signal.edge).abs() < 0.001);
    }

    #[test]
    fn test_status_signal_serialization() {
        let signal = StatusSignal {
            signature: "VeryLarge_Slow_Down_Smooth".to_string(),
            p_continuation: 0.85,
            sample_count: 50,
            edge: 0.35,
            confidence_lower: 0.73,
            confidence_upper: 0.93,
        };

        let json = serde_json::to_string(&signal).unwrap();
        assert!(json.contains("VeryLarge_Slow_Down_Smooth"));
        assert!(json.contains("0.85"));
        assert!(json.contains("0.35"));
    }

    // --- StatusAssessment Tests ---

    #[test]
    fn test_status_assessment_all_ok() {
        let assessment = StatusAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: "Momentum".to_string(),
            position_scale: 1.0,
            reasoning: "All conditions met".to_string(),
        };

        assert!(assessment.is_tradeable);
        assert_eq!(assessment.recommended_strategy, "Momentum");
    }

    #[test]
    fn test_status_assessment_midc_not_ok() {
        let assessment = StatusAssessment {
            midc_ok: false,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: false,
            recommended_strategy: "MarketMaking".to_string(),
            position_scale: 0.5,
            reasoning: "MIDC too high".to_string(),
        };

        assert!(!assessment.is_tradeable);
        assert!(!assessment.midc_ok);
    }

    #[test]
    fn test_status_assessment_none_ok() {
        let assessment = StatusAssessment {
            midc_ok: false,
            entropy_ok: false,
            persistence_ok: false,
            signals_ok: false,
            is_tradeable: false,
            recommended_strategy: "None".to_string(),
            position_scale: 0.0,
            reasoning: "All conditions failed".to_string(),
        };

        assert!(!assessment.is_tradeable);
        assert_eq!(assessment.position_scale, 0.0);
    }

    #[test]
    fn test_status_assessment_serialization() {
        let assessment = StatusAssessment {
            midc_ok: true,
            entropy_ok: false,
            persistence_ok: true,
            signals_ok: false,
            is_tradeable: false,
            recommended_strategy: "Hybrid".to_string(),
            position_scale: 0.25,
            reasoning: "Entropy too high; No signals".to_string(),
        };

        let json = serde_json::to_string(&assessment).unwrap();
        assert!(json.contains("Hybrid"));
        assert!(json.contains("0.25"));
        assert!(json.contains("Entropy too high"));
    }

    // --- Entropy Interpretation Tests ---

    #[test]
    fn test_interpret_entropy_low() {
        let result = interpret_entropy(0.2);
        assert!(result.contains("Low entropy"));
        assert!(result.contains("highly predictable"));
    }

    #[test]
    fn test_interpret_entropy_moderate() {
        let result = interpret_entropy(0.45);
        assert!(result.contains("Moderate entropy"));
    }

    #[test]
    fn test_interpret_entropy_high() {
        let result = interpret_entropy(0.7);
        assert!(result.contains("High entropy"));
        assert!(result.contains("less predictable"));
    }

    #[test]
    fn test_interpret_entropy_very_high() {
        let result = interpret_entropy(0.9);
        assert!(result.contains("Very high entropy"));
        assert!(result.contains("unpredictable"));
    }

    #[test]
    fn test_interpret_entropy_boundaries() {
        // Exactly at boundaries
        assert!(interpret_entropy(0.3).contains("Moderate")); // 0.3 is NOT < 0.3
        assert!(interpret_entropy(0.6).contains("High"));     // 0.6 is NOT < 0.6
        assert!(interpret_entropy(0.8).contains("Very high")); // 0.8 is NOT < 0.8
    }

    #[test]
    fn test_interpret_entropy_edge_cases() {
        // Zero entropy
        assert!(interpret_entropy(0.0).contains("Low entropy"));

        // Negative entropy (invalid but handle gracefully)
        assert!(interpret_entropy(-0.5).contains("Low entropy"));

        // Very high entropy
        assert!(interpret_entropy(1.0).contains("Very high entropy"));
        assert!(interpret_entropy(10.0).contains("Very high entropy"));
    }

    // --- Regime Indicator Tests ---

    #[test]
    fn test_regime_indicator_slow_diffusion() {
        assert!(regime_indicator("SlowDiffusion").contains("FAVORABLE"));
    }

    #[test]
    fn test_regime_indicator_moderate_diffusion() {
        assert!(regime_indicator("ModerateDiffusion").contains("MODERATE"));
    }

    #[test]
    fn test_regime_indicator_fast_diffusion() {
        assert!(regime_indicator("FastDiffusion").contains("UNFAVORABLE"));
    }

    #[test]
    fn test_regime_indicator_unknown() {
        assert!(regime_indicator("Unknown").contains("UNKNOWN"));
    }

    #[test]
    fn test_regime_indicator_invalid() {
        // Unrecognized regime should return empty string
        assert_eq!(regime_indicator("InvalidRegime"), "");
        assert_eq!(regime_indicator(""), "");
        assert_eq!(regime_indicator("SomethingElse"), "");
    }

    // --- Tradeable Indicator Tests ---

    #[test]
    fn test_tradeable_indicator_true() {
        assert!(tradeable_indicator(true).contains("+++"));
    }

    #[test]
    fn test_tradeable_indicator_false() {
        assert!(tradeable_indicator(false).contains("---"));
    }

    // --- StatusArgs Tests ---

    #[test]
    fn test_status_args_debug() {
        let args = StatusArgs {
            store: PathBuf::from("./debug_store"),
            symbol: "DEBUGSYMBOL".to_string(),
            json: true,
            verbose: false,
            top_signals: 15,
        };

        let debug_str = format!("{:?}", args);
        assert!(debug_str.contains("debug_store"));
        assert!(debug_str.contains("DEBUGSYMBOL"));
        assert!(debug_str.contains("15"));
    }

    #[test]
    fn test_status_args_clone() {
        let args = StatusArgs {
            store: PathBuf::from("./clone_store"),
            symbol: "CLONESYM".to_string(),
            json: false,
            verbose: true,
            top_signals: 8,
        };

        let cloned = args.clone();
        assert_eq!(cloned.store, args.store);
        assert_eq!(cloned.symbol, args.symbol);
        assert_eq!(cloned.json, args.json);
        assert_eq!(cloned.verbose, args.verbose);
        assert_eq!(cloned.top_signals, args.top_signals);
    }

    // --- Edge Value Tests for Status ---

    #[test]
    fn test_status_signal_max_values() {
        let signal = StatusSignal {
            signature: "X".repeat(1000),
            p_continuation: 1.0,
            sample_count: usize::MAX,
            edge: 0.5,
            confidence_lower: 1.0,
            confidence_upper: 1.0,
        };

        assert_eq!(signal.p_continuation, 1.0);
        assert_eq!(signal.edge, 0.5);
    }

    #[test]
    fn test_status_signal_min_values() {
        let signal = StatusSignal {
            signature: "".to_string(),
            p_continuation: 0.0,
            sample_count: 0,
            edge: -0.5,
            confidence_lower: 0.0,
            confidence_upper: 0.0,
        };

        assert_eq!(signal.p_continuation, 0.0);
        assert_eq!(signal.edge, -0.5);
    }

    #[test]
    fn test_status_result_empty_signals() {
        let result = StatusResult {
            symbol: "EMPTY".to_string(),
            state_id: "empty-id".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            data_start: None,
            data_end: None,
            midc_kappa: 0.0,
            midc_confidence: 0.0,
            midc_tau_half_seconds: f64::INFINITY,
            midc_regime: "Unknown".to_string(),
            midc_interpretation: "Very efficient (strong mean-reversion)".to_string(),
            persistence_mean_seconds: 0.0,
            persistence_median_seconds: 0.0,
            persistence_sample_count: 0,
            persistence_reliable: false,
            entropy: 0.0,
            top_signals: vec![],
            total_signals: 0,
            assessment: StatusAssessment {
                midc_ok: false,
                entropy_ok: false,
                persistence_ok: false,
                signals_ok: false,
                is_tradeable: false,
                recommended_strategy: "None".to_string(),
                position_scale: 0.0,
                reasoning: "No data".to_string(),
            },
        };

        assert!(result.top_signals.is_empty());
        assert_eq!(result.total_signals, 0);
        assert!(!result.assessment.is_tradeable);
    }

    #[test]
    fn test_status_assessment_boundary_position_scale() {
        // Zero position scale
        let assessment_zero = StatusAssessment {
            midc_ok: false,
            entropy_ok: false,
            persistence_ok: false,
            signals_ok: false,
            is_tradeable: false,
            recommended_strategy: "None".to_string(),
            position_scale: 0.0,
            reasoning: "Test".to_string(),
        };
        assert_eq!(assessment_zero.position_scale, 0.0);

        // Full position scale
        let assessment_full = StatusAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: "Momentum".to_string(),
            position_scale: 1.0,
            reasoning: "Test".to_string(),
        };
        assert_eq!(assessment_full.position_scale, 1.0);
    }

    // --- Symbol Validation for Status ---

    #[test]
    fn test_validate_status_config_various_symbols() {
        let temp_dir = TempDir::new().unwrap();

        let valid_symbols = vec![
            "BTCUSDT", "ETHUSDT", "BTC-PERP", "ETH_USD", "1000SHIBUSDT", "A",
        ];

        for symbol in valid_symbols {
            let config = StatusConfig {
                store_path: temp_dir.path().to_path_buf(),
                symbol: symbol.to_string(),
                json: false,
                verbose: false,
                top_signals: 5,
            };
            assert!(
                validate_status_config(&config).is_ok(),
                "Symbol '{}' should be valid",
                symbol
            );
        }
    }

    #[test]
    fn test_validate_status_config_unicode_symbol() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTC\u{20AC}USD".to_string(), // Euro sign
            json: false,
            verbose: false,
            top_signals: 5,
        };
        assert!(validate_status_config(&config).is_ok());
    }

    #[test]
    fn test_validate_status_config_whitespace_symbol() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTC USDT".to_string(), // Space
            json: false,
            verbose: false,
            top_signals: 5,
        };
        // We allow whitespace in symbols (no format validation)
        assert!(validate_status_config(&config).is_ok());
    }

    // --- Top Signals Boundary Tests ---

    #[test]
    fn test_validate_status_config_top_signals_one() {
        let temp_dir = TempDir::new().unwrap();
        let config = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 1,
        };
        assert!(validate_status_config(&config).is_ok());
    }

    #[test]
    fn test_validate_status_config_top_signals_boundary() {
        let temp_dir = TempDir::new().unwrap();

        // 100 should be valid
        let config_100 = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 100,
        };
        assert!(validate_status_config(&config_100).is_ok());

        // 101 should be invalid
        let config_101 = StatusConfig {
            store_path: temp_dir.path().to_path_buf(),
            symbol: "BTCUSDT".to_string(),
            json: false,
            verbose: false,
            top_signals: 101,
        };
        assert!(validate_status_config(&config_101).is_err());
    }

    // --- NaN/Infinity Edge Cases ---

    #[test]
    fn test_interpret_entropy_nan() {
        // NaN entropy - falls through all comparisons
        let result = interpret_entropy(f64::NAN);
        assert!(result.contains("Very high entropy"));
    }

    #[test]
    fn test_interpret_entropy_infinity() {
        let result = interpret_entropy(f64::INFINITY);
        assert!(result.contains("Very high entropy"));

        let result_neg = interpret_entropy(f64::NEG_INFINITY);
        assert!(result_neg.contains("Low entropy"));
    }

    #[test]
    fn test_status_result_with_nan_values() {
        let result = StatusResult {
            symbol: "NAN".to_string(),
            state_id: "nan-id".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            data_start: None,
            data_end: None,
            midc_kappa: f64::NAN,
            midc_confidence: f64::NAN,
            midc_tau_half_seconds: f64::NAN,
            midc_regime: "Unknown".to_string(),
            midc_interpretation: "Unknown".to_string(),
            persistence_mean_seconds: f64::NAN,
            persistence_median_seconds: f64::NAN,
            persistence_sample_count: 0,
            persistence_reliable: false,
            entropy: f64::NAN,
            top_signals: vec![],
            total_signals: 0,
            assessment: StatusAssessment {
                midc_ok: false,
                entropy_ok: false,
                persistence_ok: false,
                signals_ok: false,
                is_tradeable: false,
                recommended_strategy: "None".to_string(),
                position_scale: 0.0,
                reasoning: "Invalid data".to_string(),
            },
        };

        // Should still be serializable
        let json = serde_json::to_string(&result);
        assert!(json.is_ok());
    }

    // --- Integration-style Tests ---

    #[test]
    fn test_full_status_config_pipeline() {
        let temp_dir = TempDir::new().unwrap();
        let args = StatusArgs {
            store: temp_dir.path().to_path_buf(),
            symbol: "SOLUSDT".to_string(),
            json: true,
            verbose: true,
            top_signals: 20,
        };

        let config = StatusConfig::from(&args);

        assert_eq!(config.store_path, temp_dir.path());
        assert_eq!(config.symbol, "SOLUSDT");
        assert!(config.json);
        assert!(config.verbose);
        assert_eq!(config.top_signals, 20);

        // Validation should pass
        assert!(validate_status_config(&config).is_ok());
    }

    #[test]
    fn test_status_result_json_round_trip() {
        let original = StatusResult {
            symbol: "ROUNDTRIP".to_string(),
            state_id: "rt-123".to_string(),
            timestamp: "2024-06-15T12:00:00Z".to_string(),
            data_start: Some("2024-01-01T00:00:00Z".to_string()),
            data_end: Some("2024-06-15T12:00:00Z".to_string()),
            midc_kappa: 0.045,
            midc_confidence: 0.88,
            midc_tau_half_seconds: 15.4,
            midc_regime: "ModerateDiffusion".to_string(),
            midc_interpretation: "Semi-efficient".to_string(),
            persistence_mean_seconds: 18.5,
            persistence_median_seconds: 15.0,
            persistence_sample_count: 300,
            persistence_reliable: true,
            entropy: 0.42,
            top_signals: vec![
                StatusSignal {
                    signature: "Test_Signal".to_string(),
                    p_continuation: 0.65,
                    sample_count: 100,
                    edge: 0.15,
                    confidence_lower: 0.56,
                    confidence_upper: 0.74,
                },
            ],
            total_signals: 25,
            assessment: StatusAssessment {
                midc_ok: true,
                entropy_ok: true,
                persistence_ok: true,
                signals_ok: true,
                is_tradeable: true,
                recommended_strategy: "TSMOM".to_string(),
                position_scale: 0.8,
                reasoning: "Good conditions".to_string(),
            },
        };

        // Serialize
        let json = serde_json::to_string(&original).unwrap();

        // Deserialize
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify key fields
        assert_eq!(parsed["symbol"], "ROUNDTRIP");
        assert_eq!(parsed["state_id"], "rt-123");
        assert!((parsed["midc_kappa"].as_f64().unwrap() - 0.045).abs() < 0.001);
        assert_eq!(parsed["assessment"]["is_tradeable"], true);
        assert_eq!(parsed["top_signals"].as_array().unwrap().len(), 1);
    }

    // --- Comprehensive Assessment Combinations ---

    #[test]
    fn test_assessment_all_combinations() {
        // Test all 16 combinations of 4 boolean flags
        let combinations = [
            (false, false, false, false),
            (false, false, false, true),
            (false, false, true, false),
            (false, false, true, true),
            (false, true, false, false),
            (false, true, false, true),
            (false, true, true, false),
            (false, true, true, true),
            (true, false, false, false),
            (true, false, false, true),
            (true, false, true, false),
            (true, false, true, true),
            (true, true, false, false),
            (true, true, false, true),
            (true, true, true, false),
            (true, true, true, true),
        ];

        for (midc, entropy, persistence, signals) in combinations {
            let expected_tradeable = midc && entropy && persistence && signals;

            let assessment = StatusAssessment {
                midc_ok: midc,
                entropy_ok: entropy,
                persistence_ok: persistence,
                signals_ok: signals,
                is_tradeable: expected_tradeable,
                recommended_strategy: if expected_tradeable {
                    "Momentum".to_string()
                } else {
                    "None".to_string()
                },
                position_scale: if expected_tradeable { 1.0 } else { 0.0 },
                reasoning: "Test".to_string(),
            };

            assert_eq!(
                assessment.is_tradeable, expected_tradeable,
                "Failed for combination: midc={}, entropy={}, persistence={}, signals={}",
                midc, entropy, persistence, signals
            );
        }
    }

    // --- Skeptical Edge Case Tests ---

    #[test]
    fn test_status_result_very_long_reasoning() {
        let long_reasoning = "X".repeat(10000);
        let assessment = StatusAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: "Momentum".to_string(),
            position_scale: 1.0,
            reasoning: long_reasoning.clone(),
        };

        assert_eq!(assessment.reasoning.len(), 10000);

        // Should still serialize
        let json = serde_json::to_string(&assessment).unwrap();
        assert!(json.contains(&"X".repeat(100)));
    }

    #[test]
    fn test_status_signal_very_small_confidence_interval() {
        let signal = StatusSignal {
            signature: "Precise_Signal".to_string(),
            p_continuation: 0.70,
            sample_count: 10000,
            edge: 0.20,
            confidence_lower: 0.698,
            confidence_upper: 0.702,
        };

        let ci_width = signal.confidence_upper - signal.confidence_lower;
        assert!(ci_width < 0.01);
        assert!(signal.p_continuation >= signal.confidence_lower);
        assert!(signal.p_continuation <= signal.confidence_upper);
    }

    #[test]
    fn test_status_signal_inverted_confidence_interval() {
        // Edge case: CI lower > upper (invalid but should handle)
        let signal = StatusSignal {
            signature: "Inverted_CI".to_string(),
            p_continuation: 0.50,
            sample_count: 10,
            edge: 0.0,
            confidence_lower: 0.8, // Wrong: lower > upper
            confidence_upper: 0.2,
        };

        // Should still serialize (no validation on construction)
        let json = serde_json::to_string(&signal).unwrap();
        assert!(json.contains("Inverted_CI"));
    }

    #[test]
    fn test_multiple_signals_sorting_by_edge() {
        let signals = vec![
            StatusSignal {
                signature: "A".to_string(),
                p_continuation: 0.55,
                sample_count: 100,
                edge: 0.05,
                confidence_lower: 0.48,
                confidence_upper: 0.62,
            },
            StatusSignal {
                signature: "B".to_string(),
                p_continuation: 0.30,
                sample_count: 100,
                edge: -0.20,
                confidence_lower: 0.22,
                confidence_upper: 0.38,
            },
            StatusSignal {
                signature: "C".to_string(),
                p_continuation: 0.65,
                sample_count: 100,
                edge: 0.15,
                confidence_lower: 0.57,
                confidence_upper: 0.73,
            },
        ];

        let mut sorted = signals.clone();
        sorted.sort_by(|a, b| {
            b.edge.abs().partial_cmp(&a.edge.abs()).unwrap_or(std::cmp::Ordering::Equal)
        });

        // B should be first (edge -0.20, abs = 0.20)
        // C should be second (edge 0.15, abs = 0.15)
        // A should be third (edge 0.05, abs = 0.05)
        assert_eq!(sorted[0].signature, "B");
        assert_eq!(sorted[1].signature, "C");
        assert_eq!(sorted[2].signature, "A");
    }

    #[test]
    fn test_status_result_clone() {
        let original = StatusResult {
            symbol: "CLONE".to_string(),
            state_id: "clone-id".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            data_start: None,
            data_end: None,
            midc_kappa: 0.05,
            midc_confidence: 0.9,
            midc_tau_half_seconds: 13.86,
            midc_regime: "ModerateDiffusion".to_string(),
            midc_interpretation: "Semi-efficient".to_string(),
            persistence_mean_seconds: 15.0,
            persistence_median_seconds: 12.0,
            persistence_sample_count: 100,
            persistence_reliable: true,
            entropy: 0.4,
            top_signals: vec![],
            total_signals: 0,
            assessment: StatusAssessment {
                midc_ok: true,
                entropy_ok: true,
                persistence_ok: true,
                signals_ok: true,
                is_tradeable: true,
                recommended_strategy: "Momentum".to_string(),
                position_scale: 1.0,
                reasoning: "Good".to_string(),
            },
        };

        let cloned = original.clone();
        assert_eq!(cloned.symbol, original.symbol);
        assert_eq!(cloned.midc_kappa, original.midc_kappa);
        assert_eq!(cloned.assessment.is_tradeable, original.assessment.is_tradeable);
    }
}

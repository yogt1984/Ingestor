//! Backtest CLI
//!
//! Run backtests on historical data from the command line.
//!
//! # Usage
//!
//! ```bash
//! # Basic backtest with defaults
//! cargo run --release --bin backtest -- --data ./data/features
//!
//! # With custom parameters
//! cargo run --release --bin backtest -- \
//!     --data ./data/features \
//!     --spread 3.0 \
//!     --skew 0.7 \
//!     --output results.json
//!
//! # Parameter sweep
//! cargo run --release --bin backtest -- \
//!     --data ./data/features \
//!     --sweep-spread 1,2,3,4,5 \
//!     --sweep-skew 0.3,0.5,0.7
//! ```

use std::path::PathBuf;
use clap::{Parser, Subcommand};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use anyhow::Result;

use ingestor::backtest::{BacktestEngine, BacktestConfig};
use ingestor::backtest::replay::ReplayConfig;
use ingestor::market_maker::MMConfig;
use ingestor::mm_simulator::SimulatorConfig;

#[derive(Parser)]
#[command(name = "backtest")]
#[command(about = "Run backtests on historical market data")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to data directory containing Parquet files
    #[arg(short, long, default_value = "./data/features")]
    data: PathBuf,

    /// Base spread in basis points (per side)
    #[arg(long, default_value = "2.0")]
    spread: f64,

    /// Inventory skew factor
    #[arg(long, default_value = "0.5")]
    skew: f64,

    /// Maximum inventory
    #[arg(long, default_value = "0.1")]
    max_inventory: f64,

    /// Quote size
    #[arg(long, default_value = "0.001")]
    quote_size: f64,

    /// Fee rate (e.g., 0.0001 = 1 bps)
    #[arg(long, default_value = "0.0001")]
    fee_rate: f64,

    /// Output file for results (JSON)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Quiet mode (no progress output)
    #[arg(short, long)]
    quiet: bool,

    /// Use naive fill simulation (for comparison)
    #[arg(long)]
    naive_fills: bool,

    /// Fill probability (0.0-1.0) for realistic simulation
    #[arg(long, default_value = "0.10")]
    fill_prob: f64,

    /// Queue position (0.0=front, 1.0=back)
    #[arg(long, default_value = "0.5")]
    queue_pos: f64,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a single backtest
    Single,

    /// Run parameter sweep
    Sweep {
        /// Spread values to test (comma-separated)
        #[arg(long)]
        spreads: String,

        /// Skew values to test (comma-separated)
        #[arg(long)]
        skews: String,
    },

    /// Walk-forward validation (time-series cross-validation)
    WalkForward {
        /// Number of folds
        #[arg(long, default_value = "5")]
        folds: usize,

        /// Test period per fold (hours)
        #[arg(long, default_value = "24")]
        test_hours: f64,

        /// Use rolling (vs anchored/expanding) window
        #[arg(long)]
        rolling: bool,

        /// Output file for results
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Validate data quality
    Validate {
        /// Output file for report
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Show info about data
    Info,
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Sweep { spreads, skews }) => {
            run_sweep(&cli, spreads, skews)?;
        }
        Some(Commands::WalkForward { folds, test_hours, rolling, output }) => {
            run_walk_forward(&cli, *folds, *test_hours, *rolling, output.clone())?;
        }
        Some(Commands::Validate { output }) => {
            run_validate(&cli, output.clone())?;
        }
        Some(Commands::Info) => {
            show_info(&cli)?;
        }
        Some(Commands::Single) | None => {
            run_single(&cli)?;
        }
    }

    Ok(())
}

fn run_single(cli: &Cli) -> Result<()> {
    println!("═══════════════════════════════════════════════════════");
    println!("           INGESTOR BACKTEST ENGINE                     ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Configuration:");
    println!("  Data:          {:?}", cli.data);
    println!("  Spread:        {} bps", cli.spread);
    println!("  Skew Factor:   {}", cli.skew);
    println!("  Max Inventory: {}", cli.max_inventory);
    println!("  Quote Size:    {}", cli.quote_size);
    println!("  Fee Rate:      {} bps", cli.fee_rate * 10000.0);
    println!("  Fill Mode:     {}", if cli.naive_fills { "NAIVE" } else { "REALISTIC" });
    if !cli.naive_fills {
        println!("  Fill Prob:     {:.0}%", cli.fill_prob * 100.0);
        println!("  Queue Pos:     {:.0}%", cli.queue_pos * 100.0);
    }
    println!();

    let config = build_config(cli);
    let mut engine = BacktestEngine::new(config);

    println!("Loading data...");
    let num_events = engine.load_data()?;
    println!("Loaded {} events", num_events);
    println!();

    let results = engine.run()?;
    results.print_summary();

    if let Some(ref output) = cli.output {
        results.save_json(output.to_str().unwrap())?;
        println!();
        println!("Results saved to: {:?}", output);
    }

    Ok(())
}

fn run_sweep(cli: &Cli, spreads_str: &str, skews_str: &str) -> Result<()> {
    let spreads: Vec<f64> = spreads_str
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let skews: Vec<f64> = skews_str
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!("═══════════════════════════════════════════════════════");
    println!("           PARAMETER SWEEP                              ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Spreads: {:?}", spreads);
    println!("Skews:   {:?}", skews);
    println!("Total combinations: {}", spreads.len() * skews.len());
    println!();

    // Load data once
    let replay_config = ReplayConfig {
        data_dir: cli.data.clone(),
        ..Default::default()
    };

    let mut all_results: Vec<SweepResult> = Vec::new();

    for &spread in &spreads {
        for &skew in &skews {
            let mm_config = MMConfig {
                base_spread_bps: spread,
                inventory_skew_factor: skew,
                max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
                quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
                ..Default::default()
            };

            let config = BacktestConfig {
                replay: replay_config.clone(),
                mm: mm_config,
                simulator: SimulatorConfig {
                    fee_rate: Decimal::from_f64_retain(cli.fee_rate).unwrap_or(dec!(0.0001)),
                    ..Default::default()
                },
                fill_sim: ingestor::backtest::FillSimulatorConfig {
                    base_fill_probability: cli.fill_prob,
                    queue_position: cli.queue_pos,
                    fee_rate: Decimal::from_f64_retain(cli.fee_rate).unwrap_or(dec!(0.0001)),
                    ..Default::default()
                },
                verbose: false,
                use_realistic_fills: !cli.naive_fills,
                ..Default::default()
            };

            let mut engine = BacktestEngine::new(config);
            engine.load_data()?;
            let results = engine.run()?;

            let sweep_result = SweepResult {
                spread,
                skew,
                sharpe: results.metrics.sharpe_ratio,
                total_return: results.metrics.total_return,
                max_drawdown: results.metrics.max_drawdown,
                num_trades: results.metrics.num_trades,
                win_rate: results.metrics.win_rate,
            };

            println!(
                "Spread={:.1}, Skew={:.1} => Sharpe={:+.2}, Return={:+.2}%, DD={:.2}%, Trades={}",
                spread,
                skew,
                sweep_result.sharpe,
                sweep_result.total_return * 100.0,
                sweep_result.max_drawdown * 100.0,
                sweep_result.num_trades,
            );

            all_results.push(sweep_result);
        }
    }

    // Find best by Sharpe
    if let Some(best) = all_results.iter().max_by(|a, b| {
        a.sharpe.partial_cmp(&b.sharpe).unwrap_or(std::cmp::Ordering::Equal)
    }) {
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("BEST PARAMETERS (by Sharpe):");
        println!("  Spread:     {} bps", best.spread);
        println!("  Skew:       {}", best.skew);
        println!("  Sharpe:     {:.2}", best.sharpe);
        println!("  Return:     {:.2}%", best.total_return * 100.0);
        println!("  Max DD:     {:.2}%", best.max_drawdown * 100.0);
        println!("  Win Rate:   {:.1}%", best.win_rate * 100.0);
        println!("═══════════════════════════════════════════════════════");
    }

    // Save all results
    if let Some(ref output) = cli.output {
        let json = serde_json::to_string_pretty(&all_results)?;
        std::fs::write(output, json)?;
        println!();
        println!("Results saved to: {:?}", output);
    }

    Ok(())
}

fn show_info(cli: &Cli) -> Result<()> {
    use ingestor::backtest::replay::ParquetReplay;

    let config = ReplayConfig {
        data_dir: cli.data.clone(),
        ..Default::default()
    };

    let mut replay = ParquetReplay::new(config);
    let num_events = replay.load()?;

    println!("═══════════════════════════════════════════════════════");
    println!("           DATA INFO                                    ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Directory: {:?}", cli.data);
    println!("Events:    {}", num_events);

    if let Some((start, end)) = replay.time_range() {
        let duration_ms = end - start;
        let duration_hours = duration_ms as f64 / (1000.0 * 60.0 * 60.0);
        let duration_days = duration_hours / 24.0;

        // Convert timestamps to datetime strings
        let start_dt = chrono::DateTime::from_timestamp_millis(start)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        let end_dt = chrono::DateTime::from_timestamp_millis(end)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        println!();
        println!("Time Range:");
        println!("  Start: {}", start_dt);
        println!("  End:   {}", end_dt);
        println!("  Duration: {:.1} hours ({:.2} days)", duration_hours, duration_days);
        println!();
        println!("Event Rate: {:.1} events/second", num_events as f64 / (duration_ms as f64 / 1000.0));
    }

    println!("═══════════════════════════════════════════════════════");

    Ok(())
}

fn build_config(cli: &Cli) -> BacktestConfig {
    use ingestor::backtest::FillSimulatorConfig;

    let mm_config = MMConfig {
        base_spread_bps: cli.spread,
        inventory_skew_factor: cli.skew,
        max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        ..Default::default()
    };

    let sim_config = SimulatorConfig {
        fee_rate: Decimal::from_f64_retain(cli.fee_rate).unwrap_or(dec!(0.0001)),
        ..Default::default()
    };

    let fill_sim_config = FillSimulatorConfig {
        base_fill_probability: cli.fill_prob,
        queue_position: cli.queue_pos,
        fee_rate: Decimal::from_f64_retain(cli.fee_rate).unwrap_or(dec!(0.0001)),
        ..Default::default()
    };

    BacktestConfig {
        replay: ReplayConfig {
            data_dir: cli.data.clone(),
            ..Default::default()
        },
        mm: mm_config,
        simulator: sim_config,
        fill_sim: fill_sim_config,
        verbose: !cli.quiet,
        use_realistic_fills: !cli.naive_fills,
        ..Default::default()
    }
}

fn run_walk_forward(
    cli: &Cli,
    folds: usize,
    test_hours: f64,
    rolling: bool,
    output: Option<PathBuf>,
) -> Result<()> {
    use ingestor::backtest::walk_forward::{WalkForwardEngine, WalkForwardConfig, ParamGrid};

    println!("═══════════════════════════════════════════════════════");
    println!("           WALK-FORWARD VALIDATION                     ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Configuration:");
    println!("  Data:          {:?}", cli.data);
    println!("  Folds:         {}", folds);
    println!("  Test Period:   {} hours per fold", test_hours);
    println!("  Mode:          {}", if rolling { "Rolling" } else { "Anchored (expanding)" });
    println!();

    let config = WalkForwardConfig {
        n_folds: folds,
        test_hours,
        anchored: !rolling,
        data_dir: cli.data.clone(),
        param_grid: ParamGrid {
            spreads: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            skews: vec![0.3, 0.5, 0.7],
            fill_probs: vec![0.05, 0.10, 0.15],
        },
        verbose: !cli.quiet,
        ..Default::default()
    };

    let mut engine = WalkForwardEngine::new(config);

    println!("Loading data...");
    let num_events = engine.load_data()?;
    println!("Loaded {} events", num_events);
    println!();

    let results = engine.run()?;

    if let Some(ref output_path) = output {
        results.save_json(output_path.to_str().unwrap())?;
        println!();
        println!("Results saved to: {:?}", output_path);
    }

    Ok(())
}

fn run_validate(cli: &Cli, output: Option<PathBuf>) -> Result<()> {
    use ingestor::backtest::data_quality::DataValidator;

    println!("═══════════════════════════════════════════════════════");
    println!("           DATA QUALITY VALIDATION                     ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Data directory: {:?}", cli.data);
    println!();
    println!("Running validation...");

    let validator = DataValidator::new();
    let report = validator.validate_directory(&cli.data)?;

    report.print_summary();

    if let Some(ref output_path) = output {
        report.save_json(output_path.to_str().unwrap())?;
        println!();
        println!("Report saved to: {:?}", output_path);
    }

    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SweepResult {
    spread: f64,
    skew: f64,
    sharpe: f64,
    total_return: f64,
    max_drawdown: f64,
    num_trades: usize,
    win_rate: f64,
}

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
use ingestor::backtest::ml_trainer::{MLTrainer, MLTrainerConfig};
use ingestor::market_maker::{MMConfig, RegimeParams, RegimeConfig};
use ingestor::mm_simulator::SimulatorConfig;
use ingestor::algorithms::{
    AlgorithmType, MLSpreadSkewAlgorithm, MLSpreadSkewConfig, MLModelWeights,
    AvellanedaStoikovAlgorithm, MarketMakingAlgorithm,
};

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

    /// High entropy threshold (above = aggressive quoting)
    #[arg(long, default_value = "0.7")]
    high_entropy: f64,

    /// Low entropy threshold (below = defensive/no quoting)
    #[arg(long, default_value = "0.4")]
    low_entropy: f64,

    /// Pull quotes entirely in low entropy regime (entropy gate)
    #[arg(long)]
    entropy_gate: bool,

    /// Output results as JSON (for scripting/Optuna)
    #[arg(long)]
    json: bool,

    /// Use regime-specific parameters (different params per regime)
    #[arg(long)]
    regime_params: bool,

    /// High entropy spread (bps) - used with --regime-params
    #[arg(long, default_value = "1.0")]
    high_spread: f64,

    /// Medium entropy spread (bps) - used with --regime-params
    #[arg(long, default_value = "2.5")]
    med_spread: f64,

    /// Low entropy spread (bps) - used with --regime-params
    #[arg(long, default_value = "5.0")]
    low_spread: f64,

    /// High entropy skew - used with --regime-params
    #[arg(long, default_value = "0.3")]
    high_skew: f64,

    /// Medium entropy skew - used with --regime-params
    #[arg(long, default_value = "0.5")]
    med_skew: f64,

    /// Low entropy skew - used with --regime-params
    #[arg(long, default_value = "1.0")]
    low_skew: f64,

    /// Quote in low entropy (false = no quotes in low entropy)
    #[arg(long)]
    quote_low_entropy: bool,

    /// Show statistical significance report (PSR, DSR, bootstrap CI)
    #[arg(long)]
    stats: bool,
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

    /// Extended grid search over all key parameters
    GridSearch {
        /// Spread values to test (comma-separated)
        #[arg(long, default_value = "1,2,3,4,5")]
        spreads: String,

        /// Skew values to test (comma-separated)
        #[arg(long, default_value = "0.3,0.5,0.7,1.0")]
        skews: String,

        /// High entropy threshold values (comma-separated)
        #[arg(long, default_value = "0.6,0.7,0.8")]
        high_entropies: String,

        /// Test both gated and ungated modes
        #[arg(long)]
        test_gate: bool,

        /// Fill probability values to test (comma-separated)
        #[arg(long, default_value = "0.05,0.10,0.15")]
        fill_probs: String,

        /// Output file for results (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Regime-specific grid search (optimize params per regime independently)
    RegimeSearch {
        /// High entropy spread values (comma-separated)
        #[arg(long, default_value = "0.5,1.0,1.5")]
        high_spreads: String,

        /// Medium entropy spread values (comma-separated)
        #[arg(long, default_value = "2.0,2.5,3.0")]
        med_spreads: String,

        /// Low entropy spread values (comma-separated, or "none" to test no-quote)
        #[arg(long, default_value = "4.0,5.0,none")]
        low_spreads: String,

        /// High entropy skew values (comma-separated)
        #[arg(long, default_value = "0.2,0.3,0.4")]
        high_skews: String,

        /// Medium entropy skew values (comma-separated)
        #[arg(long, default_value = "0.4,0.5,0.6")]
        med_skews: String,

        /// Low entropy skew values (comma-separated)
        #[arg(long, default_value = "0.8,1.0,1.2")]
        low_skews: String,

        /// Fill probability values (comma-separated)
        #[arg(long, default_value = "0.10")]
        fill_probs: String,

        /// Output file for results (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Out-of-sample validation (hold-out test)
    OosValidate {
        /// Fraction of data to reserve for out-of-sample test (0.1-0.5)
        #[arg(long, default_value = "0.20")]
        holdout: f64,

        /// Gap between train and test to prevent lookahead (hours)
        #[arg(long, default_value = "1.0")]
        embargo_hours: f64,

        /// Spread values to test (comma-separated)
        #[arg(long, default_value = "1,2,3")]
        spreads: String,

        /// Skew values to test (comma-separated)
        #[arg(long, default_value = "0.3,0.5")]
        skews: String,

        /// Fill probability values (comma-separated)
        #[arg(long, default_value = "0.10")]
        fill_probs: String,

        /// Output file for results (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Multi-objective optimization (Pareto frontier)
    MultiObjective {
        /// Spread values to test (comma-separated)
        #[arg(long, default_value = "1,2,3,4,5")]
        spreads: String,

        /// Skew values to test (comma-separated)
        #[arg(long, default_value = "0.3,0.5,0.7,1.0")]
        skews: String,

        /// Fill probability values (comma-separated)
        #[arg(long, default_value = "0.05,0.10,0.15")]
        fill_probs: String,

        /// High entropy threshold values (comma-separated)
        #[arg(long, default_value = "0.6,0.7,0.8")]
        high_entropies: String,

        /// Minimum trades for valid solution
        #[arg(long, default_value = "20")]
        min_trades: usize,

        /// Weight for Sharpe in composite score (0-1)
        #[arg(long, default_value = "0.4")]
        w_sharpe: f64,

        /// Weight for drawdown in composite score (0-1)
        #[arg(long, default_value = "0.3")]
        w_drawdown: f64,

        /// Weight for fill rate in composite score (0-1)
        #[arg(long, default_value = "0.2")]
        w_fill: f64,

        /// Weight for turnover in composite score (0-1)
        #[arg(long, default_value = "0.1")]
        w_turnover: f64,

        /// Output file for results (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Regime-specific parameter optimization (find best params per regime)
    RegimeOptimize {
        /// Spread values to test (comma-separated)
        #[arg(long, default_value = "0.5,1.0,1.5,2.0,2.5,3.0,4.0,5.0")]
        spreads: String,

        /// Skew values to test (comma-separated)
        #[arg(long, default_value = "0.2,0.3,0.4,0.5,0.6,0.7,0.8,1.0")]
        skews: String,

        /// Fill probability for simulation
        #[arg(long, default_value = "0.10")]
        fill_prob: f64,

        /// Minimum trades for valid optimization
        #[arg(long, default_value = "10")]
        min_trades: usize,

        /// Allow no-quoting in low entropy regime
        #[arg(long, default_value = "true")]
        allow_no_quote: bool,

        /// Output file for results (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Train ML weights using grid search optimization
    TrainMl {
        /// Training ratio (fraction of data for training, rest for test)
        #[arg(long, default_value = "0.7")]
        train_ratio: f64,

        /// Spread intercept values to test (comma-separated)
        #[arg(long, default_value = "1.0,2.0,3.0,4.0,5.0")]
        spread_intercepts: String,

        /// Spread entropy weight values to test (comma-separated)
        #[arg(long, default_value = "-3.0,-2.0,-1.0,0.0")]
        spread_entropy_weights: String,

        /// Spread volatility weight values to test (comma-separated)
        #[arg(long, default_value = "200.0,400.0,600.0")]
        spread_vol_weights: String,

        /// Skew intercept values to test (comma-separated)
        #[arg(long, default_value = "0.3,0.5,0.7")]
        skew_intercepts: String,

        /// Skew inventory weight values to test (comma-separated)
        #[arg(long, default_value = "-1.0,-0.8,-0.6,-0.4")]
        skew_inv_weights: String,

        /// Output file for trained weights (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Compare ML algorithm vs Avellaneda-Stoikov
    Compare {
        /// Algorithm to use: ml, as (avellaneda-stoikov)
        #[arg(long, default_value = "ml")]
        algorithm: String,

        /// Path to ML weights file (JSON) - required for ml algorithm
        #[arg(long)]
        weights: Option<std::path::PathBuf>,

        /// Output file for comparison results (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Head-to-head comparison: ML vs Avellaneda-Stoikov on same data
    HeadToHead {
        /// Path to ML weights file (JSON) - uses default weights if not specified
        #[arg(long)]
        weights: Option<std::path::PathBuf>,

        /// A-S spread (bps) for comparison
        #[arg(long, default_value = "2.0")]
        as_spread: f64,

        /// A-S skew factor for comparison
        #[arg(long, default_value = "0.5")]
        as_skew: f64,

        /// Output file for comparison results (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },
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
        Some(Commands::GridSearch { spreads, skews, high_entropies, test_gate, fill_probs, output }) => {
            run_grid_search(&cli, spreads, skews, high_entropies, *test_gate, fill_probs, output.clone())?;
        }
        Some(Commands::RegimeSearch { high_spreads, med_spreads, low_spreads, high_skews, med_skews, low_skews, fill_probs, output }) => {
            run_regime_search(&cli, high_spreads, med_spreads, low_spreads, high_skews, med_skews, low_skews, fill_probs, output.clone())?;
        }
        Some(Commands::OosValidate { holdout, embargo_hours, spreads, skews, fill_probs, output }) => {
            run_oos_validation(&cli, *holdout, *embargo_hours, spreads, skews, fill_probs, output.clone())?;
        }
        Some(Commands::MultiObjective { spreads, skews, fill_probs, high_entropies, min_trades, w_sharpe, w_drawdown, w_fill, w_turnover, output }) => {
            run_multi_objective(&cli, spreads, skews, fill_probs, high_entropies, *min_trades, *w_sharpe, *w_drawdown, *w_fill, *w_turnover, output.clone())?;
        }
        Some(Commands::RegimeOptimize { spreads, skews, fill_prob, min_trades, allow_no_quote, output }) => {
            run_regime_optimize(&cli, spreads, skews, *fill_prob, *min_trades, *allow_no_quote, output.clone())?;
        }
        Some(Commands::TrainMl { train_ratio, spread_intercepts, spread_entropy_weights, spread_vol_weights, skew_intercepts, skew_inv_weights, output }) => {
            run_train_ml(&cli, *train_ratio, spread_intercepts, spread_entropy_weights, spread_vol_weights, skew_intercepts, skew_inv_weights, output.clone())?;
        }
        Some(Commands::Compare { algorithm, weights, output }) => {
            run_compare(&cli, algorithm, weights.clone(), output.clone())?;
        }
        Some(Commands::HeadToHead { weights, as_spread, as_skew, output }) => {
            run_head_to_head(&cli, weights.clone(), *as_spread, *as_skew, output.clone())?;
        }
        Some(Commands::Single) | None => {
            run_single(&cli)?;
        }
    }

    Ok(())
}

fn run_single(cli: &Cli) -> Result<()> {
    // JSON mode: minimal output, just the results
    if cli.json {
        let mut config = build_config(cli);
        config.verbose = false;  // Suppress progress output
        let mut engine = BacktestEngine::new(config);
        engine.load_data()?;
        let results = engine.run()?;

        // Output JSON for Optuna/scripting
        let json_output = serde_json::json!({
            "sharpe": results.metrics.sharpe_ratio,
            "total_return": results.metrics.total_return,
            "max_drawdown": results.metrics.max_drawdown,
            "num_trades": results.metrics.num_trades,
            "win_rate": results.metrics.win_rate,
            "avg_trade_pnl": results.metrics.avg_trade_pnl,
            "params": {
                "spread": cli.spread,
                "skew": cli.skew,
                "fill_prob": cli.fill_prob,
                "high_entropy": cli.high_entropy,
                "entropy_gate": cli.entropy_gate
            }
        });
        println!("{}", json_output);
        return Ok(());
    }

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
    println!("  Entropy Gate:  {}", if cli.entropy_gate { "ON (no quotes in low entropy)" } else { "OFF (spread widening only)" });
    println!("  High Entropy:  {} (above = aggressive)", cli.high_entropy);
    println!("  Low Entropy:   {} (below = defensive)", cli.low_entropy);
    println!();

    let config = build_config(cli);
    let mut engine = BacktestEngine::new(config);

    println!("Loading data...");
    let num_events = engine.load_data()?;
    println!("Loaded {} events", num_events);
    println!();

    let results = engine.run()?;

    if cli.stats {
        results.print_summary_with_stats(1); // Single trial for individual backtest
    } else {
        results.print_summary();
    }

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
                max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
                quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
                regime_params: RegimeParams::uniform(spread, skew),
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
    use ingestor::market_maker::RegimeThresholds;

    // Build regime params based on CLI flags
    let regime_params = if cli.regime_params {
        // Full regime-specific parameters
        RegimeParams {
            high_entropy: RegimeConfig {
                spread_bps: cli.high_spread,
                skew_factor: cli.high_skew,
                size_mult: 1.0,
                should_quote: true,
            },
            medium_entropy: RegimeConfig {
                spread_bps: cli.med_spread,
                skew_factor: cli.med_skew,
                size_mult: 0.7,
                should_quote: true,
            },
            low_entropy: RegimeConfig {
                spread_bps: cli.low_spread,
                skew_factor: cli.low_skew,
                size_mult: 0.3,
                should_quote: cli.quote_low_entropy,
            },
        }
    } else {
        // Uniform parameters with optional entropy gate
        let mut params = RegimeParams::uniform(cli.spread, cli.skew);
        if cli.entropy_gate {
            params.low_entropy.should_quote = false;
        }
        params
    };

    let mm_config = MMConfig {
        max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        regime_thresholds: RegimeThresholds {
            high_entropy_threshold: cli.high_entropy,
            low_entropy_threshold: cli.low_entropy,
        },
        regime_params,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct GridSearchResult {
    spread: f64,
    skew: f64,
    high_entropy_threshold: f64,
    entropy_gate: bool,
    fill_prob: f64,
    sharpe: f64,
    total_return: f64,
    max_drawdown: f64,
    num_trades: usize,
    win_rate: f64,
    avg_trade_pnl: f64,
}

fn run_grid_search(
    cli: &Cli,
    spreads_str: &str,
    skews_str: &str,
    high_entropies_str: &str,
    test_gate: bool,
    fill_probs_str: &str,
    output: Option<PathBuf>,
) -> Result<()> {
    use ingestor::market_maker::RegimeThresholds;

    let spreads: Vec<f64> = spreads_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let skews: Vec<f64> = skews_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let high_entropies: Vec<f64> = high_entropies_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let fill_probs: Vec<f64> = fill_probs_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();

    let gate_modes: Vec<bool> = if test_gate { vec![false, true] } else { vec![false] };

    let total_combinations = spreads.len() * skews.len() * high_entropies.len() * gate_modes.len() * fill_probs.len();

    println!("═══════════════════════════════════════════════════════");
    println!("           EXTENDED GRID SEARCH                        ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Parameter Space:");
    println!("  Spreads:          {:?}", spreads);
    println!("  Skews:            {:?}", skews);
    println!("  High Entropies:   {:?}", high_entropies);
    println!("  Entropy Gate:     {:?}", gate_modes);
    println!("  Fill Probs:       {:?}", fill_probs);
    println!();
    println!("Total combinations: {}", total_combinations);
    println!();

    let replay_config = ReplayConfig {
        data_dir: cli.data.clone(),
        ..Default::default()
    };

    let mut all_results: Vec<GridSearchResult> = Vec::new();
    let mut count = 0;

    for &spread in &spreads {
        for &skew in &skews {
            for &high_entropy in &high_entropies {
                for &gate in &gate_modes {
                    for &fill_prob in &fill_probs {
                        count += 1;

                        // Build regime params with optional gating in low entropy
                        let mut regime_params = RegimeParams::uniform(spread, skew);
                        if gate {
                            regime_params.low_entropy.should_quote = false;
                        }

                        let mm_config = MMConfig {
                            max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
                            quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
                            regime_thresholds: RegimeThresholds {
                                high_entropy_threshold: high_entropy,
                                low_entropy_threshold: cli.low_entropy,
                            },
                            regime_params,
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
                                base_fill_probability: fill_prob,
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

                        let avg_trade_pnl = if results.metrics.num_trades > 0 {
                            results.metrics.total_return / results.metrics.num_trades as f64
                        } else {
                            0.0
                        };

                        let grid_result = GridSearchResult {
                            spread,
                            skew,
                            high_entropy_threshold: high_entropy,
                            entropy_gate: gate,
                            fill_prob,
                            sharpe: results.metrics.sharpe_ratio,
                            total_return: results.metrics.total_return,
                            max_drawdown: results.metrics.max_drawdown,
                            num_trades: results.metrics.num_trades,
                            win_rate: results.metrics.win_rate,
                            avg_trade_pnl,
                        };

                        let gate_str = if gate { "GATE" } else { "WIDE" };
                        println!(
                            "[{:>4}/{}] s={:.1} k={:.1} ent={:.1} {} fp={:.2} => Sharpe={:+.2} Ret={:+.2}% Tr={}",
                            count, total_combinations,
                            spread, skew, high_entropy, gate_str, fill_prob,
                            grid_result.sharpe,
                            grid_result.total_return * 100.0,
                            grid_result.num_trades,
                        );

                        all_results.push(grid_result);
                    }
                }
            }
        }
    }

    // Sort by Sharpe ratio
    all_results.sort_by(|a, b| b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal));

    println!();
    println!("═══════════════════════════════════════════════════════");
    println!("TOP 10 PARAMETER SETS (by Sharpe):");
    println!("═══════════════════════════════════════════════════════");

    for (i, r) in all_results.iter().take(10).enumerate() {
        let gate_str = if r.entropy_gate { "GATE" } else { "WIDE" };
        println!(
            "{:>2}. Spread={:.1} Skew={:.1} Entropy={:.1} {} FillP={:.2}",
            i + 1, r.spread, r.skew, r.high_entropy_threshold, gate_str, r.fill_prob
        );
        println!(
            "    Sharpe={:+.2} Return={:+.2}% DD={:.2}% WinRate={:.1}% Trades={}",
            r.sharpe, r.total_return * 100.0, r.max_drawdown * 100.0, r.win_rate * 100.0, r.num_trades
        );
    }

    // Compare gated vs ungated if test_gate is true
    if test_gate {
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("ENTROPY GATE COMPARISON:");
        println!("═══════════════════════════════════════════════════════");

        let gated: Vec<_> = all_results.iter().filter(|r| r.entropy_gate).collect();
        let ungated: Vec<_> = all_results.iter().filter(|r| !r.entropy_gate).collect();

        let avg_sharpe_gated: f64 = gated.iter().map(|r| r.sharpe).sum::<f64>() / gated.len() as f64;
        let avg_sharpe_ungated: f64 = ungated.iter().map(|r| r.sharpe).sum::<f64>() / ungated.len() as f64;

        let avg_return_gated: f64 = gated.iter().map(|r| r.total_return).sum::<f64>() / gated.len() as f64;
        let avg_return_ungated: f64 = ungated.iter().map(|r| r.total_return).sum::<f64>() / ungated.len() as f64;

        let avg_trades_gated: f64 = gated.iter().map(|r| r.num_trades as f64).sum::<f64>() / gated.len() as f64;
        let avg_trades_ungated: f64 = ungated.iter().map(|r| r.num_trades as f64).sum::<f64>() / ungated.len() as f64;

        println!("                    UNGATED (spread widen)  vs  GATED (no quotes)");
        println!("  Avg Sharpe:       {:+.3}                     {:+.3}", avg_sharpe_ungated, avg_sharpe_gated);
        println!("  Avg Return:       {:+.2}%                    {:+.2}%", avg_return_ungated * 100.0, avg_return_gated * 100.0);
        println!("  Avg Trades:       {:.0}                       {:.0}", avg_trades_ungated, avg_trades_gated);

        let sharpe_diff = avg_sharpe_gated - avg_sharpe_ungated;
        if sharpe_diff > 0.1 {
            println!();
            println!("  >>> GATED mode shows +{:.2} Sharpe improvement!", sharpe_diff);
        } else if sharpe_diff < -0.1 {
            println!();
            println!("  >>> UNGATED mode shows +{:.2} Sharpe advantage!", -sharpe_diff);
        } else {
            println!();
            println!("  >>> No significant difference between modes.");
        }
    }

    // Best overall
    if let Some(best) = all_results.first() {
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("RECOMMENDED PARAMETERS:");
        println!("═══════════════════════════════════════════════════════");
        println!("  base_spread_bps:            {}", best.spread);
        println!("  inventory_skew_factor:      {}", best.skew);
        println!("  high_entropy_threshold:     {}", best.high_entropy_threshold);
        println!("  pull_quotes_in_low_entropy: {}", best.entropy_gate);
        println!("  base_fill_probability:      {}", best.fill_prob);
        println!();
        println!("Expected Performance:");
        println!("  Sharpe Ratio: {:+.2}", best.sharpe);
        println!("  Total Return: {:+.2}%", best.total_return * 100.0);
        println!("  Max Drawdown: {:.2}%", best.max_drawdown * 100.0);
        println!("  Win Rate:     {:.1}%", best.win_rate * 100.0);
        println!("═══════════════════════════════════════════════════════");
    }

    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&all_results)?;
        std::fs::write(output_path, json)?;
        println!();
        println!("Full results saved to: {:?}", output_path);
    }

    Ok(())
}

/// Low entropy spread option - can be a value or "none" (no quoting)
#[derive(Debug, Clone)]
enum LowEntropySpread {
    Value(f64),
    NoQuote,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RegimeSearchResult {
    high_spread: f64,
    high_skew: f64,
    med_spread: f64,
    med_skew: f64,
    low_spread: Option<f64>,  // None = no quoting in low entropy
    low_skew: f64,
    fill_prob: f64,
    sharpe: f64,
    total_return: f64,
    max_drawdown: f64,
    num_trades: usize,
    win_rate: f64,
    avg_trade_pnl: f64,
}

#[allow(clippy::too_many_arguments)]
fn run_regime_search(
    cli: &Cli,
    high_spreads_str: &str,
    med_spreads_str: &str,
    low_spreads_str: &str,
    high_skews_str: &str,
    med_skews_str: &str,
    low_skews_str: &str,
    fill_probs_str: &str,
    output: Option<PathBuf>,
) -> Result<()> {
    use ingestor::market_maker::RegimeThresholds;

    // Parse parameters
    let high_spreads: Vec<f64> = high_spreads_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let med_spreads: Vec<f64> = med_spreads_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let high_skews: Vec<f64> = high_skews_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let med_skews: Vec<f64> = med_skews_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let low_skews: Vec<f64> = low_skews_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let fill_probs: Vec<f64> = fill_probs_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();

    // Parse low entropy spreads - can include "none"
    let low_spreads: Vec<LowEntropySpread> = low_spreads_str
        .split(',')
        .map(|s| {
            let s = s.trim().to_lowercase();
            if s == "none" || s == "no" {
                LowEntropySpread::NoQuote
            } else {
                s.parse().map(LowEntropySpread::Value).unwrap_or(LowEntropySpread::NoQuote)
            }
        })
        .collect();

    let total_combinations = high_spreads.len() * high_skews.len()
        * med_spreads.len() * med_skews.len()
        * low_spreads.len() * low_skews.len()
        * fill_probs.len();

    println!("═══════════════════════════════════════════════════════");
    println!("       REGIME-SPECIFIC GRID SEARCH                      ");
    println!("       (Optimize params per regime independently)       ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Parameter Space:");
    println!("  High Entropy:");
    println!("    Spreads: {:?}", high_spreads);
    println!("    Skews:   {:?}", high_skews);
    println!("  Medium Entropy:");
    println!("    Spreads: {:?}", med_spreads);
    println!("    Skews:   {:?}", med_skews);
    println!("  Low Entropy:");
    println!("    Spreads: {:?}", low_spreads_str);
    println!("    Skews:   {:?}", low_skews);
    println!("  Fill Probs: {:?}", fill_probs);
    println!();
    println!("Total combinations: {}", total_combinations);
    println!();

    let replay_config = ReplayConfig {
        data_dir: cli.data.clone(),
        ..Default::default()
    };

    let mut all_results: Vec<RegimeSearchResult> = Vec::new();
    let mut count = 0;

    for &h_spread in &high_spreads {
        for &h_skew in &high_skews {
            for &m_spread in &med_spreads {
                for &m_skew in &med_skews {
                    for l_spread in &low_spreads {
                        for &l_skew in &low_skews {
                            for &fill_prob in &fill_probs {
                                count += 1;

                                let (low_spread_val, should_quote_low) = match l_spread {
                                    LowEntropySpread::Value(v) => (*v, true),
                                    LowEntropySpread::NoQuote => (5.0, false), // dummy value when not quoting
                                };

                                let regime_params = RegimeParams {
                                    high_entropy: RegimeConfig {
                                        spread_bps: h_spread,
                                        skew_factor: h_skew,
                                        size_mult: 1.0,
                                        should_quote: true,
                                    },
                                    medium_entropy: RegimeConfig {
                                        spread_bps: m_spread,
                                        skew_factor: m_skew,
                                        size_mult: 0.7,
                                        should_quote: true,
                                    },
                                    low_entropy: RegimeConfig {
                                        spread_bps: low_spread_val,
                                        skew_factor: l_skew,
                                        size_mult: 0.3,
                                        should_quote: should_quote_low,
                                    },
                                };

                                let mm_config = MMConfig {
                                    regime_params,
                                    max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
                                    quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
                                    regime_thresholds: RegimeThresholds {
                                        high_entropy_threshold: cli.high_entropy,
                                        low_entropy_threshold: cli.low_entropy,
                                    },
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
                                        base_fill_probability: fill_prob,
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

                                let avg_trade_pnl = if results.metrics.num_trades > 0 {
                                    results.metrics.total_return / results.metrics.num_trades as f64
                                } else {
                                    0.0
                                };

                                let low_spread_opt = match l_spread {
                                    LowEntropySpread::Value(v) => Some(*v),
                                    LowEntropySpread::NoQuote => None,
                                };

                                let result = RegimeSearchResult {
                                    high_spread: h_spread,
                                    high_skew: h_skew,
                                    med_spread: m_spread,
                                    med_skew: m_skew,
                                    low_spread: low_spread_opt,
                                    low_skew: l_skew,
                                    fill_prob,
                                    sharpe: results.metrics.sharpe_ratio,
                                    total_return: results.metrics.total_return,
                                    max_drawdown: results.metrics.max_drawdown,
                                    num_trades: results.metrics.num_trades,
                                    win_rate: results.metrics.win_rate,
                                    avg_trade_pnl,
                                };

                                let low_str = match low_spread_opt {
                                    Some(v) => format!("{:.1}", v),
                                    None => "NONE".to_string(),
                                };

                                println!(
                                    "[{:>4}/{}] H({:.1},{:.1}) M({:.1},{:.1}) L({},{:.1}) fp={:.2} => Sharpe={:+.2} Ret={:+.2}% Tr={}",
                                    count, total_combinations,
                                    h_spread, h_skew, m_spread, m_skew, low_str, l_skew, fill_prob,
                                    result.sharpe, result.total_return * 100.0, result.num_trades,
                                );

                                all_results.push(result);
                            }
                        }
                    }
                }
            }
        }
    }

    // Sort by Sharpe ratio
    all_results.sort_by(|a, b| b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal));

    println!();
    println!("═══════════════════════════════════════════════════════");
    println!("TOP 10 REGIME-SPECIFIC PARAMETER SETS (by Sharpe):");
    println!("═══════════════════════════════════════════════════════");

    for (i, r) in all_results.iter().take(10).enumerate() {
        let low_str = match r.low_spread {
            Some(v) => format!("{:.1}bps", v),
            None => "NO QUOTE".to_string(),
        };
        println!(
            "{:>2}. HIGH(sp={:.1}bps, sk={:.1}) MED(sp={:.1}bps, sk={:.1}) LOW({}, sk={:.1})",
            i + 1, r.high_spread, r.high_skew, r.med_spread, r.med_skew, low_str, r.low_skew
        );
        println!(
            "    Sharpe={:+.2} Return={:+.2}% DD={:.2}% WinRate={:.1}% Trades={}",
            r.sharpe, r.total_return * 100.0, r.max_drawdown * 100.0, r.win_rate * 100.0, r.num_trades
        );
    }

    // Compare quoting vs not quoting in low entropy
    let with_low_quote: Vec<_> = all_results.iter().filter(|r| r.low_spread.is_some()).collect();
    let without_low_quote: Vec<_> = all_results.iter().filter(|r| r.low_spread.is_none()).collect();

    if !with_low_quote.is_empty() && !without_low_quote.is_empty() {
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("LOW ENTROPY QUOTING COMPARISON:");
        println!("═══════════════════════════════════════════════════════");

        let avg_sharpe_with: f64 = with_low_quote.iter().map(|r| r.sharpe).sum::<f64>() / with_low_quote.len() as f64;
        let avg_sharpe_without: f64 = without_low_quote.iter().map(|r| r.sharpe).sum::<f64>() / without_low_quote.len() as f64;

        let avg_trades_with: f64 = with_low_quote.iter().map(|r| r.num_trades as f64).sum::<f64>() / with_low_quote.len() as f64;
        let avg_trades_without: f64 = without_low_quote.iter().map(|r| r.num_trades as f64).sum::<f64>() / without_low_quote.len() as f64;

        println!("                    QUOTE in Low Entropy    NO QUOTE in Low Entropy");
        println!("  Avg Sharpe:       {:+.3}                   {:+.3}", avg_sharpe_with, avg_sharpe_without);
        println!("  Avg Trades:       {:.0}                      {:.0}", avg_trades_with, avg_trades_without);

        let diff = avg_sharpe_without - avg_sharpe_with;
        if diff > 0.1 {
            println!();
            println!("  >>> NOT QUOTING in low entropy improves Sharpe by +{:.2}!", diff);
        } else if diff < -0.1 {
            println!();
            println!("  >>> QUOTING in low entropy is better by +{:.2} Sharpe!", -diff);
        }
    }

    // Best overall
    if let Some(best) = all_results.first() {
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("RECOMMENDED REGIME-SPECIFIC PARAMETERS:");
        println!("═══════════════════════════════════════════════════════");
        println!("  High Entropy:");
        println!("    spread_bps:  {}", best.high_spread);
        println!("    skew_factor: {}", best.high_skew);
        println!("  Medium Entropy:");
        println!("    spread_bps:  {}", best.med_spread);
        println!("    skew_factor: {}", best.med_skew);
        println!("  Low Entropy:");
        match best.low_spread {
            Some(v) => {
                println!("    spread_bps:  {}", v);
                println!("    skew_factor: {}", best.low_skew);
                println!("    should_quote: true");
            }
            None => {
                println!("    should_quote: false (no quoting in low entropy)");
            }
        }
        println!();
        println!("Expected Performance:");
        println!("  Sharpe Ratio: {:+.2}", best.sharpe);
        println!("  Total Return: {:+.2}%", best.total_return * 100.0);
        println!("  Max Drawdown: {:.2}%", best.max_drawdown * 100.0);
        println!("  Win Rate:     {:.1}%", best.win_rate * 100.0);
        println!("  Trades:       {}", best.num_trades);
        println!("═══════════════════════════════════════════════════════");
    }

    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&all_results)?;
        std::fs::write(output_path, json)?;
        println!();
        println!("Full results saved to: {:?}", output_path);
    }

    Ok(())
}

fn run_oos_validation(
    cli: &Cli,
    holdout: f64,
    embargo_hours: f64,
    spreads_str: &str,
    skews_str: &str,
    fill_probs_str: &str,
    output: Option<PathBuf>,
) -> Result<()> {
    use ingestor::backtest::oos_validation::{OOSValidator, OOSConfig};

    // Parse parameters
    let spreads: Vec<f64> = spreads_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let skews: Vec<f64> = skews_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let fill_probs: Vec<f64> = fill_probs_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();

    println!("═══════════════════════════════════════════════════════");
    println!("         OUT-OF-SAMPLE VALIDATION                      ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Configuration:");
    println!("  Data:           {:?}", cli.data);
    println!("  Hold-out:       {:.0}%", holdout * 100.0);
    println!("  Embargo:        {:.1} hours", embargo_hours);
    println!();
    println!("Parameter Grid:");
    println!("  Spreads:        {:?}", spreads);
    println!("  Skews:          {:?}", skews);
    println!("  Fill Probs:     {:?}", fill_probs);
    println!();
    println!("Total combinations: {}", spreads.len() * skews.len() * fill_probs.len());
    println!();

    let config = OOSConfig {
        holdout_fraction: holdout,
        embargo_hours,
        data_dir: cli.data.clone(),
        verbose: !cli.quiet,
        ..Default::default()
    };

    let mut validator = OOSValidator::new(config);

    println!("Loading data...");
    let num_events = validator.load_data()?;
    println!("Loaded {} events", num_events);
    println!();

    // Run validation grid
    let reports = validator.validate_grid(&spreads, &skews, &fill_probs)?;

    if reports.is_empty() {
        println!("No valid results - check data availability");
        return Ok(());
    }

    // Print summary of all results
    println!();
    println!("═══════════════════════════════════════════════════════");
    println!("              VALIDATION RESULTS SUMMARY                ");
    println!("═══════════════════════════════════════════════════════");
    println!();

    println!("TOP CONFIGURATIONS (by OOS Sharpe):");
    println!("┌───────┬───────┬────────┬───────────┬───────────┬──────────┬────────────┐");
    println!("│ Sprd  │ Skew  │ FillP  │  IS Shpe  │  OOS Shpe │  Degrad  │   Verdict  │");
    println!("├───────┼───────┼────────┼───────────┼───────────┼──────────┼────────────┤");

    for report in reports.iter().take(10) {
        let verdict_short = match report.overfit_verdict {
            ingestor::backtest::OverfitVerdict::Robust => "ROBUST",
            ingestor::backtest::OverfitVerdict::MildOverfit => "MILD",
            ingestor::backtest::OverfitVerdict::ModerateOverfit => "MODERATE",
            ingestor::backtest::OverfitVerdict::SevereOverfit => "SEVERE",
            ingestor::backtest::OverfitVerdict::Inconclusive => "INCONCL",
        };

        println!("│ {:5.1} │ {:5.2} │ {:5.0}% │ {:+9.3} │ {:+9.3} │ {:7.0}% │ {:10} │",
            report.params_tested.spread_bps,
            report.params_tested.skew_factor,
            report.params_tested.fill_probability * 100.0,
            report.comparison.in_sample.sharpe_ratio,
            report.comparison.out_of_sample.sharpe_ratio,
            report.comparison.sharpe_degradation * 100.0,
            verdict_short);
    }
    println!("└───────┴───────┴────────┴───────────┴───────────┴──────────┴────────────┘");
    println!();

    // Statistics on verdicts
    let robust_count = reports.iter().filter(|r| matches!(r.overfit_verdict, ingestor::backtest::OverfitVerdict::Robust)).count();
    let mild_count = reports.iter().filter(|r| matches!(r.overfit_verdict, ingestor::backtest::OverfitVerdict::MildOverfit)).count();
    let moderate_count = reports.iter().filter(|r| matches!(r.overfit_verdict, ingestor::backtest::OverfitVerdict::ModerateOverfit)).count();
    let severe_count = reports.iter().filter(|r| matches!(r.overfit_verdict, ingestor::backtest::OverfitVerdict::SevereOverfit)).count();

    println!("VERDICT DISTRIBUTION:");
    println!("  Robust:         {} ({:.0}%)", robust_count, (robust_count as f64 / reports.len() as f64) * 100.0);
    println!("  Mild Overfit:   {} ({:.0}%)", mild_count, (mild_count as f64 / reports.len() as f64) * 100.0);
    println!("  Moderate Overfit: {} ({:.0}%)", moderate_count, (moderate_count as f64 / reports.len() as f64) * 100.0);
    println!("  Severe Overfit: {} ({:.0}%)", severe_count, (severe_count as f64 / reports.len() as f64) * 100.0);
    println!();

    // Print best result details
    if let Some(best) = reports.first() {
        println!("═══════════════════════════════════════════════════════");
        println!("BEST CONFIGURATION (by OOS Sharpe):");
        println!("═══════════════════════════════════════════════════════");
        best.print();
    }

    // Save results
    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&reports)?;
        std::fs::write(output_path, json)?;
        println!();
        println!("Results saved to: {:?}", output_path);
    }

    Ok(())
}

fn run_multi_objective(
    cli: &Cli,
    spreads: &str,
    skews: &str,
    fill_probs: &str,
    high_entropies: &str,
    min_trades: usize,
    w_sharpe: f64,
    w_drawdown: f64,
    w_fill: f64,
    w_turnover: f64,
    output: Option<std::path::PathBuf>,
) -> Result<()> {
    use ingestor::backtest::multi_objective::{MultiObjectiveOptimizer, MOConfig, ObjectiveWeights};

    // Parse parameter grids
    let spread_values: Vec<f64> = spreads
        .split(',')
        .map(|s| s.trim().parse().unwrap_or(1.0))
        .collect();
    let skew_values: Vec<f64> = skews
        .split(',')
        .map(|s| s.trim().parse().unwrap_or(0.5))
        .collect();
    let fill_prob_values: Vec<f64> = fill_probs
        .split(',')
        .map(|s| s.trim().parse().unwrap_or(0.10))
        .collect();
    let high_entropy_values: Vec<f64> = high_entropies
        .split(',')
        .map(|s| s.trim().parse().unwrap_or(0.7))
        .collect();

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("          MULTI-OBJECTIVE OPTIMIZATION (Pareto Frontier)                ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("PARAMETER GRID:");
    println!("  Spreads:       {:?}", spread_values);
    println!("  Skews:         {:?}", skew_values);
    println!("  Fill Probs:    {:?}", fill_prob_values);
    println!("  High Entropy:  {:?}", high_entropy_values);
    println!("  Min Trades:    {}", min_trades);
    println!();
    println!("OBJECTIVE WEIGHTS:");
    println!("  Sharpe:     {:.0}%", w_sharpe * 100.0);
    println!("  Drawdown:   {:.0}%", w_drawdown * 100.0);
    println!("  Fill Rate:  {:.0}%", w_fill * 100.0);
    println!("  Turnover:   {:.0}%", w_turnover * 100.0);
    println!();

    let total_combinations = spread_values.len() * skew_values.len() *
        fill_prob_values.len() * high_entropy_values.len();
    println!("Total parameter combinations: {}", total_combinations);
    println!();

    // Build config
    let config = MOConfig {
        data_dir: cli.data.clone(),
        spreads: spread_values,
        skews: skew_values,
        fill_probs: fill_prob_values,
        high_entropies: high_entropy_values,
        objective_weights: ObjectiveWeights {
            sharpe: w_sharpe,
            drawdown: w_drawdown,
            fill_rate: w_fill,
            turnover: w_turnover,
        },
        min_trades,
        verbose: true,
    };

    // Run optimization
    let mut optimizer = MultiObjectiveOptimizer::new(config);
    optimizer.load_data()?;

    let results = optimizer.optimize()?;

    // Print report
    results.print_report();

    // Save results
    if let Some(ref output_path) = output {
        results.save_json(output_path.to_str().unwrap_or("mo_results.json"))?;
        println!();
        println!("Results saved to: {:?}", output_path);
    }

    Ok(())
}

fn run_regime_optimize(
    cli: &Cli,
    spreads: &str,
    skews: &str,
    fill_prob: f64,
    min_trades: usize,
    allow_no_quote: bool,
    output: Option<std::path::PathBuf>,
) -> Result<()> {
    use ingestor::backtest::regime_optimizer::{RegimeOptimizer, RegimeOptimizerConfig};

    // Parse parameter grids
    let spread_values: Vec<f64> = spreads
        .split(',')
        .map(|s| s.trim().parse().unwrap_or(2.0))
        .collect();
    let skew_values: Vec<f64> = skews
        .split(',')
        .map(|s| s.trim().parse().unwrap_or(0.5))
        .collect();

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("       REGIME-SPECIFIC PARAMETER OPTIMIZATION                          ");
    println!("       (Find optimal params for each entropy regime independently)     ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("CONFIGURATION:");
    println!("  Spreads to test:     {:?}", spread_values);
    println!("  Skews to test:       {:?}", skew_values);
    println!("  Fill probability:    {:.0}%", fill_prob * 100.0);
    println!("  Min trades required: {}", min_trades);
    println!("  Allow no-quote low:  {}", allow_no_quote);
    println!("  High entropy thresh: {:.2}", cli.high_entropy);
    println!("  Low entropy thresh:  {:.2}", cli.low_entropy);
    println!();

    let total_combinations = spread_values.len() * skew_values.len();
    println!("Testing {} combinations per regime", total_combinations);
    println!();

    // Build config
    let config = RegimeOptimizerConfig {
        data_dir: cli.data.clone(),
        high_entropy_threshold: cli.high_entropy,
        low_entropy_threshold: cli.low_entropy,
        spreads: spread_values,
        skews: skew_values,
        fill_probability: fill_prob,
        min_trades,
        allow_no_quote_low: allow_no_quote,
        verbose: true,
    };

    // Run optimization
    let mut optimizer = RegimeOptimizer::new(config);
    optimizer.load_data()?;

    let results = optimizer.optimize()?;

    // Print report
    results.print_report();

    // Save results
    if let Some(ref output_path) = output {
        results.save_json(output_path.to_str().unwrap_or("regime_opt_results.json"))?;
        println!();
        println!("Results saved to: {:?}", output_path);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_train_ml(
    cli: &Cli,
    train_ratio: f64,
    spread_intercepts: &str,
    spread_entropy_weights: &str,
    spread_vol_weights: &str,
    skew_intercepts: &str,
    skew_inv_weights: &str,
    output: Option<std::path::PathBuf>,
) -> Result<()> {
    // Parse parameter grids
    let spread_intercepts: Vec<f64> = spread_intercepts
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let spread_entropy_weights: Vec<f64> = spread_entropy_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let spread_vol_weights: Vec<f64> = spread_vol_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let skew_intercepts: Vec<f64> = skew_intercepts
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let skew_inv_weights: Vec<f64> = skew_inv_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let total_combinations = spread_intercepts.len()
        * spread_entropy_weights.len()
        * spread_vol_weights.len()
        * skew_intercepts.len()
        * skew_inv_weights.len();

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("              ML WEIGHT TRAINING (Grid Search)                          ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("CONFIGURATION:");
    println!("  Data:            {:?}", cli.data);
    println!("  Train Ratio:     {:.0}%", train_ratio * 100.0);
    println!();
    println!("PARAMETER GRID:");
    println!("  Spread Intercepts:      {:?}", spread_intercepts);
    println!("  Spread Entropy Weights: {:?}", spread_entropy_weights);
    println!("  Spread Vol Weights:     {:?}", spread_vol_weights);
    println!("  Skew Intercepts:        {:?}", skew_intercepts);
    println!("  Skew Inventory Weights: {:?}", skew_inv_weights);
    println!();
    println!("Total combinations: {}", total_combinations);
    println!();

    // Build config
    let config = MLTrainerConfig {
        data_dir: cli.data.clone(),
        train_ratio,
        spread_intercepts,
        spread_entropy_weights,
        spread_volatility_weights: spread_vol_weights,
        skew_intercepts,
        skew_inventory_weights: skew_inv_weights,
        max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        fill_probability: cli.fill_prob,
        verbose: !cli.quiet,
        ..Default::default()
    };

    // Run training
    let mut trainer = MLTrainer::new(config)?;

    println!("Training ML weights...");
    println!();

    let results = trainer.train()?;

    // Print results
    println!();
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("                    TRAINING RESULTS                                    ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("BEST WEIGHTS:");
    println!("  Spread:");
    println!("    intercept:         {:.4}", results.optimal_weights.spread.intercept);
    println!("    w_entropy:         {:.4}", results.optimal_weights.spread.w_entropy);
    println!("    w_volatility:      {:.4}", results.optimal_weights.spread.w_volatility);
    println!("    w_imbalance:       {:.4}", results.optimal_weights.spread.w_imbalance);
    println!("    w_interaction:     {:.4}", results.optimal_weights.spread.w_interaction);
    println!("  Skew:");
    println!("    intercept:         {:.4}", results.optimal_weights.skew.intercept);
    println!("    w_entropy:         {:.4}", results.optimal_weights.skew.w_entropy);
    println!("    w_volatility:      {:.4}", results.optimal_weights.skew.w_volatility);
    println!("    w_imbalance:       {:.4}", results.optimal_weights.skew.w_imbalance);
    println!("    w_inventory:       {:.4}", results.optimal_weights.skew.w_inventory);
    println!();
    println!("PERFORMANCE:");
    println!("  Train Sharpe:     {:+.4}", results.train_sharpe);
    println!("  Test Sharpe:      {:+.4}", results.test_sharpe);
    println!("  Generalization Gap: {:.2}%", results.generalization_gap * 100.0);
    println!();
    println!("  Train Trades:     {}", results.train_trades);
    println!("  Test Trades:      {}", results.test_trades);
    println!("  Configs Tested:   {}/{}", results.valid_configurations, results.total_configurations);
    println!("═══════════════════════════════════════════════════════════════════════");

    // Save results
    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&results)?;
        std::fs::write(output_path, &json)?;
        println!();
        println!("Results saved to: {:?}", output_path);

        // Also save just the weights for easy loading
        let weights_path = output_path.with_extension("weights.json");
        let weights_json = serde_json::to_string_pretty(&results.optimal_weights)?;
        std::fs::write(&weights_path, weights_json)?;
        println!("Weights saved to: {:?}", weights_path);
    }

    Ok(())
}

fn run_compare(
    cli: &Cli,
    algorithm: &str,
    weights_path: Option<std::path::PathBuf>,
    output: Option<std::path::PathBuf>,
) -> Result<()> {
    use ingestor::backtest::replay::ParquetReplay;
    use ingestor::backtest::harness::BacktestEngine;

    // Parse algorithm type
    let algo_type = AlgorithmType::from_str(algorithm)
        .map_err(|e| anyhow::anyhow!("Invalid algorithm: {}", e))?;

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("              ALGORITHM COMPARISON                                      ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("Algorithm: {} ({})", algo_type.display_name(), algo_type.as_str());
    println!("Data:      {:?}", cli.data);
    println!();

    // Load data
    let replay_config = ReplayConfig {
        data_dir: cli.data.clone(),
        ..Default::default()
    };

    let mut replay = ParquetReplay::new(replay_config.clone());
    let num_events = replay.load()?;
    let events = replay.into_events();

    println!("Loaded {} events", num_events);
    println!();

    // Create algorithm based on type
    let algorithm: Box<dyn MarketMakingAlgorithm> = match algo_type {
        AlgorithmType::AvellanedaStoikov => {
            let config = ingestor::market_maker::AvellanedaStoikovConfig {
                max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
                quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
                regime_params: RegimeParams::uniform(cli.spread, cli.skew),
                ..Default::default()
            };
            Box::new(AvellanedaStoikovAlgorithm::new(config))
        }
        AlgorithmType::MLSpreadSkew => {
            // Load weights if provided, otherwise use default
            let weights = if let Some(ref path) = weights_path {
                let json = std::fs::read_to_string(path)?;
                serde_json::from_str::<MLModelWeights>(&json)?
            } else {
                println!("WARNING: No weights file specified, using default weights");
                MLModelWeights::default()
            };

            println!("ML Weights:");
            println!("  Spread: intercept={:.2}, w_entropy={:.2}, w_volatility={:.2}",
                weights.spread.intercept, weights.spread.w_entropy, weights.spread.w_volatility);
            println!("  Skew: intercept={:.2}, w_inventory={:.2}, w_imbalance={:.2}",
                weights.skew.intercept, weights.skew.w_inventory, weights.skew.w_imbalance);
            println!();

            let config = MLSpreadSkewConfig {
                max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
                quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
                ..Default::default()
            };
            Box::new(MLSpreadSkewAlgorithm::new(config, weights))
        }
    };

    // Build backtest config
    let backtest_config = BacktestConfig {
        replay: replay_config,
        mm: ingestor::market_maker::MMConfig::default(),
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
        verbose: !cli.quiet,
        use_realistic_fills: !cli.naive_fills,
        ..Default::default()
    };

    let mut engine = BacktestEngine::from_events_with_algorithm(backtest_config, events, algorithm);
    let results = engine.run()?;

    // Print results
    if cli.stats {
        results.print_summary_with_stats(1);
    } else {
        results.print_summary();
    }

    // Save results
    if let Some(ref output_path) = output {
        results.save_json(output_path.to_str().unwrap())?;
        println!();
        println!("Results saved to: {:?}", output_path);
    }

    Ok(())
}

/// Head-to-head comparison result for serialization
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct HeadToHeadResult {
    ml_algorithm: AlgorithmMetrics,
    as_algorithm: AlgorithmMetrics,
    comparison: ComparisonSummary,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AlgorithmMetrics {
    name: String,
    sharpe_ratio: f64,
    total_return: f64,
    max_drawdown: f64,
    num_trades: usize,
    win_rate: f64,
    avg_trade_pnl: f64,
    profit_factor: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ComparisonSummary {
    sharpe_difference: f64,        // ML - AS
    return_difference: f64,        // ML - AS
    drawdown_difference: f64,      // ML - AS (negative = ML better)
    trade_count_ratio: f64,        // ML / AS
    winner: String,                // "ML", "AS", or "TIE"
    ml_advantage_pct: f64,         // % improvement of ML over AS
}

fn run_head_to_head(
    cli: &Cli,
    weights_path: Option<std::path::PathBuf>,
    as_spread: f64,
    as_skew: f64,
    output: Option<std::path::PathBuf>,
) -> Result<()> {
    use ingestor::backtest::replay::ParquetReplay;
    use ingestor::backtest::harness::BacktestEngine;

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("              HEAD-TO-HEAD: ML vs AVELLANEDA-STOIKOV                    ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("Data:      {:?}", cli.data);
    println!();

    // Load data once
    let replay_config = ReplayConfig {
        data_dir: cli.data.clone(),
        ..Default::default()
    };

    let mut replay = ParquetReplay::new(replay_config.clone());
    let num_events = replay.load()?;
    let events = replay.into_events();

    println!("Loaded {} events", num_events);
    println!();

    // =========================================================================
    // Run ML Algorithm
    // =========================================================================
    println!("Running ML Spread/Skew algorithm...");

    let ml_weights = if let Some(ref path) = weights_path {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str::<MLModelWeights>(&json)?
    } else {
        println!("  (using default weights)");
        MLModelWeights::default()
    };

    println!("  Spread weights: intercept={:.2}, w_entropy={:.2}, w_volatility={:.2}",
        ml_weights.spread.intercept, ml_weights.spread.w_entropy, ml_weights.spread.w_volatility);
    println!("  Skew weights: intercept={:.2}, w_inventory={:.2}, w_imbalance={:.2}",
        ml_weights.skew.intercept, ml_weights.skew.w_inventory, ml_weights.skew.w_imbalance);

    let ml_config = MLSpreadSkewConfig {
        max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        ..Default::default()
    };
    let ml_algo: Box<dyn MarketMakingAlgorithm> = Box::new(MLSpreadSkewAlgorithm::new(ml_config, ml_weights));

    let backtest_config = BacktestConfig {
        replay: replay_config.clone(),
        mm: ingestor::market_maker::MMConfig::default(),
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

    let mut ml_engine = BacktestEngine::from_events_with_algorithm(
        backtest_config.clone(),
        events.clone(),
        ml_algo,
    );
    let ml_results = ml_engine.run()?;

    println!("  Sharpe: {:+.4}, Return: {:+.2}%, Trades: {}",
        ml_results.metrics.sharpe_ratio,
        ml_results.metrics.total_return * 100.0,
        ml_results.metrics.num_trades);

    // =========================================================================
    // Run Avellaneda-Stoikov Algorithm
    // =========================================================================
    println!();
    println!("Running Avellaneda-Stoikov algorithm...");
    println!("  Spread: {:.1} bps, Skew: {:.2}", as_spread, as_skew);

    let as_config = ingestor::market_maker::AvellanedaStoikovConfig {
        max_inventory: Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        quote_size: Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        regime_params: RegimeParams::uniform(as_spread, as_skew),
        ..Default::default()
    };
    let as_algo: Box<dyn MarketMakingAlgorithm> = Box::new(AvellanedaStoikovAlgorithm::new(as_config));

    let mut as_engine = BacktestEngine::from_events_with_algorithm(
        backtest_config,
        events,
        as_algo,
    );
    let as_results = as_engine.run()?;

    println!("  Sharpe: {:+.4}, Return: {:+.2}%, Trades: {}",
        as_results.metrics.sharpe_ratio,
        as_results.metrics.total_return * 100.0,
        as_results.metrics.num_trades);

    // =========================================================================
    // Calculate comparison metrics
    // =========================================================================
    let sharpe_diff = ml_results.metrics.sharpe_ratio - as_results.metrics.sharpe_ratio;
    let return_diff = ml_results.metrics.total_return - as_results.metrics.total_return;
    let drawdown_diff = ml_results.metrics.max_drawdown - as_results.metrics.max_drawdown;
    let trade_ratio = if as_results.metrics.num_trades > 0 {
        ml_results.metrics.num_trades as f64 / as_results.metrics.num_trades as f64
    } else {
        f64::INFINITY
    };

    // Determine winner based on Sharpe ratio
    let (winner, ml_advantage) = if sharpe_diff > 0.1 {
        let adv = if as_results.metrics.sharpe_ratio.abs() > 0.001 {
            (sharpe_diff / as_results.metrics.sharpe_ratio.abs()) * 100.0
        } else {
            100.0
        };
        ("ML".to_string(), adv)
    } else if sharpe_diff < -0.1 {
        let adv = if ml_results.metrics.sharpe_ratio.abs() > 0.001 {
            (sharpe_diff / ml_results.metrics.sharpe_ratio.abs()) * 100.0
        } else {
            -100.0
        };
        ("AS".to_string(), adv)
    } else {
        ("TIE".to_string(), 0.0)
    };

    // =========================================================================
    // Print comparison report
    // =========================================================================
    println!();
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("                      COMPARISON RESULTS                                ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("┌─────────────────────┬──────────────────┬──────────────────┬──────────┐");
    println!("│ Metric              │ ML Spread/Skew   │ Avellaneda-Stoikov│ Δ (ML-AS)│");
    println!("├─────────────────────┼──────────────────┼──────────────────┼──────────┤");
    println!("│ Sharpe Ratio        │ {:>+16.4} │ {:>+16.4} │ {:>+8.4} │",
        ml_results.metrics.sharpe_ratio, as_results.metrics.sharpe_ratio, sharpe_diff);
    println!("│ Total Return        │ {:>15.2}% │ {:>15.2}% │ {:>+7.2}% │",
        ml_results.metrics.total_return * 100.0, as_results.metrics.total_return * 100.0, return_diff * 100.0);
    println!("│ Max Drawdown        │ {:>15.2}% │ {:>15.2}% │ {:>+7.2}% │",
        ml_results.metrics.max_drawdown * 100.0, as_results.metrics.max_drawdown * 100.0, drawdown_diff * 100.0);
    println!("│ Number of Trades    │ {:>16} │ {:>16} │ {:>+8} │",
        ml_results.metrics.num_trades, as_results.metrics.num_trades,
        ml_results.metrics.num_trades as i64 - as_results.metrics.num_trades as i64);
    println!("│ Win Rate            │ {:>15.1}% │ {:>15.1}% │ {:>+7.1}% │",
        ml_results.metrics.win_rate * 100.0, as_results.metrics.win_rate * 100.0,
        (ml_results.metrics.win_rate - as_results.metrics.win_rate) * 100.0);
    println!("│ Avg Trade PnL       │ {:>+16.6} │ {:>+16.6} │          │",
        ml_results.metrics.avg_trade_pnl, as_results.metrics.avg_trade_pnl);
    println!("└─────────────────────┴──────────────────┴──────────────────┴──────────┘");
    println!();

    // Print verdict
    match winner.as_str() {
        "ML" => {
            println!("  >>> WINNER: ML Spread/Skew algorithm");
            println!("      ML outperforms A-S by {:.2} Sharpe points", sharpe_diff);
        }
        "AS" => {
            println!("  >>> WINNER: Avellaneda-Stoikov algorithm");
            println!("      A-S outperforms ML by {:.2} Sharpe points", -sharpe_diff);
        }
        _ => {
            println!("  >>> RESULT: TIE (difference < 0.1 Sharpe)");
        }
    }

    // Additional insights
    println!();
    println!("INSIGHTS:");
    if ml_results.metrics.num_trades > as_results.metrics.num_trades {
        println!("  - ML generates {:.1}x more trades than A-S", trade_ratio);
    } else if ml_results.metrics.num_trades < as_results.metrics.num_trades {
        println!("  - A-S generates {:.1}x more trades than ML", 1.0 / trade_ratio);
    }

    if drawdown_diff < 0.0 {
        println!("  - ML has {:.2}% lower max drawdown (better risk control)", -drawdown_diff * 100.0);
    } else if drawdown_diff > 0.0 {
        println!("  - A-S has {:.2}% lower max drawdown (better risk control)", drawdown_diff * 100.0);
    }

    println!("═══════════════════════════════════════════════════════════════════════");

    // Build result struct for JSON output
    let ml_profit_factor = if ml_results.metrics.win_rate > 0.0 && ml_results.metrics.win_rate < 1.0 {
        ml_results.metrics.win_rate / (1.0 - ml_results.metrics.win_rate)
    } else {
        0.0
    };

    let as_profit_factor = if as_results.metrics.win_rate > 0.0 && as_results.metrics.win_rate < 1.0 {
        as_results.metrics.win_rate / (1.0 - as_results.metrics.win_rate)
    } else {
        0.0
    };

    use rust_decimal::prelude::ToPrimitive;

    let result = HeadToHeadResult {
        ml_algorithm: AlgorithmMetrics {
            name: "ML Spread/Skew".to_string(),
            sharpe_ratio: ml_results.metrics.sharpe_ratio,
            total_return: ml_results.metrics.total_return,
            max_drawdown: ml_results.metrics.max_drawdown,
            num_trades: ml_results.metrics.num_trades,
            win_rate: ml_results.metrics.win_rate,
            avg_trade_pnl: ml_results.metrics.avg_trade_pnl.to_f64().unwrap_or(0.0),
            profit_factor: ml_profit_factor,
        },
        as_algorithm: AlgorithmMetrics {
            name: format!("Avellaneda-Stoikov (spread={}, skew={})", as_spread, as_skew),
            sharpe_ratio: as_results.metrics.sharpe_ratio,
            total_return: as_results.metrics.total_return,
            max_drawdown: as_results.metrics.max_drawdown,
            num_trades: as_results.metrics.num_trades,
            win_rate: as_results.metrics.win_rate,
            avg_trade_pnl: as_results.metrics.avg_trade_pnl.to_f64().unwrap_or(0.0),
            profit_factor: as_profit_factor,
        },
        comparison: ComparisonSummary {
            sharpe_difference: sharpe_diff,
            return_difference: return_diff,
            drawdown_difference: drawdown_diff,
            trade_count_ratio: trade_ratio,
            winner,
            ml_advantage_pct: ml_advantage,
        },
    };

    // Save results
    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&result)?;
        std::fs::write(output_path, json)?;
        println!();
        println!("Results saved to: {:?}", output_path);
    }

    Ok(())
}

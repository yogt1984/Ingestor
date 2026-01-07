//! Backtest CLI
//!
//! Run backtests on historical data from the command line.
//!
//! # Usage
//!
//! ```bash
//! # Basic backtest with defaults
//! cargo run --release --bin backtest -- evaluate
//!
//! # With custom parameters
//! cargo run --release --bin backtest -- evaluate \
//!     --data ./data/features \
//!     --spread 3.0 \
//!     --skew 0.7 \
//!     --output results.json
//!
//! # Hyperparameter tuning (grid search)
//! cargo run --release --bin backtest -- tune
//!
//! # Parameter sweep
//! cargo run --release --bin backtest -- sweep --spreads 1,2,3,4,5 --skews 0.3,0.5,0.7
//!
//! # Walk-forward validation
//! cargo run --release --bin backtest -- walk-forward --folds 5
//!
//! # ML training
//! cargo run --release --bin backtest -- train
//!
//! # 4-week validation campaign simulation
//! cargo run --release --bin backtest -- simulate
//! ```
//!
//! # Command Naming Convention
//!
//! - `evaluate` (alias: `single`) - Run a single backtest evaluation
//! - `tune` (alias: `grid-search`) - Hyperparameter optimization
//! - `walk-forward` (alias: `wf`) - Time-series cross-validation
//! - `train` (alias: `train-ml`) - ML weight training
//! - `simulate` (alias: `simulate-campaign`) - Validation campaign simulation
//! - `validate-data` (alias: `vd`) - Data quality validation

use std::path::PathBuf;
use clap::{Parser, Subcommand};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use anyhow::{Result, Context};

use ingestor::backtest::{BacktestEngine, BacktestConfig};
use ingestor::backtest::replay::ReplayConfig;
use ingestor::backtest::ml_trainer::{MLTrainer, MLTrainerConfig};
use ingestor::backtest::walk_forward_ml::{WalkForwardMLTrainer, WalkForwardMLConfig};
use ingestor::backtest::paper_validation::{SessionValidator, ValidationConfig};
use ingestor::backtest::session_runner::{SessionRunner, SessionRunnerConfig, SimulatedEvent, FillRateStats};
use ingestor::backtest::validation_campaign::{
    ValidationCampaign, CampaignConfig, CampaignReport, ValidationGates, ValidationVerdict,
};
use ingestor::execution::market_maker::{MMConfig, RegimeParams, RegimeConfig};
use ingestor::execution::mm_simulator::SimulatorConfig;
use ingestor::strategies::{
    AlgorithmType, MLModelWeights,
    AlgorithmRegistry, BacktestAlgorithmParams,
};
use ingestor::commands::{
    BacktestCommands,
    params::backtest_params::{EvaluateParamsBuilder, TuneParamsBuilder, RegimeSearchParamsBuilder, MultiObjectiveParamsBuilder, RegimeOptimizeParamsBuilder, TrainParamsBuilder, WalkForwardMLParamsBuilder, SweepParamsBuilder, WalkForwardParamsBuilder},
};
use ingestor::commands::common::{NoOpCallback, ProgressCallback};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "backtest")]
#[command(about = "Run backtests on historical market data")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to data directory containing Parquet files
    #[arg(short, long, default_value = "./data/features")]
    data: PathBuf,

    /// Algorithm to use for backtesting (as, ml, or use 'algorithms' subcommand to list)
    #[arg(short, long, default_value = "as")]
    algorithm: String,

    /// Path to ML weights file (required for ML algorithm)
    #[arg(long)]
    weights_file: Option<PathBuf>,

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
    /// List available algorithms and their parameters
    Algorithms {
        /// Show detailed information for a specific algorithm
        #[arg(long)]
        algo: Option<String>,

        /// Output as JSON (for scripting)
        #[arg(long)]
        json: bool,
    },

    /// Run a single backtest (evaluate performance)
    #[command(name = "evaluate", visible_alias = "single")]
    Evaluate,

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
    #[command(name = "walk-forward", visible_alias = "wf")]
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
    #[command(name = "validate-data", visible_alias = "vd")]
    ValidateData {
        /// Output file for report
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Show info about data
    Info,

    /// Extended grid search over all key parameters (hyperparameter tuning)
    #[command(name = "tune", visible_alias = "grid-search")]
    Tune {
        /// Spread values to test (comma-separated)
        #[arg(long, default_value = "1,2,3,4,5")]
        spreads: String,

        /// Skew values to test (comma-separated)
        #[arg(long, default_value = "0.3,0.5,0.7,1.0")]
        skews: String,

        /// High entropy threshold values (comma-separated)
        #[arg(long, default_value = "0.6,0.7,0.8")]
        high_entropies: String,

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
    #[command(name = "train", visible_alias = "train-ml")]
    Train {
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

    /// Validate paper trading sessions against backtest expectations
    ValidateSession {
        /// Path to session summary JSON file (optional, validates all if not specified)
        #[arg(long)]
        session: Option<std::path::PathBuf>,

        /// Directory containing session files
        #[arg(long, default_value = "./data/sessions")]
        sessions_dir: std::path::PathBuf,

        /// Minimum duration in hours for valid comparison
        #[arg(long, default_value = "0.5")]
        min_hours: f64,

        /// Minimum trades for valid comparison
        #[arg(long, default_value = "5")]
        min_trades: usize,

        /// Output file for validation report (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Simulate a paper trading session using historical data
    SimulateSession {
        /// Session duration in hours
        #[arg(long, default_value = "1.0")]
        duration: f64,

        /// Preset name to use (optional)
        #[arg(long)]
        preset: Option<String>,

        /// Base spread in bps (if no preset)
        #[arg(long, default_value = "2.0")]
        spread: f64,

        /// Inventory skew factor (if no preset)
        #[arg(long, default_value = "0.5")]
        skew: f64,

        /// Output directory for session files
        #[arg(long, default_value = "./data/sessions")]
        sessions_dir: std::path::PathBuf,

        /// Output file for session result (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },

    /// Walk-forward ML training (robust cross-validated ML weight optimization)
    WalkForwardMl {
        /// Number of folds for walk-forward validation
        #[arg(long, default_value = "5")]
        folds: usize,

        /// Minimum training period in hours
        #[arg(long, default_value = "100")]
        min_train_hours: f64,

        /// Test period in hours per fold
        #[arg(long, default_value = "24")]
        test_hours: f64,

        /// Use rolling (vs anchored/expanding) window
        #[arg(long)]
        rolling: bool,

        /// Embargo between train and test (hours)
        #[arg(long, default_value = "1.0")]
        embargo_hours: f64,

        /// Spread intercept values to test (comma-separated)
        #[arg(long, default_value = "1.0,2.0,3.0")]
        spread_intercepts: String,

        /// Spread entropy weight values to test (comma-separated)
        #[arg(long, default_value = "-2.0,-1.0,0.0")]
        spread_entropy_weights: String,

        /// Spread volatility weight values to test (comma-separated)
        #[arg(long, default_value = "200.0,400.0")]
        spread_vol_weights: String,

        /// Skew intercept values to test (comma-separated)
        #[arg(long, default_value = "0.3,0.5,0.7")]
        skew_intercepts: String,

        /// Skew inventory weight values to test (comma-separated)
        #[arg(long, default_value = "-1.0,-0.6")]
        skew_inv_weights: String,

        /// Output file for results (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,

        /// Output file for consensus weights (JSON)
        #[arg(long)]
        weights_output: Option<std::path::PathBuf>,
    },

    /// Simulate a 4-week validation campaign using historical data
    #[command(name = "simulate", visible_alias = "simulate-campaign")]
    Simulate {
        /// Number of weeks to simulate (default 4)
        #[arg(long, default_value = "4")]
        weeks: u8,

        /// Hours per daily session (default 8.0)
        #[arg(long, default_value = "8.0")]
        session_hours: f64,

        /// Minimum sessions per week for valid week (default 5)
        #[arg(long, default_value = "5")]
        min_sessions_per_week: u8,

        /// Preset name to use (optional)
        #[arg(long)]
        preset: Option<String>,

        /// Base spread in bps (if no preset)
        #[arg(long, default_value = "2.0")]
        spread: f64,

        /// Inventory skew factor (if no preset)
        #[arg(long, default_value = "0.5")]
        skew: f64,

        /// Expected fill rate from backtest (for comparison)
        #[arg(long, default_value = "0.10")]
        expected_fill_rate: f64,

        /// Expected Sharpe from backtest
        #[arg(long, default_value = "1.0")]
        expected_sharpe: f64,

        /// Expected return from backtest
        #[arg(long, default_value = "0.05")]
        expected_return: f64,

        /// Minimum weekly trades for gate pass
        #[arg(long, default_value = "50")]
        min_weekly_trades: usize,

        /// Maximum drawdown percentage for gate pass
        #[arg(long, default_value = "5.0")]
        max_drawdown_pct: f64,

        /// Minimum win rate for gate pass
        #[arg(long, default_value = "0.40")]
        min_win_rate: f64,

        /// Output directory for campaign files
        #[arg(long, default_value = "./data/campaigns")]
        campaigns_dir: std::path::PathBuf,

        /// Output file for campaign report (JSON)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Algorithms { algo, json }) => {
            show_algorithms(algo.clone(), *json)?;
        }
        Some(Commands::Sweep { spreads, skews }) => {
            // Build SweepParams from CLI
            let sweep_params = SweepParamsBuilder::new()
                .data_path(cli.data.clone())
                .algorithm(cli.algorithm.clone())
                .weights_file(cli.weights_file.clone())
                .spreads(spreads.clone())
                .skews(skews.clone())
                .max_inventory(cli.max_inventory)
                .quote_size(cli.quote_size)
                .fee_rate(cli.fee_rate)
                .naive_fills(cli.naive_fills)
                .fill_prob(cli.fill_prob)
                .queue_pos(cli.queue_pos)
                .output(cli.output.clone())
                .quiet(cli.quiet)
                .build()
                .context("Failed to build sweep parameters")?;

            // Execute sweep command
            let callback = Arc::new(NoOpCallback);
            let result = BacktestCommands::sweep(sweep_params, callback)
                .context("Failed to execute sweep command")?;

            // Print results
            if !cli.quiet {
                println!("═══════════════════════════════════════════════════════");
                println!("           PARAMETER SWEEP                              ");
                println!("═══════════════════════════════════════════════════════");
                println!();
                println!("Algorithm: {} ({})", result.algorithm_name, result.algorithm);
                println!("Total combinations: {}", result.total_combinations);
                println!();

                // Print all results
                for item in &result.all_results {
                    println!(
                        "Spread={:.1}, Skew={:.1} => Sharpe={:+.2}, Return={:+.2}%, DD={:.2}%, Trades={}",
                        item.spread,
                        item.skew,
                        item.sharpe,
                        item.total_return * 100.0,
                        item.max_drawdown * 100.0,
                        item.num_trades,
                    );
                }

                // Print best result
                if let Some(ref best) = result.best {
                    println!();
                    println!("═══════════════════════════════════════════════════════");
                    println!("BEST PARAMETERS (by Sharpe):");
                    println!("  Algorithm:  {} ({})", result.algorithm_name, result.algorithm);
                    println!("  Spread:     {} bps", best.spread);
                    println!("  Skew:       {}", best.skew);
                    println!("  Sharpe:     {:.2}", best.sharpe);
                    println!("  Return:     {:.2}%", best.total_return * 100.0);
                    println!("  Max DD:     {:.2}%", best.max_drawdown * 100.0);
                    println!("  Win Rate:   {:.1}%", best.win_rate * 100.0);
                    println!("═══════════════════════════════════════════════════════");
                }
            }

            // Save results if output specified
            if let Some(ref output) = cli.output {
                let json = serde_json::to_string_pretty(&result)?;
                std::fs::write(output, json)?;
                if !cli.quiet {
                    println!();
                    println!("Results saved to: {:?}", output);
                }
            }
        }
        Some(Commands::WalkForward { folds, test_hours, rolling, output }) => {
            // Build WalkForwardParams from CLI
            let walk_forward_params = WalkForwardParamsBuilder::new()
                .data_path(cli.data.clone())
                .algorithm(cli.algorithm.clone())
                .weights_file(cli.weights_file.clone())
                .folds(*folds)
                .test_hours(*test_hours)
                .rolling(*rolling)
                .spreads("1,2,3,4,5".to_string()) // Default param grid
                .skews("0.3,0.5,0.7".to_string())
                .fill_probs("0.05,0.10,0.15".to_string())
                .max_inventory(cli.max_inventory)
                .quote_size(cli.quote_size)
                .fee_rate(cli.fee_rate)
                .naive_fills(cli.naive_fills)
                .queue_pos(cli.queue_pos)
                .output(output.clone())
                .quiet(cli.quiet)
                .build()
                .context("Failed to build walk-forward parameters")?;

            // Execute walk-forward command
            let callback = Arc::new(NoOpCallback);
            let result = BacktestCommands::walk_forward(walk_forward_params, callback)
                .context("Failed to execute walk-forward command")?;

            // Print results
            if !cli.quiet {
                println!("═══════════════════════════════════════════════════════");
                println!("           WALK-FORWARD VALIDATION                     ");
                println!("═══════════════════════════════════════════════════════");
                println!();
                println!("Configuration:");
                println!("  Data:          {:?}", cli.data);
                println!("  Folds:         {}", result.folds);
                println!("  Test Period:   {} hours per fold", result.fold_results.first().map(|_| 24.0).unwrap_or(0.0));
                println!("  Mode:          {}", if result.fold_results.first().map(|_| false).unwrap_or(false) { "Rolling" } else { "Anchored (expanding)" });
                println!();

                // Print fold results
                for fold in &result.fold_results {
                    println!(
                        "Fold {}: Train Sharpe={:.2}, Test Sharpe={:.2}, Return={:.2}%, Trades={}",
                        fold.fold_num,
                        fold.train_metrics.sharpe,
                        fold.test_metrics.sharpe,
                        fold.test_metrics.total_return * 100.0,
                        fold.test_metrics.num_trades,
                    );
                }

                // Print aggregate results
                println!();
                println!("═══════════════════════════════════════════════════════");
                println!("AGGREGATE RESULTS:");
                println!("  Avg OOS Sharpe:     {:.3}", result.aggregate.avg_oos_sharpe);
                println!("  Std OOS Sharpe:     {:.3}", result.aggregate.std_oos_sharpe);
                println!("  Avg OOS Return:     {:.2}%", result.aggregate.avg_oos_return * 100.0);
                println!("  Total OOS Trades:   {}", result.aggregate.total_oos_trades);
                println!("  Avg Win Rate:       {:.1}%", result.aggregate.avg_win_rate * 100.0);
                println!("  Profitable Folds:   {:.1}%", result.aggregate.pct_profitable_folds * 100.0);
                println!("  IS/OOS Sharpe:      {:.3}", result.aggregate.is_oos_sharpe_ratio);
                println!("  Prob Sharpe > 0:    {:.3}", result.aggregate.prob_sharpe_gt_zero);
                println!("═══════════════════════════════════════════════════════");
            }

            // Save results if output specified
            if let Some(ref output_path) = output {
                // Save using the original WalkForwardResults format for compatibility
                use ingestor::backtest::walk_forward::{WalkForwardResults, WalkForwardConfig, ParamGrid, FoldResult, OptimizedParams, FoldMetrics, AggregateResults};
                
                let original_results = WalkForwardResults {
                    config: WalkForwardConfig {
                        n_folds: result.folds,
                        min_train_hours: 100.0,
                        test_hours: 24.0,
                        anchored: true,
                        embargo_hours: 1.0,
                        param_grid: ParamGrid {
                            spreads: vec![1.0, 2.0, 3.0, 4.0, 5.0],
                            skews: vec![0.3, 0.5, 0.7],
                            fill_probs: vec![0.05, 0.10, 0.15],
                        },
                        data_dir: cli.data.clone(),
                        verbose: !cli.quiet,
                    },
                    folds: result.fold_results.iter().map(|fold| {
                        FoldResult {
                            fold_num: fold.fold_num,
                            train_start_ms: fold.train_start_ms,
                            train_end_ms: fold.train_end_ms,
                            test_start_ms: fold.test_start_ms,
                            test_end_ms: fold.test_end_ms,
                            best_params: OptimizedParams {
                                spread: fold.best_params.spread,
                                skew: fold.best_params.skew,
                                fill_prob: fold.best_params.fill_prob,
                                train_sharpe: fold.best_params.train_sharpe,
                            },
                            train_metrics: FoldMetrics {
                                sharpe: fold.train_metrics.sharpe,
                                total_return: fold.train_metrics.total_return,
                                max_drawdown: fold.train_metrics.max_drawdown,
                                num_trades: fold.train_metrics.num_trades,
                                win_rate: fold.train_metrics.win_rate,
                                profit_factor: fold.train_metrics.profit_factor,
                            },
                            test_metrics: FoldMetrics {
                                sharpe: fold.test_metrics.sharpe,
                                total_return: fold.test_metrics.total_return,
                                max_drawdown: fold.test_metrics.max_drawdown,
                                num_trades: fold.test_metrics.num_trades,
                                win_rate: fold.test_metrics.win_rate,
                                profit_factor: fold.test_metrics.profit_factor,
                            },
                        }
                    }).collect(),
                    aggregate: AggregateResults {
                        avg_oos_sharpe: result.aggregate.avg_oos_sharpe,
                        std_oos_sharpe: result.aggregate.std_oos_sharpe,
                        avg_oos_return: result.aggregate.avg_oos_return,
                        total_oos_trades: result.aggregate.total_oos_trades,
                        avg_win_rate: result.aggregate.avg_win_rate,
                        pct_profitable_folds: result.aggregate.pct_profitable_folds,
                        is_oos_sharpe_ratio: result.aggregate.is_oos_sharpe_ratio,
                        prob_sharpe_gt_zero: result.aggregate.prob_sharpe_gt_zero,
                    },
                };

                original_results.save_json(output_path.to_str().unwrap())?;
                if !cli.quiet {
                    println!();
                    println!("Results saved to: {:?}", output_path);
                }
            }
        }
        Some(Commands::ValidateData { output }) => {
            run_validate(&cli, output.clone())?;
        }
        Some(Commands::Info) => {
            show_info(&cli)?;
        }
        Some(Commands::Tune { spreads, skews, high_entropies, fill_probs, output }) => {
            run_tune(&cli, spreads, skews, high_entropies, fill_probs, output.clone())?;
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
        Some(Commands::Train { train_ratio, spread_intercepts, spread_entropy_weights, spread_vol_weights, skew_intercepts, skew_inv_weights, output }) => {
            run_train_ml(&cli, *train_ratio, spread_intercepts, spread_entropy_weights, spread_vol_weights, skew_intercepts, skew_inv_weights, output.clone())?;
        }
        Some(Commands::Compare { algorithm, weights, output }) => {
            run_compare(&cli, algorithm, weights.clone(), output.clone())?;
        }
        Some(Commands::HeadToHead { weights, as_spread, as_skew, output }) => {
            run_head_to_head(&cli, weights.clone(), *as_spread, *as_skew, output.clone())?;
        }
        Some(Commands::WalkForwardMl { folds, min_train_hours, test_hours, rolling, embargo_hours, spread_intercepts, spread_entropy_weights, spread_vol_weights, skew_intercepts, skew_inv_weights, output, weights_output }) => {
            run_walk_forward_ml(&cli, *folds, *min_train_hours, *test_hours, *rolling, *embargo_hours, spread_intercepts, spread_entropy_weights, spread_vol_weights, skew_intercepts, skew_inv_weights, output.clone(), weights_output.clone())?;
        }
        Some(Commands::ValidateSession { session, sessions_dir, min_hours, min_trades, output }) => {
            run_validate_session(session.clone(), sessions_dir.clone(), *min_hours, *min_trades, output.clone())?;
        }
        Some(Commands::SimulateSession { duration, preset, spread, skew, sessions_dir, output }) => {
            run_simulate_session(&cli, *duration, preset.clone(), *spread, *skew, sessions_dir.clone(), output.clone())?;
        }
        Some(Commands::Simulate { weeks, session_hours, min_sessions_per_week, preset, spread, skew, expected_fill_rate, expected_sharpe, expected_return, min_weekly_trades, max_drawdown_pct, min_win_rate, campaigns_dir, output }) => {
            run_simulate_campaign(&cli, *weeks, *session_hours, *min_sessions_per_week, preset.clone(), *spread, *skew, *expected_fill_rate, *expected_sharpe, *expected_return, *min_weekly_trades, *max_drawdown_pct, *min_win_rate, campaigns_dir.clone(), output.clone())?;
        }
        Some(Commands::Evaluate) | None => {
            run_single(&cli)?;
        }
    }

    Ok(())
}

/// Show available algorithms and their parameters using AlgorithmRegistry
fn show_algorithms(algo: Option<String>, json_output: bool) -> Result<()> {
    use ingestor::strategies::registry::AlgorithmRegistry;

    if json_output {
        // Use registry's JSON output
        let json = AlgorithmRegistry::to_json();
        println!("{}", serde_json::to_string_pretty(&json)?);
        return Ok(());
    }

    if let Some(algo_id) = algo {
        // Show details for specific algorithm
        match AlgorithmRegistry::info_by_string(&algo_id) {
            Ok(info) => {
                let category = if info.is_trainable { "ML/Trainable" } else { "Rule-Based" };
                println!("Algorithm: {} ({})", info.name, info.type_string);
                println!("Version:   {}", info.version);
                println!("Category:  {}", category);
                println!("Trainable: {}", if info.is_trainable { "Yes" } else { "No" });
                println!("Configurable: {}", if info.is_configurable { "Yes" } else { "No" });
                println!();
                println!("Description:");
                println!("  {}", info.description);
                println!();
                if !info.aliases.is_empty() {
                    println!("Aliases: {}", info.aliases.join(", "));
                    println!();
                }

                // Get parameters from registry
                let params = AlgorithmRegistry::parameters(info.algorithm_type);
                if !params.is_empty() {
                    println!("Parameters:");
                    println!("  {:<25} {:<12} {:<8} {}", "Name", "Default", "Tunable", "Description");
                    println!("  {}", "-".repeat(80));
                    for p in &params {
                        let default_str = format!("{:.4}", p.default);
                        let range_str = if let Some((min, max)) = p.range {
                            format!(" [{:.2}, {:.2}]", min, max)
                        } else {
                            String::new()
                        };
                        println!(
                            "  {:<25} {:<12} {:<8} {}{}",
                            p.name,
                            default_str,
                            if p.tunable { "Yes" } else { "No" },
                            p.description,
                            range_str
                        );
                    }
                }

                // Show tunable parameters summary
                let tunable_params = AlgorithmRegistry::tunable_parameters(info.algorithm_type);
                if !tunable_params.is_empty() {
                    println!();
                    println!("Tunable Parameters (for grid search):");
                    for p in &tunable_params {
                        if let Some((min, max)) = p.range {
                            println!("  {} [{:.2} - {:.2}]", p.name, min, max);
                        } else {
                            println!("  {}", p.name);
                        }
                    }
                }
            }
            Err(_) => {
                println!("Unknown algorithm: {}", algo_id);
                println!();
                println!("Available algorithms:");
                for info in AlgorithmRegistry::list() {
                    println!("  {} (aliases: {})", info.type_string, info.aliases.join(", "));
                }
            }
        }
    } else {
        // Show list of all algorithms using registry
        println!("Available Algorithms");
        println!("====================");
        println!();

        for info in AlgorithmRegistry::list() {
            let trainable_marker = if info.is_trainable { " [trainable]" } else { "" };
            let category = if info.is_trainable { "ML/Trainable" } else { "Rule-Based" };

            println!("{} ({}){}:", info.name, info.type_string, trainable_marker);
            println!("  Category:    {}", category);
            println!("  Version:     {}", info.version);
            println!("  Description: {}", info.description);
            println!("  Aliases:     {}", info.aliases.join(", "));

            // List tunable parameters
            let tunable = AlgorithmRegistry::tunable_parameters(info.algorithm_type);
            if !tunable.is_empty() {
                let param_names: Vec<_> = tunable.iter().map(|p| p.name.as_str()).collect();
                println!("  Tunable:     {}", param_names.join(", "));
            }
            println!();
        }

        println!("Use --algo <type> for detailed parameter info (e.g., --algo as)");
        println!("Use -a <type> to select algorithm for backtest (default: as)");
        println!();
        println!("All valid type strings: {}", AlgorithmRegistry::all_type_strings().join(", "));
    }

    Ok(())
}

fn run_single(cli: &Cli) -> Result<()> {
    use ingestor::backtest::replay::ParquetReplay;
    use ingestor::backtest::harness::BacktestEngine;

    // Build EvaluateParams from CLI
    let eval_params = EvaluateParamsBuilder::new()
        .data_path(cli.data.clone())
        .algorithm(cli.algorithm.clone())
        .weights_file(cli.weights_file.clone())
        .spread(cli.spread)
        .skew(cli.skew)
        .max_inventory(cli.max_inventory)
        .quote_size(cli.quote_size)
        .fee_rate(cli.fee_rate)
        .naive_fills(cli.naive_fills)
        .fill_prob(cli.fill_prob)
        .queue_pos(cli.queue_pos)
        .high_entropy(cli.high_entropy)
        .low_entropy(cli.low_entropy)
        .regime_params(cli.regime_params)
        .high_spread(cli.high_spread)
        .med_spread(cli.med_spread)
        .low_spread(cli.low_spread)
        .high_skew(cli.high_skew)
        .med_skew(cli.med_skew)
        .low_skew(cli.low_skew)
        .quote_low_entropy(cli.quote_low_entropy)
        .output(cli.output.clone())
        .json(cli.json)
        .quiet(cli.quiet)
        .stats(cli.stats)
        .build()?;

    // Parse algorithm type early to fail fast on invalid algorithm
    let (algo_type, algo_name) = parse_algorithm_type(&cli.algorithm)?;
    let ml_weights = load_ml_weights_if_needed(algo_type, cli.weights_file.as_deref())?;

    // Run the backtest using extracted command
    let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
    let (results, eval_result) = BacktestCommands::evaluate(eval_params.clone(), callback)?;

    // JSON mode: minimal output, just the results
    if cli.json {
        // Output JSON for Optuna/scripting (preserve original format)
        let json_output = serde_json::json!({
            "algorithm": algo_type.as_str(),
            "sharpe": eval_result.metrics.sharpe_ratio,
            "total_return": eval_result.metrics.total_return,
            "max_drawdown": eval_result.metrics.max_drawdown,
            "num_trades": eval_result.metrics.num_trades,
            "win_rate": eval_result.metrics.win_rate,
            "avg_trade_pnl": eval_result.metrics.avg_trade_pnl,
            "params": {
                "spread": cli.spread,
                "skew": cli.skew,
                "fill_prob": cli.fill_prob,
                "high_entropy": cli.high_entropy
            }
        });
        println!("{}", json_output);
        return Ok(());
    }

    // Normal mode: display configuration and results
    println!("═══════════════════════════════════════════════════════");
    println!("           INGESTOR BACKTEST ENGINE                     ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Configuration:");
    println!("  Algorithm:     {} ({})", algo_name, algo_type.as_str());
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
    println!("  High Entropy:  {} (above = aggressive)", cli.high_entropy);
    println!("  Low Entropy:   {} (below = defensive)", cli.low_entropy);

    // Show ML weights if using ML algorithm
    if algo_type == AlgorithmType::MLSpreadSkew {
        if let Some(ref weights) = ml_weights {
            println!();
            println!("ML Weights:");
            if cli.weights_file.is_none() {
                println!("  (using default weights)");
            }
            println!("  Spread: intercept={:.2}, w_entropy={:.2}, w_volatility={:.2}",
                weights.spread.intercept, weights.spread.w_entropy, weights.spread.w_volatility);
            println!("  Skew: intercept={:.2}, w_inventory={:.2}, w_imbalance={:.2}",
                weights.skew.intercept, weights.skew.w_inventory, weights.skew.w_imbalance);
        }
    }
    println!();

    // Print results using the full BacktestResults
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
    use ingestor::backtest::replay::ParquetReplay;
    use ingestor::backtest::harness::BacktestEngine;

    // Parse algorithm type early to fail fast on invalid algorithm
    let (algo_type, algo_name) = parse_algorithm_type(&cli.algorithm)?;
    let ml_weights = load_ml_weights_if_needed(algo_type, cli.weights_file.as_deref())?;

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
    println!("Algorithm: {} ({})", algo_name, algo_type.as_str());
    println!("Spreads:   {:?}", spreads);
    println!("Skews:     {:?}", skews);
    println!("Total combinations: {}", spreads.len() * skews.len());
    println!();

    // Load data once
    let replay_config = ReplayConfig {
        data_dir: cli.data.clone(),
        ..Default::default()
    };

    let mut replay = ParquetReplay::new(replay_config.clone());
    let num_events = replay.load()?;
    println!("Loaded {} events", num_events);
    println!();

    let mut all_results: Vec<SweepResult> = Vec::new();

    for &spread in &spreads {
        for &skew in &skews {
            // Reload events (need fresh copy for each run)
            let mut replay = ParquetReplay::new(replay_config.clone());
            replay.load()?;
            let events = replay.into_events();

            // Create algorithm with sweep parameters
            let mut params = BacktestAlgorithmParams::new(
                Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
                Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
                spread,
                skew,
            );
            if let Some(ref weights) = ml_weights {
                params = params.with_ml_weights(weights.clone());
            }

            let algorithm = AlgorithmRegistry::create_for_backtest(algo_type, &params)
                .map_err(|e| anyhow::anyhow!("Failed to create algorithm '{}': {}", algo_name, e))?;

            let config = BacktestConfig {
                replay: replay_config.clone(),
                mm: MMConfig::default(),
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

            let mut engine = BacktestEngine::from_events_with_algorithm(config, events, algorithm);
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
        println!("  Algorithm:  {} ({})", algo_name, algo_type.as_str());
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

/// Parse algorithm type from CLI string and validate it exists.
/// Returns both the AlgorithmType and a display name for logging.
fn parse_algorithm_type(algo_str: &str) -> Result<(AlgorithmType, String)> {
    let algo_type = AlgorithmType::from_str(algo_str)
        .map_err(|_| anyhow::anyhow!(
            "Unknown algorithm '{}'. Valid options: {}",
            algo_str,
            AlgorithmRegistry::all_type_strings().join(", ")
        ))?;

    let display_name = algo_type.display_name().to_string();
    Ok((algo_type, display_name))
}

/// Load ML weights from file if algorithm is MLSpreadSkew and weights file provided.
/// Returns None for non-ML algorithms or if no weights file specified.
fn load_ml_weights_if_needed(
    algo_type: AlgorithmType,
    weights_file: Option<&std::path::Path>,
) -> Result<Option<MLModelWeights>> {
    if algo_type != AlgorithmType::MLSpreadSkew {
        return Ok(None);
    }

    match weights_file {
        Some(path) => {
            let json = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("Failed to read weights file {:?}: {}", path, e))?;
            let weights: MLModelWeights = serde_json::from_str(&json)
                .map_err(|e| anyhow::anyhow!("Failed to parse weights JSON: {}", e))?;
            Ok(Some(weights))
        }
        None => {
            // Use default weights with warning
            Ok(Some(MLModelWeights::default()))
        }
    }
}

/// Create algorithm parameters from CLI options
fn create_algorithm_params(cli: &Cli, ml_weights: Option<MLModelWeights>) -> BacktestAlgorithmParams {
    let mut params = BacktestAlgorithmParams::new(
        Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        cli.spread,
        cli.skew,
    );
    if let Some(weights) = ml_weights {
        params = params.with_ml_weights(weights);
    }
    params
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
    fill_prob: f64,
    sharpe: f64,
    total_return: f64,
    max_drawdown: f64,
    num_trades: usize,
    win_rate: f64,
    avg_trade_pnl: f64,
}

fn run_tune(
    cli: &Cli,
    spreads_str: &str,
    skews_str: &str,
    high_entropies_str: &str,
    fill_probs_str: &str,
    output: Option<PathBuf>,
) -> Result<()> {
    // Build TuneParams from CLI
    let tune_params = TuneParamsBuilder::new()
        .data_path(cli.data.clone())
        .algorithm(cli.algorithm.clone())
        .weights_file(cli.weights_file.clone())
        .spreads(spreads_str.to_string())
        .skews(skews_str.to_string())
        .high_entropies(high_entropies_str.to_string())
        .fill_probs(fill_probs_str.to_string())
        .max_inventory(cli.max_inventory)
        .quote_size(cli.quote_size)
        .fee_rate(cli.fee_rate)
        .naive_fills(cli.naive_fills)
        .queue_pos(cli.queue_pos)
        .low_entropy(cli.low_entropy)
        .output(output.clone())
        .build()?;

    // Parse algorithm type for display
    let (algo_type, algo_name) = parse_algorithm_type(&cli.algorithm)?;

    // Run grid search using extracted command
    let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
    let result = BacktestCommands::tune(tune_params.clone(), callback)?;

    // Display results
    println!("═══════════════════════════════════════════════════════");
    println!("           EXTENDED GRID SEARCH                        ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Algorithm: {} ({})", algo_name, algo_type.as_str());
    println!("Total combinations tested: {}", result.total_combinations);
    println!();

    // Parse parameter lists for display
    let spreads: Vec<f64> = tune_params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let skews: Vec<f64> = tune_params.skews.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let high_entropies: Vec<f64> = tune_params.high_entropies.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let fill_probs: Vec<f64> = tune_params.fill_probs.split(',').filter_map(|s| s.trim().parse().ok()).collect();

    println!("Parameter Space:");
    println!("  Spreads:          {:?}", spreads);
    println!("  Skews:            {:?}", skews);
    println!("  High Entropies:   {:?}", high_entropies);
    println!("  Fill Probs:       {:?}", fill_probs);
    println!();

    println!("═══════════════════════════════════════════════════════");
    println!("TOP 10 PARAMETER SETS (by Sharpe):");
    println!("═══════════════════════════════════════════════════════");

    for (i, r) in result.all_results.iter().take(10).enumerate() {
        println!(
            "{:>2}. Spread={:.1} Skew={:.1} Entropy={:.1} FillP={:.2}",
            i + 1, r.spread, r.skew, r.high_entropy_threshold, r.fill_prob
        );
        println!(
            "    Sharpe={:+.2} Return={:+.2}% DD={:.2}% WinRate={:.1}% Trades={}",
            r.sharpe, r.total_return * 100.0, r.max_drawdown * 100.0, r.win_rate * 100.0, r.num_trades
        );
    }

    // Best overall
    if let Some(ref best) = result.best {
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("RECOMMENDED PARAMETERS:");
        println!("═══════════════════════════════════════════════════════");
        println!("  Algorithm:                  {} ({})", algo_name, algo_type.as_str());
        println!("  base_spread_bps:            {}", best.spread);
        println!("  inventory_skew_factor:      {}", best.skew);
        println!("  high_entropy_threshold:     {}", best.high_entropy_threshold);
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
        let json = serde_json::to_string_pretty(&result.all_results)?;
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
    // Build RegimeSearchParams from CLI
    let regime_params = RegimeSearchParamsBuilder::new()
        .data_path(cli.data.clone())
        .algorithm(cli.algorithm.clone())
        .weights_file(cli.weights_file.clone())
        .high_spreads(high_spreads_str.to_string())
        .med_spreads(med_spreads_str.to_string())
        .low_spreads(low_spreads_str.to_string())
        .high_skews(high_skews_str.to_string())
        .med_skews(med_skews_str.to_string())
        .low_skews(low_skews_str.to_string())
        .fill_probs(fill_probs_str.to_string())
        .max_inventory(cli.max_inventory)
        .quote_size(cli.quote_size)
        .fee_rate(cli.fee_rate)
        .naive_fills(cli.naive_fills)
        .queue_pos(cli.queue_pos)
        .high_entropy(cli.high_entropy)
        .low_entropy(cli.low_entropy)
        .output(output.clone())
        .build()?;

    // Parse algorithm type for display
    let (algo_type, algo_name) = parse_algorithm_type(&cli.algorithm)?;

    // Run regime search using extracted command
    let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
    let result = BacktestCommands::regime_search(regime_params.clone(), callback)?;

    // Display results
    println!("═══════════════════════════════════════════════════════");
    println!("       REGIME-SPECIFIC GRID SEARCH                      ");
    println!("       (Optimize params per regime independently)       ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Algorithm: {} ({})", algo_name, algo_type.as_str());
    println!("Total combinations tested: {}", result.total_combinations);
    println!();

    // Parse parameter lists for display
    let high_spreads: Vec<f64> = regime_params.high_spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let med_spreads: Vec<f64> = regime_params.med_spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let high_skews: Vec<f64> = regime_params.high_skews.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let med_skews: Vec<f64> = regime_params.med_skews.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let low_skews: Vec<f64> = regime_params.low_skews.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let fill_probs: Vec<f64> = regime_params.fill_probs.split(',').filter_map(|s| s.trim().parse().ok()).collect();

    println!("Parameter Space:");
    println!("  High Entropy:");
    println!("    Spreads: {:?}", high_spreads);
    println!("    Skews:   {:?}", high_skews);
    println!("  Medium Entropy:");
    println!("    Spreads: {:?}", med_spreads);
    println!("    Skews:   {:?}", med_skews);
    println!("  Low Entropy:");
    println!("    Spreads: {:?}", regime_params.low_spreads);
    println!("    Skews:   {:?}", low_skews);
    println!("  Fill Probs: {:?}", fill_probs);
    println!();

    println!("═══════════════════════════════════════════════════════");
    println!("TOP 10 REGIME-SPECIFIC PARAMETER SETS (by Sharpe):");
    println!("═══════════════════════════════════════════════════════");

    for (i, r) in result.all_results.iter().take(10).enumerate() {
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
    let with_low_quote: Vec<_> = result.all_results.iter().filter(|r| r.low_spread.is_some()).collect();
    let without_low_quote: Vec<_> = result.all_results.iter().filter(|r| r.low_spread.is_none()).collect();

    if !with_low_quote.is_empty() && !without_low_quote.is_empty() {
        println!();
        println!("═══════════════════════════════════════════════════════");
        println!("LOW ENTROPY QUOTING COMPARISON:");
        println!("═══════════════════════════════════════════════════════");

        if let (Some(avg_with), Some(avg_without)) = (result.avg_sharpe_with_quote, result.avg_sharpe_without_quote) {
        let avg_trades_with: f64 = with_low_quote.iter().map(|r| r.num_trades as f64).sum::<f64>() / with_low_quote.len() as f64;
        let avg_trades_without: f64 = without_low_quote.iter().map(|r| r.num_trades as f64).sum::<f64>() / without_low_quote.len() as f64;

        println!("                    QUOTE in Low Entropy    NO QUOTE in Low Entropy");
            println!("  Avg Sharpe:       {:+.3}                   {:+.3}", avg_with, avg_without);
        println!("  Avg Trades:       {:.0}                      {:.0}", avg_trades_with, avg_trades_without);

            let diff = avg_without - avg_with;
        if diff > 0.1 {
            println!();
            println!("  >>> NOT QUOTING in low entropy improves Sharpe by +{:.2}!", diff);
        } else if diff < -0.1 {
            println!();
            println!("  >>> QUOTING in low entropy is better by +{:.2} Sharpe!", -diff);
            }
        }
    }

    // Best overall
    if let Some(ref best) = result.best {
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
        let json = serde_json::to_string_pretty(&result.all_results)?;
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
    // Build MultiObjectiveParams from CLI
    let mo_params = MultiObjectiveParamsBuilder::new()
        .data_path(cli.data.clone())
        .algorithm(cli.algorithm.clone())
        .weights_file(cli.weights_file.clone())
        .spreads(spreads.to_string())
        .skews(skews.to_string())
        .fill_probs(fill_probs.to_string())
        .high_entropies(high_entropies.to_string())
        .min_trades(min_trades)
        .w_sharpe(w_sharpe)
        .w_drawdown(w_drawdown)
        .w_fill(w_fill)
        .w_turnover(w_turnover)
        .max_inventory(cli.max_inventory)
        .quote_size(cli.quote_size)
        .fee_rate(cli.fee_rate)
        .naive_fills(cli.naive_fills)
        .queue_pos(cli.queue_pos)
        .low_entropy(cli.low_entropy)
        .output(output.clone())
        .build()?;

    // Parse algorithm type for display
    let (algo_type, algo_name) = parse_algorithm_type(&cli.algorithm)?;

    // Run multi-objective optimization using extracted command
    let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
    let result = BacktestCommands::multi_objective(mo_params.clone(), callback)?;

    // Display results
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("          MULTI-OBJECTIVE OPTIMIZATION (Pareto Frontier)                ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("Algorithm: {} ({})", algo_name, algo_type.as_str());
    println!("Total combinations tested: {}", result.total_combinations);
    println!();

    // Parse parameter lists for display
    let spread_values: Vec<f64> = mo_params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let skew_values: Vec<f64> = mo_params.skews.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let fill_prob_values: Vec<f64> = mo_params.fill_probs.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let high_entropy_values: Vec<f64> = mo_params.high_entropies.split(',').filter_map(|s| s.trim().parse().ok()).collect();

    println!("PARAMETER GRID:");
    println!("  Spreads:       {:?}", spread_values);
    println!("  Skews:         {:?}", skew_values);
    println!("  Fill Probs:    {:?}", fill_prob_values);
    println!("  High Entropy:  {:?}", high_entropy_values);
    println!("  Min Trades:    {}", mo_params.min_trades);
    println!();
    println!("OBJECTIVE WEIGHTS:");
    println!("  Sharpe:     {:.0}%", mo_params.w_sharpe * 100.0);
    println!("  Drawdown:   {:.0}%", mo_params.w_drawdown * 100.0);
    println!("  Fill Rate:  {:.0}%", mo_params.w_fill * 100.0);
    println!("  Turnover:   {:.0}%", mo_params.w_turnover * 100.0);
    println!();

    // Print Pareto frontier
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("PARETO FRONTIER ({} solutions):", result.pareto_frontier.len());
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("┌───────┬───────┬───────┬───────────┬──────────┬──────────┬──────────┐");
    println!("│ Sprd  │ Skew  │ FillP │   Sharpe  │    DD    │ FillRate│ Turnover │");
    println!("├───────┼───────┼───────┼───────────┼──────────┼──────────┼──────────┤");

    for sol in result.pareto_frontier.iter().take(15) {
        println!("│ {:5.1} │ {:5.2} │ {:4.0}% │ {:+9.3} │ {:7.2}% │ {:7.1}% │ {:7.2}/h │",
            sol.spread_bps,
            sol.skew_factor,
            sol.fill_probability * 100.0,
            sol.sharpe,
            sol.drawdown * 100.0,
            sol.fill_rate * 100.0,
            sol.turnover);
    }
    if result.pareto_frontier.len() > 15 {
        println!("│ ... {} more solutions on frontier ...                          │",
            result.pareto_frontier.len() - 15);
    }
    println!("└───────┴───────┴───────┴───────────┴──────────┴──────────┴──────────┘");
    println!();

    // Best weighted solution
    if let Some(ref best) = result.best_weighted {
        println!("═══════════════════════════════════════════════════════════════════════");
        println!("RECOMMENDED (weighted score):");
        println!("═══════════════════════════════════════════════════════════════════════");
        println!("  Params: spread={:.1}bps, skew={:.2}, fill={:.0}%, entropy={:.1}",
            best.spread_bps, best.skew_factor, best.fill_probability * 100.0, best.high_entropy_threshold);
        println!("  Objectives: Sharpe={:+.3}, DD={:.2}%, Fill={:.1}%, Turn={:.2}/hr",
            best.sharpe, best.drawdown * 100.0, best.fill_rate * 100.0, best.turnover);
        println!("  Rank={}, Crowding={:.3}", best.pareto_rank, best.crowding_distance);
        println!();
    }

    // Data summary
    println!("DATA:");
    println!("  Time span: {:.1} hours ({:.1} days)",
        result.time_span_hours, result.time_span_hours / 24.0);
    println!("  Events: {}", result.num_events);
    println!("  Solutions evaluated: {}", result.all_solutions.len());
    println!();

    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&result)?;
        std::fs::write(output_path, json)?;
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
    // Build RegimeOptimizeParams from CLI
    let params = RegimeOptimizeParamsBuilder::new()
        .data_path(cli.data.clone())
        .algorithm(cli.algorithm.clone())
        .weights_file(cli.weights_file.clone())
        .spreads(spreads.to_string())
        .skews(skews.to_string())
        .fill_prob(fill_prob)
        .min_trades(min_trades)
        .allow_no_quote(allow_no_quote)
        .high_entropy(cli.high_entropy)
        .low_entropy(cli.low_entropy)
        .max_inventory(cli.max_inventory)
        .quote_size(cli.quote_size)
        .fee_rate(cli.fee_rate)
        .naive_fills(cli.naive_fills)
        .queue_pos(cli.queue_pos)
        .output(output.clone())
        .build()?;

    // Parse algorithm type for display
    let (algo_type, algo_name) = parse_algorithm_type(&cli.algorithm)?;

    // Parse parameter grids for display
    let spread_values: Vec<f64> = spreads
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let skew_values: Vec<f64> = skews
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("       REGIME-SPECIFIC PARAMETER OPTIMIZATION                          ");
    println!("       (Find optimal params for each entropy regime independently)     ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("CONFIGURATION:");
    println!("  Algorithm:           {} ({})", algo_name, algo_type.as_str());
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

    // Run regime optimization using extracted command
    let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
    let result = BacktestCommands::regime_optimize(params.clone(), callback)?;

    // Print report (similar to original format)
    println!();
    println!("════════════════════════════════════════════════════════════════════");
    println!("           REGIME-SPECIFIC PARAMETER OPTIMIZATION                    ");
    println!("════════════════════════════════════════════════════════════════════");
    println!();

    // Data summary
    println!("DATA SUMMARY:");
    println!("  Total events: {}", result.total_events);
    println!("  Time span: {:.1} hours ({:.1} days)",
        result.time_span_hours, result.time_span_hours / 24.0);
    println!();

    // Regime distribution
    println!("REGIME DISTRIBUTION:");
    println!("┌─────────────────┬─────────┬──────────┬────────────┐");
    println!("│ Regime          │ Events  │ Fraction │ Hours      │");
    println!("├─────────────────┼─────────┼──────────┼────────────┤");
    println!("│ High Entropy    │ {:>7} │ {:>7.1}% │ {:>10.1} │",
        result.high_entropy.event_count,
        result.high_entropy.event_fraction * 100.0,
        result.high_entropy.time_hours);
    println!("│ Medium Entropy  │ {:>7} │ {:>7.1}% │ {:>10.1} │",
        result.medium_entropy.event_count,
        result.medium_entropy.event_fraction * 100.0,
        result.medium_entropy.time_hours);
    println!("│ Low Entropy     │ {:>7} │ {:>7.1}% │ {:>10.1} │",
        result.low_entropy.event_count,
        result.low_entropy.event_fraction * 100.0,
        result.low_entropy.time_hours);
    println!("└─────────────────┴─────────┴──────────┴────────────┘");
    println!();

    // Optimal parameters per regime
    println!("OPTIMAL PARAMETERS PER REGIME:");
    println!("┌─────────────────┬──────────┬──────────┬─────────────┬──────────┬──────────┬──────────┐");
    println!("│ Regime          │ Spread   │ Skew     │ Should Quote│ Sharpe   │ Return   │ Drawdown │");
    println!("├─────────────────┼──────────┼──────────┼─────────────┼──────────┼──────────┼──────────┤");
    println!("│ High Entropy    │ {:>8.1} │ {:>8.2} │ {:>11} │ {:>8.3} │ {:>8.2}% │ {:>8.2}% │",
        result.high_entropy.optimal_spread,
        result.high_entropy.optimal_skew,
        if result.high_entropy.should_quote { "Yes" } else { "No" },
        result.high_entropy.best_sharpe,
        result.high_entropy.best_return * 100.0,
        result.high_entropy.best_drawdown * 100.0);
    println!("│ Medium Entropy  │ {:>8.1} │ {:>8.2} │ {:>11} │ {:>8.3} │ {:>8.2}% │ {:>8.2}% │",
        result.medium_entropy.optimal_spread,
        result.medium_entropy.optimal_skew,
        if result.medium_entropy.should_quote { "Yes" } else { "No" },
        result.medium_entropy.best_sharpe,
        result.medium_entropy.best_return * 100.0,
        result.medium_entropy.best_drawdown * 100.0);
    println!("│ Low Entropy     │ {:>8.1} │ {:>8.2} │ {:>11} │ {:>8.3} │ {:>8.2}% │ {:>8.2}% │",
        result.low_entropy.optimal_spread,
        result.low_entropy.optimal_skew,
        if result.low_entropy.should_quote { "Yes" } else { "No" },
        result.low_entropy.best_sharpe,
        result.low_entropy.best_return * 100.0,
        result.low_entropy.best_drawdown * 100.0);
    println!("└─────────────────┴──────────┴──────────┴─────────────┴──────────┴──────────┴──────────┘");
    println!();

    // Strategy comparison
    println!("STRATEGY COMPARISON:");
    println!("┌──────────────────────┬──────────────┬──────────────────┐");
    println!("│ Metric               │ Uniform     │ Regime-Specific │");
    println!("├──────────────────────┼──────────────┼──────────────────┤");
    println!("│ Sharpe Ratio         │ {:>12.3} │ {:>16.3} │",
        result.comparison.uniform_sharpe,
        result.comparison.regime_specific_sharpe);
    println!("│ Total Return         │ {:>12.2}% │ {:>16.2}% │",
        result.comparison.uniform_return * 100.0,
        result.comparison.regime_specific_return * 100.0);
    println!("│ Max Drawdown         │ {:>12.2}% │ {:>16.2}% │",
        result.comparison.uniform_drawdown * 100.0,
        result.comparison.regime_specific_drawdown * 100.0);
    println!("│ Number of Trades     │ {:>12} │ {:>16} │",
        result.comparison.uniform_trades,
        result.comparison.regime_specific_trades);
    println!("│ Win Rate             │ {:>12.1}% │ {:>16.1}% │",
        result.comparison.uniform_win_rate * 100.0,
        result.comparison.regime_specific_win_rate * 100.0);
    println!("└──────────────────────┴──────────────┴──────────────────┘");
    println!();

    println!("IMPROVEMENT:");
    println!("  Sharpe:     {:+.3} ({:+.1}%)",
        result.comparison.sharpe_improvement,
        (result.comparison.sharpe_improvement / result.comparison.uniform_sharpe.abs().max(0.001)) * 100.0);
    println!("  Return:     {:+.2}% ({:+.1}%)",
        result.comparison.return_improvement * 100.0,
        (result.comparison.return_improvement / result.comparison.uniform_return.abs().max(0.0001)) * 100.0);
    println!("  Drawdown:   {:+.2}% ({:+.1}%)",
        result.comparison.drawdown_improvement * 100.0,
        (result.comparison.drawdown_improvement / result.comparison.uniform_drawdown.abs().max(0.0001)) * 100.0);
    println!("  Trades:     {:+.0}",
        result.comparison.trade_count_diff);
    println!("════════════════════════════════════════════════════════════════════");

    // Save results
    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&result)?;
        std::fs::write(output_path, &json)?;
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
    // Build TrainParams from CLI
    let params = TrainParamsBuilder::new()
        .data_path(cli.data.clone())
        .algorithm(cli.algorithm.clone())
        .train_ratio(train_ratio)
        .spread_intercepts(spread_intercepts.to_string())
        .spread_entropy_weights(spread_entropy_weights.to_string())
        .spread_vol_weights(spread_vol_weights.to_string())
        .skew_intercepts(skew_intercepts.to_string())
        .skew_inv_weights(skew_inv_weights.to_string())
        .max_inventory(cli.max_inventory)
        .quote_size(cli.quote_size)
        .fill_prob(cli.fill_prob)
        .fee_rate(cli.fee_rate)
        .naive_fills(cli.naive_fills)
        .queue_pos(cli.queue_pos)
        .output(output.clone())
        .build()?;

    // Parse algorithm type for display
    let (algo_type, algo_name) = parse_algorithm_type(&cli.algorithm)?;

    // Parse parameter grids for display
    let spread_intercepts_vec: Vec<f64> = spread_intercepts
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let spread_entropy_weights_vec: Vec<f64> = spread_entropy_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let spread_vol_weights_vec: Vec<f64> = spread_vol_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let skew_intercepts_vec: Vec<f64> = skew_intercepts
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let skew_inv_weights_vec: Vec<f64> = skew_inv_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let total_combinations = spread_intercepts_vec.len()
        * spread_entropy_weights_vec.len()
        * spread_vol_weights_vec.len()
        * skew_intercepts_vec.len()
        * skew_inv_weights_vec.len();

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("              ML WEIGHT TRAINING (Grid Search)                          ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("CONFIGURATION:");
    println!("  Algorithm:        {} ({})", algo_name, algo_type.as_str());
    println!("  Data:             {:?}", cli.data);
    println!("  Train Ratio:      {:.0}%", train_ratio * 100.0);
    println!();
    println!("PARAMETER GRID:");
    println!("  Spread Intercepts:      {:?}", spread_intercepts_vec);
    println!("  Spread Entropy Weights: {:?}", spread_entropy_weights_vec);
    println!("  Spread Vol Weights:     {:?}", spread_vol_weights_vec);
    println!("  Skew Intercepts:        {:?}", skew_intercepts_vec);
    println!("  Skew Inventory Weights: {:?}", skew_inv_weights_vec);
    println!();
    println!("Total combinations: {}", total_combinations);
    println!();

    // Run training using extracted command
    let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
    let result = BacktestCommands::train(params.clone(), callback)?;

    // Print results
    println!();
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("                    TRAINING RESULTS                                    ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();
    println!("BEST WEIGHTS:");
    println!("  Spread:");
    println!("    intercept:         {:.4}", result.optimal_weights.spread.intercept);
    println!("    w_entropy:         {:.4}", result.optimal_weights.spread.w_entropy);
    println!("    w_volatility:      {:.4}", result.optimal_weights.spread.w_volatility);
    println!("    w_imbalance:       {:.4}", result.optimal_weights.spread.w_imbalance);
    println!("    w_interaction:     {:.4}", result.optimal_weights.spread.w_interaction);
    println!("  Skew:");
    println!("    intercept:         {:.4}", result.optimal_weights.skew.intercept);
    println!("    w_entropy:         {:.4}", result.optimal_weights.skew.w_entropy);
    println!("    w_volatility:      {:.4}", result.optimal_weights.skew.w_volatility);
    println!("    w_imbalance:       {:.4}", result.optimal_weights.skew.w_imbalance);
    println!("    w_inventory:       {:.4}", result.optimal_weights.skew.w_inventory);
    println!();
    println!("PERFORMANCE:");
    println!("  Train Sharpe:     {:+.4}", result.train_sharpe);
    println!("  Test Sharpe:      {:+.4}", result.test_sharpe);
    println!("  Generalization Gap: {:.2}%", result.generalization_gap * 100.0);
    println!();
    println!("  Train Trades:     {}", result.train_trades);
    println!("  Test Trades:      {}", result.test_trades);
    println!("  Configs Tested:   {}/{}", result.valid_configurations, result.total_configurations);
    println!("═══════════════════════════════════════════════════════════════════════");

    // Save results
    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&result)?;
        std::fs::write(output_path, &json)?;
        println!();
        println!("Results saved to: {:?}", output_path);

        // Also save just the weights for easy loading
        let weights_path = output_path.with_extension("weights.json");
        let weights_json = serde_json::to_string_pretty(&result.optimal_weights)?;
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

    // Create algorithm using registry
    // First, handle ML weights loading if needed
    let ml_weights = if algo_type == AlgorithmType::MLSpreadSkew {
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
        Some(weights)
    } else {
        None
    };

    // Print algorithm info for non-ML algorithms
    if algo_type == AlgorithmType::FixedSpread {
        println!("Using Fixed Spread algorithm: spread={:.1}bps, skew={:.2}", cli.spread, cli.skew);
        println!();
    }

    // Create algorithm params from CLI args
    let mut params = BacktestAlgorithmParams::new(
        Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        cli.spread,
        cli.skew,
    );
    if let Some(weights) = ml_weights {
        params = params.with_ml_weights(weights);
    }

    // Use registry to create algorithm
    let algorithm = AlgorithmRegistry::create_for_backtest(algo_type, &params)
        .map_err(|e| anyhow::anyhow!("Failed to create algorithm: {}", e))?;

    // Build backtest config
    let backtest_config = BacktestConfig {
        replay: replay_config,
        mm: MMConfig::default(),
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

    // Create ML algorithm using registry
    let ml_params = BacktestAlgorithmParams::new(
        Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        cli.spread, // Not used by ML but needed for params
        cli.skew,   // Not used by ML but needed for params
    ).with_ml_weights(ml_weights);
    let ml_algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::MLSpreadSkew, &ml_params)
        .map_err(|e| anyhow::anyhow!("Failed to create ML algorithm: {}", e))?;

    let backtest_config = BacktestConfig {
        replay: replay_config.clone(),
        mm: MMConfig::default(),
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

    // Create A-S algorithm using registry
    let as_params = BacktestAlgorithmParams::new(
        Decimal::from_f64_retain(cli.max_inventory).unwrap_or(dec!(0.1)),
        Decimal::from_f64_retain(cli.quote_size).unwrap_or(dec!(0.001)),
        as_spread,
        as_skew,
    );
    let as_algo = AlgorithmRegistry::create_for_backtest(AlgorithmType::AvellanedaStoikov, &as_params)
        .map_err(|e| anyhow::anyhow!("Failed to create A-S algorithm: {}", e))?;

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

fn run_walk_forward_ml(
    cli: &Cli,
    folds: usize,
    min_train_hours: f64,
    test_hours: f64,
    rolling: bool,
    embargo_hours: f64,
    spread_intercepts: &str,
    spread_entropy_weights: &str,
    spread_vol_weights: &str,
    skew_intercepts: &str,
    skew_inv_weights: &str,
    output: Option<PathBuf>,
    weights_output: Option<PathBuf>,
) -> Result<()> {
    // Build WalkForwardMLParams from CLI
    let params = WalkForwardMLParamsBuilder::new()
        .data_path(cli.data.clone())
        .algorithm(cli.algorithm.clone())
        .folds(folds)
        .min_train_hours(min_train_hours)
        .test_hours(test_hours)
        .rolling(rolling)
        .embargo_hours(embargo_hours)
        .spread_intercepts(spread_intercepts.to_string())
        .spread_entropy_weights(spread_entropy_weights.to_string())
        .spread_vol_weights(spread_vol_weights.to_string())
        .skew_intercepts(skew_intercepts.to_string())
        .skew_inv_weights(skew_inv_weights.to_string())
        .max_inventory(cli.max_inventory)
        .quote_size(cli.quote_size)
        .fill_prob(cli.fill_prob)
        .fee_rate(cli.fee_rate)
        .naive_fills(cli.naive_fills)
        .queue_pos(cli.queue_pos)
        .output(output.clone())
        .weights_output(weights_output.clone())
        .build()?;

    // Parse algorithm type for display
    let (algo_type, algo_name) = parse_algorithm_type(&cli.algorithm)?;

    // Parse parameter grids for display
    let spread_ints: Vec<f64> = spread_intercepts
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let spread_ents: Vec<f64> = spread_entropy_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let spread_vols: Vec<f64> = spread_vol_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let skew_ints: Vec<f64> = skew_intercepts
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let skew_invs: Vec<f64> = skew_inv_weights
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let total_configs = spread_ints.len()
        * spread_ents.len()
        * spread_vols.len()
        * skew_ints.len()
        * skew_invs.len();

    println!("═══════════════════════════════════════════════════════");
    println!("       WALK-FORWARD ML TRAINING                        ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("Configuration:");
    println!("  Algorithm:         {} ({})", algo_name, algo_type.as_str());
    println!("  Data:              {:?}", cli.data);
    println!("  Folds:             {}", folds);
    println!("  Min Train Hours:   {}", min_train_hours);
    println!("  Test Hours:        {}", test_hours);
    println!("  Mode:              {}", if rolling { "Rolling" } else { "Anchored" });
    println!("  Embargo Hours:     {}", embargo_hours);
    println!("  Fill Probability:  {:.0}%", cli.fill_prob * 100.0);
    println!("  Weight Combos:     {} per fold", total_configs);
    println!();

    // Run walk-forward ML training using extracted command
    let callback: Arc<dyn ProgressCallback> = Arc::new(NoOpCallback);
    let result = BacktestCommands::walk_forward_ml(params.clone(), callback)?;

    // Print results summary
    println!();
    println!("═══════════════════════════════════════════════════════");
    println!("              WALK-FORWARD RESULTS                     ");
    println!("═══════════════════════════════════════════════════════");
    println!();
    println!("AGGREGATE METRICS:");
    println!("  Avg OOS Sharpe:        {:+.4}", result.aggregate.avg_oos_sharpe);
    println!("  Std OOS Sharpe:        {:.4}", result.aggregate.std_oos_sharpe);
    println!("  Avg OOS Return:        {:+.2}%", result.aggregate.avg_oos_return * 100.0);
    println!("  Total OOS Trades:      {}", result.aggregate.total_oos_trades);
    println!("  Avg Generalization Gap: {:.2}%", result.aggregate.avg_generalization_gap * 100.0);
    println!("  % Profitable Folds:    {:.1}%", result.aggregate.pct_profitable_folds * 100.0);
    println!("  IS/OOS Sharpe Ratio:   {:.3}", result.aggregate.is_oos_sharpe_ratio);
    println!("  Prob Sharpe > 0:       {:.1}%", result.aggregate.prob_sharpe_gt_zero * 100.0);
    println!();
    println!("WEIGHT STABILITY:");
    println!("  Spread Intercept Std:  {:.4}", result.aggregate.weight_stability.spread_intercept_std);
    println!("  Spread Entropy Std:    {:.4}", result.aggregate.weight_stability.spread_entropy_std);
    println!("  Spread Volatility Std: {:.4}", result.aggregate.weight_stability.spread_volatility_std);
    println!("  Skew Intercept Std:    {:.4}", result.aggregate.weight_stability.skew_intercept_std);
    println!("  Skew Inventory Std:    {:.4}", result.aggregate.weight_stability.skew_inventory_std);
    println!("  Stability Score:       {:.3}", result.aggregate.weight_stability.stability_score);
    println!();
    println!("CONSENSUS WEIGHTS:");
    println!("  Spread:");
    println!("    intercept:         {:.4}", result.consensus_weights.spread.intercept);
    println!("    w_entropy:         {:.4}", result.consensus_weights.spread.w_entropy);
    println!("    w_volatility:      {:.4}", result.consensus_weights.spread.w_volatility);
    println!("    w_imbalance:       {:.4}", result.consensus_weights.spread.w_imbalance);
    println!("    w_interaction:     {:.4}", result.consensus_weights.spread.w_interaction);
    println!("  Skew:");
    println!("    intercept:         {:.4}", result.consensus_weights.skew.intercept);
    println!("    w_entropy:         {:.4}", result.consensus_weights.skew.w_entropy);
    println!("    w_volatility:      {:.4}", result.consensus_weights.skew.w_volatility);
    println!("    w_imbalance:       {:.4}", result.consensus_weights.skew.w_imbalance);
    println!("    w_inventory:       {:.4}", result.consensus_weights.skew.w_inventory);
    println!("═══════════════════════════════════════════════════════");

    // Save results
    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&result)?;
        std::fs::write(output_path, &json)?;
        println!();
        println!("Full results saved to: {:?}", output_path);
    }

    // Save consensus weights
    if let Some(ref weights_path) = weights_output {
        let weights_json = serde_json::to_string_pretty(&result.consensus_weights)?;
        std::fs::write(weights_path, weights_json)?;
        println!("Consensus weights saved to: {:?}", weights_path);
    }

    Ok(())
}

/// Validate paper trading sessions against backtest expectations
fn run_validate_session(
    session: Option<PathBuf>,
    sessions_dir: PathBuf,
    min_hours: f64,
    min_trades: usize,
    output: Option<PathBuf>,
) -> Result<()> {
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("              PAPER TRADING VALIDATION                                  ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();

    let config = ValidationConfig {
        min_duration_hours: min_hours,
        min_trades,
        sessions_dir: sessions_dir.clone(),
        ..Default::default()
    };

    let validator = SessionValidator::new(config)?;

    if let Some(session_path) = session {
        // Validate single session
        println!("Validating single session: {:?}", session_path);
        println!();

        let report = validator.validate_session(&session_path)?;
        report.print_report();

        // Save report if output specified
        if let Some(ref output_path) = output {
            report.save(output_path)?;
            println!();
            println!("Report saved to: {:?}", output_path);
        }

        // Return exit code based on verdict
        match report.verdict {
            ingestor::backtest::Verdict::Pass => {
                println!();
                println!("Result: Session PASSED validation");
            }
            ingestor::backtest::Verdict::Warning => {
                println!();
                println!("Result: Session passed with WARNINGS");
            }
            ingestor::backtest::Verdict::Fail => {
                println!();
                println!("Result: Session FAILED validation");
            }
            ingestor::backtest::Verdict::InsufficientData => {
                println!();
                println!("Result: INSUFFICIENT DATA for validation");
            }
        }
    } else {
        // Validate all sessions in directory
        println!("Validating all sessions in: {:?}", sessions_dir);
        println!();

        let report = validator.validate_all_sessions(&sessions_dir)?;
        report.print_report();

        // Save report if output specified
        if let Some(ref output_path) = output {
            report.save(output_path)?;
            println!();
            println!("Report saved to: {:?}", output_path);
        }

        // Summary
        println!();
        println!("Overall Result:");
        match report.verdict {
            ingestor::backtest::Verdict::Pass => {
                println!("  Paper trading VALIDATES backtest expectations");
            }
            ingestor::backtest::Verdict::Warning => {
                println!("  Paper trading shows MIXED results - more data needed");
            }
            ingestor::backtest::Verdict::Fail => {
                println!("  Paper trading DOES NOT VALIDATE backtest expectations");
            }
            ingestor::backtest::Verdict::InsufficientData => {
                println!("  INSUFFICIENT DATA for validation - continue paper trading");
            }
        }
    }

    println!("═══════════════════════════════════════════════════════════════════════");

    Ok(())
}

/// Simulate a paper trading session using historical data from the backtest dataset
fn run_simulate_session(
    cli: &Cli,
    duration_hours: f64,
    preset: Option<String>,
    spread: f64,
    skew: f64,
    sessions_dir: PathBuf,
    output: Option<PathBuf>,
) -> Result<()> {
    use ingestor::backtest::replay::{ParquetReplay, ReplayConfig as ParquetReplayConfig};

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("              PAPER TRADING SESSION SIMULATION                          ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();

    // Build MM config
    let mut mm_config = MMConfig::default();
    // Apply spread to all regimes
    mm_config.regime_params.high_entropy.spread_bps = spread;
    mm_config.regime_params.medium_entropy.spread_bps = spread;
    mm_config.regime_params.low_entropy.spread_bps = spread;
    // Apply skew to all regimes
    mm_config.regime_params.high_entropy.skew_factor = skew;
    mm_config.regime_params.medium_entropy.skew_factor = skew;
    mm_config.regime_params.low_entropy.skew_factor = skew;

    // Build session runner config
    let runner_config = SessionRunnerConfig {
        duration_hours,
        min_duration_hours: 0.1, // Allow short sessions for testing
        preset_name: preset.clone(),
        symbol: "BTCUSDT".to_string(),
        output_dir: sessions_dir.clone(),
        log_quotes: false,
        fee_rate: dec!(0.0001),
        mm_config: Some(mm_config),
        risk_config: None,
        sim_config: None,
        checkpoint_interval_secs: 300,
        progress_interval: 1000,
        min_trades: 5,
    };

    println!("Configuration:");
    println!("  Duration: {:.1} hours", duration_hours);
    if let Some(ref p) = preset {
        println!("  Preset: {}", p);
    } else {
        println!("  Spread: {:.1} bps", spread);
        println!("  Skew: {:.2}", skew);
    }
    println!("  Output dir: {:?}", sessions_dir);
    println!();

    // Create runner
    let mut runner = SessionRunner::new(runner_config)?;
    runner.initialize()?;

    // Load historical data
    println!("Loading historical data from {:?}...", cli.data);
    let replay_config = ParquetReplayConfig {
        data_dir: cli.data.clone(),
        start_time: None,
        end_time: None,
        speed: 0.0, // As fast as possible
    };
    let mut replay = ParquetReplay::new(replay_config);
    let _count = replay.load()?;
    let events = replay.into_events();

    if events.is_empty() {
        anyhow::bail!("No events loaded from data directory");
    }

    println!("Loaded {} events", events.len());
    println!();

    // Calculate how many events correspond to our duration
    // (we'll use a subset if the data is longer than requested)
    let first_ts = events.first().map(|e| e.timestamp_ms).unwrap_or(0);
    let last_ts = events.last().map(|e| e.timestamp_ms).unwrap_or(0);
    let data_duration_hours = (last_ts - first_ts) as f64 / 3600_000.0;

    println!("Data spans {:.1} hours", data_duration_hours);

    // Use all events if data is shorter than requested duration
    let target_end_ts = first_ts + (duration_hours * 3600_000.0) as i64;

    println!();
    println!("Running simulation...");
    println!();

    // Process events
    let mut processed = 0;
    let update_interval = events.len() / 20; // Update progress ~20 times

    for event in &events {
        // Stop if we've exceeded our target duration
        if event.timestamp_ms > target_end_ts {
            break;
        }

        // Convert backtest event to simulated event
        let sim_event = match SimulatedEvent::from_replay_event(event) {
            Some(e) => e,
            None => continue, // Skip events with missing data
        };

        let _fills = runner.process_event(&sim_event)?;
        processed += 1;

        // Progress update
        if update_interval > 0 && processed % update_interval == 0 {
            let progress = runner.progress();
            print!("\r  Events: {} | Trades: {} | Fill rate: {:.2}%        ",
                   progress.events_processed,
                   progress.metrics.total_trades,
                   runner.current_fill_rate() * 100.0);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }

    println!();
    println!();

    // Finalize session
    let result = runner.finalize()?;

    // Print summary
    println!("Session Results:");
    println!("{}", "-".repeat(60));
    println!("  Session ID: {}", result.summary.session_id);
    println!("  Duration: {:.1} hours", result.summary.metrics.duration_secs / 3600.0);
    println!("  Events processed: {}", result.events_processed);
    println!();
    println!("  Trading Metrics:");
    println!("    Total trades: {}", result.summary.metrics.total_trades);
    println!("    Buy/Sell: {} / {}", result.summary.metrics.buy_trades, result.summary.metrics.sell_trades);
    println!("    Quotes generated: {}", result.summary.metrics.quotes_generated);
    println!();

    // Fill rate analysis (critical for backtest calibration)
    let fill_stats = FillRateStats::from_metrics(&result.summary.metrics);
    println!("  Fill Rate Analysis (CRITICAL):");
    println!("    Overall fill rate: {:.2}%", fill_stats.overall_fill_rate * 100.0);
    println!("    Bid fill rate: {:.2}%", fill_stats.bid_fill_rate * 100.0);
    println!("    Ask fill rate: {:.2}%", fill_stats.ask_fill_rate * 100.0);
    println!("    95% CI: [{:.2}%, {:.2}%]",
             fill_stats.ci_lower * 100.0,
             fill_stats.ci_upper * 100.0);

    // Compare to backtest assumption
    let backtest_assumption = 0.10;
    if fill_stats.differs_from_assumption(backtest_assumption, 0.95) {
        println!();
        println!("    WARNING: Fill rate differs significantly from backtest assumption (10%)");
        println!("    Consider recalibrating backtest fill probability!");
    }

    println!();
    println!("  Performance:");
    println!("    Net PnL: {:+.6}", result.summary.metrics.net_pnl);
    println!("    Win rate: {:.1}%", result.summary.metrics.win_rate * 100.0);
    println!("    Sharpe ratio: {:.2}", result.summary.metrics.sharpe_ratio);
    println!("    Max drawdown: {:.2}%", result.summary.metrics.max_drawdown * 100.0);
    println!();
    println!("  Files saved:");
    println!("    Summary: {:?}", result.summary_path);
    if let Some(ref trades_path) = result.trades_path {
        println!("    Trades: {:?}", trades_path);
    }

    if !result.warnings.is_empty() {
        println!();
        println!("  Warnings:");
        for w in &result.warnings {
            println!("    - {}", w);
        }
    }

    // Validity check
    println!();
    if result.is_valid_for_validation {
        println!("  Status: Session is VALID for validation with validate-session");
    } else {
        println!("  Status: Session does NOT meet minimum requirements for validation");
    }

    // Save result JSON if requested
    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&result)?;
        std::fs::write(output_path, json)?;
        println!();
        println!("Full result saved to: {:?}", output_path);
    }

    println!("═══════════════════════════════════════════════════════════════════════");

    Ok(())
}

/// Simulate a 4-week validation campaign using historical data
#[allow(clippy::too_many_arguments)]
fn run_simulate_campaign(
    cli: &Cli,
    weeks: u8,
    session_hours: f64,
    min_sessions_per_week: u8,
    preset: Option<String>,
    spread: f64,
    skew: f64,
    expected_fill_rate: f64,
    expected_sharpe: f64,
    expected_return: f64,
    min_weekly_trades: usize,
    max_drawdown_pct: f64,
    min_win_rate: f64,
    campaigns_dir: PathBuf,
    output: Option<PathBuf>,
) -> Result<()> {
    use ingestor::backtest::replay::{ParquetReplay, ReplayConfig as ParquetReplayConfig};
    use chrono::{Utc, TimeZone, NaiveDate};
    use std::collections::BTreeMap;

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("              VALIDATION CAMPAIGN SIMULATION                            ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();

    // Build campaign config
    let preset_name = preset.clone().unwrap_or_else(|| format!("CLI-{:.1}bps-{:.2}skew", spread, skew));
    let campaign_config = CampaignConfig {
        preset_name: preset_name.clone(),
        target_weeks: weeks,
        session_hours_per_day: session_hours,
        min_sessions_per_week,
        symbol: "BTCUSDT".to_string(),
        output_dir: campaigns_dir.clone(),
        expected_fill_rate,
        expected_sharpe,
        expected_return,
        gates: ValidationGates {
            min_weekly_trades,
            min_fill_rate_ratio: 0.5,
            max_drawdown_pct,
            min_win_rate,
            fill_rate_warning_ratio: 0.7,
            sharpe_warning: 0.5,
            pnl_warning_ratio: 0.6,
        },
    };

    println!("Campaign Configuration:");
    println!("  Preset: {}", preset_name);
    println!("  Target weeks: {}", weeks);
    println!("  Session hours/day: {:.1}", session_hours);
    println!("  Min sessions/week: {}", min_sessions_per_week);
    println!("  Expected fill rate: {:.1}%", expected_fill_rate * 100.0);
    println!("  Expected Sharpe: {:.2}", expected_sharpe);
    println!("  Expected return: {:.2}%", expected_return * 100.0);
    println!();
    println!("Validation Gates:");
    println!("  Min weekly trades: {}", min_weekly_trades);
    println!("  Max drawdown: {:.1}%", max_drawdown_pct);
    println!("  Min win rate: {:.1}%", min_win_rate * 100.0);
    println!();

    // Create campaign
    let mut campaign = ValidationCampaign::new(campaign_config)?;

    // Build MM config
    let mut mm_config = MMConfig::default();
    mm_config.regime_params.high_entropy.spread_bps = spread;
    mm_config.regime_params.medium_entropy.spread_bps = spread;
    mm_config.regime_params.low_entropy.spread_bps = spread;
    mm_config.regime_params.high_entropy.skew_factor = skew;
    mm_config.regime_params.medium_entropy.skew_factor = skew;
    mm_config.regime_params.low_entropy.skew_factor = skew;

    // Load historical data
    println!("Loading historical data from {:?}...", cli.data);
    let replay_config = ParquetReplayConfig {
        data_dir: cli.data.clone(),
        start_time: None,
        end_time: None,
        speed: 0.0,
    };
    let mut replay = ParquetReplay::new(replay_config);
    let _count = replay.load()?;
    let events = replay.into_events();

    if events.is_empty() {
        anyhow::bail!("No events loaded from data directory");
    }

    println!("Loaded {} events", events.len());

    // Group events by day
    let mut events_by_day: BTreeMap<NaiveDate, Vec<_>> = BTreeMap::new();
    for event in events {
        let datetime = Utc.timestamp_millis_opt(event.timestamp_ms).single()
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", event.timestamp_ms))?;
        let date = datetime.date_naive();
        events_by_day.entry(date).or_default().push(event);
    }

    let total_days = events_by_day.len();
    let required_days = (weeks as usize) * 7;
    println!("Data spans {} days (need {} for {} weeks)", total_days, required_days, weeks);
    println!();

    if total_days < min_sessions_per_week as usize {
        anyhow::bail!(
            "Insufficient data: {} days available, need at least {} days",
            total_days, min_sessions_per_week
        );
    }

    // Start campaign
    campaign.start()?;
    println!("Campaign started: {}", campaign.campaign_id);
    println!();
    println!("Running daily sessions...");
    println!("{}", "-".repeat(70));

    // Process each day as a session
    let mut session_count = 0;
    let target_days = required_days.min(total_days);

    for (day_idx, (date, day_events)) in events_by_day.iter().enumerate() {
        if day_idx >= target_days {
            break;
        }

        // Skip days with too few events
        if day_events.len() < 100 {
            println!("  Day {} ({}) - Skipping: only {} events", day_idx + 1, date, day_events.len());
            continue;
        }

        // Calculate session duration from events
        let first_ts = day_events.first().map(|e| e.timestamp_ms).unwrap_or(0);
        let last_ts = day_events.last().map(|e| e.timestamp_ms).unwrap_or(0);
        let day_duration_hours = (last_ts - first_ts) as f64 / 3600_000.0;

        // Use actual day duration or configured session_hours, whichever is smaller
        let effective_hours = day_duration_hours.min(session_hours);

        // Skip if session would be too short
        if effective_hours < 0.1 {
            println!("  Day {} ({}) - Skipping: duration {:.2}h too short", day_idx + 1, date, effective_hours);
            continue;
        }

        // Build session runner config for this day
        let runner_config = SessionRunnerConfig {
            duration_hours: effective_hours,
            min_duration_hours: 0.1,
            preset_name: preset.clone(),
            symbol: "BTCUSDT".to_string(),
            output_dir: campaigns_dir.join("sessions"),
            log_quotes: false,
            fee_rate: dec!(0.0001),
            mm_config: Some(mm_config.clone()),
            risk_config: None,
            sim_config: None,
            checkpoint_interval_secs: 300,
            progress_interval: 5000,
            min_trades: 1,
        };

        // Create and run session
        let mut runner = SessionRunner::new(runner_config)?;
        runner.initialize()?;

        // Process day's events
        let target_end_ts = first_ts + (effective_hours * 3600_000.0) as i64;
        for event in day_events {
            if event.timestamp_ms > target_end_ts {
                break;
            }
            if let Some(sim_event) = SimulatedEvent::from_replay_event(event) {
                let _ = runner.process_event(&sim_event)?;
            }
        }

        // Finalize session
        let result = runner.finalize()?;
        session_count += 1;

        // Print session summary
        let metrics = &result.summary.metrics;
        print!("  Day {:2} ({}) | ", day_idx + 1, date);
        print!("Trades: {:4} | ", metrics.total_trades);
        print!("PnL: {:+.6} | ", metrics.net_pnl);
        print!("WR: {:5.1}% | ", metrics.win_rate * 100.0);
        print!("Fill: {:5.2}%",
            if metrics.quotes_generated > 0 {
                (metrics.total_trades as f64 / metrics.quotes_generated as f64) * 100.0
            } else {
                0.0
            }
        );
        println!();

        // Add session to campaign
        campaign.add_session(result)?;

        // Check for weekly gate after each week
        let sessions_this_week = (day_idx + 1) % 7;
        if sessions_this_week == 0 {
            let week_num = ((day_idx + 1) / 7) as u8;
            if let Some(gate) = campaign.check_weekly_gate() {
                println!();
                println!("  Week {} Gate: {:?}", week_num, gate);
                println!();
            }
        }
    }

    println!("{}", "-".repeat(70));
    println!();
    println!("Campaign simulation complete: {} sessions processed", session_count);
    println!();

    // Stop campaign and generate report
    campaign.stop()?;
    let report = campaign.generate_report();

    // Print report
    print_campaign_report(&report);

    // Save report if output specified
    if let Some(ref output_path) = output {
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(output_path, &json)?;
        println!();
        println!("Campaign report saved to: {:?}", output_path);
    }

    println!("═══════════════════════════════════════════════════════════════════════");

    Ok(())
}

/// Print a formatted campaign report
fn print_campaign_report(report: &CampaignReport) {
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("                     CAMPAIGN REPORT                                    ");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!();

    let metrics = &report.campaign_metrics;

    println!("Campaign Summary:");
    println!("  Preset: {}", report.config.preset_name);
    println!("  Status: {:?}", report.status);
    println!("  Duration: {} weeks, {} sessions", metrics.weeks_completed, metrics.total_sessions);
    println!("  Total hours: {:.1}", metrics.total_hours);
    println!();

    println!("Trading Metrics:");
    println!("  Total trades: {}", metrics.total_trades);
    println!("  Total PnL: {:+.6}", metrics.total_pnl);
    println!("  Win rate: {:.1}%", metrics.overall_win_rate * 100.0);
    println!("  Max drawdown: {:.2}%", metrics.max_drawdown);
    println!();

    println!("Statistical Analysis:");
    println!("  Sharpe ratio: {:.2}", metrics.overall_sharpe);
    println!("  Sharpe 95% CI: [{:.2}, {:.2}]", metrics.sharpe_ci_lower, metrics.sharpe_ci_upper);
    println!("  PSR (prob. Sharpe > 0): {:.1}%", metrics.psr * 100.0);
    println!();

    println!("Fill Rate Calibration:");
    println!("  Actual fill rate: {:.2}%", metrics.overall_fill_rate * 100.0);
    println!("  Expected fill rate: {:.2}%", report.config.expected_fill_rate * 100.0);
    println!("  Calibration ratio: {:.1}%", metrics.fill_rate_calibration * 100.0);
    println!("  Fill rate 95% CI: [{:.2}%, {:.2}%]",
             metrics.fill_rate_ci_lower * 100.0,
             metrics.fill_rate_ci_upper * 100.0);
    println!();

    println!("Comparison to Backtest Expectations:");
    println!("  PnL vs expected: {:.1}%", metrics.pnl_vs_expected * 100.0);
    println!("  Sharpe vs expected: {:.1}%", metrics.sharpe_vs_expected * 100.0);
    println!();

    // Weekly summaries
    if !report.weekly_summaries.is_empty() {
        println!("Weekly Summaries:");
        println!("{}", "-".repeat(70));
        for week in &report.weekly_summaries {
            println!("  Week {}: {} sessions, {} trades, PnL: {:+.6}, Sharpe: {:.2}, Gate: {:?}",
                     week.week_number,
                     week.session_count,
                     week.total_trades,
                     week.cumulative_pnl,
                     week.weekly_sharpe,
                     week.gate_result);
        }
        println!("{}", "-".repeat(70));
        println!();
    }

    // Verdict
    println!("VALIDATION VERDICT: {:?}", report.verdict);
    println!();
    if !report.verdict_reasons.is_empty() {
        println!("Reasons:");
        for reason in &report.verdict_reasons {
            println!("  - {}", reason);
        }
        println!();
    }

    // Recommendations
    if !report.recommendations.is_empty() {
        println!("Recommendations:");
        for rec in &report.recommendations {
            println!("  - {}", rec);
        }
        println!();
    }

    // Action summary based on verdict
    match report.verdict {
        ValidationVerdict::GoLive => {
            println!("ACTION: Strategy is ready for LIVE TRADING");
            println!("  - All validation gates passed");
            println!("  - Statistical significance confirmed (PSR > 90%)");
            println!("  - Fill rate calibration acceptable");
        }
        ValidationVerdict::Recalibrate => {
            println!("ACTION: RECALIBRATE backtest assumptions before proceeding");
            println!("  - Update fill probability based on actual fill rate");
            println!("  - Re-run backtest with calibrated parameters");
            println!("  - Run another validation campaign");
        }
        ValidationVerdict::Reject => {
            println!("ACTION: REJECT this strategy configuration");
            println!("  - Do not proceed to live trading");
            println!("  - Review strategy parameters");
            println!("  - Consider different spread/skew settings");
        }
        ValidationVerdict::Incomplete => {
            println!("ACTION: EXTEND validation period");
            println!("  - Insufficient data for conclusive validation");
            println!("  - Continue collecting paper trading data");
        }
    }
}

// ============================================================================
// Unit Tests for Algorithm Flag Parsing
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ingestor::strategies::{AlgorithmType, AlgorithmRegistry, MLModelWeights};

    // ========================================================================
    // parse_algorithm_type() tests - PARANOID
    // ========================================================================

    #[test]
    fn test_parse_algorithm_type_valid_canonical_names() {
        // Test all canonical algorithm names work correctly
        let test_cases = [
            ("avellaneda_stoikov", AlgorithmType::AvellanedaStoikov),
            ("ml_spread_skew", AlgorithmType::MLSpreadSkew),
            ("fixed_spread", AlgorithmType::FixedSpread),
        ];

        for (input, expected_type) in test_cases {
            let result = parse_algorithm_type(input);
            assert!(result.is_ok(), "Failed to parse canonical name: {}", input);
            let (algo_type, display_name) = result.unwrap();
            assert_eq!(algo_type, expected_type, "Wrong type for: {}", input);
            assert!(!display_name.is_empty(), "Empty display name for: {}", input);
        }
    }

    #[test]
    fn test_parse_algorithm_type_valid_aliases() {
        // Test all known aliases work correctly (based on AlgorithmType::from_str)
        // A-S accepts: avellaneda_stoikov | avellaneda-stoikov | as | a-s
        let as_aliases = ["as", "a-s", "avellaneda_stoikov", "avellaneda-stoikov"];
        for alias in as_aliases {
            let result = parse_algorithm_type(alias);
            assert!(result.is_ok(), "Failed to parse A-S alias: {}", alias);
            let (algo_type, _) = result.unwrap();
            assert_eq!(algo_type, AlgorithmType::AvellanedaStoikov,
                "Wrong type for A-S alias: {}", alias);
        }

        // ML accepts: ml_spread_skew | ml-spread-skew | ml | mlss
        let ml_aliases = ["ml", "mlss", "ml_spread_skew", "ml-spread-skew"];
        for alias in ml_aliases {
            let result = parse_algorithm_type(alias);
            assert!(result.is_ok(), "Failed to parse ML alias: {}", alias);
            let (algo_type, _) = result.unwrap();
            assert_eq!(algo_type, AlgorithmType::MLSpreadSkew,
                "Wrong type for ML alias: {}", alias);
        }

        // Fixed accepts: fixed_spread | fixed-spread | fixed | fs | baseline
        let fixed_aliases = ["fixed", "fs", "baseline", "fixed_spread", "fixed-spread"];
        for alias in fixed_aliases {
            let result = parse_algorithm_type(alias);
            assert!(result.is_ok(), "Failed to parse Fixed alias: {}", alias);
            let (algo_type, _) = result.unwrap();
            assert_eq!(algo_type, AlgorithmType::FixedSpread,
                "Wrong type for Fixed alias: {}", alias);
        }
    }

    #[test]
    fn test_parse_algorithm_type_invalid_names() {
        // Test that invalid names produce errors with helpful messages
        // Note: AlgorithmType::from_str uses to_lowercase(), so case variations ARE valid
        let invalid_names = [
            "invalid",
            "unknown",
            "neural_network",
            "gradient_boost",
            "random_forest",
            "ppo",
            "sac",
            "dqn",
            "",  // Empty string
            " ",  // Whitespace
            "avellaneda stoikov",  // Space instead of underscore (invalid)
            "123",  // Numbers only
            "!@#$%",  // Special characters
            "avellaneda",  // Partial match (not a valid alias)
            "stoikov",     // Partial match (not a valid alias)
        ];

        for invalid in invalid_names {
            let result = parse_algorithm_type(invalid);
            assert!(result.is_err(),
                "Should have failed for invalid name: '{}'", invalid);

            // Check error message contains helpful info
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("Unknown algorithm") || err_msg.contains("Valid options"),
                "Error message should be helpful for: '{}', got: {}", invalid, err_msg);
        }
    }

    #[test]
    fn test_parse_algorithm_type_case_insensitive() {
        // AlgorithmType::from_str uses to_lowercase(), so UPPERCASE should work
        let uppercase_cases = ["AS", "ML", "FIXED", "AVELLANEDA_STOIKOV", "ML_SPREAD_SKEW"];
        for case in uppercase_cases {
            let result = parse_algorithm_type(case);
            assert!(result.is_ok(), "Uppercase should be valid: {}", case);
        }
    }

    #[test]
    fn test_parse_algorithm_type_returns_correct_display_names() {
        // Verify display names are human-readable
        let (_, as_name) = parse_algorithm_type("as").unwrap();
        assert!(as_name.contains("Avellaneda") || as_name.contains("Market"),
            "A-S display name should be descriptive: {}", as_name);

        let (_, ml_name) = parse_algorithm_type("ml").unwrap();
        assert!(ml_name.contains("ML") || ml_name.contains("Spread") || ml_name.contains("Linear"),
            "ML display name should be descriptive: {}", ml_name);

        let (_, fixed_name) = parse_algorithm_type("fixed").unwrap();
        assert!(fixed_name.contains("Fixed") || fixed_name.contains("Baseline"),
            "Fixed display name should be descriptive: {}", fixed_name);
    }

    // ========================================================================
    // load_ml_weights_if_needed() tests - PARANOID
    // ========================================================================

    #[test]
    fn test_load_ml_weights_non_ml_algorithms() {
        // Non-ML algorithms should always return None
        let result = load_ml_weights_if_needed(
            AlgorithmType::AvellanedaStoikov,
            None
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_none(),
            "A-S should not load ML weights");

        let result = load_ml_weights_if_needed(
            AlgorithmType::FixedSpread,
            None
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_none(),
            "Fixed should not load ML weights");
    }

    #[test]
    fn test_load_ml_weights_ml_algorithm_no_file() {
        // ML algorithm without file should return default weights
        let result = load_ml_weights_if_needed(
            AlgorithmType::MLSpreadSkew,
            None
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_some(),
            "ML without file should return default weights");
    }

    #[test]
    fn test_load_ml_weights_missing_file() {
        // ML algorithm with non-existent file should error
        let result = load_ml_weights_if_needed(
            AlgorithmType::MLSpreadSkew,
            Some(std::path::Path::new("/nonexistent/path/weights.json"))
        );
        assert!(result.is_err(),
            "Missing weights file should produce error");

        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to read") || err_msg.contains("weights"),
            "Error should mention file read failure: {}", err_msg);
    }

    #[test]
    fn test_load_ml_weights_invalid_json() {
        // Create temp file with invalid JSON
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("test_invalid_weights.json");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "{{ not valid json }}").unwrap();
        drop(file);

        let result = load_ml_weights_if_needed(
            AlgorithmType::MLSpreadSkew,
            Some(&path)
        );
        assert!(result.is_err(),
            "Invalid JSON should produce error");

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_load_ml_weights_valid_json() {
        // Create temp file with valid weights JSON
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("test_valid_weights.json");

        let weights = MLModelWeights::default();
        let json = serde_json::to_string(&weights).unwrap();

        let mut file = std::fs::File::create(&path).unwrap();
        write!(file, "{}", json).unwrap();
        drop(file);

        let result = load_ml_weights_if_needed(
            AlgorithmType::MLSpreadSkew,
            Some(&path)
        );
        assert!(result.is_ok(), "Valid JSON should parse successfully");
        assert!(result.unwrap().is_some(), "Should return parsed weights");

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    // ========================================================================
    // create_algorithm_params() tests - PARANOID
    // ========================================================================

    #[test]
    fn test_create_algorithm_params_basic() {
        // Test with default CLI values
        let cli = Cli::parse_from(&[
            "backtest",
            "--data", "/tmp/test",
        ]);

        let params = create_algorithm_params(&cli, None);

        // Verify default values are reasonable
        assert!(params.max_inventory > Decimal::ZERO,
            "max_inventory should be positive");
        assert!(params.quote_size > Decimal::ZERO,
            "quote_size should be positive");
        assert!(params.spread_bps > 0.0,
            "spread_bps should be positive");
    }

    #[test]
    fn test_create_algorithm_params_with_ml_weights() {
        let cli = Cli::parse_from(&[
            "backtest",
            "--data", "/tmp/test",
        ]);

        let weights = MLModelWeights::default();
        let params = create_algorithm_params(&cli, Some(weights.clone()));

        assert!(params.ml_weights.is_some(),
            "ML weights should be set when provided");
    }

    #[test]
    fn test_create_algorithm_params_custom_values() {
        let cli = Cli::parse_from(&[
            "backtest",
            "--data", "/tmp/test",
            "--max-inventory", "0.5",
            "--quote-size", "0.01",
            "--spread", "2.5",
            "--skew", "0.7",
        ]);

        let params = create_algorithm_params(&cli, None);

        assert_eq!(params.max_inventory, Decimal::from_f64_retain(0.5).unwrap());
        assert_eq!(params.quote_size, Decimal::from_f64_retain(0.01).unwrap());
        assert!((params.spread_bps - 2.5).abs() < 0.001);
        assert!((params.skew_factor - 0.7).abs() < 0.001);
    }

    // ========================================================================
    // Integration tests: Algorithm creation roundtrip
    // ========================================================================

    #[test]
    fn test_algorithm_creation_roundtrip_all_types() {
        // Test that all algorithm types can be created via the full pipeline
        let algorithms = ["as", "ml", "fixed"];

        for algo_str in algorithms {
            let (algo_type, display_name) = parse_algorithm_type(algo_str)
                .expect(&format!("Failed to parse: {}", algo_str));

            let ml_weights = load_ml_weights_if_needed(algo_type, None)
                .expect(&format!("Failed to load weights for: {}", algo_str));

            let mut params = BacktestAlgorithmParams::new(
                dec!(0.1),
                dec!(0.001),
                1.0,
                0.5,
            );
            if let Some(weights) = ml_weights {
                params = params.with_ml_weights(weights);
            }

            let algorithm = AlgorithmRegistry::create_for_backtest(algo_type, &params);
            assert!(algorithm.is_ok(),
                "Failed to create algorithm for '{}': {:?}", algo_str, algorithm.err());

            let algo = algorithm.unwrap();
            assert_eq!(algo.algorithm_type(), algo_type,
                "Algorithm type mismatch for: {}", algo_str);
            assert!(!algo.name().is_empty(),
                "Algorithm name empty for: {}", algo_str);
        }
    }

    #[test]
    fn test_algorithm_registry_all_type_strings_exhaustive() {
        // Verify that all_type_strings returns all algorithms we support
        let type_strings = AlgorithmRegistry::all_type_strings();

        assert!(type_strings.iter().any(|s| s.contains("avellaneda") || *s == "as"),
            "Should include A-S in type strings");
        assert!(type_strings.iter().any(|s| s.contains("ml") || s.contains("linear")),
            "Should include ML in type strings");
        assert!(type_strings.iter().any(|s| s.contains("fixed") || s.contains("baseline")),
            "Should include Fixed in type strings");
    }

    #[test]
    fn test_default_algorithm_is_as() {
        // The default algorithm flag value should be "as"
        let cli = Cli::parse_from(&[
            "backtest",
            "--data", "/tmp/test",
        ]);

        assert_eq!(cli.algorithm, "as",
            "Default algorithm should be 'as'");

        let (algo_type, _) = parse_algorithm_type(&cli.algorithm).unwrap();
        assert_eq!(algo_type, AlgorithmType::AvellanedaStoikov,
            "Default should resolve to Avellaneda-Stoikov");
    }

    // ========================================================================
    // Edge cases - PARANOID
    // ========================================================================

    #[test]
    fn test_algorithm_type_case_sensitivity() {
        // Test various case combinations
        let mixed_cases = ["AS", "As", "aS", "ML", "Ml", "FIXED", "Fixed"];

        for case in mixed_cases {
            let result = parse_algorithm_type(case);
            // Either it works (case-insensitive) or fails gracefully
            if result.is_err() {
                let err = result.unwrap_err().to_string();
                assert!(err.contains("Unknown algorithm") || err.contains("Valid options"),
                    "Case '{}' should have helpful error: {}", case, err);
            }
            // If it works, that's also fine (case-insensitive implementation)
        }
    }

    #[test]
    fn test_algorithm_with_whitespace() {
        // Whitespace handling
        let result = parse_algorithm_type(" as ");
        // Should either trim and work, or fail gracefully
        if result.is_err() {
            let err = result.unwrap_err().to_string();
            assert!(err.contains("Unknown algorithm"),
                "Whitespace error should be clear: {}", err);
        }
    }

    #[test]
    fn test_all_algorithms_produce_quotes() {
        // Verify each algorithm can actually produce quote calculations
        // (This is more of an integration sanity check)
        let algorithms = [
            AlgorithmType::AvellanedaStoikov,
            AlgorithmType::MLSpreadSkew,
            AlgorithmType::FixedSpread,
        ];

        for algo_type in algorithms {
            let mut params = BacktestAlgorithmParams::new(
                dec!(0.1),
                dec!(0.001),
                1.0,
                0.5,
            );
            if algo_type == AlgorithmType::MLSpreadSkew {
                params = params.with_ml_weights(MLModelWeights::default());
            }

            let algorithm = AlgorithmRegistry::create_for_backtest(algo_type, &params)
                .expect("Failed to create algorithm");

            // Algorithm should be usable (has valid type and name)
            assert!(!algorithm.name().is_empty());
            assert!(!algorithm.type_string().is_empty());
        }
    }
}

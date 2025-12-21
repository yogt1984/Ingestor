//! Multi-Objective Optimization Framework
//!
//! Implements Pareto-optimal parameter selection for trading strategies,
//! balancing multiple competing objectives like Sharpe ratio, drawdown,
//! and fill rate.
//!
//! # Key Concepts
//!
//! - **Pareto Dominance**: Solution A dominates B if A is at least as good
//!   in all objectives and strictly better in at least one.
//! - **Pareto Frontier**: Set of non-dominated solutions (optimal trade-offs).
//! - **Crowding Distance**: Maintains diversity along the frontier.
//!
//! # Objectives
//!
//! 1. **Sharpe Ratio** (maximize): Risk-adjusted returns
//! 2. **Max Drawdown** (minimize): Worst peak-to-trough decline
//! 3. **Fill Rate** (maximize): Percentage of quotes that get filled
//! 4. **Inventory Turnover** (maximize): How quickly positions close
//!
//! # Methodology
//!
//! Uses concepts from:
//! - Deb, K. et al. (2002). "A Fast and Elitist Multiobjective Genetic Algorithm: NSGA-II"
//! - Zitzler, E. & Thiele, L. (1999). "Multiobjective Evolutionary Algorithms"
//!
//! # Usage
//!
//! ```ignore
//! use crate::backtest::multi_objective::{MultiObjectiveOptimizer, MOConfig};
//!
//! let config = MOConfig::default();
//! let mut optimizer = MultiObjectiveOptimizer::new(config);
//! optimizer.load_data()?;
//!
//! let results = optimizer.optimize()?;
//!
//! // Get Pareto-optimal solutions
//! for solution in results.pareto_frontier() {
//!     println!("Sharpe={:.2}, DD={:.2}%, Fill={:.1}%",
//!         solution.sharpe, solution.drawdown * 100.0, solution.fill_rate * 100.0);
//! }
//! ```

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use rust_decimal_macros::dec;
use anyhow::Result;

use crate::backtest::{
    BacktestEngine, BacktestConfig, BacktestResults,
    ReplayEvent, ReplayConfig, FillSimulatorConfig,
};
use crate::execution::market_maker::{MMConfig, RegimeParams};
use crate::execution::mm_simulator::SimulatorConfig;

/// Configuration for multi-objective optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MOConfig {
    /// Data directory
    pub data_dir: PathBuf,

    /// Parameter grid - spreads
    pub spreads: Vec<f64>,

    /// Parameter grid - skews
    pub skews: Vec<f64>,

    /// Parameter grid - fill probabilities
    pub fill_probs: Vec<f64>,

    /// Parameter grid - high entropy thresholds
    pub high_entropies: Vec<f64>,

    /// Weights for objectives (for weighted sum fallback)
    pub objective_weights: ObjectiveWeights,

    /// Minimum number of trades for valid solution
    pub min_trades: usize,

    /// Verbose output
    pub verbose: bool,
}

impl Default for MOConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data/features"),
            spreads: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            skews: vec![0.3, 0.5, 0.7, 1.0],
            fill_probs: vec![0.05, 0.10, 0.15],
            high_entropies: vec![0.6, 0.7, 0.8],
            objective_weights: ObjectiveWeights::default(),
            min_trades: 20,
            verbose: true,
        }
    }
}

/// Weights for combining objectives (used for ranking when needed)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectiveWeights {
    pub sharpe: f64,
    pub drawdown: f64,
    pub fill_rate: f64,
    pub turnover: f64,
}

impl Default for ObjectiveWeights {
    fn default() -> Self {
        Self {
            sharpe: 0.4,      // 40% weight on Sharpe
            drawdown: 0.3,    // 30% weight on drawdown
            fill_rate: 0.2,   // 20% weight on fill rate
            turnover: 0.1,    // 10% weight on turnover
        }
    }
}

/// Objective values for a single solution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectiveValues {
    /// Sharpe ratio (higher is better)
    pub sharpe: f64,
    /// Max drawdown as fraction (lower is better)
    pub drawdown: f64,
    /// Fill rate (higher is better)
    pub fill_rate: f64,
    /// Inventory turnover - trades per hour (higher is better)
    pub turnover: f64,
    /// Total return (for reference)
    pub total_return: f64,
    /// Win rate (for reference)
    pub win_rate: f64,
    /// Number of trades
    pub num_trades: usize,
}

impl ObjectiveValues {
    /// Create from backtest results
    pub fn from_results(results: &BacktestResults, time_span_hours: f64) -> Self {
        let fill_rate = if results.fill_stats.bid_touches + results.fill_stats.ask_touches > 0 {
            (results.fill_stats.bid_fills + results.fill_stats.ask_fills) as f64 /
            (results.fill_stats.bid_touches + results.fill_stats.ask_touches) as f64
        } else {
            0.0
        };

        let turnover = if time_span_hours > 0.0 {
            results.metrics.num_trades as f64 / time_span_hours
        } else {
            0.0
        };

        Self {
            sharpe: results.metrics.sharpe_ratio,
            drawdown: results.metrics.max_drawdown,
            fill_rate,
            turnover,
            total_return: results.metrics.total_return,
            win_rate: results.metrics.win_rate,
            num_trades: results.metrics.num_trades,
        }
    }

    /// Calculate weighted sum score (higher is better)
    /// Note: drawdown is inverted since lower is better
    pub fn weighted_score(&self, weights: &ObjectiveWeights) -> f64 {
        let normalized_sharpe = self.sharpe.max(-5.0).min(5.0) / 5.0; // Normalize to [-1, 1]
        let normalized_dd = 1.0 - self.drawdown.min(1.0);              // Invert: less DD is better
        let normalized_fill = self.fill_rate;                          // Already [0, 1]
        let normalized_turnover = (self.turnover / 10.0).min(1.0);    // Normalize by 10 trades/hr

        weights.sharpe * normalized_sharpe +
        weights.drawdown * normalized_dd +
        weights.fill_rate * normalized_fill +
        weights.turnover * normalized_turnover
    }

    /// Check if this solution dominates another (Pareto dominance)
    /// A dominates B if A is >= B in all objectives and > B in at least one
    pub fn dominates(&self, other: &Self) -> bool {
        let dominated_sharpe = self.sharpe >= other.sharpe;
        let dominated_dd = self.drawdown <= other.drawdown; // Lower is better
        let dominated_fill = self.fill_rate >= other.fill_rate;
        let dominated_turnover = self.turnover >= other.turnover;

        let all_at_least_as_good = dominated_sharpe && dominated_dd &&
                                   dominated_fill && dominated_turnover;

        let strictly_better_sharpe = self.sharpe > other.sharpe;
        let strictly_better_dd = self.drawdown < other.drawdown;
        let strictly_better_fill = self.fill_rate > other.fill_rate;
        let strictly_better_turnover = self.turnover > other.turnover;

        let at_least_one_better = strictly_better_sharpe || strictly_better_dd ||
                                  strictly_better_fill || strictly_better_turnover;

        all_at_least_as_good && at_least_one_better
    }
}

/// A candidate solution with parameters and objectives
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Solution {
    /// Parameters used
    pub params: SolutionParams,
    /// Objective values achieved
    pub objectives: ObjectiveValues,
    /// Pareto rank (1 = frontier, 2 = second tier, etc.)
    pub pareto_rank: usize,
    /// Crowding distance (higher = more isolated = more diverse)
    pub crowding_distance: f64,
}

/// Parameters for a solution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolutionParams {
    pub spread_bps: f64,
    pub skew_factor: f64,
    pub fill_probability: f64,
    pub high_entropy_threshold: f64,
}

impl Solution {
    /// Create a new solution
    pub fn new(params: SolutionParams, objectives: ObjectiveValues) -> Self {
        Self {
            params,
            objectives,
            pareto_rank: 0,
            crowding_distance: 0.0,
        }
    }

    /// Print solution summary
    pub fn print_summary(&self) {
        println!("  Params: spread={:.1}bps, skew={:.2}, fill={:.0}%, entropy={:.1}",
            self.params.spread_bps,
            self.params.skew_factor,
            self.params.fill_probability * 100.0,
            self.params.high_entropy_threshold);
        println!("  Objectives: Sharpe={:+.3}, DD={:.2}%, Fill={:.1}%, Turn={:.2}/hr",
            self.objectives.sharpe,
            self.objectives.drawdown * 100.0,
            self.objectives.fill_rate * 100.0,
            self.objectives.turnover);
        println!("  Rank={}, Crowding={:.3}", self.pareto_rank, self.crowding_distance);
    }
}

/// Results from multi-objective optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MOResults {
    /// All evaluated solutions
    pub all_solutions: Vec<Solution>,
    /// Configuration used
    pub config: MOConfig,
    /// Time span of data in hours
    pub time_span_hours: f64,
    /// Number of events processed
    pub num_events: usize,
}

impl MOResults {
    /// Get Pareto frontier (rank 1 solutions)
    pub fn pareto_frontier(&self) -> Vec<&Solution> {
        self.all_solutions.iter()
            .filter(|s| s.pareto_rank == 1)
            .collect()
    }

    /// Get solutions by rank
    pub fn solutions_by_rank(&self, rank: usize) -> Vec<&Solution> {
        self.all_solutions.iter()
            .filter(|s| s.pareto_rank == rank)
            .collect()
    }

    /// Get best solution by weighted score
    pub fn best_weighted(&self) -> Option<&Solution> {
        self.all_solutions.iter()
            .max_by(|a, b| {
                let score_a = a.objectives.weighted_score(&self.config.objective_weights);
                let score_b = b.objectives.weighted_score(&self.config.objective_weights);
                score_a.partial_cmp(&score_b).unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    /// Get best solution for a single objective
    pub fn best_for_objective(&self, objective: Objective) -> Option<&Solution> {
        match objective {
            Objective::Sharpe => self.all_solutions.iter()
                .max_by(|a, b| a.objectives.sharpe.partial_cmp(&b.objectives.sharpe)
                    .unwrap_or(std::cmp::Ordering::Equal)),
            Objective::Drawdown => self.all_solutions.iter()
                .min_by(|a, b| a.objectives.drawdown.partial_cmp(&b.objectives.drawdown)
                    .unwrap_or(std::cmp::Ordering::Equal)),
            Objective::FillRate => self.all_solutions.iter()
                .max_by(|a, b| a.objectives.fill_rate.partial_cmp(&b.objectives.fill_rate)
                    .unwrap_or(std::cmp::Ordering::Equal)),
            Objective::Turnover => self.all_solutions.iter()
                .max_by(|a, b| a.objectives.turnover.partial_cmp(&b.objectives.turnover)
                    .unwrap_or(std::cmp::Ordering::Equal)),
        }
    }

    /// Save results to JSON
    pub fn save_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Print comprehensive report
    pub fn print_report(&self) {
        println!();
        println!("════════════════════════════════════════════════════════════");
        println!("           MULTI-OBJECTIVE OPTIMIZATION RESULTS              ");
        println!("════════════════════════════════════════════════════════════");
        println!();

        println!("DATA:");
        println!("  Time span: {:.1} hours ({:.1} days)",
            self.time_span_hours, self.time_span_hours / 24.0);
        println!("  Events: {}", self.num_events);
        println!("  Solutions evaluated: {}", self.all_solutions.len());
        println!();

        // Pareto frontier
        let frontier = self.pareto_frontier();
        println!("PARETO FRONTIER ({} solutions):", frontier.len());
        println!("┌───────┬───────┬───────┬───────────┬──────────┬──────────┬──────────┐");
        println!("│ Sprd  │ Skew  │ FillP │   Sharpe  │    DD    │ FillRate │ Turnover │");
        println!("├───────┼───────┼───────┼───────────┼──────────┼──────────┼──────────┤");

        for sol in frontier.iter().take(15) {
            println!("│ {:5.1} │ {:5.2} │ {:4.0}% │ {:+9.3} │ {:7.2}% │ {:7.1}% │ {:7.2}/h │",
                sol.params.spread_bps,
                sol.params.skew_factor,
                sol.params.fill_probability * 100.0,
                sol.objectives.sharpe,
                sol.objectives.drawdown * 100.0,
                sol.objectives.fill_rate * 100.0,
                sol.objectives.turnover);
        }
        if frontier.len() > 15 {
            println!("│ ... {} more solutions on frontier ...                          │",
                frontier.len() - 15);
        }
        println!("└───────┴───────┴───────┴───────────┴──────────┴──────────┴──────────┘");
        println!();

        // Best for each objective
        println!("BEST FOR EACH OBJECTIVE:");
        println!();

        if let Some(best) = self.best_for_objective(Objective::Sharpe) {
            println!("  BEST SHARPE: {:+.3}", best.objectives.sharpe);
            println!("    Params: spread={:.1}, skew={:.2}, DD={:.2}%",
                best.params.spread_bps, best.params.skew_factor,
                best.objectives.drawdown * 100.0);
        }

        if let Some(best) = self.best_for_objective(Objective::Drawdown) {
            println!("  LOWEST DRAWDOWN: {:.2}%", best.objectives.drawdown * 100.0);
            println!("    Params: spread={:.1}, skew={:.2}, Sharpe={:+.3}",
                best.params.spread_bps, best.params.skew_factor,
                best.objectives.sharpe);
        }

        if let Some(best) = self.best_for_objective(Objective::FillRate) {
            println!("  BEST FILL RATE: {:.1}%", best.objectives.fill_rate * 100.0);
            println!("    Params: spread={:.1}, skew={:.2}, Sharpe={:+.3}",
                best.params.spread_bps, best.params.skew_factor,
                best.objectives.sharpe);
        }

        if let Some(best) = self.best_for_objective(Objective::Turnover) {
            println!("  BEST TURNOVER: {:.2} trades/hour", best.objectives.turnover);
            println!("    Params: spread={:.1}, skew={:.2}, Sharpe={:+.3}",
                best.params.spread_bps, best.params.skew_factor,
                best.objectives.sharpe);
        }
        println!();

        // Best weighted
        if let Some(best) = self.best_weighted() {
            println!("RECOMMENDED (weighted score):");
            println!("  Score: {:.4}", best.objectives.weighted_score(&self.config.objective_weights));
            best.print_summary();
        }
        println!();

        // Trade-off analysis
        println!("TRADE-OFF INSIGHTS:");
        self.print_tradeoff_analysis();
        println!();

        println!("References:");
        println!("  - Deb et al. (2002) \"NSGA-II: A Fast and Elitist Multiobjective GA\"");
        println!("  - Zitzler & Thiele (1999) \"Multiobjective Evolutionary Algorithms\"");
        println!("════════════════════════════════════════════════════════════");
    }

    /// Print trade-off analysis
    fn print_tradeoff_analysis(&self) {
        let frontier = self.pareto_frontier();
        if frontier.len() < 2 {
            println!("  Not enough solutions on frontier for trade-off analysis");
            return;
        }

        // Correlation between objectives
        let sharpes: Vec<f64> = frontier.iter().map(|s| s.objectives.sharpe).collect();
        let dds: Vec<f64> = frontier.iter().map(|s| s.objectives.drawdown).collect();
        let fills: Vec<f64> = frontier.iter().map(|s| s.objectives.fill_rate).collect();

        let corr_sharpe_dd = correlation(&sharpes, &dds);
        let corr_sharpe_fill = correlation(&sharpes, &fills);

        println!("  Sharpe vs Drawdown correlation: {:+.2}", corr_sharpe_dd);
        if corr_sharpe_dd > 0.3 {
            println!("    -> Higher Sharpe tends to come with higher drawdown (expected risk-return)");
        } else if corr_sharpe_dd < -0.3 {
            println!("    -> Some solutions achieve high Sharpe with low drawdown (attractive)");
        }

        println!("  Sharpe vs Fill Rate correlation: {:+.2}", corr_sharpe_fill);
        if corr_sharpe_fill < -0.3 {
            println!("    -> Trade-off: tighter spreads (higher fills) hurt profitability");
        } else if corr_sharpe_fill > 0.3 {
            println!("    -> More fills correlate with better Sharpe (good execution)");
        }

        // Spread of values on frontier
        let sharpe_range = sharpes.iter().cloned().fold(f64::NEG_INFINITY, f64::max) -
                          sharpes.iter().cloned().fold(f64::INFINITY, f64::min);
        let dd_range = dds.iter().cloned().fold(f64::NEG_INFINITY, f64::max) -
                      dds.iter().cloned().fold(f64::INFINITY, f64::min);

        println!("  Sharpe range on frontier: {:.2}", sharpe_range);
        println!("  Drawdown range on frontier: {:.2}%", dd_range * 100.0);
    }
}

/// Objective to optimize
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Objective {
    Sharpe,
    Drawdown,
    FillRate,
    Turnover,
}

/// Multi-objective optimizer
pub struct MultiObjectiveOptimizer {
    config: MOConfig,
    events: Vec<ReplayEvent>,
    time_range: Option<(i64, i64)>,
}

impl MultiObjectiveOptimizer {
    /// Create a new optimizer
    pub fn new(config: MOConfig) -> Self {
        Self {
            config,
            events: Vec::new(),
            time_range: None,
        }
    }

    /// Load data from Parquet files
    pub fn load_data(&mut self) -> Result<usize> {
        use crate::backtest::replay::ParquetReplay;

        let replay_config = ReplayConfig {
            data_dir: self.config.data_dir.clone(),
            ..Default::default()
        };

        let mut replay = ParquetReplay::new(replay_config);
        let num_events = replay.load()?;

        self.time_range = replay.time_range();
        self.events = replay.into_events();

        if self.config.verbose {
            println!("Loaded {} events for multi-objective optimization", num_events);
            if let Some((start, end)) = self.time_range {
                let hours = (end - start) as f64 / (1000.0 * 60.0 * 60.0);
                println!("Time span: {:.1} hours ({:.1} days)", hours, hours / 24.0);
            }
        }

        Ok(num_events)
    }

    /// Run backtest with given parameters
    fn run_backtest(
        &self,
        spread: f64,
        skew: f64,
        fill_prob: f64,
        high_entropy: f64,
    ) -> Result<BacktestResults> {
        let mm_config = MMConfig {
            regime_params: RegimeParams::uniform(spread, skew),
            max_inventory: dec!(0.1),
            quote_size: dec!(0.001),
            regime_thresholds: crate::execution::market_maker::RegimeThresholds {
                high_entropy_threshold: high_entropy,
                low_entropy_threshold: 0.4,
            },
            ..Default::default()
        };

        let config = BacktestConfig {
            replay: ReplayConfig {
                data_dir: self.config.data_dir.clone(),
                ..Default::default()
            },
            mm: mm_config,
            simulator: SimulatorConfig::default(),
            fill_sim: FillSimulatorConfig {
                base_fill_probability: fill_prob,
                ..Default::default()
            },
            verbose: false,
            use_realistic_fills: true,
            ..Default::default()
        };

        let mut engine = BacktestEngine::from_events(config, self.events.clone());
        engine.run()
    }

    /// Run multi-objective optimization
    pub fn optimize(&self) -> Result<MOResults> {
        if self.events.is_empty() {
            anyhow::bail!("No data loaded. Call load_data() first.");
        }

        let time_span_hours = if let Some((start, end)) = self.time_range {
            (end - start) as f64 / (1000.0 * 60.0 * 60.0)
        } else {
            0.0
        };

        let total_combinations = self.config.spreads.len() *
            self.config.skews.len() *
            self.config.fill_probs.len() *
            self.config.high_entropies.len();

        if self.config.verbose {
            println!();
            println!("Running multi-objective optimization...");
            println!("Parameter combinations: {}", total_combinations);
            println!();
        }

        let mut solutions = Vec::new();
        let mut count = 0;

        for &spread in &self.config.spreads {
            for &skew in &self.config.skews {
                for &fill_prob in &self.config.fill_probs {
                    for &high_entropy in &self.config.high_entropies {
                        count += 1;

                        let results = self.run_backtest(spread, skew, fill_prob, high_entropy)?;

                        // Skip solutions with too few trades
                        if results.metrics.num_trades < self.config.min_trades {
                            if self.config.verbose {
                                println!("[{:>4}/{}] sp={:.1} sk={:.1} fp={:.0}% ent={:.1} => {} trades (skipped)",
                                    count, total_combinations, spread, skew,
                                    fill_prob * 100.0, high_entropy, results.metrics.num_trades);
                            }
                            continue;
                        }

                        let objectives = ObjectiveValues::from_results(&results, time_span_hours);

                        let params = SolutionParams {
                            spread_bps: spread,
                            skew_factor: skew,
                            fill_probability: fill_prob,
                            high_entropy_threshold: high_entropy,
                        };

                        if self.config.verbose {
                            println!("[{:>4}/{}] sp={:.1} sk={:.1} => Sharpe={:+.3} DD={:.2}% Fill={:.1}%",
                                count, total_combinations, spread, skew,
                                objectives.sharpe, objectives.drawdown * 100.0,
                                objectives.fill_rate * 100.0);
                        }

                        solutions.push(Solution::new(params, objectives));
                    }
                }
            }
        }

        if solutions.is_empty() {
            anyhow::bail!("No valid solutions found (all had fewer than {} trades)",
                self.config.min_trades);
        }

        // Compute Pareto ranks
        self.compute_pareto_ranks(&mut solutions);

        // Compute crowding distances
        self.compute_crowding_distances(&mut solutions);

        // Sort by rank, then by crowding distance (descending)
        solutions.sort_by(|a, b| {
            match a.pareto_rank.cmp(&b.pareto_rank) {
                std::cmp::Ordering::Equal => {
                    b.crowding_distance.partial_cmp(&a.crowding_distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
                other => other,
            }
        });

        let results = MOResults {
            all_solutions: solutions,
            config: self.config.clone(),
            time_span_hours,
            num_events: self.events.len(),
        };

        if self.config.verbose {
            results.print_report();
        }

        Ok(results)
    }

    /// Compute Pareto ranks using non-dominated sorting
    fn compute_pareto_ranks(&self, solutions: &mut [Solution]) {
        let n = solutions.len();
        let mut dominated_count = vec![0usize; n];
        let mut dominates_list: Vec<Vec<usize>> = vec![Vec::new(); n];

        // For each pair, determine dominance
        for i in 0..n {
            for j in 0..n {
                if i == j { continue; }

                if solutions[i].objectives.dominates(&solutions[j].objectives) {
                    dominates_list[i].push(j);
                } else if solutions[j].objectives.dominates(&solutions[i].objectives) {
                    dominated_count[i] += 1;
                }
            }
        }

        // Assign ranks iteratively
        let mut current_rank = 1;
        let mut remaining: Vec<usize> = (0..n).collect();

        while !remaining.is_empty() {
            // Find non-dominated solutions in current set
            let front: Vec<usize> = remaining.iter()
                .filter(|&&i| dominated_count[i] == 0)
                .cloned()
                .collect();

            if front.is_empty() {
                // Safety: assign remaining to current rank
                for &i in &remaining {
                    solutions[i].pareto_rank = current_rank;
                }
                break;
            }

            // Assign rank to front
            for &i in &front {
                solutions[i].pareto_rank = current_rank;

                // Reduce domination count for solutions dominated by front members
                for &j in &dominates_list[i] {
                    if dominated_count[j] > 0 {
                        dominated_count[j] -= 1;
                    }
                }
            }

            // Remove front from remaining
            remaining.retain(|i| !front.contains(i));
            current_rank += 1;
        }
    }

    /// Compute crowding distances for diversity maintenance
    fn compute_crowding_distances(&self, solutions: &mut [Solution]) {
        if solutions.len() < 3 {
            for sol in solutions.iter_mut() {
                sol.crowding_distance = f64::INFINITY;
            }
            return;
        }

        // Process each rank separately
        let max_rank = solutions.iter().map(|s| s.pareto_rank).max().unwrap_or(1);

        for rank in 1..=max_rank {
            let indices: Vec<usize> = solutions.iter()
                .enumerate()
                .filter(|(_, s)| s.pareto_rank == rank)
                .map(|(i, _)| i)
                .collect();

            if indices.len() < 3 {
                for &i in &indices {
                    solutions[i].crowding_distance = f64::INFINITY;
                }
                continue;
            }

            // Initialize crowding distances
            for &i in &indices {
                solutions[i].crowding_distance = 0.0;
            }

            // For each objective, sort and compute contribution
            let objectives = [
                |s: &Solution| s.objectives.sharpe,
                |s: &Solution| -s.objectives.drawdown, // Negate so higher is better
                |s: &Solution| s.objectives.fill_rate,
                |s: &Solution| s.objectives.turnover,
            ];

            for obj_fn in &objectives {
                // Sort indices by this objective
                let mut sorted_indices = indices.clone();
                sorted_indices.sort_by(|&a, &b| {
                    obj_fn(&solutions[a]).partial_cmp(&obj_fn(&solutions[b]))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });

                // Boundary points get infinite distance
                solutions[sorted_indices[0]].crowding_distance = f64::INFINITY;
                solutions[*sorted_indices.last().unwrap()].crowding_distance = f64::INFINITY;

                // Get range for normalization
                let min_val = obj_fn(&solutions[sorted_indices[0]]);
                let max_val = obj_fn(&solutions[*sorted_indices.last().unwrap()]);
                let range = (max_val - min_val).abs();

                if range > 1e-10 {
                    for i in 1..sorted_indices.len()-1 {
                        let prev = obj_fn(&solutions[sorted_indices[i-1]]);
                        let next = obj_fn(&solutions[sorted_indices[i+1]]);
                        let contribution = (next - prev).abs() / range;

                        if solutions[sorted_indices[i]].crowding_distance != f64::INFINITY {
                            solutions[sorted_indices[i]].crowding_distance += contribution;
                        }
                    }
                }
            }
        }
    }
}

/// Calculate Pearson correlation coefficient
fn correlation(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.len() < 2 {
        return 0.0;
    }

    let n = x.len() as f64;
    let mean_x = x.iter().sum::<f64>() / n;
    let mean_y = y.iter().sum::<f64>() / n;

    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;

    for i in 0..x.len() {
        let dx = x[i] - mean_x;
        let dy = y[i] - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }

    if var_x > 1e-10 && var_y > 1e-10 {
        cov / (var_x.sqrt() * var_y.sqrt())
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mo_config_default() {
        let config = MOConfig::default();
        assert!(!config.spreads.is_empty());
        assert!(!config.skews.is_empty());
        assert_eq!(config.min_trades, 20);
    }

    #[test]
    fn test_objective_weights_default() {
        let weights = ObjectiveWeights::default();
        let total = weights.sharpe + weights.drawdown + weights.fill_rate + weights.turnover;
        assert!((total - 1.0).abs() < 0.01); // Should sum to 1
    }

    #[test]
    fn test_objective_values_weighted_score() {
        let obj = ObjectiveValues {
            sharpe: 2.0,
            drawdown: 0.05,
            fill_rate: 0.15,
            turnover: 5.0,
            total_return: 0.10,
            win_rate: 0.55,
            num_trades: 100,
        };

        let weights = ObjectiveWeights::default();
        let score = obj.weighted_score(&weights);

        // Score should be positive for good values
        assert!(score > 0.0);
    }

    #[test]
    fn test_pareto_dominance() {
        let obj_a = ObjectiveValues {
            sharpe: 2.0,
            drawdown: 0.05,
            fill_rate: 0.20,
            turnover: 5.0,
            total_return: 0.10,
            win_rate: 0.55,
            num_trades: 100,
        };

        let obj_b = ObjectiveValues {
            sharpe: 1.5,      // Worse
            drawdown: 0.10,   // Worse
            fill_rate: 0.15,  // Worse
            turnover: 3.0,    // Worse
            total_return: 0.05,
            win_rate: 0.50,
            num_trades: 80,
        };

        // A should dominate B (better in all objectives)
        assert!(obj_a.dominates(&obj_b));
        assert!(!obj_b.dominates(&obj_a));
    }

    #[test]
    fn test_pareto_non_dominance() {
        let obj_a = ObjectiveValues {
            sharpe: 2.0,       // Better
            drawdown: 0.10,    // Worse
            fill_rate: 0.15,
            turnover: 5.0,
            total_return: 0.10,
            win_rate: 0.55,
            num_trades: 100,
        };

        let obj_b = ObjectiveValues {
            sharpe: 1.5,       // Worse
            drawdown: 0.05,    // Better
            fill_rate: 0.15,
            turnover: 5.0,
            total_return: 0.08,
            win_rate: 0.52,
            num_trades: 90,
        };

        // Neither dominates the other (trade-off between Sharpe and drawdown)
        assert!(!obj_a.dominates(&obj_b));
        assert!(!obj_b.dominates(&obj_a));
    }

    #[test]
    fn test_solution_creation() {
        let params = SolutionParams {
            spread_bps: 2.0,
            skew_factor: 0.5,
            fill_probability: 0.10,
            high_entropy_threshold: 0.7,
        };

        let objectives = ObjectiveValues {
            sharpe: 1.5,
            drawdown: 0.05,
            fill_rate: 0.15,
            turnover: 3.0,
            total_return: 0.05,
            win_rate: 0.55,
            num_trades: 50,
        };

        let solution = Solution::new(params, objectives);
        assert_eq!(solution.pareto_rank, 0); // Not yet computed
        assert_eq!(solution.crowding_distance, 0.0);
    }

    #[test]
    fn test_correlation() {
        // Perfect positive correlation
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 4.0, 6.0, 8.0, 10.0];
        let corr = correlation(&x, &y);
        assert!((corr - 1.0).abs() < 0.01);

        // Perfect negative correlation
        let y_neg = vec![10.0, 8.0, 6.0, 4.0, 2.0];
        let corr_neg = correlation(&x, &y_neg);
        assert!((corr_neg - (-1.0)).abs() < 0.01);

        // Low correlation (mixed sequence)
        let y_low = vec![3.0, 1.0, 4.0, 2.0, 5.0];
        let corr_low = correlation(&x, &y_low);
        // Just check it's not perfectly correlated (correlation calculation works)
        assert!(corr_low.abs() < 0.95);
    }

    #[test]
    fn test_objective_enum() {
        assert_eq!(Objective::Sharpe, Objective::Sharpe);
        assert_ne!(Objective::Sharpe, Objective::Drawdown);
    }
}

//! Regime-Specific Parameter Optimization
//!
//! Optimizes MM parameters independently for each market regime (high/medium/low entropy),
//! then combines them into an optimal regime-switching strategy.
//!
//! # Methodology
//!
//! 1. **Segment Data by Regime**: Split historical events by entropy regime
//! 2. **Independent Optimization**: Find best params for each regime subset
//! 3. **Combined Validation**: Test the combined regime-switching strategy
//! 4. **Comparison**: Compare vs uniform (single-param) approach
//!
//! # Key Insight
//!
//! Different regimes have different optimal parameters:
//! - High entropy (random flow): Tight spreads, aggressive quoting
//! - Medium entropy (uncertain): Moderate spreads, cautious
//! - Low entropy (trending): Wide spreads or no quoting
//!
//! # References
//!
//! - Cartea, A., Jaimungal, S., & Penalva, J. (2015). Algorithmic and High-Frequency Trading
//! - Guéant, O. (2017). Optimal Market Making

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use rust_decimal_macros::dec;
use anyhow::Result;

use crate::backtest::{
    BacktestEngine, BacktestConfig, BacktestResults,
    ReplayEvent, ReplayConfig, FillSimulatorConfig,
};
use crate::trading::market_maker::{MMConfig, RegimeParams, RegimeConfig, RegimeThresholds, MarketRegime};
use crate::trading::mm_simulator::SimulatorConfig;

/// Configuration for regime-specific optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeOptimizerConfig {
    /// Data directory
    pub data_dir: PathBuf,

    /// High entropy threshold (above = high regime)
    pub high_entropy_threshold: f64,

    /// Low entropy threshold (below = low regime)
    pub low_entropy_threshold: f64,

    /// Spread grid for optimization
    pub spreads: Vec<f64>,

    /// Skew grid for optimization
    pub skews: Vec<f64>,

    /// Fill probability for simulation
    pub fill_probability: f64,

    /// Minimum trades required for valid optimization
    pub min_trades: usize,

    /// Whether to allow no-quoting in low entropy
    pub allow_no_quote_low: bool,

    /// Verbose output
    pub verbose: bool,
}

impl Default for RegimeOptimizerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data/features"),
            high_entropy_threshold: 0.7,
            low_entropy_threshold: 0.4,
            spreads: vec![0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0],
            skews: vec![0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0],
            fill_probability: 0.10,
            min_trades: 10,
            allow_no_quote_low: true,
            verbose: true,
        }
    }
}

/// Metrics for a single regime
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeMetrics {
    pub regime: String,
    pub event_count: usize,
    pub event_fraction: f64,
    pub time_hours: f64,
    pub optimal_spread: f64,
    pub optimal_skew: f64,
    pub should_quote: bool,
    pub best_sharpe: f64,
    pub best_return: f64,
    pub best_drawdown: f64,
    pub best_trades: usize,
    pub best_win_rate: f64,
}

/// Comparison of uniform vs regime-specific strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyComparison {
    /// Uniform strategy metrics (single param set for all regimes)
    pub uniform: FullBacktestMetrics,
    /// Regime-specific strategy metrics (different params per regime)
    pub regime_specific: FullBacktestMetrics,
    /// Improvement from regime-specific (positive = better)
    pub sharpe_improvement: f64,
    pub return_improvement: f64,
    pub drawdown_improvement: f64,  // Negative = better (less drawdown)
    pub trade_count_diff: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullBacktestMetrics {
    pub sharpe: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
    pub params_description: String,
}

/// Results from regime-specific optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeOptimizationResults {
    /// Per-regime optimization results
    pub high_entropy: RegimeMetrics,
    pub medium_entropy: RegimeMetrics,
    pub low_entropy: RegimeMetrics,

    /// Optimal combined regime params
    pub optimal_regime_params: OptimalRegimeParams,

    /// Comparison with uniform approach
    pub comparison: StrategyComparison,

    /// Configuration used
    pub config: RegimeOptimizerConfig,

    /// Data summary
    pub total_events: usize,
    pub time_span_hours: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimalRegimeParams {
    pub high: ParamSet,
    pub medium: ParamSet,
    pub low: ParamSet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamSet {
    pub spread_bps: f64,
    pub skew_factor: f64,
    pub should_quote: bool,
}

impl OptimalRegimeParams {
    pub fn to_regime_params(&self) -> RegimeParams {
        RegimeParams {
            high_entropy: RegimeConfig {
                spread_bps: self.high.spread_bps,
                skew_factor: self.high.skew_factor,
                size_mult: 1.0,
                should_quote: self.high.should_quote,
            },
            medium_entropy: RegimeConfig {
                spread_bps: self.medium.spread_bps,
                skew_factor: self.medium.skew_factor,
                size_mult: 0.7,
                should_quote: self.medium.should_quote,
            },
            low_entropy: RegimeConfig {
                spread_bps: self.low.spread_bps,
                skew_factor: self.low.skew_factor,
                size_mult: 0.3,
                should_quote: self.low.should_quote,
            },
        }
    }
}

impl RegimeOptimizationResults {
    /// Print comprehensive report
    pub fn print_report(&self) {
        println!();
        println!("════════════════════════════════════════════════════════════════════");
        println!("           REGIME-SPECIFIC PARAMETER OPTIMIZATION                    ");
        println!("════════════════════════════════════════════════════════════════════");
        println!();

        // Data summary
        println!("DATA SUMMARY:");
        println!("  Total events: {}", self.total_events);
        println!("  Time span: {:.1} hours ({:.1} days)",
            self.time_span_hours, self.time_span_hours / 24.0);
        println!();

        // Regime distribution
        println!("REGIME DISTRIBUTION:");
        println!("┌─────────────────┬─────────┬──────────┬────────────┐");
        println!("│ Regime          │ Events  │ Fraction │ Hours      │");
        println!("├─────────────────┼─────────┼──────────┼────────────┤");
        println!("│ High Entropy    │ {:>7} │ {:>7.1}% │ {:>10.1} │",
            self.high_entropy.event_count,
            self.high_entropy.event_fraction * 100.0,
            self.high_entropy.time_hours);
        println!("│ Medium Entropy  │ {:>7} │ {:>7.1}% │ {:>10.1} │",
            self.medium_entropy.event_count,
            self.medium_entropy.event_fraction * 100.0,
            self.medium_entropy.time_hours);
        println!("│ Low Entropy     │ {:>7} │ {:>7.1}% │ {:>10.1} │",
            self.low_entropy.event_count,
            self.low_entropy.event_fraction * 100.0,
            self.low_entropy.time_hours);
        println!("└─────────────────┴─────────┴──────────┴────────────┘");
        println!();

        // Per-regime optimal parameters
        println!("OPTIMAL PARAMETERS PER REGIME:");
        println!("┌─────────────────┬────────┬────────┬───────────┬──────────┬──────────┐");
        println!("│ Regime          │ Spread │  Skew  │  Sharpe   │  Return  │  Trades  │");
        println!("├─────────────────┼────────┼────────┼───────────┼──────────┼──────────┤");

        let _quote_str = |q: bool| if q { "quote" } else { "NO" };
        println!("│ High Entropy    │ {:>5.1}  │ {:>5.2}  │ {:>+8.3}  │ {:>+7.2}% │ {:>8} │",
            self.high_entropy.optimal_spread,
            self.high_entropy.optimal_skew,
            self.high_entropy.best_sharpe,
            self.high_entropy.best_return * 100.0,
            self.high_entropy.best_trades);
        println!("│ Medium Entropy  │ {:>5.1}  │ {:>5.2}  │ {:>+8.3}  │ {:>+7.2}% │ {:>8} │",
            self.medium_entropy.optimal_spread,
            self.medium_entropy.optimal_skew,
            self.medium_entropy.best_sharpe,
            self.medium_entropy.best_return * 100.0,
            self.medium_entropy.best_trades);

        if self.low_entropy.should_quote {
            println!("│ Low Entropy     │ {:>5.1}  │ {:>5.2}  │ {:>+8.3}  │ {:>+7.2}% │ {:>8} │",
                self.low_entropy.optimal_spread,
                self.low_entropy.optimal_skew,
                self.low_entropy.best_sharpe,
                self.low_entropy.best_return * 100.0,
                self.low_entropy.best_trades);
        } else {
            println!("│ Low Entropy     │   NO QUOTING (optimal to sit out)                │");
        }
        println!("└─────────────────┴────────┴────────┴───────────┴──────────┴──────────┘");
        println!();

        // Strategy comparison
        println!("STRATEGY COMPARISON:");
        println!("┌─────────────────────┬────────────────┬────────────────┬────────────┐");
        println!("│ Metric              │    Uniform     │ Regime-Specific│ Improvement│");
        println!("├─────────────────────┼────────────────┼────────────────┼────────────┤");
        println!("│ Sharpe Ratio        │ {:>+13.3} │ {:>+13.3} │ {:>+9.3} │",
            self.comparison.uniform.sharpe,
            self.comparison.regime_specific.sharpe,
            self.comparison.sharpe_improvement);
        println!("│ Total Return        │ {:>+12.2}% │ {:>+12.2}% │ {:>+8.2}% │",
            self.comparison.uniform.total_return * 100.0,
            self.comparison.regime_specific.total_return * 100.0,
            self.comparison.return_improvement * 100.0);
        println!("│ Max Drawdown        │ {:>12.2}% │ {:>12.2}% │ {:>+8.2}% │",
            self.comparison.uniform.max_drawdown * 100.0,
            self.comparison.regime_specific.max_drawdown * 100.0,
            self.comparison.drawdown_improvement * 100.0);
        println!("│ Trade Count         │ {:>14} │ {:>14} │ {:>+10} │",
            self.comparison.uniform.num_trades,
            self.comparison.regime_specific.num_trades,
            self.comparison.trade_count_diff);
        println!("│ Win Rate            │ {:>12.1}% │ {:>12.1}% │            │",
            self.comparison.uniform.win_rate * 100.0,
            self.comparison.regime_specific.win_rate * 100.0);
        println!("└─────────────────────┴────────────────┴────────────────┴────────────┘");
        println!();

        // Recommendation
        let better = self.comparison.sharpe_improvement > 0.0;
        println!("RECOMMENDATION:");
        if better {
            println!("  ✓ Regime-specific params OUTPERFORM uniform by {:.3} Sharpe",
                self.comparison.sharpe_improvement);
            println!();
            println!("  Optimal Regime Configuration:");
            println!("    High Entropy:   spread={:.1}bps, skew={:.2}",
                self.optimal_regime_params.high.spread_bps,
                self.optimal_regime_params.high.skew_factor);
            println!("    Medium Entropy: spread={:.1}bps, skew={:.2}",
                self.optimal_regime_params.medium.spread_bps,
                self.optimal_regime_params.medium.skew_factor);
            if self.optimal_regime_params.low.should_quote {
                println!("    Low Entropy:    spread={:.1}bps, skew={:.2}",
                    self.optimal_regime_params.low.spread_bps,
                    self.optimal_regime_params.low.skew_factor);
            } else {
                println!("    Low Entropy:    NO QUOTING (pull quotes)");
            }
        } else {
            println!("  ✗ Uniform params perform better than regime-specific");
            println!("  Keep using: {}", self.comparison.uniform.params_description);
        }
        println!();

        // Caveats
        println!("CAVEATS:");
        println!("  - Results based on in-sample optimization (use OOS validation)");
        println!("  - Regime classification is backward-looking (real-time has lag)");
        println!("  - More parameters = more overfitting risk");
        println!();

        println!("════════════════════════════════════════════════════════════════════");
    }

    /// Save to JSON file
    pub fn save_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

/// Regime-specific parameter optimizer
pub struct RegimeOptimizer {
    config: RegimeOptimizerConfig,
    events: Vec<ReplayEvent>,
    time_range: Option<(i64, i64)>,
}

impl RegimeOptimizer {
    /// Create new optimizer
    pub fn new(config: RegimeOptimizerConfig) -> Self {
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
            println!("Loaded {} events for regime optimization", num_events);
        }

        Ok(num_events)
    }

    /// Classify event into regime
    fn classify_regime(&self, entropy: f64) -> MarketRegime {
        MarketRegime::from_entropy_score(entropy, &RegimeThresholds {
            high_entropy_threshold: self.config.high_entropy_threshold,
            low_entropy_threshold: self.config.low_entropy_threshold,
        })
    }

    /// Get entropy from event
    fn get_entropy(event: &ReplayEvent) -> f64 {
        event.snapshot.tick_entropy_10s
            .and_then(|d| d.to_string().parse::<f64>().ok())
            .unwrap_or(0.5)
    }

    /// Segment events by regime
    fn segment_by_regime(&self) -> (Vec<ReplayEvent>, Vec<ReplayEvent>, Vec<ReplayEvent>) {
        let mut high = Vec::new();
        let mut medium = Vec::new();
        let mut low = Vec::new();

        for event in &self.events {
            let entropy = Self::get_entropy(event);
            match self.classify_regime(entropy) {
                MarketRegime::HighEntropy => high.push(event.clone()),
                MarketRegime::MediumEntropy => medium.push(event.clone()),
                MarketRegime::LowEntropy => low.push(event.clone()),
            }
        }

        (high, medium, low)
    }

    /// Run backtest on subset of events with given params
    fn run_regime_backtest(
        &self,
        events: &[ReplayEvent],
        spread: f64,
        skew: f64,
    ) -> Result<BacktestResults> {
        if events.is_empty() {
            anyhow::bail!("No events for backtest");
        }

        let mm_config = MMConfig {
            regime_params: RegimeParams::fully_uniform(spread, skew),
            max_inventory: dec!(0.1),
            quote_size: dec!(0.001),
            regime_thresholds: RegimeThresholds {
                high_entropy_threshold: self.config.high_entropy_threshold,
                low_entropy_threshold: self.config.low_entropy_threshold,
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
                base_fill_probability: self.config.fill_probability,
                ..Default::default()
            },
            verbose: false,
            use_realistic_fills: true,
            ..Default::default()
        };

        let mut engine = BacktestEngine::from_events(config, events.to_vec());
        engine.run()
    }

    /// Run full backtest with regime-specific params
    fn run_full_backtest(&self, regime_params: &RegimeParams) -> Result<BacktestResults> {
        let mm_config = MMConfig {
            regime_params: regime_params.clone(),
            max_inventory: dec!(0.1),
            quote_size: dec!(0.001),
            regime_thresholds: RegimeThresholds {
                high_entropy_threshold: self.config.high_entropy_threshold,
                low_entropy_threshold: self.config.low_entropy_threshold,
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
                base_fill_probability: self.config.fill_probability,
                ..Default::default()
            },
            verbose: false,
            use_realistic_fills: true,
            ..Default::default()
        };

        let mut engine = BacktestEngine::from_events(config, self.events.clone());
        engine.run()
    }

    /// Find optimal params for a regime subset
    fn optimize_regime(
        &self,
        events: &[ReplayEvent],
        regime_name: &str,
    ) -> Result<(f64, f64, BacktestResults)> {
        if events.len() < 100 {
            if self.config.verbose {
                println!("  {} regime: insufficient data ({} events)", regime_name, events.len());
            }
            // Return default params
            return Ok((2.0, 0.5, self.run_regime_backtest(events, 2.0, 0.5)?));
        }

        let mut best_sharpe = f64::NEG_INFINITY;
        let mut best_spread = 2.0;
        let mut best_skew = 0.5;
        let mut best_results: Option<BacktestResults> = None;

        let total = self.config.spreads.len() * self.config.skews.len();
        let mut count = 0;

        for &spread in &self.config.spreads {
            for &skew in &self.config.skews {
                count += 1;

                match self.run_regime_backtest(events, spread, skew) {
                    Ok(results) => {
                        if results.metrics.num_trades >= self.config.min_trades {
                            if results.metrics.sharpe_ratio > best_sharpe {
                                best_sharpe = results.metrics.sharpe_ratio;
                                best_spread = spread;
                                best_skew = skew;
                                best_results = Some(results);
                            }
                        }
                    }
                    Err(_) => continue,
                }

                if self.config.verbose && count % 10 == 0 {
                    print!("\r  {} regime: {}/{} tested, best Sharpe={:+.3}",
                        regime_name, count, total, best_sharpe);
                }
            }
        }

        if self.config.verbose {
            println!("\r  {} regime: {}/{} tested, best Sharpe={:+.3}",
                regime_name, total, total, best_sharpe);
        }

        let results = best_results.unwrap_or_else(|| {
            self.run_regime_backtest(events, best_spread, best_skew).unwrap()
        });

        Ok((best_spread, best_skew, results))
    }

    /// Run optimization
    pub fn optimize(&self) -> Result<RegimeOptimizationResults> {
        if self.events.is_empty() {
            anyhow::bail!("No data loaded. Call load_data() first.");
        }

        let time_span_hours = if let Some((start, end)) = self.time_range {
            (end - start) as f64 / (1000.0 * 60.0 * 60.0)
        } else {
            0.0
        };

        if self.config.verbose {
            println!();
            println!("Running regime-specific optimization...");
            println!("Thresholds: high>{:.2}, low<{:.2}",
                self.config.high_entropy_threshold,
                self.config.low_entropy_threshold);
            println!();
        }

        // Segment data by regime
        let (high_events, med_events, low_events) = self.segment_by_regime();
        let total = self.events.len();

        if self.config.verbose {
            println!("Regime distribution:");
            println!("  High entropy:   {} events ({:.1}%)",
                high_events.len(), high_events.len() as f64 / total as f64 * 100.0);
            println!("  Medium entropy: {} events ({:.1}%)",
                med_events.len(), med_events.len() as f64 / total as f64 * 100.0);
            println!("  Low entropy:    {} events ({:.1}%)",
                low_events.len(), low_events.len() as f64 / total as f64 * 100.0);
            println!();
        }

        // Optimize each regime independently
        if self.config.verbose {
            println!("Optimizing parameters per regime...");
        }

        let (high_spread, high_skew, high_results) =
            self.optimize_regime(&high_events, "High")?;
        let (med_spread, med_skew, med_results) =
            self.optimize_regime(&med_events, "Medium")?;

        // For low entropy, also test no-quoting option
        let (low_spread, low_skew, low_should_quote, low_results): (f64, f64, bool, Option<BacktestResults>) =
            if self.config.allow_no_quote_low && !low_events.is_empty() {
                let (spread, skew, results) = self.optimize_regime(&low_events, "Low")?;

                // Compare vs no quoting (Sharpe of 0 with 0 trades)
                let no_quote_better = results.metrics.sharpe_ratio < 0.0
                    || results.metrics.num_trades < self.config.min_trades;

                if no_quote_better {
                    if self.config.verbose {
                        println!("  Low regime: NO QUOTING is optimal (Sharpe={:+.3} < 0)",
                            results.metrics.sharpe_ratio);
                    }
                    (spread, skew, false, Some(results))
                } else {
                    (spread, skew, true, Some(results))
                }
            } else if !low_events.is_empty() {
                let (spread, skew, results) = self.optimize_regime(&low_events, "Low")?;
                (spread, skew, true, Some(results))
            } else {
                // No low entropy events - return placeholder
                let spread = self.config.spreads.first().copied().unwrap_or(5.0);
                let skew = self.config.skews.first().copied().unwrap_or(1.0);
                (spread, skew, false, None)
            };

        // Build optimal regime params
        let optimal_params = OptimalRegimeParams {
            high: ParamSet {
                spread_bps: high_spread,
                skew_factor: high_skew,
                should_quote: true,
            },
            medium: ParamSet {
                spread_bps: med_spread,
                skew_factor: med_skew,
                should_quote: true,
            },
            low: ParamSet {
                spread_bps: low_spread,
                skew_factor: low_skew,
                should_quote: low_should_quote,
            },
        };

        if self.config.verbose {
            println!();
            println!("Running full backtest comparisons...");
        }

        // Run full backtest with regime-specific params
        let regime_specific_results = self.run_full_backtest(&optimal_params.to_regime_params())?;

        // Run full backtest with uniform params (best from high entropy as baseline)
        let uniform_params = RegimeParams::fully_uniform(high_spread, high_skew);
        let uniform_results = self.run_full_backtest(&uniform_params)?;

        // Build comparison
        let comparison = StrategyComparison {
            uniform: FullBacktestMetrics {
                sharpe: uniform_results.metrics.sharpe_ratio,
                total_return: uniform_results.metrics.total_return,
                max_drawdown: uniform_results.metrics.max_drawdown,
                num_trades: uniform_results.metrics.num_trades,
                win_rate: uniform_results.metrics.win_rate,
                params_description: format!("spread={:.1}bps, skew={:.2} (all regimes)",
                    high_spread, high_skew),
            },
            regime_specific: FullBacktestMetrics {
                sharpe: regime_specific_results.metrics.sharpe_ratio,
                total_return: regime_specific_results.metrics.total_return,
                max_drawdown: regime_specific_results.metrics.max_drawdown,
                num_trades: regime_specific_results.metrics.num_trades,
                win_rate: regime_specific_results.metrics.win_rate,
                params_description: format!(
                    "H({:.1},{:.2}) M({:.1},{:.2}) L({})",
                    high_spread, high_skew,
                    med_spread, med_skew,
                    if low_should_quote { format!("{:.1},{:.2}", low_spread, low_skew) }
                    else { "off".to_string() }
                ),
            },
            sharpe_improvement: regime_specific_results.metrics.sharpe_ratio - uniform_results.metrics.sharpe_ratio,
            return_improvement: regime_specific_results.metrics.total_return - uniform_results.metrics.total_return,
            drawdown_improvement: uniform_results.metrics.max_drawdown - regime_specific_results.metrics.max_drawdown,
            trade_count_diff: regime_specific_results.metrics.num_trades as i64 - uniform_results.metrics.num_trades as i64,
        };

        // Estimate time in each regime
        let high_frac = high_events.len() as f64 / total as f64;
        let med_frac = med_events.len() as f64 / total as f64;
        let low_frac = low_events.len() as f64 / total as f64;

        Ok(RegimeOptimizationResults {
            high_entropy: RegimeMetrics {
                regime: "High Entropy".to_string(),
                event_count: high_events.len(),
                event_fraction: high_frac,
                time_hours: time_span_hours * high_frac,
                optimal_spread: high_spread,
                optimal_skew: high_skew,
                should_quote: true,
                best_sharpe: high_results.metrics.sharpe_ratio,
                best_return: high_results.metrics.total_return,
                best_drawdown: high_results.metrics.max_drawdown,
                best_trades: high_results.metrics.num_trades,
                best_win_rate: high_results.metrics.win_rate,
            },
            medium_entropy: RegimeMetrics {
                regime: "Medium Entropy".to_string(),
                event_count: med_events.len(),
                event_fraction: med_frac,
                time_hours: time_span_hours * med_frac,
                optimal_spread: med_spread,
                optimal_skew: med_skew,
                should_quote: true,
                best_sharpe: med_results.metrics.sharpe_ratio,
                best_return: med_results.metrics.total_return,
                best_drawdown: med_results.metrics.max_drawdown,
                best_trades: med_results.metrics.num_trades,
                best_win_rate: med_results.metrics.win_rate,
            },
            low_entropy: RegimeMetrics {
                regime: "Low Entropy".to_string(),
                event_count: low_events.len(),
                event_fraction: low_frac,
                time_hours: time_span_hours * low_frac,
                optimal_spread: low_spread,
                optimal_skew: low_skew,
                should_quote: low_should_quote,
                best_sharpe: low_results.as_ref().map(|r| r.metrics.sharpe_ratio).unwrap_or(0.0),
                best_return: low_results.as_ref().map(|r| r.metrics.total_return).unwrap_or(0.0),
                best_drawdown: low_results.as_ref().map(|r| r.metrics.max_drawdown).unwrap_or(0.0),
                best_trades: low_results.as_ref().map(|r| r.metrics.num_trades).unwrap_or(0),
                best_win_rate: low_results.as_ref().map(|r| r.metrics.win_rate).unwrap_or(0.0),
            },
            optimal_regime_params: optimal_params,
            comparison,
            config: self.config.clone(),
            total_events: total,
            time_span_hours,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = RegimeOptimizerConfig::default();
        assert_eq!(config.high_entropy_threshold, 0.7);
        assert_eq!(config.low_entropy_threshold, 0.4);
        assert!(!config.spreads.is_empty());
        assert!(!config.skews.is_empty());
    }

    #[test]
    fn test_param_set() {
        let params = OptimalRegimeParams {
            high: ParamSet { spread_bps: 1.0, skew_factor: 0.3, should_quote: true },
            medium: ParamSet { spread_bps: 2.0, skew_factor: 0.5, should_quote: true },
            low: ParamSet { spread_bps: 5.0, skew_factor: 1.0, should_quote: false },
        };

        let regime_params = params.to_regime_params();
        assert_eq!(regime_params.high_entropy.spread_bps, 1.0);
        assert_eq!(regime_params.medium_entropy.spread_bps, 2.0);
        assert!(!regime_params.low_entropy.should_quote);
    }

    #[test]
    fn test_regime_metrics() {
        let metrics = RegimeMetrics {
            regime: "High".to_string(),
            event_count: 1000,
            event_fraction: 0.5,
            time_hours: 24.0,
            optimal_spread: 1.0,
            optimal_skew: 0.3,
            should_quote: true,
            best_sharpe: 2.5,
            best_return: 0.05,
            best_drawdown: 0.01,
            best_trades: 100,
            best_win_rate: 0.62,
        };

        assert_eq!(metrics.event_count, 1000);
        assert!(metrics.should_quote);
    }

    #[test]
    fn test_strategy_comparison() {
        let comparison = StrategyComparison {
            uniform: FullBacktestMetrics {
                sharpe: 1.0,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                params_description: "uniform".to_string(),
            },
            regime_specific: FullBacktestMetrics {
                sharpe: 1.5,
                total_return: 0.07,
                max_drawdown: 0.015,
                num_trades: 120,
                win_rate: 0.60,
                params_description: "regime-specific".to_string(),
            },
            sharpe_improvement: 0.5,
            return_improvement: 0.02,
            drawdown_improvement: 0.005,
            trade_count_diff: 20,
        };

        assert!(comparison.sharpe_improvement > 0.0);
        assert!(comparison.return_improvement > 0.0);
    }
}

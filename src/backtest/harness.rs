//! Backtest Harness
//!
//! Run strategies on historical data and collect results.
//!
//! Uses the realistic fill simulator based on:
//! - Queue position modeling
//! - Adverse selection
//! - Trade intensity
//! - Regime awareness

use std::path::PathBuf;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use crate::market_maker::{MarketMakerEngine, MMConfig, Fill, QuoteSide};
use crate::mm_simulator::SimulatorConfig;

use super::replay::{ParquetReplay, ReplayConfig, ReplayEvent};
use super::fill_simulator::{FillSimulator, FillSimulatorConfig, MarketState};
use super::metrics::{
    PerformanceMetrics, TradeLog, TradeRecord, TradeSide,
    EquityCurve, EquityPoint,
};

/// Configuration for the backtest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    /// Replay configuration
    pub replay: ReplayConfig,
    /// Market maker configuration
    pub mm: MMConfig,
    /// Legacy simulator configuration (for fee rate)
    pub simulator: SimulatorConfig,
    /// Fill simulator configuration (realistic fills)
    pub fill_sim: FillSimulatorConfig,
    /// Initial capital (for equity tracking)
    pub initial_capital: Decimal,
    /// Risk-free rate for Sharpe calculation
    pub risk_free_rate: f64,
    /// How often to record equity (in events)
    pub equity_sample_interval: usize,
    /// Whether to print progress
    pub verbose: bool,
    /// Whether to use realistic fill simulation (vs naive)
    pub use_realistic_fills: bool,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            replay: ReplayConfig::default(),
            mm: MMConfig::default(),
            simulator: SimulatorConfig::default(),
            fill_sim: FillSimulatorConfig::default(),
            initial_capital: dec!(10000), // $10k starting capital
            risk_free_rate: 0.05, // 5% annual
            equity_sample_interval: 100, // Every 100 events
            verbose: true,
            use_realistic_fills: true, // Use realistic fills by default
        }
    }
}

/// Results from a backtest run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResults {
    pub config: BacktestConfig,
    pub metrics: PerformanceMetrics,
    pub trade_log: TradeLog,
    pub equity_curve: EquityCurve,
    pub events_processed: usize,
    pub fills_generated: usize,
    /// Fill simulation statistics
    pub fill_stats: FillStats,
}

/// Statistics from the fill simulator
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FillStats {
    pub bid_touches: u64,
    pub ask_touches: u64,
    pub bid_fills: u64,
    pub ask_fills: u64,
    pub bid_fill_rate: f64,
    pub ask_fill_rate: f64,
    pub partial_fills: u64,
    pub total_adverse_selection_cost: Decimal,
    pub avg_fill_probability: f64,
}

impl BacktestResults {
    /// Save results to JSON file
    pub fn save_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Print summary
    pub fn print_summary(&self) {
        println!();
        println!("Events Processed: {}", self.events_processed);
        println!("Fills Generated:  {}", self.fills_generated);
        println!();

        if self.config.use_realistic_fills {
            println!("FILL SIMULATION (Realistic)");
            println!("  Bid Touches: {} | Fills: {} | Rate: {:.1}%",
                self.fill_stats.bid_touches,
                self.fill_stats.bid_fills,
                self.fill_stats.bid_fill_rate * 100.0);
            println!("  Ask Touches: {} | Fills: {} | Rate: {:.1}%",
                self.fill_stats.ask_touches,
                self.fill_stats.ask_fills,
                self.fill_stats.ask_fill_rate * 100.0);
            println!("  Partial Fills: {}", self.fill_stats.partial_fills);
            println!("  Adverse Selection Cost: {:.4}",
                self.fill_stats.total_adverse_selection_cost);
            println!();
        }

        self.metrics.print_report();
    }
}

/// Backtest engine
pub struct BacktestEngine {
    config: BacktestConfig,
    replay: ParquetReplay,
    mm: MarketMakerEngine,
    fill_sim: FillSimulator,

    // State
    trade_log: TradeLog,
    equity_curve: EquityCurve,
    events_processed: usize,
    fills_generated: usize,

    // For market state tracking
    last_mid_price: Option<Decimal>,
}

impl BacktestEngine {
    /// Create a new backtest engine
    pub fn new(config: BacktestConfig) -> Self {
        let replay = ParquetReplay::new(config.replay.clone());
        let mm = MarketMakerEngine::new(config.mm.clone());
        let fill_sim = FillSimulator::new(config.fill_sim.clone());

        Self {
            config,
            replay,
            mm,
            fill_sim,
            trade_log: TradeLog::new(),
            equity_curve: EquityCurve::new(),
            events_processed: 0,
            fills_generated: 0,
            last_mid_price: None,
        }
    }

    /// Load historical data
    pub fn load_data(&mut self) -> Result<usize> {
        self.replay.load()
    }

    /// Load data from a specific directory
    pub fn load_from(&mut self, data_dir: &str) -> Result<usize> {
        self.config.replay.data_dir = PathBuf::from(data_dir);
        self.replay = ParquetReplay::new(self.config.replay.clone());
        self.replay.load()
    }

    /// Run the backtest
    pub fn run(&mut self) -> Result<BacktestResults> {
        if self.replay.is_empty() {
            anyhow::bail!("No data loaded. Call load_data() first.");
        }

        // Reset state
        self.mm.reset();
        self.fill_sim.reset();
        self.trade_log = TradeLog::new();
        self.equity_curve = EquityCurve::new();
        self.events_processed = 0;
        self.fills_generated = 0;
        self.last_mid_price = None;

        // Record initial equity
        self.record_equity(0);

        let total_events = self.replay.len();

        if self.config.verbose {
            println!("Starting backtest with {} events", total_events);
            println!("Fill simulation: {}",
                if self.config.use_realistic_fills { "REALISTIC" } else { "NAIVE" });
            if let Some((start, end)) = self.replay.time_range() {
                let duration_hours = (end - start) as f64 / (1000.0 * 60.0 * 60.0);
                println!("Time range: {:.1} hours", duration_hours);
            }
        }

        // Process each event
        while let Some(event) = self.replay.next() {
            self.process_event(&event)?;

            // Record equity periodically
            if self.events_processed % self.config.equity_sample_interval == 0 {
                self.record_equity(event.timestamp_ms);
            }

            // Progress logging
            if self.config.verbose && self.events_processed % 10000 == 0 {
                let progress = self.replay.progress() * 100.0;
                println!(
                    "  Progress: {:.1}% | Events: {} | Fills: {} | PnL: {:.4}",
                    progress,
                    self.events_processed,
                    self.fills_generated,
                    self.mm.pnl().total_pnl
                );
            }
        }

        // Record final equity
        if let Some((_, end)) = self.replay.time_range() {
            self.record_equity(end);
        }

        // Calculate metrics
        let metrics = PerformanceMetrics::calculate(
            &self.equity_curve,
            &self.trade_log,
            self.config.risk_free_rate,
        );

        // Collect fill stats
        let (bid_rate, ask_rate) = self.fill_sim.fill_rate();
        let fill_stats = FillStats {
            bid_touches: self.fill_sim.stats.bid_touches,
            ask_touches: self.fill_sim.stats.ask_touches,
            bid_fills: self.fill_sim.stats.bid_fills,
            ask_fills: self.fill_sim.stats.ask_fills,
            bid_fill_rate: bid_rate,
            ask_fill_rate: ask_rate,
            partial_fills: self.fill_sim.stats.bid_partial_fills
                + self.fill_sim.stats.ask_partial_fills,
            total_adverse_selection_cost: self.fill_sim.stats.total_adverse_selection_cost,
            avg_fill_probability: if self.fills_generated > 0 {
                self.fill_sim.stats.cumulative_fill_probability / self.fills_generated as f64
            } else {
                0.0
            },
        };

        Ok(BacktestResults {
            config: self.config.clone(),
            metrics,
            trade_log: self.trade_log.clone(),
            equity_curve: self.equity_curve.clone(),
            events_processed: self.events_processed,
            fills_generated: self.fills_generated,
            fill_stats,
        })
    }

    /// Process a single event
    fn process_event(&mut self, event: &ReplayEvent) -> Result<()> {
        let snap = &event.snapshot;
        let timestamp_ms = event.timestamp_ms as u64;

        // Extract features for MM - skip events with missing price data
        let mid_price = match snap.mid_price {
            Some(p) if p > dec!(0) => p,
            _ => {
                // Skip events with invalid/missing prices
                self.events_processed += 1;
                return Ok(());
            }
        };

        let microprice = snap.microprice.unwrap_or(mid_price);
        let volatility = snap.realized_volatility_100.unwrap_or(0.001);

        // Compute entropy score
        let entropy_score = self.mm.compute_entropy_score(
            snap.tick_entropy_1s,
            snap.tick_entropy_5s,
            snap.tick_entropy_10s,
        );

        // Compute flow imbalance
        let buy_vol = snap.aggr_ratio_100.unwrap_or(Decimal::new(5, 1));
        let sell_vol = Decimal::ONE - buy_vol;
        let flow_imbalance = self.mm.compute_flow_imbalance(buy_vol, sell_vol);

        // Compute quotes
        let quotes = self.mm.compute_quotes(
            microprice,
            mid_price,
            volatility,
            entropy_score,
            flow_imbalance,
            timestamp_ms,
        );

        // Update fill simulator with new quotes
        self.fill_sim.update_quotes(&quotes, timestamp_ms);

        // Simulate fills
        if let Some(prev_mid) = self.last_mid_price {
            let fills = if self.config.use_realistic_fills {
                // Use realistic fill simulation
                let market_state = MarketState::from_snapshot(snap, prev_mid);
                let fill_events = self.fill_sim.simulate_fills(&market_state, timestamp_ms);

                fill_events.into_iter().map(|fe| fe.fill).collect()
            } else {
                // Use naive fill simulation (legacy)
                self.simulate_fills_naive(mid_price, &quotes, timestamp_ms)
            };

            for fill in fills {
                self.process_fill(fill, timestamp_ms)?;
            }
        }

        // Update mark-to-market
        self.mm.update_mark_to_market(mid_price);

        self.last_mid_price = Some(mid_price);
        self.events_processed += 1;

        Ok(())
    }

    /// Naive fill simulation (legacy) - for comparison
    fn simulate_fills_naive(
        &self,
        current_mid: Decimal,
        quotes: &crate::market_maker::MMQuotes,
        timestamp_ms: u64,
    ) -> Vec<Fill> {
        let mut fills = Vec::new();

        // Check bid fill: if price dropped to our bid level
        if let Some(ref bid) = quotes.bid {
            if current_mid <= bid.price {
                fills.push(Fill {
                    side: QuoteSide::Bid,
                    price: bid.price,
                    size: bid.size,
                    timestamp_ms,
                });
            }
        }

        // Check ask fill: if price rose to our ask level
        if let Some(ref ask) = quotes.ask {
            if current_mid >= ask.price {
                fills.push(Fill {
                    side: QuoteSide::Ask,
                    price: ask.price,
                    size: ask.size,
                    timestamp_ms,
                });
            }
        }

        fills
    }

    /// Process a fill
    fn process_fill(&mut self, fill: Fill, timestamp_ms: u64) -> Result<()> {
        let fee_rate = self.config.fill_sim.fee_rate;
        let fee = fill.price * fill.size * fee_rate;

        // Record trade
        let pnl = self.calculate_fill_pnl(&fill);
        self.trade_log.add(TradeRecord {
            timestamp_ms: timestamp_ms as i64,
            side: match fill.side {
                QuoteSide::Bid => TradeSide::Buy,
                QuoteSide::Ask => TradeSide::Sell,
            },
            price: fill.price,
            size: fill.size,
            fee,
            pnl,
        });

        // Process in MM engine
        self.mm.process_fill(fill, fee_rate);
        self.fills_generated += 1;

        Ok(())
    }

    /// Calculate PnL from a fill (if closing position)
    fn calculate_fill_pnl(&self, fill: &Fill) -> Option<Decimal> {
        let inventory = self.mm.inventory();
        let avg_entry = self.mm.get_state().avg_entry_price;

        match fill.side {
            QuoteSide::Ask if inventory > dec!(0) => {
                // Selling while long = closing (partially or fully)
                let close_size = fill.size.min(inventory);
                if close_size > dec!(0) && avg_entry > dec!(0) {
                    Some((fill.price - avg_entry) * close_size)
                } else {
                    None
                }
            }
            QuoteSide::Bid if inventory < dec!(0) => {
                // Buying while short = closing
                let close_size = fill.size.min(inventory.abs());
                if close_size > dec!(0) && avg_entry > dec!(0) {
                    Some((avg_entry - fill.price) * close_size)
                } else {
                    None
                }
            }
            _ => None, // Opening position - no realized PnL yet
        }
    }

    /// Record current equity
    fn record_equity(&mut self, timestamp_ms: i64) {
        let pnl = self.mm.pnl();
        let equity = self.config.initial_capital + pnl.total_pnl;

        self.equity_curve.add(EquityPoint {
            timestamp_ms,
            equity,
            unrealized_pnl: pnl.unrealized_pnl,
            realized_pnl: pnl.realized_pnl,
            inventory: self.mm.inventory(),
        });
    }

    /// Get current state
    pub fn state(&self) -> BacktestState {
        BacktestState {
            events_processed: self.events_processed,
            fills_generated: self.fills_generated,
            progress: self.replay.progress(),
            current_pnl: self.mm.pnl().total_pnl,
            current_inventory: self.mm.inventory(),
        }
    }
}

/// Current backtest state (for monitoring)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestState {
    pub events_processed: usize,
    pub fills_generated: usize,
    pub progress: f64,
    pub current_pnl: Decimal,
    pub current_inventory: Decimal,
}

/// Run a quick backtest with default settings
pub fn quick_backtest(data_dir: &str) -> Result<BacktestResults> {
    let config = BacktestConfig {
        replay: ReplayConfig {
            data_dir: PathBuf::from(data_dir),
            ..Default::default()
        },
        ..Default::default()
    };

    let mut engine = BacktestEngine::new(config);
    engine.load_data()?;
    engine.run()
}

/// Run backtest with custom MM config
pub fn backtest_with_config(
    data_dir: &str,
    mm_config: MMConfig,
) -> Result<BacktestResults> {
    let config = BacktestConfig {
        replay: ReplayConfig {
            data_dir: PathBuf::from(data_dir),
            ..Default::default()
        },
        mm: mm_config,
        ..Default::default()
    };

    let mut engine = BacktestEngine::new(config);
    engine.load_data()?;
    engine.run()
}

/// Run backtest with naive fills (for comparison)
pub fn backtest_naive_fills(
    data_dir: &str,
    mm_config: MMConfig,
) -> Result<BacktestResults> {
    let config = BacktestConfig {
        replay: ReplayConfig {
            data_dir: PathBuf::from(data_dir),
            ..Default::default()
        },
        mm: mm_config,
        use_realistic_fills: false, // Use naive fills
        ..Default::default()
    };

    let mut engine = BacktestEngine::new(config);
    engine.load_data()?;
    engine.run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backtest_config_default() {
        let config = BacktestConfig::default();
        assert_eq!(config.initial_capital, dec!(10000));
        assert_eq!(config.equity_sample_interval, 100);
        assert!(config.use_realistic_fills);
    }

    #[test]
    fn test_backtest_state() {
        let state = BacktestState {
            events_processed: 100,
            fills_generated: 5,
            progress: 0.5,
            current_pnl: dec!(10),
            current_inventory: dec!(0.01),
        };
        assert_eq!(state.events_processed, 100);
    }

    #[test]
    fn test_fill_stats_default() {
        let stats = FillStats::default();
        assert_eq!(stats.bid_fills, 0);
        assert_eq!(stats.ask_fills, 0);
    }
}

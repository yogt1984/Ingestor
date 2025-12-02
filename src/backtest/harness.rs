//! Backtest Harness
//!
//! Run strategies on historical data and collect results.

use std::path::PathBuf;

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use crate::feature_fusion::FeaturesSnapshot;
use crate::market_maker::{MarketMakerEngine, MMConfig, MMQuotes, Fill, QuoteSide};
use crate::mm_simulator::{MMSimulator, SimulatorConfig};
use crate::tradeslog::Trade;

use super::replay::{ParquetReplay, ReplayConfig, ReplayEvent};
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
    /// Simulator configuration
    pub simulator: SimulatorConfig,
    /// Initial capital (for equity tracking)
    pub initial_capital: Decimal,
    /// Risk-free rate for Sharpe calculation
    pub risk_free_rate: f64,
    /// How often to record equity (in events)
    pub equity_sample_interval: usize,
    /// Whether to print progress
    pub verbose: bool,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            replay: ReplayConfig::default(),
            mm: MMConfig::default(),
            simulator: SimulatorConfig::default(),
            initial_capital: dec!(10000), // $10k starting capital
            risk_free_rate: 0.05, // 5% annual
            equity_sample_interval: 100, // Every 100 events
            verbose: true,
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
        self.metrics.print_report();
    }
}

/// Backtest engine
pub struct BacktestEngine {
    config: BacktestConfig,
    replay: ParquetReplay,
    mm: MarketMakerEngine,
    simulator: MMSimulator,

    // State
    trade_log: TradeLog,
    equity_curve: EquityCurve,
    events_processed: usize,
    fills_generated: usize,

    // For simulating trades from price movements
    last_mid_price: Option<Decimal>,
}

impl BacktestEngine {
    /// Create a new backtest engine
    pub fn new(config: BacktestConfig) -> Self {
        let replay = ParquetReplay::new(config.replay.clone());
        let mm = MarketMakerEngine::new(config.mm.clone());
        let simulator = MMSimulator::new(config.simulator.clone());

        Self {
            config,
            replay,
            mm,
            simulator,
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
        self.simulator.reset();
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

        Ok(BacktestResults {
            config: self.config.clone(),
            metrics,
            trade_log: self.trade_log.clone(),
            equity_curve: self.equity_curve.clone(),
            events_processed: self.events_processed,
            fills_generated: self.fills_generated,
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

        // Update simulator with new quotes
        self.simulator.update_quotes(&quotes);

        // Simulate fills based on price movement
        if let Some(last_mid) = self.last_mid_price {
            let fills = self.simulate_fills_from_movement(
                last_mid,
                mid_price,
                &quotes,
                timestamp_ms,
            );

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

    /// Simulate fills based on price movement
    ///
    /// This is a simplified model: if price moves through our quote,
    /// we assume we got filled.
    fn simulate_fills_from_movement(
        &mut self,
        last_mid: Decimal,
        current_mid: Decimal,
        quotes: &MMQuotes,
        timestamp_ms: u64,
    ) -> Vec<Fill> {
        let mut fills = Vec::new();

        // Check bid fill: if price dropped to our bid level
        if let Some(ref bid) = quotes.bid {
            // Simple model: if low of the move touched our bid, we got filled
            // Since we only have mid prices, we approximate:
            // If current_mid < bid.price, aggressive sell hit our bid
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
        let fee_rate = self.config.simulator.fee_rate;
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
                // PnL = (sell_price - avg_entry) * size
                let close_size = fill.size.min(inventory);
                if close_size > dec!(0) && avg_entry > dec!(0) {
                    Some((fill.price - avg_entry) * close_size)
                } else {
                    None
                }
            }
            QuoteSide::Bid if inventory < dec!(0) => {
                // Buying while short = closing
                // PnL = (avg_entry - buy_price) * size (profit if buy < entry)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backtest_config_default() {
        let config = BacktestConfig::default();
        assert_eq!(config.initial_capital, dec!(10000));
        assert_eq!(config.equity_sample_interval, 100);
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
}

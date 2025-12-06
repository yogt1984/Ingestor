//! Market Maker Simulator
//!
//! Simulates order fills based on incoming trades and order book state.
//! Uses a simple queue-position model for fill probability.
//!
//! This allows backtesting and paper trading of the MM strategy.

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use crate::algorithms::{AlgorithmType, MarketInput, MarketMakingAlgorithm};
use crate::market_maker::{Quote, QuoteSide, Fill, MarketMakerEngine, MMQuotes};
use crate::tradeslog::Trade;

/// Configuration for the simulator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatorConfig {
    /// Fee rate (e.g., 0.0001 = 1 bps)
    pub fee_rate: Decimal,
    /// Minimum time a quote must be active before it can be filled (ms)
    pub min_quote_age_ms: u64,
    /// Whether to use probabilistic fills based on queue position
    pub use_queue_model: bool,
    /// Assumed queue position as fraction of total depth (0.5 = middle of queue)
    pub queue_position_fraction: f64,
}

impl Default for SimulatorConfig {
    fn default() -> Self {
        Self {
            fee_rate: dec!(0.0001), // 1 bps
            min_quote_age_ms: 100,
            use_queue_model: true,
            queue_position_fraction: 0.5,
        }
    }
}

/// Simulator for MM fills
pub struct MMSimulator {
    config: SimulatorConfig,

    // Track active quotes
    active_bid: Option<Quote>,
    active_ask: Option<Quote>,

    // Statistics
    stats: SimulatorStats,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SimulatorStats {
    pub trades_seen: u64,
    pub bid_fills: u64,
    pub ask_fills: u64,
    pub bid_misses: u64,
    pub ask_misses: u64,
    pub total_fill_volume: Decimal,
}

impl SimulatorStats {
    /// Calculate fill rate as ratio of fills to total opportunities
    pub fn fill_rate(&self) -> f64 {
        let total_opportunities = self.bid_fills + self.ask_fills
            + self.bid_misses + self.ask_misses;
        if total_opportunities == 0 {
            return 0.0;
        }
        (self.bid_fills + self.ask_fills) as f64 / total_opportunities as f64
    }
}

impl MMSimulator {
    pub fn new(config: SimulatorConfig) -> Self {
        Self {
            config,
            active_bid: None,
            active_ask: None,
            stats: SimulatorStats::default(),
        }
    }

    /// Update the simulator with new quotes from the MM engine
    pub fn update_quotes(&mut self, quotes: &MMQuotes) {
        self.active_bid = quotes.bid.clone();
        self.active_ask = quotes.ask.clone();
    }

    /// Process an incoming trade and check if our quotes would be filled
    ///
    /// Returns fills that occurred (0, 1, or 2 fills possible)
    pub fn process_trade(&mut self, trade: &Trade, current_time_ms: u64) -> Vec<Fill> {
        let mut fills = Vec::new();
        self.stats.trades_seen += 1;

        // Check bid fill: if trade is a sell (is_buyer_maker = true means seller aggressed)
        // and trade price <= our bid price
        if trade.is_buyer_maker {
            // This is an aggressive sell hitting bids
            if let Some(ref bid) = self.active_bid {
                if self.check_fill(bid, trade, current_time_ms, true) {
                    let fill_size = trade.quantity.min(bid.size);
                    fills.push(Fill {
                        side: QuoteSide::Bid,
                        price: bid.price,
                        size: fill_size,
                        timestamp_ms: current_time_ms,
                    });
                    self.stats.bid_fills += 1;
                    self.stats.total_fill_volume += fill_size;
                } else {
                    self.stats.bid_misses += 1;
                }
            }
        } else {
            // This is an aggressive buy hitting asks
            if let Some(ref ask) = self.active_ask {
                if self.check_fill(ask, trade, current_time_ms, false) {
                    let fill_size = trade.quantity.min(ask.size);
                    fills.push(Fill {
                        side: QuoteSide::Ask,
                        price: ask.price,
                        size: fill_size,
                        timestamp_ms: current_time_ms,
                    });
                    self.stats.ask_fills += 1;
                    self.stats.total_fill_volume += fill_size;
                } else {
                    self.stats.ask_misses += 1;
                }
            }
        }

        fills
    }

    /// Check if a quote would be filled by a trade
    fn check_fill(&self, quote: &Quote, trade: &Trade, current_time_ms: u64, is_bid: bool) -> bool {
        // Check quote age
        if current_time_ms < quote.timestamp_ms + self.config.min_quote_age_ms {
            return false;
        }

        // Check price match
        let price_matches = if is_bid {
            // For bid: trade price must be at or below our bid
            trade.price <= quote.price
        } else {
            // For ask: trade price must be at or above our ask
            trade.price >= quote.price
        };

        if !price_matches {
            return false;
        }

        // If not using queue model, price match is enough
        if !self.config.use_queue_model {
            return true;
        }

        // Simple queue model: probability of fill based on trade size vs our position
        // Assume we're at queue_position_fraction of the queue
        // If trade volume > (1 - queue_position) * total_depth, we get filled
        //
        // Simplified: if trade.quantity > quote.size * (1 / queue_position_fraction), we're likely filled
        let threshold = quote.size * Decimal::from_f64(1.0 / self.config.queue_position_fraction)
            .unwrap_or(dec!(2));

        trade.quantity >= threshold
    }

    /// Get statistics
    pub fn stats(&self) -> &SimulatorStats {
        &self.stats
    }

    /// Reset simulator state
    pub fn reset(&mut self) {
        self.active_bid = None;
        self.active_ask = None;
        self.stats = SimulatorStats::default();
    }

    /// Get fill rate
    pub fn fill_rate(&self) -> f64 {
        let total_opportunities = self.stats.bid_fills + self.stats.ask_fills
            + self.stats.bid_misses + self.stats.ask_misses;
        if total_opportunities == 0 {
            return 0.0;
        }
        (self.stats.bid_fills + self.stats.ask_fills) as f64 / total_opportunities as f64
    }
}

/// Combined MM + Simulator for paper trading
pub struct PaperTradingEngine {
    pub mm: MarketMakerEngine,
    pub simulator: MMSimulator,
    last_quotes: Option<MMQuotes>,
}

impl PaperTradingEngine {
    pub fn new(mm: MarketMakerEngine, sim_config: SimulatorConfig) -> Self {
        Self {
            mm,
            simulator: MMSimulator::new(sim_config),
            last_quotes: None,
        }
    }

    /// Process a feature snapshot and compute new quotes
    pub fn on_features(
        &mut self,
        microprice: Decimal,
        mid_price: Decimal,
        volatility: f64,
        entropy_score: f64,
        flow_imbalance: f64,
        timestamp_ms: u64,
    ) -> MMQuotes {
        let quotes = self.mm.compute_quotes(
            microprice,
            mid_price,
            volatility,
            entropy_score,
            flow_imbalance,
            timestamp_ms,
        );

        self.simulator.update_quotes(&quotes);
        self.last_quotes = Some(quotes.clone());

        // Update mark-to-market
        self.mm.update_mark_to_market(mid_price);

        quotes
    }

    /// Process an incoming trade for potential fills
    pub fn on_trade(&mut self, trade: &Trade, current_time_ms: u64) -> Vec<Fill> {
        let fills = self.simulator.process_trade(trade, current_time_ms);

        // Process fills in MM engine
        for fill in &fills {
            self.mm.process_fill(fill.clone(), self.simulator.config.fee_rate);
        }

        fills
    }

    /// Get current state
    pub fn state(&self) -> PaperTradingState {
        PaperTradingState {
            mm_state: self.mm.get_state(),
            sim_stats: self.simulator.stats().clone(),
            last_quotes: self.last_quotes.clone(),
            algorithm_type: AlgorithmType::AvellanedaStoikov,
        }
    }

    /// Reset everything
    pub fn reset(&mut self) {
        self.mm.reset();
        self.simulator.reset();
        self.last_quotes = None;
    }
}

/// Generic paper trading engine that works with any MarketMakingAlgorithm
pub struct GenericPaperTradingEngine {
    pub algorithm: Box<dyn MarketMakingAlgorithm>,
    pub simulator: MMSimulator,
    last_quotes: Option<MMQuotes>,
    /// Cached best bid for computing MarketInput
    last_best_bid: Decimal,
    /// Cached best ask for computing MarketInput
    last_best_ask: Decimal,
}

impl GenericPaperTradingEngine {
    /// Create a new generic paper trading engine with any algorithm
    pub fn new(algorithm: Box<dyn MarketMakingAlgorithm>, sim_config: SimulatorConfig) -> Self {
        Self {
            algorithm,
            simulator: MMSimulator::new(sim_config),
            last_quotes: None,
            last_best_bid: dec!(0),
            last_best_ask: dec!(0),
        }
    }

    /// Create from a MarketMakerEngine (backwards compatibility)
    pub fn from_mm_engine(mm: MarketMakerEngine, sim_config: SimulatorConfig) -> Self {
        use crate::algorithms::AvellanedaStoikovAlgorithm;
        let config = mm.config().clone();
        let algorithm = Box::new(AvellanedaStoikovAlgorithm::new(config));
        Self::new(algorithm, sim_config)
    }

    /// Get the algorithm type
    pub fn algorithm_type(&self) -> AlgorithmType {
        self.algorithm.algorithm_type()
    }

    /// Get the algorithm name
    pub fn algorithm_name(&self) -> &'static str {
        self.algorithm.name()
    }

    /// Process a feature snapshot and compute new quotes
    pub fn on_features(
        &mut self,
        _microprice: Decimal,
        mid_price: Decimal,
        volatility: f64,
        entropy_score: f64,
        flow_imbalance: f64,
        timestamp_ms: u64,
    ) -> MMQuotes {
        // Calculate best bid/ask from mid_price (approximation when not provided)
        // In practice, these should be the actual order book best prices
        let half_spread = mid_price * dec!(0.00005); // Assume 0.5 bps market spread
        let best_bid = mid_price - half_spread;
        let best_ask = mid_price + half_spread;

        self.on_features_with_book(
            best_bid,
            best_ask,
            volatility,
            entropy_score,
            flow_imbalance,
            timestamp_ms,
        )
    }

    /// Process a feature snapshot with actual order book prices
    pub fn on_features_with_book(
        &mut self,
        best_bid: Decimal,
        best_ask: Decimal,
        volatility: f64,
        entropy_score: f64,
        book_imbalance: f64,
        timestamp_ms: u64,
    ) -> MMQuotes {
        // Store for later use
        self.last_best_bid = best_bid;
        self.last_best_ask = best_ask;

        // Create MarketInput for the algorithm
        let input = MarketInput {
            best_bid,
            best_ask,
            volatility,
            entropy: entropy_score,
            book_imbalance,
            timestamp_ms,
        };

        let quotes = self.algorithm.compute_quotes(&input);

        self.simulator.update_quotes(&quotes);
        self.last_quotes = Some(quotes.clone());

        // Update mark-to-market
        let mid_price = input.mid_price();
        self.algorithm.update_mark_to_market(mid_price);

        quotes
    }

    /// Process an incoming trade for potential fills
    pub fn on_trade(&mut self, trade: &Trade, current_time_ms: u64) -> Vec<Fill> {
        let fills = self.simulator.process_trade(trade, current_time_ms);

        // Process fills in algorithm
        for fill in &fills {
            self.algorithm.process_fill(fill.clone(), self.simulator.config.fee_rate);
        }

        fills
    }

    /// Get current state
    pub fn state(&self) -> PaperTradingState {
        PaperTradingState {
            mm_state: self.algorithm.get_state(),
            sim_stats: self.simulator.stats().clone(),
            last_quotes: self.last_quotes.clone(),
            algorithm_type: self.algorithm.algorithm_type(),
        }
    }

    /// Reset everything
    pub fn reset(&mut self) {
        self.algorithm.reset();
        self.simulator.reset();
        self.last_quotes = None;
    }

    /// Get algorithm parameters as JSON
    pub fn parameters_json(&self) -> serde_json::Value {
        self.algorithm.parameters_json()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperTradingState {
    pub mm_state: crate::market_maker::MMState,
    pub sim_stats: SimulatorStats,
    pub last_quotes: Option<MMQuotes>,
    #[serde(default)]
    pub algorithm_type: AlgorithmType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_maker::MMConfig;

    #[test]
    fn test_simulator_basic() {
        let config = SimulatorConfig::default();
        let mut sim = MMSimulator::new(config);

        // Set up a bid quote
        let quotes = MMQuotes {
            bid: Some(Quote {
                price: dec!(50000),
                size: dec!(0.01),
                side: QuoteSide::Bid,
                timestamp_ms: 0,
            }),
            ask: Some(Quote {
                price: dec!(50010),
                size: dec!(0.01),
                side: QuoteSide::Ask,
                timestamp_ms: 0,
            }),
            regime: crate::market_maker::MarketRegime::HighEntropy,
            fair_value: dec!(50005),
            half_spread: dec!(5),
            skew: dec!(0),
        };

        sim.update_quotes(&quotes);

        // Simulate an aggressive sell that hits our bid
        let trade = Trade {
            id: 1,
            price: dec!(50000),
            quantity: dec!(0.1), // Large enough to fill us
            timestamp: 200,
            is_buyer_maker: true, // Seller aggressed
        };

        let fills = sim.process_trade(&trade, 200);

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].side, QuoteSide::Bid);
        assert_eq!(fills[0].price, dec!(50000));
    }

    #[test]
    fn test_paper_trading_engine() {
        let mm = MarketMakerEngine::new(MMConfig::default());
        let sim_config = SimulatorConfig::default();
        let mut engine = PaperTradingEngine::new(mm, sim_config);

        // Generate quotes
        let quotes = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            0,
        );

        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());

        // Simulate a trade
        let trade = Trade {
            id: 1,
            price: quotes.bid.as_ref().unwrap().price,
            quantity: dec!(1.0),
            timestamp: 200,
            is_buyer_maker: true,
        };

        let fills = engine.on_trade(&trade, 200);

        // Should have a fill
        assert!(!fills.is_empty());

        // Check inventory updated
        let state = engine.state();
        assert!(state.mm_state.inventory > dec!(0));
    }
}

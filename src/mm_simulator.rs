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
use crate::risk_manager::{RiskManager, RiskConfig, RiskAction, RiskState, HaltReason};
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

/// Extended state including risk management information
#[derive(Debug, Clone, Serialize)]
pub struct RiskManagedState {
    /// Base trading state
    pub trading_state: PaperTradingState,
    /// Current risk action being enforced
    pub risk_action: RiskAction,
    /// Current risk state
    pub risk_state: RiskState,
    /// Risk statistics
    pub risk_stats: crate::risk_manager::RiskStats,
    /// Number of quotes blocked by risk manager
    pub quotes_blocked: u64,
    /// Number of fills blocked (would-be fills that were prevented)
    pub fills_blocked: u64,
}

/// Paper trading engine with integrated risk management
///
/// This wraps a `GenericPaperTradingEngine` and applies risk controls:
/// - Pre-quote checks: May block or modify quotes based on risk limits
/// - Post-fill updates: Tracks PnL for drawdown and daily loss limits
/// - State transitions: Normal -> ReduceOnly -> Halt -> Emergency
///
/// # Example
///
/// ```ignore
/// let algorithm = Box::new(AvellanedaStoikovAlgorithm::new(config));
/// let sim_config = SimulatorConfig::default();
/// let risk_config = RiskConfig::default();
///
/// let mut engine = RiskManagedPaperTradingEngine::new(algorithm, sim_config, risk_config);
///
/// // Process market data - risk checks are automatic
/// let quotes = engine.on_features_with_book(best_bid, best_ask, vol, entropy, imbalance, ts);
///
/// // Check if we're still allowed to trade
/// if engine.is_trading_allowed() {
///     // Process trades
///     let fills = engine.on_trade(&trade, timestamp);
/// }
/// ```
pub struct RiskManagedPaperTradingEngine {
    /// Underlying paper trading engine
    inner: GenericPaperTradingEngine,
    /// Risk manager
    risk_manager: RiskManager,
    /// Last risk action applied
    last_risk_action: RiskAction,
    /// Current volatility for risk checks
    current_volatility: f64,
    /// Statistics
    quotes_blocked: u64,
    fills_blocked: u64,
}

impl RiskManagedPaperTradingEngine {
    /// Create a new risk-managed paper trading engine
    pub fn new(
        algorithm: Box<dyn MarketMakingAlgorithm>,
        sim_config: SimulatorConfig,
        risk_config: RiskConfig,
    ) -> Self {
        Self {
            inner: GenericPaperTradingEngine::new(algorithm, sim_config),
            risk_manager: RiskManager::new(risk_config),
            last_risk_action: RiskAction::Allow,
            current_volatility: 0.0,
            quotes_blocked: 0,
            fills_blocked: 0,
        }
    }

    /// Create with default risk configuration
    pub fn with_default_risk(
        algorithm: Box<dyn MarketMakingAlgorithm>,
        sim_config: SimulatorConfig,
    ) -> Self {
        Self::new(algorithm, sim_config, RiskConfig::default())
    }

    /// Create with conservative risk configuration
    pub fn with_conservative_risk(
        algorithm: Box<dyn MarketMakingAlgorithm>,
        sim_config: SimulatorConfig,
    ) -> Self {
        Self::new(algorithm, sim_config, RiskConfig::conservative())
    }

    /// Get the algorithm type
    pub fn algorithm_type(&self) -> AlgorithmType {
        self.inner.algorithm_type()
    }

    /// Get the algorithm name
    pub fn algorithm_name(&self) -> &'static str {
        self.inner.algorithm_name()
    }

    /// Check if trading is currently allowed
    pub fn is_trading_allowed(&self) -> bool {
        self.last_risk_action.allows_quoting()
    }

    /// Check if new positions are allowed
    pub fn allows_new_position(&self) -> bool {
        self.last_risk_action.allows_new_position()
    }

    /// Get current risk state
    pub fn risk_state(&self) -> &RiskState {
        self.risk_manager.state()
    }

    /// Get current risk action
    pub fn risk_action(&self) -> &RiskAction {
        &self.last_risk_action
    }

    /// Get risk statistics
    pub fn risk_stats(&self) -> &crate::risk_manager::RiskStats {
        self.risk_manager.stats()
    }

    /// Manually trigger a halt
    pub fn manual_halt(&mut self, current_time_ms: u64) {
        self.risk_manager.manual_halt(current_time_ms);
        self.last_risk_action = RiskAction::Halt { reason: HaltReason::ManualHalt };
    }

    /// Manually reset from halt state
    pub fn manual_reset(&mut self, current_time_ms: u64) {
        self.risk_manager.reset(current_time_ms);
        self.last_risk_action = RiskAction::Allow;
    }

    /// Process a feature snapshot and compute new quotes with risk checks
    pub fn on_features(
        &mut self,
        _microprice: Decimal,
        mid_price: Decimal,
        volatility: f64,
        entropy_score: f64,
        flow_imbalance: f64,
        timestamp_ms: u64,
    ) -> MMQuotes {
        let half_spread = mid_price * dec!(0.00005);
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

    /// Process a feature snapshot with actual order book prices and risk checks
    pub fn on_features_with_book(
        &mut self,
        best_bid: Decimal,
        best_ask: Decimal,
        volatility: f64,
        entropy_score: f64,
        book_imbalance: f64,
        timestamp_ms: u64,
    ) -> MMQuotes {
        // Store current volatility for risk checks
        self.current_volatility = volatility;

        // Get current MM state for risk check
        let mm_state = self.inner.algorithm.get_state();

        // Perform pre-quote risk check
        self.last_risk_action = self.risk_manager.check_pre_quote(
            &mm_state,
            timestamp_ms,
            volatility,
        );

        // Record quote attempt
        self.risk_manager.on_quote(timestamp_ms);

        match &self.last_risk_action {
            RiskAction::Allow => {
                // Normal operation - compute quotes as usual
                self.inner.on_features_with_book(
                    best_bid,
                    best_ask,
                    volatility,
                    entropy_score,
                    book_imbalance,
                    timestamp_ms,
                )
            }
            RiskAction::ReduceOnly => {
                // Compute quotes but filter to only reduce position
                let quotes = self.inner.on_features_with_book(
                    best_bid,
                    best_ask,
                    volatility,
                    entropy_score,
                    book_imbalance,
                    timestamp_ms,
                );

                // Filter quotes based on current inventory
                self.filter_quotes_for_reduce_only(quotes, mm_state.inventory)
            }
            RiskAction::Halt { .. } | RiskAction::Emergency { .. } => {
                // Return empty quotes - no trading
                self.quotes_blocked += 1;
                self.create_empty_quotes(best_bid, best_ask, entropy_score)
            }
        }
    }

    /// Process an incoming trade for potential fills with risk updates
    pub fn on_trade(&mut self, trade: &Trade, current_time_ms: u64) -> Vec<Fill> {
        // Check if trading is halted
        if self.last_risk_action.is_stopped() {
            self.fills_blocked += 1;
            return Vec::new();
        }

        // Process trade through inner engine
        let fills = self.inner.simulator.process_trade(trade, current_time_ms);

        // If in ReduceOnly mode, filter fills
        let fills = if matches!(self.last_risk_action, RiskAction::ReduceOnly) {
            let inventory = self.inner.algorithm.get_state().inventory;
            self.filter_fills_for_reduce_only(fills, inventory)
        } else {
            fills
        };

        // Process each fill through the algorithm and risk manager
        for fill in &fills {
            // Get state before fill
            let state_before = self.inner.algorithm.get_state();

            // Process fill in algorithm
            self.inner.algorithm.process_fill(fill.clone(), self.inner.simulator.config.fee_rate);

            // Get state after fill
            let state_after = self.inner.algorithm.get_state();

            // Calculate trade PnL (change in realized PnL)
            let trade_pnl = state_after.pnl.realized_pnl - state_before.pnl.realized_pnl;

            // Update risk manager with fill
            let risk_action = self.risk_manager.on_fill(
                fill,
                &state_after,
                Some(trade_pnl),
                current_time_ms,
            );

            // Update last risk action if it changed to something more restrictive
            if risk_action.is_stopped() ||
               (matches!(risk_action, RiskAction::ReduceOnly) &&
                matches!(self.last_risk_action, RiskAction::Allow)) {
                self.last_risk_action = risk_action;
            }
        }

        fills
    }

    /// Get current state including risk information
    pub fn state(&self) -> RiskManagedState {
        RiskManagedState {
            trading_state: self.inner.state(),
            risk_action: self.last_risk_action.clone(),
            risk_state: self.risk_manager.state().clone(),
            risk_stats: self.risk_manager.stats().clone(),
            quotes_blocked: self.quotes_blocked,
            fills_blocked: self.fills_blocked,
        }
    }

    /// Get base trading state (without risk info)
    pub fn trading_state(&self) -> PaperTradingState {
        self.inner.state()
    }

    /// Reset everything including risk manager
    pub fn reset(&mut self) {
        self.inner.reset();
        self.risk_manager = RiskManager::new(self.risk_manager.config().clone());
        self.last_risk_action = RiskAction::Allow;
        self.quotes_blocked = 0;
        self.fills_blocked = 0;
    }

    /// Get algorithm parameters as JSON
    pub fn parameters_json(&self) -> serde_json::Value {
        self.inner.parameters_json()
    }

    /// Get reference to inner engine
    pub fn inner(&self) -> &GenericPaperTradingEngine {
        &self.inner
    }

    /// Get mutable reference to inner engine (use carefully)
    pub fn inner_mut(&mut self) -> &mut GenericPaperTradingEngine {
        &mut self.inner
    }

    /// Get reference to risk manager
    pub fn risk_manager(&self) -> &RiskManager {
        &self.risk_manager
    }

    // === Helper methods ===

    /// Filter quotes to only allow reducing position
    fn filter_quotes_for_reduce_only(&self, mut quotes: MMQuotes, inventory: Decimal) -> MMQuotes {
        if inventory > dec!(0) {
            // Long position - only allow asks (sells)
            quotes.bid = None;
        } else if inventory < dec!(0) {
            // Short position - only allow bids (buys)
            quotes.ask = None;
        }
        // If flat, no quotes allowed in reduce-only mode
        if inventory == dec!(0) {
            quotes.bid = None;
            quotes.ask = None;
        }
        quotes
    }

    /// Filter fills to only allow reducing position
    fn filter_fills_for_reduce_only(&self, fills: Vec<Fill>, inventory: Decimal) -> Vec<Fill> {
        fills.into_iter().filter(|fill| {
            match fill.side {
                QuoteSide::Bid => inventory < dec!(0), // Buy only if short
                QuoteSide::Ask => inventory > dec!(0), // Sell only if long
            }
        }).collect()
    }

    /// Create empty quotes (used when halted)
    fn create_empty_quotes(&self, best_bid: Decimal, best_ask: Decimal, entropy: f64) -> MMQuotes {
        use crate::market_maker::{MarketRegime, RegimeThresholds};
        let mid = (best_bid + best_ask) / dec!(2);
        MMQuotes {
            bid: None,
            ask: None,
            regime: MarketRegime::from_entropy_score(entropy, &RegimeThresholds::default()),
            fair_value: mid,
            half_spread: (best_ask - best_bid) / dec!(2),
            skew: dec!(0),
        }
    }
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

    // === Risk Managed Paper Trading Engine Tests ===

    fn create_risk_managed_engine(risk_config: RiskConfig) -> RiskManagedPaperTradingEngine {
        use crate::algorithms::AvellanedaStoikovAlgorithm;
        let mm_config = MMConfig::default();
        let algorithm = Box::new(AvellanedaStoikovAlgorithm::new(mm_config));
        let sim_config = SimulatorConfig::default();
        RiskManagedPaperTradingEngine::new(algorithm, sim_config, risk_config)
    }

    #[test]
    fn test_risk_managed_engine_creation() {
        let engine = create_risk_managed_engine(RiskConfig::default());

        assert!(engine.is_trading_allowed());
        assert!(engine.allows_new_position());
        assert_eq!(*engine.risk_state(), RiskState::Normal);
    }

    #[test]
    fn test_risk_managed_engine_normal_operation() {
        let mut engine = create_risk_managed_engine(RiskConfig::default());

        // Generate quotes - should work normally
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
        assert!(engine.is_trading_allowed());
    }

    #[test]
    fn test_risk_managed_engine_halts_on_max_inventory() {
        // Create config with very low max inventory
        let mut risk_config = RiskConfig::default();
        risk_config.max_inventory = dec!(0.0005); // Extremely small limit
        risk_config.soft_inventory_limit = dec!(0.0003);

        let mut engine = create_risk_managed_engine(risk_config);

        // Generate initial quotes
        let quotes = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            0,
        );

        assert!(quotes.bid.is_some());

        // The fill size is limited to quote.size from MMConfig::default()
        // After fill, the inventory will be the quote size (0.001 by default)
        // which exceeds our tiny max_inventory of 0.0005
        let trade = Trade {
            id: 1,
            price: quotes.bid.as_ref().unwrap().price,
            quantity: dec!(1.0), // Large trade
            timestamp: 200,
            is_buyer_maker: true,
        };

        let fills = engine.on_trade(&trade, 200);
        assert!(!fills.is_empty());

        // Now generate new quotes - inventory should exceed limit
        let _quotes2 = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            300,
        );

        // After filling a quote with size 0.001, we exceed max_inventory of 0.0005
        // Should be halted due to max inventory or in reduce-only mode
        let action = engine.risk_action();
        assert!(action.is_stopped() ||
                matches!(action, RiskAction::ReduceOnly),
                "Expected halt or reduce-only, got {:?}", action);
    }

    #[test]
    fn test_risk_managed_engine_reduce_only_mode() {
        // Create config with very low soft limit
        let mut risk_config = RiskConfig::default();
        risk_config.max_inventory = dec!(0.1);
        risk_config.soft_inventory_limit = dec!(0.0001); // Very low soft limit

        let mut engine = create_risk_managed_engine(risk_config);

        // Generate initial quotes
        let quotes = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            0,
        );

        // Simulate a fill that exceeds soft limit
        let trade = Trade {
            id: 1,
            price: quotes.bid.as_ref().unwrap().price,
            quantity: dec!(1.0),
            timestamp: 200,
            is_buyer_maker: true,
        };

        engine.on_trade(&trade, 200);

        // Generate new quotes - should be in reduce-only mode
        let quotes2 = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            300,
        );

        // With positive inventory, in reduce-only mode:
        // - Should have no bid (would increase long position)
        // - May have ask (reduces long position)
        if matches!(engine.risk_action(), RiskAction::ReduceOnly) {
            assert!(quotes2.bid.is_none(), "Should not have bid in reduce-only with long position");
        }
    }

    #[test]
    fn test_risk_managed_engine_halts_on_high_volatility() {
        let mut risk_config = RiskConfig::default();
        risk_config.max_volatility = 0.01; // 1% max volatility

        let mut engine = create_risk_managed_engine(risk_config);

        // Generate quotes with very high volatility
        let quotes = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.10, // 10% volatility - way above threshold
            0.8,
            0.0,
            0,
        );

        // Should be halted due to high volatility
        assert!(engine.risk_action().is_stopped());
        assert!(quotes.bid.is_none());
        assert!(quotes.ask.is_none());
    }

    #[test]
    fn test_risk_managed_engine_manual_halt_and_reset() {
        let mut engine = create_risk_managed_engine(RiskConfig::default());

        // Manually halt
        engine.manual_halt(1000);
        assert!(!engine.is_trading_allowed());
        assert!(engine.risk_action().is_stopped());

        // Generate quotes while halted - should be empty
        let quotes = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            1100,
        );

        assert!(quotes.bid.is_none());
        assert!(quotes.ask.is_none());
        assert!(engine.state().quotes_blocked > 0);

        // Manual reset
        engine.manual_reset(2000);
        assert!(engine.is_trading_allowed());

        // Should be able to quote again
        let quotes2 = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            2100,
        );

        assert!(quotes2.bid.is_some());
        assert!(quotes2.ask.is_some());
    }

    #[test]
    fn test_risk_managed_engine_blocks_fills_when_halted() {
        let mut engine = create_risk_managed_engine(RiskConfig::default());

        // Generate initial quotes
        let _quotes = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            0,
        );

        // Manually halt
        engine.manual_halt(100);

        // Try to process a trade while halted
        let trade = Trade {
            id: 1,
            price: dec!(50000),
            quantity: dec!(1.0),
            timestamp: 200,
            is_buyer_maker: true,
        };

        let fills = engine.on_trade(&trade, 200);

        // Should be blocked
        assert!(fills.is_empty());
        assert!(engine.state().fills_blocked > 0);
    }

    #[test]
    fn test_risk_managed_engine_state_serialization() {
        let engine = create_risk_managed_engine(RiskConfig::default());
        let state = engine.state();

        // Should be serializable
        let json = serde_json::to_string(&state).unwrap();
        assert!(!json.is_empty());
        assert!(json.contains("trading_state"));
        assert!(json.contains("risk_action"));
        assert!(json.contains("risk_state"));
    }

    #[test]
    fn test_risk_managed_engine_reset() {
        let mut engine = create_risk_managed_engine(RiskConfig::default());

        // Do some trading
        let quotes = engine.on_features(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            0,
        );

        let trade = Trade {
            id: 1,
            price: quotes.bid.as_ref().unwrap().price,
            quantity: dec!(1.0),
            timestamp: 200,
            is_buyer_maker: true,
        };
        engine.on_trade(&trade, 200);

        // Manual halt
        engine.manual_halt(300);

        // Reset everything
        engine.reset();

        // Should be back to initial state
        assert!(engine.is_trading_allowed());
        assert_eq!(*engine.risk_state(), RiskState::Normal);
        assert_eq!(engine.state().quotes_blocked, 0);
        assert_eq!(engine.state().fills_blocked, 0);
    }

    #[test]
    fn test_risk_managed_engine_with_conservative_config() {
        let engine = RiskManagedPaperTradingEngine::with_conservative_risk(
            Box::new(crate::algorithms::AvellanedaStoikovAlgorithm::new(MMConfig::default())),
            SimulatorConfig::default(),
        );

        // Verify conservative settings
        let config = engine.risk_manager().config();
        assert!(config.max_inventory < dec!(0.1));
        assert!(config.max_drawdown < 0.10);
    }

    #[test]
    fn test_risk_managed_engine_tracks_stats() {
        let mut engine = create_risk_managed_engine(RiskConfig::default());

        // Generate some quotes
        for i in 0..5 {
            engine.on_features(
                dec!(50000),
                dec!(50000),
                0.001,
                0.8,
                0.0,
                i * 1000,
            );
        }

        // Manual halt
        engine.manual_halt(6000);

        // Try to quote while halted
        for i in 0..3 {
            engine.on_features(
                dec!(50000),
                dec!(50000),
                0.001,
                0.8,
                0.0,
                (7 + i) * 1000,
            );
        }

        let state = engine.state();
        assert_eq!(state.quotes_blocked, 3);
    }

    #[test]
    fn test_filter_quotes_for_reduce_only_long_position() {
        let engine = create_risk_managed_engine(RiskConfig::default());

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

        // Long position - should filter out bid
        let filtered = engine.filter_quotes_for_reduce_only(quotes.clone(), dec!(0.05));
        assert!(filtered.bid.is_none());
        assert!(filtered.ask.is_some());

        // Short position - should filter out ask
        let filtered = engine.filter_quotes_for_reduce_only(quotes.clone(), dec!(-0.05));
        assert!(filtered.bid.is_some());
        assert!(filtered.ask.is_none());

        // Flat position - should filter both
        let filtered = engine.filter_quotes_for_reduce_only(quotes, dec!(0));
        assert!(filtered.bid.is_none());
        assert!(filtered.ask.is_none());
    }
}

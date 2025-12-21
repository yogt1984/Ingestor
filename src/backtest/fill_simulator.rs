//! Realistic Fill Simulation for Backtesting
//!
//! Implements fill probability models based on:
//! - Cont, Kukanov, Stoikov (2014): "The Price Impact of Order Book Events"
//! - Moallemi & Yuan (2017): "The Value of Queue Position"
//!
//! Key principles:
//! 1. Price touching your level ≠ fill (queue position matters)
//! 2. Adverse selection: fills tend to be followed by unfavorable price moves
//! 3. Trade intensity affects fill probability
//! 4. Partial fills are common

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use crate::features::feature_fusion::FeaturesSnapshot;
use crate::execution::market_maker::{Quote, QuoteSide, Fill, MMQuotes};

/// Configuration for the fill simulator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillSimulatorConfig {
    /// Base fill probability when price touches our level (0.0 - 1.0)
    /// Academic research suggests this is typically 0.05-0.20 for passive orders
    pub base_fill_probability: f64,

    /// Queue position as fraction (0.0 = front, 1.0 = back)
    /// Affects fill probability: front of queue = higher prob
    pub queue_position: f64,

    /// Minimum spread in bps below which we assume high competition
    pub competitive_spread_bps: f64,

    /// Adverse selection factor: how much worse fills are than mid-price moves
    /// Higher = more adverse selection (fills happen when price moves against us)
    pub adverse_selection_factor: f64,

    /// Whether to allow partial fills
    pub allow_partial_fills: bool,

    /// Minimum fill size as fraction of quote size
    pub min_fill_fraction: f64,

    /// Latency in milliseconds (quote must be active for this long)
    pub quote_latency_ms: u64,

    /// Whether to use trade intensity from features
    pub use_trade_intensity: bool,

    /// Whether to adjust for market regime
    pub regime_aware: bool,

    /// Fee rate for slippage calculation
    pub fee_rate: Decimal,
}

impl Default for FillSimulatorConfig {
    fn default() -> Self {
        Self {
            base_fill_probability: 0.10, // 10% base fill rate when price touches
            queue_position: 0.5,          // Middle of queue
            competitive_spread_bps: 2.0,  // Below 2bps = competitive
            adverse_selection_factor: 0.3, // 30% adverse selection
            allow_partial_fills: true,
            min_fill_fraction: 0.1,       // Min 10% of quote filled
            quote_latency_ms: 50,         // 50ms latency
            use_trade_intensity: true,
            regime_aware: true,
            fee_rate: dec!(0.0001),       // 1 bps
        }
    }
}

/// Represents a potential fill event with probability
#[derive(Debug, Clone)]
pub struct FillEvent {
    pub fill: Fill,
    pub probability: f64,
    pub adverse_selection_cost: Decimal,
    pub is_partial: bool,
}

/// Market state for fill probability calculation
#[derive(Debug, Clone, Default)]
pub struct MarketState {
    /// Current mid price
    pub mid_price: Decimal,
    /// Previous mid price
    pub prev_mid_price: Decimal,
    /// Current spread in bps
    pub spread_bps: f64,
    /// Trade intensity (trades per second)
    pub trade_rate: f64,
    /// Aggressor ratio (fraction of aggressive buys)
    pub aggr_buy_ratio: f64,
    /// Book imbalance (-1 to 1, positive = more bids)
    pub book_imbalance: f64,
    /// Current entropy score (0 = directional, 1 = random)
    pub entropy_score: f64,
    /// Volatility
    pub volatility: f64,
    /// Bid depth at top level
    pub bid_depth: Decimal,
    /// Ask depth at top level
    pub ask_depth: Decimal,
}

impl MarketState {
    /// Extract market state from a FeaturesSnapshot
    pub fn from_snapshot(snap: &FeaturesSnapshot, prev_mid: Decimal) -> Self {
        let mid_price = snap.mid_price.unwrap_or_default();
        let spread = snap.spread.unwrap_or_default();
        let spread_bps = if mid_price > dec!(0) {
            (spread / mid_price * dec!(10000)).to_f64().unwrap_or(0.0)
        } else {
            0.0
        };

        Self {
            mid_price,
            prev_mid_price: prev_mid,
            spread_bps,
            trade_rate: snap.trade_rate_10s.unwrap_or(0.0),
            aggr_buy_ratio: snap.aggr_ratio_100
                .map(|d| d.to_f64().unwrap_or(0.5))
                .unwrap_or(0.5),
            book_imbalance: snap.imbalance
                .map(|d| d.to_f64().unwrap_or(0.0))
                .unwrap_or(0.0),
            entropy_score: snap.tick_entropy_5s
                .map(|d| d.to_f64().unwrap_or(0.5))
                .unwrap_or(0.5),
            volatility: snap.realized_volatility_100.unwrap_or(0.001),
            bid_depth: snap.bid_volume_001.unwrap_or_default(),
            ask_depth: snap.ask_volume_001.unwrap_or_default(),
        }
    }

    /// Price moved down (favorable for bids)
    pub fn price_moved_down(&self) -> bool {
        self.mid_price < self.prev_mid_price
    }

    /// Price moved up (favorable for asks)
    pub fn price_moved_up(&self) -> bool {
        self.mid_price > self.prev_mid_price
    }

    /// Magnitude of price move in bps
    pub fn price_move_bps(&self) -> f64 {
        if self.prev_mid_price == dec!(0) {
            return 0.0;
        }
        let move_pct = (self.mid_price - self.prev_mid_price) / self.prev_mid_price;
        (move_pct * dec!(10000)).to_f64().unwrap_or(0.0).abs()
    }
}

/// The main fill simulator
pub struct FillSimulator {
    config: FillSimulatorConfig,

    /// Track quote state
    active_bid: Option<(Quote, u64)>, // (quote, timestamp_placed)
    active_ask: Option<(Quote, u64)>,

    /// Running statistics
    pub stats: FillSimulatorStats,

    /// Random seed for deterministic simulation
    seed: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FillSimulatorStats {
    pub events_processed: u64,
    pub bid_touches: u64,
    pub ask_touches: u64,
    pub bid_fills: u64,
    pub ask_fills: u64,
    pub bid_partial_fills: u64,
    pub ask_partial_fills: u64,
    pub total_fill_volume: Decimal,
    pub total_adverse_selection_cost: Decimal,
    pub cumulative_fill_probability: f64,
}

impl FillSimulator {
    pub fn new(config: FillSimulatorConfig) -> Self {
        Self {
            config,
            active_bid: None,
            active_ask: None,
            stats: FillSimulatorStats::default(),
            seed: 42,
        }
    }

    /// Update quotes from MM engine
    pub fn update_quotes(&mut self, quotes: &MMQuotes, timestamp_ms: u64) {
        if let Some(ref bid) = quotes.bid {
            // Only update if quote changed
            let should_update = self.active_bid
                .as_ref()
                .map(|(q, _)| q.price != bid.price || q.size != bid.size)
                .unwrap_or(true);

            if should_update {
                self.active_bid = Some((bid.clone(), timestamp_ms));
            }
        } else {
            self.active_bid = None;
        }

        if let Some(ref ask) = quotes.ask {
            let should_update = self.active_ask
                .as_ref()
                .map(|(q, _)| q.price != ask.price || q.size != ask.size)
                .unwrap_or(true);

            if should_update {
                self.active_ask = Some((ask.clone(), timestamp_ms));
            }
        } else {
            self.active_ask = None;
        }
    }

    /// Simulate fills based on market state transition
    ///
    /// This is the core logic. We check:
    /// 1. Did price touch/cross our quote level?
    /// 2. If so, calculate fill probability based on multiple factors
    /// 3. Determine if fill occurs (deterministic based on seed for reproducibility)
    /// 4. Calculate adverse selection cost
    pub fn simulate_fills(
        &mut self,
        market: &MarketState,
        timestamp_ms: u64,
    ) -> Vec<FillEvent> {
        self.stats.events_processed += 1;
        let mut events = Vec::new();

        // Check bid fill opportunity
        if let Some((ref bid, placed_at)) = self.active_bid.clone() {
            // Quote must be active for minimum latency
            if timestamp_ms >= placed_at + self.config.quote_latency_ms {
                // Price touched or crossed our bid?
                if market.mid_price <= bid.price {
                    self.stats.bid_touches += 1;

                    if let Some(event) = self.evaluate_bid_fill(
                        &bid,
                        market,
                        timestamp_ms,
                    ) {
                        events.push(event);
                    }
                }
            }
        }

        // Check ask fill opportunity
        if let Some((ref ask, placed_at)) = self.active_ask.clone() {
            if timestamp_ms >= placed_at + self.config.quote_latency_ms {
                if market.mid_price >= ask.price {
                    self.stats.ask_touches += 1;

                    if let Some(event) = self.evaluate_ask_fill(
                        &ask,
                        market,
                        timestamp_ms,
                    ) {
                        events.push(event);
                    }
                }
            }
        }

        events
    }

    /// Evaluate bid fill probability and generate event
    fn evaluate_bid_fill(
        &mut self,
        bid: &Quote,
        market: &MarketState,
        timestamp_ms: u64,
    ) -> Option<FillEvent> {
        let prob = self.calculate_fill_probability(
            bid,
            market,
            true, // is_bid
        );

        // Deterministic "random" based on seed and timestamp
        let rand_val = self.pseudo_random(timestamp_ms);

        if rand_val < prob {
            // Fill occurs!
            let (fill_size, is_partial) = self.determine_fill_size(bid, market, true);

            // Calculate adverse selection cost
            // When we get filled on bid, price often continues down
            let adverse_cost = self.calculate_adverse_selection(bid.price, market, true);

            self.stats.bid_fills += 1;
            if is_partial {
                self.stats.bid_partial_fills += 1;
            }
            self.stats.total_fill_volume += fill_size;
            self.stats.total_adverse_selection_cost += adverse_cost;
            self.stats.cumulative_fill_probability += prob;

            Some(FillEvent {
                fill: Fill {
                    side: QuoteSide::Bid,
                    price: bid.price,
                    size: fill_size,
                    timestamp_ms,
                },
                probability: prob,
                adverse_selection_cost: adverse_cost,
                is_partial,
            })
        } else {
            None
        }
    }

    /// Evaluate ask fill probability and generate event
    fn evaluate_ask_fill(
        &mut self,
        ask: &Quote,
        market: &MarketState,
        timestamp_ms: u64,
    ) -> Option<FillEvent> {
        let prob = self.calculate_fill_probability(
            ask,
            market,
            false, // is_ask
        );

        let rand_val = self.pseudo_random(timestamp_ms);

        if rand_val < prob {
            let (fill_size, is_partial) = self.determine_fill_size(ask, market, false);
            let adverse_cost = self.calculate_adverse_selection(ask.price, market, false);

            self.stats.ask_fills += 1;
            if is_partial {
                self.stats.ask_partial_fills += 1;
            }
            self.stats.total_fill_volume += fill_size;
            self.stats.total_adverse_selection_cost += adverse_cost;
            self.stats.cumulative_fill_probability += prob;

            Some(FillEvent {
                fill: Fill {
                    side: QuoteSide::Ask,
                    price: ask.price,
                    size: fill_size,
                    timestamp_ms,
                },
                probability: prob,
                adverse_selection_cost: adverse_cost,
                is_partial,
            })
        } else {
            None
        }
    }

    /// Calculate fill probability based on multiple factors
    ///
    /// Factors considered:
    /// 1. Base probability (from config)
    /// 2. Queue position adjustment
    /// 3. Trade intensity adjustment
    /// 4. Spread competitiveness
    /// 5. Book imbalance (flow toxicity)
    /// 6. Regime adjustment
    fn calculate_fill_probability(
        &self,
        _quote: &Quote,
        market: &MarketState,
        is_bid: bool,
    ) -> f64 {
        let mut prob = self.config.base_fill_probability;

        // 1. Queue position adjustment
        // Front of queue (0.0) = 2x base probability
        // Back of queue (1.0) = 0.2x base probability
        let queue_mult = 2.0 - 1.8 * self.config.queue_position;
        prob *= queue_mult;

        // 2. Trade intensity adjustment
        // Higher trade rate = higher fill probability
        if self.config.use_trade_intensity && market.trade_rate > 0.0 {
            // Normalize: assume 10 trades/sec is "normal"
            let intensity_mult = (market.trade_rate / 10.0).min(2.0).max(0.5);
            prob *= intensity_mult;
        }

        // 3. Spread competitiveness
        // Tighter spreads = more competition = lower individual fill prob
        if market.spread_bps > 0.0 && market.spread_bps < self.config.competitive_spread_bps {
            let competition_factor = market.spread_bps / self.config.competitive_spread_bps;
            prob *= competition_factor;
        }

        // 4. Book imbalance (flow toxicity)
        // If we're bidding and there are more asks (negative imbalance),
        // aggressive selling is likely = higher bid fill prob
        if is_bid {
            // Negative imbalance = more aggressive selling = higher bid fill prob
            let imbalance_adj = 1.0 - market.book_imbalance * 0.3;
            prob *= imbalance_adj.max(0.5).min(1.5);
        } else {
            // Positive imbalance = more aggressive buying = higher ask fill prob
            let imbalance_adj = 1.0 + market.book_imbalance * 0.3;
            prob *= imbalance_adj.max(0.5).min(1.5);
        }

        // 5. Regime adjustment
        // Low entropy (directional) = price more likely to sweep through levels
        // High entropy (random) = price bounces, less likely to fill
        if self.config.regime_aware {
            // Low entropy = higher fill probability (directional moves)
            let regime_mult = 1.5 - market.entropy_score;
            prob *= regime_mult.max(0.5).min(1.5);
        }

        // 6. Price movement in our direction
        // If price is moving toward our quote, fill is more likely
        if is_bid && market.price_moved_down() {
            prob *= 1.3;
        } else if !is_bid && market.price_moved_up() {
            prob *= 1.3;
        }

        // Clamp to valid probability
        prob.max(0.01).min(0.95)
    }

    /// Determine fill size (full or partial)
    fn determine_fill_size(
        &mut self,
        quote: &Quote,
        _market: &MarketState,
        _is_bid: bool,
    ) -> (Decimal, bool) {
        #![allow(unused_variables)]
        if !self.config.allow_partial_fills {
            return (quote.size, false);
        }

        // Partial fill more likely when:
        // 1. Large quote size relative to market
        // 2. Low trade intensity
        // 3. Back of queue

        let rand_val = self.pseudo_random(quote.timestamp_ms + 1);

        // 70% chance of full fill, 30% chance of partial
        if rand_val > 0.30 {
            return (quote.size, false);
        }

        // Partial fill: random fraction between min_fill_fraction and 1.0
        let fill_fraction = self.config.min_fill_fraction
            + rand_val / 0.30 * (1.0 - self.config.min_fill_fraction);

        let fill_size = quote.size * Decimal::from_f64(fill_fraction).unwrap_or(dec!(1));
        (fill_size, true)
    }

    /// Calculate adverse selection cost
    ///
    /// Based on the insight that fills happen when informed traders
    /// are moving the market against us. We estimate the expected
    /// price impact post-fill.
    fn calculate_adverse_selection(
        &self,
        fill_price: Decimal,
        market: &MarketState,
        _is_bid: bool,
    ) -> Decimal {
        // Adverse selection is proportional to:
        // 1. Volatility (higher vol = larger adverse moves)
        // 2. Entropy (lower entropy = more informed trading)
        // 3. Flow toxicity indicators

        let vol_factor = market.volatility * 100.0; // Scale up
        let entropy_factor = 1.5 - market.entropy_score; // Low entropy = more adverse

        let base_adverse_bps = self.config.adverse_selection_factor * vol_factor * entropy_factor;

        // Convert to absolute cost
        let adverse_move = fill_price * Decimal::from_f64(base_adverse_bps / 10000.0)
            .unwrap_or(dec!(0));

        adverse_move
    }

    /// Simple deterministic pseudo-random for reproducibility
    fn pseudo_random(&mut self, input: u64) -> f64 {
        // LCG-style PRNG
        self.seed = self.seed.wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407)
            .wrapping_add(input);

        // Convert to 0.0-1.0
        (self.seed as f64) / (u64::MAX as f64)
    }

    /// Reset simulator state
    pub fn reset(&mut self) {
        self.active_bid = None;
        self.active_ask = None;
        self.stats = FillSimulatorStats::default();
        self.seed = 42;
    }

    /// Get fill rate
    pub fn fill_rate(&self) -> (f64, f64) {
        let bid_rate = if self.stats.bid_touches > 0 {
            self.stats.bid_fills as f64 / self.stats.bid_touches as f64
        } else {
            0.0
        };

        let ask_rate = if self.stats.ask_touches > 0 {
            self.stats.ask_fills as f64 / self.stats.ask_touches as f64
        } else {
            0.0
        };

        (bid_rate, ask_rate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_market_state(mid_price: f64, prev_mid: f64) -> MarketState {
        MarketState {
            mid_price: Decimal::from_f64(mid_price).unwrap(),
            prev_mid_price: Decimal::from_f64(prev_mid).unwrap(),
            spread_bps: 2.0,
            trade_rate: 10.0,
            aggr_buy_ratio: 0.5,
            book_imbalance: 0.0,
            entropy_score: 0.7,
            volatility: 0.001,
            bid_depth: dec!(10),
            ask_depth: dec!(10),
        }
    }

    fn make_quote(price: f64, is_bid: bool) -> Quote {
        Quote {
            price: Decimal::from_f64(price).unwrap(),
            size: dec!(0.01),
            side: if is_bid { QuoteSide::Bid } else { QuoteSide::Ask },
            timestamp_ms: 0,
        }
    }

    #[test]
    fn test_no_fill_when_price_doesnt_touch() {
        let config = FillSimulatorConfig::default();
        let mut sim = FillSimulator::new(config);

        let quotes = MMQuotes {
            bid: Some(make_quote(50000.0, true)),
            ask: Some(make_quote(50010.0, false)),
            regime: crate::execution::market_maker::MarketRegime::HighEntropy,
            fair_value: dec!(50005),
            half_spread: dec!(5),
            skew: dec!(0),
        };

        sim.update_quotes(&quotes, 0);

        // Price stays above bid, below ask
        let market = make_market_state(50005.0, 50005.0);
        let events = sim.simulate_fills(&market, 100);

        assert!(events.is_empty());
        assert_eq!(sim.stats.bid_touches, 0);
        assert_eq!(sim.stats.ask_touches, 0);
    }

    #[test]
    fn test_potential_fill_when_price_touches_bid() {
        let mut config = FillSimulatorConfig::default();
        config.base_fill_probability = 1.0; // Guarantee fill for testing
        config.quote_latency_ms = 0;

        let mut sim = FillSimulator::new(config);

        let quotes = MMQuotes {
            bid: Some(make_quote(50000.0, true)),
            ask: Some(make_quote(50010.0, false)),
            regime: crate::execution::market_maker::MarketRegime::HighEntropy,
            fair_value: dec!(50005),
            half_spread: dec!(5),
            skew: dec!(0),
        };

        sim.update_quotes(&quotes, 0);

        // Price drops to bid level
        let market = make_market_state(50000.0, 50005.0);
        let events = sim.simulate_fills(&market, 100);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fill.side, QuoteSide::Bid);
        assert_eq!(sim.stats.bid_touches, 1);
        assert_eq!(sim.stats.bid_fills, 1);
    }

    #[test]
    fn test_latency_prevents_immediate_fill() {
        let mut config = FillSimulatorConfig::default();
        config.base_fill_probability = 1.0;
        config.quote_latency_ms = 100;

        let mut sim = FillSimulator::new(config);

        let quotes = MMQuotes {
            bid: Some(make_quote(50000.0, true)),
            ask: Some(make_quote(50010.0, false)),
            regime: crate::execution::market_maker::MarketRegime::HighEntropy,
            fair_value: dec!(50005),
            half_spread: dec!(5),
            skew: dec!(0),
        };

        sim.update_quotes(&quotes, 0);

        // Try to fill immediately (before latency window)
        let market = make_market_state(50000.0, 50005.0);
        let events = sim.simulate_fills(&market, 50); // Only 50ms elapsed

        assert!(events.is_empty());

        // Now with enough time elapsed
        let events = sim.simulate_fills(&market, 150); // 150ms elapsed
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_low_probability_prevents_most_fills() {
        let mut config = FillSimulatorConfig::default();
        config.base_fill_probability = 0.01; // 1% base
        config.queue_position = 1.0; // Back of queue
        config.quote_latency_ms = 0;

        let mut sim = FillSimulator::new(config);

        let quotes = MMQuotes {
            bid: Some(make_quote(50000.0, true)),
            ask: Some(make_quote(50010.0, false)),
            regime: crate::execution::market_maker::MarketRegime::HighEntropy,
            fair_value: dec!(50005),
            half_spread: dec!(5),
            skew: dec!(0),
        };

        sim.update_quotes(&quotes, 0);

        // Run many simulations
        let mut fills = 0;
        for i in 0..1000 {
            let market = make_market_state(50000.0, 50005.0);
            let events = sim.simulate_fills(&market, i * 100);
            fills += events.len();

            // Reset quotes to simulate new opportunities
            sim.update_quotes(&quotes, i * 100 + 50);
        }

        // With 1% base and back of queue (0.2x), expect ~0.2% fill rate
        // Allow for some variance
        assert!(fills < 100, "Expected low fill rate, got {}", fills);
    }

    #[test]
    fn test_adverse_selection_calculated() {
        let mut config = FillSimulatorConfig::default();
        config.base_fill_probability = 1.0;
        config.quote_latency_ms = 0;

        let mut sim = FillSimulator::new(config);

        let quotes = MMQuotes {
            bid: Some(make_quote(50000.0, true)),
            ask: None,
            regime: crate::execution::market_maker::MarketRegime::HighEntropy,
            fair_value: dec!(50005),
            half_spread: dec!(5),
            skew: dec!(0),
        };

        sim.update_quotes(&quotes, 0);

        let market = make_market_state(50000.0, 50005.0);
        let events = sim.simulate_fills(&market, 100);

        assert_eq!(events.len(), 1);
        assert!(events[0].adverse_selection_cost > dec!(0));
    }

    #[test]
    fn test_fill_probability_factors() {
        let config = FillSimulatorConfig::default();
        let sim = FillSimulator::new(config);

        let quote = make_quote(50000.0, true);

        // Base case
        let market1 = MarketState {
            entropy_score: 0.5,
            trade_rate: 10.0,
            book_imbalance: 0.0,
            spread_bps: 5.0,
            ..make_market_state(50000.0, 50005.0)
        };
        let prob1 = sim.calculate_fill_probability(&quote, &market1, true);

        // Low entropy should increase fill probability
        let market2 = MarketState {
            entropy_score: 0.2,
            ..market1.clone()
        };
        let prob2 = sim.calculate_fill_probability(&quote, &market2, true);
        assert!(prob2 > prob1, "Low entropy should increase fill prob");

        // High trade rate should increase fill probability
        let market3 = MarketState {
            trade_rate: 20.0,
            ..market1.clone()
        };
        let prob3 = sim.calculate_fill_probability(&quote, &market3, true);
        assert!(prob3 > prob1, "High trade rate should increase fill prob");

        // Negative imbalance (more asks) should increase bid fill probability
        let market4 = MarketState {
            book_imbalance: -0.5,
            ..market1.clone()
        };
        let prob4 = sim.calculate_fill_probability(&quote, &market4, true);
        assert!(prob4 > prob1, "Negative imbalance should increase bid fill prob");
    }
}

//! Market Making Engine
//!
//! Implements an Avellaneda-Stoikov style market maker with entropy-based regime control.
//!
//! Core concepts:
//! - Fair value estimation from microprice + alpha signals
//! - Spread determination based on volatility and regime
//! - Inventory-based skew to manage risk
//! - Regime detection from entropy to adjust aggressiveness
//!
//! References:
//! - Avellaneda & Stoikov (2008) "High-frequency trading in a limit order book"
//! - Guéant, Lehalle, Fernandez-Tapia (2013) "Dealing with the Inventory Risk"

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Market regime based on entropy analysis
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketRegime {
    /// High entropy: random flow, good for tight MM
    HighEntropy,
    /// Medium entropy: uncertain, use caution
    MediumEntropy,
    /// Low entropy: one-sided flow, high adverse selection risk
    LowEntropy,
}

impl MarketRegime {
    pub fn from_entropy_score(score: f64, config: &RegimeThresholds) -> Self {
        if score >= config.high_entropy_threshold {
            MarketRegime::HighEntropy
        } else if score >= config.low_entropy_threshold {
            MarketRegime::MediumEntropy
        } else {
            MarketRegime::LowEntropy
        }
    }
}

/// Thresholds for regime classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeThresholds {
    /// Above this = high entropy (good for MM)
    pub high_entropy_threshold: f64,
    /// Below this = low entropy (dangerous for MM)
    pub low_entropy_threshold: f64,
}

impl Default for RegimeThresholds {
    fn default() -> Self {
        Self {
            high_entropy_threshold: 0.7,
            low_entropy_threshold: 0.4,
        }
    }
}

/// Configuration for the market maker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MMConfig {
    /// Base half-spread in basis points (applied to each side)
    pub base_spread_bps: f64,
    /// How much to skew quotes per unit of inventory
    pub inventory_skew_factor: f64,
    /// Maximum allowed inventory (absolute value)
    pub max_inventory: Decimal,
    /// Base quote size
    pub quote_size: Decimal,
    /// Risk aversion parameter (gamma in Avellaneda-Stoikov)
    pub risk_aversion: f64,
    /// Regime-specific parameters
    pub regime_thresholds: RegimeThresholds,
    /// Spread multiplier for medium entropy regime
    pub medium_entropy_spread_mult: f64,
    /// Spread multiplier for low entropy regime
    pub low_entropy_spread_mult: f64,
    /// Whether to pull quotes entirely in low entropy
    pub pull_quotes_in_low_entropy: bool,
}

impl Default for MMConfig {
    fn default() -> Self {
        Self {
            base_spread_bps: 2.0, // 2 bps per side = 4 bps total
            inventory_skew_factor: 0.5,
            max_inventory: dec!(0.1), // 0.1 BTC max position
            quote_size: dec!(0.001), // 0.001 BTC per quote
            risk_aversion: 0.1,
            regime_thresholds: RegimeThresholds::default(),
            medium_entropy_spread_mult: 1.5,
            low_entropy_spread_mult: 3.0,
            pull_quotes_in_low_entropy: false,
        }
    }
}

/// A single quote (bid or ask)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub price: Decimal,
    pub size: Decimal,
    pub side: QuoteSide,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuoteSide {
    Bid,
    Ask,
}

/// Output of the MM engine: the quotes to place
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MMQuotes {
    pub bid: Option<Quote>,
    pub ask: Option<Quote>,
    pub regime: MarketRegime,
    pub fair_value: Decimal,
    pub half_spread: Decimal,
    pub skew: Decimal,
}

/// A simulated fill
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub side: QuoteSide,
    pub price: Decimal,
    pub size: Decimal,
    pub timestamp_ms: u64,
}

/// PnL tracking
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnLTracker {
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub num_trades: u64,
    pub total_volume: Decimal,
    pub fees_paid: Decimal,
}

impl PnLTracker {
    pub fn update(&mut self, inventory: Decimal, avg_entry: Decimal, current_price: Decimal) {
        self.unrealized_pnl = inventory * (current_price - avg_entry);
        self.total_pnl = self.realized_pnl + self.unrealized_pnl;
    }
}

/// Market Maker Engine
///
/// Computes optimal quotes based on:
/// - Current market state (microprice, spread, volatility)
/// - Inventory position
/// - Market regime (from entropy)
pub struct MarketMakerEngine {
    config: MMConfig,

    // Position state
    inventory: Decimal,
    avg_entry_price: Decimal,

    // PnL tracking
    pnl: PnLTracker,

    // Current quotes
    current_bid: Option<Quote>,
    current_ask: Option<Quote>,

    // Historical data for analysis
    recent_fills: VecDeque<Fill>,
    max_fill_history: usize,
}

impl MarketMakerEngine {
    pub fn new(config: MMConfig) -> Self {
        Self {
            config,
            inventory: dec!(0),
            avg_entry_price: dec!(0),
            pnl: PnLTracker::default(),
            current_bid: None,
            current_ask: None,
            recent_fills: VecDeque::with_capacity(1000),
            max_fill_history: 1000,
        }
    }

    /// Compute the entropy score from entropy features
    /// Returns a value in [0, 1] where higher = more random (good for MM)
    pub fn compute_entropy_score(
        &self,
        tick_entropy_1s: Option<Decimal>,
        tick_entropy_5s: Option<Decimal>,
        tick_entropy_10s: Option<Decimal>,
    ) -> f64 {
        // Maximum entropy for 3-state system (up/down/unchanged) is log2(3) ≈ 1.585
        const MAX_ENTROPY: f64 = 1.585;

        let mut sum = 0.0;
        let mut count = 0;

        if let Some(e) = tick_entropy_1s {
            sum += e.to_f64().unwrap_or(0.0);
            count += 1;
        }
        if let Some(e) = tick_entropy_5s {
            sum += e.to_f64().unwrap_or(0.0);
            count += 1;
        }
        if let Some(e) = tick_entropy_10s {
            sum += e.to_f64().unwrap_or(0.0);
            count += 1;
        }

        if count == 0 {
            return 0.5; // Default to medium if no data
        }

        let avg_entropy = sum / count as f64;
        (avg_entropy / MAX_ENTROPY).min(1.0).max(0.0)
    }

    /// Compute flow imbalance from aggressive volumes
    /// Returns value in [-1, 1]: negative = sell pressure, positive = buy pressure
    pub fn compute_flow_imbalance(
        &self,
        aggr_buy_vol: Decimal,
        aggr_sell_vol: Decimal,
    ) -> f64 {
        let total = aggr_buy_vol + aggr_sell_vol;
        if total == dec!(0) {
            return 0.0;
        }

        let imbalance = (aggr_buy_vol - aggr_sell_vol) / total;
        imbalance.to_f64().unwrap_or(0.0)
    }

    /// Main quote computation
    ///
    /// # Arguments
    /// * `microprice` - Fair value estimate from order book
    /// * `mid_price` - Simple mid price
    /// * `volatility` - Current volatility estimate (e.g., realized_volatility_100)
    /// * `entropy_score` - Normalized entropy [0, 1]
    /// * `flow_imbalance` - Flow direction [-1, 1]
    /// * `timestamp_ms` - Current timestamp
    pub fn compute_quotes(
        &mut self,
        microprice: Decimal,
        mid_price: Decimal,
        volatility: f64,
        entropy_score: f64,
        flow_imbalance: f64,
        timestamp_ms: u64,
    ) -> MMQuotes {
        // 1. Determine regime
        let regime = MarketRegime::from_entropy_score(entropy_score, &self.config.regime_thresholds);

        // 2. Compute fair value (microprice + any alpha adjustment)
        let fair_value = microprice;

        // 3. Compute base half-spread
        let base_spread = mid_price * Decimal::from_f64(self.config.base_spread_bps / 10000.0)
            .unwrap_or(dec!(0.0001));

        // 4. Adjust spread for volatility
        let vol_adjustment = Decimal::from_f64(1.0 + volatility * 100.0).unwrap_or(dec!(1));
        let vol_adjusted_spread = base_spread * vol_adjustment;

        // 5. Adjust spread for regime
        let regime_mult = match regime {
            MarketRegime::HighEntropy => dec!(1),
            MarketRegime::MediumEntropy => Decimal::from_f64(self.config.medium_entropy_spread_mult).unwrap_or(dec!(1.5)),
            MarketRegime::LowEntropy => Decimal::from_f64(self.config.low_entropy_spread_mult).unwrap_or(dec!(3)),
        };
        let half_spread = vol_adjusted_spread * regime_mult;

        // 6. Compute inventory skew
        // Positive inventory (long) -> skew ask down (more aggressive) to reduce position
        // Negative inventory (short) -> skew bid up (more aggressive) to reduce position
        let inventory_ratio = if self.config.max_inventory > dec!(0) {
            self.inventory / self.config.max_inventory
        } else {
            dec!(0)
        };
        let inv_skew = inventory_ratio * Decimal::from_f64(self.config.inventory_skew_factor).unwrap_or(dec!(0.5));

        // 7. Add flow-based skew in low entropy (lean with the flow)
        let flow_skew = if regime == MarketRegime::LowEntropy {
            Decimal::from_f64(flow_imbalance * 0.5).unwrap_or(dec!(0))
        } else {
            dec!(0)
        };

        let total_skew = inv_skew + flow_skew;

        // 8. Compute final quote prices
        let bid_price = fair_value - half_spread - (total_skew * half_spread);
        let ask_price = fair_value + half_spread - (total_skew * half_spread);

        // 9. Determine quote sizes based on regime and inventory
        let size_mult = match regime {
            MarketRegime::HighEntropy => dec!(1),
            MarketRegime::MediumEntropy => dec!(0.7),
            MarketRegime::LowEntropy => dec!(0.3),
        };

        // Reduce size on the side where we have inventory
        let bid_size = if self.inventory > dec!(0) {
            self.config.quote_size * size_mult * dec!(0.5) // Already long, less eager to buy
        } else {
            self.config.quote_size * size_mult
        };

        let ask_size = if self.inventory < dec!(0) {
            self.config.quote_size * size_mult * dec!(0.5) // Already short, less eager to sell
        } else {
            self.config.quote_size * size_mult
        };

        // 10. Check if we should pull quotes
        let should_pull = regime == MarketRegime::LowEntropy && self.config.pull_quotes_in_low_entropy;
        let at_max_inventory = self.inventory.abs() >= self.config.max_inventory;

        let bid = if should_pull || (at_max_inventory && self.inventory > dec!(0)) {
            None
        } else {
            Some(Quote {
                price: bid_price.round_dp(2),
                size: bid_size,
                side: QuoteSide::Bid,
                timestamp_ms,
            })
        };

        let ask = if should_pull || (at_max_inventory && self.inventory < dec!(0)) {
            None
        } else {
            Some(Quote {
                price: ask_price.round_dp(2),
                size: ask_size,
                side: QuoteSide::Ask,
                timestamp_ms,
            })
        };

        // Update current quotes
        self.current_bid = bid.clone();
        self.current_ask = ask.clone();

        MMQuotes {
            bid,
            ask,
            regime,
            fair_value,
            half_spread,
            skew: total_skew,
        }
    }

    /// Process a fill (when our quote gets hit)
    pub fn process_fill(&mut self, fill: Fill, fee_rate: Decimal) {
        let fill_value = fill.price * fill.size;
        let fee = fill_value * fee_rate;

        match fill.side {
            QuoteSide::Bid => {
                // We bought: inventory increases
                let old_value = self.inventory * self.avg_entry_price;
                let new_value = fill.price * fill.size;
                self.inventory += fill.size;

                if self.inventory != dec!(0) {
                    self.avg_entry_price = (old_value + new_value) / self.inventory;
                }
            }
            QuoteSide::Ask => {
                // We sold: inventory decreases
                if self.inventory > dec!(0) {
                    // Closing long position
                    let pnl = (fill.price - self.avg_entry_price) * fill.size;
                    self.pnl.realized_pnl += pnl;
                }
                self.inventory -= fill.size;

                if self.inventory < dec!(0) {
                    // Opening/adding to short
                    self.avg_entry_price = fill.price;
                }
            }
        }

        self.pnl.fees_paid += fee;
        self.pnl.realized_pnl -= fee;
        self.pnl.num_trades += 1;
        self.pnl.total_volume += fill.size;

        // Store fill
        self.recent_fills.push_back(fill);
        if self.recent_fills.len() > self.max_fill_history {
            self.recent_fills.pop_front();
        }
    }

    /// Update unrealized PnL based on current price
    pub fn update_mark_to_market(&mut self, current_price: Decimal) {
        self.pnl.update(self.inventory, self.avg_entry_price, current_price);
    }

    /// Get current state
    pub fn get_state(&self) -> MMState {
        MMState {
            inventory: self.inventory,
            avg_entry_price: self.avg_entry_price,
            pnl: self.pnl.clone(),
            current_bid: self.current_bid.clone(),
            current_ask: self.current_ask.clone(),
        }
    }

    /// Reset the engine (for new session)
    pub fn reset(&mut self) {
        self.inventory = dec!(0);
        self.avg_entry_price = dec!(0);
        self.pnl = PnLTracker::default();
        self.current_bid = None;
        self.current_ask = None;
        self.recent_fills.clear();
    }

    /// Get inventory
    pub fn inventory(&self) -> Decimal {
        self.inventory
    }

    /// Get PnL
    pub fn pnl(&self) -> &PnLTracker {
        &self.pnl
    }

    /// Get config
    pub fn config(&self) -> &MMConfig {
        &self.config
    }
}

/// Snapshot of MM state for display/logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MMState {
    pub inventory: Decimal,
    pub avg_entry_price: Decimal,
    pub pnl: PnLTracker,
    pub current_bid: Option<Quote>,
    pub current_ask: Option<Quote>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regime_detection() {
        let config = RegimeThresholds::default();

        assert_eq!(MarketRegime::from_entropy_score(0.8, &config), MarketRegime::HighEntropy);
        assert_eq!(MarketRegime::from_entropy_score(0.5, &config), MarketRegime::MediumEntropy);
        assert_eq!(MarketRegime::from_entropy_score(0.2, &config), MarketRegime::LowEntropy);
    }

    #[test]
    fn test_basic_quotes() {
        let config = MMConfig::default();
        let mut mm = MarketMakerEngine::new(config);

        let quotes = mm.compute_quotes(
            dec!(50000), // microprice
            dec!(50000), // mid
            0.001,       // volatility
            0.8,         // high entropy
            0.0,         // neutral flow
            1000,
        );

        assert!(quotes.bid.is_some());
        assert!(quotes.ask.is_some());
        assert_eq!(quotes.regime, MarketRegime::HighEntropy);

        let bid = quotes.bid.unwrap();
        let ask = quotes.ask.unwrap();

        assert!(bid.price < dec!(50000));
        assert!(ask.price > dec!(50000));
        assert!(ask.price > bid.price);
    }

    #[test]
    fn test_inventory_skew() {
        let config = MMConfig::default();
        let mut mm = MarketMakerEngine::new(config);

        // Simulate being long
        mm.inventory = dec!(0.05);

        let quotes = mm.compute_quotes(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            1000,
        );

        // When long, ask should be more aggressive (lower) relative to bid
        // This is achieved through negative skew
        assert!(quotes.skew > dec!(0)); // Positive skew when long
    }

    #[test]
    fn test_low_entropy_wider_spread() {
        let config = MMConfig::default();
        let mut mm = MarketMakerEngine::new(config);

        let high_ent = mm.compute_quotes(dec!(50000), dec!(50000), 0.001, 0.8, 0.0, 1000);
        let low_ent = mm.compute_quotes(dec!(50000), dec!(50000), 0.001, 0.2, 0.0, 2000);

        // Low entropy should have wider spread
        assert!(low_ent.half_spread > high_ent.half_spread);
    }

    #[test]
    fn test_fill_processing() {
        let config = MMConfig::default();
        let mut mm = MarketMakerEngine::new(config);

        // Buy fill
        mm.process_fill(Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        }, dec!(0.0001));

        assert_eq!(mm.inventory, dec!(0.01));
        assert_eq!(mm.avg_entry_price, dec!(50000));

        // Sell fill at higher price
        mm.process_fill(Fill {
            side: QuoteSide::Ask,
            price: dec!(50100),
            size: dec!(0.01),
            timestamp_ms: 2000,
        }, dec!(0.0001));

        assert_eq!(mm.inventory, dec!(0));
        assert!(mm.pnl.realized_pnl > dec!(0)); // Should have profit
    }
}

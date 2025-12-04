//! Market Making Algorithms
//!
//! This module contains market making algorithm implementations.
//! Each algorithm is a separate struct with its own configuration.
//!
//! # Available Algorithms
//!
//! - [`AvellanedaStoikovMM`]: Avellaneda-Stoikov market maker with regime-aware parameters
//!
//! # Adding New Algorithms
//!
//! To add a new algorithm:
//! 1. Create a new config struct (e.g., `MyAlgorithmConfig`)
//! 2. Create the algorithm struct implementing the same interface
//! 3. Add to the `Algorithm` enum for unified handling
//!
//! # References
//!
//! - Avellaneda & Stoikov (2008) "High-frequency trading in a limit order book"
//! - Guéant, Lehalle, Fernandez-Tapia (2013) "Dealing with the Inventory Risk"

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

// ============================================================================
// Common Types (shared across all algorithms)
// ============================================================================

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
    pub fn from_entropy_score(score: f64, thresholds: &RegimeThresholds) -> Self {
        if score >= thresholds.high_entropy_threshold {
            MarketRegime::HighEntropy
        } else if score >= thresholds.low_entropy_threshold {
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

/// Output of any MM algorithm: the quotes to place
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

/// PnL tracking (shared across algorithms)
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

/// Snapshot of MM state for display/logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MMState {
    pub inventory: Decimal,
    pub avg_entry_price: Decimal,
    pub pnl: PnLTracker,
    pub current_bid: Option<Quote>,
    pub current_ask: Option<Quote>,
}

// ============================================================================
// Avellaneda-Stoikov Market Maker
// ============================================================================

/// Per-regime configuration parameters for Avellaneda-Stoikov
/// Encodes the conditional probability of success given the regime into parameter aggressiveness
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeConfig {
    /// Half-spread in basis points for this regime
    pub spread_bps: f64,
    /// Inventory skew factor for this regime
    pub skew_factor: f64,
    /// Quote size multiplier (1.0 = full size)
    pub size_mult: f64,
    /// Whether to quote at all in this regime
    pub should_quote: bool,
}

impl RegimeConfig {
    pub fn new(spread_bps: f64, skew_factor: f64, size_mult: f64, should_quote: bool) -> Self {
        Self {
            spread_bps,
            skew_factor,
            size_mult,
            should_quote,
        }
    }
}

impl Default for RegimeConfig {
    fn default() -> Self {
        Self {
            spread_bps: 2.0,
            skew_factor: 0.5,
            size_mult: 1.0,
            should_quote: true,
        }
    }
}

/// Regime-specific parameter set for Avellaneda-Stoikov
/// Maps regime → optimal parameters (encodes P(success|regime))
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeParams {
    pub high_entropy: RegimeConfig,
    pub medium_entropy: RegimeConfig,
    pub low_entropy: RegimeConfig,
}

impl Default for RegimeParams {
    fn default() -> Self {
        Self {
            // High entropy: aggressive - historically ~62% win rate
            high_entropy: RegimeConfig {
                spread_bps: 1.0,
                skew_factor: 0.3,
                size_mult: 1.0,
                should_quote: true,
            },
            // Medium entropy: moderate - historically ~51% win rate
            medium_entropy: RegimeConfig {
                spread_bps: 2.5,
                skew_factor: 0.5,
                size_mult: 0.7,
                should_quote: true,
            },
            // Low entropy: defensive - historically ~38% win rate
            low_entropy: RegimeConfig {
                spread_bps: 5.0,
                skew_factor: 1.0,
                size_mult: 0.3,
                should_quote: false,
            },
        }
    }
}

impl RegimeParams {
    /// Get config for a specific regime
    pub fn for_regime(&self, regime: MarketRegime) -> &RegimeConfig {
        match regime {
            MarketRegime::HighEntropy => &self.high_entropy,
            MarketRegime::MediumEntropy => &self.medium_entropy,
            MarketRegime::LowEntropy => &self.low_entropy,
        }
    }

    /// Create uniform params (same base config, scaled by regime)
    /// Useful for simple configurations where you just want one spread/skew
    pub fn uniform(spread_bps: f64, skew_factor: f64) -> Self {
        Self {
            high_entropy: RegimeConfig {
                spread_bps,
                skew_factor,
                size_mult: 1.0,
                should_quote: true,
            },
            medium_entropy: RegimeConfig {
                spread_bps: spread_bps * 1.5,
                skew_factor,
                size_mult: 0.7,
                should_quote: true,
            },
            low_entropy: RegimeConfig {
                spread_bps: spread_bps * 3.0,
                skew_factor,
                size_mult: 0.3,
                should_quote: true,
            },
        }
    }

    /// Create fully uniform params (exact same config for all regimes)
    pub fn fully_uniform(spread_bps: f64, skew_factor: f64) -> Self {
        let config = RegimeConfig {
            spread_bps,
            skew_factor,
            size_mult: 1.0,
            should_quote: true,
        };
        Self {
            high_entropy: config.clone(),
            medium_entropy: config.clone(),
            low_entropy: config,
        }
    }
}

/// Configuration for Avellaneda-Stoikov Market Maker
///
/// This algorithm uses regime-aware parameters where spread, skew, and size
/// are adjusted based on the current market regime (entropy level).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvellanedaStoikovConfig {
    /// Maximum allowed inventory (absolute value)
    pub max_inventory: Decimal,
    /// Base quote size
    pub quote_size: Decimal,
    /// Risk aversion parameter (gamma in Avellaneda-Stoikov)
    pub risk_aversion: f64,
    /// Regime classification thresholds
    pub regime_thresholds: RegimeThresholds,
    /// Per-regime parameters (spread, skew, size per regime)
    pub regime_params: RegimeParams,
}

impl Default for AvellanedaStoikovConfig {
    fn default() -> Self {
        Self {
            max_inventory: dec!(0.1),
            quote_size: dec!(0.001),
            risk_aversion: 0.1,
            regime_thresholds: RegimeThresholds::default(),
            regime_params: RegimeParams::default(),
        }
    }
}

impl AvellanedaStoikovConfig {
    /// Create config with custom regime parameters
    pub fn with_regime_params(regime_params: RegimeParams) -> Self {
        Self {
            regime_params,
            ..Default::default()
        }
    }

    /// Create config with uniform spread/skew (simple mode)
    pub fn with_uniform_params(spread_bps: f64, skew_factor: f64) -> Self {
        Self {
            regime_params: RegimeParams::uniform(spread_bps, skew_factor),
            ..Default::default()
        }
    }
}

/// Avellaneda-Stoikov Market Maker
///
/// Implements the Avellaneda-Stoikov market making model with:
/// - Fair value estimation from microprice
/// - Regime-aware spread and skew parameters
/// - Inventory-based position management
/// - Entropy-based regime detection
///
/// # Algorithm
///
/// 1. Detect market regime from entropy score
/// 2. Select parameters for current regime
/// 3. Compute fair value from microprice
/// 4. Apply volatility adjustment to spread
/// 5. Compute inventory skew
/// 6. Generate bid/ask quotes
pub struct AvellanedaStoikovMM {
    config: AvellanedaStoikovConfig,
    inventory: Decimal,
    avg_entry_price: Decimal,
    pnl: PnLTracker,
    current_bid: Option<Quote>,
    current_ask: Option<Quote>,
    recent_fills: VecDeque<Fill>,
    max_fill_history: usize,
}

impl AvellanedaStoikovMM {
    pub fn new(config: AvellanedaStoikovConfig) -> Self {
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
        const MAX_ENTROPY: f64 = 1.585; // log2(3) for 3-state system

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
    /// * `volatility` - Current volatility estimate
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

        // 2. Get regime-specific parameters
        let regime_config = self.config.regime_params.for_regime(regime);

        // 3. Check if we should quote at all
        if !regime_config.should_quote {
            self.current_bid = None;
            self.current_ask = None;
            return MMQuotes {
                bid: None,
                ask: None,
                regime,
                fair_value: microprice,
                half_spread: dec!(0),
                skew: dec!(0),
            };
        }

        // 4. Compute fair value
        let fair_value = microprice;

        // 5. Compute half-spread using regime-specific spread
        let base_spread = mid_price * Decimal::from_f64(regime_config.spread_bps / 10000.0)
            .unwrap_or(dec!(0.0001));

        // 6. Adjust spread for volatility
        let vol_adjustment = Decimal::from_f64(1.0 + volatility * 100.0).unwrap_or(dec!(1));
        let half_spread = base_spread * vol_adjustment;

        // 7. Compute inventory skew using regime-specific skew factor
        let inventory_ratio = if self.config.max_inventory > dec!(0) {
            self.inventory / self.config.max_inventory
        } else {
            dec!(0)
        };
        let inv_skew = inventory_ratio * Decimal::from_f64(regime_config.skew_factor).unwrap_or(dec!(0.5));

        // 8. Add flow-based skew in low entropy (lean with the flow)
        let flow_skew = if regime == MarketRegime::LowEntropy {
            Decimal::from_f64(flow_imbalance * 0.5).unwrap_or(dec!(0))
        } else {
            dec!(0)
        };

        let total_skew = inv_skew + flow_skew;

        // 9. Compute final quote prices
        let bid_price = fair_value - half_spread - (total_skew * half_spread);
        let ask_price = fair_value + half_spread - (total_skew * half_spread);

        // 10. Determine quote sizes using regime-specific size multiplier
        let size_mult = Decimal::from_f64(regime_config.size_mult).unwrap_or(dec!(1));

        let bid_size = if self.inventory > dec!(0) {
            self.config.quote_size * size_mult * dec!(0.5)
        } else {
            self.config.quote_size * size_mult
        };

        let ask_size = if self.inventory < dec!(0) {
            self.config.quote_size * size_mult * dec!(0.5)
        } else {
            self.config.quote_size * size_mult
        };

        // 11. Check inventory limits
        let at_max_inventory = self.inventory.abs() >= self.config.max_inventory;

        let bid = if at_max_inventory && self.inventory > dec!(0) {
            None
        } else {
            Some(Quote {
                price: bid_price.round_dp(2),
                size: bid_size,
                side: QuoteSide::Bid,
                timestamp_ms,
            })
        };

        let ask = if at_max_inventory && self.inventory < dec!(0) {
            None
        } else {
            Some(Quote {
                price: ask_price.round_dp(2),
                size: ask_size,
                side: QuoteSide::Ask,
                timestamp_ms,
            })
        };

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
                let old_value = self.inventory * self.avg_entry_price;
                let new_value = fill.price * fill.size;
                self.inventory += fill.size;

                if self.inventory != dec!(0) {
                    self.avg_entry_price = (old_value + new_value) / self.inventory;
                }
            }
            QuoteSide::Ask => {
                if self.inventory > dec!(0) {
                    let pnl = (fill.price - self.avg_entry_price) * fill.size;
                    self.pnl.realized_pnl += pnl;
                }
                self.inventory -= fill.size;

                if self.inventory < dec!(0) {
                    self.avg_entry_price = fill.price;
                }
            }
        }

        self.pnl.fees_paid += fee;
        self.pnl.realized_pnl -= fee;
        self.pnl.num_trades += 1;
        self.pnl.total_volume += fill.size;

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
    pub fn config(&self) -> &AvellanedaStoikovConfig {
        &self.config
    }
}

// ============================================================================
// Backward Compatibility Aliases
// ============================================================================

/// Alias for backward compatibility
pub type MMConfig = AvellanedaStoikovConfig;

/// Alias for backward compatibility
pub type MarketMakerEngine = AvellanedaStoikovMM;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regime_detection() {
        let thresholds = RegimeThresholds::default();

        assert_eq!(MarketRegime::from_entropy_score(0.8, &thresholds), MarketRegime::HighEntropy);
        assert_eq!(MarketRegime::from_entropy_score(0.5, &thresholds), MarketRegime::MediumEntropy);
        assert_eq!(MarketRegime::from_entropy_score(0.2, &thresholds), MarketRegime::LowEntropy);
    }

    #[test]
    fn test_avellaneda_stoikov_config_default() {
        let config = AvellanedaStoikovConfig::default();

        assert_eq!(config.max_inventory, dec!(0.1));
        assert_eq!(config.quote_size, dec!(0.001));
        assert_eq!(config.regime_params.high_entropy.spread_bps, 1.0);
        assert!(!config.regime_params.low_entropy.should_quote);
    }

    #[test]
    fn test_avellaneda_stoikov_uniform_params() {
        let config = AvellanedaStoikovConfig::with_uniform_params(2.0, 0.5);

        assert_eq!(config.regime_params.high_entropy.spread_bps, 2.0);
        assert_eq!(config.regime_params.medium_entropy.spread_bps, 3.0);
        assert_eq!(config.regime_params.low_entropy.spread_bps, 6.0);
    }

    #[test]
    fn test_basic_quotes() {
        let config = AvellanedaStoikovConfig::with_uniform_params(2.0, 0.5);
        let mut mm = AvellanedaStoikovMM::new(config);

        let quotes = mm.compute_quotes(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8, // high entropy
            0.0,
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
    fn test_no_quote_in_low_entropy_default() {
        let config = AvellanedaStoikovConfig::default();
        let mut mm = AvellanedaStoikovMM::new(config);

        let quotes = mm.compute_quotes(
            dec!(50000),
            dec!(50000),
            0.001,
            0.2, // low entropy
            0.0,
            1000,
        );

        assert_eq!(quotes.regime, MarketRegime::LowEntropy);
        assert!(quotes.bid.is_none());
        assert!(quotes.ask.is_none());
    }

    #[test]
    fn test_inventory_skew() {
        let config = AvellanedaStoikovConfig::with_uniform_params(2.0, 0.5);
        let mut mm = AvellanedaStoikovMM::new(config);

        mm.inventory = dec!(0.05);

        let quotes = mm.compute_quotes(
            dec!(50000),
            dec!(50000),
            0.001,
            0.8,
            0.0,
            1000,
        );

        assert!(quotes.skew > dec!(0));
    }

    #[test]
    fn test_fill_processing() {
        let config = AvellanedaStoikovConfig::default();
        let mut mm = AvellanedaStoikovMM::new(config);

        mm.process_fill(Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        }, dec!(0.0001));

        assert_eq!(mm.inventory, dec!(0.01));
        assert_eq!(mm.avg_entry_price, dec!(50000));

        mm.process_fill(Fill {
            side: QuoteSide::Ask,
            price: dec!(50100),
            size: dec!(0.01),
            timestamp_ms: 2000,
        }, dec!(0.0001));

        assert_eq!(mm.inventory, dec!(0));
        assert!(mm.pnl.realized_pnl > dec!(0));
    }

    #[test]
    fn test_regime_params_for_regime() {
        let params = RegimeParams::default();

        let high = params.for_regime(MarketRegime::HighEntropy);
        let medium = params.for_regime(MarketRegime::MediumEntropy);
        let low = params.for_regime(MarketRegime::LowEntropy);

        assert_eq!(high.spread_bps, 1.0);
        assert_eq!(medium.spread_bps, 2.5);
        assert_eq!(low.spread_bps, 5.0);
    }

    #[test]
    fn test_fully_uniform_params() {
        let params = RegimeParams::fully_uniform(2.0, 0.5);

        assert_eq!(params.high_entropy.spread_bps, 2.0);
        assert_eq!(params.medium_entropy.spread_bps, 2.0);
        assert_eq!(params.low_entropy.spread_bps, 2.0);
        assert!(params.low_entropy.should_quote);
    }

    #[test]
    fn test_backward_compatibility_aliases() {
        // MMConfig should work as alias for AvellanedaStoikovConfig
        let config = MMConfig::default();
        assert_eq!(config.max_inventory, dec!(0.1));

        // MarketMakerEngine should work as alias for AvellanedaStoikovMM
        let _mm = MarketMakerEngine::new(config);
    }

    #[test]
    fn test_mm_state() {
        let config = AvellanedaStoikovConfig::default();
        let mm = AvellanedaStoikovMM::new(config);

        let state = mm.get_state();
        assert_eq!(state.inventory, dec!(0));
        assert_eq!(state.pnl.num_trades, 0);
    }

    #[test]
    fn test_reset() {
        let config = AvellanedaStoikovConfig::default();
        let mut mm = AvellanedaStoikovMM::new(config);

        mm.process_fill(Fill {
            side: QuoteSide::Bid,
            price: dec!(50000),
            size: dec!(0.01),
            timestamp_ms: 1000,
        }, dec!(0.0001));

        assert!(mm.inventory != dec!(0));

        mm.reset();

        assert_eq!(mm.inventory, dec!(0));
        assert_eq!(mm.pnl.num_trades, 0);
    }
}

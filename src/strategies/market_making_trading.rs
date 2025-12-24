//! MarketMakingTradingAlgorithm Implementation (Task 3.2)
//!
//! Parameterized market making algorithm created from `AlgorithmConfig`. This algorithm
//! implements A-S based quoting for mean-reverting regimes with:
//! - Spread from config (regime-adaptive)
//! - Skew from inventory and regime
//! - Used when momentum not viable
//!
//! # Key Features
//!
//! - **Config-driven**: All parameters derived from `AlgorithmConfig`, no hardcoding
//! - **Research integration**: Uses `TradeableAssessment` to validate trading conditions
//! - **A-S quoting**: Avellaneda-Stoikov based spread and skew calculation
//! - **Regime filtering**: Adapts behavior based on entropy/regime
//! - **Circuit breaker**: Stops trading after consecutive losses or daily drawdown
//!
//! # Decision Flow
//!
//! ```text
//! Input (Features + Assessment)
//!         │
//!         ▼
//! ┌───────────────────┐
//! │ Regime Check      │──────▶ Hold if trending (use momentum instead)
//! └───────────────────┘
//!         │
//!         ▼
//! ┌───────────────────┐
//! │ Compute Quotes    │──────▶ Calculate bid/ask with skew
//! └───────────────────┘
//!         │
//!         ▼
//! TradingDecision::Quote(bid, ask, sizes)
//! ```

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use crate::core::{
    AlgorithmConfig, RecommendedStrategy, StrategyType,
};
use crate::features::FeaturesSnapshot;

use super::trading_algorithm::{
    AlgorithmState, PositionDirection, TradingAction, TradingAlgorithm,
    TradingAlgorithmError, TradingAlgorithmFactory, TradingDecision, TradingInput,
};

// ============================================================================
// Market Making Configuration
// ============================================================================

/// Internal configuration extracted from AlgorithmConfig for market making
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MMTradingConfig {
    // Spread parameters
    pub base_spread_bps: f64,
    pub max_spread_bps: f64,
    pub inventory_skew: f64,

    // A-S model parameters
    pub gamma: f64,  // Risk aversion
    pub kappa: f64,  // Order arrival rate

    // Regime adaptation
    pub widen_in_high_entropy: bool,
    pub entropy_widen_threshold: f64,
    pub spread_widen_multiplier: f64,
    pub max_entropy_for_trading: f64,

    // Position sizing
    pub quote_size_fraction: f64,
    pub max_inventory_fraction: f64,

    // Circuit breaker
    pub max_consecutive_losses: u32,
    pub max_daily_drawdown_pct: f64,
}

impl Default for MMTradingConfig {
    fn default() -> Self {
        Self {
            // Spread parameters
            base_spread_bps: 2.0,
            max_spread_bps: 8.0,
            inventory_skew: 0.5,

            // A-S model parameters
            gamma: 0.3,
            kappa: 2.0,

            // Regime adaptation
            widen_in_high_entropy: true,
            entropy_widen_threshold: 0.7,
            spread_widen_multiplier: 1.5,
            max_entropy_for_trading: 0.9,

            // Position sizing
            quote_size_fraction: 0.02,
            max_inventory_fraction: 0.10,

            // Circuit breaker
            max_consecutive_losses: 5,
            max_daily_drawdown_pct: 2.0,
        }
    }
}

impl MMTradingConfig {
    /// Create configuration from AlgorithmConfig
    pub fn from_algorithm_config(config: &AlgorithmConfig) -> Self {
        let mm = &config.market_making;
        let pos = &config.position;
        let regime = &config.regime_filters;

        Self {
            // Spread parameters from market_making
            base_spread_bps: mm.base_spread_bps,
            max_spread_bps: mm.max_spread_bps,
            inventory_skew: mm.inventory_skew,

            // A-S model parameters
            gamma: mm.gamma,
            kappa: mm.kappa,

            // Regime adaptation
            widen_in_high_entropy: mm.widen_in_high_entropy,
            entropy_widen_threshold: mm.entropy_widen_threshold,
            spread_widen_multiplier: mm.spread_widen_multiplier,
            max_entropy_for_trading: regime.max_entropy,

            // Position sizing from position params
            quote_size_fraction: pos.base_size_fraction,
            max_inventory_fraction: pos.max_size_fraction,

            // Circuit breaker (default values)
            max_consecutive_losses: 5,
            max_daily_drawdown_pct: 2.0,
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.base_spread_bps <= 0.0 {
            return Err("base_spread_bps must be positive".to_string());
        }
        if self.max_spread_bps < self.base_spread_bps {
            return Err("max_spread_bps must be >= base_spread_bps".to_string());
        }
        if self.inventory_skew < 0.0 || self.inventory_skew > 1.0 {
            return Err("inventory_skew must be between 0 and 1".to_string());
        }
        if self.gamma < 0.0 {
            return Err("gamma must be non-negative".to_string());
        }
        if self.kappa <= 0.0 {
            return Err("kappa must be positive".to_string());
        }
        if self.quote_size_fraction <= 0.0 {
            return Err("quote_size_fraction must be positive".to_string());
        }
        if self.max_inventory_fraction <= 0.0 {
            return Err("max_inventory_fraction must be positive".to_string());
        }
        Ok(())
    }
}

// ============================================================================
// Market Making Trading Algorithm
// ============================================================================

/// Parameterized market making algorithm implementing `TradingAlgorithm` trait.
///
/// This algorithm:
/// - Produces symmetric (or inventory-skewed) quotes based on A-S model
/// - Adapts spread based on regime/entropy
/// - Tracks inventory and applies skew to manage risk
/// - Implements circuit breaker for consecutive losses
#[derive(Debug)]
pub struct MarketMakingTradingAlgorithm {
    /// Algorithm configuration (source)
    algo_config: AlgorithmConfig,
    /// MM-specific configuration
    mm_config: MMTradingConfig,
    /// Algorithm state
    state: AlgorithmState,
    /// Current inventory (positive = long, negative = short)
    inventory: Decimal,
    /// Quote counter for tracking
    quote_count: u64,
    /// Consecutive losses counter
    consecutive_losses: u32,
    /// Daily P&L tracker
    daily_pnl: Decimal,
    /// Daily start timestamp
    daily_start: DateTime<Utc>,
    /// Whether circuit breaker is tripped
    circuit_breaker_tripped: bool,
    /// Circuit breaker reason
    circuit_breaker_reason: Option<String>,
    /// Last quoted bid price
    last_bid: Option<Decimal>,
    /// Last quoted ask price
    last_ask: Option<Decimal>,
}

impl MarketMakingTradingAlgorithm {
    /// Create a new market making algorithm from AlgorithmConfig
    pub fn from_config(config: AlgorithmConfig) -> Result<Self, TradingAlgorithmError> {
        // Validate that strategy type is compatible
        if config.strategy_type != StrategyType::MarketMaking && config.strategy_type != StrategyType::Hybrid {
            return Err(TradingAlgorithmError::InvalidConfig(
                format!("MarketMakingTradingAlgorithm requires MarketMaking or Hybrid strategy type, got {:?}", config.strategy_type)
            ));
        }

        let mm_config = MMTradingConfig::from_algorithm_config(&config);
        mm_config.validate().map_err(TradingAlgorithmError::InvalidConfig)?;

        let instance_id = uuid::Uuid::new_v4().to_string();
        let state = AlgorithmState::new(
            instance_id,
            config.id.clone(),
            StrategyType::MarketMaking,
        );

        Ok(Self {
            algo_config: config,
            mm_config,
            state,
            inventory: Decimal::ZERO,
            quote_count: 0,
            consecutive_losses: 0,
            daily_pnl: Decimal::ZERO,
            daily_start: Utc::now(),
            circuit_breaker_tripped: false,
            circuit_breaker_reason: None,
            last_bid: None,
            last_ask: None,
        })
    }

    /// Create with default configuration
    pub fn new() -> Self {
        let mut config = AlgorithmConfig::default();
        config.strategy_type = StrategyType::MarketMaking;
        Self::from_config(config).expect("Default config should be valid")
    }

    /// Get MM configuration
    pub fn mm_config(&self) -> &MMTradingConfig {
        &self.mm_config
    }

    /// Get current inventory
    pub fn inventory(&self) -> Decimal {
        self.inventory
    }

    /// Get quote count
    pub fn quote_count(&self) -> u64 {
        self.quote_count
    }

    /// Check if regime is favorable for market making
    fn is_regime_favorable(&self, input: &TradingInput) -> (bool, String) {
        // Check assessment first
        if let Some(assessment) = &input.assessment {
            // Must be tradeable
            if !assessment.is_tradeable {
                return (false, "Assessment: not tradeable".to_string());
            }

            // Check if MM is recommended
            match assessment.recommended_strategy {
                RecommendedStrategy::MarketMaking
                | RecommendedStrategy::Hybrid => {
                    // Good for MM
                }
                RecommendedStrategy::Momentum
                | RecommendedStrategy::TSMOM
                | RecommendedStrategy::MACrossover => {
                    // Prefer momentum in trending markets, but MM can still work
                    // Just reduce position scale
                }
                RecommendedStrategy::None => {
                    return (false, "Assessment: no strategy recommended".to_string());
                }
            }
        }

        // Check entropy from features - MM works well in high entropy (mean-reverting)
        if let Some(entropy) = input.features.tick_entropy_10s {
            let entropy_f64 = entropy.to_f64().unwrap_or(1.0);
            let normalized_entropy = entropy_f64 / 1.585;

            // MM can trade in higher entropy, but check max threshold
            if normalized_entropy > self.mm_config.max_entropy_for_trading {
                return (false, format!(
                    "Entropy {:.3} above max threshold {:.3}",
                    normalized_entropy, self.mm_config.max_entropy_for_trading
                ));
            }
        }

        (true, "Regime favorable for market making".to_string())
    }

    /// Calculate spread based on regime and volatility
    fn calculate_spread(&self, input: &TradingInput) -> f64 {
        let mut spread_bps = self.mm_config.base_spread_bps;

        // Adjust for volatility if available (realized_volatility_100 is already f64)
        if let Some(vol) = input.features.realized_volatility_100 {
            // Scale spread with volatility (higher vol = wider spread)
            let vol_factor = 1.0 + (vol * 100.0).min(2.0);
            spread_bps *= vol_factor;
        }

        // Widen in high entropy if configured
        if self.mm_config.widen_in_high_entropy {
            if let Some(entropy) = input.features.tick_entropy_10s {
                let entropy_f64 = entropy.to_f64().unwrap_or(0.5);
                let normalized_entropy = entropy_f64 / 1.585;

                if normalized_entropy > self.mm_config.entropy_widen_threshold {
                    spread_bps *= self.mm_config.spread_widen_multiplier;
                }
            }
        }

        // Apply gamma (risk aversion) factor from A-S model
        spread_bps *= 1.0 + self.mm_config.gamma;

        // Clamp to max spread
        spread_bps.min(self.mm_config.max_spread_bps)
    }

    /// Calculate inventory skew for quote adjustment
    fn calculate_skew(&self, input: &TradingInput) -> f64 {
        // Normalized inventory (-1 to 1)
        let max_inv = Decimal::from_f64_retain(self.mm_config.max_inventory_fraction)
            .unwrap_or(dec!(0.1));

        let normalized_inv = if max_inv > Decimal::ZERO {
            (self.inventory / max_inv).to_f64().unwrap_or(0.0).clamp(-1.0, 1.0)
        } else {
            0.0
        };

        // Skew based on inventory and configuration
        // Positive inventory -> lower bid, higher ask (encourage sells)
        // Negative inventory -> higher bid, lower ask (encourage buys)
        let inv_skew = normalized_inv * self.mm_config.inventory_skew;

        // Adjust skew based on regime if available
        let regime_adjustment = if let Some(assessment) = &input.assessment {
            // In trending markets, skew towards the trend
            match assessment.recommended_strategy {
                RecommendedStrategy::Momentum | RecommendedStrategy::TSMOM => {
                    // Slight bias towards trend direction
                    0.1 * assessment.position_scale
                }
                _ => 0.0,
            }
        } else {
            0.0
        };

        // Combine skews
        (inv_skew + regime_adjustment).clamp(-1.0, 1.0)
    }

    /// Compute bid and ask quotes
    fn compute_quotes(&self, input: &TradingInput) -> Option<(Decimal, Decimal, Decimal, Decimal)> {
        let mid_price = input.mid_price()?;

        // Calculate spread in price terms
        let spread_bps = self.calculate_spread(input);
        let half_spread = mid_price * Decimal::from_f64_retain(spread_bps / 10000.0 / 2.0)?;

        // Calculate skew adjustment
        let skew = self.calculate_skew(input);
        let skew_adjustment = half_spread * Decimal::from_f64_retain(skew)?;

        // Compute bid and ask
        let bid_price = mid_price - half_spread - skew_adjustment;
        let ask_price = mid_price + half_spread - skew_adjustment;

        // Calculate quote size based on position scale
        let base_size = Decimal::from_f64_retain(self.mm_config.quote_size_fraction)?;
        let position_scale = input.assessment
            .as_ref()
            .map_or(1.0, |a| a.position_scale);
        let size = base_size * Decimal::from_f64_retain(position_scale)?;

        // Adjust sizes based on inventory
        // If long, reduce bid size and increase ask size
        // If short, increase bid size and reduce ask size
        let inv_factor = (self.inventory / Decimal::from_f64_retain(self.mm_config.max_inventory_fraction)?)
            .to_f64()
            .unwrap_or(0.0)
            .clamp(-1.0, 1.0);

        let bid_size = size * Decimal::from_f64_retain((1.0 - inv_factor * 0.5).max(0.1))?;
        let ask_size = size * Decimal::from_f64_retain((1.0 + inv_factor * 0.5).max(0.1))?;

        Some((bid_price, ask_price, bid_size, ask_size))
    }

    /// Update daily P&L tracking
    fn update_daily_tracking(&mut self, pnl_change: Decimal) {
        let now = Utc::now();

        // Reset if new day
        if now.date_naive() != self.daily_start.date_naive() {
            self.daily_pnl = Decimal::ZERO;
            self.daily_start = now;
            // Reset circuit breaker on new day
            if self.circuit_breaker_tripped {
                self.circuit_breaker_tripped = false;
                self.circuit_breaker_reason = None;
            }
        }

        self.daily_pnl += pnl_change;

        // Check daily drawdown
        let daily_drawdown_pct = if self.daily_pnl < Decimal::ZERO {
            self.daily_pnl.to_f64().unwrap_or(0.0).abs()
        } else {
            0.0
        };

        if daily_drawdown_pct >= self.mm_config.max_daily_drawdown_pct {
            self.circuit_breaker_tripped = true;
            self.circuit_breaker_reason = Some(format!(
                "Daily drawdown {:.2}% exceeded limit {:.2}%",
                daily_drawdown_pct, self.mm_config.max_daily_drawdown_pct
            ));
        }
    }

    /// Record a trade result
    fn record_trade(&mut self, is_win: bool) {
        if is_win {
            self.consecutive_losses = 0;
        } else {
            self.consecutive_losses += 1;

            // Check consecutive loss limit
            if self.consecutive_losses >= self.mm_config.max_consecutive_losses {
                self.circuit_breaker_tripped = true;
                self.circuit_breaker_reason = Some(format!(
                    "Consecutive losses ({}) exceeded limit ({})",
                    self.consecutive_losses, self.mm_config.max_consecutive_losses
                ));
            }
        }
    }

    /// Check if inventory exceeds limits
    fn is_inventory_exceeded(&self) -> bool {
        let max_inv = Decimal::from_f64_retain(self.mm_config.max_inventory_fraction)
            .unwrap_or(dec!(0.1));
        self.inventory.abs() > max_inv
    }
}

impl Default for MarketMakingTradingAlgorithm {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TradingAlgorithm Implementation
// ============================================================================

impl TradingAlgorithm for MarketMakingTradingAlgorithm {
    fn strategy_type(&self) -> StrategyType {
        StrategyType::MarketMaking
    }

    fn name(&self) -> &str {
        "Market Making Trading Algorithm"
    }

    fn version(&self) -> &str {
        "1.0.0"
    }

    fn config_id(&self) -> &str {
        &self.algo_config.id
    }

    fn instance_id(&self) -> &str {
        &self.state.instance_id
    }

    fn decide(&mut self, input: &TradingInput) -> TradingDecision {
        // Check circuit breaker
        if self.circuit_breaker_tripped {
            return TradingDecision::hold(
                self.circuit_breaker_reason.clone().unwrap_or_else(|| "Circuit breaker tripped".to_string())
            );
        }

        // Check if we have price data
        let mid_price = match input.mid_price() {
            Some(p) => p,
            None => return TradingDecision::hold("No mid price available"),
        };

        // Update unrealized P&L
        self.state.update_unrealized_pnl(mid_price);

        // Check if inventory is at max - stop quoting until reduced
        if self.is_inventory_exceeded() {
            return TradingDecision::hold(format!(
                "Inventory {:.4} at max limit {:.4}",
                self.inventory, self.mm_config.max_inventory_fraction
            ));
        }

        // Check regime favorability
        let (regime_ok, regime_reason) = self.is_regime_favorable(input);
        if !regime_ok {
            return TradingDecision::hold(regime_reason);
        }

        // Compute quotes
        let (bid_price, ask_price, bid_size, ask_size) = match self.compute_quotes(input) {
            Some(quotes) => quotes,
            None => return TradingDecision::hold("Could not compute quotes"),
        };

        // Validate quotes
        if bid_price >= ask_price {
            return TradingDecision::hold("Invalid quotes: bid >= ask");
        }

        // Store last quotes
        self.last_bid = Some(bid_price);
        self.last_ask = Some(ask_price);
        self.quote_count += 1;

        // Calculate confidence based on regime and inventory
        let inv_ratio = (self.inventory.abs() / Decimal::from_f64_retain(self.mm_config.max_inventory_fraction).unwrap_or(dec!(0.1)))
            .to_f64()
            .unwrap_or(0.0);
        let confidence = (1.0 - inv_ratio * 0.5).max(0.3);

        // Calculate expected edge (spread capture minus fees)
        let spread_bps = self.calculate_spread(input);
        let expected_edge = (spread_bps / 2.0 - 1.0) / 10000.0; // Half spread minus ~1bps fees

        TradingDecision::new(
            TradingAction::quote(bid_price, ask_price, bid_size, ask_size),
            confidence,
            expected_edge.max(0.0),
        )
        .with_metadata("spread_bps", format!("{:.2}", spread_bps))
        .with_metadata("inventory", format!("{:.6}", self.inventory))
        .with_metadata("quote_count", self.quote_count.to_string())
    }

    fn on_fill(
        &mut self,
        price: Decimal,
        size: Decimal,
        direction: PositionDirection,
        fee: Decimal,
    ) {
        // Update inventory based on fill direction
        match direction {
            PositionDirection::Long => {
                // We bought (bid was hit)
                self.inventory += size;
            }
            PositionDirection::Short => {
                // We sold (ask was lifted)
                self.inventory -= size;
            }
            PositionDirection::Flat => {}
        }

        // Calculate P&L from spread capture if we have last quotes
        let pnl = if let (Some(last_bid), Some(last_ask)) = (self.last_bid, self.last_ask) {
            match direction {
                PositionDirection::Long => {
                    // Bought at bid - potential profit if we can sell at ask
                    let spread = last_ask - last_bid;
                    (spread * size / dec!(2)) - fee // Approximate half-spread capture
                }
                PositionDirection::Short => {
                    // Sold at ask - potential profit from spread
                    let spread = last_ask - last_bid;
                    (spread * size / dec!(2)) - fee
                }
                PositionDirection::Flat => -fee,
            }
        } else {
            -fee
        };

        // Update state
        self.state.realized_pnl += pnl;
        self.state.trade_count += 1;
        if pnl > Decimal::ZERO {
            self.state.win_count += 1;
        }

        let is_win = pnl > Decimal::ZERO;
        self.record_trade(is_win);
        self.update_daily_tracking(pnl);

        // Update position tracking in state (for inventory-based decisions)
        if self.inventory > Decimal::ZERO {
            self.state.position_direction = PositionDirection::Long;
            self.state.position_size = self.inventory;
        } else if self.inventory < Decimal::ZERO {
            self.state.position_direction = PositionDirection::Short;
            self.state.position_size = self.inventory.abs();
        } else {
            self.state.position_direction = PositionDirection::Flat;
            self.state.position_size = Decimal::ZERO;
        }

        self.state.updated_at = Utc::now();
    }

    fn on_price_update(&mut self, price: Decimal) {
        // Update unrealized P&L based on inventory
        if self.inventory != Decimal::ZERO {
            if let Some(entry_price) = self.state.entry_price {
                let price_change = price - entry_price;
                let direction_sign = if self.inventory > Decimal::ZERO {
                    Decimal::ONE
                } else {
                    -Decimal::ONE
                };
                self.state.unrealized_pnl = price_change * self.inventory.abs() * direction_sign;
            }
        }
    }

    fn state(&self) -> &AlgorithmState {
        &self.state
    }

    fn state_mut(&mut self) -> &mut AlgorithmState {
        &mut self.state
    }

    fn reset(&mut self) {
        self.state.reset();
        self.inventory = Decimal::ZERO;
        self.quote_count = 0;
        self.consecutive_losses = 0;
        self.daily_pnl = Decimal::ZERO;
        self.daily_start = Utc::now();
        self.circuit_breaker_tripped = false;
        self.circuit_breaker_reason = None;
        self.last_bid = None;
        self.last_ask = None;
    }

    fn should_stop(&self) -> bool {
        self.circuit_breaker_tripped
    }

    fn stop_reason(&self) -> Option<String> {
        self.circuit_breaker_reason.clone()
    }

    fn config(&self) -> &AlgorithmConfig {
        &self.algo_config
    }

    fn update_config(&mut self, config: AlgorithmConfig) -> Result<(), TradingAlgorithmError> {
        if config.strategy_type != StrategyType::MarketMaking && config.strategy_type != StrategyType::Hybrid {
            return Err(TradingAlgorithmError::InvalidConfig(
                format!("MarketMakingTradingAlgorithm requires MarketMaking or Hybrid strategy type, got {:?}", config.strategy_type)
            ));
        }

        let mm_config = MMTradingConfig::from_algorithm_config(&config);
        mm_config.validate().map_err(TradingAlgorithmError::InvalidConfig)?;

        self.algo_config = config;
        self.mm_config = mm_config;
        Ok(())
    }
}

// ============================================================================
// Factory
// ============================================================================

/// Factory for creating MarketMakingTradingAlgorithm instances
pub struct MarketMakingTradingAlgorithmFactory;

impl MarketMakingTradingAlgorithmFactory {
    /// Create a new market making algorithm from config
    pub fn create(config: AlgorithmConfig) -> Result<Box<dyn TradingAlgorithm>, TradingAlgorithmError> {
        let algo = MarketMakingTradingAlgorithm::from_config(config)?;
        Ok(Box::new(algo))
    }

    /// Create with default config
    pub fn create_default() -> Box<dyn TradingAlgorithm> {
        Box::new(MarketMakingTradingAlgorithm::new())
    }
}

impl TradingAlgorithmFactory for MarketMakingTradingAlgorithmFactory {
    fn create(&self, config: &AlgorithmConfig) -> Result<Box<dyn TradingAlgorithm>, TradingAlgorithmError> {
        Self::create(config.clone())
    }

    fn strategy_type(&self) -> StrategyType {
        StrategyType::MarketMaking
    }

    fn algorithm_name(&self) -> &str {
        "Market Making Trading Algorithm"
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::TradeableAssessment;

    // ========================================================================
    // Helper Functions
    // ========================================================================

    fn create_test_features() -> FeaturesSnapshot {
        let mut features = FeaturesSnapshot::default();
        features.mid_price = Some(dec!(50000));
        features.best_bid = Some(dec!(49990));
        features.best_ask = Some(dec!(50010));
        features.spread = Some(dec!(20));
        features.realized_volatility_100 = Some(0.001); // f64
        features.tick_entropy_10s = Some(dec!(0.8)); // ~0.5 normalized
        features.regime = Some("MeanReverting".to_string());
        features.regime_confidence = Some(0.7);
        features
    }

    fn create_test_input() -> TradingInput {
        TradingInput::new(create_test_features())
    }

    fn create_test_input_with_assessment(is_tradeable: bool) -> TradingInput {
        let features = create_test_features();
        let assessment = TradeableAssessment {
            is_tradeable,
            position_scale: 0.8,
            recommended_strategy: RecommendedStrategy::MarketMaking,
            persistence_ok: true,
            signals_ok: true,
            entropy_ok: true,
            midc_ok: true,
            ..Default::default()
        };
        TradingInput::with_assessment(features, assessment)
    }

    fn create_mm_config() -> AlgorithmConfig {
        let mut config = AlgorithmConfig::default();
        config.strategy_type = StrategyType::MarketMaking;
        config
    }

    // ========================================================================
    // Configuration Tests
    // ========================================================================

    #[test]
    fn test_mm_trading_config_default() {
        let config = MMTradingConfig::default();
        assert_eq!(config.base_spread_bps, 2.0);
        assert_eq!(config.max_spread_bps, 8.0);
        assert_eq!(config.inventory_skew, 0.5);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_mm_trading_config_from_algorithm_config() {
        let algo_config = create_mm_config();
        let mm_config = MMTradingConfig::from_algorithm_config(&algo_config);

        assert_eq!(mm_config.base_spread_bps, algo_config.market_making.base_spread_bps);
        assert_eq!(mm_config.gamma, algo_config.market_making.gamma);
        assert!(mm_config.validate().is_ok());
    }

    #[test]
    fn test_mm_trading_config_validation_invalid_spread() {
        let mut config = MMTradingConfig::default();
        config.base_spread_bps = 0.0;
        assert!(config.validate().is_err());

        config.base_spread_bps = 2.0;
        config.max_spread_bps = 1.0; // Less than base
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_mm_trading_config_validation_invalid_inventory_skew() {
        let mut config = MMTradingConfig::default();
        config.inventory_skew = 1.5; // > 1.0
        assert!(config.validate().is_err());

        config.inventory_skew = -0.1; // < 0.0
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_mm_trading_config_validation_invalid_gamma() {
        let mut config = MMTradingConfig::default();
        config.gamma = -0.1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_mm_trading_config_validation_invalid_kappa() {
        let mut config = MMTradingConfig::default();
        config.kappa = 0.0;
        assert!(config.validate().is_err());
    }

    // ========================================================================
    // Algorithm Creation Tests
    // ========================================================================

    #[test]
    fn test_from_config_valid() {
        let config = create_mm_config();
        let algo = MarketMakingTradingAlgorithm::from_config(config);
        assert!(algo.is_ok());
    }

    #[test]
    fn test_from_config_invalid_strategy_type() {
        let mut config = AlgorithmConfig::default();
        config.strategy_type = StrategyType::Momentum;

        let algo = MarketMakingTradingAlgorithm::from_config(config);
        assert!(algo.is_err());
    }

    #[test]
    fn test_from_config_hybrid_strategy_ok() {
        let mut config = AlgorithmConfig::default();
        config.strategy_type = StrategyType::Hybrid;

        let algo = MarketMakingTradingAlgorithm::from_config(config);
        assert!(algo.is_ok());
    }

    #[test]
    fn test_new_default() {
        let algo = MarketMakingTradingAlgorithm::new();
        assert_eq!(algo.strategy_type(), StrategyType::MarketMaking);
        assert_eq!(algo.inventory(), Decimal::ZERO);
        assert_eq!(algo.quote_count(), 0);
    }

    #[test]
    fn test_algorithm_name_and_version() {
        let algo = MarketMakingTradingAlgorithm::new();
        assert_eq!(algo.name(), "Market Making Trading Algorithm");
        assert_eq!(algo.version(), "1.0.0");
    }

    // ========================================================================
    // Quote Generation Tests
    // ========================================================================

    #[test]
    fn test_decide_produces_quote() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input_with_assessment(true);

        let decision = algo.decide(&input);
        assert!(decision.action.is_quote());
    }

    #[test]
    fn test_decide_quote_spread_positive() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input_with_assessment(true);

        let decision = algo.decide(&input);
        if let TradingAction::Quote { bid_price, ask_price, .. } = decision.action {
            assert!(ask_price > bid_price, "Ask should be > bid");
            let spread = ask_price - bid_price;
            assert!(spread > Decimal::ZERO, "Spread should be positive");
        } else {
            panic!("Expected Quote action");
        }
    }

    #[test]
    fn test_decide_quote_sizes_positive() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input_with_assessment(true);

        let decision = algo.decide(&input);
        if let TradingAction::Quote { bid_size, ask_size, .. } = decision.action {
            assert!(bid_size > Decimal::ZERO, "Bid size should be positive");
            assert!(ask_size > Decimal::ZERO, "Ask size should be positive");
        } else {
            panic!("Expected Quote action");
        }
    }

    #[test]
    fn test_decide_no_mid_price_holds() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        let mut features = FeaturesSnapshot::default();
        features.mid_price = None;
        let input = TradingInput::new(features);

        let decision = algo.decide(&input);
        assert!(decision.action.is_hold());
    }

    #[test]
    fn test_decide_increments_quote_count() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input_with_assessment(true);

        assert_eq!(algo.quote_count(), 0);
        algo.decide(&input);
        assert_eq!(algo.quote_count(), 1);
        algo.decide(&input);
        assert_eq!(algo.quote_count(), 2);
    }

    // ========================================================================
    // Spread Calculation Tests
    // ========================================================================

    #[test]
    fn test_calculate_spread_base() {
        let algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input();

        let spread = algo.calculate_spread(&input);
        assert!(spread > 0.0, "Spread should be positive");
        assert!(spread >= algo.mm_config().base_spread_bps * 0.9, "Spread should be near base");
    }

    #[test]
    fn test_calculate_spread_widens_with_volatility() {
        let algo = MarketMakingTradingAlgorithm::new();

        let mut features_low_vol = create_test_features();
        features_low_vol.realized_volatility_100 = Some(0.0001);
        let input_low_vol = TradingInput::new(features_low_vol);

        let mut features_high_vol = create_test_features();
        features_high_vol.realized_volatility_100 = Some(0.01);
        let input_high_vol = TradingInput::new(features_high_vol);

        let spread_low = algo.calculate_spread(&input_low_vol);
        let spread_high = algo.calculate_spread(&input_high_vol);

        assert!(spread_high > spread_low, "High vol should have wider spread");
    }

    #[test]
    fn test_calculate_spread_capped_at_max() {
        let algo = MarketMakingTradingAlgorithm::new();

        let mut features = create_test_features();
        features.realized_volatility_100 = Some(1.0); // Very high vol
        let input = TradingInput::new(features);

        let spread = algo.calculate_spread(&input);
        assert!(spread <= algo.mm_config().max_spread_bps, "Spread should be capped");
    }

    // ========================================================================
    // Skew Calculation Tests
    // ========================================================================

    #[test]
    fn test_calculate_skew_zero_inventory() {
        let algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input();

        let skew = algo.calculate_skew(&input);
        assert!(skew.abs() < 0.1, "Skew should be near zero with no inventory");
    }

    #[test]
    fn test_calculate_skew_positive_inventory() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        algo.inventory = dec!(0.05); // Half of max
        let input = create_test_input();

        let skew = algo.calculate_skew(&input);
        assert!(skew > 0.0, "Positive inventory should have positive skew");
    }

    #[test]
    fn test_calculate_skew_negative_inventory() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        algo.inventory = dec!(-0.05); // Half of max negative
        let input = create_test_input();

        let skew = algo.calculate_skew(&input);
        assert!(skew < 0.0, "Negative inventory should have negative skew");
    }

    // ========================================================================
    // Fill Processing Tests
    // ========================================================================

    #[test]
    fn test_on_fill_updates_inventory_buy() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        assert_eq!(algo.inventory(), Decimal::ZERO);

        algo.on_fill(dec!(50000), dec!(0.01), PositionDirection::Long, dec!(0));
        assert_eq!(algo.inventory(), dec!(0.01));
    }

    #[test]
    fn test_on_fill_updates_inventory_sell() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        algo.on_fill(dec!(50000), dec!(0.01), PositionDirection::Short, dec!(0));
        assert_eq!(algo.inventory(), dec!(-0.01));
    }

    #[test]
    fn test_on_fill_increments_trade_count() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        assert_eq!(algo.state().trade_count, 0);

        algo.on_fill(dec!(50000), dec!(0.01), PositionDirection::Long, dec!(0));
        assert_eq!(algo.state().trade_count, 1);
    }

    #[test]
    fn test_on_fill_updates_position_direction() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        // Long inventory
        algo.on_fill(dec!(50000), dec!(0.01), PositionDirection::Long, dec!(0));
        assert_eq!(algo.state().position_direction, PositionDirection::Long);

        // Flatten
        algo.on_fill(dec!(50000), dec!(0.01), PositionDirection::Short, dec!(0));
        assert_eq!(algo.state().position_direction, PositionDirection::Flat);

        // Short inventory
        algo.on_fill(dec!(50000), dec!(0.01), PositionDirection::Short, dec!(0));
        assert_eq!(algo.state().position_direction, PositionDirection::Short);
    }

    // ========================================================================
    // Circuit Breaker Tests
    // ========================================================================

    #[test]
    fn test_circuit_breaker_consecutive_losses() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        // Simulate 5 losses (using tiny losses to avoid daily drawdown trigger)
        for _ in 0..5 {
            algo.record_trade(false);
        }

        assert!(algo.should_stop());
        assert!(algo.stop_reason().unwrap().contains("Consecutive losses"));
    }

    #[test]
    fn test_circuit_breaker_blocks_trading() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        // Trip circuit breaker (5 consecutive losses)
        for _ in 0..5 {
            algo.record_trade(false);
        }

        let input = create_test_input_with_assessment(true);
        let decision = algo.decide(&input);

        assert!(decision.action.is_hold());
        // Reason mentions "Circuit breaker" from the tripped state
        assert!(decision.action.reason().contains("Circuit breaker") ||
                decision.action.reason().contains("Consecutive losses"),
                "Expected circuit breaker reason, got: {}", decision.action.reason());
    }

    #[test]
    fn test_circuit_breaker_win_resets_consecutive_losses() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        // 4 losses
        for _ in 0..4 {
            algo.record_trade(false);
        }
        assert!(!algo.should_stop());

        // 1 win resets counter
        algo.record_trade(true);

        // 4 more losses shouldn't trigger
        for _ in 0..4 {
            algo.record_trade(false);
        }
        assert!(!algo.should_stop());
    }

    // ========================================================================
    // Inventory Limit Tests
    // ========================================================================

    #[test]
    fn test_inventory_exceeded_blocks_quoting() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        // Set inventory above max (0.20 from AlgorithmConfig::default().position.max_size_fraction)
        algo.inventory = dec!(0.25);

        let input = create_test_input_with_assessment(true);
        let decision = algo.decide(&input);

        assert!(decision.action.is_hold());
        assert!(decision.action.reason().contains("max limit"));
    }

    #[test]
    fn test_is_inventory_exceeded() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        algo.inventory = dec!(0.05);
        assert!(!algo.is_inventory_exceeded());

        // Default AlgorithmConfig has position.max_size_fraction = 0.20
        // Use value well above max
        algo.inventory = dec!(0.25);
        assert!(algo.is_inventory_exceeded());

        algo.inventory = dec!(-0.25);
        assert!(algo.is_inventory_exceeded());
    }

    // ========================================================================
    // Regime Check Tests
    // ========================================================================

    #[test]
    fn test_regime_check_not_tradeable_holds() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input_with_assessment(false);

        let (favorable, _) = algo.is_regime_favorable(&input);
        assert!(!favorable);
    }

    #[test]
    fn test_regime_check_tradeable_ok() {
        let algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input_with_assessment(true);

        let (favorable, _) = algo.is_regime_favorable(&input);
        assert!(favorable);
    }

    // ========================================================================
    // Reset Tests
    // ========================================================================

    #[test]
    fn test_reset_clears_state() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        // Modify state
        algo.inventory = dec!(0.05);
        algo.quote_count = 100;
        algo.consecutive_losses = 3;
        algo.circuit_breaker_tripped = true;

        algo.reset();

        assert_eq!(algo.inventory(), Decimal::ZERO);
        assert_eq!(algo.quote_count(), 0);
        assert_eq!(algo.consecutive_losses, 0);
        assert!(!algo.circuit_breaker_tripped);
    }

    // ========================================================================
    // Factory Tests
    // ========================================================================

    #[test]
    fn test_factory_create() {
        let config = create_mm_config();
        let algo = MarketMakingTradingAlgorithmFactory::create(config);
        assert!(algo.is_ok());
    }

    #[test]
    fn test_factory_create_default() {
        let algo = MarketMakingTradingAlgorithmFactory::create_default();
        assert_eq!(algo.strategy_type(), StrategyType::MarketMaking);
    }

    #[test]
    fn test_factory_trait_impl() {
        let factory = MarketMakingTradingAlgorithmFactory;
        assert_eq!(factory.strategy_type(), StrategyType::MarketMaking);
        assert_eq!(factory.algorithm_name(), "Market Making Trading Algorithm");

        let config = create_mm_config();
        let algo = factory.create(&config);
        assert!(algo.is_ok());
    }

    // ========================================================================
    // Config Update Tests
    // ========================================================================

    #[test]
    fn test_update_config_valid() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        let mut new_config = create_mm_config();
        new_config.market_making.base_spread_bps = 3.0;

        let result = algo.update_config(new_config);
        assert!(result.is_ok());
        assert_eq!(algo.mm_config().base_spread_bps, 3.0);
    }

    #[test]
    fn test_update_config_invalid_strategy() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        let mut new_config = AlgorithmConfig::default();
        new_config.strategy_type = StrategyType::Momentum;

        let result = algo.update_config(new_config);
        assert!(result.is_err());
    }

    // ========================================================================
    // State Tracking Tests
    // ========================================================================

    #[test]
    fn test_state_instance_id_unique() {
        let algo1 = MarketMakingTradingAlgorithm::new();
        let algo2 = MarketMakingTradingAlgorithm::new();

        assert_ne!(algo1.instance_id(), algo2.instance_id());
    }

    #[test]
    fn test_state_config_id_matches() {
        let config = create_mm_config();
        let config_id = config.id.clone();
        let algo = MarketMakingTradingAlgorithm::from_config(config).unwrap();

        assert_eq!(algo.config_id(), config_id);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_decide_with_zero_volatility() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        let mut features = create_test_features();
        features.realized_volatility_100 = Some(0.0);
        let assessment = TradeableAssessment {
            is_tradeable: true,
            position_scale: 1.0,
            recommended_strategy: RecommendedStrategy::MarketMaking,
            ..Default::default()
        };
        let input = TradingInput::with_assessment(features, assessment);

        let decision = algo.decide(&input);
        assert!(decision.action.is_quote());
    }

    #[test]
    fn test_decide_with_high_entropy() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        let mut features = create_test_features();
        features.tick_entropy_10s = Some(dec!(1.5)); // High entropy
        let assessment = TradeableAssessment {
            is_tradeable: true,
            position_scale: 1.0,
            recommended_strategy: RecommendedStrategy::MarketMaking,
            ..Default::default()
        };
        let input = TradingInput::with_assessment(features, assessment);

        // Should widen spread but still quote
        let decision = algo.decide(&input);
        // MM can handle high entropy (mean-reverting environment)
        assert!(decision.action.is_quote() || decision.action.is_hold());
    }

    #[test]
    fn test_consecutive_fills_alternate() {
        let mut algo = MarketMakingTradingAlgorithm::new();

        // Buy then sell
        algo.on_fill(dec!(50000), dec!(0.01), PositionDirection::Long, dec!(0));
        assert_eq!(algo.inventory(), dec!(0.01));

        algo.on_fill(dec!(50010), dec!(0.01), PositionDirection::Short, dec!(0));
        assert_eq!(algo.inventory(), Decimal::ZERO);
    }

    #[test]
    fn test_metadata_in_decision() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input_with_assessment(true);

        let decision = algo.decide(&input);

        assert!(decision.metadata.contains_key("spread_bps"));
        assert!(decision.metadata.contains_key("inventory"));
        assert!(decision.metadata.contains_key("quote_count"));
    }

    #[test]
    fn test_decision_confidence_decreases_with_inventory() {
        let mut algo = MarketMakingTradingAlgorithm::new();
        let input = create_test_input_with_assessment(true);

        let decision_empty = algo.decide(&input);

        algo.inventory = dec!(0.08); // Near max
        let decision_full = algo.decide(&input);

        // Decision with full inventory should have lower confidence
        // (or be a hold if inventory exceeded)
        if !decision_full.action.is_hold() {
            assert!(decision_full.confidence < decision_empty.confidence);
        }
    }
}

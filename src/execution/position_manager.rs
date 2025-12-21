//! Position Manager
//!
//! Manages position sizing, exposure limits, and portfolio-level position tracking.
//! Works alongside OCOManager to provide comprehensive risk management for directional trades.
//!
//! # Features
//!
//! - Position sizing based on volatility (ATR-based)
//! - Maximum exposure limits (total and per-symbol)
//! - Net exposure tracking (long vs short)
//! - Position concentration limits
//! - Kelly criterion position sizing
//!
//! # Example
//!
//! ```rust,ignore
//! use ingestor::execution::position_manager::{PositionManager, PositionConfig, PositionSizeRequest};
//! use rust_decimal_macros::dec;
//!
//! let config = PositionConfig {
//!     max_total_exposure: dec!(10.0),     // Max 10 BTC total exposure
//!     max_single_position: dec!(2.0),     // Max 2 BTC per position
//!     max_net_exposure: dec!(5.0),        // Max 5 BTC net long or short
//!     risk_per_trade_pct: 0.01,           // Risk 1% of capital per trade
//!     base_capital: dec!(100000),         // $100k capital
//!     ..Default::default()
//! };
//!
//! let mut manager = PositionManager::new(config);
//!
//! let request = PositionSizeRequest {
//!     symbol: "BTCUSDT".to_string(),
//!     side: PositionSide::Long,
//!     entry_price: dec!(50000),
//!     stop_loss_price: dec!(49500),
//!     volatility: Some(0.02),
//! };
//!
//! match manager.calculate_position_size(&request) {
//!     Ok(size) => println!("Recommended size: {} BTC", size),
//!     Err(e) => println!("Cannot take position: {:?}", e),
//! }
//! ```

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// Side of a position
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PositionSide {
    Long,
    Short,
}

impl PositionSide {
    /// Returns the opposite side
    pub fn opposite(&self) -> Self {
        match self {
            PositionSide::Long => PositionSide::Short,
            PositionSide::Short => PositionSide::Long,
        }
    }
}

/// Configuration for position management
#[derive(Debug, Clone)]
pub struct PositionConfig {
    /// Maximum total exposure across all positions (in base currency units)
    pub max_total_exposure: Decimal,
    /// Maximum size for a single position
    pub max_single_position: Decimal,
    /// Maximum net exposure (long - short, absolute value)
    pub max_net_exposure: Decimal,
    /// Maximum number of concurrent positions
    pub max_positions: usize,
    /// Maximum exposure per symbol
    pub max_exposure_per_symbol: Decimal,
    /// Risk per trade as percentage of capital (0.01 = 1%)
    pub risk_per_trade_pct: f64,
    /// Base capital for position sizing calculations
    pub base_capital: Decimal,
    /// Minimum position size
    pub min_position_size: Decimal,
    /// Position size rounding (e.g., 0.001 for 3 decimal places)
    pub size_precision: Decimal,
    /// Maximum concentration per symbol (as fraction of total exposure)
    pub max_symbol_concentration: f64,
    /// Use Kelly criterion for position sizing
    pub use_kelly_criterion: bool,
    /// Kelly fraction (typically 0.25-0.5 for conservative Kelly)
    pub kelly_fraction: f64,
}

impl Default for PositionConfig {
    fn default() -> Self {
        Self {
            max_total_exposure: dec!(10.0),
            max_single_position: dec!(2.0),
            max_net_exposure: dec!(5.0),
            max_positions: 10,
            max_exposure_per_symbol: dec!(5.0),
            risk_per_trade_pct: 0.01,
            base_capital: dec!(100000),
            min_position_size: dec!(0.001),
            size_precision: dec!(0.001),
            max_symbol_concentration: 0.5,
            use_kelly_criterion: false,
            kelly_fraction: 0.25,
        }
    }
}

/// Request for position size calculation
#[derive(Debug, Clone)]
pub struct PositionSizeRequest {
    /// Trading symbol (e.g., "BTCUSDT")
    pub symbol: String,
    /// Position side
    pub side: PositionSide,
    /// Entry price
    pub entry_price: Decimal,
    /// Stop loss price
    pub stop_loss_price: Decimal,
    /// Current volatility (optional, for ATR-based sizing)
    pub volatility: Option<f64>,
    /// Expected win rate (for Kelly criterion)
    pub win_rate: Option<f64>,
    /// Expected risk-reward ratio (for Kelly criterion)
    pub risk_reward: Option<f64>,
}

/// Result of position size calculation
#[derive(Debug, Clone)]
pub struct PositionSizeResult {
    /// Recommended position size
    pub size: Decimal,
    /// Risk amount in quote currency
    pub risk_amount: Decimal,
    /// Stop loss distance in basis points
    pub sl_distance_bps: Decimal,
    /// Method used for sizing
    pub sizing_method: SizingMethod,
    /// Reason if size was reduced
    pub reduction_reason: Option<String>,
}

/// Method used for position sizing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizingMethod {
    /// Fixed risk per trade
    FixedRisk,
    /// ATR-based volatility sizing
    Volatility,
    /// Kelly criterion
    Kelly,
    /// Capped by exposure limit
    ExposureCapped,
    /// Minimum size
    Minimum,
}

/// An active position
#[derive(Debug, Clone)]
pub struct Position {
    /// Position identifier
    pub id: String,
    /// Trading symbol
    pub symbol: String,
    /// Position side
    pub side: PositionSide,
    /// Entry price
    pub entry_price: Decimal,
    /// Current size
    pub size: Decimal,
    /// Timestamp when opened
    pub opened_at: u64,
    /// Stop loss price
    pub stop_loss: Option<Decimal>,
    /// Take profit price
    pub take_profit: Option<Decimal>,
}

impl Position {
    /// Calculate unrealized P&L
    pub fn unrealized_pnl(&self, current_price: Decimal) -> Decimal {
        match self.side {
            PositionSide::Long => (current_price - self.entry_price) * self.size,
            PositionSide::Short => (self.entry_price - current_price) * self.size,
        }
    }

    /// Calculate unrealized P&L in basis points
    pub fn unrealized_pnl_bps(&self, current_price: Decimal) -> Decimal {
        let price_diff = match self.side {
            PositionSide::Long => current_price - self.entry_price,
            PositionSide::Short => self.entry_price - current_price,
        };
        (price_diff / self.entry_price) * dec!(10000)
    }
}

/// Position management errors
#[derive(Debug, Clone, PartialEq)]
pub enum PositionError {
    /// Maximum total exposure reached
    MaxExposureReached { current: Decimal, max: Decimal },
    /// Maximum positions reached
    MaxPositionsReached { current: usize, max: usize },
    /// Maximum net exposure reached
    MaxNetExposureReached { current: Decimal, max: Decimal },
    /// Maximum symbol exposure reached
    MaxSymbolExposureReached {
        symbol: String,
        current: Decimal,
        max: Decimal,
    },
    /// Maximum concentration reached
    MaxConcentrationReached {
        symbol: String,
        concentration: f64,
        max: f64,
    },
    /// Invalid stop loss
    InvalidStopLoss { entry: Decimal, stop_loss: Decimal },
    /// Position not found
    PositionNotFound { id: String },
    /// Size below minimum
    SizeBelowMinimum { size: Decimal, min: Decimal },
}

impl std::fmt::Display for PositionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionError::MaxExposureReached { current, max } => {
                write!(
                    f,
                    "Max exposure reached: current {} / max {}",
                    current, max
                )
            }
            PositionError::MaxPositionsReached { current, max } => {
                write!(
                    f,
                    "Max positions reached: current {} / max {}",
                    current, max
                )
            }
            PositionError::MaxNetExposureReached { current, max } => {
                write!(
                    f,
                    "Max net exposure reached: current {} / max {}",
                    current, max
                )
            }
            PositionError::MaxSymbolExposureReached {
                symbol,
                current,
                max,
            } => {
                write!(
                    f,
                    "Max exposure for {} reached: current {} / max {}",
                    symbol, current, max
                )
            }
            PositionError::MaxConcentrationReached {
                symbol,
                concentration,
                max,
            } => {
                write!(
                    f,
                    "Max concentration for {} reached: {:.2}% / {:.2}%",
                    symbol,
                    concentration * 100.0,
                    max * 100.0
                )
            }
            PositionError::InvalidStopLoss { entry, stop_loss } => {
                write!(
                    f,
                    "Invalid stop loss: entry {} stop_loss {}",
                    entry, stop_loss
                )
            }
            PositionError::PositionNotFound { id } => {
                write!(f, "Position not found: {}", id)
            }
            PositionError::SizeBelowMinimum { size, min } => {
                write!(f, "Size {} below minimum {}", size, min)
            }
        }
    }
}

impl std::error::Error for PositionError {}

/// Portfolio statistics
#[derive(Debug, Clone, Default)]
pub struct PortfolioStats {
    /// Total exposure (sum of all position sizes)
    pub total_exposure: Decimal,
    /// Net exposure (long - short)
    pub net_exposure: Decimal,
    /// Long exposure
    pub long_exposure: Decimal,
    /// Short exposure
    pub short_exposure: Decimal,
    /// Number of open positions
    pub position_count: usize,
    /// Number of long positions
    pub long_count: usize,
    /// Number of short positions
    pub short_count: usize,
    /// Unrealized P&L
    pub unrealized_pnl: Decimal,
    /// Largest position size
    pub largest_position: Decimal,
    /// Exposure per symbol
    pub symbol_exposure: HashMap<String, Decimal>,
}

/// Position Manager
#[derive(Debug)]
pub struct PositionManager {
    /// Configuration
    config: PositionConfig,
    /// Active positions
    positions: HashMap<String, Position>,
    /// Position counter for ID generation
    position_counter: u64,
}

impl Default for PositionManager {
    fn default() -> Self {
        Self::new(PositionConfig::default())
    }
}

impl PositionManager {
    /// Create a new position manager
    pub fn new(config: PositionConfig) -> Self {
        Self {
            config,
            positions: HashMap::new(),
            position_counter: 0,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &PositionConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&mut self, config: PositionConfig) {
        self.config = config;
    }

    /// Calculate position size for a trade
    pub fn calculate_position_size(
        &self,
        request: &PositionSizeRequest,
    ) -> Result<PositionSizeResult, PositionError> {
        // Validate stop loss
        self.validate_stop_loss(request)?;

        // Check position limits
        self.check_position_limits(request)?;

        // Calculate base size
        let (base_size, method) = self.calculate_base_size(request);

        // Apply exposure constraints
        let (constrained_size, reduction_reason) =
            self.apply_exposure_constraints(request, base_size)?;

        // Round to precision
        let final_size = self.round_size(constrained_size);

        // Check minimum
        if final_size < self.config.min_position_size {
            return Err(PositionError::SizeBelowMinimum {
                size: final_size,
                min: self.config.min_position_size,
            });
        }

        // Calculate risk amount
        let sl_distance = (request.entry_price - request.stop_loss_price).abs();
        let risk_amount = final_size * sl_distance;
        let sl_distance_bps = (sl_distance / request.entry_price) * dec!(10000);

        Ok(PositionSizeResult {
            size: final_size,
            risk_amount,
            sl_distance_bps,
            sizing_method: if reduction_reason.is_some() {
                SizingMethod::ExposureCapped
            } else {
                method
            },
            reduction_reason,
        })
    }

    /// Validate stop loss
    fn validate_stop_loss(&self, request: &PositionSizeRequest) -> Result<(), PositionError> {
        let valid = match request.side {
            PositionSide::Long => request.stop_loss_price < request.entry_price,
            PositionSide::Short => request.stop_loss_price > request.entry_price,
        };

        if !valid {
            return Err(PositionError::InvalidStopLoss {
                entry: request.entry_price,
                stop_loss: request.stop_loss_price,
            });
        }

        Ok(())
    }

    /// Check position limits before sizing
    fn check_position_limits(&self, request: &PositionSizeRequest) -> Result<(), PositionError> {
        let stats = self.portfolio_stats(request.entry_price);

        // Check max positions
        if stats.position_count >= self.config.max_positions {
            return Err(PositionError::MaxPositionsReached {
                current: stats.position_count,
                max: self.config.max_positions,
            });
        }

        // Check concentration
        if let Some(&symbol_exp) = stats.symbol_exposure.get(&request.symbol) {
            if stats.total_exposure > Decimal::ZERO {
                let concentration: f64 = (symbol_exp / stats.total_exposure)
                    .try_into()
                    .unwrap_or(0.0);
                if concentration >= self.config.max_symbol_concentration {
                    return Err(PositionError::MaxConcentrationReached {
                        symbol: request.symbol.clone(),
                        concentration,
                        max: self.config.max_symbol_concentration,
                    });
                }
            }
        }

        Ok(())
    }

    /// Calculate base position size
    fn calculate_base_size(&self, request: &PositionSizeRequest) -> (Decimal, SizingMethod) {
        // Kelly criterion if enabled and data available
        if self.config.use_kelly_criterion {
            if let (Some(win_rate), Some(risk_reward)) = (request.win_rate, request.risk_reward) {
                let kelly_size = self.kelly_position_size(
                    request.entry_price,
                    request.stop_loss_price,
                    win_rate,
                    risk_reward,
                );
                return (kelly_size, SizingMethod::Kelly);
            }
        }

        // Volatility-based sizing if volatility provided
        if let Some(volatility) = request.volatility {
            if volatility > 0.0 {
                let vol_size = self.volatility_position_size(
                    request.entry_price,
                    request.stop_loss_price,
                    volatility,
                );
                return (vol_size, SizingMethod::Volatility);
            }
        }

        // Default: fixed risk per trade
        let fixed_size =
            self.fixed_risk_position_size(request.entry_price, request.stop_loss_price);
        (fixed_size, SizingMethod::FixedRisk)
    }

    /// Fixed risk position sizing
    fn fixed_risk_position_size(&self, entry_price: Decimal, stop_loss_price: Decimal) -> Decimal {
        let sl_distance = (entry_price - stop_loss_price).abs();
        if sl_distance == Decimal::ZERO {
            return Decimal::ZERO;
        }

        let risk_amount = self.config.base_capital
            * Decimal::try_from(self.config.risk_per_trade_pct).unwrap_or(dec!(0.01));

        risk_amount / sl_distance
    }

    /// Volatility-adjusted position sizing
    fn volatility_position_size(
        &self,
        entry_price: Decimal,
        stop_loss_price: Decimal,
        volatility: f64,
    ) -> Decimal {
        // Adjust risk per trade based on volatility
        // Higher volatility = smaller position
        let base_volatility = 0.02; // 2% daily volatility as baseline
        let vol_factor = (base_volatility / volatility).min(2.0).max(0.5);

        let adjusted_risk = self.config.risk_per_trade_pct * vol_factor;
        let sl_distance = (entry_price - stop_loss_price).abs();

        if sl_distance == Decimal::ZERO {
            return Decimal::ZERO;
        }

        let risk_amount =
            self.config.base_capital * Decimal::try_from(adjusted_risk).unwrap_or(dec!(0.01));

        risk_amount / sl_distance
    }

    /// Kelly criterion position sizing
    fn kelly_position_size(
        &self,
        entry_price: Decimal,
        stop_loss_price: Decimal,
        win_rate: f64,
        risk_reward: f64,
    ) -> Decimal {
        // Kelly formula: f* = (bp - q) / b
        // where b = win/loss ratio, p = win probability, q = loss probability
        let b = risk_reward;
        let p = win_rate;
        let q = 1.0 - p;

        let kelly = (b * p - q) / b;
        let kelly = kelly.max(0.0); // Never negative

        // Apply Kelly fraction for conservative sizing
        let fractional_kelly = kelly * self.config.kelly_fraction;

        let sl_distance = (entry_price - stop_loss_price).abs();
        if sl_distance == Decimal::ZERO {
            return Decimal::ZERO;
        }

        let risk_amount =
            self.config.base_capital * Decimal::try_from(fractional_kelly).unwrap_or(Decimal::ZERO);

        risk_amount / sl_distance
    }

    /// Apply exposure constraints to size
    fn apply_exposure_constraints(
        &self,
        request: &PositionSizeRequest,
        size: Decimal,
    ) -> Result<(Decimal, Option<String>), PositionError> {
        let stats = self.portfolio_stats(request.entry_price);
        let mut final_size = size;
        let mut reason = None;

        // Cap by single position limit
        if final_size > self.config.max_single_position {
            final_size = self.config.max_single_position;
            reason = Some("Capped by max single position".to_string());
        }

        // Cap by total exposure
        let remaining_exposure = self.config.max_total_exposure - stats.total_exposure;
        if final_size > remaining_exposure {
            if remaining_exposure <= Decimal::ZERO {
                return Err(PositionError::MaxExposureReached {
                    current: stats.total_exposure,
                    max: self.config.max_total_exposure,
                });
            }
            final_size = remaining_exposure;
            reason = Some("Capped by max total exposure".to_string());
        }

        // Cap by net exposure
        let new_net = match request.side {
            PositionSide::Long => stats.net_exposure + final_size,
            PositionSide::Short => stats.net_exposure - final_size,
        };
        if new_net.abs() > self.config.max_net_exposure {
            let available = match request.side {
                PositionSide::Long => self.config.max_net_exposure - stats.net_exposure,
                PositionSide::Short => self.config.max_net_exposure + stats.net_exposure,
            };
            if available <= Decimal::ZERO {
                return Err(PositionError::MaxNetExposureReached {
                    current: stats.net_exposure,
                    max: self.config.max_net_exposure,
                });
            }
            final_size = final_size.min(available);
            reason = Some("Capped by max net exposure".to_string());
        }

        // Cap by symbol exposure
        let current_symbol_exp = stats
            .symbol_exposure
            .get(&request.symbol)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let remaining_symbol = self.config.max_exposure_per_symbol - current_symbol_exp;
        if final_size > remaining_symbol {
            if remaining_symbol <= Decimal::ZERO {
                return Err(PositionError::MaxSymbolExposureReached {
                    symbol: request.symbol.clone(),
                    current: current_symbol_exp,
                    max: self.config.max_exposure_per_symbol,
                });
            }
            final_size = remaining_symbol;
            reason = Some(format!("Capped by max {} exposure", request.symbol));
        }

        Ok((final_size, reason))
    }

    /// Round size to precision
    fn round_size(&self, size: Decimal) -> Decimal {
        if self.config.size_precision == Decimal::ZERO {
            return size;
        }
        (size / self.config.size_precision).floor() * self.config.size_precision
    }

    /// Add a position
    pub fn add_position(&mut self, position: Position) -> Result<(), PositionError> {
        self.positions.insert(position.id.clone(), position);
        Ok(())
    }

    /// Open a new position
    pub fn open_position(
        &mut self,
        symbol: String,
        side: PositionSide,
        entry_price: Decimal,
        size: Decimal,
        stop_loss: Option<Decimal>,
        take_profit: Option<Decimal>,
    ) -> String {
        self.position_counter += 1;
        let id = format!("pos_{}", self.position_counter);

        let position = Position {
            id: id.clone(),
            symbol,
            side,
            entry_price,
            size,
            opened_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            stop_loss,
            take_profit,
        };

        self.positions.insert(id.clone(), position);
        id
    }

    /// Close a position
    pub fn close_position(&mut self, id: &str) -> Result<Position, PositionError> {
        self.positions
            .remove(id)
            .ok_or(PositionError::PositionNotFound { id: id.to_string() })
    }

    /// Get a position
    pub fn get_position(&self, id: &str) -> Option<&Position> {
        self.positions.get(id)
    }

    /// Get all positions
    pub fn positions(&self) -> impl Iterator<Item = &Position> {
        self.positions.values()
    }

    /// Get positions for a symbol
    pub fn positions_for_symbol(&self, symbol: &str) -> Vec<&Position> {
        self.positions
            .values()
            .filter(|p| p.symbol == symbol)
            .collect()
    }

    /// Get portfolio statistics
    pub fn portfolio_stats(&self, current_price: Decimal) -> PortfolioStats {
        let mut stats = PortfolioStats::default();

        for position in self.positions.values() {
            stats.total_exposure += position.size;
            stats.position_count += 1;

            match position.side {
                PositionSide::Long => {
                    stats.long_exposure += position.size;
                    stats.long_count += 1;
                    stats.net_exposure += position.size;
                }
                PositionSide::Short => {
                    stats.short_exposure += position.size;
                    stats.short_count += 1;
                    stats.net_exposure -= position.size;
                }
            }

            stats.unrealized_pnl += position.unrealized_pnl(current_price);

            if position.size > stats.largest_position {
                stats.largest_position = position.size;
            }

            *stats
                .symbol_exposure
                .entry(position.symbol.clone())
                .or_insert(Decimal::ZERO) += position.size;
        }

        stats
    }

    /// Check if can open a new position
    pub fn can_open_position(&self, request: &PositionSizeRequest) -> Result<(), PositionError> {
        self.validate_stop_loss(request)?;
        self.check_position_limits(request)?;
        Ok(())
    }

    /// Get remaining exposure capacity
    pub fn remaining_exposure(&self) -> Decimal {
        let stats = self.portfolio_stats(Decimal::ZERO);
        (self.config.max_total_exposure - stats.total_exposure).max(Decimal::ZERO)
    }

    /// Get remaining net exposure capacity for a side
    pub fn remaining_net_exposure(&self, side: PositionSide) -> Decimal {
        let stats = self.portfolio_stats(Decimal::ZERO);
        match side {
            PositionSide::Long => {
                (self.config.max_net_exposure - stats.net_exposure).max(Decimal::ZERO)
            }
            PositionSide::Short => {
                (self.config.max_net_exposure + stats.net_exposure).max(Decimal::ZERO)
            }
        }
    }

    /// Clear all positions
    pub fn clear(&mut self) {
        self.positions.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PositionConfig {
        PositionConfig {
            max_total_exposure: dec!(10.0),
            max_single_position: dec!(2.0),
            max_net_exposure: dec!(5.0),
            max_positions: 5,
            max_exposure_per_symbol: dec!(5.0),
            risk_per_trade_pct: 0.01,
            base_capital: dec!(100000),
            min_position_size: dec!(0.001),
            size_precision: dec!(0.001),
            max_symbol_concentration: 0.5,
            use_kelly_criterion: false,
            kelly_fraction: 0.25,
        }
    }

    #[test]
    fn test_position_side_opposite() {
        assert_eq!(PositionSide::Long.opposite(), PositionSide::Short);
        assert_eq!(PositionSide::Short.opposite(), PositionSide::Long);
    }

    #[test]
    fn test_fixed_risk_position_sizing() {
        let manager = PositionManager::new(test_config());

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49500), // 100 bps SL
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request).unwrap();

        // Risk 1% of 100k = 1000
        // SL distance = 500
        // Size = 1000 / 500 = 2.0
        assert_eq!(result.size, dec!(2.0));
        assert_eq!(result.sizing_method, SizingMethod::FixedRisk);
    }

    #[test]
    fn test_volatility_position_sizing() {
        let manager = PositionManager::new(test_config());

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49500),
            volatility: Some(0.04), // High vol = smaller position
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request).unwrap();

        // Vol factor = 0.02 / 0.04 = 0.5
        // Adjusted risk = 0.01 * 0.5 = 0.005
        // Risk amount = 100k * 0.005 = 500
        // Size = 500 / 500 = 1.0
        assert_eq!(result.size, dec!(1.0));
        assert_eq!(result.sizing_method, SizingMethod::Volatility);
    }

    #[test]
    fn test_kelly_position_sizing() {
        let mut config = test_config();
        config.use_kelly_criterion = true;
        config.kelly_fraction = 0.25;
        config.max_single_position = dec!(100.0);  // Remove single position cap for this test
        config.max_total_exposure = dec!(100.0);   // Remove total exposure cap
        config.max_net_exposure = dec!(100.0);     // Remove net exposure cap
        config.max_exposure_per_symbol = dec!(100.0); // Remove symbol exposure cap
        let manager = PositionManager::new(config);

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49500),
            volatility: None,
            win_rate: Some(0.6),   // 60% win rate
            risk_reward: Some(2.0), // 2:1 R:R
        };

        let result = manager.calculate_position_size(&request).unwrap();
        assert_eq!(result.sizing_method, SizingMethod::Kelly);
        // Kelly: (2*0.6 - 0.4) / 2 = 0.4
        // Fractional: 0.4 * 0.25 = 0.1
        // Risk: 100k * 0.1 = 10k
        // Size: 10k / 500 = 20
        assert_eq!(result.size, dec!(20.0));
    }

    #[test]
    fn test_max_single_position_cap() {
        let manager = PositionManager::new(test_config());

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49990), // Very tight SL = large size
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request).unwrap();

        // Without cap: 1000 / 10 = 100
        // With cap: 2.0
        assert_eq!(result.size, dec!(2.0));
        assert_eq!(result.sizing_method, SizingMethod::ExposureCapped);
    }

    #[test]
    fn test_invalid_stop_loss_long() {
        let manager = PositionManager::new(test_config());

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(50100), // SL above entry for long
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request);
        assert!(matches!(result, Err(PositionError::InvalidStopLoss { .. })));
    }

    #[test]
    fn test_invalid_stop_loss_short() {
        let manager = PositionManager::new(test_config());

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Short,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49900), // SL below entry for short
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request);
        assert!(matches!(result, Err(PositionError::InvalidStopLoss { .. })));
    }

    #[test]
    fn test_max_positions_limit() {
        let mut manager = PositionManager::new(test_config());

        // Add 5 positions (max)
        for i in 0..5 {
            manager.open_position(
                format!("BTC{}", i),
                PositionSide::Long,
                dec!(50000),
                dec!(1.0),
                None,
                None,
            );
        }

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49500),
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request);
        assert!(matches!(
            result,
            Err(PositionError::MaxPositionsReached { .. })
        ));
    }

    #[test]
    fn test_max_exposure_limit() {
        let mut manager = PositionManager::new(test_config());

        // Add position using up all exposure
        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(10.0), // Max total exposure
            None,
            None,
        );

        let request = PositionSizeRequest {
            symbol: "ETHUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(3000),
            stop_loss_price: dec!(2970),
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request);
        assert!(matches!(
            result,
            Err(PositionError::MaxExposureReached { .. })
        ));
    }

    #[test]
    fn test_net_exposure_limit() {
        let mut manager = PositionManager::new(test_config());

        // Add long position at net exposure limit
        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(5.0), // Max net exposure
            None,
            None,
        );

        let request = PositionSizeRequest {
            symbol: "ETHUSDT".to_string(),
            side: PositionSide::Long, // Same direction
            entry_price: dec!(3000),
            stop_loss_price: dec!(2970),
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request);
        assert!(matches!(
            result,
            Err(PositionError::MaxNetExposureReached { .. })
        ));

        // But short should work (reduces net exposure)
        let short_request = PositionSizeRequest {
            symbol: "ETHUSDT".to_string(),
            side: PositionSide::Short,
            entry_price: dec!(3000),
            stop_loss_price: dec!(3030),
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&short_request);
        assert!(result.is_ok());
    }

    #[test]
    fn test_symbol_exposure_limit() {
        let mut config = test_config();
        // Raise net exposure limit so symbol exposure is the binding constraint
        config.max_net_exposure = dec!(20.0);
        config.max_total_exposure = dec!(20.0);
        // Raise concentration limit above 100% so it doesn't trigger before symbol exposure
        config.max_symbol_concentration = 1.1;
        let mut manager = PositionManager::new(config);

        // Add position at symbol limit
        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(5.0), // Max per symbol
            None,
            None,
        );

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(), // Same symbol
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49500),
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request);
        assert!(
            matches!(
                result,
                Err(PositionError::MaxSymbolExposureReached { .. })
            ),
            "Expected MaxSymbolExposureReached, got {:?}",
            result
        );
    }

    #[test]
    fn test_position_unrealized_pnl() {
        let position = Position {
            id: "test".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            size: dec!(2.0),
            opened_at: 0,
            stop_loss: None,
            take_profit: None,
        };

        // Price up
        assert_eq!(position.unrealized_pnl(dec!(50100)), dec!(200));
        // Price down
        assert_eq!(position.unrealized_pnl(dec!(49900)), dec!(-200));
    }

    #[test]
    fn test_position_unrealized_pnl_short() {
        let position = Position {
            id: "test".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Short,
            entry_price: dec!(50000),
            size: dec!(2.0),
            opened_at: 0,
            stop_loss: None,
            take_profit: None,
        };

        // Price down (profit for short)
        assert_eq!(position.unrealized_pnl(dec!(49900)), dec!(200));
        // Price up (loss for short)
        assert_eq!(position.unrealized_pnl(dec!(50100)), dec!(-200));
    }

    #[test]
    fn test_portfolio_stats() {
        let mut manager = PositionManager::new(test_config());

        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(2.0),
            None,
            None,
        );

        manager.open_position(
            "ETHUSDT".to_string(),
            PositionSide::Short,
            dec!(3000),
            dec!(1.0),
            None,
            None,
        );

        let stats = manager.portfolio_stats(dec!(50000));

        assert_eq!(stats.total_exposure, dec!(3.0));
        assert_eq!(stats.long_exposure, dec!(2.0));
        assert_eq!(stats.short_exposure, dec!(1.0));
        assert_eq!(stats.net_exposure, dec!(1.0)); // 2 - 1
        assert_eq!(stats.position_count, 2);
        assert_eq!(stats.long_count, 1);
        assert_eq!(stats.short_count, 1);
        assert_eq!(stats.largest_position, dec!(2.0));
    }

    #[test]
    fn test_open_and_close_position() {
        let mut manager = PositionManager::new(test_config());

        let id = manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(1.0),
            Some(dec!(49500)),
            Some(dec!(51000)),
        );

        assert!(manager.get_position(&id).is_some());

        let closed = manager.close_position(&id).unwrap();
        assert_eq!(closed.symbol, "BTCUSDT");
        assert!(manager.get_position(&id).is_none());
    }

    #[test]
    fn test_close_nonexistent_position() {
        let mut manager = PositionManager::new(test_config());
        let result = manager.close_position("nonexistent");
        assert!(matches!(result, Err(PositionError::PositionNotFound { .. })));
    }

    #[test]
    fn test_remaining_exposure() {
        let mut manager = PositionManager::new(test_config());

        assert_eq!(manager.remaining_exposure(), dec!(10.0));

        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(3.0),
            None,
            None,
        );

        assert_eq!(manager.remaining_exposure(), dec!(7.0));
    }

    #[test]
    fn test_remaining_net_exposure() {
        let mut manager = PositionManager::new(test_config());

        assert_eq!(manager.remaining_net_exposure(PositionSide::Long), dec!(5.0));
        assert_eq!(manager.remaining_net_exposure(PositionSide::Short), dec!(5.0));

        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(3.0),
            None,
            None,
        );

        // Net is now +3, so Long has 2 remaining, Short has 8
        assert_eq!(manager.remaining_net_exposure(PositionSide::Long), dec!(2.0));
        assert_eq!(manager.remaining_net_exposure(PositionSide::Short), dec!(8.0));
    }

    #[test]
    fn test_positions_for_symbol() {
        let mut manager = PositionManager::new(test_config());

        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(1.0),
            None,
            None,
        );
        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50100),
            dec!(1.0),
            None,
            None,
        );
        manager.open_position(
            "ETHUSDT".to_string(),
            PositionSide::Long,
            dec!(3000),
            dec!(1.0),
            None,
            None,
        );

        let btc_positions = manager.positions_for_symbol("BTCUSDT");
        assert_eq!(btc_positions.len(), 2);

        let eth_positions = manager.positions_for_symbol("ETHUSDT");
        assert_eq!(eth_positions.len(), 1);
    }

    #[test]
    fn test_size_precision_rounding() {
        let mut config = test_config();
        config.size_precision = dec!(0.01);
        let manager = PositionManager::new(config);

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49900), // 20 bps SL
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request).unwrap();

        // Risk: 1000, SL: 100, Size: 10 -> capped to 2.0
        // Should be rounded to 0.01 precision
        assert_eq!(result.size, dec!(2.0));
    }

    #[test]
    fn test_minimum_size_error() {
        let mut config = test_config();
        config.min_position_size = dec!(1.0);
        config.base_capital = dec!(100); // Small capital
        let manager = PositionManager::new(config);

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49500),
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.calculate_position_size(&request);
        // Risk: 1, SL: 500, Size: 0.002 < min 1.0
        assert!(matches!(result, Err(PositionError::SizeBelowMinimum { .. })));
    }

    #[test]
    fn test_clear_positions() {
        let mut manager = PositionManager::new(test_config());

        manager.open_position(
            "BTCUSDT".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(1.0),
            None,
            None,
        );
        manager.open_position(
            "ETHUSDT".to_string(),
            PositionSide::Long,
            dec!(3000),
            dec!(1.0),
            None,
            None,
        );

        assert_eq!(manager.positions().count(), 2);

        manager.clear();

        assert_eq!(manager.positions().count(), 0);
    }

    #[test]
    fn test_can_open_position_validation() {
        let mut manager = PositionManager::new(test_config());

        // Fill up positions
        for i in 0..5 {
            manager.open_position(
                format!("SYM{}", i),
                PositionSide::Long,
                dec!(50000),
                dec!(1.0),
                None,
                None,
            );
        }

        let request = PositionSizeRequest {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            entry_price: dec!(50000),
            stop_loss_price: dec!(49500),
            volatility: None,
            win_rate: None,
            risk_reward: None,
        };

        let result = manager.can_open_position(&request);
        assert!(result.is_err());
    }
}

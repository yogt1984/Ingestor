//! OCO (One-Cancels-Other) Order Manager
//!
//! Manages take-profit and stop-loss orders for directional trades.
//! When either the take-profit or stop-loss is triggered, the other is automatically cancelled.
//!
//! # Design
//!
//! ```text
//! Entry → OCO Created → Price Movement → TP Hit → Position Closed, SL Cancelled
//!                                      → SL Hit → Position Closed, TP Cancelled
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use ingestor::execution::oco_manager::{OCOManager, OCOOrder, Side};
//! use rust_decimal_macros::dec;
//!
//! let mut manager = OCOManager::new();
//!
//! // Enter a long position with TP at +10 bps, SL at -5 bps
//! let order = OCOOrder::new(
//!     "order_1".to_string(),
//!     Side::Buy,
//!     dec!(50000.0),  // entry price
//!     dec!(1.0),      // size
//!     dec!(50050.0),  // take profit (+10 bps)
//!     dec!(49975.0),  // stop loss (-5 bps)
//! );
//!
//! manager.add_order(order);
//!
//! // Check for triggers on each price update
//! let triggers = manager.check_triggers(dec!(50060.0));
//! // Returns vec![OCOTrigger { order_id: "order_1", trigger_type: TakeProfit, ... }]
//! ```

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Side of the trade
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// Returns the opposite side
    pub fn opposite(&self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Type of OCO trigger
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerType {
    TakeProfit,
    StopLoss,
}

/// Result of an OCO trigger
#[derive(Debug, Clone)]
pub struct OCOTrigger {
    /// Unique order identifier
    pub order_id: String,
    /// What triggered the close
    pub trigger_type: TriggerType,
    /// Side of the original position
    pub side: Side,
    /// Entry price
    pub entry_price: Decimal,
    /// Exit price (TP or SL price)
    pub exit_price: Decimal,
    /// Position size
    pub size: Decimal,
    /// Realized P&L for this trade
    pub realized_pnl: Decimal,
    /// P&L in basis points
    pub pnl_bps: Decimal,
    /// Duration of the trade in milliseconds
    pub duration_ms: u64,
}

/// A single OCO order with take-profit and stop-loss levels
#[derive(Debug, Clone)]
pub struct OCOOrder {
    /// Unique identifier for this order
    pub id: String,
    /// Side of the position (Buy = long, Sell = short)
    pub side: Side,
    /// Entry price
    pub entry_price: Decimal,
    /// Position size
    pub size: Decimal,
    /// Take profit price
    pub take_profit_price: Decimal,
    /// Stop loss price
    pub stop_loss_price: Decimal,
    /// Timestamp when order was created (Unix ms)
    pub created_at: u64,
    /// Optional metadata
    pub metadata: Option<String>,
}

impl OCOOrder {
    /// Create a new OCO order
    pub fn new(
        id: String,
        side: Side,
        entry_price: Decimal,
        size: Decimal,
        take_profit_price: Decimal,
        stop_loss_price: Decimal,
    ) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            id,
            side,
            entry_price,
            size,
            take_profit_price,
            stop_loss_price,
            created_at,
            metadata: None,
        }
    }

    /// Create a new OCO order with a specific timestamp
    pub fn with_timestamp(
        id: String,
        side: Side,
        entry_price: Decimal,
        size: Decimal,
        take_profit_price: Decimal,
        stop_loss_price: Decimal,
        created_at: u64,
    ) -> Self {
        Self {
            id,
            side,
            entry_price,
            size,
            take_profit_price,
            stop_loss_price,
            created_at,
            metadata: None,
        }
    }

    /// Create an OCO order from basis points offset
    ///
    /// # Arguments
    /// * `id` - Unique order identifier
    /// * `side` - Buy (long) or Sell (short)
    /// * `entry_price` - Entry price
    /// * `size` - Position size
    /// * `tp_bps` - Take profit in basis points (positive)
    /// * `sl_bps` - Stop loss in basis points (positive)
    pub fn from_bps(
        id: String,
        side: Side,
        entry_price: Decimal,
        size: Decimal,
        tp_bps: Decimal,
        sl_bps: Decimal,
    ) -> Self {
        let bps_multiplier = dec!(0.0001); // 1 bps = 0.01%

        let (take_profit_price, stop_loss_price) = match side {
            Side::Buy => {
                // Long: TP above entry, SL below entry
                let tp = entry_price * (Decimal::ONE + tp_bps * bps_multiplier);
                let sl = entry_price * (Decimal::ONE - sl_bps * bps_multiplier);
                (tp, sl)
            }
            Side::Sell => {
                // Short: TP below entry, SL above entry
                let tp = entry_price * (Decimal::ONE - tp_bps * bps_multiplier);
                let sl = entry_price * (Decimal::ONE + sl_bps * bps_multiplier);
                (tp, sl)
            }
        };

        Self::new(id, side, entry_price, size, take_profit_price, stop_loss_price)
    }

    /// Check if take profit is triggered at the given price
    pub fn is_tp_triggered(&self, current_price: Decimal) -> bool {
        match self.side {
            Side::Buy => current_price >= self.take_profit_price,
            Side::Sell => current_price <= self.take_profit_price,
        }
    }

    /// Check if stop loss is triggered at the given price
    pub fn is_sl_triggered(&self, current_price: Decimal) -> bool {
        match self.side {
            Side::Buy => current_price <= self.stop_loss_price,
            Side::Sell => current_price >= self.stop_loss_price,
        }
    }

    /// Calculate P&L for a given exit price
    pub fn calculate_pnl(&self, exit_price: Decimal) -> Decimal {
        match self.side {
            Side::Buy => (exit_price - self.entry_price) * self.size,
            Side::Sell => (self.entry_price - exit_price) * self.size,
        }
    }

    /// Calculate P&L in basis points
    pub fn calculate_pnl_bps(&self, exit_price: Decimal) -> Decimal {
        let price_diff = match self.side {
            Side::Buy => exit_price - self.entry_price,
            Side::Sell => self.entry_price - exit_price,
        };
        (price_diff / self.entry_price) * dec!(10000)
    }

    /// Get the distance to take profit in basis points
    pub fn distance_to_tp_bps(&self, current_price: Decimal) -> Decimal {
        let distance = match self.side {
            Side::Buy => self.take_profit_price - current_price,
            Side::Sell => current_price - self.take_profit_price,
        };
        (distance / current_price) * dec!(10000)
    }

    /// Get the distance to stop loss in basis points
    pub fn distance_to_sl_bps(&self, current_price: Decimal) -> Decimal {
        let distance = match self.side {
            Side::Buy => current_price - self.stop_loss_price,
            Side::Sell => self.stop_loss_price - current_price,
        };
        (distance / current_price) * dec!(10000)
    }

    /// Add metadata to the order
    pub fn with_metadata(mut self, metadata: String) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// Statistics for OCO order performance
#[derive(Debug, Clone, Default)]
pub struct OCOStats {
    /// Total number of orders
    pub total_orders: u64,
    /// Number of take profit triggers
    pub tp_triggers: u64,
    /// Number of stop loss triggers
    pub sl_triggers: u64,
    /// Total realized P&L
    pub total_pnl: Decimal,
    /// Sum of winning trades
    pub total_wins: Decimal,
    /// Sum of losing trades
    pub total_losses: Decimal,
    /// Average trade duration in milliseconds
    pub avg_duration_ms: f64,
    /// Max drawdown seen
    pub max_drawdown: Decimal,
    /// Peak P&L
    pub peak_pnl: Decimal,
}

impl OCOStats {
    /// Calculate win rate as percentage
    pub fn win_rate(&self) -> f64 {
        if self.total_orders == 0 {
            return 0.0;
        }
        (self.tp_triggers as f64 / self.total_orders as f64) * 100.0
    }

    /// Calculate average win
    pub fn avg_win(&self) -> Decimal {
        if self.tp_triggers == 0 {
            return Decimal::ZERO;
        }
        self.total_wins / Decimal::from(self.tp_triggers)
    }

    /// Calculate average loss
    pub fn avg_loss(&self) -> Decimal {
        if self.sl_triggers == 0 {
            return Decimal::ZERO;
        }
        self.total_losses / Decimal::from(self.sl_triggers)
    }

    /// Calculate profit factor (gross wins / gross losses)
    pub fn profit_factor(&self) -> f64 {
        if self.total_losses == Decimal::ZERO {
            return f64::INFINITY;
        }
        let wins_f64: f64 = self.total_wins.try_into().unwrap_or(0.0);
        let losses_f64: f64 = self.total_losses.abs().try_into().unwrap_or(1.0);
        wins_f64 / losses_f64
    }

    /// Calculate risk-reward ratio (avg win / avg loss)
    pub fn risk_reward_ratio(&self) -> f64 {
        let avg_win: f64 = self.avg_win().try_into().unwrap_or(0.0);
        let avg_loss: f64 = self.avg_loss().abs().try_into().unwrap_or(1.0);
        if avg_loss == 0.0 {
            return f64::INFINITY;
        }
        avg_win / avg_loss
    }
}

/// Manager for OCO orders
#[derive(Debug)]
pub struct OCOManager {
    /// Active OCO orders indexed by order ID
    orders: HashMap<String, OCOOrder>,
    /// Performance statistics
    stats: OCOStats,
    /// History of completed triggers
    history: Vec<OCOTrigger>,
    /// Maximum number of concurrent orders (0 = unlimited)
    max_concurrent_orders: usize,
    /// Maximum history size to retain
    max_history_size: usize,
    /// Running sum of durations for average calculation
    total_duration_ms: u64,
}

impl Default for OCOManager {
    fn default() -> Self {
        Self::new()
    }
}

impl OCOManager {
    /// Create a new OCO manager
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            stats: OCOStats::default(),
            history: Vec::new(),
            max_concurrent_orders: 0, // unlimited
            max_history_size: 1000,
            total_duration_ms: 0,
        }
    }

    /// Create with configuration
    pub fn with_config(max_concurrent_orders: usize, max_history_size: usize) -> Self {
        Self {
            orders: HashMap::new(),
            stats: OCOStats::default(),
            history: Vec::new(),
            max_concurrent_orders,
            max_history_size,
            total_duration_ms: 0,
        }
    }

    /// Add a new OCO order
    ///
    /// Returns `Err` if max concurrent orders is reached or order ID already exists
    pub fn add_order(&mut self, order: OCOOrder) -> Result<(), OCOError> {
        if self.max_concurrent_orders > 0 && self.orders.len() >= self.max_concurrent_orders {
            return Err(OCOError::MaxOrdersReached {
                max: self.max_concurrent_orders,
            });
        }

        if self.orders.contains_key(&order.id) {
            return Err(OCOError::DuplicateOrderId {
                id: order.id.clone(),
            });
        }

        self.orders.insert(order.id.clone(), order);
        Ok(())
    }

    /// Remove an order by ID
    pub fn remove_order(&mut self, order_id: &str) -> Option<OCOOrder> {
        self.orders.remove(order_id)
    }

    /// Get an order by ID
    pub fn get_order(&self, order_id: &str) -> Option<&OCOOrder> {
        self.orders.get(order_id)
    }

    /// Get number of active orders
    pub fn active_order_count(&self) -> usize {
        self.orders.len()
    }

    /// Check if any orders exist
    pub fn has_active_orders(&self) -> bool {
        !self.orders.is_empty()
    }

    /// Get all active orders
    pub fn active_orders(&self) -> impl Iterator<Item = &OCOOrder> {
        self.orders.values()
    }

    /// Check all orders for triggers at the given price
    ///
    /// Returns a list of triggered orders. Triggered orders are automatically removed.
    pub fn check_triggers(&mut self, current_price: Decimal) -> Vec<OCOTrigger> {
        self.check_triggers_at_time(current_price, Self::current_time_ms())
    }

    /// Check triggers with a specific timestamp (useful for backtesting)
    pub fn check_triggers_at_time(
        &mut self,
        current_price: Decimal,
        current_time_ms: u64,
    ) -> Vec<OCOTrigger> {
        let mut triggers = Vec::new();
        let mut to_remove = Vec::new();

        for (order_id, order) in &self.orders {
            // Check take profit first (priority over stop loss if both triggered)
            if order.is_tp_triggered(current_price) {
                let duration_ms = current_time_ms.saturating_sub(order.created_at);
                let realized_pnl = order.calculate_pnl(order.take_profit_price);
                let pnl_bps = order.calculate_pnl_bps(order.take_profit_price);

                triggers.push(OCOTrigger {
                    order_id: order_id.clone(),
                    trigger_type: TriggerType::TakeProfit,
                    side: order.side,
                    entry_price: order.entry_price,
                    exit_price: order.take_profit_price,
                    size: order.size,
                    realized_pnl,
                    pnl_bps,
                    duration_ms,
                });
                to_remove.push(order_id.clone());
            } else if order.is_sl_triggered(current_price) {
                let duration_ms = current_time_ms.saturating_sub(order.created_at);
                let realized_pnl = order.calculate_pnl(order.stop_loss_price);
                let pnl_bps = order.calculate_pnl_bps(order.stop_loss_price);

                triggers.push(OCOTrigger {
                    order_id: order_id.clone(),
                    trigger_type: TriggerType::StopLoss,
                    side: order.side,
                    entry_price: order.entry_price,
                    exit_price: order.stop_loss_price,
                    size: order.size,
                    realized_pnl,
                    pnl_bps,
                    duration_ms,
                });
                to_remove.push(order_id.clone());
            }
        }

        // Remove triggered orders and update stats
        for order_id in to_remove {
            self.orders.remove(&order_id);
        }

        // Update statistics
        for trigger in &triggers {
            self.update_stats(trigger);
            self.add_to_history(trigger.clone());
        }

        triggers
    }

    /// Update statistics with a new trigger
    fn update_stats(&mut self, trigger: &OCOTrigger) {
        self.stats.total_orders += 1;
        self.stats.total_pnl += trigger.realized_pnl;
        self.total_duration_ms += trigger.duration_ms;

        match trigger.trigger_type {
            TriggerType::TakeProfit => {
                self.stats.tp_triggers += 1;
                self.stats.total_wins += trigger.realized_pnl;
            }
            TriggerType::StopLoss => {
                self.stats.sl_triggers += 1;
                self.stats.total_losses += trigger.realized_pnl.abs();
            }
        }

        // Update peak and drawdown
        if self.stats.total_pnl > self.stats.peak_pnl {
            self.stats.peak_pnl = self.stats.total_pnl;
        }
        let current_drawdown = self.stats.peak_pnl - self.stats.total_pnl;
        if current_drawdown > self.stats.max_drawdown {
            self.stats.max_drawdown = current_drawdown;
        }

        // Update average duration
        if self.stats.total_orders > 0 {
            self.stats.avg_duration_ms =
                self.total_duration_ms as f64 / self.stats.total_orders as f64;
        }
    }

    /// Add trigger to history, maintaining max size
    fn add_to_history(&mut self, trigger: OCOTrigger) {
        self.history.push(trigger);
        if self.history.len() > self.max_history_size {
            self.history.remove(0);
        }
    }

    /// Get performance statistics
    pub fn stats(&self) -> &OCOStats {
        &self.stats
    }

    /// Get trigger history
    pub fn history(&self) -> &[OCOTrigger] {
        &self.history
    }

    /// Reset statistics and history
    pub fn reset_stats(&mut self) {
        self.stats = OCOStats::default();
        self.history.clear();
        self.total_duration_ms = 0;
    }

    /// Clear all active orders
    pub fn clear_orders(&mut self) {
        self.orders.clear();
    }

    /// Get current time in milliseconds
    fn current_time_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Get unrealized P&L at current price
    pub fn unrealized_pnl(&self, current_price: Decimal) -> Decimal {
        self.orders
            .values()
            .map(|order| order.calculate_pnl(current_price))
            .sum()
    }

    /// Get total exposure (sum of position sizes)
    pub fn total_exposure(&self) -> Decimal {
        self.orders.values().map(|order| order.size).sum()
    }

    /// Get net exposure (long - short)
    pub fn net_exposure(&self) -> Decimal {
        self.orders
            .values()
            .map(|order| match order.side {
                Side::Buy => order.size,
                Side::Sell => -order.size,
            })
            .sum()
    }
}

/// Errors that can occur in OCO management
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OCOError {
    /// Maximum number of concurrent orders reached
    MaxOrdersReached { max: usize },
    /// Order ID already exists
    DuplicateOrderId { id: String },
    /// Order not found
    OrderNotFound { id: String },
}

impl std::fmt::Display for OCOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OCOError::MaxOrdersReached { max } => {
                write!(f, "Maximum number of concurrent orders reached: {}", max)
            }
            OCOError::DuplicateOrderId { id } => {
                write!(f, "Order ID already exists: {}", id)
            }
            OCOError::OrderNotFound { id } => {
                write!(f, "Order not found: {}", id)
            }
        }
    }
}

impl std::error::Error for OCOError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_side_opposite() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
    }

    #[test]
    fn test_oco_order_creation() {
        let order = OCOOrder::new(
            "test_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100), // TP at +20 bps
            dec!(49950), // SL at -10 bps
        );

        assert_eq!(order.id, "test_1");
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.entry_price, dec!(50000));
        assert_eq!(order.size, dec!(1.0));
        assert_eq!(order.take_profit_price, dec!(50100));
        assert_eq!(order.stop_loss_price, dec!(49950));
    }

    #[test]
    fn test_oco_order_from_bps_long() {
        let order = OCOOrder::from_bps(
            "test_bps".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(20), // 20 bps TP
            dec!(10), // 10 bps SL
        );

        // TP should be +20 bps = 50000 * 1.002 = 50100
        assert_eq!(order.take_profit_price, dec!(50100));
        // SL should be -10 bps = 50000 * 0.999 = 49950
        assert_eq!(order.stop_loss_price, dec!(49950));
    }

    #[test]
    fn test_oco_order_from_bps_short() {
        let order = OCOOrder::from_bps(
            "test_bps_short".to_string(),
            Side::Sell,
            dec!(50000),
            dec!(1.0),
            dec!(20), // 20 bps TP
            dec!(10), // 10 bps SL
        );

        // Short TP should be -20 bps = 50000 * 0.998 = 49900
        assert_eq!(order.take_profit_price, dec!(49900));
        // Short SL should be +10 bps = 50000 * 1.001 = 50050
        assert_eq!(order.stop_loss_price, dec!(50050));
    }

    #[test]
    fn test_tp_triggered_long() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );

        assert!(!order.is_tp_triggered(dec!(50000)));
        assert!(!order.is_tp_triggered(dec!(50099)));
        assert!(order.is_tp_triggered(dec!(50100)));
        assert!(order.is_tp_triggered(dec!(50200)));
    }

    #[test]
    fn test_sl_triggered_long() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );

        assert!(!order.is_sl_triggered(dec!(50000)));
        assert!(!order.is_sl_triggered(dec!(49901)));
        assert!(order.is_sl_triggered(dec!(49900)));
        assert!(order.is_sl_triggered(dec!(49800)));
    }

    #[test]
    fn test_tp_triggered_short() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Sell,
            dec!(50000),
            dec!(1.0),
            dec!(49900), // TP below for short
            dec!(50100), // SL above for short
        );

        assert!(!order.is_tp_triggered(dec!(50000)));
        assert!(!order.is_tp_triggered(dec!(49901)));
        assert!(order.is_tp_triggered(dec!(49900)));
        assert!(order.is_tp_triggered(dec!(49800)));
    }

    #[test]
    fn test_sl_triggered_short() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Sell,
            dec!(50000),
            dec!(1.0),
            dec!(49900),
            dec!(50100),
        );

        assert!(!order.is_sl_triggered(dec!(50000)));
        assert!(!order.is_sl_triggered(dec!(50099)));
        assert!(order.is_sl_triggered(dec!(50100)));
        assert!(order.is_sl_triggered(dec!(50200)));
    }

    #[test]
    fn test_pnl_calculation_long() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(2.0),
            dec!(50100),
            dec!(49900),
        );

        // TP hit: (50100 - 50000) * 2 = 200
        assert_eq!(order.calculate_pnl(dec!(50100)), dec!(200));
        // SL hit: (49900 - 50000) * 2 = -200
        assert_eq!(order.calculate_pnl(dec!(49900)), dec!(-200));
    }

    #[test]
    fn test_pnl_calculation_short() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Sell,
            dec!(50000),
            dec!(2.0),
            dec!(49900),
            dec!(50100),
        );

        // TP hit: (50000 - 49900) * 2 = 200
        assert_eq!(order.calculate_pnl(dec!(49900)), dec!(200));
        // SL hit: (50000 - 50100) * 2 = -200
        assert_eq!(order.calculate_pnl(dec!(50100)), dec!(-200));
    }

    #[test]
    fn test_pnl_bps_calculation() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100), // +20 bps
            dec!(49950), // -10 bps
        );

        assert_eq!(order.calculate_pnl_bps(dec!(50100)), dec!(20));
        assert_eq!(order.calculate_pnl_bps(dec!(49950)), dec!(-10));
    }

    #[test]
    fn test_manager_add_order() {
        let mut manager = OCOManager::new();
        let order = OCOOrder::new(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );

        assert!(manager.add_order(order).is_ok());
        assert_eq!(manager.active_order_count(), 1);
        assert!(manager.has_active_orders());
    }

    #[test]
    fn test_manager_duplicate_order() {
        let mut manager = OCOManager::new();
        let order1 = OCOOrder::new(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );
        let order2 = OCOOrder::new(
            "order_1".to_string(),
            Side::Sell,
            dec!(50000),
            dec!(1.0),
            dec!(49900),
            dec!(50100),
        );

        assert!(manager.add_order(order1).is_ok());
        assert_eq!(
            manager.add_order(order2),
            Err(OCOError::DuplicateOrderId {
                id: "order_1".to_string()
            })
        );
    }

    #[test]
    fn test_manager_max_orders() {
        let mut manager = OCOManager::with_config(2, 100);

        let order1 = OCOOrder::new(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );
        let order2 = OCOOrder::new(
            "order_2".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );
        let order3 = OCOOrder::new(
            "order_3".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );

        assert!(manager.add_order(order1).is_ok());
        assert!(manager.add_order(order2).is_ok());
        assert_eq!(
            manager.add_order(order3),
            Err(OCOError::MaxOrdersReached { max: 2 })
        );
    }

    #[test]
    fn test_check_triggers_tp() {
        let mut manager = OCOManager::new();
        let order = OCOOrder::with_timestamp(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
            1000, // created at t=1000
        );

        manager.add_order(order).unwrap();

        // Price below TP - no trigger
        let triggers = manager.check_triggers_at_time(dec!(50050), 2000);
        assert!(triggers.is_empty());
        assert_eq!(manager.active_order_count(), 1);

        // Price at TP - should trigger
        let triggers = manager.check_triggers_at_time(dec!(50100), 3000);
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].trigger_type, TriggerType::TakeProfit);
        assert_eq!(triggers[0].realized_pnl, dec!(100)); // (50100 - 50000) * 1
        assert_eq!(triggers[0].duration_ms, 2000); // 3000 - 1000
        assert_eq!(manager.active_order_count(), 0);
    }

    #[test]
    fn test_check_triggers_sl() {
        let mut manager = OCOManager::new();
        let order = OCOOrder::with_timestamp(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
            1000,
        );

        manager.add_order(order).unwrap();

        // Price at SL - should trigger
        let triggers = manager.check_triggers_at_time(dec!(49900), 2000);
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].trigger_type, TriggerType::StopLoss);
        assert_eq!(triggers[0].realized_pnl, dec!(-100)); // (49900 - 50000) * 1
        assert_eq!(manager.active_order_count(), 0);
    }

    #[test]
    fn test_stats_tracking() {
        let mut manager = OCOManager::new();

        // Add and trigger a winning trade
        let order1 = OCOOrder::with_timestamp(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
            1000,
        );
        manager.add_order(order1).unwrap();
        manager.check_triggers_at_time(dec!(50100), 2000);

        // Add and trigger a losing trade
        let order2 = OCOOrder::with_timestamp(
            "order_2".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
            3000,
        );
        manager.add_order(order2).unwrap();
        manager.check_triggers_at_time(dec!(49900), 4000);

        let stats = manager.stats();
        assert_eq!(stats.total_orders, 2);
        assert_eq!(stats.tp_triggers, 1);
        assert_eq!(stats.sl_triggers, 1);
        assert_eq!(stats.total_pnl, dec!(0)); // 100 - 100 = 0
        assert_eq!(stats.total_wins, dec!(100));
        assert_eq!(stats.total_losses, dec!(100));
        assert_eq!(stats.win_rate(), 50.0);
    }

    #[test]
    fn test_unrealized_pnl() {
        let mut manager = OCOManager::new();

        let order1 = OCOOrder::new(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );
        let order2 = OCOOrder::new(
            "order_2".to_string(),
            Side::Sell,
            dec!(50000),
            dec!(1.0),
            dec!(49900),
            dec!(50100),
        );

        manager.add_order(order1).unwrap();
        manager.add_order(order2).unwrap();

        // At current price of 50050:
        // Long: (50050 - 50000) * 1 = 50
        // Short: (50000 - 50050) * 1 = -50
        // Total: 0
        assert_eq!(manager.unrealized_pnl(dec!(50050)), dec!(0));

        // At current price of 50100:
        // Long: (50100 - 50000) * 1 = 100
        // Short: (50000 - 50100) * 1 = -100
        // Total: 0
        assert_eq!(manager.unrealized_pnl(dec!(50100)), dec!(0));
    }

    #[test]
    fn test_exposure_tracking() {
        let mut manager = OCOManager::new();

        let order1 = OCOOrder::new(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(2.0),
            dec!(50100),
            dec!(49900),
        );
        let order2 = OCOOrder::new(
            "order_2".to_string(),
            Side::Sell,
            dec!(50000),
            dec!(1.0),
            dec!(49900),
            dec!(50100),
        );

        manager.add_order(order1).unwrap();
        manager.add_order(order2).unwrap();

        assert_eq!(manager.total_exposure(), dec!(3.0)); // 2 + 1
        assert_eq!(manager.net_exposure(), dec!(1.0)); // 2 - 1
    }

    #[test]
    fn test_history_size_limit() {
        let mut manager = OCOManager::with_config(0, 3); // max 3 history entries

        for i in 0..5 {
            let order = OCOOrder::with_timestamp(
                format!("order_{}", i),
                Side::Buy,
                dec!(50000),
                dec!(1.0),
                dec!(50100),
                dec!(49900),
                i as u64 * 1000,
            );
            manager.add_order(order).unwrap();
            manager.check_triggers_at_time(dec!(50100), (i as u64 + 1) * 1000);
        }

        // Should only retain last 3 entries
        assert_eq!(manager.history().len(), 3);
        assert_eq!(manager.history()[0].order_id, "order_2");
        assert_eq!(manager.history()[2].order_id, "order_4");
    }

    #[test]
    fn test_remove_order() {
        let mut manager = OCOManager::new();
        let order = OCOOrder::new(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        );

        manager.add_order(order).unwrap();
        assert_eq!(manager.active_order_count(), 1);

        let removed = manager.remove_order("order_1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, "order_1");
        assert_eq!(manager.active_order_count(), 0);

        // Removing non-existent order returns None
        assert!(manager.remove_order("order_1").is_none());
    }

    #[test]
    fn test_clear_orders() {
        let mut manager = OCOManager::new();

        for i in 0..5 {
            let order = OCOOrder::new(
                format!("order_{}", i),
                Side::Buy,
                dec!(50000),
                dec!(1.0),
                dec!(50100),
                dec!(49900),
            );
            manager.add_order(order).unwrap();
        }

        assert_eq!(manager.active_order_count(), 5);
        manager.clear_orders();
        assert_eq!(manager.active_order_count(), 0);
    }

    #[test]
    fn test_reset_stats() {
        let mut manager = OCOManager::new();

        let order = OCOOrder::with_timestamp(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
            1000,
        );
        manager.add_order(order).unwrap();
        manager.check_triggers_at_time(dec!(50100), 2000);

        assert_eq!(manager.stats().total_orders, 1);
        assert_eq!(manager.history().len(), 1);

        manager.reset_stats();

        assert_eq!(manager.stats().total_orders, 0);
        assert!(manager.history().is_empty());
    }

    #[test]
    fn test_distance_to_tp_sl() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100), // +20 bps from entry
            dec!(49900), // -20 bps from entry
        );

        // At entry price
        let tp_dist = order.distance_to_tp_bps(dec!(50000));
        let sl_dist = order.distance_to_sl_bps(dec!(50000));
        assert_eq!(tp_dist, dec!(20));
        assert_eq!(sl_dist, dec!(20));

        // Halfway to TP
        let tp_dist = order.distance_to_tp_bps(dec!(50050));
        let sl_dist = order.distance_to_sl_bps(dec!(50050));
        // Distance to TP: (50100 - 50050) / 50050 * 10000 ≈ 9.99
        assert!(tp_dist < dec!(11) && tp_dist > dec!(9));
        // Distance to SL: (50050 - 49900) / 50050 * 10000 ≈ 29.97
        assert!(sl_dist > dec!(29) && sl_dist < dec!(31));
    }

    #[test]
    fn test_profit_factor_and_risk_reward() {
        let mut manager = OCOManager::new();

        // 2 wins of 100 each
        for i in 0..2 {
            let order = OCOOrder::with_timestamp(
                format!("win_{}", i),
                Side::Buy,
                dec!(50000),
                dec!(1.0),
                dec!(50100),
                dec!(49900),
                i as u64 * 1000,
            );
            manager.add_order(order).unwrap();
            manager.check_triggers_at_time(dec!(50100), (i as u64 + 1) * 1000);
        }

        // 1 loss of 100
        let order = OCOOrder::with_timestamp(
            "loss_0".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
            3000,
        );
        manager.add_order(order).unwrap();
        manager.check_triggers_at_time(dec!(49900), 4000);

        let stats = manager.stats();

        // Profit factor = gross wins / gross losses = 200 / 100 = 2.0
        assert!((stats.profit_factor() - 2.0).abs() < 0.001);

        // Risk/reward = avg win / avg loss = 100 / 100 = 1.0
        assert!((stats.risk_reward_ratio() - 1.0).abs() < 0.001);

        // Win rate = 2/3 = 66.67%
        assert!((stats.win_rate() - 66.666).abs() < 0.01);
    }

    #[test]
    fn test_order_metadata() {
        let order = OCOOrder::new(
            "test".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
        )
        .with_metadata("regime:trending_up".to_string());

        assert_eq!(order.metadata, Some("regime:trending_up".to_string()));
    }

    #[test]
    fn test_multiple_simultaneous_triggers() {
        let mut manager = OCOManager::new();

        // Add multiple orders at different levels
        let order1 = OCOOrder::with_timestamp(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50050), // TP at 50050
            dec!(49950),
            1000,
        );
        let order2 = OCOOrder::with_timestamp(
            "order_2".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100), // TP at 50100
            dec!(49900),
            1000,
        );

        manager.add_order(order1).unwrap();
        manager.add_order(order2).unwrap();

        // Price jump to 50100 should trigger both
        let triggers = manager.check_triggers_at_time(dec!(50100), 2000);
        assert_eq!(triggers.len(), 2);
        assert_eq!(manager.active_order_count(), 0);
    }

    #[test]
    fn test_max_drawdown_tracking() {
        let mut manager = OCOManager::new();

        // Win 100
        let order1 = OCOOrder::with_timestamp(
            "order_1".to_string(),
            Side::Buy,
            dec!(50000),
            dec!(1.0),
            dec!(50100),
            dec!(49900),
            1000,
        );
        manager.add_order(order1).unwrap();
        manager.check_triggers_at_time(dec!(50100), 2000);

        // Peak PnL is now 100
        assert_eq!(manager.stats().peak_pnl, dec!(100));
        assert_eq!(manager.stats().max_drawdown, dec!(0));

        // Lose 200 (two losses of 100 each)
        for i in 0..2 {
            let order = OCOOrder::with_timestamp(
                format!("loss_{}", i),
                Side::Buy,
                dec!(50000),
                dec!(1.0),
                dec!(50100),
                dec!(49900),
                (3000 + i * 1000) as u64,
            );
            manager.add_order(order).unwrap();
            manager.check_triggers_at_time(dec!(49900), (4000 + i * 1000) as u64);
        }

        // Total PnL: 100 - 100 - 100 = -100
        // Max drawdown: peak(100) - current(-100) = 200
        assert_eq!(manager.stats().total_pnl, dec!(-100));
        assert_eq!(manager.stats().max_drawdown, dec!(200));
    }
}

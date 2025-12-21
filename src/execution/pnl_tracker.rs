//! Real-Time Position P&L Tracker
//!
//! Provides unified, real-time profit and loss tracking across all trading activities.
//! This module aggregates P&L from multiple sources (market making, directional trades, OCO orders)
//! and maintains historical snapshots for analysis.
//!
//! # Features
//!
//! - **Real-time updates**: P&L updates on every price tick
//! - **Multiple position tracking**: Track multiple positions with individual P&L
//! - **Attribution**: Break down P&L by source (MM, directional, OCO)
//! - **Historical snapshots**: Time-series P&L history for equity curve analysis
//! - **Drawdown tracking**: Real-time drawdown calculations
//! - **FIFO cost basis**: First-in-first-out cost basis accounting
//!
//! # Example
//!
//! ```rust,ignore
//! use ingestor::execution::pnl_tracker::{RealTimePnLTracker, TrackerConfig};
//! use rust_decimal_macros::dec;
//!
//! let config = TrackerConfig::default();
//! let mut tracker = RealTimePnLTracker::new(config);
//!
//! // Record a fill (entry)
//! tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0.5), 1000);
//!
//! // Update with new price
//! tracker.on_price_update(dec!(50100), 2000);
//!
//! // Get current P&L
//! let pnl = tracker.total_pnl();
//! println!("Total P&L: {}", pnl.total);
//! ```

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the P&L tracker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerConfig {
    /// Maximum number of P&L snapshots to retain
    pub max_history_size: usize,
    /// Interval for automatic snapshots (milliseconds, 0 = disabled)
    pub snapshot_interval_ms: u64,
    /// Initial capital for percentage calculations
    pub initial_capital: Decimal,
    /// Enable FIFO cost basis tracking
    pub use_fifo: bool,
    /// Maximum number of positions to track
    pub max_positions: usize,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self {
            max_history_size: 10000,
            snapshot_interval_ms: 1000, // 1 second snapshots
            initial_capital: dec!(10000),
            use_fifo: true,
            max_positions: 100,
        }
    }
}

// ============================================================================
// P&L Structures
// ============================================================================

/// Side of a position
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
}

impl PositionSide {
    pub fn opposite(&self) -> Self {
        match self {
            PositionSide::Long => PositionSide::Short,
            PositionSide::Short => PositionSide::Long,
        }
    }

    pub fn sign(&self) -> Decimal {
        match self {
            PositionSide::Long => Decimal::ONE,
            PositionSide::Short => -Decimal::ONE,
        }
    }
}

/// Source of P&L for attribution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PnLSource {
    /// Market making activity
    MarketMaking,
    /// Directional/trend trades
    Directional,
    /// OCO order triggers
    OCO,
    /// Manual/other trades
    Manual,
}

impl Default for PnLSource {
    fn default() -> Self {
        PnLSource::Manual
    }
}

/// A single fill in a position's history (for FIFO tracking)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillRecord {
    /// Fill price
    pub price: Decimal,
    /// Remaining size (reduced as position is closed)
    pub remaining_size: Decimal,
    /// Original size
    pub original_size: Decimal,
    /// Timestamp
    pub timestamp_ms: u64,
    /// Fee paid
    pub fee: Decimal,
}

/// Individual position P&L tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionPnL {
    /// Position identifier
    pub id: String,
    /// Position side
    pub side: PositionSide,
    /// Current position size
    pub size: Decimal,
    /// Average entry price (weighted)
    pub avg_entry_price: Decimal,
    /// Current market price
    pub current_price: Decimal,
    /// Unrealized P&L
    pub unrealized_pnl: Decimal,
    /// Realized P&L (from partial closes)
    pub realized_pnl: Decimal,
    /// Total fees paid
    pub total_fees: Decimal,
    /// P&L source for attribution
    pub source: PnLSource,
    /// Entry timestamp
    pub entry_time_ms: u64,
    /// Last update timestamp
    pub last_update_ms: u64,
    /// FIFO fill history
    pub fill_history: VecDeque<FillRecord>,
    /// Peak unrealized P&L (for position drawdown)
    pub peak_unrealized: Decimal,
    /// Maximum drawdown within this position
    pub max_drawdown: Decimal,
}

impl PositionPnL {
    /// Create a new position
    pub fn new(
        id: String,
        side: PositionSide,
        entry_price: Decimal,
        size: Decimal,
        fee: Decimal,
        source: PnLSource,
        timestamp_ms: u64,
    ) -> Self {
        let fill = FillRecord {
            price: entry_price,
            remaining_size: size,
            original_size: size,
            timestamp_ms,
            fee,
        };

        Self {
            id,
            side,
            size,
            avg_entry_price: entry_price,
            current_price: entry_price,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            total_fees: fee,
            source,
            entry_time_ms: timestamp_ms,
            last_update_ms: timestamp_ms,
            fill_history: VecDeque::from([fill]),
            peak_unrealized: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
        }
    }

    /// Update unrealized P&L with new price
    pub fn update_price(&mut self, price: Decimal, timestamp_ms: u64) {
        self.current_price = price;
        self.last_update_ms = timestamp_ms;

        // Calculate unrealized P&L
        let price_diff = match self.side {
            PositionSide::Long => price - self.avg_entry_price,
            PositionSide::Short => self.avg_entry_price - price,
        };
        self.unrealized_pnl = price_diff * self.size;

        // Track peak and drawdown
        if self.unrealized_pnl > self.peak_unrealized {
            self.peak_unrealized = self.unrealized_pnl;
        }
        let drawdown = self.peak_unrealized - self.unrealized_pnl;
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
    }

    /// Add to position (same direction)
    pub fn add(&mut self, price: Decimal, size: Decimal, fee: Decimal, timestamp_ms: u64) {
        // Update weighted average entry
        let total_cost = self.avg_entry_price * self.size + price * size;
        let new_size = self.size + size;
        self.avg_entry_price = total_cost / new_size;
        self.size = new_size;
        self.total_fees += fee;

        // Add to FIFO history
        self.fill_history.push_back(FillRecord {
            price,
            remaining_size: size,
            original_size: size,
            timestamp_ms,
            fee,
        });

        self.update_price(self.current_price, timestamp_ms);
    }

    /// Reduce position (FIFO cost basis)
    /// Returns (realized_pnl, closed_avg_entry)
    pub fn reduce(&mut self, size: Decimal, exit_price: Decimal, fee: Decimal, timestamp_ms: u64) -> (Decimal, Decimal) {
        let close_size = size.min(self.size);
        if close_size == Decimal::ZERO {
            return (Decimal::ZERO, Decimal::ZERO);
        }

        // Calculate realized P&L using FIFO
        let mut remaining_to_close = close_size;
        let mut total_cost_basis = Decimal::ZERO;
        let mut closed_size = Decimal::ZERO;

        while remaining_to_close > Decimal::ZERO && !self.fill_history.is_empty() {
            if let Some(fill) = self.fill_history.front_mut() {
                let close_from_fill = remaining_to_close.min(fill.remaining_size);
                total_cost_basis += fill.price * close_from_fill;
                closed_size += close_from_fill;
                fill.remaining_size -= close_from_fill;
                remaining_to_close -= close_from_fill;

                if fill.remaining_size == Decimal::ZERO {
                    self.fill_history.pop_front();
                }
            }
        }

        let avg_cost_basis = if closed_size > Decimal::ZERO {
            total_cost_basis / closed_size
        } else {
            self.avg_entry_price
        };

        // Calculate realized P&L
        let realized = match self.side {
            PositionSide::Long => (exit_price - avg_cost_basis) * close_size,
            PositionSide::Short => (avg_cost_basis - exit_price) * close_size,
        };

        self.realized_pnl += realized;
        self.size -= close_size;
        self.total_fees += fee;
        self.last_update_ms = timestamp_ms;

        // Recalculate average entry from remaining fills
        if self.size > Decimal::ZERO {
            let mut total = Decimal::ZERO;
            let mut total_size = Decimal::ZERO;
            for fill in &self.fill_history {
                total += fill.price * fill.remaining_size;
                total_size += fill.remaining_size;
            }
            if total_size > Decimal::ZERO {
                self.avg_entry_price = total / total_size;
            }
        }

        self.update_price(self.current_price, timestamp_ms);

        (realized, avg_cost_basis)
    }

    /// Get total P&L (realized + unrealized - fees)
    pub fn total_pnl(&self) -> Decimal {
        self.realized_pnl + self.unrealized_pnl - self.total_fees
    }

    /// Get P&L in basis points relative to entry
    pub fn pnl_bps(&self) -> Decimal {
        if self.avg_entry_price == Decimal::ZERO || self.size == Decimal::ZERO {
            return Decimal::ZERO;
        }
        let pnl_per_unit = self.total_pnl() / self.size;
        (pnl_per_unit / self.avg_entry_price) * dec!(10000)
    }

    /// Get position duration in milliseconds
    pub fn duration_ms(&self) -> u64 {
        self.last_update_ms.saturating_sub(self.entry_time_ms)
    }

    /// Check if position is closed
    pub fn is_closed(&self) -> bool {
        self.size == Decimal::ZERO
    }
}

/// Aggregated P&L summary
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnLSummary {
    /// Total realized P&L
    pub realized: Decimal,
    /// Total unrealized P&L
    pub unrealized: Decimal,
    /// Total P&L (realized + unrealized)
    pub total: Decimal,
    /// Total fees paid
    pub fees: Decimal,
    /// Net P&L (total - fees)
    pub net: Decimal,
    /// Number of winning trades
    pub winning_trades: u64,
    /// Number of losing trades
    pub losing_trades: u64,
    /// Total number of trades
    pub total_trades: u64,
    /// Gross profit (sum of winning trades)
    pub gross_profit: Decimal,
    /// Gross loss (sum of losing trades, positive number)
    pub gross_loss: Decimal,
    /// Win rate (0-100)
    pub win_rate: f64,
    /// Profit factor (gross_profit / gross_loss)
    pub profit_factor: f64,
    /// Average winning trade
    pub avg_win: Decimal,
    /// Average losing trade
    pub avg_loss: Decimal,
    /// Risk/reward ratio (avg_win / avg_loss)
    pub risk_reward: f64,
    /// Maximum drawdown
    pub max_drawdown: Decimal,
    /// Current drawdown
    pub current_drawdown: Decimal,
}

impl PnLSummary {
    /// Calculate derived metrics
    pub fn calculate_metrics(&mut self) {
        self.total = self.realized + self.unrealized;
        self.net = self.total - self.fees;
        self.total_trades = self.winning_trades + self.losing_trades;

        if self.total_trades > 0 {
            self.win_rate = (self.winning_trades as f64 / self.total_trades as f64) * 100.0;
        }

        if self.gross_loss > Decimal::ZERO {
            self.profit_factor = self.gross_profit.to_f64().unwrap_or(0.0)
                / self.gross_loss.to_f64().unwrap_or(1.0);
        }

        if self.winning_trades > 0 {
            self.avg_win = self.gross_profit / Decimal::from(self.winning_trades);
        }

        if self.losing_trades > 0 {
            self.avg_loss = self.gross_loss / Decimal::from(self.losing_trades);
        }

        if self.avg_loss > Decimal::ZERO {
            self.risk_reward = self.avg_win.to_f64().unwrap_or(0.0)
                / self.avg_loss.to_f64().unwrap_or(1.0);
        }
    }
}

/// P&L snapshot at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLSnapshot {
    /// Timestamp
    pub timestamp_ms: u64,
    /// Current price
    pub price: Decimal,
    /// Total realized P&L
    pub realized_pnl: Decimal,
    /// Total unrealized P&L
    pub unrealized_pnl: Decimal,
    /// Total P&L
    pub total_pnl: Decimal,
    /// Current equity (initial_capital + total_pnl)
    pub equity: Decimal,
    /// Current drawdown from peak
    pub drawdown: Decimal,
    /// Number of open positions
    pub open_positions: usize,
    /// Total exposure (sum of position sizes)
    pub total_exposure: Decimal,
    /// Net exposure (long - short)
    pub net_exposure: Decimal,
}

/// P&L by source attribution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnLAttribution {
    pub market_making: PnLSummary,
    pub directional: PnLSummary,
    pub oco: PnLSummary,
    pub manual: PnLSummary,
}

impl PnLAttribution {
    pub fn get_mut(&mut self, source: PnLSource) -> &mut PnLSummary {
        match source {
            PnLSource::MarketMaking => &mut self.market_making,
            PnLSource::Directional => &mut self.directional,
            PnLSource::OCO => &mut self.oco,
            PnLSource::Manual => &mut self.manual,
        }
    }

    pub fn get(&self, source: PnLSource) -> &PnLSummary {
        match source {
            PnLSource::MarketMaking => &self.market_making,
            PnLSource::Directional => &self.directional,
            PnLSource::OCO => &self.oco,
            PnLSource::Manual => &self.manual,
        }
    }

    /// Get combined summary across all sources
    pub fn total(&self) -> PnLSummary {
        let mut summary = PnLSummary::default();
        for source in [PnLSource::MarketMaking, PnLSource::Directional, PnLSource::OCO, PnLSource::Manual] {
            let s = self.get(source);
            summary.realized += s.realized;
            summary.unrealized += s.unrealized;
            summary.fees += s.fees;
            summary.winning_trades += s.winning_trades;
            summary.losing_trades += s.losing_trades;
            summary.gross_profit += s.gross_profit;
            summary.gross_loss += s.gross_loss;
        }
        summary.calculate_metrics();
        summary
    }
}

// ============================================================================
// Real-Time P&L Tracker
// ============================================================================

/// Real-time P&L tracker with historical snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealTimePnLTracker {
    /// Configuration
    config: TrackerConfig,
    /// Active positions
    positions: HashMap<String, PositionPnL>,
    /// Closed positions (for history)
    closed_positions: VecDeque<PositionPnL>,
    /// P&L history snapshots
    history: VecDeque<PnLSnapshot>,
    /// Last snapshot timestamp
    last_snapshot_ms: u64,
    /// Current market price
    current_price: Decimal,
    /// Peak equity (for drawdown)
    peak_equity: Decimal,
    /// P&L by source
    attribution: PnLAttribution,
    /// Total realized P&L
    total_realized: Decimal,
    /// Total fees
    total_fees: Decimal,
    /// Position counter for ID generation
    position_counter: u64,
}

impl Default for RealTimePnLTracker {
    fn default() -> Self {
        Self::new(TrackerConfig::default())
    }
}

impl RealTimePnLTracker {
    /// Create a new tracker with configuration
    pub fn new(config: TrackerConfig) -> Self {
        Self {
            peak_equity: config.initial_capital,
            config,
            positions: HashMap::new(),
            closed_positions: VecDeque::new(),
            history: VecDeque::new(),
            last_snapshot_ms: 0,
            current_price: Decimal::ZERO,
            attribution: PnLAttribution::default(),
            total_realized: Decimal::ZERO,
            total_fees: Decimal::ZERO,
            position_counter: 0,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &TrackerConfig {
        &self.config
    }

    /// Generate a unique position ID
    pub fn generate_position_id(&mut self) -> String {
        self.position_counter += 1;
        format!("pos_{}", self.position_counter)
    }

    // ========================================================================
    // Position Management
    // ========================================================================

    /// Record a new fill (entry or addition to position)
    pub fn record_fill(
        &mut self,
        position_id: &str,
        side: PositionSide,
        price: Decimal,
        size: Decimal,
        fee: Decimal,
        source: PnLSource,
        timestamp_ms: u64,
    ) {
        self.current_price = price;
        self.total_fees += fee;

        // Track whether we need to record trade result and remove position
        let mut trade_result: Option<(Decimal, PnLSource)> = None;
        let mut should_remove = false;
        let mut position_exists = false;

        if let Some(position) = self.positions.get_mut(position_id) {
            position_exists = true;
            // Adding to existing position
            if position.side == side {
                position.add(price, size, fee, timestamp_ms);
            } else {
                // Reducing/closing opposite position
                let (realized, _) = position.reduce(size, price, fee, timestamp_ms);
                self.total_realized += realized;
                trade_result = Some((realized, source));
                should_remove = position.is_closed();
            }
        }

        // Handle trade result and position removal outside the borrow
        if let Some((realized, src)) = trade_result {
            self.record_trade_result(realized, src);
        }
        if should_remove {
            if let Some(closed) = self.positions.remove(position_id) {
                self.add_to_closed_history(closed);
            }
        }

        // New position (only if position doesn't exist yet)
        if !position_exists {
            let position = PositionPnL::new(
                position_id.to_string(),
                side,
                price,
                size,
                fee,
                source,
                timestamp_ms,
            );
            if self.positions.len() < self.config.max_positions {
                self.positions.insert(position_id.to_string(), position);
            }
        }

        self.maybe_take_snapshot(timestamp_ms);
    }

    /// Close a position (full or partial)
    pub fn close_position(
        &mut self,
        position_id: &str,
        exit_price: Decimal,
        size: Option<Decimal>,
        fee: Decimal,
        timestamp_ms: u64,
    ) -> Option<Decimal> {
        self.current_price = exit_price;
        self.total_fees += fee;

        // Get values we need from position before modifying
        let (close_size, source) = {
            let position = self.positions.get(position_id)?;
            (size.unwrap_or(position.size), position.source)
        };

        // Now modify the position
        let (realized, should_remove) = {
            let position = self.positions.get_mut(position_id)?;
            let (realized, _) = position.reduce(close_size, exit_price, fee, timestamp_ms);
            self.total_realized += realized;
            (realized, position.is_closed())
        };

        // Handle trade result and position removal outside the borrow
        self.record_trade_result(realized, source);

        if should_remove {
            if let Some(closed) = self.positions.remove(position_id) {
                self.add_to_closed_history(closed);
            }
        }

        self.maybe_take_snapshot(timestamp_ms);
        Some(realized)
    }

    /// Record a trade result for attribution
    fn record_trade_result(&mut self, realized: Decimal, source: PnLSource) {
        let summary = self.attribution.get_mut(source);
        summary.realized += realized;

        if realized > Decimal::ZERO {
            summary.winning_trades += 1;
            summary.gross_profit += realized;
        } else if realized < Decimal::ZERO {
            summary.losing_trades += 1;
            summary.gross_loss += realized.abs();
        }

        summary.calculate_metrics();
    }

    /// Add closed position to history
    fn add_to_closed_history(&mut self, position: PositionPnL) {
        self.closed_positions.push_back(position);
        // Limit history size
        while self.closed_positions.len() > self.config.max_history_size {
            self.closed_positions.pop_front();
        }
    }

    // ========================================================================
    // Price Updates
    // ========================================================================

    /// Update all positions with new market price
    pub fn on_price_update(&mut self, price: Decimal, timestamp_ms: u64) {
        self.current_price = price;

        for position in self.positions.values_mut() {
            position.update_price(price, timestamp_ms);
        }

        // Calculate unrealized P&L by source
        let mut mm_unrealized = Decimal::ZERO;
        let mut directional_unrealized = Decimal::ZERO;
        let mut oco_unrealized = Decimal::ZERO;
        let mut manual_unrealized = Decimal::ZERO;

        for position in self.positions.values() {
            match position.source {
                PnLSource::MarketMaking => mm_unrealized += position.unrealized_pnl,
                PnLSource::Directional => directional_unrealized += position.unrealized_pnl,
                PnLSource::OCO => oco_unrealized += position.unrealized_pnl,
                PnLSource::Manual => manual_unrealized += position.unrealized_pnl,
            }
        }

        // Update attribution unrealized
        self.attribution.market_making.unrealized = mm_unrealized;
        self.attribution.directional.unrealized = directional_unrealized;
        self.attribution.oco.unrealized = oco_unrealized;
        self.attribution.manual.unrealized = manual_unrealized;

        self.maybe_take_snapshot(timestamp_ms);
    }

    /// Maybe take a snapshot based on interval
    fn maybe_take_snapshot(&mut self, timestamp_ms: u64) {
        if self.config.snapshot_interval_ms == 0 {
            return;
        }

        if timestamp_ms >= self.last_snapshot_ms + self.config.snapshot_interval_ms {
            self.take_snapshot(timestamp_ms);
        }
    }

    /// Force a snapshot
    pub fn take_snapshot(&mut self, timestamp_ms: u64) {
        let equity = self.equity();

        // Update peak equity
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }

        let drawdown = self.peak_equity - equity;

        let snapshot = PnLSnapshot {
            timestamp_ms,
            price: self.current_price,
            realized_pnl: self.total_realized,
            unrealized_pnl: self.total_unrealized_pnl(),
            total_pnl: self.total_pnl(),
            equity,
            drawdown,
            open_positions: self.positions.len(),
            total_exposure: self.total_exposure(),
            net_exposure: self.net_exposure(),
        };

        self.history.push_back(snapshot);
        self.last_snapshot_ms = timestamp_ms;

        // Limit history size
        while self.history.len() > self.config.max_history_size {
            self.history.pop_front();
        }
    }

    // ========================================================================
    // P&L Queries
    // ========================================================================

    /// Get total unrealized P&L
    pub fn total_unrealized_pnl(&self) -> Decimal {
        self.positions.values().map(|p| p.unrealized_pnl).sum()
    }

    /// Get total realized P&L
    pub fn total_realized_pnl(&self) -> Decimal {
        self.total_realized
    }

    /// Get total P&L (realized + unrealized)
    pub fn total_pnl(&self) -> Decimal {
        self.total_realized + self.total_unrealized_pnl()
    }

    /// Get net P&L (total - fees)
    pub fn net_pnl(&self) -> Decimal {
        self.total_pnl() - self.total_fees
    }

    /// Get current equity
    pub fn equity(&self) -> Decimal {
        self.config.initial_capital + self.net_pnl()
    }

    /// Get total fees
    pub fn total_fees(&self) -> Decimal {
        self.total_fees
    }

    /// Get current drawdown
    pub fn current_drawdown(&self) -> Decimal {
        (self.peak_equity - self.equity()).max(Decimal::ZERO)
    }

    /// Get current drawdown percentage
    pub fn current_drawdown_pct(&self) -> f64 {
        if self.peak_equity == Decimal::ZERO {
            return 0.0;
        }
        (self.current_drawdown() / self.peak_equity)
            .to_f64()
            .unwrap_or(0.0)
            * 100.0
    }

    /// Get maximum drawdown from history
    pub fn max_drawdown(&self) -> Decimal {
        self.history
            .iter()
            .map(|s| s.drawdown)
            .max()
            .unwrap_or(Decimal::ZERO)
    }

    /// Get maximum drawdown percentage
    pub fn max_drawdown_pct(&self) -> f64 {
        if self.peak_equity == Decimal::ZERO {
            return 0.0;
        }
        (self.max_drawdown() / self.peak_equity)
            .to_f64()
            .unwrap_or(0.0)
            * 100.0
    }

    /// Get unrealized P&L by source
    pub fn unrealized_pnl_by_source(&self, source: PnLSource) -> Decimal {
        self.positions
            .values()
            .filter(|p| p.source == source)
            .map(|p| p.unrealized_pnl)
            .sum()
    }

    /// Get total exposure
    pub fn total_exposure(&self) -> Decimal {
        self.positions.values().map(|p| p.size).sum()
    }

    /// Get net exposure (long - short)
    pub fn net_exposure(&self) -> Decimal {
        self.positions
            .values()
            .map(|p| p.size * p.side.sign())
            .sum()
    }

    /// Get long exposure
    pub fn long_exposure(&self) -> Decimal {
        self.positions
            .values()
            .filter(|p| p.side == PositionSide::Long)
            .map(|p| p.size)
            .sum()
    }

    /// Get short exposure
    pub fn short_exposure(&self) -> Decimal {
        self.positions
            .values()
            .filter(|p| p.side == PositionSide::Short)
            .map(|p| p.size)
            .sum()
    }

    // ========================================================================
    // Position Queries
    // ========================================================================

    /// Get position by ID
    pub fn get_position(&self, id: &str) -> Option<&PositionPnL> {
        self.positions.get(id)
    }

    /// Get all open positions
    pub fn open_positions(&self) -> impl Iterator<Item = &PositionPnL> {
        self.positions.values()
    }

    /// Get number of open positions
    pub fn open_position_count(&self) -> usize {
        self.positions.len()
    }

    /// Get positions by source
    pub fn positions_by_source(&self, source: PnLSource) -> Vec<&PositionPnL> {
        self.positions
            .values()
            .filter(|p| p.source == source)
            .collect()
    }

    /// Get closed positions
    pub fn closed_positions(&self) -> &VecDeque<PositionPnL> {
        &self.closed_positions
    }

    /// Get total closed trade count
    pub fn total_trade_count(&self) -> usize {
        self.closed_positions.len()
    }

    // ========================================================================
    // Summary & Attribution
    // ========================================================================

    /// Get full P&L summary
    pub fn summary(&self) -> PnLSummary {
        let mut summary = self.attribution.total();
        summary.unrealized = self.total_unrealized_pnl();
        summary.max_drawdown = self.max_drawdown();
        summary.current_drawdown = self.current_drawdown();
        summary.calculate_metrics();
        summary
    }

    /// Get P&L attribution
    pub fn attribution(&self) -> &PnLAttribution {
        &self.attribution
    }

    // ========================================================================
    // History & Analysis
    // ========================================================================

    /// Get P&L history
    pub fn history(&self) -> &VecDeque<PnLSnapshot> {
        &self.history
    }

    /// Get equity curve (timestamps and equity values)
    pub fn equity_curve(&self) -> Vec<(u64, Decimal)> {
        self.history
            .iter()
            .map(|s| (s.timestamp_ms, s.equity))
            .collect()
    }

    /// Get P&L curve
    pub fn pnl_curve(&self) -> Vec<(u64, Decimal)> {
        self.history
            .iter()
            .map(|s| (s.timestamp_ms, s.total_pnl))
            .collect()
    }

    /// Get drawdown curve
    pub fn drawdown_curve(&self) -> Vec<(u64, Decimal)> {
        self.history
            .iter()
            .map(|s| (s.timestamp_ms, s.drawdown))
            .collect()
    }

    /// Calculate returns from equity curve
    pub fn returns(&self) -> Vec<f64> {
        let curve = self.equity_curve();
        if curve.len() < 2 {
            return vec![];
        }

        curve
            .windows(2)
            .filter_map(|w| {
                let prev = w[0].1.to_f64()?;
                let curr = w[1].1.to_f64()?;
                if prev > 0.0 {
                    Some((curr - prev) / prev)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Calculate Sharpe ratio (assuming daily returns)
    pub fn sharpe_ratio(&self, risk_free_rate: f64) -> f64 {
        let returns = self.returns();
        if returns.len() < 2 {
            return 0.0;
        }

        let mean: f64 = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance: f64 = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>()
            / (returns.len() - 1) as f64;
        let std_dev = variance.sqrt();

        if std_dev == 0.0 {
            return 0.0;
        }

        // Annualize (assuming ~252 trading days)
        let annualized_return = mean * 252.0;
        let annualized_std = std_dev * 252.0_f64.sqrt();

        (annualized_return - risk_free_rate) / annualized_std
    }

    // ========================================================================
    // Reset & Utilities
    // ========================================================================

    /// Reset the tracker
    pub fn reset(&mut self) {
        self.positions.clear();
        self.closed_positions.clear();
        self.history.clear();
        self.last_snapshot_ms = 0;
        self.current_price = Decimal::ZERO;
        self.peak_equity = self.config.initial_capital;
        self.attribution = PnLAttribution::default();
        self.total_realized = Decimal::ZERO;
        self.total_fees = Decimal::ZERO;
        self.position_counter = 0;
    }

    /// Get current price
    pub fn current_price(&self) -> Decimal {
        self.current_price
    }

    /// Get peak equity
    pub fn peak_equity(&self) -> Decimal {
        self.peak_equity
    }

    /// Print summary report
    pub fn print_report(&self) {
        let summary = self.summary();
        println!("REAL-TIME P&L TRACKER REPORT");
        println!("============================");
        println!("Open Positions:   {}", self.open_position_count());
        println!("Closed Trades:    {}", self.total_trade_count());
        println!();
        println!("P&L Summary:");
        println!("  Realized:       {:.4}", summary.realized);
        println!("  Unrealized:     {:.4}", summary.unrealized);
        println!("  Total:          {:.4}", summary.total);
        println!("  Fees:           {:.4}", summary.fees);
        println!("  Net:            {:.4}", summary.net);
        println!();
        println!("Performance:");
        println!("  Win Rate:       {:.1}%", summary.win_rate);
        println!("  Profit Factor:  {:.2}", summary.profit_factor);
        println!("  Risk/Reward:    {:.2}", summary.risk_reward);
        println!("  Max Drawdown:   {:.4}", summary.max_drawdown);
        println!();
        println!("Exposure:");
        println!("  Total:          {:.4}", self.total_exposure());
        println!("  Long:           {:.4}", self.long_exposure());
        println!("  Short:          {:.4}", self.short_exposure());
        println!("  Net:            {:.4}", self.net_exposure());
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a default tracker
    fn test_tracker() -> RealTimePnLTracker {
        RealTimePnLTracker::new(TrackerConfig {
            initial_capital: dec!(10000),
            snapshot_interval_ms: 0, // Disable auto snapshots for tests
            ..Default::default()
        })
    }

    // ========================================================================
    // TrackerConfig Tests
    // ========================================================================

    #[test]
    fn test_config_default() {
        let config = TrackerConfig::default();
        assert_eq!(config.max_history_size, 10000);
        assert_eq!(config.snapshot_interval_ms, 1000);
        assert_eq!(config.initial_capital, dec!(10000));
        assert!(config.use_fifo);
        assert_eq!(config.max_positions, 100);
    }

    #[test]
    fn test_config_custom() {
        let config = TrackerConfig {
            max_history_size: 5000,
            snapshot_interval_ms: 500,
            initial_capital: dec!(50000),
            use_fifo: false,
            max_positions: 50,
        };
        assert_eq!(config.max_history_size, 5000);
        assert_eq!(config.initial_capital, dec!(50000));
    }

    // ========================================================================
    // PositionSide Tests
    // ========================================================================

    #[test]
    fn test_position_side_opposite() {
        assert_eq!(PositionSide::Long.opposite(), PositionSide::Short);
        assert_eq!(PositionSide::Short.opposite(), PositionSide::Long);
    }

    #[test]
    fn test_position_side_sign() {
        assert_eq!(PositionSide::Long.sign(), Decimal::ONE);
        assert_eq!(PositionSide::Short.sign(), -Decimal::ONE);
    }

    // ========================================================================
    // PnLSource Tests
    // ========================================================================

    #[test]
    fn test_pnl_source_default() {
        assert_eq!(PnLSource::default(), PnLSource::Manual);
    }

    // ========================================================================
    // FillRecord Tests
    // ========================================================================

    #[test]
    fn test_fill_record_creation() {
        let fill = FillRecord {
            price: dec!(50000),
            remaining_size: dec!(0.1),
            original_size: dec!(0.1),
            timestamp_ms: 1000,
            fee: dec!(0.5),
        };
        assert_eq!(fill.price, dec!(50000));
        assert_eq!(fill.remaining_size, fill.original_size);
    }

    // ========================================================================
    // PositionPnL Tests
    // ========================================================================

    #[test]
    fn test_position_new() {
        let pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0.5),
            PnLSource::Directional,
            1000,
        );

        assert_eq!(pos.id, "pos_1");
        assert_eq!(pos.side, PositionSide::Long);
        assert_eq!(pos.size, dec!(0.1));
        assert_eq!(pos.avg_entry_price, dec!(50000));
        assert_eq!(pos.total_fees, dec!(0.5));
        assert_eq!(pos.source, PnLSource::Directional);
        assert!(!pos.is_closed());
    }

    #[test]
    fn test_position_update_price_long() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        // Price up
        pos.update_price(dec!(50100), 2000);
        assert_eq!(pos.unrealized_pnl, dec!(10)); // 0.1 * 100 = 10

        // Price down
        pos.update_price(dec!(49900), 3000);
        assert_eq!(pos.unrealized_pnl, dec!(-10)); // 0.1 * -100 = -10
    }

    #[test]
    fn test_position_update_price_short() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Short,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        // Price down (profit for short)
        pos.update_price(dec!(49900), 2000);
        assert_eq!(pos.unrealized_pnl, dec!(10));

        // Price up (loss for short)
        pos.update_price(dec!(50100), 3000);
        assert_eq!(pos.unrealized_pnl, dec!(-10));
    }

    #[test]
    fn test_position_add() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0.5),
            PnLSource::Directional,
            1000,
        );

        pos.add(dec!(51000), dec!(0.1), dec!(0.5), 2000);

        assert_eq!(pos.size, dec!(0.2));
        assert_eq!(pos.avg_entry_price, dec!(50500)); // (50000*0.1 + 51000*0.1) / 0.2
        assert_eq!(pos.total_fees, dec!(1.0));
    }

    #[test]
    fn test_position_reduce_partial() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.2),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        let (realized, avg_cost) = pos.reduce(dec!(0.1), dec!(50100), dec!(0), 2000);

        assert_eq!(realized, dec!(10)); // 0.1 * 100
        assert_eq!(avg_cost, dec!(50000));
        assert_eq!(pos.size, dec!(0.1));
        assert_eq!(pos.realized_pnl, dec!(10));
    }

    #[test]
    fn test_position_reduce_full() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        let (realized, _) = pos.reduce(dec!(0.1), dec!(50200), dec!(0), 2000);

        assert_eq!(realized, dec!(20)); // 0.1 * 200
        assert!(pos.is_closed());
    }

    #[test]
    fn test_position_fifo_reduce() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );
        pos.add(dec!(51000), dec!(0.1), dec!(0), 2000);

        // FIFO: first reduce from 50000 entry
        let (realized, avg_cost) = pos.reduce(dec!(0.1), dec!(52000), dec!(0), 3000);

        assert_eq!(avg_cost, dec!(50000)); // FIFO uses first fill
        assert_eq!(realized, dec!(200)); // 0.1 * 2000
    }

    #[test]
    fn test_position_total_pnl() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(1),
            PnLSource::Directional,
            1000,
        );

        pos.update_price(dec!(50100), 2000);

        // Total = unrealized (10) - fees (1) = 9
        assert_eq!(pos.total_pnl(), dec!(9));
    }

    #[test]
    fn test_position_pnl_bps() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        pos.update_price(dec!(50100), 2000);

        // P&L = 10, pnl_per_unit = 100, bps = 100/50000 * 10000 = 20 bps
        assert_eq!(pos.pnl_bps(), dec!(20));
    }

    #[test]
    fn test_position_duration() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        pos.update_price(dec!(50100), 6000);
        assert_eq!(pos.duration_ms(), 5000);
    }

    #[test]
    fn test_position_drawdown_tracking() {
        let mut pos = PositionPnL::new(
            "pos_1".to_string(),
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        pos.update_price(dec!(50200), 2000); // +20 unrealized
        assert_eq!(pos.peak_unrealized, dec!(20));

        pos.update_price(dec!(50100), 3000); // +10 unrealized (drawdown of 10)
        assert_eq!(pos.max_drawdown, dec!(10));
    }

    // ========================================================================
    // PnLSummary Tests
    // ========================================================================

    #[test]
    fn test_pnl_summary_default() {
        let summary = PnLSummary::default();
        assert_eq!(summary.realized, Decimal::ZERO);
        assert_eq!(summary.total_trades, 0);
    }

    #[test]
    fn test_pnl_summary_calculate_metrics() {
        let mut summary = PnLSummary {
            realized: dec!(100),
            unrealized: dec!(50),
            fees: dec!(10),
            winning_trades: 3,
            losing_trades: 2,
            gross_profit: dec!(150),
            gross_loss: dec!(50),
            ..Default::default()
        };

        summary.calculate_metrics();

        assert_eq!(summary.total, dec!(150));
        assert_eq!(summary.net, dec!(140));
        assert_eq!(summary.total_trades, 5);
        assert_eq!(summary.win_rate, 60.0);
        assert_eq!(summary.profit_factor, 3.0);
        assert_eq!(summary.avg_win, dec!(50));
        assert_eq!(summary.avg_loss, dec!(25));
        assert_eq!(summary.risk_reward, 2.0);
    }

    #[test]
    fn test_pnl_summary_zero_trades() {
        let mut summary = PnLSummary::default();
        summary.calculate_metrics();
        assert_eq!(summary.win_rate, 0.0);
        assert_eq!(summary.profit_factor, 0.0);
    }

    // ========================================================================
    // PnLSnapshot Tests
    // ========================================================================

    #[test]
    fn test_pnl_snapshot_creation() {
        let snapshot = PnLSnapshot {
            timestamp_ms: 1000,
            price: dec!(50000),
            realized_pnl: dec!(100),
            unrealized_pnl: dec!(50),
            total_pnl: dec!(150),
            equity: dec!(10150),
            drawdown: dec!(50),
            open_positions: 2,
            total_exposure: dec!(0.2),
            net_exposure: dec!(0.1),
        };

        assert_eq!(snapshot.timestamp_ms, 1000);
        assert_eq!(snapshot.total_pnl, dec!(150));
    }

    // ========================================================================
    // PnLAttribution Tests
    // ========================================================================

    #[test]
    fn test_attribution_get_mut() {
        let mut attr = PnLAttribution::default();
        attr.get_mut(PnLSource::MarketMaking).realized = dec!(100);
        attr.get_mut(PnLSource::Directional).realized = dec!(200);

        assert_eq!(attr.market_making.realized, dec!(100));
        assert_eq!(attr.directional.realized, dec!(200));
    }

    #[test]
    fn test_attribution_total() {
        let mut attr = PnLAttribution::default();
        attr.market_making.realized = dec!(100);
        attr.market_making.winning_trades = 5;
        attr.market_making.gross_profit = dec!(150);
        attr.directional.realized = dec!(50);
        attr.directional.winning_trades = 3;
        attr.directional.gross_profit = dec!(80);
        attr.oco.losing_trades = 2;
        attr.oco.gross_loss = dec!(30);

        let total = attr.total();
        assert_eq!(total.realized, dec!(150));
        assert_eq!(total.winning_trades, 8);
        assert_eq!(total.losing_trades, 2);
        assert_eq!(total.gross_profit, dec!(230));
    }

    // ========================================================================
    // RealTimePnLTracker Basic Tests
    // ========================================================================

    #[test]
    fn test_tracker_new() {
        let tracker = test_tracker();
        assert_eq!(tracker.open_position_count(), 0);
        assert_eq!(tracker.total_pnl(), Decimal::ZERO);
        assert_eq!(tracker.equity(), dec!(10000));
    }

    #[test]
    fn test_tracker_generate_position_id() {
        let mut tracker = test_tracker();
        assert_eq!(tracker.generate_position_id(), "pos_1");
        assert_eq!(tracker.generate_position_id(), "pos_2");
        assert_eq!(tracker.generate_position_id(), "pos_3");
    }

    #[test]
    fn test_tracker_record_fill_new_position() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0.5),
            PnLSource::Directional,
            1000,
        );

        assert_eq!(tracker.open_position_count(), 1);
        let pos = tracker.get_position("pos_1").unwrap();
        assert_eq!(pos.size, dec!(0.1));
        assert_eq!(tracker.total_fees(), dec!(0.5));
    }

    #[test]
    fn test_tracker_record_fill_add_to_position() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(51000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            2000,
        );

        assert_eq!(tracker.open_position_count(), 1);
        let pos = tracker.get_position("pos_1").unwrap();
        assert_eq!(pos.size, dec!(0.2));
        assert_eq!(pos.avg_entry_price, dec!(50500));
    }

    #[test]
    fn test_tracker_close_position() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        let realized = tracker.close_position("pos_1", dec!(50200), None, dec!(0), 2000);

        assert_eq!(realized, Some(dec!(20)));
        assert_eq!(tracker.open_position_count(), 0);
        assert_eq!(tracker.total_realized_pnl(), dec!(20));
        assert_eq!(tracker.total_trade_count(), 1);
    }

    #[test]
    fn test_tracker_close_position_partial() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.2),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        let realized = tracker.close_position("pos_1", dec!(50100), Some(dec!(0.1)), dec!(0), 2000);

        assert_eq!(realized, Some(dec!(10)));
        assert_eq!(tracker.open_position_count(), 1);
        let pos = tracker.get_position("pos_1").unwrap();
        assert_eq!(pos.size, dec!(0.1));
    }

    #[test]
    fn test_tracker_close_nonexistent() {
        let mut tracker = test_tracker();
        let realized = tracker.close_position("nonexistent", dec!(50000), None, dec!(0), 1000);
        assert!(realized.is_none());
    }

    // ========================================================================
    // Price Update Tests
    // ========================================================================

    #[test]
    fn test_tracker_on_price_update() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        tracker.on_price_update(dec!(50100), 2000);

        assert_eq!(tracker.total_unrealized_pnl(), dec!(10));
        assert_eq!(tracker.current_price(), dec!(50100));
    }

    #[test]
    fn test_tracker_on_price_update_multiple_positions() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        tracker.record_fill(
            "pos_2",
            PositionSide::Short,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::MarketMaking,
            1000,
        );

        tracker.on_price_update(dec!(50100), 2000);

        // Long: +10, Short: -10, Total: 0
        assert_eq!(tracker.total_unrealized_pnl(), Decimal::ZERO);
    }

    // ========================================================================
    // P&L Query Tests
    // ========================================================================

    #[test]
    fn test_tracker_total_pnl() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        // Close with profit
        tracker.close_position("pos_1", dec!(50100), None, dec!(0), 2000);

        // Open new position
        tracker.record_fill(
            "pos_2",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            3000,
        );

        tracker.on_price_update(dec!(50050), 4000);

        // Realized: 10, Unrealized: 5
        assert_eq!(tracker.total_realized_pnl(), dec!(10));
        assert_eq!(tracker.total_unrealized_pnl(), dec!(5));
        assert_eq!(tracker.total_pnl(), dec!(15));
    }

    #[test]
    fn test_tracker_net_pnl_with_fees() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(1),
            PnLSource::Directional,
            1000,
        );

        tracker.close_position("pos_1", dec!(50100), None, dec!(1), 2000);

        // Realized: 10, Fees: 2, Net: 8
        assert_eq!(tracker.total_pnl(), dec!(10));
        assert_eq!(tracker.total_fees(), dec!(2));
        assert_eq!(tracker.net_pnl(), dec!(8));
    }

    #[test]
    fn test_tracker_equity() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        tracker.on_price_update(dec!(50100), 2000);

        // Initial: 10000, P&L: 10, Equity: 10010
        assert_eq!(tracker.equity(), dec!(10010));
    }

    // ========================================================================
    // Drawdown Tests
    // ========================================================================

    #[test]
    fn test_tracker_drawdown() {
        let mut tracker = RealTimePnLTracker::new(TrackerConfig {
            initial_capital: dec!(10000),
            snapshot_interval_ms: 0,
            ..Default::default()
        });

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        // Price up - equity increases
        tracker.on_price_update(dec!(50200), 2000);
        tracker.take_snapshot(2000);
        assert_eq!(tracker.peak_equity(), dec!(10020)); // 10000 + 20

        // Price down - drawdown
        tracker.on_price_update(dec!(50100), 3000);
        tracker.take_snapshot(3000);
        assert_eq!(tracker.current_drawdown(), dec!(10)); // 10020 - 10010
    }

    #[test]
    fn test_tracker_drawdown_pct() {
        let mut tracker = RealTimePnLTracker::new(TrackerConfig {
            initial_capital: dec!(10000),
            snapshot_interval_ms: 0,
            ..Default::default()
        });

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(1.0),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        tracker.on_price_update(dec!(51000), 2000);
        tracker.take_snapshot(2000);
        // Peak equity: 10000 + 1000 = 11000

        tracker.on_price_update(dec!(50000), 3000);
        tracker.take_snapshot(3000);
        // Current equity: 10000 + 0 = 10000
        // Drawdown: 1000, Peak: 11000
        // Drawdown %: 1000/11000 * 100 = 9.09%

        let dd_pct = tracker.current_drawdown_pct();
        assert!((dd_pct - 9.09).abs() < 0.1);
    }

    // ========================================================================
    // Exposure Tests
    // ========================================================================

    #[test]
    fn test_tracker_exposure() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "pos_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.15),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        tracker.record_fill(
            "pos_2",
            PositionSide::Short,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::MarketMaking,
            1000,
        );

        assert_eq!(tracker.total_exposure(), dec!(0.25));
        assert_eq!(tracker.long_exposure(), dec!(0.15));
        assert_eq!(tracker.short_exposure(), dec!(0.1));
        assert_eq!(tracker.net_exposure(), dec!(0.05)); // 0.15 - 0.1
    }

    // ========================================================================
    // Attribution Tests
    // ========================================================================

    #[test]
    fn test_tracker_attribution() {
        let mut tracker = test_tracker();

        // MM trade
        tracker.record_fill(
            "mm_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::MarketMaking,
            1000,
        );
        tracker.close_position("mm_1", dec!(50100), None, dec!(0), 2000);

        // Directional trade
        tracker.record_fill(
            "dir_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            3000,
        );
        tracker.close_position("dir_1", dec!(49900), None, dec!(0), 4000);

        let attr = tracker.attribution();
        assert_eq!(attr.market_making.realized, dec!(10));
        assert_eq!(attr.market_making.winning_trades, 1);
        assert_eq!(attr.directional.realized, dec!(-10));
        assert_eq!(attr.directional.losing_trades, 1);
    }

    #[test]
    fn test_tracker_unrealized_by_source() {
        let mut tracker = test_tracker();

        tracker.record_fill(
            "mm_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::MarketMaking,
            1000,
        );

        tracker.record_fill(
            "dir_1",
            PositionSide::Long,
            dec!(50000),
            dec!(0.1),
            dec!(0),
            PnLSource::Directional,
            1000,
        );

        tracker.on_price_update(dec!(50100), 2000);

        assert_eq!(tracker.unrealized_pnl_by_source(PnLSource::MarketMaking), dec!(10));
        assert_eq!(tracker.unrealized_pnl_by_source(PnLSource::Directional), dec!(10));
        assert_eq!(tracker.unrealized_pnl_by_source(PnLSource::OCO), Decimal::ZERO);
    }

    // ========================================================================
    // Position Query Tests
    // ========================================================================

    #[test]
    fn test_tracker_positions_by_source() {
        let mut tracker = test_tracker();

        tracker.record_fill("mm_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::MarketMaking, 1000);
        tracker.record_fill("mm_2", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::MarketMaking, 1000);
        tracker.record_fill("dir_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);

        let mm_positions = tracker.positions_by_source(PnLSource::MarketMaking);
        assert_eq!(mm_positions.len(), 2);

        let dir_positions = tracker.positions_by_source(PnLSource::Directional);
        assert_eq!(dir_positions.len(), 1);
    }

    #[test]
    fn test_tracker_closed_positions() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);
        tracker.close_position("pos_1", dec!(50100), None, dec!(0), 2000);

        tracker.record_fill("pos_2", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 3000);
        tracker.close_position("pos_2", dec!(50200), None, dec!(0), 4000);

        assert_eq!(tracker.closed_positions().len(), 2);
        assert_eq!(tracker.total_trade_count(), 2);
    }

    // ========================================================================
    // History & Snapshot Tests
    // ========================================================================

    #[test]
    fn test_tracker_take_snapshot() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);
        tracker.on_price_update(dec!(50100), 2000);
        tracker.take_snapshot(2000);

        assert_eq!(tracker.history().len(), 1);
        let snap = &tracker.history()[0];
        assert_eq!(snap.timestamp_ms, 2000);
        assert_eq!(snap.price, dec!(50100));
        assert_eq!(snap.unrealized_pnl, dec!(10));
    }

    #[test]
    fn test_tracker_auto_snapshot() {
        let mut tracker = RealTimePnLTracker::new(TrackerConfig {
            initial_capital: dec!(10000),
            snapshot_interval_ms: 100,
            ..Default::default()
        });

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 0);
        tracker.on_price_update(dec!(50100), 50);
        assert_eq!(tracker.history().len(), 0);

        tracker.on_price_update(dec!(50200), 150);
        assert_eq!(tracker.history().len(), 1);
    }

    #[test]
    fn test_tracker_equity_curve() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);

        tracker.on_price_update(dec!(50100), 2000);
        tracker.take_snapshot(2000);

        tracker.on_price_update(dec!(50200), 3000);
        tracker.take_snapshot(3000);

        let curve = tracker.equity_curve();
        assert_eq!(curve.len(), 2);
        assert_eq!(curve[0], (2000, dec!(10010)));
        assert_eq!(curve[1], (3000, dec!(10020)));
    }

    #[test]
    fn test_tracker_pnl_curve() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);

        tracker.on_price_update(dec!(50100), 2000);
        tracker.take_snapshot(2000);

        tracker.on_price_update(dec!(50200), 3000);
        tracker.take_snapshot(3000);

        let curve = tracker.pnl_curve();
        assert_eq!(curve.len(), 2);
        assert_eq!(curve[0], (2000, dec!(10)));
        assert_eq!(curve[1], (3000, dec!(20)));
    }

    #[test]
    fn test_tracker_drawdown_curve() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);

        tracker.on_price_update(dec!(50200), 2000);
        tracker.take_snapshot(2000);

        tracker.on_price_update(dec!(50100), 3000);
        tracker.take_snapshot(3000);

        let curve = tracker.drawdown_curve();
        assert_eq!(curve.len(), 2);
        assert_eq!(curve[0].1, Decimal::ZERO); // Peak
        assert_eq!(curve[1].1, dec!(10)); // Drawdown
    }

    #[test]
    fn test_tracker_history_limit() {
        let mut tracker = RealTimePnLTracker::new(TrackerConfig {
            initial_capital: dec!(10000),
            snapshot_interval_ms: 0,
            max_history_size: 3,
            ..Default::default()
        });

        for i in 0..5 {
            tracker.take_snapshot(i as u64 * 1000);
        }

        assert_eq!(tracker.history().len(), 3);
        assert_eq!(tracker.history()[0].timestamp_ms, 2000); // First two removed
    }

    // ========================================================================
    // Returns & Sharpe Tests
    // ========================================================================

    #[test]
    fn test_tracker_returns() {
        let mut tracker = test_tracker();

        tracker.take_snapshot(1000); // 10000
        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1500);
        tracker.on_price_update(dec!(51000), 2000);
        tracker.take_snapshot(2000); // 10100
        tracker.on_price_update(dec!(50500), 3000);
        tracker.take_snapshot(3000); // 10050

        let returns = tracker.returns();
        assert_eq!(returns.len(), 2);
        assert!((returns[0] - 0.01).abs() < 0.001); // 1% return
        assert!((returns[1] - (-0.00495)).abs() < 0.001); // ~-0.5% return
    }

    #[test]
    fn test_tracker_sharpe_ratio() {
        let mut tracker = test_tracker();

        // Create some returns
        tracker.take_snapshot(1000);
        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1500);

        for i in 2..12 {
            let price = dec!(50000) + Decimal::from(i * 10);
            tracker.on_price_update(price, i as u64 * 1000);
            tracker.take_snapshot(i as u64 * 1000);
        }

        let sharpe = tracker.sharpe_ratio(0.0);
        // Should be positive since we have positive returns
        assert!(sharpe > 0.0);
    }

    // ========================================================================
    // Reset Tests
    // ========================================================================

    #[test]
    fn test_tracker_reset() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(1), PnLSource::Directional, 1000);
        tracker.close_position("pos_1", dec!(50100), None, dec!(1), 2000);
        tracker.take_snapshot(2000);

        tracker.reset();

        assert_eq!(tracker.open_position_count(), 0);
        assert_eq!(tracker.total_trade_count(), 0);
        assert_eq!(tracker.total_realized_pnl(), Decimal::ZERO);
        assert_eq!(tracker.total_fees(), Decimal::ZERO);
        assert_eq!(tracker.history().len(), 0);
        assert_eq!(tracker.equity(), dec!(10000));
    }

    // ========================================================================
    // Summary Tests
    // ========================================================================

    #[test]
    fn test_tracker_summary() {
        let mut tracker = test_tracker();

        // Winning trade
        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);
        tracker.close_position("pos_1", dec!(50100), None, dec!(0), 2000);

        // Losing trade
        tracker.record_fill("pos_2", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 3000);
        tracker.close_position("pos_2", dec!(49950), None, dec!(0), 4000);

        // Open position
        tracker.record_fill("pos_3", PositionSide::Long, dec!(50000), dec!(0.1), dec!(1), PnLSource::Directional, 5000);
        tracker.on_price_update(dec!(50050), 6000);

        let summary = tracker.summary();
        assert_eq!(summary.realized, dec!(5)); // 10 - 5
        assert_eq!(summary.unrealized, dec!(5));
        assert_eq!(summary.winning_trades, 1);
        assert_eq!(summary.losing_trades, 1);
        assert_eq!(summary.win_rate, 50.0);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_tracker_zero_size_position() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0), dec!(0), PnLSource::Directional, 1000);

        let pos = tracker.get_position("pos_1").unwrap();
        assert_eq!(pos.pnl_bps(), Decimal::ZERO);
    }

    #[test]
    fn test_tracker_max_positions_limit() {
        let mut tracker = RealTimePnLTracker::new(TrackerConfig {
            max_positions: 2,
            ..Default::default()
        });

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);
        tracker.record_fill("pos_2", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);
        tracker.record_fill("pos_3", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);

        assert_eq!(tracker.open_position_count(), 2); // pos_3 not added
    }

    #[test]
    fn test_tracker_opposite_side_closes() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);

        // Opposite side fill on same position reduces it
        tracker.record_fill("pos_1", PositionSide::Short, dec!(50100), dec!(0.1), dec!(0), PnLSource::Directional, 2000);

        assert_eq!(tracker.open_position_count(), 0);
        assert_eq!(tracker.total_realized_pnl(), dec!(10));
    }

    #[test]
    fn test_tracker_reduce_more_than_available() {
        let mut tracker = test_tracker();

        tracker.record_fill("pos_1", PositionSide::Long, dec!(50000), dec!(0.1), dec!(0), PnLSource::Directional, 1000);

        // Try to close more than available
        let realized = tracker.close_position("pos_1", dec!(50100), Some(dec!(0.5)), dec!(0), 2000);

        assert_eq!(realized, Some(dec!(10))); // Only 0.1 was closed
        assert_eq!(tracker.open_position_count(), 0);
    }
}

# OCO Manager & Backtest Harness Integration Analysis

**Project:** Ingestor (MARS - Momentum Adaptive Regime Strategy)  
**Date:** December 17, 2025  
**Phase:** v0.2 Risk Management Integration  
**Task:** Integrate OCOManager into BacktestEngine for testing directional trading strategies

---

## Executive Summary

This document provides a comprehensive technical analysis of:
1. **OCO Manager Implementation** - Public interface and capabilities
2. **Backtest Harness Architecture** - Current design and processing pipeline
3. **Integration Points** - Where OCO should be integrated
4. **Key Challenges** - Trade-off analysis and design considerations
5. **Recommended Integration Strategy** - Step-by-step implementation plan

**Key Finding:** The backtest harness currently processes fills through a fill simulator and MM algorithm. OCO integration requires adding a risk management layer that checks for TP/SL triggers after each price update, managing position lifecycle from entry through exit.

---

## Part 1: OCO Manager Implementation

### Location
**File:** `/home/onat/Ingestor/src/trading/oco_manager.rs`  
**Lines:** 1,285 (comprehensive, well-tested implementation)

### Public Interface

#### Core Types

```rust
// Side of the trade
pub enum Side {
    Buy,      // Long position
    Sell,     // Short position
}

// Type of OCO trigger
pub enum TriggerType {
    TakeProfit,   // TP level hit
    StopLoss,     // SL level hit
}

// Result of an OCO trigger (what happened when TP/SL hit)
pub struct OCOTrigger {
    pub order_id: String,
    pub trigger_type: TriggerType,
    pub side: Side,
    pub entry_price: Decimal,
    pub exit_price: Decimal,        // TP or SL price
    pub size: Decimal,
    pub realized_pnl: Decimal,      // $ P&L
    pub pnl_bps: Decimal,           // basis points P&L
    pub duration_ms: u64,           // trade duration
}

// Single OCO order with TP and SL levels
pub struct OCOOrder {
    pub id: String,
    pub side: Side,
    pub entry_price: Decimal,
    pub size: Decimal,
    pub take_profit_price: Decimal,
    pub stop_loss_price: Decimal,
    pub created_at: u64,            // Unix ms timestamp
    pub metadata: Option<String>,   // regime tag, etc.
}
```

#### Key Methods on OCOOrder

```rust
// Creation
OCOOrder::new(id, side, entry_price, size, tp_price, sl_price)
OCOOrder::with_timestamp(id, side, entry_price, size, tp_price, sl_price, timestamp)
OCOOrder::from_bps(id, side, entry_price, size, tp_bps, sl_bps)  // TP/SL in bps

// Trigger checking
is_tp_triggered(current_price: Decimal) -> bool
is_sl_triggered(current_price: Decimal) -> bool

// P&L calculation
calculate_pnl(exit_price: Decimal) -> Decimal
calculate_pnl_bps(exit_price: Decimal) -> Decimal
distance_to_tp_bps(current_price: Decimal) -> Decimal
distance_to_sl_bps(current_price: Decimal) -> Decimal

// Metadata attachment
with_metadata(metadata: String) -> Self
```

#### OCOManager Main API

```rust
pub struct OCOManager {
    orders: HashMap<String, OCOOrder>,
    stats: OCOStats,
    history: Vec<OCOTrigger>,
    max_concurrent_orders: usize,
    max_history_size: usize,
}

impl OCOManager {
    // Creation
    pub fn new() -> Self
    pub fn with_config(max_concurrent_orders: usize, max_history_size: usize) -> Self
    
    // Order management
    pub fn add_order(&mut self, order: OCOOrder) -> Result<(), OCOError>
    pub fn remove_order(&mut self, order_id: &str) -> Option<OCOOrder>
    pub fn get_order(&self, order_id: &str) -> Option<&OCOOrder>
    pub fn active_order_count(&self) -> usize
    pub fn has_active_orders(&self) -> bool
    pub fn active_orders(&self) -> impl Iterator<Item = &OCOOrder>
    pub fn clear_orders(&mut self)
    
    // Trigger checking (PRIMARY METHOD FOR BACKTESTING)
    pub fn check_triggers(&mut self, current_price: Decimal) -> Vec<OCOTrigger>
    pub fn check_triggers_at_time(
        &mut self,
        current_price: Decimal,
        current_time_ms: u64,
    ) -> Vec<OCOTrigger>
    
    // Statistics & history
    pub fn stats(&self) -> &OCOStats
    pub fn history(&self) -> &[OCOTrigger]
    pub fn reset_stats(&mut self)
    
    // Exposure tracking
    pub fn unrealized_pnl(&self, current_price: Decimal) -> Decimal
    pub fn total_exposure(&self) -> Decimal
    pub fn net_exposure(&self) -> Decimal
}

pub struct OCOStats {
    pub total_orders: u64,
    pub tp_triggers: u64,
    pub sl_triggers: u64,
    pub total_pnl: Decimal,
    pub total_wins: Decimal,
    pub total_losses: Decimal,
    pub avg_duration_ms: f64,
    pub max_drawdown: Decimal,
    pub peak_pnl: Decimal,
    
    // Methods
    pub fn win_rate(&self) -> f64
    pub fn avg_win(&self) -> Decimal
    pub fn avg_loss(&self) -> Decimal
    pub fn profit_factor(&self) -> f64
    pub fn risk_reward_ratio(&self) -> f64
}
```

#### Error Handling

```rust
pub enum OCOError {
    MaxOrdersReached { max: usize },
    DuplicateOrderId { id: String },
    OrderNotFound { id: String },
}
```

### Design Characteristics

#### Strengths
1. **Comprehensive Testing**: 49 unit tests covering all scenarios
2. **Timestamp Awareness**: `check_triggers_at_time()` supports backtesting with custom timestamps
3. **Statistics Tracking**: Automatic P&L, win rate, drawdown calculation
4. **Metadata Support**: Can attach regime info to orders for analysis
5. **Exposure Tracking**: Methods for unrealized PnL, net exposure, total exposure
6. **Flexible Sizing**: Support for both absolute prices and basis point offsets
7. **History Management**: Configurable history retention (useful for analysis)

#### Current Limitations for Backtest Integration
1. **No Portfolio Integration**: Tracks individual orders but no cross-order position management
2. **No Entry Signal**: Manager doesn't decide when to enter, only when to exit
3. **No Position Sizing**: Relies on external logic to determine order size
4. **No Regime Awareness**: Generic trigger checking, doesn't adapt to market conditions
5. **No Partial Position Closing**: Each order is atomic (all or nothing)

### Example Usage from Tests

```rust
// Basic long trade with TP/SL
let mut manager = OCOManager::new();
let order = OCOOrder::new(
    "trade_1".to_string(),
    Side::Buy,
    dec!(50000),     // Entry price
    dec!(1.0),       // Size (1 BTC)
    dec!(50100),     // TP: +20 bps
    dec!(49900),     // SL: -20 bps
);
manager.add_order(order)?;

// Check for triggers at different prices
let triggers = manager.check_triggers_at_time(dec!(50050), 1000);  // No trigger yet
let triggers = manager.check_triggers_at_time(dec!(50100), 2000);  // TP hit!

// Analyze results
assert_eq!(triggers[0].trigger_type, TriggerType::TakeProfit);
assert_eq!(triggers[0].realized_pnl, dec!(100));  // 1.0 * (50100 - 50000)
assert_eq!(triggers[0].pnl_bps, dec!(20));

// Check stats after trades
let stats = manager.stats();
println!("Win rate: {:.1}%", stats.win_rate());
println!("Risk/reward: {:.2}x", stats.risk_reward_ratio());
```

---

## Part 2: Backtest Harness Architecture

### Location
**File:** `/home/onat/Ingestor/src/backtest/harness.rs`  
**Lines:** ~700+ (implements full backtesting engine)

### Current Architecture

```
Historical Data (Parquet)
        ↓
    Replay Engine
        ↓
  ReplayEvent (timestamp, FeaturesSnapshot)
        ↓
    BacktestEngine::process_event()
        ├─→ Extract market data from FeaturesSnapshot
        ├─→ Compute quotes via MarketMakingAlgorithm
        ├─→ FillSimulator::update_quotes()
        ├─→ Detect fills (price touches quote level)
        ├─→ BacktestEngine::process_fill()
        │   ├─→ Calculate PnL (if closing position)
        │   ├─→ Record in TradeLog
        │   └─→ Update algorithm inventory
        ├─→ Algorithm::update_mark_to_market()
        └─→ Record equity periodically
        ↓
    BacktestResults
        ├─→ PerformanceMetrics (Sharpe, drawdown, etc.)
        ├─→ TradeLog (all trades)
        └─→ EquityCurve (equity over time)
```

### Key Data Structures

#### BacktestConfig
```rust
pub struct BacktestConfig {
    pub replay: ReplayConfig,              // Data source
    pub mm: MMConfig,                      // MM algorithm config
    pub simulator: SimulatorConfig,        // Legacy simulator config
    pub fill_sim: FillSimulatorConfig,     // Realistic fill simulation
    pub initial_capital: Decimal,          // Starting capital
    pub risk_free_rate: f64,               // For Sharpe calculation
    pub equity_sample_interval: usize,     // Record equity every N events
    pub verbose: bool,
    pub use_realistic_fills: bool,         // Use realistic vs naive fills
}
```

#### BacktestEngine
```rust
pub struct BacktestEngine {
    config: BacktestConfig,
    replay: ParquetReplay,
    algorithm: Box<dyn MarketMakingAlgorithm>,  // Polymorphic!
    fill_sim: FillSimulator,
    
    // State
    trade_log: TradeLog,
    equity_curve: EquityCurve,
    events_processed: usize,
    fills_generated: usize,
    last_mid_price: Option<Decimal>,
}

impl BacktestEngine {
    pub fn new(config: BacktestConfig) -> Self
    pub fn with_algorithm(
        config: BacktestConfig,
        algorithm: Box<dyn MarketMakingAlgorithm>,
    ) -> Self
    pub fn load_data(&mut self) -> Result<usize>
    pub fn run(&mut self) -> Result<BacktestResults>
    
    // Private methods
    fn process_event(&mut self, event: &ReplayEvent) -> Result<()>
    fn process_fill(&mut self, fill: Fill, timestamp_ms: u64) -> Result<()>
    fn calculate_fill_pnl(&self, fill: &Fill) -> Option<Decimal>
    fn record_equity(&mut self, timestamp_ms: i64)
}
```

#### BacktestResults
```rust
pub struct BacktestResults {
    pub config: BacktestConfig,
    pub metrics: PerformanceMetrics,
    pub trade_log: TradeLog,
    pub equity_curve: EquityCurve,
    pub events_processed: usize,
    pub fills_generated: usize,
    pub fill_stats: FillStats,
    
    // Methods
    pub fn save_json(&self, path: &str) -> Result<()>
    pub fn print_summary(&self)
    pub fn compute_statistics(&self, num_trials: usize) -> StatisticalReport
}
```

### Event Processing Pipeline

The core loop in `BacktestEngine::run()` processes each event:

```rust
while let Some(event) = self.replay.next() {
    self.process_event(&event)?;
    
    // Record equity periodically
    if self.events_processed % self.config.equity_sample_interval == 0 {
        self.record_equity(event.timestamp_ms);
    }
}
```

Each event goes through `process_event()`:

1. **Extract Market Data** from FeaturesSnapshot
   - mid_price, spread, volatility, entropy, trade flow, etc.

2. **Compute Quotes** via algorithm's `compute_quotes()`
   - Returns MMQuotes { bid, ask, regime, fair_value, ... }

3. **Update Fill Simulator** with new quotes

4. **Simulate Fills** (two modes available)
   - Realistic: Queue position, adverse selection, trade intensity
   - Naive: Price touches level = fill

5. **Process Each Fill**
   - Calculate PnL (if closing existing position)
   - Record in TradeLog
   - Update algorithm state (inventory, avg entry price)

6. **Update Mark-to-Market**
   - Algorithm tracks current price for unrealized PnL

7. **Record Equity** periodically for equity curve

### Current Fill Processing

```rust
fn process_fill(&mut self, fill: Fill, timestamp_ms: u64) -> Result<()> {
    let fee_rate = self.config.fill_sim.fee_rate;
    let fee = fill.price * fill.size * fee_rate;
    
    // Calculate PnL only if closing position
    let pnl = self.calculate_fill_pnl(&fill);
    
    // Record in trade log
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
    
    // Update algorithm (inventory, avg entry)
    self.algorithm.process_fill(fill, fee_rate);
    self.fills_generated += 1;
    
    Ok(())
}
```

### Trade Log Structure

```rust
pub struct TradeRecord {
    pub timestamp_ms: i64,
    pub side: TradeSide,          // Buy or Sell
    pub price: Decimal,
    pub size: Decimal,
    pub fee: Decimal,
    pub pnl: Option<Decimal>,     // Only for closing trades
}

pub struct TradeLog {
    pub trades: Vec<TradeRecord>,
}
```

### Key Files in Backtest Module

| File | Purpose | Key Exports |
|------|---------|------------|
| `harness.rs` | Main backtest engine | BacktestEngine, BacktestConfig, BacktestResults |
| `replay.rs` | Parquet data reading | ParquetReplay, ReplayEvent |
| `fill_simulator.rs` | Realistic fill modeling | FillSimulator, MarketState, FillEvent |
| `metrics.rs` | Performance calculation | PerformanceMetrics, TradeLog, EquityCurve |
| `walk_forward.rs` | Walk-forward validation | WalkForwardEngine |
| `grid_search.rs` | Parameter optimization | GridSearchEngine |
| `statistics.rs` | Statistical significance | StatisticalReport, compute_statistics |

---

## Part 3: Integration Analysis

### Current State: Why OCO Isn't Integrated Yet

The backtest harness currently:
1. ✅ Simulates market making (A-S algorithm quoting)
2. ✅ Simulates realistic fills based on price touches
3. ❌ Does NOT implement OCO (no TP/SL triggers)
4. ❌ Does NOT enforce position lifecycle management
5. ❌ Does NOT track entry-to-exit as atomic trades

### What OCO Integration Would Add

OCO integration would enable:
1. **Directional Trading**: Enter long/short with defined risk
2. **Bounded Losses**: SL ensures max loss per trade
3. **Profit Taking**: TP ensures profits are locked
4. **Position Lifecycle**: Track from entry through exit
5. **Risk Metrics**: Win rate, risk-reward ratios, max drawdown by position

### Integration Architecture (Proposed)

```
Historical Data (Parquet)
        ↓
    Replay Engine
        ↓
  ReplayEvent (timestamp, FeaturesSnapshot)
        ↓
    BacktestEngine::process_event()
        ├─→ Extract market data
        ├─→ [NEW] Run trading strategy
        │   └─→ Generates entry decisions (side, size, TP, SL)
        ├─→ [NEW] Add entry to OCOManager
        ├─→ [NEW] Check OCO triggers on current price
        │   └─→ Triggers may exit positions
        ├─→ Compute quotes (if running MM alongside)
        ├─→ Simulate MM fills (optional)
        ├─→ Process fills
        └─→ Record equity
        ↓
    BacktestResults
        ├─→ Trade analysis from TradeLog
        ├─→ OCO statistics from OCOManager
        └─→ Combined metrics
```

### Three Integration Approaches

#### Approach A: Minimal Integration (Recommended for Phase 2)

**OCO handles only order exits, backtest harness manages entry**

```rust
// In BacktestEngine::process_event()

// 1. Strategy decides to enter (external logic)
if let Some(entry_signal) = self.strategy.should_enter(&snapshot) {
    let order = OCOOrder::from_bps(
        entry_signal.order_id,
        entry_signal.side,
        current_price,
        entry_signal.size,
        entry_signal.tp_bps,
        entry_signal.sl_bps,
    );
    self.oco_manager.add_order(order)?;
}

// 2. Check for exits via OCO at current price
let triggers = self.oco_manager.check_triggers_at_time(
    current_price,
    timestamp_ms,
);

// 3. Process each triggered position
for trigger in triggers {
    self.process_oco_trigger(trigger, timestamp_ms)?;
}

// 4. Optional: still simulate MM fills (if not in directional trade)
if !self.oco_manager.has_active_orders() {
    // Run MM algorithm
}
```

Pros:
- Minimal code changes
- OCO only does what it does best (exit management)
- Strategy logic remains pluggable
- Works with both directional trades AND market making

Cons:
- Entry signal logic external to backtest harness
- Requires separate TradingStrategy trait

#### Approach B: Full Integration (Ambitious, for Phase 3)

**OCO handles both entry and exit via TradingStrategy**

```rust
// Strategy trait defines full lifecycle
pub trait TradingStrategy {
    fn on_features(&mut self, features: &FeaturesSnapshot) -> StrategyDecision;
    fn on_trigger(&mut self, trigger: &OCOTrigger);
}

pub enum StrategyDecision {
    Hold,
    Enter {
        side: Side,
        size: Decimal,
        tp_bps: Decimal,
        sl_bps: Decimal,
    },
    Exit {
        reason: ExitReason,
    },
}
```

Pros:
- Clean abstraction
- Full control over position lifecycle
- Composable with other strategies

Cons:
- Requires strategy trait implementation
- More complex test harness

#### Approach C: Hybrid (Best of Both)

**Phase 2: Minimal integration with market-making baseline**  
**Phase 3: Add strategy trait for directional trades**

---

## Part 4: Data Flow Analysis

### Input: FeaturesSnapshot

```rust
pub struct FeaturesSnapshot {
    // Price data
    pub mid_price: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub microprice: Option<Decimal>,
    
    // Trend indicators (new in v0.2)
    pub momentum: Option<Decimal>,
    pub monotonicity: Option<Decimal>,
    pub hurst_exponent: Option<Decimal>,
    pub ma_crossover: Option<Decimal>,
    
    // Volatility
    pub realized_volatility_100: Option<f64>,
    
    // Entropy (regime detection)
    pub tick_entropy_1s: Option<Decimal>,
    pub tick_entropy_5s: Option<Decimal>,
    pub tick_entropy_10s: Option<Decimal>,
    
    // Trade flow
    pub aggr_ratio_100: Option<Decimal>,
    pub trade_rate_10s: Option<f64>,
    pub vwap: Option<Decimal>,
    
    // Order book
    pub bid_volume_001: Option<Decimal>,
    pub ask_volume_001: Option<Decimal>,
    pub imbalance: Option<Decimal>,
    
    // ... 50+ more fields
}
```

### Processing Flow in OCO Context

```
FeaturesSnapshot
    ├─→ [Strategy] Detect regime/trend
    │   ├─→ momentum > threshold? → "trending_up"
    │   ├─→ monotonicity > 0.7?  → "strong_trend"
    │   └─→ entropy < 0.4?       → "directional"
    │
    ├─→ [Strategy] Generate signal
    │   └─→ "Trending up" + "high entropy" → Enter long
    │
    ├─→ [BacktestEngine] Create OCO order
    │   ├─→ side: Buy
    │   ├─→ entry_price: mid_price
    │   ├─→ tp_price: mid_price * (1 + 0.0010)  // +10 bps
    │   ├─→ sl_price: mid_price * (1 - 0.0005)  // -5 bps
    │   └─→ metadata: "regime:trending_up"
    │
    ├─→ [OCOManager] Add order to active set
    │
    └─→ [For each subsequent event]
        ├─→ new_price from snapshot
        ├─→ check_triggers_at_time(new_price, timestamp)
        ├─→ If trigger → process exit
        └─→ If no trigger → position still active
```

---

## Part 5: Integration Implementation Details

### Step 1: Add OCOManager Field to BacktestEngine

```rust
// In backtest/harness.rs
pub struct BacktestEngine {
    config: BacktestConfig,
    replay: ParquetReplay,
    algorithm: Box<dyn MarketMakingAlgorithm>,
    fill_sim: FillSimulator,
    
    // NEW
    oco_manager: OCOManager,  // Add this field
    
    trade_log: TradeLog,
    // ... rest of fields
}

impl BacktestEngine {
    pub fn new(config: BacktestConfig) -> Self {
        // ... existing code ...
        
        let oco_manager = OCOManager::with_config(
            config.oco.max_concurrent_orders,
            config.oco.max_history_size,
        );
        
        Self {
            // ... existing fields ...
            oco_manager,
            // ... rest ...
        }
    }
}
```

### Step 2: Update BacktestConfig

```rust
// In backtest/harness.rs
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub replay: ReplayConfig,
    pub mm: MMConfig,
    pub simulator: SimulatorConfig,
    pub fill_sim: FillSimulatorConfig,
    
    // NEW
    pub oco: OCOConfig,  // Add this
    
    pub initial_capital: Decimal,
    pub risk_free_rate: f64,
    pub equity_sample_interval: usize,
    pub verbose: bool,
    pub use_realistic_fills: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OCOConfig {
    pub max_concurrent_orders: usize,
    pub max_history_size: usize,
    pub enabled: bool,
}

impl Default for OCOConfig {
    fn default() -> Self {
        Self {
            max_concurrent_orders: 10,  // Max 10 concurrent positions
            max_history_size: 1000,
            enabled: true,
        }
    }
}
```

### Step 3: Add OCO Trigger Checking in Process Event

```rust
// In BacktestEngine::process_event()
fn process_event(&mut self, event: &ReplayEvent) -> Result<()> {
    let snap = &event.snapshot;
    let timestamp_ms = event.timestamp_ms as u64;
    
    // Extract current mid price
    let mid_price = match snap.mid_price {
        Some(p) if p > dec!(0) => p,
        _ => {
            self.events_processed += 1;
            return Ok(());
        }
    };
    
    // NEW: Check for OCO triggers BEFORE processing MM logic
    if self.config.oco.enabled && self.oco_manager.has_active_orders() {
        let triggers = self.oco_manager.check_triggers_at_time(
            mid_price,
            timestamp_ms,
        );
        
        // Process each triggered position exit
        for trigger in triggers {
            self.process_oco_trigger(trigger, timestamp_ms)?;
        }
    }
    
    // ... rest of existing process_event logic ...
    // (compute quotes, simulate fills, etc.)
    
    Ok(())
}
```

### Step 4: Add OCO Trigger Processing

```rust
// In BacktestEngine
fn process_oco_trigger(&mut self, trigger: OCOTrigger, timestamp_ms: u64) -> Result<()> {
    // Create a synthetic fill for the OCO exit
    let exit_fill = Fill {
        side: match trigger.side {
            oco_manager::Side::Buy => QuoteSide::Ask,   // Close long = sell
            oco_manager::Side::Sell => QuoteSide::Bid,  // Close short = buy
        },
        price: trigger.exit_price,
        size: trigger.size,
        timestamp_ms,
    };
    
    // Process the exit fill
    self.process_fill(exit_fill, timestamp_ms)?;
    
    // Log the trigger (for analysis)
    if self.config.verbose {
        println!(
            "[OCO] {} {:?} @ {} ({})",
            trigger.order_id,
            trigger.trigger_type,
            trigger.exit_price,
            trigger.pnl_bps,
        );
    }
    
    Ok(())
}
```

### Step 5: Add Method to Enter OCO Positions (Strategy Interface)

For Phase 2, this would be called from external strategy logic:

```rust
// New public method on BacktestEngine
pub fn enter_position(
    &mut self,
    order_id: String,
    side: Side,
    entry_price: Decimal,
    size: Decimal,
    tp_bps: Decimal,
    sl_bps: Decimal,
    metadata: Option<String>,
) -> Result<(), OCOError> {
    let mut order = OCOOrder::from_bps(
        order_id,
        side,
        entry_price,
        size,
        tp_bps,
        sl_bps,
    );
    
    if let Some(meta) = metadata {
        order = order.with_metadata(meta);
    }
    
    self.oco_manager.add_order(order)
}
```

### Step 6: Update BacktestResults to Include OCO Stats

```rust
// In BacktestResults
impl BacktestResults {
    /// Get OCO statistics
    pub fn oco_stats(&self) -> Option<&OCOStats> {
        Some(self.oco_manager.stats())
    }
    
    /// Get OCO trade history
    pub fn oco_history(&self) -> Option<&[OCOTrigger]> {
        Some(self.oco_manager.history())
    }
}

// Update print_summary to include OCO stats
pub fn print_summary(&self) {
    // ... existing summary ...
    
    if let Some(stats) = self.oco_stats() {
        println!("\nOCO PERFORMANCE");
        println!("  Total positions: {}", stats.total_orders);
        println!("  Win rate: {:.1}%", stats.win_rate());
        println!("  Risk/reward: {:.2}x", stats.risk_reward_ratio());
        println!("  Max drawdown: {:.4}", stats.max_drawdown);
        println!("  Total P&L: {:.4}", stats.total_pnl);
    }
}
```

---

## Part 6: Testing Strategy

### Existing Tests to Reference

| File | Coverage |
|------|----------|
| `/home/onat/Ingestor/tests/backtest_test.rs` | Basic harness (5 tests) |
| `/home/onat/Ingestor/src/trading/oco_manager.rs` | 49 unit tests (comprehensive) |

### New Tests Needed

#### Test 1: Basic OCO Entry and Exit
```rust
#[test]
fn test_backtest_with_oco_simple() {
    let config = BacktestConfig {
        oco: OCOConfig {
            enabled: true,
            ..Default::default()
        },
        ..Default::default()
    };
    
    let mut engine = BacktestEngine::new(config);
    
    // Simulate entering a position
    engine.enter_position(
        "test_1".to_string(),
        Side::Buy,
        dec!(50000),
        dec!(1.0),
        dec!(10),   // 10 bps TP
        dec!(5),    // 5 bps SL
        Some("test".to_string()),
    ).unwrap();
    
    assert!(engine.oco_manager.has_active_orders());
    assert_eq!(engine.oco_manager.active_order_count(), 1);
}
```

#### Test 2: OCO Trigger at TP
```rust
#[test]
fn test_backtest_oco_tp_trigger() {
    // Setup with entry at 50000, TP at 50100, SL at 49950
    // Simulate price moving to 50100
    // Verify trigger recorded and position closed
}
```

#### Test 3: OCO Trigger at SL
```rust
#[test]
fn test_backtest_oco_sl_trigger() {
    // Setup with entry at 50000, TP at 50100, SL at 49950
    // Simulate price moving to 49950
    // Verify SL triggered and loss recorded
}
```

#### Test 4: Multiple Concurrent OCO Orders
```rust
#[test]
fn test_backtest_oco_multiple_positions() {
    // Enter 3 positions simultaneously
    // Close 1 via TP, 1 via SL, let 1 remain
    // Verify stats correctly aggregated
}
```

#### Test 5: OCO with Partial Fills
```rust
#[test]
fn test_backtest_oco_with_partial_fills() {
    // Enter position with size 1.0
    // Get partial fill (0.6)
    // Verify position tracking and P&L calculation
}
```

#### Test 6: Integration with MM Algorithm
```rust
#[test]
fn test_backtest_oco_plus_mm() {
    // Run backtest with BOTH OCO (directional) and MM quotes active
    // Verify OCO positions exit cleanly even with concurrent MM fills
}
```

#### Test 7: Realistic Backtesting with Real Data
```rust
#[test]
fn test_backtest_oco_realistic() {
    // Load actual parquet data
    // Run simple trend-following strategy with OCO
    // Verify results are reasonable (no panics, sensible metrics)
}
```

---

## Part 7: Key Design Decisions

### Decision 1: When to Check Triggers

**Options:**
- A) At start of event processing (before MM)
- B) At end of event processing (after MM)
- C) As part of fill processing

**Decision: Option A (before MM)**

Rationale: OCO exits should take priority over MM quotes. If a position hits TP/SL, we should exit immediately rather than update quotes.

### Decision 2: Multiple Entry Methods

**Options:**
- A) Only allow programmatic entry via `enter_position()`
- B) Allow both MM fills that become OCO entries
- C) Auto-convert all fills to OCO orders

**Decision: Option A (programmatic only for Phase 2)**

Rationale: Keep strategy logic explicit. Phase 3 can add automatic entry via strategies.

### Decision 3: Position vs OCO Order Tracking

**Current:** OCOManager tracks individual orders  
**Question:** Should we also track positions (entry + management)?

**Decision: Use OCOManager as-is in Phase 2**

OCOManager already tracks P&L, duration, and triggers. A separate Position Manager can be added in Phase 3 for more complex portfolio management.

### Decision 4: Historical Data Requirements

**Question:** Do we need to modify ReplayEvent to include trend features?

**Answer: No** - v0.2 already added trend features to FeaturesSnapshot:
- momentum
- monotonicity
- hurst_exponent
- ma_crossover

These can be used for entry signal generation.

---

## Part 8: Compatibility Matrix

### What Already Works

| Component | Status | Notes |
|-----------|--------|-------|
| OCOManager (core) | ✅ Complete | 1,285 lines, 49 tests |
| PositionManager | ✅ Complete | Complements OCOManager |
| FeaturesSnapshot | ✅ Complete | Includes trend features |
| ReplayEvent | ✅ Complete | Supports custom timestamps |
| BacktestEngine | ✅ Complete | Extensible via traits |
| Fill simulator | ✅ Complete | Realistic fills |
| Trade logging | ✅ Complete | Tracks all trades |
| Metrics calculation | ✅ Complete | Sharpe, drawdown, etc. |

### What Needs Integration

| Component | Phase | Work |
|-----------|-------|------|
| OCO + Backtest | Phase 2 | Add trigger checking |
| Trading strategy trait | Phase 2 | Define entry signals |
| Strategy implementation | Phase 2 | Trend-following strategy |
| Paper trading sim | Phase 3 | Real-time OCO + MM |
| Walk-forward OCO | Phase 3 | Optimize TP/SL ratios |

---

## Part 9: Code Examples

### Example 1: Minimal Integration Test

```rust
use ingestor::backtest::{BacktestEngine, BacktestConfig, OCOConfig};
use ingestor::execution::{Side};
use rust_decimal_macros::dec;

#[test]
fn test_oco_in_backtest() {
    let config = BacktestConfig {
        oco: OCOConfig {
            enabled: true,
            max_concurrent_orders: 10,
            max_history_size: 1000,
        },
        ..Default::default()
    };
    
    let mut engine = BacktestEngine::new(config);
    
    // Entry at 50000, TP at +20 bps, SL at -10 bps
    engine.enter_position(
        "trade1".to_string(),
        Side::Buy,
        dec!(50000),
        dec!(1.0),
        dec!(20),
        dec!(10),
        Some("test_trade".to_string()),
    ).unwrap();
    
    // Simulate prices
    // Price moves to 50100 → TP triggered
    // verify position closed with +200 P&L
}
```

### Example 2: Strategy-Driven OCO Entry

```rust
// Future: TradingStrategy trait
pub trait TradingStrategy {
    fn on_snapshot(&mut self, snap: &FeaturesSnapshot) -> Signal;
}

pub enum Signal {
    None,
    EnterLong { size: Decimal, tp_bps: Decimal, sl_bps: Decimal },
    EnterShort { size: Decimal, tp_bps: Decimal, sl_bps: Decimal },
}

// In process_event:
if let Signal::EnterLong { size, tp_bps, sl_bps } = 
    self.strategy.on_snapshot(&snap) {
    
    let order = OCOOrder::from_bps(
        format!("strat_{}", self.trade_counter),
        Side::Buy,
        mid_price,
        size,
        tp_bps,
        sl_bps,
    ).with_metadata(format!("momentum: {}", snap.momentum.unwrap_or_default()));
    
    self.oco_manager.add_order(order)?;
    self.trade_counter += 1;
}
```

### Example 3: Analyzing Results

```rust
let results = engine.run()?;

// Print standard metrics
results.print_summary();

// Access OCO-specific stats
let stats = results.oco_manager.stats();
println!("Win rate: {:.1}%", stats.win_rate());
println!("Avg win: {}", stats.avg_win());
println!("Avg loss: {}", stats.avg_loss());
println!("Risk/reward: {:.2}x", stats.risk_reward_ratio());
println!("Max drawdown: {}", stats.max_drawdown);

// Analyze individual trades
for trigger in results.oco_manager.history() {
    println!("{}: {} @ {} ({}bps duration {}ms)",
        trigger.order_id,
        match trigger.trigger_type {
            TriggerType::TakeProfit => "TP",
            TriggerType::StopLoss => "SL",
        },
        trigger.exit_price,
        trigger.pnl_bps,
        trigger.duration_ms,
    );
}
```

---

## Part 10: Summary & Recommendations

### OCO Manager Readiness
**Status: PRODUCTION READY**
- Comprehensive implementation (1,285 LOC)
- 49 unit tests (100% pass)
- All features tested and working
- Timestamp-aware for backtesting

### Backtest Harness Integration Point
**Status: READY FOR INTEGRATION**
- Clear processing pipeline
- Extensible algorithm interface
- Working fill simulation
- Trade logging infrastructure

### Recommended Integration Steps

**Phase 2 (Immediate):**
1. Add OCOManager field to BacktestEngine
2. Add OCO trigger checking in process_event()
3. Implement enter_position() method
4. Add OCO-specific result reporting
5. Create 5-7 integration tests
6. Document entry signal interface

**Phase 3 (Follow-up):**
1. Define TradingStrategy trait
2. Implement trend-following strategy
3. Add walk-forward OCO optimization
4. Integrate with paper trading
5. Real-time OCO + MM backtesting

### Key Files to Modify

| File | Changes |
|------|---------|
| `src/backtest/harness.rs` | Add OCOManager, OCO trigger checking, enter_position() |
| `src/backtest/mod.rs` | Export OCO-related types |
| `src/backtest/metrics.rs` | Add OCO stat aggregation |
| `Cargo.toml` | Already has oco_manager exported |
| `tests/backtest_test.rs` | Add OCO integration tests |

### Success Criteria

- [ ] OCOManager field added to BacktestEngine
- [ ] Trigger checking implemented (before MM processing)
- [ ] Enter position method functional
- [ ] 5+ integration tests passing
- [ ] OCO stats included in BacktestResults
- [ ] Example showing simple trend + OCO strategy
- [ ] Documentation updated with OCO flow diagram

---

## Appendix A: File Paths Reference

### Core OCO Implementation
- **OCO Manager**: `/home/onat/Ingestor/src/trading/oco_manager.rs` (1,285 lines)
- **Position Manager**: `/home/onat/Ingestor/src/trading/position_manager.rs` (450+ lines)
- **Risk Manager**: `/home/onat/Ingestor/src/trading/risk_manager.rs`
- **Trading Module**: `/home/onat/Ingestor/src/trading/mod.rs` (exports all)

### Backtest Infrastructure
- **Backtest Harness**: `/home/onat/Ingestor/src/backtest/harness.rs` (700+ lines)
- **Backtest Module**: `/home/onat/Ingestor/src/backtest/mod.rs` (exports all)
- **Replay Engine**: `/home/onat/Ingestor/src/backtest/replay.rs`
- **Fill Simulator**: `/home/onat/Ingestor/src/backtest/fill_simulator.rs`
- **Metrics**: `/home/onat/Ingestor/src/backtest/metrics.rs`
- **Walk-Forward**: `/home/onat/Ingestor/src/backtest/walk_forward.rs`
- **Grid Search**: `/home/onat/Ingestor/src/backtest/grid_search.rs`

### Features & Data
- **Feature Fusion**: `/home/onat/Ingestor/src/features/feature_fusion.rs`
- **Trend Features**: `/home/onat/Ingestor/src/features/trend_features.rs`
- **Regime Detection**: `/home/onat/Ingestor/src/regime/mod.rs`

### Documentation
- **Main README**: `/home/onat/Ingestor/README.md`
- **Requirements**: `/home/onat/Ingestor/docs/REQUIREMENTS_V0.2.md`
- **Architecture**: `/home/onat/Ingestor/docs/ARCHITECTURE.md`

### Tests
- **Backtest Tests**: `/home/onat/Ingestor/tests/backtest_test.rs` (5 tests)
- **Integration Tests**: `/home/onat/Ingestor/tests/integration_full_pipeline_test.rs`
- **OCO Tests**: In `src/trading/oco_manager.rs` (49 inline tests)

---

## Appendix B: Quick Reference - OCOManager API

```rust
// Create
let mut manager = OCOManager::new();

// Add order
let order = OCOOrder::from_bps(
    "trade1",
    Side::Buy,
    dec!(50000),
    dec!(1.0),
    dec!(20),   // TP
    dec!(10),   // SL
);
manager.add_order(order)?;

// Check triggers (MAIN METHOD)
let triggers = manager.check_triggers_at_time(
    dec!(50100),  // current price
    1234567890,   // timestamp ms
);

// Get stats
let stats = manager.stats();
println!("Win rate: {:.1}%", stats.win_rate());
println!("Total P&L: {}", stats.total_pnl);

// Get history
for trigger in manager.history() {
    println!("{:?}", trigger);
}

// Query active orders
for order in manager.active_orders() {
    println!("Order {} at {}bps to TP", order.id, 
        order.distance_to_tp_bps(current_price));
}

// Cleanup
manager.clear_orders();
manager.reset_stats();
```

---

*Document prepared for OCO integration into Backtest Harness*  
*Project: Ingestor v0.2 - MARS Trading System*  
*Phase: Risk Management Integration*

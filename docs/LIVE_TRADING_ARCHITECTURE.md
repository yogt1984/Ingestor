# Live Trading Architecture Analysis

## Project Structure Overview

The Ingestor codebase is a real-time market microstructure feature extraction and market making platform. The live trading system is fully integrated with:
- Real-time WebSocket feeds from Binance
- Feature computation (60+ microstructure indicators)
- Market making algorithms
- Paper trading simulator
- Terminal UI (TUI) for real-time visualization

---

## 1. Main Orchestration (src/main.rs)

### Architecture Pattern
**Task-based orchestration** using Tokio with message-passing channels:

```
Binance WebSocket Feeds
    ↓
LOB Feed Manager ──→ OrderBook Engine ──→ OrderBookFeatures
                                               ↓
Log Feed Manager ──→ TradesLog Engine ──→ TradesLogFeatures
                                               ↓
Multiple Microstructure Engines (Entropy, Volatility, Toxicity, etc.)
    ↓
FeatureFusionEngine ──→ FeaturesSnapshot
    ↓
├── Persistence Engine (Parquet)
├── TUI (Terminal UI)
└── Forward Testing Session (Paper Trading)
```

### Key Components

#### Channel Setup (lines 43-52)
- **shutdown_rx/tx**: Global shutdown broadcast
- **orderbook_rx/tx**: Order book features (100 capacity)
- **tradeslog_rx/tx**: Trade log features (100 capacity)
- **illiq_rx/tx, entropy_rx/tx, volatility_rx/tx, toxicity_rx/tx**: Microstructure engines
- **fused_rx/tx**: Fused feature snapshots (100 capacity)
- **persist_rx/tx**: Feature persistence to disk (2048 capacity, crossbeam)
- **tui_rx/tx**: TUI updates (100 capacity, crossbeam)

#### Task Spawning Pattern
- **LOB Manager** (Tokio task): Handles WebSocket for order book updates
- **Log Manager** (Tokio task): Handles WebSocket for trade updates
- **Feature Engines** (6 Tokio tasks): Compute microstructure features
- **FeatureFusionEngine** (Tokio task): Combines all features into snapshots
- **Persistence Engine** (Tokio blocking task): Writes Parquet files
- **Forward Bridge** (Tokio task): Routes fused snapshots to TUI and persistence
- **TUI** (std::thread): Blocking thread for terminal rendering

#### Graceful Shutdown
- Uses `watch::channel` for global shutdown signal
- All tasks subscribe to `shutdown_rx.changed()`
- All task handles collected in `tokio::select!` macro (lines 236-253)
- TUI thread joined explicitly after select! completes (line 256)

**Flow Pattern**: Async data pipeline with non-blocking message bridges. Backpressure handled via retry-with-sleep on full channels.

---

## 2. Market Maker Engine (src/market_maker.rs)

### Current Implementation: Avellaneda-Stoikov

The existing `MarketMakerEngine` is an alias for `AvellanedaStoikovMM` (line 615).

#### Core Types

**MMQuotes** (Output of quote computation)
```rust
pub struct MMQuotes {
    pub bid: Option<Quote>,           // Bid side quote
    pub ask: Option<Quote>,           // Ask side quote
    pub regime: MarketRegime,         // Current market regime
    pub fair_value: Decimal,          // Estimated fair value
    pub half_spread: Decimal,         // Half-spread size
    pub skew: Decimal,                // Inventory-based skew
}
```

**MarketRegime** (Regime classification)
```rust
pub enum MarketRegime {
    HighEntropy,    // Random flow, good for tight MM
    MediumEntropy,  // Uncertain regime
    LowEntropy,     // One-sided flow, high adverse selection risk
}
```

**RegimeParams** (Per-regime parameters)
```rust
pub struct RegimeParams {
    pub high_entropy: RegimeConfig,
    pub medium_entropy: RegimeConfig,
    pub low_entropy: RegimeConfig,
}

pub struct RegimeConfig {
    pub spread_bps: f64,       // Half-spread in basis points
    pub skew_factor: f64,      // Inventory skew multiplier
    pub size_mult: f64,        // Quote size multiplier
    pub should_quote: bool,    // Whether to quote at all
}
```

#### Quote Computation Algorithm

**Method**: `compute_quotes()` (lines 412-524)

Steps:
1. **Regime Detection** (line 422): Use entropy score to determine regime
   - Input: `entropy_score` (0-1, normalized from tick entropy)
   - Thresholds: `high_entropy_threshold=0.7`, `low_entropy_threshold=0.4` (default)

2. **Parameter Selection** (line 425): Get regime-specific config

3. **Conditional Quoting** (lines 427-439): Skip quoting if regime config forbids it
   - Default: Low entropy regime has `should_quote=false`

4. **Fair Value** (line 442): Use microprice
   - Microprice = weighted average of best bid/ask by opposite side volumes

5. **Spread Calculation** (lines 444-450):
   - Base spread = `mid_price * spread_bps / 10000`
   - Volatility adjustment: `base_spread * (1 + volatility * 100)`

6. **Inventory Skew** (lines 452-465):
   - Inventory ratio = `inventory / max_inventory`
   - Skew = `inventory_ratio * regime_skew_factor`
   - Additional flow skew in low entropy: `flow_imbalance * 0.5`

7. **Quote Prices** (lines 470-471):
   ```
   bid_price = fair_value - half_spread - (total_skew * half_spread)
   ask_price = fair_value + half_spread - (total_skew * half_spread)
   ```

8. **Quote Sizing** (lines 474-486):
   - Base size = `quote_size * regime_size_mult`
   - When long: bid size = 50%, ask size = 100%
   - When short: bid size = 100%, ask size = 50%

9. **Inventory Limits** (lines 488-511):
   - If at max inventory, stop quoting the directional side

#### Supporting Methods

**compute_entropy_score()** (lines 355-385)
- Combines 1s, 5s, 10s tick entropy into single score
- Normalizes by log2(3) ≈ 1.585 (max entropy for 3-state system)
- Returns: value in [0, 1]

**compute_flow_imbalance()** (lines 389-401)
- Input: aggressive buy/sell volumes
- Output: value in [-1, 1] (negative=sell pressure, positive=buy pressure)

**process_fill()** (lines 527-563)
- Updates inventory and average entry price
- Calculates realized PnL for sell fills
- Tracks fees and trade statistics

**State Management**
- `get_state()`: Return current MMState (inventory, avg_entry, PnL, current quotes)
- `reset()`: Zero out inventory, PnL, and quotes
- `update_mark_to_market()`: Update unrealized PnL based on current price

#### Configuration

**AvellanedaStoikovConfig** (lines 269-310)
```rust
pub struct AvellanedaStoikovConfig {
    pub max_inventory: Decimal,        // Default: 0.1 BTC
    pub quote_size: Decimal,           // Default: 0.001 BTC
    pub risk_aversion: f64,            // Default: 0.1
    pub regime_thresholds: RegimeThresholds,
    pub regime_params: RegimeParams,
}
```

Default parameters:
- High entropy: spread=1bps, skew=0.3, size=1.0x, quote=true
- Medium entropy: spread=2.5bps, skew=0.5, size=0.7x, quote=true
- Low entropy: spread=5bps, skew=1.0, size=0.3x, quote=false

---

## 3. TUI Display System (src/tui.rs)

### Application Modes
```rust
enum AppMode {
    Menu,                 // Main menu
    Live,                 // Live market data (no MM)
    LiveMM,               // Live with market maker active
    PresetSelect,         // Select saved preset
    PaperTradePreset,     // Paper trading with preset
    Features,             // Feature view
    Backtest,             // Backtesting
    WalkForward,          // Walk-forward validation
    DataQuality,          // Data quality check
}
```

### LiveMM Mode Display (draw_live_mm function, lines 1261-1510)

**Layout** (5 panels):
1. **Title Panel** (1 line)
   - Symbol, time, preset info, quote count, controls

2. **Market Maker State** (8 lines)
   - Regime status (HIGH/MEDIUM/LOW ENTROPY with color)
   - Fair value, half spread, skew
   - Inventory (with color: gray=neutral, green=long, red=short)
   - Average entry price, max inventory
   - Volatility and toxicity index

3. **PnL Panel** (6 lines)
   - Realized PnL (colored: green if positive, red if negative)
   - Unrealized PnL
   - Total PnL (bold)
   - Fees paid, trade count, total volume

4. **Current Quotes** (5 lines)
   - Bid price and size
   - Ask price and size
   - Mid price, microprice, market spread

5. **Simulator Stats** (5 lines)
   - Trades seen, bid/ask fills, fill rate
   - Bid/ask misses, fill volume

6. **Market Data** (4+ lines)
   - Imbalance %, Price-Weighted Imbalance (PWI)
   - Order flow imbalance
   - Tick entropy (1s, 5s, 10s)
   - VPIN (Volume-Synchronized Probability of Informed Trading)

### Feature Accumulation & Averaging

**FeatureAccumulator** (lines 86-329)
- Accumulates 60+ raw feature values for 1-second window
- `add()` method: accumulate one snapshot worth of features
- `average()` method: compute mean over accumulated period
- Used for 1Hz update rate despite 10Hz+ input rate

### Update Mechanism

**Main Loop** (lines 681-818)
- Receives `FeaturesSnapshot` from crossbeam channel
- Updates accumulator
- Every 1 second (UPDATE_INTERVAL_MS = 1000ms), renders screen
- Event polling (keyboard input) at < 1s intervals
- Non-blocking channel receive with timeout

**Key Inputs**
- 'l': Switch to Live mode
- 'm': Switch to LiveMM mode
- 'r': Reset paper trader
- 'q': Return to menu
- 'p': Open preset selection

---

## 4. Configuration Handling

### Current System

#### CLI Arguments (Cargo.toml line 32: clap dependency available)
**Status**: Not currently used in main.rs
- No command-line parameters parsed
- Symbol hardcoded: `const SYMBOL: &str = "BTCUSDT";` (line 41 of main.rs)

#### Configuration Files

**Presets System** (src/presets.rs)
- **Location**: `./data/presets.json`
- **Structure**: `PresetStore` containing `Vec<ParameterPreset>`
- **Preset Fields**:
  - name, created_at, optimization_method
  - data_range, num_events
  - expected_return, expected_sharpe, expected_trades, expected_win_rate
  - **spread_bps**: Regime-agnostic half-spread
  - **skew**: Inventory skew factor
  - **high_entropy_threshold**, **low_entropy_threshold**: Regime boundaries
  - **fill_prob_assumption**: For paper trading calibration
  - notes

**Default Presets** (lines 151-196):
1. "GridSearch-Best": spread=1.0bps, skew=0.3, entropy_high=0.7
2. "GridSearch-Conservative": spread=1.0bps, skew=0.3, entropy_high=0.7, 5% fill rate

**Conversion**: `preset.to_mm_config()` (lines 71-82)
- Creates `RegimeParams::uniform(spread_bps, skew)` which scales by regime
- Returns `AvellanedaStoikovConfig`

#### TUI Settings (lines 50-82)
```rust
pub struct TuiSettings {
    pub persist_features: bool,
    pub max_storage_gb: f64,
    pub run_backtest: bool,
}
```

---

## 5. WebSocket Data → Quote Generation Flow

### End-to-End Data Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ BINANCE WEBSOCKET FEEDS                                         │
└──────────────┬────────────────────────────┬────────────────────┘
               │                            │
        Order Book Updates           Trade Updates
               │                            │
        ┌──────▼──────────┐        ┌───────▼────────┐
        │ LobFeedManager  │        │ LogFeedManager │
        └──────┬──────────┘        └───────┬────────┘
               │                            │
               │ Arc<ConcurrentOrderBook>   │ Arc<ConcurrentTradesLog>
               │ (Tokio task)               │ (Tokio task)
               │                            │
        ┌──────▼──────────────┐    ┌───────▼──────────────┐
        │ OrderBookEngine     │    │ TradesLogEngine      │
        │ (Tokio task)        │    │ (Tokio task)         │
        └──────┬──────────────┘    └───────┬──────────────┘
               │                            │
               └────────────┬───────────────┘
                            │
                  OrderBookFeatures + TradesLogFeatures
                            │
        ┌─────────────────────┴──────────────────┐
        │ 5 More Feature Engines                 │
        │ (Entropy, Volatility, Toxicity, etc)  │
        └──────────────────────┬─────────────────┘
                               │
                  ┌────────────▼─────────────┐
                  │ FeatureFusionEngine      │
                  │ (Tokio task)             │
                  └────────────┬─────────────┘
                               │
                       FeaturesSnapshot
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                  │
        ┌───▼────┐    ┌───────▼────────┐  ┌────▼────┐
        │ Persist │    │ PaperTrading   │  │   TUI   │
        └────────┘    └───────┬────────┘  └────────┘
                              │
                      ┌───────▼────────────┐
                      │ PaperTradingEngine │
                      │ (in TUI thread)    │
                      │                    │
                      ├─ MarketMakerEngine│
                      │  (compute_quotes) │
                      │                    │
                      ├─ MMSimulator      │
                      │  (fill matching)  │
                      └────────┬──────────┘
                               │
                         MMQuotes
                               │
                      Display in TUI LiveMM
```

### Detailed Call Chain

**Step 1: TUI Receives FeaturesSnapshot** (tui.rs line 714+)
```rust
// In main TUI loop
match fused_rx.recv() {
    Some(snapshot) => {
        // Accumulate features
        feature_accumulator.add(&snapshot);
    }
}
```

**Step 2: TUI Enters LiveMM Mode** (tui.rs lines 587-602)
```rust
// On 'm' key press
paper_trading = PaperTradingEngine::new(
    AvellanedaStoikovMM::new(default_config),
    SimulatorConfig::default()
);
mode = AppMode::LiveMM;
```

**Step 3: Quote Computation (Every 1 second)** (tui.rs lines 724-760)
```rust
if mode == AppMode::LiveMM && accumulator has data {
    // Extract averaged features from last second
    let feat = feature_accumulator.average();
    
    // Compute entropy score
    let entropy_score = paper_trading.mm.compute_entropy_score(
        feat.tick_entropy_1s,
        feat.tick_entropy_5s,
        feat.tick_entropy_10s
    );
    
    // Compute flow imbalance
    let flow_imbalance = paper_trading.mm.compute_flow_imbalance(
        buy_volume,
        sell_volume
    );
    
    // Get volatility (from realized_volatility_100)
    let volatility = feat.realized_volatility_100;
    
    // Call quote computation
    let quotes = paper_trading.on_features(
        microprice,
        mid_price,
        volatility,
        entropy_score,
        flow_imbalance,
        timestamp_ms
    );
}
```

**Step 4: PaperTradingEngine.on_features()** (mm_simulator.rs lines 202-227)
```rust
pub fn on_features(
    &mut self,
    microprice: Decimal,
    mid_price: Decimal,
    volatility: f64,
    entropy_score: f64,
    flow_imbalance: f64,
    timestamp_ms: u64,
) -> MMQuotes {
    // Call MM engine
    let quotes = self.mm.compute_quotes(
        microprice,
        mid_price,
        volatility,
        entropy_score,
        flow_imbalance,
        timestamp_ms,
    );
    
    // Update simulator with quotes
    self.simulator.update_quotes(&quotes);
    self.last_quotes = Some(quotes.clone());
    
    // Update mark-to-market
    self.mm.update_mark_to_market(mid_price);
    
    quotes
}
```

**Step 5: MarketMakerEngine.compute_quotes()** (market_maker.rs lines 412-524)
- Runs the Avellaneda-Stoikov algorithm (described in Section 2)
- Returns MMQuotes with bid/ask prices and regime

**Step 6: Display in TUI** (tui.rs lines 791-810)
```rust
AppMode::LiveMM => {
    draw_live_mm(
        f,
        &symbol,
        &current_features,
        &paper_trading,
        &forward_session,
        active_preset.as_ref()
    );
}
```

**Step 7: Fill Simulation** (When market trade occurs)
```rust
// In main TUI loop, when trade update received
for trade in trades {
    let fills = paper_trading.on_trade(&trade, timestamp);
    for fill in fills {
        // Fill processing already done in on_trade()
        // Updates inventory, PnL, average entry price
    }
}
```

---

## 6. Algorithm Selection Architecture

### Current State

**Single Algorithm Path**: 
- main.rs creates `AvellanedaStoikovMM` directly
- TUI uses presets to configure spread/skew parameters
- No algorithm selection mechanism

### Available Infrastructure

**Algorithm Module** (src/algorithms/)
- **mod.rs**: Factory functions and utilities
- **traits.rs**: `MarketMakingAlgorithm` trait
- **avellaneda_stoikov.rs**: A-S implementation (wrapper)
- **ml_spread_skew.rs**: ML implementation

**Trait-Based Design** (algorithms/traits.rs)
```rust
pub trait MarketMakingAlgorithm: Send + Sync {
    fn algorithm_type(&self) -> AlgorithmType;
    fn compute_quotes(&mut self, input: &MarketInput) -> MMQuotes;
    fn process_fill(&mut self, fill: Fill, fee_rate: Decimal);
    fn update_mark_to_market(&mut self, current_price: Decimal);
    fn get_state(&self) -> MMState;
    fn reset(&mut self);
    // ... more methods
}

pub enum AlgorithmType {
    AvellanedaStoikov,
    MLSpreadSkew,
}
```

**Factory Functions** (algorithms/mod.rs)
```rust
pub fn create_algorithm(
    algo_type: AlgorithmType,
    max_inventory: Decimal,
    quote_size: Decimal,
    regime_params: Option<RegimeParams>,
) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError>

pub fn create_ml_algorithm(
    max_inventory: Decimal,
    quote_size: Decimal,
    weights: MLModelWeights,
) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError>
```

### MLSpreadSkewAlgorithm Details

**Purpose**: Use ML to predict optimal spread and skew based on market features

**Model Architecture** (ml_spread_skew.rs lines 7-12)
```
spread_bps = w0 + w1*entropy + w2*volatility + w3*imbalance + w4*entropy*volatility
skew_factor = v0 + v1*entropy + v2*volatility + v3*imbalance + v4*inventory_ratio
```

**Weight Structures**:
- `SpreadWeights`: intercept, w_entropy, w_volatility, w_imbalance, w_interaction
- `SkewWeights`: intercept, w_entropy, w_volatility, w_imbalance, w_inventory
- `MLModelWeights`: Combined spread + skew weights with version/training metadata

**Default Weights** (ml_spread_skew.rs lines 66-114):
- Spread intercept: 3.0 bps
- Spread entropy weight: -2.0 (high entropy → tighter)
- Spread volatility weight: 500.0 (high vol → wider)
- Skew intercept: 0.5
- Skew entropy weight: -0.2
- Skew inventory weight: -0.8 (main driver)

**Capabilities**:
- Load weights from JSON file: `MLModelWeights::load_from_file()`
- Save weights to JSON: `weights.save_to_file()`
- Serialize/deserialize for storage

---

## Key Data Structures Summary

### Input Path
1. **FeaturesSnapshot** (feature_fusion.rs): 60+ market microstructure features
2. **AveragedFeatures** (tui.rs): 1-second aggregated features
3. **MarketInput** (algorithms/traits.rs): Normalized input for algorithms

### Output Path
1. **MMQuotes**: Bid/ask prices and metadata
2. **PaperTradingState**: Complete trading session state
3. **SessionMetrics**: Performance metrics for forward testing

### State Tracking
1. **MMState**: inventory, avg_entry_price, PnL, current quotes
2. **PnLTracker**: realized_pnl, unrealized_pnl, fees, num_trades, volume
3. **SimulatorStats**: trades_seen, bid_fills, ask_fills, bid_misses, ask_misses

---

## Synchronization Mechanisms

### Inter-Task Communication
- **Tokio MPSC channels**: For feature data pipeline (ordered, async)
- **Crossbeam bounded channels**: For persistence and TUI (high throughput)
- **Watch channels**: For global shutdown signal (broadcast)

### Backpressure Handling
- **Persistence forward bridge** (main.rs lines 217-227): Retry with sleep on full channel
- **TUI receiver** (main.rs line 214): Non-blocking try_send, drops if full
- **Feature engines**: All have 100-capacity channels; block if full

### Thread Safety
- **Arc<ConcurrentOrderBook>**: Shared read-only access to order book
- **Arc<ConcurrentTradesLog>**: Shared read-only access to trades
- **Tokio sync primitives**: Used for async channel coordination
- **TUI in blocking thread**: Allows expensive terminal rendering without blocking async runtime

---

## Summary Table

| Component | Language | Runs In | Responsibility |
|-----------|----------|---------|---|
| LobFeedManager | Rust | Tokio task | WebSocket → OrderBook feed |
| LogFeedManager | Rust | Tokio task | WebSocket → Trades feed |
| OrderBookEngine | Rust | Tokio task | Compute order book features |
| TradesLogEngine | Rust | Tokio task | Compute trade features |
| EntropyEngine | Rust | Tokio task | Compute entropy metrics |
| VolatilityEngine | Rust | Tokio task | Compute volatility metrics |
| ToxicityEngine | Rust | Tokio task | Compute toxicity metrics |
| IlliquidityEngine | Rust | Tokio task | Compute liquidity metrics |
| FeatureFusionEngine | Rust | Tokio task | Combine all features |
| PersistenceEngine | Rust | Tokio blocking task | Write Parquet files |
| PaperTradingEngine | Rust | Main/TUI thread | MM coordination |
| AvellanedaStoikovMM | Rust | Main/TUI thread | Quote computation |
| MMSimulator | Rust | Main/TUI thread | Fill simulation |
| TUI | Rust | std::thread | Terminal UI rendering |


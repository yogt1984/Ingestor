# INGESTOR - User Manual Summary

**Real-Time Market Microstructure & Algorithmic Trading Platform**

---

## TABLE OF CONTENTS

1. [TUI Menu Options & Key Bindings](#tui-menu-options--key-bindings)
2. [Available Algorithms](#available-algorithms)
3. [Preset System](#preset-system)
4. [CLI Commands for Backtesting](#cli-commands-for-backtesting)
5. [Data Flow Architecture](#data-flow-architecture)
6. [Feature Overview](#feature-overview)

---

## TUI MENU OPTIONS & KEY BINDINGS

### Main Menu Access
Launch with: `cargo run --release`

### Menu Options

#### DATA COLLECTION
| Option | Key | Description | Output |
|--------|-----|-------------|--------|
| **Live Dashboard** | `[0]` | Stream real-time market data, compute 60+ microstructure features, save to Parquet | `./data/features/*.parquet` |
| **Live + Market Maker** | `[1]` | Paper trade with default parameters on live data | Live P&L + session logs |
| **Paper Trade w/ Preset** | `[6]` | Paper trade with pre-optimized parameters (select from grid search results) | Session results saved |

#### BACKTESTING
| Option | Key | Description | Output |
|--------|-----|-------------|--------|
| **Run Backtest** | `[3]` | Single backtest on historical data with specified parameters | Performance metrics |
| **Walk-Forward Validation** | `[4]` | Time-series cross-validation (prevents overfitting) | Sharpe per fold + aggregate |
| **Data Quality Check** | `[5]` | Validate Parquet files before backtesting | Data issues report |
| **Campaign Simulation** | `[7]` | Simulate 4-week validation campaign on historical data | Campaign verdict |

#### INFO & SETTINGS
| Option | Key | Description |
|--------|-----|-------------|
| **Feature Descriptions** | `[2]` | Display all 60+ features with explanations |
| **Persist to Disk** | `[p]` | Toggle ON/OFF (saves features to Parquet) |
| **Max Storage** | `[s]` | Set storage limit (GB), adjust retention |
| **Quit** | `[q]` or `Esc` | Exit application |

### Live Dashboard Interface

When running `[0]` or `[1]`, displays:

```
BTCUSDT | 14:32:15 | [Live/Paper Trade] | [q] menu

┌─ MARKET MAKER STATE ────────────────────────────────────┐
│ REGIME HIGH ENTROPY   ENTROPY 0.923                     │
│ FAIR VALUE 97234.82  HALF SPREAD 4.50  SKEW +0.02       │
│ INVENTORY +0.005200  AVG ENTRY 97230.00  MAX 0.1000     │
│ VOLATILITY 0.000234  TOXICITY 0.28                      │
└─────────────────────────────────────────────────────────┘
┌─ P&L ───────────────────────────────────────────────────┐
│ REALIZED +0.0234  UNREALIZED +0.0012  TOTAL +0.0246     │
│ FEES PAID 0.000045  TRADES 12  VOLUME 0.0520            │
└─────────────────────────────────────────────────────────┘
┌─ QUOTES ────────────────────────────────────────────────┐
│   BID 97230.32 x 0.0010      ASK 97239.32 x 0.0010      │
│   MID 97234.75  MICRO 97234.82  SPREAD 0.50             │
└─────────────────────────────────────────────────────────┘
┌─ SIMULATOR ─────────────────────────────────────────────┐
│ TRADES SEEN 1234  BID FILLS 6  ASK FILLS 6  FILL 12.5%  │
│ BID MISSES 42  ASK MISSES 38  FILL VOL 0.0120           │
└─────────────────────────────────────────────────────────┘
```

### Paper Trade Mode Controls

When in **Paper Trade w/ Preset** (`[6]`):
| Key | Action |
|-----|--------|
| `[Up]` / `[k]` | Navigate presets up |
| `[Down]` / `[j]` | Navigate presets down |
| `[Enter]` | Select and run preset |
| `[r]` | Reset session state |
| `[h]` | Show help |
| `[q]` / `Esc` | Return to menu |

### Feature Display Mode (`[2]`)
| Key | Action |
|-----|--------|
| `[Up]` / `[k]` | Scroll up |
| `[Down]` / `[j]` | Scroll down |
| `[Page Up]` | Jump up 10 features |
| `[Page Down]` | Jump down 10 features |
| `[q]` / `Esc` | Return to menu |

---

## AVAILABLE ALGORITHMS

### 1. Avellaneda-Stoikov Market Making (Classic)

**Type ID:** `avellaneda_stoikov`

**Theory:** Optimal inventory-based market making from Avellaneda & Stoikov (2008)

**How It Works:**
```
1. Compute reservation price (adjust for inventory)
   reservation = mid_price - inventory × gamma × volatility² × time_horizon

2. Compute optimal spread (widens with volatility)
   spread = gamma×volatility²×T + (2/gamma)×ln(1 + gamma/k)

3. Skew quotes based on inventory
   - Long inventory: widen asks, tighten bids (want to sell)
   - Short inventory: tighten asks, widen bids (want to buy)

4. Adjust for market regime (entropy-based)
   - HIGH entropy (>0.7): tight spreads, aggressive (good for MM)
   - MEDIUM entropy (0.4-0.7): moderate spreads
   - LOW entropy (<0.4): wide spreads or no quotes (trending, dangerous)
```

**Configuration Parameters:**
- `spread_bps`: Base half-spread in basis points (default 1.0-2.0)
- `skew_factor`: Inventory skew multiplier (default 0.3-0.5)
- `max_inventory`: Position limit in BTC (default 0.1)
- `quote_size`: Order size per side in BTC (default 0.001)
- `high_entropy_threshold`: Threshold for high entropy regime (default 0.7)
- `low_entropy_threshold`: Threshold for low entropy regime (default 0.4)

**Best Observed Performance:**
- Spread: 1.0 bps (2025-12-03 grid search)
- Skew: 0.3
- Expected Return: +5.14% over 47 days
- Win Rate: 59.5%
- Trade Count: 452
- Sharpe Ratio: -1.20 (with realistic 10% fill rate)

---

### 2. ML Spread/Skew Predictor (Neural)

**Type ID:** `ml_spread_skew`

**Theory:** Machine learning with learned linear weights for adaptive spread/skew

**How It Works:**
```
1. Input features (computed in real-time):
   - Entropy score (0.0 = trending, 1.0 = mean-reverting)
   - Volatility (annualized from price returns)
   - Order book imbalance (-1.0 to 1.0, positive = buy pressure)
   - Inventory ratio (current position / max inventory)

2. Linear spread model:
   spread_bps = intercept 
              + w_entropy × entropy
              + w_volatility × volatility
              + w_imbalance × imbalance
              + w_interaction × (entropy × volatility)

3. Linear skew model:
   skew_factor = intercept
               + w_entropy × entropy
               + w_volatility × volatility
               + w_imbalance × imbalance
               + w_inventory × (inventory_ratio)

4. Generate quotes at computed spread/skew
   (same fill simulation as Avellaneda-Stoikov)
```

**Model Weights (Default Baseline):**

**Spread Model:**
- Intercept: 3.0 bps
- w_entropy: -2.0 (high entropy → tighter spread)
- w_volatility: 500.0 (high vol → wider spread)
- w_imbalance: 1.0 (high imbalance → wider spread)
- w_interaction: -100.0 (entropy dampens volatility effect)

**Skew Model:**
- Intercept: 0.5
- w_entropy: -0.2 (less aggressive in high entropy)
- w_volatility: 50.0 (higher vol → more aggressive)
- w_imbalance: 0.1 (follow flow slightly)
- w_inventory: -1.0 (main driver: reduce long/short exposure)

**Training Methods:**
1. **Walk-Forward ML** (Implemented)
   - Train on rolling windows of historical data
   - Validate on holdout periods
   - Consensus weight averaging across folds
   
2. **Bayesian Optimization** (Can integrate Optuna)
   - Smart parameter search vs grid search
   - Learns from previous trials

**Best Observed Performance (Walk-Forward):**
- Model: ML-Trained (2025-12-06)
- Training Sharpe: -1.49
- Expected Return: +3.2%
- Trades: 14 (more selective than A-S)

---

## PRESET SYSTEM

### What Are Presets?

Presets are **saved parameter configurations** with metadata:
- Parameter values (spread, skew, entropy thresholds)
- Optimization method (grid-search, walk-forward-ml, manual)
- Expected performance (return, Sharpe, win rate)
- Training data range
- Creation timestamp
- Notes

### Default Presets Included

```json
1. GridSearch-Best (2025-12-03)
   - Algorithm: Avellaneda-Stoikov
   - Spread: 1.0 bps
   - Skew: 0.3
   - Method: Grid-search (360 combinations)
   - Data: Oct 16 - Dec 2, 2025 (47 days)
   - Expected: +5.14% return, 59.5% win rate, 452 trades
   - Status: VALIDATED via walk-forward

2. GridSearch-Conservative (2025-12-03)
   - Algorithm: Avellaneda-Stoikov
   - Spread: 1.0 bps
   - Skew: 0.3
   - Fill Assumption: 5% (vs 10% for Best)
   - Expected: +1.09% return, 55% win rate, 202 trades
   - Use Case: More realistic fill rates for live trading

3. ML-Trained (2025-12-06)
   - Algorithm: ML Spread/Skew Predictor
   - Method: Walk-forward ML training
   - Data: Oct 16 - Dec 6, 2025 (50 days)
   - Model Version: walk-forward-v1
   - Training Sharpe: -1.49
   - Expected: +3.2% return
   - Trades: 14
   - Weights: Embedded in preset JSON

4. ML-Baseline (2025-12-06)
   - Algorithm: ML Spread/Skew Predictor
   - Method: Manual (default weights)
   - Use Case: Comparison baseline
```

### Using Presets

**In TUI:**
1. Press `[6]` from main menu (Paper Trade w/ Preset)
2. Navigate with `[Up]/[Down]` or `[k]/[j]`
3. Press `[Enter]` to select
4. Paper trades with selected parameters

**Via CLI:**
```bash
# Access preset in backtest (future feature)
cargo run --release --bin backtest -- --preset GridSearch-Best
```

### Creating Custom Presets

Presets are stored in `./data/presets.json`:

```json
{
  "presets": [
    {
      "name": "My Custom Preset",
      "created_at": "2025-12-08T12:00:00Z",
      "optimization_method": "manual",
      "data_range": "Dec 1-8, 2025",
      "num_events": 50000,
      "expected_return": 0.02,
      "expected_sharpe": -1.5,
      "expected_trades": 300,
      "expected_win_rate": 0.55,
      "spread_bps": 1.5,
      "skew": 0.4,
      "high_entropy_threshold": 0.7,
      "low_entropy_threshold": 0.4,
      "fill_prob_assumption": 0.10,
      "notes": "Conservative variant",
      "algorithm_type": "avellaneda_stoikov",
      "ml_weights": null,
      "ml_weights_path": null
    }
  ]
}
```

---

## CLI COMMANDS FOR BACKTESTING

### Basic Syntax
```bash
cargo run --release --bin backtest -- [COMMAND] [OPTIONS]
```

### Commands

#### 1. Single Backtest (Default)
```bash
# Run with default parameters
cargo run --release --bin backtest --

# With custom parameters
cargo run --release --bin backtest -- \
    --spread 2.0 \
    --skew 0.5 \
    --fill-prob 0.10 \
    --high-entropy 0.7

# Output as JSON (for automation)
cargo run --release --bin backtest -- \
    --spread 1.0 \
    --json \
    --output results.json
```

**Options:**
- `--data`: Path to Parquet features (default: `./data/features`)
- `--spread`: Base half-spread bps (default: 2.0)
- `--skew`: Inventory skew factor (default: 0.5)
- `--fill-prob`: Fill probability assumption (default: 0.10)
- `--high-entropy`: High entropy threshold (default: 0.7)
- `--low-entropy`: Low entropy threshold (default: 0.4)
- `--max-inventory`: Position limit BTC (default: 0.1)
- `--quote-size`: Order size BTC (default: 0.001)
- `--fee-rate`: Trading fee (default: 0.0001 = 1 bps)
- `--output`: Save results to JSON file
- `--json`: Output as machine-readable JSON
- `--quiet`: No progress output

#### 2. Parameter Sweep
```bash
cargo run --release --bin backtest -- sweep \
    --spreads 1,2,3,4,5 \
    --skews 0.3,0.5,0.7,1.0
```

**Options:**
- `--spreads`: Comma-separated spread values (bps)
- `--skews`: Comma-separated skew values

**Output:** Table ranked by Sharpe ratio

#### 3. Grid Search (Comprehensive)
```bash
cargo run --release --bin backtest -- grid-search \
    --spreads 1,2,3,4,5 \
    --skews 0.3,0.5,0.7,1.0 \
    --high-entropies 0.6,0.7,0.8 \
    --fill-probs 0.05,0.10,0.15 \
    --output grid_results.json
```

**Options:**
- `--spreads`: Spread values (default: 1,2,3,4,5)
- `--skews`: Skew values (default: 0.3,0.5,0.7,1.0)
- `--high-entropies`: High entropy thresholds (default: 0.6,0.7,0.8)
- `--fill-probs`: Fill probability assumptions (default: 0.05,0.10,0.15)
- `--output`: Save results

**Output:** Ranked by Sharpe; includes top 10 configurations

#### 4. Walk-Forward Validation
```bash
cargo run --release --bin backtest -- walk-forward \
    --folds 5 \
    --test-hours 24 \
    --rolling \
    --output validation.json
```

**Options:**
- `--folds`: Number of validation folds (default: 5)
- `--test-hours`: Test period per fold (default: 24)
- `--rolling`: Use rolling window (vs expanding anchored)
- `--output`: Save detailed results

**Output:** 
```
Fold 1: Sharpe=-1.45 Trades=456
Fold 2: Sharpe=-1.32 Trades=489
...
Average Sharpe: -1.38
Std Dev: 0.12
```

#### 5. Out-of-Sample Validation
```bash
cargo run --release --bin backtest -- oos-validate \
    --holdout 0.20 \
    --embargo-hours 1.0 \
    --spreads 1,2,3 \
    --skews 0.3,0.5 \
    --output oos_results.json
```

**Options:**
- `--holdout`: % of data to reserve for testing (0.1-0.5)
- `--embargo-hours`: Gap between train/test to prevent lookahead
- `--spreads`: Values to test
- `--skews`: Values to test
- `--fill-probs`: Fill probability assumptions

**Output:** In-sample vs out-of-sample performance comparison

#### 6. Regime-Specific Search
```bash
cargo run --release --bin backtest -- regime-search \
    --high-spreads 0.5,1.0,1.5 \
    --med-spreads 2.0,2.5,3.0 \
    --low-spreads 4.0,5.0,none \
    --high-skews 0.2,0.3,0.4 \
    --med-skews 0.4,0.5,0.6 \
    --low-skews 0.8,1.0,1.2
```

**Options:** Separate parameters for each regime (high/medium/low entropy)

**Output:** Best parameters per regime

#### 7. Multi-Objective Optimization
```bash
cargo run --release --bin backtest -- multi-objective \
    --spreads 1,2,3,4,5 \
    --skews 0.3,0.5,0.7 \
    --fill-probs 0.05,0.10,0.15 \
    --min-trades 20
```

**Output:** Pareto frontier (best tradeoff between Sharpe, drawdown, fill rate)

#### 8. Data Quality Check
```bash
cargo run --release --bin backtest -- validate \
    --output quality_report.json
```

**Output:**
- File count and size
- Missing feature percentages
- Time gaps
- Event counts per period

#### 9. Data Info
```bash
cargo run --release --bin backtest -- info
```

**Output:**
```
Data Directory: ./data/features
Files: 97 Parquet files
Total Size: 19.4 MB
Date Range: 2025-10-16 to 2025-12-02
Events: 73,092
Time Coverage: 47 days (estimated)
Avg Events/Day: 1,554
```

---

## DATA FLOW ARCHITECTURE

### Data Collection Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                      BINANCE WEBSOCKET API                         │
│                                                                     │
│  • Order Book Updates (100ms, top 20 levels)                       │
│  • Trade Stream (real-time, all trades)                            │
└────────┬──────────────────────────────┬──────────────────────────┘
         │                              │
         ▼                              ▼
    ┌──────────────┐            ┌──────────────┐
    │ OrderBook    │            │ TradesLog    │
    │ Snapshot     │            │ Snapshot     │
    │ (Concurrent  │            │ (Concurrent  │
    │  Storage)    │            │  Storage)    │
    └──────┬───────┘            └──────┬───────┘
           │                           │
           ▼                           ▼
    ┌──────────────────┐      ┌──────────────────┐
    │OrderBookEngine   │      │TradesLogEngine   │
    │ - Best bid/ask   │      │ - Trade imbalance│
    │ - Book depth     │      │ - VWAP           │
    │ - PWI (1,5,25,50)│      │ - Trade rate     │
    │ - Slopes         │      │ - Momentum       │
    └──────┬───────────┘      └──────┬───────────┘
           │                         │
           └───────────┬─────────────┘
                       │
           ┌───────────▼────────────┐
           │ Special Purpose        │
           │ Engines (Async):       │
           │ • IlliquidityEngine    │
           │ • EntropyEngine        │
           │ • VolatilityEngine     │
           │ • ToxicityEngine       │
           └───────────┬────────────┘
                       │
           ┌───────────▼────────────┐
           │ FeatureFusionEngine    │
           │                        │
           │ Merges all metrics     │
           │ into single snapshot   │
           └───────────┬────────────┘
                       │
           ┌───────────▼────────────────────┐
           │  FeaturesSnapshot              │
           │  (60+ fields + metadata)       │
           └───────────┬────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
    ┌────────┐  ┌──────────┐  ┌──────────┐
    │   TUI  │  │Persistence│  │MarketMaker│
    │Display │  │Engine     │  │Engine    │
    │(1 Hz)  │  │(Parquet)  │  │(Paper TL)│
    └────────┘  └──────────┘  └──────────┘
                    │              │
                    ▼              ▼
              ┌──────────────┐  ┌────────────┐
              │./data/       │  │MMSimulator │
              │features/     │  │ - Fills    │
              │*.parquet     │  │ - Inventory│
              │(1000 rows/   │  │ - P&L      │
              │ file ~200KB) │  │ - Session  │
              └──────────────┘  │  logs      │
                                └────────────┘
```

### Data Flow Stages

#### Stage 1: Real-Time Ingestion (Async)
- Binance WebSocket streams to concurrent data structures (lock-free)
- OrderBook updates: 100ms interval (top 20 levels)
- Trades: Real-time, all trades captured
- Rate: ~100 updates/second sustained

#### Stage 2: Feature Computation
- **OrderBookEngine**: Computes bid/ask, spread, imbalance, depth, slopes
- **TradesLogEngine**: Computes trade imbalance, VWAP, momentum, momentum
- **IlliquidityEngine**: Roll spread, Amihud λ, Kyle λ, Hasbrouck λ, VPIN
- **EntropyEngine**: Tick entropy at 1s/5s/10s/15s/30s/1m/15m windows
- **VolatilityEngine**: Realized vol, bipower variation, jumps
- **ToxicityEngine**: Toxic flow ratio, adverse selection, flow pressure

All computed with ~100ms latency

#### Stage 3: Feature Fusion (Sequential)
- Merges all metrics into single FeaturesSnapshot
- Timestamp: UTC with millisecond precision
- Output rate: ~10-100 Hz (depends on market activity)

#### Stage 4: Distribution
- **Persistence**: Write batches to Parquet (1000 rows ≈ 200KB per file)
- **TUI**: Display to user at 1 Hz (aggregates 10-100 samples)
- **Market Maker**: Feed into MM engine for quote computation

#### Stage 5: Backtesting Workflow
```
Parquet Files
    │
    ▼
ReplayEngine (chronological replay)
    │
    ├─→ State: OrderBook + Trades snapshots at each event
    │
    ├─→ MarketMakerEngine (compute quotes)
    │   ├─→ Input: Market state, entropy, volatility
    │   └─→ Output: MMQuotes (bid/ask prices and sizes)
    │
    └─→ MMSimulator (paper trading)
        ├─→ For each quote: simulate fills
        ├─→ Track inventory, P&L, fills
        └─→ Output: PerformanceMetrics (Sharpe, return, drawdown)
```

---

## FEATURE OVERVIEW

### 60+ Computed Microstructure Features

#### Order Book Features (18)
| Feature | Window | Description |
|---------|--------|-------------|
| Best Bid/Ask | Real-time | Top of book prices |
| Spread | Real-time | Ask - Bid (bps) |
| Microprice | Real-time | Volume-weighted fair value |
| Imbalance | Real-time | (Bid Vol - Ask Vol) / Total Vol |
| PWI 1/5/25/50 | Real-time | Depth-weighted imbalance at % levels |
| Bid/Ask Slope | Real-time | Order book resilience |
| Bid/Ask Depth Ratio | Real-time | Concentration (top 3 / top 10) |
| Volume at 0.1% | Real-time | Liquidity near top of book |

#### Trade Features (17)
| Feature | Window | Description |
|---------|--------|-------------|
| Last Trade Price | Real-time | Most recent executed trade |
| Trade Imbalance | 10s, 50s, 100s, 1000s | (Buy Vol - Sell Vol) / Total |
| VWAP | Total, 10s, 50s, 100s, 1000s | Volume-weighted average price |
| Price Change | Real-time | Change from last trade |
| Avg Trade Size | Real-time | Mean execution size |
| Signed Count Momentum | Real-time | Net buy/sell count |
| Trade Rate | 10s | Trades per second |
| Aggression Ratio | 10s, 50s, 100s, 1000s | Buy / (Buy + Sell) trades |

#### Entropy Metrics (14)
| Feature | Windows | Description |
|---------|---------|-------------|
| Tick Entropy | 1s, 5s, 10s, 15s, 30s, 1m, 15m | Price direction randomness |
| Volume Tick Entropy | Same windows | Trade-size weighted entropy |

**Interpretation:**
- **High (>0.7):** Mean-reverting, random walk, ideal for MM
- **Medium (0.4-0.7):** Uncertain, mixed signals
- **Low (<0.4):** Trending, one-directional, dangerous for MM

#### Volatility Metrics (5)
| Feature | Description |
|---------|-------------|
| Realized Volatility (100, 1000) | √(Σ(log returns)²) |
| Bipower Variation | Jump-robust volatility |
| Jump Indicator | Z-score detecting price jumps |
| Vol-of-Vol | Volatility of volatility |

#### Toxicity Metrics (7)
| Feature | Range | Description |
|---------|-------|-------------|
| Toxic Flow Ratio | 0-1 | Adverse selection prob at microprice |
| Adverse Selection | bps | Expected adverse price move post-fill |
| VPIN | 0-1 | Informed trading probability |
| Arrival Asymmetry | -1 to 1 | Imbalance in aggressive order arrivals |
| Size Toxicity Ratio | 0-1 | Large orders = more informed |
| Order Flow Pressure | Decimal | Cumulative imbalance (low → filled slower) |
| Toxicity Index | 0-1 | Composite adverse selection metric |

---

## WORKFLOW: DATA → OPTIMIZE → TRADE → VALIDATE

### Phase 1: Collect Data (3-6 months)
```bash
# Run live dashboard continuously
cargo run --release
# Press [0] and leave running (use tmux/screen)
# Target: 3+ months of continuous data
```
**Output:** `./data/features/*.parquet` (1000s of files, ~200KB each)

### Phase 2: Optimize Parameters (Hours)
```bash
# Grid search over all parameters
cargo run --release --bin backtest -- grid-search --test-gate

# Bayesian optimization (faster, smarter)
python3 scripts/optimize.py --trials 100 --metric sharpe
```
**Output:** Top 10 parameter sets with expected performance

### Phase 3: Paper Trade (4 weeks)
```bash
cargo run --release
# Press [6] to select best preset
# Run for 4+ weeks on live data
# Compare actual vs backtest expectations
```
**Output:** `./data/sessions/*.json` with actual fills, P&L

### Phase 4: Validate Results
Compare paper trading to backtest:
- **Fill Rate:** Expected 10% → Actual X%?
- **Sharpe Ratio:** Expected -1.2 → Actual?
- **Win Rate:** Expected 59.5% → Actual?
- **Drawdown:** Expected 0.3% → Actual?

If validation successful → Ready for live trading at small size.

---

## EXAMPLE: GRID SEARCH RESULTS (Dec 3, 2025)

Best configuration found from 360 parameter combinations:

```
Parameters:
  Spread: 1.0 bps
  Skew: 0.3
  Fill Probability Assumption: 10%
  High Entropy Threshold: 0.7

Performance:
  Return: +5.14% over 47 days
  Sharpe Ratio: -1.20
  Max Drawdown: 0.43%
  Win Rate: 59.5% (238 wins / 452 trades)
  Trades: 452
  Inventory Turnover: Good
  
Data:
  Period: Oct 16 - Dec 2, 2025
  Events: 73,000
  Files: 97 Parquet
  
Notes:
  - Only tight spread (1 bps) profitable
  - Wider spreads (2+ bps) don't recover transaction costs
  - UNGATED strategy (spread widening) >> GATED (no quotes)
  - Entropy threshold 0.6/0.7/0.8 produce identical results
```

---

## KEY REFERENCES

- **Avellaneda & Stoikov (2008):** High-frequency trading in a limit order book
- **Cont et al. (2014):** Price impact of order book events (fills)
- **Moallemi & Yuan (2017):** Queue position modeling
- **Easley et al. (2012):** Flow toxicity (VPIN)
- **Barndorff-Nielsen (2004):** Bipower variation (jump-robust vol)

---

## TROUBLESHOOTING

### Data Not Saving?
- Check `[p]` is ON in menu
- Check disk space
- Review `./data/features/` directory

### Backtest Too Slow?
- Use smaller data subset
- Reduce grid search parameter ranges
- Use `--quiet` flag to suppress progress

### Fill Rate Mismatch (Paper vs Backtest)?
1. Check queue position assumption (default 0.5 = middle of queue)
2. Verify actual spread in live market
3. Compare microprice vs mid price
4. Check latency (quote age when executed)

### Poor Sharpe Ratio?
- Negative Sharpe is expected with realistic fill model
- Focus on: return > transaction costs, win rate > 50%
- Increase data for more trades
- Try ML algorithm (more adaptive)

